import dataclasses
from decimal import Decimal
from importlib.resources import files

import pytest
import yaml

from qa_core_recon.refusals import RefusalCode, RefusalError
from qa_core_recon.spec import SpecError, _parts_cover, load_spec, parse_spec, to_s3a

TEMPLATE = (files("qa_core_recon") / "templates" / "recon.yaml").read_text(encoding="utf-8")
TARGET = "s3://demo-bucket/curated/payments"
ALLOW = ["./data", "s3://demo-bucket"]


def _text(mutate) -> str:
    data = yaml.safe_load(TEMPLATE)
    mutate(data)
    return yaml.safe_dump(data)


def _refusal(mutate) -> tuple[RefusalCode, str]:
    with pytest.raises(RefusalError) as caught:
        parse_spec(_text(mutate))
    refusal = caught.value.refusal
    return refusal.code, refusal.message


def _ok(mutate):
    return parse_spec(_text(mutate))


def _error(mutate) -> str:
    with pytest.raises(SpecError) as caught:
        parse_spec(_text(mutate))
    return str(caught.value)


def test_default_template_is_valid():
    spec = parse_spec(TEMPLATE)
    assert spec.version == 1
    assert spec.source.path == "./data/raw/payments.csv"
    assert spec.source.format == "csv"
    assert spec.source.table is None
    assert spec.source.snapshot_id is None
    assert spec.target.path == TARGET
    assert spec.target.format == "parquet"
    assert spec.target.table is None
    assert spec.target.snapshot_id is None
    assert spec.keys == ("txn_id",)
    assert spec.amounts[0].column == "amount"
    assert spec.amounts[0].scale == 2
    assert spec.amounts[0].cast is None
    assert spec.sampled is False
    assert spec.tolerance.value == Decimal("0")
    assert spec.tolerance.reason == ""
    assert spec.allowlist == ("./data", "s3://demo-bucket")
    assert isinstance(spec.keys, tuple)
    assert isinstance(spec.amounts, tuple)


def test_spec_is_frozen():
    spec = parse_spec(TEMPLATE)
    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.version = 2  # type: ignore[misc]


def test_missing_key_when_absent():
    code, message = _refusal(lambda data: data.pop("keys"))
    assert code is RefusalCode.MISSING_KEY
    assert message == "the spec has no business key"


def test_missing_key_when_empty():
    code, message = _refusal(lambda data: data.__setitem__("keys", []))
    assert code is RefusalCode.MISSING_KEY
    assert message == "the spec has no business key"


def test_missing_key_when_null():
    code, message = _refusal(lambda data: data.__setitem__("keys", None))
    assert code is RefusalCode.MISSING_KEY
    assert message == "the spec has no business key"


def test_keys_of_the_wrong_type_are_malformed():
    assert _error(lambda data: data.__setitem__("keys", "txn_id")) == "keys must be a list"


def test_tolerance_without_reason_when_blank():
    def mutate(data):
        data["tolerance"]["value"] = "0.01"
        data["tolerance"]["reason"] = "   "

    code, message = _refusal(mutate)
    assert code is RefusalCode.TOLERANCE_WITHOUT_REASON
    assert message == "tolerance is above zero and reason is empty"


def test_tolerance_without_reason_when_empty_string():
    def mutate(data):
        data["tolerance"]["value"] = "0.01"
        data["tolerance"]["reason"] = ""

    code, _message = _refusal(mutate)
    assert code is RefusalCode.TOLERANCE_WITHOUT_REASON


def test_zero_tolerance_with_blank_reason_is_ok():
    def mutate(data):
        data["tolerance"]["value"] = "0.00"
        data["tolerance"]["reason"] = " \t"

    spec = _ok(mutate)
    assert spec.tolerance.value == Decimal("0.00")


def test_positive_tolerance_with_reason_is_ok():
    def mutate(data):
        data["tolerance"]["value"] = "0.01"
        data["tolerance"]["reason"] = "fx rounding"

    spec = _ok(mutate)
    assert spec.tolerance.value == Decimal("0.01")


def test_path_not_allowed_for_source():
    code, message = _refusal(lambda data: data["source"].__setitem__("path", "./database/x.csv"))
    assert code is RefusalCode.PATH_NOT_ALLOWED
    assert message == "source path './database/x.csv' is outside the allowlist"


def test_path_not_allowed_checks_source_before_target():
    def mutate(data):
        data["source"]["path"] = "./database/x.csv"
        data["target"]["path"] = "s3://other-bucket/file.csv"

    code, message = _refusal(mutate)
    assert code is RefusalCode.PATH_NOT_ALLOWED
    assert message.startswith("source path ")


def test_allowlist_child_is_allowed():
    spec = _ok(lambda data: data["source"].__setitem__("path", "./data/x.csv"))
    assert spec.source.path == "./data/x.csv"


def test_allowlist_prefix_lookalike_is_refused():
    code, _message = _refusal(lambda data: data["source"].__setitem__("path", "./database/x.csv"))
    assert code is RefusalCode.PATH_NOT_ALLOWED
    code, _message = _refusal(lambda data: data["source"].__setitem__("path", "./data-extra/x.csv"))
    assert code is RefusalCode.PATH_NOT_ALLOWED


def test_allowlist_traversal_cannot_escape():
    escapes = (
        "./data/../../secret.csv",
        "./data/../database/x.csv",
        "data/../../etc/passwd",
    )
    for path in escapes:
        code, message = _refusal(lambda data, path=path: data["source"].__setitem__("path", path))
        assert code is RefusalCode.PATH_NOT_ALLOWED
        assert message == f"source path {path!r} is outside the allowlist"


def test_allowlist_traversal_that_returns_is_allowed():
    _ok(lambda data: data["source"].__setitem__("path", "./data/../data/x.csv"))
    _ok(lambda data: data["source"].__setitem__("path", "./data/raw/../raw/payments.csv"))


def test_allowlist_trailing_slash_still_matches():
    def mutate(data):
        data["allowlist"] = ["./data/", "s3://demo-bucket"]
        data["source"]["path"] = "./data/x.csv"
        data["target"]["path"] = "s3://demo-bucket/curated/payments"

    spec = _ok(mutate)
    assert spec.source.path == "./data/x.csv"


def test_allowlist_s3_bucket_lookalike_is_refused():
    lookalikes = (
        "s3://demo-bucket-other/curated/payments",
        "s3://demo-bucket2/file.csv",
        "s3://demo-bucket.evil/file.csv",
    )
    for path in lookalikes:
        code, message = _refusal(lambda data, path=path: data["target"].__setitem__("path", path))
        assert code is RefusalCode.PATH_NOT_ALLOWED
        assert message == f"target path {path!r} is outside the allowlist"


def test_s3_bucket_root_allows_a_child():
    spec = _ok(lambda data: data["target"].__setitem__("path", "s3://demo-bucket/curated/payments"))
    assert spec.target.path == "s3://demo-bucket/curated/payments"


def test_allowlist_s3_key_lookalike_is_refused():
    def mutate(data):
        data["allowlist"] = ["./data", "s3://demo-bucket/curated"]
        data["target"]["path"] = "s3://demo-bucket/curated2/file.csv"

    code, message = _refusal(mutate)
    assert code is RefusalCode.PATH_NOT_ALLOWED
    assert "curated2/file.csv" in message


def test_s3_dotdot_segment_is_not_folded_into_the_allowlist():
    def mutate(data):
        data["allowlist"] = ["./data", "s3://demo-bucket/curated"]
        data["target"]["path"] = "s3://demo-bucket/other/../curated/x"

    assert _error(mutate) == "target.path has a '..' key segment"


def test_empty_allowlist_refuses_every_path():
    code, message = _refusal(lambda data: data.__setitem__("allowlist", []))
    assert code is RefusalCode.PATH_NOT_ALLOWED
    assert message == "source path './data/raw/payments.csv' is outside the allowlist"


def test_allowlist_match_is_case_sensitive():
    code, _message = _refusal(lambda data: data["source"].__setitem__("path", "./Data/x.csv"))
    assert code is RefusalCode.PATH_NOT_ALLOWED


def test_absolute_and_relative_entries_stay_separate():
    code, _message = _refusal(lambda data: data["source"].__setitem__("path", "/data/x.csv"))
    assert code is RefusalCode.PATH_NOT_ALLOWED

    def mutate(data):
        data["source"]["path"] = "/var/data/x.csv"
        data["allowlist"] = ["/var/data", "s3://demo-bucket"]

    assert _ok(mutate).source.path == "/var/data/x.csv"


def test_unsupported_scheme_is_malformed():
    message = _error(lambda data: data["source"].__setitem__("path", "https://example.com/a.csv"))
    assert message == "source.path uses unsupported scheme 'https'; use a local path or s3://"


def test_float_tolerance_is_rejected():
    def mutate(data):
        data["tolerance"]["value"] = 0.01
        data["tolerance"]["reason"] = "rounding"

    message = _error(mutate)
    assert message == (
        "tolerance.value must be a quoted decimal string; a YAML float is not accepted"
    )


def test_integer_tolerance_is_rejected():
    message = _error(lambda data: data["tolerance"].__setitem__("value", 0))
    assert message == "tolerance.value must be a quoted decimal string"


def test_negative_tolerance_is_rejected():
    message = _error(lambda data: data["tolerance"].__setitem__("value", "-0.01"))
    assert message == "tolerance.value must be a finite decimal of zero or greater"


def test_structural_error_wins_over_missing_key():
    def mutate(data):
        data.pop("keys")
        data["tolerance"]["value"] = 0.5

    message = _error(mutate)
    assert "YAML float" in message


def test_unknown_keys_are_sorted():
    def mutate(data):
        data["zebra"] = 1
        data["alpha"] = 1

    assert _error(mutate) == "unknown keys: 'alpha', 'zebra'"


def test_wrong_type_is_rejected():
    assert _error(lambda data: data.__setitem__("version", "1")) == "version must be an integer"


def test_bool_is_not_an_integer_scale():
    message = _error(lambda data: data["amounts"][0].__setitem__("scale", True))
    assert message == "amounts[0].scale must be an integer"


def test_unsupported_version_is_rejected():
    message = _error(lambda data: data.__setitem__("version", 2))
    assert message == "unsupported spec version 2; only version 1 is supported"


def test_unsupported_format_is_rejected():
    message = _error(lambda data: data["source"].__setitem__("format", "json"))
    assert message == "unsupported source format 'json'; use csv, parquet, or iceberg"


def test_empty_amounts_and_groups_are_valid():
    def mutate(data):
        data["amounts"] = []
        data["group_by"] = []
        data["hash_columns"] = []

    spec = _ok(mutate)
    assert spec.amounts == ()
    assert spec.group_by == ()
    assert spec.hash_columns == ()


def test_messages_are_deterministic():
    text = _text(lambda data: data.__setitem__("keys", []))
    first = _catch(text)
    second = _catch(text)
    assert first == second == "missing-key: the spec has no business key"


def _catch(text: str) -> str:
    with pytest.raises(RefusalError) as caught:
        parse_spec(text)
    return str(caught.value)


def _parse_error(text: str) -> str:
    with pytest.raises(SpecError) as caught:
        parse_spec(text)
    return str(caught.value)


def test_duplicate_allowlist_is_malformed_and_does_not_widen_access():
    extra = 'allowlist: ["/", "s3://demo-bucket"]\n'
    text = TEMPLATE.replace("path: ./data/raw/payments.csv", "path: /etc/passwd", 1) + extra
    line = text.count("\n")
    assert _parse_error(text) == f"duplicate key 'allowlist' at line {line}"


def test_duplicate_tolerance_is_malformed():
    extra = 'tolerance:\n  value: "0.01"\n  reason: "fx rounding"\n'
    text = TEMPLATE + extra
    line = [index for index, row in enumerate(text.splitlines()) if row == "tolerance:"][1] + 1
    assert _parse_error(text) == f"duplicate key 'tolerance' at line {line}"


def test_duplicate_nested_format_is_malformed():
    text = TEMPLATE.replace(
        "  format: csv  # csv, parquet, or iceberg\n",
        "  format: json\n  format: csv\n",
        1,
    )
    line = text.splitlines().index("  format: csv") + 1
    assert _parse_error(text) == f"duplicate key 'format' at line {line}"


def test_python_object_tag_is_rejected():
    message = _parse_error("!!python/object/apply:os.system\n- 'true'\n")
    assert message.startswith("invalid YAML:")


def test_empty_allowlist_entry_is_malformed():
    entries = (".", "./data/..", "./.")
    for entry in entries:
        message = _error(
            lambda data, entry=entry: data.__setitem__("allowlist", [entry, "s3://demo-bucket"])
        )
        assert message == "allowlist[0] must not be an empty path"


def test_root_allowlist_entry_is_malformed():
    def mutate(data):
        data["source"]["path"] = "/etc/passwd"
        data["allowlist"] = ["/", "s3://demo-bucket"]

    assert _error(mutate) == "allowlist[0] must not be the filesystem root"


def test_allowlist_entry_that_still_contains_dotdot_is_malformed():
    def mutate(data):
        data["source"]["path"] = "../secret.csv"
        data["allowlist"] = ["../secret.csv", "s3://demo-bucket"]

    assert _error(mutate) == "allowlist[0] must not contain '..'"


def test_allowlist_dotdot_that_normalises_inside_is_ok():
    def mutate(data):
        data["allowlist"] = ["./data/../data", "s3://demo-bucket"]
        data["source"]["path"] = "./data/x.csv"

    assert _ok(mutate).source.path == "./data/x.csv"


def test_dotdot_after_a_matching_prefix_is_outside():
    assert _parts_cover(("data",), ("data", "..", "etc")) is False
    assert _parts_cover((), ("..", "etc")) is False
    assert _parts_cover(("data",), ("data", "x.csv")) is True


def test_s3_dot_empty_query_and_fragment_are_malformed():
    cases = (
        ("s3://demo-bucket/./curated/x", "target.path has a '.' key segment"),
        ("s3://demo-bucket/curated//x", "target.path has an empty key segment"),
        ("s3://demo-bucket/curated/payments//", "target.path has an empty key segment"),
        ("s3://demo-bucket//curated", "target.path has an empty key segment"),
        (
            "s3://demo-bucket/curated/x?versionId=1",
            "target.path must not include a query or fragment",
        ),
        ("s3://demo-bucket/curated/x#part", "target.path must not include a query or fragment"),
    )
    for path, message in cases:
        assert _error(lambda data, path=path: data["target"].__setitem__("path", path)) == message


def test_s3_one_trailing_slash_matches_and_maps_to_s3a():
    def mutate(data):
        data["allowlist"] = ["./data", "s3://demo-bucket/curated/"]
        data["target"]["path"] = "s3://demo-bucket/curated/payments/"

    spec = _ok(mutate)
    assert spec.target.path == "s3://demo-bucket/curated/payments/"
    assert to_s3a(spec.target.path) == "s3a://demo-bucket/curated/payments"
    assert to_s3a("s3://demo-bucket/") == "s3a://demo-bucket"

    def bucket_root(data):
        data["allowlist"] = ["./data", "s3://demo-bucket/"]
        data["target"]["path"] = "s3://demo-bucket/curated/payments"

    assert _ok(bucket_root).target.path == "s3://demo-bucket/curated/payments"


def test_s3_bucket_names_follow_s3_rules():
    rejected = (
        "s3://Demo/key",
        "s3://ab/key",
        "s3://demo_bucket/key",
        "s3://192.168.0.1/key",
        "s3://demo..bucket/key",
        "s3://-demo/key",
        "s3://xn--bucket/key",
        "s3://my-bucket-s3alias/key",
        "s3://my--ol-s3/key",
        "s3://my.mrap/key",
        "s3://my--x-s3/key",
        "s3://my--table-s3/key",
        "s3://sthree-bucket/key",
        "s3://amzn-s3-demo-bucket/key",
        "a" * 64,
    )
    for path in rejected:
        full = path if path.startswith("s3://") else f"s3://{path}/key"
        message = _error(lambda data, full=full: data["target"].__setitem__("path", full))
        assert message.startswith("target.path bucket ")
        assert message.endswith(" is not a valid S3 bucket name")

    def allow(data):
        data["allowlist"] = ["./data", "s3://AB"]

    assert _error(allow) == "allowlist[1] bucket 'AB' is not a valid S3 bucket name"

    def short_ok(data):
        data["allowlist"] = ["./data", "s3://abc"]
        data["target"]["path"] = "s3://abc/file.csv"

    assert _ok(short_ok).target.path == "s3://abc/file.csv"


def test_s3_glob_characters_are_rejected_on_paths_and_allowlist_entries():
    cases = (
        ("s3://demo-bucket/pq/*", "*"),
        ("s3://demo-bucket/pq/a[1].csv", "["),
        ("s3://demo-bucket/pq/a].csv", "]"),
        ("s3://demo-bucket/pq/{a,b}.csv", "{"),
        ("s3://demo-bucket/pq/a}.csv", "}"),
        ("s3://demo-bucket/pq/a\\b.csv", "\\"),
    )
    for path, char in cases:
        message = _error(lambda data, path=path: data["target"].__setitem__("path", path))
        assert f"contains glob character {char!r}" in message

    def allow(data):
        data["allowlist"] = ["./data", "s3://demo-bucket/pq/*"]

    message = _error(allow)
    assert message.startswith("allowlist[1] key segment '*' contains glob character '*'")


def test_local_path_with_a_colon_in_the_first_segment_is_rejected():
    message = _error(lambda data: data["source"].__setitem__("path", "file:/etc/hosts"))
    assert message == "source.path must not contain a colon in the first path segment"

    def allow(data):
        data["allowlist"] = ["./data", "file:"]

    assert _error(allow) == "allowlist[1] must not contain a colon in the first path segment"

    hidden = (
        "file:/../etc/hosts",
        "./file:/../etc",
        "file:/../tmp/x.csv",
    )
    for path in hidden:
        message = _error(lambda data, path=path: data["source"].__setitem__("path", path))
        assert message == "source.path must not contain a colon in the first path segment"

        def allow_hidden(data, path=path):
            data["allowlist"] = [path, "s3://demo-bucket"]

        assert _error(allow_hidden) == (
            "allowlist[0] must not contain a colon in the first path segment"
        )


def test_colon_in_a_parsed_part_is_rejected_after_dotdot_is_folded():
    message = _error(lambda data: data["source"].__setitem__("path", "data/../file:/etc/hosts"))
    assert message == "source.path must not contain a colon in the first path segment"


def test_local_glob_characters_are_rejected_on_paths_and_allowlist_entries():
    message = _error(lambda data: data["source"].__setitem__("path", "data/a[1].csv"))
    assert message == "source.path path segment 'a[1].csv' contains glob character '['"

    def allow(data):
        data["allowlist"] = ["data/a[1].csv", "s3://demo-bucket"]

    assert _error(allow) == ("allowlist[0] path segment 'a[1].csv' contains glob character '['")

    message = _error(lambda data: data["source"].__setitem__("path", "data/x?.csv"))
    assert message == "source.path path segment 'x?.csv' contains glob character '?'"

    def question(data):
        data["allowlist"] = ["data/a?", "s3://demo-bucket"]

    assert _error(question) == ("allowlist[0] path segment 'a?' contains glob character '?'")


def test_credentials_are_not_a_spec_field():
    assert (
        _error(lambda data: data["source"].__setitem__("secret", "x"))
        == "source has unknown key: 'secret'"
    )


def test_nested_unknown_keys_are_malformed():
    assert (
        _error(lambda data: data["source"].__setitem__("extra", 1))
        == "source has unknown key: 'extra'"
    )
    assert (
        _error(lambda data: data["target"].__setitem__("extra", 1))
        == "target has unknown key: 'extra'"
    )
    assert (
        _error(lambda data: data["amounts"][0].__setitem__("extra", 1))
        == "amounts[0] has unknown key: 'extra'"
    )


def test_negative_scale_is_malformed():
    message = _error(lambda data: data["amounts"][0].__setitem__("scale", -1))
    assert message == "amounts[0].scale must be zero or greater"


def test_multiline_name_is_malformed():
    message = _error(lambda data: data.__setitem__("keys", ["txn\nid"]))
    assert message == "keys[0] must be a single line"


def test_spec_file_must_be_utf8(tmp_path):
    path = tmp_path / "recon.yaml"
    path.write_bytes(b"\xff")
    with pytest.raises(SpecError) as caught:
        load_spec(path)
    assert str(caught.value) == f"spec file is not valid UTF-8: {path}"


def test_non_finite_tolerance_is_malformed():
    assert (
        _error(lambda data: data["tolerance"].__setitem__("value", "NaN"))
        == "tolerance.value must be a finite decimal of zero or greater"
    )

    def mutate(data):
        data["tolerance"]["value"] = "Infinity"
        data["tolerance"]["reason"] = "not a number"

    assert _error(mutate) == "tolerance.value must be a finite decimal of zero or greater"


def test_invalid_yaml_is_a_spec_error():
    message = _parse_error(":\n  - [\n")
    assert message.startswith("invalid YAML:")
    assert "\n" not in message


def test_spec_must_be_a_mapping():
    assert _parse_error("- just\n- a\n- list\n") == "spec must be a mapping"
    assert _error(lambda data: data.__setitem__("source", ["nope"])) == "source must be a mapping"


@pytest.mark.parametrize(
    "field",
    [
        "version",
        "source",
        "target",
        "amounts",
        "group_by",
        "hash_columns",
        "tolerance",
        "sampled",
        "allowlist",
    ],
)
def test_missing_required_field_is_malformed(field):
    assert _error(lambda data: data.pop(field)) == f"missing {field}"


def test_deeply_nested_yaml_is_malformed():
    lines = [f"{'  ' * level}a:" for level in range(1200)]
    lines.append(f"{'  ' * 1200}1")
    assert _parse_error("\n".join(lines) + "\n") == "YAML is nested too deeply"


def test_sampled_true_is_loaded_without_a_refusal():
    spec = _ok(lambda data: data.__setitem__("sampled", True))
    assert spec.sampled is True


def test_sampled_must_be_a_boolean():
    assert _error(lambda data: data.__setitem__("sampled", "false")) == "sampled must be a boolean"
    assert _error(lambda data: data.__setitem__("sampled", 0)) == "sampled must be a boolean"


def test_sampled_shape_wins_over_missing_key():
    def mutate(data):
        data.pop("keys")
        data["sampled"] = "false"

    assert _error(mutate) == "sampled must be a boolean"


def test_cast_modes_are_stored():
    for mode in ("half-even", "half-up"):
        spec = _ok(lambda data, mode=mode: data["amounts"][0].__setitem__("cast", mode))
        assert spec.amounts[0].cast == mode


def test_unknown_cast_is_malformed():
    message = _error(lambda data: data["amounts"][0].__setitem__("cast", "half-down"))
    assert message == "amounts[0].cast must be one of 'half-even', 'half-up'"


def test_cast_must_be_a_string():
    assert (
        _error(lambda data: data["amounts"][0].__setitem__("cast", True))
        == "amounts[0].cast must be a string"
    )
    assert (
        _error(lambda data: data["amounts"][0].__setitem__("cast", None))
        == "amounts[0].cast must be a string"
    )


def test_scale_38_is_valid_and_39_is_malformed():
    spec = _ok(lambda data: data["amounts"][0].__setitem__("scale", 38))
    assert spec.amounts[0].scale == 38
    message = _error(lambda data: data["amounts"][0].__setitem__("scale", 39))
    assert message == "amounts[0].scale must be at most 38"
