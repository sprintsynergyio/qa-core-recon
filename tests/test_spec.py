import dataclasses
from decimal import Decimal
from importlib.resources import files

import pytest
import yaml

from qa_core_recon.refusals import RefusalCode, RefusalError
from qa_core_recon.spec import SpecError, parse_spec

TEMPLATE = (files("qa_core_recon") / "templates" / "recon.yaml").read_text(encoding="utf-8")
TARGET = "s3://demo-bucket/curated/payments/"
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
    assert spec.target.path == TARGET
    assert spec.target.format == "parquet"
    assert spec.keys == ("txn_id",)
    assert spec.amounts[0].column == "amount"
    assert spec.amounts[0].scale == 2
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

    _code, message = _refusal(mutate)
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
        data["allowlist"] = ["./data/", "s3://demo-bucket/"]
        data["source"]["path"] = "./data/x.csv"
        data["target"]["path"] = "s3://demo-bucket/curated/payments"

    spec = _ok(mutate)
    assert spec.source.path == "./data/x.csv"


def test_allowlist_s3_bucket_lookalike_is_refused():
    lookalikes = (
        "s3://demo-bucket-other/curated/payments/",
        "s3://demo-bucket2/file.csv",
        "s3://demo-bucket.evil/file.csv",
    )
    for path in lookalikes:
        code, message = _refusal(lambda data, path=path: data["target"].__setitem__("path", path))
        assert code is RefusalCode.PATH_NOT_ALLOWED
        assert message == f"target path {path!r} is outside the allowlist"


def test_allowlist_s3_child_and_trailing_slash_are_allowed():
    _ok(lambda data: data["target"].__setitem__("path", "s3://demo-bucket"))
    _ok(lambda data: data["target"].__setitem__("path", "s3://demo-bucket/"))
    _ok(lambda data: data["target"].__setitem__("path", "s3://demo-bucket/curated/../curated/a"))


def test_allowlist_s3_key_lookalike_is_refused():
    def mutate(data):
        data["allowlist"] = ["./data", "s3://demo-bucket/curated"]
        data["target"]["path"] = "s3://demo-bucket/curated2/file.csv"

    code, message = _refusal(mutate)
    assert code is RefusalCode.PATH_NOT_ALLOWED
    assert "curated2/file.csv" in message


def test_allowlist_s3_traversal_out_of_bucket_is_refused():
    code, _message = _refusal(
        lambda data: data["target"].__setitem__("path", "s3://demo-bucket/../other/file.csv")
    )
    assert code is RefusalCode.PATH_NOT_ALLOWED


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
    assert message == "unsupported source format 'json'; use csv or parquet"


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
