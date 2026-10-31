"""Aggregate run: reports, exit codes, and refusals. Fixtures live in tmp_path."""

import hashlib
import json
import os
from decimal import Decimal

from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from qa_core_recon import __version__
from qa_core_recon.cli import main
from qa_core_recon.compare import DUPLICATE_EXAMPLE_LIMIT
from qa_core_recon.exit_codes import ExitCode
from qa_core_recon.spark import IcebergRuntimeError, SessionStartError
from tests.support import spec_data, write_csv, write_spec_file
from tests.test_iceberg import _schema, _write_iceberg
from tests.test_readers import _write_parquet

HEADER = "txn_id,amount,day,note\n"
ROW = "1,10.50,2024-01-01,a\n"
_CHECK_NAMES = [
    "row_count",
    "amount_sum",
    "null_keys",
    "duplicate_keys",
    "key_diff",
    "changed_columns",
]


def _invoke(spec, out, force=False):
    argv = ["run", "--spec", str(spec), "--out", str(out)]
    if force:
        argv.append("--force")
    return main(argv)


def _pair(tmp_path, source_body, target_body, header=HEADER, **changes):
    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    write_csv(source, source_body, header)
    write_csv(target, target_body, header)
    data = spec_data(tmp_path, source, target, target_format="csv")
    data.update(changes)
    return write_spec_file(tmp_path, data), source, target


def _load(out):
    raw = (out / "report.json").read_bytes()
    markdown = (out / "report.md").read_text(encoding="utf-8")
    report = json.loads(raw)
    return report, raw, markdown


def _assert_canonical(raw: bytes) -> None:
    text = raw.decode("utf-8")
    assert text.endswith("\n")
    assert not text.endswith("\n\n")
    loaded = json.loads(text)
    assert text == json.dumps(loaded, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _assert_prose(markdown: str) -> None:
    assert markdown.endswith("\n")
    assert "\u2014" not in markdown
    assert "\u2013" not in markdown
    for line in markdown.splitlines():
        if line.startswith("#"):
            title = line.lstrip("#").strip()
            assert title == title.title()


def _assert_error(code, captured, message: str) -> None:
    assert code is ExitCode.REFUSED
    assert captured.out == ""
    assert captured.err == f"qa-core-recon: error: {message}\n"
    assert "Traceback" not in captured.err


def _assert_refused_line(code, captured, message: str) -> None:
    assert code is ExitCode.REFUSED
    assert captured.out == ""
    assert captured.err == f"refused: {message}\n"
    assert "Traceback" not in captured.err


def _assert_report_shape(report, spec_path, status) -> None:
    assert set(report) == {
        "checks",
        "refusals",
        "row_counts",
        "spec_sha256",
        "status",
        "tool",
        "version",
    }
    assert report["tool"] == "qa-core-recon"
    assert report["version"] == __version__
    assert report["status"] == status
    assert report["spec_sha256"] == hashlib.sha256(spec_path.read_bytes()).hexdigest()


def _names(out):
    return sorted(path.name for path in out.iterdir())


def test_match_csv_and_parquet_writes_a_canonical_report(spark, tmp_path, capsys):
    source = tmp_path / "source.csv"
    target = tmp_path / "target"
    write_csv(source, ROW)
    _write_parquet(
        spark,
        target,
        [("1", Decimal("10.50"), "2024-01-01", "a")],
        DecimalType(10, 2),
    )
    data = spec_data(tmp_path, source, target)
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    code = _invoke(spec, out)
    captured = capsys.readouterr()
    assert code is ExitCode.MATCH
    assert captured.out == "match\n"
    assert captured.err == ""
    assert _names(out) == ["report.json", "report.md"]
    report, raw, markdown = _load(out)
    _assert_canonical(raw)
    _assert_prose(markdown)
    _assert_report_shape(report, spec, "match")
    assert str(out) not in raw.decode() + markdown
    assert str(spec) not in raw.decode() + markdown
    assert str(source) not in raw.decode() + markdown
    assert [check["name"] for check in report["checks"]] == _CHECK_NAMES
    assert [check["status"] for check in report["checks"]] == ["match"] * len(_CHECK_NAMES)
    assert report["row_counts"] == {"source": 1, "target": 1}
    assert report["refusals"] == []
    amount = report["checks"][1]["details"]["columns"][0]
    assert amount["source"] == "10.50"
    assert amount["target"] == "10.50"
    assert amount["difference"] == "0.00"
    assert amount["scale"] == 2
    assert isinstance(amount["source"], str)
    group = amount["groups"][0]
    assert group["group"] == {"day": "2024-01-01"}
    assert group["source"] == "10.50"
    assert group["status"] == "match"
    assert "Status: Match" in markdown
    assert "10.50" in markdown
    assert "Grouped by day." in markdown

    other = tmp_path / "other"
    assert _invoke(spec, other) is ExitCode.MATCH
    assert (out / "report.json").read_bytes() == (other / "report.json").read_bytes()
    assert (out / "report.md").read_bytes() == (other / "report.md").read_bytes()
    assert _invoke(spec, out, force=True) is ExitCode.MATCH
    assert (out / "report.json").read_bytes() == (other / "report.json").read_bytes()
    assert (out / "report.md").read_bytes() == (other / "report.md").read_bytes()


def test_match_iceberg(spark, tmp_path, capsys):
    source = tmp_path / "source.csv"
    write_csv(source, ROW)
    warehouse = tmp_path / "warehouse"
    _write_iceberg(
        spark,
        warehouse,
        "payments.txn",
        [("1", Decimal("10.50"), "2024-01-01", "a")],
        _schema(DecimalType(10, 2)),
        mode="overwrite",
    )
    spec = write_spec_file(
        tmp_path,
        spec_data(
            tmp_path,
            source,
            warehouse,
            target_format="iceberg",
            target_table="payments.txn",
        ),
    )
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MATCH
    assert capsys.readouterr().out == "match\n"
    report, _raw, _markdown = _load(out)
    assert report["status"] == "match"
    assert report["row_counts"] == {"source": 1, "target": 1}
    assert report["checks"][1]["details"]["columns"][0]["target"] == "10.50"


def test_count_mismatch(tmp_path, capsys):
    spec, _source, _target = _pair(
        tmp_path,
        ROW,
        "1,10.50,2024-01-01,a\n2,0.00,2024-01-01,b\n",
    )
    out = tmp_path / "out"
    code = _invoke(spec, out)
    captured = capsys.readouterr()
    assert code is ExitCode.MISMATCH
    assert captured.out == "mismatch\n"
    assert captured.err == ""
    report, raw, markdown = _load(out)
    _assert_canonical(raw)
    _assert_report_shape(report, spec, "mismatch")
    row_count = report["checks"][0]
    assert row_count["status"] == "mismatch"
    assert row_count["details"]["source"] == 1
    assert row_count["details"]["target"] == 2
    assert row_count["details"]["groups"][0]["source"] == 1
    assert row_count["details"]["groups"][0]["target"] == 2
    assert report["checks"][1]["status"] == "match"
    assert report["checks"][1]["details"]["columns"][0]["difference"] == "0.00"
    assert [check["name"] for check in report["checks"]] == _CHECK_NAMES
    assert "Status: Mismatch" in markdown


def test_sum_mismatch_of_one_cent(tmp_path):
    spec, _source, _target = _pair(tmp_path, ROW, "1,10.51,2024-01-01,a\n")
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    report, _raw, markdown = _load(out)
    amount = report["checks"][1]["details"]["columns"][0]
    assert report["checks"][0]["status"] == "match"
    assert amount["status"] == "mismatch"
    assert amount["source"] == "10.50"
    assert amount["target"] == "10.51"
    assert amount["difference"] == "0.01"
    assert amount["groups"][0]["difference"] == "0.01"
    assert amount["groups"][0]["status"] == "mismatch"
    assert "Difference: 0.01." in markdown


def test_tolerance_within_and_outside_its_range(tmp_path):
    within, _source, _target = _pair(
        tmp_path,
        ROW,
        "1,10.51,2024-01-01,a\n",
        tolerance={"value": "0.01", "reason": "penny rounding"},
    )
    within_out = tmp_path / "within"
    # The sums are inside the tolerance. The row hash is not, so the run mismatches.
    assert _invoke(within, within_out) is ExitCode.MISMATCH
    report, _raw, markdown = _load(within_out)
    amount = report["checks"][1]["details"]["columns"][0]
    assert amount["status"] == "match"
    assert amount["difference"] == "0.01"
    assert amount["tolerance"] == "0.01"
    assert amount["reason"] == "penny rounding"
    assert amount["groups"][0]["status"] == "match"
    assert "Reason: penny rounding." in markdown
    assert report["status"] == "mismatch"
    assert report["checks"][4]["status"] == "mismatch"
    assert report["checks"][4]["details"]["changed"]["count"] == 1

    outside_dir = tmp_path / "outside-data"
    outside_dir.mkdir()
    outside, _source, _target = _pair(
        outside_dir,
        "1,10.50,2024-01-01,a\n",
        "1,10.52,2024-01-01,a\n",
        tolerance={"value": "0.01", "reason": "penny rounding"},
    )
    outside_out = tmp_path / "outside"
    assert _invoke(outside, outside_out) is ExitCode.MISMATCH
    report, _raw, markdown = _load(outside_out)
    amount = report["checks"][1]["details"]["columns"][0]
    assert amount["difference"] == "0.02"
    assert amount["status"] == "mismatch"
    assert amount["reason"] == "penny rounding"
    assert "Reason: penny rounding." in markdown


def test_tolerance_applies_to_the_total_and_to_each_group(tmp_path):
    spec, _source, _target = _pair(
        tmp_path,
        "1,10.00,2024-01-01,a\n2,10.00,2024-01-02,b\n",
        "1,10.01,2024-01-01,a\n2,10.01,2024-01-02,b\n",
        tolerance={"value": "0.01", "reason": "penny rounding"},
    )
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    amount = _load(out)[0]["checks"][1]["details"]["columns"][0]
    assert amount["difference"] == "0.02"
    assert amount["status"] == "mismatch"
    assert [group["status"] for group in amount["groups"]] == ["match", "match"]
    assert [group["group"]["day"] for group in amount["groups"]] == ["2024-01-01", "2024-01-02"]


def test_missing_group_is_listed_in_sorted_order(tmp_path):
    spec, _source, _target = _pair(
        tmp_path,
        "1,10.00,2024-01-02,a\n2,5.00,2024-01-01,b\n3,1.00,2024-01-03,c\n",
        "1,10.00,2024-01-02,a\n4,2.00,2024-01-04,d\n",
        tolerance={"value": "1000", "reason": "a wide band that does not cover a missing group"},
    )
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    report, _raw, markdown = _load(out)
    groups = report["checks"][0]["details"]["groups"]
    assert [group["group"]["day"] for group in groups] == [
        "2024-01-01",
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
    ]
    assert [group["missing"] for group in groups] == ["target", None, "target", "source"]
    assert groups[0]["source"] == 1
    assert groups[0]["target"] == 0
    assert groups[0]["status"] == "mismatch"
    amount_groups = report["checks"][1]["details"]["columns"][0]["groups"]
    assert amount_groups[0]["status"] == "mismatch"
    assert amount_groups[0]["difference"] is None
    assert amount_groups[0]["target"] is None
    assert report["checks"][1]["status"] == "mismatch"
    assert "This group is missing on the target." in markdown
    assert "This group is missing on the source." in markdown
    assert markdown.index("day=2024-01-01") < markdown.index("day=2024-01-04")


def test_null_key_is_a_mismatch(spark, tmp_path):
    source = tmp_path / "source"
    target = tmp_path / "target"
    amount = DecimalType(10, 2)
    _write_parquet(
        spark,
        source,
        [("1", Decimal("10.00"), None, "a")],
        amount,
    )
    _write_parquet(
        spark,
        target,
        [("1", Decimal("10.00"), "2024-01-01", "a")],
        amount,
    )
    data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
    data["keys"] = ["txn_id", "day"]
    data["group_by"] = []
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    report, _raw, markdown = _load(out)
    nulls = report["checks"][2]
    assert nulls["status"] == "mismatch"
    assert nulls["details"] == {"keys": ["txn_id", "day"], "source": 1, "target": 0}
    assert "Source rows with a null key: 1." in markdown


def test_duplicate_keys_are_counted_and_examples_are_sorted(tmp_path):
    spec, _source, _target = _pair(
        tmp_path,
        "b,1.00,2024-01-01,a\nb,1.00,2024-01-01,b\na,1.00,2024-01-01,c\na,1.00,2024-01-01,d\n",
        "a,2.00,2024-01-01,a\nb,2.00,2024-01-01,b\n",
    )
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    details = _load(out)[0]["checks"][3]["details"]
    assert details["example_limit"] == DUPLICATE_EXAMPLE_LIMIT == 20
    assert details["source"] == {"count": 2, "examples": [["a"], ["b"]]}
    assert details["target"] == {"count": 0, "examples": []}


def test_duplicate_examples_stop_at_the_limit(tmp_path):
    source_rows = []
    for index in range(DUPLICATE_EXAMPLE_LIMIT + 1):
        key = f"{index:02d}"
        source_rows.append(f"{key},1.00,2024-01-01,a\n")
        source_rows.append(f"{key},1.00,2024-01-01,b\n")
    spec, _source, _target = _pair(tmp_path, "".join(source_rows), ROW)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    details = _load(out)[0]["checks"][3]["details"]["source"]
    assert details["count"] == DUPLICATE_EXAMPLE_LIMIT + 1
    examples = [item[0] for item in details["examples"]]
    assert examples == [f"{index:02d}" for index in range(DUPLICATE_EXAMPLE_LIMIT)]
    assert "20" not in examples


def test_null_amount_is_not_a_zero_sum(tmp_path):
    spec, _source, _target = _pair(tmp_path, "1,,2024-01-01,a\n", "1,0.00,2024-01-01,a\n")
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    amount = _load(out)[0]["checks"][1]["details"]["columns"][0]
    assert amount["source"] is None
    assert amount["target"] == "0.00"
    assert amount["difference"] is None
    assert amount["status"] == "mismatch"


def test_empty_datasets_match_with_null_sums(tmp_path):
    spec, _source, _target = _pair(tmp_path, "", "")
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MATCH
    report, _raw, markdown = _load(out)
    assert report["row_counts"] == {"source": 0, "target": 0}
    amount = report["checks"][1]["details"]["columns"][0]
    assert amount["source"] is None
    assert amount["target"] is None
    assert amount["status"] == "match"
    assert "No groups were present on either side." in markdown
    assert "Source sum: none." in markdown


def test_second_amount_column_is_checked(tmp_path):
    header = "txn_id,amount,fee,day,note\n"
    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    write_csv(source, "1,10.00,1.00,2024-01-01,a\n", header)
    write_csv(target, "1,10.00,1.50,2024-01-01,a\n", header)
    data = spec_data(tmp_path, source, target, target_format="csv")
    data["amounts"] = [
        {"column": "amount", "scale": 2},
        {"column": "fee", "scale": 2},
    ]
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    columns = _load(out)[0]["checks"][1]["details"]["columns"]
    assert [column["column"] for column in columns] == ["amount", "fee"]
    assert columns[0]["status"] == "match"
    assert columns[1]["difference"] == "0.50"
    assert columns[1]["status"] == "mismatch"


def test_refused_spec_writes_a_refused_report(tmp_path, capsys):
    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    write_csv(source, ROW)
    write_csv(target, ROW)
    data = spec_data(tmp_path, source, target, target_format="csv")
    data["keys"] = []
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    code = _invoke(spec, out)
    captured = capsys.readouterr()
    _assert_refused_line(code, captured, "missing-key: the spec has no business key")
    assert _names(out) == ["report.json", "report.md"]
    report, raw, markdown = _load(out)
    _assert_canonical(raw)
    _assert_prose(markdown)
    _assert_report_shape(report, spec, "refused")
    assert report["checks"] == []
    assert report["row_counts"] == {"source": None, "target": None}
    assert report["refusals"] == [
        {"code": "missing-key", "message": "the spec has no business key"}
    ]
    assert "Status: Refused" in markdown
    assert "No checks were run." in markdown

    other = tmp_path / "other"
    assert _invoke(spec, other) is ExitCode.REFUSED
    assert (out / "report.json").read_bytes() == (other / "report.json").read_bytes()
    assert (out / "report.md").read_bytes() == (other / "report.md").read_bytes()


def test_inexact_amount_writes_a_refused_report(tmp_path, capsys):
    spec, _source, _target = _pair(tmp_path, "1,10.005,2024-01-01,a\n", ROW)
    out = tmp_path / "out"
    code = _invoke(spec, out)
    captured = capsys.readouterr()
    _assert_refused_line(
        code,
        captured,
        "inexact-amount: source amount column 'amount' value '10.005' "
        "is not an exact decimal at scale 2",
    )
    report, _raw, _markdown = _load(out)
    assert report["status"] == "refused"
    assert report["refusals"][0]["code"] == "inexact-amount"
    assert report["checks"] == []


def test_sample_only_writes_a_refused_report_without_spark(monkeypatch, tmp_path, capsys):
    def fail(*_args, **_kwargs):
        raise AssertionError("spark started")

    monkeypatch.setattr("qa_core_recon.spark.local_session", fail)
    source = tmp_path / "missing-source.csv"
    target = tmp_path / "missing-target.csv"
    data = spec_data(tmp_path, source, target, target_format="csv")
    data["sampled"] = True
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    code = _invoke(spec, out)
    captured = capsys.readouterr()
    _assert_refused_line(
        code,
        captured,
        "sample-only: the spec declares sampling, so a full recon cannot be claimed",
    )
    report, _raw, _markdown = _load(out)
    assert report["status"] == "refused"
    assert report["refusals"][0]["code"] == "sample-only"


def test_s3_input_is_a_read_error_without_a_report(tmp_path, capsys):
    source = tmp_path / "source.csv"
    write_csv(source, ROW)
    data = spec_data(tmp_path, source, tmp_path / "unused.csv", target_format="csv")
    data["target"] = {"path": "s3://demo-bucket/curated/payments", "format": "parquet"}
    data["allowlist"] = [str(tmp_path), "s3://demo-bucket"]
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    code = _invoke(spec, out)
    captured = capsys.readouterr()
    _assert_error(
        code,
        captured,
        "s3 reads are not supported yet: target path 's3://demo-bucket/curated/payments'",
    )
    assert not out.exists()


def test_missing_dataset_is_a_read_error_without_a_report(tmp_path, capsys):
    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    write_csv(source, ROW)
    data = spec_data(tmp_path, source, target, target_format="csv")
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    code = _invoke(spec, out)
    captured = capsys.readouterr()
    assert code is ExitCode.REFUSED
    assert captured.out == ""
    assert "target path not found:" in captured.err
    assert "Traceback" not in captured.err
    assert not out.exists()


def test_malformed_spec_writes_no_report(tmp_path, capsys):
    spec = tmp_path / "recon.yaml"
    spec.write_text("version: 1\n", encoding="utf-8")
    out = tmp_path / "out"
    code = main(["run", "--spec", str(spec), "--out", str(out)])
    captured = capsys.readouterr()
    assert code is ExitCode.REFUSED
    assert captured.out == ""
    assert captured.err.startswith("qa-core-recon: error:")
    assert "Traceback" not in captured.err
    assert not out.exists()


def test_s3_output_is_rejected(tmp_path, capsys):
    spec, _source, _target = _pair(tmp_path, ROW, ROW)
    code = main(["run", "--spec", str(spec), "--out", "s3://bucket/reports"])
    captured = capsys.readouterr()
    _assert_error(code, captured, "s3 output is not supported: s3://bucket/reports")


def test_non_empty_output_without_force_is_rejected(tmp_path, capsys):
    spec, _source, _target = _pair(tmp_path, ROW, ROW)
    out = tmp_path / "out"
    out.mkdir()
    kept = out / "notes.txt"
    kept.write_text("keep\n", encoding="utf-8")
    code = _invoke(spec, out)
    captured = capsys.readouterr()
    _assert_error(code, captured, f"{out} is not empty; use --force to overwrite")
    assert kept.read_text(encoding="utf-8") == "keep\n"
    assert not (out / "report.json").exists()
    assert not (out / "report.md").exists()


def test_non_empty_output_with_force_keeps_other_files(tmp_path):
    spec, _source, _target = _pair(tmp_path, ROW, ROW)
    out = tmp_path / "out"
    out.mkdir()
    kept = out / "notes.txt"
    kept.write_text("keep\n", encoding="utf-8")
    assert _invoke(spec, out, force=True) is ExitCode.MATCH
    assert kept.read_text(encoding="utf-8") == "keep\n"
    assert (out / "report.json").is_file()
    assert (out / "report.md").is_file()
    assert not any(name.endswith(".tmp") for name in _names(out))


def test_output_parent_must_exist_and_a_file_is_rejected(tmp_path, capsys):
    spec, _source, _target = _pair(tmp_path, ROW, ROW)
    missing = tmp_path / "missing" / "out"
    code = _invoke(spec, missing)
    captured = capsys.readouterr()
    _assert_error(code, captured, f"parent directory does not exist: {missing.parent}")
    assert not (tmp_path / "missing").exists()

    target = tmp_path / "file-out"
    target.write_text("x", encoding="utf-8")
    code = _invoke(spec, target)
    captured = capsys.readouterr()
    _assert_error(code, captured, f"{target} is a file")
    assert target.read_text(encoding="utf-8") == "x"


def test_sum_overflow_is_a_read_error(tmp_path, capsys):
    big = "9" * 36 + ".99"
    body = f"1,{big},2024-01-01,a\n2,0.01,2024-01-01,b\n"
    spec, _source, _target = _pair(tmp_path, body, body, group_by=[])
    out = tmp_path / "out"
    code = _invoke(spec, out)
    captured = capsys.readouterr()
    _assert_error(
        code,
        captured,
        "source amount column 'amount' sum does not fit decimal(38, 2)",
    )
    assert not out.exists()


def test_group_sum_overflow_is_a_read_error_even_when_the_total_cancels(tmp_path, capsys):
    big = "9" * 36 + ".99"
    negative = "-" + big
    body = f"1,{big},a,n\n2,{big},a,n\n3,{negative},b,n\n4,{negative},b,n\n"
    spec, _source, _target = _pair(tmp_path, body, body)
    out = tmp_path / "out"
    code = _invoke(spec, out)
    captured = capsys.readouterr()
    _assert_error(
        code,
        captured,
        "source amount column 'amount' group sum does not fit decimal(38, 2)",
    )
    assert not out.exists()


def test_session_start_error_writes_no_report(monkeypatch, tmp_path, capsys):
    def boom(*_args, **_kwargs):
        raise SessionStartError("cannot start the local Spark session: boom")

    monkeypatch.setattr("qa_core_recon.spark.local_session", boom)
    monkeypatch.setattr("qa_core_recon.spark.local_session_active", lambda: False)
    spec, _source, _target = _pair(tmp_path, ROW, ROW)
    out = tmp_path / "out"
    code = _invoke(spec, out)
    captured = capsys.readouterr()
    _assert_error(code, captured, "cannot start the local Spark session: boom")
    assert not out.exists()


def test_missing_iceberg_runtime_writes_no_report(monkeypatch, tmp_path, capsys):
    def boom(*_args, **_kwargs):
        raise IcebergRuntimeError("the Iceberg runtime example")

    monkeypatch.setattr("qa_core_recon.spark.local_session", boom)
    monkeypatch.setattr("qa_core_recon.spark.local_session_active", lambda: False)
    spec, _source, _target = _pair(tmp_path, ROW, ROW)
    out = tmp_path / "out"
    code = _invoke(spec, out)
    captured = capsys.readouterr()
    _assert_error(code, captured, "the Iceberg runtime example")
    assert not out.exists()


def test_exit_codes_through_main(tmp_path):
    match, _source, _target = _pair(tmp_path, ROW, ROW)
    assert main(["run", "--spec", str(match), "--out", str(tmp_path / "match")]) is ExitCode.MATCH

    mismatch_dir = tmp_path / "mismatch-spec"
    mismatch_dir.mkdir()
    mismatch, _source, _target = _pair(
        mismatch_dir,
        ROW,
        "1,10.51,2024-01-01,a\n",
    )
    assert (
        main(["run", "--spec", str(mismatch), "--out", str(tmp_path / "mismatch")])
        is ExitCode.MISMATCH
    )

    refused_dir = tmp_path / "refused-spec"
    refused_dir.mkdir()
    source = refused_dir / "source.csv"
    target = refused_dir / "target.csv"
    write_csv(source, ROW)
    write_csv(target, ROW)
    data = spec_data(refused_dir, source, target, target_format="csv")
    data["keys"] = []
    refused = write_spec_file(refused_dir, data)
    assert (
        main(["run", "--spec", str(refused), "--out", str(tmp_path / "refused")])
        is ExitCode.REFUSED
    )


def test_nan_groups_match_across_repartitions(spark, tmp_path):
    schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("amount", DecimalType(10, 2), True),
            StructField("bucket", DoubleType(), True),
            StructField("note", StringType(), True),
        ]
    )
    nan = float("nan")
    source_rows = [
        ("1", Decimal("1.00"), nan, "a"),
        ("2", Decimal("2.00"), nan, "b"),
        ("3", Decimal("3.00"), -0.0, "c"),
    ]
    target_rows = [
        ("1", Decimal("1.00"), nan, "a"),
        ("2", Decimal("2.00"), nan, "b"),
        ("3", Decimal("3.00"), 0.0, "c"),
    ]
    source = tmp_path / "source"
    target = tmp_path / "target"
    spark.createDataFrame(source_rows, schema).repartition(2).write.mode("overwrite").parquet(
        str(source)
    )
    spark.createDataFrame(target_rows, schema).repartition(4).write.mode("overwrite").parquet(
        str(target)
    )
    data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
    data["group_by"] = ["bucket"]
    spec = write_spec_file(tmp_path, data)
    first = tmp_path / "first"
    second = tmp_path / "second"
    assert _invoke(spec, first) is ExitCode.MATCH
    assert _invoke(spec, second) is ExitCode.MATCH
    assert (first / "report.json").read_bytes() == (second / "report.json").read_bytes()
    assert (first / "report.md").read_bytes() == (second / "report.md").read_bytes()
    groups = _load(first)[0]["checks"][0]["details"]["groups"]
    assert [group["group"]["bucket"] for group in groups] == ["0.0", "NaN"]
    assert [group["status"] for group in groups] == ["match", "match"]
    assert groups[1]["source"] == 2
    assert groups[1]["target"] == 2


def test_force_read_error_removes_the_previous_report(tmp_path, capsys):
    spec, source, _target = _pair(tmp_path, ROW, ROW)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MATCH
    source.unlink()
    code = _invoke(spec, out, force=True)
    captured = capsys.readouterr()
    assert code is ExitCode.REFUSED
    assert "path not found" in captured.err
    assert "unexpected error" not in captured.err
    assert not (out / "report.json").exists()
    assert not (out / "report.md").exists()


def test_force_overflow_removes_the_previous_report(tmp_path, capsys):
    spec, source, target = _pair(tmp_path, ROW, ROW)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MATCH
    big = "9" * 36 + ".99"
    body = f"1,{big},2024-01-01,a\n2,0.01,2024-01-01,b\n"
    write_csv(source, body)
    write_csv(target, body)
    code = _invoke(spec, out, force=True)
    captured = capsys.readouterr()
    assert code is ExitCode.REFUSED
    assert "does not fit decimal(38, 2)" in captured.err
    assert not (out / "report.json").exists()
    assert not (out / "report.md").exists()


def test_failed_rename_leaves_neither_report(monkeypatch, tmp_path, capsys):
    original_replace = os.replace

    def fail_on_markdown(src, dst):
        if os.path.basename(os.fspath(dst)) == "report.md":
            raise OSError(28, "No space left on device")
        return original_replace(src, dst)

    monkeypatch.setattr(os, "replace", fail_on_markdown)
    spec, _source, _target = _pair(tmp_path, ROW, ROW)
    out = tmp_path / "out"
    code = _invoke(spec, out)
    captured = capsys.readouterr()
    assert code is ExitCode.REFUSED
    assert captured.out == ""
    assert "unexpected error" not in captured.err
    assert not (out / "report.json").exists()
    assert not (out / "report.md").exists()


def test_group_sums_outside_tolerance_mismatch_when_the_total_matches(tmp_path):
    spec, _source, _target = _pair(
        tmp_path,
        "1,10.00,a,n\n2,10.00,b,n\n",
        "1,10.02,a,n\n2,9.98,b,n\n",
        tolerance={"value": "0.01", "reason": "penny rounding"},
    )
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    amount = _load(out)[0]["checks"][1]["details"]["columns"][0]
    assert amount["difference"] == "0.00"
    assert [group["group"]["day"] for group in amount["groups"]] == ["a", "b"]
    assert [group["difference"] for group in amount["groups"]] == ["0.02", "0.02"]
    assert [group["status"] for group in amount["groups"]] == ["mismatch", "mismatch"]
    assert amount["status"] == "mismatch"


def test_sum_wider_than_28_digits_keeps_the_cent(tmp_path):
    big = "100000000000000000000000000000.01"
    spec, _source, _target = _pair(
        tmp_path,
        f"1,{big},2024-01-01,a\n2,0.01,2024-01-01,b\n",
        f"1,{big},2024-01-01,a\n2,0.02,2024-01-01,b\n",
    )
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    amount = _load(out)[0]["checks"][1]["details"]["columns"][0]
    assert amount["source"] == "100000000000000000000000000000.02"
    assert amount["target"] == "100000000000000000000000000000.03"
    assert amount["difference"] == "0.01"


def test_output_must_not_overlap_a_local_dataset(tmp_path, capsys):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    write_csv(source_dir / "part.csv", ROW)
    target = tmp_path / "target.csv"
    write_csv(target, ROW)
    data = spec_data(tmp_path, source_dir, target, source_format="csv", target_format="csv")
    spec = write_spec_file(tmp_path, data)
    code = main(["run", "--spec", str(spec), "--out", str(source_dir), "--force"])
    captured = capsys.readouterr()
    _assert_error(code, captured, f"output path {source_dir} overlaps source path {source_dir}")
    assert (source_dir / "part.csv").is_file()
    assert not (source_dir / "report.json").exists()

    nested = source_dir / "nested"
    code = main(["run", "--spec", str(spec), "--out", str(nested)])
    captured = capsys.readouterr()
    _assert_error(code, captured, f"output path {nested} overlaps source path {source_dir}")
    assert not nested.exists()

    parent = tmp_path / "holder"
    parent.mkdir()
    held = parent / "payments.csv"
    write_csv(held, ROW)
    other = parent / "other.csv"
    write_csv(other, ROW)
    held_spec = write_spec_file(
        parent,
        spec_data(parent, held, other, source_format="csv", target_format="csv"),
    )
    # write_spec_file always uses tmp_path name recon.yaml inside parent, and the
    # allowlist is parent, which contains the datasets. out == parent contains them.
    code = main(["run", "--spec", str(held_spec), "--out", str(parent), "--force"])
    captured = capsys.readouterr()
    _assert_error(code, captured, f"output path {parent} overlaps source path {held}")
    assert held.is_file()
    assert not (parent / "report.json").exists()


def test_leftover_report_temps_do_not_block_a_run(tmp_path):
    spec, _source, _target = _pair(tmp_path, ROW, ROW)
    out = tmp_path / "out"
    out.mkdir()
    leftover = out / ".report.json.999.tmp"
    leftover.write_text("partial", encoding="utf-8")
    (out / ".report.md.999.tmp").write_text("partial", encoding="utf-8")
    assert _invoke(spec, out) is ExitCode.MATCH
    assert not leftover.exists()
    assert (out / "report.json").is_file()
    assert (out / "report.md").is_file()


def test_output_overlap_follows_symlinks_and_parent_segments(tmp_path, capsys):
    root = tmp_path / "data"
    source_dir = root / "src"
    source_dir.mkdir(parents=True)
    part = source_dir / "part.csv"
    write_csv(part, ROW)
    target = root / "target.csv"
    write_csv(target, ROW)
    spec = write_spec_file(
        root,
        spec_data(root, source_dir, target, source_format="csv", target_format="csv"),
    )
    link = root / "link"
    link.symlink_to(source_dir, target_is_directory=True)
    code = main(["run", "--spec", str(spec), "--out", str(link), "--force"])
    captured = capsys.readouterr()
    _assert_error(code, captured, f"output path {link} overlaps source path {source_dir}")
    assert part.is_file()
    assert not (source_dir / "report.json").exists()
    assert not (source_dir / "report.md").exists()

    (root / "other").mkdir()
    dotted = root / "other" / ".." / "src"
    assert ".." in dotted.parts
    code = main(["run", "--spec", str(spec), "--out", str(dotted), "--force"])
    captured = capsys.readouterr()
    _assert_error(code, captured, f"output path {dotted} overlaps source path {source_dir}")
    assert part.read_text(encoding="utf-8").startswith("txn_id")
    assert not (source_dir / "report.json").exists()


def test_overlap_does_not_delete_a_data_file_named_report_json(tmp_path, capsys):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    data_file = source_dir / "report.json"
    write_csv(data_file, ROW)
    original = data_file.read_bytes()
    target = tmp_path / "target.csv"
    write_csv(target, ROW)
    spec = write_spec_file(
        tmp_path,
        spec_data(tmp_path, source_dir, target, source_format="csv", target_format="csv"),
    )
    code = main(["run", "--spec", str(spec), "--out", str(source_dir), "--force"])
    captured = capsys.readouterr()
    _assert_error(code, captured, f"output path {source_dir} overlaps source path {source_dir}")
    assert data_file.read_bytes() == original


def test_malformed_spec_leaves_the_output_directory_untouched(tmp_path, capsys):
    spec = tmp_path / "recon.yaml"
    spec.write_text("version: 1\n", encoding="utf-8")
    out = tmp_path / "out"
    out.mkdir()
    report = out / "report.json"
    report.write_text("keep\n", encoding="utf-8")
    notes = out / "notes.txt"
    notes.write_text("notes\n", encoding="utf-8")
    code = main(["run", "--spec", str(spec), "--out", str(out), "--force"])
    captured = capsys.readouterr()
    assert code is ExitCode.REFUSED
    assert captured.err.startswith("qa-core-recon: error:")
    assert "unexpected error" not in captured.err
    assert report.read_text(encoding="utf-8") == "keep\n"
    assert notes.read_text(encoding="utf-8") == "notes\n"


def test_spec_refusal_does_not_write_into_an_overlapping_output(tmp_path, capsys):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    part = source_dir / "part.csv"
    write_csv(part, ROW)
    target = tmp_path / "target.csv"
    write_csv(target, ROW)
    data = spec_data(tmp_path, source_dir, target, source_format="csv", target_format="csv")
    data["tolerance"] = {"value": "0.01", "reason": ""}
    spec = write_spec_file(tmp_path, data)
    code = main(["run", "--spec", str(spec), "--out", str(source_dir), "--force"])
    captured = capsys.readouterr()
    _assert_error(code, captured, f"output path {source_dir} overlaps source path {source_dir}")
    assert part.is_file()
    assert not (source_dir / "report.json").exists()
    assert not (source_dir / "report.md").exists()


def test_lookalike_temp_names_and_symlinks_are_kept(tmp_path, capsys):
    spec, _source, _target = _pair(tmp_path, ROW, ROW)
    out = tmp_path / "out"
    out.mkdir()
    notes = out / ".report.notes.tmp"
    notes.write_text("notes\n", encoding="utf-8")
    kept = tmp_path / "kept.txt"
    kept.write_text("target\n", encoding="utf-8")
    link = out / ".report.json.123.tmp"
    link.symlink_to(kept)
    real_temp = out / ".report.json.5.tmp"
    real_temp.write_text("partial\n", encoding="utf-8")
    code = _invoke(spec, out)
    captured = capsys.readouterr()
    _assert_error(code, captured, f"{out} is not empty; use --force to overwrite")
    assert not real_temp.exists()
    assert notes.read_text(encoding="utf-8") == "notes\n"
    assert link.is_symlink()
    assert kept.read_text(encoding="utf-8") == "target\n"
    assert not (out / "report.json").exists()


def test_unlinking_a_report_directory_is_an_output_error(tmp_path, capsys):
    spec, _source, _target = _pair(tmp_path, ROW, ROW)
    out = tmp_path / "out"
    out.mkdir()
    report_dir = out / "report.md"
    report_dir.mkdir()
    code = _invoke(spec, out, force=True)
    captured = capsys.readouterr()
    _assert_error(code, captured, f"cannot write {out}: Is a directory")
    assert report_dir.is_dir()


def test_symlink_loop_is_a_read_error(tmp_path, capsys):
    target = tmp_path / "target.csv"
    write_csv(target, ROW)
    source_loop = tmp_path / "source-loop"
    source_loop.symlink_to(source_loop)
    spec = write_spec_file(
        tmp_path,
        spec_data(tmp_path, source_loop, target, source_format="csv", target_format="csv"),
    )
    out = tmp_path / "out"
    code = _invoke(spec, out)
    captured = capsys.readouterr()
    _assert_error(
        code,
        captured,
        f"cannot read source path '{source_loop}': Symlink loop from '{source_loop}'",
    )
    assert not (out / "report.json").exists()
    assert not (out / "report.md").exists()

    source = tmp_path / "source.csv"
    write_csv(source, ROW)
    left = tmp_path / "left"
    right = tmp_path / "right"
    left.symlink_to("right")
    right.symlink_to("left")
    spec = write_spec_file(
        tmp_path,
        spec_data(tmp_path, source, left, source_format="csv", target_format="csv"),
    )
    code = _invoke(spec, out)
    captured = capsys.readouterr()
    _assert_error(
        code,
        captured,
        f"cannot read target path '{left}': Symlink loop from '{left}'",
    )
    assert not (out / "report.json").exists()


def test_csv_session_start_failure_is_a_read_error(monkeypatch, tmp_path, capsys):
    import qa_core_recon.spark as spark_mod

    monkeypatch.setattr(spark_mod, "_session", None)
    monkeypatch.setattr(spark_mod, "_warehouse", None)
    monkeypatch.setattr(spark_mod, "_iceberg_ready", False)

    def fail(_builder):
        raise RuntimeError("JAVA_GATEWAY_EXITED")

    monkeypatch.setattr(spark_mod, "_start_quietly", fail)
    spec, _source, _target = _pair(tmp_path, ROW, ROW)
    out = tmp_path / "out"
    code = main(["run", "--spec", str(spec), "--out", str(out)])
    captured = capsys.readouterr()
    _assert_error(code, captured, "cannot start the local Spark session: JAVA_GATEWAY_EXITED")
    assert not (out / "report.json").exists()

    code = main(["validate", "--spec", str(spec), "--check-data"])
    captured = capsys.readouterr()
    _assert_error(code, captured, "cannot start the local Spark session: JAVA_GATEWAY_EXITED")
