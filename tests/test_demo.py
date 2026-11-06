"""Synthetic demo fixtures and the committed reports.

The generator uses the standard library and a fixed seed. One test reruns it
into a temp directory and compares those bytes with ``examples/demo/data``.

Each spec is run through the CLI. ``match``, the Iceberg spec, and the S3
spec exit 0. The other cases exit 1. Reports are compared byte for byte with
``examples/demo/expected``.

The report checks are row count, amount sum, null keys, duplicate keys, key
diff, and changed columns. Missing, extra, and changed txn_id values appear
in the key-diff and changed-column sections.
"""

import ast
import csv
import json
import os
import shutil
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from qa_core_recon.cli import main
from qa_core_recon.exit_codes import ExitCode

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "examples" / "demo"
DATA = DEMO / "data"
SPECS = DEMO / "specs"
EXPECTED = DEMO / "expected"
GENERATOR = DEMO / "generate.py"
WAREHOUSE = "examples/demo/data/iceberg/warehouse"
BUCKET = "demo-bucket"
S3_PREFIX = "payments/match"
REPORT_CHECKS = [
    "row_count",
    "amount_sum",
    "null_keys",
    "duplicate_keys",
    "key_diff",
    "changed_columns",
]
COLUMNS = ["txn_id", "account_id", "txn_type", "amount", "currency", "txn_date"]
LOCAL_CASES = ("match", "missing-rows", "extra-rows", "changed-rows", "sum-mismatch")
EXIT_CODES = {
    "match": ExitCode.MATCH,
    "missing-rows": ExitCode.MISMATCH,
    "extra-rows": ExitCode.MISMATCH,
    "changed-rows": ExitCode.MISMATCH,
    "sum-mismatch": ExitCode.MISMATCH,
    "iceberg": ExitCode.MATCH,
    "s3": ExitCode.MATCH,
}


def _tree(root: Path) -> dict[str, bytes]:
    found = {}
    for path in root.rglob("*"):
        if path.is_file():
            found[path.relative_to(root).as_posix()] = path.read_bytes()
    return found


def test_generator_imports_only_the_standard_library():
    tree = ast.parse(GENERATOR.read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            imported.add(node.module.split(".", 1)[0])
    third_party = sorted(name for name in imported if name not in sys.stdlib_module_names)
    assert third_party == []


def test_generator_matches_the_committed_fixture(tmp_path):
    result = subprocess.run(
        [sys.executable, str(GENERATOR), "--out", str(tmp_path)],
        check=False,
        capture_output=True,
    )
    assert result.returncode == 0
    assert result.stdout == b""
    assert result.stderr == b""
    assert _tree(tmp_path) == _tree(DATA)
    assert list(tmp_path.rglob("*.parquet")) == []


def test_committed_csv_is_small_and_exact():
    paths = sorted(DATA.rglob("*.csv"))
    assert paths
    assert all(path.stat().st_size < 100_000 for path in paths)
    source = (DATA / "match" / "source.csv").read_bytes()
    counts = {}
    for path in paths:
        text = path.read_text(encoding="utf-8")
        assert text.endswith("\n")
        assert "\r" not in text
        rows = list(csv.DictReader(text.splitlines()))
        assert rows
        assert list(rows[0]) == COLUMNS
        for row in rows:
            assert row["amount"][:1] != "+"
            whole, fraction = row["amount"].lstrip("-").split(".")
            assert whole.isdigit()
            assert len(fraction) == 2 and fraction.isdigit()
        counts[path.relative_to(DATA).as_posix()] = len(rows)
        if path.name == "source.csv":
            assert path.read_bytes() == source
    assert counts["match/source.csv"] == 200
    assert counts["match/target.csv"] == 200
    assert counts["missing-rows/target.csv"] == 160
    assert counts["extra-rows/target.csv"] == 203
    assert counts["changed-rows/target.csv"] == 200
    assert counts["sum-mismatch/target.csv"] == 200


def _assert_report(out: Path, case: str, code, captured) -> None:
    expected_code = EXIT_CODES[case]
    assert code == expected_code
    assert int(code) == (0 if expected_code is ExitCode.MATCH else 1)
    assert captured.err == ""
    assert "Traceback" not in captured.out
    status = "match" if expected_code is ExitCode.MATCH else "mismatch"
    assert captured.out == f"{status}\n"
    expected = EXPECTED / case
    assert (out / "report.json").read_bytes() == (expected / "report.json").read_bytes()
    assert (out / "report.md").read_bytes() == (expected / "report.md").read_bytes()
    report = json.loads((out / "report.json").read_text(encoding="utf-8"))
    assert [item["name"] for item in report["checks"]] == REPORT_CHECKS
    by_name = {item["name"]: item for item in report["checks"]}
    if case in {"missing-rows", "extra-rows"}:
        assert by_name["row_count"]["status"] == "mismatch"
        assert by_name["amount_sum"]["status"] == "mismatch"
        assert by_name["key_diff"]["status"] == "mismatch"
        assert by_name["changed_columns"]["status"] == "match"
    elif case in {"changed-rows", "sum-mismatch"}:
        assert by_name["row_count"]["status"] == "match"
        assert by_name["amount_sum"]["status"] == "mismatch"
        assert by_name["key_diff"]["status"] == "mismatch"
        assert by_name["changed_columns"]["status"] == "mismatch"
    elif case in {"match", "iceberg", "s3"}:
        assert all(item["status"] == "match" for item in report["checks"])


def _run(case: str, out: Path):
    return main(["run", "--spec", str(SPECS / f"{case}.yaml"), "--out", str(out)])


@pytest.mark.parametrize("case", LOCAL_CASES)
def test_cli_report_matches_committed_bytes(case, monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(ROOT)
    out = tmp_path / "out"
    code = _run(case, out)
    _assert_report(out, case, code, capsys.readouterr())


def _write_iceberg(spark) -> None:
    from qa_core_recon.spark import hadoop_catalog

    rows = []
    with (DATA / "match" / "target.csv").open(encoding="utf-8", newline="") as handle:
        for record in csv.DictReader(handle):
            rows.append(
                (
                    record["txn_id"],
                    record["account_id"],
                    record["txn_type"],
                    Decimal(record["amount"]),
                    record["currency"],
                    record["txn_date"],
                )
            )
    schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("txn_type", StringType(), True),
            StructField("amount", DecimalType(38, 2), True),
            StructField("currency", StringType(), True),
            StructField("txn_date", StringType(), True),
        ]
    )
    warehouse = ROOT / WAREHOUSE
    warehouse.mkdir(parents=True, exist_ok=True)
    # The spec stores this relative path. Spark resolves it from the JVM
    # working directory, which is the directory pytest was started from.
    catalog = hadoop_catalog(spark, WAREHOUSE)
    spark.createDataFrame(rows, schema).writeTo(f"{catalog}.payments.txn").createOrReplace()


def test_iceberg_spec_matches_committed_report(spark, monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(ROOT)
    shutil.rmtree(ROOT / "examples" / "demo" / "data" / "iceberg", ignore_errors=True)
    try:
        _write_iceberg(spark)
        out = tmp_path / "out"
        code = _run("iceberg", out)
        _assert_report(out, "iceberg", code, capsys.readouterr())
    finally:
        shutil.rmtree(ROOT / "examples" / "demo" / "data" / "iceberg", ignore_errors=True)


def _s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.environ["QA_CORE_RECON_S3_ENDPOINT"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def _wipe_demo_bucket() -> None:
    from botocore.exceptions import ClientError

    client = _s3_client()
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET):
            for item in page.get("Contents", []):
                client.delete_object(Bucket=BUCKET, Key=item["Key"])
        client.delete_bucket(Bucket=BUCKET)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in {"NoSuchBucket", "404"}:
            raise


@pytest.fixture
def demo_bucket():
    """Create the demo bucket for one test, then remove it.

    Other tests expect ``s3://demo-bucket`` to be missing. Leaving the bucket
    in moto would turn those read errors into successful reads.
    """
    _wipe_demo_bucket()
    _s3_client().create_bucket(Bucket=BUCKET)
    try:
        yield BUCKET
    finally:
        _wipe_demo_bucket()


def _upload_match(bucket: str) -> None:
    client = _s3_client()
    for name in ("source.csv", "target.csv"):
        client.put_object(
            Bucket=bucket,
            Key=f"{S3_PREFIX}/{name}",
            Body=(DATA / "match" / name).read_bytes(),
        )


def test_s3_spec_matches_committed_report(demo_bucket, monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(ROOT)
    _upload_match(demo_bucket)
    out = tmp_path / "out"
    code = _run("s3", out)
    _assert_report(out, "s3", code, capsys.readouterr())
