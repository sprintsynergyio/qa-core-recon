"""Local Iceberg reads. Tables are written in tmp_path. No binary is committed."""

import json
import subprocess
import sys
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql.types import DecimalType, DoubleType, StringType, StructField, StructType

from qa_core_recon.cli import main
from qa_core_recon.exit_codes import ExitCode
from qa_core_recon.readers import ReadError, check_data, read_datasets
from qa_core_recon.refusals import RefusalCode, RefusalError
from qa_core_recon.spark import catalog_name_for
from qa_core_recon.spec import Dataset, SpecError
from tests.support import parse_data, spec_data, write_csv, write_spec_file

ROW = "1,10.50,2024-01-01,a\n"
TABLE = "payments.txn"


def _schema(amount_type, names=("txn_id", "amount", "day", "note")):
    types = {
        "txn_id": StringType(),
        "amount": amount_type,
        "day": StringType(),
        "note": StringType(),
    }
    return StructType([StructField(name, types[name], True) for name in names])


def _write_iceberg(spark, warehouse: Path, table: str, rows, schema, *, mode: str) -> Path:
    """Create or append a table in a Hadoop catalog rooted at ``warehouse``."""
    from qa_core_recon.spark import hadoop_catalog

    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = hadoop_catalog(spark, str(warehouse))
    writer = spark.createDataFrame(rows, schema).writeTo(f"{catalog}.{table}")
    if mode == "append":
        writer.append()
    else:
        writer.createOrReplace()
    return warehouse.joinpath(*table.split("."))


def _current_snapshot_id(table_dir: Path) -> int:
    metadata = table_dir / "metadata"
    hint = metadata / "version-hint.text"
    if hint.exists():
        version = hint.read_text(encoding="utf-8").strip()
        payload = json.loads((metadata / f"v{version}.metadata.json").read_text(encoding="utf-8"))
        return int(payload["current-snapshot-id"])
    newest = None
    newest_stamp = -1
    for path in metadata.glob("*.metadata.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        stamp = int(payload.get("last-updated-ms") or 0)
        if stamp >= newest_stamp:
            newest = payload
            newest_stamp = stamp
    if newest is None or newest.get("current-snapshot-id") is None:
        raise AssertionError(f"no Iceberg snapshot metadata in {metadata}")
    return int(newest["current-snapshot-id"])


def _amounts(frame):
    return {row["txn_id"]: row["amount"] for row in frame.select("txn_id", "amount").collect()}


def _refusal(spark, spec):
    with pytest.raises(RefusalError) as caught:
        check_data(spark, spec)
    return caught.value.refusal


def _target(tmp_path, source, warehouse, **kwargs):
    table = kwargs.pop("table", TABLE)
    return spec_data(
        tmp_path,
        source,
        warehouse,
        target_format="iceberg",
        target_table=table,
        **kwargs,
    )


def _base(**changes) -> dict:
    data = {
        "version": 1,
        "source": {"path": "./data/warehouse", "format": "iceberg", "table": TABLE},
        "target": {"path": "./data/raw/payments.csv", "format": "csv"},
        "keys": ["txn_id"],
        "amounts": [{"column": "amount", "scale": 2}],
        "group_by": [],
        "hash_columns": [],
        "tolerance": {"value": "0", "reason": ""},
        "sampled": False,
        "allowlist": ["./data"],
    }
    data["source"].update(changes)
    return data


def _error(data: dict) -> str:
    with pytest.raises(SpecError) as caught:
        parse_data(data)
    return str(caught.value)


def test_iceberg_decimal_is_exact_and_extra_scale_is_refused(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, ROW)
    warehouse = tmp_path / "warehouse"
    _write_iceberg(
        spark,
        warehouse,
        TABLE,
        [("1", Decimal("10.50"), "2024-01-01", "a")],
        _schema(DecimalType(10, 2)),
        mode="overwrite",
    )
    spec = parse_data(_target(tmp_path, source, warehouse))
    _source, target = read_datasets(spark, spec)
    assert target.schema["amount"].dataType == DecimalType(38, 2)
    assert _amounts(target)["1"] == Decimal("10.50")
    name = catalog_name_for(str(warehouse))
    assert spark.conf.get(f"spark.sql.catalog.{name}") == "org.apache.iceberg.spark.SparkCatalog"
    assert spark.conf.get(f"spark.sql.catalog.{name}.type") == "hadoop"
    assert spark.conf.get(f"spark.sql.catalog.{name}.warehouse") == str(warehouse)
    assert spark.conf.get(f"spark.sql.catalog.{name}.cache-enabled") == "false"
    assert spark.conf.get("spark.sql.catalogImplementation") == "in-memory"

    wider = tmp_path / "wider"
    _write_iceberg(
        spark,
        wider,
        TABLE,
        [("1", Decimal("10.005"), "2024-01-01", "a")],
        _schema(DecimalType(10, 3)),
        mode="overwrite",
    )
    data = _target(tmp_path, source, wider)
    data["amounts"][0]["cast"] = "half-even"
    refusal = _refusal(spark, parse_data(data))
    assert refusal.code is RefusalCode.INEXACT_AMOUNT
    assert refusal.message == (
        "target amount column 'amount' value '10.005' is not an exact decimal at scale 2"
    )


def test_iceberg_double_without_cast_is_float_amount(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, ROW)
    warehouse = tmp_path / "doubled"
    _write_iceberg(
        spark,
        warehouse,
        TABLE,
        [("1", 2.5, "2024-01-01", "a")],
        _schema(DoubleType()),
        mode="overwrite",
    )
    refusal = _refusal(spark, parse_data(_target(tmp_path, source, warehouse)))
    assert refusal.code is RefusalCode.FLOAT_AMOUNT
    assert refusal.message == (
        "target amount column 'amount' is double and the spec has no cast rule"
    )


def test_iceberg_double_cast_follows_the_spec(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, "1,2,2024-01-01,a\n")
    warehouse = tmp_path / "doubled"
    _write_iceberg(
        spark,
        warehouse,
        TABLE,
        [("1", 2.5, "2024-01-01", "a")],
        _schema(DoubleType()),
        mode="overwrite",
    )
    for mode, expected in (("half-even", Decimal("2")), ("half-up", Decimal("3"))):
        data = _target(tmp_path, source, warehouse)
        data["amounts"] = [{"column": "amount", "scale": 0, "cast": mode}]
        _source, loaded = read_datasets(spark, parse_data(data))
        assert loaded.schema["amount"].dataType == DecimalType(38, 0)
        assert _amounts(loaded)["1"] == expected


def test_iceberg_missing_column_is_unmapped(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, ROW)
    warehouse = tmp_path / "warehouse"
    _write_iceberg(
        spark,
        warehouse,
        TABLE,
        [("1", Decimal("10.50"), "2024-01-01")],
        _schema(DecimalType(10, 2), names=("txn_id", "amount", "day")),
        mode="overwrite",
    )
    refusal = _refusal(spark, parse_data(_target(tmp_path, source, warehouse)))
    assert refusal.code is RefusalCode.UNMAPPED_COLUMN
    assert refusal.message == "target is missing column 'note'"


def test_missing_iceberg_table_is_a_read_error(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, ROW)
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    spec = parse_data(_target(tmp_path, source, warehouse, table="payments.missing"))
    with pytest.raises(ReadError) as caught:
        check_data(spark, spec)
    assert str(caught.value) == (
        f"target iceberg table 'payments.missing' was not found in {warehouse}"
    )


def test_missing_iceberg_warehouse_is_a_read_error(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, ROW)
    missing = tmp_path / "missing-warehouse"
    spec = parse_data(_target(tmp_path, source, missing))
    with pytest.raises(ReadError) as caught:
        check_data(spark, spec)
    assert str(caught.value) == f"target path not found: {missing}"


def test_iceberg_table_identifier_must_include_a_namespace():
    assert _error(_base(table="payments")) == (
        "source.table must be a namespace.table identifier, such as 'payments.txn'"
    )
    assert _error(_base(table="payments.")) == (
        "source.table must be a namespace.table identifier, such as 'payments.txn'"
    )
    assert _error(_base(table="ns..txn")) == (
        "source.table must be a namespace.table identifier, such as 'payments.txn'"
    )
    nested = parse_data(_base(table="finance.payments.txn"))
    assert nested.source.table == "finance.payments.txn"
    assert nested.source.snapshot_id is None


def test_iceberg_table_identifier_rejects_bad_parts():
    cases = (
        ("1payments.txn", "1payments"),
        ("payments.tx-n", "tx-n"),
        ("payments.txn/extra", "txn/extra"),
    )
    for table, part in cases:
        message = _error(_base(table=table))
        assert message == (
            f"source.table part {part!r} must start with a letter or underscore "
            "and contain only letters, digits, and underscores"
        )


def test_iceberg_unknown_keys_and_snapshot_id_are_strict():
    assert _error(_base(catalog="glue")) == "source has unknown key: 'catalog'"
    missing = _base()
    del missing["source"]["table"]
    assert _error(missing) == "missing source.table"
    assert _error(_base(table=1)) == "source.table must be a string"
    assert _error(_base(snapshot_id=True)) == "source.snapshot_id must be an integer"
    assert _error(_base(snapshot_id=1.5)) == "source.snapshot_id must be an integer"
    assert _error(_base(snapshot_id="1")) == "source.snapshot_id must be an integer"
    assert _error(_base(snapshot_id=None)) == "source.snapshot_id must be an integer"
    assert _error(_base(snapshot_id=1 << 63)) == (
        "source.snapshot_id must fit in a 64-bit signed integer"
    )
    accepted = parse_data(_base(snapshot_id=-1))
    assert accepted.source.snapshot_id == -1


def test_csv_rejects_iceberg_only_keys(tmp_path):
    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    data = spec_data(tmp_path, source, target, target_format="csv")
    data["source"]["table"] = TABLE
    data["source"]["snapshot_id"] = 1
    assert _error(data) == "source has unknown keys: 'snapshot_id', 'table'"


def test_iceberg_warehouse_outside_the_allowlist_is_refused(tmp_path):
    source = tmp_path / "source.csv"
    outside = tmp_path.parent / "outside-warehouse"
    data = _target(tmp_path, source, outside)
    with pytest.raises(RefusalError) as caught:
        parse_data(data)
    refusal = caught.value.refusal
    assert refusal.code is RefusalCode.PATH_NOT_ALLOWED
    assert refusal.message == f"target path {str(outside)!r} is outside the allowlist"
    assert "not found" not in refusal.message


def test_reader_rechecks_an_iceberg_warehouse_against_the_allowlist(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, ROW)
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    spec = parse_data(_target(tmp_path, source, warehouse))
    outside = tmp_path.parent / "outside-warehouse"
    spec = replace(spec, target=Dataset(path=str(outside), format="iceberg", table=TABLE))
    refusal = _refusal(spark, spec)
    assert refusal.code is RefusalCode.PATH_NOT_ALLOWED
    assert "not found" not in refusal.message


def test_s3_iceberg_warehouse_is_a_valid_spec_and_a_read_error(spark, tmp_path):
    data = _base(path="s3://demo-bucket/warehouse")
    data["allowlist"] = ["./data", "s3://demo-bucket"]
    remote = parse_data(data)
    assert remote.source.format == "iceberg"
    assert remote.source.path == "s3://demo-bucket/warehouse"

    source = tmp_path / "source.csv"
    write_csv(source, ROW)
    spec_body = _target(tmp_path, source, tmp_path / "warehouse")
    spec_body["target"]["path"] = "s3://demo-bucket/warehouse"
    spec_body["allowlist"] = [str(tmp_path), "s3://demo-bucket"]
    with pytest.raises(ReadError) as caught:
        check_data(spark, parse_data(spec_body))
    assert str(caught.value) == (
        "s3 reads are not supported yet: target path 's3://demo-bucket/warehouse'"
    )


def test_snapshot_id_reads_that_snapshot_and_the_default_is_current(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, "1,10.00,2024-01-01,a\n2,11.00,2024-01-01,b\n")
    warehouse = tmp_path / "warehouse"
    schema = _schema(DecimalType(10, 2))
    location = _write_iceberg(
        spark,
        warehouse,
        TABLE,
        [("1", Decimal("10.00"), "2024-01-01", "a")],
        schema,
        mode="overwrite",
    )
    first = _current_snapshot_id(location)
    _write_iceberg(
        spark,
        warehouse,
        TABLE,
        [("2", Decimal("11.00"), "2024-01-01", "b")],
        schema,
        mode="append",
    )
    current = _current_snapshot_id(location)
    assert first != current

    older = _target(tmp_path, source, warehouse, target_snapshot=first)
    _source, loaded = read_datasets(spark, parse_data(older))
    assert set(_amounts(loaded)) == {"1"}

    latest = _target(tmp_path, source, warehouse)
    _source, loaded = read_datasets(spark, parse_data(latest))
    assert set(_amounts(loaded)) == {"1", "2"}


def test_missing_iceberg_snapshot_is_a_read_error(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, ROW)
    warehouse = tmp_path / "warehouse"
    _write_iceberg(
        spark,
        warehouse,
        TABLE,
        [("1", Decimal("10.50"), "2024-01-01", "a")],
        _schema(DecimalType(10, 2)),
        mode="overwrite",
    )
    for snapshot_id in (1, -1):
        data = _target(tmp_path, source, warehouse, target_snapshot=snapshot_id)
        with pytest.raises(ReadError) as caught:
            check_data(spark, parse_data(data))
        assert str(caught.value) == (
            f"target iceberg snapshot {snapshot_id} was not found for table {TABLE!r}"
        )


def test_check_data_csv_source_against_iceberg_target_prints_data_ok(spark, tmp_path, capsys):
    source = tmp_path / "source.csv"
    write_csv(source, "1,10.50,2024-01-01,a\n")
    warehouse = tmp_path / "warehouse"
    _write_iceberg(
        spark,
        warehouse,
        TABLE,
        [("1", Decimal("99.00"), "2024-01-01", "b")],
        _schema(DecimalType(10, 2)),
        mode="overwrite",
    )
    spec = write_spec_file(tmp_path, _target(tmp_path, source, warehouse))
    assert main(["validate", "--spec", str(spec), "--check-data"]) is ExitCode.MATCH
    captured = capsys.readouterr()
    assert captured.out == "data ok\n"
    assert captured.err == ""


def test_check_data_iceberg_process_prints_only_data_ok(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, "1,10.50,2024-01-01,a\n")
    warehouse = tmp_path / "warehouse"
    _write_iceberg(
        spark,
        warehouse,
        TABLE,
        [("1", Decimal("10.50"), "2024-01-01", "a")],
        _schema(DecimalType(10, 2)),
        mode="overwrite",
    )
    spec = write_spec_file(tmp_path, _target(tmp_path, source, warehouse))
    result = subprocess.run(
        [sys.executable, "-m", "qa_core_recon", "validate", "--spec", str(spec), "--check-data"],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    assert result.returncode == 0
    assert result.stdout == "data ok\n"
    assert result.stderr == ""


def test_cli_loads_the_iceberg_runtime_only_when_the_spec_uses_it(monkeypatch, tmp_path, capsys):
    calls = []

    def fake_session(*_args, **kwargs):
        calls.append(kwargs.get("iceberg"))
        return None

    monkeypatch.setattr("qa_core_recon.spark.local_session", fake_session)
    monkeypatch.setattr("qa_core_recon.spark.local_session_active", lambda: False)
    monkeypatch.setattr("qa_core_recon.spark.stop_local_session", lambda: None)
    monkeypatch.setattr("qa_core_recon.readers.check_data", lambda *_args, **_kwargs: None)

    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    spec = write_spec_file(tmp_path, spec_data(tmp_path, source, target, target_format="csv"))
    assert main(["validate", "--spec", str(spec), "--check-data"]) is ExitCode.MATCH
    assert calls == [False]
    assert capsys.readouterr().out == "data ok\n"

    warehouse = tmp_path / "warehouse"
    iceberg = write_spec_file(tmp_path, _target(tmp_path, source, warehouse))
    calls.clear()
    assert main(["validate", "--spec", str(iceberg), "--check-data"]) is ExitCode.MATCH
    assert calls == [True]
    assert capsys.readouterr().out == "data ok\n"
