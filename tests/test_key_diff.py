"""Key diff, row hashes, and changed columns. Fixtures live in tmp_path."""

import json
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from qa_core_recon.compare import KEY_DIFF_EXAMPLE_LIMIT, sort_token
from qa_core_recon.exit_codes import ExitCode
from qa_core_recon.hashing import encode_value, row_hash_hex
from tests.support import HEADER, spec_data, write_csv, write_spec_file
from tests.test_iceberg import _write_iceberg
from tests.test_run import (
    _assert_canonical,
    _assert_prose,
    _invoke,
    _load,
)

ROW = "1,10.50,2024-01-01,a\n"


def _utc(year, month, day, hour=0, minute=0, second=0, microsecond=0):
    return datetime(year, month, day, hour, minute, second, microsecond, tzinfo=UTC)


@contextmanager
def _process_tz(name):
    previous = os.environ.get("TZ")
    os.environ["TZ"] = name
    time.tzset()
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous
        time.tzset()


def _example_text(value, *, zoned):
    text = value.strftime("%Y-%m-%dT%H:%M:%S.%f")
    if zoned:
        return text + "Z"
    return text


def _csv_run(tmp_path, source_body, target_body, header=HEADER, **changes):
    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    write_csv(source, source_body, header)
    write_csv(target, target_body, header)
    data = spec_data(tmp_path, source, target, target_format="csv")
    data.update(changes)
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    code = _invoke(spec, out)
    report, raw, markdown = _load(out)
    _assert_canonical(raw)
    _assert_prose(markdown)
    return code, report, raw, markdown


def _key_diff(report):
    return report["checks"][4]


def _changed(report):
    return report["checks"][5]


def _write_frame(spark, path, rows, schema):
    spark.createDataFrame(rows, schema).write.mode("overwrite").parquet(str(path))


def _parquet_run(tmp_path, spark, source_rows, target_rows, schema, **changes):
    source = tmp_path / "source"
    target = tmp_path / "target"
    _write_frame(spark, source, source_rows, schema)
    _write_frame(spark, target, target_rows, schema)
    data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
    data.update(changes)
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    code = _invoke(spec, out)
    report, raw, markdown = _load(out)
    _assert_canonical(raw)
    _assert_prose(markdown)
    return code, report, markdown


def test_match_has_an_empty_key_diff(tmp_path):
    code, report, _raw, markdown = _csv_run(tmp_path, ROW, ROW)
    assert code is ExitCode.MATCH
    details = _key_diff(report)["details"]
    assert _key_diff(report)["status"] == "match"
    assert details["columns"] == ["amount", "day", "note"]
    assert details["type_mismatches"] == []
    assert details["changed_detection"] is None
    assert details["missing"] == {"count": 0, "examples": []}
    assert details["extra"] == {"count": 0, "examples": []}
    assert details["changed"] == {"count": 0, "examples": []}
    assert details["example_limit"] == KEY_DIFF_EXAMPLE_LIMIT == 20
    assert details["null_keys"] == "excluded"
    assert details["counting"] == "distinct_key"
    assert "Rows with a null in any key column are excluded." in details["rules"]
    assert _changed(report)["status"] == "match"
    assert _changed(report)["details"]["columns"] == []
    assert "### Key Diff" in markdown
    assert "### Changed Columns" in markdown
    assert "Compared columns, in hash order: `amount`, `day`, `note`." in markdown
    assert "No compared column differs." in markdown


def test_missing_keys_only(tmp_path):
    code, report, _raw, markdown = _csv_run(
        tmp_path,
        "1,10.00,2024-01-01,a\n2,1.00,2024-01-01,b\n",
        "1,10.00,2024-01-01,a\n",
    )
    assert code is ExitCode.MISMATCH
    details = _key_diff(report)["details"]
    assert details["missing"] == {"count": 1, "examples": [["2"]]}
    assert details["extra"]["count"] == 0
    assert details["changed"]["count"] == 0
    assert _changed(report)["status"] == "match"
    assert "Missing keys: 1." in markdown
    assert "`txn_id`=`2`" in markdown


def test_extra_keys_only(tmp_path):
    code, report, _raw, _markdown = _csv_run(
        tmp_path,
        "1,10.00,2024-01-01,a\n",
        "1,10.00,2024-01-01,a\n2,1.00,2024-01-01,b\n",
    )
    assert code is ExitCode.MISMATCH
    details = _key_diff(report)["details"]
    assert details["missing"]["count"] == 0
    assert details["extra"] == {"count": 1, "examples": [["2"]]}
    assert details["changed"]["count"] == 0
    assert _changed(report)["status"] == "match"


def test_changed_keys_only(tmp_path):
    code, report, _raw, markdown = _csv_run(
        tmp_path,
        ROW,
        "1,10.50,2024-01-01,b\n",
    )
    assert code is ExitCode.MISMATCH
    assert report["checks"][0]["status"] == "match"
    assert report["checks"][1]["status"] == "match"
    details = _key_diff(report)["details"]
    assert details["missing"]["count"] == 0
    assert details["extra"]["count"] == 0
    assert details["changed"] == {"count": 1, "examples": [["1"]]}
    columns = _changed(report)["details"]["columns"]
    assert [column["column"] for column in columns] == ["note"]
    assert columns[0]["count"] == 1
    assert columns[0]["examples"] == [
        {
            "key": ["1"],
            "source": "a",
            "source_token": encode_value("a"),
            "target": "b",
            "target_token": encode_value("b"),
        }
    ]
    assert "Column `note`. Distinct keys that differ: 1." in markdown
    assert "Source: `a`. Target: `b`." in markdown
    assert f"Source token: `{encode_value('a')}`." in markdown


def test_missing_extra_and_changed_together_are_byte_identical(tmp_path):
    source = "1,10.00,2024-01-01,a\n2,1.00,2024-01-01,b\n"
    target = "1,10.00,2024-01-01,z\n3,1.00,2024-01-01,c\n"
    code, report, raw, markdown = _csv_run(tmp_path, source, target)
    assert code is ExitCode.MISMATCH
    details = _key_diff(report)["details"]
    assert details["missing"] == {"count": 1, "examples": [["2"]]}
    assert details["extra"] == {"count": 1, "examples": [["3"]]}
    assert details["changed"] == {"count": 1, "examples": [["1"]]}
    assert _changed(report)["details"]["columns"][0]["column"] == "note"
    other = tmp_path / "other"
    spec = tmp_path / "recon.yaml"
    assert _invoke(spec, other) is ExitCode.MISMATCH
    assert (other / "report.json").read_bytes() == raw
    assert (other / "report.md").read_bytes() == markdown.encode("utf-8")


def test_composite_keys(tmp_path):
    header = "txn_id,line,amount,day,note\n"
    source = "1,1,10.00,2024-01-01,a\n1,2,10.00,2024-01-01,a\n"
    target = "1,1,10.00,2024-01-01,a\n1,3,10.00,2024-01-01,a\n"
    code, report, _raw, markdown = _csv_run(
        tmp_path,
        source,
        target,
        header=header,
        keys=["txn_id", "line"],
        group_by=[],
    )
    assert code is ExitCode.MISMATCH
    details = _key_diff(report)["details"]
    assert details["keys"] == ["txn_id", "line"]
    assert details["missing"] == {"count": 1, "examples": [["1", "2"]]}
    assert details["extra"] == {"count": 1, "examples": [["1", "3"]]}
    assert details["changed"]["count"] == 0
    assert "`txn_id`=`1`, `line`=`2`" in markdown
    assert "`txn_id`=`1`, `line`=`3`" in markdown


def test_null_compared_values_stay_distinct_from_empty_strings(spark, tmp_path):
    amount = DecimalType(10, 2)
    source_rows = [
        ("1", Decimal("1.00"), "2024-01-01", None),
        ("2", Decimal("1.00"), "2024-01-01", None),
        ("3", Decimal("1.00"), "2024-01-01", ""),
        ("4", Decimal("1.00"), "2024-01-01", ""),
    ]
    target_rows = [
        ("1", Decimal("1.00"), "2024-01-01", None),
        ("2", Decimal("1.00"), "2024-01-01", "x"),
        ("3", Decimal("1.00"), "2024-01-01", None),
        ("4", Decimal("1.00"), "2024-01-01", ""),
    ]
    code, report, _markdown = _parquet_run(
        tmp_path,
        spark,
        source_rows,
        target_rows,
        _note_schema(amount),
        group_by=[],
    )
    assert code is ExitCode.MISMATCH
    details = _key_diff(report)["details"]
    assert details["changed"]["count"] == 2
    assert details["changed"]["examples"] == [["2"], ["3"]]
    column = _changed(report)["details"]["columns"][0]
    assert column["column"] == "note"
    assert column["examples"] == [
        {
            "key": ["2"],
            "source": None,
            "source_token": "n",
            "target": "x",
            "target_token": encode_value("x"),
        },
        {
            "key": ["3"],
            "source": "",
            "source_token": encode_value(""),
            "target": None,
            "target_token": "n",
        },
    ]


def test_float_nan_and_negative_zero_share_a_row_hash(spark, tmp_path):
    schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("measure", DoubleType(), True),
        ]
    )
    source_rows = [("1", float("nan")), ("2", -0.0), ("3", float("nan")), ("4", 1.5)]
    target_rows = [("1", float("-nan")), ("2", 0.0), ("3", 1.0), ("4", 1.5)]
    code, report, _markdown = _parquet_run(
        tmp_path,
        spark,
        source_rows,
        target_rows,
        schema,
        amounts=[],
        group_by=[],
        hash_columns=["measure"],
    )
    assert code is ExitCode.MISMATCH
    details = _key_diff(report)["details"]
    assert details["changed"] == {"count": 1, "examples": [["3"]]}
    column = _changed(report)["details"]["columns"][0]
    assert column["column"] == "measure"
    assert column["examples"][0]["source"] == "NaN"
    assert column["examples"][0]["target"] == "1.0"
    assert column["examples"][0]["source_token"] == encode_value(float("nan"))
    assert column["examples"][0]["target_token"] == encode_value(1.0)


def test_decimal_scale_compares_by_value(spark, tmp_path):
    source_schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("qty", DecimalType(10, 1), True),
        ]
    )
    target_schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("qty", DecimalType(10, 2), True),
        ]
    )
    source = tmp_path / "source"
    target = tmp_path / "target"
    _write_frame(spark, source, [("1", Decimal("1.0")), ("2", Decimal("1.0"))], source_schema)
    _write_frame(spark, target, [("1", Decimal("1.00")), ("2", Decimal("1.10"))], target_schema)
    data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["qty"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    report, _raw, _markdown = _load(out)
    details = _key_diff(report)["details"]
    assert details["changed"] == {"count": 1, "examples": [["2"]]}
    example = _changed(report)["details"]["columns"][0]["examples"][0]
    assert example["source_token"] == encode_value(Decimal("1.0"))
    assert example["target_token"] == encode_value(Decimal("1.10"))
    assert example["source"] == "1.0"
    assert example["target"] == "1.10"
    assert encode_value(Decimal("1.0")) == encode_value(Decimal("1.00"))


def test_dates_timestamps_and_booleans(spark, tmp_path):
    schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("flag", BooleanType(), True),
            StructField("day", DateType(), True),
            StructField("instant", TimestampType(), True),
        ]
    )
    moment = _utc(2024, 1, 2, 3, 4, 5)
    later = _utc(2024, 1, 2, 3, 4, 6)
    source_rows = [("1", True, date(2024, 1, 2), moment), ("2", True, date(2024, 1, 2), moment)]
    target_rows = [("1", True, date(2024, 1, 2), moment), ("2", False, date(2024, 1, 2), later)]
    code, report, _markdown = _parquet_run(
        tmp_path,
        spark,
        source_rows,
        target_rows,
        schema,
        amounts=[],
        group_by=[],
        hash_columns=["flag", "instant", "day"],
    )
    assert code is ExitCode.MISMATCH
    assert _key_diff(report)["details"]["columns"] == ["day", "flag", "instant"]
    assert _key_diff(report)["details"]["changed"] == {"count": 1, "examples": [["2"]]}
    columns = {column["column"]: column for column in _changed(report)["details"]["columns"]}
    assert set(columns) == {"flag", "instant"}
    assert columns["flag"]["examples"][0]["source"] is True
    assert columns["flag"]["examples"][0]["target"] is False
    assert columns["flag"]["examples"][0]["source_token"] == encode_value(True)
    assert columns["instant"]["examples"][0]["source"] == "2024-01-02T03:04:05.000000Z"
    assert columns["instant"]["examples"][0]["target"] == "2024-01-02T03:04:06.000000Z"
    assert columns["instant"]["examples"][0]["source_token"] == encode_value(moment)
    assert columns["instant"]["examples"][0]["target_token"] == encode_value(later)


def test_examples_stop_at_the_limit_and_the_full_rows_are_not_collected(monkeypatch, tmp_path):
    collected = []

    original_collect = DataFrame.collect

    def spy_collect(self):
        rows = original_collect(self)
        collected.append(len(rows))
        return rows

    original_iterator = DataFrame.toLocalIterator

    def spy_iterator(self):
        count = 0
        for item in original_iterator(self):
            count += 1
            if count > KEY_DIFF_EXAMPLE_LIMIT:
                raise AssertionError("toLocalIterator walked past the example cap")
            yield item

    monkeypatch.setattr(DataFrame, "collect", spy_collect)
    monkeypatch.setattr(DataFrame, "toLocalIterator", spy_iterator)

    def row(key, note):
        return f"{key},1.00,2024-01-01,{note}\n"

    limit = KEY_DIFF_EXAMPLE_LIMIT
    missing = [row(f"m{index:02d}", "a") for index in range(limit + 1)]
    extra = [row(f"e{index:02d}", "a") for index in range(limit + 1)]
    changed_source = [row(f"c{index:02d}", "a") for index in range(limit + 1)]
    changed_target = [row(f"c{index:02d}", "b") for index in range(limit + 1)]
    code, report, _raw, markdown = _csv_run(
        tmp_path,
        "".join(missing + changed_source),
        "".join(extra + changed_target),
        group_by=[],
    )
    assert code is ExitCode.MISMATCH
    details = _key_diff(report)["details"]
    assert details["missing"]["count"] == limit + 1
    assert details["extra"]["count"] == limit + 1
    assert details["changed"]["count"] == limit + 1
    assert [item[0] for item in details["missing"]["examples"]] == [
        f"m{index:02d}" for index in range(limit)
    ]
    assert [item[0] for item in details["extra"]["examples"]] == [
        f"e{index:02d}" for index in range(limit)
    ]
    assert [item[0] for item in details["changed"]["examples"]] == [
        f"c{index:02d}" for index in range(limit)
    ]
    column = _changed(report)["details"]["columns"][0]
    assert column["column"] == "note"
    assert column["count"] == limit + 1
    assert len(column["examples"]) == limit
    assert "The first 20 of 21 are listed in sorted order." in markdown
    assert collected
    assert max(collected) <= KEY_DIFF_EXAMPLE_LIMIT
    assert KEY_DIFF_EXAMPLE_LIMIT in collected


def test_null_keys_are_excluded(spark, tmp_path):
    amount = DecimalType(10, 2)
    source_rows = [
        (None, Decimal("5.00"), "2024-01-01", "gone"),
        ("1", Decimal("1.00"), "2024-01-01", "a"),
    ]
    target_rows = [
        ("1", Decimal("1.00"), "2024-01-01", "a"),
        ("9", Decimal("1.00"), "2024-01-01", "a"),
    ]
    code, report, markdown = _parquet_run(
        tmp_path, spark, source_rows, target_rows, _note_schema(amount), group_by=[]
    )
    assert code is ExitCode.MISMATCH
    assert report["checks"][2]["details"]["source"] == 1
    details = _key_diff(report)["details"]
    assert details["missing"] == {"count": 0, "examples": []}
    assert details["extra"] == {"count": 1, "examples": [["9"]]}
    assert details["changed"]["count"] == 0
    assert "gone" not in markdown
    assert "Rows with a null in any key column are excluded." in markdown


def test_duplicate_keys_use_the_smallest_row_hash(tmp_path):
    winner = min(("a", "b"), key=lambda note: row_hash_hex((note,)))
    loser = "b" if winner == "a" else "a"
    header = "txn_id,note\n"
    source = f"1,{winner}\n1,{loser}\n2,{winner}\n2,{loser}\n3,{winner}\n3,{winner}\n"
    target = f"1,{winner}\n2,{loser}\n3,{winner}\n"
    code, report, _raw, markdown = _csv_run(
        tmp_path,
        source,
        target,
        header=header,
        amounts=[],
        group_by=[],
        hash_columns=["note"],
    )
    assert code is ExitCode.MISMATCH
    assert report["checks"][3]["status"] == "mismatch"
    details = _key_diff(report)["details"]
    assert details["duplicates_present"] is True
    assert details["missing"]["count"] == 0
    assert details["extra"]["count"] == 0
    assert details["changed"] == {"count": 1, "examples": [["2"]]}
    example = _changed(report)["details"]["columns"][0]["examples"][0]
    assert example == {
        "key": ["2"],
        "source": winner,
        "source_token": encode_value(winner),
        "target": loser,
        "target_token": encode_value(loser),
    }
    assert "Duplicate keys were found, so each distinct key is counted once." in markdown
    assert "When a key is duplicated, the row with the smallest row hash is used." in markdown


def test_columns_outside_the_spec_are_not_hashed(tmp_path):
    header = "txn_id,extra\n"
    code, report, _raw, _markdown = _csv_run(
        tmp_path,
        "1,a\n2,a\n",
        "1,b\n",
        header=header,
        amounts=[],
        group_by=[],
        hash_columns=[],
    )
    assert code is ExitCode.MISMATCH
    details = _key_diff(report)["details"]
    assert details["columns"] == []
    assert details["missing"] == {"count": 1, "examples": [["2"]]}
    assert details["changed"]["count"] == 0
    note = "Changed rows cannot be detected because no compared columns were declared."
    assert details["changed_detection"] == note
    assert note in _markdown
    assert report["checks"][4]["status"] == "mismatch"


def test_group_by_values_are_compared(tmp_path):
    header = "txn_id,amount,day,note\n"
    source = "k1,10.00,a,n\nk2,10.00,b,n\n"
    target = "k1,10.00,b,n\nk2,10.00,a,n\n"
    code, report, _raw, _markdown = _csv_run(
        tmp_path,
        source,
        target,
        header=header,
        group_by=["day"],
        hash_columns=["note"],
    )
    assert code is ExitCode.MISMATCH
    assert report["checks"][0]["status"] == "match"
    assert report["checks"][1]["status"] == "match"
    details = _key_diff(report)["details"]
    assert details["changed"]["count"] == 2
    assert _changed(report)["details"]["columns"][0]["column"] == "day"


def test_timestamps_use_utc_under_a_local_time_zone(spark, tmp_path):
    schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("instant", TimestampType(), True),
        ]
    )
    source = tmp_path / "source"
    target = tmp_path / "target"
    _write_frame(spark, source, [("1", _utc(2024, 11, 3, 5, 30))], schema)
    _write_frame(spark, target, [("1", _utc(2024, 11, 3, 6, 30))], schema)
    data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["instant"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    env = os.environ.copy()
    env["TZ"] = "America/New_York"
    env["JAVA_HOME"] = os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
    completed = subprocess.run(
        [sys.executable, "-m", "qa_core_recon", "run", "--spec", str(spec), "--out", str(out)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert completed.returncode == 1, completed.stderr
    report = json.loads((out / "report.json").read_text(encoding="utf-8"))
    example = _changed(report)["details"]["columns"][0]["examples"][0]
    assert example["source"] == "2024-11-03T05:30:00.000000Z"
    assert example["target"] == "2024-11-03T06:30:00.000000Z"
    assert "05:30:00.000000Z" in example["source_token"]
    rendered = (out / "report.md").read_text(encoding="utf-8")
    assert "05:30:00.000000Z" in rendered


def test_csv_values_align_with_parquet_types(spark, tmp_path):
    header = "txn_id,qty_int,qty_dec,day,instant\n"
    body = "1,5,1.50,2024-01-01,2024-01-01 05:30:00\n"
    source = tmp_path / "source.csv"
    write_csv(source, body, header)
    schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("qty_int", LongType(), True),
            StructField("qty_dec", DecimalType(10, 2), True),
            StructField("day", DateType(), True),
            StructField("instant", TimestampType(), True),
        ]
    )
    target = tmp_path / "target"
    _write_frame(
        spark,
        target,
        [("1", 5, Decimal("1.50"), date(2024, 1, 1), _utc(2024, 1, 1, 5, 30))],
        schema,
    )
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(
        amounts=[],
        group_by=[],
        hash_columns=["qty_int", "qty_dec", "day", "instant"],
    )
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MATCH
    report, _raw, _markdown = _load(out)
    assert _key_diff(report)["status"] == "match"
    assert _key_diff(report)["details"]["changed"]["count"] == 0
    assert _key_diff(report)["details"]["type_mismatches"] == []


def test_csv_values_align_with_an_iceberg_target(spark, tmp_path):
    header = "txn_id,qty_int,qty_dec,day,instant\n"
    body = "1,5,1.50,2024-01-01,2024-01-01 05:30:00\n"
    source = tmp_path / "source.csv"
    write_csv(source, body, header)
    schema = StructType(
        [
            StructField("txn_id", StringType(), False),
            StructField("qty_int", LongType(), True),
            StructField("qty_dec", DecimalType(10, 2), True),
            StructField("day", DateType(), True),
            StructField("instant", TimestampType(), True),
        ]
    )
    warehouse = tmp_path / "warehouse"
    _write_iceberg(
        spark,
        warehouse,
        "payments.txn",
        [("1", 5, Decimal("1.50"), date(2024, 1, 1), _utc(2024, 1, 1, 5, 30))],
        schema,
        mode="overwrite",
    )
    data = spec_data(
        tmp_path,
        source,
        warehouse,
        source_format="csv",
        target_format="iceberg",
        target_table="payments.txn",
    )
    data.update(
        amounts=[],
        group_by=[],
        hash_columns=["day", "instant", "qty_dec", "qty_int"],
    )
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MATCH
    report, _raw, markdown = _load(out)
    assert _key_diff(report)["status"] == "match"
    assert "### Key Diff" in markdown
    assert _key_diff(report)["details"]["changed"]["count"] == 0


def test_unparsed_string_is_changed_and_not_null(spark, tmp_path):
    header = "txn_id,day\n"
    source = tmp_path / "source.csv"
    write_csv(source, "1,not-a-date\n", header)
    schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("day", DateType(), True),
        ]
    )
    target = tmp_path / "target"
    _write_frame(spark, target, [("1", date(2024, 1, 2))], schema)
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["day"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    report = _load(out)[0]
    example = _changed(report)["details"]["columns"][0]["examples"][0]
    assert example["source"] == "not-a-date"
    assert example["source_token"].startswith("invalid:")
    assert example["target"] == "2024-01-02"
    assert example["target_token"] != "n"


def test_int_and_decimal_compare_by_value(spark, tmp_path):
    source_schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("qty", LongType(), True),
        ]
    )
    target_schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("qty", DecimalType(8, 2), True),
        ]
    )
    source = tmp_path / "source"
    target = tmp_path / "target"
    _write_frame(spark, source, [("1", 5)], source_schema)
    _write_frame(spark, target, [("1", Decimal("5.00"))], target_schema)
    data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["qty"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MATCH


def test_float_and_double_are_a_type_mismatch(spark, tmp_path):
    source_schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("measure", FloatType(), True),
        ]
    )
    target_schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("measure", DoubleType(), True),
        ]
    )
    source = tmp_path / "source"
    target = tmp_path / "target"
    _write_frame(spark, source, [("1", 1.0)], source_schema)
    _write_frame(spark, target, [("1", 1.0)], target_schema)
    data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["measure"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    report, _raw, markdown = _load(out)
    mismatches = _key_diff(report)["details"]["type_mismatches"]
    assert mismatches == [
        {
            "column": "measure",
            "role": "compared",
            "source_type": "float",
            "target_type": "double",
        }
    ]
    assert "Type mismatches." in markdown
    assert "Source type: `float`. Target type: `double`." in markdown


def test_string_key_does_not_join_a_wider_integer(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, "01,a\n", "txn_id,note\n")
    schema = StructType(
        [
            StructField("txn_id", LongType(), True),
            StructField("note", StringType(), True),
        ]
    )
    target = tmp_path / "target"
    _write_frame(spark, target, [(1, "a")], schema)
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    details = _key_diff(_load(out)[0])["details"]
    assert details["missing"] == {"count": 1, "examples": [["01"]]}
    assert details["extra"] == {"count": 1, "examples": [[1]]}
    assert details["changed"]["count"] == 0


def test_nan_and_negative_zero_examples_sort_in_a_fixed_order(spark, tmp_path):
    schema = StructType(
        [
            StructField("measure", DoubleType(), True),
            StructField("note", StringType(), True),
        ]
    )
    source_rows = [(-1.0, "a"), (-0.0, "a"), (2.0, "a"), (float("nan"), "a")]
    target_rows = [(-1.0, "b"), (0.0, "b"), (2.0, "b"), (float("-nan"), "b")]
    code, report, _markdown = _parquet_run(
        tmp_path,
        spark,
        source_rows,
        target_rows,
        schema,
        amounts=[],
        group_by=[],
        hash_columns=["note"],
        keys=["measure"],
    )
    assert code is ExitCode.MISMATCH
    examples = _key_diff(report)["details"]["changed"]["examples"]
    assert examples == [["-1.0"], ["0.0"], ["2.0"], ["NaN"]]


_KEY_SPELLINGS = ("01", "+1", " 1", "1.0", "1.00", "1e0")


def _spelling_rows(spellings):
    return "".join(f"{text},a\n" for text in spellings)


def test_int_key_text_must_be_the_digits(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, _spelling_rows(("1", *_KEY_SPELLINGS)), "txn_id,note\n")
    schema = StructType(
        [
            StructField("txn_id", LongType(), True),
            StructField("note", StringType(), True),
        ]
    )
    target = tmp_path / "target"
    _write_frame(spark, target, [(1, "a")], schema)
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    details = _key_diff(_load(out)[0])["details"]
    assert details["extra"]["count"] == 0
    assert details["changed"]["count"] == 0
    expected = [[text] for text in sorted(_KEY_SPELLINGS, key=sort_token)]
    assert details["missing"]["examples"] == expected


def test_decimal_key_text_must_use_the_column_scale(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, _spelling_rows(_KEY_SPELLINGS), "txn_id,note\n")
    schema = StructType(
        [
            StructField("txn_id", DecimalType(10, 2), True),
            StructField("note", StringType(), True),
        ]
    )
    target = tmp_path / "target"
    _write_frame(spark, target, [(Decimal("1.00"), "a")], schema)
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    details = _key_diff(_load(out)[0])["details"]
    assert details["extra"]["count"] == 0
    assert details["changed"]["count"] == 0
    unmatched = tuple(text for text in _KEY_SPELLINGS if text != "1.00")
    assert details["missing"]["examples"] == [[text] for text in sorted(unmatched, key=sort_token)]


def test_noncanonical_date_string_is_changed(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, "1,2024-1-1\n", "txn_id,day\n")
    schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("day", DateType(), True),
        ]
    )
    target = tmp_path / "target"
    _write_frame(spark, target, [("1", date(2024, 1, 1))], schema)
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["day"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    example = _changed(_load(out)[0])["details"]["columns"][0]["examples"][0]
    assert example["key"] == ["1"]
    assert example["source"] == "2024-1-1"
    assert example["source_token"].startswith("invalid:")
    assert example["target"] == "2024-01-01"


def test_special_column_names_are_quoted_in_casts(spark, tmp_path):
    key_names = ("a-b", "a b", "a.b", "a`b")
    hash_names = ("h-b", "h b", "h.b", "h`b")
    header = ",".join((*key_names, *hash_names)) + "\n"
    source = tmp_path / "source.csv"
    write_csv(source, "1,1,1,1,7,7,7,7\n", header)
    fields = [StructField(name, LongType(), True) for name in (*key_names, *hash_names)]
    target = tmp_path / "target"
    _write_frame(spark, target, [(1, 1, 1, 1, 7, 7, 7, 7)], StructType(fields))
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], keys=list(key_names), hash_columns=list(hash_names))
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MATCH
    report, _raw, _markdown = _load(out)
    assert _key_diff(report)["status"] == "match"
    assert _key_diff(report)["details"]["changed"]["count"] == 0


def test_high_scale_decimal_key_rejects_an_exponent(spark, tmp_path):
    canonical = (
        ("0.0000000000", Decimal(0)),
        ("0.0000001000", Decimal("0.0000001")),
        ("-1.5000000000", Decimal("-1.5")),
    )
    exponents = ("0E-10", "1.000E-7", "-1.5E+0")
    source = tmp_path / "source.csv"
    body = "".join(f"{text},a\n" for text, _value in canonical)
    body += "".join(f"{text},a\n" for text in exponents)
    write_csv(source, body, "txn_id,note\n")
    schema = StructType(
        [
            StructField("txn_id", DecimalType(20, 10), True),
            StructField("note", StringType(), True),
        ]
    )
    target = tmp_path / "target"
    _write_frame(spark, target, [(value, "a") for _text, value in canonical], schema)
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    details = _key_diff(_load(out)[0])["details"]
    assert details["extra"]["count"] == 0
    assert details["changed"]["count"] == 0
    assert details["missing"]["examples"] == [[text] for text in sorted(exponents, key=sort_token)]


def test_boolean_key_requires_lowercase_true_or_false(spark, tmp_path):
    bad = ("TRUE", "1", "yes", " true")
    source = tmp_path / "source.csv"
    write_csv(source, _spelling_rows(("true", "false", *bad)), "txn_id,note\n")
    schema = StructType(
        [
            StructField("txn_id", BooleanType(), True),
            StructField("note", StringType(), True),
        ]
    )
    target = tmp_path / "target"
    _write_frame(spark, target, [(True, "a"), (False, "a")], schema)
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    details = _key_diff(_load(out)[0])["details"]
    assert details["extra"]["count"] == 0
    assert details["changed"]["count"] == 0
    assert details["missing"]["examples"] == [[text] for text in sorted(bad, key=sort_token)]


def test_timestamp_key_requires_the_utc_text(spark, tmp_path):
    canonical = "2024-01-01T00:00:00.000000Z"
    bad = ("2024-01-01", " 2024-01-01")
    source = tmp_path / "source.csv"
    write_csv(source, _spelling_rows((canonical, *bad)), "txn_id,note\n")
    schema = StructType(
        [
            StructField("txn_id", TimestampType(), True),
            StructField("note", StringType(), True),
        ]
    )
    target = tmp_path / "target"
    _write_frame(spark, target, [(_utc(2024, 1, 1), "a")], schema)
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    details = _key_diff(_load(out)[0])["details"]
    assert details["extra"]["count"] == 0
    assert details["changed"]["count"] == 0
    assert details["missing"]["examples"] == [[text] for text in sorted(bad, key=sort_token)]


def test_timestamp_ntz_key_omits_the_zone_suffix(spark, tmp_path):
    canonical = "2024-01-01T00:00:00.000000"
    bad = ("2024-01-01T00:00:00.000000Z", "2024-01-01")
    source = tmp_path / "source.csv"
    write_csv(source, _spelling_rows((canonical, *bad)), "txn_id,note\n")
    schema = StructType(
        [
            StructField("txn_id", TimestampNTZType(), True),
            StructField("note", StringType(), True),
        ]
    )
    target = tmp_path / "target"
    _write_frame(spark, target, [(datetime(2024, 1, 1), "a")], schema)
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    details = _key_diff(_load(out)[0])["details"]
    assert details["extra"]["count"] == 0
    assert details["changed"]["count"] == 0
    assert details["missing"]["examples"] == [[text] for text in sorted(bad, key=sort_token)]


def test_float_key_is_a_type_mismatch(spark, tmp_path):
    bad = ("01", " 1e0")
    source = tmp_path / "source.csv"
    write_csv(source, _spelling_rows(bad), "measure,note\n")
    schema = StructType(
        [
            StructField("measure", DoubleType(), True),
            StructField("note", StringType(), True),
        ]
    )
    target = tmp_path / "target"
    _write_frame(spark, target, [(1.0, "a")], schema)
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["measure"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    report, _raw, markdown = _load(out)
    details = _key_diff(report)["details"]
    assert details["type_mismatches"] == [
        {
            "column": "measure",
            "role": "key",
            "source_type": "string",
            "target_type": "double",
        }
    ]
    assert details["missing"]["examples"] == [[text] for text in sorted(bad, key=sort_token)]
    assert details["extra"]["count"] == 1
    assert "Type mismatches." in markdown
    assert "`measure` (key)." in markdown


def test_padded_date_key_stays_unmatched(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, " 2024-01-01,a\n", "txn_id,note\n")
    schema = StructType(
        [
            StructField("txn_id", DateType(), True),
            StructField("note", StringType(), True),
        ]
    )
    target = tmp_path / "target"
    _write_frame(spark, target, [(date(2024, 1, 1), "a")], schema)
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    details = _key_diff(_load(out)[0])["details"]
    assert details["missing"]["examples"] == [[" 2024-01-01"]]
    assert details["extra"]["count"] == 1
    assert details["changed"]["count"] == 0


def test_padded_date_value_still_matches(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, "1, 2024-01-01\n", "txn_id,day\n")
    schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("day", DateType(), True),
        ]
    )
    target = tmp_path / "target"
    _write_frame(spark, target, [("1", date(2024, 1, 1))], schema)
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["day"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MATCH


_NON_ASCII_AMOUNTS = (
    ("1", "1_000", Decimal("1000.00")),
    ("2", "١٠", Decimal("10.00")),
    ("3", "１０", Decimal("10.00")),
    ("4", "\u00a010", Decimal("10.00")),
)


def test_non_ascii_amount_text_does_not_match_a_decimal_value(spark, tmp_path):
    source = tmp_path / "source.csv"
    body = "".join(f"{key},{text}\n" for key, text, _value in _NON_ASCII_AMOUNTS)
    write_csv(source, body, "txn_id,qty\n")
    schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("qty", DecimalType(12, 2), True),
        ]
    )
    target = tmp_path / "target"
    rows = [(key, value) for key, _text, value in _NON_ASCII_AMOUNTS]
    _write_frame(spark, target, rows, schema)
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["qty"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    column = _changed(_load(out)[0])["details"]["columns"][0]
    assert column["column"] == "qty"
    assert column["count"] == 4
    assert [example["key"] for example in column["examples"]] == [["1"], ["2"], ["3"], ["4"]]
    assert all(example["source_token"].startswith("invalid:") for example in column["examples"])


def test_non_ascii_amount_text_does_not_match_an_int_key(spark, tmp_path):
    source = tmp_path / "source.csv"
    body = "".join(f"{text},a\n" for _key, text, _value in _NON_ASCII_AMOUNTS)
    write_csv(source, body, "txn_id,note\n")
    schema = StructType(
        [
            StructField("txn_id", LongType(), True),
            StructField("note", StringType(), True),
        ]
    )
    target = tmp_path / "target"
    _write_frame(spark, target, [(1000, "a"), (10, "a")], schema)
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    details = _key_diff(_load(out)[0])["details"]
    texts = [text for _key, text, _value in _NON_ASCII_AMOUNTS]
    assert details["missing"]["examples"] == [[text] for text in sorted(texts, key=sort_token)]
    assert details["extra"]["examples"] == [[10], [1000]]
    assert details["changed"]["count"] == 0


def test_int_key_examples_are_the_first_by_value(spark, tmp_path):
    keys = list(range(-5, 25))
    schema = StructType(
        [
            StructField("txn_id", LongType(), True),
            StructField("note", StringType(), True),
        ]
    )
    source = tmp_path / "source"
    target = tmp_path / "target"
    _write_frame(spark, source, [(key, "a") for key in keys], schema)
    _write_frame(spark, target, [(key, "b") for key in keys], schema)
    data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    report = _load(out)[0]
    expected = [[key] for key in range(-5, 15)]
    changed = _key_diff(report)["details"]["changed"]
    assert changed["count"] == len(keys)
    assert changed["examples"] == expected
    column = _changed(report)["details"]["columns"][0]
    assert column["count"] == len(keys)
    assert [example["key"] for example in column["examples"]] == expected


def test_changed_string_keys_follow_source_values_not_tokens(spark, tmp_path):
    keys = [str(number) for number in range(-30, 10)]
    expected = [[key] for key in sorted(keys)[:KEY_DIFF_EXAMPLE_LIMIT]]
    token_page = sorted(keys, key=lambda key: encode_value(int(key)))[:KEY_DIFF_EXAMPLE_LIMIT]
    token_page.sort()
    assert expected != [[key] for key in token_page]
    assert expected != [[str(number)] for number in range(-30, -10)]
    assert expected != [[key] for key in reversed(keys)][:KEY_DIFF_EXAMPLE_LIMIT]
    source_schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("note", StringType(), True),
        ]
    )
    target_schema = StructType(
        [
            StructField("txn_id", LongType(), True),
            StructField("note", StringType(), True),
        ]
    )
    source = tmp_path / "source"
    target = tmp_path / "target"
    _write_frame(spark, source, [(key, "a") for key in reversed(keys)], source_schema)
    _write_frame(spark, target, [(int(key), "b") for key in reversed(keys)], target_schema)
    data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    report = _load(out)[0]
    changed = _key_diff(report)["details"]["changed"]
    assert changed["count"] == len(keys)
    assert changed["examples"] == expected
    column = _changed(report)["details"]["columns"][0]
    assert column["count"] == len(keys)
    assert [example["key"] for example in column["examples"]] == expected


def test_extra_examples_follow_target_values_not_the_source_side(spark, tmp_path):
    source_keys = [str(number) for number in range(30)]
    target_keys = list(range(-30, 0))
    expected_missing = [[key] for key in sorted(source_keys)[:KEY_DIFF_EXAMPLE_LIMIT]]
    token_page = sorted(source_keys, key=encode_value)[:KEY_DIFF_EXAMPLE_LIMIT]
    token_page.sort()
    assert expected_missing != [[key] for key in token_page]
    expected_extra = [[key] for key in target_keys[:KEY_DIFF_EXAMPLE_LIMIT]]
    token_longs = sorted(target_keys, key=encode_value)[:KEY_DIFF_EXAMPLE_LIMIT]
    token_longs.sort()
    assert expected_extra != [[key] for key in token_longs]
    source_schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("note", StringType(), True),
        ]
    )
    target_schema = StructType(
        [
            StructField("txn_id", LongType(), True),
            StructField("note", StringType(), True),
        ]
    )
    source = tmp_path / "source"
    target = tmp_path / "target"
    _write_frame(spark, source, [(key, "a") for key in reversed(source_keys)], source_schema)
    _write_frame(spark, target, [(key, "a") for key in reversed(target_keys)], target_schema)
    data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    details = _key_diff(_load(out)[0])["details"]
    assert details["missing"]["count"] == len(source_keys)
    assert details["missing"]["examples"] == expected_missing
    assert details["extra"]["count"] == len(target_keys)
    assert details["extra"]["examples"] == expected_extra
    assert details["changed"]["count"] == 0


def test_timestamp_key_examples_use_utc_text_in_every_zone(spark, tmp_path):
    missing = (_utc(2023, 6, 1, 12), _utc(2023, 6, 1, 18))
    extra = (_utc(2023, 6, 2), _utc(2023, 12, 1, microsecond=42))
    changed = _utc(2023, 7, 15, 8, 30)
    schema = StructType(
        [
            StructField("txn_id", TimestampType(), True),
            StructField("note", StringType(), True),
        ]
    )
    source = tmp_path / "source"
    target = tmp_path / "target"
    _write_frame(
        spark,
        source,
        [(changed, "a"), (missing[1], "a"), (missing[0], "a")],
        schema,
    )
    _write_frame(
        spark,
        target,
        [(changed, "b"), (extra[1], "a"), (extra[0], "a")],
        schema,
    )
    data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    missing_text = [[_example_text(value, zoned=True)] for value in missing]
    extra_text = [[_example_text(value, zoned=True)] for value in extra]
    changed_text = [[_example_text(changed, zoned=True)]]
    for zone in ("UTC", "Asia/Karachi"):
        with _process_tz(zone):
            out = tmp_path / zone.replace("/", "-")
            assert _invoke(spec, out) is ExitCode.MISMATCH, zone
            report = _load(out)[0]
            details = _key_diff(report)["details"]
            assert details["missing"]["examples"] == missing_text, zone
            assert details["extra"]["examples"] == extra_text, zone
            assert details["changed"]["examples"] == changed_text, zone
            column = _changed(report)["details"]["columns"][0]
            assert [example["key"] for example in column["examples"]] == changed_text, zone
            assert "17:00" not in json.dumps(report), zone


def test_timestamp_ntz_key_examples_keep_wall_time_and_microseconds(spark, tmp_path):
    missing = (datetime(2023, 6, 1, 12), datetime(2023, 6, 1, 18))
    extra = (datetime(2023, 6, 2), datetime(2023, 12, 1, microsecond=42))
    changed = datetime(2023, 7, 15, 8, 30)
    schema = StructType(
        [
            StructField("txn_id", TimestampNTZType(), True),
            StructField("note", StringType(), True),
        ]
    )
    source = tmp_path / "source"
    target = tmp_path / "target"
    _write_frame(
        spark,
        source,
        [(changed, "a"), (missing[1], "a"), (missing[0], "a")],
        schema,
    )
    _write_frame(
        spark,
        target,
        [(changed, "b"), (extra[1], "a"), (extra[0], "a")],
        schema,
    )
    data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    missing_text = [[_example_text(value, zoned=False)] for value in missing]
    extra_text = [[_example_text(value, zoned=False)] for value in extra]
    changed_text = [[_example_text(changed, zoned=False)]]
    for zone in ("UTC", "Asia/Karachi"):
        with _process_tz(zone):
            out = tmp_path / zone.replace("/", "-")
            assert _invoke(spec, out) is ExitCode.MISMATCH, zone
            report = _load(out)[0]
            details = _key_diff(report)["details"]
            assert details["missing"]["examples"] == missing_text, zone
            assert details["extra"]["examples"] == extra_text, zone
            assert details["changed"]["examples"] == changed_text, zone
            column = _changed(report)["details"]["columns"][0]
            assert [example["key"] for example in column["examples"]] == changed_text, zone
            assert all("Z" not in example[0] for example in details["extra"]["examples"])
            assert "17:00" not in json.dumps(report), zone


def test_boolean_value_rejects_a_trailing_control_character(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, "1,true\x01\n", "txn_id,flag\n")
    schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("flag", BooleanType(), True),
        ]
    )
    target = tmp_path / "target"
    _write_frame(spark, target, [("1", True)], schema)
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["flag"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    column = _changed(_load(out)[0])["details"]["columns"][0]
    assert column["column"] == "flag"
    assert column["count"] == 1
    assert column["examples"][0]["source_token"].startswith("invalid:")


def test_timestamp_value_rejects_a_leading_control_character(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, "1,\x012024-01-01T00:00:00.000000Z\n", "txn_id,instant\n")
    schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("instant", TimestampType(), True),
        ]
    )
    target = tmp_path / "target"
    _write_frame(spark, target, [("1", _utc(2024, 1, 1))], schema)
    data = spec_data(tmp_path, source, target, source_format="csv", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["instant"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    column = _changed(_load(out)[0])["details"]["columns"][0]
    assert column["column"] == "instant"
    assert column["count"] == 1
    assert column["examples"][0]["source_token"].startswith("invalid:")
    assert column["examples"][0]["target"] == "2024-01-01T00:00:00.000000Z"


_LINE_BREAKS = ("\n", "\r", "\r\n")


def _typed_cases():
    return (
        ("boolean", BooleanType(), True, "true"),
        ("timestamp", TimestampType(), _utc(2024, 1, 1), "2024-01-01T00:00:00.000000Z"),
        ("timestamp_ntz", TimestampNTZType(), datetime(2024, 1, 1), "2024-01-01T00:00:00.000000"),
        ("date", DateType(), date(2024, 1, 1), "2024-01-01"),
        ("int", LongType(), 10, "10"),
        ("decimal", DecimalType(12, 2), Decimal("10.00"), "10.00"),
    )


def test_trailing_line_breaks_do_not_match_typed_values(spark, tmp_path):
    for name, typed, good, text in _typed_cases():
        strings = [text + suffix for suffix in _LINE_BREAKS]
        source_schema = StructType(
            [
                StructField("txn_id", StringType(), True),
                StructField("value", StringType(), True),
            ]
        )
        target_schema = StructType(
            [
                StructField("txn_id", StringType(), True),
                StructField("value", typed, True),
            ]
        )
        source = tmp_path / name / "source"
        target = tmp_path / name / "target"
        _write_frame(
            spark,
            source,
            [(str(index), value) for index, value in enumerate(strings, start=1)],
            source_schema,
        )
        _write_frame(
            spark,
            target,
            [(str(index), good) for index in range(1, len(strings) + 1)],
            target_schema,
        )
        data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
        data.update(amounts=[], group_by=[], hash_columns=["value"], keys=["txn_id"])
        spec = write_spec_file(tmp_path / name, data)
        out = tmp_path / name / "out"
        assert _invoke(spec, out) is ExitCode.MISMATCH, name
        column = _changed(_load(out)[0])["details"]["columns"][0]
        assert column["count"] == len(strings), name
        assert all(example["source_token"].startswith("invalid:") for example in column["examples"])


def test_trailing_line_breaks_do_not_match_typed_keys(spark, tmp_path):
    for name, typed, good, text in _typed_cases():
        strings = [text + suffix for suffix in _LINE_BREAKS]
        source_schema = StructType(
            [
                StructField("txn_id", StringType(), True),
                StructField("note", StringType(), True),
            ]
        )
        target_schema = StructType(
            [
                StructField("txn_id", typed, True),
                StructField("note", StringType(), True),
            ]
        )
        source = tmp_path / name / "source"
        target = tmp_path / name / "target"
        _write_frame(spark, source, [(value, "a") for value in strings], source_schema)
        _write_frame(spark, target, [(good, "a")], target_schema)
        data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
        data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
        spec = write_spec_file(tmp_path / name, data)
        out = tmp_path / name / "out"
        assert _invoke(spec, out) is ExitCode.MISMATCH, name
        details = _key_diff(_load(out)[0])["details"]
        assert details["changed"]["count"] == 0, name
        assert details["missing"]["examples"] == [[value] for value in sorted(strings)], name
        assert details["extra"]["count"] == 1, name


def test_group_by_timestamps_use_utc_text(spark, tmp_path):
    schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("instant", TimestampType(), True),
            StructField("wall", TimestampNTZType(), True),
            StructField("note", StringType(), True),
        ]
    )
    row = ("1", _utc(2023, 6, 1, 12), datetime(2023, 6, 1, 12), "a")
    source = tmp_path / "source"
    target = tmp_path / "target"
    _write_frame(spark, source, [row], schema)
    _write_frame(spark, target, [row], schema)
    data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
    data.update(amounts=[], group_by=["instant", "wall"], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    expected = {
        "instant": "2023-06-01T12:00:00.000000Z",
        "wall": "2023-06-01T12:00:00.000000",
    }
    reports = []
    for zone in ("UTC", "Asia/Karachi"):
        with _process_tz(zone):
            out = tmp_path / zone.replace("/", "-")
            assert _invoke(spec, out) is ExitCode.MATCH, zone
            raw = (out / "report.json").read_bytes()
            report = json.loads(raw)
            group = report["checks"][0]["details"]["groups"][0]["group"]
            assert group == expected, zone
            assert "17:00" not in raw.decode("utf-8"), zone
            reports.append(raw)
    assert reports[0] == reports[1]


def test_report_markdown_keeps_forged_text_inside_one_span(spark, tmp_path):
    payload = "y\n\n## Result\n\nStatus: Match\n| a | b |\n"
    key = "id`|1"
    schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("note", StringType(), True),
        ]
    )
    source = tmp_path / "source"
    target = tmp_path / "target"
    _write_frame(spark, source, [(key, payload)], schema)
    _write_frame(spark, target, [(key, "ok")], schema)
    data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    report, _raw, markdown = _load(out)
    example = _changed(report)["details"]["columns"][0]["examples"][0]
    assert example["source"] == payload
    assert example["key"] == [key]
    lines = markdown.splitlines()
    matched = sum(1 for check in report["checks"] if check["status"] == "match")
    mismatched = sum(1 for check in report["checks"] if check["status"] == "mismatch")
    assert lines.count("## Result") == 1
    assert lines.count("Status: Match") == matched
    assert lines.count("Status: Mismatch") == mismatched + 1
    assert all(not line.startswith("|") for line in lines)
    assert r"y\n\n## Result\n\nStatus: Match\n\| a \| b \|\n" in markdown
    assert "``id`\\|1``" in markdown
    assert all(not line.startswith("## Result") or line == "## Result" for line in lines)


def _note_schema(amount):
    return StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("amount", amount, True),
            StructField("day", StringType(), True),
            StructField("note", StringType(), True),
        ]
    )


def test_report_markdown_escapes_nested_field_names_in_type_mismatches(spark, tmp_path):
    forged = "a\n\n## Forged\n\nStatus: Match\n<b>x</b>"
    source_schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("note", StructType([StructField(forged, LongType(), True)]), True),
        ]
    )
    target_schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("note", StringType(), True),
        ]
    )
    source = tmp_path / "source"
    target = tmp_path / "target"
    _write_frame(spark, source, [("1", (1,))], source_schema)
    _write_frame(spark, target, [("1", "x")], target_schema)
    data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
    data.update(amounts=[], group_by=[], hash_columns=["note"], keys=["txn_id"])
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    assert _invoke(spec, out) is ExitCode.MISMATCH
    report, _raw, markdown = _load(out)
    mismatch = _key_diff(report)["details"]["type_mismatches"][0]
    assert mismatch["source_type"] == f"struct<{forged}:bigint>"
    lines = markdown.splitlines()
    assert "## Forged" not in lines
    assert lines.count("Status: Match") == sum(
        1 for check in report["checks"] if check["status"] == "match"
    )
    assert all(not line.startswith("<b>") for line in lines)
    assert (
        r"Source type: `struct<a\n\n## Forged\n\nStatus: Match\n<b>x</b>:bigint>`. "
        "Target type: `string`."
    ) in markdown


def test_amount_cell_of_only_a_line_break_or_unicode_space_is_refused(spark, tmp_path):
    source_schema = _note_schema(StringType())
    target_schema = _note_schema(DecimalType(12, 2))
    for label, cell in (("lf", "\n"), ("crlf", "\r\n"), ("nbsp", "\u00a0"), ("ls", "\u2028")):
        source = tmp_path / label / "source"
        target = tmp_path / label / "target"
        _write_frame(spark, source, [("1", cell, "d", "a")], source_schema)
        _write_frame(spark, target, [("1", None, "d", "a")], target_schema)
        data = spec_data(tmp_path, source, target, source_format="parquet", target_format="parquet")
        data.update(group_by=[], hash_columns=["note"], keys=["txn_id"])
        spec = write_spec_file(tmp_path / label, data)
        out = tmp_path / label / "out"
        assert _invoke(spec, out) is ExitCode.REFUSED, label
        report = _load(out)[0]
        assert report["status"] == "refused", label
        assert report["refusals"][0]["code"] == "inexact-amount", label


def _typed_target(spark, path, rows, group_type):
    schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("amount", DecimalType(12, 2), True),
            StructField("day", group_type, True),
            StructField("note", StringType(), True),
        ]
    )
    _write_frame(spark, path, rows, schema)


def _group_run(tmp_path, source_path, target_path, source_format, target_format, allow=None):
    data = spec_data(
        tmp_path,
        source_path,
        target_path,
        source_format=source_format,
        target_format=target_format,
    )
    if allow is not None:
        data["allowlist"] = [str(allow)]
    spec = write_spec_file(tmp_path, data)
    out = tmp_path / "out"
    code = _invoke(spec, out)
    report, _raw, markdown = _load(out)
    return code, report, markdown


def test_csv_group_text_matches_a_typed_parquet_group(spark, tmp_path):
    # A raw CSV day is a string. The curated Parquet day is a date. They are
    # the same group, so a correct load is a match.
    cases = {
        "date": (
            DateType(),
            "1,10.50,2024-01-01,a\n2,1.25,2024-01-01,b\n3,5.00,2024-01-02,c\n",
            [date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 2)],
            [{"day": "2024-01-01"}, {"day": "2024-01-02"}],
        ),
        "long": (
            LongType(),
            "1,10.50,7,a\n2,1.25,7,b\n3,5.00,9,c\n",
            [7, 7, 9],
            [{"day": 7}, {"day": 9}],
        ),
        "decimal": (
            DecimalType(10, 2),
            "1,10.50,1.5,a\n2,1.25,1.50,b\n3,5.00,2,c\n",
            [Decimal("1.50"), Decimal("1.50"), Decimal("2.00")],
            [{"day": "1.50"}, {"day": "2.00"}],
        ),
        "timestamp": (
            TimestampType(),
            "1,10.50,2024-01-01 10:00:00,a\n2,1.25,2024-01-01 10:00:00,b\n"
            "3,5.00,2024-01-02 00:00:00,c\n",
            [_utc(2024, 1, 1, 10), _utc(2024, 1, 1, 10), _utc(2024, 1, 2)],
            [{"day": "2024-01-01T10:00:00.000000Z"}, {"day": "2024-01-02T00:00:00.000000Z"}],
        ),
    }
    notes = ["a", "b", "c"]
    amounts = [Decimal("10.50"), Decimal("1.25"), Decimal("5.00")]
    for name, (group_type, body, values, expected) in cases.items():
        case = tmp_path / name
        source = case / "source.csv"
        target = case / "target"
        write_csv(source, body)
        rows = [(str(index + 1), amounts[index], values[index], notes[index]) for index in range(3)]
        _typed_target(spark, target, rows, group_type)
        for flipped in (False, True):
            run_dir = case / ("flipped" if flipped else "forward")
            run_dir.mkdir()
            if flipped:
                code, report, _md = _group_run(
                    run_dir, target, source, "parquet", "csv", allow=case
                )
            else:
                code, report, _md = _group_run(
                    run_dir, source, target, "csv", "parquet", allow=case
                )
            assert code is ExitCode.MATCH, (name, flipped)
            row_count = report["checks"][0]
            assert row_count["status"] == "match", (name, flipped)
            groups = [group["group"] for group in row_count["details"]["groups"]]
            assert groups == expected, (name, flipped)
            amount_sum = report["checks"][1]
            assert amount_sum["status"] == "match", (name, flipped)


def test_csv_group_text_that_does_not_parse_stays_its_own_group(spark, tmp_path):
    source = tmp_path / "source.csv"
    target = tmp_path / "target"
    write_csv(source, "1,10.50,2024-01-01,a\n2,1.25,not-a-day,b\n")
    _typed_target(
        spark,
        target,
        [
            ("1", Decimal("10.50"), date(2024, 1, 1), "a"),
            ("2", Decimal("1.25"), date(2024, 1, 2), "b"),
        ],
        DateType(),
    )
    code, report, markdown = _group_run(tmp_path, source, target, "csv", "parquet")
    assert code is ExitCode.MISMATCH
    row_count = report["checks"][0]
    assert row_count["status"] == "mismatch"
    groups = {
        group["group"]["day"]: (group["source"], group["target"])
        for group in row_count["details"]["groups"]
    }
    assert groups == {"2024-01-01": (1, 1), "2024-01-02": (0, 1), "not-a-day": (1, 0)}
    assert "`day`=`not-a-day`" in markdown
