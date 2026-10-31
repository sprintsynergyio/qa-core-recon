"""Key diff, row hashes, and changed columns. Fixtures live in tmp_path."""

import json
import os
import subprocess
import sys
from datetime import date, datetime
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
    TimestampType,
)

from qa_core_recon.compare import KEY_DIFF_EXAMPLE_LIMIT
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
    assert "Compared columns, in hash order: amount, day, note." in markdown
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
    assert "txn_id=2" in markdown


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
    assert "Column note. Distinct keys that differ: 1." in markdown
    assert "Source: a. Target: b." in markdown
    assert f"Source token: {encode_value('a')}." in markdown


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
    assert "txn_id=1, line=2" in markdown
    assert "txn_id=1, line=3" in markdown


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
    moment = datetime(2024, 1, 2, 3, 4, 5)
    later = datetime(2024, 1, 2, 3, 4, 6)
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
    _write_frame(spark, source, [("1", datetime(2024, 11, 3, 5, 30))], schema)
    _write_frame(spark, target, [("1", datetime(2024, 11, 3, 6, 30))], schema)
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
        [("1", 5, Decimal("1.50"), date(2024, 1, 1), datetime(2024, 1, 1, 5, 30))],
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
        [("1", 5, Decimal("1.50"), date(2024, 1, 1), datetime(2024, 1, 1, 5, 30))],
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
    assert "Source type: float. Target type: double." in markdown


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


def _note_schema(amount):
    return StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("amount", amount, True),
            StructField("day", StringType(), True),
            StructField("note", StringType(), True),
        ]
    )
