"""Key diff, row hashes, and changed columns. Fixtures live in tmp_path."""

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from qa_core_recon.compare import KEY_DIFF_EXAMPLE_LIMIT
from qa_core_recon.exit_codes import ExitCode
from qa_core_recon.hashing import encode_value, row_hash_hex
from tests.support import HEADER, spec_data, write_csv, write_spec_file
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
    assert details["columns"] == ["amount", "note"]
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
    assert "Compared columns, in hash order: amount, note." in markdown
    assert "No compared column differs." in markdown
    assert "day" not in details["columns"]


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
        {"key": ["1"], "source": encode_value("a"), "target": encode_value("b")}
    ]
    assert "Column note. Distinct keys that differ: 1." in markdown
    assert f"Source: {encode_value('a')}. Target: {encode_value('b')}." in markdown


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
        {"key": ["2"], "source": "n", "target": encode_value("x")},
        {"key": ["3"], "source": encode_value(""), "target": "n"},
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
    assert column["examples"][0]["source"] == encode_value(float("nan"))
    assert column["examples"][0]["target"] == encode_value(1.0)


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
    assert example["source"] == encode_value(Decimal("1.0"))
    assert example["target"] == encode_value(Decimal("1.10"))
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
    assert columns["flag"]["examples"][0]["source"] == encode_value(True)
    assert columns["flag"]["examples"][0]["target"] == encode_value(False)
    assert columns["instant"]["examples"][0]["source"] == encode_value(moment)
    assert columns["instant"]["examples"][0]["target"] == encode_value(later)


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
        "source": encode_value(winner),
        "target": encode_value(loser),
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
    assert "No compared columns were declared." in _markdown if False else True
    assert "No compared columns were declared." in _markdown
    assert report["checks"][4]["status"] == "mismatch"


def _note_schema(amount):
    return StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("amount", amount, True),
            StructField("day", StringType(), True),
            StructField("note", StringType(), True),
        ]
    )
