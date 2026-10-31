from dataclasses import replace
from decimal import Decimal

import pytest
import yaml
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from qa_core_recon.readers import ReadError, check_data, read_datasets
from qa_core_recon.refusals import RefusalCode, RefusalError
from qa_core_recon.spec import Dataset
from tests.support import HEADER, parse_data, spec_data, write_csv

ROW = "1,10.50,2024-01-01,a\n"


def _pair(tmp_path, body: str = ROW, header: str = HEADER):
    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    write_csv(source, body, header)
    write_csv(target, body, header)
    return source, target


def _spec(tmp_path, source, target, **formats):
    return parse_data(spec_data(tmp_path, source, target, **formats))


def _refusal(spark, spec):
    with pytest.raises(RefusalError) as caught:
        check_data(spark, spec)
    return caught.value.refusal


def _write_parquet(spark, path, rows, amount_type):
    schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("amount", amount_type, True),
            StructField("day", StringType(), True),
            StructField("note", StringType(), True),
        ]
    )
    spark.createDataFrame(rows, schema).write.mode("overwrite").parquet(str(path))


def _amounts(frame):
    return {row["txn_id"]: row["amount"] for row in frame.select("txn_id", "amount").collect()}


def test_csv_amounts_are_exact_decimals_and_other_columns_stay_strings(spark, tmp_path):
    header = "txn_id,amount,day,note,code\n"
    body = "1,10.10,2024-01-01,a,001\n2,10.5,2024-01-01,b,002\n"
    source, target = _pair(tmp_path, body, header)
    spec = _spec(tmp_path, source, target, target_format="csv")
    loaded_source, loaded_target = read_datasets(spark, spec)
    amount = loaded_source.schema["amount"].dataType
    assert isinstance(amount, DecimalType)
    assert amount.precision == 38
    assert amount.scale == 2
    assert _amounts(loaded_source) == {"1": Decimal("10.10"), "2": Decimal("10.50")}
    codes = {row["txn_id"]: row["code"] for row in loaded_source.select("txn_id", "code").collect()}
    assert codes == {"1": "001", "2": "002"}
    assert loaded_source.schema["code"].dataType == StringType()
    assert _amounts(loaded_target)["1"] == Decimal("10.10")


def test_inexact_value_is_independent_of_row_order(spark, tmp_path):
    target = tmp_path / "target.csv"
    write_csv(target, ROW)
    low_first = tmp_path / "low-first.csv"
    high_first = tmp_path / "high-first.csv"
    write_csv(low_first, "1,10.005,2024-01-01,a\n2,9.001,2024-01-01,b\n")
    write_csv(high_first, "1,9.001,2024-01-01,a\n2,10.005,2024-01-01,b\n")
    first = _refusal(spark, _spec(tmp_path, low_first, target, target_format="csv"))
    second = _refusal(spark, _spec(tmp_path, high_first, target, target_format="csv"))
    assert first == second
    assert first.code is RefusalCode.INEXACT_AMOUNT
    assert first.message == (
        "source amount column 'amount' value '10.005' is not an exact decimal at scale 2"
    )


def test_csv_bom_does_not_rename_the_first_column(spark, tmp_path):
    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    text = HEADER + ROW
    source.write_bytes(b"\xef\xbb\xbf" + text.encode("utf-8"))
    target.write_text(text, encoding="utf-8")
    check_data(spark, _spec(tmp_path, source, target, target_format="csv"))


def test_short_csv_row_is_a_read_error(spark, tmp_path):
    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    write_csv(source, "1,10.00\n")
    write_csv(target, ROW)
    with pytest.raises(ReadError) as caught:
        check_data(spark, _spec(tmp_path, source, target, target_format="csv"))
    assert str(caught.value) == "source csv has a malformed row"


def test_multiline_csv_field_is_a_read_error(spark, tmp_path):
    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    source.write_text('txn_id,amount,day,note\n1,10.00,"hello\nworld",a\n', encoding="utf-8")
    write_csv(target, ROW)
    with pytest.raises(ReadError) as caught:
        check_data(spark, _spec(tmp_path, source, target, target_format="csv"))
    assert str(caught.value) == "source csv has a malformed row"


def test_csv_value_that_does_not_fit_the_scale_is_refused(spark, tmp_path):
    source, target = _pair(tmp_path, "1,10.005,2024-01-01,a\n")
    spec = _spec(tmp_path, source, target, target_format="csv")
    first = _refusal(spark, spec)
    second = _refusal(spark, spec)
    assert first == second
    assert first.code is RefusalCode.INEXACT_AMOUNT
    assert first.message == (
        "source amount column 'amount' value '10.005' is not an exact decimal at scale 2"
    )


def test_cast_rule_does_not_round_a_csv_amount(spark, tmp_path):
    source, target = _pair(tmp_path, "1,10.005,2024-01-01,a\n")
    data = spec_data(tmp_path, source, target, target_format="csv")
    data["amounts"][0]["cast"] = "half-up"
    refusal = _refusal(spark, parse_data(data))
    assert refusal.code is RefusalCode.INEXACT_AMOUNT
    assert "10.005" in refusal.message


def test_blank_amount_is_null(spark, tmp_path):
    source, target = _pair(tmp_path, "1,,2024-01-01,a\n")
    spec = _spec(tmp_path, source, target, target_format="csv")
    loaded, _target = read_datasets(spark, spec)
    assert _amounts(loaded) == {"1": None}


def test_different_amounts_are_not_compared(spark, tmp_path):
    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    write_csv(source, "1,1.00,2024-01-01,a\n")
    write_csv(target, "1,99.00,2024-01-01,a\n")
    spec = _spec(tmp_path, source, target, target_format="csv")
    check_data(spark, spec)
    loaded_source, loaded_target = read_datasets(spark, spec)
    assert _amounts(loaded_source)["1"] == Decimal("1.00")
    assert _amounts(loaded_target)["1"] == Decimal("99.00")


def test_reader_returns_every_row(spark, tmp_path):
    body = "".join(f"{index},1.00,2024-01-01,a\n" for index in range(3))
    source, target = _pair(tmp_path, body)
    spec = _spec(tmp_path, source, target, target_format="csv")
    loaded, _other = read_datasets(spark, spec)
    assert loaded.count() == 3


def test_csv_directory_skips_spark_marker_files(spark, tmp_path):
    directory = tmp_path / "parts"
    directory.mkdir()
    (directory / "_SUCCESS").write_text("", encoding="utf-8")
    write_csv(directory / "part-00000.csv", "1,10.00,2024-01-01,a\n")
    write_csv(directory / "part-00001.csv", "2,11.00,2024-01-01,b\n")
    target = tmp_path / "target.csv"
    write_csv(target, ROW)
    spec = _spec(tmp_path, directory, target, target_format="csv")
    loaded, _target = read_datasets(spark, spec)
    assert _amounts(loaded) == {"1": Decimal("10.00"), "2": Decimal("11.00")}


def test_csv_directory_headers_must_match(spark, tmp_path):
    directory = tmp_path / "parts"
    directory.mkdir()
    write_csv(directory / "a.csv", "1,10.00,2024-01-01,a\n")
    write_csv(directory / "b.csv", "1,10.00,2024-01-01,a\n", header="txn_id,amount,day,other\n")
    target = tmp_path / "target.csv"
    write_csv(target, ROW)
    spec = _spec(tmp_path, directory, target, target_format="csv")
    with pytest.raises(ReadError) as caught:
        check_data(spark, spec)
    assert str(caught.value) == "source csv header in 'b.csv' does not match 'a.csv'"


def test_parquet_decimal_is_exact_and_extra_scale_is_refused(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, ROW)
    exact = tmp_path / "exact"
    _write_parquet(spark, exact, [("1", Decimal("10.50"), "2024-01-01", "a")], DecimalType(10, 2))
    spec = _spec(tmp_path, source, exact)
    _source, target = read_datasets(spark, spec)
    assert target.schema["amount"].dataType == DecimalType(38, 2)
    assert _amounts(target)["1"] == Decimal("10.50")

    wider = tmp_path / "wider"
    _write_parquet(spark, wider, [("1", Decimal("10.005"), "2024-01-01", "a")], DecimalType(10, 3))
    data = spec_data(tmp_path, source, wider)
    data["amounts"][0]["cast"] = "half-even"
    refusal = _refusal(spark, parse_data(data))
    assert refusal.code is RefusalCode.INEXACT_AMOUNT
    assert refusal.message == (
        "target amount column 'amount' value '10.005' is not an exact decimal at scale 2"
    )


def test_integer_parquet_amount_is_a_whole_unit(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, "1,1050.00,2024-01-01,a\n")
    target = tmp_path / "target"
    _write_parquet(spark, target, [("1", 1050, "2024-01-01", "a")], LongType())
    spec = _spec(tmp_path, source, target)
    _source, loaded = read_datasets(spark, spec)
    assert _amounts(loaded)["1"] == Decimal("1050.00")


def test_float_amount_names_side_and_type(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, ROW)
    doubled = tmp_path / "doubled"
    floated = tmp_path / "floated"
    _write_parquet(spark, doubled, [("1", 2.5, "2024-01-01", "a")], DoubleType())
    _write_parquet(spark, floated, [("1", 2.5, "2024-01-01", "a")], FloatType())
    double_refusal = _refusal(spark, _spec(tmp_path, source, doubled))
    float_refusal = _refusal(spark, _spec(tmp_path, source, floated))
    assert double_refusal.code is RefusalCode.FLOAT_AMOUNT
    assert double_refusal.message == (
        "target amount column 'amount' is double and the spec has no cast rule"
    )
    assert float_refusal.code is RefusalCode.FLOAT_AMOUNT
    assert "is float " in float_refusal.message


def test_float_cast_is_applied_from_the_spec(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, "1,2,2024-01-01,a\n", header=HEADER)
    target = tmp_path / "target"
    _write_parquet(spark, target, [("1", 2.5, "2024-01-01", "a")], DoubleType())
    for mode, expected in (("half-even", Decimal("2")), ("half-up", Decimal("3"))):
        data = spec_data(tmp_path, source, target)
        data["amounts"] = [{"column": "amount", "scale": 0, "cast": mode}]
        _source, loaded = read_datasets(spark, parse_data(data))
        assert loaded.schema["amount"].dataType == DecimalType(38, 0)
        assert _amounts(loaded)["1"] == expected


def test_non_finite_float_is_inexact_with_a_cast_rule(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, ROW)
    target = tmp_path / "target"
    _write_parquet(spark, target, [("1", float("nan"), "2024-01-01", "a")], DoubleType())
    data = spec_data(tmp_path, source, target)
    data["amounts"][0]["cast"] = "half-even"
    refusal = _refusal(spark, parse_data(data))
    assert refusal.code is RefusalCode.INEXACT_AMOUNT
    assert "NaN" in refusal.message


def test_boolean_amount_cannot_be_read_as_a_decimal(spark, tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, ROW)
    target = tmp_path / "target"
    _write_parquet(spark, target, [("1", True, "2024-01-01", "a")], BooleanType())
    refusal = _refusal(spark, _spec(tmp_path, source, target))
    assert refusal.code is RefusalCode.INEXACT_AMOUNT
    assert refusal.message == (
        "target amount column 'amount' has type boolean, which is not a number type"
    )


@pytest.mark.parametrize(
    ("header", "body", "column"),
    [
        ("amount,day,note\n", "10.00,2024-01-01,a\n", "txn_id"),
        ("txn_id,day,note\n", "1,2024-01-01,a\n", "amount"),
        ("txn_id,amount,note\n", "1,10.00,a\n", "day"),
        ("txn_id,amount,day\n", "1,10.00,2024-01-01\n", "note"),
    ],
)
def test_unmapped_column_names_the_side_and_the_column(spark, tmp_path, header, body, column):
    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    write_csv(source, body, header)
    write_csv(target, ROW)
    refusal = _refusal(spark, _spec(tmp_path, source, target, target_format="csv"))
    assert refusal.code is RefusalCode.UNMAPPED_COLUMN
    assert refusal.message == f"source is missing column {column!r}"


def test_unmapped_column_checks_source_before_target(spark, tmp_path):
    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    write_csv(source, "10.00,2024-01-01,a\n", header="amount,day,note\n")
    write_csv(target, "1,10.00,a\n", header="txn_id,amount,note\n")
    refusal = _refusal(spark, _spec(tmp_path, source, target, target_format="csv"))
    assert refusal.message == "source is missing column 'txn_id'"


def test_unmapped_column_checks_roles_in_spec_order(spark, tmp_path):
    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    write_csv(source, "1,10.00\n", header="txn_id,amount\n")
    write_csv(target, ROW)
    refusal = _refusal(spark, _spec(tmp_path, source, target, target_format="csv"))
    assert refusal.message == "source is missing column 'day'"


def test_column_match_is_case_sensitive(spark, tmp_path):
    source = tmp_path / "source.csv"
    target = tmp_path / "target.csv"
    write_csv(source, "1,10.00,2024-01-01,a\n", header="Txn_Id,amount,day,note\n")
    write_csv(target, ROW)
    refusal = _refusal(spark, _spec(tmp_path, source, target, target_format="csv"))
    assert refusal.message == "source is missing column 'txn_id'"


def test_sample_only_is_refused_before_the_files_are_opened(spark, tmp_path):
    source = tmp_path / "missing-source.csv"
    target = tmp_path / "missing-target.csv"
    data = spec_data(tmp_path, source, target, target_format="csv")
    data["sampled"] = True
    data["amounts"] = [{"column": "amount", "scale": 2}]
    first = _refusal(spark, parse_data(data))
    second = _refusal(spark, parse_data(data))
    assert first == second
    assert first.code is RefusalCode.SAMPLE_ONLY
    assert first.message == "the spec declares sampling, so a full recon cannot be claimed"


def test_data_refusal_order_is_stable(spark, tmp_path):
    source = tmp_path / "source.csv"
    target = tmp_path / "target"
    write_csv(source, "1,10.005,2024-01-01,a\n9,9.001,2024-01-01,b\n")
    _write_parquet(spark, target, [("1", 1.25, "2024-01-01", "a")], DoubleType())
    both = spec_data(tmp_path, source, target)
    both["sampled"] = True
    sampled = _refusal(spark, parse_data(both))
    assert sampled.code is RefusalCode.SAMPLE_ONLY

    missing = spec_data(tmp_path, source, target)
    missing["hash_columns"] = ["note", "absent"]
    unmapped = _refusal(spark, parse_data(missing))
    assert unmapped.code is RefusalCode.UNMAPPED_COLUMN
    assert unmapped.message == "source is missing column 'absent'"

    floated = _refusal(spark, parse_data(spec_data(tmp_path, source, target)))
    assert floated.code is RefusalCode.FLOAT_AMOUNT
    assert floated.message.startswith("target amount column 'amount' is double")

    exact_target = tmp_path / "exact.csv"
    write_csv(exact_target, ROW)
    inexact = _refusal(spark, _spec(tmp_path, source, exact_target, target_format="csv"))
    again = _refusal(spark, _spec(tmp_path, source, exact_target, target_format="csv"))
    assert inexact == again
    assert inexact.code is RefusalCode.INEXACT_AMOUNT
    assert "value '10.005'" in inexact.message
    assert "9.001" not in inexact.message


def test_local_csv_directory_rejects_a_glob_file_name(spark, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "a[1].csv").write_text(HEADER + ROW, encoding="utf-8")
    target = tmp_path / "target.csv"
    write_csv(target, ROW)
    spec = _spec(tmp_path, source, target, target_format="csv")
    with pytest.raises(ReadError) as caught:
        check_data(spark, spec)
    assert str(caught.value) == "source csv object 'a[1].csv' contains glob character '['"


def test_reader_rechecks_the_allowlist_before_opening(spark, tmp_path):
    source, target = _pair(tmp_path)
    spec = _spec(tmp_path, source, target, target_format="csv")
    outside = tmp_path.parent / "outside-the-allowlist.csv"
    spec = replace(spec, source=Dataset(path=str(outside), format="csv"))
    refusal = _refusal(spark, spec)
    assert refusal.code is RefusalCode.PATH_NOT_ALLOWED
    assert "not found" not in refusal.message


def test_allowlisted_missing_s3_bucket_is_a_read_error(spark, tmp_path):
    source, _target = _pair(tmp_path)
    data = spec_data(tmp_path, source, tmp_path / "unused.csv", target_format="parquet")
    data["target"] = {"path": "s3://demo-bucket/curated/payments", "format": "parquet"}
    data["allowlist"] = [str(tmp_path), "s3://demo-bucket"]
    with pytest.raises(ReadError) as caught:
        check_data(spark, parse_data(data))
    assert str(caught.value) == "target bucket not found: s3://demo-bucket/curated/payments"
    assert "Traceback" not in str(caught.value)

    data["source"] = {"path": "s3://demo-bucket/raw/payments.csv", "format": "csv"}
    with pytest.raises(ReadError) as caught:
        check_data(spark, parse_data(data))
    assert str(caught.value) == "source bucket not found: s3://demo-bucket/raw/payments.csv"


def test_s3_outside_the_allowlist_stays_path_not_allowed(spark, tmp_path):
    source, target = _pair(tmp_path)
    spec = replace(
        _spec(tmp_path, source, target),
        target=Dataset(path="s3://other-bucket/file.parquet", format="parquet"),
    )
    refusal = _refusal(spark, spec)
    assert refusal.code is RefusalCode.PATH_NOT_ALLOWED


def test_missing_and_unreadable_files_are_read_errors(spark, tmp_path):
    target = tmp_path / "target.csv"
    write_csv(target, ROW)
    missing = tmp_path / "missing.csv"
    spec = _spec(tmp_path, missing, target, target_format="csv")
    with pytest.raises(ReadError) as caught:
        check_data(spark, spec)
    assert str(caught.value) == f"source path not found: {missing}"

    binary = tmp_path / "source.csv"
    binary.write_bytes(b"\xff")
    with pytest.raises(ReadError) as caught:
        check_data(spark, _spec(tmp_path, binary, target, target_format="csv"))
    assert str(caught.value) == "source csv is not valid UTF-8"

    bad_parquet = tmp_path / "target.parquet"
    bad_parquet.write_text("not parquet", encoding="utf-8")
    write_csv(binary, ROW)
    with pytest.raises(ReadError) as caught:
        check_data(spark, _spec(tmp_path, binary, bad_parquet))
    assert str(caught.value) == "cannot read target as parquet"


def test_symlink_loop_is_a_read_error(spark, tmp_path):
    target = tmp_path / "target.csv"
    write_csv(target, ROW)
    loop = tmp_path / "loop"
    loop.symlink_to(loop)
    with pytest.raises(ReadError) as caught:
        check_data(spark, _spec(tmp_path, loop, target, target_format="csv"))
    assert str(caught.value) == f"cannot read source path '{loop}': Symlink loop from '{loop}'"


def test_read_does_not_modify_the_source_file(spark, tmp_path):
    source, target = _pair(tmp_path)
    before = source.read_bytes()
    check_data(spark, _spec(tmp_path, source, target, target_format="csv"))
    assert source.read_bytes() == before


def test_spec_round_trip_uses_real_yaml(tmp_path):
    source, target = _pair(tmp_path)
    data = spec_data(tmp_path, source, target)
    text = yaml.safe_dump(data)
    assert "sampled:" in text
    assert parse_data(data).sampled is False
