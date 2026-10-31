"""Canonical row-hash encoding. These cases do not decide a dataset match."""

import hashlib
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

from pyspark.sql.functions import col, concat, lit, sha2, udf
from pyspark.sql.types import StringType

from qa_core_recon.hashing import (
    compared_columns,
    encode_row,
    encode_value,
    plain_decimal_text,
    row_hash_hex,
    strict_decimal_payload,
)
from qa_core_recon.spec import Amount, Dataset, Spec, Tolerance


def _spec(**overrides) -> Spec:
    values = {
        "version": 1,
        "source": Dataset(path="source.csv", format="csv"),
        "target": Dataset(path="target.csv", format="csv"),
        "keys": ("txn_id",),
        "amounts": (Amount(column="amount", scale=2),),
        "group_by": ("day",),
        "hash_columns": ("note",),
        "tolerance": Tolerance(value=Decimal("0"), reason=""),
        "sampled": False,
        "allowlist": ("./data",),
    }
    for name, value in overrides.items():
        if isinstance(value, list):
            value = tuple(value)
        values[name] = value
    return Spec(**values)


def test_null_is_distinct_from_the_empty_string():
    assert encode_value(None) == "n"
    assert encode_value("") == "string:0:"
    assert encode_value(None) != encode_value("")
    assert row_hash_hex((None,)) != row_hash_hex(("",))
    assert row_hash_hex((None, "c")) != row_hash_hex(("", "c"))


def test_length_prefix_blocks_boundary_collisions():
    assert row_hash_hex(("a,b", "c")) != row_hash_hex(("a", "b,c"))
    assert encode_row(("a,b", "c")) == "string:3:a,bstring:1:c"
    assert encode_row(("a", "b,c")) == "string:1:astring:3:b,c"
    assert row_hash_hex(("é", "a")) != row_hash_hex(("éa",))
    assert encode_value("é") == "string:2:é"


def test_decimals_compare_by_value_and_keep_significant_digits():
    assert encode_value(Decimal("1.0")) == encode_value(Decimal("1.00"))
    assert encode_value(Decimal("1")) == "decimal:1:1"
    assert encode_value(Decimal("1.10")) == encode_value(Decimal("1.1"))
    assert encode_value(Decimal("10.50")) == "decimal:4:10.5"
    assert encode_value(Decimal("-0.00")) == "decimal:1:0"
    assert encode_value(Decimal("100.00")) == "decimal:3:100"
    assert row_hash_hex((Decimal("1.0"),)) == row_hash_hex((Decimal("1.00"),))
    assert row_hash_hex((Decimal("1.0"),)) != row_hash_hex((Decimal("1.01"),))
    assert plain_decimal_text(Decimal(0), 10) == "0.0000000000"
    assert plain_decimal_text(Decimal("1E-7"), 10) == "0.0000001000"
    assert plain_decimal_text(Decimal("-1.5"), 10) == "-1.5000000000"
    assert "E" not in plain_decimal_text(Decimal("0E-10"), 10)
    assert strict_decimal_payload("1.50", 10, 2) == "1.5"
    assert strict_decimal_payload("1.001", 10, 2) is None
    assert strict_decimal_payload("01", 10, 0) == "1"
    wide = Decimal("9" * 36 + ".99")
    assert encode_value(wide) == "decimal:39:" + "9" * 36 + ".99"


def test_floats_normalise_negative_zero_and_every_nan():
    assert encode_value(-0.0) == encode_value(0.0) == "float:3:0.0"
    assert encode_value(float("nan")) == encode_value(float("-nan")) == "float:3:NaN"
    assert encode_value(float("inf")) == "float:8:Infinity"
    assert encode_value(float("-inf")) == "float:9:-Infinity"
    assert row_hash_hex((-0.0,)) == row_hash_hex((0.0,))
    assert row_hash_hex((float("nan"),)) == row_hash_hex((float("-nan"),))
    assert row_hash_hex((float("nan"),)) != row_hash_hex((0.0,))


def test_timestamps_dates_and_booleans_use_a_fixed_form():
    naive = datetime(2024, 1, 2, 3, 4, 5, 6)
    utc = datetime(2024, 1, 2, 3, 4, 5, 6, tzinfo=UTC)
    ahead = datetime(2024, 1, 2, 4, 4, 5, 6, tzinfo=timezone(timedelta(hours=1)))
    token = "timestamp:27:2024-01-02T03:04:05.000006Z"
    assert encode_value(naive) == token
    assert encode_value(utc) == token
    assert encode_value(ahead) == token
    assert encode_value(date(2024, 1, 2)) == "date:10:2024-01-02"
    assert encode_value(True) == "bool:4:true"
    assert encode_value(False) == "bool:5:false"
    assert encode_value(1) == "int:1:1"
    assert encode_value(True) != encode_value(1)
    assert encode_value(b"\x00\xff") == "bytes:4:00ff"


def test_compared_columns_are_sorted_and_skip_keys():
    spec = _spec(hash_columns=["note", "alpha"], keys=["txn_id"])
    assert compared_columns(spec) == ("alpha", "amount", "day", "note")
    keyed_amount = _spec(keys=["amount"], hash_columns=["note"])
    assert compared_columns(keyed_amount) == ("day", "note")
    assert compared_columns(_spec(amounts=[], hash_columns=[], group_by=["day"])) == ("day",)
    assert compared_columns(_spec(amounts=[], hash_columns=[], group_by=[])) == ()


def test_spark_sha256_matches_the_python_digest(spark):
    encode = udf(encode_value, StringType())
    rows_in = [("a,b", "c"), ("a", "b,c"), (None, ""), ("", None)]
    frame = spark.createDataFrame(rows_in, ["left", "right"])
    digest = sha2(concat(encode(col("left")), encode(col("right"))), 256)
    rows = frame.select(col("left"), col("right"), digest.alias("hash")).collect()
    assert rows
    for row in rows:
        expected = row_hash_hex((row["left"], row["right"]))
        assert row["hash"] == expected
    empty = spark.range(1).select(sha2(lit(""), 256).alias("hash")).first()[0]
    assert empty == hashlib.sha256(b"").hexdigest()
