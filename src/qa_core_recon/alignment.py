"""Align source and target columns before the key diff joins or hashes them.

Spark's own join casts a string key ``01`` to the long ``1``. That is a false
match. This module gives every key a string join key, and every compared
column a canonical token, using the rules below. Schemas are read on the
driver. Values stay in Spark.

* Integer and decimal columns compare by numeric value. Both tokens use the
  decimal payload, so ``1`` and ``1.00`` match.
* A string paired with a typed column is parsed with ``try_cast``. A non-null
  string that does not parse becomes an ``invalid:`` token, not null, so the
  row is changed. Decimal strings that would have to be rounded do not parse.
  Date strings must already be ``YYYY-MM-DD``.
* Key columns use a lossless parse. ``01`` does not round-trip to ``1``, so
  the string key stays unmatched and the long key stays unmatched.
* Any other pair, including float against double, is not given a new refusal
  code. The column is listed in ``type_mismatches`` and both sides are hashed
  as their string rendering.
* ``TimestampType`` is formatted with ``date_format`` under the UTC session
  before any Python code sees it. ``TimestampNTZType`` uses the same pattern
  on the stored wall time and is not shifted into the session zone.
"""

from dataclasses import dataclass

from pyspark.sql.column import Column
from pyspark.sql.functions import (
    col,
    concat,
    date_format,
    encode,
    expr,
    length,
    lit,
    trim,
    udf,
    when,
)
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NullType,
    ShortType,
    StringType,
    TimestampNTZType,
    TimestampType,
)
from pyspark.sql.types import StringType as _StringType

from qa_core_recon.hashing import encode_value, strict_decimal_payload

_INTEGER_TYPES = (ByteType, ShortType, IntegerType, LongType)
_TIMESTAMP_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"
# Fixed width of that pattern. Every character is ASCII.
_TIMESTAMP_WIDTH = 27

_ENCODE = udf(encode_value, _StringType())


def _strict_decimal_udf(text, precision, scale):
    if text is None or precision is None or scale is None:
        return None
    return strict_decimal_payload(str(text), int(precision), int(scale))


_DECIMAL_PAYLOAD = udf(_strict_decimal_udf, _StringType())


@dataclass(frozen=True)
class FieldPlan:
    """How one named field is aligned. Built from schemas only."""

    name: str
    role: str
    kind: str
    source_type: DataType
    target_type: DataType
    string_side: str | None = None

    @property
    def mismatch(self) -> dict | None:
        if self.kind != "render":
            return None
        return {
            "column": self.name,
            "role": self.role,
            "source_type": self.source_type.simpleString(),
            "target_type": self.target_type.simpleString(),
        }


def plan_fields(
    source_fields: dict[str, DataType], target_fields: dict[str, DataType], names, role: str
):
    """Return one plan per name, in the given order."""
    plans = []
    for name in names:
        plans.append(_plan(name, role, source_fields[name], target_fields[name]))
    return tuple(plans)


def type_mismatches(plans: tuple[FieldPlan, ...]) -> list[dict]:
    """Return the unalignable columns, sorted by role then name."""
    found = [plan.mismatch for plan in plans if plan.mismatch is not None]
    found.sort(key=lambda item: (item["role"], item["column"]))
    return found


def join_key(plan: FieldPlan, side: str) -> Column:
    """Return the string used to group and join this key. Null stays null."""
    dtype = _dtype(plan, side)
    column = _column(plan.name)
    if plan.kind == "parse" and plan.string_side == side:
        return _parsed_string(column, _other_type(plan, side), lossless=True, for_join=True)
    if plan.kind == "render":
        return _rendered_token(column)
    if plan.kind == "numeric":
        return _numeric_token(column, dtype)
    return _direct_token(column, dtype)


def value_token(plan: FieldPlan, side: str) -> Column:
    """Return the canonical token hashed for this compared column."""
    dtype = _dtype(plan, side)
    column = _column(plan.name)
    if plan.kind == "parse" and plan.string_side == side:
        return _parsed_string(column, _other_type(plan, side), lossless=False, for_join=False)
    if plan.kind == "render":
        return _rendered_token(column)
    if plan.kind == "numeric":
        return _numeric_token(column, dtype)
    return _direct_token(column, dtype)


def value_display(plan: FieldPlan, side: str) -> Column:
    """Return the value shown in an example.

    Timestamps are the UTC text, so the driver never converts them in the
    machine time zone. Other direct values keep their Spark type for
    :func:`qa_core_recon.compare.render_value`.
    """
    dtype = _dtype(plan, side)
    column = _column(plan.name)
    if _is_timestamp(dtype):
        return date_format(column, _TIMESTAMP_PATTERN)
    if plan.kind == "render":
        return when(column.isNull(), lit(None).cast("string")).otherwise(column.cast("string"))
    if plan.kind == "parse" and plan.string_side == side:
        return _parsed_display(column, _other_type(plan, side))
    return column


def _plan(name: str, role: str, source: DataType, target: DataType) -> FieldPlan:
    kind, string_side = _kind(source, target)
    return FieldPlan(
        name=name,
        role=role,
        kind=kind,
        source_type=source,
        target_type=target,
        string_side=string_side,
    )


def _kind(source: DataType, target: DataType) -> tuple[str, str | None]:
    if isinstance(source, NullType) or isinstance(target, NullType):
        return "direct", None
    if _same_family(source, target):
        return "direct", None
    if _is_int(source) and isinstance(target, DecimalType):
        return "numeric", None
    if _is_int(target) and isinstance(source, DecimalType):
        return "numeric", None
    if isinstance(source, StringType) and _parsable(target):
        return "parse", "source"
    if isinstance(target, StringType) and _parsable(source):
        return "parse", "target"
    return "render", None


def _same_family(source: DataType, target: DataType) -> bool:
    if isinstance(source, DecimalType) and isinstance(target, DecimalType):
        return True
    if _is_int(source) and _is_int(target):
        return True
    return type(source) is type(target)


def _parsable(dtype: DataType) -> bool:
    return _is_int(dtype) or isinstance(
        dtype,
        (
            DecimalType,
            DateType,
            TimestampType,
            TimestampNTZType,
            BooleanType,
            FloatType,
            DoubleType,
        ),
    )


def _direct_token(column: Column, dtype: DataType) -> Column:
    if isinstance(dtype, TimestampNTZType):
        # Stored wall time. date_format does not apply the session zone.
        return _timestamp_token(column)
    if isinstance(dtype, TimestampType):
        # Session zone is UTC, so this text is the instant.
        return _timestamp_token(column)
    return _cell_token(column)


def _timestamp_token(column: Column) -> Column:
    payload = date_format(column, _TIMESTAMP_PATTERN)
    return when(column.isNull(), lit("n")).otherwise(
        concat(lit(f"timestamp:{_TIMESTAMP_WIDTH}:"), payload)
    )


def _numeric_token(column: Column, dtype: DataType) -> Column:
    if _is_int(dtype):
        return _cell_token(column.cast(DecimalType(38, 0)))
    return _cell_token(column)


def _rendered_token(column: Column) -> Column:
    return _cell_token(column.cast("string"))


def _cell_token(column: Column) -> Column:
    """Encode one non-timestamp value with the Python canonical token."""
    return when(column.isNull(), lit("n")).otherwise(_ENCODE(column))


def _parsed_string(column: Column, typed: DataType, *, lossless: bool, for_join: bool) -> Column:
    if isinstance(typed, DecimalType):
        return _decimal_string_token(column, typed.precision, typed.scale, for_join=for_join)
    parsed, ok = _parse(column, typed, lossless=lossless)
    aligned = _direct_token(parsed, typed)
    # A failed key keeps its own token, so two equal bad strings still join
    # and a bad string does not join a typed value. A failed compared value
    # uses an invalid token, which is not null and does not match the typed side.
    fallback = _cell_token(column) if for_join else _invalid_token(column)
    return when(column.isNull(), lit("n")).when(ok, aligned).otherwise(fallback)


def _decimal_string_token(column: Column, precision: int, scale: int, *, for_join: bool) -> Column:
    payload = _DECIMAL_PAYLOAD(trim(column), lit(precision), lit(scale))
    size = length(encode(payload, "UTF-8")).cast("string")
    token = concat(lit("decimal:"), size, lit(":"), payload)
    fallback = _cell_token(column) if for_join else _invalid_token(column)
    return when(column.isNull(), lit("n")).when(payload.isNull(), fallback).otherwise(token)


def _parsed_display(column: Column, typed: DataType) -> Column:
    if isinstance(typed, DecimalType):
        payload = _DECIMAL_PAYLOAD(trim(column), lit(typed.precision), lit(typed.scale))
        return (
            when(column.isNull(), lit(None).cast("string"))
            .when(payload.isNull(), column.cast("string"))
            .otherwise(payload)
        )
    parsed, ok = _parse(column, typed, lossless=False)
    if _is_timestamp(typed):
        shown = date_format(parsed, _TIMESTAMP_PATTERN)
    elif isinstance(typed, DateType):
        shown = date_format(parsed, "yyyy-MM-dd")
    else:
        shown = parsed.cast("string")
    return (
        when(column.isNull(), lit(None).cast("string"))
        .when(ok, shown)
        .otherwise(column.cast("string"))
    )


def _parse(column: Column, typed: DataType, *, lossless: bool) -> tuple[Column, Column]:
    text = trim(column)
    if _is_int(typed):
        parsed = _try(text, "bigint")
        if lossless:
            ok = parsed.isNotNull() & (text == parsed.cast("string"))
        else:
            ok = parsed.isNotNull()
        return parsed, ok
    if isinstance(typed, DateType):
        parsed = _try(text, "date")
        canon = date_format(parsed, "yyyy-MM-dd")
        ok = parsed.isNotNull() & (text == canon)
        return parsed, ok
    if isinstance(typed, TimestampNTZType):
        parsed = _try(text, "timestamp_ntz")
        return parsed, parsed.isNotNull()
    if isinstance(typed, TimestampType):
        parsed = _try(text, "timestamp")
        return parsed, parsed.isNotNull()
    if isinstance(typed, BooleanType):
        parsed = _try(text, "boolean")
        return parsed, parsed.isNotNull()
    if isinstance(typed, FloatType):
        parsed = _try(text, "float")
        return parsed, parsed.isNotNull()
    if isinstance(typed, DoubleType):
        parsed = _try(text, "double")
        return parsed, parsed.isNotNull()
    if isinstance(typed, BinaryType):
        return column, lit(False)
    return column, lit(False)


def _try(column: Column, sql_type: str) -> Column:
    # try_cast returns null for a value that does not fit, including when ANSI
    # mode is on. ``toString`` of a named column is its SQL expression
    # (``qty`` or ``trim(qty)``), which try_cast can wrap.
    inner = column._jc.toString()
    return expr(f"try_cast({inner} as {sql_type})")


def _invalid_token(column: Column) -> Column:
    payload = column.cast("string")
    size = length(encode(payload, "UTF-8")).cast("string")
    return concat(lit("invalid:string:"), size, lit(":"), payload)


def _dtype(plan: FieldPlan, side: str) -> DataType:
    return plan.source_type if side == "source" else plan.target_type


def _other_type(plan: FieldPlan, side: str) -> DataType:
    return plan.target_type if side == "source" else plan.source_type


def _is_int(dtype: DataType) -> bool:
    return isinstance(dtype, _INTEGER_TYPES)


def _is_timestamp(dtype: DataType) -> bool:
    return isinstance(dtype, (TimestampType, TimestampNTZType))


def _column(name: str) -> Column:
    return col("`" + name.replace("`", "``") + "`")
