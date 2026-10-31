"""Aggregate checks for ``qa-core-recon run``.

Four checks run, in this order: row_count, amount_sum, null_keys, duplicate_keys.
All four run even when an earlier one mismatches. Each check computes its
result inside Spark. Only aggregates reach the driver: one total per side,
one row per group, the null-key counts, the duplicate-key count, and at most
:data:`qa_core_recon.compare.DUPLICATE_EXAMPLE_LIMIT` example keys.

Amount sums are not Spark ``sum``. With precision loss allowed, Spark's sum
of ``decimal(38, scale)`` returns null when the total overflows, which is the
same result as a column of nulls. The sum here is a Python ``Decimal`` folded
on the executors. The driver accepts it only when it fits
``decimal(38, scale)``. A total or a group sum that does not fit raises
:class:`~qa_core_recon.readers.ReadError` and no report is written. The
message says ``sum`` for a total and ``group sum`` for one group.

Source is checked before target. On one side, groups are checked in sorted
order and then the total. Amount columns are checked in spec order. Sorting
before the check keeps the message stable when more than one group overflows.

A null amount is left out of the sum. It is not added as zero. A side or a
group with no non-null amount has a null sum. Two null sums match. A null
sum against a number, including zero, is a mismatch.

Tolerance applies to every amount total and to every group sum on its own.
It is not a budget shared across groups. The absolute difference must be less
than or equal to the tolerance. A missing group has no sum, so tolerance does
not apply to it.

Duplicate examples are the key values that occur more than once, not the
extra rows. At most 20 are kept, in sorted order, with nulls first. The count
is the full number of duplicated key values.
"""

from dataclasses import dataclass
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import count as spark_count

from qa_core_recon.compare import (
    DUPLICATE_EXAMPLE_LIMIT,
    absolute_difference,
    add_exact,
    decimal_fits,
    format_decimal,
    format_tolerance,
    normalize_group_value,
    overall_status,
    quantize_exact,
    render_value,
    sort_token,
    sums_match,
)
from qa_core_recon.decimals import DECIMAL_PRECISION
from qa_core_recon.readers import ReadError
from qa_core_recon.spec import Spec

_CHECK_ORDER = ("row_count", "amount_sum", "null_keys", "duplicate_keys")


@dataclass(frozen=True)
class _GroupSum:
    key: tuple
    rows: int
    sums: tuple[Decimal | None, ...]


@dataclass(frozen=True)
class _Side:
    rows: int
    sums: tuple[Decimal | None, ...]
    groups: tuple[_GroupSum, ...]


def execute_checks(
    source: DataFrame, target: DataFrame, spec: Spec
) -> tuple[list[dict], dict[str, int]]:
    """Return the four check objects and the per-side row counts.

    Raises ReadError when an amount sum does not fit decimal(38, scale).
    """
    source = source.cache()
    target = target.cache()
    try:
        source_side = _profile(source, spec, "source")
        target_side = _profile(target, spec, "target")
        nulls = {
            "source": _null_key_count(source, spec),
            "target": _null_key_count(target, spec),
        }
        duplicates = {
            "source": _duplicate_keys(source, spec),
            "target": _duplicate_keys(target, spec),
        }
    finally:
        source.unpersist()
        target.unpersist()
    checks = [
        _row_count_check(source_side, target_side, spec),
        _amount_sum_check(source_side, target_side, spec),
        _null_key_check(nulls, spec),
        _duplicate_key_check(duplicates, spec),
    ]
    if tuple(check["name"] for check in checks) != _CHECK_ORDER:
        raise RuntimeError("checks were assembled out of order")
    return checks, {"source": source_side.rows, "target": target_side.rows}


def _profile(frame: DataFrame, spec: Spec, side: str) -> _Side:
    if spec.group_by:
        groups = _aggregate_groups(frame, spec, side)
        rows, sums = _totals_from_groups(groups, spec, side)
        return _Side(rows=rows, sums=sums, groups=tuple(groups))
    rows, sums = _aggregate_total(frame, spec, side)
    return _Side(rows=rows, sums=sums, groups=())


def _aggregate_total(
    frame: DataFrame, spec: Spec, side: str
) -> tuple[int, tuple[Decimal | None, ...]]:
    names = [amount.column for amount in spec.amounts]
    if not names:
        return frame.count(), ()
    selected = frame.select(*[_column(name) for name in names])
    state = selected.rdd.aggregate(_empty_state(len(names)), _merge_row, _merge_states)
    rows, sums = _decode(state)
    return rows, _accept_all(sums, spec, side, group=False)


def _aggregate_groups(frame: DataFrame, spec: Spec, side: str) -> list[_GroupSum]:
    group_names = list(spec.group_by)
    amount_names = [amount.column for amount in spec.amounts]
    width = len(group_names)
    selected = frame.select(*[_column(name) for name in (*group_names, *amount_names)])

    def to_pair(row):
        values = tuple(row)
        # NaN is not equal to itself, so each raw NaN would be its own group.
        key = tuple(normalize_group_value(value) for value in values[:width])
        return key, values[width:]

    def create(amounts):
        return _merge_row(_empty_state(len(amount_names)), tuple(amounts))

    collected = (
        selected.rdd.map(to_pair).combineByKey(create, _merge_value, _merge_states).collect()
    )
    pending: list[_GroupSum] = []
    for key, state in collected:
        key_tuple = tuple(key)
        try:
            hash(key_tuple)
        except TypeError:
            raise ReadError(f"{side} group value cannot be compared") from None
        rows, sums = _decode(state)
        pending.append(_GroupSum(key=key_tuple, rows=rows, sums=sums))
    pending.sort(key=lambda item: tuple(sort_token(value) for value in item.key))
    accepted: list[_GroupSum] = []
    for group in pending:
        sums = _accept_all(group.sums, spec, side, group=True)
        accepted.append(_GroupSum(key=group.key, rows=group.rows, sums=sums))
    return accepted


def _merge_value(state, amounts):
    return _merge_row(state, tuple(amounts))


def _totals_from_groups(
    groups: list[_GroupSum], spec: Spec, side: str
) -> tuple[int, tuple[Decimal | None, ...]]:
    rows = sum(group.rows for group in groups)
    sums = []
    for index, amount in enumerate(spec.amounts):
        total = _sum_optional(group.sums[index] for group in groups)
        sums.append(_accept(total, amount.scale, side, amount.column, group=False))
    return rows, tuple(sums)


def _empty_state(width: int):
    return (0, tuple((Decimal(0), False) for _ in range(width)))


def _merge_row(state, amounts: tuple):
    rows, sums = state
    merged = []
    for (total, seen), value in zip(sums, amounts, strict=True):
        if value is None:
            merged.append((total, seen))
            continue
        number = _as_decimal(value)
        merged.append((add_exact(total, number) if seen else number, True))
    return (rows + 1, tuple(merged))


def _merge_states(left, right):
    merged = []
    for (total, seen), (other, other_seen) in zip(left[1], right[1], strict=True):
        if seen and other_seen:
            merged.append((add_exact(total, other), True))
        elif other_seen:
            merged.append((other, True))
        else:
            merged.append((total, seen))
    return (left[0] + right[0], tuple(merged))


def _decode(state) -> tuple[int, tuple[Decimal | None, ...]]:
    rows, sums = state
    return rows, tuple(total if seen else None for total, seen in sums)


def _accept_all(sums, spec: Spec, side: str, *, group: bool) -> tuple[Decimal | None, ...]:
    accepted = []
    for value, amount in zip(sums, spec.amounts, strict=True):
        accepted.append(_accept(value, amount.scale, side, amount.column, group=group))
    return tuple(accepted)


def _accept(
    value: Decimal | None, scale: int, side: str, column: str, *, group: bool
) -> Decimal | None:
    if value is None:
        return None
    if not decimal_fits(value, scale):
        raise _overflow(side, column, scale, group=group)
    return quantize_exact(value, scale)


def _sum_optional(values) -> Decimal | None:
    total = Decimal(0)
    seen = False
    for value in values:
        if value is None:
            continue
        total = add_exact(total, value) if seen else value
        seen = True
    if not seen:
        return None
    return total


def _overflow(side: str, column: str, scale: int, *, group: bool) -> ReadError:
    kind = "group sum" if group else "sum"
    return ReadError(
        f"{side} amount column {column!r} {kind} does not fit decimal({DECIMAL_PRECISION}, {scale})"
    )


def _as_decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReadError(f"amount value of type {type(value).__name__} cannot be summed")
    return Decimal(value)


def _null_key_count(frame: DataFrame, spec: Spec) -> int:
    condition = _column(spec.keys[0]).isNull()
    for name in spec.keys[1:]:
        condition = condition | _column(name).isNull()
    return frame.filter(condition).count()


def _duplicate_keys(frame: DataFrame, spec: Spec) -> tuple[int, list[list]]:
    keys = list(spec.keys)
    alias = "__qa_row_count"
    while alias in keys:
        alias += "_"
    grouped = frame.groupBy(*[_column(name) for name in keys]).agg(spark_count(lit(1)).alias(alias))
    duplicates = grouped.filter(col(f"`{alias}`") > 1)
    found = duplicates.count()
    ordered = duplicates.orderBy(*[_column(name).asc_nulls_first() for name in keys]).limit(
        DUPLICATE_EXAMPLE_LIMIT
    )
    examples = [tuple(row[index] for index in range(len(keys))) for row in ordered.collect()]
    examples.sort(key=lambda item: tuple(sort_token(value) for value in item))
    rendered = [[render_value(value) for value in item] for item in examples]
    return found, rendered


def _row_count_check(source: _Side, target: _Side, spec: Spec) -> dict:
    groups = _count_groups(source, target, spec)
    statuses = [_row_status(source.rows, target.rows), *(group["status"] for group in groups)]
    return {
        "name": "row_count",
        "status": overall_status(statuses),
        "details": {
            "group_by": list(spec.group_by),
            "groups": groups,
            "source": source.rows,
            "target": target.rows,
        },
    }


def _count_groups(source: _Side, target: _Side, spec: Spec) -> list[dict]:
    if not spec.group_by:
        return []
    return [
        _count_group(spec, key, source_index, target_index)
        for key, source_index, target_index in _aligned(source, target)
    ]


def _count_group(spec: Spec, key: tuple, source_index: dict, target_index: dict) -> dict:
    source_rows = source_index[key].rows if key in source_index else 0
    target_rows = target_index[key].rows if key in target_index else 0
    return {
        "group": _group_object(spec, key),
        "missing": _missing(key in source_index, key in target_index),
        "source": source_rows,
        "status": _row_status(source_rows, target_rows),
        "target": target_rows,
    }


def _amount_sum_check(source: _Side, target: _Side, spec: Spec) -> dict:
    columns = [_amount_column(source, target, spec, index) for index in range(len(spec.amounts))]
    return {
        "name": "amount_sum",
        "status": overall_status(column["status"] for column in columns),
        "details": {"columns": columns},
    }


def _amount_column(source: _Side, target: _Side, spec: Spec, index: int) -> dict:
    amount = spec.amounts[index]
    tolerance = spec.tolerance.value
    source_sum = source.sums[index]
    target_sum = target.sums[index]
    groups = _amount_groups(source, target, spec, index, tolerance)
    total_status = "match" if sums_match(source_sum, target_sum, tolerance) else "mismatch"
    return {
        "column": amount.column,
        "difference": _difference(source_sum, target_sum, amount.scale),
        "groups": groups,
        "reason": spec.tolerance.reason,
        "scale": amount.scale,
        "source": _money(source_sum, amount.scale),
        "status": overall_status([total_status, *(group["status"] for group in groups)]),
        "target": _money(target_sum, amount.scale),
        "tolerance": format_tolerance(tolerance),
    }


def _amount_groups(
    source: _Side, target: _Side, spec: Spec, index: int, tolerance: Decimal
) -> list[dict]:
    if not spec.group_by:
        return []
    amount = spec.amounts[index]
    listed = []
    for key, source_index, target_index in _aligned(source, target):
        source_present = key in source_index
        target_present = key in target_index
        source_sum = source_index[key].sums[index] if source_present else None
        target_sum = target_index[key].sums[index] if target_present else None
        if source_present and target_present:
            matched = sums_match(source_sum, target_sum, tolerance)
            difference = _difference(source_sum, target_sum, amount.scale)
        else:
            matched = False
            difference = None
        listed.append(
            {
                "difference": difference,
                "group": _group_object(spec, key),
                "missing": _missing(source_present, target_present),
                "source": _money(source_sum, amount.scale),
                "status": "match" if matched else "mismatch",
                "target": _money(target_sum, amount.scale),
            }
        )
    return listed


def _aligned(source: _Side, target: _Side):
    source_index = {group.key: group for group in source.groups}
    target_index = {group.key: group for group in target.groups}
    keys = sorted(
        set(source_index) | set(target_index),
        key=lambda item: tuple(sort_token(value) for value in item),
    )
    for key in keys:
        yield key, source_index, target_index


def _null_key_check(counts: dict[str, int], spec: Spec) -> dict:
    status = "match" if counts["source"] == 0 and counts["target"] == 0 else "mismatch"
    return {
        "name": "null_keys",
        "status": status,
        "details": {
            "keys": list(spec.keys),
            "source": counts["source"],
            "target": counts["target"],
        },
    }


def _duplicate_key_check(found: dict[str, tuple[int, list[list]]], spec: Spec) -> dict:
    source_count, source_examples = found["source"]
    target_count, target_examples = found["target"]
    status = "match" if source_count == 0 and target_count == 0 else "mismatch"
    return {
        "name": "duplicate_keys",
        "status": status,
        "details": {
            "example_limit": DUPLICATE_EXAMPLE_LIMIT,
            "keys": list(spec.keys),
            "source": {"count": source_count, "examples": source_examples},
            "target": {"count": target_count, "examples": target_examples},
        },
    }


def _row_status(source_rows: int, target_rows: int) -> str:
    return "match" if source_rows == target_rows else "mismatch"


def _missing(source_present: bool, target_present: bool) -> str | None:
    if source_present and target_present:
        return None
    if not source_present:
        return "source"
    return "target"


def _group_object(spec: Spec, key: tuple) -> dict:
    return {name: render_value(value) for name, value in zip(spec.group_by, key, strict=True)}


def _money(value: Decimal | None, scale: int):
    if value is None:
        return None
    return format_decimal(value, scale)


def _difference(source: Decimal | None, target: Decimal | None, scale: int):
    if source is None or target is None:
        return None
    return format_decimal(absolute_difference(source, target), scale)


def _column(name: str):
    return col("`" + name.replace("`", "``") + "`")
