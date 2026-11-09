"""Aggregate and key-diff checks for ``qa-core-recon run``.

Six checks run, in this order: row_count, amount_sum, null_keys,
duplicate_keys, key_diff, changed_columns. All six run even when an earlier
one mismatches. Each check computes its result inside Spark. Only aggregates
reach the driver: one total per side, one row per group, the null-key counts,
the duplicate-key count, the key-diff counts, the per-column changed counts,
and at most :data:`qa_core_recon.compare.DUPLICATE_EXAMPLE_LIMIT` or
:data:`qa_core_recon.compare.KEY_DIFF_EXAMPLE_LIMIT` example keys.

The key diff drops rows with a null in any key column, then groups by the
distinct aligned key. One key contributes one count. When several rows share
a key, the row with the smallest row hash is the one that is compared. The
row hash is SHA-256 of the canonical encoding in :mod:`qa_core_recon.hashing`,
computed in Spark. Timestamps are formatted in Spark under the UTC session
before that encoding. Column types are aligned by :mod:`qa_core_recon.alignment`.
The full hash column is not collected.

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
from pyspark.sql.functions import col, concat, lit, sha2, struct, when
from pyspark.sql.functions import count as spark_count
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum

from qa_core_recon.alignment import (
    example_key,
    format_example_value,
    group_value,
    join_key,
    plan_fields,
    type_mismatches,
    value_display,
    value_token,
)
from qa_core_recon.compare import (
    DUPLICATE_EXAMPLE_LIMIT,
    KEY_DIFF_EXAMPLE_LIMIT,
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
from qa_core_recon.hashing import compared_columns
from qa_core_recon.readers import ReadError
from qa_core_recon.spec import Spec

_CHECK_ORDER = (
    "row_count",
    "amount_sum",
    "null_keys",
    "duplicate_keys",
    "key_diff",
    "changed_columns",
)

_NULL_KEY_RULE = "Rows with a null in any key column are excluded."
_DISTINCT_RULE = "Counts are by distinct key value."
_DUPLICATE_RULE = "When a key is duplicated, the row with the smallest row hash is used."
_CHANGED_COLUMN_RULE = (
    "Each count is the number of distinct keys whose value differs. "
    "Examples are listed in sorted order. "
    "When a key is duplicated, the compared row is the one with the smallest row hash."
)
_CHANGED_UNAVAILABLE = "Changed rows cannot be detected because no compared columns were declared."


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
    """Return the six check objects and the per-side row counts.

    Raises ReadError when an amount sum does not fit decimal(38, scale).
    """
    source = source.cache()
    target = target.cache()
    try:
        group_plans = plan_fields(
            _field_types(source), _field_types(target), spec.group_by, "group"
        )
        source_side = _profile(source, spec, "source", group_plans)
        target_side = _profile(target, spec, "target", group_plans)
        nulls = {
            "source": _null_key_count(source, spec),
            "target": _null_key_count(target, spec),
        }
        duplicates = {
            "source": _duplicate_keys(source, spec),
            "target": _duplicate_keys(target, spec),
        }
        duplicates_present = duplicates["source"][0] > 0 or duplicates["target"][0] > 0
        key_diff, changed_columns = _key_difference(
            source, target, spec, duplicates_present=duplicates_present
        )
    finally:
        source.unpersist()
        target.unpersist()
    checks = [
        _row_count_check(source_side, target_side, spec),
        _amount_sum_check(source_side, target_side, spec),
        _null_key_check(nulls, spec),
        _duplicate_key_check(duplicates, spec),
        key_diff,
        changed_columns,
    ]
    if tuple(check["name"] for check in checks) != _CHECK_ORDER:
        raise RuntimeError("checks were assembled out of order")
    return checks, {"source": source_side.rows, "target": target_side.rows}


def _profile(frame: DataFrame, spec: Spec, side: str, group_plans: tuple) -> _Side:
    if spec.group_by:
        groups = _aggregate_groups(frame, spec, side, group_plans)
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


def _aggregate_groups(
    frame: DataFrame, spec: Spec, side: str, group_plans: tuple
) -> list[_GroupSum]:
    group_names = list(spec.group_by)
    amount_names = [amount.column for amount in spec.amounts]
    width = len(group_names)
    # Format timestamps in Spark. A collected TimestampType is a naive local
    # datetime, so the group text would follow the host zone. A string group
    # column is parsed into the other side's type, so a CSV day such as
    # 2024-01-01 is the same group as a Parquet date. A value that does not
    # parse keeps its raw text and stays its own group.
    group_columns = []
    parse_columns = []
    parsed_at = []
    for index, plan in enumerate(group_plans):
        value, ok = group_value(plan, side)
        group_columns.append(value.alias(f"__qa_group_{index}"))
        if ok is not None:
            raw = _column(plan.name).alias(f"__qa_group_raw_{index}")
            parse_columns.extend([ok.alias(f"__qa_group_ok_{index}"), raw])
            parsed_at.append(index)
    amount_columns = [_column(name) for name in amount_names]
    selected = frame.select(*group_columns, *amount_columns, *parse_columns)
    amount_end = width + len(amount_names)
    parsed_positions = tuple(parsed_at)

    def to_pair(row):
        values = tuple(row)
        keys = list(values[:width])
        extra = values[amount_end:]
        for offset, index in enumerate(parsed_positions):
            ok, raw = extra[2 * offset], extra[2 * offset + 1]
            if not ok:
                keys[index] = raw
        # NaN is not equal to itself, so each raw NaN would be its own group.
        key = tuple(normalize_group_value(value) for value in keys)
        return key, values[width:amount_end]

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
    types = _field_types(frame)
    alias = "__qa_row_count"
    while alias in keys:
        alias += "_"
    grouped = frame.groupBy(*[_column(name) for name in keys]).agg(spark_count(lit(1)).alias(alias))
    duplicates = grouped.filter(col(f"`{alias}`") > 1)
    found = duplicates.count()
    # Order by the stored key. A timestamp is ordered as a timestamp, then
    # rendered in Spark so the driver does not apply the host zone.
    ordered = duplicates.orderBy(*[_column(name).asc_nulls_first() for name in keys]).limit(
        DUPLICATE_EXAMPLE_LIMIT
    )
    rendered_columns = [
        format_example_value(_column(name), types[name]).alias(name) for name in keys
    ]
    examples = [
        tuple(row[index] for index in range(len(keys)))
        for row in ordered.select(*rendered_columns).collect()
    ]
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


@dataclass(frozen=True)
class _Keyed:
    frame: DataFrame
    hash_name: str
    align_names: tuple[str, ...]
    token_names: tuple[str, ...]
    display_names: tuple[str, ...]
    original_names: tuple[str, ...]
    order_names: tuple[str, ...]


def _key_difference(
    source: DataFrame,
    target: DataFrame,
    spec: Spec,
    *,
    duplicates_present: bool,
) -> tuple[dict, dict]:
    """Return the key_diff and changed_columns checks.

    Rows with a null in any key column are removed before the join. Keys are
    aligned to strings first, so Spark cannot cast ``01`` to ``1``. The join
    is one row per distinct aligned key. Counts and at most
    KEY_DIFF_EXAMPLE_LIMIT examples are collected. The hash column itself is
    not collected.
    """
    columns = compared_columns(spec)
    keys = list(spec.keys)
    source_fields = _field_types(source)
    target_fields = _field_types(target)
    key_plans = plan_fields(source_fields, target_fields, keys, "key")
    column_plans = plan_fields(source_fields, target_fields, columns, "compared")
    mismatches = type_mismatches((*key_plans, *column_plans))
    align_names = _shared_names(source, target, len(key_plans), "__qa_ak")
    left = _keyed_side(source, key_plans, column_plans, "src", align_names)
    right = _keyed_side(target, key_plans, column_plans, "tgt", align_names)
    joined = left.frame.join(right.frame, on=list(left.align_names), how="fullouter").cache()
    try:
        return _key_difference_from_join(
            joined,
            spec,
            columns,
            left,
            right,
            mismatches,
            duplicates_present=duplicates_present,
        )
    finally:
        joined.unpersist()


def _key_difference_from_join(
    joined: DataFrame,
    spec: Spec,
    columns: tuple[str, ...],
    left: _Keyed,
    right: _Keyed,
    mismatches: list[dict],
    *,
    duplicates_present: bool,
) -> tuple[dict, dict]:
    keys = list(spec.keys)
    src_hash = _column(left.hash_name)
    tgt_hash = _column(right.hash_name)
    missing = src_hash.isNotNull() & tgt_hash.isNull()
    extra = tgt_hash.isNotNull() & src_hash.isNull()
    changed = src_hash.isNotNull() & tgt_hash.isNotNull() & (src_hash != tgt_hash)
    column_differs = [
        changed & (_column(src_name) != _column(tgt_name))
        for src_name, tgt_name in zip(left.token_names, right.token_names, strict=True)
    ]
    summary = joined.agg(
        spark_sum(when(missing, lit(1)).otherwise(lit(0))).alias("missing"),
        spark_sum(when(extra, lit(1)).otherwise(lit(0))).alias("extra"),
        spark_sum(when(changed, lit(1)).otherwise(lit(0))).alias("changed"),
        *[
            spark_sum(when(condition, lit(1)).otherwise(lit(0))).alias(f"d{index}")
            for index, condition in enumerate(column_differs)
        ],
    ).collect()[0]
    missing_count = _as_count(summary["missing"])
    extra_count = _as_count(summary["extra"])
    changed_count = _as_count(summary["changed"])
    column_counts = [_as_count(summary[f"d{index}"]) for index in range(len(columns))]
    if changed_count == 0 and any(column_counts):
        raise RuntimeError("a column differs while the row hash matches")
    if changed_count and not any(column_counts):
        raise RuntimeError("row hash differs but no compared column differs")

    missing_examples = (
        _take_key_examples(joined, left, right, missing, left.order_names) if missing_count else []
    )
    extra_examples = (
        _take_key_examples(joined, left, right, extra, right.order_names) if extra_count else []
    )
    changed_examples = (
        _take_key_examples(joined, left, right, changed, left.order_names) if changed_count else []
    )
    changed_column_rows = []
    for index, name in enumerate(columns):
        count = column_counts[index]
        if count == 0:
            continue
        examples = _take_value_examples(
            joined,
            left,
            right,
            column_differs[index],
            index,
            left.order_names,
        )
        changed_column_rows.append({"column": name, "count": count, "examples": examples})

    counts_match = missing_count == extra_count == changed_count == 0
    key_status = "match" if counts_match and not mismatches else "mismatch"
    column_status = "match" if not changed_column_rows else "mismatch"
    key_diff = {
        "name": "key_diff",
        "status": key_status,
        "details": {
            "changed": {"count": changed_count, "examples": changed_examples},
            "columns": list(columns),
            "counting": "distinct_key",
            "duplicates_present": duplicates_present,
            "example_limit": KEY_DIFF_EXAMPLE_LIMIT,
            "extra": {"count": extra_count, "examples": extra_examples},
            "keys": keys,
            "changed_detection": None if columns else _CHANGED_UNAVAILABLE,
            "missing": {"count": missing_count, "examples": missing_examples},
            "null_keys": "excluded",
            "rules": [_NULL_KEY_RULE, _DISTINCT_RULE, _DUPLICATE_RULE],
            "type_mismatches": mismatches,
        },
    }
    changed_columns = {
        "name": "changed_columns",
        "status": column_status,
        "details": {
            "columns": changed_column_rows,
            "counting": "distinct_key",
            "example_limit": KEY_DIFF_EXAMPLE_LIMIT,
            "keys": keys,
            "rule": _CHANGED_COLUMN_RULE,
        },
    }
    return key_diff, changed_columns


def _keyed_side(
    frame: DataFrame, key_plans, column_plans, prefix: str, align_names: tuple[str, ...]
) -> _Keyed:
    """One row per distinct aligned key, with the smallest row hash's values."""
    side = "source" if prefix == "src" else "target"
    keys = [plan.name for plan in key_plans]
    frame = _without_null_keys(frame, keys)
    taken = set(frame.columns) | set(align_names)
    original_names = tuple(
        _private(taken, f"__qa_{prefix}_k{index}") for index in range(len(key_plans))
    )
    order_names = tuple(
        _private(taken, f"__qa_{prefix}_o{index}") for index in range(len(key_plans))
    )
    token_names = tuple(
        _private(taken, f"__qa_{prefix}_t{index}") for index in range(len(column_plans))
    )
    display_names = tuple(
        _private(taken, f"__qa_{prefix}_d{index}") for index in range(len(column_plans))
    )
    selected = []
    for index, plan in enumerate(key_plans):
        selected.append(join_key(plan, side).alias(align_names[index]))
        # Example text is formatted in Spark. The order column stays the
        # original value, so a timestamp sorts as a timestamp.
        selected.append(example_key(plan, side).alias(original_names[index]))
        selected.append(_column(plan.name).alias(order_names[index]))
    for index, plan in enumerate(column_plans):
        selected.append(value_token(plan, side).alias(token_names[index]))
        selected.append(value_display(plan, side).alias(display_names[index]))
    projected = frame.select(*selected)
    hash_temp = _private(set(projected.columns), "__qa_row_hash")
    projected = projected.withColumn(hash_temp, _digest(list(token_names)))
    fields = [_column(hash_temp).alias("h")]
    for index, token_name in enumerate(token_names):
        fields.append(_column(token_name).alias(f"t{index}"))
        fields.append(_column(display_names[index]).alias(f"d{index}"))
    for index, original_name in enumerate(original_names):
        fields.append(_column(original_name).alias(f"k{index}"))
    for index, order_name in enumerate(order_names):
        fields.append(_column(order_name).alias(f"o{index}"))
    output_taken = set(align_names)
    rep = _private(output_taken, "__qa_rep")
    hash_name = _private(output_taken, f"__qa_{prefix}_hash")
    out_tokens = tuple(
        _private(output_taken, f"__qa_{prefix}_tok{index}") for index in range(len(token_names))
    )
    out_displays = tuple(
        _private(output_taken, f"__qa_{prefix}_val{index}") for index in range(len(display_names))
    )
    out_originals = tuple(
        _private(output_taken, f"__qa_{prefix}_key{index}") for index in range(len(original_names))
    )
    out_orders = tuple(
        _private(output_taken, f"__qa_{prefix}_ord{index}") for index in range(len(order_names))
    )
    grouped = projected.groupBy(*[_column(name) for name in align_names]).agg(
        spark_min(struct(*fields)).alias(rep)
    )
    chosen = [_column(name) for name in align_names]
    chosen.append(_column(rep).getField("h").alias(hash_name))
    for index, name in enumerate(out_tokens):
        chosen.append(_column(rep).getField(f"t{index}").alias(name))
    for index, name in enumerate(out_displays):
        chosen.append(_column(rep).getField(f"d{index}").alias(name))
    for index, name in enumerate(out_originals):
        chosen.append(_column(rep).getField(f"k{index}").alias(name))
    for index, name in enumerate(out_orders):
        chosen.append(_column(rep).getField(f"o{index}").alias(name))
    return _Keyed(
        frame=grouped.select(*chosen),
        hash_name=hash_name,
        align_names=align_names,
        token_names=out_tokens,
        display_names=out_displays,
        original_names=out_originals,
        order_names=out_orders,
    )


def _digest(enc_names: list[str]):
    if not enc_names:
        return sha2(lit(""), 256)
    return sha2(concat(*[_column(name) for name in enc_names]), 256)


def _without_null_keys(frame: DataFrame, keys: list[str]) -> DataFrame:
    condition = _column(keys[0]).isNotNull()
    for name in keys[1:]:
        condition = condition & _column(name).isNotNull()
    return frame.filter(condition)


def _take_key_examples(
    frame: DataFrame,
    left: _Keyed,
    right: _Keyed,
    condition,
    order_names: tuple[str, ...],
) -> list[list]:
    # Order by the original key, not the aligned token. Token text such as
    # int:2:-6 is not numeric order. Missing and changed rows use the source
    # key. Extra rows use the target key. A timestamp column is still a
    # timestamp here, so the order is the instant, not host-local text.
    # Nulls come first.
    limited = (
        frame.filter(condition)
        .orderBy(*[_column(name).asc_nulls_first() for name in order_names])
        .limit(KEY_DIFF_EXAMPLE_LIMIT)
    )
    rows = [_picked_key(item, left, right) for item in limited.collect()]
    rows.sort(key=lambda item: tuple(sort_token(value) for value in item))
    return [[render_value(value) for value in item] for item in rows]


def _take_value_examples(
    frame: DataFrame,
    left: _Keyed,
    right: _Keyed,
    condition,
    index: int,
    order_names: tuple[str, ...],
) -> list[dict]:
    source_token = left.token_names[index]
    target_token = right.token_names[index]
    source_value = left.display_names[index]
    target_value = right.display_names[index]
    limited = (
        frame.filter(condition)
        .orderBy(*[_column(name).asc_nulls_first() for name in order_names])
        .limit(KEY_DIFF_EXAMPLE_LIMIT)
    )
    found = []
    for item in limited.collect():
        key = _picked_key(item, left, right)
        found.append(
            (key, item[source_value], item[target_value], item[source_token], item[target_token])
        )
    found.sort(key=lambda item: tuple(sort_token(value) for value in item[0]))
    return [
        {
            "key": [render_value(value) for value in key],
            "source": render_value(source),
            "source_token": source_token,
            "target": render_value(target),
            "target_token": target_token,
        }
        for key, source, target, source_token, target_token in found
    ]


def _picked_key(item, left: _Keyed, right: _Keyed) -> tuple:
    values = []
    for source_name, target_name in zip(left.original_names, right.original_names, strict=True):
        source = item[source_name]
        target = item[target_name]
        values.append(source if source is not None else target)
    return tuple(values)


def _field_types(frame: DataFrame) -> dict:
    fields = {}
    for field in frame.schema.fields:
        if field.name not in fields:
            fields[field.name] = field.dataType
    return fields


def _as_count(value: object) -> int:
    if value is None:
        return 0
    return int(value)


def _shared_names(source: DataFrame, target: DataFrame, count: int, base: str) -> tuple[str, ...]:
    taken = set(source.columns) | set(target.columns)
    return tuple(_private(taken, f"{base}_{index}") for index in range(count))


def _private(taken: set[str], base: str) -> str:
    name = base
    while name in taken:
        name += "_"
    taken.add(name)
    return name
