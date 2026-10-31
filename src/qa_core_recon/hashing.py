"""Canonical row encoding for the key diff.

The digest is SHA-256 of one UTF-8 string. Nothing in this module reads a
file or decides that two datasets match. Spark computes the digest on the
executors. The driver never sees the full column of hashes.

Each value is one self-delimiting token. Null is the single character ``n``.
Every other value is ``tag:length:payload``, where ``length`` is the UTF-8
byte length of ``payload`` written in decimal with no leading zeros. Tokens
are concatenated in sorted column order with no extra separator. A length
prefix keeps ``("a,b", "c")`` distinct from ``("a", "b,c")``, and ``n`` is
distinct from the empty string (``string:0:``).

Decimal scale is not part of the token. ``1.0`` and ``1.00`` are the same
value, so they are the same payload (``1``). Trailing zeros that only record
scale are removed. Every significant digit is kept, the string is not
rounded, and scientific notation is not used. Negative zero is ``0``.

Float ``-0.0`` is ``0.0``. Every NaN, including a negative NaN, is ``NaN``.
Dates are ``YYYY-MM-DD``. Booleans are ``true`` and ``false``.

Spark ``TimestampType`` values are not passed through :func:`encode_value`.
The worker would hand them over as naive datetimes in the machine's local
time zone. The key diff formats those instants in Spark, under the UTC
session, before any Python encoder runs. ``TimestampNTZType`` is formatted
from the stored wall time and is not shifted. A naive datetime passed to
:func:`encode_value` directly is treated as UTC only when the caller already
holds a UTC value. An aware datetime is converted to UTC. The text is
``YYYY-MM-DDTHH:MM:SS.ffffffZ``.

Tolerance is not applied here. Amount sums may lie inside the spec tolerance
while the row hash still changes.
"""

import hashlib
import math
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Context, Decimal, localcontext

from qa_core_recon.spec import Spec

# Above Spark's decimal(38). normalize() must not round a full-precision value.
_DECIMAL_CONTEXT = Context(prec=100, traps=[])


def compared_columns(spec: Spec) -> tuple[str, ...]:
    """Return the non-key columns in the row hash, sorted by name.

    Amount columns, ``group_by`` columns, and ``hash_columns`` are included.
    A key column is never included. Source and target use the same names in
    this version, so the sorted names are the target order after that mapping.
    The spec order does not change the hash. An empty result means a changed
    row cannot be detected.
    """
    keys = set(spec.keys)
    names: list[str] = []
    seen: set[str] = set()
    for name in (
        *(amount.column for amount in spec.amounts),
        *spec.group_by,
        *spec.hash_columns,
    ):
        if name in keys or name in seen:
            continue
        seen.add(name)
        names.append(name)
    names.sort()
    return tuple(names)


def encode_value(value: object) -> str:
    """Return the canonical token for one value. Null is ``n``."""
    if value is None:
        return "n"
    return _pack(_tag_and_payload(value))


def encode_row(values: Sequence[object]) -> str:
    """Return the canonical text of one row, in the order given."""
    return "".join(encode_value(value) for value in values)


def row_hash_hex(values: Sequence[object]) -> str:
    """Return the SHA-256 hex digest of :func:`encode_row`."""
    return hashlib.sha256(encode_row(values).encode("utf-8")).hexdigest()


def _pack(parts: tuple[str, str]) -> str:
    tag, payload = parts
    raw = payload.encode("utf-8")
    return f"{tag}:{len(raw)}:{payload}"


def _tag_and_payload(value: object) -> tuple[str, str]:
    # bool is an int, and datetime is a date. The narrower type wins.
    if isinstance(value, bool):
        return "bool", "true" if value else "false"
    if isinstance(value, int):
        return "int", str(value)
    if isinstance(value, Decimal):
        return "decimal", _decimal_payload(value)
    if isinstance(value, float):
        return "float", _float_payload(value)
    if isinstance(value, str):
        return "string", value
    if isinstance(value, (bytes, bytearray)):
        return "bytes", bytes(value).hex()
    if isinstance(value, datetime):
        return "timestamp", _timestamp_payload(value)
    if isinstance(value, date):
        return "date", value.isoformat()
    raise TypeError(f"cannot encode {type(value).__name__} for a row hash")


def plain_decimal_text(value: Decimal, scale: int) -> str:
    """Return ``value`` at ``scale`` with no exponent and no plus sign.

    Zero is unsigned. ``decimal(20,10)`` zero is ``0.0000000000``, not ``0E-10``.
    """
    if not value.is_finite():
        raise ValueError("decimal is not finite")
    quantum = Decimal(1).scaleb(-scale)
    with localcontext(_DECIMAL_CONTEXT):
        fitted = value.quantize(quantum)
        if fitted.is_signed() and fitted == 0:
            fitted = Decimal(0).quantize(quantum)
        return format(fitted, "f")


def strict_decimal_payload(text: str, precision: int, scale: int) -> str | None:
    """Return the decimal payload when ``text`` fits ``decimal(precision, scale)``.

    Rounding is a failed parse. ``1.50`` at scale 2 is accepted. ``1.001`` at
    scale 2 is not. The payload is the same text :func:`encode_value` uses.
    """
    from decimal import InvalidOperation

    if text is None:
        return None
    raw = str(text).strip()
    if raw == "":
        return None
    try:
        value = Decimal(raw)
    except InvalidOperation:
        return None
    if not value.is_finite():
        return None
    quantum = Decimal(1).scaleb(-scale)
    with localcontext(_DECIMAL_CONTEXT):
        fitted = value.quantize(quantum)
        if fitted.compare(value) != 0:
            return None
        digits = fitted.as_tuple().digits
        if digits != (0,) and len(digits) > precision:
            return None
    return _decimal_payload(value)


def _decimal_payload(value: Decimal) -> str:
    if not value.is_finite():
        raise TypeError("cannot encode a non-finite decimal for a row hash")
    with localcontext(_DECIMAL_CONTEXT):
        if value == 0:
            return "0"
        return format(value.normalize(), "f")


def _float_payload(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    # -0.0 compares equal to 0.0. One payload keeps the hash stable.
    if value == 0.0:
        return "0.0"
    return repr(value)


def _timestamp_payload(value: datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return f"{value.strftime('%Y-%m-%dT%H:%M:%S')}.{value.microsecond:06d}Z"
