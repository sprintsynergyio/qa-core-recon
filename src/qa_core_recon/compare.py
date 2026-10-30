"""Pure comparison rules for aggregate checks.

Nothing in this module reads a file or asks a model whether two datasets
match. A sum matches when both sides are null, or when both are decimals and
the absolute difference is less than or equal to the spec tolerance. A null
sum is not zero. Tolerance is a Decimal, so a value such as ``0.01`` is not
a binary float.

Sums are formed at a precision well above 38 digits and then checked against
``decimal(38, scale)``. A number that needs more than 38 digits does not fit.
Rounding it to fit would publish a wrong total, so the caller turns that into
a read error instead.
"""

from collections.abc import Iterable
from decimal import Context, Decimal, localcontext

from qa_core_recon.decimals import DECIMAL_PRECISION
from qa_core_recon.exit_codes import ExitCode

# Far above decimal(38). Addition at this precision is exact for any sum that
# still fits in 38 digits. A larger sum may be rounded here, and the 38-digit
# check then rejects it, so the rounded value is never reported.
SUM_PRECISION = 200

DUPLICATE_EXAMPLE_LIMIT = 20

_STATUS_CODES = {
    "match": ExitCode.MATCH,
    "mismatch": ExitCode.MISMATCH,
    "refused": ExitCode.REFUSED,
}


def sum_context() -> Context:
    """Return a context that will not raise while a sum is accumulating."""
    return Context(prec=SUM_PRECISION, traps=[])


def quantize_exact(value: Decimal, scale: int) -> Decimal:
    """Return ``value`` at ``scale``, with a positive zero when the sum is zero."""
    quantum = Decimal(1).scaleb(-scale)
    with _local():
        fitted = value.quantize(quantum)
    if fitted.is_signed() and fitted == 0:
        with _local():
            fitted = Decimal(0).quantize(quantum)
    return fitted


def decimal_fits(value: Decimal, scale: int) -> bool:
    """Return True when ``value`` fits in decimal(38, scale) without rounding."""
    if not value.is_finite():
        return False
    quantum = Decimal(1).scaleb(-scale)
    with _local():
        fitted = value.quantize(quantum)
    if fitted != value:
        return False
    digits = fitted.as_tuple().digits
    if digits == (0,):
        return True
    return len(digits) <= DECIMAL_PRECISION


def format_decimal(value: Decimal, scale: int) -> str:
    """Render ``value`` with exactly ``scale`` digits after the decimal point.

    Raises ValueError when the value does not fit decimal(38, scale). Callers
    that are summing must turn that into a read error. This function does not
    round to a shorter number.
    """
    if not decimal_fits(value, scale):
        raise ValueError(f"value does not fit decimal({DECIMAL_PRECISION}, {scale})")
    return format(quantize_exact(value, scale), "f")


def format_tolerance(value: Decimal) -> str:
    """Render a tolerance with the exact digits from the spec."""
    return format(value, "f")


def add_exact(total: Decimal, value: Decimal) -> Decimal:
    """Add two decimals at the sum precision."""
    with _local():
        return total + value


def absolute_difference(source: Decimal, target: Decimal) -> Decimal:
    """Return the exact absolute difference of two sums."""
    with _local():
        return abs(source - target)


def sums_match(
    source: Decimal | None,
    target: Decimal | None,
    tolerance: Decimal,
) -> bool:
    """Return True when two sums agree, allowing the spec tolerance.

    Both null is a match: neither side had a non-null amount. Null against a
    number is a mismatch. Null is not treated as zero. When both sides are
    numbers, the absolute difference must be less than or equal to
    ``tolerance``. Equality is inside the range.
    """
    if source is None or target is None:
        return source is None and target is None
    return absolute_difference(source, target) <= tolerance


def overall_status(statuses: Iterable[str]) -> str:
    """Return mismatch when any part mismatches, otherwise match."""
    if any(status == "mismatch" for status in statuses):
        return "mismatch"
    return "match"


def exit_code_for(status: str) -> ExitCode:
    """Map a report status to the process exit code."""
    try:
        return _STATUS_CODES[status]
    except KeyError:
        raise ValueError(f"unknown report status {status!r}") from None


def render_value(value: object) -> object:
    """Return a JSON-safe form of a key or group value.

    Decimals are strings at their own scale. Other values stay strings,
    integers, or booleans. Floats are rendered with ``repr`` so a binary
    value cannot become a rounded JSON number.
    """
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        exponent = value.as_tuple().exponent
        scale = -exponent if isinstance(exponent, int) and exponent < 0 else 0
        return format_decimal(value, scale)
    if isinstance(value, float):
        return repr(value)
    iso = getattr(value, "isoformat", None)
    if callable(iso):
        return iso()
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def sort_token(value: object) -> tuple:
    """Return a total order. Nulls come first. Values of one type sort together."""
    if value is None:
        return (0, "")
    if isinstance(value, bool):
        return (1, int(value))
    if isinstance(value, int):
        return (2, value)
    if isinstance(value, Decimal):
        return (3, value)
    if isinstance(value, str):
        return (4, value)
    return (5, type(value).__name__, str(render_value(value)))


def _local():
    return localcontext(sum_context())
