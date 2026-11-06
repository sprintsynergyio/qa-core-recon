"""Exact decimal parsing for amount columns.

Precision rule: an amount is stored as Spark ``decimal(38, scale)``. 38 is the
largest precision Spark can store. ``scale`` comes from the spec and is the
number of digits after the decimal point. The integer part may use
``38 - scale`` digits. A value that needs more digits is refused. It is never
truncated and never rounded to fit.

A value is exact at the spec scale when it equals the same number quantized to
that scale. Trailing zeros are exact: ``10.500`` at scale 2 is ``10.50``.
Extra non-zero fractional digits are not: ``10.005`` at scale 2 is refused.
Equivalent spellings of the same number (``100`` and ``1e2``) are exact.
A text amount must be ASCII: optional surrounding whitespace, an optional
sign, digits with an optional fraction, and an optional exponent. Underscores
and other digit scripts are refused.

A cast rule does not relax this test for strings, integers, or decimals. It
only authorizes rounding a float or double with ``half-even`` or ``half-up``.
The float is taken from its shortest decimal spelling (``repr``), which is
what Spark's ``cast`` uses. Rounding the exact binary value of ``2.675``
with half-up would produce ``2.67``. ``Decimal(repr(2.675))`` half-up at
scale 2 is ``2.68``.
"""

import math
import re
from decimal import (
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    Context,
    Decimal,
    InvalidOperation,
    localcontext,
)

DECIMAL_PRECISION = 38
DISPLAY_LIMIT = 80

CAST_MODE_NAMES = ("half-even", "half-up")
CAST_ROUNDING = {
    "half-even": ROUND_HALF_EVEN,
    "half-up": ROUND_HALF_UP,
}

# High enough to quantize a value past the 38-digit limit without the default
# context (28 digits) rounding it first. The digit check then refuses it.
_CONTEXT = Context(prec=100)

# ASCII only. ``re.ASCII`` keeps ``\\d`` off Unicode digits, so ``1_000``,
# Arabic-Indic digits, and fullwidth digits fail closed. Space and tab may
# surround the number. A line break is not whitespace here: ``\\s`` would
# accept ``1\\n``, and ``\\Z`` would not save it because the break was already
# consumed. ``1e2`` and ``+10.5`` still match.
_AMOUNT_TEXT = re.compile(
    r"[ \t]*[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?[ \t]*\Z",
    re.ASCII,
)


class InexactAmount(ValueError):
    """A cell cannot be represented at the spec scale without rounding."""

    def __init__(self, display: str) -> None:
        shown = _clip(display)
        super().__init__(shown)
        self.display = shown


def to_decimal(raw: object, scale: int, cast: str | None) -> Decimal | None:
    """Return ``raw`` as an exact decimal at ``scale``, or None for a blank.

    ``cast`` is used only when ``raw`` is a float. Strings, integers, and
    decimals must already fit the scale.
    """
    if raw is None:
        return None
    if isinstance(raw, str):
        if raw.strip(" \t") == "":
            return None
        if _AMOUNT_TEXT.fullmatch(raw) is None:
            raise InexactAmount(raw.strip())
        text = raw.strip()
        try:
            value = Decimal(text)
        except InvalidOperation:
            raise InexactAmount(text) from None
        return _exact(value, scale, text)
    if isinstance(raw, bool):
        raise InexactAmount(str(raw))
    if isinstance(raw, int):
        return _exact(Decimal(raw), scale, str(raw))
    if isinstance(raw, Decimal):
        return _exact(raw, scale, _decimal_display(raw))
    if isinstance(raw, float):
        return _from_float(raw, scale, cast, _float_display(raw))
    raise InexactAmount(str(raw))


def _exact(value: Decimal, scale: int, display: str) -> Decimal:
    if not value.is_finite():
        raise InexactAmount(display)
    quantum = Decimal(1).scaleb(-scale)
    with localcontext(_CONTEXT):
        try:
            fitted = value.quantize(quantum)
        except InvalidOperation:
            raise InexactAmount(display) from None
    if fitted != value:
        raise InexactAmount(display)
    return _fit_precision(fitted, display)


def _from_float(raw: float, scale: int, cast: str | None, display: str) -> Decimal:
    if cast is None or cast not in CAST_ROUNDING:
        raise InexactAmount(display)
    if not math.isfinite(raw):
        raise InexactAmount(display)
    quantum = Decimal(1).scaleb(-scale)
    with localcontext(_CONTEXT):
        try:
            # Shortest round-trip spelling, not the exact binary expansion.
            fitted = Decimal(repr(raw)).quantize(quantum, rounding=CAST_ROUNDING[cast])
        except InvalidOperation:
            raise InexactAmount(display) from None
    return _fit_precision(fitted, display)


def _fit_precision(fitted: Decimal, display: str) -> Decimal:
    if len(fitted.as_tuple().digits) > DECIMAL_PRECISION:
        raise InexactAmount(display)
    return fitted


def _decimal_display(value: Decimal) -> str:
    if value.is_nan():
        return "NaN"
    if value.is_infinite():
        return "Infinity" if not value.is_signed() else "-Infinity"
    return format(value, "f")


def _float_display(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return format(Decimal.from_float(value), "f")


def _clip(text: str) -> str:
    if len(text) <= DISPLAY_LIMIT:
        return text
    return text[:DISPLAY_LIMIT] + "..."
