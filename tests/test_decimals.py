from decimal import Decimal

import pytest

from qa_core_recon.decimals import DISPLAY_LIMIT, InexactAmount, to_decimal


def test_exact_strings_quantize_to_the_spec_scale():
    assert to_decimal("10", 2, None) == Decimal("10.00")
    assert to_decimal("10.0", 2, None) == Decimal("10.00")
    assert to_decimal("10.5", 2, None) == Decimal("10.50")
    assert to_decimal("10.50", 2, None) == Decimal("10.50")
    assert to_decimal("10.500", 2, None) == Decimal("10.50")
    assert to_decimal("+10.00", 2, None) == Decimal("10.00")
    assert to_decimal("-1.20", 2, None) == Decimal("-1.20")
    assert to_decimal("1e2", 2, None) == Decimal("100.00")
    assert to_decimal("1.005e1", 2, None) == Decimal("10.05")
    assert to_decimal("0", 0, None) == Decimal("0")
    assert to_decimal("10.0", 0, None) == Decimal("10")
    fitted = to_decimal("10.500", 2, None)
    assert fitted is not None
    assert fitted.as_tuple().exponent == -2


def test_integer_and_decimal_inputs_stay_exact():
    assert to_decimal(1050, 2, None) == Decimal("1050.00")
    assert to_decimal(Decimal("10.500"), 2, None) == Decimal("10.50")


def test_blank_is_null_and_not_zero():
    assert to_decimal(None, 2, None) is None
    assert to_decimal("", 2, None) is None
    assert to_decimal("   ", 2, None) is None


def test_extra_fractional_digits_are_refused():
    for text in ("10.005", "10.00500", "1.0005e1", "0.001"):
        with pytest.raises(InexactAmount) as caught:
            to_decimal(text, 2, None)
        assert caught.value.display == text


def test_scale_zero_rejects_a_fraction():
    with pytest.raises(InexactAmount) as caught:
        to_decimal("0.5", 0, None)
    assert caught.value.display == "0.5"


def test_unparseable_and_non_finite_text_is_refused():
    with pytest.raises(InexactAmount) as caught:
        to_decimal("abc", 2, None)
    assert caught.value.display == "abc"
    with pytest.raises(InexactAmount) as caught:
        to_decimal("NaN", 2, None)
    assert caught.value.display == "NaN"
    with pytest.raises(InexactAmount):
        to_decimal("Infinity", 2, None)


def test_cast_does_not_round_a_string_or_a_decimal():
    with pytest.raises(InexactAmount) as caught:
        to_decimal("10.005", 2, "half-up")
    assert caught.value.display == "10.005"
    with pytest.raises(InexactAmount):
        to_decimal("2.5", 0, "half-up")
    with pytest.raises(InexactAmount):
        to_decimal(Decimal("10.005"), 2, "half-even")


def test_float_cast_uses_the_named_mode():
    assert to_decimal(2.5, 0, "half-even") == Decimal("2")
    assert to_decimal(2.5, 0, "half-up") == Decimal("3")
    with pytest.raises(InexactAmount):
        to_decimal(2.5, 0, None)
    with pytest.raises(InexactAmount) as caught:
        to_decimal(float("nan"), 2, "half-even")
    assert caught.value.display == "NaN"
    with pytest.raises(InexactAmount) as caught:
        to_decimal(float("inf"), 2, "half-up")
    assert caught.value.display == "Infinity"


def test_bool_is_not_an_integer_amount():
    with pytest.raises(InexactAmount):
        to_decimal(True, 2, None)


def test_precision_boundary_uses_38_digits():
    assert to_decimal("9" * 36 + ".99", 2, None) == Decimal("9" * 36 + ".99")
    assert to_decimal("1" + "0" * 35, 2, None) == Decimal("1" + "0" * 35)
    with pytest.raises(InexactAmount):
        to_decimal("1" + "0" * 36, 2, None)
    assert to_decimal("9" * 38, 0, None) == Decimal("9" * 38)
    with pytest.raises(InexactAmount):
        to_decimal("9" * 39, 0, None)


def test_long_offending_text_is_clipped():
    text = "a" * (DISPLAY_LIMIT + 1)
    with pytest.raises(InexactAmount) as caught:
        to_decimal(text, 2, None)
    assert caught.value.display == "a" * DISPLAY_LIMIT + "..."
