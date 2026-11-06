"""Comparison and exit-code rules. These do not read a dataset."""

import math
from decimal import Decimal

import pytest

from qa_core_recon.compare import (
    GROUP_NAN,
    absolute_difference,
    add_exact,
    decimal_fits,
    exit_code_for,
    format_decimal,
    format_tolerance,
    normalize_group_value,
    overall_status,
    sort_token,
    sums_match,
)
from qa_core_recon.decimals import DECIMAL_PRECISION
from qa_core_recon.exit_codes import ExitCode
from qa_core_recon.report import render_json, render_markdown


def test_exit_code_for_maps_each_report_status():
    assert exit_code_for("match") is ExitCode.MATCH
    assert exit_code_for("mismatch") is ExitCode.MISMATCH
    assert exit_code_for("refused") is ExitCode.REFUSED
    assert exit_code_for("mismatch") == 1
    with pytest.raises(ValueError):
        exit_code_for("failed")


def test_overall_status_is_mismatch_when_any_part_mismatches():
    assert overall_status(["match", "match"]) == "match"
    assert overall_status(["match", "mismatch", "match"]) == "mismatch"
    assert overall_status([]) == "match"


def test_sums_match_treats_tolerance_as_an_exact_decimal():
    assert sums_match(Decimal("10.50"), Decimal("10.50"), Decimal("0")) is True
    assert sums_match(Decimal("10.50"), Decimal("10.51"), Decimal("0")) is False
    # Equality is inside the range. A strict < would reject this pair.
    assert sums_match(Decimal("10.50"), Decimal("10.51"), Decimal("0.01")) is True
    assert sums_match(Decimal("10.50"), Decimal("10.52"), Decimal("0.01")) is False
    assert sums_match(Decimal("-1.50"), Decimal("-1.00"), Decimal("0.50")) is True
    assert sums_match(Decimal("-1.50"), Decimal("-1.00"), Decimal("0.49")) is False


def test_null_sum_is_not_zero():
    assert sums_match(None, None, Decimal("0")) is True
    assert sums_match(None, Decimal("0.00"), Decimal("0")) is False
    assert sums_match(None, Decimal("0.00"), Decimal("1")) is False
    assert sums_match(Decimal("0.00"), None, Decimal("0")) is False


def test_absolute_difference_keeps_full_precision():
    source = Decimal("9" * 36 + ".99")
    target = Decimal("0.01")
    assert absolute_difference(source, target) == Decimal("9" * 36 + ".98")


def test_decimal_fits_rejects_one_unit_past_the_maximum():
    maximum = Decimal("9" * (DECIMAL_PRECISION - 2) + ".99")
    assert decimal_fits(maximum, 2) is True
    assert format_decimal(maximum, 2) == "9" * (DECIMAL_PRECISION - 2) + ".99"
    overflow = add_exact(maximum, Decimal("0.01"))
    assert decimal_fits(overflow, 2) is False
    with pytest.raises(ValueError):
        format_decimal(overflow, 2)


def test_format_decimal_uses_the_full_scale():
    assert format_decimal(Decimal("10.5"), 2) == "10.50"
    assert format_decimal(Decimal("1"), 2) == "1.00"
    assert format_decimal(Decimal("-1.2"), 2) == "-1.20"
    assert format_decimal(Decimal("-0"), 2) == "0.00"
    assert format_decimal(Decimal("0"), 0) == "0"
    assert format_tolerance(Decimal("0.010")) == "0.010"
    assert format_tolerance(Decimal("0")) == "0"


def test_float_nan_and_negative_zero_share_group_keys():
    assert normalize_group_value(float("nan")) is GROUP_NAN
    assert normalize_group_value(float("-nan")) is GROUP_NAN
    zero = normalize_group_value(-0.0)
    assert zero == 0.0
    assert str(zero) == "0.0"
    ordered = sorted([GROUP_NAN, -2.5, -0.0, 3.0, float("nan")], key=sort_token)
    assert ordered[0] == -2.5
    assert ordered[1] == 0.0
    assert ordered[2] == 3.0
    assert ordered[3] is GROUP_NAN
    assert math.isnan(ordered[4])


def test_sort_token_orders_nulls_first():
    values = ["b", None, "a", Decimal("2"), Decimal("10")]
    ordered = sorted(values, key=sort_token)
    assert ordered == [None, Decimal("2"), Decimal("10"), "a", "b"]


def test_render_json_is_canonical_utf8():
    raw = render_json({"b": 1, "a": "café", "nested": {"z": None, "m": "0.01"}})
    assert raw.endswith(b"\n")
    assert not raw.endswith(b"\n\n")
    assert "café".encode() in raw
    assert b"\\u00e9" not in raw
    text = raw.decode("utf-8")
    assert (
        text
        == '{\n  "a": "café",\n  "b": 1,\n  "nested": {\n    "m": "0.01",\n    "z": null\n  }\n}\n'
    )


def test_refused_markdown_uses_title_case_and_no_em_dash():
    from qa_core_recon.refusals import Refusal, RefusalCode
    from qa_core_recon.report import build_refused_report

    report = build_refused_report(
        "abc",
        Refusal(RefusalCode.MISSING_KEY, "the spec has no business key"),
    )
    markdown = render_markdown(report)
    assert "\u2014" not in markdown
    assert "\u2013" not in markdown
    assert markdown.endswith("\n")
    headings = [line.lstrip("#").strip() for line in markdown.splitlines() if line.startswith("#")]
    assert headings == [
        "Reconciliation Report",
        "Result",
        "Refusals",
        "Row Counts",
        "Checks",
    ]
    for heading in headings:
        assert heading == heading.title()
    assert "Status: Refused" in markdown
    assert "missing-key: `the spec has no business key`" in markdown
    assert "No checks were run." in markdown
    assert "not counted" in markdown
