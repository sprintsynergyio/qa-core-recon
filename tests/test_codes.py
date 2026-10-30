import dataclasses

import pytest

from qa_core_recon.exit_codes import ExitCode
from qa_core_recon.refusals import Refusal, RefusalCode, RefusalError


def test_exit_code_values():
    assert [(code.name, code.value) for code in ExitCode] == [
        ("MATCH", 0),
        ("MISMATCH", 1),
        ("REFUSED", 2),
    ]


def test_refusal_code_values_in_order():
    assert [code.value for code in RefusalCode] == [
        "missing-key",
        "float-amount",
        "inexact-amount",
        "tolerance-without-reason",
        "unmapped-column",
        "path-not-allowed",
        "sample-only",
    ]


def test_refusal_is_frozen_and_carried_by_error():
    refusal = Refusal(RefusalCode.MISSING_KEY, "spec has no keys")
    with pytest.raises(dataclasses.FrozenInstanceError):
        refusal.message = "changed"  # type: ignore[misc]

    error = RefusalError(refusal)
    assert error.refusal is refusal
    assert str(error) == "missing-key: spec has no keys"
