"""Reasons the tool declines to reconcile instead of reporting a result."""

from dataclasses import dataclass
from enum import StrEnum


class RefusalCode(StrEnum):
    MISSING_KEY = "missing-key"
    FLOAT_AMOUNT = "float-amount"
    TOLERANCE_WITHOUT_REASON = "tolerance-without-reason"
    UNMAPPED_COLUMN = "unmapped-column"
    PATH_NOT_ALLOWED = "path-not-allowed"
    SAMPLE_ONLY = "sample-only"


@dataclass(frozen=True)
class Refusal:
    code: RefusalCode
    message: str


class RefusalError(Exception):
    """Raised to stop a run with exactly one refusal."""

    def __init__(self, refusal: Refusal) -> None:
        super().__init__(f"{refusal.code}: {refusal.message}")
        self.refusal = refusal
