"""Process exit codes returned by the CLI."""

from enum import IntEnum


class ExitCode(IntEnum):
    MATCH = 0
    MISMATCH = 1
    REFUSED = 2
