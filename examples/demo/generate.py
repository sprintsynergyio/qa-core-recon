"""Write the synthetic demo payments dataset.

The rows are made up. Amounts are decimal strings with two digits after the
point. Dates, account ids, and transaction types follow a fixed cycle.
Amounts come from ``random.Random(SEED)`` in the standard library, so every
run writes the same bytes.

Parquet is not written. A Spark part file name includes a random id, so those
bytes would change between runs. The demo specs read these CSV files. The
Iceberg spec's table is built by the test from the matching CSV.
"""

import argparse
import csv
import io
import random
from pathlib import Path

SEED = 20260311
ROW_COUNT = 200
COLUMNS = ("txn_id", "account_id", "txn_type", "amount", "currency", "txn_date")
TXN_TYPES = ("payment", "refund", "transfer", "fee")
CURRENCIES = ("USD", "EUR", "GBP")
DATES = ("2024-03-01", "2024-03-02", "2024-03-03", "2024-03-04", "2024-03-05")
MISSING_DATE = "2024-03-01"
# One row on each date: TXN000001 through TXN000005.
CHANGED_IDS = frozenset(f"TXN{index:06d}" for index in range(1, 6))
CHANGED_CENTS = 125
SUM_SHIFT_CENTS = 50
EXTRA_ROWS = (
    ("TXN900001", "ACC2001", "payment", 1500, "USD", "2024-03-06"),
    ("TXN900002", "ACC2002", "payment", 2550, "EUR", "2024-03-06"),
    ("TXN900003", "ACC2003", "transfer", 4025, "GBP", "2024-03-06"),
)
CASES = ("match", "missing-rows", "extra-rows", "changed-rows", "sum-mismatch")


def format_cents(cents: int) -> str:
    """Return a decimal string with two digits after the point."""
    sign = "-" if cents < 0 else ""
    number = abs(cents)
    return f"{sign}{number // 100}.{number % 100:02d}"


def parse_cents(amount: str) -> int:
    """Return the cent count for a decimal string from :func:`format_cents`."""
    sign = 1
    text = amount
    if text.startswith("-"):
        sign = -1
        text = text[1:]
    whole, fraction = text.split(".")
    return sign * (int(whole) * 100 + int(fraction))


def base_rows() -> list[dict[str, str]]:
    """Return the shared source rows. Amounts are the only seeded values."""
    rng = random.Random(SEED)
    rows = []
    for index in range(1, ROW_COUNT + 1):
        txn_type = TXN_TYPES[(index - 1) % len(TXN_TYPES)]
        cents = rng.randrange(100, 200_000)
        if txn_type == "refund":
            cents = -cents
        rows.append(
            {
                "txn_id": f"TXN{index:06d}",
                "account_id": f"ACC{1000 + (index - 1) % 20}",
                "txn_type": txn_type,
                "amount": format_cents(cents),
                "currency": CURRENCIES[(index - 1) % len(CURRENCIES)],
                "txn_date": DATES[(index - 1) % len(DATES)],
            }
        )
    return rows


def extra_rows() -> list[dict[str, str]]:
    """Return three rows the source does not have, on a later date."""
    rows = []
    for txn_id, account_id, txn_type, cents, currency, txn_date in EXTRA_ROWS:
        rows.append(
            {
                "txn_id": txn_id,
                "account_id": account_id,
                "txn_type": txn_type,
                "amount": format_cents(cents),
                "currency": currency,
                "txn_date": txn_date,
            }
        )
    return rows


def _copy(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [dict(row) for row in rows]


def missing_target(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Drop every row on the missing date."""
    return [row for row in _copy(rows) if row["txn_date"] != MISSING_DATE]


def changed_target(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Keep every key. Change the amount and type on the five chosen rows."""
    copied = _copy(rows)
    for row in copied:
        if row["txn_id"] not in CHANGED_IDS:
            continue
        row["amount"] = format_cents(parse_cents(row["amount"]) + CHANGED_CENTS)
        row["txn_type"] = "adjustment"
    return copied


def sum_mismatch_target(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Add a fixed amount to every target row. The row count stays the same."""
    copied = _copy(rows)
    for row in copied:
        row["amount"] = format_cents(parse_cents(row["amount"]) + SUM_SHIFT_CENTS)
    return copied


def case_rows() -> dict[str, tuple[list[dict[str, str]], list[dict[str, str]]]]:
    """Return source and target rows for each demo case."""
    source = base_rows()
    return {
        "match": (source, _copy(source)),
        "missing-rows": (source, missing_target(source)),
        "extra-rows": (source, _copy(source) + extra_rows()),
        "changed-rows": (source, changed_target(source)),
        "sum-mismatch": (source, sum_mismatch_target(source)),
    }


def render_csv(rows: list[dict[str, str]]) -> bytes:
    """Return UTF-8 CSV bytes with a header and a trailing newline."""
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def write_dataset(out: Path) -> None:
    """Write one source file and one target file for each case."""
    for name, (source, target) in case_rows().items():
        directory = out / name
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "source.csv").write_bytes(render_csv(source))
        (directory / "target.csv").write_bytes(render_csv(target))


def main(argv: list[str] | None = None) -> int:
    """Write the dataset under ``--out`` and return 0."""
    parser = argparse.ArgumentParser(description="Write the synthetic demo payments dataset.")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent / "data",
        help="directory for the case CSV files (default: examples/demo/data)",
    )
    args = parser.parse_args(argv)
    write_dataset(args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
