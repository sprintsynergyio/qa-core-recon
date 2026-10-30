"""Tiny dataset and spec builders for reader tests."""

from pathlib import Path

import yaml

from qa_core_recon.spec import Spec, parse_spec

HEADER = "txn_id,amount,day,note\n"


def spec_data(
    tmp_path: Path,
    source: Path,
    target: Path,
    *,
    source_format: str = "csv",
    target_format: str = "parquet",
    source_table: str | None = None,
    target_table: str | None = None,
    source_snapshot: int | None = None,
    target_snapshot: int | None = None,
) -> dict:
    source_dataset: dict = {"path": str(source), "format": source_format}
    target_dataset: dict = {"path": str(target), "format": target_format}
    if source_table is not None:
        source_dataset["table"] = source_table
    if target_table is not None:
        target_dataset["table"] = target_table
    if source_snapshot is not None:
        source_dataset["snapshot_id"] = source_snapshot
    if target_snapshot is not None:
        target_dataset["snapshot_id"] = target_snapshot
    return {
        "version": 1,
        "source": source_dataset,
        "target": target_dataset,
        "keys": ["txn_id"],
        "amounts": [{"column": "amount", "scale": 2}],
        "group_by": ["day"],
        "hash_columns": ["note"],
        "tolerance": {"value": "0", "reason": ""},
        "sampled": False,
        "allowlist": [str(tmp_path)],
    }


def parse_data(data: dict) -> Spec:
    return parse_spec(yaml.safe_dump(data))


def write_spec_file(tmp_path: Path, data: dict) -> Path:
    path = tmp_path / "recon.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def write_csv(path: Path, body: str, header: str = HEADER) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(header + body, encoding="utf-8")
