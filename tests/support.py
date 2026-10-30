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
) -> dict:
    return {
        "version": 1,
        "source": {"path": str(source), "format": source_format},
        "target": {"path": str(target), "format": target_format},
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
