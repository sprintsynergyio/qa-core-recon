"""Deterministic reconciliation reports.

``report.json`` is UTF-8 JSON with sorted keys, indent 2, and one trailing
newline. ``report.md`` is a short summary in simple English. Headings are
Title Case. Neither file contains a timestamp, and neither file contains a
path that was not already in the inputs. The output directory itself is not
written into the report.

Two runs on the same inputs therefore write byte-identical files.

``--out`` is a local directory. It is created when it is missing. It is not
checked against the input allowlist. ``s3://`` is rejected. An existing
directory that already holds files is rejected unless ``--force`` is set.
A leftover ``.report.json.<pid>.tmp`` or ``.report.md.<pid>.tmp`` file from
this tool is removed and does not count as content. Other names, and
symlinks, are left in place. Files are written to temporary names in that
directory,
flushed, and then renamed into place. If either rename fails, both final
report files are removed. When this command created the directory and the
write fails, the empty directory is removed.
"""

import hashlib
import json
import os
import re
from pathlib import Path

from qa_core_recon import __version__
from qa_core_recon.compare import overall_status
from qa_core_recon.refusals import Refusal

_STATUS_LABELS = {"match": "Match", "mismatch": "Mismatch", "refused": "Refused"}
_CHECK_HEADINGS = {
    "row_count": "Row Count",
    "amount_sum": "Amount Sum",
    "null_keys": "Null Keys",
    "duplicate_keys": "Duplicate Keys",
    "key_diff": "Key Diff",
    "changed_columns": "Changed Columns",
}


class OutputError(Exception):
    """The report directory cannot be used. No report file is left behind."""


def spec_sha256(path: Path) -> str:
    """Return the hex SHA-256 of the spec file bytes."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


# publish writes ``.report.json.<pid>.tmp`` and ``.report.md.<pid>.tmp``.
_REPORT_TEMP = re.compile(r"^\.report\.(json|md)\.\d+\.tmp$")


def _is_report_temp(name: str) -> bool:
    """True for a temporary report file left by a crashed publish."""
    return _REPORT_TEMP.fullmatch(name) is not None


def _clear_report_temps(directory: Path) -> None:
    for child in directory.iterdir():
        # is_file follows links. A symlink with this name is the user's file.
        if child.is_symlink() or not _is_report_temp(child.name):
            continue
        if not child.is_file():
            continue
        child.unlink(missing_ok=True)


def ensure_output_available(path_text: str, *, force: bool) -> Path:
    """Return the local output directory, or raise OutputError without creating it.

    A missing directory is accepted here and created later, when the report is
    published. The parent must already exist. Leftover publish temp files
    (``.report.json.<pid>.tmp`` and ``.report.md.<pid>.tmp``) are removed and
    do not make the directory count as non-empty. Other names and symlinks stay.
    """
    if path_text.startswith("s3://") or path_text.startswith("s3:"):
        raise OutputError(f"s3 output is not supported: {path_text}")
    if _has_scheme(path_text):
        raise OutputError(f"output path must be a local directory: {path_text}")
    path = Path(path_text)
    try:
        exists = path.exists()
    except OSError as exc:
        raise OutputError(_cannot_use(path, exc)) from None
    if exists:
        if not path.is_dir():
            raise OutputError(f"{path} is a file")
        try:
            _clear_report_temps(path)
            occupied = any(path.iterdir())
        except OSError as exc:
            raise OutputError(_cannot_use(path, exc)) from None
        if occupied and not force:
            raise OutputError(f"{path} is not empty; use --force to overwrite")
        return path
    parent = path.parent
    if parent.exists() and not parent.is_dir():
        raise OutputError(f"{path} cannot be created because a parent is not a directory")
    if not parent.is_dir():
        raise OutputError(f"parent directory does not exist: {parent}")
    return path


def build_report(
    *,
    spec_sha256: str,
    status: str,
    refusals: list[dict],
    checks: list[dict],
    row_counts: dict,
) -> dict:
    """Return the report object. Keys are sorted later, when the bytes are rendered."""
    return {
        "tool": "qa-core-recon",
        "version": __version__,
        "spec_sha256": spec_sha256,
        "status": status,
        "refusals": refusals,
        "checks": checks,
        "row_counts": row_counts,
    }


def build_result_report(digest: str, checks: list[dict], row_counts: dict) -> dict:
    """Return a match or mismatch report. A mismatch in any check wins."""
    return build_report(
        spec_sha256=digest,
        status=overall_status(check["status"] for check in checks),
        refusals=[],
        checks=checks,
        row_counts=row_counts,
    )


def build_refused_report(digest: str, refusal: Refusal) -> dict:
    """Return a refused report. Checks were not run, so row counts are null."""
    return build_report(
        spec_sha256=digest,
        status="refused",
        refusals=[{"code": refusal.code.value, "message": refusal.message}],
        checks=[],
        row_counts={"source": None, "target": None},
    )


def render_json(report: dict) -> bytes:
    """Return canonical JSON bytes: sorted keys, indent 2, UTF-8, one trailing newline."""
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    return (text + "\n").encode("utf-8")


def render_markdown(report: dict) -> str:
    """Return the markdown summary, ending with one newline."""
    status = report["status"]
    lines = [
        "# Reconciliation Report",
        "",
        "## Result",
        "",
        f"Status: {_STATUS_LABELS[status]}",
        "",
        f"Tool: {report['tool']}",
        f"Version: {report['version']}",
        f"Spec Hash: {report['spec_sha256']}",
        "",
    ]
    if status == "refused":
        lines.extend(
            [
                "The tool declined to reconcile these datasets.",
                "",
                "## Refusals",
                "",
            ]
        )
        for refusal in report["refusals"]:
            lines.append(f"- {refusal['code']}: {refusal['message']}")
        lines.extend(
            [
                "",
                "## Row Counts",
                "",
                "Source: not counted",
                "Target: not counted",
                "",
                "## Checks",
                "",
                "No checks were run.",
                "",
            ]
        )
    else:
        counts = report["row_counts"]
        lines.extend(
            [
                "## Row Counts",
                "",
                f"Source: {counts['source']}",
                f"Target: {counts['target']}",
                "",
                "## Checks",
                "",
            ]
        )
        for check in report["checks"]:
            lines.extend(_check_lines(check))
    return "\n".join(lines).rstrip("\n") + "\n"


def reject_output_overlap(out: Path, source_path: str, target_path: str) -> None:
    """Raise when ``out`` equals, contains, or sits inside a local dataset path.

    Writing the report into a dataset directory makes the next read treat
    ``report.json`` as another data file. ``s3://`` paths are not local, so
    they do not overlap a local output directory. A symlink loop on the
    output path is an output error. A symlink loop on the source or the
    target is a read error. Neither one is an unexpected error.
    """
    for side, dataset_path in (("source", source_path), ("target", target_path)):
        if _local_paths_overlap(out, dataset_path, side):
            raise OutputError(f"output path {out} overlaps {side} path {dataset_path}")


def _local_paths_overlap(out: Path, dataset_path: str, side: str) -> bool:
    if _has_scheme(dataset_path) or dataset_path.startswith("s3:"):
        return False
    # resolve follows symlinks and folds "..". absolute() leaves both in place,
    # so a link to the dataset, or "<data>/other/../src", would miss the check.
    # A symlink loop raises RuntimeError, not OSError.
    try:
        left = _resolve_parts(out)
    except _PathResolveError as exc:
        raise OutputError(f"cannot write {out}: {exc.detail}") from None
    try:
        right = _resolve_parts(Path(dataset_path))
    except _PathResolveError as exc:
        raise OutputError(f"cannot read {side} path {dataset_path!r}: {exc.detail}") from None
    return left == right or _is_prefix(left, right) or _is_prefix(right, left)


class _PathResolveError(Exception):
    def __init__(self, detail: str) -> None:
        self.detail = detail


def _resolve_parts(path: Path) -> tuple[str, ...]:
    try:
        return path.resolve(strict=False).parts
    except (OSError, RuntimeError) as exc:
        detail = getattr(exc, "strerror", None) or str(exc) or exc.__class__.__name__
        raise _PathResolveError(detail) from None


def _is_prefix(prefix: tuple[str, ...], path: tuple[str, ...]) -> bool:
    return len(path) > len(prefix) and path[: len(prefix)] == prefix


def publish(directory: Path, report: dict, markdown: str) -> None:
    """Create ``directory`` if needed and publish both report files.

    Both temporary files are written before either final name is replaced.
    If either rename fails, both final report files are removed so a failed
    publish never leaves one new file beside one old file.
    """
    created = not directory.exists()
    try:
        directory.mkdir(exist_ok=True)
    except OSError as exc:
        raise OutputError(_cannot_use(directory, exc)) from None
    tmp_json = directory / f".report.json.{os.getpid()}.tmp"
    tmp_md = directory / f".report.md.{os.getpid()}.tmp"
    final_json = directory / "report.json"
    final_md = directory / "report.md"
    try:
        _write_bytes(tmp_json, render_json(report))
        _write_bytes(tmp_md, markdown.encode("utf-8"))
        os.replace(tmp_json, final_json)
        os.replace(tmp_md, final_md)
    except OSError as exc:
        tmp_json.unlink(missing_ok=True)
        tmp_md.unlink(missing_ok=True)
        final_json.unlink(missing_ok=True)
        final_md.unlink(missing_ok=True)
        if created:
            _remove_if_empty(directory)
        raise OutputError(_cannot_use(directory, exc)) from None
    _fsync_directory(directory)


def _check_lines(check: dict) -> list[str]:
    name = check["name"]
    lines = [
        f"### {_CHECK_HEADINGS[name]}",
        "",
        f"Status: {_STATUS_LABELS[check['status']]}",
        "",
    ]
    details = check["details"]
    if name == "row_count":
        lines.extend(_row_count_lines(details))
    elif name == "amount_sum":
        lines.extend(_amount_lines(details))
    elif name == "null_keys":
        lines.extend(
            [
                f"Source rows with a null key: {details['source']}.",
                f"Target rows with a null key: {details['target']}.",
                "",
            ]
        )
    elif name == "duplicate_keys":
        lines.extend(_duplicate_lines(details))
    elif name == "key_diff":
        lines.extend(_key_diff_lines(details))
    elif name == "changed_columns":
        lines.extend(_changed_column_lines(details))
    return lines


def _row_count_lines(details: dict) -> list[str]:
    lines = [
        f"Source rows: {details['source']}. Target rows: {details['target']}.",
        "",
    ]
    if not details["group_by"]:
        return lines
    names = ", ".join(details["group_by"])
    lines.extend([f"Grouped by {names}.", ""])
    if not details["groups"]:
        lines.extend(["No groups were present on either side.", ""])
        return lines
    lines.extend(["Groups, in sorted order:", ""])
    for group in details["groups"]:
        sentence = (
            f"- {_group_label(group['group'])}. "
            f"Source rows: {group['source']}. Target rows: {group['target']}."
        )
        sentence += _missing_sentence(group["missing"])
        sentence += f" {_STATUS_LABELS[group['status']]}."
        lines.append(sentence)
    lines.append("")
    return lines


def _amount_lines(details: dict) -> list[str]:
    lines: list[str] = []
    if not details["columns"]:
        lines.extend(["No amount columns were declared.", ""])
        return lines
    for column in details["columns"]:
        parts = [
            f"Column {column['column']}, scale {column['scale']}.",
            f"Source sum: {_shown(column['source'])}.",
            f"Target sum: {_shown(column['target'])}.",
            f"Difference: {_shown(column['difference'])}.",
            f"Tolerance: {column['tolerance']}.",
        ]
        reason = column["reason"].strip()
        if reason:
            parts.append(f"Reason: {reason}.")
        parts.append(f"{_STATUS_LABELS[column['status']]}.")
        lines.extend([" ".join(parts), ""])
        if not column["groups"]:
            continue
        lines.extend(["Groups, in sorted order:", ""])
        for group in column["groups"]:
            sentence = (
                f"- {_group_label(group['group'])}. "
                f"Source sum: {_shown(group['source'])}. "
                f"Target sum: {_shown(group['target'])}. "
                f"Difference: {_shown(group['difference'])}."
            )
            sentence += _missing_sentence(group["missing"])
            sentence += f" {_STATUS_LABELS[group['status']]}."
            lines.append(sentence)
        lines.append("")
    return lines


def _duplicate_lines(details: dict) -> list[str]:
    lines = [f"Example limit: {details['example_limit']}.", ""]
    lines.extend(_duplicate_side("Source", details["keys"], details["source"]))
    lines.extend(_duplicate_side("Target", details["keys"], details["target"]))
    return lines


def _duplicate_side(label: str, keys: list[str], payload: dict) -> list[str]:
    count = payload["count"]
    examples = payload["examples"]
    lines = [f"{label} duplicate keys: {count}."]
    if count > len(examples):
        lines.append(f"The first {len(examples)} of {count} are listed in sorted order.")
    lines.append("")
    for example in examples:
        lines.append(f"- {_example_label(keys, example)}")
    if examples:
        lines.append("")
    return lines


def _key_diff_lines(details: dict) -> list[str]:
    lines: list[str] = []
    columns = details["columns"]
    if columns:
        lines.append("Compared columns, in hash order: " + ", ".join(columns) + ".")
    else:
        lines.append("No compared columns were declared.")
    lines.append("")
    for rule in details["rules"]:
        lines.append(rule)
    lines.append("")
    if details["duplicates_present"]:
        lines.append("Duplicate keys were found, so each distinct key is counted once.")
        lines.append("")
    lines.append(f"Example limit: {details['example_limit']}.")
    lines.append("")
    for label, name in (("Missing", "missing"), ("Extra", "extra"), ("Changed", "changed")):
        lines.extend(_key_bucket_lines(label, details["keys"], details[name]))
    return lines


def _key_bucket_lines(label: str, keys: list[str], payload: dict) -> list[str]:
    count = payload["count"]
    examples = payload["examples"]
    lines = [f"{label} keys: {count}."]
    if count > len(examples):
        lines.append(f"The first {len(examples)} of {count} are listed in sorted order.")
    lines.append("")
    for example in examples:
        lines.append(f"- {_example_label(keys, example)}")
    if examples:
        lines.append("")
    return lines


def _changed_column_lines(details: dict) -> list[str]:
    lines = [
        f"Example limit: {details['example_limit']}.",
        "",
        details["rule"],
        "",
    ]
    columns = details["columns"]
    if not columns:
        lines.extend(["No compared column differs.", ""])
        return lines
    for column in columns:
        count = column["count"]
        examples = column["examples"]
        lines.append(f"Column {column['column']}. Distinct keys that differ: {count}.")
        if count > len(examples):
            lines.append(f"The first {len(examples)} of {count} are listed in sorted order.")
        lines.append("")
        for example in examples:
            key = _example_label(details["keys"], example["key"])
            lines.append(f"- {key}. Source: {example['source']}. Target: {example['target']}.")
        if examples:
            lines.append("")
    return lines


def _missing_sentence(missing: str | None) -> str:
    if missing == "source":
        return " This group is missing on the source."
    if missing == "target":
        return " This group is missing on the target."
    return ""


def _group_label(group: dict) -> str:
    parts = []
    for name, value in group.items():
        shown = "null" if value is None else str(value)
        parts.append(f"{name}={shown}")
    return ", ".join(parts)


def _example_label(keys: list[str], example: list) -> str:
    parts = []
    for name, value in zip(keys, example, strict=True):
        shown = "null" if value is None else str(value)
        parts.append(f"{name}={shown}")
    return ", ".join(parts)


def _shown(value: object) -> str:
    if value is None:
        return "none"
    return str(value)


def _has_scheme(path_text: str) -> bool:
    scheme, separator, _rest = path_text.partition("://")
    return bool(separator) and scheme != "" and "/" not in scheme and "\\" not in scheme


def _write_bytes(path: Path, payload: bytes) -> None:
    with path.open("wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _fsync_directory(directory: Path) -> None:
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        return
    finally:
        os.close(descriptor)


def _remove_if_empty(directory: Path) -> None:
    try:
        directory.rmdir()
    except OSError:
        return


def _cannot_use(path: Path, exc: OSError) -> str:
    detail = exc.strerror or exc.__class__.__name__
    return f"cannot write {path}: {detail}"
