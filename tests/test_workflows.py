"""Workflow files pin every GitHub Action to a commit SHA."""

import re
from pathlib import Path

import yaml

_PINNED = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}$")


def uses_values(text: str) -> list[str]:
    """Return every string `uses` value in a workflow document.

    The document is parsed as YAML, so quoted, single-quoted, and flow-style
    values are included. A regex that stops at a quote would miss them.
    """
    found: list[str] = []

    def walk(node: object) -> None:
        if isinstance(node, dict):
            value = node.get("uses")
            if isinstance(value, str):
                found.append(value)
            for item in node.values():
                walk(item)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(yaml.safe_load(text))
    return found


def workflow_files(directory: Path) -> list[Path]:
    """Return `.yml` and `.yaml` workflow files in `directory`."""
    return sorted(directory.glob("*.yml")) + sorted(directory.glob("*.yaml"))


def test_quoted_and_flow_style_uses_are_read():
    sha = "c" * 40
    action = f"actions/checkout@{sha}"
    samples = [
        f'uses: "{action}"\n',
        f"uses: '{action}'\n",
        f'- {{uses: "{action}"}}\n',
        f"- {{uses: '{action}'}}\n",
        "jobs:\n  build:\n    steps:\n"
        f'      - uses: "{action}"\n'
        "      - name: plain\n"
        f"        uses: '{action}'\n",
    ]
    for sample in samples[:-1]:
        assert uses_values(sample) == [action]
    assert uses_values(samples[-1]) == [action, action]
    quoted_tag = 'jobs:\n  build:\n    steps:\n      - uses: "actions/checkout@v5"\n'
    assert uses_values(quoted_tag) == ["actions/checkout@v5"]
    assert _PINNED.fullmatch("actions/checkout@v5") is None


def test_yaml_workflow_files_are_scanned(tmp_path):
    sha = "b" * 40
    path = tmp_path / "extra.yaml"
    path.write_text(
        f'jobs:\n  build:\n    steps:\n      - uses: "actions/setup-java@{sha}"\n',
        encoding="utf-8",
    )
    files = workflow_files(tmp_path)
    assert files == [path]
    assert uses_values(path.read_text(encoding="utf-8")) == [f"actions/setup-java@{sha}"]


def test_workflow_actions_are_pinned_to_commit_shas():
    root = Path(__file__).resolve().parents[1] / ".github" / "workflows"
    files = workflow_files(root)
    assert files, "expected workflow files"
    for path in files:
        uses = uses_values(path.read_text(encoding="utf-8"))
        assert uses, f"{path.name} has no actions"
        for action in uses:
            assert _PINNED.fullmatch(action), f"{path.name} uses {action}"
            _owner, sha = action.split("@", 1)
            assert len(sha) == 40
