"""Workflow files pin every GitHub Action to a commit SHA."""

import re
from pathlib import Path

# Block style (`uses: owner/repo@sha`) and flow style (`{uses: owner/repo@sha}`).
_USES = re.compile(r"uses:\s*([^,\s#}\"']+)")
_PINNED = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}$")


def action_uses(text: str) -> list[str]:
    """Return every `uses` value in a workflow document."""
    return _USES.findall(text)


def workflow_files(directory: Path) -> list[Path]:
    """Return `.yml` and `.yaml` workflow files in `directory`."""
    return sorted(directory.glob("*.yml")) + sorted(directory.glob("*.yaml"))


def test_flow_style_uses_is_read():
    sha = "a" * 40
    text = "{uses: actions/checkout@" + sha + ", with: {fetch-depth: 1}}"
    assert action_uses(text) == [f"actions/checkout@{sha}"]
    block = f"      - uses: actions/setup-python@{sha} # v6.3.0\n"
    assert action_uses(block) == [f"actions/setup-python@{sha}"]


def test_yaml_workflow_files_are_scanned(tmp_path):
    sha = "b" * 40
    path = tmp_path / "extra.yaml"
    path.write_text("- {uses: actions/setup-java@" + sha + "}\n", encoding="utf-8")
    files = workflow_files(tmp_path)
    assert files == [path]
    assert action_uses(path.read_text(encoding="utf-8")) == [f"actions/setup-java@{sha}"]


def test_workflow_actions_are_pinned_to_commit_shas():
    root = Path(__file__).resolve().parents[1] / ".github" / "workflows"
    files = workflow_files(root)
    assert files, "expected workflow files"
    for path in files:
        uses = action_uses(path.read_text(encoding="utf-8"))
        assert uses, f"{path.name} has no actions"
        for action in uses:
            assert _PINNED.fullmatch(action), f"{path.name} uses {action}"
            _owner, sha = action.split("@", 1)
            assert len(sha) == 40
