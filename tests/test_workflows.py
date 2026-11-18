"""Workflow files pin every GitHub Action to a commit SHA."""

import re
from pathlib import Path

_USES = re.compile(r"(?m)^[ \t]*(?:-[ \t]*)?uses:[ \t]*(\S+)")
_PINNED = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}$")


def test_workflow_actions_are_pinned_to_commit_shas():
    root = Path(__file__).resolve().parents[1] / ".github" / "workflows"
    files = sorted(root.glob("*.yml"))
    assert files, "expected workflow files"
    for path in files:
        uses = _USES.findall(path.read_text(encoding="utf-8"))
        assert uses, f"{path.name} has no actions"
        for action in uses:
            assert _PINNED.fullmatch(action), f"{path.name} uses {action}"
            _owner, sha = action.split("@", 1)
            assert len(sha) == 40
