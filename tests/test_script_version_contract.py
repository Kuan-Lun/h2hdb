from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    ("base_version", "candidate_version", "error"),
    [
        ("0.21.2", "0.21.3", None),
        ("0.21.2", "0.21.2", None),
        ("0.21.2.9", "0.21.3", "must use X.Y.Z"),
        ("0.21.2.9.1", "0.21.3", "must use X.Y.Z"),
        ("0.21.2.9", "0.21.2.9", "must use X.Y.Z"),
        ("0.21.2", "0.21.2.10", "must use X.Y.Z"),
        ("0.21.2", "0.21.4", "expected project version 0.21.3"),
    ],
)
def test_version_gate_enforces_three_part_base_and_candidate(
    monkeypatch: pytest.MonkeyPatch,
    base_version: str,
    candidate_version: str,
    error: str | None,
) -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "check-version.py"
    specification = importlib.util.spec_from_file_location("version_contract", script)
    assert specification is not None
    assert specification.loader is not None
    policy = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(policy)
    version_changed = base_version != candidate_version
    audits: list[str] = []

    def git(*arguments: str) -> str:
        if arguments[0] == "rev-parse":
            return "task-commit"
        if arguments == ("write-tree",):
            return "candidate-tree"
        if arguments[0] == "diff":
            return (
                "src/runtime.py\npyproject.toml" if version_changed else "tests/test.py"
            )
        assert arguments[0] == "log"
        return "fix: update runtime" if version_changed else "test: add coverage"

    def document(tree: str) -> dict[str, object]:
        return {
            "project": {
                "name": "version-contract-fixture",
                "version": base_version if tree == "HEAD" else candidate_version,
            },
            "tool": {"h2h": {"version": {"release-paths": ["src/**"]}}},
        }

    def audit(tree: str, candidate: dict[str, object], version: str) -> None:
        assert tree == "candidate-tree"
        assert candidate == document(tree)
        audits.append(version)

    monkeypatch.setattr(policy, "_git", git)
    monkeypatch.setattr(policy, "_load_toml", document)
    monkeypatch.setattr(policy, "_validate_audit", audit)
    monkeypatch.setattr(sys, "argv", [str(script), "--index"])
    monkeypatch.setenv("WORKFLOW_MERGE_TASK_REF", "task/test-version")

    if error is not None:
        with pytest.raises(ValueError, match=error):
            policy.main()
        assert audits == []
    else:
        assert policy.main() == 0
        assert audits == ([candidate_version] if version_changed else [])
