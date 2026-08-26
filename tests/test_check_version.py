from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
CHECK_VERSION = ROOT / "scripts" / "check-version.py"


def _load_module(name: str, path: Path) -> ModuleType:
    specification = importlib.util.spec_from_file_location(name, path)
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


policy = _load_module("h2hdb_check_version", CHECK_VERSION)


def test_legacy_four_part_version_normalizes_to_three_part_baseline() -> None:
    assert policy._base_version("0.23.0.11") == (0, 23, 0)
    assert policy._expected_version((0, 23, 0), breaking=False, feature=False) == (
        0,
        23,
        1,
    )
    assert policy._expected_version((0, 23, 0), breaking=True, feature=False) == (
        0,
        24,
        0,
    )


def test_candidate_version_must_use_exactly_three_parts() -> None:
    assert policy._parse_version("0.23.1") == (0, 23, 1)
    with pytest.raises(ValueError, match="must use X.Y.Z"):
        policy._parse_version("0.23.0.12")


def test_post_one_feature_uses_semantic_minor_version() -> None:
    assert policy._expected_version((1, 4, 2), breaking=False, feature=True) == (
        1,
        5,
        0,
    )


def test_index_candidate_uses_merge_task_ref_before_merge_head_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKFLOW_MERGE_TASK_REF", "task/example")
    calls: list[tuple[str, ...]] = []

    def git(*arguments: str, input_text: str | None = None) -> str:
        assert input_text is None
        calls.append(arguments)
        if arguments == (
            "rev-parse",
            "--verify",
            "task/example^{commit}",
        ):
            return "task-commit"
        assert arguments == ("write-tree",)
        return "candidate-tree"

    monkeypatch.setattr(policy, "_git", git)
    arguments = argparse.Namespace(index=True, base=None, candidate="HEAD")

    assert policy._candidate(arguments) == (
        "HEAD",
        "candidate-tree",
        "HEAD..task-commit",
    )
    assert calls == [
        ("rev-parse", "--verify", "task/example^{commit}"),
        ("write-tree",),
    ]
