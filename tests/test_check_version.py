from __future__ import annotations

import argparse
import copy
import importlib.util
import sys
import tomllib
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


@pytest.mark.parametrize(
    "removed_path",
    (
        "scripts/upgrade-observation-upload-time-schema.py",
        "scripts/build-observation-upload-time-upgrade-bundle.py",
        "scripts/finish-schema8-cleanup.py",
    ),
)
@pytest.mark.parametrize("target_version", ("0.43.0", "0.43.1", "0.44.0"))
def test_retired_offline_commands_require_a_breaking_release(
    monkeypatch: pytest.MonkeyPatch, removed_path: str, target_version: str
) -> None:
    base = tomllib.loads((ROOT / "pyproject.toml").read_text())
    base["project"]["version"] = "0.43.0"
    candidate = copy.deepcopy(base)
    candidate["project"]["version"] = target_version
    monkeypatch.setattr(sys, "argv", [str(CHECK_VERSION)])
    monkeypatch.setattr(
        policy, "_candidate", lambda _args: ("base", "candidate", "task")
    )
    monkeypatch.setattr(
        policy, "_load_toml", lambda tree: base if tree == "base" else candidate
    )

    def git(*arguments: str) -> str:
        if arguments[0] == "diff":
            return removed_path
        assert arguments == ("log", "--format=%B%x00", "task")
        return "refactor(upgrade)!: retire completed offline conversion tooling"

    audits: list[str] = []
    monkeypatch.setattr(policy, "_git", git)
    monkeypatch.setattr(
        policy, "_validate_audit", lambda _tree, _doc, version: audits.append(version)
    )
    if target_version == "0.44.0":
        assert policy.main() == 0
        assert audits == [target_version]
    else:
        message = (
            "release surface changed without a project version bump"
            if target_version == "0.43.0"
            else "expected project version 0.44.0"
        )
        with pytest.raises(ValueError, match=message):
            policy.main()
        assert not audits
