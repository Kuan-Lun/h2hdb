from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest
from packaging.version import Version

ROOT = Path(__file__).resolve().parents[1]
RELEASE_GATE = ROOT / "scripts" / "release-gate.py"


def _load_module(name: str, path: Path) -> ModuleType:
    specification = importlib.util.spec_from_file_location(name, path)
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


gate = _load_module("h2hdb_release_gate", RELEASE_GATE)


@pytest.mark.parametrize(
    ("previous", "current", "expected"),
    [
        (None, "1.0", "initial"),
        ("1.0", "1.0", "same"),
        ("1.0", "1.0.1", "increase"),
        ("2.0", "1.9", "decrease"),
    ],
)
def test_release_gate_classifies_version_changes(
    previous: str | None, current: str, expected: str
) -> None:
    previous_version = Version(previous) if previous is not None else None
    assert gate._classify_version_change(previous_version, Version(current)) == expected


def test_release_gate_reads_and_rejects_project_versions() -> None:
    document = '[project]\nversion = "1.2.3"\n'
    assert gate._project_version(document, source="fixture") == Version("1.2.3")

    with pytest.raises(gate.ReleaseGateError, match="Cannot read project.version"):
        gate._project_version('[project]\nversion = "invalid version"\n', source="bad")


def test_release_gate_parses_pre_push_updates() -> None:
    update = gate._parse_push_updates(
        "refs/heads/master local refs/heads/master remote\n"
    )
    assert update == (
        gate.PushUpdate("refs/heads/master", "local", "refs/heads/master", "remote"),
    )

    with pytest.raises(gate.ReleaseGateError, match="Malformed pre-push input"):
        gate._parse_push_updates("only three fields")


def test_release_receipt_requires_the_exact_profile_tree_and_checks() -> None:
    document = {
        "schema_version": gate.RECEIPT_SCHEMA_VERSION,
        "profile": gate.RELEASE_PROFILE,
        "tree": "tree-1",
        "project_version": "1.2.3",
        "checks": list(gate.REQUIRED_CHECKS),
        "result": "passed",
    }
    assert gate._receipt_matches(document, tree="tree-1", version=Version("1.2.3"))
    assert not gate._receipt_matches(
        document, tree="different-tree", version=Version("1.2.3")
    )


def test_zero_oid_detection_accepts_only_nonempty_all_zero_values() -> None:
    assert gate._is_zero_oid("0" * 40)
    assert not gate._is_zero_oid("")
    assert not gate._is_zero_oid("0" * 39 + "1")


def test_version_increase_pre_commit_runs_gate_for_the_staged_tree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        gate,
        "_completed_git",
        lambda *arguments: subprocess.CompletedProcess(arguments, 1, "", ""),
    )
    versions = {
        "HEAD:pyproject.toml": Version("1.0"),
        ":pyproject.toml": Version("1.1"),
    }
    monkeypatch.setattr(
        gate,
        "_version_from_spec",
        lambda specification, missing_ok=False: versions[specification],
    )
    monkeypatch.setattr(gate, "_assert_no_unstaged_or_untracked_files", lambda: None)
    monkeypatch.setattr(gate, "_git", lambda *arguments: "candidate-tree")
    calls: list[tuple[str, Version, bool]] = []
    monkeypatch.setattr(
        gate,
        "_run_release_gate",
        lambda tree, version, refresh: calls.append((tree, version, refresh)),
    )

    gate._pre_commit(refresh=False)

    assert calls == [("candidate-tree", Version("1.1"), False)]


def test_version_increase_pre_push_requires_exact_tree_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    versions = {
        "local-oid:pyproject.toml": Version("1.1"),
        "remote-oid:pyproject.toml": Version("1.0"),
    }
    monkeypatch.setattr(
        gate,
        "_version_from_spec",
        lambda specification, missing_ok=False: versions[specification],
    )
    monkeypatch.setattr(gate, "_git", lambda *arguments: "candidate-tree")
    monkeypatch.setattr(gate, "_has_valid_receipt", lambda tree, version: False)
    update = "refs/heads/master local-oid refs/heads/master remote-oid\n"

    with pytest.raises(gate.ReleaseGateError, match="no valid local release receipt"):
        gate._pre_push(update)

    monkeypatch.setattr(gate, "_has_valid_receipt", lambda tree, version: True)
    gate._pre_push(update)


def test_initial_remote_master_push_does_not_count_as_a_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        gate,
        "_version_from_spec",
        lambda *arguments, **keywords: pytest.fail(
            "an initial remote branch must not inspect release versions"
        ),
    )
    update = "refs/heads/master local-oid refs/heads/master " + "0" * 40 + "\n"

    gate._pre_push(update)
