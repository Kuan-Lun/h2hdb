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
    assert gate.RELEASE_PROFILE == "h2hdb-release-v4"
    assert "exact-candidate-code-review" in gate.REQUIRED_CHECKS
    old_profile = dict(document, profile="h2hdb-release-v3")
    assert not gate._receipt_matches(
        old_profile, tree="tree-1", version=Version("1.2.3")
    )


def test_zero_oid_detection_accepts_only_nonempty_all_zero_values() -> None:
    assert gate._is_zero_oid("0" * 40)
    assert not gate._is_zero_oid("")
    assert not gate._is_zero_oid("0" * 39 + "1")


def test_release_branch_prefers_the_explicit_hook_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("H2HDB_RELEASE_BRANCH", "refs/heads/release")
    monkeypatch.setattr(
        gate,
        "_detect_primary_branch",
        lambda: pytest.fail("an explicit release ref must skip primary detection"),
    )

    assert gate._release_branch() == "refs/heads/release"


def test_release_branch_detects_a_renamed_primary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("H2HDB_RELEASE_BRANCH", raising=False)
    monkeypatch.setattr(gate, "_detect_primary_branch", lambda: "main")

    assert gate._release_branch() == "refs/heads/main"


def test_version_increase_pre_commit_defers_the_complete_gate_until_push(
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
    clean_checks: list[None] = []
    monkeypatch.setattr(
        gate,
        "_assert_no_unstaged_or_untracked_files",
        lambda: clean_checks.append(None),
    )
    monkeypatch.setattr(
        gate,
        "_run_release_gate",
        lambda *arguments, **keywords: pytest.fail(
            "pre-commit must not run the complete release gate"
        ),
    )

    gate._pre_commit()

    assert clean_checks == [None]


def test_version_increase_pre_push_runs_gate_when_exact_tree_lacks_receipt(
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
    git_values = {
        ("rev-parse", "local-oid^{tree}"): "candidate-tree",
        ("rev-parse", "HEAD"): "local-oid",
    }
    monkeypatch.setattr(gate, "_git", lambda *arguments: git_values[arguments])
    monkeypatch.setattr(gate, "_has_valid_receipt", lambda tree, version: False)
    clean_checks: list[None] = []
    monkeypatch.setattr(gate, "_assert_clean_head", lambda: clean_checks.append(None))
    review_calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        gate,
        "_verify_code_review",
        lambda arguments, expected_tree: review_calls.append(
            (*arguments, "--expected-tree", expected_tree)
        ),
    )
    calls: list[tuple[str, Version, bool, tuple[str, ...], tuple[str, ...]]] = []
    monkeypatch.setattr(
        gate,
        "_run_release_gate",
        lambda tree, version, refresh, version_arguments, review_arguments: (
            calls.append((tree, version, refresh, version_arguments, review_arguments))
        ),
    )
    update = "refs/heads/master local-oid refs/heads/master remote-oid\n"

    gate._pre_push(update)

    assert clean_checks == [None]
    assert review_calls == [
        ("--revision", "local-oid", "--expected-tree", "candidate-tree")
    ]
    assert calls == [
        (
            "candidate-tree",
            Version("1.1"),
            False,
            ("--base", "remote-oid", "--candidate", "local-oid"),
            ("--revision", "local-oid"),
        )
    ]


def test_version_increase_pre_push_reuses_exact_tree_receipt(
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
    monkeypatch.setattr(gate, "_has_valid_receipt", lambda tree, version: True)
    review_calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        gate,
        "_verify_code_review",
        lambda arguments, expected_tree: review_calls.append(
            (*arguments, "--expected-tree", expected_tree)
        ),
    )
    monkeypatch.setattr(
        gate,
        "_assert_clean_head",
        lambda: pytest.fail("a valid receipt must not require a clean worktree"),
    )
    monkeypatch.setattr(
        gate,
        "_assert_candidate_unchanged",
        lambda tree, version: pytest.fail(
            "historical push receipts must not require the checked-out candidate"
        ),
    )
    monkeypatch.setattr(
        gate,
        "_run_release_gate",
        lambda *arguments, **keywords: pytest.fail(
            "a valid receipt must not rerun the release gate"
        ),
    )

    gate._pre_push("refs/heads/master local-oid refs/heads/master remote-oid\n")
    assert review_calls == [
        ("--revision", "local-oid", "--expected-tree", "candidate-tree")
    ]


def test_version_increase_pre_push_rejects_unchecked_out_tree_without_receipt(
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
    git_values = {
        ("rev-parse", "local-oid^{tree}"): "candidate-tree",
        ("rev-parse", "HEAD"): "other-oid",
    }
    monkeypatch.setattr(gate, "_git", lambda *arguments: git_values[arguments])
    monkeypatch.setattr(gate, "_has_valid_receipt", lambda tree, version: False)
    monkeypatch.setattr(
        gate, "_verify_code_review", lambda arguments, expected_tree: None
    )
    update = "refs/heads/master local-oid refs/heads/master remote-oid\n"

    with pytest.raises(gate.ReleaseGateError, match="not the checked-out HEAD"):
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


def test_explicit_index_run_verifies_the_exact_staged_tree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clean_checks: list[None] = []
    monkeypatch.setattr(
        gate,
        "_assert_no_unstaged_or_untracked_files",
        lambda: clean_checks.append(None),
    )
    monkeypatch.setattr(
        gate,
        "_assert_clean_head",
        lambda: pytest.fail("an index-tree run may have staged changes"),
    )
    git_calls: list[tuple[str, ...]] = []

    def git(*arguments: str) -> str:
        git_calls.append(arguments)
        return "candidate-tree"

    monkeypatch.setattr(gate, "_git", git)
    version_specs: list[str] = []

    def version_from_spec(specification: str, *, missing_ok: bool = False) -> Version:
        version_specs.append(specification)
        return Version("1.2.3")

    monkeypatch.setattr(gate, "_version_from_spec", version_from_spec)
    calls: list[tuple[str, Version, bool, tuple[str, ...], tuple[str, ...]]] = []
    monkeypatch.setattr(
        gate,
        "_run_release_gate",
        lambda tree, version, refresh, version_arguments, review_arguments: (
            calls.append((tree, version, refresh, version_arguments, review_arguments))
        ),
    )

    gate._explicit_run(refresh=False, index=True, base=None)

    assert clean_checks == [None]
    assert git_calls == [("write-tree",)]
    assert version_specs == [":pyproject.toml"]
    assert calls == [
        ("candidate-tree", Version("1.2.3"), False, ("--index",), ("--index",))
    ]


def test_explicit_head_run_requires_a_clean_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clean_checks: list[None] = []
    monkeypatch.setattr(
        gate,
        "_assert_clean_head",
        lambda: clean_checks.append(None),
    )
    git_values = {("rev-parse", "HEAD^{tree}"): "head-tree"}
    monkeypatch.setattr(gate, "_git", lambda *arguments: git_values[arguments])
    monkeypatch.setattr(
        gate,
        "_version_from_spec",
        lambda specification, missing_ok=False: Version("1.2.3"),
    )
    calls: list[tuple[str, Version, bool, tuple[str, ...], tuple[str, ...]]] = []
    monkeypatch.setattr(
        gate,
        "_run_release_gate",
        lambda tree, version, refresh, version_arguments, review_arguments: (
            calls.append((tree, version, refresh, version_arguments, review_arguments))
        ),
    )

    gate._explicit_run(refresh=True, index=False, base="base-oid")

    assert clean_checks == [None]
    assert calls == [
        (
            "head-tree",
            Version("1.2.3"),
            True,
            ("--base", "base-oid"),
            ("--revision", "HEAD"),
        )
    ]


@pytest.mark.parametrize("arguments", [("--index",), ("--revision", "merge-oid")])
def test_code_review_verifier_uses_only_the_offline_command(
    monkeypatch: pytest.MonkeyPatch, arguments: tuple[str, ...]
) -> None:
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(gate, "_run", lambda label, command: calls.append(command))

    gate._verify_code_review(arguments, expected_tree="candidate-tree")

    assert calls == [
        (
            sys.executable,
            "scripts/review-code.py",
            "verify",
            *arguments,
            "--expected-tree",
            "candidate-tree",
        )
    ]


def test_cached_release_receipt_still_requires_a_current_code_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    monkeypatch.setattr(
        gate,
        "_verify_code_review",
        lambda arguments, expected_tree: events.append("review"),
    )
    monkeypatch.setattr(
        gate,
        "_assert_candidate_unchanged",
        lambda tree, version: events.append("candidate-recheck"),
    )

    def has_valid_receipt(tree: str, version: Version) -> bool:
        events.append("release-receipt")
        return True

    monkeypatch.setattr(gate, "_has_valid_receipt", has_valid_receipt)
    monkeypatch.setattr(
        gate,
        "_run",
        lambda *arguments, **keywords: pytest.fail(
            "valid release and review receipts must skip expensive checks"
        ),
    )

    gate._run_release_gate(
        "candidate-tree",
        Version("1.2.3"),
        refresh=False,
        version_arguments=("--index",),
        review_arguments=("--index",),
    )

    assert events == ["review", "release-receipt", "candidate-recheck"]


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ("tree", "candidate tree changed"),
        ("version", "project.version changed"),
        ("dirty", "Unstaged changes after review"),
    ],
)
def test_cached_release_receipt_rejects_candidate_changes_after_review(
    monkeypatch: pytest.MonkeyPatch, change: str, message: str
) -> None:
    monkeypatch.setattr(
        gate, "_verify_code_review", lambda arguments, expected_tree: None
    )
    monkeypatch.setattr(gate, "_has_valid_receipt", lambda tree, version: True)
    monkeypatch.setattr(
        gate,
        "_git",
        lambda *arguments: "changed-tree" if change == "tree" else "candidate-tree",
    )
    monkeypatch.setattr(
        gate,
        "_version_from_spec",
        lambda specification: Version("1.2.4" if change == "version" else "1.2.3"),
    )

    def assert_clean() -> None:
        if change == "dirty":
            raise gate.ReleaseGateError("Unstaged changes after review")

    monkeypatch.setattr(gate, "_assert_no_unstaged_or_untracked_files", assert_clean)

    with pytest.raises(gate.ReleaseGateError, match=message):
        gate._run_release_gate(
            "candidate-tree",
            Version("1.2.3"),
            refresh=False,
            version_arguments=("--index",),
            review_arguments=("--index",),
        )


@pytest.mark.parametrize(
    "message", ["missing review receipt", "review found a blocker"]
)
@pytest.mark.parametrize("entrypoint", ["run", "status", "pre-push"])
def test_review_failure_rejects_even_a_cached_release_receipt(
    monkeypatch: pytest.MonkeyPatch, message: str, entrypoint: str
) -> None:
    monkeypatch.setattr(gate, "_release_branch", lambda: "refs/heads/master")
    monkeypatch.setattr(gate, "_git", lambda *arguments: "candidate-tree")
    monkeypatch.setattr(gate, "_has_valid_receipt", lambda tree, version: True)
    monkeypatch.setattr(
        gate,
        "_version_from_spec",
        lambda specification, missing_ok=False: Version(
            "1.2.2" if specification.startswith("remote-oid:") else "1.2.3"
        ),
    )

    def reject_review(arguments: tuple[str, ...], *, expected_tree: str) -> None:
        raise gate.ReleaseGateError(message)

    monkeypatch.setattr(gate, "_verify_code_review", reject_review)
    with pytest.raises(gate.ReleaseGateError, match=message):
        match entrypoint:
            case "run":
                gate._run_release_gate(
                    "candidate-tree",
                    Version("1.2.3"),
                    refresh=False,
                    version_arguments=("--index",),
                    review_arguments=("--index",),
                )
            case "status":
                gate._receipt_status("merge-oid")
            case _:
                gate._pre_push(
                    "refs/heads/master local-oid refs/heads/master remote-oid\n"
                )


@pytest.mark.parametrize("reject_second_review", [False, True])
def test_release_gate_rechecks_review_before_writing_the_release_receipt(
    monkeypatch: pytest.MonkeyPatch, reject_second_review: bool
) -> None:
    commands: list[tuple[str, ...]] = []
    review_command = (
        sys.executable,
        "scripts/review-code.py",
        "verify",
        "--index",
        "--expected-tree",
        "candidate-tree",
    )

    def run(label: str, command: tuple[str, ...]) -> None:
        commands.append(command)
        if (
            reject_second_review
            and command == review_command
            and commands.count(command) == 2
        ):
            raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(gate, "_run", run)
    monkeypatch.setattr(gate, "_has_valid_receipt", lambda tree, version: False)
    monkeypatch.setattr(gate, "_git", lambda *arguments: "candidate-tree")
    monkeypatch.setattr(gate, "_assert_no_unstaged_or_untracked_files", lambda: None)
    monkeypatch.setattr(
        gate, "_version_from_spec", lambda specification: Version("1.2.3")
    )
    receipts: list[tuple[str, Version]] = []

    def write_receipt(tree: str, version: Version) -> Path:
        receipts.append((tree, version))
        return Path("receipt.json")

    monkeypatch.setattr(gate, "_write_receipt", write_receipt)

    def run_gate() -> None:
        gate._run_release_gate(
            "candidate-tree",
            Version("1.2.3"),
            refresh=False,
            version_arguments=("--index",),
            review_arguments=("--index",),
        )

    if reject_second_review:
        with pytest.raises(subprocess.CalledProcessError):
            run_gate()
        assert receipts == []
    else:
        run_gate()
        assert receipts == [("candidate-tree", Version("1.2.3"))]
    assert commands == [
        review_command,
        (sys.executable, "scripts/check-version.py", "--index"),
        ("scripts/check-full.sh",),
        review_command,
    ]


def test_receipt_status_verifies_the_requested_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        gate,
        "_verify_code_review",
        lambda arguments, expected_tree: review_calls.append(
            (*arguments, "--expected-tree", expected_tree)
        ),
    )
    monkeypatch.setattr(gate, "_git", lambda *arguments: "candidate-tree")
    monkeypatch.setattr(
        gate, "_version_from_spec", lambda specification: Version("1.2.3")
    )
    monkeypatch.setattr(gate, "_has_valid_receipt", lambda tree, version: True)

    gate._receipt_status("merge-oid")

    assert review_calls == [
        ("--revision", "merge-oid", "--expected-tree", "candidate-tree")
    ]


def test_same_version_push_keeps_existing_release_eligibility(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gate, "_release_branch", lambda: "refs/heads/master")
    monkeypatch.setattr(
        gate, "_version_from_spec", lambda specification: Version("1.2.3")
    )
    monkeypatch.setattr(
        gate,
        "_verify_code_review",
        lambda arguments, expected_tree: pytest.fail(
            "same-version pushes are not release gates"
        ),
    )

    gate._pre_push("refs/heads/master local-oid refs/heads/master remote-oid\n")
