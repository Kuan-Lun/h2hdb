#!/usr/bin/env python3
"""Run the local release gate and bind its result to an exact Git tree."""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import tempfile
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from packaging.version import InvalidVersion, Version

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
RECEIPT_SCHEMA_VERSION = 1
RELEASE_PROFILE = "h2hdb-release-v1"
RELEASE_BRANCH = "refs/heads/master"
REQUIRED_CHECKS = (
    "black",
    "ruff",
    "mypy",
    "coverage-contract",
    "schema-drift",
    "lean",
    "sqlite-mariadb-10.11.11-tests",
    "tlc-small",
    "distribution-boundary",
)
VersionChange = Literal["initial", "same", "increase", "decrease"]


class ReleaseGateError(RuntimeError):
    """A local release invariant was not satisfied."""


@dataclass(frozen=True)
class PushUpdate:
    local_ref: str
    local_oid: str
    remote_ref: str
    remote_oid: str


def _completed_git(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ("git", *arguments),
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def _git(*arguments: str) -> str:
    completed = _completed_git(*arguments)
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise ReleaseGateError(
            f"git {' '.join(arguments)} failed" + (f": {detail}" if detail else "")
        )
    return completed.stdout.strip()


def _run(
    label: str,
    command: tuple[str, ...],
    *,
    environment: dict[str, str] | None = None,
) -> None:
    print(f"\n==> {label}", flush=True)
    print("+", " ".join(command), flush=True)
    subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        env=environment,
        check=True,
    )


def _project_version(document: str, *, source: str) -> Version:
    try:
        value = tomllib.loads(document)["project"]["version"]
        return Version(str(value))
    except (KeyError, TypeError, InvalidVersion, tomllib.TOMLDecodeError) as error:
        raise ReleaseGateError(
            f"Cannot read project.version from {source}: {error}"
        ) from error


def _version_from_spec(
    specification: str, *, missing_ok: bool = False
) -> Version | None:
    completed = _completed_git("show", specification)
    if completed.returncode != 0:
        if missing_ok:
            return None
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise ReleaseGateError(
            f"Cannot read {specification}" + (f": {detail}" if detail else "")
        )
    return _project_version(completed.stdout, source=specification)


def _classify_version_change(
    previous: Version | None, current: Version
) -> VersionChange:
    if previous is None:
        return "initial"
    if current == previous:
        return "same"
    if current > previous:
        return "increase"
    return "decrease"


def _is_zero_oid(value: str) -> bool:
    return bool(value) and set(value) == {"0"}


def _parse_push_updates(document: str) -> tuple[PushUpdate, ...]:
    updates: list[PushUpdate] = []
    for line_number, line in enumerate(document.splitlines(), start=1):
        if not line.strip():
            continue
        fields = line.split()
        if len(fields) != 4:
            raise ReleaseGateError(
                f"Malformed pre-push input on line {line_number}: {line!r}"
            )
        updates.append(PushUpdate(*fields))
    return tuple(updates)


def _assert_no_unstaged_or_untracked_files() -> None:
    unstaged = _completed_git("diff", "--quiet", "--exit-code", "--")
    if unstaged.returncode not in (0, 1):
        raise ReleaseGateError(
            unstaged.stderr.strip() or "Cannot inspect unstaged files"
        )
    if unstaged.returncode == 1:
        raise ReleaseGateError(
            "A version-bump commit must not contain unstaged tracked changes. "
            "Stage or stash them before retrying the commit."
        )
    untracked = _git("ls-files", "--others", "--exclude-standard", "-z")
    if untracked:
        paths = [value for value in untracked.split("\0") if value]
        raise ReleaseGateError(
            "A version-bump commit must not contain untracked files: "
            + ", ".join(paths)
        )


def _assert_clean_head() -> None:
    staged = _completed_git("diff", "--cached", "--quiet", "--exit-code", "--")
    if staged.returncode not in (0, 1):
        raise ReleaseGateError(staged.stderr.strip() or "Cannot inspect staged files")
    if staged.returncode == 1:
        raise ReleaseGateError("The explicit release gate requires a clean index")
    _assert_no_unstaged_or_untracked_files()


def _receipt_directory() -> Path:
    common_directory = Path(
        _git("rev-parse", "--path-format=absolute", "--git-common-dir")
    )
    return common_directory / "h2hdb-release" / "receipts"


def _receipt_path(tree: str) -> Path:
    return _receipt_directory() / f"{tree}.json"


def _receipt_matches(document: object, *, tree: str, version: Version) -> bool:
    if not isinstance(document, dict):
        return False
    return (
        document.get("schema_version") == RECEIPT_SCHEMA_VERSION
        and document.get("profile") == RELEASE_PROFILE
        and document.get("tree") == tree
        and document.get("project_version") == str(version)
        and document.get("checks") == list(REQUIRED_CHECKS)
        and document.get("result") == "passed"
    )


def _has_valid_receipt(tree: str, version: Version) -> bool:
    path = _receipt_path(tree)
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError, json.JSONDecodeError, OSError:
        return False
    return _receipt_matches(document, tree=tree, version=version)


def _write_receipt(tree: str, version: Version) -> Path:
    directory = _receipt_directory()
    directory.mkdir(parents=True, exist_ok=True)
    destination = _receipt_path(tree)
    document = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "profile": RELEASE_PROFILE,
        "tree": tree,
        "project_version": str(version),
        "checks": list(REQUIRED_CHECKS),
        "result": "passed",
        "verified_at": datetime.now(UTC).isoformat(),
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
        },
    }
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=directory,
            prefix=f".{tree}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary_path = Path(stream.name)
            json.dump(document, stream, indent=2, sort_keys=True)
            stream.write("\n")
        os.replace(temporary_path, destination)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return destination


def _run_release_gate(tree: str, version: Version, *, refresh: bool) -> None:
    if not refresh and _has_valid_receipt(tree, version):
        print(
            f"Local release gate already passed for tree {tree} "
            f"(version {version}); reusing receipt."
        )
        return

    python = sys.executable
    _run("Black", (python, "-m", "black", "--check", "src", "tests", "scripts"))
    _run("Ruff", (python, "-m", "ruff", "check", "src", "tests", "scripts"))
    _run("mypy", (python, "-m", "mypy", "src", "tests", "scripts"))
    _run(
        "Coverage contract",
        (python, "scripts/verify-formal.py", "coverage", "--validate-only"),
    )
    _run(
        "Schema and generated-artifact drift",
        (python, "scripts/verify-formal.py", "schema"),
    )
    _run("Lean proofs", (python, "scripts/verify-formal.py", "lean"))

    test_environment = os.environ.copy()
    test_environment["H2HDB_TEST_MARIADB"] = "1"
    _run(
        "SQLite and MariaDB 10.11.11 tests",
        (python, "-m", "pytest"),
        environment=test_environment,
    )

    _run("Fetch checksum-pinned TLC", (python, "scripts/fetch-formal-tools.py"))
    _run(
        "Small TLC profiles",
        (
            python,
            "scripts/verify-formal.py",
            "tla",
            "--tla-jar",
            ".formal-tools/tla2tools-1.7.4.jar",
        ),
    )

    with tempfile.TemporaryDirectory(prefix="h2hdb-release-distributions-") as scratch:
        _run(
            "Distribution boundary",
            (
                python,
                "scripts/build-and-verify-distributions.py",
                "--output-directory",
                str(Path(scratch) / "dist"),
            ),
        )

    current_tree = _git("write-tree")
    if current_tree != tree:
        raise ReleaseGateError(
            f"The candidate tree changed during verification: {tree} -> {current_tree}"
        )
    _assert_no_unstaged_or_untracked_files()
    current_version = _version_from_spec(":pyproject.toml")
    if current_version != version:
        raise ReleaseGateError(
            f"project.version changed during verification: {version} -> {current_version}"
        )
    receipt = _write_receipt(tree, version)
    print(f"\nLocal release gate passed; wrote {receipt}")


def _pre_commit(*, refresh: bool) -> None:
    staged_pyproject = _completed_git(
        "diff", "--cached", "--quiet", "--exit-code", "--", "pyproject.toml"
    )
    if staged_pyproject.returncode == 0:
        return
    if staged_pyproject.returncode != 1:
        raise ReleaseGateError(
            staged_pyproject.stderr.strip() or "Cannot inspect staged pyproject.toml"
        )

    previous = _version_from_spec("HEAD:pyproject.toml", missing_ok=True)
    current = _version_from_spec(":pyproject.toml")
    assert current is not None
    change = _classify_version_change(previous, current)
    if change == "same":
        print(
            "pyproject.toml changed without a project.version increase; release gate skipped."
        )
        return
    if change == "decrease":
        raise ReleaseGateError(
            f"project.version decreased from {previous} to {current}"
        )

    _assert_no_unstaged_or_untracked_files()
    tree = _git("write-tree")
    print(
        f"project.version {change}: {previous or '<none>'} -> {current}; "
        "running the local release gate."
    )
    _run_release_gate(tree, current, refresh=refresh)


def _pre_push(document: str) -> None:
    for update in _parse_push_updates(document):
        if (
            update.remote_ref != RELEASE_BRANCH
            or _is_zero_oid(update.local_oid)
            or _is_zero_oid(update.remote_oid)
        ):
            continue
        current = _version_from_spec(f"{update.local_oid}:pyproject.toml")
        assert current is not None
        previous = _version_from_spec(f"{update.remote_oid}:pyproject.toml")
        assert previous is not None
        change = _classify_version_change(previous, current)
        if change == "same":
            continue
        if change == "decrease":
            raise ReleaseGateError(
                f"project.version decreased from {previous} to {current}"
            )
        tree = _git("rev-parse", f"{update.local_oid}^{{tree}}")
        if not _has_valid_receipt(tree, current):
            raise ReleaseGateError(
                f"Push would publish project.version {current}, but tree {tree} has no "
                "valid local release receipt. Commit the version increase with the "
                "installed pre-commit hook, or run "
                "`uv run --no-sync python scripts/release-gate.py run`."
            )
        print(f"Validated local release receipt for version {current} ({tree}).")


def _explicit_run(*, refresh: bool) -> None:
    _assert_clean_head()
    tree = _git("rev-parse", "HEAD^{tree}")
    version = _version_from_spec("HEAD:pyproject.toml")
    assert version is not None
    _run_release_gate(tree, version, refresh=refresh)


def _receipt_status(revision: str) -> None:
    tree = _git("rev-parse", f"{revision}^{{tree}}")
    version = _version_from_spec(f"{revision}:pyproject.toml")
    assert version is not None
    if not _has_valid_receipt(tree, version):
        raise ReleaseGateError(
            f"No valid {RELEASE_PROFILE} receipt for {revision} ({tree}, version {version})"
        )
    print(f"Valid local release receipt for {revision} ({tree}, version {version}).")


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    pre_commit = subparsers.add_parser(
        "pre-commit", help="run the gate when the staged project.version increases"
    )
    pre_commit.add_argument("--refresh", action="store_true")

    pre_push = subparsers.add_parser(
        "pre-push", help="validate receipts for version-increasing master pushes"
    )
    pre_push.add_argument("remote_name", nargs="?", default="")
    pre_push.add_argument("remote_url", nargs="?", default="")

    run = subparsers.add_parser("run", help="run the gate for a clean HEAD")
    run.add_argument("--refresh", action="store_true")

    status = subparsers.add_parser("status", help="validate a receipt")
    status.add_argument("revision", nargs="?", default="HEAD")
    return parser.parse_args()


def main() -> None:
    os.chdir(REPOSITORY_ROOT)
    arguments = _arguments()
    try:
        if arguments.command == "pre-commit":
            _pre_commit(refresh=bool(arguments.refresh))
        elif arguments.command == "pre-push":
            _pre_push(sys.stdin.read())
        elif arguments.command == "run":
            _explicit_run(refresh=bool(arguments.refresh))
        else:
            _receipt_status(str(arguments.revision))
    except (ReleaseGateError, subprocess.CalledProcessError) as error:
        print(f"h2hdb release gate: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
