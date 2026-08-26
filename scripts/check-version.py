#!/usr/bin/env python3
"""Validate a task-level X.Y.Z bump against an exact release candidate."""

from __future__ import annotations

import argparse
import copy
import fnmatch
import hashlib
import json
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

_VERSION_PATTERN = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
_LEGACY_VERSION_PATTERN = re.compile(r"^(\d+)\.(\d+)\.(\d+)\.(\d+)(?:\.\d+)*$")
_IGNORED_PATHS = (
    ".claude/**",
    ".github/**",
    ".githooks/**",
    ".gitignore",
    ".markdownlint-cli2.jsonc",
    ".release/**",
    ".vscode/**",
    "AGENTS.md",
    "CLAUDE.md",
    "README.md",
    "docs/**",
    "lean-toolchain",
    "mypy.ini",
    "package.json",
    "scripts/**",
    "tests/**",
    "verification/**",
)
_AUDIT_PATH = ".release/dependency-audit.json"


def _git(*arguments: str, input_text: str | None = None) -> str:
    return subprocess.run(
        ("git", *arguments),
        check=True,
        input=input_text,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout.rstrip("\n")


def _tree_file(tree: str, path: str) -> bytes:
    return subprocess.run(
        ("git", "show", f"{tree}:{path}"),
        check=True,
        stdout=subprocess.PIPE,
    ).stdout


def _load_toml(tree: str) -> dict[str, Any]:
    return tomllib.loads(_tree_file(tree, "pyproject.toml").decode())


def _release_metadata(document: dict[str, Any]) -> dict[str, Any]:
    project = copy.deepcopy(document.get("project", {}))
    project.pop("version", None)
    optional = project.get("optional-dependencies")
    if isinstance(optional, dict):
        optional.pop("dev", None)
    return {
        "build-system": document.get("build-system", {}),
        "project": project,
        "wheel": document.get("tool", {}).get("hatch", {}).get("build", {}),
    }


def _dependency_manifest(document: dict[str, Any], package: dict[str, Any]) -> bytes:
    project = document.get("project", {})
    manifest = {
        "build": document.get("build-system", {}).get("requires", []),
        "dependency-groups": document.get("dependency-groups", {}),
        "optional": project.get("optional-dependencies", {}),
        "runtime": project.get("dependencies", []),
        "node-dev": package.get("devDependencies", {}),
    }
    return json.dumps(
        manifest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _matches(path: str, patterns: tuple[str, ...] | list[str]) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def _parse_version(value: str) -> tuple[int, int, int]:
    match = _VERSION_PATTERN.fullmatch(value)
    if match is None:
        raise ValueError(f"candidate version must use X.Y.Z: {value}")
    major, minor, patch = match.groups()
    return int(major), int(minor), int(patch)


def _base_version(value: str) -> tuple[int, int, int]:
    match = _VERSION_PATTERN.fullmatch(value)
    if match is not None:
        major, minor, patch = match.groups()
        return int(major), int(minor), int(patch)
    legacy = _LEGACY_VERSION_PATTERN.fullmatch(value)
    if legacy is None:
        raise ValueError(f"unsupported base version: {value}")
    major, minor, patch, _legacy_counter = legacy.groups()
    return int(major), int(minor), int(patch)


def _expected_version(
    base: tuple[int, int, int],
    *,
    breaking: bool,
    feature: bool,
) -> tuple[int, int, int]:
    major, minor, patch = base
    if major == 0:
        return (0, minor + 1, 0) if breaking else (0, minor, patch + 1)
    if breaking:
        return major + 1, 0, 0
    if feature:
        return major, minor + 1, 0
    return major, minor, patch + 1


def _validate_audit(tree: str, document: dict[str, Any], version: str) -> None:
    try:
        receipt = json.loads(_tree_file(tree, _AUDIT_PATH))
        package = json.loads(_tree_file(tree, "package.json"))
    except (subprocess.CalledProcessError, json.JSONDecodeError) as error:
        raise ValueError(
            "version bump requires a valid .release/dependency-audit.json"
        ) from error
    digest = hashlib.sha256(_dependency_manifest(document, package)).hexdigest()
    if receipt.get("schema") != "h2h.dependency-audit.v1":
        raise ValueError("dependency audit receipt has an unsupported schema")
    if receipt.get("project_version") != version:
        raise ValueError("dependency audit receipt does not match project version")
    if receipt.get("manifest_sha256") != digest:
        raise ValueError("dependency audit receipt does not match dependencies")
    review = receipt.get("review", {})
    if review.get("status") != "reviewed" or not review.get("note"):
        raise ValueError("dependency audit receipt lacks a compatibility review")


def _candidate(arguments: argparse.Namespace) -> tuple[str, str, str]:
    if arguments.index:
        merge_task_ref = os.environ.get("WORKFLOW_MERGE_TASK_REF")
        if merge_task_ref:
            merge_revision = _git(
                "rev-parse", "--verify", f"{merge_task_ref}^{{commit}}"
            )
        else:
            merge_head = Path(_git("rev-parse", "--git-path", "MERGE_HEAD"))
            if not merge_head.exists():
                raise ValueError(
                    "--index requires WORKFLOW_MERGE_TASK_REF or an active merge"
                )
            merge_revision = _git("rev-parse", "MERGE_HEAD")
        return "HEAD", _git("write-tree"), f"HEAD..{merge_revision}"
    if arguments.base:
        return (
            str(arguments.base),
            str(arguments.candidate),
            f"{arguments.base}..{arguments.candidate}",
        )
    primary_name = subprocess.run(
        ("scripts/detect-primary-branch.sh",),
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout.strip()
    return primary_name, "HEAD", f"{primary_name}..HEAD"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", action="store_true")
    parser.add_argument("--base")
    parser.add_argument("--candidate", default="HEAD")
    arguments = parser.parse_args()
    if arguments.index and arguments.base:
        parser.error("--index and --base are mutually exclusive")
    if arguments.candidate != "HEAD" and not arguments.base:
        parser.error("--candidate requires --base")

    base_tree, candidate_tree, message_range = _candidate(arguments)
    base_document = _load_toml(base_tree)
    candidate_document = _load_toml(candidate_tree)
    base_version_text = str(base_document["project"]["version"])
    candidate_version_text = str(candidate_document["project"]["version"])

    changed_paths = tuple(
        path
        for path in _git(
            "diff", "--name-only", "--diff-filter=ACDMRT", base_tree, candidate_tree
        ).splitlines()
        if path
    )
    release_patterns = (
        candidate_document.get("tool", {})
        .get("h2h", {})
        .get("version", {})
        .get("release-paths", [])
    )
    release_changed = _release_metadata(base_document) != _release_metadata(
        candidate_document
    ) or any(_matches(path, release_patterns) for path in changed_paths)
    unknown = [
        path
        for path in changed_paths
        if path != "pyproject.toml"
        and not _matches(path, release_patterns)
        and not _matches(path, _IGNORED_PATHS)
    ]

    messages = _git("log", "--format=%B%x00", message_range)
    none_impact = bool(
        re.search(r"^Version-Impact:\s*none\s*$", messages, re.MULTILINE)
    )
    none_reason = re.search(r"^Version-Reason:\s*(\S.+)$", messages, re.MULTILINE)
    if none_impact != (none_reason is not None):
        raise ValueError(
            "Version-Impact: none and a nonempty Version-Reason must be used together"
        )
    if unknown and not none_impact:
        raise ValueError(
            "unclassified version impact paths: " + ", ".join(sorted(unknown))
        )

    version_changed = base_version_text != candidate_version_text
    if release_changed and not version_changed and not none_impact:
        raise ValueError("release surface changed without a project version bump")
    if version_changed and none_impact:
        raise ValueError("Version-Impact: none cannot accompany a version bump")
    if not release_changed and not unknown and version_changed:
        raise ValueError("project version changed without a release-surface change")
    if not version_changed:
        return 0

    breaking = bool(
        re.search(r"^[a-z]+(?:\([^\n)]+\))?!:", messages, re.MULTILINE)
        or re.search(r"^BREAKING CHANGE:", messages, re.MULTILINE)
    )
    feature = bool(re.search(r"^feat(?:\([^\n)]+\))?:", messages, re.MULTILINE))
    candidate_version = _parse_version(candidate_version_text)
    expected = _expected_version(
        _base_version(base_version_text),
        breaking=breaking,
        feature=feature,
    )
    if candidate_version != expected:
        expected_text = ".".join(str(part) for part in expected)
        raise ValueError(
            f"expected project version {expected_text}, got {candidate_version_text}"
        )
    _validate_audit(candidate_tree, candidate_document, candidate_version_text)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, OSError, subprocess.CalledProcessError, ValueError) as error:
        print(f"check-version: {error}", file=sys.stderr)
        raise SystemExit(1) from error
