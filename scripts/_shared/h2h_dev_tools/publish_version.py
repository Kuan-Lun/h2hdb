#!/usr/bin/env python3
"""Detect a project-version increase across an entire GitHub push."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

from packaging.version import Version

_REVISION_PATTERN = re.compile(r"[0-9a-fA-F]{40,64}")


def _project_version(document: str) -> Version:
    return Version(str(tomllib.loads(document)["project"]["version"]))


def _document_at(revision: str) -> str:
    shown = subprocess.run(
        ("git", "show", f"{revision}:pyproject.toml"),
        check=False,
        capture_output=True,
        text=True,
    )
    if shown.returncode == 0:
        return shown.stdout

    fetched = subprocess.run(
        ("git", "fetch", "--no-tags", "--depth=1", "origin", revision),
        check=False,
        capture_output=True,
        text=True,
    )
    if fetched.returncode != 0:
        detail = fetched.stderr.strip() or fetched.stdout.strip()
        raise ValueError(f"cannot fetch the pre-push revision {revision}: {detail}")
    return subprocess.run(
        ("git", "show", f"{revision}:pyproject.toml"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def _previous_version(revision: str) -> Version | None:
    if revision and set(revision) == {"0"}:
        return None
    if _REVISION_PATTERN.fullmatch(revision) is None:
        raise ValueError(f"invalid pre-push revision: {revision!r}")
    return _project_version(_document_at(revision))


def _write_outputs(
    output_path: Path,
    *,
    current: Version,
    previous: Version | None,
    bumped: bool,
) -> None:
    with output_path.open("a", encoding="utf-8") as output:
        print(f"current={current}", file=output)
        print(f"previous={previous or ''}", file=output)
        print(f"bumped={'true' if bumped else 'false'}", file=output)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--before-revision", required=True)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()

    current = _project_version(Path("pyproject.toml").read_text(encoding="utf-8"))
    previous = _previous_version(arguments.before_revision)
    if previous is not None and current < previous:
        raise ValueError(f"project.version decreased from {previous} to {current}")

    # Creating or seeding the default branch is not a version transition.
    bumped = previous is not None and current > previous
    environment_output = os.environ.get("GITHUB_OUTPUT")
    if arguments.output is None and environment_output is None:
        raise ValueError("GITHUB_OUTPUT or --output is required")
    output_path = arguments.output or Path(environment_output or "")
    _write_outputs(
        output_path,
        current=current,
        previous=previous,
        bumped=bumped,
    )
    print(
        "publish version transition: "
        f"previous={previous or 'none'}, current={current}, bumped={bumped}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, OSError, subprocess.CalledProcessError, ValueError) as error:
        print(f"check-publish-version: {error}", file=sys.stderr)
        raise SystemExit(1) from error
