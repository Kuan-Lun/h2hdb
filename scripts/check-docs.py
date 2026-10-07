#!/usr/bin/env python3
"""Check documentation from an exact Git snapshot without program checks."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory

from check_change_scope import (
    ChangeScopeError,
    add_target_arguments,
    capture_candidate,
    classify_changes,
    git_output,
    resolve_tree,
)

_CONFIG = ".markdownlint-cli2.jsonc"


def _export_documents(root: Path, tree: str, destination: Path) -> None:
    config_found = False
    for entry in git_output(root, "ls-tree", "-r", "-z", tree).split(b"\0"):
        if not entry:
            continue
        header, separator, raw_path = entry.partition(b"\t")
        if not separator:
            raise ChangeScopeError("Malformed Git tree entry")
        fields = header.split()
        if len(fields) != 3:
            raise ChangeScopeError("Malformed Git tree entry header")
        mode, kind, object_id = fields
        path = os.fsdecode(raw_path)
        if path != _CONFIG and not path.endswith(".md"):
            continue
        if mode not in (b"100644", b"100755") or kind != b"blob":
            if path == _CONFIG:
                raise ChangeScopeError("Markdown configuration must be a regular file")
            continue
        parts = PurePosixPath(path).parts
        if not parts or PurePosixPath(path).is_absolute() or ".." in parts:
            raise ChangeScopeError("Unsafe path in documentation tree")
        output = destination.joinpath(*parts)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(
            git_output(root, "cat-file", "blob", object_id.decode("ascii"))
        )
        config_found |= path == _CONFIG
    if not config_found:
        raise ChangeScopeError(f"Candidate has no regular {_CONFIG}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    add_target_arguments(parser)
    arguments = parser.parse_args()
    root = Path.cwd()
    candidate = capture_candidate(root, arguments.candidate)
    base = resolve_tree(root, arguments.base)
    if classify_changes(base, candidate, root) != "documentation":
        raise ChangeScopeError("Candidate is not a documentation-only change")
    git_output(root, "diff", "--check", "--no-ext-diff", base, candidate, "--")
    tool_name = "markdownlint-cli2.cmd" if os.name == "nt" else "markdownlint-cli2"
    executable = root / "node_modules" / ".bin" / tool_name
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise ChangeScopeError(f"Missing repository-local Markdown tool: {executable}")
    with TemporaryDirectory(prefix="h2h-docs-check-") as directory:
        snapshot = Path(directory)
        _export_documents(root, candidate, snapshot)
        try:
            subprocess.run((str(executable),), cwd=snapshot, check=True)
        except (OSError, subprocess.SubprocessError) as error:
            raise ChangeScopeError(f"Markdown check failed: {error}") from error
    if capture_candidate(root, arguments.candidate) != candidate:
        raise ChangeScopeError("Candidate changed while checking documentation")
    if resolve_tree(root, arguments.base) != base:
        raise ChangeScopeError("Base changed while checking documentation")
    print(f"Documentation checks passed for tree {candidate}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ChangeScopeError, OSError, UnicodeError) as error:
        print(f"check-docs: {error}", file=sys.stderr)
        raise SystemExit(1) from error
