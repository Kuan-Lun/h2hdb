#!/usr/bin/env python3
"""Check documentation from an exact Git snapshot without program checks."""

from __future__ import annotations

import argparse
import os
import posixpath
import subprocess
import sys
import tomllib
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


def _regular_blob(root: Path, tree: str, path: str) -> bytes | None:
    raw = git_output(root, "--literal-pathspecs", "ls-tree", "-z", tree, "--", path)
    if not raw:
        return None
    entries = raw.split(b"\0")
    header, separator, name = entries[0].partition(b"\t")
    fields = header.split()
    if (
        len(entries) != 2
        or entries[-1] != b""
        or not separator
        or name != path.encode("utf-8")
        or len(fields) != 3
        or fields[0] not in (b"100644", b"100755")
        or fields[1] != b"blob"
    ):
        raise ChangeScopeError(
            f"Documentation metadata requires a regular file: {path}"
        )
    return git_output(root, "cat-file", "blob", fields[2].decode("ascii"))


def _check_readme_reference(root: Path, tree: str) -> None:
    content = _regular_blob(root, tree, "pyproject.toml")
    if content is None:
        return
    try:
        document = tomllib.loads(content.decode("utf-8"))
    except (UnicodeError, tomllib.TOMLDecodeError) as error:
        raise ChangeScopeError(
            f"Cannot read candidate documentation metadata: {error}"
        ) from error
    project = document.get("project", {})
    if not isinstance(project, dict):
        raise ChangeScopeError("Candidate project metadata must be a table")
    readme: object = project.get("readme")
    if readme is None:
        return
    if isinstance(readme, dict):
        if set(readme) - {"file", "text", "content-type"}:
            raise ChangeScopeError("Unsupported project.readme table fields")
        if ("file" in readme) == ("text" in readme):
            raise ChangeScopeError(
                "project.readme requires exactly one of file or text"
            )
        content_type = readme.get("content-type")
        if not isinstance(content_type, str) or not content_type.strip():
            raise ChangeScopeError("project.readme table requires a content-type")
        if "text" in readme:
            if not isinstance(readme["text"], str):
                raise ChangeScopeError("project.readme text must be a string")
            return
        readme = readme["file"]
    if not isinstance(readme, str) or not readme:
        raise ChangeScopeError("project.readme file must be a nonempty relative path")
    path = posixpath.normpath(readme)
    if path in (".", "..") or path.startswith(("/", "../")):
        raise ChangeScopeError(
            "project.readme file must stay inside the candidate tree"
        )
    if _regular_blob(root, tree, path) is None:
        raise ChangeScopeError(f"Missing project.readme file in candidate: {readme}")


def _export_documents(root: Path, tree: str, destination: Path) -> None:
    config_found = False
    directories: dict[tuple[int, int], tuple[str, ...]] = {}
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
        try:
            parent = destination
            for depth, component in enumerate(parts[:-1], start=1):
                parent /= component
                parent.mkdir(exist_ok=True)
                metadata = parent.stat()
                identity = (metadata.st_dev, metadata.st_ino)
                previous = directories.setdefault(identity, parts[:depth])
                if previous != parts[:depth]:
                    raise ChangeScopeError(
                        f"Colliding documentation directories: {path!r}"
                    )
            with output.open("xb") as stream:
                stream.write(
                    git_output(root, "cat-file", "blob", object_id.decode("ascii"))
                )
        except (FileExistsError, IsADirectoryError, NotADirectoryError) as error:
            raise ChangeScopeError(
                f"Colliding documentation paths on this filesystem: {path!r}"
            ) from error
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
    _check_readme_reference(root, candidate)
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
