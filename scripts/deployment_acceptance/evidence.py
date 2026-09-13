"""Read container-written evidence without following host filesystem links.

The deployment prepares canonical temporary paths. Hold directory descriptors
while opening each component, and reject every symlink and special file. These
POSIX primitives are deliberately required rather than emulated by a racy
``resolve`` followed by an ordinary pathname open.
"""

from __future__ import annotations

import json
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

_CHUNK_BYTES = 1024 * 1024
_MAX_READ_BYTES = 256 * 1024 * 1024


class EvidenceError(ValueError):
    """Evidence could not be read or exported within its declared directory."""


def require_evidence_support() -> None:
    """Fail before deployment on hosts without safe POSIX evidence primitives."""
    if (
        not hasattr(os, "O_NOFOLLOW")
        or not hasattr(os, "O_DIRECTORY")
        or os.open not in os.supports_dir_fd
        or os.stat not in os.supports_dir_fd
        or os.listdir not in os.supports_fd
    ):
        raise EvidenceError(
            "Safe evidence access requires POSIX descriptor-relative I/O"
        )


def _name(value: str) -> str:
    if not value or value in {".", ".."} or Path(value).name != value or "\0" in value:
        raise EvidenceError("Evidence names must be single path components")
    return value


@contextmanager
def _directory(path: Path) -> Iterator[int]:
    require_evidence_support()
    absolute = path.absolute()
    if ".." in absolute.parts:
        raise EvidenceError("Evidence directory cannot contain parent traversal")
    descriptor = os.open(absolute.anchor, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for component in absolute.parts[1:]:
            child = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = child
        yield descriptor
    except OSError as error:
        raise EvidenceError(
            f"Cannot safely access evidence directory: {path}"
        ) from error
    finally:
        os.close(descriptor)


def _identity(value: os.stat_result) -> tuple[int, int, int]:
    return value.st_dev, value.st_ino, value.st_mode


@contextmanager
def _entry(directory: int, name: str, *, is_directory: bool) -> Iterator[int]:
    expected = os.stat(_name(name), dir_fd=directory, follow_symlinks=False)
    check = stat.S_ISDIR if is_directory else stat.S_ISREG
    if not check(expected.st_mode):
        raise EvidenceError(
            f"Evidence entry is not a {'directory' if is_directory else 'regular file'}: {name}"
        )
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    if is_directory:
        flags |= os.O_DIRECTORY
    descriptor = os.open(name, flags, dir_fd=directory)
    try:
        actual = os.fstat(descriptor)
        if not check(actual.st_mode) or _identity(expected) != _identity(actual):
            raise EvidenceError(f"Evidence entry changed while opening: {name}")
        yield descriptor
    finally:
        os.close(descriptor)


def _chunks(descriptor: int, size: int) -> Iterator[bytes]:
    remaining = size
    while remaining:
        value = os.read(descriptor, min(remaining, _CHUNK_BYTES))
        if not value:
            raise EvidenceError("Evidence was truncated during its read")
        remaining -= len(value)
        yield value


def read_evidence_bytes(directory: Path, name: str) -> bytes:
    """Read a regular file's initial extent; a growing JSONL tail is bounded.

    Individual parsed evidence is capped at 256 MiB. Recursive export streams
    files and has no such in-memory allocation. Callers must retain incomplete
    JSONL tails as incomplete measurement evidence.
    """
    try:
        with (
            _directory(directory) as parent,
            _entry(parent, name, is_directory=False) as source,
        ):
            size = os.fstat(source).st_size
            if size > _MAX_READ_BYTES:
                raise EvidenceError("Evidence file exceeds the 256 MiB parsing limit")
            return b"".join(_chunks(source, size))
    except OSError as error:
        raise EvidenceError(f"Cannot safely read evidence file: {name}") from error


def read_evidence_json(directory: Path, name: str) -> dict[str, Any]:
    """Read one JSON object without losing the safe-open boundary."""
    value = json.loads(read_evidence_bytes(directory, name))
    if not isinstance(value, dict):
        raise EvidenceError("Evidence JSON must contain an object")
    return value


def _copy_tree(source: int, target: int, prefix: Path) -> list[str]:
    copied: list[str] = []
    for name in sorted(os.listdir(source)):
        entry = os.stat(name, dir_fd=source, follow_symlinks=False)
        relative = prefix / name
        if stat.S_ISDIR(entry.st_mode):
            with _entry(source, name, is_directory=True) as child:
                os.mkdir(name, mode=0o700, dir_fd=target)
                with _entry(target, name, is_directory=True) as output:
                    copied.extend(_copy_tree(child, output, relative))
            continue
        with _entry(source, name, is_directory=False) as content:
            initial = os.fstat(content)
            output = os.open(
                name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=target,
            )
            with os.fdopen(output, "wb") as destination:
                for chunk in _chunks(content, initial.st_size):
                    destination.write(chunk)
            final = os.fstat(content)
            if (initial.st_size, initial.st_mtime_ns, initial.st_ctime_ns) != (
                final.st_size,
                final.st_mtime_ns,
                final.st_ctime_ns,
            ):
                raise EvidenceError(f"Evidence changed during export: {relative}")
        copied.append(relative.as_posix())
    return copied


def export_evidence(directory: Path, destination: Path) -> list[str]:
    """Stream a regular-file tree into a fresh host-owned directory.

    Call only after the owned containers have stopped. A rejected entry can
    leave a partial export, which the caller must record as failed; it never
    turns that export into a successful receipt or reads the rejected target.
    """
    if destination.absolute().is_relative_to(directory.absolute()):
        raise EvidenceError("Evidence export destination must be outside the source")
    try:
        with _directory(directory) as source, _directory(destination.parent) as parent:
            os.mkdir(_name(destination.name), mode=0o700, dir_fd=parent)
            with _entry(parent, destination.name, is_directory=True) as output:
                return _copy_tree(source, output, Path())
    except OSError as error:
        raise EvidenceError("Cannot safely export evidence") from error
