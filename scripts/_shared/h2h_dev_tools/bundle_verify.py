"""Verify the complete, committed source bundle without external dependencies."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import stat
import sys
from pathlib import Path
from typing import TypedDict, cast

BUNDLE_DIRECTORY = Path("scripts/_shared/h2h_dev_tools")
MANIFEST_NAME = "manifest.json"
BUNDLE_FILES = frozenset(
    {
        "__init__.py",
        "bundle_verify.py",
        "change_scope.py",
        "ci_dependencies.py",
        "dependency_manifest.py",
        "documentation.py",
        "publish_version.py",
    }
)
_SHA256 = re.compile(r"[0-9a-f]{64}")
_VERSION = re.compile(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)")
_CACHE_NAME = re.compile(
    r"(?P<module>[A-Za-z_][A-Za-z_0-9]*)\.cpython-[0-9]+(?:\.opt-[12])?\.pyc"
)


class BundleError(RuntimeError):
    """A generated source bundle cannot be verified or safely replaced."""


class BundleManifest(TypedDict):
    """The exact source payload and its producing toolkit release."""

    schema_version: int
    toolkit_name: str
    toolkit_version: str
    payload_sha256: str
    files: dict[str, str]


def payload_digest(files: dict[str, str]) -> str:
    """Fingerprint the named source bytes, not an unobserved wheel archive."""
    encoded = json.dumps(files, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def bundle_directory(repository: Path) -> Path:
    """Reject symlinks in every repository-relative managed directory."""
    root = repository.absolute()
    try:
        if not stat.S_ISDIR(root.lstat().st_mode):
            raise BundleError(f"Repository must be a real directory: {root}")
        current = root
        for component in BUNDLE_DIRECTORY.parts:
            current /= component
            try:
                mode = current.lstat().st_mode
            except FileNotFoundError:
                continue
            if not stat.S_ISDIR(mode):
                raise BundleError(
                    f"Managed bundle path must be a real directory: {current}"
                )
    except OSError as error:
        raise BundleError(f"Cannot inspect bundle directories: {error}") from error
    return root / BUNDLE_DIRECTORY


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise BundleError(f"Duplicate manifest key: {key}")
        result[key] = value
    return result


def regular_bytes(path: Path) -> bytes:
    """Read a generated regular file without accepting a symlink entry."""
    try:
        if not stat.S_ISREG(path.lstat().st_mode):
            raise BundleError(f"Bundle entry must be a regular file: {path}")
        return path.read_bytes()
    except OSError as error:
        raise BundleError(f"Cannot read bundle file {path}: {error}") from error


def _read_manifest(path: Path) -> BundleManifest:
    try:
        document: object = json.loads(
            regular_bytes(path), object_pairs_hook=_reject_duplicate_keys
        )
    except (UnicodeError, json.JSONDecodeError) as error:
        raise BundleError(f"Invalid bundle manifest: {error}") from error
    return validate_manifest(document)


def validate_manifest(document: object) -> BundleManifest:
    """Accept only the complete schema and safe, explicitly owned filenames."""
    if not isinstance(document, dict) or set(document) != {
        "schema_version",
        "toolkit_name",
        "toolkit_version",
        "payload_sha256",
        "files",
    }:
        raise BundleError("Bundle manifest fields do not match schema 1")
    if type(document["schema_version"]) is not int or document["schema_version"] != 1:
        raise BundleError("Unsupported bundle manifest schema")
    if document["toolkit_name"] != "h2h-dev-tools":
        raise BundleError("Bundle manifest belongs to another toolkit")
    version = document["toolkit_version"]
    if not isinstance(version, str) or _VERSION.fullmatch(version) is None:
        raise BundleError("Bundle toolkit version must be X.Y.Z")
    files = document["files"]
    if not isinstance(files, dict) or set(files) != BUNDLE_FILES:
        raise BundleError("Bundle manifest must declare the complete source file set")
    if not all(
        isinstance(value, str) and _SHA256.fullmatch(value) for value in files.values()
    ):
        raise BundleError("Invalid source file SHA-256 in bundle manifest")
    if document["payload_sha256"] != payload_digest(files):
        raise BundleError("Bundle payload fingerprint does not match its file manifest")
    return cast(BundleManifest, document)


def cache_files(directory: Path) -> tuple[Path, ...]:
    """Identify disposable CPython caches without accepting other extra content."""
    cache = directory / "__pycache__"
    try:
        try:
            mode = cache.lstat().st_mode
        except FileNotFoundError:
            return ()
        if not stat.S_ISDIR(mode):
            raise BundleError("Bundle __pycache__ must be a real directory")
        entries = tuple(cache.iterdir())
        for entry in entries:
            match = _CACHE_NAME.fullmatch(entry.name)
            if (
                match is None
                or f"{match['module']}.py" not in BUNDLE_FILES
                or not stat.S_ISREG(entry.lstat().st_mode)
            ):
                raise BundleError(f"Unknown or nonregular bundle cache entry: {entry}")
        return entries
    except OSError as error:
        raise BundleError(f"Cannot inspect bundle bytecode caches: {error}") from error


def verify_bundle(repository: Path) -> BundleManifest:
    """Prove that each declared file matches and that no extra entry exists."""
    directory = bundle_directory(repository)
    manifest = _read_manifest(directory / MANIFEST_NAME)
    try:
        actual = {entry.name for entry in directory.iterdir()}
    except OSError as error:
        raise BundleError(f"Cannot inspect bundle file closure: {error}") from error
    expected = BUNDLE_FILES | {MANIFEST_NAME}
    cache_files(directory)
    actual.discard("__pycache__")
    if actual != expected:
        raise BundleError(
            f"Bundle file closure mismatch; missing={sorted(expected - actual)}, "
            f"extra={sorted(actual - expected)}"
        )
    for name, expected_hash in manifest["files"].items():
        actual_hash = hashlib.sha256(regular_bytes(directory / name)).hexdigest()
        if actual_hash != expected_hash:
            raise BundleError(f"Generated source was modified: {name}")
    return manifest


def main(arguments: list[str] | None = None) -> int:
    """Provide a standalone consumer-side verification entry point."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True, type=Path)
    options = parser.parse_args(arguments)
    try:
        manifest = verify_bundle(options.repository)
    except BundleError as error:
        print(f"h2h-dev-tools: {error}", file=sys.stderr)
        return 1
    print(f"Verified h2h-dev-tools {manifest['toolkit_version']} source bundle.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
