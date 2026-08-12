"""Fetch checksum-pinned formal-verification tools declared by the repository."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import tempfile
import tomllib
import urllib.request
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
LOCK_PATH = REPOSITORY_ROOT / "verification" / "tools.lock.toml"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _tlc_lock() -> dict[str, Any]:
    with LOCK_PATH.open("rb") as stream:
        document = tomllib.load(stream)
    value = document.get("tla_plus", {}).get("tlc")
    if not isinstance(value, dict):
        raise RuntimeError(f"Missing [tla_plus.tlc] in {LOCK_PATH}")
    if value.get("version") != "1.7.4":
        raise RuntimeError(f"Unsupported tla_plus.tlc.version in {LOCK_PATH}")
    for field in ("filename", "url", "sha256"):
        if not isinstance(value.get(field), str) or not value[field]:
            raise RuntimeError(f"Invalid tla_plus.tlc.{field} in {LOCK_PATH}")
    if Path(value["filename"]).name != value["filename"]:
        raise RuntimeError(f"tla_plus.tlc.filename in {LOCK_PATH} must be a basename")
    if not value["url"].startswith("https://"):
        raise RuntimeError(f"tla_plus.tlc.url in {LOCK_PATH} must use HTTPS")
    if re.fullmatch(r"[0-9a-fA-F]{64}", value["sha256"]) is None:
        raise RuntimeError(f"tla_plus.tlc.sha256 in {LOCK_PATH} is invalid")
    return value


def fetch_tlc(output_directory: Path) -> Path:
    lock = _tlc_lock()
    expected = str(lock["sha256"]).lower()
    output_directory.mkdir(parents=True, exist_ok=True)
    destination = output_directory / str(lock["filename"])
    if destination.is_file() and _sha256(destination) == expected:
        return destination

    with tempfile.NamedTemporaryFile(
        dir=output_directory, prefix=".tlc-", delete=False
    ) as temporary:
        temporary_path = Path(temporary.name)
        try:
            with urllib.request.urlopen(str(lock["url"]), timeout=60) as response:
                shutil.copyfileobj(response, temporary)
        except BaseException:
            temporary_path.unlink(missing_ok=True)
            raise
    actual = _sha256(temporary_path)
    if actual != expected:
        temporary_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"TLC checksum mismatch: expected={expected} actual={actual}"
        )
    os.replace(temporary_path, destination)
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=REPOSITORY_ROOT / ".formal-tools",
    )
    arguments = parser.parse_args()
    print(fetch_tlc(arguments.output_directory))


if __name__ == "__main__":
    main()
