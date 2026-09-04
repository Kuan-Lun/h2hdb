#!/usr/bin/env python3
"""Prepare or verify a manual disposable-VM SQLite power-cut experiment."""

from __future__ import annotations

import argparse
import json
import os
import stat
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, NoReturn, cast
from uuid import RFC_4122, UUID, uuid4

from h2hdb import (
    CoreConfig,
    DatabaseConfig,
    StorageInstanceBindingMismatchError,
    VNextDatabaseAdminFacade,
)
from h2hdb.sqlite_connector import SQLiteConnector

PROTOCOL = "h2hdb-core-sqlite-storage-binding-guest-powercut-v1"
CONTROL_FILE_NAME = "guest-powercut-case.json"
DATABASE_FILE_NAME = "h2hdb.sqlite3"
READY_PREFIX = "H2HDB_GUEST_POWERCUT_READY"
VERIFIED_PREFIX = "H2HDB_GUEST_POWERCUT_STATE_VERIFIED"
MAX_CONTROL_BYTES = 4096
ALLOWED_STATE_ENTRIES = frozenset(
    {
        CONTROL_FILE_NAME,
        DATABASE_FILE_NAME,
        f"{DATABASE_FILE_NAME}-journal",
        f"{DATABASE_FILE_NAME}-shm",
        f"{DATABASE_FILE_NAME}-wal",
    }
)


class HarnessUsageError(RuntimeError):
    """The manual experiment directory or protocol record is unsafe."""


def _absolute_unsymlinked_parent(path: Path) -> Path:
    if not path.is_absolute():
        raise HarnessUsageError("--state-directory must be an absolute path")
    absolute_parent = path.parent.absolute()
    try:
        resolved_parent = path.parent.resolve(strict=True)
    except OSError as error:
        raise HarnessUsageError("state-directory parent must already exist") from error
    if absolute_parent != resolved_parent:
        raise HarnessUsageError(
            "state-directory parent path must not contain symlink components"
        )
    parent_state = resolved_parent.lstat()
    if not stat.S_ISDIR(parent_state.st_mode):
        raise HarnessUsageError("state-directory parent must be a real directory")
    return resolved_parent / path.name


def _open_directory(path: Path) -> int:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise HarnessUsageError(
            f"state directory is not safely openable: {path}"
        ) from error
    opened = os.fstat(descriptor)
    visible = path.lstat()
    if (
        not stat.S_ISDIR(opened.st_mode)
        or stat.S_ISLNK(visible.st_mode)
        or (opened.st_dev, opened.st_ino) != (visible.st_dev, visible.st_ino)
    ):
        os.close(descriptor)
        raise HarnessUsageError(f"state directory changed identity: {path}")
    return descriptor


def _create_state_directory(path: Path) -> Path:
    target = _absolute_unsymlinked_parent(path)
    if target.exists() or target.is_symlink():
        raise HarnessUsageError(
            "prepare requires a nonexistent dedicated state directory; refusing "
            "to reuse or overwrite it"
        )
    try:
        target.mkdir(mode=0o700)
    except OSError as error:
        raise HarnessUsageError("could not create dedicated state directory") from error
    descriptor = _open_directory(target)
    parent_descriptor = _open_directory(target.parent)
    try:
        os.fsync(descriptor)
        os.fsync(parent_descriptor)
    finally:
        os.close(parent_descriptor)
        os.close(descriptor)
    return target


def _require_state_directory(path: Path) -> Path:
    target = _absolute_unsymlinked_parent(path)
    try:
        visible = target.lstat()
    except OSError as error:
        raise HarnessUsageError(
            "verify requires an existing state directory"
        ) from error
    if not stat.S_ISDIR(visible.st_mode) or stat.S_ISLNK(visible.st_mode):
        raise HarnessUsageError("state directory must be a real directory")
    descriptor = _open_directory(target)
    os.close(descriptor)
    children = tuple(target.iterdir())
    unexpected = sorted(
        child.name for child in children if child.name not in ALLOWED_STATE_ENTRIES
    )
    if unexpected:
        raise HarnessUsageError(
            f"state directory contains unexpected entries: {unexpected!r}"
        )
    unsafe = sorted(
        child.name
        for child in children
        if child.is_symlink() or not stat.S_ISREG(child.lstat().st_mode)
    )
    if unsafe:
        raise HarnessUsageError(
            f"state directory contains unsafe non-regular entries: {unsafe!r}"
        )
    return target


def _write_control(path: Path, storage_instance_uuid: bytes) -> None:
    payload = (
        json.dumps(
            {
                "protocol": PROTOCOL,
                "storage_instance_uuid": storage_instance_uuid.hex(),
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        + b"\n"
    )
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            if stream.write(payload) != len(payload):
                raise OSError("control record accepted a partial write")
            stream.flush()
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory_descriptor = _open_directory(path.parent)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def _read_control(path: Path) -> bytes:
    try:
        visible = path.lstat()
    except OSError as error:
        raise HarnessUsageError("guest power-cut control record is missing") from error
    if not stat.S_ISREG(visible.st_mode) or stat.S_ISLNK(visible.st_mode):
        raise HarnessUsageError("guest power-cut control record must be a real file")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (visible.st_dev, visible.st_ino):
            raise HarnessUsageError("guest power-cut control record changed identity")
        payload = os.read(descriptor, MAX_CONTROL_BYTES + 1)
    finally:
        os.close(descriptor)
    if len(payload) > MAX_CONTROL_BYTES:
        raise HarnessUsageError("guest power-cut control record is oversized")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise HarnessUsageError("guest power-cut control record is invalid") from error
    if not isinstance(document, dict) or set(document) != {
        "protocol",
        "storage_instance_uuid",
    }:
        raise HarnessUsageError("guest power-cut control record has an unknown shape")
    if document["protocol"] != PROTOCOL:
        raise HarnessUsageError("guest power-cut control protocol does not match")
    raw_uuid = document["storage_instance_uuid"]
    if not isinstance(raw_uuid, str) or len(raw_uuid) != 32:
        raise HarnessUsageError("guest power-cut UUID is invalid")
    try:
        parsed = UUID(hex=raw_uuid)
    except ValueError as error:
        raise HarnessUsageError("guest power-cut UUID is invalid") from error
    if parsed.version != 4 or parsed.variant != RFC_4122:
        raise HarnessUsageError("guest power-cut UUID is not UUIDv4")
    return parsed.bytes


def _config(state_directory: Path) -> CoreConfig:
    return CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite",
            database=str(state_directory / DATABASE_FILE_NAME),
        )
    )


def prepare(state_directory: Path) -> NoReturn:
    target = _create_state_directory(state_directory)
    storage_instance_uuid = uuid4().bytes
    _write_control(target / CONTROL_FILE_NAME, storage_instance_uuid)
    config = _config(target)
    admin = VNextDatabaseAdminFacade(config)
    admin.initialize()
    directory_descriptor = _open_directory(target)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)

    original_commit: Callable[[SQLiteConnector], None] = SQLiteConnector.commit
    observed_commit = False

    def commit(connector: SQLiteConnector) -> None:
        nonlocal observed_commit
        if observed_commit:
            raise RuntimeError("guest power-cut harness observed a second commit")
        original_commit(connector)
        observed_commit = True
        print(
            f"{READY_PREFIX} protocol={PROTOCOL} uuid={storage_instance_uuid.hex()}",
            flush=True,
        )
        print(
            "Hard-stop the entire disposable VM externally now. Killing only "
            "this process does not constitute guest power-cut evidence.",
            flush=True,
        )
        while True:
            time.sleep(3600)

    cast(Any, SQLiteConnector).commit = commit
    admin.bind_storage_instance(storage_instance_uuid)
    raise RuntimeError("guest power-cut prepare unexpectedly returned")


def verify(state_directory: Path) -> None:
    target = _require_state_directory(state_directory)
    storage_instance_uuid = _read_control(target / CONTROL_FILE_NAME)
    database_path = target / DATABASE_FILE_NAME
    try:
        database_state = database_path.lstat()
    except OSError as error:
        raise HarnessUsageError("prepared SQLite database is missing") from error
    if not stat.S_ISREG(database_state.st_mode) or stat.S_ISLNK(database_state.st_mode):
        raise HarnessUsageError("prepared SQLite database must be a real file")

    config = _config(target)
    admin = VNextDatabaseAdminFacade(config)
    admin.check()
    different_uuid = bytearray(storage_instance_uuid)
    different_uuid[-1] ^= 1
    try:
        admin.bind_storage_instance(bytes(different_uuid))
    except StorageInstanceBindingMismatchError:
        pass
    else:
        raise RuntimeError("different UUID unexpectedly replaced the binding")
    replay = admin.bind_storage_instance(storage_instance_uuid)
    if replay.storage_instance_uuid != storage_instance_uuid:
        raise RuntimeError("exact storage-instance replay returned another UUID")
    with SQLiteConnector(str(database_path), read_only=True) as connector:
        if connector.fetch_one("PRAGMA integrity_check") != ("ok",):
            raise RuntimeError("SQLite integrity_check did not return ok")
        if connector.fetch_all("PRAGMA foreign_key_check") != []:
            raise RuntimeError("SQLite foreign_key_check reported violations")

    print(
        f"{VERIFIED_PREFIX} protocol={PROTOCOL} uuid={storage_instance_uuid.hex()}",
        flush=True,
    )
    print(
        "The persisted state is valid. Count this as guest power-cut evidence only "
        "if an external hypervisor hard-stopped the entire disposable VM after "
        f"{READY_PREFIX}; an ordinary process restart is only a harness protocol test.",
        flush=True,
    )


def _arguments(arguments: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("prepare", "verify"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument(
            "--state-directory",
            required=True,
            type=Path,
            help="absolute path to one dedicated disposable-VM state directory",
        )
    return parser.parse_args(arguments)


def main(arguments: Sequence[str] | None = None) -> int:
    parsed = _arguments(arguments)
    state_directory = cast(Path, parsed.state_directory)
    try:
        if os.name != "posix":
            raise HarnessUsageError(
                "the manual guest power-cut target requires a POSIX guest"
            )
        if parsed.command == "prepare":
            prepare(state_directory)
        else:
            verify(state_directory)
    except HarnessUsageError as error:
        print(f"storage-guest-powercut: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
