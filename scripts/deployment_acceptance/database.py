"""Native authority checks for the disposable deployment's Core database.

Executed only inside the selected role image and project-owned mounts. These
checks do not replace the publication/READY/bytes oracle in fixture.py.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

from h2hdb import VNextCatalogFacade
from h2hdb.config_loader import CoreConfig, DatabaseAccessMode, load_config
from h2hdb.repository import RepositoryContext
from h2hdb.sql_connector import DatabaseReadOnlyError, SQLConnector

SQLITE_DIRECTORY = Path("/h2hdb-database")
SQLITE_DATABASE = SQLITE_DIRECTORY / "catalog.sqlite3"
DATABASE_NAME = "h2hdb_acceptance"


def initialize_sqlite(*, uid: int, gid: int) -> dict[str, object]:
    """Prepare only a fresh named volume; leave schema initialization to ingest."""
    if uid <= 0 or gid <= 0 or os.geteuid() != 0:
        raise ValueError("Volume initialization requires root and non-root role IDs")
    if SQLITE_DIRECTORY.is_symlink() or not SQLITE_DIRECTORY.is_dir():
        raise ValueError("Missing dedicated SQLite volume")
    if any(SQLITE_DIRECTORY.iterdir()):
        raise ValueError("SQLite volume is not empty; refusing reuse")
    # An exclusive file creation prevents accidental truncation even if another
    # process changes the volume between admission and connect.
    with SQLITE_DATABASE.open("xb"):
        pass
    with closing(sqlite3.connect(SQLITE_DATABASE)) as connection:
        if connection.execute("PRAGMA journal_mode=WAL").fetchone() != ("wal",):
            raise AssertionError("SQLite failed to enable WAL")
        if connection.execute("SELECT name FROM sqlite_master").fetchall():
            raise AssertionError("SQLite initialization created schema objects")
    os.chown(SQLITE_DATABASE, uid, gid)
    SQLITE_DATABASE.chmod(0o600)
    os.chown(SQLITE_DIRECTORY, uid, gid)
    SQLITE_DIRECTORY.chmod(0o700)
    return {"status": "passed", "backend": "sqlite", "journal_mode": "wal"}


def _native_authority(
    connector: SQLConnector, config: CoreConfig, *, backend: str, role: str
) -> dict[str, Any]:
    reader = role == "opds"
    if role not in {"ingest", "opds"} or config.database.sql_type != backend:
        raise AssertionError("Role or selected Core backend differs")
    if (config.database.access_mode is DatabaseAccessMode.read_only) != reader:
        raise AssertionError("Role Core access mode differs")
    if backend == "sqlite":
        rows = connector.fetch_all("PRAGMA database_list")
        main = [row for row in rows if row[1] == "main"]
        if len(main) != 1 or Path(main[0][2]) != SQLITE_DATABASE:
            raise AssertionError("Native SQLite main is not the Core volume file")
        pragmas = {
            name: connector.fetch_one("PRAGMA " + name)[0]
            for name in ("journal_mode", "query_only", "foreign_keys", "synchronous")
        }
        if pragmas != {
            "journal_mode": "wal",
            "query_only": int(reader),
            "foreign_keys": 1,
            "synchronous": 2,
        }:
            raise AssertionError("SQLite durability/access contract differs")
        if reader:
            try:
                connector.fetch_one("PRAGMA user_version = 0")
            except sqlite3.OperationalError as error:
                if error.sqlite_errorcode != sqlite3.SQLITE_READONLY:
                    raise
            else:
                raise AssertionError("SQLite reader accepted a native write")
        state = SQLITE_DATABASE.stat()
        result: dict[str, Any] = {
            "database": str(SQLITE_DATABASE),
            "device": state.st_dev,
            "inode": state.st_ino,
            "native_write_refused": reader,
            **pragmas,
        }
    elif backend == "mariadb":
        database, version, readonly = connector.fetch_one(
            "SELECT DATABASE(), VERSION(), @@SESSION.tx_read_only"
        )
        if database != DATABASE_NAME or "MariaDB" not in str(version):
            raise AssertionError("Native server is not the disposable MariaDB database")
        if int(readonly) != int(reader):
            raise AssertionError("MariaDB session access mode differs")
        result = {
            "database": database,
            "server_version": version,
            "transaction_read_only": int(readonly),
        }
    else:
        raise AssertionError("Unknown Core backend")
    if reader:
        try:
            connector.begin()
        except DatabaseReadOnlyError:
            result["write_transaction_refused"] = True
        else:
            connector.rollback()
            raise AssertionError("Reader accepted a write transaction")
    return {"status": "passed", "backend": backend, "role": role, **result}


def inspect_database(*, config_path: Path, backend: str, role: str) -> dict[str, Any]:
    config = load_config(config_path)
    with closing(RepositoryContext.from_config(config)) as context:
        with context.SQLConnector() as connector:
            result = _native_authority(connector, config, backend=backend, role=role)
    with closing(VNextCatalogFacade(config)) as catalog:
        revision = catalog.get_catalog_revision()
    result.update(revision=revision.revision, publications=revision.publication_count)
    return result


def validate_role_authorities(
    values: dict[str, Any], *, backend: str, revision: int, publications: int
) -> None:
    if set(values) != {"ingest", "opds"}:
        raise AssertionError("Missing role Core database authority")
    for role, value in values.items():
        if any(
            value.get(key) != expected
            for key, expected in {
                "status": "passed",
                "backend": backend,
                "role": role,
                "revision": revision,
                "publications": publications,
            }.items()
        ):
            raise AssertionError("Role authority differs from the catalog oracle")
        if role == "opds" and value.get("write_transaction_refused") is not True:
            raise AssertionError("Missing native reader refusal evidence")
    if backend == "sqlite":
        for role, value in values.items():
            expected = {
                "database": str(SQLITE_DATABASE),
                "journal_mode": "wal",
                "query_only": int(role == "opds"),
                "foreign_keys": 1,
                "synchronous": 2,
                "native_write_refused": role == "opds",
            }
            if any(value.get(key) != item for key, item in expected.items()):
                raise AssertionError("Missing native SQLite access/durability evidence")
    elif backend == "mariadb":
        if any(
            value.get("database") != DATABASE_NAME
            or not isinstance(value.get("server_version"), str)
            or "MariaDB" not in value["server_version"]
            or value.get("transaction_read_only") != int(role == "opds")
            for role, value in values.items()
        ):
            raise AssertionError("Missing native MariaDB access evidence")
    else:
        raise AssertionError("Unknown selected Core backend")
    fields = (
        ("database", "device", "inode")
        if backend == "sqlite"
        else ("database", "server_version")
    )
    if any(values["ingest"].get(key) != values["opds"].get(key) for key in fields):
        raise AssertionError("Roles opened different native Core databases")
    if backend == "sqlite" and any(
        type(values["ingest"].get(key)) is not int or values["ingest"][key] <= 0
        for key in ("device", "inode")
    ):
        raise AssertionError("Missing SQLite volume identity")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    initialize = commands.add_parser("initialize-sqlite")
    initialize.add_argument("--uid", type=int, default=65534)
    initialize.add_argument("--gid", type=int, default=65534)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--config", type=Path, required=True)
    inspect.add_argument("--backend", choices=("sqlite", "mariadb"), required=True)
    inspect.add_argument("--role", choices=("ingest", "opds"), required=True)
    args = parser.parse_args(argv)
    result = (
        initialize_sqlite(uid=args.uid, gid=args.gid)
        if args.command == "initialize-sqlite"
        else inspect_database(
            config_path=args.config, backend=args.backend, role=args.role
        )
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
