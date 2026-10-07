"""FK-preserving copies of closed, public-workflow synthetic databases.

This development helper owns every database it can modify. It is neither a
production backup facility nor the malformed-graph fault snapshot helper.
Callers must close their facades and adapter writers before sealing or copying.
Native source locks exclude concurrent writes while each preservation proof is
made. No external adapter resources are copied.
"""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import sqlite3
import tempfile
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from vnext_database_snapshot import _schema_signature
from vnext_test_database import DatabaseFactory, database_connector

from h2hdb import CoreConfig, DatabaseConfig, VNextDatabaseAdminFacade
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.schema_epoch import SCHEMA_EPOCH_CONTROL_TABLE
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider

type Backend = Literal["sqlite", "mariadb"]
type Content = tuple[tuple[str, int, str], ...]

_PREFIX = "h2hdb_public_seed_"
_OWNED: dict[int, OwnedDatabase] = {}


@dataclass(eq=False, repr=False)
class OwnedDatabase:
    """An identity capability for one disposable database, never credentials."""

    _config: CoreConfig
    _sealed_content: Content | None = field(default=None, init=False)
    _schema: object = field(default=None, init=False)
    _copied: bool = field(default=False, init=False)

    @property
    def config(self) -> CoreConfig:
        """Give writers access only before the fixture becomes a sealed seed."""
        _require_owned(self)
        if self._sealed_content is not None:
            raise ValueError("sealed public seed cannot open another writer")
        return self._config


def _require_owned(database: OwnedDatabase) -> None:
    if _OWNED.get(id(database)) is not database:
        raise ValueError("snapshot requires this helper's live owned database")


def _identifier(value: str) -> str:
    if re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", value) is None:
        raise ValueError("fixture requires plain generated SQL identifiers")
    return f"`{value}`"


def _server_options(config: DatabaseConfig) -> dict[str, Any]:
    if config.host not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("public fixture snapshots require a loopback server")
    return {
        "host": config.host,
        "port": config.port,
        "user": config.user,
        "password": config.password,
    }


@contextmanager
def owned_database_from_factory(factory: DatabaseFactory) -> Iterator[OwnedDatabase]:
    """Use pytest's registered allocator without inventing admin credentials."""
    name = "public-seed-" + secrets.token_hex(12)
    config = factory.config(name)
    if config.database.sql_type == "mariadb":
        _server_options(config.database)
        if not config.database.database.startswith("h2hdb_test_"):
            raise ValueError("public test snapshot requires a local fixture database")
    factory.close_connections(config)
    owned = OwnedDatabase(config)
    _OWNED[id(owned)] = owned
    try:
        yield owned
    finally:
        try:
            factory.release(name)
        finally:
            del _OWNED[id(owned)]


@contextmanager
def owned_database_from_config(authority: CoreConfig) -> Iterator[OwnedDatabase]:
    """Allocate a fresh sibling using explicit CREATE/DROP authority.

    Never adopt or modify the input database. Restricted pytest credentials
    must instead use ``owned_database_from_factory`` and its registered owner.
    """
    if authority.database.sql_type == "sqlite":
        with tempfile.TemporaryDirectory(prefix=_PREFIX) as directory:
            config = authority.model_copy(
                update={
                    "database": DatabaseConfig(
                        sql_type="sqlite",
                        database=str(Path(directory) / "catalog.sqlite3"),
                    )
                }
            )
            owned = OwnedDatabase(config)
            _OWNED[id(owned)] = owned
            try:
                yield owned
            finally:
                del _OWNED[id(owned)]
        return
    import mysql.connector

    options = _server_options(authority.database)
    name = _PREFIX + secrets.token_hex(12)
    with mysql.connector.connect(**options) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {_identifier(name)}")
    config = authority.model_copy(
        update={"database": authority.database.model_copy(update={"database": name})}
    )
    owned = OwnedDatabase(config)
    _OWNED[id(owned)] = owned
    try:
        yield owned
    finally:
        try:
            with mysql.connector.connect(**options) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(f"DROP DATABASE {_identifier(name)}")
        finally:
            del _OWNED[id(owned)]


@contextmanager
def owned_database(
    backend: Backend, private_config: Path | None = None
) -> Iterator[OwnedDatabase]:
    """Use an explicit local server file or a disposable SQLite directory."""
    options: dict[str, Any] = {}
    if backend == "mariadb":
        if private_config is None:
            raise ValueError("MariaDB requires an explicit private local config")
        options = json.loads(private_config.read_text())
        # A private server connection may name its administrative database. The
        # factory creates a separate random database and never adopts that one.
        options.pop("database", None)
    authority = CoreConfig(database=DatabaseConfig(sql_type=backend, **options))
    with owned_database_from_config(authority) as owned:
        yield owned


def _tables(backend: Backend) -> tuple[tuple[str, tuple[str, ...]], ...]:
    payload = GeneratedVNextSchemaProvider(backend).generated_definition_data
    relations = {row["relation"]: row for row in payload["relations"]}
    ordered = [
        relations[name]
        for name in ARTIFACT["relation_order"]
        if relations[name]["kind"] == "table"
    ]
    # Epoch control is intentionally outside the generated dependency graph.
    # It has no data-plane FK; its sole row is still copied and fingerprinted.
    return ((SCHEMA_EPOCH_CONTROL_TABLE, ("singleton_id",)),) + tuple(
        (row["table"], tuple(row["primary_key"])) for row in ordered
    )


def _backend(config: CoreConfig) -> Backend:
    return "sqlite" if config.database.sql_type == "sqlite" else "mariadb"


def _constraints_enabled(connector: SQLConnector, backend: Backend) -> None:
    if backend == "sqlite":
        actual = connector.fetch_one("PRAGMA foreign_keys")
        checks = connector.fetch_one("PRAGMA ignore_check_constraints")
        if actual != (1,) or checks != (0,):
            raise ValueError("fixture connection must keep FK and CHECK checks enabled")
    elif connector.fetch_one(
        "SELECT @@SESSION.foreign_key_checks, @@SESSION.check_constraint_checks"
    ) != (1, 1):
        raise ValueError("fixture connection must keep FK and CHECK checks enabled")


@contextmanager
def _exclude_source_writers(config: CoreConfig) -> Iterator[None]:
    """Hold native write exclusion for this owned source, never the server."""
    if _backend(config) == "sqlite":
        with closing(sqlite3.connect(config.database.database, timeout=0)) as guard:
            guard.execute("PRAGMA foreign_keys = ON")
            guard.execute("BEGIN IMMEDIATE")
            try:
                yield
            finally:
                guard.rollback()
        return
    with database_connector(config) as guard:
        _constraints_enabled(guard, "mariadb")
        guard.execute("SET SESSION lock_wait_timeout = 1")
        tables = ", ".join(
            f"{_identifier(name)} READ" for name, _key in _tables("mariadb")
        )
        guard.execute("LOCK TABLES " + tables)
        try:
            yield
        finally:
            guard.execute("UNLOCK TABLES")


def _content(config: CoreConfig) -> Content:
    """Stream PK-ordered native rows; memory is bounded and OFFSET is avoided."""
    result: list[tuple[str, int, str]] = []
    with database_connector(config) as connector, connector.read_transaction():
        _constraints_enabled(connector, _backend(config))
        connection: Any = getattr(connector, "connection", None)
        if connection is None:
            raise AssertionError("fixture connector did not open a native connection")
        for table, key in _tables(_backend(config)):
            digest = hashlib.sha256()
            count = 0
            order = ", ".join(_identifier(column) for column in key)
            with closing(connection.cursor()) as cursor:
                cursor.execute(f"SELECT * FROM {_identifier(table)} ORDER BY {order}")
                while rows := cursor.fetchmany(128):
                    for row in rows:
                        payload = repr(tuple(row)).encode("utf-8")
                        digest.update(len(payload).to_bytes(8, "big"))
                        digest.update(payload)
                    count += len(rows)
            result.append((table, count, digest.hexdigest()))
    return tuple(result)


def _ready(config: CoreConfig) -> None:
    with closing(VNextDatabaseAdminFacade(config)) as admin:
        if admin.check().state != "READY":
            raise ValueError("public fixture must pass a complete READY audit")


def _receipt(database: OwnedDatabase, content: Content) -> dict[str, Any]:
    return {
        "backend": _backend(database._config),
        "READY": "READY",
        "foreign_keys": 1,
        "check_constraints": 1,
        "schema_sha256": hashlib.sha256(repr(database._schema).encode()).hexdigest(),
        "tables": [
            {"table": table, "rows": rows, "sha256": digest}
            for table, rows, digest in content
        ],
        "content_sha256": hashlib.sha256(repr(content).encode()).hexdigest(),
        "preservation_oracle": "PK-ordered all-table row counts and SHA-256; assumes collision resistance",
        "scope": "complete synthetic database only; external adapter resources excluded",
    }


def seal_public_seed(source: OwnedDatabase) -> dict[str, Any]:
    """Freeze a closed public seed after a full audit and native content proof."""
    _require_owned(source)
    if source._sealed_content is not None:
        raise ValueError("public seed is already sealed")
    with _exclude_source_writers(source._config):
        _ready(source._config)
        source._schema = _schema_signature(source._config)
        source._sealed_content = _content(source._config)
    return _receipt(source, source._sealed_content)


def clone_public_seed(source: OwnedDatabase, target: OwnedDatabase) -> dict[str, Any]:
    """Copy or restore an owned target with FK/CHECK enforcement always enabled."""
    for database in (source, target):
        _require_owned(database)
    if source is target or source._config.database == target._config.database:
        raise ValueError("source and target must be separate owned databases")
    if source._sealed_content is None or target._sealed_content is not None:
        raise ValueError("clone requires a sealed source and writable owned target")
    left, right = source._config.database, target._config.database
    if (left.sql_type, left.host, left.port, left.user) != (
        right.sql_type,
        right.host,
        right.port,
        right.user,
    ):
        raise ValueError("snapshot requires the same backend and native server")
    with _exclude_source_writers(source._config):
        if _schema_signature(source._config) != source._schema:
            raise ValueError("sealed public seed schema changed")
        if _content(source._config) != source._sealed_content:
            raise ValueError("sealed public seed content changed")
        if not target._copied:
            with database_connector(target._config) as inspector:
                query = (
                    "SELECT name FROM sqlite_master LIMIT 1"
                    if _backend(target._config) == "sqlite"
                    else "SELECT TABLE_NAME FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE() LIMIT 1"
                )
                if inspector.fetch_one(query):
                    raise ValueError("initial clone target must be empty")
            with closing(VNextDatabaseAdminFacade(target._config)) as admin:
                admin.initialize()
        if _schema_signature(target._config) != source._schema:
            raise ValueError("owned target schema drift cannot be restored")
        if _backend(source._config) == "sqlite":
            with (
                closing(
                    sqlite3.connect(Path(left.database).as_uri() + "?mode=ro", uri=True)
                ) as reader,
                closing(sqlite3.connect(right.database)) as native_writer,
            ):
                reader.execute("PRAGMA foreign_keys = ON")
                native_writer.execute("PRAGMA foreign_keys = ON")
                reader.backup(native_writer)
                if (
                    native_writer.execute("PRAGMA foreign_key_check").fetchone()
                    is not None
                ):
                    raise AssertionError(
                        "public snapshot introduced a foreign-key violation"
                    )
        else:
            with database_connector(target._config) as writer, writer.transaction():
                _constraints_enabled(writer, "mariadb")
                tables = _tables("mariadb")
                for table, _key in reversed(tables):
                    writer.execute(f"DELETE FROM {_identifier(table)}")
                source_db = _identifier(left.database)
                for table, _key in tables:
                    quoted = _identifier(table)
                    writer.execute(
                        f"INSERT INTO {quoted} SELECT * FROM {source_db}.{quoted}"
                    )
                _constraints_enabled(writer, "mariadb")
        _ready(target._config)
        target._schema = _schema_signature(target._config)
        content = _content(target._config)
        if target._schema != source._schema or content != source._sealed_content:
            raise AssertionError("public snapshot did not preserve schema and all rows")
        target._copied = True
    return _receipt(target, content)
