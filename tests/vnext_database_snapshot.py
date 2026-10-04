"""Native, fixture-owned database snapshots for exhaustive fault replay.

Only stopped test writers may be cloned. This is not a production migration or
backup facility. MariaDB rows are streamed in bounded pages in one read snapshot.
"""

from __future__ import annotations

import re
import sqlite3
from contextlib import closing
from hashlib import sha256
from typing import Any

from vnext_test_database import DatabaseFactory, database_connector

from h2hdb import CoreConfig
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider

type DatabaseDigest = tuple[tuple[str, int, str], ...]


def _schema_signature(config: CoreConfig) -> tuple[tuple[tuple[Any, ...], ...], ...]:
    """Read native schema authority, excluding mutable optimizer statistics."""
    with database_connector(config) as connector, connector.read_transaction():
        if config.database.sql_type == "sqlite":
            return (
                tuple(
                    connector.fetch_all(
                        "SELECT type, name, tbl_name, sql FROM sqlite_master "
                        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
                    )
                ),
            )
        queries = (
            "SELECT TABLE_NAME, TABLE_TYPE, ENGINE, TABLE_COLLATION, CREATE_OPTIONS "
            "FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME",
            "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, "
            "COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME, EXTRA, GENERATION_EXPRESSION "
            "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME, ORDINAL_POSITION",
            "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, COLLATION, SUB_PART, INDEX_TYPE "
            "FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX",
            "SELECT TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE FROM information_schema.TABLE_CONSTRAINTS "
            "WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME, CONSTRAINT_NAME",
            "SELECT TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION, POSITION_IN_UNIQUE_CONSTRAINT, "
            "CASE WHEN REFERENCED_TABLE_SCHEMA = DATABASE() THEN '$local_schema' ELSE REFERENCED_TABLE_SCHEMA END, "
            "REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
            "WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION",
            "SELECT TABLE_NAME, CONSTRAINT_NAME, UNIQUE_CONSTRAINT_NAME, "
            "CASE WHEN UNIQUE_CONSTRAINT_SCHEMA = DATABASE() THEN '$local_schema' ELSE UNIQUE_CONSTRAINT_SCHEMA END, "
            "MATCH_OPTION, UPDATE_RULE, DELETE_RULE, REFERENCED_TABLE_NAME "
            "FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = DATABASE() ORDER BY TABLE_NAME, CONSTRAINT_NAME",
            "SELECT TABLE_NAME, CONSTRAINT_NAME, CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS "
            "WHERE CONSTRAINT_SCHEMA = DATABASE() ORDER BY TABLE_NAME, CONSTRAINT_NAME",
            "SELECT TABLE_NAME, VIEW_DEFINITION, CHECK_OPTION, IS_UPDATABLE, SECURITY_TYPE "
            "FROM information_schema.VIEWS WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME",
            "SELECT TRIGGER_NAME, EVENT_MANIPULATION, EVENT_OBJECT_TABLE, ACTION_ORDER, ACTION_CONDITION, ACTION_STATEMENT, ACTION_ORIENTATION, ACTION_TIMING, SQL_MODE "
            "FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = DATABASE() ORDER BY TRIGGER_NAME",
        )
        qualifier = _identifier(config.database.database) + "."
        return tuple(
            tuple(
                tuple(
                    value.replace(qualifier, "`fixture`.")
                    if isinstance(value, str)
                    else value
                    for value in row
                )
                for row in connector.fetch_all(query)
            )
            for query in queries
        )


def database_digest(config: CoreConfig) -> DatabaseDigest:
    """Hash every committed base-table row in bounded pages, including control.

    This independent preservation oracle assumes SHA-256 collision resistance;
    it is not exact equality or a proof of the production schema's semantics.
    The sorted native row stream and row count preserve multiplicity. At most
    128 rows plus the schema inventory are retained in Python memory.
    """
    result: list[tuple[str, int, str]] = []
    with database_connector(config) as connector, connector.read_transaction():
        if config.database.sql_type == "sqlite":
            tables = connector.fetch_all(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        else:
            tables = connector.fetch_all(
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"
            )
        for (table,) in tables:
            if config.database.sql_type == "sqlite":
                columns = [
                    str(row[1])
                    for row in connector.fetch_all(
                        f"PRAGMA table_info({_identifier(table)})"
                    )
                ]
            else:
                columns = [
                    str(row[0])
                    for row in connector.fetch_all(
                        "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
                        (table,),
                    )
                ]
            order = ", ".join(_identifier(name) for name in columns)
            digest = sha256()
            count = 0
            while rows := connector.fetch_all(
                f"SELECT * FROM {_identifier(table)} ORDER BY {order} LIMIT 128 OFFSET %s",
                (count,),
            ):
                for row in rows:
                    payload = repr(tuple(row)).encode("utf-8")
                    digest.update(len(payload).to_bytes(8, "big"))
                    digest.update(payload)
                count += len(rows)
            result.append((table, count, digest.hexdigest()))
    return tuple(result)


class ReusableDatabaseSnapshot:
    """Restore one fixture-owned, stopped target for committed data-fault replay.

    Native DDL is cloned once. MariaDB restores all table contents in one
    committed transaction via same-server INSERT SELECT; SQLite uses backup.
    Schema mutations are rejected instead of being silently repaired. Consumers
    must close their public facades before restore; factory-owned raw helper
    handles are closed here. This never operates on arbitrary database names.
    """

    def __init__(
        self, factory: DatabaseFactory, source: CoreConfig, target: CoreConfig
    ) -> None:
        if (source.database.host, source.database.port, source.database.user) != (
            target.database.host,
            target.database.port,
            target.database.user,
        ):
            raise ValueError("reusable snapshot requires the same native server")
        factory.close_connections(source)
        factory.close_connections(target)
        clone_database(source, target)
        self.factory = factory
        self.source = source
        self.target = target
        self.schema = _schema_signature(source)
        if _schema_signature(target) != self.schema:
            raise AssertionError("native snapshot changed schema")
        self.tables = tuple(table for table, _, _ in database_digest(source))

    def restore(self) -> CoreConfig:
        self.factory.close_connections(self.source)
        self.factory.close_connections(self.target)
        if _schema_signature(self.target) != self.schema:
            raise ValueError("reusable data snapshot cannot restore schema drift")
        if self.source.database.sql_type == "sqlite":
            with (
                closing(sqlite3.connect(self.source.database.database)) as reader,
                closing(sqlite3.connect(self.target.database.database)) as writer,
            ):
                reader.backup(writer)
        else:
            source_db = _identifier(self.source.database.database)
            with database_connector(self.target) as writer:
                writer.execute("SET FOREIGN_KEY_CHECKS = 0")
                writer.execute("SET check_constraint_checks = 0")
                try:
                    with writer.transaction():
                        for table in self.tables:
                            quoted = _identifier(table)
                            writer.execute(f"DELETE FROM {quoted}")
                            writer.execute(
                                f"INSERT INTO {quoted} SELECT * FROM {source_db}.{quoted}"
                            )
                finally:
                    writer.execute("SET check_constraint_checks = 1")
                    writer.execute("SET FOREIGN_KEY_CHECKS = 1")
        return self.target


def _identifier(value: str) -> str:
    if re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", value) is None:
        raise ValueError("test snapshot requires plain SQL identifiers")
    return f"`{value}`"


def clone_database(source: CoreConfig, target: CoreConfig) -> None:
    """Copy explicit disposable fixtures, including intentionally malformed rows."""
    if source.database.sql_type != target.database.sql_type:
        raise ValueError("fault replay requires the same native backend")
    if source.database.database == target.database.database:
        raise ValueError("source and target must be separate test databases")
    if source.database.sql_type == "sqlite":
        with (
            closing(sqlite3.connect(source.database.database)) as reader,
            closing(sqlite3.connect(target.database.database)) as writer,
        ):
            if writer.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone():
                raise ValueError("snapshot target must be empty")
            reader.backup(writer)
        return
    for config in (source, target):
        if config.database.host not in {"127.0.0.1", "localhost"} or not (
            config.database.database.startswith("h2hdb_test_")
        ):
            raise ValueError("MariaDB clone requires local fixture-owned databases")
    with database_connector(source) as reader, database_connector(target) as writer:
        if writer.fetch_one(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() LIMIT 1"
        ):
            raise ValueError("snapshot target must be empty")
        tables = reader.fetch_all(
            "SELECT table_name, table_type FROM information_schema.tables "
            "WHERE table_schema = DATABASE() ORDER BY table_name"
        )
        writer.execute("SET FOREIGN_KEY_CHECKS = 0")
        writer.execute("SET check_constraint_checks = 0")
        try:
            for name, kind in tables:
                if kind != "BASE TABLE":
                    continue
                quoted = _identifier(name)
                ddl = reader.fetch_one(f"SHOW CREATE TABLE {quoted}")[1]
                writer.execute(ddl)
            # Views reference only generated current-schema relation names.
            # SHOW CREATE may qualify the source database: rewrite only that
            # exact quoted identifier, never values or arbitrary SQL text.
            views = {name for name, kind in tables if kind == "VIEW"}
            view_order = [
                statement.creates.name
                for schema_slice in GeneratedVNextSchemaProvider(
                    "mariadb"
                ).definition.slices
                for statement in schema_slice.statements
                if statement.creates.name in views
            ]
            if set(view_order) != views:
                raise ValueError("snapshot contains views outside the generated schema")
            for name in view_order:
                ddl = reader.fetch_one(f"SHOW CREATE VIEW {_identifier(name)}")[1]
                ddl = ddl.replace(
                    _identifier(source.database.database) + ".",
                    _identifier(target.database.database) + ".",
                )
                writer.execute(ddl)
            with reader.read_transaction(), writer.transaction():
                for name, kind in tables:
                    if kind != "BASE TABLE":
                        continue
                    quoted = _identifier(name)
                    # The source writer has stopped; OFFSET cannot skip rows.
                    offset = 0
                    while rows := reader.fetch_all(
                        f"SELECT * FROM {quoted} LIMIT 128 OFFSET %s", (offset,)
                    ):
                        binds = ", ".join("%s" for _ in rows[0])
                        writer.execute_many(
                            f"INSERT INTO {quoted} VALUES ({binds})", rows
                        )
                        offset += len(rows)
        finally:
            writer.execute("SET check_constraint_checks = 1")
            writer.execute("SET FOREIGN_KEY_CHECKS = 1")
