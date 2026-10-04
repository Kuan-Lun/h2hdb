"""Native, fixture-owned database snapshots for exhaustive fault replay.

Only stopped test writers may be cloned. This is not a production migration or
backup facility. MariaDB rows are streamed in bounded pages in one read snapshot.
"""

from __future__ import annotations

import re
import sqlite3
from contextlib import closing

from vnext_test_database import database_connector

from h2hdb import CoreConfig
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider


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
