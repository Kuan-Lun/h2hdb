"""Fast bare generated-SQLite setup for repository tests.

This helper deliberately does not exercise schema-epoch admission, recovery,
validation, or READY publication.  Tests for those production responsibilities
must continue to use ``SchemaEpochRunner`` or the public administration facade.
"""

from __future__ import annotations

from functools import cache
from itertools import groupby
from pathlib import Path

from h2hdb.schema_epoch import SchemaEpochDefinition
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider


@cache
def _generated_sqlite_definition() -> SchemaEpochDefinition:
    return GeneratedVNextSchemaProvider("sqlite").definition


def open_generated_sqlite_database(
    path: Path,
    *,
    connector_type: type[SQLiteConnector] = SQLiteConnector,
) -> SQLiteConnector:
    """Open a bare generated database with one atomic bootstrap transaction."""

    definition = _generated_sqlite_definition()
    connector = connector_type(str(path))
    connector.connect()
    try:
        with connector.transaction():
            for schema_slice in definition.slices:
                for statement in schema_slice.statements:
                    connector.execute(statement.sql)
            for sql, seed_group in groupby(
                definition.bootstrap_seeds,
                key=lambda seed: seed.sql,
            ):
                connector.execute_many(
                    sql,
                    [seed.parameters for seed in seed_group],
                )
    except BaseException as error:
        try:
            connector.close()
        except BaseException as close_error:
            error.add_note(f"generated SQLite connector close failed: {close_error}")
        raise
    return connector
