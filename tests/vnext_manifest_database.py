"""Execute manifest-rendered fixtures on the selected native SQL engine."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from types import ModuleType
from typing import Any

import pytest
from vnext_test_database import connector_backend, inspection_snapshot

from h2hdb.sql_connector import DatabaseDuplicateKeyError, SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector


def render_fixture(
    connector: SQLConnector,
    renderer: ModuleType,
    *arguments: Any,
    **options: Any,
) -> None:
    backend = connector_backend(connector)
    if backend == "sqlite":
        assert isinstance(connector, SQLiteConnector)
        assert connector.connection is not None
        connector.connection.executescript(
            renderer.render_sqlite_ddl(*arguments, **options)
        )
    else:
        # MariaDB DDL commits implicitly; fixture DML owns separate transactions.
        for statement in renderer.render_mariadb_ddl(*arguments, **options):
            connector.execute(statement)


def introspect_fixture(connector: SQLConnector, refinement: ModuleType) -> Any:
    with inspection_snapshot(connector):
        if connector_backend(connector) == "sqlite":
            return refinement.introspect_sqlite(connector)
        return refinement.introspect_mariadb(connector)


@contextmanager
def constraint_violation(connector: SQLConnector) -> Iterator[None]:
    """Only native constraint/domain errors count as rejection of a bad row."""
    try:
        yield
    except Exception as error:
        if isinstance(error, DatabaseDuplicateKeyError):
            return
        if connector_backend(connector) == "sqlite":
            if not isinstance(error, sqlite3.IntegrityError):
                raise
        elif not (
            type(error).__module__.startswith(("mysql.", "h2hdb.mariadb_connector"))
            and getattr(error, "errno", None)
            in {1048, 1062, 1264, 1364, 1406, 1451, 1452, 4025}
        ):
            # Connector duplicate-key errors wrap the original native cause.
            cause = error.__cause__
            if not (
                cause is not None
                and type(cause).__module__.startswith("mysql.")
                and getattr(cause, "errno", None) == 1062
            ):
                raise
    else:
        pytest.fail("native engine accepted a fixture row that violates its domain")


@contextmanager
def view_write_rejection(connector: SQLConnector) -> Iterator[None]:
    """Require this statement to fail; this does not imply every view is read-only."""
    try:
        yield
    except Exception as error:
        if connector_backend(connector) == "sqlite":
            if not isinstance(error, sqlite3.OperationalError) or "view" not in str(
                error
            ):
                raise
        elif not (
            type(error).__module__.startswith("mysql.")
            and getattr(error, "errno", None) in {1288, 1348, 1393, 1395, 1471}
        ):
            raise
    else:
        pytest.fail("native engine accepted the forbidden derived-view statement")
