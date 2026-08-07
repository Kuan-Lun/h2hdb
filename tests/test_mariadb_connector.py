from typing import Any, cast

import pytest
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.errors import ProgrammingError

from h2hdb.mariadb_connector import MariaDBConnector


class _RecordingCursor:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.closed = False

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        assert data == ()
        self.queries.append(query)

    def close(self) -> None:
        self.closed = True


class _CompatibilityPrefixedConnection:
    server_version = (5, 5, 5)

    def __init__(self, *, in_transaction: bool = False) -> None:
        self.in_transaction = in_transaction
        self.cursor_instance = _RecordingCursor()

    def cursor(self, *, buffered: bool = False) -> _RecordingCursor:
        assert buffered
        return self.cursor_instance

    def start_transaction(self, **kwargs: object) -> None:
        raise AssertionError(
            "begin_read must not use Connector/Python's MySQL-only version gate: "
            f"{kwargs}"
        )


def _connector_with(
    connection: _CompatibilityPrefixedConnection,
) -> MariaDBConnector:
    connector = MariaDBConnector(
        host="database.example",
        port=3306,
        user="h2hdb",
        password="secret",
        database="h2hdb",
    )
    connector.connection = cast(MySQLConnectionAbstract, connection)
    connector._in_transaction = False
    return connector


def test_begin_read_bypasses_mariadb_compatibility_version_prefix() -> None:
    connection = _CompatibilityPrefixedConnection()
    connector = _connector_with(connection)

    connector.begin_read()

    assert connection.cursor_instance.queries == [
        "START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT"
    ]
    assert connection.cursor_instance.closed
    assert connector._in_transaction


def test_begin_read_rejects_an_existing_transaction() -> None:
    connection = _CompatibilityPrefixedConnection(in_transaction=True)
    connector = _connector_with(connection)

    with pytest.raises(ProgrammingError, match="already in progress"):
        connector.begin_read()

    assert connection.cursor_instance.queries == []
    assert not connector._in_transaction
