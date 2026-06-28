from pathlib import Path

import pytest

from h2hdb.sql_connector import DatabaseDuplicateKeyError
from h2hdb.sqlite_connector import SQLiteConnector


@pytest.fixture
def connector(tmp_path: Path) -> SQLiteConnector:
    return SQLiteConnector(database=str(tmp_path / "connector_test.sqlite3"))


def test_check_table_exists(connector: SQLiteConnector) -> None:
    with connector:
        assert connector.check_table_exists("widgets") is False
        connector.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY)")
        assert connector.check_table_exists("widgets") is True


def test_execute_and_fetch_round_trip(connector: SQLiteConnector) -> None:
    with connector:
        connector.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)")
        connector.execute("INSERT INTO widgets (id, name) VALUES (%s, %s)", (1, "a"))
        assert connector.fetch_one("SELECT name FROM widgets WHERE id = %s", (1,)) == (
            "a",
        )
        assert connector.fetch_all("SELECT name FROM widgets") == [("a",)]


def test_execute_many(connector: SQLiteConnector) -> None:
    with connector:
        connector.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)")
        connector.execute_many(
            "INSERT INTO widgets (id, name) VALUES (%s, %s)",
            [(1, "a"), (2, "b")],
        )
        assert connector.fetch_all("SELECT id, name FROM widgets ORDER BY id") == [
            (1, "a"),
            (2, "b"),
        ]


def test_duplicate_key_raises(connector: SQLiteConnector) -> None:
    with connector:
        connector.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY)")
        connector.execute("INSERT INTO widgets (id) VALUES (%s)", (1,))
        with pytest.raises(DatabaseDuplicateKeyError):
            connector.execute("INSERT INTO widgets (id) VALUES (%s)", (1,))


def test_blob_round_trip(connector: SQLiteConnector) -> None:
    with connector:
        connector.execute("CREATE TABLE hashes (value BLOB)")
        hash_value = bytes.fromhex("ab" * 64)
        connector.execute("INSERT INTO hashes (value) VALUES (%s)", (hash_value,))
        assert connector.fetch_one("SELECT value FROM hashes") == (hash_value,)


def test_data_persists_across_reconnects(connector: SQLiteConnector) -> None:
    with connector:
        connector.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY)")
        connector.execute("INSERT INTO widgets (id) VALUES (%s)", (1,))

    with connector:
        assert connector.fetch_all("SELECT id FROM widgets") == [(1,)]
