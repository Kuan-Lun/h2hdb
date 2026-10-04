"""SQLite-only physical durability settings."""

from pathlib import Path

import pytest

from h2hdb.sqlite_connector import SQLiteConnector


@pytest.mark.backend_specific(
    backend="sqlite",
    reason="SQLite journal and synchronous PRAGMAs specify its file durability mode.",
)
def test_connect_pins_durable_sqlite_settings(tmp_path: Path) -> None:
    with SQLiteConnector(
        database=str(tmp_path / "connector_test.sqlite3")
    ) as connector:
        assert connector.fetch_one("PRAGMA synchronous") == (2,)
        assert connector.fetch_one("PRAGMA journal_mode")[0] not in {"off", "memory"}
