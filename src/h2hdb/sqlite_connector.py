import datetime
import sqlite3
from pathlib import Path
from typing import Any

from pydantic import Field

from .sql_connector import (
    DatabaseConfigurationError,
    DatabaseDuplicateKeyError,
    DatabaseReadOnlyError,
    SQLConnector,
    SQLConnectorParams,
)


def _adapt_datetime(value: datetime.datetime) -> str:
    return value.isoformat(sep=" ")


def _convert_timestamp(value: bytes) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value.decode())


# Python 3.12 deprecated (and later removed) sqlite3's default datetime
# adapter/converter. Registering our own keeps `TIMESTAMP` columns round-tripping
# through `datetime.datetime`, matching the type mysql-connector-python returns
# for MariaDB's DATETIME columns.
sqlite3.register_adapter(datetime.datetime, _adapt_datetime)
sqlite3.register_converter("TIMESTAMP", _convert_timestamp)


class SQLiteDuplicateKeyError(DatabaseDuplicateKeyError):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class SQLiteConnectorParams(SQLConnectorParams):
    """`database` must not be `:memory:`: every repository method opens and closes its
    own connection, and SQLite's in-memory databases are connection-scoped, so an
    in-memory database would lose all data between calls."""

    database: str = Field(
        min_length=1,
        description="Filesystem path to the SQLite database file",
    )
    read_only: bool = False


def _to_qmark(query: str) -> str:
    return query.replace("%s", "?")


class SQLiteConnector(SQLConnector):
    def __init__(self, database: str, read_only: bool = False) -> None:
        self.params = SQLiteConnectorParams(database=database, read_only=read_only)

    def connect(self) -> None:
        database = self.params.database
        uri = False
        if self.params.read_only:
            database = f"{Path(database).resolve().as_uri()}?mode=ro"
            uri = True
        self.connection = sqlite3.connect(
            database,
            isolation_level=None,
            detect_types=sqlite3.PARSE_DECLTYPES,
            uri=uri,
        )
        self.connection.execute("PRAGMA foreign_keys = ON")
        # Publication/head commits are only crash-safe when SQLite asks the
        # backing filesystem to make rollback-journal/WAL state durable before
        # reporting COMMIT success.  Pin and verify the connection-local safety
        # level instead of inheriting an environment-specific default.
        self.connection.execute("PRAGMA synchronous = FULL")
        if self.connection.execute("PRAGMA synchronous").fetchone() != (2,):
            self.connection.close()
            raise DatabaseConfigurationError(
                "SQLite must provide PRAGMA synchronous=FULL"
            )
        journal_row = self.connection.execute("PRAGMA journal_mode").fetchone()
        journal_mode = (
            str(journal_row[0]).lower()
            if journal_row is not None and len(journal_row) == 1
            else ""
        )
        if journal_mode in {"", "off", "memory"}:
            self.connection.close()
            raise DatabaseConfigurationError(
                "SQLite must use a durable rollback-journal or WAL mode"
            )
        if self.params.read_only:
            self.connection.execute("PRAGMA query_only = ON")

    def close(self) -> None:
        self.connection.close()

    def check_table_exists(self, table_name: str) -> bool:
        query = """
            SELECT name FROM sqlite_master WHERE type = 'table' AND name = %s
        """
        result = self.fetch_one(query, (table_name,))
        return len(result) != 0

    def commit(self) -> None:
        self.connection.commit()

    def begin(self) -> None:
        if self.params.read_only:
            raise DatabaseReadOnlyError(
                "Cannot start a write transaction in read-only mode"
            )
        self.connection.execute("BEGIN IMMEDIATE")

    def begin_read(self) -> None:
        self.connection.execute("BEGIN")

    def rollback(self) -> None:
        self.connection.rollback()

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        self._ensure_writable(query)
        try:
            self.connection.execute(_to_qmark(query), data)
        except sqlite3.IntegrityError as e:
            raise SQLiteDuplicateKeyError(str(e))

    def execute_affected(self, query: str, data: tuple[Any, ...] = ()) -> int:
        self._ensure_writable(query)
        try:
            cursor = self.connection.execute(_to_qmark(query), data)
        except sqlite3.IntegrityError as error:
            raise SQLiteDuplicateKeyError(str(error)) from error
        if cursor.rowcount < 0:
            raise RuntimeError("SQLite did not report an affected-row count")
        return cursor.rowcount

    def execute_many(self, query: str, data: list[tuple[Any, ...]]) -> None:
        self._ensure_writable(query)
        try:
            self.connection.executemany(_to_qmark(query), data)
        except sqlite3.IntegrityError as e:
            raise SQLiteDuplicateKeyError(str(e))

    def fetch_one(self, query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
        cursor = self.connection.execute(_to_qmark(query), data)
        row = cursor.fetchone()
        return row if row is not None else tuple()

    def fetch_all(
        self, query: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        cursor = self.connection.execute(_to_qmark(query), data)
        return cursor.fetchall()

    def _ensure_writable(self, query: str) -> None:
        if not self.params.read_only:
            return
        keyword = query.lstrip().split(maxsplit=1)[0].upper()
        if keyword not in {"SELECT", "PRAGMA", "EXPLAIN", "WITH"}:
            raise DatabaseReadOnlyError(
                f"Statement {keyword or '<empty>'} is not allowed in read-only mode"
            )
