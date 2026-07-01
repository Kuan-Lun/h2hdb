import datetime
import sqlite3
from typing import Any

from pydantic import Field

from .sql_connector import DatabaseDuplicateKeyError, SQLConnector, SQLConnectorParams


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


def _to_qmark(query: str) -> str:
    return query.replace("%s", "?")


class SQLiteConnector(SQLConnector):
    def __init__(self, database: str) -> None:
        self.params = SQLiteConnectorParams(database=database)

    def connect(self) -> None:
        self.connection = sqlite3.connect(
            self.params.database,
            isolation_level=None,
            detect_types=sqlite3.PARSE_DECLTYPES,
        )

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

    def rollback(self) -> None:
        self.connection.rollback()

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        try:
            self.connection.execute(_to_qmark(query), data)
        except sqlite3.IntegrityError as e:
            raise SQLiteDuplicateKeyError(str(e))

    def execute_many(self, query: str, data: list[tuple[Any, ...]]) -> None:
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
