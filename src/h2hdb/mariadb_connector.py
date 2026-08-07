from types import TracebackType
from typing import Any, cast

from mysql.connector import connect as SQLConnect
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract
from mysql.connector.errors import IntegrityError
from mysql.connector.pooling import PooledMySQLConnection
from pydantic import Field

from .sql_connector import (
    DatabaseDuplicateKeyError,
    DatabaseReadOnlyError,
    SQLConnector,
    SQLConnectorParams,
)

AUTO_COMMIT_KEYS = ["INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER"]


class MariaDBDuplicateKeyError(DatabaseDuplicateKeyError):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class MariaDBConnectorParams(SQLConnectorParams):
    host: str = Field(
        min_length=1,
        description="Host of the MariaDB database",
    )
    port: int = Field(
        ge=1,
        le=65535,
        description="Port of the MariaDB database",
    )
    user: str = Field(
        min_length=1,
        description="User for the MariaDB database",
    )
    password: str = Field(
        description="Password for the MariaDB database",
    )
    database: str = Field(
        min_length=1,
        description="Database name for the MariaDB database",
    )
    read_only: bool = False


class MariaDBCursor:
    def __init__(
        self, connection: PooledMySQLConnection | MySQLConnectionAbstract
    ) -> None:
        self.connection = connection

    def __enter__(self) -> MySQLCursorAbstract:
        self.cursor = self.connection.cursor(buffered=True)
        return self.cursor

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.cursor.close()


class MariaDBConnector(SQLConnector):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        read_only: bool = False,
    ) -> None:
        self.params = MariaDBConnectorParams(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            read_only=read_only,
        )

    def connect(self) -> None:
        connection_params = self.params.model_dump(exclude={"read_only"})
        self.connection = SQLConnect(**connection_params)
        self._in_transaction = False
        if self.params.read_only:
            with MariaDBCursor(self.connection) as cursor:
                cursor.execute("SET SESSION TRANSACTION READ ONLY")

    def close(self) -> None:
        self.connection.close()

    def check_table_exists(self, table_name: str) -> bool:
        query = f"SHOW TABLES LIKE '{table_name}'"
        result = self.fetch_one(query)
        return bool(result)

    def commit(self) -> None:
        self.connection.commit()
        self._in_transaction = False

    def begin(self) -> None:
        if self.params.read_only:
            raise DatabaseReadOnlyError(
                "Cannot start a write transaction in read-only mode"
            )
        self.connection.start_transaction()
        self._in_transaction = True

    def begin_read(self) -> None:
        self.connection.start_transaction(readonly=True, consistent_snapshot=True)
        self._in_transaction = True

    def rollback(self) -> None:
        self.connection.rollback()
        self._in_transaction = False

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        self._ensure_writable(query)
        with MariaDBCursor(self.connection) as cursor:
            try:
                cursor.execute(query, data)
            except IntegrityError as e:
                raise MariaDBDuplicateKeyError(str(e))
            except Exception as e:
                raise e
        if not self._in_transaction and any(
            key in query.upper() for key in AUTO_COMMIT_KEYS
        ):
            self.commit()

    def execute_many(self, query: str, data: list[tuple[Any, ...]]) -> None:
        self._ensure_writable(query)
        with MariaDBCursor(self.connection) as cursor:
            try:
                cursor.executemany(query, data)
            except IntegrityError as e:
                raise MariaDBDuplicateKeyError(str(e))
        if not self._in_transaction and any(
            key in query.upper() for key in AUTO_COMMIT_KEYS
        ):
            self.commit()

    def fetch_one(self, query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
        with MariaDBCursor(self.connection) as cursor:
            cursor.execute(query, data)
            vlist = cursor.fetchone()
        if isinstance(vlist, tuple):
            return vlist
        else:
            return tuple()

    def fetch_all(
        self, query: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        with MariaDBCursor(self.connection) as cursor:
            cursor.execute(query, data)
            vlist = cursor.fetchall()
        return cast(list[tuple[Any, ...]], vlist)

    def _ensure_writable(self, query: str) -> None:
        if not self.params.read_only:
            return
        keyword = query.lstrip().split(maxsplit=1)[0].upper()
        if keyword not in {"SELECT", "SHOW", "EXPLAIN", "WITH"}:
            raise DatabaseReadOnlyError(
                f"Statement {keyword or '<empty>'} is not allowed in read-only mode"
            )
