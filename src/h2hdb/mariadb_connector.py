from types import TracebackType
from typing import Any, cast

from mysql.connector import connect as SQLConnect
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract
from mysql.connector.errors import IntegrityError
from mysql.connector.pooling import PooledMySQLConnection
from pydantic import Field

from .sql_connector import DatabaseDuplicateKeyError, SQLConnector, SQLConnectorParams

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
        self, host: str, port: int, user: str, password: str, database: str
    ) -> None:
        self.params = MariaDBConnectorParams(
            host=host, port=port, user=user, password=password, database=database
        )

    def connect(self) -> None:
        self.connection = SQLConnect(**self.params.model_dump())

    def close(self) -> None:
        self.connection.close()

    def check_table_exists(self, table_name: str) -> bool:
        query = f"SHOW TABLES LIKE '{table_name}'"
        result = self.fetch_one(query)
        return result is not None

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        with MariaDBCursor(self.connection) as cursor:
            try:
                cursor.execute(query, data)
            except IntegrityError as e:
                raise MariaDBDuplicateKeyError(str(e))
            except Exception as e:
                raise e
        if any(key in query.upper() for key in AUTO_COMMIT_KEYS):
            self.commit()

    def execute_many(self, query: str, data: list[tuple[Any, ...]]) -> None:
        with MariaDBCursor(self.connection) as cursor:
            try:
                cursor.executemany(query, data)
            except IntegrityError as e:
                raise MariaDBDuplicateKeyError(str(e))
        if any(key in query.upper() for key in AUTO_COMMIT_KEYS):
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
