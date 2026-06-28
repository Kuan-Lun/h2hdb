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
    """
    Custom exception class for MariaDB duplicate key errors.

    This class inherits from the MySQL Connector/Python IntegrityError class.
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class MariaDBConnectorParams(SQLConnectorParams):
    """
    MariaDBConnectorParams is a data class that holds the connection parameters required to connect to a MariaDB database.

    The class inherits from SQLConnectorParams and adds additional parameters specific to MariaDB databases.

    The 'host' parameter is the host name or IP address of the MariaDB database server.

    The 'port' parameter is the port number to connect to the MariaDB database server.

    The 'user' parameter is the username to authenticate with the MariaDB database server.

    The 'password' parameter is the password to authenticate with the MariaDB database server.

    The 'database' parameter is the name of the MariaDB database to connect to.
    """

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
    """
    MariaDBConnector is a concrete subclass of SQLConnector that provides an implementation for connecting to a MariaDB database.

    The class uses the MySQL Connector/Python library (wire-protocol compatible with MariaDB) to establish a connection.

    The 'connect' method establishes a connection to the MariaDB database using the provided connection parameters.

    The 'close' method closes the connection to the MariaDB database.

    The 'execute' method executes a single SQL command on the MariaDB database.

    The 'execute_many' method executes multiple SQL commands on the MariaDB database.

    The 'fetch_one' method fetches a single result from the MariaDB database.

    The 'fetch_all' method fetches all results from the MariaDB database.

    The 'commit' method commits the current transaction to the MariaDB database.

    The 'rollback' method rolls back the current transaction in the MariaDB database.
    """

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
