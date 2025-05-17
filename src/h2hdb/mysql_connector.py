from mysql.connector import connect as SQLConnect
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection
from mysql.connector.errors import IntegrityError

from .sql_connector import SQLConnectorParams, SQLConnector, DatabaseDuplicateKeyError

AUTO_COMMIT_KEYS = ["INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER"]


class MySQLDuplicateKeyError(DatabaseDuplicateKeyError):
    """
    Custom exception class for MySQL duplicate key errors.

    This class inherits from the MySQL Connector/Python IntegrityError class.
    """

    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)


class MySQLConnectorParams(SQLConnectorParams):
    """
    MySQLConnectorParams is a data class that holds the connection parameters required to connect to a MySQL database.

    The class inherits from SQLConnectorParams and adds additional parameters specific to MySQL databases.

    The 'host' parameter is the host name or IP address of the MySQL database server.

    The 'port' parameter is the port number to connect to the MySQL database server.

    The 'user' parameter is the username to authenticate with the MySQL database server.

    The 'password' parameter is the password to authenticate with the MySQL database server.

    The 'database' parameter is the name of the MySQL database to connect to.
    """

    def __init__(
        self, host: str, port: str, user: str, password: str, database: str
    ) -> None:
        super().__init__(
            host=host, port=port, user=user, password=password, database=database
        )


class MySQLCursor:
    def __init__(
        self, connection: PooledMySQLConnection | MySQLConnectionAbstract
    ) -> None:
        self.connection = connection

    def __enter__(self):
        self.cursor = self.connection.cursor(buffered=True)
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()


class MySQLConnector(SQLConnector):
    """
    MySQLConnector is a concrete subclass of SQLConnector that provides an implementation for connecting to a MySQL database.

    The class uses the MySQL Connector/Python library to establish a connection to a MySQL database.

    The 'connect' method establishes a connection to the MySQL database using the provided connection parameters.

    The 'close' method closes the connection to the MySQL database.

    The 'execute' method executes a single SQL command on the MySQL database.

    The 'execute_many' method executes multiple SQL commands on the MySQL database.

    The 'fetch_one' method fetches a single result from the MySQL database.

    The 'fetch_all' method fetches all results from the MySQL database.

    The 'commit' method commits the current transaction to the MySQL database.

    The 'rollback' method rolls back the current transaction in the MySQL database.
    """

    def __init__(
        self, host: str, port: str, user: str, password: str, database: str
    ) -> None:
        self.params = MySQLConnectorParams(host, port, user, password, database)

    def connect(self) -> None:
        self.connection = SQLConnect(**self.params)

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

    def execute(self, query: str, data: tuple = ()) -> None:
        with MySQLCursor(self.connection) as cursor:
            try:
                cursor.execute(query, data)
            except IntegrityError as e:
                raise MySQLDuplicateKeyError(str(e))
            except Exception as e:
                raise e
        if any(key in query.upper() for key in AUTO_COMMIT_KEYS):
            self.commit()

    def execute_many(self, query: str, data: list[tuple]) -> None:
        with MySQLCursor(self.connection) as cursor:
            try:
                cursor.executemany(query, data)
            except IntegrityError as e:
                raise MySQLDuplicateKeyError(str(e))
        if any(key in query.upper() for key in AUTO_COMMIT_KEYS):
            self.commit()

    def fetch_one(self, query: str, data: tuple = ()) -> tuple:
        with MySQLCursor(self.connection) as cursor:
            cursor.execute(query, data)
            vlist = cursor.fetchone()
        return vlist  # type: ignore

    def fetch_all(self, query: str, data: tuple = ()) -> list:
        with MySQLCursor(self.connection) as cursor:
            cursor.execute(query, data)
            vlist = cursor.fetchall()
        return vlist
