from .logger import logger
from .sql_connector import SQLConnector


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
        logger.debug("Initializing MySQL connector...")
        super().__init__(host, port, user, password, database)
        logger.debug("MySQL connector initialized.")

    def connect(self) -> None:
        logger.debug("Establishing MySQL connection...")
        from mysql.connector import connect as SQLConnect

        self.connection = SQLConnect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        logger.debug("MySQL connection established.")
        self._cursor = self.connection.cursor()

    def close(self) -> None:
        logger.debug("Closing MySQL connection...")
        self._cursor.close()
        self.connection.close()
        logger.debug("MySQL connection closed.")

    def check_table_exists(self, table_name: str) -> bool:
        query = f"SHOW TABLES LIKE '{table_name}'"
        logger.debug(f"Executing MySQL query: {query}")
        result = self.fetch_one(query)
        return result is not None

    def commit(self) -> None:
        logger.debug("Committing MySQL transaction...")
        self.connection.commit()
        logger.debug("MySQL transaction committed.")

    def rollback(self) -> None:
        logger.debug("Rolling back MySQL transaction...")
        self.connection.rollback()
        logger.debug("MySQL transaction rolled back.")

    def execute(self, query: str, data: tuple = ()) -> None:
        logger.debug(f"Executing MySQL query: {query}")
        self._cursor.execute(query, data)

    def execute_many(self, query: str, data: list[tuple]) -> None:
        logger.debug(f"Executing multiple MySQL queries: {query}")
        self._cursor.executemany(query, data)

    def fetch_one(self, query: str, data: tuple = ()) -> tuple:
        logger.debug(f"Fetching result for MySQL query: {query}")
        self._cursor.execute(query, data)
        vlist = self._cursor.fetchone()
        return vlist  # type: ignore

    def fetch_all(self, query: str, data: tuple = ()) -> list:
        logger.debug(f"Fetching results for MySQL query: {query}")
        self._cursor.execute(query, data)
        vlist = self._cursor.fetchall()
        return vlist
