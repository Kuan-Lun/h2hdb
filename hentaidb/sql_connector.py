__all__ = ["SQLConnectorParams", "MySQLConnector", "DatabaseConfigurationError", "DatabaseDuplicateKeyError"]


from abc import ABCMeta, abstractmethod


from .logger import logger

# from .config_loader import config_loader

# match config_loader["database"]["sql_type"].lower():
#     case "mysql":
#         from mysql.connector import Error as SQLError
#         from mysql.connector import connect as SQLConnect
#         from mysql.connector.errors import IntegrityError as SQLDuplicateKeyError


class SQLConnectorParams(dict):
    """
    SQLConnectorParams is a subclass of the built-in Python dictionary (dict) class. It is designed to store parameters for SQL database connections.

    The parameters include 'host', 'port', 'user', 'password', and 'database'. Each of these parameters is a string (str) that provides the necessary information to establish a connection to an SQL database.

    'host': The hostname or IP address of the database server.
    'port': The port number the database server is listening on.
    'user': The username to authenticate with the database.
    'password': The password to authenticate with the database.
    'database': The specific database to connect to on the server.

    By subclassing dict, SQLConnectorParams inherits all the methods of a dictionary, and can be used wherever a dictionary would be used. This includes indexing, iteration, and membership tests using 'in'.

    Additional methods or attributes can be added to this class if there are specific behaviors you want for your SQL connection parameters.
    """

    def __init__(
        self, host: str, port: str, user: str, password: str, database: str
    ) -> None:
        super().__init__(
            host=host, port=port, user=user, password=password, database=database
        )


class SQLConnector(metaclass=ABCMeta):
    """
    SQLConnector is an abstract base class that provides a standard interface for SQL database connections.
    It is designed to be subclassed by specific types of SQL database connectors (e.g., MySQLConnector, PostgreSQLConnector).

    The class uses the Abstract Base Classes (ABCMeta) metaclass to enforce that subclasses implement the 'connect', 'close', 'execute', 'fetch', 'execute_many', and 'commit' methods.

    The constructor takes in the necessary parameters to establish a database connection, such as host, port, user, password, and database.

    The 'connect', 'close', 'execute', 'execute_many', 'fetch_one', 'fetch_all', 'commit', and 'rollback' methods are abstract and must be implemented by concrete subclasses.

    The 'connect' method is designed to establish a connection to the database. It doesn't take any parameters.

    The 'close' method is designed to close the connection to the database. It doesn't take any parameters.

    The 'execute' method is designed to execute a single SQL command. It takes a SQL query string and a tuple of data as parameters.

    The 'execute_many' method is designed to execute multiple SQL commands. It takes a SQL query string and a list of tuples as parameters, where each tuple contains the data for one command.

    The 'fetch_one' method is designed to fetch a single result from the database. It takes a SQL query string and a tuple of data as parameters.

    The 'fetch_all' method is designed to fetch all results from the database. It takes a SQL query string and a tuple of data as parameters.

    The 'commit' method is designed to commit the current transaction to the database. It doesn't take any parameters.

    The 'rollback' method is designed to roll back the current transaction in the database. It doesn't take any parameters.
    """

    def __init__(
        self, host: str, port: str, user: str, password: str, database: str
    ) -> None:
        """
        Initializes a SQLConnector object.

        Args:
            host (str): The host name or IP address of the database server.
            port (str): The port number to connect to the database server.
            user (str): The username to authenticate with the database server.
            password (str): The password to authenticate with the database server.
            database (str): The name of the database to connect to.

        Returns:
            None
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    @abstractmethod
    def connect(self) -> None:
        """
        Connects to the SQL database.

        This method establishes a connection to the SQL database using the provided credentials.

        Returns:
            None
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Closes the SQL connector connection.

        This method closes the connection to the SQL database.

        Returns:
            None
        """
        pass

    def __enter__(self) -> "SQLConnector":
        """
        Establishes a connection to the SQL database.

        Returns:
            SQLConnector: The SQLConnector object itself.
        """
        self.connect()
        return self

    @abstractmethod
    def commit(self) -> None:
        """
        Commits the current transaction to the database.

        This method is used to save any changes made within the current transaction
        to the database. It ensures that all changes are permanently saved and can
        be accessed by other transactions.

        Returns:
            None
        """
        pass

    @abstractmethod
    def rollback(self) -> None:
        """
        Rolls back the current transaction in the database.

        This method is used to undo any changes made within the current transaction
        and return the database to its state before the transaction began.

        Returns:
            None
        """
        pass

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Performs necessary cleanup operations when exiting a context manager.

        Args:
            exc_type (type): The type of the exception raised, if any.
            exc_value (Exception): The exception raised, if any.
            traceback (traceback): The traceback object associated with the exception, if any.
        """
        self.commit()
        self.close()

    @abstractmethod
    def execute(self, query: str, data: tuple = ()) -> None:
        """
        Executes the given SQL query with optional data parameters.

        Args:
            query (str): The SQL query to execute.
            data (tuple, optional): The data parameters to be used in the query. Defaults to ().

        Returns:
            None
        """
        pass

    @abstractmethod
    def execute_many(self, query: str, data: list[tuple]) -> None:
        """
        Executes a SQL query multiple times with different sets of data.

        Args:
            query (str): The SQL query to execute.
            data (list[tuple]): A list of tuples, where each tuple represents a set of data to be used in the query.

        Returns:
            None
        """
        pass

    @abstractmethod
    def fetch_one(self, query: str, data: tuple = ()) -> tuple:
        """
        Executes the given SQL query and returns the first row of the result set.

        Args:
            query (str): The SQL query to execute.
            data (tuple, optional): The parameters to be passed to the query. Defaults to an empty tuple.

        Returns:
            tuple: The first row of the result set.

        """
        pass

    @abstractmethod
    def fetch_all(self, query: str, data: tuple = ()) -> list:
        """
        Executes the given SQL query and fetches all the rows from the result set.

        Args:
            query (str): The SQL query to be executed.
            data (tuple, optional): The parameters to be passed to the query. Defaults to ().

        Returns:
            list: A list of tuples representing the rows fetched from the result set.
        """
        pass


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


class DatabaseConfigurationError(Exception):
    """
    Custom exception class for database configuration errors.

    This class inherits from the built-in Python Exception class. You can add additional methods or attributes if needed.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class DatabaseDuplicateKeyError(Exception):
    """
    Custom exception class for database duplicate key errors.

    This class inherits from the built-in Python Exception class. You can add additional methods or attributes if needed.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
