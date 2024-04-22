__all__ = [
    "SQLConnectorParams",
    "MySQLConnector",
    "DatabaseConfigurationError",
    "DatabaseKeyError",
    "DatabaseTableError",
]


from abc import ABCMeta, abstractmethod


class DatabaseConfigurationError(Exception):
    """
    Custom exception class for database configuration errors.

    This class inherits from the built-in Python Exception class. You can add additional methods or attributes if needed.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class DatabaseKeyError(Exception):
    """
    Custom exception class for database key errors.

    This class inherits from the built-in Python Exception class. You can add additional methods or attributes if needed.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class DatabaseTableError(Exception):
    """
    Custom exception class for database table errors.

    This class inherits from the built-in Python Exception class. You can add additional methods or attributes if needed.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class SQLConnectorParams(dict):
    pass


class SQLConnector(metaclass=ABCMeta):
    """
    SQLConnector is an abstract base class that provides a standard interface for SQL database connections.
    It is designed to be subclassed by specific types of SQL database connectors (e.g., MySQLConnector, PostgreSQLConnector).

    The class uses the Abstract Base Classes (ABCMeta) metaclass to enforce that subclasses implement the 'connect', 'close', 'execute', 'fetch', 'execute_many', and 'commit' methods.

    The constructor takes in the necessary parameters to establish a database connection, such as host, port, user, password, and database.

    The 'connect', 'close', 'check_table_exists', 'execute', 'execute_many', 'fetch_one', 'fetch_all', 'commit', and 'rollback' methods are abstract and must be implemented by concrete subclasses.

    The 'connect' method is designed to establish a connection to the database. It doesn't take any parameters.

    The 'close' method is designed to close the connection to the database. It doesn't take any parameters.

    The 'check_table_exists' method is designed to check if a table exists in the database. It takes the name of the table as a parameter and returns a boolean value.

    The 'execute' method is designed to execute a single SQL command. It takes a SQL query string and a tuple of data as parameters.

    The 'execute_many' method is designed to execute multiple SQL commands. It takes a SQL query string and a list of tuples as parameters, where each tuple contains the data for one command.

    The 'fetch_one' method is designed to fetch a single result from the database. It takes a SQL query string and a tuple of data as parameters.

    The 'fetch_all' method is designed to fetch all results from the database. It takes a SQL query string and a tuple of data as parameters.

    The 'commit' method is designed to commit the current transaction to the database. It doesn't take any parameters.

    The 'rollback' method is designed to roll back the current transaction in the database. It doesn't take any parameters.
    """

    @abstractmethod
    def __init__(self) -> None:
        pass

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
    def check_table_exists(self, table_name: str) -> bool:
        """
        Checks if a table exists in the database.

        Args:
            table_name (str): The name of the table to check for existence.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        pass

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
