from abc import ABCMeta, abstractmethod

# from functools import partial

from mysql.connector import Error as MySQLError
from mysql.connector import connect as MySQLConnect

from .config_loader import ConfigLoader


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

    The class uses the Abstract Base Classes (ABCMeta) metaclass to enforce that subclasses implement the 'connect', 'close', 'execute', and 'fetch' methods.

    The constructor takes in the necessary parameters to establish a database connection, such as host, port, user, password, and database.

    The 'connect' and 'close' methods are declared as abstract methods, which means they must be implemented by any concrete (i.e., non-abstract) subclass.

    The 'execute' method is designed to execute a given SQL command. It takes a SQL query string and a tuple of data as parameters.

    The 'fetch' method is designed to execute a given SQL command and return the fetched results. It also takes a SQL query string and a tuple of data as parameters.
    """

    def __init__(
        self, host: str, port: str, user: str, password: str, database: str
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    def __enter__(self) -> "SQLConnector":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    @abstractmethod
    def execute(self, query: str, data: tuple = ()) -> None:
        pass

    @abstractmethod
    def fetch(self, query: str, data: tuple = ()) -> list:
        pass


class MySQLConnector(SQLConnector):
    """
    MySQLConnector is a concrete implementation of the SQLConnector abstract base class, designed specifically for MySQL database connections.

    This class calls the parent class constructor to set connection parameters during initialization and sets the connection attribute to None.

    The connect method establishes a connection to the MySQL database and stores this connection object in the connection attribute.

    The close method closes the established database connection.

    The execute method executes an SQL query and commits the result. This method requires an SQL query string and a tuple of data as parameters.

    The fetch method executes an SQL query and returns all results. This method requires an SQL query string and a tuple of data as parameters.
    """

    def __init__(
        self, host: str, port: str, user: str, password: str, database: str
    ) -> None:
        super().__init__(host, port, user, password, database)
        self.connection = None

    def connect(self) -> None:
        self.connection = MySQLConnect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )

    def close(self) -> bool:
        self.connection.close()

    def execute(self, query: str, data: tuple = ()) -> None:
        cursor = self.connection.cursor()
        cursor.execute(query, data)
        self.connection.commit()
        cursor.close()

    def fetch(self, query: str, data: tuple = ()) -> list:
        cursor = self.connection.cursor()
        cursor.execute(query, data)
        vlist = cursor.fetchall()
        cursor.close()
        return vlist


class DatabaseConfigurationError(Exception):
    """
    Custom exception class for database configuration errors.

    This class inherits from the built-in Python Exception class. You can add additional methods or attributes if needed.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class hentaiDB:
    def __init__(self):
        config_loader = ConfigLoader()
        self.sql_name = config_loader["database"]["sql_type"]
        self.sql_type = config_loader["database"]["sql_type"].lower()
        self.sql_connection_params = SQLConnectorParams(
            config_loader["database"]["host"],
            config_loader["database"]["port"],
            config_loader["database"]["user"],
            config_loader["database"]["password"],
            config_loader["database"]["database"] + "_0",
        )
        match self.sql_type:
            case "mysql":
                self.connector = MySQLConnector(**self.sql_connection_params)
            case _:
                raise ValueError("Unsupported SQL type")
        match self.sql_type:
            case "mysql":
                self.SQLError = MySQLError

    def check_database_character_set(self) -> None:
        with self.connector as conn:
            match self.sql_type:
                case "mysql":
                    charset = "utf8mb4"
                    is_charset_valid = (
                        conn.fetch("SHOW VARIABLES LIKE 'character_set_database';")[0][
                            1
                        ]
                        == charset
                    )
        if not is_charset_valid:
            raise DatabaseConfigurationError(
                f"Invalid database character set. Must be '{charset}' for {self.sql_name}"
            )

    def check_database_collation(self) -> None:
        with self.connector as conn:
            match self.sql_type:
                case "mysql":
                    collation = "utf8mb4_bin"
                    is_collation_valid = (
                        conn.fetch("SHOW VARIABLES LIKE 'collation_database';")[0][1]
                        == collation
                    )
        if not is_collation_valid:
            raise DatabaseConfigurationError(
                f"Invalid database collation. Must be '{collation}' for {self.sql_name}"
            )

    def __enter__(self):
        self.connector.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.connector.close()

    def execute(self, query: str, data: tuple = ()):
        self.connector.execute(query, data)

    def fetch(self, query: str, data: tuple = ()):
        return self.connector.fetch(query, data)
