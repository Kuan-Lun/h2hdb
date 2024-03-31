__all__ = ["HentaiDB"]


from abc import ABCMeta, abstractmethod
import re

# import logging

from mysql.connector import Error as MySQLError
from mysql.connector import connect as MySQLConnect

from .logger import logger
from .config_loader import config_loader


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
        logger.debug("Initializing MySQL connector...")
        super().__init__(host, port, user, password, database)
        self.connection = None
        logger.debug("MySQL connector initialized.")

    def connect(self) -> None:
        logger.debug("Establishing MySQL connection...")
        self.connection = MySQLConnect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        logger.debug("MySQL connection established.")

    def close(self) -> bool:
        logger.debug("Closing MySQL connection...")
        self.connection.close()
        logger.debug("MySQL connection closed.")

    def execute(self, query: str, data: tuple = ()) -> None:
        logger.debug(f"Executing MySQL query: {query}")
        cursor = self.connection.cursor()
        cursor.execute(query, data)
        self.connection.commit()
        cursor.close()

    def fetch(self, query: str, data: tuple = ()) -> list:
        logger.debug(f"Fetching results for MySQL query: {query}")
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


def sql_type_to_name(sql_type: str) -> str:
    match sql_type.lower():
        case "mysql":
            return "MySQL"


class HentaiDB:
    def __init__(self) -> None:
        self.sql_type = config_loader["database"]["sql_type"].lower()
        self.sql_connection_params = SQLConnectorParams(
            config_loader["database"]["host"],
            config_loader["database"]["port"],
            config_loader["database"]["user"],
            config_loader["database"]["password"],
            config_loader["database"]["database"],
        )

        # Set the appropriate connector based on the SQL type
        logger.debug("Setting connector...")
        match self.sql_type:
            case "mysql":
                logger.debug("Setting MySQL connector...")
                self.connector = MySQLConnector(**self.sql_connection_params)
            case _:
                raise ValueError("Unsupported SQL type")
        logger.debug("Connector set.")

        # Set the appropriate error class based on the SQL type
        logger.debug("Setting error class...")
        match self.sql_type:
            case "mysql":
                logger.debug("Setting MySQL error class...")
                self.SQLError = MySQLError
        logger.debug("Error class set.")

    def check_database_character_set(self) -> None:
        logger.debug("Checking database character set...")
        with self.connector as conn:
            match self.sql_type:
                case "mysql":
                    logger.debug("Checking database character set for MySQL...")
                    charset = "utf8mb4"
                    is_charset_valid = (
                        conn.fetch("SHOW VARIABLES LIKE 'character_set_database';")[0][
                            1
                        ]
                        == charset
                    )

        logger.debug(f"Database character set: {charset}")
        if not is_charset_valid:
            message = f"Invalid database character set. Must be '{charset}' for {sql_type_to_name(self.sql_type)}"
            logger.error(message)
            raise DatabaseConfigurationError(message)
        logger.info("Database character set is valid.")

    def check_database_collation(self) -> None:
        logger.debug("Checking database collation...")
        with self.connector as conn:
            match self.sql_type:
                case "mysql":
                    logger.debug("Checking database collation for MySQL...")
                    collation = "utf8mb4_bin"
                    is_collation_valid = (
                        conn.fetch("SHOW VARIABLES LIKE 'collation_database';")[0][1]
                        == collation
                    )
        if not is_collation_valid:
            message = f"Invalid database collation. Must be '{collation}' for {sql_type_to_name(self.sql_type)}"
            logger.error(message)
            raise DatabaseConfigurationError(message)
        logger.info("Database character set and collation are valid.")

    def __enter__(self) -> "HentaiDB":
        self.connector.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.connector.close()

    def execute(self, query: str, data: tuple = ()) -> None:
        logger.info(f"Executing SQL query: {query}")
        self.connector.execute(query, data)

    def fetch(self, query: str, data: tuple = ()) -> list:
        logger.info(f"Fetching results for SQL query: {query}")
        return self.connector.fetch(query, data)

    def create_gallery_name_id_table(self) -> None:
        logger.debug("Creating GalleryNameID table...")
        match self.sql_type:
            case "mysql":
                query = """
                    CREATE TABLE IF NOT EXISTS GalleryNameID (
                        GalleryName CHAR(255) NOT NULL,
                        GalleryNameID INT UNSIGNED AUTO_INCREMENT,
                        PRIMARY KEY (GalleryNameID)
                        )
                    """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        with self.connector as conn:
            conn.execute(query)
        logger.info("GalleryNameID table created.")

    def create_gid_table(self) -> None:
        logger.debug("Creating GID table...")
        match self.sql_type:
            case "mysql":
                query = """
                    CREATE TABLE IF NOT EXISTS GID (
                        GalleryNameID INT UNSIGNED,
                        GID INT UNSIGNED NOT NULL,
                        FOREIGN KEY (GalleryNameID) REFERENCES GalleryNameID(GalleryNameID),
                        INDEX (GID, GalleryNameID)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        with self.connector as conn:
            conn.execute(query)
        logger.info("GID table created.")


def mullines2oneline(s: str) -> str:
    return re.sub(" +", " ", s).replace("\n", " ").strip()
