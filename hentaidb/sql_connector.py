__all__ = ["ComicDB"]


from abc import ABCMeta, abstractmethod
import re

# import logging

from mysql.connector import Error as MySQLError
from mysql.connector import connect as MySQLConnect
from mysql.connector.errors import IntegrityError as MySQLDuplicateKeyError

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

    The class uses the Abstract Base Classes (ABCMeta) metaclass to enforce that subclasses implement the 'connect', 'close', 'execute', 'fetch', 'execute_many', and 'commit' methods.

    The constructor takes in the necessary parameters to establish a database connection, such as host, port, user, password, and database.

    The 'connect', 'close', 'execute', 'execute_many', 'fetch_one', 'fetch_all', and 'commit' methods are abstract methods that must be implemented by subclasses.

    The 'connect' method is designed to establish a connection to the database. It doesn't take any parameters.

    The 'close' method is designed to close the connection to the database. It doesn't take any parameters.

    The 'execute' method is designed to execute a single SQL command. It takes a SQL query string and a tuple of data as parameters.

    The 'execute_many' method is designed to execute multiple SQL commands. It takes a SQL query string and a list of tuples as parameters, where each tuple contains the data for one command.

    The 'fetch_one' method is designed to fetch a single result from the database. It takes a SQL query string and a tuple of data as parameters.

    The 'fetch_all' method is designed to fetch all results from the database. It takes a SQL query string and a tuple of data as parameters.

    The 'commit' method is designed to commit the current transaction to the database. It doesn't take any parameters.
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

    def commit(self) -> None:
        logger.debug("Committing MySQL transaction...")
        self.connection.commit()
        logger.debug("MySQL transaction committed.")

    def execute(self, query: str, data: tuple = ()) -> None:
        logger.debug(f"Executing MySQL query: {query}")
        cursor = self.connection.cursor()
        cursor.execute(query, data)
        cursor.close()

    def execute_many(self, query: str, data: list[tuple]) -> None:
        logger.debug(f"Executing multiple MySQL queries: {query}")
        cursor = self.connection.cursor()
        cursor.executemany(query, data)
        self.connection.commit()
        cursor.close()

    def fetch_one(self, query: str, data: tuple = ()) -> tuple:
        logger.debug(f"Fetching result for MySQL query: {query}")
        cursor = self.connection.cursor()
        cursor.execute(query, data)
        vlist = cursor.fetchone()
        cursor.close()
        return vlist

    def fetch_all(self, query: str, data: tuple = ()) -> list:
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


class DatabaseDuplicateKeyError(Exception):
    """
    Custom exception class for database duplicate key errors.

    This class inherits from the built-in Python Exception class. You can add additional methods or attributes if needed.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


def sql_type_to_name(sql_type: str) -> str:
    match sql_type.lower():
        case "mysql":
            return "MySQL"


class ComicDB:
    def __init__(self) -> None:
        """
        Initializes the SQLConnector object.

        This method sets the SQL type, connection parameters, connector, and error class based on the configuration
        settings loaded from the config file.

        Raises:
            ValueError: If the SQL type is not supported.

        """
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
        """
        Checks the character set of the database and raises an error if it is invalid.

        Raises:
            DatabaseConfigurationError: If the database character set is invalid.
        """
        logger.debug("Checking database character set...")
        with self.connector as conn:
            match self.sql_type:
                case "mysql":
                    logger.debug("Checking database character set for MySQL...")
                    charset = "utf8mb4"
                    query = "SHOW VARIABLES LIKE 'character_set_database';"
            logger.debug(f"Database character set: {charset}")

            charset_result = conn.fetch_one(query)[1]
        is_charset_valid = charset_result == charset
        if not is_charset_valid:
            message = f"Invalid database character set. Must be '{charset}' for {sql_type_to_name(self.sql_type)} but is '{charset_result}'"
            logger.error(message)
            raise DatabaseConfigurationError(message)
        logger.info("Database character set is valid.")

    def check_database_collation(self) -> None:
        logger.debug("Checking database collation...")
        with self.connector as conn:
            match self.sql_type:
                case "mysql":
                    logger.debug("Checking database collation for MySQL...")
                    query = "SHOW VARIABLES LIKE 'collation_database';"
                    collation = "utf8mb4_bin"
            logger.debug(f"Database collation: {collation}")

            collation_result = conn.fetch_one(query)[1]
        is_collation_valid = collation_result == collation
        if not is_collation_valid:
            message = f"Invalid database collation. Must be '{collation}' for {sql_type_to_name(self.sql_type)} but is '{collation_result}'"
            logger.error(message)
            raise DatabaseConfigurationError(message)
        logger.info("Database character set and collation are valid.")

    def __enter__(self) -> "ComicDB":
        self.connector.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.connector.close()

    def create_gallery_name_id_table(self) -> None:
        table_name = "Gallery_DBID"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        Name_Part1 CHAR(191) NOT NULL,
                        Name_Part2 CHAR(64) NOT NULL,
                        DBID INT UNSIGNED AUTO_INCREMENT,
                        UNIQUE Full_Name (Name_Part1, Name_Part2),
                        PRIMARY KEY (DBID),
                        INDEX Name_Part2 (Name_Part2)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        with self.connector as conn:
            conn.execute(query)
        logger.info(f"{table_name} table created.")

    def create_gallery_name_view(self) -> None:
        table_name = "Gallery_Name"
        logger.debug(f"Creating {table_name} view...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE VIEW IF NOT EXISTS {table_name} AS
                    SELECT CONCAT(Name_Part1, Name_Part2) AS Name, DBID
                    FROM Gallery_DBID
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        with self.connector as conn:
            conn.execute(query)
        logger.info(f"{table_name} view created.")

    def insert_gallery_name_and_return_id(self, gallery_name: str) -> int:
        logger.debug(f"Inserting gallery name '{gallery_name}'...")
        table_name = "Gallery_DBID"
        if len(gallery_name) > 255:
            logger.error(
                f"Gallery name '{gallery_name}' is too long. Must be 255 characters or less."
            )
            raise ValueError("Gallery name is too long.")
        gallery_name_part1 = gallery_name[0:191]
        gallery_name_part2 = gallery_name[191:255]
        logger.debug(
            f"Gallery name '{gallery_name}' split into parts '{gallery_name_part1}' and '{gallery_name_part2}'"
        )

        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (Name_Part1, Name_Part2) VALUES (%s, %s)
                """
                select_query = f"""
                    SELECT DBID FROM {table_name} WHERE Name_Part1 = %s AND Name_Part2 = %s
                """
                DuplicateKeyError = MySQLDuplicateKeyError
        insert_query, select_query = (
            mullines2oneline(query) for query in (insert_query, select_query)
        )
        data = (gallery_name_part1, gallery_name_part2)

        with self.connector as conn:
            logger.debug(f"Insert query: {insert_query}")
            try:
                conn.execute(insert_query, data)
                conn.commit()
                logger.debug(f"Gallery name '{gallery_name}' inserted.")
            except DuplicateKeyError:
                logger.warning(
                    f"Gallery name '{gallery_name}' already exists. Retrieving ID..."
                )
            logger.debug(f"Select query: {select_query}")
            gallery_name_id = conn.fetch_one(select_query, data)[0]
        logger.info(
            f"Gallery name '{gallery_name}' inserted with ID {gallery_name_id}."
        )
        return gallery_name_id

    def insert_gallery_names_and_return_ids(
        self, gallery_names: list[str]
    ) -> dict[str, int]:
        logger.debug(f"Inserting gallery names '{gallery_names}'...")
        gallery_name_ids = dict()
        for gallery_name in gallery_names:
            gallery_name_id = self.insert_gallery_name_and_return_id(gallery_name)
            gallery_name_ids[gallery_name] = gallery_name_id
        return gallery_name_ids

    def create_gid_table(self) -> None:
        table_name = "GID"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        DBID INT UNSIGNED NOT NULL,
                        GID INT UNSIGNED NOT NULL,
                        FOREIGN KEY (DBID) REFERENCES Gallery_DBID(DBID),
                        INDEX (GID, DBID)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        with self.connector as conn:
            conn.execute(query)
        logger.info(f"{table_name} table created.")

    def insert_gid(self, gallery_name_id: int, gid: int) -> None:
        table_name = "GID"
        logger.debug(
            f"Inserting {table_name} {gid} for gallery name ID {gallery_name_id}..."
        )
        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (DBID, GID) VALUES (%s, %s)
                """
                select_query = f"""
                    SELECT GID FROM {table_name} WHERE DBID = %s AND GID = %s
                """
                DuplicateKeyError = MySQLDuplicateKeyError
        insert_query, select_query = (
            mullines2oneline(query) for query in (insert_query, select_query)
        )
        data = (gallery_name_id, gid)

        with self.connector as conn:
            logger.debug(f"Insert query: {insert_query}")
            try:
                conn.execute(insert_query, data)
                logger.debug(
                    f"GID {gid} inserted for gallery name ID {gallery_name_id}."
                )
            except DuplicateKeyError:
                logger.warning(
                    f"GID {gid} for gallery name ID {gallery_name_id} already exists."
                )
        logger.info(f"GID {gid} inserted for gallery name ID {gallery_name_id}.")

    def _create_time_table(self, table_name: str) -> None:
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        DBID INT UNSIGNED NOT NULL,
                        Time DATETIME NOT NULL,
                        FOREIGN KEY (DBID) REFERENCES Gallery_DBID(DBID),
                        INDEX (Time, DBID)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        with self.connector as conn:
            conn.execute(query)
        logger.info(f"{table_name} table created.")

    def _insert_time(self, table_name: str, gallery_name_id: int, time: str) -> None:
        logger.debug(
            f"Inserting time '{time}' for gallery name ID {gallery_name_id} into table '{table_name}'..."
        )
        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (DBID, Time) VALUES (%s, %s)
                """
                DuplicateKeyError = MySQLDuplicateKeyError
        insert_query = mullines2oneline(insert_query)
        data = (gallery_name_id, time)

        with self.connector as conn:
            logger.debug(f"Insert query: {insert_query}")
            try:
                conn.execute(insert_query, data)
                conn.commit()
                logger.debug(
                    f"Time '{time}' inserted for gallery name ID {gallery_name_id}."
                )
            except DuplicateKeyError:
                logger.warning(
                    f"Time '{time}' for gallery name ID {gallery_name_id} already exists."
                )
        logger.info(f"Time '{time}' inserted for gallery name ID {gallery_name_id}.")

    def create_download_time_table(self) -> None:
        self._create_time_table("Download_Time")

    def insert_download_time(self, gallery_name_id: int, time: str) -> None:
        self._insert_time("Download_Time", gallery_name_id, time)

    def create_upload_time_table(self) -> None:
        self._create_time_table("Upload_Time")

    def insert_upload_time(self, gallery_name_id: int, time: str) -> None:
        self._insert_time("Upload_Time", gallery_name_id, time)

    def create_modified_time_table(self) -> None:
        self._create_time_table("Modified_Time")

    def insert_modified_time(self, gallery_name_id: int, time: str) -> None:
        self._insert_time("Modified_Time", gallery_name_id, time)

    def create_access_time_table(self) -> None:
        self._create_time_table("Access_Time")

    def insert_access_time(self, gallery_name_id: int, time: str) -> None:
        self._insert_time("Access_Time", gallery_name_id, time)

    def create_gallery_info_view(self) -> None:
        logger.debug("Creating Gallery_Info view...")
        match self.sql_type:
            case "mysql":
                query = """
                    CREATE VIEW IF NOT EXISTS Gallery_Info AS
                    SELECT
                        Gallery_Name.DBID AS DBID,
                        Gallery_Name.Name AS Name,
                        GID.GID AS GID,
                        Download_Time.Time AS Download_Time,
                        Upload_Time.Time AS Upload_Time,
                        Modified_Time.Time AS Modified_Time,
                        Access_Time.Time AS Access_Time
                    FROM
                        Gallery_Name
                        LEFT JOIN GID USING (DBID)
                        LEFT JOIN Download_Time USING (DBID)
                        LEFT JOIN Upload_Time USING (DBID)
                        LEFT JOIN Modified_Time USING (DBID)
                        LEFT JOIN Access_Time USING (DBID)
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        with self.connector as conn:
            conn.execute(query)
        logger.info("Gallery_Info view created.")


def mullines2oneline(s: str) -> str:
    """
    Replaces multiple spaces with a single space, and replaces newlines with a space.

    Args:
        s (str): The input string.

    Returns:
        str: The modified string with multiple spaces replaced by a single space and newlines replaced by a space.
    """
    return re.sub(" +", " ", s).replace("\n", " ").strip()
