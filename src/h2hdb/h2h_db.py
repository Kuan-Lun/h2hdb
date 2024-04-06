__all__ = ["H2HDB"]


import re
import hashlib
import os
from abc import ABCMeta, abstractmethod
import math

from .gallery_info_parser import parse_gallery_info
from .config_loader import config_loader
from .logger import logger
from .sql_connector import (
    MySQLConnector,
    SQLConnectorParams,
    DatabaseConfigurationError,
    DatabaseKeyError,
)
from .settings import FOLDER_NAME_LENGTH_LIMIT, FILE_NAME_LENGTH_LIMIT

match config_loader.database.sql_type.lower():
    case "mysql":
        from mysql.connector import Error as SQLError

        INNODB_INDEX_PREFIX_LIMIT = 191


def split_gallery_name(gallery_name: str) -> list[str]:
    size = FOLDER_NAME_LENGTH_LIMIT // INNODB_INDEX_PREFIX_LIMIT + (
        FOLDER_NAME_LENGTH_LIMIT % INNODB_INDEX_PREFIX_LIMIT > 0
    )
    gallery_name_parts = re.findall(f".{{1,{INNODB_INDEX_PREFIX_LIMIT}}}", gallery_name)
    gallery_name_parts += [""] * (size - len(gallery_name_parts))
    return gallery_name_parts


def sql_type_to_name(sql_type: str) -> str:
    match sql_type.lower():
        case "mysql":
            name = "MySQL"
    return name


class H2HDBAbstract(metaclass=ABCMeta):
    """
    A class representing the initialization of an SQL connector for the comic database.

    This class is an abstract base class (ABC) and should not be instantiated directly.
    Subclasses must implement the abstract methods defined in this class.

    Attributes:
        sql_type (str): The type of SQL database.
        sql_connection_params (SQLConnectorParams): The parameters for establishing the SQL connection.
        connector (SQLConnector): The SQL connector object.
        SQLError (Exception): The error class for the SQL type.

    Abstract Methods:
        check_database_character_set: Checks the character set of the database.
        check_database_collation: Checks the collation of the database.
        create_main_tables: Creates the main tables for the comic database.
        insert_gallery_info: Inserts the gallery information into the database.
        select_gallery_gid: Selects the gallery GID from the database.
        select_gallery_title: Selects the gallery title from the database.
        update_access_time: Updates the access time for the gallery in the database.
        select_gallery_upload_account: Selects the gallery upload account from the database.
        select_gallery_comment: Selects the gallery comment from the database.
        insert_gallery_tag: Inserts the gallery tag into the database.
        select_gallery_tag: Selects the gallery tag from the database.
        select_gallery_file: Selects the gallery files from the database.
        delete_gallery_image: Deletes the gallery image from the database.
        delete_gallery: Deletes the gallery from the database.

    Methods:
        __init__: Initializes the H2HDBAbstract object.
        __enter__: Establishes the SQL connection and starts a transaction.
        __exit__: Commits or rolls back the transaction and closes the SQL connection.
    """

    __slots__ = [
        "sql_type",
        "sql_connection_params",
        "connector",
        "SQLError",
    ]

    def __init__(self) -> None:
        """
        Initializes the H2HDBAbstract object.

        Raises:
            ValueError: If the SQL type is unsupported.
        """
        self.sql_type = config_loader.database.sql_type.lower()
        self.sql_connection_params = SQLConnectorParams(
            config_loader.database.host,
            config_loader.database.port,
            config_loader.database.user,
            config_loader.database.password,
            config_loader.database.database,
        )

        # Set the appropriate connector based on the SQL type
        logger.debug("Setting connector...")
        match self.sql_type:
            case "mysql":
                logger.debug("Setting MySQL connector...")
                self.connector = MySQLConnector(**self.sql_connection_params)
                logger.debug("Setting MySQL error class...")
                self.SQLError = SQLError
            case _:
                raise ValueError("Unsupported SQL type")
        logger.debug("Connector set.")

    def __enter__(self) -> "H2HDBAbstract":
        """
        Establishes the SQL connection and starts a transaction.

        Returns:
            H2HDBAbstract: The initialized H2HDBAbstract object.
        """
        self.connector.connect()
        match self.sql_type:
            case "mysql":
                self.connector.execute("START TRANSACTION")
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Commits or rolls back the transaction and closes the SQL connection.

        Args:
            exc_type (type): The type of the exception raised, if any.
            exc_value (Exception): The exception raised, if any.
            traceback (traceback): The traceback information of the exception, if any.
        """
        if exc_type is None:
            self.connector.commit()
        else:
            self.connector.rollback()
        self.connector.close()

    @abstractmethod
    def check_database_character_set(self) -> None:
        """
        Checks the character set of the database.
        """
        pass

    @abstractmethod
    def check_database_collation(self) -> None:
        """
        Checks the collation of the database.
        """
        pass

    @abstractmethod
    def create_main_tables(self) -> None:
        """
        Creates the main tables for the comic database.
        """
        pass

    @abstractmethod
    def insert_gallery_info(self, gallery_path: str) -> None:
        """
        Inserts the gallery information into the database.

        Args:
            gallery_path (str): The path to the gallery folder.
        """
        pass

    @abstractmethod
    def select_gallery_gid(self, gallery_name: str) -> int:
        """
        Selects the gallery GID from the database.

        Args:
            gallery_name (str): The name of the gallery.

        Returns:
            int: The gallery GID.
        """
        pass

    @abstractmethod
    def select_gallery_title(self, gallery_name: str) -> str:
        """
        Selects the gallery title from the database.

        Args:
            gallery_name (str): The name of the gallery.

        Returns:
            str: The gallery title.
        """
        pass

    @abstractmethod
    def update_access_time(self, gallery_name: str, time: str) -> None:
        """
        Updates the access time for the gallery in the database.

        Args:
            gallery_name_id (int): The ID of the gallery.
            time (str): The access time.
        """
        pass

    @abstractmethod
    def select_gallery_upload_account(self, gallery_name: str) -> str:
        """
        Selects the gallery upload account from the database.

        Args:
            gallery_name (str): The name of the gallery.

        Returns:
            str: The gallery upload account.
        """
        pass

    @abstractmethod
    def select_gallery_comment(self, gallery_name: str) -> str:
        """
        Selects the gallery comment from the database.

        Args:
            gallery_name (str): The name of the gallery.

        Returns:
            str: The gallery comment.
        """
        pass

    @abstractmethod
    def insert_gallery_tag(
        self, gallery_name: str, tag_name: str, tag_value: str
    ) -> None:
        """
        Inserts the gallery tag into the database.

        Args:
            gallery_name (str): The name of the gallery.
            tag_name (str): The name of the tag.
            tag_value (str): The value of the tag.
        """
        pass

    @abstractmethod
    def select_gallery_tag(self, gallery_name: str, tag_name: str) -> str:
        """
        Selects the gallery tag from the database.

        Args:
            gallery_name (str): The name of the gallery.
            tag_name (str): The name of the tag.

        Returns:
            str: The value of the tag.
        """
        pass

    @abstractmethod
    def select_gallery_file(self, gallery_name: str) -> list[str]:
        """
        Selects the gallery files from the database.

        Args:
            gallery_name (str): The name of the gallery.

        Returns:
            list[str]: The list of files in the gallery.
        """
        pass

    @abstractmethod
    def delete_gallery_image(self, gallery_name: str) -> None:
        """
        Deletes the gallery image from the database.

        Args:
            gallery_name (str): The name of the gallery.
        """
        pass

    @abstractmethod
    def delete_gallery(self, gallery_name: str) -> None:
        """
        Deletes the gallery from the database.

        Args:
            gallery_name (str): The name of the gallery.
        """
        pass


class H2HDBCheckDatabaseSettings(H2HDBAbstract, metaclass=ABCMeta):
    """
    A class that checks the database settings for character set and collation.

    This class inherits from `H2HDBAbstract` and is used to ensure that the database
    character set and collation are valid. It provides methods to check the character set and
    collation of the database and raises an error if they are invalid.

    Attributes:
        sql_type (str): The type of SQL database being used.

    Methods:
        check_database_character_set: Checks the character set of the database.
        check_database_collation: Checks the collation of the database.
    """

    def check_database_character_set(self) -> None:
        """
        Checks the character set of the database and raises an error if it is invalid.

        Raises:
            DatabaseConfigurationError: If the database character set is invalid.
        """
        logger.debug("Checking database character set...")
        match self.sql_type:
            case "mysql":
                logger.debug("Checking database character set for MySQL...")
                charset = "utf8mb4"
                query = "SHOW VARIABLES LIKE 'character_set_database';"
        logger.debug(f"Database character set: {charset}")

        charset_result = self.connector.fetch_one(query)[1]
        is_charset_valid = charset_result == charset
        if not is_charset_valid:
            message = f"Invalid database character set. Must be '{charset}' for {sql_type_to_name(self.sql_type)} but is '{charset_result}'"
            logger.error(message)
            raise DatabaseConfigurationError(message)
        logger.info("Database character set is valid.")

    def check_database_collation(self) -> None:
        """
        Checks the collation of the database and raises an error if it is invalid.

        Raises:
            DatabaseConfigurationError: If the database collation is invalid.
        """
        logger.debug("Checking database collation...")
        match self.sql_type:
            case "mysql":
                logger.debug("Checking database collation for MySQL...")
                query = "SHOW VARIABLES LIKE 'collation_database';"
                collation = "utf8mb4_bin"
        logger.debug(f"Database collation: {collation}")

        collation_result = self.connector.fetch_one(query)[1]
        is_collation_valid = collation_result == collation
        if not is_collation_valid:
            message = f"Invalid database collation. Must be '{collation}' for {sql_type_to_name(self.sql_type)} but is '{collation_result}'"
            logger.error(message)
            raise DatabaseConfigurationError(message)
        logger.info("Database character set and collation are valid.")


def mysql_split_name_based_on_limit(
    name: str, name_length_limit: int
) -> tuple[list[str], str]:
    num_parts = math.ceil(name_length_limit / INNODB_INDEX_PREFIX_LIMIT)
    name_parts = [
        f"{name}_part{i} CHAR({INNODB_INDEX_PREFIX_LIMIT}) NOT NULL"
        for i in range(1, name_length_limit // INNODB_INDEX_PREFIX_LIMIT + 1)
    ]
    if name_length_limit % INNODB_INDEX_PREFIX_LIMIT > 0:
        name_parts.append(
            f"{name}_part{num_parts} CHAR({name_length_limit % INNODB_INDEX_PREFIX_LIMIT}) NOT NULL"
        )
    create_name_parts_sql = ", ".join(name_parts)
    column_name_parts = [f"{name}_part{i}" for i in range(1, num_parts + 1)]
    return column_name_parts, create_name_parts_sql


def mysql_split_gallery_name_based_on_limit(name: str) -> tuple[list[str], str]:
    return mysql_split_name_based_on_limit(name, FOLDER_NAME_LENGTH_LIMIT)


def mysql_split_file_name_based_on_limit(name: str) -> tuple[list[str], str]:
    return mysql_split_name_based_on_limit(name, FILE_NAME_LENGTH_LIMIT)


class ComaicDBDBGalleriesIDs(H2HDBAbstract, metaclass=ABCMeta):
    def _create_galleries_names_table(self) -> None:
        table_name = "galleries_names"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                column_name = "name"
                column_name_parts, create_gallery_name_parts_sql = (
                    mysql_split_gallery_name_based_on_limit(column_name)
                )
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        PRIMARY KEY (db_gallery_id),
                        db_gallery_id INT  UNSIGNED AUTO_INCREMENT,
                        {create_gallery_name_parts_sql},
                        UNIQUE real_primay_key ({", ".join(column_name_parts)}),
                        full_name     TEXT          NOT NULL,
                        FULLTEXT (full_name)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} table created.")

    def _insert_gallery_name_and_return_db_gallery_id(self, gallery_name: str) -> int:
        logger.debug(f"Inserting gallery name '{gallery_name}'...")
        table_name = "galleries_names"
        if len(gallery_name) > FOLDER_NAME_LENGTH_LIMIT:
            logger.error(
                f"Gallery name '{gallery_name}' is too long. Must be {FOLDER_NAME_LENGTH_LIMIT} characters or less."
            )
            raise ValueError("Gallery name is too long.")
        gallery_name_parts = split_gallery_name(gallery_name)
        logger.debug(
            f"Gallery name '{gallery_name}' split into parts  %s"
            % " and ".join(
                [
                    "'" + gallery_name_part + "'"
                    for gallery_name_part in gallery_name_parts
                ]
            )
        )

        match self.sql_type:
            case "mysql":
                column_name_parts, _ = mysql_split_gallery_name_based_on_limit("name")
                insert_query = f"""
                    INSERT INTO {table_name}
                        ({", ".join(column_name_parts)}, full_name)
                    VALUES ({", ".join(["%s" for _ in column_name_parts])}, %s)
                """
        insert_query = mullines2oneline(insert_query)

        try:
            gallery_name_id = self._select_gallery_name_id(gallery_name)
            logger.warning(
                f"Gallery name '{gallery_name}' already exists. Returning ID..."
            )
        except DatabaseKeyError:
            # If gallery name does not exist, insert it
            logger.debug(f"Insert query: {insert_query}")
            self.connector.execute(
                insert_query, (*tuple(gallery_name_parts), gallery_name)
            )
            logger.debug(f"Gallery name '{gallery_name}' inserted.")
            gallery_name_id = self._select_gallery_name_id(gallery_name)

        return gallery_name_id

    def _insert_galleries_names_and_return_db_gallery_id(
        self, gallery_names: list[str]
    ) -> dict[str, int]:
        logger.debug(f"Inserting gallery names '{gallery_names}'...")
        gallery_name_ids = dict()
        for gallery_name in gallery_names:
            gallery_name_id = self._insert_gallery_name_and_return_db_gallery_id(
                gallery_name
            )
            gallery_name_ids[gallery_name] = gallery_name_id
        return gallery_name_ids

    def _select_gallery_name_id(self, gallery_name: str) -> int:
        logger.debug(f"Selecting gallery name ID for gallery name '{gallery_name}'...")
        table_name = "galleries_names"
        gallery_name_parts = split_gallery_name(gallery_name)
        logger.debug(
            f"Gallery name '{gallery_name}' split into parts  %s"
            % " and ".join(
                [
                    "'" + gallery_name_part + "'"
                    for gallery_name_part in gallery_name_parts
                ]
            )
        )

        match self.sql_type:
            case "mysql":
                column_name_parts, _ = mysql_split_gallery_name_based_on_limit("name")
                select_query = f"""
                    SELECT db_gallery_id
                      FROM {table_name}
                     WHERE {" AND ".join([f"{part} = %s" for part in column_name_parts])}
                """
        select_query = mullines2oneline(select_query)

        logger.debug(f"Select query: {select_query}")
        query_result = self.connector.fetch_one(select_query, tuple(gallery_name_parts))
        if query_result is None:
            logger.debug(f"Gallery name '{gallery_name}' does not exist.")
            raise DatabaseKeyError(f"Gallery name '{gallery_name}' does not exist.")
        else:
            gallery_name_id = query_result[0]
            logger.info(
                f"Gallery name ID for gallery name '{gallery_name}' is {gallery_name_id}."
            )
        return gallery_name_id


class ComaicDBGalleriesGIDs(ComaicDBDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    """
    A class that handles the GIDs for galleries in the comic database.

    This class inherits from `H2HDBAbstract` and is used to manage the GIDs for galleries

    Attributes:
        sql_type (str): The type of SQL database being used.
        sql_connection_params (SQLConnectorParams): The parameters for establishing the SQL connection.
        connector (SQLConnector): The SQL connector object.
        SQLError (Exception): The error class for the SQL type.

    Methods:
        _create_galleries_gids_table: Creates the galleries_gids table.
        _insert_galleries_gids: Inserts the GID for the gallery name ID into the galleries_gids table.
        _select_gallery_gid: Selects the GID for the gallery name ID from the galleries_gids table.
        select_gallery_gid: Selects the GID for the gallery name from the database.
    """

    def _create_galleries_gids_table(self) -> None:
        table_name = "galleries_gids"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        PRIMARY KEY (db_gallery_id),
                        FOREIGN KEY (db_gallery_id) REFERENCES galleries_names(db_gallery_id),
                        db_gallery_id INT UNSIGNED NOT NULL,
                        gid           INT UNSIGNED NOT NULL,
                        INDEX (gid, db_gallery_id)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} table created.")

    def _insert_galleries_gids(self, gallery_name_id: int, gid: int) -> None:
        table_name = "galleries_gids"
        logger.debug(
            f"Inserting {table_name} {gid} for gallery name ID {gallery_name_id}..."
        )
        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (db_gallery_id, gid) VALUES (%s, %s)
                """
        insert_query = mullines2oneline(insert_query)
        data = (gallery_name_id, gid)

        logger.debug(f"Insert query: {insert_query}")
        try:
            self._select_gallery_gid(gallery_name_id)
            logger.warning(
                f"GID for gallery name ID {gallery_name_id} already exists. Updating..."
            )
        except DatabaseKeyError:
            self.connector.execute(insert_query, data)
            logger.debug(
                f"{table_name} {gid} inserted for gallery name ID {gallery_name_id}."
            )

    def _select_gallery_gid(self, gallery_name_id: int) -> int:
        logger.debug(f"Selecting GID for gallery name ID {gallery_name_id}...")
        table_name = "galleries_gids"
        match self.sql_type:
            case "mysql":
                select_query = f"""
                    SELECT gid
                      FROM {table_name}
                     WHERE db_gallery_id = %s
                """
        select_query = mullines2oneline(select_query)
        data = (gallery_name_id,)

        logger.debug(f"Select query: {select_query}")
        query_result = self.connector.fetch_one(select_query, data)
        if query_result is None:
            msg = f"GID for gallery name ID {gallery_name_id} does not exist."
            logger.error(msg)
            raise DatabaseKeyError(msg)
        else:
            gid = query_result[0]
            logger.info(f"GID for gallery name ID {gallery_name_id} is {gid}.")
        return gid

    def select_gallery_gid(self, gallery_name: str) -> int:
        gallery_name_id = self._select_gallery_name_id(gallery_name)
        return self._select_gallery_gid(gallery_name_id)


class ComaicDBTimes(ComaicDBDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_times_table(self, table_name: str) -> None:
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        PRIMARY KEY (db_gallery_id),
                        FOREIGN KEY (db_gallery_id) REFERENCES galleries_names(db_gallery_id),
                        db_gallery_id INT UNSIGNED NOT NULL,
                        time          DATETIME     NOT NULL,
                        INDEX (time, db_gallery_id)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} table created.")

    def _insert_time(self, table_name: str, gallery_name_id: int, time: str) -> None:
        logger.debug(
            f"Inserting time '{time}' for gallery name ID {gallery_name_id} into table '{table_name}'..."
        )
        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (db_gallery_id, time) VALUES (%s, %s)
                """
        insert_query = mullines2oneline(insert_query)
        data = (gallery_name_id, time)

        try:
            self._select_time(table_name, gallery_name_id)
        except DatabaseKeyError:
            logger.debug(f"Insert query: {insert_query}")
            self.connector.execute(insert_query, data)
            logger.debug(
                f"Time '{time}' inserted for gallery name ID {gallery_name_id}."
            )
        logger.info(f"Time '{time}' inserted for gallery name ID {gallery_name_id}.")

    def _select_time(self, table_name: str, gallery_name_id: int) -> str:
        logger.debug(
            f"Selecting time for gallery name ID {gallery_name_id} from table '{table_name}'..."
        )
        match self.sql_type:
            case "mysql":
                select_query = f"""
                    SELECT time
                      FROM {table_name}
                     WHERE db_gallery_id = %s
                """
        select_query = mullines2oneline(select_query)
        data = (gallery_name_id,)

        logger.debug(f"Select query: {select_query}")
        query_result = self.connector.fetch_one(select_query, data)
        if query_result is None:
            msg = f"Time for gallery name ID {gallery_name_id} does not exist in table '{table_name}'."
            logger.error(msg)
            raise DatabaseKeyError(msg)
        else:
            time = query_result[0]
            logger.info(
                f"Time for gallery name ID {gallery_name_id} in table '{table_name}' is '{time}'."
            )
        return time

    def _update_time(self, table_name: str, gallery_name_id: int, time: str) -> None:
        logger.debug(
            f"Updating time '{time}' for gallery name ID {gallery_name_id} in table '{table_name}'..."
        )
        match self.sql_type:
            case "mysql":
                update_query = f"""
                    UPDATE {table_name} SET time = %s WHERE db_gallery_id = %s
                """
        update_query = mullines2oneline(update_query)
        data = (time, gallery_name_id)

        logger.debug(f"Update query: {update_query}")
        self.connector.execute(update_query, data)
        logger.info(
            f"Time '{time}' updated for gallery name ID {gallery_name_id} in table '{table_name}'."
        )

    def _create_galleries_download_times_table(self) -> None:
        self._create_times_table("galleries_download_times")

    def _insert_download_time(self, gallery_name_id: int, time: str) -> None:
        self._insert_time("galleries_download_times", gallery_name_id, time)

    def _create_galleries_upload_times_table(self) -> None:
        self._create_times_table("galleries_upload_times")

    def _insert_upload_time(self, gallery_name_id: int, time: str) -> None:
        self._insert_time("galleries_upload_times", gallery_name_id, time)

    def _create_galleries_modified_times_table(self) -> None:
        self._create_times_table("galleries_modified_times")

    def _insert_modified_time(self, gallery_name_id: int, time: str) -> None:
        self._insert_time("galleries_modified_times", gallery_name_id, time)

    def _create_galleries_access_times_table(self) -> None:
        self._create_times_table("galleries_access_times")

    def _insert_access_time(self, gallery_name_id: int, time: str) -> None:
        self._insert_time("galleries_access_times", gallery_name_id, time)

    def update_access_time(self, gallery_name: str, time: str) -> None:
        gallery_name_id = self._select_gallery_name_id(gallery_name)
        self._update_time("galleries_access_times", gallery_name_id, time)


class H2HDBGalleriesTitles(ComaicDBDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_galleries_titles_table(self) -> None:
        table_name = "galleries_titles"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        PRIMARY KEY (db_gallery_id),
                        FOREIGN KEY (db_gallery_id) REFERENCES galleries_names(db_gallery_id),
                        db_gallery_id INT UNSIGNED NOT NULL,
                        title         TEXT         NOT NULL,
                        FULLTEXT (title)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} table created.")

    def _insert_gallery_title(self, gallery_name_id: int, title: str) -> None:
        logger.debug(
            f"Inserting title '{title}' for gallery name ID {gallery_name_id}..."
        )
        table_name = "galleries_titles"
        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (db_gallery_id, title) VALUES (%s, %s)
                """
        insert_query = mullines2oneline(insert_query)
        data = (gallery_name_id, title)

        try:
            self._select_gallery_title(gallery_name_id)
            logger.warning(
                f"Title for gallery name ID {gallery_name_id} already exists. Updating..."
            )
        except DatabaseKeyError:
            logger.debug(f"Insert query: {insert_query}")
            self.connector.execute(insert_query, data)
            logger.debug(
                f"Title '{title}' inserted for gallery name ID {gallery_name_id}."
            )
        logger.info(f"Title '{title}' inserted for gallery name ID {gallery_name_id}.")

    def _select_gallery_title(self, gallery_name_id: int) -> str:
        logger.debug(f"Selecting title for gallery name ID {gallery_name_id}...")
        table_name = "galleries_titles"
        match self.sql_type:
            case "mysql":
                select_query = f"""
                    SELECT title
                      FROM {table_name}
                     WHERE db_gallery_id = %s
                """
        select_query = mullines2oneline(select_query)
        data = (gallery_name_id,)

        logger.debug(f"Select query: {select_query}")
        query_result = self.connector.fetch_one(select_query, data)
        if query_result is None:
            msg = f"Title for gallery name ID {gallery_name_id} does not exist."
            logger.error(msg)
            raise DatabaseKeyError(msg)
        else:
            title = query_result[0]
            logger.info(f"Title for gallery name ID {gallery_name_id} is '{title}'.")
        return title

    def select_gallery_title(self, gallery_name: str) -> str:
        gallery_name_id = self._select_gallery_name_id(gallery_name)
        return self._select_gallery_title(gallery_name_id)


class H2HDBUploadAccounts(ComaicDBDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_upload_account_table(self) -> None:
        table_name = "galleries_upload_accounts"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        PRIMARY KEY (db_gallery_id),
                        FOREIGN KEY (db_gallery_id) REFERENCES galleries_names(db_gallery_id),
                        db_gallery_id INT UNSIGNED                      NOT NULL,
                        account       CHAR({INNODB_INDEX_PREFIX_LIMIT}) NOT NULL,
                        INDEX (account, db_gallery_id)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} table created.")

    def _insert_gallery_upload_account(
        self, gallery_name_id: int, account: str
    ) -> None:
        logger.debug(
            f"Inserting upload account '{account}' for gallery name ID {gallery_name_id}..."
        )
        table_name = "galleries_upload_accounts"
        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (db_gallery_id, account) VALUES (%s, %s)
                """
        insert_query = mullines2oneline(insert_query)
        data = (gallery_name_id, account)

        try:
            self._select_gallery_upload_account(gallery_name_id)
            logger.warning(
                f"Upload account for gallery name ID {gallery_name_id} already exists. Updating..."
            )
        except DatabaseKeyError:
            logger.debug(f"Insert query: {insert_query}")
            self.connector.execute(insert_query, data)
            logger.debug(
                f"Upload account '{account}' inserted for gallery name ID {gallery_name_id}."
            )
        logger.info(
            f"Upload account '{account}' inserted for gallery name ID {gallery_name_id}."
        )

    def _select_gallery_upload_account(self, gallery_name_id: int) -> str:
        logger.debug(
            f"Selecting upload account for gallery name ID {gallery_name_id}..."
        )
        table_name = "galleries_upload_accounts"
        match self.sql_type:
            case "mysql":
                select_query = f"""
                    SELECT account
                      FROM {table_name}
                     WHERE db_gallery_id = %s
                """
        select_query = mullines2oneline(select_query)
        data = (gallery_name_id,)

        logger.debug(f"Select query: {select_query}")
        query_result = self.connector.fetch_one(select_query, data)
        if query_result is None:
            msg = (
                f"Upload account for gallery name ID {gallery_name_id} does not exist."
            )
            logger.error(msg)
            raise DatabaseKeyError(msg)
        else:
            account = query_result[0]
            logger.info(
                f"Upload account for gallery name ID {gallery_name_id} is '{account}'."
            )
        return account

    def select_gallery_upload_account(self, gallery_name: str) -> str:
        gallery_name_id = self._select_gallery_name_id(gallery_name)
        return self._select_gallery_upload_account(gallery_name_id)


class H2HDBGalleriesInfos(
    H2HDBGalleriesTitles,
    H2HDBUploadAccounts,
    ComaicDBTimes,
    ComaicDBGalleriesGIDs,
    ComaicDBDBGalleriesIDs,
    H2HDBCheckDatabaseSettings,
):
    def _create_galleries_infos_view(self) -> None:
        logger.debug("Creating galleries_infos view...")
        match self.sql_type:
            case "mysql":
                query = """
                    CREATE VIEW IF NOT EXISTS galleries_infos AS
                    SELECT galleries_names.db_gallery_id AS db_gallery_id,
                           galleries_names.full_name AS name,
                           galleries_titles.title AS title,
                           galleries_gids.gid AS gid,
                           galleries_upload_accounts.account AS upload_account,
                           galleries_upload_times.time AS upload_time,
                           galleries_download_times.time AS download_time,
                           galleries_modified_times.time AS modified_time,
                           galleries_access_times.time AS access_time
                      FROM galleries_names
                           LEFT JOIN galleries_titles USING (db_gallery_id)
                           LEFT JOIN galleries_gids USING (db_gallery_id)
                           LEFT JOIN galleries_upload_accounts USING (db_gallery_id)
                           LEFT JOIN galleries_upload_times USING (db_gallery_id)
                           LEFT JOIN galleries_download_times USING (db_gallery_id)
                           LEFT JOIN galleries_modified_times USING (db_gallery_id)
                           LEFT JOIN galleries_access_times USING (db_gallery_id)
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info("galleries_infos view created.")


class H2HDBGalleriesComments(ComaicDBDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_galleries_comments_table(self) -> None:
        table_name = "galleries_comments"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        PRIMARY KEY (db_gallery_id),
                        FOREIGN KEY (db_gallery_id) REFERENCES galleries_names(db_gallery_id),
                        db_gallery_id INT UNSIGNED NOT NULL,
                        comment       TEXT         NOT NULL,
                        FULLTEXT (Comment)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} table created.")

    def _insert_gallery_comment(self, gallery_name_id: int, comment: str) -> None:
        logger.debug(
            f"Inserting uploader comment '{comment}' for gallery name ID {gallery_name_id}..."
        )
        table_name = "galleries_comments"
        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (db_gallery_id, comment) VALUES (%s, %s)
                """
        insert_query = mullines2oneline(insert_query)
        data = (gallery_name_id, comment)

        try:
            self._select_gallery_comment(gallery_name_id)
            logger.warning(
                f"Uploader comment for gallery name ID {gallery_name_id} already exists. Updating..."
            )
        except DatabaseKeyError:
            logger.debug(f"Insert query: {insert_query}")
            self.connector.execute(insert_query, data)
            logger.debug(
                f"Uploader comment '{comment}' inserted for gallery name ID {gallery_name_id}."
            )
        logger.info(
            f"Uploader comment '{comment}' inserted for gallery name ID {gallery_name_id}."
        )

    def _update_gallery_comment(self, gallery_name_id: int, comment: str) -> None:
        logger.debug(
            f"Updating uploader comment for gallery name ID {gallery_name_id}..."
        )
        table_name = "galleries_comments"
        match self.sql_type:
            case "mysql":
                update_query = f"""
                    UPDATE {table_name} SET Comment = %s WHERE db_gallery_id = %s
                """
        update_query = mullines2oneline(update_query)
        data = (comment, gallery_name_id)

        logger.debug(f"Update query: {update_query}")
        self.connector.execute(update_query, data)
        logger.info(f"Uploader comment updated for gallery name ID {gallery_name_id}.")

    def _select_gallery_comment(self, gallery_name_id: int) -> str:
        logger.debug(
            f"Selecting uploader comment for gallery name ID {gallery_name_id}..."
        )
        table_name = "galleries_comments"
        match self.sql_type:
            case "mysql":
                select_query = f"""
                    SELECT Comment
                      FROM {table_name}
                     WHERE db_gallery_id = %s
                """
        select_query = mullines2oneline(select_query)
        data = (gallery_name_id,)

        logger.debug(f"Select query: {select_query}")
        query_result = self.connector.fetch_one(select_query, data)
        if query_result is None:
            msg = f"Uploader comment for gallery name ID {gallery_name_id} does not exist."
            logger.error(msg)
            raise DatabaseKeyError(msg)
        else:
            comment = query_result[0]
            logger.info(
                f"Uploader comment for gallery name ID {gallery_name_id} is '{comment}'."
            )
        return comment

    def select_gallery_comment(self, gallery_name: str) -> str:
        gallery_name_id = self._select_gallery_name_id(gallery_name)
        return self._select_gallery_comment(gallery_name_id)


class H2HDBGalleriesTags(ComaicDBDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_galleries_tags_table(self, tag_name: str) -> None:
        table_name = f"galleries_tags_{tag_name}"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        PRIMARY KEY (db_gallery_id),
                        FOREIGN KEY (db_gallery_id) REFERENCES galleries_names(db_gallery_id),
                        db_gallery_id INT UNSIGNED                      NOT NULL,
                        tag           CHAR({INNODB_INDEX_PREFIX_LIMIT}) NOT NULL,
                        INDEX (tag, db_gallery_id)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} table created.")

    def _insert_gallery_tag(
        self, gallery_name_id: int, tag_name: str, tag_value: str
    ) -> None:
        logger.debug(
            f"Inserting tag '{tag_name}' with value '{tag_value}' for gallery name ID {gallery_name_id}..."
        )
        table_name = f"galleries_tags_{tag_name}"
        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (db_gallery_id, tag) VALUES (%s, %s)
                """
        insert_query = mullines2oneline(insert_query)
        data = (gallery_name_id, tag_value)

        try:
            self._select_gallery_tag(gallery_name_id, tag_name)
            logger.warning(
                f"Tag '{tag_name}' for gallery name ID {gallery_name_id} already exists. Updating..."
            )
        except (DatabaseKeyError, Exception) as e:
            if re.search(r"Table '[\w.]+' doesn't exist", str(e)):
                self._create_galleries_tags_table(tag_name)
                logger.warning(
                    f"Table '{table_name}' does not exist. Creating table..."
                )
            logger.debug(
                f"Insert query: {insert_query} with data {data} for table '{table_name}'"
            )
            self.connector.execute(insert_query, data)
            logger.debug(
                f"Tag '{tag_name}' with value '{tag_value}' inserted for gallery name ID {gallery_name_id}."
            )
        logger.info(
            f"Tag '{tag_name}' with value '{tag_value}' inserted for gallery name ID {gallery_name_id}."
        )

    def insert_gallery_tag(
        self, gallery_name: str, tag_name: str, tag_value: str
    ) -> None:
        gallery_name_id = self._select_gallery_name_id(gallery_name)
        self._insert_gallery_tag(gallery_name_id, tag_name, tag_value)

    def _select_gallery_tag(self, gallery_name_id: int, tag_name: str) -> str:
        logger.debug(f"Selecting tag '{tag_name}'...")
        table_name = f"galleries_tags_{tag_name}"
        match self.sql_type:
            case "mysql":
                select_query = f"""
                    SELECT tag
                      FROM {table_name}
                     WHERE db_gallery_id = %s
                """
        select_query = mullines2oneline(select_query)
        data = (gallery_name_id,)

        logger.debug(f"Select query: {select_query}")
        query_result = self.connector.fetch_one(select_query, data)
        if query_result is None:
            msg = f"Tag '{tag_name}' does not exist."
            logger.error(msg)
            raise DatabaseKeyError(msg)
        else:
            tag = query_result[0]
            logger.info(f"Tag '{tag_name}' is '{tag}'.")
        return tag

    def select_gallery_tag(self, gallery_name: str, tag_name: str) -> str:
        gallery_name_id = self._select_gallery_name_id(gallery_name)
        return self._select_gallery_tag(gallery_name_id, tag_name)


class H2HDBFiles(ComaicDBDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_files_names_table(self) -> None:
        table_name = f"files_names"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                column_name = "name"
                column_name_parts, create_gallery_name_parts_sql = (
                    mysql_split_file_name_based_on_limit(column_name)
                )
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        PRIMARY KEY (db_file_id),
                        db_file_id    INT UNSIGNED AUTO_INCREMENT,
                        db_gallery_id INT UNSIGNED NOT NULL,
                        FOREIGN KEY (db_gallery_id) REFERENCES galleries_names(db_gallery_id),
                        {create_gallery_name_parts_sql},
                        UNIQUE real_primay_key (db_gallery_id, {", ".join(column_name_parts)}),
                        full_name     TEXT         NOT NULL,
                        FULLTEXT (full_name)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} table created.")

    def _insert_gallery_file_and_return_id(
        self, gallery_name_id: int, file_name: str
    ) -> int:
        logger.debug(
            f"Inserting file '{file_name}' for gallery name ID {gallery_name_id}..."
        )
        table_name = "files_names"
        if len(file_name) > FILE_NAME_LENGTH_LIMIT:
            logger.error(
                f"File name '{file_name}' is too long. Must be {FILE_NAME_LENGTH_LIMIT} characters or less."
            )
            raise ValueError("File name is too long.")
        file_name_parts = split_gallery_name(file_name)
        logger.debug(
            f"File name '{file_name}' split into parts  %s"
            % " and ".join(
                ["'" + file_name_part + "'" for file_name_part in file_name_parts]
            )
        )

        match self.sql_type:
            case "mysql":
                column_name_parts, _ = mysql_split_file_name_based_on_limit("name")
                insert_query = f"""
                    INSERT INTO {table_name}
                        (db_gallery_id, {", ".join(column_name_parts)}, full_name)
                    VALUES (%s, {", ".join(["%s" for _ in column_name_parts])}, %s)
                """
        insert_query = mullines2oneline(insert_query)
        data = (gallery_name_id, *file_name_parts, file_name)

        try:
            gallery_image_id = self._select_gallery_file_id(gallery_name_id, file_name)
            logger.warning(
                f"File '{file_name}' for gallery name ID {gallery_name_id} already exists. Returning ID..."
            )
        except DatabaseKeyError:
            # If gallery name does not exist, insert it
            logger.debug(f"Insert query: {insert_query}")
            self.connector.execute(insert_query, data)
            logger.debug(f"File '{file_name}' inserted.")
            gallery_image_id = self._select_gallery_file_id(gallery_name_id, file_name)

        return gallery_image_id

    def _select_gallery_file_id(self, gallery_name_id: int, file_name: str) -> int:
        logger.debug(
            f"Selecting image ID for gallery name ID {gallery_name_id} and file '{file_name}'..."
        )
        table_name = "files_names"
        file_name_parts = split_gallery_name(file_name)
        match self.sql_type:
            case "mysql":
                column_name_parts, _ = mysql_split_file_name_based_on_limit("name")
                select_query = f"""
                    SELECT db_file_id
                      FROM {table_name}
                     WHERE db_gallery_id = %s
                       AND {" AND ".join([f"{part} = %s" for part in column_name_parts])}
                """
        select_query = mullines2oneline(select_query)
        data = (gallery_name_id, *file_name_parts)

        logger.debug(f"Select query: {select_query}")
        query_result = self.connector.fetch_one(select_query, data)
        if query_result is None:
            msg = f"Image ID for gallery name ID {gallery_name_id} and file '{file_name}' does not exist."
            logger.error(msg)
            raise DatabaseKeyError(msg)
        else:
            gallery_image_id = query_result[0]
            logger.info(
                f"Image ID for gallery name ID {gallery_name_id} and file '{file_name}' is {gallery_image_id}."
            )
        return gallery_image_id

    def select_gallery_file(self, gallery_name: str) -> list[str]:
        gallery_name_id = self._select_gallery_name_id(gallery_name)
        logger.debug(f"Selecting files for gallery name ID {gallery_name_id}...")
        table_name = "files_names"
        match self.sql_type:
            case "mysql":
                select_query = f"""
                    SELECT full_name
                        FROM {table_name}
                          WHERE db_gallery_id = %s
                """
        select_query = mullines2oneline(select_query)
        data = (gallery_name_id,)
        logger.debug(f"Select query: {select_query}")
        query_result = self.connector.fetch_all(select_query, data)
        if query_result is None:
            msg = f"Files for gallery name ID {gallery_name_id} do not exist."
            logger.error(msg)
            raise DatabaseKeyError(msg)
        else:
            files = [query[0] for query in query_result]
            logger.info(f"Files for gallery name ID {gallery_name_id} are {files}.")
        return files

    def _create_galleries_files_hashs_table(
        self, algorithm: str, output_len: int
    ) -> None:
        table_name = "files_hashs_%s" % algorithm.lower()
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        PRIMARY KEY (db_file_id),
                        FOREIGN KEY (db_file_id) REFERENCES files_names(db_file_id),
                        db_file_id INT UNSIGNED       NOT NULL,
                        hash_value CHAR({output_len}) NOT NULL,
                        INDEX (hash_value, db_file_id)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} table created.")

    def _create_galleries_files_sha224_table(self) -> None:
        self._create_galleries_files_hashs_table("sha224", output_len=56)

    def _create_galleries_files_sha256_table(self) -> None:
        self._create_galleries_files_hashs_table("sha256", output_len=64)

    def _create_galleries_files_sha384_table(self) -> None:
        self._create_galleries_files_hashs_table("sha384", output_len=96)

    def _create_galleries_files_sha1_table(self) -> None:
        self._create_galleries_files_hashs_table("sha1", output_len=40)

    def _create_galleries_files_sha512_table(self) -> None:
        self._create_galleries_files_hashs_table("sha512", output_len=128)

    def _create_galleries_files_sha3_224_table(self) -> None:
        self._create_galleries_files_hashs_table("sha3_224", output_len=56)

    def _create_galleries_files_sha3_256_table(self) -> None:
        self._create_galleries_files_hashs_table("sha3_256", output_len=64)

    def _create_galleries_files_sha3_384_table(self) -> None:
        self._create_galleries_files_hashs_table("sha3_384", output_len=96)

    def _create_galleries_files_sha3_512_table(self) -> None:
        self._create_galleries_files_hashs_table("sha3_512", output_len=128)

    def _create_galleries_files_blake2b_table(self) -> None:
        self._create_galleries_files_hashs_table("blake2b", output_len=128)

    def _create_galleries_files_blake2s_table(self) -> None:
        self._create_galleries_files_hashs_table("blake2s", output_len=64)

    def _create_galleries_files_hashs_tables(self) -> None:
        logger.debug("Creating gallery image hash tables...")
        self._create_galleries_files_sha224_table()
        self._create_galleries_files_sha256_table()
        self._create_galleries_files_sha384_table()
        self._create_galleries_files_sha1_table()
        self._create_galleries_files_sha512_table()
        self._create_galleries_files_sha3_224_table()
        self._create_galleries_files_sha3_256_table()
        self._create_galleries_files_sha3_384_table()
        self._create_galleries_files_sha3_512_table()
        self._create_galleries_files_blake2b_table()
        self._create_galleries_files_blake2s_table()
        logger.info("Gallery image hash tables created.")

    def _create_gallery_image_hash_view(self) -> None:
        table_name = "files_hashs"
        logger.debug(f"Creating {table_name} view...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE VIEW IF NOT EXISTS {table_name} AS
                    SELECT files_names.db_file_id          AS db_file_id,
                           galleries_titles.title          AS gallery_title,
                           galleries_names.full_name       AS gallery_name,
                           files_names.full_name           AS file_name,
                           files_hashs_sha224.hash_value   AS sha224,
                           files_hashs_sha256.hash_value   AS sha256,
                           files_hashs_sha384.hash_value   AS sha384,
                           files_hashs_sha1.hash_value     AS sha1,
                           files_hashs_sha512.hash_value   AS sha512,
                           files_hashs_sha3_224.hash_value AS sha3_224,
                           files_hashs_sha3_256.hash_value AS sha3_256,
                           files_hashs_sha3_384.hash_value AS sha3_384,
                           files_hashs_sha3_512.hash_value AS sha3_512,
                           files_hashs_blake2b.hash_value  AS blake2b,
                           files_hashs_blake2s.hash_value  AS blake2s
                      FROM files_names
                           LEFT JOIN galleries_titles     USING (db_gallery_id)
                           LEFT JOIN galleries_names      USING (db_gallery_id)
                           LEFT JOIN files_hashs_sha224   USING (db_file_id)
                           LEFT JOIN files_hashs_sha256   USING (db_file_id)
                           LEFT JOIN files_hashs_sha384   USING (db_file_id)
                           LEFT JOIN files_hashs_sha1     USING (db_file_id)
                           LEFT JOIN files_hashs_sha512   USING (db_file_id)
                           LEFT JOIN files_hashs_sha3_224 USING (db_file_id)
                           LEFT JOIN files_hashs_sha3_256 USING (db_file_id)
                           LEFT JOIN files_hashs_sha3_384 USING (db_file_id)
                           LEFT JOIN files_hashs_sha3_512 USING (db_file_id)
                           LEFT JOIN files_hashs_blake2b  USING (db_file_id)
                           LEFT JOIN files_hashs_blake2s  USING (db_file_id)
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} view created.")

    def _insert_gallery_file_hash(
        self, file_id: int, file_content: bytes, algorithm: str
    ) -> None:
        logger.debug(f"Inserting image hash for image ID {file_id}...")
        table_name = f"files_hashs_{algorithm.lower()}"
        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (db_file_id, hash_value) VALUES (%s, %s)
                """
        insert_query = mullines2oneline(insert_query)
        hash_function = lambda x: getattr(hashlib, algorithm.lower())(x).hexdigest()
        hash_value = hash_function(file_content)
        data = (file_id, hash_value)

        try:
            original_hash_value = self._select_gallery_file_hash(file_id, algorithm)
            logger.warning(
                f"Image hash for image ID {file_id} already exists. Updating..."
            )
            if original_hash_value != hash_value:
                logger.warning(
                    f"Original hash value '{original_hash_value}' is different from new hash value '{hash_value}'."
                )
                self._update_gallery_file_hash(file_id, data[1], algorithm)
        except DatabaseKeyError:
            logger.debug(f"Insert query: {insert_query}")
            self.connector.execute(insert_query, data)
            logger.debug(f"Image hash inserted for image ID {file_id}.")
        logger.info(f"Image hash inserted for image ID {file_id}.")

    def _select_gallery_file_hash(self, file_id: int, algorithm: str) -> str:
        logger.debug(f"Selecting image hash for image ID {file_id}...")
        table_name = f"files_hashs_{algorithm.lower()}"
        match self.sql_type:
            case "mysql":
                select_query = f"""
                    SELECT hash_value
                      FROM {table_name}
                     WHERE db_file_id = %s
                """
        select_query = mullines2oneline(select_query)
        data = (file_id,)

        logger.debug(f"Select query: {select_query}")
        query_result = self.connector.fetch_one(select_query, data)
        if query_result is None:
            msg = f"Image hash for image ID {file_id} does not exist."
            logger.error(msg)
            raise DatabaseKeyError(msg)
        else:
            hash_value = query_result[0]
            logger.info(f"Image hash for image ID {file_id} is '{hash_value}'.")
        return hash_value

    def _insert_gallery_file_sha224(self, file_id: int, file_content: bytes) -> None:
        self._insert_gallery_file_hash(file_id, file_content, "sha224")

    def _insert_gallery_file_sha256(self, file_id: int, file_content: bytes) -> None:
        self._insert_gallery_file_hash(file_id, file_content, "sha256")

    def _insert_gallery_file_sha384(self, file_id: int, file_content: bytes) -> None:
        self._insert_gallery_file_hash(file_id, file_content, "sha384")

    def _insert_gallery_file_sha1(self, file_id: int, file_content: bytes) -> None:
        self._insert_gallery_file_hash(file_id, file_content, "sha1")

    def _insert_gallery_file_sha512(self, file_id: int, file_content: bytes) -> None:
        self._insert_gallery_file_hash(file_id, file_content, "sha512")

    def _insert_gallery_file_sha3_224(self, file_id: int, file_content: bytes) -> None:
        self._insert_gallery_file_hash(file_id, file_content, "sha3_224")

    def _insert_gallery_file_sha3_256(self, file_id: int, file_content: bytes) -> None:
        self._insert_gallery_file_hash(file_id, file_content, "sha3_256")

    def _insert_gallery_file_sha3_384(self, file_id: int, file_content: bytes) -> None:
        self._insert_gallery_file_hash(file_id, file_content, "sha3_384")

    def _insert_gallery_file_sha3_512(self, file_id: int, file_content: bytes) -> None:
        self._insert_gallery_file_hash(file_id, file_content, "sha3_512")

    def _insert_gallery_file_blake2b(self, file_id: int, file_content: bytes) -> None:
        self._insert_gallery_file_hash(file_id, file_content, "blake2b")

    def _insert_gallery_file_blake2s(self, file_id: int, file_content: bytes) -> None:
        self._insert_gallery_file_hash(file_id, file_content, "blake2s")

    def _update_gallery_file_hash(
        self, file_id: int, hash_value: str, algorithm: str
    ) -> None:
        logger.debug(f"Updating image hash for image ID {file_id}...")
        table_name = f"files_hashs_{algorithm.lower()}"
        match self.sql_type:
            case "mysql":
                update_query = f"""
                    UPDATE {table_name} SET hash_value = %s WHERE db_file_id = %s
                """
        update_query = mullines2oneline(update_query)
        data = (hash_value, file_id)

        logger.debug(f"Update query: {update_query}")
        self.connector.execute(update_query, data)
        logger.info(f"Image hash updated for image ID {file_id}.")


class H2HDBRemovedGalleries(ComaicDBDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_removed_galleries_gids_table(self) -> None:
        table_name = "removed_galleries_gids"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        PRIMARY KEY (gid),
                        gid INT UNSIGNED NOT NULL
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} table created.")

    def insert_removed_gallery_gid(self, gid: int) -> None:
        logger.debug(f"Inserting removed gallery GID {gid}...")
        table_name = "removed_galleries_gids"
        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (gid) VALUES (%s)
                """
        insert_query = mullines2oneline(insert_query)
        data = (gid,)

        try:
            self.select_removed_gallery_gid(gid)
            logger.warning(f"Removed gallery GID {gid} already exists.")
        except DatabaseKeyError:
            logger.debug(f"Insert query: {insert_query}")
            self.connector.execute(insert_query, data)
            logger.debug(f"Removed gallery GID {gid} inserted.")
        logger.info(f"Removed gallery GID {gid} inserted.")

    def select_removed_gallery_gid(self, gid: int) -> int:
        logger.debug(f"Selecting removed gallery GID {gid}...")
        table_name = "removed_galleries_gids"
        match self.sql_type:
            case "mysql":
                select_query = f"""
                    SELECT gid
                      FROM {table_name}
                     WHERE gid = %s
                """
        select_query = mullines2oneline(select_query)
        data = (gid,)

        logger.debug(f"Select query: {select_query}")
        query_result = self.connector.fetch_one(select_query, data)
        if query_result is None:
            msg = f"Removed gallery GID {gid} does not exist."
            logger.error(msg)
            raise DatabaseKeyError(msg)
        else:
            gid = query_result[0]
            logger.info(f"Removed gallery GID {gid} exists.")
        return gid


class H2HDB(
    H2HDBGalleriesInfos,
    H2HDBGalleriesComments,
    H2HDBGalleriesTags,
    H2HDBFiles,
    H2HDBRemovedGalleries,
):
    def delete_gallery_image(self, gallery_name: str) -> None:
        logger.debug(f"Deleting gallery '{gallery_name}'...")
        match self.sql_type:
            case "mysql":
                select_table_name_query = f"""
                    SELECT TABLE_NAME
                      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                     WHERE REFERENCED_TABLE_SCHEMA = '{config_loader.database.database}'
                       AND REFERENCED_TABLE_NAME = 'files_names'
                       AND REFERENCED_COLUMN_NAME = 'db_file_id'
                """
                column_name_parts, _ = mysql_split_gallery_name_based_on_limit("name")
                delete_image_id_query = f"""
                    DELETE FROM %s
                    WHERE
                        db_file_id IN (
                            SELECT db_file_id
                            FROM files_names
                            WHERE db_gallery_id = (
                                SELECT db_gallery_id
                                FROM galleries_names
                                WHERE {" AND ".join([f"{part} = '%s'" for part in column_name_parts])}
                            )
                        )
                """
        select_table_name_query, delete_image_id_query = (
            mullines2oneline(query)
            for query in (select_table_name_query, delete_image_id_query)
        )

        logger.debug(f"Select query: {select_table_name_query}")
        table_names = self.connector.fetch_all(select_table_name_query)
        table_names = [t[0] for t in table_names] + ["files_names"]
        logger.debug(f"Table names: {table_names}")

        logger.debug(f"Delete query: {delete_image_id_query}")
        gallery_name_parts = split_gallery_name(gallery_name)
        for table_name in table_names:
            data = (table_name, *gallery_name_parts)
            self.connector.execute(delete_image_id_query % data)
        logger.info(f"Gallery '{gallery_name}' deleted.")

    def delete_gallery(self, gallery_name: str) -> None:
        logger.debug(f"Deleting gallery '{gallery_name}'...")
        match self.sql_type:
            case "mysql":
                select_table_name_query = f"""
                    SELECT TABLE_NAME
                      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                     WHERE REFERENCED_TABLE_SCHEMA = '{config_loader.database.database}'
                       AND REFERENCED_TABLE_NAME = 'galleries_names'
                       AND REFERENCED_COLUMN_NAME = 'db_gallery_id'
                """
                column_name_parts, _ = mysql_split_gallery_name_based_on_limit("name")
                delete_gallery_id_query = f"""
                    DELETE FROM %s
                     WHERE db_gallery_id = (
                            SELECT db_gallery_id
                            FROM galleries_names
                            WHERE {" AND ".join([f"{part} = '%s'" for part in column_name_parts])}
                           )
                """
        select_table_name_query, delete_gallery_id_query = (
            mullines2oneline(query)
            for query in (select_table_name_query, delete_gallery_id_query)
        )

        logger.debug(f"Select query: {select_table_name_query}")
        table_names = self.connector.fetch_all(select_table_name_query)
        table_names = [t[0] for t in table_names]
        logger.debug(f"Table names: {table_names}")

        logger.debug(f"Delete query: {delete_gallery_id_query}")
        gallery_name_parts = split_gallery_name(gallery_name)
        for table_name in table_names:
            data = (table_name, *gallery_name_parts)
            self.connector.execute(delete_gallery_id_query % data)
        data = ("galleries_names", *gallery_name_parts)
        self.connector.execute(delete_gallery_id_query % data)
        logger.info(f"Gallery '{gallery_name}' deleted.")

    def create_main_tables(self) -> None:
        self._create_galleries_names_table()
        self._create_galleries_gids_table()
        self._create_galleries_download_times_table()
        self._create_galleries_upload_times_table()
        self._create_galleries_modified_times_table()
        self._create_galleries_access_times_table()
        self._create_galleries_titles_table()
        self._create_upload_account_table()
        self._create_galleries_comments_table()
        self._create_galleries_infos_view()
        self._create_files_names_table()
        self._create_galleries_files_hashs_tables()
        self._create_gallery_image_hash_view()
        self._create_removed_galleries_gids_table()

    def insert_gallery_info(self, gallery_folder: str) -> None:
        gallery_info = parse_gallery_info(gallery_folder)
        try:
            id = self._insert_gallery_name_and_return_db_gallery_id(
                gallery_info.gallery_name
            )
            self._insert_galleries_gids(id, gallery_info.gid)
            self._insert_gallery_title(id, gallery_info.title)
            self._insert_upload_time(id, gallery_info.upload_time)
            self._insert_gallery_comment(id, gallery_info.galleries_comments)
            self._insert_gallery_upload_account(id, gallery_info.upload_account)
            self._insert_download_time(id, gallery_info.download_time)
            self._insert_access_time(id, gallery_info.download_time)
            self._insert_modified_time(id, gallery_info.modified_time)
            for file_path in gallery_info.files_path:
                file_id = self._insert_gallery_file_and_return_id(id, file_path)
                absolute_file_path = os.path.join(gallery_folder, file_path)
                with open(absolute_file_path, "rb") as f:
                    file_content = f.read()
                image_hash_insert_params = (file_id, file_content)
                self._insert_gallery_file_sha224(*image_hash_insert_params)
                self._insert_gallery_file_sha256(*image_hash_insert_params)
                self._insert_gallery_file_sha384(*image_hash_insert_params)
                self._insert_gallery_file_sha1(*image_hash_insert_params)
                self._insert_gallery_file_sha512(*image_hash_insert_params)
                self._insert_gallery_file_sha3_224(*image_hash_insert_params)
                self._insert_gallery_file_sha3_256(*image_hash_insert_params)
                self._insert_gallery_file_sha3_384(*image_hash_insert_params)
                self._insert_gallery_file_sha3_512(*image_hash_insert_params)
                self._insert_gallery_file_blake2b(*image_hash_insert_params)
                self._insert_gallery_file_blake2s(*image_hash_insert_params)

            # When the corresponding Tag_{tag_name} table does not exist, a table creation operation will be performed.
            # This will commit and create a new TRANSACTION.
            for tag_name, tag_value in gallery_info.tags.items():
                self._insert_gallery_tag(id, tag_name, tag_value)
        except Exception as e:
            self.delete_gallery_image(gallery_info.gallery_name)
            self.delete_gallery(gallery_info.gallery_name)
            self.connector.commit()
            raise e


def mullines2oneline(s: str) -> str:
    """
    Replaces multiple spaces with a single space, and replaces newlines with a space.

    Args:
        s (str): The input string.

    Returns:
        str: The modified string with multiple spaces replaced by a single space and newlines replaced by a space.
    """
    return re.sub(" +", " ", s.replace("\n", " ")).strip()
