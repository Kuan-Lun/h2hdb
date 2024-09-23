__all__ = ["H2HDB", "GALLERY_INFO_FILE_NAME", "_insert_h2h_download"]


from abc import ABCMeta, abstractmethod
import datetime
import re
import os
import math
from itertools import islice, chain
from functools import partial
from random import shuffle

from .gallery_info_parser import parse_gallery_info, GalleryInfoParser
from .config_loader import Config
from .logger import logger
from .sql_connector import (
    DatabaseConfigurationError,
    DatabaseKeyError,
    DatabaseDuplicateKeyError,
)
from .threading_tools import SQLThreadsList, run_in_parallel, POOL_CPU_LIMIT

from .settings import hash_function_by_file, hash_function, chunk_list
from .settings import (
    FOLDER_NAME_LENGTH_LIMIT,
    FILE_NAME_LENGTH_LIMIT,
    COMPARISON_HASH_ALGORITHM,
    GALLERY_INFO_FILE_NAME,
)

HASH_ALGORITHMS = dict[str, int](sha512=512, sha3_512=512, blake2b=512)


def get_sorting_base_level(x: int = 20) -> int:
    zero_level = max(x, 1)
    return zero_level


class FileInformation:
    def __init__(self, absolute_path: str, db_file_id: int) -> None:
        self.absolute_path = absolute_path
        self.db_file_id = db_file_id
        self.issethash = False
        self.db_hash_id = dict[str, int]()

    def sethash(self) -> None:
        if not self.issethash:
            with open(self.absolute_path, "rb") as file:
                file_content = file.read()
            algorithmlist = list(HASH_ALGORITHMS.keys())
            for algorithm in algorithmlist:
                setattr(self, algorithm, hash_function(file_content, algorithm))
            self.issethash = True

    def setdb_hash_id(self, algorithm: str, db_hash_id: int) -> None:
        self.db_hash_id[algorithm] = db_hash_id


class TagInformation:
    def __init__(self, tag_name: str, tag_value: str) -> None:
        self.tag_name = tag_name
        self.tag_value = tag_value

    def setdb_tag_id(self, db_tag_id: int) -> None:
        self.db_tag_id = db_tag_id


class H2HDBAbstract(metaclass=ABCMeta):
    """
    A class representing the initialization of an SQL connector for the comic database.

    This class is an abstract base class (ABC) and should not be instantiated directly.
    Subclasses must implement the abstract methods defined in this class.

    Attributes:
        sql_type (str): The type of SQL database.
        sql_connection_params (SQLConnectorParams): The parameters for establishing the SQL connection.
        connector (SQLConnector): The SQL connector object.

    Abstract Methods:
        check_database_character_set: Checks the character set of the database.
        check_database_collation: Checks the collation of the database.
        create_main_tables: Creates the main tables for the comic database.
        insert_gallery_info: Inserts the gallery information into the database.
        get_gid_by_gallery_name: Selects the gallery GID from the database.
        get_title_by_gallery_name: Selects the gallery title from the database.
        update_access_time: Updates the access time for the gallery in the database.
        get_upload_account_by_gallery_name: Selects the gallery upload account from the database.
        get_comment_by_gallery_name: Selects the gallery comment from the database.
        get_tag_value_by_gallery_name_and_tag_name: Selects the gallery tag from the database.
        get_files_by_gallery_name: Selects the gallery files from the database.
        delete_gallery_file: Deletes the gallery image from the database.
        delete_gallery: Deletes the gallery from the database.
        insert_todownload_gid: Inserts the GID to be downloaded into the database.
        check_todownload_gid: Checks if the GID is to be downloaded.
        get_todownload_gids: Selects the GIDs to be downloaded from the database.

    Methods:
        __init__: Initializes the H2HDBAbstract object.
        __enter__: Establishes the SQL connection and starts a transaction.
        __exit__: Commits or rolls back the transaction and closes the SQL connection.
    """

    __slots__ = [
        "sql_connection_params",
        "innodb_index_prefix_limit",
        "config",
        "SQLConnector",
    ]

    def __init__(self, config: Config) -> None:
        """
        Initializes the H2HDBAbstract object.

        Raises:
            ValueError: If the SQL type is unsupported.
        """
        self.config = config

        # Set the appropriate connector based on the SQL type
        match self.config.database.sql_type.lower():
            case "mysql":
                from .mysql_connector import MySQLConnectorParams, MySQLConnector

                self.sql_connection_params = MySQLConnectorParams(
                    self.config.database.host,
                    self.config.database.port,
                    self.config.database.user,
                    self.config.database.password,
                    self.config.database.database,
                )
                self.SQLConnector = partial(
                    MySQLConnector, **self.sql_connection_params
                )
                self.innodb_index_prefix_limit = 191
            case _:
                raise ValueError("Unsupported SQL type")

    def __enter__(self) -> "H2HDBAbstract":
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
            with self.SQLConnector() as connector:
                connector.commit()

    def _split_gallery_name(self, gallery_name: str) -> list[str]:
        size = FOLDER_NAME_LENGTH_LIMIT // self.innodb_index_prefix_limit + (
            FOLDER_NAME_LENGTH_LIMIT % self.innodb_index_prefix_limit > 0
        )
        gallery_name_parts = re.findall(
            f".{{1,{self.innodb_index_prefix_limit}}}", gallery_name
        )
        gallery_name_parts += [""] * (size - len(gallery_name_parts))
        return gallery_name_parts

    def _mysql_split_name_based_on_limit(
        self, name: str, name_length_limit: int
    ) -> tuple[list[str], str]:
        num_parts = math.ceil(name_length_limit / self.innodb_index_prefix_limit)
        name_parts = [
            f"{name}_part{i} CHAR({self.innodb_index_prefix_limit}) NOT NULL"
            for i in range(1, name_length_limit // self.innodb_index_prefix_limit + 1)
        ]
        if name_length_limit % self.innodb_index_prefix_limit > 0:
            name_parts.append(
                f"{name}_part{num_parts} CHAR({name_length_limit % self.innodb_index_prefix_limit}) NOT NULL"
            )
        column_name_parts = [f"{name}_part{i}" for i in range(1, num_parts + 1)]
        create_name_parts_sql = ", ".join(name_parts)
        return column_name_parts, create_name_parts_sql

    def mysql_split_gallery_name_based_on_limit(
        self, name: str
    ) -> tuple[list[str], str]:
        return self._mysql_split_name_based_on_limit(name, FOLDER_NAME_LENGTH_LIMIT)

    def mysql_split_file_name_based_on_limit(self, name: str) -> tuple[list[str], str]:
        return self._mysql_split_name_based_on_limit(name, FILE_NAME_LENGTH_LIMIT)

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
    def insert_gallery_info(self, gallery_path: str) -> bool:
        """
        Inserts the gallery information into the database.

        Args:
            gallery_path (str): The path to the gallery folder.
        """
        pass

    @abstractmethod
    def insert_h2h_download(self) -> None:
        """
        Inserts the H@H download information into the database.
        """
        pass

    @abstractmethod
    def get_gid_by_gallery_name(self, gallery_name: str) -> int:
        """
        Selects the gallery GID from the database.

        Args:
            gallery_name (str): The name of the gallery.

        Returns:
            int: The gallery GID.
        """
        pass

    @abstractmethod
    def get_title_by_gallery_name(self, gallery_name: str) -> str:
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
            gallery_name (str): The name of the gallery.
            time (str): The access time.
        """
        pass

    @abstractmethod
    def get_upload_account_by_gallery_name(self, gallery_name: str) -> str:
        """
        Selects the gallery upload account from the database.

        Args:
            gallery_name (str): The name of the gallery.

        Returns:
            str: The gallery upload account.
        """
        pass

    @abstractmethod
    def get_comment_by_gallery_name(self, gallery_name: str) -> str:
        """
        Selects the gallery comment from the database.

        Args:
            gallery_name (str): The name of the gallery.

        Returns:
            str: The gallery comment.
        """
        pass

    @abstractmethod
    def get_tag_value_by_gallery_name_and_tag_name(
        self, gallery_name: str, tag_name: str
    ) -> str:
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
    def get_files_by_gallery_name(self, gallery_name: str) -> list[str]:
        """
        Selects the gallery files from the database.

        Args:
            gallery_name (str): The name of the gallery.

        Returns:
            list[str]: The list of files in the gallery.
        """
        pass

    @abstractmethod
    def delete_gallery_file(self, gallery_name: str) -> None:
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

    @abstractmethod
    def insert_pending_gallery_removal(self, gallery_name: str) -> None:
        """
        Inserts the pending gallery removal into the database.

        Args:
            gallery_name (str): The name of the gallery.
        """
        pass

    @abstractmethod
    def check_pending_gallery_removal(self, gallery_name: str) -> bool:
        """
        Checks if the gallery is pending removal.

        Returns:
            bool: True if the gallery is pending removal, False otherwise.
        """
        pass

    @abstractmethod
    def get_pending_gallery_removals(self) -> list[str]:
        """
        Selects the pending gallery removals from the database.

        Returns:
            list[str]: The list of pending gallery removals.
        """
        pass

    @abstractmethod
    def delete_pending_gallery_removal(self, gallery_name: str) -> None:
        """
        Deletes the pending gallery removal from the database.

        Args:
            gallery_name (str): The name of the gallery.
        """
        pass

    @abstractmethod
    def delete_pending_gallery_removals(self) -> None:
        """
        Deletes all pending gallery removals from the database.
        """
        pass

    @abstractmethod
    def scan_current_galleries_folders(self) -> tuple[list[str], list[str]]:
        """
        Scans the current galleries folders.

        Returns:
            list[str]: The list of current galleries folders.
        """
        pass

    @abstractmethod
    def refresh_current_files_hashs(self) -> None:
        """
        Refreshes the current files hashes in the database.
        """
        pass

    @abstractmethod
    def get_komga_metadata(self, gallery_name: str) -> dict:
        """
        Selects the Komga metadata from the database.

        Args:
            gallery_name (str): The name of the gallery.

        Returns:
            dict: The Komga metadata.
        """
        pass

    @abstractmethod
    def check_todownload_gid(self, gid: int, url: str) -> bool:
        """
        Checks if the GID is to be downloaded.

        Args:
            gid (int): The gallery GID.
            url (str): The gallery URL.

        Returns:
            bool: True if the GID is to be downloaded, False otherwise.
        """
        pass

    @abstractmethod
    def get_todownload_gids(self) -> list[tuple[int, str]]:
        """
        Selects the GIDs to be downloaded from the database.

        Returns:
            list[tuple[int, str]]: The list of GIDs to be downloaded.
        """
        pass

    @abstractmethod
    def remove_todownload_gid(self, gid: int) -> None:
        """
        Removes the GID to be downloaded from the database.

        Args:
            gid (int): The gallery GID.
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
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    charset = "utf8mb4"
                    query = "SHOW VARIABLES LIKE 'character_set_database';"

            charset_result = connector.fetch_one(query)[1]
            is_charset_valid = charset_result == charset
            if not is_charset_valid:
                message = f"Invalid database character set. Must be '{charset}' but is '{charset_result}'."
                logger.error(message)
                raise DatabaseConfigurationError(message)
            logger.info("Database character set is valid.")

    def check_database_collation(self) -> None:
        """
        Checks the collation of the database and raises an error if it is invalid.

        Raises:
            DatabaseConfigurationError: If the database collation is invalid.
        """
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = "SHOW VARIABLES LIKE 'collation_database';"
                    collation = "utf8mb4_bin"

            collation_result = connector.fetch_one(query)[1]
            is_collation_valid = collation_result == collation
            if not is_collation_valid:
                message = f"Invalid database collation. Must be '{collation}' but is '{collation_result}'."
                logger.error(message)
                raise DatabaseConfigurationError(message)
            logger.info("Database character set and collation are valid.")


class H2HDBGalleriesIDs(H2HDBAbstract, metaclass=ABCMeta):
    def _create_galleries_names_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_dbids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name = "name"
                    column_name_parts, create_gallery_name_parts_sql = (
                        self.mysql_split_gallery_name_based_on_limit(column_name)
                    )
                    id_query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            db_gallery_id INT  UNSIGNED AUTO_INCREMENT,
                            {create_gallery_name_parts_sql},
                            UNIQUE real_primay_key ({", ".join(column_name_parts)})
                        )
                    """
            connector.execute(id_query)

            table_name = "galleries_names"
            match self.config.database.sql_type.lower():
                case "mysql":
                    name_query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_gallery_id INT  UNSIGNED NOT NULL,
                            full_name     TEXT          NOT NULL,
                            FULLTEXT (full_name)
                        )
                    """
            connector.execute(name_query)
            logger.info(f"{table_name} table created.")

    def _insert_gallery_name(self, gallery_name: str) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_dbids"
            gallery_name_parts = self._split_gallery_name(gallery_name)

            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name_parts, _ = self.mysql_split_gallery_name_based_on_limit(
                        "name"
                    )
                    insert_query = f"""
                        INSERT INTO {table_name}
                            ({", ".join(column_name_parts)})
                        VALUES ({", ".join(["%s" for _ in column_name_parts])})
                    """
            connector.execute(insert_query, tuple(gallery_name_parts))

            db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)

            table_name = "galleries_names"
            gallery_name_parts = self._split_gallery_name(gallery_name)

            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name_parts, _ = self.mysql_split_gallery_name_based_on_limit(
                        "name"
                    )
                    insert_query = f"""
                        INSERT INTO {table_name}
                            (db_gallery_id, full_name)
                        VALUES (%s, %s)
                    """
            connector.execute(insert_query, (db_gallery_id, gallery_name))

    def __get_db_gallery_id_by_gallery_name(self, gallery_name: str) -> tuple | None:
        with self.SQLConnector() as connector:
            table_name = "galleries_dbids"
            gallery_name_parts = self._split_gallery_name(gallery_name)

            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name_parts, _ = self.mysql_split_gallery_name_based_on_limit(
                        "name"
                    )
                    select_query = f"""
                        SELECT db_gallery_id
                        FROM {table_name}
                        WHERE {" AND ".join([f"{part} = %s" for part in column_name_parts])}
                    """

            query_result = connector.fetch_one(select_query, tuple(gallery_name_parts))
        return query_result

    def _check_galleries_dbids_by_gallery_name(self, gallery_name: str) -> bool:
        query_result = self.__get_db_gallery_id_by_gallery_name(gallery_name)
        return query_result is not None

    def _get_db_gallery_id_by_gallery_name(self, gallery_name: str) -> int:
        query_result = self.__get_db_gallery_id_by_gallery_name(gallery_name)
        if query_result is None:
            logger.debug(f"Gallery name '{gallery_name}' does not exist.")
            raise DatabaseKeyError(f"Gallery name '{gallery_name}' does not exist.")
        else:
            db_gallery_id = query_result[0]
        return db_gallery_id


class H2HDBGalleriesGIDs(H2HDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    """
    A class that handles the GIDs for galleries in the comic database.

    This class inherits from `H2HDBAbstract` and is used to manage the GIDs for galleries

    Attributes:
        sql_type (str): The type of SQL database being used.
        sql_connection_params (SQLConnectorParams): The parameters for establishing the SQL connection.
        connector (SQLConnector): The SQL connector object.

    Methods:
        _create_galleries_gids_table: Creates the galleries_gids table.
        _insert_gallery_gid: Inserts the GID for the gallery name ID into the galleries_gids table.
        get_gid_by_gallery_name: Selects the GID for the gallery name from the database.
    """

    def _create_galleries_gids_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_gallery_id INT UNSIGNED NOT NULL,
                            gid           INT UNSIGNED NOT NULL,
                            INDEX (gid)
                        )
                    """
            connector.execute(query)
            logger.info(f"{table_name} table created.")

    def _insert_gallery_gid(self, db_gallery_id: int, gid: int) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query = f"""
                        INSERT INTO {table_name} (db_gallery_id, gid) VALUES (%s, %s)
                    """
            connector.execute(insert_query, (db_gallery_id, gid))

    def _get_gid_by_db_gallery_id(self, db_gallery_id: int) -> int:
        with self.SQLConnector() as connector:
            table_name = "galleries_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT gid
                        FROM {table_name}
                        WHERE db_gallery_id = %s
                    """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
            if query_result is None:
                msg = f"GID for gallery name ID {db_gallery_id} does not exist."
                logger.error(msg)
                raise DatabaseKeyError(msg)
            else:
                gid = query_result[0]
        return gid

    def get_gid_by_gallery_name(self, gallery_name: str) -> int:
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        return self._get_gid_by_db_gallery_id(db_gallery_id)


class H2HDBTimes(H2HDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_times_table(self, table_name: str) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_gallery_id INT UNSIGNED NOT NULL,
                            time          DATETIME     NOT NULL,
                            INDEX (time)
                        )
                    """
            connector.execute(query)
            logger.info(f"{table_name} table created.")

    def _insert_time(self, table_name: str, db_gallery_id: int, time: str) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query = f"""
                        INSERT INTO {table_name} (db_gallery_id, time) VALUES (%s, %s)
                    """
            connector.execute(insert_query, (db_gallery_id, time))

    def _select_time(self, table_name: str, db_gallery_id: int) -> datetime.datetime:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT time
                        FROM {table_name}
                        WHERE db_gallery_id = %s
                    """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
            if query_result is None:
                msg = f"Time for gallery name ID {db_gallery_id} does not exist in table '{table_name}'."
                logger.error(msg)
                raise DatabaseKeyError(msg)
            else:
                time = query_result[0]
        return time

    def _update_time(self, table_name: str, db_gallery_id: int, time: str) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    update_query = f"""
                        UPDATE {table_name} SET time = %s WHERE db_gallery_id = %s
                    """
            connector.execute(update_query, (time, db_gallery_id))

    def _create_galleries_download_times_table(self) -> None:
        self._create_times_table("galleries_download_times")

    def _create_galleries_redownload_times_table(self) -> None:
        self._create_times_table("galleries_redownload_times")

    def _insert_download_time(self, db_gallery_id: int, time: str) -> None:
        self._insert_time("galleries_download_times", db_gallery_id, time)
        self._insert_time("galleries_redownload_times", db_gallery_id, time)

    def update_redownload_time(self, db_gallery_id: int, time: str) -> None:
        self._update_time("galleries_redownload_times", db_gallery_id, time)

    def _create_galleries_upload_times_table(self) -> None:
        self._create_times_table("galleries_upload_times")

    def _insert_upload_time(self, db_gallery_id: int, time: str) -> None:
        self._insert_time("galleries_upload_times", db_gallery_id, time)

    def get_upload_time_by_gallery_name(self, gallery_name: str) -> datetime.datetime:
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        return self._select_time("galleries_upload_times", db_gallery_id)

    def _create_galleries_modified_times_table(self) -> None:
        self._create_times_table("galleries_modified_times")

    def _insert_modified_time(self, db_gallery_id: int, time: str) -> None:
        self._insert_time("galleries_modified_times", db_gallery_id, time)

    def _create_galleries_access_times_table(self) -> None:
        self._create_times_table("galleries_access_times")

    def _insert_access_time(self, db_gallery_id: int, time: str) -> None:
        self._insert_time("galleries_access_times", db_gallery_id, time)

    def update_access_time(self, gallery_name: str, time: str) -> None:
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        self._update_time("galleries_access_times", db_gallery_id, time)


class H2HDBGalleriesTitles(H2HDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_galleries_titles_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_titles"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_gallery_id INT UNSIGNED NOT NULL,
                            title         TEXT         NOT NULL,
                            FULLTEXT (title)
                        )
                    """
            connector.execute(query)
            logger.info(f"{table_name} table created.")

    def _insert_gallery_title(self, db_gallery_id: int, title: str) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_titles"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query = f"""
                        INSERT INTO {table_name} (db_gallery_id, title) VALUES (%s, %s)
                    """
            connector.execute(insert_query, (db_gallery_id, title))

    def _get_title_by_db_gallery_id(self, db_gallery_id: int) -> str:
        with self.SQLConnector() as connector:
            table_name = "galleries_titles"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT title
                        FROM {table_name}
                        WHERE db_gallery_id = %s
                    """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
            if query_result is None:
                msg = f"Title for gallery name ID {db_gallery_id} does not exist."
                logger.error(msg)
                raise DatabaseKeyError(msg)
            else:
                title = query_result[0]
        return title

    def get_title_by_gallery_name(self, gallery_name: str) -> str:
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        return self._get_title_by_db_gallery_id(db_gallery_id)


class H2HDBUploadAccounts(H2HDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_upload_account_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_upload_accounts"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_gallery_id INT UNSIGNED                      NOT NULL,
                            account       CHAR({self.innodb_index_prefix_limit}) NOT NULL,
                            INDEX (account)
                        )
                    """
            connector.execute(query)
            logger.info(f"{table_name} table created.")

    def _insert_gallery_upload_account(self, db_gallery_id: int, account: str) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_upload_accounts"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query = f"""
                        INSERT INTO {table_name} (db_gallery_id, account) VALUES (%s, %s)
                    """
            connector.execute(insert_query, (db_gallery_id, account))

    def _select_gallery_upload_account(self, db_gallery_id: int) -> str:
        with self.SQLConnector() as connector:
            table_name = "galleries_upload_accounts"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT account
                        FROM {table_name}
                        WHERE db_gallery_id = %s
                    """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
            if query_result is None:
                msg = f"Upload account for gallery name ID {db_gallery_id} does not exist."
                logger.error(msg)
                raise DatabaseKeyError(msg)
            else:
                account = query_result[0]
        return account

    def get_upload_account_by_gallery_name(self, gallery_name: str) -> str:
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        return self._select_gallery_upload_account(db_gallery_id)


class H2HDBGalleriesInfos(
    H2HDBGalleriesTitles,
    H2HDBUploadAccounts,
    H2HDBTimes,
    H2HDBGalleriesGIDs,
    H2HDBGalleriesIDs,
    H2HDBCheckDatabaseSettings,
):
    def _create_galleries_infos_view(self) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
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
            connector.execute(query)
            logger.info("galleries_infos view created.")


class H2HDBGalleriesComments(H2HDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_galleries_comments_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_comments"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_gallery_id INT UNSIGNED NOT NULL,
                            comment       TEXT         NOT NULL,
                            FULLTEXT (Comment)
                        )
                    """
            connector.execute(query)
            logger.info(f"{table_name} table created.")

    def _insert_gallery_comment(self, db_gallery_id: int, comment: str) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_comments"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query = f"""
                        INSERT INTO {table_name} (db_gallery_id, comment) VALUES (%s, %s)
                    """
            connector.execute(insert_query, (db_gallery_id, comment))

    def _update_gallery_comment(self, db_gallery_id: int, comment: str) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_comments"
            match self.config.database.sql_type.lower():
                case "mysql":
                    update_query = f"""
                        UPDATE {table_name} SET Comment = %s WHERE db_gallery_id = %s
                    """
            connector.execute(update_query, (comment, db_gallery_id))

    def __get_gallery_comment_by_db_gallery_id(
        self, db_gallery_id: int
    ) -> tuple | None:
        with self.SQLConnector() as connector:
            table_name = "galleries_comments"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT Comment
                        FROM {table_name}
                        WHERE db_gallery_id = %s
                    """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
        return query_result

    def _check_gallery_comment_by_db_gallery_id(self, db_gallery_id: int) -> bool:
        query_result = self.__get_gallery_comment_by_db_gallery_id(db_gallery_id)
        return query_result is not None

    def _check_gallery_comment_by_gallery_name(self, gallery_name: str) -> bool:
        if not self._check_galleries_dbids_by_gallery_name(gallery_name):
            return False
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        return self._check_gallery_comment_by_db_gallery_id(db_gallery_id)

    def _select_gallery_comment(self, db_gallery_id: int) -> str:
        query_result = self.__get_gallery_comment_by_db_gallery_id(db_gallery_id)
        if query_result is None:
            msg = (
                f"Uploader comment for gallery name ID {db_gallery_id} does not exist."
            )
            logger.error(msg)
            raise DatabaseKeyError(msg)
        else:
            comment = query_result[0]
        return comment

    def get_comment_by_gallery_name(self, gallery_name: str) -> str:
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        return self._select_gallery_comment(db_gallery_id)


class H2HDBGalleriesTags(H2HDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_galleries_tags_table(self) -> None:
        with self.SQLConnector() as connector:
            tag_name_table_name = f"galleries_tags_names"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {tag_name_table_name} (
                            PRIMARY KEY (tag_name),
                            tag_name CHAR({self.innodb_index_prefix_limit}) NOT NULL
                        )
                    """
            connector.execute(query)
            logger.info(f"{tag_name_table_name} table created.")

            tag_value_table_name = f"galleries_tags_values"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {tag_value_table_name} (
                            PRIMARY KEY (tag_value),
                            tag_value CHAR({self.innodb_index_prefix_limit}) NOT NULL
                        )
                    """
            connector.execute(query)
            logger.info(f"{tag_value_table_name} table created.")

            tag_pairs_table_name = f"galleries_tag_pairs_dbids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {tag_pairs_table_name} (
                            PRIMARY KEY (db_tag_pair_id),
                            db_tag_pair_id INT UNSIGNED                           AUTO_INCREMENT,
                            tag_name       CHAR({self.innodb_index_prefix_limit}) NOT NULL,
                            FOREIGN KEY (tag_name) REFERENCES {tag_name_table_name}(tag_name)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            tag_value      CHAR({self.innodb_index_prefix_limit}) NOT NULL,
                            FOREIGN KEY (tag_value) REFERENCES {tag_value_table_name}(tag_value)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            UNIQUE (tag_name, tag_value),
                            INDEX (tag_value)
                        )
                    """
            connector.execute(query)
            logger.info(f"{tag_pairs_table_name} table created.")

            table_name = f"galleries_tags"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id, db_tag_pair_id),
                            db_gallery_id  INT UNSIGNED NOT NULL,
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_tag_pair_id INT UNSIGNED NOT NULL,
                            FOREIGN KEY (db_tag_pair_id) REFERENCES {tag_pairs_table_name}(db_tag_pair_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            UNIQUE (db_tag_pair_id, db_gallery_id)
                        )
                    """
            connector.execute(query)
            logger.info(f"{table_name} table created.")

    def __get_db_tag_pair_id(self, tag_name: str, tag_value: str) -> tuple | None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT db_tag_pair_id
                        FROM galleries_tag_pairs_dbids
                        WHERE tag_name = %s AND tag_value = %s
                    """
            query_result = connector.fetch_one(select_query, (tag_name, tag_value))
        return query_result

    def _check_db_tag_pair_id(self, tag_name: str, tag_value: str) -> bool:
        query_result = self.__get_db_tag_pair_id(tag_name, tag_value)
        return query_result is not None

    def _get_db_tag_pair_id(self, tag_name: str, tag_value: str) -> int:
        query_result = self.__get_db_tag_pair_id(tag_name, tag_value)
        if query_result is None:
            logger.debug(f"Tag '{tag_value}' does not exist.")
            raise DatabaseKeyError(f"Tag '{tag_value}' does not exist.")
        else:
            db_tag_id = query_result[0]
        return db_tag_id

    def _check_gallery_tag_name(self, tag_name: str) -> bool:
        with self.SQLConnector() as connector:
            table_name = f"galleries_tags_names"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT tag_name
                        FROM {table_name}
                        WHERE tag_name = %s
                    """
            query_result = connector.fetch_one(select_query, (tag_name,))
        return query_result is not None

    def _check_gallery_tag_value(self, tag_value: str) -> bool:
        with self.SQLConnector() as connector:
            table_name = f"galleries_tags_values"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT tag_value
                        FROM {table_name}
                        WHERE tag_value = %s
                    """
            query_result = connector.fetch_one(select_query, (tag_value,))
        return query_result is not None

    def _insert_tag_names(self, tag_names: list[str]) -> None:
        if len(tag_names) == 0:
            return
        with self.SQLConnector() as connector:
            table_name = f"galleries_tags_names"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query_header = f"""
                        INSERT INTO {table_name} (tag_name)
                    """
                    insert_query_values = " ".join(
                        ["VALUES", ", ".join(["(%s)" for _ in tag_names])]
                    )
                    insert_query = f"{insert_query_header} {insert_query_values}"
            try:
                connector.execute(insert_query, tuple(tag_names))
            except DatabaseDuplicateKeyError:
                pass
            except Exception as e:
                raise e

    def _insert_tag_values(self, tag_values: list[str]) -> None:
        if len(tag_values) == 0:
            return
        with self.SQLConnector() as connector:
            table_name = f"galleries_tags_values"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query_header = f"""
                        INSERT INTO {table_name} (tag_value)
                    """
                    insert_query_values = " ".join(
                        ["VALUES", ", ".join(["(%s)" for _ in tag_values])]
                    )
                    insert_query = f"{insert_query_header} {insert_query_values}"
            try:
                connector.execute(insert_query, tuple(tag_values))
            except DatabaseDuplicateKeyError:
                pass
            except Exception as e:
                raise e

    def _insert_tag_pairs_dbids(self, tags: list[TagInformation]) -> None:
        if len(tags) == 0:
            return
        with self.SQLConnector() as connector:
            tag_pairs_table_name = f"galleries_tag_pairs_dbids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query_header = f"""
                        INSERT INTO {tag_pairs_table_name} (tag_name, tag_value)
                    """
                    insert_query_values = " ".join(
                        ["VALUES", ", ".join(["(%s, %s)" for _ in tags])]
                    )
                    insert_query = f"{insert_query_header} {insert_query_values}"
            parameter = list[str]()
            for tag in tags:
                parameter.extend([tag.tag_name, tag.tag_value])
            try:
                connector.execute(insert_query, tuple(parameter))
            except DatabaseDuplicateKeyError:
                toinsert_db_tag_pair_id = list[TagInformation]()
                for tag in tags:
                    if not self._check_db_tag_pair_id(tag.tag_name, tag.tag_value):
                        toinsert_db_tag_pair_id.append(tag)
                self._insert_tag_pairs_dbids(toinsert_db_tag_pair_id)
            except Exception as e:
                raise e

    def _insert_gallery_tags(
        self, db_gallery_id: int, tags: list[TagInformation]
    ) -> None:
        if len(tags) == 0:
            return

        toinsert_db_tag_pair_id = list[TagInformation]()
        for tag in tags:
            if not self._check_db_tag_pair_id(tag.tag_name, tag.tag_value):
                toinsert_db_tag_pair_id.append(tag)

        toinsert_tag_name = list[str]()
        toinsert_tag_value = list[str]()
        for tag in toinsert_db_tag_pair_id:
            if (tag.tag_name not in toinsert_tag_name) and (
                not self._check_gallery_tag_name(tag.tag_name)
            ):
                toinsert_tag_name.append(tag.tag_name)
            if (tag.tag_value not in toinsert_tag_value) and (
                not self._check_gallery_tag_value(tag.tag_value)
            ):
                toinsert_tag_value.append(tag.tag_value)
        self._insert_tag_names(toinsert_tag_name)
        self._insert_tag_values(toinsert_tag_value)

        self._insert_tag_pairs_dbids(toinsert_db_tag_pair_id)

        db_tag_pair_ids = list[int]()
        for tag in tags:
            db_tag_pair_ids.append(
                self._get_db_tag_pair_id(tag.tag_name, tag.tag_value)
            )
        with self.SQLConnector() as connector:
            table_name = f"galleries_tags"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query_header = f"""
                        INSERT INTO {table_name} (db_gallery_id, db_tag_pair_id)
                    """
                    insert_query_values = " ".join(
                        ["VALUES", ", ".join(["(%s, %s)" for _ in db_tag_pair_ids])]
                    )
                    insert_query = f"{insert_query_header} {insert_query_values}"
            parameter = list[int]()
            for db_tag_pair_id in db_tag_pair_ids:
                parameter.extend([db_gallery_id, db_tag_pair_id])
            connector.execute(insert_query, tuple(parameter))

    def _select_gallery_tag(self, db_gallery_id: int, tag_name: str) -> str:
        with self.SQLConnector() as connector:
            table_name = f"galleries_tags_{tag_name}"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT tag
                        FROM {table_name}
                        WHERE db_gallery_id = %s
                    """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
            if query_result is None:
                msg = f"Tag '{tag_name}' does not exist."
                logger.error(msg)
                raise DatabaseKeyError(msg)
            else:
                tag = query_result[0]
        return tag

    def get_tag_value_by_gallery_name_and_tag_name(
        self, gallery_name: str, tag_name: str
    ) -> str:
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        return self._select_gallery_tag(db_gallery_id, tag_name)

    def get_tag_pairs_by_gallery_name(self, gallery_name: str) -> list[tuple[str, str]]:
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        db_tag_pair_ids = self._get_db_tag_pair_id_by_db_gallery_id(db_gallery_id)
        return [
            self._get_tag_pairs_by_db_tag_pair_id(db_tag_pair_id)
            for db_tag_pair_id in db_tag_pair_ids
        ]

    def _get_db_tag_pair_id_by_db_gallery_id(self, db_gallery_id: int) -> list[int]:
        with self.SQLConnector() as connector:
            table_name = "galleries_tags"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT db_tag_pair_id
                        FROM {table_name}
                        WHERE db_gallery_id = %s
                    """
            query_result = connector.fetch_all(select_query, (db_gallery_id,))
        return [query[0] for query in query_result]

    def _get_tag_pairs_by_db_tag_pair_id(self, db_tag_pair_id: int) -> tuple[str, str]:
        with self.SQLConnector() as connector:
            table_name = "galleries_tag_pairs_dbids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT tag_name, tag_value
                        FROM {table_name}
                        WHERE db_tag_pair_id = %s
                    """
            query_result = connector.fetch_one(select_query, (db_tag_pair_id,))
            if query_result is None:
                msg = f"Tag pair ID {db_tag_pair_id} does not exist."
                logger.error(msg)
                raise DatabaseKeyError(msg)
            else:
                tag_name, tag_value = query_result
        return tag_name, tag_value


class H2HDBFiles(H2HDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_files_names_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = f"files_dbids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name = "name"
                    column_name_parts, create_gallery_name_parts_sql = (
                        self.mysql_split_file_name_based_on_limit(column_name)
                    )
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_file_id),
                            db_file_id    INT UNSIGNED AUTO_INCREMENT,
                            db_gallery_id INT UNSIGNED NOT NULL,
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            {create_gallery_name_parts_sql},
                            UNIQUE real_primay_key (db_gallery_id, {", ".join(column_name_parts)}),
                            UNIQUE db_file_to_gallery_id (db_file_id, db_gallery_id)
                        )
                    """
            connector.execute(query)
            logger.info(f"{table_name} table created.")

            table_name = f"files_names"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_file_id),
                            FOREIGN KEY (db_file_id) REFERENCES files_dbids(db_file_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_file_id  INT UNSIGNED NOT NULL,
                            full_name   TEXT         NOT NULL,
                            FULLTEXT (full_name)
                        )
                    """
            connector.execute(query)
            logger.info(f"{table_name} table created.")

    def _insert_gallery_files(
        self, db_gallery_id: int, file_names_list: list[str]
    ) -> None:
        with self.SQLConnector() as connector:

            file_name_parts_list = list[list[str]]()
            for file_name in file_names_list:
                if len(file_name) > FILE_NAME_LENGTH_LIMIT:
                    logger.error(
                        f"File name '{file_name}' is too long. Must be {FILE_NAME_LENGTH_LIMIT} characters or less."
                    )
                    raise ValueError("File name is too long.")
                file_name_parts_list.append(self._split_gallery_name(file_name))

            table_name = "files_dbids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name_parts, _ = self.mysql_split_file_name_based_on_limit(
                        "name"
                    )
                    insert_query_header = f"""
                        INSERT INTO {table_name}
                            (db_gallery_id, {", ".join(column_name_parts)})
                    """  # VALUES (%s, {", ".join(["%s" for _ in column_name_parts])})
                    insert_query_values = " ".join(
                        [
                            "VALUES",
                            ", ".join(
                                [
                                    f"(%s, {", ".join(["%s"   for _ in column_name_parts])})"
                                    for _ in file_names_list
                                ]
                            ),
                        ]
                    )
                    insert_query = f"{insert_query_header} {insert_query_values}"
            insert_parameter = tuple(
                chain(
                    *[
                        (db_gallery_id, *file_name_parts_list[n])
                        for n in range(len(file_name_parts_list))
                    ]
                )
            )
            connector.execute(
                insert_query,
                insert_parameter,
            )

            db_file_id_list = [
                self._get_db_file_id(db_gallery_id, file_name)
                for file_name in file_names_list
            ]

            table_name = "files_names"
            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name_parts, _ = self.mysql_split_file_name_based_on_limit(
                        "name"
                    )
                    insert_query_header = f"""
                        INSERT INTO {table_name}
                            (db_file_id, full_name)
                    """
                    insert_query_values = " ".join(
                        ["VALUES", ", ".join(["(%s, %s)" for _ in file_names_list])]
                    )
            insert_query = f"{insert_query_header} {insert_query_values}"

            connector.execute(
                insert_query,
                tuple(
                    chain(
                        *[
                            (db_file_id_list[n], file_names_list[n])
                            for n in range(len(file_names_list))
                        ]
                    )
                ),
            )

    def __get_db_file_id(self, db_gallery_id: int, file_name: str) -> tuple | None:
        with self.SQLConnector() as connector:
            table_name = "files_dbids"
            file_name_parts = self._split_gallery_name(file_name)
            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name_parts, _ = self.mysql_split_file_name_based_on_limit(
                        "name"
                    )
                    select_query = f"""
                        SELECT db_file_id
                        FROM {table_name}
                        WHERE db_gallery_id = %s
                        AND {" AND ".join([f"{part} = %s" for part in column_name_parts])}
                    """
            data = (db_gallery_id, *file_name_parts)
            query_result = connector.fetch_one(select_query, data)
        return query_result

    def _check_db_file_id(self, db_gallery_id: int, file_name: str) -> bool:
        query_result = self.__get_db_file_id(db_gallery_id, file_name)
        return query_result is not None

    def _get_db_file_id(self, db_gallery_id: int, file_name: str) -> int:
        query_result = self.__get_db_file_id(db_gallery_id, file_name)
        if query_result is None:
            msg = f"Image ID for gallery name ID {db_gallery_id} and file '{file_name}' does not exist."
            logger.error(msg)
            raise DatabaseKeyError(msg)
        else:
            gallery_image_id = query_result[0]
        return gallery_image_id

    def get_files_by_gallery_name(self, gallery_name: str) -> list[str]:
        with self.SQLConnector() as connector:
            db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
            table_name = "files_names"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT full_name
                            FROM {table_name}
                            WHERE db_gallery_id = %s
                    """
            query_result = connector.fetch_all(select_query, (db_gallery_id,))
            if len(query_result) == 0:
                msg = f"Files for gallery name ID {db_gallery_id} do not exist."
                logger.error(msg)
                raise DatabaseKeyError(msg)
            else:
                files = [query[0] for query in query_result]
        return files

    def _create_galleries_files_hashs_table(
        self, algorithm: str, output_bits: int
    ) -> None:
        with self.SQLConnector() as connector:
            dbids_table_name = "files_hashs_%s_dbids" % algorithm.lower()
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {dbids_table_name} (
                            PRIMARY KEY (db_hash_id),
                            db_hash_id INT UNSIGNED AUTO_INCREMENT,
                            hash_value BINARY({output_bits/8}) NOT NULL,
                            UNIQUE (hash_value)
                        )
                    """
            connector.execute(query)
            logger.info(f"{dbids_table_name} table created.")

            table_name = "files_hashs_%s" % algorithm.lower()
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_file_id),
                            FOREIGN KEY (db_file_id) REFERENCES files_dbids(db_file_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_file_id INT UNSIGNED NOT NULL,
                            FOREIGN KEY (db_hash_id) REFERENCES {dbids_table_name}(db_hash_id)
                                ON UPDATE CASCADE,
                            db_hash_id INT UNSIGNED NOT NULL,
                            UNIQUE db_hash_id (db_hash_id, db_file_id)
                        )
                    """
            connector.execute(query)
            logger.info(f"{table_name} table created.")

    def _create_galleries_files_hashs_tables(self) -> None:
        logger.debug("Creating gallery image hash tables...")
        for algorithm, output_bits in HASH_ALGORITHMS.items():
            self._create_galleries_files_hashs_table(algorithm, output_bits)
        logger.info("Gallery image hash tables created.")

    def _create_gallery_image_hash_view(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "files_hashs"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE VIEW IF NOT EXISTS {table_name} AS
                        SELECT files_names.db_file_id               AS db_file_id,
                            galleries_titles.title               AS gallery_title,
                            galleries_names.full_name            AS gallery_name,
                            files_names.full_name                AS file_name,
                            files_hashs_sha512_dbids.hash_value  AS sha512
                        FROM files_names
                            LEFT JOIN files_dbids                USING (db_file_id)
                            LEFT JOIN galleries_titles           USING (db_gallery_id)
                            LEFT JOIN galleries_names            USING (db_gallery_id)
                            LEFT JOIN files_hashs_sha512         USING (db_file_id)
                            LEFT JOIN files_hashs_sha512_dbids   USING (db_hash_id)
                    """
            connector.execute(query)
            logger.info(f"{table_name} view created.")

    def _check_files_dbids_by_db_gallery_id(self, db_gallery_id: int) -> tuple | None:
        with self.SQLConnector() as connector:
            table_name = f"files_dbids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT COUNT(*)
                        FROM {table_name}
                        WHERE db_gallery_id = %s
                    """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
        return query_result[0] != 0

    def _insert_gallery_file_hash_for_db_gallery_id(
        self, fileinformations: list[FileInformation]
    ) -> None:
        algorithmlist = list(HASH_ALGORITHMS.keys())
        for algorithm in algorithmlist:
            toinsert = list[bytes]()
            for n in range(len(fileinformations)):
                fileinformations[n].sethash()
                filehash = getattr(fileinformations[n], algorithm)
                if filehash in toinsert:
                    continue
                else:
                    if not self._check_db_hash_id_by_hash_value(filehash, algorithm):
                        toinsert.append(filehash)
            if len(toinsert) > 0:
                self.insert_db_hash_id_by_hash_values(toinsert, algorithm)
            for n in range(len(fileinformations)):
                fileinformations[n].setdb_hash_id(
                    algorithm,
                    self.get_db_hash_id_by_hash_value(
                        getattr(fileinformations[n], algorithm), algorithm
                    ),
                )
        self.insert_hash_value_by_db_hash_ids(fileinformations)

    def _insert_gallery_file_hash(
        self, db_file_id: int, absolute_file_path: str
    ) -> None:

        algorithmlist = list(HASH_ALGORITHMS.keys())
        shuffle(algorithmlist)
        for algorithm in algorithmlist:
            is_insert = False
            current_hash_value = hash_function_by_file(absolute_file_path, algorithm)
            if self._check_hash_value_by_file_id(db_file_id, algorithm):
                original_hash_value = self.get_hash_value_by_file_id(
                    db_file_id, algorithm
                )
                if original_hash_value != current_hash_value:
                    if self._check_db_hash_id_by_hash_value(
                        current_hash_value, algorithm
                    ):
                        db_hash_id = self.get_db_hash_id_by_hash_value(
                            current_hash_value, algorithm
                        )
                        self._update_gallery_file_hash_by_db_hash_id(
                            db_file_id, db_hash_id, algorithm
                        )
                    else:
                        is_insert |= True
            else:
                is_insert |= True

            if is_insert:
                if self._check_db_hash_id_by_hash_value(current_hash_value, algorithm):
                    db_hash_id = self.get_db_hash_id_by_hash_value(
                        current_hash_value, algorithm
                    )
                else:
                    with self.SQLConnector() as connector:
                        table_name = f"files_hashs_{algorithm.lower()}_dbids"
                        match self.config.database.sql_type.lower():
                            case "mysql":
                                insert_hash_value_query = f"""
                                    INSERT INTO {table_name} (hash_value) VALUES (%s)
                                """
                        try:
                            connector.execute(
                                insert_hash_value_query, (current_hash_value,)
                            )
                        except DatabaseDuplicateKeyError:
                            logger.warning(
                                f"Hash value {current_hash_value!r} already exists in the database."
                            )
                        except Exception as e:
                            raise e
                    db_hash_id = self.get_db_hash_id_by_hash_value(
                        current_hash_value, algorithm
                    )

                with self.SQLConnector() as connector:
                    table_name = f"files_hashs_{algorithm.lower()}"
                    match self.config.database.sql_type.lower():
                        case "mysql":
                            insert_db_hash_id_query = f"""
                                INSERT INTO {table_name} (db_file_id, db_hash_id) VALUES (%s, %s)
                            """
                    connector.execute(insert_db_hash_id_query, (db_file_id, db_hash_id))

    def __get_db_hash_id_by_hash_value(
        self, hash_value: bytes, algorithm: str
    ) -> tuple | None:
        with self.SQLConnector() as connector:
            table_name = f"files_hashs_{algorithm.lower()}_dbids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT db_hash_id
                        FROM {table_name}
                        WHERE hash_value = %s
                    """
            query_result = connector.fetch_one(select_query, (hash_value,))
        return query_result

    def _check_db_hash_id_by_hash_value(
        self, hash_value: bytes, algorithm: str
    ) -> bool:
        query_result = self.__get_db_hash_id_by_hash_value(hash_value, algorithm)
        return query_result is not None

    def get_db_hash_id_by_hash_value(self, hash_value: bytes, algorithm: str) -> int:
        query_result = self.__get_db_hash_id_by_hash_value(hash_value, algorithm)
        if query_result is None:
            msg = f"Image hash for image ID {hash_value!r} does not exist."
            raise DatabaseKeyError(msg)
        else:
            db_hash_id = query_result[0]
        return db_hash_id

    def insert_hash_value_by_db_hash_ids(
        self, fileinformations: list[FileInformation]
    ) -> None:
        algorithmlist = list(HASH_ALGORITHMS.keys())
        for algorithm in algorithmlist:
            with self.SQLConnector() as connector:
                table_name = f"files_hashs_{algorithm.lower()}"
                match self.config.database.sql_type.lower():
                    case "mysql":
                        insert_query_header = f"""
                            INSERT INTO {table_name} (db_file_id, db_hash_id)
                        """
                        insert_query_values = " ".join(
                            [
                                "VALUES",
                                ", ".join(["(%s, %s)" for _ in fileinformations]),
                            ]
                        )
                insert_query = f"{insert_query_header} {insert_query_values}"
                parameters = list[int]()
                for fileinformation in fileinformations:
                    parameters += [
                        fileinformation.db_file_id,
                        fileinformation.db_hash_id[algorithm],
                    ]
                connector.execute(insert_query, tuple(parameters))

    def insert_db_hash_id_by_hash_value(
        self, hash_value: bytes, algorithm: str
    ) -> None:
        with self.SQLConnector() as connector:
            table_name = f"files_hashs_{algorithm.lower()}_dbids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query = f"""
                        INSERT INTO {table_name} (hash_value) VALUES (%s)
                    """
            connector.execute(insert_query, (hash_value,))

    def insert_db_hash_id_by_hash_values(
        self, hash_values: list[bytes], algorithm: str
    ) -> None:
        with self.SQLConnector() as connector:
            table_name = f"files_hashs_{algorithm.lower()}_dbids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query_header = f"""
                        INSERT INTO {table_name} (hash_value)
                    """
                    insert_query_values = " ".join(
                        ["VALUES", ", ".join(["(%s)" for _ in hash_values])]
                    )
            insert_query = f"{insert_query_header} {insert_query_values}"
            try:
                connector.execute(insert_query, tuple(hash_values))
            except DatabaseDuplicateKeyError:
                toinsert = list[bytes]()
                for hash_value in hash_values:
                    if not self._check_db_hash_id_by_hash_value(hash_value, algorithm):
                        toinsert.append(hash_value)
                self.insert_db_hash_id_by_hash_values(toinsert, algorithm)
            except Exception as e:
                raise e

    def get_hash_value_by_db_hash_id(self, db_hash_id: int, algorithm: str) -> bytes:
        with self.SQLConnector() as connector:
            table_name = f"files_hashs_{algorithm.lower()}_dbids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT hash_value
                        FROM {table_name}
                        WHERE db_hash_id = %s
                    """
            query_result = connector.fetch_one(select_query, (db_hash_id,))
            if query_result is None:
                msg = f"Image hash for image ID {db_hash_id} does not exist."
                raise DatabaseKeyError(msg)
            else:
                hash_value = query_result[0]
        return hash_value

    def __get_hash_value_by_file_id(
        self, db_file_id: int, algorithm: str
    ) -> tuple | None:
        with self.SQLConnector() as connector:
            table_name = f"files_hashs_{algorithm.lower()}"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT db_hash_id
                        FROM {table_name}
                        WHERE db_file_id = %s
                    """
            query_result = connector.fetch_one(select_query, (db_file_id,))
        return query_result

    def _check_hash_value_by_file_id(self, db_file_id: int, algorithm: str) -> bool:
        query_result = self.__get_hash_value_by_file_id(db_file_id, algorithm)
        return query_result is not None

    def get_hash_value_by_file_id(self, db_file_id: int, algorithm: str) -> bytes:
        query_result = self.__get_hash_value_by_file_id(db_file_id, algorithm)
        if query_result is None:
            msg = f"Image hash for image ID {db_file_id} does not exist."
            raise DatabaseKeyError(msg)
        else:
            db_hash_id = query_result[0]

        return self.get_hash_value_by_db_hash_id(db_hash_id, algorithm)

    def _update_gallery_file_hash_by_db_hash_id(
        self, db_file_id: int, db_hash_id: int, algorithm: str
    ) -> None:
        with self.SQLConnector() as connector:
            table_name = f"files_hashs_{algorithm.lower()}"
            match self.config.database.sql_type.lower():
                case "mysql":
                    update_query = f"""
                        UPDATE {table_name} SET db_hash_id = %s WHERE db_file_id = %s
                    """
            connector.execute(update_query, (db_hash_id, db_file_id))


class H2HDBRemovedGalleries(H2HDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_removed_galleries_gids_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "removed_galleries_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (gid),
                            gid INT UNSIGNED NOT NULL
                        )
                    """
            connector.execute(query)
            logger.info(f"{table_name} table created.")

    def insert_removed_gallery_gid(self, gid: int) -> None:
        with self.SQLConnector() as connector:
            table_name = "removed_galleries_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query = f"""
                        INSERT INTO {table_name} (gid) VALUES (%s)
                    """
            if self._check_removed_gallery_gid(gid):
                logger.warning(f"Removed gallery GID {gid} already exists.")
            else:
                connector.execute(insert_query, (gid,))

    def __get_removed_gallery_gid(self, gid: int) -> tuple | None:
        with self.SQLConnector() as connector:
            table_name = "removed_galleries_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT gid
                        FROM {table_name}
                        WHERE gid = %s
                    """
            query_result = connector.fetch_one(select_query, (gid,))
        return query_result

    def _check_removed_gallery_gid(self, gid: int) -> bool:
        query_result = self.__get_removed_gallery_gid(gid)
        return query_result is not None

    def select_removed_gallery_gid(self, gid: int) -> int:
        query_result = self.__get_removed_gallery_gid(gid)
        if query_result is None:
            msg = f"Removed gallery GID {gid} does not exist."
            logger.error(msg)
            raise DatabaseKeyError(msg)
        else:
            gid = query_result[0]
            logger.warning(f"Removed gallery GID {gid} exists.")
        return gid


class H2HDB(
    H2HDBGalleriesInfos,
    H2HDBGalleriesComments,
    H2HDBGalleriesTags,
    H2HDBFiles,
    H2HDBRemovedGalleries,
):
    def _create_pending_gallery_removals_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "pending_gallery_removals"
            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name = "name"
                    column_name_parts, create_gallery_name_parts_sql = (
                        self.mysql_split_gallery_name_based_on_limit(column_name)
                    )
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY ({", ".join(column_name_parts)}),
                            {create_gallery_name_parts_sql},
                            full_name TEXT NOT NULL,
                            FULLTEXT (full_name)
                        )
                    """
            connector.execute(query)
            logger.info(f"{table_name} table created.")

    def _count_duplicated_files_hashs_sha512(self) -> int:
        with self.SQLConnector() as connector:
            table_name = "duplicated_files_hashs_sha512"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        SELECT COUNT(*)
                        FROM {table_name}
                    """
            query_result = connector.fetch_one(query)
        return query_result[0]

    def _create_duplicated_galleries_tables(self) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = """
                        CREATE VIEW IF NOT EXISTS duplicated_files_hashs_sha512 AS 
                            SELECT db_file_id, db_hash_id
                            FROM files_hashs_sha512
                            GROUP BY db_hash_id
                            HAVING COUNT(*) >= 3;
                        """
            connector.execute(query)

            match self.config.database.sql_type.lower():
                case "mysql":
                    query = """
                        CREATE VIEW IF NOT EXISTS duplicated_db_dbids AS 
                            SELECT
                                galleries_dbids.db_gallery_id AS db_gallery_id,
                                files_dbids.db_file_id AS db_file_id,
                                duplicated_files_hashs_sha512.db_hash_id AS db_hash_id,
                                galleries_tag_pairs_dbids.tag_value AS artist_value
                            FROM duplicated_files_hashs_sha512
                            LEFT JOIN files_hashs_sha512
                                ON duplicated_files_hashs_sha512.db_hash_id = files_hashs_sha512.db_hash_id
                            LEFT JOIN files_dbids
                                ON files_hashs_sha512.db_file_id = files_dbids.db_file_id
                            LEFT JOIN galleries_dbids
                                ON files_dbids.db_gallery_id = galleries_dbids.db_gallery_id
                            LEFT JOIN galleries_tags
                                ON galleries_dbids.db_gallery_id = galleries_tags.db_gallery_id
                            LEFT JOIN galleries_tag_pairs_dbids
                                ON galleries_tags.db_tag_pair_id = galleries_tag_pairs_dbids.db_tag_pair_id
                            WHERE galleries_tag_pairs_dbids.tag_name = 'artist';
                        """
            connector.execute(query)

            match self.config.database.sql_type.lower():
                case "mysql":
                    query = """
                        CREATE VIEW IF NOT EXISTS duplicated_count_artists_by_db_gallery_id AS
                            SELECT
                                COUNT(DISTINCT artist_value) AS artist_count,
                                db_gallery_id
                            FROM duplicated_db_dbids
                            GROUP BY db_gallery_id
                        """
            connector.execute(query)

            match self.config.database.sql_type.lower():
                case "mysql":
                    query = """
                        CREATE VIEW IF NOT EXISTS duplicated_hash_values_by_count_artist_ratio AS
                            SELECT files_hashs_sha512_dbids.hash_value AS hash_value
                            FROM duplicated_db_dbids
                            LEFT JOIN duplicated_count_artists_by_db_gallery_id
                                ON duplicated_db_dbids.db_gallery_id = duplicated_count_artists_by_db_gallery_id.db_gallery_id
                            LEFT JOIN files_hashs_sha512_dbids
                                ON duplicated_db_dbids.db_hash_id = files_hashs_sha512_dbids.db_hash_id
                            GROUP BY duplicated_db_dbids.db_hash_id
                            HAVING COUNT(DISTINCT duplicated_db_dbids.artist_value)/MAX(duplicated_count_artists_by_db_gallery_id.artist_count) > 1
                        """
            connector.execute(query)

    def insert_pending_gallery_removal(self, gallery_name: str) -> None:
        with self.SQLConnector() as connector:
            if self.check_pending_gallery_removal(gallery_name) is False:
                table_name = "pending_gallery_removals"
                if len(gallery_name) > FOLDER_NAME_LENGTH_LIMIT:
                    logger.error(
                        f"Gallery name '{gallery_name}' is too long. Must be {FOLDER_NAME_LENGTH_LIMIT} characters or less."
                    )
                    raise ValueError("Gallery name is too long.")
                gallery_name_parts = self._split_gallery_name(gallery_name)

                match self.config.database.sql_type.lower():
                    case "mysql":
                        column_name_parts, _ = (
                            self.mysql_split_gallery_name_based_on_limit("name")
                        )
                        insert_query = f"""
                            INSERT INTO {table_name} ({", ".join(column_name_parts)}, full_name)
                            VALUES ({", ".join(["%s" for _ in column_name_parts])}, %s)
                        """
                connector.execute(
                    insert_query, (*tuple(gallery_name_parts), gallery_name)
                )

    def check_pending_gallery_removal(self, gallery_name: str) -> bool:
        with self.SQLConnector() as connector:
            table_name = "pending_gallery_removals"
            gallery_name_parts = self._split_gallery_name(gallery_name)
            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name_parts, _ = self.mysql_split_gallery_name_based_on_limit(
                        "name"
                    )
                    select_query = f"""
                        SELECT full_name
                        FROM {table_name}
                        WHERE {" AND ".join([f"{part} = %s" for part in column_name_parts])}
                    """
            query_result = connector.fetch_one(select_query, tuple(gallery_name_parts))
            return query_result is not None

    def get_pending_gallery_removals(self) -> list[str]:
        with self.SQLConnector() as connector:
            table_name = "pending_gallery_removals"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT full_name
                        FROM {table_name}
                    """

            query_result = connector.fetch_all(select_query)
            pending_gallery_removals = [query[0] for query in query_result]
        return pending_gallery_removals

    def delete_pending_gallery_removal(self, gallery_name: str) -> None:
        with self.SQLConnector() as connector:
            table_name = "pending_gallery_removals"
            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name_parts, _ = self.mysql_split_gallery_name_based_on_limit(
                        "name"
                    )
                    delete_query = f"""
                        DELETE FROM {table_name} WHERE {" AND ".join([f"{part} = %s" for part in column_name_parts])}
                    """

            gallery_name_parts = self._split_gallery_name(gallery_name)
            connector.execute(delete_query, tuple(gallery_name_parts))

    def delete_pending_gallery_removals(self) -> None:
        pending_gallery_removals = self.get_pending_gallery_removals()
        for gallery_name in pending_gallery_removals:
            self.delete_gallery_file(gallery_name)
            self.delete_gallery(gallery_name)
            self.delete_pending_gallery_removal(gallery_name)

    def delete_gallery_file(self, gallery_name: str) -> None:
        # logger.info(f"Gallery images for '{gallery_name}' deleted.")
        pass

    def delete_gallery(self, gallery_name: str) -> None:
        with self.SQLConnector() as connector:
            if not self._check_galleries_dbids_by_gallery_name(gallery_name):
                logger.debug(f"Gallery '{gallery_name}' does not exist.")
                return

            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name_parts, _ = self.mysql_split_gallery_name_based_on_limit(
                        "name"
                    )
                    get_delete_gallery_id_query = f"""
                        DELETE FROM galleries_dbids
                        WHERE {" AND ".join([f"{part} = %s" for part in column_name_parts])}
                        """

            gallery_name_parts = self._split_gallery_name(gallery_name)
            connector.execute(get_delete_gallery_id_query, tuple(gallery_name_parts))
            logger.info(f"Gallery '{gallery_name}' deleted.")

    def optimize_database(self) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_table_name_query = f"""
                        SELECT TABLE_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                        WHERE REFERENCED_TABLE_SCHEMA = '{self.config.database.database}'
                    """
            table_names = connector.fetch_all(select_table_name_query)
            table_names = [t[0] for t in table_names]

            match self.config.database.sql_type.lower():
                case "mysql":
                    get_optimize_query = lambda x: "OPTIMIZE TABLE {x}".format(x=x)

            for table_name in table_names:
                connector.execute(get_optimize_query(table_name))
            logger.info("Database optimized.")

    def _create_todownload_gids_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "todownload_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (gid),
                            gid          INT UNSIGNED NOT NULL,
                            url          CHAR({self.innodb_index_prefix_limit}) NOT NULL
                        )
                    """
            connector.execute(query)
            logger.info(f"{table_name} table created.")

    def check_todownload_gid(self, gid: int, url: str) -> bool:
        with self.SQLConnector() as connector:
            table_name = "todownload_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    if url != "":
                        select_query = f"""
                            SELECT gid
                            FROM {table_name}
                            WHERE gid = %s AND url = %s
                        """
                        query_result = connector.fetch_one(select_query, (gid, url))
                    else:
                        select_query = f"""
                            SELECT gid
                            FROM {table_name}
                            WHERE gid = %s
                        """
                        query_result = connector.fetch_one(select_query, (gid,))
        return query_result is not None

    def insert_todownload_gid(self, gid: int, url: str) -> None:
        if not self.check_todownload_gid(gid, ""):
            with self.SQLConnector() as connector:
                table_name = "todownload_gids"
                match self.config.database.sql_type.lower():
                    case "mysql":
                        insert_query = f"""
                            INSERT INTO {table_name} (gid, url) VALUES (%s, %s)
                        """
                connector.execute(insert_query, (gid, url))
        else:
            if url != "":
                if not self.check_todownload_gid(gid, url):
                    self.update_todownload_gid(gid, url)

    def update_todownload_gid(self, gid: int, url: str) -> None:
        with self.SQLConnector() as connector:
            table_name = "todownload_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    update_query = f"""
                        UPDATE {table_name} SET url = %s WHERE gid = %s
                    """
            connector.execute(update_query, (url, gid))

    def remove_todownload_gid(self, gid: int) -> None:
        with self.SQLConnector() as connector:
            table_name = "todownload_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    delete_query = f"""
                        DELETE FROM {table_name} WHERE gid = %s
                    """
            connector.execute(delete_query, (gid,))

    def get_todownload_gids(self) -> list[tuple[int, str]]:
        with self.SQLConnector() as connector:
            table_name = "todownload_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT gid, url
                        FROM {table_name}
                    """
            query_result = connector.fetch_all(select_query)
        todownload_gids = [(query[0], query[1]) for query in query_result]
        return todownload_gids

    def create_main_tables(self) -> None:
        logger.debug("Creating main tables...")
        self._create_todownload_gids_table()
        self._create_pending_gallery_removals_table()
        self._create_galleries_names_table()
        self._create_galleries_gids_table()
        self._create_galleries_download_times_table()
        self._create_galleries_redownload_times_table()
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
        self._create_galleries_tags_table()
        self._create_duplicated_galleries_tables()
        logger.info("Main tables created.")

    def _insert_gallery_info(self, gallery_info_params: GalleryInfoParser) -> None:
        self.insert_pending_gallery_removal(gallery_info_params.gallery_name)

        self._insert_gallery_name(gallery_info_params.gallery_name)
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(
            gallery_info_params.gallery_name
        )

        with SQLThreadsList() as threads:
            threads.append(
                target=self._insert_gallery_gid,
                args=(db_gallery_id, gallery_info_params.gid),
            )
            threads.append(
                target=self._insert_gallery_title,
                args=(db_gallery_id, gallery_info_params.title),
            )
            threads.append(
                target=self._insert_upload_time,
                args=(db_gallery_id, gallery_info_params.upload_time),
            )
            threads.append(
                target=self._insert_gallery_comment,
                args=(db_gallery_id, gallery_info_params.galleries_comments),
            )
            threads.append(
                target=self._insert_gallery_upload_account,
                args=(db_gallery_id, gallery_info_params.upload_account),
            )
            threads.append(
                target=self._insert_download_time,
                args=(db_gallery_id, gallery_info_params.download_time),
            )
            threads.append(
                target=self._insert_access_time,
                args=(db_gallery_id, gallery_info_params.download_time),
            )
            threads.append(
                target=self._insert_modified_time,
                args=(db_gallery_id, gallery_info_params.modified_time),
            )
            threads.append(
                target=self._insert_gallery_files,
                args=(db_gallery_id, gallery_info_params.files_path),
            )

        file_pairs = list[FileInformation]()
        for file_path in gallery_info_params.files_path:
            db_file_id = self._get_db_file_id(db_gallery_id, file_path)
            absolute_file_path = os.path.join(
                gallery_info_params.gallery_folder, file_path
            )
            file_pairs.append(FileInformation(absolute_file_path, db_file_id))
        self._insert_gallery_file_hash_for_db_gallery_id(file_pairs)

        taglist = list[TagInformation]()
        for tag in gallery_info_params.tags:
            taglist.append(TagInformation(tag[0], tag[1]))
        self._insert_gallery_tags(db_gallery_id, taglist)

        self.delete_pending_gallery_removal(gallery_info_params.gallery_name)

    def _check_gallery_info_file_hash(
        self, gallery_info_params: GalleryInfoParser
    ) -> bool:
        if not self._check_galleries_dbids_by_gallery_name(
            gallery_info_params.gallery_name
        ):
            return False
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(
            gallery_info_params.gallery_name
        )

        if not self._check_db_file_id(db_gallery_id, GALLERY_INFO_FILE_NAME):
            return False
        gallery_info_file_id = self._get_db_file_id(
            db_gallery_id, GALLERY_INFO_FILE_NAME
        )
        absolute_file_path = os.path.join(
            gallery_info_params.gallery_folder, GALLERY_INFO_FILE_NAME
        )

        if not self._check_hash_value_by_file_id(
            gallery_info_file_id, COMPARISON_HASH_ALGORITHM
        ):
            return False
        original_hash_value = self.get_hash_value_by_file_id(
            gallery_info_file_id, COMPARISON_HASH_ALGORITHM
        )
        current_hash_value = hash_function_by_file(
            absolute_file_path, COMPARISON_HASH_ALGORITHM
        )
        issame = original_hash_value == current_hash_value
        return issame

    def _get_duplicated_hash_values_by_count_artist_ratio(self) -> list[bytes]:
        with self.SQLConnector() as connector:
            table_name = "duplicated_hash_values_by_count_artist_ratio"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT hash_value
                        FROM {table_name}
                    """

            query_result = connector.fetch_all(select_query)
        return [query[0] for query in query_result]

    def insert_gallery_info(self, gallery_folder: str) -> bool:
        gallery_info_params = parse_gallery_info(gallery_folder)
        is_thesame = self._check_gallery_info_file_hash(gallery_info_params)
        is_insert = is_thesame is False
        if is_insert:
            logger.debug(f"Inserting gallery '{gallery_info_params.gallery_name}'...")
            self.delete_gallery_file(gallery_info_params.gallery_name)
            self.delete_gallery(gallery_info_params.gallery_name)
            self._insert_gallery_info(gallery_info_params)
            logger.debug(f"Gallery '{gallery_info_params.gallery_name}' inserted.")
        return is_insert

    def compress_gallery_to_cbz(
        self, gallery_folder: str, exclude_hashs: list[bytes]
    ) -> bool:
        from .compress_gallery_to_cbz import (
            compress_images_and_create_cbz,
            calculate_hash_of_file_in_cbz,
        )

        gallery_info_params = parse_gallery_info(gallery_folder)
        match self.config.h2h.cbz_grouping:
            case "date-yyyy":
                upload_time = self.get_upload_time_by_gallery_name(
                    gallery_info_params.gallery_name
                )
                relative_cbz_directory = str(upload_time.year).rjust(4, "0")
            case "date-yyyy-mm":
                upload_time = self.get_upload_time_by_gallery_name(
                    gallery_info_params.gallery_name
                )
                relative_cbz_directory = os.path.join(
                    str(upload_time.year).rjust(4, "0"),
                    str(upload_time.month).rjust(2, "0"),
                )
            case "date-yyyy-mm-dd":
                upload_time = self.get_upload_time_by_gallery_name(
                    gallery_info_params.gallery_name
                )
                relative_cbz_directory = os.path.join(
                    str(upload_time.year).rjust(4, "0"),
                    str(upload_time.month).rjust(2, "0"),
                    str(upload_time.day).rjust(2, "0"),
                )
            case "flat":
                relative_cbz_directory = ""
            case _:
                raise ValueError(
                    f"Invalid cbz_grouping value: {self.config.h2h.cbz_grouping}"
                )
        cbz_directory = os.path.join(self.config.h2h.cbz_path, relative_cbz_directory)
        cbz_log_directory = os.path.join("cbz_path", relative_cbz_directory)
        cbz_tmp_directory = os.path.join(self.config.h2h.cbz_path, "tmp")

        cbz_log_path = os.path.join(
            cbz_log_directory, gallery_info_params.gallery_name + ".cbz"
        )
        cbz_path = os.path.join(
            cbz_directory, gallery_info_params.gallery_name + ".cbz"
        )
        if os.path.exists(cbz_path):
            db_gallery_id = self._get_db_gallery_id_by_gallery_name(
                gallery_info_params.gallery_name
            )
            gallery_info_file_id = self._get_db_file_id(
                db_gallery_id, GALLERY_INFO_FILE_NAME
            )
            original_hash_value = self.get_hash_value_by_file_id(
                gallery_info_file_id, COMPARISON_HASH_ALGORITHM
            )
            cbz_hash_value = calculate_hash_of_file_in_cbz(
                cbz_path, GALLERY_INFO_FILE_NAME, COMPARISON_HASH_ALGORITHM
            )
            if original_hash_value != cbz_hash_value:
                compress_images_and_create_cbz(
                    gallery_folder,
                    cbz_directory,
                    cbz_tmp_directory,
                    self.config.h2h.cbz_max_size,
                    exclude_hashs,
                )
                logger.info(f"CBZ '{cbz_log_path}' updated.")
                result = True
            else:
                result = False
        else:
            compress_images_and_create_cbz(
                gallery_folder,
                cbz_directory,
                cbz_tmp_directory,
                self.config.h2h.cbz_max_size,
                exclude_hashs,
            )
            logger.info(f"CBZ '{cbz_log_path}' created.")
            result = True
        return result

    def scan_current_galleries_folders(self) -> tuple[list[str], list[str]]:
        self.delete_pending_gallery_removals()

        with self.SQLConnector() as connector:
            tmp_table_name = "tmp_current_galleries"
            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name = "name"
                    column_name_parts, create_gallery_name_parts_sql = (
                        self.mysql_split_gallery_name_based_on_limit(column_name)
                    )
                    query = f"""
                        CREATE TEMPORARY TABLE IF NOT EXISTS {tmp_table_name} (
                            PRIMARY KEY ({", ".join(column_name_parts)}),
                            {create_gallery_name_parts_sql}
                        )
                    """

            connector.execute(query)
            logger.info(f"{tmp_table_name} table created.")

            match self.config.database.sql_type.lower():
                case "mysql":
                    column_name_parts, _ = self.mysql_split_gallery_name_based_on_limit(
                        "name"
                    )
                    insert_query = f"""
                        INSERT INTO {tmp_table_name}
                            ({", ".join(column_name_parts)})
                        VALUES ({", ".join(["%s" for _ in column_name_parts])})
                    """

            data = list[tuple]()
            current_galleries_folders = list[str]()
            current_galleries_names = list[str]()
            for root, _, files in os.walk(self.config.h2h.download_path):
                if GALLERY_INFO_FILE_NAME in files:
                    current_galleries_folders.append(root)
                    gallery_name = os.path.basename(current_galleries_folders[-1])
                    current_galleries_names.append(gallery_name)
                    gallery_name_parts = self._split_gallery_name(gallery_name)
                    data.append(tuple(gallery_name_parts))
            group_size = 5000
            it = iter(data)
            for _ in range(0, len(data), group_size):
                connector.execute_many(insert_query, list(islice(it, group_size)))

            match self.config.database.sql_type.lower():
                case "mysql":
                    fetch_query = f"""
                        SELECT CONCAT({",".join(["galleries_dbids."+column_name for column_name in column_name_parts])})
                        FROM galleries_dbids
                        LEFT JOIN {tmp_table_name} USING ({",".join(column_name_parts)})
                        WHERE {tmp_table_name}.{column_name_parts[0]} IS NULL
                    """
            removed_galleries = connector.fetch_all(fetch_query)
            if len(removed_galleries) > 0:
                removed_galleries = [gallery[0] for gallery in removed_galleries]

        for removed_gallery in removed_galleries:
            self.insert_pending_gallery_removal(removed_gallery)

        self.delete_pending_gallery_removals()

        return (current_galleries_folders, current_galleries_names)

    def _refresh_current_cbz_files(self, current_galleries_names: list[str]) -> None:
        from .compress_gallery_to_cbz import gallery_name_to_cbz_file_name

        current_cbzs = dict[str, str]()
        for root, _, files in os.walk(self.config.h2h.cbz_path):
            for file in files:
                current_cbzs[file] = root
        for key in set(current_cbzs.keys()) - set(
            gallery_name_to_cbz_file_name(name) for name in current_galleries_names
        ):
            os.remove(os.path.join(current_cbzs[key], key))
            logger.info(f"CBZ '{key}' removed.")
        logger.info("CBZ files refreshed.")

        while True:
            directory_removed = False
            for root, dirs, files in os.walk(self.config.h2h.cbz_path, topdown=False):
                if root == self.config.h2h.cbz_path:
                    continue
                if max([len(dirs), len(files)]) == 0:
                    directory_removed = True
                    os.rmdir(root)
                    logger.info(f"Directory '{root}' removed.")
            if not directory_removed:
                break
        logger.info("Empty directories removed.")

    def _refresh_current_files_hashs(self, algorithm: str) -> None:
        if algorithm not in HASH_ALGORITHMS:
            raise ValueError(
                f"Invalid hash algorithm: {algorithm} not in {HASH_ALGORITHMS}"
            )

        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    get_delete_db_hash_id_query = (
                        lambda x, y: f"""
                        DELETE FROM {y}
                        WHERE db_hash_id IN (
                                SELECT db_hash_id
                                FROM {x}
                                RIGHT JOIN {y} USING (db_hash_id)
                                WHERE {x}.db_hash_id IS NULL
                            )
                        """
                    )
            hash_table_name = f"files_hashs_{algorithm.lower()}"
            db_table_name = f"files_hashs_{algorithm.lower()}_dbids"
            connector.execute(
                get_delete_db_hash_id_query(hash_table_name, db_table_name)
            )

    def refresh_current_files_hashs(self):
        algorithmlist = list(HASH_ALGORITHMS.keys())
        with SQLThreadsList() as threads:
            for algorithm in algorithmlist:
                threads.append(
                    target=self._refresh_current_files_hashs,
                    args=(algorithm,),
                )

    def insert_h2h_download(self) -> None:
        self.delete_pending_gallery_removals()

        current_galleries_folders, current_galleries_names = (
            self.scan_current_galleries_folders()
        )

        self._refresh_current_cbz_files(current_galleries_names)

        logger.info("Inserting galleries...")
        if self.config.h2h.cbz_sort in ["upload_time", "download_time"]:
            logger.info(f"Sorting by {self.config.h2h.cbz_sort}...")
            current_galleries_folders = sorted(
                current_galleries_folders,
                key=lambda x: getattr(parse_gallery_info(x), self.config.h2h.cbz_sort),
                reverse=True,
            )
        elif "pages" in self.config.h2h.cbz_sort:
            logger.info("Sorting by pages...")
            zero_level = (
                max(1, int(self.config.h2h.cbz_sort.split("+")[-1]))
                if "+" in self.config.h2h.cbz_sort
                else 20
            )
            logger.info(
                f"Sorting by pages with adjustment based on {zero_level} pages..."
            )
            current_galleries_folders = sorted(
                current_galleries_folders,
                key=lambda x: abs(getattr(parse_gallery_info(x), "pages") - zero_level),
            )
        else:
            current_galleries_folders = sorted(
                current_galleries_folders,
                key=lambda x: getattr(parse_gallery_info(x), "pages"),
            )
        logger.info("Galleries sorted.")

        logger.info("Getting excluded hash values...")
        exclude_hashs = list[bytes]()
        previously_count_duplicated_files = 0
        logger.info("Excluded hash values obtained.")

        def calculate_exclude_hashs(
            previously_count_duplicated_files: int, exclude_hashs: list[bytes]
        ) -> tuple[int, list[bytes]]:
            logger.debug("Checking for duplicated files...")
            current_count_duplicated_files = self._count_duplicated_files_hashs_sha512()
            new_exclude_hashs = exclude_hashs
            if current_count_duplicated_files > previously_count_duplicated_files:
                logger.debug("Duplicated files found. Updating excluded hash values...")
                previously_count_duplicated_files = current_count_duplicated_files
                new_exclude_hashs = (
                    self._get_duplicated_hash_values_by_count_artist_ratio()
                )
                logger.info("Excluded hash values updated.")
            return previously_count_duplicated_files, new_exclude_hashs

        total_inserted_in_database = 0
        total_created_cbz = 0
        is_insert_limit_reached = False
        chunked_galleries_folders = chunk_list(
            current_galleries_folders, 100 * POOL_CPU_LIMIT
        )
        logger.info("Inserting galleries in parallel...")
        for gallery_chunk in chunked_galleries_folders:
            # Insert gallery info to database
            is_insert_list = run_in_parallel(
                self.insert_gallery_info,
                [(x,) for x in gallery_chunk],
            )
            if any(is_insert_list):
                logger.info("There are new galleries inserted in database.")
                is_insert_limit_reached |= True
                total_inserted_in_database += sum(is_insert_list)

            # Compress gallery to CBZ file
            if self.config.h2h.cbz_path != "":
                if any(is_insert_list):
                    previously_count_duplicated_files, exclude_hashs = (
                        calculate_exclude_hashs(
                            previously_count_duplicated_files, exclude_hashs
                        )
                    )
                is_new_list = run_in_parallel(
                    self.compress_gallery_to_cbz,
                    [(x, exclude_hashs) for x in gallery_chunk],
                )
                if any(is_new_list):
                    logger.info("There are new CBZ files created.")
                    total_created_cbz += sum(is_new_list)
        logger.info(
            f"Total galleries inserted in database: {total_inserted_in_database}"
        )
        logger.info(f"Total CBZ files created: {total_created_cbz}")

        logger.info("Cleaning up database...")
        self.refresh_current_files_hashs()

        if is_insert_limit_reached:
            logger.info("Refreshing database...")
            return self.insert_h2h_download()

    def get_komga_metadata(self, gallery_name: str) -> dict:
        metadata = dict[str, str | list[dict[str, str]]]()
        metadata["title"] = self.get_title_by_gallery_name(gallery_name)
        if self._check_gallery_comment_by_gallery_name(gallery_name):
            metadata["summary"] = self.get_comment_by_gallery_name(gallery_name)
        else:
            metadata["summary"] = ""
        upload_time = self.get_upload_time_by_gallery_name(gallery_name)
        metadata["releaseDate"] = "-".join(
            [
                str(upload_time.year),
                "{m:02d}".format(m=upload_time.month),
                "{d:02d}".format(d=upload_time.day),
            ]
        )
        tags = self.get_tag_pairs_by_gallery_name(gallery_name)
        metadata["authors"] = [
            {"name": value, "role": key} for key, value in tags if value != ""
        ]
        return metadata


def _insert_h2h_download(config: Config, gallery_paths: list) -> None:
    with H2HDB(config=config) as connector:
        for gallery_path in gallery_paths:
            connector.insert_gallery_info(gallery_path)
