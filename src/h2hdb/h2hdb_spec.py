import math
import re
from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from functools import partial

from .config_loader import H2HDBConfig
from .logger import setup_logger
from .settings import (
    FILE_NAME_LENGTH_LIMIT,
    FOLDER_NAME_LENGTH_LIMIT,
)
from .sql_connector import SQLConnector as AbstractSQLConnector
from .sql_connector import SQLConnectorParams


class H2HDBAbstract(metaclass=ABCMeta):
    __slots__ = [
        "sql_connection_params",
        "mariadb_index_prefix_limit",
        "config",
        "SQLConnector",
        "logger",
    ]

    sql_connection_params: SQLConnectorParams
    SQLConnector: Callable[..., AbstractSQLConnector]

    def __init__(self, config: H2HDBConfig) -> None:
        """
        Initializes the H2HDBAbstract object.

        Raises:
            ValueError: If the SQL type is unsupported.
        """
        self.config = config
        self.logger = setup_logger(config.logger)

        # Set the appropriate connector based on the SQL type
        match self.config.database.sql_type.lower():
            case "mariadb":
                from .mariadb_connector import MariaDBConnector, MariaDBConnectorParams

                self.sql_connection_params = MariaDBConnectorParams(
                    host=self.config.database.host,
                    port=self.config.database.port,
                    user=self.config.database.user,
                    password=self.config.database.password,
                    database=self.config.database.database,
                )
                self.SQLConnector = partial(
                    MariaDBConnector, **self.sql_connection_params.model_dump()
                )
                self.mariadb_index_prefix_limit = 191
            case "sqlite":
                from .sqlite_connector import SQLiteConnector, SQLiteConnectorParams

                self.sql_connection_params = SQLiteConnectorParams(
                    database=self.config.database.database,
                )
                self.SQLConnector = partial(
                    SQLiteConnector, **self.sql_connection_params.model_dump()
                )
            case _:
                raise ValueError("Unsupported SQL type")

    def __enter__(self) -> H2HDBAbstract:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object | None,
    ) -> None:
        if exc_type is None:
            with self.SQLConnector() as connector:
                connector.commit()

    def _split_gallery_name(self, gallery_name: str) -> list[str]:
        match self.config.database.sql_type.lower():
            case "mariadb":
                return self._mariadb_split_name_value(gallery_name)
            case "sqlite":
                # No indexed key-prefix length limit, so the value isn't split
                # to match a multi-column schema -- it's stored in one column.
                return [gallery_name]
            case _:
                raise ValueError("Unsupported SQL type")

    def _mariadb_split_name_value(self, gallery_name: str) -> list[str]:
        size = FOLDER_NAME_LENGTH_LIMIT // self.mariadb_index_prefix_limit + (
            FOLDER_NAME_LENGTH_LIMIT % self.mariadb_index_prefix_limit > 0
        )
        gallery_name_parts = re.findall(
            f".{{1,{self.mariadb_index_prefix_limit}}}", gallery_name
        )
        gallery_name_parts += [""] * (size - len(gallery_name_parts))
        return gallery_name_parts

    def _mariadb_split_name_based_on_limit(
        self, name: str, name_length_limit: int
    ) -> tuple[list[str], str]:
        num_parts = math.ceil(name_length_limit / self.mariadb_index_prefix_limit)
        name_parts = [
            f"{name}_part{i} CHAR({self.mariadb_index_prefix_limit}) NOT NULL"
            for i in range(1, name_length_limit // self.mariadb_index_prefix_limit + 1)
        ]
        if name_length_limit % self.mariadb_index_prefix_limit > 0:
            name_parts.append(
                f"{name}_part{num_parts} CHAR({name_length_limit % self.mariadb_index_prefix_limit}) NOT NULL"
            )
        column_name_parts = [f"{name}_part{i}" for i in range(1, num_parts + 1)]
        create_name_parts_sql = ", ".join(name_parts)
        return column_name_parts, create_name_parts_sql

    def mariadb_split_gallery_name_based_on_limit(
        self, name: str
    ) -> tuple[list[str], str]:
        return self._mariadb_split_name_based_on_limit(name, FOLDER_NAME_LENGTH_LIMIT)

    def mariadb_split_file_name_based_on_limit(
        self, name: str
    ) -> tuple[list[str], str]:
        return self._mariadb_split_name_based_on_limit(name, FILE_NAME_LENGTH_LIMIT)

    def sqlite_name_columns(self, name: str) -> tuple[list[str], str]:
        """
        SQLite has no indexed key-prefix length limit (unlike MariaDB's InnoDB),
        so long names don't need to be split across multiple fixed-width columns.
        """
        return [name], f"{name} TEXT NOT NULL"

    def _create_sqlite_fts5_sync(
        self,
        connector: AbstractSQLConnector,
        table_name: str,
        column_name: str,
        rowid_column: str,
    ) -> None:
        """
        Creates an FTS5 external-content virtual table mirroring `table_name`, plus
        triggers to keep it in sync. This is SQLite's equivalent of MariaDB's
        `FULLTEXT` index -- a different mechanism, but the same searchable capability.
        """
        fts_table_name = f"{table_name}_fts"
        connector.execute(f"""
            CREATE VIRTUAL TABLE IF NOT EXISTS {fts_table_name} USING fts5(
                {column_name}, content='{table_name}', content_rowid='{rowid_column}'
            )
            """)
        connector.execute(f"""
            CREATE TRIGGER IF NOT EXISTS {table_name}_ai AFTER INSERT ON {table_name} BEGIN
                INSERT INTO {fts_table_name}(rowid, {column_name})
                VALUES (new.{rowid_column}, new.{column_name});
            END
            """)
        connector.execute(f"""
            CREATE TRIGGER IF NOT EXISTS {table_name}_ad AFTER DELETE ON {table_name} BEGIN
                INSERT INTO {fts_table_name}({fts_table_name}, rowid, {column_name})
                VALUES ('delete', old.{rowid_column}, old.{column_name});
            END
            """)
        connector.execute(f"""
            CREATE TRIGGER IF NOT EXISTS {table_name}_au AFTER UPDATE ON {table_name} BEGIN
                INSERT INTO {fts_table_name}({fts_table_name}, rowid, {column_name})
                VALUES ('delete', old.{rowid_column}, old.{column_name});
                INSERT INTO {fts_table_name}(rowid, {column_name})
                VALUES (new.{rowid_column}, new.{column_name});
            END
            """)

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
    def get_gids(self) -> list[int]:
        """
        Selects the GIDs from the database.

        Returns:
            list[int]: The list of GIDs.
        """
        pass

    @abstractmethod
    def check_gid_by_gid(self, gid: int) -> bool:
        """
        Checks if the GID exists in the database.

        Args:
            gid (int): The gallery GID.

        Returns:
            bool: True if the GID exists, False otherwise.
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
    def get_komga_metadata(
        self, gallery_name: str
    ) -> dict[str, str | list[dict[str, str]]]:
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
    def insert_todownload_gid(self, gid: int, url: str) -> None:
        """
        Inserts the GID to be downloaded into the database.

        Args:
            gid (int): The gallery GID.
            url (str): The gallery URL.
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

    @abstractmethod
    def get_pending_download_gids(self) -> list[int]:
        """
        Selects the pending download GIDs from the database.

        Returns:
            list[int]: The list of pending download GIDs.
        """
        pass

    @abstractmethod
    def insert_removed_gallery_gid(self, gid: int) -> None:
        """
        Inserts the removed gallery GID into the database.

        Args:
            gid (int): The gallery GID.
        """
        pass

    @abstractmethod
    def insert_todelete_gid(self, gid: int) -> None:
        """
        Inserts the GID to be deleted into the database.

        Args:
            gid (int): The gallery GID.
        """
        pass

    @abstractmethod
    def update_redownload_time_to_now_by_gid(self, gid: int) -> None:
        """
        Updates the redownload time to now by GID.

        Args:
            gid (int): The gallery GID.
        """
        pass
