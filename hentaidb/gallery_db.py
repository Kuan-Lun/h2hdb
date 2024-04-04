__all__ = ["ComicDB"]


import re
import hashlib
import os
from abc import ABCMeta, abstractmethod

from hentaidb import parse_gallery_info
from .config_loader import config_loader
from .logger import logger
from .sql_connector import (
    MySQLConnector,
    SQLConnectorParams,
    DatabaseConfigurationError,
)

match config_loader["database"]["sql_type"].lower():
    case "mysql":
        from mysql.connector import Error as SQLError
        from mysql.connector.errors import IntegrityError as SQLDuplicateKeyError


def sql_type_to_name(sql_type: str) -> str:
    match sql_type.lower():
        case "mysql":
            name = "MySQL"
    return name


class ComicDBInitSQLConnector(metaclass=ABCMeta):
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
                self.SQLError = SQLError
        logger.debug("Error class set.")

    def __enter__(self) -> "ComicDBInitSQLConnector":
        self.connector.connect()
        match self.sql_type:
            case "mysql":
                self.connector.execute("START TRANSACTION")
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if exc_type is None:
            self.connector.commit()
        else:
            self.connector.rollback()
        self.connector.close()


class ComicDBCheckDatabaseSettings(ComicDBInitSQLConnector, metaclass=ABCMeta):
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


class ComaicDBDBGalleriesIDs(ComicDBInitSQLConnector, metaclass=ABCMeta):
    def _create_db_galleries_ids_table(self) -> None:
        table_name = "db_galleries_ids"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        gallery_name_part1 CHAR(191) NOT NULL,
                        gallery_name_part2 CHAR(64) NOT NULL,
                        id INT UNSIGNED AUTO_INCREMENT,
                        UNIQUE full_name (gallery_name_part1, gallery_name_part2),
                        PRIMARY KEY (id),
                        INDEX gallery_name_part2 (gallery_name_part2)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} table created.")

    def _create_galleries_names_view(self) -> None:
        table_name = "galleries_names"
        logger.debug(f"Creating {table_name} view...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE VIEW IF NOT EXISTS {table_name} AS
                    SELECT db_galleries_ids.id AS db_gallery_id, CONCAT(gallery_name_part1, gallery_name_part2) AS name
                    FROM db_galleries_ids
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} view created.")

    def _insert_gallery_name_and_return_db_gallery_id(self, gallery_name: str) -> int:
        logger.debug(f"Inserting gallery name '{gallery_name}'...")
        table_name = "db_galleries_ids"
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
                    INSERT INTO {table_name} (gallery_name_part1, gallery_name_part2) VALUES (%s, %s)
                """
                select_query = f"""
                    SELECT id FROM {table_name} WHERE gallery_name_part1 = %s AND gallery_name_part2 = %s
                """
        insert_query, select_query = (
            mullines2oneline(query) for query in (insert_query, select_query)
        )
        data = (gallery_name_part1, gallery_name_part2)

        logger.debug(f"Insert query: {insert_query}")
        try:
            self.connector.execute(insert_query, data)
            logger.debug(f"Gallery name '{gallery_name}' inserted.")
        except SQLDuplicateKeyError:
            logger.warning(
                f"Gallery name '{gallery_name}' already exists. Retrieving ID..."
            )
        logger.debug(f"Select query: {select_query}")
        gallery_name_id = self.connector.fetch_one(select_query, data)[0]
        logger.info(
            f"Gallery name '{gallery_name}' inserted with ID {gallery_name_id}."
        )
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


class ComaicDBGalleriesGIDs(ComicDBInitSQLConnector, metaclass=ABCMeta):
    def _create_galleries_gids_table(self) -> None:
        table_name = "galleries_gids"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        db_gallery_id INT UNSIGNED NOT NULL,
                        gid INT UNSIGNED NOT NULL,
                        PRIMARY KEY (db_gallery_id),
                        FOREIGN KEY (db_gallery_id) REFERENCES db_galleries_ids(id),
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
            self.connector.execute(insert_query, data)
            logger.debug(f"GID {gid} inserted for gallery name ID {gallery_name_id}.")
        except SQLDuplicateKeyError:
            logger.warning(
                f"GID {gid} for gallery name ID {gallery_name_id} already exists."
            )
        logger.info(f"GID {gid} inserted for gallery name ID {gallery_name_id}.")


class ComaicDBTimes(ComicDBInitSQLConnector, metaclass=ABCMeta):
    def _create_times_table(self, table_name: str) -> None:
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        db_gallery_id INT UNSIGNED NOT NULL,
                        time DATETIME NOT NULL,
                        PRIMARY KEY (db_gallery_id),
                        FOREIGN KEY (db_gallery_id) REFERENCES db_galleries_ids(id),
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

        isupdate = False
        logger.debug(f"Insert query: {insert_query}")
        try:
            self.connector.execute(insert_query, data)
            logger.debug(
                f"Time '{time}' inserted for gallery name ID {gallery_name_id}."
            )
        except SQLDuplicateKeyError:
            logger.warning(
                f"Time '{time}' for gallery name ID {gallery_name_id} already exists."
            )
            isupdate = True
        if isupdate:
            self._update_time(table_name, gallery_name_id, time)
        logger.info(f"Time '{time}' inserted for gallery name ID {gallery_name_id}.")

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

    def update_access_time(self, gallery_name_id: int, time: str) -> None:
        self._update_time("galleries_access_times", gallery_name_id, time)


class ComicDBGalleriesTitles(ComicDBInitSQLConnector, metaclass=ABCMeta):
    def _create_galleries_titles_parts_table(self) -> None:
        table_name = "galleries_titles_parts"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        db_gallery_id INT UNSIGNED NOT NULL,
                        title_part1 CHAR(191) NOT NULL,
                        title_part2 CHAR(191) NOT NULL,
                        title_part3 CHAR(191) NOT NULL,
                        PRIMARY KEY (db_gallery_id),
                        FOREIGN KEY (db_gallery_id) REFERENCES db_galleries_ids(id),
                        INDEX (title_part1, title_part2, title_part3, db_gallery_id)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} table created.")

    def _create_galleries_titles_view(self) -> None:
        table_name = "galleries_titles"
        logger.debug(f"Creating {table_name} view...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE VIEW IF NOT EXISTS {table_name} AS
                    SELECT db_gallery_id, CONCAT(title_part1, title_part2, title_part3) AS title
                    FROM galleries_titles_parts
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} view created.")

    def _insert_gallery_title(self, gallery_name_id: int, title: str) -> None:
        logger.debug(
            f"Inserting title '{title}' for gallery name ID {gallery_name_id}..."
        )
        table_name = "galleries_titles_parts"
        if len(title) > 573:
            logger.error(
                f"Title '{title}' is too long. Must be 573 characters or less."
            )
            raise ValueError("Title is too long.")
        title_part1 = title[0:191]
        title_part2 = title[191:382]
        title_part3 = title[382:573]
        logger.debug(
            f"Title '{title}' split into parts '{title_part1}', '{title_part2}', and '{title_part3}'"
        )

        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (db_gallery_id, title_part1, title_part2, title_part3) VALUES (%s, %s, %s, %s)
                """
                select_query = f"""
                    SELECT
                        title_part1, title_part2, title_part3
                    FROM {table_name}
                    WHERE db_gallery_id = %s AND title_part1 = %s AND title_part2 = %s AND title_part3 = %s
                """
        insert_query, select_query = (
            mullines2oneline(query) for query in (insert_query, select_query)
        )
        data = (gallery_name_id, title_part1, title_part2, title_part3)

        logger.debug(f"Insert query: {insert_query}")
        try:
            self.connector.execute(insert_query, data)
            logger.debug(
                f"Title '{title}' inserted for gallery name ID {gallery_name_id}."
            )
        except SQLDuplicateKeyError:
            logger.warning(
                f"Title '{title}' for gallery name ID {gallery_name_id} already exists."
            )
        logger.info(f"Title '{title}' inserted for gallery name ID {gallery_name_id}.")


class ComicDBUploadAccounts(ComicDBInitSQLConnector, metaclass=ABCMeta):
    def _create_upload_account_table(self) -> None:
        table_name = "upload_accounts"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        db_gallery_id INT UNSIGNED NOT NULL,
                        account CHAR(191) NOT NULL,
                        PRIMARY KEY (db_gallery_id),
                        FOREIGN KEY (db_gallery_id) REFERENCES db_galleries_ids(id),
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
        table_name = "upload_accounts"
        if len(account) > 191:
            logger.error(
                f"Upload account '{account}' is too long. Must be 191 characters or less."
            )
            raise ValueError("Upload account is too long.")

        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (db_gallery_id, account) VALUES (%s, %s)
                """
                select_query = f"""
                    SELECT account FROM {table_name} WHERE db_gallery_id = %s AND account = %s
                """
        insert_query, select_query = (
            mullines2oneline(query) for query in (insert_query, select_query)
        )
        data = (gallery_name_id, account)

        logger.debug(f"Insert query: {insert_query}")
        try:
            self.connector.execute(insert_query, data)
            logger.debug(
                f"Upload account '{account}' inserted for gallery name ID {gallery_name_id}."
            )
        except SQLDuplicateKeyError:
            logger.warning(
                f"Upload account '{account}' for gallery name ID {gallery_name_id} already exists."
            )
        logger.info(
            f"Upload account '{account}' inserted for gallery name ID {gallery_name_id}."
        )


class ComicDBGalleriesInfos(
    ComaicDBDBGalleriesIDs,
    ComicDBGalleriesTitles,
    ComaicDBGalleriesGIDs,
    ComicDBUploadAccounts,
    ComaicDBTimes,
    ComicDBCheckDatabaseSettings,
):
    def _create_galleries_infos_view(self) -> None:
        logger.debug("Creating galleries_infos view...")
        match self.sql_type:
            case "mysql":
                query = """
                    CREATE VIEW IF NOT EXISTS galleries_infos AS
                    SELECT
                        galleries_names.db_gallery_id AS db_gallery_id,
                        galleries_names.name AS name,
                        galleries_titles.title AS title,
                        galleries_gids.gid AS gid,
                        upload_accounts.account AS upload_account,
                        galleries_upload_times.time AS galleries_upload_time,
                        galleries_download_times.time AS galleries_download_time,
                        galleries_modified_times.time AS galleries_modified_time,
                        galleries_access_times.time AS galleries_access_time
                    FROM
                        galleries_names
                        LEFT JOIN galleries_titles USING (db_gallery_id)
                        LEFT JOIN galleries_gids USING (db_gallery_id)
                        LEFT JOIN upload_accounts USING (db_gallery_id)
                        LEFT JOIN galleries_upload_times USING (db_gallery_id)
                        LEFT JOIN galleries_download_times USING (db_gallery_id)
                        LEFT JOIN galleries_modified_times USING (db_gallery_id)
                        LEFT JOIN galleries_access_times USING (db_gallery_id)
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info("galleries_infos view created.")


class ComicDBGalleriesComments(ComicDBInitSQLConnector):
    def _create_galleries_comments_table(self) -> None:
        table_name = "galleries_comments"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        db_gallery_id INT UNSIGNED NOT NULL,
                        comment TEXT NOT NULL,
                        PRIMARY KEY (db_gallery_id),
                        FOREIGN KEY (db_gallery_id) REFERENCES db_galleries_ids(id),
                        FULLTEXT (Comment)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} table created.")

    def _insert_gallery_comment(self, gallery_name_id: int, comment: str) -> None:
        logger.debug(
            f"Inserting uploader comment for gallery name ID {gallery_name_id}..."
        )
        table_name = "galleries_comments"
        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (db_gallery_id, comment) VALUES (%s, %s)
                """
        insert_query = mullines2oneline(insert_query)
        data = (gallery_name_id, comment)

        isupdater = False
        logger.debug(f"Insert query: {insert_query}")
        try:
            self.connector.execute(insert_query, data)
        except SQLDuplicateKeyError:
            logger.warning(
                f"Uploader comment for gallery name ID {gallery_name_id} already exists."
            )
            isupdater = True
        if isupdater:
            logger.warning(
                f"Uploader comment already exists for gallery name ID {gallery_name_id}. Updating..."
            )
            self._update_gallery_comment(gallery_name_id, comment)
        logger.info(f"Uploader comment inserted for gallery name ID {gallery_name_id}.")

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


class ComicDBGalleriesTags(ComicDBInitSQLConnector):
    def _create_galleries_tags_table(self, tag_name: str) -> None:
        table_name = f"galleries_tags_{tag_name}"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        db_gallery_id INT UNSIGNED NOT NULL,
                        tag CHAR(191) NOT NULL,
                        PRIMARY KEY (db_gallery_id),
                        FOREIGN KEY (db_gallery_id) REFERENCES db_galleries_ids(id),
                        INDEX (tag, db_gallery_id)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} table created.")

    def insert_gallery_tag(
        self, gallery_name_id: int, tag_name: str, tag_value: str
    ) -> None:
        logger.debug(f"Inserting tag '{tag_name}'...")
        table_name = f"galleries_tags_{tag_name}"
        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (db_gallery_id, tag) VALUES (%s, %s)
                """
        insert_query = mullines2oneline(insert_query)
        data = (gallery_name_id, tag_value)

        self._create_galleries_tags_table(tag_name)
        logger.debug(f"Insert query: {insert_query}")
        try:
            self.connector.execute(insert_query, data)
        except SQLDuplicateKeyError:
            logger.warning(
                f"Tag '{tag_name}' for gallery name ID {gallery_name_id} already exists."
            )
        logger.info(f"Tag '{tag_name}' inserted.")


class ComicDB(ComicDBGalleriesInfos, ComicDBGalleriesComments, ComicDBGalleriesTags):
    def _create_galleries_files_ids_table(self) -> None:
        table_name = f"db_files_ids"
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        db_gallery_id INT UNSIGNED NOT NULL,
                        file_name_part1 CHAR(191) NOT NULL,
                        file_name_part2 CHAR(64) NOT NULL,
                        db_file_id INT UNSIGNED AUTO_INCREMENT,
                        PRIMARY KEY (db_file_id),
                        UNIQUE File (db_gallery_id, file_name_Part1, file_name_part2),
                        FOREIGN KEY (db_gallery_id) REFERENCES db_galleries_ids(id)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} table created.")

    def _insert_gallery_file_and_return_id(
        self, gallery_name_id: int, file: str
    ) -> int:
        logger.debug(
            f"Inserting image ID for gallery name ID {gallery_name_id} and file '{file}'..."
        )
        table_name = "db_files_ids"
        if len(file) > 255:
            logger.error(f"File '{file}' is too long. Must be 255 characters or less.")
            raise ValueError("File is too long.")
        file_part1 = file[0:191]
        file_part2 = file[191:255]
        logger.debug(
            f"File '{file}' split into parts '{file_part1}' and '{file_part2}'"
        )

        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (db_gallery_id, file_name_part1, file_name_part2) VALUES (%s, %s, %s)
                """
                select_query = f"""
                    SELECT db_file_id FROM {table_name} WHERE db_gallery_id = %s AND file_name_part1 = %s AND file_name_part2 = %s
                """
        insert_query, select_query = (
            mullines2oneline(query) for query in (insert_query, select_query)
        )
        data = (gallery_name_id, file_part1, file_part2)

        logger.debug(f"Insert query: {insert_query}")
        try:
            self.connector.execute(insert_query, data)
            logger.debug(
                f"Image ID inserted for gallery name ID {gallery_name_id} and file '{file}'."
            )
        except SQLDuplicateKeyError:
            logger.warning(
                f"Image ID for gallery name ID {gallery_name_id} and file '{file}' already exists."
            )
        logger.debug(f"Select query: {select_query}")
        gallery_image_id = self.connector.fetch_one(select_query, data)[0]
        logger.info(
            f"Image ID inserted for gallery name ID {gallery_name_id} and file '{file}'."
        )
        return gallery_image_id

    def _creage_galleries_files_ids_view(self) -> None:
        table_name = "files_ids"
        logger.debug(f"Creating {table_name} view...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE VIEW IF NOT EXISTS {table_name} AS
                    SELECT
                        db_files_ids.db_gallery_id AS db_gallery_id,
                        galleries_names.name AS gallery_name,
                        CONCAT(File_Name_Part1, File_Name_Part2) AS file,
                        db_file_id
                    FROM db_files_ids
                    LEFT JOIN galleries_names USING (db_gallery_id)
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} view created.")

    def _create_galleries_files_hashs_table(self, algorithm: str) -> None:
        table_name = "images_hashs_%s" % algorithm.lower()
        logger.debug(f"Creating {table_name} table...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        db_file_id INT UNSIGNED NOT NULL,
                        hash_value CHAR(128) NOT NULL,
                        PRIMARY KEY (db_file_id),
                        FOREIGN KEY (db_file_id) REFERENCES db_files_ids(db_file_id),
                        INDEX (hash_value, db_file_id)
                    )
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} table created.")

    def _create_galleries_files_sha224_table(self) -> None:
        self._create_galleries_files_hashs_table("sha224")

    def _create_galleries_files_sha256_table(self) -> None:
        self._create_galleries_files_hashs_table("sha256")

    def _create_galleries_files_sha384_table(self) -> None:
        self._create_galleries_files_hashs_table("sha384")

    def _create_galleries_files_sha1_table(self) -> None:
        self._create_galleries_files_hashs_table("sha1")

    def _create_galleries_files_sha512_table(self) -> None:
        self._create_galleries_files_hashs_table("sha512")

    def _create_galleries_files_sha3_224_table(self) -> None:
        self._create_galleries_files_hashs_table("sha3_224")

    def _create_galleries_files_sha3_256_table(self) -> None:
        self._create_galleries_files_hashs_table("sha3_256")

    def _create_galleries_files_sha3_384_table(self) -> None:
        self._create_galleries_files_hashs_table("sha3_384")

    def _create_galleries_files_sha3_512_table(self) -> None:
        self._create_galleries_files_hashs_table("sha3_512")

    def _create_galleries_files_blake2b_table(self) -> None:
        self._create_galleries_files_hashs_table("blake2b")

    def _create_galleries_files_blake2s_table(self) -> None:
        self._create_galleries_files_hashs_table("blake2s")

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
        table_name = "images_hashs"
        logger.debug(f"Creating {table_name} view...")
        match self.sql_type:
            case "mysql":
                query = f"""
                    CREATE VIEW IF NOT EXISTS {table_name} AS
                    SELECT
                        images_hashs_sha224.db_file_id AS db_file_id,
                        images_hashs_sha224.hash_value AS sha224,
                        images_hashs_sha256.hash_value AS sha256,
                        images_hashs_sha384.hash_value AS sha384,
                        images_hashs_sha1.hash_value AS sha1,
                        images_hashs_sha512.hash_value AS sha512,
                        images_hashs_sha3_224.hash_value AS sha3_224,
                        images_hashs_sha3_256.hash_value AS sha3_256,
                        images_hashs_sha3_384.hash_value AS sha3_384,
                        images_hashs_sha3_512.hash_value AS sha3_512,
                        images_hashs_blake2b.hash_value AS blake2b,
                        images_hashs_blake2s.hash_value AS blake2s
                    FROM
                        images_hashs_sha224
                        LEFT JOIN images_hashs_sha256 USING (db_file_id)
                        LEFT JOIN images_hashs_sha384 USING (db_file_id)
                        LEFT JOIN images_hashs_sha1 USING (db_file_id)
                        LEFT JOIN images_hashs_sha512 USING (db_file_id)
                        LEFT JOIN images_hashs_sha3_224 USING (db_file_id)
                        LEFT JOIN images_hashs_sha3_256 USING (db_file_id)
                        LEFT JOIN images_hashs_sha3_384 USING (db_file_id)
                        LEFT JOIN images_hashs_sha3_512 USING (db_file_id)
                        LEFT JOIN images_hashs_blake2b USING (db_file_id)
                        LEFT JOIN images_hashs_blake2s USING (db_file_id)
                """
        query = mullines2oneline(query)
        logger.debug(f"Query: {query}")
        self.connector.execute(query)
        logger.info(f"{table_name} view created.")

    def _insert_gallery_file_hash(
        self, image_id: int, file_path: str, file_content: bytes, algorithm: str
    ) -> None:
        logger.debug(
            f"Inserting image hash for image ID {image_id} and file '{file_path}'..."
        )
        table_name = f"images_hashs_{algorithm.lower()}"
        hash_function = lambda x: getattr(hashlib, algorithm.lower())(x).hexdigest()
        hash_value = hash_function(file_content)
        logger.debug(f"Hash value: {hash_value}")

        match self.sql_type:
            case "mysql":
                insert_query = f"""
                    INSERT INTO {table_name} (db_file_id, hash_value) VALUES (%s, %s)
                """
                select_query = f"""
                    SELECT hash_value FROM {table_name} WHERE db_file_id = %s AND hash_value = %s
                """
        insert_query, select_query = (
            mullines2oneline(query) for query in (insert_query, select_query)
        )
        data = (image_id, hash_value)

        logger.debug(f"Insert query: {insert_query}")
        try:
            self.connector.execute(insert_query, data)
            logger.debug(
                f"Image hash '{hash_value}' inserted for image ID {image_id} and file '{file_path}'."
            )
        except SQLDuplicateKeyError:
            logger.warning(
                f"Image hash '{hash_value}' for image ID {image_id} and file '{file_path}' already exists."
            )
        logger.info(
            f"Image hash '{hash_value}' inserted for image ID {image_id} and file '{file_path}'."
        )

    def _insert_gallery_file_sha224(
        self, image_id: int, file_path: str, file_content: bytes
    ) -> None:
        self._insert_gallery_file_hash(image_id, file_path, file_content, "sha224")

    def _insert_gallery_file_sha256(
        self, image_id: int, file_path: str, file_content: bytes
    ) -> None:
        self._insert_gallery_file_hash(image_id, file_path, file_content, "sha256")

    def _insert_gallery_file_sha384(
        self, image_id: int, file_path: str, file_content: bytes
    ) -> None:
        self._insert_gallery_file_hash(image_id, file_path, file_content, "sha384")

    def _insert_gallery_file_sha1(
        self, image_id: int, file_path: str, file_content: bytes
    ) -> None:
        self._insert_gallery_file_hash(image_id, file_path, file_content, "sha1")

    def _insert_gallery_file_sha512(
        self, image_id: int, file_path: str, file_content: bytes
    ) -> None:
        self._insert_gallery_file_hash(image_id, file_path, file_content, "sha512")

    def _insert_gallery_file_sha3_224(
        self, image_id: int, file_path: str, file_content: bytes
    ) -> None:
        self._insert_gallery_file_hash(image_id, file_path, file_content, "sha3_224")

    def _insert_gallery_file_sha3_256(
        self, image_id: int, file_path: str, file_content: bytes
    ) -> None:
        self._insert_gallery_file_hash(image_id, file_path, file_content, "sha3_256")

    def _insert_gallery_file_sha3_384(
        self, image_id: int, file_path: str, file_content: bytes
    ) -> None:
        self._insert_gallery_file_hash(image_id, file_path, file_content, "sha3_384")

    def _insert_gallery_file_sha3_512(
        self, image_id: int, file_path: str, file_content: bytes
    ) -> None:
        self._insert_gallery_file_hash(image_id, file_path, file_content, "sha3_512")

    def _insert_gallery_file_blake2b(
        self, image_id: int, file_path: str, file_content: bytes
    ) -> None:
        self._insert_gallery_file_hash(image_id, file_path, file_content, "blake2b")

    def _insert_gallery_file_blake2s(
        self, image_id: int, file_path: str, file_content: bytes
    ) -> None:
        self._insert_gallery_file_hash(image_id, file_path, file_content, "blake2s")

    def _update_gallery_file_hash(
        self, image_id: int, file_path: str, hash_value: str
    ) -> None:
        logger.debug(
            f"Updating image hash '{hash_value}' for image ID {image_id} and file '{file_path}'..."
        )
        table_name = "images_hashs"
        match self.sql_type:
            case "mysql":
                update_query = f"""
                    UPDATE {table_name} SET Hash = %s WHERE DB_Image_ID = %s
                """
        update_query = mullines2oneline(update_query)
        data = (hash_value, image_id)

        logger.debug(f"Update query: {update_query}")
        self.connector.execute(update_query, data)
        logger.info(
            f"Image hash '{hash_value}' updated for image ID {image_id} and file '{file_path}'."
        )

    def delete_gallery_image(self, gallery_name: str) -> None:
        logger.debug(f"Deleting gallery '{gallery_name}'...")
        match self.sql_type:
            case "mysql":
                select_table_name_query = f"""
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE
                        REFERENCED_TABLE_SCHEMA = '{config_loader["database"]["database"]}' AND
                        REFERENCED_TABLE_NAME = 'db_files_ids' AND
                        REFERENCED_COLUMN_NAME = 'db_file_id'
                """
                delete_image_id_query = """
                    DELETE FROM %s
                    WHERE
                        db_file_id in (
                            SELECT db_file_id
                            FROM db_files_ids
                            WHERE db_gallery_id = (
                                SELECT db_gallery_id
                                FROM galleries_names
                                WHERE name = '%s'
                            )
                        )
                """
        select_table_name_query, delete_image_id_query = (
            mullines2oneline(query)
            for query in (select_table_name_query, delete_image_id_query)
        )

        logger.debug(f"Select query: {select_table_name_query}")
        table_names = self.connector.fetch_all(select_table_name_query)
        print(table_names)
        table_names = [t[0] for t in table_names] + ["db_files_ids"]
        print(table_names)
        logger.debug(f"Table names: {table_names}")

        logger.debug(f"Delete query: {delete_image_id_query}")
        for table_name in table_names:
            data = (
                table_name,
                gallery_name,
            )
            self.connector.execute(delete_image_id_query % data)
        logger.info(f"Gallery '{gallery_name}' deleted.")

    def delete_gallery(self, gallery_name: str) -> None:
        logger.debug(f"Deleting gallery '{gallery_name}'...")
        match self.sql_type:
            case "mysql":
                select_table_name_query = f"""
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE
                        REFERENCED_TABLE_SCHEMA = '{config_loader["database"]["database"]}' AND
                        REFERENCED_TABLE_NAME = 'db_galleries_ids' AND
                        REFERENCED_COLUMN_NAME = 'id'
                """
                delete_gallery_id_query = """
                    DELETE FROM %s
                    WHERE
                        %s = (
                            SELECT db_gallery_id
                            FROM galleries_names
                            WHERE name = '%s'
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
        for table_name in table_names:
            data = (table_name, "db_gallery_id", gallery_name)
            self.connector.execute(delete_gallery_id_query % data)
        data = ("db_galleries_ids", "id", gallery_name)
        self.connector.execute(delete_gallery_id_query % data)
        logger.info(f"Gallery '{gallery_name}' deleted.")

    def create_main_tables(self) -> None:
        self._create_db_galleries_ids_table()
        self._create_galleries_names_view()
        self._create_galleries_gids_table()
        self._create_galleries_download_times_table()
        self._create_galleries_upload_times_table()
        self._create_galleries_modified_times_table()
        self._create_galleries_access_times_table()
        self._create_galleries_titles_parts_table()
        self._create_galleries_titles_view()
        self._create_upload_account_table()
        self._create_galleries_comments_table()
        self._create_galleries_infos_view()
        self._create_galleries_files_ids_table()
        self._creage_galleries_files_ids_view()
        self._create_galleries_files_hashs_tables()
        self._create_gallery_image_hash_view()

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
                image_id = self._insert_gallery_file_and_return_id(id, file_path)
                absolute_file_path = os.path.join(gallery_folder, file_path)
                with open(absolute_file_path, "rb") as f:
                    file_content = f.read()
                image_hash_insert_params = (image_id, absolute_file_path, file_content)
                self._insert_gallery_file_sha224(*image_hash_insert_params)
                self._insert_gallery_file_sha256(*image_hash_insert_params)
                self._insert_gallery_file_sha384(*image_hash_insert_params)
                self._insert_gallery_file_sha1(*image_hash_insert_params)
                self._insert_gallery_file_sha512(*image_hash_insert_params)
                self._insert_gallery_file_sha3_224(*image_hash_insert_params)
                # raise ValueError  # This is a test to see if the transaction is rolled back.
                self._insert_gallery_file_sha3_256(*image_hash_insert_params)
                self._insert_gallery_file_sha3_384(*image_hash_insert_params)
                self._insert_gallery_file_sha3_512(*image_hash_insert_params)
                self._insert_gallery_file_blake2b(*image_hash_insert_params)
                self._insert_gallery_file_blake2s(*image_hash_insert_params)

            # When the corresponding Tag_{tag_name} table does not exist, a table creation operation will be performed.
            # This will commit and create a new TRANSACTION.
            for tag_name, tag_value in gallery_info.tags.items():
                self.insert_gallery_tag(id, tag_name, tag_value)
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
