__all__ = ["H2HDBGalleriesIDs", "H2HDBGalleriesGIDs"]

from .repository import BaseRepository, RepositoryContext
from .sql_connector import DatabaseKeyError


class H2HDBGalleriesIDs(BaseRepository):
    def _create_galleries_names_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_dbids"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    column_name_parts, create_gallery_name_parts_sql = (
                        self.mariadb_split_gallery_name_based_on_limit("name")
                    )
                    id_query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            db_gallery_id INT  UNSIGNED AUTO_INCREMENT,
                            {create_gallery_name_parts_sql},
                            UNIQUE real_primay_key ({", ".join(column_name_parts)})
                        )
                    """
                case "sqlite":
                    column_name_parts, create_gallery_name_parts_sql = (
                        self.sqlite_name_columns("name")
                    )
                    id_query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            db_gallery_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            {create_gallery_name_parts_sql},
                            UNIQUE ({", ".join(column_name_parts)})
                        )
                    """
            connector.execute(id_query)

            table_name = "galleries_names"
            match self.config.database.sql_type.lower():
                case "mariadb":
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
                case "sqlite":
                    name_query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            db_gallery_id INTEGER NOT NULL PRIMARY KEY
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            full_name TEXT NOT NULL
                        )
                    """
            connector.execute(name_query)

            match self.config.database.sql_type.lower():
                case "sqlite":
                    self._create_sqlite_fts5_sync(
                        connector, table_name, "full_name", "db_gallery_id"
                    )

            self.logger.info(f"{table_name} table created.")

    def _insert_gallery_name(self, gallery_name: str) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_dbids"
            gallery_name_parts = self._split_gallery_name(gallery_name)

            match self.config.database.sql_type.lower():
                case "mariadb":
                    column_name_parts, _ = (
                        self.mariadb_split_gallery_name_based_on_limit("name")
                    )
                case "sqlite":
                    column_name_parts, _ = self.sqlite_name_columns("name")
            insert_query = f"""
                INSERT INTO {table_name}
                    ({", ".join(column_name_parts)})
                VALUES ({", ".join(["%s" for _ in column_name_parts])})
            """
            connector.execute(insert_query, tuple(gallery_name_parts))

            db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)

            table_name = "galleries_names"
            insert_query = f"""
                INSERT INTO {table_name}
                    (db_gallery_id, full_name)
                VALUES (%s, %s)
            """
            connector.execute(insert_query, (db_gallery_id, gallery_name))

    def __get_db_gallery_id_by_gallery_name(self, gallery_name: str) -> tuple[int, ...]:
        with self.SQLConnector() as connector:
            table_name = "galleries_dbids"
            gallery_name_parts = self._split_gallery_name(gallery_name)

            match self.config.database.sql_type.lower():
                case "mariadb":
                    column_name_parts, _ = (
                        self.mariadb_split_gallery_name_based_on_limit("name")
                    )
                case "sqlite":
                    column_name_parts, _ = self.sqlite_name_columns("name")
            select_query = f"""
                SELECT db_gallery_id
                FROM {table_name}
                WHERE {" AND ".join([f"{part} = %s" for part in column_name_parts])}
            """

            query_result = connector.fetch_one(select_query, tuple(gallery_name_parts))
        return query_result

    def _check_galleries_dbids_by_gallery_name(self, gallery_name: str) -> bool:
        query_result = self.__get_db_gallery_id_by_gallery_name(gallery_name)
        return len(query_result) != 0

    def _get_db_gallery_id_by_gallery_name(self, gallery_name: str) -> int:
        query_result = self.__get_db_gallery_id_by_gallery_name(gallery_name)
        if query_result:
            db_gallery_id = int(query_result[0])
        else:
            self.logger.debug(f"Gallery name '{gallery_name}' does not exist.")
            raise DatabaseKeyError(f"Gallery name '{gallery_name}' does not exist.")
        return db_gallery_id


class H2HDBGalleriesGIDs(BaseRepository):
    """
    A class that handles the GIDs for galleries in the comic database.

    This repository is used to manage the GIDs for galleries

    Attributes:
        sql_type (str): The type of SQL database being used.
        sql_connection_params (SQLConnectorParams): The parameters for establishing the SQL connection.
        connector (SQLConnector): The SQL connector object.

    Methods:
        _create_galleries_gids_table: Creates the galleries_gids table.
        _insert_gallery_gid: Inserts the GID for the gallery name ID into the galleries_gids table.
        get_gid_by_gallery_name: Selects the GID for the gallery name from the database.
    """

    def __init__(
        self, context: RepositoryContext, gallery_ids: H2HDBGalleriesIDs
    ) -> None:
        super().__init__(context)
        self.gallery_ids = gallery_ids

    def _create_galleries_gids_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_gids"
            match self.config.database.sql_type.lower():
                case "mariadb":
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
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            db_gallery_id INTEGER NOT NULL PRIMARY KEY
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            gid INTEGER NOT NULL
                        )
                    """
            connector.execute(query)

            match self.config.database.sql_type.lower():
                case "sqlite":
                    connector.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_gid "
                        f"ON {table_name}(gid)"
                    )

            self.logger.info(f"{table_name} table created.")

    def _insert_gallery_gid(self, db_gallery_id: int, gid: int) -> None:
        with self.SQLConnector() as connector:
            table_name = "galleries_gids"
            insert_query = f"""
                INSERT INTO {table_name} (db_gallery_id, gid) VALUES (%s, %s)
            """
            connector.execute(insert_query, (db_gallery_id, gid))

    def _get_gid_by_db_gallery_id(self, db_gallery_id: int) -> int:
        with self.SQLConnector() as connector:
            table_name = "galleries_gids"
            select_query = f"""
                SELECT gid
                FROM {table_name}
                WHERE db_gallery_id = %s
            """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))

        if query_result:
            gid = int(query_result[0])
        else:
            msg = f"GID for gallery name ID {db_gallery_id} does not exist."
            self.logger.error(msg)
            raise DatabaseKeyError(msg)
        return gid

    def _get_db_gallery_id_by_gid(self, gid: int) -> int:
        with self.SQLConnector() as connector:
            table_name = "galleries_gids"
            select_query = f"""
                SELECT db_gallery_id
                FROM {table_name}
                WHERE gid = %s
            """
            query_result = connector.fetch_one(select_query, (gid,))

        if query_result:
            db_gallery_id = int(query_result[0])
        else:
            msg = f"Gallery name ID for GID {gid} does not exist."
            self.logger.error(msg)
            raise DatabaseKeyError(msg)
        return db_gallery_id

    def get_gid_by_gallery_name(self, gallery_name: str) -> int:
        db_gallery_id = self.gallery_ids._get_db_gallery_id_by_gallery_name(
            gallery_name
        )
        return self._get_gid_by_db_gallery_id(db_gallery_id)

    def get_gids(self) -> list[int]:
        with self.SQLConnector() as connector:
            table_name = "galleries_gids"
            select_query = f"""
                SELECT gid
                FROM {table_name}
            """
            query_result = connector.fetch_all(select_query)
        gids = [gid for gid, in query_result]
        return gids

    def check_gid_by_gid(self, gid: int) -> bool:
        with self.SQLConnector() as connector:
            table_name = "galleries_gids"
            select_query = f"""
                SELECT gid
                FROM {table_name}
                WHERE gid = %s
            """
            query_result = connector.fetch_one(select_query, (gid,))
        return len(query_result) != 0
