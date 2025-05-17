__all__ = ["H2HDBGalleriesIDs", "H2HDBGalleriesGIDs"]

from abc import ABCMeta

from .h2hdb_spec import H2HDBAbstract
from .sql_connector import DatabaseKeyError


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
            self.logger.info(f"{table_name} table created.")

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

    def __get_db_gallery_id_by_gallery_name(self, gallery_name: str) -> tuple:
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
        return len(query_result) != 0

    def _get_db_gallery_id_by_gallery_name(self, gallery_name: str) -> int:
        query_result = self.__get_db_gallery_id_by_gallery_name(gallery_name)
        if query_result:
            db_gallery_id = query_result[0]
        else:
            self.logger.debug(f"Gallery name '{gallery_name}' does not exist.")
            raise DatabaseKeyError(f"Gallery name '{gallery_name}' does not exist.")
        return db_gallery_id

    def _get_db_gallery_id_by_gid(self, gid: int) -> int:
        with self.SQLConnector() as connector:
            table_name = "galleries_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT db_gallery_id
                        FROM {table_name}
                        WHERE gid = %s
                    """
            query_result = connector.fetch_one(select_query, (gid,))

        if query_result:
            db_gallery_id = query_result[0]
        else:
            msg = f"Gallery name ID for GID {gid} does not exist."
            self.logger.error(msg)
            raise DatabaseKeyError(msg)
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
            self.logger.info(f"{table_name} table created.")

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

        if query_result:
            gid = query_result[0]
        else:
            msg = f"GID for gallery name ID {db_gallery_id} does not exist."
            self.logger.error(msg)
            raise DatabaseKeyError(msg)
        return gid

    def get_gid_by_gallery_name(self, gallery_name: str) -> int:
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        return self._get_gid_by_db_gallery_id(db_gallery_id)

    def get_gids(self) -> list[int]:
        with self.SQLConnector() as connector:
            table_name = "galleries_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
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
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT gid
                        FROM {table_name}
                        WHERE gid = %s
                    """
            query_result = connector.fetch_one(select_query, (gid,))
        return len(query_result) != 0
