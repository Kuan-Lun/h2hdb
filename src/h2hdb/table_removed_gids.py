from abc import ABCMeta

from .table_gids import H2HDBGalleriesIDs
from .h2hdb_spec import H2HDBAbstract
from .sql_connector import DatabaseKeyError


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
            self.logger.info(f"{table_name} table created.")

    def insert_removed_gallery_gid(self, gid: int) -> None:
        with self.SQLConnector() as connector:
            table_name = "removed_galleries_gids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query = f"""
                        INSERT INTO {table_name} (gid) VALUES (%s)
                    """
            if self._check_removed_gallery_gid(gid):
                self.logger.warning(f"Removed gallery GID {gid} already exists.")
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
            self.logger.error(msg)
            raise DatabaseKeyError(msg)
        else:
            gid = query_result[0]
            self.logger.warning(f"Removed gallery GID {gid} exists.")
        return gid
