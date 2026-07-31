from .repository import BaseRepository
from .sql_connector import DatabaseKeyError, SQLConnector

REMOVED_GALLERIES_TABLE = "removed_galleries_gids"


class H2HDBRemovedGalleries(BaseRepository):
    def _create_removed_galleries_gids_table(self) -> None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {REMOVED_GALLERIES_TABLE} (
                            PRIMARY KEY (gid),
                            gid INT UNSIGNED NOT NULL
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {REMOVED_GALLERIES_TABLE} (
                            gid INTEGER NOT NULL PRIMARY KEY
                        )
                    """
            connector.execute(query)
            self.logger.debug(
                f"Ensured database table exists: name={REMOVED_GALLERIES_TABLE}."
            )

    def insert_removed_gallery_gid(self, gid: int) -> None:
        with self.SQLConnector() as connector:
            with connector.transaction():
                self._insert_removed_gallery_gid_with_connector(connector, gid)

    def _insert_removed_gallery_gid_with_connector(
        self,
        connector: SQLConnector,
        gid: int,
    ) -> None:
        match self.config.database.sql_type.lower():
            case "mariadb":
                query = f"""
                    INSERT INTO {REMOVED_GALLERIES_TABLE} (gid)
                    VALUES (%s)
                    ON DUPLICATE KEY UPDATE gid = VALUES(gid)
                """
            case "sqlite":
                query = f"""
                    INSERT INTO {REMOVED_GALLERIES_TABLE} (gid)
                    VALUES (%s)
                    ON CONFLICT(gid) DO NOTHING
                """
        connector.execute(query, (gid,))

    def delete_removed_gallery_gid(self, gid: int) -> None:
        with self.SQLConnector() as connector:
            with connector.transaction():
                self._delete_removed_gallery_gid_with_connector(connector, gid)

    @staticmethod
    def _delete_removed_gallery_gid_with_connector(
        connector: SQLConnector,
        gid: int,
    ) -> None:
        connector.execute(
            f"""
            DELETE FROM {REMOVED_GALLERIES_TABLE}
            WHERE gid = %s
            """,
            (gid,),
        )

    @staticmethod
    def _get_removed_gallery_gid_with_connector(
        connector: SQLConnector,
        gid: int,
    ) -> tuple[int, ...]:
        return connector.fetch_one(
            f"""
            SELECT gid
            FROM {REMOVED_GALLERIES_TABLE}
            WHERE gid = %s
            """,
            (gid,),
        )

    def _get_removed_gallery_gid(self, gid: int) -> tuple[int, ...]:
        with self.SQLConnector() as connector:
            return self._get_removed_gallery_gid_with_connector(connector, gid)

    def _check_removed_gallery_gid(self, gid: int) -> bool:
        query_result = self._get_removed_gallery_gid(gid)
        return len(query_result) != 0

    def select_removed_gallery_gid(self, gid: int) -> int:
        query_result = self._get_removed_gallery_gid(gid)
        if query_result:
            gid = int(query_result[0])
        else:
            msg = f"Removed gallery GID {gid} does not exist."
            self.logger.error(msg)
            raise DatabaseKeyError(msg)
        return gid
