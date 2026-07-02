import datetime
from typing import cast

from .repository import BaseRepository, RepositoryContext
from .table_gids import H2HDBGalleriesIDs


class H2HDBCBZIntegrity(BaseRepository):
    def __init__(
        self, context: RepositoryContext, gallery_ids: H2HDBGalleriesIDs
    ) -> None:
        super().__init__(context)
        self.gallery_ids = gallery_ids

    def _create_cbz_hashes_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "cbz_hashes"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_gallery_id INT UNSIGNED NOT NULL,
                            sha256        BINARY(32)   NOT NULL,
                            UNIQUE INDEX (sha256)
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            db_gallery_id INTEGER NOT NULL PRIMARY KEY
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            sha256        BLOB    NOT NULL
                        )
                    """
            connector.execute(query)

            match self.config.database.sql_type.lower():
                case "sqlite":
                    connector.execute(
                        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_sha256 "
                        f"ON {table_name}(sha256)"
                    )

        self.logger.info(f"{table_name} table created.")

    def _create_cbz_verifications_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "cbz_verifications"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_gallery_id      INT UNSIGNED NOT NULL,
                            last_verified_time DATETIME     NOT NULL,
                            INDEX (last_verified_time)
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            db_gallery_id      INTEGER   NOT NULL PRIMARY KEY
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            last_verified_time TIMESTAMP NOT NULL
                        )
                    """
            connector.execute(query)

            match self.config.database.sql_type.lower():
                case "sqlite":
                    connector.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_last_verified_time "
                        f"ON {table_name}(last_verified_time)"
                    )

        self.logger.info(f"{table_name} table created.")

    def _insert_hash(self, db_gallery_id: int, sha256: bytes) -> None:
        with self.SQLConnector() as connector:
            query = """
                INSERT INTO cbz_hashes (db_gallery_id, sha256) VALUES (%s, %s)
            """
            connector.execute(query, (db_gallery_id, sha256))

    def _update_hash(self, db_gallery_id: int, sha256: bytes) -> None:
        with self.SQLConnector() as connector:
            query = """
                UPDATE cbz_hashes SET sha256 = %s WHERE db_gallery_id = %s
            """
            connector.execute(query, (sha256, db_gallery_id))

    def _get_hash(self, db_gallery_id: int) -> bytes | None:
        with self.SQLConnector() as connector:
            select_query = """
                SELECT sha256 FROM cbz_hashes WHERE db_gallery_id = %s
            """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
        return bytes(query_result[0]) if query_result else None

    def _delete_hash(self, db_gallery_id: int) -> None:
        with self.SQLConnector() as connector:
            query = "DELETE FROM cbz_hashes WHERE db_gallery_id = %s"
            connector.execute(query, (db_gallery_id,))

    def _delete_verification(self, db_gallery_id: int) -> None:
        with self.SQLConnector() as connector:
            query = "DELETE FROM cbz_verifications WHERE db_gallery_id = %s"
            connector.execute(query, (db_gallery_id,))

    def _insert_verification(self, db_gallery_id: int, time: str) -> None:
        with self.SQLConnector() as connector:
            query = """
                INSERT INTO cbz_verifications (db_gallery_id, last_verified_time)
                VALUES (%s, %s)
            """
            connector.execute(query, (db_gallery_id, time))

    def _update_verification(self, db_gallery_id: int, time: str) -> None:
        with self.SQLConnector() as connector:
            query = """
                UPDATE cbz_verifications SET last_verified_time = %s
                WHERE db_gallery_id = %s
            """
            connector.execute(query, (time, db_gallery_id))

    def _get_last_verified_time(self, db_gallery_id: int) -> datetime.datetime | None:
        with self.SQLConnector() as connector:
            select_query = """
                SELECT last_verified_time FROM cbz_verifications WHERE db_gallery_id = %s
            """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
        return cast(datetime.datetime, query_result[0]) if query_result else None
