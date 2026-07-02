import datetime

from .repository import BaseRepository, RepositoryContext
from .table_gids import H2HDBGalleriesIDs
from .table_times import H2HDBTimes


class H2HDBGalleryDeduplication(BaseRepository):
    def __init__(
        self,
        context: RepositoryContext,
        gallery_ids: H2HDBGalleriesIDs,
        gallery_times: H2HDBTimes,
    ) -> None:
        super().__init__(context)
        self.gallery_ids = gallery_ids
        self.gallery_times = gallery_times

    def _create_gallery_content_hashes_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "gallery_content_hashes"
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

    def _create_gallery_full_content_hashes_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "gallery_full_content_hashes"
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
                            INDEX (sha256)
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
                        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_sha256 "
                        f"ON {table_name}(sha256)"
                    )

        self.logger.info(f"{table_name} table created.")

    def _create_gallery_full_duplicate_names_view(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "gallery_full_duplicate_names"
            query = f"""
                CREATE VIEW IF NOT EXISTS {table_name} AS
                SELECT galleries_names.full_name AS full_name
                FROM gallery_full_content_hashes AS h
                    JOIN galleries_names
                        ON galleries_names.db_gallery_id = h.db_gallery_id
                    JOIN galleries_download_times AS dt
                        ON dt.db_gallery_id = h.db_gallery_id
                    JOIN (
                        SELECT h2.sha256, MAX(dt2.time) AS max_download_time
                        FROM gallery_full_content_hashes AS h2
                            JOIN galleries_download_times AS dt2
                                ON dt2.db_gallery_id = h2.db_gallery_id
                        GROUP BY h2.sha256
                        HAVING COUNT(*) > 1
                    ) AS newest ON h.sha256 = newest.sha256
                WHERE dt.time < newest.max_download_time
            """
            connector.execute(query)
        self.logger.info(f"{table_name} view created.")

    def _create_gallery_duplicate_warnings_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "gallery_duplicate_warnings"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id),
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            FOREIGN KEY (duplicate_of_db_gallery_id)
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_gallery_id              INT UNSIGNED NOT NULL,
                            duplicate_of_db_gallery_id INT UNSIGNED NOT NULL
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            db_gallery_id INTEGER NOT NULL PRIMARY KEY
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            duplicate_of_db_gallery_id INTEGER NOT NULL
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE ON DELETE CASCADE
                        )
                    """
            connector.execute(query)
        self.logger.info(f"{table_name} table created.")

    def _create_gallery_duplicate_warnings_names_view(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "gallery_duplicate_warnings_names"
            query = f"""
                CREATE VIEW IF NOT EXISTS {table_name} AS
                SELECT
                    duplicate_names.full_name AS duplicate_name,
                    kept_names.full_name AS kept_name
                FROM gallery_duplicate_warnings
                    JOIN galleries_names AS duplicate_names
                        ON duplicate_names.db_gallery_id = gallery_duplicate_warnings.db_gallery_id
                    JOIN galleries_names AS kept_names
                        ON kept_names.db_gallery_id
                            = gallery_duplicate_warnings.duplicate_of_db_gallery_id
            """
            connector.execute(query)
        self.logger.info(f"{table_name} view created.")

    def _get_hash_owner(self, sha256: bytes) -> int | None:
        with self.SQLConnector() as connector:
            select_query = """
                SELECT db_gallery_id FROM gallery_content_hashes WHERE sha256 = %s
            """
            query_result = connector.fetch_one(select_query, (sha256,))
        return int(query_result[0]) if query_result else None

    def _upsert_hash(self, table_name: str, db_gallery_id: int, sha256: bytes) -> None:
        with self.SQLConnector() as connector:
            select_query = f"SELECT 1 FROM {table_name} WHERE db_gallery_id = %s"
            already_has_a_hash = connector.fetch_one(select_query, (db_gallery_id,))
            if already_has_a_hash:
                query = f"UPDATE {table_name} SET sha256 = %s WHERE db_gallery_id = %s"
                connector.execute(query, (sha256, db_gallery_id))
            else:
                query = f"""
                    INSERT INTO {table_name} (db_gallery_id, sha256) VALUES (%s, %s)
                """
                connector.execute(query, (db_gallery_id, sha256))

    def _claim_hash(self, db_gallery_id: int, sha256: bytes) -> None:
        self._upsert_hash("gallery_content_hashes", db_gallery_id, sha256)

    def _evict_hash(self, db_gallery_id: int) -> None:
        with self.SQLConnector() as connector:
            query = "DELETE FROM gallery_content_hashes WHERE db_gallery_id = %s"
            connector.execute(query, (db_gallery_id,))

    def _set_full_content_hash(self, db_gallery_id: int, sha256: bytes) -> None:
        self._upsert_hash("gallery_full_content_hashes", db_gallery_id, sha256)

    def _record_duplicate_warning(
        self, db_gallery_id: int, duplicate_of_db_gallery_id: int
    ) -> None:
        with self.SQLConnector() as connector:
            select_query = """
                SELECT 1 FROM gallery_duplicate_warnings WHERE db_gallery_id = %s
            """
            already_warned = connector.fetch_one(select_query, (db_gallery_id,))
            if already_warned:
                return

            insert_query = """
                INSERT INTO gallery_duplicate_warnings
                    (db_gallery_id, duplicate_of_db_gallery_id)
                VALUES (%s, %s)
            """
            connector.execute(insert_query, (db_gallery_id, duplicate_of_db_gallery_id))

    def resolve(
        self, db_gallery_id: int, sha256: bytes, download_time: datetime.datetime
    ) -> tuple[bool, int | None]:
        """Claim `sha256` for `db_gallery_id`.

        Returns `(should_compress, evicted_db_gallery_id)`. If a strictly
        older duplicate currently owns this hash, it's evicted from
        `gallery_content_hashes` and its `db_gallery_id` is returned so the
        caller can also clean up its CBZ file and `cbz_integrity` rows. If
        `db_gallery_id` is itself the older duplicate, nothing is claimed, a
        one-time warning is recorded, and `should_compress` is `False`.
        """
        owner_id = self._get_hash_owner(sha256)
        if owner_id is None:
            self._claim_hash(db_gallery_id, sha256)
            return True, None
        if owner_id == db_gallery_id:
            return True, None

        owner_download_time = self.gallery_times.get_download_times_by_db_gallery_ids(
            [owner_id]
        )[owner_id]
        if download_time > owner_download_time:
            self._evict_hash(owner_id)
            self._claim_hash(db_gallery_id, sha256)
            return True, owner_id

        self._record_duplicate_warning(db_gallery_id, owner_id)
        return False, None
