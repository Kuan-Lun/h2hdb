import datetime

from .repository import BaseRepository, RepositoryContext
from .settings import chunk_list
from .table_gids import H2HDBGalleriesIDs
from .table_times import H2HDBTimes
from .table_titles import H2HDBGalleriesTitles

ALREADY_UPLOADED_TAG_VALUE = "already uploaded"
ALREADY_UPLOADED_BATCH_SIZE = 500


class H2HDBGalleryDeduplication(BaseRepository):
    def __init__(
        self,
        context: RepositoryContext,
        gallery_ids: H2HDBGalleriesIDs,
        gallery_times: H2HDBTimes,
        gallery_titles: H2HDBGalleriesTitles,
    ) -> None:
        super().__init__(context)
        self.gallery_ids = gallery_ids
        self.gallery_times = gallery_times
        self.gallery_titles = gallery_titles

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

    def _clear_duplicate_warning(self, db_gallery_id: int) -> None:
        with self.SQLConnector() as connector:
            query = "DELETE FROM gallery_duplicate_warnings WHERE db_gallery_id = %s"
            connector.execute(query, (db_gallery_id,))

    def get_duplicate_warning_db_gallery_ids(self) -> list[int]:
        """db_gallery_id of every gallery currently losing its content-hash
        race to another gallery -- i.e. galleries that shouldn't have a CBZ."""
        with self.SQLConnector() as connector:
            query = "SELECT db_gallery_id FROM gallery_duplicate_warnings"
            query_result = connector.fetch_all(query)
        return [int(row[0]) for row in query_result]

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

    def get_already_uploaded_flags_by_db_gallery_ids(
        self, db_gallery_ids: list[int]
    ) -> dict[int, bool]:
        if not db_gallery_ids:
            return {}

        flagged_ids: set[int] = set()
        with self.SQLConnector() as connector:
            for batch in chunk_list(db_gallery_ids, ALREADY_UPLOADED_BATCH_SIZE):
                select_query = f"""
                    SELECT DISTINCT galleries_tags.db_gallery_id
                    FROM galleries_tags
                        JOIN galleries_tag_pairs_dbids
                            ON galleries_tags.db_tag_pair_id
                                = galleries_tag_pairs_dbids.db_tag_pair_id
                    WHERE galleries_tag_pairs_dbids.tag_value = %s
                        AND galleries_tags.db_gallery_id IN (
                            {", ".join(["%s"] * len(batch))}
                        )
                """
                query_result = connector.fetch_all(
                    select_query, (ALREADY_UPLOADED_TAG_VALUE, *batch)
                )
                flagged_ids.update(int(row[0]) for row in query_result)
        return {
            db_gallery_id: db_gallery_id in flagged_ids
            for db_gallery_id in db_gallery_ids
        }

    def _get_priority_key(
        self, db_gallery_id: int
    ) -> tuple[bool, int, datetime.datetime]:
        """Rank a gallery against a hash-collision opponent.

        Lexicographic: a gallery tagged "already uploaded" always loses
        regardless of the other two fields; otherwise the longer title wins;
        only when both tie does the more recent download_time decide.
        """
        has_already_uploaded_tag = self.get_already_uploaded_flags_by_db_gallery_ids(
            [db_gallery_id]
        )[db_gallery_id]
        title = self.gallery_titles.get_titles_by_db_gallery_ids([db_gallery_id])[
            db_gallery_id
        ]
        download_time = self.gallery_times.get_download_times_by_db_gallery_ids(
            [db_gallery_id]
        )[db_gallery_id]
        return (not has_already_uploaded_tag, len(title), download_time)

    def resolve(
        self,
        db_gallery_id: int,
        sha256: bytes,
        priority_key: tuple[bool, int, datetime.datetime],
    ) -> tuple[bool, int | None]:
        """Claim `sha256` for `db_gallery_id`.

        `priority_key` (see `_get_priority_key`) ranks `db_gallery_id`
        against whichever gallery currently owns `sha256`. Returns
        `(should_compress, evicted_db_gallery_id)`. If `db_gallery_id`
        outranks the current owner (or there is none), it claims the hash,
        any stale `gallery_duplicate_warnings` row of its own from a past
        loss is cleared (it isn't a duplicate anymore), and the outranked
        owner -- if any -- is evicted from `gallery_content_hashes` and its
        `db_gallery_id` returned so the caller can also clean up its CBZ file
        and `cbz_integrity` rows. If `db_gallery_id` is outranked, nothing is
        claimed, a one-time warning is recorded, and `should_compress` is
        `False`.
        """
        owner_id = self._get_hash_owner(sha256)
        if owner_id is None:
            self._claim_hash(db_gallery_id, sha256)
            self._clear_duplicate_warning(db_gallery_id)
            return True, None
        if owner_id == db_gallery_id:
            return True, None

        owner_priority_key = self._get_priority_key(owner_id)
        if priority_key > owner_priority_key:
            self._evict_hash(owner_id)
            self._claim_hash(db_gallery_id, sha256)
            self._clear_duplicate_warning(db_gallery_id)
            return True, owner_id

        self._record_duplicate_warning(db_gallery_id, owner_id)
        return False, None
