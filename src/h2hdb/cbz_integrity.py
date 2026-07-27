import datetime
from typing import cast

from .repository import BaseRepository, RepositoryContext
from .settings import chunk_list
from .table_gids import H2HDBGalleriesIDs

INTEGRITY_BATCH_SIZE = 400


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

        self.logger.debug(f"Ensured database table exists: name={table_name}.")

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

        self.logger.debug(f"Ensured database table exists: name={table_name}.")

    def _upsert_hash(self, db_gallery_id: int, sha256: bytes) -> None:
        with self.SQLConnector() as connector:
            select_query = "SELECT 1 FROM cbz_hashes WHERE db_gallery_id = %s"
            already_has_a_hash = connector.fetch_one(select_query, (db_gallery_id,))
            if already_has_a_hash:
                query = "UPDATE cbz_hashes SET sha256 = %s WHERE db_gallery_id = %s"
                connector.execute(query, (sha256, db_gallery_id))
            else:
                query = """
                    INSERT INTO cbz_hashes (db_gallery_id, sha256) VALUES (%s, %s)
                """
                connector.execute(query, (db_gallery_id, sha256))

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

    def _invalidate_for_db_gallery_ids(self, db_gallery_ids: set[int]) -> int:
        """Remove integrity state that planned CBZ writes would make stale."""
        if not db_gallery_ids:
            return 0

        invalidated_ids = set[int]()
        with self.SQLConnector() as connector:
            has_integrity_rows = connector.fetch_one("""
                    SELECT 1 FROM cbz_hashes
                    UNION ALL
                    SELECT 1 FROM cbz_verifications
                    LIMIT 1
                """)
            if not has_integrity_rows:
                return 0

            for batch in chunk_list(sorted(db_gallery_ids), INTEGRITY_BATCH_SIZE):
                placeholders = ", ".join(["%s"] * len(batch))
                rows = connector.fetch_all(
                    f"""
                        SELECT db_gallery_id
                        FROM cbz_hashes
                        WHERE db_gallery_id IN ({placeholders})
                        UNION
                        SELECT db_gallery_id
                        FROM cbz_verifications
                        WHERE db_gallery_id IN ({placeholders})
                    """,
                    (*batch, *batch),
                )
                existing_ids = [int(row[0]) for row in rows]
                if not existing_ids:
                    continue

                existing_placeholders = ", ".join(["%s"] * len(existing_ids))
                # Delete the verification first. MariaDBConnector commits each
                # DELETE independently, so interruption between these statements
                # then leaves the gallery eligible for an early scrub instead of
                # leaving a recent verification paired with no baseline hash.
                for table_name in ("cbz_verifications", "cbz_hashes"):
                    connector.execute(
                        f"""
                            DELETE FROM {table_name}
                            WHERE db_gallery_id IN ({existing_placeholders})
                        """,
                        tuple(existing_ids),
                    )
                invalidated_ids.update(existing_ids)
        return len(invalidated_ids)

    def _invalidate_verifications_for_db_gallery_ids(
        self, db_gallery_ids: set[int]
    ) -> int:
        """Make planned integrity repairs eligible for retry without losing hashes.

        A repair can be interrupted before the corrupt file is replaced. Keeping
        its known-good hash prevents the next scrub from accepting those corrupt
        bytes as a new baseline, while deleting the verification ensures it is
        selected for another check promptly.
        """
        if not db_gallery_ids:
            return 0

        invalidated_ids = set[int]()
        with self.SQLConnector() as connector:
            for batch in chunk_list(sorted(db_gallery_ids), INTEGRITY_BATCH_SIZE):
                placeholders = ", ".join(["%s"] * len(batch))
                rows = connector.fetch_all(
                    f"""
                        SELECT db_gallery_id
                        FROM cbz_verifications
                        WHERE db_gallery_id IN ({placeholders})
                    """,
                    tuple(batch),
                )
                existing_ids = [int(row[0]) for row in rows]
                if not existing_ids:
                    continue
                connector.execute(
                    f"""
                        DELETE FROM cbz_verifications
                        WHERE db_gallery_id IN (
                            {", ".join(["%s"] * len(existing_ids))}
                        )
                    """,
                    tuple(existing_ids),
                )
                invalidated_ids.update(existing_ids)
        return len(invalidated_ids)

    def _delete_stale_rows(self) -> None:
        with self.SQLConnector() as connector:
            for table_name in ("cbz_hashes", "cbz_verifications"):
                query = f"""
                    DELETE FROM {table_name}
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM galleries_dbids
                        WHERE galleries_dbids.db_gallery_id =
                            {table_name}.db_gallery_id
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM gallery_duplicate_warnings
                        WHERE gallery_duplicate_warnings.db_gallery_id =
                            {table_name}.db_gallery_id
                    )
                """
                connector.execute(query)

    def _set_verification_to_now(self, db_gallery_id: int) -> None:
        with self.SQLConnector() as connector:
            select_query = "SELECT 1 FROM cbz_verifications WHERE db_gallery_id = %s"
            already_verified_once = connector.fetch_one(select_query, (db_gallery_id,))
            match self.config.database.sql_type.lower():
                case "mariadb":
                    now_expr = "NOW()"
                case "sqlite":
                    now_expr = "datetime('now')"
            if already_verified_once:
                query = f"""
                    UPDATE cbz_verifications SET last_verified_time = {now_expr}
                    WHERE db_gallery_id = %s
                """
            else:
                query = f"""
                    INSERT INTO cbz_verifications (db_gallery_id, last_verified_time)
                    VALUES (%s, {now_expr})
                """
            connector.execute(query, (db_gallery_id,))

    def _get_last_verified_time(self, db_gallery_id: int) -> datetime.datetime | None:
        with self.SQLConnector() as connector:
            select_query = """
                SELECT last_verified_time FROM cbz_verifications WHERE db_gallery_id = %s
            """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
        return cast(datetime.datetime, query_result[0]) if query_result else None

    def _get_scrub_candidates(self, db_gallery_ids: list[int], limit: int) -> list[int]:
        """Pick up to `limit` of `db_gallery_ids`, oldest-verified first (never-verified first)."""
        if not db_gallery_ids or limit <= 0:
            return []

        candidates = list[tuple[int, datetime.datetime | None]]()
        with self.SQLConnector() as connector:
            for batch in chunk_list(sorted(set(db_gallery_ids)), INTEGRITY_BATCH_SIZE):
                select_query = f"""
                    SELECT galleries_dbids.db_gallery_id,
                        cbz_verifications.last_verified_time
                    FROM galleries_dbids
                        LEFT JOIN cbz_verifications
                            ON cbz_verifications.db_gallery_id =
                                galleries_dbids.db_gallery_id
                    WHERE galleries_dbids.db_gallery_id IN (
                        {", ".join(["%s"] * len(batch))}
                    )
                    ORDER BY
                        cbz_verifications.last_verified_time IS NOT NULL,
                        cbz_verifications.last_verified_time ASC,
                        galleries_dbids.db_gallery_id ASC
                    LIMIT %s
                """
                query_result = connector.fetch_all(select_query, (*batch, limit))
                candidates.extend(
                    (
                        int(db_gallery_id),
                        cast(datetime.datetime | None, last_verified_time),
                    )
                    for db_gallery_id, last_verified_time in query_result
                )

        candidates.sort(
            key=lambda candidate: (
                candidate[1] is not None,
                candidate[1] or datetime.datetime.min,
                candidate[0],
            )
        )
        return [db_gallery_id for db_gallery_id, _ in candidates[:limit]]
