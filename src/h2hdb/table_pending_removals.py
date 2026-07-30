from dataclasses import dataclass
from itertools import chain

from .repository import BaseRepository, RepositoryContext
from .settings import FOLDER_NAME_LENGTH_LIMIT, chunk_list
from .sql_connector import SQLConnector
from .table_gids import H2HDBGalleriesIDs
from .todownload_queue import H2HDBToDownloadQueue

PENDING_REMOVAL_BATCH_SIZE = 500


@dataclass(frozen=True, slots=True)
class GalleryRemovalRow:
    db_gallery_id: int
    gallery_name: str
    gid: int | None
    is_deletion_candidate: bool


class H2HDBPendingGalleryRemovals(BaseRepository):
    def __init__(
        self,
        context: RepositoryContext,
        gallery_ids: H2HDBGalleriesIDs,
        todownload_queue: H2HDBToDownloadQueue,
    ) -> None:
        super().__init__(context)
        self.gallery_ids = gallery_ids
        self.todownload_queue = todownload_queue

    def _create_pending_gallery_removals_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "pending_gallery_removals"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    column_name_parts, create_gallery_name_parts_sql = (
                        self.mariadb_split_gallery_name_based_on_limit("name")
                    )
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY ({", ".join(column_name_parts)}),
                            {create_gallery_name_parts_sql},
                            full_name TEXT NOT NULL,
                            FULLTEXT (full_name)
                        )
                    """
                case "sqlite":
                    column_name_parts, create_gallery_name_parts_sql = (
                        self.sqlite_name_columns("name")
                    )
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            {create_gallery_name_parts_sql},
                            full_name TEXT NOT NULL,
                            PRIMARY KEY ({", ".join(column_name_parts)})
                        )
                    """
            connector.execute(query)

            match self.config.database.sql_type.lower():
                case "sqlite":
                    self._create_sqlite_fts5_sync(
                        connector, table_name, "full_name", "rowid"
                    )

        self.logger.debug(f"Ensured database table exists: name={table_name}.")

    def insert_pending_gallery_removal(self, gallery_name: str) -> None:
        if self.check_pending_gallery_removal(gallery_name) is True:
            return

        table_name = "pending_gallery_removals"
        if len(gallery_name) > FOLDER_NAME_LENGTH_LIMIT:
            self.logger.error(
                f"Gallery name '{gallery_name}' is too long. Must be {FOLDER_NAME_LENGTH_LIMIT} characters or less."
            )
            raise ValueError("Gallery name is too long.")
        gallery_name_parts = self._split_gallery_name(gallery_name)

        match self.config.database.sql_type.lower():
            case "mariadb":
                column_name_parts, _ = self.mariadb_split_gallery_name_based_on_limit(
                    "name"
                )
            case "sqlite":
                column_name_parts, _ = self.sqlite_name_columns("name")
        insert_query = f"""
            INSERT INTO {table_name} ({", ".join(column_name_parts)}, full_name)
            VALUES ({", ".join(["%s" for _ in column_name_parts])}, %s)
        """
        with self.SQLConnector() as connector:
            connector.execute(insert_query, (*tuple(gallery_name_parts), gallery_name))

    def insert_pending_gallery_removals(self, gallery_names: list[str]) -> None:
        if not gallery_names:
            return

        for gallery_name in gallery_names:
            if len(gallery_name) > FOLDER_NAME_LENGTH_LIMIT:
                self.logger.error(
                    f"Gallery name '{gallery_name}' is too long. Must be {FOLDER_NAME_LENGTH_LIMIT} characters or less."
                )
                raise ValueError("Gallery name is too long.")

        already_pending = self._check_pending_gallery_removals(gallery_names)
        to_insert = [
            gallery_name
            for gallery_name in gallery_names
            if gallery_name not in already_pending
        ]
        if not to_insert:
            return

        match self.config.database.sql_type.lower():
            case "mariadb":
                column_name_parts, _ = self.mariadb_split_gallery_name_based_on_limit(
                    "name"
                )
            case "sqlite":
                column_name_parts, _ = self.sqlite_name_columns("name")
        self._insert_rows(
            "pending_gallery_removals",
            [*column_name_parts, "full_name"],
            [
                (*self._split_gallery_name(gallery_name), gallery_name)
                for gallery_name in to_insert
            ],
        )

    def check_pending_gallery_removal(self, gallery_name: str) -> bool:
        with self.SQLConnector() as connector:
            table_name = "pending_gallery_removals"
            gallery_name_parts = self._split_gallery_name(gallery_name)
            match self.config.database.sql_type.lower():
                case "mariadb":
                    column_name_parts, _ = (
                        self.mariadb_split_gallery_name_based_on_limit("name")
                    )
                case "sqlite":
                    column_name_parts, _ = self.sqlite_name_columns("name")
            select_query = f"""
                SELECT full_name
                FROM {table_name}
                WHERE {" AND ".join([f"{part} = %s" for part in column_name_parts])}
            """
            query_result = connector.fetch_one(select_query, tuple(gallery_name_parts))
        return len(query_result) != 0

    def _check_pending_gallery_removals(self, gallery_names: list[str]) -> set[str]:
        """Return the subset of gallery_names already present in pending_gallery_removals."""
        if not gallery_names:
            return set()

        table_name = "pending_gallery_removals"
        match self.config.database.sql_type.lower():
            case "mariadb":
                column_name_parts, _ = self.mariadb_split_gallery_name_based_on_limit(
                    "name"
                )
            case "sqlite":
                column_name_parts, _ = self.sqlite_name_columns("name")

        name_parts_by_gallery_name = {
            gallery_name: tuple(self._split_gallery_name(gallery_name))
            for gallery_name in gallery_names
        }

        existing_name_parts = set[tuple[str, ...]]()
        with self.SQLConnector() as connector:
            for batch in chunk_list(gallery_names, PENDING_REMOVAL_BATCH_SIZE):
                where_clause = " OR ".join(
                    "("
                    + " AND ".join(f"{part} = %s" for part in column_name_parts)
                    + ")"
                    for _ in batch
                )
                select_query = f"""
                    SELECT {", ".join(column_name_parts)}
                    FROM {table_name}
                    WHERE {where_clause}
                """
                parameters = tuple(
                    chain.from_iterable(
                        name_parts_by_gallery_name[gallery_name]
                        for gallery_name in batch
                    )
                )
                query_result = connector.fetch_all(select_query, parameters)
                for row in query_result:
                    existing_name_parts.add(tuple(str(part) for part in row))

        return {
            gallery_name
            for gallery_name, name_parts in name_parts_by_gallery_name.items()
            if name_parts in existing_name_parts
        }

    def get_pending_gallery_removals(self) -> list[str]:
        with self.SQLConnector() as connector:
            table_name = "pending_gallery_removals"
            select_query = f"""
                SELECT full_name
                FROM {table_name}
            """

            query_result = connector.fetch_all(select_query)
        pending_gallery_removals = [query[0] for query in query_result]
        return pending_gallery_removals

    def delete_pending_gallery_removal(self, gallery_name: str) -> None:
        with self.SQLConnector() as connector:
            table_name = "pending_gallery_removals"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    column_name_parts, _ = (
                        self.mariadb_split_gallery_name_based_on_limit("name")
                    )
                case "sqlite":
                    column_name_parts, _ = self.sqlite_name_columns("name")
            delete_query = f"""
                DELETE FROM {table_name} WHERE {" AND ".join([f"{part} = %s" for part in column_name_parts])}
            """

            gallery_name_parts = self._split_gallery_name(gallery_name)
            connector.execute(delete_query, tuple(gallery_name_parts))

    def delete_pending_gallery_removals_by_names(
        self, gallery_names: list[str]
    ) -> None:
        if not gallery_names:
            return

        table_name = "pending_gallery_removals"
        match self.config.database.sql_type.lower():
            case "mariadb":
                column_name_parts, _ = self.mariadb_split_gallery_name_based_on_limit(
                    "name"
                )
            case "sqlite":
                column_name_parts, _ = self.sqlite_name_columns("name")

        with self.SQLConnector() as connector:
            for batch in chunk_list(gallery_names, PENDING_REMOVAL_BATCH_SIZE):
                where_clause = " OR ".join(
                    "("
                    + " AND ".join(f"{part} = %s" for part in column_name_parts)
                    + ")"
                    for _ in batch
                )
                delete_query = f"""
                    DELETE FROM {table_name} WHERE {where_clause}
                """
                parameters = tuple(
                    chain.from_iterable(
                        self._split_gallery_name(gallery_name) for gallery_name in batch
                    )
                )
                connector.execute(delete_query, parameters)

    def recover_pending_gallery_removals(self, current_gallery_names: set[str]) -> int:
        """Recover interrupted metadata writes after the filesystem scan.

        A pending name whose folder still exists is an interrupted insert or
        refresh and must be removed without creating a download request; the
        scan will reinsert it. A pending name whose folder is gone is a
        confirmed physical removal and follows the durable redownload path.
        """

        pending_names = self.get_pending_gallery_removals()
        if not pending_names:
            return 0

        present_names = [
            name for name in pending_names if name in current_gallery_names
        ]
        missing_names = [
            name for name in pending_names if name not in current_gallery_names
        ]

        if present_names:
            self.refresh_galleries(present_names)
            self.delete_pending_gallery_removals_by_names(present_names)
        return self.delete_confirmed_missing_galleries(missing_names)

    def delete_confirmed_missing_galleries(self, gallery_names: list[str]) -> int:
        """Atomically remove missing rows and enqueue fully cleared candidate GIDs."""

        if not gallery_names:
            return 0

        existing_names: list[str] = []
        candidate_gids: set[int] = set()
        with self.SQLConnector() as connector:
            with connector.transaction():
                for batch in chunk_list(gallery_names, PENDING_REMOVAL_BATCH_SIZE):
                    rows = self._get_gallery_removal_rows(connector, batch)
                    if not rows:
                        self._delete_pending_rows_with_connector(connector, batch)
                        continue

                    db_gallery_ids = [row.db_gallery_id for row in rows]
                    existing_names.extend(row.gallery_name for row in rows)
                    batch_candidate_gids = {
                        row.gid
                        for row in rows
                        if row.gid is not None and row.is_deletion_candidate
                    }
                    candidate_gids.update(batch_candidate_gids)

                    placeholders = ", ".join(["%s"] * len(db_gallery_ids))
                    connector.execute(
                        f"""
                        DELETE FROM todelete_galleries
                        WHERE db_gallery_id IN ({placeholders})
                        """,
                        tuple(db_gallery_ids),
                    )
                    connector.execute(
                        f"""
                        DELETE FROM galleries_dbids
                        WHERE db_gallery_id IN ({placeholders})
                        """,
                        tuple(db_gallery_ids),
                    )
                    self._delete_pending_rows_with_connector(connector, batch)

                remaining_candidate_gids: set[int] = set()
                for gid_batch in chunk_list(
                    sorted(candidate_gids), PENDING_REMOVAL_BATCH_SIZE
                ):
                    placeholders = ", ".join(["%s"] * len(gid_batch))
                    remaining_rows = connector.fetch_all(
                        f"""
                        SELECT DISTINCT galleries_gids.gid
                        FROM todelete_galleries
                            JOIN galleries_gids USING (db_gallery_id)
                        WHERE galleries_gids.gid IN ({placeholders})
                        """,
                        tuple(gid_batch),
                    )
                    remaining_candidate_gids.update(
                        int(row[0]) for row in remaining_rows
                    )

                queued_gids = candidate_gids.difference(remaining_candidate_gids)
                for gid in sorted(queued_gids):
                    self.todownload_queue._request_download_with_connector(
                        connector, gid
                    )

                for gid_batch in chunk_list(
                    sorted(queued_gids), PENDING_REMOVAL_BATCH_SIZE
                ):
                    placeholders = ", ".join(["%s"] * len(gid_batch))
                    connector.execute(
                        f"""
                        DELETE FROM todelete_gids
                        WHERE gid IN ({placeholders})
                            AND NOT EXISTS (
                                SELECT 1
                                FROM galleries_gids
                                WHERE galleries_gids.gid = todelete_gids.gid
                            )
                        """,
                        tuple(gid_batch),
                    )

        for gallery_name in existing_names:
            self.logger.info(
                f"Gallery removed from database: gallery={gallery_name!r}."
            )
        return len(existing_names)

    def _get_gallery_removal_rows(
        self, connector: SQLConnector, gallery_names: list[str]
    ) -> list[GalleryRemovalRow]:
        match self.config.database.sql_type.lower():
            case "mariadb":
                column_name_parts, _ = self.mariadb_split_gallery_name_based_on_limit(
                    "name"
                )
            case "sqlite":
                column_name_parts, _ = self.sqlite_name_columns("name")

        where_clause = " OR ".join(
            "(" + " AND ".join(f"d.{part} = %s" for part in column_name_parts) + ")"
            for _ in gallery_names
        )
        parameters = tuple(
            chain.from_iterable(
                self._split_gallery_name(gallery_name) for gallery_name in gallery_names
            )
        )
        rows = connector.fetch_all(
            f"""
            SELECT
                d.db_gallery_id,
                {", ".join(f"d.{part}" for part in column_name_parts)},
                g.gid,
                CASE WHEN td.db_gallery_id IS NULL THEN 0 ELSE 1 END
            FROM galleries_dbids AS d
                LEFT JOIN galleries_gids AS g USING (db_gallery_id)
                LEFT JOIN todelete_galleries AS td USING (db_gallery_id)
            WHERE {where_clause}
            """,
            parameters,
        )
        name_part_count = len(column_name_parts)
        return [
            GalleryRemovalRow(
                db_gallery_id=int(row[0]),
                gallery_name="".join(
                    str(part) for part in row[1 : 1 + name_part_count]
                ),
                gid=(
                    int(row[1 + name_part_count])
                    if row[1 + name_part_count] is not None
                    else None
                ),
                is_deletion_candidate=bool(row[2 + name_part_count]),
            )
            for row in rows
        ]

    def _delete_pending_rows_with_connector(
        self, connector: SQLConnector, gallery_names: list[str]
    ) -> None:
        if not gallery_names:
            return
        match self.config.database.sql_type.lower():
            case "mariadb":
                column_name_parts, _ = self.mariadb_split_gallery_name_based_on_limit(
                    "name"
                )
            case "sqlite":
                column_name_parts, _ = self.sqlite_name_columns("name")
        where_clause = " OR ".join(
            "(" + " AND ".join(f"{part} = %s" for part in column_name_parts) + ")"
            for _ in gallery_names
        )
        parameters = tuple(
            chain.from_iterable(
                self._split_gallery_name(gallery_name) for gallery_name in gallery_names
            )
        )
        connector.execute(
            f"DELETE FROM pending_gallery_removals WHERE {where_clause}",
            parameters,
        )

    def refresh_gallery(self, gallery_name: str) -> None:
        if self._delete_gallery_row(gallery_name):
            self.logger.info(
                "Gallery metadata changed; existing database record removed "
                f"for reinsertion: gallery={gallery_name!r}."
            )

    def refresh_galleries(self, gallery_names: list[str]) -> None:
        for gallery_name in self._delete_existing_gallery_rows(gallery_names):
            self.logger.info(
                "Gallery metadata changed; existing database record removed "
                f"for reinsertion: gallery={gallery_name!r}."
            )

    def _delete_gallery_row(self, gallery_name: str) -> bool:
        if not self.gallery_ids._check_galleries_dbids_by_gallery_name(gallery_name):
            self.logger.debug(f"Gallery '{gallery_name}' does not exist.")
            return False

        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mariadb":
                    column_name_parts, _ = (
                        self.mariadb_split_gallery_name_based_on_limit("name")
                    )
                case "sqlite":
                    column_name_parts, _ = self.sqlite_name_columns("name")
            get_delete_gallery_id_query = f"""
                DELETE FROM galleries_dbids
                WHERE {" AND ".join([f"{part} = %s" for part in column_name_parts])}
                """

            gallery_name_parts = self._split_gallery_name(gallery_name)
            connector.execute(get_delete_gallery_id_query, tuple(gallery_name_parts))
        return True

    def _delete_existing_gallery_rows(self, gallery_names: list[str]) -> list[str]:
        if not gallery_names:
            return []

        existing_db_gallery_ids = (
            self.gallery_ids._get_db_gallery_ids_by_gallery_names_from_dbids(
                gallery_names
            )
        )
        existing_gallery_names = list(existing_db_gallery_ids)
        if not existing_gallery_names:
            return []

        match self.config.database.sql_type.lower():
            case "mariadb":
                column_name_parts, _ = self.mariadb_split_gallery_name_based_on_limit(
                    "name"
                )
            case "sqlite":
                column_name_parts, _ = self.sqlite_name_columns("name")

        with self.SQLConnector() as connector:
            for batch in chunk_list(existing_gallery_names, PENDING_REMOVAL_BATCH_SIZE):
                where_clause = " OR ".join(
                    "("
                    + " AND ".join(f"{part} = %s" for part in column_name_parts)
                    + ")"
                    for _ in batch
                )
                delete_query = f"""
                    DELETE FROM galleries_dbids WHERE {where_clause}
                """
                parameters = tuple(
                    chain.from_iterable(
                        self._split_gallery_name(gallery_name) for gallery_name in batch
                    )
                )
                connector.execute(delete_query, parameters)

        return existing_gallery_names
