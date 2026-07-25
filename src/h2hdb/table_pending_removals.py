from itertools import chain

from .repository import BaseRepository, RepositoryContext
from .settings import FOLDER_NAME_LENGTH_LIMIT, chunk_list
from .table_gids import H2HDBGalleriesIDs

PENDING_REMOVAL_BATCH_SIZE = 500


class H2HDBPendingGalleryRemovals(BaseRepository):
    def __init__(
        self, context: RepositoryContext, gallery_ids: H2HDBGalleriesIDs
    ) -> None:
        super().__init__(context)
        self.gallery_ids = gallery_ids

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

        self.logger.info(f"{table_name} table created.")

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

    def delete_pending_gallery_removals(self) -> None:
        pending_gallery_removals = self.get_pending_gallery_removals()
        for gallery_name in pending_gallery_removals:
            self.delete_gallery_file(gallery_name)
            self.delete_gallery(gallery_name)
            self.delete_pending_gallery_removal(gallery_name)

    def delete_gallery_file(self, gallery_name: str) -> None:
        pass

    def delete_gallery(self, gallery_name: str) -> None:
        if self._delete_gallery_row(gallery_name):
            self.logger.info(f"Gallery '{gallery_name}' deleted.")

    def refresh_gallery(self, gallery_name: str) -> None:
        if self._delete_gallery_row(gallery_name):
            self.logger.info(
                f"Gallery '{gallery_name}' refreshed: galleryinfo.txt changed, "
                "reinserting."
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
