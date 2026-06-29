from .repository import BaseRepository, RepositoryContext
from .settings import FOLDER_NAME_LENGTH_LIMIT
from .table_gids import H2HDBGalleriesIDs


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
        with self.SQLConnector() as connector:
            if self.check_pending_gallery_removal(gallery_name) is False:
                table_name = "pending_gallery_removals"
                if len(gallery_name) > FOLDER_NAME_LENGTH_LIMIT:
                    self.logger.error(
                        f"Gallery name '{gallery_name}' is too long. Must be {FOLDER_NAME_LENGTH_LIMIT} characters or less."
                    )
                    raise ValueError("Gallery name is too long.")
                gallery_name_parts = self._split_gallery_name(gallery_name)

                match self.config.database.sql_type.lower():
                    case "mariadb":
                        column_name_parts, _ = (
                            self.mariadb_split_gallery_name_based_on_limit("name")
                        )
                    case "sqlite":
                        column_name_parts, _ = self.sqlite_name_columns("name")
                insert_query = f"""
                    INSERT INTO {table_name} ({", ".join(column_name_parts)}, full_name)
                    VALUES ({", ".join(["%s" for _ in column_name_parts])}, %s)
                """
                connector.execute(
                    insert_query, (*tuple(gallery_name_parts), gallery_name)
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

    def delete_pending_gallery_removals(self) -> None:
        pending_gallery_removals = self.get_pending_gallery_removals()
        for gallery_name in pending_gallery_removals:
            self.delete_gallery_file(gallery_name)
            self.delete_gallery(gallery_name)
            self.delete_pending_gallery_removal(gallery_name)

    def delete_gallery_file(self, gallery_name: str) -> None:
        # self.logger.info(f"Gallery images for '{gallery_name}' deleted.")
        pass

    def delete_gallery(self, gallery_name: str) -> None:
        with self.SQLConnector() as connector:
            if not self.gallery_ids._check_galleries_dbids_by_gallery_name(
                gallery_name
            ):
                self.logger.debug(f"Gallery '{gallery_name}' does not exist.")
                return

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
        self.logger.info(f"Gallery '{gallery_name}' deleted.")
