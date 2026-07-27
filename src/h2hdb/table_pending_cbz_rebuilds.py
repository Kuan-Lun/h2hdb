from itertools import chain

from .repository import BaseRepository
from .settings import FOLDER_NAME_LENGTH_LIMIT, chunk_list

PENDING_CBZ_REBUILD_BATCH_SIZE = 500


class H2HDBPendingCBZRebuilds(BaseRepository):
    def _create_pending_cbz_rebuilds_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "pending_cbz_rebuilds"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    column_name_parts, create_gallery_name_parts_sql = (
                        self.mariadb_split_gallery_name_based_on_limit("name")
                    )
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY ({", ".join(column_name_parts)}),
                            {create_gallery_name_parts_sql},
                            full_name TEXT NOT NULL
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
        self.logger.debug(f"Ensured database table exists: name={table_name}.")

    def get_pending_gallery_names(
        self, gallery_names: list[str] | None = None
    ) -> set[str]:
        if gallery_names is not None and not gallery_names:
            return set()

        if gallery_names is None:
            with self.SQLConnector() as connector:
                rows = connector.fetch_all("SELECT full_name FROM pending_cbz_rebuilds")
            return {str(full_name) for (full_name,) in rows}

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
        pending_name_parts = set[tuple[str, ...]]()
        with self.SQLConnector() as connector:
            for batch in chunk_list(gallery_names, PENDING_CBZ_REBUILD_BATCH_SIZE):
                where_clause = " OR ".join(
                    "("
                    + " AND ".join(
                        f"{column_name} = %s" for column_name in column_name_parts
                    )
                    + ")"
                    for _ in batch
                )
                parameters = tuple(
                    chain.from_iterable(
                        name_parts_by_gallery_name[gallery_name]
                        for gallery_name in batch
                    )
                )
                rows = connector.fetch_all(
                    f"""
                        SELECT {", ".join(column_name_parts)}
                        FROM pending_cbz_rebuilds
                        WHERE {where_clause}
                    """,
                    parameters,
                )
                pending_name_parts.update(
                    tuple(str(name_part) for name_part in row) for row in rows
                )
        return {
            gallery_name
            for gallery_name, name_parts in name_parts_by_gallery_name.items()
            if name_parts in pending_name_parts
        }

    def insert_pending_gallery_names(self, gallery_names: list[str]) -> None:
        if not gallery_names:
            return
        for gallery_name in gallery_names:
            if len(gallery_name) > FOLDER_NAME_LENGTH_LIMIT:
                raise ValueError(
                    f"Gallery name is too long for pending CBZ rebuild: {gallery_name!r}."
                )

        existing = self.get_pending_gallery_names(gallery_names)
        to_insert = [
            gallery_name
            for gallery_name in gallery_names
            if gallery_name not in existing
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
            "pending_cbz_rebuilds",
            [*column_name_parts, "full_name"],
            [
                (*self._split_gallery_name(gallery_name), gallery_name)
                for gallery_name in to_insert
            ],
        )

    def delete_pending_gallery_names(self, gallery_names: list[str]) -> None:
        if not gallery_names:
            return

        match self.config.database.sql_type.lower():
            case "mariadb":
                column_name_parts, _ = self.mariadb_split_gallery_name_based_on_limit(
                    "name"
                )
            case "sqlite":
                column_name_parts, _ = self.sqlite_name_columns("name")

        with self.SQLConnector() as connector:
            for batch in chunk_list(gallery_names, PENDING_CBZ_REBUILD_BATCH_SIZE):
                where_clause = " OR ".join(
                    "("
                    + " AND ".join(
                        f"{column_name} = %s" for column_name in column_name_parts
                    )
                    + ")"
                    for _ in batch
                )
                parameters = tuple(
                    chain.from_iterable(
                        self._split_gallery_name(gallery_name) for gallery_name in batch
                    )
                )
                connector.execute(
                    f"DELETE FROM pending_cbz_rebuilds WHERE {where_clause}",
                    parameters,
                )

    def delete_stale_gallery_names(self, current_gallery_names: set[str]) -> None:
        stale_names = self.get_pending_gallery_names().difference(current_gallery_names)
        self.delete_pending_gallery_names(sorted(stale_names))
