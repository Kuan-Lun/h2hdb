from itertools import chain
from typing import Any

from .information import TagInformation
from .repository import BaseRepository, RepositoryContext
from .sql_connector import (
    DatabaseDuplicateKeyError,
    DatabaseKeyError,
)
from .table_gids import H2HDBGalleriesIDs

TAG_BATCH_SIZE = 500


class H2HDBGalleriesTags(BaseRepository):
    def __init__(
        self, context: RepositoryContext, gallery_ids: H2HDBGalleriesIDs
    ) -> None:
        super().__init__(context)
        self.gallery_ids = gallery_ids

    def _create_galleries_tags_table(self) -> None:
        with self.SQLConnector() as connector:
            tag_name_table_name = "galleries_tags_names"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {tag_name_table_name} (
                            PRIMARY KEY (tag_name),
                            tag_name CHAR({self.mariadb_index_prefix_limit}) NOT NULL
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {tag_name_table_name} (
                            tag_name TEXT NOT NULL PRIMARY KEY
                        )
                    """
            connector.execute(query)
            self.logger.info(f"{tag_name_table_name} table created.")

            tag_value_table_name = "galleries_tags_values"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {tag_value_table_name} (
                            PRIMARY KEY (tag_value),
                            tag_value CHAR({self.mariadb_index_prefix_limit}) NOT NULL
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {tag_value_table_name} (
                            tag_value TEXT NOT NULL PRIMARY KEY
                        )
                    """
            connector.execute(query)
            self.logger.info(f"{tag_value_table_name} table created.")

            tag_pairs_table_name = "galleries_tag_pairs_dbids"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {tag_pairs_table_name} (
                            PRIMARY KEY (db_tag_pair_id),
                            db_tag_pair_id INT UNSIGNED                           AUTO_INCREMENT,
                            tag_name       CHAR({self.mariadb_index_prefix_limit}) NOT NULL,
                            FOREIGN KEY (tag_name) REFERENCES {tag_name_table_name}(tag_name)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            tag_value      CHAR({self.mariadb_index_prefix_limit}) NOT NULL,
                            FOREIGN KEY (tag_value) REFERENCES {tag_value_table_name}(tag_value)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            UNIQUE (tag_name, tag_value),
                            INDEX (tag_value)
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {tag_pairs_table_name} (
                            db_tag_pair_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            tag_name       TEXT NOT NULL
                                REFERENCES {tag_name_table_name}(tag_name)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            tag_value      TEXT NOT NULL
                                REFERENCES {tag_value_table_name}(tag_value)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            UNIQUE (tag_name, tag_value)
                        )
                    """
            connector.execute(query)

            match self.config.database.sql_type.lower():
                case "sqlite":
                    connector.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_{tag_pairs_table_name}_tag_value "
                        f"ON {tag_pairs_table_name}(tag_value)"
                    )

            self.logger.info(f"{tag_pairs_table_name} table created.")

            table_name = "galleries_tags"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_gallery_id, db_tag_pair_id),
                            db_gallery_id  INT UNSIGNED NOT NULL,
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_tag_pair_id INT UNSIGNED NOT NULL,
                            FOREIGN KEY (db_tag_pair_id) REFERENCES {tag_pairs_table_name}(db_tag_pair_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            UNIQUE (db_tag_pair_id, db_gallery_id)
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            db_gallery_id  INTEGER NOT NULL
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            db_tag_pair_id INTEGER NOT NULL
                                REFERENCES {tag_pairs_table_name}(db_tag_pair_id)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            PRIMARY KEY (db_gallery_id, db_tag_pair_id),
                            UNIQUE (db_tag_pair_id, db_gallery_id)
                        )
                    """
            connector.execute(query)
            self.logger.info(f"{table_name} table created.")

    @property
    def _insert_rows_batch_size(self) -> int:
        return TAG_BATCH_SIZE

    def _get_existing_tag_names(self, tag_names: set[str]) -> set[str]:
        return self._get_existing_tag_values_by_table(
            "galleries_tags_names", "tag_name", tag_names
        )

    def _get_existing_tag_values(self, tag_values: set[str]) -> set[str]:
        return self._get_existing_tag_values_by_table(
            "galleries_tags_values", "tag_value", tag_values
        )

    def _get_existing_tag_values_by_table(
        self, table_name: str, column_name: str, values: set[str]
    ) -> set[str]:
        if not values:
            return set()

        existing_values = set[str]()
        value_list = list(values)
        with self.SQLConnector() as connector:
            for start in range(0, len(value_list), TAG_BATCH_SIZE):
                batch = value_list[start : start + TAG_BATCH_SIZE]
                select_query = f"""
                    SELECT {column_name}
                    FROM {table_name}
                    WHERE {column_name} IN ({", ".join(["%s"] * len(batch))})
                """
                query_result = connector.fetch_all(select_query, tuple(batch))
                existing_values.update(str(value) for value, in query_result)
        return existing_values

    def _get_db_tag_pair_ids_by_tag_pairs(
        self, tag_pairs: set[tuple[str, str]]
    ) -> dict[tuple[str, str], int]:
        if not tag_pairs:
            return {}

        tag_pair_ids = dict[tuple[str, str], int]()
        tag_pair_list = list(tag_pairs)
        with self.SQLConnector() as connector:
            for start in range(0, len(tag_pair_list), TAG_BATCH_SIZE):
                batch = tag_pair_list[start : start + TAG_BATCH_SIZE]
                where_clause = " OR ".join(
                    ["(tag_name = %s AND tag_value = %s)" for _ in batch]
                )
                select_query = f"""
                    SELECT tag_name, tag_value, db_tag_pair_id
                    FROM galleries_tag_pairs_dbids
                    WHERE {where_clause}
                """
                parameters = tuple(chain.from_iterable(batch))
                query_result = connector.fetch_all(select_query, parameters)
                for tag_name, tag_value, db_tag_pair_id in query_result:
                    tag_pair_ids[(str(tag_name), str(tag_value))] = int(db_tag_pair_id)
        return tag_pair_ids

    def _insert_tag_rows_with_retry(
        self, table_name: str, columns: list[str], rows: list[tuple[Any, ...]]
    ) -> None:
        if not rows:
            return

        try:
            self._insert_rows(table_name, columns, rows)
            return
        except DatabaseDuplicateKeyError:
            pass

        if len(rows) == 1:
            try:
                self._insert_rows(table_name, columns, rows)
            except DatabaseDuplicateKeyError:
                pass
            return

        mid = len(rows) // 2
        self._insert_tag_rows_with_retry(table_name, columns, rows[:mid])
        self._insert_tag_rows_with_retry(table_name, columns, rows[mid:])

    def _insert_gallery_tags_many(
        self, tags_by_gallery_id: dict[int, list[TagInformation]]
    ) -> None:
        tags = [
            tag for gallery_tags in tags_by_gallery_id.values() for tag in gallery_tags
        ]
        if not tags:
            return

        tag_names = {tag.tag_name for tag in tags}
        existing_tag_names = self._get_existing_tag_names(tag_names)
        self._insert_tag_rows_with_retry(
            "galleries_tags_names",
            ["tag_name"],
            [(tag_name,) for tag_name in tag_names - existing_tag_names],
        )

        tag_values = {tag.tag_value for tag in tags}
        existing_tag_values = self._get_existing_tag_values(tag_values)
        self._insert_tag_rows_with_retry(
            "galleries_tags_values",
            ["tag_value"],
            [(tag_value,) for tag_value in tag_values - existing_tag_values],
        )

        tag_pairs = {(tag.tag_name, tag.tag_value) for tag in tags}
        existing_tag_pair_ids = self._get_db_tag_pair_ids_by_tag_pairs(tag_pairs)
        self._insert_tag_rows_with_retry(
            "galleries_tag_pairs_dbids",
            ["tag_name", "tag_value"],
            [
                tag_pair
                for tag_pair in tag_pairs
                if tag_pair not in existing_tag_pair_ids
            ],
        )
        db_tag_pair_ids = self._get_db_tag_pair_ids_by_tag_pairs(tag_pairs)

        gallery_tag_rows = list[tuple[int, int]]()
        for db_gallery_id, gallery_tags in tags_by_gallery_id.items():
            for tag in gallery_tags:
                tag_pair = (tag.tag_name, tag.tag_value)
                gallery_tag_rows.append((db_gallery_id, db_tag_pair_ids[tag_pair]))
        self._insert_tag_rows_with_retry(
            "galleries_tags",
            ["db_gallery_id", "db_tag_pair_id"],
            gallery_tag_rows,
        )

    def _select_gallery_tag(self, db_gallery_id: int, tag_name: str) -> str:
        with self.SQLConnector() as connector:
            table_name = f"galleries_tags_{tag_name}"
            select_query = f"""
                SELECT tag
                FROM {table_name}
                WHERE db_gallery_id = %s
            """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
        if query_result:
            tag = str(query_result[0])
        else:
            msg = f"Tag '{tag_name}' does not exist."
            self.logger.error(msg)
            raise DatabaseKeyError(msg)
        return tag

    def get_tag_value_by_gallery_name_and_tag_name(
        self, gallery_name: str, tag_name: str
    ) -> str:
        db_gallery_id = self.gallery_ids._get_db_gallery_id_by_gallery_name(
            gallery_name
        )
        return self._select_gallery_tag(db_gallery_id, tag_name)

    def get_tag_pairs_by_gallery_name(self, gallery_name: str) -> list[tuple[str, str]]:
        db_gallery_id = self.gallery_ids._get_db_gallery_id_by_gallery_name(
            gallery_name
        )
        db_tag_pair_ids = self._get_db_tag_pair_id_by_db_gallery_id(db_gallery_id)
        return [
            self._get_tag_pairs_by_db_tag_pair_id(db_tag_pair_id)
            for db_tag_pair_id in db_tag_pair_ids
        ]

    def _get_db_tag_pair_id_by_db_gallery_id(self, db_gallery_id: int) -> list[int]:
        with self.SQLConnector() as connector:
            table_name = "galleries_tags"
            select_query = f"""
                SELECT db_tag_pair_id
                FROM {table_name}
                WHERE db_gallery_id = %s
            """
            query_result = connector.fetch_all(select_query, (db_gallery_id,))
        return [query[0] for query in query_result]

    def _get_tag_pairs_by_db_tag_pair_id(self, db_tag_pair_id: int) -> tuple[str, str]:
        with self.SQLConnector() as connector:
            table_name = "galleries_tag_pairs_dbids"
            select_query = f"""
                SELECT tag_name, tag_value
                FROM {table_name}
                WHERE db_tag_pair_id = %s
            """
            query_result = connector.fetch_one(select_query, (db_tag_pair_id,))
        if query_result:
            tag_name, tag_value = query_result
        else:
            msg = f"Tag pair ID {db_tag_pair_id} does not exist."
            self.logger.error(msg)
            raise DatabaseKeyError(msg)
        return tag_name, tag_value
