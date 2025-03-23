from abc import ABCMeta

from .table_gids import H2HDBGalleriesIDs
from .h2hdb_spec import H2HDBAbstract
from .information import TagInformation
from .sql_connector import (
    DatabaseKeyError,
    DatabaseDuplicateKeyError,
)


class H2HDBGalleriesTags(H2HDBGalleriesIDs, H2HDBAbstract, metaclass=ABCMeta):
    def _create_galleries_tags_table(self) -> None:
        with self.SQLConnector() as connector:
            tag_name_table_name = f"galleries_tags_names"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {tag_name_table_name} (
                            PRIMARY KEY (tag_name),
                            tag_name CHAR({self.innodb_index_prefix_limit}) NOT NULL
                        )
                    """
            connector.execute(query)
            self.logger.info(f"{tag_name_table_name} table created.")

            tag_value_table_name = f"galleries_tags_values"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {tag_value_table_name} (
                            PRIMARY KEY (tag_value),
                            tag_value CHAR({self.innodb_index_prefix_limit}) NOT NULL
                        )
                    """
            connector.execute(query)
            self.logger.info(f"{tag_value_table_name} table created.")

            tag_pairs_table_name = f"galleries_tag_pairs_dbids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {tag_pairs_table_name} (
                            PRIMARY KEY (db_tag_pair_id),
                            db_tag_pair_id INT UNSIGNED                           AUTO_INCREMENT,
                            tag_name       CHAR({self.innodb_index_prefix_limit}) NOT NULL,
                            FOREIGN KEY (tag_name) REFERENCES {tag_name_table_name}(tag_name)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            tag_value      CHAR({self.innodb_index_prefix_limit}) NOT NULL,
                            FOREIGN KEY (tag_value) REFERENCES {tag_value_table_name}(tag_value)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            UNIQUE (tag_name, tag_value),
                            INDEX (tag_value)
                        )
                    """
            connector.execute(query)
            self.logger.info(f"{tag_pairs_table_name} table created.")

            table_name = f"galleries_tags"
            match self.config.database.sql_type.lower():
                case "mysql":
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
            connector.execute(query)
            self.logger.info(f"{table_name} table created.")

    def __get_db_tag_pair_id(self, tag_name: str, tag_value: str) -> tuple | None:
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT db_tag_pair_id
                        FROM galleries_tag_pairs_dbids
                        WHERE tag_name = %s AND tag_value = %s
                    """
            query_result = connector.fetch_one(select_query, (tag_name, tag_value))
        return query_result

    def _check_db_tag_pair_id(self, tag_name: str, tag_value: str) -> bool:
        query_result = self.__get_db_tag_pair_id(tag_name, tag_value)
        return query_result is not None

    def _get_db_tag_pair_id(self, tag_name: str, tag_value: str) -> int:
        query_result = self.__get_db_tag_pair_id(tag_name, tag_value)
        if query_result is None:
            self.logger.debug(f"Tag '{tag_value}' does not exist.")
            raise DatabaseKeyError(f"Tag '{tag_value}' does not exist.")
        else:
            db_tag_id = query_result[0]
        return db_tag_id

    def _check_gallery_tag_name(self, tag_name: str) -> bool:
        with self.SQLConnector() as connector:
            table_name = f"galleries_tags_names"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT tag_name
                        FROM {table_name}
                        WHERE tag_name = %s
                    """
            query_result = connector.fetch_one(select_query, (tag_name,))
        return query_result is not None

    def _check_gallery_tag_value(self, tag_value: str) -> bool:
        with self.SQLConnector() as connector:
            table_name = f"galleries_tags_values"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT tag_value
                        FROM {table_name}
                        WHERE tag_value = %s
                    """
            query_result = connector.fetch_one(select_query, (tag_value,))
        return query_result is not None

    def _insert_tag_names(self, tag_names: list[str]) -> None:
        if len(tag_names) == 0:
            return
        with self.SQLConnector() as connector:
            table_name = f"galleries_tags_names"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query_header = f"""
                        INSERT INTO {table_name} (tag_name)
                    """
                    insert_query_values = " ".join(
                        ["VALUES", ", ".join(["(%s)" for _ in tag_names])]
                    )
                    insert_query = f"{insert_query_header} {insert_query_values}"
            try:
                connector.execute(insert_query, tuple(tag_names))
            except DatabaseDuplicateKeyError:
                pass
            except Exception as e:
                raise e

    def _insert_tag_values(self, tag_values: list[str]) -> None:
        if len(tag_values) == 0:
            return
        with self.SQLConnector() as connector:
            table_name = f"galleries_tags_values"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query_header = f"""
                        INSERT INTO {table_name} (tag_value)
                    """
                    insert_query_values = " ".join(
                        ["VALUES", ", ".join(["(%s)" for _ in tag_values])]
                    )
                    insert_query = f"{insert_query_header} {insert_query_values}"
            try:
                connector.execute(insert_query, tuple(tag_values))
            except DatabaseDuplicateKeyError:
                pass
            except Exception as e:
                raise e

    def _insert_tag_pairs_dbids(
        self, tags: list[TagInformation], retry: int = 5
    ) -> None:
        if len(tags) == 0:
            return
        with self.SQLConnector() as connector:
            tag_pairs_table_name = f"galleries_tag_pairs_dbids"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query_header = f"""
                        INSERT INTO {tag_pairs_table_name} (tag_name, tag_value)
                    """
                    insert_query_values = " ".join(
                        ["VALUES", ", ".join(["(%s, %s)" for _ in tags])]
                    )
                    insert_query = f"{insert_query_header} {insert_query_values}"
            parameter = list[str]()
            for tag in tags:
                parameter.extend([tag.tag_name, tag.tag_value])
            try:
                connector.execute(insert_query, tuple(parameter))
            except DatabaseDuplicateKeyError:
                toinsert_db_tag_pair_id = list[TagInformation]()
                for tag in tags:
                    if not self._check_db_tag_pair_id(tag.tag_name, tag.tag_value):
                        toinsert_db_tag_pair_id.append(tag)
                try:
                    self._insert_tag_pairs_dbids(toinsert_db_tag_pair_id)
                except DatabaseDuplicateKeyError as e:
                    if retry > 0:
                        self._insert_tag_pairs_dbids(toinsert_db_tag_pair_id, retry - 1)
                    else:
                        raise e
            except Exception as e:
                raise e

    def _insert_gallery_tags(
        self, db_gallery_id: int, tags: list[TagInformation]
    ) -> None:
        if len(tags) == 0:
            return

        toinsert_db_tag_pair_id = list[TagInformation]()
        for tag in tags:
            if not self._check_db_tag_pair_id(tag.tag_name, tag.tag_value):
                toinsert_db_tag_pair_id.append(tag)

        toinsert_tag_name = list[str]()
        toinsert_tag_value = list[str]()
        for tag in toinsert_db_tag_pair_id:
            if (tag.tag_name not in toinsert_tag_name) and (
                not self._check_gallery_tag_name(tag.tag_name)
            ):
                toinsert_tag_name.append(tag.tag_name)
            if (tag.tag_value not in toinsert_tag_value) and (
                not self._check_gallery_tag_value(tag.tag_value)
            ):
                toinsert_tag_value.append(tag.tag_value)
        self._insert_tag_names(toinsert_tag_name)
        self._insert_tag_values(toinsert_tag_value)

        self._insert_tag_pairs_dbids(toinsert_db_tag_pair_id)

        db_tag_pair_ids = list[int]()
        for tag in tags:
            db_tag_pair_ids.append(
                self._get_db_tag_pair_id(tag.tag_name, tag.tag_value)
            )

        with self.SQLConnector() as connector:
            table_name = f"galleries_tags"
            match self.config.database.sql_type.lower():
                case "mysql":
                    insert_query_header = f"""
                        INSERT INTO {table_name} (db_gallery_id, db_tag_pair_id)
                    """
                    insert_query_values = " ".join(
                        ["VALUES", ", ".join(["(%s, %s)" for _ in db_tag_pair_ids])]
                    )
                    insert_query = f"{insert_query_header} {insert_query_values}"
            parameter = list[int]()
            for db_tag_pair_id in db_tag_pair_ids:
                parameter.extend([db_gallery_id, db_tag_pair_id])
            connector.execute(insert_query, tuple(parameter))

    def _select_gallery_tag(self, db_gallery_id: int, tag_name: str) -> str:
        with self.SQLConnector() as connector:
            table_name = f"galleries_tags_{tag_name}"
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT tag
                        FROM {table_name}
                        WHERE db_gallery_id = %s
                    """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
            if query_result is None:
                msg = f"Tag '{tag_name}' does not exist."
                self.logger.error(msg)
                raise DatabaseKeyError(msg)
            else:
                tag = query_result[0]
        return tag

    def get_tag_value_by_gallery_name_and_tag_name(
        self, gallery_name: str, tag_name: str
    ) -> str:
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        return self._select_gallery_tag(db_gallery_id, tag_name)

    def get_tag_pairs_by_gallery_name(self, gallery_name: str) -> list[tuple[str, str]]:
        db_gallery_id = self._get_db_gallery_id_by_gallery_name(gallery_name)
        db_tag_pair_ids = self._get_db_tag_pair_id_by_db_gallery_id(db_gallery_id)
        return [
            self._get_tag_pairs_by_db_tag_pair_id(db_tag_pair_id)
            for db_tag_pair_id in db_tag_pair_ids
        ]

    def _get_db_tag_pair_id_by_db_gallery_id(self, db_gallery_id: int) -> list[int]:
        with self.SQLConnector() as connector:
            table_name = "galleries_tags"
            match self.config.database.sql_type.lower():
                case "mysql":
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
            match self.config.database.sql_type.lower():
                case "mysql":
                    select_query = f"""
                        SELECT tag_name, tag_value
                        FROM {table_name}
                        WHERE db_tag_pair_id = %s
                    """
            query_result = connector.fetch_one(select_query, (db_tag_pair_id,))
            if query_result is None:
                msg = f"Tag pair ID {db_tag_pair_id} does not exist."
                self.logger.error(msg)
                raise DatabaseKeyError(msg)
            else:
                tag_name, tag_value = query_result
        return tag_name, tag_value
