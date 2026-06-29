from itertools import chain
from typing import cast

from .hash_dict import HASH_ALGORITHMS
from .information import FileInformation
from .repository import BaseRepository, RepositoryContext
from .settings import FILE_NAME_LENGTH_LIMIT, hash_multiple_by_file
from .sql_connector import (
    DatabaseDuplicateKeyError,
    DatabaseKeyError,
)
from .table_gids import H2HDBGalleriesIDs

HASH_LOOKUP_BATCH_SIZE = 500


class H2HDBFiles(BaseRepository):
    def __init__(
        self, context: RepositoryContext, gallery_ids: H2HDBGalleriesIDs
    ) -> None:
        super().__init__(context)
        self.gallery_ids = gallery_ids

    def _create_files_names_table(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "files_dbids"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    column_name_parts, create_gallery_name_parts_sql = (
                        self.mariadb_split_file_name_based_on_limit("name")
                    )
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_file_id),
                            db_file_id    INT UNSIGNED AUTO_INCREMENT,
                            db_gallery_id INT UNSIGNED NOT NULL,
                            FOREIGN KEY (db_gallery_id) REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            {create_gallery_name_parts_sql},
                            UNIQUE real_primay_key (db_gallery_id, {", ".join(column_name_parts)}),
                            UNIQUE db_file_to_gallery_id (db_file_id, db_gallery_id)
                        )
                    """
                case "sqlite":
                    column_name_parts, create_gallery_name_parts_sql = (
                        self.sqlite_name_columns("name")
                    )
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            db_file_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                            db_gallery_id INTEGER NOT NULL
                                REFERENCES galleries_dbids(db_gallery_id)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            {create_gallery_name_parts_sql},
                            UNIQUE (db_gallery_id, {", ".join(column_name_parts)}),
                            UNIQUE (db_file_id, db_gallery_id)
                        )
                    """
            connector.execute(query)
            self.logger.info(f"{table_name} table created.")

            table_name = "files_names"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_file_id),
                            FOREIGN KEY (db_file_id) REFERENCES files_dbids(db_file_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_file_id  INT UNSIGNED NOT NULL,
                            full_name   TEXT         NOT NULL,
                            FULLTEXT (full_name)
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            db_file_id INTEGER NOT NULL PRIMARY KEY
                                REFERENCES files_dbids(db_file_id)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            full_name TEXT NOT NULL
                        )
                    """
            connector.execute(query)

            match self.config.database.sql_type.lower():
                case "sqlite":
                    self._create_sqlite_fts5_sync(
                        connector, table_name, "full_name", "db_file_id"
                    )

            self.logger.info(f"{table_name} table created.")

    def _insert_gallery_files(
        self, db_gallery_id: int, file_names_list: list[str]
    ) -> dict[str, int]:
        with self.SQLConnector() as connector:

            file_name_parts_list: list[list[str]] = list()
            for file_name in file_names_list:
                if len(file_name) > FILE_NAME_LENGTH_LIMIT:
                    self.logger.error(
                        f"File name '{file_name}' is too long. Must be {FILE_NAME_LENGTH_LIMIT} characters or less."
                    )
                    raise ValueError("File name is too long.")
                file_name_parts_list.append(self._split_gallery_name(file_name))

            table_name = "files_dbids"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    column_name_parts, _ = self.mariadb_split_file_name_based_on_limit(
                        "name"
                    )
                case "sqlite":
                    column_name_parts, _ = self.sqlite_name_columns("name")
            insert_query_header = f"""
                INSERT INTO {table_name}
                    (db_gallery_id, {", ".join(column_name_parts)})
            """
            insert_query_values = " ".join(
                [
                    "VALUES",
                    ", ".join(
                        [
                            f"(%s, {", ".join(["%s" for _ in column_name_parts])})"
                            for _ in file_names_list
                        ]
                    ),
                ]
            )
            insert_query = f"{insert_query_header} {insert_query_values}"
            insert_parameter = tuple(
                chain(
                    *[
                        (db_gallery_id, *file_name_parts_list[n])
                        for n in range(len(file_name_parts_list))
                    ]
                )
            )
            connector.execute(
                insert_query,
                insert_parameter,
            )

            select_query = f"""
                SELECT {", ".join(column_name_parts)}, db_file_id
                FROM files_dbids
                WHERE db_gallery_id = %s
            """
            query_result = connector.fetch_all(select_query, (db_gallery_id,))
            db_file_id_by_parts = {
                tuple(str(part) for part in row[:-1]): int(row[-1])
                for row in query_result
            }
            db_file_id_list = [
                db_file_id_by_parts[tuple(file_name_parts_list[n])]
                for n in range(len(file_names_list))
            ]

            table_name = "files_names"
            insert_query_header = f"""
                INSERT INTO {table_name}
                    (db_file_id, full_name)
            """
            insert_query_values = " ".join(
                ["VALUES", ", ".join(["(%s, %s)" for _ in file_names_list])]
            )
            insert_query = f"{insert_query_header} {insert_query_values}"

            connector.execute(
                insert_query,
                tuple(
                    chain(
                        *[
                            (db_file_id_list[n], file_names_list[n])
                            for n in range(len(file_names_list))
                        ]
                    )
                ),
            )

        return dict(zip(file_names_list, db_file_id_list, strict=True))

    def __get_db_file_id(self, db_gallery_id: int, file_name: str) -> tuple[int, ...]:
        with self.SQLConnector() as connector:
            table_name = "files_dbids"
            file_name_parts = self._split_gallery_name(file_name)
            match self.config.database.sql_type.lower():
                case "mariadb":
                    column_name_parts, _ = self.mariadb_split_file_name_based_on_limit(
                        "name"
                    )
                case "sqlite":
                    column_name_parts, _ = self.sqlite_name_columns("name")
            select_query = f"""
                SELECT db_file_id
                FROM {table_name}
                WHERE db_gallery_id = %s
                AND {" AND ".join([f"{part} = %s" for part in column_name_parts])}
            """
            data = (db_gallery_id, *file_name_parts)
            query_result = connector.fetch_one(select_query, data)
        return query_result

    def _check_db_file_id(self, db_gallery_id: int, file_name: str) -> bool:
        query_result = self.__get_db_file_id(db_gallery_id, file_name)
        return len(query_result) != 0

    def _get_db_file_id(self, db_gallery_id: int, file_name: str) -> int:
        query_result = self.__get_db_file_id(db_gallery_id, file_name)
        if query_result:
            gallery_image_id = int(query_result[0])
        else:
            msg = f"Image ID for gallery name ID {db_gallery_id} and file '{file_name}' does not exist."
            self.logger.error(msg)
            raise DatabaseKeyError(msg)
        return gallery_image_id

    def get_files_by_gallery_name(self, gallery_name: str) -> list[str]:
        with self.SQLConnector() as connector:
            db_gallery_id = self.gallery_ids._get_db_gallery_id_by_gallery_name(
                gallery_name
            )
            select_query = """
                SELECT files_names.full_name
                FROM files_names
                JOIN files_dbids USING (db_file_id)
                WHERE files_dbids.db_gallery_id = %s
            """
            query_result = connector.fetch_all(select_query, (db_gallery_id,))
        if query_result:
            files = [query[0] for query in query_result]
        else:
            msg = f"Files for gallery name ID {db_gallery_id} do not exist."
            self.logger.error(msg)
            raise DatabaseKeyError(msg)
        return files

    def _create_galleries_files_hashs_table(
        self, algorithm: str, output_bits: int
    ) -> None:
        with self.SQLConnector() as connector:
            dbids_table_name = f"files_hashs_{algorithm.lower()}_dbids"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {dbids_table_name} (
                            PRIMARY KEY (db_hash_id),
                            db_hash_id INT UNSIGNED AUTO_INCREMENT,
                            hash_value BINARY({output_bits/8}) NOT NULL,
                            UNIQUE (hash_value)
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {dbids_table_name} (
                            db_hash_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            hash_value BLOB NOT NULL,
                            UNIQUE (hash_value)
                        )
                    """
            connector.execute(query)
            self.logger.info(f"{dbids_table_name} table created.")

            table_name = f"files_hashs_{algorithm.lower()}"
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            PRIMARY KEY (db_file_id),
                            FOREIGN KEY (db_file_id) REFERENCES files_dbids(db_file_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            db_file_id INT UNSIGNED NOT NULL,
                            FOREIGN KEY (db_hash_id) REFERENCES {dbids_table_name}(db_hash_id)
                                ON UPDATE CASCADE,
                            db_hash_id INT UNSIGNED NOT NULL,
                            UNIQUE db_hash_id (db_hash_id, db_file_id)
                        )
                    """
                case "sqlite":
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            db_file_id INTEGER NOT NULL PRIMARY KEY
                                REFERENCES files_dbids(db_file_id)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            db_hash_id INTEGER NOT NULL
                                REFERENCES {dbids_table_name}(db_hash_id)
                                ON UPDATE CASCADE,
                            UNIQUE (db_hash_id, db_file_id)
                        )
                    """
            connector.execute(query)
            self.logger.info(f"{table_name} table created.")

    def _create_galleries_files_hashs_tables(self) -> None:
        self.logger.debug("Creating gallery image hash tables...")
        for algorithm, output_bits in HASH_ALGORITHMS.items():
            self._create_galleries_files_hashs_table(algorithm, output_bits)
        self.logger.info("Gallery image hash tables created.")

    def _create_gallery_image_hash_view(self) -> None:
        with self.SQLConnector() as connector:
            table_name = "files_hashs"
            query = f"""
                CREATE VIEW IF NOT EXISTS {table_name} AS
                SELECT files_names.db_file_id               AS db_file_id,
                    galleries_titles.title               AS gallery_title,
                    galleries_names.full_name            AS gallery_name,
                    files_names.full_name                AS file_name,
                    files_hashs_sha512_dbids.hash_value  AS sha512
                FROM files_names
                    LEFT JOIN files_dbids                USING (db_file_id)
                    LEFT JOIN galleries_titles           USING (db_gallery_id)
                    LEFT JOIN galleries_names            USING (db_gallery_id)
                    LEFT JOIN files_hashs_sha512         USING (db_file_id)
                    LEFT JOIN files_hashs_sha512_dbids   USING (db_hash_id)
            """
            connector.execute(query)
            self.logger.info(f"{table_name} view created.")

    def _check_files_dbids_by_db_gallery_id(self, db_gallery_id: int) -> bool:
        with self.SQLConnector() as connector:
            table_name = "files_dbids"
            select_query = f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE db_gallery_id = %s
            """
            query_result = connector.fetch_one(select_query, (db_gallery_id,))
        return bool(query_result[0] != 0)

    def _insert_gallery_file_hash_for_db_gallery_id(
        self, fileinformations: list[FileInformation]
    ) -> None:
        for finfo in fileinformations:
            finfo.sethash()

        for algorithm in HASH_ALGORITHMS:
            hash_values = {
                cast(bytes, getattr(finfo, algorithm)) for finfo in fileinformations
            }
            self.insert_db_hash_id_by_hash_values(hash_values, algorithm)

        for algorithm in HASH_ALGORITHMS:
            hash_values = {
                cast(bytes, getattr(finfo, algorithm)) for finfo in fileinformations
            }
            db_hash_ids = self._get_db_hash_ids_by_hash_values(hash_values, algorithm)
            for finfo in fileinformations:
                hash_value = cast(bytes, getattr(finfo, algorithm))
                if hash_value not in db_hash_ids:
                    msg = (
                        f"Image hash for image ID 0x{hash_value.hex()} does not exist."
                    )
                    raise DatabaseKeyError(msg)
                finfo.setdb_hash_id(
                    algorithm,
                    db_hash_ids[hash_value],
                )
        self.insert_hash_value_by_db_hash_ids(fileinformations)

    def _insert_gallery_file_hash(
        self, db_file_id: int, absolute_file_path: str
    ) -> None:
        current_hash_values = hash_multiple_by_file(absolute_file_path, HASH_ALGORITHMS)

        for algorithm in HASH_ALGORITHMS:
            is_insert = False
            current_hash_value = current_hash_values[algorithm]
            if self._check_hash_value_by_file_id(db_file_id, algorithm):
                original_hash_value = self.get_hash_value_by_file_id(
                    db_file_id, algorithm
                )
                if original_hash_value != current_hash_value:
                    if self._check_db_hash_id_by_hash_value(
                        current_hash_value, algorithm
                    ):
                        db_hash_id = self.get_db_hash_id_by_hash_value(
                            current_hash_value, algorithm
                        )
                        self._update_gallery_file_hash_by_db_hash_id(
                            db_file_id, db_hash_id, algorithm
                        )
                    else:
                        is_insert |= True
            else:
                is_insert |= True

            if is_insert:
                if self._check_db_hash_id_by_hash_value(current_hash_value, algorithm):
                    db_hash_id = self.get_db_hash_id_by_hash_value(
                        current_hash_value, algorithm
                    )
                else:
                    with self.SQLConnector() as connector:
                        table_name = f"files_hashs_{algorithm.lower()}_dbids"
                        insert_hash_value_query = f"""
                            INSERT INTO {table_name} (hash_value) VALUES (%s)
                        """
                        try:
                            connector.execute(
                                insert_hash_value_query, (current_hash_value,)
                            )
                        except DatabaseDuplicateKeyError:
                            self.logger.warning(
                                f"Hash value {current_hash_value!r} already exists in the database."
                            )
                        except Exception as e:
                            raise e
                    db_hash_id = self.get_db_hash_id_by_hash_value(
                        current_hash_value, algorithm
                    )

                with self.SQLConnector() as connector:
                    table_name = f"files_hashs_{algorithm.lower()}"
                    insert_db_hash_id_query = f"""
                        INSERT INTO {table_name} (db_file_id, db_hash_id) VALUES (%s, %s)
                    """
                    connector.execute(insert_db_hash_id_query, (db_file_id, db_hash_id))

    def __get_db_hash_id_by_hash_value(
        self, hash_value: bytes, algorithm: str
    ) -> tuple[int, ...]:
        with self.SQLConnector() as connector:
            table_name = f"files_hashs_{algorithm.lower()}_dbids"
            select_query = f"""
                SELECT db_hash_id
                FROM {table_name}
                WHERE hash_value = %s
            """
            query_result = connector.fetch_one(select_query, (hash_value,))
        return query_result

    def _check_db_hash_id_by_hash_value(
        self, hash_value: bytes, algorithm: str
    ) -> bool:
        query_result = self.__get_db_hash_id_by_hash_value(hash_value, algorithm)
        return len(query_result) != 0

    def get_db_hash_id_by_hash_value(self, hash_value: bytes, algorithm: str) -> int:
        query_result = self.__get_db_hash_id_by_hash_value(hash_value, algorithm)
        if query_result:
            db_hash_id = int(query_result[0])
        else:
            msg = f"Image hash for image ID 0x{hash_value.hex()} does not exist."
            raise DatabaseKeyError(msg)
        return db_hash_id

    def _get_db_hash_ids_by_hash_values(
        self, hash_values: set[bytes], algorithm: str
    ) -> dict[bytes, int]:
        if not hash_values:
            return {}

        table_name = f"files_hashs_{algorithm.lower()}_dbids"
        hash_value_list = list(hash_values)
        db_hash_ids = dict[bytes, int]()
        with self.SQLConnector() as connector:
            for start in range(0, len(hash_value_list), HASH_LOOKUP_BATCH_SIZE):
                batch = hash_value_list[start : start + HASH_LOOKUP_BATCH_SIZE]
                select_query = f"""
                    SELECT hash_value, db_hash_id
                    FROM {table_name}
                    WHERE hash_value IN ({", ".join(["%s"] * len(batch))})
                """
                query_result = connector.fetch_all(select_query, tuple(batch))
                for hash_value, db_hash_id in query_result:
                    db_hash_ids[cast(bytes, hash_value)] = int(db_hash_id)
        return db_hash_ids

    def insert_hash_value_by_db_hash_ids(
        self, fileinformations: list[FileInformation]
    ) -> None:
        for algorithm in HASH_ALGORITHMS:
            with self.SQLConnector() as connector:
                table_name = f"files_hashs_{algorithm.lower()}"
                insert_query_header = f"""
                    INSERT INTO {table_name} (db_file_id, db_hash_id)
                """
                insert_query_values = " ".join(
                    ["VALUES", ", ".join(["(%s, %s)"] * len(fileinformations))]
                )
                insert_query = f"{insert_query_header} {insert_query_values}"
                parameters: list[int] = list()
                for fileinformation in fileinformations:
                    parameters += [
                        fileinformation.db_file_id,
                        fileinformation.db_hash_id[algorithm],
                    ]
                connector.execute(insert_query, tuple(parameters))

    def insert_db_hash_id_by_hash_value(
        self, hash_value: bytes, algorithm: str
    ) -> None:
        with self.SQLConnector() as connector:
            table_name = f"files_hashs_{algorithm.lower()}_dbids"
            insert_query = f"""
                INSERT INTO {table_name} (hash_value) VALUES (%s)
            """
            connector.execute(insert_query, (hash_value,))

    def _insert_db_hash_ids_with_split_retry(
        self, hash_values: list[bytes], algorithm: str
    ) -> None:
        if not hash_values:
            return

        table_name = f"files_hashs_{algorithm.lower()}_dbids"
        with self.SQLConnector() as connector:
            insert_query_header = f"""
                INSERT INTO {table_name} (hash_value)
            """
            insert_query_values = " ".join(
                ["VALUES", ", ".join(["(%s)"] * len(hash_values))]
            )
            insert_query = f"{insert_query_header} {insert_query_values}"
            try:
                connector.execute(insert_query, tuple(hash_values))
                return
            except DatabaseDuplicateKeyError:
                pass

        existing_hash_ids = self._get_db_hash_ids_by_hash_values(
            set(hash_values), algorithm
        )
        missing_hash_values = [
            hash_value
            for hash_value in hash_values
            if hash_value not in existing_hash_ids
        ]
        if not missing_hash_values:
            return

        if len(missing_hash_values) == 1:
            hash_value = missing_hash_values[0]
            self.logger.warning(
                f"Retrying to insert hash value 0x{hash_value.hex()} into the database."
            )
            try:
                self.insert_db_hash_id_by_hash_value(hash_value, algorithm)
            except DatabaseDuplicateKeyError:
                if not self._check_db_hash_id_by_hash_value(hash_value, algorithm):
                    raise
            return

        mid = len(missing_hash_values) // 2
        self._insert_db_hash_ids_with_split_retry(missing_hash_values[:mid], algorithm)
        self._insert_db_hash_ids_with_split_retry(missing_hash_values[mid:], algorithm)

    def insert_db_hash_id_by_hash_values(
        self, hash_values: set[bytes], algorithm: str
    ) -> None:
        if not hash_values:
            return

        existing_hash_ids = self._get_db_hash_ids_by_hash_values(hash_values, algorithm)
        toinsert = [
            hash_value
            for hash_value in hash_values
            if hash_value not in existing_hash_ids
        ]
        if not toinsert:
            return

        for start in range(0, len(toinsert), HASH_LOOKUP_BATCH_SIZE):
            batch = toinsert[start : start + HASH_LOOKUP_BATCH_SIZE]
            self._insert_db_hash_ids_with_split_retry(batch, algorithm)

    def get_hash_value_by_db_hash_id(self, db_hash_id: int, algorithm: str) -> bytes:
        with self.SQLConnector() as connector:
            table_name = f"files_hashs_{algorithm.lower()}_dbids"
            select_query = f"""
                SELECT hash_value
                FROM {table_name}
                WHERE db_hash_id = %s
            """
            query_result = connector.fetch_one(select_query, (db_hash_id,))
        if query_result:
            hash_value = cast(bytes, query_result[0])
        else:
            msg = f"Image hash for image ID {db_hash_id} does not exist."
            raise DatabaseKeyError(msg)
        return hash_value

    def __get_hash_value_by_file_id(
        self, db_file_id: int, algorithm: str
    ) -> tuple[int, ...]:
        with self.SQLConnector() as connector:
            table_name = f"files_hashs_{algorithm.lower()}"
            select_query = f"""
                SELECT db_hash_id
                FROM {table_name}
                WHERE db_file_id = %s
            """
            query_result = connector.fetch_one(select_query, (db_file_id,))
        return query_result

    def _check_hash_value_by_file_id(self, db_file_id: int, algorithm: str) -> bool:
        query_result = self.__get_hash_value_by_file_id(db_file_id, algorithm)
        return len(query_result) != 0

    def get_hash_value_by_file_id(self, db_file_id: int, algorithm: str) -> bytes:
        query_result = self.__get_hash_value_by_file_id(db_file_id, algorithm)
        if query_result:
            db_hash_id = int(query_result[0])
        else:
            msg = f"Image hash for image ID {db_file_id} does not exist."
            raise DatabaseKeyError(msg)
        return self.get_hash_value_by_db_hash_id(db_hash_id, algorithm)

    def _update_gallery_file_hash_by_db_hash_id(
        self, db_file_id: int, db_hash_id: int, algorithm: str
    ) -> None:
        with self.SQLConnector() as connector:
            table_name = f"files_hashs_{algorithm.lower()}"
            update_query = f"""
                UPDATE {table_name} SET db_hash_id = %s WHERE db_file_id = %s
            """
            connector.execute(update_query, (db_hash_id, db_file_id))
