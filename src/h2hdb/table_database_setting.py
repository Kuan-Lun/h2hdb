from dataclasses import dataclass

from .repository import BaseRepository
from .sql_connector import DatabaseConfigurationError, SQLConnector


@dataclass(frozen=True, slots=True)
class ReclaimableSpace:
    name: str
    free_bytes: int
    allocated_bytes: int

    @property
    def free_ratio(self) -> float:
        if self.allocated_bytes <= 0:
            return 0.0
        return self.free_bytes / self.allocated_bytes


class H2HDBCheckDatabaseSettings(BaseRepository):
    def check_database_character_set(self) -> None:
        # SQLite has no database-level character set setting (TEXT is always UTF-8), so
        # this is a no-op for the "sqlite" backend.
        match self.config.database.sql_type.lower():
            case "sqlite":
                return

        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mariadb":
                    charset = "utf8mb4"
                    query = "SHOW VARIABLES LIKE 'character_set_database';"

            charset_result: str = connector.fetch_one(query)[1]
            is_charset_valid: bool = charset_result == charset
            if not is_charset_valid:
                message = f"Invalid database character set. Must be '{charset}' but is '{charset_result}'."
                self.logger.error(message)
                raise DatabaseConfigurationError(message)
            self.logger.info(f"Database character set verified: charset={charset}.")

    def check_database_collation(self) -> None:
        # SQLite has no database-level collation setting (collation is a
        # per-column/per-expression concept in SQLite), so this is a no-op for the
        # "sqlite" backend.
        match self.config.database.sql_type.lower():
            case "sqlite":
                return

        with self.SQLConnector() as connector:
            row = connector.fetch_one("SHOW VARIABLES LIKE 'collation_database';")
        if not row:
            raise DatabaseConfigurationError(
                "MariaDB did not report its database collation."
            )
        collation = str(row[1])
        expected_collation = "utf8mb4_bin"
        if collation != expected_collation:
            message = (
                "Invalid database collation. "
                f"Must be {expected_collation!r} but is {collation!r}."
            )
            self.logger.error(message)
            raise DatabaseConfigurationError(message)
        self.logger.info(
            f"Database collation verified: collation={expected_collation}."
        )

    def _get_all_table_names(self) -> list[str]:
        # KEY_COLUMN_USAGE only lists tables that themselves declare an
        # outgoing FOREIGN KEY, which silently skips top-level parent tables
        # such as galleries_dbids (nothing for them to reference). TABLES
        # covers every base table regardless of whether it has a FK.
        select_table_name_query = """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
        """
        with self.SQLConnector() as connector:
            raw_table_names = connector.fetch_all(
                select_table_name_query, (self.config.database.database,)
            )
        return sorted(str(t[0]) for t in raw_table_names)

    def get_reclaimable_space(self) -> list[ReclaimableSpace]:
        match self.config.database.sql_type.lower():
            case "mariadb":
                with self.SQLConnector() as connector:
                    rows = connector.fetch_all(
                        """
                        SELECT
                            TABLE_NAME,
                            COALESCE(DATA_FREE, 0),
                            COALESCE(DATA_LENGTH, 0)
                                + COALESCE(INDEX_LENGTH, 0)
                                + COALESCE(DATA_FREE, 0)
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = %s
                            AND TABLE_TYPE = 'BASE TABLE'
                        ORDER BY TABLE_NAME
                        """,
                        (self.config.database.database,),
                    )
                return [
                    ReclaimableSpace(
                        name=str(table_name),
                        free_bytes=int(data_free),
                        allocated_bytes=int(allocated_bytes),
                    )
                    for table_name, data_free, allocated_bytes in rows
                ]
            case "sqlite":
                with self.SQLConnector() as connector:
                    page_count = int(connector.fetch_one("PRAGMA page_count")[0])
                    free_pages = int(connector.fetch_one("PRAGMA freelist_count")[0])
                    page_size = int(connector.fetch_one("PRAGMA page_size")[0])
                return [
                    ReclaimableSpace(
                        name="main",
                        free_bytes=free_pages * page_size,
                        allocated_bytes=page_count * page_size,
                    )
                ]
            case _:
                raise ValueError("Unsupported SQL type")

    @staticmethod
    def _quote_mariadb_identifier(identifier: str) -> str:
        return f"`{identifier.replace('`', '``')}`"

    def _run_mariadb_table_command(
        self, connector: SQLConnector, *, command: str, table_name: str
    ) -> None:
        # Administrative table statements report failures in their result
        # rows instead of always raising a connector exception.
        query = f"{command} {self._quote_mariadb_identifier(table_name)}"
        rows = connector.fetch_all(query)
        errors = [
            row for row in rows if len(row) >= 3 and str(row[2]).casefold() == "error"
        ]
        if errors:
            raise DatabaseConfigurationError(
                f"{command} failed for table {table_name!r}: {errors!r}."
            )

    def optimize_tables(self, table_names: list[str]) -> None:
        match self.config.database.sql_type.lower():
            case "sqlite":
                if table_names:
                    with self.SQLConnector() as connector:
                        connector.execute("VACUUM")
                return
            case "mariadb":
                with self.SQLConnector() as connector:
                    for table_name in table_names:
                        self._run_mariadb_table_command(
                            connector,
                            command="OPTIMIZE TABLE",
                            table_name=table_name,
                        )
                return
            case _:
                raise ValueError("Unsupported SQL type")

    def optimize_database(self) -> None:
        match self.config.database.sql_type.lower():
            case "sqlite":
                table_names = ["main"]
            case "mariadb":
                table_names = self._get_all_table_names()
            case _:
                raise ValueError("Unsupported SQL type")
        self.optimize_tables(table_names)
        self.logger.info(f"Database optimized: targets={len(table_names)}.")

    def analyze_database(self) -> None:
        match self.config.database.sql_type.lower():
            case "sqlite":
                # Bare ANALYZE (no table name) refreshes statistics for every
                # table in the main database in one pass.
                with self.SQLConnector() as connector:
                    connector.execute("ANALYZE")
                self.logger.info("Database analyzed.")
                return

        table_names = self._get_all_table_names()
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mariadb":
                    for table_name in table_names:
                        self._run_mariadb_table_command(
                            connector,
                            command="ANALYZE TABLE",
                            table_name=table_name,
                        )
        self.logger.info("Database analyzed.")
