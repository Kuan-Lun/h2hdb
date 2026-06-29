from .repository import BaseRepository
from .sql_connector import DatabaseConfigurationError


class H2HDBCheckDatabaseSettings(BaseRepository):
    """
    A class that checks the database settings for character set and collation.

    This repository is used to ensure that the database
    character set and collation are valid. It provides methods to check the character set and
    collation of the database and raises an error if they are invalid.

    Attributes:
        sql_type (str): The type of SQL database being used.

    Methods:
        check_database_character_set: Checks the character set of the database.
        check_database_collation: Checks the collation of the database.
    """

    def check_database_character_set(self) -> None:
        """
        Checks the character set of the database and raises an error if it is invalid.

        SQLite has no database-level character set setting (TEXT is always UTF-8), so this
        is a no-op for the "sqlite" backend.

        Raises:
            DatabaseConfigurationError: If the database character set is invalid.
        """
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
            self.logger.info("Database character set is valid.")

    def check_database_collation(self) -> None:
        """
        Checks the collation of the database and raises an error if it is invalid.

        SQLite has no database-level collation setting (collation is a per-column/per-expression
        concept in SQLite), so this is a no-op for the "sqlite" backend.

        Raises:
            DatabaseConfigurationError: If the database collation is invalid.
        """
        match self.config.database.sql_type.lower():
            case "sqlite":
                return

        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mariadb":
                    query = "SHOW VARIABLES LIKE 'collation_database';"
                    collation = "utf8mb4_bin"

            collation_result: str = connector.fetch_one(query)[1]
            is_collation_valid: bool = collation_result == collation
            if not is_collation_valid:
                message = f"Invalid database collation. Must be '{collation}' but is '{collation_result}'."
                self.logger.error(message)
                raise DatabaseConfigurationError(message)
            self.logger.info("Database character set and collation are valid.")

    def optimize_database(self) -> None:
        match self.config.database.sql_type.lower():
            case "sqlite":
                # SQLite has no per-table OPTIMIZE TABLE; VACUUM rebuilds and
                # defragments the whole database file instead.
                with self.SQLConnector() as connector:
                    connector.execute("VACUUM")
                self.logger.info("Database optimized.")
                return

        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mariadb":
                    select_table_name_query = f"""
                        SELECT TABLE_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                        WHERE REFERENCED_TABLE_SCHEMA = '{self.config.database.database}'
                    """
            raw_table_names = connector.fetch_all(select_table_name_query)
        table_names = [str(t[0]) for t in raw_table_names]

        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mariadb":

                    def get_optimize_query(x: str) -> str:
                        return f"OPTIMIZE TABLE {x}"

            for table_name in table_names:
                connector.execute(get_optimize_query(table_name))
        self.logger.info("Database optimized.")
