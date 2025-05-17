from abc import ABCMeta

from .h2hdb_spec import H2HDBAbstract
from .sql_connector import DatabaseConfigurationError


class H2HDBCheckDatabaseSettings(H2HDBAbstract, metaclass=ABCMeta):
    """
    A class that checks the database settings for character set and collation.

    This class inherits from `H2HDBAbstract` and is used to ensure that the database
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

        Raises:
            DatabaseConfigurationError: If the database character set is invalid.
        """
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
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

        Raises:
            DatabaseConfigurationError: If the database collation is invalid.
        """
        with self.SQLConnector() as connector:
            match self.config.database.sql_type.lower():
                case "mysql":
                    query = "SHOW VARIABLES LIKE 'collation_database';"
                    collation = "utf8mb4_bin"

            collation_result: str = connector.fetch_one(query)[1]
            is_collation_valid: bool = collation_result == collation
            if not is_collation_valid:
                message = f"Invalid database collation. Must be '{collation}' but is '{collation_result}'."
                self.logger.error(message)
                raise DatabaseConfigurationError(message)
            self.logger.info("Database character set and collation are valid.")
