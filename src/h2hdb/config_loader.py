__all__ = ["config_loader"]


import argparse
import json


DEFAULT_CONFIG = dict[str, dict](
    h2h=dict[str, str](download_path="data"),
    database=dict[str, str](
        sql_type="mysql",
        host="localhost",
        port="3306",
        user="root",
        database="h2h",
        password="password",
    ),
    logger=dict[str, str | bool | int](
        level="INFO",
        display_on_screen=True,
        max_log_entry_length=-1,
        write_to_file="",
    ),
)


class ConfigError(Exception):
    """
    Exception raised for errors in the configuration.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class DatabaseConfig:
    __slots__ = ["sql_type", "host", "port", "user", "database", "password"]

    def __init__(
        self,
        sql_type: str,
        host: str,
        port: str,
        user: str,
        database: str,
        password: str,
    ) -> None:
        self.sql_type = sql_type
        self.host = host
        self.port = port
        self.user = user
        self.database = database
        self.password = password

        if sql_type not in ["mysql"]:
            raise ConfigError("Invalid SQL type")

        if type(host) is not str:
            raise ConfigError("host must be a string")

        if type(port) is not str:
            raise ConfigError("port must be a string")

        if type(user) is not str:
            raise ConfigError("user must be a string")

        if type(database) is not str:
            raise ConfigError("database must be a string")

        if type(password) is not str:
            raise ConfigError("password must be a string")

    def __repr__(self) -> str:
        return f"DatabaseConfig(sql_type={self.sql_type}, host={self.host}, port={self.port}, user={self.user}, database={self.database}, password={self.password})"

    def __str__(self) -> str:
        return self.__repr__()


class LoggerConfig:
    __slots__ = ["level", "display_on_screen", "write_to_file", "max_log_entry_length"]

    def __init__(
        self,
        level: str,
        display_on_screen: bool,
        write_to_file: str,
        max_log_entry_length: int,
    ) -> None:
        self.level = level
        self.display_on_screen = display_on_screen
        self.write_to_file = write_to_file
        self.max_log_entry_length = max_log_entry_length

        match level.lower():
            case "notset" | "debug" | "info" | "warning" | "error" | "critical":
                pass
            case _:
                raise ConfigError(
                    f"Invalid log level {level} (must be one of NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL)"
                )

        if type(display_on_screen) is not bool:
            raise ConfigError(
                f"Incorrect type for display_on_screen: {type(display_on_screen)}"
            )

        if type(write_to_file) is not str:
            raise ConfigError(f"Incorrect type for write_to_file: {type(write_to_file)}")

        if type(max_log_entry_length) is not int:
            raise ConfigError(
                f"Incorrect type for max_log_entry_length: {type(max_log_entry_length)}"
            )

    def __repr__(self) -> str:
        return f"LoggerConfig(level={self.level}, display_on_screen={self.display_on_screen}, write_to_file={self.write_to_file}, max_log_entry_length={self.max_log_entry_length})"

    def __str__(self) -> str:
        return self.__repr__()


class H2HConfig:
    __slots__ = ["download_path"]

    def __init__(self, download_path: str) -> None:
        self.download_path = download_path

        if type(download_path) is not str:
            raise ConfigError("download_path must be a string")

    def __repr__(self) -> str:
        return f"H2HConfig(download_path={self.download_path})"

    def __str__(self) -> str:
        return self.__repr__()


class Config:
    __slots__ = ["h2h", "database", "logger"]

    def __init__(
        self,
        h2h_config: H2HConfig,
        database_config: DatabaseConfig,
        logger_config: LoggerConfig,
    ) -> None:
        self.h2h = h2h_config
        self.database = database_config
        self.logger = logger_config

    def __repr__(self) -> str:
        return f"Config(h2h={self.h2h}, database_config={self.database}, logger_config={self.logger})"

    def __str__(self) -> str:
        return self.__repr__()


def load_config() -> Config:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config")
    args = parser.parse_args()

    if args.config is None:
        user_config = DEFAULT_CONFIG
    else:
        with open(args.config, "r") as f:
            user_config = json.load(f)

    # Validate the h2h configuration
    download_path = user_config["h2h"]["download_path"]
    user_config["h2h"].pop("download_path")
    h2h_config = H2HConfig(download_path)
    if len(user_config["h2h"]) > 0:
        raise ConfigError("Invalid configuration for h2h")
    else:
        user_config.pop("h2h")

    # Validate the database configuration
    database_config = DatabaseConfig(
        sql_type=user_config["database"]["sql_type"],
        host=user_config["database"]["host"],
        port=user_config["database"]["port"],
        user=user_config["database"]["user"],
        database=user_config["database"]["database"],
        password=user_config["database"]["password"],
    )
    user_config["database"].pop("sql_type")
    user_config["database"].pop("host")
    user_config["database"].pop("port")
    user_config["database"].pop("user")
    user_config["database"].pop("database")
    user_config["database"].pop("password")
    if len(user_config["database"]) > 0:
        raise ConfigError("Invalid configuration for database")
    else:
        user_config.pop("database")

    # Validate the logger configuration
    level = user_config["logger"]["level"]
    user_config["logger"].pop("level")

    display_on_screen = user_config["logger"]["display_on_screen"]
    user_config["logger"].pop("display_on_screen")

    max_log_entry_length = user_config["logger"]["max_log_entry_length"]
    user_config["logger"].pop("max_log_entry_length")

    write_to_file = user_config["logger"]["write_to_file"]
    user_config["logger"].pop("write_to_file")

    if len(user_config["logger"]) > 0:
        raise ConfigError("Invalid configuration for logger")
    else:
        user_config.pop("logger")

    if len(user_config) > 0:
        raise ConfigError("Invalid configuration for the entire config")

    logger_config = LoggerConfig(
        level=level,
        display_on_screen=display_on_screen,
        write_to_file=write_to_file,
        max_log_entry_length=max_log_entry_length,
    )

    return Config(h2h_config, database_config, logger_config)


config_loader = load_config()
