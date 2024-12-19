__all__ = ["DatabaseConfig", "LoggerConfig", "H2HConfig", "H2HDBConfig", "load_config"]

import os
import argparse
import json


class ConfigError(Exception):
    """
    Exception raised for errors in the configuration.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message) -> None:
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

        if not isinstance(host, str):
            raise TypeError("host must be a string")

        if not isinstance(port, str):
            raise TypeError("port must be a string")

        if not isinstance(user, str):
            raise TypeError("user must be a string")

        if not isinstance(database, str):
            raise TypeError("database must be a string")

        if not isinstance(password, str):
            raise TypeError("password must be a string")


class LoggerConfig:
    __slots__ = [
        "level",
        "max_log_entry_length",
    ]

    def __init__(
        self,
        level: str,
    ) -> None:
        self.level = level

        match level.lower():
            case "notset" | "debug" | "info" | "warning" | "error" | "critical":
                pass
            case _:
                raise ConfigError(
                    f"Invalid log level {level} (must be one of NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL)"
                )


class H2HConfig:
    __slots__ = [
        "download_path",
        "cbz_path",
        "cbz_max_size",
        "cbz_grouping",
        "cbz_tmp_directory",
        "cbz_sort",
    ]

    def __init__(
        self,
        download_path: str,
        cbz_path: str,
        cbz_max_size: int,
        cbz_grouping: str,
        cbz_sort: str,
    ) -> None:
        self.download_path = download_path
        self.cbz_path = cbz_path
        self.cbz_max_size = cbz_max_size
        self.cbz_grouping = cbz_grouping
        self.cbz_sort = cbz_sort

        if not isinstance(download_path, str):
            raise TypeError("download_path must be a string")

        if not isinstance(cbz_path, str):
            raise TypeError("cbz_path must be a string")

        if not isinstance(cbz_max_size, int):
            raise TypeError("cbz_max_size must be an integer")

        if not isinstance(cbz_grouping, str):
            raise TypeError("cbz_grouping must be a string")

        if not isinstance(cbz_sort, str):
            raise TypeError("cbz_sort must be a string")

        self.cbz_tmp_directory = os.path.join(self.cbz_path, "tmp")


class H2HDBConfig:
    __slots__ = ["h2h", "database", "logger"]

    def __init__(
        self,
        h2h_config: H2HConfig,
        database_config: DatabaseConfig,
        logger_config: LoggerConfig,
    ) -> None:

        if not isinstance(h2h_config, H2HConfig):
            raise TypeError(f"Incorrect type for h2h_config: {type(h2h_config)}")

        if not isinstance(database_config, DatabaseConfig):
            raise TypeError(
                f"Incorrect type for database_config: {type(database_config)}"
            )

        if not isinstance(logger_config, LoggerConfig):
            raise TypeError(f"Incorrect type for logger_config: {type(logger_config)}")

        self.h2h = h2h_config
        self.database = database_config
        self.logger = logger_config


def set_default_config() -> dict[str, dict]:
    return dict[str, dict](
        h2h=dict[str, str | int](
            download_path="download",
            cbz_path="",
            cbz_max_size=768,
            cbz_grouping="flat",
            cbz_sort="no",
        ),
        database=dict[str, str](
            sql_type="mysql",
            host="localhost",
            port="3306",
            user="root",
            database="h2h",
            password="password",
        ),
        logger=dict[str, str](
            level="INFO",
        ),
    )


def load_config(config_path: str = "") -> H2HDBConfig:
    default_config = set_default_config()

    if config_path != "":
        with open(config_path, "r") as f:
            user_config = json.load(f)
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument("--config")
        args = parser.parse_args()

        if args.config is None:
            user_config = default_config
        else:
            with open(args.config, "r") as f:
                user_config = json.load(f)

    # Validate the h2h configuration
    download_path = user_config["h2h"]["download_path"]
    user_config["h2h"].pop("download_path")
    cbz_path = user_config["h2h"]["cbz_path"]
    user_config["h2h"].pop("cbz_path")
    cbz_max_size = user_config["h2h"]["cbz_max_size"]
    user_config["h2h"].pop("cbz_max_size")
    cbz_grouping = user_config["h2h"]["cbz_grouping"]
    user_config["h2h"].pop("cbz_grouping")
    if "cbz_sort" in user_config["h2h"]:
        cbz_sort = user_config["h2h"]["cbz_sort"]
        user_config["h2h"].pop("cbz_sort")
    else:
        cbz_sort = default_config["h2h"]["cbz_sort"]
    h2h_config = H2HConfig(
        download_path, cbz_path, cbz_max_size, cbz_grouping, cbz_sort
    )
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
    if "logger" not in user_config or user_config["logger"] == {}:
        user_config["logger"] = default_config["logger"]
    level = user_config["logger"]["level"]
    user_config["logger"].pop("level")

    if len(user_config["logger"]) > 0:
        raise ConfigError("Invalid configuration for logger")
    else:
        user_config.pop("logger")

    logger_config = LoggerConfig(level=level)

    if len(user_config) > 0:
        raise ConfigError("Invalid configuration for the entire config")

    return H2HDBConfig(
        h2h_config=h2h_config,
        database_config=database_config,
        logger_config=logger_config,
    )
