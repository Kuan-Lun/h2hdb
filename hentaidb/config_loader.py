__all__ = ["config_loader"]


import argparse
import json
from typing import TypeVar, Union, Generic


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
        write_to_file: bool,
        max_log_entry_length: int,
    ) -> None:
        self.level = level
        self.display_on_screen = display_on_screen
        self.write_to_file = write_to_file
        self.max_log_entry_length = max_log_entry_length

    def __repr__(self) -> str:
        return f"LoggerConfig(level={self.level}, display_on_screen={self.display_on_screen}, write_to_file={self.write_to_file}, max_log_entry_length={self.max_log_entry_length})"

    def __str__(self) -> str:
        return self.__repr__()


class Config:
    def __init__(
        self, database_config: DatabaseConfig, logger_config: LoggerConfig
    ) -> None:
        self.database = database_config
        self.logger = logger_config

    def __repr__(self) -> str:
        return f"Config(database_config={self.database}, logger_config={self.logger})"

    def __str__(self) -> str:
        return self.__repr__()


def load_config() -> Config:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config")
    args = parser.parse_args()

    with open(args.config, "r") as f:
        user_config = json.load(f)

    database_config = DatabaseConfig(
        sql_type=user_config["database"]["sql_type"],
        host=user_config["database"]["host"],
        port=user_config["database"]["port"],
        user=user_config["database"]["user"],
        database=user_config["database"]["database"],
        password=user_config["database"]["password"],
    )

    if "logger" not in user_config:
        level = "INFO"
        display_on_screen = False
        write_to_file = False
        max_log_entry_length = -1
    else:
        if "level" in user_config["logger"]:
            level = user_config["logger"]["level"]
        else:
            level = "INFO"
        if "display_on_screen" in user_config["logger"]:
            display_on_screen = user_config["logger"]["display_on_screen"]
            if "max_log_entry_length" in user_config["logger"]:
                max_log_entry_length = user_config["logger"]["max_log_entry_length"]
            else:
                max_log_entry_length = -1
        else:
            display_on_screen = False
            max_log_entry_length = -1
        if "write_to_file" in user_config["logger"]:
            write_to_file = user_config["logger"]["write_to_file"]
        else:
            write_to_file = False

    logger_config = LoggerConfig(
        level=level,
        display_on_screen=display_on_screen,
        write_to_file=write_to_file,
        max_log_entry_length=max_log_entry_length,
    )

    return Config(database_config, logger_config)


config_loader = load_config()
