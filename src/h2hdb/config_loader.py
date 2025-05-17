__all__ = ["DatabaseConfig", "LoggerConfig", "H2HConfig", "H2HDBConfig", "load_config"]

import argparse
import json
import os

from pydantic import BaseModel, Field, ConfigDict, field_validator

from .settings import LOG_LEVEL, CBZ_GROUPING, CBZ_SORT


class ConfigError(Exception):
    """
    Exception raised for errors in the configuration.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class ConfigModel(BaseModel):
    """
    Base class for configuration models.

    This class inherits from `pydantic.BaseModel` and is used to define the configuration
    structure for the application. It provides a way to validate and parse configuration data.
    """

    model_config = ConfigDict(extra="forbid")

    def __init__(self, **data) -> None:
        super().__init__(**data)


class DatabaseConfig(ConfigModel):
    sql_type: str = Field(
        default="mysql",
        description="Type of SQL database (e.g., mysql)",
    )
    host: str = Field(
        default="localhost",
        min_length=1,
        description="Host of the SQL database",
    )
    port: int = Field(
        default=3306,
        ge=1,
        le=65535,
        description="Port of the SQL database",
    )
    user: str = Field(
        default="root",
        min_length=1,
        description="User for the SQL database",
    )
    database: str = Field(
        default="h2h",
        min_length=1,
        description="Database name for the SQL database",
    )
    password: str = Field(
        default="password",
        description="Password for the SQL database",
    )


class LoggerConfig(BaseModel):
    level: LOG_LEVEL = Field(
        default=LOG_LEVEL.info,
        description="Log level (case-insensitive): NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL",
    )

    @field_validator("level", mode="before")
    @classmethod
    def normalize_level(cls, v) -> LOG_LEVEL:
        if isinstance(v, str):
            v_lower = v.lower()
            try:
                return LOG_LEVEL[v_lower]  # Enum lookup by name
            except KeyError:
                raise ValueError(
                    f"Invalid log level '{v}'. Must be one of: "
                    + ", ".join(name.upper() for name in LOG_LEVEL.__members__)
                )
        elif isinstance(v, int):
            try:
                return LOG_LEVEL(v)
            except ValueError:
                raise ValueError(f"Invalid log level value: {v}")
        elif isinstance(v, LOG_LEVEL):
            return v
        else:
            raise TypeError(f"Invalid type for log level: {type(v)}")


class H2HConfig(ConfigModel):
    download_path: str = Field(
        default="download",
        min_length=1,
        description="Path to download files",
    )
    cbz_path: str = Field(
        default="",
        min_length=0,
        description="Path to save CBZ files",
    )
    cbz_max_size: int = Field(
        default=768,
        ge=1,
        description="Maximum width or height (in pixels) allowed for each image in the CBZ file",
    )
    cbz_grouping: CBZ_GROUPING = Field(
        default=CBZ_GROUPING.flat,
        description="Grouping method for CBZ files: flat, date-yyyy, date-yyyy-mm, or date-yyyy-mm-dd",
    )
    cbz_sort: CBZ_SORT = Field(
        default=CBZ_SORT.no,
        description="Sorting method for CBZ files: no, upload_time, download_time, pages, or pages+[num]",
    )

    @property
    def cbz_tmp_directory(self) -> str:
        return os.path.join(self.cbz_path, "tmp")


class H2HDBConfig(ConfigModel):
    """
    Configuration class for H2HDB.

    This class combines the configurations for H2H, database, and logger into a single
    configuration object. It validates the types of each configuration component.
    """

    h2h: H2HConfig = Field(
        default_factory=H2HConfig,
        description="Configuration for H2H",
    )
    database: DatabaseConfig = Field(
        default_factory=DatabaseConfig,
        description="Configuration for the database",
    )
    logger: LoggerConfig = Field(
        default_factory=LoggerConfig,
        description="Configuration for the logger",
    )


def load_config(config_path: str = "") -> H2HDBConfig:
    if config_path:
        with open(config_path, "r") as f:
            raw = json.load(f)
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument("--config")
        args = parser.parse_args()
        if args.config:
            with open(args.config, "r") as f:
                raw = json.load(f)
        else:
            raw = {}  # ← 重點：傳空 config，讓 default 自動補

    return H2HDBConfig.model_validate(raw)
