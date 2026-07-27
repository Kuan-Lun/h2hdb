__all__ = [
    "DatabaseConfig",
    "LoggerConfig",
    "H2HConfig",
    "H2HDBConfig",
    "load_config",
    "ensure_download_path_ready",
]

import argparse
import json
import os
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .settings import CBZ_GROUPING, CBZ_SORT, LOG_LEVEL

DEFAULT_FILE_HASH_WORKERS = min(4, os.process_cpu_count() or 1)
MAX_FILE_HASH_WORKERS = 32


class ConfigError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DatabaseConfig(ConfigModel):
    sql_type: str = Field(
        default="mariadb",
        description="Type of SQL database (e.g., mariadb)",
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
    def normalize_level(cls, v: object) -> LOG_LEVEL:
        if isinstance(v, str):
            v_lower = v.lower()
            try:
                return LOG_LEVEL[v_lower]
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
    download_path: Path = Field(
        default=Path("download"),
        description="Path to download files",
    )
    cbz_path: Path | None = Field(
        default=None,
        description="Path to save CBZ files. Unset disables CBZ output.",
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
    file_hash_workers: int = Field(
        default=DEFAULT_FILE_HASH_WORKERS,
        ge=1,
        le=MAX_FILE_HASH_WORKERS,
        description=(
            "Maximum number of files to read and hash concurrently. "
            "Set to 1 for serial hashing."
        ),
    )


class H2HDBConfig(ConfigModel):
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


def ensure_download_path_ready(download_path: Path) -> None:
    try:
        is_empty = not any(download_path.iterdir())
    except OSError as e:
        raise ConfigError(f"Invalid download_path '{download_path}': {e}")
    if is_empty:
        raise ConfigError(
            f"download_path '{download_path}' is empty; check that the path is correct"
        )


def load_config(config_path: str = "") -> H2HDBConfig:
    if config_path:
        with open(config_path) as f:
            raw = json.load(f)
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument("--config")
        args = parser.parse_args()
        if args.config:
            with open(args.config) as f:
                raw = json.load(f)
        else:
            raw = {}

    return H2HDBConfig.model_validate(raw)
