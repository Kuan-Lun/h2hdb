__all__ = [
    "ConfigError",
    "CoreConfig",
    "DatabaseAccessMode",
    "DatabaseConfig",
    "DatabaseMaintenanceConfig",
    "LoggerConfig",
    "load_config",
]

import json
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .environment import resolve_environment_placeholders
from .settings import LOG_LEVEL

DEFAULT_MAINTENANCE_INTERVAL_SECONDS = 7 * 24 * 60 * 60
DEFAULT_MAINTENANCE_WORK_UNITS = 1000
DEFAULT_MIN_DATA_FREE_BYTES = 256 * 1024 * 1024
DEFAULT_MIN_DATA_FREE_RATIO = 0.20
DEFAULT_DATABASE_GATE_TIMEOUT_SECONDS = 300


class ConfigError(ValueError):
    pass


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DatabaseAccessMode(StrEnum):
    read_write = "read-write"
    read_only = "read-only"


class DatabaseConfig(ConfigModel):
    sql_type: str = Field(default="mariadb", description="mariadb or sqlite")
    host: str = Field(default="localhost", min_length=1)
    port: int = Field(default=3306, ge=1, le=65535)
    user: str = Field(default="root", min_length=1)
    database: str = Field(default="h2h", min_length=1)
    password: str = Field(default="password")
    access_mode: DatabaseAccessMode = DatabaseAccessMode.read_write

    @field_validator("sql_type")
    @classmethod
    def validate_sql_type(cls, value: str) -> str:
        normalized = value.casefold()
        if normalized not in {"mariadb", "sqlite"}:
            raise ValueError("sql_type must be either 'mariadb' or 'sqlite'")
        return normalized


class DatabaseMaintenanceConfig(ConfigModel):
    optimize_enabled: bool = True
    min_interval_seconds: int = Field(
        default=DEFAULT_MAINTENANCE_INTERVAL_SECONDS,
        ge=0,
    )
    min_work_units: int = Field(default=DEFAULT_MAINTENANCE_WORK_UNITS, ge=1)
    min_data_free_bytes: int = Field(default=DEFAULT_MIN_DATA_FREE_BYTES, ge=0)
    min_data_free_ratio: float = Field(
        default=DEFAULT_MIN_DATA_FREE_RATIO,
        ge=0,
        le=1,
    )
    lock_wait_seconds: int = Field(
        default=DEFAULT_DATABASE_GATE_TIMEOUT_SECONDS,
        ge=1,
    )


class LoggerConfig(ConfigModel):
    level: LOG_LEVEL = LOG_LEVEL.info
    file: Path | None = None

    @field_validator("level", mode="before")
    @classmethod
    def normalize_level(cls, value: object) -> LOG_LEVEL:
        if isinstance(value, LOG_LEVEL):
            return value
        if isinstance(value, str):
            try:
                return LOG_LEVEL[value.casefold()]
            except KeyError as error:
                raise ValueError(f"Unsupported log level: {value}") from error
        if isinstance(value, int):
            try:
                return LOG_LEVEL(value)
            except ValueError as error:
                raise ValueError(f"Unsupported log level value: {value}") from error
        raise TypeError(f"Unsupported log level type: {type(value).__name__}")


class CoreConfig(ConfigModel):
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    maintenance: DatabaseMaintenanceConfig = Field(
        default_factory=DatabaseMaintenanceConfig
    )
    logger: LoggerConfig = Field(default_factory=LoggerConfig)


def load_config(config_path: str | Path) -> CoreConfig:
    path = Path(config_path)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ConfigError(f"Unable to load core config from {path}: {error}") from error
    return CoreConfig.model_validate(resolve_environment_placeholders(raw))
