"""Connector-factory context shared by greenfield application facades."""

from __future__ import annotations

__all__ = ["RepositoryContext"]

from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import cast

from .config_loader import CoreConfig, DatabaseAccessMode
from .logger import HentaiDBLogger, setup_logger
from .sql_connector import SQLConnector as AbstractSQLConnector
from .sql_connector import SQLConnectorParams


@dataclass(frozen=True)
class RepositoryContext:
    config: CoreConfig
    logger: HentaiDBLogger
    sql_connection_params: SQLConnectorParams
    SQLConnector: Callable[[], AbstractSQLConnector]

    @classmethod
    def from_config(cls, config: CoreConfig) -> RepositoryContext:
        logger = setup_logger(config.logger)

        sql_connection_params: SQLConnectorParams
        connector_factory: Callable[[], AbstractSQLConnector]

        match config.database.sql_type:
            case "mariadb":
                from .mariadb_connector import MariaDBConnector, MariaDBConnectorParams

                sql_connection_params = MariaDBConnectorParams(
                    host=config.database.host,
                    port=config.database.port,
                    user=config.database.user,
                    password=config.database.password,
                    database=config.database.database,
                    read_only=(
                        config.database.access_mode is DatabaseAccessMode.read_only
                    ),
                )
                connector_factory = cast(
                    Callable[[], AbstractSQLConnector],
                    partial(MariaDBConnector, **sql_connection_params.model_dump()),
                )
            case "sqlite":
                from .sqlite_connector import SQLiteConnector, SQLiteConnectorParams

                sql_connection_params = SQLiteConnectorParams(
                    database=config.database.database,
                    read_only=(
                        config.database.access_mode is DatabaseAccessMode.read_only
                    ),
                )
                connector_factory = cast(
                    Callable[[], AbstractSQLConnector],
                    partial(SQLiteConnector, **sql_connection_params.model_dump()),
                )
            case _:
                raise ValueError("Unsupported SQL type")

        return cls(
            config=config,
            logger=logger,
            sql_connection_params=sql_connection_params,
            SQLConnector=connector_factory,
        )

    @property
    def sql_type(self) -> str:
        return self.config.database.sql_type
