"""Connector-factory context shared by greenfield application facades."""

from __future__ import annotations

__all__ = ["RepositoryContext"]

from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from os import getpid
from threading import Lock
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
    _close_connections: Callable[[], None] | None = None

    def close(self) -> None:
        if self._close_connections is not None:
            self._close_connections()

    @classmethod
    def from_config(cls, config: CoreConfig) -> RepositoryContext:
        logger = setup_logger(config.logger)

        sql_connection_params: SQLConnectorParams
        connector_factory: Callable[[], AbstractSQLConnector]

        close_connections: Callable[[], None] | None = None

        match config.database.sql_type:
            case "mariadb":
                from mysql.connector import connect
                from mysql.connector.abstracts import MySQLConnectionAbstract

                from .mariadb_connector import MariaDBConnector, MariaDBConnectorParams
                from .mariadb_pool import MariaDBConnectionPool

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
                physical_params = sql_connection_params.model_dump(
                    exclude={"read_only"}
                )
                pool = MariaDBConnectionPool(
                    lambda: cast(MySQLConnectionAbstract, connect(**physical_params))
                )
                close_connections = pool.close
                connector_factory = cast(
                    Callable[[], AbstractSQLConnector],
                    partial(
                        MariaDBConnector,
                        _pool=pool,
                        **sql_connection_params.model_dump(),
                    ),
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

        lifecycle_lock = Lock()
        owner_pid = getpid()
        closed = False

        def require_process() -> None:
            if getpid() != owner_pid:
                raise RuntimeError("Database runtime cannot be used after fork")

        def create_connector() -> AbstractSQLConnector:
            require_process()
            with lifecycle_lock:
                if closed:
                    raise RuntimeError("Database runtime is closed")
                return connector_factory()

        def close_context() -> None:
            nonlocal closed
            require_process()
            with lifecycle_lock:
                if closed:
                    return
                closed = True
            if close_connections is not None:
                close_connections()

        return cls(
            config=config,
            logger=logger,
            sql_connection_params=sql_connection_params,
            SQLConnector=create_connector,
            _close_connections=close_context,
        )

    @property
    def sql_type(self) -> str:
        return self.config.database.sql_type
