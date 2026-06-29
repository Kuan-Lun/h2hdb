import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from itertools import chain
from typing import Any, cast

from .config_loader import H2HDBConfig
from .logger import HentaiDBLogger, setup_logger
from .settings import FILE_NAME_LENGTH_LIMIT, FOLDER_NAME_LENGTH_LIMIT
from .sql_connector import SQLConnector as AbstractSQLConnector
from .sql_connector import SQLConnectorParams


@dataclass(frozen=True)
class RepositoryContext:
    config: H2HDBConfig
    logger: HentaiDBLogger
    sql_connection_params: SQLConnectorParams
    SQLConnector: Callable[[], AbstractSQLConnector]
    mariadb_index_prefix_limit: int = 191

    @classmethod
    def from_config(cls, config: H2HDBConfig) -> RepositoryContext:
        logger = setup_logger(config.logger)

        sql_connection_params: SQLConnectorParams
        connector_factory: Callable[[], AbstractSQLConnector]

        match config.database.sql_type.lower():
            case "mariadb":
                from .mariadb_connector import MariaDBConnector, MariaDBConnectorParams

                sql_connection_params = MariaDBConnectorParams(
                    host=config.database.host,
                    port=config.database.port,
                    user=config.database.user,
                    password=config.database.password,
                    database=config.database.database,
                )
                connector_factory = cast(
                    Callable[[], AbstractSQLConnector],
                    partial(MariaDBConnector, **sql_connection_params.model_dump()),
                )
            case "sqlite":
                from .sqlite_connector import SQLiteConnector, SQLiteConnectorParams

                sql_connection_params = SQLiteConnectorParams(
                    database=config.database.database,
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
        return self.config.database.sql_type.lower()


class BaseRepository:
    def __init__(self, context: RepositoryContext) -> None:
        self._context = context

    @property
    def config(self) -> H2HDBConfig:
        return self._context.config

    @property
    def logger(self) -> HentaiDBLogger:
        return self._context.logger

    @property
    def sql_connection_params(self) -> SQLConnectorParams:
        return self._context.sql_connection_params

    @property
    def SQLConnector(self) -> Callable[[], AbstractSQLConnector]:
        return self._context.SQLConnector

    @property
    def mariadb_index_prefix_limit(self) -> int:
        return self._context.mariadb_index_prefix_limit

    @property
    def _insert_rows_batch_size(self) -> int:
        return 500

    def _insert_rows(
        self, table_name: str, columns: list[str], rows: list[tuple[Any, ...]]
    ) -> None:
        if not rows:
            return

        row_placeholder = f"({', '.join(['%s'] * len(columns))})"
        batch_size = self._insert_rows_batch_size
        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            insert_query = f"""
                INSERT INTO {table_name} ({", ".join(columns)})
                VALUES {", ".join([row_placeholder] * len(batch))}
            """
            parameters = tuple(chain.from_iterable(batch))
            with self.SQLConnector() as connector:
                connector.execute(insert_query, parameters)

    def _split_gallery_name(self, gallery_name: str) -> list[str]:
        match self.config.database.sql_type.lower():
            case "mariadb":
                return self._mariadb_split_name_value(gallery_name)
            case "sqlite":
                return [gallery_name]
            case _:
                raise ValueError("Unsupported SQL type")

    def _mariadb_split_name_value(self, gallery_name: str) -> list[str]:
        size = FOLDER_NAME_LENGTH_LIMIT // self.mariadb_index_prefix_limit + (
            FOLDER_NAME_LENGTH_LIMIT % self.mariadb_index_prefix_limit > 0
        )
        gallery_name_parts = re.findall(
            f".{{1,{self.mariadb_index_prefix_limit}}}", gallery_name
        )
        gallery_name_parts += [""] * (size - len(gallery_name_parts))
        return gallery_name_parts

    def _mariadb_split_name_based_on_limit(
        self, name: str, name_length_limit: int
    ) -> tuple[list[str], str]:
        num_parts = math.ceil(name_length_limit / self.mariadb_index_prefix_limit)
        name_parts = [
            f"{name}_part{i} CHAR({self.mariadb_index_prefix_limit}) NOT NULL"
            for i in range(1, name_length_limit // self.mariadb_index_prefix_limit + 1)
        ]
        if name_length_limit % self.mariadb_index_prefix_limit > 0:
            name_parts.append(
                f"{name}_part{num_parts} CHAR({name_length_limit % self.mariadb_index_prefix_limit}) NOT NULL"
            )
        column_name_parts = [f"{name}_part{i}" for i in range(1, num_parts + 1)]
        create_name_parts_sql = ", ".join(name_parts)
        return column_name_parts, create_name_parts_sql

    def mariadb_split_gallery_name_based_on_limit(
        self, name: str
    ) -> tuple[list[str], str]:
        return self._mariadb_split_name_based_on_limit(name, FOLDER_NAME_LENGTH_LIMIT)

    def mariadb_split_file_name_based_on_limit(
        self, name: str
    ) -> tuple[list[str], str]:
        return self._mariadb_split_name_based_on_limit(name, FILE_NAME_LENGTH_LIMIT)

    def sqlite_name_columns(self, name: str) -> tuple[list[str], str]:
        return [name], f"{name} TEXT NOT NULL"

    def _create_sqlite_fts5_sync(
        self,
        connector: AbstractSQLConnector,
        table_name: str,
        column_name: str,
        rowid_column: str,
    ) -> None:
        fts_table_name = f"{table_name}_fts"
        connector.execute(f"""
            CREATE VIRTUAL TABLE IF NOT EXISTS {fts_table_name} USING fts5(
                {column_name}, content='{table_name}', content_rowid='{rowid_column}'
            )
            """)
        connector.execute(f"""
            CREATE TRIGGER IF NOT EXISTS {table_name}_ai AFTER INSERT ON {table_name} BEGIN
                INSERT INTO {fts_table_name}(rowid, {column_name})
                VALUES (new.{rowid_column}, new.{column_name});
            END
            """)
        connector.execute(f"""
            CREATE TRIGGER IF NOT EXISTS {table_name}_ad AFTER DELETE ON {table_name} BEGIN
                INSERT INTO {fts_table_name}({fts_table_name}, rowid, {column_name})
                VALUES ('delete', old.{rowid_column}, old.{column_name});
            END
            """)
        connector.execute(f"""
            CREATE TRIGGER IF NOT EXISTS {table_name}_au AFTER UPDATE ON {table_name} BEGIN
                INSERT INTO {fts_table_name}({fts_table_name}, rowid, {column_name})
                VALUES ('delete', old.{rowid_column}, old.{column_name});
                INSERT INTO {fts_table_name}(rowid, {column_name})
                VALUES (new.{rowid_column}, new.{column_name});
            END
            """)
