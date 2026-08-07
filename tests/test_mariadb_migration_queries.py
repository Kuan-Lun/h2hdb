from collections.abc import Callable
from dataclasses import replace
from typing import Any, Self, cast

from h2hdb import CoreConfig, DatabaseConfig
from h2hdb.migrations import MigrationRunner
from h2hdb.repository import RepositoryContext
from h2hdb.sql_connector import SQLConnector


class _ForeignKeyMetadataConnector:
    def __init__(self) -> None:
        self.query: str | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object | None,
    ) -> None:
        return None

    def fetch_all(
        self,
        query: str,
        data: tuple[Any, ...] = (),
    ) -> list[tuple[Any, ...]]:
        self.query = query
        assert data == ("h2hdb", "children")
        return [
            (
                "children_parent_id_fk",
                "parent_id",
                "parents",
                "id",
                1,
                "RESTRICT",
                "CASCADE",
            )
        ]


def test_mariadb_foreign_key_query_avoids_reserved_usage_alias() -> None:
    connector = _ForeignKeyMetadataConnector()
    context = replace(
        RepositoryContext.from_config(
            CoreConfig(
                database=DatabaseConfig(
                    sql_type="mariadb",
                    database="h2hdb",
                )
            )
        ),
        SQLConnector=cast(Callable[[], SQLConnector], lambda: connector),
    )

    foreign_keys = MigrationRunner(context)._foreign_keys("children")

    assert connector.query is not None
    assert " AS usage" not in connector.query
    assert " AS kcu" in connector.query
    assert foreign_keys == {
        (("parent_id",), "parents", ("id",), "NO ACTION", "CASCADE")
    }
