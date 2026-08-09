from collections.abc import Callable
from dataclasses import replace
from typing import Any, Self, cast

from h2hdb import CoreConfig, DatabaseConfig
from h2hdb.migrations import MigrationRunner
from h2hdb.repository import RepositoryContext
from h2hdb.sql_connector import SQLConnector
from h2hdb.todelete_queue import _todelete_rm_commands_query


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


def test_mariadb_projection_migration_has_bounded_cleanup_and_keyset_indexes() -> None:
    context = RepositoryContext.from_config(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="mariadb",
                database="h2hdb",
            )
        )
    )
    sql = "\n".join(MigrationRunner(context)._catalog_projection_build_statements())

    assert "ON DELETE CASCADE" not in sql
    assert "ON DELETE RESTRICT" in sql
    assert "revision, content_sha256, source_gallery_name(191)" in sql
    assert "build_id, content_sha256, gallery_key" in sql
    assert "build_id, gid, gallery_key" in sql
    assert "catalog_projection_receipt_source_revision_fk" in sql
    assert "catalog_projection_receipt_catalog_revision_fk" in sql


def test_mariadb_operational_migration_is_build_detached_and_keyset_indexed() -> None:
    context = RepositoryContext.from_config(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="mariadb",
                database="h2hdb",
            )
        )
    )
    sql = "\n".join(MigrationRunner(context)._catalog_operational_statements())

    assert "FOREIGN KEY" not in sql
    assert "NORMALIZING_TIMES" in sql
    assert "gid, build_id, preparation_id" in sql
    assert "gid, deletion_request_token, build_id, preparation_id" in sql
    assert "build_id, gid, download_time_utc, gallery_key" in sql
    assert "gallery_name(191)" in sql


def test_mariadb_deletion_view_has_exact_gate_and_shell_quoting() -> None:
    sql = _todelete_rm_commands_query(
        "mariadb",
        active_authority=True,
        if_not_exists=False,
    )

    assert "CREATE VIEW todelete_rm_commands" in sql
    assert "CONCAT(" in sql
    assert "REPLACE(deletion_path.source_locator, '''', '''\\\\''''')" in sql
    assert "activation.build_id = source_revision.active_build_id" in sql
    assert "activation.source_revision = source_revision.current_revision" in sql
    assert "consumption.deletion_request_token" in sql
    assert "marker.request_token" in sql
    assert "newer.download_time_utc > source.download_time_utc" in sql
    assert "digest.duplicate_hash_deletion_candidate = 1" in sql
    assert "WHERE NOT EXISTS" in sql
    assert "fallback_activation.source_revision" in sql
    assert "fallback_source_revision.current_revision" in sql
