from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

import pytest

from h2hdb import (
    CatalogRevisionNotFoundError,
    CoreConfig,
    DatabaseAccessMode,
    VNextDatabaseAdminFacade,
    open_database,
)
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.repository import RepositoryContext
from h2hdb.sql_connector import DatabaseDuplicateKeyError, SQLConnector

_REMOVED_GALLERY_IDENTITY_TABLES = (
    "catalog_gallery_identity_anchors",
    "catalog_gallery_identity_coordinates",
    "catalog_gallery_identity_gallery_keys",
    "catalog_gallery_identity_seals",
)


def _read_only(config: CoreConfig) -> CoreConfig:
    return config.model_copy(
        update={
            "database": config.database.model_copy(
                update={"access_mode": DatabaseAccessMode.read_only}
            )
        }
    )


def _assert_gallery_identity_schema(connector: SQLConnector, backend: str) -> None:
    assert connector.check_table_exists("catalog_gallery_identities")
    assert all(
        not connector.check_table_exists(table)
        for table in _REMOVED_GALLERY_IDENTITY_TABLES
    )

    if backend == "mariadb":
        assert connector.fetch_one("""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_TYPE = 'BASE TABLE'
            """) == (381,)
        mariadb_foreign_keys = connector.fetch_all(
            """
            SELECT CONSTRAINT_NAME, COLUMN_NAME,
                   REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND REFERENCED_TABLE_NAME IS NOT NULL
            ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION
            """,
            ("catalog_gallery_identities",),
        )
        assert mariadb_foreign_keys == [
            (
                "fk_gallery_identity_1",
                "scope_key",
                "catalog_source_scope_seals",
                "scope_key",
            ),
            (
                "fk_gallery_identity_2",
                "locator_sha256",
                "catalog_source_locator_identity",
                "locator_sha256",
            ),
        ]
        raw_indexes = connector.fetch_all(
            """
            SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
            """,
            ("catalog_gallery_identities",),
        )
        mariadb_indexes: dict[str, tuple[int, list[str]]] = {}
        for name, non_unique, _position, column in raw_indexes:
            key = str(name)
            if key not in mariadb_indexes:
                mariadb_indexes[key] = (int(non_unique), [])
            mariadb_indexes[key][1].append(str(column))
        assert (0, ["gallery_key"]) in mariadb_indexes.values()
        assert (0, ["scope_key", "locator_sha256"]) in mariadb_indexes.values()
        assert (1, ["locator_sha256"]) in mariadb_indexes.values()
        return

    assert connector.fetch_one("""
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
    """) == (381,)
    sqlite_foreign_keys = {
        (str(row[2]), str(row[3]), str(row[4]))
        for row in connector.fetch_all(
            'PRAGMA foreign_key_list("catalog_gallery_identities")'
        )
    }
    assert sqlite_foreign_keys == {
        ("catalog_source_scope_seals", "scope_key", "scope_key"),
        (
            "catalog_source_locator_identity",
            "locator_sha256",
            "locator_sha256",
        ),
    }
    sqlite_indexes: list[tuple[int, tuple[str, ...]]] = []
    for row in connector.fetch_all('PRAGMA index_list("catalog_gallery_identities")'):
        name = str(row[1])
        columns = tuple(
            str(column[2])
            for column in connector.fetch_all(f'PRAGMA index_info("{name}")')
        )
        sqlite_indexes.append((int(row[2]), columns))
    assert (1, ("gallery_key",)) in sqlite_indexes
    assert (1, ("scope_key", "locator_sha256")) in sqlite_indexes
    assert (0, ("locator_sha256",)) in sqlite_indexes


def _exercise_generated_epoch(config: CoreConfig) -> None:
    admin = VNextDatabaseAdminFacade(config)

    initialized = admin.initialize()
    assert initialized.epoch == ARTIFACT["epoch"] == 2
    assert initialized.schema_version == ARTIFACT["schema_version"] == 1
    assert initialized.state == "READY"
    assert initialized.transitioned_to_ready

    replayed = admin.initialize()
    assert replayed.state == "READY"
    assert not replayed.transitioned_to_ready
    assert replayed.manifest_sha256 == initialized.manifest_sha256

    read_only_config = _read_only(config)
    checked = VNextDatabaseAdminFacade(read_only_config).check()
    readiness = VNextDatabaseAdminFacade(read_only_config).check_readiness()
    assert checked.state == readiness.state == "READY"
    assert checked.manifest_sha256 == readiness.manifest_sha256
    assert readiness.manifest_sha256 == initialized.manifest_sha256

    reader = open_database(read_only_config)
    with pytest.raises(CatalogRevisionNotFoundError):
        reader.get_catalog_revision()

    context = RepositoryContext.from_config(read_only_config)
    with context.SQLConnector() as connector:
        assert connector.check_table_exists("h2hdb_schema_epoch")
        assert not connector.check_table_exists("catalog_build_discoveries")
        assert not connector.check_table_exists("h2hdb_schema_migrations")
        _assert_gallery_identity_schema(connector, config.database.sql_type)
        if config.database.sql_type == "mariadb":
            assert (
                connector.fetch_all(
                    """
                SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                    ("catalog_publication_candidate_projections",),
                )
                == [
                    ("candidate_id", "binary(16)", "NO"),
                    ("create_count", "bigint(21) unsigned", "NO"),
                    ("rebuild_count", "bigint(21) unsigned", "NO"),
                    ("delete_count", "bigint(21) unsigned", "NO"),
                    ("new_galleries", "bigint(21) unsigned", "NO"),
                    ("changed_galleries", "bigint(21) unsigned", "NO"),
                ]
            )

    writable_context = RepositoryContext.from_config(config)
    with writable_context.SQLConnector() as connector:
        with pytest.raises(DatabaseDuplicateKeyError):
            with connector.transaction():
                connector.execute(
                    "INSERT INTO catalog_gallery_identities "
                    "(gallery_id, gallery_key, scope_key, locator_sha256) "
                    "VALUES (%s, %s, %s, %s)",
                    (1, b"g" * 32, b"s" * 32, b"l" * 32),
                )

    backend = config.database.sql_type
    backends = cast(Mapping[str, Mapping[str, object]], ARTIFACT["backends"])
    bootstrap_seeds = cast(Sequence[object], backends[backend]["bootstrap_seeds"])
    assert len(bootstrap_seeds) == 4_913


def test_default_generated_epoch_end_to_end_on_sqlite(
    sqlite_config: CoreConfig,
) -> None:
    _exercise_generated_epoch(sqlite_config)


def test_default_generated_epoch_end_to_end_on_live_mariadb(
    mariadb_config: CoreConfig,
) -> None:
    _exercise_generated_epoch(mariadb_config)
