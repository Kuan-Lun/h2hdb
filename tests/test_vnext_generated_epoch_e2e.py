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


def _read_only(config: CoreConfig) -> CoreConfig:
    return config.model_copy(
        update={
            "database": config.database.model_copy(
                update={"access_mode": DatabaseAccessMode.read_only}
            )
        }
    )


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

    backend = config.database.sql_type
    backends = cast(Mapping[str, Mapping[str, object]], ARTIFACT["backends"])
    bootstrap_seeds = cast(Sequence[object], backends[backend]["bootstrap_seeds"])
    assert len(bootstrap_seeds) == 4_646


def test_default_generated_epoch_end_to_end_on_sqlite(
    sqlite_config: CoreConfig,
) -> None:
    _exercise_generated_epoch(sqlite_config)


def test_default_generated_epoch_end_to_end_on_live_mariadb(
    mariadb_config: CoreConfig,
) -> None:
    _exercise_generated_epoch(mariadb_config)
