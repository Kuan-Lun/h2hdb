"""The fault-matrix clone must preserve native schema and committed authority."""

from __future__ import annotations

import pytest
from vnext_database_snapshot import clone_database
from vnext_test_database import (
    DatabaseFactory,
    database_connector,
    foreign_key_checks_enabled,
)

from h2hdb import VNextDatabaseAdminFacade


def test_native_snapshot_preserves_ready_authority_and_dependent_views(
    database_factory: DatabaseFactory,
) -> None:
    source = database_factory.config("source")
    target = database_factory.config("target")
    admin = VNextDatabaseAdminFacade(source)
    admin.initialize()
    storage_uuid = bytes.fromhex("00112233445546778899aabbccddeeff")
    admin.bind_storage_instance(storage_uuid)
    clone_database(source, target)
    assert VNextDatabaseAdminFacade(target).check().state == "READY"
    assert (
        VNextDatabaseAdminFacade(target)
        .bind_storage_instance(storage_uuid)
        .storage_instance_uuid
        == storage_uuid
    )
    with database_connector(target) as connector:
        assert foreign_key_checks_enabled(connector)
    with pytest.raises(ValueError, match="target must be empty"):
        clone_database(source, target)
    database_factory.release("target")
    clone_database(source, database_factory.config("target"))
    assert (
        VNextDatabaseAdminFacade(database_factory.config("target")).check().state
        == "READY"
    )


def test_snapshot_rejects_aliasing_its_source(
    database_factory: DatabaseFactory,
) -> None:
    config = database_factory.config()
    with pytest.raises(ValueError, match="separate test databases"):
        clone_database(config, config)
