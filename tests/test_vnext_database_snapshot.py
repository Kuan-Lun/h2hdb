"""The fault-matrix clone must preserve native schema and committed authority."""

from __future__ import annotations

from contextlib import closing

import pytest
from vnext_corpora import Corpus, _build_owned_staging
from vnext_database_snapshot import (
    ReusableDatabaseSnapshot,
    clone_database,
    database_digest,
)
from vnext_pipeline import catalog_view, full_check, initialize_database
from vnext_test_database import (
    DatabaseFactory,
    database_connector,
    foreign_key_checks_enabled,
    inspect_all,
)

from h2hdb import VNextDatabaseAdminFacade


def test_native_snapshot_preserves_ready_authority_and_dependent_views(
    database_factory: DatabaseFactory,
) -> None:
    source = database_factory.config("source")
    target = database_factory.config("target")
    storage_uuid = bytes.fromhex("00112233445546778899aabbccddeeff")
    with closing(VNextDatabaseAdminFacade(source)) as admin:
        admin.initialize()
        admin.bind_storage_instance(storage_uuid)
    clone_database(source, target)
    with closing(VNextDatabaseAdminFacade(target)) as admin:
        assert admin.check().state == "READY"
        assert (
            admin.bind_storage_instance(storage_uuid).storage_instance_uuid
            == storage_uuid
        )
    with database_connector(target) as connector:
        assert foreign_key_checks_enabled(connector)
    with pytest.raises(ValueError, match="target must be empty"):
        clone_database(source, target)
    database_factory.release("target")
    clone_database(source, database_factory.config("target"))
    with closing(VNextDatabaseAdminFacade(database_factory.config("target"))) as admin:
        assert admin.check().state == "READY"


def test_snapshot_rejects_aliasing_its_source(
    database_factory: DatabaseFactory,
) -> None:
    config = database_factory.config()
    with pytest.raises(ValueError, match="separate test databases"):
        clone_database(config, config)


def _small_snapshot(factory: DatabaseFactory) -> ReusableDatabaseSnapshot:
    source, target = factory.config("source"), factory.config("target")
    with database_connector(source) as connector:
        for name in ("fixture_values", "fixture_deleted", "fixture_empty"):
            connector.execute(
                f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, value BLOB)"
            )
        with connector.transaction():
            connector.execute(
                "INSERT INTO fixture_values VALUES (1, %s)", (b"original",)
            )
            connector.execute(
                "INSERT INTO fixture_deleted VALUES (2, %s)", (b"retained",)
            )
    return ReusableDatabaseSnapshot(factory, source, target)


def test_reusable_snapshot_restores_every_table_and_committed_fault_visibility(
    database_factory: DatabaseFactory,
) -> None:
    snapshot = _small_snapshot(database_factory)
    original = database_digest(snapshot.source)
    for round_number in range(3):
        with database_connector(snapshot.target) as writer, writer.transaction():
            writer.execute("UPDATE fixture_values SET value = %s", (b"corrupt",))
            writer.execute("DELETE FROM fixture_deleted")
            writer.execute(
                "INSERT INTO fixture_empty VALUES (%s, %s)", (round_number, b"new")
            )
        # A new native connection observes the committed fault before restore.
        with database_connector(snapshot.target) as reader:
            assert inspect_all(reader, "SELECT value FROM fixture_values") == [
                (b"corrupt",)
            ]
            assert inspect_all(reader, "SELECT * FROM fixture_deleted") == []
            assert inspect_all(reader, "SELECT id FROM fixture_empty") == [
                (round_number,)
            ]
        assert database_digest(snapshot.target) != original
        snapshot.restore()
        assert database_digest(snapshot.target) == original
        assert database_digest(snapshot.source) == original
        with database_connector(snapshot.target) as reader:
            assert inspect_all(reader, "SELECT value FROM fixture_values") == [
                (b"original",)
            ]
            assert inspect_all(reader, "SELECT id FROM fixture_deleted") == [(2,)]
            assert inspect_all(reader, "SELECT * FROM fixture_empty") == []


def test_reusable_snapshot_preservation_oracle_rejects_omitted_table_reset(
    database_factory: DatabaseFactory,
) -> None:
    snapshot = _small_snapshot(database_factory)
    original = database_digest(snapshot.source)
    snapshot.restore()
    # Deliberately degraded restore leaves a committed row in a table whose
    # baseline is empty. The all-table oracle must reject this native state.
    with database_connector(snapshot.target) as writer, writer.transaction():
        writer.execute("INSERT INTO fixture_empty VALUES (7, %s)", (b"leftover",))
    with pytest.raises(AssertionError):
        assert database_digest(snapshot.target) == original
    assert database_digest(snapshot.source) == original


def test_reusable_snapshot_rejects_schema_drift_and_unowned_target(
    database_factory: DatabaseFactory,
) -> None:
    snapshot = _small_snapshot(database_factory)
    with pytest.raises(ValueError, match="owned by this factory"):
        ReusableDatabaseSnapshot(
            database_factory, snapshot.source, snapshot.target.model_copy()
        )
    with database_connector(snapshot.target) as writer:
        writer.execute("ALTER TABLE fixture_values ADD COLUMN unexpected INTEGER")
    with pytest.raises(ValueError, match="schema drift"):
        snapshot.restore()


def test_build_owned_staging_is_resumable_and_snapshot_keeps_source_immutable(
    database_factory: DatabaseFactory,
) -> None:
    source = database_factory.config("staging")
    initialize_database(source)
    adapter, library = _build_owned_staging(source)
    corpus = Corpus(
        "build-owned-staging", source, adapter, library, "source:build-owned-staging"
    )
    snapshot = ReusableDatabaseSnapshot(
        database_factory, source, database_factory.config("resumed")
    )
    original = database_digest(source)
    assert corpus.mid_flight and not corpus.consumable
    for _ in range(2):
        snapshot.restore()
        corpus.resume(snapshot.target)
        assert full_check(snapshot.target).state == "READY"
        assert catalog_view(snapshot.target)["publication_count"] == 1
        assert database_digest(source) == original
        assert library.current == {}
    # Admin handles must be closed before target reuse or fixture teardown.
    with closing(VNextDatabaseAdminFacade(snapshot.target)) as admin:
        assert admin.check().state == "READY"


def test_reusable_snapshot_rejects_trigger_added_between_replays(
    database_factory: DatabaseFactory,
) -> None:
    snapshot = _small_snapshot(database_factory)
    body = (
        "BEGIN SELECT 1; END"
        if database_factory.backend == "sqlite"
        else "SET NEW.value = NEW.value"
    )
    with database_connector(snapshot.target) as writer:
        writer.execute(
            "CREATE TRIGGER fixture_trigger BEFORE INSERT ON fixture_values FOR EACH ROW "
            + body
        )
    with pytest.raises(ValueError, match="schema drift"):
        snapshot.restore()


@pytest.mark.backend_specific(
    backend="mariadb",
    reason="MariaDB permits cross-database foreign keys; SQLite cannot express a foreign key referencing a different database",
)
@pytest.mark.mariadb
def test_reusable_snapshot_rejects_foreign_key_retargeted_to_another_database(
    mariadb_database_factory: DatabaseFactory,
) -> None:
    source = mariadb_database_factory.config("source")
    other = mariadb_database_factory.config("other")
    for config in (source, other):
        with database_connector(config) as writer:
            writer.execute("CREATE TABLE fixture_parent (id INTEGER PRIMARY KEY)")
    with database_connector(source) as writer:
        writer.execute(
            "CREATE TABLE fixture_child (id INTEGER PRIMARY KEY, "
            "CONSTRAINT fixture_parent_fk FOREIGN KEY (id) REFERENCES fixture_parent (id))"
        )
    snapshot = ReusableDatabaseSnapshot(
        mariadb_database_factory, source, mariadb_database_factory.config("target")
    )
    try:
        with database_connector(snapshot.target) as writer:
            writer.execute(
                "ALTER TABLE fixture_child DROP FOREIGN KEY fixture_parent_fk"
            )
            writer.execute(
                "ALTER TABLE fixture_child ADD CONSTRAINT fixture_parent_fk FOREIGN KEY (id) "
                f"REFERENCES `{other.database.database}`.fixture_parent (id)"
            )
        with pytest.raises(ValueError, match="schema drift"):
            snapshot.restore()
    finally:
        # This engine-specific fixture introduces a cross-database child.
        # Drop that child database before the referenced parent owner closes.
        mariadb_database_factory.release("target")
