from __future__ import annotations

import hashlib
import importlib.util
import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from time import monotonic, sleep
from types import ModuleType

import pytest

from h2hdb import CoreConfig, VNextDatabaseAdminFacade, VNextDownloadQueueFacade
from h2hdb.catalog_refinement import CatalogSemanticValidationError
from h2hdb.repository import RepositoryContext
from h2hdb.schema_admin import VNextSchemaAdmin
from h2hdb.schema_epoch import (
    SchemaEpochAdmissionError,
    SchemaEpochDriftError,
    SchemaEpochValidationError,
)
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider

_SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "upgrade-source-collection-schema.py"
)


@pytest.fixture
def converter() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "source_collection_schema_upgrade_test", _SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(params=["sqlite", pytest.param("mariadb", marks=pytest.mark.mariadb)])
def conversion_config(request: pytest.FixtureRequest) -> CoreConfig:
    config: CoreConfig = request.getfixturevalue(f"{request.param}_config")
    return config


def _old_populated_database(
    config: CoreConfig,
    converter: ModuleType,
    *,
    populate: Callable[[CoreConfig], None] | None = None,
) -> tuple[object, ...]:
    admin = VNextDatabaseAdminFacade(config)
    try:
        admin.initialize()
    finally:
        admin.close()
    if populate is not None:
        populate(config)
    queue = VNextDownloadQueueFacade(config)
    try:
        queue.ensure_download_request(12345, "https://example.invalid/g/12345/")
        before = tuple(queue.list_download_requests())
    finally:
        queue.close()
    provider = GeneratedVNextSchemaProvider(
        "sqlite" if config.database.sql_type == "sqlite" else "mariadb"
    )
    retained, additions = converter._conversion_slices(provider)
    # Reconstruct the two exact retired staging shapes and remove the eleven
    # additions. The complete schema-7 SQL is independently checksum pinned.
    assert converter._slices_digest(retained) == converter._OLD_SLICES[provider.backend]
    context = RepositoryContext.from_config(config)
    try:
        with context.SQLConnector() as connector:
            if provider.backend == "sqlite":
                connector.execute("PRAGMA foreign_keys = OFF")
            try:
                for part in reversed(additions):
                    for statement in reversed(part.statements):
                        connector.execute(
                            f"DROP {statement.creates.kind.value} {statement.creates.name}"
                        )
                # A schema-7 fixture cannot retain completed jobs for the new
                # schema-8 collection target. Keep every legacy target intact,
                # including an interrupted observation cleanup under test.
                new_jobs = (
                    "SELECT job.cleanup_id FROM operational_cleanup_jobs AS job "
                    "JOIN operational_cleanup_sweep_targets AS target "
                    "ON target.target_key = job.target_key "
                    "WHERE target.target_kind = 'SOURCE_COLLECTION'"
                )
                assert not connector.fetch_one(
                    new_jobs + " AND job.state <> 'COMPLETE' LIMIT 1"
                )
                for table in (
                    "operational_cleanup_checkpoints",
                    "operational_cleanup_cycle_roots",
                ):
                    connector.execute(
                        f"DELETE FROM {table} WHERE cleanup_id IN ({new_jobs})"
                    )
                connector.execute(
                    "DELETE FROM operational_cleanup_jobs WHERE target_key IN "
                    "(SELECT target_key FROM operational_cleanup_sweep_targets "
                    "WHERE target_kind = 'SOURCE_COLLECTION')"
                )
                connector.execute(
                    "DELETE FROM operational_cleanup_sweep_targets WHERE target_kind = 'SOURCE_COLLECTION'"
                )
                connector.execute(
                    "DELETE FROM operational_cleanup_phases WHERE target_kind = 'SOURCE_COLLECTION'"
                )
                connector.execute(
                    "DELETE FROM operational_cleanup_target_kinds WHERE target_kind = 'SOURCE_COLLECTION'"
                )
                if provider.backend == "sqlite":
                    for name in (converter._CLAIM, converter._HEADER):
                        part = converter._part(provider, name)
                        connector.execute(
                            f"DROP TABLE {part.statements[0].creates.name}"
                        )
                    for name in (converter._HEADER, converter._CLAIM):
                        part = converter._old_slice(
                            provider, converter._part(provider, name)
                        )
                        for statement in part.statements:
                            connector.execute(statement.sql)
                else:
                    header = converter._old_relation(provider, converter._HEADER)
                    storage = header["checks"][0][1]
                    connector.execute(
                        "ALTER TABLE operational_gallery_observation_stagings DROP FOREIGN KEY fk_gallery_observation_staging_1"
                    )
                    connector.execute(
                        "ALTER TABLE operational_gallery_observation_stagings "
                        "DROP INDEX uk_operational_gallery_observation_stagings_1, "
                        "DROP CONSTRAINT ck_gallery_observation_staging_storage_domain, "
                        "ADD COLUMN build_id BINARY(16) NOT NULL AFTER staging_id, "
                        "ADD CONSTRAINT uk_operational_gallery_observation_stagings_1 UNIQUE (build_id), "
                        "ADD CONSTRAINT uk_operational_gallery_observation_stagings_2 UNIQUE (gallery_id, observation_id), "
                        "ADD CONSTRAINT fk_gallery_observation_staging_1 FOREIGN KEY (build_id) REFERENCES catalog_source_build_descriptor (build_id), "
                        "ADD CONSTRAINT fk_gallery_observation_staging_2 FOREIGN KEY (gallery_id, observation_id) REFERENCES catalog_gallery_observation_allocations (gallery_id, observation_id), "
                        "ADD CONSTRAINT ck_gallery_observation_staging_build_id_len CHECK (octet_length(build_id) = 16), "
                        f"ADD CONSTRAINT ck_gallery_observation_staging_storage_domain CHECK ({storage})"
                    )
                    connector.execute(
                        "ALTER TABLE operational_gallery_observation_staging_claims DROP FOREIGN KEY fk_gallery_observation_staging_claim_2"
                    )
                    connector.execute(
                        "ALTER TABLE operational_gallery_observation_staging_claims "
                        "ADD CONSTRAINT fk_gallery_observation_staging_claim_2 FOREIGN KEY (ingest_generation) REFERENCES operational_source_build_generations (generation)"
                    )
                for name in converter._CLEANUP:
                    old = converter._old_slice(
                        provider, converter._part(provider, name)
                    )
                    if provider.backend == "sqlite":
                        table = old.statements[0].creates.name
                        temporary = "fixture_old_" + table
                        sql = old.statements[0].sql.replace(
                            'CREATE TABLE IF NOT EXISTS "' + table + '"',
                            'CREATE TABLE IF NOT EXISTS "' + temporary + '"',
                        )
                        connector.execute(sql)
                        connector.execute(
                            f"INSERT INTO {temporary} SELECT * FROM {table}"
                        )
                        connector.execute(f"DROP TABLE {table}")
                        connector.execute(f"ALTER TABLE {temporary} RENAME TO {table}")
                        for extra in old.statements[1:]:
                            connector.execute(extra.sql)
                    else:
                        target = dict(converter._relation(provider, name)["checks"])
                        for key, expression in converter._old_relation(provider, name)[
                            "checks"
                        ]:
                            if target[key] != expression:
                                connector.execute(
                                    f"ALTER TABLE {old.statements[0].creates.name} DROP CONSTRAINT {key}, ADD CONSTRAINT {key} CHECK ({expression})"
                                )
                with connector.transaction():
                    connector.execute(
                        "UPDATE h2hdb_schema_epoch SET schema_version = 7, "
                        "manifest_sha256 = %s WHERE singleton_id = 1",
                        (bytes.fromhex(converter._OLD_MANIFESTS[provider.backend]),),
                    )
            finally:
                if provider.backend == "sqlite":
                    connector.execute("PRAGMA foreign_keys = ON")
            if provider.backend == "sqlite":
                assert not connector.fetch_one("PRAGMA foreign_key_check")
    finally:
        context.close()
    return before


def _requests(config: CoreConfig) -> tuple[object, ...]:
    queue = VNextDownloadQueueFacade(config)
    try:
        return tuple(queue.list_download_requests())
    finally:
        queue.close()


def _assert_conversion_pending(config: CoreConfig, converter: ModuleType) -> None:
    provider = GeneratedVNextSchemaProvider(
        "sqlite" if config.database.sql_type == "sqlite" else "mariadb"
    )
    backend = provider.backend
    conversion_manifest = hashlib.sha256(
        b"h2hdb-offline-source-collection-schema-7-to-8\0"
        + bytes.fromhex(converter._OLD_MANIFESTS[backend])
        + bytes.fromhex(provider.definition.manifest_sha256)
    ).digest()
    context = RepositoryContext.from_config(config)
    try:
        with context.SQLConnector() as connector:
            row = converter._control(connector)
            assert row[1:5] == (3, 8, "BUILDING", conversion_manifest)
            assert row[6] is None
    finally:
        context.close()
    admin = VNextDatabaseAdminFacade(config)
    try:
        for call in (admin.check_readiness, admin.check):
            with pytest.raises(SchemaEpochAdmissionError):
                call()
        with pytest.raises(SchemaEpochDriftError, match="manifest differs"):
            admin.initialize()
    finally:
        admin.close()
    context = RepositoryContext.from_config(config)
    try:
        with context.SQLConnector() as connector:
            assert converter._control(connector) == row
    finally:
        context.close()


def _retained_data_tables(config: CoreConfig, converter: ModuleType) -> tuple[str, ...]:
    provider = GeneratedVNextSchemaProvider(
        "sqlite" if config.database.sql_type == "sqlite" else "mariadb"
    )
    excluded = {
        "h2hdb_schema_epoch",
        "operational_database_audit_states",
        "operational_cleanup_target_kinds",
        "operational_cleanup_phases",
        "operational_cleanup_sweep_targets",
    }
    return tuple(
        relation["table"]
        for relation in provider.generated_definition_data["relations"]
        if relation["relation"] not in converter._ADDITIONS
        and relation["table"] not in excluded
        and relation["kind"] == "table"
    )


def test_populated_schema_conversion_preserves_facts_and_replays(
    conversion_config: CoreConfig,
    converter: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = _old_populated_database(conversion_config, converter)
    progress: list[str] = []
    assert converter.upgrade(conversion_config, progress=progress.append) == "converted"
    assert progress[-1] == "ready_committed"
    assert _requests(conversion_config) == before
    admin = VNextDatabaseAdminFacade(conversion_config)
    try:
        assert admin.check().schema_version == 8
    finally:
        admin.close()
    assert (
        converter.upgrade(conversion_config, progress=progress.append)
        == "already_converted"
    )
    assert _requests(conversion_config) == before

    def forbid_duplicate_audit(_admin: VNextSchemaAdmin) -> None:
        pytest.fail("converter already completed the full audit")

    admin = VNextDatabaseAdminFacade(conversion_config)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(VNextSchemaAdmin, "check", forbid_duplicate_audit)
            startup = admin.start_ingest_runtime(
                lease_duration_microseconds=300_000_000
            )
        assert startup.full_audit is None
        admin.finish_ingest_runtime(startup.session)
    finally:
        admin.close()


@pytest.mark.parametrize(
    "interruption",
    [
        "conversion_marked",
        "object_created",
        "owners_copied",
        "header_rebuilt",
        "claim_rebuilt",
        "cleanup_registry_rebuilt",
        "bootstrap_completed",
        "schema_committed",
        "addition_validated",
        "full_audit_completed",
        "ready_committed",
    ],
)
def test_conversion_interruption_preserves_data_and_resumes(
    conversion_config: CoreConfig, converter: ModuleType, interruption: str
) -> None:
    before = _old_populated_database(conversion_config, converter)

    def interrupt(checkpoint: str) -> None:
        if checkpoint == interruption:
            raise RuntimeError("simulated converter termination")

    with pytest.raises(RuntimeError, match="simulated converter termination"):
        converter.upgrade(conversion_config, progress=interrupt)
    assert _requests(conversion_config) == before
    # SQLite rolls back its structural transaction or retains its committed
    # BUILDING phase. MariaDB resumes exact validated implicit DDL commits.
    context = RepositoryContext.from_config(conversion_config)
    try:
        with context.SQLConnector() as connector:
            row = converter._control(connector)
    finally:
        context.close()
    if row[3] == "BUILDING":
        admin = VNextDatabaseAdminFacade(conversion_config)
        try:
            for call in (admin.check_readiness, admin.check):
                with pytest.raises(SchemaEpochAdmissionError):
                    call()
            with pytest.raises(SchemaEpochDriftError, match="manifest differs"):
                admin.initialize()
        finally:
            admin.close()
        context = RepositoryContext.from_config(conversion_config)
        try:
            with context.SQLConnector() as connector:
                assert converter._control(connector) == row
        finally:
            context.close()
    assert converter.upgrade(conversion_config, progress=lambda _: None) in {
        "converted",
        "already_converted",
    }
    assert _requests(conversion_config) == before


def test_foreign_marker_and_extra_object_are_rejected_without_changes(
    sqlite_config: CoreConfig, converter: ModuleType
) -> None:
    before = _old_populated_database(sqlite_config, converter)
    context = RepositoryContext.from_config(sqlite_config)
    try:
        with context.SQLConnector() as connector:
            connector.execute("CREATE TABLE foreign_object (value INTEGER)")
            control_before = converter._control(connector)
        with pytest.raises(SchemaEpochValidationError, match="inventory"):
            converter.upgrade(sqlite_config, progress=lambda _: None)
        with context.SQLConnector() as connector:
            assert converter._control(connector) == control_before
            connector.execute("DROP TABLE foreign_object")
            with connector.transaction():
                connector.execute(
                    "UPDATE h2hdb_schema_epoch SET manifest_sha256 = %s WHERE singleton_id = 1",
                    (b"x" * 32,),
                )
            control_before = converter._control(connector)
        with pytest.raises(SchemaEpochAdmissionError, match="Only the exact"):
            converter.upgrade(sqlite_config, progress=lambda _: None)
        with context.SQLConnector() as connector:
            assert converter._control(connector) == control_before
    finally:
        context.close()
    assert _requests(sqlite_config) == before


def test_failed_final_audit_never_publishes_ready(
    conversion_config: CoreConfig,
    converter: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = _old_populated_database(conversion_config, converter)

    def reject(*_args: object, **_kwargs: object) -> None:
        raise SchemaEpochValidationError("semantic corruption detected")

    with monkeypatch.context() as patch:
        patch.setattr(GeneratedVNextSchemaProvider, "validate_semantics", reject)
        with pytest.raises(SchemaEpochValidationError, match="semantic corruption"):
            converter.upgrade(conversion_config, progress=lambda _: None)
    _assert_conversion_pending(conversion_config, converter)
    assert _requests(conversion_config) == before
    assert converter.upgrade(conversion_config, progress=lambda _: None) == "converted"


def test_real_role_audit_failure_preserves_facts_and_conversion_resume(
    conversion_config: CoreConfig, converter: ModuleType
) -> None:
    from test_vnext_source_batches import _source_batch, _source_batch_clock
    from test_vnext_source_marker import MarkerSource
    from vnext_fault_harness import snapshot_database
    from vnext_pipeline import claim_session, gallery, ingest_policy

    from h2hdb import VNextIngestFacade

    source = MarkerSource((gallery(8101, pages=[b"first", b"second"]),))

    def populate(config: CoreConfig) -> None:
        with (
            _source_batch_clock(config) as clock,
            VNextIngestFacade(config, clock=clock) as facade,
        ):
            session = claim_session(facade)
            policy = facade.ensure_policy(session, ingest_policy())
            _source_batch(facade, session, policy, source, None)

    requests_before = _old_populated_database(
        conversion_config, converter, populate=populate
    )
    context = RepositoryContext.from_config(conversion_config)
    try:
        with context.SQLConnector() as connector, connector.transaction():
            occurrence = connector.fetch_one(
                "SELECT gallery_id, observation_id, file_sha256, occurrence_count "
                "FROM catalog_gallery_observation_file_hash_occurrences LIMIT 1"
            )
            assert occurrence
            connector.execute(
                "DELETE FROM catalog_gallery_observation_file_hash_occurrences "
                "WHERE gallery_id = %s AND observation_id = %s AND file_sha256 = %s",
                occurrence[:3],
            )
        provider = GeneratedVNextSchemaProvider(
            "sqlite" if conversion_config.database.sql_type == "sqlite" else "mariadb"
        )
        tables = tuple(
            relation["table"]
            for relation in provider.generated_definition_data["relations"]
            if relation["kind"] == "table"
            and relation["table"].startswith("catalog_gallery_observation")
        )
        before = snapshot_database(conversion_config, tables=tables)
        for attempt in range(2):
            progress: list[str] = []
            with pytest.raises(
                CatalogSemanticValidationError,
                match="retained file-hash occurrences differ from exact CONTENT roles",
            ):
                converter.upgrade(conversion_config, progress=progress.append)
            assert progress[-1] == "full_audit_started"
            assert "schema_committed" in progress
            if attempt:
                assert "conversion_marked" not in progress
                assert "object_created" not in progress
                assert "header_rebuilt" not in progress
            _assert_conversion_pending(conversion_config, converter)
            assert snapshot_database(conversion_config, tables=tables) == before
            assert _requests(conversion_config) == requests_before
        # Restore only the deliberately removed fixture row. The converter must
        # never silently repair genuine catalog inconsistency or bypass the audit.
        with context.SQLConnector() as connector, connector.transaction():
            connector.execute(
                "INSERT INTO catalog_gallery_observation_file_hash_occurrences "
                "(gallery_id, observation_id, file_sha256, occurrence_count) "
                "VALUES (%s, %s, %s, %s)",
                occurrence,
            )
        repaired = snapshot_database(conversion_config, tables=tables)
        assert (
            converter.upgrade(conversion_config, progress=lambda _: None) == "converted"
        )
        assert (
            converter.upgrade(conversion_config, progress=lambda _: None)
            == "already_converted"
        )
        assert snapshot_database(conversion_config, tables=tables) == repaired
        assert _requests(conversion_config) == requests_before
    finally:
        context.close()


def test_failed_legacy_audit_resumes_exact_committed_cleanup_without_data_changes(
    conversion_config: CoreConfig,
    converter: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from test_role_derivation_cleanup import _prepare_retirement, _reach_fact_gap
    from vnext_fault_harness import snapshot_database

    from h2hdb import catalog_refinement

    def populate(config: CoreConfig) -> None:
        _prepare_retirement(config, pages=1)
        _reach_fact_gap(config)

    requests_before = _old_populated_database(
        conversion_config, converter, populate=populate
    )
    tables = _retained_data_tables(conversion_config, converter)
    before = snapshot_database(conversion_config, tables=tables)
    # Version 0.41.0 compared every retained FILE with stored occurrences without
    # recognizing the exact committed child-first retirement authority. Recreate
    # that omission, letting the real validator reject the genuine cleanup gap.
    with monkeypatch.context() as patch:
        patch.setattr(
            catalog_refinement,
            "_validated_open_observation_retirement",
            lambda _connector: None,
        )
        with pytest.raises(
            CatalogSemanticValidationError,
            match="retained file-hash occurrences differ from exact CONTENT roles",
        ):
            converter.upgrade(conversion_config, progress=lambda _: None)
    _assert_conversion_pending(conversion_config, converter)
    assert snapshot_database(conversion_config, tables=tables) == before

    progress: list[str] = []
    assert converter.upgrade(conversion_config, progress=progress.append) == "converted"
    assert "conversion_marked" not in progress
    assert "object_created" not in progress
    assert progress[-1] == "ready_committed"
    assert snapshot_database(conversion_config, tables=tables) == before
    assert _requests(conversion_config) == requests_before
    assert (
        converter.upgrade(conversion_config, progress=lambda _: None)
        == "already_converted"
    )
    assert snapshot_database(conversion_config, tables=tables) == before


@pytest.mark.deep
def test_real_process_kill_recovers_conversion_without_losing_facts(
    conversion_config: CoreConfig, converter: ModuleType, tmp_path: Path
) -> None:
    before = _old_populated_database(conversion_config, converter)
    config_path = tmp_path / "local-test-config.json"
    config_path.write_text(conversion_config.model_dump_json())
    reached = tmp_path / "addition-validated"
    program = """
import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from threading import Event
from h2hdb import load_config
spec = importlib.util.spec_from_file_location('offline_converter_child', sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
def checkpoint(name):
    if name == 'schema_committed':
        Path(sys.argv[3]).write_text(name)
        Event().wait(120)
module.upgrade(load_config(sys.argv[2]), progress=checkpoint)
"""
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(_SCRIPT.parents[1] / "src")
    process = subprocess.Popen(
        [sys.executable, "-c", program, str(_SCRIPT), str(config_path), str(reached)],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = monotonic() + 90
        while (
            not reached.exists() and process.poll() is None and monotonic() < deadline
        ):
            sleep(0.01)
        if not reached.exists():
            process.kill()
            stdout, stderr = process.communicate(timeout=10)
            pytest.fail(f"converter never reached kill checkpoint: {stdout} {stderr}")
        process.kill()
        process.communicate(timeout=10)
        assert process.returncode != 0
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=10)
    assert _requests(conversion_config) == before
    assert converter.upgrade(conversion_config, progress=lambda _: None) == "converted"
    assert _requests(conversion_config) == before


@pytest.mark.parametrize(
    "checkpoint", ["header_foreign_detached", "claim_foreign_detached"]
)
@pytest.mark.mariadb
def test_mariadb_detached_foreign_key_phase_resumes_exactly(
    mariadb_config: CoreConfig, converter: ModuleType, checkpoint: str
) -> None:
    before = _old_populated_database(mariadb_config, converter)

    def interrupt(current: str) -> None:
        if current == checkpoint:
            raise RuntimeError("interrupted exact detached FK shape")

    with pytest.raises(RuntimeError, match="detached FK"):
        converter.upgrade(mariadb_config, progress=interrupt)
    assert converter.upgrade(mariadb_config, progress=lambda _: None) == "converted"
    assert _requests(mariadb_config) == before


def test_conversion_preserves_publication_source_and_opaque_artifact_facts(
    conversion_config: CoreConfig, converter: ModuleType
) -> None:
    from test_vnext_source_batches import (
        _publications,
        _source_batch,
        _source_batch_clock,
    )
    from test_vnext_source_marker import MarkerSource
    from vnext_fault_harness import snapshot_database
    from vnext_pipeline import (
        MemoryLibrary,
        claim_session,
        gallery,
        ingest_policy,
        run_analysis,
        run_publication,
    )

    from h2hdb import VNextIngestFacade

    source = MarkerSource(
        (gallery(8101, pages=[b"first", b"second"]), gallery(8102, pages=[b"third"]))
    )
    library = MemoryLibrary(source)

    def populate(config: CoreConfig) -> None:
        with (
            _source_batch_clock(config) as clock,
            VNextIngestFacade(config, clock=clock) as facade,
        ):
            session = claim_session(facade)
            policy = facade.ensure_policy(
                session, ingest_policy(artifacts_required=True)
            )
            receipt, _ = _source_batch(facade, session, policy, source, None)
            run_analysis(facade, session, policy, receipt.build_id)
            run_publication(facade, session, policy, library)
            facade.complete_ingest(session)

    _old_populated_database(conversion_config, converter, populate=populate)
    tables = _retained_data_tables(conversion_config, converter)
    before = snapshot_database(conversion_config, tables=tables)
    objects = dict(library.objects)
    render_calls = library.render_calls
    assert render_calls == 2 and objects
    assert before["catalog_gallery_observations"]
    assert before["catalog_artifacts"]
    assert converter.upgrade(conversion_config, progress=lambda _: None) == "converted"
    assert snapshot_database(conversion_config, tables=tables) == before
    assert dict(library.objects) == objects and library.render_calls == render_calls
    assert [publication.gid for publication in _publications(conversion_config)] == [
        8101,
        8102,
    ]
