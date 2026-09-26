"""Schema-8 conversion retains authority across bounded copies and failures."""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import runpy
import sqlite3
import subprocess
import sys
from contextlib import nullcontext
from pathlib import Path
from typing import Any

import pytest

from h2hdb import CoreConfig
from h2hdb.database_audit import (
    DatabaseAuditStateRepository,
    _State,
    read_database_audit_state,
)
from h2hdb.repository import RepositoryContext
from h2hdb.schema_epoch import (
    MariaDBSchemaEpochCatalog,
    SchemaEpochValidationError,
    SQLiteSchemaEpochCatalog,
)
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_transaction import VNextUnitOfWork

_ROOT = Path(__file__).parents[1]
_SCRIPT = _ROOT / "scripts/upgrade-observation-upload-time-schema.py"


@pytest.fixture
def converter() -> dict[str, Any]:
    return runpy.run_path(str(_SCRIPT))


@pytest.fixture
def source_schema(tmp_path: Path) -> Path:
    source = tmp_path / "schema8-provider.bin"
    with gzip.open(_ROOT / "tests/fixtures/schema8-provider.bin.gz", "rb") as stream:
        source.write_bytes(stream.read(4_429_650))
    return source


def _create_schema8(
    config: CoreConfig, converter: dict[str, Any], source: Path
) -> None:
    backend = config.database.sql_type
    snapshot = converter["_load_source"](source, backend)
    catalog = (
        SQLiteSchemaEpochCatalog()
        if backend == "sqlite"
        else MariaDBSchemaEpochCatalog()
    )
    context = RepositoryContext.from_config(config)
    try:
        with context.SQLConnector() as connector:
            with connector.transaction() if backend == "sqlite" else nullcontext():
                catalog.create_control_table(connector)
                for part in snapshot.slices.values():
                    for statement in part.statements:
                        connector.execute(statement.sql)
            connector.commit()
            with connector.transaction():
                for seed in snapshot.payload["bootstrap_seeds"]:
                    connector.execute(seed["sql"], seed["parameters"])
                connector.execute(
                    "INSERT INTO h2hdb_schema_epoch (singleton_id,epoch,schema_version,state,manifest_sha256,started_at,ready_at) VALUES (1,3,8,'READY',%s,0,0)",
                    (bytes.fromhex(converter["_SOURCE_MANIFESTS"][backend]),),
                )
    finally:
        context.close()


@pytest.mark.merge_smoke
def test_complete_empty_schema8_conversion_and_replay(
    converter: dict[str, Any],
    source_schema: Path,
    sqlite_config: CoreConfig,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    caplog.set_level(logging.INFO, logger="h2hdb.database_performance")
    _create_schema8(sqlite_config, converter, source_schema)
    logfile = tmp_path / "unmounted-runtime-logs" / "core.log"
    offline_config = sqlite_config.model_copy(
        update={"logger": sqlite_config.logger.model_copy(update={"file": logfile})}
    )
    events: list[str] = []
    assert (
        converter["upgrade"](
            offline_config, source_schema=source_schema, progress=events.append
        )
        == "converted"
    )
    assert "full_audit_durably_recorded" in events
    assert not logfile.parent.exists()
    assert events.index("full_audit_durably_recorded") < events.index("ready_committed")
    diagnostics = [
        json.loads(record.getMessage().removeprefix("database_performance "))
        for record in caplog.records
        if record.name == "h2hdb.database_performance"
    ]
    assert diagnostics and any(
        record.get("event") == "completed" and record.get("sql_calls", 0) > 0
        for record in diagnostics
    )
    events.clear()
    assert (
        converter["upgrade"](
            sqlite_config, source_schema=source_schema, progress=events.append
        )
        == "already_converted"
    )
    assert "full_audit_started" not in events


@pytest.mark.mariadb
@pytest.mark.deep
@pytest.mark.parametrize(
    ("fault", "occurrence"),
    (
        ("new_relation_ddl_response", 1),
        *(("mariadb_ddl_response", index) for index in range(1, 12)),
        ("view_dropped", 1),
        ("full_audit_durably_recorded", 1),
        ("ready_before_commit", 1),
    ),
)
def test_mariadb_conversion_resumes_independently_committed_ddl_and_audit(
    converter: dict[str, Any],
    source_schema: Path,
    mariadb_config: CoreConfig,
    fault: str,
    occurrence: int,
) -> None:
    _create_schema8(mariadb_config, converter, source_schema)
    old_audit = _retained_audit_state(mariadb_config)
    events: list[str] = []

    def fail(event: str) -> None:
        events.append(event)
        if event == fault and events.count(fault) == occurrence:
            raise RuntimeError("injected MariaDB response loss")

    with pytest.raises(RuntimeError, match="injected MariaDB response loss"):
        converter["upgrade"](mariadb_config, source_schema=source_schema, progress=fail)
    retained = _read_audit(mariadb_config)
    assert retained is not None and retained.generation == old_audit.generation + 1
    completed = "full_audit_durably_recorded" in events
    if not completed:
        assert retained.last_audit_at == old_audit.last_audit_at
        assert retained.validator_version == old_audit.validator_version
    resumed: list[str] = []
    assert (
        converter["upgrade"](
            mariadb_config, source_schema=source_schema, progress=resumed.append
        )
        == "converted"
    )
    assert resumed.count("full_audit_started") == (0 if completed else 1)
    audited = _read_audit(mariadb_config)
    assert audited is not None and audited.audit_pending == 0
    if completed:
        assert audited == retained


def _retained_audit_state(config: CoreConfig) -> _State:
    state = _State(
        7,
        b"a" * 16,
        0,
        60_000_000,
        123_000_000,
        30,
        100,
        12,
        "h2hdb/0.42.2;database-audit/1",
        500,
        200,
        1,
    )
    context = RepositoryContext.from_config(config)
    try:
        with context.SQLConnector() as connector, connector.transaction():
            DatabaseAuditStateRepository.save(
                VNextUnitOfWork(connector, backend=config.database.sql_type),
                None,
                state,
            )
    finally:
        context.close()
    return state


def _read_audit(config: CoreConfig) -> _State | None:
    context = RepositoryContext.from_config(config)
    try:
        with context.SQLConnector() as connector, connector.read_transaction():
            return read_database_audit_state(connector)
    finally:
        context.close()


@pytest.mark.parametrize(
    "fault",
    (
        "conversion_marked",
        "new_relation_ddl_response",
        "upload_time_copies_committed",
        "sqlite_table_before_commit",
        "sqlite_table_committed",
        "view_dropped",
        "converted_structure_committed",
        "full_audit_durably_recorded",
        "ready_before_commit",
        "ready_committed",
    ),
)
def test_conversion_resumes_physical_faults_and_never_repeats_a_durable_audit(
    converter: dict[str, Any],
    source_schema: Path,
    sqlite_config: CoreConfig,
    fault: str,
) -> None:
    _create_schema8(sqlite_config, converter, source_schema)
    old_audit = _retained_audit_state(sqlite_config)
    context = RepositoryContext.from_config(sqlite_config)
    try:
        with context.SQLConnector() as connector, connector.transaction():
            connector.execute(
                "INSERT INTO catalog_gallery_upload_times VALUES (7, 1700000000)"
            )
            connector.execute(
                "INSERT INTO catalog_source_gallery_name_gids VALUES (%s, 7)",
                (b"retained-gallery",),
            )
    finally:
        context.close()
    events: list[str] = []

    def fail(event: str) -> None:
        events.append(event)
        if event == fault:
            raise RuntimeError("injected upgrade fault")

    with pytest.raises(RuntimeError, match="injected upgrade fault"):
        converter["upgrade"](sqlite_config, source_schema=source_schema, progress=fail)
    durable_audit = _read_audit(sqlite_config)
    assert durable_audit is not None
    assert durable_audit.generation == old_audit.generation + 1
    assert durable_audit.owner_token != old_audit.owner_token
    completed = "full_audit_durably_recorded" in events
    if not completed:
        assert durable_audit.last_audit_at == old_audit.last_audit_at
        assert durable_audit.validator_version == old_audit.validator_version
    retry_events: list[str] = []
    result = converter["upgrade"](
        sqlite_config, source_schema=source_schema, progress=retry_events.append
    )
    assert result == (
        "already_converted" if fault == "ready_committed" else "converted"
    )
    assert retry_events.count("full_audit_started") == (0 if completed else 1)
    after = _read_audit(sqlite_config)
    assert (
        after is not None
        and after.audit_pending == 0
        and after.lease_expires_at is None
    )
    assert (
        after.minimum_interval_microseconds == old_audit.minimum_interval_microseconds
    )
    if completed:
        assert after == durable_audit
    context = RepositoryContext.from_config(sqlite_config)
    try:
        with context.SQLConnector() as connector, connector.read_transaction():
            assert connector.fetch_all(
                "SELECT * FROM catalog_source_gallery_name_gids"
            ) == [(b"retained-gallery", 7)]
            assert connector.fetch_all(
                "SELECT * FROM catalog_gallery_gid_identities"
            ) == [(7,)]
    finally:
        context.close()


@pytest.mark.parametrize("after_write", (False, True))
def test_audit_record_failure_rolls_back_baseline_and_temporary_target_marker(
    converter: dict[str, Any],
    source_schema: Path,
    sqlite_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    after_write: bool,
) -> None:
    _exercise_audit_record_failure(
        converter, source_schema, sqlite_config, monkeypatch, after_write
    )


@pytest.mark.mariadb
@pytest.mark.deep
@pytest.mark.parametrize("after_write", (False, True))
def test_mariadb_audit_failure_keeps_previous_committed_baseline(
    converter: dict[str, Any],
    source_schema: Path,
    mariadb_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    after_write: bool,
) -> None:
    _exercise_audit_record_failure(
        converter, source_schema, mariadb_config, monkeypatch, after_write
    )


def _exercise_audit_record_failure(
    converter: dict[str, Any],
    source_schema: Path,
    config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    after_write: bool,
) -> None:
    _create_schema8(config, converter, source_schema)
    old = _retained_audit_state(config)
    original = converter["_record_audit"]

    def fail(*arguments: Any) -> None:
        if after_write:
            original(*arguments)
        if config.database.sql_type == "mariadb":
            # A success written inside the audit transaction is not externally
            # visible before its matching AUDITED marker commits.
            visible = _read_audit(config)
            assert visible is not None
            assert visible.last_audit_at == old.last_audit_at
            assert visible.validator_version == old.validator_version
        raise RuntimeError("audit commit fault")

    globals_ = converter["_convert"].__globals__
    monkeypatch.setitem(globals_, "_record_audit", fail)
    with pytest.raises(RuntimeError, match="audit commit fault"):
        converter["upgrade"](
            config, source_schema=source_schema, progress=lambda _event: None
        )
    retained = _read_audit(config)
    assert retained is not None and retained.last_audit_at == old.last_audit_at
    assert retained.validator_version == old.validator_version
    monkeypatch.setitem(globals_, "_record_audit", original)
    events: list[str] = []
    assert (
        converter["upgrade"](
            config, source_schema=source_schema, progress=events.append
        )
        == "converted"
    )
    assert events.count("full_audit_started") == 1


def test_completed_audit_marker_rejects_changed_baseline(
    converter: dict[str, Any],
    source_schema: Path,
    sqlite_config: CoreConfig,
) -> None:
    _create_schema8(sqlite_config, converter, source_schema)

    def stop(event: str) -> None:
        if event == "full_audit_durably_recorded":
            raise RuntimeError("stop after audited commit")

    with pytest.raises(RuntimeError, match="stop after audited commit"):
        converter["upgrade"](sqlite_config, source_schema=source_schema, progress=stop)
    context = RepositoryContext.from_config(sqlite_config)
    try:
        with context.SQLConnector() as connector, connector.transaction():
            connector.execute(
                "UPDATE operational_database_audit_states SET last_audit_at = last_audit_at + 1"
            )
    finally:
        context.close()
    events: list[str] = []
    with pytest.raises(
        converter["SchemaEpochAdmissionError"], match="exact manifest-bound"
    ):
        converter["upgrade"](
            sqlite_config, source_schema=source_schema, progress=events.append
        )
    assert "full_audit_started" not in events and "ready_committed" not in events


def test_offline_input_admits_both_exact_historical_backends(
    converter: dict[str, Any],
    source_schema: Path,
) -> None:
    for backend in ("sqlite", "mariadb"):
        snapshot = converter["_load_source"](source_schema, backend)
        assert "gallery_upload_time" in snapshot.slices
        assert "gallery_observation_upload_time" not in snapshot.slices
        assert (
            snapshot.relations["gallery_upload_time"]["table"]
            == "catalog_gallery_upload_times"
        )


@pytest.mark.parametrize(
    "fault_event", ("copy_page_before_commit", "copy_page_committed")
)
def test_copy_replays_exact_prefix_after_rollback_or_response_loss(
    converter: dict[str, Any],
    sqlite_config: CoreConfig,
    fault_event: str,
) -> None:
    context = RepositoryContext.from_config(sqlite_config)
    expected = [(index, index * 7) for index in range(1, 258)]
    try:
        with context.SQLConnector() as connector:
            with connector.transaction():
                connector.execute(
                    "CREATE TABLE source_rows (identity INTEGER PRIMARY KEY, stamp INTEGER NOT NULL)"
                )
                connector.execute(
                    "CREATE TABLE copied_rows (identity INTEGER PRIMARY KEY, stamp INTEGER NOT NULL)"
                )
                connector.execute_many(
                    "INSERT INTO source_rows VALUES (%s, %s)", expected
                )
            events = []

            def fail(event: str) -> None:
                events.append(event)
                if event == fault_event:
                    raise RuntimeError("injected conversion interruption")

            arguments = {
                "source_query": "SELECT identity, stamp FROM source_rows",
                "destination": "copied_rows",
                "columns": ("identity", "stamp"),
                "key_width": 1,
            }
            with pytest.raises(RuntimeError, match="conversion interruption"):
                converter["_copy_rows"](connector, **arguments, checkpoint=fail)
            assert connector.fetch_one("SELECT COUNT(*) FROM copied_rows") == (
                0 if fault_event == "copy_page_before_commit" else 128,
            )
            assert (
                converter["_copy_rows"](
                    connector, **arguments, checkpoint=lambda _event: None
                )
                == 257
            )
            assert (
                connector.fetch_all(
                    "SELECT identity, stamp FROM copied_rows ORDER BY identity"
                )
                == expected
            )
            assert (
                converter["_copy_rows"](
                    connector, **arguments, checkpoint=lambda _event: None
                )
                == 257
            )
            assert (
                connector.fetch_all(
                    "SELECT identity, stamp FROM copied_rows ORDER BY identity"
                )
                == expected
            )
    finally:
        context.close()


@pytest.mark.parametrize(
    "damage", ("conflict", "extra_prefix", "extra_suffix", "null_source")
)
def test_copy_rejects_conflicting_or_incomplete_authority(
    converter: dict[str, Any],
    sqlite_config: CoreConfig,
    damage: str,
) -> None:
    context = RepositoryContext.from_config(sqlite_config)
    try:
        with context.SQLConnector() as connector:
            with connector.transaction():
                connector.execute(
                    "CREATE TABLE source_rows (identity INTEGER PRIMARY KEY, stamp INTEGER)"
                )
                connector.execute(
                    "CREATE TABLE copied_rows (identity INTEGER PRIMARY KEY, stamp INTEGER NOT NULL)"
                )
                connector.execute(
                    "INSERT INTO source_rows VALUES (1, %s)",
                    (None if damage == "null_source" else 7,),
                )
                if damage != "null_source":
                    identity = {"conflict": 1, "extra_prefix": 0, "extra_suffix": 2}[
                        damage
                    ]
                    connector.execute(
                        "INSERT INTO copied_rows VALUES (%s, 9)", (identity,)
                    )
            with pytest.raises(SchemaEpochValidationError):
                converter["_copy_rows"](
                    connector,
                    source_query="SELECT identity, stamp FROM source_rows",
                    destination="copied_rows",
                    columns=("identity", "stamp"),
                    key_width=1,
                    checkpoint=lambda _event: None,
                )
    finally:
        context.close()


@pytest.mark.parametrize("journal", ("DELETE", "WAL"))
def test_sqlite_conversion_gate_excludes_competing_writer_across_page_commits(
    converter: dict[str, Any],
    tmp_path: Path,
    journal: str,
) -> None:
    path = tmp_path / "database.sqlite3"
    with SQLiteConnector(str(path)) as connector:
        connector.execute(f"PRAGMA journal_mode = {journal}")
        connector.execute(
            "CREATE TABLE h2hdb_schema_epoch (singleton_id INTEGER PRIMARY KEY)"
        )
        connector.execute("INSERT INTO h2hdb_schema_epoch VALUES (1)")
        with sqlite3.connect(path, timeout=0.01, isolation_level=None) as competitor:
            with converter["_exclusive_conversion"](connector, "sqlite"):
                for _ in range(3):
                    with connector.transaction():
                        connector.execute(
                            "UPDATE h2hdb_schema_epoch SET singleton_id = 1"
                        )
                    with pytest.raises(sqlite3.OperationalError, match="locked"):
                        competitor.execute("BEGIN IMMEDIATE")
        # The connection retains the lock until close, including after its
        # context manager exits, so a later phase cannot open a competing gate.
    with sqlite3.connect(path, timeout=0.01, isolation_level=None) as competitor:
        competitor.execute("BEGIN IMMEDIATE")
        competitor.rollback()


@pytest.mark.parametrize("count", (127, 128, 129, 257))
def test_composite_copy_caps_pages_and_seeks_past_retained_prefix(
    converter: dict[str, Any],
    sqlite_config: CoreConfig,
    count: int,
) -> None:
    context = RepositoryContext.from_config(sqlite_config)
    expected = [(7, number, number * 2) for number in range(1, count + 1)]
    expected += [(8, 1, 999)]
    try:
        with context.SQLConnector() as connector:
            with connector.transaction():
                for table in ("source_rows", "copied_rows"):
                    connector.execute(
                        f"CREATE TABLE {table} (gallery_id INTEGER, observation_id INTEGER, stamp INTEGER NOT NULL, PRIMARY KEY (gallery_id, observation_id))"
                    )
                connector.execute_many(
                    "INSERT INTO source_rows VALUES (%s,%s,%s)", expected
                )
            query, parameters = converter["_keyset_query"](
                "SELECT gallery_id, observation_id, stamp FROM source_rows",
                ("gallery_id", "observation_id", "stamp"),
                ("gallery_id", "observation_id"),
                (7, count - 1),
                128,
            )
            plan = connector.fetch_all("EXPLAIN QUERY PLAN " + query, parameters)
            seeks = [str(row[3]) for row in plan if "source_rows" in str(row[3])]
            assert len(seeks) == 2 and all("SEARCH" in item for item in seeks)
            assert connector.fetch_all(query, parameters) == expected[-2:]
            for _ in range(2):
                events: list[str] = []
                assert converter["_copy_rows"](
                    connector,
                    source_query="SELECT gallery_id, observation_id, stamp FROM source_rows",
                    destination="copied_rows",
                    columns=("gallery_id", "observation_id", "stamp"),
                    key_width=2,
                    checkpoint=events.append,
                ) == len(expected)
                progress = [
                    dict(field.split("=", 1) for field in event.split()[1:])
                    for event in events
                    if event.startswith("copy_progress ")
                ]
                assert len(progress) == (len(expected) + 127) // 128
                assert all(1 <= int(item["page_rows"]) <= 128 for item in progress)
            assert (
                connector.fetch_all(
                    "SELECT * FROM copied_rows ORDER BY gallery_id, observation_id"
                )
                == expected
            )
    finally:
        context.close()


@pytest.mark.mariadb
@pytest.mark.deep
def test_mariadb_conversion_lock_survives_commits_and_ddl(
    converter: dict[str, Any],
    mariadb_config: CoreConfig,
) -> None:
    from h2hdb.schema_epoch import mariadb_schema_epoch_gate_name

    context = RepositoryContext.from_config(mariadb_config)
    gate = mariadb_schema_epoch_gate_name(mariadb_config.database.database)
    try:
        with context.SQLConnector() as owner, context.SQLConnector() as other:
            with converter["_exclusive_conversion"](owner, "mariadb"):
                for step in range(3):
                    owner.execute(
                        f"CREATE TABLE gate_probe_{step} (identity BIGINT PRIMARY KEY)"
                    )
                    owner.commit()
                    assert other.fetch_one("SELECT GET_LOCK(%s, 0)", (gate,)) == (0,)
                    other.commit()
            assert other.fetch_one("SELECT GET_LOCK(%s, 0)", (gate,)) == (1,)
            assert other.fetch_one("SELECT RELEASE_LOCK(%s)", (gate,)) == (1,)
    finally:
        context.close()


def test_composite_page_budget_rejects_prefix_scan_regression(
    converter: dict[str, Any],
) -> None:
    # Fixed before measurement: 20,000 SQLite VM instructions per 128-row
    # two-key page. Two bounded seeks plus sorting at most 256 rows fit; a
    # retained-prefix scan or sorting 20,000 eligible rows does not.
    with sqlite3.connect(":memory:") as connection:
        connection.execute(
            "CREATE TABLE source_rows (gallery_id INTEGER, observation_id INTEGER, stamp INTEGER, PRIMARY KEY(gallery_id, observation_id))"
        )
        connection.executemany(
            "INSERT INTO source_rows VALUES (?,?,?)",
            ((gid, number, number) for gid in (7, 8) for number in range(1, 20_001)),
        )
        steps = 0

        def budget() -> int:
            nonlocal steps
            steps += 100
            return int(steps > 20_000)

        connection.set_progress_handler(budget, 100)
        query, parameters = converter["_keyset_query"](
            "SELECT gallery_id, observation_id, stamp FROM source_rows",
            ("gallery_id", "observation_id", "stamp"),
            ("gallery_id", "observation_id"),
            (7, 19_995),
            128,
        )
        try:
            for _ in range(2):
                steps = 0
                rows = connection.execute(
                    query.replace("%s", "?"), parameters
                ).fetchall()
                assert len(rows) == 128 and rows[0] == (7, 19_996, 19_996)
            steps = 0
            with pytest.raises(sqlite3.OperationalError, match="interrupted"):
                connection.execute(
                    "SELECT gallery_id, observation_id, stamp FROM source_rows WHERE gallery_id > ? OR (gallery_id = ? AND observation_id > ?) ORDER BY gallery_id, observation_id LIMIT 128",
                    (7, 7, 19_995),
                ).fetchall()
        finally:
            connection.set_progress_handler(None, 0)


_RETAINED_WORKFLOW = r"""
import hashlib, json, pathlib, sys
wheel, tests, config_path, output = sys.argv[1:]
sys.path.insert(0, wheel)
sys.path.insert(1, tests)
from h2hdb import load_config, VNextIngestFacade
from h2hdb.repository import RepositoryContext
from h2hdb.vnext_source_metadata import iter_metadata_chunks
from vnext_pipeline import Clock, MemoryLibrary, MemorySource, gallery, initialize_database, run_ingest_turn
config = load_config(config_path)
initialize_database(config)
source = MemorySource((gallery(1322802, locator=("published",), upload_time=1543686780000000),))
library = MemoryLibrary(source)
with VNextIngestFacade(config, clock=Clock()) as facade:
    run_ingest_turn(facade, source=source, library=library)
source.put(gallery(1322803, locator=("pending",), upload_time=1543686540000000))
class StopBeforeMetadata(RuntimeError): pass
def boundary(label):
    if label == "source.commit:METADATA_PAGE": raise StopBeforeMetadata()
try:
    with VNextIngestFacade(config, clock=Clock()) as facade:
        run_ingest_turn(facade, source=source, library=library, boundary=boundary)
except StopBeforeMetadata: pass
else: raise AssertionError("did not retain PREFIX staging")
context = RepositoryContext.from_config(config)
try:
    with context.SQLConnector() as connector:
        observations = connector.fetch_all("SELECT gallery_id, observation_id, upload_time FROM catalog_gallery_observation_metadata ORDER BY gallery_id, observation_id")
        canonical = [(g, o, stamp, hashlib.sha256(b''.join(iter_metadata_chunks(connector,g,o))).hexdigest()) for g,o,stamp in observations]
        staging = connector.fetch_all("SELECT s.staging_id, s.gallery_id, s.observation_id, s.state, p.phase FROM operational_gallery_observation_stagings s JOIN operational_gallery_observation_staging_metadata_parsers p ON p.staging_id=s.staging_id WHERE s.state='OPEN'")
        publications = connector.fetch_all("SELECT o.catalog_occurrence_sha256, u.upload_time FROM catalog_publication_occurrence_identities o JOIN catalog_publication_identities p ON p.publication_key=o.publication_key JOIN catalog_gallery_upload_times u ON u.gid=p.gid ORDER BY o.catalog_occurrence_sha256")
        assert len(canonical)==1 and len(publications)==1 and len(staging)==1 and staging[0][-1]=='PREFIX'
        result = {"canonical":canonical,"staging":staging,"publications":publications}
finally: context.close()
pathlib.Path(output).write_text(json.dumps(result, default=lambda value:value.hex()))
"""


_OPEN_GID_CLEANUP = r"""
import sys
wheel,config_path=sys.argv[1:]
sys.path.insert(0,wheel)
from h2hdb import load_config,VNextDatabaseAdminFacade
from h2hdb.repository import RepositoryContext
from h2hdb.vnext_cleanup_repository import CleanupTargetKind,VNextCleanupRepository
from h2hdb.vnext_maintenance_gate_repository import MaintenanceGateRepository
from h2hdb.vnext_transaction import VNextUnitOfWork
config=load_config(config_path)
admin=VNextDatabaseAdminFacade(config)
try:admin.initialize()
finally:admin.close()
context=RepositoryContext.from_config(config)
try:
    with context.SQLConnector() as connector:
        with connector.transaction():
            connector.execute_many("INSERT INTO catalog_gallery_upload_times VALUES (%s,%s)",[(23,123),(279,124),(24,125)])
        with connector.transaction():
            lease=MaintenanceGateRepository.claim_exclusive(VNextUnitOfWork(connector,backend=config.database.sql_type),now=1,lease_duration=100000)
        with connector.transaction():
            VNextCleanupRepository.begin_cycle(VNextUnitOfWork(connector,backend=config.database.sql_type),gate_lease=lease,target_kind=CleanupTargetKind.GALLERY_UPLOAD_TIME,shard_no=23,cycle_cutoff_at=100,max_rows_per_transaction=1,now=2)
        with connector.transaction():
            MaintenanceGateRepository.release(VNextUnitOfWork(connector,backend=config.database.sql_type),lease,now=3)
finally:context.close()
"""


@pytest.mark.parametrize(
    "fault",
    (
        "source_jobs_retired_before_marker",
        "source_jobs_marker_written",
        "conversion_marked",
        "proof_drift",
    ),
)
def test_old_job_retirement_and_building_are_one_transaction(
    converter: dict[str, Any],
    source_schema: Path,
    sqlite_config: CoreConfig,
    tmp_path: Path,
    fault: str,
) -> None:
    _exercise_cleanup_retirement(
        converter, source_schema, sqlite_config, tmp_path, fault
    )


@pytest.mark.mariadb
@pytest.mark.deep
@pytest.mark.parametrize(
    "fault", ("source_jobs_retired_before_marker", "source_jobs_marker_written")
)
def test_mariadb_old_job_retirement_and_building_are_one_transaction(
    converter: dict[str, Any],
    source_schema: Path,
    mariadb_config: CoreConfig,
    tmp_path: Path,
    fault: str,
) -> None:
    _exercise_cleanup_retirement(
        converter, source_schema, mariadb_config, tmp_path, fault
    )


def _exercise_cleanup_retirement(
    converter: dict[str, Any],
    source_schema: Path,
    config: CoreConfig,
    tmp_path: Path,
    fault: str,
) -> None:
    value = os.environ.get("H2HDB_SCHEMA8_WHEEL")
    if value is None:
        pytest.skip("Manual cleanup conversion requires an explicit Core 0.42.2 wheel")
    wheel = Path(value).resolve()
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    converter["_verify_source_wheel"](wheel, digest)
    config_path = tmp_path / "config.json"
    config_path.write_text(config.model_dump_json())
    config_path.chmod(0o600)
    subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-c",
            _OPEN_GID_CLEANUP,
            str(wheel),
            str(config_path),
        ],
        check=True,
        capture_output=True,
        timeout=60,
    )

    def stop(event: str) -> None:
        if fault == "proof_drift" and event.startswith("source_cleanup_completed "):
            context = RepositoryContext.from_config(config)
            try:
                with context.SQLConnector() as connector, connector.transaction():
                    connector.execute(
                        "UPDATE operational_cleanup_jobs SET cycle_cutoff_at=cycle_cutoff_at+1"
                    )
            finally:
                context.close()
        elif event == fault:
            raise RuntimeError("stop at old-job admission boundary")

    arguments = {
        "source_schema": source_schema,
        "source_wheel": wheel,
        "source_wheel_sha256": digest,
        "config_path": config_path,
    }
    error_type = (
        converter["SchemaEpochAdmissionError"]
        if fault == "proof_drift"
        else RuntimeError
    )
    with pytest.raises(error_type):
        converter["upgrade"](config, **arguments, progress=stop)
    context = RepositoryContext.from_config(config)
    try:
        with context.SQLConnector() as connector:
            assert connector.fetch_one(
                "SELECT schema_version,state FROM h2hdb_schema_epoch"
            ) == ((9, "BUILDING") if fault == "conversion_marked" else (8, "READY"))
            assert connector.fetch_all(
                "SELECT state FROM operational_cleanup_jobs"
            ) == ([] if fault == "conversion_marked" else [("COMPLETE",)])
            assert connector.fetch_all(
                "SELECT gid FROM catalog_gallery_upload_times ORDER BY gid"
            ) == [(24,), (279,)]
    finally:
        context.close()
    assert (
        converter["upgrade"](config, **arguments, progress=lambda _event: None)
        == "converted"
    )
    context = RepositoryContext.from_config(config)
    try:
        with context.SQLConnector() as connector:
            assert connector.fetch_all(
                "SELECT gid FROM catalog_gallery_gid_identities ORDER BY gid"
            ) == [(24,), (279,)]
            assert (
                connector.fetch_all("SELECT state FROM operational_cleanup_jobs") == []
            )
    finally:
        context.close()


def _exercise_retained_public_workflow(
    config: CoreConfig,
    converter: dict[str, Any],
    source_schema: Path,
    tmp_path: Path,
) -> None:
    from h2hdb.vnext_source_metadata import iter_metadata_chunks

    value = os.environ.get("H2HDB_SCHEMA8_WHEEL")
    if value is None:
        pytest.skip("Manual retained workflow requires an explicit Core 0.42.2 wheel")
    wheel = Path(value).resolve()
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    converter["_verify_source_wheel"](wheel, digest)
    config_path, result_path = tmp_path / "config.json", tmp_path / "retained.json"
    config_path.write_text(config.model_dump_json())
    config_path.chmod(0o600)
    subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-c",
            _RETAINED_WORKFLOW,
            str(wheel),
            str(_ROOT / "tests"),
            str(config_path),
            str(result_path),
        ],
        check=True,
        timeout=120,
        capture_output=True,
    )
    expected = json.loads(result_path.read_text())
    assert (
        converter["upgrade"](
            config,
            source_schema=source_schema,
            source_wheel=wheel,
            source_wheel_sha256=digest,
            config_path=config_path,
            progress=lambda _event: None,
        )
        == "converted"
    )
    context = RepositoryContext.from_config(config)
    try:
        with context.SQLConnector() as connector:
            actual = connector.fetch_all(
                "SELECT gallery_id, observation_id, upload_time FROM catalog_gallery_observation_upload_times ORDER BY gallery_id, observation_id"
            )
            assert actual == [tuple(row[:3]) for row in expected["canonical"]]
            for gallery_id, observation_id, _stamp, digest in expected["canonical"]:
                assert (
                    hashlib.sha256(
                        b"".join(
                            iter_metadata_chunks(connector, gallery_id, observation_id)
                        )
                    ).hexdigest()
                    == digest
                )
            staging = connector.fetch_all(
                "SELECT s.staging_id, s.gallery_id, s.observation_id, s.state, p.phase FROM operational_gallery_observation_stagings s JOIN operational_gallery_observation_staging_metadata_parsers p ON p.staging_id=s.staging_id WHERE s.state='OPEN'"
            )
            assert staging == [
                (bytes.fromhex(row[0]), *row[1:]) for row in expected["staging"]
            ]
            assert connector.fetch_all(
                "SELECT catalog_occurrence_sha256, upload_time FROM catalog_publication_upload_times ORDER BY catalog_occurrence_sha256"
            ) == [(bytes.fromhex(row[0]), row[1]) for row in expected["publications"]]
    finally:
        context.close()


@pytest.mark.deep
def test_real_schema8_publication_and_prefix_survive_conversion(
    converter: dict[str, Any],
    source_schema: Path,
    sqlite_config: CoreConfig,
    tmp_path: Path,
) -> None:
    _exercise_retained_public_workflow(
        sqlite_config, converter, source_schema, tmp_path
    )


@pytest.mark.deep
@pytest.mark.mariadb
def test_mariadb_real_schema8_publication_and_prefix_survive_conversion(
    converter: dict[str, Any],
    source_schema: Path,
    mariadb_config: CoreConfig,
    tmp_path: Path,
) -> None:
    _exercise_retained_public_workflow(
        mariadb_config, converter, source_schema, tmp_path
    )
