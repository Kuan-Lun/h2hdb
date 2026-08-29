from __future__ import annotations

import ast
import hashlib
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest

import h2hdb.operational_refinement as operational_runtime
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.operational_refinement import (
    OPERATIONAL_RUNTIME_WRITER_BLOCKERS,
    OperationalSemanticRegistryError,
    OperationalSemanticValidationError,
    _manifest_sha256,
    builtin_operational_semantic_validators,
    check_attempt_identity_contract_v1,
    check_bootstrap_contract_v1,
    check_bounded_work_contract_v1,
    check_download_ingest_handoff_contract_v1,
    check_fencing_contract_v1,
    check_gallery_staging_contract_v1,
    check_maintenance_gate_contract_v1,
    check_physical_domains_v1,
    check_revision_allocator_contract_v1,
    validate_builtin_operational_manifest,
)
from h2hdb.schema_epoch import SQLiteSchemaEpochCatalog
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector

ARTIFACT_DATA = cast(dict[str, Any], ARTIFACT)


@pytest.fixture
def greenfield(tmp_path: Path) -> Iterator[SQLiteConnector]:
    connector = SQLiteConnector(str(tmp_path / "operational-refinement.sqlite3"))
    connector.connect()
    SQLiteSchemaEpochCatalog().create_control_table(connector)
    connector.execute(
        """
        INSERT INTO h2hdb_schema_epoch
            (singleton_id, epoch, schema_version, state, manifest_sha256,
             started_at, ready_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            1,
            ARTIFACT_DATA["epoch"],
            ARTIFACT_DATA["schema_version"],
            "BUILDING",
            _manifest_sha256("sqlite"),
            0,
            None,
        ),
    )
    payload = ARTIFACT_DATA["backends"]["sqlite"]
    for _slice_id, statements in payload["slices"]:
        for _statement_id, _kind, _name, sql in statements:
            connector.execute(sql)
    for seed in payload["bootstrap_seeds"]:
        connector.execute(seed["sql"], seed["parameters"])
    try:
        yield connector
    finally:
        connector.close()


def _disable_integrity(connector: SQLiteConnector) -> None:
    connector.connection.execute("PRAGMA foreign_keys = OFF")
    connector.connection.execute("PRAGMA ignore_check_constraints = ON")


def _cleanup_cycle_id(target_kind: str, shard_no: int, generation: int) -> bytes:
    tag = hashlib.sha256(
        b"h2hdb-cleanup-cycle-v1\0" + target_kind.encode("ascii")
    ).digest()[:7]
    return tag + bytes((shard_no,)) + generation.to_bytes(8, "big")


def _insert_maintenance_head(
    connector: SQLiteConnector, *, mode: str, owner: bytes
) -> None:
    connector.connection.execute(
        """
        INSERT INTO operational_maintenance_gate_generations
            (gate_generation, mode, created_at)
        VALUES (?, ?, ?)
        """,
        (1, mode, 0),
    )
    connector.connection.execute(
        """
        INSERT INTO operational_maintenance_gate_heads
            (singleton_id, gate_generation, updated_at)
        VALUES (?, ?, ?)
        """,
        (1, 1, 0),
    )
    connector.connection.execute(
        """
        INSERT INTO operational_maintenance_gate_owners
            (owner_token, gate_generation, lease_expires_at)
        VALUES (?, ?, ?)
        """,
        (owner, 1, 10),
    )


def _insert_ready_ingest_head(connector: SQLiteConnector) -> None:
    connector.connection.execute(
        """
        INSERT INTO operational_ingest_generations
            (generation, started_at, completed_at)
        VALUES (?, ?, ?)
        """,
        (0, 0, 0),
    )
    connector.connection.execute(
        """
        INSERT INTO operational_ingest_coordination_heads
            (singleton_id, current_generation, completed_generation,
             phase, last_transition_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (1, 0, 0, "READY", 0),
    )


def test_registry_is_closed_world_and_does_not_import_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validate_builtin_operational_manifest()
    validators = builtin_operational_semantic_validators()
    expected = tuple(
        value["id"]
        for value in ARTIFACT_DATA["semantic_obligations"]
        if value["source"] == "operational"
        and value["contract"]["lifecycle"] != "building_only"
    )
    assert tuple(validators) == expected
    assert "h2hdb.operational.bootstrap-genesis.v1" not in validators

    root = Path(__file__).resolve().parents[1]
    source = root / "src" / "h2hdb" / "operational_refinement.py"
    tree = ast.parse(source.read_text())
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imports.update(
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    )
    assert not any(
        value == "verification" or value.startswith("verification.")
        for value in imports
    )

    obligations = list(ARTIFACT_DATA["semantic_obligations"])
    index = next(
        position
        for position, value in enumerate(obligations)
        if value["id"] == "h2hdb.operational.fencing.v1"
    )
    obligation = obligations[index]
    obligations[index] = {
        **obligation,
        "contract": {**obligation["contract"], "lifecycle": "ready_validation"},
    }
    monkeypatch.setitem(ARTIFACT, "semantic_obligations", tuple(obligations))
    with pytest.raises(OperationalSemanticRegistryError, match="wheel registry"):
        validate_builtin_operational_manifest()


def test_generated_greenfield_and_bootstrap_pass_every_read_validator(
    greenfield: SQLiteConnector,
) -> None:
    for validator in builtin_operational_semantic_validators().values():
        validator(greenfield)
    check_bootstrap_contract_v1(greenfield)


@pytest.mark.parametrize("orphan", ("seal", "stream"))
def test_full_check_rejects_operational_effect_roots_without_live_owner(
    greenfield: SQLiteConnector,
    orphan: str,
) -> None:
    _disable_integrity(greenfield)
    preparation_id = b"o" * 16
    if orphan == "seal":
        greenfield.connection.execute(
            "INSERT INTO operational_operational_preparation_effect_seals "
            "(preparation_id, event_count, final_chain_sha256, sealed_at) "
            "VALUES (?, 0, ?, 1)",
            (preparation_id, b"e" * 32),
        )
        message = "effect seal lacks preparation or commit authority"
    else:
        greenfield.connection.execute(
            "INSERT INTO operational_operational_event_streams "
            "(preparation_id, created_at) VALUES (?, 1)",
            (preparation_id,),
        )
        message = "event stream lacks preparation or seal authority"

    with pytest.raises(OperationalSemanticValidationError, match=message):
        operational_runtime.check_event_integrity_contract_v1(greenfield)


def test_runtime_blockers_name_every_delegated_high_cardinality_duty() -> None:
    assert set(OPERATIONAL_RUNTIME_WRITER_BLOCKERS) == set(
        builtin_operational_semantic_validators()
    ) - {"h2hdb.operational.epoch-manifest.v1"}
    cache = OPERATIONAL_RUNTIME_WRITER_BLOCKERS[
        "h2hdb.operational.canonical-hash-cache.v1"
    ]
    assert all(word in cache for word in ("framed", "SHA-256", "byte_count"))
    gallery = OPERATIONAL_RUNTIME_WRITER_BLOCKERS[
        "h2hdb.operational.gallery-staging.v1"
    ]
    assert all(word in gallery for word in ("subtype", "metadata", "membership"))
    cleanup = OPERATIONAL_RUNTIME_WRITER_BLOCKERS[
        "h2hdb.operational.cleanup-reachability.v1"
    ]
    assert "retention roots" in cleanup and "child-first" in cleanup
    queue = OPERATIONAL_RUNTIME_WRITER_BLOCKERS["h2hdb.operational.queue-history.v1"]
    assert "history" in queue


def test_static_physical_check_does_not_claim_to_rescan_corpus(
    greenfield: SQLiteConnector,
) -> None:
    _disable_integrity(greenfield)
    greenfield.connection.execute(
        """
        INSERT INTO operational_download_requests
            (gid, url, request_token, requested_at)
        VALUES (?, ?, ?, ?)
        """,
        (1, "https://example.invalid", b"short", 0),
    )
    check_physical_domains_v1(greenfield)
    with pytest.raises(OperationalSemanticValidationError, match="not empty"):
        check_bootstrap_contract_v1(greenfield)


def test_fencing_checks_only_exact_current_projection(
    greenfield: SQLiteConnector,
) -> None:
    greenfield.connection.executemany(
        """
        INSERT INTO operational_ingest_generations
            (generation, started_at, completed_at)
        VALUES (?, ?, ?)
        """,
        ((0, 0, 0), (1, 1, None)),
    )
    greenfield.connection.execute(
        """
        INSERT INTO operational_ingest_coordination_heads
            (singleton_id, current_generation, completed_generation,
             phase, last_transition_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (1, 1, 0, "DOWNLOADING", 1),
    )
    with pytest.raises(OperationalSemanticValidationError, match="exact owner"):
        check_fencing_contract_v1(greenfield)

    owner = b"ingest-owner-001"
    assert len(owner) == 16
    greenfield.connection.execute(
        """
        INSERT INTO operational_ingest_generation_owners
            (generation, owner_token, claimed_at, lease_expires_at)
        VALUES (?, ?, ?, ?)
        """,
        (1, owner, 1, 10),
    )
    check_fencing_contract_v1(greenfield)


def test_download_ingest_ready_projection_requires_exact_completion_chain(
    greenfield: SQLiteConnector,
) -> None:
    greenfield.connection.executemany(
        """
        INSERT INTO operational_download_generations
            (generation, started_at, completed_at)
        VALUES (?, ?, ?)
        """,
        ((0, 0, 0), (1, 1, 30)),
    )
    greenfield.connection.execute(
        """
        INSERT INTO operational_download_coordination_heads
            (singleton_id, current_generation, completed_generation,
             last_transition_at)
        VALUES (?, ?, ?, ?)
        """,
        (1, 1, 1, 30),
    )
    greenfield.connection.execute(
        """
        INSERT INTO operational_ingest_generations
            (generation, started_at, completed_at)
        VALUES (?, ?, ?)
        """,
        (1, 2, 30),
    )
    greenfield.connection.execute(
        """
        INSERT INTO operational_download_ingest_handoffs
            (download_generation, owner_token, handoff_kind, requested_at)
        VALUES (?, ?, ?, ?)
        """,
        (1, b"d" * 16, "DOWNLOADER", 2),
    )
    greenfield.connection.execute(
        """
        INSERT INTO operational_download_ingest_consumptions
            (download_generation, ingest_generation, consumed_at)
        VALUES (?, ?, ?)
        """,
        (1, 1, 3),
    )
    greenfield.connection.execute(
        """
        INSERT INTO operational_coordinated_ingest_completions
            (ingest_generation, owner_token, completed_at)
        VALUES (?, ?, ?)
        """,
        (1, b"i" * 16, 30),
    )
    check_download_ingest_handoff_contract_v1(greenfield)

    greenfield.connection.execute(
        "UPDATE operational_download_generations SET completed_at = 31 "
        "WHERE generation = 1"
    )
    with pytest.raises(OperationalSemanticValidationError, match="timestamps disagree"):
        check_download_ingest_handoff_contract_v1(greenfield)


def test_shared_owner_requires_exactly_one_slot(
    greenfield: SQLiteConnector,
) -> None:
    owner = b"shared-owner-001"
    assert len(owner) == 16
    _insert_maintenance_head(greenfield, mode="SHARED", owner=owner)
    with pytest.raises(OperationalSemanticValidationError, match="exactly one slot"):
        check_maintenance_gate_contract_v1(greenfield)

    greenfield.connection.execute(
        """
        INSERT INTO operational_maintenance_gate_holders (owner_token, slot)
        VALUES (?, ?)
        """,
        (owner, 7),
    )
    check_maintenance_gate_contract_v1(greenfield)

    greenfield.connection.execute(
        """
        INSERT INTO operational_maintenance_gate_holders (owner_token, slot)
        VALUES (?, ?)
        """,
        (owner, 8),
    )
    with pytest.raises(OperationalSemanticValidationError, match="exactly one slot"):
        check_maintenance_gate_contract_v1(greenfield)


def test_exclusive_owner_requires_every_slot_zero_through_63(
    greenfield: SQLiteConnector,
) -> None:
    owner = b"exclusive-owner1"
    assert len(owner) == 16
    _insert_maintenance_head(greenfield, mode="EXCLUSIVE", owner=owner)
    greenfield.connection.execute(
        """
        INSERT INTO operational_maintenance_gate_holders (owner_token, slot)
        VALUES (?, ?)
        """,
        (owner, 0),
    )
    with pytest.raises(OperationalSemanticValidationError, match="slots 0..63"):
        check_maintenance_gate_contract_v1(greenfield)

    greenfield.connection.executemany(
        """
        INSERT INTO operational_maintenance_gate_holders (owner_token, slot)
        VALUES (?, ?)
        """,
        ((owner, slot) for slot in range(1, 64)),
    )
    check_maintenance_gate_contract_v1(greenfield)


def _insert_open_cleanup_job(connector: SQLiteConnector) -> tuple[bytes, str]:
    target = connector.connection.execute("""
        SELECT target_kind, shard_no, target_key
        FROM operational_cleanup_sweep_targets
        WHERE target_kind = 'SOURCE_BUILD' AND shard_no = 0
        """).fetchone()
    assert target is not None
    target_kind, shard_no, target_key = target
    cleanup_id = _cleanup_cycle_id(target_kind, shard_no, 1)
    connector.connection.execute(
        """
        INSERT INTO operational_cleanup_jobs
            (cleanup_id, target_key, cycle_generation, cycle_cutoff_at,
             algorithm_version, max_rows_per_transaction,
             hash_cache_max_age_microseconds, frozen_root_count,
             frozen_root_set_sha256, state, created_at, completed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            cleanup_id,
            target_key,
            1,
            0,
            2,
            256,
            0,
            0,
            operational_runtime._cleanup_frozen_root_set_sha256(cleanup_id, ()),
            "OPEN",
            0,
            None,
        ),
    )
    phase = connector.connection.execute("""
        SELECT phase
        FROM operational_cleanup_phases
        WHERE target_kind = 'SOURCE_BUILD' AND phase_order = 1
        """).fetchone()
    assert phase is not None
    return cleanup_id, cast(str, phase[0])


def test_cleanup_terminal_state_is_equivalent_to_empty_receipt(
    greenfield: SQLiteConnector,
) -> None:
    cleanup_id, phase = _insert_open_cleanup_job(greenfield)
    prior_chain = b"p" * 32
    input_sha256 = b"i" * 32
    terminal_output = operational_runtime._cleanup_next_chain_sha256(
        prior_chain,
        phase,
        2,
        b"next",
        b"next",
        input_sha256,
        0,
    )
    greenfield.connection.execute(
        """
        INSERT INTO operational_cleanup_checkpoints
            (cleanup_id, phase, generation, cursor_bytes, deleted_count,
             chain_sha256, state, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (cleanup_id, phase, 2, b"next", 0, terminal_output, "OPEN", 0),
    )
    greenfield.connection.execute(
        """
            INSERT INTO operational_cleanup_batch_receipts
                (cleanup_id, phase, batch_key, start_cursor, next_cursor,
                 prior_chain_sha256, prior_deleted_count,
                 input_sha256, output_sha256, row_count,
                 committed_generation, committed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            cleanup_id,
            phase,
            b"k" * 32,
            b"next",
            b"next",
            prior_chain,
            0,
            input_sha256,
            terminal_output,
            0,
            2,
            0,
        ),
    )
    with pytest.raises(OperationalSemanticValidationError, match="equivalence"):
        check_bounded_work_contract_v1(greenfield)

    nonterminal_output = operational_runtime._cleanup_next_chain_sha256(
        prior_chain,
        phase,
        2,
        b"",
        b"next",
        input_sha256,
        1,
    )
    greenfield.connection.execute(
        "UPDATE operational_cleanup_batch_receipts "
        "SET row_count = 1, start_cursor = x'', output_sha256 = ?",
        (nonterminal_output,),
    )
    greenfield.connection.execute(
        "UPDATE operational_cleanup_checkpoints "
        "SET deleted_count = 1, chain_sha256 = ?",
        (nonterminal_output,),
    )
    check_bounded_work_contract_v1(greenfield)

    greenfield.connection.execute(
        "UPDATE operational_cleanup_checkpoints SET state = 'COMPLETE'"
    )
    with pytest.raises(OperationalSemanticValidationError, match="equivalence"):
        check_bounded_work_contract_v1(greenfield)
    next_terminal_output = operational_runtime._cleanup_next_chain_sha256(
        nonterminal_output,
        phase,
        2,
        b"next",
        b"next",
        input_sha256,
        0,
    )
    greenfield.connection.execute(
        "UPDATE operational_cleanup_batch_receipts SET row_count = 0, "
        "start_cursor = next_cursor, prior_chain_sha256 = ?, "
        "prior_deleted_count = 1, output_sha256 = ?",
        (nonterminal_output, next_terminal_output),
    )
    greenfield.connection.execute(
        "UPDATE operational_cleanup_checkpoints SET chain_sha256 = ?",
        (next_terminal_output,),
    )
    check_bounded_work_contract_v1(greenfield)


def test_generated_cleanup_layout_requires_state_to_root_phase_chains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = ARTIFACT_DATA["backends"]["sqlite"]["bootstrap_seeded_relations"]
    phase_record = cast(
        dict[str, Any],
        next(value for value in records if value["relation"] == "cleanup_phase"),
    )
    expected_rows = cast(tuple[tuple[object, ...], ...], phase_record["expected_rows"])
    drifted = tuple(
        ("AR_STATE_BYPASS", target_kind, order)
        if phase == "AR_STATE"
        else (phase, target_kind, order)
        for phase, target_kind, order in expected_rows
    )
    monkeypatch.setitem(phase_record, "expected_rows", drifted)
    with pytest.raises(OperationalSemanticRegistryError, match="phase chain drifts"):
        operational_runtime._cleanup_layout("sqlite")


def test_cleanup_cycle_codec_corruption_fails_closed(
    greenfield: SQLiteConnector,
) -> None:
    target = greenfield.connection.execute("""
        SELECT target_key
        FROM operational_cleanup_sweep_targets
        WHERE target_kind = 'SOURCE_BUILD' AND shard_no = 0
        """).fetchone()
    assert target is not None
    greenfield.connection.execute(
        """
        INSERT INTO operational_cleanup_jobs
            (cleanup_id, target_key, cycle_generation, cycle_cutoff_at,
             algorithm_version, max_rows_per_transaction,
             hash_cache_max_age_microseconds, frozen_root_count,
             frozen_root_set_sha256, state, created_at, completed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            b"x" * 16,
            target[0],
            1,
            0,
            2,
            256,
            0,
            0,
            operational_runtime._cleanup_frozen_root_set_sha256(b"x" * 16, ()),
            "OPEN",
            0,
            None,
        ),
    )
    with pytest.raises(OperationalSemanticValidationError, match="cleanup_id"):
        check_attempt_identity_contract_v1(greenfield)


def test_fixed_allocator_registry_corruption_fails_closed(
    greenfield: SQLiteConnector,
) -> None:
    greenfield.connection.execute(
        "DELETE FROM operational_revision_allocators WHERE stream = 'SOURCE'"
    )
    with pytest.raises(OperationalSemanticValidationError, match="cardinality"):
        check_revision_allocator_contract_v1(greenfield)

    greenfield.connection.execute(
        "DELETE FROM operational_identity_allocators WHERE stream = 'TAG'"
    )
    with pytest.raises(OperationalSemanticValidationError, match="cardinality"):
        check_gallery_staging_contract_v1(greenfield)


class _RecordingConnector(SQLConnector):
    def __init__(self, delegate: SQLiteConnector) -> None:
        self.delegate = delegate
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    def connect(self) -> None:
        raise AssertionError("recording connector is already connected")

    def close(self) -> None:
        pass

    def check_table_exists(self, table_name: str) -> bool:
        return self.delegate.check_table_exists(table_name)

    def commit(self) -> None:
        self.delegate.commit()

    def begin(self) -> None:
        self.delegate.begin()

    def rollback(self) -> None:
        self.delegate.rollback()

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        raise AssertionError("READY validation must not mutate")

    def execute_many(self, query: str, data: list[tuple[Any, ...]]) -> None:
        raise AssertionError("READY validation must not mutate")

    def fetch_one(self, query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
        self.queries.append((query, data))
        return self.delegate.fetch_one(query, data)

    def fetch_all(
        self, query: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        self.queries.append((query, data))
        return self.delegate.fetch_all(query, data)


def _table_name(relation_name: str) -> str:
    relations = ARTIFACT_DATA["backends"]["sqlite"]["relations"]
    return cast(
        str,
        next(
            relation["table"]
            for relation in relations
            if relation["relation"] == relation_name
        ),
    )


def test_full_check_query_budget_limits_transition_and_owner_audits(
    greenfield: SQLiteConnector,
) -> None:
    _insert_ready_ingest_head(greenfield)
    owner = b"shared-owner-001"
    _insert_maintenance_head(greenfield, mode="SHARED", owner=owner)
    greenfield.connection.execute(
        """
        INSERT INTO operational_maintenance_gate_holders (owner_token, slot)
        VALUES (?, ?)
        """,
        (owner, 0),
    )
    greenfield.connection.executemany(
        """
        INSERT INTO operational_download_requests
            (gid, url, request_token, requested_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            (gid, f"https://example.invalid/{gid}", gid.to_bytes(16, "big"), 0)
            for gid in range(1, 1001)
        ),
    )

    recording = _RecordingConnector(greenfield)
    for validator in builtin_operational_semantic_validators().values():
        validator(recording)

    allowed_relations = {
        "ingest_coordination_head",
        "ingest_generation",
        "ingest_generation_owner",
        "ingest_generation_handoff",
        "download_coordination_head",
        "download_generation",
        "download_generation_owner",
        "download_ingest_handoff",
        "download_ingest_consumption",
        "coordinated_ingest_completion",
        "maintenance_gate_head",
        "maintenance_gate_generation",
        "maintenance_gate_owner",
        "maintenance_gate_holder",
        "cleanup_target_kind",
        "cleanup_sweep_target",
        "cleanup_phase",
        "cleanup_job",
        "cleanup_cycle_root",
        "cleanup_completion",
        "cleanup_checkpoint",
        "cleanup_batch_receipt",
        "revision_allocator",
        "identity_allocator",
        "gallery_observation_staging_request_budget",
        "gallery_observation_staging_request",
        "publication_commit",
        "operational_preparation",
        "operational_preparation_effect_seal",
        "operational_event_stream",
    }
    allowed_tables = {_table_name(name) for name in allowed_relations}
    allowed_tables.add("h2hdb_schema_epoch")
    cleanup_cycle_root_table = _table_name("cleanup_cycle_root")
    assert cleanup_cycle_root_table == "operational_cleanup_cycle_roots"
    fixed_scan_tables = {
        _table_name(name)
        for name in {
            "maintenance_gate_holder",
            "cleanup_target_kind",
            "cleanup_sweep_target",
            "cleanup_phase",
            "cleanup_job",
            "cleanup_cycle_root",
            "cleanup_completion",
            "cleanup_checkpoint",
            "cleanup_batch_receipt",
            "revision_allocator",
            "identity_allocator",
            "gallery_observation_staging_request_budget",
        }
    }
    capped_request_table = _table_name("gallery_observation_staging_request")
    owner_audit_tables = {
        _table_name(name)
        for name in {
            "publication_commit",
            "operational_preparation",
            "operational_preparation_effect_seal",
            "operational_event_stream",
        }
    }
    relation_pattern = re.compile(r"\b(?:FROM|JOIN)\s+([a-z0-9_]+)", re.I)
    queried_tables: set[str] = set()
    assert len(recording.queries) < 100
    for query, data in recording.queries:
        normalized = " ".join(query.split())
        tables = {match.lower() for match in relation_pattern.findall(query)}
        queried_tables.update(tables)
        assert tables <= allowed_tables
        if tables == {capped_request_table}:
            assert normalized == f"SELECT COUNT(*) FROM {capped_request_table}"
        else:
            assert " LIMIT " in f" {normalized.upper()} "

        plan = greenfield.connection.execute(
            "EXPLAIN QUERY PLAN " + query.replace("%s", "?"), data
        ).fetchall()
        if any("SCAN" in cast(str, row[3]).upper() for row in plan):
            assert (
                tables <= fixed_scan_tables
                or tables == {capped_request_table}
                or tables <= owner_audit_tables
            )

    assert _table_name("download_request") not in queried_tables
    assert _table_name("source_revision") not in queried_tables
    assert _table_name("gallery_observation_staging") not in queried_tables
    cycle_root_queries = [
        " ".join(query.split()).upper()
        for query, _data in recording.queries
        if cleanup_cycle_root_table in query.lower()
    ]
    assert cycle_root_queries
    assert all(" LIMIT 257" in query for query in cycle_root_queries)
    assert capped_request_table in queried_tables
