from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

import h2hdb.vnext_cleanup_repository as cleanup_module
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_cleanup_repository import (
    CleanupBatchCommand,
    CleanupBatchResult,
    CleanupCorruptionError,
    CleanupCycle,
    CleanupRetentionBlockedError,
    CleanupTargetKind,
    CleanupUnavailableError,
    VNextCleanupRepository,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_transaction import LockRank, VNextUnitOfWork


def _database(path: Path) -> SQLiteConnector:
    connector = SQLiteConnector(str(path))
    connector.connect()
    payload: Any = ARTIFACT["backends"]
    payload = payload["sqlite"]
    for _slice_id, statements in payload["slices"]:
        for _statement_id, _kind, _name, sql in statements:
            connector.execute(sql)
    for seed in payload["bootstrap_seeds"]:
        connector.execute(seed["sql"], seed["parameters"])
    return connector


def _exclusive(connector: SQLiteConnector, *, token: bytes = b"x" * 16) -> GateLease:
    with (
        connector.transaction(),
        patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=token,
        ),
    ):
        return MaintenanceGateRepository.claim_exclusive(
            VNextUnitOfWork(connector, backend="sqlite"),
            now=1,
            lease_duration=100_000,
        )


def _begin(
    connector: SQLiteConnector,
    gate: GateLease,
    kind: CleanupTargetKind,
    shard: int,
    *,
    max_rows: int = 1,
) -> CleanupCycle:
    with connector.transaction():
        return VNextCleanupRepository.begin_cycle(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            target_kind=kind,
            shard_no=shard,
            cycle_cutoff_at=100,
            max_rows_per_transaction=max_rows,
            now=2,
        )


def _advance(
    connector: SQLiteConnector,
    gate: GateLease,
    cycle: CleanupCycle,
    generation: int,
    batch_key: bytes,
    *,
    now: int,
) -> CleanupBatchResult:
    with connector.transaction():
        return VNextCleanupRepository.advance(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            cycle=cycle,
            command=CleanupBatchCommand(batch_key, generation),
            now=now,
        )


def _drain(
    connector: SQLiteConnector,
    gate: GateLease,
    cycle: CleanupCycle,
    *,
    now: int = 3,
) -> list[CleanupBatchResult]:
    generation = 1
    results: list[CleanupBatchResult] = []
    for attempt in range(512):
        result = _advance(
            connector,
            gate,
            cycle,
            generation,
            attempt.to_bytes(32, "big"),
            now=now + attempt,
        )
        results.append(result)
        if result.cycle_complete:
            return results
        assert result.generation is not None
        generation = result.generation
    raise AssertionError("cleanup cycle did not terminate within its fixed phases")


def _fixture_rows(
    connector: SQLiteConnector,
    statements: list[tuple[str, tuple[object, ...]]],
) -> None:
    """Install isolated cleanup fixtures without fabricating all parent planes."""

    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        for sql, parameters in statements:
            connector.execute(sql, parameters)
    finally:
        connector.execute("PRAGMA foreign_keys = ON")


def test_content_blob_sweep_is_bounded_replayable_and_reusable(tmp_path: Path) -> None:
    connector = _database(tmp_path / "content-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)
        first_key = bytes((5,)) + b"a" * 31
        second_key = bytes((5,)) + b"b" * 31
        outside_shard = bytes((6,)) + b"c" * 31
        connector.execute_many(
            "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
            "VALUES (%s, %s)",
            [(first_key, 1), (second_key, 2), (outside_shard, 3)],
        )

        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CONTENT_BLOB,
            5,
            max_rows=1,
        )
        page_one = _advance(connector, gate, cycle, 1, b"a" * 32, now=3)
        assert (page_one.row_count, page_one.generation, page_one.cursor) == (
            1,
            2,
            first_key,
        )
        assert not page_one.phase_complete
        before = connector.fetch_all(
            "SELECT file_sha256 FROM catalog_content_blobs ORDER BY file_sha256"
        )
        replay = _advance(connector, gate, cycle, 1, b"a" * 32, now=4)
        assert replay.replayed
        assert (
            connector.fetch_all(
                "SELECT file_sha256 FROM catalog_content_blobs ORDER BY file_sha256"
            )
            == before
        )

        page_two = _advance(connector, gate, cycle, 2, b"b" * 32, now=5)
        assert (page_two.row_count, page_two.generation, page_two.cursor) == (
            1,
            3,
            second_key,
        )
        completed = _advance(connector, gate, cycle, 3, b"c" * 32, now=6)
        assert completed.cycle_complete
        assert completed.deleted_count == 2
        assert connector.fetch_all(
            "SELECT file_sha256 FROM catalog_content_blobs ORDER BY file_sha256"
        ) == [(outside_shard,)]
        with connector.transaction():
            resumed = VNextCleanupRepository.resume_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                cycle=cycle,
                now=7,
            )
        assert resumed.cycle_complete and resumed.replayed
        assert resumed.deleted_count == 2

        next_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CONTENT_BLOB,
            5,
            max_rows=1,
        )
        assert next_cycle.cycle_generation == 2
        assert next_cycle.cleanup_id != cycle.cleanup_id
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_cleanup_completions WHERE target_key = %s",
                (cycle.target_key,),
            )
            == ()
        )
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("kind", "table", "insert_sql", "values", "key"),
    (
        (
            CleanupTargetKind.FILE_NAME_IDENTITY,
            "catalog_file_name_identities",
            "INSERT INTO catalog_file_name_identities "
            "(file_key, name_bytes, file_role) VALUES (%s, %s, %s)",
            (bytes((9,)) + b"n" * 31, b"001.jpg", b"CONTENT"),
            bytes((9,)) + b"n" * 31,
        ),
        (
            CleanupTargetKind.PUBLICATION_IDENTITY,
            "catalog_publication_identities",
            "INSERT INTO catalog_publication_identities "
            "(publication_key, publication_id, gid, artifact_name) "
            "VALUES (%s, %s, %s, %s)",
            (
                bytes((9,)) + b"p" * 31,
                b"urn:h2h:gallery:9",
                9,
                b"h2h-9.cbz",
            ),
            bytes((9,)) + b"p" * 31,
        ),
    ),
)
def test_leaf_identity_strategies_delete_only_their_fixed_shard(
    tmp_path: Path,
    kind: CleanupTargetKind,
    table: str,
    insert_sql: str,
    values: tuple[object, ...],
    key: bytes,
) -> None:
    connector = _database(tmp_path / f"{kind.value}.sqlite3")
    try:
        gate = _exclusive(connector)
        connector.execute(insert_sql, values)
        cycle = _begin(connector, gate, kind, 9, max_rows=8)
        first = _advance(connector, gate, cycle, 1, b"d" * 32, now=3)
        assert first.row_count == 1 and first.cursor == key
        completed = _advance(connector, gate, cycle, 2, b"e" * 32, now=4)
        assert completed.cycle_complete and completed.deleted_count == 1
        assert connector.fetch_all(f"SELECT * FROM {table}") == []
    finally:
        connector.close()


def test_cleanup_fails_closed_for_shared_gate_and_registry_drift(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "cleanup-fail-closed.sqlite3")
    try:
        with (
            connector.transaction(),
            patch(
                "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
                return_value=b"s" * 16,
            ),
        ):
            shared = MaintenanceGateRepository.claim_shared(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=1,
                lease_duration=100_000,
            )
        with pytest.raises(CleanupUnavailableError, match="EXCLUSIVE"):
            _begin(
                connector,
                shared,
                CleanupTargetKind.CONTENT_BLOB,
                0,
            )
        assert connector.fetch_all("SELECT 1 FROM operational_cleanup_jobs") == []

        # Let the shared capability expire before acquiring the exclusive gate.
        with (
            connector.transaction(),
            patch(
                "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
                return_value=b"x" * 16,
            ),
        ):
            exclusive = MaintenanceGateRepository.claim_exclusive(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=100_001,
                lease_duration=100_000,
            )
        connector.execute(
            "DELETE FROM operational_cleanup_phases WHERE phase = 'CB_ROOT'"
        )
        with pytest.raises(CleanupCorruptionError, match="phase seed"):
            _begin(
                connector,
                exclusive,
                CleanupTargetKind.CONTENT_BLOB,
                0,
            )
        assert connector.fetch_all("SELECT 1 FROM operational_cleanup_jobs") == []
    finally:
        connector.close()


def test_all_fifteen_static_strategies_match_the_closed_phase_registry(
    tmp_path: Path,
) -> None:
    expected = {
        CleanupTargetKind.SOURCE_BUILD: (
            "SB_CANONICAL_UPLOAD",
            "SB_GALLERY",
            "SB_SATELLITES",
            "SB_GENERATION",
            "SB_ROOT",
        ),
        CleanupTargetKind.ANALYSIS_RUN: (
            "AR_BATCH",
            "AR_SEALS",
            "AR_OVERLAY",
            "AR_EVIDENCE",
            "AR_CHECKPOINT",
            "AR_ANCESTRY",
            "AR_LINKS",
            "AR_ROOT",
        ),
        CleanupTargetKind.PUBLICATION_CANDIDATE: (
            "PC_DELTAS",
            "PC_INPUT",
            "PC_SELECTION",
            "PC_BATCH",
            "PC_CHECKPOINT",
            "PC_BASES",
            "PC_ROOT",
        ),
        CleanupTargetKind.OPERATIONAL_PREPARATION: (
            "OP_BATCH",
            "OP_CHECKPOINT",
            "OP_SUBTYPE",
            "OP_EVENT",
            "OP_SEAL",
            "OP_ROOT",
        ),
        CleanupTargetKind.GALLERY_OBSERVATION: (
            "GO_STAGING_RECEIPT_FRONTIER",
            "GO_STAGING_PAGE_ASSOCIATION",
            "GO_STAGING_REQUEST_DESCRIPTOR",
            "GO_STAGING_REQUEST_IDENTITY",
            "GO_STAGING_CHECKPOINT",
            "GO_STAGING_CLAIM",
            "GO_STAGING_ROOT",
            "GO_FACTS",
            "GO_FILES",
            "GO_DESCRIPTOR",
            "GO_ROOT",
        ),
        CleanupTargetKind.GALLERY_OBSERVATION_STAGING: (
            "GOS_RECEIPT_FRONTIER",
            "GOS_PAGE_ASSOCIATION",
            "GOS_REQUEST_DESCRIPTOR",
            "GOS_REQUEST_IDENTITY",
            "GOS_CHECKPOINT",
            "GOS_CLAIM",
            "GOS_ROOT",
        ),
        CleanupTargetKind.ARTIFACT_BLOB: (
            "AB_LOCATIONS",
            "AB_IDENTITIES",
            "AB_ROOT",
        ),
        CleanupTargetKind.CANONICAL_VALUE: (
            "CV_DICTIONARY",
            "CV_SEMANTIC_LINK",
            "CV_IDENTITY",
            "CV_PARENT_DESCRIPTOR",
            "CV_PAGE",
            "CV_ROOT",
        ),
        CleanupTargetKind.CONTENT_BLOB: ("CB_ROOT",),
        CleanupTargetKind.GALLERY_OBSERVATION_PAGE: (
            "GOP_OUTGOING_CHILD",
            "GOP_BOUNDS",
            "GOP_DESCRIPTOR",
            "GOP_ROOT",
        ),
        CleanupTargetKind.FILE_NAME_IDENTITY: ("FN_ROOT",),
        CleanupTargetKind.PUBLICATION_IDENTITY: ("PI_ROOT",),
        CleanupTargetKind.GALLERY_IDENTITY: (
            "GI_OBSERVATION_ALLOCATOR",
            "GI_ROOT",
        ),
        CleanupTargetKind.CANONICAL_VALUE_UPLOAD: ("CVU_ROOT",),
        CleanupTargetKind.HASH_CACHE_OBSERVATION: ("HC_FILE", "HC_ROOT"),
    }
    assert set(expected) == set(CleanupTargetKind)
    assert {
        kind: strategy.phases for kind, strategy in cleanup_module._STRATEGIES.items()
    } == expected

    connector = _database(tmp_path / "all-empty-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)
        for kind in CleanupTargetKind:
            cycle = _begin(connector, gate, kind, 0, max_rows=8)
            results = _drain(connector, gate, cycle)
            assert results[-1].cycle_complete
            assert len(results) == len(expected[kind])
    finally:
        connector.close()


def test_source_analysis_and_candidate_strategies_delete_child_first(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "rooted-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)

        build_id = bytes((17,)) + b"b" * 15
        upload_value = b"u" * 32
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_ingest_generations "
                    "(generation, started_at, completed_at) VALUES (%s, %s, %s)",
                    (1, 0, 1),
                ),
                (
                    "INSERT INTO catalog_source_builds "
                    "(build_id, scope_key, manifest_policy_id, state, created_at, sealed_at) "
                    "VALUES (%s, %s, %s, 'ABANDONED', %s, NULL)",
                    (build_id, b"s" * 32, 1, 0),
                ),
                (
                    "INSERT INTO operational_source_build_generations "
                    "(build_id, generation) VALUES (%s, %s)",
                    (build_id, 1),
                ),
                (
                    "INSERT INTO operational_canonical_value_uploads "
                    "(generation, value_sha256) VALUES (%s, %s)",
                    (1, upload_value),
                ),
                (
                    "INSERT INTO catalog_source_build_expected_gallery "
                    "(build_id, position, gallery_id) VALUES (%s, %s, %s)",
                    (build_id, 0, 1),
                ),
                (
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
                    (build_id, 1, 1),
                ),
            ],
        )
        source_cycle = _begin(
            connector, gate, CleanupTargetKind.SOURCE_BUILD, 17, max_rows=1
        )
        source_results = _drain(connector, gate, source_cycle)
        assert source_results[-1].deleted_count == 5
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_source_builds WHERE build_id = %s", (build_id,)
            )
            == ()
        )
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_canonical_value_uploads "
                "WHERE generation = 1 AND value_sha256 = %s",
                (upload_value,),
            )
            == ()
        )

        analysis_id = bytes((18,)) + b"a" * 15
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_analysis_runs "
                    "(analysis_id, build_id, policy_id, input_manifest_sha256, "
                    "state, started_at, completed_at) "
                    "VALUES (%s, %s, %s, %s, 'ABANDONED', %s, NULL)",
                    (analysis_id, b"z" * 16, 1, b"m" * 32, 0),
                ),
                (
                    "INSERT INTO catalog_source_revision_provenance "
                    "(source_revision, analysis_id) VALUES (%s, %s)",
                    (9, analysis_id),
                ),
                (
                    "INSERT INTO catalog_analysis_impacted_gid "
                    "(analysis_id, gid) VALUES (%s, %s)",
                    (analysis_id, 7),
                ),
                (
                    "INSERT INTO catalog_analysis_checkpoints "
                    "(analysis_id, stage, generation, cursor, processed_count, "
                    "state, updated_at) VALUES (%s, %s, %s, %s, %s, 'OPEN', %s)",
                    (analysis_id, b"changed_gallery", 1, b"", 0, 0),
                ),
                (
                    "INSERT INTO catalog_analysis_state_ancestry "
                    "(analysis_id, ancestor_depth, ancestor_analysis_id) "
                    "VALUES (%s, %s, %s)",
                    (analysis_id, 0, analysis_id),
                ),
                (
                    "INSERT INTO catalog_analysis_state_anchors "
                    "(analysis_id, anchor_analysis_id, overlay_depth) "
                    "VALUES (%s, %s, %s)",
                    (analysis_id, analysis_id, 0),
                ),
            ],
        )
        analysis_cycle = _begin(
            connector, gate, CleanupTargetKind.ANALYSIS_RUN, 18, max_rows=2
        )
        analysis_results = _drain(connector, gate, analysis_cycle, now=50)
        assert analysis_results[-1].deleted_count == 6
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_analysis_runs WHERE analysis_id = %s",
                (analysis_id,),
            )
            == ()
        )

        candidate_id = bytes((19,)) + b"c" * 15
        publication_key = b"p" * 32
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_publication_candidates "
                    "(candidate_id, analysis_id, reserved_revision, channel, "
                    "artifact_policy_id, display_title_policy_id, artifacts_required, "
                    "state, created_at, sealed_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, 0, 'ABANDONED', 0, NULL)",
                    (candidate_id, b"q" * 16, 77, b"default", 1, 1),
                ),
                (
                    "INSERT INTO catalog_artifact_operations "
                    "(candidate_id, publication_key, operation) "
                    "VALUES (%s, %s, 'DELETE')",
                    (candidate_id, publication_key),
                ),
                (
                    "INSERT INTO catalog_publication_selections "
                    "(candidate_id, gallery_id, publication_key) VALUES (%s, %s, %s)",
                    (candidate_id, 2, publication_key),
                ),
                (
                    "INSERT INTO catalog_publication_candidate_base_catalog "
                    "(candidate_id, base_revision, base_catalog_generation) "
                    "VALUES (%s, %s, %s)",
                    (candidate_id, 1, 1),
                ),
            ],
        )
        candidate_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.PUBLICATION_CANDIDATE,
            19,
            max_rows=1,
        )
        candidate_results = _drain(connector, gate, candidate_cycle, now=100)
        assert candidate_results[-1].deleted_count == 4
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_publication_candidates WHERE candidate_id = %s",
                (candidate_id,),
            )
            == ()
        )
    finally:
        connector.close()


def test_operational_preparation_cleanup_preserves_activated_effects(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "preparation-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)
        activated = bytes((20,)) + b"a" * 15
        abandoned = bytes((20,)) + b"b" * 15
        unactivated = bytes((20,)) + b"c" * 15
        active_event = b"A" * 16
        abandoned_event = b"B" * 16
        _fixture_rows(
            connector,
            [
                *[
                    (
                        "INSERT INTO operational_operational_event_streams "
                        "(preparation_id, created_at) VALUES (%s, 0)",
                        (preparation,),
                    )
                    for preparation in (activated, abandoned, unactivated)
                ],
                (
                    "INSERT INTO operational_operational_preparations "
                    "(preparation_id, build_id, deletion_request_generation, "
                    "operational_policy_id, state, prepared_at, completed_at) "
                    "VALUES (%s, %s, 1, 1, 'COMPLETE', 0, 1)",
                    (activated, b"1" * 16),
                ),
                (
                    "INSERT INTO operational_operational_preparations "
                    "(preparation_id, build_id, deletion_request_generation, "
                    "operational_policy_id, state, prepared_at, completed_at) "
                    "VALUES (%s, %s, 1, 1, 'ABANDONED', 0, 1)",
                    (abandoned, b"2" * 16),
                ),
                (
                    "INSERT INTO operational_operational_preparations "
                    "(preparation_id, build_id, deletion_request_generation, "
                    "operational_policy_id, state, prepared_at, completed_at) "
                    "VALUES (%s, %s, 1, 1, 'COMPLETE', 0, 1)",
                    (unactivated, b"3" * 16),
                ),
                *[
                    (
                        "INSERT INTO operational_operational_preparation_effect_seals "
                        "(preparation_id, event_count, final_chain_sha256, sealed_at) "
                        "VALUES (%s, 1, %s, 1)",
                        (preparation, bytes((index,)) * 32),
                    )
                    for index, preparation in enumerate(
                        (activated, abandoned, unactivated), start=1
                    )
                ],
                (
                    "INSERT INTO operational_operational_events "
                    "(event_id, preparation_id, sequence_no, event_type, "
                    "event_sha256, created_at) "
                    "VALUES (%s, %s, 1, 'REMOVED_GID', %s, 1)",
                    (active_event, activated, b"e" * 32),
                ),
                (
                    "INSERT INTO operational_operational_events "
                    "(event_id, preparation_id, sequence_no, event_type, "
                    "event_sha256, created_at) "
                    "VALUES (%s, %s, 1, 'REMOVED_GID', %s, 1)",
                    (abandoned_event, abandoned, b"f" * 32),
                ),
                *[
                    (
                        "INSERT INTO operational_operational_removed_gid_events "
                        "(event_id, gid, request_token) VALUES (%s, %s, %s)",
                        (event, gid, token),
                    )
                    for event, gid, token in (
                        (active_event, 1, b"r" * 16),
                        (abandoned_event, 2, b"s" * 16),
                    )
                ],
                (
                    "INSERT INTO operational_operational_activations "
                    "(source_revision, preparation_id, operational_policy_id, activated_at) "
                    "VALUES (1, %s, 1, 1)",
                    (activated,),
                ),
                (
                    "INSERT INTO operational_operational_preparation_checkpoints "
                    "(preparation_id, phase, generation, cursor_bytes, "
                    "processed_count, chain_sha256, state, updated_at) "
                    "VALUES (%s, 'REMOVED_GID', 1, %s, 1, %s, 'COMPLETE', 1)",
                    (activated, b"", b"k" * 32),
                ),
            ],
        )

        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.OPERATIONAL_PREPARATION,
            20,
            max_rows=1,
        )
        _drain(connector, gate, cycle)

        assert connector.fetch_all(
            "SELECT preparation_id FROM operational_operational_preparations "
            "ORDER BY preparation_id"
        ) == [(unactivated,)]
        assert connector.fetch_one(
            "SELECT 1 FROM operational_operational_event_streams "
            "WHERE preparation_id = %s",
            (activated,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM operational_operational_preparation_effect_seals "
            "WHERE preparation_id = %s",
            (activated,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM operational_operational_events WHERE event_id = %s",
            (active_event,),
        ) == (1,)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_operational_event_streams "
                "WHERE preparation_id = %s",
                (abandoned,),
            )
            == ()
        )
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_operational_events WHERE event_id = %s",
                (abandoned_event,),
            )
            == ()
        )
    finally:
        connector.close()


def test_staging_compaction_and_observation_orphan_cleanup_are_separate(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "observation-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)
        staging_id = bytes((21,)) + b"s" * 15
        build_id = b"b" * 16
        request = b"r" * 32
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_gallery_observation_allocations "
                    "(gallery_id, observation_id, allocated_at) VALUES (21, 1, 0)",
                    (),
                ),
                (
                    "INSERT INTO operational_gallery_observation_stagings "
                    "(staging_id, build_id, gallery_id, observation_id, state, "
                    "created_at, sealed_at) VALUES (%s, %s, 21, 1, 'SEALED', 0, 1)",
                    (staging_id, build_id),
                ),
                (
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, 21, 1)",
                    (build_id,),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_requests "
                    "(request_sha256) VALUES (%s)",
                    (request,),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_request_owners "
                    "(request_sha256, staging_id) VALUES (%s, %s)",
                    (request, staging_id),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_request_chunks "
                    "(request_sha256, position, request_bytes) VALUES (%s, 0, %s)",
                    (request, b"frame"),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_claims "
                    "(staging_id, ingest_generation, claim_generation, updated_at) "
                    "VALUES (%s, 1, 1, 0)",
                    (staging_id,),
                ),
            ],
        )
        staging_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_OBSERVATION_STAGING,
            21,
            max_rows=1,
        )
        _drain(connector, gate, staging_cycle)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_gallery_observation_stagings "
                "WHERE staging_id = %s",
                (staging_id,),
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT observation_id FROM catalog_source_build_galleries "
            "WHERE build_id = %s AND gallery_id = 21",
            (build_id,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_gallery_observation_allocations "
            "WHERE gallery_id = 21 AND observation_id = 1"
        ) == (1,)

        reused_staging = bytes((30,)) + b"u" * 15
        reused_build = b"c" * 16
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_gallery_observation_allocations "
                    "(gallery_id, observation_id, allocated_at) VALUES (24, 2, 0)",
                    (),
                ),
                (
                    "INSERT INTO operational_gallery_observation_stagings "
                    "(staging_id, build_id, gallery_id, observation_id, state, "
                    "created_at, sealed_at) VALUES (%s, %s, 24, 2, 'REUSED', 0, 1)",
                    (reused_staging, reused_build),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_claims "
                    "(staging_id, ingest_generation, claim_generation, updated_at) "
                    "VALUES (%s, 1, 1, 0)",
                    (reused_staging,),
                ),
                (
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, 24, 1)",
                    (reused_build,),
                ),
                (
                    "INSERT INTO catalog_gallery_observation_stat "
                    "(gallery_id, observation_id, file_count, byte_count) "
                    "VALUES (24, 2, 1, 7)",
                    (),
                ),
                (
                    "INSERT INTO catalog_gallery_observation_files "
                    "(gallery_id, observation_id, file_no, file_key, file_sha256) "
                    "VALUES (24, 2, 0, %s, %s)",
                    (b"n" * 32, b"f" * 32),
                ),
                (
                    "INSERT INTO catalog_gallery_observations "
                    "(gallery_id, observation_id, observation_identity_sha256) "
                    "VALUES (24, 2, %s)",
                    (b"o" * 32,),
                ),
            ],
        )
        observation_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_OBSERVATION,
            24,
            max_rows=1,
        )
        _drain(connector, gate, observation_cycle, now=100)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_gallery_observation_allocations "
                "WHERE gallery_id = 24 AND observation_id = 2"
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT observation_id FROM catalog_source_build_galleries "
            "WHERE build_id = %s AND gallery_id = 24",
            (reused_build,),
        ) == (1,)
    finally:
        connector.close()


def test_foreign_owner_predecessor_blocks_staging_compaction(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "predecessor-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)
        selected = bytes((22,)) + b"a" * 15
        foreign = bytes((23,)) + b"b" * 15
        prior_request = b"p" * 32
        next_request = b"n" * 32
        build = b"d" * 16
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_gallery_observation_stagings "
                    "(staging_id, build_id, gallery_id, observation_id, state, "
                    "created_at, sealed_at) VALUES (%s, %s, 22, 1, 'SEALED', 0, 1)",
                    (selected, build),
                ),
                (
                    "INSERT INTO operational_gallery_observation_stagings "
                    "(staging_id, build_id, gallery_id, observation_id, state, "
                    "created_at, sealed_at) VALUES (%s, %s, 23, 1, 'OPEN', 0, NULL)",
                    (foreign, b"e" * 16),
                ),
                (
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, 22, 1)",
                    (build,),
                ),
                *[
                    (
                        "INSERT INTO operational_gallery_observation_staging_requests "
                        "(request_sha256) VALUES (%s)",
                        (request,),
                    )
                    for request in (prior_request, next_request)
                ],
                (
                    "INSERT INTO operational_gallery_observation_staging_request_owners "
                    "(request_sha256, staging_id) VALUES (%s, %s)",
                    (prior_request, selected),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_request_owners "
                    "(request_sha256, staging_id) VALUES (%s, %s)",
                    (next_request, foreign),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_request_predecessors "
                    "(request_sha256, prior_request_sha256) VALUES (%s, %s)",
                    (next_request, prior_request),
                ),
            ],
        )
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_OBSERVATION_STAGING,
            22,
            max_rows=8,
        )
        results = _drain(connector, gate, cycle)
        assert results[-1].deleted_count == 0
        assert connector.fetch_one(
            "SELECT 1 FROM operational_gallery_observation_stagings "
            "WHERE staging_id = %s",
            (selected,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM operational_gallery_observation_staging_request_predecessors "
            "WHERE request_sha256 = %s",
            (next_request,),
        ) == (1,)
    finally:
        connector.close()


def test_canonical_page_identity_upload_artifact_and_hash_cache_strategies(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "digest-cleanup.sqlite3")
    try:
        gate = _exclusive(connector)

        value = bytes((31,)) + b"v" * 31
        parent_page = b"P" * 32
        child_page = b"C" * 32
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_canonical_value_allocations "
                    "(value_sha256, digest_domain, byte_count, allocated_at) "
                    "VALUES (%s, %s, 2, 0)",
                    (value, b"source_root_v1"),
                ),
                *[
                    (
                        "INSERT INTO catalog_canonical_value_pages "
                        "(page_sha256, value_sha256, page_bytes) VALUES (%s, %s, %s)",
                        (page, value, payload),
                    )
                    for page, payload in (
                        (parent_page, b"parent"),
                        (child_page, b"child"),
                    )
                ],
                *[
                    (
                        "INSERT INTO catalog_canonical_value_page_descriptors "
                        "(page_sha256, value_sha256, level, page_position, "
                        "subtree_item_count) VALUES (%s, %s, %s, %s, 1)",
                        (page, value, level, position),
                    )
                    for page, level, position in (
                        (child_page, 0, 0),
                        (parent_page, 1, 0),
                    )
                ],
                (
                    "INSERT INTO catalog_canonical_value_page_parents "
                    "(child_sha256, parent_sha256, position) VALUES (%s, %s, 0)",
                    (child_page, parent_page),
                ),
                (
                    "INSERT INTO catalog_canonical_value_identities "
                    "(value_sha256, root_page_sha256) VALUES (%s, %s)",
                    (value, parent_page),
                ),
                (
                    "INSERT INTO catalog_source_scopes "
                    "(scope_key, source_provider, source_root_sha256, "
                    "identity_policy_version) VALUES (%s, %s, %s, 1)",
                    (value, b"fixture", b"R" * 32),
                ),
            ],
        )
        canonical_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE,
            31,
            max_rows=1,
        )
        _drain(connector, gate, canonical_cycle)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_canonical_value_allocations "
                "WHERE value_sha256 = %s",
                (value,),
            )
            == ()
        )
        assert (
            connector.fetch_all(
                "SELECT page_sha256 FROM catalog_canonical_value_pages "
                "WHERE value_sha256 = %s",
                (value,),
            )
            == []
        )

        page = bytes((32,)) + b"g" * 31
        child = bytes((33,)) + b"h" * 31
        _fixture_rows(
            connector,
            [
                *[
                    (
                        "INSERT INTO catalog_gallery_observation_pages "
                        "(page_sha256, page_bytes) VALUES (%s, %s)",
                        (digest, payload),
                    )
                    for digest, payload in ((page, b"page"), (child, b"child"))
                ],
                (
                    "INSERT INTO catalog_gallery_observation_page_descriptors "
                    "(page_sha256, component, level, subtree_item_count) "
                    "VALUES (%s, %s, 1, 1)",
                    (page, b"FILE"),
                ),
                (
                    "INSERT INTO catalog_gallery_observation_page_key_bounds "
                    "(page_sha256, first_key, last_key) VALUES (%s, %s, %s)",
                    (page, b"a", b"z"),
                ),
                (
                    "INSERT INTO catalog_gallery_observation_page_children "
                    "(parent_sha256, position, child_sha256) VALUES (%s, 0, %s)",
                    (page, child),
                ),
            ],
        )
        page_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_OBSERVATION_PAGE,
            32,
            max_rows=1,
        )
        _drain(connector, gate, page_cycle, now=50)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_gallery_observation_pages WHERE page_sha256 = %s",
                (page,),
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_gallery_observation_pages WHERE page_sha256 = %s",
            (child,),
        ) == (1,)

        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_gallery_identities "
                    "(gallery_id, gallery_key, scope_key, locator_sha256) "
                    "VALUES (34, %s, %s, %s)",
                    (b"g" * 32, b"s" * 32, b"l" * 32),
                ),
                (
                    "INSERT INTO operational_gallery_observation_allocators "
                    "(gallery_id, next_observation_id, updated_at) VALUES (34, 1, 0)",
                    (),
                ),
            ],
        )
        gallery_cycle = _begin(
            connector, gate, CleanupTargetKind.GALLERY_IDENTITY, 34, max_rows=1
        )
        _drain(connector, gate, gallery_cycle, now=100)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_gallery_identities WHERE gallery_id = 34"
            )
            == ()
        )

        upload = bytes((35,)) + b"u" * 31
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_ingest_generations "
                    "(generation, started_at, completed_at) VALUES (5, 0, 1)",
                    (),
                ),
                (
                    "INSERT INTO operational_ingest_coordination_heads "
                    "(singleton_id, current_generation, completed_generation, "
                    "phase, last_transition_at) VALUES (1, 6, 5, 'READY', 1)",
                    (),
                ),
                (
                    "INSERT INTO catalog_canonical_value_allocations "
                    "(value_sha256, digest_domain, byte_count, allocated_at) "
                    "VALUES (%s, %s, 0, 0)",
                    (upload, b"source_root_v1"),
                ),
                (
                    "INSERT INTO operational_canonical_value_uploads "
                    "(generation, value_sha256) VALUES (5, %s)",
                    (upload,),
                ),
            ],
        )
        upload_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.CANONICAL_VALUE_UPLOAD,
            35,
            max_rows=1,
        )
        _drain(connector, gate, upload_cycle, now=150)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_canonical_value_uploads "
                "WHERE generation = 5 AND value_sha256 = %s",
                (upload,),
            )
            == ()
        )

        artifact = bytes((36,)) + b"a" * 31
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_artifact_blobs "
                    "(artifact_sha256, size_bytes) VALUES (%s, 4)",
                    (artifact,),
                ),
                (
                    "INSERT INTO catalog_artifact_identity "
                    "(artifact_id, publication_key, artifact_sha256) "
                    "VALUES (%s, %s, %s)",
                    (b"artifact", b"p" * 32, artifact),
                ),
                (
                    "INSERT INTO catalog_artifact_location "
                    "(artifact_sha256, artifact_locator_sha256) VALUES (%s, %s)",
                    (artifact, b"l" * 32),
                ),
            ],
        )
        artifact_cycle = _begin(
            connector, gate, CleanupTargetKind.ARTIFACT_BLOB, 36, max_rows=1
        )
        _drain(connector, gate, artifact_cycle, now=200)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM catalog_artifact_blobs WHERE artifact_sha256 = %s",
                (artifact,),
            )
            == ()
        )

        source = bytes((37,)) + b"h" * 31
        fingerprint = b"f" * 32
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_hash_cache_observations "
                    "(source_identity_sha256, fingerprint_sha256, observed_at) "
                    "VALUES (%s, %s, 10)",
                    (source, fingerprint),
                ),
                (
                    "INSERT INTO operational_file_hash_caches "
                    "(source_identity_sha256, fingerprint_sha256, file_sha256, cached_at) "
                    "VALUES (%s, %s, %s, 10)",
                    (source, fingerprint, b"z" * 32),
                ),
            ],
        )
        cache_cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.HASH_CACHE_OBSERVATION,
            37,
            max_rows=1,
        )
        _drain(connector, gate, cache_cycle, now=250)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_hash_cache_observations "
                "WHERE source_identity_sha256 = %s AND fingerprint_sha256 = %s",
                (source, fingerprint),
            )
            == ()
        )
    finally:
        connector.close()


def test_live_generation_upload_incoming_page_and_redownload_roots_block(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "retention-blockers.sqlite3")
    try:
        gate = _exclusive(connector)
        build = bytes((40,)) + b"b" * 15
        value = bytes((41,)) + b"v" * 31
        page = bytes((42,)) + b"p" * 31
        parent = bytes((43,)) + b"q" * 31
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_ingest_generations "
                    "(generation, started_at, completed_at) VALUES (10, 0, NULL)",
                    (),
                ),
                (
                    "INSERT INTO operational_ingest_coordination_heads "
                    "(singleton_id, current_generation, completed_generation, "
                    "phase, last_transition_at) VALUES (1, 10, 0, 'INGESTING', 1)",
                    (),
                ),
                (
                    "INSERT INTO catalog_source_builds "
                    "(build_id, scope_key, manifest_policy_id, state, created_at, sealed_at) "
                    "VALUES (%s, %s, 1, 'ABANDONED', 0, NULL)",
                    (build, b"s" * 32),
                ),
                (
                    "INSERT INTO operational_source_build_generations "
                    "(build_id, generation) VALUES (%s, 10)",
                    (build,),
                ),
                (
                    "INSERT INTO catalog_canonical_value_allocations "
                    "(value_sha256, digest_domain, byte_count, allocated_at) "
                    "VALUES (%s, %s, 0, 0)",
                    (value, b"source_root_v1"),
                ),
                (
                    "INSERT INTO operational_canonical_value_uploads "
                    "(generation, value_sha256) VALUES (10, %s)",
                    (value,),
                ),
                *[
                    (
                        "INSERT INTO catalog_gallery_observation_pages "
                        "(page_sha256, page_bytes) VALUES (%s, %s)",
                        (digest, b"page"),
                    )
                    for digest in (page, parent)
                ],
                (
                    "INSERT INTO catalog_gallery_observation_page_children "
                    "(parent_sha256, position, child_sha256) VALUES (%s, 0, %s)",
                    (parent, page),
                ),
                (
                    "INSERT INTO catalog_gallery_identities "
                    "(gallery_id, gallery_key, scope_key, locator_sha256) "
                    "VALUES (44, %s, %s, %s)",
                    (b"g" * 32, b"x" * 32, b"l" * 32),
                ),
                (
                    "INSERT INTO operational_gallery_redownload_states "
                    "(gallery_id, redownload_at, through_source_revision, updated_at) "
                    "VALUES (44, 1, 1, 1)",
                    (),
                ),
            ],
        )

        for kind, shard in (
            (CleanupTargetKind.SOURCE_BUILD, 40),
            (CleanupTargetKind.CANONICAL_VALUE_UPLOAD, 41),
            (CleanupTargetKind.CANONICAL_VALUE, 41),
            (CleanupTargetKind.GALLERY_OBSERVATION_PAGE, 42),
            (CleanupTargetKind.GALLERY_IDENTITY, 44),
        ):
            cycle = _begin(connector, gate, kind, shard, max_rows=8)
            results = _drain(connector, gate, cycle, now=20 + shard)
            assert results[-1].deleted_count == 0

        assert connector.fetch_one(
            "SELECT 1 FROM catalog_source_builds WHERE build_id = %s", (build,)
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM operational_canonical_value_uploads "
            "WHERE generation = 10 AND value_sha256 = %s",
            (value,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_canonical_value_allocations "
            "WHERE value_sha256 = %s",
            (value,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_gallery_observation_pages WHERE page_sha256 = %s",
            (page,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_gallery_identities WHERE gallery_id = 44"
        ) == (1,)
    finally:
        connector.close()


def test_latest_receipt_corruption_and_stale_attempts_fail_before_writes(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "receipt-corruption.sqlite3")
    try:
        gate = _exclusive(connector)
        first = bytes((45,)) + b"a" * 31
        second = bytes((45,)) + b"b" * 31
        connector.execute_many(
            "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
            "VALUES (%s, %s)",
            [(first, 1), (second, 2)],
        )
        cycle = _begin(connector, gate, CleanupTargetKind.CONTENT_BLOB, 45, max_rows=1)
        committed = _advance(connector, gate, cycle, 1, b"a" * 32, now=3)
        assert committed.cursor == first
        remaining = connector.fetch_all(
            "SELECT file_sha256 FROM catalog_content_blobs ORDER BY file_sha256"
        )

        with pytest.raises(CleanupUnavailableError, match="stale"):
            _advance(connector, gate, cycle, 1, b"b" * 32, now=4)
        assert (
            connector.fetch_all(
                "SELECT file_sha256 FROM catalog_content_blobs ORDER BY file_sha256"
            )
            == remaining
        )

        connector.execute(
            "UPDATE operational_cleanup_batch_receipts "
            "SET output_sha256 = %s WHERE cleanup_id = %s AND phase = 'CB_ROOT'",
            (b"x" * 32, cycle.cleanup_id),
        )
        with pytest.raises(CleanupCorruptionError, match="receipt"):
            _advance(connector, gate, cycle, 1, b"a" * 32, now=5)
        assert (
            connector.fetch_all(
                "SELECT file_sha256 FROM catalog_content_blobs ORDER BY file_sha256"
            )
            == remaining
        )
    finally:
        connector.close()


def test_intermediate_empty_terminal_response_loss_is_zero_write_replay(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "terminal-transition-replay.sqlite3")
    try:
        gate = _exclusive(connector)
        cycle = _begin(connector, gate, CleanupTargetKind.ARTIFACT_BLOB, 47, max_rows=8)
        committed = _advance(connector, gate, cycle, 1, b"t" * 32, now=3)
        assert committed.phase == "AB_IDENTITIES"
        assert committed.phase_complete and not committed.replayed
        before = (
            connector.fetch_all(
                "SELECT * FROM operational_cleanup_checkpoints "
                "WHERE cleanup_id = %s ORDER BY phase",
                (cycle.cleanup_id,),
            ),
            connector.fetch_all(
                "SELECT * FROM operational_cleanup_batch_receipts "
                "WHERE cleanup_id = %s ORDER BY phase",
                (cycle.cleanup_id,),
            ),
        )
        replay = _advance(connector, gate, cycle, 1, b"t" * 32, now=4)
        assert replay == CleanupBatchResult(
            cycle=cycle,
            phase="AB_IDENTITIES",
            generation=1,
            cursor=b"",
            deleted_count=0,
            row_count=0,
            phase_complete=True,
            cycle_complete=False,
            replayed=True,
        )
        after = (
            connector.fetch_all(
                "SELECT * FROM operational_cleanup_checkpoints "
                "WHERE cleanup_id = %s ORDER BY phase",
                (cycle.cleanup_id,),
            ),
            connector.fetch_all(
                "SELECT * FROM operational_cleanup_batch_receipts "
                "WHERE cleanup_id = %s ORDER BY phase",
                (cycle.cleanup_id,),
            ),
        )
        assert after == before
    finally:
        connector.close()


def test_batch_rechecks_retention_roots_and_live_exclusive_gate(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "retention-race.sqlite3")
    try:
        gate = _exclusive(connector)
        file_sha256 = bytes((48,)) + b"f" * 31
        source = b"s" * 32
        fingerprint = b"p" * 32
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
                    "VALUES (%s, 1)",
                    (file_sha256,),
                ),
                (
                    "INSERT INTO operational_hash_cache_observations "
                    "(source_identity_sha256, fingerprint_sha256, observed_at) "
                    "VALUES (%s, %s, 1)",
                    (source, fingerprint),
                ),
            ],
        )
        cycle = _begin(connector, gate, CleanupTargetKind.CONTENT_BLOB, 48, max_rows=1)
        original = connector.fetch_all
        injected = False

        def race_retention_root(
            sql: str, parameters: tuple[object, ...] = ()
        ) -> list[tuple[Any, ...]]:
            nonlocal injected
            rows = original(sql, parameters)
            if (
                not injected
                and "FROM catalog_content_blobs AS b" in sql
                and "ORDER BY b.file_sha256" in sql
                and rows
            ):
                injected = True
                connector.execute(
                    "INSERT INTO operational_file_hash_caches "
                    "(source_identity_sha256, fingerprint_sha256, file_sha256, cached_at) "
                    "VALUES (%s, %s, %s, 1)",
                    (source, fingerprint, file_sha256),
                )
            return rows

        with (
            patch.object(connector, "fetch_all", side_effect=race_retention_root),
            pytest.raises(CleanupRetentionBlockedError, match="retention root"),
        ):
            _advance(connector, gate, cycle, 1, b"r" * 32, now=3)
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_content_blobs WHERE file_sha256 = %s",
            (file_sha256,),
        ) == (1,)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_file_hash_caches "
                "WHERE source_identity_sha256 = %s AND fingerprint_sha256 = %s",
                (source, fingerprint),
            )
            == ()
        )

        with pytest.raises(RuntimeError, match="stale or expired"):
            _advance(connector, gate, cycle, 1, b"e" * 32, now=100_002)
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_content_blobs WHERE file_sha256 = %s",
            (file_sha256,),
        ) == (1,)
    finally:
        connector.close()


def test_paired_staging_identity_delete_rolls_back_on_second_write_fault(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "paired-delete-fault.sqlite3")
    try:
        gate = _exclusive(connector)
        staging = bytes((46,)) + b"s" * 15
        request = b"r" * 32
        build = b"b" * 16
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_gallery_observation_stagings "
                    "(staging_id, build_id, gallery_id, observation_id, state, "
                    "created_at, sealed_at) VALUES (%s, %s, 46, 1, 'SEALED', 0, 1)",
                    (staging, build),
                ),
                (
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, 46, 1)",
                    (build,),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_requests "
                    "(request_sha256) VALUES (%s)",
                    (request,),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_request_owners "
                    "(request_sha256, staging_id) VALUES (%s, %s)",
                    (request, staging),
                ),
                (
                    "INSERT INTO operational_gallery_observation_staging_claims "
                    "(staging_id, ingest_generation, claim_generation, updated_at) "
                    "VALUES (%s, 1, 1, 0)",
                    (staging,),
                ),
            ],
        )
        cycle = _begin(
            connector,
            gate,
            CleanupTargetKind.GALLERY_OBSERVATION_STAGING,
            46,
            max_rows=1,
        )
        result = _advance(connector, gate, cycle, 1, b"1" * 32, now=3)
        result = _advance(
            connector,
            gate,
            cycle,
            result.generation or 1,
            b"2" * 32,
            now=4,
        )
        result = _advance(
            connector,
            gate,
            cycle,
            result.generation or 1,
            b"3" * 32,
            now=5,
        )
        assert result.phase == "GOS_REQUEST_IDENTITY" and result.generation == 1

        original = connector.execute_affected

        def fail_second_delete(sql: str, parameters: tuple[object, ...] = ()) -> int:
            if sql.startswith(
                "DELETE FROM operational_gallery_observation_staging_requests"
            ):
                raise RuntimeError("injected second-delete fault")
            return original(sql, parameters)

        with (
            patch.object(connector, "execute_affected", side_effect=fail_second_delete),
            pytest.raises(RuntimeError, match="injected"),
        ):
            _advance(connector, gate, cycle, 1, b"4" * 32, now=6)
        assert connector.fetch_one(
            "SELECT 1 FROM operational_gallery_observation_staging_request_owners "
            "WHERE request_sha256 = %s",
            (request,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT 1 FROM operational_gallery_observation_staging_requests "
            "WHERE request_sha256 = %s",
            (request,),
        ) == (1,)

        committed = _advance(connector, gate, cycle, 1, b"4" * 32, now=7)
        assert committed.row_count == 1
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_gallery_observation_staging_requests "
                "WHERE request_sha256 = %s",
                (request,),
            )
            == ()
        )
    finally:
        connector.close()


def test_cleanup_sql_is_bounded_static_and_has_portable_mariadb_lock_shape(
    tmp_path: Path,
) -> None:
    source = Path(cleanup_module.__file__).read_text(encoding="utf-8").upper()
    assert "COUNT(" not in source
    assert "SUM(" not in source
    assert "SELECT RELATION" not in source
    assert "SELECT PREDICATE" not in source

    connector = _database(tmp_path / "cleanup-explain.sqlite3")
    try:
        target = cleanup_module._STATIC_PLANS[CleanupTargetKind.ANALYSIS_RUN]
        spec = target.phases["AR_ROOT"][0]
        sql = cleanup_module._static_select_sql(
            target, spec, exact=False, has_after=False
        )
        parameters = cleanup_module._static_shard_parameters(
            target,
            CleanupCycle(
                cleanup_id=cleanup_module._cleanup_id(
                    CleanupTargetKind.ANALYSIS_RUN, 0, 1
                ),
                target_kind=CleanupTargetKind.ANALYSIS_RUN,
                shard_no=0,
                target_key=cleanup_module._target_key(
                    CleanupTargetKind.ANALYSIS_RUN, 0
                ),
                cycle_generation=1,
                cycle_cutoff_at=100,
                max_rows_per_transaction=8,
                hash_cache_max_age_microseconds=0,
            ),
        ) + (8,)
        plan = connector.fetch_all("EXPLAIN QUERY PLAN " + sql, parameters)
        assert plan
        assert any("INDEX" in str(row[-1]).upper() for row in plan)
    finally:
        connector.close()

    class _MariaRecorder:
        def __init__(self) -> None:
            self.query = ""

        def fetch_one(
            self, query: str, parameters: tuple[object, ...]
        ) -> tuple[object, ...]:
            self.query = query
            return ()

    recorder = _MariaRecorder()
    work = VNextUnitOfWork(recorder, backend="mariadb")  # type: ignore[arg-type]
    work.lock_row(
        LockRank.CHILD,
        b"cleanup-mariadb-shape",
        "SELECT candidate_id FROM catalog_publication_candidates "
        "WHERE candidate_id = %s",
        (b"c" * 16,),
    )
    assert recorder.query.endswith(" FOR UPDATE")
    assert "%s" in recorder.query and "?" not in recorder.query
