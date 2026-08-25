from __future__ import annotations

import inspect
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from vnext_analysis_fixtures import (
    complete_analysis_run,
    seed_analysis_component,
)
from vnext_canonical_value_fixtures import seed_canonical_value
from vnext_catalog_identity_fixtures import seed_gallery_identity
from vnext_catalog_registry_fixtures import (
    seed_analysis_policy,
    seed_manifest_policy,
    seed_source_scope,
)
from vnext_manifest_fixtures import (
    seed_build_manifest,
    seed_gallery_manifest,
    seed_snapshot_manifest,
)
from vnext_publication_fixtures import seed_publication_finalization_checkpoint

import h2hdb.vnext_source_build_repository as source_build_module
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_analysis_repository import ANALYSIS_COMPONENTS, AnalysisRepository
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_domains import require_int63, require_uuid16
from h2hdb.vnext_identity import (
    artifact_source_manifest_digest,
    decode_canonical_value_page,
    gallery_key,
    iter_source_relative_locator_payload,
    source_root_digest,
)
from h2hdb.vnext_ingest_fence_repository import (
    IngestFenceRepository,
    IngestFenceUnavailableError,
    IngestTurn,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_source_build_repository import (
    AssemblyBatchAttempt,
    SourceBuildConflictError,
    SourceBuildManifestSummary,
    SourceBuildNotReadyError,
    SourceBuildRepository,
    SourceDiscoveryPlan,
    SourceDiscoveryPlanError,
    SourceRootBuildCommand,
    require_source_build_publication_identity,
    source_build_recovery_identity,
    source_build_snapshot_attempt_id,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


def _generated_database(path: Path) -> SQLiteConnector:
    connector = SQLiteConnector(str(path))
    connector.connect()
    payload: Any = ARTIFACT["backends"]
    payload = payload["sqlite"]
    for _slice_id, statements in payload["slices"]:
        for _statement_id, _kind, _name, sql in statements:
            connector.execute(sql)
    for seed in payload["bootstrap_seeds"]:
        connector.execute(seed["sql"], seed["parameters"])
    seed_manifest_policy(connector)
    return connector


def _authorities(connector: SQLiteConnector) -> tuple[GateLease, IngestTurn]:
    with connector.transaction():
        with patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=b"g" * 16,
        ):
            gate = MaintenanceGateRepository.claim_shared(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=10,
                lease_duration=1_000_000,
            )
    with connector.transaction():
        turn = IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend="sqlite"),
            owner_token=b"i" * 16,
            now=11,
            lease_duration=1_000_000,
        )
    return gate, turn


def _upload(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    plan: CanonicalValueUploadPlan,
    *,
    now: int,
) -> None:
    with connector.transaction():
        CanonicalValueRepository.allocate(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=now,
        )
    for page in plan.iter_pages():
        with connector.transaction():
            CanonicalValueRepository.put_page(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                plan=plan,
                prepared_page=page,
                now=now + 1,
            )
    with connector.transaction():
        CanonicalValueRepository.seal(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=now + 2,
        )


def _open_build(
    connector: SQLiteConnector,
) -> tuple[GateLease, IngestTurn, SourceRootBuildCommand]:
    gate, turn = _authorities(connector)
    command = SourceRootBuildCommand((), b"b" * 16)
    with command.prepare_root_upload() as root:
        _upload(connector, gate, turn, root, now=21)
        with connector.transaction():
            SourceBuildRepository.handoff_root(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                command=command,
                root_plan=root,
                now=24,
            )
    return gate, turn, command


def test_source_build_database_clock_and_abandonment_replay(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "source-abandon.sqlite3")
    try:
        gate, turn, command = _open_build(connector)
        created = connector.fetch_one(
            "SELECT created.created_at, working.assigned_at "
            "FROM catalog_source_build_created_ats created "
            "JOIN operational_source_working_builds working "
            "ON working.build_id = created.build_id "
            "WHERE created.build_id = %s",
            (command.build_attempt_id,),
        )
        assert created[0] == created[1]
        assert created[0] != 24
        # This must be an absolute Unix-microsecond timestamp, not merely the
        # fractional milliseconds of the current second.  The latter can
        # happen if SQLiteConnector rewrites a strftime('%s', ...) format
        # literal as a repository parameter marker.
        assert created[0] > 1_600_000_000_000_000

        with connector.transaction():
            abandoned = SourceBuildRepository.abandon(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                build_id=command.build_attempt_id,
                now=25,
            )
        assert not abandoned.replayed
        assert connector.fetch_one(
            "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
            (command.build_attempt_id,),
        ) == ("ABANDONED",)
        assert (
            connector.fetch_all("SELECT * FROM operational_source_working_builds") == []
        )
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (turn.generation,),
        ) == (command.build_attempt_id,)

        before = connector.fetch_all(
            "SELECT build_id, state FROM catalog_source_build_states"
        )
        with connector.transaction():
            replay = SourceBuildRepository.abandon(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                build_id=command.build_attempt_id,
                now=26,
            )
        assert replay.replayed
        assert (
            connector.fetch_all(
                "SELECT build_id, state FROM catalog_source_build_states"
            )
            == before
        )

        replacement = SourceRootBuildCommand((), b"n" * 16)
        with replacement.prepare_root_upload() as root:
            _upload(connector, gate, turn, root, now=27)
            with (
                connector.transaction(),
                pytest.raises(SourceBuildConflictError, match="ABANDONED"),
            ):
                SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    command=replacement,
                    root_plan=root,
                    now=30,
                )
    finally:
        connector.close()


def test_source_build_abandonment_rolls_back_slot_delete_on_cas_fault(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "source-abandon-fault.sqlite3")
    try:
        gate, turn, command = _open_build(connector)
        original = connector.execute_affected

        def fail_state_cas(query: str, data: tuple[Any, ...] = ()) -> int:
            if "UPDATE catalog_source_build_states" in query:
                raise RuntimeError("injected abandonment CAS failure")
            return original(query, data)

        with monkeypatch.context() as context:
            context.setattr(connector, "execute_affected", fail_state_cas)
            with pytest.raises(RuntimeError, match="injected"):
                with connector.transaction():
                    SourceBuildRepository.abandon(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        build_id=command.build_attempt_id,
                        now=25,
                    )
        assert connector.fetch_one(
            "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
            (command.build_attempt_id,),
        ) == ("OPEN",)
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_working_builds WHERE slot = 1"
        ) == (command.build_attempt_id,)
    finally:
        connector.close()


def _published_commit(
    connector: SQLiteConnector,
    *,
    receipt_id: bytes,
    candidate_id: bytes,
    revision: int,
    source_revision: int,
    generation: int,
    snapshot: bytes,
    committed_at: int,
) -> None:
    connector.execute("PRAGMA foreign_keys = OFF")
    connector.execute(
        "INSERT INTO catalog_source_revision_anchors (source_revision) VALUES (%s)",
        (source_revision,),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_channels "
        "(source_revision, channel) VALUES (%s, %s)",
        (source_revision, b"default"),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_snapshot_manifests "
        "(source_revision, snapshot_manifest_sha256) VALUES (%s, %s)",
        (source_revision, snapshot),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_descriptor_seals "
        "(source_revision) VALUES (%s)",
        (source_revision,),
    )
    connector.execute(
        "INSERT INTO catalog_revision_anchors (revision) VALUES (%s)",
        (revision,),
    )
    connector.execute(
        "INSERT INTO catalog_revision_publication_counts "
        "(revision, publication_count) VALUES (%s, 0)",
        (revision,),
    )
    connector.execute(
        "INSERT INTO catalog_revision_descriptor_seals (revision) VALUES (%s)",
        (revision,),
    )
    connector.execute(
        "INSERT INTO catalog_publication_generation_nodes (generation) VALUES (%s)",
        (generation,),
    )
    connector.execute(
        "INSERT INTO catalog_publication_generation_successors "
        "(successor_generation, predecessor_generation) VALUES (%s, %s)",
        (generation, generation - 1),
    )
    connector.execute(
        "INSERT INTO catalog_publication_commit_anchors (receipt_id) VALUES (%s)",
        (receipt_id,),
    )
    members = (
        ("catalog_publication_commit_candidates", "candidate_id", candidate_id),
        (
            "catalog_publication_commit_catalog_revisions",
            "revision",
            revision,
        ),
        (
            "catalog_publication_commit_source_revisions",
            "source_revision",
            source_revision,
        ),
        ("catalog_publication_commit_generations", "generation", generation),
        (
            "catalog_publication_commit_operational_preparations",
            "preparation_id",
            bytes((generation,)) * 16,
        ),
        (
            "catalog_publication_commit_operational_policies",
            "operational_policy_id",
            1,
        ),
        ("catalog_publication_commit_artifact_policies", "artifact_policy_id", 1),
        (
            "catalog_publication_commit_display_title_policies",
            "display_title_policy_id",
            1,
        ),
        ("catalog_publication_commit_new_galleries", "new_galleries", 0),
        ("catalog_publication_commit_changed_galleries", "changed_galleries", 0),
        ("catalog_publication_commit_removed_galleries", "removed_galleries", 0),
        ("catalog_publication_commit_duplicate_losers", "duplicate_losers", 0),
        (
            "catalog_publication_commit_committed_ats",
            "committed_at",
            committed_at,
        ),
    )
    for table, column, value in members:
        connector.execute(
            f"INSERT INTO {table} (receipt_id, {column}) VALUES (%s, %s)",
            (receipt_id, value),
        )
    connector.execute(
        "INSERT INTO catalog_publication_commit_seals (receipt_id) VALUES (%s)",
        (receipt_id,),
    )
    if generation == 1:
        connector.execute(
            "INSERT INTO catalog_publication_commit_head_receipts "
            "(channel, receipt_id) VALUES (%s, %s)",
            (b"default", receipt_id),
        )
    else:
        connector.execute(
            "UPDATE catalog_publication_commit_head_receipts SET receipt_id = %s "
            "WHERE channel = %s",
            (receipt_id, b"default"),
        )
    connector.execute("PRAGMA foreign_keys = ON")


def _link_published_candidate_to_build(
    connector: SQLiteConnector,
    *,
    candidate_id: bytes,
    build_id: bytes,
    revision: int,
    timestamp: int,
) -> None:
    """Seed the sealed candidate-to-analysis-to-build proof used by publication."""

    analysis_id = sha256(b"source-working-recovery\0" + candidate_id).digest()[:16]
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        connector.execute(
            "INSERT INTO catalog_analysis_run_anchors (analysis_id) VALUES (%s)",
            (analysis_id,),
        )
        for table, column, value in (
            ("catalog_analysis_run_build_ids", "build_id", build_id),
            ("catalog_analysis_run_policy_ids", "policy_id", 1),
            (
                "catalog_analysis_run_input_manifest_sha256s",
                "input_manifest_sha256",
                sha256(b"source-working-recovery-input\0" + build_id).digest(),
            ),
            ("catalog_analysis_run_started_ats", "started_at", timestamp),
            ("catalog_analysis_run_states", "state", "COMPLETE"),
        ):
            connector.execute(
                f"INSERT INTO {table} (analysis_id, {column}) VALUES (%s, %s)",
                (analysis_id, value),
            )
        connector.execute(
            "INSERT INTO catalog_analysis_run_identities "
            "(build_id, policy_id, analysis_id) VALUES (%s, %s, %s)",
            (build_id, 1, analysis_id),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_run_descriptor_seals "
            "(analysis_id) VALUES (%s)",
            (analysis_id,),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_run_completed_ats "
            "(analysis_id, completed_at) VALUES (%s, %s)",
            (analysis_id, timestamp + 1),
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_provenance "
            "(source_revision, analysis_id) VALUES (%s, %s)",
            (revision, analysis_id),
        )

        connector.execute(
            "INSERT INTO catalog_publication_candidate_anchors "
            "(candidate_id) VALUES (%s)",
            (candidate_id,),
        )
        for table, column, value in (
            ("catalog_publication_candidate_analysis_ids", "analysis_id", analysis_id),
            (
                "catalog_publication_candidate_reserved_revisions",
                "reserved_revision",
                revision,
            ),
            (
                "catalog_publication_candidate_artifact_policy_ids",
                "artifact_policy_id",
                1,
            ),
            (
                "catalog_publication_candidate_display_title_policy_ids",
                "display_title_policy_id",
                1,
            ),
            (
                "catalog_publication_candidate_artifacts_required",
                "artifacts_required",
                0,
            ),
            ("catalog_publication_candidate_created_ats", "created_at", timestamp + 2),
        ):
            connector.execute(
                f"INSERT INTO {table} (candidate_id, {column}) VALUES (%s, %s)",
                (candidate_id, value),
            )
        connector.execute(
            "INSERT INTO catalog_publication_candidate_definition_seals "
            "(candidate_id) VALUES (%s)",
            (candidate_id,),
        )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")


def _seed_complete_analysis_for_build(
    connector: SQLiteConnector,
    *,
    analysis_id: bytes,
    build_id: bytes,
    policy_id: int = 1,
    timestamp: int,
) -> None:
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        connector.execute(
            "INSERT INTO catalog_analysis_run_anchors (analysis_id) VALUES (%s)",
            (analysis_id,),
        )
        for table, column, value in (
            ("catalog_analysis_run_build_ids", "build_id", build_id),
            ("catalog_analysis_run_policy_ids", "policy_id", policy_id),
            (
                "catalog_analysis_run_input_manifest_sha256s",
                "input_manifest_sha256",
                sha256(
                    b"foreign-provenance-input\0"
                    + build_id
                    + policy_id.to_bytes(8, "big")
                ).digest(),
            ),
            ("catalog_analysis_run_started_ats", "started_at", timestamp),
            ("catalog_analysis_run_states", "state", "COMPLETE"),
        ):
            connector.execute(
                f"INSERT INTO {table} (analysis_id, {column}) VALUES (%s, %s)",
                (analysis_id, value),
            )
        connector.execute(
            "INSERT INTO catalog_analysis_run_identities "
            "(build_id, policy_id, analysis_id) VALUES (%s, %s, %s)",
            (build_id, policy_id, analysis_id),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_run_descriptor_seals "
            "(analysis_id) VALUES (%s)",
            (analysis_id,),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_run_completed_ats "
            "(analysis_id, completed_at) VALUES (%s, %s)",
            (analysis_id, timestamp + 1),
        )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")


def _seed_publication_state(
    connector: SQLiteConnector,
    *,
    receipt_id: bytes,
    candidate_id: bytes,
    build_id: bytes,
    snapshot: bytes,
    committed_at: int,
    finalized: bool,
    revision: int = 1,
    source_revision: int = 1,
    generation: int = 1,
) -> None:
    if not connector.fetch_one(
        "SELECT value_sha256 FROM catalog_canonical_value_identities "
        "WHERE value_sha256 = %s",
        (snapshot,),
    ):
        seed_canonical_value(
            connector,
            value_sha256=snapshot,
            digest_domain=b"source_snapshot_manifest_v1",
            page_sha256=sha256(b"snapshot-fixture\0" + snapshot).digest(),
            page_bytes=b"snapshot-fixture",
            subtree_item_count=len(b"snapshot-fixture"),
            allocated_at=committed_at - 1,
        )
    seed_snapshot_manifest(
        connector,
        snapshot_manifest_sha256=snapshot,
        gallery_count=0,
        file_count=0,
        byte_count=0,
    )
    _published_commit(
        connector,
        receipt_id=receipt_id,
        candidate_id=candidate_id,
        revision=revision,
        source_revision=source_revision,
        generation=generation,
        snapshot=snapshot,
        committed_at=committed_at,
    )
    _link_published_candidate_to_build(
        connector,
        candidate_id=candidate_id,
        build_id=build_id,
        revision=source_revision,
        timestamp=committed_at - 3,
    )
    build_base = connector.fetch_one(
        "SELECT base_receipt_id FROM "
        "catalog_source_build_base_publication_commits WHERE build_id = %s",
        (build_id,),
    )
    if build_base:
        connector.execute(
            "INSERT INTO catalog_publication_candidate_base_publication_commits "
            "(candidate_id, base_receipt_id) VALUES (%s, %s)",
            (candidate_id, build_base[0]),
        )
    seed_publication_finalization_checkpoint(
        connector,
        receipt_id=receipt_id,
        updated_at=committed_at,
    )
    if finalized:
        key = (receipt_id, 1)
        connector.execute(
            "INSERT INTO catalog_publication_finalization_batch_anchors "
            "(receipt_id, start_generation) VALUES (%s, %s)",
            key,
        )
        connector.execute(
            "INSERT INTO catalog_publication_finalization_batch_coordinates "
            "(receipt_id, batch_key, start_generation) VALUES (%s, %s, %s)",
            (receipt_id, b"terminal", 1),
        )
        for table, column, value in (
            (
                "catalog_publication_finalization_batch_start_cursors",
                "start_cursor",
                b"",
            ),
            (
                "catalog_publication_finalization_batch_start_counts",
                "start_processed_count",
                0,
            ),
            (
                "catalog_publication_finalization_batch_next_cursors",
                "next_cursor",
                b"",
            ),
            ("catalog_publication_finalization_batch_row_counts", "row_count", 0),
            (
                "catalog_publication_finalization_batch_committed_ats",
                "committed_at",
                committed_at + 1,
            ),
        ):
            connector.execute(
                f"INSERT INTO {table} "
                f"(receipt_id, start_generation, {column}) VALUES (%s, %s, %s)",
                (*key, value),
            )
        connector.execute(
            "INSERT INTO catalog_publication_finalization_batch_seals "
            "(receipt_id, start_generation) VALUES (%s, %s)",
            key,
        )
        connector.execute(
            "UPDATE catalog_publication_finalization_checkpoint_generations "
            "SET generation = 2 WHERE receipt_id = %s",
            (receipt_id,),
        )
        connector.execute(
            "UPDATE catalog_publication_finalization_checkpoint_states "
            "SET state = 'COMPLETE' WHERE receipt_id = %s",
            (receipt_id,),
        )
        connector.execute(
            "UPDATE catalog_publication_finalization_checkpoint_updated_ats "
            "SET updated_at = %s WHERE receipt_id = %s",
            (committed_at + 1, receipt_id),
        )
        connector.execute(
            "INSERT INTO catalog_publication_commit_finalizations "
            "(receipt_id) VALUES (%s)",
            (receipt_id,),
        )
    expected = (
        "PROJECTION_FINALIZED" if finalized else "DB_COMMITTED",
        committed_at + 1 if finalized else None,
    )
    assert (
        connector.fetch_one(
            "SELECT state, finalized_at FROM catalog_publication_receipts "
            "WHERE receipt_id = %s",
            (receipt_id,),
        )
        == expected
    )


def _snapshot_command(
    root: tuple[str, ...],
    summary: SourceBuildManifestSummary,
) -> SourceRootBuildCommand:
    return SourceRootBuildCommand(
        root,
        source_build_snapshot_attempt_id(source_root_digest(root), summary),
        summary,
    )


def _handoff_snapshot_command(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    command: SourceRootBuildCommand,
    *,
    now: int,
) -> bytes:
    with command.prepare_root_upload() as root_plan:
        _upload(connector, gate, turn, root_plan, now=now)
        with connector.transaction():
            handoff = SourceBuildRepository.handoff_root(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                command=command,
                root_plan=root_plan,
                now=now + 3,
            )
    return handoff.build_id


def _force_sealed_snapshot_build(
    connector: SQLiteConnector,
    *,
    build_id: bytes,
    summary: SourceBuildManifestSummary,
) -> int:
    created_at = require_int63(
        connector.fetch_one(
            "SELECT created_at FROM catalog_source_build_created_ats "
            "WHERE build_id = %s",
            (build_id,),
        )[0],
        field="fixture source build created_at",
    )
    sealed_at = created_at + 1
    connector.execute(
        "UPDATE catalog_source_build_states SET state = %s WHERE build_id = %s",
        ("SEALED", build_id),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_sealed_ats (build_id, sealed_at) "
        "VALUES (%s, %s)",
        (build_id, sealed_at),
    )
    seed_build_manifest(
        connector,
        build_id=build_id,
        manifest_sha256=summary.manifest_sha256,
        gallery_count=summary.gallery_count,
        file_count=summary.file_count,
        byte_count=summary.byte_count,
        computed_at=sealed_at,
    )
    cursor = (
        b""
        if summary.gallery_count == 0
        else (summary.gallery_count - 1).to_bytes(8, "big")
    )
    connector.execute(
        "UPDATE operational_source_build_discovery_checkpoints SET "
        "generation = %s, cursor_bytes = %s, processed_count = %s, "
        "state = %s, updated_at = %s WHERE build_id = %s",
        (2, cursor, summary.gallery_count, "COMPLETE", sealed_at, build_id),
    )
    connector.execute(
        "UPDATE operational_source_build_assembly_checkpoints SET "
        "generation = %s, cursor_bytes = %s, processed_gallery_count = %s, "
        "processed_file_count = %s, processed_byte_count = %s, "
        "manifest_chain_sha256 = %s, state = %s, updated_at = %s "
        "WHERE build_id = %s",
        (
            2,
            cursor,
            summary.gallery_count,
            summary.file_count,
            summary.byte_count,
            summary.manifest_sha256,
            "COMPLETE",
            sealed_at,
            build_id,
        ),
    )
    assert (
        connector.execute_affected(
            "DELETE FROM operational_source_working_builds "
            "WHERE slot = %s AND build_id = %s",
            (1, build_id),
        )
        == 1
    )
    return sealed_at


def _restore_sealed_source_working(
    connector: SQLiteConnector,
    *,
    build_id: bytes,
) -> int:
    created_at = require_int63(
        connector.fetch_one(
            "SELECT created_at FROM catalog_source_build_created_ats "
            "WHERE build_id = %s",
            (build_id,),
        )[0],
        field="fixture source build created_at",
    )
    connector.execute(
        "INSERT INTO operational_source_working_builds "
        "(slot, build_id, assigned_at) VALUES (%s, %s, %s)",
        (1, build_id, created_at),
    )
    return created_at


def _seal_published_analysis_baseline(
    connector: SQLiteConnector,
    *,
    candidate_id: bytes,
    sealed_at: int,
) -> bytes:
    analysis_id = require_uuid16(
        connector.fetch_one(
            "SELECT analysis_id FROM catalog_publication_candidate_analysis_ids "
            "WHERE candidate_id = %s",
            (candidate_id,),
        )[0],
        field="fixture publication analysis_id",
    )
    connector.execute(
        "INSERT INTO catalog_analysis_state_ancestry "
        "(analysis_id, ancestor_depth, ancestor_analysis_id) "
        "VALUES (%s, %s, %s)",
        (analysis_id, 0, analysis_id),
    )
    for component in sorted(ANALYSIS_COMPONENTS):
        seed_analysis_component(
            connector,
            analysis_id=analysis_id,
            state_component=component,
            row_count=0,
            sealed_at=sealed_at,
            terminal_receipt=True,
        )
    return analysis_id


def _analysis_ready_published_base(
    connector: SQLiteConnector,
    *,
    root: tuple[str, ...],
) -> tuple[GateLease, int, bytes]:
    seed_analysis_policy(connector)
    gate, base_turn = _authorities(connector)
    summary = SourceBuildManifestSummary.empty()
    build_id = _handoff_snapshot_command(
        connector,
        gate,
        base_turn,
        _snapshot_command(root, summary),
        now=20,
    )
    sealed_at = _force_sealed_snapshot_build(
        connector,
        build_id=build_id,
        summary=summary,
    )
    receipt_id = b"1" * 16
    candidate_id = b"q" * 16
    _seed_publication_state(
        connector,
        receipt_id=receipt_id,
        candidate_id=candidate_id,
        build_id=build_id,
        snapshot=summary.manifest_sha256,
        committed_at=sealed_at + 10,
        finalized=True,
    )
    _seal_published_analysis_baseline(
        connector,
        candidate_id=candidate_id,
        sealed_at=sealed_at + 8,
    )
    with connector.transaction():
        IngestFenceRepository.complete(
            VNextUnitOfWork(connector, backend="sqlite"),
            base_turn,
            now=30,
        )
    return gate, sealed_at, receipt_id


def test_open_canonical_snapshot_working_build_resumes_in_new_generation(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "snapshot-open-canonical-resume.sqlite3")
    try:
        gate, first_turn = _authorities(connector)
        summary = SourceBuildManifestSummary(b"A" * 32, 1, 0, 0)
        command = _snapshot_command(("open-canonical",), summary)
        build_id = _handoff_snapshot_command(
            connector,
            gate,
            first_turn,
            command,
            now=20,
        )
        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                first_turn,
                now=30,
            )
            resumed_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"r" * 16,
                now=31,
                lease_duration=100,
            )

        assert (
            _handoff_snapshot_command(
                connector,
                gate,
                resumed_turn,
                command,
                now=32,
            )
            == build_id
        )
        assert connector.fetch_one(
            "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
            (build_id,),
        ) == ("OPEN",)
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_working_builds WHERE slot = %s",
            (1,),
        ) == (build_id,)
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (resumed_turn.generation,),
        ) == (build_id,)
    finally:
        connector.close()


def test_current_finalized_multi_policy_build_remains_authoritative(
    tmp_path: Path,
) -> None:
    connector = _generated_database(
        tmp_path / "snapshot-finalized-multi-policy.sqlite3"
    )
    try:
        seed_analysis_policy(connector)
        seed_analysis_policy(connector, policy_id=2, algorithm_version=2)
        gate, first_turn = _authorities(connector)
        root = ("multi-policy-current",)
        current_summary = SourceBuildManifestSummary.empty()
        successor_summary = SourceBuildManifestSummary(b"B" * 32, 1, 0, 0)
        current_command = _snapshot_command(root, current_summary)
        current_build = _handoff_snapshot_command(
            connector,
            gate,
            first_turn,
            current_command,
            now=20,
        )
        sealed_at = _force_sealed_snapshot_build(
            connector,
            build_id=current_build,
            summary=current_summary,
        )
        receipt_id = b"1" * 16
        candidate_id = b"q" * 16
        _seed_publication_state(
            connector,
            receipt_id=receipt_id,
            candidate_id=candidate_id,
            build_id=current_build,
            snapshot=current_summary.manifest_sha256,
            committed_at=sealed_at + 10,
            finalized=True,
        )
        secondary_analysis_id = b"2" * 16
        _seed_complete_analysis_for_build(
            connector,
            analysis_id=secondary_analysis_id,
            build_id=current_build,
            policy_id=2,
            timestamp=sealed_at + 8,
        )
        published_analysis_id = connector.fetch_one(
            "SELECT analysis_id FROM catalog_publication_candidate_analysis_ids "
            "WHERE candidate_id = %s",
            (candidate_id,),
        )[0]
        assert connector.fetch_all(
            "SELECT analysis_id, policy_id, state FROM catalog_analysis_runs "
            "WHERE build_id = %s ORDER BY policy_id",
            (current_build,),
        ) == [
            (published_analysis_id, 1, "COMPLETE"),
            (secondary_analysis_id, 2, "COMPLETE"),
        ]
        assert connector.fetch_one(
            "SELECT provenance.analysis_id, candidate.analysis_id "
            "FROM catalog_publication_commit_heads AS head "
            "JOIN catalog_publication_commit_source_revisions AS source "
            "ON source.receipt_id = head.receipt_id "
            "JOIN catalog_source_revision_provenance AS provenance "
            "ON provenance.source_revision = source.source_revision "
            "JOIN catalog_publication_commit_candidates AS committed_candidate "
            "ON committed_candidate.receipt_id = head.receipt_id "
            "JOIN catalog_publication_candidate_analysis_ids AS candidate "
            "ON candidate.candidate_id = committed_candidate.candidate_id "
            "WHERE head.channel = %s",
            (b"default",),
        ) == (published_analysis_id, published_analysis_id)

        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                first_turn,
                now=30,
            )
            replay_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"r" * 16,
                now=31,
                lease_duration=1_000_000,
            )

        assert (
            _handoff_snapshot_command(
                connector,
                gate,
                replay_turn,
                current_command,
                now=32,
            )
            == current_build
        )
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (replay_turn.generation,),
        ) == (current_build,)

        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                replay_turn,
                now=40,
            )
            successor_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"s" * 16,
                now=41,
                lease_duration=1_000_000,
            )
        successor_build = _handoff_snapshot_command(
            connector,
            gate,
            successor_turn,
            _snapshot_command(root, successor_summary),
            now=42,
        )
        assert successor_build != current_build
        assert connector.fetch_one(
            "SELECT base_receipt_id FROM "
            "catalog_source_build_base_publication_commits WHERE build_id = %s",
            (successor_build,),
        ) == (receipt_id,)
    finally:
        connector.close()


def test_snapshot_recurrence_derives_successor_from_current_publication_base(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "snapshot-a-b-a.sqlite3")
    try:
        gate, turn_a = _authorities(connector)
        root = ("cycle-root",)
        summary_a = SourceBuildManifestSummary.empty()
        summary_b = SourceBuildManifestSummary(b"B" * 32, 1, 0, 0)

        build_a = _handoff_snapshot_command(
            connector,
            gate,
            turn_a,
            _snapshot_command(root, summary_a),
            now=20,
        )
        sealed_a = _force_sealed_snapshot_build(
            connector,
            build_id=build_a,
            summary=summary_a,
        )
        _seed_publication_state(
            connector,
            receipt_id=b"1" * 16,
            candidate_id=b"q" * 16,
            build_id=build_a,
            snapshot=summary_a.manifest_sha256,
            committed_at=sealed_a + 10,
            finalized=True,
        )
        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                turn_a,
                now=30,
            )

        with connector.transaction():
            turn_b = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"j" * 16,
                now=31,
                lease_duration=1_000_000,
            )
        build_b = _handoff_snapshot_command(
            connector,
            gate,
            turn_b,
            _snapshot_command(root, summary_b),
            now=32,
        )
        assert build_b != build_a
        assert connector.fetch_one(
            "SELECT base_receipt_id FROM "
            "catalog_source_build_base_publication_commits WHERE build_id = %s",
            (build_b,),
        ) == (b"1" * 16,)
        sealed_b = _force_sealed_snapshot_build(
            connector,
            build_id=build_b,
            summary=summary_b,
        )
        _seed_publication_state(
            connector,
            receipt_id=b"2" * 16,
            candidate_id=b"r" * 16,
            build_id=build_b,
            snapshot=summary_b.manifest_sha256,
            committed_at=sealed_b + 10,
            finalized=True,
            revision=2,
            source_revision=2,
            generation=2,
        )
        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                turn_b,
                now=40,
            )

        with connector.transaction():
            turn_c = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"k" * 16,
                now=41,
                lease_duration=1_000_000,
            )
        assert (
            connector.execute_affected(
                "UPDATE catalog_source_build_base_publication_commits "
                "SET base_receipt_id = %s WHERE build_id = %s",
                (b"2" * 16, build_b),
            )
            == 1
        )
        with pytest.raises(SourceBuildConflictError, match="base|own publication"):
            _handoff_snapshot_command(
                connector,
                gate,
                turn_c,
                _snapshot_command(root, summary_b),
                now=41,
            )
        assert (
            connector.execute_affected(
                "UPDATE catalog_source_build_base_publication_commits "
                "SET base_receipt_id = %s WHERE build_id = %s",
                (b"1" * 16, build_b),
            )
            == 1
        )
        # Reproduce the old A-replay failure: the live generation and sole
        # source-working slot were left mapped to finalized historical A while
        # B is the current head.  The exact frozen A snapshot must be rebased
        # to a fresh C successor, not trapped in another historical replay.
        created_a = connector.fetch_one(
            "SELECT created_at FROM catalog_source_build_created_ats "
            "WHERE build_id = %s",
            (build_a,),
        )[0]
        connector.execute(
            "INSERT INTO operational_source_build_generations "
            "(build_id, generation) VALUES (%s, %s)",
            (build_a, turn_c.generation),
        )
        connector.execute(
            "INSERT INTO operational_source_working_builds "
            "(slot, build_id, assigned_at) VALUES (%s, %s, %s)",
            (1, build_a, created_a),
        )
        build_c = _handoff_snapshot_command(
            connector,
            gate,
            turn_c,
            _snapshot_command(root, summary_a),
            now=42,
        )
        assert build_c not in {build_a, build_b}
        assert connector.fetch_one(
            "SELECT base_receipt_id FROM "
            "catalog_source_build_base_publication_commits WHERE build_id = %s",
            (build_c,),
        ) == (b"2" * 16,)
        sealed_c = _force_sealed_snapshot_build(
            connector,
            build_id=build_c,
            summary=summary_a,
        )
        _seed_publication_state(
            connector,
            receipt_id=b"3" * 16,
            candidate_id=b"s" * 16,
            build_id=build_c,
            snapshot=summary_a.manifest_sha256,
            committed_at=sealed_c + 10,
            finalized=True,
            revision=3,
            source_revision=3,
            generation=3,
        )
        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                turn_c,
                now=50,
            )

        with connector.transaction():
            replay_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"l" * 16,
                now=51,
                lease_duration=1_000_000,
            )
        assert (
            connector.execute_affected(
                "UPDATE catalog_source_build_base_publication_commits "
                "SET base_receipt_id = %s WHERE build_id = %s",
                (b"1" * 16, build_c),
            )
            == 1
        )
        with pytest.raises(SourceBuildConflictError, match="base|identity"):
            _handoff_snapshot_command(
                connector,
                gate,
                replay_turn,
                _snapshot_command(root, summary_a),
                now=51,
            )
        assert (
            connector.execute_affected(
                "UPDATE catalog_source_build_base_publication_commits "
                "SET base_receipt_id = %s WHERE build_id = %s",
                (b"2" * 16, build_c),
            )
            == 1
        )
        replayed_build = _handoff_snapshot_command(
            connector,
            gate,
            replay_turn,
            _snapshot_command(root, summary_a),
            now=52,
        )
        assert replayed_build == build_c
    finally:
        connector.close()


def test_abandoned_snapshot_recovery_churn_replays_and_resumes_sealed_after_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "snapshot-abandoned-a-b-a-b.sqlite3"
    connector = _generated_database(database_path)
    try:
        gate, base_turn = _authorities(connector)
        root = ("abandoned-cycle",)
        base_summary = SourceBuildManifestSummary.empty()
        base_build = _handoff_snapshot_command(
            connector,
            gate,
            base_turn,
            _snapshot_command(root, base_summary),
            now=20,
        )
        sealed_base = _force_sealed_snapshot_build(
            connector,
            build_id=base_build,
            summary=base_summary,
        )
        base_receipt = b"1" * 16
        _seed_publication_state(
            connector,
            receipt_id=base_receipt,
            candidate_id=b"q" * 16,
            build_id=base_build,
            snapshot=base_summary.manifest_sha256,
            committed_at=sealed_base + 10,
            finalized=True,
        )
        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                base_turn,
                now=30,
            )

        clock = [sealed_base + 100]
        clock_reads = 0

        def database_clock(_work: VNextUnitOfWork) -> int:
            nonlocal clock_reads
            clock_reads += 1
            return clock[0]

        monkeypatch.setattr(
            source_build_module,
            "database_unix_microseconds",
            database_clock,
        )
        summary_a = SourceBuildManifestSummary(b"A" * 32, 1, 0, 0)
        summary_b = SourceBuildManifestSummary(b"B" * 32, 1, 0, 0)

        def claim(token: bytes, now: int) -> IngestTurn:
            with connector.transaction():
                return IngestFenceRepository.claim(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    owner_token=token,
                    now=now,
                    lease_duration=5,
                )

        turn_a0 = claim(b"a" * 16, 31)
        build_a0 = _handoff_snapshot_command(
            connector,
            gate,
            turn_a0,
            _snapshot_command(root, summary_a),
            now=32,
        )
        clock[0] += 1
        turn_b0 = claim(b"b" * 16, 37)
        build_b0 = _handoff_snapshot_command(
            connector,
            gate,
            turn_b0,
            _snapshot_command(root, summary_b),
            now=38,
        )
        clock[0] += 1
        turn_a1 = claim(b"c" * 16, 43)
        build_a1 = _handoff_snapshot_command(
            connector,
            gate,
            turn_a1,
            _snapshot_command(root, summary_a),
            now=44,
        )
        assert len({build_a0, build_b0, build_a1}) == 3
        created_a1 = connector.fetch_one(
            "SELECT created_at FROM catalog_source_build_created_ats "
            "WHERE build_id = %s",
            (build_a1,),
        )[0]
        assert build_a1 == source_build_recovery_identity(
            snapshot_attempt_id=source_build_snapshot_attempt_id(
                source_root_digest(root),
                summary_a,
            ),
            scope=connector.fetch_one(
                "SELECT scope_key FROM catalog_source_build_scope_keys "
                "WHERE build_id = %s",
                (build_a1,),
            )[0],
            manifest_policy_id=1,
            base_receipt_id=base_receipt,
            created_at=created_a1,
        )

        # One empty intervening ingest generation must not perturb the exact
        # live recovery incarnation.
        claim(b"d" * 16, 49)
        turn_a_resume = claim(b"e" * 16, 55)
        reads_before_resume = clock_reads
        assert (
            _handoff_snapshot_command(
                connector,
                gate,
                turn_a_resume,
                _snapshot_command(root, summary_a),
                now=56,
            )
            == build_a1
        )
        assert clock_reads == reads_before_resume
        # Same-generation response loss also returns the exact mapped build.
        assert (
            _handoff_snapshot_command(
                connector,
                gate,
                turn_a_resume,
                _snapshot_command(root, summary_a),
                now=56,
            )
            == build_a1
        )

        clock[0] += 1
        turn_b1 = claim(b"f" * 16, 61)
        build_b1 = _handoff_snapshot_command(
            connector,
            gate,
            turn_b1,
            _snapshot_command(root, summary_b),
            now=62,
        )
        clock[0] += 1
        turn_a2 = claim(b"h" * 16, 67)
        build_a2 = _handoff_snapshot_command(
            connector,
            gate,
            turn_a2,
            _snapshot_command(root, summary_a),
            now=68,
        )
        clock[0] += 1
        turn_b2 = claim(b"j" * 16, 73)
        build_b2 = _handoff_snapshot_command(
            connector,
            gate,
            turn_b2,
            _snapshot_command(root, summary_b),
            now=74,
        )
        assert len({build_a0, build_b0, build_a1, build_b1, build_a2, build_b2}) == 6

        _force_sealed_snapshot_build(
            connector,
            build_id=build_b2,
            summary=summary_b,
        )
        created_b2 = connector.fetch_one(
            "SELECT created_at FROM catalog_source_build_created_ats "
            "WHERE build_id = %s",
            (build_b2,),
        )[0]
        connector.execute(
            "INSERT INTO operational_source_working_builds "
            "(slot, build_id, assigned_at) VALUES (%s, %s, %s)",
            (1, build_b2, created_b2),
        )
        require_source_build_publication_identity(
            connector,
            build_id=build_b2,
            base_receipt_id=base_receipt,
        )

        # A new connection models process restart.  A SEALED-unpublished v3
        # remains the same pipeline authority and is not replaced.
        connector.close()
        connector = SQLiteConnector(str(database_path))
        connector.connect()
        turn_b_resume = claim(b"k" * 16, 79)
        reads_before_resume = clock_reads
        assert (
            _handoff_snapshot_command(
                connector,
                gate,
                turn_b_resume,
                _snapshot_command(root, summary_b),
                now=80,
            )
            == build_b2
        )
        assert clock_reads == reads_before_resume
        assert connector.fetch_one(
            "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
            (build_b2,),
        ) == ("SEALED",)

        assert (
            connector.execute_affected(
                "UPDATE catalog_source_build_created_ats SET created_at = %s "
                "WHERE build_id = %s",
                (created_b2 + 1, build_b2),
            )
            == 1
        )
        with pytest.raises(SourceBuildConflictError, match="identity|snapshot|base"):
            require_source_build_publication_identity(
                connector,
                build_id=build_b2,
                base_receipt_id=base_receipt,
            )
        connector.execute(
            "UPDATE catalog_source_build_created_ats SET created_at = %s "
            "WHERE build_id = %s",
            (created_b2, build_b2),
        )
        assert (
            connector.execute_affected(
                "DELETE FROM catalog_source_build_base_publication_commits "
                "WHERE build_id = %s",
                (build_b2,),
            )
            == 1
        )
        with pytest.raises(SourceBuildConflictError, match="identity|snapshot|base"):
            require_source_build_publication_identity(
                connector,
                build_id=build_b2,
                base_receipt_id=None,
            )
        connector.execute(
            "INSERT INTO catalog_source_build_base_publication_commits "
            "(build_id, base_receipt_id) VALUES (%s, %s)",
            (build_b2, base_receipt),
        )
    finally:
        connector.close()


def test_recovery_clock_collision_is_zero_write_then_self_heals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "snapshot-recovery-clock.sqlite3")
    try:
        gate, base_turn = _authorities(connector)
        root = ("clock-cycle",)
        base_summary = SourceBuildManifestSummary.empty()
        base_build = _handoff_snapshot_command(
            connector,
            gate,
            base_turn,
            _snapshot_command(root, base_summary),
            now=20,
        )
        sealed_base = _force_sealed_snapshot_build(
            connector,
            build_id=base_build,
            summary=base_summary,
        )
        _seed_publication_state(
            connector,
            receipt_id=b"1" * 16,
            candidate_id=b"q" * 16,
            build_id=base_build,
            snapshot=base_summary.manifest_sha256,
            committed_at=sealed_base + 10,
            finalized=True,
        )
        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                base_turn,
                now=30,
            )

        clock = [sealed_base + 100]
        monkeypatch.setattr(
            source_build_module,
            "database_unix_microseconds",
            lambda _work: clock[0],
        )
        summary_a = SourceBuildManifestSummary(b"A" * 32, 1, 0, 0)
        summary_b = SourceBuildManifestSummary(b"B" * 32, 1, 0, 0)

        def claim(token: bytes, now: int) -> IngestTurn:
            with connector.transaction():
                return IngestFenceRepository.claim(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    owner_token=token,
                    now=now,
                    lease_duration=5,
                )

        turn_a0 = claim(b"a" * 16, 31)
        build_a0 = _handoff_snapshot_command(
            connector,
            gate,
            turn_a0,
            _snapshot_command(root, summary_a),
            now=32,
        )
        clock[0] += 1
        turn_b0 = claim(b"b" * 16, 37)
        _handoff_snapshot_command(
            connector,
            gate,
            turn_b0,
            _snapshot_command(root, summary_b),
            now=38,
        )
        clock[0] += 1
        turn_a1 = claim(b"c" * 16, 43)
        build_a1 = _handoff_snapshot_command(
            connector,
            gate,
            turn_a1,
            _snapshot_command(root, summary_a),
            now=44,
        )
        collision_clock = clock[0]
        with connector.transaction():
            SourceBuildRepository.abandon(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn_a1,
                build_id=build_a1,
                now=45,
            )

        turn_retry = claim(b"d" * 16, 49)
        seed_manifest_policy(
            connector,
            manifest_policy_id=2,
            manifest_algorithm_version=2,
        )
        assert (
            connector.execute_affected(
                "UPDATE catalog_source_build_manifest_policy_ids "
                "SET manifest_policy_id = %s WHERE build_id = %s",
                (2, build_a0),
            )
            == 1
        )
        with pytest.raises(SourceBuildConflictError, match="canonical.*immutable"):
            _handoff_snapshot_command(
                connector,
                gate,
                turn_retry,
                _snapshot_command(root, summary_a),
                now=50,
            )
        connector.execute(
            "UPDATE catalog_source_build_manifest_policy_ids "
            "SET manifest_policy_id = %s WHERE build_id = %s",
            (1, build_a0),
        )
        assert (
            connector.execute_affected(
                "DELETE FROM catalog_source_build_channel WHERE build_id = %s",
                (build_a0,),
            )
            == 1
        )
        with pytest.raises(SourceBuildConflictError, match="channel"):
            _handoff_snapshot_command(
                connector,
                gate,
                turn_retry,
                _snapshot_command(root, summary_a),
                now=50,
            )
        connector.execute(
            "INSERT INTO catalog_source_build_channel (build_id, channel) "
            "VALUES (%s, %s)",
            (build_a0, b"default"),
        )
        assert (
            connector.execute_affected(
                "DELETE FROM catalog_source_build_base_publication_commits "
                "WHERE build_id = %s",
                (build_a0,),
            )
            == 1
        )
        with pytest.raises(SourceBuildConflictError, match="base"):
            _handoff_snapshot_command(
                connector,
                gate,
                turn_retry,
                _snapshot_command(root, summary_a),
                now=50,
            )
        connector.execute(
            "INSERT INTO catalog_source_build_base_publication_commits "
            "(build_id, base_receipt_id) VALUES (%s, %s)",
            (build_a0, b"1" * 16),
        )
        assert (
            connector.execute_affected(
                "UPDATE catalog_source_build_manifest_policy_ids "
                "SET manifest_policy_id = %s WHERE build_id = %s",
                (2, build_a1),
            )
            == 1
        )
        with pytest.raises(SourceBuildConflictError, match="recovery.*immutable"):
            _handoff_snapshot_command(
                connector,
                gate,
                turn_retry,
                _snapshot_command(root, summary_a),
                now=50,
            )
        connector.execute(
            "UPDATE catalog_source_build_manifest_policy_ids "
            "SET manifest_policy_id = %s WHERE build_id = %s",
            (1, build_a1),
        )
        assert (
            connector.execute_affected(
                "UPDATE catalog_source_build_created_ats SET created_at = %s "
                "WHERE build_id = %s",
                (collision_clock + 1, build_a1),
            )
            == 1
        )
        with pytest.raises(SourceBuildConflictError, match="recovery.*immutable"):
            _handoff_snapshot_command(
                connector,
                gate,
                turn_retry,
                _snapshot_command(root, summary_a),
                now=50,
            )
        connector.execute(
            "UPDATE catalog_source_build_created_ats SET created_at = %s "
            "WHERE build_id = %s",
            (collision_clock, build_a1),
        )
        assert (
            connector.execute_affected(
                "DELETE FROM catalog_source_build_channel WHERE build_id = %s",
                (build_a1,),
            )
            == 1
        )
        with pytest.raises(SourceBuildConflictError, match="channel"):
            _handoff_snapshot_command(
                connector,
                gate,
                turn_retry,
                _snapshot_command(root, summary_a),
                now=50,
            )
        connector.execute(
            "INSERT INTO catalog_source_build_channel (build_id, channel) "
            "VALUES (%s, %s)",
            (build_a1, b"default"),
        )
        assert (
            connector.execute_affected(
                "DELETE FROM catalog_source_build_base_publication_commits "
                "WHERE build_id = %s",
                (build_a1,),
            )
            == 1
        )
        with pytest.raises(SourceBuildConflictError, match="base"):
            _handoff_snapshot_command(
                connector,
                gate,
                turn_retry,
                _snapshot_command(root, summary_a),
                now=50,
            )
        connector.execute(
            "INSERT INTO catalog_source_build_base_publication_commits "
            "(build_id, base_receipt_id) VALUES (%s, %s)",
            (build_a1, b"1" * 16),
        )
        anchors_before = connector.fetch_all(
            "SELECT build_id FROM catalog_source_build_anchors ORDER BY build_id"
        )
        mappings_before = connector.fetch_all(
            "SELECT build_id, generation FROM operational_source_build_generations "
            "ORDER BY generation"
        )
        with pytest.raises(SourceBuildNotReadyError, match="clock.*ABANDONED"):
            _handoff_snapshot_command(
                connector,
                gate,
                turn_retry,
                _snapshot_command(root, summary_a),
                now=50,
            )
        assert (
            connector.fetch_all(
                "SELECT build_id FROM catalog_source_build_anchors ORDER BY build_id"
            )
            == anchors_before
        )
        assert (
            connector.fetch_all(
                "SELECT build_id, generation FROM operational_source_build_generations "
                "ORDER BY generation"
            )
            == mappings_before
        )
        assert (
            connector.fetch_all("SELECT * FROM operational_source_working_builds") == []
        )

        clock[0] = collision_clock + 1_000
        build_a2 = _handoff_snapshot_command(
            connector,
            gate,
            turn_retry,
            _snapshot_command(root, summary_a),
            now=50,
        )
        assert build_a2 != build_a1
        assert connector.fetch_one(
            "SELECT created_at FROM catalog_source_build_created_ats "
            "WHERE build_id = %s",
            (build_a2,),
        ) == (clock[0],)
    finally:
        connector.close()


def test_analysis_abandonment_recovery_repeats_v3_and_self_heals_after_clock_tick(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "analysis-recovery-lifecycle.sqlite3")
    try:
        root = ("analysis-recovery",)
        gate, sealed_base, base_receipt_id = _analysis_ready_published_base(
            connector,
            root=root,
        )
        clock = [sealed_base + 100]
        monkeypatch.setattr(
            source_build_module,
            "database_unix_microseconds",
            lambda _work: clock[0],
        )
        summary = SourceBuildManifestSummary(b"A" * 32, 1, 0, 0)
        command = _snapshot_command(root, summary)

        def claim(token: bytes, now: int) -> IngestTurn:
            with connector.transaction():
                return IngestFenceRepository.claim(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    owner_token=token,
                    now=now,
                    lease_duration=100,
                )

        canonical_turn = claim(b"a" * 16, 31)
        canonical_build = _handoff_snapshot_command(
            connector,
            gate,
            canonical_turn,
            command,
            now=32,
        )
        _force_sealed_snapshot_build(
            connector,
            build_id=canonical_build,
            summary=summary,
        )
        _restore_sealed_source_working(connector, build_id=canonical_build)
        with connector.transaction():
            canonical_analysis = AnalysisRepository.begin(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=canonical_turn,
                build_id=canonical_build,
                policy_id=1,
                proposed_analysis_id=b"A" * 16,
                now=36,
            )
        assert canonical_analysis.state == "OPEN" and not canonical_analysis.replayed
        with connector.transaction():
            abandoned_canonical = AnalysisRepository.abandon(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=canonical_turn,
                analysis_id=canonical_analysis.analysis_id,
                now=37,
            )
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                canonical_turn,
                now=38,
            )
        assert abandoned_canonical.state == "ABANDONED"
        assert not abandoned_canonical.replayed
        assert (
            connector.fetch_all("SELECT * FROM operational_source_working_builds") == []
        )

        recovery_turn = claim(b"b" * 16, 39)
        recovery_build = _handoff_snapshot_command(
            connector,
            gate,
            recovery_turn,
            command,
            now=40,
        )
        assert recovery_build != canonical_build
        assert recovery_build == source_build_recovery_identity(
            snapshot_attempt_id=command.build_attempt_id,
            scope=connector.fetch_one(
                "SELECT scope_key FROM catalog_source_build_scope_keys "
                "WHERE build_id = %s",
                (recovery_build,),
            )[0],
            manifest_policy_id=1,
            base_receipt_id=base_receipt_id,
            created_at=clock[0],
        )
        _force_sealed_snapshot_build(
            connector,
            build_id=recovery_build,
            summary=summary,
        )
        _restore_sealed_source_working(connector, build_id=recovery_build)
        with connector.transaction():
            recovery_analysis = AnalysisRepository.begin(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=recovery_turn,
                build_id=recovery_build,
                policy_id=1,
                proposed_analysis_id=b"B" * 16,
                now=44,
            )
        assert recovery_analysis.build_id == recovery_build
        assert recovery_analysis.state == "OPEN" and not recovery_analysis.replayed
        with connector.transaction():
            abandoned_recovery = AnalysisRepository.abandon(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=recovery_turn,
                analysis_id=recovery_analysis.analysis_id,
                now=45,
            )
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                recovery_turn,
                now=46,
            )
        assert abandoned_recovery.state == "ABANDONED"
        assert not abandoned_recovery.replayed

        retry_turn = claim(b"c" * 16, 47)
        with command.prepare_root_upload() as root_plan:
            _upload(connector, gate, retry_turn, root_plan, now=48)
            before = tuple(connector.connection.iterdump())
            with (
                connector.transaction(),
                pytest.raises(
                    SourceBuildNotReadyError,
                    match="clock.*analysis-retired",
                ),
            ):
                SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=retry_turn,
                    command=command,
                    root_plan=root_plan,
                    now=51,
                )
            assert tuple(connector.connection.iterdump()) == before

            clock[0] += 1
            with connector.transaction():
                next_handoff = SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=retry_turn,
                    command=command,
                    root_plan=root_plan,
                    now=52,
                )
        assert next_handoff.build_id not in {canonical_build, recovery_build}
        assert not next_handoff.replayed
        _force_sealed_snapshot_build(
            connector,
            build_id=next_handoff.build_id,
            summary=summary,
        )
        _restore_sealed_source_working(connector, build_id=next_handoff.build_id)
        with connector.transaction():
            next_analysis = AnalysisRepository.begin(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=retry_turn,
                build_id=next_handoff.build_id,
                policy_id=1,
                proposed_analysis_id=b"C" * 16,
                now=53,
            )
        assert next_analysis.build_id == next_handoff.build_id
        assert next_analysis.state == "OPEN" and not next_analysis.replayed
    finally:
        connector.close()


@pytest.mark.parametrize("analysis_state", (None, "OPEN", "COMPLETE"))
def test_sealed_v3_without_working_root_fails_closed_for_missing_or_live_analysis(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    analysis_state: str | None,
) -> None:
    connector = _generated_database(
        tmp_path
        / f"analysis-recovery-lost-working-{(analysis_state or 'none').lower()}.sqlite3"
    )
    try:
        root = ("analysis-lost-working",)
        gate, sealed_base, _base_receipt_id = _analysis_ready_published_base(
            connector,
            root=root,
        )
        clock = [sealed_base + 100]
        monkeypatch.setattr(
            source_build_module,
            "database_unix_microseconds",
            lambda _work: clock[0],
        )
        summary = SourceBuildManifestSummary(b"L" * 32, 1, 0, 0)
        command = _snapshot_command(root, summary)

        def claim(token: bytes, now: int, lease_duration: int) -> IngestTurn:
            with connector.transaction():
                return IngestFenceRepository.claim(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    owner_token=token,
                    now=now,
                    lease_duration=lease_duration,
                )

        canonical_turn = claim(b"d" * 16, 31, 100)
        canonical_build = _handoff_snapshot_command(
            connector,
            gate,
            canonical_turn,
            command,
            now=32,
        )
        _force_sealed_snapshot_build(
            connector,
            build_id=canonical_build,
            summary=summary,
        )
        _restore_sealed_source_working(connector, build_id=canonical_build)
        with connector.transaction():
            canonical_analysis = AnalysisRepository.begin(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=canonical_turn,
                build_id=canonical_build,
                policy_id=1,
                proposed_analysis_id=b"D" * 16,
                now=36,
            )
            AnalysisRepository.abandon(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=canonical_turn,
                analysis_id=canonical_analysis.analysis_id,
                now=37,
            )
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                canonical_turn,
                now=38,
            )

        recovery_turn = claim(b"e" * 16, 39, 20)
        recovery_build = _handoff_snapshot_command(
            connector,
            gate,
            recovery_turn,
            command,
            now=40,
        )
        assert recovery_build != canonical_build
        _force_sealed_snapshot_build(
            connector,
            build_id=recovery_build,
            summary=summary,
        )
        _restore_sealed_source_working(connector, build_id=recovery_build)
        live_analysis_id: bytes | None = None
        with connector.transaction():
            if analysis_state is not None:
                live_analysis = AnalysisRepository.begin(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=recovery_turn,
                    build_id=recovery_build,
                    policy_id=1,
                    proposed_analysis_id=b"E" * 16,
                    now=44,
                )
                live_analysis_id = live_analysis.analysis_id
            if analysis_state == "COMPLETE" and live_analysis_id is not None:
                started_at = connector.fetch_one(
                    "SELECT started_at FROM catalog_analysis_run_started_ats "
                    "WHERE analysis_id = %s",
                    (live_analysis_id,),
                )[0]
                complete_analysis_run(
                    connector,
                    analysis_id=live_analysis_id,
                    completed_at=int(started_at) + 1,
                )
            assert (
                connector.execute_affected(
                    "DELETE FROM operational_source_working_builds "
                    "WHERE slot = %s AND build_id = %s",
                    (1, recovery_build),
                )
                == 1
            )

        retry_turn = claim(b"f" * 16, 70, 100)
        with command.prepare_root_upload() as root_plan:
            _upload(connector, gate, retry_turn, root_plan, now=71)
            before = tuple(connector.connection.iterdump())
            with (
                connector.transaction(),
                pytest.raises(
                    SourceBuildConflictError,
                    match="working|authority",
                ),
            ):
                SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=retry_turn,
                    command=command,
                    root_plan=root_plan,
                    now=74,
                )
            assert tuple(connector.connection.iterdump()) == before
        if live_analysis_id is None:
            assert connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_analysis_runs WHERE build_id = %s",
                (recovery_build,),
            ) == (0,)
        else:
            assert connector.fetch_one(
                "SELECT state FROM catalog_analysis_run_states "
                "WHERE analysis_id = %s",
                (live_analysis_id,),
            ) == (analysis_state,)
        assert (
            connector.fetch_all("SELECT * FROM operational_source_working_builds") == []
        )
    finally:
        connector.close()


def test_different_root_snapshot_takeover_abandons_open_working_build(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "snapshot-root-takeover.sqlite3")
    try:
        gate, first_turn = _authorities(connector)
        summary = SourceBuildManifestSummary(b"A" * 32, 1, 0, 0)
        first = _handoff_snapshot_command(
            connector,
            gate,
            first_turn,
            _snapshot_command(("first-root",), summary),
            now=20,
        )
        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                first_turn,
                now=30,
            )
            second_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"n" * 16,
                now=31,
                lease_duration=100,
            )
        second = _handoff_snapshot_command(
            connector,
            gate,
            second_turn,
            _snapshot_command(("second-root",), summary),
            now=32,
        )
        assert second != first
        assert connector.fetch_one(
            "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
            (first,),
        ) == ("ABANDONED",)
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_working_builds WHERE slot = 1"
        ) == (second,)
    finally:
        connector.close()


def test_sealed_unpublished_legacy_working_build_resumes_after_upgrade(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "sealed-legacy-upgrade.sqlite3")
    try:
        gate, base_turn = _authorities(connector)
        summary = SourceBuildManifestSummary.empty()
        base_build = _handoff_snapshot_command(
            connector,
            gate,
            base_turn,
            _snapshot_command(("legacy-base",), summary),
            now=20,
        )
        sealed_base = _force_sealed_snapshot_build(
            connector,
            build_id=base_build,
            summary=summary,
        )
        base_receipt = b"1" * 16
        _seed_publication_state(
            connector,
            receipt_id=base_receipt,
            candidate_id=b"q" * 16,
            build_id=base_build,
            snapshot=summary.manifest_sha256,
            committed_at=sealed_base + 10,
            finalized=True,
        )
        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                base_turn,
                now=30,
            )
            legacy_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"l" * 16,
                now=31,
                lease_duration=100,
            )

        root = ("legacy-upgrade",)
        with SourceDiscoveryPlan.from_locators(()) as plan:
            legacy_digest = sha256(b"h2hdb-vnext-ingest-source-build-attempt-v1\0")
            legacy_digest.update(source_root_digest(root))
            legacy_digest.update((0).to_bytes(8, "big"))
            legacy_digest.update(plan.tree_observation_sha256)
            legacy_digest.update((1).to_bytes(8, "big"))
            legacy_build = legacy_digest.digest()[:16]
            assert (
                _handoff_snapshot_command(
                    connector,
                    gate,
                    legacy_turn,
                    SourceRootBuildCommand(root, legacy_build),
                    now=32,
                )
                == legacy_build
            )
            legacy_created_at = connector.fetch_one(
                "SELECT created_at FROM catalog_source_build_created_ats "
                "WHERE build_id = %s",
                (legacy_build,),
            )[0]
            connector.execute(
                "UPDATE catalog_publication_commit_committed_ats "
                "SET committed_at = %s WHERE receipt_id = %s",
                (legacy_created_at - 1, base_receipt),
            )
            batch = SourceBuildRepository.prepare_discovery_batch(
                connector,
                build_id=legacy_build,
                plan=plan,
            )
            with connector.transaction():
                receipt = SourceBuildRepository.commit_discovery_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=legacy_turn,
                    batch=batch,
                    resolved=(),
                    now=40,
                )
            assert receipt.terminal
        with connector.transaction():
            sealed = SourceBuildRepository.assemble_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=legacy_turn,
                build_id=legacy_build,
                attempt=SourceBuildRepository.issue_assembly_batch(),
                now=50,
            )
        assert sealed.terminal
        sealed_at = connector.fetch_one(
            "SELECT sealed_at FROM catalog_source_build_sealed_ats WHERE build_id = %s",
            (legacy_build,),
        )[0]
        analysis_id = b"L" * 16
        _seed_complete_analysis_for_build(
            connector,
            analysis_id=analysis_id,
            build_id=legacy_build,
            timestamp=sealed_at + 1,
        )

        with connector.transaction():
            resumed_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"r" * 16,
                now=132,
                lease_duration=100,
            )
        assert (
            _handoff_snapshot_command(
                connector,
                gate,
                resumed_turn,
                _snapshot_command(root, summary),
                now=133,
            )
            == legacy_build
        )
        assert connector.fetch_one(
            "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
            (legacy_build,),
        ) == ("SEALED",)
        assert connector.fetch_one(
            "SELECT build_id, state FROM catalog_analysis_runs WHERE analysis_id = %s",
            (analysis_id,),
        ) == (legacy_build, "COMPLETE")
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_working_builds WHERE slot = 1"
        ) == (legacy_build,)
    finally:
        connector.close()


@pytest.mark.parametrize("corruption", ["deleted", "foreign"])
def test_legacy_snapshot_reuse_requires_exact_historical_base_head(
    tmp_path: Path,
    corruption: str,
) -> None:
    connector = _generated_database(tmp_path / f"legacy-base-{corruption}.sqlite3")
    try:
        gate, turn_zero = _authorities(connector)
        summary = SourceBuildManifestSummary.empty()

        build_zero = _handoff_snapshot_command(
            connector,
            gate,
            turn_zero,
            _snapshot_command(("history-zero",), summary),
            now=20,
        )
        sealed_zero = _force_sealed_snapshot_build(
            connector,
            build_id=build_zero,
            summary=summary,
        )
        receipt_zero = b"0" * 16
        _seed_publication_state(
            connector,
            receipt_id=receipt_zero,
            candidate_id=b"p" * 16,
            build_id=build_zero,
            snapshot=summary.manifest_sha256,
            committed_at=sealed_zero + 10,
            finalized=True,
        )
        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                turn_zero,
                now=30,
            )
            turn_one = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"j" * 16,
                now=31,
                lease_duration=1_000_000,
            )

        build_one = _handoff_snapshot_command(
            connector,
            gate,
            turn_one,
            _snapshot_command(("history-one",), summary),
            now=32,
        )
        sealed_one = _force_sealed_snapshot_build(
            connector,
            build_id=build_one,
            summary=summary,
        )
        receipt_one = b"1" * 16
        _seed_publication_state(
            connector,
            receipt_id=receipt_one,
            candidate_id=b"q" * 16,
            build_id=build_one,
            snapshot=summary.manifest_sha256,
            committed_at=sealed_one + 10,
            finalized=True,
            revision=2,
            source_revision=2,
            generation=2,
        )
        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                turn_one,
                now=40,
            )
            legacy_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"k" * 16,
                now=41,
                lease_duration=1_000_000,
            )

        legacy_root = ("legacy-successor",)
        with SourceDiscoveryPlan.from_locators(()) as discovery:
            legacy_digest = sha256(b"h2hdb-vnext-ingest-source-build-attempt-v1\0")
            legacy_digest.update(source_root_digest(legacy_root))
            legacy_digest.update((0).to_bytes(8, "big"))
            legacy_digest.update(discovery.tree_observation_sha256)
            legacy_digest.update((1).to_bytes(8, "big"))
            legacy_build = legacy_digest.digest()[:16]
            legacy_command = SourceRootBuildCommand(legacy_root, legacy_build)
            assert (
                _handoff_snapshot_command(
                    connector,
                    gate,
                    legacy_turn,
                    legacy_command,
                    now=42,
                )
                == legacy_build
            )
            assert connector.fetch_one(
                "SELECT base_receipt_id FROM "
                "catalog_source_build_base_publication_commits "
                "WHERE build_id = %s",
                (legacy_build,),
            ) == (receipt_one,)

            legacy_created_at = require_int63(
                connector.fetch_one(
                    "SELECT created_at FROM catalog_source_build_created_ats "
                    "WHERE build_id = %s",
                    (legacy_build,),
                )[0],
                field="legacy fixture created_at",
            )
            assert (
                connector.execute_affected(
                    "UPDATE catalog_publication_commit_committed_ats "
                    "SET committed_at = %s WHERE receipt_id = %s",
                    (legacy_created_at - 2, receipt_zero),
                )
                == 1
            )
            assert (
                connector.execute_affected(
                    "UPDATE catalog_publication_commit_committed_ats "
                    "SET committed_at = %s WHERE receipt_id = %s",
                    (legacy_created_at - 1, receipt_one),
                )
                == 1
            )

            batch = SourceBuildRepository.prepare_discovery_batch(
                connector,
                build_id=legacy_build,
                plan=discovery,
            )
            with connector.transaction():
                discovery_receipt = SourceBuildRepository.commit_discovery_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=legacy_turn,
                    batch=batch,
                    resolved=(),
                    now=50,
                )
            assert discovery_receipt.terminal
        with connector.transaction():
            assembly_receipt = SourceBuildRepository.assemble_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=legacy_turn,
                build_id=legacy_build,
                attempt=SourceBuildRepository.issue_assembly_batch(),
                now=51,
            )
        assert assembly_receipt.terminal
        assert (
            connector.execute_affected(
                "DELETE FROM operational_source_working_builds WHERE build_id = %s",
                (legacy_build,),
            )
            == 1
        )
        receipt_legacy = b"2" * 16
        candidate_legacy = b"r" * 16
        _seed_publication_state(
            connector,
            receipt_id=receipt_legacy,
            candidate_id=candidate_legacy,
            build_id=legacy_build,
            snapshot=summary.manifest_sha256,
            committed_at=legacy_created_at + 10,
            finalized=True,
            revision=3,
            source_revision=3,
            generation=3,
        )
        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                legacy_turn,
                now=60,
            )

        if corruption == "deleted":
            assert (
                connector.execute_affected(
                    "DELETE FROM catalog_source_build_base_publication_commits "
                    "WHERE build_id = %s",
                    (legacy_build,),
                )
                == 1
            )
            assert (
                connector.execute_affected(
                    "DELETE FROM "
                    "catalog_publication_candidate_base_publication_commits "
                    "WHERE candidate_id = %s",
                    (candidate_legacy,),
                )
                == 1
            )
            expected_base: tuple[bytes, ...] = ()
        else:
            assert (
                connector.execute_affected(
                    "UPDATE catalog_source_build_base_publication_commits "
                    "SET base_receipt_id = %s WHERE build_id = %s",
                    (receipt_zero, legacy_build),
                )
                == 1
            )
            assert (
                connector.execute_affected(
                    "UPDATE "
                    "catalog_publication_candidate_base_publication_commits "
                    "SET base_receipt_id = %s WHERE candidate_id = %s",
                    (receipt_zero, candidate_legacy),
                )
                == 1
            )
            expected_base = (receipt_zero,)

        with connector.transaction():
            replay_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"l" * 16,
                now=61,
                lease_duration=1_000_000,
            )
        with pytest.raises(SourceBuildConflictError, match="legacy|base|creation"):
            _handoff_snapshot_command(
                connector,
                gate,
                replay_turn,
                _snapshot_command(legacy_root, summary),
                now=62,
            )
        assert (
            connector.fetch_one(
                "SELECT base_receipt_id FROM "
                "catalog_source_build_base_publication_commits WHERE build_id = %s",
                (legacy_build,),
            )
            == expected_base
        )
    finally:
        connector.close()


def test_handoff_pins_common_commit_and_replay_ignores_later_head_advance(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "handoff-base-replay.sqlite3")
    try:
        gate, turn = _authorities(connector)
        command = SourceRootBuildCommand((), b"b" * 16)
        with command.prepare_root_upload() as root:
            _upload(connector, gate, turn, root, now=12)
            seed_snapshot_manifest(
                connector,
                snapshot_manifest_sha256=root.value_sha256,
                gallery_count=0,
                file_count=0,
                byte_count=0,
            )
            _published_commit(
                connector,
                receipt_id=b"1" * 16,
                candidate_id=b"c" * 16,
                revision=1,
                source_revision=1,
                generation=1,
                snapshot=root.value_sha256,
                committed_at=15,
            )

            with connector.transaction():
                first = SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    command=command,
                    root_plan=root,
                    now=24,
                )
            assert not first.replayed
            assert connector.fetch_one(
                "SELECT build_id, base_receipt_id "
                "FROM catalog_source_build_base_publication_commits"
            ) == (command.build_attempt_id, b"1" * 16)
            assert connector.fetch_one(
                "SELECT build_id, base_source_revision, base_source_generation "
                "FROM catalog_source_build_base_source"
            ) == (command.build_attempt_id, 1, 1)

            _published_commit(
                connector,
                receipt_id=b"2" * 16,
                candidate_id=b"d" * 16,
                revision=2,
                source_revision=2,
                generation=2,
                snapshot=root.value_sha256,
                committed_at=25,
            )

            with command.prepare_root_upload() as replay_root:
                _upload(connector, gate, turn, replay_root, now=26)
                with connector.transaction():
                    replay = SourceBuildRepository.handoff_root(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        command=command,
                        root_plan=replay_root,
                        now=29,
                    )
            assert replay.replayed and replay.build_id == first.build_id
            assert connector.fetch_all(
                "SELECT build_id, base_receipt_id "
                "FROM catalog_source_build_base_publication_commits"
            ) == [(command.build_attempt_id, b"1" * 16)]
    finally:
        connector.close()


def test_successor_generation_reuse_does_not_rebase_existing_sealed_build(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "handoff-sealed-successor.sqlite3")
    try:
        gate, first_turn, command = _open_build(connector)
        with SourceDiscoveryPlan.from_locators(()) as plan:
            assert (
                _finish_discovery(
                    connector,
                    gate,
                    first_turn,
                    plan,
                    now=30,
                )
                == ()
            )
        with connector.transaction():
            sealed = SourceBuildRepository.assemble_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=first_turn,
                build_id=command.build_attempt_id,
                attempt=SourceBuildRepository.issue_assembly_batch(),
                now=40,
            )
        assert sealed.terminal
        assert (
            connector.fetch_all(
                "SELECT build_id, base_receipt_id "
                "FROM catalog_source_build_base_publication_commits"
            )
            == []
        )

        snapshot = command.source_root_sha256
        seed_snapshot_manifest(
            connector,
            snapshot_manifest_sha256=snapshot,
            gallery_count=0,
            file_count=0,
            byte_count=0,
        )
        _published_commit(
            connector,
            receipt_id=b"1" * 16,
            candidate_id=b"c" * 16,
            revision=1,
            source_revision=1,
            generation=1,
            snapshot=snapshot,
            committed_at=50,
        )
        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                first_turn,
                now=51,
            )
        with connector.transaction():
            second_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"j" * 16,
                now=52,
                lease_duration=1_000_000,
            )

        with command.prepare_root_upload() as replay_root:
            _upload(connector, gate, second_turn, replay_root, now=53)
            with connector.transaction():
                successor = SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=second_turn,
                    command=command,
                    root_plan=replay_root,
                    now=56,
                )

        assert successor.build_id == command.build_attempt_id
        assert successor.generation == second_turn.generation
        assert (
            connector.fetch_all(
                "SELECT build_id, base_receipt_id "
                "FROM catalog_source_build_base_publication_commits"
            )
            == []
        )
        assert (
            connector.fetch_all(
                "SELECT build_id, base_source_revision, base_source_generation "
                "FROM catalog_source_build_base_source"
            )
            == []
        )
    finally:
        connector.close()


def _sealed_published_source(
    connector: SQLiteConnector,
    *,
    finalized: bool,
) -> tuple[GateLease, SourceRootBuildCommand, bytes]:
    gate, first_turn, command = _open_build(connector)
    with SourceDiscoveryPlan.from_locators(()) as plan:
        assert _finish_discovery(connector, gate, first_turn, plan, now=30) == ()
    with connector.transaction():
        sealed = SourceBuildRepository.assemble_batch(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=first_turn,
            build_id=command.build_attempt_id,
            attempt=SourceBuildRepository.issue_assembly_batch(),
            now=40,
        )
    assert sealed.terminal
    sealed_at = connector.fetch_one(
        "SELECT sealed_at FROM catalog_source_build_sealed_ats WHERE build_id = %s",
        (command.build_attempt_id,),
    )[0]
    assert (
        connector.execute_affected(
            "DELETE FROM operational_source_working_builds "
            "WHERE slot = 1 AND build_id = %s",
            (command.build_attempt_id,),
        )
        == 1
    )
    receipt_id = b"p" * 16
    _seed_publication_state(
        connector,
        receipt_id=receipt_id,
        candidate_id=b"c" * 16,
        build_id=command.build_attempt_id,
        snapshot=command.source_root_sha256,
        committed_at=sealed_at + 10,
        finalized=finalized,
    )
    with connector.transaction():
        IngestFenceRepository.complete(
            VNextUnitOfWork(connector, backend="sqlite"),
            first_turn,
            now=45,
        )
    return gate, command, receipt_id


def _reservation_snapshot(connector: SQLiteConnector) -> tuple[Any, ...]:
    return (
        connector.fetch_all(
            "SELECT slot, build_id, assigned_at "
            "FROM operational_source_working_builds ORDER BY slot"
        ),
        connector.fetch_all(
            "SELECT generation, build_id "
            "FROM operational_source_build_generations ORDER BY generation"
        ),
        connector.fetch_all(
            "SELECT build_id, state FROM catalog_source_build_states ORDER BY build_id"
        ),
        connector.fetch_all(
            "SELECT build_id, base_receipt_id "
            "FROM catalog_source_build_base_publication_commits ORDER BY build_id"
        ),
    )


def test_handoff_atomically_replaces_finalized_head_replay_left_by_expired_turn(
    tmp_path: Path,
) -> None:
    connector = _generated_database(
        tmp_path / "handoff-finalized-replay-recovery.sqlite3"
    )
    try:
        gate, published, receipt_id = _sealed_published_source(
            connector,
            finalized=True,
        )
        with connector.transaction():
            replay_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"r" * 16,
                now=46,
                lease_duration=10,
            )
        with published.prepare_root_upload() as replay_root:
            _upload(connector, gate, replay_turn, replay_root, now=47)
            with connector.transaction():
                replay = SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=replay_turn,
                    command=published,
                    root_plan=replay_root,
                    now=50,
                )
        assert replay.generation == 2
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_working_builds WHERE slot = 1"
        ) == (published.build_attempt_id,)

        # Generation 2 loses its response/turn before terminal publication replay
        # can release the transient source root.  Its lease is the fencing proof;
        # generation 3 may recover only this exact finalized common-head build.
        with connector.transaction():
            replacement_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"n" * 16,
                now=56,
                lease_duration=100,
            )
        replacement = SourceRootBuildCommand(("replacement",), b"n" * 16)
        with replacement.prepare_root_upload() as replacement_root:
            _upload(connector, gate, replacement_turn, replacement_root, now=57)
            before_fault = _reservation_snapshot(connector)
            original_execute = connector.execute
            original_execute_affected = connector.execute_affected

            def fail_replacement_write(
                query: str,
                data: tuple[Any, ...] = (),
            ) -> None:
                if (
                    "operational_source_working_builds" in query
                    and replacement.build_attempt_id in data
                    and query.lstrip().upper().startswith(("INSERT", "UPDATE"))
                ):
                    raise RuntimeError("injected replacement working-root fault")
                original_execute(query, data)

            def fail_replacement_write_affected(
                query: str,
                data: tuple[Any, ...] = (),
            ) -> int:
                if (
                    "operational_source_working_builds" in query
                    and replacement.build_attempt_id in data
                    and query.lstrip().upper().startswith(("INSERT", "UPDATE"))
                ):
                    raise RuntimeError("injected replacement working-root fault")
                return original_execute_affected(query, data)

            with (
                patch.object(connector, "execute", side_effect=fail_replacement_write),
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=fail_replacement_write_affected,
                ),
                pytest.raises(RuntimeError, match="injected replacement"),
            ):
                with connector.transaction():
                    SourceBuildRepository.handoff_root(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=replacement_turn,
                        command=replacement,
                        root_plan=replacement_root,
                        now=60,
                    )
            assert _reservation_snapshot(connector) == before_fault

            with connector.transaction():
                recovered = SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=replacement_turn,
                    command=replacement,
                    root_plan=replacement_root,
                    now=61,
                )

        assert not recovered.replayed
        assert recovered.generation == replacement_turn.generation == 3
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_working_builds WHERE slot = 1"
        ) == (replacement.build_attempt_id,)
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (replacement_turn.generation,),
        ) == (replacement.build_attempt_id,)
        assert connector.fetch_one(
            "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
            (published.build_attempt_id,),
        ) == ("SEALED",)
        assert connector.fetch_one(
            "SELECT base_receipt_id "
            "FROM catalog_source_build_base_publication_commits WHERE build_id = %s",
            (replacement.build_attempt_id,),
        ) == (receipt_id,)
    finally:
        connector.close()


def test_handoff_stale_recovery_rejects_candidate_provenance_build_mismatch(
    tmp_path: Path,
) -> None:
    connector = _generated_database(
        tmp_path / "handoff-candidate-provenance-mismatch.sqlite3"
    )
    try:
        gate, published, _receipt_id = _sealed_published_source(
            connector,
            finalized=True,
        )
        with connector.transaction():
            stale_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"s" * 16,
                now=46,
                lease_duration=10,
            )
        foreign = SourceRootBuildCommand(("foreign",), b"f" * 16)
        with foreign.prepare_root_upload() as foreign_root:
            _upload(connector, gate, stale_turn, foreign_root, now=47)
            with connector.transaction():
                SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=stale_turn,
                    command=foreign,
                    root_plan=foreign_root,
                    now=50,
                )
        with SourceDiscoveryPlan.from_locators(()) as plan:
            batch = SourceBuildRepository.prepare_discovery_batch(
                connector,
                build_id=foreign.build_attempt_id,
                plan=plan,
            )
            with connector.transaction():
                SourceBuildRepository.commit_discovery_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=stale_turn,
                    batch=batch,
                    resolved=(),
                    now=51,
                )
        with connector.transaction():
            sealed = SourceBuildRepository.assemble_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=stale_turn,
                build_id=foreign.build_attempt_id,
                attempt=SourceBuildRepository.issue_assembly_batch(),
                now=52,
            )
        assert sealed.terminal

        foreign_analysis = b"x" * 16
        _seed_complete_analysis_for_build(
            connector,
            analysis_id=foreign_analysis,
            build_id=foreign.build_attempt_id,
            timestamp=53,
        )
        assert (
            connector.execute_affected(
                "DELETE FROM catalog_source_revision_provenance "
                "WHERE source_revision = %s",
                (1,),
            )
            == 1
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_provenance "
            "(source_revision, analysis_id) VALUES (%s, %s)",
            (1, foreign_analysis),
        )
        assert connector.fetch_one(
            "SELECT candidate_analysis.build_id, provenance_analysis.build_id "
            "FROM catalog_publication_commit_heads AS head "
            "JOIN catalog_publication_commit_candidates AS committed_candidate "
            "ON committed_candidate.receipt_id = head.receipt_id "
            "JOIN catalog_publication_candidate_analysis_ids AS candidate "
            "ON candidate.candidate_id = committed_candidate.candidate_id "
            "JOIN catalog_analysis_runs AS candidate_analysis "
            "ON candidate_analysis.analysis_id = candidate.analysis_id "
            "JOIN catalog_publication_commit_source_revisions AS source "
            "ON source.receipt_id = head.receipt_id "
            "JOIN catalog_source_revision_provenance AS provenance "
            "ON provenance.source_revision = source.source_revision "
            "JOIN catalog_analysis_runs AS provenance_analysis "
            "ON provenance_analysis.analysis_id = provenance.analysis_id "
            "WHERE head.channel = %s",
            (b"default",),
        ) == (published.build_attempt_id, foreign.build_attempt_id)

        with connector.transaction():
            replacement_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"n" * 16,
                now=56,
                lease_duration=100,
            )
        replacement = SourceRootBuildCommand(("replacement",), b"n" * 16)
        with replacement.prepare_root_upload() as replacement_root:
            _upload(connector, gate, replacement_turn, replacement_root, now=57)
            before = _reservation_snapshot(connector)
            with (
                connector.transaction(),
                pytest.raises(SourceBuildConflictError, match="candidate|provenance"),
            ):
                SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=replacement_turn,
                    command=replacement,
                    root_plan=replacement_root,
                    now=60,
                )
        assert _reservation_snapshot(connector) == before
    finally:
        connector.close()


def test_handoff_does_not_replace_nonfinalized_working_root(
    tmp_path: Path,
) -> None:
    connector = _generated_database(
        tmp_path / "handoff-working-nonfinalized-fail-closed.sqlite3"
    )
    try:
        gate, published, _receipt_id = _sealed_published_source(
            connector,
            finalized=False,
        )
        with connector.transaction():
            stale_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"s" * 16,
                now=46,
                lease_duration=10,
            )
        working = published
        with working.prepare_root_upload() as working_root:
            _upload(connector, gate, stale_turn, working_root, now=47)
            with connector.transaction():
                SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=stale_turn,
                    command=working,
                    root_plan=working_root,
                    now=50,
                )
        with connector.transaction():
            replacement_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"n" * 16,
                now=56,
                lease_duration=100,
            )
        replacement = SourceRootBuildCommand(("replacement",), b"n" * 16)
        with replacement.prepare_root_upload() as replacement_root:
            _upload(connector, gate, replacement_turn, replacement_root, now=57)
            before = _reservation_snapshot(connector)
            with (
                connector.transaction(),
                pytest.raises((SourceBuildConflictError, SourceBuildNotReadyError)),
            ):
                SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=replacement_turn,
                    command=replacement,
                    root_plan=replacement_root,
                    now=60,
                )
        assert _reservation_snapshot(connector) == before
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_working_builds WHERE slot = 1"
        ) == (working.build_attempt_id,)
        assert not connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (replacement_turn.generation,),
        )
        assert not connector.fetch_one(
            "SELECT build_id FROM catalog_source_build_anchors WHERE build_id = %s",
            (replacement.build_attempt_id,),
        )
    finally:
        connector.close()


def test_handoff_rejects_forged_sealed_stale_working_assignment(
    tmp_path: Path,
) -> None:
    connector = _generated_database(
        tmp_path / "handoff-working-sealed-assignment.sqlite3"
    )
    try:
        gate, published, _receipt_id = _sealed_published_source(
            connector,
            finalized=True,
        )
        with connector.transaction():
            stale_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"s" * 16,
                now=46,
                lease_duration=10,
            )
        with published.prepare_root_upload() as published_root:
            _upload(connector, gate, stale_turn, published_root, now=47)
            with connector.transaction():
                SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=stale_turn,
                    command=published,
                    root_plan=published_root,
                    now=50,
                )
        assert (
            connector.execute_affected(
                "UPDATE operational_source_working_builds SET assigned_at = %s "
                "WHERE slot = %s",
                (999, 1),
            )
            == 1
        )
        with connector.transaction():
            replacement_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"n" * 16,
                now=56,
                lease_duration=100,
            )
        replacement = SourceRootBuildCommand(("replacement",), b"n" * 16)
        with replacement.prepare_root_upload() as replacement_root:
            _upload(connector, gate, replacement_turn, replacement_root, now=57)
            with (
                connector.transaction(),
                pytest.raises(SourceBuildConflictError, match="assignment|created_at"),
            ):
                SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=replacement_turn,
                    command=replacement,
                    root_plan=replacement_root,
                    now=60,
                )
        assert connector.fetch_one(
            "SELECT build_id, assigned_at FROM operational_source_working_builds "
            "WHERE slot = %s",
            (1,),
        ) == (published.build_attempt_id, 999)
    finally:
        connector.close()


def test_handoff_atomically_abandons_prior_generation_open_working_root(
    tmp_path: Path,
) -> None:
    connector = _generated_database(
        tmp_path / "handoff-working-stale-open-recovery.sqlite3"
    )
    try:
        gate, _published, _receipt_id = _sealed_published_source(
            connector,
            finalized=True,
        )
        with connector.transaction():
            stale_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"s" * 16,
                now=46,
                lease_duration=10,
            )
        stale = SourceRootBuildCommand(("stale-open",), b"f" * 16)
        with stale.prepare_root_upload() as stale_root:
            _upload(connector, gate, stale_turn, stale_root, now=47)
            with connector.transaction():
                SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=stale_turn,
                    command=stale,
                    root_plan=stale_root,
                    now=50,
                )
        with connector.transaction():
            replacement_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"n" * 16,
                now=56,
                lease_duration=100,
            )
        replacement = SourceRootBuildCommand(("replacement",), b"n" * 16)
        with replacement.prepare_root_upload() as replacement_root:
            _upload(connector, gate, replacement_turn, replacement_root, now=57)
            with connector.transaction():
                recovered = SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=replacement_turn,
                    command=replacement,
                    root_plan=replacement_root,
                    now=60,
                )

        assert not recovered.replayed
        assert recovered.build_id == replacement.build_attempt_id
        assert connector.fetch_one(
            "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
            (stale.build_attempt_id,),
        ) == ("ABANDONED",)
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_working_builds WHERE slot = 1"
        ) == (replacement.build_attempt_id,)
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (stale_turn.generation,),
        ) == (stale.build_attempt_id,)
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (replacement_turn.generation,),
        ) == (replacement.build_attempt_id,)
    finally:
        connector.close()


def test_handoff_does_not_reclaim_open_working_without_prior_generation_authority(
    tmp_path: Path,
) -> None:
    connector = _generated_database(
        tmp_path / "handoff-working-foreign-open-fail-closed.sqlite3"
    )
    try:
        gate, stale_turn, stale = _open_build(connector)
        assert (
            connector.execute_affected(
                "DELETE FROM operational_source_build_generations "
                "WHERE generation = %s AND build_id = %s",
                (stale_turn.generation, stale.build_attempt_id),
            )
            == 1
        )
        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                stale_turn,
                now=30,
            )
        with connector.transaction():
            replacement_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"n" * 16,
                now=31,
                lease_duration=100,
            )
        replacement = SourceRootBuildCommand(("replacement",), b"n" * 16)
        with replacement.prepare_root_upload() as replacement_root:
            _upload(connector, gate, replacement_turn, replacement_root, now=32)
            before = _reservation_snapshot(connector)
            with (
                connector.transaction(),
                pytest.raises(
                    SourceBuildConflictError,
                    match="no exact generation authority",
                ),
            ):
                SourceBuildRepository.handoff_root(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=replacement_turn,
                    command=replacement,
                    root_plan=replacement_root,
                    now=35,
                )
        assert _reservation_snapshot(connector) == before
        assert connector.fetch_one(
            "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
            (stale.build_attempt_id,),
        ) == ("OPEN",)
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_working_builds WHERE slot = 1"
        ) == (stale.build_attempt_id,)
    finally:
        connector.close()


def _resolve_batch(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    plan: SourceDiscoveryPlan,
    batch: Any,
    *,
    now: int,
) -> tuple[Any, ...]:
    resolved = []
    for locator in batch.locators:
        with plan.prepare_locator_upload(locator) as upload:
            _upload(connector, gate, turn, upload, now=now)
            with connector.transaction():
                resolved.append(
                    SourceBuildRepository.resolve_discovery_locator(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        batch=batch,
                        locator=locator,
                        upload_plan=upload,
                        now=now + 3,
                    )
                )
    return tuple(resolved)


def _finish_discovery(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    plan: SourceDiscoveryPlan,
    *,
    now: int,
) -> tuple[Any, ...]:
    all_resolved: list[Any] = []
    while True:
        batch = SourceBuildRepository.prepare_discovery_batch(
            connector,
            build_id=b"b" * 16,
            plan=plan,
        )
        resolved = _resolve_batch(
            connector,
            gate,
            turn,
            plan,
            batch,
            now=now,
        )
        with connector.transaction():
            receipt = SourceBuildRepository.commit_discovery_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                batch=batch,
                resolved=resolved,
                now=now + 4,
            )
        all_resolved.extend(resolved)
        if receipt.terminal:
            return tuple(all_resolved)
        now += 10


def test_discovery_replay_rejects_dataclass_forged_sealed_replay_flag(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "discovery-forged-sealed-replay.sqlite3")
    try:
        gate, turn, command = _open_build(connector)
        with SourceDiscoveryPlan.from_locators(()) as plan:
            batch = SourceBuildRepository.prepare_discovery_batch(
                connector,
                build_id=command.build_attempt_id,
                plan=plan,
            )
            assert batch.terminal and not batch.sealed_replay
            with connector.transaction():
                receipt = SourceBuildRepository.commit_discovery_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    batch=batch,
                    resolved=(),
                    now=30,
                )
            assert receipt.terminal

            forged = replace(batch, sealed_replay=True)
            before = _source_build_discovery_snapshot(connector)
            with (
                connector.transaction(),
                pytest.raises(SourceBuildConflictError, match="sealed|replay"),
            ):
                SourceBuildRepository.commit_discovery_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    batch=forged,
                    resolved=(),
                    now=31,
                )
            assert _source_build_discovery_snapshot(connector) == before
            assert connector.fetch_one(
                "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
                (command.build_attempt_id,),
            ) == ("OPEN",)
    finally:
        connector.close()


def _insert_observation_stat(
    connector: SQLiteConnector,
    *,
    gallery_id: int,
    observation_id: int,
    file_count: int,
    byte_count: int,
) -> None:
    connector.execute(
        "INSERT INTO catalog_gallery_observation_stat "
        "(gallery_id, observation_id, file_count, byte_count) "
        "VALUES (%s, %s, %s, %s)",
        (gallery_id, observation_id, file_count, byte_count),
    )


def _insert_source_build_discovery(
    connector: SQLiteConnector,
    *,
    build_id: bytes,
    scan_attempt: bytes,
    gallery_count: int,
    tree_observation_sha256: bytes,
    completed_at: int,
) -> None:
    connector.execute(
        "INSERT INTO catalog_source_build_discovery_anchors (build_id) VALUES (%s)",
        (build_id,),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_discovery_scan_attempts "
        "(build_id, scan_attempt) VALUES (%s, %s)",
        (build_id, scan_attempt),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_discovery_gallery_counts "
        "(build_id, gallery_count) VALUES (%s, %s)",
        (build_id, gallery_count),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_discovery_tree_observation_sha256s "
        "(build_id, tree_observation_sha256) VALUES (%s, %s)",
        (build_id, tree_observation_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_discovery_completed_ats "
        "(build_id, completed_at) VALUES (%s, %s)",
        (build_id, completed_at),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_discovery_seals (build_id) VALUES (%s)",
        (build_id,),
    )


def _source_build_discovery_snapshot(
    connector: SQLiteConnector,
) -> tuple[list[tuple[Any, ...]], ...]:
    return tuple(
        connector.fetch_all(sql)
        for sql in (
            "SELECT * FROM catalog_source_build_discovery_anchors",
            "SELECT * FROM catalog_source_build_discovery_scan_attempts",
            "SELECT * FROM catalog_source_build_discovery_gallery_counts",
            "SELECT * FROM catalog_source_build_discovery_tree_observation_sha256s",
            "SELECT * FROM catalog_source_build_discovery_completed_ats",
            "SELECT * FROM catalog_source_build_discovery_seals",
        )
    )


def test_source_discovery_vertical_replay_seal_visibility_and_corruption(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "discovery-vertical-replay.sqlite3")
    try:
        _open_build(connector)
        values = (b"b" * 16, b"d" * 16, 3, b"t" * 32, 40)
        with connector.transaction():
            source_build_module._persist_source_build_discovery(
                connector,
                build_id=values[0],
                scan_attempt=values[1],
                gallery_count=values[2],
                tree_observation_sha256=values[3],
                completed_at=values[4],
            )
        committed = _source_build_discovery_snapshot(connector)
        assert all(rows for rows in committed)
        assert (
            connector.fetch_one(
                "SELECT build_id, scan_attempt, gallery_count, "
                "tree_observation_sha256, completed_at "
                "FROM catalog_source_build_discoveries WHERE build_id = %s",
                (values[0],),
            )
            == values
        )

        with connector.transaction():
            source_build_module._persist_source_build_discovery(
                connector,
                build_id=values[0],
                scan_attempt=values[1],
                gallery_count=values[2],
                tree_observation_sha256=values[3],
                completed_at=values[4],
            )
        assert _source_build_discovery_snapshot(connector) == committed

        connector.execute(
            "DELETE FROM catalog_source_build_discovery_seals WHERE build_id = %s",
            (values[0],),
        )
        assert (
            connector.fetch_all(
                "SELECT 1 FROM catalog_source_build_discoveries WHERE build_id = %s",
                (values[0],),
            )
            == []
        )
    finally:
        connector.close()

    corrupt = _generated_database(tmp_path / "discovery-vertical-corrupt.sqlite3")
    try:
        _open_build(corrupt)
        build_id = b"b" * 16
        corrupt.execute(
            "INSERT INTO catalog_source_build_discovery_anchors (build_id) VALUES (%s)",
            (build_id,),
        )
        corrupt.execute(
            "INSERT INTO catalog_source_build_discovery_completed_ats "
            "(build_id, completed_at) VALUES (%s, 999)",
            (build_id,),
        )
        before = _source_build_discovery_snapshot(corrupt)
        with pytest.raises(SourceBuildConflictError, match="completed at"):
            with corrupt.transaction():
                source_build_module._persist_source_build_discovery(
                    corrupt,
                    build_id=build_id,
                    scan_attempt=b"d" * 16,
                    gallery_count=3,
                    tree_observation_sha256=b"t" * 32,
                    completed_at=40,
                )
        assert _source_build_discovery_snapshot(corrupt) == before
        assert corrupt.fetch_all("SELECT 1 FROM catalog_source_build_discoveries") == []
    finally:
        corrupt.close()


def test_source_discovery_vertical_mariadb_sql_is_static_and_seal_last() -> None:
    class _MariaDiscoveryRecorder:
        def __init__(self) -> None:
            self.queries: list[tuple[str, tuple[Any, ...]]] = []
            self.executions: list[tuple[str, tuple[Any, ...]]] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            self.queries.append((query, data))
            return ()

        def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
            self.executions.append((query, data))

    recorder = _MariaDiscoveryRecorder()
    source_build_module._persist_source_build_discovery(
        recorder,
        build_id=b"b" * 16,
        scan_attempt=b"d" * 16,
        gallery_count=3,
        tree_observation_sha256=b"t" * 32,
        completed_at=40,
    )
    ordered_tables = (
        "catalog_source_build_discovery_anchors",
        "catalog_source_build_discovery_scan_attempts",
        "catalog_source_build_discovery_gallery_counts",
        "catalog_source_build_discovery_tree_observation_sha256s",
        "catalog_source_build_discovery_completed_ats",
        "catalog_source_build_discovery_seals",
    )
    assert len(recorder.executions) == len(ordered_tables)
    for (query, _data), table in zip(recorder.executions, ordered_tables, strict=True):
        assert query.lstrip().startswith(f"INSERT INTO {table}")
        assert "%s" in query and "?" not in query
    assert (
        recorder.executions[-1][0]
        .lstrip()
        .startswith("INSERT INTO catalog_source_build_discovery_seals")
    )


def _stage_assembly_inputs(
    connector: SQLiteConnector,
    resolved: tuple[Any, ...],
    *,
    omit_stat: bool = False,
) -> tuple[int, int]:
    total_files = 0
    total_bytes = 0
    for position, evidence in enumerate(resolved):
        file_count = position + 1
        byte_count = (position + 1) * 100
        total_files += file_count
        total_bytes += byte_count
        connector.execute(
            "INSERT INTO catalog_gallery_observation_allocations "
            "(gallery_id, observation_id, allocated_at) VALUES (%s, %s, %s)",
            (evidence.gallery_id, 1, 100),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_observations "
            "(gallery_id, observation_id, observation_identity_sha256) "
            "VALUES (%s, %s, %s)",
            (evidence.gallery_id, 1, evidence.locator_sha256),
        )
        if not omit_stat:
            _insert_observation_stat(
                connector,
                gallery_id=evidence.gallery_id,
                observation_id=1,
                file_count=file_count,
                byte_count=byte_count,
            )
        connector.execute(
            "INSERT INTO catalog_source_build_galleries "
            "(build_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
            (b"b" * 16, evidence.gallery_id, 1),
        )
        seed_gallery_manifest(
            connector,
            gallery_id=evidence.gallery_id,
            observation_id=1,
            manifest_policy_id=1,
            manifest_sha256=artifact_source_manifest_digest(
                evidence.locator_sha256,
                1,
                1,
            ),
            computed_at=101,
        )
    return total_files, total_bytes


def test_disk_plan_sorts_unsigned_digest_caps_pages_and_rejects_duplicates(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "plan.sqlite3")
    try:
        _open_build(connector)
        locators = tuple((f"gallery-{index:04d}",) for index in reversed(range(257)))
        with SourceDiscoveryPlan.from_locators(locators) as plan:
            first = SourceBuildRepository.prepare_discovery_batch(
                connector,
                build_id=b"b" * 16,
                plan=plan,
            )
            assert len(first.locators) == 256
            digests = [locator.locator_sha256 for locator in first.locators]
            assert digests == sorted(digests)
            assert not first.terminal
        with pytest.raises(SourceDiscoveryPlanError, match="duplicate"):
            SourceDiscoveryPlan.from_locators((("same",), ("same",)))

        with SourceDiscoveryPlan.from_locators(locators) as first_plan:
            first_receipt = (
                first_plan.scan_attempt,
                first_plan.gallery_count,
                first_plan.tree_observation_sha256,
            )
        with SourceDiscoveryPlan.from_locators(locators) as rebuilt:
            assert (
                rebuilt.scan_attempt,
                rebuilt.gallery_count,
                rebuilt.tree_observation_sha256,
            ) == first_receipt

        discovery_parameters = inspect.signature(
            SourceBuildRepository.prepare_discovery_batch
        ).parameters
        assert "cursor" not in discovery_parameters
        assert "count" not in discovery_parameters
        assembly_parameters = inspect.signature(
            SourceBuildRepository.assemble_batch
        ).parameters
        assert "cursor" not in assembly_parameters
        assert "file_count" not in assembly_parameters
    finally:
        connector.close()


def test_pending_source_gallery_is_bounded_pk_driven_and_decodes_plan_position(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "pending-source-gallery.sqlite3")
    try:
        gate, turn, _command = _open_build(connector)
        with SourceDiscoveryPlan.from_locators((("gallery",),)) as plan:
            resolved = _finish_discovery(
                connector,
                gate,
                turn,
                plan,
                now=40,
            )
            pending = SourceBuildRepository.get_pending_source_gallery(
                connector,
                build_id=b"b" * 16,
            )
            assert pending is not None
            assert pending.position == 0
            assert pending.gallery_id == resolved[0].gallery_id
            assert pending.locator_sha256 == resolved[0].locator_sha256
            assert plan._decode_locator(
                pending.position,
                pending.locator_sha256,
            ) == ("gallery",)

        query_plan = connector.fetch_all(
            "EXPLAIN QUERY PLAN " + source_build_module._PENDING_SOURCE_GALLERY_QUERY,
            (b"b" * 16,),
        )
        assert query_plan
        details = tuple(str(row[3]) for row in query_plan)
        assert all("SCAN " not in detail for detail in details)
        assert any(
            "catalog_source_build_expected_gallery" in detail
            or "sqlite_autoindex_catalog_source_build_expected_gallery" in detail
            for detail in details
        )
        assert any(
            "catalog_source_build_galleries" in detail
            or "sqlite_autoindex_catalog_source_build_galleries" in detail
            for detail in details
        )
    finally:
        connector.close()


def test_discovery_new_generation_assembly_and_response_loss(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "runtime.sqlite3")
    try:
        gate, turn, root_command = _open_build(connector)
        assert connector.fetch_one(
            "SELECT generation, cursor_bytes, processed_count, state "
            "FROM operational_source_build_discovery_checkpoints"
        ) == (1, b"", 0, "OPEN")
        assert connector.fetch_one(
            "SELECT generation, cursor_bytes, processed_gallery_count, "
            "processed_file_count, processed_byte_count, "
            "manifest_chain_sha256, state "
            "FROM operational_source_build_assembly_checkpoints"
        ) == (
            1,
            b"",
            0,
            0,
            0,
            bytes.fromhex(
                "121f20d26c10f4c5ce6e621dc5e41b7da2c4028af840caa7547265068f2458e3"
            ),
            "OPEN",
        )

        with SourceDiscoveryPlan.from_locators(
            (("z-last",), ("nested", "畫廊"), ("a-first",))
        ) as plan:
            first = SourceBuildRepository.prepare_discovery_batch(
                connector,
                build_id=b"b" * 16,
                plan=plan,
            )
            resolved = _resolve_batch(
                connector,
                gate,
                turn,
                plan,
                first,
                now=30,
            )
            with connector.transaction():
                committed = SourceBuildRepository.commit_discovery_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    batch=first,
                    resolved=resolved,
                    now=34,
                )
            with connector.transaction():
                replay = SourceBuildRepository.commit_discovery_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    batch=first,
                    resolved=resolved,
                    now=35,
                )
            assert replay.replayed
            assert replay.committed_at == committed.committed_at

            # Rebuilding the same complete snapshot produces the same
            # deterministic scan attempt and resumes the exact receipt chain.
            with SourceDiscoveryPlan.from_locators(
                (("z-last",), ("nested", "畫廊"), ("a-first",))
            ) as switched:
                resumed = SourceBuildRepository.prepare_discovery_batch(
                    connector,
                    build_id=b"b" * 16,
                    plan=switched,
                )
                assert resumed.terminal
                assert resumed.start_generation == committed.committed_generation
                assert resumed.scan_attempt == plan.scan_attempt

            with connector.transaction():
                IngestFenceRepository.complete(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    turn,
                    now=40,
                )
            with connector.transaction():
                turn2 = IngestFenceRepository.claim(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    owner_token=b"j" * 16,
                    now=41,
                    lease_duration=1_000_000,
                )
            with root_command.prepare_root_upload() as root2:
                _upload(connector, gate, turn2, root2, now=42)
                with (
                    patch.object(
                        source_build_module,
                        "database_unix_microseconds",
                        side_effect=AssertionError("clock"),
                    ),
                    connector.transaction(),
                ):
                    handoff2 = SourceBuildRepository.handoff_root(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn2,
                        command=root_command,
                        root_plan=root2,
                        now=45,
                    )
            assert handoff2.generation == 2
            terminal = SourceBuildRepository.prepare_discovery_batch(
                connector,
                build_id=b"b" * 16,
                plan=plan,
            )
            assert terminal.terminal
            with connector.transaction():
                terminal_receipt = SourceBuildRepository.commit_discovery_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn2,
                    batch=terminal,
                    resolved=(),
                    now=46,
                )
            with connector.transaction():
                terminal_replay = SourceBuildRepository.commit_discovery_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn2,
                    batch=terminal,
                    resolved=(),
                    now=47,
                )
            assert terminal_receipt.terminal
            assert terminal_replay.replayed
            assert connector.fetch_one(
                "SELECT scan_attempt, gallery_count, tree_observation_sha256 "
                "FROM catalog_source_build_discoveries"
            ) == (plan.scan_attempt, 3, plan.tree_observation_sha256)
            assert _source_build_discovery_snapshot(connector) == (
                [(b"b" * 16,)],
                [(b"b" * 16, plan.scan_attempt)],
                [(b"b" * 16, 3)],
                [(b"b" * 16, plan.tree_observation_sha256)],
                [(b"b" * 16, 46)],
                [(b"b" * 16,)],
            )

            total_files, total_bytes = _stage_assembly_inputs(connector, resolved)
            attempt = SourceBuildRepository.issue_assembly_batch()
            with connector.transaction():
                assembled = SourceBuildRepository.assemble_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn2,
                    build_id=b"b" * 16,
                    attempt=attempt,
                    now=50,
                )
            assert assembled.row_count == 3
            assert assembled.next_file_count == total_files
            assert assembled.next_byte_count == total_bytes
            with connector.transaction():
                assembled_replay = SourceBuildRepository.assemble_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn2,
                    build_id=b"b" * 16,
                    attempt=attempt,
                    now=51,
                )
            assert assembled_replay.replayed
            assert assembled_replay.next_manifest_chain_sha256 == (
                assembled.next_manifest_chain_sha256
            )

            terminal_attempt = SourceBuildRepository.issue_assembly_batch()
            with connector.transaction():
                sealed = SourceBuildRepository.assemble_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn2,
                    build_id=b"b" * 16,
                    attempt=terminal_attempt,
                    now=52,
                )
            assert sealed.terminal
            sealed_source = connector.fetch_one(
                "SELECT state, created_at, sealed_at FROM catalog_source_builds"
            )
            assert sealed_source[0] == "SEALED"
            assert sealed_source[1] <= sealed_source[2]
            assert sealed_source[2] != 52
            assert connector.fetch_one(
                "SELECT manifest_sha256, gallery_count, file_count, byte_count, "
                "computed_at "
                "FROM catalog_build_manifests"
            ) == (
                sealed.next_manifest_chain_sha256,
                3,
                total_files,
                total_bytes,
                sealed_source[2],
            )
            with (
                patch.object(
                    source_build_module,
                    "database_unix_microseconds",
                    side_effect=AssertionError("clock"),
                ),
                connector.transaction(),
            ):
                sealed_replay = SourceBuildRepository.assemble_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn2,
                    build_id=b"b" * 16,
                    attempt=terminal_attempt,
                    now=53,
                )
            assert sealed_replay.replayed

            # A delayed root response retry still resolves the immutable
            # generation mapping after the build has advanced to SEALED.
            with root_command.prepare_root_upload() as sealed_root_retry:
                _upload(connector, gate, turn2, sealed_root_retry, now=55)
                with connector.transaction():
                    root_replay = SourceBuildRepository.handoff_root(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn2,
                        command=root_command,
                        root_plan=sealed_root_retry,
                        now=58,
                    )
            assert root_replay.replayed
            assert (
                connector.fetch_all(
                    "SELECT 1 FROM operational_canonical_value_uploads "
                    "WHERE generation = %s AND value_sha256 = %s",
                    (turn2.generation, root_command.source_root_sha256),
                )
                == []
            )

            connector.execute(
                "UPDATE catalog_build_manifest_file_counts SET file_count = %s "
                "WHERE build_id = %s",
                (total_files + 1, b"b" * 16),
            )
            with (
                patch.object(
                    connector,
                    "execute",
                    side_effect=AssertionError("write"),
                ),
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=AssertionError("write"),
                ),
                connector.transaction(),
                pytest.raises(SourceBuildConflictError, match="manifest"),
            ):
                SourceBuildRepository.assemble_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn2,
                    build_id=b"b" * 16,
                    attempt=terminal_attempt,
                    now=59,
                )

            with pytest.raises(IngestFenceUnavailableError):
                with connector.transaction():
                    SourceBuildRepository.assemble_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        build_id=b"b" * 16,
                        attempt=terminal_attempt,
                        now=54,
                    )
    finally:
        connector.close()


def test_assembly_missing_dependency_and_scope_corruption_are_zero_write(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "corruption.sqlite3")
    try:
        gate, turn, _command = _open_build(connector)
        with SourceDiscoveryPlan.from_locators((("gallery",),)) as plan:
            resolved = _finish_discovery(
                connector,
                gate,
                turn,
                plan,
                now=30,
            )
            _stage_assembly_inputs(connector, resolved, omit_stat=True)
            attempt = SourceBuildRepository.issue_assembly_batch()
            with pytest.raises(SourceBuildNotReadyError, match="lacks"):
                with connector.transaction():
                    SourceBuildRepository.assemble_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        build_id=b"b" * 16,
                        attempt=attempt,
                        now=60,
                    )
            assert connector.fetch_one(
                "SELECT generation, processed_gallery_count "
                "FROM operational_source_build_assembly_checkpoints"
            ) == (1, 0)
            assert (
                connector.fetch_all(
                    "SELECT 1 FROM operational_source_build_assembly_batch_receipts"
                )
                == []
            )

            _insert_observation_stat(
                connector,
                gallery_id=resolved[0].gallery_id,
                observation_id=1,
                file_count=1,
                byte_count=100,
            )
            other_scope = seed_source_scope(
                connector,
                source_root_sha256=resolved[0].locator_sha256,
            ).scope_key
            connector.execute(
                "UPDATE catalog_gallery_identities SET scope_key = %s "
                "WHERE gallery_id = %s",
                (other_scope, resolved[0].gallery_id),
            )
            with pytest.raises(SourceBuildConflictError, match="scope"):
                with connector.transaction():
                    SourceBuildRepository.assemble_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        build_id=b"b" * 16,
                        attempt=attempt,
                        now=61,
                    )
            assert (
                connector.fetch_all(
                    "SELECT 1 FROM operational_source_build_assembly_batch_receipts"
                )
                == []
            )

        source = inspect.getsource(SourceBuildRepository)
        assert "COUNT(" not in source.upper()
        assert "SUM(" not in source.upper()
        with pytest.raises(TypeError):
            AssemblyBatchAttempt(b"x" * 32, object())
    finally:
        connector.close()


def test_discovery_and_assembly_major_statement_faults_roll_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "faults.sqlite3")

    def fail_once(
        method_name: str,
        fragment: str,
        operation: Any,
    ) -> None:
        with monkeypatch.context() as context:
            original = getattr(connector, method_name)

            def fail(
                query: str,
                data: tuple[Any, ...] = (),
                *,
                _original: Any = original,
            ) -> Any:
                if fragment in query:
                    raise RuntimeError("injected source-build statement fault")
                return _original(query, data)

            context.setattr(connector, method_name, fail)
            with pytest.raises(
                RuntimeError,
                match="injected source-build statement fault",
            ):
                operation()

    try:
        gate, turn, _command = _open_build(connector)
        with SourceDiscoveryPlan.from_locators((("fault-gallery",),)) as plan:
            batch = SourceBuildRepository.prepare_discovery_batch(
                connector,
                build_id=b"b" * 16,
                plan=plan,
            )
            resolved = _resolve_batch(
                connector,
                gate,
                turn,
                plan,
                batch,
                now=30,
            )

            def commit_data() -> None:
                with connector.transaction():
                    SourceBuildRepository.commit_discovery_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        batch=batch,
                        resolved=resolved,
                        now=34,
                    )

            for method, fragment in (
                ("execute", "INSERT INTO catalog_source_build_expected_gallery"),
                (
                    "execute",
                    "INSERT INTO operational_source_build_discovery_batch_receipts",
                ),
                (
                    "execute_affected",
                    "UPDATE operational_source_build_discovery_checkpoints",
                ),
            ):
                fail_once(method, fragment, commit_data)
                assert (
                    connector.fetch_all(
                        "SELECT 1 FROM catalog_source_build_expected_gallery"
                    )
                    == []
                )
                assert (
                    connector.fetch_all(
                        "SELECT 1 FROM "
                        "operational_source_build_discovery_batch_receipts"
                    )
                    == []
                )
                assert connector.fetch_one(
                    "SELECT generation, processed_count, state FROM "
                    "operational_source_build_discovery_checkpoints"
                ) == (1, 0, "OPEN")
            commit_data()

            terminal = SourceBuildRepository.prepare_discovery_batch(
                connector,
                build_id=b"b" * 16,
                plan=plan,
            )

            def commit_terminal_discovery() -> None:
                with connector.transaction():
                    SourceBuildRepository.commit_discovery_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        batch=terminal,
                        resolved=(),
                        now=40,
                    )

            for method, fragment in (
                (
                    "execute",
                    "INSERT INTO operational_source_build_discovery_batch_receipts",
                ),
                ("execute", "INSERT INTO catalog_source_build_discovery_anchors"),
                (
                    "execute",
                    "INSERT INTO catalog_source_build_discovery_scan_attempts",
                ),
                (
                    "execute",
                    "INSERT INTO catalog_source_build_discovery_gallery_counts",
                ),
                (
                    "execute",
                    "INSERT INTO "
                    "catalog_source_build_discovery_tree_observation_sha256s",
                ),
                (
                    "execute",
                    "INSERT INTO catalog_source_build_discovery_completed_ats",
                ),
                ("execute", "INSERT INTO catalog_source_build_discovery_seals"),
                (
                    "execute_affected",
                    "UPDATE operational_source_build_discovery_checkpoints",
                ),
            ):
                fail_once(method, fragment, commit_terminal_discovery)
                assert _source_build_discovery_snapshot(connector) == (
                    [],
                    [],
                    [],
                    [],
                    [],
                    [],
                )
                assert (
                    connector.fetch_all(
                        "SELECT 1 FROM catalog_source_build_discoveries"
                    )
                    == []
                )
                assert connector.fetch_one(
                    "SELECT generation, processed_count, state FROM "
                    "operational_source_build_discovery_checkpoints"
                ) == (2, 1, "OPEN")
                assert connector.fetch_one(
                    "SELECT COUNT(*) FROM "
                    "operational_source_build_discovery_batch_receipts"
                ) == (1,)
            commit_terminal_discovery()

            _stage_assembly_inputs(connector, resolved)
            assembly = SourceBuildRepository.issue_assembly_batch()

            def commit_assembly() -> None:
                with connector.transaction():
                    SourceBuildRepository.assemble_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        build_id=b"b" * 16,
                        attempt=assembly,
                        now=50,
                    )

            for method, fragment in (
                (
                    "execute",
                    "INSERT INTO operational_source_build_assembly_batch_receipts",
                ),
                (
                    "execute_affected",
                    "UPDATE operational_source_build_assembly_checkpoints",
                ),
            ):
                fail_once(method, fragment, commit_assembly)
                assert (
                    connector.fetch_all(
                        "SELECT 1 FROM operational_source_build_assembly_batch_receipts"
                    )
                    == []
                )
                assert connector.fetch_one(
                    "SELECT generation, processed_gallery_count, state FROM "
                    "operational_source_build_assembly_checkpoints"
                ) == (1, 0, "OPEN")
            commit_assembly()

            seal = SourceBuildRepository.issue_assembly_batch()

            def commit_seal() -> None:
                with connector.transaction():
                    SourceBuildRepository.assemble_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        build_id=b"b" * 16,
                        attempt=seal,
                        now=60,
                    )

            for method, fragment in (
                (
                    "execute",
                    "INSERT INTO operational_source_build_assembly_batch_receipts",
                ),
                ("execute", "INSERT INTO catalog_source_build_sealed_ats"),
                ("execute", "INSERT INTO catalog_build_manifest_anchors"),
                (
                    "execute",
                    "INSERT INTO catalog_build_manifest_manifest_sha256s",
                ),
                ("execute", "INSERT INTO catalog_build_manifest_file_counts"),
                ("execute", "INSERT INTO catalog_build_manifest_byte_counts"),
                ("execute", "INSERT INTO catalog_build_manifest_seals"),
                (
                    "execute_affected",
                    "UPDATE operational_source_build_assembly_checkpoints",
                ),
                ("execute_affected", "UPDATE catalog_source_build_states"),
            ):
                fail_once(method, fragment, commit_seal)
                assert (
                    connector.fetch_all("SELECT 1 FROM catalog_build_manifests") == []
                )
                assert connector.fetch_one(
                    "SELECT state, sealed_at FROM catalog_source_builds"
                ) == ("OPEN", None)
                assert connector.fetch_one(
                    "SELECT generation, processed_gallery_count, state FROM "
                    "operational_source_build_assembly_checkpoints"
                ) == (2, 1, "OPEN")
                assert connector.fetch_one(
                    "SELECT COUNT(*) FROM "
                    "operational_source_build_assembly_batch_receipts"
                ) == (1,)
            terminal_trace: list[str] = []
            connector.connection.set_trace_callback(terminal_trace.append)
            try:
                commit_seal()
            finally:
                connector.connection.set_trace_callback(None)
            checkpoint_cas = next(
                index
                for index, sql in enumerate(terminal_trace)
                if "UPDATE OPERATIONAL_SOURCE_BUILD_ASSEMBLY_CHECKPOINTS" in sql.upper()
            )
            state_cas = next(
                index
                for index, sql in enumerate(terminal_trace)
                if "UPDATE CATALOG_SOURCE_BUILD_STATES" in sql.upper()
            )
            manifest_anchor = next(
                index
                for index, sql in enumerate(terminal_trace)
                if "INSERT INTO CATALOG_BUILD_MANIFEST_ANCHORS" in sql.upper()
            )
            manifest_seal = next(
                index
                for index, sql in enumerate(terminal_trace)
                if "INSERT INTO CATALOG_BUILD_MANIFEST_SEALS" in sql.upper()
            )
            assert checkpoint_cas < state_cas < manifest_anchor < manifest_seal
            assert connector.fetch_one("SELECT state FROM catalog_source_builds") == (
                "SEALED",
            )
    finally:
        connector.close()


def test_assembly_pages_257_rows_in_bounded_keyset_queries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "bounded-assembly.sqlite3")
    try:
        gate, turn, _command = _open_build(connector)
        scope = connector.fetch_one(
            "SELECT scope_key FROM catalog_source_builds WHERE build_id = %s",
            (b"b" * 16,),
        )[0]
        prepared: list[tuple[bytes, bytes, bytes, int, bytes]] = []
        for index in range(257):
            leaf = f"bounded-{index:04d}".encode()
            with CanonicalValueUploadPlan.from_parts(
                "source_relative_locator_v1",
                iter_source_relative_locator_payload((leaf.decode(),)),
            ) as locator_plan:
                pages = list(locator_plan.iter_pages())
                assert len(pages) == 1
                prepared.append(
                    (
                        locator_plan.value_sha256,
                        pages[0].page_sha256,
                        pages[0].page_bytes,
                        locator_plan.byte_count,
                        leaf,
                    )
                )
        prepared.sort(key=lambda item: item[0])
        audit = sha256(b"h2hdb-vnext-source-build-discovery-audit-v1\0")
        audit.update(len(prepared).to_bytes(8, "big"))
        total_bytes = 0
        with connector.transaction():
            for position, (value, page_sha, page_bytes, byte_count, leaf) in enumerate(
                prepared
            ):
                audit.update(value)
                decoded = decode_canonical_value_page(page_bytes)
                seed_canonical_value(
                    connector,
                    value_sha256=value,
                    digest_domain=b"source_relative_locator_v1",
                    page_sha256=page_sha,
                    page_bytes=page_bytes,
                    subtree_item_count=decoded.subtree_byte_count,
                    allocated_at=70,
                    level=decoded.level,
                    page_position=decoded.page_position,
                )
                connector.execute(
                    "INSERT INTO catalog_source_locator_identity "
                    "(locator_sha256, source_gallery_name) VALUES (%s, %s)",
                    (value, leaf),
                )
                gallery_id = position + 1
                seed_gallery_identity(
                    connector,
                    gallery_id=gallery_id,
                    gallery_key=gallery_key(scope, value),
                    scope_key=scope,
                    locator_sha256=value,
                )
                connector.execute(
                    "INSERT INTO catalog_gallery_observation_allocations "
                    "(gallery_id, observation_id, allocated_at) "
                    "VALUES (%s, %s, %s)",
                    (gallery_id, 1, 71),
                )
                connector.execute(
                    "INSERT INTO catalog_gallery_observations "
                    "(gallery_id, observation_id, observation_identity_sha256) "
                    "VALUES (%s, %s, %s)",
                    (gallery_id, 1, value),
                )
                row_bytes = position + 1
                total_bytes += row_bytes
                _insert_observation_stat(
                    connector,
                    gallery_id=gallery_id,
                    observation_id=1,
                    file_count=1,
                    byte_count=row_bytes,
                )
                connector.execute(
                    "INSERT INTO catalog_source_build_expected_gallery "
                    "(build_id, position, gallery_id) VALUES (%s, %s, %s)",
                    (b"b" * 16, position, gallery_id),
                )
                connector.execute(
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
                    (b"b" * 16, gallery_id, 1),
                )
                seed_gallery_manifest(
                    connector,
                    gallery_id=gallery_id,
                    observation_id=1,
                    manifest_policy_id=1,
                    manifest_sha256=artifact_source_manifest_digest(value, 1, 1),
                    computed_at=72,
                )
            _insert_source_build_discovery(
                connector,
                build_id=b"b" * 16,
                scan_attempt=b"d" * 16,
                gallery_count=257,
                tree_observation_sha256=audit.digest(),
                completed_at=73,
            )
            connector.execute_affected(
                "UPDATE operational_source_build_discovery_checkpoints "
                "SET generation = %s, cursor_bytes = %s, processed_count = %s, "
                "state = %s, updated_at = %s WHERE build_id = %s",
                (2, (256).to_bytes(8, "big"), 257, "COMPLETE", 73, b"b" * 16),
            )

        original_fetch_all = connector.fetch_all
        observed_page_sizes: list[int] = []
        observed_queries: list[tuple[str, tuple[Any, ...]]] = []

        def bounded_fetch_all(
            query: str,
            data: tuple[Any, ...] = (),
        ) -> list[tuple[Any, ...]]:
            rows = original_fetch_all(query, data)
            if "FROM catalog_source_build_expected_gallery e" in query:
                assert data[-1] == 256
                assert "ORDER BY e.position LIMIT %s" in query
                assert len(rows) <= 256
                observed_page_sizes.append(len(rows))
                observed_queries.append((query, data))
            return rows

        with monkeypatch.context() as context:
            context.setattr(connector, "fetch_all", bounded_fetch_all)
            receipts = []
            for now in (80, 81, 82):
                with connector.transaction():
                    receipts.append(
                        SourceBuildRepository.assemble_batch(
                            VNextUnitOfWork(connector, backend="sqlite"),
                            gate_lease=gate,
                            ingest_turn=turn,
                            build_id=b"b" * 16,
                            attempt=SourceBuildRepository.issue_assembly_batch(),
                            now=now,
                        )
                    )
        assert [receipt.row_count for receipt in receipts] == [256, 1, 0]
        assert observed_page_sizes == [256, 1, 0]
        assert receipts[-1].terminal
        assert receipts[-1].next_gallery_count == 257
        assert receipts[-1].next_file_count == 257
        assert receipts[-1].next_byte_count == total_bytes
        assembly_query, assembly_data = next(
            item
            for item in observed_queries
            if "FROM catalog_source_build_expected_gallery e" in item[0]
        )
        query_plan = connector.fetch_all(
            "EXPLAIN QUERY PLAN " + assembly_query,
            assembly_data,
        )
        assert query_plan
        assert all("SCAN " not in str(row[3]) for row in query_plan)
    finally:
        connector.close()


def test_mariadb_shape_locks_authorities_and_uses_full_checkpoint_cas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    build_id = b"b" * 16
    scope = b"s" * 32
    empty_chain = bytes.fromhex(
        "121f20d26c10f4c5ce6e621dc5e41b7da2c4028af840caa7547265068f2458e3"
    )

    class RecordingConnector:
        def __init__(self) -> None:
            self.fetch_one_queries: list[str] = []
            self.fetch_all_queries: list[tuple[str, tuple[Any, ...]]] = []
            self.execute_queries: list[str] = []
            self.cas_queries: list[str] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            self.fetch_one_queries.append(query)
            if "operational_source_build_generations" in query:
                return (build_id,)
            if "operational_source_working_builds" in query:
                return (build_id, 20)
            if query.startswith("WITH family_keys(build_id)") and (
                "catalog_source_build_anchors" in query
            ):
                return (
                    build_id,
                    build_id,
                    build_id,
                    scope,
                    build_id,
                    1,
                    build_id,
                    "OPEN",
                    build_id,
                    20,
                    build_id,
                    None,
                    None,
                )
            if "FROM catalog_source_build_states" in query:
                return ("OPEN",)
            if "operational_source_build_assembly_checkpoints" in query:
                return (1, b"", 0, 0, 0, empty_chain, "OPEN", 20)
            if "operational_source_build_assembly_batch_receipts" in query:
                return ()
            if "catalog_source_build_discovery_seals" in query:
                return (0, b"d" * 32, "COMPLETE")
            if query.startswith("SELECT gallery_count FROM ") and (
                "catalog_source_build_discovery_gallery_counts" in query
            ):
                return (0,)
            if query.startswith("SELECT sealed_at FROM ") and (
                "catalog_source_build_sealed_ats" in query
            ):
                return (30_000_000,)
            if query.startswith("WITH family_keys(build_id)") and (
                "catalog_build_manifest_anchors" in query
            ):
                return ()
            if "UTC_TIMESTAMP(6)" in query:
                return (30_000_000,)
            raise AssertionError((query, data))

        def fetch_all(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> list[tuple[Any, ...]]:
            self.fetch_all_queries.append((query, data))
            return []

        def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
            del data
            self.execute_queries.append(query)

        def execute_affected(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> int:
            del data
            self.cas_queries.append(query)
            return 1

    recorder = RecordingConnector()
    monkeypatch.setattr(
        "h2hdb.vnext_source_build_repository._authorize",
        lambda *_args, **_kwargs: 1,
    )
    receipt = SourceBuildRepository.assemble_batch(
        VNextUnitOfWork(recorder, backend="mariadb"),  # type: ignore[arg-type]
        gate_lease=None,  # type: ignore[arg-type]
        ingest_turn=None,  # type: ignore[arg-type]
        build_id=build_id,
        attempt=SourceBuildRepository.issue_assembly_batch(),
        now=30,
    )
    assert receipt.terminal
    assert receipt.committed_at == 30_000_000
    locked = [query for query in recorder.fetch_one_queries if " FOR UPDATE" in query]
    assert len(locked) == 4
    assert all(query.endswith(" FOR UPDATE") for query in locked)
    assert any("UTC_TIMESTAMP(6)" in query for query in recorder.fetch_one_queries)
    manifest_writes = [
        query
        for query in recorder.execute_queries
        if "catalog_build_manifest_" in query
    ]
    assert [
        next(
            name
            for name in (
                "anchors",
                "manifest_sha256s",
                "file_counts",
                "byte_counts",
                "seals",
            )
            if f"catalog_build_manifest_{name}" in query
        )
        for query in manifest_writes
    ] == ["anchors", "manifest_sha256s", "file_counts", "byte_counts", "seals"]
    assert any(
        "LIMIT %s" in query and data[-1] == 256
        for query, data in recorder.fetch_all_queries
    )
    checkpoint_cas = next(
        query
        for query in recorder.cas_queries
        if "operational_source_build_assembly_checkpoints" in query
    )
    for predicate in (
        "generation = %s",
        "cursor_bytes = %s",
        "processed_gallery_count = %s",
        "processed_file_count = %s",
        "processed_byte_count = %s",
        "manifest_chain_sha256 = %s",
        "state = %s",
        "updated_at = %s",
    ):
        assert predicate in checkpoint_cas


def test_mariadb_abandonment_locks_exact_root_and_keeps_generation_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    build_id = b"b" * 16
    scope = b"s" * 32

    class RecordingConnector:
        def __init__(self) -> None:
            self.fetches: list[str] = []
            self.mutations: list[str] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            del data
            self.fetches.append(query)
            if "operational_source_build_generations" in query:
                return (build_id,)
            if "operational_source_working_builds" in query:
                return (build_id, 20)
            if query.startswith("WITH family_keys(build_id)"):
                return (
                    build_id,
                    build_id,
                    build_id,
                    scope,
                    build_id,
                    1,
                    build_id,
                    "OPEN",
                    build_id,
                    20,
                    build_id,
                    None,
                    None,
                )
            if "catalog_source_build_states" in query:
                return ("OPEN",)
            raise AssertionError(query)

        def execute_affected(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> int:
            del data
            self.mutations.append(query)
            return 1

    connector = RecordingConnector()
    monkeypatch.setattr(
        "h2hdb.vnext_source_build_repository._authorize",
        lambda *_args, **_kwargs: 1,
    )
    result = SourceBuildRepository.abandon(
        VNextUnitOfWork(connector, backend="mariadb"),  # type: ignore[arg-type]
        gate_lease=None,  # type: ignore[arg-type]
        ingest_turn=None,  # type: ignore[arg-type]
        build_id=build_id,
        now=30,
    )
    assert not result.replayed
    locked = [query for query in connector.fetches if " FOR UPDATE" in query]
    assert len(locked) == 3
    assert all(query.endswith(" FOR UPDATE") for query in locked)
    assert connector.mutations == [
        "DELETE FROM operational_source_working_builds "
        "WHERE slot = %s AND build_id = %s AND assigned_at = %s",
        "UPDATE catalog_source_build_states SET state = %s "
        "WHERE build_id = %s AND state = %s",
    ]
    assert not any("source_build_generations" in query for query in connector.mutations)
