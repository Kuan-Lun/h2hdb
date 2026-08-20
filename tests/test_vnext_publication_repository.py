from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from shutil import copyfile
from typing import Any
from unittest.mock import patch

import pytest
from vnext_analysis_fixtures import seed_analysis_component, seed_analysis_run
from vnext_canonical_value_fixtures import seed_canonical_value
from vnext_catalog_registry_fixtures import (
    seed_analysis_policy,
    seed_artifact_policy_semantics,
    seed_artifact_producer_fingerprint,
    seed_display_title_policy,
    seed_manifest_policy,
    seed_source_scope,
    seed_title_sort_policy,
)
from vnext_manifest_fixtures import (
    seed_build_manifest,
    seed_snapshot_manifest,
    seed_source_build,
)

from h2hdb import vnext_identity as identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_ingest_fence_repository import (
    IngestFenceRepository,
    IngestTurn,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_publication_repository import (
    PublicationCorruptionError,
    PublicationHeadRaceError,
    PublicationNotReadyError,
    PublicationRepository,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

_CANDIDATE = b"c" * 16
_BUILD = b"b" * 16
_ANALYSIS = b"a" * 16
_PREPARATION = b"o" * 16
_CHANNEL = b"default"
_EMPTY_EVENT_CHAIN = sha256(b"h2hdb-operational-event-chain-v1\0").digest()
_PRODUCER_FIELDS = (
    b"h2hdb-test-writer",
    b"cpython-test-abi",
    b"pillow-test-build",
    b"libjpeg-test-build",
    b"zlib-test-build",
)
_PRODUCER_FINGERPRINT = identity.artifact_producer_fingerprint_sha256(*_PRODUCER_FIELDS)
_SOURCE_ROOT = b"r" * 32
_SCOPE_KEY = identity.source_scope_key("filesystem", _SOURCE_ROOT, 1)


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
    return connector


def _canonical_identity(
    connector: SQLiteConnector,
    value_sha256: bytes,
    *,
    domain: bytes,
    serial: int,
) -> None:
    page_bytes = b"publication-test-page\0" + serial.to_bytes(8, "big")
    page_sha256 = sha256(page_bytes).digest()
    seed_canonical_value(
        connector,
        value_sha256=value_sha256,
        digest_domain=domain,
        page_sha256=page_sha256,
        page_bytes=page_bytes,
        subtree_item_count=len(page_bytes),
        allocated_at=1,
    )


def _seed_static_catalog(connector: SQLiteConnector) -> tuple[bytes, bytes]:
    source_root = _SOURCE_ROOT
    snapshot = b"m" * 32
    policy_component = identity.artifact_policy_digest(
        1,
        2048,
        _PRODUCER_FINGERPRINT,
    )
    _canonical_identity(
        connector,
        source_root,
        domain=b"source_root_v1",
        serial=1,
    )
    _canonical_identity(
        connector,
        snapshot,
        domain=b"source_snapshot_manifest_v1",
        serial=2,
    )
    _canonical_identity(
        connector,
        policy_component,
        domain=b"artifact_policy_v2",
        serial=3,
    )
    scope = seed_source_scope(connector, source_root_sha256=source_root)
    assert scope.scope_key == _SCOPE_KEY
    seed_manifest_policy(connector)
    seed_analysis_policy(connector)
    producer = seed_artifact_producer_fingerprint(
        connector,
        artifact_algorithm_version=1,
        writer_id=_PRODUCER_FIELDS[0],
        python_abi=_PRODUCER_FIELDS[1],
        pillow_build=_PRODUCER_FIELDS[2],
        libjpeg_build=_PRODUCER_FIELDS[3],
        zlib_build=_PRODUCER_FIELDS[4],
    )
    assert producer.producer_fingerprint_sha256 == _PRODUCER_FINGERPRINT
    semantics = seed_artifact_policy_semantics(
        connector,
        producer_fingerprint_sha256=_PRODUCER_FINGERPRINT,
    )
    assert semantics.policy_component_sha256 == policy_component
    connector.execute(
        "INSERT INTO catalog_artifact_policies "
        "(artifact_policy_id, policy_component_sha256) VALUES (%s, %s)",
        (1, policy_component),
    )
    seed_title_sort_policy(connector)
    seed_display_title_policy(connector)
    seed_snapshot_manifest(
        connector,
        snapshot_manifest_sha256=snapshot,
        gallery_count=0,
        file_count=0,
        byte_count=0,
    )
    connector.execute(
        "INSERT INTO operational_operational_policys "
        "(operational_policy_id, operational_schema_version, "
        "algorithm_version, max_batch_rows) VALUES (%s, %s, %s, %s)",
        (1, 1, 1, 128),
    )
    return snapshot, policy_component


def _analysis_input_digest(build_manifest: bytes) -> bytes:
    payload = bytearray(b"h2hdb-vnext-analysis-input\0")
    payload.extend(build_manifest)
    payload.extend((0).to_bytes(8, "big") * 3)
    payload.extend((1).to_bytes(4, "big"))
    payload.extend((1).to_bytes(8, "big"))
    payload.extend((3).to_bytes(8, "big"))
    payload.extend((1).to_bytes(4, "big"))
    payload.extend((1).to_bytes(4, "big"))
    return sha256(payload).digest()


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


def _catalog_descriptor(
    connector: SQLiteConnector, *, revision: int, publication_count: int
) -> None:
    connector.execute(
        "INSERT INTO catalog_revision_anchors (revision) VALUES (%s)",
        (revision,),
    )
    connector.execute(
        "INSERT INTO catalog_revision_publication_counts "
        "(revision, publication_count) VALUES (%s, %s)",
        (revision, publication_count),
    )
    connector.execute(
        "INSERT INTO catalog_revision_descriptor_seals (revision) VALUES (%s)",
        (revision,),
    )


def _source_descriptor(
    connector: SQLiteConnector,
    *,
    source_revision: int,
    snapshot: bytes,
) -> None:
    connector.execute(
        "INSERT INTO catalog_source_revision_anchors (source_revision) VALUES (%s)",
        (source_revision,),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_channels "
        "(source_revision, channel) VALUES (%s, %s)",
        (source_revision, _CHANNEL),
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


def _publication_validation_receipts(connector: SQLiteConnector) -> None:
    stages = (
        b"VALIDATE_SELECTION",
        b"VALIDATE_CATALOG_PROJECTION",
        b"VALIDATE_ARTIFACT_INPUT_DELTA",
        b"VALIDATE_PREPARED_ARTIFACT",
        b"VALIDATE_CREATE",
        b"VALIDATE_REBUILD",
        b"VALIDATE_DELETE",
        b"VALIDATE_UNCHANGED",
        b"VALIDATE_NEW_GALLERY",
        b"VALIDATE_CHANGED_GALLERY",
        b"VALIDATE_REMOVED_GALLERY",
        b"VALIDATE_DUPLICATE_LOSER",
    )
    for stage in stages:
        checkpoint_key = (_CANDIDATE, stage)
        connector.execute(
            "INSERT INTO catalog_publication_checkpoint_anchors "
            "(candidate_id, stage) VALUES (%s, %s)",
            checkpoint_key,
        )
        for table, column, value in (
            ("catalog_publication_checkpoint_generations", "generation", 2),
            ("catalog_publication_checkpoint_cursors", "cursor", b""),
            (
                "catalog_publication_checkpoint_processed_counts",
                "processed_count",
                0,
            ),
            ("catalog_publication_checkpoint_states", "state", "COMPLETE"),
            ("catalog_publication_checkpoint_updated_ats", "updated_at", 44),
        ):
            connector.execute(
                f"INSERT INTO {table} (candidate_id, stage, {column}) "
                "VALUES (%s, %s, %s)",
                (*checkpoint_key, value),
            )
        connector.execute(
            "INSERT INTO catalog_publication_checkpoint_seals "
            "(candidate_id, stage) VALUES (%s, %s)",
            checkpoint_key,
        )

        batch_key = (_CANDIDATE, stage, 1)
        connector.execute(
            "INSERT INTO catalog_publication_batch_receipt_anchors "
            "(candidate_id, stage, start_generation) VALUES (%s, %s, %s)",
            batch_key,
        )
        connector.execute(
            "INSERT INTO catalog_publication_batch_receipt_coordinates "
            "(candidate_id, stage, batch_key, start_generation) "
            "VALUES (%s, %s, %s, %s)",
            (_CANDIDATE, stage, b"terminal", 1),
        )
        for table, column, value in (
            ("catalog_publication_batch_receipt_start_cursors", "start_cursor", b""),
            (
                "catalog_publication_batch_receipt_start_processed_counts",
                "start_processed_count",
                0,
            ),
            ("catalog_publication_batch_receipt_next_cursors", "next_cursor", b""),
            ("catalog_publication_batch_receipt_row_counts", "row_count", 0),
            ("catalog_publication_batch_receipt_committed_ats", "committed_at", 44),
        ):
            connector.execute(
                f"INSERT INTO {table} "
                f"(candidate_id, stage, start_generation, {column}) "
                "VALUES (%s, %s, %s, %s)",
                (*batch_key, value),
            )
        connector.execute(
            "INSERT INTO catalog_publication_batch_receipt_seals "
            "(candidate_id, stage, start_generation) VALUES (%s, %s, %s)",
            batch_key,
        )


def _permanent_finalization_checkpoint(
    connector: SQLiteConnector,
    *,
    receipt_id: bytes,
    updated_at: int,
) -> None:
    connector.execute(
        "INSERT INTO catalog_publication_finalization_checkpoint_anchors "
        "(receipt_id) VALUES (%s)",
        (receipt_id,),
    )
    for table, column, value in (
        ("catalog_publication_finalization_checkpoint_generations", "generation", 1),
        ("catalog_publication_finalization_checkpoint_cursors", "cursor", b""),
        ("catalog_publication_finalization_checkpoint_counts", "processed_count", 0),
        ("catalog_publication_finalization_checkpoint_states", "state", "OPEN"),
        (
            "catalog_publication_finalization_checkpoint_updated_ats",
            "updated_at",
            updated_at,
        ),
    ):
        connector.execute(
            f"INSERT INTO {table} (receipt_id, {column}) VALUES (%s, %s)",
            (receipt_id, value),
        )
    connector.execute(
        "INSERT INTO catalog_publication_finalization_checkpoint_seals "
        "(receipt_id) VALUES (%s)",
        (receipt_id,),
    )


def _base_publication_commit(connector: SQLiteConnector, *, snapshot: bytes) -> bytes:
    receipt_id = b"h" * 16
    candidate_id = b"x" * 16
    preparation_id = b"p" * 16
    base_build_id = b"z" * 16
    _source_descriptor(connector, source_revision=1, snapshot=snapshot)
    _catalog_descriptor(connector, revision=1, publication_count=0)
    seed_source_build(
        connector,
        build_id=base_build_id,
        scope_key=_SCOPE_KEY,
        state="SEALED",
        created_at=5,
        sealed_at=10,
    )
    connector.execute(
        "INSERT INTO operational_operational_event_streams "
        "(preparation_id, created_at) VALUES (%s, %s)",
        (preparation_id, 10),
    )
    connector.execute(
        "INSERT INTO operational_operational_preparations "
        "(preparation_id, build_id, deletion_request_generation, "
        "operational_policy_id, state, prepared_at, completed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (preparation_id, base_build_id, 0, 1, "COMPLETE", 10, 20),
    )
    connector.execute(
        "INSERT INTO operational_operational_preparation_effect_seals "
        "(preparation_id, event_count, final_chain_sha256, sealed_at) "
        "VALUES (%s, %s, %s, %s)",
        (preparation_id, 0, _EMPTY_EVENT_CHAIN, 20),
    )
    connector.execute(
        "INSERT INTO catalog_publication_generation_nodes (generation) VALUES (1)"
    )
    connector.execute(
        "INSERT INTO catalog_publication_generation_successors "
        "(successor_generation, predecessor_generation) VALUES (1, 0)"
    )
    connector.execute(
        "INSERT INTO catalog_publication_commit_anchors (receipt_id) VALUES (%s)",
        (receipt_id,),
    )
    members = (
        ("catalog_publication_commit_candidates", "candidate_id", candidate_id),
        ("catalog_publication_commit_catalog_revisions", "revision", 1),
        ("catalog_publication_commit_source_revisions", "source_revision", 1),
        ("catalog_publication_commit_generations", "generation", 1),
        (
            "catalog_publication_commit_operational_preparations",
            "preparation_id",
            preparation_id,
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
        ("catalog_publication_commit_committed_ats", "committed_at", 20),
    )
    for table, column, value in members:
        connector.execute(
            f"INSERT INTO {table} (receipt_id, {column}) VALUES (%s, %s)",
            (receipt_id, value),
        )
    _permanent_finalization_checkpoint(
        connector,
        receipt_id=receipt_id,
        updated_at=20,
    )
    connector.execute(
        "INSERT INTO catalog_publication_commit_seals (receipt_id) VALUES (%s)",
        (receipt_id,),
    )
    connector.execute(
        "INSERT INTO catalog_publication_commit_head_receipts "
        "(channel, receipt_id) VALUES (%s, %s)",
        (_CHANNEL, receipt_id),
    )
    return receipt_id


def _seed_candidate(
    connector: SQLiteConnector,
    turn: IngestTurn,
    *,
    with_base: bool = False,
) -> int:
    snapshot, _policy = _seed_static_catalog(connector)
    reserved_revision = 2 if with_base else 1
    build_manifest = b"d" * 32
    input_manifest = _analysis_input_digest(build_manifest)
    assert input_manifest != build_manifest

    base_receipt = (
        _base_publication_commit(connector, snapshot=snapshot) if with_base else None
    )
    if with_base:
        connector.execute(
            "UPDATE operational_revision_allocators SET next_revision = %s "
            "WHERE stream = %s",
            (2, "SOURCE"),
        )

    seed_source_build(
        connector,
        build_id=_BUILD,
        scope_key=_SCOPE_KEY,
        state="SEALED",
        created_at=15,
        sealed_at=20,
    )
    connector.execute(
        "INSERT INTO catalog_source_build_channel (build_id, channel) VALUES (%s, %s)",
        (_BUILD, _CHANNEL),
    )
    if with_base:
        connector.execute(
            "INSERT INTO catalog_source_build_base_publication_commits "
            "(build_id, base_receipt_id) VALUES (%s, %s)",
            (_BUILD, base_receipt),
        )
    seed_build_manifest(
        connector,
        build_id=_BUILD,
        manifest_sha256=build_manifest,
        gallery_count=0,
        file_count=0,
        byte_count=0,
        computed_at=20,
    )
    seed_analysis_run(
        connector,
        analysis_id=_ANALYSIS,
        build_id=_BUILD,
        policy_id=1,
        input_manifest_sha256=input_manifest,
        started_at=22,
        state="COMPLETE",
        completed_at=35,
    )
    for component in (
        b"content_owner",
        b"content_owner_candidate",
        b"file_hash_decision",
        b"gid_candidate",
        b"gid_winner",
    ):
        seed_analysis_component(
            connector,
            analysis_id=_ANALYSIS,
            state_component=component,
            row_count=0,
            sealed_at=34,
            terminal_receipt=True,
        )
    connector.execute(
        "INSERT INTO catalog_analysis_snapshot_manifest "
        "(analysis_id, snapshot_manifest_sha256) VALUES (%s, %s)",
        (_ANALYSIS, snapshot),
    )
    connector.execute(
        "INSERT INTO catalog_publication_candidate_anchors "
        "(candidate_id) VALUES (%s)",
        (_CANDIDATE,),
    )
    for table, column, value in (
        ("catalog_publication_candidate_analysis_ids", "analysis_id", _ANALYSIS),
        (
            "catalog_publication_candidate_reserved_revisions",
            "reserved_revision",
            reserved_revision,
        ),
        ("catalog_publication_candidate_artifact_policy_ids", "artifact_policy_id", 1),
        (
            "catalog_publication_candidate_display_title_policy_ids",
            "display_title_policy_id",
            1,
        ),
        ("catalog_publication_candidate_artifacts_required", "artifacts_required", 0),
        ("catalog_publication_candidate_created_ats", "created_at", 36),
    ):
        connector.execute(
            f"INSERT INTO {table} (candidate_id, {column}) VALUES (%s, %s)",
            (_CANDIDATE, value),
        )
    connector.execute(
        "INSERT INTO catalog_publication_candidate_definition_seals "
        "(candidate_id) VALUES (%s)",
        (_CANDIDATE,),
    )
    if with_base:
        connector.execute(
            "INSERT INTO catalog_publication_candidate_base_publication_commits "
            "(candidate_id, base_receipt_id) VALUES (%s, %s)",
            (_CANDIDATE, base_receipt),
        )
    _catalog_descriptor(
        connector,
        revision=reserved_revision,
        publication_count=0,
    )
    _publication_validation_receipts(connector)
    connector.execute(
        "INSERT INTO catalog_publication_candidate_projection_seals "
        "(candidate_id) VALUES (%s)",
        (_CANDIDATE,),
    )
    connector.execute(
        "INSERT INTO operational_source_build_generations "
        "(build_id, generation) VALUES (%s, %s)",
        (_BUILD, turn.generation),
    )
    connector.execute(
        "INSERT INTO operational_source_working_builds "
        "(slot, build_id, assigned_at) VALUES (%s, %s, %s)",
        (1, _BUILD, 15),
    )
    connector.execute(
        "INSERT INTO operational_catalog_working_candidates "
        "(slot, candidate_id, assigned_at) VALUES (%s, %s, %s)",
        (1, _CANDIDATE, 36),
    )
    connector.execute(
        "INSERT INTO operational_operational_event_streams "
        "(preparation_id, created_at) VALUES (%s, %s)",
        (_PREPARATION, 30),
    )
    connector.execute(
        "INSERT INTO operational_operational_preparations "
        "(preparation_id, build_id, deletion_request_generation, "
        "operational_policy_id, state, prepared_at, completed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (_PREPARATION, _BUILD, 0, 1, "COMPLETE", 30, 40),
    )
    connector.execute(
        "INSERT INTO operational_operational_preparation_effect_seals "
        "(preparation_id, event_count, final_chain_sha256, sealed_at) "
        "VALUES (%s, %s, %s, %s)",
        (_PREPARATION, 0, _EMPTY_EVENT_CHAIN, 40),
    )
    connector.execute(
        "INSERT INTO operational_publication_candidate_preparations "
        "(candidate_id, preparation_id, bound_at) VALUES (%s, %s, %s)",
        (_CANDIDATE, _PREPARATION, 42),
    )
    connector.execute(
        "UPDATE operational_revision_allocators SET next_revision = %s "
        "WHERE stream = %s",
        (reserved_revision + 1, "CATALOG"),
    )
    return reserved_revision


def _commit(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    *,
    now: int = 100,
) -> Any:
    with connector.transaction():
        with patch(
            "h2hdb.vnext_publication_repository._new_receipt_id",
            return_value=b"q" * 16,
        ):
            return PublicationRepository.commit(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                now=now,
            )


def _candidate_lifecycle(connector: SQLiteConnector) -> str:
    if connector.fetch_one(
        "SELECT 1 FROM catalog_publication_commit_candidates "
        "WHERE candidate_id = %s",
        (_CANDIDATE,),
    ):
        return "PUBLISHED"
    if connector.fetch_one(
        "SELECT 1 FROM catalog_publication_candidate_projection_seals "
        "WHERE candidate_id = %s",
        (_CANDIDATE,),
    ):
        return "SEALED"
    if connector.fetch_one(
        "SELECT 1 FROM catalog_publication_candidate_definition_seals "
        "WHERE candidate_id = %s",
        (_CANDIDATE,),
    ):
        return "OPEN"
    raise AssertionError("candidate definition is absent")


@pytest.mark.parametrize("with_base", [False, True], ids=["genesis", "successor"])
def test_atomic_publication_genesis_and_successor(
    tmp_path: Path, with_base: bool
) -> None:
    connector = _generated_database(tmp_path / f"publish-{with_base}.sqlite3")
    gate, turn = _authorities(connector)
    reserved = _seed_candidate(connector, turn, with_base=with_base)

    receipt = _commit(connector, gate, turn)

    assert receipt.revision == reserved
    assert receipt.source_revision == reserved
    assert not receipt.replayed
    assert connector.fetch_one(
        "SELECT source_revision, generation FROM catalog_source_heads "
        "WHERE channel = %s",
        (_CHANNEL,),
    ) == (reserved, 2 if with_base else 1)
    assert connector.fetch_one(
        "SELECT revision, generation FROM catalog_publication_heads WHERE channel = %s",
        (_CHANNEL,),
    ) == (reserved, 2 if with_base else 1)
    assert connector.fetch_one(
        "SELECT source_revision, generation "
        "FROM catalog_source_revision_generations WHERE source_revision = %s",
        (reserved,),
    ) == (reserved, 2 if with_base else 1)
    assert connector.fetch_one(
        "SELECT revision, generation FROM catalog_revision_generations "
        "WHERE revision = %s",
        (reserved,),
    ) == (reserved, 2 if with_base else 1)
    assert _candidate_lifecycle(connector) == "PUBLISHED"
    assert not connector.fetch_one(
        "SELECT 1 FROM operational_source_working_builds WHERE build_id = %s",
        (_BUILD,),
    )
    assert not connector.fetch_one(
        "SELECT 1 FROM operational_catalog_working_candidates WHERE candidate_id = %s",
        (_CANDIDATE,),
    )
    connector.close()


def test_response_loss_replay_and_new_turn_recovery_need_no_old_mapping(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "recovery.sqlite3")
    gate, turn = _authorities(connector)
    _seed_candidate(connector, turn)
    first = _commit(connector, gate, turn)
    replay = _commit(connector, gate, turn, now=101)
    assert replay.replayed and replay.receipt_id == first.receipt_id

    with connector.transaction():
        IngestFenceRepository.complete(
            VNextUnitOfWork(connector, backend="sqlite"),
            turn,
            now=110,
        )
    with connector.transaction():
        successor = IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend="sqlite"),
            owner_token=b"n" * 16,
            now=111,
            lease_duration=1_000_000,
        )
    recovered = _commit(connector, gate, successor, now=112)
    assert recovered.replayed and recovered.receipt_id == first.receipt_id
    assert not connector.fetch_one(
        "SELECT 1 FROM operational_source_build_generations WHERE generation = %s",
        (successor.generation,),
    )
    assert connector.fetch_one(
        "SELECT generation, cursor, processed_count, state "
        "FROM catalog_publication_finalization_checkpoints "
        "WHERE receipt_id = %s",
        (first.receipt_id,),
    ) == (1, b"", 0, "OPEN")
    connector.close()


@pytest.mark.parametrize("corruption", ["edge", "genesis-node"])
def test_response_loss_replay_rejects_corrupt_generation_chain(
    tmp_path: Path,
    corruption: str,
) -> None:
    connector = _generated_database(tmp_path / f"chain-corrupt-{corruption}.sqlite3")
    gate, turn = _authorities(connector)
    _seed_candidate(connector, turn)
    _commit(connector, gate, turn)
    connector.execute("PRAGMA foreign_keys = OFF")
    if corruption == "edge":
        connector.execute(
            "DELETE FROM catalog_publication_generation_successors "
            "WHERE successor_generation = 1"
        )
    else:
        connector.execute(
            "DELETE FROM catalog_publication_generation_nodes WHERE generation = 0"
        )
    connector.execute("PRAGMA foreign_keys = ON")

    with pytest.raises(PublicationCorruptionError, match="generation"):
        _commit(connector, gate, turn, now=101)
    assert _candidate_lifecycle(connector) == "PUBLISHED"
    assert connector.fetch_one("SELECT COUNT(*) FROM catalog_publication_receipts") == (
        1,
    )
    connector.close()


@pytest.mark.parametrize("race", ["common-head", "deletion"])
def test_head_races_fail_without_partial_publication(tmp_path: Path, race: str) -> None:
    connector = _generated_database(tmp_path / f"race-{race}.sqlite3")
    gate, turn = _authorities(connector)
    reserved = _seed_candidate(connector, turn, with_base=race == "common-head")
    if race == "common-head":
        connector.execute(
            "DELETE FROM catalog_publication_candidate_base_publication_commits "
            "WHERE candidate_id = %s",
            (_CANDIDATE,),
        )
        connector.execute(
            "DELETE FROM catalog_source_build_base_publication_commits "
            "WHERE build_id = %s",
            (_BUILD,),
        )
    else:
        connector.execute(
            "INSERT INTO operational_deletion_request_generations "
            "(generation, allocated_at) VALUES (%s, %s)",
            (1, 50),
        )
        connector.execute(
            "UPDATE operational_deletion_request_generation_heads "
            "SET current_generation = %s, updated_at = %s WHERE singleton_id = %s",
            (1, 50, 1),
        )

    with pytest.raises(PublicationHeadRaceError):
        _commit(connector, gate, turn)

    assert _candidate_lifecycle(connector) == "SEALED"
    assert not connector.fetch_one(
        "SELECT 1 FROM catalog_publication_commit_candidates WHERE candidate_id = %s",
        (_CANDIDATE,),
    )
    assert not connector.fetch_one(
        "SELECT 1 FROM catalog_source_revision_descriptor_seals "
        "WHERE source_revision = %s",
        (reserved,),
    )
    connector.close()


@pytest.mark.parametrize("authority", ["projection", "preparation"])
def test_missing_or_conflicting_o1_authority_fails_closed(
    tmp_path: Path,
    authority: str,
) -> None:
    connector = _generated_database(tmp_path / f"authority-{authority}.sqlite3")
    gate, turn = _authorities(connector)
    _seed_candidate(connector, turn)
    expected_error: type[Exception]
    if authority == "projection":
        connector.execute(
            "DELETE FROM catalog_publication_candidate_projection_seals "
            "WHERE candidate_id = %s",
            (_CANDIDATE,),
        )
        expected_error = PublicationNotReadyError
    else:
        connector.execute(
            "DELETE FROM operational_publication_candidate_preparations "
            "WHERE candidate_id = %s",
            (_CANDIDATE,),
        )
        expected_error = PublicationNotReadyError

    with pytest.raises(expected_error):
        _commit(connector, gate, turn)

    expected_lifecycle = "OPEN" if authority == "projection" else "SEALED"
    assert _candidate_lifecycle(connector) == expected_lifecycle
    assert not connector.fetch_one("SELECT 1 FROM catalog_publication_receipts")
    assert not connector.fetch_one("SELECT 1 FROM operational_operational_activations")
    connector.close()


def test_each_pointer_mutation_fault_rolls_back_the_whole_publication(
    tmp_path: Path,
) -> None:
    base_path = tmp_path / "fault-base.sqlite3"
    base = _generated_database(base_path)
    gate, turn = _authorities(base)
    _seed_candidate(base, turn)
    base.close()

    # Genesis has 33 pointer-transaction mutations: allocator; generation node
    # and edge; four source-descriptor members; provenance; commit anchor,
    # thirteen members, seven-member permanent finalization checkpoint family,
    # and commit seal; common head; and two exact working-root deletes.
    for failure_at in range(1, 34):
        path = tmp_path / f"fault-{failure_at}.sqlite3"
        copyfile(base_path, path)
        connector = SQLiteConnector(str(path))
        connector.connect()
        original_execute = connector.execute
        original_execute_affected = connector.execute_affected
        mutation_number = 0

        def before_mutation() -> None:
            nonlocal mutation_number
            mutation_number += 1
            if mutation_number == failure_at:
                raise RuntimeError(f"injected publication mutation {failure_at}")

        def failing_execute(
            query: str,
            data: tuple[Any, ...] = (),
        ) -> None:
            before_mutation()
            original_execute(query, data)

        def failing_execute_affected(
            query: str,
            data: tuple[Any, ...] = (),
        ) -> int:
            before_mutation()
            return original_execute_affected(query, data)

        with (
            patch.object(connector, "execute", side_effect=failing_execute),
            patch.object(
                connector,
                "execute_affected",
                side_effect=failing_execute_affected,
            ),
            pytest.raises(
                RuntimeError,
                match=f"injected publication mutation {failure_at}",
            ),
        ):
            _commit(connector, gate, turn)

        assert mutation_number == failure_at
        assert connector.fetch_one(
            "SELECT next_revision FROM operational_revision_allocators "
            "WHERE stream = %s",
            ("SOURCE",),
        ) == (1,)
        assert _candidate_lifecycle(connector) == "SEALED"
        assert not connector.fetch_one("SELECT 1 FROM catalog_source_revisions")
        assert not connector.fetch_one("SELECT 1 FROM catalog_source_head_revisions")
        assert not connector.fetch_one("SELECT 1 FROM catalog_source_head_advanced_ats")
        assert not connector.fetch_one(
            "SELECT 1 FROM catalog_publication_head_revisions"
        )
        assert not connector.fetch_one(
            "SELECT 1 FROM catalog_publication_head_advanced_ats"
        )
        assert not connector.fetch_one(
            "SELECT 1 FROM catalog_source_revision_generations"
        )
        assert not connector.fetch_one("SELECT 1 FROM catalog_revision_generations")
        assert not connector.fetch_one("SELECT 1 FROM catalog_publication_receipts")
        assert not connector.fetch_one(
            "SELECT 1 FROM operational_operational_activations"
        )
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_working_builds WHERE slot = %s",
            (1,),
        ) == (_BUILD,)
        assert connector.fetch_one(
            "SELECT candidate_id FROM operational_catalog_working_candidates "
            "WHERE slot = %s",
            (1,),
        ) == (_CANDIDATE,)
        connector.close()


def test_pointer_transaction_reads_only_o1_and_fixed_seal_authorities(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "bounded-publish.sqlite3")
    gate, turn = _authorities(connector)
    _seed_candidate(connector, turn)
    statements: list[str] = []
    original_fetch_one = connector.fetch_one
    original_fetch_all = connector.fetch_all

    def recording_fetch_one(
        query: str,
        data: tuple[Any, ...] = (),
    ) -> tuple[Any, ...]:
        statements.append(query)
        return original_fetch_one(query, data)

    def recording_fetch_all(
        query: str,
        data: tuple[Any, ...] = (),
    ) -> list[tuple[Any, ...]]:
        statements.append(query)
        return original_fetch_all(query, data)

    with (
        patch.object(connector, "fetch_one", side_effect=recording_fetch_one),
        patch.object(connector, "fetch_all", side_effect=recording_fetch_all),
    ):
        _commit(connector, gate, turn)

    normalized = tuple(" ".join(statement.upper().split()) for statement in statements)
    assert not any(
        "COUNT(" in statement or "SUM(" in statement for statement in normalized
    )
    assert not any(
        legacy in statement
        for statement in normalized
        for legacy in (
            "CATALOG_CANONICAL_VALUE_ALLOCATIONS",
            "CATALOG_CANONICAL_VALUE_PAGES ",
            "CATALOG_CANONICAL_VALUE_PAGE_DESCRIPTORS",
        )
    )
    canonical_authority_queries = tuple(
        statement
        for statement in normalized
        if "CATALOG_CANONICAL_VALUE_ALLOCATION_SEALS" in statement
    )
    assert len(canonical_authority_queries) == 1
    assert "CATALOG_CANONICAL_VALUE_PAGE_SEALS" in canonical_authority_queries[0]
    candidate_lock_queries = tuple(
        statement
        for statement in normalized
        if "FROM CATALOG_PUBLICATION_CANDIDATES C" in statement
    )
    assert len(candidate_lock_queries) == 1
    assert "CATALOG_CANONICAL_VALUE_" not in candidate_lock_queries[0]
    high_cardinality_children = (
        "CATALOG_PUBLICATIONS",
        "CATALOG_PUBLICATION_ORDER",
        "CATALOG_CONTRIBUTORS",
        "CATALOG_SUBJECTS",
        "CATALOG_ARTIFACTS",
        "CATALOG_PREPARED_ARTIFACTS",
        "OPERATIONAL_OPERATIONAL_EVENTS",
    )
    for table in high_cardinality_children:
        assert not any(
            f" FROM {table} " in f" {statement} "
            or f" JOIN {table} " in f" {statement} "
            for statement in normalized
        )

    projection_plan = connector.fetch_all(
        "EXPLAIN QUERY PLAN SELECT candidate_id "
        "FROM catalog_publication_candidate_projection_seals "
        "WHERE candidate_id = %s",
        (_CANDIDATE,),
    )
    preparation_plan = connector.fetch_all(
        "EXPLAIN QUERY PLAN SELECT preparation_id "
        "FROM operational_publication_candidate_preparations "
        "WHERE candidate_id = %s",
        (_CANDIDATE,),
    )
    assert any("SEARCH" in str(row[3]).upper() for row in projection_plan)
    assert any("SEARCH" in str(row[3]).upper() for row in preparation_plan)
    connector.close()


def test_mariadb_publication_lock_sql_uses_server_placeholders_and_for_update() -> None:
    import h2hdb.vnext_publication_repository as module

    class RecordingConnector:
        def __init__(self) -> None:
            self.query = ""

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            self.query = query
            return (data[0], None, None, None, None, None, None, None)

    connector: Any = RecordingConnector()
    assert (
        module._lock_publication_commit_head(
            VNextUnitOfWork(connector, backend="mariadb"),
            _CHANNEL,
        )
        is None
    )
    assert connector.query.endswith(" FOR UPDATE")
    assert "%s" in connector.query and "?" not in connector.query
