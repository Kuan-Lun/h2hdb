from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from shutil import copyfile
from typing import Any
from unittest.mock import patch

import pytest

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
    PreparedArtifactRelease,
    PublicationConflictError,
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
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocations "
        "(value_sha256, digest_domain, byte_count, allocated_at) "
        "VALUES (%s, %s, %s, %s)",
        (value_sha256, domain, len(page_bytes), 1),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_pages "
        "(page_sha256, value_sha256, page_bytes) VALUES (%s, %s, %s)",
        (page_sha256, value_sha256, page_bytes),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_identities "
        "(value_sha256, root_page_sha256) VALUES (%s, %s)",
        (value_sha256, page_sha256),
    )


def _seed_static_catalog(connector: SQLiteConnector) -> tuple[bytes, bytes]:
    source_root = b"r" * 32
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
    connector.execute(
        "INSERT INTO catalog_source_scopes "
        "(scope_key, source_provider, source_root_sha256, identity_policy_version) "
        "VALUES (%s, %s, %s, %s)",
        (b"s" * 32, b"filesystem", source_root, 1),
    )
    connector.execute(
        "INSERT INTO catalog_manifest_policies "
        "(manifest_policy_id, manifest_algorithm_version, file_order_version) "
        "VALUES (%s, %s, %s)",
        (1, 1, 1),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_policies "
        "(policy_id, algorithm_version, spam_artist_threshold, "
        "spam_occurrence_threshold, content_owner_rule_version, "
        "gid_winner_rule_version) VALUES (%s, %s, %s, %s, %s, %s)",
        (1, 1, 1, 3, 1, 1),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_producer_fingerprints "
        "(producer_fingerprint_sha256, artifact_algorithm_version, "
        "producer_equivalence_class, writer_id, python_abi, pillow_build, "
        "libjpeg_build, zlib_build) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            _PRODUCER_FINGERPRINT,
            1,
            b"test-equivalence-v1",
            *_PRODUCER_FIELDS,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics "
        "(policy_component_sha256, artifact_algorithm_version, "
        "max_image_short_side, producer_fingerprint_sha256) "
        "VALUES (%s, %s, %s, %s)",
        (policy_component, 1, 2048, _PRODUCER_FINGERPRINT),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policies "
        "(artifact_policy_id, policy_component_sha256) VALUES (%s, %s)",
        (1, policy_component),
    )
    connector.execute(
        "INSERT INTO catalog_title_sort_policy "
        "(title_sort_policy_id, title_sort_algorithm_version, "
        "unicode_data_version) VALUES (%s, %s, %s)",
        (1, 1, b"14.0.0"),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policies "
        "(display_title_policy_id, display_title_algorithm_version, "
        "title_sort_policy_id) VALUES (%s, %s, %s)",
        (1, 1, 1),
    )
    connector.execute(
        "INSERT INTO catalog_source_snapshot_manifest_identity "
        "(snapshot_manifest_sha256, gallery_count, file_count, byte_count) "
        "VALUES (%s, %s, %s, %s)",
        (snapshot, 0, 0, 0),
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

    if with_base:
        connector.execute(
            "INSERT INTO catalog_source_revisions "
            "(source_revision, channel, snapshot_manifest_sha256, published_at) "
            "VALUES (%s, %s, %s, %s)",
            (1, _CHANNEL, snapshot, 20),
        )
        connector.execute(
            "INSERT INTO catalog_source_heads "
            "(channel, source_revision, generation, advanced_at) "
            "VALUES (%s, %s, %s, %s)",
            (_CHANNEL, 1, 1, 20),
        )
        connector.execute(
            "INSERT INTO catalog_revisions "
            "(revision, publication_count, published_at) VALUES (%s, %s, %s)",
            (1, 0, 20),
        )
        connector.execute(
            "INSERT INTO catalog_publication_heads "
            "(channel, revision, generation, advanced_at) "
            "VALUES (%s, %s, %s, %s)",
            (_CHANNEL, 1, 1, 20),
        )
        connector.execute(
            "UPDATE operational_revision_allocators SET next_revision = %s "
            "WHERE stream = %s",
            (2, "SOURCE"),
        )

    connector.execute(
        "INSERT INTO catalog_source_builds "
        "(build_id, scope_key, manifest_policy_id, state, created_at, sealed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (_BUILD, b"s" * 32, 1, "SEALED", 15, 20),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_channel (build_id, channel) "
        "VALUES (%s, %s)",
        (_BUILD, _CHANNEL),
    )
    if with_base:
        connector.execute(
            "INSERT INTO catalog_source_build_base_source "
            "(build_id, base_source_revision, base_source_generation) "
            "VALUES (%s, %s, %s)",
            (_BUILD, 1, 1),
        )
    connector.execute(
        "INSERT INTO catalog_build_manifests "
        "(build_id, manifest_sha256, gallery_count, file_count, byte_count, "
        "computed_at) VALUES (%s, %s, %s, %s, %s, %s)",
        (_BUILD, build_manifest, 0, 0, 0, 21),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_runs "
        "(analysis_id, build_id, policy_id, input_manifest_sha256, state, "
        "started_at, completed_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (_ANALYSIS, _BUILD, 1, input_manifest, "COMPLETE", 22, 35),
    )
    for component in (
        b"content_owner",
        b"content_owner_candidate",
        b"file_hash_decision",
        b"gid_candidate",
        b"gid_winner",
    ):
        connector.execute(
            "INSERT INTO catalog_analysis_state_component_seals "
            "(analysis_id, state_component, row_count, sealed_at) "
            "VALUES (%s, %s, %s, %s)",
            (_ANALYSIS, component, 0, 34),
        )
    connector.execute(
        "INSERT INTO catalog_analysis_snapshot_manifest "
        "(analysis_id, snapshot_manifest_sha256) VALUES (%s, %s)",
        (_ANALYSIS, snapshot),
    )
    connector.execute(
        "INSERT INTO catalog_publication_candidates "
        "(candidate_id, analysis_id, reserved_revision, channel, "
        "artifact_policy_id, display_title_policy_id, artifacts_required, "
        "state, created_at, sealed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (_CANDIDATE, _ANALYSIS, reserved_revision, _CHANNEL, 1, 1, 0, "SEALED", 36, 45),
    )
    if with_base:
        connector.execute(
            "INSERT INTO catalog_publication_candidate_base_sources "
            "(candidate_id, base_source_revision, base_source_generation) "
            "VALUES (%s, %s, %s)",
            (_CANDIDATE, 1, 1),
        )
        connector.execute(
            "INSERT INTO catalog_publication_candidate_base_catalog "
            "(candidate_id, base_revision, base_catalog_generation) "
            "VALUES (%s, %s, %s)",
            (_CANDIDATE, 1, 1),
        )
    connector.execute(
        "INSERT INTO catalog_revisions "
        "(revision, publication_count, published_at) VALUES (%s, %s, %s)",
        (reserved_revision, 0, 44),
    )
    connector.execute(
        "INSERT INTO catalog_publication_candidate_projection_seal "
        "(candidate_id, publication_count, artifact_input_count, "
        "prepared_artifact_count, create_count, rebuild_count, delete_count, "
        "unchanged_count, new_galleries, changed_galleries, removed_galleries, "
        "duplicate_losers, projection_sealed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (_CANDIDATE, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 44),
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


def _seed_one_prepared_artifact(connector: SQLiteConnector) -> None:
    locator = b"l" * 32
    publication_key = b"k" * 32
    artifact_sha256 = b"z" * 32
    _canonical_identity(
        connector,
        locator,
        domain=b"source_relative_locator_v1",
        serial=4,
    )
    connector.execute(
        "INSERT INTO catalog_source_locator_identity "
        "(locator_sha256, source_gallery_name) VALUES (%s, %s)",
        (locator, b"gallery"),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_identities "
        "(gallery_id, gallery_key, scope_key, locator_sha256) "
        "VALUES (%s, %s, %s, %s)",
        (1, b"y" * 32, b"s" * 32, locator),
    )
    connector.execute(
        "INSERT INTO catalog_publication_selections "
        "(candidate_id, gallery_id, publication_key) "
        "VALUES (%s, %s, %s)",
        (_CANDIDATE, 1, publication_key),
    )
    connector.execute(
        "INSERT INTO catalog_publication_identities "
        "(publication_key, publication_id, gid, artifact_name) "
        "VALUES (%s, %s, %s, %s)",
        (publication_key, identity.publication_id(1), 1, identity.artifact_name(1)),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_blobs (artifact_sha256, size_bytes) "
        "VALUES (%s, %s)",
        (artifact_sha256, 1),
    )
    connector.execute(
        "INSERT INTO catalog_prepared_artifacts "
        "(candidate_id, publication_key, artifact_sha256, storage_codec_version, "
        "protection_token, state) VALUES (%s, %s, %s, %s, %s, %s)",
        (
            _CANDIDATE,
            publication_key,
            artifact_sha256,
            1,
            b"p" * 184,
            "PREPARED",
        ),
    )
    connector.execute(
        "UPDATE catalog_publication_candidates SET artifacts_required = %s "
        "WHERE candidate_id = %s",
        (1, _CANDIDATE),
    )
    connector.execute(
        "UPDATE catalog_publication_candidate_projection_seal "
        "SET publication_count = %s, artifact_input_count = %s, "
        "prepared_artifact_count = %s, create_count = %s, "
        "new_galleries = %s WHERE candidate_id = %s",
        (1, 1, 1, 1, 1, _CANDIDATE),
    )
    connector.execute(
        "UPDATE catalog_revisions SET publication_count = %s WHERE revision = %s",
        (1, 1),
    )


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
        "SELECT revision, generation FROM catalog_publication_heads "
        "WHERE channel = %s",
        (_CHANNEL,),
    ) == (reserved, 2 if with_base else 1)
    assert connector.fetch_one(
        "SELECT state FROM catalog_publication_candidates WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == ("PUBLISHED",)
    assert not connector.fetch_one(
        "SELECT 1 FROM operational_source_working_builds WHERE build_id = %s",
        (_BUILD,),
    )
    assert not connector.fetch_one(
        "SELECT 1 FROM operational_catalog_working_candidates "
        "WHERE candidate_id = %s",
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
    with connector.transaction():
        terminal = PublicationRepository.finalize_artifacts(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=successor,
            candidate_id=_CANDIDATE,
            batch_key=b"new-turn-terminal",
            release_artifacts=lambda _releases: None,
            now=113,
        )
    assert terminal.terminal
    assert terminal.receipt_state == "PROJECTION_FINALIZED"
    connector.close()


@pytest.mark.parametrize("head", ["source", "catalog", "deletion"])
def test_head_races_fail_without_partial_publication(tmp_path: Path, head: str) -> None:
    connector = _generated_database(tmp_path / f"race-{head}.sqlite3")
    gate, turn = _authorities(connector)
    _seed_candidate(connector, turn, with_base=head != "deletion")
    if head == "source":
        connector.execute(
            "UPDATE catalog_source_heads SET generation = %s WHERE channel = %s",
            (2, _CHANNEL),
        )
    elif head == "catalog":
        connector.execute(
            "UPDATE catalog_publication_heads SET generation = %s WHERE channel = %s",
            (2, _CHANNEL),
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

    assert connector.fetch_one(
        "SELECT state FROM catalog_publication_candidates WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == ("SEALED",)
    assert not connector.fetch_one("SELECT 1 FROM catalog_publication_receipts")
    assert not connector.fetch_one("SELECT 1 FROM operational_operational_activations")
    assert not connector.fetch_one(
        "SELECT 1 FROM catalog_source_revisions WHERE source_revision > %s",
        (1 if head != "deletion" else 0,),
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
            "UPDATE catalog_publication_candidate_projection_seal "
            "SET publication_count = %s WHERE candidate_id = %s",
            (1, _CANDIDATE),
        )
        expected_error = PublicationConflictError
    else:
        connector.execute(
            "DELETE FROM operational_publication_candidate_preparations "
            "WHERE candidate_id = %s",
            (_CANDIDATE,),
        )
        expected_error = PublicationNotReadyError

    with pytest.raises(expected_error):
        _commit(connector, gate, turn)

    assert connector.fetch_one(
        "SELECT state FROM catalog_publication_candidates WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == ("SEALED",)
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

    # Genesis has ten pointer-transaction mutations: allocator, source
    # descriptor/provenance, activation, receipt, two heads, candidate CAS,
    # and two exact working-root deletes.
    for failure_at in range(1, 11):
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
        assert connector.fetch_one(
            "SELECT state FROM catalog_publication_candidates "
            "WHERE candidate_id = %s",
            (_CANDIDATE,),
        ) == ("SEALED",)
        assert not connector.fetch_one("SELECT 1 FROM catalog_source_revisions")
        assert not connector.fetch_one("SELECT 1 FROM catalog_source_heads")
        assert not connector.fetch_one("SELECT 1 FROM catalog_publication_heads")
        assert not connector.fetch_one("SELECT 1 FROM catalog_publication_receipts")
        assert not connector.fetch_one(
            "SELECT 1 FROM operational_operational_activations"
        )
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_working_builds " "WHERE slot = %s",
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
        "EXPLAIN QUERY PLAN SELECT publication_count "
        "FROM catalog_publication_candidate_projection_seal "
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


def test_empty_terminal_finalize_is_receipted_and_replay_safe(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "finalize.sqlite3")
    gate, turn = _authorities(connector)
    _seed_candidate(connector, turn)
    _commit(connector, gate, turn)
    releases: list[tuple[PreparedArtifactRelease, ...]] = []

    with connector.transaction():
        terminal = PublicationRepository.finalize_artifacts(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            batch_key=b"terminal",
            release_artifacts=releases.append,
            now=120,
        )
    assert terminal.terminal and terminal.row_count == 0
    assert terminal.receipt_state == "PROJECTION_FINALIZED"
    assert releases == []

    with connector.transaction():
        replay = PublicationRepository.finalize_artifacts(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            batch_key=b"terminal",
            release_artifacts=releases.append,
            now=121,
        )
    assert replay.replayed and replay == terminal.__class__(
        **{
            **{
                field: getattr(terminal, field)
                for field in terminal.__dataclass_fields__
            },
            "replayed": True,
        }
    )
    assert connector.fetch_one(
        "SELECT state, finalized_at FROM catalog_publication_receipts"
    ) == ("PROJECTION_FINALIZED", 120)
    connector.close()


def test_finalize_artifacts_pages_then_requires_a_terminal_empty_batch(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "finalize-one.sqlite3")
    gate, turn = _authorities(connector)
    _seed_candidate(connector, turn)
    _seed_one_prepared_artifact(connector)
    _commit(connector, gate, turn)
    releases: list[tuple[PreparedArtifactRelease, ...]] = []

    with connector.transaction():
        page = PublicationRepository.finalize_artifacts(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            batch_key=b"artifact-page",
            release_artifacts=releases.append,
            now=120,
        )
    assert not page.terminal
    assert page.row_count == 1 and page.next_processed_count == 1
    assert page.receipt_state == "DB_COMMITTED"
    assert len(releases) == 1 and len(releases[0]) == 1
    assert connector.fetch_one(
        "SELECT state FROM catalog_prepared_artifacts " "WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == ("COMMITTED",)

    with connector.transaction():
        replay = PublicationRepository.finalize_artifacts(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            batch_key=b"artifact-page",
            release_artifacts=releases.append,
            now=121,
        )
    assert replay.replayed and replay.row_count == 1
    assert len(releases) == 1

    with connector.transaction():
        terminal = PublicationRepository.finalize_artifacts(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            batch_key=b"terminal-empty",
            release_artifacts=releases.append,
            now=122,
        )
    assert terminal.terminal and terminal.row_count == 0
    assert terminal.next_processed_count == 1
    assert terminal.receipt_state == "PROJECTION_FINALIZED"
    assert len(releases) == 1
    connector.close()


def test_finalize_callback_failure_rolls_back_checkpoint_and_artifact_state(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "finalize-fault.sqlite3")
    gate, turn = _authorities(connector)
    _seed_candidate(connector, turn)
    _seed_one_prepared_artifact(connector)
    _commit(connector, gate, turn)

    def fail_release(_releases: tuple[PreparedArtifactRelease, ...]) -> None:
        raise RuntimeError("injected protection-release failure")

    with pytest.raises(RuntimeError, match="protection-release failure"):
        with connector.transaction():
            PublicationRepository.finalize_artifacts(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                batch_key=b"fault",
                release_artifacts=fail_release,
                now=120,
            )

    assert connector.fetch_one(
        "SELECT state FROM catalog_prepared_artifacts WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == ("PREPARED",)
    assert not connector.fetch_one("SELECT 1 FROM catalog_publication_checkpoints")
    assert not connector.fetch_one("SELECT 1 FROM catalog_publication_batch_receipts")
    assert connector.fetch_one(
        "SELECT state, finalized_at FROM catalog_publication_receipts"
    ) == ("DB_COMMITTED", None)
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
            del data
            self.query = query
            return ()

    connector: Any = RecordingConnector()
    assert (
        module._lock_source_head(
            VNextUnitOfWork(connector, backend="mariadb"),
            _CHANNEL,
        )
        is None
    )
    assert connector.query.endswith(" FOR UPDATE")
    assert "%s" in connector.query and "?" not in connector.query
