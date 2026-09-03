from __future__ import annotations

import unicodedata
from hashlib import sha256
from pathlib import Path
from shutil import copyfile
from typing import Any
from unittest.mock import patch

import pytest
from vnext_analysis_fixtures import (
    seed_analysis_component,
    seed_analysis_run,
    set_analysis_component_live_count,
)
from vnext_canonical_value_fixtures import seed_canonical_value
from vnext_catalog_identity_fixtures import seed_gallery_identity, seed_tag_term
from vnext_catalog_registry_fixtures import (
    seed_analysis_policy,
    seed_artifact_policy_semantics,
    seed_display_title_policy,
    seed_manifest_policy,
    seed_source_scope,
    seed_title_sort_policy,
)
from vnext_gallery_page_fixtures import (
    seed_gallery_page_bounds,
    seed_gallery_page_descriptor,
)
from vnext_generated_database import open_generated_sqlite_database
from vnext_manifest_fixtures import (
    seed_sealed_source_build,
    seed_snapshot_manifest,
)
from vnext_publication_fixtures import seed_publication_commit

from h2hdb import catalog_refinement
from h2hdb import vnext_identity as identity
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_canonical_value_repository import CanonicalValueRepository
from h2hdb.vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_publication_candidate_repository import (
    PublicationCandidate,
    PublicationCandidateConflictError,
    PublicationCandidateCorruptionError,
    PublicationCandidateHeadRaceError,
    PublicationCandidateNotReadyError,
    PublicationCandidateRepository,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

_BUILD = b"b" * 16
_ANALYSIS = b"a" * 16
_CANDIDATE = b"c" * 16
_BASE_RECEIPT = b"q" * 16
_BASE_CANDIDATE = b"p" * 16
_BASE_PREPARATION = b"e" * 16
_ARTIFACT_ADAPTER_ID = b"test-artifact-adapter"
_ARTIFACT_POLICY_FINGERPRINT = b"p" * 32
_CHANNEL = b"default"


def test_publication_candidate_dto_rejects_non_graph_abandoned_state() -> None:
    with pytest.raises(ValueError, match="state is not registered"):
        PublicationCandidate(
            candidate_id=_CANDIDATE,
            analysis_id=_ANALYSIS,
            build_id=_BUILD,
            reserved_revision=1,
            channel=_CHANNEL,
            artifact_policy_id=1,
            display_title_policy_id=1,
            artifacts_required=False,
            state="ABANDONED",
            created_at=1,
            base_source_revision=None,
            base_source_generation=None,
            base_catalog_revision=None,
            base_catalog_generation=None,
            replayed=False,
        )


def _generated_database(path: Path) -> SQLiteConnector:
    return open_generated_sqlite_database(path)


def _canonical_identity(
    connector: SQLiteConnector,
    value_sha256: bytes,
    *,
    domain: bytes,
    serial: int,
    payload: bytes | None = None,
) -> None:
    exact_payload = (
        b"candidate-test-page\0" + serial.to_bytes(8, "big")
        if payload is None
        else payload
    )
    tree = identity.build_canonical_value_tree(
        value_sha256,
        len(exact_payload),
        (exact_payload,),
    )
    assert len(tree.pages) == 1
    encoded_page = tree.pages[0]
    page = identity.decode_canonical_value_page(encoded_page.page_bytes)
    seed_canonical_value(
        connector,
        value_sha256=value_sha256,
        digest_domain=domain,
        page_sha256=encoded_page.page_sha256,
        page_bytes=encoded_page.page_bytes,
        subtree_item_count=page.subtree_byte_count,
        allocated_at=1,
        level=page.level,
        page_position=page.page_position,
    )


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


def _seed_base_publication_commit(
    connector: SQLiteConnector,
    *,
    snapshot_manifest_sha256: bytes,
) -> None:
    connector.execute(
        "INSERT INTO catalog_source_revision_descriptors "
        "(source_revision, channel, snapshot_manifest_sha256) VALUES (%s, %s, %s)",
        (1, _CHANNEL, snapshot_manifest_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_revision_descriptors "
        "(revision, publication_count, artifact_count) VALUES (%s, %s, %s)",
        (1, 0, 0),
    )
    connector.execute(
        "INSERT INTO catalog_publication_generation_nodes (generation) VALUES (%s)",
        (1,),
    )
    connector.execute(
        "INSERT INTO catalog_publication_generation_successors "
        "(successor_generation, predecessor_generation) VALUES (%s, %s)",
        (1, 0),
    )
    connector.execute(
        "INSERT INTO operational_operational_policys "
        "(operational_policy_id, operational_schema_version, algorithm_version, "
        "max_batch_rows) VALUES (%s, %s, %s, %s)",
        (1, 1, 1, 128),
    )
    connector.execute(
        "INSERT INTO operational_operational_event_streams "
        "(preparation_id, created_at) VALUES (%s, %s)",
        (_BASE_PREPARATION, 19),
    )
    connector.execute(
        "INSERT INTO operational_operational_preparation_effect_seals "
        "(preparation_id, event_count, final_chain_sha256, sealed_at) "
        "VALUES (%s, %s, %s, %s)",
        (_BASE_PREPARATION, 0, b"f" * 32, 20),
    )
    seed_publication_commit(
        connector,
        receipt_id=_BASE_RECEIPT,
        candidate_id=_BASE_CANDIDATE,
        revision=1,
        source_revision=1,
        generation=1,
        preparation_id=_BASE_PREPARATION,
        operational_policy_id=1,
        artifact_policy_id=1,
        display_title_policy_id=1,
        new_galleries=0,
        changed_galleries=0,
        removed_galleries=0,
        duplicate_losers=0,
        committed_at=20,
        channel=_CHANNEL,
    )


def _seed_completed_analysis(
    connector: SQLiteConnector,
    turn: IngestTurn,
    *,
    with_base: bool,
    forged_policy_payload: bool = False,
    source_root_components: tuple[str, ...] | None = None,
) -> None:
    source_root_payload = (
        None
        if source_root_components is None
        else identity.encode_source_root(source_root_components)
    )
    source_root = (
        b"r" * 32
        if source_root_components is None
        else identity.source_root_digest(source_root_components)
    )
    snapshot = b"m" * 32
    policy_component = identity.artifact_policy_digest(
        2,
        _ARTIFACT_ADAPTER_ID,
        _ARTIFACT_POLICY_FINGERPRINT,
    )
    _canonical_identity(
        connector,
        source_root,
        domain=b"source_root_v1",
        serial=1,
        payload=source_root_payload,
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
        domain=b"artifact_policy_v3",
        serial=3,
        payload=identity.encode_artifact_policy(
            2,
            _ARTIFACT_ADAPTER_ID,
            b"q" * 32 if forged_policy_payload else _ARTIFACT_POLICY_FINGERPRINT,
        ),
    )
    scope = seed_source_scope(connector, source_root_sha256=source_root)
    seed_manifest_policy(connector)
    seed_snapshot_manifest(
        connector,
        snapshot_manifest_sha256=snapshot,
        gallery_count=0,
        file_count=0,
        byte_count=0,
    )
    semantics = seed_artifact_policy_semantics(
        connector,
        policy_fingerprint_sha256=_ARTIFACT_POLICY_FINGERPRINT,
        adapter_id=_ARTIFACT_ADAPTER_ID,
        artifact_algorithm_version=2,
    )
    assert semantics.policy_component_sha256 == policy_component
    connector.execute(
        "INSERT INTO catalog_artifact_policies "
        "(artifact_policy_id, policy_component_sha256) VALUES (%s, %s)",
        (1, policy_component),
    )
    seed_title_sort_policy(
        connector,
        unicode_data_version=unicodedata.unidata_version.encode("ascii"),
    )
    seed_display_title_policy(connector)
    seed_analysis_policy(connector)
    seed_sealed_source_build(
        connector,
        build_id=_BUILD,
        scope_key=scope.scope_key,
        manifest_sha256=b"d" * 32,
        gallery_count=0,
        file_count=0,
        byte_count=0,
        created_at=15,
        sealed_at=20,
    )
    connector.execute(
        "INSERT INTO catalog_source_build_channel (build_id, channel) VALUES (%s, %s)",
        (_BUILD, _CHANNEL),
    )
    seed_analysis_run(
        connector,
        analysis_id=_ANALYSIS,
        build_id=_BUILD,
        policy_id=1,
        input_manifest_sha256=b"x" * 32,
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
        "INSERT INTO operational_source_build_generations "
        "(build_id, generation) VALUES (%s, %s)",
        (_BUILD, turn.generation),
    )
    connector.execute(
        "INSERT INTO operational_source_working_builds "
        "(slot, build_id, assigned_at) VALUES (%s, %s, %s)",
        (1, _BUILD, 15),
    )

    if with_base:
        _seed_base_publication_commit(
            connector,
            snapshot_manifest_sha256=snapshot,
        )
        connector.execute(
            "INSERT INTO catalog_source_build_base_publication_commits "
            "(build_id, base_receipt_id) VALUES (%s, %s)",
            (_BUILD, _BASE_RECEIPT),
        )
        connector.execute(
            "UPDATE operational_revision_allocators SET next_revision = %s "
            "WHERE stream = %s",
            (2, "CATALOG"),
        )


def _seed_selected_galleries(
    connector: SQLiteConnector,
    *,
    count: int,
    locator_components_by_gallery: dict[int, tuple[str, ...]] | None = None,
) -> None:
    scopes = connector.fetch_all(
        "SELECT scope_key FROM catalog_source_scopes ORDER BY scope_key LIMIT 2"
    )
    assert len(scopes) == 1
    scope_key = scopes[0][0]
    connector.execute(
        "INSERT INTO catalog_analysis_state_ancestry "
        "(analysis_id, ancestor_depth, ancestor_analysis_id) VALUES (%s, %s, %s)",
        (_ANALYSIS, 0, _ANALYSIS),
    )
    for gallery_id in range(1, count + 1):
        locator_components = (
            None
            if locator_components_by_gallery is None
            else locator_components_by_gallery.get(gallery_id)
        )
        locator_payload = (
            None
            if locator_components is None
            else identity.encode_source_relative_locator(locator_components)
        )
        locator = (
            sha256(b"candidate-locator\0" + gallery_id.to_bytes(8, "big")).digest()
            if locator_components is None
            else identity.source_relative_locator_digest(
                "source_relative_locator_v1", locator_components
            )
        )
        gallery_key = sha256(
            b"candidate-gallery\0" + gallery_id.to_bytes(8, "big")
        ).digest()
        observation = sha256(
            b"candidate-observation\0" + gallery_id.to_bytes(8, "big")
        ).digest()
        _canonical_identity(
            connector,
            locator,
            domain=b"source_relative_locator_v1",
            serial=1_000 + gallery_id,
            payload=locator_payload,
        )
        connector.execute(
            "INSERT INTO catalog_source_locator_identity "
            "(locator_sha256, source_gallery_name) VALUES (%s, %s)",
            (locator, f"gallery-{gallery_id}".encode()),
        )
        seed_gallery_identity(
            connector,
            gallery_id=gallery_id,
            gallery_key=gallery_key,
            scope_key=scope_key,
            locator_sha256=locator,
        )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_allocations "
            "(gallery_id, observation_id, allocated_at) VALUES (%s, %s, %s)",
            (gallery_id, 1, 20),
        )
        _canonical_identity(
            connector,
            observation,
            domain=b"gallery_observation_v1",
            serial=2_000 + gallery_id,
        )
        connector.execute(
            "INSERT INTO catalog_gallery_observations "
            "(gallery_id, observation_id, observation_identity_sha256) "
            "VALUES (%s, %s, %s)",
            (gallery_id, 1, observation),
        )
        source_gallery_name = f"gallery-{gallery_id}".encode()
        connector.execute(
            "INSERT INTO catalog_gallery_upload_times (gid, upload_time) "
            "VALUES (%s, %s)",
            (10_000 + gallery_id, 1),
        )
        connector.execute(
            "INSERT INTO catalog_source_gallery_name_gids "
            "(source_gallery_name, gid) VALUES (%s, %s)",
            (source_gallery_name, 10_000 + gallery_id),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_source_name_accesses "
            "(gallery_id, source_gallery_name) VALUES (%s, %s)",
            (gallery_id, source_gallery_name),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_metadata_locals "
            "(gallery_id, observation_id, download_time, modified_time) "
            "VALUES (%s, 1, 2, 3)",
            (gallery_id,),
        )
        connector.execute(
            "INSERT INTO catalog_source_build_expected_gallery "
            "(build_id, position, gallery_id) VALUES (%s, %s, %s)",
            (_BUILD, gallery_id - 1, gallery_id),
        )
        connector.execute(
            "INSERT INTO catalog_source_build_galleries "
            "(build_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
            (_BUILD, gallery_id, 1),
        )
        gid = 10_000 + gallery_id
        connector.execute(
            "INSERT INTO catalog_analysis_impacted_galleries "
            "(analysis_id, gallery_id) VALUES (%s, %s)",
            (_ANALYSIS, gallery_id),
        )
        connector.execute(
            "INSERT INTO catalog_a_impacted_gid_provenance_storage "
            "(analysis_id, gallery_id) VALUES (%s, %s)",
            (_ANALYSIS, gallery_id),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_impacted_gid_storage "
            "(analysis_id, gid) VALUES (%s, %s)",
            (_ANALYSIS, gid),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_gid_winner_selections "
            "(analysis_id, winner_gallery_id) VALUES (%s, %s)",
            (_ANALYSIS, gallery_id),
        )
    connector.execute(
        "UPDATE catalog_source_build_discoveries "
        "SET gallery_count = %s WHERE build_id = %s",
        (count, _BUILD),
    )
    connector.execute(
        "UPDATE catalog_source_snapshot_manifest_identity SET gallery_count = %s",
        (count,),
    )
    set_analysis_component_live_count(
        connector,
        analysis_id=_ANALYSIS,
        state_component=b"gid_winner",
        row_count=count,
    )


def _seed_projection_metadata(
    connector: SQLiteConnector,
    *,
    count: int,
    with_tags: bool = False,
) -> None:
    for gallery_id in range(1, count + 1):
        metadata = identity.GalleryObservationMetadata(
            10_000 + gallery_id,
            f"Title {gallery_id}",
            f"Summary {gallery_id}",
            f"Uploader {gallery_id}",
            1,
            2,
            3,
            1,
            0,
            None,
        )
        tree = identity.build_gallery_observation_metadata_tree(metadata)
        bounds_by_page: dict[bytes, tuple[bytes, bytes]] = {}
        for encoded in tree.pages:
            page = identity.decode_gallery_observation_page(encoded.page_bytes)
            seed_gallery_page_descriptor(
                connector,
                page_sha256=encoded.page_sha256,
                page_bytes=encoded.page_bytes,
                component=b"METADATA",
                level=page.level,
                subtree_item_count=page.subtree_item_count,
            )
            child_bounds: dict[bytes, tuple[bytes, bytes]] = {}
            if page.node_kind is identity.GalleryObservationNodeKind.BRANCH:
                for position, entry in enumerate(page.entries):
                    assert isinstance(entry, identity.GalleryObservationBranchEntry)
                    connector.execute(
                        "INSERT INTO catalog_gallery_observation_page_children "
                        "(parent_sha256, position, child_sha256) "
                        "VALUES (%s, %s, %s)",
                        (encoded.page_sha256, position, entry.child_sha256),
                    )
                    child_bounds[entry.child_sha256] = bounds_by_page[
                        entry.child_sha256
                    ]
            bounds = identity.gallery_observation_page_key_bounds(
                page,
                child_bounds=child_bounds or None,
            )
            assert bounds is not None
            seed_gallery_page_bounds(
                connector,
                page_sha256=encoded.page_sha256,
                first_key=bounds[0],
                last_key=bounds[1],
            )
            bounds_by_page[encoded.page_sha256] = bounds
        connector.execute(
            "INSERT INTO catalog_gallery_observation_tree_roots "
            "(gallery_id, observation_id, root_page_sha256) VALUES (%s, %s, %s)",
            (gallery_id, 1, tree.root_page_sha256),
        )
        if not with_tags:
            continue
        for offset, (namespace, value) in enumerate(
            ((b"language", b"english"), (b"artist", f"Artist {gallery_id}".encode()))
        ):
            tag_id = gallery_id * 10 + offset + 1
            value_sha256 = identity.canonical_value_digest(
                "tag_value_utf8_v1",
                value,
            )
            _canonical_identity(
                connector,
                value_sha256,
                domain=b"tag_value_utf8_v1",
                serial=10_000 + tag_id,
                payload=value,
            )
            seed_tag_term(
                connector,
                tag_id=tag_id,
                namespace=namespace,
                tag_value_sha256=value_sha256,
            )
            connector.execute(
                "INSERT INTO catalog_gallery_observation_tags "
                "(gallery_id, observation_id, position, tag_id) "
                "VALUES (%s, %s, %s, %s)",
                (gallery_id, 1, offset, tag_id),
            )


def _complete_selection(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    *,
    now: int = 101,
) -> None:
    timestamp = now
    for method, prefix in (
        (PublicationCandidateRepository.process_selection_batch, b"build-selection-"),
        (
            PublicationCandidateRepository.validate_selection_batch,
            b"validate-selection-",
        ),
    ):
        index = 0
        while True:
            with connector.transaction():
                batch = method(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    candidate_id=_CANDIDATE,
                    batch_key=prefix + index.to_bytes(4, "big"),
                    now=timestamp,
                )
            index += 1
            timestamp += 1
            if batch.terminal:
                break


def _upload_projection_canonical_values(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    plan: Any,
    *,
    now: int,
) -> None:
    for upload in plan.iter_canonical_value_plans():
        try:
            with connector.transaction():
                CanonicalValueRepository.allocate(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=upload,
                    now=now,
                )
            for page in upload.iter_pages():
                with connector.transaction():
                    CanonicalValueRepository.put_page(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        plan=upload,
                        prepared_page=page,
                        now=now,
                    )
            with connector.transaction():
                CanonicalValueRepository.seal(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=upload,
                    now=now,
                )
        finally:
            upload.close()


def _begin(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    *,
    artifacts_required: bool = False,
    now: int = 100,
) -> Any:
    with connector.transaction():
        with patch(
            "h2hdb.vnext_publication_candidate_repository._new_candidate_id",
            return_value=_CANDIDATE,
        ):
            return PublicationCandidateRepository.begin(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=_ANALYSIS,
                artifact_policy_id=1,
                display_title_policy_id=1,
                artifacts_required=artifacts_required,
                now=now,
            )


def _take_over_ingest_turn(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
) -> tuple[GateLease, IngestTurn]:
    with connector.transaction():
        renewed_gate = MaintenanceGateRepository.renew(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate,
            now=90,
            lease_duration=2_000_000,
        )
    with connector.transaction():
        replacement = IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend="sqlite"),
            owner_token=b"n" * 16,
            now=turn.lease_expires_at,
            lease_duration=1_000_000,
        )
        connector.execute(
            "INSERT INTO operational_source_build_generations "
            "(build_id, generation) VALUES (%s, %s)",
            (_BUILD, replacement.generation),
        )
    return renewed_gate, replacement


@pytest.mark.parametrize("with_base", [False, True], ids=["genesis", "successor"])
def test_begin_derives_revision_channel_and_exact_optional_bases(
    tmp_path: Path,
    with_base: bool,
) -> None:
    connector = _generated_database(tmp_path / f"begin-{with_base}.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=with_base)

    candidate = _begin(connector, gate, turn)

    expected_revision = 2 if with_base else 1
    assert candidate.candidate_id == _CANDIDATE
    assert candidate.build_id == _BUILD
    assert candidate.channel == _CHANNEL
    assert candidate.reserved_revision == expected_revision
    assert candidate.base_source_revision == (1 if with_base else None)
    assert candidate.base_source_generation == (1 if with_base else None)
    assert candidate.base_catalog_revision == (1 if with_base else None)
    assert candidate.base_catalog_generation == (1 if with_base else None)
    assert not candidate.replayed
    assert connector.fetch_one(
        "SELECT analysis_id, reserved_revision, artifact_policy_id, "
        "display_title_policy_id, artifacts_required, created_at "
        "FROM catalog_publication_candidates WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == (_ANALYSIS, expected_revision, 1, 1, 0, 100)
    assert not connector.fetch_one(
        "SELECT candidate_id FROM catalog_publication_candidate_projection_seals "
        "WHERE candidate_id = %s",
        (_CANDIDATE,),
    )
    assert connector.fetch_one(
        "SELECT candidate_id FROM operational_catalog_working_candidates "
        "WHERE slot = %s",
        (1,),
    ) == (_CANDIDATE,)
    assert connector.fetch_one(
        "SELECT COUNT(*) FROM catalog_publication_stages",
    ) == (17,)
    assert connector.fetch_one(
        "SELECT COUNT(*) FROM catalog_publication_checkpoints WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == (16,)
    assert not connector.fetch_one(
        "SELECT 1 FROM catalog_publication_checkpoints "
        "WHERE candidate_id = %s AND stage = %s",
        (_CANDIDATE, b"FINALIZE_ARTIFACTS"),
    )
    assert not connector.fetch_one(
        "SELECT 1 FROM catalog_revisions WHERE revision = %s",
        (expected_revision,),
    )
    connector.close()


def test_begin_response_loss_replays_sole_working_root_and_compares_request(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "begin-replay.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    first = _begin(connector, gate, turn)

    with connector.transaction():
        with patch(
            "h2hdb.vnext_publication_candidate_repository._new_candidate_id"
        ) as generated:
            replay = PublicationCandidateRepository.begin(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=_ANALYSIS,
                artifact_policy_id=1,
                display_title_policy_id=1,
                artifacts_required=False,
                now=101,
            )
    generated.assert_not_called()
    assert replay.replayed and replay.candidate_id == first.candidate_id
    assert connector.fetch_one(
        "SELECT COUNT(*) FROM catalog_publication_candidates"
    ) == (1,)
    assert connector.fetch_one(
        "SELECT next_revision FROM operational_revision_allocators WHERE stream = %s",
        ("CATALOG",),
    ) == (2,)

    with pytest.raises(PublicationCandidateConflictError):
        _begin(connector, gate, turn, artifacts_required=True, now=102)
    connector.close()


def test_begin_rejects_forged_source_working_assignment(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "begin-source-assignment.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    assert (
        connector.execute_affected(
            "UPDATE operational_source_working_builds SET assigned_at = %s "
            "WHERE slot = %s",
            (14, 1),
        )
        == 1
    )

    with pytest.raises(PublicationCandidateConflictError, match="assignment|creation"):
        _begin(connector, gate, turn)

    assert connector.fetch_one(
        "SELECT build_id, assigned_at FROM operational_source_working_builds "
        "WHERE slot = %s",
        (1,),
    ) == (_BUILD, 14)
    assert not connector.fetch_one("SELECT 1 FROM catalog_publication_candidates")
    assert connector.fetch_one(
        "SELECT next_revision FROM operational_revision_allocators WHERE stream = %s",
        ("CATALOG",),
    ) == (1,)
    connector.close()


def test_begin_resume_rejects_forged_catalog_working_assignment(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "resume-catalog-assignment.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    _begin(connector, gate, turn)
    assert (
        connector.execute_affected(
            "UPDATE operational_catalog_working_candidates SET assigned_at = %s "
            "WHERE slot = %s",
            (99, 1),
        )
        == 1
    )

    with pytest.raises(PublicationCandidateConflictError, match="assignment|creation"):
        _begin(connector, gate, turn, now=101)

    assert connector.fetch_one(
        "SELECT candidate_id, assigned_at FROM "
        "operational_catalog_working_candidates WHERE slot = %s",
        (1,),
    ) == (_CANDIDATE, 99)
    assert connector.fetch_one(
        "SELECT candidate_id FROM catalog_publication_candidates "
        "WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == (_CANDIDATE,)
    assert not connector.fetch_one(
        "SELECT candidate_id FROM catalog_publication_candidate_projection_seals "
        "WHERE candidate_id = %s",
        (_CANDIDATE,),
    )
    connector.close()


def test_forged_artifact_policy_canonical_preimage_fails_without_reservation(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "forged-policy.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(
        connector,
        turn,
        with_base=False,
        forged_policy_payload=True,
    )

    with pytest.raises(
        PublicationCandidateConflictError,
        match="canonical payload",
    ):
        _begin(connector, gate, turn)

    assert not connector.fetch_one("SELECT 1 FROM catalog_publication_candidates")
    assert not connector.fetch_one(
        "SELECT 1 FROM operational_catalog_working_candidates"
    )
    assert connector.fetch_one(
        "SELECT next_revision FROM operational_revision_allocators WHERE stream = %s",
        ("CATALOG",),
    ) == (1,)
    connector.close()


def test_begin_rejects_permanent_candidate_identity_after_transient_cleanup(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "permanent-candidate-collision.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=True)
    connector.execute(
        "INSERT INTO catalog_publication_commit_finalizations (receipt_id) VALUES (%s)",
        (_BASE_RECEIPT,),
    )
    connector.execute(
        "DELETE FROM catalog_publication_commit_head_receipts WHERE channel = %s",
        (_CHANNEL,),
    )
    before = connector.connection.total_changes

    with (
        patch(
            "h2hdb.vnext_publication_candidate_repository._new_candidate_id",
            return_value=_BASE_CANDIDATE,
        ),
        pytest.raises(PublicationCandidateConflictError, match="already exists"),
    ):
        with connector.transaction():
            PublicationCandidateRepository.begin(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=_ANALYSIS,
                artifact_policy_id=1,
                display_title_policy_id=1,
                artifacts_required=False,
                now=100,
            )

    assert connector.connection.total_changes == before
    assert not connector.fetch_one("SELECT 1 FROM catalog_publication_candidates")
    assert connector.fetch_one(
        "SELECT next_revision FROM operational_revision_allocators WHERE stream = %s",
        ("CATALOG",),
    ) == (2,)
    connector.close()


def test_begin_rejects_drifted_closed_stage_registry_before_reservation(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "stage-registry-drift.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    connector.execute(
        "UPDATE catalog_publication_stages SET cursor_codec = %s WHERE stage = %s",
        (b"caller_bytes_v1", b"BUILD_SELECTION"),
    )

    with pytest.raises(PublicationCandidateConflictError, match="closed runtime map"):
        _begin(connector, gate, turn)

    assert not connector.fetch_one("SELECT 1 FROM catalog_publication_candidates")
    assert connector.fetch_one(
        "SELECT next_revision FROM operational_revision_allocators WHERE stream = %s",
        ("CATALOG",),
    ) == (1,)
    connector.close()


@pytest.mark.parametrize("authority", ["mapping", "working"])
def test_begin_requires_exact_live_generation_and_source_working_authority(
    tmp_path: Path,
    authority: str,
) -> None:
    connector = _generated_database(tmp_path / f"missing-{authority}.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    if authority == "mapping":
        connector.execute("DELETE FROM operational_source_build_generations")
    else:
        connector.execute("DELETE FROM operational_source_working_builds")

    with pytest.raises(PublicationCandidateNotReadyError):
        _begin(connector, gate, turn)

    assert not connector.fetch_one("SELECT 1 FROM catalog_publication_candidates")
    assert connector.fetch_one(
        "SELECT next_revision FROM operational_revision_allocators WHERE stream = %s",
        ("CATALOG",),
    ) == (1,)
    connector.close()


@pytest.mark.parametrize("phase", ["fresh", "resume"])
def test_begin_or_resume_base_race_fails_without_allocator_leak(
    tmp_path: Path,
    phase: str,
) -> None:
    connector = _generated_database(tmp_path / f"base-race-{phase}.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=True)
    if phase == "resume":
        _begin(connector, gate, turn)
    connector.execute(
        "DELETE FROM catalog_publication_commit_head_receipts WHERE channel = %s",
        (_CHANNEL,),
    )

    with pytest.raises(PublicationCandidateHeadRaceError):
        _begin(connector, gate, turn, now=101)

    if phase == "fresh":
        assert not connector.fetch_one("SELECT 1 FROM catalog_publication_candidates")
        assert connector.fetch_one(
            "SELECT next_revision FROM operational_revision_allocators "
            "WHERE stream = %s",
            ("CATALOG",),
        ) == (2,)
    else:
        assert connector.fetch_one(
            "SELECT candidate_id FROM catalog_publication_candidates"
        ) == (_CANDIDATE,)
        assert not connector.fetch_one(
            "SELECT candidate_id FROM catalog_publication_candidate_projection_seals"
        )
        assert not connector.fetch_one(
            "SELECT receipt_id FROM catalog_publication_commits "
            "WHERE candidate_id = %s",
            (_CANDIDATE,),
        )
    connector.close()


def test_each_begin_mutation_fault_rolls_back_allocator_candidate_and_bases(
    tmp_path: Path,
) -> None:
    base_path = tmp_path / "begin-fault-base.sqlite3"
    base = _generated_database(base_path)
    gate, turn = _authorities(base)
    _seed_completed_analysis(base, turn, with_base=True)
    base.close()

    # Allocator, candidate, common base, atomic checkpoint set, working root.
    for failure_at in range(1, 6):
        path = tmp_path / f"begin-fault-{failure_at}.sqlite3"
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
                raise RuntimeError(f"injected begin mutation {failure_at}")

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
            pytest.raises(RuntimeError, match=f"begin mutation {failure_at}"),
        ):
            _begin(connector, gate, turn)

        assert mutation_number == failure_at
        assert not connector.fetch_one("SELECT 1 FROM catalog_publication_candidates")
        assert not connector.fetch_one(
            "SELECT 1 FROM operational_catalog_working_candidates"
        )
        assert not connector.fetch_one("SELECT 1 FROM catalog_publication_checkpoints")
        assert connector.fetch_one(
            "SELECT next_revision FROM operational_revision_allocators "
            "WHERE stream = %s",
            ("CATALOG",),
        ) == (2,)
        connector.close()


def test_empty_selection_and_independent_validation_use_terminal_empty_receipts(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "selection-empty.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    _begin(connector, gate, turn)

    with connector.transaction():
        built = PublicationCandidateRepository.process_selection_batch(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            batch_key=b"build-empty",
            now=101,
        )
    assert built.terminal
    assert built.next_state == "COMPLETE"
    assert built.row_count == built.next_processed_count == 0
    assert built.start_cursor == built.next_cursor == b""

    with connector.transaction():
        replay = PublicationCandidateRepository.process_selection_batch(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            batch_key=b"build-empty",
            now=102,
        )
    assert replay.replayed and replay == built.__class__(
        built.candidate_id,
        built.stage,
        built.batch_key,
        built.start_generation,
        built.start_cursor,
        built.start_processed_count,
        built.next_cursor,
        built.next_processed_count,
        built.next_state,
        built.row_count,
        built.terminal,
        built.committed_generation,
        built.committed_at,
        True,
    )

    with connector.transaction():
        validated = PublicationCandidateRepository.validate_selection_batch(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            batch_key=b"validate-empty",
            now=103,
        )
    assert validated.terminal and validated.next_state == "COMPLETE"
    assert connector.fetch_one(
        "SELECT COUNT(*) FROM catalog_publication_batch_receipts "
        "WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == (2,)
    connector.close()


@pytest.mark.parametrize("capability", ["source", "catalog"])
def test_publication_batch_rejects_forged_working_assignment(
    tmp_path: Path,
    capability: str,
) -> None:
    connector = _generated_database(tmp_path / f"batch-{capability}-assignment.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    _begin(connector, gate, turn)
    if capability == "source":
        table = "operational_source_working_builds"
        expected = (_BUILD, 14)
        query = (
            "SELECT build_id, assigned_at FROM operational_source_working_builds "
            "WHERE slot = %s"
        )
        assigned_at = 14
    else:
        table = "operational_catalog_working_candidates"
        expected = (_CANDIDATE, 99)
        query = (
            "SELECT candidate_id, assigned_at FROM "
            "operational_catalog_working_candidates WHERE slot = %s"
        )
        assigned_at = 99
    assert (
        connector.execute_affected(
            f"UPDATE {table} SET assigned_at = %s WHERE slot = %s",
            (assigned_at, 1),
        )
        == 1
    )

    with (
        connector.transaction(),
        pytest.raises(PublicationCandidateConflictError, match="assignment|creation"),
    ):
        PublicationCandidateRepository.process_selection_batch(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            batch_key=b"forged-working",
            now=101,
        )

    assert connector.fetch_one(query, (1,)) == expected
    assert not connector.fetch_one(
        "SELECT 1 FROM catalog_publication_batch_receipts WHERE candidate_id = %s",
        (_CANDIDATE,),
    )
    connector.close()


@pytest.mark.parametrize("capability", ["source", "catalog"])
def test_projection_receipt_revalidation_requires_exact_working_assignment(
    tmp_path: Path,
    capability: str,
) -> None:
    connector = _generated_database(
        tmp_path / f"projection-revalidate-{capability}-assignment.sqlite3"
    )
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    _begin(connector, gate, turn)
    _complete_selection(connector, gate, turn)
    with connector.transaction():
        authority = PublicationCandidateRepository.issue_projection_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            now=110,
        )
    table = (
        "operational_source_working_builds"
        if capability == "source"
        else "operational_catalog_working_candidates"
    )
    assert (
        connector.execute_affected(
            f"UPDATE {table} SET assigned_at = assigned_at - 1 WHERE slot = %s",
            (1,),
        )
        == 1
    )

    with pytest.raises(PublicationCandidateNotReadyError, match="working-root"):
        PublicationCandidateRepository.prepare_catalog_projection(
            connector,
            backend="sqlite",
            authority=authority,
        )
    connector.close()


@pytest.mark.deep
def test_selection_and_validation_cross_the_fixed_128_row_keyset_boundary(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "selection-large.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    _seed_selected_galleries(connector, count=129)
    _begin(connector, gate, turn)

    built = []
    for index in range(3):
        with connector.transaction():
            built.append(
                PublicationCandidateRepository.process_selection_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    candidate_id=_CANDIDATE,
                    batch_key=b"build" + bytes((index,)),
                    now=101 + index,
                )
            )
    assert [batch.row_count for batch in built] == [128, 1, 0]
    assert [batch.terminal for batch in built] == [False, False, True]
    assert built[0].next_cursor == (128).to_bytes(8, "big")
    assert built[1].next_cursor == built[2].next_cursor == (129).to_bytes(8, "big")

    validated = []
    for index in range(3):
        with connector.transaction():
            validated.append(
                PublicationCandidateRepository.validate_selection_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    candidate_id=_CANDIDATE,
                    batch_key=b"validate" + bytes((index,)),
                    now=110 + index,
                )
            )
    assert [batch.row_count for batch in validated] == [128, 1, 0]
    assert validated[-1].next_state == "COMPLETE"
    assert validated[-1].next_processed_count == 129
    assert connector.fetch_one(
        "SELECT COUNT(*) FROM catalog_publication_selections WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == (129,)
    assert connector.fetch_one(
        "SELECT COUNT(*) FROM catalog_publication_identities"
    ) == (129,)
    connector.close()


def test_catalog_projection_uses_typed_disk_plan_and_independent_validation(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "catalog-projection.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    _seed_selected_galleries(connector, count=1)
    _seed_projection_metadata(connector, count=1, with_tags=True)
    _begin(connector, gate, turn, artifacts_required=True)
    _complete_selection(connector, gate, turn)

    with connector.transaction():
        authority = PublicationCandidateRepository.issue_projection_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            now=110,
        )
    assert authority.publication_count == 1

    with (
        PublicationCandidateRepository.prepare_catalog_projection(
            connector,
            backend="sqlite",
            authority=authority,
        ) as plan,
        PublicationCandidateRepository.prepare_catalog_projection_validation(
            connector,
            backend="sqlite",
            authority=authority,
        ) as validation,
    ):
        assert plan.publication_count == validation.publication_count == 1
        assert plan.child_count == validation.child_count == 18
        _upload_projection_canonical_values(
            connector,
            gate,
            turn,
            plan,
            now=111,
        )

        with connector.transaction():
            built = PublicationCandidateRepository.process_catalog_projection_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                plan=plan,
                batch_key=b"catalog-build-1",
                now=112,
            )
        assert built.row_count == 18 and not built.terminal
        with connector.transaction():
            replay = PublicationCandidateRepository.process_catalog_projection_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                plan=plan,
                batch_key=b"catalog-build-1",
                now=113,
            )
        assert replay.replayed and replay.committed_at == built.committed_at
        with connector.transaction():
            terminal = PublicationCandidateRepository.process_catalog_projection_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                plan=plan,
                batch_key=b"catalog-build-2",
                now=114,
            )
        assert terminal.terminal and terminal.next_processed_count == 18

        with connector.transaction():
            checked = PublicationCandidateRepository.validate_catalog_projection_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                validation=validation,
                batch_key=b"catalog-validate-1",
                now=115,
            )
        assert checked.row_count == 18 and not checked.terminal
        with connector.transaction():
            checked_terminal = (
                PublicationCandidateRepository.validate_catalog_projection_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    candidate_id=_CANDIDATE,
                    validation=validation,
                    batch_key=b"catalog-validate-2",
                    now=116,
                )
            )
        assert checked_terminal.terminal
        assert checked_terminal.next_processed_count == 18

    publication_key = identity.publication_key(10_001)
    assert connector.fetch_one(
        "SELECT publication_count FROM catalog_revision_descriptors WHERE revision = 1"
    ) == (1,)
    assert connector.fetch_one(
        "SELECT gallery_id, modified_at "
        "FROM catalog_publications WHERE revision = 1 AND publication_key = %s",
        (publication_key,),
    ) == (1, 3)
    assert connector.fetch_one(
        "SELECT download_time FROM catalog_publications "
        "WHERE revision = 1 AND publication_key = %s",
        (publication_key,),
    ) == (2,)
    assert connector.fetch_one(
        "SELECT upload.upload_time FROM catalog_publication_identities AS identity "
        "JOIN catalog_gallery_upload_times AS upload ON upload.gid = identity.gid "
        "WHERE identity.publication_key = %s",
        (publication_key,),
    ) == (1,)
    assert connector.fetch_one(
        "SELECT position, publication_key FROM catalog_publication_order "
        "WHERE revision = 1"
    ) == (0, publication_key)
    assert connector.fetch_one(
        "SELECT COUNT(*) FROM catalog_contributors WHERE revision = 1"
    ) == (2,)
    assert connector.fetch_one(
        "SELECT COUNT(*) FROM catalog_subjects WHERE revision = 1"
    ) == (2,)
    expected_search_tokens = {b"1", b"artist", b"english", b"title", b"uploader"}
    expected_search_digests = {
        identity.canonical_value_digest("search_lexeme_utf8_v1", token)
        for token in expected_search_tokens
    }
    assert connector.fetch_one(
        "SELECT row_count FROM catalog_search_documents "
        "WHERE revision = 1 AND publication_key = %s",
        (publication_key,),
    ) == (len(expected_search_digests),)
    assert {
        row[0]
        for row in connector.fetch_all(
            "SELECT value_sha256 FROM catalog_search_postings "
            "WHERE revision = 1 AND publication_key = %s",
            (publication_key,),
        )
    } == expected_search_digests
    language_sha256 = identity.canonical_value_digest(
        "catalog_language_utf8_v1",
        b"english",
    )
    assert connector.fetch_all(
        "SELECT position, language_sha256, occurrence_count "
        "FROM catalog_language_facet_order WHERE revision = 1"
    ) == [(0, language_sha256, 1)]
    assert (
        connector.fetch_all(
            "SELECT position, tag_id, occurrence_count "
            "FROM catalog_subject_facet_order WHERE revision = 1"
        )
        == []
    )
    assert connector.fetch_all(
        "SELECT role, occurrence_count FROM catalog_contributor_facet_order "
        "WHERE revision = 1 ORDER BY position"
    ) == [(b"artist", 1), (b"uploader", 1)]
    assert connector.fetch_one(
        "SELECT policy_id FROM catalog_discovery_seals WHERE revision = 1"
    ) == (1,)
    catalog_refinement._validate_active_discovery_projection(
        connector,
        revision=1,
        display_title_policy_id=1,
        expected_publication_count=1,
    )
    assert not connector.fetch_one(
        "SELECT 1 FROM operational_canonical_value_uploads WHERE generation = %s",
        (turn.generation,),
    )
    connector.close()


@pytest.mark.deep
def test_catalog_projection_high_cardinality_mutations_are_fixed_128_children(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "catalog-projection-large.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    _seed_selected_galleries(connector, count=50)
    _seed_projection_metadata(connector, count=50)
    _begin(connector, gate, turn, artifacts_required=True)
    _complete_selection(connector, gate, turn)
    with connector.transaction():
        authority = PublicationCandidateRepository.issue_projection_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            now=110,
        )

    with (
        PublicationCandidateRepository.prepare_catalog_projection(
            connector,
            backend="sqlite",
            authority=authority,
        ) as plan,
        PublicationCandidateRepository.prepare_catalog_projection_validation(
            connector,
            backend="sqlite",
            authority=authority,
        ) as validation,
    ):
        assert plan.child_count == validation.child_count == 502
        _upload_projection_canonical_values(
            connector,
            gate,
            turn,
            plan,
            now=111,
        )
        original_fetch_all = connector.fetch_all
        mutation_queries: list[str] = []

        def recording_fetch_all(
            query: str,
            data: tuple[Any, ...] = (),
        ) -> list[tuple[Any, ...]]:
            mutation_queries.append(query)
            return original_fetch_all(query, data)

        built = []
        with patch.object(connector, "fetch_all", side_effect=recording_fetch_all):
            for index in range(5):
                with connector.transaction():
                    built.append(
                        PublicationCandidateRepository.process_catalog_projection_batch(
                            VNextUnitOfWork(connector, backend="sqlite"),
                            gate_lease=gate,
                            ingest_turn=turn,
                            candidate_id=_CANDIDATE,
                            plan=plan,
                            batch_key=b"large-build-" + bytes((index,)),
                            now=112 + index,
                        )
                    )
        assert [batch.row_count for batch in built] == [128, 128, 128, 118, 0]
        assert built[-1].terminal and built[-1].next_processed_count == 502
        normalized = " ".join(mutation_queries).upper()
        assert "COUNT(" not in normalized and "SUM(" not in normalized
        assert "CATALOG_GALLERY_OBSERVATION_TAGS" not in normalized
        assert "CATALOG_GALLERY_OBSERVATION_PAGES" not in normalized

        checked = []
        for index in range(5):
            with connector.transaction():
                checked.append(
                    PublicationCandidateRepository.validate_catalog_projection_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        candidate_id=_CANDIDATE,
                        validation=validation,
                        batch_key=b"large-validate-" + bytes((index,)),
                        now=120 + index,
                    )
                )
        assert [batch.row_count for batch in checked] == [128, 128, 128, 118, 0]
        assert checked[-1].terminal and checked[-1].next_processed_count == 502
    assert connector.fetch_one(
        "SELECT publication_count FROM catalog_revision_descriptors WHERE revision = 1"
    ) == (50,)
    connector.close()


def test_catalog_projection_major_statement_faults_roll_back_all_children(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "catalog-projection-fault.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    _seed_selected_galleries(connector, count=1)
    _seed_projection_metadata(connector, count=1)
    _begin(connector, gate, turn)
    _complete_selection(connector, gate, turn)
    with connector.transaction():
        authority = PublicationCandidateRepository.issue_projection_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            now=110,
        )
    with PublicationCandidateRepository.prepare_catalog_projection(
        connector,
        backend="sqlite",
        authority=authority,
    ) as plan:
        _upload_projection_canonical_values(
            connector,
            gate,
            turn,
            plan,
            now=111,
        )
        original_execute = connector.execute
        original_execute_affected = connector.execute_affected
        failures = (
            "INSERT INTO catalog_publication_occurrence_identities",
            "INSERT INTO catalog_publication_download_times",
            "INSERT INTO catalog_publication_batch_receipt_stored",
            "UPDATE catalog_publication_checkpoints",
        )
        for index, target in enumerate(failures):

            def failing_execute(
                query: str,
                data: tuple[Any, ...] = (),
            ) -> None:
                if target in query:
                    raise RuntimeError(f"injected {target}")
                original_execute(query, data)

            def failing_execute_affected(
                query: str,
                data: tuple[Any, ...] = (),
            ) -> int:
                if target in query:
                    raise RuntimeError(f"injected {target}")
                return original_execute_affected(query, data)

            with (
                patch.object(connector, "execute", side_effect=failing_execute),
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=failing_execute_affected,
                ),
                pytest.raises(RuntimeError, match="injected"),
            ):
                with connector.transaction():
                    PublicationCandidateRepository.process_catalog_projection_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        candidate_id=_CANDIDATE,
                        plan=plan,
                        batch_key=b"fault-" + bytes((index,)),
                        now=112 + index,
                    )
            assert not connector.fetch_one("SELECT 1 FROM catalog_revision_descriptors")
            assert not connector.fetch_one("SELECT 1 FROM catalog_publications")
            assert not connector.fetch_one("SELECT 1 FROM catalog_publication_order")
            assert not connector.fetch_one("SELECT 1 FROM catalog_publication_titles")
            assert not connector.fetch_one("SELECT 1 FROM catalog_contributors")
            assert not connector.fetch_one(
                "SELECT 1 FROM catalog_publication_batch_receipts WHERE stage = %s",
                (b"BUILD_CATALOG_PROJECTION",),
            )
            assert connector.fetch_one(
                "SELECT generation, cursor, processed_count, state "
                "FROM catalog_publication_checkpoints "
                "WHERE candidate_id = %s AND stage = %s",
                (_CANDIDATE, b"BUILD_CATALOG_PROJECTION"),
            ) == (1, b"", 0, "OPEN")
            assert connector.fetch_one(
                "SELECT 1 FROM operational_canonical_value_uploads "
                "WHERE generation = %s LIMIT 1",
                (turn.generation,),
            ) == (1,)
        missing_claim = connector.fetch_one(
            "SELECT value_sha256 FROM operational_canonical_value_uploads "
            "WHERE generation = %s ORDER BY value_sha256 LIMIT 1",
            (turn.generation,),
        )
        assert len(missing_claim) == 1
        connector.execute(
            "DELETE FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (turn.generation, missing_claim[0]),
        )
        with pytest.raises(PublicationCandidateNotReadyError, match="upload claim"):
            with connector.transaction():
                PublicationCandidateRepository.process_catalog_projection_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    candidate_id=_CANDIDATE,
                    plan=plan,
                    batch_key=b"missing-claim",
                    now=120,
                )
        assert not connector.fetch_one("SELECT 1 FROM catalog_revision_descriptors")
    connector.close()


def test_independent_catalog_validation_rejects_missing_child_without_progress(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "catalog-projection-corrupt.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    _seed_selected_galleries(connector, count=1)
    _seed_projection_metadata(connector, count=1)
    _begin(connector, gate, turn)
    _complete_selection(connector, gate, turn)
    with connector.transaction():
        authority = PublicationCandidateRepository.issue_projection_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            now=110,
        )
    with (
        PublicationCandidateRepository.prepare_catalog_projection(
            connector,
            backend="sqlite",
            authority=authority,
        ) as plan,
        PublicationCandidateRepository.prepare_catalog_projection_validation(
            connector,
            backend="sqlite",
            authority=authority,
        ) as validation,
    ):
        _upload_projection_canonical_values(
            connector,
            gate,
            turn,
            plan,
            now=111,
        )
        for index in range(2):
            with connector.transaction():
                PublicationCandidateRepository.process_catalog_projection_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    candidate_id=_CANDIDATE,
                    plan=plan,
                    batch_key=b"build-corrupt-" + bytes((index,)),
                    now=112 + index,
                )
        connector.execute(
            "DELETE FROM catalog_publication_storage WHERE "
            "catalog_occurrence_sha256 IN ("
            "SELECT catalog_occurrence_sha256 "
            "FROM catalog_publication_occurrence_identities "
            "WHERE revision = %s)",
            (1,),
        )
        with pytest.raises(PublicationCandidateConflictError, match="independent"):
            with connector.transaction():
                PublicationCandidateRepository.validate_catalog_projection_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    candidate_id=_CANDIDATE,
                    validation=validation,
                    batch_key=b"validate-corrupt",
                    now=114,
                )
    assert connector.fetch_one(
        "SELECT generation, cursor, processed_count, state "
        "FROM catalog_publication_checkpoints "
        "WHERE candidate_id = %s AND stage = %s",
        (_CANDIDATE, b"VALIDATE_CATALOG_PROJECTION"),
    ) == (1, b"", 0, "OPEN")
    assert not connector.fetch_one(
        "SELECT 1 FROM catalog_publication_batch_receipts WHERE stage = %s",
        (b"VALIDATE_CATALOG_PROJECTION",),
    )
    connector.close()


def test_selection_response_loss_replays_under_a_new_live_ingest_turn(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "selection-new-turn-replay.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    _seed_selected_galleries(connector, count=1)
    _begin(connector, gate, turn)
    with connector.transaction():
        first = PublicationCandidateRepository.process_selection_batch(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            batch_key=b"response-loss",
            now=101,
        )

    replacement_gate, replacement_turn = _take_over_ingest_turn(
        connector,
        gate,
        turn,
    )
    with connector.transaction():
        replay = PublicationCandidateRepository.process_selection_batch(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=replacement_gate,
            ingest_turn=replacement_turn,
            candidate_id=_CANDIDATE,
            batch_key=b"response-loss",
            now=turn.lease_expires_at + 1,
        )

    assert replay.replayed
    assert replay.committed_at == first.committed_at == 101
    assert replay.next_cursor == first.next_cursor == (1).to_bytes(8, "big")
    assert connector.fetch_one(
        "SELECT COUNT(*) FROM catalog_publication_batch_receipts "
        "WHERE candidate_id = %s AND stage = %s",
        (_CANDIDATE, b"BUILD_SELECTION"),
    ) == (1,)
    connector.close()


def test_selection_receipts_retain_only_the_current_batch(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "selection-current-receipt.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    _seed_selected_galleries(connector, count=1)
    _begin(connector, gate, turn)

    with connector.transaction():
        first = PublicationCandidateRepository.process_selection_batch(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            batch_key=b"current-first",
            now=101,
        )
    assert not first.terminal and first.start_generation == 1

    with connector.transaction():
        terminal = PublicationCandidateRepository.process_selection_batch(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            batch_key=b"current-terminal",
            now=102,
        )
    assert terminal.terminal and terminal.start_generation == 2
    assert connector.fetch_all(
        "SELECT start_generation, batch_key "
        "FROM catalog_publication_batch_receipt_stored "
        "WHERE candidate_id = %s AND stage = %s",
        (_CANDIDATE, b"BUILD_SELECTION"),
    ) == [(2, b"current-terminal")]

    with connector.transaction():
        replayed = PublicationCandidateRepository.process_selection_batch(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            batch_key=b"current-terminal",
            now=103,
        )
    assert replayed.replayed
    assert replayed.committed_at == terminal.committed_at == 102

    with pytest.raises(PublicationCandidateNotReadyError, match="already COMPLETE"):
        with connector.transaction():
            PublicationCandidateRepository.process_selection_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                batch_key=b"current-first",
                now=104,
            )
    connector.close()


def test_selection_missing_predecessor_rolls_back_successor_and_checkpoint(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "selection-missing-receipt.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    _seed_selected_galleries(connector, count=1)
    _begin(connector, gate, turn)

    with connector.transaction():
        PublicationCandidateRepository.process_selection_batch(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            batch_key=b"predecessor",
            now=101,
        )
    connector.execute(
        "DELETE FROM catalog_publication_batch_receipt_stored "
        "WHERE candidate_id = %s AND stage = %s AND start_generation = %s",
        (_CANDIDATE, b"BUILD_SELECTION", 1),
    )

    with pytest.raises(PublicationCandidateCorruptionError, match="predecessor"):
        with connector.transaction():
            PublicationCandidateRepository.process_selection_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                batch_key=b"successor",
                now=102,
            )

    assert connector.fetch_one(
        "SELECT generation, cursor, processed_count, state, updated_at "
        "FROM catalog_publication_checkpoints "
        "WHERE candidate_id = %s AND stage = %s",
        (_CANDIDATE, b"BUILD_SELECTION"),
    ) == (2, (1).to_bytes(8, "big"), 1, "OPEN", 101)
    assert connector.fetch_one(
        "SELECT COUNT(*) FROM catalog_publication_batch_receipt_stored "
        "WHERE candidate_id = %s AND stage = %s",
        (_CANDIDATE, b"BUILD_SELECTION"),
    ) == (0,)
    connector.close()


def test_selection_reauthorizes_candidate_base_heads_before_mutation(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "selection-head-race.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=True)
    _begin(connector, gate, turn)
    connector.execute(
        "DELETE FROM catalog_publication_commit_head_receipts WHERE channel = %s",
        (_CHANNEL,),
    )

    with pytest.raises(PublicationCandidateHeadRaceError):
        with connector.transaction():
            PublicationCandidateRepository.process_selection_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                batch_key=b"head-race",
                now=101,
            )

    assert not connector.fetch_one("SELECT 1 FROM catalog_publication_batch_receipts")
    assert connector.fetch_one(
        "SELECT generation, cursor, processed_count, state "
        "FROM catalog_publication_checkpoints "
        "WHERE candidate_id = %s AND stage = %s",
        (_CANDIDATE, b"BUILD_SELECTION"),
    ) == (1, b"", 0, "OPEN")
    connector.close()


def test_independent_selection_validation_rejects_missing_materialized_child(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "selection-corruption.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    _seed_selected_galleries(connector, count=2)
    _begin(connector, gate, turn)
    for index in range(2):
        with connector.transaction():
            PublicationCandidateRepository.process_selection_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                batch_key=b"build" + bytes((index,)),
                now=101 + index,
            )
    connector.execute(
        "DELETE FROM catalog_publication_selection_storage "
        "WHERE gallery_id = %s AND selection_occurrence_sha256 IN ("
        "SELECT selection_occurrence_sha256 "
        "FROM catalog_publication_selection_occurrence_identities "
        "WHERE candidate_id = %s)",
        (2, _CANDIDATE),
    )

    with pytest.raises(PublicationCandidateConflictError, match="independent"):
        with connector.transaction():
            PublicationCandidateRepository.validate_selection_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                batch_key=b"validate-corrupt",
                now=103,
            )

    assert not connector.fetch_one(
        "SELECT 1 FROM catalog_publication_batch_receipts WHERE stage = %s",
        (b"VALIDATE_SELECTION",),
    )
    assert connector.fetch_one(
        "SELECT generation, cursor, processed_count, state "
        "FROM catalog_publication_checkpoints "
        "WHERE candidate_id = %s AND stage = %s",
        (_CANDIDATE, b"VALIDATE_SELECTION"),
    ) == (1, b"", 0, "OPEN")
    connector.close()


def test_each_selection_batch_mutation_fault_rolls_back_children_receipt_and_cas(
    tmp_path: Path,
) -> None:
    base_path = tmp_path / "selection-fault-base.sqlite3"
    base = _generated_database(base_path)
    gate, turn = _authorities(base)
    _seed_completed_analysis(base, turn, with_base=False)
    _seed_selected_galleries(base, count=1)
    _begin(base, gate, turn)
    base.close()

    # Identity, two selection projections, atomic receipt, atomic checkpoint CAS.
    for failure_at in range(1, 6):
        path = tmp_path / f"selection-fault-{failure_at}.sqlite3"
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
                raise RuntimeError(f"injected selection mutation {failure_at}")

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
            pytest.raises(RuntimeError, match=f"selection mutation {failure_at}"),
        ):
            with connector.transaction():
                PublicationCandidateRepository.process_selection_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    candidate_id=_CANDIDATE,
                    batch_key=b"fault",
                    now=101,
                )

        assert mutation_number == failure_at
        assert not connector.fetch_one("SELECT 1 FROM catalog_publication_identities")
        assert not connector.fetch_one("SELECT 1 FROM catalog_publication_selections")
        assert not connector.fetch_one(
            "SELECT 1 FROM catalog_publication_batch_receipts"
        )
        assert connector.fetch_one(
            "SELECT generation, cursor, processed_count, state "
            "FROM catalog_publication_checkpoints "
            "WHERE candidate_id = %s AND stage = %s",
            (_CANDIDATE, b"BUILD_SELECTION"),
        ) == (1, b"", 0, "OPEN")
        connector.close()


def test_mariadb_begin_lock_sql_uses_server_placeholders_and_for_update() -> None:
    import h2hdb.vnext_publication_candidate_repository as module

    class RecordingConnector:
        def __init__(self) -> None:
            self.query = ""

        def fetch_one(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            self.query = query
            if "FROM catalog_channel_registry" in query:
                return (data[0], None, None, None, None, None, None, None, None)
            return ()

    connector: Any = RecordingConnector()
    assert (
        module._lock_catalog_working(VNextUnitOfWork(connector, backend="mariadb"))
        is None
    )
    assert connector.query.endswith(" FOR UPDATE")
    assert "%s" in connector.query and "?" not in connector.query

    assert module._lock_common_head(
        VNextUnitOfWork(connector, backend="mariadb"),
        _CHANNEL,
    ) == (None, None)
    assert "catalog_publication_commit_head_receipts" in connector.query
    assert "catalog_publication_commits" in connector.query
    assert connector.query.endswith(" FOR UPDATE")


def test_mariadb_selection_and_checkpoint_sql_keep_closed_server_shape() -> None:
    from types import SimpleNamespace

    import h2hdb.vnext_publication_candidate_repository as module

    class RecordingConnector:
        def __init__(self) -> None:
            self.query = ""
            self.data: tuple[Any, ...] = ()

        def fetch_all(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> list[tuple[Any, ...]]:
            self.query = query
            self.data = data
            return []

        def execute_affected(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> int:
            self.query = query
            self.data = data
            return 16

    connector: Any = RecordingConnector()
    work = VNextUnitOfWork(connector, backend="mariadb")
    authority: Any = SimpleNamespace(
        begin=SimpleNamespace(analysis_id=_ANALYSIS, build_id=_BUILD)
    )
    assert module._derive_selection_page(work, authority, after=0) == ()
    assert connector.query.count("%s") == 5
    assert "?" not in connector.query
    assert "ORDER BY member.gallery_id LIMIT 128" in connector.query
    assert "COUNT(" not in connector.query.upper()

    projection_authority: Any = SimpleNamespace(
        candidate_id=_CANDIDATE,
        build_id=_BUILD,
        analysis_id=_ANALYSIS,
    )
    assert (
        module._projection_source_page(
            work,
            projection_authority,
            after=0,
            validation=False,
        )
        == ()
    )
    assert connector.query.count("%s") == 3 and "?" not in connector.query
    assert "ORDER BY selected.gallery_id LIMIT 128" in connector.query
    assert "COUNT(" not in connector.query.upper()

    assert (
        module._catalog_child_kind_rows(
            work,
            revision=1,
            kind=module._CATALOG_CHILD_CONTRIBUTOR,
            after_key=b"k" * 32,
            after_subkey=(7).to_bytes(8, "big"),
            limit=128,
        )
        == ()
    )
    assert connector.query.count("%s") == 5 and "?" not in connector.query
    assert "ORDER BY publication_key, position LIMIT %s" in connector.query
    assert (
        "COUNT(" not in connector.query.upper()
        and "SUM(" not in connector.query.upper()
    )

    assert (
        module._catalog_child_kind_rows(
            work,
            revision=1,
            kind=module._CATALOG_CHILD_ARTIFACT,
            after_key=b"k" * 32,
            after_subkey=b"",
            limit=128,
        )
        == ()
    )
    assert connector.query.count("%s") == 3 and "?" not in connector.query
    assert "FROM catalog_artifacts AS artifact" in connector.query
    assert "ORDER BY artifact.publication_key LIMIT %s" in connector.query
    assert "artifact_id" not in connector.query

    module._initialize_candidate_checkpoints(work, _CANDIDATE, now=100)
    assert "SELECT %s, stage, %s, %s, %s, %s, %s" in connector.query
    assert (
        "FROM catalog_publication_stages WHERE stage <> %s ORDER BY stage"
        in connector.query
    )
    assert connector.data == (
        _CANDIDATE,
        1,
        b"",
        0,
        "OPEN",
        100,
        b"FINALIZE_ARTIFACTS",
    )
    assert connector.query.count("%s") == 7 and "?" not in connector.query


@pytest.mark.parametrize(
    ("codec", "cursor"),
    [
        (b"publication_gallery_v1", (1).to_bytes(8, "big")),
        (b"publication_key_v1", b"k" * 32),
        (
            b"publication_catalog_child_v1",
            b"\x01\x00" + b"k" * 32 + b"\x00\x00",
        ),
        (
            b"publication_catalog_child_v1",
            b"\x01\x04" + b"k" * 32 + b"\x00\x08" + (0).to_bytes(8, "big"),
        ),
        (
            b"publication_catalog_child_v1",
            b"\x01\x06" + b"k" * 32 + b"\x00\x00",
        ),
    ],
)
def test_closed_publication_cursor_codecs_accept_only_exact_frames(
    codec: bytes,
    cursor: bytes,
) -> None:
    import h2hdb.vnext_publication_candidate_repository as module

    module._validate_stage_cursor(codec, b"")
    module._validate_stage_cursor(codec, cursor)


@pytest.mark.parametrize(
    ("codec", "cursor"),
    [
        (b"publication_gallery_v1", b"\0" * 8),
        (b"publication_gallery_v1", b"\1" * 7),
        (b"publication_key_v1", b"k" * 31),
        (
            b"publication_catalog_child_v1",
            b"\x02\x00" + b"k" * 32 + b"\x00\x00",
        ),
        (
            b"publication_catalog_child_v1",
            b"\x01\x02" + b"k" * 32 + b"\x00\x01x",
        ),
        (
            b"publication_catalog_child_v1",
            b"\x01\x07" + b"k" * 32 + b"\x00\x05\x00\x04cbz",
        ),
        (
            b"publication_catalog_child_v1",
            b"\x01\x06" + b"k" * 32 + b"\x00\x05\x00\x03cbz",
        ),
    ],
)
def test_closed_publication_cursor_codecs_reject_noncanonical_frames(
    codec: bytes,
    cursor: bytes,
) -> None:
    import h2hdb.vnext_publication_candidate_repository as module

    with pytest.raises(PublicationCandidateConflictError):
        module._validate_stage_cursor(codec, cursor)
