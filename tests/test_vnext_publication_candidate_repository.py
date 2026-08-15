from __future__ import annotations

import unicodedata
from hashlib import sha256
from pathlib import Path
from shutil import copyfile
from typing import Any
from unittest.mock import patch

import pytest

from h2hdb import vnext_identity as identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_canonical_value_repository import CanonicalValueRepository
from h2hdb.vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_publication_candidate_repository import (
    PublicationCandidateConflictError,
    PublicationCandidateHeadRaceError,
    PublicationCandidateNotReadyError,
    PublicationCandidateRepository,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

_BUILD = b"b" * 16
_ANALYSIS = b"a" * 16
_CANDIDATE = b"c" * 16
_PRODUCER_FIELDS = (
    b"h2hdb-test-writer",
    b"cpython-test-abi",
    b"pillow-test-build",
    b"libjpeg-test-build",
    b"zlib-test-build",
)
_PRODUCER_FINGERPRINT = identity.artifact_producer_fingerprint_sha256(*_PRODUCER_FIELDS)
_CHANNEL = b"default"


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
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocations "
        "(value_sha256, digest_domain, byte_count, allocated_at) "
        "VALUES (%s, %s, %s, %s)",
        (value_sha256, domain, len(exact_payload), 1),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_pages "
        "(page_sha256, value_sha256, page_bytes) VALUES (%s, %s, %s)",
        (encoded_page.page_sha256, value_sha256, encoded_page.page_bytes),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_page_descriptors "
        "(page_sha256, value_sha256, level, page_position, subtree_item_count) "
        "VALUES (%s, %s, %s, %s, %s)",
        (
            encoded_page.page_sha256,
            value_sha256,
            page.level,
            page.page_position,
            page.subtree_byte_count,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_identities "
        "(value_sha256, root_page_sha256) VALUES (%s, %s)",
        (value_sha256, tree.root_page_sha256),
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
    policy_component = identity.artifact_policy_digest(1, 2048, _PRODUCER_FINGERPRINT)
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
        domain=b"artifact_policy_v2",
        serial=3,
        payload=identity.encode_artifact_policy(
            1,
            1024 if forged_policy_payload else 2048,
            _PRODUCER_FINGERPRINT,
        ),
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
        "INSERT INTO catalog_source_snapshot_manifest_identity "
        "(snapshot_manifest_sha256, gallery_count, file_count, byte_count) "
        "VALUES (%s, %s, %s, %s)",
        (snapshot, 0, 0, 0),
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
        (1, 1, unicodedata.unidata_version.encode("ascii")),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policies "
        "(display_title_policy_id, display_title_algorithm_version, "
        "title_sort_policy_id) VALUES (%s, %s, %s)",
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
        "INSERT INTO catalog_source_builds "
        "(build_id, scope_key, manifest_policy_id, state, created_at, sealed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (_BUILD, b"s" * 32, 1, "SEALED", 15, 20),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_channel (build_id, channel) VALUES (%s, %s)",
        (_BUILD, _CHANNEL),
    )
    connector.execute(
        "INSERT INTO catalog_build_manifests "
        "(build_id, manifest_sha256, gallery_count, file_count, byte_count, "
        "computed_at) VALUES (%s, %s, %s, %s, %s, %s)",
        (_BUILD, b"d" * 32, 0, 0, 0, 21),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_runs "
        "(analysis_id, build_id, policy_id, input_manifest_sha256, state, "
        "started_at, completed_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (_ANALYSIS, _BUILD, 1, b"x" * 32, "COMPLETE", 22, 35),
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
            "INSERT INTO catalog_source_build_base_source "
            "(build_id, base_source_revision, base_source_generation) "
            "VALUES (%s, %s, %s)",
            (_BUILD, 1, 1),
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
            (2, "CATALOG"),
        )


def _seed_selected_galleries(
    connector: SQLiteConnector,
    *,
    count: int,
    locator_components_by_gallery: dict[int, tuple[str, ...]] | None = None,
) -> None:
    connector.execute(
        "INSERT INTO catalog_analysis_state_anchors "
        "(analysis_id, anchor_analysis_id, overlay_depth) VALUES (%s, %s, %s)",
        (_ANALYSIS, _ANALYSIS, 0),
    )
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
        connector.execute(
            "INSERT INTO catalog_gallery_identities "
            "(gallery_id, gallery_key, scope_key, locator_sha256) "
            "VALUES (%s, %s, %s, %s)",
            (gallery_id, gallery_key, b"s" * 32, locator),
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
        connector.execute(
            "INSERT INTO catalog_gallery_observation_metadata "
            "(gallery_id, observation_id, gid, upload_time, download_time, "
            "modified_time) VALUES (%s, %s, %s, %s, %s, %s)",
            (gallery_id, 1, 10_000 + gallery_id, 1, 2, 3),
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
        connector.execute(
            "INSERT INTO catalog_analysis_gid_winner_shadows "
            "(analysis_id, gid, winner_gallery_id, decision_sha256) "
            "VALUES (%s, %s, %s, %s)",
            (
                _ANALYSIS,
                10_000 + gallery_id,
                gallery_id,
                sha256(b"winner\0" + gallery_id.to_bytes(8, "big")).digest(),
            ),
        )
    connector.execute(
        "UPDATE catalog_build_manifests SET gallery_count = %s WHERE build_id = %s",
        (count, _BUILD),
    )
    connector.execute(
        "UPDATE catalog_source_snapshot_manifest_identity SET gallery_count = %s",
        (count,),
    )
    connector.execute(
        "UPDATE catalog_analysis_state_component_seals SET row_count = %s "
        "WHERE analysis_id = %s AND state_component = %s",
        (count, _ANALYSIS, b"gid_winner"),
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
        for encoded in tree.pages:
            page = identity.decode_gallery_observation_page(encoded.page_bytes)
            connector.execute(
                "INSERT INTO catalog_gallery_observation_pages "
                "(page_sha256, page_bytes) VALUES (%s, %s)",
                (encoded.page_sha256, encoded.page_bytes),
            )
            connector.execute(
                "INSERT INTO catalog_gallery_observation_page_descriptors "
                "(page_sha256, component, level, subtree_item_count) "
                "VALUES (%s, %s, %s, %s)",
                (
                    encoded.page_sha256,
                    b"METADATA",
                    page.level,
                    page.subtree_item_count,
                ),
            )
            if page.node_kind is identity.GalleryObservationNodeKind.BRANCH:
                for position, entry in enumerate(page.entries):
                    assert isinstance(entry, identity.GalleryObservationBranchEntry)
                    connector.execute(
                        "INSERT INTO catalog_gallery_observation_page_children "
                        "(parent_sha256, position, child_sha256) "
                        "VALUES (%s, %s, %s)",
                        (encoded.page_sha256, position, entry.child_sha256),
                    )
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
            connector.execute(
                "INSERT INTO catalog_tag_terms "
                "(tag_id, namespace, tag_value_sha256) VALUES (%s, %s, %s)",
                (tag_id, namespace, value_sha256),
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
        "SELECT analysis_id, reserved_revision, channel, state, sealed_at "
        "FROM catalog_publication_candidates WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == (_ANALYSIS, expected_revision, _CHANNEL, "OPEN", None)
    assert connector.fetch_one(
        "SELECT candidate_id FROM operational_catalog_working_candidates "
        "WHERE slot = %s",
        (1,),
    ) == (_CANDIDATE,)
    assert connector.fetch_one(
        "SELECT COUNT(*) FROM catalog_publication_checkpoints WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == (17,)
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


@pytest.mark.parametrize("head", ["source", "catalog"])
def test_begin_or_resume_base_race_fails_without_allocator_leak(
    tmp_path: Path,
    head: str,
) -> None:
    connector = _generated_database(tmp_path / f"base-race-{head}.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=True)
    if head == "catalog":
        _begin(connector, gate, turn)
    connector.execute(
        f"UPDATE {'catalog_source_heads' if head == 'source' else 'catalog_publication_heads'} "
        "SET generation = %s WHERE channel = %s",
        (2, _CHANNEL),
    )

    with pytest.raises(PublicationCandidateHeadRaceError):
        _begin(connector, gate, turn, now=101)

    if head == "source":
        assert not connector.fetch_one("SELECT 1 FROM catalog_publication_candidates")
        assert connector.fetch_one(
            "SELECT next_revision FROM operational_revision_allocators "
            "WHERE stream = %s",
            ("CATALOG",),
        ) == (2,)
    else:
        assert connector.fetch_one(
            "SELECT state FROM catalog_publication_candidates"
        ) == ("OPEN",)
    connector.close()


def test_each_begin_mutation_fault_rolls_back_allocator_candidate_and_bases(
    tmp_path: Path,
) -> None:
    base_path = tmp_path / "begin-fault-base.sqlite3"
    base = _generated_database(base_path)
    gate, turn = _authorities(base)
    _seed_completed_analysis(base, turn, with_base=True)
    base.close()

    # Allocator, candidate, source base, catalog base, checkpoint set, working root.
    for failure_at in range(1, 7):
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
        assert plan.child_count == validation.child_count == 7
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
        assert built.row_count == 7 and not built.terminal
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
        assert terminal.terminal and terminal.next_processed_count == 7

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
        assert checked.row_count == 7 and not checked.terminal
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
        assert checked_terminal.next_processed_count == 7

    publication_key = identity.publication_key(10_001)
    assert connector.fetch_one(
        "SELECT publication_count FROM catalog_revisions WHERE revision = 1"
    ) == (1,)
    assert connector.fetch_one(
        "SELECT gallery_id, published_at, modified_at "
        "FROM catalog_publications WHERE revision = 1 AND publication_key = %s",
        (publication_key,),
    ) == (1, 1, 3)
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
    assert not connector.fetch_one(
        "SELECT 1 FROM operational_canonical_value_uploads WHERE generation = %s",
        (turn.generation,),
    )
    connector.close()


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
        assert plan.child_count == validation.child_count == 200
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
            for index in range(3):
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
        assert [batch.row_count for batch in built] == [128, 72, 0]
        assert built[-1].terminal and built[-1].next_processed_count == 200
        normalized = " ".join(mutation_queries).upper()
        assert "COUNT(" not in normalized and "SUM(" not in normalized
        assert "CATALOG_GALLERY_OBSERVATION_TAGS" not in normalized
        assert "CATALOG_GALLERY_OBSERVATION_PAGES" not in normalized

        checked = []
        for index in range(3):
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
        assert [batch.row_count for batch in checked] == [128, 72, 0]
        assert checked[-1].terminal and checked[-1].next_processed_count == 200
    assert connector.fetch_one(
        "SELECT publication_count FROM catalog_revisions WHERE revision = 1"
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
            "INSERT INTO catalog_publications",
            "INSERT INTO catalog_publication_batch_receipts",
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
            assert not connector.fetch_one("SELECT 1 FROM catalog_revisions")
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
        assert not connector.fetch_one("SELECT 1 FROM catalog_revisions")
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
            "DELETE FROM catalog_publication_titles WHERE revision = %s",
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


@pytest.mark.parametrize("head", ["source", "catalog"])
def test_selection_reauthorizes_candidate_base_heads_before_mutation(
    tmp_path: Path,
    head: str,
) -> None:
    connector = _generated_database(tmp_path / f"selection-head-race-{head}.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=True)
    _begin(connector, gate, turn)
    connector.execute(
        f"UPDATE {'catalog_source_heads' if head == 'source' else 'catalog_publication_heads'} "
        "SET generation = %s WHERE channel = %s",
        (2, _CHANNEL),
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
        "DELETE FROM catalog_publication_selections "
        "WHERE candidate_id = %s AND gallery_id = %s",
        (_CANDIDATE, 2),
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

    # Publication identity, selection, immutable receipt, checkpoint CAS.
    for failure_at in range(1, 5):
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
            del data
            self.query = query
            return ()

    connector: Any = RecordingConnector()
    assert (
        module._lock_catalog_working(VNextUnitOfWork(connector, backend="mariadb"))
        is None
    )
    assert connector.query.endswith(" FOR UPDATE")
    assert "%s" in connector.query and "?" not in connector.query


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
            return 17

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

    module._initialize_candidate_checkpoints(work, _CANDIDATE, now=100)
    assert "SELECT %s, stage" in connector.query
    assert "FROM catalog_publication_stages ORDER BY stage_order" in connector.query
    assert connector.query.count("%s") == 6 and "?" not in connector.query


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
            b"\x01\x07" + b"k" * 32 + b"\x00\x05\x00\x03cbz",
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
    ],
)
def test_closed_publication_cursor_codecs_reject_noncanonical_frames(
    codec: bytes,
    cursor: bytes,
) -> None:
    import h2hdb.vnext_publication_candidate_repository as module

    with pytest.raises(PublicationCandidateConflictError):
        module._validate_stage_cursor(codec, cursor)
