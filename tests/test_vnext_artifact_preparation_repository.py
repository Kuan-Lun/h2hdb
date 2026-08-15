from __future__ import annotations

from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from typing import BinaryIO, cast
from unittest.mock import patch

import pytest
from test_vnext_publication_candidate_repository import (
    _ANALYSIS,
    _BUILD,
    _CANDIDATE,
    _PRODUCER_FINGERPRINT,
    _authorities,
    _begin,
    _canonical_identity,
    _complete_selection,
    _generated_database,
    _seed_completed_analysis,
    _seed_projection_metadata,
    _seed_selected_galleries,
    _upload_projection_canonical_values,
)

from h2hdb import vnext_identity as identity
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_artifact_preparation_repository import (
    ArtifactInputProjectionPlan,
    ArtifactPreparationConflictError,
    ArtifactPreparationContractUnavailableError,
    ArtifactPreparationNotReadyError,
    ArtifactPreparationRepository,
    ArtifactStorageEvidence,
)
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_operational_event_repository import (
    OperationalEffectRepository,
    OperationalEffectSeal,
)
from h2hdb.vnext_publication_candidate_repository import (
    PublicationCandidateRepository,
    _MutationAuthority,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

_ARTIFACT_INPUT = b"u" * 16


class _RecordingStorageAdapter:
    adapter_id = b"managed-filesystem"
    producer_fingerprint_sha256 = _PRODUCER_FINGERPRINT

    def __init__(self) -> None:
        self.called = False
        self.archive = b""

    def render_member(
        self,
        source: BinaryIO,
        transform_kind: identity.ArtifactTransformKind,
        destination: BinaryIO,
    ) -> None:
        assert transform_kind is not identity.ArtifactTransformKind.RAW_COPY
        while part := source.read(64 * 1024):
            destination.write(part)

    def protect(
        self,
        archive: BinaryIO,
        locator_components: tuple[str, ...],
        protection_token: bytes,
    ) -> ArtifactStorageEvidence:
        assert locator_components[0] == "sha256"
        assert len(protection_token) == 184
        self.called = True
        self.archive = archive.read()
        return ArtifactStorageEvidence(True)


def _seed_artifact_input(
    connector: SQLiteConnector,
    *,
    source_directory: Path,
    member_size_delta: int = 0,
    materialize: bool = True,
) -> tuple[bytes, bytes]:
    metadata_bytes = b"gid=10001\n"
    image_bytes = b"raw-image-payload"
    metadata_sha256 = sha256(metadata_bytes).digest()
    image_sha256 = sha256(image_bytes).digest()
    for name, payload, digest, file_no in (
        (b"galleryinfo.txt", metadata_bytes, metadata_sha256, 1),
        (b"001.bin", image_bytes, image_sha256, 2),
    ):
        file_key = identity.file_key(name)
        connector.execute(
            "INSERT INTO catalog_file_name_identities "
            "(file_key, name_bytes, file_role) VALUES (%s, %s, %s)",
            (file_key, name, identity.file_role(name)),
        )
        connector.execute(
            "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
            "VALUES (%s, %s)",
            (digest, len(payload)),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_files "
            "(gallery_id, observation_id, file_no, file_key, file_sha256) "
            "VALUES (%s, %s, %s, %s, %s)",
            (1, 1, file_no, file_key, digest),
        )
        stat_result = (source_directory / name.decode("ascii")).stat()
        connector.execute(
            "INSERT INTO catalog_gallery_observation_file_filesystem "
            "(gallery_id, observation_id, file_key, device, inode, modified_ns, "
            "changed_ns) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                1,
                1,
                file_key,
                stat_result.st_dev.to_bytes(8, "big"),
                stat_result.st_ino.to_bytes(8, "big"),
                stat_result.st_mtime_ns.to_bytes(8, "big", signed=True),
                stat_result.st_ctime_ns.to_bytes(8, "big", signed=True),
            ),
        )

    content_payload = b"".join(
        identity.iter_effective_content_payload_ordered(1, (image_sha256,))
    )
    content_sha256 = identity.effective_content_digest((image_sha256,))
    _canonical_identity(
        connector,
        content_sha256,
        domain=b"effective_content_v1",
        serial=80,
        payload=content_payload,
    )
    connector.execute(
        "INSERT INTO catalog_analysis_content_owner_candidate_shadows "
        "(analysis_id, content_sha256, gallery_id, priority_key, candidate_sha256) "
        "VALUES (%s, %s, %s, %s, %s)",
        (_ANALYSIS, content_sha256, 1, b"priority", b"q" * 32),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_content_owner_shadows "
        "(analysis_id, content_sha256, owner_gallery_id, decision_sha256) "
        "VALUES (%s, %s, %s, %s)",
        (_ANALYSIS, content_sha256, 1, b"o" * 32),
    )
    connector.execute(
        "UPDATE catalog_analysis_state_component_seals SET row_count = %s "
        "WHERE analysis_id = %s AND state_component IN (%s, %s)",
        (1, _ANALYSIS, b"content_owner", b"content_owner_candidate"),
    )

    publication_key = identity.publication_key(10_001)
    gallery_key = connector.fetch_one(
        "SELECT gallery_key FROM catalog_gallery_identities WHERE gallery_id = %s",
        (1,),
    )[0]
    observation = connector.fetch_one(
        "SELECT observation_identity_sha256 FROM catalog_gallery_observations "
        "WHERE gallery_id = %s AND observation_id = %s",
        (1, 1),
    )[0]
    policy_component = identity.artifact_policy_digest(1, 2048, _PRODUCER_FINGERPRINT)
    source_manifest = identity.artifact_source_manifest_digest(observation, 1, 1)
    selected = identity.artifact_selected_digest(publication_key, gallery_key)
    owner = identity.artifact_owner_digest(
        content_sha256,
        gallery_key,
        10_001,
        gallery_key,
    )
    member_entries = (
        identity.ArtifactMemberPlanEntry(
            0,
            b"galleryinfo.txt",
            metadata_sha256,
            len(metadata_bytes) + member_size_delta,
            False,
        ),
        identity.ArtifactMemberPlanEntry(
            1,
            b"001.bin",
            image_sha256,
            len(image_bytes),
            False,
        ),
    )
    member_payload = identity.encode_artifact_member_plan(member_entries)
    member_plan = identity.artifact_member_plan_digest(member_entries)
    artifact_effective_payload = identity.encode_artifact_effective_content(
        (image_sha256,)
    )
    artifact_effective = identity.artifact_effective_content_digest((image_sha256,))
    semantics_payload = identity.encode_artifact_semantics(
        source_manifest,
        member_plan,
        artifact_effective,
        selected,
        owner,
        policy_component,
    )
    semantics = identity.artifact_semantics_digest(
        source_manifest,
        member_plan,
        artifact_effective,
        selected,
        owner,
        policy_component,
    )
    if not materialize:
        byte_count = len(metadata_bytes) + len(image_bytes)
        connector.execute(
            "UPDATE catalog_build_manifests SET file_count = %s, byte_count = %s "
            "WHERE build_id = %s",
            (2, byte_count, _BUILD),
        )
        connector.execute(
            "UPDATE catalog_source_snapshot_manifest_identity "
            "SET file_count = %s, byte_count = %s",
            (2, byte_count),
        )
        return publication_key, member_plan
    component_payloads = (
        (
            source_manifest,
            b"artifact_source_manifest_v1",
            identity.encode_artifact_source_manifest(observation, 1, 1),
        ),
        (member_plan, b"artifact_member_plan_v1", member_payload),
        (
            artifact_effective,
            b"artifact_effective_content_v1",
            artifact_effective_payload,
        ),
        (
            selected,
            b"artifact_selected_v1",
            identity.encode_artifact_selected(publication_key, gallery_key),
        ),
        (
            owner,
            b"artifact_owner_v1",
            identity.encode_artifact_owner(
                content_sha256,
                gallery_key,
                10_001,
                gallery_key,
            ),
        ),
        (semantics, b"artifact_semantics_v1", semantics_payload),
    )
    for serial, (digest, domain, payload) in enumerate(component_payloads, start=90):
        _canonical_identity(
            connector,
            digest,
            domain=domain,
            serial=serial,
            payload=payload,
        )
    connector.execute(
        "INSERT INTO catalog_artifact_semantic_input "
        "(artifact_semantics_sha256, source_manifest_component_sha256, "
        "member_plan_component_sha256, effective_content_component_sha256, "
        "selected_component_sha256, owner_component_sha256, "
        "policy_component_sha256) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (
            semantics,
            source_manifest,
            member_plan,
            artifact_effective,
            selected,
            owner,
            policy_component,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_candidate_artifact_inputs "
        "(artifact_input_id, candidate_id, publication_key, "
        "artifact_semantics_sha256) VALUES (%s, %s, %s, %s)",
        (
            _ARTIFACT_INPUT,
            _CANDIDATE,
            publication_key,
            semantics,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_operations "
        "(candidate_id, publication_key, operation) "
        "VALUES (%s, %s, %s)",
        (_CANDIDATE, publication_key, "CREATE"),
    )
    connector.execute(
        "UPDATE catalog_publication_checkpoints SET generation = %s, cursor = %s, "
        "processed_count = %s, state = %s, updated_at = %s "
        "WHERE candidate_id = %s AND stage = %s",
        (
            3,
            publication_key,
            1,
            "COMPLETE",
            106,
            _CANDIDATE,
            b"BUILD_ARTIFACT_DELTA_OPERATION",
        ),
    )
    connector.execute(
        "UPDATE catalog_publication_checkpoints SET generation = %s, cursor = %s, "
        "processed_count = %s, state = %s, updated_at = %s "
        "WHERE candidate_id = %s AND stage = %s",
        (
            3,
            publication_key,
            1,
            "COMPLETE",
            108,
            _CANDIDATE,
            b"VALIDATE_ARTIFACT_INPUT_DELTA",
        ),
    )
    connector.execute(
        "INSERT INTO catalog_publication_batch_receipts "
        "(candidate_id, stage, batch_key, start_generation, start_cursor, "
        "start_processed_count, next_cursor, next_processed_count, next_state, "
        "row_count, terminal, committed_generation, committed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            _CANDIDATE,
            b"VALIDATE_ARTIFACT_INPUT_DELTA",
            b"validate-artifact-row",
            1,
            b"",
            0,
            publication_key,
            1,
            "OPEN",
            1,
            0,
            2,
            107,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_publication_batch_receipts "
        "(candidate_id, stage, batch_key, start_generation, start_cursor, "
        "start_processed_count, next_cursor, next_processed_count, next_state, "
        "row_count, terminal, committed_generation, committed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            _CANDIDATE,
            b"VALIDATE_ARTIFACT_INPUT_DELTA",
            b"validate-artifact-terminal",
            2,
            publication_key,
            1,
            publication_key,
            1,
            "COMPLETE",
            0,
            1,
            3,
            108,
        ),
    )
    return publication_key, member_plan


def _database_with_artifact_input(
    tmp_path: Path,
    *,
    member_size_delta: int = 0,
) -> tuple[SQLiteConnector, GateLease, IngestTurn, bytes, bytes]:
    connector = _generated_database(tmp_path / "artifact-preparation.sqlite3")
    gate, turn = _authorities(connector)
    source_directory = tmp_path / "source" / "gallery-1"
    source_directory.mkdir(parents=True)
    (source_directory / "galleryinfo.txt").write_bytes(b"gid=10001\n")
    (source_directory / "001.bin").write_bytes(b"raw-image-payload")
    _seed_completed_analysis(
        connector,
        turn,
        with_base=False,
        source_root_components=tuple((tmp_path / "source").parts[1:]),
    )
    _seed_selected_galleries(
        connector,
        count=1,
        locator_components_by_gallery={1: ("gallery-1",)},
    )
    _begin(connector, gate, turn, artifacts_required=True)
    _complete_selection(connector, gate, turn)
    publication_key, member_plan = _seed_artifact_input(
        connector,
        source_directory=source_directory,
        member_size_delta=member_size_delta,
    )
    return connector, gate, turn, publication_key, member_plan


def _complete_catalog_projection(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    *,
    now: int,
) -> int:
    with connector.transaction():
        authority = PublicationCandidateRepository.issue_projection_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            now=now,
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
        _upload_projection_canonical_values(connector, gate, turn, plan, now=now + 1)
        timestamp = now + 2
        for method, keyword, batch_prefix in (
            (
                PublicationCandidateRepository.process_catalog_projection_batch,
                {"plan": plan},
                b"artifact-catalog-build-",
            ),
            (
                PublicationCandidateRepository.validate_catalog_projection_batch,
                {"validation": validation},
                b"artifact-catalog-validate-",
            ),
        ):
            index = 0
            while True:
                with connector.transaction():
                    result = method(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        candidate_id=_CANDIDATE,
                        batch_key=batch_prefix + index.to_bytes(4, "big"),
                        now=timestamp,
                        **keyword,
                    )
                timestamp += 1
                index += 1
                if result.terminal:
                    break
    return timestamp


def _upload_canonical_plan(
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
                now=now,
            )
    with connector.transaction():
        CanonicalValueRepository.seal(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=now,
        )


def _operational_effect_seal(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    *,
    now: int,
) -> OperationalEffectSeal:
    connector.execute(
        "INSERT INTO operational_operational_policys "
        "(operational_policy_id, operational_schema_version, algorithm_version, "
        "max_batch_rows) VALUES (%s, %s, %s, %s)",
        (1, 1, 1, 128),
    )
    with connector.transaction():
        preparation = OperationalEffectRepository.begin(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            build_id=_BUILD,
            operational_policy_id=1,
            now=now,
        )
    with connector.transaction():
        terminal = OperationalEffectRepository.append_batch(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            preparation_id=preparation.preparation_id,
            effects=(),
            now=now + 1,
        )
    assert terminal.terminal
    with connector.transaction():
        return OperationalEffectRepository.seal(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            preparation_id=preparation.preparation_id,
            now=now + 2,
        )


def _database_through_stage_seven(
    tmp_path: Path,
) -> tuple[SQLiteConnector, GateLease, IngestTurn, bytes, int]:
    connector = _generated_database(tmp_path / "artifact-stage-seven.sqlite3")
    gate, turn = _authorities(connector)
    source_directory = tmp_path / "source" / "gallery-1"
    source_directory.mkdir(parents=True)
    (source_directory / "galleryinfo.txt").write_bytes(b"gid=10001\n")
    (source_directory / "001.bin").write_bytes(b"raw-image-payload")
    _seed_completed_analysis(
        connector,
        turn,
        with_base=False,
        source_root_components=tuple((tmp_path / "source").parts[1:]),
    )
    _seed_selected_galleries(
        connector,
        count=1,
        locator_components_by_gallery={1: ("gallery-1",)},
    )
    _seed_projection_metadata(connector, count=1)
    publication_key, _member_plan = _seed_artifact_input(
        connector,
        source_directory=source_directory,
        materialize=False,
    )
    _begin(connector, gate, turn, artifacts_required=True)
    _complete_selection(connector, gate, turn)
    now = _complete_catalog_projection(connector, gate, turn, now=110)
    with connector.transaction():
        input_authority = (
            ArtifactPreparationRepository.issue_input_projection_authority(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                now=now,
            )
        )
    with (
        ArtifactPreparationRepository.prepare_artifact_input_projection(
            connector,
            backend="sqlite",
            authority=input_authority,
        ) as plan,
        ArtifactPreparationRepository.prepare_artifact_input_validation(
            connector,
            backend="sqlite",
            authority=input_authority,
        ) as validation,
    ):
        _upload_projection_canonical_values(connector, gate, turn, plan, now=now + 1)
        now += 2
        for projection_method, keyword, prefix in (
            (
                ArtifactPreparationRepository.process_artifact_input_batch,
                {"plan": plan},
                b"artifact-input-",
            ),
            (
                ArtifactPreparationRepository.process_artifact_delta_operation_batch,
                {},
                b"artifact-delta-",
            ),
            (
                ArtifactPreparationRepository.validate_artifact_input_delta_batch,
                {"validation": validation},
                b"artifact-input-validate-",
            ),
        ):
            index = 0
            while True:
                with connector.transaction():
                    batch = projection_method(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        candidate_id=_CANDIDATE,
                        batch_key=prefix + index.to_bytes(4, "big"),
                        now=now,
                        **keyword,
                    )
                now += 1
                index += 1
                if batch.terminal:
                    break
    return connector, gate, turn, publication_key, now


def _seed_many_artifact_sources(connector: SQLiteConnector, *, count: int) -> None:
    metadata_name = b"galleryinfo.txt"
    image_name = b"001.bin"
    metadata_key = identity.file_key(metadata_name)
    image_key = identity.file_key(image_name)
    metadata_sha256 = sha256(b"shared-metadata").digest()
    connector.execute(
        "INSERT INTO catalog_file_name_identities "
        "(file_key, name_bytes, file_role) VALUES (%s, %s, %s)",
        (metadata_key, metadata_name, identity.file_role(metadata_name)),
    )
    connector.execute(
        "INSERT INTO catalog_file_name_identities "
        "(file_key, name_bytes, file_role) VALUES (%s, %s, %s)",
        (image_key, image_name, identity.file_role(image_name)),
    )
    connector.execute(
        "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
        "VALUES (%s, %s)",
        (metadata_sha256, 15),
    )
    for gallery_id in range(1, count + 1):
        image_sha256 = sha256(
            b"high-card-image\0" + gallery_id.to_bytes(8, "big")
        ).digest()
        connector.execute(
            "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
            "VALUES (%s, %s)",
            (image_sha256, 17),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_files "
            "(gallery_id, observation_id, file_no, file_key, file_sha256) "
            "VALUES (%s, %s, %s, %s, %s)",
            (gallery_id, 1, 1, metadata_key, metadata_sha256),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_files "
            "(gallery_id, observation_id, file_no, file_key, file_sha256) "
            "VALUES (%s, %s, %s, %s, %s)",
            (gallery_id, 1, 2, image_key, image_sha256),
        )
        content_sha256 = identity.effective_content_digest((image_sha256,))
        _canonical_identity(
            connector,
            content_sha256,
            domain=b"effective_content_v1",
            serial=30_000 + gallery_id,
            payload=b"".join(
                identity.iter_effective_content_payload_ordered(1, (image_sha256,))
            ),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_content_owner_candidate_shadows "
            "(analysis_id, content_sha256, gallery_id, priority_key, "
            "candidate_sha256) VALUES (%s, %s, %s, %s, %s)",
            (
                _ANALYSIS,
                content_sha256,
                gallery_id,
                gallery_id.to_bytes(8, "big"),
                sha256(b"candidate\0" + gallery_id.to_bytes(8, "big")).digest(),
            ),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_content_owner_shadows "
            "(analysis_id, content_sha256, owner_gallery_id, decision_sha256) "
            "VALUES (%s, %s, %s, %s)",
            (
                _ANALYSIS,
                content_sha256,
                gallery_id,
                sha256(b"owner\0" + gallery_id.to_bytes(8, "big")).digest(),
            ),
        )
    connector.execute(
        "UPDATE catalog_analysis_state_component_seals SET row_count = %s "
        "WHERE analysis_id = %s AND state_component IN (%s, %s)",
        (count, _ANALYSIS, b"content_owner", b"content_owner_candidate"),
    )
    connector.execute(
        "UPDATE catalog_build_manifests SET file_count = %s, byte_count = %s "
        "WHERE build_id = %s",
        (count * 2, count * 32, _BUILD),
    )
    connector.execute(
        "UPDATE catalog_source_snapshot_manifest_identity "
        "SET file_count = %s, byte_count = %s",
        (count * 2, count * 32),
    )


def _force_terminal_empty_stage(
    connector: SQLiteConnector,
    *,
    stage: bytes,
    batch_key: bytes,
    now: int,
) -> None:
    assert connector.fetch_one(
        "SELECT generation, cursor, processed_count, state "
        "FROM catalog_publication_checkpoints "
        "WHERE candidate_id = %s AND stage = %s",
        (_CANDIDATE, stage),
    ) == (1, b"", 0, "OPEN")
    connector.execute(
        "INSERT INTO catalog_publication_batch_receipts "
        "(candidate_id, stage, batch_key, start_generation, start_cursor, "
        "start_processed_count, next_cursor, next_processed_count, next_state, "
        "row_count, terminal, committed_generation, committed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            _CANDIDATE,
            stage,
            batch_key,
            1,
            b"",
            0,
            b"",
            0,
            "COMPLETE",
            0,
            1,
            2,
            now,
        ),
    )
    connector.execute(
        "UPDATE catalog_publication_checkpoints SET generation = %s, state = %s, "
        "updated_at = %s WHERE candidate_id = %s AND stage = %s",
        (2, "COMPLETE", now, _CANDIDATE, stage),
    )


def _seed_input_plan_uploads(
    connector: SQLiteConnector,
    turn: IngestTurn,
    plan: ArtifactInputProjectionPlan,
) -> None:
    serial = 50_000
    for upload in plan.iter_canonical_value_plans():
        try:
            if not connector.fetch_one(
                "SELECT 1 FROM catalog_canonical_value_allocations "
                "WHERE value_sha256 = %s",
                (upload.value_sha256,),
            ):
                _canonical_identity(
                    connector,
                    upload.value_sha256,
                    domain=upload.digest_domain,
                    serial=serial,
                    payload=b"".join(upload.iter_payload_parts()),
                )
                serial += 1
            connector.execute(
                "INSERT INTO operational_canonical_value_uploads "
                "(generation, value_sha256) VALUES (%s, %s)",
                (turn.generation, upload.value_sha256),
            )
        finally:
            upload.close()


def test_authority_audit_and_canonical_storage_receipt(
    tmp_path: Path,
) -> None:
    connector, gate, turn, publication_key, _member_plan = (
        _database_with_artifact_input(tmp_path)
    )
    with connector.transaction():
        authority = ArtifactPreparationRepository.issue_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            publication_key=publication_key,
            now=110,
        )
    with connector.transaction():
        replayed_authority = ArtifactPreparationRepository.issue_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            publication_key=publication_key,
            now=111,
        )
    assert replayed_authority == authority
    source_queries: list[str] = []
    original_fetch_all = connector.fetch_all

    def recording_fetch_all(
        query: str,
        data: tuple[object, ...] = (),
    ) -> list[tuple[object, ...]]:
        if "catalog_gallery_observation_files" in query:
            source_queries.append(query)
        return original_fetch_all(query, data)

    with (
        patch.object(connector, "execute", side_effect=AssertionError("write")),
        patch.object(connector, "fetch_all", side_effect=recording_fetch_all),
    ):
        audit = ArtifactPreparationRepository.audit_inputs(
            connector,
            backend="sqlite",
            authority=authority,
        )
    assert source_queries and all("LIMIT 128" in query for query in source_queries)
    assert audit.source_entry_count == 2
    assert audit.emitted_member_count == 2
    assert audit.effective_content_file_count == 1
    assert audit.zip_comment == identity.encode_zip_comment(
        authority.source_manifest_component_sha256,
        authority.effective_content_component_sha256,
    )
    adapter = _RecordingStorageAdapter()
    with ArtifactPreparationRepository.prepare_with_storage_adapter(
        connector,
        backend="sqlite",
        audit=audit,
        adapter=adapter,
    ) as receipt:
        assert receipt.artifact_sha256 == sha256(adapter.archive).digest()
        assert receipt.size_bytes == len(adapter.archive)
        assert receipt.locator_components == identity.artifact_locator_components(
            receipt.artifact_sha256
        )
        assert (
            identity.decode_artifact_protection_token(
                receipt.protection_token
            ).candidate_id
            == _CANDIDATE
        )
    assert adapter.called
    assert not connector.fetch_one("SELECT 1 FROM catalog_artifact_blobs")
    assert not connector.fetch_one("SELECT 1 FROM catalog_prepared_artifacts")
    connector.close()


def test_wrong_storage_adapter_fails_before_rendering(tmp_path: Path) -> None:
    connector, gate, turn, publication_key, _member_plan = (
        _database_with_artifact_input(tmp_path)
    )
    with connector.transaction():
        authority = ArtifactPreparationRepository.issue_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            publication_key=publication_key,
            now=110,
        )
    audit = ArtifactPreparationRepository.audit_inputs(
        connector, backend="sqlite", authority=authority
    )
    adapter = _RecordingStorageAdapter()
    adapter.adapter_id = b"forged-storage"
    with pytest.raises(ArtifactPreparationContractUnavailableError):
        ArtifactPreparationRepository.prepare_with_storage_adapter(
            connector,
            backend="sqlite",
            audit=audit,
            adapter=adapter,
        )
    assert not adapter.called
    connector.close()


def test_storage_adapter_cannot_mutate_verified_archive(tmp_path: Path) -> None:
    connector, gate, turn, publication_key, _member_plan = (
        _database_with_artifact_input(tmp_path)
    )
    with connector.transaction():
        authority = ArtifactPreparationRepository.issue_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            publication_key=publication_key,
            now=110,
        )
    audit = ArtifactPreparationRepository.audit_inputs(
        connector,
        backend="sqlite",
        authority=authority,
    )

    class MutatingAdapter(_RecordingStorageAdapter):
        def protect(
            self,
            archive: BinaryIO,
            locator_components: tuple[str, ...],
            protection_token: bytes,
        ) -> ArtifactStorageEvidence:
            evidence = super().protect(
                archive,
                locator_components,
                protection_token,
            )
            archive.seek(0)
            archive.write(b"X")
            return evidence

    with pytest.raises(
        ArtifactPreparationConflictError,
        match="changed the verified archive bytes",
    ):
        ArtifactPreparationRepository.prepare_with_storage_adapter(
            connector,
            backend="sqlite",
            audit=audit,
            adapter=MutatingAdapter(),
        )
    assert not connector.fetch_one("SELECT 1 FROM catalog_artifact_blobs")
    connector.close()


def test_input_audit_rejects_member_plan_source_size_corruption(
    tmp_path: Path,
) -> None:
    connector, gate, turn, publication_key, _member_plan = (
        _database_with_artifact_input(tmp_path, member_size_delta=1)
    )
    with connector.transaction():
        authority = ArtifactPreparationRepository.issue_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            publication_key=publication_key,
            now=110,
        )
    with pytest.raises(
        ArtifactPreparationConflictError,
        match="member plan differs",
    ):
        ArtifactPreparationRepository.audit_inputs(
            connector,
            backend="sqlite",
            authority=authority,
        )
    connector.close()


def test_forged_repository_capability_is_revalidated_against_durable_facts(
    tmp_path: Path,
) -> None:
    connector, gate, turn, publication_key, _member_plan = (
        _database_with_artifact_input(tmp_path)
    )
    with connector.transaction():
        authority = ArtifactPreparationRepository.issue_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            publication_key=publication_key,
            now=110,
        )
    forged = replace(authority, producer_fingerprint_sha256=b"x" * 32)
    with pytest.raises(
        ArtifactPreparationConflictError,
        match="authority changed",
    ):
        ArtifactPreparationRepository.audit_inputs(
            connector,
            backend="sqlite",
            authority=forged,
        )
    connector.close()


def test_nonterminal_checkpoint_fails_closed_without_writes(tmp_path: Path) -> None:
    connector, gate, turn, publication_key, _member_plan = (
        _database_with_artifact_input(tmp_path)
    )
    connector.execute(
        "UPDATE catalog_publication_checkpoints SET state = %s "
        "WHERE candidate_id = %s AND stage = %s",
        ("OPEN", _CANDIDATE, b"VALIDATE_ARTIFACT_INPUT_DELTA"),
    )
    with pytest.raises(
        ArtifactPreparationNotReadyError,
        match="checkpoint is incomplete",
    ):
        with connector.transaction():
            ArtifactPreparationRepository.issue_authority(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                publication_key=publication_key,
                now=110,
            )
    assert not connector.fetch_one("SELECT 1 FROM catalog_prepared_artifacts")
    connector.close()


def test_artifact_projection_persistence_and_seal_end_to_end(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "artifact-projection.sqlite3")
    gate, turn = _authorities(connector)
    source_directory = tmp_path / "source" / "gallery-1"
    source_directory.mkdir(parents=True)
    (source_directory / "galleryinfo.txt").write_bytes(b"gid=10001\n")
    (source_directory / "001.bin").write_bytes(b"raw-image-payload")
    _seed_completed_analysis(
        connector,
        turn,
        with_base=False,
        source_root_components=tuple((tmp_path / "source").parts[1:]),
    )
    _seed_selected_galleries(
        connector,
        count=1,
        locator_components_by_gallery={1: ("gallery-1",)},
    )
    _seed_projection_metadata(connector, count=1)
    publication_key, _member_plan = _seed_artifact_input(
        connector,
        source_directory=source_directory,
        materialize=False,
    )
    _begin(connector, gate, turn, artifacts_required=True)
    _complete_selection(connector, gate, turn)
    now = _complete_catalog_projection(connector, gate, turn, now=110)

    with connector.transaction():
        input_authority = (
            ArtifactPreparationRepository.issue_input_projection_authority(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                now=now,
            )
        )
    with (
        ArtifactPreparationRepository.prepare_artifact_input_projection(
            connector,
            backend="sqlite",
            authority=input_authority,
        ) as plan,
        ArtifactPreparationRepository.prepare_artifact_input_validation(
            connector,
            backend="sqlite",
            authority=input_authority,
        ) as validation,
    ):
        _upload_projection_canonical_values(connector, gate, turn, plan, now=now + 1)
        now += 2
        for method, keyword, prefix in (
            (
                ArtifactPreparationRepository.process_artifact_input_batch,
                {"plan": plan},
                b"artifact-input-",
            ),
            (
                ArtifactPreparationRepository.process_artifact_delta_operation_batch,
                {},
                b"artifact-delta-",
            ),
            (
                ArtifactPreparationRepository.validate_artifact_input_delta_batch,
                {"validation": validation},
                b"artifact-input-validate-",
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
                        now=now,
                        **keyword,
                    )
                now += 1
                index += 1
                if batch.terminal:
                    break

    assert connector.fetch_one(
        "SELECT operation FROM catalog_artifact_operations "
        "WHERE candidate_id = %s AND publication_key = %s",
        (_CANDIDATE, publication_key),
    ) == ("CREATE",)
    with connector.transaction():
        authority = ArtifactPreparationRepository.issue_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            publication_key=publication_key,
            now=now,
        )
    audit = ArtifactPreparationRepository.audit_inputs(
        connector,
        backend="sqlite",
        authority=authority,
    )
    adapter = _RecordingStorageAdapter()
    effect_seal = _operational_effect_seal(connector, gate, turn, now=now)
    now += 3
    with ArtifactPreparationRepository.prepare_with_storage_adapter(
        connector,
        backend="sqlite",
        audit=audit,
        adapter=adapter,
    ) as receipt:
        _upload_canonical_plan(
            connector,
            gate,
            turn,
            receipt.locator_plan,
            now=now,
        )
        now += 1
        with connector.transaction():
            persisted = ArtifactPreparationRepository.persist_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                effect_seal=effect_seal,
                now=now,
            )
        now += 1
        with connector.transaction():
            replayed = ArtifactPreparationRepository.persist_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                effect_seal=effect_seal,
                now=now,
            )
        now += 1
        with connector.transaction():
            gate = MaintenanceGateRepository.renew(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate,
                now=now,
                lease_duration=2_000_000,
            )
        takeover_at = turn.lease_expires_at
        with connector.transaction():
            turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"n" * 16,
                now=takeover_at,
                lease_duration=1_000_000,
            )
        connector.execute(
            "INSERT INTO operational_source_build_generations "
            "(build_id, generation) VALUES (%s, %s)",
            (_BUILD, turn.generation),
        )
        now = takeover_at + 1
        with connector.transaction():
            new_turn_replay = ArtifactPreparationRepository.persist_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                effect_seal=effect_seal,
                now=now,
            )
        now += 1
    assert not persisted.replayed and replayed.replayed and new_turn_replay.replayed

    for validation_method, prefix in (
        (ArtifactPreparationRepository.validate_prepared_artifact_batch, b"prepared-"),
        (ArtifactPreparationRepository.validate_create_batch, b"create-"),
        (ArtifactPreparationRepository.validate_rebuild_batch, b"rebuild-"),
        (ArtifactPreparationRepository.validate_delete_batch, b"delete-"),
        (ArtifactPreparationRepository.validate_unchanged_batch, b"unchanged-"),
        (ArtifactPreparationRepository.validate_new_gallery_batch, b"new-"),
        (ArtifactPreparationRepository.validate_changed_gallery_batch, b"changed-"),
        (ArtifactPreparationRepository.validate_removed_gallery_batch, b"removed-"),
        (ArtifactPreparationRepository.validate_duplicate_loser_batch, b"loser-"),
    ):
        index = 0
        while True:
            with connector.transaction():
                batch = validation_method(
                    work=VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    candidate_id=_CANDIDATE,
                    batch_key=prefix + index.to_bytes(4, "big"),
                    now=now,
                )
            now += 1
            index += 1
            if batch.terminal:
                break

    assert connector.fetch_one(
        "SELECT publication_count, artifact_input_count, "
        "prepared_artifact_count, create_count, rebuild_count, delete_count, "
        "unchanged_count, new_galleries, changed_galleries, removed_galleries, "
        "duplicate_losers FROM catalog_publication_candidate_projection_seal "
        "WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == (1, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0)
    assert connector.fetch_one(
        "SELECT state FROM catalog_publication_candidates WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == ("SEALED",)
    connector.close()


def test_persistence_collision_forgery_and_each_statement_fault_roll_back(
    tmp_path: Path,
) -> None:
    connector, gate, turn, publication_key, now = _database_through_stage_seven(
        tmp_path
    )
    with connector.transaction():
        authority = ArtifactPreparationRepository.issue_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            publication_key=publication_key,
            now=now,
        )
    audit = ArtifactPreparationRepository.audit_inputs(
        connector,
        backend="sqlite",
        authority=authority,
    )
    effect_seal = _operational_effect_seal(connector, gate, turn, now=now)
    now += 3
    with ArtifactPreparationRepository.prepare_with_storage_adapter(
        connector,
        backend="sqlite",
        audit=audit,
        adapter=_RecordingStorageAdapter(),
    ) as receipt:
        original_size = receipt.size_bytes
        receipt.size_bytes += 1
        with pytest.raises(
            ArtifactPreparationConflictError,
            match="receipt token changed",
        ):
            with connector.transaction():
                ArtifactPreparationRepository.persist_prepared_artifact(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    receipt=receipt,
                    effect_seal=effect_seal,
                    now=now,
                )
        receipt.size_bytes = original_size
        _upload_canonical_plan(
            connector,
            gate,
            turn,
            receipt.locator_plan,
            now=now,
        )
        now += 1
        with pytest.raises(
            ArtifactPreparationConflictError,
            match="artifact blob collides",
        ):
            with connector.transaction():
                connector.execute(
                    "INSERT INTO catalog_artifact_blobs "
                    "(artifact_sha256, size_bytes) VALUES (%s, %s)",
                    (receipt.artifact_sha256, receipt.size_bytes + 1),
                )
                ArtifactPreparationRepository.persist_prepared_artifact(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    receipt=receipt,
                    effect_seal=effect_seal,
                    now=now,
                )
        assert not connector.fetch_one("SELECT 1 FROM catalog_artifact_blobs")

        insert_faults = (
            "INSERT INTO catalog_artifact_blobs ",
            "INSERT INTO catalog_artifact_identity ",
            "INSERT INTO catalog_artifact_location ",
            "INSERT INTO catalog_prepared_artifacts ",
            "INSERT INTO catalog_artifacts ",
            "INSERT INTO operational_publication_candidate_preparations ",
        )
        original_execute = connector.execute
        for index, fragment in enumerate(insert_faults):

            def fail_insert(
                query: str,
                data: tuple[object, ...] = (),
                *,
                exact_fragment: str = fragment,
            ) -> None:
                if exact_fragment in query:
                    raise RuntimeError(f"fault at {exact_fragment}")
                original_execute(query, data)

            with pytest.raises(RuntimeError, match="fault at"):
                with (
                    connector.transaction(),
                    patch.object(
                        connector,
                        "execute",
                        side_effect=fail_insert,
                    ),
                ):
                    ArtifactPreparationRepository.persist_prepared_artifact(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        receipt=receipt,
                        effect_seal=effect_seal,
                        now=now + index,
                    )
            for table in (
                "catalog_artifact_blobs",
                "catalog_artifact_identity",
                "catalog_artifact_location",
                "catalog_prepared_artifacts",
                "catalog_artifacts",
                "operational_publication_candidate_preparations",
            ):
                assert not connector.fetch_one(f"SELECT 1 FROM {table}")
        now += len(insert_faults)

        original_execute_affected = connector.execute_affected

        def fail_claim_handoff(
            query: str,
            data: tuple[object, ...] = (),
        ) -> int:
            if "DELETE FROM operational_canonical_value_uploads" in query:
                raise RuntimeError("fault at locator claim handoff")
            return original_execute_affected(query, data)

        with pytest.raises(RuntimeError, match="locator claim handoff"):
            with (
                connector.transaction(),
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=fail_claim_handoff,
                ),
            ):
                ArtifactPreparationRepository.persist_prepared_artifact(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    receipt=receipt,
                    effect_seal=effect_seal,
                    now=now,
                )
        assert not connector.fetch_one("SELECT 1 FROM catalog_artifact_blobs")
        assert connector.fetch_one(
            "SELECT generation FROM operational_canonical_value_uploads "
            "WHERE value_sha256 = %s",
            (receipt.artifact_locator_sha256,),
        ) == (turn.generation,)
        with connector.transaction():
            persisted = ArtifactPreparationRepository.persist_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                effect_seal=effect_seal,
                now=now + 1,
            )
    assert not persisted.replayed
    connector.execute(
        "UPDATE catalog_prepared_artifacts SET protection_token = %s "
        "WHERE candidate_id = %s AND publication_key = %s",
        (b"x" * 184, _CANDIDATE, publication_key),
    )
    with pytest.raises(
        ArtifactPreparationConflictError,
        match="protection token is malformed",
    ):
        with connector.transaction():
            ArtifactPreparationRepository.validate_prepared_artifact_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                batch_key=b"corrupt-prepared",
                now=now + 2,
            )
    assert connector.fetch_one(
        "SELECT generation, cursor, processed_count, state "
        "FROM catalog_publication_checkpoints "
        "WHERE candidate_id = %s AND stage = %s",
        (_CANDIDATE, b"VALIDATE_PREPARED_ARTIFACT"),
    ) == (1, b"", 0, "OPEN")
    connector.close()


def test_high_cardinality_stages_five_to_seven_are_fixed_128_and_no_child_scan(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "artifact-large.sqlite3")
    gate, turn = _authorities(connector)
    _seed_completed_analysis(connector, turn, with_base=False)
    _seed_selected_galleries(connector, count=129)
    _seed_projection_metadata(connector, count=129)
    _seed_many_artifact_sources(connector, count=129)
    _begin(connector, gate, turn, artifacts_required=True)
    _complete_selection(connector, gate, turn)
    _force_terminal_empty_stage(
        connector,
        stage=b"BUILD_CATALOG_PROJECTION",
        batch_key=b"forced-catalog-build-terminal",
        now=110,
    )
    _force_terminal_empty_stage(
        connector,
        stage=b"VALIDATE_CATALOG_PROJECTION",
        batch_key=b"forced-catalog-validate-terminal",
        now=111,
    )
    with connector.transaction():
        authority = ArtifactPreparationRepository.issue_input_projection_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            now=112,
        )
    with (
        ArtifactPreparationRepository.prepare_artifact_input_projection(
            connector,
            backend="sqlite",
            authority=authority,
        ) as plan,
        ArtifactPreparationRepository.prepare_artifact_input_validation(
            connector,
            backend="sqlite",
            authority=authority,
        ) as validation,
    ):
        assert plan.input_count == validation.input_count == 129
        _seed_input_plan_uploads(connector, turn, plan)
        mutation_queries: list[str] = []
        original_fetch_all = connector.fetch_all

        def recording_fetch_all(
            query: str,
            data: tuple[object, ...] = (),
        ) -> list[tuple[object, ...]]:
            mutation_queries.append(query)
            return original_fetch_all(query, data)

        results = []
        with patch.object(connector, "fetch_all", side_effect=recording_fetch_all):
            now = 113
            for method, keyword, prefix in (
                (
                    ArtifactPreparationRepository.process_artifact_input_batch,
                    {"plan": plan},
                    b"large-input-",
                ),
                (
                    ArtifactPreparationRepository.process_artifact_delta_operation_batch,
                    {},
                    b"large-delta-",
                ),
                (
                    ArtifactPreparationRepository.validate_artifact_input_delta_batch,
                    {"validation": validation},
                    b"large-validate-",
                ),
            ):
                stage_results = []
                for index in range(3):
                    with connector.transaction():
                        stage_results.append(
                            method(
                                VNextUnitOfWork(connector, backend="sqlite"),
                                gate_lease=gate,
                                ingest_turn=turn,
                                candidate_id=_CANDIDATE,
                                batch_key=prefix + index.to_bytes(4, "big"),
                                now=now,
                                **keyword,
                            )
                        )
                    now += 1
                results.append(stage_results)
        assert [batch.row_count for stage in results for batch in stage] == [
            128,
            1,
            0,
        ] * 3
        assert all(stage[-1].terminal for stage in results)
        normalized = " ".join(mutation_queries).upper()
        assert "COUNT(" not in normalized and "SUM(" not in normalized
        assert "CATALOG_GALLERY_OBSERVATION_FILES" not in normalized
        assert "CATALOG_ANALYSIS_CONTENT_OWNER" not in normalized
        bounded_queries = (
            query
            for query in mutation_queries
            if "CATALOG_CANDIDATE_ARTIFACT_INPUTS" in query.upper()
            or "CATALOG_ARTIFACTS" in query.upper()
            or "CATALOG_PREPARED_ARTIFACTS" in query.upper()
        )
        assert all("LIMIT" in query.upper() for query in bounded_queries)
    connector.close()


def test_mariadb_artifact_evaluators_use_bounded_server_sql_shape() -> None:
    from types import SimpleNamespace

    import h2hdb.vnext_artifact_preparation_repository as module

    class RecordingConnector:
        def __init__(self) -> None:
            self.queries: list[tuple[str, tuple[object, ...]]] = []

        def fetch_all(
            self,
            query: str,
            data: tuple[object, ...] = (),
        ) -> list[tuple[object, ...]]:
            self.queries.append((query, data))
            return []

    connector = RecordingConnector()
    work = VNextUnitOfWork(cast(SQLiteConnector, connector), backend="mariadb")
    mutation = cast(
        _MutationAuthority,
        SimpleNamespace(
            candidate=SimpleNamespace(candidate_id=_CANDIDATE, reserved_revision=8),
            base_catalog=SimpleNamespace(revision=7),
        ),
    )
    assert (
        module._operation_keys(
            work,
            mutation,
            operations=("CREATE", "REBUILD"),
            after=b"",
        )
        == ()
    )
    query, parameters = connector.queries[-1]
    assert "?" not in query and query.count("%s") == len(parameters) == 4
    assert "NOT EXISTS" in query and "EXISTS" in query
    assert "ORDER BY input.publication_key LIMIT 128" in query
    assert "COUNT(" not in query.upper() and "SUM(" not in query.upper()

    assert (
        module._operation_keys(
            work,
            mutation,
            operations=("DELETE",),
            after=b"",
        )
        == ()
    )
    query, parameters = connector.queries[-1]
    assert "?" not in query and query.count("%s") == len(parameters) == 3
    assert "NOT EXISTS" in query and "LIMIT 128" in query

    assert module._gallery_diff_keys(work, mutation, kind="NEW", after=b"") == ()
    query, parameters = connector.queries[-1]
    assert "?" not in query and query.count("%s") == len(parameters) == 3
    assert "ORDER BY current.publication_key LIMIT 128" in query


def test_zero_prepared_operational_binding_keeps_lock_order(tmp_path: Path) -> None:
    connector, gate, turn, _publication_key, now = _database_through_stage_seven(
        tmp_path
    )
    seal = _operational_effect_seal(connector, gate, turn, now=now)
    with connector.transaction():
        ArtifactPreparationRepository.bind_operational_preparation(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            effect_seal=seal,
            now=now + 3,
        )
    assert connector.fetch_one(
        "SELECT preparation_id FROM "
        "operational_publication_candidate_preparations "
        "WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == (seal.preparation_id,)
    connector.close()
