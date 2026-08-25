from __future__ import annotations

from collections.abc import Callable
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
    _PRODUCER_FIELDS,
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
from vnext_analysis_fixtures import (
    seed_content_owner_candidate_shadow,
    seed_content_owner_shadow,
    set_analysis_component_live_count,
)
from vnext_catalog_identity_fixtures import (
    seed_file_name_identity,
    seed_gallery_observation_file,
)

import h2hdb.vnext_artifact_preparation_repository as artifact_module
from h2hdb import vnext_identity as identity
from h2hdb.sql_connector import DatabaseDuplicateKeyError
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_artifact_family import (
    ArtifactSemanticInputFamily,
    PreparedArtifactFamily,
    cas_prepared_artifact_state,
    ensure_artifact_semantic_input_family,
    ensure_prepared_artifact_family,
)
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
    PublicationCandidateNotReadyError,
    PublicationCandidateRepository,
    _MutationAuthority,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


def _set_publication_checkpoint(
    connector: SQLiteConnector,
    *,
    stage: bytes,
    generation: int,
    cursor: bytes,
    processed_count: int,
    state: str,
    updated_at: int,
) -> None:
    for table, column, value in (
        ("catalog_publication_checkpoint_generations", "generation", generation),
        ("catalog_publication_checkpoint_cursors", "cursor", cursor),
        (
            "catalog_publication_checkpoint_processed_counts",
            "processed_count",
            processed_count,
        ),
        ("catalog_publication_checkpoint_states", "state", state),
        ("catalog_publication_checkpoint_updated_ats", "updated_at", updated_at),
    ):
        connector.execute(
            f"UPDATE {table} SET {column} = %s WHERE candidate_id = %s AND stage = %s",
            (value, _CANDIDATE, stage),
        )


def _insert_publication_batch_receipt(
    connector: SQLiteConnector,
    *,
    stage: bytes,
    batch_key: bytes,
    start_generation: int,
    start_cursor: bytes,
    start_processed_count: int,
    next_cursor: bytes,
    row_count: int,
    committed_at: int,
) -> None:
    connector.execute(
        "INSERT INTO catalog_publication_batch_receipt_anchors "
        "(candidate_id, stage, start_generation) VALUES (%s, %s, %s)",
        (_CANDIDATE, stage, start_generation),
    )
    connector.execute(
        "INSERT INTO catalog_publication_batch_receipt_coordinates "
        "(candidate_id, stage, batch_key, start_generation) "
        "VALUES (%s, %s, %s, %s)",
        (_CANDIDATE, stage, batch_key, start_generation),
    )
    for table, column, value in (
        (
            "catalog_publication_batch_receipt_start_cursors",
            "start_cursor",
            start_cursor,
        ),
        (
            "catalog_publication_batch_receipt_start_processed_counts",
            "start_processed_count",
            start_processed_count,
        ),
        (
            "catalog_publication_batch_receipt_next_cursors",
            "next_cursor",
            next_cursor,
        ),
        ("catalog_publication_batch_receipt_row_counts", "row_count", row_count),
        (
            "catalog_publication_batch_receipt_committed_ats",
            "committed_at",
            committed_at,
        ),
    ):
        connector.execute(
            f"INSERT INTO {table} "
            f"(candidate_id, stage, start_generation, {column}) "
            "VALUES (%s, %s, %s, %s)",
            (_CANDIDATE, stage, start_generation, value),
        )
    connector.execute(
        "INSERT INTO catalog_publication_batch_receipt_seals "
        "(candidate_id, stage, start_generation) VALUES (%s, %s, %s)",
        (_CANDIDATE, stage, start_generation),
    )


class _RecordingStorageAdapter:
    adapter_id = b"managed-filesystem"
    producer_fingerprint_sha256 = _PRODUCER_FINGERPRINT

    def __init__(self) -> None:
        self.called = False
        self.archive = b""
        self.protection_tokens: list[bytes] = []

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
        self.protection_tokens.append(protection_token)
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
        (b"galleryinfo.txt", metadata_bytes, metadata_sha256, 0),
        (b"001.bin", image_bytes, image_sha256, 1),
    ):
        file_key = identity.file_key(name)
        seed_file_name_identity(
            connector,
            file_key=file_key,
            name_bytes=name,
            file_role=identity.file_role(name),
        )
        connector.execute(
            "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
            "VALUES (%s, %s)",
            (digest, len(payload)),
        )
        seed_gallery_observation_file(
            connector,
            gallery_id=1,
            observation_id=1,
            file_no=file_no,
            file_key=file_key,
            file_sha256=digest,
        )
        stat_result = (source_directory / name.decode("ascii")).stat()
        filesystem_key = (1, 1, file_key)
        connector.execute(
            "INSERT INTO catalog_gallery_observation_file_filesystem_anchors "
            "(gallery_id, observation_id, file_key) VALUES (%s, %s, %s)",
            filesystem_key,
        )
        for sql, value in (
            (
                "INSERT INTO catalog_gallery_observation_file_filesystem_devices "
                "(gallery_id, observation_id, file_key, device) "
                "VALUES (%s, %s, %s, %s)",
                stat_result.st_dev.to_bytes(8, "big"),
            ),
            (
                "INSERT INTO catalog_gallery_observation_file_filesystem_inodes "
                "(gallery_id, observation_id, file_key, inode) "
                "VALUES (%s, %s, %s, %s)",
                stat_result.st_ino.to_bytes(8, "big"),
            ),
            (
                "INSERT INTO catalog_gallery_observation_file_filesystem_modified_nses "
                "(gallery_id, observation_id, file_key, modified_ns) "
                "VALUES (%s, %s, %s, %s)",
                stat_result.st_mtime_ns.to_bytes(8, "big", signed=True),
            ),
            (
                "INSERT INTO catalog_gallery_observation_file_filesystem_changed_nses "
                "(gallery_id, observation_id, file_key, changed_ns) "
                "VALUES (%s, %s, %s, %s)",
                stat_result.st_ctime_ns.to_bytes(8, "big", signed=True),
            ),
        ):
            connector.execute(
                sql,
                (*filesystem_key, value),
            )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_file_filesystem_seals "
            "(gallery_id, observation_id, file_key) VALUES (%s, %s, %s)",
            filesystem_key,
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
    seed_content_owner_candidate_shadow(
        connector,
        analysis_id=_ANALYSIS,
        gallery_id=1,
        content_sha256=content_sha256,
        prefer_not_already_uploaded=1,
        title_scalar_count=1,
        download_time=1,
    )
    seed_content_owner_shadow(
        connector,
        analysis_id=_ANALYSIS,
        content_sha256=content_sha256,
        owner_gallery_id=1,
    )
    for component in (b"content_owner", b"content_owner_candidate"):
        set_analysis_component_live_count(
            connector,
            analysis_id=_ANALYSIS,
            state_component=component,
            row_count=1,
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
            "UPDATE catalog_build_manifest_file_counts SET file_count = %s "
            "WHERE build_id = %s",
            (2, _BUILD),
        )
        connector.execute(
            "UPDATE catalog_build_manifest_byte_counts SET byte_count = %s "
            "WHERE build_id = %s",
            (byte_count, _BUILD),
        )
        connector.execute(
            "UPDATE catalog_source_snapshot_manifest_identity_file_counts "
            "SET file_count = %s",
            (2,),
        )
        connector.execute(
            "UPDATE catalog_source_snapshot_manifest_identity_byte_counts "
            "SET byte_count = %s",
            (byte_count,),
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
    ensure_artifact_semantic_input_family(
        connector,
        ArtifactSemanticInputFamily(
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
        "(candidate_id, publication_key, artifact_semantics_sha256) "
        "VALUES (%s, %s, %s)",
        (_CANDIDATE, publication_key, semantics),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_operations "
        "(candidate_id, publication_key, operation) "
        "VALUES (%s, %s, %s)",
        (_CANDIDATE, publication_key, "CREATE"),
    )
    _set_publication_checkpoint(
        connector,
        stage=b"BUILD_ARTIFACT_DELTA_OPERATION",
        generation=3,
        cursor=publication_key,
        processed_count=1,
        state="COMPLETE",
        updated_at=106,
    )
    _set_publication_checkpoint(
        connector,
        stage=b"VALIDATE_ARTIFACT_INPUT_DELTA",
        generation=3,
        cursor=publication_key,
        processed_count=1,
        state="COMPLETE",
        updated_at=108,
    )
    _insert_publication_batch_receipt(
        connector,
        stage=b"VALIDATE_ARTIFACT_INPUT_DELTA",
        batch_key=b"validate-artifact-row",
        start_generation=1,
        start_cursor=b"",
        start_processed_count=0,
        next_cursor=publication_key,
        row_count=1,
        committed_at=107,
    )
    _insert_publication_batch_receipt(
        connector,
        stage=b"VALIDATE_ARTIFACT_INPUT_DELTA",
        batch_key=b"validate-artifact-terminal",
        start_generation=2,
        start_cursor=publication_key,
        start_processed_count=1,
        next_cursor=publication_key,
        row_count=0,
        committed_at=108,
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
    seed_file_name_identity(
        connector,
        file_key=metadata_key,
        name_bytes=metadata_name,
        file_role=identity.file_role(metadata_name),
    )
    seed_file_name_identity(
        connector,
        file_key=image_key,
        name_bytes=image_name,
        file_role=identity.file_role(image_name),
    )
    connector.execute(
        "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) VALUES (%s, %s)",
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
        seed_gallery_observation_file(
            connector,
            gallery_id=gallery_id,
            observation_id=1,
            file_no=0,
            file_key=metadata_key,
            file_sha256=metadata_sha256,
        )
        seed_gallery_observation_file(
            connector,
            gallery_id=gallery_id,
            observation_id=1,
            file_no=1,
            file_key=image_key,
            file_sha256=image_sha256,
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
        seed_content_owner_candidate_shadow(
            connector,
            analysis_id=_ANALYSIS,
            gallery_id=gallery_id,
            content_sha256=content_sha256,
            prefer_not_already_uploaded=1,
            title_scalar_count=gallery_id,
            download_time=gallery_id,
        )
        seed_content_owner_shadow(
            connector,
            analysis_id=_ANALYSIS,
            content_sha256=content_sha256,
            owner_gallery_id=gallery_id,
        )
    for component in (b"content_owner", b"content_owner_candidate"):
        set_analysis_component_live_count(
            connector,
            analysis_id=_ANALYSIS,
            state_component=component,
            row_count=count,
        )
    connector.execute(
        "UPDATE catalog_build_manifest_file_counts SET file_count = %s "
        "WHERE build_id = %s",
        (count * 2, _BUILD),
    )
    connector.execute(
        "UPDATE catalog_build_manifest_byte_counts SET byte_count = %s "
        "WHERE build_id = %s",
        (count * 32, _BUILD),
    )
    connector.execute(
        "UPDATE catalog_source_snapshot_manifest_identity_file_counts "
        "SET file_count = %s",
        (count * 2,),
    )
    connector.execute(
        "UPDATE catalog_source_snapshot_manifest_identity_byte_counts "
        "SET byte_count = %s",
        (count * 32,),
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
    _insert_publication_batch_receipt(
        connector,
        stage=stage,
        batch_key=batch_key,
        start_generation=1,
        start_cursor=b"",
        start_processed_count=0,
        next_cursor=b"",
        row_count=0,
        committed_at=now,
    )
    _set_publication_checkpoint(
        connector,
        stage=stage,
        generation=2,
        cursor=b"",
        processed_count=0,
        state="COMPLETE",
        updated_at=now,
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
        if "catalog_gallery_observation_file_seals" in query:
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
    assert connector.fetch_all(
        "SELECT file_no FROM catalog_gallery_observation_file_file_nos "
        "WHERE gallery_id = 1 AND observation_id = 1 ORDER BY file_no"
    ) == [(0,), (1,)]
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
        assert len(receipt.artifact_sha256) == 32
        assert receipt.size_bytes > 0
        assert receipt.locator_components == identity.artifact_locator_components(
            receipt.artifact_sha256
        )
        assert not adapter.called
    assert not adapter.called
    assert not connector.fetch_one("SELECT 1 FROM catalog_artifact_blobs")
    assert not connector.fetch_one("SELECT 1 FROM catalog_prepared_artifacts")
    connector.close()


def test_issue_authority_loads_each_registry_family_exactly_once(
    tmp_path: Path,
) -> None:
    connector, gate, turn, publication_key, _member_plan = (
        _database_with_artifact_input(tmp_path)
    )
    queries: list[str] = []
    original_fetch_one = connector.fetch_one

    def recording_fetch_one(
        query: str,
        data: tuple[object, ...] = (),
    ) -> tuple[object, ...]:
        queries.append(query)
        return original_fetch_one(query, data)

    with (
        patch.object(connector, "fetch_one", side_effect=recording_fetch_one),
        connector.transaction(),
    ):
        ArtifactPreparationRepository.issue_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            publication_key=publication_key,
            now=110,
        )

    registry_seals = (
        "catalog_display_title_policy_seals",
        "catalog_title_sort_policy_seals",
        "catalog_source_scope_seals",
        "catalog_manifest_policy_seals",
        "catalog_analysis_policy_seals",
        "catalog_artifact_policy_semantics_seals",
        "catalog_artifact_producer_fingerprint_seals",
        "catalog_artifact_zip_writer_policy_seals",
        "catalog_artifact_storage_codec_seals",
    )
    assert {
        table: sum(table in query for query in queries) for table in registry_seals
    } == {table: 1 for table in registry_seals}
    assert (
        sum(any(table in query for table in registry_seals) for query in queries) == 9
    )
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

    adapter = MutatingAdapter()
    with ArtifactPreparationRepository.prepare_with_storage_adapter(
        connector,
        backend="sqlite",
        audit=audit,
        adapter=adapter,
    ) as receipt:
        _upload_canonical_plan(connector, gate, turn, receipt.locator_plan, now=now)
        with connector.transaction():
            intent = ArtifactPreparationRepository.persist_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                now=now + 1,
            )
        with (
            patch.object(
                artifact_module,
                "_hash_stream",
                return_value=(b"x" * 32, receipt.size_bytes),
            ),
            pytest.raises(
                ArtifactPreparationConflictError,
                match="archive bytes changed before durable persistence",
            ),
        ):
            ArtifactPreparationRepository.protect_prepared_artifact(
                connector,
                backend="sqlite",
                receipt=receipt,
                intent=intent,
                adapter=adapter,
            )
        assert not adapter.called
        with pytest.raises(
            ArtifactPreparationConflictError,
            match="changed the verified archive bytes",
        ):
            ArtifactPreparationRepository.protect_prepared_artifact(
                connector,
                backend="sqlite",
                receipt=receipt,
                intent=intent,
                adapter=adapter,
            )
        assert adapter.called
    assert connector.fetch_one(
        "SELECT state FROM catalog_prepared_artifacts "
        "WHERE candidate_id = %s AND publication_key = %s",
        (_CANDIDATE, publication_key),
    ) == ("PENDING",)
    assert not connector.fetch_one("SELECT 1 FROM catalog_artifacts")
    connector.close()


@pytest.mark.parametrize("failure_kind", ("reject", "raise"))
def test_storage_failure_leaves_exact_pending_intent_for_retry(
    tmp_path: Path,
    failure_kind: str,
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

    class FailingAdapter(_RecordingStorageAdapter):
        def protect(
            self,
            archive: BinaryIO,
            locator_components: tuple[str, ...],
            protection_token: bytes,
        ) -> ArtifactStorageEvidence:
            del archive, locator_components
            self.called = True
            self.protection_tokens.append(protection_token)
            if failure_kind == "raise":
                raise RuntimeError("simulated storage failure")
            return ArtifactStorageEvidence(False)

    failing_adapter = FailingAdapter()
    with ArtifactPreparationRepository.prepare_with_storage_adapter(
        connector,
        backend="sqlite",
        audit=audit,
        adapter=failing_adapter,
    ) as receipt:
        _upload_canonical_plan(connector, gate, turn, receipt.locator_plan, now=now)
        with connector.transaction():
            intent = ArtifactPreparationRepository.persist_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                now=now + 1,
            )
        assert intent.state == "PENDING"
        assert not intent.replayed
        assert not failing_adapter.called

        with (
            patch.object(connector, "execute", side_effect=AssertionError("write")),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
        ):
            if failure_kind == "raise":
                with pytest.raises(RuntimeError, match="simulated storage failure"):
                    ArtifactPreparationRepository.protect_prepared_artifact(
                        connector,
                        backend="sqlite",
                        receipt=receipt,
                        intent=intent,
                        adapter=failing_adapter,
                    )
            else:
                with pytest.raises(
                    ArtifactPreparationNotReadyError,
                    match="did not acknowledge exact protection",
                ):
                    ArtifactPreparationRepository.protect_prepared_artifact(
                        connector,
                        backend="sqlite",
                        receipt=receipt,
                        intent=intent,
                        adapter=failing_adapter,
                    )

        assert failing_adapter.protection_tokens == [intent.protection_token]
        assert connector.fetch_one(
            "SELECT state FROM catalog_prepared_artifacts "
            "WHERE candidate_id = %s AND publication_key = %s",
            (_CANDIDATE, publication_key),
        ) == ("PENDING",)
        assert not connector.fetch_one("SELECT 1 FROM catalog_artifacts")
        assert not connector.fetch_one(
            "SELECT 1 FROM operational_publication_candidate_preparations"
        )

        with connector.transaction():
            replay = ArtifactPreparationRepository.persist_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                now=now + 2,
            )
        assert replay.state == "PENDING"
        assert replay.replayed
        assert replay.protection_token == intent.protection_token
        assert replay.storage_generation == intent.storage_generation

        retry_adapter = _RecordingStorageAdapter()
        evidence = ArtifactPreparationRepository.protect_prepared_artifact(
            connector,
            backend="sqlite",
            receipt=receipt,
            intent=replay,
            adapter=retry_adapter,
        )
        assert evidence.intent == replay
        assert retry_adapter.protection_tokens == [intent.protection_token]
        assert connector.fetch_one(
            "SELECT state FROM catalog_prepared_artifacts "
            "WHERE candidate_id = %s AND publication_key = %s",
            (_CANDIDATE, publication_key),
        ) == ("PENDING",)
        assert not connector.fetch_one("SELECT 1 FROM catalog_artifacts")
    connector.close()


def test_prepared_artifact_requires_one_complete_atomic_row(tmp_path: Path) -> None:
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
    with ArtifactPreparationRepository.prepare_with_storage_adapter(
        connector,
        backend="sqlite",
        audit=audit,
        adapter=_RecordingStorageAdapter(),
    ) as _receipt:
        with pytest.raises(DatabaseDuplicateKeyError):
            connector.execute(
                "INSERT INTO catalog_prepared_artifacts "
                "(candidate_id, publication_key) VALUES (%s, %s)",
                (_CANDIDATE, publication_key),
            )
        assert not connector.fetch_one("SELECT 1 FROM catalog_prepared_artifacts")
        assert not connector.fetch_one("SELECT 1 FROM catalog_artifact_blobs")
    connector.close()


def test_prepared_and_committed_intents_refuse_protection_and_replay_zero_dml(
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
    adapter = _RecordingStorageAdapter()
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
            now=now + 3,
        )
        with connector.transaction():
            pending = ArtifactPreparationRepository.persist_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                now=now + 4,
            )
        evidence = ArtifactPreparationRepository.protect_prepared_artifact(
            connector,
            backend="sqlite",
            receipt=receipt,
            intent=pending,
            adapter=adapter,
        )
        with connector.transaction():
            ArtifactPreparationRepository.confirm_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                intent=pending,
                evidence=evidence,
                effect_seal=effect_seal,
                now=now + 5,
            )
        with connector.transaction():
            prepared = ArtifactPreparationRepository.persist_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                now=now + 6,
            )
        stale_adapter = _RecordingStorageAdapter()
        with pytest.raises(
            ArtifactPreparationNotReadyError,
            match="only valid for a durable PENDING intent",
        ):
            ArtifactPreparationRepository.protect_prepared_artifact(
                connector,
                backend="sqlite",
                receipt=receipt,
                intent=prepared,
                adapter=stale_adapter,
            )
        assert not stale_adapter.called

        with connector.transaction():
            committed_family = cas_prepared_artifact_state(
                VNextUnitOfWork(connector, backend="sqlite"),
                candidate_id=_CANDIDATE,
                publication_key=publication_key,
                expected_state="PREPARED",
                next_state="COMMITTED",
            )
        assert committed_family.state == "COMMITTED"
        with connector.transaction():
            committed = ArtifactPreparationRepository.persist_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                now=now + 7,
            )
        assert committed.state == "COMMITTED"
        assert committed.replayed
        assert committed.protection_token == pending.protection_token
        assert committed.storage_generation == pending.storage_generation
        with pytest.raises(
            ArtifactPreparationNotReadyError,
            match="only valid for a durable PENDING intent",
        ):
            ArtifactPreparationRepository.protect_prepared_artifact(
                connector,
                backend="sqlite",
                receipt=receipt,
                intent=committed,
                adapter=stale_adapter,
            )
        assert not stale_adapter.called

        with (
            patch.object(connector, "execute", side_effect=AssertionError("write")),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
            connector.transaction(),
        ):
            replayed = ArtifactPreparationRepository.confirm_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                intent=committed,
                evidence=None,
                effect_seal=effect_seal,
                now=now + 8,
            )
        assert replayed.state == "COMMITTED"
        assert replayed.replayed
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
        "UPDATE catalog_publication_checkpoint_states SET state = %s "
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
            intent = ArtifactPreparationRepository.persist_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                now=now,
            )
        assert intent.state == "PENDING"
        assert not intent.replayed
        assert not adapter.called
        ArtifactPreparationRepository.protect_prepared_artifact(
            connector,
            backend="sqlite",
            receipt=receipt,
            intent=intent,
            adapter=adapter,
        )
        assert adapter.protection_tokens == [intent.protection_token]
        now += 1
        with connector.transaction():
            pending_replay = ArtifactPreparationRepository.persist_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                now=now,
            )
        assert pending_replay.state == "PENDING"
        assert pending_replay.replayed
        assert pending_replay.protection_token == intent.protection_token
        assert pending_replay.storage_generation == intent.storage_generation
        assert adapter.protection_tokens == [intent.protection_token]
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
            takeover_replay = ArtifactPreparationRepository.persist_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                now=now,
            )
        assert takeover_replay.state == "PENDING"
        assert takeover_replay.replayed
        assert takeover_replay.protection_token == intent.protection_token
        assert takeover_replay.storage_generation == intent.storage_generation
        evidence = ArtifactPreparationRepository.protect_prepared_artifact(
            connector,
            backend="sqlite",
            receipt=receipt,
            intent=takeover_replay,
            adapter=adapter,
        )
        assert adapter.called
        assert adapter.protection_tokens == [
            intent.protection_token,
            intent.protection_token,
        ]
        now += 1
        with connector.transaction():
            persisted = ArtifactPreparationRepository.confirm_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                intent=takeover_replay,
                evidence=evidence,
                effect_seal=effect_seal,
                now=now,
            )
        assert persisted.state == "PREPARED"
        assert not persisted.replayed
        now += 1
        with connector.transaction():
            prepared_intent = ArtifactPreparationRepository.persist_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                now=now,
            )
        assert prepared_intent.state == "PREPARED"
        assert prepared_intent.replayed
        assert prepared_intent.protection_token == intent.protection_token
        assert prepared_intent.storage_generation == intent.storage_generation
        now += 1
        with (
            patch.object(connector, "execute", side_effect=AssertionError("write")),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
            connector.transaction(),
        ):
            replayed = ArtifactPreparationRepository.confirm_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                intent=prepared_intent,
                evidence=None,
                effect_seal=effect_seal,
                now=now,
            )
        assert replayed.state == "PREPARED"
        assert replayed.replayed
        now += 1

    terminal_certification = None
    terminal_certification_key = b""
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
            attempt = prefix + index.to_bytes(4, "big")
            with connector.transaction():
                batch = validation_method(
                    work=VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    candidate_id=_CANDIDATE,
                    batch_key=attempt,
                    now=now,
                )
            now += 1
            index += 1
            if batch.terminal:
                if prefix == b"loser-":
                    terminal_certification = batch
                    terminal_certification_key = attempt
                break

    assert terminal_certification is not None
    assert terminal_certification_key
    with (
        patch.object(connector, "execute", side_effect=AssertionError("write")),
        patch.object(
            connector,
            "execute_affected",
            side_effect=AssertionError("write"),
        ),
        connector.transaction(),
    ):
        response_loss_replay = (
            ArtifactPreparationRepository.validate_duplicate_loser_batch(
                work=VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                candidate_id=_CANDIDATE,
                batch_key=terminal_certification_key,
                now=now,
            )
        )
    assert response_loss_replay.replayed
    assert response_loss_replay.terminal
    assert response_loss_replay.committed_at == terminal_certification.committed_at
    with (
        patch.object(connector, "execute", side_effect=AssertionError("write")),
        patch.object(
            connector,
            "execute_affected",
            side_effect=AssertionError("write"),
        ),
        pytest.raises(PublicationCandidateNotReadyError, match="OPEN candidate"),
        connector.transaction(),
    ):
        ArtifactPreparationRepository.validate_duplicate_loser_batch(
            work=VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=_CANDIDATE,
            batch_key=b"fresh-after-certification",
            now=now,
        )

    assert connector.fetch_one(
        "SELECT candidate_id FROM catalog_publication_candidate_projection_seals "
        "WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == (_CANDIDATE,)
    assert connector.fetch_one(
        "SELECT create_count, rebuild_count, delete_count, new_galleries, "
        "changed_galleries FROM catalog_publication_candidate_projections "
        "WHERE candidate_id = %s",
        (_CANDIDATE,),
    ) == (1, 0, 0, 1, 0)
    assert not connector.fetch_one(
        "SELECT receipt_id FROM catalog_publication_commit_candidates "
        "WHERE candidate_id = %s",
        (_CANDIDATE,),
    )
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
    adapter = _RecordingStorageAdapter()
    with ArtifactPreparationRepository.prepare_with_storage_adapter(
        connector,
        backend="sqlite",
        audit=audit,
        adapter=adapter,
    ) as receipt:
        immutable_facts = (
            ("artifact_sha256", b"x" * 32),
            ("size_bytes", receipt.size_bytes + 1),
            ("locator_components", ("forged",)),
            ("artifact_locator_sha256", b"y" * 32),
            ("storage_codec_version", receipt.storage_codec_version + 1),
        )
        for field, forged in immutable_facts:
            with pytest.raises(AttributeError):
                setattr(receipt, field, forged)
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
                    "(artifact_sha256, size_bytes, artifact_locator_sha256) "
                    "VALUES (%s, %s, %s)",
                    (
                        receipt.artifact_sha256,
                        receipt.size_bytes + 1,
                        receipt.artifact_locator_sha256,
                    ),
                )
                ArtifactPreparationRepository.persist_prepared_artifact(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    receipt=receipt,
                    now=now,
                )
        assert not connector.fetch_one("SELECT 1 FROM catalog_artifact_blobs")

        insert_faults = (
            "INSERT INTO catalog_artifact_blobs ",
            "INSERT INTO catalog_prepared_artifacts ",
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
                        now=now + index,
                    )
            for table in (
                "catalog_artifact_blobs",
                "catalog_prepared_artifacts",
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
                    now=now,
                )
        assert not connector.fetch_one("SELECT 1 FROM catalog_artifact_blobs")
        assert connector.fetch_one(
            "SELECT generation FROM operational_canonical_value_uploads "
            "WHERE value_sha256 = %s",
            (receipt.artifact_locator_sha256,),
        ) == (turn.generation,)
        with connector.transaction():
            intent = ArtifactPreparationRepository.persist_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                now=now + 1,
            )
        assert intent.state == "PENDING"
        assert not intent.replayed
        assert not adapter.called
        evidence = ArtifactPreparationRepository.protect_prepared_artifact(
            connector,
            backend="sqlite",
            receipt=receipt,
            intent=intent,
            adapter=adapter,
        )

        original_execute_affected = connector.execute_affected

        def fail_state_cas(
            query: str,
            data: tuple[object, ...] = (),
        ) -> int:
            if "UPDATE catalog_prepared_artifacts " in query:
                raise RuntimeError("fault at prepared state CAS")
            return original_execute_affected(query, data)

        with pytest.raises(RuntimeError, match="prepared state CAS"):
            with (
                connector.transaction(),
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=fail_state_cas,
                ),
            ):
                ArtifactPreparationRepository.confirm_prepared_artifact(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    receipt=receipt,
                    intent=intent,
                    evidence=evidence,
                    effect_seal=effect_seal,
                    now=now + 2,
                )

        confirm_faults = (
            "INSERT INTO catalog_artifacts ",
            "INSERT INTO operational_publication_candidate_preparations ",
        )
        original_execute = connector.execute
        for index, fragment in enumerate(confirm_faults):

            def fail_confirm_insert(
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
                        side_effect=fail_confirm_insert,
                    ),
                ):
                    ArtifactPreparationRepository.confirm_prepared_artifact(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        receipt=receipt,
                        intent=intent,
                        evidence=evidence,
                        effect_seal=effect_seal,
                        now=now + 3 + index,
                    )
            assert connector.fetch_one(
                "SELECT state FROM catalog_prepared_artifacts "
                "WHERE candidate_id = %s AND publication_key = %s",
                (_CANDIDATE, publication_key),
            ) == ("PENDING",)
            for table in (
                "catalog_artifacts",
                "operational_publication_candidate_preparations",
            ):
                assert not connector.fetch_one(f"SELECT 1 FROM {table}")

        with connector.transaction():
            persisted = ArtifactPreparationRepository.confirm_prepared_artifact(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                receipt=receipt,
                intent=intent,
                evidence=evidence,
                effect_seal=effect_seal,
                now=now + 3 + len(confirm_faults),
            )
    assert not persisted.replayed
    connector.execute(
        "UPDATE catalog_prepared_artifacts "
        "SET protection_token = %s "
        "WHERE candidate_id = %s AND publication_key = %s",
        (b"x" * 184, _CANDIDATE, publication_key),
    )
    with pytest.raises(
        ArtifactPreparationConflictError,
        match="invalid narrow family",
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
        "SELECT generation FROM catalog_publication_checkpoint_generations "
        "WHERE candidate_id = %s AND stage = %s",
        (_CANDIDATE, b"VALIDATE_PREPARED_ARTIFACT"),
    ) == (1,)
    assert connector.fetch_one(
        "SELECT cursor FROM catalog_publication_checkpoint_cursors "
        "WHERE candidate_id = %s AND stage = %s",
        (_CANDIDATE, b"VALIDATE_PREPARED_ARTIFACT"),
    ) == (b"",)
    assert connector.fetch_one(
        "SELECT processed_count "
        "FROM catalog_publication_checkpoint_processed_counts "
        "WHERE candidate_id = %s AND stage = %s",
        (_CANDIDATE, b"VALIDATE_PREPARED_ARTIFACT"),
    ) == (0,)
    assert connector.fetch_one(
        "SELECT state FROM catalog_publication_checkpoint_states "
        "WHERE candidate_id = %s AND stage = %s",
        (_CANDIDATE, b"VALIDATE_PREPARED_ARTIFACT"),
    ) == ("OPEN",)
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
        patch.object(
            artifact_module,
            "_load_manifest_policy",
            wraps=artifact_module._load_manifest_policy,
        ) as manifest_loader,
        patch.object(
            artifact_module,
            "_load_analysis_policy",
            wraps=artifact_module._load_analysis_policy,
        ) as analysis_loader,
        patch.object(
            artifact_module,
            "_load_source_scope",
            wraps=artifact_module._load_source_scope,
        ) as source_loader,
        patch.object(
            artifact_module,
            "_load_artifact_policy_semantics",
            wraps=artifact_module._load_artifact_policy_semantics,
        ) as semantics_loader,
        patch.object(
            artifact_module,
            "_load_zip_writer_policy",
            wraps=artifact_module._load_zip_writer_policy,
        ) as zip_loader,
        patch.object(
            artifact_module,
            "_load_storage_codec",
            wraps=artifact_module._load_storage_codec,
        ) as storage_loader,
        patch.object(
            artifact_module,
            "_load_producer_fingerprint",
            wraps=artifact_module._load_producer_fingerprint,
        ) as producer_loader,
    ):
        plan = ArtifactPreparationRepository.prepare_artifact_input_projection(
            connector,
            backend="sqlite",
            authority=authority,
        )
        validation = ArtifactPreparationRepository.prepare_artifact_input_validation(
            connector,
            backend="sqlite",
            authority=authority,
        )
    for loader in (
        manifest_loader,
        analysis_loader,
        source_loader,
        semantics_loader,
        zip_loader,
        storage_loader,
        producer_loader,
    ):
        assert loader.call_count == 2
    with plan, validation:
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


def test_seeded_artifact_codecs_use_one_sealed_narrow_query_each(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "artifact-codec-shape.sqlite3")
    try:
        original_fetch_one = connector.fetch_one
        queries: list[str] = []

        def recording_fetch_one(
            query: str,
            data: tuple[object, ...] = (),
        ) -> tuple[object, ...]:
            queries.append(query)
            return original_fetch_one(query, data)

        with patch.object(connector, "fetch_one", side_effect=recording_fetch_one):
            work = VNextUnitOfWork(connector, backend="sqlite")
            assert artifact_module._load_zip_writer_policy(work, 1) == (
                artifact_module._ZIP_WRITER_POLICY_V1
            )
            assert artifact_module._load_storage_codec(work, 1) == (
                artifact_module._STORAGE_CODEC_V1
            )
        assert (
            sum("catalog_artifact_zip_writer_policy_seals" in q for q in queries) == 1
        )
        assert sum("catalog_artifact_storage_codec_seals" in q for q in queries) == 1
        normalized = " ".join(queries)
        assert "catalog_artifact_zip_writer_policies" not in normalized
        assert "catalog_artifact_storage_codecs" not in normalized
        assert "FOR UPDATE" not in normalized.upper()
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("family", "member_table"),
    (
        (
            "zip",
            "catalog_artifact_zip_writer_policy_zip_codec_versions",
        ),
        ("zip", "catalog_artifact_zip_writer_policy_compression_methods"),
        ("zip", "catalog_artifact_zip_writer_policy_compression_levels"),
        ("zip", "catalog_artifact_zip_writer_policy_dos_dates"),
        ("zip", "catalog_artifact_zip_writer_policy_dos_times"),
        ("zip", "catalog_artifact_zip_writer_policy_unix_modes"),
        ("zip", "catalog_artifact_zip_writer_policy_general_purpose_flags"),
        ("zip", "catalog_artifact_zip_writer_policy_create_systems"),
        (
            "zip",
            "catalog_artifact_zip_writer_policy_archive_name_codec_versions",
        ),
        (
            "zip",
            "catalog_artifact_zip_writer_policy_artifact_name_codec_versions",
        ),
        ("zip", "catalog_artifact_zip_writer_policy_identities"),
        ("storage", "catalog_artifact_storage_codec_adapter_ids"),
        ("storage", "catalog_artifact_storage_codec_locator_codec_versions"),
        (
            "storage",
            "catalog_artifact_storage_codec_protection_token_codec_versions",
        ),
    ),
)
def test_artifact_codec_loaders_fail_closed_for_every_missing_member(
    tmp_path: Path,
    family: str,
    member_table: str,
) -> None:
    connector = _generated_database(
        tmp_path / f"artifact-codec-partial-{member_table}.sqlite3"
    )
    try:
        key_column = (
            "artifact_algorithm_version" if family == "zip" else "storage_codec_version"
        )
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            f"DELETE FROM {member_table} WHERE {key_column} = %s",
            (1,),
        )
        connector.execute("PRAGMA foreign_keys = ON")
        loader = (
            artifact_module._load_zip_writer_policy
            if family == "zip"
            else artifact_module._load_storage_codec
        )
        with (
            patch.object(connector, "execute", wraps=connector.execute) as execute,
            pytest.raises(
                ArtifactPreparationNotReadyError, match="missing or incomplete"
            ),
        ):
            loader(VNextUnitOfWork(connector, backend="sqlite"), 1)
        execute.assert_not_called()
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("family", "member_table"),
    (
        (
            "manifest",
            "catalog_manifest_policy_manifest_algorithm_versions",
        ),
        ("manifest", "catalog_manifest_policy_file_order_versions"),
        ("manifest", "catalog_manifest_policy_identities"),
        ("source", "catalog_source_scope_source_providers"),
        ("source", "catalog_source_scope_source_root_sha256s"),
        ("source", "catalog_source_scope_identity_policy_versions"),
        ("source", "catalog_source_scope_identities"),
        (
            "semantics",
            "catalog_artifact_policy_semantics_artifact_algorithm_versions",
        ),
        ("semantics", "catalog_artifact_policy_semantics_max_image_short_sides"),
        (
            "semantics",
            "catalog_artifact_policy_semantics_producer_fingerprint_sha256s",
        ),
        ("semantics", "catalog_artifact_policy_semantics_identities"),
        (
            "producer",
            "catalog_artifact_producer_fingerprint_algorithm_versions",
        ),
        (
            "producer",
            "catalog_artifact_producer_fingerprint_equivalence_classes",
        ),
        ("producer", "catalog_artifact_producer_fingerprint_identities"),
    ),
)
def test_artifact_contract_loaders_fail_closed_for_every_missing_member(
    tmp_path: Path,
    family: str,
    member_table: str,
) -> None:
    connector = _generated_database(
        tmp_path / f"artifact-contract-partial-{member_table}.sqlite3"
    )
    try:
        _gate, turn = _authorities(connector)
        _seed_completed_analysis(connector, turn, with_base=False)
        loader: Callable[[VNextUnitOfWork, object], object]
        if family == "manifest":
            key_column = "manifest_policy_id"
            key: object = 1
            loader = cast(
                Callable[[VNextUnitOfWork, object], object],
                artifact_module._load_manifest_policy,
            )
        elif family == "source":
            key_column = "scope_key"
            key = connector.fetch_one(
                "SELECT scope_key FROM catalog_source_scope_seals"
            )[0]
            loader = cast(
                Callable[[VNextUnitOfWork, object], object],
                artifact_module._load_source_scope,
            )
        elif family == "semantics":
            key_column = "policy_component_sha256"
            key = connector.fetch_one(
                "SELECT policy_component_sha256 "
                "FROM catalog_artifact_policy_semantics_seals"
            )[0]
            loader = cast(
                Callable[[VNextUnitOfWork, object], object],
                artifact_module._load_artifact_policy_semantics,
            )
        else:
            key_column = "producer_fingerprint_sha256"
            key = _PRODUCER_FINGERPRINT
            loader = cast(
                Callable[[VNextUnitOfWork, object], object],
                artifact_module._load_producer_fingerprint,
            )
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            f"DELETE FROM {member_table} WHERE {key_column} = %s",
            (key,),
        )
        connector.execute("PRAGMA foreign_keys = ON")
        with (
            patch.object(connector, "execute", wraps=connector.execute) as execute,
            pytest.raises(
                ArtifactPreparationNotReadyError, match="missing or incomplete"
            ),
        ):
            loader(VNextUnitOfWork(connector, backend="sqlite"), key)
        execute.assert_not_called()
    finally:
        connector.close()


def test_mariadb_prepared_family_duplicate_recovery_uses_locking_narrow_sql() -> None:
    candidate = b"c" * 16
    publication = b"p" * 32
    artifact = b"a" * 32
    locator = b"l" * 32
    generation = 7
    size_bytes = 99
    token = identity.encode_artifact_protection_token(
        1,
        candidate,
        publication,
        artifact,
        locator,
        generation,
        size_bytes,
    )
    family = PreparedArtifactFamily(
        candidate,
        publication,
        artifact,
        1,
        generation,
        token,
        "PENDING",
    )
    raced_row = (
        candidate,
        publication,
        artifact,
        1,
        generation,
        token,
        "PENDING",
        size_bytes,
        locator,
    )

    class DuplicateRaceConnector:
        def __init__(self) -> None:
            self.queries: list[str] = []
            self.mutations: list[str] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[object, ...] = (),
        ) -> tuple[object, ...]:
            del data
            self.queries.append(query)
            if "FROM catalog_prepared_artifacts AS prepared" in query and (
                "FOR UPDATE" in query
            ):
                return raced_row
            return ()

        def execute(
            self,
            query: str,
            data: tuple[object, ...] = (),
        ) -> None:
            del data
            self.mutations.append(query)
            raise DatabaseDuplicateKeyError("simulated concurrent anchor winner")

    connector = DuplicateRaceConnector()
    loaded, created = ensure_prepared_artifact_family(
        connector,
        family,
        backend="mariadb",
    )
    assert loaded == family
    assert not created
    assert len(connector.mutations) == 1
    assert "catalog_prepared_artifacts" in connector.mutations[0]
    assert any("FOR UPDATE" in query for query in connector.queries)
    assert all("%s" in query and "?" not in query for query in connector.queries)
    normalized = " ".join(connector.queries).lower()
    assert " as natural" not in normalized
    assert " as keys" not in normalized
    assert "catalog_prepared_artifact_anchors" not in normalized


def test_mariadb_artifact_contract_loaders_use_plain_static_narrow_sql() -> None:
    root = b"r" * 32
    scope = identity.source_scope_key("filesystem", root, 1)
    producer = _PRODUCER_FINGERPRINT
    policy = identity.artifact_policy_digest(1, 2048, producer)

    class RecordingConnector:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def fetch_one(
            self,
            query: str,
            data: tuple[object, ...] = (),
        ) -> tuple[object, ...]:
            del data
            self.queries.append(query)
            if "catalog_manifest_policy_seals" in query:
                return (1, 1)
            if "catalog_analysis_policy_seals" in query:
                return (1, 1, 3, 1, 1)
            if "catalog_source_scope_seals" in query:
                return (b"filesystem", root, 1)
            if "catalog_artifact_policy_semantics_seals" in query:
                return (1, 2048, producer)
            if "catalog_artifact_zip_writer_policy_seals" in query:
                return artifact_module._ZIP_WRITER_POLICY_V1
            if "catalog_artifact_storage_codec_seals" in query:
                return artifact_module._STORAGE_CODEC_V1[1:]
            if "catalog_artifact_producer_fingerprint_seals" in query:
                return (
                    *_PRODUCER_FIELDS,
                    1,
                    identity.artifact_producer_equivalence_class(producer),
                )
            return ()

    connector = RecordingConnector()
    work = VNextUnitOfWork(cast(SQLiteConnector, connector), backend="mariadb")
    assert artifact_module._load_manifest_policy(work, 1) == (1, 1)
    assert artifact_module._load_analysis_policy(work, 1) == (1, 1, 3, 1, 1)
    assert artifact_module._load_source_scope(work, scope) == (b"filesystem", root, 1)
    assert artifact_module._load_artifact_policy_semantics(work, policy) == (
        1,
        2048,
        producer,
    )
    assert artifact_module._load_zip_writer_policy(work, 1) == (
        artifact_module._ZIP_WRITER_POLICY_V1
    )
    assert artifact_module._load_storage_codec(work, 1) == (
        artifact_module._STORAGE_CODEC_V1
    )
    assert artifact_module._load_producer_fingerprint(work, producer)[0] == (
        _PRODUCER_FIELDS
    )
    normalized = " ".join(connector.queries)
    assert all("?" not in query for query in connector.queries)
    assert "FOR UPDATE" not in normalized.upper()
    for old_view in (
        "catalog_manifest_policies ",
        "catalog_analysis_policies ",
        "catalog_source_scopes ",
        "catalog_artifact_policy_semantics ",
        "catalog_artifact_zip_writer_policies ",
        "catalog_artifact_storage_codecs ",
        "catalog_artifact_producer_fingerprints ",
    ):
        assert old_view not in normalized


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
            candidate=SimpleNamespace(
                candidate_id=_CANDIDATE,
                reserved_revision=8,
                display_title_policy_id=1,
            ),
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

    before = len(connector.queries)
    assert module._gallery_diff_keys(work, mutation, kind="CHANGED", after=b"") == ()
    assert len(connector.queries) == before + 1
    query, parameters = connector.queries[-1]
    normalized = query.upper()
    assert "?" not in query and query.count("%s") == len(parameters) == 4
    assert parameters == (1, 7, 8, b"")
    assert "ORDER BY CURRENT_ITEM.PUBLICATION_KEY" in normalized
    assert "LIMIT 128" in normalized
    assert "EXISTS" in normalized and "NOT EXISTS" in normalized
    assert "EXCEPT" not in normalized and "IS DISTINCT" not in normalized
    assert "ITEM_SHA256" not in normalized and "SORT_AS" not in normalized
    assert "CATALOG_PUBLICATION_ORDER" not in normalized
    assert "CATALOG_ARTIFACT_" not in normalized


def _seed_exact_delta_scalar(
    connector: SQLiteConnector,
    *,
    revision: int,
    publication_key: bytes,
    gallery_id: int,
    summary_sha256: bytes,
    language_sha256: bytes,
    modified_at: int,
) -> None:
    connector.execute(
        "INSERT INTO catalog_publications "
        "(revision, publication_key, gallery_id, summary_sha256, "
        "language_sha256, modified_at) VALUES (%s, %s, %s, %s, %s, %s)",
        (
            revision,
            publication_key,
            gallery_id,
            summary_sha256,
            language_sha256,
            modified_at,
        ),
    )


def _seed_exact_delta_title(
    connector: SQLiteConnector,
    *,
    revision: int,
    publication_key: bytes,
    source_title_sha256: bytes,
    source_gallery_name: bytes,
) -> None:
    connector.execute(
        "INSERT INTO catalog_publication_titles "
        "(revision, publication_key, source_title_sha256, source_gallery_name) "
        "VALUES (%s, %s, %s, %s)",
        (
            revision,
            publication_key,
            source_title_sha256,
            source_gallery_name,
        ),
    )


def _seed_exact_delta_contributor(
    connector: SQLiteConnector,
    *,
    revision: int,
    publication_key: bytes,
    position: int,
    contributor_name_sha256: bytes,
    role: bytes,
) -> None:
    key = (revision, publication_key, position)
    connector.execute(
        "INSERT INTO catalog_contributor_anchors "
        "(revision, publication_key, position) VALUES (%s, %s, %s)",
        key,
    )
    connector.execute(
        "INSERT INTO catalog_contributor_name_sha256s "
        "(revision, publication_key, position, contributor_name_sha256) "
        "VALUES (%s, %s, %s, %s)",
        (*key, contributor_name_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_contributor_roles "
        "(revision, publication_key, position, role) VALUES (%s, %s, %s, %s)",
        (*key, role),
    )
    connector.execute(
        "INSERT INTO catalog_contributor_identities "
        "(revision, publication_key, contributor_name_sha256, role, position) "
        "VALUES (%s, %s, %s, %s, %s)",
        (revision, publication_key, contributor_name_sha256, role, position),
    )
    connector.execute(
        "INSERT INTO catalog_contributor_seals "
        "(revision, publication_key, position) VALUES (%s, %s, %s)",
        key,
    )


def _exact_delta_database(path: Path) -> tuple[SQLiteConnector, bytes]:
    connector = SQLiteConnector(str(path))
    connector.connect()
    for statement in (
        "CREATE TABLE catalog_publications ("
        "revision INTEGER NOT NULL, publication_key BLOB NOT NULL, "
        "gallery_id INTEGER NOT NULL, summary_sha256 BLOB NOT NULL, "
        "language_sha256 BLOB NOT NULL, modified_at INTEGER NOT NULL, "
        "PRIMARY KEY (revision, publication_key), "
        "UNIQUE (revision, gallery_id))",
        "CREATE TABLE catalog_publication_titles ("
        "revision INTEGER NOT NULL, publication_key BLOB NOT NULL, "
        "source_title_sha256 BLOB NOT NULL, source_gallery_name BLOB NOT NULL, "
        "PRIMARY KEY (revision, publication_key))",
        "CREATE TABLE catalog_display_title_choices ("
        "display_title_policy_id INTEGER NOT NULL, "
        "source_title_sha256 BLOB NOT NULL, source_gallery_name BLOB NOT NULL, "
        "title_sha256 BLOB NOT NULL, PRIMARY KEY (display_title_policy_id, "
        "source_title_sha256, source_gallery_name))",
        "CREATE TABLE catalog_display_title_policy_title_sort_policy_ids ("
        "display_title_policy_id INTEGER PRIMARY KEY, "
        "title_sort_policy_id INTEGER NOT NULL)",
        "CREATE TABLE catalog_title_sorts ("
        "title_sort_policy_id INTEGER NOT NULL, title_sha256 BLOB NOT NULL, "
        "sort_title_sha256 BLOB NOT NULL, "
        "PRIMARY KEY (title_sort_policy_id, title_sha256))",
        "CREATE TABLE catalog_publication_commit_catalog_revisions ("
        "receipt_id BLOB NOT NULL UNIQUE, revision INTEGER PRIMARY KEY)",
        "CREATE TABLE catalog_publication_commit_seals (receipt_id BLOB PRIMARY KEY)",
        "CREATE TABLE catalog_publication_commit_display_title_policies ("
        "receipt_id BLOB PRIMARY KEY, display_title_policy_id INTEGER NOT NULL)",
        "CREATE TABLE catalog_publication_contents ("
        "revision INTEGER NOT NULL, publication_key BLOB NOT NULL, "
        "content_sha256 BLOB NOT NULL, PRIMARY KEY (revision, publication_key))",
        "CREATE TABLE catalog_contributor_anchors ("
        "revision INTEGER NOT NULL, publication_key BLOB NOT NULL, "
        "position INTEGER NOT NULL, "
        "PRIMARY KEY (revision, publication_key, position))",
        "CREATE TABLE catalog_contributor_name_sha256s ("
        "revision INTEGER NOT NULL, publication_key BLOB NOT NULL, "
        "position INTEGER NOT NULL, contributor_name_sha256 BLOB NOT NULL, "
        "PRIMARY KEY (revision, publication_key, position))",
        "CREATE TABLE catalog_contributor_roles ("
        "revision INTEGER NOT NULL, publication_key BLOB NOT NULL, "
        "position INTEGER NOT NULL, role BLOB NOT NULL, "
        "PRIMARY KEY (revision, publication_key, position))",
        "CREATE TABLE catalog_contributor_identities ("
        "revision INTEGER NOT NULL, publication_key BLOB NOT NULL, "
        "contributor_name_sha256 BLOB NOT NULL, role BLOB NOT NULL, "
        "position INTEGER NOT NULL, PRIMARY KEY (revision, publication_key, "
        "contributor_name_sha256, role), "
        "UNIQUE (revision, publication_key, position))",
        "CREATE TABLE catalog_contributor_seals ("
        "revision INTEGER NOT NULL, publication_key BLOB NOT NULL, "
        "position INTEGER NOT NULL, "
        "PRIMARY KEY (revision, publication_key, position))",
        "CREATE VIEW catalog_contributors AS SELECT seal.revision, "
        "seal.publication_key, seal.position, name.contributor_name_sha256, role.role "
        "FROM catalog_contributor_seals AS seal "
        "JOIN catalog_contributor_anchors AS anchor "
        "USING (revision, publication_key, position) "
        "JOIN catalog_contributor_name_sha256s AS name "
        "USING (revision, publication_key, position) "
        "JOIN catalog_contributor_roles AS role "
        "USING (revision, publication_key, position) "
        "JOIN catalog_contributor_identities AS identity_row "
        "ON identity_row.revision = seal.revision "
        "AND identity_row.publication_key = seal.publication_key "
        "AND identity_row.position = seal.position "
        "AND identity_row.contributor_name_sha256 = name.contributor_name_sha256 "
        "AND identity_row.role = role.role",
        "CREATE TABLE catalog_subjects ("
        "revision INTEGER NOT NULL, publication_key BLOB NOT NULL, "
        "position INTEGER NOT NULL, tag_id INTEGER NOT NULL, "
        "PRIMARY KEY (revision, publication_key, position), "
        "UNIQUE (revision, publication_key, tag_id))",
        "CREATE TABLE catalog_publication_order ("
        "revision INTEGER NOT NULL, position INTEGER NOT NULL, "
        "publication_key BLOB NOT NULL, PRIMARY KEY (revision, position), "
        "UNIQUE (revision, publication_key))",
        "CREATE TABLE catalog_artifacts ("
        "revision INTEGER NOT NULL, publication_key BLOB NOT NULL, "
        "artifact_sha256 BLOB NOT NULL, artifact_semantics_sha256 BLOB NOT NULL, "
        "PRIMARY KEY (revision, publication_key))",
    ):
        connector.execute(statement)

    publication_key = sha256(b"exact-item-delta-publication").digest()
    source_title = sha256(b"exact-item-delta-source-title").digest()
    title = sha256(b"exact-item-delta-title").digest()
    sort_title = sha256(b"exact-item-delta-sort-title").digest()
    receipt_id = b"o" * 16
    connector.execute(
        "INSERT INTO catalog_publication_commit_catalog_revisions "
        "(receipt_id, revision) VALUES (%s, %s)",
        (receipt_id, 1),
    )
    connector.execute(
        "INSERT INTO catalog_publication_commit_seals (receipt_id) VALUES (%s)",
        (receipt_id,),
    )
    connector.execute(
        "INSERT INTO catalog_publication_commit_display_title_policies "
        "(receipt_id, display_title_policy_id) VALUES (%s, %s)",
        (receipt_id, 1),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policy_title_sort_policy_ids "
        "(display_title_policy_id, title_sort_policy_id) VALUES (%s, %s)",
        (1, 1),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_choices "
        "(display_title_policy_id, source_title_sha256, source_gallery_name, "
        "title_sha256) VALUES (%s, %s, %s, %s)",
        (1, source_title, b"gallery", title),
    )
    connector.execute(
        "INSERT INTO catalog_title_sorts "
        "(title_sort_policy_id, title_sha256, sort_title_sha256) "
        "VALUES (%s, %s, %s)",
        (1, title, sort_title),
    )
    for revision in (1, 2):
        _seed_exact_delta_scalar(
            connector,
            revision=revision,
            publication_key=publication_key,
            gallery_id=10,
            summary_sha256=sha256(b"summary").digest(),
            language_sha256=sha256(b"language").digest(),
            modified_at=20,
        )
        _seed_exact_delta_title(
            connector,
            revision=revision,
            publication_key=publication_key,
            source_title_sha256=source_title,
            source_gallery_name=b"gallery",
        )
        connector.execute(
            "INSERT INTO catalog_publication_contents "
            "(revision, publication_key, content_sha256) VALUES (%s, %s, %s)",
            (revision, publication_key, sha256(b"content").digest()),
        )
        _seed_exact_delta_contributor(
            connector,
            revision=revision,
            publication_key=publication_key,
            position=0,
            contributor_name_sha256=sha256(b"artist").digest(),
            role=b"artist",
        )
        connector.execute(
            "INSERT INTO catalog_subjects "
            "(revision, publication_key, position, tag_id) VALUES (%s, %s, %s, %s)",
            (revision, publication_key, 0, 100),
        )
        connector.execute(
            "INSERT INTO catalog_publication_order "
            "(revision, position, publication_key) VALUES (%s, %s, %s)",
            (revision, 0, publication_key),
        )
        connector.execute(
            "INSERT INTO catalog_artifacts "
            "(revision, publication_key, artifact_sha256, "
            "artifact_semantics_sha256) VALUES (%s, %s, %s, %s)",
            (
                revision,
                publication_key,
                sha256(b"artifact").digest(),
                sha256(b"semantics").digest(),
            ),
        )
    return connector, publication_key


def _mutate_exact_delta_case(
    connector: SQLiteConnector,
    publication_key: bytes,
    case: str,
) -> int:
    if case == "unchanged":
        return 1
    if case in {"gallery-id", "modified-at"}:
        column = {
            "gallery-id": "gallery_id",
            "modified-at": "modified_at",
        }[case]
        connector.execute(
            f"UPDATE catalog_publications SET {column} = %s "
            "WHERE revision = %s AND publication_key = %s",
            (99, 2, publication_key),
        )
        return 1
    if case in {"summary", "language"}:
        column = f"{case}_sha256"
        connector.execute(
            f"UPDATE catalog_publications SET {column} = %s "
            "WHERE revision = %s AND publication_key = %s",
            (sha256(b"changed-" + case.encode()).digest(), 2, publication_key),
        )
        return 1
    if case == "title-current-missing":
        connector.execute(
            "DELETE FROM catalog_publication_titles "
            "WHERE revision = %s AND publication_key = %s",
            (2, publication_key),
        )
        return 1
    if case == "title-old-missing":
        connector.execute(
            "DELETE FROM catalog_publication_titles "
            "WHERE revision = %s AND publication_key = %s",
            (1, publication_key),
        )
        return 1
    if case in {"source-title", "source-gallery-name"}:
        current = connector.fetch_one(
            "SELECT source_title_sha256, source_gallery_name "
            "FROM catalog_publication_titles WHERE revision = %s "
            "AND publication_key = %s",
            (2, publication_key),
        )
        source_title, source_gallery_name = current
        if case == "source-title":
            source_title = sha256(b"changed-source-title").digest()
        else:
            source_gallery_name = b"changed-gallery"
        title = sha256(b"exact-item-delta-title").digest()
        connector.execute(
            "INSERT INTO catalog_display_title_choices "
            "(display_title_policy_id, source_title_sha256, source_gallery_name, "
            "title_sha256) VALUES (%s, %s, %s, %s)",
            (1, source_title, source_gallery_name, title),
        )
        connector.execute(
            "UPDATE catalog_publication_titles SET source_title_sha256 = %s, "
            "source_gallery_name = %s WHERE revision = %s "
            "AND publication_key = %s",
            (source_title, source_gallery_name, 2, publication_key),
        )
        return 1
    if case in {"display-title", "sort-title"}:
        source_title = sha256(b"exact-item-delta-source-title").digest()
        title = sha256(b"exact-item-delta-title").digest()
        sort_title = sha256(b"changed-sort-title").digest()
        sort_policy = 1 if case == "display-title" else 2
        current_title = sha256(b"changed-display-title").digest()
        if case == "sort-title":
            current_title = title
        connector.execute(
            "INSERT INTO catalog_display_title_policy_title_sort_policy_ids "
            "(display_title_policy_id, title_sort_policy_id) VALUES (%s, %s)",
            (2, sort_policy),
        )
        connector.execute(
            "INSERT INTO catalog_display_title_choices "
            "(display_title_policy_id, source_title_sha256, source_gallery_name, "
            "title_sha256) VALUES (%s, %s, %s, %s)",
            (2, source_title, b"gallery", current_title),
        )
        connector.execute(
            "INSERT INTO catalog_title_sorts "
            "(title_sort_policy_id, title_sha256, sort_title_sha256) "
            "VALUES (%s, %s, %s)",
            (sort_policy, current_title, sort_title),
        )
        return 2
    if case == "content-absent-both":
        connector.execute(
            "DELETE FROM catalog_publication_contents WHERE publication_key = %s",
            (publication_key,),
        )
        return 1
    if case in {"content-current-missing", "content-old-missing"}:
        revision = 2 if case == "content-current-missing" else 1
        connector.execute(
            "DELETE FROM catalog_publication_contents "
            "WHERE revision = %s AND publication_key = %s",
            (revision, publication_key),
        )
        return 1
    if case == "content-value":
        connector.execute(
            "UPDATE catalog_publication_contents SET content_sha256 = %s "
            "WHERE revision = %s AND publication_key = %s",
            (sha256(b"changed-content").digest(), 2, publication_key),
        )
        return 1
    if case in {"contributor-current-missing", "contributor-old-missing"}:
        revision = 2 if case == "contributor-current-missing" else 1
        connector.execute(
            "DELETE FROM catalog_contributor_seals "
            "WHERE revision = %s AND publication_key = %s",
            (revision, publication_key),
        )
        return 1
    if case == "contributor-position":
        for table in (
            "catalog_contributor_seals",
            "catalog_contributor_identities",
            "catalog_contributor_name_sha256s",
            "catalog_contributor_roles",
            "catalog_contributor_anchors",
        ):
            connector.execute(
                f"DELETE FROM {table} WHERE revision = %s "
                "AND publication_key = %s AND position = %s",
                (2, publication_key, 0),
            )
        _seed_exact_delta_contributor(
            connector,
            revision=2,
            publication_key=publication_key,
            position=1,
            contributor_name_sha256=sha256(b"artist").digest(),
            role=b"artist",
        )
        return 1
    if case in {"contributor-name", "contributor-role"}:
        name = sha256(b"artist").digest()
        role = b"artist"
        if case == "contributor-name":
            name = sha256(b"changed-contributor").digest()
        else:
            role = b"author"
        connector.execute(
            "UPDATE catalog_contributor_name_sha256s "
            "SET contributor_name_sha256 = %s WHERE revision = %s "
            "AND publication_key = %s AND position = %s",
            (name, 2, publication_key, 0),
        )
        connector.execute(
            "UPDATE catalog_contributor_roles SET role = %s WHERE revision = %s "
            "AND publication_key = %s AND position = %s",
            (role, 2, publication_key, 0),
        )
        connector.execute(
            "UPDATE catalog_contributor_identities "
            "SET contributor_name_sha256 = %s, role = %s "
            "WHERE revision = %s AND publication_key = %s AND position = %s",
            (name, role, 2, publication_key, 0),
        )
        return 1
    if case in {"subject-current-missing", "subject-old-missing"}:
        revision = 2 if case == "subject-current-missing" else 1
        connector.execute(
            "DELETE FROM catalog_subjects "
            "WHERE revision = %s AND publication_key = %s",
            (revision, publication_key),
        )
        return 1
    if case in {"subject-position", "subject-tag"}:
        column, value = (
            ("position", 1) if case == "subject-position" else ("tag_id", 101)
        )
        connector.execute(
            f"UPDATE catalog_subjects SET {column} = %s "
            "WHERE revision = %s AND publication_key = %s",
            (value, 2, publication_key),
        )
        return 1
    if case == "order-only":
        second_publication = sha256(b"exact-item-delta-second-publication").digest()
        for revision in (1, 2):
            _seed_exact_delta_scalar(
                connector,
                revision=revision,
                publication_key=second_publication,
                gallery_id=11,
                summary_sha256=sha256(b"second-summary").digest(),
                language_sha256=sha256(b"language").digest(),
                modified_at=20,
            )
            connector.execute(
                "INSERT INTO catalog_publication_order "
                "(revision, position, publication_key) VALUES (%s, %s, %s)",
                (revision, 1, second_publication),
            )
        connector.execute(
            "DELETE FROM catalog_publication_order WHERE revision = %s",
            (2,),
        )
        connector.execute(
            "INSERT INTO catalog_publication_order "
            "(revision, position, publication_key) VALUES (%s, %s, %s)",
            (2, 0, second_publication),
        )
        connector.execute(
            "INSERT INTO catalog_publication_order "
            "(revision, position, publication_key) VALUES (%s, %s, %s)",
            (2, 1, publication_key),
        )
        return 1
    if case == "artifact-only":
        connector.execute(
            "UPDATE catalog_artifacts SET artifact_sha256 = %s "
            "WHERE revision = %s AND publication_key = %s",
            (sha256(b"changed-artifact").digest(), 2, publication_key),
        )
        return 1
    raise AssertionError(f"unknown exact item-delta case: {case}")


@pytest.mark.parametrize(
    ("case", "changed"),
    (
        ("unchanged", False),
        ("gallery-id", True),
        ("summary", True),
        ("language", True),
        ("modified-at", True),
        ("title-current-missing", True),
        ("title-old-missing", True),
        ("source-title", True),
        ("source-gallery-name", True),
        ("display-title", True),
        ("sort-title", True),
        ("content-absent-both", False),
        ("content-current-missing", True),
        ("content-old-missing", True),
        ("content-value", True),
        ("contributor-current-missing", True),
        ("contributor-old-missing", True),
        ("contributor-position", True),
        ("contributor-name", True),
        ("contributor-role", True),
        ("subject-current-missing", True),
        ("subject-old-missing", True),
        ("subject-position", True),
        ("subject-tag", True),
        ("order-only", False),
        ("artifact-only", False),
    ),
)
def test_exact_changed_item_bidirectional_family_matrix(
    tmp_path: Path,
    case: str,
    changed: bool,
) -> None:
    from types import SimpleNamespace

    connector, publication_key = _exact_delta_database(
        tmp_path / f"exact-delta-{case}.sqlite3"
    )
    display_title_policy_id = _mutate_exact_delta_case(
        connector,
        publication_key,
        case,
    )
    mutation = cast(
        _MutationAuthority,
        SimpleNamespace(
            candidate=SimpleNamespace(
                candidate_id=_CANDIDATE,
                reserved_revision=2,
                display_title_policy_id=display_title_policy_id,
            ),
            base_catalog=SimpleNamespace(revision=1),
        ),
    )
    rows = artifact_module._gallery_diff_keys(
        VNextUnitOfWork(connector, backend="sqlite"),
        mutation,
        kind="CHANGED",
        after=b"",
    )
    assert rows == ((publication_key,) if changed else ())
    connector.close()


def test_exact_changed_item_keyset_returns_128_then_one(tmp_path: Path) -> None:
    from types import SimpleNamespace

    connector, baseline_key = _exact_delta_database(
        tmp_path / "exact-delta-129.sqlite3"
    )
    connector.execute(
        "UPDATE catalog_publications SET summary_sha256 = %s "
        "WHERE revision = %s AND publication_key = %s",
        (sha256(b"changed-baseline").digest(), 2, baseline_key),
    )
    keys = [baseline_key]
    for index in range(128):
        publication_key = sha256(
            b"exact-delta-129\0" + index.to_bytes(4, "big")
        ).digest()
        keys.append(publication_key)
        for revision, summary in (
            (1, sha256(b"old-summary").digest()),
            (2, sha256(b"new-summary").digest()),
        ):
            _seed_exact_delta_scalar(
                connector,
                revision=revision,
                publication_key=publication_key,
                gallery_id=index + 100,
                summary_sha256=summary,
                language_sha256=sha256(b"language").digest(),
                modified_at=20,
            )
    expected = tuple(sorted(keys))
    assert len(set(expected)) == 129
    mutation = cast(
        _MutationAuthority,
        SimpleNamespace(
            candidate=SimpleNamespace(
                candidate_id=_CANDIDATE,
                reserved_revision=2,
                display_title_policy_id=1,
            ),
            base_catalog=SimpleNamespace(revision=1),
        ),
    )
    work = VNextUnitOfWork(connector, backend="sqlite")
    first = artifact_module._gallery_diff_keys(
        work,
        mutation,
        kind="CHANGED",
        after=b"",
    )
    second = artifact_module._gallery_diff_keys(
        work,
        mutation,
        kind="CHANGED",
        after=first[-1],
    )
    assert first == expected[:128]
    assert second == expected[128:]
    assert (
        artifact_module._gallery_diff_keys(
            work,
            mutation,
            kind="CHANGED",
            after=second[-1],
        )
        == ()
    )
    connector.close()


def test_sqlite_exact_changed_item_plan_uses_leading_key_searches(
    tmp_path: Path,
) -> None:
    connector, _publication_key = _exact_delta_database(
        tmp_path / "exact-delta-plan.sqlite3"
    )
    plan = connector.fetch_all(
        "EXPLAIN QUERY PLAN " + artifact_module._EXACT_CHANGED_ITEM_QUERY,
        (1, 1, 2, b""),
    )
    details = tuple(str(row[3]).upper() for row in plan)
    assert any(
        "CATALOG_PUBLICATIONS" in detail
        and "REVISION=? AND PUBLICATION_KEY>?" in detail
        for detail in details
    )
    for leading_index_family in (
        "CATALOG_PUBLICATION_CONTENTS",
        "CATALOG_CONTRIBUTOR_SEALS",
        "CATALOG_SUBJECTS",
    ):
        assert any(
            "SEARCH" in detail
            and leading_index_family in detail
            and "REVISION=? AND PUBLICATION_KEY=?" in detail
            for detail in details
        )
    for alias in (
        "CURRENT_ITEM",
        "OLD_ITEM",
        "CURRENT_TITLE",
        "OLD_TITLE",
        "CURRENT_CONTENT",
        "OLD_CONTENT",
        "CURRENT_CONTRIBUTOR",
        "OLD_CONTRIBUTOR",
        "CURRENT_SUBJECT",
        "OLD_SUBJECT",
    ):
        assert not any(f"SCAN {alias}" in detail for detail in details)
    connector.close()


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
