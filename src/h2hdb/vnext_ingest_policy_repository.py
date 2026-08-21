"""Idempotent natural-fact policy registration for public vNext ingest."""

from __future__ import annotations

__all__ = [
    "VNextIngestPolicyConflictError",
    "VNextIngestPolicyNotReadyError",
    "VNextIngestPolicyRepository",
]

from . import vnext_identity as identity
from .domain import VNextIngestPolicy, VNextResolvedIngestPolicy
from .vnext_allocator_repository import (
    IdentityStream,
    VNextAllocatorRepository,
)
from .vnext_artifact_preparation_repository import ArtifactPreparationRepository
from .vnext_canonical_value_family import persist_in_memory_canonical_value
from .vnext_catalog_registry_repository import (
    AnalysisPolicyRecord,
    ArtifactPolicySemanticsRecord,
    ArtifactStorageCodecRecord,
    ArtifactZipWriterPolicyRecord,
    DisplayTitlePolicyRecord,
    ManifestPolicyRecord,
    TitleSortPolicyRecord,
    load_analysis_policy,
    load_artifact_policy_semantics,
    load_artifact_storage_codec,
    load_artifact_zip_writer_policy,
    load_display_title_policy,
    load_manifest_policy,
    load_title_sort_policy,
)
from .vnext_domains import (
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uint32,
)
from .vnext_ingest_fence_repository import IngestTurn
from .vnext_maintenance_gate_repository import GateLease
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_POLICY_LOCK_KEY = encode_lock_key("ingest-policy-registry")


class VNextIngestPolicyConflictError(RuntimeError):
    """Stored policy facts disagree with their immutable natural identity."""


class VNextIngestPolicyNotReadyError(RuntimeError):
    """A generated bootstrap policy required by ingest is unavailable."""


class VNextIngestPolicyRepository:
    """Resolve or seal every policy family needed by one ingest run."""

    @staticmethod
    def ensure(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        policy: VNextIngestPolicy,
        now: int,
    ) -> VNextResolvedIngestPolicy:
        if not isinstance(policy, VNextIngestPolicy):
            raise TypeError("policy must be VNextIngestPolicy")
        policy.__post_init__()
        timestamp = require_int63(now, field="ingest policy registration now")
        _require_bootstrap_policies(work, policy)

        producer = ArtifactPreparationRepository.register_producer(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            now=timestamp,
            artifact_algorithm_version=policy.artifact_algorithm_version,
            writer_id=policy.producer.writer_id,
            python_abi=policy.producer.python_abi,
            pillow_build=policy.producer.pillow_build,
            libjpeg_build=policy.producer.libjpeg_build,
            zlib_build=policy.producer.zlib_build,
        )
        artifact_semantics_created = _ensure_artifact_policy(
            work,
            ingest_turn=ingest_turn,
            policy=policy,
            now=timestamp,
        )

        existing_artifact = _artifact_by_natural(work, policy, lock=False)
        existing_manifest = _manifest_by_natural(work, policy, lock=False)
        existing_analysis = _analysis_by_natural(work, policy, lock=False)
        existing_title_sort = _title_sort_by_natural(work, policy, lock=False)
        existing_display = None
        if existing_title_sort is not None:
            existing_display = _display_by_natural(
                work,
                policy,
                title_sort_policy_id=existing_title_sort,
                lock=False,
            )
        existing_operational = _operational_by_natural(work, policy, lock=False)

        missing = (
            existing_artifact is None,
            existing_manifest is None,
            existing_analysis is None,
            existing_title_sort is None,
            existing_display is None,
            existing_operational is None,
        )
        allocated = [
            VNextAllocatorRepository.allocate_identity(
                work,
                IdentityStream.POLICY,
                updated_at=timestamp,
            )
            for needs_id in missing
            if needs_id
        ]
        allocated_iter = iter(allocated)

        _artifact_policy_id, artifact_registry_created = _ensure_artifact_registry(
            work,
            policy,
            existing_id=existing_artifact,
            proposed_id=next(allocated_iter) if missing[0] else None,
        )
        manifest_id, manifest_created = _ensure_manifest(
            work,
            policy,
            existing_id=existing_manifest,
            proposed_id=next(allocated_iter) if missing[1] else None,
        )
        analysis_id, analysis_created = _ensure_analysis(
            work,
            policy,
            existing_id=existing_analysis,
            proposed_id=next(allocated_iter) if missing[2] else None,
        )
        title_sort_id, title_sort_created = _ensure_title_sort(
            work,
            policy,
            existing_id=existing_title_sort,
            proposed_id=next(allocated_iter) if missing[3] else None,
        )
        display_id, display_created = _ensure_display(
            work,
            policy,
            title_sort_policy_id=title_sort_id,
            existing_id=existing_display,
            proposed_id=next(allocated_iter) if missing[4] else None,
        )
        operational_id, operational_created = _ensure_operational(
            work,
            policy,
            existing_id=existing_operational,
            proposed_id=next(allocated_iter) if missing[5] else None,
        )
        if next(allocated_iter, None) is not None:  # pragma: no cover - invariant
            raise AssertionError("policy allocator result count drifted")

        created = any(
            (
                not producer.replayed,
                artifact_semantics_created,
                artifact_registry_created,
                manifest_created,
                analysis_created,
                title_sort_created,
                display_created,
                operational_created,
            )
        )
        return VNextResolvedIngestPolicy(
            policy=policy,
            manifest_policy_id=manifest_id,
            analysis_policy_id=analysis_id,
            artifact_policy_sha256=policy.artifact_policy_sha256,
            producer_fingerprint_sha256=policy.producer_fingerprint_sha256,
            display_title_policy_id=display_id,
            title_sort_policy_id=title_sort_id,
            operational_policy_id=operational_id,
            replayed=not created,
        )


def _require_bootstrap_policies(
    work: VNextUnitOfWork,
    policy: VNextIngestPolicy,
) -> None:
    try:
        writer = load_artifact_zip_writer_policy(
            work.connector,
            policy.artifact_algorithm_version,
        )
        storage = load_artifact_storage_codec(
            work.connector,
            policy.storage.storage_codec_version,
        )
    except RuntimeError as error:
        raise VNextIngestPolicyNotReadyError(
            "required artifact ZIP/storage bootstrap policy is unavailable"
        ) from error
    expected_writer = ArtifactZipWriterPolicyRecord(
        policy.artifact_algorithm_version,
        policy.zip.zip_codec_version,
        policy.zip.compression_method,
        policy.zip.compression_level,
        policy.zip.dos_date,
        policy.zip.dos_time,
        policy.zip.unix_mode,
        policy.zip.general_purpose_flags,
        policy.zip.create_system,
        policy.zip.archive_name_codec_version,
        policy.zip.artifact_name_codec_version,
    )
    expected_storage = ArtifactStorageCodecRecord(
        policy.storage.storage_codec_version,
        policy.storage.adapter_id,
        policy.storage.locator_codec_version,
        policy.storage.protection_token_codec_version,
    )
    if writer != expected_writer or storage != expected_storage:
        raise VNextIngestPolicyConflictError(
            "natural ZIP/storage facts differ from generated bootstrap authority"
        )


def _ensure_artifact_policy(
    work: VNextUnitOfWork,
    *,
    ingest_turn: IngestTurn,
    policy: VNextIngestPolicy,
    now: int,
) -> bool:
    digest = policy.artifact_policy_sha256
    expected = ArtifactPolicySemanticsRecord(
        digest,
        policy.artifact_algorithm_version,
        policy.max_image_short_side,
        policy.producer_fingerprint_sha256,
    )
    if work.connector.fetch_one(
        "SELECT policy_component_sha256 "
        "FROM catalog_artifact_policy_semantics_seals "
        "WHERE policy_component_sha256 = %s",
        (digest,),
    ):
        if load_artifact_policy_semantics(work.connector, digest) != expected:
            raise VNextIngestPolicyConflictError(
                "artifact policy digest collides with different facts"
            )
        return False
    if work.connector.fetch_one(
        "SELECT policy_component_sha256 "
        "FROM catalog_artifact_policy_semantics_anchors "
        "WHERE policy_component_sha256 = %s",
        (digest,),
    ):
        raise VNextIngestPolicyConflictError("artifact policy family is not sealed")

    canonical = persist_in_memory_canonical_value(
        work,
        generation=ingest_turn.generation,
        digest_domain=identity.ARTIFACT_POLICY_DIGEST_DOMAIN,
        payload=identity.encode_artifact_policy(
            policy.artifact_algorithm_version,
            policy.max_image_short_side,
            policy.producer_fingerprint_sha256,
        ),
        now=now,
        retain_claim=False,
    )
    if canonical.value_sha256 != digest:
        raise VNextIngestPolicyConflictError(
            "canonical artifact policy digest differs from natural facts"
        )
    connector = work.connector
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics_anchors "
        "(policy_component_sha256) VALUES (%s)",
        (digest,),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics_artifact_algorithm_versions "
        "(policy_component_sha256, artifact_algorithm_version) VALUES (%s, %s)",
        (digest, policy.artifact_algorithm_version),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics_max_image_short_sides "
        "(policy_component_sha256, max_image_short_side) VALUES (%s, %s)",
        (digest, policy.max_image_short_side),
    )
    connector.execute(
        "INSERT INTO "
        "catalog_artifact_policy_semantics_producer_fingerprint_sha256s "
        "(policy_component_sha256, producer_fingerprint_sha256) VALUES (%s, %s)",
        (digest, policy.producer_fingerprint_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics_identities "
        "(artifact_algorithm_version, max_image_short_side, "
        "producer_fingerprint_sha256, policy_component_sha256) "
        "VALUES (%s, %s, %s, %s)",
        (
            policy.artifact_algorithm_version,
            policy.max_image_short_side,
            policy.producer_fingerprint_sha256,
            digest,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics_seals "
        "(policy_component_sha256) VALUES (%s)",
        (digest,),
    )
    return True


def _natural_row(
    work: VNextUnitOfWork,
    query: str,
    parameters: tuple[object, ...],
    *,
    lock: bool,
) -> tuple[object, ...]:
    if lock:
        return work.lock_row(
            LockRank.HEAD,
            _POLICY_LOCK_KEY,
            query,
            parameters,
        )
    return work.connector.fetch_one(query, parameters)


def _artifact_by_natural(
    work: VNextUnitOfWork,
    policy: VNextIngestPolicy,
    *,
    lock: bool,
) -> int | None:
    digest = policy.artifact_policy_sha256
    row = _natural_row(
        work,
        "SELECT artifact_policy_id, policy_component_sha256 "
        "FROM catalog_artifact_policies WHERE policy_component_sha256 = %s",
        (digest,),
        lock=lock,
    )
    if not row:
        return None
    if len(row) != 2:
        raise VNextIngestPolicyConflictError(
            "artifact policy registry row has an invalid shape"
        )
    policy_id = require_positive_int63(
        row[0], field="artifact policy registry policy_id"
    )
    stored_digest = require_digest32(
        row[1], field="artifact policy registry component digest"
    )
    if stored_digest != digest:
        raise VNextIngestPolicyConflictError(
            "artifact policy registry natural identity differs"
        )
    return policy_id


def _manifest_by_natural(
    work: VNextUnitOfWork,
    policy: VNextIngestPolicy,
    *,
    lock: bool,
) -> int | None:
    row = _natural_row(
        work,
        "SELECT identity.manifest_policy_id "
        "FROM catalog_manifest_policy_identities AS identity "
        "JOIN catalog_manifest_policy_seals AS seal "
        "ON seal.manifest_policy_id = identity.manifest_policy_id "
        "WHERE identity.manifest_algorithm_version = %s "
        "AND identity.file_order_version = %s",
        (policy.manifest_algorithm_version, policy.file_order_version),
        lock=lock,
    )
    return None if not row else require_int63(row[0], field="manifest policy_id")


def _analysis_by_natural(
    work: VNextUnitOfWork,
    policy: VNextIngestPolicy,
    *,
    lock: bool,
) -> int | None:
    row = _natural_row(
        work,
        "SELECT identity.policy_id FROM catalog_analysis_policy_identities AS identity "
        "JOIN catalog_analysis_policy_seals AS seal "
        "ON seal.policy_id = identity.policy_id "
        "WHERE identity.algorithm_version = %s "
        "AND identity.spam_artist_threshold = %s "
        "AND identity.spam_occurrence_threshold = %s "
        "AND identity.content_owner_rule_version = %s "
        "AND identity.gid_winner_rule_version = %s",
        (
            policy.analysis_algorithm_version,
            policy.spam_artist_threshold,
            policy.spam_occurrence_threshold,
            policy.content_owner_rule_version,
            policy.gid_winner_rule_version,
        ),
        lock=lock,
    )
    return None if not row else require_int63(row[0], field="analysis policy_id")


def _title_sort_by_natural(
    work: VNextUnitOfWork,
    policy: VNextIngestPolicy,
    *,
    lock: bool,
) -> int | None:
    row = _natural_row(
        work,
        "SELECT identity.title_sort_policy_id "
        "FROM catalog_title_sort_policy_identities AS identity "
        "JOIN catalog_title_sort_policy_seals AS seal "
        "ON seal.title_sort_policy_id = identity.title_sort_policy_id "
        "WHERE identity.title_sort_algorithm_version = %s "
        "AND identity.unicode_data_version = %s",
        (policy.title_sort_algorithm_version, policy.unicode_data_version),
        lock=lock,
    )
    return None if not row else require_uint32(row[0], field="title_sort_policy_id")


def _display_by_natural(
    work: VNextUnitOfWork,
    policy: VNextIngestPolicy,
    *,
    title_sort_policy_id: int,
    lock: bool,
) -> int | None:
    row = _natural_row(
        work,
        "SELECT identity.display_title_policy_id "
        "FROM catalog_display_title_policy_identities AS identity "
        "JOIN catalog_display_title_policy_seals AS seal "
        "ON seal.display_title_policy_id = identity.display_title_policy_id "
        "WHERE identity.display_title_algorithm_version = %s "
        "AND identity.title_sort_policy_id = %s",
        (policy.display_title_algorithm_version, title_sort_policy_id),
        lock=lock,
    )
    return None if not row else require_int63(row[0], field="display policy_id")


def _operational_by_natural(
    work: VNextUnitOfWork,
    policy: VNextIngestPolicy,
    *,
    lock: bool,
) -> int | None:
    row = _natural_row(
        work,
        "SELECT operational_policy_id FROM operational_operational_policys "
        "WHERE operational_schema_version = %s AND algorithm_version = %s "
        "AND max_batch_rows = %s",
        (
            policy.operational_schema_version,
            policy.operational_algorithm_version,
            policy.operational_max_batch_rows,
        ),
        lock=lock,
    )
    return None if not row else require_int63(row[0], field="operational policy_id")


def _proposed(value: int | None, *, field: str) -> int:
    if value is None:
        raise AssertionError(f"missing allocated {field}")
    return require_int63(value, field=field)


def _ensure_artifact_registry(
    work: VNextUnitOfWork,
    policy: VNextIngestPolicy,
    *,
    existing_id: int | None,
    proposed_id: int | None,
) -> tuple[int, bool]:
    digest = policy.artifact_policy_sha256
    policy_id = existing_id or _artifact_by_natural(work, policy, lock=True)
    if policy_id is not None:
        expected = (policy_id, digest)
        actual = work.connector.fetch_one(
            "SELECT artifact_policy_id, policy_component_sha256 "
            "FROM catalog_artifact_policies WHERE artifact_policy_id = %s",
            (policy_id,),
        )
        if actual != expected:
            raise VNextIngestPolicyConflictError(
                "artifact policy registry replay differs"
            )
        return policy_id, False

    policy_id = require_positive_int63(
        _proposed(proposed_id, field="allocated artifact policy_id"),
        field="allocated artifact policy_id",
    )
    collision = work.connector.fetch_one(
        "SELECT artifact_policy_id, policy_component_sha256 "
        "FROM catalog_artifact_policies WHERE artifact_policy_id = %s",
        (policy_id,),
    )
    if collision:
        raise VNextIngestPolicyConflictError(
            "allocated artifact policy_id is already registered"
        )
    work.connector.execute(
        "INSERT INTO catalog_artifact_policies "
        "(artifact_policy_id, policy_component_sha256) VALUES (%s, %s)",
        (policy_id, digest),
    )
    return policy_id, True


def _ensure_manifest(
    work: VNextUnitOfWork,
    policy: VNextIngestPolicy,
    *,
    existing_id: int | None,
    proposed_id: int | None,
) -> tuple[int, bool]:
    policy_id = existing_id or _manifest_by_natural(work, policy, lock=True)
    if policy_id is not None:
        expected = ManifestPolicyRecord(
            policy_id,
            policy.manifest_algorithm_version,
            policy.file_order_version,
        )
        if load_manifest_policy(work.connector, policy_id) != expected:
            raise VNextIngestPolicyConflictError("manifest policy replay differs")
        return policy_id, False
    policy_id = _proposed(proposed_id, field="allocated manifest policy_id")
    connector = work.connector
    connector.execute(
        "INSERT INTO catalog_manifest_policy_anchors (manifest_policy_id) VALUES (%s)",
        (policy_id,),
    )
    connector.execute(
        "INSERT INTO catalog_manifest_policy_manifest_algorithm_versions "
        "(manifest_policy_id, manifest_algorithm_version) VALUES (%s, %s)",
        (policy_id, policy.manifest_algorithm_version),
    )
    connector.execute(
        "INSERT INTO catalog_manifest_policy_file_order_versions "
        "(manifest_policy_id, file_order_version) VALUES (%s, %s)",
        (policy_id, policy.file_order_version),
    )
    connector.execute(
        "INSERT INTO catalog_manifest_policy_identities "
        "(manifest_algorithm_version, file_order_version, manifest_policy_id) "
        "VALUES (%s, %s, %s)",
        (
            policy.manifest_algorithm_version,
            policy.file_order_version,
            policy_id,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_manifest_policy_seals (manifest_policy_id) VALUES (%s)",
        (policy_id,),
    )
    return policy_id, True


def _ensure_analysis(
    work: VNextUnitOfWork,
    policy: VNextIngestPolicy,
    *,
    existing_id: int | None,
    proposed_id: int | None,
) -> tuple[int, bool]:
    policy_id = existing_id or _analysis_by_natural(work, policy, lock=True)
    if policy_id is not None:
        expected = AnalysisPolicyRecord(
            policy_id,
            policy.analysis_algorithm_version,
            policy.spam_artist_threshold,
            policy.spam_occurrence_threshold,
            policy.content_owner_rule_version,
            policy.gid_winner_rule_version,
        )
        if load_analysis_policy(work.connector, policy_id) != expected:
            raise VNextIngestPolicyConflictError("analysis policy replay differs")
        return policy_id, False
    policy_id = _proposed(proposed_id, field="allocated analysis policy_id")
    connector = work.connector
    connector.execute(
        "INSERT INTO catalog_analysis_policy_anchors (policy_id) VALUES (%s)",
        (policy_id,),
    )
    facts = (
        (
            "catalog_analysis_policy_algorithm_versions",
            "algorithm_version",
            policy.analysis_algorithm_version,
        ),
        (
            "catalog_analysis_policy_spam_artist_thresholds",
            "spam_artist_threshold",
            policy.spam_artist_threshold,
        ),
        (
            "catalog_analysis_policy_spam_occurrence_thresholds",
            "spam_occurrence_threshold",
            policy.spam_occurrence_threshold,
        ),
        (
            "catalog_analysis_policy_content_owner_rule_versions",
            "content_owner_rule_version",
            policy.content_owner_rule_version,
        ),
        (
            "catalog_analysis_policy_gid_winner_rule_versions",
            "gid_winner_rule_version",
            policy.gid_winner_rule_version,
        ),
    )
    for table, column, value in facts:
        connector.execute(
            f"INSERT INTO {table} (policy_id, {column}) VALUES (%s, %s)",
            (policy_id, value),
        )
    connector.execute(
        "INSERT INTO catalog_analysis_policy_identities "
        "(algorithm_version, spam_artist_threshold, spam_occurrence_threshold, "
        "content_owner_rule_version, gid_winner_rule_version, policy_id) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (
            policy.analysis_algorithm_version,
            policy.spam_artist_threshold,
            policy.spam_occurrence_threshold,
            policy.content_owner_rule_version,
            policy.gid_winner_rule_version,
            policy_id,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_policy_seals (policy_id) VALUES (%s)",
        (policy_id,),
    )
    return policy_id, True


def _ensure_title_sort(
    work: VNextUnitOfWork,
    policy: VNextIngestPolicy,
    *,
    existing_id: int | None,
    proposed_id: int | None,
) -> tuple[int, bool]:
    policy_id = existing_id or _title_sort_by_natural(work, policy, lock=True)
    if policy_id is not None:
        expected = TitleSortPolicyRecord(
            policy_id,
            policy.title_sort_algorithm_version,
            policy.unicode_data_version,
        )
        if load_title_sort_policy(work.connector, policy_id) != expected:
            raise VNextIngestPolicyConflictError("title-sort policy replay differs")
        return policy_id, False
    policy_id = require_uint32(
        _proposed(proposed_id, field="allocated title-sort policy_id"),
        field="allocated title-sort policy_id",
    )
    connector = work.connector
    connector.execute(
        "INSERT INTO catalog_title_sort_policy_anchors (title_sort_policy_id) "
        "VALUES (%s)",
        (policy_id,),
    )
    connector.execute(
        "INSERT INTO catalog_title_sort_policy_algorithm_versions "
        "(title_sort_policy_id, title_sort_algorithm_version) VALUES (%s, %s)",
        (policy_id, policy.title_sort_algorithm_version),
    )
    connector.execute(
        "INSERT INTO catalog_title_sort_policy_unicode_data_versions "
        "(title_sort_policy_id, unicode_data_version) VALUES (%s, %s)",
        (policy_id, policy.unicode_data_version),
    )
    connector.execute(
        "INSERT INTO catalog_title_sort_policy_identities "
        "(title_sort_algorithm_version, unicode_data_version, "
        "title_sort_policy_id) VALUES (%s, %s, %s)",
        (
            policy.title_sort_algorithm_version,
            policy.unicode_data_version,
            policy_id,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_title_sort_policy_seals (title_sort_policy_id) "
        "VALUES (%s)",
        (policy_id,),
    )
    return policy_id, True


def _ensure_display(
    work: VNextUnitOfWork,
    policy: VNextIngestPolicy,
    *,
    title_sort_policy_id: int,
    existing_id: int | None,
    proposed_id: int | None,
) -> tuple[int, bool]:
    policy_id = existing_id or _display_by_natural(
        work,
        policy,
        title_sort_policy_id=title_sort_policy_id,
        lock=True,
    )
    if policy_id is not None:
        expected = DisplayTitlePolicyRecord(
            policy_id,
            policy.display_title_algorithm_version,
            title_sort_policy_id,
        )
        if load_display_title_policy(work.connector, policy_id) != expected:
            raise VNextIngestPolicyConflictError("display-title policy replay differs")
        return policy_id, False
    policy_id = _proposed(proposed_id, field="allocated display policy_id")
    connector = work.connector
    connector.execute(
        "INSERT INTO catalog_display_title_policy_anchors "
        "(display_title_policy_id) VALUES (%s)",
        (policy_id,),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policy_algorithm_versions "
        "(display_title_policy_id, display_title_algorithm_version) "
        "VALUES (%s, %s)",
        (policy_id, policy.display_title_algorithm_version),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policy_title_sort_policy_ids "
        "(display_title_policy_id, title_sort_policy_id) VALUES (%s, %s)",
        (policy_id, title_sort_policy_id),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policy_identities "
        "(display_title_algorithm_version, title_sort_policy_id, "
        "display_title_policy_id) VALUES (%s, %s, %s)",
        (
            policy.display_title_algorithm_version,
            title_sort_policy_id,
            policy_id,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policy_seals "
        "(display_title_policy_id) VALUES (%s)",
        (policy_id,),
    )
    return policy_id, True


def _ensure_operational(
    work: VNextUnitOfWork,
    policy: VNextIngestPolicy,
    *,
    existing_id: int | None,
    proposed_id: int | None,
) -> tuple[int, bool]:
    policy_id = existing_id or _operational_by_natural(work, policy, lock=True)
    if policy_id is not None:
        expected = (
            policy_id,
            policy.operational_schema_version,
            policy.operational_algorithm_version,
            policy.operational_max_batch_rows,
        )
        actual = work.connector.fetch_one(
            "SELECT operational_policy_id, operational_schema_version, "
            "algorithm_version, max_batch_rows "
            "FROM operational_operational_policys "
            "WHERE operational_policy_id = %s",
            (policy_id,),
        )
        if actual != expected:
            raise VNextIngestPolicyConflictError("operational policy replay differs")
        return policy_id, False
    policy_id = _proposed(proposed_id, field="allocated operational policy_id")
    work.connector.execute(
        "INSERT INTO operational_operational_policys "
        "(operational_policy_id, operational_schema_version, algorithm_version, "
        "max_batch_rows) VALUES (%s, %s, %s, %s)",
        (
            policy_id,
            policy.operational_schema_version,
            policy.operational_algorithm_version,
            policy.operational_max_batch_rows,
        ),
    )
    return policy_id, True
