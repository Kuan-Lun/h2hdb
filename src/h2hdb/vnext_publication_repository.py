"""Atomic source/catalog publication and durable recovery for vNext.

The public mutation accepts only a candidate identity plus the two live writer
capabilities.  Revisions, projection counts, the canonical source manifest,
the operational preparation, policies, head generations, and receipt identity
are all loaded or allocated by the database-side protocol.

The high-cardinality catalog projection and protected artifacts are prepared by
bounded upstream writers while their reserved revision is not referenced by a
head.  This module deliberately performs no child ``COUNT``/``SUM`` and never
bulk-updates prepared artifacts in the pointer transaction.  A PK-only
projection certification marker plus exact terminal validation receipts form
the fixed-query O(1) authority for those already-validated facts.  Filesystem
finalization remains a separately receipted bounded phase.
"""

from __future__ import annotations

__all__ = [
    "PublicationCommitReceipt",
    "PublicationConflictError",
    "PublicationCorruptionError",
    "PublicationHeadRaceError",
    "PublicationNotReadyError",
    "PublicationRepository",
    "PublicationRepositoryError",
    "PublicationSchemaNotReadyError",
]

import secrets
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from .vnext_allocator_repository import RevisionStream, VNextAllocatorRepository
from .vnext_analysis_family import (
    AnalysisFamilyCollisionError,
    require_exact_analysis_state_components,
)
from .vnext_analysis_repository import ANALYSIS_COMPONENTS
from .vnext_canonical_value_family import (
    CanonicalValueCollisionError,
    load_sealed_value_identity,
)
from .vnext_catalog_registry_repository import (
    CatalogRegistryConflictError,
    CatalogRegistryNotReadyError,
    load_analysis_policy,
)
from .vnext_domains import (
    INT63_MAX,
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from .vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from .vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
)
from .vnext_publication_finalization_repository import (
    _initialize_finalization_checkpoint,
)
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_CANDIDATE_TABLE = "catalog_publication_candidates"
_BASE_COMMIT_TABLE = "catalog_publication_candidate_base_publication_commits"
_BUILD_BASE_COMMIT_TABLE = "catalog_source_build_base_publication_commits"
_PROJECTION_SEAL_TABLE = "catalog_publication_candidate_projection_seals"
_PROJECTION_VIEW = "catalog_publication_candidate_projections"
_ANALYSIS_MANIFEST_TABLE = "catalog_analysis_snapshot_manifest"
_CATALOG_DESCRIPTOR_TABLE = "catalog_revision_descriptors"
_SOURCE_DESCRIPTOR_TABLE = "catalog_source_revision_descriptors"
_SOURCE_REVISION_ANCHOR_TABLE = "catalog_source_revision_anchors"
_SOURCE_REVISION_CHANNEL_TABLE = "catalog_source_revision_channels"
_SOURCE_REVISION_MANIFEST_TABLE = "catalog_source_revision_snapshot_manifests"
_SOURCE_REVISION_SEAL_TABLE = "catalog_source_revision_descriptor_seals"
_SOURCE_PROVENANCE_TABLE = "catalog_source_revision_provenance"

_GENERATION_NODE_TABLE = "catalog_publication_generation_nodes"
_GENERATION_SUCCESSOR_TABLE = "catalog_publication_generation_successors"
_COMMIT_ANCHOR_TABLE = "catalog_publication_commit_anchors"
_COMMIT_CANDIDATE_TABLE = "catalog_publication_commit_candidates"
_COMMIT_CATALOG_REVISION_TABLE = "catalog_publication_commit_catalog_revisions"
_COMMIT_SOURCE_REVISION_TABLE = "catalog_publication_commit_source_revisions"
_COMMIT_GENERATION_TABLE = "catalog_publication_commit_generations"
_COMMIT_PREPARATION_TABLE = "catalog_publication_commit_operational_preparations"
_COMMIT_OPERATIONAL_POLICY_TABLE = "catalog_publication_commit_operational_policies"
_COMMIT_ARTIFACT_POLICY_TABLE = "catalog_publication_commit_artifact_policies"
_COMMIT_DISPLAY_POLICY_TABLE = "catalog_publication_commit_display_title_policies"
_COMMIT_NEW_TABLE = "catalog_publication_commit_new_galleries"
_COMMIT_CHANGED_TABLE = "catalog_publication_commit_changed_galleries"
_COMMIT_REMOVED_TABLE = "catalog_publication_commit_removed_galleries"
_COMMIT_DUPLICATE_TABLE = "catalog_publication_commit_duplicate_losers"
_COMMIT_COMMITTED_AT_TABLE = "catalog_publication_commit_committed_ats"
_COMMIT_SEAL_TABLE = "catalog_publication_commit_seals"
_COMMIT_VIEW = "catalog_publication_commits"
_PUBLICATION_RECEIPT_VIEW = "catalog_publication_receipts"
_COMMIT_HEAD_TABLE = "catalog_publication_commit_head_receipts"

_BUILD_GENERATION_TABLE = "operational_source_build_generations"
_SOURCE_WORKING_TABLE = "operational_source_working_builds"
_CATALOG_WORKING_TABLE = "operational_catalog_working_candidates"
_CANDIDATE_PREPARATION_TABLE = "operational_publication_candidate_preparations"
_PREPARATION_TABLE = "operational_operational_preparations"
_EFFECT_SEAL_TABLE = "operational_operational_preparation_effect_seals"
_ACTIVATION_TABLE = "operational_operational_activations"
_DELETION_HEAD_TABLE = "operational_deletion_request_generation_heads"

_SOURCE_MANIFEST_DOMAIN = b"source_snapshot_manifest_v1"
_PROJECTION_TERMINAL_STAGES = (
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
_REQUIRED_SCHEMA_TABLES = (
    _ANALYSIS_MANIFEST_TABLE,
    _PROJECTION_SEAL_TABLE,
    _CANDIDATE_PREPARATION_TABLE,
)


class PublicationRepositoryError(RuntimeError):
    """Base class for vNext publication failures."""


class PublicationSchemaNotReadyError(PublicationRepositoryError):
    """The generated schema lacks an O(1) authority required for publication."""


class PublicationNotReadyError(PublicationRepositoryError):
    """The candidate or one of its server-owned prerequisites is incomplete."""


class PublicationConflictError(PublicationRepositoryError):
    """An immutable recovery identity names different exact publication facts."""


class PublicationCorruptionError(PublicationRepositoryError):
    """Persisted normalized facts disagree with the publication contract."""


class PublicationHeadRaceError(PublicationNotReadyError):
    """A candidate-pinned source, catalog, or deletion head has advanced."""


@dataclass(frozen=True, slots=True)
class PublicationCommitReceipt:
    candidate_id: bytes
    receipt_id: bytes
    revision: int
    source_revision: int
    channel: bytes
    artifact_policy_id: int
    display_title_policy_id: int
    publication_count: int
    new_galleries: int
    changed_galleries: int
    removed_galleries: int
    duplicate_losers: int
    state: str
    committed_at: int
    finalized_at: int | None
    replayed: bool

    def __post_init__(self) -> None:
        require_uuid16(self.candidate_id, field="publication candidate_id")
        require_uuid16(self.receipt_id, field="publication receipt_id")
        require_positive_int63(self.revision, field="catalog revision")
        require_positive_int63(self.source_revision, field="source revision")
        require_bounded_bytes(
            self.channel,
            field="publication channel",
            minimum=1,
            maximum=64,
        )
        require_positive_int63(
            self.artifact_policy_id,
            field="publication artifact_policy_id",
        )
        require_positive_int63(
            self.display_title_policy_id,
            field="publication display_title_policy_id",
        )
        for field, value in (
            ("publication_count", self.publication_count),
            ("new_galleries", self.new_galleries),
            ("changed_galleries", self.changed_galleries),
            ("removed_galleries", self.removed_galleries),
            ("duplicate_losers", self.duplicate_losers),
        ):
            require_int63(value, field=f"publication {field}")
        if self.state not in {"DB_COMMITTED", "PROJECTION_FINALIZED"}:
            raise ValueError("publication receipt state is not registered")
        require_int63(self.committed_at, field="publication committed_at")
        if self.finalized_at is not None:
            require_int63(self.finalized_at, field="publication finalized_at")
            if self.finalized_at < self.committed_at:
                raise ValueError("publication finalized_at precedes committed_at")
        if (self.state == "DB_COMMITTED") != (self.finalized_at is None):
            raise ValueError("publication state/finalized_at pair is inconsistent")
        if not isinstance(self.replayed, bool):
            raise TypeError("publication replayed must be bool")


@dataclass(frozen=True, slots=True)
class _Candidate:
    candidate_id: bytes
    analysis_id: bytes
    reserved_revision: int
    channel: bytes
    artifact_policy_id: int
    display_title_policy_id: int
    artifacts_required: bool
    created_at: int
    build_id: bytes
    input_manifest_sha256: bytes
    analysis_completed_at: int
    build_sealed_at: int
    snapshot_manifest_sha256: bytes


@dataclass(frozen=True, slots=True)
class _ProjectionSeal:
    publication_count: int
    artifact_input_count: int
    prepared_artifact_count: int
    create_count: int
    rebuild_count: int
    delete_count: int
    unchanged_count: int
    new_galleries: int
    changed_galleries: int
    removed_galleries: int
    duplicate_losers: int
    certified_at: int


@dataclass(frozen=True, slots=True)
class _Preparation:
    preparation_id: bytes
    build_id: bytes
    deletion_generation: int
    policy_id: int
    prepared_at: int
    completed_at: int
    bound_at: int


@dataclass(frozen=True, slots=True)
class _LockedHead:
    receipt_id: bytes
    revision: int
    source_revision: int
    generation: int
    committed_at: int


@dataclass(frozen=True, slots=True)
class _PublishedCommit:
    receipt_id: bytes
    candidate_id: bytes
    revision: int
    source_revision: int
    generation: int
    preparation_id: bytes
    operational_policy_id: int
    artifact_policy_id: int
    display_title_policy_id: int
    new_galleries: int
    changed_galleries: int
    removed_galleries: int
    duplicate_losers: int
    committed_at: int
    channel: bytes
    snapshot_manifest_sha256: bytes
    publication_count: int
    finalized_at: int | None


class PublicationRepository:
    """Publish a sealed candidate and recover its immutable receipt."""

    @staticmethod
    def commit(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        candidate_id: bytes,
        now: int,
    ) -> PublicationCommitReceipt:
        """Atomically advance source/catalog heads for one sealed candidate.

        The caller owns the surrounding transaction.  Any exception therefore
        leaves allocator, revision, activation, receipt, candidate, working
        roots, and both heads unchanged when that transaction is rolled back.
        """

        candidate_key = require_uuid16(candidate_id, field="publication candidate_id")
        timestamp = require_int63(now, field="publication committed_at")
        ingest_generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        _require_schema_authorities(work)

        # The permanent mapping is intentionally consulted before any transient
        # candidate/analysis row.  A successful commit therefore remains exactly
        # replayable after cleanup, and replay performs no repair writes.
        published = _load_published_commit_by_candidate(work.connector, candidate_key)
        if published is not None:
            _validate_published_commit(work.connector, published)
            return _commit_receipt(published, replayed=True)

        candidate = _lock_candidate(work, candidate_key)
        mapping = _lock_generation_mapping(work, ingest_generation)
        source_working = _lock_source_working(work, candidate.build_id)
        catalog_working = _lock_catalog_working(work, candidate_key)
        preparation = _lock_preparation(work, candidate_key)

        if preparation.build_id != candidate.build_id:
            raise PublicationCorruptionError(
                "candidate preparation belongs to a different source build"
            )
        _require_exact_analysis_seals(work, candidate)
        projection = _lock_projection_seal(work, candidate_key)
        _validate_artifact_counts(candidate, projection)
        _lock_and_validate_effect_seal(work, preparation, timestamp=timestamp)
        _validate_catalog_revision(work, candidate, projection)
        candidate_base = _load_base_commit(work.connector, candidate_key)
        _require_build_base_commit(
            work.connector,
            build_id=candidate.build_id,
            candidate_base=candidate_base,
        )

        if mapping != candidate.build_id:
            raise PublicationNotReadyError(
                "the live ingest generation is not mapped to the candidate build"
            )
        if source_working is None or source_working[1] != candidate.build_id:
            raise PublicationNotReadyError(
                "the candidate build does not own the source working slot"
            )
        if catalog_working is None or catalog_working[1] != candidate.candidate_id:
            raise PublicationNotReadyError(
                "the candidate does not own the catalog working slot"
            )
        if timestamp < max(
            candidate.created_at,
            candidate.analysis_completed_at,
            candidate.build_sealed_at,
            projection.certified_at,
            preparation.completed_at,
            preparation.bound_at,
        ):
            raise PublicationNotReadyError(
                "publication timestamp precedes a sealed prerequisite"
            )

        _require_fresh_commit_identities(
            work.connector,
            candidate_id=candidate_key,
            revision=candidate.reserved_revision,
            preparation_id=preparation.preparation_id,
        )
        receipt_id = require_uuid16(_new_receipt_id(), field="generated receipt_id")
        if work.connector.fetch_one(
            f"SELECT receipt_id FROM {_COMMIT_ANCHOR_TABLE} WHERE receipt_id = %s",
            (receipt_id,),
        ):
            raise PublicationConflictError(
                "generated publication receipt identity already exists"
            )

        # Allocator rank precedes HEAD rank.  A later head/deletion mismatch
        # aborts the surrounding transaction and rolls this allocation back.
        source_revision = VNextAllocatorRepository.allocate_revision(
            work,
            RevisionStream.SOURCE,
            updated_at=timestamp,
        )
        head = _lock_publication_commit_head(work, candidate.channel)
        deletion_generation = _lock_deletion_generation_head(work)
        _require_exact_commit_base(candidate_base, head)
        if deletion_generation != preparation.deletion_generation:
            raise PublicationHeadRaceError(
                "deletion-request generation advanced after operational preparation"
            )

        publication_generation = _successor_generation(head)
        predecessor_generation = publication_generation - 1
        _require_available_successor(
            work.connector,
            generation=publication_generation,
            predecessor_generation=predecessor_generation,
        )

        connector = work.connector
        connector.execute(
            f"INSERT INTO {_GENERATION_NODE_TABLE} (generation) VALUES (%s)",
            (publication_generation,),
        )
        connector.execute(
            f"INSERT INTO {_GENERATION_SUCCESSOR_TABLE} "
            "(successor_generation, predecessor_generation) VALUES (%s, %s)",
            (publication_generation, predecessor_generation),
        )

        connector.execute(
            f"INSERT INTO {_SOURCE_REVISION_ANCHOR_TABLE} "
            "(source_revision) VALUES (%s)",
            (source_revision,),
        )
        connector.execute(
            f"INSERT INTO {_SOURCE_REVISION_CHANNEL_TABLE} "
            "(source_revision, channel) VALUES (%s, %s)",
            (source_revision, candidate.channel),
        )
        connector.execute(
            f"INSERT INTO {_SOURCE_REVISION_MANIFEST_TABLE} "
            "(source_revision, snapshot_manifest_sha256) VALUES (%s, %s)",
            (source_revision, candidate.snapshot_manifest_sha256),
        )
        connector.execute(
            f"INSERT INTO {_SOURCE_REVISION_SEAL_TABLE} "
            "(source_revision) VALUES (%s)",
            (source_revision,),
        )
        connector.execute(
            f"INSERT INTO {_SOURCE_PROVENANCE_TABLE} "
            "(source_revision, analysis_id) VALUES (%s, %s)",
            (source_revision, candidate.analysis_id),
        )

        _insert_publication_commit(
            connector,
            receipt_id=receipt_id,
            candidate=candidate,
            preparation=preparation,
            projection=projection,
            source_revision=source_revision,
            generation=publication_generation,
            committed_at=timestamp,
        )
        _advance_publication_commit_head(
            work,
            channel=candidate.channel,
            base=head,
            receipt_id=receipt_id,
        )
        _delete_working_root(
            work,
            table=_SOURCE_WORKING_TABLE,
            label="source working build",
            slot=source_working[0],
            identity_column="build_id",
            identity=candidate.build_id,
        )
        _delete_working_root(
            work,
            table=_CATALOG_WORKING_TABLE,
            label="catalog working candidate",
            slot=catalog_working[0],
            identity_column="candidate_id",
            identity=candidate.candidate_id,
        )

        return PublicationCommitReceipt(
            candidate.candidate_id,
            receipt_id,
            candidate.reserved_revision,
            source_revision,
            candidate.channel,
            candidate.artifact_policy_id,
            candidate.display_title_policy_id,
            projection.publication_count,
            projection.new_galleries,
            projection.changed_galleries,
            projection.removed_galleries,
            projection.duplicate_losers,
            "DB_COMMITTED",
            timestamp,
            None,
            False,
        )


def _authorize(
    work: VNextUnitOfWork,
    gate_lease: GateLease,
    ingest_turn: IngestTurn,
    *,
    now: int,
) -> int:
    live_gate = MaintenanceGateRepository.lock_and_require_live(
        work,
        gate_lease,
        now=now,
    )
    if live_gate.mode is not GateMode.SHARED:
        raise PublicationNotReadyError(
            "publication requires a live SHARED maintenance gate"
        )
    live_turn = IngestFenceRepository.lock_and_require_live(work, ingest_turn, now=now)
    return require_int63(live_turn.generation, field="publication ingest generation")


def _require_schema_authorities(work: VNextUnitOfWork) -> None:
    missing = tuple(
        table
        for table in _REQUIRED_SCHEMA_TABLES
        if not work.connector.check_table_exists(table)
    )
    if missing:
        raise PublicationSchemaNotReadyError(
            "publication is fail-closed until generated O(1) authorities exist: "
            + ", ".join(missing)
        )


def _lock_candidate(work: VNextUnitOfWork, candidate_id: bytes) -> _Candidate:
    candidate_row = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("publication", 0, candidate_id),
        f"SELECT c.analysis_id, c.reserved_revision, c.artifact_policy_id, "
        "c.display_title_policy_id, c.artifacts_required, c.created_at "
        f"FROM {_CANDIDATE_TABLE} c WHERE c.candidate_id = %s",
        (candidate_id,),
    )
    if len(candidate_row) != 6:
        raise PublicationNotReadyError("publication candidate is missing")
    context = work.connector.fetch_one(
        "SELECT run_build.build_id, run_input.input_manifest_sha256, "
        "run_state.state, run_completed.completed_at, "
        "b.state, sealed.sealed_at, bc.channel, bm_digest.manifest_sha256, "
        "bm_gallery.gallery_count, bm_file.file_count, bm_byte.byte_count, "
        "sm.snapshot_manifest_sha256, m_gallery.gallery_count, m_file.file_count, "
        "m_byte.byte_count, run_policy.policy_id "
        "FROM catalog_analysis_run_descriptor_seals run_seal "
        "JOIN catalog_analysis_run_build_ids run_build "
        "ON run_build.analysis_id = run_seal.analysis_id "
        "JOIN catalog_analysis_run_input_manifest_sha256s run_input "
        "ON run_input.analysis_id = run_seal.analysis_id "
        "JOIN catalog_analysis_run_states run_state "
        "ON run_state.analysis_id = run_seal.analysis_id "
        "JOIN catalog_analysis_run_completed_ats run_completed "
        "ON run_completed.analysis_id = run_seal.analysis_id "
        "JOIN catalog_analysis_run_policy_ids run_policy "
        "ON run_policy.analysis_id = run_seal.analysis_id "
        "JOIN catalog_source_build_descriptor_seals b_seal "
        "ON b_seal.build_id = run_build.build_id "
        "JOIN catalog_source_build_states b ON b.build_id = b_seal.build_id "
        "JOIN catalog_source_build_sealed_ats sealed ON sealed.build_id = b.build_id "
        "JOIN catalog_source_build_channel bc ON bc.build_id = b.build_id "
        "JOIN catalog_build_manifest_seals bm_seal ON bm_seal.build_id = b.build_id "
        "JOIN catalog_build_manifest_manifest_sha256s bm_digest "
        "ON bm_digest.build_id = bm_seal.build_id "
        "JOIN catalog_source_build_discovery_gallery_counts bm_gallery "
        "ON bm_gallery.build_id = bm_seal.build_id "
        "JOIN catalog_build_manifest_file_counts bm_file "
        "ON bm_file.build_id = bm_seal.build_id "
        "JOIN catalog_build_manifest_byte_counts bm_byte "
        "ON bm_byte.build_id = bm_seal.build_id "
        f"JOIN {_ANALYSIS_MANIFEST_TABLE} sm "
        "ON sm.analysis_id = run_seal.analysis_id "
        "JOIN catalog_source_snapshot_manifest_identity_seals m_seal "
        "ON m_seal.snapshot_manifest_sha256 = sm.snapshot_manifest_sha256 "
        "JOIN catalog_source_snapshot_manifest_identity_gallery_counts m_gallery "
        "ON m_gallery.snapshot_manifest_sha256 = m_seal.snapshot_manifest_sha256 "
        "JOIN catalog_source_snapshot_manifest_identity_file_counts m_file "
        "ON m_file.snapshot_manifest_sha256 = m_seal.snapshot_manifest_sha256 "
        "JOIN catalog_source_snapshot_manifest_identity_byte_counts m_byte "
        "ON m_byte.snapshot_manifest_sha256 = m_seal.snapshot_manifest_sha256 "
        "WHERE run_seal.analysis_id = %s",
        (candidate_row[0],),
    )
    if len(context) != 16:
        raise PublicationNotReadyError(
            "candidate analysis, build, or snapshot manifest is missing"
        )
    snapshot_manifest = require_digest32(
        context[11],
        field="analysis snapshot manifest",
    )
    try:
        canonical = load_sealed_value_identity(
            work.connector,
            value_sha256=snapshot_manifest,
        )
    except CanonicalValueCollisionError as error:
        raise PublicationCorruptionError(
            "canonical snapshot manifest family is partial or conflicting"
        ) from error
    if canonical is None:
        raise PublicationNotReadyError(
            "canonical snapshot manifest is not exactly sealed"
        )
    analysis_id = require_uuid16(candidate_row[0], field="candidate analysis_id")
    revision = require_positive_int63(
        candidate_row[1],
        field="candidate reserved_revision",
    )
    artifact_policy = require_positive_int63(
        candidate_row[2],
        field="candidate artifact_policy_id",
    )
    display_policy = require_positive_int63(
        candidate_row[3],
        field="candidate display_title_policy_id",
    )
    if candidate_row[4] not in {0, 1}:
        raise PublicationCorruptionError("candidate artifacts_required is not boolean")
    created_at = require_int63(candidate_row[5], field="candidate created_at")
    build_id = require_uuid16(context[0], field="candidate build_id")
    input_manifest = require_digest32(context[1], field="analysis input manifest")
    if context[2] != "COMPLETE" or context[3] is None:
        raise PublicationNotReadyError("candidate analysis is not COMPLETE")
    analysis_completed_at = require_int63(
        context[3],
        field="analysis completed_at",
    )
    if context[4] != "SEALED" or context[5] is None:
        raise PublicationNotReadyError("candidate source build is not SEALED")
    build_sealed_at = require_int63(context[5], field="source build sealed_at")
    channel = require_bounded_bytes(
        context[6],
        field="source build channel",
        minimum=1,
        maximum=64,
    )
    build_manifest = require_digest32(context[7], field="source build manifest")
    build_counts = tuple(
        require_int63(value, field=field)
        for field, value in (
            ("build gallery_count", context[8]),
            ("build file_count", context[9]),
            ("build byte_count", context[10]),
        )
    )
    snapshot_manifest = require_digest32(
        context[11],
        field="analysis snapshot manifest",
    )
    snapshot_counts = tuple(
        require_int63(value, field=field)
        for field, value in (
            ("snapshot gallery_count", context[12]),
            ("snapshot file_count", context[13]),
            ("snapshot byte_count", context[14]),
        )
    )
    try:
        policy = load_analysis_policy(
            work.connector,
            require_positive_int63(context[15], field="analysis policy_id"),
        )
    except CatalogRegistryNotReadyError as error:
        raise PublicationNotReadyError(
            "candidate analysis policy is absent, partial, or unsealed"
        ) from error
    except CatalogRegistryConflictError as error:
        raise PublicationCorruptionError(
            "candidate analysis policy contains conflicting exact facts"
        ) from error
    policy_values = (
        policy.algorithm_version,
        policy.spam_artist_threshold,
        policy.spam_occurrence_threshold,
        policy.content_owner_rule_version,
        policy.gid_winner_rule_version,
    )
    expected_input = _analysis_input_digest(
        build_manifest,
        build_counts,
        policy_values,
    )
    if input_manifest != expected_input:
        raise PublicationCorruptionError(
            "analysis input digest disagrees with its build counts and policy"
        )
    if snapshot_counts != build_counts:
        raise PublicationConflictError(
            "analysis snapshot counts disagree with the sealed source build"
        )
    if canonical.digest_domain != _SOURCE_MANIFEST_DOMAIN:
        raise PublicationCorruptionError(
            "analysis snapshot manifest uses the wrong canonical digest domain"
        )
    require_digest32(
        canonical.root_page_sha256,
        field="snapshot manifest root_page_sha256",
    )
    require_int63(
        canonical.byte_count,
        field="snapshot canonical payload byte_count",
    )
    # ``input_manifest_sha256`` is an audit digest over the build manifest,
    # all three build counts, and the complete analysis-policy tuple.  It is
    # intentionally not the raw build-manifest digest and never authorizes
    # source publication.  COMPLETE component seals plus the canonical
    # analysis_snapshot_manifest binding are the output authority here.
    if build_sealed_at > analysis_completed_at or analysis_completed_at > created_at:
        raise PublicationCorruptionError(
            "candidate definition precedes its sealed analysis/build authority"
        )
    return _Candidate(
        candidate_id,
        analysis_id,
        revision,
        channel,
        artifact_policy,
        display_policy,
        bool(candidate_row[4]),
        created_at,
        build_id,
        input_manifest,
        analysis_completed_at,
        build_sealed_at,
        snapshot_manifest,
    )


def _analysis_input_digest(
    manifest_sha256: bytes,
    counts: tuple[int, ...],
    policy_values: tuple[int, int, int, int, int],
) -> bytes:
    if len(counts) != 3:
        raise ValueError("build manifest requires exactly three aggregate counts")
    payload = bytearray(b"h2hdb-vnext-analysis-input\0")
    payload.extend(require_digest32(manifest_sha256, field="build manifest_sha256"))
    for index, count in enumerate(counts):
        payload.extend(
            require_int63(count, field=f"build manifest count {index}").to_bytes(
                8, "big"
            )
        )
    payload.extend(policy_values[0].to_bytes(4, "big"))
    payload.extend(policy_values[1].to_bytes(8, "big"))
    payload.extend(policy_values[2].to_bytes(8, "big"))
    payload.extend(policy_values[3].to_bytes(4, "big"))
    payload.extend(policy_values[4].to_bytes(4, "big"))
    return sha256(payload).digest()


def _lock_generation_mapping(work: VNextUnitOfWork, generation: int) -> bytes | None:
    row = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("publication", 1, generation),
        f"SELECT build_id FROM {_BUILD_GENERATION_TABLE} WHERE generation = %s",
        (generation,),
    )
    if not row:
        return None
    if len(row) != 1:
        raise PublicationCorruptionError("source-build generation mapping is malformed")
    return require_uuid16(row[0], field="mapped publication build_id")


def _lock_source_working(
    work: VNextUnitOfWork, build_id: bytes
) -> tuple[int, bytes, int] | None:
    row = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("publication", 2, build_id),
        f"SELECT slot, build_id, assigned_at FROM {_SOURCE_WORKING_TABLE} "
        "WHERE build_id = %s",
        (build_id,),
    )
    if not row:
        return None
    if len(row) != 3:
        raise PublicationCorruptionError("source working root is malformed")
    slot = require_positive_int63(row[0], field="source working slot")
    mapped = require_uuid16(row[1], field="source working build_id")
    assigned_at = require_int63(row[2], field="source working assigned_at")
    return slot, mapped, assigned_at


def _lock_catalog_working(
    work: VNextUnitOfWork, candidate_id: bytes
) -> tuple[int, bytes, int] | None:
    row = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("publication", 3, candidate_id),
        f"SELECT slot, candidate_id, assigned_at FROM {_CATALOG_WORKING_TABLE} "
        "WHERE candidate_id = %s",
        (candidate_id,),
    )
    if not row:
        return None
    if len(row) != 3:
        raise PublicationCorruptionError("catalog working root is malformed")
    slot = require_positive_int63(row[0], field="catalog working slot")
    mapped = require_uuid16(row[1], field="catalog working candidate_id")
    assigned_at = require_int63(row[2], field="catalog working assigned_at")
    return slot, mapped, assigned_at


def _lock_preparation(work: VNextUnitOfWork, candidate_id: bytes) -> _Preparation:
    row = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("publication", 4, candidate_id),
        f"SELECT a.preparation_id, p.build_id, p.deletion_request_generation, "
        "p.operational_policy_id, p.state, p.prepared_at, p.completed_at, "
        "a.bound_at "
        f"FROM {_CANDIDATE_PREPARATION_TABLE} a "
        f"JOIN {_PREPARATION_TABLE} p ON p.preparation_id = a.preparation_id "
        "WHERE a.candidate_id = %s",
        (candidate_id,),
    )
    if len(row) != 8:
        raise PublicationNotReadyError(
            "candidate has no exact operational preparation authority"
        )
    preparation_id = require_uuid16(row[0], field="operational preparation_id")
    build_id = require_uuid16(row[1], field="operational preparation build_id")
    deletion_generation = require_int63(row[2], field="preparation deletion generation")
    policy_id = require_positive_int63(row[3], field="operational policy_id")
    if row[4] != "COMPLETE" or row[6] is None:
        raise PublicationNotReadyError("operational preparation is not COMPLETE")
    prepared_at = require_int63(row[5], field="operational prepared_at")
    completed_at = require_int63(row[6], field="operational completed_at")
    bound_at = require_int63(row[7], field="candidate preparation bound_at")
    if completed_at < prepared_at:
        raise PublicationCorruptionError(
            "operational preparation lifecycle timestamps are not monotone"
        )
    if bound_at < completed_at:
        raise PublicationCorruptionError(
            "candidate preparation was bound before completion"
        )
    return _Preparation(
        preparation_id,
        build_id,
        deletion_generation,
        policy_id,
        prepared_at,
        completed_at,
        bound_at,
    )


def _require_exact_analysis_seals(work: VNextUnitOfWork, candidate: _Candidate) -> None:
    try:
        families = require_exact_analysis_state_components(
            work.connector,
            analysis_id=candidate.analysis_id,
            state_components=ANALYSIS_COMPONENTS,
        )
    except AnalysisFamilyCollisionError as error:
        raise PublicationNotReadyError(str(error)) from error
    if any(family.sealed_at > candidate.analysis_completed_at for family in families):
        raise PublicationCorruptionError(
            "analysis completed before one of its immutable component seals"
        )


def _lock_projection_seal(
    work: VNextUnitOfWork, candidate_id: bytes
) -> _ProjectionSeal:
    marker = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("publication", 0, candidate_id),
        f"SELECT candidate_id FROM {_PROJECTION_SEAL_TABLE} " "WHERE candidate_id = %s",
        (candidate_id,),
    )
    if marker != (candidate_id,):
        raise PublicationNotReadyError(
            "candidate has no PK-only projection certification seal"
        )
    projection_row = work.connector.fetch_one(
        f"SELECT create_count, rebuild_count, delete_count, new_galleries, "
        f"changed_galleries FROM {_PROJECTION_VIEW} WHERE candidate_id = %s",
        (candidate_id,),
    )
    if len(projection_row) != 5:
        raise PublicationCorruptionError(
            "projection certification does not derive five exact counts"
        )
    terminal = {
        stage: _load_terminal_validation_receipt(
            work.connector,
            candidate_id=candidate_id,
            stage=stage,
        )
        for stage in _PROJECTION_TERMINAL_STAGES
    }
    create_count, rebuild_count, delete_count, new_galleries, changed_galleries = (
        require_int63(value, field="derived projection count")
        for value in projection_row
    )
    derived_projection = (
        terminal[b"VALIDATE_CREATE"][0],
        terminal[b"VALIDATE_REBUILD"][0],
        terminal[b"VALIDATE_DELETE"][0],
        terminal[b"VALIDATE_NEW_GALLERY"][0],
        terminal[b"VALIDATE_CHANGED_GALLERY"][0],
    )
    if (
        create_count,
        rebuild_count,
        delete_count,
        new_galleries,
        changed_galleries,
    ) != derived_projection:
        raise PublicationCorruptionError(
            "projection view disagrees with terminal validation receipts"
        )
    seal = _ProjectionSeal(
        terminal[b"VALIDATE_SELECTION"][0],
        terminal[b"VALIDATE_ARTIFACT_INPUT_DELTA"][0],
        terminal[b"VALIDATE_PREPARED_ARTIFACT"][0],
        create_count,
        rebuild_count,
        delete_count,
        terminal[b"VALIDATE_UNCHANGED"][0],
        new_galleries,
        changed_galleries,
        terminal[b"VALIDATE_REMOVED_GALLERY"][0],
        terminal[b"VALIDATE_DUPLICATE_LOSER"][0],
        max(committed_at for _count, committed_at in terminal.values()),
    )
    if seal.prepared_artifact_count != seal.create_count + seal.rebuild_count:
        raise PublicationCorruptionError(
            "projection prepared count disagrees with CREATE/REBUILD counts"
        )
    if seal.artifact_input_count != (
        seal.create_count + seal.rebuild_count + seal.unchanged_count
    ):
        raise PublicationCorruptionError(
            "projection artifact-input count disagrees with new-side operations"
        )
    return seal


def _load_terminal_validation_receipt(
    connector: Any,
    *,
    candidate_id: bytes,
    stage: bytes,
) -> tuple[int, int]:
    row = connector.fetch_one(
        "SELECT terminal.next_processed_count, terminal.committed_at "
        "FROM catalog_publication_checkpoints AS checkpoint "
        "JOIN catalog_publication_batch_receipts AS terminal "
        "ON terminal.candidate_id = checkpoint.candidate_id "
        "AND terminal.stage = checkpoint.stage "
        "AND terminal.committed_generation = checkpoint.generation "
        "AND terminal.next_cursor = checkpoint.cursor "
        "AND terminal.next_cursor = terminal.start_cursor "
        "AND terminal.next_processed_count = checkpoint.processed_count "
        "AND terminal.committed_at = checkpoint.updated_at "
        "AND terminal.terminal = 1 "
        "AND terminal.next_state = checkpoint.state "
        "WHERE checkpoint.candidate_id = %s AND checkpoint.stage = %s "
        "AND checkpoint.state = 'COMPLETE'",
        (candidate_id, stage),
    )
    if len(row) != 2:
        raise PublicationNotReadyError(
            f"{stage.decode('ascii')} lacks one exact terminal receipt"
        )
    return (
        require_int63(row[0], field=f"{stage!r} terminal count"),
        require_int63(row[1], field=f"{stage!r} terminal committed_at"),
    )


def _lock_and_validate_effect_seal(
    work: VNextUnitOfWork,
    preparation: _Preparation,
    *,
    timestamp: int,
) -> None:
    row = work.lock_row(
        LockRank.CHECKPOINT,
        encode_lock_key("publication", 1, preparation.preparation_id),
        f"SELECT event_count, final_chain_sha256, sealed_at "
        f"FROM {_EFFECT_SEAL_TABLE} WHERE preparation_id = %s",
        (preparation.preparation_id,),
    )
    if len(row) != 3:
        raise PublicationNotReadyError("operational preparation effect seal is missing")
    require_int63(row[0], field="operational sealed event_count")
    require_digest32(row[1], field="operational final_chain_sha256")
    sealed_at = require_int63(row[2], field="operational effect sealed_at")
    if sealed_at > preparation.completed_at or timestamp < sealed_at:
        raise PublicationCorruptionError(
            "operational effect seal timestamps disagree with completion"
        )


def _validate_artifact_counts(
    candidate: _Candidate, projection: _ProjectionSeal
) -> None:
    if candidate.artifacts_required:
        if projection.artifact_input_count != projection.publication_count:
            raise PublicationCorruptionError(
                "artifact-enabled projection does not cover every publication"
            )
        return
    if any(
        (
            projection.artifact_input_count,
            projection.prepared_artifact_count,
            projection.create_count,
            projection.rebuild_count,
            projection.unchanged_count,
        )
    ):
        raise PublicationCorruptionError(
            "artifact-disabled projection contains a desired artifact side"
        )


def _load_base_commit(connector: Any, candidate_id: bytes) -> bytes | None:
    row = connector.fetch_one(
        f"SELECT base_receipt_id FROM {_BASE_COMMIT_TABLE} WHERE candidate_id = %s",
        (candidate_id,),
    )
    if not row:
        return None
    if len(row) != 1:
        raise PublicationCorruptionError("candidate base commit row is malformed")
    return require_uuid16(row[0], field="candidate base receipt_id")


def _require_build_base_commit(
    connector: Any,
    *,
    build_id: bytes,
    candidate_base: bytes | None,
) -> None:
    row = connector.fetch_one(
        f"SELECT base_receipt_id FROM {_BUILD_BASE_COMMIT_TABLE} WHERE build_id = %s",
        (build_id,),
    )
    build_base = None
    if row:
        if len(row) != 1:
            raise PublicationCorruptionError("source build base commit is malformed")
        build_base = require_uuid16(row[0], field="source build base receipt_id")
    if build_base != candidate_base:
        raise PublicationConflictError(
            "candidate base commit disagrees with its source build base"
        )


def _validate_catalog_revision(
    work: VNextUnitOfWork,
    candidate: _Candidate,
    projection: _ProjectionSeal,
) -> None:
    row = work.connector.fetch_one(
        f"SELECT publication_count FROM {_CATALOG_DESCRIPTOR_TABLE} "
        "WHERE revision = %s",
        (candidate.reserved_revision,),
    )
    if len(row) != 1:
        raise PublicationNotReadyError(
            "reserved catalog revision descriptor is absent or unsealed"
        )
    count = require_int63(row[0], field="catalog revision publication_count")
    if count != projection.publication_count:
        raise PublicationConflictError(
            "catalog revision count disagrees with its projection seal"
        )


def _lock_publication_commit_head(
    work: VNextUnitOfWork, channel: bytes
) -> _LockedHead | None:
    row = work.lock_row(
        LockRank.HEAD,
        encode_lock_key("publication", 0, channel),
        "SELECT registry.channel, head.receipt_id, seal.receipt_id, "
        "catalog.revision, source.source_revision, generation.generation, "
        "committed.committed_at, descriptor.channel "
        "FROM catalog_channel_registry AS registry "
        f"LEFT JOIN {_COMMIT_HEAD_TABLE} AS head "
        "ON head.channel = registry.channel "
        f"LEFT JOIN {_COMMIT_SEAL_TABLE} AS seal "
        "ON seal.receipt_id = head.receipt_id "
        f"LEFT JOIN {_COMMIT_CATALOG_REVISION_TABLE} AS catalog "
        "ON catalog.receipt_id = head.receipt_id "
        f"LEFT JOIN {_COMMIT_SOURCE_REVISION_TABLE} AS source "
        "ON source.receipt_id = head.receipt_id "
        f"LEFT JOIN {_COMMIT_GENERATION_TABLE} AS generation "
        "ON generation.receipt_id = head.receipt_id "
        f"LEFT JOIN {_COMMIT_COMMITTED_AT_TABLE} AS committed "
        "ON committed.receipt_id = head.receipt_id "
        f"LEFT JOIN {_SOURCE_REVISION_CHANNEL_TABLE} AS descriptor "
        "ON descriptor.source_revision = source.source_revision "
        "WHERE registry.channel = %s",
        (channel,),
    )
    if len(row) != 8 or row[0] != channel:
        raise PublicationCorruptionError(
            "publication channel registry row is missing or malformed"
        )
    members = row[1:]
    if all(value is None for value in members):
        return None
    if any(value is None for value in members):
        raise PublicationCorruptionError(
            "common publication head or its sealed commit family is incomplete"
        )
    if row[1] != row[2]:
        raise PublicationCorruptionError("common publication head is not sealed")
    descriptor_channel = require_bounded_bytes(
        row[7], field="head source descriptor channel", minimum=1, maximum=64
    )
    if descriptor_channel != channel:
        raise PublicationCorruptionError(
            "common publication head points to another channel"
        )
    return _LockedHead(
        require_uuid16(row[1], field="head receipt_id"),
        require_positive_int63(row[3], field="head catalog revision"),
        require_positive_int63(row[4], field="head source revision"),
        require_positive_int63(row[5], field="head generation"),
        require_int63(row[6], field="head committed_at"),
    )


def _lock_deletion_generation_head(work: VNextUnitOfWork) -> int:
    row = work.lock_row(
        LockRank.HEAD,
        encode_lock_key("publication", 1),
        f"SELECT current_generation FROM {_DELETION_HEAD_TABLE} "
        "WHERE singleton_id = %s",
        (1,),
    )
    if len(row) != 1:
        raise PublicationCorruptionError("deletion-request generation head is missing")
    return require_int63(row[0], field="current deletion-request generation")


def _require_exact_commit_base(
    pinned_receipt_id: bytes | None,
    actual: _LockedHead | None,
) -> None:
    if pinned_receipt_id is None:
        if actual is not None:
            raise PublicationHeadRaceError(
                "common publication head appeared after genesis reservation"
            )
        return
    if actual is None or actual.receipt_id != pinned_receipt_id:
        raise PublicationHeadRaceError(
            "common publication head changed after candidate reservation"
        )


def _successor_generation(base: _LockedHead | None) -> int:
    if base is None:
        return 1
    if base.generation == INT63_MAX:
        raise PublicationNotReadyError("publication head generation is exhausted")
    return base.generation + 1


def _require_fresh_commit_identities(
    connector: Any,
    *,
    candidate_id: bytes,
    revision: int,
    preparation_id: bytes,
) -> None:
    checks = (
        (_COMMIT_CANDIDATE_TABLE, "candidate_id", candidate_id, "candidate"),
        (_COMMIT_CATALOG_REVISION_TABLE, "revision", revision, "catalog revision"),
        (
            _COMMIT_PREPARATION_TABLE,
            "preparation_id",
            preparation_id,
            "operational preparation",
        ),
    )
    for table, column, value, label in checks:
        if connector.fetch_one(
            f"SELECT receipt_id FROM {table} WHERE {column} = %s", (value,)
        ):
            raise PublicationConflictError(
                f"publication {label} is already bound to another commit"
            )


def _require_available_successor(
    connector: Any,
    *,
    generation: int,
    predecessor_generation: int,
) -> None:
    predecessor = connector.fetch_one(
        f"SELECT generation FROM {_GENERATION_NODE_TABLE} WHERE generation = %s",
        (predecessor_generation,),
    )
    if predecessor != (predecessor_generation,):
        raise PublicationCorruptionError(
            "publication predecessor generation node is missing"
        )
    if connector.fetch_one(
        f"SELECT generation FROM {_GENERATION_NODE_TABLE} WHERE generation = %s",
        (generation,),
    ):
        raise PublicationConflictError(
            "publication successor generation already exists"
        )
    if connector.fetch_one(
        f"SELECT successor_generation FROM {_GENERATION_SUCCESSOR_TABLE} "
        "WHERE predecessor_generation = %s OR successor_generation = %s "
        "ORDER BY successor_generation LIMIT 1",
        (predecessor_generation, generation),
    ):
        raise PublicationHeadRaceError(
            "common publication head already has a durable successor"
        )


def _insert_publication_commit(
    connector: Any,
    *,
    receipt_id: bytes,
    candidate: _Candidate,
    preparation: _Preparation,
    projection: _ProjectionSeal,
    source_revision: int,
    generation: int,
    committed_at: int,
) -> None:
    connector.execute(
        f"INSERT INTO {_COMMIT_ANCHOR_TABLE} (receipt_id) VALUES (%s)",
        (receipt_id,),
    )
    members = (
        (_COMMIT_CANDIDATE_TABLE, "candidate_id", candidate.candidate_id),
        (
            _COMMIT_CATALOG_REVISION_TABLE,
            "revision",
            candidate.reserved_revision,
        ),
        (_COMMIT_SOURCE_REVISION_TABLE, "source_revision", source_revision),
        (_COMMIT_GENERATION_TABLE, "generation", generation),
        (
            _COMMIT_PREPARATION_TABLE,
            "preparation_id",
            preparation.preparation_id,
        ),
        (
            _COMMIT_OPERATIONAL_POLICY_TABLE,
            "operational_policy_id",
            preparation.policy_id,
        ),
        (
            _COMMIT_ARTIFACT_POLICY_TABLE,
            "artifact_policy_id",
            candidate.artifact_policy_id,
        ),
        (
            _COMMIT_DISPLAY_POLICY_TABLE,
            "display_title_policy_id",
            candidate.display_title_policy_id,
        ),
        (_COMMIT_NEW_TABLE, "new_galleries", projection.new_galleries),
        (
            _COMMIT_CHANGED_TABLE,
            "changed_galleries",
            projection.changed_galleries,
        ),
        (
            _COMMIT_REMOVED_TABLE,
            "removed_galleries",
            projection.removed_galleries,
        ),
        (
            _COMMIT_DUPLICATE_TABLE,
            "duplicate_losers",
            projection.duplicate_losers,
        ),
        (_COMMIT_COMMITTED_AT_TABLE, "committed_at", committed_at),
    )
    for table, column, value in members:
        connector.execute(
            f"INSERT INTO {table} (receipt_id, {column}) VALUES (%s, %s)",
            (receipt_id, value),
        )
    # The commit seal has a reverse FK to this total checkpoint seal.  Initialize
    # permanent finalization authority before making the common commit visible.
    _initialize_finalization_checkpoint(
        connector,
        receipt_id=receipt_id,
        initialized_at=committed_at,
    )
    # This reverse-FK seal is the sole publication point for the common commit.
    connector.execute(
        f"INSERT INTO {_COMMIT_SEAL_TABLE} (receipt_id) VALUES (%s)",
        (receipt_id,),
    )


def _advance_publication_commit_head(
    work: VNextUnitOfWork,
    *,
    channel: bytes,
    base: _LockedHead | None,
    receipt_id: bytes,
) -> None:
    if base is None:
        work.connector.execute(
            f"INSERT INTO {_COMMIT_HEAD_TABLE} "
            "(channel, receipt_id) VALUES (%s, %s)",
            (channel, receipt_id),
        )
        return
    work.compare_and_swap(
        f"UPDATE {_COMMIT_HEAD_TABLE} SET receipt_id = %s "
        "WHERE channel = %s AND receipt_id = %s",
        (receipt_id, channel, base.receipt_id),
        authority="common publication head receipt",
    )


def _delete_working_root(
    work: VNextUnitOfWork,
    *,
    table: str,
    label: str,
    slot: int,
    identity_column: str,
    identity: bytes,
) -> None:
    affected = work.connector.execute_affected(
        f"DELETE FROM {table} WHERE slot = %s AND {identity_column} = %s",
        (slot, identity),
    )
    if affected != 1:
        raise PublicationHeadRaceError(f"{label} changed during publication")


def _load_published_commit_by_candidate(
    connector: Any, candidate_id: bytes
) -> _PublishedCommit | None:
    mapping = connector.fetch_one(
        f"SELECT candidate.receipt_id, anchor.receipt_id, seal.receipt_id "
        f"FROM {_COMMIT_CANDIDATE_TABLE} AS candidate "
        f"LEFT JOIN {_COMMIT_ANCHOR_TABLE} AS anchor "
        "ON anchor.receipt_id = candidate.receipt_id "
        f"LEFT JOIN {_COMMIT_SEAL_TABLE} AS seal "
        "ON seal.receipt_id = candidate.receipt_id "
        "WHERE candidate.candidate_id = %s",
        (candidate_id,),
    )
    if not mapping:
        return None
    if len(mapping) != 3 or any(value is None for value in mapping):
        raise PublicationCorruptionError(
            "permanent candidate mapping names an incomplete publication commit"
        )
    receipt_id = require_uuid16(mapping[0], field="stored publication receipt_id")
    if mapping[1] != receipt_id or mapping[2] != receipt_id:
        raise PublicationCorruptionError(
            "permanent candidate mapping disagrees with its anchor or seal"
        )
    return _load_published_commit_by_receipt(connector, receipt_id)


def _load_published_commit_by_receipt(
    connector: Any, receipt_id: bytes
) -> _PublishedCommit:
    row = connector.fetch_one(
        f"SELECT published.receipt_id, published.candidate_id, published.revision, "
        "published.source_revision, published.generation, published.preparation_id, "
        "published.operational_policy_id, published.artifact_policy_id, "
        "published.display_title_policy_id, published.new_galleries, "
        "published.changed_galleries, published.removed_galleries, "
        "published.duplicate_losers, published.committed_at, source.channel, "
        "source.snapshot_manifest_sha256, receipt.publication_count, "
        "receipt.finalized_at "
        f"FROM {_COMMIT_VIEW} AS published "
        f"JOIN {_SOURCE_DESCRIPTOR_TABLE} AS source "
        "ON source.source_revision = published.source_revision "
        f"JOIN {_CATALOG_DESCRIPTOR_TABLE} AS catalog "
        "ON catalog.revision = published.revision "
        f"JOIN {_PUBLICATION_RECEIPT_VIEW} AS receipt "
        "ON receipt.receipt_id = published.receipt_id "
        "AND receipt.publication_count = catalog.publication_count "
        "WHERE published.receipt_id = %s",
        (receipt_id,),
    )
    if len(row) != 18:
        raise PublicationCorruptionError(
            "sealed publication commit or one of its descriptors is malformed"
        )
    finalized_at = (
        None
        if row[17] is None
        else require_int63(row[17], field="stored publication finalized_at")
    )
    try:
        return _PublishedCommit(
            require_uuid16(row[0], field="stored publication receipt_id"),
            require_uuid16(row[1], field="stored publication candidate_id"),
            require_positive_int63(row[2], field="stored catalog revision"),
            require_positive_int63(row[3], field="stored source revision"),
            require_positive_int63(row[4], field="stored publication generation"),
            require_uuid16(row[5], field="stored operational preparation_id"),
            require_positive_int63(row[6], field="stored operational_policy_id"),
            require_positive_int63(row[7], field="stored artifact_policy_id"),
            require_positive_int63(row[8], field="stored display_title_policy_id"),
            require_int63(row[9], field="stored new_galleries"),
            require_int63(row[10], field="stored changed_galleries"),
            require_int63(row[11], field="stored removed_galleries"),
            require_int63(row[12], field="stored duplicate_losers"),
            require_int63(row[13], field="stored publication committed_at"),
            require_bounded_bytes(
                row[14], field="stored publication channel", minimum=1, maximum=64
            ),
            require_digest32(row[15], field="stored source snapshot manifest"),
            require_int63(row[16], field="stored publication_count"),
            finalized_at,
        )
    except (TypeError, ValueError) as error:
        raise PublicationCorruptionError(
            "publication commit contains invalid domain values"
        ) from error


def _validate_published_commit(connector: Any, commit: _PublishedCommit) -> None:
    if commit.finalized_at is not None and commit.finalized_at < commit.committed_at:
        raise PublicationCorruptionError(
            "publication finalization precedes its sealed commit"
        )
    node = connector.fetch_one(
        f"SELECT edge.predecessor_generation "
        f"FROM {_GENERATION_NODE_TABLE} AS node "
        f"JOIN {_GENERATION_SUCCESSOR_TABLE} AS edge "
        "ON edge.successor_generation = node.generation "
        "WHERE node.generation = %s",
        (commit.generation,),
    )
    if node != (commit.generation - 1,):
        raise PublicationCorruptionError(
            "publication commit has a missing or non-contiguous generation edge"
        )
    predecessor_node = connector.fetch_one(
        f"SELECT generation FROM {_GENERATION_NODE_TABLE} WHERE generation = %s",
        (commit.generation - 1,),
    )
    if predecessor_node != (commit.generation - 1,):
        raise PublicationCorruptionError(
            "publication commit predecessor generation node is missing"
        )
    if commit.generation > 1:
        predecessor = connector.fetch_one(
            f"SELECT generation.receipt_id FROM {_COMMIT_GENERATION_TABLE} AS generation "
            f"JOIN {_COMMIT_SEAL_TABLE} AS seal "
            "ON seal.receipt_id = generation.receipt_id "
            "WHERE generation.generation = %s",
            (commit.generation - 1,),
        )
        if len(predecessor) != 1:
            raise PublicationCorruptionError(
                "publication commit predecessor is not a sealed commit"
            )
    activation = connector.fetch_one(
        f"SELECT preparation_id, operational_policy_id, activated_at "
        f"FROM {_ACTIVATION_TABLE} WHERE source_revision = %s",
        (commit.source_revision,),
    )
    if activation != (
        commit.preparation_id,
        commit.operational_policy_id,
        commit.committed_at,
    ):
        raise PublicationCorruptionError(
            "derived operational activation disagrees with its sealed commit"
        )


def _commit_receipt(
    commit: _PublishedCommit, *, replayed: bool
) -> PublicationCommitReceipt:
    return PublicationCommitReceipt(
        commit.candidate_id,
        commit.receipt_id,
        commit.revision,
        commit.source_revision,
        commit.channel,
        commit.artifact_policy_id,
        commit.display_title_policy_id,
        commit.publication_count,
        commit.new_galleries,
        commit.changed_galleries,
        commit.removed_galleries,
        commit.duplicate_losers,
        "PROJECTION_FINALIZED" if commit.finalized_at is not None else "DB_COMMITTED",
        commit.committed_at,
        commit.finalized_at,
        replayed,
    )


def _new_receipt_id() -> bytes:
    return secrets.token_bytes(16)
