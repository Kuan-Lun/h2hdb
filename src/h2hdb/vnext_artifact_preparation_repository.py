"""Bounded artifact projection, byte preparation, and persistence for vNext.

All caller-visible commands carry only durable roots and repository-issued
capabilities.  Component digests, input IDs, cursors, operation kinds, archive
names, byte digests, locators, protection tokens, and projection counts are
derived and independently checked by this module.
"""

from __future__ import annotations

__all__ = [
    "ArtifactPreparationAuthority",
    "ArtifactPreparationConflictError",
    "ArtifactPreparationContractUnavailableError",
    "ArtifactPreparationInputAudit",
    "ArtifactPreparationNotReadyError",
    "ArtifactPreparationRepository",
    "ArtifactPreparationRepositoryError",
    "ArtifactPreparationReceipt",
    "ArtifactProtectionEvidence",
    "ArtifactProtectionIntent",
    "ArtifactPersistenceReceipt",
    "ArtifactInputProjectionAuthority",
    "ArtifactInputProjectionPlan",
    "ArtifactStorageAdapter",
    "ArtifactStorageEvidence",
]

import os
import sqlite3
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory, TemporaryFile
from typing import Any, BinaryIO

from . import vnext_identity as identity
from .domain import (
    ArtifactArchiveRenderEvidence,
    ArtifactSourceRole,
    ArtifactStorageEvidence,
    CatalogResourceKind,
    PreparedPublicationPresentation,
    StorageObjectDescriptor,
    StorageObjectKey,
)
from .ports import ArtifactStorageAdapter
from .sql_connector import DatabaseDuplicateKeyError, SQLConnector
from .vnext_analysis_family import (
    AnalysisFamilyCollisionError,
    load_analysis_run_family,
)
from .vnext_artifact_family import (
    ArtifactFamilyCollisionError,
    ArtifactFamilyPartialError,
    ArtifactSemanticInputFamily,
    CatalogArtifactFamily,
    PreparedArtifactFamily,
    PreparedStorageObjectFamily,
    cas_prepared_artifact_state,
    ensure_artifact_semantic_input_family,
    ensure_catalog_artifact_family,
    ensure_prepared_artifact_family,
    ensure_prepared_storage_object_family,
    load_artifact_semantic_input_family,
    load_catalog_artifact_family,
    load_prepared_artifact_families,
    load_prepared_artifact_family,
    load_prepared_storage_object_family,
)
from .vnext_artifact_presentation import (
    PreparedPresentationArtifact,
    prepare_presentation,
)
from .vnext_artifact_render import (
    ArtifactSourceReference,
    render_artifact,
)
from .vnext_canonical_value_family import (
    load_sealed_value_identities,
)
from .vnext_canonical_value_repository import (
    CanonicalValueCollisionError,
    CanonicalValueNotReadyError,
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from .vnext_catalog_registry_repository import (
    CatalogRegistryError,
    CatalogRegistryNotReadyError,
    load_analysis_policy,
    load_artifact_policy_semantics,
    load_manifest_policy,
    load_source_scope,
)
from .vnext_domains import (
    INT63_MAX,
    microseconds_from_datetime,
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
from .vnext_operational_event_repository import OperationalEffectSeal
from .vnext_publication_candidate_repository import (
    _ARTIFACT_POLICY_DOMAIN,
    PublicationCandidateBatch,
    PublicationCandidateConflictError,
    PublicationCandidateRepository,
    PublicationProjectionAuthority,
    _candidate_batch_from_row,
    _commit_candidate_batch,
    _load_candidate_batch_at_generation,
    _load_projection_authority,
    _MutationAuthority,
    _prepare_candidate_batch,
)
from .vnext_transaction import VNextUnitOfWork

_AUTHORITY_TOKEN = object()
_AUDIT_TOKEN = object()
_MAX_READ_BYTES = 64 * 1024
_MAX_SOURCE_PAGE = 128
_MAX_MEMBER_PLAN_BYTES = 2 * 1024 * 1024
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_PREPARATION_RECEIPT_TOKEN = object()
_PROTECTION_INTENT_TOKEN = object()
_PROTECTION_EVIDENCE_TOKEN = object()
_INPUT_AUTHORITY_TOKEN = object()
_INPUT_PLAN_TOKEN = object()
_INPUT_VALIDATION_PLAN_TOKEN = object()

_CHECKPOINT_TABLE = "catalog_publication_checkpoints"

_SOURCE_FILE_FAMILY_SQL = (
    "FROM (SELECT file_no.gallery_id, file_no.observation_id, file_no.file_key, "
    "file_no.file_no, file_sha.file_sha256, artifact_role.artifact_role "
    "FROM catalog_gallery_observation_file_seals AS file_seal "
    "JOIN catalog_gallery_observation_file_file_nos AS file_no "
    "ON file_no.gallery_id = file_seal.gallery_id "
    "AND file_no.observation_id = file_seal.observation_id "
    "AND file_no.file_key = file_seal.file_key "
    "JOIN catalog_gallery_observation_file_file_sha256s AS file_sha "
    "ON file_sha.gallery_id = file_seal.gallery_id "
    "AND file_sha.observation_id = file_seal.observation_id "
    "AND file_sha.file_key = file_seal.file_key "
    "JOIN catalog_gallery_observation_file_artifact_role AS artifact_role "
    "ON artifact_role.gallery_id = file_seal.gallery_id "
    "AND artifact_role.observation_id = file_seal.observation_id "
    "AND artifact_role.file_key = file_seal.file_key) AS source "
    "JOIN catalog_file_name_identities AS name "
    "ON name.file_key = source.file_key "
)


class ArtifactPreparationRepositoryError(RuntimeError):
    """Base class for artifact-preparation protocol failures."""


class ArtifactPreparationNotReadyError(ArtifactPreparationRepositoryError):
    """A durable prerequisite is absent or not terminal."""


class ArtifactPreparationConflictError(ArtifactPreparationRepositoryError):
    """An immutable artifact input disagrees with its canonical source facts."""


class ArtifactPreparationContractUnavailableError(ArtifactPreparationRepositoryError):
    """The selected adapter cannot execute the registered artifact contract."""


def _registry_record[T](label: str, loader: Callable[[], T]) -> T:
    try:
        return loader()
    except CatalogRegistryNotReadyError as error:
        raise ArtifactPreparationNotReadyError(f"{label} is missing") from error
    except CatalogRegistryError as error:
        raise ArtifactPreparationConflictError(f"{label} is invalid") from error


@dataclass(frozen=True, slots=True)
class ArtifactPreparationAuthority:
    """Repository-issued immutable authority for one CREATE/REBUILD input."""

    projection: PublicationProjectionAuthority
    publication_key: bytes
    gallery_id: int
    gid: int
    gallery_key: bytes
    observation_id: int
    observation_identity_sha256: bytes
    artifact_semantics_sha256: bytes
    operation: str
    source_manifest_component_sha256: bytes
    member_plan_component_sha256: bytes
    effective_content_component_sha256: bytes
    selected_component_sha256: bytes
    owner_component_sha256: bytes
    policy_component_sha256: bytes
    content_sha256: bytes
    owner_gallery_key: bytes
    winner_gallery_key: bytes
    manifest_algorithm_version: int
    file_order_version: int
    artifact_algorithm_version: int
    adapter_id: bytes
    policy_fingerprint_sha256: bytes
    spam_artist_threshold: int
    spam_occurrence_threshold: int
    validation_checkpoint: tuple[int, bytes, int, str, int]
    validation_terminal_receipt: tuple[Any, ...]
    _capability: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._capability is not _AUTHORITY_TOKEN:
            raise TypeError("artifact preparation authorities are repository-issued")
        if not isinstance(self.projection, PublicationProjectionAuthority):
            raise TypeError("artifact preparation authority lacks projection authority")
        require_digest32(self.publication_key, field="artifact publication_key")
        require_positive_int63(self.gallery_id, field="artifact gallery_id")
        require_positive_int63(self.gid, field="artifact gid")
        require_digest32(self.gallery_key, field="artifact gallery_key")
        require_positive_int63(self.observation_id, field="artifact observation_id")
        require_digest32(
            self.observation_identity_sha256,
            field="artifact observation_identity_sha256",
        )
        for digest_label, digest_value in self.component_tuple:
            require_digest32(digest_value, field=digest_label)
        for version_label, version_value in (
            ("manifest_algorithm_version", self.manifest_algorithm_version),
            ("file_order_version", self.file_order_version),
            ("artifact_algorithm_version", self.artifact_algorithm_version),
        ):
            require_positive_int63(
                version_value,
                field=f"artifact {version_label}",
            )
        for threshold_label, threshold_value in (
            ("spam_artist_threshold", self.spam_artist_threshold),
            ("spam_occurrence_threshold", self.spam_occurrence_threshold),
        ):
            require_int63(
                threshold_value,
                field=f"artifact {threshold_label}",
            )
        require_bounded_bytes(
            self.adapter_id,
            field="artifact adapter_id",
            minimum=1,
            maximum=64,
        )
        require_digest32(
            self.policy_fingerprint_sha256,
            field="artifact policy_fingerprint_sha256",
        )
        if self.operation not in {"CREATE", "REBUILD"}:
            raise ValueError("artifact preparation operation must create bytes")
        _validate_checkpoint(self.validation_checkpoint)
        _validate_terminal_receipt(
            self.validation_terminal_receipt,
            checkpoint=self.validation_checkpoint,
        )

    @property
    def candidate_id(self) -> bytes:
        return self.projection.candidate_id

    @property
    def analysis_id(self) -> bytes:
        return self.projection.analysis_id

    @property
    def build_id(self) -> bytes:
        return self.projection.build_id

    @property
    def component_tuple(self) -> tuple[tuple[str, bytes], ...]:
        return (
            ("artifact_semantics_sha256", self.artifact_semantics_sha256),
            (
                "source_manifest_component_sha256",
                self.source_manifest_component_sha256,
            ),
            ("member_plan_component_sha256", self.member_plan_component_sha256),
            (
                "effective_content_component_sha256",
                self.effective_content_component_sha256,
            ),
            ("selected_component_sha256", self.selected_component_sha256),
            ("owner_component_sha256", self.owner_component_sha256),
            ("policy_component_sha256", self.policy_component_sha256),
            ("content_sha256", self.content_sha256),
            ("owner_gallery_key", self.owner_gallery_key),
            ("winner_gallery_key", self.winner_gallery_key),
        )


@dataclass(frozen=True, slots=True)
class ArtifactPreparationInputAudit:
    """Diagnostic proof of every currently executable artifact input fact.

    This is deliberately not a storage or prepared-artifact authority by
    itself.  The storage boundary separately requires the exact registered
    adapter ID and opaque policy fingerprint before it consumes this receipt.
    """

    authority: ArtifactPreparationAuthority
    source_entry_count: int
    emitted_member_count: int
    source_byte_count: int
    effective_content_file_count: int
    source_root_components: tuple[str, ...]
    gallery_locator_components: tuple[str, ...]
    references: tuple[ArtifactSourceReference, ...]
    _capability: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._capability is not _AUDIT_TOKEN:
            raise TypeError("artifact input audits are repository-issued")
        if self.authority._capability is not _AUTHORITY_TOKEN:
            raise TypeError("artifact input audit has a forged authority")
        for label, value in (
            ("source_entry_count", self.source_entry_count),
            ("emitted_member_count", self.emitted_member_count),
            ("source_byte_count", self.source_byte_count),
            ("effective_content_file_count", self.effective_content_file_count),
        ):
            require_int63(value, field=f"artifact audit {label}")
        object.__setattr__(
            self, "source_root_components", tuple(self.source_root_components)
        )
        object.__setattr__(
            self,
            "gallery_locator_components",
            tuple(self.gallery_locator_components),
        )
        object.__setattr__(self, "references", tuple(self.references))
        if len(self.references) != self.emitted_member_count:
            raise ValueError("artifact audit references disagree with emitted count")
        if not self.source_root_components or not self.gallery_locator_components:
            raise ValueError("artifact audit requires source root and gallery locator")


@dataclass(frozen=True, slots=True)
class ArtifactProtectionIntent:
    """Repository-issued durable intent for one generic immutable resource."""

    candidate_id: bytes
    publication_key: bytes
    resource_kind: CatalogResourceKind
    storage_object: StorageObjectDescriptor
    storage_object_key_sha256: bytes
    storage_generation: int
    protection_token: bytes
    state: str
    replayed: bool
    _capability: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._capability is not _PROTECTION_INTENT_TOKEN:
            raise TypeError("artifact protection intents are repository-issued")
        candidate = require_uuid16(self.candidate_id, field="intent candidate_id")
        publication = require_digest32(
            self.publication_key,
            field="intent publication_key",
        )
        if type(self.resource_kind) is not CatalogResourceKind:
            raise TypeError("intent resource_kind is not registered")
        if not isinstance(self.storage_object, StorageObjectDescriptor):
            raise TypeError("intent storage_object is not registered")
        key_digest = require_digest32(
            self.storage_object_key_sha256,
            field="intent storage_object_key_sha256",
        )
        if (
            identity.artifact_storage_key_digest(
                self.storage_object.key.codec,
                self.storage_object.key.segments,
            )
            != key_digest
        ):
            raise ValueError("intent storage-key digest disagrees")
        generation = require_int63(
            self.storage_generation,
            field="intent storage_generation",
        )
        token = require_digest32(
            self.protection_token,
            field="intent protection_token",
        )
        if token != identity.encode_artifact_protection_token(
            candidate,
            publication,
            self.resource_kind.value,
            key_digest,
            generation,
        ):
            raise ValueError("intent token disagrees with durable facts")
        if self.state not in {"PENDING", "PREPARED", "COMMITTED"}:
            raise ValueError("artifact protection intent state is not registered")
        if type(self.replayed) is not bool:
            raise TypeError("artifact protection intent replayed must be bool")


@dataclass(frozen=True, slots=True)
class ArtifactProtectionEvidence:
    """Core-verified acknowledgement of one exact protected resource."""

    intent: ArtifactProtectionIntent
    adapter_id: bytes
    policy_fingerprint_sha256: bytes
    storage_object: StorageObjectDescriptor
    _capability: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._capability is not _PROTECTION_EVIDENCE_TOKEN:
            raise TypeError("artifact protection evidence is repository-issued")
        if not isinstance(self.intent, ArtifactProtectionIntent):
            raise TypeError("artifact protection evidence lacks its durable intent")
        self.intent.__post_init__()
        require_bounded_bytes(
            self.adapter_id,
            field="protection evidence adapter_id",
            minimum=1,
            maximum=64,
        )
        require_digest32(
            self.policy_fingerprint_sha256,
            field="protection evidence policy fingerprint",
        )
        if self.storage_object != self.intent.storage_object:
            raise ValueError(
                "protected storage descriptor differs from core-verified bytes"
            )


class ArtifactPreparationReceipt:
    """Owned verified acquisition and optional thumbnail resource bundle."""

    __slots__ = (
        "audit",
        "_archive",
        "_artifact_storage_key_sha256",
        "_capability",
        "_closed",
        "_presentation_artifact",
        "_render_evidence",
        "_storage_object",
    )

    def __init__(
        self,
        *,
        audit: ArtifactPreparationInputAudit,
        acquisition: StorageObjectDescriptor,
        render_evidence: ArtifactArchiveRenderEvidence,
        presentation_artifact: PreparedPresentationArtifact,
        archive: BinaryIO,
        _capability: object,
    ) -> None:
        if _capability is not _PREPARATION_RECEIPT_TOKEN:
            raise TypeError("artifact preparation receipts are repository-issued")
        if audit._capability is not _AUDIT_TOKEN:
            raise TypeError("artifact preparation receipt lacks an input audit")
        if not isinstance(acquisition, StorageObjectDescriptor):
            raise TypeError("prepared acquisition descriptor is invalid")
        if not isinstance(render_evidence, ArtifactArchiveRenderEvidence):
            raise TypeError("prepared render evidence is invalid")
        render_evidence.__post_init__()
        if (
            acquisition.sha256 != render_evidence.artifact_sha256.hex()
            or acquisition.size_bytes != render_evidence.size_bytes
        ):
            raise ValueError("render evidence disagrees with acquisition descriptor")
        if not isinstance(presentation_artifact, PreparedPresentationArtifact):
            raise TypeError("prepared presentation ownership is invalid")
        for page in presentation_artifact.presentation.pages:
            if page.storage_object != acquisition:
                raise ValueError("prepared page references another acquisition object")
        if not hasattr(archive, "read") or not hasattr(archive, "seek"):
            raise TypeError("prepared archive must be seekable")
        self.audit = audit
        self._storage_object = acquisition
        self._artifact_storage_key_sha256 = identity.artifact_storage_key_digest(
            acquisition.key.codec,
            acquisition.key.segments,
        )
        self._render_evidence = render_evidence
        self._presentation_artifact = presentation_artifact
        self._archive = archive
        self._capability = _capability
        self._closed = False

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                self._archive.close()
            finally:
                self._presentation_artifact.close()

    def __enter__(self) -> ArtifactPreparationReceipt:
        if self._closed:
            raise ValueError("artifact preparation receipt is closed")
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    @property
    def artifact_sha256(self) -> bytes:
        return bytes.fromhex(self._storage_object.sha256)

    @property
    def size_bytes(self) -> int:
        return self._storage_object.size_bytes

    @property
    def storage_key(self) -> StorageObjectKey:
        return self._storage_object.key

    @property
    def storage_object(self) -> StorageObjectDescriptor:
        return self._storage_object

    @property
    def artifact_storage_key_sha256(self) -> bytes:
        return self._artifact_storage_key_sha256

    @property
    def render_evidence(self) -> ArtifactArchiveRenderEvidence:
        return self._render_evidence

    @property
    def presentation(self) -> PreparedPublicationPresentation:
        return self._presentation_artifact.presentation

    @property
    def resource_kinds(self) -> tuple[CatalogResourceKind, ...]:
        if self.presentation.thumbnail is None:
            return (CatalogResourceKind.ACQUISITION,)
        return (
            CatalogResourceKind.ACQUISITION,
            CatalogResourceKind.THUMBNAIL,
        )

    def resource_descriptor(
        self,
        resource_kind: CatalogResourceKind,
    ) -> StorageObjectDescriptor:
        self._require_open()
        if resource_kind is CatalogResourceKind.ACQUISITION:
            return self._storage_object
        if resource_kind is CatalogResourceKind.THUMBNAIL:
            thumbnail = self.presentation.thumbnail
            if thumbnail is None:
                raise KeyError("prepared artifact has no thumbnail resource")
            return thumbnail.storage_object
        raise TypeError("resource_kind is not registered")

    def resource_stream(self, resource_kind: CatalogResourceKind) -> BinaryIO:
        self._require_open()
        if resource_kind is CatalogResourceKind.ACQUISITION:
            stream = self._archive
        elif resource_kind is CatalogResourceKind.THUMBNAIL:
            if self.presentation.thumbnail is None:
                raise KeyError("prepared artifact has no thumbnail resource")
            stream = self._presentation_artifact.thumbnail
        else:
            raise TypeError("resource_kind is not registered")
        stream.seek(0)
        return stream

    def _require_open(self) -> None:
        if self._closed:
            raise TypeError("artifact preparation receipt is closed")


@dataclass(frozen=True, slots=True)
class ArtifactPersistenceReceipt:
    candidate_id: bytes
    publication_key: bytes
    artifact_sha256: bytes
    resources: tuple[tuple[CatalogResourceKind, bytes, str], ...]
    replayed: bool

    def __post_init__(self) -> None:
        require_uuid16(self.candidate_id, field="persisted artifact candidate_id")
        require_digest32(self.publication_key, field="persisted publication_key")
        require_digest32(self.artifact_sha256, field="persisted artifact_sha256")
        object.__setattr__(self, "resources", tuple(self.resources))
        kinds: list[str] = []
        for kind, token, state in self.resources:
            if type(kind) is not CatalogResourceKind:
                raise TypeError("persisted artifact resource_kind is not registered")
            require_digest32(token, field="persisted protection_token")
            if state not in {"PREPARED", "COMMITTED"}:
                raise ValueError("persisted artifact state is not registered")
            kinds.append(kind.value)
        if tuple(kinds) != tuple(sorted(set(kinds))):
            raise ValueError("persisted artifact resources are not strictly ordered")
        if type(self.replayed) is not bool:
            raise TypeError("persisted artifact replayed must be bool")


@dataclass(frozen=True, slots=True)
class ArtifactInputProjectionAuthority:
    """Repository-issued authority for stages 05 through 07."""

    projection: PublicationProjectionAuthority
    catalog_checkpoint: tuple[int, bytes, int, str, int]
    catalog_terminal_receipt: tuple[Any, ...]
    _capability: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._capability is not _INPUT_AUTHORITY_TOKEN:
            raise TypeError("artifact input authorities are repository-issued")
        if not isinstance(self.projection, PublicationProjectionAuthority):
            raise TypeError("artifact input authority lacks projection authority")
        _validate_checkpoint(self.catalog_checkpoint, cursor_maximum=2048)
        if self.catalog_checkpoint[3] != "COMPLETE":
            raise ValueError("catalog validation checkpoint is not COMPLETE")
        _validate_terminal_receipt(
            self.catalog_terminal_receipt,
            checkpoint=self.catalog_checkpoint,
            cursor_maximum=2048,
        )


class ArtifactInputProjectionPlan:
    """Opaque disk-backed artifact-input projection or independent evaluator."""

    def __init__(
        self,
        *,
        authority: ArtifactInputProjectionAuthority,
        database: sqlite3.Connection,
        payload: BinaryIO,
        temporary_directory: TemporaryDirectory[str],
        input_count: int,
        validation: bool,
        _capability: object,
    ) -> None:
        expected = _INPUT_VALIDATION_PLAN_TOKEN if validation else _INPUT_PLAN_TOKEN
        if _capability is not expected:
            raise TypeError("artifact input plans are repository-issued")
        self.authority = authority
        self.input_count = require_int63(input_count, field="artifact input plan count")
        self.validation = validation
        self._database = database
        self._payload = payload
        self._temporary_directory = temporary_directory
        self._capability = _capability
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._database.close()
        finally:
            try:
                self._payload.close()
            finally:
                self._temporary_directory.cleanup()

    def __enter__(self) -> ArtifactInputProjectionPlan:
        self._require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def iter_canonical_value_plans(self) -> Iterator[CanonicalValueUploadPlan]:
        """Yield one replayable bounded canonical upload plan at a time."""

        self._require_open()
        cursor = self._database.execute(
            "SELECT value_sha256, digest_domain, payload_offset, byte_count "
            "FROM canonical_values ORDER BY value_sha256"
        )
        while rows := cursor.fetchmany(_MAX_SOURCE_PAGE):
            for value, domain, offset, byte_count in rows:
                plan = CanonicalValueUploadPlan.from_parts(
                    bytes(domain).decode("ascii", errors="strict"),
                    _iter_file_range(
                        self._payload,
                        require_int63(offset, field="artifact payload offset"),
                        require_int63(byte_count, field="artifact payload count"),
                    ),
                )
                if plan.value_sha256 != bytes(value):
                    plan.close()
                    raise ArtifactPreparationConflictError(
                        "artifact canonical spool differs from its planned digest"
                    )
                yield plan

    def _canonical_consumer_cursor(self, value_sha256: bytes) -> bytes:
        """Return the immutable first-consumer key for restart recovery."""

        self._require_open()
        value = require_digest32(
            value_sha256,
            field="artifact canonical consumer value_sha256",
        )
        row = self._database.execute(
            "SELECT consumer_key FROM canonical_values WHERE value_sha256 = ?",
            (sqlite3.Binary(value),),
        ).fetchone()
        if row is None or len(row) != 1:
            raise ArtifactPreparationConflictError(
                "artifact canonical plan lacks its first consumer"
            )
        return require_digest32(row[0], field="artifact canonical consumer key")

    def _page_after(self, cursor: bytes) -> tuple[tuple[Any, ...], ...]:
        self._require_open()
        _validate_publication_cursor(cursor)
        rows = self._database.execute(
            "SELECT publication_key, artifact_semantics_sha256, "
            "source_manifest_component_sha256, member_plan_component_sha256, "
            "effective_content_component_sha256, selected_component_sha256, "
            "owner_component_sha256, policy_component_sha256 "
            "FROM inputs WHERE publication_key > ? "
            "ORDER BY publication_key LIMIT 128",
            (sqlite3.Binary(cursor),),
        ).fetchall()
        return tuple(tuple(row) for row in rows)

    def _require_open(self) -> None:
        if self._closed:
            raise ArtifactPreparationNotReadyError(
                "artifact input plan is already closed"
            )


@dataclass(frozen=True, slots=True)
class _AuthorityFacts:
    publication_key: bytes
    gallery_id: int
    gid: int
    gallery_key: bytes
    observation_id: int
    observation_identity_sha256: bytes
    artifact_semantics_sha256: bytes
    operation: str
    source_manifest_component_sha256: bytes
    member_plan_component_sha256: bytes
    effective_content_component_sha256: bytes
    selected_component_sha256: bytes
    owner_component_sha256: bytes
    policy_component_sha256: bytes
    content_sha256: bytes
    owner_gallery_key: bytes
    winner_gallery_key: bytes
    manifest_algorithm_version: int
    file_order_version: int
    artifact_algorithm_version: int
    adapter_id: bytes
    policy_fingerprint_sha256: bytes
    spam_artist_threshold: int
    spam_occurrence_threshold: int
    validation_checkpoint: tuple[int, bytes, int, str, int]
    validation_terminal_receipt: tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class _ArtifactContractFacts:
    scope_key: bytes
    source_provider: bytes
    source_root_sha256: bytes
    identity_policy_version: int
    manifest_algorithm_version: int
    file_order_version: int
    analysis_algorithm_version: int
    spam_artist_threshold: int
    spam_occurrence_threshold: int
    content_owner_rule_version: int
    gid_winner_rule_version: int
    policy_component_sha256: bytes
    artifact_algorithm_version: int
    adapter_id: bytes
    policy_fingerprint_sha256: bytes


class ArtifactPreparationRepository:
    """Issue exact input authority and audit the canonical member projection."""

    @staticmethod
    def issue_input_projection_authority(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        candidate_id: bytes,
        now: int,
    ) -> ArtifactInputProjectionAuthority:
        """Pin terminal catalog validation before any high-card input work."""

        projection = PublicationCandidateRepository.issue_projection_authority(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            candidate_id=candidate_id,
            now=now,
        )
        return _input_projection_authority(work, projection)

    @staticmethod
    def _issue_input_projection_authority_authorized(
        work: VNextUnitOfWork,
        *,
        candidate_id: bytes,
        generation: int,
        now: int,
    ) -> ArtifactInputProjectionAuthority:
        """Issue after an application transaction already validated its fence."""

        projection = (
            PublicationCandidateRepository._issue_projection_authority_authorized(
                work,
                candidate_id=candidate_id,
                generation=generation,
                now=now,
                validate_artifact_policy=True,
            )
        )
        return _input_projection_authority(work, projection)

    @staticmethod
    def prepare_artifact_input_projection(
        connector: SQLConnector,
        *,
        backend: str,
        authority: ArtifactInputProjectionAuthority,
    ) -> ArtifactInputProjectionPlan:
        """Build the complete desired artifact-input plan outside a write tx."""

        return _prepare_artifact_input_plan(
            connector,
            backend=backend,
            authority=authority,
            validation=False,
        )

    @staticmethod
    def prepare_artifact_input_validation(
        connector: SQLConnector,
        *,
        backend: str,
        authority: ArtifactInputProjectionAuthority,
    ) -> ArtifactInputProjectionPlan:
        """Independently rebuild stage-07 expected input rows from source roots."""

        return _prepare_artifact_input_plan(
            connector,
            backend=backend,
            authority=authority,
            validation=True,
        )

    @staticmethod
    def process_artifact_input_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        candidate_id: bytes,
        plan: ArtifactInputProjectionPlan,
        batch_key: bytes,
        now: int,
    ) -> PublicationCandidateBatch:
        """Persist at most 128 exact six-component artifact inputs."""

        mutation, checkpoint, attempt, replay = _prepare_candidate_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            candidate_id=candidate_id,
            stage=b"BUILD_ARTIFACT_INPUT",
            batch_key=batch_key,
            now=now,
        )
        if replay is not None:
            return replay
        _require_input_plan(work, mutation, plan, validation=False)
        rows = plan._page_after(checkpoint.cursor)
        _lock_input_upload_claims(work, plan, rows)
        for row in rows:
            _persist_artifact_input(work, mutation, plan, row)
        next_cursor = checkpoint.cursor if not rows else bytes(rows[-1][0])
        return _commit_candidate_batch(
            work,
            authority=mutation,
            checkpoint=checkpoint,
            stage=b"BUILD_ARTIFACT_INPUT",
            batch_key=attempt,
            next_cursor=next_cursor,
            row_count=len(rows),
            terminal=not rows,
            now=now,
        )

    @staticmethod
    def process_artifact_delta_operation_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        candidate_id: bytes,
        batch_key: bytes,
        now: int,
    ) -> PublicationCandidateBatch:
        """Materialize one bounded page of exact old/new operation facts."""

        mutation, checkpoint, attempt, replay = _prepare_candidate_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            candidate_id=candidate_id,
            stage=b"BUILD_ARTIFACT_DELTA_OPERATION",
            batch_key=batch_key,
            now=now,
        )
        if replay is not None:
            return replay
        keys = _derive_delta_keys(work, mutation, after=checkpoint.cursor)
        for publication in keys:
            _materialize_delta_key(work, mutation, publication)
        next_cursor = checkpoint.cursor if not keys else keys[-1]
        return _commit_candidate_batch(
            work,
            authority=mutation,
            checkpoint=checkpoint,
            stage=b"BUILD_ARTIFACT_DELTA_OPERATION",
            batch_key=attempt,
            next_cursor=next_cursor,
            row_count=len(keys),
            terminal=not keys,
            now=now,
        )

    @staticmethod
    def validate_artifact_input_delta_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        candidate_id: bytes,
        validation: ArtifactInputProjectionPlan,
        batch_key: bytes,
        now: int,
    ) -> PublicationCandidateBatch:
        """Merge-compare exact artifact inputs against an independent plan."""

        mutation, checkpoint, attempt, replay = _prepare_candidate_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            candidate_id=candidate_id,
            stage=b"VALIDATE_ARTIFACT_INPUT_DELTA",
            batch_key=batch_key,
            now=now,
        )
        if replay is not None:
            return replay
        _require_input_plan(work, mutation, validation, validation=True)
        expected = validation._page_after(checkpoint.cursor)
        actual = work.connector.fetch_all(
            "SELECT publication_key, artifact_semantics_sha256 "
            "FROM catalog_candidate_artifact_inputs "
            "WHERE candidate_id = %s AND publication_key > %s "
            "ORDER BY publication_key LIMIT 128",
            (mutation.candidate.candidate_id, checkpoint.cursor),
        )
        expected_input = tuple((bytes(row[0]), bytes(row[1])) for row in expected)
        if tuple(actual) != expected_input:
            raise ArtifactPreparationConflictError(
                "artifact input rows differ from the independent evaluator"
            )
        for row in expected:
            _compare_delta_for_input(work, mutation, row)
        next_cursor = checkpoint.cursor if not expected else bytes(expected[-1][0])
        return _commit_candidate_batch(
            work,
            authority=mutation,
            checkpoint=checkpoint,
            stage=b"VALIDATE_ARTIFACT_INPUT_DELTA",
            batch_key=attempt,
            next_cursor=next_cursor,
            row_count=len(expected),
            terminal=not expected,
            now=now,
        )

    @staticmethod
    def validate_prepared_artifact_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        candidate_id: bytes,
        batch_key: bytes,
        now: int,
    ) -> PublicationCandidateBatch:
        """Prove exact prepared coverage for CREATE and REBUILD operations."""

        mutation, checkpoint, attempt, replay = _prepare_candidate_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            candidate_id=candidate_id,
            stage=b"VALIDATE_PREPARED_ARTIFACT",
            batch_key=batch_key,
            now=now,
        )
        if replay is not None:
            return replay
        expected = _operation_keys(
            work,
            mutation,
            operations=("CREATE", "REBUILD"),
            after=checkpoint.cursor,
        )
        actual_rows = work.connector.fetch_all(
            "SELECT publication_key FROM catalog_prepared_artifacts "
            "WHERE candidate_id = %s AND publication_key > %s "
            "AND resource_kind = %s "
            "AND state IN ('PREPARED', 'COMMITTED') "
            "ORDER BY publication_key LIMIT 128",
            (
                mutation.candidate.candidate_id,
                checkpoint.cursor,
                CatalogResourceKind.ACQUISITION.value.encode("ascii"),
            ),
        )
        actual = tuple(
            require_digest32(row[0], field="prepared publication")
            for row in actual_rows
        )
        if actual != expected:
            raise ArtifactPreparationConflictError(
                "prepared artifact coverage differs from CREATE/REBUILD"
            )
        for publication in expected:
            _validate_prepared_row(
                work,
                mutation,
                publication,
            )
        next_cursor = checkpoint.cursor if not expected else expected[-1]
        return _commit_candidate_batch(
            work,
            authority=mutation,
            checkpoint=checkpoint,
            stage=b"VALIDATE_PREPARED_ARTIFACT",
            batch_key=attempt,
            next_cursor=next_cursor,
            row_count=len(expected),
            terminal=not expected,
            now=now,
        )

    @staticmethod
    def validate_create_batch(**kwargs: Any) -> PublicationCandidateBatch:
        return _validate_operation_batch(
            stage=b"VALIDATE_CREATE", operation="CREATE", **kwargs
        )

    @staticmethod
    def validate_rebuild_batch(**kwargs: Any) -> PublicationCandidateBatch:
        return _validate_operation_batch(
            stage=b"VALIDATE_REBUILD", operation="REBUILD", **kwargs
        )

    @staticmethod
    def validate_delete_batch(**kwargs: Any) -> PublicationCandidateBatch:
        return _validate_operation_batch(
            stage=b"VALIDATE_DELETE", operation="DELETE", **kwargs
        )

    @staticmethod
    def validate_unchanged_batch(**kwargs: Any) -> PublicationCandidateBatch:
        return _validate_operation_batch(
            stage=b"VALIDATE_UNCHANGED", operation="UNCHANGED", **kwargs
        )

    @staticmethod
    def validate_new_gallery_batch(**kwargs: Any) -> PublicationCandidateBatch:
        return _validate_gallery_diff_batch(
            stage=b"VALIDATE_NEW_GALLERY", kind="NEW", **kwargs
        )

    @staticmethod
    def validate_changed_gallery_batch(**kwargs: Any) -> PublicationCandidateBatch:
        return _validate_gallery_diff_batch(
            stage=b"VALIDATE_CHANGED_GALLERY", kind="CHANGED", **kwargs
        )

    @staticmethod
    def validate_removed_gallery_batch(**kwargs: Any) -> PublicationCandidateBatch:
        return _validate_gallery_diff_batch(
            stage=b"VALIDATE_REMOVED_GALLERY", kind="REMOVED", **kwargs
        )

    @staticmethod
    def validate_duplicate_loser_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        candidate_id: bytes,
        batch_key: bytes,
        now: int,
    ) -> PublicationCandidateBatch:
        mutation, checkpoint, attempt, replay = _prepare_candidate_batch(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            candidate_id=candidate_id,
            stage=b"VALIDATE_DUPLICATE_LOSER",
            batch_key=batch_key,
            now=now,
        )
        if replay is not None:
            return replay
        after = 0 if not checkpoint.cursor else int.from_bytes(checkpoint.cursor, "big")
        rows = work.connector.fetch_all(
            "SELECT member.gallery_id FROM catalog_source_build_galleries member "
            "LEFT JOIN catalog_publication_selections selection "
            "ON selection.candidate_id = %s "
            "AND selection.gallery_id = member.gallery_id "
            "WHERE member.build_id = %s AND member.gallery_id > %s "
            "AND selection.gallery_id IS NULL "
            "ORDER BY member.gallery_id LIMIT 128",
            (mutation.candidate.candidate_id, mutation.begin.build_id, after),
        )
        galleries = tuple(
            require_positive_int63(row[0], field="duplicate-loser gallery_id")
            for row in rows
        )
        next_cursor = (
            checkpoint.cursor if not galleries else galleries[-1].to_bytes(8, "big")
        )
        result = _commit_candidate_batch(
            work,
            authority=mutation,
            checkpoint=checkpoint,
            stage=b"VALIDATE_DUPLICATE_LOSER",
            batch_key=attempt,
            next_cursor=next_cursor,
            row_count=len(galleries),
            terminal=not galleries,
            now=now,
        )
        if result.terminal:
            _seal_projection(work, mutation, now=now)
        return result

    @staticmethod
    def issue_authority(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        candidate_id: bytes,
        publication_key: bytes,
        now: int,
    ) -> ArtifactPreparationAuthority:
        """Capture one exact CREATE/REBUILD input in a short fenced transaction."""

        candidate = require_uuid16(candidate_id, field="artifact candidate_id")
        publication = require_digest32(
            publication_key,
            field="artifact publication_key",
        )
        timestamp = require_int63(now, field="artifact authority now")
        projection = (
            PublicationCandidateRepository._issue_artifact_projection_authority(
                work,
                gate_lease=gate_lease,
                ingest_turn=ingest_turn,
                candidate_id=candidate,
                now=timestamp,
            )
        )
        return _artifact_authority(work, projection, publication, now=timestamp)

    @staticmethod
    def _issue_authority_authorized(
        work: VNextUnitOfWork,
        *,
        candidate_id: bytes,
        publication_key: bytes,
        generation: int,
        now: int,
    ) -> ArtifactPreparationAuthority:
        """Issue after an application transaction already validated its fence."""

        candidate = require_uuid16(candidate_id, field="artifact candidate_id")
        publication = require_digest32(
            publication_key,
            field="artifact publication_key",
        )
        timestamp = require_int63(now, field="artifact authority now")
        projection = (
            PublicationCandidateRepository._issue_projection_authority_authorized(
                work,
                candidate_id=candidate,
                generation=generation,
                now=timestamp,
                validate_artifact_policy=False,
            )
        )
        return _artifact_authority(work, projection, publication, now=timestamp)

    @staticmethod
    def audit_inputs(
        connector: SQLConnector,
        *,
        backend: str,
        authority: ArtifactPreparationAuthority,
    ) -> ArtifactPreparationInputAudit:
        """Stream and independently compare every currently executable input."""

        _require_authority(authority)
        with connector.read_transaction():
            work = VNextUnitOfWork(connector, backend=backend)
            _load_projection_authority(work, authority.projection)
            current = _load_authority_facts(
                work,
                authority.projection,
                authority.publication_key,
                now=None,
            )
            if current != _facts_from_authority(authority):
                raise ArtifactPreparationConflictError(
                    "artifact preparation authority changed before input audit"
                )
            _validate_fixed_components(work, authority)
            return _audit_member_and_effective_components(work, authority)

    @staticmethod
    def prepare_with_storage_adapter(
        connector: SQLConnector,
        *,
        backend: str,
        audit: ArtifactPreparationInputAudit,
        adapter: ArtifactStorageAdapter,
    ) -> ArtifactPreparationReceipt:
        """Render and core-verify one acquisition plus optional thumbnail."""

        if not isinstance(audit, ArtifactPreparationInputAudit):
            raise TypeError("audit must be ArtifactPreparationInputAudit")
        audit.__post_init__()
        _require_authority(audit.authority)
        if audit._capability is not _AUDIT_TOKEN:
            raise TypeError("artifact input audit is not repository-issued")
        if not isinstance(adapter, ArtifactStorageAdapter):
            raise TypeError("adapter must implement ArtifactStorageAdapter")
        authority = audit.authority
        _require_matching_adapter(authority, adapter)
        with connector.read_transaction():
            row = connector.fetch_one(
                "SELECT modified_at FROM catalog_publications "
                "WHERE revision = %s AND publication_key = %s",
                (
                    authority.projection.reserved_revision,
                    authority.publication_key,
                ),
            )
        if len(row) != 1:
            raise ArtifactPreparationConflictError(
                "prepared artifact lacks its immutable publication timestamp"
            )
        modified_at = _datetime_from_microseconds(
            row[0],
            field="prepared storage object modified_at",
        )
        rendered = render_artifact(
            adapter,
            gid=authority.gid,
            source_root_components=audit.source_root_components,
            gallery_locator_components=audit.gallery_locator_components,
            references=audit.references,
        )
        archive: BinaryIO | None = None
        presentation_artifact: PreparedPresentationArtifact | None = None
        try:
            acquisition_key = _adapter_storage_key(
                adapter,
                gid=authority.gid,
                resource_kind=CatalogResourceKind.ACQUISITION,
            )
            acquisition = StorageObjectDescriptor(
                acquisition_key,
                rendered.evidence.size_bytes,
                rendered.evidence.artifact_sha256.hex(),
                modified_at,
            )
            thumbnail_key = (
                None
                if not rendered.evidence.pages
                else _adapter_storage_key(
                    adapter,
                    gid=authority.gid,
                    resource_kind=CatalogResourceKind.THUMBNAIL,
                )
            )
            archive = rendered.detach_archive()
            presentation_artifact = prepare_presentation(
                adapter,
                archive=archive,
                acquisition=acquisition,
                rendered_pages=rendered.evidence.pages,
                thumbnail_key=thumbnail_key,
                modified_at=modified_at,
            )
            receipt = ArtifactPreparationReceipt(
                audit=audit,
                acquisition=acquisition,
                render_evidence=rendered.evidence,
                presentation_artifact=presentation_artifact,
                archive=archive,
                _capability=_PREPARATION_RECEIPT_TOKEN,
            )
            archive = None
            presentation_artifact = None
            return receipt
        finally:
            rendered.close()
            if archive is not None:
                archive.close()
            if presentation_artifact is not None:
                presentation_artifact.close()

    @staticmethod
    def protect_prepared_artifact(
        connector: SQLConnector,
        *,
        backend: str,
        receipt: ArtifactPreparationReceipt,
        intent: ArtifactProtectionIntent,
        adapter: ArtifactStorageAdapter,
    ) -> ArtifactProtectionEvidence:
        """Protect one durable PENDING resource and reverify its exact bytes."""

        _require_preparation_receipt(receipt)
        if not isinstance(intent, ArtifactProtectionIntent):
            raise TypeError("intent must be ArtifactProtectionIntent")
        intent.__post_init__()
        authority = receipt.audit.authority
        _require_matching_adapter(authority, adapter)
        with connector.read_transaction():
            family = _load_prepared_family_or_conflict(
                connector,
                candidate_id=authority.candidate_id,
                publication_key=authority.publication_key,
                resource_kind=intent.resource_kind,
                backend=backend,
            )
            if family is None:
                raise ArtifactPreparationNotReadyError(
                    "resource protection intent is not durably visible"
                )
            durable = _protection_intent_from_family(
                connector,
                receipt,
                family,
                replayed=True,
            )
            _validate_prepared_resource_blob(connector, receipt, family)
        if family.state != "PENDING":
            raise ArtifactPreparationNotReadyError(
                "resource protection requires a durable PENDING intent"
            )
        if _intent_facts(durable) != _intent_facts(intent):
            raise ArtifactPreparationConflictError(
                "resource protection request differs from durable intent"
            )
        stream = receipt.resource_stream(intent.resource_kind)
        expected = (
            bytes.fromhex(intent.storage_object.sha256),
            intent.storage_object.size_bytes,
        )
        if _hash_stream(stream) != expected:
            raise ArtifactPreparationConflictError(
                "prepared resource bytes changed before protection"
            )
        stream.seek(0)
        raw = adapter.protect(
            stream,
            intent.storage_object.key,
            expected[0],
            expected[1],
            intent.storage_object.modified_at,
            intent.protection_token,
        )
        if (
            not isinstance(raw, ArtifactStorageEvidence)
            or not raw.stored
            or raw.storage_object != intent.storage_object
        ):
            raise ArtifactPreparationNotReadyError(
                "storage adapter did not acknowledge the exact verified resource"
            )
        raw.__post_init__()
        if _hash_stream(stream) != expected:
            raise ArtifactPreparationConflictError(
                "storage adapter changed the verified resource"
            )
        return ArtifactProtectionEvidence(
            intent,
            authority.adapter_id,
            authority.policy_fingerprint_sha256,
            intent.storage_object,
            _capability=_PROTECTION_EVIDENCE_TOKEN,
        )

    @staticmethod
    def persist_prepared_artifact(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        receipt: ArtifactPreparationReceipt,
        now: int,
    ) -> tuple[ArtifactProtectionIntent, ...]:
        """Atomically seal every acquisition/thumbnail PENDING intent."""

        _require_preparation_receipt(receipt)
        timestamp = require_int63(now, field="prepared artifact persisted_at")
        live_generation = _authorize_artifact_mutation(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            now=timestamp,
        )
        authority = receipt.audit.authority
        _load_projection_authority(work, authority.projection)
        current_facts = _load_authority_facts(
            work,
            authority.projection,
            authority.publication_key,
            now=timestamp,
        )
        if current_facts != _facts_from_authority(authority):
            raise ArtifactPreparationConflictError(
                "prepared artifact input authority changed"
            )
        families = _load_prepared_families_or_conflict(
            work.connector,
            candidate_id=authority.candidate_id,
            publication_key=authority.publication_key,
            backend=work.backend,
        )
        expected_kinds = receipt.resource_kinds
        if families:
            if tuple(family.resource_kind for family in families) != expected_kinds:
                raise ArtifactPreparationConflictError(
                    "durable resource family does not exactly cover prepared bundle"
                )
            _validate_artifact_render_facts(work, receipt)
            for family in families:
                descriptor = receipt.resource_descriptor(family.resource_kind)
                _validate_storage_key(
                    work,
                    descriptor.key,
                    key_digest=family.storage_object_key_sha256,
                )
                _validate_prepared_resource_blob(work.connector, receipt, family)
            return tuple(
                _protection_intent_from_family(
                    work.connector,
                    receipt,
                    family,
                    replayed=True,
                )
                for family in families
            )
        if live_generation != authority.projection.generation:
            raise ArtifactPreparationNotReadyError(
                "new resource intents require the live projection generation"
            )
        _persist_artifact_render_facts(work, receipt)
        persisted: list[PreparedArtifactFamily] = []
        for kind in expected_kinds:
            descriptor = receipt.resource_descriptor(kind)
            key_digest = identity.artifact_storage_key_digest(
                descriptor.key.codec,
                descriptor.key.segments,
            )
            _persist_storage_key(work, descriptor.key, key_digest=key_digest)
            token = identity.encode_artifact_protection_token(
                authority.candidate_id,
                authority.publication_key,
                kind.value,
                key_digest,
                live_generation,
            )
            family = PreparedArtifactFamily(
                authority.candidate_id,
                authority.publication_key,
                kind,
                key_digest,
                live_generation,
                token,
                "PENDING",
            )
            try:
                stored, _created = ensure_prepared_artifact_family(
                    work.connector,
                    family,
                    backend=work.backend,
                )
            except (ArtifactFamilyCollisionError, ArtifactFamilyPartialError) as error:
                raise ArtifactPreparationConflictError(
                    "prepared resource intent collides with durable facts"
                ) from error
            _persist_prepared_resource_blob(work, receipt, stored)
            persisted.append(stored)
        return tuple(
            _protection_intent_from_family(
                work.connector,
                receipt,
                family,
                replayed=False,
            )
            for family in persisted
        )

    @staticmethod
    def confirm_prepared_artifact(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        receipt: ArtifactPreparationReceipt,
        intents: tuple[ArtifactProtectionIntent, ...],
        evidence: tuple[ArtifactProtectionEvidence, ...],
        effect_seal: OperationalEffectSeal,
        now: int,
    ) -> ArtifactPersistenceReceipt:
        """Atomically confirm the complete resource bundle and publish its facts."""

        _require_preparation_receipt(receipt)
        timestamp = require_int63(now, field="prepared artifact confirmed_at")
        _authorize_artifact_mutation(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            now=timestamp,
        )
        exact_intents = tuple(intents)
        exact_evidence = tuple(evidence)
        if tuple(intent.resource_kind for intent in exact_intents) != (
            receipt.resource_kinds
        ):
            raise ArtifactPreparationConflictError(
                "confirmation intents do not exactly cover prepared resources"
            )
        authority = receipt.audit.authority
        mutation = _load_projection_authority(work, authority.projection)
        if _load_authority_facts(
            work,
            authority.projection,
            authority.publication_key,
            now=timestamp,
        ) != _facts_from_authority(authority):
            raise ArtifactPreparationConflictError(
                "prepared artifact input authority changed before confirmation"
            )
        families = _load_prepared_families_or_conflict(
            work.connector,
            candidate_id=authority.candidate_id,
            publication_key=authority.publication_key,
            backend=work.backend,
        )
        if tuple(family.resource_kind for family in families) != receipt.resource_kinds:
            raise ArtifactPreparationNotReadyError(
                "confirmation lacks its exact durable resource family"
            )
        for family in families:
            _validate_prepared_resource_blob(work.connector, receipt, family)
        durable_intents = tuple(
            _protection_intent_from_family(
                work.connector,
                receipt,
                family,
                replayed=True,
            )
            for family in families
        )
        if tuple(map(_intent_facts, durable_intents)) != tuple(
            map(_intent_facts, exact_intents)
        ):
            raise ArtifactPreparationConflictError(
                "confirmation intents differ from durable facts"
            )
        states = {family.state for family in families}
        if states == {"PENDING"}:
            if len(exact_evidence) != len(durable_intents):
                raise ArtifactPreparationNotReadyError(
                    "PENDING resource bundle lacks complete protection evidence"
                )
            for intent, item in zip(durable_intents, exact_evidence, strict=True):
                if (
                    not isinstance(item, ArtifactProtectionEvidence)
                    or item._capability is not _PROTECTION_EVIDENCE_TOKEN
                    or _intent_facts(item.intent) != _intent_facts(intent)
                    or item.adapter_id != authority.adapter_id
                    or item.policy_fingerprint_sha256
                    != authority.policy_fingerprint_sha256
                    or item.storage_object != intent.storage_object
                ):
                    raise ArtifactPreparationNotReadyError(
                        "resource protection evidence is incomplete or noncongruent"
                    )
            _persist_confirmed_presentation(
                work,
                mutation,
                receipt=receipt,
                evidence=exact_evidence,
            )
            updated: list[PreparedArtifactFamily] = []
            for family in families:
                try:
                    updated.append(
                        cas_prepared_artifact_state(
                            work,
                            candidate_id=authority.candidate_id,
                            publication_key=authority.publication_key,
                            resource_kind=family.resource_kind,
                            expected_state="PENDING",
                            next_state="PREPARED",
                        )
                    )
                except (
                    ArtifactFamilyCollisionError,
                    ArtifactFamilyPartialError,
                ) as error:
                    raise ArtifactPreparationConflictError(
                        "prepared resource state changed during confirmation"
                    ) from error
            families = tuple(updated)
            replayed = False
        elif states <= {"PREPARED", "COMMITTED"} and len(states) == 1:
            _validate_confirmed_presentation(
                work,
                mutation,
                receipt=receipt,
            )
            replayed = True
        else:
            raise ArtifactPreparationConflictError(
                "prepared resource bundle has mixed or invalid states"
            )
        _bind_operational_effect_seal(
            work,
            candidate_id=authority.candidate_id,
            build_id=authority.build_id,
            effect_seal=effect_seal,
            now=timestamp,
            allow_insert=not replayed,
        )
        return ArtifactPersistenceReceipt(
            authority.candidate_id,
            authority.publication_key,
            receipt.artifact_sha256,
            tuple(
                (family.resource_kind, family.protection_token, family.state)
                for family in families
            ),
            replayed,
        )

    @staticmethod
    def bind_operational_preparation(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        candidate_id: bytes,
        effect_seal: OperationalEffectSeal,
        now: int,
    ) -> None:
        """Bind a COMPLETE effect seal for candidates with zero prepared bytes."""

        timestamp = require_int63(now, field="operational binding timestamp")
        projection = PublicationCandidateRepository.issue_projection_authority(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            candidate_id=candidate_id,
            now=timestamp,
        )
        _bind_operational_effect_seal(
            work,
            candidate_id=projection.candidate_id,
            build_id=projection.build_id,
            effect_seal=effect_seal,
            now=timestamp,
        )


def _artifact_authority(
    work: VNextUnitOfWork,
    projection: PublicationProjectionAuthority,
    publication_key: bytes,
    *,
    now: int,
) -> ArtifactPreparationAuthority:
    if not projection.artifacts_required:
        raise ArtifactPreparationNotReadyError(
            "candidate does not require artifact preparation"
        )
    contract = _load_projection_contract(
        work,
        projection,
        validate_policy_canonical=True,
    )
    facts = _load_authority_facts(
        work,
        projection,
        publication_key,
        now=now,
        contract=contract,
    )
    return ArtifactPreparationAuthority(
        projection,
        facts.publication_key,
        facts.gallery_id,
        facts.gid,
        facts.gallery_key,
        facts.observation_id,
        facts.observation_identity_sha256,
        facts.artifact_semantics_sha256,
        facts.operation,
        facts.source_manifest_component_sha256,
        facts.member_plan_component_sha256,
        facts.effective_content_component_sha256,
        facts.selected_component_sha256,
        facts.owner_component_sha256,
        facts.policy_component_sha256,
        facts.content_sha256,
        facts.owner_gallery_key,
        facts.winner_gallery_key,
        facts.manifest_algorithm_version,
        facts.file_order_version,
        facts.artifact_algorithm_version,
        facts.adapter_id,
        facts.policy_fingerprint_sha256,
        facts.spam_artist_threshold,
        facts.spam_occurrence_threshold,
        facts.validation_checkpoint,
        facts.validation_terminal_receipt,
        _AUTHORITY_TOKEN,
    )


def _input_projection_authority(
    work: VNextUnitOfWork,
    projection: PublicationProjectionAuthority,
) -> ArtifactInputProjectionAuthority:
    """Bind terminal catalog validation to one projection authority."""

    checkpoint, receipt = _load_complete_stage_receipt(
        work,
        projection.candidate_id,
        b"VALIDATE_CATALOG_PROJECTION",
        cursor_maximum=2048,
    )
    return ArtifactInputProjectionAuthority(
        projection,
        checkpoint,
        receipt,
        _INPUT_AUTHORITY_TOKEN,
    )


def _prepare_artifact_input_plan(
    connector: SQLConnector,
    *,
    backend: str,
    authority: ArtifactInputProjectionAuthority,
    validation: bool,
) -> ArtifactInputProjectionPlan:
    if not isinstance(authority, ArtifactInputProjectionAuthority):
        raise TypeError("authority must be ArtifactInputProjectionAuthority")
    authority.__post_init__()
    if authority._capability is not _INPUT_AUTHORITY_TOKEN:
        raise TypeError("artifact input authority is not repository-issued")
    temporary = TemporaryDirectory(prefix="h2hdb-artifact-input-")
    database = sqlite3.connect(
        Path(temporary.name) / "plan.sqlite3",
        check_same_thread=False,
    )
    payload = TemporaryFile(mode="w+b")
    try:
        _create_input_plan_schema(database)
        with connector.read_transaction():
            work = VNextUnitOfWork(connector, backend=backend)
            _load_projection_authority(work, authority.projection)
            current = _load_complete_stage_receipt(
                work,
                authority.projection.candidate_id,
                b"VALIDATE_CATALOG_PROJECTION",
                cursor_maximum=2048,
            )
            if current != (
                authority.catalog_checkpoint,
                authority.catalog_terminal_receipt,
            ):
                raise ArtifactPreparationConflictError(
                    "artifact input authority changed before planning"
                )
            count = _build_input_plan(work, authority, database, payload)
        database.commit()
        return ArtifactInputProjectionPlan(
            authority=authority,
            database=database,
            payload=payload,
            temporary_directory=temporary,
            input_count=count,
            validation=validation,
            _capability=(
                _INPUT_VALIDATION_PLAN_TOKEN if validation else _INPUT_PLAN_TOKEN
            ),
        )
    except BaseException:
        database.close()
        payload.close()
        temporary.cleanup()
        raise


def _load_source_locator_components(
    work: VNextUnitOfWork,
    authority: ArtifactPreparationAuthority,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    row = work.connector.fetch_one(
        "SELECT gallery.scope_key, gallery.locator_sha256 "
        "FROM catalog_gallery_identities AS gallery "
        "WHERE gallery.gallery_id = %s",
        (authority.gallery_id,),
    )
    if len(row) != 2:
        raise ArtifactPreparationContractUnavailableError(
            "artifact source lacks one exact gallery locator"
        )
    contract = _load_projection_contract(work, authority.projection)
    if require_digest32(row[0], field="artifact gallery scope_key") != (
        contract.scope_key
    ):
        raise ArtifactPreparationConflictError(
            "artifact gallery locator belongs to another source scope"
        )
    root_payload = bytearray()
    locator_payload = bytearray()
    try:
        root_receipt = CanonicalValueRepository.stream_and_validate(
            work,
            value_sha256=contract.source_root_sha256,
            consume_provisional=root_payload.extend,
        )
        locator_receipt = CanonicalValueRepository.stream_and_validate(
            work,
            value_sha256=require_digest32(row[1], field="artifact source locator"),
            consume_provisional=locator_payload.extend,
        )
        if (
            root_receipt.digest_domain != b"source_root_v1"
            or locator_receipt.digest_domain != b"source_relative_locator_v1"
        ):
            raise ArtifactPreparationConflictError(
                "artifact source root or locator has the wrong canonical domain"
            )
        return (
            identity.decode_source_root(bytes(root_payload)),
            identity.decode_source_relative_locator(bytes(locator_payload)),
        )
    except (
        CanonicalValueCollisionError,
        CanonicalValueNotReadyError,
        identity.VNextIdentityError,
    ) as error:
        raise ArtifactPreparationConflictError(
            "artifact source root or locator is incomplete or malformed"
        ) from error


def _hash_stream(stream: BinaryIO) -> tuple[bytes, int]:
    stream.seek(0)
    digest = sha256()
    byte_count = 0
    while True:
        part = stream.read(_MAX_READ_BYTES)
        if not isinstance(part, bytes):
            raise ArtifactPreparationConflictError(
                "artifact stream returned non-bytes data"
            )
        if not part:
            break
        digest.update(part)
        byte_count += len(part)
        if byte_count > INT63_MAX:
            raise ArtifactPreparationNotReadyError(
                "artifact stream exceeds the core byte bound"
            )
    stream.seek(0)
    return digest.digest(), byte_count


def _create_input_plan_schema(database: sqlite3.Connection) -> None:
    database.executescript(
        "CREATE TABLE canonical_values ("
        "value_sha256 BLOB PRIMARY KEY, digest_domain BLOB NOT NULL, "
        "payload_offset INTEGER NOT NULL, byte_count INTEGER NOT NULL, "
        "consumer_key BLOB NOT NULL) WITHOUT ROWID;"
        "CREATE TABLE inputs ("
        "publication_key BLOB PRIMARY KEY, "
        "artifact_semantics_sha256 BLOB NOT NULL, "
        "source_manifest_component_sha256 BLOB NOT NULL, "
        "member_plan_component_sha256 BLOB NOT NULL, "
        "effective_content_component_sha256 BLOB NOT NULL, "
        "selected_component_sha256 BLOB NOT NULL, "
        "owner_component_sha256 BLOB NOT NULL, "
        "policy_component_sha256 BLOB NOT NULL) WITHOUT ROWID;"
        "CREATE TABLE members ("
        "publication_key BLOB NOT NULL, source_position INTEGER NOT NULL, "
        "source_name_bytes BLOB NOT NULL, source_file_sha256 BLOB NOT NULL, "
        "source_size_bytes INTEGER NOT NULL, source_role INTEGER NOT NULL, "
        "PRIMARY KEY (publication_key, source_position)) WITHOUT ROWID;"
        "CREATE INDEX effective_members ON members "
        "(publication_key, source_role, source_file_sha256, source_position);"
    )


def _load_manifest_policy(
    work: VNextUnitOfWork,
    manifest_policy_id: int,
) -> tuple[int, int]:
    policy_id = require_positive_int63(
        manifest_policy_id,
        field="artifact manifest_policy_id",
    )
    record = _registry_record(
        "artifact manifest policy",
        lambda: load_manifest_policy(work.connector, policy_id),
    )
    return record.manifest_algorithm_version, record.file_order_version


def _load_analysis_policy(
    work: VNextUnitOfWork,
    policy_id: int,
) -> tuple[int, int, int, int, int]:
    analysis_policy_id = require_positive_int63(
        policy_id,
        field="artifact analysis policy_id",
    )
    record = _registry_record(
        "artifact analysis policy",
        lambda: load_analysis_policy(work.connector, analysis_policy_id),
    )
    return (
        record.algorithm_version,
        record.spam_artist_threshold,
        record.spam_occurrence_threshold,
        record.content_owner_rule_version,
        record.gid_winner_rule_version,
    )


def _load_source_scope(
    work: VNextUnitOfWork,
    scope_key: bytes,
) -> tuple[bytes, bytes, int]:
    scope = require_digest32(scope_key, field="artifact scope_key")
    record = _registry_record(
        "artifact source scope",
        lambda: load_source_scope(work.connector, scope),
    )
    return (
        record.source_provider,
        record.source_root_sha256,
        record.identity_policy_version,
    )


def _load_artifact_policy_semantics(
    work: VNextUnitOfWork,
    policy_component_sha256: bytes,
) -> tuple[int, bytes, bytes]:
    policy = require_digest32(
        policy_component_sha256,
        field="artifact policy_component_sha256",
    )
    record = _registry_record(
        "artifact policy semantics",
        lambda: load_artifact_policy_semantics(work.connector, policy),
    )
    return (
        record.artifact_algorithm_version,
        record.adapter_id,
        record.policy_fingerprint_sha256,
    )


def _load_artifact_policy_contract(
    work: VNextUnitOfWork,
    policy_component_sha256: bytes,
) -> tuple[int, bytes, bytes]:
    """Load the exact neutral protocol and adapter-owned policy identity."""

    return _load_artifact_policy_semantics(work, policy_component_sha256)


def _load_projection_contract(
    work: VNextUnitOfWork,
    projection: PublicationProjectionAuthority,
    *,
    validate_policy_canonical: bool = False,
) -> _ArtifactContractFacts:
    try:
        run = load_analysis_run_family(
            work.connector,
            analysis_id=projection.analysis_id,
        )
    except AnalysisFamilyCollisionError as error:
        raise ArtifactPreparationConflictError(str(error)) from error
    if run is None or run.build_id != projection.build_id or run.state != "COMPLETE":
        raise ArtifactPreparationNotReadyError(
            "artifact projection analysis is missing, incomplete, or mismatched"
        )
    root = work.connector.fetch_one(
        "SELECT build.scope_key, build.manifest_policy_id, "
        "policy.policy_component_sha256 "
        "FROM catalog_source_build_descriptor AS build "
        "JOIN catalog_source_build_states AS build_state "
        "ON build_state.build_id = build.build_id AND build_state.state = 'SEALED' "
        "JOIN catalog_source_build_sealed_ats AS build_completed "
        "ON build_completed.build_id = build.build_id "
        "JOIN catalog_artifact_policies AS policy "
        "ON policy.artifact_policy_id = %s "
        "WHERE build.build_id = %s",
        (
            projection.artifact_policy_id,
            projection.build_id,
        ),
    )
    if len(root) != 3:
        raise ArtifactPreparationNotReadyError(
            "artifact projection contract roots are missing"
        )
    scope_key = require_digest32(root[0], field="artifact build scope_key")
    source_scope = _load_source_scope(work, scope_key)
    manifest = _load_manifest_policy(
        work,
        require_positive_int63(root[1], field="artifact manifest_policy_id"),
    )
    analysis = _load_analysis_policy(
        work,
        run.policy_id,
    )
    policy_component = require_digest32(
        root[2], field="artifact policy_component_sha256"
    )
    semantics = _load_artifact_policy_contract(
        work,
        policy_component,
    )
    if validate_policy_canonical:
        _require_policy_canonical_value(
            work,
            policy_component=policy_component,
            algorithm_version=semantics[0],
            adapter_id=semantics[1],
            policy_fingerprint_sha256=semantics[2],
        )
    return _ArtifactContractFacts(
        scope_key,
        source_scope[0],
        source_scope[1],
        source_scope[2],
        manifest[0],
        manifest[1],
        analysis[0],
        analysis[1],
        analysis[2],
        analysis[3],
        analysis[4],
        policy_component,
        semantics[0],
        semantics[1],
        semantics[2],
    )


def _require_policy_canonical_value(
    work: VNextUnitOfWork,
    *,
    policy_component: bytes,
    algorithm_version: int,
    adapter_id: bytes,
    policy_fingerprint_sha256: bytes,
) -> None:
    expected_payload = identity.encode_artifact_policy(
        algorithm_version,
        adapter_id,
        policy_fingerprint_sha256,
    )
    if (
        identity.artifact_policy_digest(
            algorithm_version,
            adapter_id,
            policy_fingerprint_sha256,
        )
        != policy_component
    ):
        raise ArtifactPreparationConflictError(
            "artifact policy component disagrees with its registered tuple"
        )
    payload = bytearray()
    try:
        receipt = CanonicalValueRepository.stream_and_validate(
            work,
            value_sha256=policy_component,
            consume_provisional=payload.extend,
        )
    except (CanonicalValueCollisionError, CanonicalValueNotReadyError) as error:
        raise ArtifactPreparationConflictError(
            "artifact policy canonical payload is incomplete or corrupt"
        ) from error
    if (
        receipt.digest_domain != _ARTIFACT_POLICY_DOMAIN
        or receipt.byte_count != len(expected_payload)
        or bytes(payload) != expected_payload
    ):
        raise ArtifactPreparationConflictError(
            "artifact policy canonical payload disagrees with its registered tuple"
        )


def _build_input_plan(
    work: VNextUnitOfWork,
    authority: ArtifactInputProjectionAuthority,
    database: sqlite3.Connection,
    payload: BinaryIO,
) -> int:
    projection = authority.projection
    if not projection.artifacts_required:
        return 0
    contract = _load_projection_contract(work, projection)
    after = b""
    count = 0
    while True:
        rows = work.connector.fetch_all(
            "SELECT selection.publication_key, selection.gallery_id, pub.gid, "
            "gallery.gallery_key, gallery.scope_key, "
            "member.observation_id, "
            "observation.observation_identity_sha256 "
            "FROM catalog_publication_selections AS selection "
            "JOIN catalog_publication_identities AS pub "
            "ON pub.publication_key = selection.publication_key "
            "JOIN catalog_gallery_identities AS gallery "
            "ON gallery.gallery_id = selection.gallery_id "
            "JOIN catalog_source_build_galleries AS member "
            "ON member.build_id = %s AND member.gallery_id = selection.gallery_id "
            "JOIN catalog_gallery_observations AS observation "
            "ON observation.gallery_id = member.gallery_id "
            "AND observation.observation_id = member.observation_id "
            "WHERE selection.candidate_id = %s AND selection.publication_key > %s "
            "ORDER BY selection.publication_key LIMIT 128",
            (
                projection.build_id,
                projection.candidate_id,
                after,
            ),
        )
        if not rows:
            break
        for row in rows:
            _build_one_input(work, authority, contract, database, payload, row)
            after = require_digest32(row[0], field="planned publication_key")
            count += 1
            if count > INT63_MAX:
                raise ArtifactPreparationNotReadyError(
                    "artifact input count is exhausted"
                )
        if len(rows) < _MAX_SOURCE_PAGE:
            break
    return count


def _build_one_input(
    work: VNextUnitOfWork,
    authority: ArtifactInputProjectionAuthority,
    contract: _ArtifactContractFacts,
    database: sqlite3.Connection,
    payload: BinaryIO,
    row: tuple[Any, ...],
) -> None:
    if len(row) != 7:
        raise ArtifactPreparationConflictError(
            "artifact input evaluator returned a malformed row"
        )
    publication = require_digest32(row[0], field="planned publication_key")
    gallery_id = require_positive_int63(row[1], field="planned gallery_id")
    gid = require_positive_int63(row[2], field="planned gid")
    gallery_key = require_digest32(row[3], field="planned gallery_key")
    gallery_scope = require_digest32(row[4], field="planned gallery scope_key")
    if gallery_scope != contract.scope_key:
        raise ArtifactPreparationConflictError(
            "planned gallery belongs to another source scope"
        )
    observation_id = require_positive_int63(row[5], field="planned observation_id")
    observation_identity = require_digest32(
        row[6], field="planned observation identity"
    )
    manifest_version = contract.manifest_algorithm_version
    file_order_version = contract.file_order_version
    artifact_algorithm = contract.artifact_algorithm_version
    adapter_id = contract.adapter_id
    policy_fingerprint = contract.policy_fingerprint_sha256
    spam_artist = contract.spam_artist_threshold
    spam_occurrence = contract.spam_occurrence_threshold
    policy = contract.policy_component_sha256
    owner = _load_owner_facts(
        work,
        analysis_id=authority.projection.analysis_id,
        gallery_id=gallery_id,
        gid=gid,
    )
    _spool_planned_members(
        work,
        database,
        publication_key=publication,
        analysis_id=authority.projection.analysis_id,
        gallery_id=gallery_id,
        observation_id=observation_id,
        spam_artist_threshold=spam_artist,
        spam_occurrence_threshold=spam_occurrence,
    )
    member_count_row = database.execute(
        "SELECT COUNT(*) FROM members WHERE publication_key = ?",
        (sqlite3.Binary(publication),),
    ).fetchone()
    member_count = require_int63(member_count_row[0], field="planned member count")

    def member_parts() -> Iterator[bytes]:
        return identity.iter_artifact_member_plan_payload(
            member_count,
            _iter_planned_member_entries(database, publication),
        )

    member_digest = identity.canonical_value_digest_parts(
        "artifact_member_plan_v2",
        _parts_size(member_parts()),
        member_parts(),
    )

    def effective_rows() -> Iterator[bytes]:
        return _iter_planned_effective_digests(database, publication)

    effective_count_row = database.execute(
        "SELECT COUNT(*) FROM members WHERE publication_key = ? AND source_role = ?",
        (sqlite3.Binary(publication), int(identity.ArtifactMemberSourceRole.PAGE)),
    ).fetchone()
    effective_count = require_int63(
        effective_count_row[0], field="planned effective count"
    )

    def effective_parts() -> Iterator[bytes]:
        return identity.iter_artifact_effective_content_payload_ordered(
            effective_count,
            effective_rows(),
        )

    effective_digest = identity.canonical_value_digest_parts(
        "artifact_effective_content_v1",
        _parts_size(effective_parts()),
        effective_parts(),
    )
    source_manifest = identity.artifact_source_manifest_digest(
        observation_identity,
        manifest_version,
        file_order_version,
    )
    selected = identity.artifact_selected_digest(publication, gallery_key)
    owner_digest = identity.artifact_owner_digest(owner[0], owner[1], gid, owner[2])
    semantics = identity.artifact_semantics_digest(
        source_manifest,
        member_digest,
        effective_digest,
        selected,
        owner_digest,
        policy,
    )
    components: tuple[tuple[bytes, bytes, Iterator[bytes]], ...] = (
        (
            source_manifest,
            b"artifact_source_manifest_v1",
            iter(
                (
                    identity.encode_artifact_source_manifest(
                        observation_identity,
                        manifest_version,
                        file_order_version,
                    ),
                )
            ),
        ),
        (member_digest, b"artifact_member_plan_v2", member_parts()),
        (
            effective_digest,
            b"artifact_effective_content_v1",
            effective_parts(),
        ),
        (
            selected,
            b"artifact_selected_v1",
            iter((identity.encode_artifact_selected(publication, gallery_key),)),
        ),
        (
            owner_digest,
            b"artifact_owner_v1",
            iter((identity.encode_artifact_owner(owner[0], owner[1], gid, owner[2]),)),
        ),
        (
            policy,
            b"artifact_policy_v3",
            iter(
                (
                    identity.encode_artifact_policy(
                        artifact_algorithm,
                        adapter_id,
                        policy_fingerprint,
                    ),
                )
            ),
        ),
        (
            semantics,
            b"artifact_semantics_v1",
            iter(
                (
                    identity.encode_artifact_semantics(
                        source_manifest,
                        member_digest,
                        effective_digest,
                        selected,
                        owner_digest,
                        policy,
                    ),
                )
            ),
        ),
    )
    for digest, domain, parts in components:
        _append_input_canonical(
            database,
            payload,
            value_sha256=digest,
            digest_domain=domain,
            parts=parts,
            consumer_key=publication,
        )
    database.execute(
        "INSERT INTO inputs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            sqlite3.Binary(publication),
            sqlite3.Binary(semantics),
            sqlite3.Binary(source_manifest),
            sqlite3.Binary(member_digest),
            sqlite3.Binary(effective_digest),
            sqlite3.Binary(selected),
            sqlite3.Binary(owner_digest),
            sqlite3.Binary(policy),
        ),
    )


def _spool_planned_members(
    work: VNextUnitOfWork,
    database: sqlite3.Connection,
    *,
    publication_key: bytes,
    analysis_id: bytes,
    gallery_id: int,
    observation_id: int,
    spam_artist_threshold: int,
    spam_occurrence_threshold: int,
) -> None:
    after = -1
    position = 0
    metadata_count = 0
    page_count = 0
    while True:
        rows = work.connector.fetch_all(
            "SELECT source.file_no, name.name_bytes, source.file_sha256, "
            "content_blob.size_bytes, source.artifact_role, "
            "decision.occurrence_count, decision.artist_count, "
            "decision.maximum_gallery_artist_count "
            + _SOURCE_FILE_FAMILY_SQL
            + "JOIN catalog_content_blobs AS content_blob "
            "ON content_blob.file_sha256 = source.file_sha256 "
            "LEFT JOIN catalog_analysis_file_hash_decision_resolved AS decision "
            "ON decision.analysis_id = %s AND decision.file_sha256 = source.file_sha256 "
            "WHERE source.gallery_id = %s AND source.observation_id = %s "
            "AND source.file_no > %s ORDER BY source.file_no LIMIT 128",
            (analysis_id, gallery_id, observation_id, after),
        )
        if not rows:
            break
        for row in rows:
            file_no = require_int63(row[0], field="planned source file_no")
            if file_no != position:
                raise ArtifactPreparationConflictError(
                    "planned source file positions are not zero-based contiguous"
                )
            name = require_bounded_bytes(
                row[1], field="planned source name", minimum=1, maximum=255
            )
            role = require_bounded_bytes(
                row[4],
                field="planned adapter-issued source role",
                minimum=4,
                maximum=8,
            )
            if role == b"metadata":
                if any(value is not None for value in row[5:8]):
                    raise ArtifactPreparationConflictError(
                        "metadata unexpectedly participates in spam decisions"
                    )
                member_role = identity.ArtifactMemberSourceRole.METADATA
                metadata_count += 1
            elif role == b"page":
                member_role = identity.ArtifactMemberSourceRole.PAGE
                if _excluded_from_scalars(
                    row[5:8],
                    spam_artist_threshold=spam_artist_threshold,
                    spam_occurrence_threshold=spam_occurrence_threshold,
                ):
                    position += 1
                    after = file_no
                    continue
                page_count += 1
                if page_count > 4096:
                    raise ArtifactPreparationNotReadyError(
                        "artifact render plan exceeds 4096 PAGE members"
                    )
            elif role == b"other":
                position += 1
                after = file_no
                continue
            else:
                raise ArtifactPreparationConflictError(
                    "adapter-issued source role is not registered"
                )
            entry = identity.ArtifactMemberPlanEntry(
                file_no,
                name,
                require_digest32(row[2], field="planned source digest"),
                require_int63(row[3], field="planned source size"),
                member_role,
            )
            database.execute(
                "INSERT INTO members VALUES (?, ?, ?, ?, ?, ?)",
                (
                    sqlite3.Binary(publication_key),
                    entry.source_position,
                    sqlite3.Binary(entry.source_name_bytes),
                    sqlite3.Binary(entry.source_file_sha256),
                    entry.source_size_bytes,
                    int(entry.source_role),
                ),
            )
            position += 1
            after = file_no
        if len(rows) < _MAX_SOURCE_PAGE:
            break
    if metadata_count != 1:
        raise ArtifactPreparationConflictError(
            "artifact render plan requires exactly one METADATA source"
        )


def _excluded_from_scalars(
    decision: tuple[Any, ...],
    *,
    spam_artist_threshold: int,
    spam_occurrence_threshold: int,
) -> bool:
    if all(value is None for value in decision):
        return False
    if any(value is None for value in decision):
        raise ArtifactPreparationConflictError("artifact spam decision is partial")
    occurrence = require_positive_int63(decision[0], field="planned occurrence count")
    artist = require_int63(decision[1], field="planned artist count")
    maximum = require_int63(decision[2], field="planned maximum artist count")
    return (
        occurrence >= spam_occurrence_threshold
        and maximum > 0
        and artist > spam_artist_threshold * maximum
    )


def _iter_planned_member_entries(
    database: sqlite3.Connection,
    publication_key: bytes,
) -> Iterator[identity.ArtifactMemberPlanEntry]:
    cursor = database.execute(
        "SELECT source_position, source_name_bytes, source_file_sha256, "
        "source_size_bytes, source_role FROM members "
        "WHERE publication_key = ? ORDER BY source_position",
        (sqlite3.Binary(publication_key),),
    )
    while rows := cursor.fetchmany(_MAX_SOURCE_PAGE):
        for row in rows:
            yield identity.ArtifactMemberPlanEntry(
                require_int63(row[0], field="planned member position"),
                bytes(row[1]),
                bytes(row[2]),
                require_int63(row[3], field="planned member size"),
                identity.ArtifactMemberSourceRole(
                    require_int63(row[4], field="planned member source_role")
                ),
            )


def _iter_planned_effective_digests(
    database: sqlite3.Connection,
    publication_key: bytes,
) -> Iterator[bytes]:
    cursor = database.execute(
        "SELECT source_file_sha256 FROM members WHERE publication_key = ? "
        "AND source_role = ? ORDER BY source_file_sha256, source_position",
        (
            sqlite3.Binary(publication_key),
            int(identity.ArtifactMemberSourceRole.PAGE),
        ),
    )
    while rows := cursor.fetchmany(_MAX_SOURCE_PAGE):
        for row in rows:
            yield require_digest32(row[0], field="planned effective digest")


def _parts_size(parts: Iterator[bytes]) -> int:
    count = 0
    for part in parts:
        count += len(part)
        if count > INT63_MAX:
            raise ArtifactPreparationNotReadyError(
                "artifact canonical payload size is exhausted"
            )
    return count


def _append_input_canonical(
    database: sqlite3.Connection,
    payload: BinaryIO,
    *,
    value_sha256: bytes,
    digest_domain: bytes,
    parts: Iterator[bytes],
    consumer_key: bytes,
) -> None:
    existing = database.execute(
        "SELECT digest_domain, payload_offset, byte_count, consumer_key "
        "FROM canonical_values WHERE value_sha256 = ?",
        (sqlite3.Binary(value_sha256),),
    ).fetchone()
    if existing is not None:
        if bytes(existing[0]) != digest_domain:
            raise ArtifactPreparationConflictError(
                "artifact canonical digest collides across domains"
            )
        if consumer_key < bytes(existing[3]):
            database.execute(
                "UPDATE canonical_values SET consumer_key = ? WHERE value_sha256 = ?",
                (sqlite3.Binary(consumer_key), sqlite3.Binary(value_sha256)),
            )
        return
    payload.seek(0, os.SEEK_END)
    offset = payload.tell()
    count = 0
    for part in parts:
        exact = require_bounded_bytes(
            part, field="artifact canonical payload part", maximum=INT63_MAX
        )
        _write_all(payload, exact)
        count += len(exact)
        if count > INT63_MAX:
            raise ArtifactPreparationNotReadyError(
                "artifact canonical payload size is exhausted"
            )
    database.execute(
        "INSERT INTO canonical_values VALUES (?, ?, ?, ?, ?)",
        (
            sqlite3.Binary(value_sha256),
            sqlite3.Binary(digest_domain),
            offset,
            count,
            sqlite3.Binary(consumer_key),
        ),
    )


def _iter_file_range(stream: BinaryIO, offset: int, count: int) -> Iterator[bytes]:
    stream.seek(offset)
    remaining = count
    while remaining:
        part = stream.read(min(_MAX_READ_BYTES, remaining))
        if not part:
            raise ArtifactPreparationConflictError(
                "artifact canonical payload spool is truncated"
            )
        remaining -= len(part)
        yield part


def _validate_publication_cursor(cursor: bytes) -> None:
    exact = require_bounded_bytes(
        cursor, field="artifact publication cursor", maximum=32
    )
    if exact and len(exact) != 32:
        raise ValueError("artifact publication cursor must be raw32 or empty")


def _require_input_plan(
    work: VNextUnitOfWork,
    mutation: _MutationAuthority,
    plan: ArtifactInputProjectionPlan,
    *,
    validation: bool,
) -> None:
    if not isinstance(plan, ArtifactInputProjectionPlan):
        raise TypeError("plan must be ArtifactInputProjectionPlan")
    expected = _INPUT_VALIDATION_PLAN_TOKEN if validation else _INPUT_PLAN_TOKEN
    if plan._capability is not expected or plan.validation is not validation:
        raise TypeError("artifact input plan has the wrong repository capability")
    plan._require_open()
    plan.authority.__post_init__()
    require_int63(plan.input_count, field="artifact input plan count")
    if type(plan.validation) is not bool:
        raise TypeError("artifact input plan validation must be bool")
    loaded = _load_projection_authority(work, plan.authority.projection)
    if loaded != mutation:
        raise ArtifactPreparationNotReadyError(
            "artifact input plan authority differs from the live candidate"
        )
    current = _load_complete_stage_receipt(
        work,
        mutation.candidate.candidate_id,
        b"VALIDATE_CATALOG_PROJECTION",
        cursor_maximum=2048,
    )
    if current != (
        plan.authority.catalog_checkpoint,
        plan.authority.catalog_terminal_receipt,
    ):
        raise ArtifactPreparationConflictError(
            "artifact input plan catalog authority changed"
        )
    expected_count = (
        plan.authority.projection.publication_count
        if mutation.candidate.artifacts_required
        else 0
    )
    if plan.input_count != expected_count:
        raise ArtifactPreparationConflictError(
            "artifact input plan count differs from candidate policy"
        )


def _lock_input_upload_claims(
    work: VNextUnitOfWork,
    plan: ArtifactInputProjectionPlan,
    rows: tuple[tuple[Any, ...], ...],
) -> None:
    publications = tuple(bytes(row[0]) for row in rows)
    if not publications:
        return
    values: set[bytes] = set()
    for publication in publications:
        canonical = plan._database.execute(
            "SELECT value_sha256 FROM canonical_values WHERE consumer_key = ? "
            "ORDER BY value_sha256",
            (sqlite3.Binary(publication),),
        ).fetchall()
        values.update(bytes(row[0]) for row in canonical)
    generation = plan.authority.projection.generation
    for value in sorted(values):
        claim = work.connector.fetch_one(
            "SELECT generation, value_sha256 "
            "FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (generation, value),
        )
        if claim != (generation, value):
            raise ArtifactPreparationNotReadyError(
                "artifact input first consumer lacks its exact upload claim"
            )


def _persist_artifact_input(
    work: VNextUnitOfWork,
    mutation: _MutationAuthority,
    plan: ArtifactInputProjectionPlan,
    row: tuple[Any, ...],
) -> None:
    if len(row) != 8:
        raise ArtifactPreparationConflictError("artifact input plan row is malformed")
    publication = require_digest32(row[0], field="artifact input publication_key")
    digests = tuple(
        require_digest32(value, field="planned artifact component") for value in row[1:]
    )
    semantic_family = ArtifactSemanticInputFamily(
        digests[0],
        digests[1],
        digests[2],
        digests[3],
        digests[4],
        digests[5],
        digests[6],
    )
    try:
        ensure_artifact_semantic_input_family(
            work.connector,
            semantic_family,
            backend=work.backend,
        )
    except (ArtifactFamilyCollisionError, ArtifactFamilyPartialError) as error:
        raise ArtifactPreparationConflictError(
            "artifact semantic family collides with different exact facts"
        ) from error
    _insert_or_compare(
        work,
        "catalog_candidate_artifact_inputs",
        ("candidate_id", "publication_key", "artifact_semantics_sha256"),
        (
            mutation.candidate.candidate_id,
            publication,
            digests[0],
        ),
        key_where="candidate_id = %s AND publication_key = %s",
        key_parameters=(mutation.candidate.candidate_id, publication),
        conflict_label="candidate artifact input",
    )
    generation = plan.authority.projection.generation
    canonical_rows = plan._database.execute(
        "SELECT value_sha256, digest_domain, byte_count, consumer_key "
        "FROM canonical_values WHERE value_sha256 IN (?, ?, ?, ?, ?, ?, ?) "
        "ORDER BY value_sha256",
        tuple(sqlite3.Binary(value) for value in digests),
    ).fetchall()
    if len(canonical_rows) != len(set(digests)):
        raise ArtifactPreparationConflictError(
            "artifact input plan lacks one of its canonical components"
        )
    sealed_values = load_sealed_value_identities(
        work.connector,
        value_sha256s=tuple(
            require_digest32(row[0], field="artifact canonical digest")
            for row in canonical_rows
        ),
    )
    for canonical in canonical_rows:
        value = require_digest32(canonical[0], field="artifact canonical digest")
        persisted = sealed_values.get(value)
        if (
            persisted is None
            or persisted.digest_domain != bytes(canonical[1])
            or persisted.byte_count != canonical[2]
        ):
            raise ArtifactPreparationNotReadyError(
                "artifact input canonical component is not exactly sealed"
            )
        require_digest32(
            persisted.root_page_sha256,
            field="artifact canonical root",
        )
        if bytes(canonical[3]) != publication:
            continue
        deleted = work.connector.execute_affected(
            "DELETE FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (generation, value),
        )
        if deleted != 1:
            raise ArtifactPreparationConflictError(
                "artifact canonical upload claim changed during handoff"
            )


def _insert_or_compare(
    work: VNextUnitOfWork,
    table: str,
    columns: tuple[str, ...],
    expected: tuple[Any, ...],
    *,
    key_where: str,
    key_parameters: tuple[Any, ...],
    conflict_label: str,
) -> None:
    rows = work.connector.fetch_all(
        f"SELECT {', '.join(columns)} FROM {table} WHERE {key_where} LIMIT 2",
        key_parameters,
    )
    if rows:
        if tuple(rows) != (expected,):
            raise ArtifactPreparationConflictError(
                f"{conflict_label} collides with different exact facts"
            )
        return
    placeholders = ", ".join("%s" for _ in columns)
    try:
        work.connector.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            expected,
        )
    except DatabaseDuplicateKeyError:
        locking = " FOR UPDATE" if work.backend == "mariadb" else ""
        raced = work.connector.fetch_all(
            f"SELECT {', '.join(columns)} FROM {table} "
            f"WHERE {key_where} LIMIT 2{locking}",
            key_parameters,
        )
        if tuple(raced) != (expected,):
            raise ArtifactPreparationConflictError(
                f"{conflict_label} concurrent replay changed exact facts"
            )


def _derive_delta_keys(
    work: VNextUnitOfWork,
    mutation: _MutationAuthority,
    *,
    after: bytes,
) -> tuple[bytes, ...]:
    _validate_publication_cursor(after)
    candidate = mutation.candidate.candidate_id
    new_rows = work.connector.fetch_all(
        "SELECT publication_key FROM catalog_candidate_artifact_inputs "
        "WHERE candidate_id = %s AND publication_key > %s "
        "ORDER BY publication_key LIMIT 128",
        (candidate, after),
    )
    old_rows: list[tuple[Any, ...]] = []
    if mutation.base_catalog is not None:
        old_rows = work.connector.fetch_all(
            "SELECT publication_key FROM catalog_artifacts "
            "WHERE revision = %s AND publication_key > %s "
            "ORDER BY publication_key LIMIT 128",
            (mutation.base_catalog.revision, after),
        )
    values = sorted(
        {
            require_digest32(row[0], field="artifact delta publication_key")
            for row in (*new_rows, *old_rows)
        }
    )
    return tuple(values[:_MAX_SOURCE_PAGE])


def _old_artifact(
    work: VNextUnitOfWork,
    mutation: _MutationAuthority,
    publication_key: bytes,
) -> tuple[bytes, bytes] | None:
    if mutation.base_catalog is None:
        return None
    rows = work.connector.fetch_all(
        "SELECT artifact_semantics_sha256, artifact_sha256 "
        "FROM catalog_artifacts "
        "WHERE revision = %s AND publication_key = %s LIMIT 2",
        (mutation.base_catalog.revision, publication_key),
    )
    if not rows:
        return None
    if len(rows) != 1 or len(rows[0]) != 2:
        raise ArtifactPreparationConflictError(
            "base catalog has more than one artifact for a publication"
        )
    return (
        require_digest32(rows[0][0], field="old artifact semantics"),
        require_digest32(rows[0][1], field="old artifact sha256"),
    )


def _new_artifact_input(
    work: VNextUnitOfWork,
    mutation: _MutationAuthority,
    publication_key: bytes,
) -> bytes | None:
    row = work.connector.fetch_one(
        "SELECT artifact_semantics_sha256 "
        "FROM catalog_candidate_artifact_inputs "
        "WHERE candidate_id = %s AND publication_key = %s",
        (mutation.candidate.candidate_id, publication_key),
    )
    if not row:
        return None
    if len(row) != 1:
        raise ArtifactPreparationConflictError("new artifact input is malformed")
    return require_digest32(row[0], field="new artifact semantics")


def _classify_artifact_operation(
    old: tuple[bytes, bytes] | None,
    new: bytes | None,
) -> str:
    if old is None and new is not None:
        return "CREATE"
    if old is not None and new is None:
        return "DELETE"
    if old is None or new is None:
        raise ArtifactPreparationConflictError("artifact delta has no old or new side")
    return "UNCHANGED" if old[0] == new else "REBUILD"


def _materialize_delta_key(
    work: VNextUnitOfWork,
    mutation: _MutationAuthority,
    publication_key: bytes,
) -> None:
    old = _old_artifact(work, mutation, publication_key)
    new = _new_artifact_input(work, mutation, publication_key)
    operation = _classify_artifact_operation(old, new)
    candidate = mutation.candidate.candidate_id
    _insert_or_compare(
        work,
        "catalog_artifact_operations",
        ("candidate_id", "publication_key", "operation"),
        (candidate, publication_key, operation),
        key_where="candidate_id = %s AND publication_key = %s",
        key_parameters=(candidate, publication_key),
        conflict_label="artifact operation",
    )
    if operation == "UNCHANGED":
        assert old is not None
        _insert_catalog_artifact_occurrence(
            work,
            mutation,
            publication_key=publication_key,
            artifact_sha256=old[1],
            artifact_semantics_sha256=old[0],
        )


def _insert_catalog_artifact_occurrence(
    work: VNextUnitOfWork,
    mutation: _MutationAuthority,
    *,
    publication_key: bytes,
    artifact_sha256: bytes,
    artifact_semantics_sha256: bytes,
) -> None:
    revision = mutation.candidate.reserved_revision
    publication = work.connector.fetch_one(
        "SELECT publication_key FROM catalog_publications "
        "WHERE revision = %s AND publication_key = %s",
        (revision, publication_key),
    )
    if publication != (publication_key,):
        raise ArtifactPreparationNotReadyError(
            "artifact occurrence lacks its reserved catalog publication"
        )
    if mutation.base_catalog is None:
        raise ArtifactPreparationConflictError(
            "unchanged artifact has no base catalog authority"
        )
    base_revision = mutation.base_catalog.revision
    artifact = work.connector.fetch_one(
        "SELECT artifact_sha256, artifact_semantics_sha256, artifact_name, "
        "media_type, page_count FROM catalog_artifacts "
        "WHERE revision = %s AND publication_key = %s",
        (base_revision, publication_key),
    )
    if (
        len(artifact) != 5
        or artifact[0] != artifact_sha256
        or artifact[1] != artifact_semantics_sha256
    ):
        raise ArtifactPreparationConflictError(
            "base artifact occurrence differs from unchanged delta authority"
        )
    try:
        ensure_catalog_artifact_family(
            work.connector,
            CatalogArtifactFamily(
                revision,
                publication_key,
                artifact_sha256,
                artifact_semantics_sha256,
                artifact[2],
                artifact[3],
                artifact[4],
            ),
            backend=work.backend,
        )
    except (ArtifactFamilyCollisionError, ArtifactFamilyPartialError) as error:
        raise ArtifactPreparationConflictError(
            "catalog artifact occurrence collides with different exact facts"
        ) from error

    objects = work.connector.fetch_all(
        "SELECT resource_kind, storage_object_key_sha256, storage_object_sha256, "
        "size_bytes, modified_at FROM catalog_storage_objects "
        "WHERE revision = %s AND publication_key = %s ORDER BY resource_kind",
        (base_revision, publication_key),
    )
    for row in objects:
        _insert_or_compare(
            work,
            "catalog_storage_objects",
            (
                "revision",
                "publication_key",
                "resource_kind",
                "storage_object_key_sha256",
                "storage_object_sha256",
                "size_bytes",
                "modified_at",
            ),
            (revision, publication_key, *row),
            key_where=("revision = %s AND publication_key = %s AND resource_kind = %s"),
            key_parameters=(revision, publication_key, row[0]),
            conflict_label="unchanged catalog storage object",
        )
    pages = work.connector.fetch_all(
        "SELECT resource_kind, page_index, extent_offset, extent_length, "
        "media_type, image_sha256, width, height FROM catalog_pages "
        "WHERE revision = %s AND publication_key = %s ORDER BY page_index",
        (base_revision, publication_key),
    )
    for row in pages:
        _insert_or_compare(
            work,
            "catalog_pages",
            (
                "revision",
                "publication_key",
                "resource_kind",
                "page_index",
                "extent_offset",
                "extent_length",
                "media_type",
                "image_sha256",
                "width",
                "height",
            ),
            (revision, publication_key, *row),
            key_where=("revision = %s AND publication_key = %s AND page_index = %s"),
            key_parameters=(revision, publication_key, row[1]),
            conflict_label="unchanged catalog page",
        )
    thumbnails = work.connector.fetch_all(
        "SELECT resource_kind, extent_offset, extent_length, media_type, "
        "image_sha256, width, height FROM catalog_thumbnails "
        "WHERE revision = %s AND publication_key = %s LIMIT 2",
        (base_revision, publication_key),
    )
    if len(thumbnails) > 1:
        raise ArtifactPreparationConflictError(
            "base artifact contains duplicate thumbnail facts"
        )
    if thumbnails:
        _insert_or_compare(
            work,
            "catalog_thumbnails",
            (
                "revision",
                "publication_key",
                "resource_kind",
                "extent_offset",
                "extent_length",
                "media_type",
                "image_sha256",
                "width",
                "height",
            ),
            (revision, publication_key, *thumbnails[0]),
            key_where="revision = %s AND publication_key = %s",
            key_parameters=(revision, publication_key),
            conflict_label="unchanged catalog thumbnail",
        )


def _compare_delta_for_input(
    work: VNextUnitOfWork,
    mutation: _MutationAuthority,
    planned: tuple[Any, ...],
) -> None:
    publication = require_digest32(planned[0], field="validated publication_key")
    semantics = tuple(
        require_digest32(value, field="validated artifact semantic component")
        for value in planned[1:]
    )
    try:
        semantic_family = load_artifact_semantic_input_family(
            work.connector,
            artifact_semantics_sha256=semantics[0],
            backend=work.backend,
        )
    except (ArtifactFamilyCollisionError, ArtifactFamilyPartialError) as error:
        raise ArtifactPreparationConflictError(
            "artifact semantic family is incomplete or corrupt"
        ) from error
    if (
        semantic_family is None
        or (
            semantic_family.artifact_semantics_sha256,
            *semantic_family.components,
        )
        != semantics
    ):
        raise ArtifactPreparationConflictError(
            "artifact semantic tuple differs from independent evaluator"
        )
    old = _old_artifact(work, mutation, publication)
    new = semantics[0]
    expected_operation = _classify_artifact_operation(old, new)
    materialized_new = work.connector.fetch_one(
        "SELECT artifact_semantics_sha256 FROM catalog_candidate_artifact_inputs "
        "WHERE candidate_id = %s AND publication_key = %s",
        (mutation.candidate.candidate_id, publication),
    )
    materialized_operation = work.connector.fetch_one(
        "SELECT operation FROM catalog_artifact_operations "
        "WHERE candidate_id = %s AND publication_key = %s",
        (mutation.candidate.candidate_id, publication),
    )
    if materialized_new != (semantics[0],) or materialized_operation != (
        expected_operation,
    ):
        raise ArtifactPreparationConflictError(
            "artifact new delta or operation differs from evaluator"
        )
    materialized_old = work.connector.fetch_one(
        "SELECT artifact_semantics_sha256, artifact_sha256 "
        "FROM catalog_artifact_delta_old "
        "WHERE candidate_id = %s AND publication_key = %s",
        (mutation.candidate.candidate_id, publication),
    )
    expected_old = () if old is None else (old[0], old[1])
    if materialized_old != expected_old:
        raise ArtifactPreparationConflictError(
            "artifact old delta differs from evaluator"
        )


def _operation_keys(
    work: VNextUnitOfWork,
    mutation: _MutationAuthority,
    *,
    operations: tuple[str, ...],
    after: bytes,
) -> tuple[bytes, ...]:
    allowed = {"CREATE", "REBUILD", "DELETE", "UNCHANGED"}
    if not operations or any(value not in allowed for value in operations):
        raise ValueError("operation filter is not registered")
    _validate_publication_cursor(after)
    candidate = mutation.candidate.candidate_id
    base = None if mutation.base_catalog is None else mutation.base_catalog.revision
    if base is None:
        if "CREATE" not in operations:
            return ()
        rows = work.connector.fetch_all(
            "SELECT publication_key FROM catalog_candidate_artifact_inputs "
            "WHERE candidate_id = %s AND publication_key > %s "
            "ORDER BY publication_key LIMIT 128",
            (candidate, after),
        )
    elif operations == ("DELETE",):
        rows = work.connector.fetch_all(
            "SELECT publication_key FROM catalog_artifacts "
            "WHERE revision = %s AND publication_key > %s "
            "AND NOT EXISTS ("
            "SELECT 1 FROM catalog_candidate_artifact_inputs input "
            "WHERE input.candidate_id = %s "
            "AND input.publication_key = catalog_artifacts.publication_key) "
            "ORDER BY publication_key LIMIT 128",
            (base, after, candidate),
        )
    elif "DELETE" in operations:
        raise ValueError("DELETE cannot share an artifact-operation evaluator page")
    else:
        predicates: list[str] = []
        parameters: list[Any] = [base]
        old_exists = (
            "SELECT 1 FROM catalog_artifacts old_artifact "
            "WHERE old_artifact.revision = %s "
            "AND old_artifact.publication_key = input.publication_key"
        )
        if "CREATE" in operations:
            predicates.append(f"NOT EXISTS ({old_exists})")
            parameters.append(base)
        if "REBUILD" in operations:
            predicates.append(
                f"EXISTS ({old_exists} "
                "AND old_artifact.artifact_semantics_sha256 "
                "<> input.artifact_semantics_sha256)"
            )
            parameters.append(base)
        if "UNCHANGED" in operations:
            predicates.append(
                f"EXISTS ({old_exists} "
                "AND old_artifact.artifact_semantics_sha256 "
                "= input.artifact_semantics_sha256)"
            )
            parameters.append(base)
        # ``parameters`` starts with a sentinel solely so the branch above can
        # append one base revision per repeated correlated subquery.
        rows = work.connector.fetch_all(
            "SELECT input.publication_key "
            "FROM catalog_candidate_artifact_inputs input "
            "WHERE input.candidate_id = %s AND input.publication_key > %s AND ("
            + " OR ".join(predicates)
            + ") ORDER BY input.publication_key LIMIT 128",
            (candidate, after, *parameters[1:]),
        )
    result = tuple(
        require_digest32(row[0], field="evaluated operation publication")
        for row in rows
    )
    for publication in result:
        actual = _classify_artifact_operation(
            _old_artifact(work, mutation, publication),
            _new_artifact_input(work, mutation, publication),
        )
        if actual not in operations:
            raise ArtifactPreparationConflictError(
                "bounded artifact operation evaluator returned the wrong class"
            )
    return result


def _validate_operation_batch(
    *,
    stage: bytes,
    operation: str,
    work: VNextUnitOfWork,
    gate_lease: GateLease,
    ingest_turn: IngestTurn,
    candidate_id: bytes,
    batch_key: bytes,
    now: int,
) -> PublicationCandidateBatch:
    mutation, checkpoint, attempt, replay = _prepare_candidate_batch(
        work,
        gate_lease=gate_lease,
        ingest_turn=ingest_turn,
        candidate_id=candidate_id,
        stage=stage,
        batch_key=batch_key,
        now=now,
    )
    if replay is not None:
        return replay
    expected = _operation_keys(
        work,
        mutation,
        operations=(operation,),
        after=checkpoint.cursor,
    )
    rows = work.connector.fetch_all(
        "SELECT publication_key FROM catalog_artifact_operations "
        "WHERE candidate_id = %s AND operation = %s AND publication_key > %s "
        "ORDER BY publication_key LIMIT 128",
        (mutation.candidate.candidate_id, operation, checkpoint.cursor),
    )
    actual = tuple(
        require_digest32(row[0], field="validated operation publication")
        for row in rows
    )
    if actual != expected:
        raise ArtifactPreparationConflictError(
            f"materialized {operation} set differs from evaluator"
        )
    next_cursor = checkpoint.cursor if not expected else expected[-1]
    return _commit_candidate_batch(
        work,
        authority=mutation,
        checkpoint=checkpoint,
        stage=stage,
        batch_key=attempt,
        next_cursor=next_cursor,
        row_count=len(expected),
        terminal=not expected,
        now=now,
    )


# This is one fixed transaction query with a result/API bound of 128 keys.  The
# bound does not claim that a backend examines at most 128 index entries; the
# physical contract instead supplies leading (revision, publication_key, ...)
# keys and both backends must retain a selective EXPLAIN plan.  Display order is
# presentation state, while artifact bytes have their own B7 delta, so neither
# family participates in semantic item equality here.
_EXACT_CHANGED_ITEM_QUERY = """
WITH current_policy(display_title_policy_id) AS (SELECT %s)
SELECT current_item.publication_key
FROM catalog_publications AS current_item
JOIN catalog_publications AS old_item
  ON old_item.revision = %s
 AND old_item.publication_key = current_item.publication_key
WHERE current_item.revision = %s
  AND current_item.publication_key > %s
  AND (
    EXISTS (
      SELECT 1
      FROM catalog_publications AS current_scalar
      WHERE current_scalar.revision = current_item.revision
        AND current_scalar.publication_key = current_item.publication_key
        AND NOT EXISTS (
          SELECT 1
          FROM catalog_publications AS old_scalar
          WHERE old_scalar.revision = old_item.revision
            AND old_scalar.publication_key = old_item.publication_key
            AND old_scalar.gallery_id = current_scalar.gallery_id
            AND old_scalar.summary_sha256 = current_scalar.summary_sha256
            AND old_scalar.language_sha256 = current_scalar.language_sha256
            AND old_scalar.modified_at = current_scalar.modified_at
        )
    )
    OR EXISTS (
      SELECT 1
      FROM catalog_publications AS old_scalar
      WHERE old_scalar.revision = old_item.revision
        AND old_scalar.publication_key = old_item.publication_key
        AND NOT EXISTS (
          SELECT 1
          FROM catalog_publications AS current_scalar
          WHERE current_scalar.revision = current_item.revision
            AND current_scalar.publication_key = current_item.publication_key
            AND current_scalar.gallery_id = old_scalar.gallery_id
            AND current_scalar.summary_sha256 = old_scalar.summary_sha256
            AND current_scalar.language_sha256 = old_scalar.language_sha256
            AND current_scalar.modified_at = old_scalar.modified_at
        )
    )
    OR EXISTS (
      SELECT 1
      FROM catalog_publication_titles AS current_title
      CROSS JOIN current_policy
      JOIN catalog_display_title_choices AS current_choice
        ON current_choice.display_title_policy_id =
           current_policy.display_title_policy_id
       AND current_choice.source_title_sha256 =
           current_title.source_title_sha256
       AND current_choice.source_gallery_name =
           current_title.source_gallery_name
      JOIN catalog_display_title_policies AS current_policy_sort
        ON current_policy_sort.display_title_policy_id =
           current_policy.display_title_policy_id
      JOIN catalog_title_sorts AS current_sort
        ON current_sort.title_sort_policy_id =
           current_policy_sort.title_sort_policy_id
       AND current_sort.title_sha256 = current_choice.title_sha256
      WHERE current_title.revision = current_item.revision
        AND current_title.publication_key = current_item.publication_key
        AND NOT EXISTS (
          SELECT 1
          FROM catalog_publication_titles AS old_title
          JOIN catalog_publication_commits AS old_commit
            ON old_commit.revision = old_title.revision
          JOIN catalog_display_title_choices AS old_choice
            ON old_choice.display_title_policy_id =
               old_commit.display_title_policy_id
           AND old_choice.source_title_sha256 = old_title.source_title_sha256
           AND old_choice.source_gallery_name = old_title.source_gallery_name
          JOIN catalog_display_title_policies AS old_policy_sort
            ON old_policy_sort.display_title_policy_id =
               old_commit.display_title_policy_id
          JOIN catalog_title_sorts AS old_sort
            ON old_sort.title_sort_policy_id = old_policy_sort.title_sort_policy_id
           AND old_sort.title_sha256 = old_choice.title_sha256
          WHERE old_title.revision = old_item.revision
            AND old_title.publication_key = old_item.publication_key
            AND old_title.source_title_sha256 =
                current_title.source_title_sha256
            AND old_title.source_gallery_name =
                current_title.source_gallery_name
            AND old_choice.title_sha256 = current_choice.title_sha256
            AND old_sort.sort_title_sha256 = current_sort.sort_title_sha256
        )
    )
    OR EXISTS (
      SELECT 1
      FROM catalog_publication_titles AS old_title
      JOIN catalog_publication_commits AS old_commit
        ON old_commit.revision = old_title.revision
      JOIN catalog_display_title_choices AS old_choice
        ON old_choice.display_title_policy_id =
           old_commit.display_title_policy_id
       AND old_choice.source_title_sha256 = old_title.source_title_sha256
       AND old_choice.source_gallery_name = old_title.source_gallery_name
      JOIN catalog_display_title_policies AS old_policy_sort
        ON old_policy_sort.display_title_policy_id =
           old_commit.display_title_policy_id
      JOIN catalog_title_sorts AS old_sort
        ON old_sort.title_sort_policy_id = old_policy_sort.title_sort_policy_id
       AND old_sort.title_sha256 = old_choice.title_sha256
      WHERE old_title.revision = old_item.revision
        AND old_title.publication_key = old_item.publication_key
        AND NOT EXISTS (
          SELECT 1
          FROM catalog_publication_titles AS current_title
          CROSS JOIN current_policy
          JOIN catalog_display_title_choices AS current_choice
            ON current_choice.display_title_policy_id =
               current_policy.display_title_policy_id
           AND current_choice.source_title_sha256 =
               current_title.source_title_sha256
           AND current_choice.source_gallery_name =
               current_title.source_gallery_name
          JOIN catalog_display_title_policies AS current_policy_sort
            ON current_policy_sort.display_title_policy_id =
               current_policy.display_title_policy_id
          JOIN catalog_title_sorts AS current_sort
            ON current_sort.title_sort_policy_id =
               current_policy_sort.title_sort_policy_id
           AND current_sort.title_sha256 = current_choice.title_sha256
          WHERE current_title.revision = current_item.revision
            AND current_title.publication_key = current_item.publication_key
            AND current_title.source_title_sha256 = old_title.source_title_sha256
            AND current_title.source_gallery_name = old_title.source_gallery_name
            AND current_choice.title_sha256 = old_choice.title_sha256
            AND current_sort.sort_title_sha256 = old_sort.sort_title_sha256
        )
    )
    OR EXISTS (
      SELECT 1
      FROM catalog_publication_contents AS current_content
      WHERE current_content.revision = current_item.revision
        AND current_content.publication_key = current_item.publication_key
        AND NOT EXISTS (
          SELECT 1
          FROM catalog_publication_contents AS old_content
          WHERE old_content.revision = old_item.revision
            AND old_content.publication_key = old_item.publication_key
            AND old_content.content_sha256 = current_content.content_sha256
        )
    )
    OR EXISTS (
      SELECT 1
      FROM catalog_publication_contents AS old_content
      WHERE old_content.revision = old_item.revision
        AND old_content.publication_key = old_item.publication_key
        AND NOT EXISTS (
          SELECT 1
          FROM catalog_publication_contents AS current_content
          WHERE current_content.revision = current_item.revision
            AND current_content.publication_key = current_item.publication_key
            AND current_content.content_sha256 = old_content.content_sha256
        )
    )
    OR EXISTS (
      SELECT 1
      FROM catalog_contributors AS current_contributor
      WHERE current_contributor.revision = current_item.revision
        AND current_contributor.publication_key = current_item.publication_key
        AND NOT EXISTS (
          SELECT 1
          FROM catalog_contributors AS old_contributor
          WHERE old_contributor.revision = old_item.revision
            AND old_contributor.publication_key = old_item.publication_key
            AND old_contributor.position = current_contributor.position
            AND old_contributor.contributor_name_sha256 =
                current_contributor.contributor_name_sha256
            AND old_contributor.role = current_contributor.role
        )
    )
    OR EXISTS (
      SELECT 1
      FROM catalog_contributors AS old_contributor
      WHERE old_contributor.revision = old_item.revision
        AND old_contributor.publication_key = old_item.publication_key
        AND NOT EXISTS (
          SELECT 1
          FROM catalog_contributors AS current_contributor
          WHERE current_contributor.revision = current_item.revision
            AND current_contributor.publication_key = current_item.publication_key
            AND current_contributor.position = old_contributor.position
            AND current_contributor.contributor_name_sha256 =
                old_contributor.contributor_name_sha256
            AND current_contributor.role = old_contributor.role
        )
    )
    OR EXISTS (
      SELECT 1
      FROM catalog_subjects AS current_subject
      WHERE current_subject.revision = current_item.revision
        AND current_subject.publication_key = current_item.publication_key
        AND NOT EXISTS (
          SELECT 1
          FROM catalog_subjects AS old_subject
          WHERE old_subject.revision = old_item.revision
            AND old_subject.publication_key = old_item.publication_key
            AND old_subject.position = current_subject.position
            AND old_subject.tag_id = current_subject.tag_id
        )
    )
    OR EXISTS (
      SELECT 1
      FROM catalog_subjects AS old_subject
      WHERE old_subject.revision = old_item.revision
        AND old_subject.publication_key = old_item.publication_key
        AND NOT EXISTS (
          SELECT 1
          FROM catalog_subjects AS current_subject
          WHERE current_subject.revision = current_item.revision
            AND current_subject.publication_key = current_item.publication_key
            AND current_subject.position = old_subject.position
            AND current_subject.tag_id = old_subject.tag_id
        )
    )
  )
ORDER BY current_item.publication_key
LIMIT 128
"""


def _gallery_diff_keys(
    work: VNextUnitOfWork,
    mutation: _MutationAuthority,
    *,
    kind: str,
    after: bytes,
) -> tuple[bytes, ...]:
    revision = mutation.candidate.reserved_revision
    base = None if mutation.base_catalog is None else mutation.base_catalog.revision
    if kind == "NEW":
        if base is None:
            query = (
                "SELECT publication_key FROM catalog_publications "
                "WHERE revision = %s AND publication_key > %s "
                "ORDER BY publication_key LIMIT 128"
            )
            parameters: tuple[Any, ...] = (revision, after)
        else:
            query = (
                "SELECT current.publication_key FROM catalog_publications current "
                "LEFT JOIN catalog_publications old ON old.revision = %s "
                "AND old.publication_key = current.publication_key "
                "WHERE current.revision = %s AND current.publication_key > %s "
                "AND old.publication_key IS NULL "
                "ORDER BY current.publication_key LIMIT 128"
            )
            parameters = (base, revision, after)
    elif kind == "CHANGED":
        if base is None:
            return ()
        query = _EXACT_CHANGED_ITEM_QUERY
        parameters = (
            mutation.candidate.display_title_policy_id,
            base,
            revision,
            after,
        )
    elif kind == "REMOVED":
        if base is None:
            return ()
        query = (
            "SELECT old.publication_key FROM catalog_publications old "
            "LEFT JOIN catalog_publications current ON current.revision = %s "
            "AND current.publication_key = old.publication_key "
            "WHERE old.revision = %s AND old.publication_key > %s "
            "AND current.publication_key IS NULL "
            "ORDER BY old.publication_key LIMIT 128"
        )
        parameters = (revision, base, after)
    else:
        raise ValueError("gallery diff kind is not registered")
    rows = work.connector.fetch_all(query, parameters)
    return tuple(
        require_digest32(row[0], field="gallery diff publication_key") for row in rows
    )


def _validate_gallery_diff_batch(
    *,
    stage: bytes,
    kind: str,
    work: VNextUnitOfWork,
    gate_lease: GateLease,
    ingest_turn: IngestTurn,
    candidate_id: bytes,
    batch_key: bytes,
    now: int,
) -> PublicationCandidateBatch:
    mutation, checkpoint, attempt, replay = _prepare_candidate_batch(
        work,
        gate_lease=gate_lease,
        ingest_turn=ingest_turn,
        candidate_id=candidate_id,
        stage=stage,
        batch_key=batch_key,
        now=now,
    )
    if replay is not None:
        return replay
    rows = _gallery_diff_keys(work, mutation, kind=kind, after=checkpoint.cursor)
    next_cursor = checkpoint.cursor if not rows else rows[-1]
    return _commit_candidate_batch(
        work,
        authority=mutation,
        checkpoint=checkpoint,
        stage=stage,
        batch_key=attempt,
        next_cursor=next_cursor,
        row_count=len(rows),
        terminal=not rows,
        now=now,
    )


def _validate_prepared_row(
    work: VNextUnitOfWork,
    mutation: _MutationAuthority,
    publication_key: bytes,
) -> None:
    candidate = mutation.candidate.candidate_id
    revision = mutation.candidate.reserved_revision
    try:
        families = load_prepared_artifact_families(
            work.connector,
            candidate_id=candidate,
            publication_key=publication_key,
            backend=work.backend,
        )
        occurrence = load_catalog_artifact_family(
            work.connector,
            revision=revision,
            publication_key=publication_key,
            backend=work.backend,
        )
    except (ArtifactFamilyCollisionError, ArtifactFamilyPartialError) as error:
        raise ArtifactPreparationConflictError(
            "prepared artifact has an invalid narrow family"
        ) from error
    if occurrence is None:
        raise ArtifactPreparationConflictError(
            "prepared artifact lacks its catalog occurrence"
        )
    expected_kinds = (
        (
            CatalogResourceKind.ACQUISITION,
            CatalogResourceKind.THUMBNAIL,
        )
        if occurrence.page_count
        else (CatalogResourceKind.ACQUISITION,)
    )
    if tuple(family.resource_kind for family in families) != expected_kinds or any(
        family.state != "PREPARED" for family in families
    ):
        raise ArtifactPreparationConflictError(
            "candidate seal requires one exact PREPARED resource bundle"
        )

    operation = work.connector.fetch_one(
        "SELECT input.artifact_semantics_sha256, operation.operation, pub.gid "
        "FROM catalog_candidate_artifact_inputs input "
        "JOIN catalog_artifact_operations operation "
        "ON operation.candidate_id = input.candidate_id "
        "AND operation.publication_key = input.publication_key "
        "JOIN catalog_publication_identities pub "
        "ON pub.publication_key = input.publication_key "
        "WHERE input.candidate_id = %s AND input.publication_key = %s",
        (candidate, publication_key),
    )
    if len(operation) != 3 or operation[1] not in {"CREATE", "REBUILD"}:
        raise ArtifactPreparationConflictError(
            "prepared artifact has no exact byte-producing operation"
        )
    if occurrence.artifact_semantics_sha256 != operation[0]:
        raise ArtifactPreparationConflictError(
            "prepared catalog occurrence differs from its semantic input"
        )
    gid = require_positive_int63(operation[2], field="prepared artifact GID")
    if identity.publication_key(gid) != publication_key:
        raise ArtifactPreparationConflictError(
            "prepared artifact publication key differs from its GID"
        )

    descriptor = work.connector.fetch_one(
        "SELECT artifact_sha256, artifact_name, media_type, page_count "
        "FROM catalog_prepared_artifact_descriptors "
        "WHERE candidate_id = %s AND publication_key = %s",
        (candidate, publication_key),
    )
    if descriptor != (
        occurrence.artifact_sha256,
        occurrence.artifact_name,
        occurrence.media_type,
        occurrence.page_count,
    ):
        raise ArtifactPreparationConflictError(
            "prepared descriptor differs from catalog artifact occurrence"
        )
    blob = work.connector.fetch_one(
        "SELECT size_bytes FROM catalog_artifact_blobs WHERE artifact_sha256 = %s",
        (occurrence.artifact_sha256,),
    )

    objects: dict[CatalogResourceKind, PreparedStorageObjectFamily] = {}
    for family in families:
        expected_token = identity.encode_artifact_protection_token(
            candidate,
            publication_key,
            family.resource_kind.value,
            family.storage_object_key_sha256,
            family.storage_generation,
        )
        if expected_token != family.protection_token:
            raise ArtifactPreparationConflictError(
                "prepared protection token differs from locked resource facts"
            )
        mapping = work.connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (family.storage_generation,),
        )
        if mapping != (mutation.begin.build_id,):
            raise ArtifactPreparationConflictError(
                "prepared storage generation is not mapped to the candidate build"
            )
        _load_storage_key(work, family.storage_object_key_sha256)
        stored = load_prepared_storage_object_family(
            work.connector,
            candidate_id=candidate,
            publication_key=publication_key,
            resource_kind=family.resource_kind,
            backend=work.backend,
        )
        if stored is None:
            raise ArtifactPreparationConflictError(
                "prepared resource lacks its sealed storage object"
            )
        resource_blob = work.connector.fetch_all(
            "SELECT binding.storage_object_sha256, blob_row.size_bytes "
            "FROM catalog_prepared_resource_blob AS binding "
            "JOIN catalog_artifact_blobs AS blob_row "
            "ON blob_row.artifact_sha256 = binding.storage_object_sha256 "
            "WHERE binding.candidate_id = %s AND binding.publication_key = %s "
            "AND binding.resource_kind = %s LIMIT 2",
            (
                candidate,
                publication_key,
                family.resource_kind.value.encode("ascii"),
            ),
        )
        if tuple(resource_blob) != ((stored.storage_object_sha256, stored.size_bytes),):
            raise ArtifactPreparationConflictError(
                "prepared resource differs from its durable byte authority"
            )
        objects[family.resource_kind] = stored
        catalog_object = work.connector.fetch_one(
            "SELECT storage_object_key_sha256, storage_object_sha256, "
            "size_bytes, modified_at FROM catalog_storage_objects "
            "WHERE revision = %s AND publication_key = %s AND resource_kind = %s",
            (
                revision,
                publication_key,
                family.resource_kind.value.encode("ascii"),
            ),
        )
        if catalog_object != (
            family.storage_object_key_sha256,
            stored.storage_object_sha256,
            stored.size_bytes,
            stored.modified_at,
        ):
            raise ArtifactPreparationConflictError(
                "catalog storage object differs from prepared resource"
            )

    acquisition = objects[CatalogResourceKind.ACQUISITION]
    if acquisition.storage_object_sha256 != occurrence.artifact_sha256 or blob != (
        acquisition.size_bytes,
    ):
        raise ArtifactPreparationConflictError(
            "acquisition object differs from artifact blob authority"
        )

    prepared_pages = work.connector.fetch_all(
        "SELECT resource_kind, page_index, extent_offset, extent_length, "
        "media_type, image_sha256, width, height FROM catalog_prepared_pages "
        "WHERE candidate_id = %s AND publication_key = %s ORDER BY page_index",
        (candidate, publication_key),
    )
    catalog_pages = work.connector.fetch_all(
        "SELECT resource_kind, page_index, extent_offset, extent_length, "
        "media_type, image_sha256, width, height FROM catalog_pages "
        "WHERE revision = %s AND publication_key = %s ORDER BY page_index",
        (revision, publication_key),
    )
    if (
        tuple(prepared_pages) != tuple(catalog_pages)
        or len(prepared_pages) != occurrence.page_count
        or tuple(row[1] for row in prepared_pages)
        != tuple(range(occurrence.page_count))
    ):
        raise ArtifactPreparationConflictError(
            "prepared page coverage is not exact and dense"
        )
    acquisition_kind = CatalogResourceKind.ACQUISITION.value.encode("ascii")
    for row in prepared_pages:
        offset = require_int63(row[2], field="prepared page extent offset")
        length = require_positive_int63(row[3], field="prepared page extent length")
        if row[0] != acquisition_kind or offset + length > acquisition.size_bytes:
            raise ArtifactPreparationConflictError(
                "prepared page extent differs from acquisition authority"
            )
        require_digest32(row[5], field="prepared page image_sha256")
        require_positive_int63(row[6], field="prepared page width")
        require_positive_int63(row[7], field="prepared page height")

    prepared_thumbnail = work.connector.fetch_all(
        "SELECT resource_kind, extent_offset, extent_length, media_type, "
        "image_sha256, width, height FROM catalog_prepared_thumbnails "
        "WHERE candidate_id = %s AND publication_key = %s LIMIT 2",
        (candidate, publication_key),
    )
    catalog_thumbnail = work.connector.fetch_all(
        "SELECT resource_kind, extent_offset, extent_length, media_type, "
        "image_sha256, width, height FROM catalog_thumbnails "
        "WHERE revision = %s AND publication_key = %s LIMIT 2",
        (revision, publication_key),
    )
    if tuple(prepared_thumbnail) != tuple(catalog_thumbnail):
        raise ArtifactPreparationConflictError(
            "prepared thumbnail differs from catalog occurrence"
        )
    if not occurrence.page_count:
        if prepared_thumbnail:
            raise ArtifactPreparationConflictError(
                "pageless acquisition has a thumbnail resource"
            )
        return
    thumbnail = objects[CatalogResourceKind.THUMBNAIL]
    thumbnail_kind = CatalogResourceKind.THUMBNAIL.value.encode("ascii")
    if (
        len(prepared_thumbnail) != 1
        or prepared_thumbnail[0][0] != thumbnail_kind
        or prepared_thumbnail[0][1] != 0
        or prepared_thumbnail[0][2] != thumbnail.size_bytes
        or prepared_thumbnail[0][4] != thumbnail.storage_object_sha256
    ):
        raise ArtifactPreparationConflictError(
            "thumbnail is not the exact complete auxiliary object"
        )


_SEAL_STAGES = (
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


def _seal_projection(
    work: VNextUnitOfWork,
    mutation: _MutationAuthority,
    *,
    now: int,
) -> None:
    timestamp = require_int63(now, field="projection certification now")
    candidate = mutation.candidate.candidate_id
    # ``_prepare_candidate_batch`` already locked this complete immutable
    # candidate before any checkpoint lock; re-read it without changing order.
    definition = work.connector.fetch_one(
        "SELECT candidate_id FROM catalog_publication_candidates "
        "WHERE candidate_id = %s",
        (candidate,),
    )
    if definition != (candidate,):
        raise ArtifactPreparationConflictError(
            "projection certification lacks its candidate definition seal"
        )
    counts: dict[bytes, int] = {}
    latest_terminal_at = 0
    for stage in _SEAL_STAGES:
        checkpoint, _receipt = _load_complete_stage_receipt(
            work,
            candidate,
            stage,
            cursor_maximum=8 if stage == b"VALIDATE_DUPLICATE_LOSER" else 2048,
        )
        counts[stage] = checkpoint[2]
        latest_terminal_at = max(latest_terminal_at, checkpoint[4])
    publication_count = counts[b"VALIDATE_SELECTION"]
    artifact_inputs = counts[b"VALIDATE_ARTIFACT_INPUT_DELTA"]
    prepared = counts[b"VALIDATE_PREPARED_ARTIFACT"]
    create = counts[b"VALIDATE_CREATE"]
    rebuild = counts[b"VALIDATE_REBUILD"]
    unchanged = counts[b"VALIDATE_UNCHANGED"]
    if prepared != create + rebuild:
        raise ArtifactPreparationConflictError(
            "prepared artifact count differs from CREATE plus REBUILD"
        )
    if artifact_inputs != create + rebuild + unchanged:
        raise ArtifactPreparationConflictError(
            "artifact input count differs from byte-producing plus unchanged"
        )
    revision = work.connector.fetch_one(
        "SELECT publication_count, artifact_count "
        "FROM catalog_revision_descriptors "
        "WHERE revision = %s",
        (mutation.candidate.reserved_revision,),
    )
    expected_artifact_count = (
        publication_count if mutation.candidate.artifacts_required else 0
    )
    if revision != (publication_count, expected_artifact_count):
        raise ArtifactPreparationConflictError(
            "reserved catalog revision counts differ from selection"
        )
    preparation = work.connector.fetch_one(
        "SELECT binding.preparation_id, preparation.build_id, preparation.state, "
        "preparation.completed_at, seal.sealed_at "
        "FROM operational_publication_candidate_preparations binding "
        "JOIN operational_operational_preparations preparation "
        "ON preparation.preparation_id = binding.preparation_id "
        "JOIN operational_operational_preparation_effect_seals seal "
        "ON seal.preparation_id = preparation.preparation_id "
        "WHERE binding.candidate_id = %s",
        (candidate,),
    )
    if (
        len(preparation) != 5
        or preparation[1] != mutation.begin.build_id
        or preparation[2] != "COMPLETE"
        or preparation[3] is None
        or preparation[4] is None
    ):
        raise ArtifactPreparationNotReadyError(
            "candidate lacks one COMPLETE operational preparation effect seal"
        )
    if timestamp < latest_terminal_at:
        raise ArtifactPreparationNotReadyError(
            "projection certification timestamp precedes a terminal receipt"
        )
    _insert_or_compare(
        work,
        "catalog_publication_candidate_projection_seals",
        ("candidate_id",),
        (candidate,),
        key_where="candidate_id = %s",
        key_parameters=(candidate,),
        conflict_label="publication projection seal",
    )


def _datetime_from_microseconds(value: object, *, field: str) -> datetime:
    microseconds = require_int63(value, field=field)
    try:
        return _EPOCH + timedelta(microseconds=microseconds)
    except OverflowError as error:
        raise ArtifactPreparationConflictError(
            f"{field} is outside the supported datetime range"
        ) from error


def _require_matching_adapter(
    authority: ArtifactPreparationAuthority,
    adapter: ArtifactStorageAdapter,
) -> None:
    if not isinstance(adapter, ArtifactStorageAdapter):
        raise TypeError("adapter must implement ArtifactStorageAdapter")
    adapter_id = require_bounded_bytes(
        adapter.adapter_id,
        field="artifact adapter_id",
        minimum=1,
        maximum=64,
    )
    fingerprint = require_digest32(
        adapter.policy_fingerprint_sha256,
        field="artifact adapter policy fingerprint",
    )
    if (
        adapter_id != authority.adapter_id
        or fingerprint != authority.policy_fingerprint_sha256
    ):
        raise ArtifactPreparationContractUnavailableError(
            "artifact adapter differs from the registered policy authority"
        )


def _adapter_storage_key(
    adapter: ArtifactStorageAdapter,
    *,
    gid: int,
    resource_kind: CatalogResourceKind,
) -> StorageObjectKey:
    exact_gid = require_positive_int63(gid, field="artifact storage-key GID")
    if type(resource_kind) is not CatalogResourceKind:
        raise TypeError("artifact resource kind is not registered")
    try:
        first = adapter.storage_key(exact_gid, resource_kind)
        second = adapter.storage_key(exact_gid, resource_kind)
    except (AttributeError, TypeError, ValueError) as error:
        raise ArtifactPreparationContractUnavailableError(
            "artifact adapter cannot issue its deterministic storage key"
        ) from error
    if not isinstance(first, StorageObjectKey) or second != first:
        raise ArtifactPreparationContractUnavailableError(
            "artifact adapter returned an invalid or nondeterministic storage key"
        )
    first.__post_init__()
    return first


def _require_preparation_receipt(receipt: ArtifactPreparationReceipt) -> None:
    if not isinstance(receipt, ArtifactPreparationReceipt):
        raise TypeError("receipt must be ArtifactPreparationReceipt")
    if receipt._capability is not _PREPARATION_RECEIPT_TOKEN or receipt._closed:
        raise TypeError(
            "artifact preparation receipt is not live and repository-issued"
        )
    receipt.audit.__post_init__()
    _require_authority(receipt.audit.authority)
    receipt.render_evidence.__post_init__()
    receipt.presentation.__post_init__()
    artifact_digest = require_digest32(
        receipt.artifact_sha256,
        field="prepared artifact_sha256",
    )
    artifact_size = require_positive_int63(
        receipt.size_bytes,
        field="prepared artifact size_bytes",
    )
    if (
        receipt.render_evidence.artifact_sha256 != artifact_digest
        or receipt.render_evidence.size_bytes != artifact_size
        or len(receipt.render_evidence.pages) != len(receipt.presentation.pages)
    ):
        raise ArtifactPreparationConflictError(
            "prepared render evidence differs from verified resource facts"
        )
    if _hash_stream(receipt._archive) != (artifact_digest, artifact_size):
        raise ArtifactPreparationConflictError(
            "verified acquisition bytes changed before durable persistence"
        )
    acquisition = receipt.storage_object
    acquisition.__post_init__()
    if (
        bytes.fromhex(acquisition.sha256) != artifact_digest
        or acquisition.size_bytes != artifact_size
        or identity.artifact_storage_key_digest(
            acquisition.key.codec,
            acquisition.key.segments,
        )
        != receipt.artifact_storage_key_sha256
    ):
        raise ArtifactPreparationConflictError(
            "verified acquisition descriptor differs from its exact bytes"
        )
    for page in receipt.presentation.pages:
        page.__post_init__()
        if page.storage_object != acquisition:
            raise ArtifactPreparationConflictError(
                "prepared page references another acquisition object"
            )
    thumbnail = receipt.presentation.thumbnail
    if thumbnail is None:
        if receipt.resource_kinds != (CatalogResourceKind.ACQUISITION,):
            raise ArtifactPreparationConflictError(
                "empty presentation exposes an unexpected resource"
            )
        return
    thumbnail.__post_init__()
    if (
        thumbnail.extent.offset != 0
        or thumbnail.extent.length != thumbnail.storage_object.size_bytes
        or thumbnail.sha256 != thumbnail.storage_object.sha256
    ):
        raise ArtifactPreparationConflictError(
            "thumbnail is not the exact complete auxiliary storage object"
        )
    if _hash_stream(receipt._presentation_artifact.thumbnail) != (
        bytes.fromhex(thumbnail.storage_object.sha256),
        thumbnail.storage_object.size_bytes,
    ):
        raise ArtifactPreparationConflictError(
            "verified thumbnail bytes changed before durable persistence"
        )


def _authorize_artifact_mutation(
    work: VNextUnitOfWork,
    *,
    gate_lease: GateLease,
    ingest_turn: IngestTurn,
    now: int,
) -> int:
    gate = MaintenanceGateRepository.lock_and_require_live(work, gate_lease, now=now)
    if gate.mode is not GateMode.SHARED:
        raise ArtifactPreparationNotReadyError(
            "artifact mutation requires a live SHARED gate"
        )
    turn = IngestFenceRepository.lock_and_require_live(work, ingest_turn, now=now)
    return require_int63(turn.generation, field="artifact ingest generation")


def _intent_facts(intent: ArtifactProtectionIntent) -> tuple[Any, ...]:
    intent.__post_init__()
    return (
        intent.candidate_id,
        intent.publication_key,
        intent.resource_kind,
        intent.storage_object,
        intent.storage_object_key_sha256,
        intent.storage_generation,
        intent.protection_token,
        intent.state,
    )


def _load_prepared_family_or_conflict(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
    publication_key: bytes,
    resource_kind: CatalogResourceKind,
    backend: str,
) -> PreparedArtifactFamily | None:
    try:
        return load_prepared_artifact_family(
            connector,
            candidate_id=candidate_id,
            publication_key=publication_key,
            resource_kind=resource_kind,
            backend=backend,
        )
    except (ArtifactFamilyCollisionError, ArtifactFamilyPartialError) as error:
        raise ArtifactPreparationConflictError(
            "prepared resource family is partial or internally inconsistent"
        ) from error


def _load_prepared_families_or_conflict(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
    publication_key: bytes,
    backend: str,
) -> tuple[PreparedArtifactFamily, ...]:
    try:
        return load_prepared_artifact_families(
            connector,
            candidate_id=candidate_id,
            publication_key=publication_key,
            backend=backend,
        )
    except (ArtifactFamilyCollisionError, ArtifactFamilyPartialError) as error:
        raise ArtifactPreparationConflictError(
            "prepared resource bundle is partial or internally inconsistent"
        ) from error


def _resource_blob_facts(
    receipt: ArtifactPreparationReceipt,
    resource_kind: CatalogResourceKind,
) -> tuple[bytes, int]:
    descriptor = receipt.resource_descriptor(resource_kind)
    descriptor.__post_init__()
    expected = (
        bytes.fromhex(descriptor.sha256),
        descriptor.size_bytes,
    )
    actual = _hash_stream(receipt.resource_stream(resource_kind))
    if actual != expected:
        raise ArtifactPreparationConflictError(
            "prepared resource bytes differ from their verified descriptor"
        )
    return expected


def _persist_prepared_resource_blob(
    work: VNextUnitOfWork,
    receipt: ArtifactPreparationReceipt,
    family: PreparedArtifactFamily,
) -> None:
    family.__post_init__()
    digest, size_bytes = _resource_blob_facts(receipt, family.resource_kind)
    _insert_or_compare(
        work,
        "catalog_artifact_blobs",
        ("artifact_sha256", "size_bytes"),
        (digest, size_bytes),
        key_where="artifact_sha256 = %s",
        key_parameters=(digest,),
        conflict_label="prepared resource blob",
    )
    _insert_or_compare(
        work,
        "catalog_prepared_resource_blob",
        (
            "candidate_id",
            "publication_key",
            "resource_kind",
            "storage_object_sha256",
        ),
        (
            family.candidate_id,
            family.publication_key,
            family.resource_kind.value.encode("ascii"),
            digest,
        ),
        key_where=("candidate_id = %s AND publication_key = %s AND resource_kind = %s"),
        key_parameters=(
            family.candidate_id,
            family.publication_key,
            family.resource_kind.value.encode("ascii"),
        ),
        conflict_label="prepared resource blob binding",
    )


def _validate_prepared_resource_blob(
    connector: SQLConnector,
    receipt: ArtifactPreparationReceipt,
    family: PreparedArtifactFamily,
) -> None:
    family.__post_init__()
    digest, size_bytes = _resource_blob_facts(receipt, family.resource_kind)
    rows = connector.fetch_all(
        "SELECT binding.storage_object_sha256, blob_row.size_bytes "
        "FROM catalog_prepared_resource_blob AS binding "
        "JOIN catalog_artifact_blobs AS blob_row "
        "ON blob_row.artifact_sha256 = binding.storage_object_sha256 "
        "WHERE binding.candidate_id = %s AND binding.publication_key = %s "
        "AND binding.resource_kind = %s LIMIT 2",
        (
            family.candidate_id,
            family.publication_key,
            family.resource_kind.value.encode("ascii"),
        ),
    )
    if tuple(rows) != ((digest, size_bytes),):
        raise ArtifactPreparationConflictError(
            "durable resource blob binding differs from core-verified bytes"
        )


def _protection_intent_from_family(
    connector: SQLConnector,
    receipt: ArtifactPreparationReceipt,
    family: PreparedArtifactFamily,
    *,
    replayed: bool,
) -> ArtifactProtectionIntent:
    authority = receipt.audit.authority
    family.__post_init__()
    if (
        family.candidate_id != authority.candidate_id
        or family.publication_key != authority.publication_key
    ):
        raise ArtifactPreparationConflictError(
            "durable prepared resource belongs to another authority"
        )
    descriptor = receipt.resource_descriptor(family.resource_kind)
    key_digest = identity.artifact_storage_key_digest(
        descriptor.key.codec,
        descriptor.key.segments,
    )
    if family.storage_object_key_sha256 != key_digest:
        raise ArtifactPreparationConflictError(
            "durable prepared resource differs from its adapter-issued storage key"
        )
    mapping = connector.fetch_one(
        "SELECT build_id FROM operational_source_build_generations "
        "WHERE generation = %s",
        (family.storage_generation,),
    )
    if mapping != (authority.build_id,):
        raise ArtifactPreparationConflictError(
            "prepared storage generation is not mapped to the candidate build"
        )
    return ArtifactProtectionIntent(
        authority.candidate_id,
        authority.publication_key,
        family.resource_kind,
        descriptor,
        key_digest,
        family.storage_generation,
        family.protection_token,
        family.state,
        replayed,
        _capability=_PROTECTION_INTENT_TOKEN,
    )


def _persist_artifact_render_facts(
    work: VNextUnitOfWork,
    receipt: ArtifactPreparationReceipt,
) -> None:
    authority = receipt.audit.authority
    evidence = receipt.render_evidence
    _insert_or_compare(
        work,
        "catalog_artifact_blobs",
        ("artifact_sha256", "size_bytes"),
        (receipt.artifact_sha256, receipt.size_bytes),
        key_where="artifact_sha256 = %s",
        key_parameters=(receipt.artifact_sha256,),
        conflict_label="artifact blob",
    )
    _insert_or_compare(
        work,
        "catalog_prepared_artifact_descriptors",
        (
            "candidate_id",
            "publication_key",
            "artifact_sha256",
            "artifact_name",
            "media_type",
            "page_count",
        ),
        (
            authority.candidate_id,
            authority.publication_key,
            receipt.artifact_sha256,
            evidence.download_name.encode("utf-8", errors="strict"),
            evidence.media_type.encode("ascii", errors="strict"),
            len(receipt.presentation.pages),
        ),
        key_where="candidate_id = %s AND publication_key = %s",
        key_parameters=(authority.candidate_id, authority.publication_key),
        conflict_label="prepared artifact descriptor",
    )


def _validate_artifact_render_facts(
    work: VNextUnitOfWork,
    receipt: ArtifactPreparationReceipt,
) -> None:
    authority = receipt.audit.authority
    evidence = receipt.render_evidence
    expected_blob = (receipt.artifact_sha256, receipt.size_bytes)
    blob = work.connector.fetch_all(
        "SELECT artifact_sha256, size_bytes FROM catalog_artifact_blobs "
        "WHERE artifact_sha256 = %s LIMIT 2",
        (receipt.artifact_sha256,),
    )
    expected_descriptor = (
        authority.candidate_id,
        authority.publication_key,
        receipt.artifact_sha256,
        evidence.download_name.encode("utf-8", errors="strict"),
        evidence.media_type.encode("ascii", errors="strict"),
        len(receipt.presentation.pages),
    )
    descriptor = work.connector.fetch_all(
        "SELECT candidate_id, publication_key, artifact_sha256, artifact_name, "
        "media_type, page_count FROM catalog_prepared_artifact_descriptors "
        "WHERE candidate_id = %s AND publication_key = %s LIMIT 2",
        (authority.candidate_id, authority.publication_key),
    )
    if tuple(blob) != (expected_blob,) or tuple(descriptor) != (expected_descriptor,):
        raise ArtifactPreparationConflictError(
            "durable prepared artifact descriptor differs from render evidence"
        )


def _persist_storage_key(
    work: VNextUnitOfWork,
    key: StorageObjectKey,
    *,
    key_digest: bytes,
) -> None:
    key.__post_init__()
    digest = require_digest32(key_digest, field="storage object key digest")
    expected_digest = identity.artifact_storage_key_digest(key.codec, key.segments)
    if digest != expected_digest:
        raise ArtifactPreparationConflictError(
            "storage object key digest differs from its framed key"
        )
    _insert_or_compare(
        work,
        "catalog_storage_object_key_identities",
        ("storage_object_key_sha256", "key_codec", "segment_count"),
        (digest, key.codec.encode("ascii"), len(key.segments)),
        key_where="storage_object_key_sha256 = %s",
        key_parameters=(digest,),
        conflict_label="storage object key identity",
    )
    for position, segment in enumerate(key.segments):
        _insert_or_compare(
            work,
            "catalog_storage_object_key_segments",
            ("storage_object_key_sha256", "segment_position", "key_segment"),
            (digest, position, segment.encode("utf-8")),
            key_where=("storage_object_key_sha256 = %s AND segment_position = %s"),
            key_parameters=(digest, position),
            conflict_label="storage object key segment",
        )
    _validate_storage_key(work, key, key_digest=digest)


def _validate_storage_key(
    work: VNextUnitOfWork,
    key: StorageObjectKey,
    *,
    key_digest: bytes,
) -> None:
    key.__post_init__()
    digest = require_digest32(key_digest, field="storage object key digest")
    identity_row = work.connector.fetch_all(
        "SELECT storage_object_key_sha256, key_codec, segment_count "
        "FROM catalog_storage_object_key_identities "
        "WHERE storage_object_key_sha256 = %s LIMIT 2",
        (digest,),
    )
    segments = work.connector.fetch_all(
        "SELECT segment_position, key_segment "
        "FROM catalog_storage_object_key_segments "
        "WHERE storage_object_key_sha256 = %s ORDER BY segment_position",
        (digest,),
    )
    expected_segments = tuple(
        (position, segment.encode("utf-8"))
        for position, segment in enumerate(key.segments)
    )
    if (
        identity.artifact_storage_key_digest(key.codec, key.segments) != digest
        or tuple(identity_row)
        != ((digest, key.codec.encode("ascii"), len(key.segments)),)
        or tuple(segments) != expected_segments
    ):
        raise ArtifactPreparationConflictError(
            "durable storage object key is incomplete or noncongruent"
        )


def _load_storage_key(
    work: VNextUnitOfWork,
    key_digest: bytes,
) -> StorageObjectKey:
    digest = require_digest32(key_digest, field="storage object key digest")
    row = work.connector.fetch_one(
        "SELECT key_codec, segment_count "
        "FROM catalog_storage_object_key_identities "
        "WHERE storage_object_key_sha256 = %s",
        (digest,),
    )
    segments = work.connector.fetch_all(
        "SELECT segment_position, key_segment "
        "FROM catalog_storage_object_key_segments "
        "WHERE storage_object_key_sha256 = %s ORDER BY segment_position",
        (digest,),
    )
    if len(row) != 2 or row[1] != len(segments):
        raise ArtifactPreparationConflictError("storage object key family is partial")
    if tuple(item[0] for item in segments) != tuple(range(len(segments))):
        raise ArtifactPreparationConflictError(
            "storage object key segments are not dense"
        )
    try:
        key = StorageObjectKey(
            bytes(row[0]).decode("ascii", errors="strict"),
            tuple(bytes(item[1]).decode("utf-8", errors="strict") for item in segments),
        )
    except (TypeError, ValueError, UnicodeDecodeError) as error:
        raise ArtifactPreparationConflictError(
            "storage object key family contains invalid bytes"
        ) from error
    _validate_storage_key(work, key, key_digest=digest)
    return key


def _modified_at_microseconds(descriptor: StorageObjectDescriptor) -> int:
    return microseconds_from_datetime(
        descriptor.modified_at,
        field="prepared storage object modified_at",
    )


def _image_row_values(
    image: Any,
) -> tuple[int, int, bytes, bytes, int, int]:
    return (
        image.extent.offset,
        image.extent.length,
        image.media_type.encode("ascii", errors="strict"),
        bytes.fromhex(image.sha256),
        image.width,
        image.height,
    )


def _store_exact_row(
    work: VNextUnitOfWork,
    table: str,
    columns: tuple[str, ...],
    expected: tuple[Any, ...],
    *,
    key_where: str,
    key_parameters: tuple[Any, ...],
    conflict_label: str,
    allow_insert: bool,
) -> None:
    if allow_insert:
        _insert_or_compare(
            work,
            table,
            columns,
            expected,
            key_where=key_where,
            key_parameters=key_parameters,
            conflict_label=conflict_label,
        )
        return
    rows = work.connector.fetch_all(
        f"SELECT {', '.join(columns)} FROM {table} WHERE {key_where} LIMIT 2",
        key_parameters,
    )
    if tuple(rows) != (expected,):
        raise ArtifactPreparationConflictError(
            f"{conflict_label} is absent or differs from exact prepared facts"
        )


def _persist_confirmed_presentation(
    work: VNextUnitOfWork,
    mutation: _MutationAuthority,
    *,
    receipt: ArtifactPreparationReceipt,
    evidence: tuple[ArtifactProtectionEvidence, ...],
) -> None:
    expected_descriptors = tuple(
        receipt.resource_descriptor(kind) for kind in receipt.resource_kinds
    )
    if tuple(item.storage_object for item in evidence) != expected_descriptors:
        raise ArtifactPreparationConflictError(
            "protection evidence does not exactly cover prepared storage objects"
        )
    _write_or_validate_confirmed_presentation(
        work,
        mutation,
        receipt=receipt,
        allow_insert=True,
    )


def _validate_confirmed_presentation(
    work: VNextUnitOfWork,
    mutation: _MutationAuthority,
    *,
    receipt: ArtifactPreparationReceipt,
) -> None:
    _write_or_validate_confirmed_presentation(
        work,
        mutation,
        receipt=receipt,
        allow_insert=False,
    )


def _write_or_validate_confirmed_presentation(
    work: VNextUnitOfWork,
    mutation: _MutationAuthority,
    *,
    receipt: ArtifactPreparationReceipt,
    allow_insert: bool,
) -> None:
    authority = receipt.audit.authority
    candidate = authority.candidate_id
    publication = authority.publication_key
    revision = mutation.candidate.reserved_revision
    if (
        mutation.candidate.candidate_id != candidate
        or revision != authority.projection.reserved_revision
    ):
        raise ArtifactPreparationConflictError(
            "presentation mutation authority differs from prepared receipt"
        )

    for kind in receipt.resource_kinds:
        descriptor = receipt.resource_descriptor(kind)
        key_digest = identity.artifact_storage_key_digest(
            descriptor.key.codec,
            descriptor.key.segments,
        )
        _validate_storage_key(work, descriptor.key, key_digest=key_digest)
        family = PreparedStorageObjectFamily(
            candidate,
            publication,
            kind,
            bytes.fromhex(descriptor.sha256),
            descriptor.size_bytes,
            _modified_at_microseconds(descriptor),
        )
        try:
            if allow_insert:
                ensure_prepared_storage_object_family(
                    work.connector,
                    family,
                    backend=work.backend,
                )
            elif (
                load_prepared_storage_object_family(
                    work.connector,
                    candidate_id=candidate,
                    publication_key=publication,
                    resource_kind=kind,
                    backend=work.backend,
                )
                != family
            ):
                raise ArtifactPreparationConflictError(
                    "prepared storage object differs from verified bytes"
                )
        except (ArtifactFamilyCollisionError, ArtifactFamilyPartialError) as error:
            raise ArtifactPreparationConflictError(
                "prepared storage object collides with durable facts"
            ) from error
        object_values = (
            revision,
            publication,
            kind.value.encode("ascii"),
            key_digest,
            bytes.fromhex(descriptor.sha256),
            descriptor.size_bytes,
            _modified_at_microseconds(descriptor),
        )
        _store_exact_row(
            work,
            "catalog_storage_objects",
            (
                "revision",
                "publication_key",
                "resource_kind",
                "storage_object_key_sha256",
                "storage_object_sha256",
                "size_bytes",
                "modified_at",
            ),
            object_values,
            key_where=("revision = %s AND publication_key = %s AND resource_kind = %s"),
            key_parameters=(
                revision,
                publication,
                kind.value.encode("ascii"),
            ),
            conflict_label="catalog storage object",
            allow_insert=allow_insert,
        )

    render = receipt.render_evidence
    artifact_family = CatalogArtifactFamily(
        revision,
        publication,
        receipt.artifact_sha256,
        authority.artifact_semantics_sha256,
        render.download_name.encode("utf-8", errors="strict"),
        render.media_type.encode("ascii", errors="strict"),
        len(receipt.presentation.pages),
    )
    try:
        if allow_insert:
            ensure_catalog_artifact_family(
                work.connector,
                artifact_family,
                backend=work.backend,
            )
        elif (
            load_catalog_artifact_family(
                work.connector,
                revision=revision,
                publication_key=publication,
                backend=work.backend,
            )
            != artifact_family
        ):
            raise ArtifactPreparationConflictError(
                "catalog artifact differs from verified render facts"
            )
    except (ArtifactFamilyCollisionError, ArtifactFamilyPartialError) as error:
        raise ArtifactPreparationConflictError(
            "catalog artifact collides with durable facts"
        ) from error

    expected_prepared_pages: list[tuple[Any, ...]] = []
    expected_catalog_pages: list[tuple[Any, ...]] = []
    acquisition_kind = CatalogResourceKind.ACQUISITION.value.encode("ascii")
    for page in receipt.presentation.pages:
        image = _image_row_values(page)
        prepared_row = (
            candidate,
            publication,
            acquisition_kind,
            page.page_index,
            *image,
        )
        catalog_row = (
            revision,
            publication,
            acquisition_kind,
            page.page_index,
            *image,
        )
        expected_prepared_pages.append(prepared_row)
        expected_catalog_pages.append(catalog_row)
        _store_exact_row(
            work,
            "catalog_prepared_pages",
            (
                "candidate_id",
                "publication_key",
                "resource_kind",
                "page_index",
                "extent_offset",
                "extent_length",
                "media_type",
                "image_sha256",
                "width",
                "height",
            ),
            prepared_row,
            key_where=(
                "candidate_id = %s AND publication_key = %s AND page_index = %s"
            ),
            key_parameters=(candidate, publication, page.page_index),
            conflict_label="prepared page",
            allow_insert=allow_insert,
        )
        _store_exact_row(
            work,
            "catalog_pages",
            (
                "revision",
                "publication_key",
                "resource_kind",
                "page_index",
                "extent_offset",
                "extent_length",
                "media_type",
                "image_sha256",
                "width",
                "height",
            ),
            catalog_row,
            key_where=("revision = %s AND publication_key = %s AND page_index = %s"),
            key_parameters=(revision, publication, page.page_index),
            conflict_label="catalog page",
            allow_insert=allow_insert,
        )

    actual_prepared_pages = work.connector.fetch_all(
        "SELECT candidate_id, publication_key, resource_kind, page_index, "
        "extent_offset, extent_length, media_type, image_sha256, width, height "
        "FROM catalog_prepared_pages WHERE candidate_id = %s "
        "AND publication_key = %s ORDER BY page_index",
        (candidate, publication),
    )
    actual_catalog_pages = work.connector.fetch_all(
        "SELECT revision, publication_key, resource_kind, page_index, "
        "extent_offset, extent_length, media_type, image_sha256, width, height "
        "FROM catalog_pages WHERE revision = %s AND publication_key = %s "
        "ORDER BY page_index",
        (revision, publication),
    )
    if tuple(actual_prepared_pages) != tuple(expected_prepared_pages) or tuple(
        actual_catalog_pages
    ) != tuple(expected_catalog_pages):
        raise ArtifactPreparationConflictError(
            "durable page coverage differs from dense presentation evidence"
        )

    thumbnail = receipt.presentation.thumbnail
    prepared_thumbnail = work.connector.fetch_all(
        "SELECT candidate_id, publication_key, resource_kind, extent_offset, "
        "extent_length, media_type, image_sha256, width, height "
        "FROM catalog_prepared_thumbnails WHERE candidate_id = %s "
        "AND publication_key = %s LIMIT 2",
        (candidate, publication),
    )
    catalog_thumbnail = work.connector.fetch_all(
        "SELECT revision, publication_key, resource_kind, extent_offset, "
        "extent_length, media_type, image_sha256, width, height "
        "FROM catalog_thumbnails WHERE revision = %s AND publication_key = %s LIMIT 2",
        (revision, publication),
    )
    if thumbnail is None:
        if prepared_thumbnail or catalog_thumbnail:
            raise ArtifactPreparationConflictError(
                "empty presentation has a durable thumbnail"
            )
        return
    thumbnail_kind = CatalogResourceKind.THUMBNAIL.value.encode("ascii")
    image = _image_row_values(thumbnail)
    expected_prepared_thumbnail = (
        candidate,
        publication,
        thumbnail_kind,
        *image,
    )
    expected_catalog_thumbnail = (
        revision,
        publication,
        thumbnail_kind,
        *image,
    )
    _store_exact_row(
        work,
        "catalog_prepared_thumbnails",
        (
            "candidate_id",
            "publication_key",
            "resource_kind",
            "extent_offset",
            "extent_length",
            "media_type",
            "image_sha256",
            "width",
            "height",
        ),
        expected_prepared_thumbnail,
        key_where="candidate_id = %s AND publication_key = %s",
        key_parameters=(candidate, publication),
        conflict_label="prepared thumbnail",
        allow_insert=allow_insert,
    )
    _store_exact_row(
        work,
        "catalog_thumbnails",
        (
            "revision",
            "publication_key",
            "resource_kind",
            "extent_offset",
            "extent_length",
            "media_type",
            "image_sha256",
            "width",
            "height",
        ),
        expected_catalog_thumbnail,
        key_where="revision = %s AND publication_key = %s",
        key_parameters=(revision, publication),
        conflict_label="catalog thumbnail",
        allow_insert=allow_insert,
    )


def _bind_operational_effect_seal(
    work: VNextUnitOfWork,
    *,
    candidate_id: bytes,
    build_id: bytes,
    effect_seal: OperationalEffectSeal,
    now: int,
    allow_insert: bool = True,
) -> None:
    if not isinstance(effect_seal, OperationalEffectSeal):
        raise TypeError("effect_seal must be OperationalEffectSeal")
    preparation_id = require_uuid16(
        effect_seal.preparation_id, field="artifact operational preparation_id"
    )
    row = work.connector.fetch_one(
        "SELECT preparation.build_id, preparation.state, preparation.completed_at, "
        "seal.event_count, seal.final_chain_sha256, seal.sealed_at "
        "FROM operational_operational_preparations preparation "
        "JOIN operational_operational_preparation_effect_seals seal "
        "ON seal.preparation_id = preparation.preparation_id "
        "WHERE preparation.preparation_id = %s",
        (preparation_id,),
    )
    expected = (
        build_id,
        "COMPLETE",
        effect_seal.sealed_at,
        effect_seal.event_count,
        effect_seal.final_chain_sha256,
        effect_seal.sealed_at,
    )
    if row != expected or effect_seal.sealed_at > now:
        raise ArtifactPreparationConflictError(
            "operational effect seal differs from COMPLETE durable authority"
        )
    existing = work.connector.fetch_one(
        "SELECT candidate_id, preparation_id, bound_at "
        "FROM operational_publication_candidate_preparations "
        "WHERE candidate_id = %s OR preparation_id = %s",
        (candidate_id, preparation_id),
    )
    if existing:
        if (
            len(existing) != 3
            or existing[0] != candidate_id
            or existing[1] != preparation_id
            or require_int63(existing[2], field="operational binding timestamp") > now
        ):
            raise ArtifactPreparationConflictError(
                "operational preparation is bound to a different candidate"
            )
        return
    if not allow_insert:
        raise ArtifactPreparationConflictError(
            "replayed prepared artifact lacks its operational binding"
        )
    work.connector.execute(
        "INSERT INTO operational_publication_candidate_preparations "
        "(candidate_id, preparation_id, bound_at) VALUES (%s, %s, %s)",
        (candidate_id, preparation_id, now),
    )


def _load_authority_facts(
    work: VNextUnitOfWork,
    projection: PublicationProjectionAuthority,
    publication_key: bytes,
    *,
    now: int | None,
    contract: _ArtifactContractFacts | None = None,
) -> _AuthorityFacts:
    checkpoint = _load_validation_checkpoint(
        work,
        projection.candidate_id,
        now=now,
    )
    terminal = _load_validation_terminal_receipt(
        work,
        projection.candidate_id,
        checkpoint,
    )
    if contract is None:
        contract = _load_projection_contract(work, projection)
    rows = work.connector.fetch_all(
        "SELECT selection.publication_key, selection.gallery_id, pub.gid, "
        "gallery.gallery_key, gallery.scope_key, member.observation_id, "
        "observation.observation_identity_sha256, "
        "input.artifact_semantics_sha256, operation.operation, "
        "semantics.source_manifest_component_sha256, "
        "semantics.member_plan_component_sha256, "
        "semantics.effective_content_component_sha256, "
        "semantics.selected_component_sha256, "
        "semantics.owner_component_sha256, "
        "semantics.policy_component_sha256 "
        "FROM catalog_publication_selections AS selection "
        "JOIN catalog_publication_identities AS pub "
        "ON pub.publication_key = selection.publication_key "
        "JOIN catalog_gallery_identities AS gallery "
        "ON gallery.gallery_id = selection.gallery_id "
        "JOIN catalog_source_build_galleries AS member "
        "ON member.build_id = %s AND member.gallery_id = selection.gallery_id "
        "JOIN catalog_gallery_observations AS observation "
        "ON observation.gallery_id = member.gallery_id "
        "AND observation.observation_id = member.observation_id "
        "JOIN catalog_candidate_artifact_inputs AS input "
        "ON input.candidate_id = selection.candidate_id "
        "AND input.publication_key = selection.publication_key "
        "JOIN catalog_artifact_semantic_inputs AS semantics "
        "ON semantics.artifact_semantics_sha256 = input.artifact_semantics_sha256 "
        "JOIN catalog_artifact_operations AS operation "
        "ON operation.candidate_id = input.candidate_id "
        "AND operation.publication_key = input.publication_key "
        "WHERE selection.candidate_id = %s AND selection.publication_key = %s LIMIT 2",
        (
            projection.build_id,
            projection.candidate_id,
            publication_key,
        ),
    )
    if len(rows) != 1 or len(rows[0]) != 15:
        raise ArtifactPreparationNotReadyError(
            "artifact input lacks one exact selected semantic operation"
        )
    row = rows[0]
    gallery_scope = require_digest32(row[4], field="artifact gallery scope_key")
    if gallery_scope != contract.scope_key:
        raise ArtifactPreparationConflictError(
            "artifact input gallery belongs to another source scope"
        )
    operation = row[8]
    if operation not in {"CREATE", "REBUILD"}:
        raise ArtifactPreparationNotReadyError(
            "artifact preparation requires CREATE or REBUILD"
        )
    owner = _load_owner_facts(
        work,
        analysis_id=projection.analysis_id,
        gallery_id=require_positive_int63(row[1], field="artifact gallery_id"),
        gid=require_positive_int63(row[2], field="artifact gid"),
    )
    policy_component = require_digest32(row[14], field="policy_component_sha256")
    if policy_component != contract.policy_component_sha256:
        raise ArtifactPreparationConflictError(
            "artifact semantic policy differs from the candidate policy"
        )
    facts = _AuthorityFacts(
        require_digest32(row[0], field="artifact publication_key"),
        require_positive_int63(row[1], field="artifact gallery_id"),
        require_positive_int63(row[2], field="artifact gid"),
        require_digest32(row[3], field="artifact gallery_key"),
        require_positive_int63(row[5], field="artifact observation_id"),
        require_digest32(row[6], field="artifact observation identity"),
        require_digest32(row[7], field="artifact_semantics_sha256"),
        operation,
        require_digest32(row[9], field="source_manifest_component_sha256"),
        require_digest32(row[10], field="member_plan_component_sha256"),
        require_digest32(row[11], field="effective_content_component_sha256"),
        require_digest32(row[12], field="selected_component_sha256"),
        require_digest32(row[13], field="owner_component_sha256"),
        policy_component,
        owner[0],
        owner[1],
        owner[2],
        contract.manifest_algorithm_version,
        contract.file_order_version,
        contract.artifact_algorithm_version,
        contract.adapter_id,
        contract.policy_fingerprint_sha256,
        contract.spam_artist_threshold,
        contract.spam_occurrence_threshold,
        checkpoint,
        terminal,
    )
    _require_derived_fixed_digests(facts)
    return facts


def _load_owner_facts(
    work: VNextUnitOfWork,
    *,
    analysis_id: bytes,
    gallery_id: int,
    gid: int,
) -> tuple[bytes, bytes, bytes]:
    rows = work.connector.fetch_all(
        "SELECT content.content_sha256, owner_gallery.gallery_key, "
        "winner_gallery.gallery_key, owner.owner_gallery_id, winner.winner_gallery_id "
        "FROM catalog_analysis_content_owner_candidate_resolved AS content "
        "JOIN catalog_analysis_content_owner_resolved AS owner "
        "ON owner.analysis_id = content.analysis_id "
        "AND owner.content_sha256 = content.content_sha256 "
        "JOIN catalog_gallery_identities AS owner_gallery "
        "ON owner_gallery.gallery_id = owner.owner_gallery_id "
        "JOIN catalog_analysis_gid_winner_resolved AS winner "
        "ON winner.analysis_id = content.analysis_id AND winner.gid = %s "
        "JOIN catalog_gallery_identities AS winner_gallery "
        "ON winner_gallery.gallery_id = winner.winner_gallery_id "
        "WHERE content.analysis_id = %s AND content.gallery_id = %s LIMIT 2",
        (gid, analysis_id, gallery_id),
    )
    if len(rows) != 1 or len(rows[0]) != 5:
        raise ArtifactPreparationNotReadyError(
            "artifact input has no exact effective-content owner/winner"
        )
    row = rows[0]
    if row[3] != gallery_id or row[4] != gallery_id:
        raise ArtifactPreparationConflictError(
            "artifact input publication is not its exact owner and GID winner"
        )
    return (
        require_digest32(row[0], field="artifact content_sha256"),
        require_digest32(row[1], field="artifact owner_gallery_key"),
        require_digest32(row[2], field="artifact winner_gallery_key"),
    )


def _require_derived_fixed_digests(facts: _AuthorityFacts) -> None:
    expected = (
        identity.artifact_source_manifest_digest(
            facts.observation_identity_sha256,
            facts.manifest_algorithm_version,
            facts.file_order_version,
        ),
        identity.artifact_selected_digest(facts.publication_key, facts.gallery_key),
        identity.artifact_owner_digest(
            facts.content_sha256,
            facts.owner_gallery_key,
            facts.gid,
            facts.winner_gallery_key,
        ),
        identity.artifact_policy_digest(
            facts.artifact_algorithm_version,
            facts.adapter_id,
            facts.policy_fingerprint_sha256,
        ),
    )
    actual = (
        facts.source_manifest_component_sha256,
        facts.selected_component_sha256,
        facts.owner_component_sha256,
        facts.policy_component_sha256,
    )
    if actual != expected:
        raise ArtifactPreparationConflictError(
            "artifact fixed semantic component digest differs from source facts"
        )
    semantic = identity.artifact_semantics_digest(
        facts.source_manifest_component_sha256,
        facts.member_plan_component_sha256,
        facts.effective_content_component_sha256,
        facts.selected_component_sha256,
        facts.owner_component_sha256,
        facts.policy_component_sha256,
    )
    if semantic != facts.artifact_semantics_sha256:
        raise ArtifactPreparationConflictError(
            "artifact six-component semantic identity is inconsistent"
        )


def _validate_fixed_components(
    work: VNextUnitOfWork,
    authority: ArtifactPreparationAuthority,
) -> None:
    components = (
        (
            authority.source_manifest_component_sha256,
            b"artifact_source_manifest_v1",
            identity.encode_artifact_source_manifest(
                authority.observation_identity_sha256,
                authority.manifest_algorithm_version,
                authority.file_order_version,
            ),
        ),
        (
            authority.selected_component_sha256,
            b"artifact_selected_v1",
            identity.encode_artifact_selected(
                authority.publication_key,
                authority.gallery_key,
            ),
        ),
        (
            authority.owner_component_sha256,
            b"artifact_owner_v1",
            identity.encode_artifact_owner(
                authority.content_sha256,
                authority.owner_gallery_key,
                authority.gid,
                authority.winner_gallery_key,
            ),
        ),
        (
            authority.policy_component_sha256,
            b"artifact_policy_v3",
            identity.encode_artifact_policy(
                authority.artifact_algorithm_version,
                authority.adapter_id,
                authority.policy_fingerprint_sha256,
            ),
        ),
        (
            authority.artifact_semantics_sha256,
            b"artifact_semantics_v1",
            identity.encode_artifact_semantics(
                authority.source_manifest_component_sha256,
                authority.member_plan_component_sha256,
                authority.effective_content_component_sha256,
                authority.selected_component_sha256,
                authority.owner_component_sha256,
                authority.policy_component_sha256,
            ),
        ),
    )
    for digest, domain, payload in components:
        _validate_exact_canonical_payload(
            work,
            value_sha256=digest,
            expected_domain=domain,
            expected_payload=payload,
        )


def _validate_exact_canonical_payload(
    work: VNextUnitOfWork,
    *,
    value_sha256: bytes,
    expected_domain: bytes,
    expected_payload: bytes,
) -> None:
    offset = 0

    def compare(part: bytes) -> None:
        nonlocal offset
        if expected_payload[offset : offset + len(part)] != part:
            raise ArtifactPreparationConflictError(
                "canonical artifact component bytes differ from source facts"
            )
        offset += len(part)

    try:
        receipt = CanonicalValueRepository.stream_and_validate(
            work,
            value_sha256=value_sha256,
            consume_provisional=compare,
        )
    except (CanonicalValueCollisionError, CanonicalValueNotReadyError) as error:
        raise ArtifactPreparationConflictError(
            "canonical artifact component is incomplete or corrupt"
        ) from error
    if receipt.digest_domain != expected_domain or offset != len(expected_payload):
        raise ArtifactPreparationConflictError(
            "canonical artifact component domain or exact EOF differs"
        )


def _audit_member_and_effective_components(
    work: VNextUnitOfWork,
    authority: ArtifactPreparationAuthority,
) -> ArtifactPreparationInputAudit:
    member_payload = bytearray()

    def append_member_part(part: bytes) -> None:
        if len(member_payload) + len(part) > _MAX_MEMBER_PLAN_BYTES:
            raise ArtifactPreparationNotReadyError(
                "artifact member plan exceeds the core byte bound"
            )
        member_payload.extend(part)

    try:
        member_receipt = CanonicalValueRepository.stream_and_validate(
            work,
            value_sha256=authority.member_plan_component_sha256,
            consume_provisional=append_member_part,
        )
        if member_receipt.digest_domain != b"artifact_member_plan_v2":
            raise ArtifactPreparationConflictError(
                "member plan has the wrong canonical domain"
            )
        plan_entries = identity.decode_artifact_member_plan(bytes(member_payload))
        expected_entries, source_count, source_bytes = _expected_render_entries(
            work,
            authority,
        )
        if plan_entries != expected_entries:
            raise ArtifactPreparationConflictError(
                "member plan differs from sealed adapter-issued source roles"
            )
        page_digests = tuple(
            entry.source_file_sha256
            for entry in plan_entries
            if entry.source_role is identity.ArtifactMemberSourceRole.PAGE
        )
        expected_digest = identity.artifact_effective_content_digest(page_digests)
        if expected_digest != authority.effective_content_component_sha256:
            raise ArtifactPreparationConflictError(
                "effective-content digest differs from member/source facts"
            )
        _validate_exact_canonical_payload(
            work,
            value_sha256=authority.effective_content_component_sha256,
            expected_domain=b"artifact_effective_content_v1",
            expected_payload=identity.encode_artifact_effective_content(page_digests),
        )
        source_root, gallery_locator = _load_source_locator_components(
            work,
            authority,
        )
        references = tuple(
            ArtifactSourceReference(
                entry.source_position,
                (
                    ArtifactSourceRole.METADATA
                    if entry.source_role is identity.ArtifactMemberSourceRole.METADATA
                    else ArtifactSourceRole.PAGE
                ),
                entry.source_name_bytes,
                entry.source_file_sha256,
                entry.source_size_bytes,
            )
            for entry in plan_entries
        )
        return ArtifactPreparationInputAudit(
            authority,
            source_count,
            len(plan_entries),
            source_bytes,
            len(page_digests),
            source_root,
            gallery_locator,
            references,
            _AUDIT_TOKEN,
        )
    except (identity.ByteDomainError, ValueError) as error:
        raise ArtifactPreparationConflictError(
            "artifact canonical member/effective input is malformed"
        ) from error
    except (CanonicalValueCollisionError, CanonicalValueNotReadyError) as error:
        raise ArtifactPreparationConflictError(
            "artifact canonical member/effective input is incomplete or corrupt"
        ) from error


def _expected_render_entries(
    work: VNextUnitOfWork,
    authority: ArtifactPreparationAuthority,
) -> tuple[tuple[identity.ArtifactMemberPlanEntry, ...], int, int]:
    entries: list[identity.ArtifactMemberPlanEntry] = []
    source_count = 0
    source_bytes = 0
    metadata_count = 0
    after = -1
    while True:
        rows = _source_file_page(work, authority, after_file_no=after)
        if not rows:
            break
        for row in rows:
            file_no = require_int63(row[0], field="artifact source file_no")
            if file_no != source_count:
                raise ArtifactPreparationConflictError(
                    "artifact source positions are not zero-based contiguous"
                )
            name = require_bounded_bytes(
                row[1],
                field="artifact source name",
                minimum=1,
                maximum=255,
            )
            role = require_bounded_bytes(
                row[2],
                field="adapter-issued artifact source role",
                minimum=4,
                maximum=8,
            )
            digest = require_digest32(row[3], field="artifact source file_sha256")
            size = require_int63(row[4], field="artifact source size")
            source_bytes += size
            if source_bytes > INT63_MAX:
                raise ArtifactPreparationNotReadyError(
                    "artifact source byte count exceeds int63"
                )
            if role == b"metadata":
                if any(value is not None for value in row[5:8]):
                    raise ArtifactPreparationConflictError(
                        "metadata unexpectedly participates in spam decisions"
                    )
                metadata_count += 1
                member_role = identity.ArtifactMemberSourceRole.METADATA
            elif role == b"page":
                if _excluded_from_scalars(
                    row[5:8],
                    spam_artist_threshold=authority.spam_artist_threshold,
                    spam_occurrence_threshold=authority.spam_occurrence_threshold,
                ):
                    after = file_no
                    source_count += 1
                    continue
                member_role = identity.ArtifactMemberSourceRole.PAGE
            elif role == b"other":
                after = file_no
                source_count += 1
                continue
            else:
                raise ArtifactPreparationConflictError(
                    "adapter-issued artifact source role is not registered"
                )
            entries.append(
                identity.ArtifactMemberPlanEntry(
                    file_no,
                    name,
                    digest,
                    size,
                    member_role,
                )
            )
            after = file_no
            source_count += 1
        if len(rows) < _MAX_SOURCE_PAGE:
            break
    if metadata_count != 1:
        raise ArtifactPreparationConflictError(
            "artifact render input requires exactly one METADATA member"
        )
    if (
        sum(
            entry.source_role is identity.ArtifactMemberSourceRole.PAGE
            for entry in entries
        )
        > 4096
    ):
        raise ArtifactPreparationNotReadyError(
            "artifact render input exceeds 4096 PAGE members"
        )
    return tuple(entries), source_count, source_bytes


def _source_file_page(
    work: VNextUnitOfWork,
    authority: ArtifactPreparationAuthority,
    *,
    after_file_no: int,
) -> tuple[tuple[Any, ...], ...]:
    rows = work.connector.fetch_all(
        "SELECT source.file_no, name.name_bytes, source.artifact_role, "
        "source.file_sha256, content_blob.size_bytes, decision.occurrence_count, "
        "decision.artist_count, decision.maximum_gallery_artist_count "
        + _SOURCE_FILE_FAMILY_SQL
        + "JOIN catalog_content_blobs AS content_blob "
        "ON content_blob.file_sha256 = source.file_sha256 "
        "LEFT JOIN catalog_analysis_file_hash_decision_resolved AS decision "
        "ON decision.analysis_id = %s AND decision.file_sha256 = source.file_sha256 "
        "WHERE source.gallery_id = %s AND source.observation_id = %s "
        "AND source.file_no > %s ORDER BY source.file_no LIMIT 128",
        (
            authority.analysis_id,
            authority.gallery_id,
            authority.observation_id,
            after_file_no,
        ),
    )
    if len(rows) > _MAX_SOURCE_PAGE:
        raise ArtifactPreparationConflictError("source file page exceeded hard cap")
    return tuple(rows)


def _load_validation_checkpoint(
    work: VNextUnitOfWork,
    candidate_id: bytes,
    *,
    now: int | None,
) -> tuple[int, bytes, int, str, int]:
    predecessor = work.connector.fetch_one(
        f"SELECT state FROM {_CHECKPOINT_TABLE} WHERE candidate_id = %s AND stage = %s",
        (candidate_id, b"BUILD_ARTIFACT_DELTA_OPERATION"),
    )
    if predecessor != ("COMPLETE",):
        raise ArtifactPreparationNotReadyError(
            "artifact delta build checkpoint is incomplete"
        )
    checkpoint = _checkpoint_from_row(
        _load_checkpoint_row(
            work,
            candidate_id,
            b"VALIDATE_ARTIFACT_INPUT_DELTA",
        )
    )
    if checkpoint[3] != "COMPLETE":
        raise ArtifactPreparationNotReadyError(
            "artifact input/delta validation checkpoint is incomplete"
        )
    if now is not None and checkpoint[4] > now:
        raise ArtifactPreparationNotReadyError(
            "artifact validation checkpoint is from the future"
        )
    return checkpoint


def _load_complete_stage_receipt(
    work: VNextUnitOfWork,
    candidate_id: bytes,
    stage: bytes,
    *,
    cursor_maximum: int,
) -> tuple[tuple[int, bytes, int, str, int], tuple[Any, ...]]:
    row = _load_checkpoint_row(work, candidate_id, stage)
    if len(row) != 5:
        raise ArtifactPreparationNotReadyError(
            f"publication stage {stage!r} checkpoint is missing"
        )
    checkpoint = (
        require_positive_int63(row[0], field="artifact stage generation"),
        require_bounded_bytes(
            row[1], field="artifact stage cursor", maximum=cursor_maximum
        ),
        require_int63(row[2], field="artifact stage processed_count"),
        row[3],
        require_int63(row[4], field="artifact stage updated_at"),
    )
    _validate_checkpoint(checkpoint, cursor_maximum=cursor_maximum)
    if checkpoint[3] != "COMPLETE":
        raise ArtifactPreparationNotReadyError(
            f"publication stage {stage!r} is not COMPLETE"
        )
    receipt = _load_terminal_batch_receipt(
        work,
        candidate_id,
        stage,
        checkpoint[0],
    )
    if receipt is None:
        raise ArtifactPreparationNotReadyError(
            f"publication stage {stage!r} lacks one terminal receipt"
        )
    _validate_terminal_receipt(
        receipt,
        checkpoint=checkpoint,
        cursor_maximum=cursor_maximum,
    )
    return checkpoint, receipt


def _load_validation_terminal_receipt(
    work: VNextUnitOfWork,
    candidate_id: bytes,
    checkpoint: tuple[int, bytes, int, str, int],
) -> tuple[Any, ...]:
    receipt = _load_terminal_batch_receipt(
        work,
        candidate_id,
        b"VALIDATE_ARTIFACT_INPUT_DELTA",
        checkpoint[0],
    )
    if receipt is None:
        raise ArtifactPreparationNotReadyError(
            "artifact validation lacks one exact terminal receipt"
        )
    _validate_terminal_receipt(receipt, checkpoint=checkpoint)
    return receipt


def _load_checkpoint_row(
    work: VNextUnitOfWork,
    candidate_id: bytes,
    stage: bytes,
) -> tuple[Any, ...]:
    return work.connector.fetch_one(
        "SELECT generation, `cursor`, processed_count, state, updated_at "
        f"FROM {_CHECKPOINT_TABLE} WHERE candidate_id = %s AND stage = %s",
        (candidate_id, stage),
    )


def _load_terminal_batch_receipt(
    work: VNextUnitOfWork,
    candidate_id: bytes,
    stage: bytes,
    committed_generation: int,
) -> tuple[Any, ...] | None:
    generation = require_positive_int63(
        committed_generation,
        field="artifact terminal receipt committed_generation",
    )
    if generation <= 1:
        return None
    try:
        stored = _load_candidate_batch_at_generation(
            work,
            candidate_id,
            stage,
            generation - 1,
        )
        if stored is None:
            return None
        batch_key = require_bounded_bytes(
            stored[0],
            field="artifact terminal receipt batch_key",
            minimum=1,
            maximum=512,
        )
        batch = _candidate_batch_from_row(
            candidate_id,
            stage,
            batch_key,
            stored[1:],
            replayed=True,
        )
    except (PublicationCandidateConflictError, TypeError, ValueError) as error:
        raise ArtifactPreparationConflictError(
            "artifact terminal receipt vertical family is malformed"
        ) from error
    return (
        batch.batch_key,
        batch.start_generation,
        batch.start_cursor,
        batch.start_processed_count,
        batch.next_cursor,
        batch.next_processed_count,
        batch.next_state,
        batch.row_count,
        int(batch.terminal),
        batch.committed_generation,
        batch.committed_at,
    )


def _checkpoint_from_row(row: tuple[Any, ...]) -> tuple[int, bytes, int, str, int]:
    if len(row) != 5:
        raise ArtifactPreparationNotReadyError(
            "artifact validation checkpoint is missing"
        )
    checkpoint = (
        require_positive_int63(row[0], field="artifact checkpoint generation"),
        require_bounded_bytes(
            row[1],
            field="artifact checkpoint cursor",
            maximum=2048,
        ),
        require_int63(row[2], field="artifact checkpoint processed_count"),
        row[3],
        require_int63(row[4], field="artifact checkpoint updated_at"),
    )
    _validate_checkpoint(checkpoint)
    return checkpoint


def _validate_checkpoint(
    value: tuple[int, bytes, int, str, int],
    *,
    cursor_maximum: int = 32,
) -> None:
    if len(value) != 5:
        raise ValueError("artifact validation checkpoint has the wrong shape")
    require_positive_int63(value[0], field="artifact checkpoint generation")
    cursor = require_bounded_bytes(
        value[1],
        field="artifact checkpoint cursor",
        maximum=cursor_maximum,
    )
    if cursor_maximum == 32 and cursor and len(cursor) != 32:
        raise ValueError("artifact validation cursor must be raw32 or empty")
    require_int63(value[2], field="artifact checkpoint processed_count")
    if value[3] not in {"OPEN", "COMPLETE"}:
        raise ValueError("artifact validation checkpoint state is not registered")
    require_int63(value[4], field="artifact checkpoint updated_at")


def _validate_terminal_receipt(
    receipt: tuple[Any, ...],
    *,
    checkpoint: tuple[int, bytes, int, str, int],
    cursor_maximum: int = 32,
) -> None:
    if len(receipt) != 11:
        raise ValueError("artifact terminal receipt has the wrong shape")
    require_bounded_bytes(
        receipt[0],
        field="artifact terminal batch_key",
        minimum=1,
        maximum=512,
    )
    start_generation = require_positive_int63(
        receipt[1],
        field="artifact terminal start_generation",
    )
    start_cursor = require_bounded_bytes(
        receipt[2],
        field="artifact terminal start_cursor",
        maximum=cursor_maximum,
    )
    start_count = require_int63(
        receipt[3],
        field="artifact terminal start_processed_count",
    )
    if (
        start_generation + 1 != checkpoint[0]
        or receipt[4] != start_cursor
        or receipt[5] != start_count
        or receipt[6] != "COMPLETE"
        or receipt[7] != 0
        or receipt[8] != 1
        or receipt[9] != checkpoint[0]
        or receipt[10] != checkpoint[4]
        or start_cursor != checkpoint[1]
        or start_count != checkpoint[2]
        or checkpoint[3] != "COMPLETE"
    ):
        raise ValueError("artifact terminal receipt differs from its checkpoint")


def _facts_from_authority(authority: ArtifactPreparationAuthority) -> _AuthorityFacts:
    return _AuthorityFacts(
        authority.publication_key,
        authority.gallery_id,
        authority.gid,
        authority.gallery_key,
        authority.observation_id,
        authority.observation_identity_sha256,
        authority.artifact_semantics_sha256,
        authority.operation,
        authority.source_manifest_component_sha256,
        authority.member_plan_component_sha256,
        authority.effective_content_component_sha256,
        authority.selected_component_sha256,
        authority.owner_component_sha256,
        authority.policy_component_sha256,
        authority.content_sha256,
        authority.owner_gallery_key,
        authority.winner_gallery_key,
        authority.manifest_algorithm_version,
        authority.file_order_version,
        authority.artifact_algorithm_version,
        authority.adapter_id,
        authority.policy_fingerprint_sha256,
        authority.spam_artist_threshold,
        authority.spam_occurrence_threshold,
        authority.validation_checkpoint,
        authority.validation_terminal_receipt,
    )


def _require_authority(authority: ArtifactPreparationAuthority) -> None:
    if not isinstance(authority, ArtifactPreparationAuthority):
        raise TypeError("authority must be ArtifactPreparationAuthority")
    if authority._capability is not _AUTHORITY_TOKEN:
        raise TypeError("artifact preparation authority is not repository-issued")
    authority.__post_init__()
    authority.projection.__post_init__()


def _write_all(stream: BinaryIO, part: bytes) -> None:
    if stream.write(part) != len(part):
        raise OSError("artifact spool accepted a partial write")
