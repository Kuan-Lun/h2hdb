"""Bounded artifact projection, byte preparation, and persistence for vNext.

All caller-visible commands carry only durable roots and repository-issued
capabilities.  Component digests, input IDs, cursors, operation kinds, archive
names, byte digests, locators, protection tokens, and projection counts are
derived and independently checked by this module.
"""

from __future__ import annotations

__all__ = [
    "ArtifactProducerRegistration",
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
import stat
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory, TemporaryFile
from typing import Any, BinaryIO, Protocol, cast

from . import vnext_identity as identity
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
    cas_prepared_artifact_state,
    ensure_artifact_semantic_input_family,
    ensure_catalog_artifact_family,
    ensure_prepared_artifact_family,
    load_artifact_semantic_input_family,
    load_catalog_artifact_family,
    load_prepared_artifact_family,
)
from .vnext_canonical_value_family import (
    load_sealed_value_identities,
    load_sealed_value_identity,
)
from .vnext_canonical_value_repository import (
    CanonicalValueCollisionError,
    CanonicalValueNotReadyError,
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from .vnext_domains import (
    INT63_MAX,
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uint32,
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
_MEMBER_PLAN_PREFIX = b"h2hdb-vnext-artifact-member-plan\0"
_MAX_READ_BYTES = 64 * 1024
_MAX_SOURCE_PAGE = 128
_ZIP_MEMBER_NAME_MAXIMUM_BYTES = (1 << 16) - 1

_ZIP_WRITER_POLICY_V1 = (1, 8, 9, 33, 0, 33188, 2048, 3, 1, 1)
_STORAGE_CODEC_V1 = (1, b"managed-filesystem", 1, 1)
_PREPARATION_RECEIPT_TOKEN = object()
_PROTECTION_INTENT_TOKEN = object()
_PROTECTION_EVIDENCE_TOKEN = object()
_INPUT_AUTHORITY_TOKEN = object()
_INPUT_PLAN_TOKEN = object()
_INPUT_VALIDATION_PLAN_TOKEN = object()

_CHECKPOINT_GENERATION_TABLE = "catalog_publication_checkpoint_generations"
_CHECKPOINT_CURSOR_TABLE = "catalog_publication_checkpoint_cursors"
_CHECKPOINT_COUNT_TABLE = "catalog_publication_checkpoint_processed_counts"
_CHECKPOINT_STATE_TABLE = "catalog_publication_checkpoint_states"
_CHECKPOINT_UPDATED_AT_TABLE = "catalog_publication_checkpoint_updated_ats"
_CHECKPOINT_SEAL_TABLE = "catalog_publication_checkpoint_seals"

_SOURCE_FILE_FAMILY_SQL = (
    "FROM (SELECT file_no.gallery_id, file_no.observation_id, file_no.file_key, "
    "file_no.file_no, file_sha.file_sha256 "
    "FROM catalog_gallery_observation_file_seals AS file_seal "
    "JOIN catalog_gallery_observation_file_file_nos AS file_no "
    "ON file_no.gallery_id = file_seal.gallery_id "
    "AND file_no.observation_id = file_seal.observation_id "
    "AND file_no.file_key = file_seal.file_key "
    "JOIN catalog_gallery_observation_file_file_sha256s AS file_sha "
    "ON file_sha.gallery_id = file_seal.gallery_id "
    "AND file_sha.observation_id = file_seal.observation_id "
    "AND file_sha.file_key = file_seal.file_key) AS source "
    "JOIN (SELECT name_seal.file_key, name_bytes.name_bytes, role.file_role "
    "FROM catalog_file_name_identity_seals AS name_seal "
    "JOIN catalog_file_name_identity_name_bytes AS name_bytes "
    "ON name_bytes.file_key = name_seal.file_key "
    "JOIN catalog_file_name_identity_file_roles AS role "
    "ON role.file_key = name_seal.file_key) AS name "
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


@dataclass(frozen=True, slots=True)
class ArtifactProducerRegistration:
    """One exact sealed artifact byte-producer identity."""

    producer_fingerprint_sha256: bytes
    artifact_algorithm_version: int
    producer_equivalence_class: bytes
    writer_id: bytes
    python_abi: bytes
    pillow_build: bytes
    libjpeg_build: bytes
    zlib_build: bytes
    replayed: bool

    def __post_init__(self) -> None:
        fingerprint = require_digest32(
            self.producer_fingerprint_sha256,
            field="artifact producer fingerprint",
        )
        algorithm_version = require_uint32(
            self.artifact_algorithm_version,
            field="artifact producer algorithm version",
        )
        if algorithm_version == 0:
            raise ValueError("artifact producer algorithm version must be positive")
        equivalence_class = require_bounded_bytes(
            self.producer_equivalence_class,
            field="artifact producer equivalence class",
            minimum=1,
            maximum=128,
        )
        fields = tuple(
            require_bounded_bytes(
                value,
                field=f"artifact producer {field}",
                minimum=1,
                maximum=128,
            )
            for field, value in (
                ("writer_id", self.writer_id),
                ("python_abi", self.python_abi),
                ("pillow_build", self.pillow_build),
                ("libjpeg_build", self.libjpeg_build),
                ("zlib_build", self.zlib_build),
            )
        )
        if identity.artifact_producer_fingerprint_sha256(*fields) != fingerprint:
            raise ValueError("artifact producer fingerprint does not match its frame")
        if equivalence_class != identity.artifact_producer_equivalence_class(
            fingerprint
        ):
            raise ValueError(
                "artifact producer equivalence class is not repository-derived"
            )
        if not isinstance(self.replayed, bool):
            raise TypeError("artifact producer replayed must be bool")


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
    max_image_short_side: int
    producer_fingerprint_sha256: bytes
    producer_fields: tuple[bytes, bytes, bytes, bytes, bytes]
    writer_policy: tuple[int, int, int, int, int, int, int, int, int, int]
    storage_codec: tuple[int, bytes, int, int]
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
            ("max_image_short_side", self.max_image_short_side),
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
        require_digest32(
            self.producer_fingerprint_sha256,
            field="artifact producer_fingerprint_sha256",
        )
        if len(self.producer_fields) != 5:
            raise ValueError("artifact authority requires five producer fields")
        for index, value in enumerate(self.producer_fields):
            require_bounded_bytes(
                value,
                field=f"artifact producer field {index}",
                minimum=1,
                maximum=128,
            )
        if self.writer_policy != _ZIP_WRITER_POLICY_V1:
            raise ValueError("artifact authority has an unsupported ZIP policy")
        if self.storage_codec != _STORAGE_CODEC_V1:
            raise ValueError("artifact authority has an unsupported storage codec")
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
    def artifact_name(self) -> bytes:
        return identity.artifact_name(self.gid)

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
    adapter ID and producer fingerprint before it consumes this receipt.
    """

    authority: ArtifactPreparationAuthority
    source_entry_count: int
    emitted_member_count: int
    source_byte_count: int
    effective_content_file_count: int
    zip_comment: bytes
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
        require_bounded_bytes(
            self.zip_comment,
            field="artifact audit zip_comment",
            minimum=1,
            maximum=512,
        )


@dataclass(frozen=True, slots=True)
class ArtifactStorageEvidence:
    """Untrusted adapter acknowledgement wrapped by the repository."""

    stored: bool

    def __post_init__(self) -> None:
        if type(self.stored) is not bool:
            raise TypeError("artifact storage acknowledgement must be bool")


@dataclass(frozen=True, slots=True)
class ArtifactProtectionIntent:
    """Opaque durable PENDING intent issued only after its family is sealed."""

    candidate_id: bytes
    publication_key: bytes
    artifact_sha256: bytes
    size_bytes: int
    locator_components: tuple[str, ...]
    artifact_locator_sha256: bytes
    storage_codec_version: int
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
        artifact = require_digest32(
            self.artifact_sha256,
            field="intent artifact_sha256",
        )
        size = require_int63(self.size_bytes, field="intent size_bytes")
        if tuple(self.locator_components) != identity.artifact_locator_components(
            artifact
        ):
            raise ValueError("artifact intent locator is not content-addressed")
        locator = require_digest32(
            self.artifact_locator_sha256,
            field="intent artifact_locator_sha256",
        )
        if identity.artifact_locator_digest(self.locator_components) != locator:
            raise ValueError("artifact intent locator digest disagrees")
        codec = require_positive_int63(
            self.storage_codec_version,
            field="intent storage_codec_version",
        )
        generation = require_int63(
            self.storage_generation,
            field="intent storage_generation",
        )
        token_bytes = require_bounded_bytes(
            self.protection_token,
            field="intent protection_token",
            minimum=184,
            maximum=184,
        )
        token = identity.decode_artifact_protection_token(token_bytes)
        if (
            token.candidate_id != candidate
            or token.publication_key != publication
            or token.artifact_sha256 != artifact
            or token.artifact_locator_sha256 != locator
            or token.storage_codec_version != codec
            or token.storage_generation != generation
            or token.size_bytes != size
        ):
            raise ValueError("artifact intent token disagrees with durable facts")
        if self.state not in {"PENDING", "PREPARED", "COMMITTED"}:
            raise ValueError("artifact protection intent state is not registered")
        if type(self.replayed) is not bool:
            raise TypeError("artifact protection intent replayed must be bool")


@dataclass(frozen=True, slots=True)
class ArtifactProtectionEvidence:
    """Repository-issued exact acknowledgement of one durable intent."""

    intent: ArtifactProtectionIntent
    adapter_id: bytes
    producer_fingerprint_sha256: bytes
    _capability: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._capability is not _PROTECTION_EVIDENCE_TOKEN:
            raise TypeError("artifact protection evidence is repository-issued")
        if not isinstance(self.intent, ArtifactProtectionIntent):
            raise TypeError("artifact protection evidence lacks its durable intent")
        require_bounded_bytes(
            self.adapter_id,
            field="protection evidence adapter_id",
            minimum=1,
            maximum=64,
        )
        require_digest32(
            self.producer_fingerprint_sha256,
            field="protection evidence producer fingerprint",
        )


class ArtifactPreparationReceipt:
    """Opaque verified archive plus its bounded canonical locator plan.

    No external storage protection has happened when this value is returned.
    The owned archive remains open until the receipt is closed so protection
    can occur only after the database has durably sealed a PENDING intent.
    """

    __slots__ = (
        "audit",
        "_archive",
        "_artifact_locator_sha256",
        "_artifact_sha256",
        "_capability",
        "_closed",
        "_locator_components",
        "_locator_plan",
        "_size_bytes",
        "_storage_codec_version",
    )

    def __init__(
        self,
        *,
        audit: ArtifactPreparationInputAudit,
        artifact_sha256: bytes,
        size_bytes: int,
        locator_components: tuple[str, ...],
        artifact_locator_sha256: bytes,
        storage_codec_version: int,
        locator_plan: CanonicalValueUploadPlan,
        archive: BinaryIO,
        _capability: object,
    ) -> None:
        if _capability is not _PREPARATION_RECEIPT_TOKEN:
            raise TypeError("artifact preparation receipts are repository-issued")
        if audit._capability is not _AUDIT_TOKEN:
            raise TypeError("artifact preparation receipt lacks an input audit")
        self.audit = audit
        self._artifact_sha256 = require_digest32(
            artifact_sha256, field="prepared artifact_sha256"
        )
        self._size_bytes = require_int63(size_bytes, field="prepared size_bytes")
        if tuple(locator_components) != identity.artifact_locator_components(
            self._artifact_sha256
        ):
            raise ValueError("prepared locator is not content-addressed")
        self._locator_components = tuple(locator_components)
        self._artifact_locator_sha256 = require_digest32(
            artifact_locator_sha256, field="prepared artifact_locator_sha256"
        )
        if identity.artifact_locator_digest(self._locator_components) != (
            self._artifact_locator_sha256
        ):
            raise ValueError("prepared locator digest disagrees")
        self._storage_codec_version = require_positive_int63(
            storage_codec_version, field="prepared storage_codec_version"
        )
        if locator_plan.value_sha256 != self._artifact_locator_sha256:
            raise ValueError("locator upload plan disagrees with receipt")
        if not hasattr(archive, "read") or not hasattr(archive, "seek"):
            raise TypeError("prepared archive must be one seekable binary stream")
        self._locator_plan = locator_plan
        self._archive = archive
        self._capability = _capability
        self._closed = False

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                self._locator_plan.close()
            finally:
                self._archive.close()

    def __enter__(self) -> ArtifactPreparationReceipt:
        if self._closed:
            raise ValueError("artifact preparation receipt is closed")
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    @property
    def artifact_sha256(self) -> bytes:
        return self._artifact_sha256

    @property
    def size_bytes(self) -> int:
        return self._size_bytes

    @property
    def locator_components(self) -> tuple[str, ...]:
        return self._locator_components

    @property
    def artifact_locator_sha256(self) -> bytes:
        return self._artifact_locator_sha256

    @property
    def storage_codec_version(self) -> int:
        return self._storage_codec_version

    @property
    def locator_plan(self) -> CanonicalValueUploadPlan:
        return self._locator_plan


@dataclass(frozen=True, slots=True)
class ArtifactPersistenceReceipt:
    candidate_id: bytes
    publication_key: bytes
    artifact_sha256: bytes
    artifact_locator_sha256: bytes
    protection_token: bytes
    state: str
    replayed: bool

    def __post_init__(self) -> None:
        require_uuid16(self.candidate_id, field="persisted artifact candidate_id")
        require_digest32(self.publication_key, field="persisted publication_key")
        require_digest32(self.artifact_sha256, field="persisted artifact_sha256")
        require_digest32(
            self.artifact_locator_sha256,
            field="persisted artifact_locator_sha256",
        )
        require_bounded_bytes(
            self.protection_token,
            field="persisted protection_token",
            minimum=184,
            maximum=184,
        )
        if self.state not in {"PREPARED", "COMMITTED"}:
            raise ValueError("persisted artifact state is not registered")
        if type(self.replayed) is not bool:
            raise TypeError("persisted artifact replayed must be bool")


class ArtifactStorageAdapter(Protocol):
    """Internal producer/storage boundary selected by closed registries.

    Protection tokens have a monotone external lifecycle.  ``protect`` is
    idempotent while protected, and a release acknowledgement is terminal for
    that token: a delayed or retried ``protect`` must never resurrect bytes
    after release.  Implementations must retain that terminal tombstone (or an
    equivalent monotone ordering record) for the token's full retry horizon.
    """

    adapter_id: bytes
    producer_fingerprint_sha256: bytes

    def render_member(
        self,
        source: BinaryIO,
        transform_kind: identity.ArtifactTransformKind,
        destination: BinaryIO,
    ) -> None: ...

    def protect(
        self,
        archive: BinaryIO,
        locator_components: tuple[str, ...],
        protection_token: bytes,
    ) -> ArtifactStorageEvidence: ...


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


class _ArchiveSourcePlan:
    def __init__(
        self,
        *,
        database: sqlite3.Connection,
        temporary_directory: TemporaryDirectory[str],
        source_directory: Path,
        member_count: int,
    ) -> None:
        self.database = database
        self.temporary_directory = temporary_directory
        self.source_directory = source_directory
        self.member_count = member_count
        self.closed = False

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self.database.close()
            self.temporary_directory.cleanup()


class _CanonicalZipInfo(zipfile.ZipInfo):
    """Force the registered UTF-8 flag even for ASCII-only canonical names."""

    def _encodeFilenameFlags(self) -> tuple[bytes, int]:  # noqa: N802
        return self.filename.encode("ascii", errors="strict"), self.flag_bits | 2048


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
    max_image_short_side: int
    producer_fingerprint_sha256: bytes
    producer_fields: tuple[bytes, bytes, bytes, bytes, bytes]
    writer_policy: tuple[int, int, int, int, int, int, int, int, int, int]
    storage_codec: tuple[int, bytes, int, int]
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
    max_image_short_side: int
    producer_fingerprint_sha256: bytes
    producer_fields: tuple[bytes, bytes, bytes, bytes, bytes]
    writer_policy: tuple[int, int, int, int, int, int, int, int, int, int]
    storage_codec: tuple[int, bytes, int, int]


class ArtifactPreparationRepository:
    """Issue exact input authority and audit the canonical member projection."""

    @staticmethod
    def register_producer(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        now: int,
        artifact_algorithm_version: int,
        writer_id: bytes,
        python_abi: bytes,
        pillow_build: bytes,
        libjpeg_build: bytes,
        zlib_build: bytes,
    ) -> ArtifactProducerRegistration:
        """Derive, collision-check, and seal one byte-producer registration.

        The caller owns the write transaction.  The digest is never accepted
        from the caller: it is derived from the exact five-field producer
        frame, checked through both candidate keys, and made visible through
        the sealed read view only by the final seal insert.
        """

        algorithm_version = require_uint32(
            artifact_algorithm_version,
            field="artifact producer algorithm version",
        )
        if algorithm_version == 0:
            raise ValueError("artifact producer algorithm version must be positive")
        producer_fields = cast(
            tuple[bytes, bytes, bytes, bytes, bytes],
            tuple(
                require_bounded_bytes(
                    value,
                    field=f"artifact producer {field}",
                    minimum=1,
                    maximum=128,
                )
                for field, value in (
                    ("writer_id", writer_id),
                    ("python_abi", python_abi),
                    ("pillow_build", pillow_build),
                    ("libjpeg_build", libjpeg_build),
                    ("zlib_build", zlib_build),
                )
            ),
        )
        fingerprint = identity.artifact_producer_fingerprint_sha256(*producer_fields)
        equivalence_class = identity.artifact_producer_equivalence_class(fingerprint)
        _authorize_artifact_mutation(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            now=now,
        )
        if work.connector.fetch_one(
            "SELECT artifact_algorithm_version "
            "FROM catalog_artifact_zip_writer_policy_seals "
            "WHERE artifact_algorithm_version = %s",
            (algorithm_version,),
        ) != (algorithm_version,):
            raise ArtifactPreparationNotReadyError(
                "artifact producer algorithm version is not registered"
            )

        # The maintenance gate and ingest fence above are the mutable
        # serialization authorities.  Immutable registry rows are plain reads.
        digest_row = work.connector.fetch_one(
            "SELECT anchor.producer_fingerprint_sha256, "
            "algorithm.artifact_algorithm_version, "
            "equivalence.producer_equivalence_class, identity.writer_id, "
            "identity.python_abi, identity.pillow_build, identity.libjpeg_build, "
            "identity.zlib_build, seal.producer_fingerprint_sha256 "
            "FROM catalog_artifact_producer_fingerprint_anchors AS anchor "
            "LEFT JOIN catalog_artifact_producer_fingerprint_algorithm_versions "
            "AS algorithm ON algorithm.producer_fingerprint_sha256 = "
            "anchor.producer_fingerprint_sha256 "
            "LEFT JOIN catalog_artifact_producer_fingerprint_equivalence_classes "
            "AS equivalence ON equivalence.producer_fingerprint_sha256 = "
            "anchor.producer_fingerprint_sha256 "
            "LEFT JOIN catalog_artifact_producer_fingerprint_identities AS identity "
            "ON identity.producer_fingerprint_sha256 = "
            "anchor.producer_fingerprint_sha256 "
            "LEFT JOIN catalog_artifact_producer_fingerprint_seals AS seal "
            "ON seal.producer_fingerprint_sha256 = anchor.producer_fingerprint_sha256 "
            "WHERE anchor.producer_fingerprint_sha256 = %s",
            (fingerprint,),
        )
        natural_row = work.connector.fetch_one(
            "SELECT producer_fingerprint_sha256 "
            "FROM catalog_artifact_producer_fingerprint_identities "
            "WHERE writer_id = %s AND python_abi = %s AND pillow_build = %s "
            "AND libjpeg_build = %s AND zlib_build = %s",
            producer_fields,
        )

        expected = (
            fingerprint,
            algorithm_version,
            equivalence_class,
            *producer_fields,
            fingerprint,
        )
        if digest_row:
            if digest_row != expected or natural_row != (fingerprint,):
                raise ArtifactPreparationConflictError(
                    "artifact producer digest collides with another exact registration"
                )
            return ArtifactProducerRegistration(
                fingerprint,
                algorithm_version,
                equivalence_class,
                *producer_fields,
                True,
            )
        if natural_row:
            raise ArtifactPreparationConflictError(
                "artifact producer natural identity is already registered differently"
            )

        connector = work.connector
        connector.execute(
            "INSERT INTO catalog_artifact_producer_fingerprint_anchors "
            "(producer_fingerprint_sha256) VALUES (%s)",
            (fingerprint,),
        )
        connector.execute(
            "INSERT INTO catalog_artifact_producer_fingerprint_algorithm_versions "
            "(producer_fingerprint_sha256, artifact_algorithm_version) "
            "VALUES (%s, %s)",
            (fingerprint, algorithm_version),
        )
        connector.execute(
            "INSERT INTO catalog_artifact_producer_fingerprint_equivalence_classes "
            "(producer_fingerprint_sha256, producer_equivalence_class) "
            "VALUES (%s, %s)",
            (fingerprint, equivalence_class),
        )
        connector.execute(
            "INSERT INTO catalog_artifact_producer_fingerprint_identities "
            "(writer_id, python_abi, pillow_build, libjpeg_build, zlib_build, "
            "producer_fingerprint_sha256) VALUES (%s, %s, %s, %s, %s, %s)",
            (*producer_fields, fingerprint),
        )
        connector.execute(
            "INSERT INTO catalog_artifact_producer_fingerprint_seals "
            "(producer_fingerprint_sha256) VALUES (%s)",
            (fingerprint,),
        )
        return ArtifactProducerRegistration(
            fingerprint,
            algorithm_version,
            equivalence_class,
            *producer_fields,
            False,
        )

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
            "SELECT seal.publication_key FROM catalog_prepared_artifact_seals AS seal "
            "JOIN catalog_prepared_artifact_states AS state "
            "ON state.candidate_id = seal.candidate_id "
            "AND state.publication_key = seal.publication_key "
            "WHERE seal.candidate_id = %s AND seal.publication_key > %s "
            "AND state.state IN ('PREPARED', 'COMMITTED') "
            "ORDER BY seal.publication_key LIMIT 128",
            (mutation.candidate.candidate_id, checkpoint.cursor),
        )
        actual = tuple(
            require_digest32(row[0], field="prepared publication")
            for row in actual_rows
        )
        if actual != expected:
            raise ArtifactPreparationConflictError(
                "prepared artifact coverage differs from CREATE/REBUILD"
            )
        storage_codec = (
            None if not expected else _load_storage_codec(work, _STORAGE_CODEC_V1[0])
        )
        if storage_codec is not None and storage_codec != _STORAGE_CODEC_V1:
            raise ArtifactPreparationContractUnavailableError(
                "prepared artifact storage codec is unsupported"
            )
        for publication in expected:
            assert storage_codec is not None
            _validate_prepared_row(
                work,
                mutation,
                publication,
                storage_codec=storage_codec,
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
            publication,
            now=timestamp,
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
            facts.max_image_short_side,
            facts.producer_fingerprint_sha256,
            facts.producer_fields,
            facts.writer_policy,
            facts.storage_codec,
            facts.spam_artist_threshold,
            facts.spam_occurrence_threshold,
            facts.validation_checkpoint,
            facts.validation_terminal_receipt,
            _AUTHORITY_TOKEN,
        )

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
        """Render and verify deterministic ZIP bytes without external protection."""

        if not isinstance(audit, ArtifactPreparationInputAudit):
            raise TypeError("audit must be ArtifactPreparationInputAudit")
        audit.__post_init__()
        _require_authority(audit.authority)
        if audit._capability is not _AUDIT_TOKEN:
            raise TypeError("artifact input audit is not repository-issued")
        if not hasattr(adapter, "render_member") or not hasattr(adapter, "protect"):
            raise TypeError("adapter must implement ArtifactStorageAdapter")
        authority = audit.authority
        if (
            require_bounded_bytes(
                adapter.adapter_id,
                field="artifact adapter_id",
                minimum=1,
                maximum=64,
            )
            != authority.storage_codec[1]
        ):
            raise ArtifactPreparationContractUnavailableError(
                "artifact adapter does not match the registered storage codec"
            )
        if (
            require_digest32(
                adapter.producer_fingerprint_sha256,
                field="artifact adapter producer fingerprint",
            )
            != authority.producer_fingerprint_sha256
        ):
            raise ArtifactPreparationContractUnavailableError(
                "artifact adapter does not match the policy producer fingerprint"
            )
        archive_plan = _prepare_archive_source_plan(
            connector,
            backend=backend,
            audit=audit,
        )
        archive = TemporaryFile(mode="w+b")
        archive_owned = True
        locator_plan: CanonicalValueUploadPlan | None = None
        try:
            _write_canonical_archive(archive_plan, archive, audit, adapter)
            artifact_sha256, size_bytes = _hash_stream(archive)
            _validate_canonical_archive(
                archive_plan,
                archive,
                audit,
                expected_size=size_bytes,
            )
            locator_components = identity.artifact_locator_components(artifact_sha256)
            locator_plan = CanonicalValueUploadPlan.from_parts(
                "artifact_locator_bytes_v1",
                identity.iter_artifact_locator_payload(locator_components),
            )
            locator_digest = identity.artifact_locator_digest(locator_components)
            if locator_plan.value_sha256 != locator_digest:
                raise ArtifactPreparationConflictError(
                    "artifact locator upload plan has the wrong digest"
                )
            receipt = ArtifactPreparationReceipt(
                audit=audit,
                artifact_sha256=artifact_sha256,
                size_bytes=size_bytes,
                locator_components=locator_components,
                artifact_locator_sha256=locator_digest,
                storage_codec_version=authority.storage_codec[0],
                locator_plan=locator_plan,
                archive=archive,
                _capability=_PREPARATION_RECEIPT_TOKEN,
            )
            locator_plan = None
            archive_owned = False
            return receipt
        finally:
            if locator_plan is not None:
                locator_plan.close()
            if archive_owned:
                archive.close()
            archive_plan.close()

    @staticmethod
    def protect_prepared_artifact(
        connector: SQLConnector,
        *,
        backend: str,
        receipt: ArtifactPreparationReceipt,
        intent: ArtifactProtectionIntent,
        adapter: ArtifactStorageAdapter,
    ) -> ArtifactProtectionEvidence:
        """Idempotently protect bytes for one already-durable PENDING intent."""

        _require_preparation_receipt(receipt)
        if not isinstance(intent, ArtifactProtectionIntent):
            raise TypeError("intent must be ArtifactProtectionIntent")
        intent.__post_init__()
        authority = receipt.audit.authority
        with connector.read_transaction():
            durable_family = _load_prepared_family_or_conflict(
                connector,
                candidate_id=authority.candidate_id,
                publication_key=authority.publication_key,
                backend=backend,
            )
            if durable_family is None:
                raise ArtifactPreparationNotReadyError(
                    "artifact protection intent is not committed and visible"
                )
            durable_intent = _protection_intent_from_family(
                VNextUnitOfWork(connector, backend=backend),
                receipt,
                durable_family,
                replayed=True,
            )
        if durable_family.state != "PENDING":
            raise ArtifactPreparationNotReadyError(
                "artifact protection is only valid for a durable PENDING intent"
            )
        if _intent_facts(durable_intent) != _intent_facts(intent):
            raise ArtifactPreparationConflictError(
                "artifact protection request differs from committed durable intent"
            )
        expected = (
            authority.candidate_id,
            authority.publication_key,
            receipt.artifact_sha256,
            receipt.size_bytes,
            receipt.locator_components,
            receipt.artifact_locator_sha256,
            receipt.storage_codec_version,
        )
        actual = (
            intent.candidate_id,
            intent.publication_key,
            intent.artifact_sha256,
            intent.size_bytes,
            intent.locator_components,
            intent.artifact_locator_sha256,
            intent.storage_codec_version,
        )
        if actual != expected or intent.state != "PENDING":
            raise ArtifactPreparationConflictError(
                "artifact protection intent differs from the verified receipt"
            )
        adapter_id = require_bounded_bytes(
            adapter.adapter_id,
            field="artifact adapter_id",
            minimum=1,
            maximum=64,
        )
        producer = require_digest32(
            adapter.producer_fingerprint_sha256,
            field="artifact adapter producer fingerprint",
        )
        if adapter_id != authority.storage_codec[1]:
            raise ArtifactPreparationContractUnavailableError(
                "artifact adapter does not match the registered storage codec"
            )
        if producer != authority.producer_fingerprint_sha256:
            raise ArtifactPreparationContractUnavailableError(
                "artifact adapter does not match the policy producer fingerprint"
            )
        if _hash_stream(receipt._archive) != (
            receipt.artifact_sha256,
            receipt.size_bytes,
        ):
            raise ArtifactPreparationConflictError(
                "verified artifact archive changed before external protection"
            )
        receipt._archive.seek(0)
        raw = adapter.protect(
            receipt._archive,
            intent.locator_components,
            intent.protection_token,
        )
        if not isinstance(raw, ArtifactStorageEvidence) or not raw.stored:
            raise ArtifactPreparationNotReadyError(
                "artifact storage adapter did not acknowledge exact protection"
            )
        if _hash_stream(receipt._archive) != (
            receipt.artifact_sha256,
            receipt.size_bytes,
        ):
            raise ArtifactPreparationConflictError(
                "artifact storage adapter changed the verified archive bytes"
            )
        return ArtifactProtectionEvidence(
            intent,
            adapter_id,
            producer,
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
    ) -> ArtifactProtectionIntent:
        """Seal one PENDING storage intent before any external protection call."""

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
        existing = _load_prepared_family_or_conflict(
            work.connector,
            candidate_id=authority.candidate_id,
            publication_key=authority.publication_key,
            backend=work.backend,
        )
        if existing is not None:
            return _protection_intent_from_family(
                work,
                receipt,
                existing,
                replayed=True,
            )
        if live_generation != authority.projection.generation:
            raise ArtifactPreparationNotReadyError(
                "new artifact storage intent requires the live projection generation"
            )
        locator = receipt.locator_plan
        if locator.value_sha256 != receipt.artifact_locator_sha256:
            raise ArtifactPreparationConflictError(
                "prepared locator plan changed before persistence"
            )
        sealed = load_sealed_value_identity(
            work.connector,
            value_sha256=receipt.artifact_locator_sha256,
        )
        if (
            sealed is None
            or sealed.digest_domain != b"artifact_locator_bytes_v1"
            or sealed.byte_count != locator.byte_count
        ):
            raise ArtifactPreparationNotReadyError(
                "artifact locator canonical value is not exactly sealed"
            )
        require_digest32(
            sealed.root_page_sha256,
            field="artifact locator root page",
        )
        claim = work.connector.fetch_one(
            "SELECT generation, value_sha256 "
            "FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (live_generation, receipt.artifact_locator_sha256),
        )
        if claim != (live_generation, receipt.artifact_locator_sha256):
            raise ArtifactPreparationNotReadyError(
                "artifact locator first consumer lacks its upload claim"
            )
        protection_token = identity.encode_artifact_protection_token(
            receipt.storage_codec_version,
            authority.candidate_id,
            authority.publication_key,
            receipt.artifact_sha256,
            receipt.artifact_locator_sha256,
            live_generation,
            receipt.size_bytes,
        )
        _persist_artifact_byte_identities(
            work,
            receipt,
            storage_generation=live_generation,
            protection_token=protection_token,
        )
        deleted = work.connector.execute_affected(
            "DELETE FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (live_generation, receipt.artifact_locator_sha256),
        )
        if deleted != 1:
            raise ArtifactPreparationConflictError(
                "artifact locator claim changed during consumer handoff"
            )
        persisted = _load_prepared_family_or_conflict(
            work.connector,
            candidate_id=authority.candidate_id,
            publication_key=authority.publication_key,
            backend=work.backend,
        )
        if persisted is None:  # pragma: no cover - seal insert proves this
            raise ArtifactPreparationConflictError(
                "prepared artifact intent vanished after persistence"
            )
        return _protection_intent_from_family(
            work,
            receipt,
            persisted,
            replayed=False,
        )

    @staticmethod
    def confirm_prepared_artifact(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        receipt: ArtifactPreparationReceipt,
        intent: ArtifactProtectionIntent,
        evidence: ArtifactProtectionEvidence | None,
        effect_seal: OperationalEffectSeal,
        now: int,
    ) -> ArtifactPersistenceReceipt:
        """Acknowledge external protection and publish one prepared occurrence."""

        _require_preparation_receipt(receipt)
        timestamp = require_int63(now, field="prepared artifact confirmed_at")
        _authorize_artifact_mutation(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            now=timestamp,
        )
        if not isinstance(intent, ArtifactProtectionIntent):
            raise TypeError("intent must be ArtifactProtectionIntent")
        intent.__post_init__()
        authority = receipt.audit.authority
        mutation = _load_projection_authority(work, authority.projection)
        current_facts = _load_authority_facts(
            work,
            authority.projection,
            authority.publication_key,
            now=timestamp,
        )
        if current_facts != _facts_from_authority(authority):
            raise ArtifactPreparationConflictError(
                "prepared artifact input authority changed before confirmation"
            )
        family = _load_prepared_family_or_conflict(
            work.connector,
            candidate_id=authority.candidate_id,
            publication_key=authority.publication_key,
            backend=work.backend,
        )
        if family is None:
            raise ArtifactPreparationNotReadyError(
                "artifact confirmation lacks a durable PENDING intent"
            )
        durable_intent = _protection_intent_from_family(
            work,
            receipt,
            family,
            replayed=True,
        )
        if _intent_facts(durable_intent) != _intent_facts(intent):
            raise ArtifactPreparationConflictError(
                "artifact confirmation intent differs from durable facts"
            )
        was_pending = family.state == "PENDING"
        if was_pending:
            if (
                not isinstance(evidence, ArtifactProtectionEvidence)
                or evidence._capability is not _PROTECTION_EVIDENCE_TOKEN
                or _intent_facts(evidence.intent) != _intent_facts(durable_intent)
                or evidence.adapter_id != authority.storage_codec[1]
                or evidence.producer_fingerprint_sha256
                != authority.producer_fingerprint_sha256
            ):
                raise ArtifactPreparationNotReadyError(
                    "PENDING artifact lacks exact repository-issued protection evidence"
                )
            try:
                family = cas_prepared_artifact_state(
                    work,
                    candidate_id=authority.candidate_id,
                    publication_key=authority.publication_key,
                    expected_state="PENDING",
                    next_state="PREPARED",
                )
            except (ArtifactFamilyCollisionError, ArtifactFamilyPartialError) as error:
                raise ArtifactPreparationConflictError(
                    "prepared artifact state changed during confirmation"
                ) from error
        elif evidence is not None and (
            evidence._capability is not _PROTECTION_EVIDENCE_TOKEN
            or _intent_facts(evidence.intent) != _intent_facts(durable_intent)
        ):
            raise ArtifactPreparationConflictError(
                "replayed protection evidence differs from durable intent"
            )
        if family.state not in {"PREPARED", "COMMITTED"}:
            raise ArtifactPreparationConflictError(
                "confirmed artifact has an invalid durable state"
            )
        if was_pending:
            _insert_catalog_artifact_occurrence(
                work,
                mutation,
                publication_key=authority.publication_key,
                artifact_sha256=receipt.artifact_sha256,
                artifact_semantics_sha256=authority.artifact_semantics_sha256,
            )
        else:
            try:
                occurrence = load_catalog_artifact_family(
                    work.connector,
                    revision=mutation.candidate.reserved_revision,
                    publication_key=authority.publication_key,
                    backend=work.backend,
                )
            except (ArtifactFamilyCollisionError, ArtifactFamilyPartialError) as error:
                raise ArtifactPreparationConflictError(
                    "replayed catalog artifact occurrence is incomplete"
                ) from error
            expected_occurrence = CatalogArtifactFamily(
                mutation.candidate.reserved_revision,
                authority.publication_key,
                receipt.artifact_sha256,
                authority.artifact_semantics_sha256,
            )
            if occurrence != expected_occurrence:
                raise ArtifactPreparationConflictError(
                    "replayed catalog artifact occurrence differs from durable intent"
                )
        _bind_operational_effect_seal(
            work,
            candidate_id=authority.candidate_id,
            build_id=authority.build_id,
            effect_seal=effect_seal,
            now=timestamp,
            allow_insert=was_pending,
        )
        return ArtifactPersistenceReceipt(
            authority.candidate_id,
            authority.publication_key,
            receipt.artifact_sha256,
            receipt.artifact_locator_sha256,
            family.protection_token,
            family.state,
            not was_pending,
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
    database = sqlite3.connect(Path(temporary.name) / "plan.sqlite3")
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


def _prepare_archive_source_plan(
    connector: SQLConnector,
    *,
    backend: str,
    audit: ArtifactPreparationInputAudit,
) -> _ArchiveSourcePlan:
    temporary = TemporaryDirectory(prefix="h2hdb-artifact-archive-")
    database = sqlite3.connect(Path(temporary.name) / "archive.sqlite3")
    try:
        database.executescript(
            "CREATE TABLE members ("
            "entry_position INTEGER PRIMARY KEY, source_name_bytes BLOB NOT NULL, "
            "source_file_sha256 BLOB NOT NULL, source_size_bytes INTEGER NOT NULL, "
            "excluded_flag INTEGER NOT NULL, archive_name BLOB, "
            "transform_kind INTEGER NOT NULL, device BLOB NOT NULL, "
            "inode BLOB NOT NULL, modified_ns BLOB NOT NULL, changed_ns BLOB NOT NULL);"
        )
        with connector.read_transaction():
            work = VNextUnitOfWork(connector, backend=backend)
            authority = audit.authority
            _load_projection_authority(work, authority.projection)
            contract = _load_projection_contract(work, authority.projection)
            current = _load_authority_facts(
                work,
                authority.projection,
                authority.publication_key,
                now=None,
                contract=contract,
            )
            if current != _facts_from_authority(authority):
                raise ArtifactPreparationConflictError(
                    "artifact authority changed before archive preparation"
                )
            _validate_fixed_components(work, authority)
            source_directory = _load_source_directory(
                work,
                authority,
                contract=contract,
            )
            member_payload = TemporaryFile(mode="w+b")
            try:
                receipt = CanonicalValueRepository.stream_and_validate(
                    work,
                    value_sha256=authority.member_plan_component_sha256,
                    consume_provisional=lambda part: _write_all(member_payload, part),
                )
                if receipt.digest_domain != b"artifact_member_plan_v1":
                    raise ArtifactPreparationConflictError(
                        "archive member plan has the wrong canonical domain"
                    )
                member_payload.seek(0)
                if _read_exact(member_payload, len(_MEMBER_PLAN_PREFIX)) != (
                    _MEMBER_PLAN_PREFIX
                ):
                    raise ArtifactPreparationConflictError(
                        "archive member plan prefix differs"
                    )
                if int.from_bytes(_read_exact(member_payload, 4), "big") != 1:
                    raise ArtifactPreparationConflictError(
                        "archive member plan version is not registered"
                    )
                member_count = require_int63(
                    int.from_bytes(_read_exact(member_payload, 8), "big"),
                    field="archive member count",
                )
                _spool_archive_source_rows(
                    work,
                    authority,
                    member_payload,
                    database,
                    member_count=member_count,
                )
                if member_payload.read(1):
                    raise ArtifactPreparationConflictError(
                        "archive member plan has trailing bytes"
                    )
            finally:
                member_payload.close()
        database.commit()
        if member_count != audit.source_entry_count:
            raise ArtifactPreparationConflictError(
                "archive source plan differs from its input audit"
            )
        return _ArchiveSourcePlan(
            database=database,
            temporary_directory=temporary,
            source_directory=source_directory,
            member_count=member_count,
        )
    except BaseException:
        database.close()
        temporary.cleanup()
        raise


def _load_source_directory(
    work: VNextUnitOfWork,
    authority: ArtifactPreparationAuthority,
    *,
    contract: _ArtifactContractFacts,
) -> Path:
    row = work.connector.fetch_one(
        "SELECT gallery.scope_key, gallery.locator_sha256 "
        "FROM catalog_gallery_identity_seals AS gallery_seal "
        "JOIN catalog_gallery_identity_coordinates AS gallery "
        "ON gallery.gallery_id = gallery_seal.gallery_id "
        "WHERE gallery_seal.gallery_id = %s",
        (authority.gallery_id,),
    )
    if len(row) != 2:
        raise ArtifactPreparationContractUnavailableError(
            "artifact archive source has no exact gallery identity"
        )
    gallery_scope = require_digest32(row[0], field="artifact gallery scope_key")
    if gallery_scope != contract.scope_key:
        raise ArtifactPreparationConflictError(
            "artifact archive gallery belongs to another source scope"
        )
    if contract.source_provider != b"filesystem":
        raise ArtifactPreparationContractUnavailableError(
            "artifact archive source requires the filesystem provider"
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
        root = identity.decode_source_root(bytes(root_payload))
        locator = identity.decode_source_relative_locator(bytes(locator_payload))
    except (
        CanonicalValueCollisionError,
        CanonicalValueNotReadyError,
        identity.VNextIdentityError,
    ) as error:
        raise ArtifactPreparationConflictError(
            "artifact source root or locator is incomplete or malformed"
        ) from error
    path = Path("/")
    for component in (*root, *locator):
        path /= component
    return path


def _spool_archive_source_rows(
    work: VNextUnitOfWork,
    authority: ArtifactPreparationAuthority,
    payload: BinaryIO,
    database: sqlite3.Connection,
    *,
    member_count: int,
) -> None:
    after = -1
    position = 0
    while True:
        rows = work.connector.fetch_all(
            "SELECT source.file_no, name.name_bytes, name.file_role, "
            "source.file_sha256, blob.size_bytes, decision.occurrence_count, "
            "decision.artist_count, decision.maximum_gallery_artist_count, "
            "filesystem.device, filesystem.inode, filesystem.modified_ns, "
            "filesystem.changed_ns "
            + _SOURCE_FILE_FAMILY_SQL
            + "JOIN catalog_content_blobs blob ON blob.file_sha256 = source.file_sha256 "
            "JOIN catalog_gallery_observation_file_filesystem filesystem "
            "ON filesystem.gallery_id = source.gallery_id "
            "AND filesystem.observation_id = source.observation_id "
            "AND filesystem.file_key = source.file_key "
            "LEFT JOIN catalog_analysis_file_hash_decision_resolved decision "
            "ON decision.analysis_id = %s AND decision.file_sha256 = source.file_sha256 "
            "WHERE source.gallery_id = %s AND source.observation_id = %s "
            "AND source.file_no > %s ORDER BY source.file_no LIMIT 128",
            (
                authority.analysis_id,
                authority.gallery_id,
                authority.observation_id,
                after,
            ),
        )
        if not rows:
            break
        for row in rows:
            if position >= member_count:
                raise ArtifactPreparationConflictError(
                    "archive member plan omits a source row"
                )
            file_no = require_int63(row[0], field="archive source file_no")
            if file_no != position:
                raise ArtifactPreparationConflictError(
                    "archive source positions are not zero-based contiguous"
                )
            entry = _read_member_entry(payload, expected_position=position)
            name = require_bounded_bytes(
                row[1], field="archive source name", minimum=1, maximum=255
            )
            role = require_bounded_bytes(
                row[2], field="archive source role", minimum=7, maximum=8
            )
            digest = require_digest32(row[3], field="archive source digest")
            size = require_int63(row[4], field="archive source size")
            excluded = _source_file_excluded(row, authority, role=role)
            if (
                entry.source_name_bytes != name
                or entry.source_file_sha256 != digest
                or entry.source_size_bytes != size
                or entry.excluded_flag is not excluded
            ):
                raise ArtifactPreparationConflictError(
                    "archive member plan differs from immutable source facts"
                )
            filesystem = tuple(
                require_bounded_bytes(
                    value,
                    field="archive filesystem scalar",
                    minimum=8,
                    maximum=8,
                )
                for value in row[8:12]
            )
            database.execute(
                "INSERT INTO members VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    position,
                    sqlite3.Binary(name),
                    sqlite3.Binary(digest),
                    size,
                    int(excluded),
                    (
                        None
                        if entry.archive_member_name_bytes is None
                        else sqlite3.Binary(entry.archive_member_name_bytes)
                    ),
                    int(entry.transform_kind),
                    *(sqlite3.Binary(value) for value in filesystem),
                ),
            )
            after = file_no
            position += 1
        if len(rows) < _MAX_SOURCE_PAGE:
            break
    if position != member_count:
        raise ArtifactPreparationConflictError(
            "archive member plan has rows outside the source snapshot"
        )


def _write_canonical_archive(
    plan: _ArchiveSourcePlan,
    archive_stream: BinaryIO,
    audit: ArtifactPreparationInputAudit,
    adapter: ArtifactStorageAdapter,
) -> None:
    archive_stream.seek(0)
    archive_stream.truncate(0)
    with zipfile.ZipFile(
        archive_stream,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
        allowZip64=True,
        strict_timestamps=True,
    ) as archive:
        cursor = plan.database.execute(
            "SELECT entry_position, source_name_bytes, source_file_sha256, "
            "source_size_bytes, excluded_flag, archive_name, transform_kind, "
            "device, inode, modified_ns, changed_ns "
            "FROM members ORDER BY entry_position"
        )
        while rows := cursor.fetchmany(_MAX_SOURCE_PAGE):
            for row in rows:
                if bool(row[4]):
                    if row[5] is not None:
                        raise ArtifactPreparationConflictError(
                            "excluded archive source unexpectedly has a member name"
                        )
                    continue
                archive_name = require_bounded_bytes(
                    row[5],
                    field="archive member name",
                    minimum=1,
                    maximum=_ZIP_MEMBER_NAME_MAXIMUM_BYTES,
                )
                try:
                    member_name = archive_name.decode("ascii", errors="strict")
                except UnicodeDecodeError as error:
                    raise ArtifactPreparationConflictError(
                        "derived archive member name is not ASCII"
                    ) from error
                source_name = require_bounded_bytes(
                    row[1], field="archive source name", minimum=1, maximum=255
                )
                source_path = plan.source_directory / os.fsdecode(source_name)
                source = _open_verified_source(source_path, row)
                try:
                    info = _CanonicalZipInfo(
                        member_name, date_time=(1980, 1, 1, 0, 0, 0)
                    )
                    info.compress_type = zipfile.ZIP_DEFLATED
                    info.create_system = 3
                    info.external_attr = 33188 << 16
                    info.flag_bits = 2048
                    transform = identity.ArtifactTransformKind(
                        require_int63(row[6], field="archive transform kind")
                    )
                    with archive.open(info, mode="w", force_zip64=False) as destination:
                        if transform is identity.ArtifactTransformKind.RAW_COPY:
                            _copy_and_verify_source(
                                source,
                                cast(BinaryIO, destination),
                                expected_sha256=require_digest32(
                                    row[2], field="archive raw source digest"
                                ),
                                expected_size=require_int63(
                                    row[3], field="archive raw source size"
                                ),
                            )
                        else:
                            before = _hash_open_stream(source)
                            if before != (
                                require_digest32(row[2], field="archive source digest"),
                                require_int63(row[3], field="archive source size"),
                            ):
                                raise ArtifactPreparationConflictError(
                                    "archive transform source bytes changed"
                                )
                            source.seek(0)
                            adapter.render_member(
                                source, transform, cast(BinaryIO, destination)
                            )
                            after = _hash_open_stream(source)
                            if after != before:
                                raise ArtifactPreparationConflictError(
                                    "archive transform source changed while rendering"
                                )
                    _require_open_source_stat(source, row)
                finally:
                    source.close()
        archive.comment = audit.zip_comment
    archive_stream.flush()


def _open_verified_source(path: Path, row: tuple[Any, ...]) -> BinaryIO:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ArtifactPreparationNotReadyError(
            f"artifact source file is unavailable: {path}"
        ) from error
    source = os.fdopen(descriptor, "rb", closefd=True)
    try:
        _require_open_source_stat(source, row)
    except BaseException:
        source.close()
        raise
    return source


def _require_open_source_stat(source: BinaryIO, row: tuple[Any, ...]) -> None:
    value = os.fstat(source.fileno())
    if not stat.S_ISREG(value.st_mode):
        raise ArtifactPreparationConflictError("artifact source is not a regular file")
    expected = (
        int.from_bytes(bytes(row[7]), "big"),
        int.from_bytes(bytes(row[8]), "big"),
        require_int63(row[3], field="artifact source stat size"),
        int.from_bytes(bytes(row[9]), "big", signed=True),
        int.from_bytes(bytes(row[10]), "big", signed=True),
    )
    actual = (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )
    if actual != expected:
        raise ArtifactPreparationConflictError(
            "artifact source filesystem identity changed after observation"
        )


def _hash_open_stream(source: BinaryIO) -> tuple[bytes, int]:
    source.seek(0)
    digest = sha256()
    count = 0
    while part := source.read(_MAX_READ_BYTES):
        digest.update(part)
        count += len(part)
        if count > INT63_MAX:
            raise ArtifactPreparationNotReadyError(
                "artifact source byte count is exhausted"
            )
    return digest.digest(), count


def _copy_and_verify_source(
    source: BinaryIO,
    destination: BinaryIO,
    *,
    expected_sha256: bytes,
    expected_size: int,
) -> None:
    source.seek(0)
    digest = sha256()
    count = 0
    while part := source.read(_MAX_READ_BYTES):
        digest.update(part)
        count += len(part)
        if destination.write(part) != len(part):
            raise OSError("ZIP member writer accepted a partial source chunk")
    if digest.digest() != expected_sha256 or count != expected_size:
        raise ArtifactPreparationConflictError(
            "artifact source bytes differ from their immutable observation"
        )


def _hash_stream(stream: BinaryIO) -> tuple[bytes, int]:
    stream.seek(0)
    digest = sha256()
    count = 0
    while part := stream.read(_MAX_READ_BYTES):
        digest.update(part)
        count += len(part)
        if count > INT63_MAX:
            raise ArtifactPreparationNotReadyError(
                "artifact archive byte count is exhausted"
            )
    return digest.digest(), count


def _validate_canonical_archive(
    plan: _ArchiveSourcePlan,
    archive_stream: BinaryIO,
    audit: ArtifactPreparationInputAudit,
    *,
    expected_size: int,
) -> None:
    archive_stream.seek(0, os.SEEK_END)
    if archive_stream.tell() != expected_size:
        raise ArtifactPreparationConflictError(
            "artifact archive size changed after hashing"
        )
    archive_stream.seek(0)
    try:
        with zipfile.ZipFile(archive_stream, mode="r") as archive:
            if archive.comment != audit.zip_comment:
                raise ArtifactPreparationConflictError(
                    "artifact archive comment differs from canonical semantics"
                )
            infos = archive.infolist()
            expected_rows = plan.database.execute(
                "SELECT archive_name, transform_kind, source_file_sha256 "
                "FROM members WHERE excluded_flag = 0 ORDER BY entry_position"
            ).fetchall()
            if len(infos) != len(expected_rows) or len(infos) != (
                audit.emitted_member_count
            ):
                raise ArtifactPreparationConflictError(
                    "artifact archive member count differs from its plan"
                )
            for info, expected in zip(infos, expected_rows, strict=True):
                name = require_bounded_bytes(
                    expected[0], field="validated archive name", minimum=1, maximum=255
                ).decode("ascii", errors="strict")
                if (
                    info.filename != name
                    or info.compress_type != 8
                    or info.flag_bits != 2048
                    or info.date_time != (1980, 1, 1, 0, 0, 0)
                    or info.create_system != 3
                    or info.external_attr >> 16 != 33188
                    or info.extra
                    or info.comment
                ):
                    raise ArtifactPreparationConflictError(
                        "artifact ZIP member metadata differs from writer policy"
                    )
                digest = sha256()
                with archive.open(info, mode="r") as member:
                    while part := member.read(_MAX_READ_BYTES):
                        digest.update(part)
                if int(expected[1]) == int(
                    identity.ArtifactTransformKind.RAW_COPY
                ) and digest.digest() != bytes(expected[2]):
                    raise ArtifactPreparationConflictError(
                        "artifact raw ZIP member differs from source bytes"
                    )
            if archive.testzip() is not None:
                raise ArtifactPreparationConflictError(
                    "artifact ZIP member CRC validation failed"
                )
    except (OSError, zipfile.BadZipFile, UnicodeError) as error:
        raise ArtifactPreparationConflictError(
            "artifact archive is not one exact canonical ZIP"
        ) from error


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
        "publication_key BLOB NOT NULL, entry_position INTEGER NOT NULL, "
        "source_name_bytes BLOB NOT NULL, source_file_sha256 BLOB NOT NULL, "
        "source_size_bytes INTEGER NOT NULL, excluded_flag INTEGER NOT NULL, "
        "PRIMARY KEY (publication_key, entry_position)) WITHOUT ROWID;"
        "CREATE INDEX effective_members ON members "
        "(publication_key, source_file_sha256, entry_position);"
    )


def _load_manifest_policy(
    work: VNextUnitOfWork,
    manifest_policy_id: int,
) -> tuple[int, int]:
    policy_id = require_positive_int63(
        manifest_policy_id,
        field="artifact manifest_policy_id",
    )
    row = work.connector.fetch_one(
        "SELECT algorithm.manifest_algorithm_version, ordering.file_order_version "
        "FROM catalog_manifest_policy_seals AS seal "
        "JOIN catalog_manifest_policy_manifest_algorithm_versions AS algorithm "
        "ON algorithm.manifest_policy_id = seal.manifest_policy_id "
        "JOIN catalog_manifest_policy_file_order_versions AS ordering "
        "ON ordering.manifest_policy_id = seal.manifest_policy_id "
        "JOIN catalog_manifest_policy_identities AS natural "
        "ON natural.manifest_policy_id = seal.manifest_policy_id "
        "AND natural.manifest_algorithm_version = "
        "algorithm.manifest_algorithm_version "
        "AND natural.file_order_version = ordering.file_order_version "
        "WHERE seal.manifest_policy_id = %s",
        (policy_id,),
    )
    if len(row) != 2:
        raise ArtifactPreparationNotReadyError(
            "artifact manifest policy is missing or incomplete"
        )
    algorithm_version = require_uint32(
        row[0], field="artifact manifest_algorithm_version"
    )
    file_order_version = require_uint32(row[1], field="artifact file_order_version")
    if algorithm_version == 0 or file_order_version == 0:
        raise ArtifactPreparationConflictError(
            "artifact manifest policy has a zero codec version"
        )
    return algorithm_version, file_order_version


def _load_analysis_policy(
    work: VNextUnitOfWork,
    policy_id: int,
) -> tuple[int, int, int, int, int]:
    analysis_policy_id = require_positive_int63(
        policy_id,
        field="artifact analysis policy_id",
    )
    row = work.connector.fetch_one(
        "SELECT algorithm.algorithm_version, artist.spam_artist_threshold, "
        "occurrence.spam_occurrence_threshold, "
        "owner.content_owner_rule_version, winner.gid_winner_rule_version "
        "FROM catalog_analysis_policy_seals AS seal "
        "JOIN catalog_analysis_policy_algorithm_versions AS algorithm "
        "ON algorithm.policy_id = seal.policy_id "
        "JOIN catalog_analysis_policy_spam_artist_thresholds AS artist "
        "ON artist.policy_id = seal.policy_id "
        "JOIN catalog_analysis_policy_spam_occurrence_thresholds AS occurrence "
        "ON occurrence.policy_id = seal.policy_id "
        "JOIN catalog_analysis_policy_content_owner_rule_versions AS owner "
        "ON owner.policy_id = seal.policy_id "
        "JOIN catalog_analysis_policy_gid_winner_rule_versions AS winner "
        "ON winner.policy_id = seal.policy_id "
        "JOIN catalog_analysis_policy_identities AS natural "
        "ON natural.policy_id = seal.policy_id "
        "AND natural.algorithm_version = algorithm.algorithm_version "
        "AND natural.spam_artist_threshold = artist.spam_artist_threshold "
        "AND natural.spam_occurrence_threshold = "
        "occurrence.spam_occurrence_threshold "
        "AND natural.content_owner_rule_version = "
        "owner.content_owner_rule_version "
        "AND natural.gid_winner_rule_version = winner.gid_winner_rule_version "
        "WHERE seal.policy_id = %s",
        (analysis_policy_id,),
    )
    if len(row) != 5:
        raise ArtifactPreparationNotReadyError(
            "artifact analysis policy is missing or incomplete"
        )
    algorithm_version = require_uint32(
        row[0], field="artifact analysis algorithm_version"
    )
    content_owner_version = require_uint32(
        row[3], field="artifact content_owner_rule_version"
    )
    gid_winner_version = require_uint32(
        row[4], field="artifact gid_winner_rule_version"
    )
    if algorithm_version == 0 or content_owner_version == 0 or gid_winner_version == 0:
        raise ArtifactPreparationConflictError(
            "artifact analysis policy has a zero algorithm version"
        )
    return (
        algorithm_version,
        require_int63(row[1], field="artifact spam_artist_threshold"),
        require_int63(row[2], field="artifact spam_occurrence_threshold"),
        content_owner_version,
        gid_winner_version,
    )


def _load_source_scope(
    work: VNextUnitOfWork,
    scope_key: bytes,
) -> tuple[bytes, bytes, int]:
    scope = require_digest32(scope_key, field="artifact scope_key")
    row = work.connector.fetch_one(
        "SELECT provider.source_provider, root.source_root_sha256, "
        "version.identity_policy_version "
        "FROM catalog_source_scope_seals AS seal "
        "JOIN catalog_source_scope_source_providers AS provider "
        "ON provider.scope_key = seal.scope_key "
        "JOIN catalog_source_scope_source_root_sha256s AS root "
        "ON root.scope_key = seal.scope_key "
        "JOIN catalog_source_scope_identity_policy_versions AS version "
        "ON version.scope_key = seal.scope_key "
        "JOIN catalog_source_scope_identities AS natural "
        "ON natural.scope_key = seal.scope_key "
        "AND natural.source_provider = provider.source_provider "
        "AND natural.source_root_sha256 = root.source_root_sha256 "
        "AND natural.identity_policy_version = version.identity_policy_version "
        "WHERE seal.scope_key = %s",
        (scope,),
    )
    if len(row) != 3:
        raise ArtifactPreparationNotReadyError(
            "artifact source scope is missing or incomplete"
        )
    provider = require_bounded_bytes(
        row[0], field="artifact source_provider", minimum=1, maximum=64
    )
    root_sha256 = require_digest32(row[1], field="artifact source_root_sha256")
    policy_version = require_uint32(row[2], field="artifact identity_policy_version")
    if policy_version == 0:
        raise ArtifactPreparationConflictError(
            "artifact source scope has a zero identity-policy version"
        )
    try:
        expected_scope = identity.source_scope_key(
            provider.decode("ascii", errors="strict"),
            root_sha256,
            policy_version,
        )
    except (UnicodeError, identity.VNextIdentityError) as error:
        raise ArtifactPreparationConflictError(
            "artifact source scope has a malformed natural identity"
        ) from error
    if expected_scope != scope:
        raise ArtifactPreparationConflictError(
            "artifact source scope digest differs from its natural identity"
        )
    return provider, root_sha256, policy_version


def _load_artifact_policy_semantics(
    work: VNextUnitOfWork,
    policy_component_sha256: bytes,
) -> tuple[int, int, bytes]:
    policy = require_digest32(
        policy_component_sha256,
        field="artifact policy_component_sha256",
    )
    row = work.connector.fetch_one(
        "SELECT algorithm.artifact_algorithm_version, "
        "side.max_image_short_side, producer.producer_fingerprint_sha256 "
        "FROM catalog_artifact_policy_semantics_seals AS seal "
        "JOIN catalog_artifact_policy_semantics_artifact_algorithm_versions "
        "AS algorithm ON algorithm.policy_component_sha256 = "
        "seal.policy_component_sha256 "
        "JOIN catalog_artifact_policy_semantics_max_image_short_sides AS side "
        "ON side.policy_component_sha256 = seal.policy_component_sha256 "
        "JOIN catalog_artifact_policy_semantics_producer_fingerprint_sha256s "
        "AS producer ON producer.policy_component_sha256 = "
        "seal.policy_component_sha256 "
        "JOIN catalog_artifact_policy_semantics_identities AS natural "
        "ON natural.policy_component_sha256 = seal.policy_component_sha256 "
        "AND natural.artifact_algorithm_version = "
        "algorithm.artifact_algorithm_version "
        "AND natural.max_image_short_side = side.max_image_short_side "
        "AND natural.producer_fingerprint_sha256 = "
        "producer.producer_fingerprint_sha256 "
        "WHERE seal.policy_component_sha256 = %s",
        (policy,),
    )
    if len(row) != 3:
        raise ArtifactPreparationNotReadyError(
            "artifact policy semantics are missing or incomplete"
        )
    algorithm_version = require_uint32(row[0], field="artifact algorithm_version")
    if algorithm_version == 0:
        raise ArtifactPreparationConflictError(
            "artifact policy semantics have a zero algorithm version"
        )
    max_short_side = require_positive_int63(
        row[1], field="artifact max_image_short_side"
    )
    producer = require_digest32(row[2], field="artifact producer_fingerprint_sha256")
    if (
        identity.artifact_policy_digest(
            algorithm_version,
            max_short_side,
            producer,
        )
        != policy
    ):
        raise ArtifactPreparationConflictError(
            "artifact policy digest differs from its registered semantics"
        )
    return algorithm_version, max_short_side, producer


def _load_zip_writer_policy(
    work: VNextUnitOfWork,
    artifact_algorithm_version: int,
) -> tuple[int, int, int, int, int, int, int, int, int, int]:
    algorithm_version = require_uint32(
        artifact_algorithm_version,
        field="artifact ZIP algorithm_version",
    )
    if algorithm_version == 0:
        raise ArtifactPreparationConflictError("artifact ZIP algorithm version is zero")
    row = work.connector.fetch_one(
        "SELECT zip.zip_codec_version, method.compression_method, "
        "level.compression_level, date.dos_date, time.dos_time, mode.unix_mode, "
        "flags.general_purpose_flags, system.create_system, "
        "archive.archive_name_codec_version, "
        "artifact.artifact_name_codec_version "
        "FROM catalog_artifact_zip_writer_policy_seals AS seal "
        "JOIN catalog_artifact_zip_writer_policy_zip_codec_versions AS zip "
        "ON zip.artifact_algorithm_version = seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_compression_methods AS method "
        "ON method.artifact_algorithm_version = seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_compression_levels AS level "
        "ON level.artifact_algorithm_version = seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_dos_dates AS date "
        "ON date.artifact_algorithm_version = seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_dos_times AS time "
        "ON time.artifact_algorithm_version = seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_unix_modes AS mode "
        "ON mode.artifact_algorithm_version = seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_general_purpose_flags AS flags "
        "ON flags.artifact_algorithm_version = seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_create_systems AS system "
        "ON system.artifact_algorithm_version = seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_archive_name_codec_versions "
        "AS archive ON archive.artifact_algorithm_version = "
        "seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_artifact_name_codec_versions "
        "AS artifact ON artifact.artifact_algorithm_version = "
        "seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_identities AS natural "
        "ON natural.artifact_algorithm_version = seal.artifact_algorithm_version "
        "AND natural.zip_codec_version = zip.zip_codec_version "
        "AND natural.compression_method = method.compression_method "
        "AND natural.compression_level = level.compression_level "
        "AND natural.dos_date = date.dos_date "
        "AND natural.dos_time = time.dos_time "
        "AND natural.unix_mode = mode.unix_mode "
        "AND natural.general_purpose_flags = flags.general_purpose_flags "
        "AND natural.create_system = system.create_system "
        "AND natural.archive_name_codec_version = "
        "archive.archive_name_codec_version "
        "AND natural.artifact_name_codec_version = "
        "artifact.artifact_name_codec_version "
        "WHERE seal.artifact_algorithm_version = %s",
        (algorithm_version,),
    )
    if len(row) != 10:
        raise ArtifactPreparationNotReadyError(
            "artifact ZIP writer policy is missing or incomplete"
        )
    return cast(
        tuple[int, int, int, int, int, int, int, int, int, int],
        tuple(
            require_int63(value, field=f"artifact ZIP writer fact {index}")
            for index, value in enumerate(row)
        ),
    )


def _load_storage_codec(
    work: VNextUnitOfWork,
    storage_codec_version: int,
) -> tuple[int, bytes, int, int]:
    version = require_positive_int63(
        storage_codec_version,
        field="artifact storage_codec_version",
    )
    row = work.connector.fetch_one(
        "SELECT adapter.adapter_id, locator.locator_codec_version, "
        "token.protection_token_codec_version "
        "FROM catalog_artifact_storage_codec_seals AS seal "
        "JOIN catalog_artifact_storage_codec_adapter_ids AS adapter "
        "ON adapter.storage_codec_version = seal.storage_codec_version "
        "JOIN catalog_artifact_storage_codec_locator_codec_versions AS locator "
        "ON locator.storage_codec_version = seal.storage_codec_version "
        "JOIN catalog_artifact_storage_codec_protection_token_codec_versions "
        "AS token ON token.storage_codec_version = seal.storage_codec_version "
        "WHERE seal.storage_codec_version = %s",
        (version,),
    )
    if len(row) != 3:
        raise ArtifactPreparationNotReadyError(
            "artifact storage codec is missing or incomplete"
        )
    return (
        version,
        require_bounded_bytes(
            row[0], field="artifact storage adapter_id", minimum=1, maximum=64
        ),
        require_positive_int63(row[1], field="artifact locator_codec_version"),
        require_positive_int63(row[2], field="artifact protection_token_codec_version"),
    )


def _load_producer_fingerprint(
    work: VNextUnitOfWork,
    producer_fingerprint_sha256: bytes,
) -> tuple[tuple[bytes, bytes, bytes, bytes, bytes], int, bytes]:
    fingerprint = require_digest32(
        producer_fingerprint_sha256,
        field="artifact producer_fingerprint_sha256",
    )
    row = work.connector.fetch_one(
        "SELECT natural.writer_id, natural.python_abi, natural.pillow_build, "
        "natural.libjpeg_build, natural.zlib_build, "
        "algorithm.artifact_algorithm_version, "
        "equivalence.producer_equivalence_class "
        "FROM catalog_artifact_producer_fingerprint_seals AS seal "
        "JOIN catalog_artifact_producer_fingerprint_identities AS natural "
        "ON natural.producer_fingerprint_sha256 = "
        "seal.producer_fingerprint_sha256 "
        "JOIN catalog_artifact_producer_fingerprint_algorithm_versions "
        "AS algorithm ON algorithm.producer_fingerprint_sha256 = "
        "seal.producer_fingerprint_sha256 "
        "JOIN catalog_artifact_producer_fingerprint_equivalence_classes "
        "AS equivalence ON equivalence.producer_fingerprint_sha256 = "
        "seal.producer_fingerprint_sha256 "
        "WHERE seal.producer_fingerprint_sha256 = %s",
        (fingerprint,),
    )
    if len(row) != 7:
        raise ArtifactPreparationNotReadyError(
            "artifact producer fingerprint is missing or incomplete"
        )
    fields = cast(
        tuple[bytes, bytes, bytes, bytes, bytes],
        tuple(
            require_bounded_bytes(
                value,
                field=f"artifact producer field {index}",
                minimum=1,
                maximum=128,
            )
            for index, value in enumerate(row[:5])
        ),
    )
    algorithm_version = require_uint32(
        row[5], field="artifact producer algorithm_version"
    )
    equivalence = require_bounded_bytes(
        row[6],
        field="artifact producer equivalence class",
        minimum=1,
        maximum=128,
    )
    if (
        algorithm_version == 0
        or identity.artifact_producer_fingerprint_sha256(*fields) != fingerprint
        or identity.artifact_producer_equivalence_class(fingerprint) != equivalence
    ):
        raise ArtifactPreparationConflictError(
            "artifact producer registry is not exactly derived"
        )
    return fields, algorithm_version, equivalence


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
        "SELECT build_scope.scope_key, build_policy.manifest_policy_id, "
        "policy.policy_component_sha256 "
        "FROM catalog_source_build_descriptor_seals AS build_seal "
        "JOIN catalog_source_build_scope_keys AS build_scope "
        "ON build_scope.build_id = build_seal.build_id "
        "JOIN catalog_source_build_manifest_policy_ids AS build_policy "
        "ON build_policy.build_id = build_seal.build_id "
        "JOIN catalog_source_build_states AS build_state "
        "ON build_state.build_id = build_seal.build_id AND build_state.state = 'SEALED' "
        "JOIN catalog_source_build_sealed_ats AS build_completed "
        "ON build_completed.build_id = build_seal.build_id "
        "JOIN catalog_artifact_policies AS policy "
        "ON policy.artifact_policy_id = %s "
        "WHERE build_seal.build_id = %s",
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
    semantics = _load_artifact_policy_semantics(work, policy_component)
    producer_fields, producer_algorithm, _equivalence = _load_producer_fingerprint(
        work,
        semantics[2],
    )
    if producer_algorithm != semantics[0]:
        raise ArtifactPreparationConflictError(
            "artifact producer algorithm differs from policy semantics"
        )
    writer_policy = _load_zip_writer_policy(work, semantics[0])
    storage_codec = _load_storage_codec(work, _STORAGE_CODEC_V1[0])
    if writer_policy != _ZIP_WRITER_POLICY_V1:
        raise ArtifactPreparationContractUnavailableError(
            "artifact ZIP writer policy is unsupported"
        )
    if storage_codec != _STORAGE_CODEC_V1:
        raise ArtifactPreparationContractUnavailableError(
            "artifact storage codec is unsupported"
        )
    if validate_policy_canonical:
        _require_policy_canonical_value(
            work,
            policy_component=policy_component,
            algorithm_version=semantics[0],
            max_short_side=semantics[1],
            producer_fingerprint_sha256=semantics[2],
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
        producer_fields,
        writer_policy,
        storage_codec,
    )


def _require_policy_canonical_value(
    work: VNextUnitOfWork,
    *,
    policy_component: bytes,
    algorithm_version: int,
    max_short_side: int,
    producer_fingerprint_sha256: bytes,
) -> None:
    expected_payload = identity.encode_artifact_policy(
        algorithm_version,
        max_short_side,
        producer_fingerprint_sha256,
    )
    if (
        identity.artifact_policy_digest(
            algorithm_version,
            max_short_side,
            producer_fingerprint_sha256,
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
            "gallery.gallery_key, gallery_coordinate.scope_key, "
            "member.observation_id, "
            "observation.observation_identity_sha256 "
            "FROM catalog_publication_selections AS selection "
            "JOIN catalog_publication_identities AS pub "
            "ON pub.publication_key = selection.publication_key "
            "JOIN catalog_gallery_identity_seals AS gallery_seal "
            "ON gallery_seal.gallery_id = selection.gallery_id "
            "JOIN catalog_gallery_identity_gallery_keys AS gallery "
            "ON gallery.gallery_id = gallery_seal.gallery_id "
            "JOIN catalog_gallery_identity_coordinates AS gallery_coordinate "
            "ON gallery_coordinate.gallery_id = gallery_seal.gallery_id "
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
    max_short_side = contract.max_image_short_side
    producer = contract.producer_fingerprint_sha256
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
        "artifact_member_plan_v1",
        _parts_size(member_parts()),
        member_parts(),
    )

    def effective_rows() -> Iterator[bytes]:
        return _iter_planned_effective_digests(database, publication)

    effective_count_row = database.execute(
        "SELECT COUNT(*) FROM members WHERE publication_key = ? "
        "AND excluded_flag = 0 AND source_name_bytes != ?",
        (sqlite3.Binary(publication), sqlite3.Binary(b"galleryinfo.txt")),
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
        (member_digest, b"artifact_member_plan_v1", member_parts()),
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
            b"artifact_policy_v2",
            iter(
                (
                    identity.encode_artifact_policy(
                        artifact_algorithm, max_short_side, producer
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
    while True:
        rows = work.connector.fetch_all(
            "SELECT source.file_no, name.name_bytes, source.file_sha256, "
            "blob.size_bytes, decision.occurrence_count, decision.artist_count, "
            "decision.maximum_gallery_artist_count "
            + _SOURCE_FILE_FAMILY_SQL
            + "JOIN catalog_content_blobs AS blob ON blob.file_sha256 = source.file_sha256 "
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
            role = identity.file_role(name)
            excluded = _excluded_from_scalars(
                role,
                row[4:7],
                spam_artist_threshold=spam_artist_threshold,
                spam_occurrence_threshold=spam_occurrence_threshold,
            )
            entry = identity.ArtifactMemberPlanEntry(
                position,
                name,
                require_digest32(row[2], field="planned source digest"),
                require_int63(row[3], field="planned source size"),
                excluded,
            )
            database.execute(
                "INSERT INTO members VALUES (?, ?, ?, ?, ?, ?)",
                (
                    sqlite3.Binary(publication_key),
                    position,
                    sqlite3.Binary(entry.source_name_bytes),
                    sqlite3.Binary(entry.source_file_sha256),
                    entry.source_size_bytes,
                    int(entry.excluded_flag),
                ),
            )
            position += 1
            after = file_no
        if len(rows) < _MAX_SOURCE_PAGE:
            break


def _excluded_from_scalars(
    role: bytes,
    decision: tuple[Any, ...],
    *,
    spam_artist_threshold: int,
    spam_occurrence_threshold: int,
) -> bool:
    if role == b"METADATA":
        if any(value is not None for value in decision):
            raise ArtifactPreparationConflictError(
                "metadata unexpectedly participates in spam decisions"
            )
        return False
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
        "SELECT entry_position, source_name_bytes, source_file_sha256, "
        "source_size_bytes, excluded_flag FROM members "
        "WHERE publication_key = ? ORDER BY entry_position",
        (sqlite3.Binary(publication_key),),
    )
    while rows := cursor.fetchmany(_MAX_SOURCE_PAGE):
        for row in rows:
            yield identity.ArtifactMemberPlanEntry(
                require_int63(row[0], field="planned member position"),
                bytes(row[1]),
                bytes(row[2]),
                require_int63(row[3], field="planned member size"),
                bool(row[4]),
            )


def _iter_planned_effective_digests(
    database: sqlite3.Connection,
    publication_key: bytes,
) -> Iterator[bytes]:
    cursor = database.execute(
        "SELECT source_file_sha256 FROM members WHERE publication_key = ? "
        "AND excluded_flag = 0 AND source_name_bytes != ? "
        "ORDER BY source_file_sha256, entry_position",
        (sqlite3.Binary(publication_key), sqlite3.Binary(b"galleryinfo.txt")),
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
            "SELECT publication_key FROM catalog_artifact_seals "
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
        "SELECT semantics.artifact_semantics_sha256, digest.artifact_sha256 "
        "FROM catalog_artifact_seals AS seal "
        "JOIN catalog_artifact_semantics_sha256s AS semantics "
        "ON semantics.revision = seal.revision "
        "AND semantics.publication_key = seal.publication_key "
        "JOIN catalog_artifact_sha256s AS digest "
        "ON digest.revision = seal.revision "
        "AND digest.publication_key = seal.publication_key "
        "WHERE seal.revision = %s AND seal.publication_key = %s LIMIT 2",
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
    publication = work.connector.fetch_one(
        "SELECT publication_key FROM catalog_publications "
        "WHERE revision = %s AND publication_key = %s",
        (mutation.candidate.reserved_revision, publication_key),
    )
    if publication != (publication_key,):
        raise ArtifactPreparationNotReadyError(
            "artifact occurrence lacks its reserved catalog publication"
        )
    try:
        ensure_catalog_artifact_family(
            work.connector,
            CatalogArtifactFamily(
                mutation.candidate.reserved_revision,
                publication_key,
                artifact_sha256,
                artifact_semantics_sha256,
            ),
            backend=work.backend,
        )
    except (ArtifactFamilyCollisionError, ArtifactFamilyPartialError) as error:
        raise ArtifactPreparationConflictError(
            "catalog artifact occurrence collides with different exact facts"
        ) from error


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
        "SELECT artifact_semantics_sha256 FROM catalog_artifact_delta_new "
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
            "SELECT publication_key FROM catalog_artifact_seals "
            "WHERE revision = %s AND publication_key > %s "
            "AND NOT EXISTS ("
            "SELECT 1 FROM catalog_candidate_artifact_inputs input "
            "WHERE input.candidate_id = %s "
            "AND input.publication_key = catalog_artifact_seals.publication_key) "
            "ORDER BY publication_key LIMIT 128",
            (base, after, candidate),
        )
    elif "DELETE" in operations:
        raise ValueError("DELETE cannot share an artifact-operation evaluator page")
    else:
        predicates: list[str] = []
        parameters: list[Any] = [base]
        old_exists = (
            "SELECT 1 FROM catalog_artifact_seals old_seal "
            "JOIN catalog_artifact_semantics_sha256s old_artifact "
            "ON old_artifact.revision = old_seal.revision "
            "AND old_artifact.publication_key = old_seal.publication_key "
            "WHERE old_seal.revision = %s "
            "AND old_seal.publication_key = input.publication_key"
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
      JOIN catalog_display_title_policy_title_sort_policy_ids AS current_policy_sort
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
          JOIN catalog_publication_commit_catalog_revisions AS old_commit_revision
            ON old_commit_revision.revision = old_title.revision
          JOIN catalog_publication_commit_seals AS old_commit_seal
            ON old_commit_seal.receipt_id = old_commit_revision.receipt_id
          JOIN catalog_publication_commit_display_title_policies AS old_commit_policy
            ON old_commit_policy.receipt_id = old_commit_seal.receipt_id
          JOIN catalog_display_title_choices AS old_choice
            ON old_choice.display_title_policy_id =
               old_commit_policy.display_title_policy_id
           AND old_choice.source_title_sha256 = old_title.source_title_sha256
           AND old_choice.source_gallery_name = old_title.source_gallery_name
          JOIN catalog_display_title_policy_title_sort_policy_ids AS old_policy_sort
            ON old_policy_sort.display_title_policy_id =
               old_commit_policy.display_title_policy_id
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
      JOIN catalog_publication_commit_catalog_revisions AS old_commit_revision
        ON old_commit_revision.revision = old_title.revision
      JOIN catalog_publication_commit_seals AS old_commit_seal
        ON old_commit_seal.receipt_id = old_commit_revision.receipt_id
      JOIN catalog_publication_commit_display_title_policies AS old_commit_policy
        ON old_commit_policy.receipt_id = old_commit_seal.receipt_id
      JOIN catalog_display_title_choices AS old_choice
        ON old_choice.display_title_policy_id =
           old_commit_policy.display_title_policy_id
       AND old_choice.source_title_sha256 = old_title.source_title_sha256
       AND old_choice.source_gallery_name = old_title.source_gallery_name
      JOIN catalog_display_title_policy_title_sort_policy_ids AS old_policy_sort
        ON old_policy_sort.display_title_policy_id =
           old_commit_policy.display_title_policy_id
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
          JOIN catalog_display_title_policy_title_sort_policy_ids AS current_policy_sort
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
    *,
    storage_codec: tuple[int, bytes, int, int],
) -> None:
    try:
        prepared = load_prepared_artifact_family(
            work.connector,
            candidate_id=mutation.candidate.candidate_id,
            publication_key=publication_key,
            backend=work.backend,
        )
        occurrence = load_catalog_artifact_family(
            work.connector,
            revision=mutation.candidate.reserved_revision,
            publication_key=publication_key,
            backend=work.backend,
        )
    except (ArtifactFamilyCollisionError, ArtifactFamilyPartialError) as error:
        raise ArtifactPreparationConflictError(
            "prepared artifact has an invalid narrow family"
        ) from error
    if prepared is None or occurrence is None:
        raise ArtifactPreparationConflictError(
            "prepared artifact lacks one exact family or catalog occurrence"
        )
    if prepared.state != "PREPARED":
        raise ArtifactPreparationConflictError(
            "candidate seal requires PREPARED artifact state"
        )
    if occurrence.artifact_sha256 != prepared.artifact_sha256:
        raise ArtifactPreparationConflictError(
            "prepared occurrence byte digest differs from prepared family"
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
        (mutation.candidate.candidate_id, publication_key),
    )
    if len(operation) != 3 or operation[1] not in {"CREATE", "REBUILD"}:
        raise ArtifactPreparationConflictError(
            "prepared artifact has no exact byte-producing operation"
        )
    if occurrence.artifact_semantics_sha256 != operation[0]:
        raise ArtifactPreparationConflictError(
            "prepared catalog occurrence differs from its semantic input"
        )
    try:
        token = identity.decode_artifact_protection_token(prepared.protection_token)
    except identity.VNextIdentityError as error:
        raise ArtifactPreparationConflictError(
            "prepared artifact protection token is malformed"
        ) from error
    expected_token = (
        prepared.storage_codec_version,
        mutation.candidate.candidate_id,
        publication_key,
        prepared.artifact_sha256,
        token.artifact_locator_sha256,
        token.size_bytes,
    )
    actual_token = (
        token.storage_codec_version,
        token.candidate_id,
        token.publication_key,
        token.artifact_sha256,
        token.artifact_locator_sha256,
        token.size_bytes,
    )
    if actual_token != expected_token:
        raise ArtifactPreparationConflictError(
            "prepared protection token differs from joined authority"
        )
    mapping = work.connector.fetch_one(
        "SELECT build_id FROM operational_source_build_generations "
        "WHERE generation = %s",
        (token.storage_generation,),
    )
    if mapping != (mutation.begin.build_id,):
        raise ArtifactPreparationConflictError(
            "prepared storage generation is not mapped to the candidate build"
        )
    if (
        prepared.storage_codec_version != storage_codec[0]
        or storage_codec != _STORAGE_CODEC_V1
    ):
        raise ArtifactPreparationConflictError(
            "prepared artifact storage codec is not registered"
        )
    locator_parts = bytearray()

    def consume_locator(part: bytes) -> None:
        if len(locator_parts) > 4096 - len(part):
            raise ArtifactPreparationConflictError(
                "prepared artifact locator exceeds its registered bound"
            )
        locator_parts.extend(part)

    try:
        receipt = CanonicalValueRepository.stream_and_validate(
            work,
            value_sha256=token.artifact_locator_sha256,
            consume_provisional=consume_locator,
        )
        components = identity.decode_artifact_locator(bytes(locator_parts))
    except (
        CanonicalValueCollisionError,
        CanonicalValueNotReadyError,
        identity.VNextIdentityError,
    ) as error:
        raise ArtifactPreparationConflictError(
            "prepared artifact locator is incomplete or malformed"
        ) from error
    if (
        receipt.digest_domain != b"artifact_locator_bytes_v1"
        or components != identity.artifact_locator_components(prepared.artifact_sha256)
        or identity.artifact_locator_digest(components) != token.artifact_locator_sha256
    ):
        raise ArtifactPreparationConflictError(
            "prepared artifact locator differs from content-addressed bytes"
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
    # ``_prepare_candidate_batch`` already locked this exact definition seal
    # before any checkpoint lock; re-read it here without violating lock order.
    definition = work.connector.fetch_one(
        "SELECT candidate_id FROM catalog_publication_candidate_definition_seals "
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
        "SELECT count.publication_count "
        "FROM catalog_revision_descriptor_seals AS seal "
        "JOIN catalog_revision_publication_counts AS count "
        "ON count.revision = seal.revision WHERE seal.revision = %s",
        (mutation.candidate.reserved_revision,),
    )
    if revision != (publication_count,):
        raise ArtifactPreparationConflictError(
            "reserved catalog revision count differs from selection"
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


def _require_preparation_receipt(receipt: ArtifactPreparationReceipt) -> None:
    if not isinstance(receipt, ArtifactPreparationReceipt):
        raise TypeError("receipt must be ArtifactPreparationReceipt")
    if receipt._capability is not _PREPARATION_RECEIPT_TOKEN or receipt._closed:
        raise TypeError(
            "artifact preparation receipt is not live and repository-issued"
        )
    receipt.audit.__post_init__()
    authority = receipt.audit.authority
    _require_authority(authority)
    require_digest32(receipt.artifact_sha256, field="prepared artifact_sha256")
    require_int63(receipt.size_bytes, field="prepared artifact size_bytes")
    require_digest32(
        receipt.artifact_locator_sha256,
        field="prepared artifact_locator_sha256",
    )
    require_positive_int63(
        receipt.storage_codec_version,
        field="prepared storage_codec_version",
    )
    if type(receipt.locator_plan) is not CanonicalValueUploadPlan:
        raise TypeError("prepared locator plan must be exact")
    require_digest32(
        receipt.locator_plan.value_sha256,
        field="prepared locator plan value_sha256",
    )
    require_int63(
        receipt.locator_plan.byte_count,
        field="prepared locator plan byte_count",
    )
    if _hash_stream(receipt._archive) != (
        receipt.artifact_sha256,
        receipt.size_bytes,
    ):
        raise ArtifactPreparationConflictError(
            "verified artifact archive bytes changed before durable persistence"
        )
    expected_components = identity.artifact_locator_components(receipt.artifact_sha256)
    if (
        receipt.locator_components != expected_components
        or identity.artifact_locator_digest(expected_components)
        != receipt.artifact_locator_sha256
        or receipt.storage_codec_version != authority.storage_codec[0]
    ):
        raise ArtifactPreparationConflictError(
            "verified artifact receipt differs from derived locator or codec"
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
    return (
        intent.candidate_id,
        intent.publication_key,
        intent.artifact_sha256,
        intent.size_bytes,
        intent.locator_components,
        intent.artifact_locator_sha256,
        intent.storage_codec_version,
        intent.storage_generation,
        intent.protection_token,
    )


def _load_prepared_family_or_conflict(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
    publication_key: bytes,
    backend: str,
) -> PreparedArtifactFamily | None:
    try:
        return load_prepared_artifact_family(
            connector,
            candidate_id=candidate_id,
            publication_key=publication_key,
            backend=backend,
        )
    except (ArtifactFamilyCollisionError, ArtifactFamilyPartialError) as error:
        raise ArtifactPreparationConflictError(
            "prepared artifact family is partial or internally inconsistent"
        ) from error


def _protection_intent_from_family(
    work: VNextUnitOfWork,
    receipt: ArtifactPreparationReceipt,
    family: PreparedArtifactFamily,
    *,
    replayed: bool,
) -> ArtifactProtectionIntent:
    authority = receipt.audit.authority
    if (
        family.candidate_id != authority.candidate_id
        or family.publication_key != authority.publication_key
        or family.artifact_sha256 != receipt.artifact_sha256
        or family.storage_codec_version != receipt.storage_codec_version
    ):
        raise ArtifactPreparationConflictError(
            "durable prepared artifact differs from verified archive facts"
        )
    try:
        token = identity.decode_artifact_protection_token(family.protection_token)
    except identity.VNextIdentityError as error:
        raise ArtifactPreparationConflictError(
            "durable prepared artifact token is malformed"
        ) from error
    expected_token = (
        authority.candidate_id,
        authority.publication_key,
        receipt.artifact_sha256,
        receipt.artifact_locator_sha256,
        receipt.storage_codec_version,
        family.storage_generation,
        receipt.size_bytes,
    )
    actual_token = (
        token.candidate_id,
        token.publication_key,
        token.artifact_sha256,
        token.artifact_locator_sha256,
        token.storage_codec_version,
        token.storage_generation,
        token.size_bytes,
    )
    if actual_token != expected_token:
        raise ArtifactPreparationConflictError(
            "durable prepared artifact token differs from verified archive"
        )
    mapping = work.connector.fetch_one(
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
        receipt.artifact_sha256,
        receipt.size_bytes,
        receipt.locator_components,
        receipt.artifact_locator_sha256,
        receipt.storage_codec_version,
        family.storage_generation,
        family.protection_token,
        family.state,
        replayed,
        _capability=_PROTECTION_INTENT_TOKEN,
    )


def _persist_artifact_byte_identities(
    work: VNextUnitOfWork,
    receipt: ArtifactPreparationReceipt,
    *,
    storage_generation: int,
    protection_token: bytes,
) -> None:
    authority = receipt.audit.authority
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
        "catalog_artifact_location",
        ("artifact_sha256", "artifact_locator_sha256"),
        (receipt.artifact_sha256, receipt.artifact_locator_sha256),
        key_where="artifact_sha256 = %s OR artifact_locator_sha256 = %s",
        key_parameters=(
            receipt.artifact_sha256,
            receipt.artifact_locator_sha256,
        ),
        conflict_label="artifact location",
    )
    try:
        ensure_prepared_artifact_family(
            work.connector,
            PreparedArtifactFamily(
                authority.candidate_id,
                authority.publication_key,
                receipt.artifact_sha256,
                receipt.storage_codec_version,
                storage_generation,
                protection_token,
                "PENDING",
            ),
            backend=work.backend,
        )
    except (ArtifactFamilyCollisionError, ArtifactFamilyPartialError) as error:
        raise ArtifactPreparationConflictError(
            "prepared artifact family collides with different exact facts"
        ) from error


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
        "gallery.gallery_key, gallery_coordinate.scope_key, member.observation_id, "
        "observation.observation_identity_sha256, "
        "input.artifact_semantics_sha256, operation.operation, "
        "semantics.source_manifest_component_sha256, "
        "semantic_member.member_plan_component_sha256, "
        "semantic_effective.effective_content_component_sha256, "
        "semantic_selected.selected_component_sha256, "
        "semantic_owner.owner_component_sha256, "
        "semantic_policy.policy_component_sha256 "
        "FROM catalog_publication_selections AS selection "
        "JOIN catalog_publication_identities AS pub "
        "ON pub.publication_key = selection.publication_key "
        "JOIN catalog_gallery_identity_seals AS gallery_seal "
        "ON gallery_seal.gallery_id = selection.gallery_id "
        "JOIN catalog_gallery_identity_gallery_keys AS gallery "
        "ON gallery.gallery_id = gallery_seal.gallery_id "
        "JOIN catalog_gallery_identity_coordinates AS gallery_coordinate "
        "ON gallery_coordinate.gallery_id = gallery_seal.gallery_id "
        "JOIN catalog_source_build_galleries AS member "
        "ON member.build_id = %s AND member.gallery_id = selection.gallery_id "
        "JOIN catalog_gallery_observations AS observation "
        "ON observation.gallery_id = member.gallery_id "
        "AND observation.observation_id = member.observation_id "
        "JOIN catalog_candidate_artifact_inputs AS input "
        "ON input.candidate_id = selection.candidate_id "
        "AND input.publication_key = selection.publication_key "
        "JOIN catalog_artifact_semantic_input_seals AS semantic_seal "
        "ON semantic_seal.artifact_semantics_sha256 = input.artifact_semantics_sha256 "
        "JOIN catalog_artifact_semantic_source_manifest_sha256s AS semantics "
        "ON semantics.artifact_semantics_sha256 = "
        "semantic_seal.artifact_semantics_sha256 "
        "JOIN catalog_artifact_semantic_member_plan_sha256s AS semantic_member "
        "ON semantic_member.artifact_semantics_sha256 = "
        "semantic_seal.artifact_semantics_sha256 "
        "JOIN catalog_artifact_semantic_effective_content_sha256s AS semantic_effective "
        "ON semantic_effective.artifact_semantics_sha256 = "
        "semantic_seal.artifact_semantics_sha256 "
        "JOIN catalog_artifact_semantic_selected_sha256s AS semantic_selected "
        "ON semantic_selected.artifact_semantics_sha256 = "
        "semantic_seal.artifact_semantics_sha256 "
        "JOIN catalog_artifact_semantic_owner_sha256s AS semantic_owner "
        "ON semantic_owner.artifact_semantics_sha256 = "
        "semantic_seal.artifact_semantics_sha256 "
        "JOIN catalog_artifact_semantic_policy_sha256s AS semantic_policy "
        "ON semantic_policy.artifact_semantics_sha256 = "
        "semantic_seal.artifact_semantics_sha256 "
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
        contract.max_image_short_side,
        contract.producer_fingerprint_sha256,
        contract.producer_fields,
        contract.writer_policy,
        contract.storage_codec,
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
        "JOIN catalog_gallery_identity_seals AS owner_gallery_seal "
        "ON owner_gallery_seal.gallery_id = owner.owner_gallery_id "
        "JOIN catalog_gallery_identity_gallery_keys AS owner_gallery "
        "ON owner_gallery.gallery_id = owner_gallery_seal.gallery_id "
        "JOIN catalog_analysis_gid_winner_resolved AS winner "
        "ON winner.analysis_id = content.analysis_id AND winner.gid = %s "
        "JOIN catalog_gallery_identity_seals AS winner_gallery_seal "
        "ON winner_gallery_seal.gallery_id = winner.winner_gallery_id "
        "JOIN catalog_gallery_identity_gallery_keys AS winner_gallery "
        "ON winner_gallery.gallery_id = winner_gallery_seal.gallery_id "
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
            facts.max_image_short_side,
            facts.producer_fingerprint_sha256,
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
            b"artifact_policy_v2",
            identity.encode_artifact_policy(
                authority.artifact_algorithm_version,
                authority.max_image_short_side,
                authority.producer_fingerprint_sha256,
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
    member_payload = TemporaryFile(mode="w+b")
    effective_payload = TemporaryFile(mode="w+b")
    plan = sqlite3.connect(":memory:")
    try:
        plan.executescript(
            "CREATE TABLE archive_names (name BLOB PRIMARY KEY) WITHOUT ROWID;"
            "CREATE TABLE effective_files ("
            "file_sha256 BLOB NOT NULL, entry_position INTEGER NOT NULL, "
            "PRIMARY KEY (file_sha256, entry_position)) WITHOUT ROWID;"
        )
        member_receipt = CanonicalValueRepository.stream_and_validate(
            work,
            value_sha256=authority.member_plan_component_sha256,
            consume_provisional=lambda part: _write_all(member_payload, part),
        )
        if member_receipt.digest_domain != b"artifact_member_plan_v1":
            raise ArtifactPreparationConflictError(
                "member plan has the wrong canonical domain"
            )
        member_payload.seek(0)
        header = _read_exact(member_payload, len(_MEMBER_PLAN_PREFIX))
        if header != _MEMBER_PLAN_PREFIX:
            raise ArtifactPreparationConflictError("member plan prefix differs")
        if int.from_bytes(_read_exact(member_payload, 4), "big") != 1:
            raise ArtifactPreparationConflictError(
                "member plan version is not registered"
            )
        entry_count = int.from_bytes(_read_exact(member_payload, 8), "big")
        require_int63(entry_count, field="member plan entry_count")
        source_count, emitted_count, source_bytes = _compare_member_entries(
            work,
            authority,
            member_payload,
            plan,
            entry_count=entry_count,
        )
        if member_payload.read(1):
            raise ArtifactPreparationConflictError(
                "member plan contains trailing bytes"
            )
        effective_count = _effective_file_count(plan)
        expected_parts = identity.iter_artifact_effective_content_payload_ordered(
            effective_count,
            _iter_effective_file_digests(plan),
        )
        expected_digest = identity.artifact_effective_content_digest_ordered(
            effective_count,
            _iter_effective_file_digests(plan),
        )
        if expected_digest != authority.effective_content_component_sha256:
            raise ArtifactPreparationConflictError(
                "effective-content digest differs from member/source facts"
            )
        effective_receipt = CanonicalValueRepository.stream_and_validate(
            work,
            value_sha256=authority.effective_content_component_sha256,
            consume_provisional=lambda part: _write_all(effective_payload, part),
        )
        if effective_receipt.digest_domain != b"artifact_effective_content_v1":
            raise ArtifactPreparationConflictError(
                "effective content has the wrong canonical domain"
            )
        effective_payload.seek(0)
        for part in expected_parts:
            if _read_exact(effective_payload, len(part)) != part:
                raise ArtifactPreparationConflictError(
                    "effective-content canonical bytes differ from source facts"
                )
        if effective_payload.read(1):
            raise ArtifactPreparationConflictError(
                "effective-content canonical payload has trailing bytes"
            )
        return ArtifactPreparationInputAudit(
            authority,
            source_count,
            emitted_count,
            source_bytes,
            effective_count,
            identity.encode_zip_comment(
                authority.source_manifest_component_sha256,
                authority.effective_content_component_sha256,
            ),
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
    finally:
        plan.close()
        member_payload.close()
        effective_payload.close()


def _compare_member_entries(
    work: VNextUnitOfWork,
    authority: ArtifactPreparationAuthority,
    payload: BinaryIO,
    plan: sqlite3.Connection,
    *,
    entry_count: int,
) -> tuple[int, int, int]:
    position = 0
    emitted = 0
    total_bytes = 0
    last_file_no = -1
    while True:
        rows = _source_file_page(work, authority, after_file_no=last_file_no)
        if not rows:
            break
        for row in rows:
            if position >= entry_count:
                raise ArtifactPreparationConflictError(
                    "member plan omits immutable source files"
                )
            file_no = require_int63(row[0], field="artifact source file_no")
            if file_no != position:
                raise ArtifactPreparationConflictError(
                    "artifact source file positions are not zero-based contiguous"
                )
            entry = _read_member_entry(payload, expected_position=position)
            name = require_bounded_bytes(
                row[1],
                field="artifact source name",
                minimum=1,
                maximum=255,
            )
            role = require_bounded_bytes(
                row[2],
                field="artifact source role",
                minimum=7,
                maximum=8,
            )
            digest = require_digest32(row[3], field="artifact source file_sha256")
            size = require_int63(row[4], field="artifact source size")
            excluded = _source_file_excluded(row, authority, role=role)
            if (
                entry.source_name_bytes != name
                or entry.source_file_sha256 != digest
                or entry.source_size_bytes != size
                or bytes(entry.source_role.name, "ascii") != role
                or entry.excluded_flag is not excluded
            ):
                raise ArtifactPreparationConflictError(
                    "member plan differs from immutable source file facts"
                )
            archive_name = entry.archive_member_name_bytes
            if archive_name is not None:
                try:
                    plan.execute(
                        "INSERT INTO archive_names (name) VALUES (?)",
                        (sqlite3.Binary(archive_name),),
                    )
                except sqlite3.IntegrityError as error:
                    raise ArtifactPreparationConflictError(
                        "member plan repeats an exact archive member name"
                    ) from error
                emitted += 1
            if role == b"CONTENT" and not excluded:
                plan.execute(
                    "INSERT INTO effective_files (file_sha256, entry_position) "
                    "VALUES (?, ?)",
                    (sqlite3.Binary(digest), position),
                )
            if total_bytes > INT63_MAX - size:
                raise ArtifactPreparationNotReadyError(
                    "artifact source byte count is exhausted"
                )
            total_bytes += size
            last_file_no = file_no
            position += 1
        if len(rows) < _MAX_SOURCE_PAGE:
            break
    if position != entry_count:
        raise ArtifactPreparationConflictError(
            "member plan contains rows outside the immutable source snapshot"
        )
    return position, emitted, total_bytes


def _source_file_page(
    work: VNextUnitOfWork,
    authority: ArtifactPreparationAuthority,
    *,
    after_file_no: int,
) -> tuple[tuple[Any, ...], ...]:
    rows = work.connector.fetch_all(
        "SELECT source.file_no, name.name_bytes, name.file_role, "
        "source.file_sha256, blob.size_bytes, decision.occurrence_count, "
        "decision.artist_count, decision.maximum_gallery_artist_count "
        + _SOURCE_FILE_FAMILY_SQL
        + "JOIN catalog_content_blobs AS blob "
        "ON blob.file_sha256 = source.file_sha256 "
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


def _source_file_excluded(
    row: tuple[Any, ...],
    authority: ArtifactPreparationAuthority,
    *,
    role: bytes,
) -> bool:
    if role == b"METADATA":
        if any(value is not None for value in row[5:8]):
            raise ArtifactPreparationConflictError(
                "metadata file unexpectedly participates in spam decisions"
            )
        return False
    if role != b"CONTENT":
        raise ArtifactPreparationConflictError("source file role is not registered")
    if all(value is None for value in row[5:8]):
        return False
    if any(value is None for value in row[5:8]):
        raise ArtifactPreparationConflictError("source file decision is partial")
    occurrence = require_positive_int63(row[5], field="artifact occurrence_count")
    artist = require_int63(row[6], field="artifact artist_count")
    maximum = require_int63(
        row[7],
        field="artifact maximum_gallery_artist_count",
    )
    return (
        occurrence >= authority.spam_occurrence_threshold
        and maximum > 0
        and artist > authority.spam_artist_threshold * maximum
    )


def _read_member_entry(
    payload: BinaryIO,
    *,
    expected_position: int,
) -> identity.ArtifactMemberPlanEntry:
    position = int.from_bytes(_read_exact(payload, 8), "big")
    if position != expected_position:
        raise ArtifactPreparationConflictError(
            "member plan positions are not zero-based contiguous"
        )
    if _read_exact(payload, 1) != b"\0":
        raise ArtifactPreparationConflictError(
            "member plan entry kind is not SOURCE_FILE"
        )
    source_name_length = int.from_bytes(_read_exact(payload, 4), "big")
    if not 1 <= source_name_length <= 255:
        raise ArtifactPreparationConflictError(
            "member plan source name exceeds the closed file-name domain"
        )
    source_name = _read_exact(payload, source_name_length)
    source_digest = _read_exact(payload, 32)
    source_size = int.from_bytes(_read_exact(payload, 8), "big")
    require_int63(source_size, field="member plan source_size_bytes")
    source_role = int.from_bytes(_read_exact(payload, 1), "big")
    excluded_tag = int.from_bytes(_read_exact(payload, 1), "big")
    presence = int.from_bytes(_read_exact(payload, 1), "big")
    if excluded_tag not in {0, 1} or presence not in {0, 1}:
        raise ArtifactPreparationConflictError(
            "member plan boolean tag is not registered"
        )
    archive_name: bytes | None = None
    if presence:
        archive_size = int.from_bytes(_read_exact(payload, 4), "big")
        if not 1 <= archive_size <= _ZIP_MEMBER_NAME_MAXIMUM_BYTES:
            raise ArtifactPreparationConflictError(
                "archive member name exceeds the ZIP filename bound"
            )
        archive_name = _read_exact(payload, archive_size)
    transform = int.from_bytes(_read_exact(payload, 1), "big")
    try:
        entry = identity.ArtifactMemberPlanEntry(
            entry_position=position,
            source_name_bytes=source_name,
            source_file_sha256=source_digest,
            source_size_bytes=source_size,
            excluded_flag=bool(excluded_tag),
        )
    except (identity.ByteDomainError, ValueError) as error:
        raise ArtifactPreparationConflictError(
            "member plan entry violates its exact domain"
        ) from error
    if int(entry.source_role) != source_role or int(entry.transform_kind) != transform:
        raise ArtifactPreparationConflictError(
            "member plan derived role or transform tag disagrees"
        )
    if entry.archive_member_name_bytes != archive_name:
        raise ArtifactPreparationConflictError(
            "member plan archive name differs from the server derivation"
        )
    return entry


def _effective_file_count(plan: sqlite3.Connection) -> int:
    count = 0
    cursor = plan.execute(
        "SELECT file_sha256 FROM effective_files ORDER BY file_sha256, entry_position"
    )
    while rows := cursor.fetchmany(_MAX_SOURCE_PAGE):
        count += len(rows)
        if count > INT63_MAX:
            raise ArtifactPreparationNotReadyError(
                "effective-content file count is exhausted"
            )
    return count


def _iter_effective_file_digests(plan: sqlite3.Connection) -> Iterator[bytes]:
    cursor = plan.execute(
        "SELECT file_sha256 FROM effective_files ORDER BY file_sha256, entry_position"
    )
    while rows := cursor.fetchmany(_MAX_SOURCE_PAGE):
        for (value,) in rows:
            yield require_digest32(value, field="effective-content file_sha256")


def _load_validation_checkpoint(
    work: VNextUnitOfWork,
    candidate_id: bytes,
    *,
    now: int | None,
) -> tuple[int, bytes, int, str, int]:
    predecessor = work.connector.fetch_one(
        "SELECT state.state "
        f"FROM {_CHECKPOINT_STATE_TABLE} AS state "
        f"JOIN {_CHECKPOINT_SEAL_TABLE} AS seal "
        "ON seal.candidate_id = state.candidate_id AND seal.stage = state.stage "
        "WHERE state.candidate_id = %s AND state.stage = %s",
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
        "SELECT generation.generation, cursor.cursor, count.processed_count, "
        "state.state, updated.updated_at "
        f"FROM {_CHECKPOINT_SEAL_TABLE} AS seal "
        f"JOIN {_CHECKPOINT_GENERATION_TABLE} AS generation "
        "ON generation.candidate_id = seal.candidate_id "
        "AND generation.stage = seal.stage "
        f"JOIN {_CHECKPOINT_CURSOR_TABLE} AS cursor "
        "ON cursor.candidate_id = seal.candidate_id AND cursor.stage = seal.stage "
        f"JOIN {_CHECKPOINT_COUNT_TABLE} AS count "
        "ON count.candidate_id = seal.candidate_id AND count.stage = seal.stage "
        f"JOIN {_CHECKPOINT_STATE_TABLE} AS state "
        "ON state.candidate_id = seal.candidate_id AND state.stage = seal.stage "
        f"JOIN {_CHECKPOINT_UPDATED_AT_TABLE} AS updated "
        "ON updated.candidate_id = seal.candidate_id AND updated.stage = seal.stage "
        "WHERE seal.candidate_id = %s AND seal.stage = %s",
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
        authority.max_image_short_side,
        authority.producer_fingerprint_sha256,
        authority.producer_fields,
        authority.writer_policy,
        authority.storage_codec,
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


def _read_exact(stream: BinaryIO, size: int) -> bytes:
    if not 0 <= size <= _ZIP_MEMBER_NAME_MAXIMUM_BYTES:
        raise ArtifactPreparationConflictError("artifact input field is oversized")
    output = bytearray()
    while len(output) < size:
        chunk = stream.read(min(size - len(output), _MAX_READ_BYTES))
        if not chunk:
            raise ArtifactPreparationConflictError("artifact input is truncated")
        output.extend(chunk)
    return bytes(output)


def _write_all(stream: BinaryIO, part: bytes) -> None:
    if stream.write(part) != len(part):
        raise OSError("artifact spool accepted a partial write")
