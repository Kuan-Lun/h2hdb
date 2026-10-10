"""Immutable cleanup capabilities and transaction-local operation values."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum

from h2hdb._cleanup.eligibility import CurrentOnlyEligibilityProof
from h2hdb.domain import CurrentOnlyCleanupTerminalState
from h2hdb.vnext_domains import (
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

_StaticScalar = bytes | int | str

_CLEANUP_ID_DOMAIN = b"h2hdb-cleanup-cycle-v1\0"

_TARGET_KEY_DOMAIN = b"h2hdb-cleanup-target-v1\0"

_MAX_BATCH_ROWS = 256

_CLEANUP_ALGORITHM_VERSION = 2

_EMPTY_CURSOR = b""

_SWEEP_TABLE = "operational_cleanup_sweep_targets"

_PHASE_TABLE = "operational_cleanup_phases"

_JOB_TABLE = "operational_cleanup_jobs"

_CHECKPOINT_TABLE = "operational_cleanup_checkpoints"

_FROZEN_ROOT_TABLE = "operational_cleanup_cycle_roots"


class CleanupTargetKind(StrEnum):
    SOURCE_COLLECTION = "SOURCE_COLLECTION"
    SOURCE_BUILD = "SOURCE_BUILD"
    ANALYSIS_RUN = "ANALYSIS_RUN"
    CATALOG_PUBLICATION = "CATALOG_PUBLICATION"
    PUBLICATION_COMMIT = "PUBLICATION_COMMIT"
    CATALOG_REVISION_DESCRIPTOR = "CATALOG_REVISION_DESCRIPTOR"
    SOURCE_REVISION_DESCRIPTOR = "SOURCE_REVISION_DESCRIPTOR"
    PUBLICATION_GENERATION = "PUBLICATION_GENERATION"
    PUBLICATION_CANDIDATE = "PUBLICATION_CANDIDATE"
    OPERATIONAL_PREPARATION = "OPERATIONAL_PREPARATION"
    GALLERY_OBSERVATION = "GALLERY_OBSERVATION"
    GALLERY_OBSERVATION_STAGING = "GALLERY_OBSERVATION_STAGING"
    ARTIFACT_BLOB = "ARTIFACT_BLOB"
    STORAGE_OBJECT_KEY = "STORAGE_OBJECT_KEY"
    CANONICAL_VALUE = "CANONICAL_VALUE"
    CONTENT_BLOB = "CONTENT_BLOB"
    GALLERY_OBSERVATION_PAGE = "GALLERY_OBSERVATION_PAGE"
    FILE_NAME_IDENTITY = "FILE_NAME_IDENTITY"
    PUBLICATION_IDENTITY = "PUBLICATION_IDENTITY"
    GALLERY_IDENTITY = "GALLERY_IDENTITY"
    SOURCE_GALLERY_NAME_GID = "SOURCE_GALLERY_NAME_GID"
    GALLERY_GID_IDENTITY = "GALLERY_GID_IDENTITY"
    CANONICAL_VALUE_UPLOAD = "CANONICAL_VALUE_UPLOAD"
    HASH_CACHE_OBSERVATION = "HASH_CACHE_OBSERVATION"


class CatalogPublicationMaintenanceState(StrEnum):
    """Optimistic current-only state used to avoid gate writes on idle polls."""

    DONE = "DONE"
    BLOCKED = "BLOCKED"
    ACTIONABLE = "ACTIONABLE"


class CleanupUnavailableError(RuntimeError):
    """A cleanup authority is stale, incomplete, or not installed."""


class CleanupCorruptionError(RuntimeError):
    """Durable cleanup control rows do not refine the closed contract."""


class CleanupRetentionBlockedError(CleanupUnavailableError):
    """A retention root appeared while a destructive batch was in flight."""


class CleanupCycleExhaustedError(OverflowError):
    """The fixed shard cycle generation cannot advance within int63."""


@dataclass(frozen=True, slots=True)
class CleanupCycle:
    cleanup_id: bytes
    target_kind: CleanupTargetKind
    shard_no: int
    target_key: bytes
    cycle_generation: int
    cycle_cutoff_at: int
    max_rows_per_transaction: int
    hash_cache_max_age_microseconds: int

    def __post_init__(self) -> None:
        require_uuid16(self.cleanup_id, field="cleanup_id")
        if not isinstance(self.target_kind, CleanupTargetKind):
            raise TypeError("target_kind must be a CleanupTargetKind")
        _require_shard(self.shard_no)
        require_digest32(self.target_key, field="cleanup target_key")
        require_positive_int63(self.cycle_generation, field="cleanup cycle_generation")
        require_int63(self.cycle_cutoff_at, field="cleanup cycle_cutoff_at")
        _require_batch_bound(self.max_rows_per_transaction)
        require_int63(
            self.hash_cache_max_age_microseconds,
            field="cleanup hash_cache_max_age_microseconds",
        )
        if self.cleanup_id != _cleanup_id(
            self.target_kind, self.shard_no, self.cycle_generation
        ):
            raise CleanupCorruptionError("cleanup_id does not match its fixed shard")
        if self.target_key != _target_key(self.target_kind, self.shard_no):
            raise CleanupCorruptionError("target_key does not match its fixed shard")


@dataclass(frozen=True, slots=True)
class CleanupBatchCommand:
    batch_key: bytes
    expected_generation: int

    def __post_init__(self) -> None:
        require_digest32(self.batch_key, field="cleanup batch_key")
        require_positive_int63(
            self.expected_generation, field="cleanup expected_generation"
        )


@dataclass(frozen=True, slots=True)
class CurrentOnlyCleanupSelection:
    cycle: CleanupCycle | CurrentOnlyCleanupTerminalState
    eligibility_proof: CurrentOnlyEligibilityProof


@dataclass(frozen=True, slots=True)
class _Checkpoint:
    phase: str
    generation: int
    cursor: bytes
    deleted_count: int
    chain_sha256: bytes
    state: str
    receipt_batch_key: bytes | None
    receipt_row_count: int | None
    receipt_start_cursor: bytes | None
    receipt_prior_chain_sha256: bytes | None
    receipt_prior_deleted_count: int | None
    receipt_input_sha256: bytes | None


@dataclass(frozen=True, slots=True)
class _Mutation:
    next_cursor: bytes
    row_keys: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class _CleanupOperation:
    """Authority owned by one repository call under its locked transaction.

    No connector, unit of work, or facade retains this value. Each entry locks
    and validates durable authority afresh, including after rollback or restart.
    Frozen roots are immutable until completion. Empty static phases may share
    their absence proofs only while this call advances without deleting payload;
    the first nonempty mutation ends the call.
    """

    work: VNextUnitOfWork
    cycle: CleanupCycle
    initial_checkpoint: _Checkpoint | None
    complete: bool
    frozen_roots: tuple[tuple[_StaticScalar, ...], ...]
    empty_static_phases: set[str] = field(default_factory=set)


_Mutator = Callable[[_CleanupOperation, bytes], _Mutation]


@dataclass(frozen=True, slots=True)
class _Strategy:
    phases: tuple[str, ...]
    mutators: tuple[_Mutator, ...]

    def __post_init__(self) -> None:
        if not self.phases or len(self.phases) != len(self.mutators):
            raise RuntimeError("cleanup strategy phases and mutators disagree")


def _require_shard(value: object) -> int:
    shard = require_int63(value, field="cleanup shard_no")
    if shard > 255:
        raise ValueError("cleanup shard_no must be in 0..255")
    return shard


def _require_batch_bound(value: object) -> int:
    bound = require_positive_int63(value, field="cleanup max_rows_per_transaction")
    if bound > _MAX_BATCH_ROWS:
        raise ValueError(f"cleanup batches are capped at {_MAX_BATCH_ROWS} rows")
    return bound


def _cleanup_id(kind: CleanupTargetKind, shard_no: int, generation: int) -> bytes:
    tag = hashlib.sha256(_CLEANUP_ID_DOMAIN + kind.value.encode("ascii")).digest()[:7]
    return (
        tag
        + bytes((_require_shard(shard_no),))
        + require_positive_int63(generation, field="cleanup generation").to_bytes(
            8, "big"
        )
    )


def _target_key(kind: CleanupTargetKind, shard_no: int) -> bytes:
    tag = hashlib.sha256(_TARGET_KEY_DOMAIN + kind.value.encode("ascii")).digest()[:16]
    shard = _require_shard(shard_no)
    return tag + shard.to_bytes(8, "big") + bytes(8)


def _as_text(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise CleanupCorruptionError(f"{field} must be text")
    return value
