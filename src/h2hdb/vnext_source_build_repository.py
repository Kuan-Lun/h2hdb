"""Bounded source discovery and manifest assembly for vNext builds.

The filesystem adapter freezes an exact, disk-backed locator plan before any
database mutation.  Database batches are capped at 256 rows, carry
repository-issued capabilities, persist their full pre/post state, and move
checkpoints only by compare-and-swap.  Digests, counts, cursors, positions,
surrogate IDs, and batch keys are never accepted as caller authority.
"""

from __future__ import annotations

__all__ = [
    "AssemblyBatchAttempt",
    "AssemblyBatchReceipt",
    "DiscoveryBatch",
    "DiscoveryBatchReceipt",
    "PreparedDiscoveryLocator",
    "ResolvedDiscoveryLocator",
    "SourceBuildConflictError",
    "SourceBuildAbandonment",
    "SourceBuildHandoff",
    "SourceBuildNotReadyError",
    "SourceBuildRepository",
    "SourceDiscoveryPlan",
    "SourceDiscoveryPlanError",
    "SourceRootBuildCommand",
]

import secrets
import sqlite3
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from .vnext_allocator_repository import IdentityStream, VNextAllocatorRepository
from .vnext_canonical_value_family import load_sealed_value_identity
from .vnext_canonical_value_repository import (
    CanonicalValueUploadPlan,
    _authorize,
)
from .vnext_catalog_identity_family import (
    CatalogIdentityCollisionError,
    GalleryIdentity,
    ensure_gallery_identity,
    load_gallery_identities,
    load_gallery_identity_candidates,
)
from .vnext_catalog_registry_repository import (
    CatalogRegistryConflictError,
    CatalogRegistryNotReadyError,
    ensure_source_scope,
    load_manifest_policy_by_natural,
    load_source_scope,
)
from .vnext_domains import (
    INT63_MAX,
    require_ascii_bytes,
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from .vnext_identity import (
    SOURCE_ROOT_DIGEST_DOMAIN,
    canonical_value_digest_parts,
    gallery_key,
    iter_source_relative_locator_payload,
    iter_source_root_payload,
    source_root_digest,
    source_scope_key,
    validate_source_relative_locator_parts,
    validate_source_root_parts,
)
from .vnext_ingest_fence_repository import IngestTurn
from .vnext_maintenance_gate_repository import GateLease
from .vnext_manifest_family import (
    ManifestFamilyCollisionError,
    SourceBuildFamily,
    database_unix_microseconds,
    ensure_build_manifest_family,
    ensure_source_build_family,
    load_build_manifest_family,
    load_source_build_family,
)
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_FILESYSTEM = "filesystem"
_FILESYSTEM_BYTES = b"filesystem"
_DEFAULT_CHANNEL = b"default"
_IDENTITY_POLICY_VERSION = 1
_MANIFEST_ALGORITHM_VERSION = 1
_FILE_ORDER_VERSION = 1
_LOCATOR_DOMAIN = "source_relative_locator_v1"
_LOCATOR_DOMAIN_BYTES = _LOCATOR_DOMAIN.encode("ascii")
_BATCH_LIMIT = 256
_STREAM_BYTES = 64 * 1024
_DISCOVERY_AUDIT_PREFIX = b"h2hdb-vnext-source-build-discovery-audit-v1\0"
_DISCOVERY_BATCH_PREFIX = b"h2hdb-vnext-source-build-discovery-batch-v1\0"
_MANIFEST_AUDIT_PREFIX = b"h2hdb-vnext-source-build-manifest-audit-v1\0"
_EMPTY_MANIFEST_CHAIN = bytes.fromhex(
    "121f20d26c10f4c5ce6e621dc5e41b7da2c4028af840caa7547265068f2458e3"
)
_PLAN_CONSTRUCTOR_TOKEN = object()
_DISCOVERY_BATCH_TOKEN = object()
_ASSEMBLY_ATTEMPT_TOKEN = object()

_PUBLICATION_COMMIT_HEAD_TABLE = "catalog_publication_commit_head_receipts"
_PUBLICATION_COMMIT_SEAL_TABLE = "catalog_publication_commit_seals"
_PUBLICATION_COMMIT_SOURCE_TABLE = "catalog_publication_commit_source_revisions"
_PUBLICATION_COMMIT_GENERATION_TABLE = "catalog_publication_commit_generations"
_PUBLICATION_COMMIT_COMMITTED_AT_TABLE = "catalog_publication_commit_committed_ats"
_SOURCE_REVISION_CHANNEL_TABLE = "catalog_source_revision_channels"
_SOURCE_BUILD_BASE_COMMIT_TABLE = "catalog_source_build_base_publication_commits"


class SourceBuildConflictError(RuntimeError):
    """An immutable source scope, build, generation, or working slot differs."""


class SourceBuildNotReadyError(RuntimeError):
    """The root seal, policy, claim, or exact live authority is absent."""


class SourceDiscoveryPlanError(ValueError):
    """A filesystem discovery stream is malformed, duplicated, or changed."""


def _new_scan_attempt() -> bytes:
    return secrets.token_bytes(16)


def _new_assembly_batch_key() -> bytes:
    return secrets.token_bytes(32)


@dataclass(frozen=True, slots=True)
class PreparedDiscoveryLocator:
    """One fixed-width locator descriptor issued by a discovery plan."""

    position: int
    locator_sha256: bytes
    payload_byte_count: int
    payload_sha256: bytes
    source_gallery_name: bytes
    _plan_capability: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        require_int63(self.position, field="locator position")
        require_digest32(self.locator_sha256, field="locator_sha256")
        require_int63(self.payload_byte_count, field="locator payload_byte_count")
        require_digest32(self.payload_sha256, field="locator payload_sha256")
        require_bounded_bytes(
            self.source_gallery_name,
            field="source_gallery_name",
            minimum=1,
            maximum=255,
        )


@dataclass(frozen=True, slots=True)
class DiscoveryBatch:
    """Repository-issued checkpoint pre-state plus at most 256 locators."""

    build_id: bytes
    batch_key: bytes
    scan_attempt: bytes
    gallery_count: int
    tree_observation_sha256: bytes
    start_generation: int
    start_cursor: bytes
    start_processed_count: int
    locators: tuple[PreparedDiscoveryLocator, ...]
    terminal: bool
    _plan_capability: object = field(repr=False, compare=False)
    _batch_capability: object = field(repr=False, compare=False)
    _constructor_token: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        require_uuid16(self.build_id, field="build_id")
        require_digest32(self.batch_key, field="batch_key")
        require_uuid16(self.scan_attempt, field="scan_attempt")
        require_int63(self.gallery_count, field="gallery_count")
        require_digest32(
            self.tree_observation_sha256,
            field="tree_observation_sha256",
        )
        require_int63(self.start_generation, field="start_generation")
        require_bounded_bytes(self.start_cursor, field="start_cursor", maximum=8)
        require_int63(self.start_processed_count, field="start_processed_count")
        if type(self.locators) is not tuple or any(
            type(locator) is not PreparedDiscoveryLocator for locator in self.locators
        ):
            raise TypeError(
                "discovery locators must be an exact tuple of prepared locators"
            )
        for locator in self.locators:
            locator.__post_init__()
        if len(self.locators) > _BATCH_LIMIT:
            raise ValueError("discovery batch exceeds 256 locators")
        if type(self.terminal) is not bool:
            raise TypeError("discovery terminal must be bool")
        if self.terminal != (not self.locators):
            raise ValueError("only an empty discovery page is terminal")
        if self._constructor_token is not _DISCOVERY_BATCH_TOKEN:
            raise TypeError("use SourceBuildRepository.prepare_discovery_batch")


@dataclass(frozen=True, slots=True)
class ResolvedDiscoveryLocator:
    """Durable identity evidence issued by one short locator handoff tx."""

    build_id: bytes
    batch_key: bytes
    position: int
    locator_sha256: bytes
    gallery_id: int
    gallery_key: bytes
    replayed: bool
    _batch_capability: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        require_uuid16(self.build_id, field="build_id")
        require_digest32(self.batch_key, field="batch_key")
        require_int63(self.position, field="position")
        require_digest32(self.locator_sha256, field="locator_sha256")
        require_positive_int63(self.gallery_id, field="gallery_id")
        require_digest32(self.gallery_key, field="gallery_key")
        if not isinstance(self.replayed, bool):
            raise TypeError("replayed must be bool")


@dataclass(frozen=True, slots=True)
class DiscoveryBatchReceipt:
    build_id: bytes
    batch_key: bytes
    start_generation: int
    start_cursor: bytes
    start_processed_count: int
    next_cursor: bytes
    next_processed_count: int
    next_state: str
    row_count: int
    terminal: bool
    committed_generation: int
    committed_at: int
    replayed: bool


@dataclass(frozen=True, slots=True)
class AssemblyBatchAttempt:
    """Opaque response-loss token; it contains no caller-provided cursor."""

    batch_key: bytes
    _constructor_token: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        require_digest32(self.batch_key, field="batch_key")
        if self._constructor_token is not _ASSEMBLY_ATTEMPT_TOKEN:
            raise TypeError("use SourceBuildRepository.issue_assembly_batch")


@dataclass(frozen=True, slots=True)
class AssemblyBatchReceipt:
    build_id: bytes
    batch_key: bytes
    start_generation: int
    start_cursor: bytes
    start_gallery_count: int
    start_file_count: int
    start_byte_count: int
    start_manifest_chain_sha256: bytes
    next_cursor: bytes
    next_gallery_count: int
    next_file_count: int
    next_byte_count: int
    next_manifest_chain_sha256: bytes
    next_state: str
    row_count: int
    terminal: bool
    committed_generation: int
    committed_at: int
    replayed: bool


class SourceDiscoveryPlan:
    """Private disk-backed, digest-sorted snapshot of exact nested locators."""

    def __init__(
        self,
        *,
        temporary: TemporaryDirectory[str],
        index: sqlite3.Connection,
        scan_attempt: bytes,
        gallery_count: int,
        tree_observation_sha256: bytes,
        _constructor_token: object,
    ) -> None:
        if _constructor_token is not _PLAN_CONSTRUCTOR_TOKEN:
            raise TypeError("use SourceDiscoveryPlan.from_locators")
        self._temporary = temporary
        self._index = index
        self._directory = Path(temporary.name)
        self._capability = object()
        self._closed = False
        self.scan_attempt = require_uuid16(scan_attempt, field="scan_attempt")
        self.gallery_count = require_int63(gallery_count, field="gallery_count")
        self.tree_observation_sha256 = require_digest32(
            tree_observation_sha256,
            field="tree_observation_sha256",
        )

    @classmethod
    def from_locators(
        cls,
        locators: Iterable[tuple[str, ...]],
    ) -> SourceDiscoveryPlan:
        if isinstance(locators, (str, bytes)):
            raise TypeError("locators must be an iterable of exact tuples")
        temporary = TemporaryDirectory(prefix="h2hdb-source-discovery-")
        directory = Path(temporary.name)
        index = sqlite3.connect(directory / "index.sqlite3")
        try:
            index.execute(
                "CREATE TABLE locator_entries ("
                "locator_sha256 BLOB PRIMARY KEY, position INTEGER UNIQUE, "
                "payload_name TEXT UNIQUE NOT NULL, payload_byte_count INTEGER NOT NULL, "
                "payload_sha256 BLOB NOT NULL, source_gallery_name BLOB NOT NULL)"
            )
            count = 0
            for components in locators:
                if not isinstance(components, tuple):
                    raise TypeError("each source locator must be an exact tuple")
                if count == INT63_MAX:
                    raise SourceDiscoveryPlanError("too many source locators")
                payload_name = f"locator-{count:016x}.bin"
                payload_path = directory / payload_name
                payload_digest = sha256()
                byte_count = 0
                with payload_path.open("wb") as payload:
                    for part in iter_source_relative_locator_payload(components):
                        exact = require_bounded_bytes(
                            part,
                            field="source locator part",
                            maximum=INT63_MAX,
                        )
                        byte_count = _checked_add(
                            byte_count,
                            len(exact),
                            field="locator payload bytes",
                        )
                        payload_digest.update(exact)
                        if payload.write(exact) != len(exact):
                            raise OSError("locator spool accepted a partial write")
                receipt = validate_source_relative_locator_parts(
                    _read_file_parts(payload_path)
                )
                if (
                    receipt.payload_byte_count != byte_count
                    or receipt.payload_sha256 != payload_digest.digest()
                ):
                    raise SourceDiscoveryPlanError(
                        "locator spool changed during validation"
                    )
                locator_sha256 = canonical_value_digest_parts(
                    _LOCATOR_DOMAIN,
                    byte_count,
                    _read_file_parts(payload_path),
                )
                leaf = components[-1].encode("utf-8", errors="strict")
                duplicate = index.execute(
                    "SELECT payload_name FROM locator_entries WHERE locator_sha256 = ?",
                    (locator_sha256,),
                ).fetchone()
                if duplicate is not None:
                    other = directory / str(duplicate[0])
                    same = _files_equal(payload_path, other)
                    payload_path.unlink()
                    if same:
                        raise SourceDiscoveryPlanError("duplicate source locator bytes")
                    raise SourceDiscoveryPlanError("source locator digest collision")
                index.execute(
                    "INSERT INTO locator_entries "
                    "(locator_sha256, position, payload_name, payload_byte_count, "
                    "payload_sha256, source_gallery_name) VALUES (?, NULL, ?, ?, ?, ?)",
                    (
                        locator_sha256,
                        payload_name,
                        byte_count,
                        receipt.payload_sha256,
                        leaf,
                    ),
                )
                count += 1
            index.commit()

            # A second connection keeps only one fixed-width row in memory
            # while assigning the unsigned BLOB order to durable positions.
            reader = sqlite3.connect(directory / "index.sqlite3")
            audit = sha256(_DISCOVERY_AUDIT_PREFIX)
            audit.update(count.to_bytes(8, "big"))
            try:
                rows = reader.execute(
                    "SELECT locator_sha256 FROM locator_entries ORDER BY locator_sha256"
                )
                for position, row in enumerate(rows):
                    digest = require_digest32(row[0], field="locator_sha256")
                    audit.update(digest)
                    index.execute(
                        "UPDATE locator_entries SET position = ? "
                        "WHERE locator_sha256 = ? AND position IS NULL",
                        (position, digest),
                    )
            finally:
                reader.close()
            index.commit()
            return cls(
                temporary=temporary,
                index=index,
                scan_attempt=_new_scan_attempt(),
                gallery_count=count,
                tree_observation_sha256=audit.digest(),
                _constructor_token=_PLAN_CONSTRUCTOR_TOKEN,
            )
        except BaseException:
            index.close()
            temporary.cleanup()
            raise

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._index.close()
            self._temporary.cleanup()

    def __enter__(self) -> SourceDiscoveryPlan:
        self._require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def prepare_locator_upload(
        self,
        locator: PreparedDiscoveryLocator,
    ) -> CanonicalValueUploadPlan:
        self._require_locator(locator)
        row = self._index.execute(
            "SELECT payload_name, payload_byte_count, payload_sha256, "
            "source_gallery_name FROM locator_entries WHERE position = ?",
            (locator.position,),
        ).fetchone()
        expected = (
            locator.payload_byte_count,
            locator.payload_sha256,
            locator.source_gallery_name,
        )
        if row is None or row[1:] != expected:
            raise SourceDiscoveryPlanError("locator plan index changed")
        plan = CanonicalValueUploadPlan.from_parts(
            _LOCATOR_DOMAIN,
            _read_file_parts(self._directory / str(row[0])),
        )
        if (
            plan.value_sha256 != locator.locator_sha256
            or plan.byte_count != locator.payload_byte_count
        ):
            plan.close()
            raise SourceDiscoveryPlanError("locator payload changed while replaying")
        plan._source_discovery_capability = (  # type: ignore[attr-defined]
            self._capability,
            locator.position,
            locator.payload_sha256,
        )
        return plan

    def _page(self, start_position: int) -> tuple[PreparedDiscoveryLocator, ...]:
        self._require_open()
        start = require_int63(start_position, field="start_position")
        rows = self._index.execute(
            "SELECT position, locator_sha256, payload_byte_count, payload_sha256, "
            "source_gallery_name FROM locator_entries WHERE position >= ? "
            "ORDER BY position LIMIT ?",
            (start, _BATCH_LIMIT),
        ).fetchall()
        return tuple(
            PreparedDiscoveryLocator(
                require_int63(row[0], field="locator position"),
                require_digest32(row[1], field="locator_sha256"),
                require_int63(row[2], field="locator payload_byte_count"),
                require_digest32(row[3], field="locator payload_sha256"),
                require_bounded_bytes(
                    row[4],
                    field="source_gallery_name",
                    minimum=1,
                    maximum=255,
                ),
                self._capability,
            )
            for row in rows
        )

    def _require_locator(self, locator: PreparedDiscoveryLocator) -> None:
        self._require_open()
        if type(locator) is not PreparedDiscoveryLocator:
            raise TypeError("locator must be an exact PreparedDiscoveryLocator")
        locator.__post_init__()
        if locator._plan_capability is not self._capability:
            raise SourceDiscoveryPlanError("locator belongs to another discovery plan")

    def _require_open(self) -> None:
        if self._closed:
            raise ValueError("source discovery plan is closed")


@dataclass(frozen=True, slots=True)
class SourceRootBuildCommand:
    """Public source-root command; scope keys are deliberately absent."""

    source_root_components: tuple[str, ...]
    build_attempt_id: bytes
    source_root_sha256: bytes = field(init=False)
    source_root_byte_count: int = field(init=False)
    source_root_payload_sha256: bytes = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.source_root_components, tuple):
            raise ValueError("source_root_components must be an exact tuple")
        # Fully consume the codec now so malformed Unicode/root segments never
        # reach a transaction.
        root_receipt = validate_source_root_parts(
            iter_source_root_payload(self.source_root_components)
        )
        object.__setattr__(
            self,
            "source_root_byte_count",
            root_receipt.payload_byte_count,
        )
        object.__setattr__(
            self,
            "source_root_payload_sha256",
            root_receipt.payload_sha256,
        )
        object.__setattr__(
            self,
            "source_root_sha256",
            source_root_digest(self.source_root_components),
        )
        require_uuid16(self.build_attempt_id, field="build_attempt_id")

    def prepare_root_upload(self) -> CanonicalValueUploadPlan:
        return CanonicalValueUploadPlan.from_parts(
            SOURCE_ROOT_DIGEST_DOMAIN,
            iter_source_root_payload(self.source_root_components),
        )


@dataclass(frozen=True, slots=True)
class SourceBuildHandoff:
    build_id: bytes
    generation: int
    scope_key: bytes
    source_root_sha256: bytes
    manifest_policy_id: int
    replayed: bool

    def __post_init__(self) -> None:
        require_uuid16(self.build_id, field="build_id")
        require_int63(self.generation, field="generation")
        require_digest32(self.scope_key, field="scope_key")
        require_digest32(self.source_root_sha256, field="source_root_sha256")
        require_positive_int63(self.manifest_policy_id, field="manifest_policy_id")
        if not isinstance(self.replayed, bool):
            raise ValueError("replayed must be bool")


@dataclass(frozen=True, slots=True)
class SourceBuildAbandonment:
    build_id: bytes
    generation: int
    replayed: bool

    def __post_init__(self) -> None:
        require_uuid16(self.build_id, field="build_id")
        require_positive_int63(self.generation, field="generation")
        if not isinstance(self.replayed, bool):
            raise TypeError("replayed must be bool")


@dataclass(frozen=True, slots=True)
class _SourceHead:
    receipt_id: bytes
    revision: int
    generation: int
    committed_at: int


class SourceBuildRepository:
    """Transaction-local root consumer and source-build reservation writer."""

    @staticmethod
    def handoff_root(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        command: SourceRootBuildCommand,
        root_plan: CanonicalValueUploadPlan,
        now: int,
    ) -> SourceBuildHandoff:
        if type(command) is not SourceRootBuildCommand:
            raise TypeError("command must be an exact SourceRootBuildCommand")
        command.__post_init__()
        if type(root_plan) is not CanonicalValueUploadPlan:
            raise TypeError("root_plan must be an exact CanonicalValueUploadPlan")
        timestamp = require_int63(now, field="now")
        generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        connector = work.connector

        if root_plan.digest_domain != SOURCE_ROOT_DIGEST_DOMAIN.encode("ascii"):
            raise SourceBuildConflictError("root upload uses the wrong digest domain")
        expected_root = command.source_root_sha256
        if root_plan.value_sha256 != expected_root:
            raise SourceBuildConflictError(
                "root upload is not the command's exact canonical root"
            )
        validation = root_plan.source_root_receipt
        if (
            validation.component_count != len(command.source_root_components)
            or validation.payload_byte_count != command.source_root_byte_count
            or validation.payload_byte_count != root_plan.byte_count
            or validation.payload_sha256 != command.source_root_payload_sha256
        ):
            raise SourceBuildConflictError("root payload length changed")
        scope = source_scope_key(
            _FILESYSTEM,
            expected_root,
            _IDENTITY_POLICY_VERSION,
        )

        # One global working-root lock serializes reservation and response-loss
        # recovery.  It is acquired after gate and ingest authority and before
        # any catalog child or upload claim, matching the global lock DAG.
        working = work.lock_row(
            LockRank.WORKING_ROOT,
            encode_lock_key("source-working-build", 1),
            "SELECT slot, build_id, assigned_at "
            "FROM operational_source_working_builds WHERE slot = %s",
            (1,),
        )

        manifest_policy_id = _manifest_policy_id(connector)
        _require_registry_value(
            connector,
            table="catalog_source_provider_registry",
            column="source_provider",
            expected=_FILESYSTEM_BYTES,
        )
        _require_registry_value(
            connector,
            table="catalog_channel_registry",
            column="channel",
            expected=_DEFAULT_CHANNEL,
        )

        mapping = connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (generation,),
        )
        if mapping:
            mapped_build = require_uuid16(mapping[0], field="mapped build_id")
            _validate_existing_handoff(
                connector,
                build_id=mapped_build,
                generation=generation,
                scope=scope,
                source_root_sha256=expected_root,
                manifest_policy_id=manifest_policy_id,
                working=working,
            )
            _require_sealed_root_identity(
                connector,
                source_root_sha256=expected_root,
                byte_count=root_plan.byte_count,
                root_page_sha256=root_plan.root_page_sha256,
            )
            replay_claim = work.lock_row(
                LockRank.CHECKPOINT,
                encode_lock_key("source-root-upload", generation, expected_root),
                "SELECT generation, value_sha256 "
                "FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                (generation, expected_root),
            )
            if replay_claim:
                _require_exact(
                    "replayed source-root upload claim",
                    replay_claim,
                    (generation, expected_root),
                )
                affected = connector.execute_affected(
                    "DELETE FROM operational_canonical_value_uploads "
                    "WHERE generation = %s AND value_sha256 = %s",
                    (generation, expected_root),
                )
                if affected != 1:
                    raise SourceBuildNotReadyError(
                        "replayed source-root claim changed before release"
                    )
            return SourceBuildHandoff(
                mapped_build,
                generation,
                scope,
                expected_root,
                manifest_policy_id,
                True,
            )

        if working and working[1] != command.build_attempt_id:
            raise SourceBuildConflictError(
                "the sole source working slot belongs to another build"
            )

        # The root identity is the consumer prerequisite.  Its upload claim is
        # checked before all inserts and remains until the last statement.
        claim = work.lock_row(
            LockRank.CHECKPOINT,
            encode_lock_key("source-root-upload", generation, expected_root),
            "SELECT generation, value_sha256 "
            "FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (generation, expected_root),
        )
        if claim != (generation, expected_root):
            raise SourceBuildNotReadyError("exact source-root upload claim is absent")
        _require_sealed_root_identity(
            connector,
            source_root_sha256=expected_root,
            byte_count=root_plan.byte_count,
            root_page_sha256=root_plan.root_page_sha256,
        )
        base_source = _lock_source_head(work, _DEFAULT_CHANNEL)

        _insert_or_validate_scope(
            connector,
            scope=scope,
            source_root_sha256=expected_root,
        )
        try:
            durable_build = load_source_build_family(
                connector,
                build_id=command.build_attempt_id,
            )
        except ManifestFamilyCollisionError as error:
            raise SourceBuildConflictError(str(error)) from error
        if durable_build is None:
            created_at = database_unix_microseconds(work)
            _insert_or_validate_build(
                connector,
                build_id=command.build_attempt_id,
                scope=scope,
                manifest_policy_id=manifest_policy_id,
                created_at=created_at,
            )
        else:
            _require_exact(
                "successor-generation source build immutable fields",
                (durable_build.scope_key, durable_build.manifest_policy_id),
                (scope, manifest_policy_id),
            )
            if durable_build.state not in {"OPEN", "SEALED"}:
                raise SourceBuildConflictError(
                    "successor generation cannot reuse an ABANDONED source build"
                )
            created_at = durable_build.created_at
        _insert_or_validate(
            connector,
            label="source build channel",
            select_sql=(
                "SELECT build_id, channel FROM catalog_source_build_channel "
                "WHERE build_id = %s"
            ),
            select_data=(command.build_attempt_id,),
            insert_sql=(
                "INSERT INTO catalog_source_build_channel "
                "(build_id, channel) VALUES (%s, %s)"
            ),
            expected=(command.build_attempt_id, _DEFAULT_CHANNEL),
        )
        if base_source is not None:
            _insert_or_validate(
                connector,
                label="source build base publication commit",
                select_sql=(
                    f"SELECT build_id, base_receipt_id "
                    f"FROM {_SOURCE_BUILD_BASE_COMMIT_TABLE} WHERE build_id = %s"
                ),
                select_data=(command.build_attempt_id,),
                insert_sql=(
                    f"INSERT INTO {_SOURCE_BUILD_BASE_COMMIT_TABLE} "
                    "(build_id, base_receipt_id) VALUES (%s, %s)"
                ),
                expected=(command.build_attempt_id, base_source.receipt_id),
            )
        if durable_build is None:
            _insert_or_validate_checkpoints(
                connector,
                build_id=command.build_attempt_id,
                created_at=created_at,
            )
        else:
            _require_checkpoint_pair(
                connector,
                build_id=command.build_attempt_id,
                build_state=durable_build.state,
            )
        _insert_or_validate(
            connector,
            label="source build generation",
            select_sql=(
                "SELECT build_id, generation "
                "FROM operational_source_build_generations WHERE generation = %s"
            ),
            select_data=(generation,),
            insert_sql=(
                "INSERT INTO operational_source_build_generations "
                "(build_id, generation) VALUES (%s, %s)"
            ),
            expected=(command.build_attempt_id, generation),
        )
        _insert_or_validate(
            connector,
            label="source working build",
            select_sql=(
                "SELECT slot, build_id, assigned_at "
                "FROM operational_source_working_builds WHERE slot = %s"
            ),
            select_data=(1,),
            insert_sql=(
                "INSERT INTO operational_source_working_builds "
                "(slot, build_id, assigned_at) VALUES (%s, %s, %s)"
            ),
            expected=(1, command.build_attempt_id, created_at),
        )

        affected = connector.execute_affected(
            "DELETE FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (generation, expected_root),
        )
        if affected != 1:
            raise SourceBuildNotReadyError(
                "source-root claim changed before durable consumer handoff"
            )
        return SourceBuildHandoff(
            command.build_attempt_id,
            generation,
            scope,
            expected_root,
            manifest_policy_id,
            False,
        )

    @staticmethod
    def abandon(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        build_id: bytes,
        now: int,
    ) -> SourceBuildAbandonment:
        """Atomically release one exact OPEN working root as ABANDONED.

        The generation mapping deliberately remains durable, so a response-loss
        replay is read-only and the same generation cannot reserve a replacement.
        """

        build = require_uuid16(build_id, field="build_id")
        generation = _authorize(
            work,
            gate_lease,
            ingest_turn,
            now=require_int63(now, field="now"),
        )
        mapping = work.lock_row(
            LockRank.WORKING_ROOT,
            encode_lock_key("source-build", 0, generation),
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (generation,),
        )
        if mapping != (build,):
            raise SourceBuildNotReadyError(
                "live ingest generation is not mapped to this source build"
            )
        working = work.lock_row(
            LockRank.WORKING_ROOT,
            encode_lock_key("source-build", 1, 1),
            "SELECT build_id, assigned_at FROM operational_source_working_builds "
            "WHERE slot = %s",
            (1,),
        )
        family = _load_source_build_or_conflict(work.connector, build)
        state = work.lock_row(
            LockRank.WORKING_ROOT,
            encode_lock_key("source-build", 2, build),
            "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
            (build,),
        )
        if state != (family.state,):
            raise SourceBuildConflictError("source build state changed while locking")
        if family.state == "ABANDONED":
            if working:
                raise SourceBuildConflictError(
                    "ABANDONED source build retained its working root"
                )
            return SourceBuildAbandonment(build, generation, True)
        if family.state != "OPEN":
            raise SourceBuildConflictError("terminal source build cannot be abandoned")
        if working != (build, family.created_at):
            raise SourceBuildNotReadyError(
                "source build is not the exact locked working root"
            )
        deleted = work.connector.execute_affected(
            "DELETE FROM operational_source_working_builds "
            "WHERE slot = %s AND build_id = %s AND assigned_at = %s",
            (1, build, family.created_at),
        )
        if deleted != 1:
            raise SourceBuildConflictError(
                "source working root changed before abandonment"
            )
        work.compare_and_swap(
            "UPDATE catalog_source_build_states SET state = %s "
            "WHERE build_id = %s AND state = %s",
            ("ABANDONED", build, "OPEN"),
            authority="source build abandonment",
        )
        return SourceBuildAbandonment(build, generation, False)

    @staticmethod
    def prepare_discovery_batch(
        connector: Any,
        *,
        build_id: bytes,
        plan: SourceDiscoveryPlan,
    ) -> DiscoveryBatch:
        """Issue the next locator page using a read-only database snapshot.

        This performs private-spool reads outside the later mutation
        transaction.  The mutation path rechecks the complete checkpoint
        pre-state and every fixed-width descriptor.
        """

        build = require_uuid16(build_id, field="build_id")
        if not isinstance(plan, SourceDiscoveryPlan):
            raise TypeError("plan must be SourceDiscoveryPlan")
        plan._require_open()
        build_row = _load_source_build_or_conflict(connector, build)
        if build_row.state != "OPEN":
            raise SourceBuildNotReadyError("discovery requires an OPEN source build")
        checkpoint = connector.fetch_one(
            "SELECT generation, cursor_bytes, processed_count, state "
            "FROM operational_source_build_discovery_checkpoints "
            "WHERE build_id = %s",
            (build,),
        )
        start_generation, cursor, processed_count = _validate_discovery_checkpoint(
            checkpoint,
            require_open=True,
        )
        if processed_count > plan.gallery_count:
            raise SourceBuildConflictError(
                "discovery checkpoint exceeds the frozen locator plan"
            )
        _require_discovery_plan_binding(
            connector,
            build_id=build,
            plan=plan,
            start_generation=start_generation,
            start_cursor=cursor,
            start_processed_count=processed_count,
        )
        locators = plan._page(processed_count)
        if locators and locators[0].position != processed_count:
            raise SourceDiscoveryPlanError("locator plan has a position gap")
        if not locators and processed_count != plan.gallery_count:
            raise SourceDiscoveryPlanError("locator plan ended before its exact count")
        batch_capability = object()
        return DiscoveryBatch(
            build,
            _discovery_batch_key(plan, build, start_generation),
            plan.scan_attempt,
            plan.gallery_count,
            plan.tree_observation_sha256,
            start_generation,
            cursor,
            processed_count,
            locators,
            not locators,
            plan._capability,
            batch_capability,
            _DISCOVERY_BATCH_TOKEN,
        )

    @staticmethod
    def resolve_discovery_locator(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        batch: DiscoveryBatch,
        locator: PreparedDiscoveryLocator,
        upload_plan: CanonicalValueUploadPlan,
        now: int,
    ) -> ResolvedDiscoveryLocator:
        """Consume one locator claim in its own bounded short transaction.

        Separating these handoffs from the membership checkpoint transaction
        prevents a rank-40 checkpoint lock from being followed by a rank-50
        allocation and then another rank-40 upload claim.
        """

        if type(batch) is not DiscoveryBatch:
            raise TypeError("batch must be an exact DiscoveryBatch")
        batch.__post_init__()
        if type(locator) is not PreparedDiscoveryLocator:
            raise TypeError("locator must be an exact PreparedDiscoveryLocator")
        locator.__post_init__()
        if locator._plan_capability is not batch._plan_capability:
            raise SourceBuildConflictError("locator belongs to another plan")
        if not any(item is locator for item in batch.locators):
            raise SourceBuildConflictError("locator was not issued in this batch")
        if type(upload_plan) is not CanonicalValueUploadPlan:
            raise TypeError("upload_plan must be an exact CanonicalValueUploadPlan")
        _validate_locator_upload(locator, upload_plan)

        timestamp = require_int63(now, field="now")
        generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        scope, _policy, state, _created_at = _lock_build_context(
            work,
            generation=generation,
            build_id=batch.build_id,
        )
        if state != "OPEN":
            raise SourceBuildNotReadyError("locator handoff requires an OPEN build")
        connector = work.connector
        claim = work.lock_row(
            LockRank.CHECKPOINT,
            encode_lock_key("canonical-upload", generation, locator.locator_sha256),
            "SELECT generation, value_sha256 "
            "FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (generation, locator.locator_sha256),
        )
        if claim and claim != (generation, locator.locator_sha256):
            raise SourceBuildConflictError("locator upload claim differs")
        _require_sealed_locator(connector, locator, upload_plan)

        stable_key = gallery_key(scope, locator.locator_sha256)
        locator_row = connector.fetch_one(
            "SELECT source_gallery_name FROM catalog_source_locator_identity "
            "WHERE locator_sha256 = %s",
            (locator.locator_sha256,),
        )
        if locator_row:
            _require_exact(
                "source locator leaf",
                locator_row,
                (locator.source_gallery_name,),
            )
        existing = _load_gallery_identity(
            connector,
            scope=scope,
            locator_sha256=locator.locator_sha256,
            stable_key=stable_key,
        )
        replayed = existing is not None
        if existing is None:
            gallery_id = VNextAllocatorRepository.allocate_identity(
                work,
                IdentityStream.GALLERY,
                updated_at=timestamp,
            )
            # The allocator serializes creation.  Re-read both natural keys
            # before inserting collision-sensitive catalog identities.
            existing = _load_gallery_identity(
                connector,
                scope=scope,
                locator_sha256=locator.locator_sha256,
                stable_key=stable_key,
            )
            if existing is None:
                if not locator_row:
                    if not claim:
                        raise SourceBuildNotReadyError(
                            "new locator identity requires its current upload claim"
                        )
                    connector.execute(
                        "INSERT INTO catalog_source_locator_identity "
                        "(locator_sha256, source_gallery_name) VALUES (%s, %s)",
                        (locator.locator_sha256, locator.source_gallery_name),
                    )
                try:
                    created = ensure_gallery_identity(
                        connector,
                        identity=GalleryIdentity(
                            gallery_id,
                            stable_key,
                            scope,
                            locator.locator_sha256,
                        ),
                    )
                except CatalogIdentityCollisionError as error:
                    raise SourceBuildConflictError(str(error)) from error
                if not created:
                    raise SourceBuildConflictError(
                        "gallery identity appeared after its allocator re-read"
                    )
                connector.execute(
                    "INSERT INTO operational_gallery_observation_allocators "
                    "(gallery_id, next_observation_id, updated_at) "
                    "VALUES (%s, %s, %s)",
                    (gallery_id, 1, timestamp),
                )
            else:
                gallery_id = existing
                replayed = True
        else:
            gallery_id = existing
            if not locator_row:
                raise SourceBuildConflictError(
                    "gallery identity has no exact source locator identity"
                )
        _require_gallery_observation_allocator(connector, gallery_id)
        if claim:
            affected = connector.execute_affected(
                "DELETE FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                (generation, locator.locator_sha256),
            )
            if affected != 1:
                raise SourceBuildConflictError(
                    "locator upload claim changed before durable handoff"
                )
        return ResolvedDiscoveryLocator(
            batch.build_id,
            batch.batch_key,
            locator.position,
            locator.locator_sha256,
            gallery_id,
            stable_key,
            replayed,
            batch._batch_capability,
        )

    @staticmethod
    def commit_discovery_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        batch: DiscoveryBatch,
        resolved: tuple[ResolvedDiscoveryLocator, ...],
        now: int,
    ) -> DiscoveryBatchReceipt:
        if type(batch) is not DiscoveryBatch:
            raise TypeError("batch must be an exact DiscoveryBatch")
        batch.__post_init__()
        if type(resolved) is not tuple:
            raise TypeError("resolved must be an exact tuple")
        if any(type(item) is not ResolvedDiscoveryLocator for item in resolved):
            raise TypeError(
                "resolved must contain exact ResolvedDiscoveryLocator values"
            )
        for item in resolved:
            item.__post_init__()
        timestamp = require_int63(now, field="now")
        generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        _scope, _policy, state, _created_at = _lock_build_context(
            work,
            generation=generation,
            build_id=batch.build_id,
        )
        if state != "OPEN":
            raise SourceBuildNotReadyError("discovery requires an OPEN source build")
        checkpoint = work.lock_row(
            LockRank.CHECKPOINT,
            encode_lock_key("source-discovery-checkpoint", batch.build_id),
            "SELECT generation, cursor_bytes, processed_count, state, updated_at "
            "FROM operational_source_build_discovery_checkpoints "
            "WHERE build_id = %s",
            (batch.build_id,),
        )
        connector = work.connector
        stored = connector.fetch_one(
            _DISCOVERY_RECEIPT_SELECT + " WHERE build_id = %s AND batch_key = %s",
            (batch.build_id, batch.batch_key),
        )
        if stored:
            receipt = _discovery_receipt_from_row(stored, replayed=True)
            _require_discovery_receipt_matches_batch(receipt, batch)
            _require_discovery_replay_state(
                connector,
                checkpoint=checkpoint,
                receipt=receipt,
                batch=batch,
            )
            return receipt

        start_generation, start_cursor, start_count = _validate_discovery_checkpoint(
            checkpoint[:4],
            require_open=True,
        )
        _require_exact(
            "discovery batch checkpoint pre-state",
            (start_generation, start_cursor, start_count),
            (
                batch.start_generation,
                batch.start_cursor,
                batch.start_processed_count,
            ),
        )
        expected_key = _discovery_batch_key_values(
            batch.scan_attempt,
            batch.tree_observation_sha256,
            batch.build_id,
            batch.start_generation,
        )
        if expected_key != batch.batch_key:
            raise SourceBuildConflictError("discovery batch capability changed")
        _require_discovery_batch_binding(connector, batch)

        if len(resolved) != len(batch.locators):
            raise SourceBuildNotReadyError(
                "every locator in the page must have durable identity evidence"
            )
        try:
            durable_identities = load_gallery_identities(
                connector,
                gallery_ids=tuple(evidence.gallery_id for evidence in resolved),
            )
        except CatalogIdentityCollisionError as error:
            raise SourceBuildConflictError(str(error)) from error
        if len(durable_identities) != len(resolved):
            raise SourceBuildConflictError(
                "resolved discovery batch lacks complete gallery identities"
            )
        for expected_locator, evidence in zip(batch.locators, resolved, strict=True):
            _validate_resolved_locator(batch, expected_locator, evidence)
            gallery = durable_identities.get(evidence.gallery_id)
            gallery_row = (
                ()
                if gallery is None
                else (
                    gallery.gallery_key,
                    gallery.scope_key,
                    gallery.locator_sha256,
                )
            )
            _require_exact(
                "resolved gallery identity",
                gallery_row,
                (evidence.gallery_key, _scope, evidence.locator_sha256),
            )
            expected_row = (
                batch.build_id,
                expected_locator.position,
                evidence.gallery_id,
            )
            by_gallery = connector.fetch_one(
                "SELECT build_id, position, gallery_id "
                "FROM catalog_source_build_expected_gallery "
                "WHERE build_id = %s AND gallery_id = %s",
                (batch.build_id, evidence.gallery_id),
            )
            if by_gallery:
                _require_exact(
                    "expected source gallery identity",
                    by_gallery,
                    expected_row,
                )
            _insert_or_validate(
                connector,
                label="expected source gallery position",
                select_sql=(
                    "SELECT build_id, position, gallery_id "
                    "FROM catalog_source_build_expected_gallery "
                    "WHERE build_id = %s AND position = %s"
                ),
                select_data=(batch.build_id, expected_locator.position),
                insert_sql=(
                    "INSERT INTO catalog_source_build_expected_gallery "
                    "(build_id, position, gallery_id) VALUES (%s, %s, %s)"
                ),
                expected=expected_row,
            )
            persisted_gallery = connector.fetch_one(
                "SELECT build_id, position, gallery_id "
                "FROM catalog_source_build_expected_gallery "
                "WHERE build_id = %s AND gallery_id = %s",
                (batch.build_id, evidence.gallery_id),
            )
            _require_exact(
                "expected source gallery identity",
                persisted_gallery,
                expected_row,
            )

        terminal = batch.terminal
        row_count = len(batch.locators)
        if terminal:
            if resolved or start_count != batch.gallery_count:
                raise SourceBuildConflictError(
                    "terminal discovery must be the exact empty plan page"
                )
            next_cursor = start_cursor
            next_count = start_count
            next_state = "COMPLETE"
        else:
            next_count = _checked_add(
                start_count,
                row_count,
                field="discovery processed_count",
            )
            next_cursor = _encode_cursor(batch.locators[-1].position)
            if next_count > batch.gallery_count:
                raise SourceBuildConflictError("discovery batch exceeds plan count")
            next_state = "OPEN"
        committed_generation = _checked_add(
            start_generation,
            1,
            field="discovery committed_generation",
        )
        receipt_tuple = (
            batch.build_id,
            batch.batch_key,
            start_generation,
            start_cursor,
            start_count,
            next_cursor,
            next_count,
            next_state,
            row_count,
            int(terminal),
            committed_generation,
            timestamp,
        )
        existing_start = connector.fetch_one(
            _DISCOVERY_RECEIPT_SELECT
            + " WHERE build_id = %s AND start_generation = %s",
            (batch.build_id, start_generation),
        )
        if existing_start:
            raise SourceBuildConflictError(
                "discovery generation already belongs to another batch"
            )
        connector.execute(
            "INSERT INTO operational_source_build_discovery_batch_receipts "
            "(build_id, batch_key, start_generation, start_cursor, "
            "start_processed_count, next_cursor, next_processed_count, "
            "next_state, row_count, terminal, committed_generation, committed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            receipt_tuple,
        )
        if terminal:
            _persist_source_build_discovery(
                connector,
                build_id=batch.build_id,
                scan_attempt=batch.scan_attempt,
                gallery_count=batch.gallery_count,
                tree_observation_sha256=batch.tree_observation_sha256,
                completed_at=timestamp,
            )
        work.compare_and_swap(
            "UPDATE operational_source_build_discovery_checkpoints "
            "SET generation = %s, cursor_bytes = %s, processed_count = %s, "
            "state = %s, updated_at = %s WHERE build_id = %s "
            "AND generation = %s AND cursor_bytes = %s "
            "AND processed_count = %s AND state = %s AND updated_at = %s",
            (
                committed_generation,
                next_cursor,
                next_count,
                next_state,
                timestamp,
                batch.build_id,
                start_generation,
                start_cursor,
                start_count,
                "OPEN",
                checkpoint[4],
            ),
            authority="source discovery checkpoint",
        )
        return DiscoveryBatchReceipt(
            batch.build_id,
            batch.batch_key,
            start_generation,
            start_cursor,
            start_count,
            next_cursor,
            next_count,
            next_state,
            row_count,
            terminal,
            committed_generation,
            timestamp,
            False,
        )

    @staticmethod
    def issue_assembly_batch() -> AssemblyBatchAttempt:
        """Issue an opaque idempotency capability outside any transaction."""

        return AssemblyBatchAttempt(
            _new_assembly_batch_key(),
            _ASSEMBLY_ATTEMPT_TOKEN,
        )

    @staticmethod
    def assemble_batch(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        build_id: bytes,
        attempt: AssemblyBatchAttempt,
        now: int,
    ) -> AssemblyBatchReceipt:
        build = require_uuid16(build_id, field="build_id")
        if type(attempt) is not AssemblyBatchAttempt:
            raise TypeError("attempt must be an exact AssemblyBatchAttempt")
        attempt.__post_init__()
        timestamp = require_int63(now, field="now")
        generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        scope, manifest_policy_id, build_state, created_at = _lock_build_context(
            work,
            generation=generation,
            build_id=build,
            allow_sealed=True,
        )
        checkpoint = work.lock_row(
            LockRank.CHECKPOINT,
            encode_lock_key("source-assembly-checkpoint", build),
            "SELECT generation, cursor_bytes, processed_gallery_count, "
            "processed_file_count, processed_byte_count, "
            "manifest_chain_sha256, state, updated_at "
            "FROM operational_source_build_assembly_checkpoints "
            "WHERE build_id = %s",
            (build,),
        )
        connector = work.connector
        stored = connector.fetch_one(
            _ASSEMBLY_RECEIPT_SELECT + " WHERE build_id = %s AND batch_key = %s",
            (build, attempt.batch_key),
        )
        if stored:
            receipt = _assembly_receipt_from_row(stored, replayed=True)
            _validate_assembly_replay(
                connector,
                receipt=receipt,
                checkpoint=checkpoint,
                build_state=build_state,
                scope=scope,
                manifest_policy_id=manifest_policy_id,
                created_at=created_at,
            )
            return receipt
        if build_state != "OPEN":
            raise SourceBuildNotReadyError("new assembly work requires an OPEN build")
        (
            start_generation,
            start_cursor,
            start_gallery_count,
            start_file_count,
            start_byte_count,
            start_chain,
        ) = _validate_assembly_checkpoint(checkpoint[:7], require_open=True)
        discovery = connector.fetch_one(
            "SELECT gallery_count.gallery_count, "
            "tree.tree_observation_sha256, checkpoint.state "
            "FROM catalog_source_build_discovery_seals seal "
            "JOIN catalog_source_build_discovery_gallery_counts gallery_count "
            "ON gallery_count.build_id = seal.build_id "
            "JOIN catalog_source_build_discovery_tree_observation_sha256s tree "
            "ON tree.build_id = seal.build_id "
            "JOIN operational_source_build_discovery_checkpoints checkpoint "
            "ON checkpoint.build_id = seal.build_id WHERE seal.build_id = %s",
            (build,),
        )
        if len(discovery) != 3 or discovery[2] != "COMPLETE":
            raise SourceBuildNotReadyError("assembly requires complete discovery")
        discovered_count = require_int63(discovery[0], field="discovered gallery_count")
        require_digest32(discovery[1], field="tree_observation_sha256")

        rows = _load_assembly_page(
            connector,
            build_id=build,
            cursor=start_cursor,
            manifest_policy_id=manifest_policy_id,
        )
        next_gallery_count = start_gallery_count
        next_file_count = start_file_count
        next_byte_count = start_byte_count
        next_chain = start_chain
        for offset, row in enumerate(rows):
            expected_position = _checked_add(
                start_gallery_count,
                offset,
                field="assembly expected position",
            )
            (
                position,
                _gallery_id,
                gallery_key_bytes,
                gallery_scope,
                _observation_id,
                observation_identity,
                file_count,
                byte_count,
                row_policy,
                manifest_sha256,
            ) = _validate_assembly_row(
                row,
                expected_position=expected_position,
                scope=scope,
                manifest_policy_id=manifest_policy_id,
            )
            next_gallery_count = _checked_add(
                next_gallery_count,
                1,
                field="assembly gallery count",
            )
            next_file_count = _checked_add(
                next_file_count,
                file_count,
                field="assembly file count",
            )
            next_byte_count = _checked_add(
                next_byte_count,
                byte_count,
                field="assembly byte count",
            )
            next_chain = _manifest_chain_step(
                next_chain,
                position=position,
                gallery_key_bytes=gallery_key_bytes,
                observation_identity_sha256=observation_identity,
                gallery_manifest_sha256=manifest_sha256,
                file_count=file_count,
                byte_count=byte_count,
            )
            require_positive_int63(row_policy, field="manifest_policy_id")

        terminal = not rows
        row_count = len(rows)
        if terminal:
            if start_gallery_count != discovered_count:
                raise SourceBuildConflictError(
                    "assembly ended before the exact discovered gallery count"
                )
            next_cursor = start_cursor
            next_state = "COMPLETE"
        else:
            next_cursor = _encode_cursor(
                require_int63(rows[-1][0], field="assembly last position")
            )
            if next_gallery_count > discovered_count:
                raise SourceBuildConflictError(
                    "assembly contains more galleries than discovery"
                )
            next_state = "OPEN"
        committed_generation = _checked_add(
            start_generation,
            1,
            field="assembly committed_generation",
        )
        committed_at = database_unix_microseconds(work) if terminal else timestamp
        receipt_tuple = (
            build,
            attempt.batch_key,
            start_generation,
            start_cursor,
            start_gallery_count,
            start_file_count,
            start_byte_count,
            start_chain,
            next_cursor,
            next_gallery_count,
            next_file_count,
            next_byte_count,
            next_chain,
            next_state,
            row_count,
            int(terminal),
            committed_generation,
            committed_at,
        )
        existing_start = connector.fetch_one(
            _ASSEMBLY_RECEIPT_SELECT + " WHERE build_id = %s AND start_generation = %s",
            (build, start_generation),
        )
        if existing_start:
            raise SourceBuildConflictError(
                "assembly generation already belongs to another batch"
            )
        connector.execute(
            "INSERT INTO operational_source_build_assembly_batch_receipts "
            "(build_id, batch_key, start_generation, start_cursor, "
            "start_gallery_count, start_file_count, start_byte_count, "
            "start_manifest_chain_sha256, next_cursor, next_gallery_count, "
            "next_file_count, next_byte_count, next_manifest_chain_sha256, "
            "next_state, row_count, terminal, committed_generation, committed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s, %s, %s)",
            receipt_tuple,
        )
        if terminal:
            connector.execute(
                "INSERT INTO catalog_source_build_sealed_ats "
                "(build_id, sealed_at) VALUES (%s, %s)",
                (build, committed_at),
            )
        work.compare_and_swap(
            "UPDATE operational_source_build_assembly_checkpoints SET "
            "generation = %s, cursor_bytes = %s, processed_gallery_count = %s, "
            "processed_file_count = %s, processed_byte_count = %s, "
            "manifest_chain_sha256 = %s, state = %s, updated_at = %s "
            "WHERE build_id = %s AND generation = %s AND cursor_bytes = %s "
            "AND processed_gallery_count = %s AND processed_file_count = %s "
            "AND processed_byte_count = %s AND manifest_chain_sha256 = %s "
            "AND state = %s AND updated_at = %s",
            (
                committed_generation,
                next_cursor,
                next_gallery_count,
                next_file_count,
                next_byte_count,
                next_chain,
                next_state,
                timestamp,
                build,
                start_generation,
                start_cursor,
                start_gallery_count,
                start_file_count,
                start_byte_count,
                start_chain,
                "OPEN",
                checkpoint[7],
            ),
            authority="source assembly checkpoint",
        )
        if terminal:
            work.compare_and_swap(
                "UPDATE catalog_source_build_states SET state = %s "
                "WHERE build_id = %s AND state = %s",
                ("SEALED", build, "OPEN"),
                authority="source build seal",
            )
            try:
                ensure_build_manifest_family(
                    connector,
                    build_id=build,
                    manifest_sha256=next_chain,
                    gallery_count=next_gallery_count,
                    file_count=next_file_count,
                    byte_count=next_byte_count,
                    computed_at=committed_at,
                )
            except ManifestFamilyCollisionError as error:
                raise SourceBuildConflictError(str(error)) from error
        return AssemblyBatchReceipt(
            build,
            attempt.batch_key,
            start_generation,
            start_cursor,
            start_gallery_count,
            start_file_count,
            start_byte_count,
            start_chain,
            next_cursor,
            next_gallery_count,
            next_file_count,
            next_byte_count,
            next_chain,
            next_state,
            row_count,
            terminal,
            committed_generation,
            committed_at,
            False,
        )


_DISCOVERY_RECEIPT_SELECT = (
    "SELECT build_id, batch_key, start_generation, start_cursor, "
    "start_processed_count, next_cursor, next_processed_count, next_state, "
    "row_count, terminal, committed_generation, committed_at "
    "FROM operational_source_build_discovery_batch_receipts"
)
_ASSEMBLY_RECEIPT_SELECT = (
    "SELECT build_id, batch_key, start_generation, start_cursor, "
    "start_gallery_count, start_file_count, start_byte_count, "
    "start_manifest_chain_sha256, next_cursor, next_gallery_count, "
    "next_file_count, next_byte_count, next_manifest_chain_sha256, "
    "next_state, row_count, terminal, committed_generation, committed_at "
    "FROM operational_source_build_assembly_batch_receipts"
)


def _read_file_parts(path: Path) -> Iterator[bytes]:
    with path.open("rb") as stream:
        while True:
            part = stream.read(_STREAM_BYTES)
            if not part:
                return
            yield part


def _files_equal(left: Path, right: Path) -> bool:
    if left.stat().st_size != right.stat().st_size:
        return False
    with left.open("rb") as left_stream, right.open("rb") as right_stream:
        while True:
            left_part = left_stream.read(_STREAM_BYTES)
            right_part = right_stream.read(_STREAM_BYTES)
            if left_part != right_part:
                return False
            if not left_part:
                return True


def _checked_add(left: int, right: int, *, field: str) -> int:
    first = require_int63(left, field=field)
    second = require_int63(right, field=field)
    if first > INT63_MAX - second:
        raise SourceBuildConflictError(f"{field} exceeds signed-int63")
    return first + second


def _load_source_build_or_conflict(
    connector: Any,
    build_id: bytes,
) -> SourceBuildFamily:
    try:
        family = load_source_build_family(connector, build_id=build_id)
    except ManifestFamilyCollisionError as error:
        raise SourceBuildConflictError(str(error)) from error
    if family is None:
        raise SourceBuildNotReadyError("source build is missing")
    return family


def _encode_cursor(position: int) -> bytes:
    return require_int63(position, field="cursor position").to_bytes(8, "big")


def _validate_cursor(cursor: object, processed_count: int, *, field: str) -> bytes:
    exact = require_bounded_bytes(cursor, field=field, maximum=8)
    count = require_int63(processed_count, field=f"{field} processed_count")
    expected = b"" if count == 0 else _encode_cursor(count - 1)
    if exact != expected:
        raise SourceBuildConflictError(f"{field} is not the exact contiguous cursor")
    return exact


def _insert_or_validate_checkpoints(
    connector: Any,
    *,
    build_id: bytes,
    created_at: int,
) -> None:
    discovery = connector.fetch_one(
        "SELECT generation, cursor_bytes, processed_count, state, updated_at "
        "FROM operational_source_build_discovery_checkpoints WHERE build_id = %s",
        (build_id,),
    )
    assembly = connector.fetch_one(
        "SELECT generation, cursor_bytes, processed_gallery_count, "
        "processed_file_count, processed_byte_count, manifest_chain_sha256, "
        "state, updated_at FROM operational_source_build_assembly_checkpoints "
        "WHERE build_id = %s",
        (build_id,),
    )
    if bool(discovery) != bool(assembly):
        raise SourceBuildConflictError("source build checkpoint pair is incomplete")
    if discovery:
        _validate_discovery_checkpoint(discovery[:4], require_open=False)
        require_int63(discovery[4], field="discovery updated_at")
        _validate_assembly_checkpoint(assembly[:7], require_open=True)
        require_int63(assembly[7], field="assembly updated_at")
        _require_checkpoint_cross_state(
            connector,
            build_id=build_id,
            discovery=discovery,
            assembly=assembly,
            build_state="OPEN",
        )
        return
    connector.execute(
        "INSERT INTO operational_source_build_discovery_checkpoints "
        "(build_id, generation, cursor_bytes, processed_count, state, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (build_id, 1, b"", 0, "OPEN", created_at),
    )
    connector.execute(
        "INSERT INTO operational_source_build_assembly_checkpoints "
        "(build_id, generation, cursor_bytes, processed_gallery_count, "
        "processed_file_count, processed_byte_count, manifest_chain_sha256, "
        "state, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (build_id, 1, b"", 0, 0, 0, _EMPTY_MANIFEST_CHAIN, "OPEN", created_at),
    )


def _require_checkpoint_pair(
    connector: Any,
    *,
    build_id: bytes,
    build_state: str,
) -> None:
    discovery = connector.fetch_one(
        "SELECT generation, cursor_bytes, processed_count, state, updated_at "
        "FROM operational_source_build_discovery_checkpoints WHERE build_id = %s",
        (build_id,),
    )
    assembly = connector.fetch_one(
        "SELECT generation, cursor_bytes, processed_gallery_count, "
        "processed_file_count, processed_byte_count, manifest_chain_sha256, "
        "state, updated_at FROM operational_source_build_assembly_checkpoints "
        "WHERE build_id = %s",
        (build_id,),
    )
    if not discovery or not assembly:
        raise SourceBuildConflictError("replayed build checkpoint pair is missing")
    _validate_discovery_checkpoint(discovery[:4], require_open=False)
    _validate_assembly_checkpoint(assembly[:7], require_open=False)
    require_int63(discovery[4], field="discovery updated_at")
    require_int63(assembly[7], field="assembly updated_at")
    _require_checkpoint_cross_state(
        connector,
        build_id=build_id,
        discovery=discovery,
        assembly=assembly,
        build_state=build_state,
    )


def _require_checkpoint_cross_state(
    connector: Any,
    *,
    build_id: bytes,
    discovery: tuple[Any, ...],
    assembly: tuple[Any, ...],
    build_state: str,
) -> None:
    discovery_count = require_int63(discovery[2], field="discovery processed_count")
    assembly_count = require_int63(
        assembly[2],
        field="assembly processed_gallery_count",
    )
    if assembly_count > discovery_count:
        raise SourceBuildConflictError("assembly checkpoint exceeds discovery")
    discovery_row = connector.fetch_one(
        "SELECT scan_attempt, gallery_count, tree_observation_sha256, completed_at "
        "FROM catalog_source_build_discoveries WHERE build_id = %s",
        (build_id,),
    )
    if discovery[3] == "COMPLETE":
        if len(discovery_row) != 4:
            raise SourceBuildConflictError(
                "complete discovery checkpoint has no durable discovery"
            )
        require_uuid16(discovery_row[0], field="scan_attempt")
        if require_int63(discovery_row[1], field="gallery_count") != discovery_count:
            raise SourceBuildConflictError(
                "durable discovery count differs from its checkpoint"
            )
        require_digest32(discovery_row[2], field="tree_observation_sha256")
        require_int63(discovery_row[3], field="discovery completed_at")
    elif discovery_row:
        raise SourceBuildConflictError("OPEN discovery has a terminal discovery row")
    if discovery[3] != "COMPLETE" and assembly_count != 0:
        raise SourceBuildConflictError("assembly advanced before discovery completed")
    expected_assembly_state = "COMPLETE" if build_state == "SEALED" else "OPEN"
    if assembly[6] != expected_assembly_state:
        raise SourceBuildConflictError(
            "source build and assembly checkpoint states disagree"
        )
    try:
        manifest = load_build_manifest_family(connector, build_id=build_id)
    except ManifestFamilyCollisionError as error:
        raise SourceBuildConflictError(str(error)) from error
    if build_state == "SEALED":
        if manifest is None:
            raise SourceBuildConflictError("SEALED build has no exact manifest")
        _require_exact(
            "sealed build manifest checkpoint",
            (
                manifest.manifest_sha256,
                manifest.gallery_count,
                manifest.file_count,
                manifest.byte_count,
            ),
            (assembly[5], assembly[2], assembly[3], assembly[4]),
        )
        require_int63(manifest.computed_at, field="build manifest computed_at")
    elif manifest is not None:
        raise SourceBuildConflictError("OPEN build already has a build manifest")


def _validate_discovery_checkpoint(
    row: tuple[Any, ...],
    *,
    require_open: bool,
) -> tuple[int, bytes, int]:
    if len(row) != 4:
        raise SourceBuildNotReadyError("source discovery checkpoint is missing")
    generation = require_positive_int63(row[0], field="discovery generation")
    processed_count = require_int63(row[2], field="discovery processed_count")
    cursor = _validate_cursor(
        row[1],
        processed_count,
        field="discovery cursor",
    )
    if row[3] not in {"OPEN", "COMPLETE"}:
        raise SourceBuildConflictError("discovery checkpoint has an invalid state")
    if require_open and row[3] != "OPEN":
        raise SourceBuildNotReadyError("source discovery is already complete")
    if generation == 1 and (cursor != b"" or processed_count != 0):
        raise SourceBuildConflictError("discovery genesis checkpoint is corrupt")
    return generation, cursor, processed_count


def _validate_assembly_checkpoint(
    row: tuple[Any, ...],
    *,
    require_open: bool,
) -> tuple[int, bytes, int, int, int, bytes]:
    if len(row) != 7:
        raise SourceBuildNotReadyError("source assembly checkpoint is missing")
    generation = require_positive_int63(row[0], field="assembly generation")
    gallery_count = require_int63(row[2], field="assembly gallery count")
    cursor = _validate_cursor(row[1], gallery_count, field="assembly cursor")
    file_count = require_int63(row[3], field="assembly file count")
    byte_count = require_int63(row[4], field="assembly byte count")
    chain = require_digest32(row[5], field="manifest_chain_sha256")
    if row[6] not in {"OPEN", "COMPLETE"}:
        raise SourceBuildConflictError("assembly checkpoint has an invalid state")
    if require_open and row[6] != "OPEN":
        raise SourceBuildNotReadyError("source assembly is already complete")
    if generation == 1 and (
        cursor != b""
        or gallery_count != 0
        or file_count != 0
        or byte_count != 0
        or chain != _EMPTY_MANIFEST_CHAIN
    ):
        raise SourceBuildConflictError("assembly genesis checkpoint is corrupt")
    return generation, cursor, gallery_count, file_count, byte_count, chain


def _lock_build_context(
    work: VNextUnitOfWork,
    *,
    generation: int,
    build_id: bytes,
    allow_sealed: bool = False,
) -> tuple[bytes, int, str, int]:
    mapping = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("source-build", 0, generation),
        "SELECT build_id FROM operational_source_build_generations "
        "WHERE generation = %s",
        (generation,),
    )
    if mapping != (build_id,):
        raise SourceBuildNotReadyError(
            "live ingest generation is not mapped to this source build"
        )
    working = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("source-build", 1, 1),
        "SELECT build_id, assigned_at FROM operational_source_working_builds "
        "WHERE slot = %s",
        (1,),
    )
    build = _load_source_build_or_conflict(work.connector, build_id)
    locked_state = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("source-build", 2, build_id),
        "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
        (build_id,),
    )
    if locked_state != (build.state,):
        raise SourceBuildConflictError("source build state changed while locking")
    scope = build.scope_key
    policy = build.manifest_policy_id
    state = build.state
    created_at = build.created_at
    if working != (build_id, created_at):
        raise SourceBuildNotReadyError("source build is not the exact working root")
    if state == "OPEN":
        if build.sealed_at is not None:
            raise SourceBuildConflictError("OPEN source build has sealed_at")
    elif allow_sealed and state == "SEALED":
        require_int63(build.sealed_at, field="source build sealed_at")
    else:
        raise SourceBuildNotReadyError("source build is not writable")
    return scope, policy, state, created_at


def _discovery_batch_key(
    plan: SourceDiscoveryPlan,
    build_id: bytes,
    start_generation: int,
) -> bytes:
    return _discovery_batch_key_values(
        plan.scan_attempt,
        plan.tree_observation_sha256,
        build_id,
        start_generation,
    )


def _discovery_batch_key_values(
    scan_attempt: bytes,
    tree_observation_sha256: bytes,
    build_id: bytes,
    start_generation: int,
) -> bytes:
    digest = sha256(_DISCOVERY_BATCH_PREFIX)
    digest.update(require_uuid16(scan_attempt, field="scan_attempt"))
    digest.update(
        require_digest32(
            tree_observation_sha256,
            field="tree_observation_sha256",
        )
    )
    digest.update(require_uuid16(build_id, field="build_id"))
    digest.update(
        require_positive_int63(
            start_generation,
            field="start_generation",
        ).to_bytes(8, "big")
    )
    return digest.digest()


def _require_discovery_plan_binding(
    connector: Any,
    *,
    build_id: bytes,
    plan: SourceDiscoveryPlan,
    start_generation: int,
    start_cursor: bytes,
    start_processed_count: int,
) -> None:
    if start_generation == 1:
        if start_cursor != b"" or start_processed_count != 0:
            raise SourceBuildConflictError("discovery genesis binding is corrupt")
        return
    previous = connector.fetch_one(
        _DISCOVERY_RECEIPT_SELECT + " WHERE build_id = %s AND start_generation = %s",
        (build_id, start_generation - 1),
    )
    if not previous:
        raise SourceBuildConflictError("discovery receipt chain is broken")
    receipt = _discovery_receipt_from_row(previous, replayed=False)
    if (
        receipt.batch_key != _discovery_batch_key(plan, build_id, start_generation - 1)
        or receipt.committed_generation != start_generation
        or receipt.next_cursor != start_cursor
        or receipt.next_processed_count != start_processed_count
        or receipt.next_state != "OPEN"
    ):
        raise SourceBuildConflictError("discovery plan differs from prior batches")


def _require_discovery_batch_binding(connector: Any, batch: DiscoveryBatch) -> None:
    if batch.start_generation == 1:
        if batch.start_cursor != b"" or batch.start_processed_count != 0:
            raise SourceBuildConflictError("discovery batch has corrupt genesis")
        return
    previous = connector.fetch_one(
        _DISCOVERY_RECEIPT_SELECT + " WHERE build_id = %s AND start_generation = %s",
        (batch.build_id, batch.start_generation - 1),
    )
    if not previous:
        raise SourceBuildConflictError("discovery receipt chain is broken")
    receipt = _discovery_receipt_from_row(previous, replayed=False)
    expected_key = _discovery_batch_key_values(
        batch.scan_attempt,
        batch.tree_observation_sha256,
        batch.build_id,
        batch.start_generation - 1,
    )
    if (
        receipt.batch_key != expected_key
        or receipt.committed_generation != batch.start_generation
        or receipt.next_cursor != batch.start_cursor
        or receipt.next_processed_count != batch.start_processed_count
        or receipt.next_state != "OPEN"
    ):
        raise SourceBuildConflictError("discovery plan or checkpoint chain changed")


def _validate_locator_upload(
    locator: PreparedDiscoveryLocator,
    plan: CanonicalValueUploadPlan,
) -> None:
    domain = require_ascii_bytes(
        plan.digest_domain,
        field="discovery locator plan domain",
        minimum=1,
        maximum=64,
    )
    value_sha256 = require_digest32(
        plan.value_sha256,
        field="discovery locator plan value_sha256",
    )
    byte_count = require_int63(
        plan.byte_count,
        field="discovery locator plan byte_count",
    )
    capability = getattr(plan, "_source_discovery_capability", None)
    if capability != (
        locator._plan_capability,
        locator.position,
        locator.payload_sha256,
    ):
        raise SourceBuildConflictError(
            "locator upload was not issued by this discovery plan"
        )
    if (
        domain != _LOCATOR_DOMAIN_BYTES
        or value_sha256 != locator.locator_sha256
        or byte_count != locator.payload_byte_count
    ):
        raise SourceBuildConflictError("locator upload differs from plan evidence")
    receipt = plan.tree_receipt
    receipt.__post_init__()
    if (
        receipt.value_sha256 != locator.locator_sha256
        or receipt.byte_count != locator.payload_byte_count
    ):
        raise SourceBuildConflictError("locator upload tree receipt differs")


def _require_sealed_root_identity(
    connector: Any,
    *,
    source_root_sha256: bytes,
    byte_count: int,
    root_page_sha256: bytes,
) -> None:
    identity = load_sealed_value_identity(
        connector,
        value_sha256=source_root_sha256,
    )
    if identity is None:
        raise SourceBuildNotReadyError("canonical source root is not sealed")
    _require_exact(
        "sealed source root",
        (
            identity.digest_domain,
            identity.byte_count,
            identity.root_page_sha256,
        ),
        (
            SOURCE_ROOT_DIGEST_DOMAIN.encode("ascii"),
            require_int63(byte_count, field="source root byte_count"),
            require_digest32(root_page_sha256, field="root_page_sha256"),
        ),
    )


def _require_sealed_locator(
    connector: Any,
    locator: PreparedDiscoveryLocator,
    plan: CanonicalValueUploadPlan,
) -> None:
    identity = load_sealed_value_identity(
        connector,
        value_sha256=locator.locator_sha256,
    )
    _require_exact(
        "sealed source locator",
        (
            (
                identity.digest_domain,
                identity.byte_count,
                identity.root_page_sha256,
            )
            if identity is not None
            else ()
        ),
        (_LOCATOR_DOMAIN_BYTES, locator.payload_byte_count, plan.root_page_sha256),
    )


def _load_gallery_identity(
    connector: Any,
    *,
    scope: bytes,
    locator_sha256: bytes,
    stable_key: bytes,
) -> int | None:
    try:
        rows = load_gallery_identity_candidates(
            connector,
            scope_key=scope,
            locator_sha256=locator_sha256,
            gallery_key=stable_key,
        )
    except CatalogIdentityCollisionError as error:
        raise SourceBuildConflictError(str(error)) from error
    if not rows:
        return None
    if len(rows) != 1:
        raise SourceBuildConflictError(
            "gallery natural identity and stable key name different surrogates"
        )
    identity = rows[0]
    if (
        identity.gallery_key,
        identity.scope_key,
        identity.locator_sha256,
    ) != (stable_key, scope, locator_sha256):
        raise SourceBuildConflictError(
            "gallery natural identity or stable key collision differs"
        )
    return identity.gallery_id


def _require_gallery_observation_allocator(connector: Any, gallery_id: int) -> None:
    row = connector.fetch_one(
        "SELECT next_observation_id FROM operational_gallery_observation_allocators "
        "WHERE gallery_id = %s",
        (gallery_id,),
    )
    if len(row) != 1:
        raise SourceBuildConflictError("gallery observation allocator is missing")
    require_positive_int63(row[0], field="next_observation_id")


def _validate_resolved_locator(
    batch: DiscoveryBatch,
    locator: PreparedDiscoveryLocator,
    evidence: ResolvedDiscoveryLocator,
) -> None:
    if not isinstance(evidence, ResolvedDiscoveryLocator):
        raise TypeError("resolved evidence must be ResolvedDiscoveryLocator")
    if evidence._batch_capability is not batch._batch_capability:
        raise SourceBuildConflictError("resolved locator belongs to another batch")
    expected = (
        batch.build_id,
        batch.batch_key,
        locator.position,
        locator.locator_sha256,
    )
    actual = (
        evidence.build_id,
        evidence.batch_key,
        evidence.position,
        evidence.locator_sha256,
    )
    _require_exact("resolved locator evidence", actual, expected)


def _discovery_receipt_from_row(
    row: tuple[Any, ...],
    *,
    replayed: bool,
) -> DiscoveryBatchReceipt:
    if len(row) != 12:
        raise SourceBuildConflictError("discovery receipt has the wrong shape")
    terminal_value = require_int63(row[9], field="discovery terminal")
    if terminal_value not in (0, 1):
        raise SourceBuildConflictError("discovery terminal is not a boolean byte")
    receipt = DiscoveryBatchReceipt(
        require_uuid16(row[0], field="build_id"),
        require_digest32(row[1], field="batch_key"),
        require_positive_int63(row[2], field="start_generation"),
        require_bounded_bytes(row[3], field="start_cursor", maximum=8),
        require_int63(row[4], field="start_processed_count"),
        require_bounded_bytes(row[5], field="next_cursor", maximum=8),
        require_int63(row[6], field="next_processed_count"),
        row[7],
        require_int63(row[8], field="row_count"),
        bool(terminal_value),
        require_positive_int63(row[10], field="committed_generation"),
        require_int63(row[11], field="committed_at"),
        replayed,
    )
    if receipt.committed_generation != receipt.start_generation + 1:
        raise SourceBuildConflictError("discovery receipt generation is corrupt")
    if (
        receipt.next_processed_count
        != receipt.start_processed_count + receipt.row_count
    ):
        raise SourceBuildConflictError("discovery receipt count is corrupt")
    if receipt.terminal:
        if (
            receipt.row_count != 0
            or receipt.next_state != "COMPLETE"
            or receipt.next_cursor != receipt.start_cursor
        ):
            raise SourceBuildConflictError("terminal discovery receipt is corrupt")
    elif receipt.row_count == 0 or receipt.next_state != "OPEN":
        raise SourceBuildConflictError("nonterminal discovery receipt is corrupt")
    return receipt


def _require_discovery_receipt_matches_batch(
    receipt: DiscoveryBatchReceipt,
    batch: DiscoveryBatch,
) -> None:
    row_count = len(batch.locators)
    next_cursor = (
        batch.start_cursor
        if batch.terminal
        else _encode_cursor(batch.locators[-1].position)
    )
    next_count = batch.start_processed_count + row_count
    expected = (
        batch.build_id,
        batch.batch_key,
        batch.start_generation,
        batch.start_cursor,
        batch.start_processed_count,
        next_cursor,
        next_count,
        "COMPLETE" if batch.terminal else "OPEN",
        row_count,
        batch.terminal,
        batch.start_generation + 1,
    )
    actual = (
        receipt.build_id,
        receipt.batch_key,
        receipt.start_generation,
        receipt.start_cursor,
        receipt.start_processed_count,
        receipt.next_cursor,
        receipt.next_processed_count,
        receipt.next_state,
        receipt.row_count,
        receipt.terminal,
        receipt.committed_generation,
    )
    _require_exact("replayed discovery receipt", actual, expected)


def _require_discovery_replay_state(
    connector: Any,
    *,
    checkpoint: tuple[Any, ...],
    receipt: DiscoveryBatchReceipt,
    batch: DiscoveryBatch,
) -> None:
    generation, cursor, count = _validate_discovery_checkpoint(
        checkpoint[:4],
        require_open=False,
    )
    if (
        generation < receipt.committed_generation
        or count < receipt.next_processed_count
    ):
        raise SourceBuildConflictError("discovery checkpoint precedes its receipt")
    if generation == receipt.committed_generation:
        _require_exact(
            "replayed discovery checkpoint",
            (cursor, count, checkpoint[3]),
            (
                receipt.next_cursor,
                receipt.next_processed_count,
                receipt.next_state,
            ),
        )
    else:
        latest = connector.fetch_one(
            _DISCOVERY_RECEIPT_SELECT
            + " WHERE build_id = %s AND start_generation = %s",
            (batch.build_id, generation - 1),
        )
        if not latest:
            raise SourceBuildConflictError(
                "discovery checkpoint has no latest receipt authority"
            )
        latest_receipt = _discovery_receipt_from_row(latest, replayed=False)
        _require_exact(
            "latest discovery checkpoint receipt",
            (
                latest_receipt.committed_generation,
                latest_receipt.next_cursor,
                latest_receipt.next_processed_count,
                latest_receipt.next_state,
            ),
            (generation, cursor, count, checkpoint[3]),
        )
    if receipt.terminal:
        discovery = connector.fetch_one(
            "SELECT build_id, scan_attempt, gallery_count, "
            "tree_observation_sha256, completed_at "
            "FROM catalog_source_build_discoveries WHERE build_id = %s",
            (batch.build_id,),
        )
        _require_exact(
            "replayed source build discovery",
            discovery,
            (
                batch.build_id,
                batch.scan_attempt,
                batch.gallery_count,
                batch.tree_observation_sha256,
                receipt.committed_at,
            ),
        )


def _load_assembly_page(
    connector: Any,
    *,
    build_id: bytes,
    cursor: bytes,
    manifest_policy_id: int,
) -> tuple[tuple[Any, ...], ...]:
    select = (
        "SELECT e.position, e.gallery_id, gik.gallery_key, gic.scope_key, "
        "member.observation_id, observation.observation_identity_sha256, "
        "stat_file.file_count, stat_byte.byte_count, manifest_seal.manifest_policy_id, "
        "manifest.manifest_sha256 "
        "FROM catalog_source_build_expected_gallery e "
        "LEFT JOIN catalog_gallery_identity_seals gis "
        "ON gis.gallery_id = e.gallery_id "
        "LEFT JOIN catalog_gallery_identity_anchors gia "
        "ON gia.gallery_id = gis.gallery_id "
        "LEFT JOIN catalog_gallery_identity_coordinates gic "
        "ON gic.gallery_id = gis.gallery_id "
        "LEFT JOIN catalog_gallery_identity_gallery_keys gik "
        "ON gik.gallery_id = gis.gallery_id "
        "LEFT JOIN catalog_source_build_galleries member "
        "ON member.build_id = e.build_id AND member.gallery_id = e.gallery_id "
        "LEFT JOIN catalog_gallery_observations observation "
        "ON observation.gallery_id = member.gallery_id "
        "AND observation.observation_id = member.observation_id "
        "LEFT JOIN catalog_gallery_observation_stat_seals stat_seal "
        "ON stat_seal.gallery_id = member.gallery_id "
        "AND stat_seal.observation_id = member.observation_id "
        "LEFT JOIN catalog_gallery_observation_stat_file_counts stat_file "
        "ON stat_file.gallery_id = stat_seal.gallery_id "
        "AND stat_file.observation_id = stat_seal.observation_id "
        "LEFT JOIN catalog_gallery_observation_stat_byte_counts stat_byte "
        "ON stat_byte.gallery_id = stat_seal.gallery_id "
        "AND stat_byte.observation_id = stat_seal.observation_id "
        "LEFT JOIN catalog_gallery_manifest_seals manifest_seal "
        "ON manifest_seal.gallery_id = member.gallery_id "
        "AND manifest_seal.observation_id = member.observation_id "
        "AND manifest_seal.manifest_policy_id = %s "
        "LEFT JOIN catalog_gallery_manifest_manifest_sha256s manifest "
        "ON manifest.gallery_id = manifest_seal.gallery_id "
        "AND manifest.observation_id = manifest_seal.observation_id "
        "AND manifest.manifest_policy_id = manifest_seal.manifest_policy_id "
    )
    if cursor:
        position = int.from_bytes(cursor, "big")
        rows = connector.fetch_all(
            select + "WHERE e.build_id = %s AND e.position > %s "
            "ORDER BY e.position LIMIT %s",
            (manifest_policy_id, build_id, position, _BATCH_LIMIT),
        )
    else:
        rows = connector.fetch_all(
            select + "WHERE e.build_id = %s ORDER BY e.position LIMIT %s",
            (manifest_policy_id, build_id, _BATCH_LIMIT),
        )
    if len(rows) > _BATCH_LIMIT:
        raise SourceBuildConflictError("assembly query exceeded its hard batch cap")
    return tuple(rows)


def _validate_assembly_row(
    row: tuple[Any, ...],
    *,
    expected_position: int,
    scope: bytes,
    manifest_policy_id: int,
) -> tuple[int, int, bytes, bytes, int, bytes, int, int, int, bytes]:
    if len(row) != 10 or any(value is None for value in row):
        raise SourceBuildNotReadyError(
            "expected gallery lacks its linked observation, stat, or manifest"
        )
    position = require_int63(row[0], field="expected position")
    gallery_id = require_positive_int63(row[1], field="gallery_id")
    stable_key = require_digest32(row[2], field="gallery_key")
    row_scope = require_digest32(row[3], field="gallery scope_key")
    observation_id = require_positive_int63(row[4], field="observation_id")
    observation_identity = require_digest32(
        row[5],
        field="observation_identity_sha256",
    )
    file_count = require_int63(row[6], field="gallery file_count")
    byte_count = require_int63(row[7], field="gallery byte_count")
    policy = require_positive_int63(row[8], field="gallery manifest_policy_id")
    manifest = require_digest32(row[9], field="gallery manifest_sha256")
    if position != expected_position:
        raise SourceBuildConflictError("expected gallery positions are not contiguous")
    if row_scope != scope:
        raise SourceBuildConflictError("gallery scope differs from source build scope")
    if policy != manifest_policy_id:
        raise SourceBuildConflictError("gallery manifest uses another policy")
    return (
        position,
        gallery_id,
        stable_key,
        row_scope,
        observation_id,
        observation_identity,
        file_count,
        byte_count,
        policy,
        manifest,
    )


def _manifest_chain_step(
    prior_chain: bytes,
    *,
    position: int,
    gallery_key_bytes: bytes,
    observation_identity_sha256: bytes,
    gallery_manifest_sha256: bytes,
    file_count: int,
    byte_count: int,
) -> bytes:
    digest = sha256(_MANIFEST_AUDIT_PREFIX)
    digest.update(require_digest32(prior_chain, field="prior manifest chain"))
    digest.update(require_int63(position, field="position").to_bytes(8, "big"))
    digest.update(require_digest32(gallery_key_bytes, field="gallery_key"))
    digest.update(
        require_digest32(
            observation_identity_sha256,
            field="observation_identity_sha256",
        )
    )
    digest.update(
        require_digest32(
            gallery_manifest_sha256,
            field="gallery_manifest_sha256",
        )
    )
    digest.update(require_int63(file_count, field="file_count").to_bytes(8, "big"))
    digest.update(require_int63(byte_count, field="byte_count").to_bytes(8, "big"))
    return digest.digest()


def _assembly_receipt_from_row(
    row: tuple[Any, ...],
    *,
    replayed: bool,
) -> AssemblyBatchReceipt:
    if len(row) != 18:
        raise SourceBuildConflictError("assembly receipt has the wrong shape")
    terminal_value = require_int63(row[15], field="assembly terminal")
    if terminal_value not in (0, 1):
        raise SourceBuildConflictError("assembly terminal is not a boolean byte")
    receipt = AssemblyBatchReceipt(
        require_uuid16(row[0], field="build_id"),
        require_digest32(row[1], field="batch_key"),
        require_positive_int63(row[2], field="start_generation"),
        require_bounded_bytes(row[3], field="start_cursor", maximum=8),
        require_int63(row[4], field="start_gallery_count"),
        require_int63(row[5], field="start_file_count"),
        require_int63(row[6], field="start_byte_count"),
        require_digest32(row[7], field="start_manifest_chain_sha256"),
        require_bounded_bytes(row[8], field="next_cursor", maximum=8),
        require_int63(row[9], field="next_gallery_count"),
        require_int63(row[10], field="next_file_count"),
        require_int63(row[11], field="next_byte_count"),
        require_digest32(row[12], field="next_manifest_chain_sha256"),
        row[13],
        require_int63(row[14], field="row_count"),
        bool(terminal_value),
        require_positive_int63(row[16], field="committed_generation"),
        require_int63(row[17], field="committed_at"),
        replayed,
    )
    if (
        receipt.committed_generation != receipt.start_generation + 1
        or receipt.next_gallery_count != receipt.start_gallery_count + receipt.row_count
        or receipt.next_file_count < receipt.start_file_count
        or receipt.next_byte_count < receipt.start_byte_count
    ):
        raise SourceBuildConflictError("assembly receipt transition is corrupt")
    if receipt.terminal:
        if (
            receipt.row_count != 0
            or receipt.next_state != "COMPLETE"
            or receipt.next_cursor != receipt.start_cursor
            or receipt.next_gallery_count != receipt.start_gallery_count
            or receipt.next_file_count != receipt.start_file_count
            or receipt.next_byte_count != receipt.start_byte_count
            or receipt.next_manifest_chain_sha256 != receipt.start_manifest_chain_sha256
        ):
            raise SourceBuildConflictError("terminal assembly receipt is corrupt")
    elif receipt.row_count == 0 or receipt.next_state != "OPEN":
        raise SourceBuildConflictError("nonterminal assembly receipt is corrupt")
    return receipt


def _validate_assembly_replay(
    connector: Any,
    *,
    receipt: AssemblyBatchReceipt,
    checkpoint: tuple[Any, ...],
    build_state: str,
    scope: bytes,
    manifest_policy_id: int,
    created_at: int,
) -> None:
    (
        current_generation,
        current_cursor,
        current_gallery_count,
        current_file_count,
        current_byte_count,
        current_chain,
    ) = _validate_assembly_checkpoint(checkpoint[:7], require_open=False)
    if (
        current_generation < receipt.committed_generation
        or current_gallery_count < receipt.next_gallery_count
        or current_file_count < receipt.next_file_count
        or current_byte_count < receipt.next_byte_count
    ):
        raise SourceBuildConflictError("assembly checkpoint precedes its receipt")
    current_tuple = (
        current_generation,
        current_cursor,
        current_gallery_count,
        current_file_count,
        current_byte_count,
        current_chain,
        checkpoint[6],
    )
    if current_generation == receipt.committed_generation:
        _require_exact(
            "replayed assembly checkpoint",
            current_tuple,
            (
                receipt.committed_generation,
                receipt.next_cursor,
                receipt.next_gallery_count,
                receipt.next_file_count,
                receipt.next_byte_count,
                receipt.next_manifest_chain_sha256,
                receipt.next_state,
            ),
        )
    else:
        latest = connector.fetch_one(
            _ASSEMBLY_RECEIPT_SELECT + " WHERE build_id = %s AND start_generation = %s",
            (receipt.build_id, current_generation - 1),
        )
        if not latest:
            raise SourceBuildConflictError(
                "assembly checkpoint has no latest receipt authority"
            )
        latest_receipt = _assembly_receipt_from_row(latest, replayed=False)
        _require_exact(
            "latest assembly checkpoint receipt",
            current_tuple,
            (
                latest_receipt.committed_generation,
                latest_receipt.next_cursor,
                latest_receipt.next_gallery_count,
                latest_receipt.next_file_count,
                latest_receipt.next_byte_count,
                latest_receipt.next_manifest_chain_sha256,
                latest_receipt.next_state,
            ),
        )
    if receipt.terminal:
        try:
            manifest = load_build_manifest_family(
                connector,
                build_id=receipt.build_id,
            )
        except ManifestFamilyCollisionError as error:
            raise SourceBuildConflictError(str(error)) from error
        if manifest is None:
            raise SourceBuildConflictError("replayed build manifest is missing")
        _require_exact(
            "replayed build manifest",
            (
                manifest.manifest_sha256,
                manifest.gallery_count,
                manifest.file_count,
                manifest.byte_count,
                manifest.computed_at,
            ),
            (
                receipt.next_manifest_chain_sha256,
                receipt.next_gallery_count,
                receipt.next_file_count,
                receipt.next_byte_count,
                receipt.committed_at,
            ),
        )
        if build_state != "SEALED":
            raise SourceBuildConflictError("terminal assembly build is not SEALED")
        build = _load_source_build_or_conflict(connector, receipt.build_id)
        _require_exact(
            "replayed sealed source build",
            (
                build.scope_key,
                build.manifest_policy_id,
                build.state,
                build.created_at,
                build.sealed_at,
            ),
            (
                scope,
                manifest_policy_id,
                "SEALED",
                created_at,
                receipt.committed_at,
            ),
        )


def _manifest_policy_id(connector: Any) -> int:
    try:
        policy = load_manifest_policy_by_natural(
            connector,
            manifest_algorithm_version=_MANIFEST_ALGORITHM_VERSION,
            file_order_version=_FILE_ORDER_VERSION,
        )
    except CatalogRegistryNotReadyError as error:
        raise SourceBuildNotReadyError(
            "exact v1 manifest policy tuple is not sealed"
        ) from error
    except CatalogRegistryConflictError as error:
        raise SourceBuildConflictError(
            "exact v1 manifest policy tuple is corrupt"
        ) from error
    return policy.manifest_policy_id


def _require_registry_value(
    connector: Any,
    *,
    table: str,
    column: str,
    expected: bytes,
) -> None:
    # table and column are closed constants from the two callsites above.
    row = connector.fetch_one(
        f"SELECT {column} FROM {table} WHERE {column} = %s",
        (expected,),
    )
    if row != (expected,):
        raise SourceBuildNotReadyError(
            f"{table} is not the exact closed schema-v1 registry"
        )


def _insert_or_validate_scope(
    connector: Any,
    *,
    scope: bytes,
    source_root_sha256: bytes,
) -> None:
    try:
        result = ensure_source_scope(
            connector,
            source_provider=_FILESYSTEM_BYTES,
            source_root_sha256=source_root_sha256,
            identity_policy_version=_IDENTITY_POLICY_VERSION,
        )
    except CatalogRegistryNotReadyError as error:
        raise SourceBuildNotReadyError(
            "source scope family is incomplete or unsealed"
        ) from error
    except CatalogRegistryConflictError as error:
        raise SourceBuildConflictError(
            "source scope identity conflicts with durable registry facts"
        ) from error
    _require_exact("derived source scope key", (result.record.scope_key,), (scope,))


def _insert_or_validate_build(
    connector: Any,
    *,
    build_id: bytes,
    scope: bytes,
    manifest_policy_id: int,
    created_at: int,
) -> None:
    try:
        ensure_source_build_family(
            connector,
            build_id=build_id,
            scope_key=scope,
            manifest_policy_id=manifest_policy_id,
            created_at=created_at,
        )
    except ManifestFamilyCollisionError as error:
        raise SourceBuildConflictError(str(error)) from error


def _persist_source_build_discovery(
    connector: Any,
    *,
    build_id: bytes,
    scan_attempt: bytes,
    gallery_count: int,
    tree_observation_sha256: bytes,
    completed_at: int,
) -> None:
    key = (build_id,)
    _insert_or_validate(
        connector,
        label="source build discovery anchor",
        select_sql=(
            "SELECT build_id FROM catalog_source_build_discovery_anchors "
            "WHERE build_id = %s"
        ),
        select_data=key,
        insert_sql=(
            "INSERT INTO catalog_source_build_discovery_anchors (build_id) VALUES (%s)"
        ),
        expected=key,
    )
    for label, select_sql, insert_sql, value in (
        (
            "source build discovery scan attempt",
            "SELECT build_id, scan_attempt "
            "FROM catalog_source_build_discovery_scan_attempts "
            "WHERE build_id = %s",
            "INSERT INTO catalog_source_build_discovery_scan_attempts "
            "(build_id, scan_attempt) VALUES (%s, %s)",
            scan_attempt,
        ),
        (
            "source build discovery gallery count",
            "SELECT build_id, gallery_count "
            "FROM catalog_source_build_discovery_gallery_counts "
            "WHERE build_id = %s",
            "INSERT INTO catalog_source_build_discovery_gallery_counts "
            "(build_id, gallery_count) VALUES (%s, %s)",
            gallery_count,
        ),
        (
            "source build discovery tree observation digest",
            "SELECT build_id, tree_observation_sha256 "
            "FROM catalog_source_build_discovery_tree_observation_sha256s "
            "WHERE build_id = %s",
            "INSERT INTO catalog_source_build_discovery_tree_observation_sha256s "
            "(build_id, tree_observation_sha256) VALUES (%s, %s)",
            tree_observation_sha256,
        ),
        (
            "source build discovery completed at",
            "SELECT build_id, completed_at "
            "FROM catalog_source_build_discovery_completed_ats "
            "WHERE build_id = %s",
            "INSERT INTO catalog_source_build_discovery_completed_ats "
            "(build_id, completed_at) VALUES (%s, %s)",
            completed_at,
        ),
    ):
        _insert_or_validate(
            connector,
            label=label,
            select_sql=select_sql,
            select_data=key,
            insert_sql=insert_sql,
            expected=(build_id, value),
        )
    _insert_or_validate(
        connector,
        label="source build discovery seal",
        select_sql=(
            "SELECT build_id FROM catalog_source_build_discovery_seals "
            "WHERE build_id = %s"
        ),
        select_data=key,
        insert_sql=(
            "INSERT INTO catalog_source_build_discovery_seals (build_id) VALUES (%s)"
        ),
        expected=key,
    )


def _insert_or_validate(
    connector: Any,
    *,
    label: str,
    select_sql: str,
    select_data: tuple[Any, ...],
    insert_sql: str,
    expected: tuple[Any, ...],
) -> None:
    row = connector.fetch_one(select_sql, select_data)
    if row:
        _require_exact(label, row, expected)
    else:
        connector.execute(insert_sql, expected)


def _lock_source_head(
    work: VNextUnitOfWork,
    channel: bytes,
) -> _SourceHead | None:
    row = work.lock_row(
        LockRank.HEAD,
        encode_lock_key("source-build-head", channel),
        "SELECT registry.channel, head.receipt_id, seal.receipt_id, "
        "source.source_revision, generation.generation, committed.committed_at, "
        "descriptor.channel "
        "FROM catalog_channel_registry AS registry "
        f"LEFT JOIN {_PUBLICATION_COMMIT_HEAD_TABLE} AS head "
        "ON head.channel = registry.channel "
        f"LEFT JOIN {_PUBLICATION_COMMIT_SEAL_TABLE} AS seal "
        "ON seal.receipt_id = head.receipt_id "
        f"LEFT JOIN {_PUBLICATION_COMMIT_SOURCE_TABLE} AS source "
        "ON source.receipt_id = head.receipt_id "
        f"LEFT JOIN {_PUBLICATION_COMMIT_GENERATION_TABLE} AS generation "
        "ON generation.receipt_id = head.receipt_id "
        f"LEFT JOIN {_PUBLICATION_COMMIT_COMMITTED_AT_TABLE} AS committed "
        "ON committed.receipt_id = head.receipt_id "
        f"LEFT JOIN {_SOURCE_REVISION_CHANNEL_TABLE} AS descriptor "
        "ON descriptor.source_revision = source.source_revision "
        "WHERE registry.channel = %s",
        (channel,),
    )
    if len(row) != 7 or row[0] != channel:
        raise SourceBuildConflictError(
            "source channel registry is missing or malformed"
        )
    members = row[1:]
    if all(value is None for value in members):
        return None
    if any(value is None for value in members):
        raise SourceBuildConflictError("source head vertical family is incomplete")
    if row[1] != row[2]:
        raise SourceBuildConflictError("common publication head is not sealed")
    descriptor_channel = require_bounded_bytes(
        row[6],
        field="source head descriptor channel",
        minimum=1,
        maximum=64,
    )
    if descriptor_channel != channel:
        raise SourceBuildConflictError(
            "source head points to a revision from another channel"
        )
    return _SourceHead(
        require_uuid16(row[1], field="source head receipt_id"),
        require_positive_int63(row[3], field="source head revision"),
        require_positive_int63(row[4], field="source head generation"),
        require_int63(row[5], field="source head committed_at"),
    )


def _validate_build_base_source(
    connector: Any,
    *,
    build_id: bytes,
) -> None:
    row = connector.fetch_one(
        f"SELECT base.base_receipt_id, seal.receipt_id, source.source_revision, "
        "generation.generation, descriptor.channel "
        f"FROM {_SOURCE_BUILD_BASE_COMMIT_TABLE} AS base "
        f"LEFT JOIN {_PUBLICATION_COMMIT_SEAL_TABLE} AS seal "
        "ON seal.receipt_id = base.base_receipt_id "
        f"LEFT JOIN {_PUBLICATION_COMMIT_SOURCE_TABLE} AS source "
        "ON source.receipt_id = base.base_receipt_id "
        f"LEFT JOIN {_PUBLICATION_COMMIT_GENERATION_TABLE} AS generation "
        "ON generation.receipt_id = base.base_receipt_id "
        f"LEFT JOIN {_SOURCE_REVISION_CHANNEL_TABLE} AS descriptor "
        "ON descriptor.source_revision = source.source_revision "
        "WHERE base.build_id = %s",
        (build_id,),
    )
    if not row:
        return
    if len(row) != 5 or any(value is None for value in row):
        raise SourceBuildConflictError(
            "source build base commit lacks its immutable sealed authority"
        )
    receipt_id = require_uuid16(row[0], field="source build base receipt_id")
    if row[1] != receipt_id:
        raise SourceBuildConflictError("source build base commit is not sealed")
    require_positive_int63(row[2], field="source build base source revision")
    require_positive_int63(row[3], field="source build base generation")
    if row[4] != _DEFAULT_CHANNEL:
        raise SourceBuildConflictError(
            "source build base commit belongs to another channel"
        )


def _validate_existing_handoff(
    connector: Any,
    *,
    build_id: bytes,
    generation: int,
    scope: bytes,
    source_root_sha256: bytes,
    manifest_policy_id: int,
    working: tuple[Any, ...],
) -> None:
    try:
        scope_record = load_source_scope(connector, scope)
    except CatalogRegistryNotReadyError as error:
        raise SourceBuildNotReadyError(
            "replayed source scope is absent or unsealed"
        ) from error
    except CatalogRegistryConflictError as error:
        raise SourceBuildConflictError("replayed source scope is corrupt") from error
    _require_exact(
        "replayed source scope",
        (
            scope_record.scope_key,
            scope_record.source_provider,
            scope_record.source_root_sha256,
            scope_record.identity_policy_version,
        ),
        (scope, _FILESYSTEM_BYTES, source_root_sha256, _IDENTITY_POLICY_VERSION),
    )
    build = _load_source_build_or_conflict(connector, build_id)
    created_at = build.created_at
    _require_exact(
        "replayed source build immutable fields",
        (build.scope_key, build.manifest_policy_id),
        (scope, manifest_policy_id),
    )
    if build.state == "OPEN":
        if build.sealed_at is not None:
            raise SourceBuildConflictError("replayed OPEN build has sealed_at")
    elif build.state == "SEALED":
        require_int63(build.sealed_at, field="replayed build sealed_at")
        try:
            manifest = load_build_manifest_family(connector, build_id=build_id)
        except ManifestFamilyCollisionError as error:
            raise SourceBuildConflictError(str(error)) from error
        if manifest is None:
            raise SourceBuildConflictError("replayed SEALED build has no manifest")
        manifest.__post_init__()
    else:
        raise SourceBuildConflictError("replayed source build is ABANDONED")
    channel = connector.fetch_one(
        "SELECT channel FROM catalog_source_build_channel WHERE build_id = %s",
        (build_id,),
    )
    _require_exact("replayed source channel", channel, (_DEFAULT_CHANNEL,))
    _validate_build_base_source(connector, build_id=build_id)
    mapping = connector.fetch_one(
        "SELECT build_id, generation FROM operational_source_build_generations "
        "WHERE generation = %s",
        (generation,),
    )
    _require_exact("replayed source generation", mapping, (build_id, generation))
    _require_exact(
        "replayed source working build",
        working,
        (1, build_id, created_at),
    )
    _require_checkpoint_pair(
        connector,
        build_id=build_id,
        build_state=build.state,
    )


def _require_exact(
    label: str,
    actual: tuple[Any, ...],
    expected: tuple[Any, ...],
) -> None:
    if actual != expected:
        raise SourceBuildConflictError(
            f"{label} conflicts with its immutable exact tuple"
        )


# Validate closed constants at import without accepting text from callers.
require_ascii_bytes(
    _FILESYSTEM_BYTES,
    field="filesystem provider",
    minimum=1,
    maximum=64,
)
require_ascii_bytes(
    _DEFAULT_CHANNEL,
    field="default channel",
    minimum=1,
    maximum=64,
)
