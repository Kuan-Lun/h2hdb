"""Canonical source-locator and gallery-identity handoff for vNext.

The public command contains only the exact source-root-relative locator.  Its
canonical digest, stable gallery key, numeric gallery ID, and per-gallery
observation allocator are derived by the repository.  A caller therefore
cannot promote a surrogate ID or a leaf name to identity authority.

Canonical bytes are uploaded and sealed through
``CanonicalValueRepository`` before this short transaction.  This handoff
then creates (or exact-compares) the locator type row, gallery identity, and
observation allocator before releasing only the current ingest generation's
upload claim.
"""

from __future__ import annotations

__all__ = [
    "GalleryIdentityConflictError",
    "GalleryIdentityHandoff",
    "GalleryIdentityNotReadyError",
    "GalleryIdentityRepository",
    "SourceLocatorCommand",
]

from dataclasses import dataclass, field
from typing import Any

from .vnext_allocator_repository import IdentityStream, VNextAllocatorRepository
from .vnext_canonical_value_repository import CanonicalValueUploadPlan
from .vnext_domains import (
    require_ascii_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from .vnext_identity import (
    gallery_key,
    iter_source_relative_locator_payload,
    source_relative_locator_digest,
    validate_source_relative_locator_parts,
)
from .vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from .vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
)
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_LOCATOR_DOMAIN = "source_relative_locator_v1"
_LOCATOR_DOMAIN_BYTES = _LOCATOR_DOMAIN.encode("ascii")


class GalleryIdentityConflictError(RuntimeError):
    """A digest, natural key, surrogate, or immutable tuple disagrees."""


class GalleryIdentityNotReadyError(RuntimeError):
    """The live build, canonical seal, or required upload claim is absent."""


@dataclass(frozen=True, slots=True)
class SourceLocatorCommand:
    """Exact nested locator command with transaction-independent receipts."""

    components: tuple[str, ...]
    locator_sha256: bytes = field(init=False)
    payload_byte_count: int = field(init=False)
    source_gallery_name: bytes = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.components, tuple):
            raise TypeError("components must be an exact tuple")
        receipt = validate_source_relative_locator_parts(
            iter_source_relative_locator_payload(self.components)
        )
        digest = source_relative_locator_digest(_LOCATOR_DOMAIN, self.components)
        leaf = self.components[-1].encode("utf-8", errors="strict")
        object.__setattr__(self, "locator_sha256", digest)
        object.__setattr__(self, "payload_byte_count", receipt.payload_byte_count)
        object.__setattr__(self, "source_gallery_name", leaf)

    def prepare_upload(self) -> CanonicalValueUploadPlan:
        """Spool the exact canonical locator for bounded database upload."""

        return CanonicalValueUploadPlan.from_parts(
            _LOCATOR_DOMAIN,
            iter_source_relative_locator_payload(self.components),
        )


@dataclass(frozen=True, slots=True)
class GalleryIdentityHandoff:
    build_id: bytes
    gallery_id: int
    gallery_key: bytes
    scope_key: bytes
    locator_sha256: bytes
    replayed: bool

    def __post_init__(self) -> None:
        require_uuid16(self.build_id, field="build_id")
        require_positive_int63(self.gallery_id, field="gallery_id")
        require_digest32(self.gallery_key, field="gallery_key")
        require_digest32(self.scope_key, field="scope_key")
        require_digest32(self.locator_sha256, field="locator_sha256")
        if not isinstance(self.replayed, bool):
            raise TypeError("replayed must be bool")


class GalleryIdentityRepository:
    """Transaction-local consumer of one sealed canonical locator."""

    @staticmethod
    def handoff_locator(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        build_id: bytes,
        command: SourceLocatorCommand,
        locator_plan: CanonicalValueUploadPlan,
        now: int,
    ) -> GalleryIdentityHandoff:
        build = require_uuid16(build_id, field="build_id")
        if type(command) is not SourceLocatorCommand:
            raise TypeError("command must be an exact SourceLocatorCommand")
        # Recompute every init=False authority instead of trusting a frozen
        # command that may have been changed with ``object.__setattr__``.
        command.__post_init__()
        if type(locator_plan) is not CanonicalValueUploadPlan:
            raise TypeError("locator_plan must be an exact CanonicalValueUploadPlan")
        timestamp = require_int63(now, field="now")
        generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        scope = _lock_working_build(work, generation=generation, build_id=build)

        _validate_plan(command, locator_plan)
        connector = work.connector
        claim = work.lock_row(
            LockRank.CHECKPOINT,
            encode_lock_key(
                "source-locator-upload",
                generation,
                command.locator_sha256,
            ),
            "SELECT generation, value_sha256 "
            "FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (generation, command.locator_sha256),
        )
        if claim and claim != (generation, command.locator_sha256):
            raise GalleryIdentityConflictError("locator upload claim differs")

        _require_sealed_locator(connector, command, locator_plan)
        stable_key = gallery_key(scope, command.locator_sha256)
        existing = _load_gallery_identity(
            connector,
            scope=scope,
            locator_sha256=command.locator_sha256,
            stable_key=stable_key,
        )
        if existing is not None:
            gallery_id = existing
            _require_observation_allocator(connector, gallery_id)
            _release_optional_claim(
                connector,
                generation=generation,
                locator_sha256=command.locator_sha256,
                claim=claim,
            )
            return GalleryIdentityHandoff(
                build,
                gallery_id,
                stable_key,
                scope,
                command.locator_sha256,
                True,
            )

        # Serialize all new gallery identities with the portable global
        # allocator.  Re-read after the lock because another source scope may
        # have installed the shared locator type row while we waited.
        gallery_id = VNextAllocatorRepository.allocate_identity(
            work,
            IdentityStream.GALLERY,
            updated_at=timestamp,
        )
        existing = _load_gallery_identity(
            connector,
            scope=scope,
            locator_sha256=command.locator_sha256,
            stable_key=stable_key,
        )
        if existing is not None:
            _require_observation_allocator(connector, existing)
            _release_optional_claim(
                connector,
                generation=generation,
                locator_sha256=command.locator_sha256,
                claim=claim,
            )
            return GalleryIdentityHandoff(
                build,
                existing,
                stable_key,
                scope,
                command.locator_sha256,
                True,
            )

        locator_row = connector.fetch_one(
            "SELECT source_gallery_name FROM catalog_source_locator_identity "
            "WHERE locator_sha256 = %s",
            (command.locator_sha256,),
        )
        if locator_row:
            _require_exact(
                "source locator leaf",
                locator_row,
                (command.source_gallery_name,),
            )
        else:
            if not claim:
                raise GalleryIdentityNotReadyError(
                    "new source locator requires its current upload claim"
                )
            connector.execute(
                "INSERT INTO catalog_source_locator_identity "
                "(locator_sha256, source_gallery_name) VALUES (%s, %s)",
                (command.locator_sha256, command.source_gallery_name),
            )

        expected_gallery = (
            gallery_id,
            stable_key,
            scope,
            command.locator_sha256,
        )
        connector.execute(
            "INSERT INTO catalog_gallery_identities "
            "(gallery_id, gallery_key, scope_key, locator_sha256) "
            "VALUES (%s, %s, %s, %s)",
            expected_gallery,
        )
        connector.execute(
            "INSERT INTO operational_gallery_observation_allocators "
            "(gallery_id, next_observation_id, updated_at) VALUES (%s, 1, %s)",
            (gallery_id, timestamp),
        )
        _release_optional_claim(
            connector,
            generation=generation,
            locator_sha256=command.locator_sha256,
            claim=claim,
        )
        return GalleryIdentityHandoff(
            build,
            gallery_id,
            stable_key,
            scope,
            command.locator_sha256,
            False,
        )


def _authorize(
    work: VNextUnitOfWork,
    gate_lease: GateLease,
    ingest_turn: IngestTurn,
    *,
    now: int,
) -> int:
    gate = MaintenanceGateRepository.lock_and_require_live(work, gate_lease, now=now)
    if gate.mode is not GateMode.SHARED:
        raise GalleryIdentityNotReadyError(
            "gallery identity handoff requires a live SHARED maintenance gate"
        )
    turn = IngestFenceRepository.lock_and_require_live(work, ingest_turn, now=now)
    return require_int63(turn.generation, field="ingest generation")


def _lock_working_build(
    work: VNextUnitOfWork,
    *,
    generation: int,
    build_id: bytes,
) -> bytes:
    mapping = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("source-build", 0, generation),
        "SELECT build_id FROM operational_source_build_generations "
        "WHERE generation = %s",
        (generation,),
    )
    if mapping != (build_id,):
        raise GalleryIdentityNotReadyError(
            "live ingest generation is not mapped to this source build"
        )
    working = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("source-build", 1, 1),
        "SELECT build_id FROM operational_source_working_builds WHERE slot = 1",
    )
    if working != (build_id,):
        raise GalleryIdentityNotReadyError("source build is not the working root")
    row = work.connector.fetch_one(
        "SELECT scope_key, state FROM catalog_source_builds WHERE build_id = %s",
        (build_id,),
    )
    if len(row) != 2 or row[1] != "OPEN":
        raise GalleryIdentityNotReadyError("source build is not OPEN")
    return require_digest32(row[0], field="source build scope_key")


def _validate_plan(
    command: SourceLocatorCommand,
    plan: CanonicalValueUploadPlan,
) -> None:
    domain = require_ascii_bytes(
        plan.digest_domain,
        field="locator plan digest_domain",
        minimum=1,
        maximum=64,
    )
    value = require_digest32(plan.value_sha256, field="locator plan value_sha256")
    count = require_int63(plan.byte_count, field="locator plan byte_count")
    if domain != _LOCATOR_DOMAIN_BYTES:
        raise GalleryIdentityConflictError("locator upload uses the wrong domain")
    if value != command.locator_sha256:
        raise GalleryIdentityConflictError("locator upload is not the exact command")
    if count != command.payload_byte_count:
        raise GalleryIdentityConflictError("locator upload byte count differs")
    # ``tree_receipt`` is produced only after page iteration reached exact EOF.
    receipt = plan.tree_receipt
    if (
        receipt.value_sha256 != command.locator_sha256
        or receipt.byte_count != command.payload_byte_count
    ):
        raise GalleryIdentityConflictError("locator tree receipt differs")
    # The command was fully codec-validated before the transaction and its
    # domain-separated digest is the canonical collision-free identity.  Do
    # not replay the potentially unbounded spool in this handoff transaction.


def _require_sealed_locator(
    connector: Any,
    command: SourceLocatorCommand,
    plan: CanonicalValueUploadPlan,
) -> None:
    row = connector.fetch_one(
        "SELECT a.digest_domain, a.byte_count, i.root_page_sha256 "
        "FROM catalog_canonical_value_allocations a "
        "JOIN catalog_canonical_value_identities i "
        "ON i.value_sha256 = a.value_sha256 WHERE a.value_sha256 = %s",
        (command.locator_sha256,),
    )
    expected = (
        _LOCATOR_DOMAIN_BYTES,
        command.payload_byte_count,
        plan.root_page_sha256,
    )
    _require_exact("sealed source locator", row, expected)


def _load_gallery_identity(
    connector: Any,
    *,
    scope: bytes,
    locator_sha256: bytes,
    stable_key: bytes,
) -> int | None:
    expected_tail = (stable_key, scope, locator_sha256)
    by_natural = connector.fetch_one(
        "SELECT gallery_id, gallery_key, scope_key, locator_sha256 "
        "FROM catalog_gallery_identities "
        "WHERE scope_key = %s AND locator_sha256 = %s",
        (scope, locator_sha256),
    )
    by_key = connector.fetch_one(
        "SELECT gallery_id, gallery_key, scope_key, locator_sha256 "
        "FROM catalog_gallery_identities WHERE gallery_key = %s",
        (stable_key,),
    )
    if by_natural:
        gallery_id = require_positive_int63(by_natural[0], field="persisted gallery_id")
        _require_exact(
            "gallery natural identity",
            by_natural,
            (gallery_id, *expected_tail),
        )
        if by_key:
            _require_exact(
                "gallery key identity",
                by_key,
                (gallery_id, *expected_tail),
            )
        return gallery_id
    if by_key:
        gallery_id = require_positive_int63(by_key[0], field="persisted gallery_id")
        _require_exact(
            "gallery key collision",
            by_key,
            (gallery_id, *expected_tail),
        )
        return gallery_id
    return None


def _require_observation_allocator(connector: Any, gallery_id: int) -> None:
    row = connector.fetch_one(
        "SELECT next_observation_id FROM "
        "operational_gallery_observation_allocators WHERE gallery_id = %s",
        (gallery_id,),
    )
    if len(row) != 1:
        raise GalleryIdentityConflictError(
            "durable gallery identity has no observation allocator"
        )
    require_positive_int63(row[0], field="next_observation_id")


def _release_optional_claim(
    connector: Any,
    *,
    generation: int,
    locator_sha256: bytes,
    claim: tuple[Any, ...],
) -> None:
    if not claim:
        return
    affected = connector.execute_affected(
        "DELETE FROM operational_canonical_value_uploads "
        "WHERE generation = %s AND value_sha256 = %s",
        (generation, locator_sha256),
    )
    if affected != 1:
        raise GalleryIdentityConflictError(
            "source locator upload claim changed before consumer handoff"
        )


def _require_exact(
    label: str,
    actual: tuple[Any, ...],
    expected: tuple[Any, ...],
) -> None:
    if actual != expected:
        raise GalleryIdentityConflictError(
            f"{label} conflicts with its immutable exact tuple"
        )
