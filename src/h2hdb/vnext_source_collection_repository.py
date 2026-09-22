"""Fenced first-scan checkpoints whose identity does not require a complete cut."""

from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from .domain import VNextResolvedIngestPolicy
from .vnext_canonical_value_repository import CanonicalValueUploadPlan, _authorize
from .vnext_catalog_identity_family import load_gallery_identities
from .vnext_domains import (
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from .vnext_gallery_identity_repository import (
    SourceLocatorCommand,
    handoff_locator_in_scope,
)
from .vnext_identity import (
    SOURCE_ROOT_DIGEST_DOMAIN,
    iter_source_root_payload,
    source_root_digest,
    source_scope_key,
    validate_source_root_parts,
)
from .vnext_ingest_fence_repository import IngestTurn
from .vnext_ingest_policy_repository import VNextIngestPolicyRepository
from .vnext_maintenance_gate_repository import GateLease
from .vnext_manifest_family import (
    ensure_gallery_manifest_family,
    load_gallery_manifest_family,
    load_source_build_family,
)
from .vnext_source_build_repository import (
    _insert_or_validate_scope,
    _require_sealed_root_identity,
)
from .vnext_source_marker_family import CachedSourceObservation
from .vnext_source_observation_family import (
    SealedSourceObservation,
    load_sealed_source_observation,
)
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

if TYPE_CHECKING:
    from .vnext_gallery_staging_repository import GalleryStagingSeal


class SourceCollectionConflictError(RuntimeError):
    """An immutable collection, sealed observation or exact claim disagrees."""


class SourceCollectionNotReadyError(RuntimeError):
    """The collection is no longer the live exact working authority."""


@dataclass(frozen=True, slots=True)
class SourceCollectionRootCommand:
    source_root_components: tuple[str, ...]
    source_root_sha256: bytes = field(init=False)
    source_root_byte_count: int = field(init=False)
    source_root_payload_sha256: bytes = field(init=False)

    def __post_init__(self) -> None:
        if type(self.source_root_components) is not tuple:
            raise TypeError("source_root_components must be a tuple")
        receipt = validate_source_root_parts(
            iter_source_root_payload(self.source_root_components)
        )
        object.__setattr__(
            self, "source_root_sha256", source_root_digest(self.source_root_components)
        )
        object.__setattr__(self, "source_root_byte_count", receipt.payload_byte_count)
        object.__setattr__(self, "source_root_payload_sha256", receipt.payload_sha256)

    def prepare_root_upload(self) -> CanonicalValueUploadPlan:
        return CanonicalValueUploadPlan.from_parts(
            SOURCE_ROOT_DIGEST_DOMAIN,
            iter_source_root_payload(self.source_root_components),
        )


@dataclass(frozen=True, slots=True)
class SourceCollectionHandle:
    collection_id: bytes
    scope_key: bytes
    manifest_policy_id: int
    qualification_policy_sha256: bytes
    ingest_generation: int
    claim_generation: int
    created_at: int

    def __post_init__(self) -> None:
        require_uuid16(self.collection_id, field="collection_id")
        require_digest32(self.scope_key, field="scope_key")
        require_positive_int63(self.manifest_policy_id, field="manifest_policy_id")
        require_digest32(
            self.qualification_policy_sha256, field="qualification_policy_sha256"
        )
        require_int63(self.ingest_generation, field="ingest_generation")
        require_positive_int63(self.claim_generation, field="claim_generation")
        require_int63(self.created_at, field="created_at")


@dataclass(frozen=True, slots=True)
class CollectionGalleryIdentityHandoff:
    collection_id: bytes
    gallery_id: int
    gallery_key: bytes
    scope_key: bytes
    locator_sha256: bytes
    replayed: bool

    def __post_init__(self) -> None:
        require_uuid16(self.collection_id, field="collection_id")
        require_positive_int63(self.gallery_id, field="gallery_id")
        for name in ("gallery_key", "scope_key", "locator_sha256"):
            require_digest32(getattr(self, name), field=name)
        if type(self.replayed) is not bool:
            raise TypeError("replayed must be bool")


@dataclass(frozen=True, slots=True)
class SourceCollectionRetention:
    observation: SealedSourceObservation
    replayed: bool

    def __post_init__(self) -> None:
        if type(self.observation) is not SealedSourceObservation:
            raise TypeError("retention observation must be SealedSourceObservation")
        self.observation.__post_init__()
        if type(self.replayed) is not bool:
            raise TypeError("retention replayed must be bool")


def _working_collection(work: VNextUnitOfWork) -> tuple[Any, ...]:
    return work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("source-collection", 1),
        "SELECT collection_id, assigned_at FROM operational_source_working_collections WHERE slot = 1",
    )


def _load_handle(
    connector: Any, collection_id: bytes
) -> tuple[SourceCollectionHandle, str]:
    row = connector.fetch_one(
        "SELECT root.scope_key, policy.manifest_policy_id, qualification.qualification_policy_sha256, "
        "claim.ingest_generation, claim.claim_generation, created.created_at, state.state "
        "FROM catalog_source_collections AS root "
        "LEFT JOIN catalog_source_collection_manifest_policies AS policy ON policy.collection_id = root.collection_id "
        "LEFT JOIN catalog_source_collection_qualification_policies AS qualification ON qualification.collection_id = root.collection_id "
        "LEFT JOIN catalog_source_collection_created_ats AS created ON created.collection_id = root.collection_id "
        "LEFT JOIN operational_source_collection_states AS state ON state.collection_id = root.collection_id "
        "LEFT JOIN operational_source_collection_claims AS claim ON claim.collection_id = root.collection_id "
        "WHERE root.collection_id = %s",
        (collection_id,),
    )
    if len(row) != 7 or any(value is None for value in row):
        raise SourceCollectionConflictError(
            "collection descriptor or claim is incomplete"
        )
    handle = SourceCollectionHandle(collection_id, *row[:6])
    state = row[6]
    if state not in {"OPEN", "CONSUMED", "ABANDONED"}:
        raise SourceCollectionConflictError("collection state is invalid")
    return handle, state


def authorize_current_collection(
    work: VNextUnitOfWork,
    *,
    collection_id: bytes,
    generation: int,
) -> SourceCollectionHandle:
    """Require the exact owner after the caller has acquired outer ingest locks."""
    collection = require_uuid16(collection_id, field="collection_id")
    current = require_int63(generation, field="generation")
    working = _working_collection(work)
    handle, state = _load_handle(work.connector, collection)
    if (
        working != (collection, handle.created_at)
        or state != "OPEN"
        or handle.ingest_generation != current
    ):
        raise SourceCollectionNotReadyError(
            "collection is not the live working authority"
        )
    return handle


class SourceCollectionRepository:
    @staticmethod
    def _authorize_handle(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        handle: SourceCollectionHandle,
        now: int,
    ) -> SourceCollectionHandle:
        if type(handle) is not SourceCollectionHandle:
            raise TypeError("handle must be SourceCollectionHandle")
        handle.__post_init__()
        generation = _authorize(
            work, gate_lease, ingest_turn, now=require_int63(now, field="now")
        )
        durable = authorize_current_collection(
            work, collection_id=handle.collection_id, generation=generation
        )
        if durable != handle:
            raise SourceCollectionNotReadyError("collection handle was superseded")
        return durable

    @staticmethod
    def handoff_root(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        command: SourceCollectionRootCommand,
        root_plan: CanonicalValueUploadPlan,
        policy: VNextResolvedIngestPolicy,
        now: int,
    ) -> SourceCollectionHandle:
        if type(command) is not SourceCollectionRootCommand:
            raise TypeError("command must be SourceCollectionRootCommand")
        command.__post_init__()
        if type(root_plan) is not CanonicalValueUploadPlan:
            raise TypeError("root_plan must be CanonicalValueUploadPlan")
        timestamp = require_int63(now, field="now")
        generation = _authorize(work, gate_lease, ingest_turn, now=timestamp)
        policy = VNextIngestPolicyRepository.require_exact(work, policy)
        receipt = root_plan.source_root_receipt
        if (
            root_plan.digest_domain != SOURCE_ROOT_DIGEST_DOMAIN.encode("ascii")
            or root_plan.value_sha256 != command.source_root_sha256
            or root_plan.byte_count != command.source_root_byte_count
            or receipt.payload_byte_count != command.source_root_byte_count
            or receipt.payload_sha256 != command.source_root_payload_sha256
            or receipt.component_count != len(command.source_root_components)
        ):
            raise SourceCollectionConflictError(
                "root upload differs from the exact root command"
            )
        scope = source_scope_key("filesystem", command.source_root_sha256, 1)
        qualification = sha256(
            b"h2hdb-source-qualification-policy-v1\0"
            + policy.artifact_policy_sha256
            + bytes((int(policy.policy.artifacts_required),))
        ).digest()
        working = _working_collection(work)
        if work.connector.fetch_one(
            "SELECT 1 FROM operational_source_collection_claims AS claim "
            "JOIN catalog_source_collections AS collection ON collection.collection_id = claim.collection_id "
            "JOIN catalog_source_collection_manifest_policies AS manifest ON manifest.collection_id = collection.collection_id "
            "JOIN catalog_source_collection_qualification_policies AS qualification ON qualification.collection_id = collection.collection_id "
            "WHERE claim.ingest_generation = %s AND (collection.scope_key <> %s "
            "OR manifest.manifest_policy_id <> %s OR qualification.qualification_policy_sha256 <> %s) LIMIT 1",
            (generation, scope, policy.manifest_policy_id, qualification),
        ):
            raise SourceCollectionConflictError(
                "one ingest generation cannot switch source root or collection policy; claim a new ingest session"
            )
        claim = work.lock_row(
            LockRank.CHECKPOINT,
            encode_lock_key(
                "source-root-upload", generation, command.source_root_sha256
            ),
            "SELECT generation, value_sha256 FROM operational_canonical_value_uploads "
            "WHERE generation = %s AND value_sha256 = %s",
            (generation, command.source_root_sha256),
        )
        if claim and claim != (generation, command.source_root_sha256):
            raise SourceCollectionConflictError("source root upload claim differs")
        if not claim:
            if not working:
                raise SourceCollectionNotReadyError(
                    "exact source root upload claim is absent"
                )
            replay, replay_state = _load_handle(work.connector, working[0])
            if (
                working != (replay.collection_id, replay.created_at)
                or replay_state != "OPEN"
                or replay.ingest_generation != generation
                or (
                    replay.scope_key,
                    replay.manifest_policy_id,
                    replay.qualification_policy_sha256,
                )
                != (scope, policy.manifest_policy_id, qualification)
            ):
                raise SourceCollectionNotReadyError(
                    "absent upload claim is not an exact collection handoff replay"
                )
        _require_sealed_root_identity(
            work.connector,
            source_root_sha256=command.source_root_sha256,
            byte_count=root_plan.byte_count,
            root_page_sha256=root_plan.root_page_sha256,
        )
        _insert_or_validate_scope(
            work.connector, scope=scope, source_root_sha256=command.source_root_sha256
        )
        old: SourceCollectionHandle | None = None
        if working:
            old, state = _load_handle(work.connector, working[0])
            if working != (old.collection_id, old.created_at) or state != "OPEN":
                raise SourceCollectionConflictError(
                    "working collection is not exact OPEN authority"
                )
            if old.ingest_generation > generation:
                raise SourceCollectionConflictError(
                    "working collection has a future ingest claim"
                )
            if (
                old.scope_key,
                old.manifest_policy_id,
                old.qualification_policy_sha256,
            ) != (
                scope,
                policy.manifest_policy_id,
                qualification,
            ):
                # Old staging cannot authorize another write after generation takeover.
                work.connector.execute(
                    "UPDATE operational_gallery_observation_stagings SET state = 'ABANDONED' "
                    "WHERE state = 'OPEN' AND staging_id IN (SELECT staging_id FROM operational_gallery_staging_collections WHERE collection_id = %s)",
                    (old.collection_id,),
                )
                work.compare_and_swap(
                    "UPDATE operational_source_collection_states SET state = 'ABANDONED' "
                    "WHERE collection_id = %s AND state = 'OPEN'",
                    (old.collection_id,),
                    authority="collection abandonment",
                )
                work.compare_and_swap(
                    "DELETE FROM operational_source_working_collections WHERE slot = 1 AND collection_id = %s AND assigned_at = %s",
                    (old.collection_id, old.created_at),
                    authority="working collection abandonment",
                )
                old = None
        if old is None:
            collection = uuid4().bytes
            handle = SourceCollectionHandle(
                collection,
                scope,
                policy.manifest_policy_id,
                qualification,
                generation,
                1,
                timestamp,
            )
            for table, columns, values in (
                (
                    "catalog_source_collections",
                    "collection_id, scope_key",
                    (collection, scope),
                ),
                (
                    "catalog_source_collection_manifest_policies",
                    "collection_id, manifest_policy_id",
                    (collection, policy.manifest_policy_id),
                ),
                (
                    "catalog_source_collection_qualification_policies",
                    "collection_id, qualification_policy_sha256",
                    (collection, qualification),
                ),
                (
                    "catalog_source_collection_created_ats",
                    "collection_id, created_at",
                    (collection, timestamp),
                ),
                (
                    "operational_source_collection_states",
                    "collection_id, state",
                    (collection, "OPEN"),
                ),
                (
                    "operational_source_collection_claims",
                    "collection_id, ingest_generation, claim_generation, updated_at",
                    (collection, generation, 1, timestamp),
                ),
                (
                    "operational_source_working_collections",
                    "slot, collection_id, assigned_at",
                    (1, collection, timestamp),
                ),
            ):
                placeholders = ", ".join("%s" for _ in values)
                work.connector.execute(
                    f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", values
                )
        elif old.ingest_generation == generation:
            handle = old
        else:
            if old.ingest_generation > generation:
                raise SourceCollectionNotReadyError(
                    "collection claim belongs to a newer generation"
                )
            next_claim = require_positive_int63(
                old.claim_generation + 1, field="next collection claim"
            )
            work.compare_and_swap(
                "UPDATE operational_source_collection_claims SET ingest_generation = %s, claim_generation = %s, updated_at = %s "
                "WHERE collection_id = %s AND ingest_generation = %s AND claim_generation = %s",
                (
                    generation,
                    next_claim,
                    timestamp,
                    old.collection_id,
                    old.ingest_generation,
                    old.claim_generation,
                ),
                authority="collection takeover",
            )
            handle = SourceCollectionHandle(
                old.collection_id,
                old.scope_key,
                old.manifest_policy_id,
                old.qualification_policy_sha256,
                generation,
                next_claim,
                old.created_at,
            )
        if claim:
            work.compare_and_swap(
                "DELETE FROM operational_canonical_value_uploads WHERE generation = %s AND value_sha256 = %s",
                (generation, command.source_root_sha256),
                authority="collection root upload handoff",
            )
        return handle

    @staticmethod
    def handoff_locator(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        handle: SourceCollectionHandle,
        command: SourceLocatorCommand,
        locator_plan: CanonicalValueUploadPlan,
        now: int,
    ) -> CollectionGalleryIdentityHandoff:
        authority = SourceCollectionRepository._authorize_handle(
            work, gate_lease=gate_lease, ingest_turn=ingest_turn, handle=handle, now=now
        )
        identity, replayed = handoff_locator_in_scope(
            work,
            generation=authority.ingest_generation,
            scope=authority.scope_key,
            command=command,
            locator_plan=locator_plan,
            now=now,
        )
        return CollectionGalleryIdentityHandoff(
            authority.collection_id,
            identity.gallery_id,
            identity.gallery_key,
            identity.scope_key,
            identity.locator_sha256,
            replayed,
        )

    @staticmethod
    def load_observation(
        connector: Any,
        *,
        handle: SourceCollectionHandle,
        gallery_id: int,
        observation_id: int,
    ) -> SealedSourceObservation:
        handle.__post_init__()
        row = connector.fetch_one(
            "SELECT 1 FROM catalog_source_collection_observations "
            "WHERE collection_id = %s AND gallery_id = %s AND observation_id = %s",
            (handle.collection_id, gallery_id, observation_id),
        )
        if row != (1,):
            raise SourceCollectionConflictError(
                "selected observation is outside this collection"
            )
        durable, _state = _load_handle(connector, handle.collection_id)
        if durable != handle:
            raise SourceCollectionNotReadyError(
                "collection observation handle was superseded"
            )
        observation = load_sealed_source_observation(
            connector, gallery_id=gallery_id, observation_id=observation_id
        )
        _require_observation_policy(connector, handle, observation)
        if (
            load_gallery_manifest_family(
                connector,
                gallery_id=gallery_id,
                observation_id=observation_id,
                manifest_policy_id=handle.manifest_policy_id,
            )
            is None
        ):
            raise SourceCollectionConflictError(
                "retained observation lost its gallery manifest"
            )
        return observation

    @staticmethod
    def retain_observation(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        handle: SourceCollectionHandle,
        observation: SealedSourceObservation | CachedSourceObservation,
        now: int,
    ) -> SourceCollectionRetention:
        SourceCollectionRepository._authorize_handle(
            work, gate_lease=gate_lease, ingest_turn=ingest_turn, handle=handle, now=now
        )
        expected = (
            SealedSourceObservation.from_cached(observation)
            if type(observation) is CachedSourceObservation
            else observation
        )
        if type(expected) is not SealedSourceObservation:
            raise TypeError("observation must be a sealed source receipt")
        durable = load_sealed_source_observation(
            work.connector,
            gallery_id=expected.gallery_id,
            observation_id=expected.observation_id,
        )
        if durable != expected:
            raise SourceCollectionConflictError(
                "selected sealed observation differs from durable facts"
            )
        _require_observation_policy(work.connector, handle, durable)
        if work.connector.fetch_one(
            "SELECT 1 FROM operational_gallery_staging_collections WHERE collection_id = %s",
            (handle.collection_id,),
        ):
            raise SourceCollectionNotReadyError(
                "cached reuse cannot bypass pending collection staging"
            )
        ensure_gallery_manifest_family(
            work,
            gallery_id=durable.gallery_id,
            observation_id=durable.observation_id,
            manifest_policy_id=handle.manifest_policy_id,
        )
        member = work.connector.fetch_one(
            "SELECT 1 FROM catalog_source_collection_observations WHERE collection_id = %s AND gallery_id = %s AND observation_id = %s",
            (handle.collection_id, durable.gallery_id, durable.observation_id),
        )
        if not member:
            work.connector.execute(
                "INSERT INTO catalog_source_collection_observations (collection_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
                (handle.collection_id, durable.gallery_id, durable.observation_id),
            )
        return SourceCollectionRetention(durable, bool(member))

    @staticmethod
    def attach_observation(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        handle: SourceCollectionHandle,
        build_id: bytes,
        observation: SealedSourceObservation,
        now: int,
    ) -> GalleryStagingSeal:
        from .domain import GalleryStagingOwner
        from .vnext_gallery_staging_repository import (
            GalleryStagingSeal,
            _lock_and_require_working_build,
        )
        from .vnext_source_marker_repository import _require_member

        build_id = require_uuid16(build_id, field="build_id")
        generation = _authorize(work, gate_lease, ingest_turn, now=now)
        scope, _state = _lock_and_require_working_build(
            work, generation=generation, build_id=build_id
        )
        current = authorize_current_collection(
            work, collection_id=handle.collection_id, generation=generation
        )
        if current != handle or current.scope_key != scope:
            raise SourceCollectionConflictError("build and collection authority differ")
        if type(observation) is not SealedSourceObservation:
            raise TypeError("observation must be SealedSourceObservation")
        durable = SourceCollectionRepository.load_observation(
            work.connector,
            handle=handle,
            gallery_id=observation.gallery_id,
            observation_id=observation.observation_id,
        )
        if durable != observation:
            raise SourceCollectionConflictError(
                "source cut observation differs from selected sealed authority"
            )
        _require_member(work.connector, build_id, observation.gallery_id, scope)
        build = load_source_build_family(work.connector, build_id=build_id)
        if build is None or build.manifest_policy_id != handle.manifest_policy_id:
            raise SourceCollectionConflictError(
                "collection manifest policy differs from source build"
            )
        if work.connector.fetch_one(
            "SELECT 1 FROM operational_gallery_staging_source_builds WHERE build_id = %s",
            (build_id,),
        ):
            raise SourceCollectionNotReadyError(
                "source attachment cannot bypass pending staging"
            )
        existing = work.connector.fetch_one(
            "SELECT observation_id FROM catalog_source_build_galleries WHERE build_id = %s AND gallery_id = %s",
            (build_id, observation.gallery_id),
        )
        if existing and existing != (observation.observation_id,):
            raise SourceCollectionConflictError(
                "source cut already selected a different observation"
            )
        if not existing:
            work.connector.execute(
                "INSERT INTO catalog_source_build_galleries (build_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
                (build_id, observation.gallery_id, observation.observation_id),
            )
        return GalleryStagingSeal(
            GalleryStagingOwner("SOURCE_BUILD", build_id),
            observation.gallery_id,
            observation.observation_id,
            observation.observation_identity_sha256,
            "REUSED",
            bool(existing),
        )

    @staticmethod
    def lock_for_assembly(
        work: VNextUnitOfWork,
        *,
        handle: SourceCollectionHandle,
        build_id: bytes,
        generation: int,
    ) -> None:
        """Acquire the collection after source-build locks, before checkpoints."""
        handle.__post_init__()
        working = _working_collection(work)
        current, state = _load_handle(work.connector, handle.collection_id)
        if current != handle or current.ingest_generation != generation:
            raise SourceCollectionNotReadyError(
                "collection assembly claim was superseded"
            )
        if state == "OPEN" and working == (handle.collection_id, handle.created_at):
            return
        consumed = work.connector.fetch_one(
            "SELECT build_id FROM catalog_source_collection_consumptions WHERE collection_id = %s",
            (handle.collection_id,),
        )
        if (
            state != "CONSUMED"
            or consumed != (build_id,)
            or working == (handle.collection_id, handle.created_at)
        ):
            raise SourceCollectionNotReadyError(
                "collection is not live or an exact completed replay"
            )

    @staticmethod
    def consume_authorized(
        work: VNextUnitOfWork,
        *,
        handle: SourceCollectionHandle,
        build_id: bytes,
        generation: int,
    ) -> None:
        """Complete under the already-held outer and working locks, without I/O."""
        current, state = _load_handle(work.connector, handle.collection_id)
        if current != handle or current.ingest_generation != generation:
            raise SourceCollectionNotReadyError(
                "collection completion claim was superseded"
            )
        working = work.connector.fetch_one(
            "SELECT collection_id, assigned_at FROM operational_source_working_collections WHERE slot = 1"
        )
        consumed = work.connector.fetch_one(
            "SELECT build_id FROM catalog_source_collection_consumptions WHERE collection_id = %s",
            (handle.collection_id,),
        )
        if (
            state == "CONSUMED"
            and consumed == (build_id,)
            and working != (handle.collection_id, handle.created_at)
        ):
            return
        if (
            state != "OPEN"
            or working != (handle.collection_id, handle.created_at)
            or consumed
        ):
            raise SourceCollectionNotReadyError(
                "collection cannot complete from this state"
            )
        build = load_source_build_family(work.connector, build_id=build_id)
        if (
            build is None
            or build.state != "SEALED"
            or build.scope_key != handle.scope_key
            or build.manifest_policy_id != handle.manifest_policy_id
            or work.connector.fetch_one(
                "SELECT build_id FROM operational_source_build_generations WHERE generation = %s",
                (generation,),
            )
            != (build_id,)
        ):
            raise SourceCollectionConflictError(
                "collection completion requires its current sealed source build"
            )
        if work.connector.fetch_one(
            "SELECT 1 FROM operational_gallery_staging_collections WHERE collection_id = %s",
            (handle.collection_id,),
        ):
            raise SourceCollectionNotReadyError(
                "collection still owns unretired staging"
            )
        work.connector.execute(
            "INSERT INTO catalog_source_collection_consumptions (collection_id, build_id) VALUES (%s, %s)",
            (handle.collection_id, build_id),
        )
        work.compare_and_swap(
            "UPDATE operational_source_collection_states SET state = 'CONSUMED' WHERE collection_id = %s AND state = 'OPEN'",
            (handle.collection_id,),
            authority="collection completion",
        )
        work.compare_and_swap(
            "DELETE FROM operational_source_working_collections WHERE slot = 1 AND collection_id = %s AND assigned_at = %s",
            (handle.collection_id, handle.created_at),
            authority="collection working release",
        )


def _require_observation_policy(
    connector: Any, handle: SourceCollectionHandle, observation: SealedSourceObservation
) -> None:
    identities = load_gallery_identities(
        connector, gallery_ids=(observation.gallery_id,)
    )
    identity = identities.get(observation.gallery_id)
    qualification = connector.fetch_one(
        "SELECT qualification_policy_sha256 FROM catalog_gallery_observation_validation_policies WHERE gallery_id = %s AND observation_id = %s",
        (observation.gallery_id, observation.observation_id),
    )
    if (
        identity is None
        or identity.scope_key != handle.scope_key
        or qualification != (handle.qualification_policy_sha256,)
    ):
        raise SourceCollectionConflictError(
            "observation scope or qualification differs from collection"
        )
