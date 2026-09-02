"""Restartable publication orchestration for the vNext ingest facade.

The public facade delegates one ``issue -> prepare -> commit`` step at a time
to :class:`VNextIngestPublication`.  Issue and commit calls own fresh, short
database transactions.  Rendering, storage protection/release, and library
activation work happen only after the issuing transaction has ended.

The caller never supplies a stage, cursor, count, digest, batch key, or
publication identity.  The next action is reconstructed from sealed database
facts and repository-owned checkpoints.  Opaque issued/prepared values bind a
step to the stable identity of an ingest session while deliberately excluding
lease expiry, so the commit side accepts a renewed copy of the same authority.
"""

from __future__ import annotations

__all__ = [
    "LibraryActivationCheckpoint",
    "LibraryActivationStatus",
    "VNextLibraryActivationAdapter",
    "VNextIngestPublication",
    "VNextIssuedPublicationStep",
    "VNextPreparedPublicationStep",
]

import secrets
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from threading import Event, Lock
from time import time_ns
from typing import Protocol, cast, runtime_checkable

from .domain import (
    ArtifactReleaseStorageEvidence,
    CatalogResourceKind,
    VNextIngestAdvanceResult,
    VNextIngestPhase,
    VNextIngestSession,
    VNextLibraryActivationCursor,
    VNextLibraryActivationItem,
    VNextResolvedIngestPolicy,
)
from .ports import ArtifactReleaseAdapter, ArtifactStorageAdapter
from .repository import RepositoryContext
from .sql_connector import SQLConnector
from .vnext_artifact_family import (
    PreparedArtifactFamily,
    load_prepared_artifact_families,
)
from .vnext_artifact_preparation_repository import (
    ArtifactInputProjectionPlan,
    ArtifactPreparationAuthority,
    ArtifactPreparationReceipt,
    ArtifactPreparationRepository,
    ArtifactProtectionEvidence,
    ArtifactProtectionIntent,
    _protection_intent_from_family,
)
from .vnext_canonical_value_family import (
    load_page_family,
    load_sealed_value_identity,
)
from .vnext_canonical_value_repository import (
    CanonicalValueCollisionError,
    CanonicalValueNotReadyError,
    CanonicalValuePartialFamilyError,
    CanonicalValueReadReceipt,
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
    PreparedCanonicalPage,
    _allocate_authorized,
)
from .vnext_canonical_value_repository import (
    _authorize as _authorize_canonical_write,
)
from .vnext_domains import (
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from .vnext_download_ingest_repository import (
    CoordinatedIngestTurn,
    DownloadIngestRepository,
    HandoffKind,
)
from .vnext_ingest_fence_repository import IngestTurn
from .vnext_library_activation_repository import (
    LibraryActivationResourceRepository,
)
from .vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
)
from .vnext_operational_event_repository import (
    DeletionConsumption,
    OperationalEffect,
    OperationalEffectRepository,
    OperationalEffectSeal,
    RemovedGid,
)
from .vnext_publication_candidate_repository import (
    PublicationCandidateBatch,
    PublicationCandidateRepository,
    PublicationCatalogProjectionPlan,
)
from .vnext_publication_finalization_repository import (
    _PRELOCKED_GATE_CAPABILITY,
    PublicationFinalizationAcknowledgement,
    PublicationFinalizationPage,
    PublicationFinalizationRepository,
    PublicationFinalizationUnavailableError,
    _acknowledge,
    _resolve_adapters,
)
from .vnext_publication_repository import PublicationRepository
from .vnext_transaction import VNextUnitOfWork

_STEP_TOKEN = object()
_PREPARED_TOKEN = object()
_MAX_PAGE_ROWS = 128
_MAX_CACHED_ARTIFACT_RESOURCE_BYTES = 256 * 1024 * 1024

_CANDIDATE_STAGES = (
    b"BUILD_SELECTION",
    b"VALIDATE_SELECTION",
    b"BUILD_CATALOG_PROJECTION",
    b"VALIDATE_CATALOG_PROJECTION",
    b"BUILD_ARTIFACT_INPUT",
    b"BUILD_ARTIFACT_DELTA_OPERATION",
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


def _now_microseconds() -> int:
    return time_ns() // 1_000


class LibraryActivationStatus(StrEnum):
    """Durable state owned by the library-activation adapter."""

    SPOOL = "SPOOL"
    RECONCILE = "RECONCILE"
    READY = "READY"
    COMPLETE = "COMPLETE"


@dataclass(frozen=True, slots=True)
class LibraryActivationCheckpoint:
    """Adapter-owned restart checkpoint; its cursor is never caller input."""

    revision: int
    receipt_id: bytes
    status: LibraryActivationStatus
    cursor: VNextLibraryActivationCursor | None

    def __post_init__(self) -> None:
        require_positive_int63(
            self.revision,
            field="library activation adapter revision",
        )
        require_uuid16(
            self.receipt_id,
            field="library activation adapter receipt_id",
        )
        if not isinstance(self.status, LibraryActivationStatus):
            raise TypeError("library activation status is not registered")
        cursor = self.cursor
        if cursor is not None and not isinstance(cursor, VNextLibraryActivationCursor):
            raise TypeError(
                "library activation adapter cursor must be VNextLibraryActivationCursor"
            )
        if cursor is not None:
            cursor.__post_init__()
        if (
            self.status
            in {
                LibraryActivationStatus.READY,
                LibraryActivationStatus.COMPLETE,
            }
            and cursor is not None
        ):
            raise ValueError("a terminal activation checkpoint cannot expose a cursor")


@runtime_checkable
class VNextLibraryActivationAdapter(Protocol):
    """Neutral crash-safe stable-library activation port consumed by core."""

    def begin(
        self,
        revision: int,
        receipt_id: bytes,
    ) -> LibraryActivationCheckpoint: ...

    def activate_page(
        self,
        revision: int,
        items: Sequence[VNextLibraryActivationItem],
    ) -> None: ...

    def seal(self, revision: int) -> None: ...

    def reconcile_page(
        self,
        revision: int,
        receipt_id: bytes,
        *,
        limit: int,
    ) -> LibraryActivationCheckpoint: ...

    def complete(self, revision: int, receipt_id: bytes) -> None: ...


class _Action(StrEnum):
    BEGIN = "BEGIN"
    BUILD_SELECTION = "BUILD_SELECTION"
    VALIDATE_SELECTION = "VALIDATE_SELECTION"
    BUILD_CATALOG = "BUILD_CATALOG"
    VALIDATE_CATALOG = "VALIDATE_CATALOG"
    BUILD_ARTIFACT_INPUT = "BUILD_ARTIFACT_INPUT"
    BUILD_ARTIFACT_DELTA = "BUILD_ARTIFACT_DELTA"
    VALIDATE_ARTIFACT_INPUT = "VALIDATE_ARTIFACT_INPUT"
    BEGIN_OPERATIONAL = "BEGIN_OPERATIONAL"
    APPEND_OPERATIONAL = "APPEND_OPERATIONAL"
    SEAL_OPERATIONAL = "SEAL_OPERATIONAL"
    PREPARE_ARTIFACT = "PREPARE_ARTIFACT"
    BIND_OPERATIONAL = "BIND_OPERATIONAL"
    VALIDATE_PREPARED = "VALIDATE_PREPARED"
    VALIDATE_CREATE = "VALIDATE_CREATE"
    VALIDATE_REBUILD = "VALIDATE_REBUILD"
    VALIDATE_DELETE = "VALIDATE_DELETE"
    VALIDATE_UNCHANGED = "VALIDATE_UNCHANGED"
    VALIDATE_NEW = "VALIDATE_NEW"
    VALIDATE_CHANGED = "VALIDATE_CHANGED"
    VALIDATE_REMOVED = "VALIDATE_REMOVED"
    VALIDATE_DUPLICATE = "VALIDATE_DUPLICATE"
    COMMIT_PUBLICATION = "COMMIT_PUBLICATION"
    LIBRARY_ACTIVATION = "LIBRARY_ACTIVATION"
    FINALIZE = "FINALIZE"
    COMPLETE = "COMPLETE"
    CANONICAL_ALLOCATE = "CANONICAL_ALLOCATE"
    CANONICAL_PAGE = "CANONICAL_PAGE"
    CANONICAL_SEAL = "CANONICAL_SEAL"


_PLAN_ACTIONS = {
    _Action.BUILD_CATALOG,
    _Action.VALIDATE_CATALOG,
    _Action.BUILD_ARTIFACT_INPUT,
    _Action.VALIDATE_ARTIFACT_INPUT,
}

_PLAN_STAGE_BY_ACTION = {
    _Action.BUILD_CATALOG: b"BUILD_CATALOG_PROJECTION",
    _Action.VALIDATE_CATALOG: b"VALIDATE_CATALOG_PROJECTION",
    _Action.BUILD_ARTIFACT_INPUT: b"BUILD_ARTIFACT_INPUT",
    _Action.VALIDATE_ARTIFACT_INPUT: b"VALIDATE_ARTIFACT_INPUT_DELTA",
}

_PLAN_BUILD_ACTIONS = {
    _Action.BUILD_CATALOG,
    _Action.BUILD_ARTIFACT_INPUT,
}


@dataclass(frozen=True, slots=True)
class _Root:
    build_id: bytes
    analysis_id: bytes
    artifact_policy_id: int
    candidate_id: bytes | None
    receipt_id: bytes | None
    revision: int | None
    receipt_state: str | None


@dataclass(frozen=True, slots=True)
class _CandidateWork:
    candidate_id: bytes
    batch_key: bytes
    payload: object | None = None
    resource_owner: object | None = field(default=None, repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class _OperationalWork:
    candidate_id: bytes
    build_id: bytes
    operational_policy_id: int
    preparation_id: bytes | None = None
    effect_seal: OperationalEffectSeal | None = None


@dataclass(frozen=True, slots=True)
class _ArtifactWork:
    authority: ArtifactPreparationAuthority
    effect_seal: OperationalEffectSeal
    families: tuple[PreparedArtifactFamily, ...] | None


class _ArtifactReceiptOwner:
    """Exclusive owner for one disposable rendered artifact receipt."""

    __slots__ = ("__receipt",)

    def __init__(self, receipt: ArtifactPreparationReceipt) -> None:
        self.__receipt: ArtifactPreparationReceipt | None = receipt

    @property
    def receipt(self) -> ArtifactPreparationReceipt:
        receipt = self.__receipt
        if receipt is None:
            raise RuntimeError("artifact preparation receipt ownership was transferred")
        return receipt

    def detach(self) -> ArtifactPreparationReceipt:
        receipt = self.receipt
        self.__receipt = None
        return receipt

    def close(self) -> None:
        receipt = self.__receipt
        if receipt is None:
            return
        self.__receipt = None
        receipt.close()

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException:
            pass


class _ArtifactReceiptCache:
    """Facade-owned optional spool for one exact durable PENDING bundle."""

    __slots__ = ("authority", "families", "__receipt")

    def __init__(
        self,
        *,
        authority: ArtifactPreparationAuthority,
        families: tuple[PreparedArtifactFamily, ...],
        receipt: ArtifactPreparationReceipt,
    ) -> None:
        self.authority = authority
        self.families = families
        self.__receipt: ArtifactPreparationReceipt | None = receipt

    def matches(
        self,
        authority: ArtifactPreparationAuthority,
        families: tuple[PreparedArtifactFamily, ...],
    ) -> bool:
        return self.authority == authority and self.families == families

    def take(self) -> _ArtifactReceiptOwner:
        receipt = self.__receipt
        if receipt is None:
            raise RuntimeError("artifact receipt cache is already consumed")
        owner = _ArtifactReceiptOwner(receipt)
        self.__receipt = None
        return owner

    def close(self) -> None:
        receipt = self.__receipt
        if receipt is None:
            return
        self.__receipt = None
        receipt.close()

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException:
            pass


@dataclass(frozen=True, slots=True)
class _ArtifactPrepared:
    receipt_owner: _ArtifactReceiptOwner = field(repr=False, compare=False)
    effect_seal: OperationalEffectSeal
    intents: tuple[ArtifactProtectionIntent, ...]
    evidence: tuple[ArtifactProtectionEvidence, ...]

    @property
    def receipt(self) -> ArtifactPreparationReceipt:
        return self.receipt_owner.receipt


@dataclass(frozen=True, slots=True)
class _LibraryActivationWork:
    receipt_id: bytes
    revision: int
    checkpoint: LibraryActivationCheckpoint | None = None
    items: tuple[VNextLibraryActivationItem, ...] | None = None
    terminal: bool = False


@dataclass(frozen=True, slots=True)
class _CanonicalStageFence:
    candidate_id: bytes
    stage_action: _Action
    first_consumer_cursor: bytes
    ingest_generation: int


@dataclass(frozen=True, slots=True)
class _CanonicalWork:
    plan: CanonicalValueUploadPlan
    owner: object
    page: PreparedCanonicalPage | None = None
    stage_fence: _CanonicalStageFence | None = None


class _PublicationPlanLease:
    """Keep one retired plan alive until its prepared step is closed."""

    __slots__ = ("__cache", "__closed")

    def __init__(self, cache: _PublicationPlanCache) -> None:
        self.__cache: _PublicationPlanCache | None = cache
        self.__closed = False

    def close(self) -> None:
        if self.__closed:
            return
        cache = self.__cache
        if cache is None:
            raise RuntimeError("publication plan cache lease lost its owner")
        cache.release()
        self.__cache = None
        self.__closed = True

    @property
    def cache(self) -> _PublicationPlanCache:
        cache = self.__cache
        if cache is None:
            raise RuntimeError("publication plan cache lease is closed")
        return cache

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException:
            pass


class _PublicationPlanCache:
    """Disposable stage-local plan with a monotone canonical validation cursor."""

    __slots__ = (
        "action",
        "authority",
        "plan",
        "__active",
        "__borrowers",
        "__closed",
        "__iterator",
        "__lock",
        "__observed_sealed",
        "__page_iterator",
        "__pending_page",
        "__retired",
    )

    def __init__(
        self,
        *,
        action: _Action,
        authority: object,
        plan: PublicationCatalogProjectionPlan | ArtifactInputProjectionPlan,
    ) -> None:
        if action not in _PLAN_ACTIONS:
            raise ValueError("publication plan cache action is not plan-backed")
        self.action = action
        self.authority = authority
        self.plan = plan
        self.__active: CanonicalValueUploadPlan | None = None
        self.__borrowers = 0
        self.__closed = False
        self.__iterator: Iterator[CanonicalValueUploadPlan] | None = (
            plan.iter_canonical_value_plans()
        )
        self.__lock = Lock()
        self.__observed_sealed: CanonicalValueReadReceipt | None = None
        self.__page_iterator: Iterator[PreparedCanonicalPage] | None = None
        self.__pending_page: PreparedCanonicalPage | None = None
        self.__retired = False

    @property
    def available(self) -> bool:
        with self.__lock:
            return not self.__retired and not self.__closed and self.__borrowers == 0

    def matches(self, action: _Action, authority: object) -> bool:
        return self.action is action and self.authority == authority

    def borrow(self) -> _PublicationPlanLease:
        with self.__lock:
            if self.__retired or self.__closed:
                raise RuntimeError("publication plan cache is retired")
            if self.__borrowers != 0:
                raise RuntimeError("publication plan cache already has a borrower")
            self.__borrowers = 1
        return _PublicationPlanLease(self)

    def release(self) -> None:
        with self.__lock:
            if self.__borrowers != 1:
                raise RuntimeError("publication plan cache lease is unbalanced")
            self.__borrowers = 0
            if self.__retired:
                self.__close()

    def current_canonical_plan(self) -> CanonicalValueUploadPlan | None:
        if self.__retired or self.__closed:
            raise RuntimeError("publication plan cache is retired")
        active = self.__active
        if active is not None:
            return active
        iterator = self.__iterator
        if iterator is None:
            return None
        try:
            active = next(iterator)
        except StopIteration:
            close = getattr(iterator, "close", None)
            if callable(close):
                close()
            self.__iterator = None
            return None
        if not isinstance(active, CanonicalValueUploadPlan):
            raise TypeError("projection yielded a non-canonical upload plan")
        self.__active = active
        return active

    def advance_canonical_plan(self, plan: CanonicalValueUploadPlan) -> None:
        if self.__active is not plan:
            raise RuntimeError("canonical cursor cannot skip its active upload plan")
        self.__close_page_iterator()
        plan.close()
        self.__active = None
        self.__observed_sealed = None

    def require_stable_sealed_observation(
        self,
        plan: CanonicalValueUploadPlan,
        sealed: CanonicalValueReadReceipt | None,
    ) -> None:
        if self.__active is not plan:
            raise RuntimeError("sealed observation lacks its active upload plan")
        observed = self.__observed_sealed
        if observed is not None and sealed != observed:
            raise RuntimeError("sealed canonical identity changed after observation")

    def record_sealed_observation(
        self,
        plan: CanonicalValueUploadPlan,
        sealed: CanonicalValueReadReceipt,
    ) -> None:
        if self.__active is not plan:
            raise RuntimeError("sealed canonical observation cursor is inconsistent")
        observed = self.__observed_sealed
        if observed is not None and observed != sealed:
            raise RuntimeError("sealed canonical identity changed after observation")
        self.__observed_sealed = sealed

    def current_canonical_page(
        self,
        plan: CanonicalValueUploadPlan,
    ) -> PreparedCanonicalPage | None:
        if self.__active is not plan:
            raise RuntimeError("canonical page cursor lacks its active upload plan")
        pending = self.__pending_page
        if pending is not None:
            return pending
        iterator = self.__page_iterator
        if iterator is None:
            iterator = plan.iter_pages()
            self.__page_iterator = iterator
        try:
            pending = next(iterator)
        except StopIteration:
            self.__close_page_iterator()
            return None
        self.__pending_page = pending
        return pending

    def advance_canonical_page(self, page: PreparedCanonicalPage) -> None:
        if self.__pending_page is not page:
            raise RuntimeError("canonical page cursor cannot skip its pending page")
        self.__pending_page = None

    def canonical_consumer_cursor(
        self,
        plan: CanonicalValueUploadPlan,
    ) -> bytes:
        if self.__active is not plan:
            raise RuntimeError("canonical consumer cursor lacks its active upload plan")
        return self.plan._canonical_consumer_cursor(plan.value_sha256)

    def claim_required(
        self,
        *,
        consumer_cursor: bytes,
        checkpoint_cursor: bytes,
        checkpoint_state: str,
    ) -> bool:
        if self.action not in _PLAN_BUILD_ACTIONS:
            return False
        if checkpoint_state == "COMPLETE":
            return False
        return consumer_cursor > checkpoint_cursor

    def retire(self) -> None:
        with self.__lock:
            if self.__retired:
                return
            self.__retired = True
            if self.__borrowers == 0:
                self.__close()

    def __close(self) -> None:
        if self.__closed:
            return
        self.__closed = True
        iterator = self.__iterator
        self.__iterator = None
        try:
            if iterator is not None:
                close = getattr(iterator, "close", None)
                if callable(close):
                    close()
        finally:
            try:
                self.__close_page_iterator()
            finally:
                active = self.__active
                self.__active = None
                self.__observed_sealed = None
                try:
                    if active is not None:
                        active.close()
                finally:
                    self.plan.close()

    def __close_page_iterator(self) -> None:
        iterator = self.__page_iterator
        self.__page_iterator = None
        self.__pending_page = None
        if iterator is not None:
            close = getattr(iterator, "close", None)
            if callable(close):
                close()

    def __del__(self) -> None:
        try:
            self.retire()
        except BaseException:
            pass


@dataclass(frozen=True, slots=True)
class _LibraryActivationPrepared:
    receipt_id: bytes
    checkpoint: LibraryActivationCheckpoint | None
    processed_rows: int
    terminal_page: bool
    ready_for_finalization: bool


class VNextIssuedPublicationStep:
    """Opaque repository-issued description of the next durable action."""

    __slots__ = ("_action", "_payload", "_session", "_token")

    def __init__(
        self,
        *,
        action: _Action,
        payload: object,
        session: VNextIngestSession,
        _token: object,
    ) -> None:
        if _token is not _STEP_TOKEN:
            raise TypeError("publication steps are issued by the orchestrator")
        self._action = action
        self._payload = payload
        self._session = session
        self._token = _token

    @property
    def operation(self) -> str:
        """Diagnostic operation name; it is not accepted back as authority."""

        return self._action.value


class VNextPreparedPublicationStep:
    """Opaque prepared work that owns all temporary local resources."""

    __slots__ = ("_action", "_closed", "_issued", "_payload", "_token")

    def __init__(
        self,
        *,
        issued: VNextIssuedPublicationStep,
        action: _Action,
        payload: object,
        _token: object,
    ) -> None:
        if _token is not _PREPARED_TOKEN:
            raise TypeError("prepared publication steps are orchestrator-issued")
        self._issued = issued
        self._action = action
        self._payload = payload
        self._closed = False
        self._token = _token

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        owner = _owned_resource(self._payload)
        close = getattr(owner, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> VNextPreparedPublicationStep:
        self._require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _require_open(self) -> None:
        if self._closed:
            raise ValueError("prepared publication step is closed")


class VNextIngestPublication:
    """Transaction-owning restartable publication state machine."""

    __slots__ = (
        "__backend",
        "__clock",
        "__closed",
        "__context",
        "__activation_checkpoints",
        "__activation_ready",
        "__artifact_receipt",
        "__artifact_receipt_lock",
        "__publication_plan",
        "__publication_plan_lock",
    )

    def __init__(
        self,
        context: RepositoryContext,
        *,
        clock: Callable[[], int] = _now_microseconds,
    ) -> None:
        if not isinstance(context, RepositoryContext):
            raise TypeError("context must be RepositoryContext")
        if not callable(clock):
            raise TypeError("clock must be callable")
        self.__context = context
        self.__backend = context.sql_type
        self.__clock = clock
        self.__closed = Event()
        # Disposable hints only.  A new process recovers both from the durable
        # library adapter before it performs any irreversible work.
        self.__activation_checkpoints: dict[bytes, LibraryActivationCheckpoint] = {}
        self.__activation_ready: set[bytes] = set()
        self.__artifact_receipt: _ArtifactReceiptCache | None = None
        self.__artifact_receipt_lock = Lock()
        self.__publication_plan: _PublicationPlanCache | None = None
        self.__publication_plan_lock = Lock()

    def close(self) -> None:
        """Release every process-local hint owned by this orchestrator.

        Issued and prepared steps are caller-owned values.  Closing the
        orchestrator does not consume them, but no later issue, prepare, or
        commit call is admitted.  A cache take that linearized before close
        transfers ownership to its prepared step; every other cached resource
        is retired exactly once here.
        """

        self.__closed.set()
        self.__activation_checkpoints.clear()
        self.__activation_ready.clear()
        with self.__publication_plan_lock:
            plan = self.__publication_plan
            self.__publication_plan = None
        try:
            if plan is not None:
                plan.retire()
        finally:
            with self.__artifact_receipt_lock:
                receipt = self.__artifact_receipt
                self.__artifact_receipt = None
            if receipt is not None:
                receipt.close()

    def __enter__(self) -> VNextIngestPublication:
        self.__require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException:
            pass

    def issue_step(
        self,
        session: VNextIngestSession,
        policy: VNextResolvedIngestPolicy,
    ) -> VNextIssuedPublicationStep:
        """Issue the next DB authority without calling an external adapter."""

        self.__require_open()
        _require_policy(policy)
        now = require_int63(self.__clock(), field="publication issue now")
        gate, _coordinated = _repository_authority(session)
        with self.__context.SQLConnector() as connector:
            with connector.transaction():
                work = VNextUnitOfWork(connector, backend=self.__backend)
                generation = _resume_authority(work, session, now)
                root = _load_root(work, session, policy)
                action, payload = _issue_database_action(
                    work,
                    generation=generation,
                    root=root,
                    policy=policy,
                    now=now,
                )

        if action is _Action.LIBRARY_ACTIVATION:
            activation = cast(_LibraryActivationWork, payload)
            if activation.receipt_id in self.__activation_ready:
                action, payload = self.__issue_finalization(
                    gate=gate,
                    receipt_id=activation.receipt_id,
                    now=now,
                )
            else:
                checkpoint = self.__activation_checkpoints.get(activation.receipt_id)
                if checkpoint is not None:
                    payload = replace(activation, checkpoint=checkpoint)

        with self.__publication_plan_lock:
            self.__require_open()
            self.__retire_mismatched_plan(action, payload)
        with self.__artifact_receipt_lock:
            self.__require_open()
            self.__retire_mismatched_artifact_receipt(action, payload)

        self.__require_open()
        return VNextIssuedPublicationStep(
            action=action,
            payload=payload,
            session=session,
            _token=_STEP_TOKEN,
        )

    def prepare_step(
        self,
        issued: VNextIssuedPublicationStep,
        *,
        artifact_adapters: Mapping[bytes, ArtifactStorageAdapter],
        finalization_adapters: Mapping[bytes, ArtifactReleaseAdapter],
        library_activation: VNextLibraryActivationAdapter,
    ) -> VNextPreparedPublicationStep:
        """Prepare local work; no fenced database write occurs here."""

        self.__require_open()
        exact = _require_issued(issued)
        _require_library_activation_adapter(library_activation)
        issued_session = exact._session
        action = exact._action
        payload: object = exact._payload

        if action in {_Action.BUILD_CATALOG, _Action.VALIDATE_CATALOG}:
            with self.__publication_plan_lock:
                self.__require_open()
                work = cast(_CandidateWork, payload)
                authority = work.payload
                cached = self.__reusable_plan(action, authority)
                if cached is None:
                    with self.__context.SQLConnector() as connector:
                        if action is _Action.BUILD_CATALOG:
                            catalog_plan = PublicationCandidateRepository.prepare_catalog_projection(
                                connector,
                                backend=self.__backend,
                                authority=authority,  # type: ignore[arg-type]
                            )
                        else:
                            catalog_plan = PublicationCandidateRepository.prepare_catalog_projection_validation(
                                connector,
                                backend=self.__backend,
                                authority=authority,  # type: ignore[arg-type]
                            )
                    cached = self.__install_plan(action, authority, catalog_plan)
                payload, action = self.__prepare_plan_action(
                    issued_session,
                    cached=cached,
                    fallback=work,
                    fallback_action=action,
                )
        elif action in {
            _Action.BUILD_ARTIFACT_INPUT,
            _Action.VALIDATE_ARTIFACT_INPUT,
        }:
            with self.__publication_plan_lock:
                self.__require_open()
                work = cast(_CandidateWork, payload)
                authority = work.payload
                cached = self.__reusable_plan(action, authority)
                if cached is None:
                    with self.__context.SQLConnector() as connector:
                        if action is _Action.BUILD_ARTIFACT_INPUT:
                            input_plan = ArtifactPreparationRepository.prepare_artifact_input_projection(
                                connector,
                                backend=self.__backend,
                                authority=authority,  # type: ignore[arg-type]
                            )
                        else:
                            input_plan = ArtifactPreparationRepository.prepare_artifact_input_validation(
                                connector,
                                backend=self.__backend,
                                authority=authority,  # type: ignore[arg-type]
                            )
                    cached = self.__install_plan(action, authority, input_plan)
                payload, action = self.__prepare_plan_action(
                    issued_session,
                    cached=cached,
                    fallback=work,
                    fallback_action=action,
                )
        elif action is _Action.PREPARE_ARTIFACT:
            prepared_artifact = self.__prepare_artifact(
                cast(_ArtifactWork, payload),
                artifact_adapters,
            )
            payload = prepared_artifact
        elif action is _Action.LIBRARY_ACTIVATION:
            payload = self.__prepare_library_activation(
                cast(_LibraryActivationWork, payload),
                library_activation,
            )
        elif action is _Action.COMPLETE:
            self.__complete_library_activation(
                cast(_Root, payload),
                library_activation,
            )
        elif action is _Action.FINALIZE:
            payload = _release_finalization_page(
                cast(PublicationFinalizationPage, payload),
                finalization_adapters,
            )

        prepared = VNextPreparedPublicationStep(
            issued=exact,
            action=action,
            payload=payload,
            _token=_PREPARED_TOKEN,
        )
        try:
            self.__require_open()
        except BaseException:
            prepared.close()
            raise
        return prepared

    def commit_step(
        self,
        session: VNextIngestSession,
        prepared: VNextPreparedPublicationStep,
    ) -> VNextIngestAdvanceResult:
        """Commit one bounded action using the current renewed session."""

        self.__require_open()
        exact = _require_prepared(prepared)
        _require_same_session_authority(exact._issued._session, session)
        gate, coordinated = _repository_authority(session)
        now = require_int63(self.__clock(), field="publication commit now")
        try:
            with self.__context.SQLConnector() as connector:
                with connector.transaction():
                    work = VNextUnitOfWork(connector, backend=self.__backend)
                    outcome = _commit_action(
                        work,
                        action=exact._action,
                        payload=exact._payload,
                        session=session,
                        gate=gate,
                        turn=coordinated.ingest_turn,
                        now=now,
                    )
            if exact._action is _Action.LIBRARY_ACTIVATION:
                activation = cast(_LibraryActivationPrepared, exact._payload)
                if activation.checkpoint is not None:
                    self.__activation_checkpoints[activation.receipt_id] = (
                        activation.checkpoint
                    )
                if activation.ready_for_finalization:
                    self.__activation_ready.add(activation.receipt_id)
            elif exact._action is _Action.FINALIZE and bool(
                getattr(outcome, "terminal", False)
            ):
                acknowledgement = cast(
                    PublicationFinalizationAcknowledgement,
                    exact._payload,
                )
                self.__activation_ready.discard(acknowledgement.page.receipt_id)
            if exact._action in _PLAN_ACTIONS and bool(
                getattr(outcome, "terminal", False)
            ):
                with self.__publication_plan_lock:
                    self.__retire_committed_plan(cast(_CandidateWork, exact._payload))
            result = _advance_result(exact._action, outcome)
            if exact._action is _Action.PREPARE_ARTIFACT:
                artifact = cast(_ArtifactPrepared, exact._payload)
                if not artifact.intents:
                    # Transfer only after the transaction context reported a
                    # successful PENDING commit.  Commit response loss keeps
                    # ownership on the prepared step and therefore rerenders.
                    self.__retain_persisted_artifact_receipt(artifact, outcome)
            return result
        finally:
            exact.close()

    def __prepare_plan_action(
        self,
        session: VNextIngestSession,
        *,
        cached: _PublicationPlanCache,
        fallback: _CandidateWork,
        fallback_action: _Action,
    ) -> tuple[object, _Action]:
        lease = cached.borrow()
        try:
            with self.__context.SQLConnector() as connector:
                with connector.read_transaction():
                    checkpoint_cursor, checkpoint_state = _load_plan_checkpoint(
                        connector,
                        candidate_id=fallback.candidate_id,
                        action=fallback_action,
                    )
                    selected = _next_cached_canonical_work(
                        connector,
                        backend=self.__backend,
                        session=session,
                        cached=cached,
                        owner=lease,
                        candidate_id=fallback.candidate_id,
                        checkpoint_cursor=checkpoint_cursor,
                        checkpoint_state=checkpoint_state,
                    )
            if selected is None:
                return (
                    replace(
                        fallback,
                        payload=cached.plan,
                        resource_owner=lease,
                    ),
                    fallback_action,
                )
            return selected
        except BaseException:
            lease.close()
            if self.__publication_plan is cached:
                self.__publication_plan = None
            cached.retire()
            raise

    def __reusable_plan(
        self,
        action: _Action,
        authority: object,
    ) -> _PublicationPlanCache | None:
        cached = self.__publication_plan
        if (
            cached is not None
            and cached.matches(action, authority)
            and cached.available
        ):
            return cached
        if cached is not None:
            self.__publication_plan = None
            cached.retire()
        return None

    def __install_plan(
        self,
        action: _Action,
        authority: object,
        plan: PublicationCatalogProjectionPlan | ArtifactInputProjectionPlan,
    ) -> _PublicationPlanCache:
        try:
            cached = _PublicationPlanCache(
                action=action,
                authority=authority,
                plan=plan,
            )
        except BaseException:
            plan.close()
            raise
        if self.__closed.is_set():
            cached.retire()
            raise ValueError("ingest publication orchestrator is closed")
        self.__publication_plan = cached
        return cached

    def __retire_mismatched_plan(self, action: _Action, payload: object) -> None:
        cached = self.__publication_plan
        if cached is None:
            return
        if (
            action in _PLAN_ACTIONS
            and isinstance(payload, _CandidateWork)
            and cached.matches(action, payload.payload)
        ):
            return
        self.__publication_plan = None
        cached.retire()

    def __retire_committed_plan(self, candidate: _CandidateWork) -> None:
        owner = candidate.resource_owner
        if not isinstance(owner, _PublicationPlanLease):
            raise RuntimeError("terminal plan action lacks its cache lease")
        committed = owner.cache
        committed.retire()
        current = self.__publication_plan
        if current is committed:
            self.__publication_plan = None
            return
        if current is not None and current.matches(
            committed.action,
            committed.authority,
        ):
            self.__publication_plan = None
            current.retire()

    def __prepare_artifact(
        self,
        work: _ArtifactWork,
        adapters: Mapping[bytes, ArtifactStorageAdapter],
    ) -> _ArtifactPrepared:
        authority = work.authority
        try:
            adapter = adapters[authority.adapter_id]
        except KeyError as error:
            raise RuntimeError("artifact storage adapter is not installed") from error
        # Immutable reads are fully spooled before renderer/protection I/O.
        with self.__context.SQLConnector() as connector:
            audit = ArtifactPreparationRepository.audit_inputs(
                connector,
                backend=self.__backend,
                authority=authority,
            )
        families = work.families
        # A cache hit never replaces the fresh durable input audit above.
        owner = (
            None
            if families is None
            else self.__take_artifact_receipt(authority, families)
        )
        if owner is not None and owner.receipt.audit != audit:
            # The durable authority/family key still matches, but the cached
            # receipt no longer refines the freshly audited inputs.  Dispose
            # the hint and take the same renderer path as a cache miss.
            owner.close()
            owner = None
        if owner is not None:
            try:
                ArtifactPreparationRepository.revalidate_cached_sources(
                    audit=audit,
                    adapter=adapter,
                )
            except BaseException:
                owner.close()
                raise
        if owner is None:
            with self.__context.SQLConnector() as connector:
                receipt = ArtifactPreparationRepository.prepare_with_storage_adapter(
                    connector,
                    backend=self.__backend,
                    audit=audit,
                    adapter=adapter,
                )
            owner = _ArtifactReceiptOwner(receipt)
        try:
            receipt = owner.receipt
            if receipt.audit != audit:
                raise RuntimeError(
                    "artifact receipt differs from its fresh input audit"
                )
            if families is None:
                return _ArtifactPrepared(owner, work.effect_seal, (), ())
            with self.__context.SQLConnector() as connector:
                with connector.read_transaction():
                    intents = tuple(
                        _protection_intent_from_family(
                            connector,
                            receipt,
                            family,
                            replayed=True,
                        )
                        for family in families
                    )
            evidence: list[ArtifactProtectionEvidence] = []
            for intent in intents:
                with self.__context.SQLConnector() as connector:
                    evidence.append(
                        ArtifactPreparationRepository.protect_prepared_artifact(
                            connector,
                            backend=self.__backend,
                            receipt=receipt,
                            intent=intent,
                            adapter=adapter,
                        )
                    )
            return _ArtifactPrepared(
                owner,
                work.effect_seal,
                intents,
                tuple(evidence),
            )
        except BaseException:
            owner.close()
            raise

    def __take_artifact_receipt(
        self,
        authority: ArtifactPreparationAuthority,
        families: tuple[PreparedArtifactFamily, ...],
    ) -> _ArtifactReceiptOwner | None:
        with self.__artifact_receipt_lock:
            cached = self.__artifact_receipt
            if self.__closed.is_set():
                self.__artifact_receipt = None
                if cached is not None:
                    cached.close()
                raise ValueError("ingest publication orchestrator is closed")
            if cached is None:
                return None
            self.__artifact_receipt = None
            if cached.matches(authority, families):
                return cached.take()
            cached.close()
            return None

    def __retain_persisted_artifact_receipt(
        self,
        prepared: _ArtifactPrepared,
        outcome: object,
    ) -> None:
        families = _pending_artifact_families(outcome, receipt=prepared.receipt)
        if families is None:
            return
        if not _artifact_receipt_fits_cache(prepared.receipt):
            # The optimization is optional.  Large receipts keep reference
            # restart behavior instead of pinning a multi-gigabyte temp spool
            # inside a long-lived publication facade.
            return
        receipt = prepared.receipt_owner.detach()
        cached = _ArtifactReceiptCache(
            authority=receipt.audit.authority,
            families=families,
            receipt=receipt,
        )
        try:
            self.__install_artifact_receipt(cached)
        except BaseException:
            cached.close()
            raise

    def __install_artifact_receipt(self, cached: _ArtifactReceiptCache) -> None:
        with self.__artifact_receipt_lock:
            current = self.__artifact_receipt
            self.__artifact_receipt = None
            if current is not None:
                current.close()
            if self.__closed.is_set():
                cached.close()
                return
            self.__artifact_receipt = cached

    def __retire_mismatched_artifact_receipt(
        self,
        action: _Action,
        payload: object,
    ) -> None:
        cached = self.__artifact_receipt
        if cached is None:
            return
        if (
            action is _Action.PREPARE_ARTIFACT
            and isinstance(payload, _ArtifactWork)
            and payload.families is not None
            and cached.matches(payload.authority, payload.families)
        ):
            return
        self.__artifact_receipt = None
        cached.close()

    def __prepare_library_activation(
        self,
        work: _LibraryActivationWork,
        adapter: VNextLibraryActivationAdapter,
    ) -> _LibraryActivationPrepared:
        current = _require_library_activation_checkpoint(
            adapter.begin(work.revision, work.receipt_id),
            revision=work.revision,
            receipt_id=work.receipt_id,
        )
        checkpoint = work.checkpoint
        if checkpoint is None:
            if current.status is LibraryActivationStatus.SPOOL:
                return _LibraryActivationPrepared(
                    work.receipt_id,
                    current,
                    0,
                    False,
                    False,
                )
            if current.status is LibraryActivationStatus.RECONCILE:
                reconciled = _require_library_activation_checkpoint(
                    adapter.reconcile_page(
                        work.revision,
                        work.receipt_id,
                        limit=_MAX_PAGE_ROWS,
                    ),
                    revision=work.revision,
                    receipt_id=work.receipt_id,
                )
                if reconciled.status not in {
                    LibraryActivationStatus.RECONCILE,
                    LibraryActivationStatus.READY,
                }:
                    raise RuntimeError(
                        "library reconcile page returned an invalid state"
                    )
                return _LibraryActivationPrepared(
                    work.receipt_id,
                    None,
                    0,
                    reconciled.status is LibraryActivationStatus.READY,
                    reconciled.status is LibraryActivationStatus.READY,
                )
            if current.status is LibraryActivationStatus.READY:
                return _LibraryActivationPrepared(
                    work.receipt_id,
                    None,
                    0,
                    True,
                    True,
                )
            return _LibraryActivationPrepared(
                work.receipt_id,
                None,
                0,
                True,
                True,
            )
        exact = _require_library_activation_checkpoint(
            checkpoint,
            revision=work.revision,
            receipt_id=work.receipt_id,
        )
        if current != exact:
            raise RuntimeError(
                "library activation checkpoint advanced after page issue"
            )
        if exact.status is not LibraryActivationStatus.SPOOL:
            raise RuntimeError("issued library activation page is not a spool page")
        with self.__context.SQLConnector() as connector:
            page = LibraryActivationResourceRepository.list_page(
                connector,
                receipt_id=work.receipt_id,
                cursor=exact.cursor,
                page_limit=_MAX_PAGE_ROWS,
            )
        items = page.items
        adapter.activate_page(work.revision, items)
        if page.terminal:
            adapter.seal(work.revision)
        # Adapter progress is durable even if commit response is lost.  Force
        # the next turn to recover its cursor from the adapter.
        self.__activation_checkpoints.pop(work.receipt_id, None)
        return _LibraryActivationPrepared(
            work.receipt_id,
            None,
            len(items),
            page.terminal,
            False,
        )

    @staticmethod
    def __complete_library_activation(
        root: _Root,
        adapter: VNextLibraryActivationAdapter,
    ) -> None:
        if root.receipt_id is None or root.revision is None:
            raise RuntimeError("completed publication lacks activation identity")
        checkpoint = _require_library_activation_checkpoint(
            adapter.begin(root.revision, root.receipt_id),
            revision=root.revision,
            receipt_id=root.receipt_id,
        )
        if checkpoint.status is LibraryActivationStatus.READY:
            adapter.complete(root.revision, root.receipt_id)
            return
        if checkpoint.status is not LibraryActivationStatus.COMPLETE:
            raise RuntimeError(
                "published library activation is neither READY nor COMPLETE"
            )

    def __issue_finalization(
        self,
        *,
        gate: GateLease,
        receipt_id: bytes,
        now: int,
    ) -> tuple[_Action, object]:
        with self.__context.SQLConnector() as connector:
            page = PublicationFinalizationRepository.issue_page(
                connector,
                backend=self.__backend,
                gate_lease=gate,
                receipt_id=receipt_id,
                batch_key=secrets.token_bytes(32),
                page_limit=_MAX_PAGE_ROWS,
                now=now,
            )
        return _Action.FINALIZE, page

    def __require_open(self) -> None:
        if self.__closed.is_set():
            raise ValueError("ingest publication orchestrator is closed")


def _release_finalization_page(
    page: PublicationFinalizationPage,
    adapters: Mapping[bytes, ArtifactReleaseAdapter],
) -> PublicationFinalizationAcknowledgement:
    """Perform release I/O from a DB-issued immutable page, with no DB call."""

    resolved = _resolve_adapters(adapters, page.items)
    for item in page.items:
        descriptor = item.storage_object
        evidence = resolved[item.adapter_id].release(
            descriptor.key,
            bytes.fromhex(descriptor.sha256),
            descriptor.size_bytes,
            item.protection_token,
        )
        if (
            type(evidence) is not ArtifactReleaseStorageEvidence
            or not evidence.released
        ):
            raise PublicationFinalizationUnavailableError(
                "storage did not acknowledge terminal protection release"
            )
    return _acknowledge(page)


def _issue_database_action(
    work: VNextUnitOfWork,
    *,
    generation: int,
    root: _Root,
    policy: VNextResolvedIngestPolicy,
    now: int,
) -> tuple[_Action, object]:
    if root.receipt_id is not None:
        if root.receipt_state == "PUBLISHED":
            return _Action.COMPLETE, root
        if root.receipt_state != "DB_COMMITTED" or root.revision is None:
            raise RuntimeError("publication receipt has an invalid durable state")
        return (
            _Action.LIBRARY_ACTIVATION,
            _LibraryActivationWork(
                root.receipt_id,
                root.revision,
            ),
        )
    if root.candidate_id is None:
        return _Action.BEGIN, _BeginWithPolicy(root, policy)

    candidate = root.candidate_id
    checkpoints = _load_checkpoints(work.connector, candidate)
    first_open = next(
        (stage for stage, state in checkpoints if state != "COMPLETE"), None
    )
    if first_open is None:
        return _Action.COMMIT_PUBLICATION, candidate

    batch_key = secrets.token_bytes(32)
    if first_open == b"BUILD_SELECTION":
        return _Action.BUILD_SELECTION, _CandidateWork(candidate, batch_key)
    if first_open == b"VALIDATE_SELECTION":
        return _Action.VALIDATE_SELECTION, _CandidateWork(candidate, batch_key)
    if first_open in {
        b"BUILD_CATALOG_PROJECTION",
        b"VALIDATE_CATALOG_PROJECTION",
    }:
        authority = (
            PublicationCandidateRepository._issue_projection_authority_authorized(
                work,
                candidate_id=candidate,
                generation=generation,
                now=now,
                validate_artifact_policy=True,
            )
        )
        action = (
            _Action.BUILD_CATALOG
            if first_open == b"BUILD_CATALOG_PROJECTION"
            else _Action.VALIDATE_CATALOG
        )
        return action, _CandidateWork(candidate, batch_key, authority)
    if first_open in {
        b"BUILD_ARTIFACT_INPUT",
        b"VALIDATE_ARTIFACT_INPUT_DELTA",
    }:
        input_authority = (
            ArtifactPreparationRepository._issue_input_projection_authority_authorized(
                work,
                candidate_id=candidate,
                generation=generation,
                now=now,
            )
        )
        action = (
            _Action.BUILD_ARTIFACT_INPUT
            if first_open == b"BUILD_ARTIFACT_INPUT"
            else _Action.VALIDATE_ARTIFACT_INPUT
        )
        return action, _CandidateWork(candidate, batch_key, input_authority)
    if first_open == b"BUILD_ARTIFACT_DELTA_OPERATION":
        return _Action.BUILD_ARTIFACT_DELTA, _CandidateWork(candidate, batch_key)
    if first_open == b"VALIDATE_PREPARED_ARTIFACT":
        special = _issue_artifact_or_operational(
            work,
            generation=generation,
            candidate_id=candidate,
            build_id=root.build_id,
            operational_policy_id=policy.operational_policy_id,
            now=now,
        )
        if special is not None:
            return special

    validators = {
        b"VALIDATE_PREPARED_ARTIFACT": _Action.VALIDATE_PREPARED,
        b"VALIDATE_CREATE": _Action.VALIDATE_CREATE,
        b"VALIDATE_REBUILD": _Action.VALIDATE_REBUILD,
        b"VALIDATE_DELETE": _Action.VALIDATE_DELETE,
        b"VALIDATE_UNCHANGED": _Action.VALIDATE_UNCHANGED,
        b"VALIDATE_NEW_GALLERY": _Action.VALIDATE_NEW,
        b"VALIDATE_CHANGED_GALLERY": _Action.VALIDATE_CHANGED,
        b"VALIDATE_REMOVED_GALLERY": _Action.VALIDATE_REMOVED,
        b"VALIDATE_DUPLICATE_LOSER": _Action.VALIDATE_DUPLICATE,
    }
    try:
        action = validators[first_open]
    except KeyError as error:
        raise RuntimeError("publication checkpoint stage is unsupported") from error
    return action, _CandidateWork(candidate, batch_key)


def _issue_artifact_or_operational(
    work: VNextUnitOfWork,
    *,
    generation: int,
    candidate_id: bytes,
    build_id: bytes,
    operational_policy_id: int,
    now: int,
) -> tuple[_Action, object] | None:
    preparation = work.connector.fetch_one(
        "SELECT p.preparation_id, p.state, checkpoint.state "
        "FROM operational_deletion_request_generation_heads AS head "
        "LEFT JOIN operational_operational_preparations AS p "
        "ON p.build_id = %s AND p.deletion_request_generation = "
        "head.current_generation AND p.operational_policy_id = %s "
        "LEFT JOIN operational_operational_preparation_checkpoints AS checkpoint "
        "ON checkpoint.preparation_id = p.preparation_id "
        "AND checkpoint.phase = 'EFFECTS' WHERE head.singleton_id = 1",
        (build_id, operational_policy_id),
    )
    if not preparation or preparation[0] is None:
        return (
            _Action.BEGIN_OPERATIONAL,
            _OperationalWork(candidate_id, build_id, operational_policy_id),
        )
    if len(preparation) != 3:
        raise RuntimeError("operational preparation authority is malformed")
    preparation_id = require_uuid16(
        preparation[0], field="publication operational preparation_id"
    )
    state = str(preparation[1])
    checkpoint_state = str(preparation[2])
    if state == "OPEN" and checkpoint_state == "OPEN":
        return (
            _Action.APPEND_OPERATIONAL,
            _OperationalWork(
                candidate_id,
                build_id,
                operational_policy_id,
                preparation_id,
            ),
        )
    if state == "OPEN" and checkpoint_state == "COMPLETE":
        return (
            _Action.SEAL_OPERATIONAL,
            _OperationalWork(
                candidate_id,
                build_id,
                operational_policy_id,
                preparation_id,
            ),
        )
    if state != "COMPLETE" or checkpoint_state != "COMPLETE":
        raise RuntimeError("operational preparation has an invalid state")
    effect_seal = OperationalEffectRepository._load_complete_seal_authorized(
        work,
        preparation_id=preparation_id,
        build_id=build_id,
    )
    row = work.connector.fetch_one(
        "SELECT operation.publication_key "
        "FROM catalog_artifact_operations AS operation "
        "WHERE operation.candidate_id = %s "
        "AND operation.operation IN ('CREATE', 'REBUILD') "
        "AND (NOT EXISTS (SELECT 1 FROM catalog_prepared_artifacts prepared "
        "WHERE prepared.candidate_id = operation.candidate_id "
        "AND prepared.publication_key = operation.publication_key) "
        "OR EXISTS (SELECT 1 FROM catalog_prepared_artifacts prepared "
        "WHERE prepared.candidate_id = operation.candidate_id "
        "AND prepared.publication_key = operation.publication_key "
        "AND prepared.state = 'PENDING')) "
        "ORDER BY operation.publication_key LIMIT 1",
        (candidate_id,),
    )
    if row:
        publication = require_digest32(row[0], field="next artifact publication_key")
        authority = ArtifactPreparationRepository._issue_authority_authorized(
            work,
            candidate_id=candidate_id,
            publication_key=publication,
            generation=generation,
            now=now,
        )
        families = load_prepared_artifact_families(
            work.connector,
            candidate_id=candidate_id,
            publication_key=publication,
            backend=work.backend,
        )
        if families and any(family.state != "PENDING" for family in families):
            raise RuntimeError("next artifact resource bundle has mixed states")
        return _Action.PREPARE_ARTIFACT, _ArtifactWork(
            authority,
            effect_seal,
            None if not families else families,
        )
    binding = work.connector.fetch_one(
        "SELECT preparation_id FROM operational_publication_candidate_preparations "
        "WHERE candidate_id = %s",
        (candidate_id,),
    )
    if not binding:
        return (
            _Action.BIND_OPERATIONAL,
            _OperationalWork(
                candidate_id,
                build_id,
                operational_policy_id,
                preparation_id,
                effect_seal,
            ),
        )
    if binding != (preparation_id,):
        raise RuntimeError("candidate operational binding changed")
    return None


def _commit_action(
    work: VNextUnitOfWork,
    *,
    action: _Action,
    payload: object,
    session: VNextIngestSession,
    gate: GateLease,
    turn: IngestTurn,
    now: int,
) -> object:
    if action is _Action.LIBRARY_ACTIVATION:
        _resume_authority(work, session, now)
        return payload
    if action is _Action.COMPLETE:
        root = cast(_Root, payload)
        if root.receipt_id is None:
            raise RuntimeError("completed publication root has no receipt")
        PublicationRepository.release_replayed_source_working(
            work,
            gate_lease=gate,
            ingest_turn=turn,
            build_id=root.build_id,
            receipt_id=root.receipt_id,
            now=now,
        )
        return root
    if action is _Action.BEGIN:
        beginning = cast(_BeginWithPolicy, payload)
        root = beginning.root
        return PublicationCandidateRepository.begin(
            work,
            gate_lease=gate,
            ingest_turn=turn,
            analysis_id=root.analysis_id,
            artifact_policy_id=root.artifact_policy_id,
            display_title_policy_id=beginning.policy.display_title_policy_id,
            artifacts_required=beginning.policy.policy.artifacts_required,
            now=now,
        )
    if action in {
        _Action.CANONICAL_ALLOCATE,
        _Action.CANONICAL_PAGE,
        _Action.CANONICAL_SEAL,
    }:
        return _commit_canonical_work(
            work,
            action=action,
            canonical=cast(_CanonicalWork, payload),
            gate=gate,
            turn=turn,
            now=now,
        )
    if action in _CANDIDATE_METHODS:
        candidate = cast(_CandidateWork, payload)
        method = _CANDIDATE_METHODS[action]
        keyword: dict[str, object] = {}
        if action in {_Action.BUILD_CATALOG, _Action.BUILD_ARTIFACT_INPUT}:
            keyword["plan"] = candidate.payload
        elif action in {_Action.VALIDATE_CATALOG, _Action.VALIDATE_ARTIFACT_INPUT}:
            keyword["validation"] = candidate.payload
        return method(
            work=work,
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=candidate.candidate_id,
            batch_key=candidate.batch_key,
            now=now,
            **keyword,
        )
    if action is _Action.BEGIN_OPERATIONAL:
        operational = cast(_OperationalWork, payload)
        return OperationalEffectRepository.begin(
            work,
            gate_lease=gate,
            ingest_turn=turn,
            build_id=operational.build_id,
            operational_policy_id=operational.operational_policy_id,
            now=now,
        )
    if action is _Action.APPEND_OPERATIONAL:
        operational = cast(_OperationalWork, payload)
        assert operational.preparation_id is not None
        effects = _derive_operational_effects(work, operational.preparation_id)
        return OperationalEffectRepository.append_batch(
            work,
            gate_lease=gate,
            ingest_turn=turn,
            preparation_id=operational.preparation_id,
            effects=effects,
            now=now,
        )
    if action is _Action.SEAL_OPERATIONAL:
        operational = cast(_OperationalWork, payload)
        assert operational.preparation_id is not None
        return OperationalEffectRepository.seal(
            work,
            gate_lease=gate,
            ingest_turn=turn,
            preparation_id=operational.preparation_id,
            now=now,
        )
    if action is _Action.BIND_OPERATIONAL:
        operational = cast(_OperationalWork, payload)
        assert operational.effect_seal is not None
        return ArtifactPreparationRepository.bind_operational_preparation(
            work,
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=operational.candidate_id,
            effect_seal=operational.effect_seal,
            now=now,
        )
    if action is _Action.PREPARE_ARTIFACT:
        prepared = cast(_ArtifactPrepared, payload)
        if not prepared.intents:
            if prepared.evidence:
                raise RuntimeError("unpersisted artifact has protection evidence")
            return ArtifactPreparationRepository.persist_prepared_artifact(
                work,
                gate_lease=gate,
                ingest_turn=turn,
                receipt=prepared.receipt,
                now=now,
            )
        if len(prepared.evidence) != len(prepared.intents):
            raise RuntimeError("durable artifact intent lacks storage evidence")
        return ArtifactPreparationRepository.confirm_prepared_artifact(
            work,
            gate_lease=gate,
            ingest_turn=turn,
            receipt=prepared.receipt,
            intents=prepared.intents,
            evidence=prepared.evidence,
            effect_seal=prepared.effect_seal,
            now=now,
        )
    if action is _Action.COMMIT_PUBLICATION:
        return PublicationRepository.commit(
            work,
            gate_lease=gate,
            ingest_turn=turn,
            candidate_id=cast(bytes, payload),
            now=now,
        )
    if action is _Action.FINALIZE:
        acknowledgement = cast(PublicationFinalizationAcknowledgement, payload)
        renewed_page = _renew_finalization_page(acknowledgement.page, session)
        renewed = replace(acknowledgement, page=renewed_page)
        activation = (
            PublicationRepository.prepare_finalized_commit_activation(
                work,
                gate_lease=gate,
                ingest_turn=turn,
                receipt_id=renewed_page.receipt_id,
                now=now,
            )
            if renewed_page.terminal
            else None
        )
        receipt = PublicationFinalizationRepository.commit_page(
            work,
            acknowledgement=renewed,
            now=now,
            _prelocked_gate_capability=(
                _PRELOCKED_GATE_CAPABILITY if activation is not None else None
            ),
        )
        if receipt.terminal and activation is not None:
            PublicationRepository.activate_finalized_commit(
                work,
                authority=activation,
            )
        return receipt
    if isinstance(payload, _LibraryActivationPrepared):
        return _resume_authority(work, session, now)
    raise RuntimeError(f"unsupported publication action {action.value}")


def _commit_canonical_work(
    work: VNextUnitOfWork,
    *,
    action: _Action,
    canonical: _CanonicalWork,
    gate: GateLease,
    turn: IngestTurn,
    now: int,
) -> object:
    if action is _Action.CANONICAL_ALLOCATE:
        fence = canonical.stage_fence
        if fence is None or fence.stage_action not in _PLAN_BUILD_ACTIONS:
            raise RuntimeError("canonical allocation lacks its publication fence")
        generation = _authorize_canonical_write(
            work,
            gate,
            turn,
            now=now,
        )
        if generation != fence.ingest_generation:
            raise RuntimeError("canonical allocation ingest generation changed")
        stage = _PLAN_STAGE_BY_ACTION[fence.stage_action]
        PublicationCandidateRepository._lock_canonical_allocation_fence_authorized(
            work,
            candidate_id=fence.candidate_id,
            stage=stage,
            first_consumer_cursor=fence.first_consumer_cursor,
        )
        return _allocate_authorized(
            work,
            generation=generation,
            plan=canonical.plan,
            now=now,
        )
    if action is _Action.CANONICAL_PAGE:
        if canonical.page is None:
            raise RuntimeError("canonical page action lacks a prepared page")
        return CanonicalValueRepository.put_page(
            work,
            gate_lease=gate,
            ingest_turn=turn,
            plan=canonical.plan,
            prepared_page=canonical.page,
            now=now,
        )
    if action is _Action.CANONICAL_SEAL:
        return CanonicalValueRepository.seal(
            work,
            gate_lease=gate,
            ingest_turn=turn,
            plan=canonical.plan,
            now=now,
        )
    raise RuntimeError("canonical commit received a non-canonical action")


_CANDIDATE_METHODS: dict[_Action, Callable[..., PublicationCandidateBatch]] = {
    _Action.BUILD_SELECTION: PublicationCandidateRepository.process_selection_batch,
    _Action.VALIDATE_SELECTION: PublicationCandidateRepository.validate_selection_batch,
    _Action.BUILD_CATALOG: PublicationCandidateRepository.process_catalog_projection_batch,
    _Action.VALIDATE_CATALOG: PublicationCandidateRepository.validate_catalog_projection_batch,
    _Action.BUILD_ARTIFACT_INPUT: ArtifactPreparationRepository.process_artifact_input_batch,
    _Action.BUILD_ARTIFACT_DELTA: ArtifactPreparationRepository.process_artifact_delta_operation_batch,
    _Action.VALIDATE_ARTIFACT_INPUT: ArtifactPreparationRepository.validate_artifact_input_delta_batch,
    _Action.VALIDATE_PREPARED: ArtifactPreparationRepository.validate_prepared_artifact_batch,
    _Action.VALIDATE_CREATE: ArtifactPreparationRepository.validate_create_batch,
    _Action.VALIDATE_REBUILD: ArtifactPreparationRepository.validate_rebuild_batch,
    _Action.VALIDATE_DELETE: ArtifactPreparationRepository.validate_delete_batch,
    _Action.VALIDATE_UNCHANGED: ArtifactPreparationRepository.validate_unchanged_batch,
    _Action.VALIDATE_NEW: ArtifactPreparationRepository.validate_new_gallery_batch,
    _Action.VALIDATE_CHANGED: ArtifactPreparationRepository.validate_changed_gallery_batch,
    _Action.VALIDATE_REMOVED: ArtifactPreparationRepository.validate_removed_gallery_batch,
    _Action.VALIDATE_DUPLICATE: ArtifactPreparationRepository.validate_duplicate_loser_batch,
}


def _load_root(
    work: VNextUnitOfWork,
    session: VNextIngestSession,
    policy: VNextResolvedIngestPolicy,
) -> _Root:
    connector = work.connector
    build_row = connector.fetch_one(
        "SELECT build_id FROM operational_source_build_generations "
        "WHERE generation = %s",
        (session.ingest_generation,),
    )
    if len(build_row) != 1:
        raise RuntimeError("ingest generation has no exact source build")
    build_id = require_uuid16(build_row[0], field="publication source build_id")
    analyses = connector.fetch_all(
        "SELECT analysis_id FROM catalog_analysis_runs "
        "WHERE build_id = %s AND policy_id = %s AND state = 'COMPLETE' "
        "ORDER BY analysis_id LIMIT 2",
        (build_id, policy.analysis_policy_id),
    )
    if len(analyses) != 1:
        raise RuntimeError("source build lacks one exact completed analysis")
    analysis_id = require_uuid16(
        analyses[0][0], field="publication completed analysis_id"
    )
    artifact = connector.fetch_one(
        "SELECT artifact_policy_id FROM catalog_artifact_policies "
        "WHERE policy_component_sha256 = %s",
        (policy.artifact_policy_sha256,),
    )
    if len(artifact) != 1:
        raise RuntimeError("resolved artifact policy lacks its compact registry ID")
    artifact_policy_id = require_positive_int63(
        artifact[0], field="publication artifact_policy_id"
    )
    candidate = connector.fetch_one(
        "SELECT working.candidate_id FROM operational_catalog_working_candidates "
        "AS working JOIN catalog_publication_candidates AS candidate "
        "ON candidate.candidate_id = working.candidate_id "
        "LEFT JOIN catalog_publication_commits AS committed "
        "ON committed.candidate_id = candidate.candidate_id "
        "WHERE working.slot = 1 AND candidate.analysis_id = %s "
        "AND committed.candidate_id IS NULL",
        (analysis_id,),
    )
    if candidate:
        return _Root(
            build_id,
            analysis_id,
            artifact_policy_id,
            require_uuid16(candidate[0], field="working publication candidate_id"),
            None,
            None,
            None,
        )
    receipts = connector.fetch_all(
        "SELECT receipt.receipt_id, receipt.revision, receipt.state "
        "FROM catalog_publication_commits AS committed "
        "JOIN catalog_publication_candidates AS candidate "
        "ON candidate.candidate_id = committed.candidate_id "
        "JOIN catalog_publication_receipts AS receipt "
        "ON receipt.receipt_id = committed.receipt_id "
        "WHERE candidate.analysis_id = %s ORDER BY receipt.receipt_id LIMIT 2",
        (analysis_id,),
    )
    if len(receipts) > 1:
        raise RuntimeError("analysis maps to multiple publication receipts")
    if receipts:
        row = receipts[0]
        return _Root(
            build_id,
            analysis_id,
            artifact_policy_id,
            None,
            require_uuid16(row[0], field="publication receipt_id"),
            require_positive_int63(row[1], field="publication catalog revision"),
            str(row[2]),
        )
    return _Root(
        build_id,
        analysis_id,
        artifact_policy_id,
        None,
        None,
        None,
        None,
    )


def _load_checkpoints(
    connector: SQLConnector,
    candidate_id: bytes,
) -> tuple[tuple[bytes, str], ...]:
    rows = connector.fetch_all(
        "SELECT checkpoint.stage, checkpoint.state "
        "FROM catalog_publication_checkpoints AS checkpoint "
        "JOIN catalog_publication_stages AS ordering "
        "ON ordering.stage = checkpoint.stage "
        "WHERE checkpoint.candidate_id = %s ORDER BY ordering.stage_order",
        (candidate_id,),
    )
    result = tuple((bytes(row[0]), str(row[1])) for row in rows)
    if tuple(stage for stage, _state in result) != _CANDIDATE_STAGES:
        raise RuntimeError(
            "publication candidate checkpoint registry is incomplete or reordered"
        )
    if any(state not in {"OPEN", "COMPLETE"} for _stage, state in result):
        raise RuntimeError("publication checkpoint has an invalid state")
    seen_open = False
    for _stage, state in result:
        if state == "OPEN":
            seen_open = True
        elif seen_open:
            raise RuntimeError("publication checkpoints are not prefix-complete")
    return result


def _load_plan_checkpoint(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
    action: _Action,
) -> tuple[bytes, str]:
    stage = _PLAN_STAGE_BY_ACTION.get(action)
    if stage is None:
        raise ValueError("publication plan action has no durable stage")
    candidate = require_uuid16(candidate_id, field="publication plan candidate_id")
    row = connector.fetch_one(
        "SELECT `cursor`, state FROM catalog_publication_checkpoints "
        "WHERE candidate_id = %s AND stage = %s",
        (candidate, stage),
    )
    if len(row) != 2:
        raise RuntimeError("publication plan checkpoint is missing or malformed")
    cursor = require_bounded_bytes(
        row[0],
        field="publication plan checkpoint cursor",
        maximum=2048,
    )
    state = str(row[1])
    if state not in {"OPEN", "COMPLETE"}:
        raise RuntimeError("publication plan checkpoint has an invalid state")
    return cursor, state


def _next_canonical_work(
    connector: SQLConnector,
    *,
    backend: str,
    session: VNextIngestSession,
    plans: tuple[CanonicalValueUploadPlan, ...],
    owner: object,
    claim_required: Callable[[CanonicalValueUploadPlan], bool] | None = None,
) -> tuple[_CanonicalWork, _Action] | None:
    generation = require_positive_int63(
        session.ingest_generation,
        field="canonical upload generation",
    )
    for plan in plans:
        selected = _next_canonical_plan_work(
            connector,
            backend=backend,
            generation=generation,
            plan=plan,
            owner=owner,
            claim_required=(True if claim_required is None else claim_required(plan)),
        )
        if selected is not None:
            return selected
    return None


def _next_cached_canonical_work(
    connector: SQLConnector,
    *,
    backend: str,
    session: VNextIngestSession,
    cached: _PublicationPlanCache,
    owner: object,
    candidate_id: bytes,
    checkpoint_cursor: bytes,
    checkpoint_state: str,
) -> tuple[_CanonicalWork, _Action] | None:
    generation = require_positive_int63(
        session.ingest_generation,
        field="canonical upload generation",
    )
    while (plan := cached.current_canonical_plan()) is not None:
        consumer_cursor = cached.canonical_consumer_cursor(plan)
        required = cached.claim_required(
            consumer_cursor=consumer_cursor,
            checkpoint_cursor=checkpoint_cursor,
            checkpoint_state=checkpoint_state,
        )
        fence = _CanonicalStageFence(
            candidate_id,
            cached.action,
            consumer_cursor,
            generation,
        )
        sealed, claim = _load_canonical_plan_state(
            connector,
            generation=generation,
            plan=plan,
        )
        cached.require_stable_sealed_observation(plan, sealed)
        if sealed is not None:
            _compare_sealed_canonical_plan(
                connector,
                backend=backend,
                plan=plan,
                sealed=sealed,
            )
            cached.record_sealed_observation(plan, sealed)
            if required and claim is None:
                return (
                    _CanonicalWork(plan, owner, stage_fence=fence),
                    _Action.CANONICAL_ALLOCATE,
                )
            cached.advance_canonical_plan(plan)
            continue
        if not required:
            raise RuntimeError("consumed canonical value is no longer exactly sealed")
        if claim is None:
            return (
                _CanonicalWork(plan, owner, stage_fence=fence),
                _Action.CANONICAL_ALLOCATE,
            )
        while (page := cached.current_canonical_page(plan)) is not None:
            if not _canonical_page_is_exact(connector, page):
                return (
                    _CanonicalWork(plan, owner, page, fence),
                    _Action.CANONICAL_PAGE,
                )
            cached.advance_canonical_page(page)
        return (
            _CanonicalWork(plan, owner, stage_fence=fence),
            _Action.CANONICAL_SEAL,
        )
    return None


def _next_canonical_plan_work(
    connector: SQLConnector,
    *,
    backend: str,
    generation: int,
    plan: CanonicalValueUploadPlan,
    owner: object,
    claim_required: bool,
) -> tuple[_CanonicalWork, _Action] | None:
    sealed, claim = _load_canonical_plan_state(
        connector,
        generation=generation,
        plan=plan,
    )
    if sealed is not None:
        _compare_sealed_canonical_plan(
            connector,
            backend=backend,
            plan=plan,
            sealed=sealed,
        )
        if claim_required and claim is None:
            return _CanonicalWork(plan, owner), _Action.CANONICAL_ALLOCATE
        return None
    if not claim_required:
        raise RuntimeError("consumed canonical value is no longer exactly sealed")
    if claim is None:
        return _CanonicalWork(plan, owner), _Action.CANONICAL_ALLOCATE
    for page in plan.iter_pages():
        if not _canonical_page_is_exact(connector, page):
            return _CanonicalWork(plan, owner, page), _Action.CANONICAL_PAGE
    return _CanonicalWork(plan, owner), _Action.CANONICAL_SEAL


def _load_canonical_plan_state(
    connector: SQLConnector,
    *,
    generation: int,
    plan: CanonicalValueUploadPlan,
) -> tuple[CanonicalValueReadReceipt | None, tuple[int, bytes] | None]:
    value = require_digest32(
        plan.value_sha256,
        field="canonical upload plan value_sha256",
    )
    try:
        sealed = load_sealed_value_identity(
            connector,
            value_sha256=value,
        )
    except (
        CanonicalValueCollisionError,
        CanonicalValuePartialFamilyError,
    ) as error:
        raise RuntimeError("canonical sealed identity is partial or corrupt") from error
    claim_row = connector.fetch_one(
        "SELECT generation, value_sha256 FROM operational_canonical_value_uploads "
        "WHERE generation = %s AND value_sha256 = %s",
        (generation, value),
    )
    claim = _require_canonical_claim(
        claim_row,
        generation=generation,
        value_sha256=value,
    )
    return sealed, claim


def _canonical_page_is_exact(
    connector: SQLConnector,
    page: PreparedCanonicalPage,
) -> bool:
    try:
        family = load_page_family(
            connector,
            page_sha256=page.page_sha256,
        )
    except (
        CanonicalValueCollisionError,
        CanonicalValuePartialFamilyError,
    ) as error:
        raise RuntimeError("canonical page family is partial or corrupt") from error
    if family is None:
        return False
    if family.page_bytes != page.page_bytes:
        raise RuntimeError("canonical page digest collides with another exact preimage")
    return True


def _require_canonical_claim(
    row: tuple[object, ...],
    *,
    generation: int,
    value_sha256: bytes,
) -> tuple[int, bytes] | None:
    if not row:
        return None
    if len(row) != 2:
        raise RuntimeError("canonical upload claim is malformed")
    try:
        actual = (
            require_positive_int63(row[0], field="canonical claim generation"),
            require_digest32(row[1], field="canonical claim value_sha256"),
        )
    except (TypeError, ValueError) as error:
        raise RuntimeError("canonical upload claim is malformed") from error
    expected = (generation, value_sha256)
    if actual != expected:
        raise RuntimeError("canonical upload claim belongs to another authority")
    return actual


class _CanonicalPreimageComparator:
    def __init__(self, expected: Iterator[bytes]) -> None:
        self.__expected = expected
        self.__buffer = bytearray()
        self.__ended = False

    def consume(self, actual: bytes) -> None:
        part = require_bounded_bytes(
            actual,
            field="stored canonical preimage part",
            maximum=64 * 1024,
        )
        needed = len(part)
        while len(self.__buffer) < needed and not self.__ended:
            try:
                self.__buffer.extend(next(self.__expected))
            except StopIteration:
                self.__ended = True
        if bytes(self.__buffer[:needed]) != part:
            raise RuntimeError(
                "sealed canonical identity differs from the plan's exact preimage"
            )
        del self.__buffer[:needed]

    def finish(self) -> None:
        while not self.__ended:
            try:
                remaining = require_bounded_bytes(
                    next(self.__expected),
                    field="expected canonical preimage part",
                    maximum=64 * 1024,
                )
            except StopIteration:
                self.__ended = True
                break
            if remaining:
                self.__buffer.extend(remaining)
                break
        if self.__buffer or not self.__ended:
            raise RuntimeError(
                "sealed canonical identity differs at exact preimage EOF"
            )


def _compare_sealed_canonical_plan(
    connector: SQLConnector,
    *,
    backend: str,
    plan: CanonicalValueUploadPlan,
    sealed: CanonicalValueReadReceipt,
) -> None:
    comparator = _CanonicalPreimageComparator(plan.iter_payload_parts())
    try:
        receipt = CanonicalValueRepository.stream_and_validate(
            VNextUnitOfWork(connector, backend=backend),
            value_sha256=plan.value_sha256,
            consume_provisional=comparator.consume,
        )
    except (CanonicalValueCollisionError, CanonicalValueNotReadyError) as error:
        raise RuntimeError(
            "sealed canonical identity failed full tree validation"
        ) from error
    comparator.finish()
    expected = (
        plan.value_sha256,
        plan.digest_domain,
        plan.byte_count,
        sealed.root_page_sha256,
    )
    actual = (
        receipt.value_sha256,
        receipt.digest_domain,
        receipt.byte_count,
        receipt.root_page_sha256,
    )
    if actual != expected:
        raise RuntimeError(
            "sealed canonical identity receipt differs from the upload plan"
        )


def _derive_operational_effects(
    work: VNextUnitOfWork,
    preparation_id: bytes,
) -> tuple[OperationalEffect, ...]:
    preparation = require_uuid16(
        preparation_id, field="operational effect preparation_id"
    )
    row = work.connector.fetch_one(
        "SELECT policy.max_batch_rows, preparation.deletion_request_generation, "
        "head.current_generation FROM operational_operational_preparations "
        "AS preparation JOIN operational_operational_policys AS policy "
        "ON policy.operational_policy_id = preparation.operational_policy_id "
        "JOIN operational_deletion_request_generation_heads AS head "
        "ON head.singleton_id = 1 WHERE preparation.preparation_id = %s",
        (preparation,),
    )
    if len(row) != 3 or int(row[1]) != int(row[2]):
        raise RuntimeError("operational queue generation changed before batching")
    limit = min(
        require_positive_int63(row[0], field="operational effect page limit"),
        _MAX_PAGE_ROWS,
    )
    removed = work.connector.fetch_all(
        "SELECT removed.gid FROM operational_removed_gids AS removed "
        "WHERE NOT EXISTS (SELECT 1 "
        "FROM operational_operational_events AS event "
        "JOIN operational_operational_removed_gid_events AS subtype "
        "ON subtype.event_id = event.event_id "
        "WHERE event.preparation_id = %s AND subtype.gid = removed.gid) "
        "ORDER BY removed.gid LIMIT %s",
        (preparation, limit),
    )
    effects: list[OperationalEffect] = []
    for row in removed:
        effects.append(
            RemovedGid(
                require_positive_int63(row[0], field="removed gid"),
                _new_removed_token(work),
            )
        )
    remaining = limit - len(effects)
    if remaining:
        deletions = work.connector.fetch_all(
            "SELECT head.gid, head.request_token "
            "FROM operational_deletion_request_heads AS head "
            "WHERE NOT EXISTS (SELECT 1 "
            "FROM operational_operational_deletion_consumption_events AS consumed "
            "WHERE consumed.deletion_request_token = head.request_token) "
            "ORDER BY head.gid LIMIT %s",
            (remaining,),
        )
        for row in deletions:
            effects.append(
                DeletionConsumption(
                    require_positive_int63(row[0], field="deletion gid"),
                    require_uuid16(row[1], field="deletion request token"),
                )
            )
    if len(effects) > _MAX_PAGE_ROWS:
        raise RuntimeError("operational effect planner exceeded 128 rows")
    return tuple(effects)


def _new_removed_token(work: VNextUnitOfWork) -> bytes:
    for _attempt in range(8):
        token = secrets.token_bytes(16)
        if not work.connector.fetch_one(
            "SELECT request_token FROM operational_operational_removed_gid_events "
            "WHERE request_token = %s",
            (token,),
        ):
            return token
    raise RuntimeError("unable to allocate a unique removed-GID request token")


def _repository_authority(
    session: VNextIngestSession,
) -> tuple[GateLease, CoordinatedIngestTurn]:
    if not isinstance(session, VNextIngestSession):
        raise TypeError("session must be VNextIngestSession")
    session.__post_init__()
    gate = GateLease(
        session.gate_owner_token,
        session.gate_generation,
        GateMode.SHARED,
        (session.gate_slot,),
        session.gate_lease_expires_at,
    )
    kind = HandoffKind(session.handoff_kind) if session.handoff_kind else None
    coordinated = CoordinatedIngestTurn(
        IngestTurn(
            session.ingest_generation,
            session.ingest_owner_token,
            session.ingest_lease_expires_at,
        ),
        session.download_generation,
        session.handoff_owner_token,
        kind,
        session.consumed_at,
    )
    return gate, coordinated


def _resume_authority(
    work: VNextUnitOfWork,
    session: VNextIngestSession,
    now: int,
) -> int:
    gate, coordinated = _repository_authority(session)
    MaintenanceGateRepository.resume(work, gate, now=now)
    current = DownloadIngestRepository.resume_ingest(work, coordinated, now=now)
    return require_positive_int63(
        current.ingest_turn.generation,
        field="publication ingest generation",
    )


def _session_identity(session: VNextIngestSession) -> tuple[object, ...]:
    if not isinstance(session, VNextIngestSession):
        raise TypeError("session must be VNextIngestSession")
    session.__post_init__()
    return (
        session.gate_owner_token,
        session.gate_generation,
        session.gate_slot,
        session.ingest_generation,
        session.ingest_owner_token,
        session.download_generation,
        session.handoff_owner_token,
        session.handoff_kind,
        session.consumed_at,
    )


def _require_same_session_authority(
    issued: VNextIngestSession,
    current: VNextIngestSession,
) -> None:
    if _session_identity(issued) != _session_identity(current):
        raise ValueError("publication step belongs to another ingest session authority")


def _renew_finalization_page(
    page: PublicationFinalizationPage,
    session: VNextIngestSession,
) -> PublicationFinalizationPage:
    gate, _turn = _repository_authority(session)
    return replace(page, gate_lease=gate)


def _require_policy(policy: VNextResolvedIngestPolicy) -> None:
    if not isinstance(policy, VNextResolvedIngestPolicy):
        raise TypeError("policy must be VNextResolvedIngestPolicy")
    policy.__post_init__()


def _require_library_activation_adapter(
    adapter: VNextLibraryActivationAdapter,
) -> None:
    if not isinstance(adapter, VNextLibraryActivationAdapter):
        raise TypeError("library_activation must implement its neutral adapter port")


def _require_library_activation_checkpoint(
    checkpoint: LibraryActivationCheckpoint,
    *,
    revision: int | None = None,
    receipt_id: bytes | None = None,
) -> LibraryActivationCheckpoint:
    if not isinstance(checkpoint, LibraryActivationCheckpoint):
        raise TypeError("library activation adapter returned a foreign checkpoint")
    checkpoint.__post_init__()
    if revision is not None and checkpoint.revision != revision:
        raise ValueError("library activation checkpoint belongs to another revision")
    if receipt_id is not None and checkpoint.receipt_id != receipt_id:
        raise ValueError("library activation checkpoint belongs to another receipt")
    return checkpoint


def _require_issued(
    issued: VNextIssuedPublicationStep,
) -> VNextIssuedPublicationStep:
    if (
        not isinstance(issued, VNextIssuedPublicationStep)
        or issued._token is not _STEP_TOKEN
    ):
        raise TypeError("issued must be an orchestrator-issued publication step")
    return issued


def _require_prepared(
    prepared: VNextPreparedPublicationStep,
) -> VNextPreparedPublicationStep:
    if (
        not isinstance(prepared, VNextPreparedPublicationStep)
        or prepared._token is not _PREPARED_TOKEN
    ):
        raise TypeError("prepared must be an orchestrator-prepared publication step")
    prepared._require_open()
    return prepared


def _pending_artifact_families(
    outcome: object,
    *,
    receipt: ArtifactPreparationReceipt,
) -> tuple[PreparedArtifactFamily, ...] | None:
    if not isinstance(outcome, tuple) or not outcome:
        raise RuntimeError("persisted artifact did not return its resource intents")
    authority = receipt.audit.authority
    families: list[PreparedArtifactFamily] = []
    all_pending = True
    for item in outcome:
        if not isinstance(item, ArtifactProtectionIntent):
            raise RuntimeError("persisted artifact returned a foreign resource intent")
        item.__post_init__()
        if (
            item.candidate_id != authority.candidate_id
            or item.publication_key != authority.publication_key
        ):
            raise RuntimeError("persisted artifact intent belongs to another authority")
        all_pending = all_pending and item.state == "PENDING"
        families.append(
            PreparedArtifactFamily(
                item.candidate_id,
                item.publication_key,
                item.resource_kind,
                item.storage_object_key_sha256,
                item.storage_generation,
                item.protection_token,
                item.state,
            )
        )
    exact = tuple(families)
    if tuple(family.resource_kind for family in exact) != receipt.resource_kinds:
        raise RuntimeError("persisted artifact intents do not cover its receipt bundle")
    return exact if all_pending else None


def _artifact_receipt_fits_cache(receipt: ArtifactPreparationReceipt) -> bool:
    cached_resource_bytes = receipt.size_bytes
    if CatalogResourceKind.THUMBNAIL in receipt.resource_kinds:
        cached_resource_bytes += receipt.resource_descriptor(
            CatalogResourceKind.THUMBNAIL
        ).size_bytes
    return cached_resource_bytes <= _MAX_CACHED_ARTIFACT_RESOURCE_BYTES


def _owned_resource(payload: object) -> object:
    if isinstance(payload, _CanonicalWork):
        return payload.owner
    if isinstance(payload, _CandidateWork) and isinstance(
        payload.payload,
        (PublicationCatalogProjectionPlan, ArtifactInputProjectionPlan),
    ):
        if payload.resource_owner is not None:
            return payload.resource_owner
        return payload.payload
    if isinstance(payload, _ArtifactPrepared):
        return payload.receipt_owner
    return payload


def _advance_result(action: _Action, outcome: object) -> VNextIngestAdvanceResult:
    phase = (
        VNextIngestPhase.FINALIZATION
        if action in {_Action.LIBRARY_ACTIVATION, _Action.FINALIZE, _Action.COMPLETE}
        else VNextIngestPhase.PUBLICATION
    )
    rows = 0
    terminal = action is _Action.COMPLETE
    replayed = bool(getattr(outcome, "replayed", False))
    if isinstance(outcome, PublicationCandidateBatch):
        rows = outcome.row_count
    elif hasattr(outcome, "row_count"):
        rows = require_int63(getattr(outcome, "row_count"), field="publication rows")
    elif isinstance(outcome, _LibraryActivationPrepared):
        rows = outcome.processed_rows
    return VNextIngestAdvanceResult(phase, rows, terminal, replayed)


@dataclass(frozen=True, slots=True)
class _BeginWithPolicy:
    root: _Root
    policy: VNextResolvedIngestPolicy = field(repr=False)
