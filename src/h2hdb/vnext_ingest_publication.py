"""Restartable publication orchestration for the vNext ingest facade.

The public facade delegates one ``issue -> prepare -> commit`` step at a time
to :class:`VNextIngestPublication`.  Issue and commit calls own fresh, short
database transactions.  Rendering, storage protection/release, and current
projection work happen only after the issuing transaction has ended.

The caller never supplies a stage, cursor, count, digest, batch key, or
publication identity.  The next action is reconstructed from sealed database
facts and repository-owned checkpoints.  Opaque issued/prepared values bind a
step to the stable identity of an ingest session while deliberately excluding
lease expiry, so the commit side accepts a renewed copy of the same authority.
"""

from __future__ import annotations

__all__ = [
    "CurrentProjectionCheckpoint",
    "CurrentProjectionStatus",
    "VNextCurrentProjectionAdapter",
    "VNextIngestPublication",
    "VNextIssuedPublicationStep",
    "VNextPreparedPublicationStep",
]

import secrets
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from tempfile import TemporaryFile
from time import time_ns
from typing import BinaryIO, Protocol, cast, runtime_checkable

from . import vnext_identity as identity
from .domain import (
    ArtifactReleaseStorageEvidence,
    ArtifactStorageEvidence,
    VNextCurrentProjectionItem,
    VNextIngestAdvanceResult,
    VNextIngestPhase,
    VNextIngestSession,
    VNextResolvedIngestPolicy,
)
from .ports import ArtifactReleaseAdapter, ArtifactStorageAdapter
from .repository import RepositoryContext
from .sql_connector import SQLConnector
from .vnext_artifact_family import (
    PreparedArtifactFamily,
    load_prepared_artifact_family,
)
from .vnext_artifact_preparation_repository import (
    _PREPARATION_RECEIPT_TOKEN,
    _PROTECTION_EVIDENCE_TOKEN,
    _PROTECTION_INTENT_TOKEN,
    ArtifactInputProjectionPlan,
    ArtifactPreparationAuthority,
    ArtifactPreparationConflictError,
    ArtifactPreparationContractUnavailableError,
    ArtifactPreparationInputAudit,
    ArtifactPreparationNotReadyError,
    ArtifactPreparationReceipt,
    ArtifactPreparationRepository,
    ArtifactProtectionEvidence,
    ArtifactProtectionIntent,
    _ArchiveSourcePlan,
    _hash_stream,
    _prepare_archive_source_plan,
    _validate_canonical_archive,
    _write_canonical_archive,
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
)
from .vnext_current_projection_repository import (
    CurrentProjectionArtifactItem,
    CurrentProjectionArtifactRepository,
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


class CurrentProjectionStatus(StrEnum):
    """Durable state owned by the current-projection adapter."""

    SPOOL = "SPOOL"
    RECONCILE = "RECONCILE"
    COMPLETE = "COMPLETE"


@dataclass(frozen=True, slots=True)
class CurrentProjectionCheckpoint:
    """Adapter-owned restart checkpoint; its cursor is never caller input."""

    revision: int
    receipt_id: bytes
    status: CurrentProjectionStatus
    last_publication_key: bytes | None

    def __post_init__(self) -> None:
        require_positive_int63(
            self.revision,
            field="current projection adapter revision",
        )
        require_uuid16(
            self.receipt_id,
            field="current projection adapter receipt_id",
        )
        if not isinstance(self.status, CurrentProjectionStatus):
            raise TypeError("current projection status is not registered")
        cursor = self.last_publication_key
        if cursor is not None:
            require_digest32(cursor, field="current projection adapter cursor")
        if self.status is not CurrentProjectionStatus.SPOOL and cursor is not None:
            raise ValueError("only an open projection spool may expose a cursor")


@runtime_checkable
class VNextCurrentProjectionAdapter(Protocol):
    """Neutral crash-safe current-projection port consumed by core."""

    def begin(
        self,
        revision: int,
        receipt_id: bytes,
    ) -> CurrentProjectionCheckpoint: ...

    def append_page(
        self,
        revision: int,
        items: Sequence[VNextCurrentProjectionItem],
    ) -> None: ...

    def seal(self, revision: int) -> None: ...

    def reconcile(self, revision: int) -> None: ...


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
    CURRENT_PROJECTION = "CURRENT_PROJECTION"
    FINALIZE = "FINALIZE"
    COMPLETE = "COMPLETE"
    CANONICAL_ALLOCATE = "CANONICAL_ALLOCATE"
    CANONICAL_PAGE = "CANONICAL_PAGE"
    CANONICAL_SEAL = "CANONICAL_SEAL"


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
    intent: ArtifactProtectionIntent | None


@dataclass(frozen=True, slots=True)
class _ArtifactPrepared:
    receipt: ArtifactPreparationReceipt = field(repr=False, compare=False)
    effect_seal: OperationalEffectSeal
    intent: ArtifactProtectionIntent | None
    evidence: ArtifactProtectionEvidence | None


@dataclass(frozen=True, slots=True)
class _ProjectionWork:
    receipt_id: bytes
    revision: int
    checkpoint: CurrentProjectionCheckpoint | None = None
    items: tuple[VNextCurrentProjectionItem, ...] | None = None
    terminal: bool = False


@dataclass(frozen=True, slots=True)
class _CanonicalWork:
    plan: CanonicalValueUploadPlan
    owner: object
    page: PreparedCanonicalPage | None = None


@dataclass(frozen=True, slots=True)
class _ProjectionPrepared:
    receipt_id: bytes
    checkpoint: CurrentProjectionCheckpoint | None
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
        "__context",
        "__projection_cursors",
        "__projection_ready",
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
        # Disposable hints only.  A new process recovers both from the durable
        # projection adapter before it performs any irreversible work.
        self.__projection_cursors: dict[bytes, CurrentProjectionCheckpoint] = {}
        self.__projection_ready: set[bytes] = set()

    def issue_step(
        self,
        session: VNextIngestSession,
        policy: VNextResolvedIngestPolicy,
    ) -> VNextIssuedPublicationStep:
        """Issue the next DB authority without calling an external adapter."""

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

        if action is _Action.CURRENT_PROJECTION:
            projection = cast(_ProjectionWork, payload)
            if projection.receipt_id in self.__projection_ready:
                action, payload = self.__issue_finalization(
                    gate=gate,
                    receipt_id=projection.receipt_id,
                    now=now,
                )
            else:
                checkpoint = self.__projection_cursors.get(projection.receipt_id)
                if checkpoint is not None:
                    payload = replace(projection, checkpoint=checkpoint)

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
        current_projection: VNextCurrentProjectionAdapter,
    ) -> VNextPreparedPublicationStep:
        """Prepare local work; no fenced database write occurs here."""

        exact = _require_issued(issued)
        _require_projection_adapter(current_projection)
        issued_session = exact._session
        action = exact._action
        payload: object = exact._payload

        if action in {_Action.BUILD_CATALOG, _Action.VALIDATE_CATALOG}:
            work = cast(_CandidateWork, payload)
            authority = work.payload
            with self.__context.SQLConnector() as connector:
                if action is _Action.BUILD_CATALOG:
                    catalog_plan = (
                        PublicationCandidateRepository.prepare_catalog_projection(
                            connector,
                            backend=self.__backend,
                            authority=authority,  # type: ignore[arg-type]
                        )
                    )
                else:
                    catalog_plan = PublicationCandidateRepository.prepare_catalog_projection_validation(
                        connector,
                        backend=self.__backend,
                        authority=authority,  # type: ignore[arg-type]
                    )
            payload, action = self.__prepare_plan_action(
                issued_session,
                plan=catalog_plan,
                owner=catalog_plan,
                fallback=work,
                fallback_action=action,
            )
        elif action in {
            _Action.BUILD_ARTIFACT_INPUT,
            _Action.VALIDATE_ARTIFACT_INPUT,
        }:
            work = cast(_CandidateWork, payload)
            authority = work.payload
            with self.__context.SQLConnector() as connector:
                if action is _Action.BUILD_ARTIFACT_INPUT:
                    input_plan = (
                        ArtifactPreparationRepository.prepare_artifact_input_projection(
                            connector,
                            backend=self.__backend,
                            authority=authority,  # type: ignore[arg-type]
                        )
                    )
                else:
                    input_plan = (
                        ArtifactPreparationRepository.prepare_artifact_input_validation(
                            connector,
                            backend=self.__backend,
                            authority=authority,  # type: ignore[arg-type]
                        )
                    )
            payload, action = self.__prepare_plan_action(
                issued_session,
                plan=input_plan,
                owner=input_plan,
                fallback=work,
                fallback_action=action,
            )
        elif action is _Action.PREPARE_ARTIFACT:
            prepared_artifact = self.__prepare_artifact(
                cast(_ArtifactWork, payload),
                artifact_adapters,
            )
            try:
                if prepared_artifact.intent is None:
                    payload, action = self.__prepare_artifact_action(
                        issued_session,
                        prepared_artifact,
                    )
                else:
                    payload = prepared_artifact
            except BaseException:
                prepared_artifact.receipt.close()
                raise
        elif action is _Action.CURRENT_PROJECTION:
            payload = self.__prepare_current_projection(
                cast(_ProjectionWork, payload),
                current_projection,
            )
        elif action is _Action.FINALIZE:
            payload = _release_finalization_page(
                cast(PublicationFinalizationPage, payload),
                finalization_adapters,
            )

        return VNextPreparedPublicationStep(
            issued=exact,
            action=action,
            payload=payload,
            _token=_PREPARED_TOKEN,
        )

    def commit_step(
        self,
        session: VNextIngestSession,
        prepared: VNextPreparedPublicationStep,
    ) -> VNextIngestAdvanceResult:
        """Commit one bounded action using the current renewed session."""

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
            if exact._action is _Action.CURRENT_PROJECTION:
                projection = cast(_ProjectionPrepared, exact._payload)
                if projection.checkpoint is not None:
                    self.__projection_cursors[projection.receipt_id] = (
                        projection.checkpoint
                    )
                if projection.ready_for_finalization:
                    self.__projection_ready.add(projection.receipt_id)
            elif exact._action is _Action.FINALIZE and bool(
                getattr(outcome, "terminal", False)
            ):
                acknowledgement = cast(
                    PublicationFinalizationAcknowledgement,
                    exact._payload,
                )
                self.__projection_ready.discard(acknowledgement.page.receipt_id)
            return _advance_result(exact._action, outcome)
        finally:
            exact.close()

    def __prepare_plan_action(
        self,
        session: VNextIngestSession,
        *,
        plan: PublicationCatalogProjectionPlan | ArtifactInputProjectionPlan,
        owner: object,
        fallback: _CandidateWork,
        fallback_action: _Action,
    ) -> tuple[object, _Action]:
        with self.__context.SQLConnector() as connector:
            with connector.read_transaction():
                selected = _next_canonical_work(
                    connector,
                    backend=self.__backend,
                    session=session,
                    plans=tuple(plan.iter_canonical_value_plans()),
                    owner=owner,
                )
        if selected is None:
            return replace(fallback, payload=plan), fallback_action
        return selected

    def __prepare_artifact_action(
        self,
        session: VNextIngestSession,
        prepared: _ArtifactPrepared,
    ) -> tuple[object, _Action]:
        with self.__context.SQLConnector() as connector:
            with connector.read_transaction():
                selected = _next_canonical_work(
                    connector,
                    backend=self.__backend,
                    session=session,
                    plans=(prepared.receipt.locator_plan,),
                    owner=prepared.receipt,
                )
        if selected is None:
            return prepared, _Action.PREPARE_ARTIFACT
        return selected

    def __prepare_artifact(
        self,
        work: _ArtifactWork,
        adapters: Mapping[bytes, ArtifactStorageAdapter],
    ) -> _ArtifactPrepared:
        authority = work.authority
        adapter_id = authority.storage_codec[1]
        try:
            adapter = adapters[adapter_id]
        except KeyError as error:
            raise RuntimeError("artifact storage adapter is not installed") from error
        # Immutable reads are fully spooled before renderer/protection I/O.
        with self.__context.SQLConnector() as connector:
            audit = ArtifactPreparationRepository.audit_inputs(
                connector,
                backend=self.__backend,
                authority=authority,
            )
        with self.__context.SQLConnector() as connector:
            archive_plan = _prepare_archive_source_plan(
                connector,
                backend=self.__backend,
                audit=audit,
            )
        receipt = _render_prepared_artifact(audit, archive_plan, adapter)
        try:
            evidence = (
                None
                if work.intent is None
                else _protect_prepared_artifact_local(
                    receipt,
                    work.intent,
                    adapter,
                )
            )
            return _ArtifactPrepared(
                receipt,
                work.effect_seal,
                work.intent,
                evidence,
            )
        except BaseException:
            receipt.close()
            raise

    def __prepare_current_projection(
        self,
        work: _ProjectionWork,
        adapter: VNextCurrentProjectionAdapter,
    ) -> _ProjectionPrepared:
        current = _require_projection_checkpoint(
            adapter.begin(work.revision, work.receipt_id),
            revision=work.revision,
            receipt_id=work.receipt_id,
        )
        checkpoint = work.checkpoint
        if checkpoint is None:
            if current.status is CurrentProjectionStatus.SPOOL:
                return _ProjectionPrepared(
                    work.receipt_id,
                    current,
                    0,
                    False,
                    False,
                )
            if current.status is CurrentProjectionStatus.RECONCILE:
                adapter.reconcile(work.revision)
                return _ProjectionPrepared(
                    work.receipt_id,
                    None,
                    0,
                    True,
                    False,
                )
            return _ProjectionPrepared(
                work.receipt_id,
                None,
                0,
                True,
                True,
            )
        exact = _require_projection_checkpoint(
            checkpoint,
            revision=work.revision,
            receipt_id=work.receipt_id,
        )
        if current != exact:
            raise RuntimeError(
                "current projection checkpoint advanced after page issue"
            )
        if exact.status is not CurrentProjectionStatus.SPOOL:
            raise RuntimeError("issued current projection page is not a spool page")
        with self.__context.SQLConnector() as connector:
            page = CurrentProjectionArtifactRepository.list_page(
                connector,
                receipt_id=work.receipt_id,
                cursor=exact.last_publication_key,
                page_limit=_MAX_PAGE_ROWS,
            )
        items = tuple(_to_projection_item(item) for item in page.items)
        adapter.append_page(work.revision, items)
        if page.terminal:
            adapter.seal(work.revision)
        # Adapter progress is durable even if commit response is lost.  Force
        # the next turn to recover its cursor from the adapter.
        self.__projection_cursors.pop(work.receipt_id, None)
        return _ProjectionPrepared(
            work.receipt_id,
            None,
            len(items),
            page.terminal,
            False,
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


def _render_prepared_artifact(
    audit: ArtifactPreparationInputAudit,
    archive_plan: _ArchiveSourcePlan,
    adapter: ArtifactStorageAdapter,
) -> ArtifactPreparationReceipt:
    """Render only from an already-detached immutable source spool."""

    authority = audit.authority
    if (
        require_bounded_bytes(
            adapter.adapter_id,
            field="artifact adapter_id",
            minimum=1,
            maximum=64,
        )
        != authority.storage_codec[1]
        or require_digest32(
            adapter.producer_fingerprint_sha256,
            field="artifact adapter producer fingerprint",
        )
        != authority.producer_fingerprint_sha256
    ):
        archive_plan.close()
        raise ArtifactPreparationContractUnavailableError(
            "artifact adapter differs from the sealed producer contract"
        )
    archive = cast(BinaryIO, TemporaryFile(mode="w+b"))
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
        components = identity.artifact_locator_components(artifact_sha256)
        locator_plan = CanonicalValueUploadPlan.from_parts(
            "artifact_locator_bytes_v1",
            identity.iter_artifact_locator_payload(components),
        )
        locator_sha256 = identity.artifact_locator_digest(components)
        if locator_plan.value_sha256 != locator_sha256:
            raise ArtifactPreparationConflictError(
                "rendered artifact locator plan has the wrong digest"
            )
        receipt = ArtifactPreparationReceipt(
            audit=audit,
            artifact_sha256=artifact_sha256,
            size_bytes=size_bytes,
            locator_components=components,
            artifact_locator_sha256=locator_sha256,
            storage_codec_version=authority.storage_codec[0],
            locator_plan=locator_plan,
            archive=archive,
            _capability=_PREPARATION_RECEIPT_TOKEN,
        )
        locator_plan = None
        archive_owned = False
        return receipt
    finally:
        archive_plan.close()
        if locator_plan is not None:
            locator_plan.close()
        if archive_owned:
            archive.close()


def _protect_prepared_artifact_local(
    receipt: ArtifactPreparationReceipt,
    intent: ArtifactProtectionIntent,
    adapter: ArtifactStorageAdapter,
) -> ArtifactProtectionEvidence:
    """Protect exact durable PENDING facts without reopening the database."""

    intent.__post_init__()
    authority = receipt.audit.authority
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
    if intent.state != "PENDING" or actual != expected:
        raise ArtifactPreparationConflictError(
            "durable protection intent differs from rendered artifact facts"
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
    if (
        adapter_id != authority.storage_codec[1]
        or producer != authority.producer_fingerprint_sha256
    ):
        raise ArtifactPreparationContractUnavailableError(
            "artifact adapter differs from the durable protection contract"
        )
    before = _hash_stream(receipt._archive)
    if before != (receipt.artifact_sha256, receipt.size_bytes):
        raise ArtifactPreparationConflictError(
            "rendered artifact changed before external protection"
        )
    receipt._archive.seek(0)
    evidence = adapter.protect(
        receipt._archive,
        intent.locator_components,
        intent.protection_token,
    )
    if type(evidence) is not ArtifactStorageEvidence or not evidence.stored:
        raise ArtifactPreparationNotReadyError(
            "artifact storage did not acknowledge exact protection"
        )
    if _hash_stream(receipt._archive) != before:
        raise ArtifactPreparationConflictError(
            "artifact storage changed the rendered archive"
        )
    return ArtifactProtectionEvidence(
        intent,
        adapter_id,
        producer,
        _capability=_PROTECTION_EVIDENCE_TOKEN,
    )


def _intent_from_durable_family(
    family: PreparedArtifactFamily,
) -> ArtifactProtectionIntent:
    family.__post_init__()
    token = identity.decode_artifact_protection_token(family.protection_token)
    components = identity.artifact_locator_components(family.artifact_sha256)
    return ArtifactProtectionIntent(
        family.candidate_id,
        family.publication_key,
        family.artifact_sha256,
        token.size_bytes,
        components,
        token.artifact_locator_sha256,
        family.storage_codec_version,
        family.storage_generation,
        family.protection_token,
        family.state,
        True,
        _capability=_PROTECTION_INTENT_TOKEN,
    )


def _to_projection_item(
    item: CurrentProjectionArtifactItem,
) -> VNextCurrentProjectionItem:
    return VNextCurrentProjectionItem(
        item.publication_key,
        item.gid,
        item.source_gallery_name,
        item.upload_time,
        item.artifact_locator_components,
        item.artifact_sha256,
        item.size_bytes,
    )


def _release_finalization_page(
    page: PublicationFinalizationPage,
    adapters: Mapping[bytes, ArtifactReleaseAdapter],
) -> PublicationFinalizationAcknowledgement:
    """Perform release I/O from a DB-issued immutable page, with no DB call."""

    resolved = _resolve_adapters(adapters, page.items)
    for item in page.items:
        evidence = resolved[item.adapter_id].release(
            item.locator_components,
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
        if root.receipt_state == "PROJECTION_FINALIZED":
            return _Action.COMPLETE, root
        if root.receipt_state != "DB_COMMITTED" or root.revision is None:
            raise RuntimeError("publication receipt has an invalid durable state")
        return (
            _Action.CURRENT_PROJECTION,
            _ProjectionWork(
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
        "SELECT operation.publication_key, prepared.state "
        "FROM catalog_artifact_operations AS operation "
        "LEFT JOIN catalog_prepared_artifacts AS prepared "
        "ON prepared.candidate_id = operation.candidate_id "
        "AND prepared.publication_key = operation.publication_key "
        "WHERE operation.candidate_id = %s "
        "AND operation.operation IN ('CREATE', 'REBUILD') "
        "AND (prepared.publication_key IS NULL OR prepared.state = 'PENDING') "
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
        family = load_prepared_artifact_family(
            work.connector,
            candidate_id=candidate_id,
            publication_key=publication,
            backend=work.backend,
        )
        intent = None if family is None else _intent_from_durable_family(family)
        if intent is not None and intent.state != "PENDING":
            raise RuntimeError("next artifact durable intent is not PENDING")
        return _Action.PREPARE_ARTIFACT, _ArtifactWork(
            authority,
            effect_seal,
            intent,
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
    if action is _Action.CURRENT_PROJECTION:
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
        if prepared.intent is None:
            if prepared.evidence is not None:
                raise RuntimeError("unpersisted artifact has protection evidence")
            return ArtifactPreparationRepository.persist_prepared_artifact(
                work,
                gate_lease=gate,
                ingest_turn=turn,
                receipt=prepared.receipt,
                now=now,
            )
        if prepared.evidence is None:
            raise RuntimeError("durable artifact intent lacks storage evidence")
        return ArtifactPreparationRepository.confirm_prepared_artifact(
            work,
            gate_lease=gate,
            ingest_turn=turn,
            receipt=prepared.receipt,
            intent=prepared.intent,
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
        return PublicationFinalizationRepository.commit_page(
            work,
            acknowledgement=renewed,
            now=now,
        )
    if isinstance(payload, _ProjectionPrepared):
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
        return CanonicalValueRepository.allocate(
            work,
            gate_lease=gate,
            ingest_turn=turn,
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
        "WHERE working.slot = 1 AND candidate.analysis_id = %s",
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
        "FROM catalog_publication_commit_candidates AS committed "
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
        "JOIN catalog_publication_stage_orders AS ordering "
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


def _next_canonical_work(
    connector: SQLConnector,
    *,
    backend: str,
    session: VNextIngestSession,
    plans: tuple[CanonicalValueUploadPlan, ...],
    owner: object,
) -> tuple[_CanonicalWork, _Action] | None:
    generation = require_positive_int63(
        session.ingest_generation,
        field="canonical upload generation",
    )
    for plan in plans:
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
            raise RuntimeError(
                "canonical sealed identity is partial or corrupt"
            ) from error
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
        if sealed is not None:
            _compare_sealed_canonical_plan(
                connector,
                backend=backend,
                plan=plan,
                sealed=sealed,
            )
            if claim is None:
                # Allocation is a response-loss-safe replay for a sealed value;
                # it installs this generation's first-consumer claim.
                return _CanonicalWork(plan, owner), _Action.CANONICAL_ALLOCATE
            continue
        if claim is None:
            return _CanonicalWork(plan, owner), _Action.CANONICAL_ALLOCATE
        for page in plan.iter_pages():
            try:
                family = load_page_family(
                    connector,
                    page_sha256=page.page_sha256,
                )
            except (
                CanonicalValueCollisionError,
                CanonicalValuePartialFamilyError,
            ) as error:
                raise RuntimeError(
                    "canonical page family is partial or corrupt"
                ) from error
            if family is None:
                return _CanonicalWork(plan, owner, page), _Action.CANONICAL_PAGE
            if family.page_bytes != page.page_bytes:
                raise RuntimeError(
                    "canonical page digest collides with another exact preimage"
                )
        return _CanonicalWork(plan, owner), _Action.CANONICAL_SEAL
    return None


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


def _require_projection_adapter(
    adapter: VNextCurrentProjectionAdapter,
) -> None:
    if not isinstance(adapter, VNextCurrentProjectionAdapter):
        raise TypeError("current_projection must implement its neutral adapter port")


def _require_projection_checkpoint(
    checkpoint: CurrentProjectionCheckpoint,
    *,
    revision: int | None = None,
    receipt_id: bytes | None = None,
) -> CurrentProjectionCheckpoint:
    if not isinstance(checkpoint, CurrentProjectionCheckpoint):
        raise TypeError("current projection adapter returned a foreign checkpoint")
    checkpoint.__post_init__()
    if revision is not None and checkpoint.revision != revision:
        raise ValueError("current projection checkpoint belongs to another revision")
    if receipt_id is not None and checkpoint.receipt_id != receipt_id:
        raise ValueError("current projection checkpoint belongs to another receipt")
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


def _owned_resource(payload: object) -> object:
    if isinstance(payload, _CanonicalWork):
        return payload.owner
    if isinstance(payload, _CandidateWork) and isinstance(
        payload.payload,
        (PublicationCatalogProjectionPlan, ArtifactInputProjectionPlan),
    ):
        return payload.payload
    if isinstance(payload, _ArtifactPrepared):
        return payload.receipt
    return payload


def _advance_result(action: _Action, outcome: object) -> VNextIngestAdvanceResult:
    phase = (
        VNextIngestPhase.FINALIZATION
        if action in {_Action.CURRENT_PROJECTION, _Action.FINALIZE, _Action.COMPLETE}
        else VNextIngestPhase.PUBLICATION
    )
    rows = 0
    terminal = action is _Action.COMPLETE
    replayed = bool(getattr(outcome, "replayed", False))
    if isinstance(outcome, PublicationCandidateBatch):
        rows = outcome.row_count
    elif hasattr(outcome, "row_count"):
        rows = require_int63(getattr(outcome, "row_count"), field="publication rows")
    elif isinstance(outcome, _ProjectionPrepared):
        rows = outcome.processed_rows
    if action is _Action.FINALIZE:
        terminal = bool(getattr(outcome, "terminal", False))
    return VNextIngestAdvanceResult(phase, rows, terminal, replayed)


@dataclass(frozen=True, slots=True)
class _BeginWithPolicy:
    root: _Root
    policy: VNextResolvedIngestPolicy = field(repr=False)
