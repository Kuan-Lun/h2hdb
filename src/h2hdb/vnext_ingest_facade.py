"""Transaction-owning public orchestration for greenfield vNext ingest."""

from __future__ import annotations

__all__ = [
    "LibraryActivationCheckpoint",
    "LibraryActivationStatus",
    "GalleryStagingCapacityError",
    "GalleryStagingRetiredError",
    "VNextAnalysisAdvanceResult",
    "VNextLibraryActivationAdapter",
    "VNextCurrentOnlyMaintenanceOutcome",
    "VNextIngestFacade",
    "VNextIssuedAnalysisStep",
    "VNextIssuedPublicationStep",
    "VNextIssuedSourceStep",
    "VNextPreparedAnalysis",
    "VNextPreparedAnalysisStep",
    "VNextPreparedPublicationStep",
    "VNextPreparedSource",
    "VNextPreparedSourceStep",
    "VNextSourceManifestMismatchError",
]

import secrets
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from enum import StrEnum
from threading import Lock
from time import time_ns
from typing import TypeVar

from .config_loader import CoreConfig
from .domain import (
    VNextIngestAdvanceResult,
    VNextIngestCompletionReceipt,
    VNextIngestPage,
    VNextIngestPhase,
    VNextIngestPolicy,
    VNextIngestSession,
    VNextIngestSourceReceipt,
    VNextResolvedIngestPolicy,
)
from .ports import (
    ArtifactReleaseAdapter,
    ArtifactStorageAdapter,
    VNextIngestSourceAdapter,
)
from .repository import RepositoryContext
from .sql_connector import SQLConnector
from .vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
    PreparedCanonicalPage,
)
from .vnext_cleanup_repository import (
    CatalogPublicationMaintenanceState,
    CleanupBatchCommand,
    CleanupBatchResult,
    CleanupCycle,
    VNextCleanupRepository,
)
from .vnext_domains import require_int63, require_positive_int63
from .vnext_download_ingest_repository import (
    CoordinatedIngestCompletion,
    CoordinatedIngestTurn,
    DownloadIngestRepository,
    DownloadIngestUnavailableError,
    HandoffKind,
)
from .vnext_gallery_staging_repository import (
    BatchAttempt,
    DirectoryBatchCommand,
    FileBatchCommand,
    GalleryObservationStagingRepository,
    GalleryStagingCapacityError,
    GalleryStagingHandle,
    GalleryStagingPendingRetirement,
    GalleryStagingProgress,
    GalleryStagingReceipt,
    GalleryStagingRetiredError,
    GalleryStagingRetirement,
    GalleryStagingSeal,
    MatchBatchCommand,
    MatchBatchReceipt,
    MetadataBatchCommand,
    TagBatchCommand,
)
from .vnext_identity import (
    GalleryObservationComponent,
    encode_source_relative_locator,
)
from .vnext_ingest_analysis import (
    VNextAnalysisAdvanceResult,
    VNextIngestAnalysisOrchestrator,
    VNextIssuedAnalysisStep,
    VNextPreparedAnalysis,
    VNextPreparedAnalysisStep,
)
from .vnext_ingest_fence_repository import IngestTurn
from .vnext_ingest_policy_repository import VNextIngestPolicyRepository
from .vnext_ingest_publication import (
    LibraryActivationCheckpoint,
    LibraryActivationStatus,
    VNextIngestPublication,
    VNextIssuedPublicationStep,
    VNextLibraryActivationAdapter,
    VNextPreparedPublicationStep,
)
from .vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
    MaintenanceGateTokenCollisionError,
    MaintenanceGateUnavailableError,
)
from .vnext_source_build_repository import (
    AssemblyBatchAttempt,
    AssemblyBatchReceipt,
    DiscoveryBatch,
    DiscoveryBatchReceipt,
    PendingSourceGallery,
    ResolvedDiscoveryLocator,
    SourceBuildHandoff,
    SourceBuildManifestSummary,
    SourceBuildRepository,
    SourceBuildSnapshotMismatchError,
    SourceDiscoveryPlan,
    SourceRootBuildCommand,
    _SourceDrainRetry,
)
from .vnext_source_observation_spool import (
    FrozenGalleryObservation,
    FrozenSourceObservationSpool,
)
from .vnext_transaction import VNextUnitOfWork

_ResultT = TypeVar("_ResultT")
_PREPARED_SOURCE_TOKEN = object()
_ISSUED_SOURCE_STEP_TOKEN = object()
_PREPARED_SOURCE_STEP_TOKEN = object()
_SOURCE_LOCATOR_PAGE_LIMIT = 256
_CURRENT_ONLY_BATCHES_PER_ATTEMPT = 16


def _now_microseconds() -> int:
    return time_ns() // 1_000


class VNextSourceManifestMismatchError(RuntimeError):
    """The replayed source bytes differ from their complete frozen preflight."""


class VNextCurrentOnlyMaintenanceOutcome(StrEnum):
    """Result of one bounded public current-only maintenance attempt."""

    DONE = "DONE"
    PROGRESSED = "PROGRESSED"
    BLOCKED = "BLOCKED"
    CONTENDED = "CONTENDED"


class _CurrentOnlyMaintenancePending(RuntimeError):
    """An interrupted current-only cleanup must finish before new ingest."""


class _SourceAction(StrEnum):
    INITIALIZE = "INITIALIZE"
    ROOT_ALLOCATE = "ROOT_ALLOCATE"
    ROOT_PAGE = "ROOT_PAGE"
    ROOT_PUT_PAGE = "ROOT_PUT_PAGE"
    ROOT_SEAL = "ROOT_SEAL"
    ROOT_HANDOFF = "ROOT_HANDOFF"
    DISCOVERY_BATCH = "DISCOVERY_BATCH"
    LOCATOR_INITIALIZE = "LOCATOR_INITIALIZE"
    LOCATOR_ALLOCATE = "LOCATOR_ALLOCATE"
    LOCATOR_PAGE = "LOCATOR_PAGE"
    LOCATOR_PUT_PAGE = "LOCATOR_PUT_PAGE"
    LOCATOR_SEAL = "LOCATOR_SEAL"
    LOCATOR_RESOLVE = "LOCATOR_RESOLVE"
    DISCOVERY_COMMIT = "DISCOVERY_COMMIT"
    STAGING_FIND = "STAGING_FIND"
    STAGING_SELECT = "STAGING_SELECT"
    STAGING_COMPLETE = "STAGING_COMPLETE"
    STAGING_BEGIN = "STAGING_BEGIN"
    STAGING_RECOVER = "STAGING_RECOVER"
    FILE_PAGE = "FILE_PAGE"
    DIRECTORY_PAGE = "DIRECTORY_PAGE"
    TAG_PAGE = "TAG_PAGE"
    METADATA_PAGE = "METADATA_PAGE"
    MATCH = "MATCH"
    STAGING_SEAL = "STAGING_SEAL"
    STAGING_RETIRE = "STAGING_RETIRE"
    ASSEMBLY = "ASSEMBLY"
    COMPLETE = "COMPLETE"


@dataclass(slots=True)
class _SourceMachine:
    action: _SourceAction = _SourceAction.INITIALIZE
    policy: VNextResolvedIngestPolicy | None = None
    build_id: bytes | None = None
    root_command: SourceRootBuildCommand | None = None
    root_upload: CanonicalValueUploadPlan | None = None
    root_pages: Iterator[PreparedCanonicalPage] | None = None
    handoff: SourceBuildHandoff | None = None
    discovery_batch: DiscoveryBatch | None = None
    resolved: list[ResolvedDiscoveryLocator] | None = None
    locator_index: int = 0
    locator_upload: CanonicalValueUploadPlan | None = None
    locator_pages: Iterator[PreparedCanonicalPage] | None = None
    discovered_galleries: int = 0
    staged_galleries: int = 0
    sealed: bool = False
    pending_gallery: PendingSourceGallery | None = None
    locator_components: tuple[str, ...] | None = None
    observation: FrozenGalleryObservation | None = None
    staging_handle: GalleryStagingHandle | None = None
    staging_seal: GalleryStagingSeal | None = None
    file_after: bytes | None = None
    directory_after: bytes | None = None
    tag_after: int | None = None
    metadata_chunks: Iterator[tuple[bytes, bool]] | None = None
    previous_operation_id: bytes | None = None
    match_previous_operation_id: bytes | None = None
    drained_page: _SourceDrainRetry | None = None


@dataclass(frozen=True, slots=True)
class _ObservationComponentStep:
    command: (
        FileBatchCommand
        | DirectoryBatchCommand
        | TagBatchCommand
        | MetadataBatchCommand
    )
    next_after: bytes | int | None


class VNextIssuedSourceStep:
    """Opaque DB-issued instruction for one source state-machine step."""

    __slots__ = ("_action", "_payload", "_session", "_source", "_token")

    def __init__(
        self,
        *,
        source: VNextPreparedSource,
        action: _SourceAction,
        payload: object,
        session: VNextIngestSession,
        _constructor_token: object,
    ) -> None:
        if _constructor_token is not _ISSUED_SOURCE_STEP_TOKEN:
            raise TypeError("use VNextIngestFacade.issue_source_step")
        self._source = source
        self._action = action
        self._payload = payload
        self._session = session
        self._token = object()


class VNextPreparedSourceStep:
    """Opaque local-preparation result accepted only by ``commit_source_step``."""

    __slots__ = ("_action", "_issued", "_payload", "_token")

    def __init__(
        self,
        *,
        issued: VNextIssuedSourceStep,
        action: _SourceAction,
        payload: object,
        _constructor_token: object,
    ) -> None:
        if _constructor_token is not _PREPARED_SOURCE_STEP_TOKEN:
            raise TypeError("use VNextIngestFacade.prepare_source_step")
        self._issued = issued
        self._action = action
        self._payload = payload
        self._token = object()


class VNextPreparedSource:
    """Opaque disk snapshot consumed by the issue/prepare/commit source API.

    Repository plans and checkpoint worksets intentionally have no public
    accessors.  The handle is a process-local resource, not database authority.
    """

    __slots__ = (
        "_active_issue",
        "_active_step",
        "_closed",
        "_machine",
        "_manifest_summary",
        "_plan",
        "_snapshot",
        "_source_root_components",
    )

    def __init__(
        self,
        *,
        snapshot: FrozenSourceObservationSpool,
        plan: SourceDiscoveryPlan,
        manifest_summary: SourceBuildManifestSummary,
        source_root_components: tuple[str, ...],
        _constructor_token: object,
    ) -> None:
        if _constructor_token is not _PREPARED_SOURCE_TOKEN:
            raise TypeError("use VNextIngestFacade.prepare_source")
        self._snapshot = snapshot
        self._plan = plan
        self._manifest_summary = manifest_summary
        self._source_root_components = source_root_components
        self._closed = False
        self._machine = _SourceMachine()
        self._active_issue: VNextIssuedSourceStep | None = None
        self._active_step: VNextPreparedSourceStep | None = None

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            if self._machine.root_upload is not None:
                self._machine.root_upload.close()
            if self._machine.locator_upload is not None:
                self._machine.locator_upload.close()
            _close_source_step_payload(self._active_step)
            try:
                self._snapshot.close()
            finally:
                self._plan.close()

    def __enter__(self) -> VNextPreparedSource:
        self._require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _require_open(self) -> None:
        if self._closed:
            raise ValueError("prepared source is closed")
        self._snapshot._require_open()
        self._plan._require_open()


class VNextIngestFacade:
    """Own fresh connections and bounded transactions for ingest calls."""

    __slots__ = (
        "__analysis",
        "__backend",
        "__clock",
        "__closed",
        "__context",
        "__lifecycle_lock",
        "__publication",
    )

    def __init__(
        self,
        config: CoreConfig,
        *,
        clock: Callable[[], int] = _now_microseconds,
    ) -> None:
        if not isinstance(config, CoreConfig):
            raise TypeError("config must be CoreConfig")
        if not callable(clock):
            raise TypeError("clock must be callable")
        context = RepositoryContext.from_config(config)
        self.__context = context
        self.__backend = context.sql_type
        self.__clock = clock
        self.__closed = False
        self.__lifecycle_lock = Lock()
        self.__analysis: VNextIngestAnalysisOrchestrator | None = None
        self.__publication: VNextIngestPublication | None = None

    def close(self) -> None:
        """Release facade-owned process-local publication resources.

        Prepared source, analysis, and publication-step handles remain owned
        by their callers and retain their own explicit ``close`` methods.
        Closing this facade is idempotent and rejects every later ingest call.
        """

        with self.__lifecycle_lock:
            if self.__closed:
                return
            self.__closed = True
            self.__analysis = None
            publication = self.__publication
            self.__publication = None
        if publication is not None:
            publication.close()

    def __enter__(self) -> VNextIngestFacade:
        self.__require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException:
            pass

    def prepare_source(
        self,
        adapter: VNextIngestSourceAdapter,
    ) -> VNextPreparedSource:
        """Freeze one complete source snapshot outside database transactions."""

        self.__require_open()
        if not isinstance(adapter, VNextIngestSourceAdapter):
            raise TypeError("adapter must implement VNextIngestSourceAdapter")
        root = adapter.source_root_components
        if not isinstance(root, tuple):
            raise TypeError("adapter source_root_components must be an exact tuple")
        # SourceDiscoveryPlan validates the root-independent locator codec and
        # owns cleanup if page consumption fails midway.
        plan = SourceDiscoveryPlan.from_locators(_iter_source_locators(adapter))
        snapshot: FrozenSourceObservationSpool | None = None
        try:
            snapshot = FrozenSourceObservationSpool.freeze(
                adapter,
                plan=plan,
                source_root_components=root,
            )
            return VNextPreparedSource(
                snapshot=snapshot,
                plan=plan,
                manifest_summary=snapshot.manifest_summary,
                source_root_components=root,
                _constructor_token=_PREPARED_SOURCE_TOKEN,
            )
        except BaseException:
            if snapshot is not None:
                snapshot.close()
            plan.close()
            raise

    def issue_source_step(
        self,
        session: VNextIngestSession,
        policy: VNextResolvedIngestPolicy,
        prepared: VNextPreparedSource,
    ) -> VNextIssuedSourceStep:
        """Issue one short-lived step without performing external source I/O."""

        self.__require_open()
        source = _require_prepared_source(prepared)
        _require_resolved_source_policy(policy)
        machine = source._machine
        if machine.policy is not None and not _same_resolved_policy(
            machine.policy,
            policy,
        ):
            raise ValueError("prepared source is bound to another ingest policy")
        active = source._active_issue
        if active is not None:
            _require_same_session_authority(active._session, session)
            self.__write(lambda work: _resume_authority(work, session, self.__clock()))
            return active

        action = machine.action

        def issue(work: VNextUnitOfWork) -> object:
            now = self.__clock()
            if action is _SourceAction.STAGING_FIND:
                # The staging repository owns this operation's outer gate/fence
                # authorization, so delegate before acquiring any facade locks.
                if machine.build_id is None:
                    raise RuntimeError("source build is not initialized")
                gate, turn = _repository_authority(session)
                retirement = (
                    GalleryObservationStagingRepository.find_pending_retirement(
                        work,
                        gate_lease=gate,
                        ingest_turn=turn.ingest_turn,
                        build_id=machine.build_id,
                        now=now,
                    )
                )
                if retirement is not None:
                    return retirement
                return SourceBuildRepository.get_pending_source_gallery(
                    work.connector,
                    build_id=machine.build_id,
                )
            _resume_authority(work, session, now)
            if action is _SourceAction.DISCOVERY_BATCH:
                if machine.build_id is None:
                    raise RuntimeError("source build is not initialized")
                return SourceBuildRepository.prepare_discovery_batch(
                    work.connector,
                    build_id=machine.build_id,
                    plan=source._plan,
                )
            if action is _SourceAction.COMPLETE:
                raise ValueError("prepared source is already complete")
            return None

        payload = self.__write(issue)
        if machine.policy is None:
            machine.policy = policy
        issued = VNextIssuedSourceStep(
            source=source,
            action=action,
            payload=payload,
            session=session,
            _constructor_token=_ISSUED_SOURCE_STEP_TOKEN,
        )
        source._active_issue = issued
        return issued

    def prepare_source_step(
        self,
        prepared: VNextPreparedSource,
        issued: VNextIssuedSourceStep,
    ) -> VNextPreparedSourceStep:
        """Perform one bounded adapter/disk step without database authority."""

        self.__require_open()
        source = _require_prepared_source(prepared)
        if not isinstance(issued, VNextIssuedSourceStep):
            raise TypeError("issued must be VNextIssuedSourceStep")
        if issued._source is not source or source._active_issue is not issued:
            raise ValueError("issued source step is stale or belongs to another source")
        if source._active_step is not None:
            return source._active_step

        machine = source._machine
        action = issued._action
        local_action = action
        payload = issued._payload
        if action is _SourceAction.INITIALIZE:
            policy = machine.policy
            if policy is None:
                raise RuntimeError("source policy binding is absent")
            command = SourceRootBuildCommand(
                source._source_root_components,
                source._manifest_summary,
            )
            build_id = command.build_attempt_id
            upload = command.prepare_root_upload()
            payload = (build_id, command, upload, upload.iter_pages())
        elif action is _SourceAction.ROOT_PAGE:
            if machine.root_pages is None:
                raise RuntimeError("source-root page iterator is absent")
            try:
                payload = next(machine.root_pages)
                local_action = _SourceAction.ROOT_PUT_PAGE
            except StopIteration:
                payload = None
                local_action = _SourceAction.ROOT_SEAL
        elif action is _SourceAction.LOCATOR_INITIALIZE:
            batch = _require_discovery_batch(machine)
            locator = batch.locators[machine.locator_index]
            upload = source._plan.prepare_locator_upload(locator)
            payload = (upload, upload.iter_pages())
        elif action is _SourceAction.LOCATOR_PAGE:
            if machine.locator_pages is None:
                raise RuntimeError("source-locator page iterator is absent")
            try:
                payload = next(machine.locator_pages)
                local_action = _SourceAction.LOCATOR_PUT_PAGE
            except StopIteration:
                payload = None
                local_action = _SourceAction.LOCATOR_SEAL
        elif action is _SourceAction.STAGING_FIND:
            pending = issued._payload
            if isinstance(pending, GalleryStagingPendingRetirement):
                payload = pending.seal
                local_action = (
                    _SourceAction.STAGING_RETIRE
                    if pending.acknowledged
                    else _SourceAction.STAGING_RECOVER
                )
            elif pending is None:
                payload = None
                local_action = _SourceAction.STAGING_COMPLETE
            else:
                if not isinstance(pending, PendingSourceGallery):
                    raise RuntimeError("pending source gallery receipt is invalid")
                decoded_locator = source._plan._decode_locator(
                    pending.position,
                    pending.locator_sha256,
                )
                observation = source._snapshot.open_gallery(
                    position=pending.position,
                    locator_sha256=pending.locator_sha256,
                    locator_components=decoded_locator,
                )
                payload = (pending, decoded_locator, observation)
                local_action = _SourceAction.STAGING_SELECT
        elif action is _SourceAction.STAGING_RETIRE:
            if machine.staging_seal is None:
                raise RuntimeError("terminal gallery staging seal is absent")
            payload = machine.staging_seal
        elif action in {
            _SourceAction.FILE_PAGE,
            _SourceAction.DIRECTORY_PAGE,
            _SourceAction.TAG_PAGE,
            _SourceAction.METADATA_PAGE,
        }:
            payload = _prepare_observation_component(source, action)
        elif action is _SourceAction.MATCH:
            payload = MatchBatchCommand(
                secrets.token_bytes(16),
                machine.match_previous_operation_id,
            )
        elif action is _SourceAction.ASSEMBLY:
            payload = SourceBuildRepository.issue_assembly_batch()

        step = VNextPreparedSourceStep(
            issued=issued,
            action=local_action,
            payload=payload,
            _constructor_token=_PREPARED_SOURCE_STEP_TOKEN,
        )
        source._active_step = step
        return step

    def commit_source_step(
        self,
        session: VNextIngestSession,
        prepared_step: VNextPreparedSourceStep,
    ) -> VNextIngestAdvanceResult:
        """Commit one prepared source step with the current renewed session."""

        self.__require_open()
        if not isinstance(prepared_step, VNextPreparedSourceStep):
            raise TypeError("prepared_step must be VNextPreparedSourceStep")
        issued = prepared_step._issued
        source = _require_prepared_source(issued._source)
        if (
            source._active_issue is not issued
            or source._active_step is not prepared_step
        ):
            raise ValueError("prepared source step is stale")
        _require_same_session_authority(issued._session, session)
        gate, turn = _repository_authority(session)
        machine = source._machine
        action = prepared_step._action
        now = require_int63(self.__clock(), field="source step commit now")

        def commit(work: VNextUnitOfWork) -> object:
            if action in {
                _SourceAction.INITIALIZE,
                _SourceAction.LOCATOR_INITIALIZE,
            }:
                return _resume_authority(work, session, now)
            if action is _SourceAction.ROOT_ALLOCATE:
                return CanonicalValueRepository.allocate(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    plan=_require_root_upload(machine),
                    now=now,
                )
            if action is _SourceAction.ROOT_PUT_PAGE:
                return CanonicalValueRepository.put_page(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    plan=_require_root_upload(machine),
                    prepared_page=_require_canonical_page(prepared_step._payload),
                    now=now,
                )
            if action is _SourceAction.ROOT_SEAL:
                return CanonicalValueRepository.seal(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    plan=_require_root_upload(machine),
                    now=now,
                )
            if action is _SourceAction.ROOT_HANDOFF:
                if machine.root_command is None:
                    raise RuntimeError("source-root command is absent")
                if machine.policy is None:
                    raise RuntimeError("source machine lacks its resolved policy")
                return SourceBuildRepository.handoff_root_or_drain(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    command=machine.root_command,
                    root_plan=_require_root_upload(machine),
                    analysis_policy_id=machine.policy.analysis_policy_id,
                    drained_page=machine.drained_page,
                    now=now,
                )
            if action is _SourceAction.DISCOVERY_BATCH:
                batch = _require_exact_discovery_batch(prepared_step._payload)
                if batch.terminal:
                    return SourceBuildRepository.commit_discovery_batch(
                        work,
                        gate_lease=gate,
                        ingest_turn=turn.ingest_turn,
                        batch=batch,
                        resolved=(),
                        now=now,
                    )
                return _resume_authority(work, session, now)
            if action is _SourceAction.LOCATOR_ALLOCATE:
                return CanonicalValueRepository.allocate(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    plan=_require_locator_upload(machine),
                    now=now,
                )
            if action is _SourceAction.LOCATOR_PUT_PAGE:
                return CanonicalValueRepository.put_page(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    plan=_require_locator_upload(machine),
                    prepared_page=_require_canonical_page(prepared_step._payload),
                    now=now,
                )
            if action is _SourceAction.LOCATOR_SEAL:
                return CanonicalValueRepository.seal(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    plan=_require_locator_upload(machine),
                    now=now,
                )
            if action is _SourceAction.LOCATOR_RESOLVE:
                batch = _require_discovery_batch(machine)
                locator = batch.locators[machine.locator_index]
                return SourceBuildRepository.resolve_discovery_locator(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    batch=batch,
                    locator=locator,
                    upload_plan=_require_locator_upload(machine),
                    now=now,
                )
            if action is _SourceAction.DISCOVERY_COMMIT:
                batch = _require_discovery_batch(machine)
                resolved = machine.resolved
                if resolved is None:
                    raise RuntimeError("resolved discovery workset is absent")
                return SourceBuildRepository.commit_discovery_batch(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    batch=batch,
                    resolved=tuple(resolved),
                    now=now,
                )
            if action in {
                _SourceAction.STAGING_SELECT,
                _SourceAction.STAGING_COMPLETE,
                _SourceAction.STAGING_RECOVER,
            }:
                return _resume_authority(work, session, now)
            if action is _SourceAction.STAGING_BEGIN:
                pending = _require_pending_gallery(machine)
                return GalleryObservationStagingRepository.begin_or_resume(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    build_id=pending.build_id,
                    gallery_id=pending.gallery_id,
                    now=now,
                )
            if action is _SourceAction.FILE_PAGE:
                component_step = _require_component_step(prepared_step._payload)
                command = component_step.command
                if not isinstance(command, FileBatchCommand):
                    raise TypeError("FILE source step has an invalid command")
                return GalleryObservationStagingRepository.put_files(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    handle=_require_staging_handle(machine),
                    command=command,
                    now=now,
                )
            if action is _SourceAction.DIRECTORY_PAGE:
                component_step = _require_component_step(prepared_step._payload)
                command = component_step.command
                if not isinstance(command, DirectoryBatchCommand):
                    raise TypeError("DIRECTORY source step has an invalid command")
                return GalleryObservationStagingRepository.put_directories(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    handle=_require_staging_handle(machine),
                    command=command,
                    now=now,
                )
            if action is _SourceAction.TAG_PAGE:
                component_step = _require_component_step(prepared_step._payload)
                command = component_step.command
                if not isinstance(command, TagBatchCommand):
                    raise TypeError("TAG source step has an invalid command")
                return GalleryObservationStagingRepository.put_tags(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    handle=_require_staging_handle(machine),
                    command=command,
                    now=now,
                )
            if action is _SourceAction.METADATA_PAGE:
                component_step = _require_component_step(prepared_step._payload)
                command = component_step.command
                if not isinstance(command, MetadataBatchCommand):
                    raise TypeError("METADATA source step has an invalid command")
                return GalleryObservationStagingRepository.put_metadata(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    handle=_require_staging_handle(machine),
                    command=command,
                    now=now,
                )
            if action is _SourceAction.MATCH:
                match_command = prepared_step._payload
                if not isinstance(match_command, MatchBatchCommand):
                    raise TypeError("MATCH source step has an invalid command")
                return GalleryObservationStagingRepository.match_files_to_directory(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    handle=_require_staging_handle(machine),
                    command=match_command,
                    now=now,
                )
            if action is _SourceAction.STAGING_SEAL:
                return GalleryObservationStagingRepository.seal(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    handle=_require_staging_handle(machine),
                    now=now,
                )
            if action is _SourceAction.STAGING_RETIRE:
                seal = prepared_step._payload
                if not isinstance(seal, GalleryStagingSeal):
                    raise TypeError("STAGING_RETIRE source step has an invalid seal")
                return GalleryObservationStagingRepository.retire_sealed(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    seal=seal,
                    now=now,
                )
            if action is _SourceAction.ASSEMBLY:
                attempt = prepared_step._payload
                if not isinstance(attempt, AssemblyBatchAttempt):
                    raise TypeError("ASSEMBLY source step has an invalid attempt")
                if machine.build_id is None:
                    raise RuntimeError("source build is not initialized")
                receipt = SourceBuildRepository.assemble_batch(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    build_id=machine.build_id,
                    attempt=attempt,
                    now=now,
                )
                if receipt.terminal:
                    _require_source_manifest_summary(
                        receipt,
                        source._manifest_summary,
                    )
                return receipt
            raise RuntimeError(f"unsupported source action {action.value}")

        try:
            outcome = self.__write(commit)
        except (
            SourceBuildSnapshotMismatchError,
            VNextSourceManifestMismatchError,
        ) as mismatch:
            build_id = machine.build_id
            if build_id is None:
                raise RuntimeError(
                    "source manifest mismatch has no build authority"
                ) from None
            # The failed terminal assembly transaction has already rolled back.
            # Release only the exact OPEN build still mapped to this live
            # generation; SourceBuildRepository.abandon retains the generation
            # mapping as response-loss evidence and rejects foreign authority.
            self.__write(
                lambda work: SourceBuildRepository.abandon(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn.ingest_turn,
                    build_id=build_id,
                    now=now,
                )
            )
            source.close()
            if isinstance(mismatch, SourceBuildSnapshotMismatchError):
                raise VNextSourceManifestMismatchError(
                    "durable source build manifest differs from its frozen "
                    "preflight snapshot"
                ) from mismatch
            raise
        processed_rows, replayed = _apply_source_outcome(
            source,
            prepared_step,
            outcome,
        )
        source._active_step = None
        source._active_issue = None
        return _source_advance_result(
            source,
            processed_rows=processed_rows,
            replayed=replayed,
        )

    def prepare_analysis(
        self,
        build_id: bytes,
        policy: VNextResolvedIngestPolicy,
        *,
        max_rows: int,
    ) -> VNextPreparedAnalysis:
        """Create restartable local analysis state without opening the database."""

        self.__require_open()
        return self.__analysis_orchestrator().prepare_analysis(
            build_id,
            policy,
            max_rows=max_rows,
        )

    def issue_analysis_step(
        self,
        session: VNextIngestSession,
        prepared: VNextPreparedAnalysis,
    ) -> VNextIssuedAnalysisStep:
        """Issue one bounded analysis action from durable authority."""

        self.__require_open()
        return self.__analysis_orchestrator().issue_analysis_step(session, prepared)

    def prepare_analysis_step(
        self,
        prepared: VNextPreparedAnalysis,
        issued: VNextIssuedAnalysisStep,
    ) -> VNextPreparedAnalysisStep:
        """Perform one analysis preparation step outside session serialization."""

        self.__require_open()
        return self.__analysis_orchestrator().prepare_analysis_step(prepared, issued)

    def commit_analysis_step(
        self,
        session: VNextIngestSession,
        prepared_step: VNextPreparedAnalysisStep,
    ) -> VNextAnalysisAdvanceResult:
        """Commit one analysis step using the current renewed session receipt."""

        self.__require_open()
        return self.__analysis_orchestrator().commit_analysis_step(
            session,
            prepared_step,
        )

    def issue_publication_step(
        self,
        session: VNextIngestSession,
        policy: VNextResolvedIngestPolicy,
    ) -> VNextIssuedPublicationStep:
        """Issue one bounded publication action from durable authority."""

        self.__require_open()
        return self.__publication_orchestrator().issue_step(session, policy)

    def prepare_publication_step(
        self,
        issued: VNextIssuedPublicationStep,
        *,
        artifact_adapters: Mapping[bytes, ArtifactStorageAdapter],
        finalization_adapters: Mapping[bytes, ArtifactReleaseAdapter],
        library_activation: VNextLibraryActivationAdapter,
    ) -> VNextPreparedPublicationStep:
        """Perform publication adapter work outside fenced DB transactions."""

        self.__require_open()
        return self.__publication_orchestrator().prepare_step(
            issued,
            artifact_adapters=artifact_adapters,
            finalization_adapters=finalization_adapters,
            library_activation=library_activation,
        )

    def commit_publication_step(
        self,
        session: VNextIngestSession,
        prepared: VNextPreparedPublicationStep,
    ) -> VNextIngestAdvanceResult:
        """Commit one publication step using the current renewed session."""

        self.__require_open()
        return self.__publication_orchestrator().commit_step(session, prepared)

    def try_claim_ingest(
        self,
        periodic: bool,
        lease_duration_microseconds: int,
    ) -> VNextIngestSession | None:
        """Try to atomically claim the gate and ingest turn.

        Ordinary contention or absence of eligible download work returns
        ``None``.  Capability collisions and corrupt authority still fail
        closed with their typed repository error.
        """

        self.__require_open()
        duration = require_positive_int63(
            lease_duration_microseconds,
            field="ingest session lease_duration_microseconds",
        )
        if type(periodic) is not bool:
            raise TypeError("periodic must be bool")
        now = require_int63(self.__clock(), field="ingest session claim now")

        def claim(work: VNextUnitOfWork) -> VNextIngestSession:
            gate = MaintenanceGateRepository.claim_shared(
                work,
                now=now,
                lease_duration=duration,
            )
            maintenance_state = VNextCleanupRepository.current_only_maintenance_state(
                work,
                cycle_cutoff_at=now,
            )
            if maintenance_state is CatalogPublicationMaintenanceState.ACTIONABLE:
                # Raising inside the same transaction rolls the tentative
                # SHARED claim back.  A bounded EXCLUSIVE attempt may have
                # removed an entire shard and closed its job while other old
                # payload remains, so OPEN jobs alone are not a sufficient
                # fence against recreating a predecessor pin.
                raise _CurrentOnlyMaintenancePending
            turn = DownloadIngestRepository.claim_ingest(
                work,
                now=now,
                lease_duration=duration,
                periodic=periodic,
            )
            return _public_session(gate, turn)

        try:
            return self.__write(claim)
        except MaintenanceGateTokenCollisionError:
            raise
        except (
            _CurrentOnlyMaintenancePending,
            MaintenanceGateUnavailableError,
            DownloadIngestUnavailableError,
        ):
            return None

    def resume_ingest(self, session: VNextIngestSession) -> VNextIngestSession:
        """Revalidate an exact public ingest capability."""

        self.__require_open()
        gate, turn = _repository_authority(session)
        now = require_int63(self.__clock(), field="ingest session resume now")

        def resume(work: VNextUnitOfWork) -> VNextIngestSession:
            resumed_gate = MaintenanceGateRepository.resume(work, gate, now=now)
            resumed_turn = DownloadIngestRepository.resume_ingest(
                work,
                turn,
                now=now,
            )
            return _public_session(resumed_gate, resumed_turn)

        return self.__write(resume)

    def renew_ingest(
        self,
        session: VNextIngestSession,
        lease_duration_microseconds: int,
    ) -> VNextIngestSession:
        """Atomically renew both authorities in an ingest session."""

        self.__require_open()
        gate, turn = _repository_authority(session)
        duration = require_positive_int63(
            lease_duration_microseconds,
            field="ingest session lease_duration_microseconds",
        )
        now = require_int63(self.__clock(), field="ingest session renew now")

        def renew(work: VNextUnitOfWork) -> VNextIngestSession:
            renewed_gate = MaintenanceGateRepository.renew(
                work,
                gate,
                now=now,
                lease_duration=duration,
            )
            renewed_turn = DownloadIngestRepository.renew_ingest(
                work,
                turn,
                now=now,
                lease_duration=duration,
            )
            return _public_session(renewed_gate, renewed_turn)

        return self.__write(renew)

    def complete_ingest(
        self,
        session: VNextIngestSession,
    ) -> VNextIngestCompletionReceipt:
        """Complete ingest and release its gate in one response-loss-safe call."""

        self.__require_open()
        gate, turn = _repository_authority(session)
        now = require_int63(self.__clock(), field="ingest completion now")

        def complete(work: VNextUnitOfWork) -> VNextIngestCompletionReceipt:
            replay = DownloadIngestRepository.get_ingest_completion(work, turn)
            if replay is not None:
                return _public_completion(replay, replayed=True)

            # Gate locks have the lowest global rank.  Delete the exact live
            # lease first; a later completion failure rolls the deletion back.
            MaintenanceGateRepository.release(work, gate, now=now)
            completed = DownloadIngestRepository.complete_ingest(
                work,
                turn,
                now=now,
            )
            return _public_completion(completed, replayed=False)

        try:
            return self.__write(complete)
        except MaintenanceGateUnavailableError as unavailable:
            # A concurrent retry may have committed completion and gate release
            # while this call waited for the gate row.  Re-read canonical
            # completion before reporting a genuinely stale session.
            replay = self.__write(
                lambda work: DownloadIngestRepository.get_ingest_completion(
                    work,
                    turn,
                )
            )
            if replay is None:
                raise unavailable
            return _public_completion(replay, replayed=True)

    def drain_current_only_maintenance(
        self,
        lease_duration_microseconds: int,
    ) -> VNextCurrentOnlyMaintenanceOutcome:
        """Advance one bounded publication/resource current-only fixed point.

        Each durable cleanup transaction selects at most 256 logical cleanup
        keys/families; a key can execute a schema-fixed bounded compound delete
        across its vertical family.  One public attempt advances at most 16
        such transactions.  ``PROGRESSED``
        means at least one advance committed *and work remains*, so callers
        should retry promptly; ``BLOCKED`` and ``CONTENDED`` should use the
        ordinary poll cadence.  Callers retain no capability, and interrupted
        shard jobs resume before new work.  Every completed cycle restarts the
        21-target dependency-priority scan from its head.

        File-derived hash-cache expiration is intentionally separate from this
        publication/resource fixed point.  It requires an explicit nonzero age policy;
        fresh cache rows therefore neither prevent ``DONE`` nor get deleted by
        a resident idle poll.
        """

        self.__require_open()
        duration = require_positive_int63(
            lease_duration_microseconds,
            field="current-only maintenance lease_duration_microseconds",
        )
        cycle_cutoff_at = require_int63(
            self.__clock(), field="current-only maintenance cycle cutoff"
        )
        with self.__context.SQLConnector() as connector:
            with connector.read_transaction():
                maintenance_state = (
                    VNextCleanupRepository.current_only_maintenance_state(
                        VNextUnitOfWork(connector, backend=self.__backend),
                        cycle_cutoff_at=cycle_cutoff_at,
                    )
                )
            if maintenance_state is CatalogPublicationMaintenanceState.DONE:
                return VNextCurrentOnlyMaintenanceOutcome.DONE
            if maintenance_state is CatalogPublicationMaintenanceState.BLOCKED:
                return VNextCurrentOnlyMaintenanceOutcome.BLOCKED

            try:
                claim_now = require_int63(
                    self.__clock(),
                    field="current-only maintenance claim now",
                )
                with connector.transaction():
                    lease = MaintenanceGateRepository.claim_exclusive(
                        VNextUnitOfWork(connector, backend=self.__backend),
                        now=claim_now,
                        lease_duration=duration,
                    )
            except MaintenanceGateUnavailableError:
                return VNextCurrentOnlyMaintenanceOutcome.CONTENDED

            try:
                lease = self.__renew_current_only_lease(
                    connector, lease, duration=duration
                )
                advanced_batches = 0
                while advanced_batches < _CURRENT_ONLY_BATCHES_PER_ATTEMPT:
                    lease = self.__renew_current_only_lease(
                        connector, lease, duration=duration
                    )
                    cycle = self.__next_current_only_cycle(
                        connector,
                        lease,
                        cycle_cutoff_at=cycle_cutoff_at,
                    )
                    if cycle is None:
                        break
                    lease = self.__renew_current_only_lease(
                        connector, lease, duration=duration
                    )
                    result = self.__resume_current_only_cycle(
                        connector, lease, cycle=cycle
                    )
                    while (
                        not result.cycle_complete
                        and advanced_batches < _CURRENT_ONLY_BATCHES_PER_ATTEMPT
                    ):
                        generation = result.generation
                        if generation is None:
                            raise RuntimeError(
                                "open catalog cleanup lacks a generation"
                            )
                        lease = self.__renew_current_only_lease(
                            connector, lease, duration=duration
                        )
                        result = self.__advance_current_only_shard(
                            connector,
                            lease,
                            cycle=cycle,
                            generation=generation,
                        )
                        advanced_batches += 1
                    if not result.cycle_complete:
                        break
                    # A later target can release a foreign-key blocker for an
                    # earlier one, so every completed cycle starts the exact
                    # priority scan again instead of continuing linearly.
                lease = self.__renew_current_only_lease(
                    connector, lease, duration=duration
                )
                remaining = self.__current_only_state(
                    connector,
                    lease,
                    cycle_cutoff_at=cycle_cutoff_at,
                )
                if remaining is CatalogPublicationMaintenanceState.DONE:
                    outcome = VNextCurrentOnlyMaintenanceOutcome.DONE
                elif advanced_batches:
                    outcome = VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
                else:
                    outcome = VNextCurrentOnlyMaintenanceOutcome.BLOCKED
            except BaseException:
                self.__release_current_only_after_failure(connector, lease)
                raise

            release_now = require_int63(
                self.__clock(),
                field="current-only maintenance release now",
            )
            with connector.transaction():
                MaintenanceGateRepository.release(
                    VNextUnitOfWork(connector, backend=self.__backend),
                    lease,
                    now=release_now,
                )
            return outcome

    def __current_only_state(
        self,
        connector: SQLConnector,
        lease: GateLease,
        *,
        cycle_cutoff_at: int,
    ) -> CatalogPublicationMaintenanceState:
        now = require_int63(
            self.__clock(), field="current-only maintenance preflight now"
        )
        with connector.transaction():
            work = VNextUnitOfWork(connector, backend=self.__backend)
            state = VNextCleanupRepository.current_only_maintenance_state(
                work,
                cycle_cutoff_at=cycle_cutoff_at,
                gate_lease=lease,
                now=now,
            )
        return state

    def __next_current_only_cycle(
        self,
        connector: SQLConnector,
        lease: GateLease,
        *,
        cycle_cutoff_at: int,
    ) -> CleanupCycle | None:
        now = require_int63(
            self.__clock(), field="current-only maintenance next-cycle now"
        )
        with connector.transaction():
            work = VNextUnitOfWork(connector, backend=self.__backend)
            cycle = VNextCleanupRepository.next_current_only_cycle(
                work,
                gate_lease=lease,
                cycle_cutoff_at=cycle_cutoff_at,
                now=now,
            )
        return cycle

    def __resume_current_only_cycle(
        self,
        connector: SQLConnector,
        lease: GateLease,
        *,
        cycle: CleanupCycle,
    ) -> CleanupBatchResult:
        now = require_int63(self.__clock(), field="current-only maintenance resume now")
        with connector.transaction():
            work = VNextUnitOfWork(connector, backend=self.__backend)
            result = VNextCleanupRepository.resume_cycle(
                work,
                gate_lease=lease,
                cycle=cycle,
                now=now,
            )
        return result

    def __advance_current_only_shard(
        self,
        connector: SQLConnector,
        lease: GateLease,
        *,
        cycle: CleanupCycle,
        generation: int,
    ) -> CleanupBatchResult:
        now = require_int63(
            self.__clock(), field="current-only maintenance advance now"
        )
        with connector.transaction():
            work = VNextUnitOfWork(connector, backend=self.__backend)
            result = VNextCleanupRepository.advance(
                work,
                gate_lease=lease,
                cycle=cycle,
                command=CleanupBatchCommand(
                    secrets.token_bytes(32),
                    generation,
                ),
                now=now,
            )
        return result

    def __renew_current_only_lease(
        self,
        connector: SQLConnector,
        lease: GateLease,
        *,
        duration: int,
    ) -> GateLease:
        """Renew in its own transaction before cleanup reacquires gate locks."""

        now = require_int63(
            self.__clock(),
            field="current-only maintenance renewal now",
        )
        with connector.transaction():
            return MaintenanceGateRepository.renew(
                VNextUnitOfWork(connector, backend=self.__backend),
                lease,
                now=now,
                lease_duration=duration,
            )

    def __release_current_only_after_failure(
        self,
        connector: SQLConnector,
        lease: GateLease,
    ) -> None:
        try:
            now = require_int63(
                self.__clock(),
                field="failed current-only maintenance release now",
            )
            with connector.transaction():
                MaintenanceGateRepository.release(
                    VNextUnitOfWork(connector, backend=self.__backend),
                    lease,
                    now=now,
                )
        except MaintenanceGateUnavailableError:
            # A process crash likewise loses this capability; expiry/takeover
            # plus durable cleanup checkpoints make a later call safe.
            return

    def ensure_policy(
        self,
        session: VNextIngestSession,
        policy: VNextIngestPolicy,
    ) -> VNextResolvedIngestPolicy:
        """Resolve/allocate every registry ID from complete natural facts."""

        self.__require_open()
        gate, turn = _repository_authority(session)
        if not isinstance(policy, VNextIngestPolicy):
            raise TypeError("policy must be VNextIngestPolicy")
        policy.__post_init__()
        now = require_int63(self.__clock(), field="ingest policy registration now")
        return self.__write(
            lambda work: VNextIngestPolicyRepository.ensure(
                work,
                gate_lease=gate,
                ingest_turn=turn.ingest_turn,
                policy=policy,
                now=now,
            )
        )

    def __analysis_orchestrator(self) -> VNextIngestAnalysisOrchestrator:
        with self.__lifecycle_lock:
            self.__require_open_unlocked()
            orchestrator = self.__analysis
            if orchestrator is None:
                orchestrator = VNextIngestAnalysisOrchestrator(
                    self.__context,
                    clock=self.__clock,
                )
                self.__analysis = orchestrator
            return orchestrator

    def __publication_orchestrator(self) -> VNextIngestPublication:
        with self.__lifecycle_lock:
            self.__require_open_unlocked()
            orchestrator = self.__publication
            if orchestrator is None:
                orchestrator = VNextIngestPublication(
                    self.__context,
                    clock=self.__clock,
                )
                self.__publication = orchestrator
            return orchestrator

    def __read(
        self,
        operation: Callable[[SQLConnector], _ResultT],
    ) -> _ResultT:
        self.__require_open()
        with self.__context.SQLConnector() as connector:
            with connector.read_transaction():
                return operation(connector)

    def __write(
        self,
        operation: Callable[[VNextUnitOfWork], _ResultT],
    ) -> _ResultT:
        self.__require_open()
        with self.__context.SQLConnector() as connector:
            with connector.transaction():
                return operation(VNextUnitOfWork(connector, backend=self.__backend))

    def __require_open(self) -> None:
        with self.__lifecycle_lock:
            self.__require_open_unlocked()

    def __require_open_unlocked(self) -> None:
        if self.__closed:
            raise ValueError("ingest facade is closed")


def _public_session(
    gate: GateLease,
    turn: CoordinatedIngestTurn,
) -> VNextIngestSession:
    if gate.mode != GateMode.SHARED or len(gate.slots) != 1:
        raise RuntimeError("ingest received invalid SHARED gate authority")
    linked_kind = turn.handoff_kind.value if turn.handoff_kind is not None else None
    return VNextIngestSession(
        gate_owner_token=gate.owner_token,
        gate_generation=gate.gate_generation,
        gate_slot=gate.slots[0],
        gate_lease_expires_at=gate.lease_expires_at,
        ingest_generation=turn.ingest_turn.generation,
        ingest_owner_token=turn.ingest_turn.owner_token,
        ingest_lease_expires_at=turn.ingest_turn.lease_expires_at,
        download_generation=turn.download_generation,
        handoff_owner_token=turn.handoff_owner_token,
        handoff_kind=linked_kind,
        consumed_at=turn.consumed_at,
    )


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
    handoff_kind = (
        HandoffKind(session.handoff_kind) if session.handoff_kind is not None else None
    )
    turn = CoordinatedIngestTurn(
        IngestTurn(
            session.ingest_generation,
            session.ingest_owner_token,
            session.ingest_lease_expires_at,
        ),
        session.download_generation,
        session.handoff_owner_token,
        handoff_kind,
        session.consumed_at,
    )
    return gate, turn


def _public_completion(
    completion: CoordinatedIngestCompletion,
    *,
    replayed: bool,
) -> VNextIngestCompletionReceipt:
    return VNextIngestCompletionReceipt(
        ingest_generation=completion.ingest_generation,
        owner_token=completion.owner_token,
        completed_at=completion.completed_at,
        download_generation=completion.download_generation,
        replayed=replayed,
    )


def _require_prepared_source(prepared: VNextPreparedSource) -> VNextPreparedSource:
    if not isinstance(prepared, VNextPreparedSource):
        raise TypeError("prepared must be VNextPreparedSource")
    prepared._require_open()
    return prepared


def _require_resolved_source_policy(policy: VNextResolvedIngestPolicy) -> None:
    if not isinstance(policy, VNextResolvedIngestPolicy):
        raise TypeError("policy must be VNextResolvedIngestPolicy")
    policy.__post_init__()
    if (
        policy.policy.manifest_algorithm_version != 1
        or policy.policy.file_order_version != 1
    ):
        raise ValueError("source orchestration supports manifest policy v1 only")


def _same_resolved_policy(
    left: VNextResolvedIngestPolicy,
    right: VNextResolvedIngestPolicy,
) -> bool:
    return (
        left.policy,
        left.manifest_policy_id,
        left.analysis_policy_id,
        left.artifact_policy_sha256,
        left.artifact_policy_fingerprint_sha256,
        left.display_title_policy_id,
        left.title_sort_policy_id,
        left.operational_policy_id,
    ) == (
        right.policy,
        right.manifest_policy_id,
        right.analysis_policy_id,
        right.artifact_policy_sha256,
        right.artifact_policy_fingerprint_sha256,
        right.display_title_policy_id,
        right.title_sort_policy_id,
        right.operational_policy_id,
    )


def _session_authority_identity(session: VNextIngestSession) -> tuple[object, ...]:
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
    if _session_authority_identity(issued) != _session_authority_identity(current):
        raise ValueError("source step belongs to another ingest session authority")


def _resume_authority(
    work: VNextUnitOfWork,
    session: VNextIngestSession,
    now: int,
) -> VNextIngestSession:
    gate, turn = _repository_authority(session)
    timestamp = require_int63(now, field="source step authorization now")
    resumed_gate = MaintenanceGateRepository.resume(work, gate, now=timestamp)
    resumed_turn = DownloadIngestRepository.resume_ingest(work, turn, now=timestamp)
    return _public_session(resumed_gate, resumed_turn)


def _require_root_upload(machine: _SourceMachine) -> CanonicalValueUploadPlan:
    if machine.root_upload is None:
        raise RuntimeError("source-root upload plan is absent")
    return machine.root_upload


def _require_locator_upload(machine: _SourceMachine) -> CanonicalValueUploadPlan:
    if machine.locator_upload is None:
        raise RuntimeError("source-locator upload plan is absent")
    return machine.locator_upload


def _require_canonical_page(payload: object) -> PreparedCanonicalPage:
    if not isinstance(payload, PreparedCanonicalPage):
        raise TypeError("prepared source step lacks a canonical page")
    payload.__post_init__()
    return payload


def _require_exact_discovery_batch(payload: object) -> DiscoveryBatch:
    if type(payload) is not DiscoveryBatch:
        raise TypeError("prepared source step lacks an exact discovery batch")
    payload.__post_init__()
    return payload


def _require_discovery_batch(machine: _SourceMachine) -> DiscoveryBatch:
    if machine.discovery_batch is None:
        raise RuntimeError("source discovery batch is absent")
    return machine.discovery_batch


def _require_pending_gallery(machine: _SourceMachine) -> PendingSourceGallery:
    if machine.pending_gallery is None:
        raise RuntimeError("pending source gallery is absent")
    return machine.pending_gallery


def _require_staging_handle(machine: _SourceMachine) -> GalleryStagingHandle:
    if machine.staging_handle is None:
        raise RuntimeError("gallery staging handle is absent")
    return machine.staging_handle


def _require_component_step(payload: object) -> _ObservationComponentStep:
    if not isinstance(payload, _ObservationComponentStep):
        raise TypeError("prepared source step lacks an observation component page")
    return payload


def _prepare_observation_component(
    source: VNextPreparedSource,
    action: _SourceAction,
) -> _ObservationComponentStep:
    machine = source._machine
    observation = machine.observation
    if observation is None:
        raise RuntimeError("gallery observation is absent")
    attempt = BatchAttempt(secrets.token_bytes(16), machine.previous_operation_id)
    if action is _SourceAction.FILE_PAGE:
        file_page = source._snapshot.list_file_observations(
            observation,
            after_name_bytes=machine.file_after,
            limit=256,
        )
        file_next_after = _require_named_component_page(
            file_page,
            after=machine.file_after,
            capacity=256,
            label="FILE",
        )
        return _ObservationComponentStep(
            FileBatchCommand(file_page.items, file_page.terminal, attempt),
            file_next_after,
        )
    if action is _SourceAction.DIRECTORY_PAGE:
        directory_page = source._snapshot.list_directory_observations(
            observation,
            after_name_bytes=machine.directory_after,
            limit=192,
        )
        directory_next_after = _require_named_component_page(
            directory_page,
            after=machine.directory_after,
            capacity=192,
            label="DIRECTORY",
        )
        return _ObservationComponentStep(
            DirectoryBatchCommand(
                directory_page.items,
                directory_page.terminal,
                attempt,
            ),
            directory_next_after,
        )
    if action is _SourceAction.TAG_PAGE:
        tag_page = source._snapshot.list_tag_observations(
            observation,
            after_ordinal=machine.tag_after,
            limit=256,
        )
        tag_next_after = _require_tag_component_page(
            tag_page,
            after=machine.tag_after,
        )
        return _ObservationComponentStep(
            TagBatchCommand(tag_page.items, tag_page.terminal, attempt),
            tag_next_after,
        )
    if action is _SourceAction.METADATA_PAGE:
        chunks = machine.metadata_chunks
        if chunks is None:
            raise RuntimeError("gallery metadata chunk stream is absent")
        try:
            chunk, terminal = next(chunks)
        except StopIteration as error:
            raise RuntimeError(
                "gallery metadata stream ended without a page"
            ) from error
        return _ObservationComponentStep(
            MetadataBatchCommand(chunk, terminal, attempt),
            None,
        )
    raise RuntimeError(f"{action.value} is not an observation component action")


def _require_named_component_page(
    page: VNextIngestPage[object],
    *,
    after: bytes | None,
    capacity: int,
    label: str,
) -> bytes | None:
    if not isinstance(page, VNextIngestPage):
        raise TypeError(f"{label} adapter must return VNextIngestPage")
    page.__post_init__()
    if not page.terminal and len(page.items) != capacity:
        raise ValueError(f"nonterminal {label} page must contain {capacity} items")
    if page.terminal and not page.items and after is not None:
        raise ValueError(f"nonempty {label} streams cannot end with an empty page")
    prior = after
    for item in page.items:
        name = getattr(item, "name_bytes", None)
        if not isinstance(name, bytes):
            raise TypeError(f"{label} observation must expose bytes name_bytes")
        if prior is not None and name <= prior:
            raise ValueError(f"{label} page keys must be strictly increasing")
        prior = name
    if page.terminal:
        return None
    if not isinstance(page.next_after, bytes):
        raise TypeError(f"{label} next_after must be bytes")
    if not page.items or page.next_after != getattr(page.items[-1], "name_bytes"):
        raise ValueError(f"{label} next_after must equal the last item key")
    return page.next_after


def _require_tag_component_page(
    page: VNextIngestPage[object],
    *,
    after: int | None,
) -> int | None:
    if not isinstance(page, VNextIngestPage):
        raise TypeError("TAG adapter must return VNextIngestPage")
    page.__post_init__()
    if not page.terminal and len(page.items) != 256:
        raise ValueError("nonterminal TAG page must contain 256 items")
    if page.terminal and not page.items and after is not None:
        raise ValueError("nonempty TAG streams cannot end with an empty page")
    if page.terminal:
        return None
    if not isinstance(page.next_after, int):
        raise TypeError("TAG next_after must be an ordinal")
    start = 0 if after is None else after + 1
    if page.next_after != start + len(page.items) - 1:
        raise ValueError("TAG next_after must equal the last page ordinal")
    return page.next_after


def _resume_staging_machine(
    source: VNextPreparedSource,
    progress: GalleryStagingProgress,
) -> None:
    """Restore frozen-spool cursors from fixed-size repository authority."""

    machine = source._machine
    progress.handle.__post_init__()
    machine.staging_handle = progress.handle
    machine.file_after = None
    machine.directory_after = None
    machine.tag_after = None
    machine.metadata_chunks = None
    machine.previous_operation_id = None
    machine.match_previous_operation_id = None
    expected = (
        GalleryObservationComponent.FILE,
        GalleryObservationComponent.DIRECTORY,
        GalleryObservationComponent.TAG,
        GalleryObservationComponent.METADATA,
    )
    if len(progress.components) != len(expected):
        raise RuntimeError("gallery staging progress has the wrong component count")

    first_open: int | None = None
    for index, (component_progress, component) in enumerate(
        zip(progress.components, expected, strict=True)
    ):
        if component_progress.component is not component:
            raise RuntimeError("gallery staging progress component order differs")
        if component_progress.state not in {"OPEN", "COMPLETE"}:
            raise RuntimeError("gallery staging progress state is invalid")
        if first_open is None and component_progress.state == "OPEN":
            first_open = index
            machine.previous_operation_id = component_progress.latest_operation_id
            if component is GalleryObservationComponent.FILE:
                machine.file_after = component_progress.after_name_bytes
                machine.action = _SourceAction.FILE_PAGE
            elif component is GalleryObservationComponent.DIRECTORY:
                machine.directory_after = component_progress.after_name_bytes
                machine.action = _SourceAction.DIRECTORY_PAGE
            elif component is GalleryObservationComponent.TAG:
                machine.tag_after = component_progress.after_ordinal
                machine.action = _SourceAction.TAG_PAGE
            else:
                observation = machine.observation
                if observation is None:
                    raise RuntimeError("gallery observation is absent")
                machine.metadata_chunks = source._snapshot.iter_metadata_chunks(
                    observation,
                    start_offset=component_progress.cursor,
                )
                machine.action = _SourceAction.METADATA_PAGE
            _require_resumed_component_cursor(component_progress)
        elif first_open is not None and (
            component_progress.state != "OPEN"
            or component_progress.cursor != 0
            or component_progress.latest_operation_id is not None
        ):
            raise RuntimeError(
                "gallery staging progress advanced components out of order"
            )

    if first_open is not None:
        if (
            progress.match_state != "OPEN"
            or progress.matched_count != 0
            or progress.match_latest_operation_id is not None
        ):
            raise RuntimeError("gallery match advanced before component completion")
        return
    if progress.match_state == "OPEN":
        machine.match_previous_operation_id = progress.match_latest_operation_id
        machine.action = _SourceAction.MATCH
        return
    if progress.match_state == "COMPLETE":
        machine.action = _SourceAction.STAGING_SEAL
        return
    raise RuntimeError("gallery match progress state is invalid")


def _require_resumed_component_cursor(component_progress: object) -> None:
    cursor = getattr(component_progress, "cursor", None)
    operation = getattr(component_progress, "latest_operation_id", None)
    after_name = getattr(component_progress, "after_name_bytes", None)
    after_ordinal = getattr(component_progress, "after_ordinal", None)
    component = getattr(component_progress, "component", None)
    if not isinstance(cursor, int) or cursor < 0:
        raise RuntimeError("gallery component cursor is invalid")
    if (cursor == 0) != (operation is None):
        raise RuntimeError(
            "gallery component cursor and latest operation token disagree"
        )
    if component in {
        GalleryObservationComponent.FILE,
        GalleryObservationComponent.DIRECTORY,
    }:
        if (cursor == 0) != (after_name is None) or after_ordinal is not None:
            raise RuntimeError("gallery named-component cursor is invalid")
    elif component is GalleryObservationComponent.TAG:
        if (
            after_name is not None
            or (cursor > 0 and after_ordinal != cursor - 1)
            or (cursor == 0 and after_ordinal is not None)
        ):
            raise RuntimeError("gallery TAG cursor is invalid")
    elif component is GalleryObservationComponent.METADATA:
        if after_name is not None or after_ordinal is not None:
            raise RuntimeError("gallery METADATA cursor is invalid")
    else:
        raise RuntimeError("gallery component cursor type is invalid")


def _apply_source_outcome(
    source: VNextPreparedSource,
    step: VNextPreparedSourceStep,
    outcome: object,
) -> tuple[int, bool]:
    machine = source._machine
    action = step._action
    processed_rows = 0
    replayed = False
    if action is _SourceAction.INITIALIZE:
        payload = step._payload
        if not isinstance(payload, tuple) or len(payload) != 4:
            raise RuntimeError("prepared source initialization payload is invalid")
        build_id, command, upload, pages = payload
        if (
            not isinstance(build_id, bytes)
            or type(command) is not SourceRootBuildCommand
            or not isinstance(upload, CanonicalValueUploadPlan)
            or not isinstance(pages, Iterator)
        ):
            raise RuntimeError("prepared source initialization types are invalid")
        machine.build_id = build_id
        machine.root_command = command
        machine.root_upload = upload
        machine.root_pages = pages
        machine.action = _SourceAction.ROOT_ALLOCATE
    elif action is _SourceAction.ROOT_ALLOCATE:
        machine.action = _SourceAction.ROOT_PAGE
    elif action is _SourceAction.ROOT_PUT_PAGE:
        machine.action = _SourceAction.ROOT_PAGE
        processed_rows = 1
    elif action is _SourceAction.ROOT_SEAL:
        machine.action = _SourceAction.ROOT_HANDOFF
    elif action is _SourceAction.ROOT_HANDOFF:
        if isinstance(outcome, _SourceDrainRetry):
            # One bounded page of a retiring build's preparations was
            # abandoned; the root upload and frozen snapshot are untouched.
            # Re-issue the handoff to drain the rest before any new build; the
            # next page must start strictly past this committed one.
            processed_rows = outcome.abandoned
            machine.drained_page = outcome
            machine.action = _SourceAction.ROOT_HANDOFF
            return processed_rows, replayed
        if not isinstance(outcome, SourceBuildHandoff):
            raise RuntimeError("source-root handoff returned an invalid receipt")
        machine.drained_page = None
        policy = machine.policy
        if policy is None or outcome.manifest_policy_id != policy.manifest_policy_id:
            raise RuntimeError("source-root handoff used another manifest policy")
        machine.handoff = outcome
        machine.build_id = outcome.build_id
        replayed = outcome.replayed
        _require_root_upload(machine).close()
        machine.root_upload = None
        machine.root_pages = None
        machine.action = _SourceAction.DISCOVERY_BATCH
    elif action is _SourceAction.DISCOVERY_BATCH:
        batch = _require_exact_discovery_batch(step._payload)
        if batch.terminal:
            if not isinstance(outcome, DiscoveryBatchReceipt):
                raise RuntimeError("terminal discovery returned an invalid receipt")
            machine.discovered_galleries = outcome.next_processed_count
            replayed = outcome.replayed
            if batch.sealed_replay:
                if not outcome.replayed:
                    raise RuntimeError(
                        "sealed discovery did not replay its durable receipt"
                    )
                machine.staged_galleries = outcome.next_processed_count
                machine.sealed = True
                machine.action = _SourceAction.COMPLETE
            else:
                machine.action = _SourceAction.STAGING_FIND
        else:
            machine.discovery_batch = batch
            machine.resolved = []
            machine.locator_index = 0
            machine.action = _SourceAction.LOCATOR_INITIALIZE
    elif action is _SourceAction.LOCATOR_INITIALIZE:
        payload = step._payload
        if not isinstance(payload, tuple) or len(payload) != 2:
            raise RuntimeError("source-locator initialization payload is invalid")
        upload, pages = payload
        if not isinstance(upload, CanonicalValueUploadPlan) or not isinstance(
            pages,
            Iterator,
        ):
            raise RuntimeError("source-locator initialization types are invalid")
        machine.locator_upload = upload
        machine.locator_pages = pages
        machine.action = _SourceAction.LOCATOR_ALLOCATE
    elif action is _SourceAction.LOCATOR_ALLOCATE:
        machine.action = _SourceAction.LOCATOR_PAGE
    elif action is _SourceAction.LOCATOR_PUT_PAGE:
        machine.action = _SourceAction.LOCATOR_PAGE
        processed_rows = 1
    elif action is _SourceAction.LOCATOR_SEAL:
        machine.action = _SourceAction.LOCATOR_RESOLVE
    elif action is _SourceAction.LOCATOR_RESOLVE:
        if not isinstance(outcome, ResolvedDiscoveryLocator):
            raise RuntimeError("locator resolution returned an invalid receipt")
        if machine.resolved is None:
            raise RuntimeError("resolved discovery workset is absent")
        machine.resolved.append(outcome)
        replayed = outcome.replayed
        processed_rows = 1
        _require_locator_upload(machine).close()
        machine.locator_upload = None
        machine.locator_pages = None
        machine.locator_index += 1
        batch = _require_discovery_batch(machine)
        machine.action = (
            _SourceAction.LOCATOR_INITIALIZE
            if machine.locator_index < len(batch.locators)
            else _SourceAction.DISCOVERY_COMMIT
        )
    elif action is _SourceAction.DISCOVERY_COMMIT:
        if not isinstance(outcome, DiscoveryBatchReceipt):
            raise RuntimeError("discovery commit returned an invalid receipt")
        machine.discovered_galleries = outcome.next_processed_count
        processed_rows = outcome.row_count
        replayed = outcome.replayed
        machine.discovery_batch = None
        machine.resolved = None
        machine.locator_index = 0
        machine.action = _SourceAction.DISCOVERY_BATCH
    elif action is _SourceAction.STAGING_SELECT:
        payload = step._payload
        if not isinstance(payload, tuple) or len(payload) != 3:
            raise RuntimeError("pending gallery local observation is invalid")
        pending, locator, observation = payload
        if (
            not isinstance(pending, PendingSourceGallery)
            or not isinstance(locator, tuple)
            or not isinstance(observation, FrozenGalleryObservation)
        ):
            raise RuntimeError("pending gallery local observation types are invalid")
        machine.pending_gallery = pending
        machine.locator_components = locator
        machine.observation = observation
        machine.staged_galleries = pending.position
        machine.action = _SourceAction.STAGING_BEGIN
    elif action is _SourceAction.STAGING_COMPLETE:
        machine.staged_galleries = source._plan.gallery_count
        machine.action = _SourceAction.ASSEMBLY
    elif action is _SourceAction.STAGING_BEGIN:
        if not isinstance(outcome, GalleryStagingProgress):
            raise RuntimeError("gallery staging begin returned invalid progress")
        _resume_staging_machine(source, outcome)
    elif action is _SourceAction.STAGING_RECOVER:
        seal = step._payload
        if not isinstance(seal, GalleryStagingSeal):
            raise RuntimeError("recovered gallery staging seal is invalid")
        machine.staging_seal = seal
        replayed = True
        machine.action = _SourceAction.STAGING_RETIRE
    elif action in {
        _SourceAction.FILE_PAGE,
        _SourceAction.DIRECTORY_PAGE,
        _SourceAction.TAG_PAGE,
        _SourceAction.METADATA_PAGE,
    }:
        component_step = _require_component_step(step._payload)
        if not isinstance(outcome, GalleryStagingReceipt):
            raise RuntimeError("gallery component commit returned an invalid receipt")
        command = component_step.command
        machine.previous_operation_id = command.attempt.operation_id
        processed_rows = (
            1 if isinstance(command, MetadataBatchCommand) else len(command.entries)
        )
        replayed = outcome.replayed
        if action is _SourceAction.FILE_PAGE:
            if not isinstance(command, FileBatchCommand):
                raise RuntimeError("FILE outcome is bound to another command")
            if outcome.state == "COMPLETE":
                machine.previous_operation_id = None
                machine.action = _SourceAction.DIRECTORY_PAGE
            else:
                if not isinstance(component_step.next_after, bytes):
                    raise RuntimeError("nonterminal FILE page lost its cursor")
                machine.file_after = component_step.next_after
                machine.action = _SourceAction.FILE_PAGE
        elif action is _SourceAction.DIRECTORY_PAGE:
            if not isinstance(command, DirectoryBatchCommand):
                raise RuntimeError("DIRECTORY outcome is bound to another command")
            if outcome.state == "COMPLETE":
                machine.previous_operation_id = None
                machine.action = _SourceAction.TAG_PAGE
            else:
                if not isinstance(component_step.next_after, bytes):
                    raise RuntimeError("nonterminal DIRECTORY page lost its cursor")
                machine.directory_after = component_step.next_after
                machine.action = _SourceAction.DIRECTORY_PAGE
        elif action is _SourceAction.TAG_PAGE:
            if not isinstance(command, TagBatchCommand):
                raise RuntimeError("TAG outcome is bound to another command")
            if outcome.state == "COMPLETE":
                observation = machine.observation
                if observation is None:
                    raise RuntimeError("gallery observation is absent")
                machine.previous_operation_id = None
                machine.metadata_chunks = source._snapshot.iter_metadata_chunks(
                    observation,
                    start_offset=0,
                )
                machine.action = _SourceAction.METADATA_PAGE
            else:
                if not isinstance(component_step.next_after, int):
                    raise RuntimeError("nonterminal TAG page lost its cursor")
                machine.tag_after = component_step.next_after
                machine.action = _SourceAction.TAG_PAGE
        else:
            if not isinstance(command, MetadataBatchCommand):
                raise RuntimeError("METADATA outcome is bound to another command")
            if outcome.state == "COMPLETE":
                machine.previous_operation_id = None
                machine.metadata_chunks = None
                machine.action = _SourceAction.MATCH
            else:
                machine.action = _SourceAction.METADATA_PAGE
    elif action is _SourceAction.MATCH:
        command = step._payload
        if not isinstance(command, MatchBatchCommand) or not isinstance(
            outcome,
            MatchBatchReceipt,
        ):
            raise RuntimeError("gallery match returned an invalid receipt")
        machine.match_previous_operation_id = command.operation_id
        replayed = outcome.replayed
        processed_rows = outcome.matched_count
        machine.action = (
            _SourceAction.STAGING_SEAL
            if outcome.state == "COMPLETE"
            else _SourceAction.MATCH
        )
    elif action is _SourceAction.STAGING_SEAL:
        if not isinstance(outcome, GalleryStagingSeal):
            raise RuntimeError("gallery staging seal returned an invalid receipt")
        pending = _require_pending_gallery(machine)
        machine.staged_galleries = pending.position + 1
        replayed = outcome.replayed
        processed_rows = 1
        machine.staging_seal = outcome
        machine.action = _SourceAction.STAGING_RETIRE
    elif action is _SourceAction.STAGING_RETIRE:
        seal = step._payload
        if not isinstance(seal, GalleryStagingSeal) or not isinstance(
            outcome,
            GalleryStagingRetirement,
        ):
            raise RuntimeError("gallery staging retirement returned an invalid receipt")
        machine.staging_seal = seal
        processed_rows = outcome.deleted_count
        replayed = outcome.replayed
        if not outcome.complete:
            machine.action = _SourceAction.STAGING_RETIRE
            return processed_rows, replayed
        machine.pending_gallery = None
        machine.locator_components = None
        machine.observation = None
        machine.staging_handle = None
        machine.file_after = None
        machine.directory_after = None
        machine.tag_after = None
        machine.previous_operation_id = None
        machine.match_previous_operation_id = None
        machine.staging_seal = None
        machine.action = _SourceAction.STAGING_FIND
    elif action is _SourceAction.ASSEMBLY:
        if not isinstance(outcome, AssemblyBatchReceipt):
            raise RuntimeError("source assembly returned an invalid receipt")
        processed_rows = outcome.row_count
        replayed = outcome.replayed
        if outcome.terminal:
            machine.sealed = True
            machine.action = _SourceAction.COMPLETE
        else:
            machine.action = _SourceAction.ASSEMBLY
    else:
        raise RuntimeError(f"cannot apply source action {action.value}")
    return processed_rows, replayed


def _source_advance_result(
    source: VNextPreparedSource,
    *,
    processed_rows: int,
    replayed: bool,
) -> VNextIngestAdvanceResult:
    machine = source._machine
    if machine.build_id is None:
        raise RuntimeError("source step completed without a build ID")
    receipt = VNextIngestSourceReceipt(
        build_id=machine.build_id,
        discovered_galleries=machine.discovered_galleries,
        staged_galleries=machine.staged_galleries,
        sealed=machine.sealed,
        replayed=replayed,
    )
    return VNextIngestAdvanceResult(
        phase=VNextIngestPhase.SOURCE,
        processed_rows=processed_rows,
        terminal=machine.sealed,
        replayed=replayed,
        source_receipt=receipt,
    )


def _close_source_step_payload(step: VNextPreparedSourceStep | None) -> None:
    if step is None:
        return
    payload = step._payload
    if step._action in {_SourceAction.INITIALIZE, _SourceAction.LOCATOR_INITIALIZE}:
        if isinstance(payload, tuple):
            for item in payload:
                if isinstance(item, CanonicalValueUploadPlan):
                    item.close()


def _require_source_manifest_summary(
    receipt: AssemblyBatchReceipt,
    expected: SourceBuildManifestSummary,
) -> None:
    actual = SourceBuildManifestSummary(
        receipt.next_manifest_chain_sha256,
        receipt.next_gallery_count,
        receipt.next_file_count,
        receipt.next_byte_count,
    )
    if actual != expected:
        raise VNextSourceManifestMismatchError(
            "durable source build manifest differs from its frozen preflight snapshot"
        )


def _iter_source_locators(
    adapter: VNextIngestSourceAdapter,
) -> Iterator[tuple[str, ...]]:
    after: tuple[str, ...] | None = None
    prior_key: bytes | None = None
    while True:
        page = adapter.list_gallery_locators(
            after_locator=after,
            limit=_SOURCE_LOCATOR_PAGE_LIMIT,
        )
        if not isinstance(page, VNextIngestPage):
            raise TypeError("list_gallery_locators must return VNextIngestPage")
        page.__post_init__()
        for locator in page.items:
            if not isinstance(locator, tuple):
                raise TypeError("gallery locator must be an exact tuple")
            locator_key = encode_source_relative_locator(locator)
            if prior_key is not None and locator_key <= prior_key:
                raise ValueError("gallery locator pages must be strictly increasing")
            prior_key = locator_key
            yield locator
        if page.terminal:
            return
        if not isinstance(page.next_after, tuple):
            raise TypeError("gallery locator next_after must be a locator tuple")
        if not page.items or page.next_after != page.items[-1]:
            raise ValueError("gallery locator next_after must equal the last item")
        after = page.next_after
