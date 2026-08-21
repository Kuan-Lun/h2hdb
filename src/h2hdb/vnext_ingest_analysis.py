"""Bounded issue/prepare/commit orchestration for vNext analysis.

This module is an internal application component intended for composition by
``VNextIngestFacade``. It exposes no repository or connector to consumers.
Every issue and commit owns one bounded write transaction. Gallery and final
snapshot preparation use independent read transactions and disk-backed
canonical plans outside ingest-session serialization.
"""

from __future__ import annotations

__all__ = [
    "VNextAnalysisAdvanceResult",
    "VNextIngestAnalysisOrchestrator",
    "VNextIssuedAnalysisStep",
    "VNextPreparedAnalysis",
    "VNextPreparedAnalysisStep",
]

import secrets
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import StrEnum
from time import time_ns
from typing import TypeVar

from .domain import VNextIngestSession, VNextResolvedIngestPolicy
from .repository import RepositoryContext
from .vnext_analysis_repository import (
    AnalysisBatchResult,
    AnalysisGalleryPreparation,
    AnalysisRepository,
    AnalysisSnapshotPreparation,
    AnalysisStageIssue,
)
from .vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
    PreparedCanonicalPage,
)
from .vnext_domains import (
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
from .vnext_transaction import VNextUnitOfWork

_ResultT = TypeVar("_ResultT")
_PREPARED_ANALYSIS_TOKEN = object()
_ISSUED_ANALYSIS_STEP_TOKEN = object()
_PREPARED_ANALYSIS_STEP_TOKEN = object()
_ANALYSIS_SNAPSHOT_STAGE = b"snapshot_manifest"


def _now_microseconds() -> int:
    return time_ns() // 1_000


class _AnalysisAction(StrEnum):
    PREPARE_BATCH = "PREPARE_BATCH"
    PREPARE_SNAPSHOT = "PREPARE_SNAPSHOT"
    UPLOAD_NEXT = "UPLOAD_NEXT"
    UPLOAD_PAGE = "UPLOAD_PAGE"
    UPLOAD_ALLOCATE = "UPLOAD_ALLOCATE"
    UPLOAD_PUT = "UPLOAD_PUT"
    UPLOAD_SEAL = "UPLOAD_SEAL"
    PROCESS_BATCH = "PROCESS_BATCH"
    HANDOFF_SNAPSHOT = "HANDOFF_SNAPSHOT"
    COMPLETE = "COMPLETE"


@dataclass(frozen=True, slots=True)
class VNextAnalysisAdvanceResult:
    """Progress from one bounded analysis commit."""

    analysis_id: bytes
    stage: bytes
    processed_rows: int
    stage_terminal: bool
    terminal: bool
    replayed: bool
    snapshot_manifest_sha256: bytes | None = None

    def __post_init__(self) -> None:
        require_uuid16(self.analysis_id, field="analysis result analysis_id")
        require_bounded_bytes(
            self.stage,
            field="analysis result stage",
            minimum=1,
            maximum=64,
        )
        require_int63(self.processed_rows, field="analysis result processed_rows")
        if any(
            type(value) is not bool
            for value in (self.stage_terminal, self.terminal, self.replayed)
        ):
            raise TypeError("analysis result flags must be bool")
        if self.snapshot_manifest_sha256 is not None:
            require_digest32(
                self.snapshot_manifest_sha256,
                field="analysis result snapshot_manifest_sha256",
            )
        if self.terminal != (self.snapshot_manifest_sha256 is not None):
            raise ValueError(
                "terminal analysis result must carry exactly one snapshot digest"
            )


@dataclass(slots=True)
class _LocalAnalysisWork:
    issue: AnalysisStageIssue | None
    preparations: tuple[AnalysisGalleryPreparation | None, ...]
    snapshot: AnalysisSnapshotPreparation | None
    plans: tuple[CanonicalValueUploadPlan, ...]
    plan_index: int = 0
    pages: Iterator[PreparedCanonicalPage] | None = None

    @property
    def analysis_id(self) -> bytes:
        if self.issue is not None:
            return self.issue.analysis_id
        if self.snapshot is not None:
            return self.snapshot.analysis_id
        raise RuntimeError("local analysis work has no authority")

    @property
    def stage(self) -> bytes:
        if self.issue is not None:
            if self.issue.stage is None:
                raise RuntimeError("analysis batch work has no stage")
            return self.issue.stage
        return _ANALYSIS_SNAPSHOT_STAGE

    @property
    def current_plan(self) -> CanonicalValueUploadPlan:
        if not 0 <= self.plan_index < len(self.plans):
            raise RuntimeError("analysis upload plan position is exhausted")
        return self.plans[self.plan_index]

    def begin_current_pages(self) -> None:
        if self.pages is not None:
            raise RuntimeError("analysis upload page iterator is already active")
        self.pages = self.current_plan.iter_pages()

    def next_page(self) -> PreparedCanonicalPage | None:
        if self.pages is None:
            raise RuntimeError("analysis upload page iterator is absent")
        try:
            return next(self.pages)
        except StopIteration:
            return None

    def advance_plan(self) -> bool:
        self.pages = None
        self.plan_index += 1
        return self.plan_index < len(self.plans)

    def close(self) -> None:
        if self.snapshot is not None:
            self.snapshot.close()
            return
        for preparation in self.preparations:
            if preparation is not None:
                preparation.close()


@dataclass(slots=True)
class _AnalysisMachine:
    analysis_id: bytes | None = None
    action: _AnalysisAction | None = None
    local: _LocalAnalysisWork | None = None
    snapshot_manifest_sha256: bytes | None = None


class VNextPreparedAnalysis:
    """Closeable process-local analysis state backed by durable checkpoints."""

    __slots__ = (
        "_active_issue",
        "_active_step",
        "_build_id",
        "_closed",
        "_machine",
        "_max_rows",
        "_policy",
    )

    def __init__(
        self,
        *,
        build_id: bytes,
        policy: VNextResolvedIngestPolicy,
        max_rows: int,
        _constructor_token: object,
    ) -> None:
        if _constructor_token is not _PREPARED_ANALYSIS_TOKEN:
            raise TypeError("use VNextIngestFacade.prepare_analysis")
        self._build_id = require_uuid16(build_id, field="analysis build_id")
        _require_resolved_policy(policy)
        self._policy = policy
        limit = require_positive_int63(max_rows, field="analysis max_rows")
        if limit > 128:
            raise ValueError("analysis max_rows exceeds 128")
        self._max_rows = limit
        self._machine = _AnalysisMachine()
        self._active_issue: VNextIssuedAnalysisStep | None = None
        self._active_step: VNextPreparedAnalysisStep | None = None
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        local = self._machine.local
        step_local = _local_step_payload(self._active_step)
        if step_local is not None and step_local is not local:
            step_local.close()
        if local is not None:
            local.close()
        self._active_issue = None
        self._active_step = None
        self._machine.local = None

    def __enter__(self) -> VNextPreparedAnalysis:
        self._require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _require_open(self) -> None:
        if self._closed:
            raise ValueError("prepared analysis is closed")


class VNextIssuedAnalysisStep:
    """Opaque one-step issue bound to an ingest-session authority."""

    __slots__ = ("_action", "_analysis", "_payload", "_session")

    def __init__(
        self,
        *,
        analysis: VNextPreparedAnalysis,
        action: _AnalysisAction,
        payload: AnalysisStageIssue | None,
        session: VNextIngestSession,
        _constructor_token: object,
    ) -> None:
        if _constructor_token is not _ISSUED_ANALYSIS_STEP_TOKEN:
            raise TypeError("use VNextIngestFacade.issue_analysis_step")
        self._analysis = analysis
        self._action = action
        self._payload = payload
        self._session = session


class VNextPreparedAnalysisStep:
    """Opaque local preparation consumed by one bounded commit."""

    __slots__ = ("_action", "_issued", "_payload")

    def __init__(
        self,
        *,
        issued: VNextIssuedAnalysisStep,
        action: _AnalysisAction,
        payload: _LocalAnalysisWork | PreparedCanonicalPage | None,
        _constructor_token: object,
    ) -> None:
        if _constructor_token is not _PREPARED_ANALYSIS_STEP_TOKEN:
            raise TypeError("use VNextIngestFacade.prepare_analysis_step")
        self._issued = issued
        self._action = action
        self._payload = payload


class VNextIngestAnalysisOrchestrator:
    """Own connections and advance one analysis substep per public call."""

    __slots__ = ("__backend", "__clock", "__context", "__token_factory")

    def __init__(
        self,
        context: RepositoryContext,
        *,
        clock: Callable[[], int] = _now_microseconds,
        token_factory: Callable[[], bytes] = lambda: secrets.token_bytes(16),
    ) -> None:
        if not isinstance(context, RepositoryContext):
            raise TypeError("context must be RepositoryContext")
        if not callable(clock) or not callable(token_factory):
            raise TypeError("clock and token_factory must be callable")
        self.__context = context
        self.__backend = context.sql_type
        self.__clock = clock
        self.__token_factory = token_factory

    def prepare_analysis(
        self,
        build_id: bytes,
        policy: VNextResolvedIngestPolicy,
        *,
        max_rows: int,
    ) -> VNextPreparedAnalysis:
        """Create a restartable local handle without opening the database."""

        return VNextPreparedAnalysis(
            build_id=build_id,
            policy=policy,
            max_rows=max_rows,
            _constructor_token=_PREPARED_ANALYSIS_TOKEN,
        )

    def issue_analysis_step(
        self,
        session: VNextIngestSession,
        prepared: VNextPreparedAnalysis,
    ) -> VNextIssuedAnalysisStep:
        """Issue one DB-bounded action from durable analysis authority."""

        analysis = _require_prepared_analysis(prepared)
        active = analysis._active_issue
        if active is not None:
            _require_same_session_authority(active._session, session)
            self.__write(lambda work: _resume_authority(work, session, self.__clock()))
            return active

        machine = analysis._machine
        payload: AnalysisStageIssue | None = None
        if machine.action is None:
            proposed_analysis_id = _token16(
                self.__token_factory(),
                field="proposed analysis_id",
            )
            batch_key = _token16(
                self.__token_factory(),
                field="analysis batch_key",
            )
            now = require_int63(self.__clock(), field="analysis issue now")

            def issue(work: VNextUnitOfWork) -> tuple[bytes, AnalysisStageIssue]:
                gate, turn = _repository_authority(session)
                run, stage_issue = AnalysisRepository.begin_and_issue_next_batch(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn,
                    build_id=analysis._build_id,
                    policy_id=analysis._policy.analysis_policy_id,
                    proposed_analysis_id=proposed_analysis_id,
                    batch_key=batch_key,
                    max_rows=analysis._max_rows,
                    now=now,
                )
                return run.analysis_id, stage_issue

            analysis_id, issued_payload = self.__write(issue)
            payload = issued_payload
            if machine.analysis_id is not None and machine.analysis_id != analysis_id:
                raise RuntimeError("analysis natural-key replay changed analysis_id")
            machine.analysis_id = analysis_id
            if issued_payload.stage is not None:
                action = _AnalysisAction.PREPARE_BATCH
            elif issued_payload.completion_snapshot_sha256 is not None:
                action = _AnalysisAction.COMPLETE
            else:
                action = _AnalysisAction.PREPARE_SNAPSHOT
            machine.action = action
        else:
            action = machine.action
            assert action is not None
            self.__write(lambda work: _resume_authority(work, session, self.__clock()))

        issued = VNextIssuedAnalysisStep(
            analysis=analysis,
            action=action,
            payload=payload,
            session=session,
            _constructor_token=_ISSUED_ANALYSIS_STEP_TOKEN,
        )
        analysis._active_issue = issued
        return issued

    def prepare_analysis_step(
        self,
        prepared: VNextPreparedAnalysis,
        issued: VNextIssuedAnalysisStep,
    ) -> VNextPreparedAnalysisStep:
        """Perform gallery/snapshot/page I/O without session serialization."""

        analysis = _require_prepared_analysis(prepared)
        if not isinstance(issued, VNextIssuedAnalysisStep):
            raise TypeError("issued must be VNextIssuedAnalysisStep")
        if issued._analysis is not analysis or analysis._active_issue is not issued:
            raise ValueError("issued analysis step is stale or belongs elsewhere")
        if analysis._active_step is not None:
            return analysis._active_step

        action = issued._action
        payload: _LocalAnalysisWork | PreparedCanonicalPage | None = None
        if action is _AnalysisAction.PREPARE_BATCH:
            if issued._payload is None:
                raise RuntimeError("analysis batch issue payload is absent")
            local = self._prepare_gallery_work(issued._payload)
            payload = local
            if local.plans:
                local.begin_current_pages()
                action = _AnalysisAction.UPLOAD_ALLOCATE
            else:
                action = _AnalysisAction.PROCESS_BATCH
        elif action is _AnalysisAction.PREPARE_SNAPSHOT:
            if issued._payload is None:
                raise RuntimeError("analysis snapshot authority is absent")
            local = self._prepare_snapshot_work(issued._payload)
            local.begin_current_pages()
            payload = local
            action = _AnalysisAction.UPLOAD_ALLOCATE
        elif action is _AnalysisAction.UPLOAD_NEXT:
            local = _require_local_work(analysis)
            local.begin_current_pages()
            action = _AnalysisAction.UPLOAD_ALLOCATE
        elif action is _AnalysisAction.UPLOAD_PAGE:
            page = _require_local_work(analysis).next_page()
            if page is None:
                action = _AnalysisAction.UPLOAD_SEAL
            else:
                action = _AnalysisAction.UPLOAD_PUT
                payload = page
        elif action not in {
            _AnalysisAction.PROCESS_BATCH,
            _AnalysisAction.HANDOFF_SNAPSHOT,
            _AnalysisAction.COMPLETE,
        }:
            raise RuntimeError(f"analysis action cannot be prepared: {action}")

        step = VNextPreparedAnalysisStep(
            issued=issued,
            action=action,
            payload=payload,
            _constructor_token=_PREPARED_ANALYSIS_STEP_TOKEN,
        )
        analysis._active_step = step
        return step

    def commit_analysis_step(
        self,
        session: VNextIngestSession,
        prepared_step: VNextPreparedAnalysisStep,
    ) -> VNextAnalysisAdvanceResult:
        """Commit one bounded upload, analysis batch, handoff, or replay."""

        if not isinstance(prepared_step, VNextPreparedAnalysisStep):
            raise TypeError("prepared_step must be VNextPreparedAnalysisStep")
        issued = prepared_step._issued
        analysis = _require_prepared_analysis(issued._analysis)
        if (
            analysis._active_issue is not issued
            or analysis._active_step is not prepared_step
        ):
            raise ValueError("prepared analysis step is stale")
        _require_same_session_authority(issued._session, session)
        gate, turn = _repository_authority(session)
        action = prepared_step._action
        local = _local_for_commit(analysis, prepared_step)
        now = require_int63(self.__clock(), field="analysis commit now")

        def commit(work: VNextUnitOfWork) -> object:
            if action is _AnalysisAction.UPLOAD_ALLOCATE:
                return CanonicalValueRepository.allocate(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=local.current_plan,
                    now=now,
                )
            if action is _AnalysisAction.UPLOAD_PUT:
                page = prepared_step._payload
                if not isinstance(page, PreparedCanonicalPage):
                    raise TypeError("analysis upload page is missing")
                return CanonicalValueRepository.put_page(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=local.current_plan,
                    prepared_page=page,
                    now=now,
                )
            if action is _AnalysisAction.UPLOAD_SEAL:
                return CanonicalValueRepository.seal(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn,
                    plan=local.current_plan,
                    now=now,
                )
            if action is _AnalysisAction.PROCESS_BATCH:
                if local.issue is None:
                    raise RuntimeError("analysis batch local work is absent")
                return AnalysisRepository.process_issued_batch(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn,
                    issue=local.issue,
                    preparations=local.preparations,
                    now=now,
                )
            if action is _AnalysisAction.HANDOFF_SNAPSHOT:
                if local.snapshot is None:
                    raise RuntimeError("analysis snapshot local work is absent")
                return AnalysisRepository.handoff_snapshot_manifest(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn,
                    preparation=local.snapshot,
                    now=now,
                )
            if action is _AnalysisAction.COMPLETE:
                payload = issued._payload
                digest = (
                    analysis._machine.snapshot_manifest_sha256
                    if payload is None
                    else payload.completion_snapshot_sha256
                )
                if digest is None or analysis._machine.analysis_id is None:
                    raise RuntimeError("completed analysis issue is incomplete")
                AnalysisRepository.complete(
                    work,
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=analysis._machine.analysis_id,
                    now=now,
                )
                return digest
            raise RuntimeError(f"analysis action cannot be committed: {action}")

        outcome = self.__write(commit)
        result = _apply_commit_outcome(
            analysis,
            issued,
            prepared_step,
            local,
            outcome,
        )
        analysis._active_step = None
        analysis._active_issue = None
        return result

    def _prepare_gallery_work(
        self,
        issue: AnalysisStageIssue,
    ) -> _LocalAnalysisWork:
        preparations: list[AnalysisGalleryPreparation | None] = []
        try:
            for gallery_id, observation_id in issue.memberships:
                if observation_id is None:
                    preparations.append(None)
                    continue
                with self.__context.SQLConnector() as connector:
                    preparation = AnalysisRepository.prepare_gallery(
                        connector,
                        backend=self.__backend,
                        authority=issue.preparation_authority,
                        gallery_id=gallery_id,
                    )
                if preparation.observation_id != observation_id:
                    preparation.close()
                    raise RuntimeError(
                        "prepared gallery changed from its issued membership"
                    )
                preparations.append(preparation)
        except BaseException:
            for prepared_gallery in preparations:
                if prepared_gallery is not None:
                    prepared_gallery.close()
            raise
        exact = tuple(preparations)
        plans = tuple(
            preparation.content_upload_plan
            for preparation in exact
            if preparation is not None and preparation.content_upload_plan is not None
        )
        return _LocalAnalysisWork(issue, exact, None, plans)

    def _prepare_snapshot_work(
        self,
        issue: AnalysisStageIssue,
    ) -> _LocalAnalysisWork:
        with self.__context.SQLConnector() as connector:
            snapshot = AnalysisRepository.prepare_snapshot_manifest(
                connector,
                backend=self.__backend,
                authority=issue.preparation_authority,
            )
        return _LocalAnalysisWork(None, (), snapshot, (snapshot.upload_plan,))

    def __write(
        self,
        operation: Callable[[VNextUnitOfWork], _ResultT],
    ) -> _ResultT:
        with self.__context.SQLConnector() as connector:
            with connector.transaction():
                return operation(VNextUnitOfWork(connector, backend=self.__backend))


def _require_resolved_policy(policy: VNextResolvedIngestPolicy) -> None:
    if not isinstance(policy, VNextResolvedIngestPolicy):
        raise TypeError("policy must be VNextResolvedIngestPolicy")
    policy.__post_init__()


def _require_prepared_analysis(
    prepared: VNextPreparedAnalysis,
) -> VNextPreparedAnalysis:
    if not isinstance(prepared, VNextPreparedAnalysis):
        raise TypeError("prepared must be VNextPreparedAnalysis")
    prepared._require_open()
    return prepared


def _require_local_work(analysis: VNextPreparedAnalysis) -> _LocalAnalysisWork:
    local = analysis._machine.local
    if local is None:
        raise RuntimeError("analysis local work is absent")
    return local


def _local_for_commit(
    analysis: VNextPreparedAnalysis,
    step: VNextPreparedAnalysisStep,
) -> _LocalAnalysisWork:
    if isinstance(step._payload, _LocalAnalysisWork):
        return step._payload
    if step._action is _AnalysisAction.COMPLETE:
        # COMPLETE has no local spool; this sentinel is never dereferenced.
        return _LocalAnalysisWork(None, (), None, ())
    return _require_local_work(analysis)


def _local_step_payload(
    step: VNextPreparedAnalysisStep | None,
) -> _LocalAnalysisWork | None:
    if step is not None and isinstance(step._payload, _LocalAnalysisWork):
        return step._payload
    return None


def _apply_commit_outcome(
    analysis: VNextPreparedAnalysis,
    issued: VNextIssuedAnalysisStep,
    step: VNextPreparedAnalysisStep,
    local: _LocalAnalysisWork,
    outcome: object,
) -> VNextAnalysisAdvanceResult:
    machine = analysis._machine
    analysis_id = machine.analysis_id
    if analysis_id is None:
        raise RuntimeError("analysis commit has no durable analysis_id")
    action = step._action
    if action is _AnalysisAction.UPLOAD_ALLOCATE:
        if machine.local is None:
            machine.local = local
        elif machine.local is not local:
            raise RuntimeError("analysis upload changed local work")
        machine.action = _AnalysisAction.UPLOAD_PAGE
        return _progress_result(local, analysis_id)
    if action is _AnalysisAction.UPLOAD_PUT:
        machine.action = _AnalysisAction.UPLOAD_PAGE
        return _progress_result(local, analysis_id)
    if action is _AnalysisAction.UPLOAD_SEAL:
        if local.advance_plan():
            machine.action = _AnalysisAction.UPLOAD_NEXT
        elif local.issue is not None:
            machine.action = _AnalysisAction.PROCESS_BATCH
        else:
            machine.action = _AnalysisAction.HANDOFF_SNAPSHOT
        return _progress_result(local, analysis_id)
    if action is _AnalysisAction.PROCESS_BATCH:
        if not isinstance(outcome, AnalysisBatchResult):
            raise TypeError("analysis batch commit returned a foreign result")
        outcome.__post_init__()
        if local.issue is None or outcome.analysis_id != local.issue.analysis_id:
            raise RuntimeError("analysis batch result differs from its issue")
        result = VNextAnalysisAdvanceResult(
            analysis_id,
            outcome.stage,
            outcome.row_count,
            outcome.terminal,
            False,
            outcome.replayed,
        )
        local.close()
        machine.local = None
        machine.action = None
        return result
    if action is _AnalysisAction.HANDOFF_SNAPSHOT:
        digest = require_digest32(outcome, field="analysis snapshot handoff digest")
        result = VNextAnalysisAdvanceResult(
            analysis_id,
            _ANALYSIS_SNAPSHOT_STAGE,
            0,
            True,
            True,
            False,
            digest,
        )
        local.close()
        machine.local = None
        machine.action = _AnalysisAction.COMPLETE
        machine.snapshot_manifest_sha256 = digest
        return result
    if action is _AnalysisAction.COMPLETE:
        digest = require_digest32(outcome, field="completed analysis snapshot digest")
        machine.action = _AnalysisAction.COMPLETE
        machine.snapshot_manifest_sha256 = digest
        return VNextAnalysisAdvanceResult(
            analysis_id,
            _ANALYSIS_SNAPSHOT_STAGE,
            0,
            True,
            True,
            True,
            digest,
        )
    raise RuntimeError(f"analysis outcome has unsupported action: {action}")


def _progress_result(
    local: _LocalAnalysisWork,
    analysis_id: bytes,
) -> VNextAnalysisAdvanceResult:
    return VNextAnalysisAdvanceResult(
        analysis_id,
        local.stage,
        0,
        False,
        False,
        False,
    )


def _repository_authority(
    session: VNextIngestSession,
) -> tuple[GateLease, IngestTurn]:
    if not isinstance(session, VNextIngestSession):
        raise TypeError("session must be VNextIngestSession")
    session.__post_init__()
    return (
        GateLease(
            session.gate_owner_token,
            session.gate_generation,
            GateMode.SHARED,
            (session.gate_slot,),
            session.gate_lease_expires_at,
        ),
        IngestTurn(
            session.ingest_generation,
            session.ingest_owner_token,
            session.ingest_lease_expires_at,
        ),
    )


def _resume_authority(
    work: VNextUnitOfWork,
    session: VNextIngestSession,
    now: int,
) -> None:
    gate, turn = _repository_authority(session)
    timestamp = require_int63(now, field="analysis issue authorization now")
    MaintenanceGateRepository.resume(work, gate, now=timestamp)
    IngestFenceRepository.lock_and_require_live(work, turn, now=timestamp)


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
        raise ValueError("analysis step belongs to another ingest session authority")


def _token16(value: bytes, *, field: str) -> bytes:
    return require_uuid16(value, field=field)
