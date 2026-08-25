from __future__ import annotations

import inspect
import sqlite3
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

import h2hdb
import h2hdb.vnext_ingest_publication as publication
from h2hdb.config_loader import CoreConfig, DatabaseConfig
from h2hdb.domain import (
    ArtifactReleaseStorageEvidence,
    VNextCurrentProjectionItem,
    VNextIngestSession,
)
from h2hdb.repository import RepositoryContext
from h2hdb.vnext_artifact_preparation_repository import ArtifactPreparationRepository
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValuePartialFamilyError,
    CanonicalValueReadReceipt,
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_current_projection_repository import (
    CurrentProjectionArtifactItem,
    CurrentProjectionArtifactPage,
    CurrentProjectionArtifactRepository,
)
from h2hdb.vnext_download_ingest_repository import DownloadIngestRepository
from h2hdb.vnext_identity import (
    artifact_locator_components,
    artifact_locator_digest,
    encode_artifact_protection_token,
    publication_key,
)
from h2hdb.vnext_ingest_fence_repository import IngestTurn
from h2hdb.vnext_maintenance_gate_repository import MaintenanceGateRepository
from h2hdb.vnext_operational_event_repository import (
    OperationalEffectRepository,
    RemovedGid,
)
from h2hdb.vnext_publication_candidate_repository import (
    PublicationCandidateRepository,
)
from h2hdb.vnext_publication_finalization_repository import (
    _PAGE_CAPABILITY,
    PublicationFinalizationItem,
    PublicationFinalizationPage,
    PublicationFinalizationRepository,
)
from h2hdb.vnext_transaction import LockRank, encode_lock_key


def _context(path: Path) -> RepositoryContext:
    return RepositoryContext.from_config(
        CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    )


def _counting_context(path: Path, begins: list[str]) -> RepositoryContext:
    context = _context(path)
    original_factory = context.SQLConnector

    def factory() -> Any:
        connector = original_factory()
        original_begin = connector.begin

        def begin() -> None:
            begins.append("BEGIN IMMEDIATE")
            original_begin()

        cast(Any, connector).begin = begin
        return connector

    return replace(context, SQLConnector=factory)


def _probe_begin_immediate(path: Path) -> None:
    database = sqlite3.connect(path, isolation_level=None, timeout=0.05)
    try:
        database.execute("BEGIN IMMEDIATE")
        database.rollback()
    finally:
        database.close()


def test_publication_contract_is_top_level_and_facade_owned() -> None:
    assert {
        "CurrentProjectionCheckpoint",
        "CurrentProjectionStatus",
        "VNextCurrentProjectionAdapter",
        "VNextIssuedPublicationStep",
        "VNextPreparedPublicationStep",
    } <= set(h2hdb.__all__)
    assert "VNextIngestPublication" not in h2hdb.__all__
    assert h2hdb.CurrentProjectionCheckpoint is publication.CurrentProjectionCheckpoint
    assert h2hdb.CurrentProjectionStatus is publication.CurrentProjectionStatus
    assert h2hdb.VNextCurrentProjectionAdapter is (
        publication.VNextCurrentProjectionAdapter
    )
    assert h2hdb.VNextIssuedPublicationStep is (publication.VNextIssuedPublicationStep)
    assert h2hdb.VNextPreparedPublicationStep is (
        publication.VNextPreparedPublicationStep
    )
    assert tuple(
        inspect.signature(h2hdb.VNextIngestFacade.issue_publication_step).parameters
    ) == ("self", "session", "policy")
    assert tuple(
        inspect.signature(h2hdb.VNextIngestFacade.prepare_publication_step).parameters
    ) == (
        "self",
        "issued",
        "artifact_adapters",
        "finalization_adapters",
        "current_projection",
    )
    assert tuple(
        inspect.signature(h2hdb.VNextIngestFacade.commit_publication_step).parameters
    ) == ("self", "session", "prepared")
    assert tuple(inspect.signature(publication.VNextIngestPublication).parameters) == (
        "context",
        "clock",
    )


def test_publication_facade_reuses_its_repository_context_and_clock() -> None:
    def clock() -> int:
        return 123

    facade = h2hdb.VNextIngestFacade(
        CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=":memory:")),
        clock=clock,
    )

    orchestrator = cast(Any, facade)._VNextIngestFacade__publication_orchestrator()

    assert orchestrator._VNextIngestPublication__context is (
        cast(Any, facade)._VNextIngestFacade__context
    )
    assert orchestrator._VNextIngestPublication__clock is clock


def _session(*, expires_at: int = 100, owner: bytes = b"i" * 16) -> VNextIngestSession:
    return VNextIngestSession(
        gate_owner_token=b"g" * 16,
        gate_generation=1,
        gate_slot=0,
        gate_lease_expires_at=expires_at,
        ingest_generation=1,
        ingest_owner_token=owner,
        ingest_lease_expires_at=expires_at,
        download_generation=None,
        handoff_owner_token=None,
        handoff_kind=None,
        consumed_at=None,
    )


class _ProjectionAdapter:
    def __init__(
        self,
        checkpoint: publication.CurrentProjectionCheckpoint,
        *,
        callback: Callable[[], None] | None = None,
    ) -> None:
        self.checkpoint = checkpoint
        self.appended: list[tuple[VNextCurrentProjectionItem, ...]] = []
        self.sealed = False
        self.reconciled = False
        self.guard_depth = 0
        self.callback = callback

    @contextmanager
    def publication_guard(self) -> Iterator[None]:
        if self.guard_depth:
            raise RuntimeError("nested publication guard")
        self.guard_depth = 1
        try:
            yield
        finally:
            self.guard_depth = 0

    def _called(self) -> None:
        assert self.guard_depth == 1
        if self.callback is not None:
            self.callback()

    def begin(
        self,
        revision: int,
        receipt_id: bytes,
    ) -> publication.CurrentProjectionCheckpoint:
        self._called()
        assert revision == self.checkpoint.revision
        assert receipt_id == self.checkpoint.receipt_id
        return self.checkpoint

    def append_page(
        self,
        revision: int,
        items: Sequence[VNextCurrentProjectionItem],
    ) -> None:
        self._called()
        assert revision == self.checkpoint.revision
        self.appended.append(tuple(items))
        if items:
            self.checkpoint = publication.CurrentProjectionCheckpoint(
                revision,
                self.checkpoint.receipt_id,
                publication.CurrentProjectionStatus.SPOOL,
                items[-1].publication_key,
            )

    def seal(self, revision: int) -> None:
        self._called()
        assert revision == self.checkpoint.revision
        self.sealed = True
        self.checkpoint = publication.CurrentProjectionCheckpoint(
            revision,
            self.checkpoint.receipt_id,
            publication.CurrentProjectionStatus.RECONCILE,
            None,
        )

    def reconcile(self, revision: int) -> None:
        self._called()
        assert revision == self.checkpoint.revision
        self.reconciled = True
        self.checkpoint = publication.CurrentProjectionCheckpoint(
            revision,
            self.checkpoint.receipt_id,
            publication.CurrentProjectionStatus.COMPLETE,
            None,
        )


def _projection_item(gid: int) -> CurrentProjectionArtifactItem:
    artifact = gid.to_bytes(32, "big")
    return CurrentProjectionArtifactItem(
        publication_key(gid),
        gid,
        f"gallery-{gid}",
        gid,
        artifact_locator_components(artifact),
        artifact,
        gid,
    )


def test_checkpoint_is_receipt_scoped_and_cursor_is_spool_only() -> None:
    receipt = b"r" * 16
    cursor = publication_key(1)
    checkpoint = publication.CurrentProjectionCheckpoint(
        3,
        receipt,
        publication.CurrentProjectionStatus.SPOOL,
        cursor,
    )
    assert checkpoint.last_publication_key == cursor

    with pytest.raises(ValueError, match="only an open projection spool"):
        publication.CurrentProjectionCheckpoint(
            3,
            receipt,
            publication.CurrentProjectionStatus.COMPLETE,
            cursor,
        )
    with pytest.raises(ValueError, match="another receipt"):
        publication._require_projection_checkpoint(
            checkpoint,
            revision=3,
            receipt_id=b"x" * 16,
        )


def test_current_projection_maps_repository_items_to_the_shared_neutral_type(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = b"r" * 16
    checkpoint = publication.CurrentProjectionCheckpoint(
        3,
        receipt,
        publication.CurrentProjectionStatus.SPOOL,
        None,
    )
    adapter = _ProjectionAdapter(checkpoint)
    page = CurrentProjectionArtifactPage(
        receipt,
        3,
        (_projection_item(1),),
        publication_key(1),
        False,
    )
    calls: list[tuple[bytes | None, int]] = []

    def list_page(_connector: object, **kwargs: Any) -> CurrentProjectionArtifactPage:
        calls.append((kwargs["cursor"], kwargs["page_limit"]))
        return page

    monkeypatch.setattr(
        CurrentProjectionArtifactRepository,
        "list_page",
        staticmethod(list_page),
    )
    machine = publication.VNextIngestPublication(
        _context(tmp_path / "projection.sqlite3")
    )
    with adapter.publication_guard():
        prepared = cast(
            Any, machine
        )._VNextIngestPublication__prepare_current_projection(
            publication._ProjectionWork(receipt, 3, checkpoint), adapter
        )

    assert prepared.processed_rows == 1
    assert prepared.terminal_page is False
    assert calls == [(None, 128)]
    assert len(adapter.appended) == 1
    assert type(adapter.appended[0][0]) is VNextCurrentProjectionItem


def test_restart_after_database_commit_uses_adapter_owned_cursor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = b"r" * 16
    cursor = publication_key(1)
    checkpoint = publication.CurrentProjectionCheckpoint(
        3,
        receipt,
        publication.CurrentProjectionStatus.SPOOL,
        cursor,
    )
    adapter = _ProjectionAdapter(checkpoint)
    observed: list[bytes | None] = []

    def list_page(_connector: object, **kwargs: Any) -> CurrentProjectionArtifactPage:
        observed.append(kwargs["cursor"])
        return CurrentProjectionArtifactPage(receipt, 3, (), None, True)

    monkeypatch.setattr(
        CurrentProjectionArtifactRepository,
        "list_page",
        staticmethod(list_page),
    )
    restarted = publication.VNextIngestPublication(
        _context(tmp_path / "restart.sqlite3")
    )
    with adapter.publication_guard():
        result = cast(
            Any, restarted
        )._VNextIngestPublication__prepare_current_projection(
            publication._ProjectionWork(receipt, 3, checkpoint), adapter
        )

    assert observed == [cursor]
    assert result.terminal_page is True
    assert adapter.appended == [()]
    assert adapter.sealed is True


def test_projection_page_hard_cap_is_always_128(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = b"r" * 16
    checkpoint = publication.CurrentProjectionCheckpoint(
        3,
        receipt,
        publication.CurrentProjectionStatus.SPOOL,
        None,
    )
    adapter = _ProjectionAdapter(checkpoint)
    items = tuple(
        sorted(
            (_projection_item(gid) for gid in range(1, 129)),
            key=lambda item: item.publication_key,
        )
    )
    page = CurrentProjectionArtifactPage(
        receipt,
        3,
        items,
        items[-1].publication_key,
        False,
    )

    def list_page(_connector: object, **kwargs: Any) -> CurrentProjectionArtifactPage:
        assert kwargs["page_limit"] == 128
        return page

    monkeypatch.setattr(
        CurrentProjectionArtifactRepository,
        "list_page",
        staticmethod(list_page),
    )
    machine = publication.VNextIngestPublication(_context(tmp_path / "cap.sqlite3"))
    with adapter.publication_guard():
        result = cast(Any, machine)._VNextIngestPublication__prepare_current_projection(
            publication._ProjectionWork(receipt, 3, checkpoint), adapter
        )
    assert result.processed_rows == 128
    assert len(adapter.appended[0]) == 128


def test_projection_adapter_io_runs_under_only_the_callers_outer_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "projection-boundary.sqlite3"
    sqlite3.connect(path).close()
    receipt = b"r" * 16
    checkpoint = publication.CurrentProjectionCheckpoint(
        3,
        receipt,
        publication.CurrentProjectionStatus.SPOOL,
        None,
    )
    probes: list[str] = []

    def probe() -> None:
        _probe_begin_immediate(path)
        probes.append("adapter")

    adapter = _ProjectionAdapter(checkpoint, callback=probe)

    def list_page(connector: Any, **_kwargs: Any) -> CurrentProjectionArtifactPage:
        with connector.read_transaction():
            assert connector.fetch_one("SELECT 1") == (1,)
        return CurrentProjectionArtifactPage(receipt, 3, (), None, True)

    monkeypatch.setattr(
        CurrentProjectionArtifactRepository,
        "list_page",
        staticmethod(list_page),
    )
    machine = publication.VNextIngestPublication(_context(path))
    with adapter.publication_guard():
        result = cast(Any, machine)._VNextIngestPublication__prepare_current_projection(
            publication._ProjectionWork(receipt, 3, checkpoint), adapter
        )
        with pytest.raises(RuntimeError, match="nested publication guard"):
            with adapter.publication_guard():
                pass

    assert result.terminal_page is True
    assert probes == ["adapter", "adapter", "adapter"]
    assert (
        "current_projection"
        not in inspect.signature(
            publication.VNextIngestPublication.issue_step
        ).parameters
    )


def test_renewed_session_is_accepted_but_foreign_owner_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = _session(expires_at=100)
    renewed = replace(
        original,
        gate_lease_expires_at=200,
        ingest_lease_expires_at=200,
    )
    foreign = replace(renewed, ingest_owner_token=b"x" * 16)
    issued = publication.VNextIssuedPublicationStep(
        action=publication._Action.COMPLETE,
        payload=object(),
        session=original,
        _token=publication._STEP_TOKEN,
    )
    machine = publication.VNextIngestPublication(_context(tmp_path / "session.sqlite3"))
    adapter = _ProjectionAdapter(
        publication.CurrentProjectionCheckpoint(
            1,
            b"r" * 16,
            publication.CurrentProjectionStatus.COMPLETE,
            None,
        )
    )
    monkeypatch.setattr(
        publication,
        "_commit_action",
        lambda *_args, **_kwargs: object(),
    )

    prepared = machine.prepare_step(
        issued,
        artifact_adapters={},
        finalization_adapters={},
        current_projection=adapter,
    )
    machine.commit_step(renewed, prepared)
    foreign_prepared = machine.prepare_step(
        issued,
        artifact_adapters={},
        finalization_adapters={},
        current_projection=adapter,
    )
    with pytest.raises(ValueError, match="another ingest session authority"):
        machine.commit_step(
            foreign,
            foreign_prepared,
        )
    foreign_prepared.close()


def test_issue_and_commit_each_own_one_short_write_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    begins: list[str] = []
    machine = publication.VNextIngestPublication(
        _counting_context(tmp_path / "transaction-count.sqlite3", begins),
        clock=lambda: 10,
    )
    session = _session()
    root = publication._Root(b"b" * 16, b"a" * 16, 1, None, None, None, None)
    monkeypatch.setattr(publication, "_require_policy", lambda _policy: None)
    monkeypatch.setattr(
        publication,
        "_resume_authority",
        lambda _work, current, _now: current,
    )
    monkeypatch.setattr(publication, "_load_root", lambda *_args: root)
    monkeypatch.setattr(
        publication,
        "_issue_database_action",
        lambda *_args, **_kwargs: (publication._Action.COMPLETE, root),
    )
    monkeypatch.setattr(
        publication,
        "_commit_action",
        lambda *_args, **_kwargs: object(),
    )
    checkpoint = publication.CurrentProjectionCheckpoint(
        1,
        b"r" * 16,
        publication.CurrentProjectionStatus.COMPLETE,
        None,
    )
    adapter = _ProjectionAdapter(checkpoint)

    issued = machine.issue_step(session, cast(Any, object()))
    assert begins == ["BEGIN IMMEDIATE"]
    with adapter.publication_guard():
        prepared = machine.prepare_step(
            issued,
            artifact_adapters={},
            finalization_adapters={},
            current_projection=adapter,
        )
    assert begins == ["BEGIN IMMEDIATE"]
    machine.commit_step(session, prepared)
    assert begins == ["BEGIN IMMEDIATE", "BEGIN IMMEDIATE"]


def test_catalog_issue_reuses_the_outer_gate_and_ingest_authorization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The BUILD_CATALOG issuer must never restart at MAINTENANCE_GATE."""

    root = publication._Root(b"b" * 16, b"a" * 16, 1, b"c" * 16, None, None, None)
    states = tuple(
        (stage, "COMPLETE" if index < 2 else "OPEN")
        for index, stage in enumerate(publication._CANDIDATE_STAGES)
    )

    def resume_gate(work: Any, lease: Any, *, now: int) -> Any:
        assert now == 10
        work.lock_row(
            LockRank.MAINTENANCE_GATE,
            encode_lock_key("test-gate"),
            "SELECT 1",
        )
        return lease

    def resume_ingest(work: Any, coordinated: Any, *, now: int) -> Any:
        assert now == 10
        work.lock_row(
            LockRank.DOWNLOAD_FENCE,
            encode_lock_key("test-download"),
            "SELECT 1",
        )
        work.lock_row(
            LockRank.INGEST_FENCE,
            encode_lock_key("test-ingest"),
            "SELECT 1",
        )
        return coordinated

    def issue_authorized(work: Any, **kwargs: Any) -> object:
        assert kwargs["generation"] == 1
        work.lock_row(
            LockRank.WORKING_ROOT,
            encode_lock_key("test-publication"),
            "SELECT 1",
        )
        return object()

    def refence_from_gate(work: Any, **_kwargs: Any) -> object:
        # This is the pre-regression public path.  If orchestration calls it,
        # the real UoW checker raises MAINTENANCE_GATE-after-INGEST_FENCE.
        work.lock_row(
            LockRank.MAINTENANCE_GATE,
            encode_lock_key("test-gate-again"),
            "SELECT 1",
        )
        return object()

    monkeypatch.setattr(publication, "_require_policy", lambda _policy: None)
    monkeypatch.setattr(publication, "_load_root", lambda *_args: root)
    monkeypatch.setattr(publication, "_load_checkpoints", lambda *_args: states)
    monkeypatch.setattr(
        MaintenanceGateRepository,
        "resume",
        staticmethod(resume_gate),
    )
    monkeypatch.setattr(
        DownloadIngestRepository,
        "resume_ingest",
        staticmethod(resume_ingest),
    )
    monkeypatch.setattr(
        PublicationCandidateRepository,
        "_issue_projection_authority_authorized",
        staticmethod(issue_authorized),
    )
    monkeypatch.setattr(
        PublicationCandidateRepository,
        "issue_projection_authority",
        staticmethod(refence_from_gate),
    )

    machine = publication.VNextIngestPublication(
        _context(tmp_path / "catalog-issue-lock-order.sqlite3"),
        clock=lambda: 10,
    )
    issued = machine.issue_step(_session(), cast(Any, object()))

    assert issued.operation == "BUILD_CATALOG"


class _CanonicalStateConnector:
    def __init__(self, claim: tuple[object, ...] = ()) -> None:
        self.claim = claim

    def fetch_one(
        self,
        query: str,
        _parameters: object = (),
    ) -> tuple[object, ...]:
        assert "operational_canonical_value_uploads" in query
        return self.claim


def test_canonical_preimage_comparator_drains_empty_parts_at_exact_eof() -> None:
    comparator = publication._CanonicalPreimageComparator(iter((b"", b"")))

    comparator.finish()


def test_reused_sealed_canonical_requires_and_then_accepts_current_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"same complete canonical preimage"
    plan = CanonicalValueUploadPlan.from_parts("artifact_locator_bytes_v1", (payload,))
    connector = _CanonicalStateConnector()
    sealed = CanonicalValueReadReceipt(
        plan.value_sha256,
        plan.digest_domain,
        plan.byte_count,
        b"r" * 32,
    )
    comparisons: list[bytes] = []

    monkeypatch.setattr(
        publication,
        "load_sealed_value_identity",
        lambda *_args, **_kwargs: sealed,
    )

    def stream(
        _work: object,
        *,
        value_sha256: bytes,
        consume_provisional: Callable[[bytes], None],
    ) -> CanonicalValueReadReceipt:
        assert value_sha256 == plan.value_sha256
        consume_provisional(payload[:7])
        consume_provisional(payload[7:])
        comparisons.append(value_sha256)
        return sealed

    monkeypatch.setattr(
        CanonicalValueRepository,
        "stream_and_validate",
        staticmethod(stream),
    )
    try:
        first = publication._next_canonical_work(
            cast(Any, connector),
            backend="sqlite",
            session=_session(),
            plans=(plan,),
            owner=object(),
        )
        assert first is not None
        assert first[0].plan is plan
        assert first[1] is publication._Action.CANONICAL_ALLOCATE

        connector.claim = (_session().ingest_generation, plan.value_sha256)
        assert (
            publication._next_canonical_work(
                cast(Any, connector),
                backend="sqlite",
                session=_session(),
                plans=(plan,),
                owner=object(),
            )
            is None
        )
        assert comparisons == [plan.value_sha256, plan.value_sha256]
    finally:
        plan.close()


def test_canonical_claim_foreign_and_malformed_rows_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = CanonicalValueUploadPlan.from_parts("artifact_locator_bytes_v1", (b"x",))
    connector = _CanonicalStateConnector()
    monkeypatch.setattr(
        publication,
        "load_sealed_value_identity",
        lambda *_args, **_kwargs: None,
    )
    try:
        for claim in (
            (2, plan.value_sha256),
            (True, plan.value_sha256),
            (1, b"short"),
            (1, plan.value_sha256, b"extra"),
        ):
            connector.claim = claim
            with pytest.raises(RuntimeError, match="canonical upload claim"):
                publication._next_canonical_work(
                    cast(Any, connector),
                    backend="sqlite",
                    session=_session(),
                    plans=(plan,),
                    owner=object(),
                )
    finally:
        plan.close()


def test_fresh_canonical_keeps_allocate_page_and_seal_progression(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = CanonicalValueUploadPlan.from_parts("artifact_locator_bytes_v1", (b"new",))
    connector = _CanonicalStateConnector()
    family: object | None = None
    monkeypatch.setattr(
        publication,
        "load_sealed_value_identity",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        publication,
        "load_page_family",
        lambda *_args, **_kwargs: family,
    )
    try:
        allocated = publication._next_canonical_work(
            cast(Any, connector),
            backend="sqlite",
            session=_session(),
            plans=(plan,),
            owner=object(),
        )
        assert allocated is not None
        assert allocated[1] is publication._Action.CANONICAL_ALLOCATE

        connector.claim = (1, plan.value_sha256)
        paged = publication._next_canonical_work(
            cast(Any, connector),
            backend="sqlite",
            session=_session(),
            plans=(plan,),
            owner=object(),
        )
        assert paged is not None
        assert paged[1] is publication._Action.CANONICAL_PAGE
        assert paged[0].page is not None

        family = SimpleNamespace(page_bytes=paged[0].page.page_bytes)
        sealed = publication._next_canonical_work(
            cast(Any, connector),
            backend="sqlite",
            session=_session(),
            plans=(plan,),
            owner=object(),
        )
        assert sealed is not None
        assert sealed[1] is publication._Action.CANONICAL_SEAL

        family = SimpleNamespace(page_bytes=b"collision")
        with pytest.raises(RuntimeError, match="exact preimage"):
            publication._next_canonical_work(
                cast(Any, connector),
                backend="sqlite",
                session=_session(),
                plans=(plan,),
                owner=object(),
            )
    finally:
        plan.close()


def test_sealed_canonical_full_preimage_mismatch_rejects_even_exact_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"expected"
    plan = CanonicalValueUploadPlan.from_parts("artifact_locator_bytes_v1", (payload,))
    connector = _CanonicalStateConnector((1, plan.value_sha256))
    sealed = CanonicalValueReadReceipt(
        plan.value_sha256,
        plan.digest_domain,
        plan.byte_count,
        b"r" * 32,
    )
    monkeypatch.setattr(
        publication,
        "load_sealed_value_identity",
        lambda *_args, **_kwargs: sealed,
    )

    def collide(
        _work: object,
        *,
        value_sha256: bytes,
        consume_provisional: Callable[[bytes], None],
    ) -> CanonicalValueReadReceipt:
        assert value_sha256 == plan.value_sha256
        consume_provisional(b"differen")
        return sealed

    monkeypatch.setattr(
        CanonicalValueRepository,
        "stream_and_validate",
        staticmethod(collide),
    )
    try:
        with pytest.raises(RuntimeError, match="exact preimage"):
            publication._next_canonical_work(
                cast(Any, connector),
                backend="sqlite",
                session=_session(),
                plans=(plan,),
                owner=object(),
            )
    finally:
        plan.close()


def test_partial_sealed_canonical_family_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = CanonicalValueUploadPlan.from_parts("artifact_locator_bytes_v1", (b"x",))
    connector = _CanonicalStateConnector()

    def partial(*_args: object, **_kwargs: object) -> None:
        raise CanonicalValuePartialFamilyError("injected partial identity")

    monkeypatch.setattr(publication, "load_sealed_value_identity", partial)
    try:
        with pytest.raises(RuntimeError, match="partial or corrupt"):
            publication._next_canonical_work(
                cast(Any, connector),
                backend="sqlite",
                session=_session(),
                plans=(plan,),
                owner=object(),
            )
    finally:
        plan.close()


def test_zero_artifact_candidate_binds_effect_seal_before_stage_eight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    preparation_id = b"p" * 16
    seal = SimpleNamespace(preparation_id=preparation_id)

    class Connector:
        bound = False

        def fetch_one(self, query: str, _parameters: object = ()) -> tuple[object, ...]:
            if "preparation_checkpoints" in query:
                return (preparation_id, "COMPLETE", "COMPLETE")
            if "catalog_artifact_operations" in query:
                return ()
            if "publication_candidate_preparations" in query:
                return (preparation_id,) if self.bound else ()
            raise AssertionError(query)

    connector = Connector()
    work = cast(Any, SimpleNamespace(connector=connector))
    monkeypatch.setattr(
        OperationalEffectRepository,
        "_load_complete_seal_authorized",
        staticmethod(lambda *_args, **_kwargs: seal),
    )

    action = publication._issue_artifact_or_operational(
        work,
        generation=1,
        candidate_id=b"c" * 16,
        build_id=b"b" * 16,
        operational_policy_id=1,
        now=10,
    )
    assert action is not None and action[0] is publication._Action.BIND_OPERATIONAL

    connector.bound = True
    assert (
        publication._issue_artifact_or_operational(
            work,
            generation=1,
            candidate_id=b"c" * 16,
            build_id=b"b" * 16,
            operational_policy_id=1,
            now=10,
        )
        is None
    )


def test_operational_effect_planner_enforces_128_row_cap() -> None:
    class Connector:
        def fetch_one(self, query: str, _parameters: object = ()) -> tuple[object, ...]:
            if "max_batch_rows" in query:
                return (999, 4, 4)
            if "request_token FROM" in query:
                return ()
            raise AssertionError(query)

        def fetch_all(
            self,
            query: str,
            _parameters: object = (),
        ) -> list[tuple[int]]:
            if "operational_removed_gids" in query:
                return [(gid,) for gid in range(1, 129)]
            if "deletion_request_heads" in query:
                raise AssertionError("full removed page must defer deletion rows")
            raise AssertionError(query)

    work = cast(Any, SimpleNamespace(connector=Connector()))
    effects = publication._derive_operational_effects(work, b"p" * 16)
    assert len(effects) == 128
    assert all(isinstance(effect, RemovedGid) for effect in effects)


def test_checkpoint_loader_accepts_only_sixteen_candidate_owned_stages() -> None:
    class Connector:
        def __init__(self) -> None:
            self.rows = [(stage, "COMPLETE") for stage in publication._CANDIDATE_STAGES]

        def fetch_all(
            self,
            _query: str,
            _parameters: object = (),
        ) -> list[tuple[bytes, str]]:
            return self.rows

    connector = Connector()
    expected = tuple(connector.rows)
    assert publication._load_checkpoints(cast(Any, connector), b"c" * 16) == expected

    connector.rows.append((b"FINALIZE_ARTIFACTS", "OPEN"))
    with pytest.raises(RuntimeError, match="incomplete or reordered"):
        publication._load_checkpoints(cast(Any, connector), b"c" * 16)


def test_genesis_route_covers_sixteen_candidate_stages_then_commits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = b"c" * 16
    root = publication._Root(b"b" * 16, b"a" * 16, 1, candidate, None, None, None)
    policy = cast(
        Any,
        SimpleNamespace(operational_policy_id=1),
    )
    turn = IngestTurn(1, b"i" * 16, 100)
    connector = SimpleNamespace()
    work = cast(Any, SimpleNamespace(connector=connector))
    monkeypatch.setattr(
        PublicationCandidateRepository,
        "_issue_projection_authority_authorized",
        staticmethod(lambda *_args, **_kwargs: object()),
    )
    monkeypatch.setattr(
        ArtifactPreparationRepository,
        "_issue_input_projection_authority_authorized",
        staticmethod(lambda *_args, **_kwargs: object()),
    )
    monkeypatch.setattr(
        publication,
        "_issue_artifact_or_operational",
        lambda *_args, **_kwargs: None,
    )
    expected = (
        publication._Action.BUILD_SELECTION,
        publication._Action.VALIDATE_SELECTION,
        publication._Action.BUILD_CATALOG,
        publication._Action.VALIDATE_CATALOG,
        publication._Action.BUILD_ARTIFACT_INPUT,
        publication._Action.BUILD_ARTIFACT_DELTA,
        publication._Action.VALIDATE_ARTIFACT_INPUT,
        publication._Action.VALIDATE_PREPARED,
        publication._Action.VALIDATE_CREATE,
        publication._Action.VALIDATE_REBUILD,
        publication._Action.VALIDATE_DELETE,
        publication._Action.VALIDATE_UNCHANGED,
        publication._Action.VALIDATE_NEW,
        publication._Action.VALIDATE_CHANGED,
        publication._Action.VALIDATE_REMOVED,
        publication._Action.VALIDATE_DUPLICATE,
    )
    for index, expected_action in enumerate(expected):
        states = tuple(
            (stage, "COMPLETE" if position < index else "OPEN")
            for position, stage in enumerate(publication._CANDIDATE_STAGES)
        )
        monkeypatch.setattr(
            publication, "_load_checkpoints", lambda *_args, states=states: states
        )
        action, _payload = publication._issue_database_action(
            work,
            generation=turn.generation,
            root=root,
            policy=policy,
            now=10,
        )
        assert action is expected_action

    terminal_states = tuple(
        (stage, "COMPLETE") for stage in publication._CANDIDATE_STAGES
    )
    monkeypatch.setattr(
        publication,
        "_load_checkpoints",
        lambda *_args: terminal_states,
    )
    action, payload = publication._issue_database_action(
        work,
        generation=turn.generation,
        root=root,
        policy=policy,
        now=10,
    )
    assert action is publication._Action.COMMIT_PUBLICATION
    assert payload == candidate


def test_every_candidate_mapping_dispatches_work_as_a_keyword(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _session()
    gate, coordinated = publication._repository_authority(session)
    work = cast(Any, object())
    payload = object()

    for action in tuple(publication._CANDIDATE_METHODS):
        seen: dict[str, object] = {}

        def keyword_only_method(**kwargs: object) -> object:
            seen.update(kwargs)
            return action

        monkeypatch.setitem(
            publication._CANDIDATE_METHODS,
            action,
            keyword_only_method,
        )
        outcome = publication._commit_action(
            work,
            action=action,
            payload=publication._CandidateWork(
                b"c" * 16,
                b"batch",
                payload,
            ),
            session=session,
            gate=gate,
            turn=coordinated.ingest_turn,
            now=10,
        )

        assert outcome is action
        assert seen["work"] is work
        assert seen["candidate_id"] == b"c" * 16
        assert seen["batch_key"] == b"batch"
        assert seen["now"] == 10
        assert seen["gate_lease"] is gate
        assert seen["ingest_turn"] is coordinated.ingest_turn
        if action in {
            publication._Action.BUILD_CATALOG,
            publication._Action.BUILD_ARTIFACT_INPUT,
        }:
            assert seen["plan"] is payload
        elif action in {
            publication._Action.VALIDATE_CATALOG,
            publication._Action.VALIDATE_ARTIFACT_INPUT,
        }:
            assert seen["validation"] is payload


def test_restart_after_storage_protect_reissues_same_durable_intent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Receipt:
        locator_plan = object()

        def close(self) -> None:
            pass

    authority = cast(Any, SimpleNamespace(storage_codec=(1, b"store")))
    seal = cast(Any, SimpleNamespace(preparation_id=b"p" * 16))
    intent = cast(Any, SimpleNamespace(protection_token=b"t" * 184))
    evidence = cast(Any, SimpleNamespace(intent=intent))
    adapter = cast(Any, SimpleNamespace())
    protected: list[object] = []

    monkeypatch.setattr(
        ArtifactPreparationRepository,
        "audit_inputs",
        staticmethod(lambda *_args, **_kwargs: object()),
    )
    monkeypatch.setattr(
        publication,
        "_prepare_archive_source_plan",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        publication,
        "_render_prepared_artifact",
        lambda *_args, **_kwargs: Receipt(),
    )

    def protect(_receipt: object, durable: object, _adapter: object) -> object:
        protected.append(durable)
        return evidence

    monkeypatch.setattr(
        publication,
        "_protect_prepared_artifact_local",
        protect,
    )
    machine = publication.VNextIngestPublication(
        _context(tmp_path / "artifact-restart.sqlite3"),
        clock=lambda: 10,
    )
    work = publication._ArtifactWork(authority, seal, intent)

    first = cast(Any, machine)._VNextIngestPublication__prepare_artifact(
        work, {b"store": adapter}
    )
    # Simulate process loss after protect and before confirm; the durable
    # PENDING intent is reconstructed and protect is idempotently retried.
    second = cast(Any, machine)._VNextIngestPublication__prepare_artifact(
        work, {b"store": adapter}
    )

    assert protected == [intent, intent]
    cast(publication._ArtifactPrepared, first).receipt.close()
    cast(publication._ArtifactPrepared, second).receipt.close()


def test_genesis_one_gallery_vertical_flow_holds_one_outer_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "vertical.sqlite3"
    receipt_id = b"r" * 16
    candidate_id = b"c" * 16
    artifact_sha256 = b"z" * 32
    key = publication_key(1)
    components = artifact_locator_components(artifact_sha256)
    locator_sha256 = artifact_locator_digest(components)
    token = encode_artifact_protection_token(
        1,
        candidate_id,
        key,
        artifact_sha256,
        locator_sha256,
        1,
        101,
    )
    finalization_item = PublicationFinalizationItem(
        candidate_id,
        key,
        artifact_sha256,
        locator_sha256,
        components,
        101,
        1,
        1,
        token,
        b"store",
        "PREPARED",
    )
    projection_item = CurrentProjectionArtifactItem(
        key,
        1,
        "gallery-1",
        10,
        components,
        artifact_sha256,
        101,
    )
    state = {"finalized": False, "finalization_issues": 0}
    root = publication._Root(
        b"b" * 16,
        b"a" * 16,
        1,
        None,
        receipt_id,
        3,
        "DB_COMMITTED",
    )
    session = _session()
    checkpoint = publication.CurrentProjectionCheckpoint(
        3,
        receipt_id,
        publication.CurrentProjectionStatus.SPOOL,
        None,
    )
    adapter = _ProjectionAdapter(checkpoint)

    class ReleaseAdapter:
        adapter_id = b"store"

        def __init__(self) -> None:
            self.tokens: list[bytes] = []

        def release(
            self,
            locator_components: tuple[str, ...],
            protection_token: bytes,
        ) -> ArtifactReleaseStorageEvidence:
            assert adapter.guard_depth == 1
            assert locator_components == components
            _probe_begin_immediate(database_path)
            self.tokens.append(protection_token)
            return ArtifactReleaseStorageEvidence(True)

    releaser = ReleaseAdapter()
    machine = publication.VNextIngestPublication(
        _context(database_path),
        clock=lambda: 10,
    )
    monkeypatch.setattr(publication, "_require_policy", lambda _policy: None)
    monkeypatch.setattr(
        publication,
        "_resume_authority",
        lambda _work, current, _now: current,
    )
    monkeypatch.setattr(publication, "_load_root", lambda *_args: root)

    def issue_database_action(*_args: object, **_kwargs: object) -> tuple[Any, Any]:
        if state["finalized"]:
            return publication._Action.COMPLETE, root
        return publication._Action.CURRENT_PROJECTION, publication._ProjectionWork(
            receipt_id,
            3,
        )

    monkeypatch.setattr(
        publication,
        "_issue_database_action",
        issue_database_action,
    )

    def list_page(_connector: object, **kwargs: Any) -> CurrentProjectionArtifactPage:
        assert kwargs["page_limit"] == 128
        if kwargs["cursor"] is None:
            return CurrentProjectionArtifactPage(
                receipt_id,
                3,
                (projection_item,),
                key,
                False,
            )
        assert kwargs["cursor"] == key
        return CurrentProjectionArtifactPage(receipt_id, 3, (), None, True)

    monkeypatch.setattr(
        CurrentProjectionArtifactRepository,
        "list_page",
        staticmethod(list_page),
    )

    def issue_finalization(
        _connector: object, **kwargs: Any
    ) -> PublicationFinalizationPage:
        issue = state["finalization_issues"]
        state["finalization_issues"] = issue + 1
        gate = kwargs["gate_lease"]
        if issue == 0:
            return PublicationFinalizationPage(
                gate,
                receipt_id,
                candidate_id,
                b"1" * 32,
                1,
                b"",
                0,
                5,
                5,
                1,
                128,
                (finalization_item,),
                key,
                False,
                _PAGE_CAPABILITY,
            )
        return PublicationFinalizationPage(
            gate,
            receipt_id,
            candidate_id,
            b"2" * 32,
            2,
            key,
            1,
            6,
            5,
            1,
            128,
            (),
            key,
            True,
            _PAGE_CAPABILITY,
        )

    monkeypatch.setattr(
        PublicationFinalizationRepository,
        "issue_page",
        staticmethod(issue_finalization),
    )

    def commit_action(*_args: object, **kwargs: Any) -> object:
        if kwargs["action"] is publication._Action.FINALIZE:
            acknowledgement = kwargs["payload"]
            page = acknowledgement.page
            if page.terminal:
                state["finalized"] = True
            return SimpleNamespace(
                row_count=len(page.items),
                terminal=page.terminal,
                replayed=False,
            )
        return kwargs["payload"]

    monkeypatch.setattr(publication, "_commit_action", commit_action)

    operations: list[str] = []
    with adapter.publication_guard():
        for _turn in range(16):
            issued = machine.issue_step(session, cast(Any, object()))
            operations.append(issued.operation)
            prepared = machine.prepare_step(
                issued,
                artifact_adapters={},
                finalization_adapters={b"store": releaser},
                current_projection=adapter,
            )
            result = machine.commit_step(session, prepared)
            if result.terminal:
                break
        else:  # pragma: no cover - fail-closed diagnostic
            raise AssertionError("vertical publication flow did not terminate")
        operations.append(machine.issue_step(session, cast(Any, object())).operation)

    assert operations.count("CURRENT_PROJECTION") == 6
    assert operations.count("FINALIZE") == 2
    assert operations[-1] == "COMPLETE"
    assert releaser.tokens == [token]
    assert adapter.reconciled is True
    assert type(adapter.appended[0][0]) is VNextCurrentProjectionItem
