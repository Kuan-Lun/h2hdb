from __future__ import annotations

import inspect
import sqlite3
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
import test_vnext_publication_repository as publication_fixtures

import h2hdb
import h2hdb.vnext_ingest_publication as publication
from h2hdb.config_loader import CoreConfig, DatabaseConfig
from h2hdb.domain import (
    ArtifactReleaseStorageEvidence,
    CatalogResourceKind,
    StorageObjectDescriptor,
    StorageObjectKey,
    VNextIngestSession,
    VNextLibraryActivationCursor,
    VNextLibraryActivationItem,
)
from h2hdb.repository import RepositoryContext
from h2hdb.vnext_artifact_preparation_repository import ArtifactPreparationRepository
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValuePartialFamilyError,
    CanonicalValueReadReceipt,
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_download_ingest_repository import DownloadIngestRepository
from h2hdb.vnext_identity import (
    artifact_storage_key_digest,
    encode_artifact_protection_token,
    publication_key,
)
from h2hdb.vnext_ingest_fence_repository import IngestTurn
from h2hdb.vnext_library_activation_repository import (
    LibraryActivationResourcePage,
    LibraryActivationResourceRepository,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
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
from h2hdb.vnext_publication_repository import PublicationRepository
from h2hdb.vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key


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
        "LibraryActivationCheckpoint",
        "LibraryActivationStatus",
        "VNextLibraryActivationAdapter",
        "VNextIssuedPublicationStep",
        "VNextPreparedPublicationStep",
    } <= set(h2hdb.__all__)
    assert "VNextIngestPublication" not in h2hdb.__all__
    assert h2hdb.LibraryActivationCheckpoint is publication.LibraryActivationCheckpoint
    assert h2hdb.LibraryActivationStatus is publication.LibraryActivationStatus
    assert h2hdb.VNextLibraryActivationAdapter is (
        publication.VNextLibraryActivationAdapter
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
        "library_activation",
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


class _LibraryActivationAdapter:
    def __init__(
        self,
        checkpoint: publication.LibraryActivationCheckpoint,
        *,
        callback: Callable[[], None] | None = None,
    ) -> None:
        self.checkpoint = checkpoint
        self.appended: list[tuple[VNextLibraryActivationItem, ...]] = []
        self.sealed = False
        self.reconciled = False
        self.completed = False
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
    ) -> publication.LibraryActivationCheckpoint:
        self._called()
        assert revision == self.checkpoint.revision
        assert receipt_id == self.checkpoint.receipt_id
        return self.checkpoint

    def activate_page(
        self,
        revision: int,
        items: Sequence[VNextLibraryActivationItem],
    ) -> None:
        self._called()
        assert revision == self.checkpoint.revision
        self.appended.append(tuple(items))
        if items:
            self.checkpoint = publication.LibraryActivationCheckpoint(
                revision,
                self.checkpoint.receipt_id,
                publication.LibraryActivationStatus.SPOOL,
                VNextLibraryActivationCursor(
                    items[-1].publication_key,
                    items[-1].resource_kind,
                ),
            )

    def seal(self, revision: int) -> None:
        self._called()
        assert revision == self.checkpoint.revision
        self.sealed = True
        self.checkpoint = publication.LibraryActivationCheckpoint(
            revision,
            self.checkpoint.receipt_id,
            publication.LibraryActivationStatus.RECONCILE,
            None,
        )

    def reconcile_page(
        self,
        revision: int,
        receipt_id: bytes,
        *,
        limit: int,
    ) -> publication.LibraryActivationCheckpoint:
        self._called()
        assert revision == self.checkpoint.revision
        assert receipt_id == self.checkpoint.receipt_id
        assert limit == 128
        self.reconciled = True
        self.checkpoint = publication.LibraryActivationCheckpoint(
            revision,
            self.checkpoint.receipt_id,
            publication.LibraryActivationStatus.READY,
            None,
        )
        return self.checkpoint

    def complete(self, revision: int, receipt_id: bytes) -> None:
        self._called()
        assert revision == self.checkpoint.revision
        assert receipt_id == self.checkpoint.receipt_id
        assert self.checkpoint.status in {
            publication.LibraryActivationStatus.READY,
            publication.LibraryActivationStatus.COMPLETE,
        }
        self.completed = True
        self.checkpoint = publication.LibraryActivationCheckpoint(
            revision,
            receipt_id,
            publication.LibraryActivationStatus.COMPLETE,
            None,
        )


def _storage_object(
    gid: int,
    resource_kind: CatalogResourceKind = CatalogResourceKind.ACQUISITION,
) -> StorageObjectDescriptor:
    return StorageObjectDescriptor(
        StorageObjectKey(
            "test-storage-v2",
            (f"{gid:016x}", resource_kind.value),
        ),
        100 + gid,
        sha256(f"{gid}:{resource_kind.value}".encode()).hexdigest(),
        datetime(2025, 1, 1, tzinfo=UTC),
    )


def _library_activation_item(
    gid: int,
    resource_kind: CatalogResourceKind = CatalogResourceKind.ACQUISITION,
) -> VNextLibraryActivationItem:
    return VNextLibraryActivationItem(
        publication_key(gid),
        gid,
        resource_kind,
        _storage_object(gid, resource_kind),
    )


def _activation_cursor(
    gid: int,
    resource_kind: CatalogResourceKind = CatalogResourceKind.ACQUISITION,
) -> VNextLibraryActivationCursor:
    return VNextLibraryActivationCursor(publication_key(gid), resource_kind)


def test_checkpoint_is_receipt_scoped_and_cursor_is_spool_only() -> None:
    receipt = b"r" * 16
    cursor = _activation_cursor(1)
    checkpoint = publication.LibraryActivationCheckpoint(
        3,
        receipt,
        publication.LibraryActivationStatus.SPOOL,
        cursor,
    )
    assert checkpoint.cursor == cursor

    with pytest.raises(ValueError, match="terminal activation checkpoint"):
        publication.LibraryActivationCheckpoint(
            3,
            receipt,
            publication.LibraryActivationStatus.COMPLETE,
            cursor,
        )
    with pytest.raises(ValueError, match="another receipt"):
        publication._require_library_activation_checkpoint(
            checkpoint,
            revision=3,
            receipt_id=b"x" * 16,
        )


def test_library_activation_maps_repository_items_to_the_shared_neutral_type(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = b"r" * 16
    checkpoint = publication.LibraryActivationCheckpoint(
        3,
        receipt,
        publication.LibraryActivationStatus.SPOOL,
        None,
    )
    adapter = _LibraryActivationAdapter(checkpoint)
    page = LibraryActivationResourcePage(
        receipt,
        3,
        (_library_activation_item(1),),
        _activation_cursor(1),
        False,
    )
    calls: list[tuple[VNextLibraryActivationCursor | None, int]] = []

    def list_page(_connector: object, **kwargs: Any) -> LibraryActivationResourcePage:
        calls.append((kwargs["cursor"], kwargs["page_limit"]))
        return page

    monkeypatch.setattr(
        LibraryActivationResourceRepository,
        "list_page",
        staticmethod(list_page),
    )
    machine = publication.VNextIngestPublication(
        _context(tmp_path / "projection.sqlite3")
    )
    with adapter.publication_guard():
        prepared = cast(
            Any, machine
        )._VNextIngestPublication__prepare_library_activation(
            publication._LibraryActivationWork(receipt, 3, checkpoint), adapter
        )

    assert prepared.processed_rows == 1
    assert prepared.terminal_page is False
    assert calls == [(None, 128)]
    assert len(adapter.appended) == 1
    assert type(adapter.appended[0][0]) is VNextLibraryActivationItem


def test_restart_after_database_commit_uses_adapter_owned_cursor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = b"r" * 16
    cursor = _activation_cursor(1)
    checkpoint = publication.LibraryActivationCheckpoint(
        3,
        receipt,
        publication.LibraryActivationStatus.SPOOL,
        cursor,
    )
    adapter = _LibraryActivationAdapter(checkpoint)
    observed: list[VNextLibraryActivationCursor | None] = []

    def list_page(_connector: object, **kwargs: Any) -> LibraryActivationResourcePage:
        observed.append(kwargs["cursor"])
        return LibraryActivationResourcePage(receipt, 3, (), None, True)

    monkeypatch.setattr(
        LibraryActivationResourceRepository,
        "list_page",
        staticmethod(list_page),
    )
    restarted = publication.VNextIngestPublication(
        _context(tmp_path / "restart.sqlite3")
    )
    with adapter.publication_guard():
        result = cast(
            Any, restarted
        )._VNextIngestPublication__prepare_library_activation(
            publication._LibraryActivationWork(receipt, 3, checkpoint), adapter
        )

    assert observed == [cursor]
    assert result.terminal_page is True
    assert adapter.appended == [()]
    assert adapter.sealed is True


def test_library_activation_page_hard_cap_is_always_128(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = b"r" * 16
    checkpoint = publication.LibraryActivationCheckpoint(
        3,
        receipt,
        publication.LibraryActivationStatus.SPOOL,
        None,
    )
    adapter = _LibraryActivationAdapter(checkpoint)
    items = tuple(
        sorted(
            (
                item
                for gid in range(1, 65)
                for item in (
                    _library_activation_item(gid, CatalogResourceKind.ACQUISITION),
                    _library_activation_item(gid, CatalogResourceKind.THUMBNAIL),
                )
            ),
            key=lambda item: (item.publication_key, item.resource_kind.value),
        )
    )
    page = LibraryActivationResourcePage(
        receipt,
        3,
        items,
        VNextLibraryActivationCursor(
            items[-1].publication_key,
            items[-1].resource_kind,
        ),
        False,
    )

    def list_page(_connector: object, **kwargs: Any) -> LibraryActivationResourcePage:
        assert kwargs["page_limit"] == 128
        return page

    monkeypatch.setattr(
        LibraryActivationResourceRepository,
        "list_page",
        staticmethod(list_page),
    )
    machine = publication.VNextIngestPublication(_context(tmp_path / "cap.sqlite3"))
    with adapter.publication_guard():
        result = cast(Any, machine)._VNextIngestPublication__prepare_library_activation(
            publication._LibraryActivationWork(receipt, 3, checkpoint), adapter
        )
    assert result.processed_rows == 128
    assert len(adapter.appended[0]) == 128


def test_library_activation_adapter_io_runs_under_only_the_callers_outer_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "projection-boundary.sqlite3"
    sqlite3.connect(path).close()
    receipt = b"r" * 16
    checkpoint = publication.LibraryActivationCheckpoint(
        3,
        receipt,
        publication.LibraryActivationStatus.SPOOL,
        None,
    )
    probes: list[str] = []

    def probe() -> None:
        _probe_begin_immediate(path)
        probes.append("adapter")

    adapter = _LibraryActivationAdapter(checkpoint, callback=probe)

    def list_page(connector: Any, **_kwargs: Any) -> LibraryActivationResourcePage:
        with connector.read_transaction():
            assert connector.fetch_one("SELECT 1") == (1,)
        return LibraryActivationResourcePage(receipt, 3, (), None, True)

    monkeypatch.setattr(
        LibraryActivationResourceRepository,
        "list_page",
        staticmethod(list_page),
    )
    machine = publication.VNextIngestPublication(_context(path))
    with adapter.publication_guard():
        result = cast(Any, machine)._VNextIngestPublication__prepare_library_activation(
            publication._LibraryActivationWork(receipt, 3, checkpoint), adapter
        )
        with pytest.raises(RuntimeError, match="nested publication guard"):
            with adapter.publication_guard():
                pass

    assert result.terminal_page is True
    assert probes == ["adapter", "adapter", "adapter"]
    assert (
        "library_activation"
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
    root = publication._Root(
        b"b" * 16,
        b"a" * 16,
        1,
        None,
        b"r" * 16,
        1,
        "PUBLISHED",
    )
    issued = publication.VNextIssuedPublicationStep(
        action=publication._Action.COMPLETE,
        payload=root,
        session=original,
        _token=publication._STEP_TOKEN,
    )
    machine = publication.VNextIngestPublication(_context(tmp_path / "session.sqlite3"))
    adapter = _LibraryActivationAdapter(
        publication.LibraryActivationCheckpoint(
            1,
            b"r" * 16,
            publication.LibraryActivationStatus.COMPLETE,
            None,
        )
    )
    monkeypatch.setattr(
        publication,
        "_commit_action",
        lambda *_args, **_kwargs: object(),
    )

    with adapter.publication_guard():
        prepared = machine.prepare_step(
            issued,
            artifact_adapters={},
            finalization_adapters={},
            library_activation=adapter,
        )
    machine.commit_step(renewed, prepared)
    with adapter.publication_guard():
        foreign_prepared = machine.prepare_step(
            issued,
            artifact_adapters={},
            finalization_adapters={},
            library_activation=adapter,
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
    root = publication._Root(
        b"b" * 16,
        b"a" * 16,
        1,
        None,
        b"r" * 16,
        1,
        "PUBLISHED",
    )
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
    checkpoint = publication.LibraryActivationCheckpoint(
        1,
        b"r" * 16,
        publication.LibraryActivationStatus.COMPLETE,
        None,
    )
    adapter = _LibraryActivationAdapter(checkpoint)

    issued = machine.issue_step(session, cast(Any, object()))
    assert begins == ["BEGIN IMMEDIATE"]
    with adapter.publication_guard():
        prepared = machine.prepare_step(
            issued,
            artifact_adapters={},
            finalization_adapters={},
            library_activation=adapter,
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
    plan = CanonicalValueUploadPlan.from_parts("storage_object_key_v2", (payload,))
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
    plan = CanonicalValueUploadPlan.from_parts("storage_object_key_v2", (b"x",))
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
    plan = CanonicalValueUploadPlan.from_parts("storage_object_key_v2", (b"new",))
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
    plan = CanonicalValueUploadPlan.from_parts("storage_object_key_v2", (payload,))
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
    plan = CanonicalValueUploadPlan.from_parts("storage_object_key_v2", (b"x",))
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

    authority = cast(Any, SimpleNamespace(adapter_id=b"store"))
    seal = cast(Any, SimpleNamespace(preparation_id=b"p" * 16))
    intent = cast(Any, SimpleNamespace(protection_token=b"t" * 32))
    evidence = cast(Any, SimpleNamespace(intent=intent))
    family = cast(Any, SimpleNamespace(resource_kind=CatalogResourceKind.ACQUISITION))
    adapter = cast(Any, SimpleNamespace())
    protected: list[object] = []

    monkeypatch.setattr(
        ArtifactPreparationRepository,
        "audit_inputs",
        staticmethod(lambda *_args, **_kwargs: object()),
    )
    monkeypatch.setattr(
        ArtifactPreparationRepository,
        "prepare_with_storage_adapter",
        staticmethod(lambda *_args, **_kwargs: Receipt()),
    )
    monkeypatch.setattr(
        publication,
        "_protection_intent_from_family",
        lambda *_args, **_kwargs: intent,
    )

    def protect(*_args: object, **kwargs: object) -> object:
        protected.append(kwargs["intent"])
        return evidence

    monkeypatch.setattr(
        ArtifactPreparationRepository,
        "protect_prepared_artifact",
        staticmethod(protect),
    )
    machine = publication.VNextIngestPublication(
        _context(tmp_path / "artifact-restart.sqlite3"),
        clock=lambda: 10,
    )
    work = publication._ArtifactWork(authority, seal, (family,))

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


def test_terminal_head_activation_continues_to_complete_library_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "vertical.sqlite3"
    receipt_id = b"r" * 16
    candidate_id = b"c" * 16
    key = publication_key(1)
    descriptors = {
        kind: _storage_object(1, kind)
        for kind in (
            CatalogResourceKind.ACQUISITION,
            CatalogResourceKind.THUMBNAIL,
        )
    }
    key_digests = {
        kind: artifact_storage_key_digest(
            descriptor.key.codec,
            descriptor.key.segments,
        )
        for kind, descriptor in descriptors.items()
    }
    tokens = {
        kind: encode_artifact_protection_token(
            candidate_id,
            key,
            kind.value,
            key_digests[kind],
            1,
        )
        for kind in descriptors
    }
    finalization_items = tuple(
        PublicationFinalizationItem(
            candidate_id,
            key,
            kind,
            key_digests[kind],
            descriptors[kind],
            1,
            tokens[kind],
            b"store",
            "PREPARED",
        )
        for kind in descriptors
    )
    activation_items = tuple(
        VNextLibraryActivationItem(
            key,
            1,
            kind,
            descriptors[kind],
        )
        for kind in descriptors
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
    checkpoint = publication.LibraryActivationCheckpoint(
        3,
        receipt_id,
        publication.LibraryActivationStatus.SPOOL,
        None,
    )
    adapter = _LibraryActivationAdapter(checkpoint)

    class ReleaseAdapter:
        adapter_id = b"store"

        def __init__(self) -> None:
            self.tokens: list[bytes] = []

        def release(
            self,
            exact_storage_key: StorageObjectKey,
            expected_sha256: bytes,
            expected_size_bytes: int,
            protection_token: bytes,
        ) -> ArtifactReleaseStorageEvidence:
            assert adapter.guard_depth == 1
            kind = next(
                resource_kind
                for resource_kind, descriptor in descriptors.items()
                if descriptor.key == exact_storage_key
            )
            descriptor = descriptors[kind]
            assert expected_sha256 == bytes.fromhex(descriptor.sha256)
            assert expected_size_bytes == descriptor.size_bytes
            assert protection_token == tokens[kind]
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
        return (
            publication._Action.LIBRARY_ACTIVATION,
            publication._LibraryActivationWork(
                receipt_id,
                3,
            ),
        )

    monkeypatch.setattr(
        publication,
        "_issue_database_action",
        issue_database_action,
    )

    final_cursor = VNextLibraryActivationCursor(
        key,
        CatalogResourceKind.THUMBNAIL,
    )

    def list_page(_connector: object, **kwargs: Any) -> LibraryActivationResourcePage:
        assert kwargs["page_limit"] == 128
        if kwargs["cursor"] is None:
            return LibraryActivationResourcePage(
                receipt_id,
                3,
                activation_items,
                final_cursor,
                False,
            )
        assert kwargs["cursor"] == final_cursor
        return LibraryActivationResourcePage(receipt_id, 3, (), None, True)

    monkeypatch.setattr(
        LibraryActivationResourceRepository,
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
                128,
                finalization_items,
                final_cursor.to_bytes(),
                False,
                _PAGE_CAPABILITY,
            )
        return PublicationFinalizationPage(
            gate,
            receipt_id,
            candidate_id,
            b"2" * 32,
            2,
            final_cursor.to_bytes(),
            2,
            6,
            5,
            128,
            (),
            final_cursor.to_bytes(),
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
                library_activation=adapter,
            )
            result = machine.commit_step(session, prepared)
            if result.terminal:
                break
        else:  # pragma: no cover - fail-closed diagnostic
            raise AssertionError("vertical publication flow did not terminate")
    assert operations.count("LIBRARY_ACTIVATION") == 5
    assert operations.count("FINALIZE") == 2
    assert operations[-1] == "COMPLETE"
    assert releaser.tokens == [
        tokens[CatalogResourceKind.ACQUISITION],
        tokens[CatalogResourceKind.THUMBNAIL],
    ]
    assert adapter.reconciled is True
    assert adapter.completed is True
    assert tuple(type(item) for item in adapter.appended[0]) == (
        VNextLibraryActivationItem,
        VNextLibraryActivationItem,
    )


def test_complete_persisted_before_response_loss_replays_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recover COMPLETE from the adapter without repeating terminal effects."""

    database_path = tmp_path / "complete-response-loss.sqlite3"
    connector = publication_fixtures._generated_database(database_path)
    gate, first_turn = publication_fixtures._authorities(connector)
    publication_fixtures._seed_candidate(connector, first_turn)
    published, replay_turn = publication_fixtures._prepare_finalized_replay(
        connector,
        gate,
        first_turn,
    )
    build_id = publication_fixtures._BUILD
    receipt_id = published.receipt_id
    root = publication._Root(
        build_id,
        publication_fixtures._ANALYSIS,
        1,
        None,
        receipt_id,
        published.revision,
        "PUBLISHED",
    )
    session = VNextIngestSession(
        gate.owner_token,
        gate.gate_generation,
        gate.slots[0],
        gate.lease_expires_at,
        replay_turn.generation,
        replay_turn.owner_token,
        replay_turn.lease_expires_at,
        None,
        None,
        None,
        None,
    )
    policy = cast(Any, object())
    adapter_journal = {"present": True}
    finalization_calls: list[str] = []
    cleanup_calls: list[tuple[bytes, bytes]] = []
    original_cleanup = PublicationRepository.release_replayed_source_working
    assert connector.fetch_one(
        "SELECT build_id FROM operational_source_working_builds WHERE slot = 1"
    ) == (build_id,)
    original_head = connector.fetch_one(
        "SELECT receipt_id, revision, generation "
        "FROM catalog_publication_commit_heads WHERE channel = %s",
        (publication_fixtures._CHANNEL,),
    )
    connector.close()

    class PersistThenLoseResponseAdapter(_LibraryActivationAdapter):
        def __init__(self) -> None:
            super().__init__(
                publication.LibraryActivationCheckpoint(
                    published.revision,
                    receipt_id,
                    publication.LibraryActivationStatus.READY,
                    None,
                )
            )
            self.complete_calls = 0

        def complete(self, revision: int, exact_receipt_id: bytes) -> None:
            self.complete_calls += 1
            super().complete(revision, exact_receipt_id)
            # The adapter transaction committed COMPLETE and removed its journal,
            # but its acknowledgement never reached the orchestrator.
            adapter_journal["present"] = False
            if self.complete_calls == 1:
                raise ConnectionError("lost COMPLETE acknowledgement")

    adapter = PersistThenLoseResponseAdapter()
    monkeypatch.setattr(publication, "_require_policy", lambda _policy: None)
    monkeypatch.setattr(
        publication,
        "_resume_authority",
        lambda _work, current, _now: current.ingest_generation,
    )
    monkeypatch.setattr(publication, "_load_root", lambda *_args: root)

    def forbid_finalization(*_args: object, **_kwargs: object) -> object:
        finalization_calls.append("forbidden")
        raise AssertionError("published COMPLETE replay attempted finalization")

    monkeypatch.setattr(
        PublicationFinalizationRepository,
        "issue_page",
        staticmethod(forbid_finalization),
    )
    monkeypatch.setattr(
        publication,
        "_release_finalization_page",
        forbid_finalization,
    )
    monkeypatch.setattr(
        PublicationRepository,
        "activate_finalized_commit",
        staticmethod(forbid_finalization),
    )

    def cleanup_once(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        build_id: bytes,
        receipt_id: bytes,
        now: int,
    ) -> bool:
        cleanup_calls.append((build_id, receipt_id))
        return original_cleanup(
            work,
            gate_lease=gate_lease,
            ingest_turn=ingest_turn,
            build_id=build_id,
            receipt_id=receipt_id,
            now=now,
        )

    monkeypatch.setattr(
        PublicationRepository,
        "release_replayed_source_working",
        staticmethod(cleanup_once),
    )

    first_machine = publication.VNextIngestPublication(
        _context(database_path),
        clock=lambda: 120,
    )
    with adapter.publication_guard():
        issued = first_machine.issue_step(session, policy)
        assert issued.operation == "COMPLETE"
        with pytest.raises(
            ConnectionError,
            match="lost COMPLETE acknowledgement",
        ):
            first_machine.prepare_step(
                issued,
                artifact_adapters={},
                finalization_adapters={},
                library_activation=adapter,
            )

    assert adapter.checkpoint.status is publication.LibraryActivationStatus.COMPLETE
    assert adapter.complete_calls == 1
    assert adapter_journal == {"present": False}
    assert cleanup_calls == []
    connector = publication_fixtures._generated_database(database_path)
    assert connector.fetch_one(
        "SELECT build_id FROM operational_source_working_builds WHERE slot = 1"
    ) == (build_id,)
    connector.close()

    # A new process has no in-memory activation hint. It recovers COMPLETE from
    # the adapter, skips complete()/finalization/release, and performs only the
    # idempotent database marker cleanup that did not run before response loss.
    restarted_machine = publication.VNextIngestPublication(
        _context(database_path),
        clock=lambda: 120,
    )
    with adapter.publication_guard():
        replayed_issue = restarted_machine.issue_step(session, policy)
        assert replayed_issue.operation == "COMPLETE"
        replayed_preparation = restarted_machine.prepare_step(
            replayed_issue,
            artifact_adapters={},
            finalization_adapters={},
            library_activation=adapter,
        )
        result = restarted_machine.commit_step(session, replayed_preparation)

    assert result == h2hdb.VNextIngestAdvanceResult(
        h2hdb.VNextIngestPhase.FINALIZATION,
        0,
        True,
        False,
    )
    assert adapter.complete_calls == 1
    assert cleanup_calls == [(build_id, receipt_id)]
    assert finalization_calls == []
    assert adapter_journal == {"present": False}
    connector = publication_fixtures._generated_database(database_path)
    assert not connector.fetch_one(
        "SELECT 1 FROM operational_source_working_builds WHERE slot = 1"
    )
    assert connector.fetch_one(
        "SELECT state FROM catalog_publication_receipts WHERE receipt_id = %s",
        (receipt_id,),
    ) == ("PUBLISHED",)
    assert (
        connector.fetch_one(
            "SELECT receipt_id, revision, generation "
            "FROM catalog_publication_commit_heads WHERE channel = %s",
            (publication_fixtures._CHANNEL,),
        )
        == original_head
    )
    connector.close()
