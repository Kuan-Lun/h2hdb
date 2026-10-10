"""Orphan-first scheduling preserves the complete public cleanup lifecycle.

The source, candidate, publication and resource graph comes exclusively from
public ingestion. Native SQL is a read-only oracle; interrupted cleanup is
created through its fenced repository operation, with foreign keys enabled.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from typing import Any

import pytest
from test_vnext_pipeline_workflows import Pipeline
from vnext_fault_harness import backend_of, open_connector
from vnext_pipeline import (
    LEASE_MICROSECONDS,
    Clock,
    MemoryLibrary,
    MemorySource,
    claim_session,
    gallery,
    ingest_policy,
    initialize_database,
    library_view,
    run_ingest_turn,
)
from vnext_test_database import assert_foreign_key_integrity

import h2hdb._cleanup.selection as cleanup_selection
from h2hdb import CoreConfig, VNextCurrentOnlyMaintenanceOutcome, VNextIngestFacade
from h2hdb._cleanup.cycle import CleanupCycleRepository
from h2hdb._cleanup.eligibility import CurrentOnlyEligibilityProof
from h2hdb._cleanup.model import CleanupCycle, CleanupTargetKind
from h2hdb.domain import ArtifactReleaseStorageEvidence, StorageObjectKey
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_artifact_release_repository import ArtifactReleaseRepository
from h2hdb.vnext_maintenance_gate_repository import MaintenanceGateRepository
from h2hdb.vnext_transaction import VNextUnitOfWork

pytestmark = [pytest.mark.cleanup_acceptance, pytest.mark.deep]


class _AbandonedTurn(Exception):
    pass


class _ResponseLost(Exception):
    pass


@dataclass
class _LeaseClock:
    base: Clock
    elapsed: int = 0

    def __call__(self) -> int:
        return self.base() + self.elapsed


@pytest.fixture
def pipeline(db_config: CoreConfig) -> Pipeline:
    initialize_database(db_config)
    source = MemorySource([gallery(1001, pages=[b"page-a"], artists=["alice"])])
    return Pipeline(db_config, source, MemoryLibrary(source))


def _pending_tokens(pipeline: Pipeline) -> set[bytes]:
    with closing(open_connector(pipeline.config)) as connector:
        with connector.read_transaction():
            rows = connector.fetch_all(
                "SELECT prepared.protection_token "
                "FROM catalog_prepared_artifacts prepared "
                "WHERE prepared.state IN ('PENDING', 'PREPARED') "
                "AND NOT EXISTS (SELECT 1 "
                "FROM operational_catalog_working_candidates working "
                "WHERE working.candidate_id = prepared.candidate_id) "
                "AND NOT EXISTS (SELECT 1 FROM catalog_publication_commits receipt "
                "WHERE receipt.candidate_id = prepared.candidate_id)"
            )
    return {bytes(row[0]) for row in rows}


def _make_orphans(pipeline: Pipeline, *, round_number: int = 0) -> Clock:
    clock = Clock()
    pipeline.source.put(
        gallery(1001, pages=[f"page-{round_number}".encode()], artists=["alice"])
    )

    def abandon(label: str) -> None:
        if label == "publication.commit:VALIDATE_PREPARED":
            raise _AbandonedTurn

    with VNextIngestFacade(pipeline.config, clock=clock) as facade:
        session = claim_session(facade)
        with pytest.raises(_AbandonedTurn):
            run_ingest_turn(
                facade,
                session=session,
                source=pipeline.source,
                library=pipeline.library,
                boundary=abandon,
            )
        # Stop the producer through public coordination without publishing its
        # candidate. A later policy handoff retires that complete working graph.
        facade.complete_ingest(session)
    pipeline.turn(
        clock=clock,
        policy=ingest_policy(spam_occurrence_threshold=7 + round_number),
        drain=False,
    )
    assert len(_pending_tokens(pipeline)) == 2
    return clock


def _drain(facade: VNextIngestFacade, pipeline: Pipeline) -> None:
    for _attempt in range(256):
        outcome = facade.drain_current_only_maintenance(
            LEASE_MICROSECONDS,
            artifact_release_adapters={pipeline.library.adapter_id: pipeline.library},
        )
        if outcome is VNextCurrentOnlyMaintenanceOutcome.DONE:
            return
        assert outcome is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
    pytest.fail("public cleanup did not reach DONE within its bounded attempts")


def _assert_complete(facade: VNextIngestFacade, pipeline: Pipeline) -> None:
    assert not _pending_tokens(pipeline)
    with closing(open_connector(pipeline.config)) as connector:
        assert_foreign_key_integrity(connector)
    pipeline.ready()
    session = facade.try_claim_ingest(True, LEASE_MICROSECONDS)
    assert session is not None
    facade.complete_ingest(session)


def test_restarted_cleanup_finds_new_orphans_after_done(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Neither DONE nor an empty candidate result survives a new public turn."""

    selector_calls = 0
    select_candidate = cleanup_selection._next_current_only_candidate

    def counted_candidate(
        work: VNextUnitOfWork,
        *,
        cycle_cutoff_at: int,
        eligibility_proof: CurrentOnlyEligibilityProof | None = None,
    ) -> tuple[
        tuple[CleanupTargetKind, int] | None, CurrentOnlyEligibilityProof | None
    ]:
        nonlocal selector_calls
        selector_calls += 1
        return select_candidate(
            work,
            cycle_cutoff_at=cycle_cutoff_at,
            eligibility_proof=eligibility_proof,
        )

    monkeypatch.setattr(
        cleanup_selection, "_next_current_only_candidate", counted_candidate
    )
    previous_tokens: set[bytes] = set()
    clock = _LeaseClock(Clock())
    with VNextIngestFacade(pipeline.config, clock=clock) as facade:
        for round_number in range(2):
            clock.base = _make_orphans(pipeline, round_number=round_number)
            tokens = _pending_tokens(pipeline)
            assert tokens.isdisjoint(previous_tokens)
            current_library = library_view(pipeline.library)
            before_release = selector_calls
            first = facade.drain_current_only_maintenance(
                LEASE_MICROSECONDS,
                artifact_release_adapters={
                    pipeline.library.adapter_id: pipeline.library
                },
            )
            assert first is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
            assert len(_pending_tokens(pipeline)) == 1
            assert selector_calls == before_release
            if round_number:
                # The original facade already observed DONE before new work.
                # A restart now has no in-memory cursor for the remaining item.
                with VNextIngestFacade(pipeline.config, clock=clock) as restarted:
                    before_completion = selector_calls
                    _drain(restarted, pipeline)
                    assert selector_calls > before_completion
                    _assert_complete(restarted, pipeline)
            before_completion = selector_calls
            _drain(facade, pipeline)
            assert selector_calls > before_completion
            _assert_complete(facade, pipeline)
            assert library_view(pipeline.library) == current_library
            assert tokens <= pipeline.library.tombstones
            previous_tokens |= tokens


def test_release_response_loss_restarts_with_the_same_token(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _make_orphans(pipeline)
    tokens = _pending_tokens(pipeline)
    current_library = library_view(pipeline.library)
    release = pipeline.library.release
    lost = False
    transaction_depth = 0
    transaction = SQLConnector.transaction

    @contextmanager
    def tracked_transaction(connector: SQLConnector) -> Iterator[None]:
        nonlocal transaction_depth
        with transaction(connector):
            transaction_depth += 1
            try:
                yield
            finally:
                transaction_depth -= 1

    def lose_response(
        key: StorageObjectKey, digest: bytes, size: int, token: bytes
    ) -> ArtifactReleaseStorageEvidence:
        nonlocal lost
        assert transaction_depth == 0
        result = release(key, digest, size, token)
        if not lost:
            lost = True
            raise _ResponseLost
        return result

    monkeypatch.setattr(SQLConnector, "transaction", tracked_transaction)
    monkeypatch.setattr(pipeline.library, "release", lose_response)
    start = len(pipeline.library.release_calls)
    with VNextIngestFacade(pipeline.config, clock=clock) as facade:
        with pytest.raises(_ResponseLost):
            facade.drain_current_only_maintenance(
                LEASE_MICROSECONDS,
                artifact_release_adapters={
                    pipeline.library.adapter_id: pipeline.library
                },
            )
    assert _pending_tokens(pipeline) == tokens
    assert len(pipeline.library.tombstones & tokens) == 1
    with VNextIngestFacade(pipeline.config, clock=clock) as restarted:
        _drain(restarted, pipeline)
        _assert_complete(restarted, pipeline)
    calls = pipeline.library.release_calls[start:]
    assert len(calls) == 3
    assert calls[0] == calls[1]
    assert {token for _key, token in calls} == tokens
    assert library_view(pipeline.library) == current_library


def test_stale_positive_hint_requires_empty_issue_and_fresh_done_proof(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A scheduling hint cannot turn an empty issued page into progress."""

    clock = _make_orphans(pipeline)
    with VNextIngestFacade(pipeline.config, clock=clock) as facade:
        _drain(facade, pipeline)
        assert not _pending_tokens(pipeline)
        current_library = library_view(pipeline.library)
        release_calls = len(pipeline.library.release_calls)
        pending = ArtifactReleaseRepository.has_pending_release
        issue = ArtifactReleaseRepository.issue_page
        select = cleanup_selection._next_current_only_candidate
        forced_hint = False
        issued_terminal: list[bool] = []
        selector_calls = 0

        def stale_hint(work: VNextUnitOfWork) -> bool:
            nonlocal forced_hint
            actual = pending(work)
            assert not actual
            if not forced_hint:
                forced_hint = True
                return True
            return actual

        def issued_page(*args: Any, **kwargs: Any) -> Any:
            page = issue(*args, **kwargs)
            issued_terminal.append(page.terminal)
            return page

        def counted_candidate(
            work: VNextUnitOfWork,
            *,
            cycle_cutoff_at: int,
            eligibility_proof: CurrentOnlyEligibilityProof | None = None,
        ) -> tuple[
            tuple[CleanupTargetKind, int] | None, CurrentOnlyEligibilityProof | None
        ]:
            nonlocal selector_calls
            selector_calls += 1
            return select(
                work,
                cycle_cutoff_at=cycle_cutoff_at,
                eligibility_proof=eligibility_proof,
            )

        monkeypatch.setattr(
            ArtifactReleaseRepository, "has_pending_release", staticmethod(stale_hint)
        )
        monkeypatch.setattr(
            ArtifactReleaseRepository, "issue_page", staticmethod(issued_page)
        )
        monkeypatch.setattr(
            cleanup_selection, "_next_current_only_candidate", counted_candidate
        )
        outcome = facade.drain_current_only_maintenance(
            LEASE_MICROSECONDS,
            artifact_release_adapters={pipeline.library.adapter_id: pipeline.library},
        )
        assert outcome is VNextCurrentOnlyMaintenanceOutcome.DONE
        assert forced_hint
        assert issued_terminal == [True]
        assert selector_calls > 0
        assert len(pipeline.library.release_calls) == release_calls
        assert library_view(pipeline.library) == current_library
        _assert_complete(facade, pipeline)


@pytest.mark.parametrize("expiry_point", ["before_io", "after_io", "after_commit"])
def test_release_expiry_preserves_authority_and_replays(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch, expiry_point: str
) -> None:
    clock = _LeaseClock(_make_orphans(pipeline))
    tokens = _pending_tokens(pipeline)
    current_library = library_view(pipeline.library)
    expired = False
    original_release_page = ArtifactReleaseRepository.release_page
    original_commit_page = ArtifactReleaseRepository.commit_page
    release = pipeline.library.release

    def expire() -> None:
        nonlocal expired
        if not expired:
            expired = True
            clock.elapsed += LEASE_MICROSECONDS + 1

    def release_page(*args: Any, **kwargs: Any) -> Any:
        if expiry_point == "before_io":
            expire()
        return original_release_page(*args, **kwargs)

    def commit_page(*args: Any, **kwargs: Any) -> Any:
        result = original_commit_page(*args, **kwargs)
        if expiry_point == "after_commit":
            expire()
        return result

    def release_and_expire(
        key: StorageObjectKey, digest: bytes, size: int, token: bytes
    ) -> ArtifactReleaseStorageEvidence:
        result = release(key, digest, size, token)
        if expiry_point == "after_io":
            expire()
        return result

    monkeypatch.setattr(
        ArtifactReleaseRepository, "release_page", staticmethod(release_page)
    )
    monkeypatch.setattr(
        ArtifactReleaseRepository, "commit_page", staticmethod(commit_page)
    )
    monkeypatch.setattr(pipeline.library, "release", release_and_expire)
    start = len(pipeline.library.release_calls)
    with VNextIngestFacade(pipeline.config, clock=clock) as facade:
        first = facade.drain_current_only_maintenance(
            LEASE_MICROSECONDS,
            artifact_release_adapters={pipeline.library.adapter_id: pipeline.library},
        )
    assert expired
    assert first is (
        VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        if expiry_point == "after_commit"
        else VNextCurrentOnlyMaintenanceOutcome.CONTENDED
    )
    assert len(_pending_tokens(pipeline)) == (
        1 if expiry_point == "after_commit" else 2
    )
    assert len(pipeline.library.release_calls) - start == int(
        expiry_point != "before_io"
    )
    with VNextIngestFacade(pipeline.config, clock=clock) as restarted:
        _drain(restarted, pipeline)
        _assert_complete(restarted, pipeline)
    calls = pipeline.library.release_calls[start:]
    assert len(calls) == (3 if expiry_point == "after_io" else 2)
    if expiry_point == "after_io":
        assert calls[0] == calls[1]
    assert tokens <= pipeline.library.tombstones
    assert library_view(pipeline.library) == current_library


def _open_hash_cleanup(pipeline: Pipeline, clock: Callable[[], int]) -> CleanupCycle:
    with closing(open_connector(pipeline.config)) as connector:
        backend = backend_of(pipeline.config)
        with connector.transaction():
            gate = MaintenanceGateRepository.claim_exclusive(
                VNextUnitOfWork(connector, backend=backend),
                now=clock(),
                lease_duration=LEASE_MICROSECONDS,
            )
        with connector.transaction():
            cycle = CleanupCycleRepository.begin_cycle(
                VNextUnitOfWork(connector, backend=backend),
                gate_lease=gate,
                target_kind=CleanupTargetKind.HASH_CACHE_OBSERVATION,
                shard_no=0,
                cycle_cutoff_at=clock(),
                max_rows_per_transaction=1,
                now=clock(),
            )
        with connector.transaction():
            MaintenanceGateRepository.release(
                VNextUnitOfWork(connector, backend=backend), gate, now=clock()
            )
    return cycle


@pytest.mark.parametrize("insertion_point", ["before_hint", "before_claim"])
def test_interrupted_cycle_precedes_release_even_after_the_hint(
    pipeline: Pipeline, monkeypatch: pytest.MonkeyPatch, insertion_point: str
) -> None:
    clock = _make_orphans(pipeline)
    tokens = _pending_tokens(pipeline)
    current_library = library_view(pipeline.library)
    opened: CleanupCycle | None = None
    armed = False
    transaction = SQLConnector.transaction
    has_pending_release = ArtifactReleaseRepository.has_pending_release
    advance_cycle = CleanupCycleRepository.advance_current_only_cycle
    completed: list[CleanupCycle] = []

    if insertion_point == "before_hint":
        opened = _open_hash_cleanup(pipeline, clock)

    def pending(work: VNextUnitOfWork) -> bool:
        nonlocal armed
        result = has_pending_release(work)
        if result and opened is None:
            armed = True
        return result

    @contextmanager
    def insert_before_claim(connector: SQLConnector) -> Iterator[None]:
        nonlocal armed, opened
        if armed:
            # The optimistic read has committed. A different owner now opens
            # a real durable cycle before the facade's claim transaction.
            armed = False
            opened = _open_hash_cleanup(pipeline, clock)
        with transaction(connector):
            yield

    def advance(*args: Any, **kwargs: Any) -> Any:
        results = advance_cycle(*args, **kwargs)
        if results[-1].cycle_complete:
            completed.append(kwargs["cycle"])
        return results

    monkeypatch.setattr(
        ArtifactReleaseRepository, "has_pending_release", staticmethod(pending)
    )
    monkeypatch.setattr(SQLConnector, "transaction", insert_before_claim)
    monkeypatch.setattr(
        CleanupCycleRepository, "advance_current_only_cycle", staticmethod(advance)
    )
    start = len(pipeline.library.release_calls)
    with VNextIngestFacade(pipeline.config, clock=clock) as facade:
        first = facade.drain_current_only_maintenance(
            LEASE_MICROSECONDS,
            artifact_release_adapters={pipeline.library.adapter_id: pipeline.library},
        )
        assert first is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        assert opened is not None
        assert completed and completed[0] == opened
        assert len(pipeline.library.release_calls) == start
        assert _pending_tokens(pipeline) == tokens
        _drain(facade, pipeline)
        _assert_complete(facade, pipeline)
    assert tokens <= pipeline.library.tombstones
    assert library_view(pipeline.library) == current_library
