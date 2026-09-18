"""Terminal candidate proofs are reused only before a fresh fenced release."""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass, field
from typing import Any

import pytest
from vnext_fault_harness import backend_of, open_connector
from vnext_pipeline import initialize_database

import h2hdb.vnext_cleanup_repository as cleanup
import h2hdb.vnext_ingest_facade as facade_module
from h2hdb import CoreConfig, VNextCurrentOnlyMaintenanceOutcome, VNextIngestFacade
from h2hdb.domain import CurrentOnlyCleanupTerminalState
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_cleanup_repository import CleanupTargetKind, VNextCleanupRepository
from h2hdb.vnext_maintenance_gate_repository import GateLease, MaintenanceGateRepository
from h2hdb.vnext_transaction import VNextUnitOfWork

pytestmark = pytest.mark.cleanup_acceptance

_DURATION = 1_000
_NEXT = "_VNextIngestFacade__next_current_only_cycle"
_STATE = "_VNextIngestFacade__current_only_state"
_CANONICAL_PROBE = (
    "SELECT r.value_sha256 FROM catalog_canonical_value_allocation_anchors AS r "
)


@dataclass
class _Clock:
    now: int = 100

    def __call__(self) -> int:
        return self.now


def _seed(config: CoreConfig) -> None:
    initialize_database(config)
    with closing(open_connector(config)) as connector:
        with connector.transaction():
            connector.execute(
                "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
                "VALUES (%s, 0)",
                (bytes((201,)) + bytes(31),),
            )
            lease = MaintenanceGateRepository.claim_exclusive(
                VNextUnitOfWork(connector, backend=backend_of(config)),
                now=1,
                lease_duration=_DURATION,
            )
        with connector.transaction():
            VNextCleanupRepository.begin_cycle(
                VNextUnitOfWork(connector, backend=backend_of(config)),
                gate_lease=lease,
                target_kind=CleanupTargetKind.CONTENT_BLOB,
                shard_no=201,
                cycle_cutoff_at=100,
                now=2,
            )
        with connector.transaction():
            MaintenanceGateRepository.release(
                VNextUnitOfWork(connector, backend=backend_of(config)), lease, now=3
            )


@dataclass
class _SqlEvidence:
    statements: list[str] = field(default_factory=list)

    @property
    def canonical_probes(self) -> int:
        return sum(query.startswith(_CANONICAL_PROBE) for query in self.statements)


def _observe_sql(config: CoreConfig, monkeypatch: pytest.MonkeyPatch) -> _SqlEvidence:
    with closing(open_connector(config)) as connector:
        connector_type = type(connector)
    fetch_one = connector_type.fetch_one
    evidence = _SqlEvidence()

    def measured(
        self: SQLConnector, query: str, data: tuple[Any, ...] = ()
    ) -> tuple[Any, ...]:
        result = fetch_one(self, query, data)
        evidence.statements.append(" ".join(query.split()))
        return result

    monkeypatch.setattr(connector_type, "fetch_one", measured)
    return evidence


def _assert_single_terminal_scan(evidence: _SqlEvidence) -> None:
    assert evidence.canonical_probes == 1, "terminal proof scanned candidates again"


@pytest.mark.parametrize("duplicate_proof", [False, True])
def test_terminal_scan_budget_rejects_duplicate_scan_negative_control(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    duplicate_proof: bool,
) -> None:
    _seed(db_config)
    evidence = _observe_sql(db_config, monkeypatch)
    if duplicate_proof:
        select = getattr(VNextIngestFacade, _NEXT)

        def repeated(self: VNextIngestFacade, *args: Any, **kwargs: Any) -> Any:
            result = select(self, *args, **kwargs)
            if isinstance(result, CurrentOnlyCleanupTerminalState):
                getattr(self, _STATE)(*args, **kwargs)
            return result

        monkeypatch.setattr(VNextIngestFacade, _NEXT, repeated)

    with VNextIngestFacade(db_config, clock=_Clock()) as facade:
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
        )
        if duplicate_proof:
            with pytest.raises(AssertionError, match="scanned candidates again"):
                _assert_single_terminal_scan(evidence)
            assert evidence.canonical_probes == 2
        else:
            _assert_single_terminal_scan(evidence)
        # An idle retry and a subsequent SHARED claim independently recheck
        # durable state; the previous terminal result cannot authorize either.
        prior = evidence.canonical_probes
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
        )
        assert evidence.canonical_probes == prior + 1
        session = facade.try_claim_ingest(True, _DURATION)
        assert session is not None
        assert evidence.canonical_probes == prior + 2
        facade.complete_ingest(session)


@pytest.mark.parametrize("budget", [1, 2])
def test_batch_budget_still_requires_complete_final_state(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, budget: int
) -> None:
    _seed(db_config)
    monkeypatch.setattr(facade_module, "_CURRENT_ONLY_BATCHES_PER_ATTEMPT", budget)
    evidence = _observe_sql(db_config, monkeypatch)
    state = getattr(VNextIngestFacade, _STATE)
    final_calls = 0

    def recorded(*args: Any, **kwargs: Any) -> Any:
        nonlocal final_calls
        final_calls += 1
        return state(*args, **kwargs)

    monkeypatch.setattr(VNextIngestFacade, _STATE, recorded)
    with VNextIngestFacade(db_config, clock=_Clock()) as facade:
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
            if budget == 1
            else VNextCurrentOnlyMaintenanceOutcome.DONE
        )
    assert final_calls == 1
    # At budget=1 the OPEN cycle itself proves more work. At budget=2 the
    # completed cycle requires the full candidate scan to establish DONE.
    assert evidence.canonical_probes == (0 if budget == 1 else 1)


@pytest.mark.parametrize("replace_owner", [False, True])
def test_expiry_or_takeover_after_terminal_selection_never_returns_done(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, replace_owner: bool
) -> None:
    _seed(db_config)
    clock = _Clock()
    select = getattr(VNextIngestFacade, _NEXT)
    terminals: list[CurrentOnlyCleanupTerminalState] = []
    replacement: list[GateLease] = []

    def expire(*args: Any, **kwargs: Any) -> Any:
        result = select(*args, **kwargs)
        if isinstance(result, CurrentOnlyCleanupTerminalState):
            terminals.append(result)
            clock.now += _DURATION
            if replace_owner:
                with (
                    closing(open_connector(db_config)) as connector,
                    connector.transaction(),
                ):
                    replacement.append(
                        MaintenanceGateRepository.claim_exclusive(
                            VNextUnitOfWork(connector, backend=backend_of(db_config)),
                            now=clock.now,
                            lease_duration=_DURATION,
                        )
                    )
        return result

    monkeypatch.setattr(VNextIngestFacade, _NEXT, expire)
    with VNextIngestFacade(db_config, clock=clock) as facade:
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        )
        assert terminals == [CurrentOnlyCleanupTerminalState.DONE]
        with (
            closing(open_connector(db_config)) as connector,
            connector.read_transaction(),
        ):
            assert connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_content_blobs"
            ) == (0,)
            if replacement:
                lease = replacement[0]
                assert connector.fetch_all(
                    "SELECT owner_token, gate_generation, lease_expires_at "
                    "FROM operational_maintenance_gate_owners"
                ) == [
                    (lease.owner_token, lease.gate_generation, lease.lease_expires_at)
                ]
        if replacement:
            with (
                closing(open_connector(db_config)) as connector,
                connector.transaction(),
            ):
                MaintenanceGateRepository.release(
                    VNextUnitOfWork(connector, backend=backend_of(db_config)),
                    replacement[0],
                    now=clock.now,
                )
        # The retry independently observes already committed cleanup.
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
        )


@pytest.mark.parametrize("loss_at", ["selection", "release"])
def test_terminal_commit_response_loss_is_not_reported_as_done(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, loss_at: str
) -> None:
    _seed(db_config)
    with closing(open_connector(db_config)) as connector:
        connector_type = type(connector)
    commit = connector_type.commit
    classify = cleanup._current_only_terminal_state
    release = "_VNextIngestFacade__release_current_only_lease"
    release_method = getattr(VNextIngestFacade, release)
    armed = False
    injected = False

    def classified(*args: Any, **kwargs: Any) -> CurrentOnlyCleanupTerminalState:
        nonlocal armed
        result = classify(*args, **kwargs)
        if loss_at == "selection":
            armed = True
        return result

    def releasing(*args: Any, **kwargs: Any) -> None:
        nonlocal armed
        if loss_at == "release":
            armed = True
        release_method(*args, **kwargs)

    def lost(self: SQLConnector) -> None:
        nonlocal injected
        commit(self)
        if armed and not injected:
            injected = True
            raise RuntimeError("lost terminal COMMIT response")

    monkeypatch.setattr(cleanup, "_current_only_terminal_state", classified)
    monkeypatch.setattr(VNextIngestFacade, release, releasing)
    monkeypatch.setattr(connector_type, "commit", lost)
    with VNextIngestFacade(db_config, clock=_Clock()) as facade:
        with pytest.raises(RuntimeError, match="lost terminal COMMIT response"):
            facade.drain_current_only_maintenance(_DURATION)
        assert injected
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
        )


def test_terminal_blocked_classification_does_not_repeat_candidate_sql(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(db_config)
    evidence = _observe_sql(db_config, monkeypatch)
    # The durable blocked-payload predicates have separate integration tests;
    # this branch fault forces their result after real cleanup and real SQL
    # candidate exhaustion, to check terminal orchestration and SQL cost.
    monkeypatch.setattr(
        cleanup, "_catalog_publication_payload_is_blocked", lambda _: True
    )
    with VNextIngestFacade(db_config, clock=_Clock()) as facade:
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        )
    _assert_single_terminal_scan(evidence)


def test_terminal_proof_cannot_authorize_claim_after_new_cleanup_work(
    db_config: CoreConfig,
) -> None:
    _seed(db_config)
    with VNextIngestFacade(db_config, clock=_Clock()) as facade:
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
        )
        with closing(open_connector(db_config)) as connector, connector.transaction():
            connector.execute(
                "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
                "VALUES (%s, 0)",
                (bytes((202,)) + bytes(31),),
            )
        # A prior DONE cannot bypass the fresh proof under SHARED claim locks.
        assert facade.try_claim_ingest(True, _DURATION) is None
        assert facade.drain_current_only_maintenance(_DURATION) is (
            VNextCurrentOnlyMaintenanceOutcome.DONE
        )
        session = facade.try_claim_ingest(True, _DURATION)
        assert session is not None
        facade.complete_ingest(session)
