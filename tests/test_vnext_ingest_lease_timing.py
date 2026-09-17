"""Elapsed database work must not consume a newly granted ingest lease."""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from typing import Any

import pytest
from vnext_fault_harness import backend_of, open_connector
from vnext_pipeline import initialize_database

from h2hdb import CoreConfig, VNextIngestFacade
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_cleanup_repository import VNextCleanupRepository
from h2hdb.vnext_download_ingest_repository import DownloadIngestRepository
from h2hdb.vnext_maintenance_gate_repository import (
    MaintenanceGateRepository,
    MaintenanceGateUnavailableError,
)
from h2hdb.vnext_transaction import LockRank, VNextUnitOfWork

pytestmark = pytest.mark.cleanup_acceptance

_SECOND = 1_000_000
_DURATION = 300 * _SECOND


@dataclass
class _Clock:
    now: int = 1_000 * _SECOND

    def __call__(self) -> int:
        return self.now


def _delay_once(
    monkeypatch: pytest.MonkeyPatch,
    config: CoreConfig,
    clock: _Clock,
    point: str,
    seconds: int,
) -> list[str]:
    fired: list[str] = []

    def advance() -> None:
        if not fired:
            fired.append(point)
            clock.now += seconds * _SECOND

    match point:
        case "maintenance":
            original_probe = VNextCleanupRepository.current_only_maintenance_state

            def probe(*args: Any, **kwargs: Any) -> Any:
                result = original_probe(*args, **kwargs)
                advance()
                return result

            monkeypatch.setattr(
                VNextCleanupRepository,
                "current_only_maintenance_state",
                staticmethod(probe),
            )
        case "connect" | "commit":
            with closing(open_connector(config)) as connector:
                connector_type = type(connector)
            original_boundary = getattr(connector_type, point)

            def boundary(self: SQLConnector) -> None:
                original_boundary(self)
                advance()

            monkeypatch.setattr(connector_type, point, boundary)
        case _:
            expected_rank = {
                "gate": LockRank.MAINTENANCE_GATE,
                "download": LockRank.DOWNLOAD_FENCE,
                "ingest": LockRank.INGEST_FENCE,
            }[point]
            original_lock = VNextUnitOfWork.lock_row

            def lock(
                self: VNextUnitOfWork,
                rank: LockRank,
                key: bytes,
                query: str,
                data: tuple[Any, ...] = (),
            ) -> tuple[Any, ...]:
                result = original_lock(self, rank, key, query, data)
                if rank is expected_rank:
                    advance()
                return result

            monkeypatch.setattr(VNextUnitOfWork, "lock_row", lock)
    return fired


def _owners(config: CoreConfig) -> tuple[list[tuple[Any, ...]], ...]:
    with closing(open_connector(config)) as connector, connector.read_transaction():
        return tuple(
            connector.fetch_all(f"SELECT * FROM {table}")
            for table in (
                "operational_maintenance_gate_owners",
                "operational_ingest_generation_owners",
            )
        )


@pytest.mark.parametrize(
    "point", ["connect", "gate", "maintenance", "download", "ingest"]
)
@pytest.mark.parametrize("seconds", [288, 301])
def test_claim_grants_both_leases_after_database_waits(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    point: str,
    seconds: int,
) -> None:
    initialize_database(db_config)
    clock = _Clock()
    with VNextIngestFacade(db_config, clock=clock) as facade:
        with monkeypatch.context() as delay:
            fired = _delay_once(delay, db_config, clock, point, seconds)
            session = facade.try_claim_ingest(True, _DURATION)
        assert fired == [point]
        assert session is not None
        assert session.gate_lease_expires_at == clock.now + _DURATION
        assert session.ingest_lease_expires_at == session.gate_lease_expires_at
        clock.now += 60 * _SECOND
        renewed = facade.renew_ingest(session, _DURATION)
        assert renewed.gate_lease_expires_at == clock.now + _DURATION
        assert renewed.ingest_lease_expires_at == renewed.gate_lease_expires_at
        facade.complete_ingest(renewed)


@pytest.mark.parametrize("point", ["connect", "gate", "download", "ingest"])
@pytest.mark.parametrize("seconds", [239, 240, 241])
def test_renewal_uses_time_after_all_locks_and_cannot_revive_expired_owners(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    point: str,
    seconds: int,
) -> None:
    initialize_database(db_config)
    clock = _Clock()
    with VNextIngestFacade(db_config, clock=clock) as facade:
        session = facade.try_claim_ingest(True, _DURATION)
        assert session is not None
        before = _owners(db_config)
        clock.now += 60 * _SECOND
        with monkeypatch.context() as delay:
            fired = _delay_once(delay, db_config, clock, point, seconds)
            if seconds < 240:
                renewed = facade.renew_ingest(session, _DURATION)
                assert renewed.gate_lease_expires_at == clock.now + _DURATION
                assert renewed.ingest_lease_expires_at == renewed.gate_lease_expires_at
            else:
                with pytest.raises(MaintenanceGateUnavailableError, match="expired"):
                    facade.renew_ingest(session, _DURATION)
        assert fired == [point]
        if seconds >= 240:
            assert _owners(db_config) == before
            replacement = facade.try_claim_ingest(True, _DURATION)
            assert replacement is not None
            assert replacement.ingest_generation > session.ingest_generation
            with pytest.raises(MaintenanceGateUnavailableError):
                facade.renew_ingest(session, _DURATION)
            facade.complete_ingest(replacement)


def test_claim_expired_during_commit_is_not_returned_and_can_be_reclaimed(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    initialize_database(db_config)
    clock = _Clock()
    with VNextIngestFacade(db_config, clock=clock) as facade:
        with monkeypatch.context() as delay:
            fired = _delay_once(delay, db_config, clock, "commit", 300)
            with pytest.raises(
                MaintenanceGateUnavailableError, match="committed.*expired"
            ):
                facade.try_claim_ingest(True, _DURATION)
        assert fired == ["commit"]
        # COMMIT succeeded: the expired generation remains durable history.
        replacement = facade.try_claim_ingest(True, _DURATION)
        assert replacement is not None
        assert replacement.ingest_generation == 2
        facade.complete_ingest(replacement)


def test_renewal_expired_during_commit_is_not_returned_as_live(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    initialize_database(db_config)
    clock = _Clock()
    with VNextIngestFacade(db_config, clock=clock) as facade:
        session = facade.try_claim_ingest(True, _DURATION)
        assert session is not None
        clock.now += 60 * _SECOND
        with monkeypatch.context() as delay:
            fired = _delay_once(delay, db_config, clock, "commit", 300)
            with pytest.raises(MaintenanceGateUnavailableError, match="expired"):
                facade.renew_ingest(session, _DURATION)
        assert fired == ["commit"]
        replacement = facade.try_claim_ingest(True, _DURATION)
        assert replacement is not None
        assert replacement.ingest_generation > session.ingest_generation
        facade.complete_ingest(replacement)


@pytest.mark.parametrize("mode", ["exclusive", "full_shared"])
def test_contended_gate_rejects_before_expensive_maintenance_probes(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    initialize_database(db_config)
    clock = _Clock()
    with closing(open_connector(db_config)) as connector:
        # 64 is the maintenance gate's complete fixed slot domain.
        for _ in range(64 if mode == "full_shared" else 1):
            with connector.transaction():
                claim = (
                    MaintenanceGateRepository.claim_shared
                    if mode == "full_shared"
                    else MaintenanceGateRepository.claim_exclusive
                )
                claim(
                    VNextUnitOfWork(connector, backend=backend_of(db_config)),
                    now=clock.now,
                    lease_duration=_DURATION,
                )

    def forbidden_probe(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("a contended gate must not scan maintenance candidates")

    monkeypatch.setattr(
        VNextCleanupRepository,
        "current_only_maintenance_state",
        staticmethod(forbidden_probe),
    )
    before = _owners(db_config)
    with VNextIngestFacade(db_config, clock=clock) as facade:
        assert facade.try_claim_ingest(True, _DURATION) is None
    assert _owners(db_config) == before


@pytest.mark.parametrize("kind", ["handoff", "expired_download"])
def test_delayed_claim_preserves_download_handoff_and_completion(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    initialize_database(db_config)
    clock = _Clock()
    with closing(open_connector(db_config)) as connector:
        with connector.transaction():
            turn = DownloadIngestRepository.claim_download(
                VNextUnitOfWork(connector, backend=backend_of(db_config)),
                now=clock.now,
                lease_duration=_DURATION,
            )
        if kind == "handoff":
            with connector.transaction():
                DownloadIngestRepository.handoff_download(
                    VNextUnitOfWork(connector, backend=backend_of(db_config)),
                    turn,
                    now=clock.now,
                )
    with VNextIngestFacade(db_config, clock=clock) as facade:
        with monkeypatch.context() as delay:
            fired = _delay_once(delay, db_config, clock, "ingest", 301)
            session = facade.try_claim_ingest(False, _DURATION)
        assert fired == ["ingest"]
        assert session is not None
        assert session.download_generation == turn.generation
        assert session.handoff_kind == (
            "DOWNLOADER" if kind == "handoff" else "EXPIRED_TAKEOVER"
        )
        assert session.gate_lease_expires_at == clock.now + _DURATION
        assert session.ingest_lease_expires_at == session.gate_lease_expires_at
        assert session.consumed_at == clock.now
        completed = facade.complete_ingest(session)
        assert completed.download_generation == turn.generation


def test_second_renewal_mutation_failure_rolls_back_both_owners(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    initialize_database(db_config)
    clock = _Clock()
    with VNextIngestFacade(db_config, clock=clock) as facade:
        session = facade.try_claim_ingest(True, _DURATION)
        assert session is not None
        before = _owners(db_config)
        clock.now += 60 * _SECOND
        original = VNextUnitOfWork.compare_and_swap
        updates: list[str] = []

        def fail_second_update(
            self: VNextUnitOfWork,
            query: str,
            data: tuple[Any, ...] = (),
            *,
            authority: str,
        ) -> None:
            updates.append(authority)
            if authority == "ingest generation owner lease":
                raise RuntimeError("injected second renewal write failure")
            original(self, query, data, authority=authority)

        with monkeypatch.context() as fault:
            fault.setattr(VNextUnitOfWork, "compare_and_swap", fail_second_update)
            with pytest.raises(RuntimeError, match="injected second renewal"):
                facade.renew_ingest(session, _DURATION)
        assert updates == [
            "maintenance gate owner lease",
            "ingest generation owner lease",
        ]
        assert _owners(db_config) == before
        renewed = facade.renew_ingest(session, _DURATION)
        assert renewed.gate_lease_expires_at == clock.now + _DURATION
        facade.complete_ingest(renewed)


def test_expired_committed_handoff_fails_explicitly_and_preserves_consumption(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject unusable authority without disguising a committed handoff as idle.

    The current immutable one-to-one consumption contract has no linked-turn
    takeover. This test proves failure visibility/history preservation, not
    automatic recovery of an expired linked turn.
    """

    initialize_database(db_config)
    clock = _Clock()
    with closing(open_connector(db_config)) as connector:
        with connector.transaction():
            turn = DownloadIngestRepository.claim_download(
                VNextUnitOfWork(connector, backend=backend_of(db_config)),
                now=clock.now,
                lease_duration=_DURATION,
            )
        with connector.transaction():
            DownloadIngestRepository.handoff_download(
                VNextUnitOfWork(connector, backend=backend_of(db_config)),
                turn,
                now=clock.now,
            )
    with VNextIngestFacade(db_config, clock=clock) as facade:
        with monkeypatch.context() as delay:
            _delay_once(delay, db_config, clock, "commit", 300)
            with pytest.raises(
                MaintenanceGateUnavailableError, match="committed.*expired"
            ):
                facade.try_claim_ingest(False, _DURATION)
    with closing(open_connector(db_config)) as connector, connector.read_transaction():
        assert connector.fetch_all(
            "SELECT download_generation, ingest_generation "
            "FROM operational_download_ingest_consumptions"
        ) == [(turn.generation, 1)]
        assert connector.fetch_one(
            "SELECT completed_at FROM operational_download_generations WHERE generation = %s",
            (turn.generation,),
        ) == (None,)
