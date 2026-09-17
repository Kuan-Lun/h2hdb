"""Cleanup diagnostics preserve commit evidence, phase identity, and lease timing."""

from __future__ import annotations

import json
import logging
from contextlib import closing, contextmanager
from dataclasses import dataclass
from typing import Any

import pytest
from vnext_fault_harness import backend_of, open_connector
from vnext_pipeline import initialize_database

import h2hdb.vnext_cleanup_repository as cleanup
from h2hdb import CoreConfig, VNextCurrentOnlyMaintenanceOutcome, VNextIngestFacade
from h2hdb.config_loader import LoggerConfig
from h2hdb.settings import LOG_LEVEL
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_cleanup_repository import CleanupTargetKind, VNextCleanupRepository
from h2hdb.vnext_maintenance_gate_repository import (
    LockedGateRenewal,
    MaintenanceGateRepository,
)
from h2hdb.vnext_transaction import LockRank, VNextUnitOfWork

pytestmark = pytest.mark.cleanup_acceptance

_LOGGER = "h2hdb.database_performance"
_DURATION = 100_000


@dataclass
class _Clock:
    now: int = 100

    def __call__(self) -> int:
        return self.now


def _seed(config: CoreConfig, *, rows: int, max_rows: int = 256) -> CoreConfig:
    initialize_database(config)
    with closing(open_connector(config)) as connector:
        with connector.transaction():
            for ordinal in range(rows):
                connector.execute(
                    "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
                    "VALUES (%s, 0)",
                    (bytes((201,)) + ordinal.to_bytes(31, "big"),),
                )
            work = VNextUnitOfWork(connector, backend=backend_of(config))
            lease = MaintenanceGateRepository.claim_exclusive(
                work, now=1, lease_duration=_DURATION
            )
        with connector.transaction():
            VNextCleanupRepository.begin_cycle(
                VNextUnitOfWork(connector, backend=backend_of(config)),
                gate_lease=lease,
                target_kind=CleanupTargetKind.CONTENT_BLOB,
                shard_no=201,
                cycle_cutoff_at=100,
                max_rows_per_transaction=max_rows,
                now=2,
            )
        with connector.transaction():
            MaintenanceGateRepository.release(
                VNextUnitOfWork(connector, backend=backend_of(config)), lease, now=3
            )
    return config.model_copy(update={"logger": LoggerConfig(level=LOG_LEVEL.debug)})


def _events(caplog: pytest.LogCaptureFixture) -> list[dict[str, Any]]:
    prefix = "database_performance "
    return [
        json.loads(record.getMessage()[len(prefix) :])
        for record in caplog.records
        if record.name == _LOGGER and record.getMessage().startswith(prefix)
    ]


def _phase(events: list[dict[str, Any]], name: str) -> list[dict[str, Any]]:
    return [
        item for item in events if item["event"] == "phase" and item["phase"] == name
    ]


def _summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    return [
        item
        for item in events
        if item["event"] in {"completed", "failed", "interrupted"}
        and item["operation"] == "current_only_cleanup"
    ][-1]


@pytest.mark.parametrize("rows", [255, 256, 257])
def test_batch_boundary_reports_executed_phase_and_committed_logical_rows(
    db_config: CoreConfig, caplog: pytest.LogCaptureFixture, rows: int
) -> None:
    config = _seed(db_config, rows=rows)
    with caplog.at_level(logging.DEBUG, logger=_LOGGER):
        with VNextIngestFacade(config, clock=_Clock()) as facade:
            assert facade.drain_current_only_maintenance(_DURATION) is (
                VNextCurrentOnlyMaintenanceOutcome.DONE
            )
    events = _events(caplog)
    batches = _phase(events, "cleanup_batch")
    assert len(batches) == (rows + 255) // 256 + 1
    assert sum(item["labels"]["committed_logical_rows"] for item in batches) == rows
    assert all(item["labels"]["transaction_outcome"] == "committed" for item in batches)
    assert all(item["labels"]["committed_logical_rows"] <= 256 for item in batches)
    phases = _phase(events, "cleanup_phase")
    assert len(phases) == len(batches)
    assert all(item["labels"]["phase"] == "CB_ROOT" for item in phases)
    assert phases[-1]["labels"]["cycle_complete"] is True
    assert phases[-1]["labels"]["attempted_logical_rows"] == 0
    assert all(item["labels"]["target"] == "CONTENT_BLOB" for item in phases)
    assert all(item["labels"]["shard"] == 201 for item in phases)
    summary = _summary(events)
    assert summary["labels"]["committed_logical_rows"] == rows
    assert summary["labels"]["committed_batches"] == len(batches)
    assert summary["labels"]["outcome"] == "DONE"
    assert summary["sql_calls"] > 0
    assert all(item["sql_calls"] > 0 for item in batches)
    assert "SELECT " not in caplog.text and "INSERT INTO " not in caplog.text
    assert (bytes((201,)) + bytes(31)).hex() not in caplog.text


def test_failed_mutation_records_unconfirmed_batch_without_committed_rows(
    db_config: CoreConfig,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _seed(db_config, rows=3)
    original = cleanup._advance_checkpoint

    def fail_after_delete(*args: Any, **kwargs: Any) -> Any:
        original(*args, **kwargs)
        raise RuntimeError("private source path must not appear in diagnostic payload")

    monkeypatch.setattr(cleanup, "_advance_checkpoint", fail_after_delete)
    with caplog.at_level(logging.DEBUG, logger=_LOGGER):
        with VNextIngestFacade(config, clock=_Clock()) as facade:
            with pytest.raises(RuntimeError, match="private source"):
                facade.drain_current_only_maintenance(_DURATION)
    events = _events(caplog)
    batch = _phase(events, "cleanup_batch")[0]
    assert batch["status"] == "failed"
    assert batch["labels"]["transaction_outcome"] == "unconfirmed"
    assert batch["labels"]["committed_logical_rows"] == 0
    assert _summary(events)["labels"]["committed_logical_rows"] == 0
    with closing(open_connector(config)) as connector, connector.read_transaction():
        assert connector.fetch_one("SELECT COUNT(*) FROM catalog_content_blobs") == (3,)
    assert "private source path" not in caplog.text


def test_committed_progress_remains_visible_when_following_lease_expires(
    db_config: CoreConfig,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _seed(db_config, rows=3, max_rows=1)
    clock = _Clock()
    original = VNextCleanupRepository.advance_current_only_cycle
    first = True

    def slow_batch(*args: Any, **kwargs: Any) -> Any:
        nonlocal first
        result = original(*args, **kwargs)
        if first:
            first = False
            clock.now += _DURATION
        return result

    monkeypatch.setattr(
        VNextCleanupRepository, "advance_current_only_cycle", slow_batch
    )
    with caplog.at_level(logging.DEBUG, logger=_LOGGER):
        with VNextIngestFacade(config, clock=clock) as facade:
            assert facade.drain_current_only_maintenance(_DURATION) is (
                VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
            )
    events = _events(caplog)
    summary = _summary(events)
    assert summary["labels"]["committed_logical_rows"] == 1
    assert summary["labels"]["outcome"] == "PROGRESSED"
    renewal = _phase(events, "gate_renew")[0]
    assert renewal["labels"]["lease_remaining_us"] == 0
    assert renewal["status"] == "failed"
    assert renewal["labels"]["transaction_outcome"] == "unconfirmed"


def test_gate_remaining_time_uses_same_fresh_post_lock_clock_as_authorization(
    db_config: CoreConfig,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _seed(db_config, rows=1)
    clock = _Clock()
    original_lock = VNextUnitOfWork.lock_rows
    original_live = LockedGateRenewal.require_live
    admitted: list[int] = []

    def wait(self: VNextUnitOfWork, *args: Any, **kwargs: Any) -> Any:
        result = original_lock(self, *args, **kwargs)
        if args[0] is LockRank.MAINTENANCE_GATE:
            clock.now += 7
        return result

    def live(self: LockedGateRenewal, *, now: int) -> Any:
        result = original_live(self, now=now)
        admitted.append(result.lease_expires_at - now)
        return result

    monkeypatch.setattr(VNextUnitOfWork, "lock_rows", wait)
    monkeypatch.setattr(LockedGateRenewal, "require_live", live)
    with caplog.at_level(logging.DEBUG, logger=_LOGGER):
        with VNextIngestFacade(config, clock=clock) as facade:
            assert facade.drain_current_only_maintenance(_DURATION) is (
                VNextCurrentOnlyMaintenanceOutcome.DONE
            )
    events = _events(caplog)
    validated = _phase(events, "gate_validate") + _phase(events, "gate_release")
    assert sorted(item["labels"]["lease_remaining_us"] for item in validated) == sorted(
        admitted
    )
    assert _phase(events, "gate_claim")[0]["labels"]["lease_remaining_us"] == _DURATION
    assert all(item["labels"]["lease_remaining_us"] < _DURATION for item in validated)
    assert all(item["elapsed_seconds"] >= 0 for item in _phase(events, "gate_lock"))


def test_no_synchronous_diagnostic_handler_runs_inside_transaction(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _seed(db_config, rows=3)
    transaction = SQLConnector.transaction
    read_transaction = SQLConnector.read_transaction
    depth = 0
    emission_depths: list[int] = []

    @contextmanager
    def observed_transaction(self: SQLConnector) -> Any:
        nonlocal depth
        depth += 1
        try:
            with transaction(self):
                yield
        finally:
            depth -= 1

    @contextmanager
    def observed_read_transaction(self: SQLConnector) -> Any:
        nonlocal depth
        depth += 1
        try:
            with read_transaction(self):
                yield
        finally:
            depth -= 1

    class Observer(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if record.name == _LOGGER:
                emission_depths.append(depth)

    monkeypatch.setattr(SQLConnector, "transaction", observed_transaction)
    monkeypatch.setattr(SQLConnector, "read_transaction", observed_read_transaction)
    logger = logging.getLogger(_LOGGER)
    handler = Observer()
    logger.addHandler(handler)
    prior = logger.level
    logger.setLevel(logging.DEBUG)
    try:
        with VNextIngestFacade(config, clock=_Clock()) as facade:
            assert facade.drain_current_only_maintenance(_DURATION) is (
                VNextCurrentOnlyMaintenanceOutcome.DONE
            )
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prior)
    assert emission_depths and not any(emission_depths)


def test_commit_response_loss_never_claims_confirmed_deletion(
    db_config: CoreConfig,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _seed(db_config, rows=3)
    with closing(open_connector(config)) as connector:
        connector_type = type(connector)
    commit = connector_type.commit
    advance = VNextCleanupRepository.advance_current_only_cycle
    fail_commit = False

    def completed_advance(*args: Any, **kwargs: Any) -> Any:
        nonlocal fail_commit
        result = advance(*args, **kwargs)
        fail_commit = True
        return result

    def lost_response(self: SQLConnector) -> None:
        nonlocal fail_commit
        commit(self)
        if fail_commit:
            fail_commit = False
            raise RuntimeError("injected lost COMMIT response")

    monkeypatch.setattr(
        VNextCleanupRepository, "advance_current_only_cycle", completed_advance
    )
    monkeypatch.setattr(connector_type, "commit", lost_response)
    with caplog.at_level(logging.DEBUG, logger=_LOGGER):
        with VNextIngestFacade(config, clock=_Clock()) as facade:
            with pytest.raises(RuntimeError, match="lost COMMIT response"):
                facade.drain_current_only_maintenance(_DURATION)
    events = _events(caplog)
    batch = _phase(events, "cleanup_batch")[0]
    assert batch["status"] == "failed"
    assert batch["labels"]["transaction_outcome"] == "unconfirmed"
    assert batch["labels"]["committed_logical_rows"] == 0
    assert _phase(events, "cleanup_phase")[0]["labels"]["attempted_logical_rows"] == 3
    assert _summary(events)["labels"]["committed_logical_rows"] == 0
    # The server did commit. An unknown client outcome must not be relabeled
    # rollback or confirmed deletion merely from the phase's successful return.
    with closing(open_connector(config)) as connector, connector.read_transaction():
        assert connector.fetch_one("SELECT COUNT(*) FROM catalog_content_blobs") == (0,)


@pytest.mark.parametrize("rows", [15 * 256, 16 * 256, 16 * 256 + 1])
def test_attempt_boundary_and_repeated_cycles_have_exact_committed_costs(
    db_config: CoreConfig, caplog: pytest.LogCaptureFixture, rows: int
) -> None:
    config = _seed(db_config, rows=rows)
    with caplog.at_level(logging.DEBUG, logger=_LOGGER):
        with VNextIngestFacade(config, clock=_Clock()) as facade:
            first = facade.drain_current_only_maintenance(_DURATION)
            assert first is (
                VNextCurrentOnlyMaintenanceOutcome.DONE
                if rows == 15 * 256
                else VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
            )
            assert facade.drain_current_only_maintenance(_DURATION) is (
                VNextCurrentOnlyMaintenanceOutcome.DONE
            )
            # Repeating the same durable source keys starts another cleanup
            # cycle; per-attempt diagnostic counters cannot leak prior work.
            with closing(open_connector(config)) as connector, connector.transaction():
                connector.execute(
                    "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) VALUES (%s, 0)",
                    (bytes((201,)) + bytes(31),),
                )
            assert facade.drain_current_only_maintenance(_DURATION) is (
                VNextCurrentOnlyMaintenanceOutcome.DONE
            )
    summaries = [
        item
        for item in _events(caplog)
        if item["event"] == "completed" and item["operation"] == "current_only_cleanup"
    ]
    assert len(summaries) == 3
    assert summaries[0]["labels"]["committed_batches"] == 16
    assert summaries[0]["labels"]["committed_logical_rows"] == min(rows, 16 * 256)
    assert (
        sum(item["labels"]["committed_logical_rows"] for item in summaries) == rows + 1
    )
    assert summaries[-1]["labels"]["committed_logical_rows"] == 1
    assert len({item["operation_id"] for item in summaries}) == 3
    assert all(item["labels"]["committed_batches"] <= 16 for item in summaries)
