import sqlite3
from collections.abc import Iterator
from threading import Event, Thread
from time import monotonic

import pytest

import h2hdb.__main__ as main_module
from h2hdb import (
    H2HDB,
    DatabaseMaintenanceResult,
    GalleryIngestPhase,
    H2HDBConfig,
    SyncOutcome,
)
from h2hdb.__main__ import process_available_gallery_ingest
from h2hdb.sql_connector import DatabaseConfigurationError
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.table_gallery_ingest_coordination import GalleryIngestTurn


@pytest.fixture
def db(sqlite_config: H2HDBConfig) -> Iterator[H2HDB]:
    instance = H2HDB(config=sqlite_config)
    with instance:
        instance.create_main_tables()
        yield instance


def _complete_baseline_ingest(db: H2HDB) -> None:
    turn = db._claim_gallery_ingest(
        lease_seconds=60,
        periodic_scan=False,
    )
    assert turn is not None
    assert db._complete_gallery_ingest(turn) is True


def _expire_active_lease(db: H2HDB) -> None:
    with db.SQLConnector() as connector:
        connector.execute("""
            UPDATE gallery_ingest_state
            SET lease_expires_at = 0
            WHERE state_id = 1
            """)


def _no_maintenance() -> DatabaseMaintenanceResult:
    return DatabaseMaintenanceResult(False, tuple(), 0)


def test_main_repeats_until_stable_and_completes_after_maintenance(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    download_turn = db.claim_download_turn(lease_seconds=60)
    assert download_turn is not None
    assert db.request_gallery_ingest(download_turn) is True

    outcomes = iter(
        [
            SyncOutcome(new_galleries=1, changed_galleries=0, removed_galleries=0),
            SyncOutcome(new_galleries=0, changed_galleries=0, removed_galleries=0),
        ]
    )
    reset_calls = 0
    maintenance_calls = 0

    def synchronize_once() -> SyncOutcome:
        assert db.get_gallery_ingest_state().phase == GalleryIngestPhase.ingesting
        return next(outcomes)

    def reset_redownload_times() -> None:
        nonlocal reset_calls
        reset_calls += 1

    def run_maintenance() -> DatabaseMaintenanceResult:
        nonlocal maintenance_calls
        maintenance_calls += 1
        state = db.get_gallery_ingest_state()
        assert state.phase == GalleryIngestPhase.ingesting
        assert state.completed_generation == 0
        return _no_maintenance()

    monkeypatch.setattr(db, "synchronize_once", synchronize_once)
    monkeypatch.setattr(db, "reset_redownload_times", reset_redownload_times)
    monkeypatch.setattr(db, "run_scheduled_database_maintenance", run_maintenance)

    assert (
        process_available_gallery_ingest(
            db,
            periodic_scan=False,
        )
        is True
    )
    assert reset_calls == 1
    assert maintenance_calls == 1
    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.ready
    assert state.completed_generation == download_turn.generation


def test_sqlite_heartbeat_retries_exclusive_lock_then_renews_and_acks(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    download_turn = db.claim_download_turn(lease_seconds=60)
    assert download_turn is not None
    assert db.request_gallery_ingest(download_turn) is True

    monkeypatch.setattr(main_module, "INGEST_HEARTBEAT_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(main_module, "SQLITE_HEARTBEAT_LOCK_WAIT_SECONDS", 0.02)
    monkeypatch.setattr(main_module, "SQLITE_HEARTBEAT_RETRY_PAUSE_SECONDS", 0.005)
    original_renew = db._renew_gallery_ingest_lease
    lock_acquired = Event()
    release_lock = Event()
    contention_seen = Event()
    renewed_after_release = Event()
    locker_errors: list[BaseException] = []

    def renew_ingest(
        turn: GalleryIngestTurn,
        *,
        lease_seconds: int,
        sqlite_busy_timeout_ms: int | None = None,
    ) -> int | None:
        try:
            result = original_renew(
                turn,
                lease_seconds=lease_seconds,
                sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
            )
        except sqlite3.OperationalError as error:
            assert int(error.sqlite_errorcode) & 0xFF in {
                sqlite3.SQLITE_BUSY,
                sqlite3.SQLITE_LOCKED,
            }
            contention_seen.set()
            raise
        if contention_seen.is_set() and result is not None:
            renewed_after_release.set()
        return result

    def hold_exclusive_lock() -> None:
        connection = sqlite3.connect(
            db.config.database.database,
            isolation_level=None,
        )
        try:
            connection.execute("BEGIN EXCLUSIVE")
            lock_acquired.set()
            if not release_lock.wait(timeout=2):
                raise AssertionError("exclusive-lock release was not requested")
            connection.rollback()
        except BaseException as error:
            locker_errors.append(error)
            lock_acquired.set()
        finally:
            connection.close()

    def synchronize_once() -> SyncOutcome:
        locker = Thread(target=hold_exclusive_lock)
        locker.start()
        try:
            assert lock_acquired.wait(timeout=1)
            assert not locker_errors
            assert contention_seen.wait(timeout=1)
            release_lock.set()
            assert renewed_after_release.wait(timeout=1)
        finally:
            release_lock.set()
            locker.join(timeout=1)
        assert locker.is_alive() is False
        assert not locker_errors
        return SyncOutcome(0, 0, 0)

    monkeypatch.setattr(db, "_renew_gallery_ingest_lease", renew_ingest)
    monkeypatch.setattr(db, "synchronize_once", synchronize_once)
    monkeypatch.setattr(db, "run_scheduled_database_maintenance", _no_maintenance)

    assert process_available_gallery_ingest(db, periodic_scan=False) is True
    assert contention_seen.is_set()
    assert renewed_after_release.is_set()
    assert (
        db.get_gallery_ingest_state().completed_generation == download_turn.generation
    )


def test_sqlite_sustained_exclusive_lock_expires_heartbeat_without_ack(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    download_turn = db.claim_download_turn(lease_seconds=60)
    assert download_turn is not None
    assert db.request_gallery_ingest(download_turn) is True

    monkeypatch.setattr(main_module, "INGEST_LEASE_SECONDS", 2)
    monkeypatch.setattr(main_module, "INGEST_HEARTBEAT_INTERVAL_SECONDS", 0.05)
    monkeypatch.setattr(main_module, "SQLITE_HEARTBEAT_LOCK_WAIT_SECONDS", 0.05)
    monkeypatch.setattr(main_module, "SQLITE_HEARTBEAT_RETRY_PAUSE_SECONDS", 0.005)
    captured_heartbeats: list[main_module.IngestLeaseHeartbeat] = []
    lock_acquired = Event()
    release_lock = Event()
    locker_errors: list[BaseException] = []

    class CapturingHeartbeat(main_module.IngestLeaseHeartbeat):
        def __enter__(self) -> main_module.IngestLeaseHeartbeat:
            captured_heartbeats.append(self)
            return super().__enter__()

    def hold_exclusive_lock() -> None:
        connection = sqlite3.connect(
            db.config.database.database,
            isolation_level=None,
        )
        try:
            connection.execute("BEGIN EXCLUSIVE")
            lock_acquired.set()
            if not release_lock.wait(timeout=3):
                raise AssertionError("exclusive-lock release was not requested")
            connection.rollback()
        except BaseException as error:
            locker_errors.append(error)
            lock_acquired.set()
        finally:
            connection.close()

    def synchronize_once() -> SyncOutcome:
        locker = Thread(target=hold_exclusive_lock)
        locker.start()
        try:
            assert lock_acquired.wait(timeout=1)
            assert not locker_errors
            heartbeat = captured_heartbeats[0]
            heartbeat._thread.join(timeout=2)
            assert heartbeat._thread.is_alive() is False
        finally:
            release_lock.set()
            locker.join(timeout=1)
        assert locker.is_alive() is False
        assert not locker_errors
        return SyncOutcome(0, 0, 0)

    def unexpected_maintenance() -> DatabaseMaintenanceResult:
        raise AssertionError("maintenance must not run after heartbeat expiry")

    monkeypatch.setattr(main_module, "IngestLeaseHeartbeat", CapturingHeartbeat)
    monkeypatch.setattr(db, "synchronize_once", synchronize_once)
    monkeypatch.setattr(
        db,
        "run_scheduled_database_maintenance",
        unexpected_maintenance,
    )

    with pytest.raises(DatabaseConfigurationError, match="expired"):
        process_available_gallery_ingest(db, periodic_scan=False)

    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.ingesting
    assert state.completed_generation == 0


def test_sqlite_optimized_maintenance_can_ack_same_expired_owner(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    download_turn = db.claim_download_turn(lease_seconds=60)
    assert download_turn is not None
    assert db.request_gallery_ingest(download_turn) is True

    def run_vacuum_like_maintenance() -> DatabaseMaintenanceResult:
        connection = sqlite3.connect(
            db.config.database.database,
            isolation_level=None,
        )
        try:
            connection.execute("BEGIN EXCLUSIVE")
            connection.execute("""
                UPDATE gallery_ingest_state
                SET lease_expires_at = 0
                WHERE state_id = 1
                """)
            connection.commit()
        finally:
            connection.close()
        return DatabaseMaintenanceResult(True, ("main",), 0)

    monkeypatch.setattr(db, "synchronize_once", lambda: SyncOutcome(0, 0, 0))
    monkeypatch.setattr(
        db,
        "run_scheduled_database_maintenance",
        run_vacuum_like_maintenance,
    )

    assert process_available_gallery_ingest(db, periodic_scan=False) is True
    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.ready
    assert state.completed_generation == download_turn.generation


def test_sqlite_replacement_owner_wins_after_optimized_maintenance(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    download_turn = db.claim_download_turn(lease_seconds=60)
    assert download_turn is not None
    assert db.request_gallery_ingest(download_turn) is True
    replacement_turns: list[GalleryIngestTurn] = []

    def run_vacuum_like_maintenance() -> DatabaseMaintenanceResult:
        connection = sqlite3.connect(
            db.config.database.database,
            isolation_level=None,
        )
        try:
            connection.execute("BEGIN EXCLUSIVE")
            connection.execute("""
                UPDATE gallery_ingest_state
                SET lease_expires_at = 0
                WHERE state_id = 1
                """)
            connection.commit()
        finally:
            connection.close()
        replacement = db._claim_gallery_ingest(
            lease_seconds=60,
            periodic_scan=False,
        )
        assert replacement is not None
        replacement_turns.append(replacement)
        return DatabaseMaintenanceResult(True, ("main",), 0)

    monkeypatch.setattr(db, "synchronize_once", lambda: SyncOutcome(0, 0, 0))
    monkeypatch.setattr(
        db,
        "run_scheduled_database_maintenance",
        run_vacuum_like_maintenance,
    )

    with pytest.raises(DatabaseConfigurationError, match="ownership was lost"):
        process_available_gallery_ingest(db, periodic_scan=False)

    state = db.get_gallery_ingest_state()
    assert replacement_turns
    assert state.phase == GalleryIngestPhase.ingesting
    assert state.owner_token == replacement_turns[0].owner_token
    assert state.completed_generation == 0


def test_lost_ingest_heartbeat_prevents_ack(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    download_turn = db.claim_download_turn(lease_seconds=60)
    assert download_turn is not None
    assert db.request_gallery_ingest(download_turn) is True

    monkeypatch.setattr(main_module, "INGEST_HEARTBEAT_INTERVAL_SECONDS", 0.01)
    captured_heartbeats: list[main_module.IngestLeaseHeartbeat] = []

    class CapturingHeartbeat(main_module.IngestLeaseHeartbeat):
        def __enter__(self) -> main_module.IngestLeaseHeartbeat:
            captured_heartbeats.append(self)
            return super().__enter__()

    def synchronize_once() -> SyncOutcome:
        heartbeat = captured_heartbeats[0]
        heartbeat._thread.join(timeout=1)
        assert heartbeat._thread.is_alive() is False
        return SyncOutcome(0, 0, 0)

    monkeypatch.setattr(main_module, "IngestLeaseHeartbeat", CapturingHeartbeat)
    monkeypatch.setattr(
        db,
        "_renew_gallery_ingest_lease",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(db, "synchronize_once", synchronize_once)

    with pytest.raises(DatabaseConfigurationError, match="ownership was lost"):
        process_available_gallery_ingest(db, periodic_scan=False)

    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.ingesting
    assert state.completed_generation == 0


def test_mariadb_heartbeat_exception_is_not_retried(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    turn = GalleryIngestTurn(
        generation=1,
        owner_token="owner",
        lease_expires_at=60,
        claimed_from_phase=GalleryIngestPhase.ingest_requested,
    )
    renewal_calls = 0

    def fail_renewal(*args: object, **kwargs: object) -> int | None:
        nonlocal renewal_calls
        renewal_calls += 1
        error = sqlite3.OperationalError("database is locked")
        error.sqlite_errorcode = sqlite3.SQLITE_BUSY
        raise error

    monkeypatch.setattr(db.config.database, "sql_type", "mariadb")
    monkeypatch.setattr(db, "_renew_gallery_ingest_lease", fail_renewal)
    heartbeat = main_module.IngestLeaseHeartbeat(
        db,
        turn,
        lease_seconds=60,
        interval_seconds=1,
        lease_deadline_monotonic=monotonic() + 59,
    )

    assert heartbeat._renew_with_retry() is False
    assert renewal_calls == 1
    with pytest.raises(DatabaseConfigurationError, match="heartbeat failed"):
        heartbeat.raise_if_failed()


def test_main_treats_sqlite_exclusive_lock_during_claim_as_unavailable(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    original_connect = SQLiteConnector.connect
    sync_calls = 0

    def connect_with_short_busy_timeout(connector: SQLiteConnector) -> None:
        original_connect(connector)
        connector.connection.execute("PRAGMA busy_timeout = 10")

    def synchronize_once() -> SyncOutcome:
        nonlocal sync_calls
        sync_calls += 1
        return SyncOutcome(0, 0, 0)

    monkeypatch.setattr(SQLiteConnector, "connect", connect_with_short_busy_timeout)
    monkeypatch.setattr(db, "synchronize_once", synchronize_once)
    monkeypatch.setattr(db, "run_scheduled_database_maintenance", _no_maintenance)

    lock_connection = sqlite3.connect(
        db.config.database.database,
        isolation_level=None,
    )
    try:
        lock_connection.execute("BEGIN EXCLUSIVE")
        assert process_available_gallery_ingest(db, periodic_scan=True) is False
        assert sync_calls == 0
    finally:
        lock_connection.rollback()
        lock_connection.close()

    assert process_available_gallery_ingest(db, periodic_scan=True) is True
    assert sync_calls == 1


def test_main_does_not_swallow_sqlite_busy_after_claim(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)

    def fail_synchronization() -> SyncOutcome:
        error = sqlite3.OperationalError("database is locked")
        error.sqlite_errorcode = sqlite3.SQLITE_BUSY
        raise error

    monkeypatch.setattr(db, "synchronize_once", fail_synchronization)

    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        process_available_gallery_ingest(db, periodic_scan=True)

    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.ingesting
    assert state.completed_generation == 0


def test_main_runs_ready_periodic_scan_only_when_due(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    sync_calls = 0

    def synchronize_once() -> SyncOutcome:
        nonlocal sync_calls
        sync_calls += 1
        return SyncOutcome(0, 0, 0)

    monkeypatch.setattr(db, "synchronize_once", synchronize_once)
    monkeypatch.setattr(
        db,
        "run_scheduled_database_maintenance",
        _no_maintenance,
    )

    assert (
        process_available_gallery_ingest(
            db,
            periodic_scan=False,
        )
        is False
    )
    assert (
        process_available_gallery_ingest(
            db,
            periodic_scan=True,
        )
        is True
    )
    assert sync_calls == 1
    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.ready
    assert state.completed_generation == 0


def test_main_does_not_scan_while_download_lease_is_fresh(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    assert db.claim_download_turn(lease_seconds=60) is not None

    def unexpected_synchronize_once() -> SyncOutcome:
        raise AssertionError("fresh DOWNLOADING must block ingestion")

    monkeypatch.setattr(db, "synchronize_once", unexpected_synchronize_once)

    assert (
        process_available_gallery_ingest(
            db,
            periodic_scan=True,
        )
        is False
    )
    assert db.get_gallery_ingest_state().phase == GalleryIngestPhase.downloading


def test_resident_loop_uses_short_poll_while_periodic_scan_is_blocked(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    assert db.claim_download_turn(lease_seconds=60) is not None
    sleep_calls: list[float] = []

    def stop_after_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        raise RuntimeError("stop resident loop")

    monkeypatch.setattr(main_module, "monotonic", lambda: 0.0)
    monkeypatch.setattr(main_module, "sleep", stop_after_sleep)

    with pytest.raises(RuntimeError, match="stop resident loop"):
        main_module.run_resident_loop(db)

    assert sleep_calls == [main_module.COORDINATION_POLL_INTERVAL_SECONDS]


def test_main_recovers_expired_download_turn(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    download_turn = db.claim_download_turn(lease_seconds=60)
    assert download_turn is not None
    _expire_active_lease(db)

    monkeypatch.setattr(
        db,
        "synchronize_once",
        lambda: SyncOutcome(0, 0, 0),
    )
    monkeypatch.setattr(
        db,
        "run_scheduled_database_maintenance",
        _no_maintenance,
    )

    assert (
        process_available_gallery_ingest(
            db,
            periodic_scan=False,
        )
        is True
    )
    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.ready
    assert state.completed_generation == download_turn.generation


def test_main_sync_exception_does_not_ack_generation(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    download_turn = db.claim_download_turn(lease_seconds=60)
    assert download_turn is not None
    assert db.request_gallery_ingest(download_turn) is True

    def fail_synchronization() -> SyncOutcome:
        raise RuntimeError("injected synchronization failure")

    monkeypatch.setattr(db, "synchronize_once", fail_synchronization)

    with pytest.raises(RuntimeError, match="injected synchronization failure"):
        process_available_gallery_ingest(
            db,
            periodic_scan=False,
        )

    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.ingesting
    assert state.completed_generation == 0


def test_main_maintenance_exception_does_not_ack_generation(
    db: H2HDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _complete_baseline_ingest(db)
    download_turn = db.claim_download_turn(lease_seconds=60)
    assert download_turn is not None
    assert db.request_gallery_ingest(download_turn) is True

    monkeypatch.setattr(
        db,
        "synchronize_once",
        lambda: SyncOutcome(0, 0, 0),
    )

    def fail_maintenance() -> object:
        raise RuntimeError("injected maintenance failure")

    monkeypatch.setattr(db, "run_scheduled_database_maintenance", fail_maintenance)

    with pytest.raises(RuntimeError, match="injected maintenance failure"):
        process_available_gallery_ingest(
            db,
            periodic_scan=False,
        )

    state = db.get_gallery_ingest_state()
    assert state.phase == GalleryIngestPhase.ingesting
    assert state.completed_generation == 0
