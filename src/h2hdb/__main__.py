import sqlite3
from threading import Event, Lock, Thread
from time import monotonic, sleep
from types import TracebackType

from h2hdb import H2HDB

from .config_loader import ensure_download_path_ready, load_config
from .sql_connector import DatabaseConfigurationError
from .table_gallery_ingest_coordination import GalleryIngestTurn

SLEEP_INTERVAL_SECONDS = 1800
COORDINATION_POLL_INTERVAL_SECONDS = 5
INGEST_LEASE_SECONDS = 300
INGEST_HEARTBEAT_INTERVAL_SECONDS = 60.0
DATABASE_CLOCK_GRANULARITY_SECONDS = 1.0
SQLITE_HEARTBEAT_LOCK_WAIT_SECONDS = 1.0
SQLITE_HEARTBEAT_RETRY_PAUSE_SECONDS = 0.05


def _is_sqlite_lock_contention(
    connector: H2HDB,
    error: BaseException,
) -> bool:
    if connector.config.database.sql_type.lower() != "sqlite":
        return False
    if not isinstance(error, sqlite3.OperationalError):
        return False
    error_code = getattr(error, "sqlite_errorcode", None)
    if error_code is None:
        return False
    primary_error_code = int(error_code) & 0xFF
    return primary_error_code in {
        sqlite3.SQLITE_BUSY,
        sqlite3.SQLITE_LOCKED,
    }


class IngestLeaseHeartbeat:
    def __init__(
        self,
        connector: H2HDB,
        turn: GalleryIngestTurn,
        *,
        lease_seconds: int = INGEST_LEASE_SECONDS,
        interval_seconds: float | None = None,
        lease_deadline_monotonic: float,
    ) -> None:
        self.connector = connector
        self.turn = turn
        self.lease_seconds = lease_seconds
        self.interval_seconds = (
            INGEST_HEARTBEAT_INTERVAL_SECONDS
            if interval_seconds is None
            else interval_seconds
        )
        self._safe_lease_duration = (
            self.lease_seconds - DATABASE_CLOCK_GRANULARITY_SECONDS
        )
        if self.interval_seconds <= 0 or self.interval_seconds >= self.lease_seconds:
            raise ValueError(
                "Ingest heartbeat interval must be greater than zero and "
                "shorter than the lease."
            )
        if self._safe_lease_duration <= 0:
            raise ValueError("Ingest lease must exceed the database clock granularity.")
        if self.interval_seconds >= self._safe_lease_duration:
            raise ValueError(
                "Ingest heartbeat interval must be shorter than the safe "
                "lease duration."
            )
        self._lease_deadline_monotonic = lease_deadline_monotonic
        self._stop = Event()
        self._renew_lock = Lock()
        self._failure: BaseException | None = None
        self._thread = Thread(
            target=self._run,
            name="h2hdb-ingest-heartbeat",
            daemon=True,
        )

    def __enter__(self) -> IngestLeaseHeartbeat:
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._stop.set()
        self._thread.join()
        if exc_type is None:
            self.raise_if_failed()

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            if not self._renew_with_retry():
                return

    def renew_now(self) -> None:
        if not self._renew_with_retry():
            self.raise_if_failed()

    def _renew_with_retry(self) -> bool:
        with self._renew_lock:
            while not self._stop.is_set():
                remaining_seconds = self._lease_deadline_monotonic - monotonic()
                if remaining_seconds <= 0:
                    self._failure = DatabaseConfigurationError(
                        "Gallery ingest lease expired while waiting to renew."
                    )
                    return False

                attempt_started = monotonic()
                sqlite_busy_timeout_ms = self._sqlite_busy_timeout_ms(remaining_seconds)
                try:
                    lease_expires_at = self.connector._renew_gallery_ingest_lease(
                        self.turn,
                        lease_seconds=self.lease_seconds,
                        sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
                    )
                except BaseException as error:
                    if not _is_sqlite_lock_contention(self.connector, error):
                        self._failure = error
                        return False
                    remaining_seconds = self._lease_deadline_monotonic - monotonic()
                    if remaining_seconds <= 0:
                        self._failure = DatabaseConfigurationError(
                            "Gallery ingest lease expired during SQLite lock "
                            "contention."
                        )
                        return False
                    self._stop.wait(
                        min(
                            SQLITE_HEARTBEAT_RETRY_PAUSE_SECONDS,
                            remaining_seconds,
                        )
                    )
                    continue

                if lease_expires_at is None:
                    self._failure = DatabaseConfigurationError(
                        "Gallery ingest turn ownership was lost while its "
                        "heartbeat was running."
                    )
                    return False
                self._lease_deadline_monotonic = (
                    attempt_started + self._safe_lease_duration
                )
                return True
        return False

    def _sqlite_busy_timeout_ms(
        self,
        remaining_seconds: float,
    ) -> int | None:
        if self.connector.config.database.sql_type.lower() != "sqlite":
            return None
        wait_seconds = min(
            SQLITE_HEARTBEAT_LOCK_WAIT_SECONDS,
            remaining_seconds,
        )
        if wait_seconds < 0.001:
            return 0
        return int(wait_seconds * 1000)

    def raise_if_failed(self) -> None:
        if self._failure is None:
            return
        if isinstance(self._failure, DatabaseConfigurationError):
            raise self._failure
        raise DatabaseConfigurationError(
            "Gallery ingest lease heartbeat failed."
        ) from self._failure


def process_available_gallery_ingest(
    connector: H2HDB,
    *,
    periodic_scan: bool,
) -> bool:
    claim_started_at = monotonic()
    try:
        turn = connector._claim_gallery_ingest(
            lease_seconds=INGEST_LEASE_SECONDS,
            periodic_scan=periodic_scan,
        )
    except sqlite3.OperationalError as error:
        if not _is_sqlite_lock_contention(connector, error):
            raise
        connector.logger.debug(
            "Gallery ingest claim is temporarily unavailable because another "
            "SQLite connection holds the database lock."
        )
        return False
    if turn is None:
        return False

    connector.logger.info(
        "Gallery ingest turn claimed: "
        f"generation={turn.generation} "
        f"trigger={turn.claimed_from_phase.value}."
    )
    safe_lease_deadline = (
        claim_started_at + INGEST_LEASE_SECONDS - DATABASE_CLOCK_GRANULARITY_SECONDS
    )
    is_sqlite = connector.config.database.sql_type.lower() == "sqlite"
    maintenance_result = None
    with IngestLeaseHeartbeat(
        connector,
        turn,
        lease_seconds=INGEST_LEASE_SECONDS,
        lease_deadline_monotonic=safe_lease_deadline,
    ) as heartbeat:
        while True:
            heartbeat.raise_if_failed()
            outcome = connector.synchronize_once()
            heartbeat.raise_if_failed()
            if not outcome.needs_immediate_rescan:
                break
            connector.reset_redownload_times()
            connector.logger.info(
                "Gallery insertions or metadata changes detected; "
                "starting another scan immediately."
            )

        if is_sqlite:
            # Establish a fresh lease immediately before maintenance, then
            # stop the heartbeat. SQLite VACUUM owns an exclusive database
            # lock, which itself fences every competing state transition.
            heartbeat.renew_now()
        else:
            # Keep the downloader paused through MariaDB maintenance while the
            # heartbeat continues on its independent short connection.
            maintenance_result = connector.run_scheduled_database_maintenance()
            heartbeat.raise_if_failed()

    if is_sqlite:
        maintenance_result = connector.run_scheduled_database_maintenance()

    assert maintenance_result is not None
    allow_expired_sqlite_lease = is_sqlite and maintenance_result.optimized
    if not connector._complete_gallery_ingest(
        turn,
        allow_expired_sqlite_lease=allow_expired_sqlite_lease,
    ):
        raise DatabaseConfigurationError(
            "Gallery ingest turn ownership was lost before completion."
        )
    connector.logger.info(
        f"Gallery ingest turn completed: generation={turn.generation}."
    )
    return True


def run_resident_loop(connector: H2HDB) -> None:
    next_periodic_scan_at = monotonic()
    while True:
        periodic_scan = monotonic() >= next_periodic_scan_at
        cycle_start_time = monotonic()
        if process_available_gallery_ingest(
            connector,
            periodic_scan=periodic_scan,
        ):
            next_periodic_scan_at = cycle_start_time + SLEEP_INTERVAL_SECONDS
            remaining_seconds = max(0.0, next_periodic_scan_at - monotonic())
            connector.logger.info(
                "Waiting for a downloader ingest request or the next periodic "
                f"scan in {remaining_seconds:.0f} seconds..."
            )
            continue

        seconds_until_periodic_scan = next_periodic_scan_at - monotonic()
        sleep_seconds = (
            COORDINATION_POLL_INTERVAL_SECONDS
            if seconds_until_periodic_scan <= 0
            else min(
                COORDINATION_POLL_INTERVAL_SECONDS,
                seconds_until_periodic_scan,
            )
        )
        sleep(sleep_seconds)


if __name__ == "__main__":
    config = load_config()
    ensure_download_path_ready(config.h2h.download_path)
    with H2HDB(config=config) as connector:
        connector.check_database_character_set()
        connector.check_database_collation()
        connector.create_main_tables()
        run_resident_loop(connector)
