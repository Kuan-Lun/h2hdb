from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from secrets import randbelow
from time import time

from .repository import BaseRepository, RepositoryContext
from .sql_connector import DatabaseConfigurationError, SQLConnector
from .table_database_setting import (
    H2HDBCheckDatabaseSettings,
    ReclaimableSpace,
)

MAINTENANCE_STATE_ID = 1
MAINTENANCE_STATE_TABLE = "database_maintenance_state"
DATABASE_GATE_SLOT_COUNT = 64


@dataclass(frozen=True, slots=True)
class DatabaseMaintenanceState:
    accumulated_work: int
    last_evaluated_at: int | None
    last_optimized_at: int | None


@dataclass(frozen=True, slots=True)
class DatabaseMaintenanceResult:
    evaluated: bool
    optimized_targets: tuple[str, ...]
    accumulated_work: int

    @property
    def optimized(self) -> bool:
        return bool(self.optimized_targets)


class H2HDBDatabaseMaintenance(BaseRepository):
    def __init__(
        self,
        context: RepositoryContext,
        database_settings: H2HDBCheckDatabaseSettings,
    ) -> None:
        super().__init__(context)
        self.database_settings = database_settings

    def _create_database_maintenance_state_table(self) -> None:
        match self.config.database.sql_type.lower():
            case "mariadb":
                create_query = f"""
                    CREATE TABLE IF NOT EXISTS {MAINTENANCE_STATE_TABLE} (
                        state_id TINYINT UNSIGNED NOT NULL,
                        accumulated_work BIGINT UNSIGNED NOT NULL DEFAULT 0,
                        last_evaluated_at BIGINT UNSIGNED NULL,
                        last_optimized_at BIGINT UNSIGNED NULL,
                        PRIMARY KEY (state_id),
                        CONSTRAINT database_maintenance_singleton
                            CHECK (state_id = {MAINTENANCE_STATE_ID})
                    )
                """
                seed_query = f"""
                    INSERT IGNORE INTO {MAINTENANCE_STATE_TABLE} (state_id)
                    VALUES (%s)
                """
            case "sqlite":
                create_query = f"""
                    CREATE TABLE IF NOT EXISTS {MAINTENANCE_STATE_TABLE} (
                        state_id INTEGER NOT NULL PRIMARY KEY
                            CHECK (state_id = {MAINTENANCE_STATE_ID}),
                        accumulated_work INTEGER NOT NULL DEFAULT 0
                            CHECK (accumulated_work >= 0),
                        last_evaluated_at INTEGER NULL,
                        last_optimized_at INTEGER NULL
                    )
                """
                seed_query = f"""
                    INSERT OR IGNORE INTO {MAINTENANCE_STATE_TABLE} (state_id)
                    VALUES (%s)
                """
            case _:
                raise ValueError("Unsupported SQL type")

        with self.SQLConnector() as connector:
            connector.execute(create_query)
            connector.execute(seed_query, (MAINTENANCE_STATE_ID,))
        self.logger.debug(
            f"Ensured database table exists: name={MAINTENANCE_STATE_TABLE}."
        )

    def get_state(self) -> DatabaseMaintenanceState:
        with self.SQLConnector() as connector:
            row = connector.fetch_one(
                f"""
                SELECT accumulated_work, last_evaluated_at, last_optimized_at
                FROM {MAINTENANCE_STATE_TABLE}
                WHERE state_id = %s
                """,
                (MAINTENANCE_STATE_ID,),
            )
        if not row:
            raise DatabaseConfigurationError(
                "Database maintenance state is missing; run the h2hdb schema "
                "migration command."
            )
        return DatabaseMaintenanceState(
            accumulated_work=int(row[0]),
            last_evaluated_at=int(row[1]) if row[1] is not None else None,
            last_optimized_at=int(row[2]) if row[2] is not None else None,
        )

    def record_gallery_changes(
        self, *, changed_galleries: int, removed_galleries: int
    ) -> None:
        with self.SQLConnector() as connector:
            self._record_gallery_changes_with_connector(
                connector,
                changed_galleries=changed_galleries,
                removed_galleries=removed_galleries,
            )

    def _record_gallery_changes_with_connector(
        self,
        connector: SQLConnector,
        *,
        changed_galleries: int,
        removed_galleries: int,
    ) -> None:
        work = changed_galleries + removed_galleries
        if work <= 0:
            return
        connector.execute(
            f"""
            UPDATE {MAINTENANCE_STATE_TABLE}
            SET accumulated_work = accumulated_work + %s
            WHERE state_id = %s
            """,
            (work, MAINTENANCE_STATE_ID),
        )
        self.logger.debug(
            "Recorded database maintenance work: "
            f"changed={changed_galleries} removed={removed_galleries} "
            f"work={work}."
        )

    @staticmethod
    def _is_due(
        state: DatabaseMaintenanceState,
        *,
        now: int,
        min_work_units: int,
        min_interval_seconds: int,
    ) -> bool:
        if state.accumulated_work < min_work_units:
            return False
        if state.last_evaluated_at is None:
            return True
        return now - state.last_evaluated_at >= min_interval_seconds

    def _mark_evaluated(self, evaluated_at: int) -> None:
        with self.SQLConnector() as connector:
            connector.execute(
                f"""
                UPDATE {MAINTENANCE_STATE_TABLE}
                SET last_evaluated_at = %s
                WHERE state_id = %s
                """,
                (evaluated_at, MAINTENANCE_STATE_ID),
            )

    def _mark_optimized(self, *, optimized_at: int, completed_work: int) -> None:
        with self.SQLConnector() as connector:
            connector.execute(
                f"""
                UPDATE {MAINTENANCE_STATE_TABLE}
                SET accumulated_work = CASE
                        WHEN accumulated_work >= %s
                            THEN accumulated_work - %s
                        ELSE 0
                    END,
                    last_evaluated_at = %s,
                    last_optimized_at = %s
                WHERE state_id = %s
                """,
                (
                    completed_work,
                    completed_work,
                    optimized_at,
                    optimized_at,
                    MAINTENANCE_STATE_ID,
                ),
            )

    def _eligible_targets(self, reclaimable_space: list[ReclaimableSpace]) -> list[str]:
        config = self.config.maintenance
        return [
            space.name
            for space in reclaimable_space
            if space.free_bytes >= config.min_data_free_bytes
            and space.free_ratio >= config.min_data_free_ratio
        ]

    def run_scheduled_optimization(self) -> DatabaseMaintenanceResult:
        # Even the cheap readiness read participates in the cooperative gate.
        # Release its shared slot before upgrading to the all-slot exclusive
        # gate below, otherwise this operation could deadlock against itself.
        with self.database_gate():
            state = self.get_state()
            config = self.config.maintenance
            if not config.optimize_enabled:
                return DatabaseMaintenanceResult(
                    False,
                    tuple(),
                    state.accumulated_work,
                )

            now = int(time())
            if not self._is_due(
                state,
                now=now,
                min_work_units=config.min_work_units,
                min_interval_seconds=config.min_interval_seconds,
            ):
                return DatabaseMaintenanceResult(
                    False,
                    tuple(),
                    state.accumulated_work,
                )

        with self.maintenance_gate():
            state = self.get_state()
            now = int(time())
            if not self._is_due(
                state,
                now=now,
                min_work_units=config.min_work_units,
                min_interval_seconds=config.min_interval_seconds,
            ):
                return DatabaseMaintenanceResult(False, tuple(), state.accumulated_work)

            # Persist the evaluation time before potentially long metadata
            # inspection or DDL. A failure therefore cannot cause the resident
            # loop to retry OPTIMIZE every thirty minutes.
            self._mark_evaluated(now)
            reclaimable_space = self.database_settings.get_reclaimable_space()
            targets = self._eligible_targets(reclaimable_space)
            if not targets:
                self.logger.info(
                    "Database optimization evaluated but skipped: "
                    f"accumulated_work={state.accumulated_work} "
                    "no target met both reclaimable-space thresholds."
                )
                return DatabaseMaintenanceResult(True, tuple(), state.accumulated_work)

            self.logger.info(
                "Starting scheduled database optimization: "
                f"accumulated_work={state.accumulated_work} "
                f"targets={targets!r}."
            )
            self.database_settings.optimize_tables(targets)
            completed_at = int(time())
            self._mark_optimized(
                optimized_at=completed_at,
                completed_work=state.accumulated_work,
            )
            remaining_work = self.get_state().accumulated_work
            self.logger.info(
                "Scheduled database optimization completed: "
                f"targets={len(targets)} remaining_work={remaining_work}."
            )
            return DatabaseMaintenanceResult(True, tuple(targets), remaining_work)

    def optimize_now(self) -> DatabaseMaintenanceResult:
        with self.maintenance_gate():
            state = self.get_state()
            match self.config.database.sql_type.lower():
                case "mariadb":
                    targets = self.database_settings._get_all_table_names()
                case "sqlite":
                    targets = ["main"]
                case _:
                    raise ValueError("Unsupported SQL type")
            self.database_settings.optimize_tables(targets)
            completed_at = int(time())
            self._mark_optimized(
                optimized_at=completed_at,
                completed_work=state.accumulated_work,
            )
            remaining_work = self.get_state().accumulated_work
        self.logger.info(
            "Manual database optimization completed: "
            f"targets={len(targets)} remaining_work={remaining_work}."
        )
        return DatabaseMaintenanceResult(True, tuple(targets), remaining_work)

    def _maintenance_lock_name(self, slot: int) -> str:
        database_key = sha256(
            self.config.database.database.encode("utf-8")
        ).hexdigest()[:32]
        return f"h2hdb:{database_key}:maintenance:{slot}"

    @staticmethod
    def _get_named_lock(
        connector: SQLConnector, *, lock_name: str, timeout_seconds: int
    ) -> bool:
        try:
            row = connector.fetch_one(
                "SELECT GET_LOCK(%s, %s)",
                (lock_name, timeout_seconds),
            )
        except Exception as error:
            raise DatabaseConfigurationError(
                f"MariaDB database gate {lock_name!r} failed: {error!r}."
            ) from error
        if not row or row[0] is None:
            raise DatabaseConfigurationError(
                f"MariaDB could not acquire database gate {lock_name!r}."
            )
        try:
            result = int(row[0])
        except (TypeError, ValueError) as error:
            raise DatabaseConfigurationError(
                "MariaDB returned an invalid GET_LOCK result for database gate "
                f"{lock_name!r}: {row[0]!r}."
            ) from error
        if result not in (0, 1):
            raise DatabaseConfigurationError(
                "MariaDB returned an invalid GET_LOCK result for database gate "
                f"{lock_name!r}: {result!r}."
            )
        return result == 1

    @contextmanager
    def database_gate(self, *, timeout_seconds: int | None = None) -> Generator[None]:
        """Hold one shared operation slot while a public database call runs.

        MariaDB named locks are exclusive, so one global name would serialize
        every OPDS read, queue operation, and ingest heartbeat. Operations
        instead spread over independent slots; maintenance acquires every slot
        before DDL and therefore still waits for all in-flight participants.
        """

        slot = randbelow(DATABASE_GATE_SLOT_COUNT)
        with self._database_gate_slots((slot,), timeout_seconds=timeout_seconds):
            yield

    @contextmanager
    def maintenance_gate(
        self,
        *,
        timeout_seconds: int | None = None,
    ) -> Generator[None]:
        """Exclusively stop all participating public operations for maintenance."""

        with self._database_gate_slots(
            tuple(range(DATABASE_GATE_SLOT_COUNT)),
            timeout_seconds=timeout_seconds,
        ):
            yield

    @contextmanager
    def _database_gate_slots(
        self,
        slots: tuple[int, ...],
        *,
        timeout_seconds: int | None,
    ) -> Generator[None]:
        if self.config.database.sql_type.lower() == "sqlite":
            yield
            return
        if self.config.database.sql_type.lower() != "mariadb":
            raise ValueError("Unsupported SQL type")

        wait_seconds = (
            self.config.maintenance.lock_wait_seconds
            if timeout_seconds is None
            else timeout_seconds
        )
        if wait_seconds < 1:
            raise ValueError("Database gate timeout must be at least one second.")

        lock_names = tuple(self._maintenance_lock_name(slot) for slot in slots)
        with self.SQLConnector() as connector:
            acquired_names: list[str] = []
            for lock_name in lock_names:
                acquired = self._get_named_lock(
                    connector, lock_name=lock_name, timeout_seconds=0
                )
                if not acquired:
                    self.logger.info(
                        "Database maintenance gate is occupied; "
                        f"waiting in {wait_seconds}-second intervals."
                    )
                while not acquired:
                    acquired = self._get_named_lock(
                        connector,
                        lock_name=lock_name,
                        timeout_seconds=wait_seconds,
                    )
                    if not acquired:
                        self.logger.warning(
                            "Database maintenance gate is still occupied after "
                            f"{wait_seconds} seconds; continuing to wait."
                        )
                acquired_names.append(lock_name)

            body_raised = False
            try:
                yield
            except BaseException:
                body_raised = True
                raise
            finally:
                release_error: Exception | None = None
                for lock_name in reversed(acquired_names):
                    try:
                        row = connector.fetch_one(
                            "SELECT RELEASE_LOCK(%s)",
                            (lock_name,),
                        )
                        released_normally = bool(
                            row and row[0] is not None and int(row[0]) == 1
                        )
                    except Exception as error:
                        release_error = error
                        self.logger.error(
                            f"MariaDB database gate {lock_name!r} could not be "
                            "explicitly released; closing its connection: "
                            f"{error!r}."
                        )
                    else:
                        if not released_normally:
                            self.logger.warning(
                                f"MariaDB database gate {lock_name!r} was not "
                                "released normally; closing its connection."
                            )
                if release_error is not None and not body_raised:
                    raise release_error
