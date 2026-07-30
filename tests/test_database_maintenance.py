from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from typing import Any, cast

import pytest

from h2hdb import (
    H2HDB,
    DatabaseConfig,
    DatabaseMaintenanceConfig,
    H2HDBConfig,
    SyncOutcome,
)
from h2hdb import table_database_maintenance as maintenance_module
from h2hdb.repository import RepositoryContext
from h2hdb.sql_connector import DatabaseConfigurationError, SQLConnector
from h2hdb.table_database_maintenance import H2HDBDatabaseMaintenance
from h2hdb.table_database_setting import (
    H2HDBCheckDatabaseSettings,
    ReclaimableSpace,
)


@pytest.fixture
def sqlite_db(sqlite_config: H2HDBConfig) -> Iterator[H2HDB]:
    instance = H2HDB(config=sqlite_config)
    with instance:
        instance.create_main_tables()
        yield instance


def _configure_scheduled_maintenance(
    db: H2HDB,
    *,
    min_interval_seconds: int = 0,
    min_work_units: int = 1,
    min_data_free_bytes: int = 1,
    min_data_free_ratio: float = 0.1,
) -> None:
    db.config.maintenance = DatabaseMaintenanceConfig(
        min_interval_seconds=min_interval_seconds,
        min_work_units=min_work_units,
        min_data_free_bytes=min_data_free_bytes,
        min_data_free_ratio=min_data_free_ratio,
    )


def test_maintenance_defaults_and_sync_work_units() -> None:
    config = DatabaseMaintenanceConfig()

    assert config.optimize_enabled is True
    assert config.min_interval_seconds == 604800
    assert config.min_work_units == 1000
    assert config.min_data_free_bytes == 268435456
    assert config.min_data_free_ratio == 0.20
    assert SyncOutcome(999, 2, 3).maintenance_work == 5


class _AdministrativeResultConnector:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.queries = list[str]()

    def fetch_all(self, query: str) -> list[tuple[object, ...]]:
        self.queries.append(query)
        return self.rows


def test_mariadb_administrative_error_rows_are_not_treated_as_success(
    sqlite_db: H2HDB,
) -> None:
    connector = _AdministrativeResultConnector(
        [("catalog.widgets", "optimize", "error", "operation failed")]
    )

    with pytest.raises(DatabaseConfigurationError, match="operation failed"):
        sqlite_db.database_settings._run_mariadb_table_command(
            cast(SQLConnector, connector),
            command="OPTIMIZE TABLE",
            table_name="odd`name",
        )

    assert connector.queries == ["OPTIMIZE TABLE `odd``name`"]


def test_scheduled_optimization_requires_both_space_thresholds(
    sqlite_db: H2HDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_scheduled_maintenance(
        sqlite_db,
        min_data_free_bytes=100,
        min_data_free_ratio=0.2,
    )
    sqlite_db.database_maintenance.record_gallery_changes(
        changed_galleries=1, removed_galleries=0
    )
    spaces = [
        ReclaimableSpace("not-enough-bytes", 99, 100),
        ReclaimableSpace("not-enough-ratio", 100, 1000),
        ReclaimableSpace("eligible", 200, 1000),
    ]
    optimized_targets = list[str]()
    monkeypatch.setattr(
        sqlite_db.database_settings,
        "get_reclaimable_space",
        lambda: spaces,
    )
    monkeypatch.setattr(
        sqlite_db.database_settings,
        "optimize_tables",
        lambda targets: optimized_targets.extend(targets),
    )

    result = sqlite_db.run_scheduled_database_maintenance()

    assert result.evaluated is True
    assert result.optimized_targets == ("eligible",)
    assert optimized_targets == ["eligible"]
    assert sqlite_db.database_maintenance.get_state().accumulated_work == 0


def test_no_eligible_target_retains_work_and_throttles_next_evaluation(
    sqlite_db: H2HDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_scheduled_maintenance(
        sqlite_db,
        min_interval_seconds=100,
        min_data_free_bytes=100,
        min_data_free_ratio=0.2,
    )
    sqlite_db.database_maintenance.record_gallery_changes(
        changed_galleries=4, removed_galleries=0
    )
    clock = [10_000]
    inspections = [0]
    monkeypatch.setattr(maintenance_module, "time", lambda: float(clock[0]))

    def inspect_space() -> list[ReclaimableSpace]:
        inspections[0] += 1
        return [ReclaimableSpace("too-small", 99, 100)]

    monkeypatch.setattr(
        sqlite_db.database_settings,
        "get_reclaimable_space",
        inspect_space,
    )

    first = sqlite_db.run_scheduled_database_maintenance()
    state_after_first = sqlite_db.database_maintenance.get_state()
    clock[0] += 1
    second = sqlite_db.run_scheduled_database_maintenance()

    assert first.evaluated is True
    assert first.optimized is False
    assert state_after_first.accumulated_work == 4
    assert state_after_first.last_evaluated_at == 10_000
    assert state_after_first.last_optimized_at is None
    assert second.evaluated is False
    assert inspections == [1]


def test_failed_optimization_retains_work_and_throttles_retry(
    sqlite_db: H2HDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_scheduled_maintenance(
        sqlite_db,
        min_interval_seconds=100,
    )
    sqlite_db.database_maintenance.record_gallery_changes(
        changed_galleries=3, removed_galleries=0
    )
    clock = [20_000]
    optimization_calls = [0]
    monkeypatch.setattr(maintenance_module, "time", lambda: float(clock[0]))
    monkeypatch.setattr(
        sqlite_db.database_settings,
        "get_reclaimable_space",
        lambda: [ReclaimableSpace("main", 1, 1)],
    )

    def fail_optimization(targets: list[str]) -> None:
        optimization_calls[0] += 1
        raise RuntimeError(f"failed targets: {targets!r}")

    monkeypatch.setattr(
        sqlite_db.database_settings,
        "optimize_tables",
        fail_optimization,
    )

    with pytest.raises(RuntimeError, match="failed targets"):
        sqlite_db.run_scheduled_database_maintenance()

    failed_state = sqlite_db.database_maintenance.get_state()
    clock[0] += 1
    retry = sqlite_db.run_scheduled_database_maintenance()

    assert failed_state.accumulated_work == 3
    assert failed_state.last_evaluated_at == 20_000
    assert failed_state.last_optimized_at is None
    assert retry.evaluated is False
    assert optimization_calls == [1]


def test_success_subtracts_snapshot_without_losing_concurrent_work(
    sqlite_db: H2HDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_scheduled_maintenance(sqlite_db, min_work_units=2)
    sqlite_db.database_maintenance.record_gallery_changes(
        changed_galleries=2, removed_galleries=0
    )
    monkeypatch.setattr(
        sqlite_db.database_settings,
        "get_reclaimable_space",
        lambda: [ReclaimableSpace("main", 1, 1)],
    )

    def optimize_while_new_work_arrives(targets: list[str]) -> None:
        assert targets == ["main"]
        sqlite_db.database_maintenance.record_gallery_changes(
            changed_galleries=5, removed_galleries=0
        )

    monkeypatch.setattr(
        sqlite_db.database_settings,
        "optimize_tables",
        optimize_while_new_work_arrives,
    )

    result = sqlite_db.run_scheduled_database_maintenance()
    state = sqlite_db.database_maintenance.get_state()

    assert result.optimized_targets == ("main",)
    assert result.accumulated_work == 5
    assert state.accumulated_work == 5
    assert state.last_optimized_at is not None


def test_disabled_scheduler_does_not_evaluate(
    sqlite_db: H2HDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    sqlite_db.config.maintenance = DatabaseMaintenanceConfig(
        optimize_enabled=False,
        min_interval_seconds=0,
        min_work_units=1,
        min_data_free_bytes=0,
        min_data_free_ratio=0,
    )
    sqlite_db.database_maintenance.record_gallery_changes(
        changed_galleries=1, removed_galleries=0
    )

    def unexpected_inspection() -> list[ReclaimableSpace]:
        raise AssertionError("disabled maintenance must not inspect reclaimable space")

    monkeypatch.setattr(
        sqlite_db.database_settings,
        "get_reclaimable_space",
        unexpected_inspection,
    )

    result = sqlite_db.run_scheduled_database_maintenance()

    assert result.evaluated is False
    assert sqlite_db.database_maintenance.get_state().last_evaluated_at is None


def test_manual_analyze_runs_inside_database_gate(
    sqlite_db: H2HDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = list[str]()

    @contextmanager
    def recording_gate(*, timeout_seconds: int | None = None) -> Iterator[None]:
        assert timeout_seconds is None
        events.append("acquire")
        try:
            yield
        finally:
            events.append("release")

    monkeypatch.setattr(sqlite_db, "database_gate", recording_gate)
    monkeypatch.setattr(
        sqlite_db.database_settings,
        "analyze_database",
        lambda: events.append("analyze"),
    )

    sqlite_db.analyze_database()

    assert events == ["acquire", "analyze", "release"]


class _RecordingLogger:
    def __init__(self) -> None:
        self.info_messages = list[str]()
        self.warning_messages = list[str]()
        self.error_messages = list[str]()

    def debug(self, message: str) -> None:
        pass

    def info(self, message: str) -> None:
        self.info_messages.append(message)

    def warning(self, message: str) -> None:
        self.warning_messages.append(message)

    def error(self, message: str) -> None:
        self.error_messages.append(message)


class _NamedLockConnector:
    def __init__(
        self,
        lock_results: list[object],
        *,
        release_result: object = 1,
        release_error: Exception | None = None,
    ) -> None:
        self.lock_results = iter(lock_results)
        self.release_result = release_result
        self.release_error = release_error
        self.events = list[tuple[str, tuple[Any, ...]]]()

    def __enter__(self) -> _NamedLockConnector:
        self.events.append(("enter", tuple()))
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object | None,
    ) -> None:
        self.events.append(("exit", tuple()))

    def fetch_one(self, query: str, data: tuple[Any, ...] = ()) -> tuple[object, ...]:
        if "GET_LOCK" in query:
            self.events.append(("get", data))
            result = next(self.lock_results)
            if isinstance(result, Exception):
                raise result
            return (result,)
        if "RELEASE_LOCK" in query:
            self.events.append(("release", data))
            if self.release_error is not None:
                raise self.release_error
            return (self.release_result,)
        raise AssertionError(f"Unexpected query: {query}")


def _maintenance_with_lock_connector(
    connector: _NamedLockConnector,
) -> tuple[H2HDBDatabaseMaintenance, _RecordingLogger]:
    config = H2HDBConfig(
        database=DatabaseConfig(sql_type="mariadb", database="catalog")
    )
    logger = _RecordingLogger()
    context = replace(
        RepositoryContext.from_config(config),
        logger=logger,
        SQLConnector=lambda: connector,
    )
    settings = H2HDBCheckDatabaseSettings(context)
    return H2HDBDatabaseMaintenance(context, settings), logger


def test_database_gate_retries_on_same_connection_and_releases() -> None:
    connector = _NamedLockConnector([0, 0, 1])
    maintenance, logger = _maintenance_with_lock_connector(connector)

    with maintenance.database_gate(timeout_seconds=7):
        connector.events.append(("body", tuple()))

    lock_name = "h2hdb:catalog:maintenance"
    assert connector.events == [
        ("enter", tuple()),
        ("get", (lock_name, 0)),
        ("get", (lock_name, 7)),
        ("get", (lock_name, 7)),
        ("body", tuple()),
        ("release", (lock_name,)),
        ("exit", tuple()),
    ]
    assert len(logger.info_messages) == 1
    assert len(logger.warning_messages) == 1


def test_database_gate_releases_when_body_raises() -> None:
    connector = _NamedLockConnector([1])
    maintenance, _ = _maintenance_with_lock_connector(connector)

    with pytest.raises(RuntimeError, match="body failure"):
        with maintenance.database_gate(timeout_seconds=1):
            raise RuntimeError("body failure")

    assert [event[0] for event in connector.events] == [
        "enter",
        "get",
        "release",
        "exit",
    ]


def test_database_gate_does_not_mask_body_error_when_release_fails() -> None:
    connector = _NamedLockConnector(
        [1],
        release_error=RuntimeError("release failure"),
    )
    maintenance, logger = _maintenance_with_lock_connector(connector)

    with pytest.raises(RuntimeError, match="body failure"):
        with maintenance.database_gate(timeout_seconds=1):
            raise RuntimeError("body failure")

    assert len(logger.error_messages) == 1
    assert connector.events[-1] == ("exit", tuple())


@pytest.mark.parametrize(
    "invalid_result",
    [None, 2, "not-an-integer", RuntimeError("server unavailable")],
)
def test_database_gate_rejects_invalid_get_lock_result(
    invalid_result: object,
) -> None:
    connector = _NamedLockConnector([invalid_result])
    maintenance, _ = _maintenance_with_lock_connector(connector)

    with pytest.raises(DatabaseConfigurationError):
        with maintenance.database_gate(timeout_seconds=1):
            pass

    assert [event[0] for event in connector.events] == ["enter", "get", "exit"]


def test_public_database_gate_holds_and_releases_exact_mariadb_lock(
    mariadb_config: H2HDBConfig,
) -> None:
    db = H2HDB(config=mariadb_config)
    lock_name = f"h2hdb:{mariadb_config.database.database}:maintenance"

    with db.database_gate(timeout_seconds=1):
        with db.SQLConnector() as observer:
            assert observer.fetch_one("SELECT IS_USED_LOCK(%s)", (lock_name,))[0]
            assert observer.fetch_one("SELECT GET_LOCK(%s, 0)", (lock_name,)) == (0,)

    with db.SQLConnector() as observer:
        assert observer.fetch_one("SELECT GET_LOCK(%s, 0)", (lock_name,)) == (1,)
        assert observer.fetch_one("SELECT RELEASE_LOCK(%s)", (lock_name,)) == (1,)
