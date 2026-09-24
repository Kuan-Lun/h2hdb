from __future__ import annotations

import secrets
from collections.abc import Iterator
from dataclasses import dataclass

import pytest

import h2hdb.database_audit as audit_module
from h2hdb import (
    CoreConfig,
    DatabaseAuditPolicy,
    DatabaseAuditReason,
    DatabaseAuditReport,
    DatabaseAuditSessionLostError,
    DatabaseAuditSessionUnavailableError,
    DatabaseAuditStateError,
    SchemaEpochReport,
    VNextDatabaseAdminFacade,
)
from h2hdb.repository import RepositoryContext
from h2hdb.schema_admin import VNextSchemaAdmin
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_domains import INT63_MAX
from h2hdb.vnext_transaction import VNextUnitOfWork

_LEASE = 60_000_000
_POLICY = DatabaseAuditPolicy(
    minimum_interval_microseconds=1_000_000_000, duration_multiplier=100
)


@dataclass
class _Clock:
    now: int = 1_000_000_000_000
    ticks: int = 0
    full_checks: int = 0
    audit_duration: int = 2_000_000

    def advance(self, microseconds: int) -> None:
        self.now += microseconds
        self.ticks += microseconds * 1_000


@pytest.fixture
def audit_clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    clock = _Clock()
    original = VNextSchemaAdmin.check

    def database_clock(_work: VNextUnitOfWork) -> int:
        return clock.now

    def full_check(admin: VNextSchemaAdmin) -> SchemaEpochReport:
        result = original(admin)
        clock.full_checks += 1
        clock.advance(clock.audit_duration)
        return result

    monkeypatch.setattr(audit_module, "database_unix_microseconds", database_clock)
    monkeypatch.setattr(audit_module, "perf_counter_ns", lambda: clock.ticks)
    monkeypatch.setattr(VNextSchemaAdmin, "check", full_check)
    return clock


@pytest.fixture
def admin(sqlite_config: CoreConfig) -> Iterator[VNextDatabaseAdminFacade]:
    result = VNextDatabaseAdminFacade(sqlite_config)
    try:
        result.initialize()
        yield result
    finally:
        result.close()


def _start(admin: VNextDatabaseAdminFacade) -> DatabaseAuditReport:
    return admin.start_ingest_runtime(
        policy=_POLICY, lease_duration_microseconds=_LEASE
    )


def test_first_full_clean_restart_and_exact_finish_replay(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    sqlite_config: CoreConfig,
) -> None:
    first = _start(admin)
    assert first.reason is DatabaseAuditReason.FIRST_RUN
    assert first.full_audit is not None
    assert first.last_full_audit_at == audit_clock.now
    assert first.last_full_audit_duration_microseconds == audit_clock.audit_duration
    assert (
        first.next_full_audit_at
        == audit_clock.now + _POLICY.minimum_interval_microseconds
    )
    admin.finish_ingest_runtime(first.session)
    admin.close()
    replacement = VNextDatabaseAdminFacade(sqlite_config)
    replacement.finish_ingest_runtime(first.session)
    second = _start(replacement)
    assert second.full_audit is None
    assert second.last_full_audit_at == first.last_full_audit_at
    assert second.session.generation == first.session.generation + 1
    assert second.session.run_token != first.session.run_token
    assert audit_clock.full_checks == 1
    with pytest.raises(DatabaseAuditSessionLostError):
        replacement.finish_ingest_runtime(first.session)
    replacement.finish_ingest_runtime(second.session)
    replacement.close()


def test_initial_long_catchup_gets_one_grace_without_fabricated_audit(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
) -> None:
    first = _start(admin)
    # A continuously nonempty initial backlog has no finite cohort receipt.
    # Repeatedly passing the due date cannot fabricate catch-up completion.
    for _ in range(3):
        audit_clock.advance(_POLICY.minimum_interval_microseconds * 10)
        deferred = admin.check_ingest_runtime_if_due(first.session)
        assert deferred.full_audit is None
        assert deferred.reason is DatabaseAuditReason.INITIAL_CATCHUP
        assert deferred.next_full_audit_at == first.next_full_audit_at
        assert deferred.last_full_audit_at == first.last_full_audit_at
    completed = admin.mark_initial_catchup_complete(first.session)
    assert not completed.initial_catchup_pending
    assert completed.last_full_audit_at == first.last_full_audit_at
    assert (
        completed.next_full_audit_at
        == audit_clock.now + _POLICY.minimum_interval_microseconds
    )
    audit_clock.advance(20_000_000)
    repeated = admin.mark_initial_catchup_complete(first.session)
    assert repeated.next_full_audit_at == completed.next_full_audit_at
    admin.finish_ingest_runtime(first.session)
    restarted = _start(admin)
    assert restarted.next_full_audit_at == completed.next_full_audit_at
    assert restarted.reason is DatabaseAuditReason.RECENT_AUDIT
    assert audit_clock.full_checks == 1


def test_periodic_full_check_uses_measured_duration_budget(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
) -> None:
    first = _start(admin)
    completed = admin.mark_initial_catchup_complete(first.session)
    audit_clock.advance(completed.next_full_audit_at - audit_clock.now)
    audit_clock.audit_duration = _POLICY.minimum_interval_microseconds
    periodic = admin.check_ingest_runtime_if_due(first.session)
    assert periodic.reason is DatabaseAuditReason.SCHEDULE_DUE
    assert periodic.full_audit is not None
    assert periodic.last_full_audit_at == audit_clock.now
    assert (
        periodic.next_full_audit_at
        == audit_clock.now + audit_clock.audit_duration * _POLICY.duration_multiplier
    )
    assert audit_clock.full_checks == 2


@pytest.mark.cleanup_acceptance
def test_live_runtime_contends_expired_runtime_requires_full_and_fences_old_owner(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    sqlite_config: CoreConfig,
) -> None:
    first = _start(admin)
    contender = VNextDatabaseAdminFacade(sqlite_config)
    with pytest.raises(DatabaseAuditSessionUnavailableError):
        _start(contender)
    audit_clock.advance(_LEASE)
    next_run = _start(contender)
    assert next_run.reason is DatabaseAuditReason.PREVIOUS_INTERRUPTION
    assert next_run.full_audit is not None
    with pytest.raises(DatabaseAuditSessionLostError):
        admin.renew_ingest_runtime(first.session, _LEASE)
    with pytest.raises(DatabaseAuditSessionLostError):
        admin.mark_initial_catchup_complete(first.session)
    with pytest.raises(DatabaseAuditSessionLostError):
        admin.finish_ingest_runtime(first.session)


def test_long_full_audit_can_finish_after_lease_expiry_without_takeover(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
) -> None:
    audit_clock.audit_duration = _LEASE * 5
    report = _start(admin)
    assert report.full_audit is not None
    assert report.last_full_audit_duration_microseconds == _LEASE * 5
    admin.renew_ingest_runtime(report.session, _LEASE)
    admin.finish_ingest_runtime(report.session)


def test_competing_takeover_prevents_old_audit_recording_success(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    sqlite_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = VNextSchemaAdmin.check
    newer: list[DatabaseAuditReport] = []
    entered = False

    def intercepted(value: VNextSchemaAdmin) -> SchemaEpochReport:
        nonlocal entered
        result = original(value)
        if not entered:
            entered = True
            audit_clock.advance(_LEASE)
            contender = VNextDatabaseAdminFacade(sqlite_config)
            newer.append(_start(contender))
        return result

    monkeypatch.setattr(VNextSchemaAdmin, "check", intercepted)
    with pytest.raises(DatabaseAuditSessionLostError):
        _start(admin)
    assert len(newer) == 1
    with RepositoryContext.from_config(sqlite_config).SQLConnector() as connector:
        state = audit_module.read_database_audit_state(connector)
    assert state is not None
    assert state.session == newer[0].session
    assert state.last_audit_at == newer[0].last_full_audit_at
    assert state.audit_pending == 0


@pytest.mark.cleanup_acceptance
def test_failed_full_audit_retains_dirty_pending_state_without_success(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    sqlite_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(_value: VNextSchemaAdmin) -> SchemaEpochReport:
        raise RuntimeError("semantic corruption")

    monkeypatch.setattr(VNextSchemaAdmin, "check", fail)
    with pytest.raises(RuntimeError, match="semantic corruption"):
        _start(admin)
    with RepositoryContext.from_config(sqlite_config).SQLConnector() as connector:
        state = audit_module.read_database_audit_state(connector)
    assert state is not None
    assert state.last_audit_at is None
    assert state.next_audit_at is None
    assert state.audit_pending == 1
    with pytest.raises(DatabaseAuditSessionUnavailableError):
        admin.finish_ingest_runtime(state.session)
    assert audit_clock.full_checks == 0


def test_renew_during_sqlite_full_read_snapshot_is_read_only(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    sqlite_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = VNextSchemaAdmin.check

    def intercepted(value: VNextSchemaAdmin) -> SchemaEpochReport:
        with RepositoryContext.from_config(sqlite_config).SQLConnector() as connector:
            with connector.read_transaction():
                state = audit_module.read_database_audit_state(connector)
                assert state is not None
                assert state.audit_pending == 1
                admin.renew_ingest_runtime(state.session, _LEASE)
        return original(value)

    monkeypatch.setattr(VNextSchemaAdmin, "check", intercepted)
    first = _start(admin)
    assert first.full_audit is not None
    assert audit_clock.full_checks == 1


def test_force_and_validator_version_changes_require_full_checks(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    sqlite_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _start(admin)
    admin.finish_ingest_runtime(first.session)
    forced = admin.start_ingest_runtime(
        policy=_POLICY, lease_duration_microseconds=_LEASE, force_full=True
    )
    assert forced.reason is DatabaseAuditReason.FORCED
    admin.finish_ingest_runtime(forced.session)
    monkeypatch.setattr(audit_module, "version", lambda _name: "999.0.0")
    newer = VNextDatabaseAdminFacade(sqlite_config)
    changed = _start(newer)
    assert changed.reason is DatabaseAuditReason.VALIDATOR_CHANGED
    assert changed.full_audit is not None
    assert audit_clock.full_checks == 3


def test_clock_regression_requires_full_check(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
) -> None:
    first = _start(admin)
    admin.mark_initial_catchup_complete(first.session)
    audit_clock.now = first.last_full_audit_at - 10_000_000
    changed = admin.check_ingest_runtime_if_due(first.session)
    assert changed.reason is DatabaseAuditReason.CLOCK_CHANGED
    assert changed.full_audit is not None
    following = admin.check_ingest_runtime_if_due(first.session)
    assert following.reason is DatabaseAuditReason.RECENT_AUDIT
    assert following.full_audit is None


def test_clean_quick_path_uses_only_bounded_control_queries(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _start(admin)
    admin.mark_initial_catchup_complete(first.session)
    admin.finish_ingest_runtime(first.session)
    statements: list[str] = []
    original = SQLiteConnector.connect

    def connect(connector: SQLiteConnector) -> None:
        original(connector)
        connector.connection.set_trace_callback(statements.append)

    monkeypatch.setattr(SQLiteConnector, "connect", connect)
    second = _start(admin)
    assert second.full_audit is None
    reads = [
        statement
        for statement in statements
        if statement.lstrip().upper().startswith("SELECT")
    ]
    assert 1 <= len(reads) <= 8
    assert all("catalog_" not in statement for statement in reads)
    assert audit_clock.full_checks == 1


def test_explicit_check_does_not_change_scheduling_state(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    sqlite_config: CoreConfig,
) -> None:
    _start(admin)
    context = RepositoryContext.from_config(sqlite_config)
    with context.SQLConnector() as connector:
        before = audit_module.read_database_audit_state(connector)
    admin.check()
    with context.SQLConnector() as connector:
        after = audit_module.read_database_audit_state(connector)
    assert before == after
    assert audit_clock.full_checks == 2


def test_decision_notification_precedes_full_check_and_failure_is_isolated(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
) -> None:
    decisions: list[DatabaseAuditReason] = []

    def notify(reason: DatabaseAuditReason) -> None:
        assert audit_clock.full_checks == 0
        decisions.append(reason)
        raise RuntimeError("log handler failed")

    first = admin.start_ingest_runtime(
        policy=_POLICY, lease_duration_microseconds=_LEASE, on_check=notify
    )
    assert decisions == [DatabaseAuditReason.FIRST_RUN]
    assert first.full_audit is not None


def test_corrupt_partial_success_cannot_select_quick(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    sqlite_config: CoreConfig,
) -> None:
    first = _start(admin)
    admin.finish_ingest_runtime(first.session)
    with RepositoryContext.from_config(sqlite_config).SQLConnector() as connector:
        connector.execute("PRAGMA ignore_check_constraints = ON")
        connector.execute(
            "UPDATE operational_database_audit_states SET validator_version = NULL"
        )
    with pytest.raises(DatabaseAuditStateError, match="incomplete"):
        _start(admin)
    assert audit_clock.full_checks == 1


@pytest.mark.parametrize(
    "minimum,multiplier", [(0, 100), (1, 0), (True, 1), (1, True), (-1, 1)]
)
def test_invalid_audit_policy_is_rejected(minimum: int, multiplier: int) -> None:
    with pytest.raises((TypeError, ValueError)):
        DatabaseAuditPolicy(
            minimum_interval_microseconds=minimum, duration_multiplier=multiplier
        )


def _read_audit_state(config: CoreConfig) -> audit_module._State | None:
    with RepositoryContext.from_config(config).SQLConnector() as connector:
        return audit_module.read_database_audit_state(connector)


def _record_offline(config: CoreConfig, *, duration: int = 23_000_000) -> None:
    context = RepositoryContext.from_config(config)
    with context.SQLConnector() as connector:
        with connector.transaction():
            audit_module.record_offline_audit_completion(
                VNextUnitOfWork(connector, backend=context.sql_type),
                duration_microseconds=duration,
            )


def _fail_full_check(_admin: VNextSchemaAdmin) -> SchemaEpochReport:
    raise RuntimeError("interrupted runtime audit")


@pytest.mark.parametrize("caught_up", [False, True])
@pytest.mark.parametrize("retained", ["closed", "live", "expired", "pending"])
def test_offline_audit_refreshes_retained_runtime_and_fences_old_owner(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    sqlite_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    retained: str,
    caught_up: bool,
) -> None:
    first = _start(admin)
    if caught_up:
        admin.mark_initial_catchup_complete(first.session)
    if retained in ("closed", "pending"):
        admin.finish_ingest_runtime(first.session)
    if retained == "expired":
        audit_clock.advance(_LEASE)
    if retained == "pending":
        with monkeypatch.context() as patch:
            patch.setattr(VNextSchemaAdmin, "check", _fail_full_check)
            with pytest.raises(RuntimeError, match="interrupted runtime audit"):
                admin.start_ingest_runtime(
                    policy=_POLICY,
                    lease_duration_microseconds=_LEASE,
                    force_full=True,
                )
    previous = _read_audit_state(sqlite_config)
    assert previous is not None
    admin.check()
    _record_offline(sqlite_config)
    following = _read_audit_state(sqlite_config)
    assert following is not None
    assert following.generation == previous.generation + 1
    assert following.owner_token != previous.owner_token
    assert following.lease_expires_at is None
    assert following.lease_duration_microseconds == previous.lease_duration_microseconds
    assert (
        following.minimum_interval_microseconds == _POLICY.minimum_interval_microseconds
    )
    assert following.duration_multiplier == _POLICY.duration_multiplier
    assert following.last_audit_at == audit_clock.now
    assert following.audit_duration_microseconds == 23_000_000
    assert following.validator_version == audit_module._validator_version()
    assert (
        following.next_audit_at
        == audit_clock.now + 23_000_000 * _POLICY.duration_multiplier
    )
    assert following.initial_catchup_at == previous.initial_catchup_at
    assert following.audit_pending == 0
    with pytest.raises(DatabaseAuditSessionLostError):
        admin.renew_ingest_runtime(previous.session, _LEASE)
    with pytest.raises(DatabaseAuditSessionLostError):
        admin.check_ingest_runtime_if_due(previous.session)
    with pytest.raises(DatabaseAuditSessionLostError):
        admin.mark_initial_catchup_complete(previous.session)
    with pytest.raises(DatabaseAuditSessionLostError):
        admin.finish_ingest_runtime(previous.session)
    restarted = _start(admin)
    assert restarted.full_audit is None
    assert restarted.initial_catchup_pending is not caught_up
    assert restarted.reason is (
        DatabaseAuditReason.RECENT_AUDIT
        if caught_up
        else DatabaseAuditReason.INITIAL_CATCHUP
    )
    assert restarted.last_full_audit_at == following.last_audit_at


@pytest.mark.parametrize("interrupted_first_audit", [False, True])
def test_offline_audit_seeds_empty_or_unfinished_first_runtime(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    sqlite_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    interrupted_first_audit: bool,
) -> None:
    if interrupted_first_audit:
        with monkeypatch.context() as patch:
            patch.setattr(VNextSchemaAdmin, "check", _fail_full_check)
            with pytest.raises(RuntimeError, match="interrupted runtime audit"):
                _start(admin)
    previous = _read_audit_state(sqlite_config)
    if previous is not None:
        assert previous.last_audit_at is None
        assert previous.audit_pending == 1
    admin.check()
    _record_offline(sqlite_config, duration=0)
    following = _read_audit_state(sqlite_config)
    assert following is not None
    assert following.generation == (2 if interrupted_first_audit else 1)
    assert following.lease_expires_at is None
    assert following.last_audit_at == audit_clock.now
    assert following.audit_duration_microseconds == 0
    assert following.initial_catchup_at is None
    assert following.audit_pending == 0
    expected_policy = _POLICY if interrupted_first_audit else DatabaseAuditPolicy()
    assert (
        following.minimum_interval_microseconds
        == expected_policy.minimum_interval_microseconds
    )
    assert (
        following.next_audit_at
        == audit_clock.now + expected_policy.minimum_interval_microseconds
    )
    restarted = _start(admin)
    assert restarted.full_audit is None
    assert restarted.reason is DatabaseAuditReason.INITIAL_CATCHUP


def test_offline_audit_clock_regression_preserves_completed_catchup(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    sqlite_config: CoreConfig,
) -> None:
    first = _start(admin)
    admin.mark_initial_catchup_complete(first.session)
    audit_clock.now = first.last_full_audit_at - 10_000_000
    admin.check()
    _record_offline(sqlite_config)
    following = _read_audit_state(sqlite_config)
    assert following is not None
    assert following.initial_catchup_at == audit_clock.now
    restarted = _start(admin)
    assert restarted.full_audit is None
    assert not restarted.initial_catchup_pending
    assert restarted.reason is DatabaseAuditReason.RECENT_AUDIT
    assert (
        admin.mark_initial_catchup_complete(restarted.session).next_full_audit_at
        == restarted.next_full_audit_at
    )


def test_offline_audit_generation_boundary_never_wraps_or_reuses_old_authority(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    sqlite_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _start(admin)
    with RepositoryContext.from_config(sqlite_config).SQLConnector() as connector:
        connector.execute(
            "UPDATE operational_database_audit_states SET generation = %s",
            (INT63_MAX - 1,),
        )
    previous = _read_audit_state(sqlite_config)
    assert previous is not None
    # Even a repeated random token cannot revive authority across generations.
    monkeypatch.setattr(secrets, "token_bytes", lambda _size: previous.owner_token)
    admin.check()
    _record_offline(sqlite_config)
    following = _read_audit_state(sqlite_config)
    assert following is not None
    assert following.generation == INT63_MAX
    assert following.owner_token == previous.owner_token
    with pytest.raises(DatabaseAuditSessionLostError):
        admin.finish_ingest_runtime(previous.session)
    with pytest.raises(ValueError, match="portable timestamp range"):
        _record_offline(sqlite_config)
    assert _read_audit_state(sqlite_config) == following


@pytest.mark.parametrize("duration", [-1, True, INT63_MAX + 1])
def test_offline_audit_invalid_duration_cannot_replace_baseline(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    sqlite_config: CoreConfig,
    duration: int,
) -> None:
    _start(admin)
    previous = _read_audit_state(sqlite_config)
    with pytest.raises(ValueError, match="offline audit duration"):
        _record_offline(sqlite_config, duration=duration)
    assert _read_audit_state(sqlite_config) == previous


def test_offline_audit_deadline_boundary_rejects_overflow_without_writes(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    sqlite_config: CoreConfig,
) -> None:
    _start(admin)
    audit_clock.now = INT63_MAX - _POLICY.minimum_interval_microseconds
    _record_offline(sqlite_config, duration=0)
    previous = _read_audit_state(sqlite_config)
    assert previous is not None
    assert previous.next_audit_at == INT63_MAX
    audit_clock.advance(1)
    with pytest.raises(ValueError, match="portable timestamp range"):
        _record_offline(sqlite_config, duration=0)
    assert _read_audit_state(sqlite_config) == previous


@pytest.mark.parametrize(
    "assignment",
    [
        "validator_version = NULL",
        "audit_pending = 1, lease_expires_at = NULL",
        "owner_token = X'01'",
        "generation = 0",
    ],
)
def test_offline_audit_rejects_malformed_retained_state_without_repair(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    sqlite_config: CoreConfig,
    assignment: str,
) -> None:
    _start(admin)
    context = RepositoryContext.from_config(sqlite_config)
    with context.SQLConnector() as connector:
        connector.execute("PRAGMA ignore_check_constraints = ON")
        connector.execute(f"UPDATE operational_database_audit_states SET {assignment}")
        previous = connector.fetch_all(
            "SELECT * FROM operational_database_audit_states"
        )
    with pytest.raises(DatabaseAuditStateError):
        _record_offline(sqlite_config)
    with context.SQLConnector() as connector:
        assert (
            connector.fetch_all("SELECT * FROM operational_database_audit_states")
            == previous
        )


@pytest.mark.parametrize("retained", [False, True])
def test_offline_audit_record_rolls_back_with_conversion_transaction(
    admin: VNextDatabaseAdminFacade,
    audit_clock: _Clock,
    sqlite_config: CoreConfig,
    retained: bool,
) -> None:
    if retained:
        _start(admin)
    previous = _read_audit_state(sqlite_config)
    admin.check()
    context = RepositoryContext.from_config(sqlite_config)
    with pytest.raises(RuntimeError, match="READY activation failed"):
        with context.SQLConnector() as connector:
            with connector.transaction():
                audit_module.record_offline_audit_completion(
                    VNextUnitOfWork(connector, backend=context.sql_type),
                    duration_microseconds=23_000_000,
                )
                assert audit_module.read_database_audit_state(connector) != previous
                raise RuntimeError("READY activation failed")
    assert _read_audit_state(sqlite_config) == previous
    if previous is not None:
        admin.renew_ingest_runtime(previous.session, _LEASE)


@pytest.mark.mariadb
@pytest.mark.mariadb_smoke
@pytest.mark.cleanup_acceptance
def test_mariadb_full_clean_quick_and_interrupted_runtime_fencing(
    mariadb_config: CoreConfig,
    audit_clock: _Clock,
) -> None:
    admin = VNextDatabaseAdminFacade(mariadb_config)
    replacement = VNextDatabaseAdminFacade(mariadb_config)
    try:
        admin.initialize()
        first = _start(admin)
        assert first.full_audit is not None
        assert first.reason is DatabaseAuditReason.FIRST_RUN
        admin.mark_initial_catchup_complete(first.session)
        admin.finish_ingest_runtime(first.session)
        quick = _start(replacement)
        assert quick.full_audit is None
        assert quick.reason is DatabaseAuditReason.RECENT_AUDIT
        assert audit_clock.full_checks == 1
        replacement.renew_ingest_runtime(quick.session, _LEASE)
        with pytest.raises(DatabaseAuditSessionUnavailableError):
            _start(admin)
        audit_clock.advance(_LEASE)
        recovered = _start(admin)
        assert recovered.full_audit is not None
        assert recovered.reason is DatabaseAuditReason.PREVIOUS_INTERRUPTION
        assert audit_clock.full_checks == 2
        with pytest.raises(DatabaseAuditSessionLostError):
            replacement.finish_ingest_runtime(quick.session)
        admin.finish_ingest_runtime(recovered.session)
    finally:
        replacement.close()
        admin.close()
