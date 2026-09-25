"""Durable ingest-process audit scheduling, separate from database health.

The singleton is mutable scheduling state. It is neither an immutable audit
receipt nor evidence that later writes are healthy. Explicit full checks remain
read-only; only this coordinator records checks that it executed successfully.
"""

from __future__ import annotations

import secrets
from collections.abc import Callable
from dataclasses import dataclass, replace
from importlib.metadata import version
from time import perf_counter_ns
from typing import TypeVar

from .database_clock import database_unix_microseconds
from .domain import (
    DatabaseAuditPolicy,
    DatabaseAuditReason,
    DatabaseAuditReport,
    DatabaseAuditSession,
    SchemaEpochReadiness,
    SchemaEpochReport,
)
from .repository import RepositoryContext
from .schema_admin import VNextSchemaAdmin
from .schema_epoch import SchemaEpochValidationError
from .sql_connector import SQLConnector
from .vnext_domains import (
    INT63_MAX,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_TABLE = "operational_database_audit_states"
_COLUMNS = (
    "generation, owner_token, lease_expires_at, lease_duration_microseconds, "
    "minimum_interval_microseconds, duration_multiplier, last_audit_at, "
    "audit_duration_microseconds, validator_version, next_audit_at, "
    "initial_catchup_at, audit_pending"
)
_FIELDS = tuple(_COLUMNS.split(", "))
_ResultT = TypeVar("_ResultT")


class DatabaseAuditSessionUnavailableError(RuntimeError):
    """Another ingest process holds a live scheduling lease."""


class DatabaseAuditSessionLostError(RuntimeError):
    """The exact ingest-process generation/token was superseded or is closed."""


class DatabaseAuditStateError(SchemaEpochValidationError):
    """Persisted scheduling state is malformed; it cannot select a quick check."""


@dataclass(frozen=True, slots=True)
class _State:
    generation: int
    owner_token: bytes
    lease_expires_at: int | None
    lease_duration_microseconds: int
    minimum_interval_microseconds: int
    duration_multiplier: int
    last_audit_at: int | None
    audit_duration_microseconds: int | None
    validator_version: str | None
    next_audit_at: int | None
    initial_catchup_at: int | None
    audit_pending: int

    @property
    def session(self) -> DatabaseAuditSession:
        return DatabaseAuditSession(self.generation, self.owner_token)

    def values(self) -> tuple[object, ...]:
        return tuple(getattr(self, field) for field in _FIELDS)


def _optional_int(value: object, label: str) -> int | None:
    return None if value is None else require_int63(value, field=label)


def read_database_audit_state(connector: SQLConnector) -> _State | None:
    """Independently validate at most two rows, including all NULL groups."""

    rows = connector.fetch_all(f"SELECT singleton_id, {_COLUMNS} FROM {_TABLE} LIMIT 2")
    if not rows:
        return None
    if len(rows) != 1 or len(rows[0]) != 13 or rows[0][0] != 1:
        raise DatabaseAuditStateError("database audit scheduling state is not singular")
    row = rows[0][1:]
    try:
        token = row[1]
        if isinstance(token, (bytearray, memoryview)):
            token = bytes(token)
        state = _State(
            generation=require_positive_int63(row[0], field="audit generation"),
            owner_token=require_uuid16(token, field="audit owner token"),
            lease_expires_at=_optional_int(row[2], "audit lease expiry"),
            lease_duration_microseconds=require_positive_int63(
                row[3], field="audit lease duration"
            ),
            minimum_interval_microseconds=require_positive_int63(
                row[4], field="audit minimum interval"
            ),
            duration_multiplier=require_positive_int63(
                row[5], field="audit duration multiplier"
            ),
            last_audit_at=_optional_int(row[6], "last audit time"),
            audit_duration_microseconds=_optional_int(row[7], "audit duration"),
            validator_version=_validator_text(row[8]),
            next_audit_at=_optional_int(row[9], "next audit time"),
            initial_catchup_at=_optional_int(row[10], "initial catchup time"),
            audit_pending=require_int63(row[11], field="audit pending"),
        )
    except (TypeError, ValueError) as error:
        raise DatabaseAuditStateError(
            "database audit scheduling fields are invalid"
        ) from error
    audit_values = (
        state.last_audit_at,
        state.audit_duration_microseconds,
        state.validator_version,
        state.next_audit_at,
    )
    if any(value is None for value in audit_values) and not all(
        value is None for value in audit_values
    ):
        raise DatabaseAuditStateError("database audit success fields are incomplete")
    if state.audit_pending not in (0, 1):
        raise DatabaseAuditStateError("database audit pending state is invalid")
    if state.last_audit_at is None and state.audit_pending != 1:
        raise DatabaseAuditStateError("database audit has no successful baseline")
    if state.audit_pending and state.lease_expires_at is None:
        raise DatabaseAuditStateError(
            "an unfinished full audit cannot be cleanly closed"
        )
    if state.last_audit_at is not None and (
        state.next_audit_at is None or state.next_audit_at < state.last_audit_at
    ):
        raise DatabaseAuditStateError("next full audit precedes its completed baseline")
    return state


def _validator_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.isascii() or not 1 <= len(value) <= 191:
        raise ValueError("audit validator version must be bounded ASCII")
    return value


def _add_time(timestamp: int, duration: int) -> int:
    if timestamp > INT63_MAX - duration:
        raise ValueError("database audit deadline exceeds the portable timestamp range")
    return timestamp + duration


def _interval(state: _State, duration: int) -> int:
    return max(
        state.minimum_interval_microseconds,
        min(INT63_MAX, duration * state.duration_multiplier),
    )


class DatabaseAuditStateRepository:
    """Persist one atomic scheduling head in a caller-owned locked transaction."""

    @staticmethod
    def save(work: VNextUnitOfWork, previous: _State | None, following: _State) -> None:
        if previous is None:
            work.connector.execute(
                f"INSERT INTO {_TABLE} (singleton_id, {_COLUMNS}) VALUES (1, {', '.join(['%s'] * len(_FIELDS))})",
                following.values(),
            )
        elif previous != following:
            assignments = ", ".join(f"{field} = %s" for field in _FIELDS)
            work.compare_and_swap(
                f"UPDATE {_TABLE} SET {assignments} WHERE singleton_id = 1 AND generation = %s AND owner_token = %s",
                (*following.values(), previous.generation, previous.owner_token),
                authority="database audit scheduling",
            )


class DatabaseAuditManager:
    """Coordinate one ingest runtime with short, exact-token write transactions."""

    def __init__(self, context: RepositoryContext, admin: VNextSchemaAdmin) -> None:
        self._context = context
        self._admin = admin
        self._validator_version = _validator_version()
        _, self._definition = admin._resolve_provider()

    def start(
        self,
        *,
        policy: DatabaseAuditPolicy,
        lease_duration_microseconds: int,
        force_full: bool,
        on_check: Callable[[DatabaseAuditReason], None] | None = None,
    ) -> DatabaseAuditReport:
        if not isinstance(policy, DatabaseAuditPolicy):
            raise TypeError("policy must be DatabaseAuditPolicy")
        duration = require_positive_int63(
            lease_duration_microseconds, field="audit runtime lease duration"
        )
        if type(force_full) is not bool:
            raise TypeError("force_full must be bool")
        readiness = self._admin.check_readiness()

        def reserve(
            work: VNextUnitOfWork, previous: _State | None, now: int
        ) -> tuple[_State, DatabaseAuditReason]:
            if (
                previous is not None
                and previous.lease_expires_at is not None
                and previous.lease_expires_at > now
            ):
                raise DatabaseAuditSessionUnavailableError(
                    "another ingest runtime has a live audit scheduling lease"
                )
            if previous is None:
                state = _State(
                    1,
                    secrets.token_bytes(16),
                    _add_time(now, duration),
                    duration,
                    policy.minimum_interval_microseconds,
                    policy.duration_multiplier,
                    None,
                    None,
                    None,
                    None,
                    None,
                    1,
                )
                reason = DatabaseAuditReason.FIRST_RUN
            else:
                scheduled = replace(
                    previous,
                    minimum_interval_microseconds=policy.minimum_interval_microseconds,
                    duration_multiplier=policy.duration_multiplier,
                )
                # Configuration changes reuse the existing audit/catch-up
                # anchors. A restart itself is never a new scheduling anchor.
                if (
                    scheduled.last_audit_at is not None
                    and scheduled.audit_duration_microseconds is not None
                ):
                    anchor = max(
                        scheduled.last_audit_at, scheduled.initial_catchup_at or 0
                    )
                    scheduled = replace(
                        scheduled,
                        next_audit_at=_add_time(
                            anchor,
                            _interval(scheduled, scheduled.audit_duration_microseconds),
                        ),
                    )
                reason = self._reason(
                    scheduled, now, force_full=force_full, starting=True
                )
                state = replace(
                    scheduled,
                    generation=_add_time(previous.generation, 1),
                    owner_token=secrets.token_bytes(16),
                    lease_expires_at=_add_time(now, duration),
                    lease_duration_microseconds=duration,
                    audit_pending=int(_full(reason)),
                )
            DatabaseAuditStateRepository.save(work, previous, state)
            return state, reason

        state, reason = self._write(reserve)
        return self._complete_check(state, reason, readiness, on_check)

    def renew(
        self, session: DatabaseAuditSession, lease_duration_microseconds: int
    ) -> None:
        self._require_session(session)
        duration = require_positive_int63(
            lease_duration_microseconds, field="audit runtime lease duration"
        )
        # A full audit may own a long SQLite rollback-journal read snapshot.
        # Polling its pending flag is read-only; renewal must not queue a writer
        # behind that reader. Exact-token finalization renews after the audit.
        with self._context.SQLConnector() as connector:
            with connector.read_transaction():
                current = self._owned(read_database_audit_state(connector), session)
                if current.audit_pending:
                    return

        def renew(work: VNextUnitOfWork, state: _State | None, now: int) -> None:
            current = self._owned(state, session)
            if current.audit_pending:
                return
            DatabaseAuditStateRepository.save(
                work,
                current,
                replace(
                    current,
                    lease_expires_at=_add_time(now, duration),
                    lease_duration_microseconds=duration,
                ),
            )

        self._write(renew)

    def check_if_due(
        self,
        session: DatabaseAuditSession,
        *,
        on_check: Callable[[DatabaseAuditReason], None] | None = None,
    ) -> DatabaseAuditReport:
        self._require_session(session)
        readiness = self._admin.check_readiness()

        def select(
            work: VNextUnitOfWork, state: _State | None, now: int
        ) -> tuple[_State, DatabaseAuditReason]:
            current = self._owned(state, session)
            if current.audit_pending:
                raise DatabaseAuditSessionUnavailableError(
                    "this ingest runtime already has a full audit in progress"
                )
            reason = self._reason(current, now, force_full=False, starting=False)
            following = replace(
                current,
                audit_pending=int(_full(reason)),
                lease_expires_at=_add_time(now, current.lease_duration_microseconds),
            )
            DatabaseAuditStateRepository.save(work, current, following)
            return following, reason

        state, reason = self._write(select)
        return self._complete_check(state, reason, readiness, on_check)

    def mark_caught_up(self, session: DatabaseAuditSession) -> DatabaseAuditReport:
        self._require_session(session)
        readiness = self._admin.check_readiness()

        def mark(work: VNextUnitOfWork, state: _State | None, now: int) -> _State:
            current = self._owned(state, session)
            if current.audit_pending:
                raise DatabaseAuditSessionUnavailableError(
                    "cannot finish initial catch-up during a full audit"
                )
            if current.initial_catchup_at is not None:
                return current
            if (
                current.audit_duration_microseconds is None
                or current.next_audit_at is None
            ):
                raise DatabaseAuditStateError(
                    "initial catch-up lacks a completed audit baseline"
                )
            following = replace(
                current,
                initial_catchup_at=now,
                next_audit_at=max(
                    current.next_audit_at,
                    _add_time(
                        now, _interval(current, current.audit_duration_microseconds)
                    ),
                ),
            )
            DatabaseAuditStateRepository.save(work, current, following)
            return following

        return self._report(
            self._write(mark), DatabaseAuditReason.INITIAL_CATCHUP, readiness, None
        )

    def finish(self, session: DatabaseAuditSession) -> None:
        self._require_session(session)

        def finish(work: VNextUnitOfWork, state: _State | None, _now: int) -> None:
            current = self._owned(state, session, allow_closed=True)
            if current.audit_pending:
                raise DatabaseAuditSessionUnavailableError(
                    "an unfinished full audit cannot be marked clean"
                )
            if current.lease_expires_at is not None:
                DatabaseAuditStateRepository.save(
                    work, current, replace(current, lease_expires_at=None)
                )

        self._write(finish)

    def _complete_check(
        self,
        state: _State,
        reason: DatabaseAuditReason,
        readiness: SchemaEpochReadiness,
        on_check: Callable[[DatabaseAuditReason], None] | None,
    ) -> DatabaseAuditReport:
        if on_check is not None:
            try:
                on_check(reason)
            except Exception:
                # Diagnostic delivery cannot alter a durable admission decision.
                pass
        if not _full(reason):
            return self._report(state, reason, readiness, None)
        started = perf_counter_ns()
        full_audit = self._admin.check()
        if self._admin.check_readiness() != readiness:
            raise DatabaseAuditStateError(
                "schema incarnation changed during full audit"
            )
        elapsed = max(0, (perf_counter_ns() - started) // 1_000)

        def complete(work: VNextUnitOfWork, actual: _State | None, now: int) -> _State:
            # Expiry permits takeover. It does not invalidate a completed read
            # audit if no competing owner actually replaced this exact token.
            current = self._owned(actual, state.session)
            if not current.audit_pending:
                raise DatabaseAuditSessionLostError(
                    "the pending full audit was already replaced"
                )
            following = replace(
                current,
                last_audit_at=now,
                audit_duration_microseconds=elapsed,
                validator_version=self._validator_version,
                next_audit_at=_add_time(now, _interval(current, elapsed)),
                # This is a scheduling anchor, not immutable catch-up history.
                # Rebase a future grace anchor after a backwards clock change,
                # so one successful re-audit cannot trigger an endless loop.
                initial_catchup_at=(
                    min(now, current.initial_catchup_at)
                    if current.initial_catchup_at is not None
                    else None
                ),
                audit_pending=0,
                lease_expires_at=_add_time(now, current.lease_duration_microseconds),
            )
            DatabaseAuditStateRepository.save(work, current, following)
            return following

        return self._report(self._write(complete), reason, readiness, full_audit)

    def _reason(
        self, state: _State, now: int, *, force_full: bool, starting: bool
    ) -> DatabaseAuditReason:
        if force_full:
            return DatabaseAuditReason.FORCED
        if starting and state.lease_expires_at is not None:
            return DatabaseAuditReason.PREVIOUS_INTERRUPTION
        if state.last_audit_at is None or state.audit_pending:
            return DatabaseAuditReason.FIRST_RUN
        if now < state.last_audit_at or (
            state.initial_catchup_at is not None and now < state.initial_catchup_at
        ):
            return DatabaseAuditReason.CLOCK_CHANGED
        if state.validator_version != self._validator_version:
            return DatabaseAuditReason.VALIDATOR_CHANGED
        if state.initial_catchup_at is None:
            return DatabaseAuditReason.INITIAL_CATCHUP
        if state.next_audit_at is not None and now >= state.next_audit_at:
            return DatabaseAuditReason.SCHEDULE_DUE
        return DatabaseAuditReason.RECENT_AUDIT

    def _write(
        self, operation: Callable[[VNextUnitOfWork, _State | None, int], _ResultT]
    ) -> _ResultT:
        with self._context.SQLConnector() as connector:
            with connector.transaction():
                work = VNextUnitOfWork(connector, backend=self._context.sql_type)
                # The always-present epoch row serializes first insert as well
                # as subsequent singleton changes on both database backends.
                epoch = work.lock_row(
                    LockRank.MAINTENANCE_GATE,
                    encode_lock_key("database-audit"),
                    "SELECT epoch, schema_version, state, manifest_sha256 FROM h2hdb_schema_epoch WHERE singleton_id = 1",
                )
                expected = (
                    self._definition.epoch,
                    self._definition.schema_version,
                    "READY",
                    bytes.fromhex(self._definition.manifest_sha256),
                )
                if epoch != expected:
                    raise DatabaseAuditStateError(
                        "database audit scheduling requires the exact READY schema"
                    )
                return operation(
                    work,
                    read_database_audit_state(connector),
                    database_unix_microseconds(work),
                )

    @staticmethod
    def _require_session(session: DatabaseAuditSession) -> None:
        if not isinstance(session, DatabaseAuditSession):
            raise TypeError("session must be DatabaseAuditSession")

    @staticmethod
    def _owned(
        state: _State | None,
        session: DatabaseAuditSession,
        *,
        allow_closed: bool = False,
    ) -> _State:
        if (
            state is None
            or state.session != session
            or (state.lease_expires_at is None and not allow_closed)
        ):
            raise DatabaseAuditSessionLostError(
                "the ingest audit scheduling session is stale or closed"
            )
        return state

    @staticmethod
    def _report(
        state: _State,
        reason: DatabaseAuditReason,
        readiness: SchemaEpochReadiness,
        full_audit: SchemaEpochReport | None,
    ) -> DatabaseAuditReport:
        if (
            state.last_audit_at is None
            or state.audit_duration_microseconds is None
            or state.next_audit_at is None
        ):
            raise DatabaseAuditStateError(
                "database audit report has no completed baseline"
            )
        return DatabaseAuditReport(
            state.session,
            readiness,
            full_audit,
            reason,
            state.last_audit_at,
            state.audit_duration_microseconds,
            state.next_audit_at,
            state.initial_catchup_at is None,
        )


def _full(reason: DatabaseAuditReason) -> bool:
    return reason not in (
        DatabaseAuditReason.RECENT_AUDIT,
        DatabaseAuditReason.INITIAL_CATCHUP,
    )


def _validator_version() -> str:
    return f"h2hdb/{version('h2hdb')};database-audit/1"
