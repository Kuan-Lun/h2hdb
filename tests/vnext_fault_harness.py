"""Statement-level fault injection and exact database snapshots for tests.

The harness wraps the two production connectors so a workflow can be
interrupted before any mutation statement (transaction rollback) or right after
any successful commit (response loss).  It also captures exact row-level
snapshots of every physical table so rollback exactness and replay convergence
are checked against the real database rather than against test bookkeeping.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Literal, cast

import pytest

import h2hdb.mariadb_connector as mariadb_connector_module
import h2hdb.sqlite_connector as sqlite_connector_module
from h2hdb import CoreConfig
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.repository import RepositoryContext
from h2hdb.schema_epoch import SchemaObjectKind
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider

EPOCH_CONTROL_TABLE = "h2hdb_schema_epoch"
_MUTATION_KEYWORDS = ("INSERT", "UPDATE", "DELETE", "REPLACE")


class InjectedFault(RuntimeError):
    """Raised by the harness at the configured statement boundary."""


@dataclass(frozen=True, slots=True)
class TransactionRecord:
    """One committed write transaction: its mutation statements and ordinals."""

    first_mutation: int
    commit: int
    statements: tuple[str, ...]

    @property
    def shape(self) -> tuple[str, ...]:
        return tuple(_normalize(sql) for sql in self.statements)


def _normalize(sql: str) -> str:
    return " ".join(sql.split())


@dataclass(slots=True)
class FaultInjector:
    """Count mutation statements and commits; fire at most one fault.

    ``fail_before_mutation`` is the one-based ordinal of the mutation statement
    (INSERT/UPDATE/DELETE across every connection) that must raise before it
    executes.  ``fail_after_commit`` is the one-based ordinal of the commit that
    must raise *after* the database has durably committed, modelling a lost
    commit response.  ``on_first_mutation`` is invoked with the ordinal of the
    next statement whenever a new write transaction starts, so callers can
    capture the exact pre-transaction snapshot of the transaction that will be
    interrupted.
    """

    fail_before_mutation: int | None = None
    fail_after_commit: int | None = None
    mutations: int = 0
    commits: int = 0
    fired: str | None = None
    fired_sql: str | None = None
    transaction_mutations: int = 0
    statements: list[str] = field(default_factory=list)
    transactions: list[TransactionRecord] = field(default_factory=list)
    _current: list[str] = field(default_factory=list)
    _current_first: int = 0
    on_first_mutation: Callable[[int], None] | None = None

    def before_mutation(self, sql: str) -> None:
        if self.transaction_mutations == 0:
            self._current = []
            self._current_first = self.mutations + 1
            if self.on_first_mutation is not None:
                self.on_first_mutation(self.mutations + 1)
        self.mutations += 1
        self.transaction_mutations += 1
        self.statements.append(sql)
        self._current.append(sql)
        if (
            self.fired is None
            and self.fail_before_mutation is not None
            and self.mutations == self.fail_before_mutation
        ):
            self.fired = "before_mutation"
            self.fired_sql = sql
            raise InjectedFault(
                f"injected fault before mutation {self.mutations}: {_head(sql)}"
            )

    def after_commit(self) -> None:
        self.commits += 1
        if self.transaction_mutations:
            self.transactions.append(
                TransactionRecord(
                    self._current_first,
                    self.commits,
                    tuple(self._current),
                )
            )
        self.transaction_mutations = 0
        self._current = []
        if (
            self.fired is None
            and self.fail_after_commit is not None
            and self.commits == self.fail_after_commit
        ):
            self.fired = "after_commit"
            raise InjectedFault(f"injected response loss after commit {self.commits}")

    def on_rollback(self) -> None:
        self.transaction_mutations = 0
        self._current = []


@dataclass(frozen=True, slots=True)
class FaultPoint:
    """One deduplicated fault point derived from a recorded dry run."""

    kind: str
    ordinal: int
    shape_index: int
    statement_index: int
    statement: str


def fault_points(
    injector: FaultInjector,
    *,
    kinds: Sequence[str] = ("before_mutation", "after_commit"),
) -> tuple[FaultPoint, ...]:
    """Every distinct transaction shape, interrupted before each statement
    and after its commit, taken from the shape's first occurrence."""

    seen: dict[tuple[str, ...], int] = {}
    points: list[FaultPoint] = []
    for record in injector.transactions:
        shape = record.shape
        if shape in seen:
            continue
        shape_index = len(seen)
        seen[shape] = shape_index
        if "before_mutation" in kinds:
            for offset, statement in enumerate(record.statements):
                points.append(
                    FaultPoint(
                        "before_mutation",
                        record.first_mutation + offset,
                        shape_index,
                        offset,
                        _head(statement),
                    )
                )
        if "after_commit" in kinds:
            points.append(
                FaultPoint(
                    "after_commit",
                    record.commit,
                    shape_index,
                    len(record.statements),
                    _head(record.statements[-1]),
                )
            )
    return tuple(points)


def transaction_shapes(injector: FaultInjector) -> tuple[tuple[str, ...], ...]:
    seen: dict[tuple[str, ...], None] = {}
    for record in injector.transactions:
        seen.setdefault(record.shape, None)
    return tuple(seen)


def _head(sql: str) -> str:
    return " ".join(sql.split())[:96]


def _is_mutation(sql: str) -> bool:
    remaining = sql.lstrip()
    while remaining.startswith("/*"):
        end = remaining.find("*/", 2)
        if end < 0:
            return False
        remaining = remaining[end + 2 :].lstrip()
    keyword = remaining.split(maxsplit=1)[0].upper() if remaining else ""
    return keyword in _MUTATION_KEYWORDS


class _FaultSQLiteConnector(SQLiteConnector):
    injector: FaultInjector

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        if _is_mutation(query):
            self.injector.before_mutation(query)
        super().execute(query, data)

    def execute_affected(self, query: str, data: tuple[Any, ...] = ()) -> int:
        if _is_mutation(query):
            self.injector.before_mutation(query)
        return super().execute_affected(query, data)

    def execute_many(self, query: str, data: list[tuple[Any, ...]]) -> None:
        if _is_mutation(query):
            self.injector.before_mutation(query)
        super().execute_many(query, data)

    def commit(self) -> None:
        super().commit()
        self.injector.after_commit()

    def rollback(self) -> None:
        super().rollback()
        self.injector.on_rollback()


class _FaultMariaDBConnector(MariaDBConnector):
    injector: FaultInjector

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        if _is_mutation(query):
            self.injector.before_mutation(query)
        super().execute(query, data)

    def execute_affected(self, query: str, data: tuple[Any, ...] = ()) -> int:
        if _is_mutation(query):
            self.injector.before_mutation(query)
        return super().execute_affected(query, data)

    def execute_many(self, query: str, data: list[tuple[Any, ...]]) -> None:
        if _is_mutation(query):
            self.injector.before_mutation(query)
        super().execute_many(query, data)

    def commit(self) -> None:
        super().commit()
        self.injector.after_commit()

    def rollback(self) -> None:
        super().rollback()
        self.injector.on_rollback()


@contextmanager
def fault_injection(
    monkeypatch: pytest.MonkeyPatch,
    injector: FaultInjector,
) -> Iterator[FaultInjector]:
    """Route every production connector opened by facades through ``injector``.

    ``RepositoryContext.from_config`` imports the connector classes lazily from
    their modules, so patching the module attributes is sufficient and leaves
    the production classes untouched.
    """

    sqlite_type = type(
        "InjectedSQLiteConnector",
        (_FaultSQLiteConnector,),
        {"injector": injector},
    )
    mariadb_type = type(
        "InjectedMariaDBConnector",
        (_FaultMariaDBConnector,),
        {"injector": injector},
    )
    with monkeypatch.context() as patch:
        patch.setattr(sqlite_connector_module, "SQLiteConnector", sqlite_type)
        patch.setattr(mariadb_connector_module, "MariaDBConnector", mariadb_type)
        yield injector


Backend = Literal["sqlite", "mariadb"]


def physical_tables(backend: Backend) -> tuple[str, ...]:
    """Every generated physical table plus the epoch-control relation."""

    definition = GeneratedVNextSchemaProvider(backend).definition
    tables = sorted(
        value.name
        for value in definition.expected_objects
        if value.kind is SchemaObjectKind.TABLE
    )
    return (*tables, EPOCH_CONTROL_TABLE)


def backend_of(config: CoreConfig) -> Backend:
    backend = config.database.sql_type
    if backend not in {"sqlite", "mariadb"}:
        raise ValueError(f"unsupported backend {backend!r}")
    return cast(Backend, backend)


def open_connector(config: CoreConfig) -> SQLConnector:
    connector = RepositoryContext.from_config(config).SQLConnector()
    connector.connect()
    return connector


def _row_key(row: tuple[Any, ...]) -> tuple[tuple[str, str], ...]:
    return tuple((type(value).__name__, repr(value)) for value in row)


def snapshot_database(
    config: CoreConfig,
    *,
    tables: Sequence[str] | None = None,
) -> dict[str, tuple[tuple[Any, ...], ...]]:
    """Exact sorted contents of every physical table in one read snapshot."""

    names = tuple(tables) if tables is not None else physical_tables(backend_of(config))
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            return {
                name: tuple(
                    sorted(connector.fetch_all(f"SELECT * FROM {name}"), key=_row_key)
                )
                for name in names
            }
    finally:
        connector.close()


def snapshot_difference(
    before: Mapping[str, tuple[tuple[Any, ...], ...]],
    after: Mapping[str, tuple[tuple[Any, ...], ...]],
) -> dict[str, tuple[int, int]]:
    """Tables whose exact row sets differ, with (before, after) row counts."""

    return {
        name: (len(before.get(name, ())), len(after.get(name, ())))
        for name in sorted(set(before) | set(after))
        if before.get(name, ()) != after.get(name, ())
    }


def row_counts(
    snapshot: Mapping[str, tuple[tuple[Any, ...], ...]],
) -> dict[str, int]:
    return {name: len(rows) for name, rows in snapshot.items() if rows}


def count_mutations(
    monkeypatch: pytest.MonkeyPatch,
    workflow: Callable[[], object],
) -> FaultInjector:
    """Dry-run ``workflow`` and return its exact mutation/commit counters."""

    injector = FaultInjector()
    with fault_injection(monkeypatch, injector):
        workflow()
    return injector


def run_fault_point(
    monkeypatch: pytest.MonkeyPatch,
    *,
    config: CoreConfig,
    point: FaultPoint,
    workflow: Callable[[], object],
    snapshot_tables: Sequence[str] | None = None,
) -> tuple[FaultInjector, dict[str, tuple[tuple[Any, ...], ...]] | None]:
    """Run ``workflow`` with exactly one fault and return the pre-state of the
    interrupted transaction (for ``before_mutation`` points).

    The snapshot is taken once, through an independent read transaction, when
    the transaction that will be interrupted issues its first mutation, so it
    reflects only committed state.
    """

    injector = FaultInjector(
        fail_before_mutation=point.ordinal if point.kind == "before_mutation" else None,
        fail_after_commit=point.ordinal if point.kind == "after_commit" else None,
    )
    pre_transaction: dict[str, tuple[tuple[Any, ...], ...]] | None = None
    if point.kind == "before_mutation":
        interrupted_first = point.ordinal - point.statement_index

        def capture(next_ordinal: int) -> None:
            nonlocal pre_transaction
            if next_ordinal == interrupted_first:
                pre_transaction = snapshot_database(config, tables=snapshot_tables)

        injector.on_first_mutation = capture
    with fault_injection(monkeypatch, injector):
        with pytest.raises(InjectedFault):
            workflow()
    if injector.fired is None:
        raise AssertionError("the configured fault point never fired")
    return injector, pre_transaction


MAINTENANCE_GATE_TABLES = frozenset(
    {
        "operational_maintenance_gate_generations",
        "operational_maintenance_gate_heads",
        "operational_maintenance_gate_owners",
        "operational_maintenance_gate_holders",
    }
)


def assert_exact_rollback(
    config: CoreConfig,
    pre_transaction: Mapping[str, tuple[tuple[Any, ...], ...]] | None,
    *,
    snapshot_tables: Sequence[str] | None = None,
    compensation_tables: frozenset[str] = frozenset(),
) -> None:
    """Prove the interrupted transaction left no row behind.

    ``compensation_tables`` names the only tables a facade may touch in a
    separate compensating transaction after the failure (the current-only
    maintenance facade releases its EXCLUSIVE gate lease); every other table
    must equal the committed state captured when the interrupted transaction
    began.
    """

    if pre_transaction is None:
        raise AssertionError("no pre-transaction snapshot was captured")
    after = snapshot_database(config, tables=snapshot_tables)
    difference = snapshot_difference(pre_transaction, after)
    leaked = {
        name: value
        for name, value in difference.items()
        if name not in compensation_tables
    }
    assert leaked == {}, f"interrupted transaction leaked rows: {leaked}"
    if difference:
        # A gate release after failure may only remove live ownership.
        assert not after.get("operational_maintenance_gate_owners"), difference
        assert not after.get("operational_maintenance_gate_holders"), difference


def table_names_touched(injector: FaultInjector) -> frozenset[str]:
    """Physical tables named by the mutation statements the injector saw."""

    names: set[str] = set()
    for sql in injector.statements:
        words = sql.replace("(", " ").split()
        for index, word in enumerate(words):
            if word.upper() in {"INTO", "UPDATE", "FROM"} and index + 1 < len(words):
                candidate = words[index + 1].strip("`\"';,")
                if candidate and candidate[0].isalpha():
                    names.add(candidate)
    return frozenset(names)


def mutation_relations(backend: Backend) -> frozenset[str]:
    return frozenset(physical_tables(backend)) - {EPOCH_CONTROL_TABLE}


def cast_any(value: object) -> Any:
    return cast(Any, value)
