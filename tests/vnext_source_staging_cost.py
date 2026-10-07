"""Native source-selection budgets fixed before the window implementation.

The 256-position assembly window is protocol-owned, not fitted to timing.
The SQLite query's inspected VDBE has one expected-range loop, two singleton
joins, fewer than 32 instructions per candidate and fewer than 128 fixed
instructions. MariaDB may visit at most that range and two full-PK joins.
The deliberately unbounded reader is retained only as an executed negative
control. Both readers use the complete public source workflow with FKs enabled.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import patch

from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_source_build_repository import (
    PendingSourceGallery,
    SourceBuildNotReadyError,
    SourceBuildRepository,
)

WINDOW = 256
SQLITE_VM_LIMIT = 128 + 32 * WINDOW
HANDLER_LIMITS = {
    "Handler_read_first": 0,
    "Handler_read_key": 2 * WINDOW + 4,
    "Handler_read_last": 0,
    "Handler_read_next": WINDOW,
    "Handler_read_prev": 0,
    "Handler_read_rnd": 0,
    "Handler_read_rnd_deleted": 0,
    "Handler_read_rnd_next": 0,
}
QUERY_PREFIX = "SELECT expected.position, expected.gallery_id, identity.locator_sha256 "
UNBOUNDED_QUERY = (
    QUERY_PREFIX + "FROM catalog_source_build_expected_gallery AS expected "
    "LEFT JOIN catalog_gallery_identities AS identity "
    "ON identity.gallery_id = expected.gallery_id "
    "LEFT JOIN catalog_source_build_galleries AS member "
    "ON member.build_id = expected.build_id "
    "AND member.gallery_id = expected.gallery_id "
    "WHERE expected.build_id = %s AND member.gallery_id IS NULL "
    "ORDER BY expected.position LIMIT 1"
)


@dataclass
class PendingQuery:
    position: int | None = None
    window_start: int | None = None
    window_end: int | None = None
    sqlite_vm_steps: int | None = None
    handler_reads: dict[str, int] = field(default_factory=dict)


@dataclass
class SelectionWork:
    samples: list[PendingQuery] = field(default_factory=list)
    connections: int = 0
    source_sqlite_vm_steps: int = 0
    source_handler_reads: dict[str, int] = field(default_factory=dict)


def _handlers(connector: SQLConnector) -> dict[str, int]:
    return {
        str(name): int(value)
        for name, value in connector.fetch_all(
            "SHOW SESSION STATUS LIKE 'Handler_read_%'"
        )
    }


@contextmanager
def observe_pending_queries(backend: str) -> Iterator[SelectionWork]:
    """Instrument the real query once; no replay is added to measured work."""

    work = SelectionWork()
    samples = work.samples
    connector_type = SQLiteConnector if backend == "sqlite" else MariaDBConnector
    original = connector_type.fetch_one
    original_connect = connector_type.connect
    original_close = connector_type.close
    connections: dict[int, dict[str, int]] = {}

    def source_tick() -> int:
        work.source_sqlite_vm_steps += 1
        return 0

    def connect(connector: SQLiteConnector | MariaDBConnector) -> None:
        original_connect(connector)  # type: ignore[arg-type]  # selected backend type.
        work.connections += 1
        connections[id(connector)] = (
            _handlers(connector) if isinstance(connector, MariaDBConnector) else {}
        )
        if isinstance(connector, SQLiteConnector):
            connector.connection.set_progress_handler(source_tick, 1)

    def close(connector: SQLiteConnector | MariaDBConnector) -> None:
        initial = connections.pop(id(connector), None)
        try:
            if initial is not None and isinstance(connector, MariaDBConnector):
                current = _handlers(connector)
                for name, value in initial.items():
                    delta = current[name] - value
                    if delta < 0:
                        raise AssertionError("source Handler counters decreased")
                    work.source_handler_reads[name] = (
                        work.source_handler_reads.get(name, 0) + delta
                    )
            if isinstance(connector, SQLiteConnector):
                connector.connection.set_progress_handler(None, 0)
        finally:
            original_close(connector)  # type: ignore[arg-type]  # selected backend type.

    def observed(
        connector: SQLiteConnector | MariaDBConnector,
        query: str,
        data: tuple[Any, ...] = (),
    ) -> tuple[Any, ...]:
        if not query.startswith(QUERY_PREFIX):
            return original(connector, query, data)  # type: ignore[arg-type]  # backend selected above.
        sample = PendingQuery()
        if len(data) == 3:
            sample.window_start, sample.window_end = data[1:]
        before = _handlers(connector) if isinstance(connector, MariaDBConnector) else {}
        steps = 0

        def tick() -> int:
            nonlocal steps
            steps += 1
            source_tick()
            return 0

        if isinstance(connector, SQLiteConnector):
            connector.connection.set_progress_handler(tick, 1)
        try:
            result = original(connector, query, data)  # type: ignore[arg-type]  # exact selected native type.
        finally:
            if isinstance(connector, SQLiteConnector):
                connector.connection.set_progress_handler(source_tick, 1)
        if isinstance(connector, SQLiteConnector):
            sample.sqlite_vm_steps = steps
        else:
            after = _handlers(connector)
            if before.keys() != after.keys():
                raise AssertionError("native Handler counter set changed")
            sample.handler_reads = {
                name: after[name] - value for name, value in before.items()
            }
        sample.position = int(result[0]) if result else None
        samples.append(sample)
        return result

    with (
        patch.object(connector_type, "fetch_one", observed),
        patch.object(connector_type, "connect", connect),
        patch.object(connector_type, "close", close),
    ):
        yield work
    if connections:
        raise AssertionError("measured source connection did not close")


def budget_failures(samples: list[PendingQuery], backend: str) -> list[str]:
    """Fixed bounds concern actual backend work, not wrapper call counts."""

    if not samples:
        return ["pending-query measurement is empty"]
    failures = []
    for index, sample in enumerate(samples):
        if backend == "sqlite":
            steps = sample.sqlite_vm_steps
            if steps is None or not 0 < steps <= SQLITE_VM_LIMIT:
                failures.append(
                    f"query {index}: SQLite VM work {steps} exceeds {SQLITE_VM_LIMIT}"
                )
        elif not HANDLER_LIMITS.keys() <= sample.handler_reads.keys() or any(
            not 0 <= value <= HANDLER_LIMITS.get(name, 0)
            for name, value in sample.handler_reads.items()
        ):
            failures.append(
                f"query {index}: MariaDB work exceeds the bounded PK window"
            )
    return failures


def _unbounded_pending(
    connector: SQLConnector, *, build_id: bytes
) -> PendingSourceGallery | None:
    """Execute the old global-prefix algorithm, preserving valid workflow output."""

    if connector.fetch_one(
        "SELECT state FROM operational_source_build_discovery_checkpoints WHERE build_id = %s",
        (build_id,),
    ) != ("COMPLETE",):
        raise SourceBuildNotReadyError("negative control requires complete discovery")
    row = connector.fetch_one(UNBOUNDED_QUERY, (build_id,))
    return None if not row else PendingSourceGallery(build_id, *row)


@contextmanager
def unbounded_pending_control() -> Iterator[None]:
    """Replace only selection; real authority, attachment and assembly still run."""

    with patch.object(
        SourceBuildRepository,
        "get_pending_assembly_gallery",
        staticmethod(_unbounded_pending),
    ):
        yield
