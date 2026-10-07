"""Changed-hash page write costs and recovery through the public workflow.

The fixed budget is one connector batch per nonempty authenticated page, and
zero target writes for terminal/replay. Native row writes remain proportional
to the selected hashes; this contract makes no whole-workflow wall-time claim.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import patch

import pytest
from vnext_fault_harness import open_connector
from vnext_pipeline import (
    Clock,
    MemoryLibrary,
    MemorySource,
    claim_session,
    drain_maintenance,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_ingest_turn,
)
from vnext_test_database import DatabaseFactory, inspect_all

from h2hdb import CoreConfig, VNextCatalogFacade, VNextIngestFacade
from h2hdb import vnext_analysis_repository as analysis
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_transaction import VNextUnitOfWork

_TARGET = "INSERT INTO catalog_analysis_changed_file_hashes "
_STATE_TABLES = (
    "catalog_analysis_changed_file_hashes",
    "catalog_analysis_checkpoints",
    "catalog_analysis_batch_receipts",
    "catalog_analysis_batch_receipt_stored",
)


class _InterruptedBatch(Exception):
    pass


@dataclass
class BatchWrites:
    rows: int = 0
    execute: int = 0
    execute_many: int = 0

    def require_budget(self, *, replayed: bool) -> None:
        assert 0 <= self.rows <= 128
        expected = int(bool(self.rows) and not replayed)
        assert self.execute == 0, "changed-hash per-row write budget exceeded"
        assert self.execute_many == expected, "changed-hash batch write budget exceeded"


@dataclass
class WorkflowWrites:
    pages: list[BatchWrites] = field(default_factory=list)
    replay_pages: list[BatchWrites] = field(default_factory=list)
    rolled_back: bool = False
    restarted_replay: bool = False
    next_claim_acquired: bool = False


def _state(config: CoreConfig) -> tuple[Any, ...]:
    with closing(open_connector(config)) as connector, connector.read_transaction():
        return tuple(
            tuple(sorted(inspect_all(connector, f"SELECT * FROM {table}"), key=repr))
            for table in _STATE_TABLES
        )


def exercise_changed_hash_workflow(
    config: CoreConfig,
    *,
    hashes: int,
    per_row: bool = False,
    fault: str | None = None,
    native_observer: Callable[[SQLConnector, Callable[[], None], int], None]
    | None = None,
) -> WorkflowWrites:
    """Use real source/analysis/publication writers with FKs and final READY.

    ``per_row`` is the deliberately regressed pre-batching target INSERT loop.
    ``native_observer`` is only for opt-in measurements, outside the test budget.
    """
    initialize_database(config)
    source = MemorySource(
        [gallery(1001, pages=[f"hash-{index}".encode() for index in range(hashes)])]
        if hashes
        else []
    )
    library = MemoryLibrary(source)
    measured = WorkflowWrites()
    original_batch = analysis.AnalysisRepository.process_changed_file_hash_batch
    original_commit = VNextIngestFacade.commit_analysis_step
    failed = False
    replayed = False
    response_armed = False
    checkpoint: tuple[Any, ...] | None = None
    clock = Clock()
    native_type = (
        SQLiteConnector if config.database.sql_type == "sqlite" else MariaDBConnector
    )
    original_native_commit = native_type.commit

    def native_commit(connector: Any) -> None:
        nonlocal response_armed, replayed
        original_native_commit(connector)
        if response_armed:
            response_armed = False
            replayed = True
            raise _InterruptedBatch

    def observed(work: VNextUnitOfWork, **kwargs: Any) -> Any:
        nonlocal failed, response_armed
        connector = work.connector
        assert connector.fetch_one(
            "PRAGMA foreign_keys"
            if config.database.sql_type == "sqlite"
            else "SELECT @@SESSION.foreign_key_checks"
        ) == (1,)
        writes = BatchWrites()
        execute = connector.execute
        execute_many = connector.execute_many

        def run(action: Callable[[], None], count: int) -> None:
            if native_observer is None:
                action()
            else:
                native_observer(connector, action, count)

        def single(query: str, values: tuple[Any, ...] = ()) -> None:
            if query.startswith(_TARGET):
                writes.execute += 1
                run(lambda: execute(query, values), 1)
            else:
                execute(query, values)

        def many(query: str, values: list[tuple[Any, ...]]) -> None:
            nonlocal failed
            if not query.startswith(_TARGET):
                execute_many(query, values)
            elif fault == "partial_batch" and not failed:
                assert len(values) > 1
                execute_many(query, values[:1])
                failed = True
                raise _InterruptedBatch
            elif per_row:
                for row in values:
                    single(query, row)
            else:
                writes.execute_many += 1
                run(lambda: execute_many(query, values), len(values))

        with (
            patch.object(connector, "execute", single),
            patch.object(connector, "execute_many", many),
        ):
            result = original_batch(work, **kwargs)
        writes.rows = result.row_count
        (measured.replay_pages if result.replayed else measured.pages).append(writes)
        if fault == "response_loss" and not replayed and result.row_count:
            response_armed = True
        return result

    def commit(facade: VNextIngestFacade, session: Any, prepared: Any) -> Any:
        nonlocal checkpoint
        issued = prepared._issued._payload
        changed = issued is not None and issued.stage == b"changed_file_hash"
        if changed and fault == "partial_batch" and not failed:
            checkpoint = _state(config)
        try:
            result = original_commit(facade, session, prepared)
        except _InterruptedBatch:
            if fault == "partial_batch":
                assert checkpoint is not None and _state(config) == checkpoint
                measured.rolled_back = True
                result = original_commit(facade, session, prepared)
            else:
                assert fault == "response_loss" and replayed
                checkpoint = _state(config)
                # Native commit succeeded but its response was lost before the
                # facade could advance its local state. Retry with a new facade.
                with closing(VNextIngestFacade(config, clock=clock)) as restarted:
                    result = original_commit(restarted, session, prepared)
                assert result.replayed and result.processed_rows > 0
                assert _state(config) == checkpoint
                measured.restarted_replay = True
        return result

    with (
        patch.object(
            analysis.AnalysisRepository, "process_changed_file_hash_batch", observed
        ),
        patch.object(VNextIngestFacade, "commit_analysis_step", commit),
        patch.object(native_type, "commit", native_commit),
        closing(VNextIngestFacade(config, clock=clock)) as facade,
    ):
        run_ingest_turn(
            facade,
            source=source,
            library=library,
            policy=ingest_policy(artifacts_required=False),
        )
        drain_maintenance(facade)
        with closing(VNextCatalogFacade(config)) as catalog:
            assert catalog.get_catalog_revision().publication_count == int(hashes > 0)
        assert full_check(config).state == "READY"
        next_session = claim_session(facade, periodic=True)
        assert next_session.ingest_generation > 0
        measured.next_claim_acquired = True
    assert full_check(config).state == "READY"
    assert sum(page.rows for page in measured.pages) == hashes
    assert measured.pages[-1].rows == 0
    return measured


@pytest.mark.performance_acceptance
@pytest.mark.parametrize(
    "hashes",
    (
        0,
        1,
        *(
            pytest.param(count, marks=pytest.mark.deep)
            for count in (127, 128, 129, 257)
        ),
    ),
)
def test_changed_hash_write_budget_across_capacity(
    database_factory: DatabaseFactory, hashes: int
) -> None:
    measured = exercise_changed_hash_workflow(database_factory.config(), hashes=hashes)
    for page in measured.pages:
        page.require_budget(replayed=False)


@pytest.mark.parametrize("fault", ("partial_batch", "response_loss"))
@pytest.mark.deep
@pytest.mark.performance_acceptance
def test_changed_hash_batch_recovery_preserves_exact_authority(
    database_factory: DatabaseFactory, fault: str
) -> None:
    measured = exercise_changed_hash_workflow(
        database_factory.config(), hashes=129, fault=fault
    )
    assert measured.rolled_back == (fault == "partial_batch")
    assert measured.restarted_replay == (fault == "response_loss")
    for page in measured.pages:
        page.require_budget(replayed=False)
    for page in measured.replay_pages:
        page.require_budget(replayed=True)


@pytest.mark.deep
@pytest.mark.performance_acceptance
def test_changed_hash_per_row_negative_control_fails_same_budget(
    database_factory: DatabaseFactory,
) -> None:
    measured = exercise_changed_hash_workflow(
        database_factory.config(), hashes=129, per_row=True
    )
    with pytest.raises(AssertionError, match="per-row write budget exceeded"):
        for page in measured.pages:
            page.require_budget(replayed=False)
