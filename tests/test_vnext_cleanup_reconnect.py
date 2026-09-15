"""Actual transaction rollback and committed-response loss across connections."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
from typing import Any
from unittest.mock import patch

import pytest
from vnext_fault_harness import (
    backend_of,
    open_connector,
    physical_tables,
    snapshot_database,
)
from vnext_pipeline import full_check, initialize_database

from h2hdb import CoreConfig
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_cleanup_repository import (
    CleanupBatchCommand,
    CleanupCycle,
    CleanupTargetKind,
    VNextCleanupRepository,
)
from h2hdb.vnext_maintenance_gate_repository import GateLease, MaintenanceGateRepository
from h2hdb.vnext_transaction import VNextUnitOfWork

pytestmark = pytest.mark.cleanup_acceptance

_DIGESTS = tuple(bytes((201,)) + ordinal.to_bytes(31, "big") for ordinal in range(3))


class _AbortTransaction(RuntimeError):
    pass


class _LostCommittedResponse(RuntimeError):
    pass


def _seed(config: CoreConfig) -> tuple[GateLease, CleanupCycle]:
    initialize_database(config)
    backend = backend_of(config)
    with closing(open_connector(config)) as connector:
        with connector.transaction():
            for index, digest in enumerate(_DIGESTS):
                connector.execute(
                    "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
                    "VALUES (%s, %s)",
                    (digest, index),
                )
        with connector.transaction():
            gate = MaintenanceGateRepository.claim_exclusive(
                VNextUnitOfWork(connector, backend=backend),
                now=1,
                lease_duration=100_000,
            )
        with connector.transaction():
            cycle = VNextCleanupRepository.begin_cycle(
                VNextUnitOfWork(connector, backend=backend),
                gate_lease=gate,
                target_kind=CleanupTargetKind.CONTENT_BLOB,
                shard_no=201,
                cycle_cutoff_at=100,
                max_rows_per_transaction=1,
                now=2,
            )
    return gate, cycle


def _snapshot(config: CoreConfig) -> dict[str, tuple[tuple[Any, ...], ...]]:
    # Include every physical cleanup/gate relation, not just their logical
    # projections: an orphan child or partially changed receipt is observable.
    tables = tuple(
        name
        for name in physical_tables(backend_of(config))
        if name == "catalog_content_blobs"
        or name.startswith(("operational_cleanup_", "operational_maintenance_gate_"))
    )
    assert "catalog_content_blobs" in tables
    assert any(name.startswith("operational_cleanup_") for name in tables)
    return snapshot_database(config, tables=tables)


def _blobs(connector: SQLConnector) -> tuple[tuple[bytes, int], ...]:
    return tuple(
        (bytes(row[0]), int(row[1]))
        for row in connector.fetch_all(
            "SELECT file_sha256, size_bytes FROM catalog_content_blobs "
            "ORDER BY file_sha256"
        )
    )


def _finish_after_reconnect(
    config: CoreConfig,
    gate: GateLease,
    cycle: CleanupCycle,
    *,
    already_deleted: int,
) -> None:
    backend = backend_of(config)
    # Every attempt uses a new driver connection, and every committed batch
    # must delete exactly one next key under the original durable bound.
    for deleted in range(already_deleted + 1, 4):
        with closing(open_connector(config)) as connector:
            with connector.transaction():
                results = VNextCleanupRepository.advance_current_only_cycle(
                    VNextUnitOfWork(connector, backend=backend),
                    gate_lease=gate,
                    cycle=cycle,
                    now=10 + deleted,
                )
                assert len(results) == 1
                result = results[0]
                assert result.row_count == 1
                assert result.deleted_count == deleted
                assert not result.replayed and not result.cycle_complete
                assert _blobs(connector) == tuple(
                    (digest, index)
                    for index, digest in enumerate(_DIGESTS)
                    if index >= deleted
                )
    with closing(open_connector(config)) as connector, connector.transaction():
        terminal = VNextCleanupRepository.advance_current_only_cycle(
            VNextUnitOfWork(connector, backend=backend),
            gate_lease=gate,
            cycle=cycle,
            now=20,
        )
        assert terminal[-1].cycle_complete
        assert terminal[-1].deleted_count == 3
        assert all(result.row_count == 0 for result in terminal)
        assert _blobs(connector) == ()
    completed = _snapshot(config)
    with closing(open_connector(config)) as connector, connector.transaction():
        replay = VNextCleanupRepository.advance_current_only_cycle(
            VNextUnitOfWork(connector, backend=backend),
            gate_lease=gate,
            cycle=cycle,
            now=21,
        )
        assert len(replay) == 1 and replay[0].replayed
        assert replay[0].deleted_count == 3
    assert _snapshot(config) == completed
    with closing(open_connector(config)) as connector, connector.transaction():
        MaintenanceGateRepository.release(
            VNextUnitOfWork(connector, backend=backend), gate, now=22
        )
    assert full_check(config).state == "READY"


def test_nonempty_cleanup_abort_restores_exact_facts_and_checkpoint_after_reconnect(
    db_config: CoreConfig,
) -> None:
    gate, cycle = _seed(db_config)
    before = _snapshot(db_config)
    with pytest.raises(_AbortTransaction, match="after nonempty delete"):
        with closing(open_connector(db_config)) as connector, connector.transaction():
            results = VNextCleanupRepository.advance_current_only_cycle(
                VNextUnitOfWork(connector, backend=backend_of(db_config)),
                gate_lease=gate,
                cycle=cycle,
                now=3,
            )
            assert len(results) == 1
            assert results[0].row_count == results[0].deleted_count == 1
            assert not results[0].cycle_complete
            assert _blobs(connector) == ((_DIGESTS[1], 1), (_DIGESTS[2], 2))
            raise _AbortTransaction("after nonempty delete and checkpoint update")
    # Both the managed transaction and its connection have exited. Snapshot
    # opens a genuinely fresh driver connection to inspect persisted state.
    assert _snapshot(db_config) == before
    _finish_after_reconnect(db_config, gate, cycle, already_deleted=0)


def test_nonempty_cleanup_lost_commit_response_replays_exactly_after_reconnect(
    db_config: CoreConfig,
) -> None:
    gate, cycle = _seed(db_config)
    before = _snapshot(db_config)
    command = CleanupBatchCommand(b"lost-commit-response".ljust(32, b"\0"), 1)
    with pytest.raises(_LostCommittedResponse, match="after successful commit"):
        with closing(open_connector(db_config)) as connector:
            with connector.transaction():
                committed = VNextCleanupRepository.advance(
                    VNextUnitOfWork(connector, backend=backend_of(db_config)),
                    gate_lease=gate,
                    cycle=cycle,
                    command=command,
                    now=3,
                )
                assert committed.row_count == committed.deleted_count == 1
                assert not committed.replayed and not committed.cycle_complete
            # Simulate a lost application response only after real DB COMMIT;
            # neither connector results nor persistence are mocked.
            raise _LostCommittedResponse("after successful commit")
    persisted = _snapshot(db_config)
    assert persisted != before
    assert persisted["catalog_content_blobs"] == ((_DIGESTS[1], 1), (_DIGESTS[2], 2))
    with closing(open_connector(db_config)) as connector, connector.transaction():
        with (
            patch.object(connector, "execute", wraps=connector.execute) as execute,
            patch.object(
                connector, "execute_affected", wraps=connector.execute_affected
            ) as execute_affected,
        ):
            replay = VNextCleanupRepository.advance(
                VNextUnitOfWork(connector, backend=backend_of(db_config)),
                gate_lease=gate,
                cycle=cycle,
                command=command,
                now=4,
            )
        execute.assert_not_called()
        execute_affected.assert_not_called()
        assert replay.replayed
        assert replay == replace(committed, replayed=True)
    assert _snapshot(db_config) == persisted
    _finish_after_reconnect(db_config, gate, cycle, already_deleted=1)
