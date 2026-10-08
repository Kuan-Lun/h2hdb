"""Real analysis cleanup range work, continuation and retained-root evidence."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from dataclasses import replace
from typing import Any

import pytest
from vnext_analysis_fixtures import seed_analysis_run
from vnext_publication_cleanup_fixtures import partial_publication_setup
from vnext_test_database import (
    DatabaseFactory,
    connector_backend,
    open_generated_database,
)

from h2hdb import vnext_cleanup_repository as cleanup
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_transaction import VNextUnitOfWork

pytestmark = [pytest.mark.cleanup_acceptance, pytest.mark.deep]

_TABLE = "catalog_analysis_exclusion_delta_anchors"
_PHASE = "AR_EXCLUSION_ANCHOR"
_RETAINED = bytes((23, 255)) + bytes(14)
_PLAN = cleanup._STATIC_PLANS[cleanup.CleanupTargetKind.ANALYSIS_RUN]


def _root(index: int) -> bytes:
    return bytes((23, index)) + bytes(14)


def _file(index: int) -> bytes:
    return index.to_bytes(32, "big")


def _seed(
    connector: SQLConnector, distribution: tuple[int, ...], *, retained: int = 32768
) -> None:
    with partial_publication_setup(connector, backend=connector_backend(connector)):
        connector.execute(
            "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) VALUES (%s, 1)",
            (_file(1),),
        )
        for root, size in (
            *((_root(index), count) for index, count in enumerate(distribution)),
            (_RETAINED, retained),
        ):
            connector.execute(
                "INSERT INTO catalog_source_build_descriptor "
                "(build_id, scope_key, manifest_policy_id, created_at) "
                "VALUES (%s, %s, 1, 0)",
                (root, bytes(32)),
            )
            connector.execute(
                "INSERT INTO catalog_source_build_sealed_ats "
                "(build_id, sealed_at) VALUES (%s, 0)",
                (root,),
            )
            seed_analysis_run(
                connector,
                analysis_id=root,
                build_id=root,
                policy_id=1,
                input_manifest_sha256=root * 2,
                started_at=0,
                state="COMPLETE",
                completed_at=1,
            )
            for start in range(0, size, 1000):
                connector.execute_many(
                    f"INSERT INTO {_TABLE} (analysis_id, file_sha256) VALUES (%s, %s)",
                    [
                        (root, _file(i + 1))
                        for i in range(start, min(size, start + 1000))
                    ],
                )
        connector.execute(
            "INSERT INTO operational_source_working_builds "
            "(slot, build_id, assigned_at) VALUES (1, %s, 0)",
            (_RETAINED,),
        )


def _operation(connector: SQLConnector, root_count: int) -> cleanup._CleanupOperation:
    kind = cleanup.CleanupTargetKind.ANALYSIS_RUN
    cycle = cleanup.CleanupCycle(
        cleanup._cleanup_id(kind, 23, 1),
        kind,
        23,
        cleanup._target_key(kind, 23),
        1,
        100,
        256,
        0,
    )
    return cleanup._CleanupOperation(
        VNextUnitOfWork(connector, backend=connector_backend(connector)),
        cycle,
        None,
        False,
        tuple((_root(index),) for index in range(root_count)),
    )


@contextmanager
def _native_cost(connector: SQLConnector) -> Iterator[Callable[[], int]]:
    if isinstance(connector, SQLiteConnector):
        steps = 0

        def progress() -> int:
            nonlocal steps
            steps += 100
            return 0

        connector.connection.set_progress_handler(progress, 100)
        try:
            yield lambda: steps
        finally:
            connector.connection.set_progress_handler(None, 0)
    else:

        def reads() -> int:
            values = connector.fetch_all("SHOW SESSION STATUS LIKE 'Handler_read_%'")
            return sum(int(value) for _name, value in values)

        before = reads()
        assert reads() == before
        yield lambda: reads() - before


@pytest.mark.parametrize("count", [63, 64, 65, 4096])
def test_analysis_owned_complete_phase_keeps_skewed_roots_and_cursor(
    database_factory: DatabaseFactory, count: int
) -> None:
    distribution = (0, count, 0, 17, 1)
    with closing(open_generated_database(database_factory.config())) as connector:
        _seed(connector, distribution)
        plan = replace(_PLAN, phases={_PHASE: _PLAN.phases[_PHASE]})
        cursor = b""
        removed: list[bytes] = []
        for _step in range((sum(distribution) + 255) // 256 + 2):
            with connector.transaction():
                result = cleanup._run_static_phase(
                    _operation(connector, len(distribution)), cursor, plan, _PHASE
                )
            assert len(result.row_keys) <= 256
            removed.extend(result.row_keys)
            cursor = result.next_cursor
            if not result.row_keys:
                break
        else:
            pytest.fail("analysis anchor phase exceeded its bounded batch count")
        expected = {
            cleanup._encode_static_cursor(0, (_root(root), _root(root), _file(index)))
            for root, size in enumerate(distribution)
            for index in range(1, size + 1)
        }
        assert len(removed) == len(set(removed))
        assert set(removed) == expected
        with connector.read_transaction():
            assert connector.fetch_one(f"SELECT COUNT(*) FROM {_TABLE}") == (32768,)
            assert connector.fetch_one(
                f"SELECT COUNT(*) FROM {_TABLE} WHERE analysis_id = %s", (_RETAINED,)
            ) == (32768,)


def test_analysis_range_work_rejects_the_unbounded_historical_shape(
    database_factory: DatabaseFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Independent predeclared engine budgets: one 256-key page and an empty
    # durable prefix must not scan the 32k retained/pending relation.
    with closing(open_generated_database(database_factory.config())) as connector:
        _seed(connector, (32768,))
        spec = _PLAN.phases[_PHASE][0]
        after = (_root(0), _root(0), _file(16384))
        with connector.transaction():
            operation = _operation(connector, 1)
            shard = cleanup._static_shard_parameters(_PLAN, operation.cycle)
            frozen, bindings = cleanup._frozen_root_predicate(
                _PLAN, operation.frozen_roots
            )
            budget = 150000 if connector_backend(connector) == "sqlite" else 1024

            def selected() -> list[tuple[Any, ...]]:
                return cleanup._select_static_candidates(
                    operation,
                    plan=_PLAN,
                    spec=spec,
                    after=after,
                    eligibility=None,
                    policy=(),
                    shard=shard,
                    remaining=256,
                )

            with _native_cost(connector) as cost:
                rows = selected()
                assert cost() <= budget
            assert rows == [(_root(0), _root(0), _file(i)) for i in range(16385, 16641)]
            with monkeypatch.context() as patch:
                patch.setattr(
                    cleanup, "_analysis_owned_suffix", lambda _plan, _spec: None
                )
                with _native_cost(connector) as cost:
                    assert selected() == rows
                    assert cost() > budget, (
                        "negative control did not expose a full scan"
                    )
            connector.execute(
                f"DELETE FROM {_TABLE} WHERE analysis_id = %s AND file_sha256 <= %s",
                (_root(0), _file(16384)),
            )

            def prefix_exists() -> bool:
                return cleanup._static_raw_responsibility_exists(
                    operation.work,
                    plan=_PLAN,
                    spec=spec,
                    frozen_root_predicate=frozen,
                    frozen_root_parameters=bindings,
                    shard_parameters=shard,
                    through=after,
                )

            with _native_cost(connector) as cost:
                assert not prefix_exists()
                assert cost() <= budget
            with monkeypatch.context() as patch:
                patch.setattr(
                    cleanup, "_analysis_owned_suffix", lambda _plan, _spec: None
                )
                with _native_cost(connector) as cost:
                    assert not prefix_exists()
                    # MariaDB already ranges the historical raw probe; SQLite
                    # needs the explicit positive suffix boundary as well.
                    if connector_backend(connector) == "sqlite":
                        assert cost() > budget
            connector.execute(
                f"INSERT INTO {_TABLE} (analysis_id, file_sha256) VALUES (%s, %s)",
                (_root(0), _file(1)),
            )
            assert prefix_exists(), "reappearing durable prefix must fail closed"


def test_analysis_cursor_owner_mismatch_is_rejected(
    database_factory: DatabaseFactory,
) -> None:
    with closing(open_generated_database(database_factory.config())) as connector:
        _seed(connector, (1,))
        with (
            connector.transaction(),
            pytest.raises(cleanup.CleanupCorruptionError, match="owner prefix"),
        ):
            operation = _operation(connector, 1)
            cleanup._select_static_candidates(
                operation,
                plan=_PLAN,
                spec=_PLAN.phases[_PHASE][0],
                after=(_root(0), _root(1), _file(1)),
                eligibility=None,
                policy=(),
                shard=cleanup._static_shard_parameters(_PLAN, operation.cycle),
                remaining=256,
            )


def test_analysis_raw_prefix_covers_earlier_roots_and_retention_blocks_completion(
    database_factory: DatabaseFactory,
) -> None:
    with closing(open_generated_database(database_factory.config())) as connector:
        _seed(connector, (1, 0, 65))
        plan = replace(_PLAN, phases={_PHASE: _PLAN.phases[_PHASE]})
        with connector.transaction():
            operation = _operation(connector, 3)
            frozen, bindings = cleanup._frozen_root_predicate(
                plan, operation.frozen_roots
            )
            assert cleanup._static_raw_responsibility_exists(
                operation.work,
                plan=plan,
                spec=plan.phases[_PHASE][0],
                frozen_root_predicate=frozen,
                frozen_root_parameters=bindings,
                shard_parameters=cleanup._static_shard_parameters(
                    plan, operation.cycle
                ),
                through=(_root(2), _root(2), _file(0)),
            ), "the earlier frozen root must remain inside the durable prefix proof"
            connector.execute(
                "UPDATE operational_source_working_builds SET build_id = %s WHERE slot = 1",
                (_root(2),),
            )
            result = cleanup._run_static_phase(operation, b"", plan, _PHASE)
            assert len(result.row_keys) == 1
        with (
            connector.transaction(),
            pytest.raises(
                cleanup.CleanupRetentionBlockedError, match="still owns rows"
            ),
        ):
            cleanup._run_static_phase(
                _operation(connector, 3), result.next_cursor, plan, _PHASE
            )
        with connector.read_transaction():
            assert connector.fetch_one(
                f"SELECT COUNT(*) FROM {_TABLE} WHERE analysis_id = %s", (_root(2),)
            ) == (65,)
