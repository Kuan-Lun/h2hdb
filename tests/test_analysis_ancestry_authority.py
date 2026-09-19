"""Both ancestry read strategies reject corruption of every inherited authority."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import closing, contextmanager
from typing import cast

import pytest
from analysis_ancestry_baseline import historical_validate_ancestry_suffixes
from analysis_ancestry_experiment import Sample
from test_vnext_analysis_repository import _seed_build, _seed_root
from vnext_analysis_fixtures import seed_analysis_component, seed_analysis_run
from vnext_fault_harness import open_connector
from vnext_pipeline import initialize_database

import h2hdb.vnext_analysis_repository as analysis
from h2hdb import CoreConfig
from h2hdb.sql_connector import SQLConnector
from h2hdb.sql_performance import instrument_connector, measure_sql
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_transaction import VNextUnitOfWork

pytestmark = pytest.mark.performance_acceptance


class _Rollback(Exception):
    pass


@contextmanager
def _rollback(connector: SQLConnector) -> Iterator[None]:
    try:
        with connector.transaction():
            yield
            raise _Rollback
    except _Rollback:
        pass


def _seed(connector: SQLConnector, count: int) -> tuple[bytes, ...]:
    """Only inherited scalar authority; the separate public test seals real data."""
    fixture = cast(SQLiteConnector, connector)
    ancestry = tuple(number.to_bytes(16, "big") for number in range(count, 0, -1))
    with connector.transaction():
        scope = _seed_root(fixture)
        for offset in reversed(range(count)):
            ancestor = ancestry[offset]
            build = (100 + offset).to_bytes(16, "big")
            _seed_build(
                fixture,
                build_id=build,
                scope=scope,
                manifest_byte=offset + 1,
                gallery_count=0,
            )
            seed_analysis_run(
                connector,
                analysis_id=ancestor,
                build_id=build,
                policy_id=1,
                input_manifest_sha256=bytes((offset,)) * 32,
                started_at=30,
                state="COMPLETE",
                completed_at=50,
            )
            for depth, suffix in enumerate(ancestry[offset:]):
                connector.execute(
                    "INSERT INTO catalog_analysis_state_ancestry "
                    "(analysis_id, ancestor_depth, ancestor_analysis_id) VALUES (%s, %s, %s)",
                    (ancestor, depth, suffix),
                )
            if offset + 1 < count:
                connector.execute(
                    "INSERT INTO catalog_analysis_baselines "
                    "(analysis_id, base_analysis_id) VALUES (%s, %s)",
                    (ancestor, ancestry[offset + 1]),
                )
            for component in analysis.ANALYSIS_COMPONENTS:
                seed_analysis_component(
                    connector,
                    analysis_id=ancestor,
                    state_component=component,
                    row_count=0,
                    sealed_at=40,
                    terminal_receipt=True,
                )
    return ancestry


def _validate(
    connector: SQLConnector,
    backend: str,
    ancestry: tuple[bytes, ...],
    *,
    historical: bool = False,
) -> int:
    recorder = Sample("historical" if historical else "production")
    function = (
        historical_validate_ancestry_suffixes
        if historical
        else analysis._validate_ancestry_suffixes
    )
    with measure_sql(recorder):
        function(
            VNextUnitOfWork(instrument_connector(connector), backend=backend),
            ancestry=ancestry,
            anchor_analysis_id=ancestry[-1],
            policy_id=1,
        )
    return recorder.sql_calls


@pytest.mark.parametrize(
    "count",
    (
        1,
        pytest.param(2, marks=pytest.mark.deep),
        pytest.param(8, marks=pytest.mark.deep),
        pytest.param(16, marks=pytest.mark.deep),
        pytest.param(17, marks=pytest.mark.deep),
    ),
)
def test_exact_ancestry_authority_read_cost_has_no_per_ancestor_roundtrips(
    db_config: CoreConfig, count: int
) -> None:
    initialize_database(db_config)
    with closing(open_connector(db_config)) as connector:
        ancestry = _seed(connector, count)
        with connector.read_transaction():
            for _ in range(3):
                assert _validate(connector, db_config.database.sql_type, ancestry) == 6
                assert (
                    _validate(
                        connector,
                        db_config.database.sql_type,
                        ancestry,
                        historical=True,
                    )
                    == 26 * count
                )


@pytest.mark.deep
def test_all_inherited_authorities_remain_fail_closed(db_config: CoreConfig) -> None:
    initialize_database(db_config)
    with closing(open_connector(db_config)) as connector:
        ancestry = _seed(connector, 17)
        connector.execute(
            "PRAGMA foreign_keys = OFF"
            if db_config.database.sql_type == "sqlite"
            else "SET FOREIGN_KEY_CHECKS = 0"
        )
        target = ancestry[8]
        component = b"file_hash_decision"
        stage = b"validate_file_hash_decision"
        faults: tuple[tuple[str, tuple[object, ...]], ...] = (
            (
                "DELETE FROM catalog_analysis_run_descriptor WHERE analysis_id = %s",
                (target,),
            ),
            (
                "DELETE FROM catalog_analysis_run_states WHERE analysis_id = %s",
                (target,),
            ),
            (
                "DELETE FROM catalog_analysis_run_completed_ats WHERE analysis_id = %s",
                (target,),
            ),
            (
                "UPDATE catalog_analysis_run_states SET state = 'OPEN' WHERE analysis_id = %s",
                (target,),
            ),
            (
                "UPDATE catalog_analysis_run_descriptor SET policy_id = 2 WHERE analysis_id = %s",
                (target,),
            ),
            (
                "UPDATE catalog_analysis_run_descriptor SET started_at = 1 WHERE analysis_id = %s",
                (target,),
            ),
            (
                "DELETE FROM catalog_analysis_state_ancestry WHERE analysis_id = %s AND ancestor_depth = 1",
                (target,),
            ),
            (
                "UPDATE catalog_analysis_state_ancestry SET ancestor_analysis_id = %s WHERE analysis_id = %s AND ancestor_depth = 1",
                (ancestry[0], target),
            ),
            (
                "UPDATE catalog_analysis_baselines SET base_analysis_id = %s WHERE analysis_id = %s",
                (ancestry[-1], target),
            ),
            (
                "DELETE FROM catalog_analysis_state_component_seals WHERE analysis_id = %s AND state_component = %s",
                (target, component),
            ),
            (
                "UPDATE catalog_analysis_state_component_seals SET row_count = 1 WHERE analysis_id = %s AND state_component = %s",
                (target, component),
            ),
            (
                "UPDATE catalog_analysis_state_component_seals SET sealed_at = 41 WHERE analysis_id = %s AND state_component = %s",
                (target, component),
            ),
            (
                "DELETE FROM catalog_analysis_batch_receipt_stored WHERE analysis_id = %s AND stage = %s",
                (target, stage),
            ),
            (
                "INSERT INTO catalog_analysis_batch_receipt_stored "
                "(analysis_id, stage, start_generation, batch_key, start_cursor, "
                "start_processed_count, page_limit, next_cursor, row_count, committed_at) "
                "SELECT analysis_id, stage, start_generation + 1000, %s, start_cursor, "
                "start_processed_count, page_limit, next_cursor, row_count, committed_at "
                "FROM catalog_analysis_batch_receipt_stored WHERE analysis_id = %s AND stage = %s",
                (b"duplicate-terminal", target, stage),
            ),
            (
                "UPDATE catalog_analysis_batch_receipt_stored SET next_cursor = %s WHERE analysis_id = %s AND stage = %s",
                (b"malformed", target, stage),
            ),
            (
                "UPDATE catalog_analysis_checkpoints SET state = 'OPEN' WHERE analysis_id = %s AND stage = %s",
                (target, stage),
            ),
            (
                "DELETE FROM catalog_analysis_checkpoints WHERE analysis_id = %s AND stage = %s",
                (target, stage),
            ),
        )
        for sql, parameters in faults:
            with _rollback(connector):
                connector.execute(sql, parameters)
                for historical in (False, True):
                    with pytest.raises(
                        (
                            analysis.AnalysisCorruptionError,
                            analysis.AnalysisNotReadyError,
                        )
                    ):
                        _validate(
                            connector,
                            db_config.database.sql_type,
                            ancestry,
                            historical=historical,
                        )
            with connector.read_transaction():
                assert _validate(connector, db_config.database.sql_type, ancestry) == 6
