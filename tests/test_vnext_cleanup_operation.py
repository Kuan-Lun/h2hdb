"""Real transaction evidence for operation-local cleanup validation reuse."""

from __future__ import annotations

from contextlib import closing
from typing import Any
from unittest.mock import patch

import pytest
from vnext_analysis_fixtures import seed_analysis_run
from vnext_canonical_value_fixtures import seed_canonical_value
from vnext_catalog_registry_fixtures import (
    seed_analysis_policy,
    seed_manifest_policy,
    seed_source_scope,
)
from vnext_fault_harness import backend_of, open_connector
from vnext_manifest_fixtures import seed_sealed_source_build
from vnext_pipeline import initialize_database

from h2hdb import CoreConfig
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_cleanup_repository import (
    CleanupBatchResult,
    CleanupCorruptionError,
    CleanupCycle,
    CleanupRetentionBlockedError,
    CleanupTargetKind,
    VNextCleanupRepository,
)
from h2hdb.vnext_maintenance_gate_repository import GateLease, MaintenanceGateRepository
from h2hdb.vnext_transaction import VNextUnitOfWork

pytestmark = pytest.mark.cleanup_acceptance

_ANALYSIS_ID = b"a" * 16
_BUILD_ID = b"b" * 16


class _AbortTransaction(RuntimeError):
    pass


def _seed(
    config: CoreConfig, connector: SQLConnector
) -> tuple[GateLease, CleanupCycle]:
    backend = backend_of(config)
    with connector.transaction():
        seed_manifest_policy(connector)
        seed_analysis_policy(connector)
        seed_canonical_value(
            connector,
            value_sha256=b"s" * 32,
            digest_domain=b"source_root_v1",
            page_sha256=b"p" * 32,
            page_bytes=b"x",
            subtree_item_count=1,
            allocated_at=0,
        )
        scope = seed_source_scope(connector, source_root_sha256=b"s" * 32)
        seed_sealed_source_build(
            connector,
            build_id=_BUILD_ID,
            scope_key=scope.scope_key,
            manifest_sha256=b"m" * 32,
            gallery_count=0,
            file_count=0,
            byte_count=0,
            created_at=0,
            sealed_at=0,
        )
        seed_analysis_run(
            connector,
            analysis_id=_ANALYSIS_ID,
            build_id=_BUILD_ID,
            policy_id=1,
            input_manifest_sha256=b"m" * 32,
            started_at=0,
            state="ABANDONED",
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
            target_kind=CleanupTargetKind.ANALYSIS_RUN,
            shard_no=_ANALYSIS_ID[0],
            cycle_cutoff_at=100,
            max_rows_per_transaction=2,
            now=2,
        )
    with connector.read_transaction():
        assert connector.fetch_one(
            "SELECT frozen_root_count FROM operational_cleanup_jobs WHERE cleanup_id = %s",
            (cycle.cleanup_id,),
        ) == (1,)
    return gate, cycle


def _advance(
    config: CoreConfig,
    connector: SQLConnector,
    gate: GateLease,
    cycle: CleanupCycle,
    *,
    now: int,
) -> tuple[tuple[CleanupBatchResult, ...], tuple[str, ...]]:
    with (
        patch.object(connector, "fetch_one", wraps=connector.fetch_one) as one,
        patch.object(connector, "fetch_all", wraps=connector.fetch_all) as many,
    ):
        results = VNextCleanupRepository.advance_current_only_cycle(
            VNextUnitOfWork(connector, backend=backend_of(config)),
            gate_lease=gate,
            cycle=cycle,
            now=now,
        )
    queries = tuple(
        " ".join(call.args[0].split())
        for reader in (one, many)
        for call in reader.call_args_list
    )
    return results, queries


def _assert_one_operation_reads(queries: tuple[str, ...]) -> None:
    # Count the actual DB boundary, not calls to the implementation's cache.
    assert (
        sum(
            query.startswith(
                "SELECT frozen_root_key FROM operational_cleanup_cycle_roots"
            )
            for query in queries
        )
        == 1
    )
    assert (
        sum(
            query.startswith(
                "SELECT phase, phase_order FROM operational_cleanup_phases"
            )
            for query in queries
        )
        == 1
    )
    assert not any(
        query.startswith(
            (
                "SELECT frozen_root_count, frozen_root_set_sha256",
                "SELECT target_kind, phase_order FROM operational_cleanup_phases",
            )
        )
        for query in queries
    )
    # A representative early empty relation is proved once even when the same
    # operation continues across all the subsequent empty phases.
    assert (
        sum(
            "SELECT 1 FROM catalog_a_file_decision_shadow_seals AS c" in query
            for query in queries
        )
        == 1
    )


def _snapshot(connector: SQLConnector) -> tuple[list[tuple[Any, ...]], ...]:
    with connector.read_transaction():
        return tuple(
            connector.fetch_all(query)
            for query in (
                "SELECT * FROM operational_cleanup_jobs ORDER BY cleanup_id",
                "SELECT * FROM operational_cleanup_checkpoints ORDER BY cleanup_id, phase",
                "SELECT * FROM operational_cleanup_cycle_roots "
                "ORDER BY cleanup_id, frozen_root_key",
                "SELECT * FROM catalog_analysis_run_descriptor ORDER BY analysis_id",
                "SELECT * FROM catalog_analysis_run_states ORDER BY analysis_id",
                "SELECT * FROM catalog_analysis_run_completed_ats ORDER BY analysis_id",
            )
        )


def test_empty_phase_chain_reads_immutable_authority_once_per_operation(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    with closing(open_connector(db_config)) as connector:
        gate, cycle = _seed(db_config, connector)
        with connector.transaction():
            results, queries = _advance(db_config, connector, gate, cycle, now=3)
        assert len(results) > 2
        assert all(result.row_count == 0 for result in results[:-1])
        assert results[-1].phase == "AR_STATE" and results[-1].row_count == 1
        _assert_one_operation_reads(queries)
        # The same connector's next transaction must rebuild every proof.
        with connector.transaction():
            resumed, next_queries = _advance(db_config, connector, gate, cycle, now=4)
        assert resumed[-1].phase == "AR_ROOT" and resumed[-1].row_count == 1
        _assert_one_operation_reads(next_queries)


@pytest.mark.parametrize("corruption", ("seal", "roots", "phase"))
def test_next_transaction_revalidates_loaded_cleanup_authority(
    db_config: CoreConfig, corruption: str
) -> None:
    initialize_database(db_config)
    with closing(open_connector(db_config)) as connector:
        gate, cycle = _seed(db_config, connector)
        with connector.transaction():
            VNextCleanupRepository.resume_cycle(
                VNextUnitOfWork(connector, backend=backend_of(db_config)),
                gate_lease=gate,
                cycle=cycle,
                now=3,
            )
        with connector.transaction():
            match corruption:
                case "seal":
                    connector.execute(
                        "UPDATE operational_cleanup_jobs SET frozen_root_set_sha256 = %s "
                        "WHERE cleanup_id = %s",
                        (b"z" * 32, cycle.cleanup_id),
                    )
                case "roots":
                    connector.execute(
                        "DELETE FROM operational_cleanup_cycle_roots WHERE cleanup_id = %s",
                        (cycle.cleanup_id,),
                    )
                case _:
                    connector.execute(
                        "DELETE FROM operational_cleanup_phases WHERE phase = %s",
                        ("AR_ROOT",),
                    )
        before = _snapshot(connector)
        with pytest.raises(CleanupCorruptionError), connector.transaction():
            _advance(db_config, connector, gate, cycle, now=4)
        assert _snapshot(connector) == before


def test_rollback_discards_empty_phase_and_frozen_root_proofs(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    with closing(open_connector(db_config)) as connector:
        gate, cycle = _seed(db_config, connector)
        before = _snapshot(connector)
        with pytest.raises(_AbortTransaction), connector.transaction():
            results, queries = _advance(db_config, connector, gate, cycle, now=3)
            assert results[-1].row_count == 1
            _assert_one_operation_reads(queries)
            raise _AbortTransaction
        assert _snapshot(connector) == before
        with connector.transaction():
            retried, queries = _advance(db_config, connector, gate, cycle, now=4)
        assert retried[-1].phase == "AR_STATE" and retried[-1].row_count == 1
        _assert_one_operation_reads(queries)


def test_earlier_empty_phase_reappearance_is_rejected_in_next_transaction(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    with closing(open_connector(db_config)) as connector:
        gate, cycle = _seed(db_config, connector)
        with connector.transaction():
            results, _queries = _advance(db_config, connector, gate, cycle, now=3)
        assert results[-1].phase == "AR_STATE" and results[-1].row_count == 1
        with connector.transaction():
            connector.execute(
                "INSERT INTO catalog_analysis_run_completed_ats "
                "(analysis_id, completed_at) VALUES (%s, %s)",
                (_ANALYSIS_ID, 0),
            )
        before = _snapshot(connector)
        with (
            pytest.raises(CleanupRetentionBlockedError, match="still owns rows"),
            connector.transaction(),
        ):
            _advance(db_config, connector, gate, cycle, now=4)
        assert _snapshot(connector) == before
