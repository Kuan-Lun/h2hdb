"""Collection cleanup shard inputs remain reproducible across fault replays."""

from __future__ import annotations

import secrets
from collections.abc import Callable
from typing import cast
from uuid import RFC_4122, UUID

import pytest
import test_vnext_pipeline_fault_matrix as matrix
from vnext_collection_identities import (
    PREFIX_NAMESPACE,
    PREFIX_SHARD,
    RECOVERY_NAMESPACE,
    RECOVERY_SHARD,
    TARGET_NAMESPACE,
    TARGET_SHARD,
    collection_identities,
)
from vnext_fault_harness import (
    MAINTENANCE_GATE_TABLES,
    FaultInjector,
    assert_exact_rollback,
    fault_injection,
    fault_points,
    run_fault_point,
)
from vnext_pipeline import Clock, full_check, takeover_clock
from vnext_test_database import DatabaseFactory

import h2hdb.vnext_cleanup_repository as cleanup
import h2hdb.vnext_source_collection_repository as collections
from h2hdb.vnext_transaction import VNextUnitOfWork


def _collection_factory() -> Callable[[], UUID]:
    return cast(Callable[[], UUID], getattr(collections, "uuid4"))


def test_collection_sequences_are_distinct_repeatable_and_scoped() -> None:
    original_uuid = _collection_factory()
    original_entropy = secrets.token_bytes
    domains = (
        (PREFIX_NAMESPACE, PREFIX_SHARD),
        (TARGET_NAMESPACE, TARGET_SHARD),
        (RECOVERY_NAMESPACE, RECOVERY_SHARD),
    )
    all_values: set[UUID] = set()
    for namespace, shard in domains:
        repetitions = []
        for _ in range(2):
            with collection_identities(namespace, shard=shard) as sequence:
                assert _collection_factory() is sequence
                assert secrets.token_bytes is original_entropy
                values = tuple(_collection_factory()() for _ in range(128))
            assert _collection_factory() is original_uuid
            assert len(set(values)) == len(values)
            assert all(
                value.version == 4
                and value.variant == RFC_4122
                and value.bytes[0] == shard
                for value in values
            )
            repetitions.append(values)
        assert repetitions[0] == repetitions[1]
        assert all_values.isdisjoint(repetitions[0])
        all_values.update(repetitions[0])
    with pytest.raises(RuntimeError, match="scope failure"):
        with collection_identities(TARGET_NAMESPACE, shard=TARGET_SHARD):
            raise RuntimeError("scope failure")
    assert _collection_factory() is original_uuid
    assert secrets.token_bytes is original_entropy


@pytest.mark.deep
@pytest.mark.parametrize("collides", (False, True))
def test_exact_fault_target_retains_collection_shard_branch(
    database_factory: DatabaseFactory,
    monkeypatch: pytest.MonkeyPatch,
    collides: bool,
) -> None:
    baseline = matrix._Baseline(database_factory, matrix.SCENARIOS["cleanup"])
    target_shard = PREFIX_SHARD if collides else TARGET_SHARD
    other_shard = TARGET_SHARD if collides else PREFIX_SHARD
    source_cycle_transactions: list[int] = []
    original_begin = cleanup._begin_cycle_under_exclusive
    dry = FaultInjector()

    def observe_begin(
        work: VNextUnitOfWork,
        *,
        kind: cleanup.CleanupTargetKind,
        shard: int,
        cutoff: int,
        max_rows: int,
        max_age: int,
        now: int,
    ) -> cleanup.CleanupCycle:
        if kind is cleanup.CleanupTargetKind.SOURCE_COLLECTION:
            source_cycle_transactions.append(len(dry.transactions))
        return original_begin(
            work,
            kind=kind,
            shard=shard,
            cutoff=cutoff,
            max_rows=max_rows,
            max_age=max_age,
            now=now,
        )

    config, source, library = baseline.fresh_copy()
    with collection_identities(TARGET_NAMESPACE, shard=target_shard):
        with monkeypatch.context() as patch:
            patch.setattr(cleanup, "_begin_cycle_under_exclusive", observe_begin)
            with fault_injection(monkeypatch, dry):
                matrix._turn(config, source, library, clock=Clock())
    assert len(source_cycle_transactions) == 1
    transaction = dry.transactions[source_cycle_transactions[0]]
    expected_first = "DELETE" if collides else "INSERT"
    assert transaction.shape[0].startswith(
        f"{expected_first} "
        + ("FROM" if collides else "INTO")
        + " operational_cleanup_jobs"
    )
    reference = matrix._reference(config, source, library)
    assert full_check(config).state == "READY"
    baseline.discard(config)
    # Select a real later statement so both opposite-shard workflows reach the
    # same numeric ordinal. The complete prior trace must distinguish them.
    point = next(
        point
        for point in fault_points(dry)
        if point.kind == "before_mutation"
        and point.statement_index > 0
        and point.recorded_transactions[point.transaction_index].first_mutation
        > transaction.first_mutation + 10
    )
    config, source, library = baseline.fresh_copy()
    with collection_identities(TARGET_NAMESPACE, shard=other_shard):
        with pytest.raises(
            AssertionError, match="fault target prior transaction trace drift"
        ):
            run_fault_point(
                monkeypatch,
                config=config,
                point=point,
                workflow=lambda: matrix._turn(config, source, library, clock=Clock()),
            )
    with collection_identities(RECOVERY_NAMESPACE, shard=RECOVERY_SHARD):
        matrix._turn(config, source, library, clock=takeover_clock())
    assert full_check(config).state == "READY"
    assert matrix._reference(config, source, library) == reference
    baseline.discard(config)
    config, source, library = baseline.fresh_copy()
    with collection_identities(TARGET_NAMESPACE, shard=target_shard):
        injector, before = run_fault_point(
            monkeypatch,
            config=config,
            point=point,
            workflow=lambda: matrix._turn(config, source, library, clock=Clock()),
        )
    assert injector.fired == "before_mutation"
    assert_exact_rollback(config, before, compensation_tables=MAINTENANCE_GATE_TABLES)
    with collection_identities(RECOVERY_NAMESPACE, shard=RECOVERY_SHARD):
        matrix._turn(config, source, library, clock=takeover_clock())
    assert full_check(config).state == "READY"
    assert matrix._reference(config, source, library) == reference
    baseline.discard(config)
    baseline.assert_preserved()
