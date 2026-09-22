"""Collection retention, compound staging retirement and bounded cleanup costs."""

from __future__ import annotations

from contextlib import closing
from typing import Any

import pytest
from test_vnext_source_batches import _source_batch_clock
from test_vnext_source_collection import _finish_source
from vnext_fault_harness import (
    InjectedFault,
    backend_of,
    open_connector,
)
from vnext_pipeline import (
    MemorySource,
    claim_session,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
)

from h2hdb import CoreConfig, VNextIngestFacade
from h2hdb.schema_epoch import SchemaEpochValidationError
from h2hdb.source_collection_refinement import (
    check_source_collection_durable_observations_v1,
)
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


def _read_one(
    connector: SQLConnector, query: str, data: tuple[Any, ...] = ()
) -> tuple[Any, ...]:
    with connector.read_transaction():
        return connector.fetch_one(query, data)


def _read_all(
    connector: SQLConnector, query: str, data: tuple[Any, ...] = ()
) -> list[tuple[Any, ...]]:
    with connector.read_transaction():
        return connector.fetch_all(query, data)


def _one_gallery(config: CoreConfig, *, stop_at_seal: bool = False) -> None:
    initialize_database(config)
    source = MemorySource((gallery(1001),))
    with (
        _source_batch_clock(config) as clock,
        VNextIngestFacade(config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        if not stop_at_seal:
            _finish_source(facade, session, policy, source)
        else:
            with facade.prepare_source(source, policy=policy) as prepared:
                for _ in range(500):
                    issued = facade.issue_source_step(session, policy, prepared)
                    local = facade.prepare_source_step(prepared, issued)
                    facade.commit_source_step(session, local)
                    with closing(open_connector(config)) as reader:
                        if reader.fetch_one(
                            "SELECT 1 FROM catalog_source_collection_observations"
                        ):
                            break
                else:
                    raise AssertionError("fixture never sealed its collection member")
        facade.complete_ingest(session)


def _exclusive(connector: SQLConnector, config: CoreConfig, now: int) -> GateLease:
    with connector.transaction():
        return MaintenanceGateRepository.claim_exclusive(
            VNextUnitOfWork(connector, backend=backend_of(config)),
            now=now,
            lease_duration=10**12,
        )


def _begin(
    connector: SQLConnector,
    config: CoreConfig,
    gate: GateLease,
    kind: CleanupTargetKind,
    shard: int,
    *,
    limit: int = 1,
    now: int,
) -> CleanupCycle:
    with connector.transaction():
        return VNextCleanupRepository.begin_cycle(
            VNextUnitOfWork(connector, backend=backend_of(config)),
            gate_lease=gate,
            target_kind=kind,
            shard_no=shard,
            cycle_cutoff_at=now,
            max_rows_per_transaction=limit,
            now=now,
        )


def _advance(
    connector: SQLConnector,
    config: CoreConfig,
    gate: GateLease,
    cycle: CleanupCycle,
    generation: int,
    *,
    now: int,
) -> Any:
    with connector.transaction():
        return VNextCleanupRepository.advance(
            VNextUnitOfWork(connector, backend=backend_of(config)),
            gate_lease=gate,
            cycle=cycle,
            command=CleanupBatchCommand(now.to_bytes(32, "big"), generation),
            now=now,
        )


def test_open_collection_retains_sealed_observation_after_lease_and_staging_retirement(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _one_gallery(db_config, stop_at_seal=True)
    with (
        _source_batch_clock(db_config) as clock,
        closing(open_connector(db_config)) as connector,
    ):
        now = clock() + 10**10
        gate = _exclusive(connector, db_config, now)
        staging, collection, gallery_id, observation_id = _read_one(
            connector,
            "SELECT s.staging_id, b.collection_id, s.gallery_id, s.observation_id "
            "FROM operational_gallery_observation_stagings s JOIN operational_gallery_staging_collections b ON b.staging_id = s.staging_id",
        )
        cycle = _begin(
            connector,
            db_config,
            gate,
            CleanupTargetKind.GALLERY_OBSERVATION_STAGING,
            staging[0],
            now=now + 1,
        )
        generation = 1
        fault_seen = False
        for step in range(128):
            phase = _read_one(
                connector,
                "SELECT phase FROM operational_cleanup_checkpoints WHERE cleanup_id = %s AND state = 'OPEN'",
                (cycle.cleanup_id,),
            )[0]
            if phase == "GOS_ROOT" and not fault_seen:
                execute = connector.execute_affected

                def reject_header(sql: str, data: tuple[Any, ...] = ()) -> int:
                    if sql.startswith(
                        "DELETE FROM operational_gallery_observation_stagings "
                    ):
                        raise InjectedFault("between owner and staging root")
                    return execute(sql, data)

                with monkeypatch.context() as patched, pytest.raises(InjectedFault):
                    patched.setattr(connector, "execute_affected", reject_header)
                    _advance(
                        connector,
                        db_config,
                        gate,
                        cycle,
                        generation,
                        now=now + step + 2,
                    )
                assert _read_one(
                    connector,
                    "SELECT 1 FROM operational_gallery_staging_collections WHERE staging_id = %s",
                    (staging,),
                ) == (1,)
                assert _read_one(
                    connector,
                    "SELECT 1 FROM operational_gallery_observation_stagings WHERE staging_id = %s",
                    (staging,),
                ) == (1,)
                fault_seen = True
            result = _advance(
                connector, db_config, gate, cycle, generation, now=now + step + 2
            )
            assert result.row_count <= 1
            # Even the one-row limit cannot expose a header without its owner.
            assert _read_one(
                connector,
                "SELECT COUNT(*) FROM operational_gallery_observation_stagings",
            ) == _read_one(
                connector,
                "SELECT COUNT(*) FROM operational_gallery_staging_collections",
            )
            assert full_check(db_config).state == "READY"
            if result.cycle_complete:
                break
            assert result.generation is not None
            generation = result.generation
        else:
            raise AssertionError("staging cleanup did not complete")
        assert fault_seen
        for kind, shard in (
            (CleanupTargetKind.GALLERY_OBSERVATION, gallery_id % 256),
            (CleanupTargetKind.SOURCE_COLLECTION, collection[0]),
        ):
            cycle = _begin(connector, db_config, gate, kind, shard, now=now + 100)
            generation = 1
            for step in range(40):
                result = _advance(
                    connector, db_config, gate, cycle, generation, now=now + 101 + step
                )
                assert result.row_count == 0
                if result.cycle_complete:
                    break
                assert result.generation is not None
                generation = result.generation
            else:
                raise AssertionError("empty protected cleanup did not finish")
        assert _read_one(
            connector,
            "SELECT observation_id FROM catalog_source_collection_observations WHERE collection_id = %s AND gallery_id = %s",
            (collection, gallery_id),
        ) == (observation_id,)
        assert _read_one(
            connector,
            "SELECT 1 FROM catalog_gallery_observations WHERE gallery_id = %s AND observation_id = %s",
            (gallery_id, observation_id),
        ) == (1,)


def test_consumed_collection_cleanup_checks_each_durable_phase_and_preserves_build(
    db_config: CoreConfig,
) -> None:
    _one_gallery(db_config)
    with (
        _source_batch_clock(db_config) as clock,
        closing(open_connector(db_config)) as connector,
    ):
        now = clock() + 10**10
        gate = _exclusive(connector, db_config, now)
        collection = _read_one(
            connector, "SELECT collection_id FROM catalog_source_collections"
        )[0]
        expected_members = _read_all(
            connector,
            "SELECT gallery_id, observation_id FROM catalog_source_build_galleries",
        )
        cycle = _begin(
            connector,
            db_config,
            gate,
            CleanupTargetKind.SOURCE_COLLECTION,
            collection[0],
            now=now + 1,
        )
        generation = 1
        rejected_gap = False
        for step in range(128):
            phase = _read_one(
                connector,
                "SELECT phase FROM operational_cleanup_checkpoints WHERE cleanup_id = %s AND state = 'OPEN'",
                (cycle.cleanup_id,),
            )[0]
            if phase == "SC_METADATA" and not rejected_gap:
                with pytest.raises(SchemaEpochValidationError), connector.transaction():
                    connector.execute(
                        "DELETE FROM catalog_source_collection_qualification_policies WHERE collection_id = %s",
                        (collection,),
                    )
                    check_source_collection_durable_observations_v1(connector)
                rejected_gap = True
            result = _advance(
                connector, db_config, gate, cycle, generation, now=now + step + 2
            )
            assert result.row_count <= 1
            assert full_check(db_config).state == "READY"
            assert (
                _read_all(
                    connector,
                    "SELECT gallery_id, observation_id FROM catalog_source_build_galleries",
                )
                == expected_members
            )
            if result.cycle_complete:
                break
            assert result.generation is not None
            generation = result.generation
        else:
            raise AssertionError("collection cleanup did not complete")
        assert (
            _read_all(connector, "SELECT collection_id FROM catalog_source_collections")
            == []
        )
        assert (
            _read_all(
                connector,
                "SELECT gallery_id, observation_id FROM catalog_gallery_observations",
            )
            == expected_members
        )
        assert rejected_gap


def _clone_abandoned_collections(
    connector: SQLConnector,
    base: bytes,
    *,
    count: int,
    repetition: int,
    shard: int,
) -> tuple[bytes, ...]:
    """Scale only collection rows while reusing one real sealed source observation."""
    keys = tuple(
        bytes((shard, repetition)) + index.to_bytes(14, "big") for index in range(count)
    )
    with connector.transaction():
        for key in keys:
            connector.execute(
                "INSERT INTO catalog_source_collections (collection_id, scope_key) SELECT %s, scope_key FROM catalog_source_collections WHERE collection_id = %s",
                (key, base),
            )
            for table, column in (
                ("catalog_source_collection_manifest_policies", "manifest_policy_id"),
                (
                    "catalog_source_collection_qualification_policies",
                    "qualification_policy_sha256",
                ),
                ("catalog_source_collection_created_ats", "created_at"),
            ):
                connector.execute(
                    f"INSERT INTO {table} (collection_id, {column}) SELECT %s, {column} FROM {table} WHERE collection_id = %s",
                    (key, base),
                )
            connector.execute(
                "INSERT INTO operational_source_collection_claims (collection_id, ingest_generation, claim_generation, updated_at) "
                "SELECT %s, ingest_generation, claim_generation, updated_at FROM operational_source_collection_claims WHERE collection_id = %s",
                (key, base),
            )
            connector.execute(
                "INSERT INTO operational_source_collection_states (collection_id, state) VALUES (%s, 'ABANDONED')",
                (key,),
            )
            connector.execute(
                "INSERT INTO catalog_source_collection_observations (collection_id, gallery_id, observation_id) "
                "SELECT %s, gallery_id, observation_id FROM catalog_source_collection_observations WHERE collection_id = %s",
                (key, base),
            )
    return keys


@pytest.mark.deep
@pytest.mark.parametrize("count", [127, 128, 129])
def test_collection_cleanup_capacity_and_repeated_cycles(
    db_config: CoreConfig,
    count: int,
) -> None:
    # Units: selected/deleted relation rows and frozen roots, not wall time.
    # Input dimensions: collection roots (127/128/129), one sealed member/root,
    # configured transaction capacity 128, and two complete collection cycles.
    # Counterexamples: root pagination skips the 129th member, repeat-cycle
    # cursor reuse skips the next generation, or cleanup releases another owner.
    _one_gallery(db_config)
    with (
        _source_batch_clock(db_config) as clock,
        closing(open_connector(db_config)) as connector,
    ):
        now = clock() + 10**10
        gate = _exclusive(connector, db_config, now)
        base = _read_one(
            connector, "SELECT collection_id FROM catalog_source_collections"
        )[0]
        shard = base[0] ^ 255
        for repetition in (1, 2):
            keys = _clone_abandoned_collections(
                connector, base, count=count, repetition=repetition, shard=shard
            )
            assert len(keys) == count
            assert full_check(db_config).state == "READY"
            removed = 0
            for frozen_cycle in range((count + 127) // 128):
                cycle = _begin(
                    connector,
                    db_config,
                    gate,
                    CleanupTargetKind.SOURCE_COLLECTION,
                    shard,
                    limit=128,
                    now=now + 1000 * repetition + 100 * frozen_cycle,
                )
                generation = 1
                for step in range(40):
                    result = _advance(
                        connector,
                        db_config,
                        gate,
                        cycle,
                        generation,
                        now=now + 1000 * repetition + 100 * frozen_cycle + step + 1,
                    )
                    assert 0 <= result.row_count <= 128
                    removed += result.row_count
                    assert full_check(db_config).state == "READY"
                    if result.cycle_complete:
                        break
                    assert result.generation is not None
                    generation = result.generation
                else:
                    raise AssertionError("bounded collection pages did not finish")
            # root + three policy/time children + state + claim + retained member.
            assert removed == 7 * count
            assert _read_all(
                connector, "SELECT collection_id FROM catalog_source_collections"
            ) == [(base,)]
            assert _read_one(
                connector, "SELECT COUNT(*) FROM catalog_gallery_observations"
            ) == (1,)
