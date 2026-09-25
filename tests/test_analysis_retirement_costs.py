"""Real bounded analysis-fact retirement; partial graphs are not READY audits."""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass, field, replace
from typing import Any, Literal

import pytest
from vnext_analysis_fixtures import seed_analysis_run
from vnext_fault_harness import backend_of, open_connector
from vnext_pipeline import initialize_database
from vnext_publication_cleanup_fixtures import partial_publication_setup

import h2hdb.vnext_cleanup_repository as cleanup
from h2hdb import CoreConfig
from h2hdb.sql_connector import SQLConnector
from h2hdb.sql_performance import instrument_connector, measure_sql
from h2hdb.vnext_maintenance_gate_repository import GateLease, MaintenanceGateRepository
from h2hdb.vnext_transaction import VNextUnitOfWork

pytestmark = [pytest.mark.cleanup_acceptance, pytest.mark.deep]

_OLD = bytes((23, 1)) + bytes(14)
_RETAINED = bytes((23, 2)) + bytes(14)
_FACTS = {
    "AR_OVERLAY": (("catalog_a_file_decision_shadow_seals", None),),
    "AR_FILE_HASH_VALUES": (
        ("catalog_a_file_decision_shadow_occurrences", "occurrence_count"),
        ("catalog_a_file_decision_shadow_artists", "artist_count"),
        (
            "catalog_a_file_decision_shadow_gallery_artist_max",
            "maximum_gallery_artist_count",
        ),
    ),
    "AR_FILE_HASH_ANCHOR": (("catalog_a_file_decision_shadow_anchors", None),),
    "AR_EVIDENCE": (
        ("catalog_analysis_changed_file_hashes", None),
        ("catalog_analysis_exclusion_delta_seals", None),
    ),
    "AR_EXCLUSION_VALUES": (
        ("catalog_analysis_exclusion_delta_old_excluded_flags", "old_excluded"),
        ("catalog_analysis_exclusion_delta_new_excluded_flags", "new_excluded"),
    ),
    "AR_EXCLUSION_ANCHOR": (("catalog_analysis_exclusion_delta_anchors", None),),
}


@dataclass
class _Counter:
    queries: list[str] = field(default_factory=list)

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        _elapsed: float,
        query: str,
        _rows: int,
    ) -> None:
        if category == "sql":
            self.queries.append(" ".join(query.split()))


@dataclass(frozen=True)
class _Sample:
    phase: str
    keys: tuple[bytes, ...]
    queries: tuple[str, ...]


def _seed_run(connector: SQLConnector, identity: bytes, files: int) -> None:
    seed_analysis_run(
        connector,
        analysis_id=identity,
        build_id=identity,
        policy_id=1,
        input_manifest_sha256=identity * 2,
        started_at=0,
        state="COMPLETE",
        completed_at=1,
    )
    for facts in _FACTS.values():
        for table, column in facts:
            columns = "analysis_id, file_sha256" + (f", {column}" if column else "")
            values = "%s, %s" + (", 1" if column else "")
            connector.execute_many(
                f"INSERT INTO {table} ({columns}) VALUES ({values})",
                [
                    (identity, (position + 1).to_bytes(32, "big"))
                    for position in range(files)
                ],
            )


def _begin(
    config: CoreConfig, gate: GateLease, *, now: int = 2
) -> cleanup.CleanupCycle:
    with closing(open_connector(config)) as connector, connector.transaction():
        return cleanup.VNextCleanupRepository.begin_cycle(
            VNextUnitOfWork(connector, backend=backend_of(config)),
            gate_lease=gate,
            target_kind=cleanup.CleanupTargetKind.ANALYSIS_RUN,
            shard_no=_OLD[0],
            cycle_cutoff_at=100,
            max_rows_per_transaction=256,
            now=now,
        )


def _seed(config: CoreConfig, files: int) -> tuple[GateLease, cleanup.CleanupCycle]:
    initialize_database(config)
    with closing(open_connector(config)) as connector:
        with partial_publication_setup(connector, backend=backend_of(config)):
            for identity in (_OLD, _RETAINED):
                connector.execute(
                    "INSERT INTO catalog_source_build_sealed_ats "
                    "(build_id, sealed_at) VALUES (%s, 0)",
                    (identity,),
                )
                _seed_run(connector, identity, files)
            # Same-shard retention is independent from terminal run state.
            connector.execute(
                "INSERT INTO operational_source_working_builds "
                "(slot, build_id, assigned_at) VALUES (1, %s, 0)",
                (_RETAINED,),
            )
        with connector.transaction():
            gate = MaintenanceGateRepository.claim_exclusive(
                VNextUnitOfWork(connector, backend=backend_of(config)),
                now=1,
                lease_duration=100_000,
            )
    return gate, _begin(config, gate)


def _drain(
    config: CoreConfig,
    gate: GateLease,
    cycle: cleanup.CleanupCycle,
    monkeypatch: pytest.MonkeyPatch,
    *,
    files: int,
    now: int = 3,
) -> tuple[_Sample, ...]:
    samples: list[_Sample] = []
    original = cleanup._run_static_phase

    def observed(
        operation: cleanup._CleanupOperation,
        cursor: bytes,
        plan: cleanup._StaticTargetPlan,
        phase: str,
        **kwargs: Any,
    ) -> cleanup._Mutation:
        if phase not in _FACTS:
            return original(operation, cursor, plan, phase, **kwargs)
        counter = _Counter()
        with measure_sql(counter, observe_nested=True):
            result = original(operation, cursor, plan, phase, **kwargs)
        samples.append(_Sample(phase, result.row_keys, tuple(counter.queries)))
        return result

    all_calls = _Counter()
    with monkeypatch.context() as patch, measure_sql(all_calls, observe_nested=True):
        patch.setattr(cleanup, "_run_static_phase", observed)
        with closing(open_connector(config)) as raw:
            connector = instrument_connector(raw)
            for step in range(64):
                with connector.transaction():
                    result = cleanup.VNextCleanupRepository.advance_current_only_cycle(
                        VNextUnitOfWork(connector, backend=backend_of(config)),
                        gate_lease=gate,
                        cycle=cycle,
                        now=now + step,
                    )
                if result[-1].cycle_complete:
                    break
            else:
                pytest.fail("analysis cleanup exceeded 64 fixture calls")
            for facts in _FACTS.values():
                for table, _column in facts:
                    assert connector.fetch_one(
                        f"SELECT COUNT(*) FROM {table} WHERE analysis_id = %s", (_OLD,)
                    ) == (0,)
                    assert connector.fetch_one(
                        f"SELECT COUNT(*) FROM {table} WHERE analysis_id = %s",
                        (_RETAINED,),
                    ) == (files,)
            assert connector.fetch_all(
                "SELECT analysis_id FROM catalog_analysis_run_descriptor "
                "ORDER BY analysis_id"
            ) == [(_RETAINED,)]
    assert 0 < sum(len(sample.queries) for sample in samples) < len(all_calls.queries)
    return tuple(samples)


def _assert_costs(samples: tuple[_Sample, ...], *, files: int) -> None:
    for phase, facts in _FACTS.items():
        selected = tuple(sample for sample in samples if sample.phase == phase)
        keys = tuple(key for sample in selected for key in sample.keys)
        expected = files * len(facts)
        assert len(keys) == len(set(keys)) == expected
        assert all(len(sample.keys) <= 256 for sample in selected)
        assert selected[-1].keys == ()
        queries = tuple(query for sample in selected for query in sample.queries)
        # Retain the already fixed per-retirement-phase acceptance ceiling.
        ceiling = 2 * ((expected + 63) // 64) + 16 * ((expected + 255) // 256 + 1)
        assert len(queries) <= ceiling, "SQL cost: analysis retirement budget"
        assert all(query.count("%s") <= 900 for query in queries)
        deletes = [query for query in queries if query.startswith("DELETE ")]
        locks = [query for query in queries if "AS requested ON" in query]
        assert len(deletes) == len(locks), "SQL cost: missing bounded locking read"
        assert all(query.count(" OR ") < 64 for query in deletes)


@pytest.mark.parametrize("files", [63, 64, 65, 255, 256, 257])
def test_analysis_retirement_cost_and_retention_at_both_page_boundaries(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, files: int
) -> None:
    gate, cycle = _seed(db_config, files)
    _assert_costs(_drain(db_config, gate, cycle, monkeypatch, files=files), files=files)


def test_analysis_retirement_repeated_cycles_revalidate_retained_sibling(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    files = 65
    gate, cycle = _seed(db_config, files)
    identities: set[bytes] = set()
    signatures = []
    for lap in range(3):
        identities.add(cycle.cleanup_id)
        samples = _drain(
            db_config, gate, cycle, monkeypatch, files=files, now=3 + lap * 100
        )
        _assert_costs(samples, files=files)
        signatures.append(
            tuple(
                (sample.phase, len(sample.keys), len(sample.queries))
                for sample in samples
            )
        )
        if lap < 2:
            with closing(open_connector(db_config)) as connector:
                with partial_publication_setup(
                    connector, backend=backend_of(db_config)
                ):
                    _seed_run(connector, _OLD, files)
            cycle = _begin(db_config, gate, now=90 + lap * 100)
    assert len(identities) == 3
    assert signatures == [signatures[0]] * 3


def test_analysis_cost_oracle_rejects_original_scalar_path(
    sqlite_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    files = 65
    gate, cycle = _seed(sqlite_config, files)
    plan = cleanup._STATIC_PLANS[cleanup.CleanupTargetKind.ANALYSIS_RUN]
    with monkeypatch.context() as patch:
        for phase in _FACTS:
            patch.setitem(
                plan.phases,
                phase,
                tuple(
                    replace(spec, batch_exact_primary_keys=False)
                    for spec in plan.phases[phase]
                ),
            )
        samples = _drain(sqlite_config, gate, cycle, monkeypatch, files=files)
    with pytest.raises(AssertionError, match="SQL cost"):
        _assert_costs(samples, files=files)
