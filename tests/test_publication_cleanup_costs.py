"""Constructed Lean traces versus actual publication-cleanup connector calls.

Cost is client SQL calls, with mutation, selection, cursor coverage and terminal
checks counted separately. Fixtures omit unrelated publication families and do
not claim a full READY audit; gate/frozen-root/receipt/checkpoint transitions and
the SQL mutations use production code. These are finite correspondence
checks on SQLite and MariaDB, not a wall-clock, query-plan or universal
Python-refinement proof.
"""

from __future__ import annotations

import shutil
import sqlite3
import subprocess
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import pytest
from vnext_fault_harness import backend_of, open_connector
from vnext_publication_cleanup_fixtures import (
    PUBLICATION_KEY,
    partial_publication_setup,
    seed_publication_cleanup,
)

import h2hdb.vnext_cleanup_repository as cleanup
from h2hdb import CoreConfig, DatabaseConfig
from h2hdb.sql_performance import instrument_connector, measure_sql
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_cleanup_repository import CleanupCycle, VNextCleanupRepository
from h2hdb.vnext_maintenance_gate_repository import GateLease
from h2hdb.vnext_transaction import VNextUnitOfWork

pytestmark = pytest.mark.cleanup_acceptance

MODEL = (
    Path(__file__).resolve().parents[1]
    / "verification/lean/PublicationCleanupBatch.lean"
)
_PHASE_TABLE = {
    "CP_SUBJECT": "catalog_subjects",
    "CP_CONTRIBUTOR": "catalog_contributors",
}
_ORDERED_ARITY = {"CP_SUBJECT": 5, "CP_CONTRIBUTOR": 6}
_ModelCosts = dict[tuple[int, int, int], tuple[int, int, int]]


@pytest.fixture(scope="module")
def model_costs() -> _ModelCosts:
    lean = shutil.which("lean")
    assert lean is not None, "Lean is required for executable cost correspondence"
    completed = subprocess.run(
        [lean, "--error=warning", "--run", str(MODEL)],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    result: _ModelCosts = {}
    for line in completed.stdout.splitlines():
        fixed, arity, rows, capacity, chunks, sql = map(int, line.split(","))
        result[fixed, arity, rows] = capacity, chunks, sql
    assert (5, 5, 65) in result and (515, 6, 256) in result
    return result


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
class _PhaseSample:
    phase: str
    rows: int
    fixed_binds: int
    has_cursor: bool
    unchecked_specs: int
    queries: tuple[str, ...]


def _config(path: Path) -> CoreConfig:
    return CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))


def _drain(
    config: CoreConfig,
    gate: GateLease,
    cycle: CleanupCycle,
    monkeypatch: pytest.MonkeyPatch,
    *,
    phase: str,
    start_at: int = 3,
) -> tuple[_PhaseSample, ...]:
    samples: list[_PhaseSample] = []
    original = cleanup._run_static_phase

    def observed(
        operation: cleanup._CleanupOperation,
        cursor: bytes,
        plan: cleanup._StaticTargetPlan,
        current_phase: str,
        **kwargs: Any,
    ) -> cleanup._Mutation:
        if current_phase != phase:
            return original(operation, cursor, plan, current_phase, **kwargs)
        # Count the whole production phase, not merely the optimized helper.
        # Inclusive delivery remains correct if another telemetry scope nests.
        counter = _Counter()
        preceding = tuple(plan.phases)[: tuple(plan.phases).index(phase) + 1]
        unchecked = sum(
            len(plan.phases[name])
            for name in preceding
            if name not in operation.empty_static_phases
        )
        with measure_sql(counter, observe_nested=True):
            result = original(operation, cursor, plan, current_phase, **kwargs)
        samples.append(
            _PhaseSample(
                phase,
                len(result.row_keys),
                3 + 2 * len(operation.frozen_roots),
                bool(cursor),
                unchecked,
                tuple(counter.queries),
            )
        )
        return result

    all_calls = _Counter()
    with monkeypatch.context() as patch:
        patch.setattr(cleanup, "_run_static_phase", observed)
        with closing(open_connector(config)) as raw:
            if isinstance(raw, SQLiteConnector):
                raw.connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999)
            with measure_sql(all_calls, observe_nested=True):
                connector = instrument_connector(raw)
                for step in range(32):
                    with connector.transaction():
                        result = VNextCleanupRepository.advance_current_only_cycle(
                            VNextUnitOfWork(connector, backend=backend_of(config)),
                            gate_lease=gate,
                            cycle=cycle,
                            now=start_at + step,
                        )
                    if result[-1].cycle_complete:
                        break
                else:
                    pytest.fail("bounded fixture did not finish its cleanup cycle")
                table = _PHASE_TABLE[phase]
                assert connector.fetch_one(
                    f"SELECT COUNT(*) FROM {table} WHERE revision = 1"
                ) == (0,)
                assert connector.fetch_one(
                    "SELECT COUNT(*) FROM catalog_publication_occurrence_identities "
                    "WHERE revision = 1"
                ) == (0,)
    assert sum(len(sample.queries) for sample in samples) < len(all_calls.queries)
    assert samples
    return tuple(samples)


def _assert_costs(
    samples: tuple[_PhaseSample, ...], model: _ModelCosts, *, expected_rows: int
) -> None:
    assert sum(sample.rows for sample in samples) == expected_rows
    assert samples[-1].rows == 0
    for sample in samples:
        capacity, chunks, mutation_sql = model[
            sample.fixed_binds, _ORDERED_ARITY[sample.phase], sample.rows
        ]
        assert capacity <= 64
        # One candidate query per selected relation in this single-spec phase.
        # A fresh transaction separately rechecks any existing cursor prefix.
        # Empty terminal checks cover the still-unproved preceding specs, with
        # independently stated 8-arm and 900-bind budgets.
        terminal_width = min(8, 900 // sample.fixed_binds)
        terminal_sql = (
            (sample.unchecked_specs + terminal_width - 1) // terminal_width
            if sample.rows == 0
            else 0
        )
        fixed_sql = 1 + int(sample.has_cursor) + terminal_sql
        locks = sum("AS requested ON" in query for query in sample.queries)
        deletes = sum(
            query.startswith(f"DELETE FROM {_PHASE_TABLE[sample.phase]} ")
            for query in sample.queries
        )
        assert locks == deletes == chunks, "SQL cost: exact-key chunk count differs"
        assert len(sample.queries) == mutation_sql + fixed_sql, (
            "SQL cost: whole-phase work exceeds the constructed trace and "
            "explicit selection/cursor/terminal costs"
        )
        assert all(query.count("%s") <= 900 for query in sample.queries)


def test_runtime_capacity_matches_constructed_model(model_costs: _ModelCosts) -> None:
    for (fixed, arity, _rows), (capacity, _chunks, _sql) in model_costs.items():
        assert (
            cleanup._static_delete_page_size(fixed_binds=fixed, key_arity=arity)
            == capacity
        )
        assert fixed + capacity * arity + 1 <= 900
    # One extra fixed bind crosses the six-column query's actual capacity.
    assert model_costs[515, 6, 64][0] == 64
    assert model_costs[516, 6, 64][0] == 63


@pytest.mark.deep
def test_maximum_frozen_roots_and_widest_publication_keys_fit_actual_sql_budget(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, model_costs: _ModelCosts
) -> None:
    phase = "CP_CONTRIBUTOR"
    gate, cycle = seed_publication_cleanup(
        db_config, rows=1, phase=phase, root_count=256
    )
    samples = _drain(db_config, gate, cycle, monkeypatch, phase=phase)
    assert all(sample.fixed_binds == 515 for sample in samples)
    _assert_costs(samples, model_costs, expected_rows=256)
    assert (
        max(
            query.count("%s")
            for sample in samples
            for query in sample.queries
            if "AS requested ON" in query
        )
        == 900
    )


@pytest.mark.parametrize("phase", ["CP_SUBJECT", "CP_CONTRIBUTOR"])
@pytest.mark.parametrize("rows", [63, 64, 65, 255, 256, 257])
@pytest.mark.deep
def test_publication_phase_sql_matches_lean_at_both_batch_boundaries(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    model_costs: _ModelCosts,
    phase: str,
    rows: int,
) -> None:
    gate, cycle = seed_publication_cleanup(db_config, rows=rows, phase=phase)
    samples = _drain(db_config, gate, cycle, monkeypatch, phase=phase)
    _assert_costs(samples, model_costs, expected_rows=rows)
    with closing(open_connector(db_config)) as connector:
        assert connector.fetch_one(
            f"SELECT COUNT(*) FROM {_PHASE_TABLE[phase]} WHERE revision = 2"
        ) == (rows,)


@pytest.mark.parametrize("mutant", ["scalar_chunks", "extra_per_row_query"])
def test_cost_oracle_rejects_real_per_row_regression_despite_correct_deletion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    model_costs: _ModelCosts,
    mutant: str,
) -> None:
    phase = "CP_SUBJECT"
    baseline_config = _config(tmp_path / "baseline.sqlite3")
    gate, cycle = seed_publication_cleanup(baseline_config, rows=65, phase=phase)
    baseline = _drain(baseline_config, gate, cycle, monkeypatch, phase=phase)
    _assert_costs(baseline, model_costs, expected_rows=65)

    config = _config(tmp_path / "degraded.sqlite3")
    gate, cycle = seed_publication_cleanup(config, rows=65, phase=phase)
    original = cleanup._delete_static_key_page

    def degraded(work: VNextUnitOfWork, **kwargs: Any) -> None:
        if kwargs["phase"] != phase:
            original(work, **kwargs)
        elif mutant == "scalar_chunks":
            for candidate in kwargs["candidates"]:
                original(work, **{**kwargs, "candidates": (candidate,)})
        else:
            for _candidate in kwargs["candidates"]:
                # The real connector executes each unnecessary query. Keeping
                # the batch helper's outputs correct must not satisfy cost.
                work.connector.fetch_one("SELECT 1")
            original(work, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(cleanup, "_delete_static_key_page", degraded)
        samples = _drain(config, gate, cycle, monkeypatch, phase=phase)
    with pytest.raises(AssertionError, match="SQL cost"):
        _assert_costs(samples, model_costs, expected_rows=65)
    with closing(open_connector(config)) as connector:
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_subjects WHERE revision = 2"
        ) == (65,)


@pytest.mark.deep
def test_fresh_cleanup_cycles_repeat_the_same_sql_cost_without_cached_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, model_costs: _ModelCosts
) -> None:
    config = _config(tmp_path / "cycles.sqlite3")
    phase = "CP_CONTRIBUTOR"
    gate, cycle = seed_publication_cleanup(config, rows=65, phase=phase)
    with closing(open_connector(config)) as connector:
        occurrence = connector.fetch_one(
            "SELECT catalog_occurrence_sha256, revision, publication_key "
            "FROM catalog_publication_occurrence_identities WHERE revision = 1"
        )
        children = connector.fetch_all(
            "SELECT revision, publication_key, contributor_name_sha256, role, position "
            "FROM catalog_contributors WHERE revision = 1 ORDER BY position"
        )
    cycle_ids: set[bytes] = set()
    signatures: list[tuple[tuple[int, int], ...]] = []
    for lap in range(3):
        cycle_ids.add(cycle.cleanup_id)
        samples = _drain(
            config, gate, cycle, monkeypatch, phase=phase, start_at=3 + 100 * lap
        )
        _assert_costs(samples, model_costs, expected_rows=65)
        signatures.append(
            tuple((sample.rows, len(sample.queries)) for sample in samples)
        )
        if lap == 2:
            break
        with closing(open_connector(config)) as connector:
            with partial_publication_setup(connector, backend="sqlite"):
                connector.execute(
                    "INSERT INTO catalog_publication_occurrence_identities "
                    "(catalog_occurrence_sha256, revision, publication_key) "
                    "VALUES (%s, %s, %s)",
                    occurrence,
                )
                connector.execute_many(
                    "INSERT INTO catalog_contributors "
                    "(revision, publication_key, contributor_name_sha256, role, position) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    children,
                )
            with connector.transaction():
                cycle = VNextCleanupRepository.begin_cycle(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    target_kind=cleanup.CleanupTargetKind.CATALOG_PUBLICATION,
                    shard_no=PUBLICATION_KEY[0],
                    cycle_cutoff_at=100,
                    max_rows_per_transaction=256,
                    now=50 + 100 * lap,
                )
    assert len(cycle_ids) == 3
    assert signatures == [signatures[0]] * 3
