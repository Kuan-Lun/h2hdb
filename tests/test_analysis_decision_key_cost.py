"""Finite source-work contracts for depth-zero duplicate-page decision keys.

These checks bound real query work, not only returned rows. The old production
UNION and an explicit full-scan mutant must violate the predeclared VM budget.
They are not wall-time predictions or proofs about MariaDB execution plans.
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import pytest

from h2hdb import vnext_analysis_repository as analysis
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_analysis_hash_keys import (
    AnalysisHashKeyPlan,
    build_analysis_hash_key_plan,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


@pytest.fixture
def probe() -> Iterator[ModuleType]:
    name = "analysis_decision_key_probe_under_test"
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "analysis_decision_key_probe.py"
    )
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    previous = list(sys.path)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
        yield module
    finally:
        sys.path[:] = previous
        sys.modules.pop(name, None)


@pytest.mark.parametrize(
    "galleries,hashes",
    [
        (15, 1),
        (16, 1),
        (17, 1),
        (127, 1),
        (128, 1),
        (129, 1),
        (2, 127),
        (2, 128),
        (2, 129),
    ],
)
@pytest.mark.parametrize("rejected,history", [(0, 0), (1, 2)])
def test_current_observation_stream_matches_fixed_cost_across_capacity(
    probe: ModuleType, galleries: int, hashes: int, rejected: int, history: int
) -> None:
    shape = probe.Shape(galleries, hashes, rejected, history)
    with probe.databases("sqlite", 1) as connections:
        result = probe.measure_case(next(connections), "sqlite", shape)
    assert result["candidate"]["calls"] == shape.source_calls
    assert result["candidate"]["occurrences"] == shape.occurrences
    assert result["candidate"]["memberships"] == galleries
    assert result["candidate"]["vm_steps"] <= shape.vm_budget
    assert result["unique_keys"] == 1 + shape.occurrences
    assert result["three_traversals_without_sql"]


def test_all_rejected_memberships_still_advance_raw_bounded_pages(
    probe: ModuleType,
) -> None:
    with probe.databases("sqlite", 1) as connections:
        case = probe.measure_case(
            next(connections), "sqlite", probe.Shape(129, 4, 129, 2)
        )
    assert case["candidate"]["memberships"] == 129
    assert case["candidate"]["occurrences"] == 0
    assert case["candidate"]["calls"] == 3
    assert case["unique_keys"] == 1


@pytest.mark.parametrize("galleries", [126, 127, 128])
def test_candidate_timing_includes_one_complete_delivery_and_owned_cleanup(
    probe: ModuleType, galleries: int
) -> None:
    elapsed = 0.0
    delivery_calls = 0
    cleanup_calls = 0
    original_build = probe.build_analysis_hash_key_plan
    original_page = AnalysisHashKeyPlan.source_page
    original_close = AnalysisHashKeyPlan.close
    original_handlers = probe.handler_counts
    original_old = probe.measure_old

    def build(*args: Any, **kwargs: Any) -> AnalysisHashKeyPlan:
        nonlocal elapsed
        result = original_build(*args, **kwargs)
        elapsed += 5
        return cast(AnalysisHashKeyPlan, result)

    def page(
        plan: AnalysisHashKeyPlan, *, after: bytes | None, limit: int
    ) -> tuple[bytes, ...]:
        nonlocal elapsed, delivery_calls
        elapsed += 2
        delivery_calls += 1
        return original_page(plan, after=after, limit=limit)

    def close(plan: AnalysisHashKeyPlan) -> None:
        nonlocal elapsed, cleanup_calls
        elapsed += 3
        cleanup_calls += 1
        original_close(plan)

    def handlers(connector: SQLConnector) -> dict[str, int]:
        nonlocal elapsed
        elapsed += 7
        return cast(dict[str, int], original_handlers(connector))

    def old(connector: SQLConnector, oracle: tuple[bytes, ...]) -> Any:
        nonlocal elapsed
        result = original_old(connector, oracle)
        elapsed += 13
        return result

    with probe.databases("sqlite", 1) as connections:
        connector = cast(SQLConnector, next(connections))
        original_fetch = connector.fetch_all

        def fetch(sql: str, parameters: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
            nonlocal elapsed
            if sql.startswith("EXPLAIN"):
                elapsed += 11
            return original_fetch(sql, parameters)

        with (
            patch.object(probe, "time", SimpleNamespace(perf_counter=lambda: elapsed)),
            patch.object(probe, "build_analysis_hash_key_plan", build),
            patch.object(AnalysisHashKeyPlan, "source_page", page),
            patch.object(AnalysisHashKeyPlan, "close", close),
            patch.object(probe, "handler_counts", handlers),
            patch.object(probe, "measure_old", old),
            patch.object(connector, "fetch_all", fetch),
        ):
            case = probe.measure_case(connector, "sqlite", probe.Shape(galleries, 1))

    # One changed key plus every gallery key, followed by one empty-tail call.
    calls_per_delivery = (galleries + 1 + 127) // 128 + 1
    assert delivery_calls == 3 * calls_per_delivery
    assert cleanup_calls == 1
    assert case["candidate_preparation_seconds"] == 5
    assert case["candidate_first_delivery_seconds"] == 2 * calls_per_delivery
    assert case["candidate_cleanup_seconds"] == 3
    assert case["candidate_seconds"] == 5 + 2 * calls_per_delivery + 3
    assert case["old_union_seconds"] == 13


def test_old_production_query_and_full_scan_mutant_fail_fixed_vm_budget(
    probe: ModuleType,
) -> None:
    shape = probe.Shape(2048, 4)
    with probe.databases("sqlite", 1) as connections:
        connector = cast(SQLConnector, next(connections))
        oracle = probe.seed(connector, "sqlite", shape)
        old = probe.measure_old(connector, oracle)
        assert (
            probe.query_fingerprint(probe.old_query(probe.digest(128))[0])
            == "cca9a391f4977cca"
        )
        with pytest.raises(RuntimeError, match="fixed linear VM budget"):
            probe.require_vm_budget(old, shape)
        # Inject a redundant table walk into every real bounded source query.
        # It returns identical results and preserves query count/page sizes;
        # only actual VM instructions expose the degradation.
        original = connector.fetch_all

        def degraded(
            sql: str, parameters: tuple[Any, ...] = ()
        ) -> list[tuple[Any, ...]]:
            original(
                "SELECT SUM(occurrence_count) FROM catalog_gallery_observation_file_hash_occurrences"
            )
            return original(sql, parameters)

        with (
            patch.object(connector, "fetch_all", degraded),
            probe.measure_reads(connector, source=True) as reads,
        ):
            result = tuple(
                analysis._iter_decision_source_hashes(
                    connector, probe.ANALYSIS, probe.BUILD
                )
            )
        assert tuple(sorted(set(result))) == oracle
        assert (reads.calls, reads.memberships, reads.occurrences) == (
            shape.source_calls,
            shape.galleries,
            shape.occurrences,
        )
        with pytest.raises(RuntimeError, match="fixed linear VM budget"):
            probe.require_source_budget(reads, shape)


@pytest.mark.parametrize("stage", [b"changed_file_hash", b"file_hash_decision"])
def test_hash_key_capability_is_bound_to_its_stage(stage: bytes) -> None:
    from test_analysis_changed_hash_cost import _authority, _issue

    authority = _authority()
    plan = build_analysis_hash_key_plan(authority, b"i" * 32, (b"k" * 32,), stage=stage)
    try:
        issue = replace(_issue(authority), stage=stage)
        page = analysis.AnalysisRepository.prepare_hash_key_page(issue=issue, plan=plan)
        other = (
            b"file_hash_decision"
            if stage == b"changed_file_hash"
            else b"changed_file_hash"
        )
        with pytest.raises(ValueError, match="authority changed"):
            replace(page, stage=other).verify()
        with pytest.raises(analysis.AnalysisNotReadyError, match="another stage"):
            analysis.AnalysisRepository.prepare_hash_key_page(
                issue=replace(issue, stage=other), plan=plan
            )
        plan.stage = other
        with pytest.raises(ValueError, match="metadata was modified"):
            plan.source_page(after=None, limit=128)
    finally:
        plan.close()


@pytest.mark.parametrize("fault", ["working_slot", "checkpoint"])
def test_decision_plan_rechecks_immutable_authority_and_closes_failed_preparation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    from test_vnext_analysis_repository import (
        _authorities,
        _begin,
        _generated_database,
        _run_stage,
        _seed_initial_snapshot,
    )

    connector = _generated_database(tmp_path / "decision-plan-authority.sqlite3")
    plans: list[AnalysisHashKeyPlan] = []
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            _scope, build, _first, _second = _seed_initial_snapshot(connector)
        run = _begin(
            connector, gate, turn, build_id=build, analysis_id=b"d" * 16, now=30
        )
        for operation in (
            analysis.AnalysisRepository.process_changed_gallery_batch,
            analysis.AnalysisRepository.process_changed_file_hash_batch,
        ):
            _run_stage(
                connector,
                gate,
                turn,
                operation,
                analysis_id=run.analysis_id,
                prefix=operation.__name__.encode(),
                max_rows=128,
            )
        with connector.transaction():
            issue = analysis.AnalysisRepository.issue_next_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                batch_key=b"decision",
                max_rows=128,
                now=300,
            )

        def changed(*args: Any, **kwargs: Any) -> AnalysisHashKeyPlan:
            plan = build_analysis_hash_key_plan(*args, **kwargs)
            plans.append(plan)
            with connector.transaction():
                if fault == "working_slot":
                    connector.execute("DELETE FROM operational_source_working_builds")
                else:
                    connector.execute(
                        "UPDATE catalog_analysis_checkpoints SET processed_count = processed_count + 1 WHERE analysis_id = %s AND stage = %s",
                        (run.analysis_id, b"changed_file_hash"),
                    )
            return plan

        monkeypatch.setattr(analysis, "build_analysis_hash_key_plan", changed)
        with pytest.raises(
            analysis.AnalysisNotReadyError, match="working slot|input changed"
        ):
            analysis.AnalysisRepository.prepare_decision_hash_plan(
                connector, backend="sqlite", authority=issue.preparation_authority
            )
        assert len(plans) == 1 and plans[0]._closed
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_a_file_decision_shadow_anchors WHERE analysis_id = %s",
            (run.analysis_id,),
        ) == (0,)
    finally:
        connector.close()


def test_group_keyset_handles_unequal_fanout_empty_tails_and_unordered_union(
    probe: ModuleType,
) -> None:
    counts = (0, 1, 7, 8, 9, 15, 16, 17, 127, 128, 129, 2, 4, 10, 32, 64)
    shape = probe.Shape(16, 129)
    with probe.databases("sqlite", 1) as connections:
        connector = cast(SQLConnector, next(connections))
        probe.seed(connector, "sqlite", shape)
        expected: list[bytes] = []
        with connector.transaction():
            for gallery, count in enumerate(counts, 1):
                connector.execute(
                    "DELETE FROM catalog_gallery_observation_file_hash_occurrences WHERE gallery_id = %s AND file_sha256 > %s",
                    (gallery, probe.digest((gallery - 1) * 129 + count)),
                )
                expected.extend(
                    probe.digest((gallery - 1) * 129 + page)
                    for page in range(1, count + 1)
                )
        original = connector.fetch_all
        statements = []

        def reversed_union(
            sql: str, parameters: tuple[Any, ...] = ()
        ) -> list[tuple[Any, ...]]:
            rows = original(sql, parameters)
            assert len(rows) <= 128
            assert sql.count("AS source_slot") <= 16
            statements.append((sql, parameters))
            return list(reversed(rows))

        with (
            patch.object(connector, "fetch_all", reversed_union),
            probe.measure_reads(connector, source=True) as reads,
        ):
            found = list(
                analysis._iter_observation_hash_group(
                    connector, [(gallery, 1) for gallery in range(1, 17)], lambda: None
                )
            )
        assert sorted(found) == expected
        assert len(found) == len(set(found))
        assert reads.occurrences == sum(counts)
        assert reads.branch_ranges <= sum(count // 8 + 1 for count in counts)
        assert reads.calls < max(counts) // 8 + 1
        # Empty branches leave immediately; all branches progress independently.
        assert all(
            "source_keys_0" not in sql and "source_keys_1 " not in sql
            for sql, _parameters in statements[1:]
        )


@pytest.mark.parametrize("compact", [False, True])
def test_decision_plan_selected_by_depth_including_policy_compaction(
    tmp_path: Path, compact: bool
) -> None:
    from test_vnext_analysis_repository import (
        _authorities,
        _generated_database,
        _independent_file_oracle,
        _prepare_incremental,
        _run_file_slice,
    )
    from vnext_catalog_registry_fixtures import seed_analysis_policy

    connector = _generated_database(tmp_path / "decision-compaction.sqlite3")
    try:
        gate, first_turn = _authorities(connector)
        turn, build, unchanged, removed, added = _prepare_incremental(
            connector, gate, first_turn
        )
        with connector.transaction():
            if compact:
                seed_analysis_policy(
                    connector,
                    policy_id=2,
                    algorithm_version=1,
                    spam_artist_threshold=2,
                    spam_occurrence_threshold=3,
                    content_owner_rule_version=1,
                    gid_winner_rule_version=1,
                )
            run = analysis.AnalysisRepository.begin(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                build_id=build,
                policy_id=2 if compact else 1,
                proposed_analysis_id=b"p" * 16,
                now=530,
            )
        assert run.overlay_depth == (0 if compact else 1)
        assert run.baseline_analysis_id == b"B" * 16
        original = analysis.AnalysisRepository.prepare_decision_hash_plan
        plans: list[AnalysisHashKeyPlan | None] = []

        def prepare(*args: Any, **kwargs: Any) -> AnalysisHashKeyPlan | None:
            plan = original(*args, **kwargs)
            plans.append(plan)
            if plan is not None:
                assert plan.source_page(after=None, limit=128) == tuple(
                    sorted((unchanged, removed, added))
                )
            return plan

        with patch.object(
            analysis.AnalysisRepository, "prepare_decision_hash_plan", prepare
        ):
            _run_file_slice(
                connector, gate, turn, run.analysis_id, max_rows=1, start_now=600
            )
        assert len(plans) == 1
        assert (plans[0] is not None) == compact
        if plans[0] is not None:
            assert plans[0]._closed
        actual = {
            row[0]: tuple(row[1:])
            for row in connector.fetch_all(
                "SELECT file_sha256, occurrence_count, artist_count, maximum_gallery_artist_count FROM catalog_analysis_file_hash_decision_resolved WHERE analysis_id = %s",
                (run.analysis_id,),
            )
        }
        assert actual == _independent_file_oracle(connector, build)
    finally:
        connector.close()
