"""Finite one-pass correspondence, source semantics, and rejected old-query cost.

Dimensions: changed galleries, hashes per observation, unrelated history, and
128-row page capacity. Units: connector rows/calls and SQLite VM instructions;
wall time is diagnostic only. The independently retained UNION implementation
must fail the same cost bound, so LIMIT alone cannot satisfy the regression.
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from h2hdb import vnext_analysis_repository as analysis
from h2hdb.vnext_changed_hash_plan import build_changed_hash_plan


@pytest.fixture
def probe() -> Iterator[ModuleType]:
    name = "analysis_changed_hash_probe_under_test"
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "analysis_changed_hash_probe.py"
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


def _authority() -> analysis.AnalysisPreparationAuthority:
    return analysis.AnalysisPreparationAuthority(
        b"a" * 16, b"c" * 16, 1, 1, b"m" * 32, (), analysis._PREPARATION_TOKEN
    )


def _issue(
    authority: analysis.AnalysisPreparationAuthority,
) -> analysis.AnalysisStageIssue:
    return analysis.AnalysisStageIssue(
        authority.analysis_id,
        authority.build_id,
        b"changed_file_hash",
        b"page",
        128,
        1,
        analysis._encode_cursor(b"D", None),
        0,
        (),
        (),
        authority,
        None,
        None,
        analysis._STAGE_ISSUE_TOKEN,
    )


@pytest.mark.parametrize("count", [0, 1, 127, 128, 129, 257])
def test_sorted_unique_plan_pages_cross_capacity_and_repeat(count: int) -> None:
    keys = tuple(index.to_bytes(32, "big") for index in range(count))
    authority = _authority()
    plan = build_changed_hash_plan(authority, b"i" * 32, (*reversed(keys), *keys))
    try:
        assert plan.row_count == count
        for _cycle in range(3):
            after = None
            found: list[bytes] = []
            while True:
                page = plan.source_page(after=after, limit=128)
                assert len(page) <= 128
                if not page:
                    break
                found.extend(page)
                after = page[-1]
            assert tuple(found) == keys
        prepared = analysis.AnalysisRepository.prepare_changed_hash_page(
            issue=_issue(authority), plan=plan
        )
        assert prepared.keys == keys[:128]
        prepared.verify()
    finally:
        plan.close()
    plan.close()
    with pytest.raises(ValueError, match="closed"):
        plan.source_page(after=None, limit=1)


@pytest.mark.parametrize("fault", ["count", "authority", "binding", "record", "append"])
def test_changed_hash_plan_rejects_mutated_authority_and_payload(fault: str) -> None:
    plan = build_changed_hash_plan(_authority(), b"i" * 32, (b"1" * 32, b"2" * 32))
    try:
        match fault:
            case "count":
                plan.row_count = 1
                plan._payload.truncate(64)
            case "authority":
                plan.authority = replace(plan.authority, generation=2)
            case "binding":
                plan.input_binding = b"z" * 32
            case "record":
                plan._payload.seek(0)
                plan._payload.write(b"x")
            case _:
                plan._payload.seek(0, 2)
                plan._payload.write(b"x")
        with pytest.raises(ValueError, match="modified|length changed"):
            plan.source_page(after=None, limit=1)
    finally:
        plan.close()


@pytest.mark.parametrize(
    "field,value",
    [
        ("keys", (b"x" * 32,)),
        ("batch_key", b"other"),
        ("source_count", 2),
        ("checkpoint_cursor", b"wrong"),
        ("input_binding", b"x" * 32),
    ],
)
def test_changed_hash_page_rejects_forged_membership_and_coordinates(
    field: str, value: Any
) -> None:
    plan = build_changed_hash_plan(_authority(), b"i" * 32, (b"1" * 32,))
    try:
        page = analysis.AnalysisRepository.prepare_changed_hash_page(
            issue=_issue(plan.authority), plan=plan
        )
        with pytest.raises(ValueError, match="changed|modified"):
            replace(page, **{field: value}).verify()
        with pytest.raises(RuntimeError, match="another authority"):
            analysis.AnalysisRepository.prepare_changed_hash_page(
                issue=_issue(replace(plan.authority, generation=2)), plan=plan
            )
    finally:
        plan.close()


@pytest.mark.parametrize(
    "galleries,hashes", [(127, 1), (128, 1), (129, 1), (2, 127), (2, 128), (2, 129)]
)
def test_runtime_source_pages_match_one_pass_cost_at_both_capacity_boundaries(
    probe: ModuleType, galleries: int, hashes: int
) -> None:
    shape = probe.Shape(galleries, hashes)
    with probe.databases("sqlite", 1) as connections:
        result = probe.measure_case(next(connections), "sqlite", shape)
    assert result["source_calls"] == shape.source_calls
    assert result["source_memberships"] == galleries
    assert result["source_occurrences"] == 2 * galleries * hashes
    assert result["three_local_traversals_match"]


def test_unrelated_history_adds_no_source_calls_or_source_rows(
    probe: ModuleType,
) -> None:
    shapes = (probe.Shape(2, 129), probe.Shape(2, 129, 1000))
    with probe.databases("sqlite", 2) as connections:
        results = [
            probe.measure_case(connector, "sqlite", shape)
            for connector, shape in zip(connections, shapes, strict=True)
        ]
    for name in (
        "source_calls",
        "source_memberships",
        "source_occurrences",
        "unique_hashes",
    ):
        assert results[0][name] == results[1][name]
    assert (
        abs(results[0]["sqlite_vm_steps"] - results[1]["sqlite_vm_steps"])
        <= 100 * shapes[0].source_calls
    )


def test_deliberate_old_union_mutant_fails_actual_vm_work_bound(
    probe: ModuleType,
) -> None:
    shape = probe.Shape(32, 128)
    with probe.databases("sqlite", 1) as connections:
        result = probe.measure_case(
            next(connections), "sqlite", shape, complete_old=True
        )
    reads = probe.Reads(
        calls=result["source_calls"],
        memberships=result["source_memberships"],
        occurrences=result["source_occurrences"],
        vm_steps=result["sqlite_vm_steps"],
    )
    probe.require_linear_source_cost(reads, shape)
    reads.vm_steps = result["old"]["vm_steps"]
    with pytest.raises(RuntimeError, match="VM work exceeds"):
        probe.require_linear_source_cost(reads, shape)


def test_current_baseline_rejected_removed_added_and_duplicate_hash_semantics(
    probe: ModuleType,
) -> None:
    with probe.databases("sqlite", 1) as connections:
        connector = next(connections)
        probe.seed(connector, "sqlite", probe.Shape(5, 2, 20))
        with connector.transaction():
            connector.execute(
                "DELETE FROM catalog_source_build_galleries WHERE build_id = %s AND gallery_id = 1",
                (probe.CURRENT,),
            )
            connector.execute(
                "DELETE FROM catalog_source_build_galleries WHERE build_id = %s AND gallery_id = 2",
                (probe.BASELINE,),
            )
            connector.execute(
                "UPDATE catalog_gallery_observation_validation_dispositions SET accepted = 0 WHERE gallery_id = 3"
            )
            connector.execute(
                "UPDATE catalog_gallery_observation_validation_dispositions SET accepted = 0 WHERE gallery_id = 4 AND observation_id = 2"
            )
            connector.execute(
                "UPDATE catalog_gallery_observation_file_hash_occurrences SET file_sha256 = %s WHERE gallery_id = 5 AND file_sha256 = %s",
                (probe.key(1, 0), probe.key(5, 0)),
            )
            connector.execute(
                "UPDATE catalog_source_build_galleries SET observation_id = 1 WHERE gallery_id = 5 AND build_id = %s",
                (probe.CURRENT,),
            )
        expected = tuple(
            sorted(
                {
                    probe.key(gallery, page)
                    for gallery in (1, 2, 4, 5)
                    for page in range(2)
                }
                - {probe.key(5, 0)}
            )
        )
        plan = build_changed_hash_plan(
            _authority(),
            b"i" * 32,
            analysis._iter_changed_source_hashes(
                connector, probe.ANALYSIS, probe.CURRENT, probe.BASELINE
            ),
        )
        try:
            assert plan.source_page(after=None, limit=128) == expected
            assert plan.source_page(after=expected[2], limit=128) == expected[3:]
            assert (
                tuple(row[0] for row in connector.fetch_all(*probe.old_query(None)))
                == expected
            )
        finally:
            plan.close()
        current_only = set(
            analysis._iter_changed_source_hashes(
                connector, probe.ANALYSIS, probe.CURRENT, None
            )
        )
        assert current_only == {probe.key(2, page) for page in range(2)} | {
            probe.key(1, 0),
            probe.key(5, 1),
        }


@pytest.mark.parametrize("fault", ["working_slot", "changed_checkpoint"])
def test_plan_rechecks_durable_source_authority_after_reads_and_closes_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    from test_vnext_analysis_repository import (
        _authorities,
        _begin,
        _generated_database,
        _run_stage,
        _seed_initial_snapshot,
    )

    from h2hdb.vnext_changed_hash_plan import AnalysisChangedHashPlan
    from h2hdb.vnext_transaction import VNextUnitOfWork

    connector = _generated_database(tmp_path / "changed-hash-authority.sqlite3")
    plans: list[AnalysisChangedHashPlan] = []
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            _scope, build, _first, _second = _seed_initial_snapshot(connector)
        run = _begin(
            connector, gate, turn, build_id=build, analysis_id=b"z" * 16, now=30
        )
        _run_stage(
            connector,
            gate,
            turn,
            analysis.AnalysisRepository.process_changed_gallery_batch,
            analysis_id=run.analysis_id,
            prefix=b"changed",
            max_rows=128,
        )
        with connector.transaction():
            issue = analysis.AnalysisRepository.issue_next_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                batch_key=b"hashes",
                max_rows=128,
                now=300,
            )
        original = build_changed_hash_plan

        def changed(*args: Any, **kwargs: Any) -> AnalysisChangedHashPlan:
            plan = original(*args, **kwargs)
            plans.append(plan)
            with connector.transaction():
                if fault == "working_slot":
                    connector.execute("DELETE FROM operational_source_working_builds")
                else:
                    connector.execute(
                        "UPDATE catalog_analysis_checkpoints SET processed_count = processed_count + 1 WHERE analysis_id = %s AND stage = %s",
                        (run.analysis_id, b"changed_gallery"),
                    )
            return plan

        monkeypatch.setattr(analysis, "build_changed_hash_plan", changed)
        with pytest.raises(
            analysis.AnalysisNotReadyError, match="working slot|input changed"
        ):
            analysis.AnalysisRepository.prepare_changed_hash_plan(
                connector, backend="sqlite", authority=issue.preparation_authority
            )
        assert len(plans) == 1 and plans[0]._closed
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_analysis_changed_file_hashes WHERE analysis_id = %s",
            (run.analysis_id,),
        ) == (0,)
    finally:
        connector.close()


def test_preparation_progress_counts_galleries_instead_of_hashes(
    probe: ModuleType,
) -> None:
    with probe.databases("sqlite", 1) as connections:
        connector = next(connections)
        probe.seed(connector, "sqlite", probe.Shape(2, 129))
        counts: list[int] = []
        keys = tuple(
            analysis._iter_changed_source_hashes(
                connector, probe.ANALYSIS, probe.CURRENT, probe.BASELINE, counts.append
            )
        )
        assert len(keys) == 516
        assert counts == sorted(counts)
        assert counts[0] == 0 and counts[-1] == 2
        assert set(counts) == {0, 1, 2}
