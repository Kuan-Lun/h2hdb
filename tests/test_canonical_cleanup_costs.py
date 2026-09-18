"""Engine-work regression evidence, separate from cleanup result cardinality."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from h2hdb import CoreConfig
from h2hdb import vnext_cleanup_repository as cleanup


@pytest.fixture
def cost_probe() -> ModuleType:
    before = list(sys.path)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    try:
        return importlib.import_module("ingest_canonical_cost")
    finally:
        sys.path[:] = before


def test_cost_oracle_counts_repeated_scans_instead_of_returned_rows(
    cost_probe: ModuleType,
) -> None:
    assert (
        cost_probe.cache_visits(
            [
                {"table_name": "choice", "r_loops": 128, "r_rows": 64.5},
                {"table_name": "title_sort", "r_loops": 4, "r_rows": 2},
                {"table_name": "unrelated", "r_loops": 1000, "r_rows": 1000},
                {"table_name": "choice", "r_loops": 0, "r_rows": None},
            ]
        )
        == 8264
    )


@pytest.mark.parametrize(
    "node",
    [
        {"r_loops": 1},
        {"r_rows": 1},
        {"r_loops": -1, "r_rows": 1},
        {"r_loops": 1, "r_rows": float("nan")},
        {"r_loops": True, "r_rows": 1},
        {"r_loops": False, "r_rows": None},
        {"r_loops": 1, "r_rows": -1},
    ],
)
def test_missing_or_invalid_engine_work_is_not_zero(
    cost_probe: ModuleType, node: dict[str, Any]
) -> None:
    with pytest.raises(RuntimeError, match="valid runtime counts"):
        cost_probe.cache_visits([{"table_name": "choice", **node}])


def test_absent_cache_nodes_fail_closed(cost_probe: ModuleType) -> None:
    with pytest.raises(RuntimeError, match="omitted"):
        cost_probe.cache_visits([{"table_name": "unrelated"}])


@pytest.mark.parametrize("filter_kind", ["index_condition", "rowid_filter"])
def test_filtered_table_rows_do_not_masquerade_as_complete_access_cost(
    cost_probe: ModuleType, filter_kind: str
) -> None:
    with pytest.raises(RuntimeError, match="expanded cost model"):
        cost_probe.cache_visits(
            [{"table_name": "choice", "r_loops": 1, "r_rows": 1, filter_kind: {}}]
        )


def test_linear_target_uses_independent_cardinalities(cost_probe: ModuleType) -> None:
    assert (
        cost_probe.title_cache_budget(
            {"allocations": 913, "display_choices": 128, "title_sorts": 128}
        )
        == 4676
    )
    with pytest.raises(ValueError, match="complete nonnegative"):
        cost_probe.title_cache_budget({"allocations": 913})
    with pytest.raises(ValueError, match="complete nonnegative"):
        cost_probe.title_cache_budget(
            {"allocations": 913, "display_choices": -1, "title_sorts": 128}
        )


def test_historical_control_preserves_query_boundary(cost_probe: ModuleType) -> None:
    prefix = "SELECT r.value_sha256 FROM catalog_canonical_value_allocation_anchors AS r WHERE ("
    suffix = ") ORDER BY r.value_sha256 LIMIT 1"
    query = prefix + cleanup._CANONICAL_VALUE_ELIGIBILITY + suffix
    historical = cost_probe.historical_query(query)
    assert historical != query
    assert historical.startswith(prefix) and historical.endswith(suffix)
    assert "OR title_sort.sort_title_sha256 = r.value_sha256" in historical
    with pytest.raises(ValueError, match="one captured"):
        cost_probe.historical_query(query + query)
    with pytest.raises(ValueError, match="one captured"):
        cost_probe.historical_query("SELECT 1")


def test_historical_control_does_not_accept_silent_fixture_drift(
    cost_probe: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cost_probe, "BASELINE_SHA256", "0" * 64)
    with pytest.raises(RuntimeError, match="fingerprint changed"):
        cost_probe.historical_query("SELECT 1")


@pytest.mark.deep
@pytest.mark.mariadb
def test_real_pipeline_rejects_historical_quadratic_title_cache_work(
    mariadb_config: CoreConfig, tmp_path: Path, cost_probe: ModuleType
) -> None:
    del cost_probe
    previous = list(sys.path)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    try:
        growth = importlib.import_module("ingest_growth_cleanup_probe")
        report = growth.run_case(
            mariadb_config,
            gallery_count=32,
            revisions=2,
            output=tmp_path / "cost.json",
            mode="idle",
            profile_mariadb=True,
        )
    finally:
        sys.path[:] = previous
    assert report["status"] == "completed"
    assert len(report["cases"]) == 2
    for case in report["cases"]:
        probes = case["mariadb_candidate_diagnostics"]["measurement"][
            "candidate_probes"
        ]
        canonical = [p for p in probes if p["target"] == "CANONICAL_VALUE"]
        assert len(canonical) == 1
        comparison = canonical[0]["mariadb_diagnostics"]["canonical_comparison"]
        assert comparison["ordered_full_eligible_sets_equal"]
        assert comparison["eligible_rows"] == 0
        assert comparison["production_target_met"]
        assert comparison["historical_negative_control_rejected"]
        for variant in comparison["variants"].values():
            assert len(variant["handler_repetitions"]) == 3
        assert case["idle_claim_completed"]
        assert case["idle_catalog_snapshot_unchanged"]
        assert case["idle_retained_roots_unchanged"]
