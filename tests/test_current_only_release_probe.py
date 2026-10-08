"""The local release-cost oracle rejects repeated scans and missing evidence."""

from __future__ import annotations

import copy
import importlib.util
import json
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from vnext_test_database import DatabaseFactory, database_connector

from h2hdb import CoreConfig


@pytest.fixture
def release_probe() -> Iterator[ModuleType]:
    name = "current_only_release_probe_under_test"
    path = Path(__file__).parents[1] / "scripts" / "current_only_release_probe.py"
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


def _operations(probe: ModuleType) -> list[dict[str, Any]]:
    targets = [target.value for target in probe.cleanup._CURRENT_ONLY_TARGET_PRIORITY]
    proof = {
        "owner_token": "12" * 16,
        "gate_generation": 1,
        "cycle_cutoff_at": 100,
        "absent_targets": ["CONTENT_BLOB", "FILE_NAME_IDENTITY"],
    }
    queries = [{"target": target, "candidate_found": False} for target in targets]
    authority = [
        {
            "kind": "selection",
            "fences": [
                {
                    "owner_token": proof["owner_token"],
                    "gate_generation": 1,
                    "mode": "EXCLUSIVE",
                    "slots": list(range(64)),
                    "validated_at": 101,
                    "lease_expires_at": 200,
                }
            ],
            "cycle_cutoff_at": 100,
            "proof_in": None,
            "proof_out": proof,
            "queries": queries,
            "state": "DONE",
        },
        {
            "kind": "release",
            "fences": [],
            "queries": [],
            "owner_token": proof["owner_token"],
            "gate_generation": 1,
        },
    ]
    return [
        {
            "released": 1,
            "advance_count": 0,
            "candidate_probes": [],
            "outcome": "PROGRESSED",
            "whole_operation_native_work": {"sqlite_progress_operations_estimate": 100},
        },
        {
            "released": 0,
            "advance_count": 1,
            "candidate_probes": copy.deepcopy(queries),
            "terminal_authority": authority,
            "outcome": "DONE",
            "whole_operation_native_work": {"sqlite_progress_operations_estimate": 200},
        },
    ]


def test_result_equivalent_repeated_scan_is_rejected(release_probe: ModuleType) -> None:
    good = _operations(release_probe)
    assert release_probe.evaluate(good, 1)["passed"]
    degraded = copy.deepcopy(good)
    degraded[0]["candidate_probes"] = copy.deepcopy(good[-1]["candidate_probes"])
    result = release_probe.evaluate(degraded, 1)
    assert result["evidence_complete"]
    assert not result["passed"]
    assert result["violations"] == ["pure_release_repeats_eligibility"]


@pytest.mark.parametrize("failure", ("empty", "native", "terminal", "release"))
def test_missing_evidence_cannot_pass(release_probe: ModuleType, failure: str) -> None:
    operations = _operations(release_probe)
    if failure == "empty":
        operations.clear()
    elif failure == "native":
        del operations[0]["whole_operation_native_work"]
    elif failure == "terminal":
        operations[-1]["candidate_probes"].pop()
    else:
        operations[0]["released"] = 0
    assert not release_probe.evaluate(operations, 1)["passed"]


@pytest.mark.parametrize(
    "failure",
    (
        "none",
        "proof",
        "origin",
        "fence",
        "expired",
        "owner",
        "generation",
        "cutoff",
        "invalidation",
        "release",
        "release_owner",
        "unknown",
    ),
)
def test_terminal_reuse_requires_complete_same_attempt_evidence(
    release_probe: ModuleType, failure: str
) -> None:
    operations = _operations(release_probe)
    terminal = operations[-1]
    events = terminal["terminal_authority"]
    repeated = copy.deepcopy(events[0])
    repeated["proof_in"] = copy.deepcopy(repeated["proof_out"])
    repeated["queries"] = [
        query
        for query in repeated["queries"]
        if query["target"] not in {"CONTENT_BLOB", "FILE_NAME_IDENTITY"}
    ]
    events.insert(1, repeated)
    terminal["candidate_probes"].extend(copy.deepcopy(repeated["queries"]))
    if failure == "proof":
        del repeated["proof_in"]
    elif failure == "origin":
        events.pop(0)
    elif failure == "fence":
        repeated["fences"].clear()
    elif failure == "expired":
        repeated["fences"][0]["validated_at"] = 200
    elif failure in {"owner", "generation", "cutoff"}:
        key, value = {
            "owner": ("owner_token", "34" * 16),
            "generation": ("gate_generation", 2),
            "cutoff": ("cycle_cutoff_at", 101),
        }[failure]
        repeated["proof_in"][key] = value
    elif failure == "invalidation":
        events.insert(
            1,
            {
                "kind": "advance",
                "fences": copy.deepcopy(repeated["fences"]),
                "target": "SOURCE_BUILD",
                "queries": [],
            },
        )
    elif failure == "release":
        events.pop()
    elif failure == "release_owner":
        events[-1]["owner_token"] = "34" * 16
    elif failure == "unknown":
        repeated["kind"] = "unclassified"
    result = release_probe.evaluate(operations, 1)
    assert result["passed"] is (failure == "none")
    assert result["evidence_complete"] is (failure == "none")
    assert result["terminal_fresh_authority"] is (failure == "none")
    # Physical SQL and proof-backed authority are reported separately.
    assert not result["terminal_fresh_scan"]


def test_native_budget_is_fixed_and_rejects_cost_transfer(
    release_probe: ModuleType,
) -> None:
    case = {
        "measurement_schema": release_probe.MEASUREMENT_SCHEMA,
        "backend": "sqlite",
        "unique_filenames": True,
        "actual_A": 1,
        "B": 1,
        "before": {"N": {"canonical": 500}},
        "cost_result": {
            "pure_release_native_work": 1000,
            "whole_drain_native_work": 2000,
        },
    }
    candidate = copy.deepcopy(case)
    candidate["cost_result"]["pure_release_native_work"] = 500
    assert release_probe.compare_native(case, candidate)["passed"]
    candidate["cost_result"]["pure_release_native_work"] = 501
    assert not release_probe.compare_native(case, candidate)["passed"]
    candidate["before"]["N"]["canonical"] += 1
    with pytest.raises(ValueError, match="actual N/A/B"):
        release_probe.compare_native(case, candidate)


def test_native_operation_counts_queries_and_restores_connections(
    release_probe: ModuleType, database_factory: DatabaseFactory
) -> None:
    config = database_factory.config("native_probe")
    with release_probe.native_operation(config.database.sql_type) as native:
        with database_connector(config) as connector:
            # A recursive query does engine work on both real selected backends.
            with release_probe.growth.sample_sqlite_candidate(connector) as sample:
                row = connector.fetch_one(
                    "WITH RECURSIVE tally(n) AS (SELECT 1 UNION ALL "
                    "SELECT n + 1 FROM tally WHERE n < 100) SELECT SUM(n) FROM tally"
                )
                assert int(row[0]) == 5050
            connector.fetch_one("SELECT 42")
            if config.database.sql_type == "sqlite":
                assert sample is not None
                assert native["sqlite_progress_callbacks"] >= sample.callbacks > 0
    assert native["connections"] == 1
    assert release_probe.native_total({"whole_operation_native_work": native}) > 0
    with database_connector(config) as connector:
        assert connector.fetch_one("SELECT 42")[0] == 42


@pytest.mark.deep
@pytest.mark.cleanup_acceptance
@pytest.mark.parametrize("degraded", (False, True), ids=("candidate", "repeated-scan"))
def test_real_release_cost_rejects_a_result_equivalent_degradation(
    release_probe: ModuleType,
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    degraded: bool,
) -> None:
    """Execute the real slower path; do not manufacture measurement records.

    Both variants generate the same one-gallery public graph, settle database
    cleanup to BLOCKED without adapters, then release its two orphan resources
    with B=1. Suppressing the fresh hint deliberately restores the repeated
    eligibility scan without changing release correctness or terminal cleanup.
    """

    if degraded:
        monkeypatch.setattr(
            release_probe.artifact_release.ArtifactReleaseRepository,
            "has_pending_release",
            staticmethod(lambda _work: False),
        )
    result = release_probe.run_case(
        db_config, galleries=1, backlog=2, capacity=1, unique_filenames=True
    )
    assert result["actual_A"] == 2
    assert result["B"] == 1
    assert result["correctness"] == {
        "READY": True,
        "current_catalog_unchanged": True,
        "current_library_unchanged": True,
        "next_claim_completed": True,
    }
    cost = result["cost_result"]
    assert cost["evidence_complete"]
    assert cost["pure_release_calls"] == 2
    assert cost["terminal_fresh_authority"]
    assert cost["pure_release_native_work"] > 0
    assert cost["passed"] is not degraded
    if degraded:
        assert cost["violations"] == ["pure_release_repeats_eligibility"]
        assert cost["pure_release_eligibility_probes"] > 0
    else:
        assert cost["violations"] == []
        assert cost["pure_release_eligibility_probes"] == 0
    # Corrupt only the recorded evidence from this actual backend execution.
    # Identical DONE/native/correctness results cannot hide a missing or invalid
    # proof, fence, or source observation in the performance acceptance report.
    for failure in ("proof", "fence", "identity", "origin"):
        operations = copy.deepcopy(result["operations"])
        events = operations[-1]["terminal_authority"]
        reused = next(
            event
            for event in events
            if event.get("proof_in") is not None
            if event["proof_in"]["absent_targets"]
        )
        if failure == "proof":
            del reused["proof_in"]
        elif failure == "fence":
            reused["fences"].clear()
        elif failure == "identity":
            reused["proof_in"]["gate_generation"] += 1
        else:
            del events[: events.index(reused)]
        invalid = release_probe.evaluate(operations, result["actual_A"])
        assert not invalid["passed"]
        assert not invalid["evidence_complete"]
        assert not invalid["terminal_fresh_authority"]


def test_cli_reports_incomplete_instead_of_success_for_invalid_input(
    tmp_path: Path,
) -> None:
    script = Path(__file__).parents[1] / "scripts" / "current_only_release_probe.py"
    output = tmp_path / "invalid.json"
    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--backend",
            "sqlite",
            "--galleries",
            "0",
            "--backlog",
            "0",
            "--capacity",
            "1",
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert completed.returncode == 2
    assert json.loads(completed.stderr) == {
        "status": "incomplete",
        "error_type": "ValueError",
    }
    assert not output.exists()
