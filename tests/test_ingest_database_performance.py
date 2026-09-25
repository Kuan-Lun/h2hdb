"""Test the acceptance tool, including rejection of known production costs.

Passing these tests means the detector works, not that performance acceptance
passed. The manual CLI independently returns nonzero for observed violations.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from copy import deepcopy
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


@pytest.fixture(scope="module")
def acceptance() -> ModuleType:
    name = "ingest_database_performance_under_test"
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "check-ingest-database-performance.py"
    )
    spec = importlib.util.spec_from_file_location(name, script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    previous = list(sys.path)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = previous
    return module


def _turn(acceptance: ModuleType, *, calls: int = 1) -> dict[str, Any]:
    observer = acceptance.AcceptanceObserver()
    for phase in acceptance.PHASE_CEILINGS:
        observer.phase = phase
        for _ in range(calls):
            observer.record_sql_operation("sql", 0.001, "SELECT 1", 1)
    measurements = observer.report()
    for row in measurements["queries"]:
        if row["pipeline"] == "source":
            row.update(phase="commit", operation="FILE_PAGE")
    return {
        "selected": 1,
        "added": 1,
        "phases": dict.fromkeys(acceptance.PHASE_CEILINGS, 0.01),
        "cleanup": "DONE",
        "measurements": measurements,
    }


@pytest.mark.parametrize("pages", [0, 1, 63, 64, 65, 127, 128, 129, 255, 256, 257, 512])
def test_fixed_budgets_have_independent_boundaries(
    acceptance: ModuleType, pages: int
) -> None:
    files = pages + 1
    assert acceptance.file_page_budget(3, pages) == 3 * (
        8 * files + 256 * ((files + 127) // 128)
    )
    for multiplier in (1, 4):
        rows = multiplier * files
        expected = 2 * ((rows + 63) // 64) + 16 * ((rows + 255) // 256 + 1)
        assert acceptance.retirement_budget(rows) == expected
        assert (
            acceptance.check("boundary", expected, expected, "fixed")["status"]
            == "satisfied"
        )
        assert (
            acceptance.check("above", expected + 1, expected, "fixed")["status"]
            == "violated"
        )


def test_history_does_not_expand_budget_and_shared_slowdown_is_rejected(
    acceptance: ModuleType,
) -> None:
    small = _turn(acceptance)
    large = deepcopy(small)
    large["selected"] = 132046
    first = acceptance.assess_turn(small, pages=1)
    second = acceptance.assess_turn(large, pages=1)
    assert first["checks"] == second["checks"]
    assert first["status"] == "satisfied"
    degraded = acceptance.assess_turn(_turn(acceptance, calls=10000), pages=1)
    assert degraded["status"] == "violated"
    assert any(
        row["name"] == "claim.sql_calls" and row["status"] == "violated"
        for row in degraded["checks"]
    )


@pytest.mark.parametrize(
    "fault",
    [
        "missing_phase",
        "file_commit_missing",
        "negative_seconds",
        "nan_seconds",
        "truncated",
        "omitted",
    ],
)
def test_incomplete_measurements_cannot_pass(
    acceptance: ModuleType, fault: str
) -> None:
    turn = _turn(acceptance)
    measurements = turn["measurements"]
    if fault == "missing_phase":
        for row in measurements["queries"]:
            if row["pipeline"] == "cleanup":
                row["pipeline"] = "claim"
                row["operation"] = "second"
    elif fault == "file_commit_missing":
        for row in measurements["queries"]:
            if row["pipeline"] == "source":
                row["operation"] = "OTHER"
    elif fault == "negative_seconds":
        turn["phases"]["source"] = -1
    elif fault == "nan_seconds":
        turn["phases"]["source"] = float("nan")
    elif fault == "truncated":
        measurements["query_details_truncated"] = True
    else:
        measurements["omitted_query_events"] = 1
    with pytest.raises(ValueError):
        acceptance.assess_turn(turn, pages=1)


def test_exact_observer_rejects_overflow_and_lost_query_group(
    acceptance: ModuleType,
) -> None:
    observer = acceptance.AcceptanceObserver()
    observer.query_budget = 1
    observer.record_sql_operation("sql", 0.01, "SELECT 1", 1)
    observer.record_sql_operation("sql", 0.01, "SELECT 2", 1)
    with pytest.raises(RuntimeError, match="invalid"):
        observer.report()
    observer = acceptance.AcceptanceObserver()
    observer.record_sql_operation("sql", 0.01, "SELECT 1", 1)
    observer.queries.clear()
    with pytest.raises(ValueError, match="event ledger"):
        observer.report()


@pytest.mark.parametrize("value", ["", "1", "1:2:3", "-1:1", "513:1", "1:0", "1:9"])
def test_replacement_dimensions_are_bounded(acceptance: ModuleType, value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        acceptance.parse_replacement(value)


def test_missing_turn_or_retirement_acceptance_is_incomplete(
    acceptance: ModuleType,
) -> None:
    passed = {"status": "satisfied"}
    append: dict[str, Any] = {
        "kind": "append",
        "galleries": 2,
        "batch": 1,
        "turns": [
            {"selected": 1, "added": 1, "acceptance": passed},
            {"selected": 2, "added": 1},
        ],
    }
    assert acceptance.acceptance_status([append]) == "incomplete"
    replacement: dict[str, Any] = {
        "kind": "replacement",
        "replacement_cycles": 1,
        "turns": [
            {"cycle": 0, "acceptance": passed},
            {"cycle": 1, "acceptance": passed},
        ],
    }
    assert acceptance.acceptance_status([replacement]) == "incomplete"
    replacement["turns"][1]["retirement_acceptance"] = passed
    assert acceptance.acceptance_status([replacement]) == "satisfied"
    replacement["turns"].pop()
    assert acceptance.acceptance_status([replacement]) == "incomplete"


@pytest.mark.parametrize(
    ("status", "exit_code"), [("satisfied", 0), ("violated", 1), ("incomplete", 2)]
)
def test_cli_never_converts_violation_or_incomplete_to_success(
    acceptance: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    status: str,
    exit_code: int,
) -> None:
    output = tmp_path / "report.json"
    monkeypatch.setattr(
        sys, "argv", ["acceptance", "--case", "1:1:1", "--output", str(output)]
    )
    monkeypatch.setattr(acceptance.probe, "source_provenance", dict)
    monkeypatch.setattr(acceptance, "source_hashes", dict)
    monkeypatch.setattr(acceptance, "imported_sources", dict)
    monkeypatch.setattr(
        acceptance.batch_probe,
        "run_case",
        lambda *_args, **_kwargs: {
            "galleries": 1,
            "batch": 1,
            "turns": [_turn(acceptance)],
        },
    )
    monkeypatch.setattr(
        acceptance, "assess_turn", lambda *_args, **_kwargs: {"status": status}
    )
    assert acceptance.main() == exit_code
    report = json.loads(output.read_text())
    assert report["acceptance"]["status"] == status


def test_cli_requires_mariadb_opt_in(
    acceptance: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "acceptance",
            "--backend",
            "mariadb",
            "--case",
            "1:1:1",
            "--output",
            str(tmp_path / "r.json"),
        ],
    )
    with pytest.raises(SystemExit) as result:
        acceptance.main()
    assert result.value.code == 2


def test_provenance_rejects_already_imported_foreign_package(
    acceptance: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import h2hdb

    evidence = acceptance.imported_sources()
    assert evidence["h2hdb"]["path"] == str(Path(h2hdb.__file__).resolve())
    assert len(evidence["h2hdb"]["sha256"]) == 64
    monkeypatch.setattr(h2hdb, "__file__", str(tmp_path / "h2hdb" / "__init__.py"))
    with pytest.raises(RuntimeError, match="not from the measured checkout"):
        acceptance.imported_sources()


def test_progress_never_duplicates_full_query_details(
    acceptance: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    acceptance.print_progress(
        {
            "event": "turn_completed",
            "selected": 3,
            "added": 1,
            "cleanup": "DONE",
            "sql_calls": 123,
            "sql_seconds": 0.2,
            "measurements": {"queries": [{"sql": "SELECT private_evidence"}]},
        }
    )
    result = json.loads(capsys.readouterr().out)
    assert result == {
        "event": "turn_completed",
        "selected": 3,
        "added": 1,
        "cleanup": "DONE",
        "sql_calls": 123,
        "sql_seconds": 0.2,
    }


@pytest.mark.deep
def test_real_metadata_only_replacement_retires_exact_facts(
    acceptance: ModuleType,
) -> None:
    result = acceptance.run_replacement("sqlite", 0, 1)
    assert len(result["turns"]) == 2
    for turn in result["turns"]:
        assert turn["cleanup"] == "DONE"
        assert turn["full_ready_audit"] == "passed"
        assert turn["next_claim"] == "passed"
    retired = result["turns"][1]["retirement_acceptance"]["checks"]
    assert [row["retired_rows"] for row in retired] == [1, 4, 1]


@pytest.mark.deep
def test_real_replacement_detects_current_scalar_cleanup_cost(
    acceptance: ModuleType,
) -> None:
    result = acceptance.run_replacement("sqlite", 64, 3)
    assert len(result["turns"]) == 4
    for turn in result["turns"]:
        assert turn["cleanup"] == "DONE"
        assert turn["full_ready_audit"] == "passed"
        assert turn["next_claim"] == "passed"
        if turn["cycle"]:
            assert turn["retirement_acceptance"]["status"] == "violated"
            assert all(
                row["retired_rows"] == row["expected_retired_rows"]
                for row in turn["retirement_acceptance"]["checks"]
            )


@pytest.mark.deep
def test_real_redundant_read_mutant_preserves_oracle_but_fails_fixed_cost(
    acceptance: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2hdb.sql_performance import _MeasuredConnector

    original = _MeasuredConnector.fetch_one

    def repeated_read(
        self: _MeasuredConnector, query: str, data: tuple[Any, ...] = ()
    ) -> tuple[Any, ...]:
        result = original(self, query, data)
        # Read-only constant work cannot change catalog facts or consume rows.
        # Every extra call traverses the actual instrumented connector.
        for _ in range(32):
            original(self, "SELECT 1")
        return result

    monkeypatch.setattr(_MeasuredConnector, "fetch_one", repeated_read)
    result = acceptance.batch_probe.run_case(
        "sqlite",
        1,
        1,
        1,
        query_limit=None,
        observer_factory=acceptance.AcceptanceObserver,
        check_next_claim=True,
    )
    assert result["full_ready_audit"] == "passed"
    assert result["turns"][0]["next_claim"] == "passed"
    assert acceptance.assess_turn(result["turns"][0], pages=1)["status"] == "violated"
