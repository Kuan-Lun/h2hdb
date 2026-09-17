"""Verify the manual observer catches hidden, repeated, and slow physical SQL."""

from __future__ import annotations

import importlib.util
import logging
import sys
import time
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest

from h2hdb.ingest_performance import IngestPerformance
from h2hdb.sql_performance import instrument_connector
from h2hdb.sqlite_connector import SQLiteConnector


@pytest.fixture
def probe() -> ModuleType:
    name = "maintenance_performance_investigation_under_test"
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "investigate-maintenance-performance.py"
    )
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    original_path = sys.path[:]
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = original_path
    return module


def test_matrix_covers_row_windows_and_raw_lengths_without_conflating_dimensions(
    probe: ModuleType,
) -> None:
    cases = probe.matrix("all")
    assert len({case.name for case in cases}) == len(cases)
    assert [case.shape.pages for case in probe.matrix("pages")] == [127, 128, 129]
    assert [case.shape.metadata_bytes for case in probe.matrix("canonical")] == [
        32767,
        32768,
        32769,
        65535,
        65536,
        65537,
    ]
    for case in (*probe.matrix("pages"), *probe.matrix("canonical")):
        assert case.shape.galleries == 2
        assert case.shape.tags == 0
    assert probe.matrix("retention")[0].revisions > 2
    with pytest.raises(ValueError, match="unknown matrix"):
        probe.matrix("remote")


def test_nested_telemetry_cannot_hide_or_double_count_physical_sql(
    probe: ModuleType, tmp_path: Path
) -> None:
    telemetry = IngestPerformance(
        logging.getLogger("h2hdb.probe-test"), backend="sqlite"
    )

    def action() -> None:
        with instrument_connector(
            SQLiteConnector(str(tmp_path / "nested.db"))
        ) as connector:
            assert connector.fetch_one("SELECT %s", (1,)) == (1,)
            with telemetry.step("analysis", "prepare", "parent", 1):
                assert connector.fetch_one("SELECT %s", (2,)) == (2,)
                with telemetry.step("analysis", "prepare", "child", 1):
                    assert connector.fetch_one("SELECT %s", (3,)) == (3,)

    _, result = probe.measured(action)
    assert result["sql_calls"] == result["returned_rows"] == 3
    rows = [row for row in result["queries"] if row["category"] == "sql"]
    assert {row["operation"] for row in rows} == {"outside", "parent", "child"}
    assert {row["sql"] for row in rows} == {"SELECT %s"}
    assert all("parameters" not in row for row in rows)


def test_added_n_plus_one_sql_is_rejected_by_the_same_cost_budget(
    probe: ModuleType, tmp_path: Path
) -> None:
    def run(redundant: int) -> dict[str, Any]:
        def action() -> None:
            with instrument_connector(
                SQLiteConnector(str(tmp_path / f"n{redundant}.db"))
            ) as connector:
                assert connector.fetch_all("SELECT %s UNION ALL SELECT %s", (1, 2)) == [
                    (1,),
                    (2,),
                ]
                for index in range(redundant):
                    connector.fetch_one("SELECT %s", (index,))

        return cast(dict[str, Any], probe.measured(action)[1])

    def require_single_batch(result: dict[str, Any]) -> None:
        assert result["sql_calls"] <= 1, "redundant per-row SQL exceeds batch cost"

    baseline, degraded = run(0), run(2)
    require_single_batch(baseline)
    with pytest.raises(AssertionError, match="redundant per-row"):
        require_single_batch(degraded)
    assert degraded["sql_calls"] - baseline["sql_calls"] == 2


def test_known_query_delay_is_attributed_to_the_delayed_fingerprint(
    probe: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = SQLiteConnector.fetch_one

    def delayed(
        self: SQLiteConnector, query: str, data: tuple[Any, ...] = ()
    ) -> tuple[Any, ...]:
        if query == "SELECT 42":
            time.sleep(0.02)
        return original(self, query, data)

    monkeypatch.setattr(SQLiteConnector, "fetch_one", delayed)

    def action() -> None:
        with instrument_connector(
            SQLiteConnector(str(tmp_path / "delay.db"))
        ) as connector:
            connector.fetch_one("SELECT 41")
            connector.fetch_one("SELECT 42")

    _, result = probe.measured(action)
    by_sql = {row["sql"]: row for row in result["queries"] if row["category"] == "sql"}
    # sleep requests at least 20 ms; allow 5 ms margin for timer resolution.
    assert by_sql["SELECT 42"]["seconds"] >= 0.015
    assert by_sql["SELECT 42"]["calls"] == 1
    assert result["sql_calls"] == 2


def test_budget_overflow_and_exception_restore_physical_observer(
    probe: ModuleType, tmp_path: Path
) -> None:
    original = probe._MeasuredConnector._call
    with SQLiteConnector(str(tmp_path / "overflow.db")) as raw:
        with pytest.raises(RuntimeError, match="budget exceeded"):
            with probe.PhysicalObserver(budget=2).installed():
                connector = instrument_connector(raw)
                connector.fetch_one("SELECT 1")
                connector.fetch_one("SELECT 2")
                connector.fetch_one("SELECT 3")
    assert probe._MeasuredConnector._call is original


def test_production_phase_counters_match_independent_calls_and_reject_corruption(
    probe: ModuleType, tmp_path: Path
) -> None:
    from h2hdb.database_performance import DatabasePerformance, database_phase

    telemetry = DatabasePerformance(
        logging.getLogger("h2hdb.probe-telemetry"),
        backend="sqlite",
        level=logging.DEBUG,
    )

    def action() -> None:
        with telemetry.operation("test_audit"):
            with instrument_connector(
                SQLiteConnector(str(tmp_path / "phases.db"))
            ) as connector:
                connector.fetch_one("SELECT 1")
                with database_phase("validator"):
                    connector.fetch_one("SELECT 2")

    _, result = probe.measured(action)
    probe.verify_diagnostic_counters(result, required=True)
    assert result["diagnostic_counter_check"] == "passed"
    phases = [event for event in result["database_events"] if event["event"] == "phase"]
    assert len(phases) == 1 and phases[0]["sql_calls"] == 1
    result["sql_calls"] += 1
    with pytest.raises(RuntimeError, match="disagree"):
        probe.verify_diagnostic_counters(result, required=True)


def test_query_dictionary_is_lossless_and_cost_repeat_regression_is_rejected(
    probe: ModuleType,
) -> None:
    dictionary: dict[str, str] = {}
    rows = [{"fingerprint": "a", "sql": "SELECT 1", "calls": n} for n in (1, 3)]
    probe.compact_query_texts(rows, dictionary)
    assert dictionary == {"a": "SELECT 1"}
    assert [row["calls"] for row in rows] == [1, 3]
    assert all("sql" not in row for row in rows)
    with pytest.raises(RuntimeError, match="fingerprint collision"):
        probe.compact_query_texts({"fingerprint": "a", "sql": "SELECT 2"}, dictionary)
    baseline = {"name": "same", "revisions": [{"source": {"sql_calls": 7}}]}
    probe.verify_repeat_counts([baseline, baseline])
    degraded = {"name": "same", "revisions": [{"source": {"sql_calls": 8}}]}
    with pytest.raises(RuntimeError, match="different SQL counts"):
        probe.verify_repeat_counts([baseline, degraded])


def test_info_quiet_readonly_done_is_explicit_not_a_fake_counter_match(
    probe: ModuleType, tmp_path: Path
) -> None:
    from h2hdb.database_performance import DatabasePerformance

    telemetry = DatabasePerformance(
        logging.getLogger("h2hdb.probe-quiet"), backend="sqlite", level=logging.INFO
    )

    def action() -> None:
        with telemetry.operation("current_only_cleanup") as span:
            with instrument_connector(
                SQLiteConnector(str(tmp_path / "quiet.db"))
            ) as connector:
                connector.fetch_one("SELECT 1")
            span.describe(quiet=True)

    _, result = probe.measured(action)
    result["outcome"] = "DONE"
    probe.verify_diagnostic_counters(result, required=True, allow_quiet=True)
    assert result["diagnostic_counter_check"] == "quiet_readonly_DONE_at_INFO"
    result["outcome"] = "PROGRESSED"
    with pytest.raises(RuntimeError, match="read-only DONE"):
        probe.verify_diagnostic_counters(result, required=True, allow_quiet=True)
