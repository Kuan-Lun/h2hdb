"""Verify the manual observer catches hidden, repeated, and slow physical SQL."""

from __future__ import annotations

import importlib.util
import logging
import sys
import time
from contextlib import closing
from pathlib import Path
from types import ModuleType
from typing import Any, cast
from unittest.mock import Mock

import pytest
from vnext_probe_databases import observe_probe_claims
from vnext_test_database import (
    DatabaseFactory,
    database_connector,
    inspect_one,
    open_database,
)

from h2hdb import VNextIngestFacade
from h2hdb.ingest_performance import IngestPerformance
from h2hdb.sql_connector import SQLConnector
from h2hdb.sql_performance import instrument_connector


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
    probe: ModuleType,
    tmp_path: Path,
    database_factory: DatabaseFactory,
) -> None:
    telemetry = IngestPerformance(
        logging.getLogger("h2hdb.probe-test"), backend=database_factory.backend
    )

    def action() -> None:
        with instrument_connector(
            database_connector(database_factory.config("nested.db"))
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
    probe: ModuleType,
    tmp_path: Path,
    database_factory: DatabaseFactory,
) -> None:
    def run(redundant: int) -> dict[str, Any]:
        def action() -> None:
            with instrument_connector(
                database_connector(database_factory.config(f"n{redundant}.db"))
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


def test_source_batch_aggregation_keeps_independent_action_cost_labels(
    probe: ModuleType,
    tmp_path: Path,
    database_factory: DatabaseFactory,
) -> None:
    telemetry = IngestPerformance(
        logging.getLogger("h2hdb.probe-source"), backend=database_factory.backend
    )

    def action() -> None:
        with instrument_connector(
            database_connector(database_factory.config("source-phases.db"))
        ) as connector:
            for component in ("FILE_PAGE", "TAG_PAGE"):
                for phase in ("issue", "prepare", "commit"):
                    with telemetry.step("source", f"{component}.{phase}", "SOURCE", 1):
                        connector.fetch_one("SELECT %s", (1,))

    _, result = probe.measured(action)
    assert result["sql_calls"] == result["returned_rows"] == 6
    sql = [item for item in result["queries"] if item["category"] == "sql"]
    assert {item["operation"] for item in sql} == {
        f"source.{phase}:{component}"
        for component in ("FILE_PAGE", "TAG_PAGE")
        for phase in ("issue", "prepare", "commit")
    }
    assert all(item["calls"] == 1 for item in sql)


def test_known_query_delay_is_attributed_to_the_delayed_fingerprint(
    probe: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    database_factory: DatabaseFactory,
) -> None:
    connector_type = type(database_connector(database_factory.config("delay.db")))
    original = connector_type.fetch_one

    def delayed(
        self: SQLConnector, query: str, data: tuple[Any, ...] = ()
    ) -> tuple[Any, ...]:
        if query == "SELECT 42":
            time.sleep(0.02)
        return original(self, query, data)

    monkeypatch.setattr(connector_type, "fetch_one", delayed)

    def action() -> None:
        with instrument_connector(
            database_connector(database_factory.config("delay.db"))
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
    probe: ModuleType,
    tmp_path: Path,
    database_factory: DatabaseFactory,
) -> None:
    original = probe._MeasuredConnector._call
    with database_connector(database_factory.config("overflow.db")) as raw:
        with pytest.raises(RuntimeError, match="budget exceeded"):
            with probe.PhysicalObserver(budget=2).installed():
                connector = instrument_connector(raw)
                connector.fetch_one("SELECT 1")
                connector.fetch_one("SELECT 2")
                connector.fetch_one("SELECT 3")
    assert probe._MeasuredConnector._call is original


def test_production_phase_counters_match_independent_calls_and_reject_corruption(
    probe: ModuleType,
    tmp_path: Path,
    database_factory: DatabaseFactory,
) -> None:
    from h2hdb.database_performance import DatabasePerformance, database_phase

    telemetry = DatabasePerformance(
        logging.getLogger("h2hdb.probe-telemetry"),
        backend=database_factory.backend,
        level=logging.DEBUG,
    )

    def action() -> None:
        with telemetry.operation("test_audit"):
            with instrument_connector(
                database_connector(database_factory.config("phases.db"))
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
    probe: ModuleType,
    tmp_path: Path,
    database_factory: DatabaseFactory,
) -> None:
    from h2hdb.database_performance import DatabasePerformance

    telemetry = DatabasePerformance(
        logging.getLogger("h2hdb.probe-quiet"),
        backend=database_factory.backend,
        level=logging.INFO,
    )

    def action() -> None:
        with telemetry.operation("current_only_cleanup") as span:
            with instrument_connector(
                database_connector(database_factory.config("quiet.db"))
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


def test_direct_successor_claim_never_drains_or_retries(
    probe: ModuleType,
) -> None:
    facade = Mock(spec=VNextIngestFacade)
    facade.try_claim_ingest.return_value = None
    with pytest.raises(RuntimeError, match="next ingest claim after DONE was refused"):
        probe.require_direct_claim(facade)
    assert facade.method_calls == [
        ("try_claim_ingest", (True, probe.LEASE_MICROSECONDS), {})
    ]


@pytest.mark.deep
@pytest.mark.parametrize("case_name,revisions", [("baseline", 2), ("depth", 18)])
def test_real_revision_claims_preserve_natural_generations_and_compaction(
    probe: ModuleType,
    database_factory: DatabaseFactory,
    monkeypatch: pytest.MonkeyPatch,
    case_name: str,
    revisions: int,
) -> None:
    config = database_factory.config("natural-revisions")
    claims = observe_probe_claims(monkeypatch)
    result = probe.run_case(
        config,
        probe.Case(case_name, probe.Shape(galleries=1, pages=1, tags=0), revisions),
        "info",
    )
    expected = list(range(1, revisions + 2))
    assert result["measurement_protocol"] == "consecutive-work-generations-v1"
    assert claims == expected
    records = result["revisions"]
    assert [record["ingest_generation"] for record in records] == expected[:-1]
    assert [
        record["next_claim"]["ingest_generation"] for record in records
    ] == expected[1:]
    for ordinal, record in enumerate(records, start=1):
        assert record["publication_oracle"]["verified"]
        assert record["cleanup"]["outcome"] == "DONE"
        assert record["next_claim"]["granted"]
        assert "post_claim_cleanup" not in record
        assert len(record["audits"]) == 3
        assert {audit["sql_calls"] for audit in record["audits"]} == {
            record["audits"][0]["sql_calls"]
        }
        queries = [
            query for query in record["claim"]["queries"] if query["category"] == "sql"
        ]
        assert queries
        assert not any(
            query["sql"].lstrip().upper().startswith("DELETE") for query in queries
        )
        if ordinal < revisions:
            assert record["next_claim"]["kind"] == "next_revision"
            assert record["next_claim"]["measurement_revision"] == ordinal + 1
    if case_name == "depth":
        assert [record["overlay_depth"] for record in records] == [*range(17), 0]
        assert records[16]["ingest_generation"] == 17
        assert records[17]["ingest_generation"] == 18
    assert result["final_full_ready_audit"]["diagnostic_counter_check"] == "passed"
    assert result["final_full_ready_audit_scope"] == (
        "after_final_cleanup_before_final_claim_probe"
    )
    final = result["post_measurement_next_claim"]
    assert final["ingest_generation"] == expected[-1]
    assert final["granted"] and final["completed"] and final["state_changed"]
    assert not final["included_in_revision_costs"]
    assert (
        final["cleanup_after_probe"]
        == final["ready_audit_after_probe"]
        == "not_checked"
    )
    assert records[-1]["next_claim"]["kind"] == "post_measurement_probe"
    with closing(open_database(config)) as connector:
        assert inspect_one(
            connector,
            "SELECT current_generation, completed_generation, phase "
            "FROM operational_ingest_coordination_heads WHERE singleton_id = 1",
        ) == (expected[-1], expected[-1], "READY")
