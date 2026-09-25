"""Exact local attribution rejects the production first-64 counterexample."""

from __future__ import annotations

import importlib.util
import json
import logging
import sys
from collections.abc import Iterator
from copy import deepcopy
from functools import partial
from hashlib import sha256
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from h2hdb.ingest_performance import IngestPerformance
from h2hdb.sql_performance import (
    instrument_connector,
    measure_sql,
    query_totals_snapshot,
)
from h2hdb.sqlite_connector import SQLiteConnector

_LATE_QUERY = "SELECT %s AS private_late_family"
_LATE_FINGERPRINT = sha256(_LATE_QUERY.encode()).hexdigest()[:16]


@pytest.fixture
def attribution() -> Iterator[ModuleType]:
    yield from _load_script("performance_attribution")


@pytest.fixture
def probe() -> Iterator[ModuleType]:
    yield from _load_script("ingest_pipeline_probe")


def _load_script(name: str) -> Iterator[ModuleType]:
    key = f"{name}_attribution_under_test"
    path = Path(__file__).resolve().parents[1] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(key, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    previous_path = list(sys.path)
    sys.modules[key] = module
    try:
        spec.loader.exec_module(module)
        yield module
    finally:
        sys.path[:] = previous_path
        sys.modules.pop(key, None)


@pytest.mark.parametrize("prefix_families", [63, 64, 65, 130])
@pytest.mark.parametrize("cross_step", [False, True])
def test_real_sqlite_late_cumulative_cost_survives_capacity_and_cycles(
    attribution: ModuleType,
    probe: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    prefix_families: int,
    cross_step: bool,
) -> None:
    """Calls/rows and synthetic seconds are exact, not a latency benchmark.

    Warm-up families each cost one second. A later family costs only 0.25s per
    call, but its three 1000-call cycles dominate cumulative time. Thus it never
    enters the five slowest-call heap. The production recorder is an intentional
    degraded control once its first 64 identities have been admitted.
    """

    now = [0.0]
    clock = lambda: now[0]  # noqa: E731 - shared injectable diagnostic clock.
    fetch = SQLiteConnector.fetch_one

    def delayed_fetch(
        self: SQLiteConnector, query: str, data: tuple[Any, ...] = ()
    ) -> tuple[Any, ...]:
        result = fetch(self, query, data)
        now[0] += 0.25 if query == _LATE_QUERY else 1.0
        return result

    monkeypatch.setattr(SQLiteConnector, "fetch_one", delayed_fetch)
    monkeypatch.setattr(probe, "measure_sql", partial(measure_sql, clock=clock))
    monkeypatch.setattr(probe.time, "perf_counter", clock)
    observer = probe.Observer()
    performance = IngestPerformance(
        logging.getLogger("attribution.acceptance"), backend="sqlite", clock=clock
    )
    with (
        observer.installed(),
        instrument_connector(
            SQLiteConnector(str(tmp_path / "observed.db"))
        ) as connector,
    ):

        def warmup() -> None:
            for index in range(prefix_families):
                assert connector.fetch_one(
                    f"SELECT %s AS prefix_{index}", ("private-value",)
                ) == ("private-value",)

        def cycle() -> None:
            for _ in range(1000):
                assert connector.fetch_one(_LATE_QUERY, ("private-value",)) == (
                    "private-value",
                )

        if cross_step:
            with performance.step("source", "COLLECTION_FREEZE.issue", "SOURCE", 1):
                warmup()
            for _ in range(3):
                with performance.step("source", "FILE_PAGE.commit", "SOURCE", 1):
                    cycle()
        else:
            with performance.step("source", "FILE_PAGE.commit", "SOURCE", 1):
                warmup()
                for _ in range(3):
                    cycle()
    stage = performance._stage
    assert stage is not None
    exact = attribution.assess_observer_report(observer.report)
    assert exact["schema_version"] == 1
    assert exact["status"] == "complete"
    assert exact["sql_calls"] == exact["returned_rows"] == prefix_families + 3000
    assert exact["sql_seconds"] == prefix_families + 750.0
    assert exact["fingerprint_count"] == prefix_families + 1
    assert exact["families"][0] == {
        "fingerprint": _LATE_FINGERPRINT,
        "calls": 3000,
        "seconds": 750.0,
        "returned_rows": 3000,
        "max_seconds": 0.25,
    }
    assert sum(row["sql_calls"] for row in exact["phases"]) == exact["sql_calls"]
    assert sum(row["sql_seconds"] for row in exact["phases"]) == exact["sql_seconds"]
    assert (
        sum(row["returned_rows"] for row in exact["phases"]) == exact["returned_rows"]
    )
    assert len(exact["phases"]) == (2 if cross_step else 1)
    output = json.dumps(exact)
    assert "SELECT" not in output
    assert "private" not in output
    assert stage.counters.sql_calls == exact["sql_calls"]
    assert stage.counters.sql_seconds == exact["sql_seconds"]
    assert _LATE_FINGERPRINT not in {
        row["fingerprint"] for row in stage.slowest.snapshot()
    }
    if prefix_families >= 64:
        assert _LATE_FINGERPRINT not in stage.queries
        assert stage.queries["other"].seconds >= 750.0
        assert (
            attribution.assess_attribution(
                {
                    "sql_calls": stage.counters.sql_calls,
                    "sql_seconds": stage.counters.sql_seconds,
                    "returned_rows": stage.counters.read_rows,
                    "query_top": query_totals_snapshot(stage.queries),
                    "query_overflow": {"seconds": stage.queries["other"].seconds},
                }
            )["status"]
            == "incomplete"
        )
    else:
        assert stage.queries[_LATE_FINGERPRINT].seconds == 750.0
    performance.close()


def _report() -> dict[str, Any]:
    query = "SELECT 'private-literal' AS private_alias"
    return {
        "sql_calls": 3,
        "sql_seconds": 0.3,
        "returned_rows": 3,
        "query_group_count": 1,
        "query_group_budget": 8192,
        "query_details_truncated": False,
        "omitted_query_events": 0,
        "queries": [
            {
                "pipeline": "source",
                "phase": "commit",
                "operation": "FILE_PAGE",
                "category": "sql",
                "sql": query,
                "fingerprint": sha256(query.encode()).hexdigest()[:16],
                "calls": 3,
                "seconds": 0.1 + 0.2,
                "returned_rows": 3,
                "max_seconds": 0.2,
            }
        ],
    }


@pytest.mark.parametrize(
    "field,value",
    [
        ("query_details_truncated", True),
        ("omitted_query_events", 1),
        ("omitted_query_events", False),
        ("query_group_count", 2),
        ("query_group_budget", 0),
        ("sql_calls", -1),
        ("sql_calls", 4),
        ("returned_rows", 4),
        ("sql_seconds", float("nan")),
        ("sql_seconds", float("inf")),
        ("sql_seconds", -0.1),
        ("sql_seconds", 0.31),
        ("sql_seconds", True),
        ("sql_seconds", 10**1000),
    ],
)
def test_missing_or_invalid_evidence_is_incomplete(
    attribution: ModuleType, field: str, value: object
) -> None:
    report = _report()
    report[field] = value
    assert attribution.assess_attribution(report)["status"] == "incomplete"
    report = _report()
    del report[field]
    assert attribution.assess_attribution(report)["status"] == "incomplete"


@pytest.mark.parametrize(
    "field,value",
    [
        ("category", "unknown"),
        ("fingerprint", "other"),
        ("fingerprint", "0" * 16),
        ("calls", True),
        ("calls", 0),
        ("returned_rows", -1),
        ("seconds", float("nan")),
        ("max_seconds", -1),
        ("max_seconds", 1.0),
        ("sql", None),
    ],
)
def test_invalid_groups_cannot_be_accepted_as_complete(
    attribution: ModuleType, field: str, value: object
) -> None:
    report = _report()
    report["queries"][0][field] = value
    assert attribution.assess_attribution(report)["status"] == "incomplete"


def test_query_details_not_displayed_remain_usable_if_full_report_is_available(
    attribution: ModuleType, probe: ModuleType, tmp_path: Path
) -> None:
    observer = probe.Observer()
    with (
        observer.installed(),
        instrument_connector(SQLiteConnector(str(tmp_path / "full.db"))) as connector,
    ):
        assert connector.fetch_one("SELECT %s", ("private-value",)) == (
            "private-value",
        )
    assert (
        attribution.assess_attribution(observer.report(query_limit=0))["status"]
        == "incomplete"
    )
    assert attribution.assess_observer_report(observer.report)["status"] == "complete"


def test_overflow_and_unfinished_scopes_fail_closed(
    attribution: ModuleType, probe: ModuleType, tmp_path: Path
) -> None:
    observer = probe.Observer(query_budget=1)
    with (
        observer.installed(),
        instrument_connector(
            SQLiteConnector(str(tmp_path / "overflow.db"))
        ) as connector,
    ):
        assert connector.fetch_one("SELECT 1") == (1,)
        assert connector.fetch_one("SELECT 2") == (2,)
    assert observer.failure is not None
    assert attribution.assess_observer_report(observer.report) == {
        "schema_version": 1,
        "status": "incomplete",
        "reasons": ["observer_report_unavailable"],
    }
    unfinished = probe.Observer()
    unfinished.pending[1] = {}
    assert (
        attribution.assess_observer_report(unfinished.report)["status"] == "incomplete"
    )


def test_duplicate_or_missing_rows_and_empty_measurement_are_not_complete(
    attribution: ModuleType,
) -> None:
    report = _report()
    report["queries"].append(deepcopy(report["queries"][0]))
    report["query_group_count"] = 2
    assert attribution.assess_attribution(report)["reasons"] == [
        "duplicate_query_group"
    ]
    report = _report()
    report["queries"] = []
    report["query_group_count"] = 0
    assert attribution.assess_attribution(report)["reasons"] == [
        "sql_totals_not_conserved"
    ]
    report.update(sql_calls=0, sql_seconds=0.0, returned_rows=0)
    assert attribution.assess_attribution(report)["reasons"] == ["no_sql_observed"]


def test_finite_groups_with_overflowing_aggregate_are_incomplete(
    attribution: ModuleType,
) -> None:
    report = _report()
    first = report["queries"][0]
    first.update(seconds=1e308, max_seconds=1e308)
    second = deepcopy(first)
    second["phase"] = "prepare"
    report["queries"].append(second)
    report.update(query_group_count=2, sql_calls=6, returned_rows=6, sql_seconds=1e308)
    assert attribution.assess_attribution(report)["reasons"] == [
        "invalid_sql_aggregate"
    ]


def test_cli_is_private_and_incomplete_exit_is_nonzero(
    attribution: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "report.json"
    path.write_text(json.dumps(_report()))
    assert attribution.main([str(path)]) == 0
    output = capsys.readouterr().out
    assert json.loads(output)["status"] == "complete"
    assert "private" not in output and "SELECT" not in output
    path.write_text('{"sql_calls": 3}')
    assert attribution.main([str(path)]) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "incomplete"
    path.write_text("private invalid json")
    assert attribution.main([str(path)]) == 2
    output = capsys.readouterr().out
    assert "private" not in output
    assert json.loads(output)["status"] == "incomplete"
