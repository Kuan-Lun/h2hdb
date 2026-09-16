from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import Mock

import pytest


@pytest.fixture
def collector() -> ModuleType:
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "collect_mariadb_performance.py"
    )
    spec = importlib.util.spec_from_file_location("collect_mariadb_performance", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def sample(module: ModuleType, count: int, *, digest: str = "abc") -> dict[str, Any]:
    return {
        "global_status": {
            **dict.fromkeys(module.STATUS_NAMES, count),
            "Uptime": count + 100,
        },
        "digest_status": "available",
        "consumers": {
            name: "YES"
            for name in (
                "global_instrumentation",
                "thread_instrumentation",
                "statements_digest",
            )
        },
        "statement_instruments": [{"enabled": "YES", "timed": "YES", "count": 100}],
        "global_digest_overflow_events": 0,
        "digests": {
            digest: {
                "normalized_sql_prefix": "SELECT ?",
                **dict.fromkeys(module.COUNTERS, count),
            }
        },
    }


def test_differences_do_not_sum_cumulative_snapshots(collector: ModuleType) -> None:
    before, after = sample(collector, 2), sample(collector, 5)
    before["digests"]["abc"]["SUM_TIMER_WAIT"] = 1_000_000_000_000
    after["digests"]["abc"]["SUM_TIMER_WAIT"] = 2_500_000_000_000
    result = collector.differences(before, after)
    assert result["complete_counter_comparison"] is True
    assert result["digest_deltas"][0]["COUNT_STAR"] == 3
    assert result["digest_deltas"][0]["server_elapsed_seconds"] == 1.5
    assert result["global_status_delta"]["Questions"] == 3


@pytest.mark.parametrize(
    "condition",
    [
        "reset",
        "missing",
        "truncated",
        "disabled",
        "overflow",
        "untimed",
        "consumer",
        "restart",
        "missing_uptime",
    ],
)
def test_incomplete_evidence_is_not_a_zero_cost_claim(
    collector: ModuleType, condition: str
) -> None:
    before, after = sample(collector, 2), sample(collector, 5)
    if condition == "reset":
        after["digests"]["abc"]["COUNT_STAR"] = 1
    elif condition == "missing":
        after["digests"] = {}
    elif condition in {"truncated", "disabled"}:
        after["digest_status"] = condition
    elif condition == "overflow":
        after["global_digest_overflow_events"] = 100
    elif condition == "untimed":
        after["statement_instruments"][0]["timed"] = "NO"
    elif condition == "consumer":
        after["consumers"]["statements_digest"] = "NO"
    elif condition == "restart":
        after["global_status"]["Uptime"] = 1
    else:
        del after["global_status"]["Uptime"]
    result = collector.differences(before, after)
    assert result["complete_counter_comparison"] is False
    assert result["limitations_detected"]


def test_new_digest_counts_start_at_zero(collector: ModuleType) -> None:
    before, after = sample(collector, 2), sample(collector, 5)
    after["digests"]["new"] = {
        "normalized_sql_prefix": "SELECT x",
        **dict.fromkeys(collector.COUNTERS, 7),
    }
    result = collector.differences(before, after)
    found = {row["digest"]: row for row in result["digest_deltas"]}
    assert found["new"]["COUNT_STAR"] == 7


def test_same_missing_global_counter_in_both_snapshots_is_incomplete(
    collector: ModuleType,
) -> None:
    before, after = sample(collector, 2), sample(collector, 5)
    for snapshot_result in (before, after):
        del snapshot_result["global_status"]["Innodb_row_lock_time"]
    result = collector.differences(before, after)
    assert result["complete_counter_comparison"] is False
    assert any(
        "Innodb_row_lock_time" in problem for problem in result["limitations_detected"]
    )


def test_disabled_performance_schema_skips_digest_queries(
    collector: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    fetch = Mock(side_effect=[[("Uptime", "5")], [(0,)]])
    monkeypatch.setattr(collector, "query", fetch)
    result = collector.snapshot(Mock(), "private_schema")
    assert result["digest_status"] == "disabled"
    assert fetch.call_count == 2


def test_digest_permission_error_preserves_other_evidence_without_secret(
    collector: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    fetch = Mock(
        side_effect=[[("Uptime", "5")], [(1,)], RuntimeError("password=SECRET")]
    )
    monkeypatch.setattr(collector, "query", fetch)
    result = collector.snapshot(Mock(), "private_schema")
    assert result["digest_status"] == "unavailable"
    assert result["global_status"] == {"Uptime": 5}
    assert "SECRET" not in json.dumps(result)


def test_connection_has_no_default_schema_and_sets_driver_timeouts(
    collector: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "ingest.json"
    path.write_text(
        json.dumps(
            {
                "core": {
                    "database": {
                        "sql_type": "mariadb",
                        "host": "example.invalid",
                        "database": "catalog",
                        "password": "secret",
                    }
                },
                "paths": {},
            }
        )
    )
    config = collector.load_database(path, "ingest")
    connect = Mock()
    monkeypatch.setattr(collector, "connect", connect)
    collector.open_connection(config)
    assert "database" not in connect.call_args.kwargs
    assert connect.call_args.kwargs["read_timeout"] == 10
    assert connect.call_args.kwargs["connection_timeout"] == 5
    assert connect.call_args.kwargs["autocommit"] is True


def test_atomic_report_never_overwrites_existing_path(
    collector: ModuleType, tmp_path: Path
) -> None:
    output = tmp_path / "report.json"
    output.write_text("preserve")
    with pytest.raises(FileExistsError):
        collector.write_report(output, {"status": "collected"})
    assert output.read_text() == "preserve"
    assert list(tmp_path.iterdir()) == [output]


def test_collect_only_changes_its_own_session_and_closes(
    collector: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = Mock()
    cursor = Mock()
    connection.cursor.return_value.__enter__ = Mock(return_value=cursor)
    connection.cursor.return_value.__exit__ = Mock()
    monkeypatch.setattr(collector, "open_connection", Mock(return_value=connection))
    fetch = Mock(side_effect=[[("10.11.11-MariaDB",)]] + [[(1,)]] * 20)
    monkeypatch.setattr(collector, "query", fetch)
    monkeypatch.setattr(
        collector,
        "snapshot",
        Mock(side_effect=[sample(collector, 2), sample(collector, 3)]),
    )
    monkeypatch.setattr(collector.time, "sleep", Mock())
    config = Mock()
    config.database.database = "catalog"
    report = collector.collect(config, 1)
    assert report["status"] == "collected"
    cursor.execute.assert_called_once_with("SET SESSION TRANSACTION READ ONLY")
    connection.close.assert_called_once()
    assert len(report["select_one_client_seconds"]["samples"]) == 20
    assert {call.args[1] for call in fetch.call_args_list} == {
        "SELECT VERSION()",
        "SELECT 1",
    }


@pytest.mark.parametrize("seconds", [0, 601, float("nan")])
def test_invalid_window_does_not_connect(
    collector: ModuleType, monkeypatch: pytest.MonkeyPatch, seconds: float
) -> None:
    connect = Mock()
    monkeypatch.setattr(collector, "open_connection", connect)
    with pytest.raises(ValueError):
        collector.collect(Mock(), seconds)
    connect.assert_not_called()
