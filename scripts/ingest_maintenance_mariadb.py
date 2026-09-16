"""Read-only diagnostics for the growth probe's private MariaDB fixture.

This is not a database CLI: callers cannot supply SQL or server credentials.
The owning probe captures trusted runtime candidate SELECTs from its synthetic
Testcontainer, then replays them separately from baseline measurements.
"""

from __future__ import annotations

import json
import statistics
import time
from collections.abc import Callable
from hashlib import sha256
from typing import Any
from unittest.mock import patch

from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.sql_connector import SQLConnector
from h2hdb.sql_performance import _MeasuredConnector

HANDLER_STATUS = (
    "SHOW SESSION STATUS WHERE Variable_name LIKE 'Handler_read_%' "
    "AND Variable_name <> 'Handler_read_retry'"
)
LOCK_STATUS = "SHOW GLOBAL STATUS WHERE Variable_name LIKE 'Innodb_row_lock%'"


def raw_mariadb(connector: SQLConnector) -> MariaDBConnector:
    raw = (
        connector._connector if isinstance(connector, _MeasuredConnector) else connector
    )
    if not isinstance(raw, MariaDBConnector):
        raise TypeError("MariaDB diagnostics require the private MariaDB fixture")
    return raw


def counters(connector: MariaDBConnector, query: str) -> dict[str, int]:
    result = {str(name): int(value) for name, value in connector.fetch_all(query)}
    if not result or any(value < 0 for value in result.values()):
        raise RuntimeError("MariaDB diagnostic counters were missing or invalid")
    return result


def counter_delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    if before.keys() != after.keys():
        raise RuntimeError("MariaDB diagnostic counter set changed")
    result = {name: after[name] - value for name, value in before.items()}
    if any(value < 0 for value in result.values()):
        raise RuntimeError("MariaDB diagnostic cumulative counter decreased")
    return result


def lock_counters(connector: SQLConnector) -> dict[str, int]:
    values = counters(raw_mariadb(connector), LOCK_STATUS)
    required = {"Innodb_row_lock_time", "Innodb_row_lock_waits"}
    if not required <= values.keys():
        raise RuntimeError("MariaDB row-lock cumulative counters are unavailable")
    return {key: values[key] for key in sorted(required)}


def validate_candidate_select(query: str) -> None:
    normalized = " ".join(query.upper().split())
    if not normalized.startswith("SELECT ") or any(
        text in normalized
        for text in (";", " FOR UPDATE", "LOCK IN SHARE MODE", " INTO ")
    ):
        raise ValueError(
            "diagnostics accept only captured nonlocking candidate SELECTs"
        )


def decode_plan(row: tuple[Any, ...]) -> dict[str, Any]:
    if len(row) != 1 or not isinstance(row[0], str):
        raise RuntimeError("MariaDB returned an invalid JSON plan row")
    result = json.loads(row[0])
    if not isinstance(result, dict) or not isinstance(result.get("query_block"), dict):
        raise RuntimeError("MariaDB JSON plan lacks a query block")
    return result


def plan_table_nodes(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Keep r_rows and r_loops separate: neither is distinct examined rows."""
    result: list[dict[str, Any]] = []

    def visit(value: object, path: str) -> None:
        if isinstance(value, dict):
            if "table_name" in value:
                result.append(
                    {
                        "path": path,
                        **{
                            key: value[key]
                            for key in (
                                "table_name",
                                "access_type",
                                "key",
                                "rows",
                                "r_rows",
                                "r_loops",
                                "r_filtered",
                                "r_table_time_ms",
                                "r_other_time_ms",
                                "r_engine_stats",
                            )
                            if key in value
                        },
                    }
                )
            for key, child in value.items():
                visit(child, f"{path}.{key}")
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(child, f"{path}[{index}]")

    visit(plan, "$")
    return result


def profile_candidate[T](
    connector: SQLConnector, action: Callable[[], T]
) -> tuple[T, dict[str, Any]]:
    """Capture one real candidate call, then profile its SELECT in the same cut.

    All diagnostic statements bypass the core observer and are kept out of the
    separately executed baseline. The enclosing diagnostic pass wall time does
    include them, so it must never be compared with the baseline wall time.
    """
    raw = raw_mariadb(connector)
    original = raw.fetch_one
    captured: list[tuple[str, tuple[Any, ...], tuple[Any, ...]]] = []

    def capture(query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
        validate_candidate_select(query)
        if captured:
            raise RuntimeError("candidate probe executed more than one SELECT")
        row = original(query, data)
        captured.append((query, data, row))
        return row

    with patch.object(raw, "fetch_one", capture):
        result = action()
    if len(captured) != 1:
        raise RuntimeError("candidate probe did not execute exactly one SELECT")
    query, parameters, expected = captured[0]
    if expected:
        raise RuntimeError("idle diagnostic candidate unexpectedly returned work")
    first = counters(raw, HANDLER_STATUS)
    before = counters(raw, HANDLER_STATUS)
    control = counter_delta(first, before)
    if any(control.values()):
        raise RuntimeError(
            "SHOW STATUS changed handler counters; attribution is invalid"
        )
    started = time.perf_counter()
    replayed = original(query, parameters)
    replay_seconds = time.perf_counter() - started
    after = counters(raw, HANDLER_STATUS)
    if replayed != expected:
        raise RuntimeError("candidate replay disagrees with the real idle query")
    handler_reads = counter_delta(before, after)
    explained = decode_plan(original("EXPLAIN FORMAT=JSON " + query, parameters))
    started = time.perf_counter()
    analyzed = decode_plan(original("ANALYZE FORMAT=JSON " + query, parameters))
    analyze_seconds = time.perf_counter() - started
    return result, {
        "sql_fingerprint": sha256(query.encode()).hexdigest()[:16],
        "parameter_count": len(parameters),
        "returned_rows": len(replayed),
        "replay_client_seconds": replay_seconds,
        "handler_status_control_delta": control,
        "handler_read_delta": handler_reads,
        "explain_json": explained,
        "analyze_json": analyzed,
        "analyze_client_seconds": analyze_seconds,
        "analyze_root_server_time_ms": analyzed["query_block"].get("r_total_time_ms"),
        "analyze_optimization_time_ms": analyzed.get("query_optimization", {}).get(
            "r_total_time_ms"
        ),
        "analyze_table_nodes": plan_table_nodes(analyzed),
    }


def roundtrip_control(
    connector: SQLConnector, *, repetitions: int = 20
) -> dict[str, Any]:
    if not 2 <= repetitions <= 100:
        raise ValueError("roundtrip repetitions must be between 2 and 100")
    raw = raw_mariadb(connector)
    samples = []
    for _ in range(repetitions):
        started = time.perf_counter()
        row = raw.fetch_one("SELECT 1")
        samples.append(time.perf_counter() - started)
        if row != (1,):
            raise RuntimeError("SELECT 1 roundtrip control returned an unexpected row")
    return {
        "repetitions": repetitions,
        "client_seconds": samples,
        "median_client_seconds": statistics.median(samples),
        "notes": "Repeated on one separate control connection to the same private fixture, not the candidate connection; includes driver, local transport and server. Not pure network RTT and not subtracted from candidate timings.",
    }


def diagnostic_notes() -> dict[str, Any]:
    return {
        "scope": "Separate read-only diagnostic pass after baseline and claim followup cleanup, on the same private MariaDB 10.11.11 fixture.",
        "notes": "Each candidate SELECT is captured from the real facade call and replayed once for session Handler_read counters, once by ANALYZE, and planned by EXPLAIN. Diagnostic overhead and cache warming are excluded from earlier baseline timings. Handler counters count engine access requests, not unique examined rows. ANALYZE r_rows is average rows per execution and r_loops is execution count; nested node times are inclusive and must not be summed. The root server time and client SELECT replay are different executions, so their difference is not a network latency measurement. Raw bound parameter values are not logged; plans may contain synthetic fixture constants. Global cumulative InnoDB row-lock counters cover only this isolated server's baseline window; zero excludes observed row waits here, not production or metadata-lock waits.",
        "references": [
            "https://mariadb.com/docs/server/reference/sql-statements/administrative-sql-statements/analyze-and-explain-statements/analyze-format-json",
            "https://mariadb.com/docs/server/server-management/variables-and-modes/server-status-variables",
            "https://mariadb.com/docs/server/ha-and-performance/optimization-and-tuning/system-variables/innodb-status-variables",
        ],
    }
