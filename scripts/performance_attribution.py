"""Validate exact dev-probe SQL attribution before drawing cost conclusions.

This consumes the complete report from ingest_pipeline_probe.Observer. It does
not change runtime telemetry, collect SQL, or accept truncated top-N summaries
as complete evidence. Output contains fingerprints and counts, never SQL text.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from hashlib import sha256
from pathlib import Path
from typing import Any


def _incomplete(reason: str) -> dict[str, Any]:
    return {"schema_version": 1, "status": "incomplete", "reasons": [reason]}


def _count(value: object) -> bool:
    return type(value) is int and value >= 0


def _seconds(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value) and value >= 0
    except OverflowError:
        return False


def _same_seconds(first: float, second: float) -> bool:
    # The observer sums by event while this verifier sums by group. Permit only
    # floating-point accumulation error, not missing measured time.
    return math.isclose(first, second, rel_tol=1e-12, abs_tol=1e-9)


def assess_attribution(report: Mapping[str, Any]) -> dict[str, Any]:
    """Fail closed unless every observed SQL group is available and conserved."""

    if report.get("query_details_truncated") is not False:
        return _incomplete("exact_query_details_unavailable")
    if (
        not _count(report.get("omitted_query_events"))
        or report["omitted_query_events"] != 0
    ):
        return _incomplete("query_events_omitted")
    queries = report.get("queries")
    if not isinstance(queries, list):
        return _incomplete("exact_query_details_unavailable")
    if not _count(report.get("query_group_count")) or report[
        "query_group_count"
    ] != len(queries):
        return _incomplete("query_group_count_mismatch")
    if (
        not _count(report.get("query_group_budget"))
        or report["query_group_budget"] == 0
        or len(queries) > report["query_group_budget"]
    ):
        return _incomplete("query_group_budget_exceeded")
    if (
        not _count(report.get("sql_calls"))
        or not _count(report.get("returned_rows"))
        or not _seconds(report.get("sql_seconds"))
    ):
        return _incomplete("invalid_sql_totals")

    families: dict[str, dict[str, Any]] = {}
    fingerprints: dict[str, str] = {}
    phases: dict[tuple[str, str, str], dict[str, Any]] = {}
    groups: set[tuple[str, ...]] = set()
    for row in queries:
        if not isinstance(row, dict):
            return _incomplete("invalid_query_group")
        names = ("pipeline", "phase", "operation", "category", "sql", "fingerprint")
        if any(not isinstance(row.get(name), str) for name in names):
            return _incomplete("invalid_query_group")
        fingerprint = row["fingerprint"]
        if (
            row["category"] not in {"sql", "connection", "transaction"}
            or re.fullmatch(r"[0-9a-f]{16}", fingerprint) is None
            or fingerprint != sha256(row["sql"].encode()).hexdigest()[:16]
            or not _count(row.get("calls"))
            or row["calls"] == 0
            or not _count(row.get("returned_rows"))
            or not _seconds(row.get("seconds"))
            or not _seconds(row.get("max_seconds"))
            or row["max_seconds"] > row["seconds"]
        ):
            return _incomplete("invalid_query_group")
        key = tuple(row[name] for name in names[:-1])
        if key in groups:
            return _incomplete("duplicate_query_group")
        groups.add(key)
        previous_query = fingerprints.setdefault(fingerprint, row["sql"])
        if previous_query != row["sql"]:
            return _incomplete("fingerprint_collision")
        if row["category"] != "sql":
            continue
        family = families.setdefault(
            fingerprint,
            {
                "fingerprint": fingerprint,
                "calls": 0,
                "seconds": 0.0,
                "returned_rows": 0,
                "max_seconds": 0.0,
            },
        )
        family["calls"] += row["calls"]
        family["seconds"] += row["seconds"]
        family["returned_rows"] += row["returned_rows"]
        family["max_seconds"] = max(family["max_seconds"], row["max_seconds"])
        phase = phases.setdefault(
            (row["pipeline"], row["phase"], row["operation"]),
            {
                "pipeline": row["pipeline"],
                "phase": row["phase"],
                "operation": row["operation"],
                "sql_calls": 0,
                "sql_seconds": 0.0,
                "returned_rows": 0,
            },
        )
        phase["sql_calls"] += row["calls"]
        phase["sql_seconds"] += row["seconds"]
        phase["returned_rows"] += row["returned_rows"]
    calls = sum(row["calls"] for row in families.values())
    seconds = sum(row["seconds"] for row in families.values())
    returned_rows = sum(row["returned_rows"] for row in families.values())
    if not _seconds(seconds):
        return _incomplete("invalid_sql_aggregate")
    if (
        calls != report["sql_calls"]
        or returned_rows != report["returned_rows"]
        or not _same_seconds(seconds, report["sql_seconds"])
    ):
        return _incomplete("sql_totals_not_conserved")
    if calls == 0:
        return _incomplete("no_sql_observed")
    return {
        "schema_version": 1,
        "status": "complete",
        "reasons": [],
        "sql_calls": calls,
        "sql_seconds": seconds,
        "returned_rows": returned_rows,
        "query_group_count": len(queries),
        "fingerprint_count": len(families),
        "fingerprint_algorithm": "sha256-raw-query-first16",
        "phases": sorted(
            phases.values(),
            key=lambda row: (row["pipeline"], row["phase"], row["operation"]),
        ),
        "families": sorted(
            families.values(), key=lambda row: (-row["seconds"], row["fingerprint"])
        ),
    }


def assess_observer_report(
    read_report: Callable[[], Mapping[str, Any]],
) -> dict[str, Any]:
    """Observer failures and unfinished scopes cannot become passing evidence."""

    try:
        return assess_attribution(read_report())
    except Exception:
        # Exception text may contain SQL literals or external parameter values.
        return _incomplete("observer_report_unavailable")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    args = parser.parse_args(argv)
    try:
        raw = json.loads(args.report.read_text(encoding="utf-8"))
        result = (
            assess_attribution(raw)
            if isinstance(raw, dict)
            else _incomplete("invalid_report")
        )
    except OSError, ValueError, TypeError, OverflowError:
        result = _incomplete("invalid_report")
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0 if result["status"] == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
