"""Interpret measured acceptance evidence without treating activity as success."""

from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from .evidence import read_evidence_bytes


def stage_totals(log: str) -> list[dict[str, Any]]:
    """Keep completed stage totals, never sum cumulative progress samples."""
    result: list[dict[str, Any]] = []
    for line in log.splitlines():
        if "ingest_db_performance " not in line:
            continue
        fields = dict(re.findall(r"(\w+)=([^\s]+)", line))
        if fields.get("event") not in {"stage_transition", "stage_terminal"}:
            continue
        row: dict[str, Any] = {}
        for key, value in fields.items():
            if key.endswith("_seconds"):
                row[key] = float(value)
            elif key in {
                "calls",
                "processed_rows",
                "replayed_calls",
                "read_rows",
                "generation",
            } or key.endswith("_calls"):
                row[key] = int(value)
            else:
                row[key] = value
        result.append(row)
    return result


def summarize_log(log: str) -> dict[str, Any]:
    totals = stage_totals(log)
    completions = []
    for line in log.splitlines():
        if "ingest_progress " not in line:
            continue
        fields = dict(re.findall(r"([\w.]+)=([^\s]+)", line))
        if (
            fields.get("event") == "work_finished"
            and fields.get("status") == "completed"
            and int(fields.get("counter.publication_batches_finalized", "0")) > 0
        ):
            completions.append(fields)
    analysis = [
        row
        for row in totals
        if row.get("pipeline") == "analysis"
        and row.get("operation") != "COMPLETE"
        and row.get("processed_rows", 0) > 0
        and row.get("replayed_calls", 0) < row.get("calls", 0)
    ]
    analysis_rows = [row for row in totals if row.get("pipeline") == "analysis"]
    analysis_completions = [
        row
        for row in analysis_rows
        if row.get("operation") == "COMPLETE" and row.get("event") == "stage_terminal"
    ]
    complete_replays = [
        row for row in analysis_completions if row.get("replayed_calls", 0) > 0
    ]
    return {
        "completed_batches": len(completions),
        "cbz_render_operations": sum(
            int(row.get("counter.archives_rendered", "0")) for row in completions
        ),
        "real_analysis_observed": bool(analysis),
        "analysis_terminal_observed": any(
            row.get("event") == "stage_terminal" for row in analysis_rows
        ),
        "analysis_complete_receipt_observed": bool(analysis_completions),
        "analysis_complete_replay_only": bool(complete_replays)
        and len(analysis_rows) == len(analysis_completions)
        and all(row.get("processed_rows", 0) == 0 for row in analysis_completions),
        "analysis_replayed_calls": sum(
            row.get("replayed_calls", 0) for row in analysis_rows
        ),
        "target_analysis_observed": any(
            row.get("operation") == "validate_file_hash_decision" for row in analysis
        ),
        "completed_stages": totals,
        "sql_calls": sum(row.get("sql_calls", 0) for row in totals),
        "core_sql_seconds": sum(row.get("sql_seconds", 0.0) for row in totals),
        "errors": [
            line
            for line in log.splitlines()
            if "[ERROR]" in line or "Traceback (most recent call last)" in line
        ],
        "warnings": [line for line in log.splitlines() if "[WARNING]" in line],
        "limitations": [
            "Completed work and detailed stage evidence require DEBUG diagnostics; human INFO wording is not a machine protocol.",
            "Only completed core stages are totaled; source/admin and incomplete interrupted stages are separate.",
            "Core SQL timing excludes adapter SQLite and native filesystem operations.",
            "Render operations are not distinct gallery identities.",
            "A COMPLETE replay is receipt reuse, not evidence of a fresh analysis pass.",
        ],
    }


def read_probe_events(paths: Iterable[Path]) -> tuple[list[dict[str, Any]], list[str]]:
    events: list[dict[str, Any]] = []
    damaged: list[str] = []
    for path in sorted(set(paths)):
        identity = str(path.absolute())
        first_event = len(events)
        first_damage = len(damaged)
        content = read_evidence_bytes(path.parent, path.name)
        for number, line in enumerate(content.splitlines(keepends=True), 1):
            if not line.endswith(b"\n"):
                damaged.append(f"{path.name}:{number}:truncated")
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError, UnicodeDecodeError:
                damaged.append(f"{path.name}:{number}")
                continue
            if _valid_probe_event(value):
                # The source file, not an in-container PID, owns process identity.
                value["evidence_file"] = identity
                events.append(value)
            else:
                damaged.append(f"{path.name}:{number}")
        if len(damaged) != first_damage:
            for event in events[first_event:]:
                event["evidence_file_damaged"] = True
    return events, damaged


def _valid_probe_event(value: Any) -> bool:
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        return False
    for field in ("pid", "sequence", "monotonic_ns"):
        if type(value.get(field)) is not int or value[field] <= 0:
            return False
    for field in ("process_instance", "scenario", "event", "operation"):
        if not isinstance(value.get(field), str) or not value[field]:
            return False
    counters = value.get("counters")
    if not isinstance(counters, dict):
        return False
    for operation, counter in counters.items():
        if not isinstance(operation, str) or not isinstance(counter, dict):
            return False
        for field in ("calls", "completed", "failed", "logical_bytes"):
            if type(counter.get(field)) is not int or counter[field] < 0:
                return False
        if counter["completed"] + counter["failed"] > counter["calls"]:
            return False
        seconds = counter.get("seconds")
        if (
            not isinstance(seconds, int | float)
            or isinstance(seconds, bool)
            or not math.isfinite(seconds)
            or seconds < 0
        ):
            return False
    if "capabilities" in value and not isinstance(value["capabilities"], dict):
        return False
    if value["event"] == "audit_result":
        elapsed = value.get("elapsed_seconds")
        if (
            value.get("mode") not in {"quick", "full"}
            or not isinstance(value.get("reason"), str)
            or not value["reason"]
            or not isinstance(elapsed, int | float)
            or isinstance(elapsed, bool)
            or not math.isfinite(elapsed)
            or elapsed < 0
        ):
            return False
    return True


def summarize_probe(events: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Retain final counters per process and identify process-exit evidence."""
    by_process: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for event in events:
        key = event.get("evidence_file", event.get("process_instance"))
        if not isinstance(key, str) or not key:
            raise ValueError(
                "probe events require a source file or process-instance identity"
            )
        by_process[key].append(event)
    processes: list[dict[str, Any]] = []
    for key, values in sorted(by_process.items()):
        latest = values[-1]
        exits = [row for row in values if row.get("event") == "process_exit"]
        installations = [row for row in values if row.get("event") == "installed"]
        issues: list[str] = []
        if any(row.get("evidence_file_damaged") is True for row in values):
            issues.append("file_contains_invalid_or_truncated_events")
        if [row.get("sequence") for row in values] != list(range(1, len(values) + 1)):
            issues.append("event_sequence_has_gaps_duplicates_or_reordering")
        for field in ("pid", "scenario", "process_instance"):
            if len({row.get(field) for row in values}) != 1:
                issues.append(f"inconsistent_{field}")
        if len(installations) != 1:
            issues.append("missing_or_duplicate_installation_receipt")
        if len(exits) > 1 or (exits and latest.get("event") != "process_exit"):
            issues.append("exit_receipt_is_duplicate_or_not_last")
        final = exits[-1] if exits else {}
        complete = (
            not issues
            and latest.get("event") == "process_exit"
            and final.get("measurement_valid") is True
            and final.get("counters_complete") is True
            and final.get("counter_tail_status") == "complete"
        )
        processes.append(
            {
                "process": key,
                "pid": latest.get("pid"),
                "scenario": latest.get("scenario"),
                "events": len(values),
                "audit_results": [
                    {
                        field: row[field]
                        for field in (
                            "sequence",
                            "operation",
                            "mode",
                            "reason",
                            "elapsed_seconds",
                        )
                    }
                    for row in values
                    if row.get("event") == "audit_result"
                ],
                "clean_exit_recorded": bool(exits),
                "measurement_valid": final.get("measurement_valid")
                if not issues
                else False,
                "counters_complete": complete,
                "counter_tail_status": "complete" if complete else "incomplete",
                "last_counters": latest.get("counters", {}),
                "last_sequence": latest.get("sequence"),
                "last_monotonic_ns": latest.get("monotonic_ns"),
                "last_active_spans": latest.get("active_spans", []),
                "probe_self_io_seconds_before_last_event": latest.get(
                    "probe_self_io_seconds_before_event"
                ),
                "probe_evidence_fsync_calls_before_last_event": latest.get(
                    "probe_evidence_fsync_calls_before_event"
                ),
                "versions": installations[0].get("versions") if installations else None,
                "capabilities": installations[0].get("capabilities")
                if installations
                else None,
                "fault_injection": any(
                    row.get("fault_injection") is True for row in values
                ),
                "fault_injection_enabled": any(
                    row.get("capabilities", {}).get("fault_injection") is True
                    for row in values
                ),
                "evidence_issues": issues,
            }
        )
    return {
        "processes": processes,
        "limitations": [
            "A process without an exit record has incomplete final counters; never impute a complete total.",
            "Python explicit fsync counts exclude SQLite/native-library synchronization calls.",
            "Instrumented elapsed times include observer overhead and are not uninstrumented wall-clock baselines.",
            "Cumulative snapshots are never added; last_counters is the last observed value per process file.",
            "An atexit record does not establish application success; check its real exit code and independent oracle.",
            "SIGKILL may lose observations since the latest durable event, even if all visible JSON lines parse.",
            "Fault-injected processes are recovery evidence and must not be performance baselines.",
        ],
    }


def assess_probe_measurement(
    summary: Mapping[str, Any],
    damaged: Iterable[str],
    *,
    instrumented: bool,
) -> dict[str, Any]:
    """Assess observer evidence independently of application correctness."""
    if not instrumented:
        return {"status": "not_requested", "issues": [], "incomplete_processes": 0}
    issues: list[str] = []
    if list(damaged):
        issues.append("probe_evidence_has_damaged_lines")
    processes = summary.get("processes", [])
    if not isinstance(processes, list) or not processes:
        return {
            "status": "failed",
            "issues": [*issues, "probe_process_evidence_missing"],
            "incomplete_processes": 0,
        }
    complete_ingest_capabilities = False
    incomplete = 0
    for position, process in enumerate(processes, 1):
        if not isinstance(process, dict):
            issues.append(f"process_{position}:invalid_process_summary")
            continue
        if process.get("measurement_valid") is False:
            issues.append(f"process_{position}:measurement_invalid")
        if process.get("evidence_issues"):
            issues.append(f"process_{position}:invalid_event_sequence_or_receipts")
        capabilities = process.get("capabilities")
        if not isinstance(capabilities, dict):
            issues.append(f"process_{position}:installation_capabilities_missing")
        elif capabilities.get("core") is True and capabilities.get("ingest") is True:
            complete_ingest_capabilities = True
        if process.get("counters_complete") is not True:
            incomplete += 1
    if not complete_ingest_capabilities:
        issues.append("installed_core_and_ingest_capabilities_missing")
    return {
        "status": "failed" if issues else "passed",
        "issues": issues,
        "incomplete_processes": incomplete,
    }
