"""Evidence contracts for opt-in equal-append resident growth rounds."""

from __future__ import annotations

import math
import re
from typing import Any


def growth_evidence(
    log: str,
    summary: dict[str, Any],
    oracle: dict[str, Any],
    *,
    expected_galleries: int,
    pages_per_gallery: int,
) -> dict[str, Any]:
    """Require real cross-page work without assuming validation is always full-scan."""
    expected_pages = expected_galleries * pages_per_gallery
    if (
        oracle.get("actual_source_galleries") != expected_galleries
        or oracle.get("verified_publications") != expected_galleries
        or oracle.get("actual_source_pages") != expected_pages
        or oracle.get("verified_pages") != expected_pages
    ):
        raise AssertionError("Growth round oracle does not match its exact source size")
    stages = summary["completed_stages"]
    targets = [
        row
        for row in stages
        if row.get("pipeline") == "analysis"
        and row.get("operation") == "validate_file_hash_decision"
        and row.get("replayed_calls", 0) < row.get("calls", 0)
    ]
    if not any(row.get("processed_rows", 0) > 128 for row in targets):
        raise AssertionError(
            "Growth round lacks non-replayed validation beyond 128 keys"
        )
    analysis_generations = {
        row["generation"]
        for row in stages
        if row.get("pipeline") == "analysis"
        and row.get("event") == "stage_terminal"
        and row.get("operation") != "COMPLETE"
    }
    publication_generations = {
        row["generation"]
        for row in stages
        if row.get("pipeline") == "publication"
        and row.get("event") == "stage_terminal"
        and row.get("operation") == "COMPLETE"
    }
    target_generations = {row["generation"] for row in targets}
    if not target_generations <= analysis_generations & publication_generations:
        raise AssertionError(
            "Growth round lacks completed analysis/publication evidence"
        )
    for row in stages:
        if (
            type(row.get("sql_calls")) is not int
            or row["sql_calls"] < 0
            or any(
                not isinstance(row.get(field), (int, float))
                or not math.isfinite(row[field])
                or row[field] < 0
                for field in ("wall_seconds", "sql_seconds")
            )
        ):
            raise AssertionError(
                "Growth round has missing or invalid stage cost evidence"
            )
    phases = []
    by_generation: dict[int, set[str]] = {}
    for line in log.splitlines():
        if "ingest_progress " not in line:
            continue
        fields = dict(re.findall(r"([\w.]+)=([^\s]+)", line))
        if fields.get("event") != "phase_ended":
            continue
        generation = int(fields["generation"])
        elapsed = float(fields["phase_elapsed_seconds"])
        work_elapsed = float(fields["elapsed_seconds"])
        if not all(
            math.isfinite(value) and value >= 0 for value in (elapsed, work_elapsed)
        ):
            raise AssertionError("Growth round has invalid phase timing")
        phase = fields["phase"]
        by_generation.setdefault(generation, set()).add(phase)
        phases.append(
            {
                "generation": generation,
                "phase": phase,
                "phase_elapsed_seconds": elapsed,
                "work_elapsed_seconds": work_elapsed,
            }
        )
    complete_phases = {
        generation
        for generation, names in by_generation.items()
        if {"source", "analysis", "publication"} <= names
    }
    if not complete_phases or len(complete_phases) != summary["completed_batches"]:
        raise AssertionError(
            "Growth round lacks complete source/analysis/publication phases"
        )
    return {
        "expected_source_galleries": expected_galleries,
        "expected_source_pages": expected_pages,
        "completed_resident_batches": summary["completed_batches"],
        "analysis_generations": sorted(analysis_generations),
        "publication_generations": sorted(publication_generations),
        "progress_generations": sorted(complete_phases),
        "validation_processed_rows": [row["processed_rows"] for row in targets],
        "phase_timings": phases,
        "limitations": [
            "An equal input round can produce multiple resident generations; actual counts are reported, not called one atomic publication.",
            "Validation rows need not equal the whole current source when a valid implementation uses incremental validation.",
            "Phase durations contain nested core stages; do not add both totals. Progress durations have log rounding precision.",
            "Core stage SQL excludes preclaim cleanup and adapter work; process-wide probe counters are separate cumulative evidence.",
        ],
    }
