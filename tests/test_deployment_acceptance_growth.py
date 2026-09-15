"""Offline growth evidence contracts, independent of Docker and image generation."""

from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

_DIRECTORY = Path(__file__).resolve().parents[1] / "scripts/deployment_acceptance"
_NAME = "acceptance_growth_under_test"
_SPEC = importlib.util.spec_from_file_location(
    _NAME, _DIRECTORY / "__init__.py", submodule_search_locations=[str(_DIRECTORY)]
)
assert _SPEC is not None and _SPEC.loader is not None
_PACKAGE = importlib.util.module_from_spec(_SPEC)
sys.modules[_NAME] = _PACKAGE
_SPEC.loader.exec_module(_PACKAGE)
growth = importlib.import_module(_NAME + ".growth")
report = importlib.import_module(_NAME + ".report")


def _log(generation: int = 3, *, rows: int = 258) -> str:
    stages = (
        ("analysis", "validate_file_hash_decision", "stage_transition", rows),
        ("analysis", "snapshot_manifest", "stage_terminal", 6),
        ("publication", "COMPLETE", "stage_terminal", 0),
    )
    return "\n".join(
        [
            f"ingest_db_performance event={event} pipeline={pipeline} operation={operation} "
            f"generation={generation} calls=3 replayed_calls=0 processed_rows={processed} "
            "sql_calls=7 wall_seconds=0.5 sql_seconds=0.2"
            for pipeline, operation, event, processed in stages
        ]
        + [
            "ingest_progress event=phase_ended "
            f"generation={generation} phase={phase} phase_elapsed_seconds=1.0 elapsed_seconds=3.0"
            for phase in ("source", "analysis", "publication")
        ]
        + [
            "ingest_progress event=work_finished status=completed "
            "counter.publication_batches_finalized=1 counter.archives_rendered=2"
        ]
    )


def _evidence(log: str, **changes: Any) -> dict[str, Any]:
    oracle = {
        "actual_source_galleries": 6,
        "verified_publications": 6,
        "actual_source_pages": 774,
        "verified_pages": 774,
        **changes,
    }
    result: dict[str, Any] = growth.growth_evidence(
        log,
        report.summarize_log(log),
        oracle,
        expected_galleries=6,
        pages_per_gallery=129,
    )
    return result


def test_growth_accepts_incremental_validation_and_reports_actual_generations() -> None:
    result = _evidence(_log())
    assert result["validation_processed_rows"] == [258]
    assert result["expected_source_pages"] == 774
    assert result["analysis_generations"] == [3]
    assert result["completed_resident_batches"] == 1
    assert len(result["phase_timings"]) == 3


def test_growth_does_not_mislabel_multiple_generations_as_one_batch() -> None:
    result = _evidence(_log(3) + "\n" + _log(4))
    assert result["completed_resident_batches"] == 2
    assert result["analysis_generations"] == [3, 4]
    assert result["publication_generations"] == [3, 4]


@pytest.mark.parametrize(
    "missing",
    [
        "phase=source",
        "phase=analysis",
        "phase=publication",
        "operation=snapshot_manifest",
        "operation=COMPLETE",
    ],
)
def test_growth_requires_complete_phase_and_terminal_evidence(missing: str) -> None:
    log = "\n".join(line for line in _log().splitlines() if missing not in line)
    with pytest.raises(AssertionError, match="evidence|phases"):
        _evidence(log)


@pytest.mark.parametrize("rows", [0, 128])
def test_growth_rejects_small_validation_even_with_large_final_catalog(
    rows: int,
) -> None:
    with pytest.raises(AssertionError, match="beyond 128"):
        _evidence(_log(rows=rows))


def test_growth_rejects_replayed_hash_stage() -> None:
    with pytest.raises(AssertionError, match="beyond 128"):
        _evidence(_log().replace("replayed_calls=0", "replayed_calls=3"))


@pytest.mark.parametrize("field", ["actual_source_galleries", "verified_pages"])
def test_growth_requires_exact_independent_source_and_catalog(field: str) -> None:
    with pytest.raises(AssertionError, match="exact source size"):
        _evidence(_log(), **{field: 1})


@pytest.mark.parametrize(
    "before,after",
    [
        ("wall_seconds=0.5", "wall_seconds=nan"),
        ("sql_calls=7", ""),
        ("sql_seconds=0.2", "sql_seconds=-1"),
        ("phase_elapsed_seconds=1.0", "phase_elapsed_seconds=inf"),
    ],
)
def test_growth_rejects_missing_or_invalid_costs(before: str, after: str) -> None:
    with pytest.raises(AssertionError, match="cost evidence|phase timing"):
        _evidence(_log().replace(before, after))
