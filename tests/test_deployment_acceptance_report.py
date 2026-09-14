from __future__ import annotations

import importlib
import importlib.util
import json
import logging
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from h2hdb.ingest_performance import IngestPerformance

_SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "deployment_acceptance"
    / "report.py"
)


@pytest.fixture
def report() -> ModuleType:
    name = "deployment_acceptance_report_under_test"
    spec = importlib.util.spec_from_file_location(
        name,
        _SCRIPT.parent / "__init__.py",
        submodule_search_locations=[str(_SCRIPT.parent)],
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return importlib.import_module(name + ".report")


def test_real_logger_progress_and_debug_calls_are_not_double_counted(
    report: ModuleType,
    caplog: pytest.LogCaptureFixture,
) -> None:
    now = 0.0
    logger = logging.getLogger("acceptance.completed-stages")
    performance = IngestPerformance(
        logger, backend="sqlite", level=logging.DEBUG, clock=lambda: now
    )
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        for count in range(3):
            with performance.step(
                "analysis", "commit", "validate_file_hash_decision", 1
            ) as step:
                step.processed_rows = 128
                step.counters.sql_calls = 3
                step.counters.sql_seconds = 0.2
                now += 65
                step.terminal = count == 2
        performance.close()
    assert "event=completed" in caplog.text
    assert "event=stage_progress" in caplog.text
    summary = report.summarize_log(caplog.text)
    assert summary["sql_calls"] == 9
    assert summary["core_sql_seconds"] == pytest.approx(0.6)
    assert summary["real_analysis_observed"] is True
    assert summary["target_analysis_observed"] is True
    assert summary["analysis_complete_replay_only"] is False
    assert len(summary["completed_stages"]) == 1
    assert summary["completed_stages"][0]["processed_rows"] == 384


def test_complete_receipt_replay_is_not_fresh_analysis_or_a_completed_batch(
    report: ModuleType,
) -> None:
    # A replayed commit accompanies ordinary issue/prepare calls: 1 replay of 3
    # facade calls still means the COMPLETE receipt was reused.
    log = (
        "ingest_db_performance event=stage_started backend=mariadb pipeline=analysis "
        "operation=COMPLETE generation=48 calls=0 processed_rows=0 replayed_calls=0 sql_calls=0\n"
        "ingest_db_performance event=stage_terminal backend=mariadb pipeline=analysis "
        "operation=COMPLETE generation=48 calls=3 processed_rows=0 replayed_calls=1 sql_calls=204\n"
    )
    result = report.summarize_log(log)
    assert result["analysis_terminal_observed"] is True
    assert result["analysis_complete_receipt_observed"] is True
    assert result["analysis_complete_replay_only"] is True
    assert result["analysis_replayed_calls"] == 1
    assert result["real_analysis_observed"] is False
    assert result["target_analysis_observed"] is False
    assert result["completed_batches"] == 0
    assert result["cbz_render_operations"] == 0


def test_fresh_analysis_ends_with_snapshot_manifest_without_complete_replay(
    report: ModuleType,
) -> None:
    result = report.summarize_log(
        "ingest_db_performance event=stage_transition backend=mariadb pipeline=analysis "
        "operation=validate_file_hash_decision generation=1 calls=6 processed_rows=16 replayed_calls=0\n"
        "ingest_db_performance event=stage_terminal backend=mariadb pipeline=analysis "
        "operation=snapshot_manifest generation=1 calls=9 processed_rows=0 replayed_calls=0\n"
    )
    assert result["analysis_terminal_observed"] is True
    assert result["analysis_complete_receipt_observed"] is False
    assert result["analysis_complete_replay_only"] is False
    assert result["real_analysis_observed"] is True


def test_incomplete_progress_cannot_establish_completed_analysis(
    report: ModuleType,
) -> None:
    result = report.summarize_log(
        "ingest_db_performance event=stage_progress backend=mariadb pipeline=analysis "
        "operation=validate_file_hash_decision generation=1 calls=25 processed_rows=900 sql_calls=200"
    )
    assert result["real_analysis_observed"] is False
    assert result["completed_stages"] == []
    assert result["sql_calls"] == 0


def test_work_evidence_uses_debug_snapshot_not_human_wording(
    report: ModuleType,
) -> None:
    result = report.summarize_log(
        "[INFO] Ingest work completed: catalog batches published 99; CBZs rendered this work 99\n"
        "[DEBUG] ingest_progress event=periodic counter.publication_batches_finalized=1 counter.archives_rendered=4\n"
        "[DEBUG] ingest_progress event=work_finished status=failed counter.publication_batches_finalized=1 counter.archives_rendered=4\n"
        "[DEBUG] ingest_progress event=work_finished status=completed counter.archives_rendered=4\n"
        "[DEBUG] ingest_progress event=work_finished status=completed counter.publication_batches_finalized=1 counter.archives_rendered=4\n"
        "[DEBUG] ingest_progress event=work_finished status=completed counter.publication_batches_finalized=1\n"
    )
    assert result["completed_batches"] == 2
    assert result["cbz_render_operations"] == 4


def test_human_only_log_cannot_establish_machine_evidence(report: ModuleType) -> None:
    result = report.summarize_log(
        "Ingest work completed: catalog batches published 1; CBZs rendered this work 2"
    )
    assert result["completed_batches"] == 0
    assert result["real_analysis_observed"] is False


def _event(
    sequence: int, event: str, count: int, *, instance: str = "container-instance"
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema_version": 1,
        "pid": 1,
        "scenario": "restart",
        "process_instance": instance,
        "sequence": sequence,
        "monotonic_ns": sequence * 1000,
        "event": event,
        "operation": "test.operation",
        "counter_tail_status": "unsealed",
        "counters": {
            "python_explicit_fsync": {
                "calls": count,
                "completed": count,
                "failed": 0,
                "seconds": count / 1000,
                "logical_bytes": 0,
            }
        },
    }
    if event == "installed":
        value["capabilities"] = {"fault_injection": False, "core": True}
        value["versions"] = {"h2hdb": "0.37.3"}
    elif event == "process_exit":
        value.update(
            measurement_valid=True,
            counters_complete=True,
            counter_tail_status="complete",
        )
    return value


def _write(path: Path, events: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(event) + "\n" for event in events))


def test_container_pid_reuse_is_separate_and_snapshots_are_not_added(
    report: ModuleType,
    tmp_path: Path,
) -> None:
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"
    _write(
        first,
        [
            _event(1, "installed", 0),
            _event(2, "snapshot", 5),
            _event(3, "process_exit", 6),
        ],
    )
    _write(second, [_event(1, "installed", 0), _event(2, "snapshot", 2)])
    events, damaged = report.read_probe_events([second, first, first])
    assert not damaged
    result = report.summarize_probe(events)
    processes = result["processes"]
    assert len(processes) == 2
    assert {row["pid"] for row in processes} == {1}
    assert processes[0]["last_counters"]["python_explicit_fsync"]["calls"] == 6
    assert processes[0]["counters_complete"] is True
    assert processes[1]["last_counters"]["python_explicit_fsync"]["calls"] == 2
    assert processes[1]["counters_complete"] is False
    assert processes[1]["counter_tail_status"] == "incomplete"
    assert processes[1]["measurement_valid"] is None


@pytest.mark.parametrize(
    "tail", [b'{"unfinished":', b'{"not_an_event": 1}\n', b"\xff\n"]
)
def test_damaged_tail_invalidates_even_an_earlier_exit_receipt(
    report: ModuleType,
    tmp_path: Path,
    tail: bytes,
) -> None:
    path = tmp_path / "events.jsonl"
    _write(path, [_event(1, "installed", 0), _event(2, "process_exit", 1)])
    with path.open("ab") as destination:
        destination.write(tail)
    events, damaged = report.read_probe_events([path])
    assert len(damaged) == 1
    result = report.summarize_probe(events)["processes"][0]
    assert result["counters_complete"] is False
    assert result["measurement_valid"] is False
    assert "file_contains_invalid_or_truncated_events" in result["evidence_issues"]


def test_missing_middle_event_and_fault_mode_are_explicit(
    report: ModuleType,
    tmp_path: Path,
) -> None:
    path = tmp_path / "events.jsonl"
    reached = _event(3, "fault_reached", 1)
    reached.update(token="kill-one", fault_injection=True)
    _write(path, [_event(1, "installed", 0), reached, _event(4, "process_exit", 1)])
    events, damaged = report.read_probe_events([path])
    assert not damaged
    process = report.summarize_probe(events)["processes"][0]
    assert process["fault_injection"] is True
    assert process["counters_complete"] is False
    assert (
        "event_sequence_has_gaps_duplicates_or_reordering" in process["evidence_issues"]
    )


def test_enabled_fault_control_does_not_mean_a_fault_was_actually_injected(
    report: ModuleType,
    tmp_path: Path,
) -> None:
    path = tmp_path / "enabled-only.jsonl"
    installed = _event(1, "installed", 0)
    installed["capabilities"]["fault_injection"] = True
    _write(path, [installed, _event(2, "process_exit", 5)])
    events, damaged = report.read_probe_events([path])
    assert not damaged
    process = report.summarize_probe(events)["processes"][0]
    assert process["fault_injection_enabled"] is True
    assert process["fault_injection"] is False


def test_direct_events_require_process_identity_instead_of_grouping_by_pid(
    report: ModuleType,
) -> None:
    with pytest.raises(ValueError, match="identity"):
        report.summarize_probe([{"pid": 1, "sequence": 1}])


def test_probe_reader_rejects_symlink_instead_of_reading_its_target(
    report: ModuleType,
    tmp_path: Path,
) -> None:
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    target = tmp_path / "outside.jsonl"
    _write(target, [_event(1, "installed", 0)])
    linked = evidence / "probe-linked.jsonl"
    linked.symlink_to(target)
    with pytest.raises(ValueError, match="not a regular file"):
        report.read_probe_events([linked])


def test_measurement_is_not_required_for_an_uninstrumented_baseline(
    report: ModuleType,
) -> None:
    result = report.assess_probe_measurement({}, ["unrelated"], instrumented=False)
    assert result == {
        "status": "not_requested",
        "issues": [],
        "incomplete_processes": 0,
    }


def test_sigkill_incomplete_tail_is_valid_measurement_without_claiming_completion(
    report: ModuleType,
) -> None:
    installed = _event(1, "installed", 0)
    installed["capabilities"]["ingest"] = True
    summary = report.summarize_probe([installed, _event(2, "snapshot", 6)])
    assert summary["processes"][0]["measurement_valid"] is None
    result = report.assess_probe_measurement(summary, [], instrumented=True)
    assert result == {"status": "passed", "issues": [], "incomplete_processes": 1}


@pytest.mark.parametrize(
    "failure", ["damaged", "observer_failed", "no_ingest", "no_install", "missing"]
)
def test_instrumented_acceptance_rejects_unusable_measurement_evidence(
    report: ModuleType,
    failure: str,
) -> None:
    installed = _event(1, "installed", 0)
    installed["capabilities"]["ingest"] = failure != "no_ingest"
    events = [installed, _event(2, "process_exit", 3)]
    if failure == "observer_failed":
        events[-1]["measurement_valid"] = False
    elif failure == "no_install":
        events[0]["event"] = "snapshot"
    elif failure == "missing":
        events = []
    summary = report.summarize_probe(events)
    result = report.assess_probe_measurement(
        summary,
        ["damaged-line"] if failure == "damaged" else [],
        instrumented=True,
    )
    assert result["status"] == "failed"
    assert result["issues"]
