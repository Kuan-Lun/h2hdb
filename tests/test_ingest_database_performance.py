"""Test the acceptance tool, including rejection of known production costs.

Passing these tests means the detector works, not that performance acceptance
passed. The manual CLI independently returns nonzero for observed violations.
"""

from __future__ import annotations

import argparse
import errno
import importlib.util
import json
import os
import py_compile
import signal
import subprocess
import sys
import time
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest


@pytest.fixture(scope="module")
def acceptance() -> ModuleType:
    name = "ingest_database_performance_under_test"
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "check-ingest-database-performance.py"
    )
    spec = importlib.util.spec_from_file_location(name, script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    previous = list(sys.path)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = previous
    return module


def _turn(acceptance: ModuleType, *, calls: int = 1) -> dict[str, Any]:
    observer = acceptance.AcceptanceObserver()
    for phase in acceptance.PHASE_CEILINGS:
        observer.phase = phase
        for _ in range(calls):
            observer.record_sql_operation("sql", 0.001, "SELECT 1", 1)
    measurements = observer.report()
    for row in measurements["queries"]:
        if row["pipeline"] == "source":
            row.update(phase="commit", operation="FILE_PAGE")
    return {
        "selected": 1,
        "added": 1,
        "phases": dict.fromkeys(acceptance.PHASE_CEILINGS, 0.01),
        "cleanup": "DONE",
        "measurements": measurements,
    }


def _audit(acceptance: ModuleType, *, seconds: float = 1.0) -> dict[str, Any]:
    observer = acceptance.AcceptanceObserver()
    observer.phase = "ready_audit"
    observer.record_sql_operation("sql", 0.001, "SELECT 1", 1)
    return {
        "state": "READY",
        "wall_seconds": seconds,
        "measurements": observer.report(),
    }


@pytest.mark.parametrize("files", [1, 127, 128, 129, 66048])
def test_full_audit_cost_has_an_independent_fixed_boundary(
    acceptance: ModuleType, files: int
) -> None:
    # The arithmetic oracle is deliberately independent of production constants.
    ceiling = 60.0 + files / 500
    for elapsed, expected in (
        (ceiling - 0.001, "satisfied"),
        (ceiling, "satisfied"),
        (ceiling + 0.001, "violated"),
    ):
        verdict = acceptance.assess_ready_audit(
            _audit(acceptance, seconds=elapsed), retained_galleries=1, pages=files - 1
        )
        assert verdict["status"] == expected
        assert verdict["checks"][0]["upper_bound"] == ceiling
        assert verdict["dimensions"]["retained_source_files"] == files
        assert verdict["attribution"]["status"] == "complete"


@pytest.mark.parametrize(
    "fault",
    [
        "unfinished",
        "negative",
        "nan",
        "missing",
        "scope",
        "sql_wall",
        "truncated",
        "empty_sql",
    ],
)
def test_incomplete_audit_cannot_be_accepted(
    acceptance: ModuleType, fault: str
) -> None:
    audit = _audit(acceptance)
    if fault == "unfinished":
        audit["state"] = "BUILDING"
    elif fault == "negative":
        audit["wall_seconds"] = -1.0
    elif fault == "nan":
        audit["wall_seconds"] = float("nan")
    elif fault == "missing":
        del audit["wall_seconds"]
    elif fault == "scope":
        audit["measurements"]["queries"][0]["pipeline"] = "source"
    elif fault == "sql_wall":
        audit["wall_seconds"] = 0.0001
    elif fault == "truncated":
        audit["measurements"]["query_details_truncated"] = True
    else:
        observer = acceptance.AcceptanceObserver()
        observer.phase = "ready_audit"
        observer.record_sql_operation("connection", 0.001, "connect", 0)
        audit["measurements"] = observer.report()
    with pytest.raises(ValueError):
        acceptance.assess_ready_audit(audit, retained_galleries=1, pages=1)


def test_audit_violation_is_independent_and_cannot_hide_behind_pipeline_success(
    acceptance: ModuleType,
) -> None:
    case: dict[str, Any] = {
        "kind": "append",
        "galleries": 1,
        "batch": 1,
        "turns": [{"selected": 1, "added": 1, "acceptance": {"status": "satisfied"}}],
        "audit_acceptance": {"status": "violated"},
    }
    assert acceptance.acceptance_status([case], scope="pipeline") == "satisfied"
    assert acceptance.acceptance_status([case], scope="ready_audit") == "violated"
    assert acceptance.acceptance_status([case]) == "violated"
    del case["audit_acceptance"]
    assert acceptance.acceptance_status([case]) == "incomplete"


@pytest.mark.parametrize("pages", [0, 1, 63, 64, 65, 127, 128, 129, 255, 256, 257, 512])
def test_fixed_budgets_have_independent_boundaries(
    acceptance: ModuleType, pages: int
) -> None:
    files = pages + 1
    assert acceptance.file_page_budget(3, pages) == 3 * (
        8 * files + 256 * ((files + 127) // 128)
    )
    for multiplier in (1, 4):
        rows = multiplier * files
        expected = 2 * ((rows + 63) // 64) + 16 * ((rows + 255) // 256 + 1)
        assert acceptance.retirement_budget(rows) == expected
        assert (
            acceptance.check("boundary", expected, expected, "fixed")["status"]
            == "satisfied"
        )
        assert (
            acceptance.check("above", expected + 1, expected, "fixed")["status"]
            == "violated"
        )


def test_history_does_not_expand_budget_and_shared_slowdown_is_rejected(
    acceptance: ModuleType,
) -> None:
    small = _turn(acceptance)
    large = deepcopy(small)
    large["selected"] = 132046
    first = acceptance.assess_turn(small, pages=1)
    second = acceptance.assess_turn(large, pages=1)
    assert first["checks"] == second["checks"]
    assert first["status"] == "satisfied"
    degraded = acceptance.assess_turn(_turn(acceptance, calls=10000), pages=1)
    assert degraded["status"] == "violated"
    assert any(
        row["name"] == "claim.sql_calls" and row["status"] == "violated"
        for row in degraded["checks"]
    )


@pytest.mark.parametrize(
    "fault",
    [
        "missing_phase",
        "file_commit_missing",
        "negative_seconds",
        "nan_seconds",
        "truncated",
        "omitted",
    ],
)
def test_incomplete_measurements_cannot_pass(
    acceptance: ModuleType, fault: str
) -> None:
    turn = _turn(acceptance)
    measurements = turn["measurements"]
    if fault == "missing_phase":
        for row in measurements["queries"]:
            if row["pipeline"] == "cleanup":
                row["pipeline"] = "claim"
                row["operation"] = "second"
    elif fault == "file_commit_missing":
        for row in measurements["queries"]:
            if row["pipeline"] == "source":
                row["operation"] = "OTHER"
    elif fault == "negative_seconds":
        turn["phases"]["source"] = -1
    elif fault == "nan_seconds":
        turn["phases"]["source"] = float("nan")
    elif fault == "truncated":
        measurements["query_details_truncated"] = True
    else:
        measurements["omitted_query_events"] = 1
    with pytest.raises(ValueError):
        acceptance.assess_turn(turn, pages=1)


def test_exact_observer_rejects_overflow_and_lost_query_group(
    acceptance: ModuleType,
) -> None:
    observer = acceptance.AcceptanceObserver()
    observer.query_budget = 1
    observer.record_sql_operation("sql", 0.01, "SELECT 1", 1)
    observer.record_sql_operation("sql", 0.01, "SELECT 2", 1)
    with pytest.raises(RuntimeError, match="invalid"):
        observer.report()
    observer = acceptance.AcceptanceObserver()
    observer.record_sql_operation("sql", 0.01, "SELECT 1", 1)
    observer.queries.clear()
    with pytest.raises(ValueError, match="event ledger"):
        observer.report()


@pytest.mark.parametrize("value", ["", "1", "1:2:3", "-1:1", "513:1", "1:0", "1:9"])
def test_replacement_dimensions_are_bounded(acceptance: ModuleType, value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        acceptance.parse_replacement(value)


def test_missing_turn_or_retirement_acceptance_is_incomplete(
    acceptance: ModuleType,
) -> None:
    passed = {"status": "satisfied"}
    append: dict[str, Any] = {
        "kind": "append",
        "galleries": 2,
        "batch": 1,
        "audit_acceptance": passed,
        "turns": [
            {"selected": 1, "added": 1, "acceptance": passed},
            {"selected": 2, "added": 1},
        ],
    }
    assert acceptance.acceptance_status([append]) == "incomplete"
    replacement: dict[str, Any] = {
        "kind": "replacement",
        "replacement_cycles": 1,
        "turns": [
            {"cycle": 0, "acceptance": passed, "audit_acceptance": passed},
            {"cycle": 1, "acceptance": passed, "audit_acceptance": passed},
        ],
    }
    assert acceptance.acceptance_status([replacement]) == "incomplete"
    replacement["turns"][1]["retirement_acceptance"] = passed
    assert acceptance.acceptance_status([replacement]) == "satisfied"
    replacement["turns"].pop()
    assert acceptance.acceptance_status([replacement]) == "incomplete"


@pytest.mark.parametrize(
    ("status", "exit_code"), [("satisfied", 0), ("violated", 1), ("incomplete", 2)]
)
@pytest.mark.parametrize("failing_scope", ["pipeline", "ready_audit"])
def test_cli_never_converts_violation_or_incomplete_to_success(
    acceptance: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    status: str,
    exit_code: int,
    failing_scope: str,
) -> None:
    output = tmp_path / "report.json"
    monkeypatch.setattr(
        sys, "argv", ["acceptance", "--case", "1:1:1", "--output", str(output)]
    )
    monkeypatch.setattr(acceptance.probe, "source_provenance", dict)
    monkeypatch.setattr(acceptance, "source_hashes", dict)
    monkeypatch.setattr(acceptance, "imported_sources", dict)
    monkeypatch.setattr(acceptance, "fresh_runtime_evidence", dict)
    monkeypatch.setattr(
        acceptance.batch_probe,
        "run_case",
        lambda *_args, **_kwargs: {
            "galleries": 1,
            "batch": 1,
            "ready_audit": _audit(acceptance),
            "turns": [_turn(acceptance)],
        },
    )
    monkeypatch.setattr(
        acceptance,
        "assess_turn",
        lambda *_args, **_kwargs: {
            "status": status if failing_scope == "pipeline" else "satisfied"
        },
    )
    monkeypatch.setattr(
        acceptance,
        "assess_ready_audit",
        lambda *_args, **_kwargs: {
            "status": status if failing_scope == "ready_audit" else "satisfied"
        },
    )
    assert acceptance.main() == exit_code
    report = json.loads(output.read_text())
    assert report["acceptance"]["status"] == status
    assert report["acceptance"][failing_scope + "_status"] == status


def test_cli_requires_mariadb_opt_in(
    acceptance: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "acceptance",
            "--backend",
            "mariadb",
            "--case",
            "1:1:1",
            "--output",
            str(tmp_path / "r.json"),
        ],
    )
    with pytest.raises(SystemExit) as result:
        acceptance.main()
    assert result.value.code == 2


@pytest.mark.parametrize("failure_at", [1, 2, 3])
@pytest.mark.parametrize("error_number", [errno.ENOSPC, errno.EACCES, errno.EISDIR])
@pytest.mark.parametrize("persistent", [False, True])
def test_report_io_failure_is_incomplete_and_preserves_last_atomic_output(
    acceptance: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    failure_at: int,
    error_number: int,
    persistent: bool,
) -> None:
    output = tmp_path / "report.json"
    output.write_text("previous evidence")
    monkeypatch.setattr(
        sys, "argv", ["acceptance", "--case", "1:1:1", "--output", str(output)]
    )
    monkeypatch.setattr(acceptance, "fresh_runtime_evidence", dict)
    monkeypatch.setattr(acceptance.probe, "source_provenance", dict)
    monkeypatch.setattr(acceptance, "source_hashes", dict)
    monkeypatch.setattr(acceptance, "imported_sources", dict)
    monkeypatch.setattr(
        acceptance.batch_probe,
        "run_case",
        lambda *_args, **_kwargs: {
            "galleries": 1,
            "batch": 1,
            "ready_audit": _audit(acceptance),
            "turns": [_turn(acceptance)],
        },
    )
    monkeypatch.setattr(
        acceptance, "assess_turn", lambda *_args, **_kwargs: {"status": "violated"}
    )
    original = acceptance.probe.write_report
    writes = 0

    def fail_output(path: Path, report: dict[str, Any]) -> None:
        nonlocal writes
        writes += 1
        if writes == failure_at or (persistent and writes > failure_at):
            raise OSError(error_number, "injected output failure")
        original(path, report)

    monkeypatch.setattr(acceptance.probe, "write_report", fail_output)
    assert acceptance.main() == 2
    assert "acceptance incomplete" in capsys.readouterr().err
    if failure_at == 1:
        assert output.read_text() == "previous evidence"
    else:
        saved = json.loads(output.read_text())
        assert saved["status"] == "error"
        assert saved["acceptance"]["status"] == "incomplete"
        assert len(saved["cases"]) == (1 if failure_at == 3 or not persistent else 0)


def test_destination_changed_to_directory_is_execution_failure(
    acceptance: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output = tmp_path / "report.json"
    monkeypatch.setattr(
        sys, "argv", ["acceptance", "--case", "1:1:1", "--output", str(output)]
    )
    monkeypatch.setattr(acceptance, "fresh_runtime_evidence", dict)
    monkeypatch.setattr(acceptance.probe, "source_provenance", dict)
    monkeypatch.setattr(acceptance, "source_hashes", dict)
    monkeypatch.setattr(acceptance, "imported_sources", dict)

    def changed_destination(*_args: object, **_kwargs: object) -> dict[str, Any]:
        output.unlink()
        output.mkdir()
        (output / "concurrent-owner").write_text("preserve")
        return {
            "galleries": 1,
            "batch": 1,
            "ready_audit": _audit(acceptance),
            "turns": [_turn(acceptance)],
        }

    monkeypatch.setattr(acceptance.batch_probe, "run_case", changed_destination)
    assert acceptance.main() == 2
    assert (output / "concurrent-owner").read_text() == "preserve"
    assert not list(tmp_path.glob(".pipeline-report-*"))


def test_fresh_worker_ignores_stale_same_size_same_mtime_bytecode(
    acceptance: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = tmp_path / "stale_fixture.py"
    module.write_text("VALUE = 'old'\n")
    original_stat = module.stat()
    py_compile.compile(str(module), doraise=True)
    module.write_text("VALUE = 'new'\n")
    os.utime(module, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
    old = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; sys.pycache_prefix = None; import stale_fixture; print(stale_fixture.VALUE)",
        ],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )
    assert old.stdout.strip() == "old"
    evidence = tmp_path / "loaded-value.json"
    worker = tmp_path / "worker.py"
    worker.write_text(
        "import json, sys\nfrom pathlib import Path\nimport stale_fixture\nPath(sys.argv[2]).write_text(json.dumps({'value': stale_fixture.VALUE, 'cache': sys.pycache_prefix}))\n"
    )
    monkeypatch.setattr(acceptance, "__file__", str(worker))
    assert acceptance.launch_fresh_worker([str(evidence)]) == 0
    measured = json.loads(evidence.read_text())
    assert measured["value"] == "new"
    assert not Path(measured["cache"]).exists()


@pytest.mark.parametrize("directory", ["src", "tests", "scripts"])
def test_worker_rejects_prepopulated_checkout_caches(
    acceptance: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    directory: str,
) -> None:
    monkeypatch.setattr(sys, "pycache_prefix", str(tmp_path))
    acceptance._validate_empty_checkout_cache()
    cache = Path(
        importlib.util.cache_from_source(
            str(acceptance.ROOT / directory / "fixture.py")
        )
    )
    cache.parent.mkdir(parents=True)
    cache.write_bytes(b"stale")
    with pytest.raises(RuntimeError, match="prepopulated"):
        acceptance._validate_empty_checkout_cache()


def test_supervisor_spawn_failure_is_incomplete(
    acceptance: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def failed_spawn(*_args: object, **_kwargs: object) -> None:
        raise PermissionError("cannot start interpreter")

    monkeypatch.setattr(acceptance.subprocess, "Popen", failed_spawn)
    assert acceptance.launch_fresh_worker([]) == 2
    assert "cannot start interpreter" in capsys.readouterr().err


def test_real_cli_help_runs_with_fresh_imports(acceptance: ModuleType) -> None:
    assert acceptance.__file__ is not None
    result = subprocess.run(
        [sys.executable, acceptance.__file__, "--help"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert "--replacement-case" in result.stdout


def test_real_cli_initial_output_error_returns_incomplete(
    acceptance: ModuleType,
    tmp_path: Path,
) -> None:
    assert acceptance.__file__ is not None
    result = subprocess.run(
        [
            sys.executable,
            acceptance.__file__,
            "--case",
            "1:1:1",
            "--output",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 2
    assert "initial report" in result.stderr
    assert tmp_path.is_dir()


def test_worker_startup_exception_does_not_look_like_cost_violation(
    acceptance: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    worker = tmp_path / "worker.py"
    worker.write_text(
        "raise RuntimeError('startup failed before an acceptance result')\n"
    )
    monkeypatch.setattr(acceptance, "__file__", str(worker))
    assert acceptance.launch_fresh_worker([]) == 2


@pytest.mark.skipif(os.name != "posix", reason="POSIX SIGTERM ownership regression")
def test_supervisor_sigterm_waits_for_child_and_removes_private_cache(
    acceptance: ModuleType,
    tmp_path: Path,
) -> None:
    worker = tmp_path / "sleeper.py"
    evidence = tmp_path / "worker.json"
    worker.write_text(
        "import json, os, sys, time\nfrom pathlib import Path\nPath(sys.argv[2]).write_text(json.dumps({'pid': os.getpid(), 'cache': sys.pycache_prefix}))\ntime.sleep(60)\n"
    )
    launcher = tmp_path / "launcher.py"
    launcher.write_text(
        "import importlib.util, sys\n"
        f"spec = importlib.util.spec_from_file_location('acceptance_signal_test', {acceptance.__file__!r})\n"
        "module = importlib.util.module_from_spec(spec)\nsys.modules[spec.name] = module\nspec.loader.exec_module(module)\n"
        f"module.__file__ = {str(worker)!r}\n"
        f"raise SystemExit(module.launch_fresh_worker([{str(evidence)!r}]))\n"
    )
    parent = subprocess.Popen(
        [sys.executable, str(launcher)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 15
        while not evidence.exists() and time.monotonic() < deadline:
            assert parent.poll() is None
            time.sleep(0.02)
        assert evidence.exists()
        child = json.loads(evidence.read_text())
        os.kill(parent.pid, signal.SIGTERM)
        _out, error = parent.communicate(timeout=10)
        assert parent.returncode == 2, error
        with pytest.raises(ProcessLookupError):
            os.kill(child["pid"], 0)
        assert not Path(child["cache"]).exists()
    finally:
        if parent.poll() is None:
            parent.kill()
            parent.wait()


def test_provenance_rejects_already_imported_foreign_package(
    acceptance: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import h2hdb

    evidence = acceptance.imported_sources()
    assert evidence["h2hdb"]["path"] == str(Path(h2hdb.__file__).resolve())
    assert len(evidence["h2hdb"]["sha256"]) == 64
    monkeypatch.setattr(h2hdb, "__file__", str(tmp_path / "h2hdb" / "__init__.py"))
    with pytest.raises(RuntimeError, match="not from the measured checkout"):
        acceptance.imported_sources()


def test_progress_never_duplicates_full_query_details(
    acceptance: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    acceptance.print_progress(
        {
            "event": "turn_completed",
            "selected": 3,
            "added": 1,
            "cleanup": "DONE",
            "sql_calls": 123,
            "sql_seconds": 0.2,
            "measurements": {"queries": [{"sql": "SELECT private_evidence"}]},
        }
    )
    result = json.loads(capsys.readouterr().out)
    assert result == {
        "event": "turn_completed",
        "selected": 3,
        "added": 1,
        "cleanup": "DONE",
        "sql_calls": 123,
        "sql_seconds": 0.2,
    }


@pytest.mark.deep
def test_real_metadata_only_replacement_retires_exact_facts(
    acceptance: ModuleType,
) -> None:
    result = acceptance.run_replacement("sqlite", 0, 1)
    assert len(result["turns"]) == 2
    for turn in result["turns"]:
        assert turn["cleanup"] == "DONE"
        assert turn["full_ready_audit"] == "passed"
        assert turn["audit_acceptance"]["status"] == "satisfied"
        assert turn["audit_acceptance"]["dimensions"]["retained_source_files"] == 1
        assert turn["next_claim"] == "passed"
    retired = result["turns"][1]["retirement_acceptance"]["checks"]
    assert [row["retired_rows"] for row in retired] == [1, 4, 1]


@pytest.mark.deep
@pytest.mark.parametrize("scalar_cleanup", [False, True])
def test_real_replacement_rejects_scalar_cleanup_but_accepts_batched_cost(
    acceptance: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    scalar_cleanup: bool,
) -> None:
    if scalar_cleanup:
        # Select the existing correct scalar implementation for the three
        # measured phases. Their facts/oracle remain exact, while real SQL work
        # deliberately loses batching; no fabricated call counts are supplied.
        cleanup = acceptance.cleanup
        plan = cleanup._STATIC_PLANS[cleanup.CleanupTargetKind.GALLERY_OBSERVATION]
        for phase in acceptance.GO_FILE_MULTIPLICITY:
            monkeypatch.setitem(
                plan.phases,
                phase,
                tuple(
                    replace(spec, batch_exact_primary_keys=False)
                    for spec in plan.phases[phase]
                ),
            )
    result = acceptance.run_replacement("sqlite", 64, 3)
    assert len(result["turns"]) == 4
    for turn in result["turns"]:
        assert turn["cleanup"] == "DONE"
        assert turn["full_ready_audit"] == "passed"
        assert turn["audit_acceptance"]["dimensions"]["retained_source_files"] == 65
        assert turn["next_claim"] == "passed"
        if turn["cycle"]:
            assert turn["retirement_acceptance"]["status"] == (
                "violated" if scalar_cleanup else "satisfied"
            )
            assert all(
                row["retired_rows"] == row["expected_retired_rows"]
                for row in turn["retirement_acceptance"]["checks"]
            )


@pytest.mark.deep
def test_real_redundant_read_mutant_preserves_oracle_but_fails_fixed_cost(
    acceptance: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2hdb.sql_performance import _MeasuredConnector

    original = _MeasuredConnector.fetch_one

    def repeated_read(
        self: _MeasuredConnector, query: str, data: tuple[Any, ...] = ()
    ) -> tuple[Any, ...]:
        result = original(self, query, data)
        # Read-only constant work cannot change catalog facts or consume rows.
        # Every extra call traverses the actual instrumented connector.
        for _ in range(32):
            original(self, "SELECT 1")
        return result

    monkeypatch.setattr(_MeasuredConnector, "fetch_one", repeated_read)
    result = acceptance.batch_probe.run_case(
        "sqlite",
        1,
        1,
        1,
        query_limit=None,
        observer_factory=acceptance.AcceptanceObserver,
        check_next_claim=True,
    )
    assert result["full_ready_audit"] == "passed"
    assert result["turns"][0]["next_claim"] == "passed"
    assert acceptance.assess_turn(result["turns"][0], pages=1)["status"] == "violated"


@pytest.mark.deep
def test_real_full_audit_delay_mutant_preserves_ready_but_fails_cost(
    acceptance: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An injected clock advance models an extra minute spent in the actual
    # audit without sleeping in a correctness test. Every real validator still
    # executes; the test must not substitute a fake READY result or SQL ledger.
    original = acceptance.batch_probe.full_check
    clock_offset = 0.0
    original_clock = time.perf_counter

    def delayed_check(config: Any) -> Any:
        nonlocal clock_offset
        result = original(config)
        clock_offset += 61.0
        return result

    monkeypatch.setattr(acceptance.batch_probe, "full_check", delayed_check)
    monkeypatch.setattr(
        acceptance.batch_probe,
        "time",
        SimpleNamespace(perf_counter=lambda: original_clock() + clock_offset),
    )
    result = acceptance.batch_probe.run_case(
        "sqlite",
        1,
        1,
        1,
        query_limit=None,
        observer_factory=acceptance.AcceptanceObserver,
        check_next_claim=True,
    )
    assert result["full_ready_audit"] == "passed"
    assert result["ready_audit"]["measurements"]["sql_calls"] > 0
    assert set(result["turns"][0]["phases"]) == set(acceptance.PHASE_CEILINGS)
    assert all(
        row["pipeline"] != "ready_audit"
        for row in result["turns"][0]["measurements"]["queries"]
    )
    verdict = acceptance.assess_ready_audit(
        result["ready_audit"], retained_galleries=1, pages=1
    )
    assert verdict["status"] == "violated"
    assert verdict["attribution"]["status"] == "complete"
