from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import Mock, call

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "run-pytest.py"


def _load_module(name: str, path: Path) -> ModuleType:
    specification = importlib.util.spec_from_file_location(name, path)
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


runner = _load_module("h2hdb_pytest_budget_runner", RUNNER)


def test_merge_profile_has_one_aggregate_five_minute_budget() -> None:
    arguments = runner._arguments(["merge"])

    assert arguments.profile == "merge"
    assert arguments.budget_seconds is None
    assert runner.DEFAULT_MERGE_BUDGET_SECONDS == 300.0


def test_main_applies_default_budget_only_to_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_profile = Mock(return_value=0)
    monkeypatch.setattr(runner, "run_profile", run_profile)

    assert runner.main(["merge"]) == 0
    assert runner.main(["deep"]) == 0
    assert run_profile.call_args_list == [
        call("merge", budget_seconds=300.0),
        call("deep", budget_seconds=None),
    ]


def test_merge_profile_selectors_split_sqlite_from_mariadb_smoke() -> None:
    sqlite_phase, mariadb_phase = runner.MERGE_PHASES

    assert sqlite_phase.marker_expression == "not deep and not mariadb"
    assert sqlite_phase.worker_count == "auto"
    assert not sqlite_phase.mariadb_enabled
    assert mariadb_phase.marker_expression == "mariadb_smoke and mariadb and not deep"
    assert mariadb_phase.worker_count == "0"
    assert mariadb_phase.mariadb_enabled


def test_deep_profile_is_complete_unbounded_and_separate_from_merge() -> None:
    arguments = runner._arguments(["deep"])
    sqlite_phase, mariadb_phase = runner.DEEP_PHASES

    assert arguments.budget_seconds is None
    assert sqlite_phase.marker_expression == "not mariadb"
    assert mariadb_phase.marker_expression == "mariadb"
    assert not sqlite_phase.stop_after_first_failure
    assert not mariadb_phase.stop_after_first_failure


def test_phase_environment_isolates_backend_and_pytest_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("H2HDB_TEST_MARIADB", "1")
    monkeypatch.setenv("PYTEST_ADDOPTS", "--collect-only -qq")

    sqlite_environment = runner._phase_environment(runner.MERGE_PHASES[0])
    mariadb_environment = runner._phase_environment(runner.MERGE_PHASES[1])

    assert "H2HDB_TEST_MARIADB" not in sqlite_environment
    assert mariadb_environment["H2HDB_TEST_MARIADB"] == "1"
    assert "PYTEST_ADDOPTS" not in sqlite_environment
    assert "PYTEST_ADDOPTS" not in mariadb_environment
    assert sqlite_environment["PYTHONDONTWRITEBYTECODE"] == "1"


def test_phase_command_is_strict_bounded_and_single_worker_for_mariadb() -> None:
    sqlite_command = runner._pytest_command(runner.MERGE_PHASES[0])
    mariadb_command = runner._pytest_command(runner.MERGE_PHASES[1])

    assert sqlite_command[:3] == (sys.executable, "-m", "pytest")
    assert sqlite_command[3:5] == ("-o", "addopts=")
    assert "--strict-markers" in sqlite_command
    assert "--numprocesses=auto" in sqlite_command
    assert "--dist=loadgroup" in sqlite_command
    assert "--max-worker-restart=0" in sqlite_command
    assert "--maxfail=1" in sqlite_command
    assert "--numprocesses=0" in mariadb_command
    assert "--dist=loadgroup" not in mariadb_command


def test_deep_commands_clear_default_addopts() -> None:
    for phase in runner.DEEP_PHASES:
        command = runner._pytest_command(phase)

        assert command.count("-m") == 2
        assert command[3:5] == ("-o", "addopts=")
        assert "not deep" not in command


def test_start_phase_inherits_live_output_and_starts_posix_process_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock()
    popen = Mock(return_value=process)
    monkeypatch.setattr(runner.subprocess, "Popen", popen)
    monkeypatch.setattr(runner.os, "name", "posix")

    assert runner._start_phase(runner.MERGE_PHASES[0]) is process

    _, keywords = popen.call_args
    assert keywords["start_new_session"] is True
    assert "stdout" not in keywords
    assert "stderr" not in keywords


def test_windows_start_requires_a_real_new_process_group_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    popen = Mock()
    monkeypatch.setattr(runner.subprocess, "Popen", popen)
    monkeypatch.setattr(runner.os, "name", "nt")
    monkeypatch.delattr(runner.subprocess, "CREATE_NEW_PROCESS_GROUP", raising=False)

    with pytest.raises(RuntimeError, match="CREATE_NEW_PROCESS_GROUP"):
        runner._start_phase(runner.MERGE_PHASES[0])
    popen.assert_not_called()


def test_windows_start_uses_new_process_group_and_inherits_live_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock()
    popen = Mock(return_value=process)
    monkeypatch.setattr(runner.subprocess, "Popen", popen)
    monkeypatch.setattr(
        runner.subprocess, "CREATE_NEW_PROCESS_GROUP", 512, raising=False
    )
    monkeypatch.setattr(runner.os, "name", "nt")

    assert runner._start_phase(runner.MERGE_PHASES[1]) is process

    _, keywords = popen.call_args
    assert keywords["creationflags"] == 512
    assert "start_new_session" not in keywords
    assert "stdout" not in keywords
    assert "stderr" not in keywords


@pytest.mark.parametrize(
    ("terminated", "expected"),
    (
        (True, runner.TIMEOUT_EXIT_CODE),
        (False, runner.TERMINATION_FAILED_EXIT_CODE),
    ),
)
def test_timeout_checks_process_group_termination(
    monkeypatch: pytest.MonkeyPatch,
    terminated: bool,
    expected: int,
) -> None:
    process = Mock()
    process.wait.side_effect = subprocess.TimeoutExpired(cmd="pytest", timeout=3.0)
    terminate = Mock(return_value=terminated)
    monotonic = Mock(side_effect=(100.0, 101.0, 102.0))
    monkeypatch.setattr(runner, "_start_phase", Mock(return_value=process))
    monkeypatch.setattr(runner, "terminate_process_group", terminate)
    monkeypatch.setattr(runner.time, "monotonic", monotonic)

    assert runner._run_phase(runner.MERGE_PHASES[0], hard_deadline=110.0) == expected
    process.wait.assert_called_once_with(timeout=3.0)
    terminate.assert_called_once_with(process, deadline=110.0)


def test_phase_wait_subtracts_popen_overhead_from_absolute_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock()
    process.wait.return_value = 0
    monotonic = Mock(side_effect=(100.0, 103.0, 103.5))
    monkeypatch.setattr(runner, "_start_phase", Mock(return_value=process))
    monkeypatch.setattr(runner.time, "monotonic", monotonic)

    assert runner._run_phase(runner.MERGE_PHASES[0], hard_deadline=110.0) == 0
    process.wait.assert_called_once_with(timeout=1.0)


def test_posix_termination_kills_surviving_group_after_leader_exits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock(pid=4312)
    process.poll.return_value = 0
    kill_process_group = Mock()
    wait_for_group = Mock(side_effect=(False, True))
    monkeypatch.setattr(runner.os, "killpg", kill_process_group)
    monkeypatch.setattr(runner.os, "name", "posix")
    monkeypatch.setattr(runner, "_wait_for_posix_process_group_exit", wait_for_group)

    assert runner.terminate_process_group(process, deadline=1000.0)

    assert kill_process_group.call_args_list == [
        call(4312, runner.signal.SIGTERM),
        call(4312, runner.signal.SIGKILL),
    ]
    assert wait_for_group.call_count == 2


def test_posix_group_probe_does_not_trust_an_exited_leader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock(pid=4312)
    process.poll.return_value = 0
    process.wait.return_value = 0
    kill_process_group = Mock(side_effect=(None, ProcessLookupError()))
    monotonic = Mock(side_effect=(1.0, 2.0))
    sleep = Mock()
    monkeypatch.setattr(runner.time, "monotonic", monotonic)
    monkeypatch.setattr(runner.time, "sleep", sleep)

    assert runner._wait_for_posix_process_group_exit(
        process,
        kill_process_group=kill_process_group,
        deadline=10.0,
    )
    assert kill_process_group.call_args_list == [call(4312, 0), call(4312, 0)]
    sleep.assert_called_once_with(runner.TERMINATION_POLL_SECONDS)
    process.wait.assert_called_once_with(timeout=8.0)


def test_posix_termination_reports_a_group_that_survives_sigkill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock(pid=4312)
    kill_process_group = Mock()
    wait_for_group = Mock(side_effect=(False, False))
    monkeypatch.setattr(runner.os, "killpg", kill_process_group)
    monkeypatch.setattr(runner.os, "name", "posix")
    monkeypatch.setattr(runner, "_wait_for_posix_process_group_exit", wait_for_group)

    assert not runner.terminate_process_group(process, deadline=1000.0)


def test_windows_taskkill_success_is_checked_and_leader_is_reaped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock(pid=4312)
    process.wait.return_value = 1
    taskkill = Mock(
        return_value=subprocess.CompletedProcess(
            args=(),
            returncode=0,
            stdout="SUCCESS",
            stderr="",
        )
    )
    monotonic = Mock(side_effect=(100.0, 101.0))
    monkeypatch.setattr(runner.os, "name", "nt")
    monkeypatch.setattr(runner.subprocess, "run", taskkill)
    monkeypatch.setattr(runner.time, "monotonic", monotonic)

    assert runner.terminate_process_group(process, deadline=110.0)
    taskkill.assert_called_once_with(
        ("taskkill", "/PID", "4312", "/T", "/F"),
        check=False,
        capture_output=True,
        text=True,
        timeout=runner.TERMINATION_GRACE_SECONDS,
    )
    process.wait.assert_called_once_with(timeout=9.0)


def test_windows_taskkill_nonzero_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock(pid=4312)
    taskkill = Mock(
        return_value=subprocess.CompletedProcess(
            args=(),
            returncode=1,
            stdout="",
            stderr="process tree not found",
        )
    )
    monkeypatch.setattr(runner.os, "name", "nt")
    monkeypatch.setattr(runner.subprocess, "run", taskkill)
    monkeypatch.setattr(runner.time, "monotonic", Mock(return_value=100.0))

    assert not runner.terminate_process_group(process, deadline=110.0)
    process.wait.assert_not_called()


def test_profile_uses_one_deadline_across_both_pytest_phases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic = Mock(side_effect=(100.0, 100.0, 225.0))
    observed: list[tuple[str, float | None]] = []

    def run_phase(phase: Any, *, hard_deadline: float | None) -> int:
        observed.append((phase.label, hard_deadline))
        return 0

    monkeypatch.setattr(runner.time, "monotonic", monotonic)
    monkeypatch.setattr(runner, "_run_phase", run_phase)

    assert runner.run_profile("merge", budget_seconds=300.0) == 0
    assert observed == [
        ("SQLite merge profile", 400.0),
        ("MariaDB 10.11.11 smoke profile", 400.0),
    ]


def test_profile_does_not_start_without_termination_reserve(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic = Mock(side_effect=(100.0, 395.0))
    run_phase = Mock()
    monkeypatch.setattr(runner.time, "monotonic", monotonic)
    monkeypatch.setattr(runner, "_run_phase", run_phase)

    assert runner.run_profile("merge", budget_seconds=300.0) == 124
    run_phase.assert_not_called()


def test_profile_does_not_start_second_phase_after_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_phase = Mock(return_value=7)
    monkeypatch.setattr(runner, "_run_phase", run_phase)

    assert runner.run_profile("merge", budget_seconds=None) == 7
    assert run_phase.call_count == 1


def test_full_gate_uses_merge_runner_and_manual_deep_entry_is_not_in_gate() -> None:
    full_gate = (ROOT / "scripts" / "check-full.sh").read_text(encoding="utf-8")
    deep_gate = (ROOT / "scripts" / "check-pytest-deep.sh").read_text(encoding="utf-8")

    assert ".venv/bin/python scripts/run-pytest.py merge" in full_gate
    assert "check-pytest-deep.sh" not in full_gate
    assert "scripts/run-pytest.py deep" not in full_gate
    assert ".venv/bin/pytest" not in full_gate
    assert "scripts/run-pytest.py deep" in deep_gate


def test_release_receipt_names_the_bounded_merge_evidence() -> None:
    release_gate = _load_module(
        "h2hdb_pytest_budget_release_gate",
        ROOT / "scripts" / "release-gate.py",
    )

    assert release_gate.RELEASE_PROFILE == "h2hdb-release-v3"
    assert "sqlite-merge-tests-parallel" in release_gate.REQUIRED_CHECKS
    assert "mariadb-10.11.11-smoke-single-worker" in release_gate.REQUIRED_CHECKS
    assert "pytest-total-budget-300s" in release_gate.REQUIRED_CHECKS
