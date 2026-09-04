from __future__ import annotations

import ctypes
import importlib.util
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest

pytestmark = pytest.mark.skipif(os.name != "nt", reason="requires Windows Job Objects")

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "run-pytest.py"
_PROCESS_WAIT_SECONDS = 15.0
_CHILD_SLEEP_SECONDS = 60


def _load_runner(name: str) -> ModuleType:
    specification = importlib.util.spec_from_file_location(name, RUNNER)
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


def _tree_command(pid_path: Path, *, leader_exits: bool) -> tuple[str, ...]:
    script = (
        "import os,pathlib,subprocess,sys,time;"
        "child=subprocess.Popen((sys.executable,'-c',"
        f"'import time;time.sleep({_CHILD_SLEEP_SECONDS})'),"
        "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,"
        "stderr=subprocess.DEVNULL);"
        "pathlib.Path(sys.argv[1]).write_text(str(child.pid),encoding='ascii');"
        + ("os._exit(0)" if leader_exits else f"time.sleep({_CHILD_SLEEP_SECONDS})")
    )
    return sys.executable, "-c", script, str(pid_path)


def _process_is_running(pid: int) -> bool:
    if os.name != "nt":
        return False
    from ctypes import wintypes

    win_dll = getattr(ctypes, "WinDLL", None)
    assert callable(win_dll)
    kernel32: Any = win_dll("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenProcess(0x00100000, False, pid)
    if not handle:
        return False
    try:
        return int(kernel32.WaitForSingleObject(handle, 0)) == 0x00000102
    finally:
        kernel32.CloseHandle(handle)


def _wait_for_path(path: Path) -> None:
    deadline = time.monotonic() + _PROCESS_WAIT_SECONDS
    while not path.is_file():
        if time.monotonic() >= deadline:
            pytest.fail(f"process fixture did not publish {path}")
        time.sleep(0.02)


def _wait_for_process_exit(pid: int) -> None:
    deadline = time.monotonic() + _PROCESS_WAIT_SECONDS
    while _process_is_running(pid):
        if time.monotonic() >= deadline:
            pytest.fail(f"process {pid} survived its Windows Job owner")
        time.sleep(0.02)


def _runner_harness(pid_path: Path) -> str:
    command = repr(_tree_command(pid_path, leader_exits=False))
    return f"""
import importlib.util
import sys

path = {str(RUNNER)!r}
spec = importlib.util.spec_from_file_location("windows_runner_harness", path)
assert spec is not None and spec.loader is not None
runner = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = runner
spec.loader.exec_module(runner)
runner._phases = lambda _profile: (runner.MERGE_PHASES[0],)
runner._pytest_command = lambda _phase: {command}
raise SystemExit(runner.main(["merge", "--budget-seconds", "20"]))
"""


def _force_tree_cleanup(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    subprocess.run(
        ("taskkill", "/PID", str(process.pid), "/T", "/F"),
        check=False,
        capture_output=True,
        timeout=5,
    )
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _force_pid_cleanup(pid: int | None) -> None:
    if pid is None or not _process_is_running(pid):
        return
    subprocess.run(
        ("taskkill", "/PID", str(pid), "/T", "/F"),
        check=False,
        capture_output=True,
        timeout=5,
    )


def test_windows_timeout_terminates_parent_and_descendant(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner("windows_runner_timeout")
    pid_path = tmp_path / "timeout-child.pid"
    child_pid: int | None = None
    monkeypatch.setattr(runner, "_phases", lambda _profile: (runner.MERGE_PHASES[0],))
    monkeypatch.setattr(
        runner,
        "_pytest_command",
        lambda _phase: _tree_command(pid_path, leader_exits=False),
    )

    try:
        assert (
            runner.run_profile("merge", budget_seconds=8.0) == runner.TIMEOUT_EXIT_CODE
        )
        _wait_for_path(pid_path)
        child_pid = int(pid_path.read_text(encoding="ascii"))
        _wait_for_process_exit(child_pid)
    finally:
        _force_pid_cleanup(child_pid)


def test_windows_normal_leader_exit_cleans_descendant_and_fails_gate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner("windows_runner_survivor")
    pid_path = tmp_path / "survivor-child.pid"
    child_pid: int | None = None
    monkeypatch.setattr(runner, "_phases", lambda _profile: (runner.MERGE_PHASES[0],))
    monkeypatch.setattr(
        runner,
        "_pytest_command",
        lambda _phase: _tree_command(pid_path, leader_exits=True),
    )

    try:
        assert (
            runner.main(["merge", "--budget-seconds", "10"])
            == runner.TERMINATION_FAILED_EXIT_CODE
        )
        _wait_for_path(pid_path)
        child_pid = int(pid_path.read_text(encoding="ascii"))
        _wait_for_process_exit(child_pid)
    finally:
        _force_pid_cleanup(child_pid)


def test_windows_ctrl_break_cleans_owned_tree(tmp_path: Path) -> None:
    pid_path = tmp_path / "break-child.pid"
    child_pid: int | None = None
    creation_flag = cast(int, cast(Any, subprocess).CREATE_NEW_PROCESS_GROUP)
    ctrl_break = cast(int, cast(Any, signal).CTRL_BREAK_EVENT)
    sigbreak = cast(int, cast(Any, signal).SIGBREAK)
    process = subprocess.Popen(
        (sys.executable, "-c", _runner_harness(pid_path)),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=creation_flag,
    )
    try:
        _wait_for_path(pid_path)
        child_pid = int(pid_path.read_text(encoding="ascii"))
        process.send_signal(ctrl_break)
        assert process.wait(timeout=_PROCESS_WAIT_SECONDS) == 128 + sigbreak
        _wait_for_process_exit(child_pid)
    finally:
        _force_tree_cleanup(process)
        _force_pid_cleanup(child_pid)


def test_windows_forced_runner_exit_uses_kill_on_job_close(tmp_path: Path) -> None:
    pid_path = tmp_path / "forced-child.pid"
    child_pid: int | None = None
    creation_flag = cast(int, cast(Any, subprocess).CREATE_NEW_PROCESS_GROUP)
    process = subprocess.Popen(
        (sys.executable, "-c", _runner_harness(pid_path)),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=creation_flag,
    )
    try:
        _wait_for_path(pid_path)
        child_pid = int(pid_path.read_text(encoding="ascii"))
        process.terminate()
        process.wait(timeout=_PROCESS_WAIT_SECONDS)
        _wait_for_process_exit(child_pid)
    finally:
        _force_tree_cleanup(process)
        _force_pid_cleanup(child_pid)


def test_windows_real_venv_redirector_keeps_job_ownership(tmp_path: Path) -> None:
    venv = tmp_path / "venv"
    subprocess.run(
        (sys.executable, "-m", "venv", "--without-pip", str(venv)),
        check=True,
        timeout=60,
    )
    venv_python = venv / "Scripts" / "python.exe"
    harness = f"""
import importlib.util
import sys

assert sys.executable.lower() != sys._base_executable.lower()
spec = importlib.util.spec_from_file_location("windows_venv_runner", {str(RUNNER)!r})
assert spec is not None and spec.loader is not None
runner = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = runner
spec.loader.exec_module(runner)
runner._phases = lambda _profile: (runner.MERGE_PHASES[0],)
runner._pytest_command = lambda _phase: (
    sys.executable,
    "-c",
    "import sys; assert sys.prefix != sys.base_prefix",
)
raise SystemExit(runner.main(["merge", "--budget-seconds", "10"]))
"""

    completed = subprocess.run(
        (str(venv_python), "-c", harness),
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, (
        f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
    )


def test_windows_failed_first_phase_never_starts_second_phase(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner("windows_runner_phase_barrier")
    pid_path = tmp_path / "phase-one-child.pid"
    second_phase_marker = tmp_path / "phase-two-started"
    child_pid: int | None = None
    first_phase, second_phase = runner.MERGE_PHASES

    def command(phase: object) -> tuple[str, ...]:
        if phase is first_phase:
            return _tree_command(pid_path, leader_exits=True)
        return (
            sys.executable,
            "-c",
            "import pathlib,sys;pathlib.Path(sys.argv[1]).touch()",
            str(second_phase_marker),
        )

    monkeypatch.setattr(runner, "_pytest_command", command)
    try:
        assert (
            runner.main(["merge", "--budget-seconds", "15"])
            == runner.TERMINATION_FAILED_EXIT_CODE
        )
        _wait_for_path(pid_path)
        child_pid = int(pid_path.read_text(encoding="ascii"))
        _wait_for_process_exit(child_pid)
        assert not second_phase_marker.exists()
        assert (first_phase, second_phase) == runner.MERGE_PHASES
    finally:
        _force_pid_cleanup(child_pid)
