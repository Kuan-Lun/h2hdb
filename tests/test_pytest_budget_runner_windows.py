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
import pytest_process_pid
from pytest_process_pid import publish_pid, wait_for_pid

requires_windows = pytest.mark.skipif(
    os.name != "nt", reason="requires Windows Job Objects"
)

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
        f"sys.path.insert(0,{str(Path(__file__).resolve().parent)!r});"
        "from pytest_process_pid import publish_pid;"
        "child=subprocess.Popen((sys.executable,'-c',"
        f"'import time;time.sleep({_CHILD_SLEEP_SECONDS})'),"
        "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,"
        "stderr=subprocess.DEVNULL);"
        "publish_pid(pathlib.Path(sys.argv[1]),child.pid);"
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


@pytest.mark.parametrize("partial", ("", "123"), ids=("empty", "valid-prefix"))
def test_pid_publication_hides_incomplete_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    partial: str,
) -> None:
    pid_path = tmp_path / "child.pid"
    expected_pid = 12345

    def staged_write(path: Path, payload: str, *, encoding: str) -> int:
        with path.open("w", encoding=encoding) as stream:
            stream.write(partial)
            stream.flush()
            assert path.read_text(encoding=encoding) == partial
            # Even a prefix that int() would accept must remain unpublished.
            with pytest.raises(TimeoutError, match="did not publish"):
                wait_for_pid(pid_path, timeout=0)
            stream.write(payload[len(partial) :])
            stream.flush()
            assert not pid_path.exists()
        return len(payload)

    monkeypatch.setattr(Path, "write_text", staged_write)

    publish_pid(pid_path, expected_pid)

    assert wait_for_pid(pid_path, timeout=0) == expected_pid
    assert list(tmp_path.iterdir()) == [pid_path]


def test_pid_publication_failure_does_not_publish_partial_data(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pid_path = tmp_path / "child.pid"

    def failing_write(path: Path, payload: str, *, encoding: str) -> int:
        with path.open("w", encoding=encoding) as stream:
            stream.write(payload[:2])
            stream.flush()
            raise OSError("injected partial write failure")

    monkeypatch.setattr(Path, "write_text", failing_write)

    with pytest.raises(OSError, match="injected partial write failure"):
        publish_pid(pid_path, 12345)

    assert not pid_path.exists()
    assert not list(tmp_path.iterdir())


def test_pid_publication_rename_failure_cleans_temporary_data(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pid_path = tmp_path / "child.pid"

    def failing_replace(path: Path, target: Path) -> Path:
        assert path.read_text(encoding="ascii") == "12345"
        assert target == pid_path
        raise PermissionError("injected rename failure")

    monkeypatch.setattr(Path, "replace", failing_replace)

    with pytest.raises(PermissionError, match="injected rename failure"):
        publish_pid(pid_path, 12345)

    assert not pid_path.exists()
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("pid", (0, -1, True))
def test_pid_publication_requires_positive_integer(tmp_path: Path, pid: int) -> None:
    with pytest.raises(ValueError, match="positive PID"):
        publish_pid(tmp_path / "child.pid", pid)
    assert not list(tmp_path.iterdir())


def test_pid_wait_reads_complete_publication_after_missing_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pid_path = tmp_path / "child.pid"
    pauses: list[float] = []

    def publish_on_poll(delay: float) -> None:
        pauses.append(delay)
        publish_pid(pid_path, 12345)

    monkeypatch.setattr(pytest_process_pid, "monotonic", lambda: 0.0)
    monkeypatch.setattr(pytest_process_pid, "sleep", publish_on_poll)

    assert wait_for_pid(pid_path, timeout=1) == 12345
    assert pauses == [0.02]


def test_pid_publication_works_across_process_boundary(tmp_path: Path) -> None:
    pid_path = tmp_path / "subprocess.pid"
    command = (
        sys.executable,
        "-c",
        "import os,pathlib,sys;"
        "sys.path.insert(0,sys.argv[1]);"
        "from pytest_process_pid import publish_pid;"
        "publish_pid(pathlib.Path(sys.argv[2]),os.getpid());"
        "print(os.getpid())",
        str(Path(__file__).resolve().parent),
        str(pid_path),
    )

    completed = subprocess.run(
        command,
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
        timeout=_PROCESS_WAIT_SECONDS,
    )

    assert wait_for_pid(pid_path, timeout=0) == int(completed.stdout)
    assert list(tmp_path.iterdir()) == [pid_path]


def test_pid_wait_missing_publication_obeys_deadline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    now = 0.0
    pauses: list[float] = []

    def advance_clock(delay: float) -> None:
        nonlocal now
        pauses.append(delay)
        now += delay

    monkeypatch.setattr(pytest_process_pid, "monotonic", lambda: now)
    monkeypatch.setattr(pytest_process_pid, "sleep", advance_clock)

    with pytest.raises(TimeoutError, match="did not publish"):
        wait_for_pid(tmp_path / "missing.pid", timeout=0.03)

    assert pauses == pytest.approx([0.02, 0.01])
    assert now == pytest.approx(0.03)


@pytest.mark.parametrize(
    "payload", (b"", b"0", b"-1", b"+1", b"01", b"12\n", b"x", b"\xff")
)
def test_pid_wait_rejects_malformed_publication_without_retry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    payload: bytes,
) -> None:
    pid_path = tmp_path / "malformed.pid"
    pid_path.write_bytes(payload)

    def unexpected_retry(_delay: float) -> None:
        pytest.fail("malformed published data must fail without retrying")

    monkeypatch.setattr(pytest_process_pid, "sleep", unexpected_retry)

    with pytest.raises(ValueError):
        wait_for_pid(pid_path, timeout=1)


@requires_windows
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
        child_pid = wait_for_pid(pid_path, timeout=_PROCESS_WAIT_SECONDS)
        _wait_for_process_exit(child_pid)
    finally:
        _force_pid_cleanup(child_pid)


@requires_windows
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
        child_pid = wait_for_pid(pid_path, timeout=_PROCESS_WAIT_SECONDS)
        _wait_for_process_exit(child_pid)
    finally:
        _force_pid_cleanup(child_pid)


@requires_windows
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
        child_pid = wait_for_pid(pid_path, timeout=_PROCESS_WAIT_SECONDS)
        process.send_signal(ctrl_break)
        assert process.wait(timeout=_PROCESS_WAIT_SECONDS) == 128 + sigbreak
        _wait_for_process_exit(child_pid)
    finally:
        _force_tree_cleanup(process)
        _force_pid_cleanup(child_pid)


@requires_windows
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
        child_pid = wait_for_pid(pid_path, timeout=_PROCESS_WAIT_SECONDS)
        process.terminate()
        process.wait(timeout=_PROCESS_WAIT_SECONDS)
        _wait_for_process_exit(child_pid)
    finally:
        _force_tree_cleanup(process)
        _force_pid_cleanup(child_pid)


@requires_windows
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


@requires_windows
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
        child_pid = wait_for_pid(pid_path, timeout=_PROCESS_WAIT_SECONDS)
        _wait_for_process_exit(child_pid)
        assert not second_phase_marker.exists()
        assert (first_phase, second_phase) == runner.MERGE_PHASES
    finally:
        _force_pid_cleanup(child_pid)
