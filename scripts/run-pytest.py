#!/usr/bin/env python3
"""Run bounded merge/server-crash profiles or the manual deep profile."""

from __future__ import annotations

import argparse
import ctypes
import math
import ntpath
import os
import shlex
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Any, Final, Literal, cast

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MERGE_BUDGET_SECONDS = 300.0
TERMINATION_GRACE_SECONDS = 2.0
TERMINATION_RESERVE_SECONDS = 6.0
TERMINATION_POLL_SECONDS = 0.05
TIMEOUT_EXIT_CODE = 124
TERMINATION_FAILED_EXIT_CODE = 125
INTERRUPTED_EXIT_CODE = 130
_WINDOWS_SUPERVISOR_MODE: Final = "--internal-windows-supervisor"
_WINDOWS_START_TOKEN: Final = b"\x01"
ProfileName = Literal["merge", "deep", "mariadb-server-crash"]


class RunnerSignalInterrupt(BaseException):
    """A POSIX termination signal converted into controlled runner shutdown."""

    def __init__(self, signal_number: int) -> None:
        super().__init__(signal_number)
        self.signal_number = signal_number


class _ProcessOwnershipError(RuntimeError):
    """The runner could not verify that its managed pytest process tree exited."""


@dataclass
class _SignalController:
    """Defer asynchronous interruption only across ownership-critical regions."""

    pending_signal: int | None = None
    defer_depth: int = 0
    interruption_started: bool = False

    def receive(self, signal_number: int) -> None:
        if self.interruption_started:
            return
        if self.defer_depth:
            if self.pending_signal is None:
                self.pending_signal = signal_number
            return
        self._raise(signal_number)

    @contextmanager
    def defer(self) -> Iterator[None]:
        self.defer_depth += 1
        body_failed = True
        try:
            yield
            body_failed = False
        finally:
            self.defer_depth -= 1
            if self.defer_depth == 0 and not body_failed:
                self.raise_pending()

    def raise_pending(self) -> None:
        if self.pending_signal is None:
            return
        pending = self.pending_signal
        self.pending_signal = None
        self._raise(pending)

    def _raise(self, signal_number: int) -> None:
        self.interruption_started = True
        if signal_number == signal.SIGINT:
            raise KeyboardInterrupt
        raise RunnerSignalInterrupt(signal_number)


@contextmanager
def _controlled_termination_signals() -> Iterator[_SignalController]:
    """Control platform termination signals and restore prior handlers."""

    signal_numbers: tuple[int, ...]
    if os.name == "nt":
        break_signal = cast(int | None, getattr(signal, "SIGBREAK", None))
        if break_signal is None:
            raise RuntimeError("Windows requires SIGBREAK support")
        signal_numbers = (signal.SIGINT, break_signal)
    else:
        signal_numbers = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    previous_handlers = [
        (signal_number, signal.getsignal(signal_number))
        for signal_number in signal_numbers
    ]
    controller = _SignalController()

    def interrupt(signal_number: int, frame: FrameType | None) -> None:
        del frame
        controller.receive(signal_number)

    installed: list[int] = []
    try:
        for signal_number, _previous_handler in previous_handlers:
            signal.signal(signal_number, interrupt)
            installed.append(signal_number)
        yield controller
    finally:
        previous_by_signal = dict(previous_handlers)
        for signal_number in reversed(installed):
            signal.signal(signal_number, previous_by_signal[signal_number])


@contextmanager
def _defer_signal_interrupts(
    controller: _SignalController | None,
) -> Iterator[None]:
    if controller is None:
        yield
        return
    with controller.defer():
        yield


class _WindowsJob:
    """A non-inheritable Windows Job with kill-on-owner-close semantics."""

    _KILL_ON_JOB_CLOSE: Final = 0x00002000
    _EXTENDED_LIMIT_INFORMATION: Final = 9
    _BASIC_ACCOUNTING_INFORMATION: Final = 1

    def __init__(
        self,
        kernel32: Any,
        handle: int,
        accounting_information_type: type[ctypes.Structure],
    ) -> None:
        self._kernel32 = kernel32
        self._handle: int | None = handle
        self._accounting_information_type = accounting_information_type

    @classmethod
    def create(cls) -> _WindowsJob:
        if os.name != "nt":
            raise RuntimeError("Windows Job Objects are unavailable")
        from ctypes import wintypes

        class _IoCounters(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class _BasicLimitInformation(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class _ExtendedLimitInformation(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", _BasicLimitInformation),
                ("IoInfo", _IoCounters),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        class _BasicAccountingInformation(ctypes.Structure):
            _fields_ = [
                ("TotalUserTime", ctypes.c_longlong),
                ("TotalKernelTime", ctypes.c_longlong),
                ("ThisPeriodTotalUserTime", ctypes.c_longlong),
                ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
                ("TotalPageFaultCount", wintypes.DWORD),
                ("TotalProcesses", wintypes.DWORD),
                ("ActiveProcesses", wintypes.DWORD),
                ("TotalTerminatedProcesses", wintypes.DWORD),
            ]

        win_dll = getattr(ctypes, "WinDLL", None)
        if not callable(win_dll):
            raise RuntimeError("Windows Job Object APIs are unavailable")
        kernel32 = win_dll("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
        ]
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.AssignProcessToJobObject.argtypes = [
            wintypes.HANDLE,
            wintypes.HANDLE,
        ]
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
        kernel32.TerminateJobObject.restype = wintypes.BOOL
        kernel32.QueryInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
            ctypes.c_void_p,
        ]
        kernel32.QueryInformationJobObject.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL

        raw_handle = kernel32.CreateJobObjectW(None, None)
        if not raw_handle:
            raise cls._last_error("CreateJobObjectW")
        handle = int(raw_handle)
        information = _ExtendedLimitInformation()
        information.BasicLimitInformation.LimitFlags = cls._KILL_ON_JOB_CLOSE
        if not kernel32.SetInformationJobObject(
            handle,
            cls._EXTENDED_LIMIT_INFORMATION,
            ctypes.byref(information),
            ctypes.sizeof(information),
        ):
            error = cls._last_error("SetInformationJobObject")
            kernel32.CloseHandle(handle)
            raise error
        return cls(kernel32, handle, _BasicAccountingInformation)

    @staticmethod
    def _last_error(operation: str) -> OSError:
        get_last_error = cast(Any, getattr(ctypes, "get_last_error", lambda: 0))
        code = int(get_last_error())
        return OSError(code, f"{operation} failed with Windows error {code}")

    def assign(self, process: subprocess.Popen[bytes]) -> None:
        process_handle = getattr(process, "_handle", None)
        if process_handle is None:
            raise RuntimeError("Python did not expose the child process handle")
        if self._handle is None:
            raise RuntimeError("Windows Job Object is already closed")
        if not self._kernel32.AssignProcessToJobObject(
            self._handle,
            int(process_handle),
        ):
            raise self._last_error("AssignProcessToJobObject")

    def terminate(self) -> None:
        if self._handle is None:
            return
        if not self._kernel32.TerminateJobObject(self._handle, 1):
            raise self._last_error("TerminateJobObject")

    def active_processes(self) -> int:
        if self._handle is None:
            return 0
        information = self._accounting_information_type()
        if not self._kernel32.QueryInformationJobObject(
            self._handle,
            self._BASIC_ACCOUNTING_INFORMATION,
            ctypes.byref(information),
            ctypes.sizeof(information),
            None,
        ):
            raise self._last_error("QueryInformationJobObject")
        return int(information.ActiveProcesses)

    def wait_empty(self, *, deadline: float) -> bool:
        while self.active_processes() != 0:
            remaining_seconds = _seconds_remaining(deadline)
            if remaining_seconds == 0:
                return False
            time.sleep(min(TERMINATION_POLL_SECONDS, remaining_seconds))
        return True

    def close(self) -> None:
        handle = self._handle
        if handle is None:
            return
        if not self._kernel32.CloseHandle(handle):
            raise self._last_error("CloseHandle")
        self._handle = None


@dataclass(frozen=True)
class _OwnedPhaseProcess:
    process: subprocess.Popen[bytes]
    windows_job: _WindowsJob | None = None


@dataclass(frozen=True)
class PytestPhase:
    """One isolated pytest invocation within a named profile."""

    label: str
    marker_expression: str
    worker_count: str
    mariadb_enabled: bool
    durations: int
    stop_after_first_failure: bool


MERGE_PHASES = (
    PytestPhase(
        label="SQLite merge profile",
        marker_expression="not deep and not mariadb",
        worker_count="auto",
        mariadb_enabled=False,
        durations=20,
        stop_after_first_failure=True,
    ),
    PytestPhase(
        label="MariaDB 10.11.11 smoke profile",
        marker_expression="mariadb_smoke and mariadb and not deep",
        worker_count="0",
        mariadb_enabled=True,
        durations=20,
        stop_after_first_failure=True,
    ),
)

MARIADB_SERVER_CRASH_PHASE = PytestPhase(
    label="Disposable MariaDB 10.11.11 server SIGKILL profile",
    marker_expression="mariadb_server_crash",
    worker_count="0",
    mariadb_enabled=True,
    durations=10,
    stop_after_first_failure=True,
)

DEEP_PHASES = (
    PytestPhase(
        label="SQLite complete manual profile",
        marker_expression="not mariadb",
        worker_count="auto",
        mariadb_enabled=False,
        durations=50,
        stop_after_first_failure=False,
    ),
    PytestPhase(
        label="MariaDB 10.11.11 complete manual profile",
        marker_expression="mariadb and not mariadb_server_crash",
        worker_count="0",
        mariadb_enabled=True,
        durations=50,
        stop_after_first_failure=False,
    ),
    MARIADB_SERVER_CRASH_PHASE,
)


def _positive_seconds(value: str) -> float:
    try:
        seconds = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "must be a number greater than zero"
        ) from error
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("must be a finite number greater than zero")
    return seconds


def _phases(profile: ProfileName) -> tuple[PytestPhase, ...]:
    if profile == "merge":
        return MERGE_PHASES
    if profile == "mariadb-server-crash":
        return (MARIADB_SERVER_CRASH_PHASE,)
    return DEEP_PHASES


def _pytest_command(phase: PytestPhase) -> tuple[str, ...]:
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-o",
        "addopts=",
        "--strict-markers",
        "-m",
        phase.marker_expression,
        f"--numprocesses={phase.worker_count}",
        "--max-worker-restart=0",
        f"--durations={phase.durations}",
        "--tb=short",
        "-ra",
    ]
    if phase.worker_count != "0":
        command.append("--dist=loadgroup")
    if phase.stop_after_first_failure:
        command.append("--maxfail=1")
    return tuple(command)


def _phase_environment(phase: PytestPhase) -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment.pop("PYTEST_ADDOPTS", None)
    if phase.mariadb_enabled:
        environment["H2HDB_TEST_MARIADB"] = "1"
    else:
        environment.pop("H2HDB_TEST_MARIADB", None)
    return environment


def _start_phase(phase: PytestPhase) -> subprocess.Popen[bytes]:
    command = _pytest_command(phase)
    print("+", shlex.join(command), flush=True)
    return subprocess.Popen(  # noqa: S603 -- fixed interpreter and pytest CLI.
        command,
        cwd=REPOSITORY_ROOT,
        env=_phase_environment(phase),
        start_new_session=True,
    )


def _windows_supervisor_launch(phase: PytestPhase) -> tuple[str, dict[str, str]]:
    executable = sys.executable
    base_executable = getattr(sys, "_base_executable", None)
    if not isinstance(base_executable, str) or not base_executable:
        raise RuntimeError("Windows Python did not expose its base executable")
    environment = _phase_environment(phase)
    environment.pop("__PYVENV_LAUNCHER__", None)
    if ntpath.normcase(ntpath.normpath(base_executable)) != ntpath.normcase(
        ntpath.normpath(executable)
    ):
        if not Path(base_executable).is_file():
            raise RuntimeError("Windows Python base executable is unavailable")
        executable = base_executable
    return executable, environment


def _cleanup_windows_supervisor(
    process: subprocess.Popen[bytes],
    job: _WindowsJob,
    *,
    assigned: bool,
    deadline: float,
) -> bool:
    cleanup_succeeded = True
    if process.stdin is not None:
        try:
            process.stdin.close()
        except OSError:
            cleanup_succeeded = False
    if assigned:
        try:
            job.terminate()
        except OSError:
            cleanup_succeeded = False
        try:
            cleanup_succeeded = job.wait_empty(deadline=deadline) and cleanup_succeeded
        except OSError:
            cleanup_succeeded = False
    elif process.poll() is None:
        try:
            process.kill()
        except OSError:
            cleanup_succeeded = False
    try:
        process.wait(timeout=_seconds_remaining(deadline))
    except subprocess.TimeoutExpired:
        cleanup_succeeded = False
    try:
        job.close()
    except OSError:
        cleanup_succeeded = False
    return cleanup_succeeded and process.poll() is not None


def _start_windows_phase(
    phase: PytestPhase,
    *,
    deadline: float,
    signal_controller: _SignalController | None,
) -> _OwnedPhaseProcess:
    command = _pytest_command(phase)
    print("+", shlex.join(command), flush=True)
    creation_flag = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    if not isinstance(creation_flag, int) or creation_flag == 0:
        raise _ProcessOwnershipError(phase.label)
    job: _WindowsJob | None = None
    supervisor: subprocess.Popen[bytes] | None = None
    assigned = False
    cleanup_performed = False
    try:
        with _defer_signal_interrupts(signal_controller):
            try:
                job = _WindowsJob.create()
                executable, environment = _windows_supervisor_launch(phase)
                supervisor = subprocess.Popen(  # noqa: S603 -- fixed supervisor.
                    (
                        executable,
                        str(Path(__file__).resolve()),
                        _WINDOWS_SUPERVISOR_MODE,
                        *command,
                    ),
                    cwd=REPOSITORY_ROOT,
                    env=environment,
                    stdin=subprocess.PIPE,
                    creationflags=creation_flag,
                )
                job.assign(supervisor)
                assigned = True
                if signal_controller is not None:
                    signal_controller.raise_pending()
                if supervisor.stdin is None:
                    raise RuntimeError(
                        "Windows pytest supervisor start gate is unavailable"
                    )
                written = supervisor.stdin.write(_WINDOWS_START_TOKEN)
                if written != len(_WINDOWS_START_TOKEN):
                    raise BlockingIOError(
                        "Windows pytest supervisor start gate was partial"
                    )
                supervisor.stdin.close()
            except BaseException as startup_error:
                cleaned = True
                if job is None:
                    cleaned = supervisor is None
                elif supervisor is None:
                    try:
                        job.close()
                    except OSError:
                        cleaned = False
                else:
                    cleanup_performed = True
                    cleaned = _cleanup_windows_supervisor(
                        supervisor,
                        job,
                        assigned=assigned,
                        deadline=deadline,
                    )
                if not cleaned:
                    raise _ProcessOwnershipError(phase.label) from startup_error
                if signal_controller is not None:
                    signal_controller.raise_pending()
                if isinstance(
                    startup_error,
                    (KeyboardInterrupt, RunnerSignalInterrupt),
                ):
                    raise
                raise _ProcessOwnershipError(phase.label) from startup_error
        return _OwnedPhaseProcess(supervisor, job)
    except (KeyboardInterrupt, RunnerSignalInterrupt) as startup_error:
        if job is not None and supervisor is not None and not cleanup_performed:
            cleaned = _cleanup_windows_supervisor(
                supervisor,
                job,
                assigned=assigned,
                deadline=deadline,
            )
            if not cleaned:
                raise _ProcessOwnershipError(phase.label) from startup_error
        raise


def _start_owned_phase(
    phase: PytestPhase,
    *,
    deadline: float,
    signal_controller: _SignalController | None,
) -> _OwnedPhaseProcess:
    if os.name == "nt":
        return _start_windows_phase(
            phase,
            deadline=deadline,
            signal_controller=signal_controller,
        )
    return _OwnedPhaseProcess(_start_phase(phase))


def _seconds_remaining(deadline: float) -> float:
    return max(0.0, deadline - time.monotonic())


def _wait_for_process_until(
    process: subprocess.Popen[bytes], *, deadline: float
) -> bool:
    remaining_seconds = _seconds_remaining(deadline)
    if remaining_seconds == 0:
        return process.poll() is not None
    try:
        process.wait(timeout=remaining_seconds)
    except subprocess.TimeoutExpired:
        return False
    return True


def _wait_for_posix_process_group_exit(
    process: subprocess.Popen[bytes],
    *,
    kill_process_group: Callable[[int, int], None],
    deadline: float,
) -> bool:
    while True:
        # poll() reaps an exited leader; group membership, rather than leader
        # state, remains the authority for whether descendants survived.
        process.poll()
        try:
            kill_process_group(process.pid, 0)
        except ProcessLookupError:
            return _wait_for_process_until(process, deadline=deadline)
        except PermissionError:
            pass
        remaining_seconds = _seconds_remaining(deadline)
        if remaining_seconds <= 0:
            return False
        time.sleep(min(TERMINATION_POLL_SECONDS, remaining_seconds))


def _terminate_posix_process_group(
    process: subprocess.Popen[bytes], *, deadline: float
) -> bool:
    kill_process_group = cast(
        Callable[[int, int], None] | None,
        getattr(os, "killpg", None),
    )
    if kill_process_group is None:
        return False

    try:
        kill_process_group(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return _wait_for_process_until(process, deadline=deadline)
    except OSError:
        return False
    graceful_deadline = min(deadline, time.monotonic() + TERMINATION_GRACE_SECONDS)
    if _wait_for_posix_process_group_exit(
        process,
        kill_process_group=kill_process_group,
        deadline=graceful_deadline,
    ):
        return True
    try:
        kill_process_group(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return _wait_for_process_until(process, deadline=deadline)
    except OSError:
        return False
    return _wait_for_posix_process_group_exit(
        process,
        kill_process_group=kill_process_group,
        deadline=deadline,
    )


def _wait_for_owned_tree_exit(
    owner: _OwnedPhaseProcess,
    *,
    deadline: float,
) -> bool:
    if owner.windows_job is not None:
        try:
            return owner.windows_job.wait_empty(
                deadline=deadline
            ) and _wait_for_process_until(
                owner.process,
                deadline=deadline,
            )
        except OSError:
            return False
    kill_process_group = cast(
        Callable[[int, int], None] | None,
        getattr(os, "killpg", None),
    )
    if kill_process_group is None:
        return False
    return _wait_for_posix_process_group_exit(
        owner.process,
        kill_process_group=kill_process_group,
        deadline=deadline,
    )


def _taskkill_windows_tree(
    process: subprocess.Popen[bytes], *, deadline: float
) -> bool:
    remaining_seconds = _seconds_remaining(deadline)
    if remaining_seconds == 0:
        return False
    try:
        completed = subprocess.run(  # noqa: S603,S607 -- Windows tree utility.
            ("taskkill", "/PID", str(process.pid), "/T", "/F"),
            check=False,
            capture_output=True,
            text=True,
            timeout=min(TERMINATION_GRACE_SECONDS, remaining_seconds),
        )
    except OSError, subprocess.TimeoutExpired:
        return False
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        print(
            f"taskkill failed with exit {completed.returncode}"
            + (f": {detail}" if detail else ""),
            file=sys.stderr,
            flush=True,
        )
        return False
    return True


def _terminate_windows_tree(
    owner: _OwnedPhaseProcess,
    *,
    deadline: float,
) -> bool:
    job = owner.windows_job
    if job is None or _seconds_remaining(deadline) == 0:
        return False
    try:
        job.terminate()
    except OSError as error:
        print(str(error), file=sys.stderr, flush=True)
        if not _taskkill_windows_tree(owner.process, deadline=deadline):
            return False
    return _wait_for_owned_tree_exit(owner, deadline=deadline)


def _terminate_owned_tree(
    owner: _OwnedPhaseProcess,
    *,
    deadline: float,
) -> bool:
    """Terminate and verify the managed pytest process tree before deadline."""

    if owner.windows_job is not None:
        return _terminate_windows_tree(owner, deadline=deadline)
    return _terminate_posix_process_group(owner.process, deadline=deadline)


def _close_owner(owner: _OwnedPhaseProcess) -> bool:
    job = owner.windows_job
    if job is None:
        return True
    try:
        job.close()
    except OSError as error:
        print(str(error), file=sys.stderr, flush=True)
        return False
    return True


def _run_phase(
    phase: PytestPhase,
    *,
    hard_deadline: float | None,
    signal_controller: _SignalController | None = None,
) -> int:
    started_at = time.monotonic()
    budget_detail = (
        "unbounded"
        if hard_deadline is None
        else f"{max(0.0, hard_deadline - started_at):.1f}s total remaining"
    )
    print(f"\n==> {phase.label} ({budget_detail})", flush=True)
    execution_deadline = (
        None if hard_deadline is None else hard_deadline - TERMINATION_RESERVE_SECONDS
    )
    if execution_deadline is not None and execution_deadline <= started_at:
        print(
            f"pytest budget lacks the {TERMINATION_RESERVE_SECONDS:.1f}s "
            f"termination reserve before {phase.label}",
            file=sys.stderr,
            flush=True,
        )
        return TIMEOUT_EXIT_CODE
    ownership_deadline = (
        hard_deadline
        if hard_deadline is not None
        else started_at + TERMINATION_RESERVE_SECONDS
    )
    owner: _OwnedPhaseProcess | None = None
    tree_empty_proven = False
    return_code = TERMINATION_FAILED_EXIT_CODE
    try:
        # Do not deliver a controlled platform signal after Popen creates the
        # child but before this frame owns its process tree. The handler records
        # a pending signal and delivers it after ownership is established.
        with _defer_signal_interrupts(signal_controller):
            owner = _start_owned_phase(
                phase,
                deadline=ownership_deadline,
                signal_controller=signal_controller,
            )
        timeout_seconds = (
            None
            if execution_deadline is None
            else max(0.0, execution_deadline - time.monotonic())
        )
        try:
            return_code = owner.process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            print(
                f"pytest budget expired during {phase.label}; "
                "terminating its process tree",
                file=sys.stderr,
                flush=True,
            )
            assert hard_deadline is not None
            with _defer_signal_interrupts(signal_controller):
                if not _terminate_owned_tree(owner, deadline=hard_deadline):
                    print(
                        f"could not verify process-tree termination for {phase.label}",
                        file=sys.stderr,
                        flush=True,
                    )
                    raise _ProcessOwnershipError(phase.label)
                tree_empty_proven = True
            return_code = TIMEOUT_EXIT_CODE
        else:
            cleanup_started = time.monotonic()
            termination_deadline = (
                cleanup_started + TERMINATION_RESERVE_SECONDS
                if hard_deadline is None
                else hard_deadline
            )
            clean_exit_deadline = min(
                termination_deadline,
                cleanup_started + TERMINATION_POLL_SECONDS * 5,
            )
            survivor_found = False
            with _defer_signal_interrupts(signal_controller):
                if _wait_for_owned_tree_exit(owner, deadline=clean_exit_deadline):
                    tree_empty_proven = True
                else:
                    survivor_found = True
                    if not _terminate_owned_tree(
                        owner,
                        deadline=termination_deadline,
                    ):
                        print(
                            f"could not verify process-tree termination for "
                            f"{phase.label}",
                            file=sys.stderr,
                            flush=True,
                        )
                        raise _ProcessOwnershipError(phase.label)
                    tree_empty_proven = True
            if survivor_found:
                print(
                    "pytest leader exited with surviving process-tree members "
                    f"during {phase.label}",
                    file=sys.stderr,
                    flush=True,
                )
                raise _ProcessOwnershipError(phase.label)
    except KeyboardInterrupt, RunnerSignalInterrupt:
        print(
            f"pytest interrupted during {phase.label}; terminating its process tree",
            file=sys.stderr,
            flush=True,
        )
        if owner is not None and not tree_empty_proven:
            cleanup_started = time.monotonic()
            termination_deadline = cleanup_started + TERMINATION_RESERVE_SECONDS
            if hard_deadline is not None:
                termination_deadline = min(hard_deadline, termination_deadline)
            with _defer_signal_interrupts(signal_controller):
                if not _terminate_owned_tree(owner, deadline=termination_deadline):
                    print(
                        f"could not verify process-tree termination for {phase.label}",
                        file=sys.stderr,
                        flush=True,
                    )
                    raise _ProcessOwnershipError(phase.label)
                tree_empty_proven = True
        raise
    finally:
        if owner is not None:
            # Once cleanup starts, another termination signal may wait, but it
            # may not interrupt the only code capable of closing Job ownership.
            with _defer_signal_interrupts(signal_controller):
                if not _close_owner(owner):
                    raise _ProcessOwnershipError(phase.label)
                if (
                    not tree_empty_proven
                    and signal_controller is not None
                    and signal_controller.pending_signal is not None
                ):
                    raise _ProcessOwnershipError(phase.label)
    elapsed = time.monotonic() - started_at
    print(f"<== {phase.label}: {elapsed:.1f}s (exit {return_code})", flush=True)
    return return_code


def run_profile(
    profile: ProfileName,
    *,
    budget_seconds: float | None,
    signal_controller: _SignalController | None = None,
) -> int:
    """Run a profile sequentially under one aggregate wall-clock deadline."""

    deadline = None if budget_seconds is None else time.monotonic() + budget_seconds
    for phase in _phases(profile):
        if (
            deadline is not None
            and _seconds_remaining(deadline) <= TERMINATION_RESERVE_SECONDS
        ):
            print(
                f"pytest budget lacks the {TERMINATION_RESERVE_SECONDS:.1f}s "
                f"termination reserve before {phase.label}",
                file=sys.stderr,
                flush=True,
            )
            return TIMEOUT_EXIT_CODE
        return_code = _run_phase(
            phase,
            hard_deadline=deadline,
            signal_controller=signal_controller,
        )
        if return_code != 0:
            return return_code
    return 0


def _windows_supervisor_main(command: Sequence[str]) -> int:
    """Wait for Job ownership before starting the requested pytest phase."""

    if os.name != "nt" or not command:
        return TERMINATION_FAILED_EXIT_CODE
    try:
        token = sys.stdin.buffer.read(len(_WINDOWS_START_TOKEN))
    except OSError:
        return TERMINATION_FAILED_EXIT_CODE
    if token != _WINDOWS_START_TOKEN:
        return TERMINATION_FAILED_EXIT_CODE
    try:
        process = subprocess.Popen(tuple(command))  # noqa: S603 -- parent-owned argv.
    except OSError as error:
        print(f"could not start pytest: {error}", file=sys.stderr, flush=True)
        return TERMINATION_FAILED_EXIT_CODE
    return process.wait()


def _arguments(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "profile",
        choices=("merge", "deep", "mariadb-server-crash"),
        nargs="?",
        default="merge",
        help=(
            "merge is bounded and selective; deep is complete and manual-only; "
            "mariadb-server-crash is its isolated destructive-container phase"
        ),
    )
    parser.add_argument(
        "--budget-seconds",
        type=_positive_seconds,
        help=(
            "aggregate wall-clock budget for every pytest phase; defaults to 300 "
            "for merge and no limit for deep"
        ),
    )
    return parser.parse_args(arguments)


def main(arguments: Sequence[str] | None = None) -> int:
    parsed = _arguments(arguments)
    profile = cast(ProfileName, parsed.profile)
    requested_budget = cast(float | None, parsed.budget_seconds)
    budget_seconds = (
        DEFAULT_MERGE_BUDGET_SECONDS
        if requested_budget is None and profile == "merge"
        else requested_budget
    )
    try:
        with _controlled_termination_signals() as signal_controller:
            return run_profile(
                profile,
                budget_seconds=budget_seconds,
                signal_controller=signal_controller,
            )
    except RunnerSignalInterrupt as interruption:
        return 128 + interruption.signal_number
    except _ProcessOwnershipError:
        return TERMINATION_FAILED_EXIT_CODE
    except KeyboardInterrupt:
        return INTERRUPTED_EXIT_CODE


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == _WINDOWS_SUPERVISOR_MODE:
        raise SystemExit(_windows_supervisor_main(sys.argv[2:]))
    raise SystemExit(main())
