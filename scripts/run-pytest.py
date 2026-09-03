#!/usr/bin/env python3
"""Run the bounded merge pytest profile or the unbounded manual deep profile."""

from __future__ import annotations

import argparse
import math
import os
import shlex
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MERGE_BUDGET_SECONDS = 300.0
TERMINATION_GRACE_SECONDS = 2.0
TERMINATION_RESERVE_SECONDS = 6.0
TERMINATION_POLL_SECONDS = 0.05
TIMEOUT_EXIT_CODE = 124
TERMINATION_FAILED_EXIT_CODE = 125
INTERRUPTED_EXIT_CODE = 130
ProfileName = Literal["merge", "deep"]


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
        marker_expression="mariadb",
        worker_count="0",
        mariadb_enabled=True,
        durations=50,
        stop_after_first_failure=False,
    ),
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
    return MERGE_PHASES if profile == "merge" else DEEP_PHASES


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
    if os.name == "nt":
        raw_creation_flag = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", None)
        if not isinstance(raw_creation_flag, int) or raw_creation_flag == 0:
            raise RuntimeError(
                "Windows pytest isolation requires CREATE_NEW_PROCESS_GROUP"
            )
        return subprocess.Popen(  # noqa: S603 -- fixed interpreter and pytest CLI.
            command,
            cwd=REPOSITORY_ROOT,
            env=_phase_environment(phase),
            creationflags=raw_creation_flag,
        )
    return subprocess.Popen(  # noqa: S603 -- fixed interpreter and pytest CLI.
        command,
        cwd=REPOSITORY_ROOT,
        env=_phase_environment(phase),
        start_new_session=True,
    )


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


def _terminate_windows_process_group(
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
    return _wait_for_process_until(process, deadline=deadline)


def terminate_process_group(
    process: subprocess.Popen[bytes], *, deadline: float
) -> bool:
    """Terminate and verify the pytest leader and its children before deadline."""

    if os.name == "nt":
        return _terminate_windows_process_group(process, deadline=deadline)
    return _terminate_posix_process_group(process, deadline=deadline)


def _run_phase(phase: PytestPhase, *, hard_deadline: float | None) -> int:
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
    process = _start_phase(phase)
    timeout_seconds = (
        None
        if execution_deadline is None
        else max(0.0, execution_deadline - time.monotonic())
    )
    try:
        try:
            return_code = process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            print(
                f"pytest budget expired during {phase.label}; "
                "terminating its process group",
                file=sys.stderr,
                flush=True,
            )
            assert hard_deadline is not None
            terminated = terminate_process_group(process, deadline=hard_deadline)
            if terminated:
                return_code = TIMEOUT_EXIT_CODE
            else:
                print(
                    f"could not verify process-group termination for {phase.label}",
                    file=sys.stderr,
                    flush=True,
                )
                return_code = TERMINATION_FAILED_EXIT_CODE
    except KeyboardInterrupt:
        print(
            f"pytest interrupted during {phase.label}; terminating its process group",
            file=sys.stderr,
            flush=True,
        )
        termination_deadline = (
            hard_deadline
            if hard_deadline is not None
            else time.monotonic() + TERMINATION_RESERVE_SECONDS
        )
        if not terminate_process_group(process, deadline=termination_deadline):
            print(
                f"could not verify process-group termination for {phase.label}",
                file=sys.stderr,
                flush=True,
            )
        raise
    elapsed = time.monotonic() - started_at
    print(f"<== {phase.label}: {elapsed:.1f}s (exit {return_code})", flush=True)
    return return_code


def run_profile(profile: ProfileName, *, budget_seconds: float | None) -> int:
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
        return_code = _run_phase(phase, hard_deadline=deadline)
        if return_code != 0:
            return return_code
    return 0


def _arguments(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "profile",
        choices=("merge", "deep"),
        nargs="?",
        default="merge",
        help="merge is bounded and selective; deep is complete and manual-only",
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
        return run_profile(profile, budget_seconds=budget_seconds)
    except KeyboardInterrupt:
        return INTERRUPTED_EXIT_CODE


if __name__ == "__main__":
    raise SystemExit(main())
