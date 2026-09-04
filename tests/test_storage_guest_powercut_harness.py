from __future__ import annotations

import os
import selectors
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "scripts" / "storage-guest-powercut.py"
READY_PREFIX = "H2HDB_GUEST_POWERCUT_READY"
VERIFIED_PREFIX = "H2HDB_GUEST_POWERCUT_STATE_VERIFIED"
PROCESS_ONLY_DISCLAIMER = "does not constitute guest power-cut evidence"
VERIFY_DISCLAIMER = "ordinary process restart is only a harness protocol test"
PROCESS_OUTPUT_TIMEOUT_SECONDS = 60.0
PROCESS_REAP_TIMEOUT_SECONDS = 5.0


def _environment() -> dict[str, str]:
    environment = os.environ.copy()
    existing = environment.get("PYTHONPATH")
    source = str(ROOT / "src")
    environment["PYTHONPATH"] = (
        f"{source}{os.pathsep}{existing}" if existing else source
    )
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return environment


def _command(action: str, state_directory: Path) -> tuple[str, ...]:
    return (
        sys.executable,
        str(HARNESS),
        action,
        "--state-directory",
        str(state_directory),
    )


def _wait_for_output(
    process: subprocess.Popen[bytes],
    *,
    required: tuple[bytes, ...],
) -> str:
    assert process.stdout is not None
    observed = b""
    deadline = time.monotonic() + PROCESS_OUTPUT_TIMEOUT_SECONDS
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        while not all(fragment in observed for fragment in required):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                pytest.fail(
                    f"prepare output timed out; observed {observed!r}",
                    pytrace=False,
                )
            if not selector.select(timeout=remaining):
                continue
            chunk = os.read(process.stdout.fileno(), 4096)
            if chunk:
                observed += chunk
                continue
            pytest.fail(
                f"prepare exited before its barrier with {process.poll()}; "
                f"observed {observed!r}",
                pytrace=False,
            )
    return observed.decode(errors="replace")


@pytest.mark.skipif(os.name != "posix", reason="manual harness requires POSIX")
def test_normal_process_restart_validates_protocol_but_not_guest_power_cut(
    tmp_path: Path,
) -> None:
    """Exercise prepare/verify without claiming a whole-guest hard stop."""

    state_directory = tmp_path / "dedicated-guest-state"
    process = subprocess.Popen(  # noqa: S603 -- fixed repository harness path.
        _command("prepare", state_directory),
        cwd=ROOT,
        env=_environment(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=0,
        start_new_session=True,
    )
    try:
        output = _wait_for_output(
            process,
            required=(READY_PREFIX.encode(), PROCESS_ONLY_DISCLAIMER.encode()),
        )
        assert "Hard-stop the entire disposable VM externally now" in output
        assert process.poll() is None
        process.kill()
        assert process.wait(timeout=PROCESS_REAP_TIMEOUT_SECONDS) == -signal.SIGKILL
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=PROCESS_REAP_TIMEOUT_SECONDS)

    verified = subprocess.run(  # noqa: S603 -- fixed repository harness path.
        _command("verify", state_directory),
        cwd=ROOT,
        env=_environment(),
        check=False,
        capture_output=True,
        text=True,
        timeout=PROCESS_OUTPUT_TIMEOUT_SECONDS,
    )
    assert verified.returncode == 0, verified.stdout + verified.stderr
    assert VERIFIED_PREFIX in verified.stdout
    assert VERIFY_DISCLAIMER in verified.stdout

    refused = subprocess.run(  # noqa: S603 -- fixed repository harness path.
        _command("prepare", state_directory),
        cwd=ROOT,
        env=_environment(),
        check=False,
        capture_output=True,
        text=True,
        timeout=PROCESS_REAP_TIMEOUT_SECONDS,
    )
    assert refused.returncode == 2
    assert "refusing to reuse or overwrite it" in refused.stderr
