"""Publish complete child PIDs before process-supervision fixtures act on them."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic, sleep


def publish_pid(path: Path, pid: int) -> None:
    """Make a complete PID visible with one same-filesystem rename."""
    if type(pid) is not int or pid <= 0:
        raise ValueError("process fixture requires a positive PID")
    with TemporaryDirectory(prefix=f".{path.name}-", dir=path.parent) as directory:
        pending = Path(directory) / "pid"
        pending.write_text(str(pid), encoding="ascii")
        pending.replace(path)


def wait_for_pid(path: Path, *, timeout: float) -> int:
    """Wait only for publication; malformed published data fails immediately."""
    deadline = monotonic() + timeout
    while True:
        try:
            payload = path.read_text(encoding="ascii")
        except FileNotFoundError:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError(f"process fixture did not publish {path}") from None
            sleep(min(0.02, remaining))
        else:
            if not payload.isdecimal() or payload.startswith("0"):
                raise ValueError(f"process fixture published invalid PID {payload!r}")
            return int(payload)
