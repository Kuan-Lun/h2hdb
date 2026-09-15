"""Explicit test gate holding a real OPDS archive descriptor across publication.

The original iterator reads every byte after release. Pausing before its first
read makes descriptor lifetime observable with small archives, independently of
socket buffering. This opt-in correctness gate is not performance evidence.
"""

from __future__ import annotations

import functools
import importlib
import importlib.metadata
import json
import math
import os
import re
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any, BinaryIO, Protocol

_TOKEN = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")


def stream_lifetime_evidence(
    events: list[dict[str, Any]], token: str
) -> dict[str, Any]:
    """Require one paired gate on the same real descriptor and worker thread.

    These timestamps are compared within the OPDS process only. The runner owns
    the cross-service barriers: observe reached before changing the source, and
    observe cleanup DONE before issuing the release.
    """
    selected = [
        event
        for event in events
        if event.get("token") == token
        and str(event.get("event", "")).startswith("stream_gate_")
    ]
    reached = [
        event for event in selected if event.get("event") == "stream_gate_reached"
    ]
    released = [
        event for event in selected if event.get("event") == "stream_gate_released"
    ]
    if len(selected) != 2 or len(reached) != 1 or len(released) != 1:
        raise AssertionError(
            "Stream lifetime requires exactly one reached/released pair"
        )
    before, after = reached[0], released[0]
    identity = (
        "process_instance",
        "thread_id",
        "descriptor_device",
        "descriptor_inode",
        "byte_length",
    )
    integers = (
        "sequence",
        "monotonic_ns",
        "thread_id",
        "descriptor_inode",
        "byte_length",
    )
    for event in (before, after):
        if (
            event.get("operation") != "opds.archive.read"
            or event.get("fault_injection") is not True
            or not isinstance(event.get("process_instance"), str)
            or not event["process_instance"]
            or any(
                type(event.get(key)) is not int or event[key] <= 0 for key in integers
            )
            or type(event.get("descriptor_device")) is not int
            or event["descriptor_device"] < 0
            or type(event.get("links")) is not int
            or event["links"] < 0
        ):
            raise AssertionError(
                "Stream lifetime event has missing or invalid identity"
            )
    if (
        any(before[key] != after[key] for key in identity)
        or before["sequence"] >= after["sequence"]
        or before["monotonic_ns"] >= after["monotonic_ns"]
    ):
        raise AssertionError("Stream lifetime changed its descriptor or event ordering")
    fields = (*identity, "sequence", "monotonic_ns", "links")
    return {
        "status": "passed",
        "token": token,
        "reached": {key: before[key] for key in fields},
        "released": {key: after[key] for key in fields},
        "held_seconds": (after["monotonic_ns"] - before["monotonic_ns"])
        / 1_000_000_000,
    }


class StreamProbe(Protocol):
    control: Path | None
    path: Path

    def emit(self, event: str, operation: str, **details: object) -> None: ...


def install(state: StreamProbe) -> bool:
    """Install only in an actual OPDS wheel, preserving the real read iterator."""
    try:
        importlib.metadata.version("h2hdb-opds")
    except importlib.metadata.PackageNotFoundError:
        return False
    acquisition = importlib.import_module("h2hdb_opds.acquisition")
    original = acquisition._read_file
    claimed: set[str] = set()
    lock = threading.Lock()

    @functools.wraps(original)
    def read_file(
        source: BinaryIO, byte_range: Any, *, extent_offset: int = 0
    ) -> Iterator[bytes]:
        iterator = original(source, byte_range, extent_offset=extent_offset)
        try:
            arm = _arm(state.control)
            if (
                arm is not None
                and extent_offset == 0
                and byte_range.start == 0
                and byte_range.length == arm[1]
            ):
                token, size, seconds = arm
                with lock:
                    selected = token not in claimed
                    if selected:
                        claimed.add(token)
                if selected:
                    _hold_descriptor(state, source, token, size, seconds)
            yield from iterator
        finally:
            iterator.close()
            # Closing an unstarted generator does not execute its finally.
            # The real FD must also close if arm validation or the gate fails.
            source.close()

    setattr(acquisition, "_read_file", read_file)  # noqa: B010 - Optional wheel exposes this private test hook at runtime.
    return True


def _arm(control: Path | None) -> tuple[str, int, float] | None:
    if control is None:
        return None
    try:
        with (control / "stream-arm.json").open("rb") as stream:
            raw = stream.read(4097)
    except FileNotFoundError:
        return None
    if len(raw) > 4096:
        raise ValueError("stream gate arm exceeds its byte limit")
    value = json.loads(raw)
    if not isinstance(value, dict) or set(value) != {
        "token",
        "byte_length",
        "deadline_seconds",
    }:
        raise ValueError("stream gate arm has an invalid shape")
    token, size, seconds = (
        value["token"],
        value["byte_length"],
        value["deadline_seconds"],
    )
    if (
        not isinstance(token, str)
        or _TOKEN.fullmatch(token) is None
        or type(size) is not int
        or size <= 0
        or type(seconds) not in {int, float}
        or not math.isfinite(seconds)
        or not 0 < seconds <= 3600
    ):
        raise ValueError("stream gate arm has invalid values")
    return token, size, float(seconds)


def _hold_descriptor(
    state: StreamProbe,
    source: BinaryIO,
    token: str,
    size: int,
    seconds: float,
) -> None:
    assert state.control is not None
    before = os.fstat(source.fileno())
    if before.st_size != size:
        raise ValueError("stream gate descriptor does not match the expected archive")
    details = {
        "token": token,
        "descriptor_device": before.st_dev,
        "descriptor_inode": before.st_ino,
        "byte_length": before.st_size,
        "fault_injection": True,
    }
    state.emit(
        "stream_gate_reached", "opds.archive.read", **details, links=before.st_nlink
    )
    ready = state.path.parent / f"stream-ready-{token}.json"
    descriptor = os.open(
        ready,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    with os.fdopen(descriptor, "w") as stream:
        json.dump(details, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    started = time.monotonic()
    release = state.control / f"stream-release-{token}"
    while not release.is_file():
        if time.monotonic() - started >= seconds:
            state.emit("stream_gate_timeout", "opds.archive.read", **details)
            raise TimeoutError("OPDS stream descriptor gate was not released")
        time.sleep(0.05)
    after = os.fstat(source.fileno())
    if (after.st_dev, after.st_ino, after.st_size) != (
        before.st_dev,
        before.st_ino,
        before.st_size,
    ):
        raise AssertionError("Held archive descriptor identity changed")
    state.emit(
        "stream_gate_released",
        "opds.archive.read",
        **details,
        links=after.st_nlink,
        held_seconds=time.monotonic() - started,
    )
