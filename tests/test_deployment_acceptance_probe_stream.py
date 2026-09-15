"""The stream gate holds a real file descriptor without substituting its bytes."""

from __future__ import annotations

import importlib
import json
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import BinaryIO

import pytest


@pytest.fixture
def stream_probe(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    monkeypatch.syspath_prepend(
        str(Path(__file__).resolve().parents[1] / "scripts/deployment_acceptance")
    )
    return importlib.import_module("probe_stream")


class State:
    def __init__(self, root: Path) -> None:
        self.control = root
        self.path = root / "probe.jsonl"
        self.events: list[tuple[str, dict[str, object]]] = []
        self.reached = threading.Event()

    def emit(self, event: str, operation: str, **details: object) -> None:
        assert operation == "opds.archive.read"
        self.events.append((event, details))
        if event == "stream_gate_reached":
            self.reached.set()


@dataclass(frozen=True)
class ByteRange:
    start: int
    length: int


def install(
    monkeypatch: pytest.MonkeyPatch, module: ModuleType, state: State
) -> SimpleNamespace:
    def read(
        source: BinaryIO, byte_range: ByteRange, *, extent_offset: int = 0
    ) -> Iterator[bytes]:
        try:
            source.seek(extent_offset + byte_range.start)
            while chunk := source.read(3):
                yield chunk
        finally:
            source.close()

    acquisition = SimpleNamespace(_read_file=read)
    monkeypatch.setattr(module.importlib.metadata, "version", lambda _name: "test")
    monkeypatch.setattr(module.importlib, "import_module", lambda _name: acquisition)
    assert module.install(state) is True
    return acquisition


def arm(root: Path, *, seconds: float = 5, size: int = 9) -> None:
    (root / "stream-arm.json").write_text(
        json.dumps({"token": "case", "byte_length": size, "deadline_seconds": seconds})
    )


def test_gate_keeps_real_old_inode_open_after_path_replacement(
    stream_probe: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state = State(tmp_path)
    acquisition = install(monkeypatch, stream_probe, state)
    path = tmp_path / "archive.cbz"
    path.write_bytes(b"old bytes")
    source = path.open("rb")
    arm(tmp_path)
    result = bytearray()
    failures: list[BaseException] = []

    def consume() -> None:
        try:
            for value in acquisition._read_file(source, ByteRange(0, 9)):
                result.extend(value)
        except BaseException as error:
            failures.append(error)

    worker = threading.Thread(target=consume)
    worker.start()
    try:
        assert state.reached.wait(3)
        assert not result and not source.closed
        path.unlink()
        path.write_bytes(b"new bytes")
        (tmp_path / "stream-release-case").touch()
    finally:
        (tmp_path / "stream-release-case").touch()
        worker.join(3)
    assert not worker.is_alive() and not failures
    assert result == b"old bytes" and path.read_bytes() == b"new bytes"
    assert source.closed
    ready = json.loads((tmp_path / "stream-ready-case.json").read_text())
    assert ready["byte_length"] == 9
    assert [event for event, _details in state.events] == [
        "stream_gate_reached",
        "stream_gate_released",
    ]
    before, after = (details for _event, details in state.events)
    assert before["descriptor_inode"] == after["descriptor_inode"]
    assert before["links"] == 1 and after["links"] == 0


def test_gate_failure_closes_even_an_unstarted_original_generator(
    stream_probe: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state = State(tmp_path)
    acquisition = install(monkeypatch, stream_probe, state)
    path = tmp_path / "archive.cbz"
    path.write_bytes(b"old bytes")
    arm(tmp_path, seconds=0.01)
    source = path.open("rb")
    with pytest.raises(TimeoutError, match="not released"):
        list(acquisition._read_file(source, ByteRange(0, 9)))
    assert source.closed
    assert state.events[-1][0] == "stream_gate_timeout"


def test_gate_claims_matching_full_download_only_once(
    stream_probe: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state = State(tmp_path)
    acquisition = install(monkeypatch, stream_probe, state)
    path = tmp_path / "archive.cbz"
    path.write_bytes(b"old bytes")
    arm(tmp_path)
    (tmp_path / "stream-release-case").touch()
    with path.open("rb") as source:
        assert b"".join(acquisition._read_file(source, ByteRange(0, 9))) == b"old bytes"
    (tmp_path / "stream-release-case").unlink()
    with path.open("rb") as source:
        assert b"".join(acquisition._read_file(source, ByteRange(0, 9))) == b"old bytes"
    assert len(state.events) == 2


@pytest.mark.parametrize(
    "value",
    [
        {},
        {"token": "../escape", "byte_length": 9, "deadline_seconds": 1},
        {"token": "case", "byte_length": True, "deadline_seconds": 1},
        {"token": "case", "byte_length": 9, "deadline_seconds": 3601},
    ],
)
def test_invalid_arm_cannot_start_a_gate(
    stream_probe: ModuleType, tmp_path: Path, value: object
) -> None:
    (tmp_path / "stream-arm.json").write_text(json.dumps(value))
    with pytest.raises(ValueError):
        stream_probe._arm(tmp_path)


def lifetime_events() -> list[dict[str, object]]:
    common = {
        "operation": "opds.archive.read",
        "token": "case",
        "fault_injection": True,
        "process_instance": "opds-17",
        "thread_id": 99,
        "descriptor_device": 1,
        "descriptor_inode": 5,
        "byte_length": 100,
        "links": 1,
    }
    return [
        {
            **common,
            "event": "stream_gate_reached",
            "sequence": 12,
            "monotonic_ns": 1_000_000_000,
        },
        {
            **common,
            "event": "stream_gate_released",
            "sequence": 15,
            "monotonic_ns": 4_000_000_000,
            "links": 0,
        },
    ]


def test_lifetime_receipt_keeps_descriptor_identity_and_same_process_elapsed(
    stream_probe: ModuleType,
) -> None:
    result = stream_probe.stream_lifetime_evidence(lifetime_events(), "case")
    assert result["status"] == "passed" and result["held_seconds"] == 3
    assert (
        result["reached"]["descriptor_inode"] == result["released"]["descriptor_inode"]
    )
    assert result["released"]["links"] == 0


@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "duplicate",
        "timeout",
        "thread",
        "process",
        "inode",
        "size",
        "ordering",
        "clock",
        "no_identity",
    ],
)
def test_lifetime_receipt_rejects_incomplete_or_mismatched_evidence(
    stream_probe: ModuleType, change: str
) -> None:
    events = lifetime_events()
    if change == "missing":
        events.pop()
    elif change == "duplicate":
        events.append(events[0])
    elif change == "timeout":
        events.append({"token": "case", "event": "stream_gate_timeout"})
    elif change == "no_identity":
        del events[0]["descriptor_inode"]
    else:
        key, value = {
            "thread": ("thread_id", 98),
            "process": ("process_instance", "opds-other"),
            "inode": ("descriptor_inode", 7),
            "size": ("byte_length", 101),
            "ordering": ("sequence", 12),
            "clock": ("monotonic_ns", 1_000_000_000),
        }[change]
        events[1][key] = value
    with pytest.raises(AssertionError, match="Stream lifetime"):
        stream_probe.stream_lifetime_evidence(events, "case")
