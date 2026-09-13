"""Evidence boundaries use temporary files only; no Docker process is started."""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest


def _load_package() -> ModuleType:
    directory = Path(__file__).resolve().parents[1] / "scripts/deployment_acceptance"
    name = "acceptance_evidence_under_test"
    spec = importlib.util.spec_from_file_location(
        name,
        directory / "__init__.py",
        submodule_search_locations=[str(directory)],
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


package = _load_package()
evidence = importlib.import_module(package.__name__ + ".evidence")
runner = importlib.import_module(package.__name__ + ".runner")

try:
    evidence.require_evidence_support()
except evidence.EvidenceError:
    _SUPPORTED = False
else:
    _SUPPORTED = True

pytestmark = pytest.mark.skipif(
    not _SUPPORTED,
    reason="Safe evidence tests require POSIX descriptor-relative I/O with O_NOFOLLOW",
)


@pytest.fixture
def source(tmp_path: Path) -> Path:
    # Canonicalize the trusted temporary root at creation, never untrusted leaves.
    root = tmp_path.resolve() / "evidence"
    root.mkdir()
    return root


def test_regular_json_and_recursive_export_preserve_exact_bytes(source: Path) -> None:
    (source / "oracle.json").write_bytes(b'{"pages": 2, "sha256": "synthetic"}\n')
    nested = source / "nested"
    nested.mkdir()
    (nested / "probe.jsonl").write_bytes(b'{}\n{"complete": true}\n')
    (nested / "empty").mkdir()
    assert evidence.read_evidence_json(source, "oracle.json") == {
        "pages": 2,
        "sha256": "synthetic",
    }
    target = source.parent / "export"
    assert evidence.export_evidence(source, target) == [
        "nested/probe.jsonl",
        "oracle.json",
    ]
    assert (target / "nested/empty").is_dir()
    for relative in ("oracle.json", "nested/probe.jsonl"):
        assert (target / relative).read_bytes() == (source / relative).read_bytes()
        assert not (target / relative).is_symlink()


@pytest.mark.parametrize(
    "name", ["", ".", "..", "../outside", "/outside", "a/b", "a\0b"]
)
def test_reader_rejects_names_outside_one_directory(source: Path, name: str) -> None:
    with pytest.raises(evidence.EvidenceError):
        evidence.read_evidence_bytes(source, name)


@pytest.mark.parametrize("linked", ["leaf", "root", "ancestor"])
def test_reader_never_follows_symlinks(source: Path, linked: str) -> None:
    outside = source.parent / "outside"
    outside.mkdir()
    (outside / "secret").write_bytes(b"host bytes must not enter evidence")
    directory = source
    name = "secret"
    if linked == "leaf":
        (source / name).symlink_to(outside / name)
    elif linked == "root":
        directory = source.parent / "linked"
        directory.symlink_to(outside, target_is_directory=True)
    else:
        link = source.parent / "linked"
        link.symlink_to(source.parent, target_is_directory=True)
        directory = link / "outside"
    with pytest.raises(evidence.EvidenceError):
        evidence.read_evidence_bytes(directory, name)


@pytest.mark.parametrize("linked", ["leaf", "directory", "destination"])
def test_export_never_follows_source_or_destination_links(
    source: Path, linked: str
) -> None:
    outside = source.parent / "outside"
    outside.mkdir()
    (outside / "secret").write_bytes(b"private host contents")
    target = source.parent / "export"
    if linked == "destination":
        target.symlink_to(outside, target_is_directory=True)
    else:
        (source / "link").symlink_to(
            outside if linked == "directory" else outside / "secret",
            target_is_directory=linked == "directory",
        )
    with pytest.raises(evidence.EvidenceError):
        evidence.export_evidence(source, target)
    assert (outside / "secret").read_bytes() == b"private host contents"
    assert sorted(path.name for path in outside.iterdir()) == ["secret"]
    if linked != "destination":
        assert list(target.iterdir()) == []


@pytest.mark.parametrize("kind", ["fifo", "directory"])
def test_reader_rejects_special_files_without_opening_them(
    source: Path, kind: str
) -> None:
    path = source / "special"
    if kind == "fifo":
        os.mkfifo(path)
    else:
        path.mkdir()
    with pytest.raises(evidence.EvidenceError, match="regular file"):
        evidence.read_evidence_bytes(source, path.name)
    if kind != "directory":
        with pytest.raises(evidence.EvidenceError, match="regular file"):
            evidence.export_evidence(source, source.parent / "export")


def test_export_rejects_recursive_destination_and_existing_directory(
    source: Path,
) -> None:
    with pytest.raises(evidence.EvidenceError, match="outside"):
        evidence.export_evidence(source, source / "nested")
    existing = source.parent / "existing"
    existing.mkdir()
    (existing / "keep").write_text("untouched")
    with pytest.raises(evidence.EvidenceError):
        evidence.export_evidence(source, existing)
    assert (existing / "keep").read_text() == "untouched"


def test_reader_bounds_memory_and_rejects_nonobject_json(
    source: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (source / "large").write_bytes(b"12345")
    monkeypatch.setattr(evidence, "_MAX_READ_BYTES", 4)
    with pytest.raises(evidence.EvidenceError, match="parsing limit"):
        evidence.read_evidence_bytes(source, "large")
    (source / "list.json").write_text("[]")
    with pytest.raises(evidence.EvidenceError, match="object"):
        evidence.read_evidence_json(source, "list.json")


def test_reader_does_not_chase_concurrently_appended_bytes(
    source: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = source / "probe.jsonl"
    path.write_bytes(b"first\n")
    original = os.read

    def append(descriptor: int, size: int) -> bytes:
        with path.open("ab") as output:
            output.write(b"later\n")
        return original(descriptor, size)

    monkeypatch.setattr(evidence.os, "read", append)
    assert evidence.read_evidence_bytes(source, path.name) == b"first\n"
    assert path.read_bytes() == b"first\nlater\n"


def test_export_rejects_source_changed_during_copy(
    source: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = source / "probe.jsonl"
    path.write_bytes(b"first\n")
    original = os.read

    def append(descriptor: int, size: int) -> bytes:
        with path.open("ab") as output:
            output.write(b"later\n")
        return original(descriptor, size)

    monkeypatch.setattr(evidence.os, "read", append)
    with pytest.raises(evidence.EvidenceError, match="changed during export"):
        evidence.export_evidence(source, source.parent / "export")


def _acceptance(source: Path) -> Any:
    value = runner.Acceptance.__new__(runner.Acceptance)
    value.args = SimpleNamespace(keep_fixtures=False, instrumented=False)
    value.root = source.parent
    value.project = "h2hdb-acceptance-evidence-test"
    value.prepared = SimpleNamespace(
        evidence_dir=source, compose_path=source / "compose.json"
    )
    output = source.parent.parent / (source.parent.name + "-output")
    output.mkdir()

    def fail_before_work(_project: str) -> None:
        raise RuntimeError("intentional offline failure before work")

    value.commands = SimpleNamespace(
        output=output,
        assert_fresh_project=fail_before_work,
        cleanup=lambda *_args: {"verified_empty": True, "remaining": {}},
    )
    value.report = {"scenarios": []}
    value.helper = lambda _args: None
    return value


def test_runner_verification_uses_safe_reader_and_preserves_fields(
    source: Path,
) -> None:
    value = _acceptance(source)
    expected = {"artifacts": [{"gid": 1, "sha256": "a" * 64}], "verified_pages": 2}
    (source / "oracle-fresh.json").write_text(json.dumps(expected))
    assert value.verify("fresh") == expected
    assert (
        json.loads((value.commands.output / "oracle-fresh.json").read_text())
        == expected
    )
    (source / "oracle-linked.json").symlink_to(source / "oracle-fresh.json")
    with pytest.raises(evidence.EvidenceError):
        value.verify("linked")
    assert not (value.commands.output / "oracle-linked.json").exists()


@pytest.mark.parametrize("failure", ["symlink", "parser"])
def test_final_evidence_failure_preserves_cleanup_report_and_fixture(
    source: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    value = _acceptance(source)
    if failure == "symlink":
        outside = source.parent / "host-file"
        outside.write_text("do not export")
        (source / "unsafe").symlink_to(outside)
    else:

        def fail_parser(_paths: object) -> None:
            raise OSError("intentional evidence parser failure")

        monkeypatch.setattr(runner, "read_probe_events", fail_parser)
    assert value.run() == 1
    result = json.loads((value.commands.output / "report.json").read_text())
    assert result["cleanup"] == {"verified_empty": True, "remaining": {}}
    assert result["evidence"]["status"] == "failed"
    assert result["measurement"]["status"] == "failed"
    assert result["fixture_retained"] is True
    assert result["finished_utc"]
    assert value.root.exists()


def test_baseline_export_without_probe_is_not_an_instrumentation_failure(
    source: Path,
) -> None:
    value = _acceptance(source)
    (source / "oracle.json").write_text("{}")
    assert (
        value.run() == 1
    )  # The intentionally injected startup error remains a failure.
    result = json.loads((value.commands.output / "report.json").read_text())
    assert result["evidence"]["status"] == "exported"
    assert result["measurement"]["status"] == "not_requested"
    assert result["fixture_retained"] is False
    assert not value.root.exists()
