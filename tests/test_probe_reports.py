from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


@pytest.fixture
def reports() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts/probe_reports.py"
    specification = importlib.util.spec_from_file_location(
        "probe_reports_under_test", path
    )
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def test_report_replaces_complete_json(reports: ModuleType, tmp_path: Path) -> None:
    output = tmp_path / "report.json"
    reports.write_report(output, {"status": "incomplete"})
    reports.write_report(output, {"status": "completed", "cases": [1]})
    assert (
        output.read_text()
        == '{\n  "status": "completed",\n  "cases": [\n    1\n  ]\n}\n'
    )
    assert list(tmp_path.iterdir()) == [output]


@pytest.mark.parametrize("operation", ["fsync", "replace"])
@pytest.mark.parametrize("failure", [OSError, TimeoutError, KeyboardInterrupt])
def test_failed_write_preserves_previous_report(
    reports: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    failure: type[BaseException],
) -> None:
    output = tmp_path / "report.json"
    reports.write_report(output, {"status": "incomplete"})

    def fail(*_args: Any) -> None:
        raise failure("interrupted write")

    monkeypatch.setattr(reports.os, operation, fail)
    with pytest.raises(failure, match="interrupted write"):
        reports.write_report(output, {"status": "completed"})
    assert json.loads(output.read_text()) == {"status": "incomplete"}
    assert list(tmp_path.iterdir()) == [output]


def test_invalid_payload_preserves_previous_report(
    reports: ModuleType, tmp_path: Path
) -> None:
    output = tmp_path / "report.json"
    reports.write_report(output, {"status": "incomplete"})
    with pytest.raises(TypeError):
        reports.write_report(output, {"invalid": object()})
    assert json.loads(output.read_text()) == {"status": "incomplete"}
    assert list(tmp_path.iterdir()) == [output]
