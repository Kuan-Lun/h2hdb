"""Real public catch-up costs, never a deployment wall-clock SLO assertion."""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


@pytest.fixture
def probe() -> ModuleType:
    name = "ingest_batch_scaling_probe_under_test"
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "ingest_batch_scaling_probe.py"
    )
    spec = importlib.util.spec_from_file_location(name, script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    previous = list(sys.path)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = previous
    return module


@pytest.mark.parametrize(
    "value", ("", "1:2", "0:1:1", "2:3:1", "4097:1:1", "4096:1:17")
)
def test_shape_requires_bounded_positive_dimensions(
    probe: ModuleType, value: str
) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        probe.parse_case(value)


def _calls(result: dict[str, Any]) -> int:
    return sum(int(turn["measurements"]["sql_calls"]) for turn in result["turns"])


def test_fewer_publications_reduce_real_sql_and_preserve_public_oracle(
    probe: ModuleType,
) -> None:
    repeated = probe.run_case("sqlite", 4, 1, 2)
    combined = probe.run_case("sqlite", 4, 4, 2)
    assert repeated["oracle"] == combined["oracle"]
    assert repeated["full_ready_audit"] == combined["full_ready_audit"] == "passed"
    assert _calls(repeated) > _calls(combined)
    assert all(turn["deep_reads"] == 1 for turn in repeated["turns"])
    assert all(turn["cleanup"] == "DONE" for turn in repeated["turns"])
    # The intentionally over-published, equivalent run is a cost negative
    # control: functional correctness alone cannot satisfy this query budget.
    budget = (_calls(repeated) + _calls(combined)) // 2
    assert _calls(combined) <= budget
    assert not _calls(repeated) <= budget


def test_neutral_artifact_mode_checks_complete_public_pipeline(
    probe: ModuleType,
) -> None:
    result = probe.run_case("sqlite", 2, 1, 2, artifacts=True)
    assert result["artifacts"]
    assert result["full_ready_audit"] == "passed"
    assert [turn["selected"] for turn in result["turns"]] == [1, 2]
    assert all(turn["deep_reads"] == 1 for turn in result["turns"])


@pytest.mark.parametrize("failure", ["case_failure", "source_drift"])
def test_cli_preserves_failure_status_in_report(
    probe: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    import json

    output = tmp_path / "report.json"
    monkeypatch.setattr(
        sys,
        "argv",
        ["probe", "--case", "1:1:1", "--output", str(output)],
    )
    monkeypatch.setattr(probe.probe, "source_provenance", lambda: {"source": "fixed"})
    source_hashes = {"fixture": "initial"}
    monkeypatch.setattr(probe, "experiment_source_hashes", lambda: dict(source_hashes))

    def simulated_case(*_args: object, **_kwargs: object) -> dict[str, str]:
        if failure == "case_failure":
            raise RuntimeError("case failed before completing the oracle")
        source_hashes["fixture"] = "modified"
        return {"oracle": "complete"}

    monkeypatch.setattr(probe, "run_case", simulated_case)
    with pytest.raises(RuntimeError):
        probe.main()
    report = json.loads(output.read_text())
    assert report["status"] == "failed"
    assert report["error"] == "RuntimeError"
    assert report["experiment_sources_sha256"] == {"fixture": "initial"}
