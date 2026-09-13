from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


def _load_module() -> ModuleType:
    root = Path(__file__).resolve().parents[1] / "scripts/deployment_acceptance"
    package_name = "deployment_acceptance_execution_test_package"
    package = importlib.util.spec_from_file_location(
        package_name, root / "__init__.py", submodule_search_locations=[str(root)]
    )
    assert package is not None and package.loader is not None
    module = importlib.util.module_from_spec(package)
    sys.modules[package_name] = module
    package.loader.exec_module(module)
    spec = importlib.util.spec_from_file_location(
        f"{package_name}.execution", root / "execution.py"
    )
    assert spec is not None and spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = loaded
    spec.loader.exec_module(loaded)
    return loaded


execution = _load_module()
PROJECT = "h2hdb-acceptance-execution-test"


def test_timeout_keeps_partial_output_and_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = execution.Commands(
        context="test-context", output=tmp_path / "output", seconds=120
    )

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert kwargs["timeout"] <= 2
        raise subprocess.TimeoutExpired(
            command,
            kwargs["timeout"],
            output=b"partial stdout\n",
            stderr=b"partial stderr\n",
        )

    monkeypatch.setattr(execution.subprocess, "run", run)
    with pytest.raises(subprocess.TimeoutExpired):
        runner.run(["docker", "example"], timeout=2)
    record = json.loads((runner.output / "commands.jsonl").read_text())
    assert record["exception"] == "TimeoutExpired"
    assert record["command"] == ["docker", "example"]
    assert (
        runner.output / record["output"]
    ).read_text() == "partial stdout\npartial stderr\n"


@pytest.mark.parametrize(
    "failure",
    [FileNotFoundError("missing executable"), KeyboardInterrupt("interrupted")],
)
def test_launch_error_and_interrupt_are_journaled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: BaseException
) -> None:
    runner = execution.Commands(
        context="test-context", output=tmp_path / "output", seconds=120
    )

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise failure

    monkeypatch.setattr(execution.subprocess, "run", run)
    with pytest.raises(type(failure)):
        runner.run(["docker", "example"])
    record = json.loads((runner.output / "commands.jsonl").read_text())
    assert record["exception"] == type(failure).__name__
    assert (runner.output / record["output"]).exists()


def test_deadline_reserves_cleanup_and_bounds_late_cleanup_attempts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = execution.Commands(
        context="test-context", output=tmp_path / "output", seconds=120
    )
    runner.deadline = execution.time.monotonic() - 1
    calls = []

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(kwargs["timeout"])
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(execution.subprocess, "run", run)
    with pytest.raises(TimeoutError, match="reserved"):
        runner.run(["normal"])
    assert calls == []
    runner.run(["cleanup"], cleanup=True)
    assert calls == [1]


class FakeDocker:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.resources: dict[str, set[str]] = {
            kind: set() for kind in ("containers", "networks", "volumes")
        }
        self.foreign: dict[str, set[str]] = {kind: set() for kind in self.resources}
        self.fail_logs = False
        self.fail_down = False
        self.fail_remove: set[str] = set()
        self.fail_final_listing: str | None = None
        self.removing = False

    def run(self, command: Sequence[str], **options: Any) -> str:
        self.calls.append(list(command))
        assert list(command[:3]) == ["docker", "--context", "test-context"]
        if "compose" in command:
            if "logs" in command and self.fail_logs:
                raise subprocess.TimeoutExpired(command, 1, output="partial logs")
            if "down" in command:
                assert "--remove-orphans" not in command
                if self.fail_down:
                    raise RuntimeError("down failed")
                for values in self.resources.values():
                    values.clear()
            return ""
        kind = (
            "networks"
            if "network" in command
            else "volumes"
            if "volume" in command
            else "containers"
        )
        if "--filter" in command:
            if self.removing and self.fail_final_listing == kind:
                raise RuntimeError("cannot verify daemon inventory")
            label = command[-1]
            assert label in {
                f"label={execution.OWNER_LABEL}={PROJECT}",
                f"label=com.docker.compose.project={PROJECT}",
            }
            values = self.resources[kind]
            if label.startswith("label=com.docker.compose.project="):
                values = values | self.foreign[kind]
            return "\n".join(sorted(values))
        assert "rm" in command and command[-2] == "--"
        self.removing = True
        identity = command[-1]
        assert identity in self.resources[kind]
        assert identity not in self.foreign[kind]
        if identity in self.fail_remove:
            raise RuntimeError(f"cannot remove {identity}")
        self.resources[kind].remove(identity)
        return identity


def admitted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake: FakeDocker) -> Any:
    runner = execution.Commands(
        context="test-context", output=tmp_path / "output", seconds=120
    )
    monkeypatch.setattr(runner, "run", fake.run)
    runner.assert_fresh_project(PROJECT)
    fake.resources = {
        "containers": {"container-a", "container-b"},
        "networks": {"network-a"},
        "volumes": {"volume-a"},
    }
    return runner


def test_cleanup_falls_back_after_logs_and_down_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeDocker()
    runner = admitted(tmp_path, monkeypatch, fake)
    fake.fail_logs = fake.fail_down = True
    receipt = runner.cleanup(PROJECT, tmp_path / "compose.json")
    assert receipt["verified_empty"] is True
    assert receipt["remaining"] == {kind: [] for kind in fake.resources}
    assert receipt["removed"] == {
        "containers": ["container-a", "container-b"],
        "networks": ["network-a"],
        "volumes": ["volume-a"],
    }
    assert {item["phase"] for item in receipt["diagnostics"]} == {
        "logs",
        "compose down",
    }
    assert json.loads((runner.output / "cleanup.json").read_text()) == receipt


def test_cleanup_attempts_other_resources_after_individual_removal_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeDocker()
    runner = admitted(tmp_path, monkeypatch, fake)
    fake.fail_down = True
    fake.fail_remove = {"container-a", "network-a"}
    with pytest.raises(execution.CleanupError) as error:
        runner.cleanup(PROJECT, tmp_path / "compose.json")
    receipt = error.value.receipt
    assert receipt["verified_empty"] is False
    assert receipt["remaining"] == {
        "containers": ["container-a"],
        "networks": ["network-a"],
        "volumes": [],
    }
    assert receipt["removed"]["containers"] == ["container-b"]
    assert receipt["removed"]["volumes"] == ["volume-a"]


def test_cleanup_preserves_foreign_project_resources_and_skips_compose_down(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeDocker()
    runner = admitted(tmp_path, monkeypatch, fake)
    fake.foreign["containers"] = {"unrelated-container"}
    receipt = runner.cleanup(PROJECT, tmp_path / "compose.json")
    assert receipt["verified_empty"] is True
    assert fake.foreign["containers"] == {"unrelated-container"}
    assert receipt["foreign_project_resources"]["containers"] == ["unrelated-container"]
    assert not any("down" in command for command in fake.calls)


def test_failed_inventory_cannot_be_reported_as_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeDocker()
    runner = admitted(tmp_path, monkeypatch, fake)
    fake.fail_down = True
    fake.fail_final_listing = "networks"
    with pytest.raises(execution.CleanupError) as error:
        runner.cleanup(PROJECT, tmp_path / "compose.json")
    assert error.value.receipt["remaining"]["networks"] is None
    assert error.value.receipt["verified_empty"] is False
    assert fake.resources["volumes"] == set()


@pytest.mark.parametrize("kind", ["containers", "networks", "volumes"])
def test_project_collision_checks_compose_labels_and_never_arms_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    fake = FakeDocker()
    fake.foreign[kind] = {"preexisting"}
    runner = execution.Commands(
        context="test-context", output=tmp_path / "output", seconds=120
    )
    monkeypatch.setattr(runner, "run", fake.run)
    with pytest.raises(ValueError, match="pre-existing"):
        runner.assert_fresh_project(PROJECT)
    calls = len(fake.calls)
    with pytest.raises(ValueError, match="fresh-project admission"):
        runner.cleanup(PROJECT, tmp_path / "compose.json")
    assert len(fake.calls) == calls
    assert fake.foreign[kind] == {"preexisting"}
