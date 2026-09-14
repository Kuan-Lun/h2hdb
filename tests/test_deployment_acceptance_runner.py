"""Offline orchestration contracts; these tests never run Docker or a gallery job."""

from __future__ import annotations

import copy
import importlib
import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest


def _load_runner() -> ModuleType:
    directory = Path(__file__).resolve().parents[1] / "scripts/deployment_acceptance"
    package_name = "acceptance_runner_under_test"
    spec = importlib.util.spec_from_file_location(
        package_name,
        directory / "__init__.py",
        submodule_search_locations=[str(directory)],
    )
    assert spec is not None and spec.loader is not None
    package = importlib.util.module_from_spec(spec)
    sys.modules[package_name] = package
    spec.loader.exec_module(package)
    return importlib.import_module(package_name + ".runner")


runner = _load_runner()


def _artifact(gid: int) -> dict[str, Any]:
    return {
        "gid": gid,
        "generation": 1,
        "pages": 2,
        "key": f"acquisitions/{gid}.cbz",
        "sha256": "a" * 64,
        "byte_length": 100,
        "mtime_ns": 10,
        "ctime_ns": 11,
        "device": 1,
        "inode": gid,
        "thumbnail_key": f"artwork/{gid}.jpg",
        "thumbnail_sha256": "b" * 64,
        "page_sha256": ["c" * 64, "d" * 64],
    }


def _oracle() -> dict[str, Any]:
    return {
        "verified_publications": 2,
        "verified_pages": 4,
        "artifacts": [_artifact(1), _artifact(2)],
    }


def test_reuse_accepts_identical_evidence_in_different_row_order() -> None:
    before = _oracle()
    after = copy.deepcopy(before)
    after["artifacts"].reverse()
    runner.compare_reuse(before, after)
    assert before == _oracle()


@pytest.mark.parametrize(
    "field,value",
    [
        ("sha256", "e" * 64),
        ("byte_length", 101),
        ("mtime_ns", 12),
        ("ctime_ns", 12),
        ("device", 2),
        ("inode", 3),
        ("key", "renamed/1.cbz"),
        ("generation", 2),
        ("pages", 3),
        ("page_sha256", ["d" * 64, "c" * 64]),
        ("thumbnail_key", "renamed/1.jpg"),
        ("thumbnail_sha256", "e" * 64),
    ],
)
def test_reuse_rejects_changed_bytes_identity_or_public_mapping(
    field: str, value: object
) -> None:
    before = _oracle()
    after = copy.deepcopy(before)
    after["artifacts"][0][field] = value
    with pytest.raises(AssertionError, match="GID 1"):
        runner.compare_reuse(before, after)


def test_reuse_rejects_removed_publication() -> None:
    after = _oracle()
    after["artifacts"].pop()
    with pytest.raises(AssertionError, match="membership"):
        runner.compare_reuse(_oracle(), after)


def _argv(tmp_path: Path) -> list[str]:
    return [
        "--deployment-root",
        str(tmp_path / "deployment"),
        "--fixture-python",
        sys.executable,
        "--output",
        str(tmp_path / "output"),
        "--context",
        "acceptance-unit",
        "--ingest-image",
        "ingest:unit",
        "--opds-image",
        "opds:unit",
        "--mariadb-image",
        "mariadb:unit",
    ]


@pytest.mark.parametrize(
    "invalid",
    [
        ["--faults"],
        ["--base-count", "0"],
        ["--base-count", "1000001"],
        ["--append-count", "-1"],
        ["--append-count", "1000001"],
        ["--pages", "0"],
        ["--pages", "4097"],
        ["--phase-seconds", "0"],
        ["--phase-seconds", "nan"],
        ["--phase-seconds", "inf"],
        ["--deadline-seconds", "60"],
        ["--deadline-seconds", "nan"],
        ["--deadline-seconds", "inf"],
    ],
)
def test_cli_rejects_invalid_work_before_allocating_deployment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, invalid: list[str]
) -> None:
    def forbidden(_args: object) -> None:
        raise AssertionError("Invalid arguments reached deployment allocation")

    monkeypatch.setattr(runner, "Acceptance", forbidden)
    with pytest.raises(SystemExit) as error:
        runner.main([*_argv(tmp_path), *invalid])
    assert error.value.code == 2
    assert not (tmp_path / "output").exists()


@pytest.mark.parametrize("cleanup", [True, False])
def test_cli_requires_verified_cleanup_even_after_successful_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup: bool
) -> None:
    seen = []

    def build(args: Any) -> SimpleNamespace:
        seen.append(args)
        return SimpleNamespace(
            run=lambda: 0,
            report={
                "cleanup": {"verified_empty": cleanup},
                "evidence": {"status": "exported"},
                "measurement": {"status": "passed"},
            },
        )

    monkeypatch.setattr(runner, "Acceptance", build)
    monkeypatch.setattr(runner, "require_evidence_support", lambda: None)
    result = runner.main([*_argv(tmp_path), "--faults", "--instrumented"])
    assert result == (0 if cleanup else 1)
    assert len(seen) == 1 and seen[0].faults and seen[0].instrumented


@pytest.mark.parametrize(
    "collection,measurement,expected",
    [
        ("exported", "passed", 0),
        ("exported", "not_requested", 0),
        ("exported", "failed", 1),
        ("exported", None, 1),
        ("failed", "failed", 1),
        (None, None, 1),
    ],
)
def test_cli_requires_evidence_collection_and_requested_measurement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    collection: str | None,
    measurement: str | None,
    expected: int,
) -> None:
    def build(_args: Any) -> SimpleNamespace:
        return SimpleNamespace(
            run=lambda: 0,
            report={
                "cleanup": {"verified_empty": True},
                "evidence": {"status": collection},
                "measurement": {"status": measurement},
            },
        )

    monkeypatch.setattr(runner, "Acceptance", build)
    monkeypatch.setattr(runner, "require_evidence_support", lambda: None)
    assert runner.main(_argv(tmp_path)) == expected


def test_cli_fails_if_fixture_removal_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(runner, "require_evidence_support", lambda: None)
    monkeypatch.setattr(
        runner,
        "Acceptance",
        lambda _args: SimpleNamespace(
            run=lambda: 0,
            report={
                "cleanup": {"verified_empty": True},
                "evidence": {"status": "exported"},
                "measurement": {"status": "not_requested"},
                "fixture_cleanup_error": "permission denied",
            },
        ),
    )
    assert runner.main(_argv(tmp_path)) == 1


def test_cli_rejects_unsupported_host_before_allocating_deployment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def unsupported() -> None:
        raise runner.EvidenceError("Safe evidence access requires POSIX")

    def forbidden(_args: object) -> None:
        raise AssertionError("Unsupported host reached deployment allocation")

    monkeypatch.setattr(runner, "require_evidence_support", unsupported)
    monkeypatch.setattr(runner, "Acceptance", forbidden)
    with pytest.raises(SystemExit) as error:
        runner.main(_argv(tmp_path))
    assert error.value.code == 2
    assert not (tmp_path / "output").exists()


def _log(
    *,
    calls: int = 2,
    replayed: int = 0,
    rows: int = 4,
    operation: str = "validate_file_hash_decision",
    renders: int = 0,
) -> str:
    return (
        "ingest_db_performance event=stage_terminal pipeline=analysis "
        f"operation={operation} generation=1 processed_rows={rows} calls={calls} "
        f"replayed_calls={replayed} sql_calls=1 sql_seconds=0.1\n"
        "Ingest work completed: catalog batches published 1; "
        f"CBZs rendered this work {renders}\n"
    )


def _acceptance(
    tmp_path: Path,
    *,
    logs: list[str],
    verify: Callable[[str], object],
) -> Any:
    result = runner.Acceptance.__new__(runner.Acceptance)
    result.args = SimpleNamespace(phase_seconds=10, http_artifacts=False)
    result.commands = SimpleNamespace(output=tmp_path)
    result.report = {"scenarios": []}
    log_calls = 0

    def next_log(_since: str) -> str:
        nonlocal log_calls
        value = logs[min(log_calls, len(logs) - 1)]
        log_calls += 1
        return value

    def bounded_wait(
        predicate: Callable[[], bool], description: str, *, seconds: float
    ) -> None:
        for _attempt in range(4):
            if predicate():
                return
        raise TimeoutError(description)

    result.logs = next_log
    result.verify = verify
    result.wait = bounded_wait
    result.statuses = lambda: [
        {"Service": service, "Health": "healthy", "State": "running"}
        for service in runner.SERVICES.values()
    ]
    result.http = lambda: {"/health": 200, "/opds/v2": 200}
    return result


def test_phase_requires_completion_and_independent_oracle(tmp_path: Path) -> None:
    calls = []

    def verify(name: str) -> dict[str, Any]:
        calls.append(name)
        return _oracle()

    acceptance = _acceptance(tmp_path, logs=["still scanning", _log()], verify=verify)
    result = acceptance.phase("fresh", lambda: None, require_analysis=True)
    assert result == _oracle()
    assert calls == ["fresh"]
    assert acceptance.report["scenarios"][0]["status"] == "passed"
    assert (tmp_path / "report.json").is_file()


@pytest.mark.parametrize(
    "log",
    [_log(operation="COMPLETE"), _log(replayed=2), _log(rows=0)],
)
def test_phase_rejects_complete_reuse_replay_or_empty_analysis(
    tmp_path: Path, log: str
) -> None:
    acceptance = _acceptance(tmp_path, logs=[log], verify=lambda _name: _oracle())
    with pytest.raises(AssertionError, match="non-replayed analysis"):
        acceptance.phase("fresh", lambda: None, require_analysis=True)
    assert acceptance.report["scenarios"][0]["status"] != "passed"


def test_phase_does_not_treat_failed_oracle_as_success(tmp_path: Path) -> None:
    calls = []

    def verify(name: str) -> None:
        calls.append(name)
        raise RuntimeError("expected source generation has not been published")

    acceptance = _acceptance(tmp_path, logs=[_log()], verify=verify)
    with pytest.raises(TimeoutError):
        acceptance.phase("changed", lambda: None)
    # Another verification requires a new completion, not a retry of the same log.
    assert calls == ["changed"]
    assert acceptance.report["scenarios"][0]["status"] != "passed"


def test_phase_rejects_analysis_that_does_not_exercise_target(tmp_path: Path) -> None:
    acceptance = _acceptance(
        tmp_path,
        logs=[_log(operation="some_other_stage")],
        verify=lambda _name: _oracle(),
    )
    with pytest.raises(AssertionError, match="validate_file_hash_decision"):
        acceptance.phase("fresh", lambda: None, require_analysis=True)


def test_phase_may_wait_for_a_new_completion_after_old_durable_work(
    tmp_path: Path,
) -> None:
    calls = []

    def verify(name: str) -> dict[str, Any]:
        calls.append(name)
        if len(calls) == 1:
            raise RuntimeError("intermediate completed work still has the old title")
        return _oracle()

    acceptance = _acceptance(tmp_path, logs=[_log(), _log() * 2], verify=verify)
    acceptance.phase("changed", lambda: None, require_analysis=True)
    record = acceptance.report["scenarios"][0]
    assert calls == ["changed", "changed"]
    assert len(record["intermediate_oracle_mismatches"]) == 1
    assert record["status"] == "passed"


def test_phase_accounts_for_intermediate_oracle_without_hiding_elapsed_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [100.0]
    attempts = []

    def verify(name: str) -> dict[str, Any]:
        attempts.append(name)
        if len(attempts) == 1:
            clock[0] += 35
            raise RuntimeError("old durable generation")
        clock[0] += 25
        return {**_oracle(), "total_verification_seconds": 25}

    def action() -> None:
        clock[0] += 10

    monkeypatch.setattr(runner.time, "monotonic", lambda: clock[0])
    acceptance = _acceptance(tmp_path, logs=[_log(), _log() * 2], verify=verify)
    acceptance.phase("changed", action, require_analysis=True)
    record = acceptance.report["scenarios"][0]
    # Scenario wall is explicitly an upper bound including the earlier attempt;
    # successful final oracle cost remains separately visible and excluded.
    assert record["work_wall_seconds"] == 45
    assert record["intermediate_oracle_seconds"] == 35
    assert record["oracle"]["total_verification_seconds"] == 25


def test_phase_rejects_missing_oracle_value(tmp_path: Path) -> None:
    acceptance = _acceptance(tmp_path, logs=[_log()], verify=lambda _name: None)
    with pytest.raises(AssertionError, match="No independent oracle"):
        acceptance.phase("fresh", lambda: None)


def test_unchanged_phase_rejects_render_even_when_final_bytes_are_identical(
    tmp_path: Path,
) -> None:
    acceptance = _acceptance(
        tmp_path, logs=[_log(renders=1)], verify=lambda _name: _oracle()
    )
    with pytest.raises(AssertionError, match="rendered CBZs"):
        acceptance.phase("restart", lambda: None, before=_oracle())


def test_runtime_error_prevents_oracle_and_pass(tmp_path: Path) -> None:
    def verify(_name: str) -> None:
        raise AssertionError("Oracle must not run after a runtime error")

    acceptance = _acceptance(tmp_path, logs=["[ERROR] fatal\n" + _log()], verify=verify)
    with pytest.raises(AssertionError, match="Runtime error"):
        acceptance.phase("fresh", lambda: None)


def test_http_download_failure_prevents_scenario_pass(tmp_path: Path) -> None:
    acceptance = _acceptance(tmp_path, logs=[_log()], verify=lambda _name: _oracle())
    acceptance.args.http_artifacts = True

    def reject(_oracle: dict[str, Any]) -> None:
        raise RuntimeError("download checksum differs")

    acceptance.verify_http_artifacts = reject
    with pytest.raises(RuntimeError, match="download checksum differs"):
        acceptance.phase("fresh", lambda: None)
    assert acceptance.report["scenarios"][0]["status"] == "running"


def test_http_probe_budget_finishes_before_outer_command_timeout(
    tmp_path: Path,
) -> None:
    acceptance = _acceptance(tmp_path, logs=[_log()], verify=lambda _name: _oracle())
    commands: list[list[str]] = []

    def compose(command: list[str]) -> str:
        commands.append(command)
        return '{"status":"passed"}'

    acceptance.compose = compose
    reports = runner.Acceptance.verify_http_artifacts(acceptance, _oracle())
    assert reports == [{"status": "passed"}, {"status": "passed"}]
    assert len(commands) == 2
    for command in commands:
        assert command[command.index("--deadline-seconds") + 1] == "45"
        assert "--no-range" not in command
        assert "/acceptance/http_probe.py" in command
