"""Offline orchestration contracts; these tests never run Docker or a gallery job."""

from __future__ import annotations

import copy
import importlib
import importlib.util
import json
import os
import shutil
import subprocess
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


def _observer_package(tmp_path: Path, *, complete: bool = True) -> Path:
    root = tmp_path / "mounted-acceptance"
    package = root / "deployment_acceptance"
    package.mkdir(parents=True)
    source = Path(__file__).resolve().parents[1] / "scripts/deployment_acceptance"
    names = ["__init__.py", "http_observer.py"]
    if complete:
        names.append("http_probe.py")
    for name in names:
        shutil.copy2(source / name, package / name)
    return root


@pytest.mark.skipif(
    os.name != "posix",
    reason="Compose acceptance uses a Linux env command on a POSIX host",
)
def test_observer_package_launches_without_ambient_pythonpath(tmp_path: Path) -> None:
    root = _observer_package(tmp_path)
    receipt = tmp_path / "http-result-test.json"
    environment = {"PATH": os.defpath}
    assert "PYTHONPATH" not in environment
    result = subprocess.run(
        runner.http_observer_command(
            ["--help"], result_path=receipt, package_root=root, python=sys.executable
        ),
        cwd=tmp_path,
        env=environment,
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "--control-directory" in result.stdout
    assert "--evidence-directory" in result.stdout
    assert not receipt.exists()


@pytest.mark.skipif(
    os.name != "posix",
    reason="Compose acceptance uses a Linux env command on a POSIX host",
)
@pytest.mark.parametrize("broken_import", [True, False])
def test_observer_bootstrap_records_import_or_argument_failure_before_ready(
    tmp_path: Path,
    broken_import: bool,
) -> None:
    root = _observer_package(tmp_path, complete=not broken_import)
    receipt = tmp_path / "http-result-test.json"
    result = subprocess.run(
        runner.http_observer_command(
            ["--help"] if broken_import else ["--invalid"],
            result_path=receipt,
            package_root=root,
            python=sys.executable,
        ),
        cwd=tmp_path,
        env={"PATH": os.defpath},
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert result.returncode != 0
    failure = json.loads(receipt.read_text())
    assert failure["status"] == "failed" and failure["stage"] == "observer-launch"
    assert failure["failure"].startswith("Observer bootstrap failed: ")
    assert not (tmp_path / "http-ready-test.json").exists()


def test_observer_accepts_deployment_advertised_origin_using_shared_loopback_policy() -> (
    None
):
    assert runner.__package__ is not None
    observer = importlib.import_module(runner.__package__ + ".http_observer")
    base = observer.http._base("http://127.0.0.1:8000")
    assert (
        observer.http._acquisition_url(
            base, "http://h2hdb-opds:8000/opds/v2/acquisition?revision=7&gid=1"
        )
        == "http://127.0.0.1:8000/opds/v2/acquisition?revision=7&gid=1"
    )


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


def _cleanup_fault_events() -> list[dict[str, Any]]:
    operation = "core.cleanup.committed_nonempty_shard"
    facts = [
        {
            "event": "cleanup_shard_committed",
            "operation": operation,
            "row_count": 5,
            "cycle_complete": False,
            "after_ingest_generation": None,
        },
        {
            "event": "ingest_completed",
            "ingest_generation": 14,
            "publication_terminal": True,
            "replayed": False,
        },
        {
            "event": "cleanup_shard_committed",
            "operation": operation,
            "row_count": 19,
            "cycle_complete": False,
            "after_ingest_generation": 14,
        },
        {
            "event": "fault_reached",
            "operation": operation,
            "fault_injection": True,
            "completed_ingest_generation": 14,
            "required_after_ingest_generation": 13,
            "committed_shard_sequence": 3,
        },
    ]
    return [
        {**fact, "sequence": sequence, "process_instance": "restarted-ingest"}
        for sequence, fact in enumerate(facts, 1)
    ]


def test_cleanup_fault_uses_exact_cause_despite_startup_shards_with_no_generation() -> (
    None
):
    events = _cleanup_fault_events()
    result = runner.cleanup_fault_evidence(events, events[-1], prior_generation=13)
    assert result["committed_shard_sequence"] == 3
    assert result["completion_sequence"] == 2
    assert result["ingest_generation"] == 14 and result["row_count"] == 19


@pytest.mark.parametrize("cause", [1, 2, 4, 999, None, True])
def test_cleanup_fault_rejects_wrong_cause_instead_of_searching_other_good_shards(
    cause: object,
) -> None:
    events = _cleanup_fault_events()
    events[-1]["committed_shard_sequence"] = cause
    with pytest.raises(AssertionError, match="Cleanup fault"):
        runner.cleanup_fault_evidence(events, events[-1], prior_generation=13)


@pytest.mark.parametrize("generation", [None, 12, 13, True])
def test_cleanup_fault_rejects_a_gate_before_the_required_publication(
    generation: object,
) -> None:
    events = _cleanup_fault_events()
    events[-1]["completed_ingest_generation"] = generation
    with pytest.raises(AssertionError, match="fresh generation"):
        runner.cleanup_fault_evidence(events, events[-1], prior_generation=13)


def _oracle() -> dict[str, Any]:
    return {
        "revision": 1,
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
        ["--cleanup-faults"],
        ["--concurrent-http"],
        ["--instrumented", "--concurrent-http", "--phase-seconds", "3481"],
        ["--base-count", "0"],
        ["--base-count", "1000001"],
        ["--append-count", "-1"],
        ["--append-count", "1000001"],
        ["--pages", "0"],
        ["--pages", "4097"],
        ["--growth-batches", "-1"],
        ["--growth-batches", "4"],
        ["--growth-batches", "1"],
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
    assert seen[0].growth_batches == 0


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
        "ingest_progress event=work_finished status=completed "
        "counter.publication_batches_finalized=1 "
        f"counter.archives_rendered={renders}\n"
    )


def _acceptance(
    tmp_path: Path,
    *,
    logs: list[str],
    verify: Callable[[str], object],
) -> Any:
    result = runner.Acceptance.__new__(runner.Acceptance)
    result.args = SimpleNamespace(
        phase_seconds=10,
        http_artifacts=False,
        instrumented=False,
        concurrent_http=False,
    )
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


def test_instrumented_phase_cannot_pass_at_publication_without_cleanup(
    tmp_path: Path,
) -> None:
    acceptance = _acceptance(tmp_path, logs=[_log()], verify=lambda _name: _oracle())
    acceptance.args.instrumented = True
    acceptance.probe_events = list
    with pytest.raises(TimeoutError, match="cleanup DONE"):
        acceptance.phase("fresh", lambda: None, require_analysis=True)
    assert acceptance.report["scenarios"][0]["status"] == "running"


def test_instrumented_phase_waits_for_cleanup_after_independent_oracle(
    tmp_path: Path,
) -> None:
    order: list[str] = []

    def verify(_name: str) -> dict[str, Any]:
        order.append("oracle")
        return _oracle()

    acceptance = _acceptance(tmp_path, logs=[_log()], verify=verify)
    acceptance.args.instrumented = True
    acceptance.probe_events = list

    def cleanup(record: dict[str, Any], boundary: dict[str, int]) -> None:
        assert order == ["oracle"]
        assert boundary == {}
        order.append("cleanup-DONE")
        record["catalog_cleanup"] = {"status": "passed"}

    acceptance.await_cleanup = cleanup
    acceptance.phase("fresh", lambda: None)
    assert order == ["oracle", "cleanup-DONE", "oracle"]
    assert acceptance.report["scenarios"][0]["catalog_cleanup"]["status"] == "passed"


def test_cleanup_must_preserve_full_oracle_bytes_after_done(tmp_path: Path) -> None:
    count = 0

    def verify(_name: str) -> dict[str, Any]:
        nonlocal count
        count += 1
        result = _oracle()
        if count == 2:
            result["artifacts"][0]["sha256"] = "f" * 64
        return result

    acceptance = _acceptance(tmp_path, logs=[_log()], verify=verify)
    acceptance.args.instrumented = True
    acceptance.probe_events = list
    acceptance.await_cleanup = lambda record, _boundary: record.update(
        catalog_cleanup={"status": "passed"}
    )
    with pytest.raises(AssertionError, match="rewrote artifact"):
        acceptance.phase("fresh", lambda: None)
    assert count == 2
    assert acceptance.report["scenarios"][0]["status"] != "passed"


def test_baseline_does_not_claim_cleanup_acceptance(tmp_path: Path) -> None:
    acceptance = _acceptance(tmp_path, logs=[_log()], verify=lambda _name: _oracle())
    acceptance.phase("baseline", lambda: None)
    assert (
        acceptance.report["scenarios"][0]["catalog_cleanup"]["status"] == "not_observed"
    )


def test_concurrent_http_is_held_through_cleanup_and_post_cleanup_oracle(
    tmp_path: Path,
) -> None:
    order: list[str] = []

    def verify(_name: str) -> dict[str, Any]:
        order.append("oracle")
        return _oracle()

    acceptance = _acceptance(tmp_path, logs=[_log()], verify=verify)
    acceptance.args.instrumented = True
    acceptance.args.concurrent_http = True
    acceptance._last_oracle = _oracle()
    acceptance.probe_events = list

    def start(_oracle: dict[str, Any]) -> str:
        order.append("held")
        return "token"

    def cleanup(record: dict[str, Any], _boundary: dict[str, int]) -> None:
        order.append("DONE")
        record["catalog_cleanup"] = {"status": "passed"}

    def finish(token: str, oracle: dict[str, Any]) -> dict[str, Any]:
        assert token == "token" and oracle == _oracle()
        assert order == ["held", "action", "oracle", "DONE", "oracle"]
        order.append("released")
        return {"status": "passed"}

    acceptance.start_http_observer = start
    acceptance.await_cleanup = cleanup
    acceptance.finish_http_observer = finish
    acceptance.phase("append", lambda: order.append("action"))
    assert order[-1] == "released"
    assert acceptance.report["scenarios"][0]["concurrent_http"]["status"] == "passed"


def test_detached_observer_response_loss_still_retains_gate_release_capability(
    tmp_path: Path,
) -> None:
    acceptance = _acceptance(tmp_path, logs=[], verify=lambda _name: _oracle())
    control = tmp_path / "control"
    control.mkdir()
    acceptance.prepared = SimpleNamespace(control_dir=control)

    def response_lost(_command: list[str]) -> str:
        raise RuntimeError("detached process may already have started")

    acceptance.compose = response_lost
    with pytest.raises(RuntimeError, match="already have started"):
        acceptance.start_http_observer(_oracle())
    token = acceptance._http_observer_token
    assert token
    acceptance.release_http_observer()
    assert (control / f"stream-release-{token}").is_file()
    assert not (control / "stream-arm.json").exists()


def test_cleanup_fault_arms_the_latest_completed_generation_before_new_input(
    tmp_path: Path,
) -> None:
    acceptance = _acceptance(tmp_path, logs=[], verify=lambda _name: _oracle())
    control = tmp_path / "control"
    control.mkdir()
    acceptance.prepared = SimpleNamespace(control_dir=control)
    acceptance.report["scenarios"] = [{"catalog_cleanup": {"ingest_generation": 13}}]

    def generate(count: int, gid: int) -> None:
        assert (count, gid) == (1, 1_000_014)
        arm = json.loads((control / "arm.json").read_text())
        assert set(arm) == {"operation", "token", "after_ingest_generation"}
        assert arm["operation"] == "core.cleanup.committed_nonempty_shard"
        assert arm["after_ingest_generation"] == 13
        raise RuntimeError("stop before any Docker wait or signal")

    acceptance.generate = generate
    with pytest.raises(RuntimeError, match="stop before"):
        acceptance.fault("SIGKILL", 1_000_014, cleanup=True)


def test_growth_rounds_add_equal_inputs_and_keep_actual_generation_evidence(
    tmp_path: Path,
) -> None:
    acceptance = _acceptance(tmp_path, logs=[], verify=lambda _name: _oracle())
    acceptance.args = SimpleNamespace(
        growth_batches=3,
        append_count=2,
        pages=129,
        image_profile="small",
        fixture_python=Path("/cohort/python"),
    )
    acceptance.prepared = SimpleNamespace(source_dir=tmp_path / "source")
    commands: list[list[str]] = []
    acceptance.commands.run = lambda argv, **_kwargs: commands.append(argv)
    phases = []

    def phase(name: str, action: Callable[[], None], **kwargs: Any) -> None:
        action()
        phases.append((name, kwargs))

    acceptance.phase = phase
    assert acceptance.growth_rounds(1_000_005) == 1_000_011
    assert [name for name, _ in phases] == [
        "growth-append-1",
        "growth-append-2",
        "growth-append-3",
    ]
    assert [values["growth_galleries"] for _, values in phases] == [6, 8, 10]
    assert all(values["require_analysis"] for _, values in phases)
    assert [row[row.index("--start-gid") + 1] for row in commands] == [
        "1000005",
        "1000007",
        "1000009",
    ]
    assert all("append-collection" in row for row in commands)
    assert all(row[row.index("--count") + 1] == "2" for row in commands)


def test_growth_phase_cannot_pass_without_complete_phase_evidence(
    tmp_path: Path,
) -> None:
    oracle = {
        **_oracle(),
        "actual_source_galleries": 2,
        "actual_source_pages": 258,
        "verified_pages": 258,
    }
    acceptance = _acceptance(
        tmp_path, logs=[_log(rows=258)], verify=lambda _name: oracle
    )
    acceptance.args.pages = 129
    with pytest.raises(AssertionError, match="completed analysis/publication"):
        acceptance.phase(
            "growth-append-1", lambda: None, require_analysis=True, growth_galleries=2
        )
    assert acceptance.report["scenarios"][0]["status"] != "passed"


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
