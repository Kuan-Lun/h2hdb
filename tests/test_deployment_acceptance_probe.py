from __future__ import annotations

import importlib.util
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_DIRECTORY = _ROOT / "scripts" / "deployment_acceptance"
_MODULE = "deployment_acceptance_probe_under_test"


def _load_probe() -> ModuleType:
    spec = importlib.util.spec_from_file_location(_MODULE, _DIRECTORY / "probe.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def probe() -> ModuleType:
    # Do not install globally in pytest: subprocesses own all application hooks.
    return _load_probe()


def _environment(evidence: Path | None = None) -> dict[str, str]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("H2HDB_ACCEPTANCE_")
    }
    environment["PYTHONPATH"] = os.pathsep.join([str(_DIRECTORY), str(_ROOT / "src")])
    if evidence is not None:
        environment["H2HDB_ACCEPTANCE_PROBE_DIR"] = str(evidence)
        environment["H2HDB_ACCEPTANCE_SCENARIO"] = "probe-test"
    return environment


def _events(directory: Path) -> list[dict[str, Any]]:
    paths = list(directory.glob("probe-*.jsonl"))
    assert len(paths) == 1
    return [json.loads(line) for line in paths[0].read_text().splitlines()]


def _run(
    source: str, cwd: Path, environment: dict[str, str]
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", source],
        cwd=cwd,
        env=environment,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )


def test_unconfigured_sitecustomize_preserves_application_and_creates_no_evidence(
    tmp_path: Path,
) -> None:
    result = _run(
        "import sys; assert '_h2hdb_acceptance_probe' not in sys.modules; "
        "print(sys.argv); raise SystemExit(7)",
        tmp_path,
        _environment(),
    )
    assert result.returncode == 7
    assert result.stdout == "['-c']\n"
    assert not result.stderr
    assert not list(tmp_path.iterdir())


def test_observer_preserves_return_identity_arguments_and_original_exception(
    probe: ModuleType,
    tmp_path: Path,
) -> None:
    state = probe._Probe(tmp_path, "forwarding")
    expected = object()
    calls: list[tuple[object, int]] = []

    def operation(value: object, *, count: int) -> object:
        calls.append((value, count))
        return value

    observed = probe._observe(
        state,
        "test.success",
        operation,
        boundary=True,
        byte_counter=lambda args, kwargs: kwargs["count"],
    )
    assert observed(expected, count=4) is expected
    assert calls == [(expected, 4)]
    failure = RuntimeError("private-password-do-not-report")

    def failing() -> None:
        raise failure

    failed = probe._observe(state, "test.failure", failing, boundary=True)
    with pytest.raises(RuntimeError) as raised:
        failed()
    assert raised.value is failure
    state.close()
    events = _events(tmp_path)
    counters = events[-1]["counters"]
    assert counters["test.success"]["logical_bytes"] == 4
    assert counters["test.success"]["completed"] == 1
    assert counters["test.failure"]["failed"] == 1
    assert "private-password" not in json.dumps(events)
    assert events[-1]["counters_complete"] is True
    assert events[-1]["counter_tail_status"] == "complete"


def test_evidence_failure_preserves_active_application_exception_and_marks_invalid(
    probe: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = probe._Probe(tmp_path, "observer-failure")
    original = state._write_event
    failure = ValueError("original-private-application-failure")

    def failing_evidence(event: str, operation: str, **details: object) -> None:
        if event == "end":
            raise OSError("simulated evidence disk error")
        original(event, operation, **details)

    def application() -> None:
        raise failure

    monkeypatch.setattr(state, "_write_event", failing_evidence)
    wrapped = probe._observe(state, "test.failure", application, boundary=True)
    with pytest.raises(ValueError) as raised:
        wrapped()
    assert raised.value is failure
    assert any("measurement invalid" in note for note in failure.__notes__)
    state.close()
    assert _events(tmp_path)[-1]["measurement_valid"] is False


def test_real_sqlite_and_fsync_forwarding_separates_observer_io_and_redacts_parameters(
    tmp_path: Path,
) -> None:
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    result = _run(
        """
import os
import sqlite3
import _h2hdb_acceptance_probe as probe

connection = sqlite3.connect('library-activation.sqlite3', timeout=2, isolation_level='DEFERRED')
connection.execute('CREATE TABLE private_table (value TEXT)')
connection.execute('INSERT INTO private_table VALUES (?)', ('private-password',))
connection.commit()
assert connection.execute('SELECT value FROM private_table').fetchone() == ('private-password',)
try:
    connection.execute('SELECT invalid_private_column FROM private_table')
except sqlite3.OperationalError:
    pass
else:
    raise AssertionError('original SQLite failure was suppressed')
connection.rollback()
with connection:
    connection.executemany('INSERT INTO private_table VALUES (?)', [('second',), ('third',)])
connection.close()
class OwnedConnection(sqlite3.Connection):
    pass
owned = sqlite3.connect(':memory:', factory=OwnedConnection)
assert type(owned) is OwnedConnection
owned.close()
with open('application-data', 'wb') as output:
    output.write(b'content')
    output.flush()
    os.fsync(output.fileno())
probe.snapshot('verified')
print('original behavior verified')
""",
        tmp_path,
        _environment(evidence),
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "original behavior verified\n"
    events = _events(evidence)
    final = events[-1]
    assert final["measurement_valid"] is True
    assert final["counters_complete"] is True
    counters = final["counters"]
    assert counters["sqlite.journal.connect"]["calls"] == 1
    assert counters["sqlite.journal.commit"]["calls"] == 1
    assert counters["sqlite.journal.rollback"]["calls"] == 1
    assert counters["sqlite.journal.context_exit"]["completed"] == 1
    assert counters["sqlite.journal.execute"]["calls"] == 4
    assert counters["sqlite.journal.execute"]["failed"] == 1
    assert counters["sqlite.custom_factory.connect"]["completed"] == 1
    assert counters["python_explicit_fsync"]["calls"] == 1
    assert final["probe_evidence_fsync_calls_before_event"] > 1
    assert len(events) < 10  # No synchronous evidence event per SQL or fsync.
    assert "private-password" not in json.dumps(events)
    assert "private_table" not in json.dumps(events)
    assert "invalid_private_column" not in json.dumps(events)
    assert [event["sequence"] for event in events] == list(range(1, len(events) + 1))
    assert all(event["pid"] > 0 and event["monotonic_ns"] > 0 for event in events)


def test_real_core_admin_and_original_semantic_validators_still_detect_drift(
    tmp_path: Path,
) -> None:
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    result = _run(
        """
import sqlite3
from h2hdb import CoreConfig, VNextDatabaseAdminFacade
from h2hdb.config_loader import DatabaseConfig
admin = VNextDatabaseAdminFacade(CoreConfig(database=DatabaseConfig(sql_type='sqlite', database='core.sqlite')))
assert admin.initialize().state == 'READY'
assert admin.check().state == 'READY'
assert admin.check_readiness().state == 'READY'
connection = sqlite3.connect('core.sqlite')
connection.execute('CREATE TABLE foreign_relation (id INTEGER)')
connection.commit()
connection.close()
try:
    admin.check()
except Exception:
    print('original drift validation rejected foreign relation')
else:
    raise AssertionError('original audit was skipped')
""",
        tmp_path,
        _environment(evidence),
    )
    assert result.returncode == 0, result.stderr
    assert "original drift validation rejected" in result.stdout
    counters = _events(evidence)[-1]["counters"]
    assert counters["core.admin.initialize"]["completed"] == 1
    assert counters["core.admin.check"]["completed"] == 1
    assert counters["core.admin.check"]["failed"] == 1
    assert counters["core.admin.check_readiness"]["completed"] == 1
    validators = {
        key: value
        for key, value in counters.items()
        if key.startswith("core.validator.")
    }
    assert validators
    assert all(value["completed"] > 0 for value in validators.values())


def test_core_sql_counts_real_sqlite_calls_outside_performance_context(
    tmp_path: Path,
) -> None:
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    result = _run(
        """
from h2hdb.sqlite_connector import SQLiteConnector, SQLiteDuplicateKeyError
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb import VNextIngestFacade
import _h2hdb_acceptance_probe as probe

# No measure_sql context or driver replacement: exercise the actual connector.
connector = SQLiteConnector('logical-core.sqlite')
connector.connect()
try:
    assert connector.execute('CREATE TABLE private_sql_table (id INTEGER PRIMARY KEY, value TEXT)') is None
    assert connector.execute('INSERT INTO private_sql_table VALUES (%s, %s)', (1, 'private-parameter')) is None
    assert connector.execute_many('INSERT INTO private_sql_table VALUES (%s, %s)', [(2, 'two'), (3, 'three')]) is None
    assert connector.execute_affected('UPDATE private_sql_table SET value = %s WHERE id = %s', ('changed', 2)) == 1
    assert connector.fetch_one('SELECT value FROM private_sql_table WHERE id = %s', (1,)) == ('private-parameter',)
    assert connector.fetch_one('SELECT value FROM private_sql_table WHERE id = %s', (4,)) == ()
    assert connector.fetch_all('SELECT id, value FROM private_sql_table ORDER BY id') == [(1, 'private-parameter'), (2, 'changed'), (3, 'three')]
    try:
        connector.execute('INSERT INTO private_sql_table VALUES (%s, %s)', (1, 'duplicate-private-parameter'))
    except SQLiteDuplicateKeyError:
        pass
    else:
        raise AssertionError('original duplicate-key translation was suppressed')
finally:
    connector.close()
# Wrapper presence is evidence of capability, not a MariaDB execution claim.
for method in ('execute', 'execute_affected', 'execute_many', 'fetch_one', 'fetch_all'):
    assert callable(getattr(MariaDBConnector, method).__wrapped__)
for method in ('prepare_source', 'issue_source_step', 'prepare_source_step', 'commit_source_step'):
    assert callable(getattr(VNextIngestFacade, method).__wrapped__)
probe.snapshot('sql-verified')
print('logical connector behavior verified')
""",
        tmp_path,
        _environment(evidence),
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "logical connector behavior verified\n"
    events = _events(evidence)
    installed = next(event for event in events if event["event"] == "installed")
    assert installed["capabilities"]["core_sqlite_connector"] is True
    assert installed["capabilities"]["core_mariadb_connector"] is True
    assert installed["capabilities"]["core_source"] is True
    counters = events[-1]["counters"]
    expected = {
        "execute": (3, 2, 1),
        "execute_affected": (1, 1, 0),
        "execute_many": (1, 1, 0),
        "fetch_one": (2, 2, 0),
        "fetch_all": (1, 1, 0),
    }
    for method, values in expected.items():
        counter = counters[f"core.sqlite.sql.{method}"]
        assert (counter["calls"], counter["completed"], counter["failed"]) == values
        assert counter["seconds"] >= 0
        assert counter["logical_bytes"] == 0
    assert not any(key.startswith("core.mariadb.sql.") for key in counters)
    assert not any(
        event["operation"].startswith("core.sqlite.sql.") for event in events
    )
    assert len(events) < 10  # Cumulative counters, no event/fsync for each method.
    serialized = json.dumps(events)
    assert (
        "private_sql_table" not in serialized and "private-parameter" not in serialized
    )
    assert events[-1]["counters_complete"] is True


def test_source_facade_hooks_forward_original_calls_and_exceptions_once(
    probe: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = probe._Probe(tmp_path, "source-forwarding")
    calls = []
    result = object()
    policy = object()
    failure = RuntimeError("private-source-arguments")

    class Source:
        pass

    def original(owner: object, *args: object, **kwargs: object) -> object:
        calls.append((owner, args, kwargs))
        if kwargs["fail"]:
            raise failure
        return result

    for method in probe._CORE_SOURCE_METHODS:
        setattr(Source, method, original)
    imported = probe.importlib.import_module

    def load(name: str, package: str | None = None) -> object:
        return (
            SimpleNamespace(VNextIngestFacade=Source)
            if name == "h2hdb"
            else imported(name, package)
        )

    monkeypatch.setattr(probe.importlib, "import_module", load)
    probe._install_core_source(state)
    source = Source()
    for method in probe._CORE_SOURCE_METHODS:
        assert getattr(source, method)(result, policy=policy, fail=False) is result
        with pytest.raises(RuntimeError) as caught:
            getattr(source, method)(result, policy=policy, fail=True)
        assert caught.value is failure
    assert len(calls) == 8
    assert all(
        owner is source and args == (result,) and kwargs["policy"] is policy
        for owner, args, kwargs in calls
    )
    assert state.sequence == 0  # All four source hooks use cumulative snapshots.
    state.close()
    events = _events(tmp_path)
    for method in probe._CORE_SOURCE_METHODS:
        counter = events[-1]["counters"][f"core.source.{method}"]
        assert (counter["calls"], counter["completed"], counter["failed"]) == (2, 1, 1)
    assert "private-source-arguments" not in json.dumps(events)
    assert events[-1]["counters_complete"] is True


@pytest.mark.parametrize("missing", ["h2hdb.mariadb_connector", "mysql.connector"])
def test_optional_mariadb_module_guard_does_not_hide_broken_dependencies(
    probe: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    state = probe._Probe(tmp_path, "backend-capability")

    class FakeSQLite:
        pass

    for method in probe._CORE_SQL_METHODS:
        setattr(FakeSQLite, method, lambda *args, **kwargs: None)
    failure = ModuleNotFoundError("not exposed", name=missing)

    def load(name: str) -> object:
        if name == "h2hdb.sqlite_connector":
            return SimpleNamespace(SQLiteConnector=FakeSQLite)
        assert name == "h2hdb.mariadb_connector"
        raise failure

    monkeypatch.setattr(probe.importlib, "import_module", load)
    try:
        if missing == "h2hdb.mariadb_connector":
            assert probe._install_core_sql(state) == {"sqlite": True, "mariadb": False}
        else:
            with pytest.raises(ModuleNotFoundError) as caught:
                probe._install_core_sql(state)
            assert caught.value is failure
    finally:
        state.close()


@pytest.mark.parametrize("invalid", ["missing", "relative", "control_without_probe"])
def test_broken_setup_fails_before_application_instead_of_being_ignored(
    tmp_path: Path,
    invalid: str,
) -> None:
    environment = _environment(tmp_path / "missing")
    if invalid == "relative":
        environment["H2HDB_ACCEPTANCE_PROBE_DIR"] = "."
    elif invalid == "control_without_probe":
        del environment["H2HDB_ACCEPTANCE_PROBE_DIR"]
        environment["H2HDB_ACCEPTANCE_CONTROL_DIR"] = str(tmp_path)
    result = _run("print('APPLICATION_MUST_NOT_START')", tmp_path, environment)
    assert result.returncode != 0
    assert "APPLICATION_MUST_NOT_START" not in result.stdout
    assert "H2HDB_ACCEPTANCE_PROBE_INSTALL_FAILED" in result.stderr


def _wait_for_event(
    process: subprocess.Popen[str],
    evidence: Path,
    event: str,
    operation: str | None = None,
) -> dict[str, Any]:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            pytest.fail(
                f"child exited before {event}: {process.returncode}; {stdout}; {stderr}"
            )
        paths = list(evidence.glob("probe-*.jsonl"))
        if paths:
            # A concurrent append can expose an incomplete last line to a reader.
            data = paths[0].read_bytes()
            for line in data.splitlines(keepends=True):
                if line.endswith(b"\n"):
                    item = json.loads(line)
                    if item["event"] == event and (
                        operation is None or item["operation"] == operation
                    ):
                        return dict(item)
        time.sleep(0.01)
    pytest.fail(f"timed out waiting for probe event {event}")


@pytest.mark.skipif(
    os.name != "posix", reason="Tests actual POSIX SIGKILL evidence truncation"
)
def test_sigkill_preserves_stage_entry_without_claiming_complete_counters(
    tmp_path: Path,
) -> None:
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import time; import _h2hdb_acceptance_probe as p; "
            "p._STATE.call('test.blocking', lambda: time.sleep(60), boundary=True)",
        ],
        cwd=tmp_path,
        env=_environment(evidence),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_event(process, evidence, "start", "test.blocking")
        process.kill()
        process.communicate(timeout=5)
        assert process.returncode == -signal.SIGKILL
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
    events = _events(evidence)
    stage = [event for event in events if event["operation"] == "test.blocking"]
    assert [event["event"] for event in stage] == ["start"]
    assert stage[0]["counter_tail_status"] == "unsealed"
    assert stage[0]["active_spans"][0]["operation"] == "test.blocking"
    assert not any(event["event"] == "process_exit" for event in events)


def test_fault_gate_runs_original_first_and_hits_each_token_only_once(
    probe: ModuleType,
    tmp_path: Path,
) -> None:
    evidence = tmp_path / "evidence"
    control = tmp_path / "control"
    evidence.mkdir()
    control.mkdir()
    operation = "library.commit_pending_install"
    (control / "arm.json").write_text(
        json.dumps({"operation": operation, "token": "once"})
    )
    state = probe._Probe(evidence, "fault-test", control)
    calls: list[int] = []
    expected = object()

    def original(number: int) -> object:
        calls.append(number)
        # Only the original action can release this gate in this unit fixture.
        if calls == [1]:
            assert not any(
                event["event"] == "fault_reached" for event in _events(evidence)
            )
        (control / "release-once").touch()
        return expected

    state.emit("start", "fixture")
    observed = probe._observe(
        state, operation, original, boundary=False, fault_gate=True
    )
    assert observed(1) is expected
    (control / "release-once").unlink()
    assert observed(2) is expected
    assert calls == [1, 2]
    state.close()
    events = _events(evidence)
    reached = [event for event in events if event["event"] == "fault_reached"]
    assert len(reached) == 1
    assert reached[0]["token"] == "once"
    assert reached[0]["fault_injection"] is True
    assert reached[0]["counters"][operation]["completed"] == 1


def test_fault_gate_timeout_is_bounded_and_invalidates_measurement(
    probe: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = tmp_path / "evidence"
    control = tmp_path / "control"
    evidence.mkdir()
    control.mkdir()
    operation = "library.commit_pending_install"
    (control / "arm.json").write_text(
        json.dumps({"operation": operation, "token": "timeout"})
    )
    monkeypatch.setattr(probe, "_FAULT_TIMEOUT_SECONDS", 0.01)
    state = probe._Probe(evidence, "fault-test", control)
    observed = probe._observe(
        state, operation, lambda: None, boundary=False, fault_gate=True
    )
    with pytest.raises(TimeoutError, match="not released"):
        observed()
    state.close()
    events = _events(evidence)
    assert any(event["event"] == "fault_timeout" for event in events)
    assert events[-1]["measurement_valid"] is False
    assert events[-1]["counters_complete"] is False


@pytest.mark.parametrize(
    "arm",
    [
        {"operation": "unknown", "token": "one"},
        {"operation": "library.commit_pending_install", "token": "../escape"},
        {"operation": "library.commit_pending_install", "token": "one", "extra": True},
    ],
)
def test_fault_gate_rejects_unknown_target_or_unsafe_token(
    probe: ModuleType,
    tmp_path: Path,
    arm: dict[str, object],
) -> None:
    control = tmp_path / "control"
    control.mkdir()
    (control / "arm.json").write_text(json.dumps(arm))
    state = probe._Probe(tmp_path, "invalid-fault", control)
    try:
        with pytest.raises(ValueError, match="invalid"):
            state.fault_gate("library.commit_pending_install")
    finally:
        state.close()


@pytest.mark.skipif(
    os.name != "posix",
    reason="Verifies forwarding to the application's POSIX SIGTERM handler",
)
def test_fault_wait_does_not_replace_application_sigterm_handler(
    tmp_path: Path,
) -> None:
    evidence = tmp_path / "evidence"
    control = tmp_path / "control"
    evidence.mkdir()
    control.mkdir()
    (control / "arm.json").write_text(
        json.dumps(
            {
                "operation": "library.commit_pending_install",
                "token": "term",
            }
        )
    )
    # Exercise the gate with an ordinary app-owned signal handler in a real process.
    # No ingest package or production journal is needed for this forwarding contract.
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            """
import atexit
from pathlib import Path
import signal
import probe
state = probe._Probe(Path('evidence').resolve(), 'term-test', Path('control').resolve())
atexit.register(state.close)
received = []
def handler(number, frame):
    received.append(number)
    Path('signal-received').touch()
signal.signal(signal.SIGTERM, handler)
def original():
    Path('original-completed').touch()
    return 42
observed = probe._observe(state, 'library.commit_pending_install', original, boundary=False, fault_gate=True)
assert observed() == 42
assert received == [signal.SIGTERM]
assert signal.getsignal(signal.SIGTERM) is handler
print('application gracefully stopped')
""",
        ],
        cwd=tmp_path,
        env=_environment(),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_event(process, evidence, "fault_reached")
        assert (tmp_path / "original-completed").is_file()
        (control / "arm.json").unlink()
        process.send_signal(signal.SIGTERM)
        deadline = time.monotonic() + 5
        while not (tmp_path / "signal-received").is_file():
            assert time.monotonic() < deadline
            time.sleep(0.01)
        assert process.poll() is None
        (control / "release-term").touch()
        stdout, stderr = process.communicate(timeout=5)
        assert process.returncode == 0, stderr
        assert stdout == "application gracefully stopped\n"
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
    events = _events(evidence)
    assert events[-1]["measurement_valid"] is True
    assert any(event["event"] == "fault_released" for event in events)
