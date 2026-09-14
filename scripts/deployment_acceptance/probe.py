"""Opt-in observations for disposable acceptance processes, never shipped runtime.

Wall time from an uninstrumented run remains the performance reference. These
wrappers execute the original operation exactly once; observer work and evidence
fsyncs are separate counters. Python fsync calls exclude SQLite/native syscalls.
Logical byte counts describe successful hash/verification passes, not physical
disk reads. A log without ``process_exit`` has an unknown, incomplete tail.
"""

from __future__ import annotations

import atexit
import functools
import importlib
import importlib.metadata
import json
import os
import re
import sqlite3
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, cast

_ORIGINAL_FSYNC = os.fsync
_ORIGINAL_CONNECT = sqlite3.connect
_CONNECTION = sqlite3.Connection
_INTERVAL_SECONDS = 5.0
_STATE: _Probe | None = None
_LABEL = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
_TOKEN = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
_FAULT_OPERATION = "library.commit_pending_installs"
_FAULT_TIMEOUT_SECONDS = 60.0
_CORE_SQL_METHODS = (
    "execute",
    "execute_affected",
    "execute_many",
    "fetch_one",
    "fetch_all",
)
_CORE_SOURCE_METHODS = (
    "prepare_source",
    "issue_source_step",
    "prepare_source_step",
    "commit_source_step",
)


def _require_directory(directory: Path) -> None:
    if not directory.is_absolute() or directory.is_symlink():
        raise ValueError("probe directory must be an absolute real directory")
    if not directory.is_dir():
        raise ValueError("probe directory must already exist")


@dataclass
class _Counter:
    calls: int = 0
    completed: int = 0
    failed: int = 0
    seconds: float = 0.0
    logical_bytes: int = 0

    def record(self) -> dict[str, int | float]:
        return {
            "calls": self.calls,
            "completed": self.completed,
            "failed": self.failed,
            "seconds": self.seconds,
            "logical_bytes": self.logical_bytes,
        }


class _Probe:
    def __init__(
        self, directory: Path, scenario: str, control: Path | None = None
    ) -> None:
        _require_directory(directory)
        if control is not None:
            _require_directory(control)
        if _LABEL.fullmatch(scenario) is None:
            raise ValueError("probe scenario must be a short identifier")
        self.scenario = scenario
        self.control = control
        self.fault_tokens: set[str] = set()
        self.pid = os.getpid()
        self.lock = threading.RLock()
        self.counters: dict[str, _Counter] = {}
        self.sequence = 0
        self.span_sequence = 0
        self.active: dict[int, dict[str, int | str]] = {}
        self.observer_seconds = 0.0
        self.evidence_fsync_calls = 0
        self.failure: Exception | None = None
        self.stopped = threading.Event()
        self.monitor: threading.Thread | None = None
        self.closed = False
        self.path = directory / f"probe-{self.pid}-{time.time_ns()}.jsonl"
        self.descriptor = os.open(
            self.path,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | os.O_APPEND
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        # Persist the evidence name independently of the application's storage.
        try:
            parent = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                _ORIGINAL_FSYNC(parent)
                self.evidence_fsync_calls += 1
            finally:
                os.close(parent)
        except BaseException:
            os.close(self.descriptor)
            raise

    def fault_gate(self, operation: str) -> None:
        """Pause only after an original operation returns, in fault runs alone."""
        if self.control is None:
            return
        arm = self.control / "arm.json"
        try:
            with arm.open("rb") as source:
                raw = source.read(4097)
        except FileNotFoundError:
            return
        if len(raw) > 4096:
            raise ValueError("acceptance fault arm is too large")
        declaration = json.loads(raw)
        if not isinstance(declaration, dict) or set(declaration) != {
            "operation",
            "token",
        }:
            raise ValueError("acceptance fault arm has an invalid shape")
        token = declaration["token"]
        if (
            declaration["operation"] != _FAULT_OPERATION
            or not isinstance(token, str)
            or _TOKEN.fullmatch(token) is None
        ):
            raise ValueError("acceptance fault arm target or token is invalid")
        if operation != declaration["operation"]:
            return
        with self.lock:
            if token in self.fault_tokens:
                return
            self.fault_tokens.add(token)
        self.emit("fault_reached", operation, token=token, fault_injection=True)
        started = time.monotonic()
        release = self.control / f"release-{token}"
        while not release.is_file():
            if time.monotonic() - started >= _FAULT_TIMEOUT_SECONDS:
                self.emit("fault_timeout", operation, token=token, fault_injection=True)
                raise TimeoutError(
                    "acceptance fault gate was not released within 60 seconds"
                )
            time.sleep(0.05)
        self.emit("fault_released", operation, token=token, fault_injection=True)

    def emit(self, event: str, operation: str, **details: object) -> None:
        try:
            self._write_event(event, operation, **details)
        except Exception as error:
            self.report_failure(error)
            raise

    def _write_event(self, event: str, operation: str, **details: object) -> None:
        started = time.perf_counter()
        with self.lock:
            if self.closed:
                raise RuntimeError("acceptance probe evidence is closed")
            self.sequence += 1
            record = {
                "schema_version": 1,
                "scenario": self.scenario,
                "pid": self.pid,
                "process_instance": self.path.stem,
                "thread_id": threading.get_ident(),
                "sequence": self.sequence,
                "monotonic_ns": time.monotonic_ns(),
                "event": event,
                "operation": operation,
                "counter_tail_status": "unsealed",
                "counters": {
                    name: counter.record()
                    for name, counter in sorted(self.counters.items())
                },
                "active_spans": list(self.active.values()),
                "probe_self_io_seconds_before_event": self.observer_seconds,
                "probe_evidence_fsync_calls_before_event": self.evidence_fsync_calls,
                **details,
            }
            payload = (json.dumps(record, sort_keys=True) + "\n").encode("utf-8")
            while payload:
                written = os.write(self.descriptor, payload)
                if written <= 0:
                    raise OSError("acceptance probe evidence write did not advance")
                payload = payload[written:]
            _ORIGINAL_FSYNC(self.descriptor)
            self.evidence_fsync_calls += 1
            self.observer_seconds += time.perf_counter() - started

    def report_failure(self, error: Exception) -> None:
        self.failure = error
        # No exception message, SQL, application arguments or credentials escape.
        os.write(
            2,
            f"H2HDB_ACCEPTANCE_PROBE_FAILED {type(error).__name__}\n".encode("ascii"),
        )

    def call[T](
        self,
        operation: str,
        action: Callable[[], T],
        *,
        boundary: bool = False,
        logical_bytes: Callable[[T], int] | None = None,
    ) -> T:
        if self.failure is not None:
            raise RuntimeError("acceptance probe previously failed") from self.failure
        with self.lock:
            counter = self.counters.setdefault(operation, _Counter())
            counter.calls += 1
            self.span_sequence += 1
            span = self.span_sequence
            if boundary:
                self.active[span] = {
                    "span_id": span,
                    "thread_id": threading.get_ident(),
                    "operation": operation,
                    "started_monotonic_ns": time.monotonic_ns(),
                }
        if boundary:
            self.emit("start", operation, span_id=span)
        started = time.perf_counter()
        try:
            result = action()
        except BaseException as error:
            with self.lock:
                counter.failed += 1
                counter.seconds += time.perf_counter() - started
                self.active.pop(span, None)
            if boundary:
                try:
                    self.emit(
                        "end",
                        operation,
                        span_id=span,
                        outcome="failed",
                        exception_type=type(error).__name__,
                    )
                except Exception as observer_error:
                    self.report_failure(observer_error)
                    error.add_note(
                        "Acceptance probe evidence failed; measurement invalid"
                    )
            raise
        with self.lock:
            counter.completed += 1
            counter.seconds += time.perf_counter() - started
            if logical_bytes is not None:
                count = logical_bytes(result)
                if type(count) is not int or count < 0:
                    raise ValueError("probe logical byte counter is invalid")
                counter.logical_bytes += count
            self.active.pop(span, None)
        if boundary:
            self.emit("end", operation, span_id=span, outcome="completed")
        return result

    def start_monitor(self) -> None:
        def monitor() -> None:
            try:
                while not self.stopped.wait(_INTERVAL_SECONDS):
                    self.emit("snapshot", "periodic")
            except Exception as error:
                self.report_failure(error)

        self.monitor = threading.Thread(
            target=monitor, name="acceptance-probe-observer", daemon=True
        )
        self.monitor.start()

    def close(self) -> None:
        if self.closed:
            return
        self.stopped.set()
        if self.monitor is not None:
            self.monitor.join(timeout=_INTERVAL_SECONDS + 1)
            if self.monitor.is_alive():
                self.report_failure(RuntimeError("probe observer did not stop"))
        try:
            with self.lock:
                complete = (
                    self.failure is None
                    and not self.active
                    and all(
                        counter.calls == counter.completed + counter.failed
                        for counter in self.counters.values()
                    )
                )
            self.emit(
                "process_exit",
                "process",
                measurement_valid=self.failure is None,
                counters_complete=complete,
                counter_tail_status="complete" if complete else "incomplete",
            )
        except Exception as error:
            self.report_failure(error)
        finally:
            with self.lock:
                self.closed = True
                os.close(self.descriptor)


def _observe[**P, T](
    state: _Probe,
    operation: str,
    original: Callable[P, T],
    *,
    boundary: bool,
    byte_counter: Callable[[tuple[Any, ...], dict[str, Any]], int] | None = None,
    result_bytes: Callable[[T], int] | None = None,
    fault_gate: bool = False,
) -> Callable[P, T]:
    @functools.wraps(original)
    def observed(*args: P.args, **kwargs: P.kwargs) -> T:
        result = state.call(
            operation,
            lambda: original(*args, **kwargs),
            boundary=boundary,
            logical_bytes=(
                result_bytes
                if byte_counter is None
                else lambda result: byte_counter(args, kwargs)
            ),
        )
        if fault_gate:
            try:
                state.fault_gate(operation)
            except Exception as error:
                state.report_failure(error)
                raise
        return result

    return observed


def _hook(
    state: _Probe,
    owner: object,
    attribute: str,
    operation: str,
    *,
    boundary: bool = True,
    byte_counter: Callable[[tuple[Any, ...], dict[str, Any]], int] | None = None,
    result_bytes: Callable[[Any], int] | None = None,
    fault_gate: bool = False,
) -> None:
    original = getattr(owner, attribute)
    if not callable(original):
        raise TypeError(f"acceptance probe target is not callable: {operation}")
    setattr(
        owner,
        attribute,
        _observe(
            state,
            operation,
            original,
            boundary=boundary,
            byte_counter=byte_counter,
            result_bytes=result_bytes,
            fault_gate=fault_gate,
        ),
    )


def _database_kind(database: object) -> str:
    if database == ":memory:":
        return "memory"
    if isinstance(database, str | os.PathLike):
        if Path(database).name == "library-activation.sqlite3":
            return "journal"
    return "other"


def _install_sqlite(state: _Probe) -> None:
    class ObservedConnection(_CONNECTION):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.probe_kind = _database_kind(
                args[0] if args else kwargs.get("database")
            )
            super().__init__(*args, **kwargs)

        def execute(self, *args: Any, **kwargs: Any) -> sqlite3.Cursor:
            return state.call(
                f"sqlite.{self.probe_kind}.execute",
                lambda: _CONNECTION.execute(self, *args, **kwargs),
            )

        def executemany(self, *args: Any, **kwargs: Any) -> sqlite3.Cursor:
            return state.call(
                f"sqlite.{self.probe_kind}.executemany",
                lambda: _CONNECTION.executemany(self, *args, **kwargs),
            )

        def executescript(self, *args: Any, **kwargs: Any) -> sqlite3.Cursor:
            return state.call(
                f"sqlite.{self.probe_kind}.executescript",
                lambda: _CONNECTION.executescript(self, *args, **kwargs),
            )

        def commit(self) -> None:
            state.call(
                f"sqlite.{self.probe_kind}.commit", lambda: _CONNECTION.commit(self)
            )

        def rollback(self) -> None:
            state.call(
                f"sqlite.{self.probe_kind}.rollback", lambda: _CONNECTION.rollback(self)
            )

        def close(self) -> None:
            state.call(
                f"sqlite.{self.probe_kind}.close", lambda: _CONNECTION.close(self)
            )

        def __exit__(self, *args: Any, **kwargs: Any) -> Literal[False]:
            return state.call(
                f"sqlite.{self.probe_kind}.context_exit",
                lambda: _CONNECTION.__exit__(self, *args, **kwargs),
            )

    def connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        kind = _database_kind(args[0] if args else kwargs.get("database"))
        supplied = args[5] if len(args) > 5 else kwargs.get("factory", _CONNECTION)
        if supplied is _CONNECTION:
            if len(args) > 5:
                args = (*args[:5], ObservedConnection, *args[6:])
            else:
                kwargs = {**kwargs, "factory": ObservedConnection}
        else:
            # Respect caller-owned factories exactly; do not create a new MRO.
            kind = "custom_factory"
        return cast(
            sqlite3.Connection,
            state.call(
                f"sqlite.{kind}.connect", lambda: _ORIGINAL_CONNECT(*args, **kwargs)
            ),
        )

    setattr(sqlite3, "connect", connect)  # noqa: B010 - Runtime wrapper retains all SQLite connect overloads.
    os.fsync = _observe(state, "python_explicit_fsync", _ORIGINAL_FSYNC, boundary=False)


def _install_core(state: _Probe) -> None:
    admin = importlib.import_module("h2hdb.schema_admin")
    provider = importlib.import_module("h2hdb.vnext_schema_provider")
    for method in ("initialize", "check", "check_readiness"):
        _hook(state, admin.VNextSchemaAdmin, method, f"core.admin.{method}")
    for method in ("validate_global", "validate_bootstrap_seeds", "validate_semantics"):
        _hook(
            state,
            provider.GeneratedVNextSchemaProvider,
            method,
            f"core.schema.{method}",
        )
    original_loader = provider._load_builtin_semantic_validators

    @functools.wraps(original_loader)
    def load_validators() -> Mapping[str, Callable[..., object]]:
        # The original wheel registry still owns every ID and original validator.
        # Do not replace providers, reorder validation, or accept unknown IDs.
        validators = cast(Mapping[str, Callable[..., object]], original_loader())
        return MappingProxyType(
            {
                key: _observe(state, f"core.validator.{key}", validator, boundary=True)
                for key, validator in validators.items()
            }
        )

    setattr(provider, "_load_builtin_semantic_validators", load_validators)  # noqa: B010 - Dynamically loaded module exposes this private hook at runtime.


def _install_core_sql(state: _Probe) -> dict[str, bool]:
    """Count real connector calls even outside core performance contexts.

    These five methods call their driver directly, not each other. In particular,
    MariaDB execute_many may make several packet-sized driver calls; one logical
    connector call remains one counter increment. SQL and arguments are never
    inspected, serialized or used as counter labels.
    """
    capabilities = {}
    for backend, class_name in (
        ("sqlite", "SQLiteConnector"),
        ("mariadb", "MariaDBConnector"),
    ):
        module_name = f"h2hdb.{backend}_connector"
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as error:
            # A missing optional backend module is different from a broken
            # present module (for example, its missing driver dependency).
            if backend != "mariadb" or error.name != module_name:
                raise
            capabilities[backend] = False
            continue
        connector = getattr(module, class_name)
        for method in _CORE_SQL_METHODS:
            _hook(
                state, connector, method, f"core.{backend}.sql.{method}", boundary=False
            )
        capabilities[backend] = True
    return capabilities


def _install_core_source(state: _Probe) -> None:
    facade = importlib.import_module("h2hdb").VNextIngestFacade
    for method in _CORE_SOURCE_METHODS:
        _hook(state, facade, method, f"core.source.{method}", boundary=False)


def _expected_source_size(args: tuple[Any, ...], kwargs: dict[str, Any]) -> int:
    member = args[0] if args else kwargs["member"]
    return cast(int, member.expected_size_bytes)


def _expected_digest_size(args: tuple[Any, ...], kwargs: dict[str, Any]) -> int:
    return cast(int, args[1] if len(args) > 1 else kwargs["size"])


def _install_ingest(state: _Probe) -> bool:
    try:
        importlib.metadata.version("h2hdb-ingest")
    except importlib.metadata.PackageNotFoundError:
        return False
    artifact = importlib.import_module("h2hdb_ingest.artifact")
    library = importlib.import_module("h2hdb_ingest.library")
    resident = importlib.import_module("h2hdb_ingest.resident")
    journal = importlib.import_module("h2hdb_ingest._library_journal")
    for method in (
        "initialize",
        "_claim_after_maintenance",
        "_run_library_maintenance",
        "_try_current_only_maintenance",
    ):
        _hook(state, resident.ResidentIngestor, method, f"ingest.resident.{method}")
    for method in (
        "_ensure_layout",
        "maintain_cleanup",
        "activate_page",
        "reconcile_page",
    ):
        _hook(
            state,
            library.ManagedFilesystemLibraryAdapter,
            method,
            f"ingest.library.{method}",
            boundary=method != "_ensure_layout",
        )
    _hook(
        state,
        library.ManagedFilesystemLibraryAdapter,
        "_commit_pending_installs",
        _FAULT_OPERATION,
        boundary=False,
        fault_gate=True,
    )
    # A helper is imported under an alias by library.py; patch both call sites.
    _hook(
        state, journal, "require_exact_schema", "ingest.journal.schema", boundary=False
    )
    setattr(library, "_require_exact_journal_schema", journal.require_exact_schema)  # noqa: B010 - Optional installed module exposes this alias at runtime.
    for name in (
        "_render_archive",
        "_render_presentation",
        "inspect_presentation_archive",
    ):
        _hook(state, artifact, name, f"ingest.artifact.{name}", boundary=False)
    _hook(
        state,
        artifact,
        "_render_page_member",
        "ingest.artifact.page_render",
        boundary=False,
    )
    _hook(
        state,
        artifact,
        "_verify_source_stream",
        "ingest.source.verify_hash",
        boundary=False,
        byte_counter=_expected_source_size,
    )
    _hook(
        state,
        artifact,
        "_stream_digest",
        "ingest.archive.hash",
        boundary=False,
        byte_counter=_expected_digest_size,
    )
    _hook(
        state,
        artifact._ArchiveScratch,
        "read",
        "ingest.archive.python_read",
        boundary=False,
        result_bytes=len,
    )
    return True


def snapshot(operation: str = "manual") -> None:
    """Persist a named observer checkpoint without changing application state."""
    if _LABEL.fullmatch(operation) is None:
        raise ValueError("probe snapshot operation must be a short identifier")
    if _STATE is not None:
        _STATE.emit("snapshot", operation)


def install() -> bool:
    """Install once only when the explicit disposable evidence directory is set."""
    global _STATE
    configured = os.environ.get("H2HDB_ACCEPTANCE_PROBE_DIR")
    control = os.environ.get("H2HDB_ACCEPTANCE_CONTROL_DIR")
    if configured is None:
        if control is not None:
            raise ValueError("acceptance fault control requires the probe")
        return False
    if _STATE is not None:
        return True
    started = time.perf_counter()
    state = _Probe(
        Path(configured),
        os.environ.get("H2HDB_ACCEPTANCE_SCENARIO", "unspecified"),
        None if control is None else Path(control),
    )
    state.emit("start", "probe.install")
    _STATE = state
    try:
        _install_sqlite(state)
        _install_core(state)
        core_sql = _install_core_sql(state)
        _install_core_source(state)
        has_ingest = _install_ingest(state)
        if control is not None and not has_ingest:
            raise RuntimeError(
                "acceptance fault target requires installed ingest hooks"
            )
        versions: dict[str, str | None] = {}
        for name in (
            "h2hdb",
            "h2hdb-ingest",
            "Pillow",
            "pyvips",
            "mysql-connector-python",
        ):
            try:
                versions[name] = importlib.metadata.version(name)
            except importlib.metadata.PackageNotFoundError:
                versions[name] = None
        state.emit(
            "installed",
            "probe.install",
            versions=versions,
            capabilities={
                "core": True,
                "core_source": True,
                "core_sqlite_connector": core_sql["sqlite"],
                "core_mariadb_connector": core_sql["mariadb"],
                "ingest": has_ingest,
                "sqlite": True,
                "python_explicit_fsync": True,
                "fault_injection": control is not None,
            },
            install_seconds=time.perf_counter() - started,
            interval_seconds=_INTERVAL_SECONDS,
            limitations=[
                "Not a production observer; compare against an uninstrumented CLI run.",
                "Python fsync excludes SQLite and native-library internal sync syscalls.",
                "SQLite commit counts explicit method calls; context exits are separate.",
                "SQLite execute counts Connection helpers, not direct Cursor operations.",
                "Core SQL counters are logical connector method calls, not server statements; execute_many can split into multiple driver batches.",
                "Core SQL and source durations are inclusive; connector counters overlap stdlib SQLite helpers and are not additive across layers.",
                "Source and core SQL hooks only update cumulative counters; periodic snapshots do not fsync per call.",
                "Caller-provided custom SQLite factories are unchanged and connect-only.",
                "Logical hash bytes are successful complete passes, not physical disk I/O.",
                "Missing process_exit means incomplete counters and a potentially truncated tail.",
                "Probe eagerly imports hook modules; install_seconds belongs to observer setup.",
                "Nested durations are inclusive and must not be added as disjoint wall time.",
            ],
        )
        state.start_monitor()
        atexit.register(state.close)
        return True
    except BaseException as error:
        state.report_failure(
            RuntimeError(f"probe installation failed: {type(error).__name__}")
        )
        state.close()
        raise
