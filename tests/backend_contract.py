"""Fail closed on unclassified native SQL tests and missing backend variants.

This dev-only pytest plugin is self-contained so each consumer can be tested
from a clean checkout. Collection proves case pairing, not execution or SQL
correctness. Native connection interception detects ordinary in-process tests
that bypass the registered backend fixtures; subprocesses need explicit review.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Generator, Iterable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import pytest

BACKENDS = frozenset({"sqlite", "mariadb"})
_CONNECTED_BACKENDS = pytest.StashKey[set[str]]()


@dataclass(frozen=True)
class _ConnectionScope:
    nodeid: str
    allowed: frozenset[str]
    connected: set[str]


@dataclass
class _ScopeState:
    current: _ConnectionScope | None = None


_SCOPE = _ScopeState()


@contextmanager
def _connection_scope(owner: _ConnectionScope) -> Generator[None]:
    # Like the connector monkeypatch, admission is process-wide. Pytest runs
    # cases serially within each worker process; a fixture's joined worker
    # threads must see the same cleanup authority as its main thread.
    previous, _SCOPE.current = _SCOPE.current, owner
    try:
        yield
    finally:
        _SCOPE.current = previous


@dataclass(frozen=True)
class BackendCase:
    family: str
    parameters: tuple[tuple[str, str], ...]
    backend: str
    execution_policy: tuple[str, ...] = ()


def missing_pairs(cases: Iterable[BackendCase]) -> tuple[str, ...]:
    groups: dict[tuple[str, tuple[tuple[str, str], ...]], set[str]] = defaultdict(set)
    policies: dict[tuple[str, tuple[tuple[str, str], ...]], set[tuple[str, ...]]] = (
        defaultdict(set)
    )
    for case in cases:
        groups[case.family, case.parameters].add(case.backend)
        policies[case.family, case.parameters].add(case.execution_policy)
    problems = []
    for (family, parameters), backends in sorted(groups.items()):
        if backends != BACKENDS:
            problems.append(
                f"{family} {parameters}: missing {', '.join(sorted(BACKENDS - backends))}"
            )
        if len(policies[family, parameters]) != 1:
            problems.append(
                f"{family} {parameters}: asymmetric backend skip/xfail policy"
            )
    return tuple(problems)


def selected_backend(parameters: Mapping[str, Any], names: Iterable[str]) -> str | None:
    selected = {parameters[name] for name in names if name in parameters}
    if not selected:
        return None
    if len(selected) != 1 or not selected <= BACKENDS:
        raise ValueError(
            f"invalid or conflicting native backend parameters: {selected}"
        )
    return str(next(iter(selected)))


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addini(
        "backend_contract_fixtures",
        "Native backend parameter fixture names",
        type="args",
    )
    parser.addoption(
        "--check-backend-pairs",
        action="store_true",
        help="Require both backend variants before selection; use whole-suite collection",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "backend_specific(backend, reason): native engine contract with no portable twin",
    )
    config.addinivalue_line(
        "markers",
        "backend_reference(reason): portable case additionally opens another backend oracle",
    )
    config.addinivalue_line(
        "markers",
        "backend_external(reason): SQL execution occurs in an explicitly configured child",
    )


def _marker(item: pytest.Item, name: str) -> dict[str, Any] | None:
    markers = list(item.iter_markers_with_node(name))
    if not markers:
        return None
    if len(markers) != 1:
        raise pytest.UsageError(f"{item.nodeid}: duplicate {name} contracts")
    owner, marker = markers[0]
    if name == "backend_specific" and owner is not item:
        raise pytest.UsageError(
            f"{item.nodeid}: backend_specific must be declared on the individual "
            "test or parameter, not inherited from a module or class"
        )
    if marker.args or not isinstance(marker.kwargs.get("reason"), str):
        raise pytest.UsageError(f"{item.nodeid}: {name} requires keyword reason")
    if len(marker.kwargs["reason"].strip()) < 20:
        raise pytest.UsageError(f"{item.nodeid}: {name} needs a concrete reason")
    expected_keys = {"reason", "backend"} if name == "backend_specific" else {"reason"}
    if set(marker.kwargs) != expected_keys:
        raise pytest.UsageError(f"{item.nodeid}: invalid {name} fields")
    if name == "backend_specific" and marker.kwargs["backend"] not in BACKENDS:
        raise pytest.UsageError(f"{item.nodeid}: invalid engine-specific backend")
    return dict(marker.kwargs)


def _selection(item: pytest.Item) -> tuple[str | None, Any]:
    callspec = getattr(item, "callspec", None)
    parameters = {} if callspec is None else callspec.params
    names = item.config.getini("backend_contract_fixtures")
    try:
        return selected_backend(parameters, names), callspec
    except ValueError as error:
        raise pytest.UsageError(f"{item.nodeid}: {error}") from error


def _execution_policy(item: pytest.Item) -> tuple[str, ...]:
    # Shared platform/opt-in marks remain valid; a one-sided mark cannot make
    # an unimplemented backend look like an equivalent executable variant.
    return tuple(
        sorted(
            repr((marker.name, marker.args, sorted(marker.kwargs.items())))
            for marker in item.iter_markers()
            if marker.name in {"skip", "skipif", "xfail"}
        )
    )


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    cases: list[BackendCase] = []
    names = set(config.getini("backend_contract_fixtures"))
    for item in items:
        backend, callspec = _selection(item)
        specific = _marker(item, "backend_specific")
        reference = _marker(item, "backend_reference")
        external = _marker(item, "backend_external")
        if specific and backend:
            raise pytest.UsageError(
                f"{item.nodeid}: portable and engine-specific conflict"
            )
        if (reference or external) and backend is None:
            raise pytest.UsageError(
                f"{item.nodeid}: reference/child requires paired fixture"
            )
        if backend is not None:
            family = item.nodeid.split("[", 1)[0]
            parameters = tuple(
                sorted(
                    (k, repr(v)) for k, v in callspec.params.items() if k not in names
                )
            )
            cases.append(
                BackendCase(family, parameters, backend, _execution_policy(item))
            )
    if config.getoption("--check-backend-pairs"):
        problems = missing_pairs(cases)
        if problems:
            raise pytest.UsageError(
                "Incomplete database backend matrix:\n" + "\n".join(problems)
            )


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_protocol(
    item: pytest.Item, nextitem: pytest.Item | None
) -> Generator[None, bool, bool]:
    del nextitem
    from h2hdb.mariadb_connector import MariaDBConnector
    from h2hdb.sqlite_connector import SQLiteConnector

    backend, _ = _selection(item)
    specific = _marker(item, "backend_specific")
    reference = _marker(item, "backend_reference")
    allowed = {backend} if backend else set()
    if specific:
        allowed.add(specific["backend"])
    if reference:
        allowed.update(BACKENDS)
    connected: set[str] = set()
    item.stash[_CONNECTED_BACKENDS] = connected
    with (
        _connection_scope(_ConnectionScope(item.nodeid, frozenset(allowed), connected)),
        pytest.MonkeyPatch.context() as patcher,
    ):
        for name, connector_type in (
            ("sqlite", SQLiteConnector),
            ("mariadb", MariaDBConnector),
        ):
            original = connector_type.connect

            def guarded(
                self: Any, *, _name: str = name, _original: Any = original
            ) -> None:
                scope = _SCOPE.current
                if scope is None or _name not in scope.allowed:
                    owner = item.nodeid if scope is None else scope.nodeid
                    pytest.fail(
                        f"{owner}: native {_name} connection lacks its paired backend "
                        "fixture or an explicit engine-specific contract",
                        pytrace=False,
                    )
                _original(self)
                scope.connected.add(_name)

            patcher.setattr(connector_type, "connect", guarded)
        return (yield)


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_fixture_setup(
    fixturedef: pytest.FixtureDef[Any], request: pytest.FixtureRequest
) -> Generator[None, Any, Any]:
    del fixturedef
    owner = _SCOPE.current
    if owner is not None:
        register = request.addfinalizer

        def add_owned_finalizer(finalizer: Callable[[], object]) -> None:
            def finish() -> None:
                with _connection_scope(owner):
                    finalizer()

            register(finish)

        # A SubRequest belongs to one fixture instance, including its parameter.
        # Keep registration wrapped for its lifetime: yield cleanup may itself
        # register another finalizer. Module/session fixtures can finish while a
        # different backend's case is being set up; that case grants no authority
        # to the old fixture and receives no connection credit from its cleanup.
        # Each request owns its registration callback for this fixture instance.
        request.addfinalizer = add_owned_finalizer  # type: ignore[method-assign]
    return (yield)


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    del call
    report = yield
    if (
        report.when == "call"
        and report.passed
        and _marker(item, "backend_reference") is not None
    ):
        backend, _ = _selection(item)
        if backend not in item.stash.get(_CONNECTED_BACKENDS, set()):
            # Do not raise outside runtest's reporting boundary: retain an
            # ordinary failed test and its teardown instead of INTERNALERROR.
            report.outcome = "failed"
            report.longrepr = (
                f"{item.nodeid}: reference case never opened its selected native "
                f"{backend} backend successfully"
            )
    return report
