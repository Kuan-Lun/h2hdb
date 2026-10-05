"""Negative controls for collection pairing and native connection admission."""

from __future__ import annotations

from pathlib import Path

import pytest
from backend_contract import BackendCase, missing_pairs, selected_backend

pytest_plugins = ("pytester",)


def test_pairs_compare_actual_non_backend_cases() -> None:
    cases = (
        BackendCase("test_resume", (("signal", "TERM"),), "sqlite"),
        BackendCase("test_resume", (("signal", "KILL"),), "sqlite"),
        BackendCase("test_resume", (("signal", "TERM"),), "mariadb"),
    )
    assert missing_pairs(cases) == (
        "test_resume (('signal', 'KILL'),): missing mariadb",
    )
    assert (
        missing_pairs(
            (*cases, BackendCase("test_resume", (("signal", "KILL"),), "mariadb"))
        )
        == ()
    )


def test_backend_selection_rejects_crossed_or_invalid_fixture_dimensions() -> None:
    assert selected_backend({"db": "sqlite"}, ("db",)) == "sqlite"
    assert selected_backend({"dialect": "sqlite"}, ("db",)) is None
    for parameters in ({"db": "oracle"}, {"db": "sqlite", "other": "mariadb"}):
        with pytest.raises(ValueError, match="invalid or conflicting"):
            selected_backend(parameters, ("db", "other"))


def _suite(pytester: pytest.Pytester, source: str) -> None:
    plugin = Path(__file__).with_name("backend_contract.py").read_text()
    pytester.makepyfile(backend_contract=plugin, test_contract=source)
    package = pytester.path / "h2hdb"
    package.mkdir()
    (package / "__init__.py").write_text("")
    # These stubs isolate the guard. Product database correctness is covered by
    # the real native fixtures, not this deliberately trivial connector body.
    for module, name in (
        ("sqlite_connector", "SQLiteConnector"),
        ("mariadb_connector", "MariaDBConnector"),
    ):
        (package / f"{module}.py").write_text(
            f"class {name}:\n    def connect(self):\n        pass\n"
        )
    pytester.makeconftest("""
import pytest
pytest_plugins = ("backend_contract",)
@pytest.fixture(params=("sqlite", "mariadb"))
def database(request):
    return request.param
""")
    pytester.makeini("[pytest]\nbackend_contract_fixtures = database\n")


def test_native_connection_without_backend_fixture_is_rejected(
    pytester: pytest.Pytester,
) -> None:
    _suite(
        pytester,
        """
from h2hdb.sqlite_connector import SQLiteConnector
def test_unpaired():
    SQLiteConnector().connect()
""",
    )
    result = pytester.runpytest_subprocess("-q", "--check-backend-pairs")
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*native sqlite connection lacks its paired backend*"])


def test_backend_label_cannot_hide_hardcoded_sqlite(pytester: pytest.Pytester) -> None:
    _suite(
        pytester,
        """
from h2hdb.sqlite_connector import SQLiteConnector
def test_wrong_backend(database):
    SQLiteConnector().connect()
""",
    )
    pytester.runpytest_subprocess("-q", "--check-backend-pairs").assert_outcomes(
        passed=1, failed=1
    )


def test_explicit_native_pair_and_engine_contract_are_accepted(
    pytester: pytest.Pytester,
) -> None:
    _suite(
        pytester,
        """
import pytest
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.mariadb_connector import MariaDBConnector
def test_portable(database):
    (SQLiteConnector if database == "sqlite" else MariaDBConnector)().connect()
@pytest.mark.backend_specific(backend="sqlite", reason="SQLite journal durability PRAGMA is engine-specific")
def test_engine():
    SQLiteConnector().connect()
""",
    )
    pytester.runpytest_subprocess("-q", "--check-backend-pairs").assert_outcomes(
        passed=3
    )


@pytest.mark.parametrize("scope", ("module", "class"))
def test_inherited_engine_exemption_is_rejected_before_execution(
    pytester: pytest.Pytester, scope: str
) -> None:
    declaration = (
        "pytest.mark.backend_specific(backend='sqlite', "
        "reason='SQLite journal durability PRAGMA is engine-specific')"
    )
    body = (
        f"pytestmark = {declaration}\n"
        "def test_new_case():\n"
        "    raise AssertionError('collection must fail first')\n"
        if scope == "module"
        else f"@{declaration}\n"
        "class TestNative:\n"
        "    def test_new_case(self):\n"
        "        raise AssertionError('collection must fail first')\n"
    )
    _suite(pytester, "import pytest\n" + body)
    result = pytester.runpytest_subprocess("-q", "--check-backend-pairs")
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    result.stderr.fnmatch_lines(
        ["*backend_specific must be declared on the individual test or parameter*"]
    )


def test_explicit_parameter_engine_exemption_is_accepted(
    pytester: pytest.Pytester,
) -> None:
    _suite(
        pytester,
        """
import pytest
from h2hdb.sqlite_connector import SQLiteConnector
@pytest.mark.parametrize('journal', [pytest.param('wal', marks=pytest.mark.backend_specific(
    backend='sqlite', reason='SQLite journal durability PRAGMA is engine-specific'))])
def test_journal(journal):
    SQLiteConnector().connect()
""",
    )
    pytester.runpytest_subprocess("-q", "--check-backend-pairs").assert_outcomes(
        passed=1
    )


def test_missing_non_backend_parameter_twin_fails_before_execution(
    pytester: pytest.Pytester,
) -> None:
    _suite(
        pytester,
        """
import pytest
@pytest.mark.parametrize("database,signal", [("sqlite", "TERM"), ("sqlite", "KILL"), ("mariadb", "TERM")])
def test_partial(database, signal):
    raise AssertionError("collection must fail first")
""",
    )
    result = pytester.runpytest_subprocess("-q", "--check-backend-pairs")
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    result.stderr.fnmatch_lines(["*missing mariadb*"])


def test_pairing_ignores_pytest_combination_indices(pytester: pytest.Pytester) -> None:
    _suite(
        pytester,
        """
import pytest
@pytest.mark.parametrize("database,state", [("sqlite", "READY"), ("sqlite", "BUILDING"), ("mariadb", "READY"), ("mariadb", "BUILDING")])
def test_states(database, state):
    pass
""",
    )
    pytester.runpytest_subprocess("-q", "--check-backend-pairs").assert_outcomes(
        passed=4
    )


@pytest.mark.parametrize(
    "execution_mark",
    (
        "pytest.mark.skip(reason='backend is not implemented')",
        "pytest.mark.skipif(True, reason='backend is not implemented')",
        "pytest.mark.xfail(reason='backend is not implemented', strict=True)",
    ),
)
def test_one_sided_execution_policy_cannot_supply_a_missing_backend(
    pytester: pytest.Pytester, execution_mark: str
) -> None:
    _suite(
        pytester,
        f"""
import pytest
@pytest.mark.parametrize("database", ["sqlite", pytest.param("mariadb", marks={execution_mark})])
def test_partial(database):
    raise AssertionError("collection must fail before execution")
""",
    )
    result = pytester.runpytest_subprocess(
        "-q", "--collect-only", "--check-backend-pairs"
    )
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    result.stderr.fnmatch_lines(["*asymmetric backend skip/xfail policy*"])


def test_shared_platform_skip_preserves_pairing_without_claiming_execution(
    pytester: pytest.Pytester,
) -> None:
    _suite(
        pytester,
        """
import pytest
@pytest.mark.skipif(True, reason="Shared unavailable process platform contract")
def test_portable(database):
    raise AssertionError("shared platform exclusion must remain effective")
""",
    )
    pytester.runpytest_subprocess("-q", "--check-backend-pairs").assert_outcomes(
        skipped=2
    )


def test_reference_backend_cannot_replace_selected_native_backend(
    pytester: pytest.Pytester,
) -> None:
    _suite(
        pytester,
        """
import pytest
from h2hdb.sqlite_connector import SQLiteConnector
@pytest.mark.backend_reference(reason="Independent SQLite reference accompanies the selected native case")
def test_wrong_backend(database):
    SQLiteConnector().connect()
""",
    )
    result = pytester.runpytest_subprocess("-q", "--check-backend-pairs")
    result.assert_outcomes(passed=1, failed=1)
    result.stdout.fnmatch_lines(["*never opened its selected native mariadb backend*"])


def test_reference_backend_allows_actual_selected_connection_and_other_oracle(
    pytester: pytest.Pytester,
) -> None:
    _suite(
        pytester,
        """
import pytest
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.mariadb_connector import MariaDBConnector
@pytest.mark.backend_reference(reason="Independent SQLite reference accompanies the selected native case")
def test_both(database):
    SQLiteConnector().connect()
    (SQLiteConnector if database == "sqlite" else MariaDBConnector)().connect()
""",
    )
    pytester.runpytest_subprocess("-q", "--check-backend-pairs").assert_outcomes(
        passed=2
    )


def test_failed_selected_connection_does_not_satisfy_reference_contract(
    pytester: pytest.Pytester,
) -> None:
    _suite(
        pytester,
        """
import pytest
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.mariadb_connector import MariaDBConnector
@pytest.mark.backend_reference(reason="Independent SQLite reference accompanies the selected native case")
def test_failed_connection(database):
    SQLiteConnector().connect()
    try:
        (SQLiteConnector if database == "sqlite" else MariaDBConnector)().connect()
    except RuntimeError:
        pass
""",
    )
    (pytester.path / "h2hdb/mariadb_connector.py").write_text(
        "class MariaDBConnector:\n"
        "    def connect(self):\n"
        "        raise RuntimeError('native connection failed')\n"
    )
    result = pytester.runpytest_subprocess("-q", "--check-backend-pairs")
    result.assert_outcomes(passed=1, failed=1)
    result.stdout.fnmatch_lines(["*never opened its selected native mariadb backend*"])


@pytest.mark.parametrize("order", (("sqlite", "mariadb"), ("mariadb", "sqlite")))
@pytest.mark.parametrize("scope", ("module", "session"))
@pytest.mark.parametrize("cleanup", ("yield", "explicit", "late", "threaded"))
def test_fixture_cleanup_keeps_its_backend_without_relaxing_the_next_case(
    pytester: pytest.Pytester,
    order: tuple[str, str],
    scope: str,
    cleanup: str,
) -> None:
    _suite(
        pytester,
        f"""
import pytest
from concurrent.futures import ThreadPoolExecutor
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.mariadb_connector import MariaDBConnector

CONNECTORS = {{"sqlite": SQLiteConnector, "mariadb": MariaDBConnector}}

@pytest.fixture(scope={scope!r}, params={order!r})
def database(request):
    return request.param

@pytest.fixture(scope={scope!r})
def parent(database, request):
    connector = CONNECTORS[database]()
    request.addfinalizer(connector.connect)
    yield
    connector.connect()

@pytest.fixture(scope={scope!r})
def resource(database, parent, request):
    connector = CONNECTORS[database]()
    connector.connect()
    if {cleanup!r} == "explicit":
        request.addfinalizer(connector.connect)
    yield connector
    if {cleanup!r} == "yield":
        connector.connect()
    elif {cleanup!r} == "late":
        request.addfinalizer(connector.connect)
    elif {cleanup!r} == "threaded":
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(connector.connect).result(timeout=10)

def test_correct(database, resource):
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(resource.connect).result(timeout=10)

def test_wrong_body(database, resource):
    wrong = CONNECTORS["mariadb" if database == "sqlite" else "sqlite"]()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(wrong.connect).result(timeout=10)
""",
    )
    result = pytester.runpytest_subprocess("-q", "--check-backend-pairs")
    result.assert_outcomes(passed=2, failed=2)
    result.stdout.fnmatch_lines(["*native * connection lacks its paired backend*"])


def test_old_fixture_cleanup_cannot_credit_the_next_reference_case(
    pytester: pytest.Pytester,
) -> None:
    _suite(
        pytester,
        """
import pytest
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.mariadb_connector import MariaDBConnector

@pytest.fixture(scope="module", params=("sqlite", "mariadb"))
def database(request):
    return request.param

@pytest.fixture(scope="module")
def resource(database):
    yield
    MariaDBConnector().connect()

@pytest.mark.backend_reference(reason="Independent reference cleanup uses both native engines")
def test_reference(database, resource):
    SQLiteConnector().connect()
""",
    )
    result = pytester.runpytest_subprocess("-q", "--check-backend-pairs")
    result.assert_outcomes(passed=1, failed=1)
    result.stdout.fnmatch_lines(["*never opened its selected native mariadb backend*"])


def test_failed_fixture_cleanup_restores_the_next_case_guard(
    pytester: pytest.Pytester,
) -> None:
    _suite(
        pytester,
        """
import pytest
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.mariadb_connector import MariaDBConnector

@pytest.fixture(scope="module", params=("sqlite", "mariadb"))
def database(request):
    return request.param

@pytest.fixture(scope="module")
def resource(database):
    yield
    (SQLiteConnector if database == "sqlite" else MariaDBConnector)().connect()
    raise RuntimeError("intentional fixture cleanup failure")

def test_first(database, resource):
    pass

def test_second(database, resource):
    (MariaDBConnector if database == "sqlite" else SQLiteConnector)().connect()
""",
    )
    result = pytester.runpytest_subprocess("-q", "--check-backend-pairs")
    result.assert_outcomes(passed=1, failed=2, errors=2)
    result.stdout.fnmatch_lines(
        ["*intentional fixture cleanup failure*", "*native * connection lacks*"]
    )


def test_failed_setup_finalizer_keeps_its_original_backend(
    pytester: pytest.Pytester,
) -> None:
    _suite(
        pytester,
        """
import pytest
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.mariadb_connector import MariaDBConnector

@pytest.fixture(scope="module", params=("sqlite", "mariadb"))
def database(request):
    return request.param

@pytest.fixture(scope="module")
def resource(database, request):
    connector = (SQLiteConnector if database == "sqlite" else MariaDBConnector)()
    request.addfinalizer(connector.connect)
    raise RuntimeError("intentional setup failure")

def test_first(database, resource):
    raise AssertionError("setup must fail first")

def test_second(database):
    (SQLiteConnector if database == "sqlite" else MariaDBConnector)().connect()
""",
    )
    result = pytester.runpytest_subprocess("-q", "--check-backend-pairs")
    result.assert_outcomes(passed=2, errors=2)
    result.stdout.fnmatch_lines(
        [
            "ERROR test_contract.py::test_first[[]sqlite] - RuntimeError: intentional setup*",
            "ERROR test_contract.py::test_first[[]mariadb] - RuntimeError: intentional setup*",
        ]
    )


def test_fixture_cleanup_still_rejects_the_wrong_backend(
    pytester: pytest.Pytester,
) -> None:
    _suite(
        pytester,
        """
import pytest
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.mariadb_connector import MariaDBConnector

@pytest.fixture
def resource(database):
    yield
    (MariaDBConnector if database == "sqlite" else SQLiteConnector)().connect()

def test_body(database, resource):
    (SQLiteConnector if database == "sqlite" else MariaDBConnector)().connect()
""",
    )
    result = pytester.runpytest_subprocess("-q", "--check-backend-pairs")
    result.assert_outcomes(passed=2, errors=2)
    result.stdout.fnmatch_lines(["*native * connection lacks its paired backend*"])
