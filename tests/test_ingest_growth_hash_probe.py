from __future__ import annotations

import importlib.util
import json
import signal
import sqlite3
import sys
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


@pytest.fixture
def probe() -> Iterator[ModuleType]:
    name = "ingest_growth_hash_probe_under_test"
    script = (
        Path(__file__).resolve().parents[1] / "scripts" / "ingest_growth_hash_probe.py"
    )
    spec = importlib.util.spec_from_file_location(name, script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    previous_path = list(sys.path)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
        yield module
    finally:
        sys.path[:] = previous_path
        sys.modules.pop(name, None)


@pytest.mark.parametrize(
    "hashes,history,galleries",
    [(0, 0, 2), (4097, 0, 2), (2, -1, 2), (2, 65, 2), (2, 0, 6)],
)
def test_fixture_refuses_unbounded_shapes(
    probe: ModuleType, hashes: int, history: int, galleries: int
) -> None:
    with pytest.raises(ValueError):
        probe.Shape(hashes, history, galleries)


def test_real_validation_crosses_page_boundary_and_matches_independent_oracle(
    probe: ModuleType,
) -> None:
    with probe.databases("sqlite", 1) as connections:
        connector = next(connections)
        result = probe.measure_case(connector, "sqlite", probe.Shape(65, 1))
        assert result["oracle_matches"]
        assert result["active_hashes"] == 130
        assert result["all_occurrence_rows"] == 260
        assert result["pages"] == [
            {"row_count": 128, "terminal": False},
            {"row_count": 2, "terminal": False},
            {"row_count": 0, "terminal": True},
        ]
        assert result["sql_calls"] > 0
        assert result["sql_seconds"] > 0
        assert Counter(query["kind"] for query in result["queries"]) == {
            "validation_union": 3,
            "occurrences": 2,
            "distinct_artists": 2,
            "maximum_gallery_artists": 2,
        }
        for query in result["queries"]:
            assert query["rows"] <= (
                129 if query["kind"] == "validation_union" else 128
            )
            assert query["sqlite_vm_steps"] >= 0
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connector.connection.execute("SELECT 1")


def test_first_page_membership_stays_fixed_as_fixture_grows(probe: ModuleType) -> None:
    small = probe.Shape(64)
    large = probe.Shape(256)
    assert small.keys == large.keys[:128]
    # Interleaving fixes the gallery owning each key independently of total size.
    assert {int.from_bytes(key, "big") % 2 for key in small.keys} == {0, 1}


@pytest.mark.skipif(
    not all(hasattr(signal, name) for name in ("SIGALRM", "setitimer")),
    reason="manual CLI uses a POSIX cooperative alarm",
)
def test_failure_keeps_partial_report_and_closes_database_owner(
    probe: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "report.json"
    closed = []

    @contextmanager
    def databases(_backend: str, _count: int) -> Iterator[Iterator[object]]:
        assert json.loads(output.read_text())["status"] == "incomplete"
        try:
            yield iter((object(),))
        finally:
            closed.append(True)

    def fail(*_args: Any, **_kwargs: Any) -> None:
        raise ValueError("synthetic case failure")

    monkeypatch.setattr(probe, "databases", databases)
    monkeypatch.setattr(probe, "measure_case", fail)
    monkeypatch.setattr(
        sys, "argv", ["hash-probe", "--hashes", "2", "--output", str(output)]
    )
    with pytest.raises(ValueError, match="synthetic case failure"):
        probe.main()
    report = json.loads(output.read_text())
    assert report["status"] == "incomplete"
    assert report["error"]["type"] == "ValueError"
    assert closed == [True]
    assert not list(tmp_path.glob(".hash-report-*"))


def test_failed_container_start_still_stops_and_preserves_start_exception(
    probe: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    import testcontainers.community.mysql as mysql_containers

    stopped = []

    class Container:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def start(self) -> None:
            raise ValueError("synthetic start failure")

        def stop(self) -> None:
            stopped.append(True)
            raise RuntimeError("synthetic stop failure")

    monkeypatch.setattr(mysql_containers, "MySqlContainer", Container)
    with pytest.raises(ValueError, match="synthetic start failure") as error:
        with probe.databases("mariadb", 1):
            pytest.fail("failed start yielded a connection")
    assert stopped == [True]
    assert "synthetic stop failure" in error.value.__notes__[0]


def test_atomic_report_failure_keeps_previous_valid_evidence(
    probe: ModuleType, tmp_path: Path
) -> None:
    output = tmp_path / "report.json"
    probe.write_report(output, {"status": "incomplete", "cases": [1]})
    with pytest.raises(TypeError, match="unsupported JSON value"):
        probe.write_report(output, {"invalid": object()})
    assert json.loads(output.read_text()) == {"status": "incomplete", "cases": [1]}
    assert not list(tmp_path.glob(".hash-report-*"))


def test_missing_query_classification_cannot_report_success(
    probe: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(probe, "query_kind", lambda _sql: None)
    with probe.databases("sqlite", 1) as connections:
        with pytest.raises(RuntimeError, match="incomplete query measurement"):
            probe.measure_case(next(connections), "sqlite", probe.Shape(2))
