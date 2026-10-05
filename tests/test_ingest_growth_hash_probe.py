from __future__ import annotations

import importlib.util
import json
import signal
import sqlite3
import sys
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import Mock

import pytest
from vnext_probe_databases import generated_probe_databases
from vnext_test_database import DatabaseFactory


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
    database_factory: DatabaseFactory,
) -> None:
    with generated_probe_databases(database_factory, 1) as connections:
        connector = next(connections)
        result = probe.measure_case(
            connector, database_factory.backend, probe.Shape(65, 1)
        )
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
            "validation_actual_union": 6,
            "shadow_family_points": 2,
            "tombstone_points": 2,
            "source_memberships": 1,
            "source_artists": 2,
            "source_occurrences": 2,
        }
        for query in result["queries"]:
            assert query["rows"] <= (
                129 if query["kind"] == "validation_actual_union" else 128
            )
            if database_factory.backend == "sqlite":
                assert query["sqlite_vm_steps"] >= 0
            else:
                assert query["sqlite_vm_steps"] is None
                assert query["plan"]["handler_read_delta"]
        assert result["point_work_bound"] == (
            "passed"
            if database_factory.backend == "mariadb"
            else "not measured on SQLite"
        )
    with pytest.raises((sqlite3.ProgrammingError, RuntimeError), match="closed"):
        connector.fetch_one("SELECT 1")


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
    probe: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    database_factory: DatabaseFactory,
) -> None:
    monkeypatch.setattr(probe, "query_kind", lambda _sql: None)
    with generated_probe_databases(database_factory, 1) as connections:
        with pytest.raises(RuntimeError, match="incomplete query measurement"):
            probe.measure_case(
                next(connections), database_factory.backend, probe.Shape(2)
            )


def test_validation_source_work_scales_with_current_facts_not_retained_history(
    probe: ModuleType,
    database_factory: DatabaseFactory,
) -> None:
    shapes = [probe.Shape(64), probe.Shape(256), probe.Shape(256, 8, 2, True)]
    with generated_probe_databases(database_factory, len(shapes)) as connections:
        cases = [
            probe.measure_case(connector, database_factory.backend, shape)
            for connector, shape in zip(connections, shapes, strict=True)
        ]
    assert [case["source_rows_read"] for case in cases] == [128, 512, 512]
    assert all(case["validation_source_aggregates"] == 0 for case in cases)
    if database_factory.backend == "sqlite":
        steps = [
            sum(
                query["sqlite_vm_steps"]
                for query in case["queries"]
                if query["kind"] == "source_occurrences"
            )
            for case in cases
        ]
        # Four times the selected facts may perform four times the source VM work,
        # plus one 100-opcode sampling interval for each of six bounded reads.
        # Unselected distinct history (with a long hash gap) adds no source reads.
        assert steps[1] <= 4 * steps[0] + 600
        assert abs(steps[2] - steps[1]) <= 600
    else:
        for case in cases:
            for values in case["queries"]:
                if values["kind"] == "source_occurrences":
                    probe.require_source_work_bound(probe.Query(**values), "mariadb")


def _cost_query(
    probe: ModuleType, kind: str, *, analyses: int = 1, hashes: int = 128
) -> Any:
    rows = 128 if kind == "source_occurrences" else analyses * hashes
    parameters = (
        ()
        if kind == "source_occurrences"
        else (
            *(value.to_bytes(16, "big") for value in range(analyses)),
            *(value.to_bytes(32, "big") for value in range(hashes)),
            rows + 1,
        )
    )
    query = probe.Query(kind, "", parameters, rows, 0, None)
    query.plan = {
        "handler_read_delta": dict.fromkeys(probe.HANDLER_READ_COUNTERS, 0),
        "analyze": [
            {
                "table": table,
                "type": "eq_ref",
                "key": "PRIMARY",
                "key_len": "48",
                "rows": 1,
                "r_rows": 1.0,
            }
            for table in probe._POINT_TABLES.get(kind, ())
        ],
    }
    return query


def _require_cost(probe: ModuleType, query: Any) -> None:
    if query.kind == "source_occurrences":
        probe.require_source_work_bound(query, "mariadb")
    else:
        probe.require_point_work_bound(query)


@pytest.mark.parametrize(
    "kind,families",
    [("source_occurrences", 0), ("shadow_family_points", 5), ("tombstone_points", 1)],
)
@pytest.mark.parametrize(
    "counter",
    ["first", "key", "last", "next", "prev", "rnd", "rnd_deleted", "rnd_next", "retry"],
)
def test_complete_handler_budget_rejects_every_excess_read(
    probe: ModuleType, kind: str, families: int, counter: str
) -> None:
    query = _cost_query(probe, kind)
    maximum = (
        {"key": 1, "next": 128}
        if families == 0
        else {"key": families * 128, "rnd": 128, "rnd_next": 514}
    )
    values = query.plan["handler_read_delta"]
    values.update({"Handler_read_" + name: value for name, value in maximum.items()})
    _require_cost(probe, query)
    values["Handler_read_" + counter] = maximum.get(counter, 0) + 1
    with pytest.raises(RuntimeError, match="work exceeded"):
        _require_cost(probe, query)


@pytest.mark.parametrize(
    "kind", ["source_occurrences", "shadow_family_points", "tombstone_points"]
)
@pytest.mark.parametrize(
    "damage",
    ["missing", "unknown", "negative", "float", "nan", "bool", "string", "no-evidence"],
)
def test_cost_oracles_reject_incomplete_or_invalid_counters(
    probe: ModuleType, kind: str, damage: str
) -> None:
    query = _cost_query(probe, kind)
    values = query.plan["handler_read_delta"]
    if damage == "missing":
        for name in tuple(values):
            removed = values.pop(name)
            with pytest.raises(RuntimeError, match="Handler"):
                _require_cost(probe, query)
            values[name] = removed
        return
    if damage == "unknown":
        values["Handler_read_unknown"] = 0
    elif damage == "no-evidence":
        query.plan.pop("handler_read_delta")
    else:
        values["Handler_read_key"] = {
            "negative": -1,
            "float": 1.0,
            "nan": float("nan"),
            "bool": True,
            "string": "1",
        }[damage]
    with pytest.raises(RuntimeError, match="Handler"):
        _require_cost(probe, query)


@pytest.mark.parametrize("kind", ["shadow_family_points", "tombstone_points"])
@pytest.mark.parametrize(
    "analyses,hashes",
    [(1, 1), (1, 127), (1, 128), (17, 1), (17, 128), (0, 1), (18, 1), (1, 0), (1, 129)],
)
def test_point_grid_boundary_has_fixed_input_derived_budget(
    probe: ModuleType, kind: str, analyses: int, hashes: int
) -> None:
    query = _cost_query(probe, kind, analyses=analyses, hashes=hashes)
    if not 1 <= analyses <= 17 or not 1 <= hashes <= 128:
        with pytest.raises(RuntimeError, match="requested grid"):
            probe.require_point_work_bound(query)
        return
    query.plan["handler_read_delta"].update(
        {
            "Handler_read_key": (5 if kind == "shadow_family_points" else 1)
            * analyses
            * hashes,
            "Handler_read_rnd": analyses * hashes,
            "Handler_read_rnd_next": 2 * analyses * hashes
            + 2 * max(analyses, hashes)
            + 2,
        }
    )
    probe.require_point_work_bound(query)


@pytest.mark.parametrize("kind", ["shadow_family_points", "tombstone_points"])
@pytest.mark.parametrize(
    "damage",
    [
        "missing-table",
        "extra-table",
        "duplicate-table",
        "scan",
        "partial-key",
        "wrong-key",
        "nan",
        "bool",
        "missing-native-rows",
        "empty",
        "unvisited",
    ],
)
def test_point_plan_cannot_hide_family_scan_in_temporary_grid_allowance(
    probe: ModuleType, kind: str, damage: str
) -> None:
    query = _cost_query(probe, kind)
    rows = query.plan["analyze"]
    if damage == "missing-table":
        rows.pop()
    elif damage == "extra-table":
        rows.append(
            {"table": "unrelated_base", "type": "ALL", "rows": 1, "r_rows": 1.0}
        )
    elif damage == "duplicate-table":
        rows.append(dict(rows[0]))
    elif damage == "missing-native-rows":
        rows[0].pop("r_rows")
    elif damage == "empty":
        rows[0].update(type="ALL", key=None, key_len=None, rows=0, r_rows=0.0)
    elif damage == "unvisited":
        rows[0]["r_rows"] = None
    else:
        rows[0].update(
            {
                "scan": {"type": "ALL"},
                "partial-key": {"key_len": "16"},
                "wrong-key": {"key": "other"},
                "nan": {"r_rows": float("nan")},
                "bool": {"r_rows": True},
            }[damage]
        )
    if damage in {"empty", "unvisited"}:
        probe.require_point_work_bound(query)
    else:
        # All old key/next/prev counters are zero, so the previous oracle passed.
        assert not any(query.plan["handler_read_delta"].values())
        with pytest.raises(RuntimeError, match="point work"):
            probe.require_point_work_bound(query)


@pytest.mark.parametrize(
    "estimated,actual,accepted",
    [
        ("1", "1.00", True),
        ("1", "0.25", True),
        ("0", "0.00", True),
        (None, None, True),
        ("1", "1.01", False),
        ("1.0", "1.00", False),
        (True, "1.00", False),
        ("1", True, False),
        ("1", "NaN", False),
        ("1", "Infinity", False),
        ("1", float("inf"), False),
        ("1", "-1", False),
        ("1", "+1", False),
        ("1", "1e0", False),
        ("1", " 1", False),
        ("1", "１", False),
        ("-1", "1.00", False),
    ],
)
def test_point_native_numeric_rows_are_exact_and_fail_closed(
    probe: ModuleType, estimated: Any, actual: Any, *, accepted: bool
) -> None:
    query = _cost_query(probe, "shadow_family_points")
    query.plan["analyze"][0].update(rows=estimated, r_rows=actual)
    if accepted:
        probe.require_point_work_bound(query)
    else:
        with pytest.raises(RuntimeError, match="point work"):
            probe.require_point_work_bound(query)


@pytest.mark.parametrize(
    "damage", ["decreased", "missing", "unknown", "duplicate", "nan", "bool"]
)
def test_profile_rejects_unusable_native_counter_snapshots(
    probe: ModuleType, damage: str
) -> None:
    before = dict.fromkeys(probe.HANDLER_READ_COUNTERS, 1)
    after: dict[str, Any] = dict(before)
    if damage == "decreased":
        after["Handler_read_key"] = 0
    elif damage == "missing":
        after.pop("Handler_read_rnd_next")
    elif damage == "unknown":
        after["Handler_read_unknown"] = 0
    elif damage == "nan":
        after["Handler_read_key"] = float("nan")
    elif damage == "bool":
        after["Handler_read_key"] = True
    raw_after = list(after.items())
    if damage == "duplicate":
        raw_after.append(raw_after[0])
    connector = Mock()
    connector.fetch_all.side_effect = [list(before.items()), raw_after]
    connector.connection.cursor.return_value.__enter__ = Mock(return_value=Mock())
    connector.connection.cursor.return_value.__exit__ = Mock(return_value=False)
    with pytest.raises(RuntimeError, match="[Hh]andler|counter"):
        probe.profile_mariadb(connector, _cost_query(probe, "source_occurrences"))


def test_same_source_rows_reject_a_real_full_scan(
    probe: ModuleType,
    database_factory: DatabaseFactory,
    record_property: Callable[[str, object], None],
) -> None:
    shape = probe.Shape(64, 8, 2, True)
    table = "catalog_gallery_observation_file_hash_occurrences"
    suffix = (
        "WHERE gallery_id = %s AND observation_id = %s ORDER BY file_sha256 LIMIT %s"
    )
    prefix = f"SELECT file_sha256, occurrence_count FROM {table} "
    scan_hint = (
        "NOT INDEXED "
        if database_factory.backend == "sqlite"
        else "IGNORE INDEX (PRIMARY, ix_observation_hash_occurrence_group) "
    )
    queries = []
    expected = [(key, 1) for key in shape.keys[::2]]
    with generated_probe_databases(database_factory, 1) as connections:
        connector = next(connections)
        probe.seed(connector, database_factory.backend, shape)
        for hint in ("", scan_hint):
            with probe.record(connector) as observed:
                rows = connector.fetch_all(prefix + hint + suffix, (1, 1, 128))
            assert rows == expected
            assert len(observed.queries) == 1
            query = observed.queries[0]
            if database_factory.backend == "mariadb":
                query.plan = probe.profile_mariadb(connector, query)
            else:
                query.plan = connector.fetch_all(
                    "EXPLAIN QUERY PLAN " + query.sql, query.parameters
                )
            queries.append(query)
        indexed, scanned = queries
        probe.require_source_work_bound(indexed, database_factory.backend)
        if database_factory.backend == "mariadb":
            counts = scanned.plan["handler_read_delta"]
            # The previous three-counter contract incorrectly accepts this scan.
            assert counts["Handler_read_key"] <= 1
            assert counts["Handler_read_next"] <= len(expected)
            assert counts["Handler_read_prev"] == 0
            assert counts["Handler_read_rnd_next"] >= 1152
            assert any(
                row.get("table") == table and row.get("type") == "ALL"
                for row in scanned.plan["analyze"]
            )
        else:
            assert any(str(row[3]).startswith(f"SCAN {table}") for row in scanned.plan)
            assert scanned.sqlite_vm_steps > indexed.sqlite_vm_steps
        with pytest.raises(RuntimeError, match="source work exceeded"):
            probe.require_source_work_bound(scanned, database_factory.backend)
    record_property(
        "full_scan_control",
        json.dumps(
            {
                "backend": database_factory.backend,
                "selected_rows": len(expected),
                "physical_rows": 1152,
                "exact_rows_equal": True,
                "indexed_vm_steps": indexed.sqlite_vm_steps,
                "scan_vm_steps": scanned.sqlite_vm_steps,
                "indexed_plan": indexed.plan,
                "scan_plan": scanned.plan,
            },
            default=str,
        ),
    )
