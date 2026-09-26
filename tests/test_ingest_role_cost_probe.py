"""Validate the diagnostic oracle without starting a live database service."""

from __future__ import annotations

import importlib.util
import json
import re
import signal
import sqlite3
import sys
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from itertools import product
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest


@pytest.fixture
def probe() -> Iterator[ModuleType]:
    name = "ingest_role_cost_probe_under_test"
    path = Path(__file__).resolve().parents[1] / "scripts/ingest_role_cost_probe.py"
    spec = importlib.util.spec_from_file_location(name, path)
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


@pytest.mark.parametrize("files", [0, 2_097_153, True, -1])
def test_fixture_rejects_unbounded_or_non_integer_sizes(
    probe: ModuleType, files: int
) -> None:
    with pytest.raises(ValueError, match="files must"):
        probe.Shape(files)


def test_regimes_keep_independent_metadata_and_multiplicity_cardinalities(
    probe: ModuleType,
) -> None:
    distinct = probe.file_facts(probe.Shape(32768))
    duplicate = probe.file_facts(probe.Shape(32768, "duplicate"))
    metadata = probe.file_facts(probe.Shape(32768, "metadata"))
    assert distinct[:129] == probe.file_facts(probe.Shape(129))
    assert len(probe.content_occurrences(distinct)) == 32768
    multiplicities = Counter(probe.content_occurrences(duplicate).values())
    assert multiplicities[257] == 32
    assert sum(multiplicities.values()) == 32768 - 32 * 256
    assert probe.stream_sizes(metadata)["anchors"] == 32768
    assert probe.stream_sizes(metadata)["derived_hash_occurrences"] == 32768
    assert probe.seek_budget("anchors", metadata) == 1064
    assert probe.seek_budget("derived_hash_occurrences", metadata) == 1064


@pytest.mark.parametrize(
    "changes",
    [
        {"galleries": 0},
        {"galleries": True},
        {"galleries": 131073},
        {"observations": 0},
        {"observations": 65},
        {"observations": True},
        {"shared_names": 1},
    ],
)
def test_shape_dimensions_are_independently_bounded(
    probe: ModuleType, changes: dict[str, Any]
) -> None:
    with pytest.raises(ValueError):
        probe.Shape(129, **changes)
    assert probe.Shape(2_097_152, galleries=8192, observations=2, shared_names=True)


def test_shared_names_isolate_join_fanout_without_changing_hash_cardinality(
    probe: ModuleType,
) -> None:
    unique = probe.file_facts(probe.Shape(32768, galleries=128))
    shared = probe.file_facts(probe.Shape(32768, galleries=128, shared_names=True))
    assert len({fact.key for fact in unique}) == 32768
    assert len({fact.key for fact in shared}) == 128
    assert set(Counter(fact.key for fact in shared).values()) == {256}
    assert [fact.digest for fact in unique] == [fact.digest for fact in shared]
    assert probe.stream_sizes(unique) == probe.stream_sizes(shared)
    assert probe.seek_budget("derived_hash_occurrences", unique) == 1064
    assert probe.seek_budget("derived_hash_occurrences", shared) == 1064


@pytest.mark.parametrize("regime", ("distinct", "duplicate", "metadata"))
def test_changed_gallery_history_and_shared_names_seed_exact_real_authorities(
    probe: ModuleType, regime: str
) -> None:
    facts = probe.file_facts(
        probe.Shape(257, regime, galleries=17, observations=3, shared_names=True)
    )
    expected = probe.expected_stream_rows(facts)
    streams = probe.fixture_streams(facts)
    assert {kind: list(rows) for kind, rows in streams.items()} == expected
    with probe.databases("sqlite", 1) as connections:
        connector = next(connections)
        probe.seed_fixture(connector, facts)
        assert connector.fetch_all(
            "SELECT gallery_id, observation_id "
            "FROM catalog_gallery_observation_allocations "
            "ORDER BY gallery_id, observation_id"
        ) == sorted({(fact.gallery, fact.observation) for fact in facts})
        for _ in range(3):
            captured, measured = probe.capture_validator(connector, facts)
            assert measured["fixed_select_calls"]["metadata_file_identity"] == 1
            assert sum(
                query.returned_rows for query in captured["derived_hash_occurrences"]
            ) == len(facts)
            ordered_content = [
                row
                for row in expected["derived_hash_occurrences"]
                if row[3] != probe.identity.file_key(b"galleryinfo.txt")
            ]
            for query in captured["derived_hash_occurrences"]:
                sql, parameters = probe.joined_content_baseline(
                    query.sql, query.parameters
                )
                cursor = query.parameters[-5:-1]
                assert (
                    connector.fetch_all(sql, parameters)
                    == [row for row in ordered_content if row > cursor][:128]
                )


@pytest.mark.parametrize("lookup_calls", (0, 2))
def test_fixed_cost_oracle_rejects_missing_or_repeated_metadata_lookup(
    probe: ModuleType, monkeypatch: pytest.MonkeyPatch, lookup_calls: int
) -> None:
    facts = probe.file_facts(probe.Shape(129))
    original = probe.role.check_role_derivation_v1

    def degraded(connector: Any) -> None:
        lookup = connector.fetch_one

        def repeat(sql: str, parameters: tuple[Any, ...] = ()) -> tuple[Any, ...]:
            if probe.fixed_query_kind(sql) != "metadata_file_identity":
                return cast(tuple[Any, ...], lookup(sql, parameters))
            result: tuple[Any, ...] = ()
            for _ in range(lookup_calls):
                result = lookup(sql, parameters)
            return result

        with monkeypatch.context() as scoped:
            scoped.setattr(connector, "fetch_one", repeat)
            original(connector)

    monkeypatch.setattr(probe.role, "check_role_derivation_v1", degraded)
    with probe.databases("sqlite", 1) as connections:
        connector = next(connections)
        probe.seed_fixture(connector, facts)
        with pytest.raises(
            RuntimeError,
            match="fixed role query count mismatch: metadata_file_identity",
        ):
            probe.capture_validator(connector, facts)


@pytest.mark.parametrize("files, calls", [(127, 14), (128, 14), (129, 21)])
def test_actual_role_validator_has_seven_streams_and_terminal_empty_page(
    probe: ModuleType, files: int, calls: int
) -> None:
    facts = probe.file_facts(probe.Shape(files))
    with probe.databases("sqlite", 1) as connections:
        connector = next(connections)
        probe.seed_fixture(connector, facts)
        captured, measured = probe.capture_validator(connector, facts)
    assert measured["stream_select_calls"] == calls
    assert measured["select_calls"] == calls + 9
    assert measured["fixed_select_calls_total"] == 9
    assert measured["fixed_select_calls"] == {
        **dict.fromkeys(probe.REGISTRY_QUERIES, 1),
        "observation_cleanup_authority": 1,
        "metadata_file_identity": 1,
    }
    assert set(captured) == set(probe.STREAMS)
    for queries in captured.values():
        assert queries[-1].returned_rows == 0
        assert sum(query.returned_rows for query in queries) == files
        positions = probe.sample_positions(queries)
        assert positions["last_empty"] == len(queries) - 1
        if files <= 128:
            assert (
                positions["first"]
                == positions["middle"]
                == positions["last_nonempty"]
                == 0
            )


@pytest.mark.parametrize("regime,files", [("duplicate", 8224), ("metadata", 129)])
def test_real_validator_accepts_duplicate_groups_and_metadata_filter(
    probe: ModuleType, regime: str, files: int
) -> None:
    facts = probe.file_facts(probe.Shape(files, regime))
    expected = probe.stream_sizes(facts)
    with probe.databases("sqlite", 1) as connections:
        connector = next(connections)
        probe.seed_fixture(connector, facts)
        captured, measured = probe.capture_validator(connector, facts)
    assert measured["stream_select_calls"] == sum(
        (count + 127) // 128 + 1 for count in expected.values()
    )
    assert {
        kind: sum(query.returned_rows for query in queries)
        for kind, queries in captured.items()
    } == expected
    if regime == "duplicate":
        assert expected["derived_hash_occurrences"] == 8224
        assert expected["stored_hash_occurrences"] == 32
    else:
        assert expected["derived_hash_occurrences"] == 129


@pytest.mark.parametrize("files", (127, 128, 129))
@pytest.mark.parametrize("regime", ("distinct", "duplicate", "metadata"))
def test_production_boundary_pages_match_fixture_facts_in_repeated_cycles(
    probe: ModuleType, files: int, regime: str
) -> None:
    facts = probe.file_facts(probe.Shape(files, regime))
    expected = probe.stream_sizes(facts)
    with probe.databases("sqlite", 1) as connections:
        connector = next(connections)
        probe.seed_fixture(connector, facts)
        for _ in range(3):
            captured, measured = probe.capture_validator(connector, facts)
            assert measured["stream_select_calls"] == sum(
                (size + 127) // 128 + 1 for size in expected.values()
            )
            assert measured["fixed_select_calls_total"] == 9
            assert measured["fixed_select_calls"]["observation_cleanup_authority"] == 1
            assert measured["select_calls"] == measured["stream_select_calls"] + 9
            for queries in captured.values():
                assert queries[-1].returned_rows == 0
                for query in queries:
                    # This also rejects accidental removal of either optimizer
                    # predicate; the baseline recognizer checks both bindings.
                    probe.tuple_seek_baseline(query.sql, query.parameters)


@pytest.mark.parametrize("authority_calls", (0, 2))
def test_fixed_cost_oracle_rejects_missing_or_repeated_authority_probe(
    probe: ModuleType, monkeypatch: pytest.MonkeyPatch, authority_calls: int
) -> None:
    facts = probe.file_facts(probe.Shape(129))
    with probe.databases("sqlite", 1) as connections:
        connector = next(connections)
        probe.seed_fixture(connector, facts)
        original = probe.role._validated_open_observation_retirement

        def degraded(connection: Any) -> None:
            for _ in range(authority_calls):
                assert original(connection) is None

        monkeypatch.setattr(
            probe.role, "_validated_open_observation_retirement", degraded
        )
        with pytest.raises(
            RuntimeError,
            match="fixed role query count mismatch: observation_cleanup_authority",
        ):
            probe.capture_validator(connector, facts)


def test_equal_count_wrong_rows_fail_the_independent_fixture_oracle(
    probe: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    facts = probe.file_facts(probe.Shape(129))
    with probe.databases("sqlite", 1) as connections:
        connector = next(connections)
        probe.seed_fixture(connector, facts)
        original = connector.fetch_all

        def reordered(
            sql: str, parameters: tuple[Any, ...] = ()
        ) -> list[tuple[Any, ...]]:
            rows = original(sql, parameters)
            return list(reversed(rows)) if probe.query_kind(sql) == "anchors" else rows

        monkeypatch.setattr(connector, "fetch_all", reordered)
        with pytest.raises(RuntimeError, match="differs from fixture facts"):
            probe.capture_validator(connector, facts)


@pytest.mark.deep
@pytest.mark.parametrize("files", (4096, 32768))
@pytest.mark.parametrize("regime", ("distinct", "duplicate", "metadata"))
def test_sqlite_production_seek_cost_and_removed_tuple_negative_control(
    probe: ModuleType, tmp_path: Path, files: int, regime: str
) -> None:
    facts = probe.file_facts(probe.Shape(files, regime))
    expected = probe.expected_stream_rows(facts)
    receipts = []
    with probe.databases("sqlite", 1) as connections:
        connector = next(connections)
        probe.seed_fixture(connector, facts)

        def measured(sql: str, parameters: tuple[Any, ...]) -> tuple[list[Any], int]:
            steps = 0

            def progress() -> int:
                nonlocal steps
                steps += 100
                return 0

            connector.connection.set_progress_handler(progress, 100)
            try:
                rows = connector.fetch_all(sql, parameters)
            finally:
                connector.connection.set_progress_handler(None, 0)
            # The progress callback counts complete 100-opcode blocks. Add one
            # block to bound the unobserved partial block instead of hiding it.
            return rows, steps + 100

        for _cycle in range(3):
            captured, _ = probe.capture_validator(connector, facts)
            for kind, queries in captured.items():
                # An independently declared generous VM budget for bounded
                # row work and point joins, not an elapsed-time threshold.
                budget = 128 * (128 + 1) + 256
                rejected = False
                for index in set(probe.sample_positions(queries).values()):
                    query = queries[index]
                    page = expected[kind][index * 128 : (index + 1) * 128]
                    rows, steps = measured(query.sql, query.parameters)
                    assert rows == page
                    assert steps <= budget, (kind, index, steps, budget)
                    production_steps = steps
                    # Removing the row constructor preserves non-NULL ordering
                    # but regresses SQLite into rescanning a growing prefix.
                    degraded, count = re.subn(
                        r"\s+AND\s+\([\w.,\s]+\)\s*>\s*\((?:%s,?\s*)+\)",
                        "",
                        query.sql,
                    )
                    assert count == 1
                    key_size = 4 if kind == "derived_hash_occurrences" else 3
                    parameters = (*query.parameters[: -key_size - 1], 128)
                    rows, steps = measured(degraded, parameters)
                    assert rows == page
                    rejected |= steps > budget
                    receipts.append(
                        {
                            "cycle": _cycle,
                            "stream": kind,
                            "page": index,
                            "returned_rows": len(rows),
                            "production_vm_upper_bound": production_steps,
                            "removed_tuple_vm_upper_bound": steps,
                            "budget": budget,
                        }
                    )
                # Duplicate 4096 input has only 32 stored hash groups, so its
                # entire stream legitimately fits inside the page-work budget.
                if len(expected[kind]) > 128:
                    assert rejected, (kind, files, regime)
    (tmp_path / "sqlite-seek-cost.json").write_text(
        json.dumps(
            {
                "files": files,
                "regime": regime,
                "sqlite_version": sqlite3.sqlite_version,
                "samples": receipts,
            }
        )
    )


def test_missing_scope_is_rejected_instead_of_reporting_success(
    probe: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    facts = probe.file_facts(probe.Shape(1))
    original = probe.query_kind
    monkeypatch.setattr(
        probe, "query_kind", lambda sql: None if "AS anchor" in sql else original(sql)
    )
    with probe.databases("sqlite", 1) as connections:
        connector = next(connections)
        probe.seed_fixture(connector, facts)
        with pytest.raises(RuntimeError, match="incomplete role stream"):
            probe.capture_validator(connector, facts)


@pytest.mark.parametrize(
    "after", [(0, 0, b""), (1, 1, b"a"), (1, 2, b"b"), (2, 2, b"z")]
)
def test_production_predicates_and_controls_preserve_independent_ordered_output(
    probe: ModuleType, after: tuple[int, int, bytes]
) -> None:
    query = (
        "SELECT gallery_id, observation_id, file_key FROM facts "
        "WHERE (gallery_id > %s OR (gallery_id = %s AND observation_id > %s) "
        "OR (gallery_id = %s AND observation_id = %s AND file_key > %s)) "
        "AND (gallery_id, observation_id, file_key) > (%s, %s, %s) "
        "ORDER BY gallery_id, observation_id, file_key LIMIT %s"
    )
    gallery, observation, key = after
    parameters = (gallery, gallery, observation, gallery, observation, key, *after, 4)
    facts = list(product((1, 2), (1, 2), (b"a", b"b", b"c")))
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute(
            "CREATE TABLE facts (gallery_id INTEGER, observation_id INTEGER, file_key BLOB)"
        )
        connection.executemany(
            "INSERT INTO facts VALUES (?,?,?)",
            facts,
        )
        expected = sorted(row for row in facts if row > after)[:4]
        assert (
            connection.execute(query.replace("%s", "?"), parameters).fetchall()
            == expected
        )
        for rewrite in (probe.tuple_seek_baseline, probe.degraded_order):
            sql, args = rewrite(query, parameters)
            assert (
                connection.execute(sql.replace("%s", "?"), args).fetchall() == expected
            )
    finally:
        connection.close()


def test_baseline_refuses_unknown_or_null_keysets(probe: ModuleType) -> None:
    with pytest.raises(ValueError, match="exactly one"):
        probe.tuple_seek_baseline("SELECT 1", ())
    with pytest.raises(ValueError, match="non-NULL"):
        probe.tuple_seek_baseline(
            "SELECT * FROM f WHERE unknown ORDER BY a, b, c LIMIT %s",
            (1, 1, 1, 1, 1, None, 1, 1, None, 128),
        )


def test_cost_oracle_rejects_degraded_work_without_relabeling_production(
    probe: ModuleType,
) -> None:
    assert probe.cost_verdict(768, [770, 770, 770], 1064) == "observed_within_budget"
    assert probe.cost_verdict(32768, [32769, 32769, 32769], 1064) == "violated"
    # Returned rows may be tiny after an index-side filter: Handler evidence
    # independently prevents low r_rows from incorrectly passing the oracle.
    assert probe.cost_verdict(0, [32769], 1064) == "violated"
    with pytest.raises(ValueError, match="missing measured"):
        probe.cost_verdict(0, [], 1064)
    with pytest.raises(ValueError, match="invalid cost"):
        probe.cost_verdict(0, [-1], 1064)
    with pytest.raises(ValueError, match="invalid cost"):
        probe.cost_verdict(float("nan"), [1], 1064)


@pytest.mark.parametrize(
    "node",
    [
        {"table_name": "t", "r_rows": 1},
        {"table_name": "t", "r_loops": 1},
        {"table_name": "t", "r_loops": 1, "r_rows": None},
        {"table_name": "t", "r_loops": -1, "r_rows": 1},
        {"table_name": "t", "r_loops": 1, "r_rows": float("nan")},
    ],
)
def test_missing_executed_plan_counters_fail_closed(
    probe: ModuleType, node: dict[str, Any]
) -> None:
    with pytest.raises(ValueError, match="valid"):
        probe.table_row_visits({"query_block": {"table": node}})


def test_unexecuted_empty_tail_join_is_explicitly_zero(probe: ModuleType) -> None:
    assert (
        probe.table_row_visits(
            {
                "query_block": {
                    "table": {
                        "table_name": "join",
                        "r_loops": 0,
                        "r_rows": None,
                    }
                }
            }
        )
        == 0
    )
    with pytest.raises(ValueError, match="lacks table"):
        probe.table_row_visits({"query_block": {}})


def test_nonstandard_plan_string_repairs_preserve_raw_and_numeric_evidence(
    probe: ModuleType,
) -> None:
    raw = '{"query_block":{"attached_condition":"key=\\q\u0001","table":{"table_name":"t","r_loops":2,"r_rows":128}}}'
    decoded = probe.decode_plan((raw,))
    assert decoded["raw_text"] == raw
    assert decoded["nonstandard_strings_repaired"] is True
    assert decoded["plan"]["query_block"]["attached_condition"] == "key=\\q\u0001"
    assert probe.table_row_visits(decoded["plan"]) == 256
    with pytest.raises(json.JSONDecodeError):
        probe.decode_plan(('{"query_block":{"rows": NaNo}}',))


def test_binary_condition_with_raw_quote_keeps_counts_and_original_evidence(
    probe: ModuleType,
) -> None:
    # Reduced real MariaDB 10.11.11 output: a binary key inside a SQL string
    # contains a newline and an unescaped JSON quote.
    raw = """{
  "query_block": {
    "table": {
      "table_name": "file_sha",
      "r_loops": 1,
      "r_rows": 128,
      "attached_condition": "key > <cache>('binary\nraw"quote')",
      "using_index": true
    }
  }
}"""
    result = probe.decode_plan((raw,))
    assert result["raw_text"] == raw
    assert result["condition_strings_redacted"] == 1
    assert result["nonstandard_strings_repaired"] is True
    assert probe.table_row_visits(result["plan"]) == 128
    assert result["plan"]["query_block"]["table"]["using_index"] is True
    with pytest.raises(json.JSONDecodeError):
        probe.decode_plan((raw.replace('"r_rows": 128', '"r_rows": INVALID'),))


@pytest.mark.skipif(
    not hasattr(signal, "SIGALRM"), reason="manual CLI requires POSIX cooperative alarm"
)
@pytest.mark.parametrize("timeout_seconds", (900, 1801, 3600))
def test_case_failure_keeps_partial_report_and_closes_fixture_owner(
    probe: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    timeout_seconds: int,
) -> None:
    output = tmp_path / "report.json"
    closed = []

    class Connector:
        def fetch_one(self, _sql: str) -> tuple[str]:
            return ("10.11.11-disposable",)

        def execute(self, _sql: str) -> None:
            pass

    @contextmanager
    def databases(_backend: str, _count: int) -> Iterator[Iterator[Connector]]:
        assert json.loads(output.read_text())["status"] == "incomplete"
        try:
            yield iter((Connector(),))
        finally:
            closed.append(True)

    def fail(*_args: Any, **_kwargs: Any) -> None:
        raise ValueError("synthetic diagnostic failure")

    monkeypatch.setattr(probe, "databases", databases)
    monkeypatch.setattr(probe, "measure_case", fail)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "role-probe",
            "--scales",
            "127",
            "--regimes",
            "distinct",
            "--timeout-seconds",
            str(timeout_seconds),
            "--output",
            str(output),
        ],
    )
    with pytest.raises(ValueError, match="synthetic diagnostic failure"):
        probe.main()
    assert closed == [True]
    assert json.loads(output.read_text())["status"] == "incomplete"
    assert json.loads(output.read_text())["error"]["type"] == "ValueError"
    assert (
        json.loads(output.read_text())["configured_timeout_seconds"] == timeout_seconds
    )


@pytest.mark.parametrize("timeout_seconds", (0, 3601))
def test_cli_rejects_unbounded_execution_envelope_before_database_or_report(
    probe: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    timeout_seconds: int,
) -> None:
    output = tmp_path / "must-not-exist.json"

    def forbidden(*_args: Any, **_kwargs: Any) -> None:
        pytest.fail("invalid CLI started a database fixture")

    monkeypatch.setattr(probe, "databases", forbidden)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "role-probe",
            "--timeout-seconds",
            str(timeout_seconds),
            "--output",
            str(output),
        ],
    )
    with pytest.raises(SystemExit) as error:
        probe.main()
    assert error.value.code == 2
    assert not output.exists()
