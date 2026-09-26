"""Real audit pages have two bounded PK ranges, independent of history length.

SQLite exposes plans, not rows examined: its evidence is two range SEARCHes plus
SQL LIMITs. MariaDB ANALYZE additionally measures actual base rows read. Neither
contract is an elapsed-time or whole-database audit completion-time claim.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import closing
from typing import Any
from unittest.mock import patch

import pytest
from vnext_fault_harness import open_connector
from vnext_pipeline import gallery

from h2hdb import CoreConfig, vnext_identity
from h2hdb import catalog_refinement as refinement
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.schema_epoch import _ReadOnlySemanticConnector
from h2hdb.sql_connector import SQLConnector

_PAGE = 128
_UPLOAD = 41
_RELATION = "catalog_gallery_observations"
_UPLOAD_RELATION = "catalog_gallery_observation_upload_times"
_SELECT = (
    "SELECT observed.gallery_id, observed.observation_id, uploaded.upload_time "
    "FROM catalog_gallery_observations AS observed "
    "LEFT JOIN catalog_gallery_observation_upload_times AS uploaded "
    "ON uploaded.gallery_id = observed.gallery_id "
    "AND uploaded.observation_id = observed.observation_id "
)
_ORDER = " ORDER BY observed.gallery_id, observed.observation_id LIMIT 128"


def _create_page_relations(connector: SQLConnector, backend: str) -> None:
    # This is a real generated two-relation query fixture, not a READY database.
    # Unrelated gallery parents/metadata authority are deliberately outside scope.
    connector.execute(
        "PRAGMA foreign_keys = OFF"
        if backend == "sqlite"
        else "SET FOREIGN_KEY_CHECKS=0"
    )
    payload: Any = ARTIFACT["backends"]
    for slice_name, statements in payload[backend]["slices"]:
        if slice_name in (
            "relation:gallery_observation",
            "relation:gallery_observation_upload_time",
        ):
            for _statement_id, _kind, _name, sql in statements:
                connector.execute(sql)


def _coordinates(count: int, distribution: str) -> list[tuple[int, int]]:
    if distribution == "one_gallery":
        return [(1, index) for index in range(1, count + 1)]
    if distribution == "one_observation":
        return [(index, 1) for index in range(1, count + 1)]
    assert distribution == "mixed_257_per_gallery"
    return [(index // 257 + 1, index % 257 + 1) for index in range(count)]


def _seed(connector: SQLConnector, coordinates: list[tuple[int, int]]) -> None:
    with connector.transaction():
        connector.execute(f"DELETE FROM {_UPLOAD_RELATION}")
        connector.execute(f"DELETE FROM {_RELATION}")
        for start in range(0, len(coordinates), _PAGE):
            chunk = coordinates[start : start + _PAGE]
            connector.execute_many(
                f"INSERT INTO {_RELATION} VALUES (%s, %s, %s)",
                [
                    (g, o, (start + i + 1).to_bytes(32, "big"))
                    for i, (g, o) in enumerate(chunk)
                ],
            )
            connector.execute_many(
                f"INSERT INTO {_UPLOAD_RELATION} VALUES (%s, %s, %s)",
                [(g, o, _UPLOAD) for g, o in chunk],
            )


def _tables(value: Any) -> Iterator[dict[str, Any]]:
    if isinstance(value, dict):
        if "table_name" in value:
            yield value
        for item in value.values():
            yield from _tables(item)
    elif isinstance(value, list):
        for item in value:
            yield from _tables(item)


def _assert_page_cost(
    connector: SQLConnector,
    backend: str,
    query: str,
    parameters: tuple[int, ...],
) -> None:
    if backend == "sqlite":
        plan = connector.fetch_all("EXPLAIN QUERY PLAN " + query, parameters)
        base = [str(row[3]) for row in plan if _RELATION in str(row[3])]
        assert len(base) == 2 and all(row.startswith("SEARCH ") for row in base), (
            "observation base ranges must seek, not scan",
            base,
        )
        assert any("(gallery_id=? AND observation_id>?)" in row for row in base)
        assert any("(gallery_id>?)" in row for row in base)
        # Two branch caps and one final cap give <=256 candidates and <=128 rows.
        # Combined with the observed range access this excludes a prefix scan;
        # this does not pretend SQLite reports actual rows-examined counters.
        assert query.count("LIMIT 128") == 3
    else:
        plan = json.loads(
            connector.fetch_one("ANALYZE FORMAT=JSON " + query, parameters)[0]
        )
        base_tables = [
            table
            for table in _tables(plan)
            if table["table_name"] in (_RELATION, "observed")
        ]
        assert len(base_tables) == 2, "expected two independently bounded base ranges"
        examined = [
            float(table["r_rows"]) * int(table["r_loops"]) for table in base_tables
        ]
        assert all(value <= _PAGE for value in examined), examined
        assert sum(examined) <= 2 * _PAGE, examined
        assert all(
            table["access_type"] == "range" and table["key"] == "PRIMARY"
            for table in base_tables
        ), base_tables


def _run_audit(connector: SQLConnector, expected: list[tuple[int, int]]) -> str:
    metadata = vnext_identity.encode_gallery_observation_metadata(
        gallery(1, upload_time=_UPLOAD).metadata()
    )
    decoded = 0
    read: list[tuple[int, int]] = []
    statements: list[str] = []
    actual_fetch = connector.fetch_all
    actual_decode = vnext_identity.validate_gallery_observation_metadata_parts

    def fetch(query: str, parameters: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        statements.append(query)
        rows = actual_fetch(query, parameters)
        assert len(rows) <= _PAGE
        return rows

    def chunks(
        _connector: SQLConnector, gallery_id: int, observation_id: int
    ) -> Iterator[bytes]:
        read.append((gallery_id, observation_id))
        yield metadata

    def decode(parts: Any) -> Any:
        nonlocal decoded
        decoded += 1
        return actual_decode(parts)

    with (
        patch.object(
            refinement, "_validated_open_observation_retirement", return_value=None
        ),
        patch.object(refinement, "require_source_qualification"),
        patch.object(refinement, "iter_metadata_chunks", side_effect=chunks),
        patch.object(
            vnext_identity,
            "validate_gallery_observation_metadata_parts",
            side_effect=decode,
        ),
        patch.object(connector, "fetch_all", side_effect=fetch),
        patch.object(
            connector,
            "fetch_one",
            side_effect=AssertionError("unexpected per-observation SQL"),
        ),
    ):
        refinement.check_source_qualification_v1(_ReadOnlySemanticConnector(connector))
    assert read == expected
    assert decoded == len(expected)
    assert len(statements) == (len(expected) + _PAGE - 1) // _PAGE + 1
    assert len(set(statements)) == 1
    assert statements[0].count(connector.primary_key_table_reference(_RELATION)) == 2
    return statements[0]


@pytest.mark.parametrize(
    "distribution", ("one_gallery", "one_observation", "mixed_257_per_gallery")
)
def test_qualification_real_pages_remain_bounded_through_history_and_repeats(
    db_config: CoreConfig, distribution: str
) -> None:
    backend = db_config.database.sql_type
    with closing(open_connector(db_config)) as connector:
        _create_page_relations(connector, backend)
        for count in (0, 127, 128, 129, 8193):
            coordinates = _coordinates(count, distribution)
            _seed(connector, coordinates)
            for _cycle in range(2):
                with connector.read_transaction():
                    query = _run_audit(connector, coordinates)
                    if count != 8193:
                        continue
                    for before in (0, 127, 128, 129, 4096, 8064, 8192, 8193):
                        after = (0, 0) if before == 0 else coordinates[before - 1]
                        parameters: tuple[int, ...] = (after[0], after[1], after[0])
                        assert connector.fetch_all(query, parameters) == [
                            (*key, _UPLOAD)
                            for key in coordinates[before : before + _PAGE]
                        ]
                        _assert_page_cost(connector, backend, query, parameters)

        # Execute an actually degraded query with identical rows: the guard must
        # reject it. OR regresses SQLite; tuple comparison regresses MariaDB.
        after = coordinates[8063]
        if backend == "sqlite":
            degraded = (
                _SELECT + "WHERE observed.gallery_id > %s OR "
                "(observed.gallery_id = %s AND observed.observation_id > %s)" + _ORDER
            )
            parameters = (after[0], after[0], after[1])
        else:
            degraded = (
                _SELECT
                + "WHERE (observed.gallery_id, observed.observation_id) > (%s, %s)"
                + _ORDER
            )
            parameters = after
        assert connector.fetch_all(degraded, parameters) == [
            (*key, _UPLOAD) for key in coordinates[8064 : 8064 + _PAGE]
        ]
        if backend == "mariadb":
            plan = json.loads(
                connector.fetch_one("ANALYZE FORMAT=JSON " + degraded, parameters)[0]
            )
            examined = sum(
                float(table["r_rows"]) * int(table["r_loops"])
                for table in _tables(plan)
                if table["table_name"] == "observed"
            )
            assert examined > 2 * _PAGE
        with pytest.raises(AssertionError, match="base ranges"):
            _assert_page_cost(connector, backend, degraded, parameters)
