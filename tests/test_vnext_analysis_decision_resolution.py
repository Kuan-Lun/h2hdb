"""Bounded physical decision resolution and hidden-layer corruption evidence.

The storage fixtures disable FKs to isolate the reader; full source/analysis
workflow and fresh preparation authority are exercised by bounded preparation.
Costs are SQLite VM instructions, MariaDB Handler reads, SQL calls and returned
rows. D is ancestry layers (1..17), K is requested keys (1..128), and H is
unrelated retained decisions. Budgets are fixed independently of measured timing.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest
from test_vnext_analysis_decision_layers import _seed_shadows
from vnext_generated_database import open_generated_sqlite_database

from h2hdb import CoreConfig, VNextDatabaseAdminFacade
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_analysis_decision_batch import load_resolved_file_decision_page
from h2hdb.vnext_analysis_family import AnalysisFamilyCollisionError
from h2hdb.vnext_analysis_overlay_family import AnalysisFileHashDecisionShadowFamily

_TABLES = (
    "catalog_a_file_decision_shadow_anchors",
    "catalog_a_file_decision_shadow_occurrences",
    "catalog_a_file_decision_shadow_artists",
    "catalog_a_file_decision_shadow_gallery_artist_max",
    "catalog_a_file_decision_shadow_seals",
)
_TOMBSTONE = "catalog_analysis_file_hash_decision_tombstone"
# Reverse byte order deliberately: ancestry order must not be sorted by ID.
_LAYERS = tuple(index.to_bytes(16, "big") for index in range(17, 0, -1))
_KEYS = tuple(index.to_bytes(32, "big") for index in range(128))


@pytest.fixture
def connector(tmp_path: Path) -> Iterator[SQLiteConnector]:
    with open_generated_sqlite_database(tmp_path / "resolution.sqlite3") as database:
        assert isinstance(database, SQLiteConnector)
        database.execute("PRAGMA foreign_keys = OFF")
        yield database


def _read(
    connector: SQLConnector,
    *,
    layers: tuple[bytes, ...] = _LAYERS,
    keys: tuple[bytes, ...] = _KEYS,
) -> dict[bytes, AnalysisFileHashDecisionShadowFamily]:
    return load_resolved_file_decision_page(connector, ancestry=layers, digests=keys)


def _seed_resolution(
    connector: SQLConnector, layers: tuple[bytes, ...], keys: tuple[bytes, ...]
) -> dict[bytes, AnalysisFileHashDecisionShadowFamily]:
    # Each key exists in the anchor and in a nearer layer. An older tombstone
    # after the winning shadow must not suppress it. Every key's winner varies.
    coordinates = {(layers[-1], key) for key in keys}
    for index, key in enumerate(keys):
        coordinates.add((layers[index % len(layers)], key))
    with connector.transaction():
        families = _seed_shadows(connector, sorted(coordinates))
        tombstones = [
            (layers[index + 1], key)
            for key_no, key in enumerate(keys)
            if (index := key_no % len(layers)) + 1 < len(layers) - 1
        ]
        if tombstones:
            connector.execute_many(
                f"INSERT INTO {_TOMBSTONE} VALUES (%s, %s)", tombstones
            )
    return {
        key: next(families[layer, key] for layer in layers if (layer, key) in families)
        for key in keys
    }


def _sqlite_steps(connector: SQLiteConnector, operation: Callable[[], object]) -> int:
    instructions = 0

    def progress() -> int:
        nonlocal instructions
        instructions += 1
        return 0

    connector.connection.set_progress_handler(progress, 1)
    try:
        operation()
    finally:
        connector.connection.set_progress_handler(None, 0)
    return instructions


def _sqlite_budget(layers: int, keys: int) -> int:
    # Six point joins, two bounded window passes and result validation. This
    # instruction envelope is fixed before measurement; H never enters it.
    return 4096 + 512 * layers * keys


@pytest.mark.parametrize("layer_count", [1, 2, 16, 17])
@pytest.mark.parametrize("key_count", [1, 2, 45, 127, 128])
def test_sqlite_exact_resolution_stays_within_fixed_product_budget(
    connector: SQLiteConnector, layer_count: int, key_count: int
) -> None:
    layers, keys = _LAYERS[:layer_count], _KEYS[:key_count]
    expected = _seed_resolution(connector, layers, keys)
    for _cycle in range(3):
        with patch.object(connector, "fetch_all", wraps=connector.fetch_all) as fetched:
            steps = _sqlite_steps(
                connector,
                lambda: _assert_result(connector, layers, keys[::-1], expected),
            )
        assert steps <= _sqlite_budget(layer_count, key_count)
        assert fetched.call_count == 1
        sql, parameters = fetched.call_args.args
        assert parameters[-1] == key_count + 1
        plan = connector.fetch_all("EXPLAIN QUERY PLAN " + sql, parameters)
        descriptions = [str(row[3]) for row in plan]
        for table in (*_TABLES, _TOMBSTONE):
            assert any(
                f"SEARCH {table} USING" in description
                and "analysis_id=? AND file_sha256=?" in description
                for description in descriptions
            ), descriptions


def _assert_result(
    connector: SQLConnector,
    layers: tuple[bytes, ...],
    keys: tuple[bytes, ...],
    expected: dict[bytes, AnalysisFileHashDecisionShadowFamily],
) -> None:
    assert _read(connector, layers=layers, keys=keys) == expected


@pytest.mark.parametrize(
    ("layers", "keys"),
    [
        ((), _KEYS[:1]),
        (_LAYERS, ()),
        (_LAYERS + (b"x" * 16,), _KEYS[:1]),
        (_LAYERS[:1], _KEYS + (b"x" * 32,)),
        ((_LAYERS[0], _LAYERS[0]), _KEYS[:1]),
        (_LAYERS[:1], (_KEYS[0], _KEYS[0])),
        ((b"short",), _KEYS[:1]),
        (_LAYERS[:1], (b"short",)),
    ],
)
def test_invalid_or_over_capacity_inputs_issue_no_query(
    layers: tuple[bytes, ...], keys: tuple[bytes, ...]
) -> None:
    with pytest.raises(ValueError):
        _read(cast(SQLConnector, object()), layers=layers, keys=keys)


@pytest.mark.parametrize("table", _TABLES)
@pytest.mark.parametrize("fault", ["missing-member", "orphan-only"])
def test_hidden_incomplete_families_fail_closed(
    connector: SQLiteConnector, table: str, fault: str
) -> None:
    layers, keys = _LAYERS[:2], _KEYS[:1]
    with connector.transaction():
        _seed_shadows(connector, [(layer, keys[0]) for layer in layers])
        removed = (
            (table,)
            if fault == "missing-member"
            else tuple(other for other in _TABLES if other != table)
        )
        for relation in removed:
            connector.execute(
                f"DELETE FROM {relation} WHERE analysis_id = %s", (layers[-1],)
            )
    with pytest.raises(AnalysisFamilyCollisionError, match="partial or conflicting"):
        _read(connector, layers=layers, keys=keys)


@pytest.mark.parametrize("fault", ["absent", "nearest-tombstone", "hidden-conflict"])
def test_absent_tombstoned_or_conflicting_decisions_are_rejected(
    connector: SQLiteConnector, fault: str
) -> None:
    layers, keys = _LAYERS[:2], _KEYS[:1]
    with connector.transaction():
        if fault != "absent":
            _seed_shadows(connector, [(layers[-1], keys[0])])
            tombstone_layer = layers[0]
            if fault == "hidden-conflict":
                _seed_shadows(connector, [(layers[0], keys[0])])
                tombstone_layer = layers[-1]
            connector.execute(
                f"INSERT INTO {_TOMBSTONE} VALUES (%s, %s)",
                (tombstone_layer, keys[0]),
            )
    with pytest.raises(AnalysisFamilyCollisionError):
        _read(connector, layers=layers, keys=keys)


@pytest.mark.parametrize(
    ("table", "column", "value"),
    [
        (_TABLES[1], "occurrence_count", 0),
        (_TABLES[1], "occurrence_count", 1.5),
        (_TABLES[2], "artist_count", -1),
        (_TABLES[3], "maximum_gallery_artist_count", -1),
    ],
)
def test_hidden_invalid_scalars_are_not_masked_by_nearer_shadows(
    connector: SQLiteConnector, table: str, column: str, value: int | float
) -> None:
    connector.execute("PRAGMA ignore_check_constraints = ON")
    layers, keys = _LAYERS[:2], _KEYS[:1]
    with connector.transaction():
        _seed_shadows(connector, [(layer, keys[0]) for layer in layers])
        connector.execute(
            f"UPDATE {table} SET {column} = %s WHERE analysis_id = %s",
            (value, layers[-1]),
        )
    with pytest.raises(AnalysisFamilyCollisionError):
        _read(connector, layers=layers, keys=keys)


def test_exact_int63_endpoint_is_accepted_without_float_rounding(
    connector: SQLiteConnector,
) -> None:
    layers, keys = _LAYERS[:1], _KEYS[:1]
    _seed_resolution(connector, layers, keys)
    with connector.transaction():
        for table, column in zip(
            _TABLES[1:4],
            ("occurrence_count", "artist_count", "maximum_gallery_artist_count"),
            strict=True,
        ):
            connector.execute(f"UPDATE {table} SET {column} = %s", ((1 << 63) - 1,))
    assert _read(connector, layers=layers, keys=keys)[keys[0]] == (
        AnalysisFileHashDecisionShadowFamily(
            layers[0], keys[0], (1 << 63) - 1, (1 << 63) - 1, (1 << 63) - 1
        )
    )


def test_unrelated_history_cannot_consume_product_budget_and_mutant_is_rejected(
    connector: SQLiteConnector,
) -> None:
    layers, keys = _LAYERS, _KEYS[:1]
    expected = _seed_resolution(connector, layers, keys)
    retained = 0
    for count in (0, 8192, 16384):
        if count:
            with connector.transaction():
                connector.execute_many(
                    f"INSERT INTO {_TABLES[0]} VALUES (%s, %s)",
                    [
                        (b"h" * 16, index.to_bytes(32, "big"))
                        for index in range(retained, count)
                    ],
                )
        retained = count
        for _cycle in range(3):
            steps = _sqlite_steps(
                connector, lambda: _assert_result(connector, layers, keys, expected)
            )
            assert steps <= _sqlite_budget(len(layers), len(keys))

    def degraded_but_correct() -> None:
        connector.fetch_one(
            f"SELECT SUM(LENGTH(file_sha256)) FROM {_TABLES[0]} NOT INDEXED"
        )
        _assert_result(connector, layers, keys, expected)

    with pytest.raises(AssertionError):
        assert _sqlite_steps(connector, degraded_but_correct) <= _sqlite_budget(
            len(layers), len(keys)
        )


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [(b"x" * 16, _KEYS[0], 1, 1, 0, 0, 0)],
        [(_LAYERS[0], b"x" * 32, 1, 1, 0, 0, 0)],
        [(_LAYERS[0], _KEYS[0], 1, 1, 0, 0)],
        [(_LAYERS[0], _KEYS[0], 1, 1, 0, 0, 0)] * 2,
        [(_LAYERS[0], _KEYS[0], 1, 1, 0, 0, True)],
        [(_LAYERS[0], _KEYS[0], True, 1, 0, 0, 0)],
        [(_LAYERS[0], _KEYS[0], 1, 0, 0, 0, 0)],
    ],
)
def test_driver_result_shape_identity_and_scalar_validation_fail_closed(
    connector: SQLiteConnector, rows: list[tuple[Any, ...]]
) -> None:
    with (
        patch.object(connector, "fetch_all", return_value=rows),
        pytest.raises(AnalysisFamilyCollisionError),
    ):
        _read(connector, layers=_LAYERS[:1], keys=_KEYS[:1])


def test_local_mariadb_maximum_product_and_hidden_corruption(
    mariadb_config: CoreConfig,
) -> None:
    from test_vnext_live_mariadb_analysis_repository import _connector

    VNextDatabaseAdminFacade(mariadb_config).initialize()
    with _connector(mariadb_config) as database:
        database.execute("SET FOREIGN_KEY_CHECKS = 0")
        expected = _seed_resolution(database, _LAYERS, _KEYS)
        with database.read_transaction():
            for _cycle in range(3):
                before = _handler_reads(database)
                with patch.object(
                    database, "fetch_all", wraps=database.fetch_all
                ) as fetched:
                    _assert_result(database, _LAYERS, _KEYS, expected)
                after = _handler_reads(database)
                assert after - before <= 512 + 64 * len(_LAYERS) * len(_KEYS)
                assert fetched.call_count == 1
                sql, parameters = fetched.call_args.args
                plan = database.fetch_all("EXPLAIN " + sql, parameters)
                physical = [row for row in plan if row[2] in (*_TABLES, _TOMBSTONE)]
                assert len(physical) == 6
                assert all(
                    row[3] == "eq_ref" and str(row[6]) == "48" for row in physical
                )
        with database.transaction():
            database.execute(
                f"DELETE FROM {_TABLES[-1]} WHERE analysis_id = %s AND file_sha256 = %s",
                (_LAYERS[-1], _KEYS[0]),
            )
        with pytest.raises(
            AnalysisFamilyCollisionError, match="partial or conflicting"
        ):
            _read(database)


def _handler_reads(connector: SQLConnector) -> int:
    return sum(
        int(row[1])
        for row in connector.fetch_all("SHOW SESSION STATUS LIKE 'Handler_read%'")
    )
