from __future__ import annotations

from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock, patch

import pytest
from vnext_generated_database import open_generated_sqlite_database

from h2hdb import CoreConfig, VNextDatabaseAdminFacade
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_analysis_decision_reader import (
    _KEY_TABLES,
    _layer_key_page,
    iter_resolved_file_decisions,
)
from h2hdb.vnext_analysis_family import AnalysisFamilyCollisionError
from h2hdb.vnext_analysis_overlay_family import (
    AnalysisFileHashDecisionShadowFamily,
    ensure_analysis_file_hash_decision_shadow_family,
)
from h2hdb.vnext_analysis_repository import (
    _iter_snapshot_decisions,
    _Policy,
    _RunAuthority,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


def _key(index: int) -> bytes:
    return index.to_bytes(32, "big")


def _seed_layer(
    connector: SQLConnector, analysis: bytes, values: dict[int, int | None]
) -> None:
    for index, count in values.items():
        if count is None:
            connector.execute(
                "INSERT INTO catalog_analysis_file_hash_decision_tombstone "
                "(analysis_id, file_sha256) VALUES (%s, %s)",
                (analysis, _key(index)),
            )
        else:
            ensure_analysis_file_hash_decision_shadow_family(
                connector,
                AnalysisFileHashDecisionShadowFamily(
                    analysis, _key(index), count, 0, 0
                ),
            )


def _exercise_layers(connector: SQLConnector, *, backend: str, count: int) -> None:
    # Isolate the physical reader from source/operational parent facts. Complete
    # analysis and snapshot handoff remain covered by the repository tests.
    layers: tuple[dict[int, int | None], ...] = (
        {0: None, 1: 91, 130: 92},
        {1: None, 2: 51, 130: None},
        dict.fromkeys(range(count), 7),
    )
    ancestry = (b"a" * 16, b"b" * 16, b"c" * 16)
    with connector.transaction():
        for analysis, values in zip(ancestry, layers, strict=True):
            _seed_layer(connector, analysis, values)
        # An unrelated analysis must never affect the result or key pages.
        _seed_layer(connector, b"u" * 16, {0: 999, 999: 1})
    expected: dict[int, int | None] = {}
    for values in reversed(layers):
        expected.update(values)
    expected_rows = [
        (_key(key), value) for key, value in sorted(expected.items()) if value
    ]
    with connector.read_transaction():
        for _repeat in range(2):
            rows = list(iter_resolved_file_decisions(connector, ancestry=ancestry))
            assert [
                (row.file_sha256, row.occurrence_count) for row in rows
            ] == expected_rows
        if count != 130:
            return
        with patch.object(connector, "fetch_all", wraps=connector.fetch_all) as fetched:
            tail = _layer_key_page(connector, analysis_id=ancestry[-1], after=_key(127))
        assert tail == (_key(128), _key(129))
        sql, parameters = fetched.call_args.args
        assert sql.count("LIMIT %s") == len(_KEY_TABLES) + 1
        if backend == "mariadb":
            plan = connector.fetch_all("EXPLAIN " + sql, parameters)
            physical = [row for row in plan if row[2] in _KEY_TABLES]
            assert len(physical) == len(_KEY_TABLES)
            for row in physical:
                assert row[3] == "range", row
                if row[2] == "catalog_analysis_file_hash_decision_tombstone":
                    # Sparse tombstones can favor the hash-first FK index.
                    # Either range must include the 32-byte cursor, rather
                    # than rereading the 16-byte analysis prefix alone.
                    assert row[5] in {
                        "PRIMARY",
                        "ix_fk_analysis_file_hash_decision_tombstone_2_file_sha256",
                    }, row
                    assert int(row[6]) >= 32, row
                else:
                    assert row[5] == "PRIMARY", row
                    assert str(row[6]) == "48", row
        else:
            plan = connector.fetch_all("EXPLAIN QUERY PLAN " + sql, parameters)
            descriptions = [str(row[3]) for row in plan]
            for table in _KEY_TABLES:
                assert any(
                    f"SEARCH {table} USING" in row
                    and "analysis_id=? AND file_sha256>?" in row
                    for row in descriptions
                ), plan


@pytest.mark.parametrize("count", [0, 1, 127, 128, 129, 130])
def test_sqlite_nearest_layers_cross_pages_and_seek_tail(
    tmp_path: Path, count: int
) -> None:
    with open_generated_sqlite_database(tmp_path / "decisions.sqlite3") as connector:
        connector.execute("PRAGMA foreign_keys = OFF")
        _exercise_layers(connector, backend="sqlite", count=count)


def test_live_mariadb_nearest_layers_cross_pages_and_seek_tail(
    mariadb_config: CoreConfig,
) -> None:
    from test_vnext_live_mariadb_analysis_repository import _connector

    VNextDatabaseAdminFacade(mariadb_config).initialize()
    with _connector(mariadb_config) as connector:
        connector.execute("SET FOREIGN_KEY_CHECKS = 0")
        _exercise_layers(connector, backend="mariadb", count=130)


@pytest.mark.parametrize("fault", ["partial", "shadow_and_tombstone"])
def test_partial_or_conflicting_layer_fails_closed(tmp_path: Path, fault: str) -> None:
    with open_generated_sqlite_database(tmp_path / "corrupt.sqlite3") as connector:
        connector.execute("PRAGMA foreign_keys = OFF")
        analysis = b"a" * 16
        with connector.transaction():
            _seed_layer(connector, analysis, {0: 1})
            if fault == "partial":
                connector.execute(
                    "DELETE FROM catalog_a_file_decision_shadow_anchors "
                    "WHERE analysis_id = %s",
                    (analysis,),
                )
            else:
                _seed_layer(connector, analysis, {0: None})
        with connector.read_transaction(), pytest.raises(AnalysisFamilyCollisionError):
            list(iter_resolved_file_decisions(connector, ancestry=(analysis,)))


def test_snapshot_still_rejects_zero_occurrences_when_schema_checks_are_bypassed(
    tmp_path: Path,
) -> None:
    with open_generated_sqlite_database(tmp_path / "zero.sqlite3") as connector:
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute("PRAGMA ignore_check_constraints = ON")
        analysis = b"a" * 16
        with connector.transaction():
            _seed_layer(connector, analysis, {0: 1})
            connector.execute(
                "UPDATE catalog_a_file_decision_shadow_occurrences "
                "SET occurrence_count = 0 WHERE analysis_id = %s",
                (analysis,),
            )
            connector.execute(
                "INSERT INTO catalog_analysis_state_ancestry "
                "(analysis_id, ancestor_depth, ancestor_analysis_id) VALUES (%s, 0, %s)",
                (analysis, analysis),
            )
        authority = _RunAuthority(
            analysis, b"b" * 16, _Policy(1, 1, 1, 3, 1, 1), None, 0
        )
        with (
            connector.read_transaction(),
            pytest.raises(ValueError, match="occurrence_count must be in 1"),
        ):
            list(
                _iter_snapshot_decisions(
                    VNextUnitOfWork(connector, backend="sqlite"), authority
                )
            )


def test_seventeen_layers_preserve_revival_and_reject_masked_orphans(
    tmp_path: Path,
) -> None:
    with open_generated_sqlite_database(tmp_path / "deep.sqlite3") as connector:
        connector.execute("PRAGMA foreign_keys = OFF")
        ancestry = tuple(index.to_bytes(16, "big") for index in range(17))
        with connector.transaction():
            for depth, analysis in enumerate(ancestry):
                _seed_layer(connector, analysis, {0: None if depth % 2 else depth + 1})
        with connector.read_transaction():
            rows = list(iter_resolved_file_decisions(connector, ancestry=ancestry))
            assert [(row.file_sha256, row.occurrence_count) for row in rows] == [
                (_key(0), 1)
            ]
        with connector.transaction():
            connector.execute(
                "DELETE FROM catalog_a_file_decision_shadow_anchors "
                "WHERE analysis_id = %s",
                (ancestry[-1],),
            )
        with connector.read_transaction(), pytest.raises(AnalysisFamilyCollisionError):
            list(iter_resolved_file_decisions(connector, ancestry=ancestry))


@pytest.mark.parametrize(
    "ancestry",
    [(), (b"a" * 16,) * 2, (b"short",), tuple(_key(i)[:16] for i in range(18))],
)
def test_invalid_ancestry_is_rejected_before_sql(ancestry: tuple[bytes, ...]) -> None:
    connector = Mock(spec=SQLConnector)
    with pytest.raises(ValueError):
        list(
            iter_resolved_file_decisions(
                cast(SQLConnector, connector), ancestry=ancestry
            )
        )
    connector.fetch_all.assert_not_called()


@pytest.mark.parametrize(
    "rows",
    [
        [(_key(1),), (_key(1),)],
        [(_key(1),), (_key(0),)],
        [(_key(0),)],
        [(b"short",)],
        [(_key(1), 1)],
        [(_key(i),) for i in range(1, 130)],
    ],
)
def test_malformed_or_nonadvancing_key_page_is_rejected(
    rows: list[tuple[Any, ...]],
) -> None:
    connector = Mock(spec=SQLConnector)
    connector.fetch_all.return_value = rows
    with pytest.raises((ValueError, AnalysisFamilyCollisionError)):
        _layer_key_page(
            cast(SQLConnector, connector), analysis_id=b"a" * 16, after=_key(0)
        )
