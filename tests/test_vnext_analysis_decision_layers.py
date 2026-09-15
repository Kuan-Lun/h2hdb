"""Physical decision layers are bounded independently of retained history.

These focused storage fixtures omit unrelated source parents with FKs disabled;
they do not claim to exercise the complete READY/source lifecycle.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any, Literal, cast
from unittest.mock import patch

import pytest
from vnext_generated_database import open_generated_sqlite_database

from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_analysis_decision_batch import (
    load_file_decision_shadow_layers,
    load_file_decision_shadow_page,
    load_file_decision_tombstone_layers,
    load_file_decision_tombstone_page,
)
from h2hdb.vnext_analysis_family import (
    AnalysisFamilyCollisionError,
    AnalysisFamilyPartialError,
)
from h2hdb.vnext_analysis_overlay_family import AnalysisFileHashDecisionShadowFamily

_SHADOW_TABLES = (
    "catalog_a_file_decision_shadow_anchors",
    "catalog_a_file_decision_shadow_occurrences",
    "catalog_a_file_decision_shadow_artists",
    "catalog_a_file_decision_shadow_gallery_artist_max",
    "catalog_a_file_decision_shadow_seals",
)
_TOMBSTONE = "catalog_analysis_file_hash_decision_tombstone"
_ANALYSES = tuple(index.to_bytes(16, "big") for index in range(17))
_KEYS = tuple(index.to_bytes(32, "big") for index in range(128))
type _Kind = Literal["shadow", "tombstone"]
type _LayerKey = tuple[bytes, bytes]


@pytest.fixture
def connector(tmp_path: Path) -> Iterator[SQLConnector]:
    database = open_generated_sqlite_database(tmp_path / "layers.sqlite3")
    try:
        database.execute("PRAGMA foreign_keys = OFF")
        yield database
    finally:
        database.close()


def _read(
    connector: SQLConnector,
    kind: _Kind,
    *,
    analysis_ids: Sequence[bytes] = _ANALYSES,
    digests: Sequence[bytes] = _KEYS,
) -> dict[_LayerKey, AnalysisFileHashDecisionShadowFamily] | frozenset[_LayerKey]:
    if kind == "shadow":
        return load_file_decision_shadow_layers(
            connector, analysis_ids=analysis_ids, digests=digests
        )
    return load_file_decision_tombstone_layers(
        connector, analysis_ids=analysis_ids, digests=digests
    )


def _seed_shadows(
    connector: SQLConnector, identities: Sequence[_LayerKey]
) -> dict[_LayerKey, AnalysisFileHashDecisionShadowFamily]:
    families = {
        (analysis, key): AnalysisFileHashDecisionShadowFamily(
            analysis, key, index + 1, index % 11, index % 7
        )
        for index, (analysis, key) in enumerate(identities)
    }
    for table, field in zip(
        _SHADOW_TABLES,
        (
            None,
            "occurrence_count",
            "artist_count",
            "maximum_gallery_artist_count",
            None,
        ),
        strict=True,
    ):
        rows = [
            (*identity,) if field is None else (*identity, getattr(family, field))
            for identity, family in families.items()
        ]
        slots = "%s, %s" if field is None else "%s, %s, %s"
        columns = "analysis_id, file_sha256" + ("" if field is None else f", {field}")
        connector.execute_many(
            f"INSERT INTO {table} ({columns}) VALUES ({slots})", rows
        )
    return families


@pytest.mark.parametrize("kind", ("shadow", "tombstone"))
def test_maximum_layer_product_uses_one_query_and_exact_index_keys(
    connector: SQLConnector, kind: _Kind
) -> None:
    requested = tuple((analysis, key) for analysis in _ANALYSES for key in _KEYS)
    distractors = ((b"z" * 16, _KEYS[0]), (_ANALYSES[0], b"z" * 32))
    identities = (*requested, *distractors)
    with connector.transaction():
        if kind == "shadow":
            families = _seed_shadows(connector, identities)
            expected: object = {identity: families[identity] for identity in requested}
        else:
            connector.execute_many(
                f"INSERT INTO {_TOMBSTONE} VALUES (%s, %s)", list(identities)
            )
            expected = frozenset(requested)
    with patch.object(connector, "fetch_all", wraps=connector.fetch_all) as fetched:
        result = _read(
            connector, kind, analysis_ids=_ANALYSES[::-1], digests=_KEYS[::-1]
        )
    assert result == expected and len(result) == 2176
    assert fetched.call_count == 1
    sql, parameters = fetched.call_args.args
    assert parameters[-1] == 2177
    assert "CROSS JOIN requested_hashes" in sql and " WHERE " not in sql
    plan = connector.fetch_all("EXPLAIN QUERY PLAN " + sql, parameters)
    descriptions = [str(row[3]) for row in plan]
    tables = _SHADOW_TABLES if kind == "shadow" else (_TOMBSTONE,)
    for table in tables:
        assert any(
            f"SEARCH {table} USING" in description
            and "analysis_id=? AND file_sha256=?" in description
            for description in descriptions
        ), descriptions


def test_single_readers_share_layer_validation_and_preserve_family_values(
    connector: SQLConnector,
) -> None:
    analysis, key = _ANALYSES[0], _KEYS[0]
    with connector.transaction():
        families = _seed_shadows(connector, ((analysis, key), (_ANALYSES[1], key)))
        connector.execute(
            f"INSERT INTO {_TOMBSTONE} VALUES (%s, %s)", (analysis, _KEYS[1])
        )
    with patch.object(connector, "fetch_all", wraps=connector.fetch_all) as fetched:
        assert load_file_decision_shadow_page(
            connector, analysis_id=analysis, digests=_KEYS[:2]
        ) == {key: families[analysis, key]}
        assert load_file_decision_tombstone_page(
            connector, analysis_id=analysis, digests=_KEYS[:2]
        ) == {_KEYS[1]}
    assert fetched.call_count == 2
    assert all(call.args[1][-1] == 3 for call in fetched.call_args_list)


@pytest.mark.parametrize("kind", ("shadow", "tombstone"))
@pytest.mark.parametrize(
    ("analyses", "keys"),
    (
        (_ANALYSES + (b"z" * 16,), _KEYS),
        ((_ANALYSES[0],) * 2, _KEYS),
        ((b"short",), _KEYS),
        (_ANALYSES, _KEYS + (b"z" * 32,)),
        (_ANALYSES, (_KEYS[0],) * 2),
        (_ANALYSES, (b"short",)),
        ((), (b"short",)),
        ((b"short",), ()),
    ),
)
def test_invalid_layer_input_is_rejected_before_query(
    kind: _Kind, analyses: tuple[bytes, ...], keys: tuple[bytes, ...]
) -> None:
    with pytest.raises(ValueError):
        _read(cast(SQLConnector, object()), kind, analysis_ids=analyses, digests=keys)


@pytest.mark.parametrize("kind", ("shadow", "tombstone"))
@pytest.mark.parametrize(("analyses", "keys"), (((), _KEYS), (_ANALYSES, ()), ((), ())))
def test_empty_product_never_queries(
    kind: _Kind, analyses: tuple[bytes, ...], keys: tuple[bytes, ...]
) -> None:
    assert not _read(
        cast(SQLConnector, object()), kind, analysis_ids=analyses, digests=keys
    )


@pytest.mark.parametrize("missing_table", _SHADOW_TABLES)
def test_every_incomplete_layer_is_rejected_including_orphan_children(
    connector: SQLConnector, missing_table: str
) -> None:
    identity = (_ANALYSES[1], _KEYS[0])
    with connector.transaction():
        _seed_shadows(connector, ((_ANALYSES[0], _KEYS[0]), identity))
        connector.execute(
            f"DELETE FROM {missing_table} WHERE analysis_id = %s AND file_sha256 = %s",
            identity,
        )
    with pytest.raises(AnalysisFamilyPartialError, match="partial"):
        load_file_decision_shadow_layers(
            connector, analysis_ids=_ANALYSES[:2], digests=_KEYS[:1]
        )


def _shadow_row(analysis: bytes, key: bytes) -> tuple[Any, ...]:
    return analysis, key, analysis, analysis, 1, analysis, 2, analysis, 3, analysis


@pytest.mark.parametrize("kind", ("shadow", "tombstone"))
@pytest.mark.parametrize(
    "fault", ("shape", "analysis", "key", "bytes", "duplicate", "overflow")
)
def test_corrupt_result_rows_fail_closed(
    connector: SQLConnector, kind: _Kind, fault: str
) -> None:
    analysis, key = _ANALYSES[0], _KEYS[0]
    row = _shadow_row(analysis, key) if kind == "shadow" else (analysis, key, analysis)
    second = (
        _shadow_row(analysis, _KEYS[1])
        if kind == "shadow"
        else (analysis, _KEYS[1], analysis)
    )
    rows = [row, second]
    if fault == "shape":
        rows[0] = row[:-1]
    elif fault == "analysis":
        rows[0] = (b"z" * 16, *row[1:])
    elif fault == "key":
        rows[0] = (analysis, b"z" * 32, *row[2:])
    elif fault == "bytes":
        rows[0] = (bytearray(analysis), *row[1:])
    elif fault == "duplicate":
        rows = [row, row]
    elif fault == "overflow":
        rows = [row] * 3
    with (
        patch.object(connector, "fetch_all", return_value=rows),
        pytest.raises(AnalysisFamilyCollisionError),
    ):
        _read(connector, kind, analysis_ids=(analysis,), digests=_KEYS[:2])


@pytest.mark.parametrize("index", (4, 6, 8))
@pytest.mark.parametrize("value", (-1, 1 << 63, True, "1", None))
def test_invalid_scalar_facts_are_rejected(
    connector: SQLConnector, index: int, value: object
) -> None:
    row = list(_shadow_row(_ANALYSES[0], _KEYS[0]))
    row[index] = value
    with (
        patch.object(connector, "fetch_all", return_value=[tuple(row)]),
        pytest.raises(AnalysisFamilyCollisionError, match="invalid facts"),
    ):
        load_file_decision_shadow_layers(
            connector, analysis_ids=_ANALYSES[:1], digests=_KEYS[:1]
        )


@pytest.mark.parametrize("kind", ("shadow", "tombstone"))
def test_mariadb_physical_branches_retain_connector_primary_hint(kind: _Kind) -> None:
    connector = MariaDBConnector(
        host="unused.invalid",
        port=3306,
        user="unused",
        password="unused",
        database="unused",
    )
    absent_rows = [
        (analysis, key, *((None,) * (8 if kind == "shadow" else 1)))
        for analysis in _ANALYSES[:2]
        for key in _KEYS[:3]
    ]
    with patch.object(connector, "fetch_all", return_value=absent_rows) as fetched:
        assert not _read(connector, kind, analysis_ids=_ANALYSES[:2], digests=_KEYS[:3])
    assert fetched.call_count == 1
    sql, parameters = fetched.call_args.args
    tables = _SHADOW_TABLES if kind == "shadow" else (_TOMBSTONE,)
    for table in tables:
        assert (
            f"LEFT JOIN {table} FORCE INDEX (PRIMARY) "
            f"ON {table}.analysis_id = g.analysis_id "
            f"AND {table}.file_sha256 = h.file_sha256"
        ) in sql
    assert sql.count("CAST(%s AS BINARY(16))") == 2
    assert sql.count("CAST(%s AS BINARY(32))") == 3
    assert " WHERE " not in sql
    assert parameters == (*_ANALYSES[:2], *_KEYS[:3], 7)


def test_zero_occurrences_violate_the_stored_shadow_domain(
    connector: SQLConnector,
) -> None:
    row = list(_shadow_row(_ANALYSES[0], _KEYS[0]))
    row[4] = 0
    with (
        patch.object(connector, "fetch_all", return_value=[tuple(row)]),
        pytest.raises(AnalysisFamilyCollisionError, match="invalid facts"),
    ):
        load_file_decision_shadow_layers(
            connector, analysis_ids=_ANALYSES[:1], digests=_KEYS[:1]
        )


@pytest.mark.parametrize("kind", ("shadow", "tombstone"))
def test_a_truncated_requested_grid_is_rejected(
    connector: SQLConnector, kind: _Kind
) -> None:
    with (
        patch.object(connector, "fetch_all", return_value=[]),
        pytest.raises(AnalysisFamilyCollisionError, match="requested grid"),
    ):
        _read(connector, kind, analysis_ids=_ANALYSES[:1], digests=_KEYS[:1])


def test_tombstone_join_identity_must_equal_requested_layer(
    connector: SQLConnector,
) -> None:
    with (
        patch.object(
            connector,
            "fetch_all",
            return_value=[(_ANALYSES[0], _KEYS[0], _ANALYSES[1])],
        ),
        pytest.raises(AnalysisFamilyCollisionError, match="mismatched identity"),
    ):
        load_file_decision_tombstone_layers(
            connector, analysis_ids=_ANALYSES[:1], digests=_KEYS[:1]
        )
