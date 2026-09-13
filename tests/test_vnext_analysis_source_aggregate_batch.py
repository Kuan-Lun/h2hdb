from __future__ import annotations

from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock, patch

import pytest
from test_vnext_analysis_repository import (
    _independent_file_oracle,
    _seed_build,
    _seed_gallery,
    _seed_root,
    _source_build_id,
)
from vnext_generated_database import open_generated_sqlite_database

import h2hdb.vnext_analysis_repository as analysis
from h2hdb import CoreConfig, VNextDatabaseAdminFacade, VNextSourceQualification
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_transaction import VNextUnitOfWork


def _authority(build: bytes = b"b" * 16) -> analysis._RunAuthority:
    return analysis._RunAuthority(
        b"a" * 16,
        build,
        analysis._Policy(1, 1, 1, 1, 1, 1),
        None,
        0,
    )


def _exercise_source_aggregates(connector: SQLConnector, *, backend: str) -> None:
    keys = tuple(index.to_bytes(32, "big") for index in range(131))
    fixture_connector = cast(Any, connector)
    with connector.transaction():
        scope = _seed_root(fixture_connector)
        build = _source_build_id(
            fixture_connector,
            scope=scope,
            manifest_sha256=b"m" * 32,
            gallery_count=4,
        )
        _seed_build(
            fixture_connector,
            build_id=build,
            scope=scope,
            manifest_byte=ord("m"),
            gallery_count=4,
        )
        galleries = (
            (
                tuple((key, index % 5 + 1) for index, key in enumerate(keys[:128])),
                (1, 2),
            ),
            (tuple((key, 2) for key in keys[:128:2]), (2, 3)),
            (((keys[128], 3),), ()),
            (((keys[0], 99), (keys[129], 11)), (3,)),
        )
        for gallery_id, (occurrences, artists) in enumerate(galleries, start=1):
            _seed_gallery(
                fixture_connector,
                build_id=build,
                scope=scope,
                gallery_id=gallery_id,
                observation_id=1,
                occurrences=occurrences,
                artists=artists,
                serial=100 + gallery_id,
                qualification=(
                    VNextSourceQualification(False, "image_decode_failed", b"page.jpg")
                    if gallery_id == 4
                    else VNextSourceQualification()
                ),
            )
    work = VNextUnitOfWork(connector, backend=backend)
    with connector.read_transaction():
        expected = _independent_file_oracle(fixture_connector, build)
        assert expected[keys[0]] == (3, 3, 2)
        assert expected[keys[1]] == (2, 2, 2)
        assert expected[keys[128]] == (3, 0, 0)
        assert keys[129] not in expected and keys[130] not in expected
        with patch.object(connector, "fetch_all", wraps=connector.fetch_all) as fetched:
            page = analysis._evaluate_file_decision_page(
                work, _authority(build), keys[:128]
            )
        assert fetched.call_count == 3
        assert all(call.args[1][-1] == 129 for call in fetched.call_args_list)
        assert {key: decision.row for key, decision in page.items()} == {
            key: expected[key] for key in keys[:128]
        }
        remaining = analysis._evaluate_file_decision_page(
            work, _authority(build), keys[128:]
        )
        assert {key: decision.row for key, decision in remaining.items()} == {
            keys[128]: (3, 0, 0)
        }


def test_sqlite_source_aggregates_match_python_oracle_for_a_full_page(
    tmp_path: Path,
) -> None:
    connector = open_generated_sqlite_database(tmp_path / "source-aggregates.sqlite3")
    try:
        _exercise_source_aggregates(connector, backend="sqlite")
    finally:
        connector.close()


def test_live_mariadb_source_aggregates_match_python_oracle_for_a_full_page(
    mariadb_config: CoreConfig,
) -> None:
    from test_vnext_live_mariadb_analysis_repository import _connector

    VNextDatabaseAdminFacade(mariadb_config).initialize()
    with _connector(mariadb_config) as connector:
        _exercise_source_aggregates(connector, backend="mariadb")


@pytest.mark.parametrize(
    "keys",
    [
        tuple(index.to_bytes(32, "big") for index in range(129)),
        (b"k" * 32, b"k" * 32),
        (b"short",),
    ],
)
def test_source_aggregate_rejects_invalid_page_before_sql(
    keys: tuple[bytes, ...],
) -> None:
    connector = Mock(spec=SQLConnector)
    with pytest.raises(ValueError):
        analysis._evaluate_file_decision_page(
            VNextUnitOfWork(cast(SQLConnector, connector), backend="sqlite"),
            _authority(),
            keys,
        )
    connector.fetch_all.assert_not_called()


def test_source_aggregate_empty_page_performs_no_sql() -> None:
    connector = Mock(spec=SQLConnector)
    assert (
        analysis._evaluate_file_decision_page(
            VNextUnitOfWork(cast(SQLConnector, connector), backend="sqlite"),
            _authority(),
            (),
        )
        == {}
    )
    connector.fetch_all.assert_not_called()


@pytest.mark.parametrize(
    ("aggregate_index", "replacement"),
    [
        (0, [(b"k" * 32, 1, 2)]),
        (0, [(b"x" * 32, 1)]),
        (0, [(b"k" * 32, 1), (b"k" * 32, 1)]),
        (0, []),
        (1, []),
        (2, []),
    ],
)
def test_source_aggregate_rejects_malformed_or_inconsistent_result_keys(
    aggregate_index: int, replacement: list[tuple[Any, ...]]
) -> None:
    connector = Mock(spec=SQLConnector)
    results = [[(b"k" * 32, 1)] for _ in range(3)]
    results[aggregate_index] = replacement
    connector.fetch_all.side_effect = results
    with pytest.raises(analysis.AnalysisCorruptionError):
        analysis._evaluate_file_decision_page(
            VNextUnitOfWork(cast(SQLConnector, connector), backend="sqlite"),
            _authority(),
            (b"k" * 32,),
        )


@pytest.mark.parametrize(
    ("aggregate_index", "value"),
    [(0, 0), (0, 1 << 63), (1, -1), (2, None)],
)
def test_source_aggregate_rejects_invalid_count_domains(
    aggregate_index: int, value: object
) -> None:
    connector = Mock(spec=SQLConnector)
    results: list[list[tuple[Any, ...]]] = [[(b"k" * 32, 1)] for _ in range(3)]
    results[aggregate_index] = [(b"k" * 32, value)]
    connector.fetch_all.side_effect = results
    with pytest.raises(ValueError):
        analysis._evaluate_file_decision_page(
            VNextUnitOfWork(cast(SQLConnector, connector), backend="sqlite"),
            _authority(),
            (b"k" * 32,),
        )
