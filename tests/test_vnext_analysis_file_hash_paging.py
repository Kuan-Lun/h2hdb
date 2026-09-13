from __future__ import annotations

from pathlib import Path
from typing import cast
from unittest.mock import patch

import pytest
from vnext_generated_database import open_generated_sqlite_database

from h2hdb import CoreConfig, VNextDatabaseAdminFacade
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_analysis_repository import (
    _decision_work_rows,
    _file_hash_union_page,
    _Policy,
    _RunAuthority,
    _validation_key_rows,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

_SHADOW_COLUMNS = {
    "catalog_a_file_decision_shadow_anchors": None,
    "catalog_a_file_decision_shadow_occurrences": "occurrence_count",
    "catalog_a_file_decision_shadow_artists": "artist_count",
    "catalog_a_file_decision_shadow_gallery_artist_max": "maximum_gallery_artist_count",
    "catalog_a_file_decision_shadow_seals": None,
}


def _digest(value: int) -> bytes:
    return value.to_bytes(32, "big")


def _exercise_pages(connector: SQLConnector, backend: str) -> None:
    build, analysis, parent, unrelated, ancestor = (
        letter * 16 for letter in (b"b", b"a", b"p", b"u", b"g")
    )
    authority = _RunAuthority(analysis, build, _Policy(1, 1, 2, 3, 1, 1), parent, 1)
    source = set(range(1, 601, 2))
    baseline = set(range(200, 501, 3))
    orphans = set(range(701, 706))
    tombstones = {3, 800}
    changed = {3, 900}
    with connector.transaction():
        # More duplicate occurrences than one page: DISTINCT must precede the
        # branch LIMIT, including when every early occurrence has the same hash.
        members = [(build, gallery, 1) for gallery in range(1, 153)]
        connector.execute_many(
            "INSERT INTO catalog_source_build_galleries "
            "(build_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
            [*members, (unrelated, 153, 1)],
        )
        connector.execute_many(
            "INSERT INTO catalog_gallery_observation_validation_dispositions "
            "(gallery_id, observation_id, accepted) VALUES (%s, %s, %s)",
            [(gallery, 1, int(gallery != 152)) for gallery in range(1, 154)],
        )
        connector.execute_many(
            "INSERT INTO catalog_gallery_observation_file_hash_occurrences "
            "(gallery_id, observation_id, file_sha256, occurrence_count) "
            "VALUES (%s, %s, %s, %s)",
            [
                *((1, 1, _digest(key), 1) for key in sorted(source)),
                *((gallery, 1, _digest(1), 1) for gallery in range(2, 152)),
                (152, 1, _digest(950), 1),
                (153, 1, _digest(951), 1),
            ],
        )
        connector.execute(
            "INSERT INTO catalog_analysis_state_ancestry "
            "(analysis_id, ancestor_depth, ancestor_analysis_id) VALUES (%s, 0, %s)",
            (parent, parent),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_state_ancestry "
            "(analysis_id, ancestor_depth, ancestor_analysis_id) VALUES (%s, 1, %s)",
            (parent, ancestor),
        )
        for offset, (table, column) in enumerate(_SHADOW_COLUMNS.items(), start=701):
            columns = "analysis_id, file_sha256" + (f", {column}" if column else "")
            placeholders = "%s, %s" + (", 1" if column else "")
            connector.execute_many(
                f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
                [
                    *((parent, _digest(key)) for key in sorted(baseline)),
                    (analysis, _digest(offset)),
                    (unrelated, _digest(960)),
                    *((ancestor, _digest(key)) for key in (820, 821, 822)),
                ],
            )
        connector.execute_many(
            "INSERT INTO catalog_analysis_file_hash_decision_tombstone "
            "(analysis_id, file_sha256) VALUES (%s, %s)",
            [
                *((analysis, _digest(key)) for key in sorted(tombstones)),
                (parent, _digest(820)),
            ],
        )
        connector.execute_many(
            "INSERT INTO catalog_analysis_changed_file_hashes "
            "(analysis_id, file_sha256) VALUES (%s, %s)",
            [(analysis, _digest(key)) for key in sorted(changed)],
        )

    work = VNextUnitOfWork(connector, backend=backend)
    expected = sorted(source | baseline | orphans | tombstones | {821, 822})
    with connector.read_transaction():
        for limit in (1, 7, 129):
            found: list[int] = []
            after = None
            while rows := _validation_key_rows(
                work, authority, after=after, limit=limit
            ):
                assert len(rows) <= limit
                found.extend(int.from_bytes(row[0], "big") for row in rows)
                after = rows[-1][0]
            assert found == expected
        for boundary in (0, 1, 257, 599, 705, 800, 1000):
            rows = _validation_key_rows(
                work, authority, after=_digest(boundary), limit=129
            )
            assert rows == [(_digest(key),) for key in expected if key > boundary][:129]
        assert _decision_work_rows(work, authority, after=None, limit=129) == [
            (_digest(key),) for key in sorted(changed)
        ]
        depth_zero = _RunAuthority(analysis, build, authority.policy, None, 0)
        found = []
        after = None
        while rows := _decision_work_rows(work, depth_zero, after=after, limit=129):
            found.extend(int.from_bytes(row[0], "big") for row in rows)
            after = rows[-1][0]
        assert found == sorted(source | changed)

        with patch.object(connector, "fetch_all", wraps=connector.fetch_all) as fetched:
            _validation_key_rows(work, authority, after=_digest(700), limit=129)
        assert fetched.call_count == 1
        sql, parameters = fetched.call_args.args
        assert sql.count("LIMIT %s") == 9  # Eight bounded inputs and the final page.
        assert parameters.count(_digest(700)) == 8
    # A fresh validation page must discover newly added orphan facts instead of
    # reusing an in-memory key cache from an earlier transaction.
    with connector.transaction():
        connector.execute(
            "INSERT INTO catalog_a_file_decision_shadow_seals "
            "(analysis_id, file_sha256) VALUES (%s, %s)",
            (analysis, _digest(970)),
        )
    with connector.read_transaction():
        assert _validation_key_rows(work, authority, after=_digest(822), limit=129) == [
            (_digest(970),)
        ]


def test_sqlite_file_hash_pages_cover_sources_orphans_and_baseline(
    tmp_path: Path,
) -> None:
    connector = open_generated_sqlite_database(tmp_path / "file-hash-pages.sqlite3")
    try:
        # These are deliberately incomplete overlay families. Full production
        # validation tests separately require their rejection with FKs enabled.
        connector.execute("PRAGMA foreign_keys = OFF")
        _exercise_pages(connector, "sqlite")
    finally:
        connector.close()


def test_live_mariadb_file_hash_pages_cover_sources_orphans_and_baseline(
    mariadb_config: CoreConfig,
) -> None:
    from test_vnext_live_mariadb_analysis_repository import _connector

    VNextDatabaseAdminFacade(mariadb_config).initialize()
    with _connector(mariadb_config) as connector:
        connector.execute("SET FOREIGN_KEY_CHECKS = 0")
        _exercise_pages(connector, "mariadb")


@pytest.mark.parametrize("limit", (0, 130, True))
def test_file_hash_key_page_rejects_invalid_limit_before_sql(limit: int) -> None:
    work = VNextUnitOfWork(cast(SQLConnector, object()), backend="sqlite")
    with pytest.raises((TypeError, ValueError)):
        _file_hash_union_page(work, [], after=None, limit=limit)
