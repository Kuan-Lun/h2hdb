from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from h2hdb import vnext_identity as identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_publication_family import (
    CatalogContributorFamily,
    CatalogPublicationDownloadTimeFamily,
    CatalogPublicationFamily,
    CatalogPublicationTitleFamily,
    PublicationFamilyCollisionError,
    ensure_catalog_contributor_family,
    ensure_catalog_publication_download_time_family,
    ensure_catalog_publication_family,
    ensure_catalog_publication_title_family,
    load_catalog_contributor_family,
    load_catalog_publication_download_time_family,
    load_catalog_publication_family,
    load_catalog_publication_title_family,
)


def _generated_database(path: Path) -> SQLiteConnector:
    connector = SQLiteConnector(str(path))
    connector.connect()
    payload: Any = ARTIFACT["backends"]
    payload = payload["sqlite"]
    for _slice_id, statements in payload["slices"]:
        for _statement_id, _kind, _name, sql in statements:
            connector.execute(sql)
    for seed in payload["bootstrap_seeds"]:
        connector.execute(seed["sql"], seed["parameters"])
    return connector


class _ContributorRecorder:
    def __init__(self, family: CatalogContributorFamily | None = None) -> None:
        self.row: tuple[Any, ...] = tuple()
        if family is not None:
            self.row = (
                family.revision,
                family.publication_key,
                family.position,
                family.contributor_name_sha256,
                family.role,
            )
        self.fetches: list[tuple[str, tuple[Any, ...]]] = []
        self.inserts: list[tuple[str, tuple[Any, ...]]] = []

    def fetch_one(self, query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
        self.fetches.append((query, data))
        return self.row

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> int:
        self.inserts.append((query, data))
        self.row = (data[0], data[1], data[4], data[2], data[3])
        return 1


def test_catalog_contributor_uses_one_atomic_bcnf_relation() -> None:
    family = CatalogContributorFamily(
        revision=3,
        publication_key=b"p" * 32,
        position=7,
        contributor_name_sha256=b"n" * 32,
        role=b"artist",
    )
    connector = _ContributorRecorder(family)

    assert (
        load_catalog_contributor_family(
            connector,
            revision=family.revision,
            publication_key=family.publication_key,
            position=family.position,
            backend="mariadb",
            locking=True,
        )
        == family
    )
    query, parameters = connector.fetches[-1]
    assert "FROM catalog_contributors" in query
    assert query.endswith(" FOR UPDATE")
    assert parameters == (family.revision, family.publication_key, family.position)
    assert "anchor" not in query and "seal" not in query

    empty_connector = _ContributorRecorder()
    assert ensure_catalog_contributor_family(empty_connector, family) == (family, True)
    assert len(empty_connector.inserts) == 1
    insert, parameters = empty_connector.inserts[0]
    assert "INSERT INTO catalog_contributors" in insert
    assert parameters == (
        family.revision,
        family.publication_key,
        family.contributor_name_sha256,
        family.role,
        family.position,
    )
    assert ensure_catalog_contributor_family(empty_connector, family) == (family, False)
    assert len(empty_connector.inserts) == 1


def test_catalog_publication_and_title_are_atomic_exact_replay_rows(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "publication-family.sqlite3")
    gid = 17
    publication_key = identity.publication_key(gid)
    publication = CatalogPublicationFamily(
        revision=1,
        publication_key=publication_key,
        gallery_id=7,
        summary_sha256=b"s" * 32,
        language_sha256=b"l" * 32,
        modified_at=11,
        source_title_sha256=b"t" * 32,
    )
    title = CatalogPublicationTitleFamily(
        revision=1,
        publication_key=publication.publication_key,
        source_title_sha256=b"t" * 32,
        source_gallery_name=b"gallery",
    )
    download_time = CatalogPublicationDownloadTimeFamily(
        revision=publication.revision,
        publication_key=publication.publication_key,
        download_time=10,
    )
    try:
        connector.execute("PRAGMA foreign_keys = OFF")
        with connector.transaction():
            connector.execute(
                "INSERT INTO catalog_gallery_source_name_accesses "
                "(gallery_id, source_gallery_name) VALUES (%s, %s)",
                (publication.gallery_id, title.source_gallery_name),
            )
            connector.execute(
                "INSERT INTO catalog_source_gallery_name_gids "
                "(source_gallery_name, gid) VALUES (%s, %s)",
                (title.source_gallery_name, gid),
            )
            connector.execute(
                "INSERT INTO catalog_publication_identities "
                "(publication_key, gid) VALUES (%s, %s)",
                (publication.publication_key, gid),
            )
            assert ensure_catalog_publication_family(connector, publication) == (
                publication,
                True,
            )
            assert ensure_catalog_publication_download_time_family(
                connector, download_time
            ) == (download_time, True)
            assert ensure_catalog_publication_title_family(connector, title) == (
                title,
                False,
            )
        connector.execute("PRAGMA foreign_keys = ON")

        assert connector.fetch_all(
            "SELECT revision, publication_key, gallery_id, summary_sha256, "
            "language_sha256, modified_at, download_time FROM catalog_publications"
        ) == [
            (
                publication.revision,
                publication.publication_key,
                publication.gallery_id,
                publication.summary_sha256,
                publication.language_sha256,
                publication.modified_at,
                download_time.download_time,
            )
        ]
        assert connector.fetch_all(
            "SELECT revision, publication_key, source_title_sha256, "
            "source_gallery_name FROM catalog_publication_titles"
        ) == [
            (
                title.revision,
                title.publication_key,
                title.source_title_sha256,
                title.source_gallery_name,
            )
        ]

        with connector.transaction():
            assert ensure_catalog_publication_family(connector, publication) == (
                publication,
                False,
            )
            assert ensure_catalog_publication_download_time_family(
                connector, download_time
            ) == (download_time, False)
            assert ensure_catalog_publication_title_family(connector, title) == (
                title,
                False,
            )
            assert (
                load_catalog_publication_family(
                    connector,
                    revision=publication.revision,
                    publication_key=publication.publication_key,
                    locking=True,
                )
                == publication
            )
            assert (
                load_catalog_publication_download_time_family(
                    connector,
                    revision=download_time.revision,
                    publication_key=download_time.publication_key,
                    locking=True,
                )
                == download_time
            )
            assert (
                load_catalog_publication_title_family(
                    connector,
                    revision=title.revision,
                    publication_key=title.publication_key,
                    locking=True,
                )
                == title
            )

        changed_publication = CatalogPublicationFamily(
            revision=publication.revision,
            publication_key=publication.publication_key,
            gallery_id=publication.gallery_id,
            summary_sha256=b"x" * 32,
            language_sha256=publication.language_sha256,
            modified_at=publication.modified_at,
            source_title_sha256=publication.source_title_sha256,
        )
        changed_title = CatalogPublicationTitleFamily(
            revision=title.revision,
            publication_key=title.publication_key,
            source_title_sha256=title.source_title_sha256,
            source_gallery_name=b"changed",
        )
        changed_download_time = CatalogPublicationDownloadTimeFamily(
            revision=download_time.revision,
            publication_key=download_time.publication_key,
            download_time=download_time.download_time + 1,
        )
        with (
            connector.transaction(),
            pytest.raises(
                PublicationFamilyCollisionError,
                match="catalog download-time replay changed exact facts",
            ),
        ):
            ensure_catalog_publication_download_time_family(
                connector, changed_download_time
            )
        with (
            connector.transaction(),
            pytest.raises(
                PublicationFamilyCollisionError,
                match="catalog publication replay changed exact facts",
            ),
        ):
            ensure_catalog_publication_family(connector, changed_publication)
        with (
            connector.transaction(),
            pytest.raises(
                PublicationFamilyCollisionError,
                match="catalog title replay changed exact facts",
            ),
        ):
            ensure_catalog_publication_title_family(connector, changed_title)
    finally:
        connector.close()
