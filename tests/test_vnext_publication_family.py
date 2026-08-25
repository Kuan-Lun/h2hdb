from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from h2hdb import vnext_identity as identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_publication_family import (
    CatalogPublicationFamily,
    CatalogPublicationTitleFamily,
    PublicationFamilyCollisionError,
    ensure_catalog_publication_family,
    ensure_catalog_publication_title_family,
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
            assert ensure_catalog_publication_title_family(connector, title) == (
                title,
                False,
            )
        connector.execute("PRAGMA foreign_keys = ON")

        assert connector.fetch_all(
            "SELECT revision, publication_key, gallery_id, summary_sha256, "
            "language_sha256, modified_at FROM catalog_publications"
        ) == [
            (
                publication.revision,
                publication.publication_key,
                publication.gallery_id,
                publication.summary_sha256,
                publication.language_sha256,
                publication.modified_at,
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
