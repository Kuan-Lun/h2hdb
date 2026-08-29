from __future__ import annotations

from typing import Any

import pytest

from h2hdb import CoreConfig
from h2hdb import vnext_identity as identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.vnext_catalog_reader_repository import VNextCatalogReaderRepository
from h2hdb.vnext_publication_family import (
    CatalogPublicationFamily,
    CatalogPublicationTitleFamily,
    PublicationFamilyCollisionError,
    ensure_catalog_publication_family,
    ensure_catalog_publication_title_family,
)


def _generated_mariadb(config: CoreConfig) -> MariaDBConnector:
    database = config.database
    connector = MariaDBConnector(
        host=database.host,
        port=database.port,
        user=database.user,
        password=database.password,
        database=database.database,
    )
    connector.connect()
    payload: Any = ARTIFACT["backends"]
    payload = payload["mariadb"]
    for _slice_id, statements in payload["slices"]:
        for _statement_id, _kind, _name, sql in statements:
            connector.execute(sql)
    for seed in payload["bootstrap_seeds"]:
        connector.execute(seed["sql"], seed["parameters"])
    return connector


def test_mariadb_selected_cte_preserves_binary_publication_keys(
    mariadb_config: CoreConfig,
) -> None:
    connector = _generated_mariadb(mariadb_config)
    publication_key = b"\x80\xff" + b"publication-key-binary-value!".ljust(30, b"!")
    artifact_sha256 = b"\x81" + b"artifact-digest-value".ljust(31, b"!")
    semantics_sha256 = b"\x82" + b"semantics-digest-value".ljust(31, b"!")
    assert all(
        len(value) == 32
        for value in (
            publication_key,
            artifact_sha256,
            semantics_sha256,
        )
    )
    try:
        connector.execute("SET FOREIGN_KEY_CHECKS = 0")
        with connector.transaction():
            connector.execute(
                "INSERT INTO catalog_artifact_blobs "
                "(artifact_sha256, size_bytes) VALUES (%s, %s)",
                (artifact_sha256, 123),
            )
            connector.execute(
                "INSERT INTO catalog_artifacts "
                "(revision, publication_key, artifact_sha256, "
                "artifact_semantics_sha256) VALUES (%s, %s, %s, %s)",
                (1, publication_key, artifact_sha256, semantics_sha256),
            )
        connector.execute("SET FOREIGN_KEY_CHECKS = 1")

        reader = VNextCatalogReaderRepository(backend="mariadb")
        with connector.read_transaction():
            facts = reader._artifact_facts_for_publications(
                connector,
                revision=1,
                publication_keys=(publication_key,),
            )

        assert facts == {
            publication_key: (
                artifact_sha256,
                123,
                semantics_sha256,
            )
        }
    finally:
        connector.close()


def test_mariadb_catalog_publication_rows_are_atomic_and_exact_replayable(
    mariadb_config: CoreConfig,
) -> None:
    connector = _generated_mariadb(mariadb_config)
    gid = 17
    publication = CatalogPublicationFamily(
        revision=1,
        publication_key=identity.publication_key(gid),
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
        connector.execute("SET FOREIGN_KEY_CHECKS = 0")
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
            assert ensure_catalog_publication_family(
                connector,
                publication,
                backend="mariadb",
            ) == (publication, True)
            assert ensure_catalog_publication_title_family(
                connector,
                title,
                backend="mariadb",
            ) == (title, False)

        with connector.transaction():
            assert ensure_catalog_publication_family(
                connector,
                publication,
                backend="mariadb",
            ) == (publication, False)
            assert ensure_catalog_publication_title_family(
                connector,
                title,
                backend="mariadb",
            ) == (title, False)

        changed = CatalogPublicationFamily(
            revision=publication.revision,
            publication_key=publication.publication_key,
            gallery_id=publication.gallery_id,
            summary_sha256=b"x" * 32,
            language_sha256=publication.language_sha256,
            modified_at=publication.modified_at,
            source_title_sha256=publication.source_title_sha256,
        )
        with (
            connector.transaction(),
            pytest.raises(
                PublicationFamilyCollisionError,
                match="catalog publication replay changed exact facts",
            ),
        ):
            ensure_catalog_publication_family(
                connector,
                changed,
                backend="mariadb",
            )

        assert connector.fetch_one(
            "SELECT gallery_id, summary_sha256, language_sha256, modified_at "
            "FROM catalog_publications WHERE revision = %s "
            "AND publication_key = %s",
            (publication.revision, publication.publication_key),
        ) == (
            publication.gallery_id,
            publication.summary_sha256,
            publication.language_sha256,
            publication.modified_at,
        )
        assert connector.fetch_one(
            "SELECT source_title_sha256, source_gallery_name "
            "FROM catalog_publication_titles WHERE revision = %s "
            "AND publication_key = %s",
            (title.revision, title.publication_key),
        ) == (title.source_title_sha256, title.source_gallery_name)
    finally:
        connector.execute("SET FOREIGN_KEY_CHECKS = 1")
        connector.close()
