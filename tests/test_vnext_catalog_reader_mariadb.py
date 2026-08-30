from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from h2hdb import (
    CatalogArtifact,
    CatalogPublication,
    CatalogRecentOrder,
    CoreConfig,
    artifact_storage_key,
)
from h2hdb import vnext_identity as identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.vnext_catalog_reader_repository import VNextCatalogReaderRepository
from h2hdb.vnext_publication_family import (
    CatalogPublicationDownloadTimeFamily,
    CatalogPublicationFamily,
    CatalogPublicationTitleFamily,
    PublicationFamilyCollisionError,
    ensure_catalog_publication_download_time_family,
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
    download_time = CatalogPublicationDownloadTimeFamily(
        revision=publication.revision,
        publication_key=publication.publication_key,
        download_time=10,
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
            assert ensure_catalog_publication_download_time_family(
                connector,
                download_time,
                backend="mariadb",
            ) == (download_time, True)
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
            assert ensure_catalog_publication_download_time_family(
                connector,
                download_time,
                backend="mariadb",
            ) == (download_time, False)
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
            "SELECT gallery_id, summary_sha256, language_sha256, modified_at, "
            "download_time "
            "FROM catalog_publications WHERE revision = %s "
            "AND publication_key = %s",
            (publication.revision, publication.publication_key),
        ) == (
            publication.gallery_id,
            publication.summary_sha256,
            publication.language_sha256,
            publication.modified_at,
            download_time.download_time,
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


def test_mariadb_recent_artifact_window_executes_dynamic_download_order(
    mariadb_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_mariadb(mariadb_config)
    gid = 23
    publication_key = identity.publication_key(gid)
    occurrence_sha256 = b"o" * 32
    artifact_sha256 = b"a" * 32
    artifact_semantics_sha256 = b"s" * 32
    publication = CatalogPublication(
        publication_id=identity.publication_id(gid).decode("ascii"),
        gid=gid,
        title="title",
        source_title="source title",
        sort_title="title",
        summary="summary",
        language="en",
        published_at=datetime.fromtimestamp(6, UTC),
        modified_at=datetime.fromtimestamp(7, UTC),
        downloaded_at=datetime.fromtimestamp(8, UTC),
        source_gallery_name="gallery",
        artifacts=(
            CatalogArtifact(
                artifact_id="artifact",
                name=identity.artifact_name(gid).decode("ascii"),
                storage_key=artifact_storage_key(gid),
                media_type="application/vnd.comicbook+zip",
                size_bytes=1,
                sha256=artifact_sha256.hex(),
                modified_at=datetime.fromtimestamp(7, UTC),
            ),
        ),
    )
    try:
        connector.execute("SET FOREIGN_KEY_CHECKS = 0")
        with connector.transaction():
            connector.execute(
                "INSERT INTO catalog_source_revision_descriptors "
                "(source_revision, channel, snapshot_manifest_sha256) "
                "VALUES (%s, %s, %s)",
                (1, b"default", b"m" * 32),
            )
            connector.execute(
                "INSERT INTO catalog_revision_descriptors "
                "(revision, publication_count, artifact_count) "
                "VALUES (%s, %s, %s)",
                (1, 1, 1),
            )
            connector.execute(
                "INSERT INTO catalog_publication_commits "
                "(receipt_id, candidate_id, revision, source_revision, generation, "
                "preparation_id, operational_policy_id, artifact_policy_id, "
                "display_title_policy_id, new_galleries, changed_galleries, "
                "removed_galleries, duplicate_losers, committed_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    b"r" * 16,
                    b"c" * 16,
                    1,
                    1,
                    1,
                    b"p" * 16,
                    1,
                    1,
                    1,
                    1,
                    0,
                    0,
                    0,
                    5_000_000,
                ),
            )
            connector.execute(
                "INSERT INTO catalog_publication_commit_head_receipts "
                "(channel, receipt_id) VALUES (%s, %s)",
                (b"default", b"r" * 16),
            )
            connector.execute(
                "INSERT INTO catalog_publication_identities "
                "(publication_key, gid) VALUES (%s, %s)",
                (publication_key, gid),
            )
            connector.execute(
                "INSERT INTO catalog_gallery_upload_times "
                "(gid, upload_time) VALUES (%s, %s)",
                (gid, 6_000_000),
            )
            connector.execute(
                "INSERT INTO catalog_publication_occurrence_identities "
                "(revision, publication_key, catalog_occurrence_sha256) "
                "VALUES (%s, %s, %s)",
                (1, publication_key, occurrence_sha256),
            )
            connector.execute(
                "INSERT INTO catalog_publication_download_times "
                "(catalog_occurrence_sha256, download_time) VALUES (%s, %s)",
                (occurrence_sha256, 8_000_000),
            )
            connector.execute(
                "INSERT INTO catalog_artifacts "
                "(revision, publication_key, artifact_sha256, "
                "artifact_semantics_sha256) VALUES (%s, %s, %s, %s)",
                (
                    1,
                    publication_key,
                    artifact_sha256,
                    artifact_semantics_sha256,
                ),
            )
        connector.execute("SET FOREIGN_KEY_CHECKS = 1")

        reader = VNextCatalogReaderRepository(backend="mariadb")
        monkeypatch.setattr(
            reader,
            "_hydrate_publications",
            lambda *_args, **_kwargs: {publication_key: publication},
        )
        with connector.read_transaction():
            window = reader.list_recent_artifact_publications(
                connector,
                order=CatalogRecentOrder.DOWNLOADED,
                revision=1,
            )

        assert window.order is CatalogRecentOrder.DOWNLOADED
        assert window.publications == (publication,)
    finally:
        connector.execute("SET FOREIGN_KEY_CHECKS = 1")
        connector.close()
