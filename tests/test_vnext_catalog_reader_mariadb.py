from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from unicodedata import unidata_version

import pytest
from vnext_canonical_value_fixtures import seed_canonical_value

from h2hdb import (
    CatalogArtifact,
    CatalogContributorFilter,
    CatalogDiscoveryQuery,
    CatalogFacetKind,
    CatalogPublication,
    CatalogRecentOrder,
    CatalogSubjectFilter,
    CoreConfig,
    StorageObjectDescriptor,
    StorageObjectKey,
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


def _canonical(connector: MariaDBConnector, domain: str, payload: bytes) -> bytes:
    value_sha256 = identity.canonical_value_digest(domain, payload)
    entries = () if not payload else (identity.CanonicalValueChunk(0, payload),)
    page = identity.CanonicalValuePage(
        owner_value_sha256=value_sha256,
        node_kind=identity.GalleryObservationNodeKind.LEAF,
        level=0,
        page_position=0,
        subtree_byte_count=len(payload),
        entries=entries,
    )
    page_bytes = identity.encode_canonical_value_page(page)
    seed_canonical_value(
        connector,
        value_sha256=value_sha256,
        digest_domain=domain.encode("ascii"),
        page_sha256=identity.canonical_value_page_digest(page_bytes),
        page_bytes=page_bytes,
        subtree_item_count=len(payload),
        allocated_at=1,
    )
    return value_sha256


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
                "artifact_semantics_sha256, artifact_name, media_type, page_count) "
                "VALUES (%s, %s, %s, %s, %s, %s, 0)",
                (
                    1,
                    publication_key,
                    artifact_sha256,
                    semantics_sha256,
                    b"artifact-1.cbz",
                    b"application/vnd.comicbook+zip",
                ),
            )
        connector.execute("SET FOREIGN_KEY_CHECKS = 1")

        reader = VNextCatalogReaderRepository(backend="mariadb")
        with connector.read_transaction():
            facts = reader._artifact_facts_for_publications(
                connector,
                revision=1,
                publication_keys=(publication_key,),
            )

        assert set(facts) == {publication_key}
        artifact = facts[publication_key]
        assert artifact.artifact_sha256 == artifact_sha256
        assert artifact.size_bytes == 123
        assert artifact.artifact_semantics_sha256 == semantics_sha256
        assert artifact.artifact_name == b"artifact-1.cbz"
        assert artifact.media_type == b"application/vnd.comicbook+zip"
        assert artifact.page_count == 0
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


def test_mariadb_discovery_facets_and_presentation_hydrate_real_rows(
    mariadb_config: CoreConfig,
) -> None:
    connector = _generated_mariadb(mariadb_config)
    gid = 31
    publication_key = identity.publication_key(gid)
    source_title = _canonical(
        connector,
        "source_title_utf8_v1",
        b"MariaDB Reader",
    )
    display_title = _canonical(
        connector,
        "display_title_utf8_v1",
        b"MariaDB Reader",
    )
    sort_title = _canonical(
        connector,
        "title_sort_utf8_v1",
        b"mariadb reader",
    )
    summary = _canonical(connector, "catalog_summary_utf8_v1", b"summary")
    language = _canonical(connector, "catalog_language_utf8_v1", b"en")
    contributor = _canonical(connector, "contributor_name_utf8_v1", b"Reader")
    subject = _canonical(connector, "tag_value_utf8_v1", b"manga")
    lexeme = _canonical(connector, "search_lexeme_utf8_v1", b"mariadb")
    artifact_sha256 = sha256(b"mariadb acquisition").digest()
    thumbnail_sha256 = sha256(b"mariadb thumbnail").digest()
    acquisition_key = StorageObjectKey("mariadb-reader-v2", ("acquisition", "31"))
    thumbnail_key = StorageObjectKey("mariadb-reader-v2", ("thumbnail", "31"))

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
                "(revision, publication_count, artifact_count) VALUES (1, 1, 1)"
            )
            connector.execute(
                "INSERT INTO catalog_publication_commits "
                "(receipt_id, candidate_id, revision, source_revision, generation, "
                "preparation_id, operational_policy_id, artifact_policy_id, "
                "display_title_policy_id, new_galleries, changed_galleries, "
                "removed_galleries, duplicate_losers, committed_at) "
                "VALUES (%s, %s, 1, 1, 1, %s, 1, 1, 1, 1, 0, 0, 0, 1000000)",
                (b"r" * 16, b"c" * 16, b"p" * 16),
            )
            connector.execute(
                "INSERT INTO catalog_publication_commit_head_receipts "
                "(channel, receipt_id) VALUES (%s, %s)",
                (b"default", b"r" * 16),
            )
            connector.execute(
                "INSERT INTO catalog_gallery_upload_times (gid, upload_time) "
                "VALUES (%s, 2000000)",
                (gid,),
            )
            connector.execute(
                "INSERT INTO catalog_source_gallery_name_gids "
                "(source_gallery_name, gid) VALUES (%s, %s)",
                (b"gallery-31", gid),
            )
            connector.execute(
                "INSERT INTO catalog_gallery_source_name_accesses "
                "(gallery_id, source_gallery_name) VALUES (31, %s)",
                (b"gallery-31",),
            )
            connector.execute(
                "INSERT INTO catalog_publication_identities "
                "(publication_key, gid) VALUES (%s, %s)",
                (publication_key, gid),
            )
            ensure_catalog_publication_family(
                connector,
                CatalogPublicationFamily(
                    revision=1,
                    publication_key=publication_key,
                    gallery_id=31,
                    summary_sha256=summary,
                    language_sha256=language,
                    modified_at=3_000_000,
                    source_title_sha256=source_title,
                ),
                backend="mariadb",
            )
            ensure_catalog_publication_download_time_family(
                connector,
                CatalogPublicationDownloadTimeFamily(
                    revision=1,
                    publication_key=publication_key,
                    download_time=2_500_000,
                ),
                backend="mariadb",
            )
            ensure_catalog_publication_title_family(
                connector,
                CatalogPublicationTitleFamily(
                    revision=1,
                    publication_key=publication_key,
                    source_title_sha256=source_title,
                    source_gallery_name=b"gallery-31",
                ),
                backend="mariadb",
            )
            connector.execute(
                "INSERT INTO catalog_publication_order "
                "(revision, position, publication_key) VALUES (1, 0, %s)",
                (publication_key,),
            )
            connector.execute(
                "INSERT INTO catalog_title_sort_policy "
                "(title_sort_policy_id, title_sort_algorithm_version, "
                "unicode_data_version) VALUES (1, 1, %s)",
                (unidata_version.encode("ascii"),),
            )
            connector.execute(
                "INSERT INTO catalog_display_title_policies "
                "(display_title_policy_id, display_title_algorithm_version, "
                "title_sort_policy_id) VALUES (1, 1, 1)"
            )
            connector.execute(
                "INSERT INTO catalog_display_title_choices "
                "(display_title_policy_id, source_title_sha256, "
                "source_gallery_name, title_sha256) VALUES (1, %s, %s, %s)",
                (source_title, b"gallery-31", display_title),
            )
            connector.execute(
                "INSERT INTO catalog_title_sorts "
                "(title_sort_policy_id, title_sha256, sort_title_sha256) "
                "VALUES (1, %s, %s)",
                (display_title, sort_title),
            )
            connector.execute(
                "INSERT INTO catalog_contributors "
                "(revision, publication_key, position, contributor_name_sha256, role) "
                "VALUES (1, %s, 0, %s, %s)",
                (publication_key, contributor, b"author"),
            )
            connector.execute(
                "INSERT INTO catalog_tag_terms "
                "(tag_id, namespace, tag_value_sha256) VALUES (1, %s, %s)",
                (b"genre", subject),
            )
            connector.execute(
                "INSERT INTO catalog_subjects "
                "(revision, publication_key, position, tag_id) VALUES (1, %s, 0, 1)",
                (publication_key,),
            )
            connector.execute(
                "INSERT INTO catalog_artifact_blobs (artifact_sha256, size_bytes) "
                "VALUES (%s, 100), (%s, 20)",
                (artifact_sha256, thumbnail_sha256),
            )
            connector.execute(
                "INSERT INTO catalog_artifacts "
                "(revision, publication_key, artifact_sha256, "
                "artifact_semantics_sha256, artifact_name, media_type, page_count) "
                "VALUES (1, %s, %s, %s, %s, %s, 1)",
                (
                    publication_key,
                    artifact_sha256,
                    b"s" * 32,
                    b"reader-31.cbz",
                    b"application/vnd.comicbook+zip",
                ),
            )
            for key in (acquisition_key, thumbnail_key):
                key_sha256 = identity.artifact_storage_key_digest(
                    key.codec,
                    key.segments,
                )
                connector.execute(
                    "INSERT INTO catalog_storage_object_key_identities "
                    "(storage_object_key_sha256, key_codec, segment_count) "
                    "VALUES (%s, %s, %s)",
                    (key_sha256, key.codec.encode("ascii"), len(key.segments)),
                )
                for position, segment in enumerate(key.segments):
                    connector.execute(
                        "INSERT INTO catalog_storage_object_key_segments "
                        "(storage_object_key_sha256, segment_position, key_segment) "
                        "VALUES (%s, %s, %s)",
                        (key_sha256, position, segment.encode()),
                    )
            connector.execute(
                "INSERT INTO catalog_storage_objects "
                "(revision, publication_key, resource_kind, "
                "storage_object_key_sha256, storage_object_sha256, size_bytes, "
                "modified_at) VALUES "
                "(1, %s, %s, %s, %s, 100, 3000000), "
                "(1, %s, %s, %s, %s, 20, 3000000)",
                (
                    publication_key,
                    b"acquisition",
                    identity.artifact_storage_key_digest(
                        acquisition_key.codec,
                        acquisition_key.segments,
                    ),
                    artifact_sha256,
                    publication_key,
                    b"thumbnail",
                    identity.artifact_storage_key_digest(
                        thumbnail_key.codec,
                        thumbnail_key.segments,
                    ),
                    thumbnail_sha256,
                ),
            )
            connector.execute(
                "INSERT INTO catalog_pages "
                "(revision, publication_key, resource_kind, page_index, "
                "extent_offset, extent_length, media_type, image_sha256, width, "
                "height) VALUES (1, %s, %s, 0, 10, 20, %s, %s, 640, 480)",
                (publication_key, b"acquisition", b"image/jpeg", b"i" * 32),
            )
            connector.execute(
                "INSERT INTO catalog_thumbnails "
                "(revision, publication_key, resource_kind, extent_offset, "
                "extent_length, media_type, image_sha256, width, height) "
                "VALUES (1, %s, %s, 0, 20, %s, %s, 160, 120)",
                (publication_key, b"thumbnail", b"image/jpeg", thumbnail_sha256),
            )
            connector.execute(
                "INSERT INTO catalog_search_lexemes (value_sha256) VALUES (%s)",
                (lexeme,),
            )
            connector.execute(
                "INSERT INTO catalog_search_documents "
                "(revision, publication_key, row_count) VALUES (1, %s, 1)",
                (publication_key,),
            )
            connector.execute(
                "INSERT INTO catalog_search_postings "
                "(revision, value_sha256, publication_key) VALUES (1, %s, %s)",
                (lexeme, publication_key),
            )
            connector.execute(
                "INSERT INTO catalog_language_facet_order "
                "(revision, position, language_sha256, occurrence_count) "
                "VALUES (1, 0, %s, 1)",
                (language,),
            )
            connector.execute(
                "INSERT INTO catalog_subject_facet_order "
                "(revision, position, tag_id, occurrence_count) VALUES (1, 0, 1, 1)"
            )
            connector.execute(
                "INSERT INTO catalog_contributor_facet_order "
                "(revision, position, contributor_name_sha256, role, "
                "occurrence_count) VALUES (1, 0, %s, %s, 1)",
                (contributor, b"author"),
            )
            connector.execute(
                "INSERT INTO catalog_discovery_seals (revision, policy_id) "
                "VALUES (1, 1)"
            )
        connector.execute("SET FOREIGN_KEY_CHECKS = 1")

        reader = VNextCatalogReaderRepository(backend="mariadb")
        query = CatalogDiscoveryQuery(
            search="MariaDB",
            language="en",
            subject=CatalogSubjectFilter(namespace="genre", value="manga"),
            contributor=CatalogContributorFilter(name="Reader", role="author"),
        )
        with connector.read_transaction():
            page = reader.discover_publications(connector, query=query, limit=1)
            language_facets = reader.list_publication_facets(
                connector,
                facet=CatalogFacetKind.LANGUAGE,
                query=query,
                limit=1,
            )
            presentation = reader.get_publication_presentation(
                connector,
                identity.publication_id(gid).decode("ascii"),
            )
            image = reader.get_publication_page(
                connector,
                identity.publication_id(gid).decode("ascii"),
                0,
            )

        assert len(page.publications) == 1
        publication = page.publications[0]
        assert publication.gid == gid
        assert publication.artifacts[0].storage_object.key == acquisition_key
        assert publication.cover == image
        assert publication.thumbnail is not None
        assert presentation is not None
        assert presentation.cover == image
        assert presentation.thumbnail == publication.thumbnail
        assert [
            (value.value, value.publication_count) for value in language_facets.values
        ] == [("en", 1)]
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
        page_count=0,
        cover=None,
        thumbnail=None,
        artifacts=(
            CatalogArtifact(
                artifact_id=identity.artifact_id(gid, artifact_sha256).decode("ascii"),
                name=f"artifact-{gid}.cbz",
                storage_object=StorageObjectDescriptor(
                    key=StorageObjectKey(
                        codec="mariadb-reader-test",
                        segments=(str(gid),),
                    ),
                    size_bytes=1,
                    sha256=artifact_sha256.hex(),
                    modified_at=datetime.fromtimestamp(7, UTC),
                ),
                media_type="application/vnd.comicbook+zip",
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
                "artifact_semantics_sha256, artifact_name, media_type, page_count) "
                "VALUES (%s, %s, %s, %s, %s, %s, 0)",
                (
                    1,
                    publication_key,
                    artifact_sha256,
                    artifact_semantics_sha256,
                    f"artifact-{gid}.cbz".encode("ascii"),
                    b"application/vnd.comicbook+zip",
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
            window = reader.list_recent_publications(
                connector,
                order=CatalogRecentOrder.DOWNLOADED,
                revision=1,
            )

        assert window.order is CatalogRecentOrder.DOWNLOADED
        assert window.publications == (publication,)
    finally:
        connector.execute("SET FOREIGN_KEY_CHECKS = 1")
        connector.close()
