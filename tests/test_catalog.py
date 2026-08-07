from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import pytest

from h2hdb import (
    H2HDB,
    CatalogArtifact,
    CatalogContributor,
    CatalogPublication,
    CatalogPublicationSelection,
    CatalogReader,
    CatalogRevision,
    CatalogRevisionNotFoundError,
    CatalogSnapshot,
    CatalogSubject,
    CoreConfig,
    DatabaseAccessMode,
    GallerySourceFile,
    GallerySourceRecord,
    GalleryTag,
    IngestTurnLostError,
    open_database,
)


def _publication(
    number: int,
    *,
    title: str,
    summary: str,
    contributor: str,
    subject: str,
    redownload_required: bool = False,
) -> CatalogPublication:
    timestamp = datetime(2025, number, number, 12, 0, tzinfo=UTC)
    publication_id = f"urn:h2h:gallery:{700_000 + number}"
    return CatalogPublication(
        publication_id=publication_id,
        gid=700_000 + number,
        title=title,
        source_title=title,
        sort_title=title.casefold(),
        summary=summary,
        language="und",
        published_at=timestamp,
        modified_at=timestamp,
        contributors=(
            CatalogContributor(
                name=contributor,
                role="uploader",
            ),
        ),
        subjects=(CatalogSubject(name=subject, scheme="h2h:tag:topic", code="topic"),),
        artifacts=(
            CatalogArtifact(
                artifact_id=f"urn:h2hdb:artifact:{number}",
                name=f"gallery-{number}.cbz",
                location=Path(f"/catalog/gallery-{number}.cbz"),
                media_type="application/vnd.comicbook+zip",
                size_bytes=number * 1_000,
                sha256=f"{number:02x}" * 32,
                modified_at=timestamp,
            ),
        ),
        redownload_required=redownload_required,
        source_gallery_name=f"gallery-{700_000 + number}",
        content_sha256=sha256(f"content:{publication_id}".encode()).hexdigest(),
    )


@pytest.fixture
def publications() -> tuple[CatalogPublication, ...]:
    return (
        _publication(
            1,
            title="Alpha Gallery",
            summary="First catalog entry",
            contributor="Alice",
            subject="fantasy",
        ),
        _publication(
            2,
            title="Beta Gallery",
            summary="Contains the cobalt keyword",
            contributor="Bob",
            subject="science fiction",
            redownload_required=True,
        ),
        _publication(
            3,
            title="Gamma Gallery",
            summary="Third catalog entry",
            contributor="Carol",
            subject="adventure",
        ),
    )


def _source_record(publication: CatalogPublication) -> GallerySourceRecord:
    token = publication.publication_id.rsplit(":", 1)[-1]
    file_sha256 = sha256(
        f"source-file:{publication.publication_id}".encode()
    ).hexdigest()
    manifest_sha256 = sha256(
        (
            f"{publication.publication_id}\0{publication.title}\0"
            f"{publication.summary}\0{file_sha256}"
        ).encode()
    ).hexdigest()
    content_sha256 = sha256(
        f"content:{publication.publication_id}".encode()
    ).hexdigest()
    return GallerySourceRecord(
        gallery_name=f"gallery-{token}",
        gid=publication.gid,
        title=publication.source_title,
        comment=publication.summary,
        upload_account=(
            publication.contributors[0].name if publication.contributors else ""
        ),
        upload_time=publication.published_at.replace(tzinfo=None),
        download_time=publication.modified_at.replace(tzinfo=None),
        modified_time=publication.modified_at.replace(tzinfo=None),
        tags=tuple(
            GalleryTag(subject.code or "tag", subject.name)
            for subject in publication.subjects
        ),
        files=(
            GallerySourceFile(
                name=f"page-{token}.jpg",
                size_bytes=100 + publication.gid,
                sha256=file_sha256,
            ),
        ),
        source_manifest_sha256=manifest_sha256,
        content_sha256=content_sha256,
    )


def _snapshot(
    publications: tuple[CatalogPublication, ...] | list[CatalogPublication],
) -> CatalogSnapshot:
    publication_tuple = tuple(publications)
    return CatalogSnapshot(
        galleries=tuple(
            _source_record(publication) for publication in publication_tuple
        ),
        selections=tuple(
            CatalogPublicationSelection(
                source_gallery_name=_source_record(publication).gallery_name,
                artifacts=publication.artifacts,
                redownload_required=publication.redownload_required,
            )
            for publication in publication_tuple
        ),
    )


def _publish_snapshot(
    database: H2HDB,
    publications: tuple[CatalogPublication, ...] | list[CatalogPublication],
) -> CatalogRevision:
    snapshot = _snapshot(publications)
    turn = database.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert turn is not None
    try:
        return database.publish_snapshot(
            snapshot,
            ingest_turn=turn,
        ).revision
    finally:
        assert database.complete_gallery_ingest(turn)


@pytest.fixture
def catalog(
    sqlite_config: CoreConfig,
    publications: tuple[CatalogPublication, ...],
) -> H2HDB:
    database = H2HDB(sqlite_config)
    database.migrate()
    revision = _publish_snapshot(database, publications)
    assert revision.revision == 1
    return database


def _read_only_config(config: CoreConfig) -> CoreConfig:
    return config.model_copy(
        update={
            "database": config.database.model_copy(
                update={"access_mode": DatabaseAccessMode.read_only}
            )
        }
    )


def test_publish_and_read_complete_revision(
    catalog: H2HDB,
    publications: tuple[CatalogPublication, ...],
) -> None:
    revision = catalog.get_catalog_revision()
    assert revision.revision == 1
    assert revision.publication_count == 3

    first_page = catalog.list_publications(offset=0, limit=2)
    assert first_page.revision == revision
    assert first_page.offset == 0
    assert first_page.limit == 2
    assert first_page.total == 3
    assert first_page.publications == publications[:2]

    second_page = catalog.list_publications(offset=2, limit=2)
    assert second_page.revision == revision
    assert second_page.publications == publications[2:]

    assert catalog.get_publication(publications[1].publication_id) == publications[1]
    assert catalog.get_publication("urn:h2hdb:publication:missing") is None
    assert catalog.get_artifact(publications[1].artifacts[0].artifact_id) == (
        publications[1].artifacts[0]
    )
    assert catalog.get_artifact("urn:h2hdb:artifact:missing") is None


@pytest.mark.parametrize(
    ("query", "expected_index"),
    [
        ("alpha", 0),
        ("cobalt", 1),
        ("bob", 1),
        ("science fiction", 1),
        ("gallery:700003", 2),
    ],
)
def test_search_uses_current_revision_metadata(
    catalog: H2HDB,
    publications: tuple[CatalogPublication, ...],
    query: str,
    expected_index: int,
) -> None:
    page = catalog.list_publications(query=query, limit=20)

    assert page.total == 1
    assert page.publications == (publications[expected_index],)


def test_artifact_name_lookup_is_batched_deduplicated_and_reports_absence(
    catalog: H2HDB,
    publications: tuple[CatalogPublication, ...],
) -> None:
    first_name = publications[0].artifacts[0].name
    second_name = publications[1].artifacts[0].name

    result = catalog.get_publications_by_artifact_names(
        [second_name, "missing.cbz", first_name, second_name]
    )

    assert result == {
        first_name: publications[0],
        second_name: publications[1],
    }


def test_artifact_name_lookup_can_pin_an_older_revision(
    catalog: H2HDB,
    publications: tuple[CatalogPublication, ...],
) -> None:
    pinned = catalog.get_catalog_revision()
    artifact_name = publications[0].artifacts[0].name
    _publish_snapshot(catalog, [publications[1]])

    assert catalog.get_publications_by_artifact_names([artifact_name]) == {}
    assert catalog.get_publications_by_artifact_names(
        [artifact_name],
        revision=pinned,
    ) == {artifact_name: publications[0]}


def test_artifact_filter_applies_to_total_and_offset_at_one_revision(
    catalog: H2HDB,
    publications: tuple[CatalogPublication, ...],
) -> None:
    artifactless = replace(publications[1], artifacts=())
    revision = _publish_snapshot(
        catalog,
        [publications[0], artifactless, publications[2]],
    )

    page = catalog.list_publications(
        offset=1,
        limit=1,
        revision=revision,
        require_artifact=True,
    )

    assert page.total == 2
    assert page.publications == (publications[2],)


@pytest.mark.parametrize("query", ["%", "_", "!", "\\"])
def test_search_treats_sql_wildcards_as_literal_text(
    catalog: H2HDB,
    query: str,
) -> None:
    page = catalog.list_publications(query=query, limit=20)

    assert page.total == 0
    assert page.publications == ()


def test_new_revision_replaces_the_published_snapshot(
    catalog: H2HDB,
    publications: tuple[CatalogPublication, ...],
) -> None:
    replacement = replace(
        publications[1],
        title="Beta Gallery Revised",
        source_title="Beta Gallery Revised",
        sort_title="beta gallery revised",
    )

    first_revision = catalog.get_catalog_revision()
    revision = _publish_snapshot(catalog, [replacement])

    assert revision.revision == 2
    assert revision.publication_count == 1
    assert catalog.get_catalog_revision(first_revision.revision) == first_revision
    assert catalog.get_catalog_revision(revision.revision) == revision
    page = catalog.list_publications(limit=20)
    assert page.revision == revision
    assert page.publications == (replacement,)
    assert catalog.get_publication(publications[0].publication_id) is None
    assert catalog.get_artifact(publications[0].artifacts[0].artifact_id) is None
    assert (
        catalog.get_publication(
            publications[0].publication_id,
            revision=first_revision,
        )
        == publications[0]
    )
    assert (
        catalog.get_artifact(
            publications[0].artifacts[0].artifact_id,
            revision=first_revision,
        )
        == publications[0].artifacts[0]
    )


def test_revision_publish_atomically_enqueues_removed_canonical_galleries(
    catalog: H2HDB,
    publications: tuple[CatalogPublication, ...],
) -> None:
    revision = _publish_snapshot(catalog, [publications[0]])

    assert revision.revision == 2
    assert catalog.get_download_request(publications[1].gid) is not None
    assert catalog.get_download_request(publications[2].gid) is not None


def test_invalid_snapshot_is_rejected_without_advancing_revision(
    catalog: H2HDB,
    publications: tuple[CatalogPublication, ...],
) -> None:
    original_revision = catalog.get_catalog_revision()
    duplicate_gid = replace(publications[1], gid=publications[0].gid)

    with pytest.raises(ValueError, match="duplicate GID"):
        _publish_snapshot(catalog, [publications[0], duplicate_gid])

    assert catalog.get_catalog_revision() == original_revision
    assert catalog.list_publications(limit=20).publications == publications


def test_catalog_revision_history_reports_absent_revision(catalog: H2HDB) -> None:
    with pytest.raises(CatalogRevisionNotFoundError) as captured:
        catalog.get_catalog_revision(999)

    assert captured.value.revision == 999

    with pytest.raises(ValueError, match="negative"):
        catalog.get_catalog_revision(-1)


def test_stale_ingest_turn_fences_revision_and_queue_atomically(
    catalog: H2HDB,
    publications: tuple[CatalogPublication, ...],
) -> None:
    original_revision = catalog.get_catalog_revision()
    original_publications = catalog.list_publications(limit=20).publications
    stale_turn = catalog.claim_gallery_ingest(
        lease_seconds=60,
        periodic_scan=True,
    )
    assert stale_turn is not None
    assert catalog.complete_gallery_ingest(stale_turn)

    with pytest.raises(IngestTurnLostError, match="no longer live"):
        catalog.publish_snapshot(
            _snapshot([replace(publications[0], title="Must Not Publish")]),
            ingest_turn=stale_turn,
        )

    assert catalog.get_catalog_revision() == original_revision
    assert catalog.list_publications(limit=20).publications == original_publications
    assert catalog.get_download_request(publications[1].gid) is None
    assert catalog.get_download_request(publications[2].gid) is None
    with pytest.raises(CatalogRevisionNotFoundError):
        catalog.get_catalog_revision(original_revision.revision + 1)


def test_read_only_catalog_reader_can_read_but_cannot_publish(
    sqlite_config: CoreConfig,
    catalog: H2HDB,
    publications: tuple[CatalogPublication, ...],
) -> None:
    reader = open_database(_read_only_config(sqlite_config))

    assert isinstance(reader, CatalogReader)
    assert reader.list_publications(limit=20).publications == publications
    turn = catalog.claim_gallery_ingest(lease_seconds=60, periodic_scan=True)
    assert turn is not None
    try:
        with pytest.raises(PermissionError, match="read-only"):
            reader.publish_snapshot(_snapshot([publications[0]]), ingest_turn=turn)
    finally:
        assert catalog.complete_gallery_ingest(turn)


@pytest.mark.parametrize(
    ("offset", "limit", "message"),
    [(-1, 10, "offset"), (0, 0, "limit"), (0, 201, "limit")],
)
def test_catalog_pagination_rejects_invalid_ranges(
    catalog: H2HDB,
    offset: int,
    limit: int,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        catalog.list_publications(offset=offset, limit=limit)
