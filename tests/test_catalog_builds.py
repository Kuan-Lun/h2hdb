from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256

import pytest

from h2hdb import (
    H2HDB,
    CatalogBuildBatchConflictError,
    CatalogBuildCoordinator,
    CatalogBuildPhase,
    CatalogBuildPublishResult,
    CatalogBuildStateError,
    CatalogSourceDiscoveryCompletion,
    CatalogSourceFileChunk,
    CatalogSourceGalleryAnalysis,
    CatalogSourceGalleryCompletion,
    CatalogSourceGalleryDiscovery,
    CatalogSourceGalleryHeader,
    CoreConfig,
    FileHashCacheEntry,
    FileHashCacheKey,
    GalleryIngestTurn,
    GallerySourceFile,
    GalleryTag,
    IngestTurnLostError,
)


def _digest(value: str) -> str:
    return sha256(value.encode()).hexdigest()


def _header(name: str, gid: int) -> CatalogSourceGalleryHeader:
    timestamp = datetime(2026, 8, 9, 1, 2, 3, tzinfo=UTC)
    return CatalogSourceGalleryHeader(
        gallery_name=name,
        gid=gid,
        title=f"Title {name}",
        comment="comment",
        upload_account="uploader",
        upload_time=timestamp,
        download_time=timestamp,
        modified_time=timestamp,
        tags=(GalleryTag("language", "english"),),
    )


def _claim(database: H2HDB) -> GalleryIngestTurn:
    turn = database.claim_gallery_ingest(
        lease_seconds=120,
        periodic_scan=False,
    )
    assert turn is not None
    return turn


def _completion(
    gallery_name: str,
    *,
    expected_file_count: int = 1,
) -> CatalogSourceGalleryCompletion:
    return CatalogSourceGalleryCompletion(
        gallery_name=gallery_name,
        expected_file_count=expected_file_count,
        scan_observation_sha256=_digest(f"scan:{gallery_name}"),
        scan_observation_version=2,
        metadata_sha256=_digest(f"metadata:{gallery_name}"),
        page_count=expected_file_count,
        directory_entry_count=expected_file_count + 1,
        directory_observation_sha256=_digest(f"directory:{gallery_name}"),
    )


def test_catalog_build_is_turn_fenced_scope_bound_and_resumable(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = database.begin_catalog_build(
        scope_key="root=/library;policy=v1",
        ingest_turn=turn,
    )

    assert isinstance(database, CatalogBuildCoordinator)
    assert build.build_id != str(turn.generation)
    assert len(build.build_id) == 32
    wrong_turn = replace(turn, owner_token="wrong-token")
    with pytest.raises(IngestTurnLostError):
        database.discover_catalog_galleries(
            build,
            ["gallery-a"],
            batch_id="discovery-wrong-token",
            ingest_turn=wrong_turn,
        )

    restarted = H2HDB(sqlite_config)
    resumed = restarted.resume_catalog_build(
        scope_key="root=/library;policy=v1",
        ingest_turn=turn,
    )
    assert resumed is not None
    assert resumed.build_id == build.build_id
    with pytest.raises(CatalogBuildStateError, match="different source scope"):
        restarted.resume_catalog_build(
            scope_key="root=/different;policy=v1",
            ingest_turn=turn,
        )
    working = restarted.get_working_catalog_build()
    assert working is not None
    assert working.scope_key == "root=/library;policy=v1"
    resumed_for_abandon = restarted.resume_catalog_build(
        scope_key=working.scope_key,
        ingest_turn=turn,
    )
    assert resumed_for_abandon is not None
    assert (
        restarted.abandon_catalog_build(
            resumed_for_abandon,
            ingest_turn=turn,
        ).phase
        is CatalogBuildPhase.abandoned
    )


def test_discovery_resume_tolerates_changed_batch_boundaries(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = database.begin_catalog_build(scope_key="scope", ingest_turn=turn)

    with pytest.raises(CatalogBuildStateError, match="duplicate gallery names"):
        database.discover_catalog_galleries(
            build,
            ["gallery-a", "gallery-a"],
            batch_id="duplicate-within-batch",
            ingest_turn=turn,
        )

    first = database.discover_catalog_galleries(
        build,
        ["gallery-a", "gallery-b"],
        batch_id="first-boundary",
        ingest_turn=turn,
    )
    overlap = database.discover_catalog_galleries(
        build,
        ["gallery-b", "gallery-c"],
        batch_id="changed-boundary",
        ingest_turn=turn,
    )
    replay = database.discover_catalog_galleries(
        build,
        ["gallery-b", "gallery-c"],
        batch_id="changed-boundary",
        ingest_turn=turn,
    )

    assert first.item_count == 2
    assert overlap.item_count == 1
    assert replay.applied is False
    with pytest.raises(CatalogBuildBatchConflictError, match="different source"):
        database.discover_catalog_sources(
            build,
            [CatalogSourceGalleryDiscovery("gallery-b", "nested/gallery-b")],
            batch_id="conflicting-locator",
            ingest_turn=turn,
        )
    completed = database.complete_catalog_discovery(build, ingest_turn=turn)
    assert completed.expected_gallery_count == 3


def test_changed_partial_gallery_can_abandon_build_without_losing_hash_cache(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = database.begin_catalog_build(scope_key="scope", ingest_turn=turn)
    database.discover_catalog_galleries(
        build,
        ["gallery-a"],
        batch_id="discover",
        ingest_turn=turn,
    )
    build = database.complete_catalog_discovery(build, ingest_turn=turn)
    database.begin_catalog_gallery(
        build,
        _header("gallery-a", 900_002),
        batch_id="header",
        ingest_turn=turn,
    )
    old_file = GallerySourceFile("old.jpg", 1, _digest("old"))
    database.stage_catalog_file_chunk(
        build,
        "gallery-a",
        [old_file],
        batch_id="files",
        ingest_turn=turn,
    )
    cache_key = FileHashCacheKey("scope/gallery-a/old.jpg", "stat-v1")
    database.cache_catalog_file_hashes(
        build,
        [FileHashCacheEntry(cache_key, old_file.sha256)],
        batch_id="cache",
        ingest_turn=turn,
    )

    abandoned = database.abandon_catalog_build(build, ingest_turn=turn)
    assert abandoned.phase is CatalogBuildPhase.abandoned
    replacement = database.begin_catalog_build(
        scope_key="scope",
        ingest_turn=turn,
    )
    assert replacement.build_id != build.build_id
    assert database.get_catalog_file_hashes([cache_key])[cache_key] == old_file.sha256


def test_multi_gallery_stage_batches_are_atomic_and_idempotent(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = database.begin_catalog_build(scope_key="scope", ingest_turn=turn)
    database.discover_catalog_galleries(
        build,
        ["gallery-a", "gallery-b"],
        batch_id="discover",
        ingest_turn=turn,
    )
    build = database.complete_catalog_discovery(build, ingest_turn=turn)

    with pytest.raises(CatalogBuildStateError, match="absent from discovery"):
        database.stage_catalog_gallery_headers(
            build,
            [_header("gallery-a", 1), _header("not-discovered", 2)],
            batch_id="headers-invalid",
            ingest_turn=turn,
        )
    assert all(
        not progress.header_staged
        for progress in database.list_pending_catalog_galleries(
            build.build_id
        ).galleries
    )

    headers = (_header("gallery-a", 1), _header("gallery-b", 2))
    assert (
        database.stage_catalog_gallery_headers(
            build,
            headers,
            batch_id="headers",
            ingest_turn=turn,
        ).item_count
        == 2
    )
    assert (
        database.stage_catalog_gallery_headers(
            build,
            headers,
            batch_id="headers",
            ingest_turn=turn,
        ).applied
        is False
    )

    files = {
        "gallery-a": GallerySourceFile("a.jpg", 1, _digest("a")),
        "gallery-b": GallerySourceFile("b.jpg", 1, _digest("b")),
    }
    with pytest.raises(CatalogBuildStateError, match="header that is not staged"):
        database.stage_catalog_file_chunks(
            build,
            [
                CatalogSourceFileChunk("gallery-a", (files["gallery-a"],)),
                CatalogSourceFileChunk(
                    "not-staged",
                    (GallerySourceFile("x.jpg", 1, _digest("x")),),
                ),
            ],
            batch_id="files-invalid",
            ingest_turn=turn,
        )
    assert all(
        progress.staged_file_count == 0
        for progress in database.list_pending_catalog_galleries(
            build.build_id
        ).galleries
    )

    chunks = tuple(
        CatalogSourceFileChunk(name, (source_file,))
        for name, source_file in files.items()
    )
    assert (
        database.stage_catalog_file_chunks(
            build,
            chunks,
            batch_id="files",
            ingest_turn=turn,
        ).file_count
        == 2
    )

    with pytest.raises(CatalogBuildStateError, match="expected unique count"):
        database.complete_catalog_galleries(
            build,
            [_completion("gallery-a"), _completion("gallery-b", expected_file_count=2)],
            batch_id="complete-invalid",
            ingest_turn=turn,
        )
    assert len(database.list_pending_catalog_galleries(build.build_id).galleries) == 2

    completions = (_completion("gallery-a"), _completion("gallery-b"))
    completed = database.complete_catalog_galleries(
        build,
        completions,
        batch_id="complete",
        ingest_turn=turn,
    )
    assert (completed.item_count, completed.file_count) == (2, 2)
    assert (
        database.complete_catalog_galleries(
            build,
            completions,
            batch_id="complete",
            ingest_turn=turn,
        ).applied
        is False
    )

    build = database.complete_catalog_source_staging(build, ingest_turn=turn)
    database.stage_catalog_analysis(
        build,
        [
            CatalogSourceGalleryAnalysis(
                gallery_name="gallery-a",
                content_sha256=None,
                selected=True,
                source_manifest_sha256=_digest("manifest-a"),
                source_manifest_version=1,
            ),
            CatalogSourceGalleryAnalysis(
                gallery_name="gallery-b",
                content_sha256=_digest("content-b"),
                selected=False,
                duplicate_of_gallery_name="gallery-a",
                source_manifest_sha256=_digest("manifest-b"),
                source_manifest_version=1,
            ),
        ],
        batch_id="invalid-null-owner",
        ingest_turn=turn,
    )
    with pytest.raises(CatalogBuildStateError, match="invalid duplicate owner"):
        database.complete_catalog_analysis(build, ingest_turn=turn)


def test_chunked_build_cache_partial_visibility_and_idempotent_publish(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = database.begin_catalog_build(scope_key="scope-v1", ingest_turn=turn)
    database.discover_catalog_sources(
        build,
        [
            CatalogSourceGalleryDiscovery(
                "gallery-a",
                "nested/gallery-a",
                "dev=1;ino=2;size=3;mtime=4;ctime=5",
            )
        ],
        batch_id="discover-0",
        ingest_turn=turn,
    )
    with pytest.raises(CatalogBuildBatchConflictError):
        database.discover_catalog_galleries(
            build,
            ["different"],
            batch_id="discover-0",
            ingest_turn=turn,
        )
    build = database.complete_catalog_discovery(
        build,
        ingest_turn=turn,
        completion=CatalogSourceDiscoveryCompletion(
            scan_attempt=build.discovery_epoch or "",
            gallery_count=1,
            tree_observation_sha256=_digest("tree"),
        ),
    )
    assert build.phase is CatalogBuildPhase.staging
    assert build.discovery_tree_sha256 == _digest("tree")
    assert database.complete_catalog_discovery(
        build,
        ingest_turn=turn,
        completion=CatalogSourceDiscoveryCompletion(
            scan_attempt=build.discovery_epoch or "",
            gallery_count=1,
            tree_observation_sha256=_digest("tree"),
        ),
    ).discovery_tree_sha256 == _digest("tree")
    with pytest.raises(CatalogBuildStateError, match="different data"):
        database.complete_catalog_discovery(
            build,
            ingest_turn=turn,
            completion=CatalogSourceDiscoveryCompletion(
                scan_attempt=build.discovery_epoch or "",
                gallery_count=1,
                tree_observation_sha256=_digest("changed-tree"),
            ),
        )

    header = _header("gallery-a", 900_001)
    database.begin_catalog_gallery(
        build,
        header,
        batch_id="header-0",
        ingest_turn=turn,
    )
    # A restart can replay a header under a newly chosen batch boundary.
    duplicate_header = database.begin_catalog_gallery(
        build,
        header,
        batch_id="header-after-restart",
        ingest_turn=turn,
    )
    assert duplicate_header.item_count == 0

    files = (
        GallerySourceFile(
            "001.jpg",
            10,
            _digest("file-1"),
            "nested/gallery-a/001.jpg",
            2**63 + 5,
            2**63 + 7,
            1_800_000_000_000_000_000,
            1_800_000_000_000_000_001,
        ),
        GallerySourceFile(
            "002.jpg",
            20,
            _digest("file-2"),
            "nested/gallery-a/002.jpg",
            2**63 + 5,
            2**63 + 8,
            1_800_000_000_000_000_002,
            1_800_000_000_000_000_003,
        ),
    )
    first_chunk = database.stage_catalog_file_chunk(
        build,
        "gallery-a",
        files[:1],
        batch_id="files-0",
        ingest_turn=turn,
    )
    replay = database.stage_catalog_file_chunk(
        build,
        "gallery-a",
        files[:1],
        batch_id="files-0",
        ingest_turn=turn,
    )
    assert first_chunk.applied is True
    assert replay.applied is False
    pending = database.list_pending_catalog_galleries(build.build_id)
    assert pending.galleries[0].staged_file_count == 1
    assert pending.galleries[0].source_locator == "nested/gallery-a"
    database.stage_catalog_file_chunk(
        build,
        "gallery-a",
        files[::-1],
        batch_id="files-1",
        ingest_turn=turn,
    )
    first_file_page = database.list_catalog_build_files(
        build.build_id,
        "gallery-a",
        limit=1,
    )
    assert first_file_page.files == files[:1]
    assert first_file_page.has_more is True
    assert first_file_page.next_cursor is not None
    second_file_page = database.list_catalog_build_files(
        build.build_id,
        "gallery-a",
        after=first_file_page.next_cursor,
        limit=1,
    )
    assert second_file_page.files == files[1:]
    assert second_file_page.has_more is False

    cache_key = FileHashCacheKey(
        source_key="scope-v1/gallery-a/001.jpg",
        fingerprint="dev=1;ino=2;size=10;mtime=3;ctime=4",
    )
    cache_entry = FileHashCacheEntry(cache_key, files[0].sha256)
    database.cache_catalog_file_hashes(
        build,
        [cache_entry],
        batch_id="cache-0",
        ingest_turn=turn,
    )
    assert H2HDB(sqlite_config).get_catalog_file_hashes([cache_key]) == {
        cache_key: files[0].sha256
    }

    database.complete_catalog_gallery(
        build,
        CatalogSourceGalleryCompletion(
            gallery_name="gallery-a",
            expected_file_count=2,
            scan_observation_sha256=_digest("scan-observation"),
            scan_observation_version=2,
            raw_content_sha256=_digest("raw-content"),
            metadata_sha256=_digest("metadata"),
            page_count=2,
            directory_entry_count=3,
            directory_observation_sha256=_digest("directory"),
        ),
        batch_id="complete-0",
        ingest_turn=turn,
    )
    # Explicit build inspection can see completed work, but active readers
    # remain pinned to revision zero until the source pointer is swapped.
    assert database.list_catalog_build_sources(build.build_id).total == 1
    assert database.list_catalog_sources().total == 0
    assert database.get_active_catalog_build() is None

    build = database.complete_catalog_source_staging(build, ingest_turn=turn)
    analysis = CatalogSourceGalleryAnalysis(
        gallery_name="gallery-a",
        content_sha256=_digest("effective-content"),
        selected=True,
        source_manifest_sha256=_digest("canonical-manifest"),
        source_manifest_version=1,
    )
    database.stage_catalog_analysis(
        build,
        [analysis],
        batch_id="analysis-0",
        ingest_turn=turn,
    )
    assert (
        database.stage_catalog_analysis(
            build,
            [analysis],
            batch_id="analysis-after-restart",
            ingest_turn=turn,
        ).item_count
        == 0
    )
    build = database.complete_catalog_analysis(build, ingest_turn=turn)
    assert build.phase is CatalogBuildPhase.artifacts
    build = database.seal_catalog_build(build, ingest_turn=turn)
    assert build.phase is CatalogBuildPhase.sealed
    assert build.seal_sha256 is not None

    published = database.publish_catalog_build(build, ingest_turn=turn)
    retried = database.publish_catalog_build(
        published.build,
        ingest_turn=turn,
    )
    assert published.source_revision == 1
    assert retried == published
    active = database.list_catalog_sources()
    assert active.revision.revision == 1
    assert active.total == 1
    assert active.galleries[0].source_file_count == 2
    assert active.galleries[0].source_locator == "nested/gallery-a"
    assert active.galleries[0].metadata_fingerprint == (
        "dev=1;ino=2;size=3;mtime=4;ctime=5"
    )
    assert active.galleries[0].metadata_sha256 == _digest("metadata")
    assert active.galleries[0].source_manifest_version == 1
    assert active.galleries[0].scan_observation_sha256 == _digest("scan-observation")
    assert active.galleries[0].scan_observation_version == 2
    assert active.galleries[0].directory_entry_count == 3
    assert active.galleries[0].directory_observation_sha256 == _digest("directory")
    assert not hasattr(active.galleries[0], "files")
    assert active.galleries[0].content_sha256 == _digest("effective-content")


def test_inactive_source_build_pruning_is_bounded_and_protects_active(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()

    def publish_empty(
        scope_key: str,
        *,
        periodic_scan: bool,
    ) -> CatalogBuildPublishResult:
        turn = database.claim_gallery_ingest(
            lease_seconds=120,
            periodic_scan=periodic_scan,
        )
        assert turn is not None
        build = database.begin_catalog_build(
            scope_key=scope_key,
            ingest_turn=turn,
        )
        build = database.complete_catalog_discovery(build, ingest_turn=turn)
        build = database.complete_catalog_source_staging(build, ingest_turn=turn)
        build = database.complete_catalog_analysis(build, ingest_turn=turn)
        build = database.seal_catalog_build(build, ingest_turn=turn)
        result = database.publish_catalog_build(build, ingest_turn=turn)
        assert database.complete_gallery_ingest(turn)
        return result

    first = publish_empty("scope-1", periodic_scan=False)
    second = publish_empty("scope-2", periodic_scan=True)
    assert first.build.build_id != second.build.build_id

    with pytest.raises(CatalogBuildStateError, match="active or working"):
        database.prune_catalog_build(second.build.build_id, max_rows=1)
    partial = database.prune_catalog_build(first.build.build_id, max_rows=1)
    assert partial.deleted_rows <= 1
    assert partial.complete is False
    for _ in range(10):
        completed = database.prune_catalog_build(first.build.build_id, max_rows=1)
        assert completed.deleted_rows <= 1
        if completed.complete:
            break
    else:
        pytest.fail("Inactive source build did not finish bounded cleanup")
    with pytest.raises(LookupError):
        database.get_catalog_source_revision(first.source_revision)
    assert database.get_catalog_source_revision().revision == second.source_revision
