import sqlite3
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256

import pytest

from h2hdb import (
    H2HDB,
    CatalogAnalysisPhase,
    CatalogAnalysisScanCompletion,
    CatalogBuild,
    CatalogBuildAnalyzer,
    CatalogBuildBatchConflictError,
    CatalogBuildPhase,
    CatalogBuildStateError,
    CatalogContentDigest,
    CatalogContentOwner,
    CatalogFinalAnalysisCursor,
    CatalogGalleryFileHashCursor,
    CatalogGalleryFileHashRow,
    CatalogGidWinner,
    CatalogPublicationSelection,
    CatalogSnapshot,
    CatalogSourceGalleryCompletion,
    CatalogSourceGalleryHeader,
    CatalogSourceManifest,
    CatalogSourceManifestCursor,
    CatalogSourceManifestRow,
    CoreConfig,
    DatabaseAccessMode,
    GalleryIngestTurn,
    GallerySourceFile,
    GallerySourceRecord,
    GalleryTag,
    IngestTurnLostError,
)


def _digest(value: str) -> str:
    return sha256(value.encode()).hexdigest()


def _claim(database: H2HDB, *, periodic: bool = False) -> GalleryIngestTurn:
    turn = database.claim_gallery_ingest(
        lease_seconds=120,
        periodic_scan=periodic,
    )
    assert turn is not None
    return turn


def _file_spam_completion(
    database: H2HDB,
    build: CatalogBuild,
) -> CatalogAnalysisScanCompletion:
    after = None
    while True:
        page = database.list_catalog_file_hash_aggregates(
            build.build_id,
            after=after,
            limit=2,
        )
        if not page.items:
            assert page.completion is not None
            return page.completion
        after = page.items[-1].cursor


def _header(
    name: str,
    gid: int,
    *,
    artist: str | None,
    extra_tags: tuple[GalleryTag, ...] = (),
) -> CatalogSourceGalleryHeader:
    timestamp = datetime(2026, 8, 9, 1, 2, 3, tzinfo=UTC)
    tags = (() if artist is None else (GalleryTag("artist", artist),)) + extra_tags
    return CatalogSourceGalleryHeader(
        gallery_name=name,
        gid=gid,
        title=f"Title {name}",
        comment="comment",
        upload_account="account",
        upload_time=timestamp,
        download_time=timestamp,
        modified_time=timestamp,
        tags=tags,
    )


def _stage_source_build(database: H2HDB, turn: GalleryIngestTurn) -> CatalogBuild:
    names = ("gallery-a", "gallery-b", "gallery-c", "gallery-d")
    build = database.begin_catalog_build(scope_key="analysis-scope", ingest_turn=turn)
    database.discover_catalog_galleries(
        build,
        names,
        batch_id="discover",
        ingest_turn=turn,
    )
    build = database.complete_catalog_discovery(build, ingest_turn=turn)
    database.stage_catalog_gallery_headers(
        build,
        (
            _header(
                "gallery-a",
                10,
                artist="alice",
                extra_tags=(GalleryTag("status", "Already Uploaded"),),
            ),
            _header("gallery-b", 10, artist="bob"),
            _header("gallery-c", 20, artist=None),
            _header("gallery-d", 20, artist="charlie"),
        ),
        batch_id="headers",
        ingest_turn=turn,
    )
    common = _digest("cross-artist-spam")
    same_content = _digest("same-content")
    files = {
        "gallery-a": (
            GallerySourceFile("galleryinfo.txt", 1, _digest("metadata-a")),
            GallerySourceFile("001.jpg", 2, common),
            GallerySourceFile("002.jpg", 3, same_content),
        ),
        "gallery-b": (
            GallerySourceFile("galleryinfo.txt", 1, _digest("metadata-b")),
            GallerySourceFile("001.jpg", 2, common),
            GallerySourceFile("002.jpg", 3, same_content),
        ),
        "gallery-d": (
            GallerySourceFile("galleryinfo.txt", 1, _digest("metadata-d")),
            GallerySourceFile("001.jpg", 2, common),
        ),
    }
    for name, source_files in files.items():
        database.stage_catalog_file_chunk(
            build,
            name,
            source_files,
            batch_id=f"files:{name}",
            ingest_turn=turn,
        )
    database.complete_catalog_galleries(
        build,
        tuple(
            CatalogSourceGalleryCompletion(
                gallery_name=name,
                expected_file_count=len(files.get(name, ())),
                scan_observation_sha256=_digest(f"scan:{name}"),
                scan_observation_version=2,
                metadata_sha256=_digest(f"metadata:{name}"),
            )
            for name in names
        ),
        batch_id="completions",
        ingest_turn=turn,
    )
    return database.complete_catalog_source_staging(build, ingest_turn=turn)


def _stage_single_to_content(
    database: H2HDB,
    turn: GalleryIngestTurn,
    *,
    name: str,
    gid: int,
    content_sha256: str,
) -> CatalogBuild:
    build = database.begin_catalog_build(
        scope_key=f"single:{name}",
        ingest_turn=turn,
    )
    database.discover_catalog_galleries(
        build,
        (name,),
        batch_id="discover",
        ingest_turn=turn,
    )
    build = database.complete_catalog_discovery(build, ingest_turn=turn)
    database.begin_catalog_gallery(
        build,
        _header(name, gid, artist="artist"),
        batch_id="header",
        ingest_turn=turn,
    )
    database.stage_catalog_file_chunk(
        build,
        name,
        (
            GallerySourceFile("galleryinfo.txt", 1, _digest(f"metadata:{name}")),
            GallerySourceFile("001.jpg", 1, _digest(f"file:{name}")),
        ),
        batch_id="files",
        ingest_turn=turn,
    )
    database.complete_catalog_gallery(
        build,
        CatalogSourceGalleryCompletion(
            gallery_name=name,
            expected_file_count=2,
            scan_observation_sha256=_digest(f"scan:{name}"),
            scan_observation_version=2,
            metadata_sha256=_digest(f"metadata:{name}"),
        ),
        batch_id="completion",
        ingest_turn=turn,
    )
    build = database.complete_catalog_source_staging(build, ingest_turn=turn)
    database.stage_catalog_source_manifests(
        build,
        (CatalogSourceManifest(name, _digest(f"manifest:{name}")),),
        batch_id="manifest",
        ingest_turn=turn,
    )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.source_manifests,
        ingest_turn=turn,
    )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.file_spam,
        ingest_turn=turn,
        scan_completion=_file_spam_completion(database, build),
    )
    database.stage_catalog_content_digests(
        build,
        (CatalogContentDigest(name, content_sha256),),
        batch_id="content",
        ingest_turn=turn,
    )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.content_digests,
        ingest_turn=turn,
    )
    return build


def test_file_spam_requires_terminal_proof_and_preserves_legacy_metadata_membership(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    names = ("gallery", "gallery ")
    build = database.begin_catalog_build(scope_key="metadata-parity", ingest_turn=turn)
    database.discover_catalog_galleries(
        build,
        names,
        batch_id="discover",
        ingest_turn=turn,
    )
    build = database.complete_catalog_discovery(build, ingest_turn=turn)
    database.stage_catalog_gallery_headers(
        build,
        (
            _header(names[0], 501, artist="artist"),
            _header(names[1], 502, artist="artist "),
        ),
        batch_id="headers",
        ingest_turn=turn,
    )
    shared = _digest("content-and-other-gallery-metadata")
    files = {
        names[0]: (
            GallerySourceFile("galleryinfo.txt", 1, _digest("metadata:first")),
            GallerySourceFile("001.jpg", 1, shared),
        ),
        names[1]: (
            GallerySourceFile("galleryinfo.txt", 1, shared),
            GallerySourceFile("001.jpg", 1, _digest("other-content")),
        ),
    }
    for name in names:
        database.stage_catalog_file_chunk(
            build,
            name,
            files[name],
            batch_id=f"files:{name}",
            ingest_turn=turn,
        )
    database.complete_catalog_galleries(
        build,
        tuple(
            CatalogSourceGalleryCompletion(
                gallery_name=name,
                expected_file_count=2,
                scan_observation_sha256=_digest(f"scan:{name}"),
                scan_observation_version=2,
                metadata_sha256=_digest(f"metadata:{name}"),
            )
            for name in names
        ),
        batch_id="completions",
        ingest_turn=turn,
    )
    build = database.complete_catalog_source_staging(build, ingest_turn=turn)
    manifest_rows: list[CatalogSourceManifestRow] = []
    manifest_after = None
    while True:
        page = database.list_catalog_source_manifest_rows(
            build.build_id,
            after=manifest_after,
            limit=1,
        )
        if not page.items:
            break
        manifest_rows.extend(page.items)
        manifest_after = page.items[-1].cursor
    assert {row.gallery_name for row in manifest_rows} == set(names)
    assert len({row.gallery_key for row in manifest_rows}) == 2
    database.stage_catalog_source_manifests(
        build,
        tuple(
            CatalogSourceManifest(name, _digest(f"manifest:{name}")) for name in names
        ),
        batch_id="manifests",
        ingest_turn=turn,
    )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.source_manifests,
        ingest_turn=turn,
    )
    aggregates = database.list_catalog_file_hash_aggregates(
        build.build_id,
        limit=10,
    ).items
    shared_aggregate = next(item for item in aggregates if item.file_sha256 == shared)
    # galleryinfo is excluded from occurrence thresholds, but a matching metadata
    # file still contributes its gallery's exact artist membership.
    assert shared_aggregate.occurrence_count == 1
    assert shared_aggregate.distinct_artist_count == 2
    assert shared_aggregate.maximum_gallery_artist_count == 1
    with pytest.raises(CatalogBuildStateError, match="explicit terminal"):
        database.complete_catalog_analysis_phase(
            build,
            CatalogAnalysisPhase.file_spam,
            ingest_turn=turn,
        )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.file_spam,
        ingest_turn=turn,
        scan_completion=_file_spam_completion(database, build),
    )


def test_durable_analysis_is_bounded_exact_and_retryable(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = _stage_source_build(database, turn)
    assert isinstance(database, CatalogBuildAnalyzer)

    with pytest.raises(CatalogBuildStateError, match="complete in order"):
        database.complete_catalog_analysis_phase(
            build,
            CatalogAnalysisPhase.file_spam,
            ingest_turn=turn,
        )

    manifest_rows: list[CatalogSourceManifestRow] = []
    manifest_after: CatalogSourceManifestCursor | None = None
    while True:
        page = database.list_catalog_source_manifest_rows(
            build.build_id,
            after=manifest_after,
            limit=2,
        )
        if not page.items:
            break
        manifest_rows.extend(page.items)
        manifest_after = page.items[-1].cursor
    assert len(manifest_rows) == 9
    empty_rows = [row for row in manifest_rows if row.file_name is None]
    assert len(empty_rows) == 1
    assert empty_rows[0].gallery_name == "gallery-c"
    assert empty_rows[0].empty_gallery_metadata_sha256 == _digest("metadata:gallery-c")

    manifests = tuple(
        CatalogSourceManifest(name, _digest(f"manifest:{name}"))
        for name in ("gallery-a", "gallery-b", "gallery-c", "gallery-d")
    )
    first = database.stage_catalog_source_manifests(
        build,
        manifests,
        batch_id="manifest-batch",
        ingest_turn=turn,
    )
    assert first.item_count == 4
    assert (
        database.stage_catalog_source_manifests(
            build,
            manifests,
            batch_id="manifest-batch",
            ingest_turn=turn,
        ).applied
        is False
    )
    with pytest.raises(CatalogBuildBatchConflictError, match="different data"):
        database.stage_catalog_source_manifests(
            build,
            (CatalogSourceManifest("gallery-a", _digest("changed")),),
            batch_id="manifest-batch",
            ingest_turn=turn,
        )
    with pytest.raises(IngestTurnLostError):
        database.stage_catalog_source_manifests(
            build,
            (),
            batch_id="wrong-turn",
            ingest_turn=replace(turn, owner_token="wrong"),
        )
    assert database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.source_manifests,
        ingest_turn=turn,
    ).applied
    assert not database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.source_manifests,
        ingest_turn=turn,
    ).applied

    aggregates = database.list_catalog_file_hash_aggregates(
        build.build_id,
        limit=100,
    ).items
    with pytest.raises(ValueError, match="between 1 and 1000"):
        database.list_catalog_file_hash_aggregates(build.build_id, limit=1001)
    by_hash = {item.file_sha256: item for item in aggregates}
    common = _digest("cross-artist-spam")
    assert by_hash[common].occurrence_count == 3
    assert by_hash[common].distinct_artist_count == 3
    assert by_hash[common].maximum_gallery_artist_count == 1
    assert _digest("metadata-a") not in by_hash
    with pytest.raises(ValueError, match="at most 1000"):
        database.stage_catalog_excluded_file_hashes(
            build,
            tuple(_digest(f"too-large:{index}") for index in range(1001)),
            batch_id="too-large",
            ingest_turn=turn,
        )
    database.stage_catalog_excluded_file_hashes(
        build,
        (common,),
        batch_id="excluded",
        ingest_turn=turn,
    )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.file_spam,
        ingest_turn=turn,
        scan_completion=database.list_catalog_file_hash_aggregates(
            build.build_id,
            after=aggregates[-1].cursor,
            limit=100,
        ).completion,
    )

    hash_rows: list[CatalogGalleryFileHashRow] = []
    hash_after: CatalogGalleryFileHashCursor | None = None
    while True:
        hash_page = database.list_catalog_gallery_file_hashes(
            build.build_id,
            after=hash_after,
            limit=2,
        )
        if not hash_page.items:
            break
        hash_rows.extend(hash_page.items)
        hash_after = hash_page.items[-1].cursor
    assert len(hash_rows) == 9
    assert sum(row.excluded_as_spam for row in hash_rows) == 3
    assert any(
        row.gallery_name == "gallery-c" and row.file_name is None for row in hash_rows
    )

    content = _digest("effective-content")
    database.stage_catalog_content_digests(
        build,
        (
            CatalogContentDigest(
                "gallery-a",
                content,
                duplicate_hash_deletion_candidate=True,
            ),
            CatalogContentDigest("gallery-b", content),
            CatalogContentDigest("gallery-c", None),
            CatalogContentDigest("gallery-d", None),
        ),
        batch_id="content",
        ingest_turn=turn,
    )
    with sqlite3.connect(sqlite_config.database.database) as connection:
        assert (
            connection.execute(
                """
            SELECT duplicate_hash_deletion_candidate
            FROM catalog_build_content_digests
            WHERE build_id = ? AND gallery_name = 'gallery-a'
            """,
                (build.build_id,),
            ).fetchone()
            == (1,)
        )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.content_digests,
        ingest_turn=turn,
    )
    content_candidates = database.list_catalog_content_candidates(
        build.build_id,
        limit=1,
    )
    assert len(content_candidates.items) == 1
    assert content_candidates.items[0].candidate.gallery_name == "gallery-b"
    assert content_candidates.items[0].candidate.tags == (GalleryTag("artist", "bob"),)
    second_candidates = database.list_catalog_content_candidates(
        build.build_id,
        after=content_candidates.items[-1].cursor,
        limit=1,
    )
    assert second_candidates.items[0].candidate.gallery_name == "gallery-a"
    assert second_candidates.items[0].candidate.tags == (
        GalleryTag("artist", "alice"),
        GalleryTag("status", "Already Uploaded"),
    )
    assert all(
        row.incumbent_gallery_name is None
        for row in content_candidates.items + second_candidates.items
    )
    database.stage_catalog_content_owners(
        build,
        (CatalogContentOwner(content, "gallery-a"),),
        batch_id="owners",
        ingest_turn=turn,
    )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.content_owners,
        ingest_turn=turn,
    )

    gid_candidates = database.list_catalog_gid_candidates(
        build.build_id,
        limit=100,
    ).items
    assert {row.candidate.gallery_name for row in gid_candidates} == {
        "gallery-a",
        "gallery-c",
        "gallery-d",
    }
    database.stage_catalog_gid_winners(
        build,
        (CatalogGidWinner(10, "gallery-a"), CatalogGidWinner(20, "gallery-d")),
        batch_id="gid-winners",
        ingest_turn=turn,
    )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.gid_winners,
        ingest_turn=turn,
    )

    final_first = database.list_catalog_final_analyses(
        build.build_id,
        limit=2,
    )
    final_after = final_first.items[-1].gallery_key
    assert final_after is not None
    final_second = database.list_catalog_final_analyses(
        build.build_id,
        after=CatalogFinalAnalysisCursor(final_after),
        limit=2,
    )
    final = final_first.items + final_second.items
    assert sorted(
        [
            (item.gallery_name, item.selected, item.duplicate_of_gallery_name)
            for item in final
        ]
    ) == [
        ("gallery-a", True, None),
        ("gallery-b", False, "gallery-a"),
        ("gallery-c", False, None),
        ("gallery-d", True, None),
    ]
    database.stage_catalog_final_analyses(
        build,
        final[:2],
        batch_id="final-1",
        ingest_turn=turn,
    )
    database.stage_catalog_final_analyses(
        build,
        final[2:],
        batch_id="final-2",
        ingest_turn=turn,
    )
    with pytest.raises(CatalogBuildStateError, match="FINAL_ANALYSES"):
        database.complete_catalog_analysis(build, ingest_turn=turn)
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.final_analyses,
        ingest_turn=turn,
    )
    assert database.is_catalog_analysis_phase_complete(
        build.build_id,
        CatalogAnalysisPhase.final_analyses,
    )
    analyzed = database.complete_catalog_analysis(build, ingest_turn=turn)
    assert analyzed.phase is CatalogBuildPhase.artifacts

    sealed = database.seal_catalog_build(analyzed, ingest_turn=turn)
    database.publish_catalog_build(sealed, ingest_turn=turn)
    assert database.complete_gallery_ingest(turn)

    next_turn = _claim(database, periodic=True)
    next_build = _stage_single_to_content(
        database,
        next_turn,
        name="gallery-new",
        gid=10,
        content_sha256=content,
    )
    incumbent = database.list_catalog_content_candidates(
        next_build.build_id,
        limit=10,
    ).items[0]
    assert incumbent.incumbent_gallery_name == "gallery-a"


def test_analysis_falls_back_to_legacy_projection_incumbents(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    timestamp = datetime(2026, 8, 9, tzinfo=UTC)
    content = _digest("legacy-content")
    legacy = GallerySourceRecord(
        gallery_name="legacy-gallery",
        gid=77,
        title="Legacy",
        comment="",
        upload_account="",
        upload_time=timestamp,
        download_time=timestamp,
        modified_time=timestamp,
        tags=(),
        files=(GallerySourceFile("001.jpg", 1, _digest("legacy-file")),),
        source_manifest_sha256=_digest("legacy-manifest"),
        content_sha256=content,
    )
    legacy_turn = _claim(database)
    database.publish_snapshot(
        CatalogSnapshot(
            galleries=(legacy,),
            selections=(CatalogPublicationSelection("legacy-gallery"),),
        ),
        ingest_turn=legacy_turn,
    )
    assert database.complete_gallery_ingest(legacy_turn)
    assert database.get_active_catalog_build() is None

    turn = _claim(database, periodic=True)
    build = _stage_single_to_content(
        database,
        turn,
        name="replacement-gallery",
        gid=77,
        content_sha256=content,
    )
    candidate = database.list_catalog_content_candidates(
        build.build_id,
        limit=10,
    ).items[0]
    assert candidate.incumbent_gallery_name == "legacy-gallery"
    database.stage_catalog_content_owners(
        build,
        (CatalogContentOwner(content, "replacement-gallery"),),
        batch_id="owner",
        ingest_turn=turn,
    )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.content_owners,
        ingest_turn=turn,
    )
    gid_candidate = database.list_catalog_gid_candidates(
        build.build_id,
        limit=10,
    ).items[0]
    assert gid_candidate.incumbent_gallery_name == "legacy-gallery"


def test_analysis_mutations_reject_expired_turns_and_read_only_connectors(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = _stage_source_build(database, turn)

    read_only_config = sqlite_config.model_copy(
        update={
            "database": sqlite_config.database.model_copy(
                update={"access_mode": DatabaseAccessMode.read_only}
            )
        }
    )
    with pytest.raises(PermissionError, match="read-only"):
        H2HDB(read_only_config).stage_catalog_source_manifests(
            build,
            (),
            batch_id="read-only",
            ingest_turn=turn,
        )

    import sqlite3

    with sqlite3.connect(sqlite_config.database.database) as connection:
        connection.execute(
            "UPDATE gallery_ingest_state SET lease_expires_at = 0 WHERE state_id = 1"
        )
    with pytest.raises(IngestTurnLostError):
        database.stage_catalog_source_manifests(
            build,
            (),
            batch_id="expired",
            ingest_turn=turn,
        )
