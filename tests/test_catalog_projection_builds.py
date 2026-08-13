from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from h2hdb import (
    H2HDB,
    CatalogAnalysisPhase,
    CatalogArtifact,
    CatalogBuild,
    CatalogBuildProjectionCoordinator,
    CatalogBuildProjectionPhase,
    CatalogBuildStateError,
    CatalogContentDigest,
    CatalogContentOwner,
    CatalogGidWinner,
    CatalogOperationalGenerationStaleError,
    CatalogPreparedArtifact,
    CatalogProjectionBatchConflictError,
    CatalogProjectionPublicationState,
    CatalogProjectionSelectedGalleryCursor,
    CatalogProjectionSelectionCursor,
    CatalogProjectionStateError,
    CatalogPublicationSelection,
    CatalogSnapshot,
    CatalogSourceGalleryAnalysis,
    CatalogSourceGalleryCompletion,
    CatalogSourceGalleryDiscovery,
    CatalogSourceGalleryHeader,
    CatalogSourceManifest,
    CoreConfig,
    GalleryIngestTurn,
    GallerySourceFile,
    GallerySourceRecord,
    GalleryTag,
    IngestTurnLostError,
)
from h2hdb.sqlite_connector import SQLiteConnector, SQLiteDuplicateKeyError


def _digest(value: str) -> str:
    return sha256(value.encode()).hexdigest()


TIMESTAMP = datetime(2026, 8, 9, 1, 2, 3, tzinfo=UTC)


def _claim(database: H2HDB, *, periodic: bool = False) -> GalleryIngestTurn:
    turn = database.claim_gallery_ingest(
        lease_seconds=120,
        periodic_scan=periodic,
    )
    assert turn is not None
    return turn


def _ready_build(
    database: H2HDB,
    turn: GalleryIngestTurn,
    *,
    scope: str,
    name: str = "gallery-a",
    gid: int = 1001,
) -> CatalogBuild:
    build = database.begin_catalog_build(scope_key=scope, ingest_turn=turn)
    database.discover_catalog_sources(
        build,
        [CatalogSourceGalleryDiscovery(name, f"nested/{name}", "metadata-stat")],
        batch_id=f"discover-{scope}",
        ingest_turn=turn,
    )
    build = database.complete_catalog_discovery(build, ingest_turn=turn)
    database.begin_catalog_gallery(
        build,
        CatalogSourceGalleryHeader(
            gallery_name=name,
            gid=gid,
            title="Stable title",
            comment="summary",
            upload_account="uploader",
            upload_time=TIMESTAMP,
            download_time=TIMESTAMP,
            modified_time=TIMESTAMP,
            tags=(
                GalleryTag("artist", "Artist"),
                GalleryTag("language", "english"),
            ),
        ),
        batch_id=f"header-{scope}",
        ingest_turn=turn,
    )
    source_file = GallerySourceFile(
        name="001.jpg",
        size_bytes=10,
        sha256=_digest("file"),
        relative_locator=f"nested/{name}/001.jpg",
        device=1,
        inode=2,
        modified_ns=3,
        changed_ns=4,
    )
    database.stage_catalog_file_chunk(
        build,
        name,
        [source_file],
        batch_id=f"file-{scope}",
        ingest_turn=turn,
    )
    database.complete_catalog_gallery(
        build,
        CatalogSourceGalleryCompletion(
            gallery_name=name,
            expected_file_count=1,
            scan_observation_sha256=_digest("scan"),
            scan_observation_version=2,
            metadata_sha256=_digest("metadata"),
            page_count=1,
            directory_entry_count=2,
            directory_observation_sha256=_digest("directory"),
        ),
        batch_id=f"complete-{scope}",
        ingest_turn=turn,
    )
    build = database.complete_catalog_source_staging(build, ingest_turn=turn)
    return _complete_durable_analysis(
        database,
        build,
        turn,
        analyses=(
            CatalogSourceGalleryAnalysis(
                gallery_name=name,
                content_sha256=_digest("content"),
                selected=True,
                source_manifest_sha256=_digest("manifest"),
                source_manifest_version=1,
            ),
        ),
        gids={name: gid},
        batch_scope=scope,
    )


def _complete_durable_analysis(
    database: H2HDB,
    build: CatalogBuild,
    turn: GalleryIngestTurn,
    *,
    analyses: tuple[CatalogSourceGalleryAnalysis, ...],
    gids: dict[str, int],
    batch_scope: str,
) -> CatalogBuild:
    manifests = tuple(
        CatalogSourceManifest(
            item.gallery_name,
            item.source_manifest_sha256 or _digest(f"manifest:{item.gallery_name}"),
        )
        for item in analyses
    )
    for offset in range(0, len(manifests) or 1, 1_000):
        database.stage_catalog_source_manifests(
            build,
            manifests[offset : offset + 1_000],
            batch_id=f"analysis-manifests-{batch_scope}-{offset}",
            ingest_turn=turn,
        )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.source_manifests,
        ingest_turn=turn,
    )
    while True:
        page = database.get_catalog_file_spam_page(
            build,
            minimum_occurrences=3,
            limit=100,
            ingest_turn=turn,
        )
        database.apply_catalog_file_spam_page(
            build,
            page,
            (),
            ingest_turn=turn,
        )
        if page.terminal:
            break
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.file_spam,
        ingest_turn=turn,
    )
    digests = tuple(
        CatalogContentDigest(item.gallery_name, item.content_sha256)
        for item in analyses
    )
    for offset in range(0, len(digests) or 1, 1_000):
        database.stage_catalog_content_digests(
            build,
            digests[offset : offset + 1_000],
            batch_id=f"analysis-content-{batch_scope}-{offset}",
            ingest_turn=turn,
        )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.content_digests,
        ingest_turn=turn,
    )
    owners = tuple(
        CatalogContentOwner(item.content_sha256, item.gallery_name)
        for item in analyses
        if item.content_sha256 is not None and item.duplicate_of_gallery_name is None
    )
    for offset in range(0, len(owners) or 1, 1_000):
        database.stage_catalog_content_owners(
            build,
            owners[offset : offset + 1_000],
            batch_id=f"analysis-owners-{batch_scope}-{offset}",
            ingest_turn=turn,
        )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.content_owners,
        ingest_turn=turn,
    )
    winners = tuple(
        CatalogGidWinner(gids[item.gallery_name], item.gallery_name)
        for item in analyses
        if item.selected
    )
    for offset in range(0, len(winners) or 1, 1_000):
        database.stage_catalog_gid_winners(
            build,
            winners[offset : offset + 1_000],
            batch_id=f"analysis-gid-winners-{batch_scope}-{offset}",
            ingest_turn=turn,
        )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.gid_winners,
        ingest_turn=turn,
    )
    for offset in range(0, len(analyses) or 1, 1_000):
        database.stage_catalog_final_analyses(
            build,
            analyses[offset : offset + 1_000],
            batch_id=f"analysis-final-{batch_scope}-{offset}",
            ingest_turn=turn,
        )
    database.complete_catalog_analysis_phase(
        build,
        CatalogAnalysisPhase.final_analyses,
        ingest_turn=turn,
    )
    return database.complete_catalog_analysis(build, ingest_turn=turn)


def _ready_build_many(
    database: H2HDB,
    turn: GalleryIngestTurn,
    *,
    scope: str,
    galleries: Sequence[tuple[str, int, str]],
    timestamp: datetime = TIMESTAMP,
    selected_names: frozenset[str] | None = None,
) -> CatalogBuild:
    build = database.begin_catalog_build(scope_key=scope, ingest_turn=turn)
    database.discover_catalog_sources(
        build,
        tuple(
            CatalogSourceGalleryDiscovery(name, f"nested/{name}", "metadata-stat")
            for name, _gid, _manifest in galleries
        ),
        batch_id=f"discover-{scope}",
        ingest_turn=turn,
    )
    build = database.complete_catalog_discovery(build, ingest_turn=turn)
    database.stage_catalog_gallery_headers(
        build,
        tuple(
            CatalogSourceGalleryHeader(
                gallery_name=name,
                gid=gid,
                title=f"Title {name}",
                comment="summary",
                upload_account="uploader",
                upload_time=timestamp,
                download_time=timestamp,
                modified_time=timestamp,
                tags=(
                    GalleryTag("artist", f"Artist {gid}"),
                    GalleryTag("language", "english"),
                ),
            )
            for name, gid, _manifest in galleries
        ),
        batch_id=f"headers-{scope}",
        ingest_turn=turn,
    )
    database.complete_catalog_galleries(
        build,
        tuple(
            CatalogSourceGalleryCompletion(
                gallery_name=name,
                expected_file_count=0,
                scan_observation_sha256=_digest(f"scan:{name}"),
                scan_observation_version=2,
                metadata_sha256=_digest(f"metadata:{name}"),
                page_count=0,
                directory_entry_count=1,
                directory_observation_sha256=_digest(f"directory:{name}"),
            )
            for name, _gid, _manifest in galleries
        ),
        batch_id=f"complete-{scope}",
        ingest_turn=turn,
    )
    build = database.complete_catalog_source_staging(build, ingest_turn=turn)
    analyses = tuple(
        CatalogSourceGalleryAnalysis(
            gallery_name=name,
            content_sha256=_digest(f"content:{name}"),
            selected=selected_names is None or name in selected_names,
            source_manifest_sha256=manifest,
            source_manifest_version=1,
        )
        for name, _gid, manifest in galleries
    )
    return _complete_durable_analysis(
        database,
        build,
        turn,
        analyses=analyses,
        gids={name: gid for name, gid, _manifest in galleries},
        batch_scope=scope,
    )


def _stage_projection(
    database: H2HDB,
    build: CatalogBuild,
    turn: GalleryIngestTurn,
    *,
    artifact: CatalogArtifact | None,
) -> None:
    database.begin_catalog_build_projection(
        build,
        artifacts_required=artifact is not None,
        ingest_turn=turn,
    )
    gallery_page = database.list_catalog_projection_selected_galleries(
        build.build_id,
        limit=1,
    )
    assert len(gallery_page.items) == 1
    gallery = gallery_page.items[0]
    assert gallery.source_locator == "nested/gallery-a"
    file_page = database.list_catalog_projection_selected_files(
        build.build_id,
        gallery.gallery_key,
        limit=1,
    )
    assert file_page.items[0].relative_locator.endswith("001.jpg")
    if artifact is not None:
        database.record_catalog_prepared_artifacts(
            build,
            (CatalogPreparedArtifact(gallery.gallery_key, artifact),),
            batch_id="prepared",
            ingest_turn=turn,
        )
        database.advance_catalog_artifact_checkpoint(
            build,
            expected_after=None,
            after=gallery.cursor,
            batch_id="artifact-page",
            ingest_turn=turn,
        )
        expected_artifact_cursor: CatalogProjectionSelectedGalleryCursor | None = (
            gallery.cursor
        )
    else:
        expected_artifact_cursor = None
    database.complete_catalog_artifact_preparation(
        build,
        expected_after=expected_artifact_cursor,
        ingest_turn=turn,
    )
    selection_page = database.list_catalog_projection_selections(
        build.build_id,
        limit=1,
    )
    assert len(selection_page.items) == 1
    selection = selection_page.items[0]
    assert selection.artifact == artifact
    result = database.stage_catalog_projection_selections(
        build,
        selection_page.items,
        expected_after=None,
        after=selection.cursor,
        batch_id="selection-page",
        ingest_turn=turn,
    )
    assert result.applied
    assert not database.stage_catalog_projection_selections(
        build,
        selection_page.items,
        expected_after=None,
        after=selection.cursor,
        batch_id="selection-page",
        ingest_turn=turn,
    ).applied
    database.complete_catalog_projection_staging(
        build,
        expected_after=selection.cursor,
        ingest_turn=turn,
    )
    _prepare_operations(database, build, turn)


def _prepare_operations(
    database: H2HDB,
    build: CatalogBuild,
    turn: GalleryIngestTurn,
) -> None:
    while True:
        state = database.prepare_catalog_build_operations(
            build,
            max_rows=2,
            ingest_turn=turn,
        )
        if state.complete:
            return


def _stage_projection_without_artifacts(
    database: H2HDB,
    build: CatalogBuild,
    turn: GalleryIngestTurn,
    *,
    prepare_operations: bool = True,
) -> None:
    database.begin_catalog_build_projection(
        build,
        artifacts_required=False,
        ingest_turn=turn,
    )
    database.complete_catalog_artifact_preparation(
        build,
        expected_after=None,
        ingest_turn=turn,
    )
    after: CatalogProjectionSelectionCursor | None = None
    while True:
        page = database.list_catalog_projection_selections(
            build.build_id,
            after=after,
            limit=200,
        )
        if not page.items:
            break
        database.stage_catalog_projection_selections(
            build,
            page.items,
            expected_after=after,
            after=page.items[-1].cursor,
            batch_id=f"selection-{len(page.items)}-{page.items[-1].gallery_key}",
            ingest_turn=turn,
        )
        after = page.items[-1].cursor
    database.complete_catalog_projection_staging(
        build,
        expected_after=after,
        ingest_turn=turn,
    )
    if prepare_operations:
        _prepare_operations(database, build, turn)


def test_joint_projection_is_invisible_then_atomically_published_and_recoverable(
    sqlite_config: CoreConfig,
    tmp_path: Path,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    assert isinstance(database, CatalogBuildProjectionCoordinator)
    turn = _claim(database)
    build = _ready_build(database, turn, scope="first")
    artifact = CatalogArtifact(
        artifact_id="urn:test:artifact:1",
        name="gallery-a.cbz",
        location=tmp_path / "gallery-a.cbz",
        media_type="application/vnd.comicbook+zip",
        size_bytes=20,
        sha256=_digest("artifact"),
        modified_at=TIMESTAMP,
    )
    _stage_projection(database, build, turn, artifact=artifact)

    projection = database.get_catalog_build_projection(build.build_id)
    assert projection is not None
    assert projection.phase is CatalogBuildProjectionPhase.complete
    assert database.get_catalog_revision().revision == 0
    assert database.list_publications().total == 0

    database.seal_catalog_build_projection(build, ingest_turn=turn)
    build = database.seal_catalog_build(build, ingest_turn=turn)
    sealed_build = build
    published = database.publish_catalog_build_with_projection(
        build,
        ingest_turn=turn,
    )
    retried = database.publish_catalog_build_with_projection(
        sealed_build,
        ingest_turn=turn,
    )
    assert retried.receipt == published.receipt
    assert (
        published.receipt.state is CatalogProjectionPublicationState.database_committed
    )
    assert database.get_catalog_source_revision().revision == 1
    assert (
        database.get_catalog_revision().revision
        == published.receipt.catalog_revision.revision
    )
    assert database.list_publications().publications[0].artifacts == (artifact,)
    artifact_page = database.list_published_catalog_projection_artifacts(
        build.build_id,
        limit=1,
    )
    assert artifact_page.artifacts == (artifact,)
    assert artifact_page.items[0].gallery_name == "gallery-a"
    assert artifact_page.items[0].gid == 1001
    assert artifact_page.items[0].upload_time == TIMESTAMP
    acknowledged = database.acknowledge_catalog_projection_finalized(
        published.build,
        catalog_revision=published.receipt.catalog_revision.revision,
        ingest_turn=turn,
    )
    assert acknowledged.state is CatalogProjectionPublicationState.projection_finalized
    assert (
        database.acknowledge_catalog_projection_finalized(
            published.build,
            catalog_revision=published.receipt.catalog_revision.revision,
            ingest_turn=turn,
        )
        == acknowledged
    )


def test_artifact_disabled_projection_and_stale_turn_fence(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = _ready_build(database, turn, scope="without-artifacts")
    wrong = replace(turn, owner_token="wrong")
    with pytest.raises(IngestTurnLostError):
        database.begin_catalog_build_projection(
            build,
            artifacts_required=False,
            ingest_turn=wrong,
        )
    _stage_projection(database, build, turn, artifact=None)
    database.seal_catalog_build_projection(build, ingest_turn=turn)
    build = database.seal_catalog_build(build, ingest_turn=turn)
    result = database.publish_catalog_build_with_projection(
        build,
        ingest_turn=turn,
    )
    assert result.receipt.selected_galleries == 1
    assert database.list_publications().publications[0].artifacts == ()


def test_identical_projection_reuses_current_catalog_revision(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    first_turn = _claim(database)
    first = _ready_build(database, first_turn, scope="same-1")
    _stage_projection(database, first, first_turn, artifact=None)
    database.seal_catalog_build_projection(first, ingest_turn=first_turn)
    first = database.seal_catalog_build(first, ingest_turn=first_turn)
    first_result = database.publish_catalog_build_with_projection(
        first,
        ingest_turn=first_turn,
    )
    assert database.complete_gallery_ingest(first_turn)

    second_turn = _claim(database, periodic=True)
    second = _ready_build(database, second_turn, scope="same-2")
    _stage_projection(database, second, second_turn, artifact=None)
    database.seal_catalog_build_projection(second, ingest_turn=second_turn)
    second = database.seal_catalog_build(second, ingest_turn=second_turn)
    second_result = database.publish_catalog_build_with_projection(
        second,
        ingest_turn=second_turn,
    )
    assert (
        second_result.receipt.catalog_revision.revision
        == first_result.receipt.catalog_revision.revision
    )
    assert second_result.receipt.source_revision == 2


def test_legacy_allocator_skips_reserved_revision_and_joint_base_race_is_atomic(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = _ready_build(database, turn, scope="race")
    _stage_projection(database, build, turn, artifact=None)
    database.seal_catalog_build_projection(build, ingest_turn=turn)
    build = database.seal_catalog_build(build, ingest_turn=turn)
    legacy_gallery = GallerySourceRecord(
        gallery_name="legacy-gallery",
        gid=9001,
        title="Legacy",
        comment="",
        upload_account="",
        upload_time=TIMESTAMP,
        download_time=TIMESTAMP,
        modified_time=TIMESTAMP,
        tags=(),
        files=(GallerySourceFile("legacy.jpg", 1, _digest("legacy-file")),),
        source_manifest_sha256=_digest("legacy-manifest"),
    )
    legacy = database.publish_snapshot(
        CatalogSnapshot(
            galleries=(legacy_gallery,),
            selections=(CatalogPublicationSelection("legacy-gallery"),),
        ),
        ingest_turn=turn,
    )
    # Revision 1 is durably reserved by the invisible candidate.
    assert legacy.revision.revision == 2
    assert database.get_catalog_source_revision().revision == 0
    with pytest.raises(CatalogOperationalGenerationStaleError):
        database.publish_catalog_build_with_projection(build, ingest_turn=turn)
    _prepare_operations(database, build, turn)
    with pytest.raises(CatalogProjectionStateError, match="base revision"):
        database.publish_catalog_build_with_projection(build, ingest_turn=turn)
    assert database.get_catalog_source_revision().revision == 0
    assert database.get_catalog_revision().revision == 2
    assert database.list_publications().publications[0].gid == 9001


def test_projection_ranges_reject_an_unbounded_cursor_jump(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = _ready_build_many(
        database,
        turn,
        scope="bounded-range",
        galleries=tuple(
            (f"gallery-{index:03d}", 10_000 + index, _digest(f"manifest:{index}"))
            for index in range(201)
        ),
    )
    database.begin_catalog_build_projection(
        build,
        artifacts_required=False,
        ingest_turn=turn,
    )
    first = database.list_catalog_projection_selected_galleries(
        build.build_id,
        limit=200,
    )
    second = database.list_catalog_projection_selected_galleries(
        build.build_id,
        after=first.next_cursor,
        limit=200,
    )
    assert len(first.items) == 200
    assert len(second.items) == 1
    with pytest.raises(CatalogProjectionStateError, match="bounded page limit"):
        database.advance_catalog_artifact_checkpoint(
            build,
            expected_after=None,
            after=second.items[0].cursor,
            batch_id="malicious-artifact-jump",
            ingest_turn=turn,
        )

    database.complete_catalog_artifact_preparation(
        build,
        expected_after=None,
        ingest_turn=turn,
    )
    final_selection = database.list_catalog_projection_selections(
        build.build_id,
        after=CatalogProjectionSelectionCursor(first.items[-1].gallery_key),
        limit=1,
    )
    assert len(final_selection.items) == 1
    with pytest.raises(CatalogProjectionStateError, match="bounded page limit"):
        database.stage_catalog_projection_selections(
            build,
            final_selection.items,
            expected_after=None,
            after=final_selection.items[0].cursor,
            batch_id="malicious-selection-jump",
            ingest_turn=turn,
        )

    abandoned = database.abandon_catalog_build(build, ingest_turn=turn)
    pruned = database.prune_catalog_build_projection(
        abandoned,
        max_rows=1,
        ingest_turn=turn,
    )
    assert pruned.complete
    assert pruned.deleted_rows == 1


def test_plural_artifact_receipts_and_checkpoints_resume_after_response_loss(
    sqlite_config: CoreConfig,
    tmp_path: Path,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = _ready_build_many(
        database,
        turn,
        scope="artifact-resume",
        galleries=(
            ("gallery-a", 101, _digest("manifest:a")),
            ("gallery-b", 102, _digest("manifest:b")),
        ),
    )
    database.begin_catalog_build_projection(
        build,
        artifacts_required=True,
        ingest_turn=turn,
    )
    gallery_page = database.list_catalog_projection_selected_galleries(
        build.build_id,
        limit=2,
    )
    prepared = tuple(
        CatalogPreparedArtifact(
            item.gallery_key,
            CatalogArtifact(
                artifact_id=f"urn:test:artifact:{item.gid}",
                name=f"{item.gallery_name}.cbz",
                location=tmp_path / f"{item.gallery_name}.cbz",
                media_type="application/vnd.comicbook+zip",
                size_bytes=item.gid,
                sha256=_digest(f"artifact:{item.gid}"),
                modified_at=TIMESTAMP,
            ),
        )
        for item in gallery_page.items
    )
    database.record_catalog_prepared_artifacts(
        build,
        prepared[:1],
        batch_id="prepared-first",
        ingest_turn=turn,
    )
    with pytest.raises(
        CatalogProjectionStateError, match="missing a prepared artifact"
    ):
        database.advance_catalog_artifact_checkpoint(
            build,
            expected_after=None,
            after=gallery_page.items[-1].cursor,
            batch_id="checkpoint-too-soon",
            ingest_turn=turn,
        )
    recorded = database.record_catalog_prepared_artifacts(
        build,
        prepared,
        batch_id="prepared-page",
        ingest_turn=turn,
    )
    assert recorded.applied
    assert not database.record_catalog_prepared_artifacts(
        build,
        prepared,
        batch_id="prepared-page",
        ingest_turn=turn,
    ).applied
    changed = replace(
        prepared[-1],
        artifact=replace(prepared[-1].artifact, sha256=_digest("changed")),
    )
    with pytest.raises(CatalogProjectionBatchConflictError, match="different data"):
        database.record_catalog_prepared_artifacts(
            build,
            (*prepared[:-1], changed),
            batch_id="prepared-page",
            ingest_turn=turn,
        )

    first_cursor = gallery_page.items[0].cursor
    final_cursor = gallery_page.items[-1].cursor
    database.advance_catalog_artifact_checkpoint(
        build,
        expected_after=None,
        after=first_cursor,
        batch_id="artifact-page-1",
        ingest_turn=turn,
    )
    restarted = H2HDB(sqlite_config)
    assert (
        restarted.get_catalog_projection_checkpoint(
            build.build_id
        ).artifact_after_gallery_key
        == first_cursor.gallery_key
    )
    restarted.advance_catalog_artifact_checkpoint(
        build,
        expected_after=first_cursor,
        after=final_cursor,
        batch_id="artifact-page-2",
        ingest_turn=turn,
    )
    restarted.complete_catalog_artifact_preparation(
        build,
        expected_after=final_cursor,
        ingest_turn=turn,
    )
    selections = restarted.list_catalog_projection_selections(
        build.build_id,
        limit=2,
    )
    staged = restarted.stage_catalog_projection_selections(
        build,
        selections.items,
        expected_after=None,
        after=selections.items[-1].cursor,
        batch_id="selection-page",
        ingest_turn=turn,
    )
    assert staged.applied
    assert not restarted.stage_catalog_projection_selections(
        build,
        selections.items,
        expected_after=None,
        after=selections.items[-1].cursor,
        batch_id="selection-page",
        ingest_turn=turn,
    ).applied
    with pytest.raises(CatalogProjectionBatchConflictError, match="different data"):
        restarted.stage_catalog_projection_selections(
            build,
            (
                *selections.items[:-1],
                replace(selections.items[-1], redownload_required=True),
            ),
            expected_after=None,
            after=selections.items[-1].cursor,
            batch_id="selection-page",
            ingest_turn=turn,
        )
    with pytest.raises(CatalogProjectionBatchConflictError, match="compare-and-swap"):
        restarted.stage_catalog_projection_selections(
            build,
            selections.items,
            expected_after=None,
            after=selections.items[-1].cursor,
            batch_id="stale-cursor",
            ingest_turn=turn,
        )
    restarted.complete_catalog_projection_staging(
        build,
        expected_after=selections.items[-1].cursor,
        ingest_turn=turn,
    )


def test_pending_receipt_can_be_finalized_by_a_new_live_lease(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    original_turn = _claim(database)
    build = _ready_build(database, original_turn, scope="recovery")
    _stage_projection(database, build, original_turn, artifact=None)
    database.seal_catalog_build_projection(build, ingest_turn=original_turn)
    build = database.seal_catalog_build(build, ingest_turn=original_turn)
    published = database.publish_catalog_build_with_projection(
        build,
        ingest_turn=original_turn,
    )
    assert (
        database.get_catalog_projection_publication_receipt(pending_only=True)
        == published.receipt
    )
    with database._context.SQLConnector() as connector:
        with connector.transaction():
            connector.execute(
                "UPDATE gallery_ingest_state SET lease_expires_at = 0 "
                "WHERE state_id = 1"
            )
    recovery_turn = _claim(database, periodic=True)
    assert recovery_turn.owner_token != original_turn.owner_token
    with pytest.raises(IngestTurnLostError):
        database.acknowledge_catalog_projection_finalized(
            published.build,
            catalog_revision=published.receipt.catalog_revision.revision,
            ingest_turn=original_turn,
        )
    with pytest.raises(CatalogProjectionStateError, match="different catalog revision"):
        database.acknowledge_catalog_projection_finalized(
            published.build,
            catalog_revision=published.receipt.catalog_revision.revision + 1,
            ingest_turn=recovery_turn,
        )
    recovered = database.acknowledge_catalog_projection_finalized(
        published.build,
        catalog_revision=published.receipt.catalog_revision.revision,
        ingest_turn=recovery_turn,
    )
    assert recovered.state is CatalogProjectionPublicationState.projection_finalized
    assert (
        database.acknowledge_catalog_projection_finalized(
            published.build,
            catalog_revision=published.receipt.catalog_revision.revision,
            ingest_turn=recovery_turn,
        )
        == recovered
    )
    assert (
        database.get_catalog_projection_publication_receipt(pending_only=True) is None
    )


def test_reused_candidate_is_cleaned_child_first_without_losing_receipt(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    first_turn = _claim(database)
    first = _ready_build(database, first_turn, scope="cleanup-1")
    _stage_projection(database, first, first_turn, artifact=None)
    database.seal_catalog_build_projection(first, ingest_turn=first_turn)
    first = database.seal_catalog_build(first, ingest_turn=first_turn)
    first_result = database.publish_catalog_build_with_projection(
        first,
        ingest_turn=first_turn,
    )
    database.acknowledge_catalog_projection_finalized(
        first_result.build,
        catalog_revision=first_result.receipt.catalog_revision.revision,
        ingest_turn=first_turn,
    )
    assert database.complete_gallery_ingest(first_turn)

    second_turn = _claim(database, periodic=True)
    second = _ready_build(database, second_turn, scope="cleanup-2")
    _stage_projection(database, second, second_turn, artifact=None)
    sealed_projection = database.seal_catalog_build_projection(
        second,
        ingest_turn=second_turn,
    )
    second = database.seal_catalog_build(second, ingest_turn=second_turn)
    second_result = database.publish_catalog_build_with_projection(
        second,
        ingest_turn=second_turn,
    )
    assert (
        second_result.receipt.catalog_revision.revision
        == first_result.receipt.catalog_revision.revision
    )
    assert (
        sealed_projection.reserved_revision
        != first_result.receipt.catalog_revision.revision
    )
    database.acknowledge_catalog_projection_finalized(
        second_result.build,
        catalog_revision=second_result.receipt.catalog_revision.revision,
        ingest_turn=second_turn,
    )
    for _ in range(20):
        result = database.prune_catalog_build_projection(
            second_result.build,
            max_rows=1,
            ingest_turn=second_turn,
        )
        assert result.deleted_rows <= 1
        if result.complete:
            break
    else:
        pytest.fail("Reused candidate did not finish bounded cleanup")
    with database._context.SQLConnector() as connector:
        assert not connector.fetch_one(
            "SELECT 1 FROM catalog_publications WHERE revision = %s",
            (sealed_projection.reserved_revision,),
        )
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_build_projections WHERE build_id = %s",
            (second.build_id,),
        )
        assert connector.fetch_one(
            "SELECT 1 FROM catalog_projection_publication_receipts WHERE build_id = %s",
            (second.build_id,),
        )
    assert database.list_publications().publications[0].gid == 1001


def test_joint_pointer_failure_rolls_back_both_revisions(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = _ready_build(database, turn, scope="pointer-failure")
    _stage_projection(database, build, turn, artifact=None)
    database.seal_catalog_build_projection(build, ingest_turn=turn)
    build = database.seal_catalog_build(build, ingest_turn=turn)
    with database._context.SQLConnector() as connector:
        connector.execute("""
            CREATE TRIGGER reject_catalog_pointer
            BEFORE UPDATE ON catalog_revision
            BEGIN
                SELECT RAISE(ABORT, 'forced catalog pointer failure');
            END
            """)
    with pytest.raises(SQLiteDuplicateKeyError, match="forced catalog pointer failure"):
        database.publish_catalog_build_with_projection(build, ingest_turn=turn)
    assert database.get_catalog_source_revision().revision == 0
    assert database.get_catalog_revision().revision == 0
    assert database.get_catalog_projection_publication_receipt(build.build_id) is None
    with database._context.SQLConnector() as connector:
        connector.execute("DROP TRIGGER reject_catalog_pointer")
    published = database.publish_catalog_build_with_projection(
        build,
        ingest_turn=turn,
    )
    assert published.receipt.source_revision == 1


def test_joint_publish_transaction_never_scans_source_or_candidate_rows(
    sqlite_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = _ready_build(database, turn, scope="publish-sql")
    _stage_projection(database, build, turn, artifact=None)
    database.seal_catalog_build_projection(build, ingest_turn=turn)
    build = database.seal_catalog_build(build, ingest_turn=turn)
    queries: list[str] = []
    original_fetch_one = SQLiteConnector.fetch_one
    original_fetch_all = SQLiteConnector.fetch_all

    def tracked_fetch_one(
        connector: SQLiteConnector,
        query: str,
        data: tuple[Any, ...] = (),
    ) -> tuple[Any, ...]:
        queries.append(query)
        return original_fetch_one(connector, query, data)

    def tracked_fetch_all(
        connector: SQLiteConnector,
        query: str,
        data: tuple[Any, ...] = (),
    ) -> list[tuple[Any, ...]]:
        queries.append(query)
        return original_fetch_all(connector, query, data)

    monkeypatch.setattr(SQLiteConnector, "fetch_one", tracked_fetch_one)
    monkeypatch.setattr(SQLiteConnector, "fetch_all", tracked_fetch_all)
    database.publish_catalog_build_with_projection(build, ingest_turn=turn)
    normalized = "\n".join(query.casefold() for query in queries)
    assert "catalog_source_galleries" not in normalized
    assert "catalog_source_files" not in normalized
    assert "catalog_publications" not in normalized
    assert "catalog_artifacts" not in normalized


def test_first_source_build_diff_uses_the_legacy_canonical_baseline(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    legacy_turn = _claim(database)

    def legacy_gallery(name: str, gid: int, manifest: str) -> GallerySourceRecord:
        return GallerySourceRecord(
            gallery_name=name,
            gid=gid,
            title=name,
            comment="",
            upload_account="",
            upload_time=TIMESTAMP,
            download_time=TIMESTAMP,
            modified_time=TIMESTAMP,
            tags=(),
            files=(),
            source_manifest_sha256=manifest,
        )

    legacy_galleries = (
        legacy_gallery("same", 201, _digest("manifest:same")),
        legacy_gallery("changed", 202, _digest("manifest:old")),
        legacy_gallery("removed", 203, _digest("manifest:removed")),
    )
    database.publish_snapshot(
        CatalogSnapshot(
            galleries=legacy_galleries,
            selections=tuple(
                CatalogPublicationSelection(gallery.gallery_name)
                for gallery in legacy_galleries
            ),
        ),
        ingest_turn=legacy_turn,
    )
    assert database.complete_gallery_ingest(legacy_turn)
    turn = _claim(database, periodic=True)
    build = _ready_build_many(
        database,
        turn,
        scope="legacy-baseline",
        galleries=(
            ("same", 201, _digest("manifest:same")),
            ("changed", 202, _digest("manifest:new")),
            ("new", 204, _digest("manifest:new-gallery")),
        ),
    )
    _stage_projection_without_artifacts(database, build, turn)
    sealed = database.seal_catalog_build_projection(build, ingest_turn=turn)
    assert (
        sealed.new_galleries,
        sealed.changed_galleries,
        sealed.removed_galleries,
    ) == (
        1,
        1,
        1,
    )


def test_operational_removed_gid_events_are_invisible_until_activation_and_acked(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    first_turn = _claim(database)
    first = _ready_build_many(
        database,
        first_turn,
        scope="operations-first",
        galleries=(
            ("keep", 501, _digest("manifest:keep")),
            ("remove", 502, _digest("manifest:remove")),
        ),
    )
    _stage_projection_without_artifacts(database, first, first_turn)
    database.seal_catalog_build_projection(first, ingest_turn=first_turn)
    first = database.seal_catalog_build(first, ingest_turn=first_turn)
    database.publish_catalog_build_with_projection(first, ingest_turn=first_turn)
    assert database.complete_gallery_ingest(first_turn)

    second_turn = _claim(database, periodic=True)
    second = _ready_build_many(
        database,
        second_turn,
        scope="operations-second",
        galleries=(("keep", 501, _digest("manifest:keep")),),
    )
    _stage_projection_without_artifacts(database, second, second_turn)
    assert database.get_download_request(502) is None
    database.seal_catalog_build_projection(second, ingest_turn=second_turn)
    second = database.seal_catalog_build(second, ingest_turn=second_turn)
    database.publish_catalog_build_with_projection(second, ingest_turn=second_turn)

    request = database.get_download_request(502)
    assert request is not None
    states = database.get_candidate_states((501, 502))
    assert states[501].cataloged
    assert not states[502].cataloged
    assert states[502].requested
    database.complete_download_request(request)
    assert database.get_download_request(502) is None


def test_operational_deletion_consumption_is_tokenized_and_rerequestable(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    first_turn = _claim(database)
    first = _ready_build_many(
        database,
        first_turn,
        scope="deletion-first",
        galleries=(("remove", 601, _digest("manifest:remove")),),
    )
    _stage_projection_without_artifacts(database, first, first_turn)
    database.seal_catalog_build_projection(first, ingest_turn=first_turn)
    first = database.seal_catalog_build(first, ingest_turn=first_turn)
    database.publish_catalog_build_with_projection(first, ingest_turn=first_turn)
    assert database.complete_gallery_ingest(first_turn)

    database.request_gallery_deletion(601)
    assert database.get_gallery_deletion_requests() == [601]
    second_turn = _claim(database, periodic=True)
    second = _ready_build_many(
        database,
        second_turn,
        scope="deletion-second",
        galleries=(),
    )
    _stage_projection_without_artifacts(database, second, second_turn)
    database.seal_catalog_build_projection(second, ingest_turn=second_turn)
    second = database.seal_catalog_build(second, ingest_turn=second_turn)
    database.publish_catalog_build_with_projection(second, ingest_turn=second_turn)
    assert database.get_gallery_deletion_requests() == []
    assert database.get_download_request(601) is None

    database.request_gallery_deletion(601)
    assert database.get_gallery_deletion_requests() == [601]
    database.request_gallery_deletion(601)
    assert database.get_gallery_deletion_requests() == [601]


def test_deletion_command_view_uses_exact_active_source_authority_and_quoting(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()

    def rm_command(path: str) -> str:
        return "rm -rf -- '" + path.replace("'", "'\\''") + "'"

    def deletion_commands() -> set[str]:
        with database._context.SQLConnector() as connector:
            rows = connector.fetch_all("SELECT cmd FROM todelete_rm_commands")
        return {str(row[0]) for row in rows}

    legacy = GallerySourceRecord(
        gallery_name="legacy 'only",
        gid=8_000,
        title="Legacy",
        comment="",
        upload_account="",
        upload_time=TIMESTAMP,
        download_time=TIMESTAMP,
        modified_time=TIMESTAMP,
        tags=(),
        files=(),
        source_manifest_sha256=_digest("legacy-only"),
    )
    legacy_turn = _claim(database)
    legacy_snapshot = CatalogSnapshot(
        galleries=(legacy,),
        selections=(CatalogPublicationSelection(legacy.gallery_name),),
    )
    database.publish_snapshot(legacy_snapshot, ingest_turn=legacy_turn)
    assert database.complete_gallery_ingest(legacy_turn)
    database.request_gallery_deletion(legacy.gid)
    refresh_turn = _claim(database, periodic=True)
    database.publish_snapshot(legacy_snapshot, ingest_turn=refresh_turn)
    assert database.complete_gallery_ingest(refresh_turn)
    assert deletion_commands() == {rm_command(legacy.gallery_name)}

    turn = _claim(database, periodic=True)
    explicit_name = "explicit 'quote"
    older_name = "older-copy"
    newer_name = "newer-copy"
    duplicate_name = "duplicate-pages"
    keep_name = "keep"
    build = _ready_build_many(
        database,
        turn,
        scope="active-deletion-view",
        galleries=(
            (explicit_name, 8_001, _digest("manifest:explicit")),
            (older_name, 8_002, _digest("manifest:older")),
            (newer_name, 8_002, _digest("manifest:newer")),
            (duplicate_name, 8_003, _digest("manifest:duplicate")),
            (keep_name, 8_004, _digest("manifest:keep")),
        ),
        selected_names=frozenset(
            {explicit_name, newer_name, duplicate_name, keep_name}
        ),
    )
    with database._context.SQLConnector() as connector:
        with connector.transaction():
            connector.execute(
                """
                UPDATE catalog_source_galleries
                SET download_time = %s
                WHERE build_id = %s AND gallery_key = %s
                """,
                (
                    "2025-01-01T00:00:00+00:00",
                    build.build_id,
                    _digest(older_name),
                ),
            )
            connector.execute(
                """
                UPDATE catalog_source_galleries
                SET download_time = %s
                WHERE build_id = %s AND gallery_key = %s
                """,
                (
                    "2025-02-01T00:00:00+00:00",
                    build.build_id,
                    _digest(newer_name),
                ),
            )
            connector.execute(
                """
                UPDATE catalog_build_content_digests
                SET content_sha256 = NULL,
                    duplicate_hash_deletion_candidate = 1
                WHERE build_id = %s AND gallery_key = %s
                """,
                (build.build_id, _digest(duplicate_name)),
            )
    database.request_gallery_deletion(8_001)
    _stage_projection_without_artifacts(database, build, turn)
    database.seal_catalog_build_projection(build, ingest_turn=turn)
    build = database.seal_catalog_build(build, ingest_turn=turn)
    result = database.publish_catalog_build_with_projection(build, ingest_turn=turn)

    active_commands = deletion_commands()
    assert active_commands == {
        rm_command(f"nested/{explicit_name}"),
        rm_command(f"nested/{older_name}"),
        rm_command(f"nested/{duplicate_name}"),
    }
    assert rm_command(legacy.gallery_name) not in active_commands
    assert rm_command(f"nested/{newer_name}") not in active_commands
    assert rm_command(f"nested/{keep_name}") not in active_commands

    with database._context.SQLConnector() as connector:
        with connector.transaction():
            token_row = connector.fetch_one(
                "SELECT request_token FROM todelete_gids WHERE gid = %s",
                (8_001,),
            )
            activation_row = connector.fetch_one(
                """
                SELECT preparation_id
                FROM catalog_operational_activations
                WHERE build_id = %s AND source_revision = %s
                """,
                (build.build_id, result.receipt.source_revision),
            )
            assert token_row is not None and activation_row is not None
            connector.execute(
                """
                INSERT INTO catalog_build_deletion_consumptions (
                    build_id,
                    preparation_id,
                    gid,
                    deletion_request_token
                ) VALUES (%s, %s, %s, %s)
                """,
                (
                    build.build_id,
                    str(activation_row[0]),
                    8_001,
                    str(token_row[0]),
                ),
            )
    assert rm_command(f"nested/{explicit_name}") not in deletion_commands()

    database.request_gallery_deletion(8_001)
    assert rm_command(f"nested/{explicit_name}") in deletion_commands()

    with database._context.SQLConnector() as connector:
        connector.execute(
            "DELETE FROM catalog_operational_activations WHERE build_id = %s",
            (build.build_id,),
        )
    assert deletion_commands() == {rm_command(legacy.gallery_name)}


def test_operational_generation_race_is_refreshable_on_the_same_sealed_build(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = _ready_build(database, turn, scope="generation-race")
    _stage_projection(database, build, turn, artifact=None)
    database.seal_catalog_build_projection(build, ingest_turn=turn)
    build = database.seal_catalog_build(build, ingest_turn=turn)

    database.request_gallery_deletion(999_001)
    with pytest.raises(CatalogOperationalGenerationStaleError):
        database.publish_catalog_build_with_projection(build, ingest_turn=turn)
    _prepare_operations(database, build, turn)
    result = database.publish_catalog_build_with_projection(build, ingest_turn=turn)
    assert result.receipt.source_revision == 1


def test_joint_publish_accounts_maintenance_once_across_receipt_retry(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = _ready_build(database, turn, scope="maintenance")
    _stage_projection(database, build, turn, artifact=None)
    database.seal_catalog_build_projection(build, ingest_turn=turn)
    build = database.seal_catalog_build(build, ingest_turn=turn)
    first = database.publish_catalog_build_with_projection(build, ingest_turn=turn)
    accumulated = database._database_maintenance.get_state().accumulated_work
    retried = database.publish_catalog_build_with_projection(
        first.build,
        ingest_turn=turn,
    )
    assert retried == first
    assert database._database_maintenance.get_state().accumulated_work == accumulated
    assert accumulated == 1


def test_active_source_redownload_runtime_survives_outside_immutable_build(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = _ready_build_many(
        database,
        turn,
        scope="redownload-runtime",
        galleries=(("old", 701, _digest("manifest:old")),),
        timestamp=datetime(2020, 1, 1, tzinfo=UTC),
    )
    _stage_projection_without_artifacts(database, build, turn)
    database.seal_catalog_build_projection(build, ingest_turn=turn)
    build = database.seal_catalog_build(build, ingest_turn=turn)
    database.publish_catalog_build_with_projection(build, ingest_turn=turn)

    assert database.get_pending_redownload_gids() == [701]
    assert database.get_candidate_states((701,))[701].redownload_required
    database.record_accepted_submission(701)
    assert database.get_pending_redownload_gids() == []


def test_active_pending_redownload_honors_duplicate_hash_deletion_flag(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    build = _ready_build_many(
        database,
        turn,
        scope="duplicate-hash-flag",
        galleries=(
            ("flagged", 702, _digest("manifest:flagged")),
            ("eligible", 703, _digest("manifest:eligible")),
        ),
        timestamp=datetime(2020, 1, 1, tzinfo=UTC),
    )
    with database._context.SQLConnector() as connector:
        connector.execute_many(
            """
            UPDATE catalog_build_content_digests
            SET content_sha256 = NULL,
                duplicate_hash_deletion_candidate = %s
            WHERE build_id = %s AND gallery_key = %s
            """,
            [
                (True, build.build_id, _digest("flagged")),
                (False, build.build_id, _digest("eligible")),
            ],
        )
    _stage_projection_without_artifacts(database, build, turn)
    database.seal_catalog_build_projection(build, ingest_turn=turn)
    build = database.seal_catalog_build(build, ingest_turn=turn)
    database.publish_catalog_build_with_projection(build, ingest_turn=turn)

    assert database.get_pending_redownload_gids() == [703]
    states = database.get_candidate_states((702, 703))
    assert not states[702].redownload_required
    assert states[703].redownload_required


def test_operational_preparation_is_hard_capped_and_restartable(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    turn = _claim(database)
    galleries = tuple(
        (f"gallery-{index:04d}", 10_000 + index, _digest(f"manifest:{index}"))
        for index in range(1001)
    )
    build = _ready_build_many(
        database,
        turn,
        scope="bounded-operations",
        galleries=galleries,
    )
    _stage_projection_without_artifacts(
        database,
        build,
        turn,
        prepare_operations=False,
    )

    previous_normalized = 0
    calls = 0
    while True:
        state = database.prepare_catalog_build_operations(
            build,
            max_rows=37,
            ingest_turn=turn,
        )
        calls += 1
        assert state.normalized_gallery_count - previous_normalized <= 37
        previous_normalized = state.normalized_gallery_count
        if state.complete:
            break
    assert calls > 27
    assert state.normalized_gallery_count == len(galleries)


def test_inactive_finalized_build_cleanup_preserves_historical_catalog_revision(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    first_turn = _claim(database)
    first = _ready_build_many(
        database,
        first_turn,
        scope="cleanup-published-a",
        galleries=(("gallery-a", 801, _digest("manifest:a")),),
    )
    _stage_projection_without_artifacts(database, first, first_turn)
    database.seal_catalog_build_projection(first, ingest_turn=first_turn)
    first = database.seal_catalog_build(first, ingest_turn=first_turn)
    first_result = database.publish_catalog_build_with_projection(
        first,
        ingest_turn=first_turn,
    )
    database.acknowledge_catalog_projection_finalized(
        first_result.build,
        catalog_revision=first_result.receipt.catalog_revision.revision,
        ingest_turn=first_turn,
    )
    assert database.complete_gallery_ingest(first_turn)

    second_turn = _claim(database, periodic=True)
    second = _ready_build_many(
        database,
        second_turn,
        scope="cleanup-published-b",
        galleries=(("gallery-b", 802, _digest("manifest:b")),),
    )
    _stage_projection_without_artifacts(database, second, second_turn)
    database.seal_catalog_build_projection(second, ingest_turn=second_turn)
    second = database.seal_catalog_build(second, ingest_turn=second_turn)
    second_result = database.publish_catalog_build_with_projection(
        second,
        ingest_turn=second_turn,
    )
    candidates = database.list_catalog_build_cleanup_candidates()
    assert first.build_id in {candidate.build_id for candidate in candidates}
    assert second.build_id not in {candidate.build_id for candidate in candidates}

    while not database.prune_catalog_build_projection(
        first_result.build,
        max_rows=1,
        ingest_turn=second_turn,
    ).complete:
        pass
    while not database.prune_catalog_build(first.build_id, max_rows=1).complete:
        pass

    historical = database.list_publications(
        revision=first_result.receipt.catalog_revision,
    )
    assert historical.publications[0].gid == 801
    with pytest.raises(LookupError):
        database.get_catalog_source_revision(first_result.receipt.source_revision)
    assert (
        database.get_catalog_source_revision().build_id == second_result.build.build_id
    )


def test_cleanup_discovery_protects_active_and_pending_receipt_builds(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    first_turn = _claim(database)
    first = _ready_build(database, first_turn, scope="cleanup-pending-a")
    _stage_projection(database, first, first_turn, artifact=None)
    database.seal_catalog_build_projection(first, ingest_turn=first_turn)
    first = database.seal_catalog_build(first, ingest_turn=first_turn)
    first_result = database.publish_catalog_build_with_projection(
        first,
        ingest_turn=first_turn,
    )
    assert database.complete_gallery_ingest(first_turn)

    second_turn = _claim(database, periodic=True)
    second = _ready_build(database, second_turn, scope="cleanup-active-b")
    _stage_projection(database, second, second_turn, artifact=None)
    database.seal_catalog_build_projection(second, ingest_turn=second_turn)
    second = database.seal_catalog_build(second, ingest_turn=second_turn)
    second_result = database.publish_catalog_build_with_projection(
        second,
        ingest_turn=second_turn,
    )

    assert database.list_catalog_build_cleanup_candidates() == ()
    with pytest.raises(CatalogProjectionStateError):
        database.prune_catalog_build_projection(
            first_result.build,
            ingest_turn=second_turn,
        )
    with pytest.raises(CatalogBuildStateError, match="projection cleanup"):
        database.prune_catalog_build(first.build_id)
    with pytest.raises(CatalogBuildStateError, match="active or working"):
        database.prune_catalog_build(second_result.build.build_id)


def test_operational_activation_gate_falls_back_for_legacy_source_and_guards_publish(
    sqlite_config: CoreConfig,
) -> None:
    database = H2HDB(sqlite_config)
    database.migrate()
    legacy_turn = _claim(database)
    legacy_gallery = GallerySourceRecord(
        gallery_name="legacy-authority",
        gid=901,
        title="Legacy",
        comment="",
        upload_account="",
        upload_time=TIMESTAMP,
        download_time=TIMESTAMP,
        modified_time=TIMESTAMP,
        tags=(),
        files=(),
        source_manifest_sha256=_digest("legacy-authority"),
    )
    database.publish_snapshot(
        CatalogSnapshot(
            galleries=(legacy_gallery,),
            selections=(CatalogPublicationSelection("legacy-authority"),),
        ),
        ingest_turn=legacy_turn,
    )
    assert database.complete_gallery_ingest(legacy_turn)

    turn = _claim(database, periodic=True)
    build = _ready_build(database, turn, scope="activation-gate", gid=902)
    build = database.seal_catalog_build(build, ingest_turn=turn)
    published = database.publish_catalog_build(build, ingest_turn=turn)
    states = database.get_candidate_states((901, 902))
    assert not states[901].cataloged
    assert states[902].cataloged
    with pytest.raises(CatalogBuildStateError, match="Legacy snapshot"):
        database.publish_snapshot(
            CatalogSnapshot(galleries=(), selections=()),
            ingest_turn=turn,
        )

    # A source pointer created by pre-v5 code has no matching activation and
    # therefore remains on the legacy operational authority after migration.
    with database._context.SQLConnector() as connector:
        connector.execute(
            "DELETE FROM catalog_operational_activations WHERE build_id = %s",
            (published.build.build_id,),
        )
    fallback = database.get_candidate_states((901, 902))
    assert fallback[901].cataloged
    assert not fallback[902].cataloged
