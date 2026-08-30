from __future__ import annotations

from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from vnext_generated_database import open_generated_sqlite_database

from h2hdb import (
    CoreConfig,
    DatabaseConfig,
    DirectoryObservation,
    FileContentReceipt,
    FileObservation,
    GalleryObservationDirectoryFileType,
    GalleryObservationMetadata,
    TagObservation,
    VNextArtifactProducer,
    VNextArtifactStoragePolicy,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextDatabaseAdminFacade,
    VNextIngestFacade,
    VNextIngestGalleryObservation,
    VNextIngestPage,
    VNextIngestPolicy,
    VNextSourceManifestMismatchError,
)
from h2hdb.repository import RepositoryContext
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_gallery_staging_repository import (
    GalleryObservationComponentRootBuilder,
)
from h2hdb.vnext_identity import (
    GalleryObservationComponent,
    GalleryObservationDirectoryEntry,
    GalleryObservationFileEntry,
    GalleryObservationTagEntry,
    build_gallery_observation_metadata_tree,
    build_gallery_observation_tree,
    file_key,
    iter_gallery_observation_metadata_stream,
)
from h2hdb.vnext_source_build_repository import (
    SourceBuildManifestSummary,
    SourceBuildRepository,
)
from h2hdb.vnext_source_observation_spool import _iter_metadata_chunks


def _generated_database(path: Path) -> None:
    open_generated_sqlite_database(path).close()


def _drain_current_only_maintenance(facade: VNextIngestFacade) -> None:
    outcomes: list[VNextCurrentOnlyMaintenanceOutcome] = []
    for _attempt in range(64):
        outcomes.append(facade.drain_current_only_maintenance(1_000_000))
        if outcomes[-1] is VNextCurrentOnlyMaintenanceOutcome.DONE:
            break
    else:
        pytest.fail("current-only maintenance did not finish within 64 attempts")
    assert outcomes[-1] is VNextCurrentOnlyMaintenanceOutcome.DONE
    assert all(
        outcome is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        for outcome in outcomes[:-1]
    )


class _BoundarySource:
    source_root_components = ("manifest-preflight",)

    def __init__(self, file_count: int) -> None:
        self._file_count = file_count
        self._files = tuple(
            FileObservation(
                f"{index:04d}.jpg".encode("ascii"),
                FileContentReceipt.from_parts((index.to_bytes(2, "big"),)),
                1,
                index + 1,
                index,
                index,
            )
            for index in range(file_count)
        )
        self._directories = tuple(
            DirectoryObservation(
                item.name_bytes,
                item.content.size_bytes,
                item.device,
                item.inode,
                item.modified_ns,
                item.changed_ns,
                GalleryObservationDirectoryFileType.REGULAR,
            )
            for item in self._files
        )
        self._tags = tuple(
            TagObservation("artist", f"artist-{index}") for index in range(file_count)
        )
        self._observation = VNextIngestGalleryObservation(
            ("gallery",),
            GalleryObservationMetadata(
                gid=1,
                title="boundary",
                comment="",
                upload_account="uploader",
                upload_time=1,
                download_time=2,
                modified_time=3,
                scan_observation_version=1,
                source_file_count=file_count,
                page_count=file_count,
            ),
        )

    def list_gallery_locators(
        self,
        *,
        after_locator: tuple[str, ...] | None,
        limit: int,
    ) -> VNextIngestPage[tuple[str, ...]]:
        assert after_locator is None and limit == 256
        locators = () if self._file_count == 0 else (("gallery",),)
        return VNextIngestPage(locators, None, True)

    def observe_gallery(
        self,
        locator_components: tuple[str, ...],
    ) -> VNextIngestGalleryObservation:
        assert self._file_count > 0
        assert locator_components == ("gallery",)
        return self._observation

    def list_file_observations(
        self,
        observation: VNextIngestGalleryObservation,
        *,
        after_name_bytes: bytes | None,
        limit: int,
    ) -> VNextIngestPage[FileObservation]:
        assert observation is self._observation and limit == 256
        return self._named_page(self._files, after_name_bytes, limit)

    def list_directory_observations(
        self,
        observation: VNextIngestGalleryObservation,
        *,
        after_name_bytes: bytes | None,
        limit: int,
    ) -> VNextIngestPage[DirectoryObservation]:
        assert observation is self._observation and limit == 192
        return self._named_page(self._directories, after_name_bytes, limit)

    def list_tag_observations(
        self,
        observation: VNextIngestGalleryObservation,
        *,
        after_ordinal: int | None,
        limit: int,
    ) -> VNextIngestPage[TagObservation]:
        assert observation is self._observation and limit == 256
        start = 0 if after_ordinal is None else after_ordinal + 1
        items = self._tags[start : start + limit]
        terminal = start + len(items) == len(self._tags)
        return VNextIngestPage(
            items,
            None if terminal else start + len(items) - 1,
            terminal,
        )

    @staticmethod
    def _named_page[ObservationT: (FileObservation, DirectoryObservation)](
        items: tuple[ObservationT, ...],
        after_name_bytes: bytes | None,
        limit: int,
    ) -> VNextIngestPage[ObservationT]:
        start = (
            0
            if after_name_bytes is None
            else next(
                index + 1
                for index, item in enumerate(items)
                if item.name_bytes == after_name_bytes
            )
        )
        page = items[start : start + limit]
        terminal = start + len(page) == len(items)
        return VNextIngestPage(
            page,
            None if terminal else page[-1].name_bytes,
            terminal,
        )


def _policy() -> VNextIngestPolicy:
    return VNextIngestPolicy(
        producer=VNextArtifactProducer(
            writer_id=b"writer",
            python_abi=b"cp313",
            pillow_build=b"pillow-11",
            libjpeg_build=b"libjpeg-turbo-3",
            zlib_build=b"zlib-1.3",
        ),
        storage=VNextArtifactStoragePolicy(adapter_id=b"managed-filesystem"),
    )


@pytest.mark.parametrize(
    "component",
    (
        GalleryObservationComponent.FILE,
        GalleryObservationComponent.DIRECTORY,
        GalleryObservationComponent.TAG,
    ),
    ids=("file", "directory", "tag"),
)
@pytest.mark.parametrize("item_count", (0, 1, 255, 256, 257))
def test_bounded_component_builder_matches_reference_codec_at_leaf_boundaries(
    component: GalleryObservationComponent,
    item_count: int,
) -> None:
    files = tuple(
        FileObservation(
            f"{index:04d}.jpg".encode("ascii"),
            FileContentReceipt.from_parts((index.to_bytes(2, "big"),)),
            1,
            index + 1,
            index,
            index,
        )
        for index in range(item_count)
    )
    source_entries: tuple[Any, ...]
    oracle_entries: tuple[Any, ...]
    if component is GalleryObservationComponent.FILE:
        source_entries = files
        oracle_entries = tuple(
            GalleryObservationFileEntry(
                index,
                file_key(item.name_bytes),
                item.content.file_sha256,
                item.content.size_bytes,
                item.device,
                item.inode,
                item.modified_ns,
                item.changed_ns,
            )
            for index, item in enumerate(files)
        )
        capacity = 256
    elif component is GalleryObservationComponent.DIRECTORY:
        directories = tuple(
            DirectoryObservation(
                item.name_bytes,
                item.content.size_bytes,
                item.device,
                item.inode,
                item.modified_ns,
                item.changed_ns,
                GalleryObservationDirectoryFileType.REGULAR,
            )
            for item in files
        )
        source_entries = directories
        oracle_entries = tuple(
            GalleryObservationDirectoryEntry(
                index,
                item.name_bytes,
                item.size_bytes,
                item.device,
                item.inode,
                item.modified_ns,
                item.changed_ns,
                item.file_type,
            )
            for index, item in enumerate(directories)
        )
        capacity = 192
    else:
        tags = tuple(
            TagObservation("artist", f"artist-{index:04d}")
            for index in range(item_count)
        )
        source_entries = tags
        oracle_entries = tuple(
            GalleryObservationTagEntry(
                index,
                item.namespace,
                item._value_sha256,
            )
            for index, item in enumerate(tags)
        )
        capacity = 256

    builder = GalleryObservationComponentRootBuilder(component)
    if not source_entries:
        builder.append_page((), terminal=True)
    else:
        for offset in range(0, len(source_entries), capacity):
            page = source_entries[offset : offset + capacity]
            builder.append_page(
                page,
                terminal=offset + len(page) == len(source_entries),
            )
    actual = builder.finish()
    oracle = build_gallery_observation_tree(component, oracle_entries)

    assert (actual.root_page_sha256, actual.item_count) == (
        oracle.root_page_sha256,
        oracle.item_count,
    )
    assert actual.byte_count == (
        2 * item_count if component is GalleryObservationComponent.FILE else 0
    )
    assert actual.regular_count == (
        item_count if component is GalleryObservationComponent.DIRECTORY else 0
    )


@pytest.mark.parametrize("leaf_count", (1, 255, 256, 257))
def test_bounded_metadata_builder_matches_reference_codec_at_frontier_boundaries(
    leaf_count: int,
) -> None:
    base = GalleryObservationMetadata(
        gid=1,
        title="",
        comment="",
        upload_account="",
        upload_time=1,
        download_time=2,
        modified_time=3,
        scan_observation_version=1,
        source_file_count=0,
        page_count=0,
    )
    fixed_size = sum(
        len(part) for part in iter_gallery_observation_metadata_stream(base)
    )
    target_size = leaf_count * 32_768
    assert fixed_size < target_size
    metadata = GalleryObservationMetadata(
        gid=base.gid,
        title=base.title,
        comment="x" * (target_size - fixed_size),
        upload_account=base.upload_account,
        upload_time=base.upload_time,
        download_time=base.download_time,
        modified_time=base.modified_time,
        scan_observation_version=base.scan_observation_version,
        source_file_count=base.source_file_count,
        page_count=base.page_count,
    )
    observation = VNextIngestGalleryObservation(("gallery",), metadata)
    builder = GalleryObservationComponentRootBuilder(
        GalleryObservationComponent.METADATA
    )
    observed_leaf_count = 0
    for chunk, terminal in _iter_metadata_chunks(observation):
        observed_leaf_count += 1
        builder.append_page((chunk,), terminal=terminal)
    actual = builder.finish()
    oracle = build_gallery_observation_metadata_tree(metadata)

    assert observed_leaf_count == leaf_count
    assert (actual.root_page_sha256, actual.item_count) == (
        oracle.root_page_sha256,
        oracle.item_count,
    )


def test_metadata_component_has_no_canonical_empty_leaf() -> None:
    builder = GalleryObservationComponentRootBuilder(
        GalleryObservationComponent.METADATA
    )
    with pytest.raises(ValueError, match="no terminal page"):
        builder.finish()


@pytest.mark.parametrize("file_count", (0, 257), ids=("empty", "leaf-boundary-257"))
def test_preflight_summary_equals_durable_sqlite_build_manifest(
    tmp_path: Path,
    file_count: int,
) -> None:
    path = tmp_path / f"source-preflight-{file_count}.sqlite3"
    _generated_database(path)
    facade = VNextIngestFacade(
        CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path))),
        clock=lambda: 100,
    )
    session = facade.try_claim_ingest(True, 1_000_000)
    assert session is not None
    policy = facade.ensure_policy(session, _policy())

    with facade.prepare_source(_BoundarySource(file_count)) as source:
        expected = source._manifest_summary
        for _step in range(300):
            issued = facade.issue_source_step(session, policy, source)
            prepared = facade.prepare_source_step(source, issued)
            result = facade.commit_source_step(session, prepared)
            if result.terminal:
                break
        else:
            pytest.fail("source manifest boundary did not seal")

    assert result.source_receipt is not None
    with SQLiteConnector(str(path)) as connector:
        durable = connector.fetch_one(
            "SELECT manifest.manifest_sha256, discovery.gallery_count, "
            "manifest.file_count, manifest.byte_count "
            "FROM catalog_build_manifest_core AS manifest "
            "JOIN catalog_source_build_discoveries AS discovery "
            "ON discovery.build_id = manifest.build_id "
            "JOIN catalog_source_build_sealed_ats AS sealed "
            "ON sealed.build_id = manifest.build_id "
            "WHERE manifest.build_id = %s",
            (result.source_receipt.build_id,),
        )
    assert durable == (
        expected.manifest_sha256,
        expected.gallery_count,
        expected.file_count,
        expected.byte_count,
    )


def test_staging_uses_only_frozen_spool_after_prepare_and_close_cleans_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "source-frozen-adapter-disabled.sqlite3"
    _generated_database(path)
    facade = VNextIngestFacade(
        CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path))),
        clock=lambda: 100,
    )
    session = facade.try_claim_ingest(True, 1_000_000)
    assert session is not None
    policy = facade.ensure_policy(session, _policy())
    adapter = _BoundarySource(257)

    source = facade.prepare_source(adapter)
    snapshot_directory = source._snapshot._directory
    expected = source._manifest_summary
    assert snapshot_directory.is_dir()

    def reject_live_read(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("live adapter was read after prepare_source")

    for method_name in (
        "list_gallery_locators",
        "observe_gallery",
        "list_file_observations",
        "list_directory_observations",
        "list_tag_observations",
    ):
        monkeypatch.setattr(adapter, method_name, reject_live_read)

    try:
        for _step in range(300):
            issued = facade.issue_source_step(session, policy, source)
            prepared = facade.prepare_source_step(source, issued)
            result = facade.commit_source_step(session, prepared)
            if result.terminal:
                break
        else:
            pytest.fail("frozen source did not seal")
        assert result.source_receipt is not None
        with SQLiteConnector(str(path)) as connector:
            assert connector.fetch_one(
                "SELECT manifest.manifest_sha256, discovery.gallery_count, "
                "manifest.file_count, manifest.byte_count "
                "FROM catalog_build_manifest_core AS manifest "
                "JOIN catalog_source_build_discoveries AS discovery "
                "ON discovery.build_id = manifest.build_id "
                "JOIN catalog_source_build_sealed_ats AS sealed "
                "ON sealed.build_id = manifest.build_id "
                "WHERE manifest.build_id = %s",
                (result.source_receipt.build_id,),
            ) == (
                expected.manifest_sha256,
                expected.gallery_count,
                expected.file_count,
                expected.byte_count,
            )
    finally:
        source.close()

    assert not snapshot_directory.exists()


def test_live_mutation_after_prepare_stages_the_frozen_snapshot(
    tmp_path: Path,
) -> None:
    path = tmp_path / "source-frozen-live-mutation.sqlite3"
    _generated_database(path)
    facade = VNextIngestFacade(
        CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path))),
        clock=lambda: 100,
    )
    session = facade.try_claim_ingest(True, 1_000_000)
    assert session is not None
    policy = facade.ensure_policy(session, _policy())
    adapter = _BoundarySource(1)

    with facade.prepare_source(adapter) as source:
        frozen = source._manifest_summary
        adapter._observation = VNextIngestGalleryObservation(
            adapter._observation.locator_components,
            replace(adapter._observation.metadata, title="live-B-after-frozen-A"),
        )
        for _step in range(200):
            issued = facade.issue_source_step(session, policy, source)
            prepared = facade.prepare_source_step(source, issued)
            result = facade.commit_source_step(session, prepared)
            if result.terminal:
                break
        else:
            pytest.fail("frozen A source did not seal after live mutation B")

    assert result.source_receipt is not None
    with SQLiteConnector(str(path)) as connector:
        assert connector.fetch_one(
            "SELECT manifest.manifest_sha256, discovery.gallery_count, "
            "manifest.file_count, manifest.byte_count "
            "FROM catalog_build_manifest_core AS manifest "
            "JOIN catalog_source_build_discoveries AS discovery "
            "ON discovery.build_id = manifest.build_id "
            "JOIN catalog_source_build_sealed_ats AS sealed "
            "ON sealed.build_id = manifest.build_id "
            "WHERE manifest.build_id = %s",
            (result.source_receipt.build_id,),
        ) == (
            frozen.manifest_sha256,
            frozen.gallery_count,
            frozen.file_count,
            frozen.byte_count,
        )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_source_build_states WHERE state = 'ABANDONED'"
        ) == (0,)


def test_frozen_pages_replay_exactly_and_metadata_resumes_from_byte_cursor(
    tmp_path: Path,
) -> None:
    path = tmp_path / "source-frozen-page-replay.sqlite3"
    _generated_database(path)
    facade = VNextIngestFacade(
        CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    )
    adapter = _BoundarySource(257)
    adapter._observation = VNextIngestGalleryObservation(
        adapter._observation.locator_components,
        replace(adapter._observation.metadata, title="x" * 40_000),
    )

    with facade.prepare_source(adapter) as source:
        locator = source._plan._page(0)[0]
        components = source._plan._decode_locator(
            locator.position,
            locator.locator_sha256,
        )
        observation = source._snapshot.open_gallery(
            position=locator.position,
            locator_sha256=locator.locator_sha256,
            locator_components=components,
        )

        first = source._snapshot.list_file_observations(
            observation,
            after_name_bytes=None,
            limit=256,
        )
        response_loss_replay = source._snapshot.list_file_observations(
            observation,
            after_name_bytes=None,
            limit=256,
        )
        assert response_loss_replay == first
        assert not first.terminal
        assert isinstance(first.next_after, bytes)
        continuation = source._snapshot.list_file_observations(
            observation,
            after_name_bytes=first.next_after,
            limit=256,
        )
        assert continuation.terminal and len(continuation.items) == 1

        metadata = source._snapshot.iter_metadata_chunks(
            observation,
            start_offset=0,
        )
        first_chunk, first_terminal = next(metadata)
        assert not first_terminal and len(first_chunk) == 32_768
        second_chunk, second_terminal = next(metadata)
        assert second_terminal
        with pytest.raises(StopIteration):
            next(metadata)

        replayed_first = next(
            source._snapshot.iter_metadata_chunks(
                observation,
                start_offset=0,
            )
        )
        resumed_second = next(
            source._snapshot.iter_metadata_chunks(
                observation,
                start_offset=len(first_chunk),
            )
        )
        assert replayed_first == (first_chunk, False)
        assert resumed_second == (second_chunk, True)

        page_counts = source._snapshot._index.execute(
            "SELECT component, COUNT(*) FROM component_pages "
            "GROUP BY component ORDER BY component"
        ).fetchall()
        assert page_counts == [(0, 2), (1, 2), (2, 2), (3, 2)]


def test_manifest_mismatch_abandons_exact_build_and_next_stable_scan_replays(
    tmp_path: Path,
) -> None:
    path = tmp_path / "source-preflight-terminal-mismatch.sqlite3"
    _generated_database(path)
    facade = VNextIngestFacade(
        CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path))),
        clock=lambda: 100,
    )
    session = facade.try_claim_ingest(True, 1_000_000)
    assert session is not None
    policy = facade.ensure_policy(session, _policy())
    adapter = _BoundarySource(1)

    with facade.prepare_source(adapter) as source:
        exact = source._manifest_summary
        source._manifest_summary = SourceBuildManifestSummary(
            sha256(b"defensive-codec-mismatch").digest(),
            exact.gallery_count,
            exact.file_count,
            exact.byte_count,
        )
        for _step in range(200):
            issued = facade.issue_source_step(session, policy, source)
            prepared = facade.prepare_source_step(source, issued)
            try:
                facade.commit_source_step(session, prepared)
            except VNextSourceManifestMismatchError as error:
                assert "differs from its frozen preflight snapshot" in str(error)
                break
        else:
            pytest.fail("changed adapter replay did not reach manifest comparison")

    with SQLiteConnector(str(path)) as connector:
        abandoned_build = connector.fetch_one(
            "SELECT build_id FROM catalog_source_build_states WHERE state = 'ABANDONED'"
        )
        assert len(abandoned_build) == 1
        assert (
            connector.fetch_all("SELECT * FROM operational_source_working_builds") == []
        )
        assert connector.fetch_all("SELECT * FROM catalog_build_manifest_core") == []
        assert connector.fetch_one(
            "SELECT state, processed_gallery_count "
            "FROM operational_source_build_assembly_checkpoints"
        ) == ("OPEN", 1)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_source_build_assembly_batch_receipts "
            "WHERE terminal = 1"
        ) == (0,)

    facade.complete_ingest(session)

    def drive_stable(
        stable_facade: VNextIngestFacade,
    ) -> tuple[bytes, bool]:
        _drain_current_only_maintenance(stable_facade)
        stable_session = stable_facade.try_claim_ingest(True, 1_000_000)
        assert stable_session is not None
        stable_policy = stable_facade.ensure_policy(stable_session, _policy())
        with stable_facade.prepare_source(adapter) as stable_source:
            for _step in range(200):
                issued = stable_facade.issue_source_step(
                    stable_session,
                    stable_policy,
                    stable_source,
                )
                prepared = stable_facade.prepare_source_step(stable_source, issued)
                result = stable_facade.commit_source_step(stable_session, prepared)
                if result.terminal:
                    break
            else:
                pytest.fail("stable source did not seal")
        assert result.source_receipt is not None
        stable_facade.complete_ingest(stable_session)
        return result.source_receipt.build_id, result.replayed

    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    stable_build, stable_replayed = drive_stable(
        VNextIngestFacade(config, clock=lambda: 200),
    )
    assert stable_build != abandoned_build[0]
    assert not stable_replayed
    replay_build, replayed = drive_stable(
        VNextIngestFacade(config, clock=lambda: 300),
    )
    assert replay_build == stable_build
    assert replayed


def test_new_generation_atomically_recovers_stale_open_mismatch_build(
    tmp_path: Path,
) -> None:
    path = tmp_path / "source-preflight-stale-open-recovery.sqlite3"
    _generated_database(path)
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    facade = VNextIngestFacade(config, clock=lambda: 100)
    session = facade.try_claim_ingest(True, 1_000_000)
    assert session is not None
    policy = facade.ensure_policy(session, _policy())
    adapter = _BoundarySource(1)

    with (
        facade.prepare_source(adapter) as source,
        patch.object(
            SourceBuildRepository,
            "abandon",
            side_effect=RuntimeError("lost abandonment response"),
        ),
    ):
        exact = source._manifest_summary
        source._manifest_summary = SourceBuildManifestSummary(
            sha256(b"defensive-response-loss-mismatch").digest(),
            exact.gallery_count,
            exact.file_count,
            exact.byte_count,
        )
        with pytest.raises(RuntimeError, match="lost abandonment response"):
            for _step in range(200):
                issued = facade.issue_source_step(session, policy, source)
                prepared = facade.prepare_source_step(source, issued)
                facade.commit_source_step(session, prepared)

    with SQLiteConnector(str(path)) as connector:
        stale_build = connector.fetch_one(
            "SELECT build_id FROM catalog_source_build_states WHERE state = 'OPEN'"
        )
        assert len(stale_build) == 1
        assert (
            connector.fetch_one(
                "SELECT build_id FROM operational_source_working_builds WHERE slot = 1"
            )
            == stale_build
        )

    facade.complete_ingest(session)
    successor = VNextIngestFacade(config, clock=lambda: 200)
    _drain_current_only_maintenance(successor)
    successor_session = successor.try_claim_ingest(True, 1_000_000)
    assert successor_session is not None
    successor_policy = successor.ensure_policy(successor_session, _policy())
    with successor.prepare_source(adapter) as source:
        for _step in range(200):
            issued = successor.issue_source_step(
                successor_session,
                successor_policy,
                source,
            )
            prepared = successor.prepare_source_step(source, issued)
            result = successor.commit_source_step(successor_session, prepared)
            if result.terminal:
                break
        else:
            pytest.fail("successor source did not recover the stale OPEN build")

    assert result.source_receipt is not None
    assert result.source_receipt.build_id != stale_build[0]
    with SQLiteConnector(str(path)) as connector:
        assert connector.fetch_one(
            "SELECT state FROM catalog_source_build_states WHERE build_id = %s",
            (stale_build[0],),
        ) == ("ABANDONED",)
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_working_builds WHERE slot = 1"
        ) == (result.source_receipt.build_id,)


def test_live_mariadb_manifest_mismatch_abandons_then_stable_source_replays(
    mariadb_config: CoreConfig,
) -> None:
    VNextDatabaseAdminFacade(mariadb_config).initialize()
    adapter = _BoundarySource(1)
    first = VNextIngestFacade(mariadb_config, clock=lambda: 100)
    first_session = first.try_claim_ingest(True, 1_000_000)
    assert first_session is not None
    first_policy = first.ensure_policy(first_session, _policy())

    with first.prepare_source(adapter) as source:
        exact = source._manifest_summary
        source._manifest_summary = SourceBuildManifestSummary(
            sha256(b"mariadb-defensive-codec-mismatch").digest(),
            exact.gallery_count,
            exact.file_count,
            exact.byte_count,
        )
        with pytest.raises(VNextSourceManifestMismatchError):
            for _step in range(200):
                issued = first.issue_source_step(first_session, first_policy, source)
                prepared = first.prepare_source_step(source, issued)
                first.commit_source_step(first_session, prepared)

    context = RepositoryContext.from_config(mariadb_config)
    with context.SQLConnector() as connector, connector.read_transaction():
        abandoned = connector.fetch_one(
            "SELECT build_id FROM catalog_source_build_states WHERE state = 'ABANDONED'"
        )
        assert len(abandoned) == 1
        assert (
            connector.fetch_all("SELECT * FROM operational_source_working_builds") == []
        )
    first.complete_ingest(first_session)

    def stable_turn(now: int) -> tuple[bytes, bool]:
        facade = VNextIngestFacade(mariadb_config, clock=lambda: now)
        _drain_current_only_maintenance(facade)
        session = facade.try_claim_ingest(True, 1_000_000)
        assert session is not None
        policy = facade.ensure_policy(session, _policy())
        with facade.prepare_source(adapter) as source:
            for _step in range(200):
                issued = facade.issue_source_step(session, policy, source)
                prepared = facade.prepare_source_step(source, issued)
                result = facade.commit_source_step(session, prepared)
                if result.terminal:
                    break
            else:
                pytest.fail("stable MariaDB source did not seal")
        assert result.source_receipt is not None
        facade.complete_ingest(session)
        return result.source_receipt.build_id, result.replayed

    stable_build, stable_replayed = stable_turn(200)
    replay_build, replayed = stable_turn(300)
    assert stable_build != abandoned[0]
    assert not stable_replayed
    assert replay_build == stable_build
    assert replayed
