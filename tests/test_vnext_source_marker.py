"""Public source-to-publication evidence for durable completion-marker reuse."""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import closing
from dataclasses import replace
from typing import Literal

import pytest
from vnext_fault_harness import (
    FaultInjector,
    InjectedFault,
    fault_injection,
    open_connector,
    snapshot_database,
)
from vnext_pipeline import (
    METADATA_NAME,
    Clock,
    MemoryGallery,
    MemoryLibrary,
    MemorySource,
    claim_session,
    drain_maintenance,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_ingest_turn,
    takeover_clock,
)

from h2hdb import (
    ArtifactSourceRole,
    CoreConfig,
    DirectoryObservation,
    FileContentReceipt,
    FileObservation,
    VNextIngestFacade,
    VNextIngestGalleryObservation,
    VNextIngestPage,
    VNextSourceChangedError,
    VNextSourceCompletionMarker,
)
from h2hdb.vnext_ingest_fence_repository import IngestFenceUnavailableError
from h2hdb.vnext_maintenance_gate_repository import MaintenanceGateUnavailableError
from h2hdb.vnext_source_marker_repository import SourceMarkerConflictError


class MarkerSource(MemorySource):
    """Count deep reads separately from fresh, byte-backed marker probes."""

    def __init__(self, galleries: Sequence[MemoryGallery] = ()) -> None:
        super().__init__(galleries)
        self.observation_version = 1
        self.marker_stats: dict[tuple[str, ...], tuple[int, int, int, int]] = {}
        self.deep_reads: list[tuple[str, ...]] = []
        self.forbidden_reads: set[tuple[str, ...]] = set()
        self.marker_calls = 0
        self.change_during_observation = False

    def observe_completion_marker(
        self, locator_components: tuple[str, ...]
    ) -> VNextSourceCompletionMarker:
        self.marker_calls += 1
        value = self.get(locator_components)
        device, inode, modified, changed = self.marker_stats.get(
            locator_components, self._stat(value, METADATA_NAME, directory=False)
        )
        return VNextSourceCompletionMarker(
            FileObservation(
                METADATA_NAME,
                FileContentReceipt.from_parts((value.files[METADATA_NAME],)),
                ArtifactSourceRole.METADATA,
                device,
                inode,
                modified,
                changed,
            ),
            self.observation_version,
        )

    def observe_gallery(
        self, locator_components: tuple[str, ...]
    ) -> VNextIngestGalleryObservation:
        assert locator_components not in self.forbidden_reads, (
            "unchanged gallery was deeply observed"
        )
        self.deep_reads.append(locator_components)
        observed = super().observe_gallery(locator_components)
        if self.change_during_observation:
            value = self.get(locator_components)
            self.put(replace(value, modified_time=value.modified_time + 1))
        return replace(
            observed,
            metadata=replace(
                observed.metadata, scan_observation_version=self.observation_version
            ),
        )

    def list_file_observations(
        self,
        observation: VNextIngestGalleryObservation,
        *,
        after_name_bytes: bytes | None,
        limit: int,
    ) -> VNextIngestPage[FileObservation]:
        page = super().list_file_observations(
            observation, after_name_bytes=after_name_bytes, limit=limit
        )
        marker = self.observe_completion_marker(observation.locator_components)
        return replace(
            page,
            items=tuple(
                marker.file if item.name_bytes == METADATA_NAME else item
                for item in page.items
            ),
        )

    def list_directory_observations(
        self,
        observation: VNextIngestGalleryObservation,
        *,
        after_name_bytes: bytes | None,
        limit: int,
    ) -> VNextIngestPage[DirectoryObservation]:
        page = super().list_directory_observations(
            observation, after_name_bytes=after_name_bytes, limit=limit
        )
        marker = self.observe_completion_marker(observation.locator_components).file
        return replace(
            page,
            items=tuple(
                replace(
                    item,
                    device=marker.device,
                    inode=marker.inode,
                    modified_ns=marker.modified_ns,
                    changed_ns=marker.changed_ns,
                )
                if item.name_bytes == METADATA_NAME
                else item
                for item in page.items
            ),
        )


def _turn(config: CoreConfig, source: MarkerSource, library: MemoryLibrary) -> bool:
    with VNextIngestFacade(config, clock=Clock()) as facade:
        receipts = run_ingest_turn(
            facade,
            source=source,
            library=library,
            policy=ingest_policy(artifacts_required=False),
        )
        drain_maintenance(facade)
    assert full_check(config).state == "READY"
    with closing(open_connector(config)) as connector:
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_source_build_galleries WHERE build_id = %s",
            (receipts.source.build_id,),
        ) == (len(source.galleries),)
    return receipts.source.replayed


@pytest.mark.mariadb_smoke
def test_marker_cache_survives_restart_and_reuses_prior_membership(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    original = gallery(1001, pages=[])
    first = MarkerSource([original])
    library = MemoryLibrary(first)
    assert not _turn(db_config, first, library)
    assert first.deep_reads == [original.locator]

    added = gallery(1002, pages=[])
    restarted = MarkerSource([original, added])
    library.source = restarted
    restarted.forbidden_reads.add(original.locator)
    assert not _turn(db_config, restarted, library)
    assert restarted.deep_reads == [added.locator]

    restarted.forbidden_reads.add(added.locator)
    restarted.deep_reads.clear()
    assert _turn(db_config, restarted, library)
    assert restarted.deep_reads == []
    assert restarted.marker_calls >= 4


@pytest.mark.parametrize(
    "change", ["mtime", "ctime", "device", "inode", "hash", "size", "version"]
)
def test_marker_evidence_changes_invalidate_only_the_changed_gallery(
    db_config: CoreConfig,
    change: Literal["mtime", "ctime", "device", "inode", "hash", "size", "version"],
) -> None:
    initialize_database(db_config)
    target, untouched = gallery(1001, pages=[]), gallery(1002, pages=[])
    source = MarkerSource([target, untouched])
    library = MemoryLibrary(source)
    _turn(db_config, source, library)
    source.deep_reads.clear()
    marker = source.observe_completion_marker(target.locator)
    if change in {"mtime", "ctime", "device", "inode"}:
        values = [
            marker.file.device,
            marker.file.inode,
            marker.file.modified_ns,
            marker.file.changed_ns,
        ]
        values[{"device": 0, "inode": 1, "mtime": 2, "ctime": 3}[change]] += 1
        source.marker_stats[target.locator] = (
            values[0],
            values[1],
            values[2],
            values[3],
        )
    elif change in {"hash", "size"}:
        contents = target.files[METADATA_NAME]
        replacement = (
            contents.replace(b"Title", b"TITLE")
            if change == "hash"
            else contents + b"\n"
        )
        source.put(replace(target, files={**target.files, METADATA_NAME: replacement}))
    else:
        source.observation_version += 1
    if change != "version":
        source.forbidden_reads.add(untouched.locator)

    assert not _turn(db_config, source, library)
    assert set(source.deep_reads) == (
        {target.locator, untouched.locator} if change == "version" else {target.locator}
    )


def test_marker_rewrite_with_identical_content_still_refreshes_image_observations(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    original = gallery(1001, pages=[b"old image"])
    source = MarkerSource([original])
    library = MemoryLibrary(source)
    _turn(db_config, source, library)
    before = source.observe_completion_marker(original.locator)
    source.put(
        replace(
            original,
            files={**original.files, b"000.png": b"new image"},
            modified_time=original.modified_time + 1,
        )
    )
    after = source.observe_completion_marker(original.locator)
    assert before.file.content == after.file.content
    assert before != after
    source.deep_reads.clear()

    assert not _turn(db_config, source, library)
    assert source.deep_reads == [original.locator]


def test_marker_change_during_preparation_does_not_seed_a_cache_entry(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    original = gallery(1001, pages=[])
    source = MarkerSource([original])
    library = MemoryLibrary(source)
    source.change_during_observation = True
    with VNextIngestFacade(db_config) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        with pytest.raises(VNextSourceChangedError, match="marker changed"):
            facade.prepare_source(source, policy=policy)
        facade.complete_ingest(session)

    source.change_during_observation = False
    source.deep_reads.clear()
    assert not _turn(db_config, source, library)
    assert source.deep_reads == [original.locator]


def test_marker_cache_keeps_complete_source_membership_and_reclaims_removed_gallery(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    first, removed, retained = (gallery(gid, pages=[]) for gid in (1001, 1002, 1003))
    source = MarkerSource([first, removed, retained])
    library = MemoryLibrary(source)
    _turn(db_config, source, library)
    source.remove(removed.locator)
    source.forbidden_reads.update((first.locator, retained.locator))
    source.deep_reads.clear()

    assert not _turn(db_config, source, library)
    assert source.deep_reads == []


@pytest.mark.parametrize("field", ["byte_count", "gallery_id"])
def test_completion_marker_rejects_corrupt_authority(
    db_config: CoreConfig, field: str
) -> None:
    initialize_database(db_config)
    original = gallery(1001, pages=[])
    source = MarkerSource([original])
    library = MemoryLibrary(source)
    _turn(db_config, source, library)
    source.put(gallery(1002, pages=[]))
    source.forbidden_reads.add(original.locator)

    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        with facade.prepare_source(source, policy=policy) as prepared:
            for _ in range(512):
                issued = facade.issue_source_step(session, policy, prepared)
                local = facade.prepare_source_step(prepared, issued)
                if issued._action.value == "STAGING_REUSE":
                    observation = prepared._machine.observation
                    assert observation is not None and observation.cached is not None
                    cached = observation.cached
                    object.__setattr__(cached, field, getattr(cached, field) + 1)
                    before = snapshot_database(db_config)
                    with pytest.raises(SourceMarkerConflictError, match="durable"):
                        facade.commit_source_step(session, local)
                    assert snapshot_database(db_config) == before
                    break
                assert not facade.commit_source_step(session, local).terminal
            else:
                pytest.fail("source did not reuse the retained observation")


@pytest.mark.parametrize("fault", ["rollback", "response_loss", "takeover"])
def test_cached_source_step_recovers_commit_faults_and_rejects_stale_owner(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    initialize_database(db_config)
    original = gallery(1001, pages=[])
    source = MarkerSource([original])
    library = MemoryLibrary(source)
    _turn(db_config, source, library)
    source.put(gallery(1002, pages=[]))
    source.forbidden_reads.add(original.locator)
    injector = FaultInjector()
    with (
        fault_injection(monkeypatch, injector),
        VNextIngestFacade(db_config, clock=Clock()) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        with facade.prepare_source(source, policy=policy) as prepared:
            checked = False
            for _ in range(512):
                issued = facade.issue_source_step(session, policy, prepared)
                local = facade.prepare_source_step(prepared, issued)
                if issued._action.value == "STAGING_REUSE" and not checked:
                    checked = True
                    if fault == "takeover":
                        with VNextIngestFacade(
                            db_config, clock=takeover_clock()
                        ) as next_facade:
                            successor = claim_session(next_facade)
                            before = snapshot_database(db_config)
                            with pytest.raises(
                                (
                                    IngestFenceUnavailableError,
                                    MaintenanceGateUnavailableError,
                                )
                            ):
                                facade.commit_source_step(session, local)
                            assert snapshot_database(db_config) == before
                            next_facade.complete_ingest(successor)
                        break
                    before = snapshot_database(db_config)
                    if fault == "rollback":
                        injector.fail_before_mutation = injector.mutations + 1
                    else:
                        injector.fail_after_commit = injector.commits + 1
                    with pytest.raises(InjectedFault):
                        facade.commit_source_step(session, local)
                    committed = snapshot_database(db_config)
                    if fault == "rollback":
                        assert committed == before
                    else:
                        assert committed != before
                    injector.fail_before_mutation = None
                    injector.fail_after_commit = None
                    result = facade.commit_source_step(session, local)
                    assert result.replayed == (fault == "response_loss")
                    if fault == "response_loss":
                        assert snapshot_database(db_config) == committed
                    continue
                result = facade.commit_source_step(session, local)
                if result.terminal:
                    facade.complete_ingest(session)
                    break
            else:
                pytest.fail("source did not finish or reach the stale owner boundary")
            assert checked
    assert full_check(db_config).state == "READY"
