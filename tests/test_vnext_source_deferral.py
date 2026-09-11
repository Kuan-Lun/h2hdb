"""Independent source observations converge without deleting updating galleries."""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import closing
from typing import Literal

import pytest
from test_vnext_source_batches import _publications, _publish_batch, _source_batch
from test_vnext_source_marker import MarkerSource
from vnext_fault_harness import open_connector
from vnext_pipeline import (
    Clock,
    MemoryGallery,
    MemoryLibrary,
    claim_session,
    gallery,
    ingest_policy,
    initialize_database,
    library_view,
)

from h2hdb import (
    CoreConfig,
    DirectoryObservation,
    VNextIngestFacade,
    VNextIngestGalleryObservation,
    VNextIngestPage,
    VNextSourceChangedError,
    VNextSourceCompletionMarker,
    VNextSourceDeferredError,
)
from h2hdb.vnext_identity import source_relative_locator_digest


class UpdatingSource(MarkerSource):
    def __init__(self, galleries: Sequence[MemoryGallery] = ()) -> None:
        super().__init__(galleries)
        self.waiting: set[tuple[str, ...]] = set()
        self.omitted: set[tuple[str, ...]] = set()
        self.presence_probes: list[tuple[str, ...]] = []
        self.defer_at: Literal["marker", "directory"] = "marker"
        self.global_failure = False

    def gallery_exists(self, locator_components: tuple[str, ...]) -> bool:
        self.presence_probes.append(locator_components)
        return super().gallery_exists(locator_components)

    def list_gallery_locators(
        self,
        *,
        after_locator: tuple[str, ...] | None,
        limit: int,
    ) -> VNextIngestPage[tuple[str, ...]]:
        keys = tuple(
            value.locator
            for value in self.galleries
            if value.locator not in self.omitted
            and (after_locator is None or value.locator > after_locator)
        )
        terminal = len(keys) <= limit
        items = keys[:limit]
        return VNextIngestPage(items, None if terminal else items[-1], terminal)

    def observe_completion_marker(
        self, locator_components: tuple[str, ...]
    ) -> VNextSourceCompletionMarker:
        if self.global_failure:
            raise VNextSourceChangedError("source root was replaced")
        if locator_components in self.waiting and self.defer_at == "marker":
            raise VNextSourceDeferredError("producer has not completed this gallery")
        return super().observe_completion_marker(locator_components)

    def list_directory_observations(
        self,
        observation: VNextIngestGalleryObservation,
        *,
        after_name_bytes: bytes | None,
        limit: int,
    ) -> VNextIngestPage[DirectoryObservation]:
        if (
            observation.locator_components in self.waiting
            and self.defer_at == "directory"
        ):
            # FILE pages have already been written into the provisional spool.
            raise VNextSourceDeferredError("gallery changed after file observation")
        return super().list_directory_observations(
            observation, after_name_bytes=after_name_bytes, limit=limit
        )


@pytest.mark.parametrize(
    "defer_at", [pytest.param("marker", marks=pytest.mark.mariadb_smoke), "directory"]
)
def test_updating_gallery_keeps_published_version_while_other_galleries_advance(
    db_config: CoreConfig, defer_at: Literal["marker", "directory"]
) -> None:
    initialize_database(db_config)
    original = gallery(1001, title="Original", pages=[b"original page"])
    source = UpdatingSource([original])
    library = MemoryLibrary(source)
    _publish_batch(db_config, source, library, limit=1, artifacts_required=True)
    original_publication = _publications(db_config)[0]
    original_library = library_view(library)

    replacement = gallery(
        1001, title="Updated", pages=[b"updated page"], modified_time=9
    )
    ready = gallery(1002, title="Independent", pages=[b"independent page"])
    source.put(replacement)
    source.put(ready)
    source.waiting.add(original.locator)
    source.defer_at = defer_at
    receipt, quota = _publish_batch(
        db_config, source, library, limit=1, artifacts_required=True
    )
    assert (receipt.discovered_galleries, quota) == (2, 0)
    publications = _publications(db_config)
    assert [item.title for item in publications] == ["Original", "Independent"]
    assert (
        publications[0].artifacts[0].artifact_id
        == original_publication.artifacts[0].artifact_id
    )
    assert original_library.items() <= library_view(library).items()

    # Reopening the facade on each batch recovers solely from published authority.
    source.waiting.clear()
    _publish_batch(db_config, source, library, limit=1, artifacts_required=True)
    publications = _publications(db_config)
    assert [item.title for item in publications] == ["Updated", "Independent"]
    assert (
        publications[0].artifacts[0].artifact_id
        != original_publication.artifacts[0].artifact_id
    )


@pytest.mark.parametrize("limit", [None, 1])
def test_incomplete_new_gallery_does_not_consume_admission_budget(
    db_config: CoreConfig, limit: int | None
) -> None:
    initialize_database(db_config)
    values = sorted(
        (gallery(gid) for gid in range(1001, 1004)),
        key=lambda value: source_relative_locator_digest(
            "source_relative_locator_v1", value.locator
        ),
    )
    waiting, ready, later = values
    source = UpdatingSource(values)
    source.waiting.add(waiting.locator)
    source.defer_at = "directory"
    library = MemoryLibrary(source)
    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        with facade.prepare_source(
            source, policy=policy, max_new_galleries=limit
        ) as prepared:
            assert prepared.waiting_gallery_count == 1
            assert prepared.deferred_gallery_count == (0 if limit is None else 1)
        facade.complete_ingest(session)
    receipt, quota = _publish_batch(db_config, source, library, limit=limit)
    assert receipt.discovered_galleries == (2 if limit is None else 1)
    assert quota == (0 if limit is None else 1)
    expected = {ready.gid, later.gid} if limit is None else {ready.gid}
    assert {item.gid for item in _publications(db_config)} == expected
    source.waiting.clear()
    _publish_batch(db_config, source, library, limit=None)
    assert {item.gid for item in _publications(db_config)} == {
        value.gid for value in values
    }


@pytest.mark.parametrize("waiting_index", [0, 2])
def test_marker_deferred_gallery_is_waiting_even_after_admission_quota_is_full(
    db_config: CoreConfig, waiting_index: int
) -> None:
    initialize_database(db_config)
    values = sorted(
        (gallery(gid) for gid in range(1001, 1004)),
        key=lambda value: source_relative_locator_digest(
            "source_relative_locator_v1", value.locator
        ),
    )
    source = UpdatingSource(values)
    source.waiting.add(values[waiting_index].locator)
    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        with facade.prepare_source(
            source, policy=policy, max_new_galleries=1
        ) as prepared:
            assert prepared.gallery_count == 1
            assert prepared.waiting_gallery_count == 1
            assert prepared.deferred_gallery_count == 1
        facade.complete_ingest(session)


def test_discovery_omission_requires_fresh_absence_before_deleting_published_gallery(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    kept, removed = gallery(1001), gallery(1002)
    source = UpdatingSource([kept, removed])
    library = MemoryLibrary(source)
    _publish_batch(db_config, source, library, limit=None)
    source.omitted.update((kept.locator, removed.locator))
    source.remove(removed.locator)
    source.waiting.add(kept.locator)
    _publish_batch(db_config, source, library, limit=None)
    assert {item.gid for item in _publications(db_config)} == {kept.gid}
    assert set(source.presence_probes) == {kept.locator, removed.locator}


def test_unpublished_newer_marker_binding_cannot_replace_deferred_published_observation(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    original = gallery(1001, title="Published")
    source = UpdatingSource([original])
    library = MemoryLibrary(source)
    _publish_batch(db_config, source, library, limit=None)
    source.put(gallery(1001, title="Unpublished", modified_time=9))
    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        _source_batch(facade, session, policy, source, None)
        facade.complete_ingest(session)
    source.waiting.add(original.locator)
    source.put(gallery(1002, title="Ready"))
    _publish_batch(db_config, source, library, limit=None)
    assert [item.title for item in _publications(db_config)] == ["Published", "Ready"]


@pytest.mark.parametrize("artifacts_required", [False, True])
def test_unpublished_source_seal_is_freshly_observed_after_restart_when_artifacts_are_required(
    db_config: CoreConfig, artifacts_required: bool
) -> None:
    initialize_database(db_config)
    original = gallery(1001)
    source = UpdatingSource([original])
    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(
            session, ingest_policy(artifacts_required=artifacts_required)
        )
        _source_batch(facade, session, policy, source, None)
        facade.complete_ingest(session)
    source.deep_reads.clear()
    with VNextIngestFacade(db_config, clock=Clock()) as restarted:
        session = claim_session(restarted)
        policy = restarted.ensure_policy(
            session, ingest_policy(artifacts_required=artifacts_required)
        )
        with restarted.prepare_source(source, policy=policy) as prepared:
            assert prepared.gallery_count == 1
        restarted.complete_ingest(session)
    assert source.deep_reads == ([original.locator] if artifacts_required else [])


def test_deferred_gallery_cannot_claim_replacement_qualification_policy(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    original = gallery(1001)
    source = UpdatingSource([original])
    library = MemoryLibrary(source)
    _publish_batch(db_config, source, library, limit=None, artifacts_required=False)
    source.waiting.add(original.locator)
    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=True))
        with pytest.raises(VNextSourceChangedError, match="current completion policy"):
            facade.prepare_source(source, policy=policy)
        facade.complete_ingest(session)
    assert [item.title for item in _publications(db_config)] == [original.title]


def test_source_wide_failure_is_not_swallowed_as_gallery_deferral(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    source = UpdatingSource([gallery(1001)])
    source.global_failure = True
    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        with pytest.raises(VNextSourceChangedError, match="root was replaced"):
            facade.prepare_source(source, policy=policy)
        facade.complete_ingest(session)
    with closing(open_connector(db_config)) as connector:
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_gallery_observations"
        ) == (0,)
