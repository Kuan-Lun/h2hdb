"""Cumulative source batches use published membership as their only checkpoint."""

from __future__ import annotations

from contextlib import closing
from pathlib import Path
from typing import cast

import pytest
from test_vnext_source_marker import MarkerSource
from vnext_pipeline import (
    Clock,
    MemoryLibrary,
    claim_session,
    drain_maintenance,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    library_view,
    run_analysis,
    run_publication,
)

from h2hdb import (
    CatalogPublication,
    CatalogResourceKind,
    CoreConfig,
    DatabaseConfig,
    VNextCatalogFacade,
    VNextIngestFacade,
    VNextIngestPhase,
    VNextIngestSession,
    VNextIngestSourceReceipt,
    VNextResolvedIngestPolicy,
    VNextSourceChangedError,
)


def _source_batch(
    facade: VNextIngestFacade,
    session: VNextIngestSession,
    policy: VNextResolvedIngestPolicy,
    source: MarkerSource,
    limit: int | None,
) -> tuple[VNextIngestSourceReceipt, int]:
    with facade.prepare_source(
        source, policy=policy, max_new_galleries=limit
    ) as prepared:
        deferred = prepared.deferred_gallery_count
        for _ in range(10_000):
            issued = facade.issue_source_step(session, policy, prepared)
            local = facade.prepare_source_step(prepared, issued)
            result = facade.commit_source_step(session, local)
            assert result.phase is VNextIngestPhase.SOURCE
            if result.terminal:
                receipt = result.source_receipt
                assert receipt is not None and receipt.sealed
                return receipt, deferred
    raise AssertionError("source batch did not seal within its step budget")


def _publish_batch(
    config: CoreConfig,
    source: MarkerSource,
    library: MemoryLibrary,
    *,
    limit: int | None,
    artifacts_required: bool = False,
) -> tuple[VNextIngestSourceReceipt, int]:
    # Reopening every turn proves that membership is durable, not a caller cursor.
    with VNextIngestFacade(config, clock=Clock()) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(
            session, ingest_policy(artifacts_required=artifacts_required)
        )
        receipt, deferred = _source_batch(facade, session, policy, source, limit)
        run_analysis(facade, session, policy, receipt.build_id)
        run_publication(facade, session, policy, library)
        facade.complete_ingest(session)
        drain_maintenance(facade)
    assert full_check(config).state == "READY"
    return receipt, deferred


def _publications(config: CoreConfig) -> tuple[CatalogPublication, ...]:
    with closing(VNextCatalogFacade(config)) as catalog:
        revision = catalog.get_catalog_revision()
        page = catalog.discover_publications(revision=revision, limit=128)
        assert page.next_cursor is None
        assert len(page.publications) == revision.publication_count
        return tuple(sorted(page.publications, key=lambda item: item.gid))


@pytest.mark.parametrize(
    "gallery_count",
    (pytest.param(3, marks=pytest.mark.mariadb_smoke), 5),
)
def test_source_batches_accumulate_after_restart_without_reobserving_known_galleries(
    db_config: CoreConfig, gallery_count: int
) -> None:
    initialize_database(db_config)
    galleries = tuple(gallery(1001 + index, pages=[]) for index in range(gallery_count))
    source = MarkerSource(galleries)
    library = MemoryLibrary(source)
    known: set[tuple[str, ...]] = set()
    inventory = {value.locator for value in galleries}

    for admitted in range(2, gallery_count + 2, 2):
        expected = min(admitted, gallery_count)
        source.deep_reads.clear()
        receipt, deferred = _publish_batch(db_config, source, library, limit=2)
        assert receipt.discovered_galleries == expected
        assert receipt.staged_galleries == expected
        assert deferred == gallery_count - expected
        new = set(source.deep_reads)
        assert len(source.deep_reads) == len(new) == expected - len(known)
        assert new <= inventory - known
        known.update(new)
        source.forbidden_reads.update(known)

    assert known == inventory
    source.deep_reads.clear()
    receipt, deferred = _publish_batch(db_config, source, library, limit=2)
    assert receipt.replayed
    assert deferred == 0
    assert source.deep_reads == []


def test_known_changes_and_deletions_do_not_consume_new_gallery_budget(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    first, removed, waiting, last = (
        gallery(gid, title=f"Original {gid}") for gid in (1001, 1002, 1003, 1004)
    )
    source = MarkerSource((first, removed))
    library = MemoryLibrary(source)
    receipt, deferred = _publish_batch(db_config, source, library, limit=2)
    assert (receipt.discovered_galleries, deferred) == (2, 0)

    changed = gallery(
        first.gid,
        title="Updated known gallery",
        modified_time=first.modified_time + 1,
    )
    inserted = gallery(1000, title="Inserted before the old discovery position")
    source.put(changed)
    source.put(inserted)
    source.put(waiting)
    source.put(last)
    source.remove(removed.locator)
    source.deep_reads.clear()
    receipt, deferred = _publish_batch(db_config, source, library, limit=1)
    assert (receipt.discovered_galleries, deferred) == (2, 2)
    additions = {value.locator: value for value in (inserted, waiting, last)}
    assert len(source.deep_reads) == 2
    assert changed.locator in source.deep_reads
    admitted = set(source.deep_reads) - {changed.locator}
    assert len(admitted) == 1 and admitted <= additions.keys()
    expected_titles = {changed.gid: changed.title}
    expected_titles.update(
        (additions[locator].gid, additions[locator].title) for locator in admitted
    )
    assert {
        item.gid: item.title for item in _publications(db_config)
    } == expected_titles

    source.forbidden_reads.update((changed.locator, *admitted))
    source.deep_reads.clear()
    receipt, deferred = _publish_batch(db_config, source, library, limit=2)
    assert (receipt.discovered_galleries, deferred) == (4, 0)
    assert set(source.deep_reads) == additions.keys() - admitted
    assert {item.gid for item in _publications(db_config)} == {1000, 1001, 1003, 1004}


def test_unpublished_source_cache_cannot_bypass_new_gallery_quota(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    galleries = tuple(gallery(gid, pages=[]) for gid in range(1001, 1006))
    source = MarkerSource(galleries)
    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        receipt, deferred = _source_batch(facade, session, policy, source, None)
        assert (receipt.discovered_galleries, deferred) == (5, 0)
        source.deep_reads.clear()
        source.forbidden_reads.update(value.locator for value in galleries)

        # All five observations are durable, but none has entered a published head.
        with facade.prepare_source(
            source, policy=policy, max_new_galleries=2
        ) as prepared:
            assert prepared.deferred_gallery_count == 3
            assert source.deep_reads == []
        facade.complete_ingest(session)


@pytest.mark.parametrize("limit", [None, 2])
def test_batch_rejects_a_preparation_from_before_another_publication(
    db_config: CoreConfig,
    limit: int | None,
) -> None:
    initialize_database(db_config)
    source = MarkerSource(tuple(gallery(gid) for gid in range(1001, 1004)))
    library = MemoryLibrary(source)
    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        facade.complete_ingest(session)
        with facade.prepare_source(
            source, policy=policy, max_new_galleries=limit
        ) as stale:
            receipt, deferred = _publish_batch(db_config, source, library, limit=1)
            assert (receipt.discovered_galleries, deferred) == (1, 2)
            published = _publications(db_config)
            assert len(published) == 1
            session = claim_session(facade)
            policy = facade.ensure_policy(
                session, ingest_policy(artifacts_required=False)
            )
            with pytest.raises(VNextSourceChangedError, match="publication"):
                issued = facade.issue_source_step(session, policy, stale)
                local = facade.prepare_source_step(stale, issued)
                facade.commit_source_step(session, local)
            assert _publications(db_config) == published
            facade.complete_ingest(session)


def test_cumulative_batches_rebuild_global_duplicates_like_a_complete_ingest(
    db_config: CoreConfig, tmp_path: Path
) -> None:
    initialize_database(db_config)
    shared = b"shared page whose third distinct artist makes it spam"
    galleries = tuple(
        gallery(
            1001 + index,
            pages=(shared, f"unique-{index}".encode()),
            artists=(artist,),
        )
        for index, artist in enumerate(("ann", "ben", "cid"))
    ) + (gallery(1004, pages=(shared, b"unique-0"), artists=("ann",)),)
    source = MarkerSource(galleries)
    library = MemoryLibrary(source)
    receipt, deferred = _publish_batch(
        db_config, source, library, limit=2, artifacts_required=True
    )
    assert (receipt.discovered_galleries, deferred) == (2, 2)
    first_publications = _publications(db_config)
    first_objects = dict(library.current)
    assert any(item.page_count == 2 for item in first_publications)
    first_members = set(source.deep_reads)
    assert len(first_members) == 2
    source.forbidden_reads.update(first_members)
    source.deep_reads.clear()

    receipt, deferred = _publish_batch(
        db_config, source, library, limit=2, artifacts_required=True
    )
    assert (receipt.discovered_galleries, deferred) == (4, 0)
    assert (
        set(source.deep_reads) == {value.locator for value in galleries} - first_members
    )
    assert tuple(item.page_count for item in _publications(db_config)) == (1, 1, 1)
    for key, descriptor in first_objects.items():
        if key[1] is CatalogResourceKind.ACQUISITION:
            assert (
                key not in library.current
                or library.current[key].sha256 != descriptor.sha256
            )

    reference = CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite", database=str(tmp_path / "whole-source.sqlite3")
        )
    )
    initialize_database(reference)
    complete_source = MarkerSource(source.galleries)
    complete_library = MemoryLibrary(complete_source)
    _publish_batch(
        reference,
        complete_source,
        complete_library,
        limit=None,
        artifacts_required=True,
    )
    assert _publications(db_config) == _publications(reference)
    assert library_view(library) == library_view(complete_library)
    assert library.objects == complete_library.objects


@pytest.mark.parametrize("invalid", (True, False, 0, -1, 1_000_001, 1.5, "2"))
def test_invalid_source_batch_limit_fails_before_discovery(
    sqlite_config: CoreConfig, invalid: object
) -> None:
    source = MarkerSource((gallery(1001),))
    with VNextIngestFacade(sqlite_config) as facade:
        with pytest.raises(ValueError, match="max_new_galleries"):
            facade.prepare_source(
                source,
                policy=cast(VNextResolvedIngestPolicy, object()),
                max_new_galleries=cast(int, invalid),
            )
    assert source.page_calls == 0
    assert source.deep_reads == []
    assert not Path(sqlite_config.database.database).exists()
