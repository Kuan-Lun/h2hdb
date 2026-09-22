"""Sealed observations survive retries while live artifact bytes remain exact."""

from __future__ import annotations

from collections import Counter
from dataclasses import replace
from typing import BinaryIO, cast

import pytest
from test_vnext_source_batches import (
    _publications,
    _publish_batch,
    _source_batch,
    _source_batch_clock,
)
from test_vnext_source_marker import MarkerSource
from vnext_pipeline import (
    LEASE_MICROSECONDS,
    MemoryLibrary,
    claim_session,
    collect_source,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_analysis,
    run_publication,
)

from h2hdb import (
    ArtifactArchiveRenderEvidence,
    ArtifactSourceMember,
    CatalogRevisionNotFoundError,
    CoreConfig,
    VNextIngestFacade,
    VNextSourceChangedError,
)


class _InterruptedLibrary(MemoryLibrary):
    def __init__(self, source: MarkerSource) -> None:
        super().__init__(source)
        self.interrupt_after_first = False
        self.rendered_gids: list[int] = []
        self.opened_sources: list[tuple[tuple[str, ...], bytes]] = []

    def open_source(
        self,
        *,
        source_root_components: tuple[str, ...],
        gallery_locator_components: tuple[str, ...],
        source_name: bytes,
    ) -> BinaryIO:
        value = self.source.get(gallery_locator_components)
        if (
            self.interrupt_after_first
            and self.rendered_gids
            and value.gid != self.rendered_gids[0]
        ):
            raise VNextSourceChangedError("source is temporarily unavailable")
        self.opened_sources.append((gallery_locator_components, source_name))
        try:
            return super().open_source(
                source_root_components=source_root_components,
                gallery_locator_components=gallery_locator_components,
                source_name=source_name,
            )
        except KeyError as error:
            raise VNextSourceChangedError("source member disappeared") from error

    def render_archive(
        self,
        members: tuple[ArtifactSourceMember, ...],
        destination: BinaryIO,
        *,
        gid: int,
    ) -> ArtifactArchiveRenderEvidence:
        result = super().render_archive(members, destination, gid=gid)
        self.rendered_gids.append(gid)
        return result


def _seal_unpublished(config: CoreConfig, source: MarkerSource) -> None:
    with (
        _source_batch_clock(config) as clock,
        VNextIngestFacade(config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        _source_batch(facade, session, policy, source, None)
        facade.complete_ingest(session)
    source.deep_reads.clear()


@pytest.mark.mariadb_smoke
def test_exact_retry_preserves_prepared_artifacts_without_deep_source_reads(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    values = tuple(gallery(gid) for gid in range(1001, 1004))
    source = MarkerSource(values)
    library = _InterruptedLibrary(source)
    library.interrupt_after_first = True
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        receipt, _deferred = _source_batch(facade, session, policy, source, None)
        run_analysis(facade, session, policy, receipt.build_id)
        with pytest.raises(VNextSourceChangedError, match="temporarily unavailable"):
            run_publication(facade, session, policy, library)
        facade.complete_ingest(session)
        protected_before_maintenance = dict(library.staging)
        facade.drain_current_only_maintenance(
            LEASE_MICROSECONDS,
            artifact_release_adapters={library.adapter_id: library},
        )
        assert library.staging == protected_before_maintenance
        assert not library.release_calls
    assert len(library.rendered_gids) == 1
    with pytest.raises(CatalogRevisionNotFoundError):
        _publications(db_config)
    protected = dict(library.staging)
    assert protected
    assert full_check(db_config).state == "READY"

    source.deep_reads.clear()
    source.forbidden_reads.update(value.locator for value in values)
    library.interrupt_after_first = False
    resumed, _deferred = _publish_batch(
        db_config, source, library, limit=None, artifacts_required=True
    )
    assert resumed.build_id == receipt.build_id
    assert source.deep_reads == []
    assert Counter(library.rendered_gids) == Counter(value.gid for value in values)
    assert {item.gid for item in _publications(db_config)} == {
        value.gid for value in values
    }


@pytest.mark.parametrize("retain_receipt", [True, False])
def test_artifact_live_reads_distinguish_receipt_revalidation_from_spool_hashing(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    retain_receipt: bool,
) -> None:
    initialize_database(db_config)
    original = gallery(1001, pages=[b"exact source page"])
    source = MarkerSource((original,))
    library = _InterruptedLibrary(source)
    if not retain_receipt:
        monkeypatch.setattr(
            "h2hdb.vnext_ingest_publication._MAX_CACHED_ARTIFACT_RESOURCE_BYTES", 0
        )
    _publish_batch(db_config, source, library, limit=None, artifacts_required=True)
    assert Counter(library.opened_sources) == Counter(
        {(original.locator, name): 2 for name in original.files}
    )
    assert library.render_calls == (1 if retain_receipt else 2)


@pytest.mark.parametrize("missing", [False, True])
def test_marker_reuse_cannot_accept_changed_or_missing_artifact_bytes(
    db_config: CoreConfig, missing: bool
) -> None:
    initialize_database(db_config)
    original = gallery(1001, pages=[b"original page"])
    source = MarkerSource((original,))
    _seal_unpublished(db_config, source)
    files = dict(original.files)
    if missing:
        del files[b"000.png"]
    else:
        files[b"000.png"] = b"modified page"
    source.put(replace(original, files=files))
    source.forbidden_reads.add(original.locator)
    library = _InterruptedLibrary(source)
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        receipt, _deferred = _source_batch(facade, session, policy, source, None)
        run_analysis(facade, session, policy, receipt.build_id)
        with pytest.raises(VNextSourceChangedError):
            run_publication(facade, session, policy, library)
        facade.complete_ingest(session)
    assert source.deep_reads == []
    assert not library.rendered_gids
    assert not library.staging
    with pytest.raises(CatalogRevisionNotFoundError):
        _publications(db_config)
    assert full_check(db_config).state == "READY"


def test_accumulated_refresh_hints_observe_only_failed_galleries_across_retries(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    values = tuple(gallery(gid) for gid in range(1001, 1005))
    source = MarkerSource(values)
    _seal_unpublished(db_config, source)
    targets = tuple(value.locator for value in values[:2])
    source.forbidden_reads.update(value.locator for value in values[2:])
    for _cycle in range(3):
        source.deep_reads.clear()
        with (
            _source_batch_clock(db_config) as clock,
            VNextIngestFacade(db_config, clock=clock) as facade,
        ):
            session = claim_session(facade)
            policy = facade.ensure_policy(session, ingest_policy())
            with facade.prepare_source(
                source,
                policy=policy,
                reobserve_gallery_locators=targets,
            ) as prepared:
                collect_source(facade, session, policy, prepared)
                assert prepared.gallery_count == len(values)
            facade.complete_ingest(session)
        assert Counter(source.deep_reads) == Counter(targets)


@pytest.mark.deep
@pytest.mark.parametrize("gallery_count", [127, 128, 129])
def test_targeted_reread_cost_across_source_lookup_page_boundary(
    db_config: CoreConfig, gallery_count: int
) -> None:
    initialize_database(db_config)
    values = tuple(gallery(1001 + index) for index in range(gallery_count))
    source = MarkerSource(values)
    _seal_unpublished(db_config, source)
    targets = (values[0].locator, values[-1].locator)
    for _cycle in range(3):
        source.deep_reads.clear()
        with (
            _source_batch_clock(db_config) as clock,
            VNextIngestFacade(db_config, clock=clock) as facade,
        ):
            session = claim_session(facade)
            policy = facade.ensure_policy(session, ingest_policy())
            with facade.prepare_source(
                source, policy=policy, reobserve_gallery_locators=targets
            ) as prepared:
                collect_source(facade, session, policy, prepared)
                assert prepared.gallery_count == gallery_count
            facade.complete_ingest(session)
        assert Counter(source.deep_reads) == Counter(targets)

    source.deep_reads.clear()
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        with facade.prepare_source(
            source, policy=policy, reuse_sealed_observations=False
        ) as prepared:
            collect_source(facade, session, policy, prepared)
            assert prepared.gallery_count == gallery_count
        facade.complete_ingest(session)
    assert Counter(source.deep_reads) == Counter(value.locator for value in values)
    with pytest.raises(AssertionError):
        assert Counter(source.deep_reads) == Counter(targets)


@pytest.mark.parametrize("count", [127, 128, 129])
def test_refresh_hint_capacity_is_bounded_without_inventing_source_membership(
    db_config: CoreConfig, count: int
) -> None:
    initialize_database(db_config)
    original = gallery(1001)
    source = MarkerSource((original,))
    _seal_unpublished(db_config, source)
    locators = (original.locator,) + tuple(
        (f"absent-{index}",) for index in range(count - 1)
    )
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        if count > 128:
            with pytest.raises(ValueError, match="at most 128"):
                facade.prepare_source(
                    source, policy=policy, reobserve_gallery_locators=locators
                )
            assert source.deep_reads == []
        else:
            with facade.prepare_source(
                source, policy=policy, reobserve_gallery_locators=locators
            ) as prepared:
                collect_source(facade, session, policy, prepared)
                assert prepared.gallery_count == 1
            assert source.deep_reads == [original.locator]
        facade.complete_ingest(session)


@pytest.mark.parametrize("change", ["marker", "qualification_policy"])
def test_unpublished_reuse_requires_exact_marker_and_qualification_policy(
    db_config: CoreConfig, change: str
) -> None:
    initialize_database(db_config)
    values = tuple(gallery(gid) for gid in range(1001, 1004))
    source = MarkerSource(values)
    _seal_unpublished(db_config, source)
    requested = ingest_policy()
    if change == "marker":
        changed = values[0]
        source.put(replace(changed, modified_time=changed.modified_time + 1))
        expected = [changed.locator]
    else:
        requested = replace(
            requested,
            artifact=replace(requested.artifact, policy_fingerprint_sha256=b"q" * 32),
        )
        expected = [value.locator for value in values]
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, requested)
        with facade.prepare_source(source, policy=policy) as prepared:
            collect_source(facade, session, policy, prepared)
            assert prepared.gallery_count == len(values)
        facade.complete_ingest(session)
    assert Counter(source.deep_reads) == Counter(expected)


def test_full_refresh_is_an_explicit_fallback_and_detects_cost_regression(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    values = tuple(gallery(gid) for gid in range(1001, 1005))
    source = MarkerSource(values)
    _seal_unpublished(db_config, source)
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        with facade.prepare_source(
            source, policy=policy, reuse_sealed_observations=False
        ) as prepared:
            collect_source(facade, session, policy, prepared)
            assert prepared.gallery_count == len(values)
        facade.complete_ingest(session)
    assert Counter(source.deep_reads) == Counter(value.locator for value in values)
    # This intentionally degraded path violates the actual targeted-read cost,
    # proving the counter oracle rejects replacing targeted retry by a full scan.
    with pytest.raises(AssertionError):
        assert Counter(source.deep_reads) == Counter((values[0].locator,))


@pytest.mark.parametrize(
    ("locators", "reuse", "error_type"),
    [
        ([("gallery",)], True, TypeError),
        ((["gallery"],), True, TypeError),
        (((),), True, ValueError),
        ((("..",),), True, ValueError),
        ((("gallery",), ("gallery",)), True, ValueError),
        ((("gallery",),), False, ValueError),
        ((), 1, TypeError),
    ],
)
def test_invalid_refresh_hints_fail_before_observing_source(
    db_config: CoreConfig,
    locators: object,
    reuse: object,
    error_type: type[Exception],
) -> None:
    initialize_database(db_config)
    source = MarkerSource((gallery(1001),))
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        with pytest.raises(error_type):
            facade.prepare_source(
                source,
                policy=policy,
                reobserve_gallery_locators=cast(
                    "tuple[tuple[str, ...], ...]", locators
                ),
                reuse_sealed_observations=cast("bool", reuse),
            )
        facade.complete_ingest(session)
    assert source.marker_calls == 0
    assert source.deep_reads == []
