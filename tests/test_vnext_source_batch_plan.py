"""Bounded admission retains every published member of a fresh inventory."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import closing

import pytest
from vnext_pipeline import MemorySource, gallery

from h2hdb import (
    VNextIngestGalleryObservation,
    VNextSourcePreparationOperation,
    VNextSourcePreparationProgress,
)
from h2hdb.vnext_source_build_repository import (
    SourceDiscoveryPlan,
    SourceDiscoveryPlanError,
)
from h2hdb.vnext_source_observation_spool import (
    FrozenSourceObservationError,
    FrozenSourceObservationSpool,
)


def _locators(plan: SourceDiscoveryPlan) -> Iterator[tuple[str, ...]]:
    position = 0
    while position < plan.gallery_count:
        page = plan._page(position)
        for item in page:
            yield plan._decode_locator(item.position, item.locator_sha256)
        position += len(page)


def test_admission_limit_never_truncates_later_known_members() -> None:
    galleries = tuple(gallery(index + 1, pages=[]) for index in range(385))
    source = MemorySource(galleries)
    inventory = tuple(value.locator for value in galleries)
    known = set(inventory[::2])
    calls: list[int] = []

    def membership(page: tuple[tuple[str, ...], ...]) -> tuple[bool, ...]:
        calls.append(len(page))
        return tuple(locator in known for locator in page)

    with SourceDiscoveryPlan.from_locators(inventory) as full:
        ordered_new = tuple(
            locator for locator in _locators(full) if locator not in known
        )
        with closing(
            FrozenSourceObservationSpool.freeze(
                source,
                plan=full,
                source_root_components=source.source_root_components,
                max_new_galleries=3,
                membership_lookup=membership,
            )
        ) as batch:
            assert set(batch.selected_locators()) == known | set(ordered_new[:3])
            assert batch.manifest_summary.gallery_count == len(known) + 3
            assert batch.deferred_gallery_count == len(ordered_new) - 3
            assert batch.waiting_gallery_count == 0
    assert sum(calls) == len(inventory)
    assert max(calls) <= 128


def test_empty_inventory_does_not_invent_deferred_work() -> None:
    with SourceDiscoveryPlan.from_locators(()) as full:
        source = MemorySource()
        with closing(
            FrozenSourceObservationSpool.freeze(
                source,
                plan=full,
                source_root_components=source.source_root_components,
                max_new_galleries=1,
            )
        ) as batch:
            assert batch.manifest_summary.gallery_count == 0
            assert batch.deferred_gallery_count == 0
            assert batch.waiting_gallery_count == 0


def test_complete_inventory_is_validated_before_admission() -> None:
    def repeated() -> Iterator[tuple[str, ...]]:
        yield ("first",)
        yield ("deferred",)
        yield ("deferred",)

    with pytest.raises(SourceDiscoveryPlanError, match="duplicate"):
        SourceDiscoveryPlan.from_locators(repeated())


def test_membership_page_must_be_exact() -> None:
    with SourceDiscoveryPlan.from_locators((("gallery",),)) as full:
        with pytest.raises(ValueError, match="invalid page"):
            source = MemorySource()
            FrozenSourceObservationSpool.freeze(
                source,
                plan=full,
                source_root_components=source.source_root_components,
                max_new_galleries=1,
                membership_lookup=lambda _: (),
            )


class _CountedSource(MemorySource):
    def __init__(self, count: int) -> None:
        super().__init__(tuple(gallery(index + 1, pages=[]) for index in range(count)))
        self.observed: list[tuple[str, ...]] = []

    def observe_gallery(
        self, locator_components: tuple[str, ...]
    ) -> VNextIngestGalleryObservation:
        self.observed.append(locator_components)
        return super().observe_gallery(locator_components)


@pytest.mark.parametrize("count", (127, 128, 129))
def test_incremental_freeze_yields_before_observing_the_next_gallery(
    count: int,
) -> None:
    source = _CountedSource(count)
    with SourceDiscoveryPlan.from_locators(
        value.locator for value in source.galleries
    ) as plan:
        with closing(
            FrozenSourceObservationSpool.start(
                source,
                plan=plan,
                source_root_components=source.source_root_components,
            )
        ) as spool:
            assert source.observed == []
            for completed in range(count):
                item = spool.freeze_next()
                assert item is not None
                assert len(source.observed) == completed + 1
                assert spool.manifest_summary.gallery_count == completed + 1
                assert not spool.complete
            assert spool.freeze_next() is None
            assert spool.complete
            assert len(set(source.observed)) == count
            with pytest.raises(ValueError, match="already complete"):
                spool.freeze_next()


def test_incremental_freeze_distinguishes_deferred_entry_from_completion() -> None:
    source = _CountedSource(3)
    with SourceDiscoveryPlan.from_locators(
        value.locator for value in source.galleries
    ) as plan:
        with closing(
            FrozenSourceObservationSpool.start(
                source,
                plan=plan,
                source_root_components=source.source_root_components,
                max_new_galleries=1,
            )
        ) as spool:
            assert spool.freeze_next() is not None
            for deferred in (1, 2):
                assert spool.freeze_next() is None
                assert not spool.complete
                assert spool.deferred_gallery_count == deferred
                assert len(source.observed) == 1
            assert spool.freeze_next() is None
            assert spool.complete


@pytest.mark.parametrize("failure", ("adapter", "observer_cancellation"))
def test_failed_incremental_generator_cannot_be_retried_as_end_of_inventory(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    source = _CountedSource(3)
    observe = source.observe_gallery

    def fail_adapter(locator: tuple[str, ...]) -> VNextIngestGalleryObservation:
        if len(source.observed) == 1:
            raise OSError("source read interrupted")
        return observe(locator)

    def cancel(progress: VNextSourcePreparationProgress) -> None:
        if (
            progress.operation is VNextSourcePreparationOperation.SOURCE_FREEZE
            and progress.completed == 2
        ):
            raise KeyboardInterrupt("observer cancelled")

    if failure == "adapter":
        monkeypatch.setattr(source, "observe_gallery", fail_adapter)
    with SourceDiscoveryPlan.from_locators(
        value.locator for value in source.galleries
    ) as plan:
        with closing(
            FrozenSourceObservationSpool.start(
                source,
                plan=plan,
                source_root_components=source.source_root_components,
                progress=cancel if failure == "observer_cancellation" else None,
            )
        ) as spool:
            assert spool.freeze_next() is not None
            with pytest.raises(OSError if failure == "adapter" else KeyboardInterrupt):
                spool.freeze_next()
            assert not spool.complete
            for _attempt in range(2):
                with pytest.raises(FrozenSourceObservationError, match="failed"):
                    spool.freeze_next()
                assert not spool.complete
            with pytest.raises(FrozenSourceObservationError, match="failed"):
                tuple(spool.selected_locators())
