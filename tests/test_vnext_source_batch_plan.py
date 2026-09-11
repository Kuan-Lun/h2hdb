"""Bounded admission retains every published member of a fresh inventory."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import closing

import pytest
from vnext_pipeline import MemorySource, gallery

from h2hdb.vnext_source_build_repository import (
    SourceDiscoveryPlan,
    SourceDiscoveryPlanError,
)
from h2hdb.vnext_source_observation_spool import FrozenSourceObservationSpool


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
