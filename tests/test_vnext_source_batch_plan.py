"""Bounded admission retains every published member of a fresh inventory."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from h2hdb.vnext_source_batch_plan import prepare_source_batch
from h2hdb.vnext_source_build_repository import (
    SourceDiscoveryPlan,
    SourceDiscoveryPlanError,
)


def _locators(plan: SourceDiscoveryPlan) -> Iterator[tuple[str, ...]]:
    position = 0
    while position < plan.gallery_count:
        page = plan._page(position)
        for item in page:
            yield plan._decode_locator(item.position, item.locator_sha256)
        position += len(page)


def test_admission_limit_never_truncates_later_known_members() -> None:
    inventory = tuple((f"gallery-{index}",) for index in range(385))
    known = set(inventory[::2])
    calls: list[int] = []

    def membership(page: tuple[tuple[str, ...], ...]) -> tuple[bool, ...]:
        calls.append(len(page))
        return tuple(locator in known for locator in page)

    with SourceDiscoveryPlan.from_locators(inventory) as full:
        ordered_new = tuple(
            locator for locator in _locators(full) if locator not in known
        )
        batch = prepare_source_batch(
            full, max_new_galleries=3, lookup_members=membership
        )
        with batch.plan as selected:
            assert set(_locators(selected)) == known | set(ordered_new[:3])
            assert selected.gallery_count == len(known) + 3
        assert batch.deferred_gallery_count == len(ordered_new) - 3
    assert sum(calls) == len(inventory)
    assert max(calls) <= 128


def test_empty_inventory_does_not_invent_deferred_work() -> None:
    with SourceDiscoveryPlan.from_locators(()) as full:
        batch = prepare_source_batch(
            full, max_new_galleries=1, lookup_members=lambda page: (False,) * len(page)
        )
        with batch.plan:
            assert batch.plan.gallery_count == 0
            assert batch.deferred_gallery_count == 0


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
            prepare_source_batch(full, max_new_galleries=1, lookup_members=lambda _: ())
