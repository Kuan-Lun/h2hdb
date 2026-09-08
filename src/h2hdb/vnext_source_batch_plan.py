"""Select an exact cumulative source cut from a complete discovery inventory.

Published membership, rather than a cache hit or caller cursor, is the restart
checkpoint. Every still-present published gallery is admitted, including changed
ones whose old source bytes may no longer exist. The limit applies only to new
members; omitted new galleries remain discoverable in the next fresh inventory.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass

from .vnext_source_build_repository import SourceDiscoveryPlan

MAX_NEW_GALLERIES = 1_000_000
type SourceMembershipLookup = Callable[[tuple[tuple[str, ...], ...]], tuple[bool, ...]]


def require_source_batch_limit(value: int) -> int:
    if type(value) is not int or not 1 <= value <= MAX_NEW_GALLERIES:
        raise ValueError(
            f"max_new_galleries must be an integer in 1..{MAX_NEW_GALLERIES}"
        )
    return value


@dataclass(frozen=True, slots=True)
class SourceBatchPlan:
    plan: SourceDiscoveryPlan
    deferred_gallery_count: int


def prepare_source_batch(
    inventory: SourceDiscoveryPlan,
    *,
    max_new_galleries: int,
    lookup_members: SourceMembershipLookup,
) -> SourceBatchPlan:
    """Read the whole inventory in bounded pages and re-seal selected locators."""

    limit = require_source_batch_limit(max_new_galleries)
    deferred = 0

    def selected_locators() -> Iterator[tuple[str, ...]]:
        nonlocal deferred
        admitted = 0
        position = 0
        while position < inventory.gallery_count:
            page = inventory._page(position)[:128]
            if not page:
                raise ValueError("source inventory ended before its sealed count")
            locators = tuple(
                inventory._decode_locator(item.position, item.locator_sha256)
                for item in page
            )
            membership = lookup_members(locators)
            if len(membership) != len(locators) or any(
                type(known) is not bool for known in membership
            ):
                raise ValueError("source membership lookup returned an invalid page")
            for locator, known in zip(locators, membership, strict=True):
                if known:
                    yield locator
                elif admitted < limit:
                    admitted += 1
                    yield locator
                else:
                    deferred += 1
            position += len(page)

    plan = SourceDiscoveryPlan.from_locators(selected_locators())
    return SourceBatchPlan(plan, deferred)
