"""The shared source adapter must honor canonical-byte keyset ordering at scale."""

from __future__ import annotations

import pytest
from vnext_pipeline import (
    MemorySource,
    claim_session,
    gallery,
    ingest_policy,
    initialize_database,
    run_source,
)

from h2hdb import CoreConfig, VNextIngestFacade
from h2hdb.ports import VNextIngestSourceAdapter
from h2hdb.vnext_identity import encode_source_relative_locator

pytestmark = pytest.mark.performance_acceptance

_MULTIPART_UTF8 = (
    ("z",),
    ("aa",),
    ("é",),
    ("中",),
    ("🙂",),
    ("a", "b"),
    ("a", "é"),
    ("a", "中"),
    ("a", "日本"),
    ("z", "x"),
    ("aaaa", "a"),
    ("a", "b", "c"),
)


def _order(locator: tuple[str, ...]) -> tuple[int, tuple[tuple[int, bytes], ...]]:
    """Independent order oracle from the public locator-frame specification.

    The header compares component count before component data; each component
    compares UTF-8 byte length before its exact bytes. Text tuple order is not
    equivalent, even for ordinary ASCII gallery-9/gallery-10/gallery-100 names.
    """
    encoded = tuple(component.encode("utf-8") for component in locator)
    return len(encoded), tuple((len(component), component) for component in encoded)


def _source(count: int) -> tuple[MemorySource, tuple[tuple[str, ...], ...]]:
    locators = (
        tuple((f"gallery-{number}",) for number in range(1, count + 1))
        + _MULTIPART_UTF8
    )
    expected = tuple(sorted(locators, key=_order))
    assert expected != tuple(sorted(locators))  # This corpus rejects the old fake.
    # The durable source-name authority maps a basename to one GID, even when
    # distinct nested locations intentionally share that basename.
    gids = {
        name: gid for gid, name in enumerate(sorted({item[-1] for item in locators}), 1)
    }
    values = [
        gallery(gids[locator[-1]], locator=locator) for locator in reversed(locators)
    ]
    source = MemorySource(values)
    assert isinstance(source, VNextIngestSourceAdapter)
    return source, expected


@pytest.mark.parametrize("count", (10, 100, 129))
@pytest.mark.parametrize("limit", (1, 7, 128))
def test_locator_pages_replay_and_resume_in_exact_canonical_byte_order(
    count: int, limit: int
) -> None:
    source, expected = _source(count)
    assert tuple(item.locator for item in source.galleries) == expected
    encoded = [encode_source_relative_locator(locator) for locator in expected]
    assert encoded == sorted(encoded)
    for _ in range(2):
        after = None
        found: list[tuple[str, ...]] = []
        while True:
            page = source.list_gallery_locators(after_locator=after, limit=limit)
            assert page == source.list_gallery_locators(
                after_locator=after, limit=limit
            )
            assert len(page.items) <= limit
            found.extend(page.items)
            if page.terminal:
                assert page.next_after is None
                break
            assert page.items and page.next_after == page.items[-1]
            after = page.next_after
        assert tuple(found) == expected
    for after in (
        expected[0],
        expected[-1],
        ("gallery-20a",),
        ("a", "aa"),
        ("é", "middle"),
    ):
        page = source.list_gallery_locators(after_locator=after, limit=limit)
        remaining = tuple(
            locator for locator in expected if _order(locator) > _order(after)
        )
        assert page.items == remaining[:limit]
        assert page.terminal == (len(remaining) <= limit)
        assert page.next_after == (None if page.terminal else page.items[-1])


@pytest.mark.deep
@pytest.mark.parametrize("count", (10, 100, 129))
def test_public_source_seals_decimal_growth_and_multipart_utf8_locators(
    db_config: CoreConfig, count: int
) -> None:
    initialize_database(db_config)
    source, expected = _source(count)
    with VNextIngestFacade(db_config) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        receipt = run_source(facade, session, policy, source)
        assert receipt.sealed
        assert receipt.discovered_galleries == len(expected)
        assert receipt.staged_galleries == len(expected)
