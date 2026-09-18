"""Canonical cleanup keeps exact live references across both SQL backends."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass

import pytest
from vnext_canonical_value_fixtures import seed_canonical_value
from vnext_catalog_registry_fixtures import (
    seed_display_title_policy,
    seed_title_sort_policy,
)
from vnext_fault_harness import open_connector
from vnext_pipeline import (
    Clock,
    MemoryLibrary,
    MemorySource,
    drain_maintenance,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_ingest_turn,
)

import h2hdb.vnext_cleanup_repository as cleanup
from h2hdb import CoreConfig, VNextIngestFacade, vnext_identity
from h2hdb.sql_connector import SQLConnector

pytestmark = pytest.mark.cleanup_acceptance


class _Rollback(Exception):
    pass


@contextmanager
def _rolled_back(connector: SQLConnector) -> Iterator[None]:
    """Inspect alternate reference states without changing public fixtures."""
    try:
        with connector.transaction():
            yield
            raise _Rollback
    except _Rollback:
        pass


def _value(connector: SQLConnector, label: bytes, domain: str) -> bytes:
    digest = vnext_identity.canonical_value_digest(domain, label)
    tree = vnext_identity.build_canonical_value_tree(digest, len(label), (label,))
    (page,) = tree.pages
    seed_canonical_value(
        connector,
        value_sha256=digest,
        digest_domain=domain.encode("ascii"),
        page_sha256=page.page_sha256,
        page_bytes=page.page_bytes,
        subtree_item_count=len(label),
        allocated_at=1,
    )
    return digest


def _eligible(connector: SQLConnector) -> set[bytes]:
    return {
        bytes(row[0])
        for row in connector.fetch_all(
            "SELECT r.value_sha256 "
            "FROM catalog_canonical_value_allocation_anchors r "
            f"WHERE {cleanup._CANONICAL_VALUE_ELIGIBILITY}"
        )
    }


@dataclass(frozen=True)
class _Title:
    policy: int
    source: bytes
    name: bytes
    display: bytes
    sort_policy: int
    sort: bytes

    @property
    def values(self) -> set[bytes]:
        return {self.source, self.display, self.sort}


def _title(connector: SQLConnector, *, revision: int) -> _Title | None:
    row = connector.fetch_one(
        "SELECT choice.display_title_policy_id, choice.source_title_sha256, "
        "choice.source_gallery_name, choice.title_sha256, "
        "policy.title_sort_policy_id, title_sort.sort_title_sha256 "
        "FROM catalog_publication_titles title "
        "JOIN catalog_publication_candidates candidate "
        "ON candidate.reserved_revision = title.revision "
        "JOIN catalog_display_title_choices choice "
        "ON choice.display_title_policy_id = candidate.display_title_policy_id "
        "AND choice.source_title_sha256 = title.source_title_sha256 "
        "AND choice.source_gallery_name = title.source_gallery_name "
        "JOIN catalog_display_title_policies policy "
        "ON policy.display_title_policy_id = choice.display_title_policy_id "
        "JOIN catalog_title_sorts title_sort "
        "ON title_sort.title_sort_policy_id = policy.title_sort_policy_id "
        "AND title_sort.title_sha256 = choice.title_sha256 "
        "WHERE title.revision = %s",
        (revision,),
    )
    return _Title(*row) if row else None


def _live(connector: SQLConnector, title: _Title) -> tuple[bool, bool]:
    choice = connector.fetch_one(
        "SELECT 1 FROM catalog_display_title_choices choice "
        "WHERE choice.display_title_policy_id = %s "
        "AND choice.source_title_sha256 = %s AND choice.source_gallery_name = %s "
        f"AND ({cleanup._live_display_title_choice('choice')})",
        (title.policy, title.source, title.name),
    )
    sort = connector.fetch_one(
        "SELECT 1 FROM catalog_title_sorts title_sort "
        "WHERE title_sort.title_sort_policy_id = %s AND title_sort.title_sha256 = %s "
        f"AND ({cleanup._live_title_sort('title_sort')})",
        (title.sort_policy, title.display),
    )
    return bool(choice), bool(sort)


@pytest.mark.parametrize("same_value", [False, True])
def test_hash_cache_keeps_each_reference_until_it_is_released(
    db_config: CoreConfig, *, same_value: bool
) -> None:
    initialize_database(db_config)
    with closing(open_connector(db_config)) as connector:
        with connector.transaction():
            first = _value(connector, b"identity", "source_title_utf8_v1")
            second = (
                first
                if same_value
                else _value(connector, b"fingerprint", "source_title_utf8_v1")
            )
            orphan = _value(connector, b"orphan", "source_title_utf8_v1")
            connector.execute(
                "INSERT INTO operational_hash_cache_observations "
                "(source_identity_sha256, fingerprint_sha256, observed_at) "
                "VALUES (%s, %s, 1)",
                (first, second),
            )
        with connector.read_transaction():
            assert _eligible(connector) == {orphan}
        with connector.transaction():
            connector.execute("DELETE FROM operational_hash_cache_observations")
        with connector.read_transaction():
            assert _eligible(connector) == {first, second, orphan}


def _assert_exact_title_keys(connector: SQLConnector, current: _Title) -> None:
    """Each exact key matters even when another field matches live authority."""
    seed_title_sort_policy(
        connector, title_sort_policy_id=2, title_sort_algorithm_version=2
    )
    seed_display_title_policy(
        connector, display_title_policy_id=2, title_sort_policy_id=2
    )
    other_source = _value(connector, b"Other Source", "source_title_utf8_v1")
    unreachable: set[bytes] = {other_source}
    for index, (policy, source, name, sort_policy) in enumerate(
        (
            (current.policy, other_source, current.name, current.sort_policy),
            (current.policy, current.source, b"other-gallery", current.sort_policy),
            (2, current.source, current.name, 2),
        )
    ):
        label = f"Detached {index}".encode("ascii")
        display = _value(connector, label, "display_title_utf8_v1")
        sort = _value(connector, label.lower(), "title_sort_utf8_v1")
        connector.execute(
            "INSERT INTO catalog_display_title_choices "
            "(display_title_policy_id, source_title_sha256, "
            "source_gallery_name, title_sha256) VALUES (%s, %s, %s, %s)",
            (policy, source, name, display),
        )
        connector.execute(
            "INSERT INTO catalog_title_sorts "
            "(title_sort_policy_id, title_sha256, sort_title_sha256) "
            "VALUES (%s, %s, %s)",
            (sort_policy, display, sort),
        )
        detached = _Title(policy, source, name, display, sort_policy, sort)
        assert _live(connector, detached) == (False, False)
        unreachable.update((display, sort))
    wrong_sort = _value(connector, b"other policy", "title_sort_utf8_v1")
    connector.execute(
        "INSERT INTO catalog_display_title_choices "
        "(display_title_policy_id, source_title_sha256, "
        "source_gallery_name, title_sha256) VALUES (2, %s, %s, %s)",
        (current.source, b"shared-display", current.display),
    )
    connector.execute(
        "INSERT INTO catalog_title_sorts "
        "(title_sort_policy_id, title_sha256, sort_title_sha256) VALUES (2, %s, %s)",
        (current.display, wrong_sort),
    )
    # The live policy-1 choice and dead policy-2 choice share one display
    # digest. A live choice under a different sort policy cannot protect this
    # sort, even though each half of an incorrectly separated join would match.
    detached_sort = _Title(
        2, current.source, b"shared-display", current.display, 2, wrong_sort
    )
    assert _live(connector, detached_sort) == (False, False)
    unreachable.add(wrong_sort)
    eligible = _eligible(connector)
    assert unreachable <= eligible
    assert not current.values & eligible


def test_title_cache_reachability_follows_current_working_and_uncommitted_authority(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    source = MemorySource([gallery(1, title="First TITLE")])
    library = MemoryLibrary(source)
    observed_uncommitted = False

    def observe_uncommitted(label: str) -> None:
        nonlocal observed_uncommitted
        if observed_uncommitted or not label.startswith("publication.commit:"):
            return
        with closing(open_connector(db_config)) as connector:
            with connector.read_transaction():
                title = _title(connector, revision=1)
                committed = connector.fetch_one(
                    "SELECT 1 FROM catalog_publication_commits WHERE revision = 1"
                )
            if title is None or committed:
                return
            with _rolled_back(connector):
                assert _live(connector, title) == (True, True)
                assert not title.values & _eligible(connector)
                # An uncommitted candidate remains live even when its working
                # slot has been released. A stale slot is not its only authority.
                connector.execute("DELETE FROM operational_catalog_working_candidates")
                assert _live(connector, title) == (True, True)
                assert not title.values & _eligible(connector)
            observed_uncommitted = True

    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        run_ingest_turn(
            facade,
            source=source,
            library=library,
            policy=ingest_policy(artifacts_required=False),
            boundary=observe_uncommitted,
        )
        assert observed_uncommitted
        with closing(open_connector(db_config)) as connector:
            with connector.read_transaction():
                first = _title(connector, revision=1)
                assert first is not None
                assert _live(connector, first) == (True, True)
                assert not first.values & _eligible(connector)
                assert not connector.fetch_one(
                    "SELECT 1 FROM operational_catalog_working_candidates"
                )
            with _rolled_back(connector):
                _assert_exact_title_keys(connector, first)

        source.put(gallery(1, title="Second TITLE"))
        run_ingest_turn(
            facade,
            source=source,
            library=library,
            policy=ingest_policy(artifacts_required=False),
        )
        with closing(open_connector(db_config)) as connector:
            with connector.read_transaction():
                current = _title(connector, revision=2)
                assert current is not None
                assert _live(connector, current) == (True, True)
                assert _live(connector, first) == (False, False)
                eligible = _eligible(connector)
                assert first.sort in eligible
                assert not current.values & eligible
                # Old source/display bytes can still have other physical
                # consumers: only the independent sort is asserted reclaimable.
                old_candidate = connector.fetch_one(
                    "SELECT candidate_id FROM catalog_publication_candidates "
                    "WHERE reserved_revision = 1"
                )
                assert old_candidate
            with _rolled_back(connector):
                connector.execute(
                    "INSERT INTO operational_catalog_working_candidates "
                    "(slot, candidate_id, assigned_at) VALUES (1, %s, 1)",
                    old_candidate,
                )
                assert _live(connector, first) == (True, True)
                assert first.sort not in _eligible(connector)
            with connector.read_transaction():
                assert _live(connector, first) == (False, False)

        drain_maintenance(facade)
        with closing(open_connector(db_config)) as connector:
            with connector.read_transaction():
                assert _live(connector, current) == (True, True)
                assert not current.values & _eligible(connector)
                assert not connector.fetch_one(
                    "SELECT 1 FROM catalog_canonical_value_allocation_anchors "
                    "WHERE value_sha256 = %s",
                    (first.sort,),
                )
    full_check(db_config)


def test_orphan_cache_with_coincident_reference_digests_is_deleted_once(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    with closing(open_connector(db_config)) as connector:
        with connector.transaction():
            value = _value(connector, b"coincident", "source_title_utf8_v1")
            seed_title_sort_policy(connector)
            seed_display_title_policy(connector)
            connector.execute(
                "INSERT INTO catalog_display_title_choices "
                "(display_title_policy_id, source_title_sha256, "
                "source_gallery_name, title_sha256) VALUES (1, %s, %s, %s)",
                (value, b"unreferenced", value),
            )
            connector.execute(
                "INSERT INTO catalog_title_sorts "
                "(title_sort_policy_id, title_sha256, sort_title_sha256) "
                "VALUES (1, %s, %s)",
                (value, value),
            )
        with connector.read_transaction():
            assert _eligible(connector) == {value}
    with VNextIngestFacade(db_config, clock=Clock()) as facade:
        drain_maintenance(facade)
    with closing(open_connector(db_config)) as connector:
        with connector.read_transaction():
            assert not connector.fetch_all(
                "SELECT * FROM catalog_display_title_choices"
            )
            assert not connector.fetch_all("SELECT * FROM catalog_title_sorts")
            assert not connector.fetch_one(
                "SELECT 1 FROM catalog_canonical_value_allocation_anchors "
                "WHERE value_sha256 = %s",
                (value,),
            )
    full_check(db_config)
