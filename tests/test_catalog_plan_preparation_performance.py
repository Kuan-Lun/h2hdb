from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pytest
import test_vnext_publication_candidate_repository as fixture
from vnext_canonical_value_fixtures import (
    seed_canonical_allocation,
    seed_canonical_page,
)
from vnext_catalog_identity_fixtures import seed_tag_term

from h2hdb import vnext_identity as identity
from h2hdb import vnext_publication_candidate_repository as projection
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_transaction import VNextUnitOfWork


def _seed_tag_value(connector: SQLiteConnector, value: bytes) -> bytes:
    digest = identity.canonical_value_digest("tag_value_utf8_v1", value)
    tree = identity.build_canonical_value_tree(digest, len(value), (value,))
    seed_canonical_allocation(
        connector,
        value_sha256=digest,
        digest_domain=b"tag_value_utf8_v1",
        byte_count=len(value),
        allocated_at=1,
    )
    for encoded in tree.pages:
        page = identity.decode_canonical_value_page(encoded.page_bytes)
        seed_canonical_page(
            connector,
            page_sha256=encoded.page_sha256,
            value_sha256=digest,
            page_bytes=encoded.page_bytes,
            level=page.level,
            page_position=page.page_position,
            subtree_item_count=page.subtree_byte_count,
        )
        for position, entry in enumerate(page.entries):
            if isinstance(entry, identity.CanonicalValueBranchEntry):
                connector.execute(
                    "INSERT INTO catalog_canonical_value_page_parents "
                    "(parent_sha256, position, child_sha256) VALUES (%s, %s, %s)",
                    (encoded.page_sha256, position, entry.child_page_sha256),
                )
    connector.execute(
        "INSERT INTO catalog_canonical_value_identities "
        "(value_sha256, root_page_sha256) VALUES (%s, %s)",
        (digest, tree.root_page_sha256),
    )
    return digest


@contextmanager
def _source(
    tmp_path: Path,
    *,
    galleries: int = 3,
    tags: tuple[bytes, ...] = (b"english", b"Shared Artist"),
) -> Iterator[
    tuple[SQLiteConnector, projection.PublicationProjectionAuthority, tuple[bytes, ...]]
]:
    connector = fixture._generated_database(tmp_path / "catalog.sqlite3")
    try:
        gate, turn = fixture._authorities(connector)
        with connector.transaction():
            fixture._seed_completed_analysis(connector, turn, with_base=False)
            fixture._seed_selected_galleries(connector, count=galleries)
            fixture._seed_projection_metadata(connector, count=galleries)
            values = tuple(_seed_tag_value(connector, value) for value in tags)
            for position, digest in enumerate(values):
                seed_tag_term(
                    connector,
                    tag_id=position + 1,
                    namespace=(
                        b"language"
                        if position == 0
                        else b"artist"
                        if position == 1
                        else b"tag"
                    ),
                    tag_value_sha256=digest,
                )
            connector.execute_many(
                "INSERT INTO catalog_gallery_observation_tags "
                "(gallery_id, observation_id, position, tag_id) VALUES (%s, %s, %s, %s)",
                [
                    (gallery, 1, position, position + 1)
                    for gallery in range(1, galleries + 1)
                    for position in range(len(tags))
                ],
            )
        fixture._begin(connector, gate, turn, artifacts_required=True)
        fixture._complete_selection(connector, gate, turn)
        with connector.transaction():
            authority = (
                projection.PublicationCandidateRepository.issue_projection_authority(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    candidate_id=fixture._CANDIDATE,
                    now=100_000,
                )
            )
        yield connector, authority, values
    finally:
        connector.close()


def _plan_digest(
    plan: projection.PublicationCatalogProjectionPlan,
) -> tuple[bytes, bytes]:
    database = sha256()
    for line in plan._database.iterdump():
        database.update(line.encode())
        database.update(b"\n")
    payload = sha256()
    plan._payload.seek(0)
    while chunk := plan._payload.read(65536):
        payload.update(chunk)
    return database.digest(), payload.digest()


def test_catalog_preparation_batches_unique_tag_bytes_and_rebuilds_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tags = (
        b"english",
        b"Shared Artist",
        *(f"tag {index}".encode() for index in range(128)),
    )
    with _source(tmp_path, tags=tags) as (connector, authority, digests):
        reads: list[tuple[bytes, ...]] = []
        fetch_all = connector.fetch_all
        fetch_one = connector.fetch_one

        def record_query(query: str, data: tuple[Any, ...]) -> None:
            if "FROM catalog_canonical_value_allocation_anchors a " in query:
                selected = tuple(value for value in data if value in digests)
                if selected:
                    reads.append(selected)

        def all_rows(query: str, data: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
            record_query(query, data)
            return fetch_all(query, data)

        def one_row(query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
            record_query(query, data)
            return fetch_one(query, data)

        monkeypatch.setattr(connector, "fetch_all", all_rows)
        monkeypatch.setattr(connector, "fetch_one", one_row)
        results = []
        for prepare in (
            projection.PublicationCandidateRepository.prepare_catalog_projection,
            projection.PublicationCandidateRepository.prepare_catalog_projection_validation,
        ):
            reads.clear()
            with prepare(connector, backend="sqlite", authority=authority) as plan:
                # Three galleries share 130 values, across the 128-tag boundary.
                # SQL must scale with unique bounded pages, not 390 occurrences.
                assert sorted(len(batch) for batch in reads) == [2, 128]
                assert {value for batch in reads for value in batch} == set(digests)
                assert plan._database.execute(
                    "SELECT tag_id, tag_value, COUNT(*) FROM subjects GROUP BY tag_id, tag_value ORDER BY tag_id"
                ).fetchall() == [
                    (index + 1, value, 3) for index, value in enumerate(tags)
                ]
                assert (
                    plan._database.execute(
                        "SELECT name FROM sqlite_temp_master"
                    ).fetchall()
                    == []
                )
                assert not plan._database.in_transaction
                results.append(_plan_digest(plan))
        assert results[0] == results[1]


@pytest.mark.parametrize("byte_count", [32769, 65536])
def test_catalog_preparation_streams_long_tag_values_without_truncation(
    tmp_path: Path, byte_count: int
) -> None:
    long_tag = b"x" * byte_count
    assert len(long_tag) > identity.CANONICAL_VALUE_CHUNK_BYTES
    with _source(tmp_path, tags=(b"english", b"Shared Artist", b"", long_tag)) as (
        connector,
        authority,
        _digests,
    ):
        for prepare in (
            projection.PublicationCandidateRepository.prepare_catalog_projection,
            projection.PublicationCandidateRepository.prepare_catalog_projection_validation,
        ):
            with prepare(connector, backend="sqlite", authority=authority) as plan:
                assert (
                    plan._database.execute(
                        "SELECT tag_value FROM subjects WHERE tag_id = 4 ORDER BY publication_key"
                    ).fetchall()
                    == [(long_tag,)] * 3
                )
                assert (
                    plan._database.execute(
                        "SELECT tag_value FROM subjects WHERE tag_id = 3 ORDER BY publication_key"
                    ).fetchall()
                    == [(b"",)] * 3
                )


@pytest.mark.parametrize("corruption", ["payload", "domain", "unsealed", "extra_edge"])
def test_catalog_validation_rejects_corruption_after_a_prior_cached_build(
    tmp_path: Path, corruption: str
) -> None:
    with _source(tmp_path) as (connector, authority, digests):
        with projection.PublicationCandidateRepository.prepare_catalog_projection(
            connector, backend="sqlite", authority=authority
        ):
            pass
        digest = digests[0]
        (root,) = connector.fetch_one(
            "SELECT root_page_sha256 FROM catalog_canonical_value_identities WHERE value_sha256 = %s",
            (digest,),
        )
        match corruption:
            case "payload":
                (payload,) = connector.fetch_one(
                    "SELECT page_bytes FROM catalog_canonical_value_page_payloads WHERE page_sha256 = %s",
                    (root,),
                )
                connector.execute(
                    "UPDATE catalog_canonical_value_page_payloads SET page_bytes = %s WHERE page_sha256 = %s",
                    (payload[:-1] + bytes([payload[-1] ^ 1]), root),
                )
            case "domain":
                connector.execute(
                    "UPDATE catalog_canonical_value_allocation_digest_domains SET digest_domain = %s WHERE value_sha256 = %s",
                    (b"catalog_language_utf8_v1", digest),
                )
            case "unsealed":
                # Fault injection bypasses the FK that normally protects this seal.
                connector.execute("PRAGMA foreign_keys = OFF")
                try:
                    connector.execute(
                        "DELETE FROM catalog_canonical_value_allocation_seals WHERE value_sha256 = %s",
                        (digest,),
                    )
                finally:
                    connector.execute("PRAGMA foreign_keys = ON")
            case _:
                connector.execute(
                    "INSERT INTO catalog_canonical_value_page_parents (parent_sha256, position, child_sha256) VALUES (%s, 0, %s)",
                    (root, root),
                )
        with pytest.raises(
            projection.PublicationCandidateConflictError, match="corrupt"
        ):
            projection.PublicationCandidateRepository.prepare_catalog_projection_validation(
                connector, backend="sqlite", authority=authority
            )


@pytest.mark.parametrize("failure", ["populate", "commit"])
def test_failed_scratch_plan_is_discarded_and_next_attempt_starts_fresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    with _source(tmp_path) as (connector, authority, _digests):
        directories: list[Path] = []
        databases: list[sqlite3.Connection] = []
        original_directory = TemporaryDirectory
        original_connect = sqlite3.connect
        fail_once = True

        def directory(*, prefix: str) -> TemporaryDirectory[str]:
            result = original_directory(prefix=prefix, dir=tmp_path)
            directories.append(Path(result.name))
            return result

        class ScratchConnection(sqlite3.Connection):
            def execute(self, sql: str, parameters: Any = (), /) -> sqlite3.Cursor:
                nonlocal fail_once
                if failure == "commit" and sql == "COMMIT" and fail_once:
                    fail_once = False
                    raise OSError("scratch commit failed")
                return super().execute(sql, parameters)

        def connect(
            database: str, *, isolation_level: None, check_same_thread: bool
        ) -> sqlite3.Connection:
            result = original_connect(
                database,
                isolation_level=isolation_level,
                check_same_thread=check_same_thread,
                factory=ScratchConnection,
            )
            databases.append(result)
            return result

        original_children = projection._populate_projection_children

        def children(database: sqlite3.Connection) -> int:
            nonlocal fail_once
            result = original_children(database)
            assert database.in_transaction
            if failure == "populate" and fail_once:
                fail_once = False
                raise OSError("scratch populate failed")
            return result

        monkeypatch.setattr(projection, "TemporaryDirectory", directory)
        monkeypatch.setattr(sqlite3, "connect", connect)
        monkeypatch.setattr(projection, "_populate_projection_children", children)
        with pytest.raises(OSError, match="scratch"):
            projection.PublicationCandidateRepository.prepare_catalog_projection(
                connector, backend="sqlite", authority=authority
            )
        assert all(not path.exists() for path in directories)
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            databases[0].execute("SELECT 1")
        with projection.PublicationCandidateRepository.prepare_catalog_projection(
            connector, backend="sqlite", authority=authority
        ) as plan:
            assert plan.publication_count == 3
            assert not plan._database.in_transaction
        assert all(not path.exists() for path in directories)


def test_corrupt_batched_tag_edges_are_rejected_with_a_bounded_sql_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tags = tuple(f"tag {index}".encode() for index in range(10))
    with _source(tmp_path, tags=tags) as (connector, authority, digests):
        roots = tuple(
            connector.fetch_one(
                "SELECT root_page_sha256 FROM catalog_canonical_value_identities WHERE value_sha256 = %s",
                (digest,),
            )[0]
            for digest in digests
        )
        # Each requested root is a leaf and should have zero outgoing edges.
        connector.execute_many(
            "INSERT INTO catalog_canonical_value_page_parents "
            "(parent_sha256, position, child_sha256) VALUES (%s, %s, %s)",
            [(roots[0], index, child) for index, child in enumerate(roots[1:])],
        )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_canonical_value_page_parents WHERE parent_sha256 = %s",
            (roots[0],),
        ) == (9,)
        received_rows = []
        original = connector.fetch_all

        def fetch_all(query: str, data: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
            result = original(query, data)
            if (
                "FROM catalog_canonical_value_page_parents " in query
                and "WHERE parent_sha256 IN (" in query
            ):
                received_rows.append(len(result))
            return result

        monkeypatch.setattr(connector, "fetch_all", fetch_all)
        with pytest.raises(
            projection.PublicationCandidateConflictError, match="corrupt"
        ):
            projection.PublicationCandidateRepository.prepare_catalog_projection(
                connector, backend="sqlite", authority=authority
            )
        # Verify the SQL connector delivered one sentinel row, rather than all
        # corrupt edges being materialized and truncated later in Python.
        assert received_rows == [1]
