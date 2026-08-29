from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest
from vnext_publication_fixtures import (
    seed_catalog_publication,
    seed_catalog_publication_title,
    seed_publication_commit,
    seed_publication_identity,
)

from h2hdb import vnext_identity as identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_artifact_family import (
    CatalogArtifactFamily,
    ensure_catalog_artifact_family,
)
from h2hdb.vnext_library_activation_repository import (
    LibraryActivationArtifactRepository,
    LibraryActivationCursorError,
    LibraryActivationReadError,
)

_CHANNEL = b"default"


def _database(path: Path) -> SQLiteConnector:
    connector = SQLiteConnector(str(path))
    connector.connect()
    payload: Any = ARTIFACT["backends"]
    payload = payload["sqlite"]
    for _slice_id, statements in payload["slices"]:
        for _statement_id, _kind, _name, sql in statements:
            connector.execute(sql)
    for seed in payload["bootstrap_seeds"]:
        connector.execute(seed["sql"], seed["parameters"])
    return connector


def _insert_finalization_batch(
    connector: SQLiteConnector,
    *,
    receipt_id: bytes,
    start_generation: int,
    batch_key: bytes,
    start_cursor: bytes,
    start_count: int,
    next_cursor: bytes,
    row_count: int,
    committed_at: int,
) -> None:
    connector.execute(
        "INSERT INTO catalog_publication_finalization_batch_stored "
        "(receipt_id, start_generation, batch_key, start_cursor, "
        "start_processed_count, next_cursor, row_count, committed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            receipt_id,
            start_generation,
            batch_key,
            start_cursor,
            start_count,
            next_cursor,
            row_count,
            committed_at,
        ),
    )


def _mark_published(
    connector: SQLiteConnector,
    *,
    receipt_id: bytes,
    publication_keys: tuple[bytes, ...],
    committed_at: int,
) -> None:
    cursor = max(publication_keys, default=b"")
    if publication_keys:
        _insert_finalization_batch(
            connector,
            receipt_id=receipt_id,
            start_generation=1,
            batch_key=b"artifact-items",
            start_cursor=b"",
            start_count=0,
            next_cursor=cursor,
            row_count=len(publication_keys),
            committed_at=committed_at + 1,
        )
        terminal_generation = 2
        checkpoint_generation = 3
    else:
        terminal_generation = 1
        checkpoint_generation = 2
    _insert_finalization_batch(
        connector,
        receipt_id=receipt_id,
        start_generation=terminal_generation,
        batch_key=b"artifact-terminal",
        start_cursor=cursor,
        start_count=len(publication_keys),
        next_cursor=cursor,
        row_count=0,
        committed_at=committed_at + 2,
    )
    connector.execute(
        "UPDATE catalog_publication_finalization_checkpoints "
        "SET generation = %s, `cursor` = %s, processed_count = %s, "
        "state = %s, updated_at = %s WHERE receipt_id = %s",
        (
            checkpoint_generation,
            cursor,
            len(publication_keys),
            "COMPLETE",
            committed_at + 2,
            receipt_id,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_publication_commit_finalizations (receipt_id) VALUES (%s)",
        (receipt_id,),
    )


def _seed_projection(
    connector: SQLiteConnector,
    *,
    revision: int,
    gids: tuple[int, ...],
    receipt_id: bytes,
    finalized: bool = False,
    channel: bytes = _CHANNEL,
) -> dict[bytes, tuple[int, str, int, tuple[str, ...], bytes, int]]:
    source_revision = revision
    committed_at = 1_000_000 + revision
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        connector.execute(
            "INSERT INTO catalog_revision_descriptors "
            "(revision, publication_count, artifact_count) VALUES (%s, %s, %s)",
            (revision, len(gids), len(gids)),
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_descriptors "
            "(source_revision, channel, snapshot_manifest_sha256) "
            "VALUES (%s, %s, %s)",
            (
                source_revision,
                channel,
                sha256(f"snapshot-{revision}".encode()).digest(),
            ),
        )
        seed_publication_commit(
            connector,
            receipt_id=receipt_id,
            candidate_id=revision.to_bytes(16, "big"),
            revision=revision,
            source_revision=source_revision,
            generation=revision,
            preparation_id=(revision + 100).to_bytes(16, "big"),
            operational_policy_id=1,
            artifact_policy_id=1,
            display_title_policy_id=1,
            new_galleries=len(gids),
            changed_galleries=0,
            removed_galleries=0,
            duplicate_losers=0,
            committed_at=committed_at,
        )

        expected: dict[
            bytes,
            tuple[int, str, int, tuple[str, ...], bytes, int],
        ] = {}
        for position, gid in enumerate(gids):
            publication_key = identity.publication_key(gid)
            upload_time = 2_000_000 + gid
            source_name = f"gallery-{gid}"
            source_title_sha256 = sha256(
                f"source-title-{revision}-{gid}".encode()
            ).digest()
            artifact_payload = f"artifact-{revision}-{gid}".encode()
            artifact_sha256 = sha256(artifact_payload).digest()
            storage_key_components = identity.artifact_storage_key_components(gid)
            semantics_sha256 = sha256(f"semantics-{revision}-{gid}".encode()).digest()
            connector.execute(
                "INSERT INTO catalog_gallery_upload_times (gid, upload_time) "
                "VALUES (%s, %s)",
                (gid, upload_time),
            )
            seed_publication_identity(connector, gid=gid)
            if not connector.fetch_one(
                "SELECT 1 FROM catalog_source_gallery_name_gids "
                "WHERE source_gallery_name = %s",
                (source_name.encode(),),
            ):
                connector.execute(
                    "INSERT INTO catalog_source_gallery_name_gids "
                    "(source_gallery_name, gid) VALUES (%s, %s)",
                    (source_name.encode(), gid),
                )
            connector.execute(
                "INSERT INTO catalog_gallery_source_name_accesses "
                "(gallery_id, source_gallery_name) VALUES (%s, %s)",
                (revision * 10_000 + position + 1, source_name.encode()),
            )
            seed_catalog_publication(
                connector,
                revision=revision,
                publication_key=publication_key,
                gallery_id=revision * 10_000 + position + 1,
                summary_sha256=sha256(f"summary-{revision}-{gid}".encode()).digest(),
                language_sha256=sha256(b"language-zh").digest(),
                modified_at=3_000_000 + gid,
                source_title_sha256=source_title_sha256,
            )
            seed_catalog_publication_title(
                connector,
                revision=revision,
                publication_key=publication_key,
                source_title_sha256=source_title_sha256,
                source_gallery_name=source_name.encode(),
            )
            connector.execute(
                "INSERT INTO catalog_artifact_blobs "
                "(artifact_sha256, size_bytes) VALUES (%s, %s)",
                (artifact_sha256, len(artifact_payload)),
            )
            ensure_catalog_artifact_family(
                connector,
                CatalogArtifactFamily(
                    revision,
                    publication_key,
                    artifact_sha256,
                    semantics_sha256,
                ),
            )
            expected[publication_key] = (
                gid,
                source_name,
                upload_time,
                storage_key_components,
                artifact_sha256,
                len(artifact_payload),
            )
        publication_keys = tuple(sorted(expected))
        if finalized:
            _mark_published(
                connector,
                receipt_id=receipt_id,
                publication_keys=publication_keys,
                committed_at=committed_at,
            )
        connector.execute(
            "INSERT OR REPLACE INTO catalog_publication_commit_head_receipts "
            "(channel, receipt_id) VALUES (%s, %s)",
            (channel, receipt_id),
        )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")
    expected_state = "PUBLISHED" if finalized else "DB_COMMITTED"
    assert connector.fetch_one(
        "SELECT state FROM catalog_publication_receipts WHERE receipt_id = %s",
        (receipt_id,),
    ) == (expected_state,)
    return expected


def test_db_committed_projection_pages_are_bounded_and_empty_terminal(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "bounded.sqlite3")
    try:
        receipt_id = b"r" * 16
        expected = _seed_projection(
            connector,
            revision=1,
            gids=(101, 102, 103, 104),
            receipt_id=receipt_id,
        )
        ordered_keys = tuple(sorted(expected))

        first = LibraryActivationArtifactRepository.list_page(
            connector,
            receipt_id=receipt_id,
            page_limit=2,
        )
        assert (first.receipt_id, first.catalog_revision) == (receipt_id, 1)
        assert tuple(item.publication_key for item in first.items) == ordered_keys[:2]
        assert first.next_cursor == ordered_keys[1]
        assert not first.terminal

        second = LibraryActivationArtifactRepository.list_page(
            connector,
            receipt_id=first.receipt_id,
            cursor=first.next_cursor,
            page_limit=2,
        )
        assert tuple(item.publication_key for item in second.items) == ordered_keys[2:]
        assert not second.terminal
        for item in (*first.items, *second.items):
            assert (
                item.gid,
                item.source_gallery_name,
                item.upload_time,
                item.storage_key.segments,
                item.artifact_sha256,
                item.size_bytes,
            ) == expected[item.publication_key]

        terminal = LibraryActivationArtifactRepository.list_page(
            connector,
            receipt_id=second.receipt_id,
            cursor=second.next_cursor,
            page_limit=2,
        )
        assert terminal.receipt_id == receipt_id
        assert terminal.catalog_revision == 1
        assert terminal.items == ()
        assert terminal.next_cursor is None
        assert terminal.terminal
    finally:
        connector.close()


def test_receipt_pinned_continuation_is_independent_of_reader_head(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "head-advance.sqlite3")
    try:
        old_receipt = b"o" * 16
        _seed_projection(
            connector,
            revision=1,
            gids=(201, 202),
            receipt_id=old_receipt,
        )
        first = LibraryActivationArtifactRepository.list_page(
            connector,
            receipt_id=old_receipt,
            page_limit=1,
        )
        new_receipt = b"n" * 16
        new_expected = _seed_projection(
            connector,
            revision=2,
            gids=(301,),
            receipt_id=new_receipt,
        )

        continuation = LibraryActivationArtifactRepository.list_page(
            connector,
            receipt_id=first.receipt_id,
            cursor=first.next_cursor,
            page_limit=1,
        )
        assert continuation.catalog_revision == 1

        current = LibraryActivationArtifactRepository.list_page(
            connector,
            receipt_id=new_receipt,
        )
        assert tuple(item.publication_key for item in current.items) == tuple(
            sorted(new_expected)
        )
    finally:
        connector.close()


def test_finalized_receipt_remains_replayable(tmp_path: Path) -> None:
    connector = _database(tmp_path / "finalized.sqlite3")
    try:
        receipt_id = b"f" * 16
        expected = _seed_projection(
            connector,
            revision=1,
            gids=(401,),
            receipt_id=receipt_id,
            finalized=True,
        )
        page = LibraryActivationArtifactRepository.list_page(
            connector,
            receipt_id=receipt_id,
        )
        assert tuple(item.publication_key for item in page.items) == tuple(expected)
    finally:
        connector.close()


def test_cursor_and_page_bounds_fail_closed(tmp_path: Path) -> None:
    connector = _database(tmp_path / "cursor.sqlite3")
    try:
        receipt_id = b"r" * 16
        _seed_projection(
            connector,
            revision=1,
            gids=(501,),
            receipt_id=receipt_id,
        )
        with pytest.raises(ValueError, match="must not exceed 128"):
            LibraryActivationArtifactRepository.list_page(
                connector, receipt_id=receipt_id, page_limit=129
            )
        with pytest.raises(ValueError, match="must be in 1"):
            LibraryActivationArtifactRepository.list_page(
                connector, receipt_id=receipt_id, page_limit=0
            )
        with pytest.raises(ValueError, match="exactly 32 bytes|contain 32"):
            LibraryActivationArtifactRepository.list_page(
                connector,
                receipt_id=receipt_id,
                cursor=b"short",
            )
        with pytest.raises(LibraryActivationCursorError, match="exact artifact"):
            LibraryActivationArtifactRepository.list_page(
                connector,
                receipt_id=receipt_id,
                cursor=b"x" * 32,
            )
    finally:
        connector.close()


@pytest.mark.parametrize("corruption", ("title-row", "blob-row"))
def test_sealed_projection_corruption_is_not_silently_omitted(
    tmp_path: Path,
    corruption: str,
) -> None:
    connector = _database(tmp_path / f"corrupt-{corruption}.sqlite3")
    try:
        expected = _seed_projection(
            connector,
            revision=1,
            gids=(601,),
            receipt_id=b"r" * 16,
        )
        publication_key = next(iter(expected))
        connector.execute("PRAGMA foreign_keys = OFF")
        try:
            if corruption == "title-row":
                connector.execute(
                    "DELETE FROM catalog_publication_storage WHERE "
                    "catalog_occurrence_sha256 = ("
                    "SELECT catalog_occurrence_sha256 "
                    "FROM catalog_publication_occurrence_identities "
                    "WHERE revision = 1 AND publication_key = %s)",
                    (publication_key,),
                )
            else:
                artifact_sha256 = expected[publication_key][4]
                connector.execute(
                    "DELETE FROM catalog_artifact_blobs WHERE artifact_sha256 = %s",
                    (artifact_sha256,),
                )
        finally:
            connector.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(LibraryActivationReadError, match="corrupt durable facts"):
            LibraryActivationArtifactRepository.list_page(
                connector,
                receipt_id=b"r" * 16,
            )
    finally:
        connector.close()


def test_sqlite_page_scan_uses_revision_publication_key_primary_index(
    tmp_path: Path,
) -> None:
    import h2hdb.vnext_library_activation_repository as module

    connector = _database(tmp_path / "explain.sqlite3")
    try:
        expected = _seed_projection(
            connector,
            revision=1,
            gids=(701, 702),
            receipt_id=b"r" * 16,
        )
        first_key = min(expected)
        for cursor, expected_constraint in (
            (None, "(revision=?)"),
            (first_key, "(revision=? AND publication_key>?)"),
        ):
            query, parameters = module._artifact_page_query(
                revision=1,
                cursor=cursor,
                page_limit=128,
            )
            plan = connector.fetch_all("EXPLAIN QUERY PLAN " + query, parameters)
            details = tuple(str(row[3]) for row in plan)
            artifact_scan = tuple(
                detail
                for detail in details
                if "artifact" in detail and "SEARCH" in detail
            )
            assert any(
                "USING INDEX sqlite_autoindex_catalog_artifacts_1" in detail
                and expected_constraint in detail
                for detail in artifact_scan
            ), details
    finally:
        connector.close()
