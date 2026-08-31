from __future__ import annotations

from hashlib import sha256
from pathlib import Path

import pytest
from vnext_generated_database import open_generated_sqlite_database
from vnext_publication_fixtures import (
    seed_catalog_publication,
    seed_publication_commit,
    seed_publication_finalization,
    seed_publication_identity,
)

from h2hdb import vnext_identity as identity
from h2hdb.domain import CatalogResourceKind, VNextLibraryActivationCursor
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_library_activation_repository import (
    LibraryActivationCursorError,
    LibraryActivationReadError,
    LibraryActivationResourceRepository,
)

_CHANNEL = b"default"


def _database(path: Path) -> SQLiteConnector:
    return open_generated_sqlite_database(path)


def _seed_resource(
    connector: SQLiteConnector,
    *,
    revision: int,
    publication_key: bytes,
    gid: int,
    kind: CatalogResourceKind,
    object_digest: bytes,
    size_bytes: int,
) -> None:
    storage_key = ("fixture-v2", (f"gid-{gid}", kind.value))
    key_digest = identity.artifact_storage_key_digest(*storage_key)
    connector.execute(
        "INSERT OR IGNORE INTO catalog_storage_object_key_identities "
        "(storage_object_key_sha256, key_codec, segment_count) "
        "VALUES (%s, %s, %s)",
        (key_digest, storage_key[0].encode("ascii"), len(storage_key[1])),
    )
    for position, segment in enumerate(storage_key[1]):
        connector.execute(
            "INSERT OR IGNORE INTO catalog_storage_object_key_segments "
            "(storage_object_key_sha256, segment_position, key_segment) "
            "VALUES (%s, %s, %s)",
            (key_digest, position, segment.encode("utf-8")),
        )
    connector.execute(
        "INSERT INTO catalog_storage_objects "
        "(revision, publication_key, resource_kind, storage_object_key_sha256, "
        "storage_object_sha256, size_bytes, modified_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (
            revision,
            publication_key,
            kind.value.encode("ascii"),
            key_digest,
            object_digest,
            size_bytes,
            3_000_000 + gid,
        ),
    )


def _seed_projection(
    connector: SQLiteConnector,
    *,
    revision: int,
    gids: tuple[int, ...],
    receipt_id: bytes,
    published: bool = False,
) -> tuple[tuple[bytes, CatalogResourceKind], ...]:
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
            (revision, _CHANNEL, sha256(f"snapshot-{revision}".encode()).digest()),
        )
        seed_publication_commit(
            connector,
            receipt_id=receipt_id,
            candidate_id=revision.to_bytes(16, "big"),
            revision=revision,
            source_revision=revision,
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

        coordinates: list[tuple[bytes, CatalogResourceKind]] = []
        for position, gid in enumerate(gids):
            publication_key = identity.publication_key(gid)
            gallery_id = revision * 10_000 + position + 1
            source_gallery_name = f"gallery-{revision}-{gid}".encode()
            artifact_payload = f"artifact-{revision}-{gid}".encode()
            artifact_digest = sha256(artifact_payload).digest()
            thumbnail_payload = f"thumbnail-{revision}-{gid}".encode()
            thumbnail_digest = sha256(thumbnail_payload).digest()
            connector.execute(
                "INSERT INTO catalog_gallery_upload_times (gid, upload_time) "
                "VALUES (%s, %s)",
                (gid, 2_000_000 + gid),
            )
            seed_publication_identity(connector, gid=gid)
            connector.execute(
                "INSERT INTO catalog_source_gallery_name_gids "
                "(source_gallery_name, gid) VALUES (%s, %s)",
                (source_gallery_name, gid),
            )
            connector.execute(
                "INSERT INTO catalog_gallery_source_name_accesses "
                "(gallery_id, source_gallery_name) VALUES (%s, %s)",
                (gallery_id, source_gallery_name),
            )
            seed_catalog_publication(
                connector,
                revision=revision,
                publication_key=publication_key,
                gallery_id=gallery_id,
                summary_sha256=sha256(f"summary-{revision}-{gid}".encode()).digest(),
                language_sha256=sha256(b"language-zh").digest(),
                modified_at=3_000_000 + gid,
                source_title_sha256=sha256(
                    f"source-title-{revision}-{gid}".encode()
                ).digest(),
            )
            for digest, size in (
                (artifact_digest, len(artifact_payload)),
                (thumbnail_digest, len(thumbnail_payload)),
            ):
                connector.execute(
                    "INSERT INTO catalog_artifact_blobs "
                    "(artifact_sha256, size_bytes) VALUES (%s, %s)",
                    (digest, size),
                )
            connector.execute(
                "INSERT INTO catalog_artifacts "
                "(revision, publication_key, artifact_sha256, "
                "artifact_semantics_sha256, artifact_name, media_type, page_count) "
                "VALUES (%s, %s, %s, %s, %s, %s, 1)",
                (
                    revision,
                    publication_key,
                    artifact_digest,
                    sha256(f"semantics-{revision}-{gid}".encode()).digest(),
                    f"publication-{gid}.bin".encode(),
                    b"application/octet-stream",
                ),
            )
            _seed_resource(
                connector,
                revision=revision,
                publication_key=publication_key,
                gid=gid,
                kind=CatalogResourceKind.ACQUISITION,
                object_digest=artifact_digest,
                size_bytes=len(artifact_payload),
            )
            _seed_resource(
                connector,
                revision=revision,
                publication_key=publication_key,
                gid=gid,
                kind=CatalogResourceKind.THUMBNAIL,
                object_digest=thumbnail_digest,
                size_bytes=len(thumbnail_payload),
            )
            coordinates.extend(
                (
                    (publication_key, CatalogResourceKind.ACQUISITION),
                    (publication_key, CatalogResourceKind.THUMBNAIL),
                )
            )
        ordered = tuple(
            sorted(coordinates, key=lambda value: (value[0], value[1].value))
        )
        if published:
            cursor = VNextLibraryActivationCursor(*ordered[-1]).to_bytes()
            seed_publication_finalization(
                connector,
                receipt_id=receipt_id,
                cursor=cursor,
                processed_count=len(ordered),
                finalized_at=committed_at + 1,
            )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")
    expected_state = "PUBLISHED" if published else "DB_COMMITTED"
    assert connector.fetch_one(
        "SELECT state FROM catalog_publication_receipts WHERE receipt_id = %s",
        (receipt_id,),
    ) == (expected_state,)
    return ordered


def test_resource_pages_use_typed_composite_cursor_and_empty_terminal(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "bounded.sqlite3")
    try:
        receipt_id = b"r" * 16
        expected = _seed_projection(
            connector,
            revision=1,
            gids=(101, 102),
            receipt_id=receipt_id,
        )
        first = LibraryActivationResourceRepository.list_page(
            connector,
            receipt_id=receipt_id,
            page_limit=2,
        )
        assert (
            tuple((item.publication_key, item.resource_kind) for item in first.items)
            == expected[:2]
        )
        assert first.next_cursor == VNextLibraryActivationCursor(*expected[1])
        assert not first.terminal

        second = LibraryActivationResourceRepository.list_page(
            connector,
            receipt_id=receipt_id,
            cursor=first.next_cursor,
            page_limit=2,
        )
        assert (
            tuple((item.publication_key, item.resource_kind) for item in second.items)
            == expected[2:]
        )
        for item in (*first.items, *second.items):
            assert item.storage_object.key.segments == (
                f"gid-{item.gid}",
                item.resource_kind.value,
            )

        terminal = LibraryActivationResourceRepository.list_page(
            connector,
            receipt_id=receipt_id,
            cursor=second.next_cursor,
            page_limit=2,
        )
        assert terminal.items == ()
        assert terminal.next_cursor is None
        assert terminal.terminal
    finally:
        connector.close()


def test_published_receipt_resources_remain_replayable(tmp_path: Path) -> None:
    connector = _database(tmp_path / "published.sqlite3")
    try:
        receipt_id = b"f" * 16
        expected = _seed_projection(
            connector,
            revision=1,
            gids=(401,),
            receipt_id=receipt_id,
            published=True,
        )
        page = LibraryActivationResourceRepository.list_page(
            connector,
            receipt_id=receipt_id,
        )
        assert (
            tuple((item.publication_key, item.resource_kind) for item in page.items)
            == expected
        )
    finally:
        connector.close()


def test_cursor_membership_and_page_bounds_fail_closed(tmp_path: Path) -> None:
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
            LibraryActivationResourceRepository.list_page(
                connector,
                receipt_id=receipt_id,
                page_limit=129,
            )
        forged = VNextLibraryActivationCursor(
            identity.publication_key(999),
            CatalogResourceKind.ACQUISITION,
        )
        with pytest.raises(LibraryActivationCursorError, match="exact resource"):
            LibraryActivationResourceRepository.list_page(
                connector,
                receipt_id=receipt_id,
                cursor=forged,
            )
    finally:
        connector.close()


def test_corrupt_storage_descriptor_is_not_silently_omitted(tmp_path: Path) -> None:
    connector = _database(tmp_path / "corrupt.sqlite3")
    try:
        receipt_id = b"r" * 16
        expected = _seed_projection(
            connector,
            revision=1,
            gids=(601,),
            receipt_id=receipt_id,
        )
        publication_key = expected[0][0]
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "UPDATE catalog_storage_objects SET size_bytes = size_bytes + 1 "
            "WHERE revision = 1 AND publication_key = %s "
            "AND resource_kind = %s",
            (publication_key, b"acquisition"),
        )
        connector.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(LibraryActivationReadError, match="corrupt durable"):
            LibraryActivationResourceRepository.list_page(
                connector,
                receipt_id=receipt_id,
            )
    finally:
        connector.close()
