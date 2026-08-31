from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from h2hdb import vnext_identity as identity
from h2hdb.domain import (
    CatalogResourceKind,
    StorageObjectDescriptor,
    StorageObjectKey,
    VNextLibraryActivationCursor,
    VNextLibraryActivationItem,
)
from h2hdb.vnext_library_activation_repository import (
    LibraryActivationReadError,
    LibraryActivationResourcePage,
    _items_from_rows,
)


def test_activation_cursor_round_trips_exact_composite_and_rejects_forgery() -> None:
    publication_key = identity.publication_key(7)
    for kind, tag in (
        (CatalogResourceKind.ACQUISITION, b"\x00"),
        (CatalogResourceKind.THUMBNAIL, b"\x01"),
    ):
        cursor = VNextLibraryActivationCursor(publication_key, kind)
        assert cursor.to_bytes() == publication_key + tag
        assert VNextLibraryActivationCursor.from_bytes(cursor.to_bytes()) == cursor

    forged = VNextLibraryActivationCursor(
        publication_key, CatalogResourceKind.ACQUISITION
    )
    object.__setattr__(forged, "resource_kind", "bogus")
    with pytest.raises(TypeError, match="not registered"):
        forged.to_bytes()


def test_activation_page_revalidates_forged_cursor() -> None:
    publication_key = identity.publication_key(7)
    descriptor = StorageObjectDescriptor(
        StorageObjectKey("test-v2", ("resource",)),
        1,
        "11" * 32,
        datetime(2026, 1, 1, tzinfo=UTC),
    )
    item = VNextLibraryActivationItem(
        publication_key,
        7,
        CatalogResourceKind.ACQUISITION,
        descriptor,
    )
    cursor = VNextLibraryActivationCursor(
        publication_key, CatalogResourceKind.ACQUISITION
    )
    object.__setattr__(cursor, "resource_kind", "bogus")

    with pytest.raises(TypeError, match="not registered"):
        LibraryActivationResourcePage(b"r" * 16, 1, (item,), cursor, False)


class _SegmentConnector:
    def __init__(self, key_digest: bytes) -> None:
        self.key_digest = key_digest

    def fetch_all(
        self,
        _query: str,
        _parameters: tuple[object, ...],
    ) -> list[tuple[Any, ...]]:
        return [(self.key_digest, 0, b"resource")]


def _activation_row(modified_at: int) -> tuple[object, ...]:
    publication_key = identity.publication_key(7)
    storage_key = StorageObjectKey("test-v2", ("resource",))
    key_digest = identity.artifact_storage_key_digest(
        storage_key.codec, storage_key.segments
    )
    object_digest = bytes.fromhex("11" * 32)
    return (
        publication_key,
        publication_key,
        publication_key,
        7,
        b"acquisition",
        key_digest,
        object_digest,
        3,
        modified_at,
        key_digest,
        b"test-v2",
        1,
        object_digest,
        3,
        0,
    )


def test_activation_modified_at_conversion_preserves_exact_microseconds() -> None:
    modified_at = 9_007_199_254_740_993
    row = _activation_row(modified_at)
    key_digest = row[5]
    assert isinstance(key_digest, bytes)
    connector = _SegmentConnector(key_digest)

    item = _items_from_rows(  # noqa: SLF001
        connector,  # type: ignore[arg-type]  # deterministic repository fake
        [row],
    )[0]

    assert item.storage_object.modified_at == (
        datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=modified_at)
    )


def test_activation_modified_at_rejects_datetime_overflow() -> None:
    row = _activation_row((1 << 63) - 1)
    key_digest = row[5]
    assert isinstance(key_digest, bytes)
    connector = _SegmentConnector(key_digest)

    with pytest.raises(LibraryActivationReadError, match="corrupt durable"):
        _items_from_rows(  # noqa: SLF001
            connector,  # type: ignore[arg-type]  # deterministic repository fake
            [row],
        )
