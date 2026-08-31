"""Bounded neutral storage-resource pages for one library activation."""

from __future__ import annotations

__all__ = [
    "LibraryActivationCursorError",
    "LibraryActivationReadError",
    "LibraryActivationResourcePage",
    "LibraryActivationResourceRepository",
    "LibraryActivationUnavailableError",
]

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from . import vnext_identity as identity
from .domain import (
    CatalogResourceKind,
    StorageObjectDescriptor,
    StorageObjectKey,
    VNextLibraryActivationCursor,
    VNextLibraryActivationItem,
)
from .sql_connector import SQLConnector
from .vnext_domains import (
    require_ascii_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)

_MAX_PAGE_ITEMS = 128
_PUBLICATION_RECEIPT_STATES = frozenset({"DB_COMMITTED", "PUBLISHED"})
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class LibraryActivationReadError(RuntimeError):
    """A sealed library-activation fact is incomplete or inconsistent."""


class LibraryActivationUnavailableError(LibraryActivationReadError):
    """The requested durable publication receipt is unavailable."""


class LibraryActivationCursorError(LibraryActivationReadError, ValueError):
    """A continuation cursor does not name one exact resource."""


@dataclass(frozen=True, slots=True)
class LibraryActivationResourcePage:
    """One hard-bounded resource-keyset page pinned to a commit receipt."""

    receipt_id: bytes
    catalog_revision: int
    items: tuple[VNextLibraryActivationItem, ...]
    next_cursor: VNextLibraryActivationCursor | None
    terminal: bool

    def __post_init__(self) -> None:
        require_uuid16(self.receipt_id, field="library activation receipt_id")
        require_positive_int63(
            self.catalog_revision,
            field="library activation catalog_revision",
        )
        if not isinstance(self.items, tuple):
            raise TypeError("library activation page items must be an exact tuple")
        if len(self.items) > _MAX_PAGE_ITEMS:
            raise ValueError("library activation page exceeds 128 resources")
        coordinates: list[tuple[bytes, str]] = []
        for item in self.items:
            if not isinstance(item, VNextLibraryActivationItem):
                raise TypeError("library activation page contains a foreign item")
            item.__post_init__()
            coordinates.append((item.publication_key, item.resource_kind.value))
        if tuple(coordinates) != tuple(sorted(set(coordinates))):
            raise ValueError(
                "library activation resource coordinates must be strictly ordered"
            )
        if type(self.terminal) is not bool:
            raise TypeError("library activation terminal marker must be bool")
        if not coordinates:
            if not self.terminal or self.next_cursor is not None:
                raise ValueError("an empty activation page must be terminal")
            return
        if self.terminal:
            raise ValueError("a nonempty activation page cannot be terminal")
        if not isinstance(self.next_cursor, VNextLibraryActivationCursor):
            raise TypeError("a nonempty activation page requires an exact cursor")
        self.next_cursor.__post_init__()
        expected = VNextLibraryActivationCursor(
            self.items[-1].publication_key,
            self.items[-1].resource_kind,
        )
        if self.next_cursor != expected:
            raise ValueError("activation page cursor is not its final coordinate")


@dataclass(frozen=True, slots=True)
class _PinnedReceipt:
    receipt_id: bytes
    revision: int
    channel: bytes
    state: str


class LibraryActivationResourceRepository:
    """Read exact generic resource descriptors without adapter layout knowledge."""

    @staticmethod
    def list_page(
        connector: SQLConnector,
        *,
        receipt_id: bytes,
        cursor: VNextLibraryActivationCursor | None = None,
        page_limit: int = _MAX_PAGE_ITEMS,
    ) -> LibraryActivationResourcePage:
        bound = _require_page_limit(page_limit)
        pinned_receipt = require_uuid16(
            receipt_id,
            field="library activation receipt_id",
        )
        if cursor is not None and not isinstance(cursor, VNextLibraryActivationCursor):
            raise TypeError(
                "library activation cursor must be VNextLibraryActivationCursor"
            )
        if cursor is not None:
            cursor.__post_init__()
        pin = _load_receipt_pin(connector, receipt_id=pinned_receipt)
        if cursor is not None:
            _require_exact_cursor(
                connector,
                revision=pin.revision,
                cursor=cursor,
            )
        query, parameters = _resource_page_query(
            revision=pin.revision,
            cursor=cursor,
            page_limit=bound,
        )
        rows = connector.fetch_all(query, parameters)
        items = _items_from_rows(connector, rows)
        if len(items) > bound:
            raise LibraryActivationReadError(
                "library activation query exceeded its server-owned bound"
            )
        coordinates = tuple(_coordinate(item) for item in items)
        if coordinates != tuple(sorted(set(coordinates))):
            raise LibraryActivationReadError(
                "library activation query returned unordered resources"
            )
        if (
            cursor is not None
            and coordinates
            and coordinates[0]
            <= (
                cursor.publication_key,
                cursor.resource_kind.value,
            )
        ):
            raise LibraryActivationReadError(
                "library activation query did not advance its keyset cursor"
            )
        next_cursor = (
            None
            if not items
            else VNextLibraryActivationCursor(
                items[-1].publication_key,
                items[-1].resource_kind,
            )
        )
        _require_receipt_pin(connector, pin)
        return LibraryActivationResourcePage(
            pin.receipt_id,
            pin.revision,
            items,
            next_cursor,
            not items,
        )


def _coordinate(item: VNextLibraryActivationItem) -> tuple[bytes, str]:
    return item.publication_key, item.resource_kind.value


def _require_page_limit(value: object) -> int:
    limit = require_positive_int63(value, field="library activation page_limit")
    if limit > _MAX_PAGE_ITEMS:
        raise ValueError("library activation page_limit must not exceed 128")
    return limit


def _load_receipt_pin(
    connector: SQLConnector,
    *,
    receipt_id: bytes,
) -> _PinnedReceipt:
    row = connector.fetch_one(
        "SELECT receipt.receipt_id, receipt.revision, receipt.channel, "
        "receipt.state FROM catalog_publication_receipts AS receipt "
        "WHERE receipt.receipt_id = %s",
        (receipt_id,),
    )
    if len(row) != 4:
        raise LibraryActivationUnavailableError(
            "pinned publication receipt is unavailable"
        )
    try:
        stored_receipt = require_uuid16(
            row[0], field="stored library activation receipt_id"
        )
        revision = require_positive_int63(
            row[1], field="stored library activation catalog revision"
        )
        channel = require_ascii_bytes(
            row[2],
            field="stored library activation channel",
            minimum=1,
            maximum=64,
        )
    except (TypeError, ValueError) as error:
        raise LibraryActivationReadError(
            "library activation receipt contains invalid durable facts"
        ) from error
    state = row[3]
    if not isinstance(state, str) or state not in _PUBLICATION_RECEIPT_STATES:
        raise LibraryActivationUnavailableError(
            "publication receipt is not readable by library activation"
        )
    if stored_receipt != receipt_id:
        raise LibraryActivationReadError(
            "pinned publication receipt returned another identity"
        )
    return _PinnedReceipt(stored_receipt, revision, channel, state)


def _require_receipt_pin(connector: SQLConnector, pin: _PinnedReceipt) -> None:
    if _load_receipt_pin(connector, receipt_id=pin.receipt_id) != pin:
        raise LibraryActivationUnavailableError(
            "library activation receipt changed during the page read"
        )


def _require_exact_cursor(
    connector: SQLConnector,
    *,
    revision: int,
    cursor: VNextLibraryActivationCursor,
) -> None:
    rows = connector.fetch_all(
        _RESOURCE_SELECT + "WHERE object.revision = %s AND object.publication_key = %s "
        "AND object.resource_kind = %s",
        (
            revision,
            cursor.publication_key,
            cursor.resource_kind.value.encode("ascii"),
        ),
    )
    try:
        items = _items_from_rows(connector, rows)
    except LibraryActivationReadError as error:
        raise LibraryActivationCursorError(
            "library activation cursor names corrupt durable facts"
        ) from error
    if len(items) != 1 or _coordinate(items[0]) != (
        cursor.publication_key,
        cursor.resource_kind.value,
    ):
        raise LibraryActivationCursorError(
            "library activation cursor does not name one exact resource"
        )


_RESOURCE_SELECT = (
    "SELECT object.publication_key, publication.publication_key, "
    "publication_identity.publication_key, publication_identity.gid, "
    "object.resource_kind, object.storage_object_key_sha256, "
    "object.storage_object_sha256, object.size_bytes, object.modified_at, "
    "key_row.storage_object_key_sha256, key_row.key_codec, "
    "key_row.segment_count, artifact.artifact_sha256, "
    "artifact_blob.size_bytes, artifact.page_count "
    "FROM catalog_storage_objects AS object "
    "LEFT JOIN catalog_publications AS publication "
    "ON publication.revision = object.revision "
    "AND publication.publication_key = object.publication_key "
    "LEFT JOIN catalog_publication_identities AS publication_identity "
    "ON publication_identity.publication_key = publication.publication_key "
    "LEFT JOIN catalog_storage_object_key_identities AS key_row "
    "ON key_row.storage_object_key_sha256 = object.storage_object_key_sha256 "
    "LEFT JOIN catalog_artifacts AS artifact "
    "ON artifact.revision = object.revision "
    "AND artifact.publication_key = object.publication_key "
    "LEFT JOIN catalog_artifact_blobs AS artifact_blob "
    "ON artifact_blob.artifact_sha256 = artifact.artifact_sha256 "
)


def _resource_page_query(
    *,
    revision: int,
    cursor: VNextLibraryActivationCursor | None,
    page_limit: int,
) -> tuple[str, tuple[object, ...]]:
    if cursor is None:
        predicate = "WHERE object.revision = %s "
        parameters: tuple[object, ...] = (revision, page_limit)
    else:
        kind = cursor.resource_kind.value.encode("ascii")
        predicate = (
            "WHERE object.revision = %s AND (object.publication_key > %s OR "
            "(object.publication_key = %s AND object.resource_kind > %s)) "
        )
        parameters = (
            revision,
            cursor.publication_key,
            cursor.publication_key,
            kind,
            page_limit,
        )
    return (
        _RESOURCE_SELECT
        + predicate
        + "ORDER BY object.publication_key, object.resource_kind LIMIT %s",
        parameters,
    )


def _items_from_rows(
    connector: SQLConnector,
    rows: list[tuple[Any, ...]],
) -> tuple[VNextLibraryActivationItem, ...]:
    headers: list[
        tuple[bytes, int, CatalogResourceKind, bytes, bytes, int, int, str, int]
    ] = []
    key_counts: dict[bytes, int] = {}
    try:
        for row in rows:
            if len(row) != 15 or any(value is None for value in row):
                raise ValueError("activation resource row has an invalid shape")
            publication_key = require_digest32(
                row[0], field="activation publication_key"
            )
            if row[1] != publication_key or row[2] != publication_key:
                raise ValueError("activation publication identities are noncongruent")
            gid = require_positive_int63(row[3], field="activation gid")
            if identity.publication_key(gid) != publication_key:
                raise ValueError("activation GID disagrees with publication_key")
            kind = _resource_kind(row[4])
            key_digest = require_digest32(row[5], field="activation storage key digest")
            object_digest = require_digest32(
                row[6], field="activation storage object digest"
            )
            size_bytes = require_positive_int63(
                row[7], field="activation storage object size"
            )
            modified_at = require_int63(
                row[8], field="activation storage object modified_at"
            )
            if (
                require_digest32(row[9], field="activation key identity digest")
                != key_digest
            ):
                raise ValueError("activation storage key identity is noncongruent")
            codec = require_ascii_bytes(
                row[10], field="activation storage key codec", minimum=1, maximum=64
            ).decode("ascii")
            segment_count = require_positive_int63(
                row[11], field="activation storage key segment_count"
            )
            if segment_count > 16:
                raise ValueError("activation storage key has too many segments")
            artifact_digest = require_digest32(
                row[12], field="activation artifact digest"
            )
            artifact_size = require_positive_int63(
                row[13], field="activation artifact size"
            )
            page_count = require_int63(row[14], field="activation page_count")
            if page_count > 4096:
                raise ValueError("activation artifact exceeds 4096 pages")
            if kind is CatalogResourceKind.ACQUISITION and (
                object_digest != artifact_digest or size_bytes != artifact_size
            ):
                raise ValueError("activation acquisition disagrees with artifact")
            if kind is CatalogResourceKind.THUMBNAIL and page_count == 0:
                raise ValueError("empty presentation has a thumbnail resource")
            previous_count = key_counts.setdefault(key_digest, segment_count)
            if previous_count != segment_count:
                raise ValueError("activation storage key counts conflict")
            headers.append(
                (
                    publication_key,
                    gid,
                    kind,
                    key_digest,
                    object_digest,
                    size_bytes,
                    modified_at,
                    codec,
                    segment_count,
                )
            )
        segments = _load_key_segments(connector, key_counts)
        items: list[VNextLibraryActivationItem] = []
        for header in headers:
            (
                publication_key,
                gid,
                kind,
                key_digest,
                object_digest,
                size_bytes,
                modified_at,
                codec,
                segment_count,
            ) = header
            exact_segments = segments.get(key_digest, ())
            if len(exact_segments) != segment_count:
                raise ValueError("activation storage key family is incomplete")
            storage_key = StorageObjectKey(codec, exact_segments)
            if (
                identity.artifact_storage_key_digest(codec, exact_segments)
                != key_digest
            ):
                raise ValueError("activation storage key digest disagrees")
            try:
                modified_datetime = _EPOCH + timedelta(microseconds=modified_at)
            except OverflowError as error:
                raise ValueError(
                    "activation storage object modified_at is out of range"
                ) from error
            descriptor = StorageObjectDescriptor(
                storage_key,
                size_bytes,
                object_digest.hex(),
                modified_datetime,
            )
            items.append(
                VNextLibraryActivationItem(
                    publication_key,
                    gid,
                    kind,
                    descriptor,
                )
            )
        return tuple(items)
    except (TypeError, UnicodeError, ValueError) as error:
        raise LibraryActivationReadError(
            "library activation contains corrupt durable resource facts"
        ) from error


def _load_key_segments(
    connector: SQLConnector,
    key_counts: dict[bytes, int],
) -> dict[bytes, tuple[str, ...]]:
    if not key_counts:
        return {}
    digests = tuple(sorted(key_counts))
    placeholders = ", ".join("%s" for _ in digests)
    rows = connector.fetch_all(
        "SELECT storage_object_key_sha256, segment_position, key_segment "
        "FROM catalog_storage_object_key_segments "
        f"WHERE storage_object_key_sha256 IN ({placeholders}) "
        "ORDER BY storage_object_key_sha256, segment_position",
        digests,
    )
    grouped: dict[bytes, list[str]] = {}
    for row in rows:
        if len(row) != 3:
            raise ValueError("activation storage key segment row is malformed")
        digest = require_digest32(row[0], field="activation key segment digest")
        if digest not in key_counts:
            raise ValueError("activation storage key segment is unexpected")
        position = require_int63(row[1], field="activation key segment position")
        current = grouped.setdefault(digest, [])
        if position != len(current):
            raise ValueError("activation key segments are not dense")
        raw = row[2]
        if not isinstance(raw, bytes):
            raise TypeError("activation key segment must be bytes")
        current.append(raw.decode("utf-8", errors="strict"))
    return {digest: tuple(values) for digest, values in grouped.items()}


def _resource_kind(value: object) -> CatalogResourceKind:
    raw = require_ascii_bytes(
        value,
        field="activation resource_kind",
        minimum=1,
        maximum=11,
    )
    return CatalogResourceKind(raw.decode("ascii"))
