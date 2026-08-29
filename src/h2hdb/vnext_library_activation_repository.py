"""Bounded artifact pages for one not-yet-visible library activation.

Every page is pinned to the immutable publication receipt being activated.
The receipt does not need to be the reader head; final publication advances
that head only after the adapter reports a terminal READY checkpoint.

Every returned neutral storage key is derived from the collision-checked GID;
artifact content digests remain integrity facts and never determine placement.
"""

from __future__ import annotations

__all__ = [
    "LibraryActivationArtifactItem",
    "LibraryActivationArtifactPage",
    "LibraryActivationArtifactRepository",
    "LibraryActivationCursorError",
    "LibraryActivationReadError",
    "LibraryActivationUnavailableError",
]

from dataclasses import dataclass
from typing import Any

from . import vnext_identity as identity
from .domain import ArtifactStorageKey, artifact_storage_key
from .sql_connector import SQLConnector
from .vnext_domains import (
    require_ascii_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_utf8_bytes,
    require_uuid16,
)

_MAX_PAGE_ITEMS = 128
_PUBLICATION_RECEIPT_STATES = frozenset({"DB_COMMITTED", "PUBLISHED"})


class LibraryActivationReadError(RuntimeError):
    """A sealed library-activation fact is incomplete or inconsistent."""


class LibraryActivationUnavailableError(LibraryActivationReadError):
    """The requested channel or durable publication receipt is unavailable."""


class LibraryActivationCursorError(LibraryActivationReadError, ValueError):
    """A continuation cursor does not name an item in its pinned receipt."""


@dataclass(frozen=True, slots=True)
class LibraryActivationArtifactItem:
    """Exact immutable facts needed to install one library CBZ artifact."""

    publication_key: bytes
    gid: int
    source_gallery_name: str
    upload_time: int
    storage_key: ArtifactStorageKey
    artifact_sha256: bytes
    size_bytes: int

    def __post_init__(self) -> None:
        publication = require_digest32(
            self.publication_key,
            field="library activation publication_key",
        )
        gid = require_positive_int63(self.gid, field="library activation gid")
        if identity.publication_key(gid) != publication:
            raise ValueError(
                "library activation publication_key disagrees with its durable GID"
            )
        if not isinstance(self.source_gallery_name, str):
            raise TypeError("library activation source_gallery_name must be str")
        source_name = self.source_gallery_name.encode("utf-8", errors="strict")
        require_utf8_bytes(
            source_name,
            field="library activation source_gallery_name",
            minimum=1,
            maximum=255,
            reject_nul=True,
        )
        require_int63(self.upload_time, field="library activation upload_time")
        require_digest32(
            self.artifact_sha256,
            field="library activation artifact_sha256",
        )
        if self.storage_key != artifact_storage_key(gid):
            raise ValueError("library activation storage key disagrees with its GID")
        require_int63(self.size_bytes, field="library activation size_bytes")


@dataclass(frozen=True, slots=True)
class LibraryActivationArtifactPage:
    """One hard-bounded keyset page pinned to a durable publication receipt."""

    receipt_id: bytes
    catalog_revision: int
    items: tuple[LibraryActivationArtifactItem, ...]
    next_cursor: bytes | None
    terminal: bool

    def __post_init__(self) -> None:
        require_uuid16(
            self.receipt_id,
            field="library activation receipt_id",
        )
        require_positive_int63(
            self.catalog_revision,
            field="library activation catalog_revision",
        )
        if not isinstance(self.items, tuple):
            raise TypeError("library activation page items must be an exact tuple")
        if len(self.items) > _MAX_PAGE_ITEMS:
            raise ValueError("library activation page exceeds 128 items")
        keys: list[bytes] = []
        for item in self.items:
            if not isinstance(item, LibraryActivationArtifactItem):
                raise TypeError("library activation page contains a foreign item")
            item.__post_init__()
            keys.append(item.publication_key)
        if tuple(keys) != tuple(sorted(set(keys))):
            raise ValueError(
                "library activation page keys must be unique and strictly ordered"
            )
        if type(self.terminal) is not bool:
            raise TypeError("library activation terminal marker must be bool")
        if not keys:
            if not self.terminal or self.next_cursor is not None:
                raise ValueError("an empty library activation page must be terminal")
            return
        next_cursor = require_digest32(
            self.next_cursor,
            field="library activation next_cursor",
        )
        if self.terminal or next_cursor != keys[-1]:
            raise ValueError(
                "a nonempty library activation page must advance its exact cursor"
            )


@dataclass(frozen=True, slots=True)
class _PinnedReceipt:
    receipt_id: bytes
    revision: int
    channel: bytes
    state: str


class LibraryActivationArtifactRepository:
    """Read activation artifact facts without exposing SQL internals."""

    @staticmethod
    def list_page(
        connector: SQLConnector,
        *,
        receipt_id: bytes,
        cursor: bytes | None = None,
        page_limit: int = _MAX_PAGE_ITEMS,
    ) -> LibraryActivationArtifactPage:
        """Return one bounded page from an exact immutable commit receipt."""

        bound = _require_page_limit(page_limit)
        pinned_receipt = require_uuid16(
            receipt_id,
            field="library activation receipt_id",
        )
        after = (
            None
            if cursor is None
            else require_digest32(cursor, field="library activation cursor")
        )
        pin = _load_receipt_pin(
            connector,
            receipt_id=pinned_receipt,
        )
        if after is not None:
            _require_exact_cursor(
                connector,
                revision=pin.revision,
                cursor=after,
            )
        query, parameters = _artifact_page_query(
            revision=pin.revision,
            cursor=after,
            page_limit=bound,
        )
        rows = connector.fetch_all(query, parameters)
        try:
            items = tuple(_item_from_row(row) for row in rows)
        except (TypeError, UnicodeError, ValueError) as error:
            raise LibraryActivationReadError(
                "library activation contains incomplete or corrupt durable facts"
            ) from error
        if len(items) > bound:
            raise LibraryActivationReadError(
                "library activation query exceeded its server-owned bound"
            )
        keys = tuple(item.publication_key for item in items)
        if keys != tuple(sorted(set(keys))):
            raise LibraryActivationReadError(
                "library activation query returned unordered or duplicate keys"
            )
        if after is not None and keys and keys[0] <= after:
            raise LibraryActivationReadError(
                "library activation query did not advance its keyset cursor"
            )
        next_cursor = None if not items else items[-1].publication_key
        _require_receipt_pin(connector, pin)
        return LibraryActivationArtifactPage(
            pin.receipt_id,
            pin.revision,
            items,
            next_cursor,
            not items,
        )


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
    missing = "pinned publication receipt is unavailable"
    if len(row) != 4:
        raise LibraryActivationUnavailableError(missing)
    try:
        stored_receipt = require_uuid16(
            row[0],
            field="stored library activation receipt_id",
        )
        revision = require_positive_int63(
            row[1],
            field="stored library activation catalog revision",
        )
        stored_channel = require_ascii_bytes(
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
            "pinned publication receipt lookup returned another identity"
        )
    return _PinnedReceipt(stored_receipt, revision, stored_channel, state)


def _require_receipt_pin(
    connector: SQLConnector,
    pin: _PinnedReceipt,
) -> None:
    current = _load_receipt_pin(
        connector,
        receipt_id=pin.receipt_id,
    )
    if current != pin:
        raise LibraryActivationUnavailableError(
            "library activation receipt changed during the page read"
        )


def _require_exact_cursor(
    connector: SQLConnector,
    *,
    revision: int,
    cursor: bytes,
) -> None:
    query, parameters = _artifact_exact_query(
        revision=revision,
        publication_key=cursor,
    )
    rows = connector.fetch_all(query, parameters)
    if len(rows) != 1:
        raise LibraryActivationCursorError(
            "library activation cursor does not name one exact artifact"
        )
    try:
        item = _item_from_row(rows[0])
    except (TypeError, UnicodeError, ValueError) as error:
        raise LibraryActivationCursorError(
            "library activation cursor names corrupt durable facts"
        ) from error
    if item.publication_key != cursor:
        raise LibraryActivationCursorError(
            "library activation cursor lookup returned another key"
        )


_ARTIFACT_SELECT = (
    "SELECT artifact.publication_key, publication.publication_key, "
    "title.publication_key, publication_identity.gid, "
    "title.source_gallery_name, upload.upload_time, "
    "artifact.artifact_sha256, artifact_blob.size_bytes "
    "FROM catalog_artifacts AS artifact "
    "LEFT JOIN catalog_publications AS publication "
    "ON publication.revision = artifact.revision "
    "AND publication.publication_key = artifact.publication_key "
    "LEFT JOIN catalog_publication_identities AS publication_identity "
    "ON publication_identity.publication_key = publication.publication_key "
    "LEFT JOIN catalog_gallery_upload_times AS upload "
    "ON upload.gid = publication_identity.gid "
    "LEFT JOIN catalog_publication_titles AS title "
    "ON title.revision = publication.revision "
    "AND title.publication_key = publication.publication_key "
    "LEFT JOIN catalog_artifact_blobs AS artifact_blob "
    "ON artifact_blob.artifact_sha256 = artifact.artifact_sha256 "
)


def _artifact_page_query(
    *,
    revision: int,
    cursor: bytes | None,
    page_limit: int,
) -> tuple[str, tuple[object, ...]]:
    """Return the exact production page query for execution and plan proofs."""

    if cursor is None:
        predicate = "WHERE artifact.revision = %s "
        parameters: tuple[object, ...] = (revision, page_limit)
    else:
        predicate = "WHERE artifact.revision = %s AND artifact.publication_key > %s "
        parameters = (revision, cursor, page_limit)
    return (
        _ARTIFACT_SELECT + predicate + "ORDER BY artifact.publication_key LIMIT %s",
        parameters,
    )


def _artifact_exact_query(
    *,
    revision: int,
    publication_key: bytes,
) -> tuple[str, tuple[object, ...]]:
    return (
        _ARTIFACT_SELECT + "WHERE artifact.revision = %s "
        "AND artifact.publication_key = %s",
        (revision, publication_key),
    )


def _item_from_row(row: tuple[Any, ...]) -> LibraryActivationArtifactItem:
    if len(row) != 8 or any(value is None for value in row):
        raise ValueError("library activation artifact row has an invalid shape")
    publication_key = require_digest32(
        row[0],
        field="stored library activation publication_key",
    )
    if row[1] != publication_key or row[2] != publication_key:
        raise ValueError(
            "library activation artifact lacks its atomic publication/title rows"
        )
    gid = require_positive_int63(row[3], field="stored library activation gid")
    source_name = require_utf8_bytes(
        row[4],
        field="stored library activation source_gallery_name",
        minimum=1,
        maximum=255,
        reject_nul=True,
    ).decode("utf-8", errors="strict")
    upload_time = require_int63(
        row[5],
        field="stored library activation upload_time",
    )
    artifact_sha256 = require_digest32(
        row[6],
        field="stored library activation artifact_sha256",
    )
    size_bytes = require_int63(
        row[7],
        field="stored library activation size_bytes",
    )
    return LibraryActivationArtifactItem(
        publication_key,
        gid,
        source_name,
        upload_time,
        artifact_storage_key(gid),
        artifact_sha256,
        size_bytes,
    )
