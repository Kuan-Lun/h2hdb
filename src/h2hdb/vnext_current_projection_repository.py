"""Receipt-pinned bounded artifact pages for the current projection.

The mutable channel head is consulted only for the first page.  That read pins
one immutable publication receipt and catalog revision; continuation pages are
then addressed by the exact receipt plus the previous 32-byte publication key,
so a concurrent head advance cannot mix revisions in one projection spool.

Artifact paths are never reconstructed from a display or source file name.
Every returned locator is derived from the sealed catalog artifact digest and
exact-compared with its normalized content-addressed locator identity.
"""

from __future__ import annotations

__all__ = [
    "CurrentProjectionArtifactItem",
    "CurrentProjectionArtifactPage",
    "CurrentProjectionArtifactRepository",
    "CurrentProjectionCursorError",
    "CurrentProjectionReadError",
    "CurrentProjectionUnavailableError",
]

from dataclasses import dataclass
from typing import Any

from . import vnext_identity as identity
from .sql_connector import SQLConnector
from .vnext_domains import (
    require_ascii_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_utf8_bytes,
    require_uuid16,
)

_DEFAULT_CHANNEL = b"default"
_MAX_PAGE_ITEMS = 128
_PUBLICATION_RECEIPT_STATES = frozenset({"DB_COMMITTED", "PROJECTION_FINALIZED"})


class CurrentProjectionReadError(RuntimeError):
    """A sealed current-projection fact is incomplete or inconsistent."""


class CurrentProjectionUnavailableError(CurrentProjectionReadError):
    """The requested channel or durable publication receipt is unavailable."""


class CurrentProjectionCursorError(CurrentProjectionReadError, ValueError):
    """A continuation cursor does not name an item in its pinned receipt."""


@dataclass(frozen=True, slots=True)
class CurrentProjectionArtifactItem:
    """Exact immutable facts needed to materialize one current CBZ artifact."""

    publication_key: bytes
    gid: int
    source_gallery_name: str
    upload_time: int
    artifact_locator_components: tuple[str, ...]
    artifact_sha256: bytes
    size_bytes: int

    def __post_init__(self) -> None:
        publication = require_digest32(
            self.publication_key,
            field="current projection publication_key",
        )
        gid = require_positive_int63(self.gid, field="current projection gid")
        if identity.publication_key(gid) != publication:
            raise ValueError(
                "current projection publication_key disagrees with its durable GID"
            )
        if not isinstance(self.source_gallery_name, str):
            raise TypeError("current projection source_gallery_name must be str")
        source_name = self.source_gallery_name.encode("utf-8", errors="strict")
        require_utf8_bytes(
            source_name,
            field="current projection source_gallery_name",
            minimum=1,
            maximum=255,
            reject_nul=True,
        )
        require_int63(self.upload_time, field="current projection upload_time")
        artifact = require_digest32(
            self.artifact_sha256,
            field="current projection artifact_sha256",
        )
        expected_locator = identity.artifact_locator_components(artifact)
        if (
            not isinstance(self.artifact_locator_components, tuple)
            or self.artifact_locator_components != expected_locator
        ):
            raise ValueError(
                "current projection artifact locator is not content-addressed"
            )
        require_int63(self.size_bytes, field="current projection size_bytes")


@dataclass(frozen=True, slots=True)
class CurrentProjectionArtifactPage:
    """One hard-bounded keyset page pinned to a durable publication receipt."""

    receipt_id: bytes
    catalog_revision: int
    items: tuple[CurrentProjectionArtifactItem, ...]
    next_cursor: bytes | None
    terminal: bool

    def __post_init__(self) -> None:
        require_uuid16(
            self.receipt_id,
            field="current projection receipt_id",
        )
        require_positive_int63(
            self.catalog_revision,
            field="current projection catalog_revision",
        )
        if not isinstance(self.items, tuple):
            raise TypeError("current projection page items must be an exact tuple")
        if len(self.items) > _MAX_PAGE_ITEMS:
            raise ValueError("current projection page exceeds 128 items")
        keys: list[bytes] = []
        for item in self.items:
            if not isinstance(item, CurrentProjectionArtifactItem):
                raise TypeError("current projection page contains a foreign item")
            item.__post_init__()
            keys.append(item.publication_key)
        if tuple(keys) != tuple(sorted(set(keys))):
            raise ValueError(
                "current projection page keys must be unique and strictly ordered"
            )
        if type(self.terminal) is not bool:
            raise TypeError("current projection terminal marker must be bool")
        if not keys:
            if not self.terminal or self.next_cursor is not None:
                raise ValueError("an empty current projection page must be terminal")
            return
        next_cursor = require_digest32(
            self.next_cursor,
            field="current projection next_cursor",
        )
        if self.terminal or next_cursor != keys[-1]:
            raise ValueError(
                "a nonempty current projection page must advance its exact cursor"
            )


@dataclass(frozen=True, slots=True)
class _PinnedReceipt:
    receipt_id: bytes
    revision: int
    channel: bytes
    state: str


class CurrentProjectionArtifactRepository:
    """Read current artifact facts without exposing SQL or transaction internals."""

    @staticmethod
    def list_page(
        connector: SQLConnector,
        *,
        channel: bytes | None = None,
        receipt_id: bytes | None = None,
        cursor: bytes | None = None,
        page_limit: int = _MAX_PAGE_ITEMS,
    ) -> CurrentProjectionArtifactPage:
        """Return one bounded page from a current or already-pinned receipt.

        ``receipt_id=None`` is an initial read and pins the current sealed head.
        A continuation supplies the returned receipt identity and its exact
        nonterminal cursor.  Nonempty pages deliberately remain nonterminal;
        the following empty page is the terminal proof, keeping every physical
        scan at or below the hard cap of 128 rows.
        """

        exact_channel = (
            _DEFAULT_CHANNEL
            if channel is None and receipt_id is None
            else (
                None
                if channel is None
                else require_ascii_bytes(
                    channel,
                    field="current projection channel",
                    minimum=1,
                    maximum=64,
                )
            )
        )
        bound = _require_page_limit(page_limit)
        pinned_receipt = (
            None
            if receipt_id is None
            else require_uuid16(
                receipt_id,
                field="current projection receipt_id",
            )
        )
        after = (
            None
            if cursor is None
            else require_digest32(cursor, field="current projection cursor")
        )
        if pinned_receipt is None and after is not None:
            raise CurrentProjectionCursorError(
                "an initial current-projection page cannot supply a cursor"
            )

        pin = _load_receipt_pin(
            connector,
            channel=exact_channel,
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
            raise CurrentProjectionReadError(
                "current projection contains incomplete or corrupt durable facts"
            ) from error
        if len(items) > bound:
            raise CurrentProjectionReadError(
                "current projection query exceeded its server-owned bound"
            )
        keys = tuple(item.publication_key for item in items)
        if keys != tuple(sorted(set(keys))):
            raise CurrentProjectionReadError(
                "current projection query returned unordered or duplicate keys"
            )
        if after is not None and keys and keys[0] <= after:
            raise CurrentProjectionReadError(
                "current projection query did not advance its keyset cursor"
            )
        next_cursor = None if not items else items[-1].publication_key
        return CurrentProjectionArtifactPage(
            pin.receipt_id,
            pin.revision,
            items,
            next_cursor,
            not items,
        )


def _require_page_limit(value: object) -> int:
    limit = require_positive_int63(value, field="current projection page_limit")
    if limit > _MAX_PAGE_ITEMS:
        raise ValueError("current projection page_limit must not exceed 128")
    return limit


def _load_receipt_pin(
    connector: SQLConnector,
    *,
    channel: bytes | None,
    receipt_id: bytes | None,
) -> _PinnedReceipt:
    if receipt_id is None:
        if channel is None:
            raise CurrentProjectionCursorError(
                "an initial current-projection page requires a channel"
            )
        row = connector.fetch_one(
            "SELECT head.receipt_id, receipt.revision, receipt.channel, "
            "receipt.state FROM catalog_publication_commit_head_receipts AS head "
            "JOIN catalog_publication_receipts AS receipt "
            "ON receipt.receipt_id = head.receipt_id "
            "WHERE head.channel = %s AND receipt.channel = head.channel",
            (channel,),
        )
        missing = "current sealed publication head is unavailable"
    else:
        row = connector.fetch_one(
            "SELECT receipt.receipt_id, receipt.revision, receipt.channel, "
            "receipt.state FROM catalog_publication_receipts AS receipt "
            "WHERE receipt.receipt_id = %s",
            (receipt_id,),
        )
        missing = "pinned publication receipt is unavailable"
    if len(row) != 4:
        raise CurrentProjectionUnavailableError(missing)
    try:
        stored_receipt = require_uuid16(
            row[0],
            field="stored current projection receipt_id",
        )
        revision = require_positive_int63(
            row[1],
            field="stored current projection catalog revision",
        )
        stored_channel = require_ascii_bytes(
            row[2],
            field="stored current projection channel",
            minimum=1,
            maximum=64,
        )
    except (TypeError, ValueError) as error:
        raise CurrentProjectionReadError(
            "current projection receipt contains invalid durable facts"
        ) from error
    state = row[3]
    if not isinstance(state, str) or state not in _PUBLICATION_RECEIPT_STATES:
        raise CurrentProjectionUnavailableError(
            "publication receipt is not readable by the current projection"
        )
    if receipt_id is not None and stored_receipt != receipt_id:
        raise CurrentProjectionReadError(
            "pinned publication receipt lookup returned another identity"
        )
    if channel is not None and stored_channel != channel:
        raise CurrentProjectionCursorError(
            "pinned publication receipt belongs to another channel"
        )
    return _PinnedReceipt(stored_receipt, revision, stored_channel, state)


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
        raise CurrentProjectionCursorError(
            "current projection cursor does not name one exact artifact"
        )
    try:
        item = _item_from_row(rows[0])
    except (TypeError, UnicodeError, ValueError) as error:
        raise CurrentProjectionCursorError(
            "current projection cursor names corrupt durable facts"
        ) from error
    if item.publication_key != cursor:
        raise CurrentProjectionCursorError(
            "current projection cursor lookup returned another key"
        )


_ARTIFACT_SELECT = (
    "SELECT artifact.publication_key, publication_seal.publication_key, "
    "title_seal.publication_key, publication_identity.gid, "
    "title_name.source_gallery_name, upload.upload_time, "
    "artifact.artifact_sha256, artifact_blob.artifact_locator_sha256, "
    "artifact_blob.size_bytes FROM catalog_artifacts AS artifact "
    "LEFT JOIN catalog_publication_seals AS publication_seal "
    "ON publication_seal.revision = artifact.revision "
    "AND publication_seal.publication_key = artifact.publication_key "
    "LEFT JOIN catalog_publication_identities AS publication_identity "
    "ON publication_identity.publication_key = artifact.publication_key "
    "LEFT JOIN catalog_gallery_upload_times AS upload "
    "ON upload.gid = publication_identity.gid "
    "LEFT JOIN catalog_publication_title_seals AS title_seal "
    "ON title_seal.revision = artifact.revision "
    "AND title_seal.publication_key = artifact.publication_key "
    "LEFT JOIN catalog_publication_title_source_gallery_names AS title_name "
    "ON title_name.revision = title_seal.revision "
    "AND title_name.publication_key = title_seal.publication_key "
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
        predicate = "WHERE artifact.revision = %s " "AND artifact.publication_key > %s "
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


def _item_from_row(row: tuple[Any, ...]) -> CurrentProjectionArtifactItem:
    if len(row) != 9 or any(value is None for value in row):
        raise ValueError("current projection artifact row has an invalid shape")
    publication_key = require_digest32(
        row[0],
        field="stored current projection publication_key",
    )
    if row[1] != publication_key or row[2] != publication_key:
        raise ValueError(
            "current projection artifact lacks its sealed publication/title family"
        )
    gid = require_positive_int63(row[3], field="stored current projection gid")
    source_name = require_utf8_bytes(
        row[4],
        field="stored current projection source_gallery_name",
        minimum=1,
        maximum=255,
        reject_nul=True,
    ).decode("utf-8", errors="strict")
    upload_time = require_int63(
        row[5],
        field="stored current projection upload_time",
    )
    artifact_sha256 = require_digest32(
        row[6],
        field="stored current projection artifact_sha256",
    )
    locator_sha256 = require_digest32(
        row[7],
        field="stored current projection artifact_locator_sha256",
    )
    size_bytes = require_int63(
        row[8],
        field="stored current projection size_bytes",
    )
    locator_components = identity.artifact_locator_components(artifact_sha256)
    if identity.artifact_locator_digest(locator_components) != locator_sha256:
        raise ValueError(
            "current projection artifact locator disagrees with content bytes"
        )
    return CurrentProjectionArtifactItem(
        publication_key,
        gid,
        source_name,
        upload_time,
        locator_components,
        artifact_sha256,
        size_bytes,
    )
