"""Exact storage protocol for the vertically split canonical-value families.

This module is deliberately below the workflow repositories.  Callers must
already hold their mutable transaction authority before invoking a writer;
immutable allocation and page members are then read without ``FOR UPDATE``.
Every family insert writes its seal last, and an existing incomplete family is
corruption rather than an invitation to repair it.
"""

from __future__ import annotations

__all__ = [
    "CanonicalValueAllocation",
    "CanonicalValueCollisionError",
    "CanonicalValueNotReadyError",
    "CanonicalValuePageCoordinate",
    "CanonicalValuePageFamily",
    "CanonicalValuePageEnsureReceipt",
    "CanonicalValuePartialFamilyError",
    "CanonicalValueReadReceipt",
    "ensure_allocation_family",
    "ensure_canonical_value_identity",
    "ensure_exact_page_parent_edges",
    "ensure_page_family",
    "load_allocation_family",
    "load_page_families",
    "load_page_family",
    "load_page_family_by_coordinate",
    "load_sealed_allocation_allocated_at",
    "load_sealed_allocation_byte_count",
    "load_sealed_allocation_digest_domain",
    "load_sealed_page_coordinate",
    "load_sealed_page_payload",
    "load_sealed_page_subtree_item_count",
    "load_sealed_value_identities",
    "load_sealed_value_identity",
    "persist_in_memory_canonical_value",
    "validate_exact_page_parent_edges",
    "validate_exact_page_parent_edges_batched",
]

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, cast

from .vnext_domains import (
    require_ascii_bytes,
    require_bounded_bytes,
    require_digest32,
    require_int63,
)
from .vnext_identity import (
    CANONICAL_VALUE_CHUNK_BYTES,
    CanonicalValueBranchEntry,
    GalleryObservationNodeKind,
    build_canonical_value_tree,
    canonical_value_digest,
    canonical_value_page_digest,
    decode_canonical_value_page,
)
from .vnext_transaction import VNextUnitOfWork

_ALLOCATION_ANCHOR = "catalog_canonical_value_allocation_anchors"
_ALLOCATION_DOMAIN = "catalog_canonical_value_allocation_digest_domains"
_ALLOCATION_COUNT = "catalog_canonical_value_allocation_byte_counts"
_ALLOCATION_TIME = "catalog_canonical_value_allocation_allocated_ats"
_ALLOCATION_SEAL = "catalog_canonical_value_allocation_seals"

_PAGE_ANCHOR = "catalog_canonical_value_page_anchors"
_PAGE_PAYLOAD = "catalog_canonical_value_page_payloads"
_PAGE_COORDINATE = "catalog_canonical_value_page_coordinates"
_PAGE_COUNT = "catalog_canonical_value_page_subtree_item_counts"
_PAGE_SEAL = "catalog_canonical_value_page_seals"
_PAGE_PARENT = "catalog_canonical_value_page_parents"
_VALUE_IDENTITY = "catalog_canonical_value_identities"
_UPLOAD = "operational_canonical_value_uploads"

_PAGE_RECEIPT_TOKEN = object()
_IN_MEMORY_VALUE_MAXIMUM_BYTES = 64 * 1024
_READ_BATCH_LIMIT = 128


class CanonicalValueCollisionError(RuntimeError):
    """A complete immutable canonical tuple disagrees with exact authority."""


class CanonicalValuePartialFamilyError(CanonicalValueCollisionError):
    """At least one member exists, but the sealed family is incomplete."""


class CanonicalValueNotReadyError(RuntimeError):
    """A required canonical family, policy, claim, or identity is absent."""


@dataclass(frozen=True, slots=True)
class CanonicalValueAllocation:
    value_sha256: bytes
    digest_domain: bytes
    byte_count: int
    allocated_at: int

    def __post_init__(self) -> None:
        require_digest32(self.value_sha256, field="value_sha256")
        require_ascii_bytes(
            self.digest_domain,
            field="digest_domain",
            minimum=1,
            maximum=64,
        )
        require_int63(self.byte_count, field="byte_count")
        require_int63(self.allocated_at, field="allocated_at")


@dataclass(frozen=True, slots=True)
class CanonicalValueReadReceipt:
    value_sha256: bytes
    digest_domain: bytes
    byte_count: int
    root_page_sha256: bytes

    def __post_init__(self) -> None:
        require_digest32(self.value_sha256, field="value_sha256")
        require_ascii_bytes(
            self.digest_domain,
            field="digest_domain",
            minimum=1,
            maximum=64,
        )
        require_int63(self.byte_count, field="byte_count")
        require_digest32(self.root_page_sha256, field="root_page_sha256")


@dataclass(frozen=True, slots=True)
class CanonicalValuePageCoordinate:
    value_sha256: bytes
    level: int
    page_position: int

    def __post_init__(self) -> None:
        require_digest32(self.value_sha256, field="page owner value_sha256")
        level = require_int63(self.level, field="canonical page level")
        if level > 8:
            raise ValueError("canonical page level must be in 0..8")
        require_int63(self.page_position, field="canonical page position")


@dataclass(frozen=True, slots=True)
class CanonicalValuePageFamily:
    page_sha256: bytes
    page_bytes: bytes
    coordinate: CanonicalValuePageCoordinate
    subtree_item_count: int

    def __post_init__(self) -> None:
        digest = require_digest32(self.page_sha256, field="page_sha256")
        payload = require_bounded_bytes(
            self.page_bytes,
            field="canonical page_bytes",
            minimum=1,
            maximum=64 * 1024,
        )
        if type(self.coordinate) is not CanonicalValuePageCoordinate:
            raise TypeError("coordinate must be an exact CanonicalValuePageCoordinate")
        self.coordinate.__post_init__()
        count = require_int63(
            self.subtree_item_count,
            field="canonical page subtree_item_count",
        )
        if canonical_value_page_digest(payload) != digest:
            raise ValueError("canonical page digest does not match page_bytes")
        decoded = decode_canonical_value_page(payload)
        if (
            decoded.owner_value_sha256,
            decoded.level,
            decoded.page_position,
            decoded.subtree_byte_count,
        ) != (
            self.coordinate.value_sha256,
            self.coordinate.level,
            self.coordinate.page_position,
            count,
        ):
            raise ValueError("canonical page facts disagree with decoded page_bytes")

    @classmethod
    def from_payload(
        cls,
        *,
        page_sha256: bytes,
        page_bytes: bytes,
    ) -> CanonicalValuePageFamily:
        digest = require_digest32(page_sha256, field="page_sha256")
        payload = require_bounded_bytes(
            page_bytes,
            field="canonical page_bytes",
            minimum=1,
            maximum=64 * 1024,
        )
        decoded = decode_canonical_value_page(payload)
        return cls(
            digest,
            payload,
            CanonicalValuePageCoordinate(
                decoded.owner_value_sha256,
                decoded.level,
                decoded.page_position,
            ),
            decoded.subtree_byte_count,
        )


@dataclass(frozen=True, slots=True)
class CanonicalValuePageEnsureReceipt:
    page: CanonicalValuePageFamily
    created: bool
    _capability: object = field(repr=False, compare=False)


def load_allocation_family(
    connector: Any,
    *,
    value_sha256: bytes,
) -> CanonicalValueAllocation | None:
    """Load one complete allocation or reject any persisted partial family."""

    value = require_digest32(value_sha256, field="value_sha256")
    row = connector.fetch_one(
        f"WITH family_keys(value_sha256) AS ("
        f"SELECT value_sha256 FROM {_ALLOCATION_ANCHOR} WHERE value_sha256 = %s "
        "UNION "
        f"SELECT value_sha256 FROM {_ALLOCATION_DOMAIN} WHERE value_sha256 = %s "
        "UNION "
        f"SELECT value_sha256 FROM {_ALLOCATION_COUNT} WHERE value_sha256 = %s "
        "UNION "
        f"SELECT value_sha256 FROM {_ALLOCATION_TIME} WHERE value_sha256 = %s "
        "UNION "
        f"SELECT value_sha256 FROM {_ALLOCATION_SEAL} WHERE value_sha256 = %s) "
        "SELECT k.value_sha256, a.value_sha256, d.value_sha256, d.digest_domain, "
        "c.value_sha256, c.byte_count, t.value_sha256, t.allocated_at, "
        "s.value_sha256 FROM family_keys k "
        f"LEFT JOIN {_ALLOCATION_ANCHOR} a ON a.value_sha256 = k.value_sha256 "
        f"LEFT JOIN {_ALLOCATION_DOMAIN} d ON d.value_sha256 = k.value_sha256 "
        f"LEFT JOIN {_ALLOCATION_COUNT} c ON c.value_sha256 = k.value_sha256 "
        f"LEFT JOIN {_ALLOCATION_TIME} t ON t.value_sha256 = k.value_sha256 "
        f"LEFT JOIN {_ALLOCATION_SEAL} s ON s.value_sha256 = k.value_sha256",
        (value, value, value, value, value),
    )
    if not row:
        return None
    if len(row) != 9 or any(row[index] != value for index in (0, 1, 2, 4, 6, 8)):
        raise CanonicalValuePartialFamilyError(
            "canonical allocation has an existing incomplete sealed family"
        )
    try:
        return CanonicalValueAllocation(value, row[3], row[5], row[7])
    except (TypeError, ValueError) as error:
        raise CanonicalValueCollisionError(
            "canonical allocation contains an invalid immutable fact"
        ) from error


def ensure_allocation_family(
    connector: Any,
    *,
    value_sha256: bytes,
    digest_domain: bytes,
    byte_count: int,
    allocated_at: int,
) -> CanonicalValueAllocation:
    """Insert one allocation seal-last or replay it without changing its time."""

    proposed = CanonicalValueAllocation(
        value_sha256,
        digest_domain,
        byte_count,
        allocated_at,
    )
    existing = load_allocation_family(
        connector,
        value_sha256=proposed.value_sha256,
    )
    if existing is not None:
        if (existing.digest_domain, existing.byte_count) != (
            proposed.digest_domain,
            proposed.byte_count,
        ):
            raise CanonicalValueCollisionError(
                "canonical allocation conflicts with its immutable preimage"
            )
        # allocated_at is the first successful allocation time, not replay input.
        return existing
    connector.execute(
        f"INSERT INTO {_ALLOCATION_ANCHOR} (value_sha256) VALUES (%s)",
        (proposed.value_sha256,),
    )
    connector.execute(
        f"INSERT INTO {_ALLOCATION_DOMAIN} "
        "(value_sha256, digest_domain) VALUES (%s, %s)",
        (proposed.value_sha256, proposed.digest_domain),
    )
    connector.execute(
        f"INSERT INTO {_ALLOCATION_COUNT} (value_sha256, byte_count) VALUES (%s, %s)",
        (proposed.value_sha256, proposed.byte_count),
    )
    connector.execute(
        f"INSERT INTO {_ALLOCATION_TIME} (value_sha256, allocated_at) VALUES (%s, %s)",
        (proposed.value_sha256, proposed.allocated_at),
    )
    connector.execute(
        f"INSERT INTO {_ALLOCATION_SEAL} (value_sha256) VALUES (%s)",
        (proposed.value_sha256,),
    )
    return proposed


def load_page_family(
    connector: Any,
    *,
    page_sha256: bytes,
) -> CanonicalValuePageFamily | None:
    """Load a full page family by digest, including derived-fact congruence."""

    digest = require_digest32(page_sha256, field="page_sha256")
    row = connector.fetch_one(
        f"WITH family_keys(page_sha256) AS ("
        f"SELECT page_sha256 FROM {_PAGE_ANCHOR} WHERE page_sha256 = %s "
        "UNION "
        f"SELECT page_sha256 FROM {_PAGE_PAYLOAD} WHERE page_sha256 = %s "
        "UNION "
        f"SELECT page_sha256 FROM {_PAGE_COORDINATE} WHERE page_sha256 = %s "
        "UNION "
        f"SELECT page_sha256 FROM {_PAGE_COUNT} WHERE page_sha256 = %s "
        "UNION "
        f"SELECT page_sha256 FROM {_PAGE_SEAL} WHERE page_sha256 = %s) "
        "SELECT k.page_sha256, a.page_sha256, p.page_sha256, p.page_bytes, "
        "c.page_sha256, c.value_sha256, c.level, c.page_position, "
        "n.page_sha256, n.subtree_item_count, s.page_sha256 "
        "FROM family_keys k "
        f"LEFT JOIN {_PAGE_ANCHOR} a ON a.page_sha256 = k.page_sha256 "
        f"LEFT JOIN {_PAGE_PAYLOAD} p ON p.page_sha256 = k.page_sha256 "
        f"LEFT JOIN {_PAGE_COORDINATE} c ON c.page_sha256 = k.page_sha256 "
        f"LEFT JOIN {_PAGE_COUNT} n ON n.page_sha256 = k.page_sha256 "
        f"LEFT JOIN {_PAGE_SEAL} s ON s.page_sha256 = k.page_sha256",
        (digest, digest, digest, digest, digest),
    )
    if not row:
        return None
    if len(row) != 11 or any(row[index] != digest for index in (0, 1, 2, 4, 8, 10)):
        raise CanonicalValuePartialFamilyError(
            "canonical page has an existing incomplete sealed family"
        )
    try:
        return CanonicalValuePageFamily(
            digest,
            row[3],
            CanonicalValuePageCoordinate(row[5], row[6], row[7]),
            row[9],
        )
    except (TypeError, ValueError) as error:
        raise CanonicalValueCollisionError(
            "canonical page facts do not recompute their digest and coordinate"
        ) from error


def load_page_families(
    connector: Any,
    *,
    page_sha256s: Sequence[bytes],
) -> dict[bytes, CanonicalValuePageFamily]:
    """Load at most 128 complete page families in one bounded query."""

    if len(page_sha256s) > _READ_BATCH_LIMIT:
        raise ValueError("canonical page-family batch is limited to 128 pages")
    digests = tuple(
        sorted(
            {
                require_digest32(page_sha256, field="page_sha256")
                for page_sha256 in page_sha256s
            }
        )
    )
    if not digests:
        return {}
    placeholders = ", ".join("%s" for _ in digests)
    rows = connector.fetch_all(
        f"WITH family_keys(page_sha256) AS ("
        f"SELECT page_sha256 FROM {_PAGE_ANCHOR} "
        f"WHERE page_sha256 IN ({placeholders}) UNION "
        f"SELECT page_sha256 FROM {_PAGE_PAYLOAD} "
        f"WHERE page_sha256 IN ({placeholders}) UNION "
        f"SELECT page_sha256 FROM {_PAGE_COORDINATE} "
        f"WHERE page_sha256 IN ({placeholders}) UNION "
        f"SELECT page_sha256 FROM {_PAGE_COUNT} "
        f"WHERE page_sha256 IN ({placeholders}) UNION "
        f"SELECT page_sha256 FROM {_PAGE_SEAL} "
        f"WHERE page_sha256 IN ({placeholders})) "
        "SELECT k.page_sha256, a.page_sha256, p.page_sha256, p.page_bytes, "
        "c.page_sha256, c.value_sha256, c.level, c.page_position, "
        "n.page_sha256, n.subtree_item_count, s.page_sha256 "
        "FROM family_keys k "
        f"LEFT JOIN {_PAGE_ANCHOR} a ON a.page_sha256 = k.page_sha256 "
        f"LEFT JOIN {_PAGE_PAYLOAD} p ON p.page_sha256 = k.page_sha256 "
        f"LEFT JOIN {_PAGE_COORDINATE} c ON c.page_sha256 = k.page_sha256 "
        f"LEFT JOIN {_PAGE_COUNT} n ON n.page_sha256 = k.page_sha256 "
        f"LEFT JOIN {_PAGE_SEAL} s ON s.page_sha256 = k.page_sha256 "
        "ORDER BY k.page_sha256",
        digests * 5,
    )
    expected = set(digests)
    result: dict[bytes, CanonicalValuePageFamily] = {}
    for row in rows:
        if len(row) != 11:
            raise CanonicalValuePartialFamilyError(
                "canonical page has an invalid physical shape"
            )
        digest = require_digest32(row[0], field="page_sha256")
        if digest not in expected or digest in result:
            raise CanonicalValueCollisionError(
                "canonical page-family batch returned an unexpected duplicate"
            )
        if any(row[index] != digest for index in (1, 2, 4, 8, 10)):
            raise CanonicalValuePartialFamilyError(
                "canonical page has an existing incomplete sealed family"
            )
        try:
            result[digest] = CanonicalValuePageFamily(
                digest,
                row[3],
                CanonicalValuePageCoordinate(row[5], row[6], row[7]),
                row[9],
            )
        except (TypeError, ValueError) as error:
            raise CanonicalValueCollisionError(
                "canonical page facts do not recompute their digest and coordinate"
            ) from error
    return result


def load_page_family_by_coordinate(
    connector: Any,
    *,
    value_sha256: bytes,
    level: int,
    page_position: int,
) -> CanonicalValuePageFamily | None:
    coordinate = CanonicalValuePageCoordinate(value_sha256, level, page_position)
    row = connector.fetch_one(
        f"SELECT page_sha256 FROM {_PAGE_COORDINATE} "
        "WHERE value_sha256 = %s AND level = %s AND page_position = %s",
        (
            coordinate.value_sha256,
            coordinate.level,
            coordinate.page_position,
        ),
    )
    if not row:
        return None
    if len(row) != 1:
        raise CanonicalValuePartialFamilyError(
            "canonical page coordinate has an invalid physical shape"
        )
    page = load_page_family(connector, page_sha256=row[0])
    if page is None or page.coordinate != coordinate:
        raise CanonicalValuePartialFamilyError(
            "canonical page coordinate does not resolve a complete exact page"
        )
    return page


def ensure_page_family(
    connector: Any,
    *,
    page: CanonicalValuePageFamily,
) -> CanonicalValuePageEnsureReceipt:
    """Insert one page's four facts seal-last, checking both candidate keys."""

    if type(page) is not CanonicalValuePageFamily:
        raise TypeError("page must be an exact CanonicalValuePageFamily")
    page.__post_init__()
    by_digest = load_page_family(connector, page_sha256=page.page_sha256)
    by_coordinate = load_page_family_by_coordinate(
        connector,
        value_sha256=page.coordinate.value_sha256,
        level=page.coordinate.level,
        page_position=page.coordinate.page_position,
    )
    if by_digest is not None or by_coordinate is not None:
        if by_digest != page or by_coordinate != page:
            raise CanonicalValueCollisionError(
                "canonical page digest or natural coordinate already names another page"
            )
        return CanonicalValuePageEnsureReceipt(page, False, _PAGE_RECEIPT_TOKEN)
    connector.execute(
        f"INSERT INTO {_PAGE_ANCHOR} (page_sha256) VALUES (%s)",
        (page.page_sha256,),
    )
    connector.execute(
        f"INSERT INTO {_PAGE_PAYLOAD} (page_sha256, page_bytes) VALUES (%s, %s)",
        (page.page_sha256, page.page_bytes),
    )
    connector.execute(
        f"INSERT INTO {_PAGE_COORDINATE} "
        "(value_sha256, level, page_position, page_sha256) "
        "VALUES (%s, %s, %s, %s)",
        (
            page.coordinate.value_sha256,
            page.coordinate.level,
            page.coordinate.page_position,
            page.page_sha256,
        ),
    )
    connector.execute(
        f"INSERT INTO {_PAGE_COUNT} (page_sha256, subtree_item_count) VALUES (%s, %s)",
        (page.page_sha256, page.subtree_item_count),
    )
    connector.execute(
        f"INSERT INTO {_PAGE_SEAL} (page_sha256) VALUES (%s)",
        (page.page_sha256,),
    )
    return CanonicalValuePageEnsureReceipt(page, True, _PAGE_RECEIPT_TOKEN)


def _expected_page_parent_rows(
    page: CanonicalValuePageFamily,
) -> tuple[tuple[bytes, int, bytes], ...]:
    decoded = decode_canonical_value_page(page.page_bytes)
    if decoded.node_kind is GalleryObservationNodeKind.LEAF:
        return ()
    rows: list[tuple[bytes, int, bytes]] = []
    for position, entry in enumerate(decoded.entries):
        if not isinstance(entry, CanonicalValueBranchEntry):
            raise CanonicalValueCollisionError(
                "canonical branch contains a non-branch descriptor"
            )
        rows.append((page.page_sha256, position, entry.child_page_sha256))
    return tuple(rows)


def validate_exact_page_parent_edges(
    connector: Any,
    *,
    page: CanonicalValuePageFamily,
) -> None:
    """Require the entire outgoing edge set, including absence of extras."""

    if type(page) is not CanonicalValuePageFamily:
        raise TypeError("page must be an exact CanonicalValuePageFamily")
    expected = _expected_page_parent_rows(page)
    rows = connector.fetch_all(
        f"SELECT parent_sha256, position, child_sha256 FROM {_PAGE_PARENT} "
        "WHERE parent_sha256 = %s ORDER BY position",
        (page.page_sha256,),
    )
    if tuple(rows) != expected:
        raise CanonicalValueCollisionError(
            "canonical page has a missing, changed, or extra parent edge"
        )


def validate_exact_page_parent_edges_batched(
    connector: Any,
    *,
    pages: Sequence[CanonicalValuePageFamily],
) -> None:
    """Validate complete outgoing edge sets for at most 128 sealed pages."""

    if len(pages) > _READ_BATCH_LIMIT:
        raise ValueError("canonical parent-edge batch is limited to 128 pages")
    exact_pages: dict[bytes, CanonicalValuePageFamily] = {}
    for page in pages:
        if type(page) is not CanonicalValuePageFamily:
            raise TypeError("page must be an exact CanonicalValuePageFamily")
        page.__post_init__()
        if page.page_sha256 in exact_pages:
            raise ValueError("canonical parent-edge batch contains a duplicate page")
        exact_pages[page.page_sha256] = page
    if not exact_pages:
        return
    digests = tuple(sorted(exact_pages))
    rows = connector.fetch_all(
        f"SELECT parent_sha256, position, child_sha256 FROM {_PAGE_PARENT} "
        f"WHERE parent_sha256 IN ({', '.join('%s' for _ in digests)}) "
        "ORDER BY parent_sha256, position",
        digests,
    )
    actual: dict[bytes, list[tuple[bytes, int, bytes]]] = {}
    for row in rows:
        if len(row) != 3:
            raise CanonicalValueCollisionError(
                "canonical parent edge has an invalid physical shape"
            )
        try:
            parent = require_digest32(row[0], field="parent_sha256")
            position = require_int63(row[1], field="parent edge position")
            child = require_digest32(row[2], field="child_sha256")
        except (TypeError, ValueError) as error:
            raise CanonicalValueCollisionError(
                "canonical parent edge contains an invalid immutable fact"
            ) from error
        if parent not in exact_pages:
            raise CanonicalValueCollisionError(
                "canonical parent-edge batch returned an unexpected parent"
            )
        actual.setdefault(parent, []).append((parent, position, child))
    for digest, page in exact_pages.items():
        if tuple(actual.get(digest, ())) != _expected_page_parent_rows(page):
            raise CanonicalValueCollisionError(
                "canonical page has a missing, changed, or extra parent edge"
            )


def ensure_exact_page_parent_edges(
    connector: Any,
    *,
    receipt: CanonicalValuePageEnsureReceipt,
) -> None:
    """Insert edges only for a page created by this call; replays only validate."""

    if (
        type(receipt) is not CanonicalValuePageEnsureReceipt
        or receipt._capability is not _PAGE_RECEIPT_TOKEN
    ):
        raise TypeError("receipt was not issued by ensure_page_family")
    expected = _expected_page_parent_rows(receipt.page)
    rows = connector.fetch_all(
        f"SELECT parent_sha256, position, child_sha256 FROM {_PAGE_PARENT} "
        "WHERE parent_sha256 = %s ORDER BY position",
        (receipt.page.page_sha256,),
    )
    if not receipt.created:
        if tuple(rows) != expected:
            raise CanonicalValueCollisionError(
                "existing canonical branch has a missing, changed, or extra edge"
            )
        return
    if rows:
        raise CanonicalValueCollisionError(
            "new canonical page already has an outgoing edge"
        )
    if not expected:
        return
    children = tuple(row[2] for row in expected)
    placeholders = ", ".join("%s" for _ in children)
    sealed_children = connector.fetch_all(
        f"SELECT page_sha256 FROM {_PAGE_SEAL} "
        f"WHERE page_sha256 IN ({placeholders}) ORDER BY page_sha256",
        children,
    )
    if tuple(row[0] for row in sealed_children) != tuple(sorted(children)):
        raise CanonicalValueNotReadyError(
            "canonical branch child page family is not sealed"
        )
    incoming = connector.fetch_all(
        f"SELECT parent_sha256, position, child_sha256 FROM {_PAGE_PARENT} "
        f"WHERE child_sha256 IN ({placeholders}) ORDER BY child_sha256",
        children,
    )
    if incoming:
        raise CanonicalValueCollisionError(
            "canonical branch child already belongs to another parent position"
        )
    for row in expected:
        connector.execute(
            f"INSERT INTO {_PAGE_PARENT} "
            "(parent_sha256, position, child_sha256) VALUES (%s, %s, %s)",
            row,
        )


def ensure_canonical_value_identity(
    connector: Any,
    *,
    value_sha256: bytes,
    root_page_sha256: bytes,
) -> None:
    value = require_digest32(value_sha256, field="value_sha256")
    root = require_digest32(root_page_sha256, field="root_page_sha256")
    expected = (value, root)
    identity = connector.fetch_one(
        f"SELECT value_sha256, root_page_sha256 FROM {_VALUE_IDENTITY} "
        "WHERE value_sha256 = %s",
        (value,),
    )
    reverse = connector.fetch_one(
        f"SELECT value_sha256, root_page_sha256 FROM {_VALUE_IDENTITY} "
        "WHERE root_page_sha256 = %s",
        (root,),
    )
    if identity or reverse:
        if identity != expected or reverse != expected:
            raise CanonicalValueCollisionError(
                "canonical value or root identity conflicts with its exact tuple"
            )
        return
    connector.execute(
        f"INSERT INTO {_VALUE_IDENTITY} "
        "(value_sha256, root_page_sha256) VALUES (%s, %s)",
        expected,
    )


def _identity_rows(
    connector: Any,
    values: tuple[bytes, ...],
) -> list[tuple[Any, ...]]:
    placeholders = ", ".join("%s" for _ in values)
    return cast(
        list[tuple[Any, ...]],
        connector.fetch_all(
            "SELECT a.value_sha256, d.digest_domain, c.byte_count, "
            "i.root_page_sha256, a.value_sha256, d.value_sha256, "
            "c.value_sha256, t.value_sha256, s.value_sha256, "
            "ps.page_sha256, pp.page_sha256, py.page_sha256, "
            "px.value_sha256, px.level, px.page_position, pn.page_sha256, "
            "pn.subtree_item_count "
            f"FROM {_ALLOCATION_ANCHOR} a "
            f"LEFT JOIN {_ALLOCATION_DOMAIN} d ON d.value_sha256 = a.value_sha256 "
            f"LEFT JOIN {_ALLOCATION_COUNT} c ON c.value_sha256 = a.value_sha256 "
            f"LEFT JOIN {_ALLOCATION_TIME} t ON t.value_sha256 = a.value_sha256 "
            f"LEFT JOIN {_ALLOCATION_SEAL} s ON s.value_sha256 = a.value_sha256 "
            f"LEFT JOIN {_VALUE_IDENTITY} i ON i.value_sha256 = a.value_sha256 "
            f"LEFT JOIN {_PAGE_SEAL} ps ON ps.page_sha256 = i.root_page_sha256 "
            f"LEFT JOIN {_PAGE_ANCHOR} pp ON pp.page_sha256 = i.root_page_sha256 "
            f"LEFT JOIN {_PAGE_PAYLOAD} py ON py.page_sha256 = i.root_page_sha256 "
            f"LEFT JOIN {_PAGE_COORDINATE} px ON px.page_sha256 = i.root_page_sha256 "
            f"LEFT JOIN {_PAGE_COUNT} pn ON pn.page_sha256 = i.root_page_sha256 "
            f"WHERE a.value_sha256 IN ({placeholders}) ORDER BY a.value_sha256",
            values,
        ),
    )


def load_sealed_value_identities(
    connector: Any,
    *,
    value_sha256s: Sequence[bytes],
) -> dict[bytes, CanonicalValueReadReceipt]:
    """Batch-load complete allocation+identity+root-page authority."""

    if len(value_sha256s) > 256:
        raise ValueError("canonical identity batch is limited to 256 values")
    values = tuple(
        sorted(
            {require_digest32(value, field="value_sha256") for value in value_sha256s}
        )
    )
    if not values:
        return {}
    receipts: dict[bytes, CanonicalValueReadReceipt] = {}
    for row in _identity_rows(connector, values):
        if len(row) != 17:
            raise CanonicalValuePartialFamilyError(
                "canonical identity projection has an invalid physical shape"
            )
        value = require_digest32(row[0], field="value_sha256")
        # Allocation members and its seal are mandatory once the anchor exists.
        if any(row[index] != value for index in (4, 5, 6, 7, 8)):
            raise CanonicalValuePartialFamilyError(
                "canonical identity references an incomplete allocation family"
            )
        if row[3] is None:
            continue
        root = require_digest32(row[3], field="root_page_sha256")
        if any(row[index] != root for index in (9, 10, 11, 15)):
            raise CanonicalValuePartialFamilyError(
                "canonical identity references an incomplete root page family"
            )
        if row[12] != value:
            raise CanonicalValueCollisionError(
                "canonical root page belongs to another allocation"
            )
        try:
            domain = require_ascii_bytes(
                row[1], field="digest_domain", minimum=1, maximum=64
            )
            byte_count = require_int63(row[2], field="byte_count")
            level = require_int63(row[13], field="root level")
            position = require_int63(row[14], field="root page_position")
            subtree_count = require_int63(row[16], field="root subtree count")
        except (TypeError, ValueError) as error:
            raise CanonicalValueCollisionError(
                "canonical identity contains an invalid immutable fact"
            ) from error
        if (
            position != 0
            or subtree_count != byte_count
            or level != _expected_root_level(byte_count)
        ):
            raise CanonicalValueCollisionError(
                "canonical root coordinate or count disagrees with allocation"
            )
        receipts[value] = CanonicalValueReadReceipt(
            value,
            domain,
            byte_count,
            root,
        )
    return receipts


def load_sealed_value_identity(
    connector: Any,
    *,
    value_sha256: bytes,
) -> CanonicalValueReadReceipt | None:
    value = require_digest32(value_sha256, field="value_sha256")
    return load_sealed_value_identities(
        connector,
        value_sha256s=(value,),
    ).get(value)


def _load_narrow_fact(
    connector: Any,
    *,
    seal_table: str,
    fact_table: str,
    key_column: str,
    fact_column: str,
    key: bytes,
) -> Any | None:
    row = connector.fetch_one(
        f"SELECT s.{key_column}, f.{fact_column} FROM {seal_table} s "
        f"LEFT JOIN {fact_table} f ON f.{key_column} = s.{key_column} "
        f"WHERE s.{key_column} = %s",
        (key,),
    )
    if not row:
        return None
    if len(row) != 2 or row[0] != key or row[1] is None:
        raise CanonicalValuePartialFamilyError(
            f"sealed canonical fact {fact_column} is missing"
        )
    return row[1]


def load_sealed_allocation_digest_domain(
    connector: Any, *, value_sha256: bytes
) -> bytes | None:
    value = require_digest32(value_sha256, field="value_sha256")
    result = _load_narrow_fact(
        connector,
        seal_table=_ALLOCATION_SEAL,
        fact_table=_ALLOCATION_DOMAIN,
        key_column="value_sha256",
        fact_column="digest_domain",
        key=value,
    )
    if result is None:
        return None
    return require_ascii_bytes(result, field="digest_domain", minimum=1, maximum=64)


def load_sealed_allocation_byte_count(
    connector: Any, *, value_sha256: bytes
) -> int | None:
    value = require_digest32(value_sha256, field="value_sha256")
    result = _load_narrow_fact(
        connector,
        seal_table=_ALLOCATION_SEAL,
        fact_table=_ALLOCATION_COUNT,
        key_column="value_sha256",
        fact_column="byte_count",
        key=value,
    )
    return None if result is None else require_int63(result, field="byte_count")


def load_sealed_allocation_allocated_at(
    connector: Any, *, value_sha256: bytes
) -> int | None:
    value = require_digest32(value_sha256, field="value_sha256")
    result = _load_narrow_fact(
        connector,
        seal_table=_ALLOCATION_SEAL,
        fact_table=_ALLOCATION_TIME,
        key_column="value_sha256",
        fact_column="allocated_at",
        key=value,
    )
    return None if result is None else require_int63(result, field="allocated_at")


def load_sealed_page_payload(connector: Any, *, page_sha256: bytes) -> bytes | None:
    digest = require_digest32(page_sha256, field="page_sha256")
    result = _load_narrow_fact(
        connector,
        seal_table=_PAGE_SEAL,
        fact_table=_PAGE_PAYLOAD,
        key_column="page_sha256",
        fact_column="page_bytes",
        key=digest,
    )
    if result is None:
        return None
    payload = require_bounded_bytes(
        result,
        field="canonical page_bytes",
        minimum=1,
        maximum=64 * 1024,
    )
    if canonical_value_page_digest(payload) != digest:
        raise CanonicalValueCollisionError(
            "sealed canonical page payload does not recompute its digest"
        )
    return payload


def load_sealed_page_coordinate(
    connector: Any, *, page_sha256: bytes
) -> CanonicalValuePageCoordinate | None:
    digest = require_digest32(page_sha256, field="page_sha256")
    row = connector.fetch_one(
        f"SELECT s.page_sha256, c.value_sha256, c.level, c.page_position "
        f"FROM {_PAGE_SEAL} s LEFT JOIN {_PAGE_COORDINATE} c "
        "ON c.page_sha256 = s.page_sha256 WHERE s.page_sha256 = %s",
        (digest,),
    )
    if not row:
        return None
    if len(row) != 4 or row[0] != digest or any(value is None for value in row[1:]):
        raise CanonicalValuePartialFamilyError(
            "sealed canonical page coordinate is missing"
        )
    return CanonicalValuePageCoordinate(row[1], row[2], row[3])


def load_sealed_page_subtree_item_count(
    connector: Any, *, page_sha256: bytes
) -> int | None:
    digest = require_digest32(page_sha256, field="page_sha256")
    result = _load_narrow_fact(
        connector,
        seal_table=_PAGE_SEAL,
        fact_table=_PAGE_COUNT,
        key_column="page_sha256",
        fact_column="subtree_item_count",
        key=digest,
    )
    return None if result is None else require_int63(result, field="subtree_item_count")


def persist_in_memory_canonical_value(
    work: VNextUnitOfWork,
    *,
    generation: int,
    digest_domain: str,
    payload: bytes,
    now: int,
    retain_claim: bool,
) -> CanonicalValueReadReceipt:
    """Persist a bounded value under authority already locked by the caller.

    This is the shared replacement for the staging repository's duplicate SQL
    protocol.  It intentionally does not reacquire gate or ingest-fence locks;
    the caller must pass the generation returned by its outer authorization in
    this same transaction.
    """

    exact_generation = require_int63(generation, field="generation")
    timestamp = require_int63(now, field="now")
    if type(retain_claim) is not bool:
        raise TypeError("retain_claim must be bool")
    domain = require_ascii_bytes(
        digest_domain.encode("ascii", errors="strict"),
        field="digest_domain",
        minimum=1,
        maximum=64,
    )
    exact_payload = require_bounded_bytes(
        payload,
        field="in-memory canonical payload",
        maximum=_IN_MEMORY_VALUE_MAXIMUM_BYTES,
    )
    value = canonical_value_digest(digest_domain, exact_payload)
    connector = work.connector
    if connector.fetch_one(
        "SELECT digest_domain FROM catalog_canonical_digest_policies "
        "WHERE digest_domain = %s",
        (domain,),
    ) != (domain,):
        raise CanonicalValueNotReadyError(
            f"canonical digest policy {digest_domain!r} is not registered"
        )
    allocation = ensure_allocation_family(
        connector,
        value_sha256=value,
        digest_domain=domain,
        byte_count=len(exact_payload),
        allocated_at=timestamp,
    )
    claim = connector.fetch_one(
        f"SELECT generation, value_sha256 FROM {_UPLOAD} "
        "WHERE generation = %s AND value_sha256 = %s",
        (exact_generation, value),
    )
    if claim:
        if claim != (exact_generation, value):
            raise CanonicalValueCollisionError(
                "canonical upload claim conflicts with its exact tuple"
            )
    else:
        connector.execute(
            f"INSERT INTO {_UPLOAD} (generation, value_sha256) VALUES (%s, %s)",
            (exact_generation, value),
        )
    tree = build_canonical_value_tree(value, allocation.byte_count, (exact_payload,))
    for encoded in tree.pages:
        page = CanonicalValuePageFamily.from_payload(
            page_sha256=encoded.page_sha256,
            page_bytes=encoded.page_bytes,
        )
        receipt = ensure_page_family(connector, page=page)
        ensure_exact_page_parent_edges(connector, receipt=receipt)
    if connector.fetch_one(
        f"SELECT parent_sha256 FROM {_PAGE_PARENT} WHERE child_sha256 = %s",
        (tree.root_page_sha256,),
    ):
        raise CanonicalValueCollisionError("canonical root page already has a parent")
    ensure_canonical_value_identity(
        connector,
        value_sha256=value,
        root_page_sha256=tree.root_page_sha256,
    )
    if not retain_claim:
        deleted = connector.execute_affected(
            f"DELETE FROM {_UPLOAD} WHERE generation = %s AND value_sha256 = %s",
            (exact_generation, value),
        )
        if deleted != 1:
            raise CanonicalValueCollisionError(
                "canonical upload claim changed before durable handoff"
            )
    return CanonicalValueReadReceipt(
        value,
        allocation.digest_domain,
        allocation.byte_count,
        tree.root_page_sha256,
    )


def _expected_root_level(byte_count: int) -> int:
    pages = max(
        1,
        (byte_count + CANONICAL_VALUE_CHUNK_BYTES - 1) // CANONICAL_VALUE_CHUNK_BYTES,
    )
    level = 0
    while pages > 1:
        pages = (pages + 255) // 256
        level += 1
    return level
