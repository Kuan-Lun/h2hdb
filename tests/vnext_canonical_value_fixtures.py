from __future__ import annotations

from typing import Any


def seed_canonical_allocation(
    connector: Any,
    *,
    value_sha256: bytes,
    digest_domain: bytes,
    byte_count: int,
    allocated_at: int,
) -> None:
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocation_anchors "
        "(value_sha256) VALUES (%s)",
        (value_sha256,),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocation_digest_domains "
        "(value_sha256, digest_domain) VALUES (%s, %s)",
        (value_sha256, digest_domain),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocation_byte_counts "
        "(value_sha256, byte_count) VALUES (%s, %s)",
        (value_sha256, byte_count),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocation_allocated_ats "
        "(value_sha256, allocated_at) VALUES (%s, %s)",
        (value_sha256, allocated_at),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocation_seals "
        "(value_sha256) VALUES (%s)",
        (value_sha256,),
    )


def seed_canonical_page(
    connector: Any,
    *,
    page_sha256: bytes,
    value_sha256: bytes,
    page_bytes: bytes,
    level: int,
    page_position: int,
    subtree_item_count: int,
) -> None:
    connector.execute(
        "INSERT INTO catalog_canonical_value_page_anchors (page_sha256) VALUES (%s)",
        (page_sha256,),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_page_payloads "
        "(page_sha256, page_bytes) VALUES (%s, %s)",
        (page_sha256, page_bytes),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_page_coordinates "
        "(value_sha256, level, page_position, page_sha256) "
        "VALUES (%s, %s, %s, %s)",
        (value_sha256, level, page_position, page_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_page_subtree_item_counts "
        "(page_sha256, subtree_item_count) VALUES (%s, %s)",
        (page_sha256, subtree_item_count),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_page_seals (page_sha256) VALUES (%s)",
        (page_sha256,),
    )


def seed_canonical_value(
    connector: Any,
    *,
    value_sha256: bytes,
    digest_domain: bytes,
    page_sha256: bytes,
    page_bytes: bytes,
    subtree_item_count: int,
    allocated_at: int,
    level: int = 0,
    page_position: int = 0,
) -> None:
    seed_canonical_allocation(
        connector,
        value_sha256=value_sha256,
        digest_domain=digest_domain,
        byte_count=subtree_item_count,
        allocated_at=allocated_at,
    )
    seed_canonical_page(
        connector,
        page_sha256=page_sha256,
        value_sha256=value_sha256,
        page_bytes=page_bytes,
        level=level,
        page_position=page_position,
        subtree_item_count=subtree_item_count,
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_identities "
        "(value_sha256, root_page_sha256) VALUES (%s, %s)",
        (value_sha256, page_sha256),
    )
