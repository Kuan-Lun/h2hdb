from __future__ import annotations

from typing import Any


def seed_gallery_page_descriptor(
    connector: Any,
    *,
    page_sha256: bytes,
    page_bytes: bytes,
    component: bytes,
    level: int,
    subtree_item_count: int,
) -> None:
    """Seed one complete sealed physical gallery-page descriptor family."""

    connector.execute(
        "INSERT INTO catalog_gallery_observation_page_descriptor_anchors "
        "(page_sha256) VALUES (%s)",
        (page_sha256,),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_pages "
        "(page_sha256, page_bytes) VALUES (%s, %s)",
        (page_sha256, page_bytes),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_page_descriptor_components "
        "(page_sha256, component) VALUES (%s, %s)",
        (page_sha256, component),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_page_descriptor_levels "
        "(page_sha256, level) VALUES (%s, %s)",
        (page_sha256, level),
    )
    connector.execute(
        "INSERT INTO "
        "catalog_gallery_observation_page_descriptor_subtree_item_counts "
        "(page_sha256, subtree_item_count) VALUES (%s, %s)",
        (page_sha256, subtree_item_count),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_page_descriptor_seals "
        "(page_sha256) VALUES (%s)",
        (page_sha256,),
    )


def seed_gallery_page_bounds(
    connector: Any,
    *,
    page_sha256: bytes,
    first_key: bytes,
    last_key: bytes,
) -> None:
    """Seed one complete optional sealed physical page-bounds family."""

    connector.execute(
        "INSERT INTO catalog_gallery_observation_page_key_bounds_anchors "
        "(page_sha256) VALUES (%s)",
        (page_sha256,),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_page_key_bounds_first_keys "
        "(page_sha256, first_key) VALUES (%s, %s)",
        (page_sha256, first_key),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_page_key_bounds_last_keys "
        "(page_sha256, last_key) VALUES (%s, %s)",
        (page_sha256, last_key),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_page_key_bounds_seals "
        "(page_sha256) VALUES (%s)",
        (page_sha256,),
    )
