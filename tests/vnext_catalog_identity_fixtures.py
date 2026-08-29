from __future__ import annotations

from typing import Any

from h2hdb.vnext_identity import file_role as derive_file_role


def seed_gallery_identity(
    connector: Any,
    *,
    gallery_id: int,
    gallery_key: bytes,
    scope_key: bytes,
    locator_sha256: bytes,
) -> None:
    connector.execute(
        "INSERT INTO catalog_gallery_identities "
        "(gallery_id, gallery_key, scope_key, locator_sha256) "
        "VALUES (%s, %s, %s, %s)",
        (gallery_id, gallery_key, scope_key, locator_sha256),
    )


def seed_file_name_identity(
    connector: Any,
    *,
    file_key: bytes,
    name_bytes: bytes,
    file_role: bytes,
) -> None:
    if file_role != derive_file_role(name_bytes):
        raise ValueError("file_role must match the exact name classifier")
    connector.execute(
        "INSERT INTO catalog_file_name_identities "
        "(file_key, name_bytes) VALUES (%s, %s)",
        (file_key, name_bytes),
    )


def seed_gallery_observation_file(
    connector: Any,
    *,
    gallery_id: int,
    observation_id: int,
    file_no: int,
    file_key: bytes,
    file_sha256: bytes,
) -> None:
    key = (gallery_id, observation_id, file_key)
    connector.execute(
        "INSERT INTO catalog_gallery_observation_file_anchors "
        "(gallery_id, observation_id, file_key) VALUES (%s, %s, %s)",
        key,
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_file_file_nos "
        "(gallery_id, observation_id, file_key, file_no) "
        "VALUES (%s, %s, %s, %s)",
        (*key, file_no),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_file_file_sha256s "
        "(gallery_id, observation_id, file_key, file_sha256) "
        "VALUES (%s, %s, %s, %s)",
        (*key, file_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_file_seals "
        "(gallery_id, observation_id, file_key) VALUES (%s, %s, %s)",
        key,
    )


def seed_tag_term(
    connector: Any,
    *,
    tag_id: int,
    namespace: bytes,
    tag_value_sha256: bytes,
) -> None:
    connector.execute(
        "INSERT INTO catalog_tag_terms "
        "(tag_id, namespace, tag_value_sha256) VALUES (%s, %s, %s)",
        (tag_id, namespace, tag_value_sha256),
    )
