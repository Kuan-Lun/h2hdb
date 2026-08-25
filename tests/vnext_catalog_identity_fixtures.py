from __future__ import annotations

from typing import Any


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
    connector.execute(
        "INSERT INTO catalog_file_name_identity_anchors (file_key) VALUES (%s)",
        (file_key,),
    )
    connector.execute(
        "INSERT INTO catalog_file_name_identity_name_bytes "
        "(file_key, name_bytes) VALUES (%s, %s)",
        (file_key, name_bytes),
    )
    connector.execute(
        "INSERT INTO catalog_file_name_identity_file_roles "
        "(file_key, file_role) VALUES (%s, %s)",
        (file_key, file_role),
    )
    connector.execute(
        "INSERT INTO catalog_file_name_identity_seals (file_key) VALUES (%s)",
        (file_key,),
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
        "INSERT INTO catalog_tag_term_anchors (tag_id) VALUES (%s)",
        (tag_id,),
    )
    connector.execute(
        "INSERT INTO catalog_tag_term_identities "
        "(namespace, tag_value_sha256, tag_id) VALUES (%s, %s, %s)",
        (namespace, tag_value_sha256, tag_id),
    )
    connector.execute(
        "INSERT INTO catalog_tag_term_seals (tag_id) VALUES (%s)",
        (tag_id,),
    )
