"""Seal-last physical-family fixtures for B3b source and manifest rows."""

from __future__ import annotations

from typing import Any

from h2hdb.vnext_manifest_family import (
    BuildManifestFamily,
    GalleryManifestFamily,
    SnapshotManifestFamily,
    SourceBuildFamily,
    ensure_build_manifest_family,
    ensure_snapshot_manifest_family,
    load_gallery_manifest_family,
    load_source_build_family,
)


def seed_source_build(
    connector: Any,
    *,
    build_id: bytes,
    scope_key: bytes,
    manifest_policy_id: int = 1,
    state: str = "OPEN",
    created_at: int = 12,
    sealed_at: int | None = None,
) -> SourceBuildFamily:
    expected = SourceBuildFamily(
        build_id,
        scope_key,
        manifest_policy_id,
        state,
        created_at,
        sealed_at,
    )
    existing = load_source_build_family(connector, build_id=build_id)
    if existing is not None:
        if existing != expected:
            raise AssertionError("source-build fixture replay differs")
        return existing
    connector.execute(
        "INSERT INTO catalog_source_build_anchors (build_id) VALUES (%s)",
        (build_id,),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_scope_keys (build_id, scope_key) "
        "VALUES (%s, %s)",
        (build_id, scope_key),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_manifest_policy_ids "
        "(build_id, manifest_policy_id) VALUES (%s, %s)",
        (build_id, manifest_policy_id),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_states (build_id, state) VALUES (%s, %s)",
        (build_id, state),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_created_ats (build_id, created_at) "
        "VALUES (%s, %s)",
        (build_id, created_at),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_descriptor_seals (build_id) VALUES (%s)",
        (build_id,),
    )
    if sealed_at is not None:
        connector.execute(
            "INSERT INTO catalog_source_build_sealed_ats (build_id, sealed_at) "
            "VALUES (%s, %s)",
            (build_id, sealed_at),
        )
    return expected


def seed_build_manifest(
    connector: Any,
    *,
    build_id: bytes,
    manifest_sha256: bytes,
    gallery_count: int,
    file_count: int,
    byte_count: int,
    computed_at: int,
    discovery_tree_observation_sha256: bytes = b"t" * 32,
) -> BuildManifestFamily:
    discovery_count = connector.fetch_one(
        "SELECT gallery_count FROM catalog_source_build_discovery_gallery_counts "
        "WHERE build_id = %s",
        (build_id,),
    )
    if discovery_count:
        if discovery_count != (gallery_count,):
            raise AssertionError("build-manifest fixture discovery count differs")
    else:
        connector.execute(
            "INSERT INTO catalog_source_build_discovery_anchors (build_id) VALUES (%s)",
            (build_id,),
        )
        connector.execute(
            "INSERT INTO catalog_source_build_discovery_scan_attempts "
            "(build_id, scan_attempt) VALUES (%s, %s)",
            (build_id, b"d" * 16),
        )
        connector.execute(
            "INSERT INTO catalog_source_build_discovery_gallery_counts "
            "(build_id, gallery_count) VALUES (%s, %s)",
            (build_id, gallery_count),
        )
        connector.execute(
            "INSERT INTO catalog_source_build_discovery_tree_observation_sha256s "
            "(build_id, tree_observation_sha256) VALUES (%s, %s)",
            (build_id, discovery_tree_observation_sha256),
        )
        connector.execute(
            "INSERT INTO catalog_source_build_discovery_completed_ats "
            "(build_id, completed_at) VALUES (%s, %s)",
            (build_id, computed_at),
        )
        connector.execute(
            "INSERT INTO catalog_source_build_discovery_seals (build_id) VALUES (%s)",
            (build_id,),
        )
    return ensure_build_manifest_family(
        connector,
        build_id=build_id,
        manifest_sha256=manifest_sha256,
        gallery_count=gallery_count,
        file_count=file_count,
        byte_count=byte_count,
        computed_at=computed_at,
    )


def seed_gallery_manifest(
    connector: Any,
    *,
    gallery_id: int,
    observation_id: int,
    manifest_policy_id: int,
    manifest_sha256: bytes,
    computed_at: int,
) -> GalleryManifestFamily:
    expected = GalleryManifestFamily(
        gallery_id,
        observation_id,
        manifest_policy_id,
        manifest_sha256,
        computed_at,
    )
    existing = load_gallery_manifest_family(
        connector,
        gallery_id=gallery_id,
        observation_id=observation_id,
        manifest_policy_id=manifest_policy_id,
    )
    if existing is not None:
        if existing != expected:
            raise AssertionError("gallery-manifest fixture replay differs")
        return existing
    key = (gallery_id, observation_id, manifest_policy_id)
    connector.execute(
        "INSERT INTO catalog_gallery_manifests "
        "(gallery_id, observation_id, manifest_policy_id, manifest_sha256, "
        "computed_at) VALUES (%s, %s, %s, %s, %s)",
        (*key, manifest_sha256, computed_at),
    )
    return expected


def seed_snapshot_manifest(
    connector: Any,
    *,
    snapshot_manifest_sha256: bytes,
    gallery_count: int,
    file_count: int,
    byte_count: int,
) -> SnapshotManifestFamily:
    return ensure_snapshot_manifest_family(
        connector,
        snapshot_manifest_sha256=snapshot_manifest_sha256,
        gallery_count=gallery_count,
        file_count=file_count,
        byte_count=byte_count,
    )
