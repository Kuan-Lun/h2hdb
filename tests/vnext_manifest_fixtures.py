"""Atomic BCNF fixtures for source-build and manifest relations."""

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
    if state == "SEALED":
        raise ValueError(
            "SEALED fixtures require seed_sealed_source_build so the manifest "
            "core and sealed timestamp are created atomically"
        )
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
        "INSERT INTO catalog_source_build_descriptor "
        "(build_id, scope_key, manifest_policy_id, created_at) "
        "VALUES (%s, %s, %s, %s)",
        (build_id, scope_key, manifest_policy_id, created_at),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_states (build_id, state) VALUES (%s, %s)",
        (build_id, state),
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
        "SELECT gallery_count FROM catalog_source_build_discoveries "
        "WHERE build_id = %s",
        (build_id,),
    )
    if discovery_count:
        if discovery_count != (gallery_count,):
            raise AssertionError("build-manifest fixture discovery count differs")
    else:
        connector.execute(
            "INSERT INTO catalog_source_build_discoveries "
            "(build_id, scan_attempt, gallery_count, tree_observation_sha256, "
            "completed_at) VALUES (%s, %s, %s, %s, %s)",
            (
                build_id,
                b"d" * 16,
                gallery_count,
                discovery_tree_observation_sha256,
                computed_at,
            ),
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


def seed_sealed_source_build(
    connector: Any,
    *,
    build_id: bytes,
    scope_key: bytes,
    manifest_sha256: bytes,
    gallery_count: int,
    file_count: int,
    byte_count: int,
    manifest_policy_id: int = 1,
    created_at: int = 12,
    sealed_at: int = 13,
    discovery_tree_observation_sha256: bytes = b"t" * 32,
) -> SourceBuildFamily:
    """Seed one complete SEALED lifecycle in the caller-owned transaction."""

    expected = SourceBuildFamily(
        build_id,
        scope_key,
        manifest_policy_id,
        "SEALED",
        created_at,
        sealed_at,
    )
    existing = load_source_build_family(connector, build_id=build_id)
    if existing is not None:
        if existing != expected:
            raise AssertionError("sealed source-build fixture replay differs")
        seed_build_manifest(
            connector,
            build_id=build_id,
            manifest_sha256=manifest_sha256,
            gallery_count=gallery_count,
            file_count=file_count,
            byte_count=byte_count,
            computed_at=sealed_at,
            discovery_tree_observation_sha256=discovery_tree_observation_sha256,
        )
        return existing
    seed_source_build(
        connector,
        build_id=build_id,
        scope_key=scope_key,
        manifest_policy_id=manifest_policy_id,
        state="OPEN",
        created_at=created_at,
    )
    seed_build_manifest(
        connector,
        build_id=build_id,
        manifest_sha256=manifest_sha256,
        gallery_count=gallery_count,
        file_count=file_count,
        byte_count=byte_count,
        computed_at=sealed_at,
        discovery_tree_observation_sha256=discovery_tree_observation_sha256,
    )
    connector.execute(
        "UPDATE catalog_source_build_states SET state = %s "
        "WHERE build_id = %s AND state = %s",
        ("SEALED", build_id, "OPEN"),
    )
    result = load_source_build_family(connector, build_id=build_id)
    if result is None:
        raise AssertionError("sealed source-build fixture was not persisted")
    if result != expected:
        raise AssertionError("sealed source-build fixture differs after persistence")
    return result


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
