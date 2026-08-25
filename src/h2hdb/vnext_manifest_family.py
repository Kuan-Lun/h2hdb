"""Exact storage protocols for source and manifest families.

Source, build, and snapshot descriptors remain sealed narrow families.  A
gallery manifest is one atomic BCNF row, so readers and writers use that base
directly and exact-compare it on replay.
"""

from __future__ import annotations

__all__ = [
    "BuildManifestFamily",
    "GalleryManifestFamily",
    "ManifestFamilyCollisionError",
    "ManifestFamilyPartialError",
    "SnapshotManifestFamily",
    "SourceBuildFamily",
    "database_unix_microseconds",
    "ensure_build_manifest_family",
    "ensure_gallery_manifest_family",
    "ensure_snapshot_manifest_family",
    "ensure_source_build_family",
    "load_build_manifest_family",
    "load_gallery_manifest_family",
    "load_snapshot_manifest_family",
    "load_source_build_family",
]

from dataclasses import dataclass
from typing import Any

from .vnext_domains import (
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from .vnext_identity import artifact_source_manifest_digest
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_SOURCE_BUILD_ANCHOR = "catalog_source_build_anchors"
_SOURCE_BUILD_SCOPE = "catalog_source_build_scope_keys"
_SOURCE_BUILD_POLICY = "catalog_source_build_manifest_policy_ids"
_SOURCE_BUILD_STATE = "catalog_source_build_states"
_SOURCE_BUILD_CREATED = "catalog_source_build_created_ats"
_SOURCE_BUILD_DESCRIPTOR_SEAL = "catalog_source_build_descriptor_seals"
_SOURCE_BUILD_SEALED = "catalog_source_build_sealed_ats"

_BUILD_MANIFEST_ANCHOR = "catalog_build_manifest_anchors"
_BUILD_MANIFEST_DIGEST = "catalog_build_manifest_manifest_sha256s"
_BUILD_MANIFEST_FILE_COUNT = "catalog_build_manifest_file_counts"
_BUILD_MANIFEST_BYTE_COUNT = "catalog_build_manifest_byte_counts"
_BUILD_MANIFEST_SEAL = "catalog_build_manifest_seals"
_DISCOVERY_GALLERY_COUNT = "catalog_source_build_discovery_gallery_counts"

_GALLERY_MANIFEST = "catalog_gallery_manifests"

_SNAPSHOT_ANCHOR = "catalog_source_snapshot_manifest_identity_anchors"
_SNAPSHOT_GALLERY_COUNT = "catalog_source_snapshot_manifest_identity_gallery_counts"
_SNAPSHOT_FILE_COUNT = "catalog_source_snapshot_manifest_identity_file_counts"
_SNAPSHOT_BYTE_COUNT = "catalog_source_snapshot_manifest_identity_byte_counts"
_SNAPSHOT_SEAL = "catalog_source_snapshot_manifest_identity_seals"


class ManifestFamilyCollisionError(RuntimeError):
    """A complete immutable family disagrees with its exact authority."""


class ManifestFamilyPartialError(ManifestFamilyCollisionError):
    """At least one physical member exists without one complete family."""


@dataclass(frozen=True, slots=True)
class SourceBuildFamily:
    build_id: bytes
    scope_key: bytes
    manifest_policy_id: int
    state: str
    created_at: int
    sealed_at: int | None

    def __post_init__(self) -> None:
        require_uuid16(self.build_id, field="build_id")
        require_digest32(self.scope_key, field="scope_key")
        require_positive_int63(self.manifest_policy_id, field="manifest_policy_id")
        if self.state not in {"OPEN", "SEALED", "ABANDONED"}:
            raise ValueError("source build has an invalid state")
        created = require_int63(self.created_at, field="source build created_at")
        if self.state == "SEALED":
            sealed = require_int63(self.sealed_at, field="source build sealed_at")
            if sealed < created:
                raise ValueError("source build sealed_at precedes created_at")
        elif self.sealed_at is not None:
            raise ValueError("OPEN or ABANDONED source build has sealed_at")


@dataclass(frozen=True, slots=True)
class BuildManifestFamily:
    build_id: bytes
    manifest_sha256: bytes
    gallery_count: int
    file_count: int
    byte_count: int
    computed_at: int

    def __post_init__(self) -> None:
        require_uuid16(self.build_id, field="build_id")
        require_digest32(self.manifest_sha256, field="manifest_sha256")
        require_int63(self.gallery_count, field="gallery_count")
        require_int63(self.file_count, field="file_count")
        require_int63(self.byte_count, field="byte_count")
        require_int63(self.computed_at, field="computed_at")


@dataclass(frozen=True, slots=True)
class GalleryManifestFamily:
    gallery_id: int
    observation_id: int
    manifest_policy_id: int
    manifest_sha256: bytes
    computed_at: int

    def __post_init__(self) -> None:
        require_positive_int63(self.gallery_id, field="gallery_id")
        require_positive_int63(self.observation_id, field="observation_id")
        require_positive_int63(self.manifest_policy_id, field="manifest_policy_id")
        require_digest32(self.manifest_sha256, field="manifest_sha256")
        require_int63(self.computed_at, field="computed_at")


@dataclass(frozen=True, slots=True)
class SnapshotManifestFamily:
    snapshot_manifest_sha256: bytes
    gallery_count: int
    file_count: int
    byte_count: int

    def __post_init__(self) -> None:
        require_digest32(
            self.snapshot_manifest_sha256,
            field="snapshot_manifest_sha256",
        )
        require_int63(self.gallery_count, field="snapshot gallery_count")
        require_int63(self.file_count, field="snapshot file_count")
        require_int63(self.byte_count, field="snapshot byte_count")


def database_unix_microseconds(work: VNextUnitOfWork) -> int:
    """Read one database-owned UTC Unix timestamp inside the current tx."""

    if not isinstance(work, VNextUnitOfWork):
        raise TypeError("work must be a VNextUnitOfWork")
    if work.backend == "sqlite":
        # Do not use strftime('%s', ...): SQLiteConnector translates the
        # repository's ``%s`` parameter markers to qmark markers and would
        # therefore rewrite the format literal itself.  ``unixepoch`` keeps
        # the SQL placeholder-free while the fractional expression supplies
        # the millisecond precision supported by the SQLite clock.
        row = work.connector.fetch_one(
            "SELECT CAST(unixepoch('now') AS INTEGER) * 1000000 + "
            "CAST(substr(strftime('%f', 'now'), 4, 3) AS INTEGER) * 1000"
        )
    else:
        row = work.connector.fetch_one(
            "SELECT TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', "
            "UTC_TIMESTAMP(6))"
        )
    if len(row) != 1:
        raise ManifestFamilyCollisionError("database clock returned no exact scalar")
    return require_int63(row[0], field="database unix microseconds")


def load_source_build_family(
    connector: Any,
    *,
    build_id: bytes,
) -> SourceBuildFamily | None:
    build = require_uuid16(build_id, field="build_id")
    row = connector.fetch_one(
        "WITH family_keys(build_id) AS ("
        f"SELECT build_id FROM {_SOURCE_BUILD_ANCHOR} WHERE build_id = %s UNION "
        f"SELECT build_id FROM {_SOURCE_BUILD_SCOPE} WHERE build_id = %s UNION "
        f"SELECT build_id FROM {_SOURCE_BUILD_POLICY} WHERE build_id = %s UNION "
        f"SELECT build_id FROM {_SOURCE_BUILD_STATE} WHERE build_id = %s UNION "
        f"SELECT build_id FROM {_SOURCE_BUILD_CREATED} WHERE build_id = %s UNION "
        f"SELECT build_id FROM {_SOURCE_BUILD_DESCRIPTOR_SEAL} WHERE build_id = %s UNION "
        f"SELECT build_id FROM {_SOURCE_BUILD_SEALED} WHERE build_id = %s) "
        "SELECT k.build_id, a.build_id, scope.build_id, scope.scope_key, "
        "policy.build_id, policy.manifest_policy_id, state.build_id, state.state, "
        "created.build_id, created.created_at, descriptor.build_id, "
        "sealed.build_id, sealed.sealed_at FROM family_keys k "
        f"LEFT JOIN {_SOURCE_BUILD_ANCHOR} a ON a.build_id = k.build_id "
        f"LEFT JOIN {_SOURCE_BUILD_SCOPE} scope ON scope.build_id = k.build_id "
        f"LEFT JOIN {_SOURCE_BUILD_POLICY} policy ON policy.build_id = k.build_id "
        f"LEFT JOIN {_SOURCE_BUILD_STATE} state ON state.build_id = k.build_id "
        f"LEFT JOIN {_SOURCE_BUILD_CREATED} created ON created.build_id = k.build_id "
        f"LEFT JOIN {_SOURCE_BUILD_DESCRIPTOR_SEAL} descriptor "
        "ON descriptor.build_id = k.build_id "
        f"LEFT JOIN {_SOURCE_BUILD_SEALED} sealed ON sealed.build_id = k.build_id",
        (build,) * 7,
    )
    if not row:
        return None
    if len(row) != 13 or any(row[index] != build for index in (0, 1, 2, 4, 6, 8, 10)):
        raise ManifestFamilyPartialError(
            "source build has an existing incomplete descriptor family"
        )
    if row[11] not in {None, build}:
        raise ManifestFamilyPartialError("source build sealed_at key differs")
    try:
        return SourceBuildFamily(build, row[3], row[5], row[7], row[9], row[12])
    except (TypeError, ValueError) as error:
        raise ManifestFamilyCollisionError(
            "source build contains invalid descriptor facts"
        ) from error


def ensure_source_build_family(
    connector: Any,
    *,
    build_id: bytes,
    scope_key: bytes,
    manifest_policy_id: int,
    created_at: int,
) -> SourceBuildFamily:
    proposed = SourceBuildFamily(
        build_id,
        scope_key,
        manifest_policy_id,
        "OPEN",
        created_at,
        None,
    )
    existing = load_source_build_family(connector, build_id=proposed.build_id)
    if existing is not None:
        if existing != proposed:
            raise ManifestFamilyCollisionError(
                "source build attempt capability conflicts with durable facts"
            )
        return existing
    connector.execute(
        f"INSERT INTO {_SOURCE_BUILD_ANCHOR} (build_id) VALUES (%s)",
        (proposed.build_id,),
    )
    connector.execute(
        f"INSERT INTO {_SOURCE_BUILD_SCOPE} (build_id, scope_key) VALUES (%s, %s)",
        (proposed.build_id, proposed.scope_key),
    )
    connector.execute(
        f"INSERT INTO {_SOURCE_BUILD_POLICY} "
        "(build_id, manifest_policy_id) VALUES (%s, %s)",
        (proposed.build_id, proposed.manifest_policy_id),
    )
    connector.execute(
        f"INSERT INTO {_SOURCE_BUILD_STATE} (build_id, state) VALUES (%s, %s)",
        (proposed.build_id, "OPEN"),
    )
    connector.execute(
        f"INSERT INTO {_SOURCE_BUILD_CREATED} (build_id, created_at) VALUES (%s, %s)",
        (proposed.build_id, proposed.created_at),
    )
    connector.execute(
        f"INSERT INTO {_SOURCE_BUILD_DESCRIPTOR_SEAL} (build_id) VALUES (%s)",
        (proposed.build_id,),
    )
    return proposed


def load_build_manifest_family(
    connector: Any,
    *,
    build_id: bytes,
) -> BuildManifestFamily | None:
    build = require_uuid16(build_id, field="build_id")
    row = connector.fetch_one(
        "WITH family_keys(build_id) AS ("
        f"SELECT build_id FROM {_BUILD_MANIFEST_ANCHOR} WHERE build_id = %s UNION "
        f"SELECT build_id FROM {_BUILD_MANIFEST_DIGEST} WHERE build_id = %s UNION "
        f"SELECT build_id FROM {_BUILD_MANIFEST_FILE_COUNT} WHERE build_id = %s UNION "
        f"SELECT build_id FROM {_BUILD_MANIFEST_BYTE_COUNT} WHERE build_id = %s UNION "
        f"SELECT build_id FROM {_BUILD_MANIFEST_SEAL} WHERE build_id = %s) "
        "SELECT k.build_id, a.build_id, digest.build_id, digest.manifest_sha256, "
        "files.build_id, files.file_count, bytes.build_id, bytes.byte_count, "
        "seal.build_id, galleries.build_id, galleries.gallery_count, "
        "completed.build_id, completed.sealed_at FROM family_keys k "
        f"LEFT JOIN {_BUILD_MANIFEST_ANCHOR} a ON a.build_id = k.build_id "
        f"LEFT JOIN {_BUILD_MANIFEST_DIGEST} digest ON digest.build_id = k.build_id "
        f"LEFT JOIN {_BUILD_MANIFEST_FILE_COUNT} files ON files.build_id = k.build_id "
        f"LEFT JOIN {_BUILD_MANIFEST_BYTE_COUNT} bytes ON bytes.build_id = k.build_id "
        f"LEFT JOIN {_BUILD_MANIFEST_SEAL} seal ON seal.build_id = k.build_id "
        f"LEFT JOIN {_DISCOVERY_GALLERY_COUNT} galleries "
        "ON galleries.build_id = k.build_id "
        f"LEFT JOIN {_SOURCE_BUILD_SEALED} completed ON completed.build_id = k.build_id",
        (build,) * 5,
    )
    if not row:
        return None
    if len(row) != 13 or any(
        row[index] != build for index in (0, 1, 2, 4, 6, 8, 9, 11)
    ):
        raise ManifestFamilyPartialError(
            "build manifest has an existing incomplete sealed family"
        )
    try:
        return BuildManifestFamily(build, row[3], row[10], row[5], row[7], row[12])
    except (TypeError, ValueError) as error:
        raise ManifestFamilyCollisionError(
            "build manifest contains invalid immutable facts"
        ) from error


def ensure_build_manifest_family(
    connector: Any,
    *,
    build_id: bytes,
    manifest_sha256: bytes,
    gallery_count: int,
    file_count: int,
    byte_count: int,
    computed_at: int,
) -> BuildManifestFamily:
    proposed = BuildManifestFamily(
        build_id,
        manifest_sha256,
        gallery_count,
        file_count,
        byte_count,
        computed_at,
    )
    existing = load_build_manifest_family(connector, build_id=proposed.build_id)
    if existing is not None:
        if existing != proposed:
            raise ManifestFamilyCollisionError("build manifest replay differs")
        return existing
    if connector.fetch_one(
        f"SELECT gallery_count FROM {_DISCOVERY_GALLERY_COUNT} WHERE build_id = %s",
        (proposed.build_id,),
    ) != (proposed.gallery_count,):
        raise ManifestFamilyCollisionError(
            "build manifest gallery count differs from sealed discovery"
        )
    if connector.fetch_one(
        f"SELECT sealed_at FROM {_SOURCE_BUILD_SEALED} WHERE build_id = %s",
        (proposed.build_id,),
    ) != (proposed.computed_at,):
        raise ManifestFamilyCollisionError(
            "build manifest computed_at differs from source build sealed_at"
        )
    connector.execute(
        f"INSERT INTO {_BUILD_MANIFEST_ANCHOR} (build_id) VALUES (%s)",
        (proposed.build_id,),
    )
    connector.execute(
        f"INSERT INTO {_BUILD_MANIFEST_DIGEST} "
        "(build_id, manifest_sha256) VALUES (%s, %s)",
        (proposed.build_id, proposed.manifest_sha256),
    )
    connector.execute(
        f"INSERT INTO {_BUILD_MANIFEST_FILE_COUNT} "
        "(build_id, file_count) VALUES (%s, %s)",
        (proposed.build_id, proposed.file_count),
    )
    connector.execute(
        f"INSERT INTO {_BUILD_MANIFEST_BYTE_COUNT} "
        "(build_id, byte_count) VALUES (%s, %s)",
        (proposed.build_id, proposed.byte_count),
    )
    connector.execute(
        f"INSERT INTO {_BUILD_MANIFEST_SEAL} (build_id) VALUES (%s)",
        (proposed.build_id,),
    )
    return proposed


def load_gallery_manifest_family(
    connector: Any,
    *,
    gallery_id: int,
    observation_id: int,
    manifest_policy_id: int,
) -> GalleryManifestFamily | None:
    key = (
        require_positive_int63(gallery_id, field="gallery_id"),
        require_positive_int63(observation_id, field="observation_id"),
        require_positive_int63(manifest_policy_id, field="manifest_policy_id"),
    )
    row = connector.fetch_one(
        "SELECT manifest_sha256, computed_at FROM catalog_gallery_manifests "
        "WHERE gallery_id = %s AND observation_id = %s "
        "AND manifest_policy_id = %s",
        key,
    )
    if not row:
        return None
    try:
        if len(row) != 2:
            raise ValueError("gallery manifest row has an invalid shape")
        return GalleryManifestFamily(*key, row[0], row[1])
    except (TypeError, ValueError) as error:
        raise ManifestFamilyCollisionError(
            "gallery manifest contains invalid immutable facts"
        ) from error


def ensure_gallery_manifest_family(
    work: VNextUnitOfWork,
    *,
    gallery_id: int,
    observation_id: int,
    manifest_policy_id: int,
) -> GalleryManifestFamily:
    gallery = require_positive_int63(gallery_id, field="gallery_id")
    observation = require_positive_int63(observation_id, field="observation_id")
    policy_id = require_positive_int63(
        manifest_policy_id,
        field="manifest_policy_id",
    )
    observation_row = work.lock_row(
        LockRank.CHILD,
        encode_lock_key("gallery-manifest", 0, gallery, observation),
        "SELECT observation_identity_sha256 FROM catalog_gallery_observations "
        "WHERE gallery_id = %s AND observation_id = %s",
        (gallery, observation),
    )
    if len(observation_row) != 1:
        raise ManifestFamilyCollisionError("gallery manifest observation is absent")
    observation_identity = require_digest32(
        observation_row[0],
        field="observation_identity_sha256",
    )
    policy = work.connector.fetch_one(
        "SELECT algorithm.manifest_algorithm_version, orders.file_order_version "
        "FROM catalog_manifest_policy_seals seal "
        "JOIN catalog_manifest_policy_manifest_algorithm_versions algorithm "
        "ON algorithm.manifest_policy_id = seal.manifest_policy_id "
        "JOIN catalog_manifest_policy_file_order_versions orders "
        "ON orders.manifest_policy_id = seal.manifest_policy_id "
        "WHERE seal.manifest_policy_id = %s",
        (policy_id,),
    )
    if len(policy) != 2:
        raise ManifestFamilyCollisionError("gallery manifest policy is unsealed")
    algorithm_version = require_positive_int63(
        policy[0],
        field="manifest_algorithm_version",
    )
    file_order_version = require_positive_int63(
        policy[1],
        field="file_order_version",
    )
    digest = artifact_source_manifest_digest(
        observation_identity,
        algorithm_version,
        file_order_version,
    )
    existing = load_gallery_manifest_family(
        work.connector,
        gallery_id=gallery,
        observation_id=observation,
        manifest_policy_id=policy_id,
    )
    if existing is not None:
        if existing.manifest_sha256 != digest:
            raise ManifestFamilyCollisionError(
                "gallery manifest replay differs from its exact codec"
            )
        return existing
    computed_at = database_unix_microseconds(work)
    proposed = GalleryManifestFamily(
        gallery,
        observation,
        policy_id,
        digest,
        computed_at,
    )
    key = (gallery, observation, policy_id)
    work.connector.execute(
        f"INSERT INTO {_GALLERY_MANIFEST} "
        "(gallery_id, observation_id, manifest_policy_id, manifest_sha256, "
        "computed_at) VALUES (%s, %s, %s, %s, %s)",
        (*key, digest, computed_at),
    )
    return proposed


def load_snapshot_manifest_family(
    connector: Any,
    *,
    snapshot_manifest_sha256: bytes,
) -> SnapshotManifestFamily | None:
    value = require_digest32(
        snapshot_manifest_sha256,
        field="snapshot_manifest_sha256",
    )
    row = connector.fetch_one(
        "WITH family_keys(snapshot_manifest_sha256) AS ("
        f"SELECT snapshot_manifest_sha256 FROM {_SNAPSHOT_ANCHOR} WHERE snapshot_manifest_sha256 = %s UNION "
        f"SELECT snapshot_manifest_sha256 FROM {_SNAPSHOT_GALLERY_COUNT} WHERE snapshot_manifest_sha256 = %s UNION "
        f"SELECT snapshot_manifest_sha256 FROM {_SNAPSHOT_FILE_COUNT} WHERE snapshot_manifest_sha256 = %s UNION "
        f"SELECT snapshot_manifest_sha256 FROM {_SNAPSHOT_BYTE_COUNT} WHERE snapshot_manifest_sha256 = %s UNION "
        f"SELECT snapshot_manifest_sha256 FROM {_SNAPSHOT_SEAL} WHERE snapshot_manifest_sha256 = %s) "
        "SELECT k.snapshot_manifest_sha256, a.snapshot_manifest_sha256, "
        "g.snapshot_manifest_sha256, g.gallery_count, f.snapshot_manifest_sha256, "
        "f.file_count, b.snapshot_manifest_sha256, b.byte_count, "
        "s.snapshot_manifest_sha256 FROM family_keys k "
        f"LEFT JOIN {_SNAPSHOT_ANCHOR} a USING (snapshot_manifest_sha256) "
        f"LEFT JOIN {_SNAPSHOT_GALLERY_COUNT} g USING (snapshot_manifest_sha256) "
        f"LEFT JOIN {_SNAPSHOT_FILE_COUNT} f USING (snapshot_manifest_sha256) "
        f"LEFT JOIN {_SNAPSHOT_BYTE_COUNT} b USING (snapshot_manifest_sha256) "
        f"LEFT JOIN {_SNAPSHOT_SEAL} s USING (snapshot_manifest_sha256)",
        (value,) * 5,
    )
    if not row:
        return None
    if len(row) != 9 or any(row[index] != value for index in (0, 1, 2, 4, 6, 8)):
        raise ManifestFamilyPartialError(
            "snapshot manifest has an existing incomplete sealed family"
        )
    try:
        return SnapshotManifestFamily(value, row[3], row[5], row[7])
    except (TypeError, ValueError) as error:
        raise ManifestFamilyCollisionError(
            "snapshot manifest contains invalid immutable counts"
        ) from error


def ensure_snapshot_manifest_family(
    connector: Any,
    *,
    snapshot_manifest_sha256: bytes,
    gallery_count: int,
    file_count: int,
    byte_count: int,
) -> SnapshotManifestFamily:
    proposed = SnapshotManifestFamily(
        snapshot_manifest_sha256,
        gallery_count,
        file_count,
        byte_count,
    )
    existing = load_snapshot_manifest_family(
        connector,
        snapshot_manifest_sha256=proposed.snapshot_manifest_sha256,
    )
    if existing is not None:
        if existing != proposed:
            raise ManifestFamilyCollisionError(
                "snapshot manifest replay has conflicting aggregate counts"
            )
        return existing
    connector.execute(
        f"INSERT INTO {_SNAPSHOT_ANCHOR} (snapshot_manifest_sha256) VALUES (%s)",
        (proposed.snapshot_manifest_sha256,),
    )
    for table, column, value in (
        (_SNAPSHOT_GALLERY_COUNT, "gallery_count", proposed.gallery_count),
        (_SNAPSHOT_FILE_COUNT, "file_count", proposed.file_count),
        (_SNAPSHOT_BYTE_COUNT, "byte_count", proposed.byte_count),
    ):
        connector.execute(
            f"INSERT INTO {table} (snapshot_manifest_sha256, {column}) "
            "VALUES (%s, %s)",
            (proposed.snapshot_manifest_sha256, value),
        )
    connector.execute(
        f"INSERT INTO {_SNAPSHOT_SEAL} (snapshot_manifest_sha256) VALUES (%s)",
        (proposed.snapshot_manifest_sha256,),
    )
    return proposed
