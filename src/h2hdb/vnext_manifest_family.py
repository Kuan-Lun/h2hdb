"""Exact storage protocols for source and manifest relations.

Immutable descriptors and manifests are atomic BCNF rows.  Source-build state
and its optional terminal timestamp remain separate lifecycle authorities, so
readers validate the complete lifecycle shape and every replay exact-compares
the durable tuple.
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

_SOURCE_BUILD_DESCRIPTOR = "catalog_source_build_descriptor"
_SOURCE_BUILD_STATE = "catalog_source_build_states"
_SOURCE_BUILD_SEALED = "catalog_source_build_sealed_ats"

_BUILD_MANIFEST_CORE = "catalog_build_manifest_core"
_SOURCE_BUILD_DISCOVERY = "catalog_source_build_discoveries"

_GALLERY_MANIFEST = "catalog_gallery_manifests"
_MANIFEST_POLICY = "catalog_manifest_policies"

_SNAPSHOT_MANIFEST = "catalog_source_snapshot_manifest_identity"


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
            "SELECT TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(6))"
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
        f"SELECT build_id FROM {_SOURCE_BUILD_DESCRIPTOR} WHERE build_id = %s UNION "
        f"SELECT build_id FROM {_SOURCE_BUILD_STATE} WHERE build_id = %s UNION "
        f"SELECT build_id FROM {_SOURCE_BUILD_SEALED} WHERE build_id = %s) "
        "SELECT k.build_id, descriptor.build_id, descriptor.scope_key, "
        "descriptor.manifest_policy_id, descriptor.created_at, state.build_id, "
        "state.state, sealed.build_id, sealed.sealed_at FROM family_keys k "
        f"LEFT JOIN {_SOURCE_BUILD_DESCRIPTOR} descriptor "
        "ON descriptor.build_id = k.build_id "
        f"LEFT JOIN {_SOURCE_BUILD_STATE} state ON state.build_id = k.build_id "
        f"LEFT JOIN {_SOURCE_BUILD_SEALED} sealed ON sealed.build_id = k.build_id",
        (build,) * 3,
    )
    if not row:
        return None
    if len(row) != 9 or any(row[index] != build for index in (0, 1, 5)):
        raise ManifestFamilyPartialError(
            "source build has an existing incomplete lifecycle family"
        )
    if row[7] not in {None, build}:
        raise ManifestFamilyPartialError("source build sealed_at key differs")
    try:
        return SourceBuildFamily(build, row[2], row[3], row[6], row[4], row[8])
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
        f"INSERT INTO {_SOURCE_BUILD_DESCRIPTOR} "
        "(build_id, scope_key, manifest_policy_id, created_at) "
        "VALUES (%s, %s, %s, %s)",
        (
            proposed.build_id,
            proposed.scope_key,
            proposed.manifest_policy_id,
            proposed.created_at,
        ),
    )
    connector.execute(
        f"INSERT INTO {_SOURCE_BUILD_STATE} (build_id, state) VALUES (%s, %s)",
        (proposed.build_id, "OPEN"),
    )
    return proposed


def load_build_manifest_family(
    connector: Any,
    *,
    build_id: bytes,
) -> BuildManifestFamily | None:
    build = require_uuid16(build_id, field="build_id")
    row = connector.fetch_one(
        "SELECT manifest.build_id, manifest.manifest_sha256, "
        "manifest.file_count, manifest.byte_count, discovery.build_id, "
        "discovery.gallery_count, completed.build_id, completed.sealed_at FROM "
        f"{_BUILD_MANIFEST_CORE} manifest "
        f"LEFT JOIN {_SOURCE_BUILD_DISCOVERY} discovery "
        "ON discovery.build_id = manifest.build_id "
        f"LEFT JOIN {_SOURCE_BUILD_SEALED} completed "
        "ON completed.build_id = manifest.build_id WHERE manifest.build_id = %s",
        (build,),
    )
    if not row:
        return None
    if len(row) != 8 or any(row[index] != build for index in (0, 4, 6)):
        raise ManifestFamilyPartialError(
            "build manifest has an existing incomplete sealed family"
        )
    try:
        return BuildManifestFamily(build, row[1], row[5], row[2], row[3], row[7])
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
        f"SELECT gallery_count FROM {_SOURCE_BUILD_DISCOVERY} WHERE build_id = %s",
        (proposed.build_id,),
    ) != (proposed.gallery_count,):
        raise ManifestFamilyCollisionError(
            "build manifest gallery count differs from sealed discovery"
        )
    if connector.fetch_one(
        f"SELECT sealed_at FROM {_SOURCE_BUILD_SEALED} WHERE build_id = %s",
        (proposed.build_id,),
    ):
        raise ManifestFamilyPartialError(
            "source build sealed_at exists without its manifest core"
        )
    connector.execute(
        f"INSERT INTO {_BUILD_MANIFEST_CORE} "
        "(build_id, manifest_sha256, file_count, byte_count) "
        "VALUES (%s, %s, %s, %s)",
        (
            proposed.build_id,
            proposed.manifest_sha256,
            proposed.file_count,
            proposed.byte_count,
        ),
    )
    connector.execute(
        f"INSERT INTO {_SOURCE_BUILD_SEALED} (build_id, sealed_at) VALUES (%s, %s)",
        (proposed.build_id, proposed.computed_at),
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
        "SELECT manifest_algorithm_version, file_order_version "
        f"FROM {_MANIFEST_POLICY} WHERE manifest_policy_id = %s",
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
        "SELECT snapshot_manifest_sha256, gallery_count, file_count, byte_count "
        f"FROM {_SNAPSHOT_MANIFEST} WHERE snapshot_manifest_sha256 = %s",
        (value,),
    )
    if not row:
        return None
    try:
        if len(row) != 4 or row[0] != value:
            raise ValueError("snapshot manifest row has an invalid shape")
        return SnapshotManifestFamily(value, row[1], row[2], row[3])
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
        f"INSERT INTO {_SNAPSHOT_MANIFEST} "
        "(snapshot_manifest_sha256, gallery_count, file_count, byte_count) "
        "VALUES (%s, %s, %s, %s)",
        (
            proposed.snapshot_manifest_sha256,
            proposed.gallery_count,
            proposed.file_count,
            proposed.byte_count,
        ),
    )
    return proposed
