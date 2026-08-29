"""Typed, fail-closed access to immutable vNext catalog registries.

Each registry is one authoritative BCNF row.  A row is therefore either
present in full or absent; loaders validate every value and every derived
identity before returning it.  The source-scope writer derives its digest,
checks both candidate-key directions, and inserts the complete row in the
caller-owned transaction.
"""

from __future__ import annotations

__all__ = [
    "AnalysisPolicyRecord",
    "ArtifactPolicySemanticsRecord",
    "ArtifactProducerFingerprintRecord",
    "ArtifactStorageCodecRecord",
    "ArtifactZipWriterPolicyRecord",
    "CatalogRegistryConflictError",
    "CatalogRegistryError",
    "CatalogRegistryNotReadyError",
    "DisplayTitlePolicyRecord",
    "ManifestPolicyRecord",
    "SourceScopeRecord",
    "SourceScopeWrite",
    "TitleSortPolicyRecord",
    "ensure_source_scope",
    "load_analysis_policy",
    "load_artifact_policy_semantics",
    "load_artifact_producer_fingerprint",
    "load_artifact_storage_codec",
    "load_artifact_zip_writer_policy",
    "load_display_title_policy",
    "load_manifest_policy",
    "load_manifest_policy_by_natural",
    "load_source_scope",
    "load_title_sort_policy",
]

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from . import vnext_identity as identity
from .sql_connector import SQLConnector
from .vnext_domains import (
    require_ascii_bytes,
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uint32,
)


class CatalogRegistryError(RuntimeError):
    """Base class for immutable catalog-registry contract failures."""


class CatalogRegistryNotReadyError(CatalogRegistryError):
    """A requested authoritative registry row is absent."""


class CatalogRegistryConflictError(CatalogRegistryError):
    """Durable registry facts disagree with their domain or identity codec."""


def _positive_uint32(value: object, *, field: str) -> int:
    result = require_uint32(value, field=field)
    if result == 0:
        raise ValueError(f"{field} must be positive")
    return result


@dataclass(frozen=True, slots=True)
class ManifestPolicyRecord:
    manifest_policy_id: int
    manifest_algorithm_version: int
    file_order_version: int

    def __post_init__(self) -> None:
        require_positive_int63(self.manifest_policy_id, field="manifest_policy_id")
        _positive_uint32(
            self.manifest_algorithm_version,
            field="manifest_algorithm_version",
        )
        _positive_uint32(self.file_order_version, field="file_order_version")


@dataclass(frozen=True, slots=True)
class AnalysisPolicyRecord:
    policy_id: int
    algorithm_version: int
    spam_artist_threshold: int
    spam_occurrence_threshold: int
    content_owner_rule_version: int
    gid_winner_rule_version: int

    def __post_init__(self) -> None:
        require_positive_int63(self.policy_id, field="analysis policy_id")
        _positive_uint32(self.algorithm_version, field="analysis algorithm_version")
        require_int63(
            self.spam_artist_threshold,
            field="analysis spam_artist_threshold",
        )
        require_int63(
            self.spam_occurrence_threshold,
            field="analysis spam_occurrence_threshold",
        )
        _positive_uint32(
            self.content_owner_rule_version,
            field="analysis content_owner_rule_version",
        )
        _positive_uint32(
            self.gid_winner_rule_version,
            field="analysis gid_winner_rule_version",
        )


@dataclass(frozen=True, slots=True)
class ArtifactZipWriterPolicyRecord:
    artifact_algorithm_version: int
    zip_codec_version: int
    compression_method: int
    compression_level: int
    dos_date: int
    dos_time: int
    unix_mode: int
    general_purpose_flags: int
    create_system: int
    archive_name_codec_version: int
    artifact_name_codec_version: int

    def __post_init__(self) -> None:
        _positive_uint32(
            self.artifact_algorithm_version,
            field="ZIP artifact_algorithm_version",
        )
        for field_name, value in (
            ("zip_codec_version", self.zip_codec_version),
            ("archive_name_codec_version", self.archive_name_codec_version),
            ("artifact_name_codec_version", self.artifact_name_codec_version),
        ):
            _positive_uint32(value, field=f"ZIP {field_name}")
        for field_name, value in (
            ("compression_method", self.compression_method),
            ("compression_level", self.compression_level),
            ("dos_date", self.dos_date),
            ("dos_time", self.dos_time),
            ("unix_mode", self.unix_mode),
            ("general_purpose_flags", self.general_purpose_flags),
            ("create_system", self.create_system),
        ):
            require_uint32(value, field=f"ZIP {field_name}")


@dataclass(frozen=True, slots=True)
class ArtifactStorageCodecRecord:
    storage_codec_version: int
    adapter_id: bytes
    locator_codec_version: int
    protection_token_codec_version: int

    def __post_init__(self) -> None:
        _positive_uint32(
            self.storage_codec_version,
            field="storage_codec_version",
        )
        require_ascii_bytes(
            self.adapter_id,
            field="storage adapter_id",
            minimum=1,
            maximum=64,
        )
        _positive_uint32(
            self.locator_codec_version,
            field="storage locator_codec_version",
        )
        _positive_uint32(
            self.protection_token_codec_version,
            field="storage protection_token_codec_version",
        )


@dataclass(frozen=True, slots=True)
class ArtifactPolicySemanticsRecord:
    """Policy semantics with the algorithm derived from producer registration."""

    policy_component_sha256: bytes
    artifact_algorithm_version: int
    max_image_short_side: int
    producer_fingerprint_sha256: bytes

    def __post_init__(self) -> None:
        policy_digest = require_digest32(
            self.policy_component_sha256,
            field="artifact policy_component_sha256",
        )
        algorithm_version = _positive_uint32(
            self.artifact_algorithm_version,
            field="artifact policy algorithm version",
        )
        short_side = _positive_uint32(
            self.max_image_short_side,
            field="artifact policy max_image_short_side",
        )
        producer = require_digest32(
            self.producer_fingerprint_sha256,
            field="artifact policy producer_fingerprint_sha256",
        )
        if (
            identity.artifact_policy_digest(
                algorithm_version,
                short_side,
                producer,
            )
            != policy_digest
        ):
            raise ValueError("artifact policy digest disagrees with its exact facts")


@dataclass(frozen=True, slots=True)
class TitleSortPolicyRecord:
    title_sort_policy_id: int
    title_sort_algorithm_version: int
    unicode_data_version: bytes

    def __post_init__(self) -> None:
        require_uint32(self.title_sort_policy_id, field="title_sort_policy_id")
        _positive_uint32(
            self.title_sort_algorithm_version,
            field="title_sort_algorithm_version",
        )
        require_bounded_bytes(
            self.unicode_data_version,
            field="unicode_data_version",
            minimum=1,
            maximum=32,
        )


@dataclass(frozen=True, slots=True)
class DisplayTitlePolicyRecord:
    display_title_policy_id: int
    display_title_algorithm_version: int
    title_sort_policy_id: int

    def __post_init__(self) -> None:
        require_positive_int63(
            self.display_title_policy_id,
            field="display_title_policy_id",
        )
        _positive_uint32(
            self.display_title_algorithm_version,
            field="display_title_algorithm_version",
        )
        require_uint32(self.title_sort_policy_id, field="title_sort_policy_id")


@dataclass(frozen=True, slots=True)
class SourceScopeRecord:
    scope_key: bytes
    source_provider: bytes
    source_root_sha256: bytes
    identity_policy_version: int

    def __post_init__(self) -> None:
        scope_key = require_digest32(self.scope_key, field="source scope_key")
        provider = require_ascii_bytes(
            self.source_provider,
            field="source_provider",
            minimum=1,
            maximum=64,
        )
        root = require_digest32(
            self.source_root_sha256,
            field="source_root_sha256",
        )
        policy_version = _positive_uint32(
            self.identity_policy_version,
            field="source identity_policy_version",
        )
        if (
            identity.source_scope_key(
                provider.decode("ascii"),
                root,
                policy_version,
            )
            != scope_key
        ):
            raise ValueError("source scope key disagrees with its exact facts")


@dataclass(frozen=True, slots=True)
class SourceScopeWrite:
    record: SourceScopeRecord
    replayed: bool

    def __post_init__(self) -> None:
        if not isinstance(self.record, SourceScopeRecord):
            raise TypeError("source-scope write requires a SourceScopeRecord")
        if not isinstance(self.replayed, bool):
            raise TypeError("source-scope replayed must be bool")


@dataclass(frozen=True, slots=True)
class ArtifactProducerFingerprintRecord:
    producer_fingerprint_sha256: bytes
    artifact_algorithm_version: int
    producer_equivalence_class: bytes
    writer_id: bytes
    python_abi: bytes
    pillow_build: bytes
    libjpeg_build: bytes
    zlib_build: bytes

    def __post_init__(self) -> None:
        fingerprint = require_digest32(
            self.producer_fingerprint_sha256,
            field="artifact producer_fingerprint_sha256",
        )
        _positive_uint32(
            self.artifact_algorithm_version,
            field="artifact producer algorithm version",
        )
        equivalence = require_bounded_bytes(
            self.producer_equivalence_class,
            field="artifact producer_equivalence_class",
            minimum=1,
            maximum=128,
        )
        fields = tuple(
            require_bounded_bytes(
                value,
                field=f"artifact producer {field_name}",
                minimum=1,
                maximum=128,
            )
            for field_name, value in (
                ("writer_id", self.writer_id),
                ("python_abi", self.python_abi),
                ("pillow_build", self.pillow_build),
                ("libjpeg_build", self.libjpeg_build),
                ("zlib_build", self.zlib_build),
            )
        )
        if identity.artifact_producer_fingerprint_sha256(*fields) != fingerprint:
            raise ValueError(
                "artifact producer fingerprint disagrees with its exact frame"
            )
        if identity.artifact_producer_equivalence_class(fingerprint) != equivalence:
            raise ValueError(
                "artifact producer equivalence class is not repository-derived"
            )


def _validated[T](label: str, factory: Callable[[], T]) -> T:
    try:
        return factory()
    except CatalogRegistryError:
        raise
    except (TypeError, ValueError, identity.VNextIdentityError) as error:
        raise CatalogRegistryConflictError(
            f"{label} contains invalid or identity-incongruent facts"
        ) from error


def _required_row(
    connector: SQLConnector,
    *,
    query: str,
    parameters: tuple[Any, ...],
    label: str,
) -> tuple[Any, ...]:
    row = connector.fetch_one(query, parameters)
    if not row:
        raise CatalogRegistryNotReadyError(f"{label} is absent")
    return row


_MANIFEST_POLICY_BY_ID = (
    "SELECT manifest_policy_id, manifest_algorithm_version, file_order_version "
    "FROM catalog_manifest_policies WHERE manifest_policy_id = %s"
)


def load_manifest_policy(
    connector: SQLConnector,
    manifest_policy_id: int,
) -> ManifestPolicyRecord:
    policy_id = require_positive_int63(
        manifest_policy_id,
        field="manifest_policy_id",
    )
    row = _required_row(
        connector,
        query=_MANIFEST_POLICY_BY_ID,
        parameters=(policy_id,),
        label="manifest policy",
    )
    return _validated("manifest policy", lambda: ManifestPolicyRecord(*row))


def load_manifest_policy_by_natural(
    connector: SQLConnector,
    *,
    manifest_algorithm_version: int,
    file_order_version: int,
) -> ManifestPolicyRecord:
    algorithm_version = _positive_uint32(
        manifest_algorithm_version,
        field="manifest_algorithm_version",
    )
    order_version = _positive_uint32(
        file_order_version,
        field="file_order_version",
    )
    row = _required_row(
        connector,
        query=(
            "SELECT manifest_policy_id, manifest_algorithm_version, "
            "file_order_version FROM catalog_manifest_policies "
            "WHERE manifest_algorithm_version = %s AND file_order_version = %s"
        ),
        parameters=(algorithm_version, order_version),
        label="manifest policy natural identity",
    )
    return _validated("manifest policy", lambda: ManifestPolicyRecord(*row))


def load_analysis_policy(
    connector: SQLConnector,
    policy_id: int,
) -> AnalysisPolicyRecord:
    exact_id = require_positive_int63(policy_id, field="analysis policy_id")
    row = _required_row(
        connector,
        query=(
            "SELECT policy_id, algorithm_version, spam_artist_threshold, "
            "spam_occurrence_threshold, content_owner_rule_version, "
            "gid_winner_rule_version FROM catalog_analysis_policies "
            "WHERE policy_id = %s"
        ),
        parameters=(exact_id,),
        label="analysis policy",
    )
    return _validated("analysis policy", lambda: AnalysisPolicyRecord(*row))


def load_artifact_zip_writer_policy(
    connector: SQLConnector,
    artifact_algorithm_version: int,
) -> ArtifactZipWriterPolicyRecord:
    algorithm_version = _positive_uint32(
        artifact_algorithm_version,
        field="ZIP artifact_algorithm_version",
    )
    row = _required_row(
        connector,
        query=(
            "SELECT artifact_algorithm_version, zip_codec_version, "
            "compression_method, compression_level, dos_date, dos_time, "
            "unix_mode, general_purpose_flags, create_system, "
            "archive_name_codec_version, artifact_name_codec_version "
            "FROM catalog_artifact_zip_writer_policies "
            "WHERE artifact_algorithm_version = %s"
        ),
        parameters=(algorithm_version,),
        label="artifact ZIP writer policy",
    )
    return _validated(
        "artifact ZIP writer policy",
        lambda: ArtifactZipWriterPolicyRecord(*row),
    )


def load_artifact_storage_codec(
    connector: SQLConnector,
    storage_codec_version: int,
) -> ArtifactStorageCodecRecord:
    codec_version = _positive_uint32(
        storage_codec_version,
        field="storage_codec_version",
    )
    row = _required_row(
        connector,
        query=(
            "SELECT storage_codec_version, adapter_id, locator_codec_version, "
            "protection_token_codec_version FROM catalog_artifact_storage_codecs "
            "WHERE storage_codec_version = %s"
        ),
        parameters=(codec_version,),
        label="artifact storage codec",
    )
    return _validated(
        "artifact storage codec",
        lambda: ArtifactStorageCodecRecord(*row),
    )


def load_artifact_policy_semantics(
    connector: SQLConnector,
    policy_component_sha256: bytes,
) -> ArtifactPolicySemanticsRecord:
    digest = require_digest32(
        policy_component_sha256,
        field="artifact policy_component_sha256",
    )
    row = _required_row(
        connector,
        query=(
            "SELECT semantics.policy_component_sha256, "
            "producer.artifact_algorithm_version, "
            "semantics.max_image_short_side, "
            "semantics.producer_fingerprint_sha256 "
            "FROM catalog_artifact_policy_semantics AS semantics "
            "JOIN catalog_artifact_producer_fingerprints AS producer "
            "ON producer.producer_fingerprint_sha256 = "
            "semantics.producer_fingerprint_sha256 "
            "WHERE semantics.policy_component_sha256 = %s"
        ),
        parameters=(digest,),
        label="artifact policy semantics",
    )
    return _validated(
        "artifact policy semantics",
        lambda: ArtifactPolicySemanticsRecord(*row),
    )


def load_title_sort_policy(
    connector: SQLConnector,
    title_sort_policy_id: int,
) -> TitleSortPolicyRecord:
    policy_id = require_uint32(
        title_sort_policy_id,
        field="title_sort_policy_id",
    )
    row = _required_row(
        connector,
        query=(
            "SELECT title_sort_policy_id, title_sort_algorithm_version, "
            "unicode_data_version FROM catalog_title_sort_policy "
            "WHERE title_sort_policy_id = %s"
        ),
        parameters=(policy_id,),
        label="title-sort policy",
    )
    return _validated("title-sort policy", lambda: TitleSortPolicyRecord(*row))


def load_display_title_policy(
    connector: SQLConnector,
    display_title_policy_id: int,
) -> DisplayTitlePolicyRecord:
    policy_id = require_positive_int63(
        display_title_policy_id,
        field="display_title_policy_id",
    )
    row = _required_row(
        connector,
        query=(
            "SELECT display_title_policy_id, display_title_algorithm_version, "
            "title_sort_policy_id FROM catalog_display_title_policies "
            "WHERE display_title_policy_id = %s"
        ),
        parameters=(policy_id,),
        label="display-title policy",
    )
    return _validated(
        "display-title policy",
        lambda: DisplayTitlePolicyRecord(*row),
    )


def load_source_scope(
    connector: SQLConnector,
    scope_key: bytes,
) -> SourceScopeRecord:
    digest = require_digest32(scope_key, field="source scope_key")
    row = _required_row(
        connector,
        query=(
            "SELECT scope_key, source_provider, source_root_sha256, "
            "identity_policy_version FROM catalog_source_scopes "
            "WHERE scope_key = %s"
        ),
        parameters=(digest,),
        label="source scope",
    )
    return _validated("source scope", lambda: SourceScopeRecord(*row))


def load_artifact_producer_fingerprint(
    connector: SQLConnector,
    producer_fingerprint_sha256: bytes,
) -> ArtifactProducerFingerprintRecord:
    digest = require_digest32(
        producer_fingerprint_sha256,
        field="artifact producer_fingerprint_sha256",
    )
    row = _required_row(
        connector,
        query=(
            "SELECT producer_fingerprint_sha256, artifact_algorithm_version, "
            "producer_equivalence_class, writer_id, python_abi, pillow_build, "
            "libjpeg_build, zlib_build FROM catalog_artifact_producer_fingerprints "
            "WHERE producer_fingerprint_sha256 = %s"
        ),
        parameters=(digest,),
        label="artifact producer fingerprint",
    )
    return _validated(
        "artifact producer fingerprint",
        lambda: ArtifactProducerFingerprintRecord(*row),
    )


def ensure_source_scope(
    connector: SQLConnector,
    *,
    source_provider: bytes,
    source_root_sha256: bytes,
    identity_policy_version: int,
) -> SourceScopeWrite:
    """Return or insert one exact source scope inside the caller's transaction.

    The source-build coordinator serializes this operation with its existing
    working-root lock.  Immutable registry rows themselves are never locked
    with ``FOR UPDATE``.
    """

    provider = require_ascii_bytes(
        source_provider,
        field="source_provider",
        minimum=1,
        maximum=64,
    )
    root = require_digest32(source_root_sha256, field="source_root_sha256")
    policy_version = _positive_uint32(
        identity_policy_version,
        field="source identity_policy_version",
    )
    scope_key = _validated(
        "source scope input",
        lambda: identity.source_scope_key(
            provider.decode("ascii"),
            root,
            policy_version,
        ),
    )
    expected = (scope_key, provider, root, policy_version)
    by_digest = connector.fetch_one(
        "SELECT scope_key, source_provider, source_root_sha256, "
        "identity_policy_version FROM catalog_source_scopes WHERE scope_key = %s",
        (scope_key,),
    )
    by_natural = connector.fetch_one(
        "SELECT scope_key FROM catalog_source_scopes "
        "WHERE source_provider = %s AND source_root_sha256 = %s "
        "AND identity_policy_version = %s",
        (provider, root, policy_version),
    )

    if by_digest:
        if by_digest != expected or by_natural != (scope_key,):
            raise CatalogRegistryConflictError(
                "source scope digest collides with different durable facts"
            )
        return SourceScopeWrite(
            _validated(
                "source scope",
                lambda: SourceScopeRecord(
                    scope_key,
                    provider,
                    root,
                    policy_version,
                ),
            ),
            True,
        )
    if by_natural:
        raise CatalogRegistryConflictError(
            "source scope natural identity is already mapped to another digest"
        )

    connector.execute(
        "INSERT INTO catalog_source_scopes "
        "(scope_key, source_provider, source_root_sha256, identity_policy_version) "
        "VALUES (%s, %s, %s, %s)",
        expected,
    )
    return SourceScopeWrite(load_source_scope(connector, scope_key), False)
