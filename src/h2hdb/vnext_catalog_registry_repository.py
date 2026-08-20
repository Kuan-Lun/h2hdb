"""Typed, fail-closed access to sealed vNext catalog registries.

The catalog manifest exposes compatibility views for broad logical reads, but
runtime hot paths use the narrow physical satellites directly.  Every loader
below starts from a totality seal and joins every required fact plus its cold
natural identity.  Consequently an absent or partially-written family is
never mistaken for a usable policy.

The only writer in this module is source-scope registration.  Its caller owns
the transaction and must already hold the source-build allocation lock.  It
derives the digest, checks both candidate-key directions, inserts the identity
last-but-one, and publishes the family with the seal as the final statement.
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
    """A requested family is absent, incomplete, or not yet sealed."""


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


def _sealed_row(
    connector: SQLConnector,
    *,
    query: str,
    parameters: tuple[Any, ...],
    label: str,
) -> tuple[Any, ...]:
    row = connector.fetch_one(query, parameters)
    if not row:
        raise CatalogRegistryNotReadyError(f"{label} is absent or not sealed")
    return row


_MANIFEST_POLICY_BY_ID = (
    "SELECT seal.manifest_policy_id, algorithm.manifest_algorithm_version, "
    "file_order.file_order_version "
    "FROM catalog_manifest_policy_seals AS seal "
    "JOIN catalog_manifest_policy_manifest_algorithm_versions AS algorithm "
    "ON algorithm.manifest_policy_id = seal.manifest_policy_id "
    "JOIN catalog_manifest_policy_file_order_versions AS file_order "
    "ON file_order.manifest_policy_id = seal.manifest_policy_id "
    "JOIN catalog_manifest_policy_identities AS identity "
    "ON identity.manifest_policy_id = seal.manifest_policy_id "
    "AND identity.manifest_algorithm_version = "
    "algorithm.manifest_algorithm_version "
    "AND identity.file_order_version = file_order.file_order_version "
    "WHERE seal.manifest_policy_id = %s"
)


def load_manifest_policy(
    connector: SQLConnector,
    manifest_policy_id: int,
) -> ManifestPolicyRecord:
    policy_id = require_positive_int63(
        manifest_policy_id,
        field="manifest_policy_id",
    )
    row = _sealed_row(
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
    row = _sealed_row(
        connector,
        query=(
            "SELECT seal.manifest_policy_id, "
            "algorithm.manifest_algorithm_version, file_order.file_order_version "
            "FROM catalog_manifest_policy_identities AS identity "
            "JOIN catalog_manifest_policy_seals AS seal "
            "ON seal.manifest_policy_id = identity.manifest_policy_id "
            "JOIN catalog_manifest_policy_manifest_algorithm_versions AS algorithm "
            "ON algorithm.manifest_policy_id = identity.manifest_policy_id "
            "AND algorithm.manifest_algorithm_version = "
            "identity.manifest_algorithm_version "
            "JOIN catalog_manifest_policy_file_order_versions AS file_order "
            "ON file_order.manifest_policy_id = identity.manifest_policy_id "
            "AND file_order.file_order_version = identity.file_order_version "
            "WHERE identity.manifest_algorithm_version = %s "
            "AND identity.file_order_version = %s"
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
    row = _sealed_row(
        connector,
        query=(
            "SELECT seal.policy_id, algorithm.algorithm_version, "
            "artist.spam_artist_threshold, occurrence.spam_occurrence_threshold, "
            "owner.content_owner_rule_version, winner.gid_winner_rule_version "
            "FROM catalog_analysis_policy_seals AS seal "
            "JOIN catalog_analysis_policy_algorithm_versions AS algorithm "
            "ON algorithm.policy_id = seal.policy_id "
            "JOIN catalog_analysis_policy_spam_artist_thresholds AS artist "
            "ON artist.policy_id = seal.policy_id "
            "JOIN catalog_analysis_policy_spam_occurrence_thresholds AS occurrence "
            "ON occurrence.policy_id = seal.policy_id "
            "JOIN catalog_analysis_policy_content_owner_rule_versions AS owner "
            "ON owner.policy_id = seal.policy_id "
            "JOIN catalog_analysis_policy_gid_winner_rule_versions AS winner "
            "ON winner.policy_id = seal.policy_id "
            "JOIN catalog_analysis_policy_identities AS identity "
            "ON identity.policy_id = seal.policy_id "
            "AND identity.algorithm_version = algorithm.algorithm_version "
            "AND identity.spam_artist_threshold = artist.spam_artist_threshold "
            "AND identity.spam_occurrence_threshold = "
            "occurrence.spam_occurrence_threshold "
            "AND identity.content_owner_rule_version = "
            "owner.content_owner_rule_version "
            "AND identity.gid_winner_rule_version = winner.gid_winner_rule_version "
            "WHERE seal.policy_id = %s"
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
    row = _sealed_row(
        connector,
        query=(
            "SELECT seal.artifact_algorithm_version, zip.zip_codec_version, "
            "method.compression_method, level.compression_level, "
            "date_fact.dos_date, time_fact.dos_time, mode.unix_mode, "
            "flags.general_purpose_flags, system_fact.create_system, "
            "archive.archive_name_codec_version, "
            "artifact.artifact_name_codec_version "
            "FROM catalog_artifact_zip_writer_policy_seals AS seal "
            "JOIN catalog_artifact_zip_writer_policy_zip_codec_versions AS zip "
            "ON zip.artifact_algorithm_version = seal.artifact_algorithm_version "
            "JOIN catalog_artifact_zip_writer_policy_compression_methods AS method "
            "ON method.artifact_algorithm_version = seal.artifact_algorithm_version "
            "JOIN catalog_artifact_zip_writer_policy_compression_levels AS level "
            "ON level.artifact_algorithm_version = seal.artifact_algorithm_version "
            "JOIN catalog_artifact_zip_writer_policy_dos_dates AS date_fact "
            "ON date_fact.artifact_algorithm_version = seal.artifact_algorithm_version "
            "JOIN catalog_artifact_zip_writer_policy_dos_times AS time_fact "
            "ON time_fact.artifact_algorithm_version = seal.artifact_algorithm_version "
            "JOIN catalog_artifact_zip_writer_policy_unix_modes AS mode "
            "ON mode.artifact_algorithm_version = seal.artifact_algorithm_version "
            "JOIN catalog_artifact_zip_writer_policy_general_purpose_flags AS flags "
            "ON flags.artifact_algorithm_version = seal.artifact_algorithm_version "
            "JOIN catalog_artifact_zip_writer_policy_create_systems AS system_fact "
            "ON system_fact.artifact_algorithm_version = seal.artifact_algorithm_version "
            "JOIN catalog_artifact_zip_writer_policy_archive_name_codec_versions "
            "AS archive ON archive.artifact_algorithm_version = "
            "seal.artifact_algorithm_version "
            "JOIN catalog_artifact_zip_writer_policy_artifact_name_codec_versions "
            "AS artifact ON artifact.artifact_algorithm_version = "
            "seal.artifact_algorithm_version "
            "JOIN catalog_artifact_zip_writer_policy_identities AS identity "
            "ON identity.artifact_algorithm_version = seal.artifact_algorithm_version "
            "AND identity.zip_codec_version = zip.zip_codec_version "
            "AND identity.compression_method = method.compression_method "
            "AND identity.compression_level = level.compression_level "
            "AND identity.dos_date = date_fact.dos_date "
            "AND identity.dos_time = time_fact.dos_time "
            "AND identity.unix_mode = mode.unix_mode "
            "AND identity.general_purpose_flags = flags.general_purpose_flags "
            "AND identity.create_system = system_fact.create_system "
            "AND identity.archive_name_codec_version = "
            "archive.archive_name_codec_version "
            "AND identity.artifact_name_codec_version = "
            "artifact.artifact_name_codec_version "
            "WHERE seal.artifact_algorithm_version = %s"
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
    row = _sealed_row(
        connector,
        query=(
            "SELECT seal.storage_codec_version, adapter.adapter_id, "
            "locator.locator_codec_version, "
            "protection.protection_token_codec_version "
            "FROM catalog_artifact_storage_codec_seals AS seal "
            "JOIN catalog_artifact_storage_codec_adapter_ids AS adapter "
            "ON adapter.storage_codec_version = seal.storage_codec_version "
            "JOIN catalog_artifact_storage_codec_locator_codec_versions AS locator "
            "ON locator.storage_codec_version = seal.storage_codec_version "
            "JOIN catalog_artifact_storage_codec_protection_token_codec_versions "
            "AS protection ON protection.storage_codec_version = "
            "seal.storage_codec_version "
            "WHERE seal.storage_codec_version = %s"
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
    row = _sealed_row(
        connector,
        query=(
            "SELECT seal.policy_component_sha256, "
            "algorithm.artifact_algorithm_version, side.max_image_short_side, "
            "producer.producer_fingerprint_sha256 "
            "FROM catalog_artifact_policy_semantics_seals AS seal "
            "JOIN catalog_artifact_policy_semantics_artifact_algorithm_versions "
            "AS algorithm ON algorithm.policy_component_sha256 = "
            "seal.policy_component_sha256 "
            "JOIN catalog_artifact_policy_semantics_max_image_short_sides AS side "
            "ON side.policy_component_sha256 = seal.policy_component_sha256 "
            "JOIN catalog_artifact_policy_semantics_producer_fingerprint_sha256s "
            "AS producer ON producer.policy_component_sha256 = "
            "seal.policy_component_sha256 "
            "JOIN catalog_artifact_policy_semantics_identities AS identity "
            "ON identity.policy_component_sha256 = seal.policy_component_sha256 "
            "AND identity.artifact_algorithm_version = "
            "algorithm.artifact_algorithm_version "
            "AND identity.max_image_short_side = side.max_image_short_side "
            "AND identity.producer_fingerprint_sha256 = "
            "producer.producer_fingerprint_sha256 "
            "WHERE seal.policy_component_sha256 = %s"
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
    row = _sealed_row(
        connector,
        query=(
            "SELECT seal.title_sort_policy_id, "
            "algorithm.title_sort_algorithm_version, unicode.unicode_data_version "
            "FROM catalog_title_sort_policy_seals AS seal "
            "JOIN catalog_title_sort_policy_algorithm_versions AS algorithm "
            "ON algorithm.title_sort_policy_id = seal.title_sort_policy_id "
            "JOIN catalog_title_sort_policy_unicode_data_versions AS unicode "
            "ON unicode.title_sort_policy_id = seal.title_sort_policy_id "
            "JOIN catalog_title_sort_policy_identities AS identity "
            "ON identity.title_sort_policy_id = seal.title_sort_policy_id "
            "AND identity.title_sort_algorithm_version = "
            "algorithm.title_sort_algorithm_version "
            "AND identity.unicode_data_version = unicode.unicode_data_version "
            "WHERE seal.title_sort_policy_id = %s"
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
    row = _sealed_row(
        connector,
        query=(
            "SELECT seal.display_title_policy_id, "
            "algorithm.display_title_algorithm_version, "
            "sort.title_sort_policy_id "
            "FROM catalog_display_title_policy_seals AS seal "
            "JOIN catalog_display_title_policy_algorithm_versions AS algorithm "
            "ON algorithm.display_title_policy_id = seal.display_title_policy_id "
            "JOIN catalog_display_title_policy_title_sort_policy_ids AS sort "
            "ON sort.display_title_policy_id = seal.display_title_policy_id "
            "JOIN catalog_display_title_policy_identities AS identity "
            "ON identity.display_title_policy_id = seal.display_title_policy_id "
            "AND identity.display_title_algorithm_version = "
            "algorithm.display_title_algorithm_version "
            "AND identity.title_sort_policy_id = sort.title_sort_policy_id "
            "WHERE seal.display_title_policy_id = %s"
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
    row = _sealed_row(
        connector,
        query=(
            "SELECT seal.scope_key, provider.source_provider, "
            "root.source_root_sha256, version.identity_policy_version "
            "FROM catalog_source_scope_seals AS seal "
            "JOIN catalog_source_scope_source_providers AS provider "
            "ON provider.scope_key = seal.scope_key "
            "JOIN catalog_source_scope_source_root_sha256s AS root "
            "ON root.scope_key = seal.scope_key "
            "JOIN catalog_source_scope_identity_policy_versions AS version "
            "ON version.scope_key = seal.scope_key "
            "JOIN catalog_source_scope_identities AS identity "
            "ON identity.scope_key = seal.scope_key "
            "AND identity.source_provider = provider.source_provider "
            "AND identity.source_root_sha256 = root.source_root_sha256 "
            "AND identity.identity_policy_version = version.identity_policy_version "
            "WHERE seal.scope_key = %s"
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
    row = _sealed_row(
        connector,
        query=(
            "SELECT seal.producer_fingerprint_sha256, "
            "algorithm.artifact_algorithm_version, "
            "equivalence.producer_equivalence_class, identity.writer_id, "
            "identity.python_abi, identity.pillow_build, identity.libjpeg_build, "
            "identity.zlib_build "
            "FROM catalog_artifact_producer_fingerprint_seals AS seal "
            "JOIN catalog_artifact_producer_fingerprint_algorithm_versions "
            "AS algorithm ON algorithm.producer_fingerprint_sha256 = "
            "seal.producer_fingerprint_sha256 "
            "JOIN catalog_artifact_producer_fingerprint_equivalence_classes "
            "AS equivalence ON equivalence.producer_fingerprint_sha256 = "
            "seal.producer_fingerprint_sha256 "
            "JOIN catalog_artifact_producer_fingerprint_identities AS identity "
            "ON identity.producer_fingerprint_sha256 = "
            "seal.producer_fingerprint_sha256 "
            "WHERE seal.producer_fingerprint_sha256 = %s"
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
    """Return or seal one exact source scope inside the caller's transaction.

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
    expected = (
        scope_key,
        provider,
        root,
        policy_version,
        provider,
        root,
        policy_version,
        scope_key,
        scope_key,
    )
    by_digest = connector.fetch_one(
        "SELECT anchor.scope_key, provider.source_provider, "
        "root.source_root_sha256, version.identity_policy_version, "
        "identity.source_provider, identity.source_root_sha256, "
        "identity.identity_policy_version, identity.scope_key, seal.scope_key "
        "FROM catalog_source_scope_anchors AS anchor "
        "LEFT JOIN catalog_source_scope_source_providers AS provider "
        "ON provider.scope_key = anchor.scope_key "
        "LEFT JOIN catalog_source_scope_source_root_sha256s AS root "
        "ON root.scope_key = anchor.scope_key "
        "LEFT JOIN catalog_source_scope_identity_policy_versions AS version "
        "ON version.scope_key = anchor.scope_key "
        "LEFT JOIN catalog_source_scope_identities AS identity "
        "ON identity.scope_key = anchor.scope_key "
        "LEFT JOIN catalog_source_scope_seals AS seal "
        "ON seal.scope_key = anchor.scope_key "
        "WHERE anchor.scope_key = %s",
        (scope_key,),
    )
    by_natural = connector.fetch_one(
        "SELECT scope_key FROM catalog_source_scope_identities "
        "WHERE source_provider = %s AND source_root_sha256 = %s "
        "AND identity_policy_version = %s",
        (provider, root, policy_version),
    )

    if by_digest:
        if any(value is None for value in by_digest):
            raise CatalogRegistryNotReadyError(
                "source scope family exists but is incomplete or unsealed"
            )
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
        "INSERT INTO catalog_source_scope_anchors (scope_key) VALUES (%s)",
        (scope_key,),
    )
    connector.execute(
        "INSERT INTO catalog_source_scope_source_providers "
        "(scope_key, source_provider) VALUES (%s, %s)",
        (scope_key, provider),
    )
    connector.execute(
        "INSERT INTO catalog_source_scope_source_root_sha256s "
        "(scope_key, source_root_sha256) VALUES (%s, %s)",
        (scope_key, root),
    )
    connector.execute(
        "INSERT INTO catalog_source_scope_identity_policy_versions "
        "(scope_key, identity_policy_version) VALUES (%s, %s)",
        (scope_key, policy_version),
    )
    connector.execute(
        "INSERT INTO catalog_source_scope_identities "
        "(source_provider, source_root_sha256, identity_policy_version, scope_key) "
        "VALUES (%s, %s, %s, %s)",
        (provider, root, policy_version, scope_key),
    )
    connector.execute(
        "INSERT INTO catalog_source_scope_seals (scope_key) VALUES (%s)",
        (scope_key,),
    )
    return SourceScopeWrite(load_source_scope(connector, scope_key), False)
