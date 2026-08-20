"""Seal-last helpers for test-owned immutable catalog registry rows.

Production code intentionally has no general policy writer: schema bootstrap
owns fixed registries and orchestration owns source-scope registration.  Tests
that need non-bootstrap policy rows use these exact physical-family helpers
instead of attempting DML through the logical compatibility views.
"""

from __future__ import annotations

from h2hdb import vnext_identity as identity
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_catalog_registry_repository import (
    AnalysisPolicyRecord,
    ArtifactPolicySemanticsRecord,
    ArtifactProducerFingerprintRecord,
    ArtifactStorageCodecRecord,
    ArtifactZipWriterPolicyRecord,
    DisplayTitlePolicyRecord,
    ManifestPolicyRecord,
    SourceScopeRecord,
    TitleSortPolicyRecord,
    ensure_source_scope,
    load_analysis_policy,
    load_artifact_policy_semantics,
    load_artifact_producer_fingerprint,
    load_artifact_storage_codec,
    load_artifact_zip_writer_policy,
    load_display_title_policy,
    load_manifest_policy,
    load_source_scope,
    load_title_sort_policy,
)


def _exact_fixture_replay[T](label: str, actual: T, expected: T) -> T:
    if actual != expected:
        raise AssertionError(f"{label} fixture replay differs from requested facts")
    return actual


def seed_manifest_policy(
    connector: SQLConnector,
    *,
    manifest_policy_id: int = 1,
    manifest_algorithm_version: int = 1,
    file_order_version: int = 1,
) -> ManifestPolicyRecord:
    if connector.fetch_one(
        "SELECT manifest_policy_id FROM catalog_manifest_policy_seals "
        "WHERE manifest_policy_id = %s",
        (manifest_policy_id,),
    ):
        return _exact_fixture_replay(
            "manifest policy",
            load_manifest_policy(connector, manifest_policy_id),
            ManifestPolicyRecord(
                manifest_policy_id,
                manifest_algorithm_version,
                file_order_version,
            ),
        )
    connector.execute(
        "INSERT INTO catalog_manifest_policy_anchors (manifest_policy_id) "
        "VALUES (%s)",
        (manifest_policy_id,),
    )
    connector.execute(
        "INSERT INTO catalog_manifest_policy_manifest_algorithm_versions "
        "(manifest_policy_id, manifest_algorithm_version) VALUES (%s, %s)",
        (manifest_policy_id, manifest_algorithm_version),
    )
    connector.execute(
        "INSERT INTO catalog_manifest_policy_file_order_versions "
        "(manifest_policy_id, file_order_version) VALUES (%s, %s)",
        (manifest_policy_id, file_order_version),
    )
    connector.execute(
        "INSERT INTO catalog_manifest_policy_identities "
        "(manifest_algorithm_version, file_order_version, manifest_policy_id) "
        "VALUES (%s, %s, %s)",
        (manifest_algorithm_version, file_order_version, manifest_policy_id),
    )
    connector.execute(
        "INSERT INTO catalog_manifest_policy_seals (manifest_policy_id) VALUES (%s)",
        (manifest_policy_id,),
    )
    return load_manifest_policy(connector, manifest_policy_id)


def seed_analysis_policy(
    connector: SQLConnector,
    *,
    policy_id: int = 1,
    algorithm_version: int = 1,
    spam_artist_threshold: int = 1,
    spam_occurrence_threshold: int = 3,
    content_owner_rule_version: int = 1,
    gid_winner_rule_version: int = 1,
) -> AnalysisPolicyRecord:
    if connector.fetch_one(
        "SELECT policy_id FROM catalog_analysis_policy_seals WHERE policy_id = %s",
        (policy_id,),
    ):
        return _exact_fixture_replay(
            "analysis policy",
            load_analysis_policy(connector, policy_id),
            AnalysisPolicyRecord(
                policy_id,
                algorithm_version,
                spam_artist_threshold,
                spam_occurrence_threshold,
                content_owner_rule_version,
                gid_winner_rule_version,
            ),
        )
    connector.execute(
        "INSERT INTO catalog_analysis_policy_anchors (policy_id) VALUES (%s)",
        (policy_id,),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_policy_algorithm_versions "
        "(policy_id, algorithm_version) VALUES (%s, %s)",
        (policy_id, algorithm_version),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_policy_spam_artist_thresholds "
        "(policy_id, spam_artist_threshold) VALUES (%s, %s)",
        (policy_id, spam_artist_threshold),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_policy_spam_occurrence_thresholds "
        "(policy_id, spam_occurrence_threshold) VALUES (%s, %s)",
        (policy_id, spam_occurrence_threshold),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_policy_content_owner_rule_versions "
        "(policy_id, content_owner_rule_version) VALUES (%s, %s)",
        (policy_id, content_owner_rule_version),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_policy_gid_winner_rule_versions "
        "(policy_id, gid_winner_rule_version) VALUES (%s, %s)",
        (policy_id, gid_winner_rule_version),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_policy_identities "
        "(algorithm_version, spam_artist_threshold, spam_occurrence_threshold, "
        "content_owner_rule_version, gid_winner_rule_version, policy_id) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (
            algorithm_version,
            spam_artist_threshold,
            spam_occurrence_threshold,
            content_owner_rule_version,
            gid_winner_rule_version,
            policy_id,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_policy_seals (policy_id) VALUES (%s)",
        (policy_id,),
    )
    return load_analysis_policy(connector, policy_id)


def seed_artifact_zip_writer_policy(
    connector: SQLConnector,
    *,
    artifact_algorithm_version: int = 1,
    zip_codec_version: int = 1,
    compression_method: int = 8,
    compression_level: int = 9,
    dos_date: int = 33,
    dos_time: int = 0,
    unix_mode: int = 33188,
    general_purpose_flags: int = 2048,
    create_system: int = 3,
    archive_name_codec_version: int = 1,
    artifact_name_codec_version: int = 1,
) -> ArtifactZipWriterPolicyRecord:
    if connector.fetch_one(
        "SELECT artifact_algorithm_version "
        "FROM catalog_artifact_zip_writer_policy_seals "
        "WHERE artifact_algorithm_version = %s",
        (artifact_algorithm_version,),
    ):
        return _exact_fixture_replay(
            "artifact ZIP writer policy",
            load_artifact_zip_writer_policy(
                connector,
                artifact_algorithm_version,
            ),
            ArtifactZipWriterPolicyRecord(
                artifact_algorithm_version,
                zip_codec_version,
                compression_method,
                compression_level,
                dos_date,
                dos_time,
                unix_mode,
                general_purpose_flags,
                create_system,
                archive_name_codec_version,
                artifact_name_codec_version,
            ),
        )
    connector.execute(
        "INSERT INTO catalog_artifact_zip_writer_policy_anchors "
        "(artifact_algorithm_version) VALUES (%s)",
        (artifact_algorithm_version,),
    )
    facts = (
        (
            "INSERT INTO catalog_artifact_zip_writer_policy_zip_codec_versions "
            "(artifact_algorithm_version, zip_codec_version) VALUES (%s, %s)",
            zip_codec_version,
        ),
        (
            "INSERT INTO catalog_artifact_zip_writer_policy_compression_methods "
            "(artifact_algorithm_version, compression_method) VALUES (%s, %s)",
            compression_method,
        ),
        (
            "INSERT INTO catalog_artifact_zip_writer_policy_compression_levels "
            "(artifact_algorithm_version, compression_level) VALUES (%s, %s)",
            compression_level,
        ),
        (
            "INSERT INTO catalog_artifact_zip_writer_policy_dos_dates "
            "(artifact_algorithm_version, dos_date) VALUES (%s, %s)",
            dos_date,
        ),
        (
            "INSERT INTO catalog_artifact_zip_writer_policy_dos_times "
            "(artifact_algorithm_version, dos_time) VALUES (%s, %s)",
            dos_time,
        ),
        (
            "INSERT INTO catalog_artifact_zip_writer_policy_unix_modes "
            "(artifact_algorithm_version, unix_mode) VALUES (%s, %s)",
            unix_mode,
        ),
        (
            "INSERT INTO catalog_artifact_zip_writer_policy_general_purpose_flags "
            "(artifact_algorithm_version, general_purpose_flags) VALUES (%s, %s)",
            general_purpose_flags,
        ),
        (
            "INSERT INTO catalog_artifact_zip_writer_policy_create_systems "
            "(artifact_algorithm_version, create_system) VALUES (%s, %s)",
            create_system,
        ),
        (
            "INSERT INTO "
            "catalog_artifact_zip_writer_policy_archive_name_codec_versions "
            "(artifact_algorithm_version, archive_name_codec_version) "
            "VALUES (%s, %s)",
            archive_name_codec_version,
        ),
        (
            "INSERT INTO "
            "catalog_artifact_zip_writer_policy_artifact_name_codec_versions "
            "(artifact_algorithm_version, artifact_name_codec_version) "
            "VALUES (%s, %s)",
            artifact_name_codec_version,
        ),
    )
    for query, value in facts:
        connector.execute(query, (artifact_algorithm_version, value))
    natural = (
        zip_codec_version,
        compression_method,
        compression_level,
        dos_date,
        dos_time,
        unix_mode,
        general_purpose_flags,
        create_system,
        archive_name_codec_version,
        artifact_name_codec_version,
    )
    connector.execute(
        "INSERT INTO catalog_artifact_zip_writer_policy_identities "
        "(zip_codec_version, compression_method, compression_level, dos_date, "
        "dos_time, unix_mode, general_purpose_flags, create_system, "
        "archive_name_codec_version, artifact_name_codec_version, "
        "artifact_algorithm_version) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (*natural, artifact_algorithm_version),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_zip_writer_policy_seals "
        "(artifact_algorithm_version) VALUES (%s)",
        (artifact_algorithm_version,),
    )
    return load_artifact_zip_writer_policy(connector, artifact_algorithm_version)


def seed_artifact_storage_codec(
    connector: SQLConnector,
    *,
    storage_codec_version: int = 1,
    adapter_id: bytes = b"managed-filesystem",
    locator_codec_version: int = 1,
    protection_token_codec_version: int = 1,
) -> ArtifactStorageCodecRecord:
    if connector.fetch_one(
        "SELECT storage_codec_version FROM catalog_artifact_storage_codec_seals "
        "WHERE storage_codec_version = %s",
        (storage_codec_version,),
    ):
        return _exact_fixture_replay(
            "artifact storage codec",
            load_artifact_storage_codec(connector, storage_codec_version),
            ArtifactStorageCodecRecord(
                storage_codec_version,
                adapter_id,
                locator_codec_version,
                protection_token_codec_version,
            ),
        )
    connector.execute(
        "INSERT INTO catalog_artifact_storage_codec_anchors "
        "(storage_codec_version) VALUES (%s)",
        (storage_codec_version,),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_storage_codec_adapter_ids "
        "(storage_codec_version, adapter_id) VALUES (%s, %s)",
        (storage_codec_version, adapter_id),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_storage_codec_locator_codec_versions "
        "(storage_codec_version, locator_codec_version) VALUES (%s, %s)",
        (storage_codec_version, locator_codec_version),
    )
    connector.execute(
        "INSERT INTO "
        "catalog_artifact_storage_codec_protection_token_codec_versions "
        "(storage_codec_version, protection_token_codec_version) VALUES (%s, %s)",
        (storage_codec_version, protection_token_codec_version),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_storage_codec_seals "
        "(storage_codec_version) VALUES (%s)",
        (storage_codec_version,),
    )
    return load_artifact_storage_codec(connector, storage_codec_version)


def seed_artifact_producer_fingerprint(
    connector: SQLConnector,
    *,
    artifact_algorithm_version: int = 1,
    writer_id: bytes = b"writer",
    python_abi: bytes = b"python",
    pillow_build: bytes = b"pillow",
    libjpeg_build: bytes = b"jpeg",
    zlib_build: bytes = b"zlib",
) -> ArtifactProducerFingerprintRecord:
    fields = (writer_id, python_abi, pillow_build, libjpeg_build, zlib_build)
    fingerprint = identity.artifact_producer_fingerprint_sha256(*fields)
    if connector.fetch_one(
        "SELECT producer_fingerprint_sha256 "
        "FROM catalog_artifact_producer_fingerprint_seals "
        "WHERE producer_fingerprint_sha256 = %s",
        (fingerprint,),
    ):
        return _exact_fixture_replay(
            "artifact producer fingerprint",
            load_artifact_producer_fingerprint(connector, fingerprint),
            ArtifactProducerFingerprintRecord(
                fingerprint,
                artifact_algorithm_version,
                identity.artifact_producer_equivalence_class(fingerprint),
                *fields,
            ),
        )
    equivalence = identity.artifact_producer_equivalence_class(fingerprint)
    connector.execute(
        "INSERT INTO catalog_artifact_producer_fingerprint_anchors "
        "(producer_fingerprint_sha256) VALUES (%s)",
        (fingerprint,),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_producer_fingerprint_algorithm_versions "
        "(producer_fingerprint_sha256, artifact_algorithm_version) "
        "VALUES (%s, %s)",
        (fingerprint, artifact_algorithm_version),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_producer_fingerprint_equivalence_classes "
        "(producer_fingerprint_sha256, producer_equivalence_class) "
        "VALUES (%s, %s)",
        (fingerprint, equivalence),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_producer_fingerprint_identities "
        "(writer_id, python_abi, pillow_build, libjpeg_build, zlib_build, "
        "producer_fingerprint_sha256) VALUES (%s, %s, %s, %s, %s, %s)",
        (*fields, fingerprint),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_producer_fingerprint_seals "
        "(producer_fingerprint_sha256) VALUES (%s)",
        (fingerprint,),
    )
    return load_artifact_producer_fingerprint(connector, fingerprint)


def seed_artifact_policy_semantics(
    connector: SQLConnector,
    *,
    artifact_algorithm_version: int = 1,
    max_image_short_side: int = 2048,
    producer_fingerprint_sha256: bytes,
) -> ArtifactPolicySemanticsRecord:
    policy_digest = identity.artifact_policy_digest(
        artifact_algorithm_version,
        max_image_short_side,
        producer_fingerprint_sha256,
    )
    if connector.fetch_one(
        "SELECT policy_component_sha256 "
        "FROM catalog_artifact_policy_semantics_seals "
        "WHERE policy_component_sha256 = %s",
        (policy_digest,),
    ):
        return _exact_fixture_replay(
            "artifact policy semantics",
            load_artifact_policy_semantics(connector, policy_digest),
            ArtifactPolicySemanticsRecord(
                policy_digest,
                artifact_algorithm_version,
                max_image_short_side,
                producer_fingerprint_sha256,
            ),
        )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics_anchors "
        "(policy_component_sha256) VALUES (%s)",
        (policy_digest,),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics_artifact_algorithm_versions "
        "(policy_component_sha256, artifact_algorithm_version) VALUES (%s, %s)",
        (policy_digest, artifact_algorithm_version),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics_max_image_short_sides "
        "(policy_component_sha256, max_image_short_side) VALUES (%s, %s)",
        (policy_digest, max_image_short_side),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics_producer_fingerprint_sha256s "
        "(policy_component_sha256, producer_fingerprint_sha256) VALUES (%s, %s)",
        (policy_digest, producer_fingerprint_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics_identities "
        "(artifact_algorithm_version, max_image_short_side, "
        "producer_fingerprint_sha256, policy_component_sha256) "
        "VALUES (%s, %s, %s, %s)",
        (
            artifact_algorithm_version,
            max_image_short_side,
            producer_fingerprint_sha256,
            policy_digest,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics_seals "
        "(policy_component_sha256) VALUES (%s)",
        (policy_digest,),
    )
    return load_artifact_policy_semantics(connector, policy_digest)


def seed_title_sort_policy(
    connector: SQLConnector,
    *,
    title_sort_policy_id: int = 1,
    title_sort_algorithm_version: int = 1,
    unicode_data_version: bytes = b"14.0.0",
) -> TitleSortPolicyRecord:
    if connector.fetch_one(
        "SELECT title_sort_policy_id FROM catalog_title_sort_policy_seals "
        "WHERE title_sort_policy_id = %s",
        (title_sort_policy_id,),
    ):
        return _exact_fixture_replay(
            "title-sort policy",
            load_title_sort_policy(connector, title_sort_policy_id),
            TitleSortPolicyRecord(
                title_sort_policy_id,
                title_sort_algorithm_version,
                unicode_data_version,
            ),
        )
    connector.execute(
        "INSERT INTO catalog_title_sort_policy_anchors (title_sort_policy_id) "
        "VALUES (%s)",
        (title_sort_policy_id,),
    )
    connector.execute(
        "INSERT INTO catalog_title_sort_policy_algorithm_versions "
        "(title_sort_policy_id, title_sort_algorithm_version) VALUES (%s, %s)",
        (title_sort_policy_id, title_sort_algorithm_version),
    )
    connector.execute(
        "INSERT INTO catalog_title_sort_policy_unicode_data_versions "
        "(title_sort_policy_id, unicode_data_version) VALUES (%s, %s)",
        (title_sort_policy_id, unicode_data_version),
    )
    connector.execute(
        "INSERT INTO catalog_title_sort_policy_identities "
        "(title_sort_algorithm_version, unicode_data_version, "
        "title_sort_policy_id) VALUES (%s, %s, %s)",
        (
            title_sort_algorithm_version,
            unicode_data_version,
            title_sort_policy_id,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_title_sort_policy_seals (title_sort_policy_id) "
        "VALUES (%s)",
        (title_sort_policy_id,),
    )
    return load_title_sort_policy(connector, title_sort_policy_id)


def seed_display_title_policy(
    connector: SQLConnector,
    *,
    display_title_policy_id: int = 1,
    display_title_algorithm_version: int = 1,
    title_sort_policy_id: int = 1,
) -> DisplayTitlePolicyRecord:
    if connector.fetch_one(
        "SELECT display_title_policy_id FROM catalog_display_title_policy_seals "
        "WHERE display_title_policy_id = %s",
        (display_title_policy_id,),
    ):
        return _exact_fixture_replay(
            "display-title policy",
            load_display_title_policy(connector, display_title_policy_id),
            DisplayTitlePolicyRecord(
                display_title_policy_id,
                display_title_algorithm_version,
                title_sort_policy_id,
            ),
        )
    connector.execute(
        "INSERT INTO catalog_display_title_policy_anchors "
        "(display_title_policy_id) VALUES (%s)",
        (display_title_policy_id,),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policy_algorithm_versions "
        "(display_title_policy_id, display_title_algorithm_version) "
        "VALUES (%s, %s)",
        (display_title_policy_id, display_title_algorithm_version),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policy_title_sort_policy_ids "
        "(display_title_policy_id, title_sort_policy_id) VALUES (%s, %s)",
        (display_title_policy_id, title_sort_policy_id),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policy_identities "
        "(display_title_algorithm_version, title_sort_policy_id, "
        "display_title_policy_id) VALUES (%s, %s, %s)",
        (
            display_title_algorithm_version,
            title_sort_policy_id,
            display_title_policy_id,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policy_seals "
        "(display_title_policy_id) VALUES (%s)",
        (display_title_policy_id,),
    )
    return load_display_title_policy(connector, display_title_policy_id)


def seed_source_scope(
    connector: SQLConnector,
    *,
    source_provider: bytes = b"filesystem",
    source_root_sha256: bytes,
    identity_policy_version: int = 1,
) -> SourceScopeRecord:
    write = ensure_source_scope(
        connector,
        source_provider=source_provider,
        source_root_sha256=source_root_sha256,
        identity_policy_version=identity_policy_version,
    )
    return load_source_scope(connector, write.record.scope_key)
