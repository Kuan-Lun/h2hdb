"""Exact-row helpers for test-owned immutable catalog registries.

Production code intentionally has no general policy writer: schema bootstrap
owns fixed registries and orchestration owns source-scope registration.  Tests
that need non-bootstrap policy rows insert the same authoritative BCNF rows.
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
    existing = connector.fetch_one(
        "SELECT manifest_policy_id, manifest_algorithm_version, file_order_version "
        "FROM catalog_manifest_policies WHERE manifest_policy_id = %s",
        (manifest_policy_id,),
    )
    expected = ManifestPolicyRecord(
        manifest_policy_id,
        manifest_algorithm_version,
        file_order_version,
    )
    if existing:
        return _exact_fixture_replay(
            "manifest policy",
            ManifestPolicyRecord(*existing),
            expected,
        )
    connector.execute(
        "INSERT INTO catalog_manifest_policies "
        "(manifest_policy_id, manifest_algorithm_version, file_order_version) "
        "VALUES (%s, %s, %s)",
        (
            expected.manifest_policy_id,
            expected.manifest_algorithm_version,
            expected.file_order_version,
        ),
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
    existing = connector.fetch_one(
        "SELECT policy_id, algorithm_version, spam_artist_threshold, "
        "spam_occurrence_threshold, content_owner_rule_version, "
        "gid_winner_rule_version FROM catalog_analysis_policies "
        "WHERE policy_id = %s",
        (policy_id,),
    )
    expected = AnalysisPolicyRecord(
        policy_id,
        algorithm_version,
        spam_artist_threshold,
        spam_occurrence_threshold,
        content_owner_rule_version,
        gid_winner_rule_version,
    )
    if existing:
        return _exact_fixture_replay(
            "analysis policy",
            AnalysisPolicyRecord(*existing),
            expected,
        )
    connector.execute(
        "INSERT INTO catalog_analysis_policies "
        "(policy_id, algorithm_version, spam_artist_threshold, "
        "spam_occurrence_threshold, content_owner_rule_version, "
        "gid_winner_rule_version) VALUES (%s, %s, %s, %s, %s, %s)",
        (
            expected.policy_id,
            expected.algorithm_version,
            expected.spam_artist_threshold,
            expected.spam_occurrence_threshold,
            expected.content_owner_rule_version,
            expected.gid_winner_rule_version,
        ),
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
    existing = connector.fetch_one(
        "SELECT artifact_algorithm_version, zip_codec_version, "
        "compression_method, compression_level, dos_date, dos_time, unix_mode, "
        "general_purpose_flags, create_system, archive_name_codec_version, "
        "artifact_name_codec_version FROM catalog_artifact_zip_writer_policies "
        "WHERE artifact_algorithm_version = %s",
        (artifact_algorithm_version,),
    )
    expected = ArtifactZipWriterPolicyRecord(
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
    )
    if existing:
        return _exact_fixture_replay(
            "artifact ZIP writer policy",
            ArtifactZipWriterPolicyRecord(*existing),
            expected,
        )
    connector.execute(
        "INSERT INTO catalog_artifact_zip_writer_policies "
        "(artifact_algorithm_version, zip_codec_version, compression_method, "
        "compression_level, dos_date, dos_time, unix_mode, general_purpose_flags, "
        "create_system, archive_name_codec_version, artifact_name_codec_version) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            expected.artifact_algorithm_version,
            expected.zip_codec_version,
            expected.compression_method,
            expected.compression_level,
            expected.dos_date,
            expected.dos_time,
            expected.unix_mode,
            expected.general_purpose_flags,
            expected.create_system,
            expected.archive_name_codec_version,
            expected.artifact_name_codec_version,
        ),
    )
    return load_artifact_zip_writer_policy(connector, artifact_algorithm_version)


def seed_artifact_storage_codec(
    connector: SQLConnector,
    *,
    storage_codec_version: int = 1,
    adapter_id: bytes = b"managed-filesystem",
    storage_key_codec_version: int = 1,
    protection_token_codec_version: int = 1,
) -> ArtifactStorageCodecRecord:
    existing = connector.fetch_one(
        "SELECT storage_codec_version, adapter_id, storage_key_codec_version, "
        "protection_token_codec_version FROM catalog_artifact_storage_codecs "
        "WHERE storage_codec_version = %s",
        (storage_codec_version,),
    )
    expected = ArtifactStorageCodecRecord(
        storage_codec_version,
        adapter_id,
        storage_key_codec_version,
        protection_token_codec_version,
    )
    if existing:
        return _exact_fixture_replay(
            "artifact storage codec",
            ArtifactStorageCodecRecord(*existing),
            expected,
        )
    connector.execute(
        "INSERT INTO catalog_artifact_storage_codecs "
        "(storage_codec_version, adapter_id, storage_key_codec_version, "
        "protection_token_codec_version) VALUES (%s, %s, %s, %s)",
        (
            expected.storage_codec_version,
            expected.adapter_id,
            expected.storage_key_codec_version,
            expected.protection_token_codec_version,
        ),
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
    existing = connector.fetch_one(
        "SELECT producer_fingerprint_sha256, artifact_algorithm_version, "
        "producer_equivalence_class, writer_id, python_abi, pillow_build, "
        "libjpeg_build, zlib_build FROM catalog_artifact_producer_fingerprints "
        "WHERE producer_fingerprint_sha256 = %s",
        (fingerprint,),
    )
    expected = ArtifactProducerFingerprintRecord(
        fingerprint,
        artifact_algorithm_version,
        identity.artifact_producer_equivalence_class(fingerprint),
        *fields,
    )
    if existing:
        return _exact_fixture_replay(
            "artifact producer fingerprint",
            ArtifactProducerFingerprintRecord(*existing),
            expected,
        )
    connector.execute(
        "INSERT INTO catalog_artifact_producer_fingerprints "
        "(producer_fingerprint_sha256, artifact_algorithm_version, "
        "producer_equivalence_class, writer_id, python_abi, pillow_build, "
        "libjpeg_build, zlib_build) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            expected.producer_fingerprint_sha256,
            expected.artifact_algorithm_version,
            expected.producer_equivalence_class,
            expected.writer_id,
            expected.python_abi,
            expected.pillow_build,
            expected.libjpeg_build,
            expected.zlib_build,
        ),
    )
    return load_artifact_producer_fingerprint(connector, fingerprint)


def seed_artifact_policy_semantics(
    connector: SQLConnector,
    *,
    artifact_algorithm_version: int = 1,
    max_image_short_side: int = 2048,
    producer_fingerprint_sha256: bytes,
) -> ArtifactPolicySemanticsRecord:
    producer = load_artifact_producer_fingerprint(
        connector,
        producer_fingerprint_sha256,
    )
    if producer.artifact_algorithm_version != artifact_algorithm_version:
        raise AssertionError(
            "artifact policy fixture algorithm differs from its registered producer"
        )
    policy_digest = identity.artifact_policy_digest(
        artifact_algorithm_version,
        max_image_short_side,
        producer_fingerprint_sha256,
    )
    existing = connector.fetch_one(
        "SELECT semantics.policy_component_sha256, "
        "producer.artifact_algorithm_version, "
        "semantics.max_image_short_side, "
        "semantics.producer_fingerprint_sha256 "
        "FROM catalog_artifact_policy_semantics AS semantics "
        "JOIN catalog_artifact_producer_fingerprints AS producer "
        "ON producer.producer_fingerprint_sha256 = "
        "semantics.producer_fingerprint_sha256 "
        "WHERE semantics.policy_component_sha256 = %s",
        (policy_digest,),
    )
    expected = ArtifactPolicySemanticsRecord(
        policy_digest,
        artifact_algorithm_version,
        max_image_short_side,
        producer_fingerprint_sha256,
    )
    if existing:
        return _exact_fixture_replay(
            "artifact policy semantics",
            ArtifactPolicySemanticsRecord(*existing),
            expected,
        )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics "
        "(policy_component_sha256, max_image_short_side, "
        "producer_fingerprint_sha256) VALUES (%s, %s, %s)",
        (
            expected.policy_component_sha256,
            expected.max_image_short_side,
            expected.producer_fingerprint_sha256,
        ),
    )
    return load_artifact_policy_semantics(connector, policy_digest)


def seed_title_sort_policy(
    connector: SQLConnector,
    *,
    title_sort_policy_id: int = 1,
    title_sort_algorithm_version: int = 1,
    unicode_data_version: bytes = b"14.0.0",
) -> TitleSortPolicyRecord:
    existing = connector.fetch_one(
        "SELECT title_sort_policy_id, title_sort_algorithm_version, "
        "unicode_data_version FROM catalog_title_sort_policy "
        "WHERE title_sort_policy_id = %s",
        (title_sort_policy_id,),
    )
    expected = TitleSortPolicyRecord(
        title_sort_policy_id,
        title_sort_algorithm_version,
        unicode_data_version,
    )
    if existing:
        return _exact_fixture_replay(
            "title-sort policy",
            TitleSortPolicyRecord(*existing),
            expected,
        )
    connector.execute(
        "INSERT INTO catalog_title_sort_policy "
        "(title_sort_policy_id, title_sort_algorithm_version, "
        "unicode_data_version) VALUES (%s, %s, %s)",
        (
            expected.title_sort_policy_id,
            expected.title_sort_algorithm_version,
            expected.unicode_data_version,
        ),
    )
    return load_title_sort_policy(connector, title_sort_policy_id)


def seed_display_title_policy(
    connector: SQLConnector,
    *,
    display_title_policy_id: int = 1,
    display_title_algorithm_version: int = 1,
    title_sort_policy_id: int = 1,
) -> DisplayTitlePolicyRecord:
    existing = connector.fetch_one(
        "SELECT display_title_policy_id, display_title_algorithm_version, "
        "title_sort_policy_id FROM catalog_display_title_policies "
        "WHERE display_title_policy_id = %s",
        (display_title_policy_id,),
    )
    expected = DisplayTitlePolicyRecord(
        display_title_policy_id,
        display_title_algorithm_version,
        title_sort_policy_id,
    )
    if existing:
        return _exact_fixture_replay(
            "display-title policy",
            DisplayTitlePolicyRecord(*existing),
            expected,
        )
    connector.execute(
        "INSERT INTO catalog_display_title_policies "
        "(display_title_policy_id, display_title_algorithm_version, "
        "title_sort_policy_id) VALUES (%s, %s, %s)",
        (
            expected.display_title_policy_id,
            expected.display_title_algorithm_version,
            expected.title_sort_policy_id,
        ),
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
