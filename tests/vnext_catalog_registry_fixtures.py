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
    ArtifactAdapterPolicyRecord,
    ArtifactPolicySemanticsRecord,
    DisplayTitlePolicyRecord,
    ManifestPolicyRecord,
    SourceScopeRecord,
    TitleSortPolicyRecord,
    ensure_source_scope,
    load_analysis_policy,
    load_artifact_adapter_policy,
    load_artifact_policy_semantics,
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


def seed_artifact_adapter_policy(
    connector: SQLConnector,
    *,
    policy_fingerprint_sha256: bytes,
    adapter_id: bytes = b"test-artifact-adapter",
) -> ArtifactAdapterPolicyRecord:
    expected = ArtifactAdapterPolicyRecord(
        policy_fingerprint_sha256,
        adapter_id,
    )
    existing = connector.fetch_one(
        "SELECT policy_fingerprint_sha256, adapter_id "
        "FROM catalog_artifact_adapter_policy "
        "WHERE policy_fingerprint_sha256 = %s",
        (expected.policy_fingerprint_sha256,),
    )
    if existing:
        return _exact_fixture_replay(
            "artifact adapter policy",
            ArtifactAdapterPolicyRecord(*existing),
            expected,
        )
    connector.execute(
        "INSERT INTO catalog_artifact_adapter_policy "
        "(policy_fingerprint_sha256, adapter_id) VALUES (%s, %s)",
        (
            expected.policy_fingerprint_sha256,
            expected.adapter_id,
        ),
    )
    return load_artifact_adapter_policy(
        connector,
        expected.policy_fingerprint_sha256,
    )


def seed_artifact_policy_semantics(
    connector: SQLConnector,
    *,
    policy_fingerprint_sha256: bytes,
    adapter_id: bytes = b"test-artifact-adapter",
    artifact_algorithm_version: int = 2,
) -> ArtifactPolicySemanticsRecord:
    adapter = seed_artifact_adapter_policy(
        connector,
        policy_fingerprint_sha256=policy_fingerprint_sha256,
        adapter_id=adapter_id,
    )
    policy_digest = identity.artifact_policy_digest(
        artifact_algorithm_version,
        adapter.adapter_id,
        adapter.policy_fingerprint_sha256,
    )
    existing = connector.fetch_one(
        "SELECT semantics.policy_component_sha256, "
        "semantics.artifact_algorithm_version, "
        "semantics.policy_fingerprint_sha256, adapter.adapter_id "
        "FROM catalog_artifact_policy_semantics AS semantics "
        "JOIN catalog_artifact_adapter_policy AS adapter "
        "ON adapter.policy_fingerprint_sha256 = "
        "semantics.policy_fingerprint_sha256 "
        "WHERE semantics.policy_component_sha256 = %s",
        (policy_digest,),
    )
    expected = ArtifactPolicySemanticsRecord(
        policy_digest,
        artifact_algorithm_version,
        policy_fingerprint_sha256,
        adapter_id,
    )
    if existing:
        return _exact_fixture_replay(
            "artifact policy semantics",
            ArtifactPolicySemanticsRecord(*existing),
            expected,
        )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics "
        "(policy_component_sha256, artifact_algorithm_version, "
        "policy_fingerprint_sha256) VALUES (%s, %s, %s)",
        (
            expected.policy_component_sha256,
            expected.artifact_algorithm_version,
            expected.policy_fingerprint_sha256,
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
