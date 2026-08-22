from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from h2hdb import catalog_refinement, vnext_identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector


def _generated_catalog_database(path: Path) -> SQLiteConnector:
    connector = SQLiteConnector(str(path))
    connector.connect()
    payload: Any = ARTIFACT["backends"]
    payload = payload["sqlite"]
    for _slice_id, statements in payload["slices"]:
        for _statement_id, _kind, _name, sql in statements:
            connector.execute(sql)
    for seed in payload["bootstrap_seeds"]:
        if seed["seed_id"].startswith("catalog."):
            connector.execute(seed["sql"], seed["parameters"])
    return connector


def _insert_artifact_producer(
    connector: SQLiteConnector,
    *,
    fingerprint: bytes,
    algorithm_version: int,
    equivalence_class: bytes,
    fields: tuple[bytes, bytes, bytes, bytes, bytes],
) -> None:
    connector.execute(
        "INSERT INTO catalog_artifact_producer_fingerprint_anchors "
        "(producer_fingerprint_sha256) VALUES (%s)",
        (fingerprint,),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_producer_fingerprint_algorithm_versions "
        "(producer_fingerprint_sha256, artifact_algorithm_version) VALUES (%s, %s)",
        (fingerprint, algorithm_version),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_producer_fingerprint_equivalence_classes "
        "(producer_fingerprint_sha256, producer_equivalence_class) VALUES (%s, %s)",
        (fingerprint, equivalence_class),
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


def _insert_manifest_policy(connector: SQLiteConnector, policy_id: int) -> None:
    connector.execute(
        "INSERT INTO catalog_manifest_policy_anchors VALUES (%s)", (policy_id,)
    )
    connector.execute(
        "INSERT INTO catalog_manifest_policy_manifest_algorithm_versions "
        "VALUES (%s, 1)",
        (policy_id,),
    )
    connector.execute(
        "INSERT INTO catalog_manifest_policy_file_order_versions VALUES (%s, 1)",
        (policy_id,),
    )
    connector.execute(
        "INSERT INTO catalog_manifest_policy_identities VALUES (1, 1, %s)",
        (policy_id,),
    )
    connector.execute(
        "INSERT INTO catalog_manifest_policy_seals VALUES (%s)", (policy_id,)
    )


def _insert_source_scope(
    connector: SQLiteConnector,
    *,
    scope_key: bytes,
    source_root: bytes,
) -> None:
    connector.execute(
        "INSERT INTO catalog_source_scope_anchors VALUES (%s)", (scope_key,)
    )
    connector.execute(
        "INSERT INTO catalog_source_scope_source_providers VALUES (%s, %s)",
        (scope_key, b"filesystem"),
    )
    connector.execute(
        "INSERT INTO catalog_source_scope_source_root_sha256s VALUES (%s, %s)",
        (scope_key, source_root),
    )
    connector.execute(
        "INSERT INTO catalog_source_scope_identity_policy_versions VALUES (%s, 1)",
        (scope_key,),
    )
    connector.execute(
        "INSERT INTO catalog_source_scope_identities VALUES (%s, %s, 1, %s)",
        (b"filesystem", source_root, scope_key),
    )
    connector.execute(
        "INSERT INTO catalog_source_scope_seals VALUES (%s)", (scope_key,)
    )


def _insert_analysis_policy(
    connector: SQLiteConnector,
    policy_id: int,
    *,
    algorithm_version: int = 1,
) -> None:
    connector.execute(
        "INSERT INTO catalog_analysis_policy_anchors VALUES (%s)", (policy_id,)
    )
    facts = (
        ("algorithm_versions", "algorithm_version", algorithm_version),
        ("spam_artist_thresholds", "spam_artist_threshold", 1),
        ("spam_occurrence_thresholds", "spam_occurrence_threshold", 1),
        ("content_owner_rule_versions", "content_owner_rule_version", 1),
        ("gid_winner_rule_versions", "gid_winner_rule_version", 1),
    )
    for table_suffix, _attribute, value in facts:
        connector.execute(
            f"INSERT INTO catalog_analysis_policy_{table_suffix} VALUES (%s, %s)",
            (policy_id, value),
        )
    connector.execute(
        "INSERT INTO catalog_analysis_policy_identities VALUES (%s, 1, 1, 1, 1, %s)",
        (algorithm_version, policy_id),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_policy_seals VALUES (%s)", (policy_id,)
    )


def _insert_artifact_policy_semantics(
    connector: SQLiteConnector,
    *,
    policy_component: bytes,
    producer_fingerprint: bytes,
) -> None:
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics_anchors VALUES (%s)",
        (policy_component,),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics_artifact_algorithm_versions "
        "VALUES (%s, 1)",
        (policy_component,),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics_max_image_short_sides "
        "VALUES (%s, 2048)",
        (policy_component,),
    )
    connector.execute(
        "INSERT INTO "
        "catalog_artifact_policy_semantics_producer_fingerprint_sha256s "
        "VALUES (%s, %s)",
        (policy_component, producer_fingerprint),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics_identities "
        "VALUES (1, 2048, %s, %s)",
        (producer_fingerprint, policy_component),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics_seals VALUES (%s)",
        (policy_component,),
    )


def _insert_title_policies(
    connector: SQLiteConnector,
    *,
    display_policy_id: int,
) -> None:
    unicode_version = catalog_refinement._RUNTIME_UNICODE_DATA_VERSION
    connector.execute("INSERT INTO catalog_title_sort_policy_anchors VALUES (1)")
    connector.execute(
        "INSERT INTO catalog_title_sort_policy_algorithm_versions VALUES (1, 1)"
    )
    connector.execute(
        "INSERT INTO catalog_title_sort_policy_unicode_data_versions VALUES (1, %s)",
        (unicode_version,),
    )
    connector.execute(
        "INSERT INTO catalog_title_sort_policy_identities VALUES (1, %s, 1)",
        (unicode_version,),
    )
    connector.execute("INSERT INTO catalog_title_sort_policy_seals VALUES (1)")
    connector.execute(
        "INSERT INTO catalog_display_title_policy_anchors VALUES (%s)",
        (display_policy_id,),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policy_algorithm_versions VALUES (%s, 1)",
        (display_policy_id,),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policy_title_sort_policy_ids VALUES (%s, 1)",
        (display_policy_id,),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policy_identities VALUES (1, 1, %s)",
        (display_policy_id,),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policy_seals VALUES (%s)",
        (display_policy_id,),
    )


class _ReadRecorder:
    def __init__(self, connector: SQLiteConnector) -> None:
        self.connector = connector
        self.reads: list[tuple[str, tuple[Any, ...], int]] = []

    @property
    def queries(self) -> list[str]:
        return [query for query, _data, _row_count in self.reads]

    def fetch_all(
        self, query: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        rows = self.connector.fetch_all(query, data)
        self.reads.append((query, data, len(rows)))
        return rows


def test_empty_generated_catalog_passes_every_bounded_validator(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "catalog-ready.sqlite3")
    recorder = _ReadRecorder(connector)
    try:
        catalog_refinement.check_bootstrap_v1(recorder)  # type: ignore[arg-type]
        for validator in catalog_refinement.builtin_semantic_validators().values():
            validator(recorder)  # type: ignore[arg-type]
    finally:
        connector.close()

    assert recorder.queries
    assert all(
        query.lstrip().upper().startswith("SELECT") for query in recorder.queries
    )
    unbounded = tuple(
        query for query in recorder.queries if " LIMIT " not in f" {query.upper()} "
    )
    assert len(unbounded) == 5
    assert all("catalog_publication_" in query for query in unbounded)
    assert not any("COUNT(" in query.upper() for query in recorder.queries)
    assert not any("FOREIGN_KEY_CHECK" in query.upper() for query in recorder.queries)


def test_closed_digest_registry_rejects_an_unregistered_row(tmp_path: Path) -> None:
    connector = _generated_catalog_database(tmp_path / "catalog-registry.sqlite3")
    try:
        connector.execute(
            "INSERT INTO catalog_canonical_digest_policies (digest_domain) VALUES (%s)",
            (b"forged_domain_v1",),
        )
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="exact closed digest-domain registry",
        ):
            catalog_refinement.check_canonical_reference_domains_v1(connector)
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("mutation_sql", "error_match"),
    (
        (
            "UPDATE catalog_artifact_zip_writer_policy_compression_levels "
            "SET compression_level = 8",
            "artifact_zip_writer_policy.*exact v1 singleton",
        ),
        (
            "UPDATE catalog_artifact_storage_codec_locator_codec_versions "
            "SET locator_codec_version = 2",
            "artifact_storage_codec.*managed-filesystem v1 singleton",
        ),
    ),
)
def test_bootstrap_rejects_artifact_registry_corruption(
    tmp_path: Path,
    mutation_sql: str,
    error_match: str,
) -> None:
    connector = _generated_catalog_database(
        tmp_path / f"registry-{sha256(mutation_sql.encode()).hexdigest()}.sqlite3"
    )
    try:
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(mutation_sql)
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match=error_match,
        ):
            catalog_refinement.check_bootstrap_v1(connector)
    finally:
        connector.close()


def test_static_vertical_seed_fanout_rejects_missing_extra_order_and_partial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = cast(tuple[dict[str, Any], ...], ARTIFACT["bootstrap_seeds"])
    zip_indexes = tuple(
        index
        for index, seed in enumerate(original)
        if str(seed["relation"]).startswith("artifact_zip_writer_policy_")
    )
    assert len(zip_indexes) == 13
    first, second = zip_indexes[:2]
    identity_index = next(
        index
        for index in zip_indexes
        if original[index]["relation"] == "artifact_zip_writer_policy_identity"
    )
    partial_identity = dict(original[identity_index])
    partial_identity["value"] = partial_identity["value"][:-1]
    extra_seed = dict(original[first])
    extra_seed["id"] = f"{extra_seed['id']}.duplicate"
    mutations = (
        original[:first] + original[first + 1 :],
        original[: second + 1] + (extra_seed,) + original[second + 1 :],
        original[:first] + (original[second], original[first]) + original[second + 1 :],
        original[:identity_index]
        + (partial_identity,)
        + original[identity_index + 1 :],
    )

    for mutated in mutations:
        monkeypatch.setitem(ARTIFACT, "bootstrap_seeds", mutated)
        with pytest.raises(
            catalog_refinement.BuiltinSemanticRegistryError,
            match="vertical policy seed fanout differs",
        ):
            catalog_refinement._validate_static_catalog_contract()
        monkeypatch.setitem(ARTIFACT, "bootstrap_seeds", original)


def test_building_bootstrap_rejects_a_business_row(tmp_path: Path) -> None:
    connector = _generated_catalog_database(tmp_path / "catalog-bootstrap.sqlite3")
    try:
        connector.execute("INSERT INTO catalog_revision_anchors (revision) VALUES (1)")
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="catalog_revision.*not empty",
        ):
            catalog_refinement.check_bootstrap_v1(connector)
    finally:
        connector.close()


def _insert_canonical_seal(
    connector: SQLiteConnector,
    *,
    value_sha256: bytes,
    digest_domain: bytes,
    page_sha256: bytes,
) -> None:
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocation_anchors "
        "(value_sha256) VALUES (%s)",
        (value_sha256,),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocation_digest_domains "
        "(value_sha256, digest_domain) VALUES (%s, %s)",
        (value_sha256, digest_domain),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocation_byte_counts "
        "(value_sha256, byte_count) VALUES (%s, 0)",
        (value_sha256,),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocation_allocated_ats "
        "(value_sha256, allocated_at) VALUES (%s, 0)",
        (value_sha256,),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocation_seals "
        "(value_sha256) VALUES (%s)",
        (value_sha256,),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_page_anchors (page_sha256) VALUES (%s)",
        (page_sha256,),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_page_payloads "
        "(page_sha256, page_bytes) VALUES (%s, %s)",
        (page_sha256, b"x"),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_page_coordinates "
        "(value_sha256, level, page_position, page_sha256) "
        "VALUES (%s, 0, 0, %s)",
        (value_sha256, page_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_page_subtree_item_counts "
        "(page_sha256, subtree_item_count) VALUES (%s, 0)",
        (page_sha256,),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_page_seals (page_sha256) VALUES (%s)",
        (page_sha256,),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_identities "
        "(value_sha256, root_page_sha256) VALUES (%s, %s)",
        (value_sha256, page_sha256),
    )


def _insert_analysis_seals(
    connector: SQLiteConnector,
    analysis_id: bytes,
) -> None:
    for component in sorted(catalog_refinement._EXPECTED_ANALYSIS_COMPONENTS):
        component_bytes = component.encode("ascii")
        connector.execute(
            "INSERT INTO catalog_analysis_state_component_anchors "
            "(analysis_id, state_component) VALUES (%s, %s)",
            (analysis_id, component_bytes),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_state_component_row_counts "
            "(analysis_id, state_component, row_count) VALUES (%s, %s, 0)",
            (analysis_id, component_bytes),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_state_component_sealed_ats "
            "(analysis_id, state_component, sealed_at) VALUES (%s, %s, 1)",
            (analysis_id, component_bytes),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_state_component_completion_seals "
            "(analysis_id, state_component) VALUES (%s, %s)",
            (analysis_id, component_bytes),
        )


def _insert_analysis_run(
    connector: SQLiteConnector,
    *,
    analysis_id: bytes,
    build_id: bytes,
    input_manifest_sha256: bytes,
    policy_id: int = 1,
    state: str = "COMPLETE",
    started_at: int = 0,
    completed_at: int | None = 1,
) -> None:
    connector.execute(
        "INSERT INTO catalog_analysis_run_anchors (analysis_id) VALUES (%s)",
        (analysis_id,),
    )
    for table, column, value in (
        ("catalog_analysis_run_build_ids", "build_id", build_id),
        ("catalog_analysis_run_policy_ids", "policy_id", policy_id),
        (
            "catalog_analysis_run_input_manifest_sha256s",
            "input_manifest_sha256",
            input_manifest_sha256,
        ),
        ("catalog_analysis_run_started_ats", "started_at", started_at),
        ("catalog_analysis_run_states", "state", state),
    ):
        connector.execute(
            f"INSERT INTO {table} (analysis_id, {column}) VALUES (%s, %s)",
            (analysis_id, value),
        )
    connector.execute(
        "INSERT INTO catalog_analysis_run_identities "
        "(build_id, policy_id, analysis_id) VALUES (%s, %s, %s)",
        (build_id, policy_id, analysis_id),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_run_descriptor_seals (analysis_id) VALUES (%s)",
        (analysis_id,),
    )
    if completed_at is not None:
        connector.execute(
            "INSERT INTO catalog_analysis_run_completed_ats "
            "(analysis_id, completed_at) VALUES (%s, %s)",
            (analysis_id, completed_at),
        )


def _insert_sealed_source_build(
    connector: SQLiteConnector,
    *,
    build_id: bytes,
    scope_key: bytes,
    created_at: int = 0,
    sealed_at: int = 1,
) -> None:
    connector.execute(
        "INSERT INTO catalog_source_build_anchors (build_id) VALUES (%s)",
        (build_id,),
    )
    for table, column, value in (
        ("catalog_source_build_scope_keys", "scope_key", scope_key),
        (
            "catalog_source_build_manifest_policy_ids",
            "manifest_policy_id",
            1,
        ),
        ("catalog_source_build_states", "state", "SEALED"),
        ("catalog_source_build_created_ats", "created_at", created_at),
    ):
        connector.execute(
            f"INSERT INTO {table} (build_id, {column}) VALUES (%s, %s)",
            (build_id, value),
        )
    connector.execute(
        "INSERT INTO catalog_source_build_descriptor_seals (build_id) VALUES (%s)",
        (build_id,),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_sealed_ats (build_id, sealed_at) "
        "VALUES (%s, %s)",
        (build_id, sealed_at),
    )


def _insert_build_manifest(
    connector: SQLiteConnector,
    *,
    build_id: bytes,
    manifest_sha256: bytes,
    gallery_count: int = 0,
    file_count: int = 0,
    byte_count: int = 0,
    computed_at: int = 1,
) -> None:
    connector.execute(
        "INSERT INTO catalog_source_build_discovery_anchors (build_id) VALUES (%s)",
        (build_id,),
    )
    for table, column, value in (
        (
            "catalog_source_build_discovery_scan_attempts",
            "scan_attempt",
            sha256(build_id + b"scan").digest()[:16],
        ),
        (
            "catalog_source_build_discovery_gallery_counts",
            "gallery_count",
            gallery_count,
        ),
        (
            "catalog_source_build_discovery_tree_observation_sha256s",
            "tree_observation_sha256",
            sha256(build_id + b"tree").digest(),
        ),
        (
            "catalog_source_build_discovery_completed_ats",
            "completed_at",
            computed_at,
        ),
    ):
        connector.execute(
            f"INSERT INTO {table} (build_id, {column}) VALUES (%s, %s)",
            (build_id, value),
        )
    connector.execute(
        "INSERT INTO catalog_source_build_discovery_seals (build_id) VALUES (%s)",
        (build_id,),
    )

    connector.execute(
        "INSERT INTO catalog_build_manifest_anchors (build_id) VALUES (%s)",
        (build_id,),
    )
    for table, column, value in (
        (
            "catalog_build_manifest_manifest_sha256s",
            "manifest_sha256",
            manifest_sha256,
        ),
        ("catalog_build_manifest_file_counts", "file_count", file_count),
        ("catalog_build_manifest_byte_counts", "byte_count", byte_count),
    ):
        connector.execute(
            f"INSERT INTO {table} (build_id, {column}) VALUES (%s, %s)",
            (build_id, value),
        )
    connector.execute(
        "INSERT INTO catalog_build_manifest_seals (build_id) VALUES (%s)",
        (build_id,),
    )


def _insert_snapshot_manifest_identity(
    connector: SQLiteConnector,
    *,
    snapshot_manifest_sha256: bytes,
    gallery_count: int = 0,
    file_count: int = 0,
    byte_count: int = 0,
) -> None:
    connector.execute(
        "INSERT INTO catalog_source_snapshot_manifest_identity_anchors "
        "(snapshot_manifest_sha256) VALUES (%s)",
        (snapshot_manifest_sha256,),
    )
    for table, column, value in (
        (
            "catalog_source_snapshot_manifest_identity_gallery_counts",
            "gallery_count",
            gallery_count,
        ),
        (
            "catalog_source_snapshot_manifest_identity_file_counts",
            "file_count",
            file_count,
        ),
        (
            "catalog_source_snapshot_manifest_identity_byte_counts",
            "byte_count",
            byte_count,
        ),
    ):
        connector.execute(
            f"INSERT INTO {table} (snapshot_manifest_sha256, {column}) VALUES (%s, %s)",
            (snapshot_manifest_sha256, value),
        )
    connector.execute(
        "INSERT INTO catalog_source_snapshot_manifest_identity_seals "
        "(snapshot_manifest_sha256) VALUES (%s)",
        (snapshot_manifest_sha256,),
    )


def _insert_active_source_head(connector: SQLiteConnector) -> bytes:
    source_root = b"r" * 32
    snapshot_manifest = b"s" * 32
    _insert_canonical_seal(
        connector,
        value_sha256=source_root,
        digest_domain=b"source_root_v1",
        page_sha256=b"R" * 32,
    )
    _insert_canonical_seal(
        connector,
        value_sha256=snapshot_manifest,
        digest_domain=b"source_snapshot_manifest_v1",
        page_sha256=b"S" * 32,
    )
    scope_key = vnext_identity.source_scope_key("filesystem", source_root, 1)
    build_id = b"b" * 16
    analysis_id = b"a" * 16
    _insert_manifest_policy(connector, 1)
    _insert_source_scope(
        connector,
        scope_key=scope_key,
        source_root=source_root,
    )
    _insert_sealed_source_build(
        connector,
        build_id=build_id,
        scope_key=scope_key,
    )
    connector.execute(
        "INSERT INTO catalog_source_build_channel VALUES (%s, %s)",
        (build_id, b"default"),
    )
    _insert_build_manifest(
        connector,
        build_id=build_id,
        manifest_sha256=b"m" * 32,
    )
    _insert_analysis_policy(connector, 1)
    _insert_analysis_run(
        connector,
        analysis_id=analysis_id,
        build_id=build_id,
        input_manifest_sha256=b"i" * 32,
    )
    connector.execute(
        "INSERT INTO catalog_analysis_state_ancestry VALUES (%s, 0, %s)",
        (analysis_id, analysis_id),
    )
    _insert_analysis_seals(connector, analysis_id)
    _insert_snapshot_manifest_identity(
        connector,
        snapshot_manifest_sha256=snapshot_manifest,
    )
    connector.execute(
        "INSERT INTO catalog_analysis_snapshot_manifest VALUES (%s, %s)",
        (analysis_id, snapshot_manifest),
    )
    connector.execute("PRAGMA foreign_keys = OFF")
    connector.execute(
        "INSERT INTO catalog_source_revision_anchors (source_revision) VALUES (1)"
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_channels "
        "(source_revision, channel) VALUES (1, %s)",
        (b"default",),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_snapshot_manifests "
        "(source_revision, snapshot_manifest_sha256) VALUES (1, %s)",
        (snapshot_manifest,),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_descriptor_seals "
        "(source_revision) VALUES (1)"
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_provenance VALUES (1, %s)",
        (analysis_id,),
    )
    connector.execute("INSERT INTO catalog_revision_anchors (revision) VALUES (1)")
    connector.execute(
        "INSERT INTO catalog_revision_publication_counts "
        "(revision, publication_count) VALUES (1, 0)"
    )
    connector.execute(
        "INSERT INTO catalog_revision_descriptor_seals (revision) VALUES (1)"
    )
    connector.execute(
        "INSERT INTO catalog_publication_generation_nodes (generation) VALUES (1)"
    )
    connector.execute(
        "INSERT INTO catalog_publication_generation_successors "
        "(successor_generation, predecessor_generation) VALUES (1, 0)"
    )
    receipt_id = b"t" * 16
    connector.execute(
        "INSERT INTO catalog_publication_commit_anchors (receipt_id) VALUES (%s)",
        (receipt_id,),
    )
    for table, column, value in (
        ("catalog_publication_commit_candidates", "candidate_id", b"c" * 16),
        ("catalog_publication_commit_catalog_revisions", "revision", 1),
        ("catalog_publication_commit_source_revisions", "source_revision", 1),
        ("catalog_publication_commit_generations", "generation", 1),
        (
            "catalog_publication_commit_operational_preparations",
            "preparation_id",
            b"p" * 16,
        ),
        (
            "catalog_publication_commit_operational_policies",
            "operational_policy_id",
            1,
        ),
        ("catalog_publication_commit_artifact_policies", "artifact_policy_id", 1),
        (
            "catalog_publication_commit_display_title_policies",
            "display_title_policy_id",
            1,
        ),
        ("catalog_publication_commit_new_galleries", "new_galleries", 0),
        ("catalog_publication_commit_changed_galleries", "changed_galleries", 0),
        ("catalog_publication_commit_removed_galleries", "removed_galleries", 0),
        ("catalog_publication_commit_duplicate_losers", "duplicate_losers", 0),
        ("catalog_publication_commit_committed_ats", "committed_at", 1),
    ):
        connector.execute(
            f"INSERT INTO {table} (receipt_id, {column}) VALUES (%s, %s)",
            (receipt_id, value),
        )
    connector.execute(
        "INSERT INTO catalog_publication_commit_seals (receipt_id) VALUES (%s)",
        (receipt_id,),
    )
    connector.execute(
        "INSERT INTO catalog_publication_commit_head_receipts "
        "(channel, receipt_id) VALUES (%s, %s)",
        (b"default", receipt_id),
    )
    connector.execute("PRAGMA foreign_keys = ON")
    return analysis_id


_IMPACTED_FAMILY_FIXTURES = (
    (
        "content",
        "content_sha256",
        b"r" * 32,
        "catalog_a_impacted_content_anchors",
        "catalog_a_impacted_content_provenance",
        "catalog_a_impacted_content_witnesses",
        "catalog_a_impacted_content_seals",
        "ix_a_impacted_content_key_gallery",
    ),
    (
        "gid",
        "gid",
        17,
        "catalog_a_impacted_gid_anchors",
        "catalog_a_impacted_gid_provenance",
        "catalog_a_impacted_gid_witnesses",
        "catalog_a_impacted_gid_seals",
        "ix_a_impacted_gid_key_gallery",
    ),
)


def _insert_impacted_gallery_workset(
    connector: SQLiteConnector,
    analysis_id: bytes,
) -> None:
    scope_key = vnext_identity.source_scope_key("filesystem", b"r" * 32, 1)
    for gallery_id in (1, 2):
        locator_sha256 = bytes((gallery_id,)) * 32
        _insert_canonical_seal(
            connector,
            value_sha256=locator_sha256,
            digest_domain=b"source_relative_locator_v1",
            page_sha256=bytes((gallery_id + 4,)) * 32,
        )
        connector.execute(
            "INSERT INTO catalog_source_locator_identity "
            "(locator_sha256, source_gallery_name) VALUES (%s, %s)",
            (locator_sha256, f"gallery-{gallery_id}".encode("ascii")),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_identity_anchors (gallery_id) VALUES (%s)",
            (gallery_id,),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_identity_coordinates "
            "(scope_key, locator_sha256, gallery_id) VALUES (%s, %s, %s)",
            (scope_key, locator_sha256, gallery_id),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_identity_gallery_keys "
            "(gallery_id, gallery_key) VALUES (%s, %s)",
            (gallery_id, bytes((gallery_id + 2,)) * 32),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_identity_seals (gallery_id) VALUES (%s)",
            (gallery_id,),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_impacted_galleries "
            "(analysis_id, gallery_id) VALUES (%s, %s)",
            (analysis_id, gallery_id),
        )


def _insert_impacted_key_family(
    connector: SQLiteConnector,
    analysis_id: bytes,
    fixture: tuple[str, str, bytes | int, str, str, str, str, str],
    *,
    witness_gallery_id: int = 1,
) -> None:
    (
        _family,
        key_column,
        key_value,
        anchor_table,
        provenance_table,
        witness_table,
        seal_table,
        _lookup_index,
    ) = fixture
    connector.execute(
        f"INSERT INTO {anchor_table} (analysis_id, {key_column}) VALUES (%s, %s)",
        (analysis_id, key_value),
    )
    for gallery_id in (1, 2):
        connector.execute(
            f"INSERT INTO {provenance_table} "
            f"(analysis_id, gallery_id, {key_column}) VALUES (%s, %s, %s)",
            (analysis_id, gallery_id, key_value),
        )
    connector.execute(
        f"INSERT INTO {witness_table} "
        f"(analysis_id, {key_column}, witness_gallery_id) VALUES (%s, %s, %s)",
        (analysis_id, key_value, witness_gallery_id),
    )
    connector.execute(
        f"INSERT INTO {seal_table} (analysis_id, {key_column}) VALUES (%s, %s)",
        (analysis_id, key_value),
    )


def _insert_complete_analysis(
    connector: SQLiteConnector,
    *,
    analysis_id: bytes,
    build_id: bytes,
) -> None:
    scope_key = vnext_identity.source_scope_key("filesystem", b"r" * 32, 1)
    _insert_sealed_source_build(
        connector,
        build_id=build_id,
        scope_key=scope_key,
    )
    connector.execute(
        "INSERT INTO catalog_source_build_channel (build_id, channel) VALUES (%s, %s)",
        (build_id, b"default"),
    )
    _insert_build_manifest(
        connector,
        build_id=build_id,
        manifest_sha256=sha256(build_id).digest(),
    )
    _insert_analysis_run(
        connector,
        analysis_id=analysis_id,
        build_id=build_id,
        input_manifest_sha256=sha256(analysis_id).digest(),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_state_ancestry "
        "(analysis_id, ancestor_depth, ancestor_analysis_id) VALUES (%s, 0, %s)",
        (analysis_id, analysis_id),
    )
    _insert_analysis_seals(connector, analysis_id)


def _replace_active_analysis_chain(
    connector: SQLiteConnector,
    root_analysis_id: bytes,
    *,
    overlay_depth: int,
) -> tuple[bytes, ...]:
    ancestors = [root_analysis_id]
    for offset in range(1, overlay_depth + 1):
        analysis_id = b"p" + offset.to_bytes(15, "big")
        build_id = b"q" + offset.to_bytes(15, "big")
        _insert_complete_analysis(
            connector,
            analysis_id=analysis_id,
            build_id=build_id,
        )
        ancestors.append(analysis_id)

    for analysis_id in ancestors:
        connector.execute(
            "DELETE FROM catalog_analysis_state_ancestry WHERE analysis_id = %s",
            (analysis_id,),
        )
        connector.execute(
            "DELETE FROM catalog_analysis_baselines WHERE analysis_id = %s",
            (analysis_id,),
        )
    for offset, analysis_id in enumerate(ancestors):
        suffix = ancestors[offset:]
        depth = len(suffix) - 1
        for ancestor_depth, ancestor_id in enumerate(suffix):
            connector.execute(
                "INSERT INTO catalog_analysis_state_ancestry "
                "(analysis_id, ancestor_depth, ancestor_analysis_id) "
                "VALUES (%s, %s, %s)",
                (analysis_id, ancestor_depth, ancestor_id),
            )
        if depth:
            connector.execute(
                "INSERT INTO catalog_analysis_baselines "
                "(analysis_id, base_analysis_id) VALUES (%s, %s)",
                (analysis_id, suffix[1]),
            )
    return tuple(ancestors)


_PUBLICATION_VALIDATION_STAGES = (
    b"VALIDATE_SELECTION",
    b"VALIDATE_CATALOG_PROJECTION",
    b"VALIDATE_ARTIFACT_INPUT_DELTA",
    b"VALIDATE_PREPARED_ARTIFACT",
    b"VALIDATE_CREATE",
    b"VALIDATE_REBUILD",
    b"VALIDATE_DELETE",
    b"VALIDATE_UNCHANGED",
    b"VALIDATE_NEW_GALLERY",
    b"VALIDATE_CHANGED_GALLERY",
    b"VALIDATE_REMOVED_GALLERY",
    b"VALIDATE_DUPLICATE_LOSER",
)


def _insert_publication_terminal_stage(
    connector: SQLiteConnector,
    *,
    candidate_id: bytes,
    stage: bytes,
    processed_count: int = 0,
    committed_at: int = 1,
) -> None:
    checkpoint_key = (candidate_id, stage)
    connector.execute(
        "INSERT INTO catalog_publication_checkpoint_anchors "
        "(candidate_id, stage) VALUES (%s, %s)",
        checkpoint_key,
    )
    for table, column, value in (
        ("catalog_publication_checkpoint_generations", "generation", 2),
        ("catalog_publication_checkpoint_cursors", "cursor", b""),
        (
            "catalog_publication_checkpoint_processed_counts",
            "processed_count",
            processed_count,
        ),
        ("catalog_publication_checkpoint_states", "state", "COMPLETE"),
        ("catalog_publication_checkpoint_updated_ats", "updated_at", committed_at),
    ):
        connector.execute(
            f"INSERT INTO {table} (candidate_id, stage, {column}) "
            "VALUES (%s, %s, %s)",
            (*checkpoint_key, value),
        )
    connector.execute(
        "INSERT INTO catalog_publication_checkpoint_seals "
        "(candidate_id, stage) VALUES (%s, %s)",
        checkpoint_key,
    )

    receipt_key = (candidate_id, stage, 1)
    connector.execute(
        "INSERT INTO catalog_publication_batch_receipt_anchors "
        "(candidate_id, stage, start_generation) VALUES (%s, %s, %s)",
        receipt_key,
    )
    connector.execute(
        "INSERT INTO catalog_publication_batch_receipt_coordinates "
        "(candidate_id, stage, batch_key, start_generation) "
        "VALUES (%s, %s, %s, %s)",
        (candidate_id, stage, b"terminal", 1),
    )
    for table, column, value in (
        ("catalog_publication_batch_receipt_start_cursors", "start_cursor", b""),
        (
            "catalog_publication_batch_receipt_start_processed_counts",
            "start_processed_count",
            processed_count,
        ),
        ("catalog_publication_batch_receipt_next_cursors", "next_cursor", b""),
        ("catalog_publication_batch_receipt_row_counts", "row_count", 0),
        (
            "catalog_publication_batch_receipt_committed_ats",
            "committed_at",
            committed_at,
        ),
    ):
        connector.execute(
            f"INSERT INTO {table} "
            f"(candidate_id, stage, start_generation, {column}) "
            "VALUES (%s, %s, %s, %s)",
            (*receipt_key, value),
        )
    connector.execute(
        "INSERT INTO catalog_publication_batch_receipt_seals "
        "(candidate_id, stage, start_generation) VALUES (%s, %s, %s)",
        receipt_key,
    )


def _insert_open_publication_finalization_checkpoint(
    connector: SQLiteConnector,
    *,
    receipt_id: bytes,
) -> None:
    connector.execute(
        "INSERT INTO catalog_publication_finalization_checkpoint_anchors "
        "(receipt_id) VALUES (%s)",
        (receipt_id,),
    )
    for table, column, value in (
        ("catalog_publication_finalization_checkpoint_generations", "generation", 1),
        ("catalog_publication_finalization_checkpoint_cursors", "cursor", b""),
        ("catalog_publication_finalization_checkpoint_counts", "processed_count", 0),
        ("catalog_publication_finalization_checkpoint_states", "state", "OPEN"),
        ("catalog_publication_finalization_checkpoint_updated_ats", "updated_at", 1),
    ):
        connector.execute(
            f"INSERT INTO {table} (receipt_id, {column}) VALUES (%s, %s)",
            (receipt_id, value),
        )
    connector.execute(
        "INSERT INTO catalog_publication_finalization_checkpoint_seals "
        "(receipt_id) VALUES (%s)",
        (receipt_id,),
    )


def _complete_publication_finalization(
    connector: SQLiteConnector,
    *,
    receipt_id: bytes,
    committed_at: int = 2,
) -> None:
    receipt_key = (receipt_id, 1)
    connector.execute(
        "INSERT INTO catalog_publication_finalization_batch_anchors "
        "(receipt_id, start_generation) VALUES (%s, %s)",
        receipt_key,
    )
    connector.execute(
        "INSERT INTO catalog_publication_finalization_batch_coordinates "
        "(receipt_id, batch_key, start_generation) VALUES (%s, %s, %s)",
        (receipt_id, b"terminal", 1),
    )
    for table, column, value in (
        ("catalog_publication_finalization_batch_start_cursors", "start_cursor", b""),
        (
            "catalog_publication_finalization_batch_start_counts",
            "start_processed_count",
            0,
        ),
        ("catalog_publication_finalization_batch_next_cursors", "next_cursor", b""),
        ("catalog_publication_finalization_batch_row_counts", "row_count", 0),
        (
            "catalog_publication_finalization_batch_committed_ats",
            "committed_at",
            committed_at,
        ),
    ):
        connector.execute(
            f"INSERT INTO {table} (receipt_id, start_generation, {column}) "
            "VALUES (%s, %s, %s)",
            (*receipt_key, value),
        )
    connector.execute(
        "INSERT INTO catalog_publication_finalization_batch_seals "
        "(receipt_id, start_generation) VALUES (%s, %s)",
        receipt_key,
    )
    connector.execute(
        "UPDATE catalog_publication_finalization_checkpoint_generations "
        "SET generation = 2 WHERE receipt_id = %s",
        (receipt_id,),
    )
    connector.execute(
        "UPDATE catalog_publication_finalization_checkpoint_states "
        "SET state = 'COMPLETE' WHERE receipt_id = %s",
        (receipt_id,),
    )
    connector.execute(
        "UPDATE catalog_publication_finalization_checkpoint_updated_ats "
        "SET updated_at = %s WHERE receipt_id = %s",
        (committed_at, receipt_id),
    )
    connector.execute(
        "INSERT INTO catalog_publication_commit_finalizations (receipt_id) "
        "VALUES (%s)",
        (receipt_id,),
    )


def _insert_active_publication(
    connector: SQLiteConnector,
    analysis_id: bytes,
) -> tuple[bytes, bytes]:
    artifact_policy_id = 1
    display_title_policy_id = 1
    producer_values = (b"writer", b"cp314", b"pillow", b"libjpeg", b"zlib")
    producer_fingerprint = vnext_identity.artifact_producer_fingerprint_sha256(
        *producer_values
    )
    policy_component = vnext_identity.artifact_policy_digest(
        1,
        2048,
        producer_fingerprint,
    )
    _insert_canonical_seal(
        connector,
        value_sha256=policy_component,
        digest_domain=b"artifact_policy_v2",
        page_sha256=sha256(b"artifact-policy-page").digest(),
    )
    _insert_artifact_producer(
        connector,
        fingerprint=producer_fingerprint,
        algorithm_version=1,
        equivalence_class=vnext_identity.artifact_producer_equivalence_class(
            producer_fingerprint
        ),
        fields=producer_values,
    )
    _insert_artifact_policy_semantics(
        connector,
        policy_component=policy_component,
        producer_fingerprint=producer_fingerprint,
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policies "
        "(artifact_policy_id, policy_component_sha256) VALUES (%s, %s)",
        (artifact_policy_id, policy_component),
    )
    _insert_title_policies(connector, display_policy_id=display_title_policy_id)

    candidate_id = b"c" * 16
    connector.execute(
        "INSERT INTO catalog_publication_candidate_anchors (candidate_id) VALUES (%s)",
        (candidate_id,),
    )
    for table, column, value in (
        ("catalog_publication_candidate_analysis_ids", "analysis_id", analysis_id),
        ("catalog_publication_candidate_reserved_revisions", "reserved_revision", 1),
        (
            "catalog_publication_candidate_artifact_policy_ids",
            "artifact_policy_id",
            artifact_policy_id,
        ),
        (
            "catalog_publication_candidate_display_title_policy_ids",
            "display_title_policy_id",
            display_title_policy_id,
        ),
        (
            "catalog_publication_candidate_artifacts_required",
            "artifacts_required",
            0,
        ),
        ("catalog_publication_candidate_created_ats", "created_at", 0),
    ):
        connector.execute(
            f"INSERT INTO {table} (candidate_id, {column}) VALUES (%s, %s)",
            (candidate_id, value),
        )
    connector.execute(
        "INSERT INTO catalog_publication_candidate_definition_seals "
        "(candidate_id) VALUES (%s)",
        (candidate_id,),
    )
    for stage in _PUBLICATION_VALIDATION_STAGES:
        _insert_publication_terminal_stage(
            connector,
            candidate_id=candidate_id,
            stage=stage,
        )
    connector.execute(
        "INSERT INTO catalog_publication_candidate_projection_seals "
        "(candidate_id) VALUES (%s)",
        (candidate_id,),
    )
    receipt_id = b"t" * 16
    _insert_open_publication_finalization_checkpoint(
        connector,
        receipt_id=receipt_id,
    )
    return candidate_id, receipt_id


def test_active_source_head_requires_all_five_analysis_seals(tmp_path: Path) -> None:
    connector = _generated_catalog_database(tmp_path / "catalog-seals.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        catalog_refinement.check_source_baseline_channel_v1(connector)

        connector.execute(
            "DELETE FROM catalog_analysis_state_component_completion_seals "
            "WHERE analysis_id = %s AND state_component = %s",
            (analysis_id, b"gid_winner"),
        )
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="exact five immutable component seals",
        ):
            catalog_refinement.check_source_baseline_channel_v1(connector)
    finally:
        connector.close()


def test_publication_projection_requires_each_fixed_terminal_receipt(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "projection-receipts.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        candidate_id, _receipt_id = _insert_active_publication(connector, analysis_id)
        projection_query = (
            "SELECT create_count, rebuild_count, delete_count, new_galleries, "
            "changed_galleries FROM catalog_publication_candidate_projections "
            "WHERE candidate_id = %s"
        )
        assert connector.fetch_all(projection_query, (candidate_id,)) == [
            (0, 0, 0, 0, 0)
        ]

        connector.execute(
            "DELETE FROM catalog_publication_batch_receipt_seals "
            "WHERE candidate_id = %s AND stage = %s",
            (candidate_id, b"VALIDATE_DELETE"),
        )

        assert connector.fetch_all(projection_query, (candidate_id,)) == []
    finally:
        connector.close()


def test_incremental_impact_accepts_complete_minimum_witness_families_and_uses_lookup_indexes(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "impacted-key-valid.sqlite3")
    recorder = _ReadRecorder(connector)
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        _insert_impacted_gallery_workset(connector, analysis_id)
        for fixture in _IMPACTED_FAMILY_FIXTURES:
            _insert_impacted_key_family(connector, analysis_id, fixture)

        catalog_refinement.check_incremental_impact_v1(cast(Any, recorder))

        minimum_reads = tuple(
            (query, data)
            for query, data, _row_count in recorder.reads
            if "smaller.gallery_id < candidate.gallery_id" in query
        )
        assert len(minimum_reads) == 2
        assert all("MIN(" not in query.upper() for query, _data in minimum_reads)
        for fixture in _IMPACTED_FAMILY_FIXTURES:
            provenance_table = fixture[4]
            lookup_index = fixture[7]
            query, data = next(
                item for item in minimum_reads if provenance_table in item[0]
            )
            plans = connector.fetch_all(f"EXPLAIN QUERY PLAN {query}", data)
            plan_text = " ".join(str(row[3]) for row in plans).upper()
            assert lookup_index.upper() in plan_text
    finally:
        connector.close()


@pytest.mark.parametrize(
    "fixture",
    _IMPACTED_FAMILY_FIXTURES,
    ids=tuple(fixture[0] for fixture in _IMPACTED_FAMILY_FIXTURES),
)
@pytest.mark.parametrize(
    ("fault", "error_match"),
    (
        (
            "missing_provenance",
            "anchor lacks provenance, witness, or seal",
        ),
        (
            "missing_witness",
            "anchor lacks provenance, witness, or seal",
        ),
        (
            "missing_seal",
            "anchor lacks provenance, witness, or seal",
        ),
        ("orphan_provenance", "provenance has no anchor"),
        ("orphan_witness", "witness lacks its anchor or provenance"),
        ("orphan_seal", "seal lacks its anchor or witness"),
        ("nonminimum_witness", "witness is not the minimum provenance gallery"),
    ),
)
def test_incremental_impact_rejects_partial_or_nonminimum_witness_families(
    tmp_path: Path,
    fixture: tuple[str, str, bytes | int, str, str, str, str, str],
    fault: str,
    error_match: str,
) -> None:
    family, key_column, key_value, anchor, provenance, witness, seal, _index = fixture
    connector = _generated_catalog_database(
        tmp_path / f"impacted-key-{family}-{fault}.sqlite3"
    )
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        _insert_impacted_gallery_workset(connector, analysis_id)
        _insert_impacted_key_family(connector, analysis_id, fixture)

        connector.execute("PRAGMA foreign_keys = OFF")
        if fault == "missing_provenance":
            for table in (seal, witness, provenance):
                connector.execute(
                    f"DELETE FROM {table} WHERE analysis_id = %s AND {key_column} = %s",
                    (analysis_id, key_value),
                )
        elif fault == "missing_witness":
            for table in (seal, witness):
                connector.execute(
                    f"DELETE FROM {table} WHERE analysis_id = %s AND {key_column} = %s",
                    (analysis_id, key_value),
                )
        elif fault == "missing_seal":
            connector.execute(
                f"DELETE FROM {seal} WHERE analysis_id = %s AND {key_column} = %s",
                (analysis_id, key_value),
            )
        elif fault == "orphan_provenance":
            for table in (seal, witness, anchor):
                connector.execute(
                    f"DELETE FROM {table} WHERE analysis_id = %s AND {key_column} = %s",
                    (analysis_id, key_value),
                )
        elif fault == "orphan_witness":
            for table in (seal, provenance, anchor):
                connector.execute(
                    f"DELETE FROM {table} WHERE analysis_id = %s AND {key_column} = %s",
                    (analysis_id, key_value),
                )
        elif fault == "orphan_seal":
            for table in (witness, provenance, anchor):
                connector.execute(
                    f"DELETE FROM {table} WHERE analysis_id = %s AND {key_column} = %s",
                    (analysis_id, key_value),
                )
        else:
            connector.execute(
                f"UPDATE {witness} SET witness_gallery_id = 2 "
                f"WHERE analysis_id = %s AND {key_column} = %s",
                (analysis_id, key_value),
            )
        connector.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match=error_match,
        ):
            catalog_refinement.check_incremental_impact_v1(connector)
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("corruption", "error_match"),
    (
        ("missing_parent_suffix", "complete parent ancestry suffix"),
        ("wrong_parent_baseline", "not its immediate sealed parent"),
        ("deep_policy_drift", "crosses policy identities"),
        ("missing_deep_seal", "exact five immutable component seals"),
    ),
)
def test_active_analysis_rejects_corruption_anywhere_in_the_bounded_parent_chain(
    tmp_path: Path,
    corruption: str,
    error_match: str,
) -> None:
    connector = _generated_catalog_database(tmp_path / f"chain-{corruption}.sqlite3")
    try:
        root_analysis_id = _insert_active_source_head(connector)
        ancestors = _replace_active_analysis_chain(
            connector,
            root_analysis_id,
            overlay_depth=2,
        )
        catalog_refinement.check_source_baseline_channel_v1(connector)

        if corruption == "missing_parent_suffix":
            connector.execute(
                "DELETE FROM catalog_analysis_state_ancestry "
                "WHERE analysis_id = %s AND ancestor_depth = 1",
                (ancestors[1],),
            )
        elif corruption == "wrong_parent_baseline":
            connector.execute(
                "UPDATE catalog_analysis_baselines SET base_analysis_id = %s "
                "WHERE analysis_id = %s",
                (ancestors[0], ancestors[1]),
            )
        elif corruption == "deep_policy_drift":
            _insert_analysis_policy(connector, 2, algorithm_version=2)
            connector.execute("PRAGMA foreign_keys = OFF")
            connector.execute(
                "UPDATE catalog_analysis_run_policy_ids "
                "SET policy_id = 2 WHERE analysis_id = %s",
                (ancestors[2],),
            )
            connector.execute("PRAGMA foreign_keys = ON")
        else:
            connector.execute(
                "DELETE FROM catalog_analysis_state_component_completion_seals "
                "WHERE analysis_id = %s AND state_component = %s",
                (ancestors[2], b"gid_winner"),
            )

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match=error_match,
        ):
            catalog_refinement.check_source_baseline_channel_v1(connector)
    finally:
        connector.close()


def test_depth_16_analysis_validation_has_a_fixed_query_and_index_budget(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "chain-budget.sqlite3")
    recorder = _ReadRecorder(connector)
    try:
        root_analysis_id = _insert_active_source_head(connector)
        _replace_active_analysis_chain(
            connector,
            root_analysis_id,
            overlay_depth=16,
        )
        catalog_refinement.check_source_baseline_channel_v1(cast(Any, recorder))

        ancestry_reads = tuple(
            (query, data)
            for query, data, _row_count in recorder.reads
            if "FROM catalog_analysis_state_ancestry AS ancestry" in query
        )
        assert len(ancestry_reads) == 17
        for query, data in ancestry_reads:
            plans = connector.fetch_all(f"EXPLAIN QUERY PLAN {query}", data)
            plan_text = " ".join(str(row[3]) for row in plans).upper()
            assert "SEARCH ANCESTRY USING" in plan_text
        permitted_bounded_scans = {
            "CATALOG_ANALYSIS_STAGES",
            "CATALOG_CHANNEL_REGISTRY",
            "CATALOG_PUBLICATION_STAGES",
            "CATALOG_SOURCE_PROVIDER_REGISTRY",
            "CATALOG_CANONICAL_DIGEST_POLICIES",
            "CATALOG_SOURCE_HEAD_REVISIONS",
            "CATALOG_SOURCE_HEAD_ADVANCED_ATS",
            "CATALOG_ARTIFACT_PRODUCER_FINGERPRINT_SEALS",
            "CATALOG_ARTIFACT_ZIP_WRITER_POLICIES",
            "CATALOG_ARTIFACT_STORAGE_CODECS",
            "MEMBER_1",
            "REGISTRY",
            "SEALED",
            "SEAL",
        }
        for query, data, _row_count in recorder.reads:
            plans = connector.fetch_all(f"EXPLAIN QUERY PLAN {query}", data)
            for plan in plans:
                detail = str(plan[3]).upper()
                if detail.startswith("SCAN "):
                    assert any(
                        f"SCAN {table}" in detail for table in permitted_bounded_scans
                    ), detail
    finally:
        connector.close()

    assert len(recorder.reads) <= 96
    assert max(row_count for _query, _data, row_count in recorder.reads) <= 23
    assert sum(row_count for _query, _data, row_count in recorder.reads) <= 343
    assert all(" LIMIT " in f" {query.upper()} " for query in recorder.queries)
    assert not any("COUNT(" in query.upper() for query in recorder.queries)
    assert not any(
        table in query
        for query in recorder.queries
        for table in (
            "catalog_publication_selections",
            "catalog_publications",
            "catalog_artifact_inputs",
        )
    )


def test_valid_active_publication_checks_full_history_and_bounded_active_reads(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "publication-valid.sqlite3")
    recorder = _ReadRecorder(connector)
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        assert connector.fetch_all(
            "SELECT state, finalized_at FROM catalog_publication_receipts "
            "WHERE revision = 1"
        ) == [("DB_COMMITTED", None)]
        assert connector.fetch_all(
            "SELECT create_count, rebuild_count, delete_count, new_galleries, "
            "changed_galleries FROM catalog_publication_candidate_projections "
            "WHERE candidate_id = %s",
            (b"c" * 16,),
        ) == [(0, 0, 0, 0, 0)]
        catalog_refinement.check_publication_atomicity_v1(cast(Any, recorder))
        # READY deliberately audits the complete Batch-0B sealed commit chain.
        # Active-context reads remain keyed/bounded; only the five chain-set
        # comparisons below are allowed to scale with publication history.
        permitted_scans = {
            "CATALOG_ANALYSIS_STAGES",
            "CATALOG_CHANNEL_REGISTRY",
            "CATALOG_PUBLICATION_STAGES",
            "CATALOG_SOURCE_PROVIDER_REGISTRY",
            "CATALOG_CANONICAL_DIGEST_POLICIES",
            "CATALOG_PUBLICATION_HEAD_REVISIONS",
            "CATALOG_PUBLICATION_HEAD_ADVANCED_ATS",
            "CATALOG_ARTIFACT_PRODUCER_FINGERPRINT_SEALS",
            "CATALOG_ARTIFACT_ZIP_WRITER_POLICIES",
            "CATALOG_ARTIFACT_STORAGE_CODECS",
            "CATALOG_PUBLICATION_COMMIT_SEALS",
            "CATALOG_PUBLICATION_GENERATION_NODES",
            "CATALOG_PUBLICATION_GENERATION_SUCCESSORS",
            "CATALOG_PUBLICATION_CANDIDATE_PROJECTIONS",
            "ANCHOR",
            "COMMITTED",
            "DESCRIPTOR",
            "FINALIZATION",
            "RECEIPT",
            "REGISTRY",
            "MAPPING",
            "MEMBER_1",
            "MEMBER_4",
            "HEAD",
            "SEALED",
            "SEAL",
        }
        for query, data, _row_count in recorder.reads:
            plans = connector.fetch_all(f"EXPLAIN QUERY PLAN {query}", data)
            for plan in plans:
                detail = str(plan[3]).upper()
                if detail.startswith("SCAN "):
                    assert any(
                        f"SCAN {table}" in detail for table in permitted_scans
                    ), detail
    finally:
        connector.close()

    assert len(recorder.reads) <= 52
    assert sum(row_count for _query, _data, row_count in recorder.reads) <= 100
    unbounded = tuple(
        query for query in recorder.queries if " LIMIT " not in f" {query.upper()} "
    )
    assert len(unbounded) == 5
    assert all("catalog_publication_" in query for query in unbounded)
    assert not any("COUNT(" in query.upper() for query in recorder.queries)
    assert not any(
        table in query
        for query in recorder.queries
        for table in (
            "catalog_publication_selections",
            "catalog_publications",
            "catalog_artifact_inputs",
        )
    )


def test_active_publication_compares_descriptor_count_with_transient_projection(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "publication-count.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        connector.execute(
            "UPDATE catalog_revision_publication_counts SET publication_count = 1 "
            "WHERE revision = 1"
        )
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="terminal selection receipt and catalog publication_count differ",
        ):
            catalog_refinement.check_publication_atomicity_v1(connector)
    finally:
        connector.close()


def test_active_source_rejects_analysis_output_manifest_corruption(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "source-output.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        forged_manifest = b"x" * 32
        _insert_canonical_seal(
            connector,
            value_sha256=forged_manifest,
            digest_domain=b"source_snapshot_manifest_v1",
            page_sha256=b"X" * 32,
        )
        _insert_snapshot_manifest_identity(
            connector,
            snapshot_manifest_sha256=forged_manifest,
        )
        connector.execute(
            "UPDATE catalog_analysis_snapshot_manifest "
            "SET snapshot_manifest_sha256 = %s WHERE analysis_id = %s",
            (forged_manifest, analysis_id),
        )
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="revision manifest differs from its analysis output",
        ):
            catalog_refinement.check_source_baseline_channel_v1(connector)
    finally:
        connector.close()


def test_active_publication_rejects_projection_seal_result_corruption(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "projection-result.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        candidate_id, _receipt_id = _insert_active_publication(connector, analysis_id)
        connector.execute(
            "UPDATE catalog_publication_checkpoint_processed_counts "
            "SET processed_count = 1 "
            "WHERE candidate_id = %s AND stage = %s",
            (candidate_id, b"VALIDATE_NEW_GALLERY"),
        )
        connector.execute(
            "UPDATE catalog_publication_batch_receipt_start_processed_counts "
            "SET start_processed_count = 1 "
            "WHERE candidate_id = %s AND stage = %s AND start_generation = 1",
            (candidate_id, b"VALIDATE_NEW_GALLERY"),
        )
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="terminal publication-result receipts and permanent commit differ",
        ):
            catalog_refinement.check_publication_atomicity_v1(connector)
    finally:
        connector.close()


def test_projection_finalized_requires_exact_empty_terminal_authority(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "projection-terminal.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        _candidate_id, receipt_id = _insert_active_publication(connector, analysis_id)
        connector.execute(
            "INSERT INTO catalog_publication_commit_finalizations (receipt_id) "
            "VALUES (%s)",
            (receipt_id,),
        )
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="initial permanent finalization checkpoint is not exact OPEN genesis",
        ):
            catalog_refinement.check_publication_atomicity_v1(connector)
    finally:
        connector.close()


def test_projection_finalized_accepts_keyed_empty_terminal_authority(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "projection-finalized.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        _candidate_id, receipt_id = _insert_active_publication(connector, analysis_id)
        _complete_publication_finalization(connector, receipt_id=receipt_id)
        assert connector.fetch_all(
            "SELECT state, finalized_at FROM catalog_publication_receipts "
            "WHERE receipt_id = %s",
            (receipt_id,),
        ) == [("PROJECTION_FINALIZED", 2)]
        catalog_refinement.check_publication_atomicity_v1(connector)
    finally:
        connector.close()


@pytest.mark.parametrize(
    "mutation_sql",
    (
        "DELETE FROM catalog_publication_commit_finalizations",
        "DELETE FROM catalog_publication_finalization_batch_seals",
        "UPDATE catalog_publication_finalization_checkpoint_generations "
        "SET generation = 3",
        "UPDATE catalog_publication_finalization_checkpoint_cursors "
        "SET cursor = X'01'",
        "UPDATE catalog_publication_finalization_checkpoint_counts "
        "SET processed_count = 1",
        "UPDATE catalog_publication_finalization_checkpoint_updated_ats "
        "SET updated_at = 3",
    ),
)
def test_projection_finalized_fails_closed_without_exact_permanent_terminal_dag(
    tmp_path: Path,
    mutation_sql: str,
) -> None:
    connector = _generated_catalog_database(
        tmp_path
        / f"projection-finalized-{sha256(mutation_sql.encode()).hexdigest()}.sqlite3"
    )
    try:
        analysis_id = _insert_active_source_head(connector)
        _candidate_id, receipt_id = _insert_active_publication(connector, analysis_id)
        _complete_publication_finalization(connector, receipt_id=receipt_id)
        connector.execute(mutation_sql)
        assert (
            connector.fetch_all(
                "SELECT state, finalized_at FROM catalog_publication_receipts "
                "WHERE receipt_id = %s",
                (receipt_id,),
            )
            == []
        )
        with pytest.raises(catalog_refinement.CatalogSemanticValidationError):
            catalog_refinement.check_publication_atomicity_v1(connector)
    finally:
        connector.close()


def test_active_publication_requires_receipt_source_provenance_from_candidate(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "publication-source.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        forged_analysis_id = b"f" * 16
        _insert_complete_analysis(
            connector,
            analysis_id=forged_analysis_id,
            build_id=b"g" * 16,
        )
        connector.execute(
            "UPDATE catalog_source_revision_provenance SET analysis_id = %s "
            "WHERE source_revision = 1",
            (forged_analysis_id,),
        )
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="not produced by its candidate analysis",
        ):
            catalog_refinement.check_publication_atomicity_v1(connector)
    finally:
        connector.close()


def test_active_publication_requires_candidate_and_build_base_receipt_cas_match(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "publication-cas.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        candidate_id, _receipt_id = _insert_active_publication(connector, analysis_id)
        connector.execute(
            "INSERT INTO catalog_publication_candidate_base_publication_commits "
            "(candidate_id, base_receipt_id) VALUES (%s, %s)",
            (candidate_id, b"t" * 16),
        )
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="base-receipt CAS differs",
        ):
            catalog_refinement.check_publication_atomicity_v1(connector)
    finally:
        connector.close()


def test_publication_history_rejects_an_orphan_sealed_source_descriptor(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "publication-head.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "INSERT INTO catalog_source_revision_anchors (source_revision) VALUES (2)"
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_channels "
            "(source_revision, channel) VALUES (2, %s)",
            (b"default",),
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_snapshot_manifests "
            "(source_revision, snapshot_manifest_sha256) VALUES (2, %s)",
            (b"s" * 32,),
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_descriptor_seals "
            "(source_revision) VALUES (2)"
        )
        connector.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="sealed source descriptor lacks its sealed publication commit",
        ):
            catalog_refinement.check_publication_atomicity_v1(connector)
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("mutation_sqls", "error_match"),
    (
        (
            (
                "UPDATE "
                "catalog_artifact_policy_semantics_artifact_algorithm_versions "
                "SET artifact_algorithm_version = 999",
                "UPDATE catalog_artifact_policy_semantics_identities "
                "SET artifact_algorithm_version = 999",
            ),
            "active artifact policy must resolve to exactly one row",
        ),
        (
            (
                "UPDATE catalog_artifact_producer_fingerprint_identities "
                "SET writer_id = X'78'",
            ),
            "active artifact producer fingerprint does not match",
        ),
        (
            (
                "UPDATE catalog_display_title_policy_algorithm_versions "
                "SET display_title_algorithm_version = 999",
                "UPDATE catalog_display_title_policy_identities "
                "SET display_title_algorithm_version = 999",
            ),
            "unsupported runtime algorithm/Unicode tuple",
        ),
        (
            (
                "UPDATE catalog_title_sort_policy_algorithm_versions "
                "SET title_sort_algorithm_version = 999",
                "UPDATE catalog_title_sort_policy_identities "
                "SET title_sort_algorithm_version = 999",
            ),
            "unsupported runtime algorithm/Unicode tuple",
        ),
        (
            (
                "UPDATE catalog_title_sort_policy_unicode_data_versions "
                "SET unicode_data_version = X'00'",
                "UPDATE catalog_title_sort_policy_identities "
                "SET unicode_data_version = X'00'",
            ),
            "unsupported runtime algorithm/Unicode tuple",
        ),
    ),
)
def test_active_publication_rejects_unregistered_runtime_policy_tuples(
    tmp_path: Path,
    mutation_sqls: tuple[str, ...],
    error_match: str,
) -> None:
    connector = _generated_catalog_database(
        tmp_path
        / f"publication-policy-{sha256(repr(mutation_sqls).encode()).hexdigest()}.sqlite3"
    )
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        connector.execute("PRAGMA foreign_keys = OFF")
        for mutation_sql in mutation_sqls:
            connector.execute(mutation_sql)
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match=error_match,
        ):
            catalog_refinement.check_artifact_semantics_v1(connector)
    finally:
        connector.close()


def test_active_title_sort_unicode_version_must_be_strict_bytes(tmp_path: Path) -> None:
    connector = _generated_catalog_database(tmp_path / "publication-unicode.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        connector.execute("PRAGMA ignore_check_constraints = ON")
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "UPDATE catalog_title_sort_policy_unicode_data_versions "
            "SET unicode_data_version = %s",
            (catalog_refinement._RUNTIME_UNICODE_DATA_VERSION.decode("ascii"),),
        )
        connector.execute(
            "UPDATE catalog_title_sort_policy_identities SET unicode_data_version = %s",
            (catalog_refinement._RUNTIME_UNICODE_DATA_VERSION.decode("ascii"),),
        )
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="not strict bytes",
        ):
            catalog_refinement.check_artifact_semantics_v1(connector)
    finally:
        connector.close()
