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
    assert all(" LIMIT " in f" {query.upper()} " for query in recorder.queries)
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
            "UPDATE catalog_artifact_zip_writer_policies SET compression_level = 8",
            "artifact_zip_writer_policy.*exact v1 singleton",
        ),
        (
            "UPDATE catalog_artifact_storage_codecs SET locator_codec_version = 2",
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
        connector.execute(mutation_sql)
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match=error_match,
        ):
            catalog_refinement.check_bootstrap_v1(connector)
    finally:
        connector.close()


def test_building_bootstrap_rejects_a_business_row(tmp_path: Path) -> None:
    connector = _generated_catalog_database(tmp_path / "catalog-bootstrap.sqlite3")
    try:
        connector.execute(
            "INSERT INTO catalog_revisions (revision, publication_count, published_at) "
            "VALUES (1, 0, 0)"
        )
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
        "INSERT INTO catalog_canonical_value_allocations "
        "(value_sha256, digest_domain, byte_count, allocated_at) "
        "VALUES (%s, %s, 0, 0)",
        (value_sha256, digest_domain),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_pages "
        "(page_sha256, value_sha256, page_bytes) VALUES (%s, %s, %s)",
        (page_sha256, value_sha256, b"x"),
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
        connector.execute(
            "INSERT INTO catalog_analysis_state_component_seals "
            "(analysis_id, state_component, row_count, sealed_at) "
            "VALUES (%s, %s, 0, 1)",
            (analysis_id, component.encode("ascii")),
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
    connector.execute("INSERT INTO catalog_manifest_policies VALUES (1, 1, 1)")
    connector.execute(
        "INSERT INTO catalog_source_scopes VALUES (%s, %s, %s, 1)",
        (scope_key, b"filesystem", source_root),
    )
    connector.execute(
        "INSERT INTO catalog_source_builds VALUES (%s, %s, 1, 'SEALED', 0, 1)",
        (build_id, scope_key),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_channel VALUES (%s, %s)",
        (build_id, b"default"),
    )
    connector.execute(
        "INSERT INTO catalog_build_manifests VALUES (%s, %s, 0, 0, 0, 1)",
        (build_id, b"m" * 32),
    )
    connector.execute("INSERT INTO catalog_analysis_policies VALUES (1, 1, 1, 1, 1, 1)")
    connector.execute(
        "INSERT INTO catalog_analysis_runs VALUES (%s, %s, 1, %s, 'COMPLETE', 0, 1)",
        (analysis_id, build_id, b"i" * 32),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_state_anchors VALUES (%s, %s, 0)",
        (analysis_id, analysis_id),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_state_ancestry VALUES (%s, 0, %s)",
        (analysis_id, analysis_id),
    )
    _insert_analysis_seals(connector, analysis_id)
    connector.execute(
        "INSERT INTO catalog_source_snapshot_manifest_identity VALUES (%s, 0, 0, 0)",
        (snapshot_manifest,),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_snapshot_manifest VALUES (%s, %s)",
        (analysis_id, snapshot_manifest),
    )
    connector.execute(
        "INSERT INTO catalog_source_revisions VALUES (1, %s, %s, 1)",
        (b"default", snapshot_manifest),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_provenance VALUES (1, %s)",
        (analysis_id,),
    )
    connector.execute(
        "INSERT INTO catalog_source_heads VALUES (%s, 1, 1, 1)",
        (b"default",),
    )
    return analysis_id


def _insert_complete_analysis(
    connector: SQLiteConnector,
    *,
    analysis_id: bytes,
    build_id: bytes,
) -> None:
    scope_key = vnext_identity.source_scope_key("filesystem", b"r" * 32, 1)
    connector.execute(
        "INSERT INTO catalog_source_builds "
        "(build_id, scope_key, manifest_policy_id, state, created_at, sealed_at) "
        "VALUES (%s, %s, 1, 'SEALED', 0, 1)",
        (build_id, scope_key),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_channel (build_id, channel) VALUES (%s, %s)",
        (build_id, b"default"),
    )
    connector.execute(
        "INSERT INTO catalog_build_manifests "
        "(build_id, manifest_sha256, gallery_count, file_count, byte_count, computed_at) "
        "VALUES (%s, %s, 0, 0, 0, 1)",
        (build_id, sha256(build_id).digest()),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_runs "
        "(analysis_id, build_id, policy_id, input_manifest_sha256, state, "
        "started_at, completed_at) "
        "VALUES (%s, %s, 1, %s, 'COMPLETE', 0, 1)",
        (analysis_id, build_id, sha256(analysis_id).digest()),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_state_anchors "
        "(analysis_id, anchor_analysis_id, overlay_depth) VALUES (%s, %s, 0)",
        (analysis_id, analysis_id),
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
            "DELETE FROM catalog_analysis_state_anchors WHERE analysis_id = %s",
            (analysis_id,),
        )
        connector.execute(
            "DELETE FROM catalog_analysis_baselines WHERE analysis_id = %s",
            (analysis_id,),
        )
    anchor_analysis_id = ancestors[-1]
    for offset, analysis_id in enumerate(ancestors):
        suffix = ancestors[offset:]
        depth = len(suffix) - 1
        connector.execute(
            "INSERT INTO catalog_analysis_state_anchors "
            "(analysis_id, anchor_analysis_id, overlay_depth) VALUES (%s, %s, %s)",
            (analysis_id, anchor_analysis_id, depth),
        )
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
    connector.execute(
        "INSERT INTO catalog_artifact_producer_fingerprints "
        "(producer_fingerprint_sha256, artifact_algorithm_version, "
        "producer_equivalence_class, writer_id, python_abi, pillow_build, "
        "libjpeg_build, zlib_build) VALUES (%s, 1, %s, %s, %s, %s, %s, %s)",
        (producer_fingerprint, b"exact-v1", *producer_values),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics "
        "(policy_component_sha256, artifact_algorithm_version, "
        "max_image_short_side, producer_fingerprint_sha256) "
        "VALUES (%s, 1, 2048, %s)",
        (policy_component, producer_fingerprint),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policies "
        "(artifact_policy_id, policy_component_sha256) VALUES (%s, %s)",
        (artifact_policy_id, policy_component),
    )
    connector.execute(
        "INSERT INTO catalog_title_sort_policy "
        "(title_sort_policy_id, title_sort_algorithm_version, unicode_data_version) "
        "VALUES (1, 1, %s)",
        (catalog_refinement._RUNTIME_UNICODE_DATA_VERSION,),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policies "
        "(display_title_policy_id, display_title_algorithm_version, "
        "title_sort_policy_id) VALUES (%s, 1, 1)",
        (display_title_policy_id,),
    )

    candidate_id = b"c" * 16
    receipt_id = b"t" * 16
    connector.execute(
        "INSERT INTO catalog_publication_candidates "
        "(candidate_id, analysis_id, reserved_revision, channel, "
        "artifact_policy_id, display_title_policy_id, artifacts_required, "
        "state, created_at, sealed_at) "
        "VALUES (%s, %s, 1, %s, %s, %s, 0, 'PUBLISHED', 0, 1)",
        (
            candidate_id,
            analysis_id,
            b"default",
            artifact_policy_id,
            display_title_policy_id,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_publication_candidate_projection_seal "
        "(candidate_id, publication_count, artifact_input_count, "
        "prepared_artifact_count, create_count, rebuild_count, delete_count, "
        "unchanged_count, new_galleries, changed_galleries, removed_galleries, "
        "duplicate_losers, projection_sealed_at) "
        "VALUES (%s, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1)",
        (candidate_id,),
    )
    connector.execute(
        "INSERT INTO catalog_revisions (revision, publication_count, published_at) "
        "VALUES (1, 0, 1)"
    )
    connector.execute(
        "INSERT INTO catalog_publication_receipts "
        "(receipt_id, revision, source_revision, reserved_revision, channel, "
        "artifact_policy_id, display_title_policy_id, publication_count, "
        "new_galleries, changed_galleries, removed_galleries, duplicate_losers, "
        "state, committed_at, finalized_at) "
        "VALUES (%s, 1, 1, 1, %s, %s, %s, 0, 0, 0, 0, 0, "
        "'DB_COMMITTED', 1, NULL)",
        (
            receipt_id,
            b"default",
            artifact_policy_id,
            display_title_policy_id,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_publication_heads "
        "(channel, revision, generation, advanced_at) VALUES (%s, 1, 1, 1)",
        (b"default",),
    )
    return candidate_id, receipt_id


def test_active_source_head_requires_all_five_analysis_seals(tmp_path: Path) -> None:
    connector = _generated_catalog_database(tmp_path / "catalog-seals.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        catalog_refinement.check_source_baseline_channel_v1(connector)

        connector.execute(
            "DELETE FROM catalog_analysis_state_component_seals "
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
            connector.execute(
                "INSERT INTO catalog_analysis_policies "
                "(policy_id, algorithm_version, spam_artist_threshold, "
                "spam_occurrence_threshold, content_owner_rule_version, "
                "gid_winner_rule_version) VALUES (2, 2, 1, 1, 1, 1)"
            )
            connector.execute(
                "UPDATE catalog_analysis_runs SET policy_id = 2 WHERE analysis_id = %s",
                (ancestors[2],),
            )
        else:
            connector.execute(
                "DELETE FROM catalog_analysis_state_component_seals "
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
            "CATALOG_SOURCE_HEADS",
            "CATALOG_ARTIFACT_ZIP_WRITER_POLICIES",
            "CATALOG_ARTIFACT_STORAGE_CODECS",
        }
        for query, data, _row_count in recorder.reads:
            plans = connector.fetch_all(f"EXPLAIN QUERY PLAN {query}", data)
            for plan in plans:
                detail = str(plan[3]).upper()
                if detail.startswith("SCAN "):
                    assert any(
                        f"SCAN {table}" in detail for table in permitted_bounded_scans
                    )
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


def test_valid_active_publication_uses_only_bounded_head_and_seal_reads(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "publication-valid.sqlite3")
    recorder = _ReadRecorder(connector)
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        catalog_refinement.check_publication_atomicity_v1(cast(Any, recorder))
        permitted_bounded_scans = {
            "CATALOG_ANALYSIS_STAGES",
            "CATALOG_CHANNEL_REGISTRY",
            "CATALOG_PUBLICATION_STAGES",
            "CATALOG_SOURCE_PROVIDER_REGISTRY",
            "CATALOG_CANONICAL_DIGEST_POLICIES",
            "CATALOG_PUBLICATION_HEADS",
            "CATALOG_ARTIFACT_ZIP_WRITER_POLICIES",
            "CATALOG_ARTIFACT_STORAGE_CODECS",
        }
        for query, data, _row_count in recorder.reads:
            plans = connector.fetch_all(f"EXPLAIN QUERY PLAN {query}", data)
            for plan in plans:
                detail = str(plan[3]).upper()
                if detail.startswith("SCAN "):
                    assert any(
                        f"SCAN {table}" in detail for table in permitted_bounded_scans
                    )
    finally:
        connector.close()

    assert len(recorder.reads) <= 40
    assert sum(row_count for _query, _data, row_count in recorder.reads) <= 79
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


def test_active_publication_compares_the_two_authoritative_count_scalars(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "publication-count.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        connector.execute(
            "UPDATE catalog_publication_receipts SET publication_count = 1 "
            "WHERE revision = 1"
        )
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="publication_count differ",
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
        connector.execute(
            "INSERT INTO catalog_source_snapshot_manifest_identity "
            "VALUES (%s, 0, 0, 0)",
            (forged_manifest,),
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
            "UPDATE catalog_publication_candidate_projection_seal "
            "SET new_galleries = 1 WHERE candidate_id = %s",
            (candidate_id,),
        )
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="projection seal and receipt result scalars differ",
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
            "UPDATE catalog_publication_receipts "
            "SET state = 'PROJECTION_FINALIZED', finalized_at = 2 "
            "WHERE receipt_id = %s",
            (receipt_id,),
        )
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="finalized artifact checkpoint must resolve to exactly one row",
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
        candidate_id, receipt_id = _insert_active_publication(connector, analysis_id)
        stage = b"FINALIZE_ARTIFACTS"
        connector.execute(
            "INSERT INTO catalog_publication_checkpoints "
            "(candidate_id, stage, generation, cursor, processed_count, state, updated_at) "
            "VALUES (%s, %s, 1, %s, 0, 'COMPLETE', 2)",
            (candidate_id, stage, b""),
        )
        connector.execute(
            "INSERT INTO catalog_publication_batch_receipts "
            "(candidate_id, stage, batch_key, start_generation, start_cursor, "
            "start_processed_count, next_cursor, next_processed_count, row_count, "
            "next_state, terminal, committed_generation, committed_at) "
            "VALUES (%s, %s, %s, 0, %s, 0, %s, 0, 0, 'COMPLETE', 1, 1, 2)",
            (candidate_id, stage, b"terminal", b"", b""),
        )
        connector.execute(
            "UPDATE catalog_publication_receipts "
            "SET state = 'PROJECTION_FINALIZED', finalized_at = 2 "
            "WHERE receipt_id = %s",
            (receipt_id,),
        )
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


def test_active_publication_requires_candidate_and_build_base_source_cas_match(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "publication-cas.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        candidate_id, _receipt_id = _insert_active_publication(connector, analysis_id)
        connector.execute(
            "INSERT INTO catalog_publication_candidate_base_sources "
            "(candidate_id, base_source_revision, base_source_generation) "
            "VALUES (%s, 1, 1)",
            (candidate_id,),
        )
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="base-source CAS differs",
        ):
            catalog_refinement.check_publication_atomicity_v1(connector)
    finally:
        connector.close()


def test_active_publication_requires_the_source_head_cas_result(tmp_path: Path) -> None:
    connector = _generated_catalog_database(tmp_path / "publication-head.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        connector.execute(
            "UPDATE catalog_source_heads SET generation = 2 WHERE channel = %s",
            (b"default",),
        )
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="active source-head CAS result",
        ):
            catalog_refinement.check_publication_atomicity_v1(connector)
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("mutation_sql", "error_match"),
    (
        (
            "UPDATE catalog_artifact_policy_semantics "
            "SET artifact_algorithm_version = 999",
            "active artifact policy must resolve to exactly one row",
        ),
        (
            "UPDATE catalog_artifact_producer_fingerprints SET writer_id = X'78'",
            "active artifact producer fingerprint does not match",
        ),
        (
            "UPDATE catalog_display_title_policies "
            "SET display_title_algorithm_version = 999",
            "unsupported runtime algorithm/Unicode tuple",
        ),
        (
            "UPDATE catalog_title_sort_policy SET title_sort_algorithm_version = 999",
            "unsupported runtime algorithm/Unicode tuple",
        ),
        (
            "UPDATE catalog_title_sort_policy SET unicode_data_version = X'00'",
            "unsupported runtime algorithm/Unicode tuple",
        ),
    ),
)
def test_active_publication_rejects_unregistered_runtime_policy_tuples(
    tmp_path: Path,
    mutation_sql: str,
    error_match: str,
) -> None:
    connector = _generated_catalog_database(
        tmp_path
        / f"publication-policy-{sha256(mutation_sql.encode()).hexdigest()}.sqlite3"
    )
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
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
        connector.execute(
            "UPDATE catalog_title_sort_policy SET unicode_data_version = %s",
            (catalog_refinement._RUNTIME_UNICODE_DATA_VERSION.decode("ascii"),),
        )
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="not strict bytes",
        ):
            catalog_refinement.check_artifact_semantics_v1(connector)
    finally:
        connector.close()
