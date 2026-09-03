from __future__ import annotations

from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Any, cast

import pytest
from vnext_canonical_value_fixtures import seed_canonical_value

from h2hdb import catalog_refinement, vnext_identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.catalog_search import iter_search_lexemes
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_canonical_value_repository import (
    stream_and_validate_canonical_value,
)


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


def _insert_artifact_adapter_policy(
    connector: SQLiteConnector,
    *,
    policy_fingerprint: bytes,
    adapter_id: bytes,
) -> None:
    connector.execute(
        "INSERT INTO catalog_artifact_adapter_policy "
        "(policy_fingerprint_sha256, adapter_id) VALUES (%s, %s)",
        (policy_fingerprint, adapter_id),
    )


def _insert_manifest_policy(connector: SQLiteConnector, policy_id: int) -> None:
    connector.execute(
        "INSERT INTO catalog_manifest_policies "
        "(manifest_policy_id, manifest_algorithm_version, file_order_version) "
        "VALUES (%s, 1, 1)",
        (policy_id,),
    )


def _insert_source_scope(
    connector: SQLiteConnector,
    *,
    scope_key: bytes,
    source_root: bytes,
) -> None:
    connector.execute(
        "INSERT INTO catalog_source_scopes "
        "(scope_key, source_provider, source_root_sha256, identity_policy_version) "
        "VALUES (%s, %s, %s, 1)",
        (scope_key, b"filesystem", source_root),
    )


def _insert_analysis_policy(
    connector: SQLiteConnector,
    policy_id: int,
    *,
    algorithm_version: int = 1,
) -> None:
    connector.execute(
        "INSERT INTO catalog_analysis_policies "
        "(policy_id, algorithm_version, spam_artist_threshold, "
        "spam_occurrence_threshold, content_owner_rule_version, "
        "gid_winner_rule_version) VALUES (%s, %s, 1, 1, 1, 1)",
        (policy_id, algorithm_version),
    )


def _insert_artifact_policy_semantics(
    connector: SQLiteConnector,
    *,
    policy_component: bytes,
    algorithm_version: int,
    policy_fingerprint: bytes,
) -> None:
    connector.execute(
        "INSERT INTO catalog_artifact_policy_semantics "
        "(policy_component_sha256, artifact_algorithm_version, "
        "policy_fingerprint_sha256) VALUES (%s, %s, %s)",
        (policy_component, algorithm_version, policy_fingerprint),
    )


def _insert_title_policies(
    connector: SQLiteConnector,
    *,
    display_policy_id: int,
    title_sort_policy_id: int = 1,
) -> None:
    unicode_version = catalog_refinement._RUNTIME_UNICODE_DATA_VERSION
    connector.execute(
        "INSERT INTO catalog_title_sort_policy "
        "(title_sort_policy_id, title_sort_algorithm_version, unicode_data_version) "
        "VALUES (%s, 1, %s)",
        (title_sort_policy_id, unicode_version),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_policies "
        "(display_title_policy_id, display_title_algorithm_version, "
        "title_sort_policy_id) VALUES (%s, 1, %s)",
        (display_policy_id, title_sort_policy_id),
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
            "DELETE FROM catalog_resource_kinds "
            "WHERE resource_kind = X'7468756d626e61696c'",
            "catalog_resource_kind.*exact neutral two-role registry",
        ),
        (
            "DELETE FROM catalog_resource_kinds",
            "catalog_resource_kind.*exact neutral two-role registry",
        ),
    ),
)
def test_bootstrap_rejects_resource_kind_registry_corruption(
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


def test_static_search_policy_seed_rejects_missing_extra_order_and_partial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = cast(tuple[dict[str, Any], ...], ARTIFACT["bootstrap_seeds"])
    search_policy_indexes = tuple(
        index
        for index, seed in enumerate(original)
        if seed["relation"] == "search_policy"
    )
    assert len(search_policy_indexes) == 1
    index = search_policy_indexes[0]
    partial_policy = dict(original[index])
    partial_policy["value"] = partial_policy["value"][:-1]
    extra_seed = dict(original[index])
    extra_seed["id"] = f"{extra_seed['id']}.duplicate"
    reordered_policy = dict(original[index])
    first_cell, second_cell, *remaining_cells = reordered_policy["value"]
    reordered_policy["value"] = (second_cell, first_cell, *remaining_cells)
    mutations = (
        original[:index] + original[index + 1 :],
        original[: index + 1] + (extra_seed,) + original[index + 1 :],
        original[:index] + (reordered_policy,) + original[index + 1 :],
        original[:index] + (partial_policy,) + original[index + 1 :],
    )

    for mutated in mutations:
        monkeypatch.setitem(ARTIFACT, "bootstrap_seeds", mutated)
        with pytest.raises(
            catalog_refinement.BuiltinSemanticRegistryError,
            match="wide policy seeds differ",
        ):
            catalog_refinement._validate_static_catalog_contract()
        monkeypatch.setitem(ARTIFACT, "bootstrap_seeds", original)


def test_building_bootstrap_rejects_a_business_row(tmp_path: Path) -> None:
    connector = _generated_catalog_database(tmp_path / "catalog-bootstrap.sqlite3")
    try:
        connector.execute(
            "INSERT INTO catalog_revision_descriptors "
            "(revision, publication_count, artifact_count) VALUES (1, 0, 0)"
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
            "INSERT INTO catalog_analysis_state_component_seals "
            "(analysis_id, state_component, row_count, sealed_at) "
            "VALUES (%s, %s, 0, 1)",
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
        "INSERT INTO catalog_analysis_run_descriptor "
        "(analysis_id, build_id, policy_id, input_manifest_sha256, started_at) "
        "VALUES (%s, %s, %s, %s, %s)",
        (analysis_id, build_id, policy_id, input_manifest_sha256, started_at),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_run_states (analysis_id, state) VALUES (%s, %s)",
        (analysis_id, state),
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
        "INSERT INTO catalog_source_build_descriptor "
        "(build_id, scope_key, manifest_policy_id, created_at) "
        "VALUES (%s, %s, 1, %s)",
        (build_id, scope_key, created_at),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_states (build_id, state) VALUES (%s, %s)",
        (build_id, "OPEN"),
    )
    # The immutable sealed timestamp is inserted only after build-manifest
    # authority exists; `_insert_build_manifest` performs that transition.
    assert sealed_at == 1


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
        "INSERT INTO catalog_source_build_discoveries "
        "(build_id, scan_attempt, gallery_count, tree_observation_sha256, "
        "completed_at) VALUES (%s, %s, %s, %s, %s)",
        (
            build_id,
            sha256(build_id + b"scan").digest()[:16],
            gallery_count,
            sha256(build_id + b"tree").digest(),
            computed_at,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_build_manifest_core "
        "(build_id, manifest_sha256, file_count, byte_count) "
        "VALUES (%s, %s, %s, %s)",
        (build_id, manifest_sha256, file_count, byte_count),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_sealed_ats (build_id, sealed_at) "
        "VALUES (%s, 1)",
        (build_id,),
    )
    connector.execute(
        "UPDATE catalog_source_build_states SET state = 'SEALED' WHERE build_id = %s",
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
        "INSERT INTO catalog_source_snapshot_manifest_identity "
        "(snapshot_manifest_sha256, gallery_count, file_count, byte_count) "
        "VALUES (%s, %s, %s, %s)",
        (snapshot_manifest_sha256, gallery_count, file_count, byte_count),
    )


def _insert_active_source_head(
    connector: SQLiteConnector,
    *,
    published: bool = True,
) -> bytes:
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
    input_manifest = b"m" * 32
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
        manifest_sha256=input_manifest,
    )
    _insert_analysis_policy(connector, 1)
    _insert_analysis_run(
        connector,
        analysis_id=analysis_id,
        build_id=build_id,
        input_manifest_sha256=input_manifest,
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
        "INSERT INTO catalog_source_revision_descriptors "
        "(source_revision, channel, snapshot_manifest_sha256) VALUES (1, %s, %s)",
        (b"default", snapshot_manifest),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_provenance VALUES (1, %s)",
        (analysis_id,),
    )
    connector.execute(
        "INSERT INTO catalog_revision_descriptors "
        "(revision, publication_count, artifact_count) VALUES (1, 0, 0)"
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
    connector.execute(
        "INSERT INTO catalog_publication_commits "
        "(receipt_id, candidate_id, revision, source_revision, generation, "
        "preparation_id, operational_policy_id, artifact_policy_id, "
        "display_title_policy_id, new_galleries, changed_galleries, "
        "removed_galleries, duplicate_losers, committed_at) "
        "VALUES (%s, %s, 1, 1, 1, %s, 1, 1, 1, 0, 0, 0, 0, 1)",
        (receipt_id, b"c" * 16, b"p" * 16),
    )
    _insert_open_publication_finalization_checkpoint(
        connector,
        receipt_id=receipt_id,
    )
    if published:
        _complete_publication_finalization(
            connector,
            receipt_id=receipt_id,
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
        "catalog_analysis_impacted_content",
        "catalog_a_impacted_content_provenance",
        "ix_a_impacted_content_key_gallery",
    ),
    (
        "gid",
        "gid",
        17,
        "catalog_analysis_impacted_gid",
        "catalog_a_impacted_gid_provenance",
        "sqlite_autoindex_catalog_a_impacted_gid_provenance_storage_1",
    ),
)


def _insert_impacted_gallery_workset(
    connector: SQLiteConnector,
    analysis_id: bytes,
) -> None:
    scope_key = vnext_identity.source_scope_key("filesystem", b"r" * 32, 1)
    connector.execute(
        "INSERT INTO catalog_gallery_upload_times (gid, upload_time) VALUES (%s, %s)",
        (17, 1),
    )
    for gallery_id in (1, 2):
        locator_sha256 = bytes((gallery_id,)) * 32
        source_gallery_name = f"gallery-{gallery_id}".encode("ascii")
        _insert_canonical_seal(
            connector,
            value_sha256=locator_sha256,
            digest_domain=b"source_relative_locator_v1",
            page_sha256=bytes((gallery_id + 4,)) * 32,
        )
        connector.execute(
            "INSERT INTO catalog_source_locator_identity "
            "(locator_sha256, source_gallery_name) VALUES (%s, %s)",
            (locator_sha256, source_gallery_name),
        )
        connector.execute(
            "INSERT INTO catalog_source_gallery_name_gids "
            "(source_gallery_name, gid) VALUES (%s, %s)",
            (source_gallery_name, 17),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_identities "
            "(gallery_id, gallery_key, scope_key, locator_sha256) "
            "VALUES (%s, %s, %s, %s)",
            (
                gallery_id,
                bytes((gallery_id + 2,)) * 32,
                scope_key,
                locator_sha256,
            ),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_source_name_accesses "
            "(gallery_id, source_gallery_name) VALUES (%s, %s)",
            (gallery_id, source_gallery_name),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_impacted_galleries "
            "(analysis_id, gallery_id) VALUES (%s, %s)",
            (analysis_id, gallery_id),
        )


def _insert_impacted_key_family(
    connector: SQLiteConnector,
    analysis_id: bytes,
    fixture: tuple[str, str, bytes | int, str, str, str],
    *,
    witness_gallery_id: int = 1,
) -> None:
    (
        _family,
        key_column,
        key_value,
        impacted_table,
        provenance_table,
        _lookup_index,
    ) = fixture
    if _family == "gid":
        for gallery_id in (1, 2):
            connector.execute(
                "INSERT INTO catalog_a_impacted_gid_provenance_storage "
                "(analysis_id, gallery_id) VALUES (%s, %s)",
                (analysis_id, gallery_id),
            )
        connector.execute(
            "INSERT INTO catalog_analysis_impacted_gid_storage "
            "(analysis_id, gid) VALUES (%s, %s)",
            (analysis_id, key_value),
        )
        return
    for gallery_id in (1, 2):
        connector.execute(
            f"INSERT INTO {provenance_table} "
            f"(analysis_id, gallery_id, {key_column}) VALUES (%s, %s, %s)",
            (analysis_id, gallery_id, key_value),
        )
    connector.execute(
        f"INSERT INTO {impacted_table} "
        f"(analysis_id, {key_column}, witness_gallery_id) VALUES (%s, %s, %s)",
        (analysis_id, key_value, witness_gallery_id),
    )


def _insert_complete_analysis(
    connector: SQLiteConnector,
    *,
    analysis_id: bytes,
    build_id: bytes,
) -> None:
    scope_key = vnext_identity.source_scope_key("filesystem", b"r" * 32, 1)
    input_manifest = sha256(build_id).digest()
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
        manifest_sha256=input_manifest,
    )
    _insert_analysis_run(
        connector,
        analysis_id=analysis_id,
        build_id=build_id,
        input_manifest_sha256=input_manifest,
    )
    connector.execute(
        "INSERT INTO catalog_analysis_state_ancestry "
        "(analysis_id, ancestor_depth, ancestor_analysis_id) VALUES (%s, 0, %s)",
        (analysis_id, analysis_id),
    )
    _insert_analysis_seals(connector, analysis_id)


def _insert_detached_snapshot_audit(
    connector: SQLiteConnector,
    *,
    snapshot_manifest_sha256: bytes = b"h" * 32,
) -> tuple[bytes, bytes]:
    """Insert a valid inactive analysis whose snapshot bytes are already gone."""

    source_root = b"r" * 32
    _insert_canonical_seal(
        connector,
        value_sha256=source_root,
        digest_domain=b"source_root_v1",
        page_sha256=b"R" * 32,
    )
    _insert_manifest_policy(connector, 1)
    scope_key = vnext_identity.source_scope_key("filesystem", source_root, 1)
    _insert_source_scope(connector, scope_key=scope_key, source_root=source_root)
    _insert_analysis_policy(connector, 1)
    analysis_id = b"h" * 16
    build_id = b"j" * 16
    _insert_complete_analysis(
        connector,
        analysis_id=analysis_id,
        build_id=build_id,
    )
    connector.execute(
        "INSERT INTO catalog_analysis_snapshot_manifest "
        "(analysis_id, snapshot_manifest_sha256) VALUES (%s, %s)",
        (analysis_id, snapshot_manifest_sha256),
    )
    return analysis_id, build_id


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
    connector.execute(
        "INSERT INTO catalog_publication_checkpoints "
        "(candidate_id, stage, generation, cursor, processed_count, state, updated_at) "
        "VALUES (%s, %s, 2, %s, %s, %s, %s)",
        (candidate_id, stage, b"", processed_count, "COMPLETE", committed_at),
    )
    connector.execute(
        "INSERT INTO catalog_publication_batch_receipt_stored "
        "(candidate_id, stage, start_generation, batch_key, start_cursor, "
        "start_processed_count, next_cursor, row_count, committed_at) "
        "VALUES (%s, %s, 1, %s, %s, %s, %s, 0, %s)",
        (candidate_id, stage, b"terminal", b"", processed_count, b"", committed_at),
    )


def _insert_open_publication_finalization_checkpoint(
    connector: SQLiteConnector,
    *,
    receipt_id: bytes,
) -> None:
    connector.execute(
        "INSERT INTO catalog_publication_finalization_checkpoints "
        "(receipt_id, generation, cursor, processed_count, state, updated_at) "
        "VALUES (%s, 1, %s, 0, %s, 1)",
        (receipt_id, b"", "OPEN"),
    )


def _complete_publication_finalization(
    connector: SQLiteConnector,
    *,
    receipt_id: bytes,
    committed_at: int = 2,
) -> None:
    connector.execute(
        "INSERT INTO catalog_publication_finalization_batch_stored "
        "(receipt_id, start_generation, batch_key, start_cursor, "
        "start_processed_count, next_cursor, row_count, committed_at) "
        "VALUES (%s, 1, %s, %s, 0, %s, 0, %s)",
        (receipt_id, b"terminal", b"", b"", committed_at),
    )
    connector.execute(
        "UPDATE catalog_publication_finalization_checkpoints "
        "SET generation = 2, state = 'COMPLETE', updated_at = %s "
        "WHERE receipt_id = %s",
        (committed_at, receipt_id),
    )
    connector.execute(
        "INSERT INTO catalog_publication_commit_finalizations (receipt_id) VALUES (%s)",
        (receipt_id,),
    )


def _insert_active_publication(
    connector: SQLiteConnector,
    analysis_id: bytes,
) -> tuple[bytes, bytes]:
    artifact_policy_id = 1
    display_title_policy_id = 1
    algorithm_version = 2
    adapter_id = b"test-artifact-adapter"
    policy_fingerprint = b"p" * 32
    policy_component = vnext_identity.artifact_policy_digest(
        algorithm_version,
        adapter_id,
        policy_fingerprint,
    )
    _insert_canonical_seal(
        connector,
        value_sha256=policy_component,
        digest_domain=b"artifact_policy_v3",
        page_sha256=sha256(b"artifact-policy-page").digest(),
    )
    _insert_artifact_adapter_policy(
        connector,
        policy_fingerprint=policy_fingerprint,
        adapter_id=adapter_id,
    )
    _insert_artifact_policy_semantics(
        connector,
        policy_component=policy_component,
        algorithm_version=algorithm_version,
        policy_fingerprint=policy_fingerprint,
    )
    connector.execute(
        "INSERT INTO catalog_artifact_policies "
        "(artifact_policy_id, policy_component_sha256) VALUES (%s, %s)",
        (artifact_policy_id, policy_component),
    )
    _insert_title_policies(connector, display_policy_id=display_title_policy_id)

    candidate_id = b"c" * 16
    connector.execute(
        "INSERT INTO catalog_publication_candidates "
        "(candidate_id, analysis_id, reserved_revision, artifact_policy_id, "
        "display_title_policy_id, artifacts_required, created_at) "
        "VALUES (%s, %s, 1, %s, %s, 0, 0)",
        (candidate_id, analysis_id, artifact_policy_id, display_title_policy_id),
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
    connector.execute(
        "INSERT INTO catalog_discovery_seals (revision, policy_id) VALUES (1, 1)"
    )
    receipt_id = b"t" * 16
    return candidate_id, receipt_id


def _insert_exact_canonical_payload(
    connector: SQLiteConnector,
    *,
    domain: str,
    payload: bytes,
) -> bytes:
    value_sha256 = vnext_identity.canonical_value_digest(domain, payload)
    page = vnext_identity.CanonicalValuePage(
        value_sha256,
        vnext_identity.GalleryObservationNodeKind.LEAF,
        0,
        0,
        len(payload),
        (vnext_identity.CanonicalValueChunk(0, payload),),
    )
    page_bytes = vnext_identity.encode_canonical_value_page(page)
    seed_canonical_value(
        connector,
        value_sha256=value_sha256,
        digest_domain=domain.encode("ascii"),
        page_sha256=vnext_identity.canonical_value_page_digest(page_bytes),
        page_bytes=page_bytes,
        subtree_item_count=len(payload),
        allocated_at=1,
    )
    return value_sha256


def _insert_nonempty_discovery_projection(
    connector: SQLiteConnector,
) -> dict[str, bytes]:
    """Install one exact active discovery root for validator corruption tests."""

    analysis_id = _insert_active_source_head(connector)
    _insert_active_publication(connector, analysis_id)
    source_title = _insert_exact_canonical_payload(
        connector,
        domain="source_title_utf8_v1",
        payload=b"source",
    )
    display_title = _insert_exact_canonical_payload(
        connector,
        domain="display_title_utf8_v1",
        payload=b"display",
    )
    replacement_title = _insert_exact_canonical_payload(
        connector,
        domain="display_title_utf8_v1",
        payload=b"changed",
    )
    summary = _insert_exact_canonical_payload(
        connector,
        domain="catalog_summary_utf8_v1",
        payload=b"summary",
    )
    language = _insert_exact_canonical_payload(
        connector,
        domain="catalog_language_utf8_v1",
        payload=b"en",
    )
    contributor = _insert_exact_canonical_payload(
        connector,
        domain="contributor_name_utf8_v1",
        payload=b"writer",
    )
    subject = _insert_exact_canonical_payload(
        connector,
        domain="tag_value_utf8_v1",
        payload=b"space",
    )
    publication_key = vnext_identity.publication_key(17)
    occurrence = vnext_identity.catalog_publication_occurrence_sha256(
        1,
        publication_key,
    )
    source_gallery_name = b"gallery-17"

    connector.execute("PRAGMA foreign_keys = OFF")
    connector.execute(
        "UPDATE catalog_revision_descriptors "
        "SET publication_count = 1 WHERE revision = 1"
    )
    connector.execute(
        "INSERT INTO catalog_gallery_upload_times (gid, upload_time) VALUES (17, 1)"
    )
    connector.execute(
        "INSERT INTO catalog_publication_identities (publication_key, gid) "
        "VALUES (%s, 17)",
        (publication_key,),
    )
    connector.execute(
        "INSERT INTO catalog_source_gallery_name_gids (source_gallery_name, gid) "
        "VALUES (%s, 17)",
        (source_gallery_name,),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_source_name_accesses "
        "(gallery_id, source_gallery_name) VALUES (17, %s)",
        (source_gallery_name,),
    )
    connector.execute(
        "INSERT INTO catalog_publication_occurrence_identities "
        "(catalog_occurrence_sha256, revision, publication_key) "
        "VALUES (%s, 1, %s)",
        (occurrence, publication_key),
    )
    connector.execute(
        "INSERT INTO catalog_publication_storage "
        "(catalog_occurrence_sha256, gallery_id, summary_sha256, "
        "language_sha256, modified_at, source_title_sha256) "
        "VALUES (%s, 17, %s, %s, 1, %s)",
        (occurrence, summary, language, source_title),
    )
    connector.execute(
        "INSERT INTO catalog_publication_download_times "
        "(catalog_occurrence_sha256, download_time) VALUES (%s, 1)",
        (occurrence,),
    )
    connector.execute(
        "INSERT INTO catalog_display_title_choices "
        "(display_title_policy_id, source_title_sha256, source_gallery_name, "
        "title_sha256) VALUES (1, %s, %s, %s)",
        (source_title, source_gallery_name, display_title),
    )
    connector.execute(
        "INSERT INTO catalog_contributors "
        "(revision, publication_key, contributor_name_sha256, role, position) "
        "VALUES (1, %s, %s, %s, 0)",
        (publication_key, contributor, b"author"),
    )
    connector.execute(
        "INSERT INTO catalog_tag_terms (tag_id, namespace, tag_value_sha256) "
        "VALUES (1, %s, %s)",
        (b"genre", subject),
    )
    connector.execute(
        "INSERT INTO catalog_subjects "
        "(revision, publication_key, position, tag_id) VALUES (1, %s, 0, 1)",
        (publication_key,),
    )

    lexemes = tuple(
        dict.fromkeys(iter_search_lexemes((b"source", b"display", b"writer", b"space")))
    )
    connector.execute(
        "INSERT INTO catalog_search_documents "
        "(revision, publication_key, row_count) VALUES (1, %s, %s)",
        (publication_key, len(lexemes)),
    )
    lexeme_digests: dict[bytes, bytes] = {}
    for lexeme in (*lexemes, b"changed"):
        value_sha256 = _insert_exact_canonical_payload(
            connector,
            domain="search_lexeme_utf8_v1",
            payload=lexeme,
        )
        lexeme_digests[lexeme] = value_sha256
        connector.execute(
            "INSERT INTO catalog_search_lexemes (value_sha256) VALUES (%s)",
            (value_sha256,),
        )
        if lexeme in lexemes:
            connector.execute(
                "INSERT INTO catalog_search_postings "
                "(revision, value_sha256, publication_key) VALUES (1, %s, %s)",
                (value_sha256, publication_key),
            )
    connector.execute(
        "INSERT INTO catalog_language_facet_order "
        "(revision, position, language_sha256, occurrence_count) "
        "VALUES (1, 0, %s, 1)",
        (language,),
    )
    connector.execute(
        "INSERT INTO catalog_subject_facet_order "
        "(revision, position, tag_id, occurrence_count) VALUES (1, 0, 1, 1)"
    )
    connector.execute(
        "INSERT INTO catalog_contributor_facet_order "
        "(revision, position, contributor_name_sha256, role, occurrence_count) "
        "VALUES (1, 0, %s, %s, 1)",
        (contributor, b"author"),
    )
    connector.execute("PRAGMA foreign_keys = ON")
    return {
        "publication_key": publication_key,
        "source_title": source_title,
        "source_gallery_name": source_gallery_name,
        "display_title": display_title,
        "replacement_title": replacement_title,
        "language": language,
        "contributor": contributor,
        "subject": subject,
        "first_lexeme": lexeme_digests[lexemes[0]],
    }


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


def test_retention_v2_requires_current_source_provenance_baseline(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "source-provenance.sqlite3")
    try:
        _insert_active_source_head(connector)
        connector.execute("DELETE FROM catalog_source_revision_provenance")

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="active source revision provenance must resolve to exactly one row",
        ):
            catalog_refinement.check_retention_contract_v2(connector)
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
            "DELETE FROM catalog_publication_batch_receipt_stored "
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
            lookup_index = fixture[5]
            query, data = next(
                item for item in minimum_reads if provenance_table in item[0]
            )
            plans = connector.fetch_all(f"EXPLAIN QUERY PLAN {query}", data)
            plan_text = " ".join(str(row[3]) for row in plans).upper()
            assert lookup_index.upper() in plan_text
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("fixture", "fault", "error_match"),
    (
        (
            _IMPACTED_FAMILY_FIXTURES[0],
            "missing_provenance",
            "row lacks its witness provenance",
        ),
        (
            _IMPACTED_FAMILY_FIXTURES[0],
            "missing_witness",
            "row lacks its witness provenance",
        ),
        (
            _IMPACTED_FAMILY_FIXTURES[0],
            "orphan_provenance",
            "provenance has no atomic key row",
        ),
        (
            _IMPACTED_FAMILY_FIXTURES[0],
            "nonminimum_witness",
            "witness is not the minimum provenance gallery",
        ),
        (
            _IMPACTED_FAMILY_FIXTURES[1],
            "missing_provenance",
            "key storage lacks complete derived provenance",
        ),
        (
            _IMPACTED_FAMILY_FIXTURES[1],
            "orphan_provenance",
            "provenance has no atomic key row",
        ),
        (
            _IMPACTED_FAMILY_FIXTURES[1],
            "missing_identity_chain",
            "provenance storage lacks its identity chain or atomic key",
        ),
    ),
    ids=(
        "content-missing-provenance",
        "content-missing-witness",
        "content-orphan-provenance",
        "content-nonminimum-witness",
        "gid-missing-provenance",
        "gid-orphan-provenance",
        "gid-missing-identity-chain",
    ),
)
def test_incremental_impact_rejects_partial_or_nonminimum_witness_families(
    tmp_path: Path,
    fixture: tuple[str, str, bytes | int, str, str, str],
    fault: str,
    error_match: str,
) -> None:
    family, key_column, key_value, impacted, provenance, _index = fixture
    connector = _generated_catalog_database(
        tmp_path / f"impacted-key-{family}-{fault}.sqlite3"
    )
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        _insert_impacted_gallery_workset(connector, analysis_id)
        _insert_impacted_key_family(connector, analysis_id, fixture)

        connector.execute("PRAGMA foreign_keys = OFF")
        if family == "gid" and fault == "missing_provenance":
            connector.execute(
                "DELETE FROM catalog_a_impacted_gid_provenance_storage "
                "WHERE analysis_id = %s",
                (analysis_id,),
            )
        elif family == "gid" and fault == "orphan_provenance":
            connector.execute(
                "DELETE FROM catalog_analysis_impacted_gid_storage "
                "WHERE analysis_id = %s AND gid = %s",
                (analysis_id, key_value),
            )
        elif family == "gid":
            connector.execute(
                "DELETE FROM catalog_gallery_source_name_accesses "
                "WHERE gallery_id = %s",
                (1,),
            )
        elif fault == "missing_provenance":
            connector.execute(
                f"DELETE FROM {provenance} WHERE analysis_id = %s "
                f"AND {key_column} = %s",
                (analysis_id, key_value),
            )
        elif fault == "missing_witness":
            connector.execute(
                f"UPDATE {impacted} SET witness_gallery_id = 3 "
                f"WHERE analysis_id = %s AND {key_column} = %s",
                (analysis_id, key_value),
            )
        elif fault == "orphan_provenance":
            connector.execute(
                f"DELETE FROM {impacted} WHERE analysis_id = %s AND {key_column} = %s",
                (analysis_id, key_value),
            )
        else:
            connector.execute(
                f"UPDATE {impacted} SET witness_gallery_id = 2 "
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
                "UPDATE catalog_analysis_run_descriptor "
                "SET policy_id = 2 WHERE analysis_id = %s",
                (ancestors[2],),
            )
            connector.execute("PRAGMA foreign_keys = ON")
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
            "CATALOG_RESOURCE_KINDS",
            "CATALOG_SEARCH_POLICIES",
            "CATALOG_SOURCE_HEAD_REVISIONS",
            "CATALOG_SOURCE_HEAD_ADVANCED_ATS",
            "MEMBER_1",
            "REGISTRY",
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
                        f"SCAN {table}" in detail for table in permitted_bounded_scans
                    ), detail
    finally:
        connector.close()

    assert len(recorder.reads) <= 96
    assert max(row_count for _query, _data, row_count in recorder.reads) <= 23
    # One bounded registry read (the exact search-policy singleton) joined the
    # fixed READY budget; every other read is unchanged.
    assert sum(row_count for _query, _data, row_count in recorder.reads) <= 344
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
        ) == [("PUBLISHED", 2)]
        assert connector.fetch_all(
            "SELECT create_count, rebuild_count, delete_count, new_galleries, "
            "changed_galleries FROM catalog_publication_candidate_projections "
            "WHERE candidate_id = %s",
            (b"c" * 16,),
        ) == [(0, 0, 0, 0, 0)]
        catalog_refinement.check_publication_atomicity_v1(cast(Any, recorder))
        # READY audits both the compacted retained commit window and current
        # discovery projection through fixed-size keyset pages.
        permitted_scans = {
            "CATALOG_ANALYSIS_STAGES",
            "CATALOG_CHANNEL_REGISTRY",
            "CATALOG_PUBLICATION_STAGES",
            "CATALOG_SOURCE_PROVIDER_REGISTRY",
            "CATALOG_CANONICAL_DIGEST_POLICIES",
            "CATALOG_RESOURCE_KINDS",
            "CATALOG_PUBLICATION_HEAD_REVISIONS",
            "CATALOG_PUBLICATION_HEAD_ADVANCED_ATS",
            "CATALOG_PUBLICATION_COMMIT_ANCHORS",
            "CATALOG_PUBLICATION_COMMITS",
            "CATALOG_PUBLICATION_GENERATION_NODES",
            "CATALOG_PUBLICATION_GENERATION_SUCCESSORS",
            "CATALOG_PUBLICATION_CANDIDATE_PROJECTIONS",
            "ANCHOR",
            "COMMITTED",
            "DESCRIPTOR",
            "FINALIZATION",
            "OCCURRENCE",
            "RECEIPT",
            "REGISTRY",
            "MAPPING",
            "MEMBER_1",
            "MEMBER_4",
            "COMMIT_ROW",
            "HEAD",
            "SEALED",
            "SEAL",
            "CATALOG_SEARCH_POLICIES",
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

    # Exact discovery coverage adds eight bounded seal/source/facet reads.
    assert len(recorder.reads) <= 60
    # The closed neutral resource registry contributes exactly two bounded rows.
    assert sum(row_count for _query, _data, row_count in recorder.reads) <= 102
    assert all(" LIMIT " in f" {query.upper()} " for query in recorder.queries)
    assert not any("COUNT(" in query.upper() for query in recorder.queries)
    assert not any(
        table in query
        for query in recorder.queries
        for table in (
            "catalog_publication_selections",
            "catalog_candidate_artifact_inputs",
        )
    )


@pytest.mark.parametrize(
    ("relation", "mutation_sql", "parameters"),
    (
        (
            "search_document",
            "INSERT INTO catalog_search_documents "
            "(revision, publication_key, row_count) VALUES (1, %s, 0)",
            (b"d" * 32,),
        ),
        (
            "search_posting",
            "INSERT INTO catalog_search_postings "
            "(revision, value_sha256, publication_key) VALUES (1, %s, %s)",
            (b"l" * 32, b"d" * 32),
        ),
        (
            "language_facet",
            "INSERT INTO catalog_language_facet_order "
            "(revision, position, language_sha256, occurrence_count) "
            "VALUES (1, 0, %s, 1)",
            (b"l" * 32,),
        ),
        (
            "subject_facet",
            "INSERT INTO catalog_subject_facet_order "
            "(revision, position, tag_id, occurrence_count) VALUES (1, 0, 1, 1)",
            (),
        ),
        (
            "contributor_facet",
            "INSERT INTO catalog_contributor_facet_order "
            "(revision, position, contributor_name_sha256, role, occurrence_count) "
            "VALUES (1, 0, %s, %s, 1)",
            (b"n" * 32, b"author"),
        ),
    ),
)
def test_ready_rejects_active_discovery_projection_corruption(
    tmp_path: Path,
    relation: str,
    mutation_sql: str,
    parameters: tuple[object, ...],
) -> None:
    connector = _generated_catalog_database(
        tmp_path / f"active-discovery-{relation}.sqlite3"
    )
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        catalog_refinement.check_discovery_exactness_v1(connector)

        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(mutation_sql, parameters)
        connector.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(catalog_refinement.CatalogSemanticValidationError):
            catalog_refinement.check_discovery_exactness_v1(connector)
    finally:
        connector.close()


@pytest.mark.parametrize(
    "missing_family",
    (
        "search_document",
        "search_posting",
        "language_facet",
        "subject_facet",
        "contributor_facet",
    ),
)
def test_ready_rejects_active_discovery_projection_omission(
    tmp_path: Path,
    missing_family: str,
) -> None:
    connector = _generated_catalog_database(
        tmp_path / f"active-discovery-missing-{missing_family}.sqlite3"
    )
    try:
        values = _insert_nonempty_discovery_projection(connector)
        deletion = {
            "search_document": (
                "DELETE FROM catalog_search_documents WHERE revision = 1 "
                "AND publication_key = %s",
                (values["publication_key"],),
            ),
            "search_posting": (
                "DELETE FROM catalog_search_postings WHERE revision = 1 "
                "AND value_sha256 = %s AND publication_key = %s",
                (values["first_lexeme"], values["publication_key"]),
            ),
            "language_facet": (
                "DELETE FROM catalog_language_facet_order WHERE revision = 1 "
                "AND language_sha256 = %s",
                (values["language"],),
            ),
            "subject_facet": (
                "DELETE FROM catalog_subject_facet_order WHERE revision = 1 "
                "AND tag_id = 1",
                (),
            ),
            "contributor_facet": (
                "DELETE FROM catalog_contributor_facet_order WHERE revision = 1 "
                "AND contributor_name_sha256 = %s AND role = %s",
                (values["contributor"], b"author"),
            ),
        }[missing_family]
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(*deletion)
        connector.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(catalog_refinement.CatalogSemanticValidationError):
            catalog_refinement.check_discovery_exactness_v1(connector)
    finally:
        connector.close()


def test_ready_accepts_exact_nonempty_discovery_projection(tmp_path: Path) -> None:
    connector = _generated_catalog_database(tmp_path / "active-discovery-valid.sqlite3")
    try:
        _insert_nonempty_discovery_projection(connector)

        catalog_refinement.check_discovery_exactness_v1(connector)
    finally:
        connector.close()


def test_ready_canonical_cache_is_exact_bounded_and_evictable() -> None:
    cache = catalog_refinement._CanonicalValidationCache()
    domains: list[tuple[bytes, bytes, bytes]] = []
    for index in range(catalog_refinement._CANONICAL_VALIDATION_CACHE_MAX_ENTRIES + 1):
        digest = index.to_bytes(32, "big")
        domain = b"source_title_utf8_v1"
        payload = f"payload-{index}".encode()
        cache.remember(
            digest,
            domain,
            BytesIO(payload),
            byte_count=len(payload),
        )
        domains.append((digest, domain, payload))

    assert cache.open(domains[0][0], domains[0][1]) is None
    latest = cache.open(domains[-1][0], domains[-1][1])
    assert latest is not None
    latest_spool, latest_count = latest
    assert latest_count == len(domains[-1][2])
    assert latest_spool.read() == domains[-1][2]

    oversized = b"x" * (
        catalog_refinement._CANONICAL_VALIDATION_CACHE_MAX_VALUE_BYTES + 1
    )
    oversized_digest = b"z" * 32
    cache.remember(
        oversized_digest,
        b"source_title_utf8_v1",
        BytesIO(oversized),
        byte_count=len(oversized),
    )
    assert cache.open(oversized_digest, b"source_title_utf8_v1") is None


def test_ready_canonical_cache_hit_matches_streaming_and_failures_are_not_cached(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_catalog_database(tmp_path / "ready-cache.sqlite3")
    calls = 0
    original = stream_and_validate_canonical_value

    def observed_stream(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(
        catalog_refinement,
        "stream_and_validate_canonical_value",
        observed_stream,
    )
    try:
        values = _insert_nonempty_discovery_projection(connector)
        cache = catalog_refinement._CanonicalValidationCache()
        first, first_count = catalog_refinement._validated_canonical_spool(
            connector,
            values["source_title"],
            expected_domain=b"source_title_utf8_v1",
            detail="cache differential",
            cache=cache,
        )
        second, second_count = catalog_refinement._validated_canonical_spool(
            connector,
            values["source_title"],
            expected_domain=b"source_title_utf8_v1",
            detail="cache differential",
            cache=cache,
        )
        try:
            assert first_count == second_count == len(b"source")
            assert first.read() == second.read() == b"source"
            assert calls == 1
        finally:
            first.close()
            second.close()

        for _ in range(2):
            with pytest.raises(
                catalog_refinement.CatalogSemanticValidationError,
                match="wrong domain",
            ):
                invalid, _count = catalog_refinement._validated_canonical_spool(
                    connector,
                    values["source_title"],
                    expected_domain=b"display_title_utf8_v1",
                    detail="wrong domain",
                    cache=cache,
                )
                invalid.close()
        assert calls == 3
    finally:
        connector.close()


def test_ready_rejects_same_cardinality_display_title_replacement(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(
        tmp_path / "active-discovery-display-title.sqlite3"
    )
    try:
        values = _insert_nonempty_discovery_projection(connector)
        connector.execute(
            "UPDATE catalog_display_title_choices SET title_sha256 = %s "
            "WHERE display_title_policy_id = 1 AND source_title_sha256 = %s "
            "AND source_gallery_name = %s",
            (
                values["replacement_title"],
                values["source_title"],
                values["source_gallery_name"],
            ),
        )

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="postings differ from exact tokenizer output",
        ):
            catalog_refinement.check_discovery_exactness_v1(connector)
    finally:
        connector.close()


def test_ready_rejects_non_utf8_subject_namespace(tmp_path: Path) -> None:
    connector = _generated_catalog_database(
        tmp_path / "active-discovery-subject-namespace.sqlite3"
    )
    try:
        _insert_nonempty_discovery_projection(connector)
        connector.execute(
            "UPDATE catalog_tag_terms SET namespace = %s WHERE tag_id = 1",
            (b"\xff",),
        )

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="subject namespace is not exact bounded UTF-8",
        ):
            catalog_refinement.check_discovery_exactness_v1(connector)
    finally:
        connector.close()


@pytest.mark.parametrize(
    "fault",
    (
        "missing_payload",
        "missing_download_time",
        "missing_gallery_chain",
        "wrong_gallery_publication",
    ),
)
def test_catalog_occurrence_storage_rejects_relational_corruption(
    tmp_path: Path,
    fault: str,
) -> None:
    connector = _generated_catalog_database(
        tmp_path / f"catalog-occurrence-{fault}.sqlite3"
    )
    publication_key = vnext_identity.publication_key(17)
    occurrence = vnext_identity.catalog_publication_occurrence_sha256(
        1, publication_key
    )
    try:
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "INSERT INTO catalog_gallery_upload_times (gid, upload_time) "
            "VALUES (%s, %s)",
            (17, 1),
        )
        connector.execute(
            "INSERT INTO catalog_source_gallery_name_gids "
            "(source_gallery_name, gid) VALUES (%s, %s)",
            (b"gallery-1", 17),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_source_name_accesses "
            "(gallery_id, source_gallery_name) VALUES (%s, %s)",
            (1, b"gallery-1"),
        )
        connector.execute(
            "INSERT INTO catalog_publication_identities (publication_key, gid) "
            "VALUES (%s, %s)",
            (publication_key, 17),
        )
        connector.execute(
            "INSERT INTO catalog_publication_occurrence_identities "
            "(catalog_occurrence_sha256, revision, publication_key) "
            "VALUES (%s, %s, %s)",
            (occurrence, 1, publication_key),
        )
        connector.execute(
            "INSERT INTO catalog_publication_storage "
            "(catalog_occurrence_sha256, gallery_id, summary_sha256, "
            "language_sha256, modified_at, source_title_sha256) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (occurrence, 1, b"s" * 32, b"l" * 32, 1, b"t" * 32),
        )
        connector.execute(
            "INSERT INTO catalog_publication_download_times "
            "(catalog_occurrence_sha256, download_time) VALUES (%s, %s)",
            (occurrence, 1),
        )
        connector.execute("PRAGMA foreign_keys = ON")

        catalog_refinement._validate_catalog_occurrence_storage(
            connector,
            revision=1,
        )

        if fault == "missing_payload":
            connector.execute(
                "DELETE FROM catalog_publication_storage "
                "WHERE catalog_occurrence_sha256 = %s",
                (occurrence,),
            )
        elif fault == "missing_download_time":
            connector.execute(
                "DELETE FROM catalog_publication_download_times "
                "WHERE catalog_occurrence_sha256 = %s",
                (occurrence,),
            )
        elif fault == "missing_gallery_chain":
            connector.execute(
                "DELETE FROM catalog_gallery_source_name_accesses "
                "WHERE gallery_id = %s",
                (1,),
            )
        else:
            other_publication_key = vnext_identity.publication_key(18)
            connector.execute(
                "INSERT INTO catalog_gallery_upload_times (gid, upload_time) "
                "VALUES (%s, %s)",
                (18, 1),
            )
            connector.execute(
                "INSERT INTO catalog_source_gallery_name_gids "
                "(source_gallery_name, gid) VALUES (%s, %s)",
                (b"gallery-other", 18),
            )
            connector.execute(
                "INSERT INTO catalog_publication_identities (publication_key, gid) "
                "VALUES (%s, %s)",
                (other_publication_key, 18),
            )
            connector.execute(
                "UPDATE catalog_gallery_source_name_accesses "
                "SET source_gallery_name = %s WHERE gallery_id = %s",
                (b"gallery-other", 1),
            )

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="identity/storage/download-time is not congruent",
        ):
            catalog_refinement._validate_catalog_occurrence_storage(
                connector,
                revision=1,
            )
    finally:
        connector.close()


def test_active_publication_compares_descriptor_count_with_discovery_projection(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "publication-count.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        connector.execute(
            "UPDATE catalog_revision_descriptors SET publication_count = 1 "
            "WHERE revision = 1"
        )
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="active search source count differs from catalog revision",
        ):
            catalog_refinement.check_discovery_exactness_v1(connector)
    finally:
        connector.close()


def test_active_publication_rejects_partial_artifact_coverage(tmp_path: Path) -> None:
    connector = _generated_catalog_database(tmp_path / "partial-artifacts.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        connector.execute("PRAGMA ignore_check_constraints = ON")
        connector.execute(
            "UPDATE catalog_revision_descriptors "
            "SET publication_count = 2, artifact_count = 1 WHERE revision = 1"
        )
        connector.execute("PRAGMA ignore_check_constraints = OFF")

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="artifact_count is neither zero nor publication_count",
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


def test_historical_snapshot_audit_digest_does_not_require_payload(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "historical-audit.sqlite3")
    try:
        _insert_detached_snapshot_audit(connector)

        catalog_refinement.check_canonical_reference_domains_v1(connector)
        catalog_refinement.check_retention_contract_v2(connector)
    finally:
        connector.close()


def test_ready_rejects_canonical_reference_sealed_under_another_domain(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "wrong-domain.sqlite3")
    try:
        wrong_domain_value = _insert_exact_canonical_payload(
            connector,
            domain="source_title_utf8_v1",
            payload=b"not-a-tag",
        )
        connector.execute(
            "INSERT INTO catalog_tag_terms (tag_id, namespace, tag_value_sha256) "
            "VALUES (1, %s, %s)",
            (b"artist", wrong_domain_value),
        )

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="tag_term.tag_value_sha256 is not sealed under tag_value_utf8_v1",
        ):
            catalog_refinement.check_canonical_reference_domains_v1(connector)
    finally:
        connector.close()


def test_live_source_working_snapshot_pin_requires_payload(tmp_path: Path) -> None:
    connector = _generated_catalog_database(tmp_path / "working-snapshot-pin.sqlite3")
    try:
        _analysis_id, build_id = _insert_detached_snapshot_audit(connector)
        connector.execute(
            "INSERT INTO operational_source_working_builds "
            "(slot, build_id, assigned_at) VALUES (1, %s, 1)",
            (build_id,),
        )

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="live source-working analysis or publication-candidate snapshot",
        ):
            catalog_refinement.check_retention_contract_v2(connector)
    finally:
        connector.close()


def test_uncommitted_publication_candidate_snapshot_pin_requires_payload(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "candidate-snapshot-pin.sqlite3")
    snapshot_manifest = b"h" * 32
    try:
        analysis_id, _build_id = _insert_detached_snapshot_audit(
            connector,
            snapshot_manifest_sha256=snapshot_manifest,
        )
        candidate_id = b"k" * 16
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "INSERT INTO catalog_publication_candidates "
            "(candidate_id, analysis_id, reserved_revision, artifact_policy_id, "
            "display_title_policy_id, artifacts_required, created_at) "
            "VALUES (%s, %s, 2, 1, 1, 0, 0)",
            (candidate_id, analysis_id),
        )
        connector.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="live source-working analysis or publication-candidate snapshot",
        ):
            catalog_refinement.check_canonical_reference_domains_v1(connector)

        _insert_canonical_seal(
            connector,
            value_sha256=snapshot_manifest,
            digest_domain=b"source_snapshot_manifest_v1",
            page_sha256=b"H" * 32,
        )
        _insert_snapshot_manifest_identity(
            connector,
            snapshot_manifest_sha256=snapshot_manifest,
        )
        catalog_refinement.check_canonical_reference_domains_v1(connector)
    finally:
        connector.close()


def test_current_source_snapshot_pin_requires_payload(tmp_path: Path) -> None:
    connector = _generated_catalog_database(tmp_path / "current-snapshot-pin.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        missing_manifest = b"x" * 32
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "UPDATE catalog_source_revision_descriptors "
            "SET snapshot_manifest_sha256 = %s WHERE source_revision = 1",
            (missing_manifest,),
        )
        connector.execute(
            "UPDATE catalog_analysis_snapshot_manifest "
            "SET snapshot_manifest_sha256 = %s WHERE analysis_id = %s",
            (missing_manifest, analysis_id),
        )
        connector.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="active source snapshot descriptor must resolve to exactly one row",
        ):
            catalog_refinement.check_retention_contract_v2(connector)
    finally:
        connector.close()


def _insert_retained_file_family(
    connector: SQLiteConnector,
    *,
    name_bytes: bytes,
    file_no: int,
    file_sha256: bytes,
) -> bytes:
    file_key = vnext_identity.file_key(name_bytes)
    connector.execute(
        "INSERT INTO catalog_file_name_identities (file_key, name_bytes) "
        "VALUES (%s, %s)",
        (file_key, name_bytes),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_file_anchors "
        "(gallery_id, observation_id, file_key) VALUES (1, 1, %s)",
        (file_key,),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_file_file_nos "
        "(gallery_id, observation_id, file_key, file_no) "
        "VALUES (1, 1, %s, %s)",
        (file_key, file_no),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_file_file_sha256s "
        "(gallery_id, observation_id, file_key, file_sha256) "
        "VALUES (1, 1, %s, %s)",
        (file_key, file_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_file_artifact_role "
        "(gallery_id, observation_id, file_key, artifact_role) "
        "VALUES (1, 1, %s, %s)",
        (file_key, b"page"),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_file_seals "
        "(gallery_id, observation_id, file_key) VALUES (1, 1, %s)",
        (file_key,),
    )
    return file_key


def test_ready_rejects_retained_file_hash_occurrence_role_drift(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "retained-file-role.sqlite3")
    try:
        connector.execute("PRAGMA foreign_keys = OFF")
        file_sha256 = b"f" * 32
        _insert_retained_file_family(
            connector,
            name_bytes=b"001.png",
            file_no=0,
            file_sha256=file_sha256,
        )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_file_hash_occurrences "
            "(gallery_id, observation_id, file_sha256, occurrence_count) "
            "VALUES (1, 1, %s, 1)",
            (file_sha256,),
        )
        connector.execute("PRAGMA foreign_keys = ON")

        catalog_refinement.check_role_derivation_v1(connector)
        connector.execute(
            "DELETE FROM catalog_gallery_observation_file_hash_occurrences"
        )

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="file-hash occurrences differ from exact CONTENT roles",
        ):
            catalog_refinement.check_role_derivation_v1(connector)
    finally:
        connector.close()


@pytest.mark.parametrize(
    "mutation_sql",
    (
        "DELETE FROM catalog_file_name_identities",
        "DELETE FROM catalog_gallery_observation_file_file_nos",
        "DELETE FROM catalog_gallery_observation_file_file_sha256s",
        "DELETE FROM catalog_gallery_observation_file_artifact_role",
        "DELETE FROM catalog_gallery_observation_file_seals",
    ),
)
def test_ready_rejects_a_missing_retained_file_family_member(
    tmp_path: Path,
    mutation_sql: str,
) -> None:
    connector = _generated_catalog_database(
        tmp_path
        / f"missing-file-family-{sha256(mutation_sql.encode()).hexdigest()}.sqlite3"
    )
    try:
        connector.execute("PRAGMA foreign_keys = OFF")
        _insert_retained_file_family(
            connector,
            name_bytes=b"001.png",
            file_no=0,
            file_sha256=b"f" * 32,
        )
        connector.execute(mutation_sql)
        connector.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="incomplete sealed family",
        ):
            catalog_refinement.check_role_derivation_v1(connector)
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("mutation_sql", "parameters"),
    (
        (
            "INSERT INTO catalog_gallery_observation_file_file_nos "
            "(gallery_id, observation_id, file_key, file_no) "
            "VALUES (1, 1, %s, 0)",
            (b"x" * 32,),
        ),
        (
            "INSERT INTO catalog_gallery_observation_file_file_sha256s "
            "(gallery_id, observation_id, file_key, file_sha256) "
            "VALUES (1, 1, %s, %s)",
            (b"x" * 32, b"f" * 32),
        ),
        (
            "INSERT INTO catalog_gallery_observation_file_artifact_role "
            "(gallery_id, observation_id, file_key, artifact_role) "
            "VALUES (1, 1, %s, %s)",
            (b"x" * 32, b"page"),
        ),
        (
            "INSERT INTO catalog_gallery_observation_file_seals "
            "(gallery_id, observation_id, file_key) VALUES (1, 1, %s)",
            (b"x" * 32,),
        ),
    ),
)
def test_ready_rejects_a_file_family_member_without_an_anchor(
    tmp_path: Path,
    mutation_sql: str,
    parameters: tuple[bytes, ...],
) -> None:
    connector = _generated_catalog_database(
        tmp_path
        / f"extra-file-family-{sha256(mutation_sql.encode()).hexdigest()}.sqlite3"
    )
    try:
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(mutation_sql, parameters)
        connector.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="no anchor authority",
        ):
            catalog_refinement.check_role_derivation_v1(connector)
    finally:
        connector.close()


def test_role_derivation_pages_every_file_family_and_uses_range_seeks(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "file-family-pages.sqlite3")
    recorder = _ReadRecorder(connector)
    try:
        connector.execute("PRAGMA foreign_keys = OFF")
        file_sha256 = b"f" * 32
        for file_no in range(129):
            _insert_retained_file_family(
                connector,
                name_bytes=f"{file_no:03}.png".encode(),
                file_no=file_no,
                file_sha256=file_sha256,
            )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_file_hash_occurrences "
            "(gallery_id, observation_id, file_sha256, occurrence_count) "
            "VALUES (1, 1, %s, 129)",
            (file_sha256,),
        )
        connector.execute("PRAGMA foreign_keys = ON")

        catalog_refinement.check_role_derivation_v1(cast(Any, recorder))

        role_reads = tuple(
            (query, data, row_count)
            for query, data, row_count in recorder.reads
            if (
                "FROM catalog_gallery_observation_file_anchors AS anchor" in query
                or " AS member" in query
                or (
                    "FROM catalog_gallery_observation_file_file_sha256s AS file_sha"
                    in query
                )
                or "FROM catalog_gallery_observation_file_hash_occurrences" in query
            )
        )
        assert role_reads
        assert max(row_count for _query, _data, row_count in role_reads) <= 128
        derived_reads = tuple(
            row_count
            for query, _data, row_count in role_reads
            if (
                "FROM catalog_gallery_observation_file_file_sha256s AS file_sha"
                in query
            )
        )
        assert derived_reads == (128, 1, 0)
        for query, data, _row_count in role_reads:
            plans = connector.fetch_all(f"EXPLAIN QUERY PLAN {query}", data)
            plan_text = " ".join(str(row[3]) for row in plans).upper()
            assert "USE TEMP B-TREE" not in plan_text
            assert not any(str(row[3]).upper().startswith("SCAN ") for row in plans)
            if (
                "FROM catalog_gallery_observation_file_file_sha256s AS file_sha"
                in query
            ):
                assert "IX_GALLERY_FILE_HASH_READY" in plan_text
    finally:
        connector.close()


def test_ready_rejects_retained_title_sort_that_differs_from_casefold(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "retained-title-sort.sqlite3")
    try:
        _insert_title_policies(connector, display_policy_id=1)
        title = _insert_exact_canonical_payload(
            connector,
            domain="display_title_utf8_v1",
            payload="Straße".encode(),
        )
        correct_sort = _insert_exact_canonical_payload(
            connector,
            domain="title_sort_utf8_v1",
            payload=b"strasse",
        )
        wrong_sort = _insert_exact_canonical_payload(
            connector,
            domain="title_sort_utf8_v1",
            payload=b"wrong",
        )
        connector.execute(
            "INSERT INTO catalog_title_sorts "
            "(title_sort_policy_id, title_sha256, sort_title_sha256) "
            "VALUES (1, %s, %s)",
            (title, correct_sort),
        )

        catalog_refinement.check_identity_codecs_v1(connector)
        connector.execute(
            "UPDATE catalog_title_sorts SET sort_title_sha256 = %s",
            (wrong_sort,),
        )

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="differs from exact Unicode casefold",
        ):
            catalog_refinement.check_identity_codecs_v1(connector)
    finally:
        connector.close()


def test_ready_accepts_the_declared_zero_title_sort_policy_id(tmp_path: Path) -> None:
    connector = _generated_catalog_database(tmp_path / "zero-title-sort.sqlite3")
    try:
        _insert_title_policies(
            connector,
            display_policy_id=1,
            title_sort_policy_id=0,
        )
        title = _insert_exact_canonical_payload(
            connector,
            domain="display_title_utf8_v1",
            payload=b"Zero",
        )
        sort_title = _insert_exact_canonical_payload(
            connector,
            domain="title_sort_utf8_v1",
            payload=b"zero",
        )
        connector.execute(
            "INSERT INTO catalog_title_sorts "
            "(title_sort_policy_id, title_sha256, sort_title_sha256) "
            "VALUES (0, %s, %s)",
            (title, sort_title),
        )

        catalog_refinement.check_identity_codecs_v1(connector)
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
            "UPDATE catalog_publication_checkpoints "
            "SET processed_count = 1 "
            "WHERE candidate_id = %s AND stage = %s",
            (candidate_id, b"VALIDATE_NEW_GALLERY"),
        )
        connector.execute(
            "UPDATE catalog_publication_batch_receipt_stored "
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


def test_published_requires_exact_empty_terminal_authority(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "projection-terminal.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector, published=False)
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


def test_published_accepts_keyed_empty_terminal_authority(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "published.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector, published=False)
        _candidate_id, receipt_id = _insert_active_publication(connector, analysis_id)
        _complete_publication_finalization(connector, receipt_id=receipt_id)
        assert connector.fetch_all(
            "SELECT state, finalized_at FROM catalog_publication_receipts "
            "WHERE receipt_id = %s",
            (receipt_id,),
        ) == [("PUBLISHED", 2)]
        catalog_refinement.check_publication_atomicity_v1(connector)
    finally:
        connector.close()


@pytest.mark.parametrize(
    "mutation_sql",
    (
        "DELETE FROM catalog_publication_commit_finalizations",
        "DELETE FROM catalog_publication_finalization_batch_stored",
        "UPDATE catalog_publication_finalization_checkpoints SET generation = 3",
        "UPDATE catalog_publication_finalization_checkpoints SET cursor = X'01'",
        "UPDATE catalog_publication_finalization_checkpoints SET processed_count = 1",
        "UPDATE catalog_publication_finalization_checkpoints SET updated_at = 3",
    ),
)
def test_published_fails_closed_without_exact_permanent_terminal_dag(
    tmp_path: Path,
    mutation_sql: str,
) -> None:
    connector = _generated_catalog_database(
        tmp_path / f"published-{sha256(mutation_sql.encode()).hexdigest()}.sqlite3"
    )
    try:
        analysis_id = _insert_active_source_head(connector, published=False)
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


def test_active_publication_rejects_unconsumed_candidate_base_authority(
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
            match="consumed candidate base authority",
        ):
            catalog_refinement.check_publication_atomicity_v1(connector)
    finally:
        connector.close()


def _insert_reader_invisible_commit(
    connector: SQLiteConnector,
    *,
    generation: int,
) -> None:
    receipt_id = bytes((0x40 + generation,)) * 16
    candidate_id = bytes((0x50 + generation,)) * 16
    preparation_id = bytes((0x60 + generation,)) * 16
    connector.execute(
        "INSERT INTO catalog_publication_commit_anchors (receipt_id) VALUES (%s)",
        (receipt_id,),
    )
    connector.execute(
        "INSERT INTO catalog_publication_commits "
        "(receipt_id, candidate_id, revision, source_revision, generation, "
        "preparation_id, operational_policy_id, artifact_policy_id, "
        "display_title_policy_id, new_galleries, changed_galleries, "
        "removed_galleries, duplicate_losers, committed_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, 1, 1, 1, 0, 0, 0, 0, 1)",
        (
            receipt_id,
            candidate_id,
            generation,
            generation,
            generation,
            preparation_id,
        ),
    )
    _insert_open_publication_finalization_checkpoint(
        connector,
        receipt_id=receipt_id,
    )


@pytest.mark.parametrize("missing", ("edge", "node"))
def test_publication_history_rejects_missing_pending_successor_authority(
    tmp_path: Path,
    missing: str,
) -> None:
    connector = _generated_catalog_database(
        tmp_path / f"publication-pending-missing-{missing}.sqlite3"
    )
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "INSERT INTO catalog_publication_generation_nodes (generation) VALUES (2)"
        )
        connector.execute(
            "INSERT INTO catalog_publication_generation_successors "
            "(successor_generation, predecessor_generation) VALUES (2, 1)"
        )
        _insert_reader_invisible_commit(connector, generation=2)
        if missing == "edge":
            connector.execute(
                "DELETE FROM catalog_publication_generation_successors "
                "WHERE successor_generation = 2"
            )
            error_match = "successor chain"
        else:
            connector.execute(
                "DELETE FROM catalog_publication_generation_nodes WHERE generation = 2"
            )
            error_match = "generation nodes"
        connector.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match=error_match,
        ):
            catalog_refinement._validate_publication_generation_history(connector)
    finally:
        connector.close()


@pytest.mark.parametrize("shape", ("gap", "multiple"))
def test_publication_history_rejects_nonexact_pending_successor_shape(
    tmp_path: Path,
    shape: str,
) -> None:
    connector = _generated_catalog_database(
        tmp_path / f"publication-pending-{shape}.sqlite3"
    )
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        connector.execute("PRAGMA foreign_keys = OFF")
        for generation in (2, 3):
            connector.execute(
                "INSERT INTO catalog_publication_generation_nodes "
                "(generation) VALUES (%s)",
                (generation,),
            )
            connector.execute(
                "INSERT INTO catalog_publication_generation_successors "
                "(successor_generation, predecessor_generation) VALUES (%s, %s)",
                (generation, generation - 1),
            )
        if shape == "multiple":
            _insert_reader_invisible_commit(connector, generation=2)
            error_match = "more than one commit beyond"
        else:
            error_match = "exact successor"
        _insert_reader_invisible_commit(connector, generation=3)
        connector.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match=error_match,
        ):
            catalog_refinement._validate_publication_generation_history(connector)
    finally:
        connector.close()


def test_publication_history_rejects_a_successor_crossing_its_retained_floor(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "publication-head.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "UPDATE catalog_publication_generation_successors "
            "SET predecessor_generation = 1 WHERE successor_generation = 1"
        )
        connector.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="successor chain is gapped, forked, or crosses the compacted floor",
        ):
            catalog_refinement.check_publication_atomicity_v1(connector)
    finally:
        connector.close()


def test_publication_history_accepts_a_positive_compacted_generation_floor(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(tmp_path / "publication-compacted.sqlite3")
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "UPDATE catalog_publication_commits SET generation = 5 "
            "WHERE receipt_id = %s",
            (b"t" * 16,),
        )
        connector.execute("DELETE FROM catalog_publication_generation_successors")
        connector.execute("DELETE FROM catalog_publication_generation_nodes")
        connector.execute(
            "INSERT INTO catalog_publication_generation_nodes (generation) VALUES (5)"
        )
        connector.execute("PRAGMA foreign_keys = ON")

        catalog_refinement._validate_publication_generation_history(connector)
    finally:
        connector.close()


def test_publication_history_accepts_a_contiguous_prefix_pending_compaction(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(
        tmp_path / "publication-pending-prefix.sqlite3"
    )
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "UPDATE catalog_publication_commits SET generation = 5 "
            "WHERE receipt_id = %s",
            (b"t" * 16,),
        )
        connector.execute("DELETE FROM catalog_publication_generation_successors")
        connector.execute("DELETE FROM catalog_publication_generation_nodes")
        connector.execute_many(
            "INSERT INTO catalog_publication_generation_nodes (generation) VALUES (%s)",
            [(generation,) for generation in range(2, 6)],
        )
        connector.execute_many(
            "INSERT INTO catalog_publication_generation_successors "
            "(successor_generation, predecessor_generation) VALUES (%s, %s)",
            [(generation, generation - 1) for generation in range(3, 6)],
        )
        connector.execute("PRAGMA foreign_keys = ON")

        catalog_refinement._validate_publication_generation_history(connector)
    finally:
        connector.close()


def test_publication_generation_nodes_reject_a_gap_on_a_late_bounded_page(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(
        tmp_path / "publication-late-node-gap.sqlite3"
    )
    try:
        connector.execute_many(
            "INSERT INTO catalog_publication_generation_nodes (generation) VALUES (%s)",
            [(generation,) for generation in range(1, 131)],
        )
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "DELETE FROM catalog_publication_generation_nodes WHERE generation = 129"
        )
        connector.execute("PRAGMA foreign_keys = ON")
        recorder = _ReadRecorder(connector)

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="generation nodes differ",
        ):
            catalog_refinement._validate_publication_generation_nodes(
                cast(Any, recorder),
                floor=1,
                tip=130,
            )

        assert len(recorder.reads) == 2
        assert max(row_count for _query, _data, row_count in recorder.reads) <= 128
        assert all("LIMIT %s" in query for query in recorder.queries)
    finally:
        connector.close()


def test_publication_generation_nodes_reject_a_gigantic_sparse_tip_without_expansion(
    tmp_path: Path,
) -> None:
    connector = _generated_catalog_database(
        tmp_path / "publication-gigantic-node-gap.sqlite3"
    )
    try:
        gigantic_tip = 9_223_372_036_854_775_807
        connector.execute(
            "INSERT INTO catalog_publication_generation_nodes (generation) VALUES (%s)",
            (gigantic_tip,),
        )
        recorder = _ReadRecorder(connector)

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="generation nodes differ",
        ):
            catalog_refinement._validate_publication_generation_nodes(
                cast(Any, recorder),
                floor=1,
                tip=gigantic_tip,
            )

        assert recorder.reads == [
            (
                "SELECT generation FROM catalog_publication_generation_nodes "
                "WHERE generation > %s ORDER BY generation LIMIT %s",
                (-1, 128),
                2,
            )
        ]
    finally:
        connector.close()


def test_publication_history_pages_257_commits_without_retirement_n_plus_one(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_catalog_database(
        tmp_path / "publication-history-query-bound.sqlite3"
    )
    try:
        analysis_id = _insert_active_source_head(connector)
        _insert_active_publication(connector, analysis_id)
        generations = tuple(range(2, 258))

        def identity(prefix: bytes, generation: int) -> bytes:
            return prefix + generation.to_bytes(15, "big")

        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute_many(
            "INSERT INTO catalog_publication_commit_anchors (receipt_id) VALUES (%s)",
            [(identity(b"r", generation),) for generation in generations],
        )
        connector.execute_many(
            "INSERT INTO catalog_publication_commits "
            "(receipt_id, candidate_id, revision, source_revision, generation, "
            "preparation_id, operational_policy_id, artifact_policy_id, "
            "display_title_policy_id, new_galleries, changed_galleries, "
            "removed_galleries, duplicate_losers, committed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, 1, 1, 1, 0, 0, 0, 0, 1)",
            [
                (
                    identity(b"r", generation),
                    identity(b"c", generation),
                    generation,
                    generation,
                    generation,
                    identity(b"p", generation),
                )
                for generation in generations
            ],
        )
        connector.execute_many(
            "INSERT INTO catalog_publication_finalization_checkpoints "
            "(receipt_id, generation, cursor, processed_count, state, updated_at) "
            "VALUES (%s, 2, %s, 0, 'COMPLETE', 2)",
            [(identity(b"r", generation), b"") for generation in generations],
        )
        connector.execute_many(
            "INSERT INTO catalog_publication_finalization_batch_stored "
            "(receipt_id, start_generation, batch_key, start_cursor, "
            "start_processed_count, next_cursor, row_count, committed_at) "
            "VALUES (%s, 1, %s, %s, 0, %s, 0, 2)",
            [
                (identity(b"r", generation), b"terminal", b"", b"")
                for generation in generations
            ],
        )
        connector.execute_many(
            "INSERT INTO catalog_publication_generation_nodes (generation) VALUES (%s)",
            [(generation,) for generation in generations],
        )
        connector.execute_many(
            "INSERT INTO catalog_publication_generation_successors "
            "(successor_generation, predecessor_generation) VALUES (%s, %s)",
            [(generation, generation - 1) for generation in generations],
        )
        connector.execute("PRAGMA foreign_keys = ON")
        recorder = _ReadRecorder(connector)
        transition_loads = 0

        def validated_transitions(
            _connector: object,
        ) -> dict[bytes, catalog_refinement._OpenPcomTransition]:
            nonlocal transition_loads
            transition_loads += 1
            return {
                identity(b"r", generation): catalog_refinement._OpenPcomTransition(
                    preparation_id=identity(b"p", generation),
                    phase="PCOM_COMMIT_EFFECT_ROOT",
                    cursor=b"",
                    phase_order=9,
                )
                for generation in generations
            }

        monkeypatch.setattr(
            catalog_refinement,
            "_validated_open_pcom_transitions",
            validated_transitions,
        )

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match="more than one commit beyond",
        ):
            catalog_refinement._validate_publication_generation_history(
                cast(Any, recorder)
            )

        assert transition_loads == 1
        commit_page_reads = [
            read
            for read in recorder.reads
            if "ORDER BY committed.generation LIMIT %s" in read[0]
        ]
        assert [row_count for _query, _data, row_count in commit_page_reads] == [
            128,
            128,
            1,
        ]
        assert not any(
            "FROM catalog_publication_finalization_batch_receipts " in query
            and "LEFT JOIN catalog_publication_finalization_batch_receipts" not in query
            for query in recorder.queries
        )
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("mutation_sqls", "error_match"),
    (
        (
            (
                "UPDATE catalog_artifact_policy_semantics "
                "SET artifact_algorithm_version = 999",
            ),
            "unregistered runtime algorithm version",
        ),
        (
            ("UPDATE catalog_artifact_adapter_policy SET adapter_id = X'78'",),
            "active artifact policy component does not match its exact tuple",
        ),
        (
            (
                "UPDATE catalog_display_title_policies "
                "SET display_title_algorithm_version = 999",
            ),
            "unsupported runtime algorithm/Unicode tuple",
        ),
        (
            (
                "UPDATE catalog_title_sort_policy "
                "SET title_sort_algorithm_version = 999",
            ),
            "unsupported runtime algorithm/Unicode tuple",
        ),
        (
            ("UPDATE catalog_title_sort_policy SET unicode_data_version = X'00'",),
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
