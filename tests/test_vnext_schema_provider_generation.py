from __future__ import annotations

import ast
import hashlib
import subprocess
import sys
import tomllib
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

from h2hdb import vnext_schema_provider as provider_module
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.schema_epoch import SchemaEpochValidationError, SchemaSeedStatement
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_schema_provider import (
    GeneratedVNextSchemaProvider,
    VNextSchemaProviderUnavailableError,
)

ROOT = Path(__file__).resolve().parents[1]
GENERATOR = ROOT / "scripts" / "generate-vnext-schema-provider.py"
GENERATED = ROOT / "src" / "h2hdb" / "_generated_vnext_schema.py"
DATA_PHYSICAL = ROOT / "verification" / "schema" / "physical.toml"
OPERATIONAL_PHYSICAL = ROOT / "verification" / "schema" / "operational_physical.toml"
ARTIFACT_DATA = cast(dict[str, Any], ARTIFACT)


def _load(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        return tomllib.load(stream)


def test_generated_provider_artifact_is_deterministic_and_current() -> None:
    subprocess.run(
        [sys.executable, str(GENERATOR), "--check"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    for relative_path, expected_sha256 in ARTIFACT_DATA["source_provenance"]:
        assert (
            hashlib.sha256((ROOT / relative_path).read_bytes()).hexdigest()
            == expected_sha256
        )


def test_runtime_provider_modules_do_not_import_verification() -> None:
    for path in (
        GENERATED,
        ROOT / "src" / "h2hdb" / "vnext_schema_provider.py",
    ):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imported.update(
            str(node.module)
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        )
        assert not any(
            name == "verification" or name.startswith("verification.")
            for name in imported
        )

    probe = """
import importlib.abc
import sys

class RejectVerification(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'verification' or fullname.startswith('verification.'):
            raise AssertionError(f'runtime imported {fullname}')
        return None

sys.meta_path.insert(0, RejectVerification())
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider
provider = GeneratedVNextSchemaProvider('sqlite')
assert provider.generated_definition_data['relations']
assert not any(name == 'verification' or name.startswith('verification.') for name in sys.modules)
"""
    subprocess.run(
        [sys.executable, "-c", probe],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


def test_generated_coverage_is_exact_and_excludes_control_and_stubs() -> None:
    data = _load(DATA_PHYSICAL)
    operational = _load(OPERATIONAL_PHYSICAL)
    expected_data = tuple(data["source_slice"])
    expected_operational = tuple(operational["source_slice"])

    assert ARTIFACT_DATA["data_relations"] == expected_data
    assert ARTIFACT_DATA["operational_relations"] == expected_operational
    assert "schema_epoch_control" not in ARTIFACT_DATA["relation_order"]
    assert set(ARTIFACT_DATA["relation_order"]) == set(expected_data) | set(
        expected_operational
    )
    assert len(ARTIFACT_DATA["relation_order"]) == len(expected_data) + len(
        expected_operational
    )

    stub_tables = {value["table"] for value in operational["external_stub"]}
    data_tables = {value["table"] for value in data["relation"]}
    assert stub_tables <= data_tables
    for backend_payload in ARTIFACT_DATA["backends"].values():
        generated_tables = {value["table"] for value in backend_payload["relations"]}
        assert generated_tables == data_tables | {
            value["table"]
            for value in operational["relation"]
            if value["name"] != "schema_epoch_control"
        }
        assert len(generated_tables) == len(backend_payload["relations"])
        assert backend_payload["epoch_control"]["table"] == "h2hdb_schema_epoch"
        assert "h2hdb_schema_epoch" not in generated_tables


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_generated_metadata_view_is_a_sealed_vertical_join(backend: str) -> None:
    payload = ARTIFACT_DATA["backends"][backend]
    metadata = next(
        relation
        for relation in payload["relations"]
        if relation["relation"] == "gallery_observation_metadata"
    )
    assert metadata["kind"] == "view"
    assert metadata["table"] == "catalog_gallery_observation_metadata"
    assert metadata["view_dependencies"] == (
        "gallery_observation_metadata_anchor",
        "gallery_observation_metadata_seal",
        "gallery_source_name_access",
        "source_gallery_name_gid",
        "gallery_upload_time",
        "gallery_observation_download_time",
        "gallery_observation_modified_time",
    )
    statements = dict(payload["slices"])["relation:gallery_observation_metadata"]
    assert len(statements) == 1
    _statement_id, object_kind, object_name, sql = statements[0]
    assert object_kind == "view"
    assert object_name == "catalog_gallery_observation_metadata"
    assert "catalog_gallery_observation_metadata_seals" in sql
    assert "catalog_gallery_source_name_accesses" in sql
    assert "catalog_source_gallery_name_gids" in sql
    assert "catalog_gallery_upload_times" in sql
    assert "catalog_gallery_observation_modified_times" in sql


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
@pytest.mark.parametrize(
    ("relation_name", "table", "dependencies", "physical_dependencies"),
    [
        (
            "artifact_semantic_input",
            "catalog_artifact_semantic_input",
            (
                "artifact_semantic_input_anchor",
                "artifact_semantic_input_seal",
                "artifact_semantic_input_source_manifest_component_sha256",
                "artifact_semantic_input_member_plan_component_sha256",
                "artifact_semantic_input_effective_content_component_sha256",
                "artifact_semantic_input_selected_component_sha256",
                "artifact_semantic_input_owner_component_sha256",
                "artifact_semantic_input_policy_component_sha256",
                "artifact_semantic_input_identity",
            ),
            (
                "catalog_artifact_semantic_input_anchors",
                "catalog_artifact_semantic_input_seals",
                "catalog_artifact_semantic_source_manifest_sha256s",
                "catalog_artifact_semantic_member_plan_sha256s",
                "catalog_artifact_semantic_effective_content_sha256s",
                "catalog_artifact_semantic_selected_sha256s",
                "catalog_artifact_semantic_owner_sha256s",
                "catalog_artifact_semantic_policy_sha256s",
                "catalog_artifact_semantic_input_identities",
            ),
        ),
        (
            "prepared_artifact",
            "catalog_prepared_artifacts",
            (
                "prepared_artifact_anchor",
                "prepared_artifact_seal",
                "prepared_artifact_sha256",
                "prepared_artifact_storage_codec_version",
                "prepared_artifact_storage_generation",
                "prepared_artifact_protection_token",
                "prepared_artifact_state",
            ),
            (
                "catalog_prepared_artifact_anchors",
                "catalog_prepared_artifact_seals",
                "catalog_prepared_artifact_sha256s",
                "catalog_prepared_artifact_storage_codec_versions",
                "catalog_prepared_artifact_storage_generations",
                "catalog_prepared_artifact_protection_tokens",
                "catalog_prepared_artifact_states",
            ),
        ),
        (
            "catalog_artifact",
            "catalog_artifacts",
            (
                "catalog_artifact_anchor",
                "catalog_artifact_seal",
                "catalog_artifact_sha256",
                "catalog_artifact_semantics_sha256",
            ),
            (
                "catalog_artifact_anchors",
                "catalog_artifact_seals",
                "catalog_artifact_sha256s",
                "catalog_artifact_semantics_sha256s",
            ),
        ),
    ],
)
def test_generated_batch7_artifact_views_are_sealed_vertical_joins(
    backend: str,
    relation_name: str,
    table: str,
    dependencies: tuple[str, ...],
    physical_dependencies: tuple[str, ...],
) -> None:
    payload = ARTIFACT_DATA["backends"][backend]
    relation = next(
        value for value in payload["relations"] if value["relation"] == relation_name
    )
    assert relation["kind"] == "view"
    assert relation["table"] == table
    assert relation["view_dependencies"] == dependencies
    statements = dict(payload["slices"])[f"relation:{relation_name}"]
    assert len(statements) == 1
    _statement_id, object_kind, object_name, sql = statements[0]
    assert object_kind == "view"
    assert object_name == table
    for physical_dependency in physical_dependencies:
        assert physical_dependency in sql
    assert "catalog_artifact_identity" not in sql


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_generated_batch7_artifact_delta_views_use_occurrences_and_inputs(
    backend: str,
) -> None:
    payload = ARTIFACT_DATA["backends"][backend]
    relations = {value["relation"]: value for value in payload["relations"]}
    slices = dict(payload["slices"])

    old = relations["artifact_delta_old"]
    assert old["kind"] == "view"
    assert old["view_dependencies"] == (
        "publication_candidate",
        "publication_candidate_base_catalog",
        "catalog_artifact",
    )
    old_sql = slices["relation:artifact_delta_old"][0][3]
    assert "catalog_publication_candidate_base_catalog" in old_sql
    assert "catalog_publication_candidates" in old_sql
    assert "catalog_artifacts" in old_sql
    assert "artifact_semantics_sha256" in old_sql
    assert "artifact_sha256" in old_sql

    new = relations["artifact_delta_new"]
    assert new["kind"] == "view"
    assert new["view_dependencies"] == ("artifact_input",)
    new_sql = slices["relation:artifact_delta_new"][0][3]
    assert "catalog_candidate_artifact_inputs" in new_sql
    assert "artifact_semantics_sha256" in new_sql

    assert "artifact_identity" not in old_sql
    assert "artifact_identity" not in new_sql
    assert "artifact_id" not in old_sql
    assert "artifact_id" not in new_sql


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
@pytest.mark.parametrize(
    ("relation_name", "table", "dependencies", "physical_dependencies"),
    [
        (
            "source_build_discovery",
            "catalog_source_build_discoveries",
            (
                "source_build_discovery_anchor",
                "source_build_discovery_seal",
                "source_build_discovery_scan_attempt",
                "source_build_discovery_gallery_count",
                "source_build_discovery_tree_observation_sha256",
                "source_build_discovery_completed_at",
            ),
            (
                "catalog_source_build_discovery_anchors",
                "catalog_source_build_discovery_seals",
                "catalog_source_build_discovery_scan_attempts",
                "catalog_source_build_discovery_gallery_counts",
                "catalog_source_build_discovery_tree_observation_sha256s",
                "catalog_source_build_discovery_completed_ats",
            ),
        ),
        (
            "gallery_observation_scan",
            "catalog_gallery_observation_scans",
            (
                "gallery_observation_scan_anchor",
                "gallery_observation_scan_seal",
                "gallery_observation_scan_observation_sha256",
                "gallery_observation_scan_observation_version",
                "gallery_observation_scan_source_file_count",
            ),
            (
                "catalog_gallery_observation_scan_anchors",
                "catalog_gallery_observation_scan_seals",
                "catalog_gallery_observation_scan_observation_sha256s",
                "catalog_gallery_observation_scan_observation_versions",
                "catalog_gallery_observation_scan_source_file_counts",
            ),
        ),
        (
            "gallery_observation_directory",
            "catalog_gallery_observation_directories",
            (
                "gallery_observation_directory_anchor",
                "gallery_observation_directory_seal",
                "gallery_observation_directory_entry_count",
                "gallery_observation_directory_observation_sha256",
            ),
            (
                "catalog_gallery_observation_directory_anchors",
                "catalog_gallery_observation_directory_seals",
                "catalog_gallery_observation_directory_entry_counts",
                "catalog_gallery_observation_directory_observation_sha256s",
            ),
        ),
        (
            "gallery_observation_stat",
            "catalog_gallery_observation_stat",
            (
                "gallery_observation_stat_anchor",
                "gallery_observation_stat_seal",
                "gallery_observation_stat_file_count",
                "gallery_observation_stat_byte_count",
            ),
            (
                "catalog_gallery_observation_stat_anchors",
                "catalog_gallery_observation_stat_seals",
                "catalog_gallery_observation_stat_file_counts",
                "catalog_gallery_observation_stat_byte_counts",
            ),
        ),
        (
            "gallery_observation_file_filesystem",
            "catalog_gallery_observation_file_filesystem",
            (
                "gallery_observation_file_filesystem_anchor",
                "gallery_observation_file_filesystem_seal",
                "gallery_observation_file_filesystem_device",
                "gallery_observation_file_filesystem_inode",
                "gallery_observation_file_filesystem_modified_ns",
                "gallery_observation_file_filesystem_changed_ns",
            ),
            (
                "catalog_gallery_observation_file_filesystem_anchors",
                "catalog_gallery_observation_file_filesystem_seals",
                "catalog_gallery_observation_file_filesystem_devices",
                "catalog_gallery_observation_file_filesystem_inodes",
                "catalog_gallery_observation_file_filesystem_modified_nses",
                "catalog_gallery_observation_file_filesystem_changed_nses",
            ),
        ),
        (
            "file_name_identity",
            "catalog_file_name_identities",
            (
                "file_name_identity_anchor",
                "file_name_identity_seal",
                "file_name_identity_name_bytes",
                "file_name_identity_file_role",
            ),
            (
                "catalog_file_name_identity_anchors",
                "catalog_file_name_identity_seals",
                "catalog_file_name_identity_name_bytes",
                "catalog_file_name_identity_file_roles",
            ),
        ),
        (
            "gallery_observation_file",
            "catalog_gallery_observation_files",
            (
                "gallery_observation_file_anchor",
                "gallery_observation_file_seal",
                "gallery_observation_file_file_no",
                "gallery_observation_file_file_sha256",
            ),
            (
                "catalog_gallery_observation_file_anchors",
                "catalog_gallery_observation_file_seals",
                "catalog_gallery_observation_file_file_nos",
                "catalog_gallery_observation_file_file_sha256s",
            ),
        ),
        (
            "tag_term",
            "catalog_tag_terms",
            ("tag_term_anchor", "tag_term_seal", "tag_term_identity"),
            (
                "catalog_tag_term_anchors",
                "catalog_tag_term_seals",
                "catalog_tag_term_identities",
            ),
        ),
        (
            "source_build",
            "catalog_source_builds",
            (
                "source_build_anchor",
                "source_build_descriptor_seal",
                "source_build_scope_key",
                "source_build_manifest_policy_id",
                "source_build_state",
                "source_build_created_at",
                "source_build_sealed_at",
            ),
            (
                "catalog_source_build_anchors",
                "catalog_source_build_descriptor_seals",
                "catalog_source_build_scope_keys",
                "catalog_source_build_manifest_policy_ids",
                "catalog_source_build_states",
                "catalog_source_build_created_ats",
                "catalog_source_build_sealed_ats",
            ),
        ),
        (
            "build_manifest",
            "catalog_build_manifests",
            (
                "build_manifest_anchor",
                "build_manifest_seal",
                "build_manifest_manifest_sha256",
                "source_build_discovery_gallery_count",
                "build_manifest_file_count",
                "build_manifest_byte_count",
                "source_build_sealed_at",
            ),
            (
                "catalog_build_manifest_anchors",
                "catalog_build_manifest_seals",
                "catalog_build_manifest_manifest_sha256s",
                "catalog_source_build_discovery_gallery_counts",
                "catalog_build_manifest_file_counts",
                "catalog_build_manifest_byte_counts",
                "catalog_source_build_sealed_ats",
            ),
        ),
        (
            "gallery_manifest",
            "catalog_gallery_manifests",
            (
                "gallery_manifest_anchor",
                "gallery_manifest_seal",
                "gallery_manifest_manifest_sha256",
                "gallery_manifest_computed_at",
            ),
            (
                "catalog_gallery_manifest_anchors",
                "catalog_gallery_manifest_seals",
                "catalog_gallery_manifest_manifest_sha256s",
                "catalog_gallery_manifest_computed_ats",
            ),
        ),
        (
            "source_snapshot_manifest_identity",
            "catalog_source_snapshot_manifest_identity",
            (
                "source_snapshot_manifest_identity_anchor",
                "source_snapshot_manifest_identity_seal",
                "source_snapshot_manifest_identity_gallery_count",
                "source_snapshot_manifest_identity_file_count",
                "source_snapshot_manifest_identity_byte_count",
            ),
            (
                "catalog_source_snapshot_manifest_identity_anchors",
                "catalog_source_snapshot_manifest_identity_seals",
                "catalog_source_snapshot_manifest_identity_gallery_counts",
                "catalog_source_snapshot_manifest_identity_file_counts",
                "catalog_source_snapshot_manifest_identity_byte_counts",
            ),
        ),
        (
            "analysis_run",
            "catalog_analysis_runs",
            (
                "analysis_run_anchor",
                "analysis_run_descriptor_seal",
                "analysis_run_build_id",
                "analysis_run_policy_id",
                "analysis_run_input_manifest_sha256",
                "analysis_run_identity",
                "analysis_run_started_at",
                "analysis_run_state",
                "analysis_run_completed_at",
            ),
            (
                "catalog_analysis_run_anchors",
                "catalog_analysis_run_descriptor_seals",
                "catalog_analysis_run_build_ids",
                "catalog_analysis_run_policy_ids",
                "catalog_analysis_run_input_manifest_sha256s",
                "catalog_analysis_run_identities",
                "catalog_analysis_run_started_ats",
                "catalog_analysis_run_states",
                "catalog_analysis_run_completed_ats",
            ),
        ),
        (
            "analysis_state_anchor",
            "catalog_analysis_state_anchors",
            ("analysis_state_ancestry",),
            ("catalog_analysis_state_ancestry",),
        ),
        (
            "analysis_state_component_seal",
            "catalog_analysis_state_component_seals",
            (
                "analysis_state_component_anchor",
                "analysis_state_component_completion_seal",
                "analysis_state_component_row_count",
                "analysis_state_component_sealed_at",
            ),
            (
                "catalog_analysis_state_component_anchors",
                "catalog_analysis_state_component_completion_seals",
                "catalog_analysis_state_component_row_counts",
                "catalog_analysis_state_component_sealed_ats",
            ),
        ),
        (
            "analysis_exclusion_delta",
            "catalog_analysis_exclusion_deltas",
            (
                "analysis_exclusion_delta_anchor",
                "analysis_exclusion_delta_seal",
                "analysis_exclusion_delta_old_excluded",
                "analysis_exclusion_delta_new_excluded",
            ),
            (
                "catalog_analysis_exclusion_delta_anchors",
                "catalog_analysis_exclusion_delta_seals",
                "catalog_analysis_exclusion_delta_old_excluded_flags",
                "catalog_analysis_exclusion_delta_new_excluded_flags",
            ),
        ),
    ],
)
def test_generated_first_batch_views_are_exact_sealed_vertical_joins(
    backend: str,
    relation_name: str,
    table: str,
    dependencies: tuple[str, ...],
    physical_dependencies: tuple[str, ...],
) -> None:
    payload = ARTIFACT_DATA["backends"][backend]
    relation = next(
        value for value in payload["relations"] if value["relation"] == relation_name
    )
    assert relation["kind"] == "view"
    assert relation["table"] == table
    assert relation["view_dependencies"] == dependencies
    statements = dict(payload["slices"])[f"relation:{relation_name}"]
    assert len(statements) == 1
    _statement_id, object_kind, object_name, sql = statements[0]
    assert object_kind == "view"
    assert object_name == table
    for physical_dependency in physical_dependencies:
        assert physical_dependency in sql


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_generated_source_build_optional_time_is_constrained_and_non_null(
    backend: str,
) -> None:
    payload = ARTIFACT_DATA["backends"][backend]
    source_build_sql = dict(payload["slices"])["relation:source_build"][0][3]
    assert "LEFT JOIN" in source_build_sql
    assert "'SEALED'" in source_build_sql
    assert "'OPEN', 'ABANDONED'" in source_build_sql
    assert "sealed_at" in source_build_sql
    sealed_at = next(
        relation
        for relation in payload["relations"]
        if relation["relation"] == "source_build_sealed_at"
    )
    assert (
        next(column for column in sealed_at["columns"] if column[0] == "sealed_at")[3]
        is False
    )
    build_manifest_sql = dict(payload["slices"])["relation:build_manifest"][0][3]
    assert (
        'member_5."sealed_at" AS "computed_at"'
        if backend == "sqlite"
        else "member_5.`sealed_at` AS `computed_at`"
    ) in build_manifest_sql


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_generated_analysis_run_optional_completion_is_constrained(
    backend: str,
) -> None:
    payload = ARTIFACT_DATA["backends"][backend]
    sql = dict(payload["slices"])["relation:analysis_run"][0][3]
    assert "LEFT JOIN" in sql
    assert "'COMPLETE'" in sql
    assert "'OPEN', 'ABANDONED'" in sql
    assert "completed_at" in sql


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
@pytest.mark.parametrize(
    ("relation_name", "table", "dependencies", "physical_dependencies"),
    [
        (
            "artifact_producer_fingerprint",
            "catalog_artifact_producer_fingerprints",
            (
                "artifact_producer_fingerprint_anchor",
                "artifact_producer_fingerprint_seal",
                "artifact_producer_fingerprint_algorithm_version",
                "artifact_producer_fingerprint_equivalence_class",
                "artifact_producer_fingerprint_identity",
            ),
            (
                "catalog_artifact_producer_fingerprint_anchors",
                "catalog_artifact_producer_fingerprint_seals",
                "catalog_artifact_producer_fingerprint_algorithm_versions",
                "catalog_artifact_producer_fingerprint_equivalence_classes",
                "catalog_artifact_producer_fingerprint_identities",
            ),
        ),
        (
            "source_build_base_source",
            "catalog_source_build_base_source",
            (
                "source_build_base_publication_commit",
                "publication_commit",
            ),
            (
                "catalog_source_build_base_publication_commits",
                "catalog_publication_commits",
            ),
        ),
        (
            "publication_candidate_base_source",
            "catalog_publication_candidate_base_sources",
            (
                "publication_candidate_base_publication_commit",
                "publication_commit",
            ),
            (
                "catalog_publication_candidate_base_publication_commits",
                "catalog_publication_commits",
            ),
        ),
        (
            "publication_candidate_base_catalog",
            "catalog_publication_candidate_base_catalog",
            (
                "publication_candidate_base_publication_commit",
                "publication_commit",
            ),
            (
                "catalog_publication_candidate_base_publication_commits",
                "catalog_publication_commits",
            ),
        ),
        (
            "source_head",
            "catalog_source_heads",
            ("publication_commit_head",),
            ("catalog_publication_commit_heads",),
        ),
        (
            "publication_head",
            "catalog_publication_heads",
            ("publication_commit_head",),
            ("catalog_publication_commit_heads",),
        ),
        (
            "source_revision",
            "catalog_source_revisions",
            ("source_revision_descriptor", "publication_commit"),
            ("catalog_source_revision_descriptors", "catalog_publication_commits"),
        ),
        (
            "catalog_revision",
            "catalog_revisions",
            ("catalog_revision_descriptor", "publication_commit"),
            ("catalog_revision_descriptors", "catalog_publication_commits"),
        ),
        (
            "analysis_batch_receipt",
            "catalog_analysis_batch_receipts",
            ("analysis_batch_receipt_stored",),
            ("catalog_analysis_batch_receipt_stored",),
        ),
        (
            "publication_batch_receipt",
            "catalog_publication_batch_receipts",
            ("publication_batch_receipt_stored",),
            ("catalog_publication_batch_receipt_stored",),
        ),
        (
            "publication_commit_head",
            "catalog_publication_commit_heads",
            (
                "publication_commit_head_receipt",
                "publication_commit",
                "source_revision_descriptor",
            ),
            (
                "catalog_publication_commit_head_receipts",
                "catalog_publication_commits",
                "catalog_source_revision_descriptors",
            ),
        ),
        (
            "publication_receipt",
            "catalog_publication_receipts",
            (
                "publication_commit",
                "catalog_revision_descriptor",
                "source_revision_descriptor",
                "publication_commit_finalization",
                "publication_finalization_checkpoint",
                "publication_finalization_batch_receipt",
            ),
            (
                "catalog_publication_commits",
                "catalog_revision_descriptors",
                "catalog_source_revision_descriptors",
                "catalog_publication_commit_finalizations",
                "catalog_publication_finalization_checkpoint",
                "catalog_publication_finalization_batch_receipt",
            ),
        ),
        (
            "operational_activation",
            "operational_operational_activations",
            (
                "publication_commit_seal",
                "publication_commit_source_revision",
                "publication_commit_operational_preparation",
                "publication_commit_operational_policy",
                "publication_commit_committed_at",
            ),
            (
                "catalog_publication_commit_seals",
                "catalog_publication_commit_source_revisions",
                "catalog_publication_commit_operational_preparations",
                "catalog_publication_commit_operational_policies",
                "catalog_publication_commit_committed_ats",
            ),
        ),
    ],
)
def test_generated_batch_zero_views_are_exact_on_both_backends(
    backend: str,
    relation_name: str,
    table: str,
    dependencies: tuple[str, ...],
    physical_dependencies: tuple[str, ...],
) -> None:
    payload = ARTIFACT_DATA["backends"][backend]
    relation = next(
        value for value in payload["relations"] if value["relation"] == relation_name
    )
    assert relation["kind"] == "view"
    assert relation["table"] == table
    assert relation["view_dependencies"] == dependencies
    statements = dict(payload["slices"])[f"relation:{relation_name}"]
    assert len(statements) == 1
    _statement_id, object_kind, object_name, sql = statements[0]
    assert object_kind == "view"
    assert object_name == table
    expected_prefix = (
        "CREATE VIEW IF NOT EXISTS"
        if backend == "sqlite"
        else "CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS"
    )
    assert sql.startswith(expected_prefix)
    for physical_dependency in physical_dependencies:
        assert physical_dependency in sql


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_generated_discovery_completed_at_member_is_non_null(backend: str) -> None:
    relation = next(
        value
        for value in ARTIFACT_DATA["backends"][backend]["relations"]
        if value["relation"] == "source_build_discovery_completed_at"
    )
    completed_at = next(
        column for column in relation["columns"] if column[0] == "completed_at"
    )
    assert completed_at[3] is False


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_generated_analysis_component_sealed_at_member_is_non_null(
    backend: str,
) -> None:
    relation = next(
        value
        for value in ARTIFACT_DATA["backends"][backend]["relations"]
        if value["relation"] == "analysis_state_component_sealed_at"
    )
    sealed_at = next(
        column for column in relation["columns"] if column[0] == "sealed_at"
    )
    assert sealed_at[3] is False


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_generated_dependency_order_and_fk_targets_are_closed(backend: str) -> None:
    payload = ARTIFACT_DATA["backends"][backend]
    relations = {value["relation"]: value for value in payload["relations"]}
    by_table = {value["table"]: value for value in payload["relations"]}
    position = {
        relation_name: index
        for index, relation_name in enumerate(ARTIFACT_DATA["relation_order"])
    }

    for relation_name, relation in relations.items():
        for _name, _columns, target_table, target_columns in relation["foreign_keys"]:
            target = by_table[target_table]
            assert position[target["relation"]] < position[relation_name]
            sql_keys = {
                target["primary_key"],
                *target["unique_keys"],
                *target["referential_unique_keys"],
            }
            assert target_columns in sql_keys
        for dependency in relation["view_dependencies"]:
            assert position[dependency] < position[relation_name]

    slices = payload["slices"]
    assert tuple(value[0].removeprefix("relation:") for value in slices) == tuple(
        ARTIFACT_DATA["relation_order"]
    )
    declared_objects = tuple(
        (kind, name)
        for _slice_id, statements in slices
        for _statement_id, kind, name, _sql in statements
    )
    assert tuple(sorted(declared_objects)) == payload["expected_objects"]
    assert len(declared_objects) == len(set(declared_objects))
    if backend == "mariadb":
        assert all(kind != "index" for kind, _name in declared_objects)
    else:
        assert any(kind == "index" for kind, _name in declared_objects)


def test_formal_seed_and_obligation_contracts_are_machine_bound() -> None:
    from h2hdb import catalog_writer

    data = _load(DATA_PHYSICAL)
    operational = _load(OPERATIONAL_PHYSICAL)
    expected_obligation_ids = tuple(
        value["id"]
        for document in (data, operational)
        for value in document.get("semantic_obligation", ())
    )
    expected_recurring_obligation_ids = tuple(
        value["id"]
        for document in (data, operational)
        for value in document.get("semantic_obligation", ())
        if value["lifecycle"] != "building_only"
    )
    expected_seed_ids = tuple(
        seed_id
        for document in (data, operational)
        for seed_id in (
            *(value["id"] for value in document.get("bootstrap_seed", ())),
            *(
                f"{value['id']}.shard-{shard_no:03d}"
                for value in document.get("bootstrap_seed_range", ())
                for shard_no in range(256)
            ),
        )
    )

    assert (
        tuple(value["id"] for value in ARTIFACT_DATA["semantic_obligations"])
        == expected_obligation_ids
    )
    assert tuple(value["id"] for value in ARTIFACT_DATA["bootstrap_seeds"]) == (
        expected_seed_ids
    )
    assert expected_obligation_ids
    assert expected_seed_ids
    assert len(ARTIFACT_DATA["obligation_manifest_sha256"]) == 64
    cleanup_seeds = tuple(
        value
        for value in ARTIFACT_DATA["bootstrap_seeds"]
        if value["relation"] == "cleanup_sweep_target"
    )
    cleanup_ranges = tuple(operational.get("bootstrap_seed_range", ()))
    assert cleanup_ranges
    assert len(cleanup_seeds) == len(cleanup_ranges) * 256
    cleanup_keys = tuple(bytes.fromhex(value["value"][2][3]) for value in cleanup_seeds)
    assert len(set(cleanup_keys)) == len(cleanup_keys)
    assert all(len(value) == 32 for value in cleanup_keys)

    for backend in ("sqlite", "mariadb"):
        provider = GeneratedVNextSchemaProvider(backend)
        payload = ARTIFACT_DATA["backends"][backend]
        assert tuple(value["seed_id"] for value in payload["bootstrap_seeds"]) == (
            expected_seed_ids
        )
        assert len(payload["seed_manifest_sha256"]) == 64
        assert tuple(provider.semantic_validators) == (
            expected_recurring_obligation_ids
        )
        assert tuple(provider.writer_hook_bindings) == tuple(
            catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS
        )
        assert tuple(provider.writer_hook_bindings) == (
            expected_recurring_obligation_ids
        )
        assert len(provider.writer_hook_bindings) == 25
        assert not provider.blockers
        assert not any("validators are missing" in value for value in provider.blockers)
        assert not any("undeclared IDs" in value for value in provider.blockers)
        assert provider.definition.epoch == ARTIFACT_DATA["epoch"]


def test_generated_provider_rejects_caller_supplied_semantic_validators() -> None:
    with pytest.raises(TypeError):
        GeneratedVNextSchemaProvider(  # type: ignore[call-arg]
            "sqlite",
            {"catalog.identity-codecs.v1": lambda _connector: None},
        )

    provider = GeneratedVNextSchemaProvider("sqlite")
    expected_ids = tuple(
        value["id"]
        for value in ARTIFACT_DATA["semantic_obligations"]
        if value["contract"]["lifecycle"] != "building_only"
    )
    assert tuple(provider.semantic_validators) == expected_ids
    with pytest.raises(TypeError):
        cast(dict[str, Any], provider.semantic_validators)[
            "catalog.identity-codecs.v1"
        ] = lambda _connector: None
    with pytest.raises(TypeError):
        cast(dict[str, Any], provider.writer_hook_bindings)[
            "catalog.identity-codecs.v1"
        ] = object()
    assert provider.definition.schema_version == ARTIFACT_DATA["schema_version"]


def test_generated_provider_reports_every_recurring_writer_hook_exactly() -> None:
    from h2hdb import catalog_writer

    provider = GeneratedVNextSchemaProvider("sqlite")
    recurring = tuple(
        value
        for value in ARTIFACT_DATA["semantic_obligations"]
        if value["contract"]["lifecycle"] != "building_only"
    )
    building_only_ids = tuple(
        value["id"]
        for value in ARTIFACT_DATA["semantic_obligations"]
        if value["contract"]["lifecycle"] == "building_only"
    )
    writer_blockers = tuple(
        blocker
        for blocker in provider.blockers
        if blocker.startswith("semantic obligation ") and " writer hook " in blocker
    )

    installed_ids = frozenset(catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS)
    unresolved = tuple(
        obligation for obligation in recurring if obligation["id"] not in installed_ids
    )

    assert len(recurring) == 25
    assert len(installed_ids) == 25
    assert len(writer_blockers) == len(unresolved) == 0
    assert installed_ids == frozenset(value["id"] for value in recurring)
    assert installed_ids.isdisjoint(building_only_ids)


def test_generated_provider_derives_writer_blockers_from_wheel_resolver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2hdb import catalog_writer

    recurring = tuple(
        value
        for value in ARTIFACT_DATA["semantic_obligations"]
        if value["contract"]["lifecycle"] != "building_only"
    )
    expected_calls = tuple(
        (
            value["id"],
            value["contract"]["writer_hook"],
            value["contract"]["writer_hook_version"],
        )
        for value in recurring
    )
    target_id = next(iter(catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS))
    unavailable_id, unavailable_name, unavailable_version = next(
        value for value in expected_calls if value[0] == target_id
    )
    calls: list[tuple[str, str, int]] = []
    original_resolver = catalog_writer.resolve_writer_hook

    def resolve(obligation_id: str, name: str, version: int) -> object:
        calls.append((obligation_id, name, version))
        if (obligation_id, name, version) == (
            unavailable_id,
            unavailable_name,
            unavailable_version,
        ):
            raise catalog_writer.WriterHookUnavailableError("probe unavailable")
        return original_resolver(obligation_id, name, version)

    monkeypatch.setattr(catalog_writer, "resolve_writer_hook", resolve)
    provider = GeneratedVNextSchemaProvider("sqlite")
    writer_blockers = tuple(
        blocker
        for blocker in provider.blockers
        if blocker.startswith("semantic obligation ") and " writer hook " in blocker
    )

    assert tuple(calls) == expected_calls
    assert len(writer_blockers) == 1
    probe_blocker = next(
        blocker for blocker in writer_blockers if "probe unavailable" in blocker
    )
    assert unavailable_id in probe_blocker
    assert unavailable_name in probe_blocker
    assert f"v{unavailable_version}" in probe_blocker
    with pytest.raises(VNextSchemaProviderUnavailableError, match="probe unavailable"):
        _ = provider.definition


def test_generated_provider_rejects_a_noncanonical_writer_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2hdb import catalog_writer

    target_id, canonical = next(
        iter(catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS.items())
    )
    original_resolver = catalog_writer.resolve_writer_hook

    def resolve(obligation_id: str, name: str, version: int) -> object:
        binding = original_resolver(obligation_id, name, version)
        return replace(binding) if obligation_id == target_id else binding

    monkeypatch.setattr(catalog_writer, "resolve_writer_hook", resolve)
    provider = GeneratedVNextSchemaProvider("sqlite")

    assert target_id not in provider.writer_hook_bindings
    assert len(provider.writer_hook_bindings) == 24
    assert any(
        repr(target_id) in blocker and "non-canonical binding" in blocker
        for blocker in provider.blockers
    )
    assert canonical is catalog_writer.BUILTIN_WRITER_HOOK_BINDINGS[target_id]
    with pytest.raises(
        VNextSchemaProviderUnavailableError,
        match="non-canonical binding",
    ):
        _ = provider.definition


def test_generated_provider_rejects_registry_omission_and_noop_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2hdb import catalog_refinement

    installed = dict(catalog_refinement.builtin_semantic_validators())
    missing_id = next(iter(installed))
    monkeypatch.setattr(
        catalog_refinement,
        "builtin_semantic_validators",
        lambda: {
            obligation_id: validator
            for obligation_id, validator in installed.items()
            if obligation_id != missing_id
        },
    )
    missing_provider = GeneratedVNextSchemaProvider("sqlite")
    assert any(
        "validators are missing" in blocker and repr(missing_id) in blocker
        for blocker in missing_provider.blockers
    )
    with pytest.raises(VNextSchemaProviderUnavailableError):
        _ = missing_provider.definition

    unexpected_id = "caller.noop.v1"
    monkeypatch.setattr(
        catalog_refinement,
        "builtin_semantic_validators",
        lambda: {**installed, unexpected_id: lambda _connector: None},
    )
    extra_provider = GeneratedVNextSchemaProvider("sqlite")
    assert any(
        "undeclared IDs" in blocker and repr(unexpected_id) in blocker
        for blocker in extra_provider.blockers
    )
    with pytest.raises(VNextSchemaProviderUnavailableError):
        _ = extra_provider.definition


def test_generated_ready_validation_never_scans_all_foreign_key_rows() -> None:
    source = (ROOT / "src" / "h2hdb" / "vnext_schema_provider.py").read_text()
    assert "PRAGMA foreign_key_check" not in source.replace(
        "Do not run PRAGMA foreign_key_check", ""
    )


def test_generated_mariadb_constraint_names_use_portable_identifier_codec() -> None:
    value = "uk_operational_gallery_observation_staging_request_predecessors_1"
    expected = f"{value[:50]}_{hashlib.sha256(value.encode('ascii')).hexdigest()[:12]}"
    assert len(expected.encode("ascii")) == 63
    assert provider_module._ddl_identifier(value) == expected


def test_generated_seed_statements_are_backend_specific_and_idempotent() -> None:
    sqlite_seeds = ARTIFACT_DATA["backends"]["sqlite"]["bootstrap_seeds"]
    mariadb_seeds = ARTIFACT_DATA["backends"]["mariadb"]["bootstrap_seeds"]
    assert tuple(value["parameters"] for value in sqlite_seeds) == tuple(
        value["parameters"] for value in mariadb_seeds
    )
    assert all(" ON CONFLICT " in value["sql"] for value in sqlite_seeds)
    assert all(value["sql"].endswith(" DO NOTHING") for value in sqlite_seeds)
    assert all(" ON DUPLICATE KEY UPDATE " in value["sql"] for value in mariadb_seeds)
    assert all(
        SchemaSeedStatement(
            seed_id=value["seed_id"],
            target_table=value["target_table"],
            sql=value["sql"],
            parameters=value["parameters"],
        )
        for value in (*sqlite_seeds, *mariadb_seeds)
    )


def test_sqlite_bootstrap_validation_is_exact(tmp_path: Path) -> None:
    payload = ARTIFACT_DATA["backends"]["sqlite"]
    connector = SQLiteConnector(str(tmp_path / "generated-seeds.sqlite3"))
    connector.connect()
    try:
        for _slice_id, statements in payload["slices"]:
            for _statement_id, _kind, _name, sql in statements:
                connector.execute(sql)
        for seed in payload["bootstrap_seeds"]:
            connector.execute(seed["sql"], seed["parameters"])
            connector.execute(seed["sql"], seed["parameters"])

        expected_ids = tuple(value["seed_id"] for value in payload["bootstrap_seeds"])
        assert (
            provider_module._validate_bootstrap_seed_records(connector, payload)
            == expected_ids
        )

        connector.execute(
            "UPDATE operational_revision_allocators "
            "SET next_revision = next_revision + 1 WHERE stream = %s",
            ("SOURCE",),
        )
        with pytest.raises(SchemaEpochValidationError, match="exact generated row"):
            provider_module._validate_bootstrap_seed_records(connector, payload)
    finally:
        connector.close()


def test_generated_manifests_are_backend_specific_and_well_formed() -> None:
    assert ARTIFACT_DATA["artifact_version"] == 1
    assert ARTIFACT_DATA["epoch"] == 2
    assert ARTIFACT_DATA["schema_version"] == 1
    assert len(ARTIFACT_DATA["source_manifest_sha256"]) == 64
    sqlite_manifest = ARTIFACT_DATA["backends"]["sqlite"]["ddl_manifest_sha256"]
    mariadb_manifest = ARTIFACT_DATA["backends"]["mariadb"]["ddl_manifest_sha256"]
    assert len(sqlite_manifest) == len(mariadb_manifest) == 64
    assert sqlite_manifest != mariadb_manifest
    sqlite_seed_manifest = ARTIFACT_DATA["backends"]["sqlite"]["seed_manifest_sha256"]
    mariadb_seed_manifest = ARTIFACT_DATA["backends"]["mariadb"]["seed_manifest_sha256"]
    assert len(sqlite_seed_manifest) == len(mariadb_seed_manifest) == 64
    assert sqlite_seed_manifest != mariadb_seed_manifest


def test_mariadb_view_body_normalization_preserves_semantics() -> None:
    expected = """
        CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS `resolved_value`
            (`analysis_id`, `value`) AS
        SELECT path.`analysis_id` AS `analysis_id`, shadow.`value` AS `value`
        FROM `analysis_path` AS path
        JOIN `analysis_shadow` AS shadow
          ON shadow.`analysis_id` = path.`ancestor_analysis_id`
        WHERE NOT EXISTS (
          SELECT 1 FROM `analysis_tombstone` AS tomb
          WHERE tomb.`analysis_id` = path.`ancestor_analysis_id`
            AND tomb.`value` = shadow.`value`
        )
    """
    stored = """
        select `path`.`analysis_id` AS `analysis_id`,
               `shadow`.`value` AS `value`
        from (`catalog_test`.`analysis_path` `path`
        join `catalog_test`.`analysis_shadow` `shadow`
          on (`shadow`.`analysis_id` = `path`.`ancestor_analysis_id`))
        where !exists (
          select 1 from `catalog_test`.`analysis_tombstone` `tomb`
          where `tomb`.`analysis_id` = `path`.`ancestor_analysis_id`
            and `tomb`.`value` = `shadow`.`value`
          limit 1
        )
    """

    expected_tokens = provider_module._mariadb_view_body_tokens(
        provider_module._mariadb_expected_view_body(expected),
        database_name="catalog_test",
    )
    actual_tokens = provider_module._mariadb_view_body_tokens(
        stored,
        database_name="catalog_test",
    )
    assert actual_tokens == expected_tokens

    wrong = stored.replace("and `tomb`.`value`", "or `tomb`.`value`")
    assert (
        provider_module._mariadb_view_body_tokens(
            wrong,
            database_name="catalog_test",
        )
        != expected_tokens
    )

    # LIMIT 1 is semantics-neutral only at the tail of EXISTS.  It must remain
    # authoritative in a scalar subquery or at the outer SELECT level.
    scalar_limited = "SELECT (SELECT 1 LIMIT 1) AS `value`"
    scalar_unlimited = "SELECT (SELECT 1) AS `value`"
    assert provider_module._mariadb_view_body_tokens(
        scalar_limited,
        database_name="catalog_test",
    ) != provider_module._mariadb_view_body_tokens(
        scalar_unlimited,
        database_name="catalog_test",
    )

    with pytest.raises(
        SchemaEpochValidationError,
        match="unbalanced parentheses",
    ):
        provider_module._mariadb_view_body_tokens(
            stored + "(",
            database_name="catalog_test",
        )


def test_mariadb_view_body_normalization_accepts_left_deep_inner_joins() -> None:
    expected = """
        SELECT sealed.`value_sha256` AS `value_sha256`,
               member_1.`digest_domain` AS `digest_domain`,
               member_2.`byte_count` AS `byte_count`,
               member_3.`allocated_at` AS `allocated_at`
        FROM `catalog_canonical_value_allocation_seals` AS sealed
        JOIN `catalog_canonical_value_allocation_anchors` AS anchor
          ON anchor.`value_sha256` = sealed.`value_sha256`
        JOIN `catalog_canonical_value_allocation_digest_domains` AS member_1
          ON member_1.`value_sha256` = sealed.`value_sha256`
        JOIN `catalog_canonical_value_allocation_byte_counts` AS member_2
          ON member_2.`value_sha256` = sealed.`value_sha256`
        JOIN `catalog_canonical_value_allocation_allocated_ats` AS member_3
          ON member_3.`value_sha256` = sealed.`value_sha256`
    """
    stored = """
        SELECT `sealed`.`value_sha256` AS `value_sha256`,
               `member_1`.`digest_domain` AS `digest_domain`,
               `member_2`.`byte_count` AS `byte_count`,
               `member_3`.`allocated_at` AS `allocated_at`
        FROM ((((`catalog_test`.`catalog_canonical_value_allocation_seals` `sealed`
        JOIN `catalog_test`.`catalog_canonical_value_allocation_anchors` `anchor`
          ON (`anchor`.`value_sha256` = `sealed`.`value_sha256`))
        JOIN `catalog_test`.`catalog_canonical_value_allocation_digest_domains`
             `member_1`
          ON (`member_1`.`value_sha256` = `sealed`.`value_sha256`))
        JOIN `catalog_test`.`catalog_canonical_value_allocation_byte_counts`
             `member_2`
          ON (`member_2`.`value_sha256` = `sealed`.`value_sha256`))
        JOIN `catalog_test`.`catalog_canonical_value_allocation_allocated_ats`
             `member_3`
          ON (`member_3`.`value_sha256` = `sealed`.`value_sha256`))
    """

    expected_tokens = provider_module._mariadb_view_body_tokens(
        expected,
        database_name="catalog_test",
    )
    assert (
        provider_module._mariadb_view_body_tokens(
            stored,
            database_name="catalog_test",
        )
        == expected_tokens
    )

    wrong_predicate = stored.replace(
        "`member_2`.`value_sha256` = `sealed`.`value_sha256`",
        "`member_2`.`value_sha256` <> `sealed`.`value_sha256`",
        1,
    )
    assert (
        provider_module._mariadb_view_body_tokens(
            wrong_predicate,
            database_name="catalog_test",
        )
        != expected_tokens
    )

    outer_join = stored.replace(
        "JOIN `catalog_test`.`catalog_canonical_value_allocation_byte_counts`",
        "LEFT JOIN `catalog_test`.`catalog_canonical_value_allocation_byte_counts`",
        1,
    )
    assert (
        provider_module._mariadb_view_body_tokens(
            outer_join,
            database_name="catalog_test",
        )
        != expected_tokens
    )


def test_mariadb_view_body_normalization_accepts_optional_member_storage() -> None:
    expected = """
        SELECT sealed.`build_id` AS `build_id`, optional.`sealed_at` AS `sealed_at`
        FROM `source_build_seals` AS sealed
        JOIN `source_build_anchors` AS anchor
          ON anchor.`build_id` = sealed.`build_id`
        LEFT JOIN `source_build_sealed_ats` AS optional
          ON optional.`build_id` = sealed.`build_id`
        WHERE (sealed.`state` = 'SEALED' AND optional.`sealed_at` IS NOT NULL
           OR sealed.`state` = 'OPEN' AND optional.`sealed_at` IS NULL)
    """
    stored = """
        SELECT `sealed`.`build_id` AS `build_id`,
               `optional`.`sealed_at` AS `sealed_at`
        FROM ((`catalog_test`.`source_build_seals` `sealed`
        JOIN `catalog_test`.`source_build_anchors` `anchor`
          ON (`anchor`.`build_id` = `sealed`.`build_id`))
        LEFT JOIN `catalog_test`.`source_build_sealed_ats` `optional`
          ON (`optional`.`build_id` = `sealed`.`build_id`))
        WHERE `sealed`.`state` = 'SEALED'
          AND `optional`.`sealed_at` IS NOT NULL
           OR `sealed`.`state` = 'OPEN' AND `optional`.`sealed_at` IS NULL
    """

    expected_tokens = provider_module._mariadb_view_body_tokens(
        expected,
        database_name="catalog_test",
    )
    assert (
        provider_module._mariadb_view_body_tokens(
            stored,
            database_name="catalog_test",
        )
        == expected_tokens
    )

    changed_join = stored.replace("LEFT JOIN", "JOIN", 1)
    assert (
        provider_module._mariadb_view_body_tokens(
            changed_join,
            database_name="catalog_test",
        )
        != expected_tokens
    )

    changed_grouping = stored.replace(
        "`optional`.`sealed_at` IS NOT NULL",
        "(`optional`.`sealed_at` IS NOT NULL",
        1,
    ).replace(
        "`sealed`.`state` = 'OPEN'",
        "`sealed`.`state` = 'OPEN')",
        1,
    )
    assert (
        provider_module._mariadb_view_body_tokens(
            changed_grouping,
            database_name="catalog_test",
        )
        != expected_tokens
    )


def test_mariadb_view_body_normalization_accepts_projection_addition_storage() -> None:
    expected = """
        SELECT (stored.`start_count` + stored.`row_count`) AS `next_count`,
               (stored.`generation` + 1) AS `next_generation`
        FROM `batch_receipt_stored` AS stored
    """
    stored = """
        SELECT `stored`.`start_count` + `stored`.`row_count` AS `next_count`,
               `stored`.`generation` + 1 AS `next_generation`
        FROM `catalog_test`.`batch_receipt_stored` `stored`
    """
    expected_tokens = provider_module._mariadb_view_body_tokens(
        expected,
        database_name="catalog_test",
    )
    assert (
        provider_module._mariadb_view_body_tokens(
            stored,
            database_name="catalog_test",
        )
        == expected_tokens
    )

    changed_operator = stored.replace(" + 1", " - 1", 1)
    assert (
        provider_module._mariadb_view_body_tokens(
            changed_operator,
            database_name="catalog_test",
        )
        != expected_tokens
    )

    changed_precedence = stored.replace(
        "`stored`.`row_count`",
        "`stored`.`row_count` * 2",
        1,
    )
    assert (
        provider_module._mariadb_view_body_tokens(
            changed_precedence,
            database_name="catalog_test",
        )
        != expected_tokens
    )


@pytest.mark.parametrize(
    "relation_name",
    (
        "analysis_batch_receipt",
        "publication_batch_receipt",
        "publication_receipt",
        "operational_activation",
    ),
)
def test_batch_zero_b_view_drift_remains_semantically_visible(
    relation_name: str,
) -> None:
    for backend in ("sqlite", "mariadb"):
        statements = dict(ARTIFACT_DATA["backends"][backend]["slices"])[
            f"relation:{relation_name}"
        ]
        sql = statements[0][3]
        if "THEN 'COMPLETE' ELSE 'OPEN'" in sql:
            wrong = sql.replace(
                "THEN 'COMPLETE' ELSE 'OPEN'",
                "THEN 'OPEN' ELSE 'COMPLETE'",
                1,
            )
        elif "THEN 'DB_COMMITTED' ELSE 'PROJECTION_FINALIZED'" in sql:
            wrong = sql.replace(
                "THEN 'DB_COMMITTED' ELSE 'PROJECTION_FINALIZED'",
                "THEN 'PROJECTION_FINALIZED' ELSE 'DB_COMMITTED'",
                1,
            )
        else:
            wrong = sql.replace(" = sealed.", " <> sealed.", 1)
        assert wrong != sql
        if backend == "sqlite":
            assert provider_module._normalize_sqlite_ddl(wrong) != (
                provider_module._normalize_sqlite_ddl(sql)
            )
        else:
            assert provider_module._mariadb_view_body_tokens(
                provider_module._mariadb_expected_view_body(wrong),
                database_name="catalog_test",
            ) != provider_module._mariadb_view_body_tokens(
                provider_module._mariadb_expected_view_body(sql),
                database_name="catalog_test",
            )


def test_mariadb_check_normalization_preserves_not_equal_literals() -> None:
    assert provider_module._normalize_check(
        "component != X'46494C45' AND label = 'literal != value'"
    ) == provider_module._normalize_check(
        "`component` <> x'46494c45' and `label` = 'literal != value'"
    )
    assert provider_module._normalize_check(
        "label = 'literal != value'"
    ) != provider_module._normalize_check("label = 'literal <> value'")
