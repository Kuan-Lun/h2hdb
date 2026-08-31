from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "verification" / "schema" / "check_narrow_physical.py"


def _load_checker() -> ModuleType:
    specification = importlib.util.spec_from_file_location(
        "h2hdb_narrow_physical_checker", CHECKER
    )
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


checker = _load_checker()


def _relation(
    table: str,
    primary_key: tuple[str, ...],
    non_key_columns: tuple[str, ...],
    *,
    sqlite_value_type: str = "BLOB",
    mariadb_value_type: str = "VARBINARY(255)",
) -> Any:
    key = tuple(
        checker.ColumnShape(attribute, "INTEGER", "BIGINT UNSIGNED")
        for attribute in primary_key
    )
    values = tuple(
        checker.ColumnShape(attribute, sqlite_value_type, mariadb_value_type)
        for attribute in non_key_columns
    )
    return checker.RelationShape(table, primary_key, key + values)


def test_current_width_policy_is_exact_closed_world_with_reviewed_wide_bcnf_tables() -> (
    None
):
    report = checker.check_current_policy()

    assert report.is_policy_clean
    assert not report.is_fully_narrow
    assert len(report.relations) == 170
    assert tuple(relation.table for relation in report.violations) == (
        "catalog_analysis_batch_receipt_stored",
        "catalog_analysis_checkpoints",
        "catalog_analysis_content_owner_candidate_shadows",
        "catalog_analysis_policies",
        "catalog_analysis_run_descriptor",
        "catalog_analysis_stages",
        "catalog_analysis_state_component_seals",
        "catalog_artifact_policy_semantics",
        "catalog_artifact_semantic_inputs",
        "catalog_artifacts",
        "catalog_build_manifest_core",
        "catalog_contributor_facet_order",
        "catalog_display_title_policies",
        "catalog_gallery_identities",
        "catalog_gallery_manifests",
        "catalog_gallery_observation_directories",
        "catalog_gallery_observation_metadata_locals",
        "catalog_gallery_observation_scans",
        "catalog_gallery_observation_stat",
        "catalog_language_facet_order",
        "catalog_manifest_policies",
        "catalog_pages",
        "catalog_prepared_artifact_descriptors",
        "catalog_prepared_artifacts",
        "catalog_prepared_pages",
        "catalog_prepared_storage_objects",
        "catalog_prepared_thumbnails",
        "catalog_publication_batch_receipt_stored",
        "catalog_publication_candidates",
        "catalog_publication_checkpoints",
        "catalog_publication_commits",
        "catalog_publication_finalization_batch_stored",
        "catalog_publication_finalization_checkpoints",
        "catalog_publication_stages",
        "catalog_publication_storage",
        "catalog_revision_descriptors",
        "catalog_search_policies",
        "catalog_source_build_descriptor",
        "catalog_source_build_discoveries",
        "catalog_source_revision_descriptors",
        "catalog_source_scopes",
        "catalog_source_snapshot_manifest_identity",
        "catalog_storage_object_key_identities",
        "catalog_storage_objects",
        "catalog_subject_facet_order",
        "catalog_tag_terms",
        "catalog_thumbnails",
        "catalog_title_sort_policy",
    )
    assert checker.APPROVED_WIDE_BCNF_RELATIONS[
        "catalog_artifact_policy_semantics"
    ] == (
        "artifact_algorithm_version",
        "policy_fingerprint_sha256",
    )
    assert checker.APPROVED_WIDE_BCNF_RELATIONS[
        "catalog_storage_object_key_identities"
    ] == (
        "key_codec",
        "segment_count",
    )
    assert checker.APPROVED_WIDE_BCNF_RELATIONS["catalog_publication_commits"] == (
        "candidate_id",
        "revision",
        "source_revision",
        "generation",
        "preparation_id",
        "operational_policy_id",
        "artifact_policy_id",
        "display_title_policy_id",
        "new_galleries",
        "changed_galleries",
        "removed_galleries",
        "duplicate_losers",
        "committed_at",
    )
    assert {relation.table for relation in report.relations} == set(
        checker.NARROW_LAYOUT_DECLARATIONS
    ) | set(checker.APPROVED_WIDE_BCNF_RELATIONS)

    rendered = report.render()
    assert "relations=170, narrow=122, wide=48" in rendered
    assert "Approved wide relations (complete):" in rendered
    assert "catalog_gallery_identities" in rendered


def test_approved_wide_bcnf_relation_is_part_of_normal_policy() -> None:
    report = checker.check_current_policy()

    assert report.is_policy_clean
    assert not report.is_fully_narrow


def test_composite_primary_key_plus_one_value_is_narrow() -> None:
    relation = _relation(
        "catalog_gallery_upload_time",
        ("gallery_id", "observation_id"),
        ("upload_time",),
    )
    declaration = checker.NarrowLayoutDeclaration(
        semantic_key=("gallery_id", "observation_id"),
        semantic_value=("upload_time",),
    )

    report = checker.evaluate_policy(
        (relation,),
        approved_wide_bcnf={},
        declarations={relation.table: declaration},
    )

    assert report.is_policy_clean
    assert report.is_fully_narrow


@pytest.mark.parametrize(
    ("owner", "expected_non_key_columns"),
    (
        (
            "analysis",
            (
                "batch_key",
                "start_cursor",
                "start_processed_count",
                "page_limit",
                "next_cursor",
                "row_count",
                "committed_at",
            ),
        ),
        (
            "publication",
            (
                "batch_key",
                "start_cursor",
                "start_processed_count",
                "next_cursor",
                "row_count",
                "committed_at",
            ),
        ),
    ),
)
def test_recomposed_batch_receipts_are_reviewed_wide_bcnf_relations(
    owner: str,
    expected_non_key_columns: tuple[str, ...],
) -> None:
    table = f"catalog_{owner}_batch_receipt_stored"
    assert checker.APPROVED_WIDE_BCNF_RELATIONS[table] == expected_non_key_columns
    assert f"catalog_{owner}_batch_receipt_coordinates" not in (
        checker.NARROW_LAYOUT_DECLARATIONS
    )


def test_manifest_loader_excludes_views_and_non_catalog_tables() -> None:
    def raw_relation(table: str, *, kind: str | None = None) -> dict[str, object]:
        relation: dict[str, object] = {
            "table": table,
            "primary_key": ["id"],
            "column": [
                {
                    "attribute": "id",
                    "sqlite": {"type": "INTEGER"},
                    "mariadb": {"type": "BIGINT UNSIGNED"},
                },
                *(
                    {
                        "attribute": f"value_{index}",
                        "sqlite": {"type": "BLOB"},
                        "mariadb": {"type": "BLOB"},
                    }
                    for index in range(3)
                ),
            ],
        }
        if kind is not None:
            relation["kind"] = kind
        return relation

    relations = checker.catalog_base_relations(
        {
            "relation": [
                raw_relation("catalog_read_model", kind="view"),
                raw_relation("operational_work"),
                raw_relation("catalog_base"),
            ]
        }
    )

    assert [relation.table for relation in relations] == ["catalog_base"]


def test_new_or_changed_width_violation_fails_with_complete_report() -> None:
    first = _relation("catalog_first", ("id",), ("left", "right"))
    second = _relation("catalog_second", ("id",), ("x", "y", "z"))
    report = checker.evaluate_policy(
        (first, second),
        approved_wide_bcnf={"catalog_first": ("left", "right")},
        declarations={},
    )

    assert not report.is_policy_clean
    assert {relation.table for relation in report.violations} == {
        "catalog_first",
        "catalog_second",
    }
    rendered = report.render()
    assert "catalog_first" in rendered
    assert "catalog_second" in rendered
    assert "new width violation" in rendered

    changed = checker.evaluate_policy(
        (first,),
        approved_wide_bcnf={"catalog_first": ("left", "different")},
        declarations={},
    )
    assert "approved-wide non-key columns changed" in changed.render()


def test_resolved_approved_exception_requires_policy_metadata_update() -> None:
    relation = _relation("catalog_resolved", ("id",), ("value",))

    report = checker.evaluate_policy(
        (relation,),
        approved_wide_bcnf={"catalog_resolved": ("old_a", "old_b")},
        declarations={},
    )

    assert not report.is_policy_clean
    assert report.is_fully_narrow
    assert "approved-wide exception is no longer wide" in report.render()


def test_ordinary_value_cannot_be_hidden_in_physical_primary_key() -> None:
    relation = _relation(
        "catalog_key_cheat",
        ("entity_id", "ordinary_value"),
        (),
    )
    declaration = checker.NarrowLayoutDeclaration(
        semantic_key=("entity_id",),
        semantic_value=("ordinary_value",),
    )

    report = checker.evaluate_policy(
        (relation,),
        approved_wide_bcnf={},
        declarations={relation.table: declaration},
    )

    assert not report.is_policy_clean
    assert report.is_fully_narrow
    assert "do not move ordinary values into the primary key" in report.render()


def test_tuple_blob_requires_rejected_packing_metadata() -> None:
    relation = _relation("catalog_packed", ("id",), ("tuple_blob",))
    declaration = checker.NarrowLayoutDeclaration(
        semantic_key=("id",),
        semantic_value=("tuple_blob",),
        value_representation="packed_tuple",
    )

    report = checker.evaluate_policy(
        (relation,),
        approved_wide_bcnf={},
        declarations={relation.table: declaration},
    )

    assert "tuple/record packing is forbidden" in report.render()


def test_json_and_eav_cannot_disguise_multiple_values_as_one_column() -> None:
    json_relation = _relation(
        "catalog_json_cheat",
        ("id",),
        ("payload",),
        sqlite_value_type="JSON",
        mariadb_value_type="JSON",
    )
    json_declaration = checker.NarrowLayoutDeclaration(
        semantic_key=("id",),
        semantic_value=("payload",),
    )
    json_report = checker.evaluate_policy(
        (json_relation,),
        approved_wide_bcnf={},
        declarations={json_relation.table: json_declaration},
    )
    assert "JSON storage is forbidden" in json_report.render()

    eav_relation = _relation(
        "catalog_eav_cheat",
        ("entity_id", "attribute_name"),
        ("value",),
    )
    eav_declaration = checker.NarrowLayoutDeclaration(
        semantic_key=("entity_id", "attribute_name"),
        semantic_value=("value",),
    )
    eav_report = checker.evaluate_policy(
        (eav_relation,),
        approved_wide_bcnf={},
        declarations={eav_relation.table: eav_declaration},
    )
    assert "generic attribute/value columns form an EAV layout" in eav_report.render()
