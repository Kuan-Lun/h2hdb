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


def test_current_width_policy_is_exact_closed_world_and_debt_free() -> None:
    report = checker.check_current_policy()

    assert report.is_policy_clean
    assert report.is_fully_narrow
    assert len(report.relations) == 361
    assert report.violations == ()
    assert checker.GRANDFATHERED_WIDE_RELATIONS == {}
    assert {relation.table for relation in report.relations} == set(
        checker.NARROW_LAYOUT_DECLARATIONS
    )

    rendered = report.render()
    assert "relations=361, narrow=361, wide=0" in rendered
    assert "Width violations (complete):\n  (none)" in rendered


def test_complete_mode_is_mandatory_after_verticalization() -> None:
    report = checker.check_current_policy(require_complete=True)

    assert report.is_policy_clean
    assert report.is_fully_narrow


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
        grandfathered={},
        declarations={relation.table: declaration},
    )

    assert report.is_policy_clean
    assert report.is_fully_narrow


@pytest.mark.parametrize("owner", ("analysis", "publication"))
def test_compact_batch_family_roles_come_from_generic_vertical_metadata(
    owner: str,
) -> None:
    coordinate = checker.NARROW_LAYOUT_DECLARATIONS[
        f"catalog_{owner}_batch_receipt_coordinates"
    ]
    owner_attribute = "analysis_id" if owner == "analysis" else "candidate_id"
    assert coordinate.semantic_key == (owner_attribute, "stage", "batch_key")
    assert coordinate.semantic_value == ("start_generation",)

    row_count = checker.NARROW_LAYOUT_DECLARATIONS[
        f"catalog_{owner}_batch_receipt_row_counts"
    ]
    assert row_count.semantic_key == (
        owner_attribute,
        "stage",
        "start_generation",
    )
    assert row_count.semantic_value == ("row_count",)


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
        grandfathered={"catalog_first": ("left", "right")},
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
        grandfathered={"catalog_first": ("left", "different")},
        declarations={},
    )
    assert "grandfathered non-key columns changed" in changed.render()


def test_resolved_width_debt_requires_baseline_and_metadata_update() -> None:
    relation = _relation("catalog_resolved", ("id",), ("value",))

    report = checker.evaluate_policy(
        (relation,),
        grandfathered={"catalog_resolved": ("old_a", "old_b")},
        declarations={},
    )

    assert not report.is_policy_clean
    assert report.is_fully_narrow
    assert "width debt is resolved" in report.render()


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
        grandfathered={},
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
        grandfathered={},
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
        grandfathered={},
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
        grandfathered={},
        declarations={eav_relation.table: eav_declaration},
    )
    assert "generic attribute/value columns form an EAV layout" in eav_report.render()
