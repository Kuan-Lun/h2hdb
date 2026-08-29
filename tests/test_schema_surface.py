from __future__ import annotations

import importlib.util
import io
import sys
import tarfile
import zipfile
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SURFACE_GATE = ROOT / "scripts" / "verify-schema-surface.py"
DISTRIBUTION_GATE = ROOT / "scripts" / "build-and-verify-distributions.py"


def _load_module(name: str, path: Path) -> ModuleType:
    specification = importlib.util.spec_from_file_location(name, path)
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


gate = _load_module("h2hdb_schema_surface_gate", SCHEMA_SURFACE_GATE)
distribution_gate = _load_module("h2hdb_distribution_gate", DISTRIBUTION_GATE)


def test_physical_manifests_are_the_only_relation_allowlist() -> None:
    allowed = gate.allowed_relations()

    assert "catalog_source_build_discoveries" in allowed
    assert "h2hdb_schema_epoch" in allowed
    assert "catalog_build_discoveries" not in allowed
    assert "h2hdb_schema_migrations" not in allowed


def test_python_scanner_uses_compiled_string_values_and_sql_positions() -> None:
    source = """
catalog_revision = "this is an application value, not a relation reference"
DIGEST_DOMAIN = "catalog_summary_utf8_v1"
QUERY = (
    "SELECT gallery_key "
    "FROM catalog_manifest_relation WHERE gallery_key = %s"
)
"""

    references = gate.references_in_python(source, source="fixture.py")

    assert [reference.relation for reference in references] == [
        "catalog_manifest_relation"
    ]


def test_python_scanner_covers_static_dynamic_sql_dispatch() -> None:
    source = """
_STATIC_TABLE = "catalog_static_relation"

def cleanup_specs():
    return direct("operational_cleanup_relation", ("relation_id",))
"""

    references = gate.references_in_python(source, source="fixture.py")

    assert {reference.relation for reference in references} == {
        "catalog_static_relation",
        "operational_cleanup_relation",
    }


def test_python_scanner_covers_cleanup_generator_dispatch_registry() -> None:
    source = """
def cleanup_specs():
    return tuple(
        direct(table, pk)
        for table, pk in (
            ("catalog_generator_relation", ("relation_id",)),
        )
    )
"""

    references = gate.references_in_python(
        source,
        source="vnext_cleanup_repository.py",
    )

    assert {reference.relation for reference in references} == {
        "catalog_generator_relation"
    }


def test_python_scanner_ignores_cleanup_enum_values() -> None:
    source = """
class CleanupKind:
    OPERATIONAL_PREPARATION = "OPERATIONAL_PREPARATION"
"""

    assert (
        gate.references_in_python(
            source,
            source="vnext_cleanup_repository.py",
        )
        == ()
    )


def test_python_scanner_does_not_treat_semantic_domains_as_relations() -> None:
    source = """
CATALOG_DIGEST_DOMAIN = "catalog_summary_utf8_v1"
value = register(digest_domain="operational_policy_id")
"""

    assert gate.references_in_python(source, source="fixture.py") == ()


def test_mutation_scanner_resolves_direct_and_module_constant_targets() -> None:
    source = """
READ_VIEW = "catalog_read_projection"
DIRECT = "DELETE FROM operational_read_projection WHERE id = %s"

def write(connector):
    connector.execute(
        f"INSERT INTO {READ_VIEW} (id) VALUES (%s)",
        (1,),
    )
"""

    mutations = gate.mutations_in_python(source, source="fixture.py")

    assert {(item.verb, item.relation) for item in mutations} == {
        ("delete from", "operational_read_projection"),
        ("insert into", "catalog_read_projection"),
    }


def test_mutation_scanner_covers_static_local_dispatch_targets() -> None:
    source = """
def write(connector):
    bindings = (
        ("catalog_first_component", "value"),
        ("catalog_second_component", "value"),
    )
    for table, column in bindings:
        connector.execute(f"INSERT INTO {table} ({column}) VALUES (%s)", (1,))
"""

    mutations = gate.mutations_in_python(source, source="fixture.py")

    assert {item.relation for item in mutations} == {
        "catalog_first_component",
        "catalog_second_component",
    }


def test_mutation_scanner_covers_static_helper_calls() -> None:
    source = """
WRITE_TABLE = "catalog_write_component"

def persist(work):
    _insert_or_compare(WRITE_TABLE, ("id",), (1,))

def cleanup_specs():
    return tuple(
        direct(table, ("id",))
        for table in ("catalog_cleanup_component",)
    )
"""

    mutations = gate.mutations_in_python(
        source,
        source="vnext_cleanup_repository.py",
    )

    assert {(item.verb, item.relation) for item in mutations} == {
        ("insert into", "catalog_write_component"),
        ("delete from", "catalog_cleanup_component"),
    }


def test_view_mutation_gate_rejects_dml_but_allows_table_dml() -> None:
    mutations = gate.mutations_in_sql(
        "INSERT INTO catalog_read_projection VALUES (1); "
        "UPDATE catalog_write_table SET value = 2",
        source="fixture.sql",
    )

    with pytest.raises(gate.SchemaSurfaceError, match="read-only manifest views"):
        gate.assert_no_view_mutations(
            mutations,
            kinds={
                "catalog_read_projection": "view",
                "catalog_write_table": "table",
            },
        )


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE TABLE catalog_manifest_relation (id INTEGER)",
        "CREATE VIEW catalog_manifest_relation AS SELECT 1",
        "SELECT 1 FROM catalog_manifest_relation",
        "SELECT 1 FROM app.catalog_manifest_relation",
        "SELECT 1 FROM x JOIN catalog_manifest_relation ON 1 = 1",
        "INSERT OR IGNORE INTO catalog_manifest_relation VALUES (1)",
        "UPDATE catalog_manifest_relation SET id = 1",
        "DELETE FROM catalog_manifest_relation",
        "ALTER TABLE catalog_manifest_relation ADD COLUMN value INTEGER",
        "DROP VIEW IF EXISTS catalog_manifest_relation",
        "CREATE TABLE x (id INTEGER REFERENCES catalog_manifest_relation(id))",
    ],
)
def test_scanner_covers_relation_bearing_sql_positions(statement: str) -> None:
    references = gate.references_in_sql(statement, source="fixture.sql")
    assert [reference.relation for reference in references] == [
        "catalog_manifest_relation"
    ]


def test_closed_world_gate_rejects_an_unmanifested_relation() -> None:
    references = gate.references_in_sql(
        "SELECT 1 FROM catalog_build_discoveries", source="legacy.sql"
    )

    with pytest.raises(gate.SchemaSurfaceError, match="catalog_build_discoveries"):
        gate.assert_closed_schema_surface(references, allowed=frozenset())


def test_wheel_archive_rejects_removed_legacy_modules(tmp_path: Path) -> None:
    wheel = tmp_path / "fixture.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("h2hdb/__init__.py", "")
        archive.writestr("h2hdb/migrations.py", "class MigrationRunner: pass\n")

    with pytest.raises(RuntimeError, match="removed legacy modules.*migrations.py"):
        distribution_gate._verify_wheel_archive(wheel)


def test_wheel_schema_gate_rejects_packaged_view_mutation(tmp_path: Path) -> None:
    wheel = tmp_path / "view-mutation.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(
            "h2hdb/writer.py",
            'VIEW = "catalog_read_projection"\n'
            'QUERY = f"UPDATE {VIEW} SET value = %s"\n',
        )

    mutations = gate.wheel_mutations(wheel)

    with pytest.raises(gate.SchemaSurfaceError, match="catalog_read_projection"):
        gate.assert_no_view_mutations(
            mutations,
            kinds={"catalog_read_projection": "view"},
        )


def test_sdist_archive_rejects_removed_legacy_modules(tmp_path: Path) -> None:
    sdist = tmp_path / "fixture.tar.gz"
    payload = b"class H2HDB: pass\n"
    with tarfile.open(sdist, "w:gz") as archive:
        member = tarfile.TarInfo("h2hdb-0/src/h2hdb/service.py")
        member.size = len(payload)
        archive.addfile(member, io.BytesIO(payload))

    with pytest.raises(RuntimeError, match="sdist.*removed legacy modules.*service.py"):
        distribution_gate._verify_sdist_archive(sdist)


def test_packaged_source_relation_surface_is_closed() -> None:
    gate.assert_closed_schema_surface(gate.source_references())


def test_packaged_source_never_mutates_a_manifest_view() -> None:
    gate.assert_no_view_mutations(gate.source_mutations())


def test_recomposed_b1_relations_are_atomic_base_tables() -> None:
    recomposed_tables = {
        "catalog_manifest_policies",
        "catalog_analysis_policies",
        "catalog_artifact_zip_writer_policies",
        "catalog_artifact_storage_codecs",
        "catalog_artifact_policy_semantics",
        "catalog_title_sort_policy",
        "catalog_display_title_policies",
        "catalog_source_scopes",
    }

    kinds = gate.relation_kinds()
    assert {relation: kinds[relation] for relation in recomposed_tables} == {
        relation: "table" for relation in recomposed_tables
    }


def test_catalog_hot_paths_do_not_read_b2_wide_views() -> None:
    wide_views = {
        "catalog_canonical_value_allocations",
        "catalog_canonical_value_pages",
        "catalog_canonical_value_page_descriptors",
        "catalog_gallery_observation_page_descriptors",
        "catalog_gallery_observation_page_key_bounds",
    }
    hot_modules = (
        "vnext_canonical_value_family.py",
        "vnext_canonical_value_repository.py",
        "vnext_gallery_staging_repository.py",
        "vnext_analysis_repository.py",
        "vnext_publication_candidate_repository.py",
        "vnext_publication_repository.py",
        "vnext_artifact_preparation_repository.py",
        "vnext_source_build_repository.py",
        "vnext_hash_cache_repository.py",
        "vnext_gallery_identity_repository.py",
        "vnext_cleanup_repository.py",
        "vnext_catalog_reader_repository.py",
    )

    references = {
        reference.relation
        for module_name in hot_modules
        for reference in gate.references_in_python(
            (ROOT / "src" / "h2hdb" / module_name).read_text(encoding="utf-8"),
            source=module_name,
        )
    }

    assert references.isdisjoint(wide_views)


def test_catalog_hot_paths_do_not_read_remaining_b3a_wide_views() -> None:
    wide_views = {
        "catalog_file_name_identities",
        "catalog_gallery_observation_files",
        "catalog_tag_terms",
    }
    hot_modules = (
        "vnext_catalog_identity_family.py",
        "vnext_gallery_identity_repository.py",
        "vnext_source_build_repository.py",
        "vnext_gallery_staging_repository.py",
        "vnext_analysis_repository.py",
        "vnext_artifact_preparation_repository.py",
        "vnext_publication_candidate_repository.py",
        "vnext_catalog_reader_repository.py",
        "vnext_cleanup_repository.py",
    )

    references = {
        reference.relation
        for module_name in hot_modules
        for reference in gate.references_in_python(
            (ROOT / "src" / "h2hdb" / module_name).read_text(encoding="utf-8"),
            source=module_name,
        )
    }

    assert references.isdisjoint(wide_views)


def test_catalog_hot_paths_do_not_read_b3b_wide_views() -> None:
    wide_views = {
        "catalog_source_builds",
        "catalog_build_manifests",
    }
    hot_modules = (
        "vnext_manifest_family.py",
        "vnext_source_build_repository.py",
        "vnext_gallery_identity_repository.py",
        "vnext_gallery_staging_repository.py",
        "vnext_analysis_repository.py",
        "vnext_artifact_preparation_repository.py",
        "vnext_publication_candidate_repository.py",
        "vnext_publication_repository.py",
        "vnext_operational_event_repository.py",
        "vnext_cleanup_repository.py",
    )

    references = {
        reference.relation
        for module_name in hot_modules
        for reference in gate.references_in_python(
            (ROOT / "src" / "h2hdb" / module_name).read_text(encoding="utf-8"),
            source=module_name,
        )
    }

    assert references.isdisjoint(wide_views)
