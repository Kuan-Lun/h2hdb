from __future__ import annotations

import hashlib
import importlib.util
import sqlite3
import subprocess
import sys
import tomllib
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from h2hdb import H2HDB, CoreConfig
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.sqlite_connector import SQLiteConnector

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "verification" / "schema" / "catalog.toml"
PHYSICAL = ROOT / "verification" / "schema" / "physical.toml"
REFINEMENT = ROOT / "verification" / "schema" / "refinement.py"
GENERATOR = ROOT / "verification" / "schema" / "generate_physical.py"


def _load_refinement() -> ModuleType:
    specification = importlib.util.spec_from_file_location(
        "h2hdb_schema_refinement", REFINEMENT
    )
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


refinement = _load_refinement()


def test_data_bootstrap_cells_are_exact_typed_ascii_enums() -> None:
    with PHYSICAL.open("rb") as stream:
        document = tomllib.load(stream)

    seeds = document["bootstrap_seed"]
    assert len(seeds) == 23
    for seed in seeds:
        assert seed["version"] == 1
        assert len(seed["value"]) == 1
        cell = seed["value"][0]
        assert set(cell) == {"attribute", "type", "encoding", "text"}
        assert cell["type"] == "ascii_enum"
        assert cell["encoding"] == "utf8"
        cell["text"].encode("ascii")


def test_data_runtime_obligation_bindings_are_an_exact_machine_bijection() -> None:
    with PHYSICAL.open("rb") as stream:
        document = tomllib.load(stream)

    obligations = document["semantic_obligation"]
    owners = {
        path: obligation["id"]
        for obligation in obligations
        for path in obligation["covers"]
        if not path.startswith("machine_contract.")
    }
    bindings = document["runtime_obligation_binding"]
    assert len(bindings) == len(owners) == len(document["runtime_obligations"]) == 56
    assert len({binding["path"] for binding in bindings}) == len(bindings)
    assert tuple(binding["text"] for binding in bindings) == tuple(
        document["runtime_obligations"]
    )
    assert {
        binding["path"]: binding["semantic_obligation_id"] for binding in bindings
    } == owners


def test_physical_loader_rejects_bootstrap_partition_drift(tmp_path: Path) -> None:
    logical = refinement.load_logical_schema(CATALOG)
    original = PHYSICAL.read_text(encoding="utf-8")
    invalid = original.replace(
        'seeded_relations = ["canonical_digest_policy", "channel_registry", '
        '"source_provider_registry"]',
        'seeded_relations = ["canonical_digest_policy", "channel_registry", '
        '"manifest_policy"]',
        1,
    )
    assert invalid != original
    invalid_path = tmp_path / "invalid-bootstrap-physical.toml"
    invalid_path.write_text(invalid, encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="seeded and absent relations overlap|seed exactly three registries",
    ):
        refinement.load_physical_schema(invalid_path, logical)


def _canonical_digest(payload: bytes, *, policy_id: int = 1) -> bytes:
    framing = policy_id.to_bytes(4, "big") + len(payload).to_bytes(8, "big") + payload
    return hashlib.sha256(framing).digest()


def _gallery_key(scope_key: bytes, gallery_name: bytes) -> bytes:
    framing = (
        (1).to_bytes(4, "big")
        + len(scope_key).to_bytes(4, "big")
        + scope_key
        + len(gallery_name).to_bytes(4, "big")
        + gallery_name
    )
    return hashlib.sha256(framing).digest()


class _SQLiteConnectionReader:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def fetch_all(
        self, query: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        return self.connection.execute(query, data).fetchall()


class _MariaDBMetadataFixture:
    """Small INFORMATION_SCHEMA fixture, not a SQL parser or DB emulator."""

    def fetch_all(
        self, query: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        assert data == ()
        if "INFORMATION_SCHEMA.TABLES" in query:
            return [("author",), ("book",)]
        if "INFORMATION_SCHEMA.COLUMNS" in query:
            return [
                ("author", "author_id", 1, "bigint unsigned", "NO", None),
                ("author", "email", 2, "varchar(255)", "NO", "utf8mb4_bin"),
                ("book", "book_id", 1, "bigint unsigned", "NO", None),
                ("book", "author_id", 2, "bigint unsigned", "NO", None),
                ("book", "isbn", 3, "char(13)", "NO", "ascii_bin"),
            ]
        if "INFORMATION_SCHEMA.STATISTICS" in query:
            return [
                ("author", "PRIMARY", 0, 1, "author_id"),
                ("author", "author_email", 0, 1, "email"),
                ("book", "PRIMARY", 0, 1, "book_id"),
                ("book", "book_isbn", 0, 1, "isbn"),
            ]
        if "INFORMATION_SCHEMA.KEY_COLUMN_USAGE" in query:
            return [("book", "book_author_fk", "author_id", 1, "author", "author_id")]
        if "INFORMATION_SCHEMA.CHECK_CONSTRAINTS" in query:
            return []
        raise AssertionError(f"unexpected metadata query: {query}")


def _book_logical_schema() -> Any:
    return refinement.LogicalSchema(
        "book-contract",
        (
            refinement.LogicalRelation(
                "author",
                ("author_id", "email"),
                (frozenset({"author_id"}), frozenset({"email"})),
                (),
            ),
            refinement.LogicalRelation(
                "book",
                ("book_id", "author_id", "isbn"),
                (frozenset({"book_id"}), frozenset({"isbn"})),
                (
                    refinement.LogicalForeignKey(
                        ("author_id",), "author", ("author_id",)
                    ),
                ),
            ),
        ),
    )


def test_sqlite_introspection_and_refinement_accept_matching_schema() -> None:
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript("""
            PRAGMA foreign_keys = ON;
            CREATE TABLE author (
                author_id INTEGER NOT NULL PRIMARY KEY,
                email TEXT NOT NULL UNIQUE
            );
            CREATE TABLE book (
                book_id INTEGER NOT NULL PRIMARY KEY,
                author_id INTEGER NOT NULL,
                isbn TEXT NOT NULL UNIQUE,
                FOREIGN KEY (author_id) REFERENCES author (author_id)
            );
            """)

        physical = refinement.introspect_sqlite(_SQLiteConnectionReader(connection))
        report = refinement.compare_refinement(_book_logical_schema(), physical)
    finally:
        connection.close()

    assert physical.backend == "sqlite"
    assert physical.table("author").primary_key == ("author_id",)
    assert physical.table("author").candidate_keys == (
        frozenset({"email"}),
        frozenset({"author_id"}),
    ) or physical.table("author").candidate_keys == (
        frozenset({"author_id"}),
        frozenset({"email"}),
    )
    assert physical.table("book").foreign_keys == (
        refinement.ForeignKeyShape(("author_id",), "author", ("author_id",)),
    )
    assert report.conforms
    assert report.render() == (
        "schema refinement PASS: contract='book-contract' backend='sqlite' "
        "relations=2 mismatches=0"
    )
    refinement.assert_refines(report)


def test_mariadb_information_schema_is_normalized_to_the_same_model() -> None:
    physical = refinement.introspect_mariadb(_MariaDBMetadataFixture())

    report = refinement.compare_refinement(_book_logical_schema(), physical)

    assert physical.backend == "mariadb"
    assert physical.table("author").columns == ("author_id", "email")
    assert set(physical.table("author").candidate_keys) == {
        frozenset({"author_id"}),
        frozenset({"email"}),
    }
    assert physical.table("book").foreign_keys == (
        refinement.ForeignKeyShape(("author_id",), "author", ("author_id",)),
    )
    assert physical.table("author").column("email") == refinement.ColumnShape(
        "email",
        "VARCHAR(255)",
        False,
        "utf8mb4_bin",
    )
    assert (
        refinement.IndexShape("author_email", ("email",), True)
        in physical.table("author").indexes
    )
    assert report.conforms


def test_refinement_reports_columns_keys_and_foreign_key_mismatches() -> None:
    physical = refinement.DatabaseShape(
        "fixture",
        (
            refinement.TableShape(
                "author",
                ("author_id", "legacy_name"),
                ("author_id",),
                (),
                (),
            ),
            refinement.TableShape(
                "book",
                ("book_id", "author_id", "isbn", "legacy_flag"),
                ("author_id",),
                (),
                (),
            ),
        ),
    )

    report = refinement.compare_refinement(_book_logical_schema(), physical)

    assert not report.conforms
    assert {mismatch.code for mismatch in report.mismatches} == {
        "missing-candidate-keys",
        "missing-columns",
        "missing-foreign-keys",
        "primary-key-not-candidate",
        "unexpected-candidate-keys",
        "unexpected-columns",
    }
    rendered = report.render()
    assert rendered.startswith(
        "schema refinement FAIL: contract='book-contract' backend='fixture' "
        "relations=2 mismatches="
    )
    assert "[missing-columns] author: table 'author' lacks {'email'}" in rendered
    assert (
        "[unexpected-columns] book: table 'book' has unmapped {'legacy_flag'}"
        in rendered
    )
    assert "[missing-foreign-keys] book:" in rendered
    with pytest.raises(refinement.SchemaRefinementError) as captured:
        refinement.assert_refines(report)
    assert str(captured.value) == rendered


def test_relation_mapping_supports_table_and_column_renames() -> None:
    physical = refinement.DatabaseShape(
        "fixture",
        (
            refinement.TableShape(
                "authors_v2",
                ("id", "email_address"),
                ("id",),
                (("email_address",),),
                (),
            ),
        ),
    )

    report = refinement.compare_refinement(
        _book_logical_schema(),
        physical,
        (
            refinement.RelationMapping(
                "author",
                "authors_v2",
                (("author_id", "id"), ("email", "email_address")),
            ),
        ),
    )

    assert report.conforms
    assert report.checked_relations == ("author",)


def test_manifest_loader_exposes_data_plane_contract_without_claiming_ddl_fit() -> None:
    logical = refinement.load_logical_schema(CATALOG)

    assert logical.name == "h2hdb-vnext-catalog"
    assert logical.relation("gallery_observation_file") is not None
    assert logical.relation("analysis_batch_receipt") is not None
    assert logical.relation("publication_head") is not None


def test_physical_spec_is_closed_world_and_uses_real_overlay_views() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)

    assert len(logical.relations) == 115
    assert len(physical_spec.implemented_relations) == 115
    assert len(physical_spec.pending_relations) == 0
    assert set(physical_spec.source_slice) == {
        relation.relation for relation in physical_spec.implemented_relations
    }
    assert physical_spec.complete
    assert "analysis_run" not in physical_spec.pending_relations
    assert "analysis_state_component_seal" not in physical_spec.pending_relations
    assert physical_spec.relation("analysis_run") is not None
    assert physical_spec.relation("analysis_batch_receipt") is not None
    assert physical_spec.relation("source_revision") is not None
    assert "publication_head" not in physical_spec.pending_relations
    resolved = physical_spec.relation("analysis_file_hash_decision_resolved")
    assert resolved is not None
    assert resolved.kind == "view"
    assert resolved.overlay_view == refinement.OverlayViewSpec(
        "analysis_state_ancestry",
        "analysis_file_hash_decision_shadow",
        "analysis_file_hash_decision_tombstone",
    )
    gallery_identity = physical_spec.relation("gallery_identity")
    assert gallery_identity is not None
    gallery_columns = {column.attribute: column for column in gallery_identity.columns}
    assert gallery_columns["locator_sha256"].mariadb.type_name == "BINARY(32)"
    assert gallery_columns["locator_sha256"].mariadb.collation is None
    assert (
        physical_spec.source_locator_protocol
        == refinement.PhysicalSourceLocatorProtocol(
            "source_locator_identity",
            "gallery_identity",
            "locator_sha256",
            "source_gallery_name",
            "canonical_value_identity",
            "source_relative_locator_v1",
            "u32be(codec_version) || u32be(segment_count) || repeated(u32be(segment_length) || segment_utf8)",
            "runtime_recompute_and_collision_compare",
        )
    )
    tag_term = physical_spec.relation("tag_term")
    assert tag_term is not None
    tag_columns = {column.attribute: column for column in tag_term.columns}
    assert tag_columns["namespace"].mariadb.type_name == "VARBINARY(128)"
    assert tag_columns["tag_value_sha256"].mariadb.type_name == "BINARY(32)"
    assert tag_columns["namespace"].mariadb.collation is None
    canonical_value = physical_spec.relation("canonical_value_identity")
    assert canonical_value is not None
    assert canonical_value.primary_key == ("value_sha256",)
    assert canonical_value.unique_keys == (("root_page_sha256",),)
    assert canonical_value.runtime_unique_keys == ()
    assert tuple(column.attribute for column in canonical_value.columns) == (
        "value_sha256",
        "root_page_sha256",
    )
    canonical_page = physical_spec.relation("canonical_value_page")
    assert canonical_page is not None
    assert canonical_page.runtime_unique_keys == (("page_bytes",),)
    assert canonical_page.columns[2].mariadb.type_name == "MEDIUMBLOB"
    content_candidate = physical_spec.relation("analysis_content_owner_candidate")
    assert content_candidate is not None
    assert content_candidate.columns[3].attribute == "priority_key"
    assert content_candidate.columns[3].mariadb.type_name == "VARBINARY(512)"
    assert content_candidate.required_indexes[0].attributes == (
        "analysis_id",
        "content_sha256",
        "priority_key",
        "gallery_id",
    )
    analysis_receipt = physical_spec.relation("analysis_batch_receipt")
    assert analysis_receipt is not None
    assert analysis_receipt.columns[2].attribute == "batch_key"
    assert analysis_receipt.columns[2].mariadb.type_name == "VARBINARY(512)"
    assert not any(
        "LONGBLOB" in column.mariadb.type_name
        for relation in physical_spec.implemented_relations
        for column in relation.columns
        if column.attribute
        in {
            attribute
            for key in (relation.primary_key, *relation.unique_keys)
            for attribute in key
        }
    )
    direct_payloads = {"metadata_fingerprint", "cursor", "protection_token"}
    expected_shapes = {
        "metadata_fingerprint": "BINARY(40)",
        "cursor": "VARBINARY(2048)",
        "protection_token": "VARBINARY(512)",
    }
    direct_occurrences: dict[str, int] = {name: 0 for name in direct_payloads}
    for relation in physical_spec.implemented_relations:
        keys_and_indexes = (
            relation.primary_key,
            *relation.unique_keys,
            *(index.attributes for index in relation.required_indexes),
        )
        foreign_key_attributes = {
            attribute
            for foreign_key in relation.foreign_keys
            for attribute in foreign_key.attributes
        }
        for column in relation.columns:
            if column.attribute not in direct_payloads:
                continue
            direct_occurrences[column.attribute] += 1
            assert column.mariadb.type_name == expected_shapes[column.attribute]
            assert all(column.attribute not in key for key in keys_and_indexes)
            assert column.attribute not in foreign_key_attributes
    assert direct_occurrences == {
        "metadata_fingerprint": 1,
        "cursor": 2,
        "protection_token": 1,
    }
    assert not any(
        "LONGBLOB" in column.mariadb.type_name
        for relation in physical_spec.implemented_relations
        for column in relation.columns
    )


def test_canonical_value_physical_protocol_is_owner_scoped_and_chunked() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    digest = physical_spec.canonical_digest_protocol
    pages = physical_spec.canonical_value_page_protocol

    assert digest is not None
    assert pages is not None
    assert (
        digest.allocation_relation,
        digest.page_relation,
        digest.descriptor_relation,
        digest.parent_relation,
        digest.value_relation,
    ) == (
        "canonical_value_allocation",
        "canonical_value_page",
        "canonical_value_page_descriptor",
        "canonical_value_page_parent",
        "canonical_value_identity",
    )
    assert digest.enforcement == (
        "bounded_stream_recompute_tree_validate_and_collision_compare"
    )
    assert (
        pages.codec_version,
        pages.maximum_page_bytes,
        pages.chunk_maximum_bytes,
        pages.branch_capacity,
        pages.maximum_level,
        pages.maximum_byte_count,
    ) == (1, 65536, 32768, 256, 8, (1 << 63) - 1)
    assert "owner_value_sha256" in pages.framing
    assert all(
        "value_bytes" not in {column.attribute for column in relation.columns}
        for relation in physical_spec.implemented_relations
    )


def test_sqlite_raw_u64_signed_i64_and_int63_boundaries_are_exact() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.execute("PRAGMA foreign_keys = OFF")
        high_u64 = ((1 << 64) - 1).to_bytes(8, "big")
        negative_i64 = ((1 << 64) - 1).to_bytes(8, "big")
        connection.execute(
            "INSERT INTO catalog_gallery_observation_file_filesystem "
            "(gallery_id, observation_id, file_key, device, inode, modified_ns, changed_ns) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (1, 1, bytes(32), high_u64, high_u64, negative_i64, negative_i64),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_files "
            "(gallery_id, observation_id, file_key, file_no, file_sha256) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, 1, bytes(32), (1 << 63) - 1, bytes(32)),
        )
        connection.execute(
            "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) VALUES (?, ?)",
            (bytes.fromhex("01" * 32), (1 << 63) - 1),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_gallery_observation_file_filesystem "
                "(gallery_id, observation_id, file_key, device, inode, modified_ns, changed_ns) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (2, 1, bytes(32), bytes(7), high_u64, negative_i64, negative_i64),
            )
    finally:
        connection.close()


def test_sqlite_canonical_page_positions_match_runtime_domains() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.execute("PRAGMA foreign_keys = OFF")
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_canonical_value_page_descriptors "
                "(page_sha256, value_sha256, level, page_position, subtree_item_count) "
                "VALUES (?, ?, ?, ?, ?)",
                (bytes(32), bytes.fromhex("01" * 32), 0, -1, 0),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_canonical_value_page_parents "
                "(child_sha256, parent_sha256, position) VALUES (?, ?, ?)",
                (bytes(32), bytes.fromhex("01" * 32), 256),
            )
    finally:
        connection.close()

    artifact_policy = physical_spec.relation("artifact_policy_semantics")
    assert artifact_policy is not None
    assert artifact_policy.unique_keys == (
        ("artifact_algorithm_version", "max_image_short_side"),
    )
    semantic_input = physical_spec.relation("artifact_semantic_input")
    assert semantic_input is not None
    assert semantic_input.unique_keys == (
        (
            "source_manifest_component_sha256",
            "member_plan_component_sha256",
            "effective_content_component_sha256",
            "selected_component_sha256",
            "owner_component_sha256",
            "policy_component_sha256",
        ),
    )
    assert refinement.maximum_mariadb_index_width(physical_spec) == (
        600,
        "analysis_batch_receipt",
        (
            "analysis_id",
            "stage",
            "committed_at",
            "batch_key",
        ),
    )


def test_every_table_foreign_key_has_a_child_side_left_prefix_access_path() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)

    checked = 0
    generated = 0
    for relation in physical_spec.implemented_relations:
        if relation.kind != "table":
            continue
        access_paths = (
            relation.primary_key,
            *relation.unique_keys,
            *(index.attributes for index in relation.required_indexes),
        )
        for foreign_key in relation.foreign_keys:
            checked += 1
            assert any(
                path[: len(foreign_key.attributes)] == foreign_key.attributes
                for path in access_paths
            ), (relation.relation, foreign_key.name)
        generated += sum(
            index.name.startswith("ix_fk_") for index in relation.required_indexes
        )

    assert checked >= 145
    assert generated > 50


def test_physical_loader_rejects_an_unindexed_child_foreign_key() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    canonical_value = physical_spec.relation("canonical_value_page")
    assert canonical_value is not None
    generated_index = next(
        index
        for index in canonical_value.required_indexes
        if index.attributes == ("value_sha256",)
    )
    broken_relation = replace(
        canonical_value,
        required_indexes=tuple(
            index
            for index in canonical_value.required_indexes
            if index != generated_index
        ),
    )
    broken = replace(
        physical_spec,
        relations=tuple(
            broken_relation if relation is canonical_value else relation
            for relation in physical_spec.relations
        ),
    )

    with pytest.raises(ValueError, match="child-side left-prefix"):
        refinement._validate_physical_schema(broken, logical)


def test_fresh_complete_sqlite_ddl_refines_physical_spec() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.executescript(
            refinement.render_sqlite_ddl(physical_spec, idempotent=True)
        )
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute(
            "INSERT INTO catalog_gallery_observation_metadata VALUES (?, ?, ?, ?, ?, ?)",
            (
                1,
                1,
                1,
                1_767_225_600_000_000,
                1_767_225_600_000_000,
                1_767_225_600_000_000,
            ),
        )
        database = refinement.introspect_sqlite(_SQLiteConnectionReader(connection))
    finally:
        connection.close()

    report = refinement.compare_physical_refinement(
        logical,
        physical_spec,
        database,
    )

    assert report.conforms
    assert report.fully_conforms
    assert not report.ddl_only
    assert len(report.checked_relations) == 115
    assert len(report.pending_relations) == 0
    assert report.mismatches == ()
    assert report.render().splitlines()[0] == (
        "physical schema refinement PASS: "
        "specification='h2hdb-vnext-physical' "
        "contract='h2hdb-vnext-catalog' backend='sqlite' "
        f"implemented={len(physical_spec.implemented_relations)} pending=0 "
        f"runtime_obligations={len(physical_spec.runtime_obligations)} mismatches=0"
    )
    source_file = database.table("catalog_gallery_observation_files")
    assert source_file is not None
    assert source_file.column("file_sha256") == refinement.ColumnShape(
        "file_sha256",
        "BLOB",
        False,
        None,
    )
    assert (
        refinement.IndexShape(
            "ix_gallery_file_hash",
            ("file_sha256", "gallery_id", "observation_id", "file_no"),
            False,
        )
        in source_file.indexes
    )
    assert any("file_no >= 0" in check.expression for check in source_file.checks)
    refinement.assert_physical_refines(report)
    refinement.assert_physical_refines(report, require_complete=True)


def test_generated_physical_contract_is_not_stale() -> None:
    result = subprocess.run(
        [sys.executable, str(GENERATOR), "--check"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr or result.stdout


def test_sqlite_overlay_view_uses_nearest_shadow_and_tombstone() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    connection = sqlite3.connect(":memory:")
    root = b"r" * 16
    middle = b"m" * 16
    leaf = b"l" * 16
    first_hash = b"1" * 32
    removed_hash = b"2" * 32
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.executemany(
            "INSERT INTO catalog_analysis_state_ancestry VALUES (?, ?, ?)",
            (
                (root, 0, root),
                (middle, 0, middle),
                (middle, 1, root),
                (leaf, 0, leaf),
                (leaf, 1, middle),
                (leaf, 2, root),
            ),
        )
        connection.executemany(
            "INSERT INTO catalog_analysis_file_hash_decision_shadow VALUES (?, ?, ?, ?, ?, ?)",
            (
                (root, first_hash, 2, 1, 1, b"a" * 32),
                (root, removed_hash, 3, 1, 1, b"b" * 32),
                (middle, first_hash, 4, 2, 2, b"c" * 32),
            ),
        )
        connection.execute(
            "INSERT INTO catalog_analysis_file_hash_decision_tombstone VALUES (?, ?)",
            (middle, removed_hash),
        )

        assert (
            connection.execute(
                """
            SELECT file_sha256, occurrence_count, evidence_sha256
            FROM catalog_analysis_file_hash_decision_resolved
            WHERE analysis_id = ?
            ORDER BY file_sha256
            """,
                (leaf,),
            ).fetchall()
            == [(first_hash, 4, b"c" * 32)]
        )
        assert (
            connection.execute(
                """
            SELECT file_sha256, occurrence_count, evidence_sha256
            FROM catalog_analysis_file_hash_decision_resolved
            WHERE analysis_id = ?
            ORDER BY file_sha256
            """,
                (root,),
            ).fetchall()
            == [
                (first_hash, 2, b"a" * 32),
                (removed_hash, 3, b"b" * 32),
            ]
        )

        connection.execute(
            "INSERT INTO catalog_analysis_file_hash_decision_tombstone VALUES (?, ?)",
            (leaf, first_hash),
        )
        assert (
            connection.execute(
                """
            SELECT file_sha256
            FROM catalog_analysis_file_hash_decision_resolved
            WHERE analysis_id = ?
            """,
                (leaf,),
            ).fetchall()
            == []
        )
    finally:
        connection.close()


def test_analysis_sqlite_fixture_enforces_group_membership_and_checks() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    connection = sqlite3.connect(":memory:")
    analysis_id = b"a" * 16
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute(
            "INSERT INTO catalog_analysis_content_owners VALUES (?, ?, ?, ?)",
            (analysis_id, b"c" * 32, 1, b"0" * 32),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_analysis_content_owners VALUES (?, ?, ?, ?)",
                (analysis_id, b"c" * 32, 2, b"1" * 32),
            )

        connection.execute(
            "INSERT INTO catalog_analysis_gid_winners VALUES (?, ?, ?, ?)",
            (analysis_id, 100, 1, b"4" * 32),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_analysis_gid_winners VALUES (?, ?, ?, ?)",
                (analysis_id, 100, 2, b"5" * 32),
            )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_analysis_exclusion_deltas VALUES (?, ?, ?, ?)",
                (analysis_id, b"6" * 32, 2, 0),
            )

        connection.execute(
            "INSERT INTO catalog_analysis_batch_receipts VALUES (?, ?, ?, ?, ?)",
            (
                analysis_id,
                b"HASH_STATS",
                b"batch-1",
                2,
                1_767_225_602_000_000,
            ),
        )
        assert connection.execute(
            "SELECT row_count FROM catalog_analysis_batch_receipts"
        ).fetchone() == (2,)
    finally:
        connection.close()


def test_sqlite_fixture_enforces_storage_classes_positive_revisions_and_states() -> (
    None
):
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.execute("PRAGMA foreign_keys = OFF")

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_content_blobs VALUES (?, ?)",
                ("a" * 32, 1),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_content_blobs VALUES (?, ?)",
                (b"a" * 32, 1.5),
            )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_source_build_base_source VALUES (?, ?, ?)",
                (b"b" * 16, 0, 1),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_source_build_base_source VALUES (?, ?, ?)",
                (b"b" * 16, 1, 0),
            )
        connection.execute(
            "INSERT INTO catalog_source_build_base_source VALUES (?, ?, ?)",
            (b"b" * 16, 1, 1),
        )

        source_build = "INSERT INTO catalog_source_builds VALUES (?, ?, ?, ?, ?, ?)"
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                source_build,
                (b"c" * 16, b"d" * 32, 1, "OPEN", 1, 2),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                source_build,
                (b"e" * 16, b"f" * 32, 1, "SEALED", 1, None),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                source_build,
                (b"g" * 16, b"h" * 32, 1, "ABANDONED", 1, 2),
            )
        connection.execute(
            source_build,
            (b"i" * 16, b"j" * 32, 1, "ABANDONED", 1, None),
        )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_gallery_observation_page_counts VALUES (?, ?, ?)",
                (1, 1, 4_294_967_296),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_gallery_observation_scans VALUES (?, ?, ?, ?, ?)",
                (1, 1, b"s" * 32, 4_294_967_296, 0),
            )
        for malformed in (b"f" * 39, b"f" * 41):
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    "INSERT INTO catalog_gallery_observation_discovery_fingerprints "
                    "VALUES (?, ?, ?)",
                    (1, 1, malformed),
                )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_analysis_checkpoints VALUES (?, ?, ?, ?, ?, ?)",
                (b"a" * 16, b"HASH", 1, b"c" * 2049, "OPEN", 1),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_publication_checkpoints VALUES (?, ?, ?, ?, ?, ?)",
                (b"p" * 16, b"ITEMS", 1, b"c" * 2049, "OPEN", 1),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_prepared_artifacts VALUES (?, ?, ?, ?, ?, ?)",
                (
                    b"q" * 16,
                    b"k" * 32,
                    b"artifact.cbz",
                    b"h" * 32,
                    b"t" * 513,
                    "PREPARED",
                ),
            )
    finally:
        connection.close()


def test_mariadb_renderer_preserves_exact_binary_types_checks_and_views() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    statements = refinement.render_mariadb_ddl(physical_spec)
    ddl = "\n".join(statements)

    assert len(statements) == len(physical_spec.implemented_relations)
    assert sum(value.startswith("CREATE TABLE") for value in statements) == sum(
        relation.kind == "table" for relation in physical_spec.implemented_relations
    )
    assert (
        sum(
            value.startswith("CREATE SQL SECURITY INVOKER VIEW") for value in statements
        )
        == 5
    )
    assert "`analysis_id` BINARY(16) NOT NULL" in ddl
    assert "`locator_sha256` BINARY(32) NOT NULL" in ddl
    assert "`source_gallery_name` VARBINARY(255) NOT NULL" in ddl
    assert "`namespace` VARBINARY(128) NOT NULL" in ddl
    assert "`page_bytes` MEDIUMBLOB NOT NULL" in ddl
    assert "`root_page_sha256` BINARY(32) NOT NULL" in ddl
    assert "`device` BINARY(8) NOT NULL" in ddl
    assert "`inode` BINARY(8) NOT NULL" in ddl
    assert "`modified_ns` BINARY(8) NOT NULL" in ddl
    assert "`changed_ns` BINARY(8) NOT NULL" in ddl
    assert "`file_no` BIGINT UNSIGNED NOT NULL" in ddl
    assert "file_no <= 9223372036854775807" in ddl
    assert "file_no >= 0" in ddl
    assert "page_count <= 4294967295" in ddl
    assert "scan_observation_version <= 4294967295" in ddl
    assert "`metadata_fingerprint` BINARY(40) NOT NULL" in ddl
    assert "octet_length(metadata_fingerprint) = 40" in ddl
    assert "`cursor` VARBINARY(2048) NOT NULL" in ddl
    assert "octet_length(`cursor`) <= 2048" in ddl
    assert "octet_length(protection_token) <= 512" in ddl
    assert "`summary_sha256` BINARY(32) NOT NULL" in ddl
    assert "`language_sha256` BINARY(32) NOT NULL" in ddl
    assert "`artifact_locator_sha256` BINARY(32) NOT NULL" in ddl
    assert "KEY `ix_gallery_file_hash`" in ddl
    assert "`priority_key` VARBINARY(512) NOT NULL" in ddl
    assert "old_excluded IN (0, 1) AND new_excluded IN (0, 1)" in ddl
    assert "KEY `ix_analysis_content_candidate_group`" in ddl
    assert "KEY `ix_analysis_gid_candidate_order`" in ddl
    assert "KEY `ix_fk_canonical_value_page_1_value_sha256`" in ddl
    assert (
        "CREATE SQL SECURITY INVOKER VIEW `catalog_analysis_file_hash_decision_resolved`"
        in ddl
    )
    assert "`value_bytes`" not in ddl
    assert refinement.maximum_mariadb_index_width(physical_spec)[0] == 600
    idempotent = refinement.render_mariadb_ddl(
        physical_spec,
        idempotent=True,
    )
    assert all(
        "IF NOT EXISTS" in statement.partition("\n")[0] for statement in idempotent
    )
    assert any(
        statement.startswith(
            "CREATE SQL SECURITY INVOKER VIEW IF NOT EXISTS "
            "`catalog_analysis_file_hash_decision_resolved`"
        )
        for statement in idempotent
    )


def test_fresh_source_slice_mariadb_ddl_refines_physical_spec(
    mariadb_config: CoreConfig,
) -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    database_config = mariadb_config.database
    with MariaDBConnector(
        host=database_config.host,
        port=database_config.port,
        user=database_config.user,
        password=database_config.password,
        database=database_config.database,
    ) as connector:
        for statement in refinement.render_mariadb_ddl(physical_spec):
            connector.execute(statement)
        for statement in refinement.render_mariadb_ddl(
            physical_spec,
            idempotent=True,
        ):
            connector.execute(statement)
        connector.execute("SET FOREIGN_KEY_CHECKS = 0")
        connector.execute(
            "INSERT INTO catalog_gallery_observation_metadata VALUES (%s, %s, %s, %s, %s, %s)",
            (
                1,
                1,
                1,
                1_767_225_600_000_000,
                1_767_225_600_000_000,
                1_767_225_600_000_000,
            ),
        )
        assert connector.fetch_one(
            "SELECT gid, upload_time, download_time, modified_time "
            "FROM catalog_gallery_observation_metadata"
        ) == (1, 1_767_225_600_000_000, 1_767_225_600_000_000, 1_767_225_600_000_000)
        connector.execute("SET FOREIGN_KEY_CHECKS = 1")
        database = refinement.introspect_mariadb(connector)

    report = refinement.compare_physical_refinement(
        logical,
        physical_spec,
        database,
    )

    assert report.conforms, report.render()
    assert report.fully_conforms
    assert not report.ddl_only
    gallery_name = database.table("catalog_source_locator_identity").column(
        "source_gallery_name"
    )
    assert gallery_name.type_name == "VARBINARY(255)"
    assert gallery_name.collation is None
    priority_key = database.table("catalog_analysis_content_owner_candidates").column(
        "priority_key"
    )
    assert priority_key.type_name == "VARBINARY(512)"
    assert priority_key.collation is None
    refinement.assert_physical_refines(report)


def test_current_fresh_sqlite_schema_fails_source_slice_refinement(
    sqlite_config: CoreConfig,
) -> None:
    H2HDB(sqlite_config).migrate()
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    with SQLiteConnector(database=sqlite_config.database.database) as connector:
        database = refinement.introspect_sqlite(connector)

    report = refinement.compare_physical_refinement(
        logical,
        physical_spec,
        database,
    )

    assert not report.conforms
    assert database.table("catalog_source_files") is not None
    assert "missing-table" in {mismatch.code for mismatch in report.mismatches}
    rendered = report.render()
    assert "physical schema refinement FAIL" in rendered
    assert "gallery_observation_file" in rendered
    assert "catalog_gallery_observation_files" in rendered
    assert (
        f"implemented={len(physical_spec.implemented_relations)} pending=0 "
        f"runtime_obligations={len(physical_spec.runtime_obligations)}" in rendered
    )


def test_current_fresh_mariadb_schema_fails_source_slice_refinement(
    mariadb_config: CoreConfig,
) -> None:
    H2HDB(mariadb_config).migrate()
    database_config = mariadb_config.database
    with MariaDBConnector(
        host=database_config.host,
        port=database_config.port,
        user=database_config.user,
        password=database_config.password,
        database=database_config.database,
    ) as connector:
        physical = refinement.introspect_mariadb(connector)
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)

    report = refinement.compare_physical_refinement(
        logical,
        physical_spec,
        physical,
    )

    assert not report.conforms
    assert report.backend == "mariadb"
    assert any(
        mismatch.relation == "gallery_observation_file"
        for mismatch in report.mismatches
    )
