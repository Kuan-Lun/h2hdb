from __future__ import annotations

import hashlib
import importlib.util
import re
import sqlite3
import subprocess
import sys
import tomllib
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest

from h2hdb import CoreConfig, VNextDatabaseAdminFacade
from h2hdb._generated_vnext_schema import ARTIFACT
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


def test_data_bootstrap_cells_are_exact_typed_scalars() -> None:
    with PHYSICAL.open("rb") as stream:
        document = tomllib.load(stream)

    seeds = document["bootstrap_seed"]
    assert len(seeds) == 177
    for seed in seeds:
        assert seed["version"] == 1
        assert seed["value"]
        for cell in seed["value"]:
            if cell["type"] == "ascii_enum":
                assert set(cell) == {"attribute", "type", "encoding", "text"}
                assert cell["encoding"] == "utf8"
                cell["text"].encode("ascii")
            else:
                assert set(cell) == {"attribute", "type", "integer"}
                assert cell["type"] in {"uint32", "uint64"}
                assert isinstance(cell["integer"], int)
                assert 0 <= cell["integer"] <= 2 ** int(cell["type"][4:]) - 1


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
    assert len(bindings) == len(owners) == len(document["runtime_obligations"]) == 85
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
        '"source_provider_registry", "analysis_stage_anchor"',
        'seeded_relations = ["canonical_digest_policy", "channel_registry", '
        '"source_provider_registry", "manifest_policy_anchor"',
        1,
    )
    assert invalid != original
    invalid_path = tmp_path / "invalid-bootstrap-physical.toml"
    invalid_path.write_text(invalid, encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="relation partitions overlap|seed exact base facts",
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
        if "INFORMATION_SCHEMA.VIEWS" in query:
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

    assert len(logical.relations) == 439
    assert len(physical_spec.implemented_relations) == 439
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
    manifest_fact = physical_spec.relation("manifest_policy_manifest_algorithm_version")
    manifest_identity = physical_spec.relation("manifest_policy_identity")
    assert manifest_fact is not None
    assert manifest_identity is not None
    assert manifest_fact.unique_keys == ()
    assert manifest_fact.referential_unique_keys == (
        ("manifest_policy_id", "manifest_algorithm_version"),
    )
    assert any(
        foreign_key.attributes == ("manifest_policy_id", "manifest_algorithm_version")
        and foreign_key.referenced_relation
        == "manifest_policy_manifest_algorithm_version"
        and foreign_key.referenced_attributes
        == ("manifest_policy_id", "manifest_algorithm_version")
        for foreign_key in manifest_identity.foreign_keys
    )
    publication = physical_spec.relation("catalog_publication")
    assert publication is not None
    assert tuple(column.attribute for column in publication.columns) == (
        "revision",
        "publication_key",
        "gallery_id",
        "summary_sha256",
        "language_sha256",
        "modified_at",
    )
    resolved = physical_spec.relation("analysis_file_hash_decision_resolved")
    assert resolved is not None
    assert resolved.kind == "view"
    assert resolved.overlay_view == refinement.OverlayViewSpec(
        "analysis_state_ancestry",
        "analysis_file_hash_decision_shadow",
        "analysis_file_hash_decision_tombstone",
    )
    metadata = physical_spec.relation("gallery_observation_metadata")
    assert metadata is not None
    assert metadata.kind == "view"
    assert metadata.table == "catalog_gallery_observation_metadata"
    assert metadata.vertical_view == refinement.SealedVerticalViewSpec(
        family="gallery_observation_metadata_vertical",
        anchor_relation="gallery_observation_metadata_anchor",
        seal_relation="gallery_observation_metadata_seal",
        key_attributes=("gallery_id", "observation_id"),
        members=(
            refinement.VerticalViewMemberSpec(
                "gallery_source_name_access",
                ("gallery_id",),
                "source_gallery_name",
                "gallery_observation_metadata_seal",
                ("gallery_id",),
                ("gallery_id",),
                project=False,
                projection_attribute="source_gallery_name",
            ),
            refinement.VerticalViewMemberSpec(
                "source_gallery_name_gid",
                ("source_gallery_name",),
                "gid",
                "gallery_source_name_access",
                ("source_gallery_name",),
                ("source_gallery_name",),
                project=True,
                projection_attribute="gid",
            ),
            refinement.VerticalViewMemberSpec(
                "gallery_upload_time",
                ("gid",),
                "upload_time",
                "source_gallery_name_gid",
                ("gid",),
                ("gid",),
                project=True,
                projection_attribute="upload_time",
            ),
            refinement.VerticalViewMemberSpec(
                "gallery_observation_download_time",
                ("gallery_id", "observation_id"),
                "download_time",
                "gallery_observation_metadata_seal",
                ("gallery_id", "observation_id"),
                ("gallery_id", "observation_id"),
                project=True,
                projection_attribute="download_time",
            ),
            refinement.VerticalViewMemberSpec(
                "gallery_observation_modified_time",
                ("gallery_id", "observation_id"),
                "modified_time",
                "gallery_observation_metadata_seal",
                ("gallery_id", "observation_id"),
                ("gallery_id", "observation_id"),
                project=True,
                projection_attribute="modified_time",
            ),
        ),
    )
    for relation_name in (
        "gallery_observation_metadata_anchor",
        "gallery_upload_time",
        "source_gallery_name_gid",
        "gallery_source_name_access",
        "gallery_observation_download_time",
        "gallery_observation_modified_time",
        "gallery_observation_metadata_seal",
    ):
        base = physical_spec.relation(relation_name)
        assert base is not None
        assert base.kind == "table"
        assert len(base.columns) - len(base.primary_key) <= 1
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
    content_candidate = physical_spec.relation(
        "analysis_content_owner_candidate_shadow_content_sha256"
    )
    assert content_candidate is not None
    assert content_candidate.columns[2].attribute == "content_sha256"
    assert content_candidate.columns[2].mariadb.type_name == "BINARY(32)"
    assert content_candidate.required_indexes[0].attributes == (
        "analysis_id",
        "content_sha256",
        "gallery_id",
    )
    assert physical_spec.relation("analysis_gid_candidate") is None
    assert physical_spec.relation("analysis_gid_winner") is None
    gid_candidate = physical_spec.relation("analysis_gid_candidate_shadow")
    assert gid_candidate is not None
    assert tuple(column.attribute for column in gid_candidate.columns) == (
        "analysis_id",
        "gallery_id",
    )
    gid_selection = physical_spec.relation("analysis_gid_winner_selection")
    assert gid_selection is not None
    assert tuple(column.attribute for column in gid_selection.columns) == (
        "analysis_id",
        "winner_gallery_id",
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
        "protection_token": "BINARY(184)",
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
            if column.attribute == "protection_token":
                assert (column.attribute,) in keys_and_indexes
            else:
                assert all(column.attribute not in key for key in keys_and_indexes)
            assert column.attribute not in foreign_key_attributes
    assert direct_occurrences == {
        "metadata_fingerprint": 1,
        "cursor": 6,
        "protection_token": 2,
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
            "INSERT INTO catalog_gallery_observation_file_filesystem_anchors "
            "(gallery_id, observation_id, file_key) VALUES (?, ?, ?)",
            (1, 1, bytes(32)),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_file_filesystem_devices "
            "(gallery_id, observation_id, file_key, device) VALUES (?, ?, ?, ?)",
            (1, 1, bytes(32), high_u64),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_file_filesystem_inodes "
            "(gallery_id, observation_id, file_key, inode) VALUES (?, ?, ?, ?)",
            (1, 1, bytes(32), high_u64),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_file_filesystem_modified_nses "
            "(gallery_id, observation_id, file_key, modified_ns) VALUES (?, ?, ?, ?)",
            (1, 1, bytes(32), negative_i64),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_file_filesystem_changed_nses "
            "(gallery_id, observation_id, file_key, changed_ns) VALUES (?, ?, ?, ?)",
            (1, 1, bytes(32), negative_i64),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_file_filesystem_seals "
            "(gallery_id, observation_id, file_key) VALUES (?, ?, ?)",
            (1, 1, bytes(32)),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_file_anchors "
            "(gallery_id, observation_id, file_key) VALUES (?, ?, ?)",
            (1, 1, bytes(32)),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_file_file_nos "
            "(gallery_id, observation_id, file_key, file_no) VALUES (?, ?, ?, ?)",
            (1, 1, bytes(32), (1 << 63) - 1),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_file_file_sha256s "
            "(gallery_id, observation_id, file_key, file_sha256) "
            "VALUES (?, ?, ?, ?)",
            (1, 1, bytes(32), bytes(32)),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_file_seals "
            "(gallery_id, observation_id, file_key) VALUES (?, ?, ?)",
            (1, 1, bytes(32)),
        )
        connection.execute(
            "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) VALUES (?, ?)",
            (bytes.fromhex("01" * 32), (1 << 63) - 1),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_gallery_observation_file_filesystem_devices "
                "(gallery_id, observation_id, file_key, device) VALUES (?, ?, ?, ?)",
                (2, 1, bytes(32), bytes(7)),
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
                "INSERT INTO catalog_canonical_value_page_coordinates "
                "(value_sha256, level, page_position, page_sha256) "
                "VALUES (?, ?, ?, ?)",
                (bytes.fromhex("01" * 32), 0, -1, bytes(32)),
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
        (
            "artifact_algorithm_version",
            "max_image_short_side",
            "producer_fingerprint_sha256",
        ),
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
        640,
        "artifact_producer_fingerprint_identity",
        (
            "writer_id",
            "python_abi",
            "pillow_build",
            "libjpeg_build",
            "zlib_build",
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
    canonical_value = physical_spec.relation("canonical_value_allocation_digest_domain")
    assert canonical_value is not None
    generated_index = next(
        index
        for index in canonical_value.required_indexes
        if index.attributes == ("digest_domain",)
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


@pytest.mark.parametrize(
    ("relation_name", "index_name", "attributes"),
    (
        (
            "analysis_impacted_content_provenance",
            "ix_a_impacted_content_key_gallery",
            ("analysis_id", "content_sha256", "gallery_id"),
        ),
        (
            "analysis_impacted_gid_provenance",
            "ix_a_impacted_gid_key_gallery",
            ("analysis_id", "gid", "gallery_id"),
        ),
    ),
)
def test_impacted_provenance_requires_exact_key_first_lookup_index(
    relation_name: str,
    index_name: str,
    attributes: tuple[str, ...],
) -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    provenance = physical_spec.relation(relation_name)
    assert provenance is not None
    expected = refinement.PhysicalIndexSpec(index_name, attributes, False)
    assert expected in provenance.required_indexes

    broken_provenance = replace(
        provenance,
        required_indexes=tuple(
            index for index in provenance.required_indexes if index != expected
        ),
    )
    broken = replace(
        physical_spec,
        relations=tuple(
            broken_provenance if relation is provenance else relation
            for relation in physical_spec.relations
        ),
    )
    with pytest.raises(ValueError, match="exact key-first lookup index"):
        refinement._validate_physical_schema(broken, logical)


def test_physical_vertical_view_metadata_cannot_omit_a_member() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    metadata = physical_spec.relation("gallery_observation_metadata")
    assert metadata is not None
    assert metadata.vertical_view is not None
    broken_metadata = replace(
        metadata,
        vertical_view=replace(
            metadata.vertical_view,
            members=metadata.vertical_view.members[:-1],
        ),
    )
    broken = replace(
        physical_spec,
        relations=tuple(
            broken_metadata if relation is metadata else relation
            for relation in physical_spec.relations
        ),
    )

    with pytest.raises(
        ValueError,
        match="projection is not exactly the key and every projected non-join attribute",
    ):
        refinement._validate_physical_schema(broken, logical)


def test_physical_optional_vertical_metadata_is_closed_and_directional() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    source_build = physical_spec.relation("source_build")
    assert source_build is not None
    assert source_build.vertical_view is not None
    optional = next(
        member
        for member in source_build.vertical_view.members
        if member.relation == "source_build_sealed_at"
    )
    wrongly_required = replace(optional, required=True)
    broken_view = replace(
        source_build.vertical_view,
        members=tuple(
            wrongly_required if member is optional else member
            for member in source_build.vertical_view.members
        ),
    )
    broken_source_build = replace(source_build, vertical_view=broken_view)
    broken = replace(
        physical_spec,
        relations=tuple(
            broken_source_build if relation is source_build else relation
            for relation in physical_spec.relations
        ),
    )
    with pytest.raises(ValueError, match="lacks its participation FK"):
        refinement._validate_physical_schema(broken, logical)

    no_presence = replace(
        source_build,
        vertical_view=replace(source_build.vertical_view, optional_presence=None),
    )
    broken = replace(
        physical_spec,
        relations=tuple(
            no_presence if relation is source_build else relation
            for relation in physical_spec.relations
        ),
    )
    with pytest.raises(ValueError, match="optional members require one closed"):
        refinement._validate_physical_schema(broken, logical)


def test_physical_vertical_projection_alias_cannot_drift() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    build_manifest = physical_spec.relation("build_manifest")
    assert build_manifest is not None
    assert build_manifest.vertical_view is not None
    timestamp = next(
        member
        for member in build_manifest.vertical_view.members
        if member.relation == "source_build_sealed_at"
    )
    broken_timestamp = replace(timestamp, projection_attribute="sealed_at")
    broken_manifest = replace(
        build_manifest,
        vertical_view=replace(
            build_manifest.vertical_view,
            members=tuple(
                broken_timestamp if member is timestamp else member
                for member in build_manifest.vertical_view.members
            ),
        ),
    )
    broken = replace(
        physical_spec,
        relations=tuple(
            broken_manifest if relation is build_manifest else relation
            for relation in physical_spec.relations
        ),
    )
    with pytest.raises(ValueError, match="projection is not exactly"):
        refinement._validate_physical_schema(broken, logical)


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_batch3b_refinement_and_provider_views_are_exactly_equal(
    backend: str,
) -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    relation_by_name = {
        relation.relation: relation for relation in physical_spec.relations
    }
    refinement_by_relation = {
        relation_name: refinement._render_view(
            relation_by_name[relation_name],
            relation_by_name,
            backend,
            idempotent=True,
        )
        for relation_name in (
            "source_build",
            "build_manifest",
            "gallery_manifest",
            "source_snapshot_manifest_identity",
        )
    }
    artifact = cast(dict[str, Any], ARTIFACT)
    provider_slices = dict(artifact["backends"][backend]["slices"])

    for relation_name in (
        "source_build",
        "build_manifest",
        "gallery_manifest",
        "source_snapshot_manifest_identity",
    ):
        provider_statements = provider_slices[f"relation:{relation_name}"]
        assert len(provider_statements) == 1
        assert provider_statements[0][3] == refinement_by_relation[relation_name]

    source_build_sql = refinement_by_relation["source_build"]
    assert "LEFT JOIN" in source_build_sql
    assert "state" in source_build_sql
    assert "'SEALED'" in source_build_sql
    assert "'OPEN', 'ABANDONED'" in source_build_sql
    assert "sealed_at" in source_build_sql
    assert (
        'AS "computed_at"' if backend == "sqlite" else "AS `computed_at`"
    ) in refinement_by_relation["build_manifest"]


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_batch4_refinement_and_provider_views_are_exactly_equal(
    backend: str,
) -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    relation_by_name = {
        relation.relation: relation for relation in physical_spec.relations
    }
    artifact = cast(dict[str, Any], ARTIFACT)
    provider_slices = dict(artifact["backends"][backend]["slices"])
    for relation_name in (
        "analysis_run",
        "analysis_state_anchor",
        "analysis_state_component_seal",
        "analysis_exclusion_delta",
    ):
        expected = refinement._render_view(
            relation_by_name[relation_name],
            relation_by_name,
            backend,
            idempotent=True,
        )
        provider_statements = provider_slices[f"relation:{relation_name}"]
        assert len(provider_statements) == 1
        assert provider_statements[0][3] == expected

    analysis_run_sql = provider_slices["relation:analysis_run"][0][3]
    assert "LEFT JOIN" in analysis_run_sql
    assert "'COMPLETE'" in analysis_run_sql
    assert "'OPEN', 'ABANDONED'" in analysis_run_sql
    endpoint_sql = provider_slices["relation:analysis_state_anchor"][0][3]
    assert "NOT EXISTS" in endpoint_sql


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_publication_candidate_projection_uses_fixed_terminal_receipt_joins(
    backend: str,
) -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    relation_by_name = {
        relation.relation: relation for relation in physical_spec.relations
    }
    expected = refinement._render_view(
        relation_by_name["publication_candidate_projection"],
        relation_by_name,
        backend,
        idempotent=True,
    )
    artifact = cast(dict[str, Any], ARTIFACT)
    provider_slices = dict(artifact["backends"][backend]["slices"])
    provider_statements = provider_slices["relation:publication_candidate_projection"]

    assert len(provider_statements) == 1
    assert provider_statements[0][3] == expected
    for forbidden in (
        "MAX(",
        "SUM(",
        "COUNT(",
        "COALESCE(",
        "GROUP BY",
        "HAVING",
    ):
        assert forbidden not in expected

    quote = '"' if backend == "sqlite" else "`"
    checkpoint_table = f"{quote}catalog_publication_checkpoints{quote}"
    receipt_table = f"{quote}catalog_publication_batch_receipts{quote}"
    checkpoint_clause = "FROM" if backend == "sqlite" else "JOIN"
    assert expected.count(f"{checkpoint_clause} {checkpoint_table} AS checkpoint_") == 5
    assert expected.count(f"JOIN {receipt_table} AS receipt_") == 5
    count_stages = (
        ("create_count", "VALIDATE_CREATE"),
        ("rebuild_count", "VALIDATE_REBUILD"),
        ("delete_count", "VALIDATE_DELETE"),
        ("new_galleries", "VALIDATE_NEW_GALLERY"),
        ("changed_galleries", "VALIDATE_CHANGED_GALLERY"),
    )
    for attribute, stage in count_stages:
        stage_literal = (
            "X'" + stage.encode("ascii").hex().upper() + "'"
            if backend == "sqlite"
            else f"'{stage}'"
        )
        if backend == "sqlite":
            assert (
                f"exact.{quote}{attribute}{quote} AS {quote}{attribute}{quote}"
            ) in expected
            assert (f"exact.{quote}{attribute}{quote} IS NOT NULL") in expected
            assert (
                f"(SELECT receipt_{attribute}." f"{quote}next_processed_count{quote}"
            ) in expected
        else:
            assert (
                f"receipt_{attribute}.{quote}next_processed_count{quote} AS "
                f"{quote}{attribute}{quote}"
            ) in expected
        assert (
            f"checkpoint_{attribute}.{quote}stage{quote} = {stage_literal}"
        ) in expected


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
            "INSERT INTO catalog_gallery_observation_metadata_anchors VALUES (?, ?)",
            (1, 1),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_upload_times VALUES (?, ?)",
            (1, 1_767_225_600_000_000),
        )
        connection.execute(
            "INSERT INTO catalog_source_gallery_name_gids VALUES (?, ?)",
            (sqlite3.Binary(b"gallery-1"), 1),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_source_name_accesses VALUES (?, ?)",
            (1, sqlite3.Binary(b"gallery-1")),
        )
        for table in (
            "catalog_gallery_observation_download_times",
            "catalog_gallery_observation_modified_times",
        ):
            connection.execute(
                f"INSERT INTO {table} VALUES (?, ?, ?)",
                (1, 1, 1_767_225_600_000_000),
            )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_metadata_seals VALUES (?, ?)",
            (1, 1),
        )
        assert connection.execute(
            "SELECT gid, upload_time, download_time, modified_time "
            "FROM catalog_gallery_observation_metadata"
        ).fetchone() == (
            1,
            1_767_225_600_000_000,
            1_767_225_600_000_000,
            1_767_225_600_000_000,
        )
        with pytest.raises(sqlite3.OperationalError, match="view"):
            connection.execute(
                "INSERT INTO catalog_gallery_observation_metadata "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (1, 2, 1, 1, 1, 1),
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
    assert len(report.checked_relations) == 439
    assert len(report.pending_relations) == 0
    assert report.mismatches == ()
    assert report.render().splitlines()[0] == (
        "physical schema refinement PASS: "
        "specification='h2hdb-vnext-physical' "
        "contract='h2hdb-vnext-catalog' backend='sqlite' "
        f"implemented={len(physical_spec.implemented_relations)} pending=0 "
        f"runtime_obligations={len(physical_spec.runtime_obligations)} mismatches=0"
    )
    source_file_digest = database.table("catalog_gallery_observation_file_file_sha256s")
    assert source_file_digest is not None
    assert source_file_digest.column("file_sha256") == refinement.ColumnShape(
        "file_sha256",
        "BLOB",
        False,
        None,
    )
    assert (
        refinement.IndexShape(
            "ix_gallery_file_hash",
            ("file_sha256", "gallery_id", "observation_id", "file_key"),
            False,
        )
        in source_file_digest.indexes
    )
    source_file_no = database.table("catalog_gallery_observation_file_file_nos")
    assert source_file_no is not None
    assert any("file_no >= 0" in check.expression for check in source_file_no.checks)
    refinement.assert_physical_refines(report)
    refinement.assert_physical_refines(report, require_complete=True)


def test_vertical_metadata_view_requires_every_member_before_seal() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.execute("PRAGMA foreign_keys = OFF")
        digest = sqlite3.Binary(bytes(32))
        scope_key = sqlite3.Binary(bytes([1]) * 32)
        locator_sha256 = sqlite3.Binary(bytes([2]) * 32)
        connection.execute(
            "INSERT INTO catalog_gallery_identities VALUES (?, ?, ?, ?)",
            (1, digest, scope_key, locator_sha256),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_allocations VALUES (?, ?, ?)",
            (1, 1, 1),
        )
        connection.commit()
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            "INSERT INTO catalog_gallery_observation_metadata_anchors VALUES (?, ?)",
            (1, 1),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_upload_times VALUES (?, ?)", (7, 11)
        )
        connection.execute(
            "INSERT INTO catalog_source_gallery_name_gids VALUES (?, ?)",
            (sqlite3.Binary(b"gallery-7"), 7),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_source_name_accesses VALUES (?, ?)",
            (1, sqlite3.Binary(b"gallery-7")),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_download_times VALUES (?, ?, ?)",
            (1, 1, 13),
        )
        assert (
            connection.execute(
                "SELECT * FROM catalog_gallery_observation_metadata"
            ).fetchall()
            == []
        )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            connection.execute(
                "INSERT INTO catalog_gallery_observation_metadata_seals VALUES (?, ?)",
                (1, 1),
            )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_modified_times VALUES (?, ?, ?)",
            (1, 1, 17),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_metadata_seals VALUES (?, ?)",
            (1, 1),
        )
        assert connection.execute(
            "SELECT * FROM catalog_gallery_observation_metadata"
        ).fetchone() == (1, 1, 7, 11, 13, 17)
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("anchor", "key", "members", "seal", "view", "expected"),
    [
        (
            "catalog_source_build_discovery_anchors",
            (b"b" * 16,),
            (
                (
                    "catalog_source_build_discovery_scan_attempts",
                    (b"b" * 16, b"a" * 16),
                ),
                ("catalog_source_build_discovery_gallery_counts", (b"b" * 16, 0)),
                (
                    "catalog_source_build_discovery_tree_observation_sha256s",
                    (b"b" * 16, b"t" * 32),
                ),
                ("catalog_source_build_discovery_completed_ats", (b"b" * 16, 5)),
            ),
            "catalog_source_build_discovery_seals",
            "catalog_source_build_discoveries",
            (b"b" * 16, b"a" * 16, 0, b"t" * 32, 5),
        ),
        (
            "catalog_gallery_observation_scan_anchors",
            (1, 2),
            (
                (
                    "catalog_gallery_observation_scan_observation_sha256s",
                    (1, 2, b"s" * 32),
                ),
                ("catalog_gallery_observation_scan_observation_versions", (1, 2, 1)),
                ("catalog_gallery_observation_scan_source_file_counts", (1, 2, 0)),
            ),
            "catalog_gallery_observation_scan_seals",
            "catalog_gallery_observation_scans",
            (1, 2, b"s" * 32, 1, 0),
        ),
        (
            "catalog_gallery_observation_directory_anchors",
            (1, 2),
            (
                ("catalog_gallery_observation_directory_entry_counts", (1, 2, 0)),
                (
                    "catalog_gallery_observation_directory_observation_sha256s",
                    (1, 2, b"d" * 32),
                ),
            ),
            "catalog_gallery_observation_directory_seals",
            "catalog_gallery_observation_directories",
            (1, 2, 0, b"d" * 32),
        ),
        (
            "catalog_gallery_observation_stat_anchors",
            (1, 2),
            (
                ("catalog_gallery_observation_stat_file_counts", (1, 2, 0)),
                ("catalog_gallery_observation_stat_byte_counts", (1, 2, 0)),
            ),
            "catalog_gallery_observation_stat_seals",
            "catalog_gallery_observation_stat",
            (1, 2, 0, 0),
        ),
        (
            "catalog_gallery_observation_file_filesystem_anchors",
            (1, 2, b"k" * 32),
            (
                (
                    "catalog_gallery_observation_file_filesystem_devices",
                    (1, 2, b"k" * 32, b"d" * 8),
                ),
                (
                    "catalog_gallery_observation_file_filesystem_inodes",
                    (1, 2, b"k" * 32, b"i" * 8),
                ),
                (
                    "catalog_gallery_observation_file_filesystem_modified_nses",
                    (1, 2, b"k" * 32, b"m" * 8),
                ),
                (
                    "catalog_gallery_observation_file_filesystem_changed_nses",
                    (1, 2, b"k" * 32, b"c" * 8),
                ),
            ),
            "catalog_gallery_observation_file_filesystem_seals",
            "catalog_gallery_observation_file_filesystem",
            (1, 2, b"k" * 32, b"d" * 8, b"i" * 8, b"m" * 8, b"c" * 8),
        ),
        (
            "catalog_file_name_identity_anchors",
            (b"k" * 32,),
            (
                ("catalog_file_name_identity_name_bytes", (b"k" * 32, b"a.jpg")),
                ("catalog_file_name_identity_file_roles", (b"k" * 32, b"CONTENT")),
            ),
            "catalog_file_name_identity_seals",
            "catalog_file_name_identities",
            (b"k" * 32, b"a.jpg", b"CONTENT"),
        ),
        (
            "catalog_gallery_observation_file_anchors",
            (1, 2, b"k" * 32),
            (
                (
                    "catalog_gallery_observation_file_file_nos",
                    (1, 2, b"k" * 32, 0),
                ),
                (
                    "catalog_gallery_observation_file_file_sha256s",
                    (1, 2, b"k" * 32, b"h" * 32),
                ),
            ),
            "catalog_gallery_observation_file_seals",
            "catalog_gallery_observation_files",
            (1, 2, 0, b"k" * 32, b"h" * 32),
        ),
        (
            "catalog_tag_term_anchors",
            (7,),
            (("catalog_tag_term_identities", (b"artist", b"t" * 32, 7)),),
            "catalog_tag_term_seals",
            "catalog_tag_terms",
            (7, b"artist", b"t" * 32),
        ),
    ],
)
def test_new_vertical_views_require_every_member_and_are_read_only(
    anchor: str,
    key: tuple[object, ...],
    members: tuple[tuple[str, tuple[object, ...]], ...],
    seal: str,
    view: str,
    expected: tuple[object, ...],
) -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute(
            f"INSERT INTO {anchor} VALUES ({', '.join('?' for _ in key)})",
            key,
        )
        for member_table, values in members[:-1]:
            connection.execute(
                f"INSERT INTO {member_table} VALUES ({', '.join('?' for _ in values)})",
                values,
            )
        connection.commit()
        connection.execute("PRAGMA foreign_keys = ON")
        assert connection.execute(f"SELECT * FROM {view}").fetchall() == []
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            connection.execute(
                f"INSERT INTO {seal} VALUES ({', '.join('?' for _ in key)})",
                key,
            )
        connection.rollback()
        connection.execute("PRAGMA foreign_keys = OFF")
        final_member, final_values = members[-1]
        connection.execute(
            f"INSERT INTO {final_member} VALUES "
            f"({', '.join('?' for _ in final_values)})",
            final_values,
        )
        connection.commit()
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            f"INSERT INTO {seal} VALUES ({', '.join('?' for _ in key)})",
            key,
        )
        assert connection.execute(f"SELECT * FROM {view}").fetchone() == expected
        with pytest.raises(sqlite3.OperationalError, match="view"):
            connection.execute(
                f"INSERT INTO {view} VALUES ({', '.join('?' for _ in expected)})",
                expected,
            )
        first_column = connection.execute(f"PRAGMA table_info({view})").fetchone()[1]
        with pytest.raises(sqlite3.OperationalError, match="view"):
            connection.execute(f"UPDATE {view} SET {first_column} = {first_column}")
        with pytest.raises(sqlite3.OperationalError, match="view"):
            connection.execute(f"DELETE FROM {view}")
    finally:
        connection.close()


def test_batch2_page_families_are_total_share_one_seal_and_are_read_only() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    value_sha256 = b"v" * 32
    canonical_page_sha256 = b"c" * 32
    gallery_page_sha256 = b"g" * 32
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            "INSERT INTO catalog_canonical_digest_policies VALUES (?)",
            (b"source_root_v1",),
        )
        connection.execute(
            "INSERT INTO catalog_canonical_value_allocation_anchors VALUES (?)",
            (value_sha256,),
        )
        connection.execute(
            "INSERT INTO catalog_canonical_value_allocation_digest_domains "
            "VALUES (?, ?)",
            (value_sha256, b"source_root_v1"),
        )
        connection.execute(
            "INSERT INTO catalog_canonical_value_allocation_byte_counts VALUES (?, 3)",
            (value_sha256,),
        )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            connection.execute(
                "INSERT INTO catalog_canonical_value_allocation_seals VALUES (?)",
                (value_sha256,),
            )
        connection.execute(
            "INSERT INTO catalog_canonical_value_allocation_allocated_ats "
            "VALUES (?, 1)",
            (value_sha256,),
        )
        connection.execute(
            "INSERT INTO catalog_canonical_value_allocation_seals VALUES (?)",
            (value_sha256,),
        )
        assert connection.execute(
            "SELECT * FROM catalog_canonical_value_allocations"
        ).fetchone() == (value_sha256, b"source_root_v1", 3, 1)

        connection.execute(
            "INSERT INTO catalog_canonical_value_page_anchors VALUES (?)",
            (canonical_page_sha256,),
        )
        connection.execute(
            "INSERT INTO catalog_canonical_value_page_payloads VALUES (?, ?)",
            (canonical_page_sha256, b"canonical-page"),
        )
        connection.execute(
            "INSERT INTO catalog_canonical_value_page_coordinates VALUES (?, 0, 0, ?)",
            (value_sha256, canonical_page_sha256),
        )
        assert (
            connection.execute("SELECT * FROM catalog_canonical_value_pages").fetchall()
            == []
        )
        assert (
            connection.execute(
                "SELECT * FROM catalog_canonical_value_page_descriptors"
            ).fetchall()
            == []
        )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            connection.execute(
                "INSERT INTO catalog_canonical_value_page_seals VALUES (?)",
                (canonical_page_sha256,),
            )
        connection.execute(
            "INSERT INTO catalog_canonical_value_page_subtree_item_counts "
            "VALUES (?, 3)",
            (canonical_page_sha256,),
        )
        connection.execute(
            "INSERT INTO catalog_canonical_value_page_seals VALUES (?)",
            (canonical_page_sha256,),
        )
        assert connection.execute(
            "SELECT * FROM catalog_canonical_value_pages"
        ).fetchone() == (canonical_page_sha256, value_sha256, b"canonical-page")
        assert connection.execute(
            "SELECT * FROM catalog_canonical_value_page_descriptors"
        ).fetchone() == (canonical_page_sha256, value_sha256, 0, 0, 3)

        connection.execute(
            "INSERT INTO catalog_gallery_observation_page_descriptor_anchors "
            "VALUES (?)",
            (gallery_page_sha256,),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_pages VALUES (?, ?)",
            (gallery_page_sha256, b"gallery-page"),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_page_descriptor_components "
            "VALUES (?, ?)",
            (gallery_page_sha256, b"FILE"),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_page_descriptor_levels "
            "VALUES (?, 0)",
            (gallery_page_sha256,),
        )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            connection.execute(
                "INSERT INTO catalog_gallery_observation_page_descriptor_seals "
                "VALUES (?)",
                (gallery_page_sha256,),
            )
        connection.execute(
            "INSERT INTO "
            "catalog_gallery_observation_page_descriptor_subtree_item_counts "
            "VALUES (?, 0)",
            (gallery_page_sha256,),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_page_descriptor_seals VALUES (?)",
            (gallery_page_sha256,),
        )
        assert connection.execute(
            "SELECT * FROM catalog_gallery_observation_page_descriptors"
        ).fetchone() == (gallery_page_sha256, b"FILE", 0, 0)

        connection.execute(
            "INSERT INTO catalog_gallery_observation_page_key_bounds_anchors "
            "VALUES (?)",
            (gallery_page_sha256,),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_page_key_bounds_first_keys "
            "VALUES (?, ?)",
            (gallery_page_sha256, b"a"),
        )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            connection.execute(
                "INSERT INTO catalog_gallery_observation_page_key_bounds_seals "
                "VALUES (?)",
                (gallery_page_sha256,),
            )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_page_key_bounds_last_keys "
            "VALUES (?, ?)",
            (gallery_page_sha256, b"z"),
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_page_key_bounds_seals VALUES (?)",
            (gallery_page_sha256,),
        )
        assert connection.execute(
            "SELECT * FROM catalog_gallery_observation_page_key_bounds"
        ).fetchone() == (gallery_page_sha256, b"a", b"z")

        for view, values in (
            (
                "catalog_canonical_value_allocations",
                (value_sha256, b"source_root_v1", 3, 1),
            ),
            (
                "catalog_canonical_value_pages",
                (canonical_page_sha256, value_sha256, b"canonical-page"),
            ),
            (
                "catalog_canonical_value_page_descriptors",
                (canonical_page_sha256, value_sha256, 0, 0, 3),
            ),
            (
                "catalog_gallery_observation_page_descriptors",
                (gallery_page_sha256, b"FILE", 0, 0),
            ),
            (
                "catalog_gallery_observation_page_key_bounds",
                (gallery_page_sha256, b"a", b"z"),
            ),
        ):
            with pytest.raises(sqlite3.OperationalError, match="view"):
                connection.execute(
                    f"INSERT INTO {view} VALUES ({', '.join('?' for _ in values)})",
                    values,
                )
    finally:
        connection.close()


def test_secondary_sealed_projection_metadata_drift_is_rejected() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    page = physical_spec.relation("canonical_value_page")
    assert page is not None
    assert page.vertical_view is not None
    broken_page = replace(
        page,
        vertical_view=replace(
            page.vertical_view,
            projection_attributes=(
                "page_sha256",
                "value_sha256",
                "page_bytes",
                "level",
            ),
        ),
    )
    broken = replace(
        physical_spec,
        relations=tuple(
            broken_page if relation is page else relation
            for relation in physical_spec.relations
        ),
    )

    with pytest.raises(ValueError, match="share one sealed family"):
        refinement._validate_physical_schema(broken, logical)


def test_artifact_producer_view_requires_natural_identity_and_seal() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    digest = b"d" * 32
    producer_fields = (b"writer", b"python", b"pillow", b"jpeg", b"zlib")
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute(
            "INSERT INTO catalog_artifact_producer_fingerprint_anchors VALUES (?)",
            (digest,),
        )
        connection.execute(
            "INSERT INTO catalog_artifact_producer_fingerprint_algorithm_versions "
            "VALUES (?, ?)",
            (digest, 1),
        )
        connection.execute(
            "INSERT INTO catalog_artifact_producer_fingerprint_equivalence_classes "
            "VALUES (?, ?)",
            (digest, b"equivalent"),
        )
        connection.commit()
        connection.execute("PRAGMA foreign_keys = ON")
        assert (
            connection.execute(
                "SELECT * FROM catalog_artifact_producer_fingerprints"
            ).fetchall()
            == []
        )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            connection.execute(
                "INSERT INTO catalog_artifact_producer_fingerprint_seals VALUES (?)",
                (digest,),
            )
        connection.execute(
            "INSERT INTO catalog_artifact_producer_fingerprint_identities "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (*producer_fields, digest),
        )
        connection.execute(
            "INSERT INTO catalog_artifact_producer_fingerprint_seals VALUES (?)",
            (digest,),
        )
        assert connection.execute(
            "SELECT * FROM catalog_artifact_producer_fingerprints"
        ).fetchone() == (digest, 1, b"equivalent", *producer_fields)
        with pytest.raises(sqlite3.OperationalError, match="view"):
            connection.execute(
                "INSERT INTO catalog_artifact_producer_fingerprints "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (digest, 1, b"equivalent", *producer_fields),
            )
    finally:
        connection.close()


def test_generation_views_derive_one_mapping_value_and_are_read_only() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    build_id = b"b" * 16
    candidate_id = b"c" * 16
    channel = b"default"
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.execute("PRAGMA foreign_keys = OFF")
        receipt_id = b"r" * 16
        preparation_id = b"p" * 16
        snapshot_manifest = b"s" * 32
        for statement, values in (
            (
                "INSERT INTO catalog_source_revision_anchors VALUES (?)",
                (7,),
            ),
            (
                "INSERT INTO catalog_source_revision_channels VALUES (?, ?)",
                (7, channel),
            ),
            (
                "INSERT INTO catalog_source_revision_snapshot_manifests VALUES (?, ?)",
                (7, snapshot_manifest),
            ),
            (
                "INSERT INTO catalog_source_revision_descriptor_seals VALUES (?)",
                (7,),
            ),
            (
                "INSERT INTO catalog_publication_commit_anchors VALUES (?)",
                (receipt_id,),
            ),
            (
                "INSERT INTO catalog_publication_commit_candidates VALUES (?, ?)",
                (receipt_id, candidate_id),
            ),
            (
                "INSERT INTO catalog_publication_commit_catalog_revisions VALUES (?, ?)",
                (receipt_id, 9),
            ),
            (
                "INSERT INTO catalog_publication_commit_source_revisions VALUES (?, ?)",
                (receipt_id, 7),
            ),
            (
                "INSERT INTO catalog_publication_commit_generations VALUES (?, ?)",
                (receipt_id, 4),
            ),
            (
                "INSERT INTO catalog_publication_commit_operational_preparations "
                "VALUES (?, ?)",
                (receipt_id, preparation_id),
            ),
            (
                "INSERT INTO catalog_publication_commit_operational_policies "
                "VALUES (?, ?)",
                (receipt_id, 1),
            ),
            (
                "INSERT INTO catalog_publication_commit_artifact_policies VALUES (?, ?)",
                (receipt_id, 1),
            ),
            (
                "INSERT INTO catalog_publication_commit_display_title_policies "
                "VALUES (?, ?)",
                (receipt_id, 1),
            ),
            (
                "INSERT INTO catalog_publication_commit_new_galleries VALUES (?, ?)",
                (receipt_id, 1),
            ),
            (
                "INSERT INTO catalog_publication_commit_changed_galleries "
                "VALUES (?, ?)",
                (receipt_id, 2),
            ),
            (
                "INSERT INTO catalog_publication_commit_removed_galleries VALUES (?, ?)",
                (receipt_id, 3),
            ),
            (
                "INSERT INTO catalog_publication_commit_duplicate_losers VALUES (?, ?)",
                (receipt_id, 4),
            ),
            (
                "INSERT INTO catalog_publication_commit_committed_ats VALUES (?, ?)",
                (receipt_id, 12),
            ),
            (
                "INSERT INTO catalog_publication_commit_seals VALUES (?)",
                (receipt_id,),
            ),
            (
                "INSERT INTO catalog_source_build_base_publication_commits VALUES (?, ?)",
                (build_id, receipt_id),
            ),
            (
                "INSERT INTO catalog_publication_candidate_base_publication_commits "
                "VALUES (?, ?)",
                (candidate_id, receipt_id),
            ),
            (
                "INSERT INTO catalog_publication_commit_head_receipts VALUES (?, ?)",
                (channel, receipt_id),
            ),
        ):
            connection.execute(statement, values)

        assert connection.execute(
            "SELECT * FROM catalog_source_build_base_source"
        ).fetchone() == (build_id, 7, 4)
        assert connection.execute("SELECT * FROM catalog_source_heads").fetchone() == (
            channel,
            7,
            4,
            12,
        )
        assert connection.execute(
            "SELECT * FROM catalog_publication_candidate_base_catalog"
        ).fetchone() == (candidate_id, 9, 4)
        assert connection.execute(
            "SELECT * FROM catalog_publication_heads"
        ).fetchone() == (channel, 9, 4, 12)
        with pytest.raises(sqlite3.OperationalError, match="view"):
            connection.execute(
                "INSERT INTO catalog_source_heads VALUES (?, ?, ?, ?)",
                (channel, 7, 4, 12),
            )
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("relation_name", "table", "dependency", "replacement"),
    [
        (
            "gallery_observation_metadata",
            "catalog_gallery_observation_metadata",
            "catalog_gallery_upload_times",
            "catalog_gallery_observation_download_times",
        ),
        (
            "source_build_discovery",
            "catalog_source_build_discoveries",
            "catalog_source_build_discovery_scan_attempts",
            "catalog_source_build_discovery_gallery_counts",
        ),
        (
            "gallery_observation_scan",
            "catalog_gallery_observation_scans",
            "catalog_gallery_observation_scan_observation_sha256s",
            "catalog_gallery_observation_scan_observation_versions",
        ),
        (
            "gallery_observation_directory",
            "catalog_gallery_observation_directories",
            "catalog_gallery_observation_directory_entry_counts",
            "catalog_gallery_observation_directory_observation_sha256s",
        ),
        (
            "gallery_observation_stat",
            "catalog_gallery_observation_stat",
            "catalog_gallery_observation_stat_file_counts",
            "catalog_gallery_observation_stat_byte_counts",
        ),
        (
            "gallery_observation_file_filesystem",
            "catalog_gallery_observation_file_filesystem",
            "catalog_gallery_observation_file_filesystem_devices",
            "catalog_gallery_observation_file_filesystem_inodes",
        ),
        (
            "artifact_producer_fingerprint",
            "catalog_artifact_producer_fingerprints",
            "catalog_artifact_producer_fingerprint_algorithm_versions",
            "catalog_artifact_producer_fingerprint_equivalence_classes",
        ),
        (
            "source_build_base_source",
            "catalog_source_build_base_source",
            "catalog_source_build_base_publication_commits",
            "catalog_publication_candidate_base_publication_commits",
        ),
        (
            "publication_candidate_base_source",
            "catalog_publication_candidate_base_sources",
            "catalog_publication_candidate_base_publication_commits",
            "catalog_source_build_base_publication_commits",
        ),
        (
            "publication_candidate_base_catalog",
            "catalog_publication_candidate_base_catalog",
            "catalog_publication_candidate_base_publication_commits",
            "catalog_source_build_base_publication_commits",
        ),
        (
            "source_head",
            "catalog_source_heads",
            "catalog_publication_commit_heads",
            "catalog_publication_commits",
        ),
        (
            "publication_head",
            "catalog_publication_heads",
            "catalog_publication_commit_heads",
            "catalog_publication_commits",
        ),
    ],
)
def test_vertical_view_definition_drift_is_rejected(
    relation_name: str,
    table: str,
    dependency: str,
    replacement: str,
) -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        database = refinement.introspect_sqlite(_SQLiteConnectionReader(connection))
    finally:
        connection.close()

    vertical_view = database.table(table)
    assert vertical_view is not None
    assert vertical_view.definition is not None
    drifted_view = replace(
        vertical_view,
        definition=vertical_view.definition.replace(dependency, replacement),
    )
    assert drifted_view.definition != vertical_view.definition
    drifted = replace(
        database,
        tables=tuple(
            drifted_view if database_table is vertical_view else database_table
            for database_table in database.tables
        ),
    )

    report = refinement.compare_physical_refinement(
        logical,
        physical_spec,
        drifted,
    )

    assert not report.conforms
    assert any(
        mismatch.relation == relation_name and mismatch.code == "view-definition"
        for mismatch in report.mismatches
    )


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
        file_decisions = (
            (root, first_hash, 2, 1, 1),
            (root, removed_hash, 3, 1, 1),
            (middle, first_hash, 4, 2, 2),
        )
        connection.executemany(
            "INSERT INTO catalog_a_file_decision_shadow_anchors VALUES (?, ?)",
            (
                (analysis_id, file_sha256)
                for analysis_id, file_sha256, *_ in file_decisions
            ),
        )
        for table, position in (
            ("catalog_a_file_decision_shadow_occurrences", 2),
            ("catalog_a_file_decision_shadow_artists", 3),
            ("catalog_a_file_decision_shadow_gallery_artist_max", 4),
        ):
            connection.executemany(
                f"INSERT INTO {table} VALUES (?, ?, ?)",
                (
                    (analysis_id, file_sha256, row[position])
                    for row in file_decisions
                    for analysis_id, file_sha256 in (row[:2],)
                ),
            )
        connection.executemany(
            "INSERT INTO catalog_a_file_decision_shadow_seals VALUES (?, ?)",
            (
                (analysis_id, file_sha256)
                for analysis_id, file_sha256, *_ in file_decisions
            ),
        )
        connection.execute(
            "INSERT INTO catalog_analysis_file_hash_decision_tombstone VALUES (?, ?)",
            (middle, removed_hash),
        )

        assert (
            connection.execute(
                """
            SELECT file_sha256, occurrence_count, artist_count,
                   maximum_gallery_artist_count
            FROM catalog_analysis_file_hash_decision_resolved
            WHERE analysis_id = ?
            ORDER BY file_sha256
            """,
                (leaf,),
            ).fetchall()
            == [(first_hash, 4, 2, 2)]
        )
        assert (
            connection.execute(
                """
            SELECT file_sha256, occurrence_count, artist_count,
                   maximum_gallery_artist_count
            FROM catalog_analysis_file_hash_decision_resolved
            WHERE analysis_id = ?
            ORDER BY file_sha256
            """,
                (root,),
            ).fetchall()
            == [
                (first_hash, 2, 1, 1),
                (removed_hash, 3, 1, 1),
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
            "INSERT INTO catalog_a_content_owner_shadow_galleries VALUES (?, ?, ?)",
            (analysis_id, b"c" * 32, 1),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_a_content_owner_shadow_galleries VALUES (?, ?, ?)",
                (analysis_id, b"d" * 32, 1),
            )

        connection.execute(
            "INSERT INTO catalog_analysis_gid_winner_selections VALUES (?, ?)",
            (analysis_id, 1),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_analysis_gid_winner_selections VALUES (?, ?)",
                (analysis_id, 1),
            )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_analysis_exclusion_delta_old_excluded_flags "
                "VALUES (?, ?, ?)",
                (analysis_id, b"6" * 32, 2),
            )

        stage = b"changed_gallery"
        for statement, values in (
            (
                "INSERT INTO catalog_analysis_batch_receipt_anchors VALUES (?, ?, ?)",
                (analysis_id, stage, 1),
            ),
            (
                "INSERT INTO catalog_analysis_batch_receipt_coordinates "
                "VALUES (?, ?, ?, ?)",
                (analysis_id, stage, b"batch-1", 1),
            ),
            (
                "INSERT INTO catalog_analysis_batch_receipt_start_cursors "
                "VALUES (?, ?, ?, ?)",
                (analysis_id, stage, 1, b""),
            ),
            (
                "INSERT INTO catalog_analysis_batch_receipt_start_processed_counts "
                "VALUES (?, ?, ?, ?)",
                (analysis_id, stage, 1, 0),
            ),
            (
                "INSERT INTO catalog_analysis_batch_receipt_page_limits "
                "VALUES (?, ?, ?, ?)",
                (analysis_id, stage, 1, 128),
            ),
            (
                "INSERT INTO catalog_analysis_batch_receipt_next_cursors "
                "VALUES (?, ?, ?, ?)",
                (analysis_id, stage, 1, b"next"),
            ),
            (
                "INSERT INTO catalog_analysis_batch_receipt_row_counts "
                "VALUES (?, ?, ?, ?)",
                (analysis_id, stage, 1, 2),
            ),
            (
                "INSERT INTO catalog_analysis_batch_receipt_committed_ats "
                "VALUES (?, ?, ?, ?)",
                (analysis_id, stage, 1, 1_767_225_602_000_000),
            ),
            (
                "INSERT INTO catalog_analysis_batch_receipt_seals VALUES (?, ?, ?)",
                (analysis_id, stage, 1),
            ),
        ):
            connection.execute(statement, values)
        assert connection.execute(
            "SELECT row_count FROM catalog_analysis_batch_receipts"
        ).fetchone() == (2,)

        # state_component is an exact binary domain.  Its enum predicate must
        # use binary literals as well; SQLite TEXT literals can never satisfy
        # the simultaneous typeof(...)=blob constraint.
        connection.execute(
            "INSERT INTO catalog_analysis_state_component_anchors VALUES (?, ?)",
            (analysis_id, b"file_hash_decision"),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_analysis_state_component_anchors VALUES (?, ?)",
                (analysis_id, "content_owner"),
            )
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
                "INSERT INTO catalog_source_revision_anchors VALUES (?)",
                (0,),
            )
        connection.execute(
            "INSERT INTO catalog_source_revision_anchors VALUES (?)",
            (1,),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_revision_anchors VALUES (?)",
                (0,),
            )
        connection.execute(
            "INSERT INTO catalog_revision_anchors VALUES (?)",
            (1,),
        )
        connection.execute(
            "INSERT INTO catalog_publication_generation_nodes VALUES (?)",
            (0,),
        )
        connection.execute(
            "INSERT INTO catalog_publication_generation_nodes VALUES (?)",
            (1,),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_publication_generation_successors VALUES (?, ?)",
                (0, 0),
            )
        connection.execute(
            "INSERT INTO catalog_publication_generation_successors VALUES (?, ?)",
            (1, 0),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_source_build_base_publication_commits "
                "VALUES (?, ?)",
                (b"b" * 16, b"r" * 15),
            )
        connection.execute(
            "INSERT INTO catalog_source_build_base_publication_commits VALUES (?, ?)",
            (b"b" * 16, b"r" * 16),
        )

        connection.execute(
            "INSERT INTO catalog_title_sort_policy_anchors VALUES (?)", (1,)
        )
        connection.execute(
            "INSERT INTO catalog_title_sort_policy_algorithm_versions VALUES (?, ?)",
            (1, 1),
        )
        connection.execute(
            "INSERT INTO catalog_title_sort_policy_unicode_data_versions VALUES (?, ?)",
            (1, b"16.0.0"),
        )
        connection.execute(
            "INSERT INTO catalog_title_sort_policy_identities VALUES (?, ?, ?)",
            (1, b"16.0.0", 1),
        )
        connection.execute(
            "INSERT INTO catalog_title_sort_policy_seals VALUES (?)", (1,)
        )
        for malformed_unicode_version in (b"", b"v" * 33, "16.0.0"):
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    "INSERT INTO catalog_title_sort_policy_unicode_data_versions "
                    "VALUES (?, ?)",
                    (2, malformed_unicode_version),
                )

        builds = (
            (b"c" * 16, b"d" * 32, "OPEN", 2),
            (b"e" * 16, b"f" * 32, "SEALED", None),
            (b"g" * 16, b"h" * 32, "ABANDONED", 2),
            (b"i" * 16, b"j" * 32, "ABANDONED", None),
        )
        for build_id, scope_key, state, sealed_at in builds:
            connection.execute(
                "INSERT INTO catalog_source_build_anchors VALUES (?)", (build_id,)
            )
            connection.execute(
                "INSERT INTO catalog_source_build_scope_keys VALUES (?, ?)",
                (build_id, scope_key),
            )
            connection.execute(
                "INSERT INTO catalog_source_build_manifest_policy_ids VALUES (?, ?)",
                (build_id, 1),
            )
            connection.execute(
                "INSERT INTO catalog_source_build_states VALUES (?, ?)",
                (build_id, state),
            )
            connection.execute(
                "INSERT INTO catalog_source_build_created_ats VALUES (?, ?)",
                (build_id, 1),
            )
            connection.execute(
                "INSERT INTO catalog_source_build_descriptor_seals VALUES (?)",
                (build_id,),
            )
            if sealed_at is not None:
                connection.execute(
                    "INSERT INTO catalog_source_build_sealed_ats VALUES (?, ?)",
                    (build_id, sealed_at),
                )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_source_build_sealed_ats VALUES (?, ?)",
                (b"z" * 16, None),
            )
        assert connection.execute(
            "SELECT build_id, state, sealed_at FROM catalog_source_builds"
        ).fetchall() == [(b"i" * 16, "ABANDONED", None)]

        analyses = (
            (b"k" * 16, b"K" * 16, 1, "COMPLETE", 2),
            (b"l" * 16, b"L" * 16, 2, "COMPLETE", None),
            (b"m" * 16, b"M" * 16, 3, "OPEN", 2),
            (b"n" * 16, b"N" * 16, 4, "ABANDONED", None),
        )
        for analysis_id, build_id, policy_id, state, completed_at in analyses:
            connection.execute(
                "INSERT INTO catalog_analysis_run_anchors VALUES (?)", (analysis_id,)
            )
            connection.execute(
                "INSERT INTO catalog_analysis_run_build_ids VALUES (?, ?)",
                (analysis_id, build_id),
            )
            connection.execute(
                "INSERT INTO catalog_analysis_run_policy_ids VALUES (?, ?)",
                (analysis_id, policy_id),
            )
            connection.execute(
                "INSERT INTO catalog_analysis_run_input_manifest_sha256s VALUES (?, ?)",
                (analysis_id, bytes([policy_id]) * 32),
            )
            connection.execute(
                "INSERT INTO catalog_analysis_run_identities VALUES (?, ?, ?)",
                (build_id, policy_id, analysis_id),
            )
            connection.execute(
                "INSERT INTO catalog_analysis_run_started_ats VALUES (?, 1)",
                (analysis_id,),
            )
            connection.execute(
                "INSERT INTO catalog_analysis_run_states VALUES (?, ?)",
                (analysis_id, state),
            )
            connection.execute(
                "INSERT INTO catalog_analysis_run_descriptor_seals VALUES (?)",
                (analysis_id,),
            )
            if completed_at is not None:
                connection.execute(
                    "INSERT INTO catalog_analysis_run_completed_ats VALUES (?, ?)",
                    (analysis_id, completed_at),
                )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_analysis_run_completed_ats VALUES (?, ?)",
                (b"z" * 16, None),
            )
        assert connection.execute(
            "SELECT analysis_id, state, completed_at FROM catalog_analysis_runs "
            "ORDER BY analysis_id"
        ).fetchall() == [
            (b"k" * 16, "COMPLETE", 2),
            (b"n" * 16, "ABANDONED", None),
        ]

        for invalid_page_limit in (0, 129):
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    "INSERT INTO catalog_analysis_batch_receipt_page_limits "
                    "VALUES (?, ?, ?, ?)",
                    (
                        b"x" * 16,
                        b"changed_gallery",
                        invalid_page_limit + 1,
                        invalid_page_limit,
                    ),
                )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_gallery_observation_page_counts VALUES (?, ?, ?)",
                (1, 1, 4_294_967_296),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_gallery_observation_scan_observation_versions "
                "VALUES (?, ?, ?)",
                (1, 1, 4_294_967_296),
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
                "INSERT INTO catalog_analysis_checkpoint_cursors VALUES (?, ?, ?)",
                (b"a" * 16, b"HASH", b"c" * 2049),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_publication_checkpoint_cursors VALUES (?, ?, ?)",
                (b"p" * 16, b"ITEMS", b"c" * 2049),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_prepared_artifact_protection_tokens "
                "VALUES (?, ?, ?)",
                (
                    b"q" * 16,
                    b"k" * 32,
                    b"t" * 185,
                ),
            )
    finally:
        connection.close()


def test_sqlite_vertical_identity_cannot_disagree_with_narrow_facts() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.execute(
            "INSERT INTO catalog_manifest_policy_anchors VALUES (?)", (1,)
        )
        connection.execute(
            "INSERT INTO catalog_manifest_policy_manifest_algorithm_versions "
            "VALUES (?, ?)",
            (1, 1),
        )
        connection.execute(
            "INSERT INTO catalog_manifest_policy_file_order_versions VALUES (?, ?)",
            (1, 1),
        )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_manifest_policy_identities VALUES (?, ?, ?)",
                (2, 1, 1),
            )

        connection.execute(
            "INSERT INTO catalog_manifest_policy_identities VALUES (?, ?, ?)",
            (1, 1, 1),
        )
        connection.execute("INSERT INTO catalog_manifest_policy_seals VALUES (?)", (1,))
        assert connection.execute(
            "SELECT * FROM catalog_manifest_policies"
        ).fetchall() == [(1, 1, 1)]
    finally:
        connection.close()


def test_mariadb_renderer_preserves_exact_binary_types_checks_and_views() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    statements = refinement.render_mariadb_ddl(physical_spec)
    ddl = "\n".join(statements)
    external_relations = refinement._catalog_external_physical_relations()

    assert len(statements) == len(physical_spec.implemented_relations) + len(
        external_relations
    )
    assert sum(value.startswith("CREATE TABLE") for value in statements) == sum(
        relation.kind == "table" for relation in physical_spec.implemented_relations
    ) + len(external_relations)
    external_tables = tuple(relation.table for relation in external_relations.values())
    assert all(table is not None for table in external_tables)
    assert (
        tuple(
            statement.partition("`")[2].partition("`")[0]
            for statement in statements[: len(external_tables)]
        )
        == external_tables
    )
    commit_preparation_position = next(
        position
        for position, statement in enumerate(statements)
        if statement.startswith(
            "CREATE TABLE `catalog_publication_commit_operational_preparations`"
        )
    )
    assert (
        statements.index(
            next(
                statement
                for statement in statements
                if statement.startswith(
                    "CREATE TABLE `operational_operational_preparation_effect_seals`"
                )
            )
        )
        < commit_preparation_position
    )
    assert (
        sum(
            value.startswith("CREATE SQL SECURITY INVOKER VIEW") for value in statements
        )
        == 81
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
    assert "octet_length(protection_token) = 184" in ddl
    assert "`summary_sha256` BINARY(32) NOT NULL" in ddl
    assert "`language_sha256` BINARY(32) NOT NULL" in ddl
    assert "`artifact_locator_sha256` BINARY(32) NOT NULL" in ddl
    assert "KEY `ix_gallery_file_hash`" in ddl
    assert "`priority_key`" not in ddl
    assert "old_excluded IN (0, 1)" in ddl
    assert "new_excluded IN (0, 1)" in ddl
    assert "KEY `ix_a_content_candidate_group`" in ddl
    assert "catalog_analysis_gid_winner_selections" in ddl
    assert "ix_analysis_gid_candidate_order" not in ddl
    assert (
        "CREATE SQL SECURITY INVOKER VIEW `catalog_analysis_gid_winner_shadows`" in ddl
    )
    assert "KEY `ix_fk_canonical_value_allocation_digest_domain_2_digest_domain`" in ddl
    assert "KEY `ix_fk_canonical_value_page_1_value_sha256`" not in ddl
    assert (
        "CREATE SQL SECURITY INVOKER VIEW `catalog_analysis_file_hash_decision_resolved`"
        in ddl
    )
    analysis_receipt_ddl = next(
        statement
        for statement in statements
        if statement.startswith(
            "CREATE SQL SECURITY INVOKER VIEW `catalog_analysis_batch_receipts`"
        )
    )
    assert "CAST(CASE WHEN stored.`row_count` = 0 THEN 1 ELSE 0 END AS UNSIGNED)" in (
        analysis_receipt_ddl
    )
    assert (
        "CAST(CASE WHEN stored.`row_count` = 0 THEN 'COMPLETE' ELSE 'OPEN' END "
        "AS CHAR(32) CHARSET ascii) COLLATE ascii_bin"
    ) in analysis_receipt_ddl
    analysis_receipt = physical_spec.relation("analysis_batch_receipt")
    assert analysis_receipt is not None
    terminal_column = next(
        column for column in analysis_receipt.columns if column.attribute == "terminal"
    )
    assert terminal_column.mariadb.type_name == "BIGINT UNSIGNED"
    next_state_column = next(
        column
        for column in analysis_receipt.columns
        if column.attribute == "next_state"
    )
    assert next_state_column.mariadb.type_name == "VARCHAR(32)"
    assert next_state_column.mariadb.collation == "ascii_bin"
    assert next_state_column.mariadb.nullable
    assert not next_state_column.sqlite.nullable
    assert "`value_bytes`" not in ddl
    constraint_names = re.findall(r"CONSTRAINT `([^`]+)`", ddl)
    assert constraint_names
    assert all(len(name.encode("ascii")) <= 63 for name in constraint_names)
    for relation in physical_spec.implemented_relations:
        if relation.kind != "table" or relation.table is None:
            continue
        unique_keys = (*relation.unique_keys, *relation.referential_unique_keys)
        for position, _key in enumerate(unique_keys, start=1):
            raw_name = f"uk_{relation.table}_{position}"
            expected_name = (
                raw_name
                if len(raw_name.encode("ascii")) <= 63
                else f"{raw_name[:50]}_"
                f"{hashlib.sha256(raw_name.encode('ascii')).hexdigest()[:12]}"
            )
            assert refinement._portable_identifier(raw_name) == expected_name
            assert f"CONSTRAINT `{expected_name}` UNIQUE" in ddl
    with pytest.raises(ValueError, match="portable 63-byte identifier domain"):
        refinement._validate_identifier("x" * 64, "test identifier")
    assert refinement.maximum_mariadb_index_width(physical_spec)[0] == 640
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


def test_mariadb_view_normalizer_preserves_semantic_parentheses_and_predicates() -> (
    None
):
    table_names = ("catalog_a", "catalog_b", "catalog_tomb")
    expected = """SELECT MAX(a.score) + b.offset AS total
    FROM catalog_a AS a
    JOIN catalog_b AS b ON b.id = a.id
    WHERE NOT EXISTS (
      SELECT 1 FROM catalog_tomb AS tomb WHERE tomb.id = a.id
    )"""
    actual = """select max(a.score) + b.offset AS total
    from (test_database.catalog_a a
      join test_database.catalog_b b on(b.id = a.id))
    where !exists(select 1 from test_database.catalog_tomb tomb
      where tomb.id = a.id limit 1)"""

    expected_normalized = refinement._normalize_view_definition(
        expected,
        table_names=table_names,
        collapse_inner_join_tree=True,
    )
    actual_normalized = refinement._normalize_view_definition(
        actual,
        table_names=table_names,
        collapse_inner_join_tree=True,
    )

    assert actual_normalized == expected_normalized
    for semantic_drift in (
        actual.replace(
            "join test_database.catalog_b", "left join test_database.catalog_b"
        ),
        actual.replace("tomb.id = a.id", "tomb.id <> a.id"),
        actual.replace("max(a.score) + b.offset", "max(a.score + b.offset)"),
    ):
        assert (
            refinement._normalize_view_definition(
                semantic_drift,
                table_names=table_names,
                collapse_inner_join_tree=True,
            )
            != expected_normalized
        )


def test_analysis_ancestry_endpoint_view_is_portable_and_uses_max_depth() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    relation_by_name = {
        relation.relation: relation for relation in physical_spec.relations
    }
    relation = relation_by_name["analysis_state_anchor"]
    for backend in ("sqlite", "mariadb"):
        view = refinement._render_view(
            relation,
            relation_by_name,
            backend,
            idempotent=True,
        )
        assert "NOT EXISTS" in view
        assert "catalog_analysis_state_ancestry" in view
        assert "ancestor_depth" in view


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
            "INSERT INTO catalog_gallery_observation_metadata_anchors VALUES (%s, %s)",
            (1, 1),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_upload_times VALUES (%s, %s)",
            (1, 1_767_225_600_000_000),
        )
        connector.execute(
            "INSERT INTO catalog_source_gallery_name_gids VALUES (%s, %s)",
            (b"gallery-1", 1),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_source_name_accesses VALUES (%s, %s)",
            (1, b"gallery-1"),
        )
        for table in (
            "catalog_gallery_observation_download_times",
            "catalog_gallery_observation_modified_times",
        ):
            connector.execute(
                f"INSERT INTO {table} VALUES (%s, %s, %s)",
                (1, 1, 1_767_225_600_000_000),
            )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_metadata_seals VALUES (%s, %s)",
            (1, 1),
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
    assert database.table("catalog_analysis_gid_candidates") is None
    selection = database.table("catalog_analysis_gid_winner_selections")
    assert selection is not None
    assert selection.columns == (
        "analysis_id",
        "winner_gallery_id",
    )
    refinement.assert_physical_refines(report)


def test_current_fresh_sqlite_schema_refines_source_slice(
    sqlite_config: CoreConfig,
) -> None:
    VNextDatabaseAdminFacade(sqlite_config).initialize()
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    with SQLiteConnector(database=sqlite_config.database.database) as connector:
        database = refinement.introspect_sqlite(connector)

    report = refinement.compare_physical_refinement(
        logical,
        physical_spec,
        database,
    )

    assert report.conforms, report.render()
    assert report.fully_conforms
    assert database.table("catalog_gallery_observation_files") is not None
    refinement.assert_physical_refines(report)


def test_current_fresh_mariadb_schema_refines_source_slice(
    mariadb_config: CoreConfig,
) -> None:
    VNextDatabaseAdminFacade(mariadb_config).initialize()
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

    assert report.conforms, report.render()
    assert report.fully_conforms
    assert report.backend == "mariadb"
    refinement.assert_physical_refines(report)
