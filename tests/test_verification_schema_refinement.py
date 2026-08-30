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
    assert len(seeds) == 65
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
    assert len(bindings) == len(owners) == len(document["runtime_obligations"]) == 86
    assert len({binding["path"] for binding in bindings}) == len(bindings)
    assert tuple(binding["text"] for binding in bindings) == tuple(
        document["runtime_obligations"]
    )
    assert {
        binding["path"]: binding["semantic_obligation_id"] for binding in bindings
    } == owners


def test_snapshot_audit_digests_do_not_fk_pin_canonical_payload() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.execute("PRAGMA foreign_keys = ON")

        source_fks = {
            (row[3], row[2], row[4])
            for row in connection.execute(
                "PRAGMA foreign_key_list(catalog_source_revision_descriptors)"
            )
        }
        analysis_fks = {
            (row[3], row[2], row[4])
            for row in connection.execute(
                "PRAGMA foreign_key_list(catalog_analysis_snapshot_manifest)"
            )
        }
        assert source_fks == {
            (
                "channel",
                "catalog_channel_registry",
                "channel",
            )
        }
        assert analysis_fks == {
            (
                "analysis_id",
                "catalog_analysis_run_descriptor",
                "analysis_id",
            )
        }

        missing_payload_digest = b"x" * 32
        connection.execute(
            "INSERT INTO catalog_channel_registry (channel) VALUES (?)",
            (b"default",),
        )
        connection.execute(
            "INSERT INTO catalog_source_revision_descriptors "
            "(source_revision, channel, snapshot_manifest_sha256) VALUES (1, ?, ?)",
            (b"default", missing_payload_digest),
        )
        assert connection.execute(
            "SELECT snapshot_manifest_sha256 FROM catalog_source_revision_descriptors"
        ).fetchone() == (missing_payload_digest,)
        assert (
            connection.execute(
                "SELECT 1 FROM catalog_source_snapshot_manifest_identity"
            ).fetchone()
            is None
        )
    finally:
        connection.close()


def test_physical_loader_rejects_bootstrap_partition_drift(tmp_path: Path) -> None:
    logical = refinement.load_logical_schema(CATALOG)
    original = PHYSICAL.read_text(encoding="utf-8")
    invalid = original.replace(
        'seeded_relations = ["canonical_digest_policy", "channel_registry", '
        '"source_provider_registry", "analysis_stage"',
        'seeded_relations = ["canonical_digest_policy", "channel_registry", '
        '"source_provider_registry", "manifest_policy"',
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

    assert len(logical.relations) == 198
    assert len(physical_spec.implemented_relations) == 185
    assert set(physical_spec.inline_projections) == {
        "canonical_value_page",
        "canonical_value_page_descriptor",
        "source_build_base_source",
        "gallery_observation_page_descriptor",
        "gallery_observation_file",
        "build_manifest",
        "analysis_exclusion_delta",
        "publication_candidate_base_source",
        "artifact_delta_new",
        "catalog_revision_generation",
        "publication_head_revision",
        "publication_head_advanced_at",
        "publication_head",
    }
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
    manifest_policy = physical_spec.relation("manifest_policy")
    assert manifest_policy is not None
    assert tuple(column.attribute for column in manifest_policy.columns) == (
        "manifest_policy_id",
        "manifest_algorithm_version",
        "file_order_version",
    )
    assert manifest_policy.primary_key == ("manifest_policy_id",)
    assert manifest_policy.unique_keys == (
        ("manifest_algorithm_version", "file_order_version"),
    )
    assert manifest_policy.referential_unique_keys == (
        ("manifest_policy_id", "manifest_algorithm_version"),
        ("manifest_policy_id", "file_order_version"),
    )
    assert manifest_policy.kind == "table"
    publication = physical_spec.relation("catalog_publication")
    assert publication is not None
    assert tuple(column.attribute for column in publication.columns) == (
        "revision",
        "publication_key",
        "gallery_id",
        "summary_sha256",
        "language_sha256",
        "modified_at",
        "download_time",
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
    assert metadata.vertical_view is None
    assert metadata.derived_view == refinement.DerivedViewSpec(
        pattern="gallery_observation_metadata_projection",
        source_relations=(
            "gallery_observation_metadata_local",
            "gallery_source_name_access",
            "source_gallery_name_gid",
            "gallery_upload_time",
        ),
    )
    for relation_name in (
        "gallery_upload_time",
        "source_gallery_name_gid",
        "gallery_source_name_access",
        "gallery_observation_metadata_local",
    ):
        base = physical_spec.relation(relation_name)
        assert base is not None
        assert base.kind == "table"
        if relation_name == "gallery_observation_metadata_local":
            assert len(base.columns) - len(base.primary_key) == 2
        else:
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
    assert physical_spec.relation("canonical_value_page") is None
    canonical_page_payload = physical_spec.relation("canonical_value_page_payload")
    assert canonical_page_payload is not None
    assert canonical_page_payload.runtime_unique_keys == (("page_bytes",),)
    assert canonical_page_payload.columns[1].mariadb.type_name == "MEDIUMBLOB"
    content_candidate = physical_spec.relation(
        "analysis_content_owner_candidate_shadow"
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
        "cursor": 3,
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
                (3, 1, bytes(32), bytes(7)),
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
        "artifact_producer_fingerprint",
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


def test_impacted_gid_requires_exact_storage_and_provenance_views() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    storage = physical_spec.relation("analysis_impacted_gid_storage")
    assert storage is not None
    assert storage.kind == "table"
    assert storage.primary_key == ("analysis_id", "gid")

    broken_storage = replace(storage, primary_key=("gid", "analysis_id"))
    broken = replace(
        physical_spec,
        relations=tuple(
            broken_storage if relation is storage else relation
            for relation in physical_spec.relations
        ),
    )
    with pytest.raises(ValueError, match="storage/provenance recomposition"):
        refinement._validate_physical_schema(broken, logical)


def test_physical_metadata_projection_cannot_omit_an_authority_source() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    metadata = physical_spec.relation("gallery_observation_metadata")
    assert metadata is not None
    assert metadata.derived_view is not None
    broken_metadata = replace(
        metadata,
        derived_view=replace(
            metadata.derived_view,
            source_relations=metadata.derived_view.source_relations[:-1],
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
        match="gallery observation metadata source authority drifted",
    ):
        refinement._validate_physical_schema(broken, logical)


def test_physical_lifecycle_projection_sources_are_closed_and_directional() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    source_build = physical_spec.relation("source_build")
    assert source_build is not None
    assert source_build.vertical_view is None
    assert source_build.derived_view == refinement.DerivedViewSpec(
        pattern="lifecycle_projection",
        source_relations=(
            "source_build_descriptor",
            "source_build_state",
            "source_build_sealed_at",
        ),
    )
    broken_source_build = replace(
        source_build,
        derived_view=replace(
            source_build.derived_view,
            source_relations=(
                "source_build_descriptor",
                "source_build_state",
            ),
        ),
    )
    relation_by_name = {
        relation.relation: (
            broken_source_build if relation is source_build else relation
        )
        for relation in physical_spec.relations
    }
    with pytest.raises(
        ValueError,
        match="lifecycle_projection requires descriptor, state, terminal",
    ):
        refinement._render_view(
            broken_source_build,
            relation_by_name,
            "sqlite",
        )


def test_build_manifest_is_an_inline_projection_over_physical_authorities() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    assert physical_spec.relation("build_manifest") is None
    assert "build_manifest" in physical_spec.inline_projections
    assert {
        "build_manifest_core",
        "source_build_discovery",
        "source_build_sealed_at",
    } <= {relation.relation for relation in physical_spec.relations}


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
        for relation_name in ("source_build", "gallery_observation_metadata")
    }
    artifact = cast(dict[str, Any], ARTIFACT)
    provider_slices = dict(artifact["backends"][backend]["slices"])

    for relation_name in ("source_build", "gallery_observation_metadata"):
        provider_statements = provider_slices[f"relation:{relation_name}"]
        assert len(provider_statements) == 1
        assert provider_statements[0][3] == refinement_by_relation[relation_name]

    source_build_sql = refinement_by_relation["source_build"]
    assert "LEFT JOIN" in source_build_sql
    assert "state" in source_build_sql
    assert "'SEALED'" in source_build_sql
    assert "'OPEN', 'ABANDONED'" in source_build_sql
    assert "sealed_at" in source_build_sql
    assert physical_spec.relation("build_manifest") is None
    assert "relation:build_manifest" not in provider_slices


@pytest.mark.parametrize("backend", ["sqlite", "mariadb"])
def test_every_refinement_and_provider_view_is_exactly_equal(backend: str) -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    relation_by_name = {
        relation.relation: relation for relation in physical_spec.relations
    }
    artifact = cast(dict[str, Any], ARTIFACT)
    provider_slices = dict(artifact["backends"][backend]["slices"])

    for relation in physical_spec.relations:
        if relation.kind != "view":
            continue
        provider_statements = provider_slices[f"relation:{relation.relation}"]
        assert len(provider_statements) == 1
        assert provider_statements[0][3] == refinement._render_view(
            relation,
            relation_by_name,
            backend,
            idempotent=True,
        )


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
    component_seal = relation_by_name["analysis_state_component_seal"]
    assert component_seal.kind == "table"
    assert component_seal.vertical_view is None
    assert component_seal.derived_view is None
    assert provider_slices["relation:analysis_state_component_seal"][0][3].startswith(
        "CREATE TABLE"
    )


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
                f"(SELECT receipt_{attribute}.{quote}next_processed_count{quote}"
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
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_gallery_observation_metadata_locals "
                "(gallery_id, observation_id, download_time) VALUES (?, ?, ?)",
                (1, 1, 1_767_225_600_000_000),
            )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_metadata_locals "
            "VALUES (?, ?, ?, ?)",
            (1, 1, 1_767_225_600_000_000, 1_767_225_600_000_000),
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
    assert len(report.checked_relations) == 185
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


def test_metadata_projection_requires_one_complete_local_row() -> None:
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
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_gallery_observation_metadata_locals "
                "(gallery_id, observation_id, download_time) VALUES (?, ?, ?)",
                (1, 1, 13),
            )
        assert (
            connection.execute(
                "SELECT * FROM catalog_gallery_observation_metadata"
            ).fetchall()
            == []
        )
        connection.execute(
            "INSERT INTO catalog_gallery_observation_metadata_locals "
            "VALUES (?, ?, ?, ?)",
            (1, 1, 13, 17),
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


@pytest.mark.parametrize(
    ("relation_name", "table", "values"),
    (
        (
            "gallery_observation_scan",
            "catalog_gallery_observation_scans",
            (1, 2, b"s" * 32, 1, 0),
        ),
        (
            "gallery_observation_directory",
            "catalog_gallery_observation_directories",
            (1, 2, 0, b"d" * 32),
        ),
        (
            "gallery_observation_stat",
            "catalog_gallery_observation_stat",
            (1, 2, 0, 0),
        ),
    ),
)
def test_recomposed_gallery_observation_facts_are_atomic_tables(
    relation_name: str,
    table: str,
    values: tuple[object, ...],
) -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    relation = physical_spec.relation(relation_name)
    assert relation is not None
    assert relation.kind == "table"
    assert relation.vertical_view is None
    assert relation.derived_view is None

    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.execute("PRAGMA foreign_keys = OFF")
        placeholders = ", ".join("?" for _ in values)
        connection.execute(f"INSERT INTO {table} VALUES ({placeholders})", values)
        assert connection.execute(f"SELECT * FROM {table}").fetchone() == values
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(f"INSERT INTO {table} VALUES ({placeholders})", values)
    finally:
        connection.close()


def test_batch2_page_families_are_total_and_inline_projections_are_not_objects() -> (
    None
):
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
            connection.execute(
                "SELECT name FROM sqlite_master WHERE name IN "
                "('catalog_canonical_value_pages', "
                "'catalog_canonical_value_page_descriptors')"
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
            "SELECT seal.page_sha256, coordinate.value_sha256, payload.page_bytes, "
            "coordinate.level, coordinate.page_position, counts.subtree_item_count "
            "FROM catalog_canonical_value_page_seals AS seal "
            "JOIN catalog_canonical_value_page_payloads AS payload "
            "ON payload.page_sha256 = seal.page_sha256 "
            "JOIN catalog_canonical_value_page_coordinates AS coordinate "
            "ON coordinate.page_sha256 = seal.page_sha256 "
            "JOIN catalog_canonical_value_page_subtree_item_counts AS counts "
            "ON counts.page_sha256 = seal.page_sha256"
        ).fetchone() == (
            canonical_page_sha256,
            value_sha256,
            b"canonical-page",
            0,
            0,
            3,
        )

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
            "SELECT seal.page_sha256, component.component, level.level, "
            "counts.subtree_item_count "
            "FROM catalog_gallery_observation_page_descriptor_seals AS seal "
            "JOIN catalog_gallery_observation_page_descriptor_components AS component "
            "ON component.page_sha256 = seal.page_sha256 "
            "JOIN catalog_gallery_observation_page_descriptor_levels AS level "
            "ON level.page_sha256 = seal.page_sha256 "
            "JOIN catalog_gallery_observation_page_descriptor_subtree_item_counts "
            "AS counts ON counts.page_sha256 = seal.page_sha256"
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
    broken = replace(
        physical_spec,
        inline_projections=tuple(
            name
            for name in physical_spec.inline_projections
            if name != "canonical_value_page"
        ),
    )

    with pytest.raises(ValueError, match="canonical page read shapes"):
        refinement._validate_physical_schema(broken, logical)


def test_recomposed_artifact_producer_table_is_atomic_and_uniquely_keyed() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    digest = b"d" * 32
    producer_fields = (b"writer", b"python", b"pillow", b"jpeg", b"zlib")
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.execute("PRAGMA foreign_keys = OFF")
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_artifact_producer_fingerprints "
                "(producer_fingerprint_sha256, artifact_algorithm_version) "
                "VALUES (?, ?)",
                (digest, 1),
            )
        connection.execute(
            "INSERT INTO catalog_artifact_producer_fingerprints "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (digest, 1, b"equivalent", *producer_fields),
        )
        assert connection.execute(
            "SELECT * FROM catalog_artifact_producer_fingerprints"
        ).fetchone() == (digest, 1, b"equivalent", *producer_fields)
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_artifact_producer_fingerprints "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (b"e" * 32, 1, b"equivalent", *producer_fields),
            )
    finally:
        connection.close()


def test_generation_projections_derive_one_commit_mapping_without_extra_objects() -> (
    None
):
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    build_id = b"b" * 16
    candidate_id = b"c" * 16
    channel = b"default"
    receipt_id = b"r" * 16
    preparation_id = b"p" * 16
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute(
            "INSERT INTO catalog_publication_commits VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                receipt_id,
                candidate_id,
                9,
                7,
                4,
                preparation_id,
                1,
                1,
                1,
                1,
                2,
                3,
                4,
                12,
            ),
        )
        connection.execute(
            "INSERT INTO catalog_source_build_base_publication_commits VALUES (?, ?)",
            (build_id, receipt_id),
        )
        connection.execute(
            "INSERT INTO catalog_publication_candidate_base_publication_commits "
            "VALUES (?, ?)",
            (candidate_id, receipt_id),
        )
        connection.execute(
            "INSERT INTO catalog_source_revision_descriptors VALUES (?, ?, ?)",
            (7, channel, b"s" * 32),
        )
        connection.execute(
            "INSERT INTO catalog_publication_commit_head_receipts VALUES (?, ?)",
            (channel, receipt_id),
        )

        assert connection.execute(
            "SELECT base.build_id, committed.source_revision, committed.generation "
            "FROM catalog_source_build_base_publication_commits AS base "
            "JOIN catalog_publication_commits AS committed "
            "ON committed.receipt_id = base.base_receipt_id"
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
            "SELECT head.channel, committed.revision, committed.generation, "
            "committed.committed_at "
            "FROM catalog_publication_commit_heads AS head "
            "JOIN catalog_publication_commits AS committed "
            "ON committed.receipt_id = head.receipt_id"
        ).fetchone() == (channel, 9, 4, 12)
        assert (
            connection.execute(
                "SELECT name FROM sqlite_master WHERE name IN "
                "('catalog_source_build_base_source', 'catalog_publication_heads')"
            ).fetchall()
            == []
        )
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
            "gallery_observation_file_filesystem",
            "catalog_gallery_observation_file_filesystem",
            "catalog_gallery_observation_file_filesystem_devices",
            "catalog_gallery_observation_file_filesystem_inodes",
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

        assert connection.execute(
            """
            SELECT file_sha256, occurrence_count, artist_count,
                   maximum_gallery_artist_count
            FROM catalog_analysis_file_hash_decision_resolved
            WHERE analysis_id = ?
            ORDER BY file_sha256
            """,
            (leaf,),
        ).fetchall() == [(first_hash, 4, 2, 2)]
        assert connection.execute(
            """
            SELECT file_sha256, occurrence_count, artist_count,
                   maximum_gallery_artist_count
            FROM catalog_analysis_file_hash_decision_resolved
            WHERE analysis_id = ?
            ORDER BY file_sha256
            """,
            (root,),
        ).fetchall() == [
            (first_hash, 2, 1, 1),
            (removed_hash, 3, 1, 1),
        ]

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
            "INSERT INTO catalog_analysis_content_owner_shadows VALUES (?, ?, ?)",
            (analysis_id, b"c" * 32, 1),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_analysis_content_owner_shadows VALUES (?, ?, ?)",
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
        connection.execute(
            "INSERT INTO catalog_analysis_batch_receipt_stored "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                analysis_id,
                stage,
                1,
                b"batch-1",
                b"",
                0,
                128,
                b"next",
                2,
                1_767_225_602_000_000,
            ),
        )
        assert connection.execute(
            "SELECT row_count FROM catalog_analysis_batch_receipts"
        ).fetchone() == (2,)

        # state_component is an exact binary domain in the atomic wide seal.
        connection.execute(
            "INSERT INTO catalog_analysis_state_component_seals VALUES (?, ?, ?, ?)",
            (analysis_id, b"file_hash_decision", 0, 1),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_analysis_state_component_seals VALUES (?, ?, ?, ?)",
                (analysis_id, "content_owner", 0, 1),
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
                "INSERT INTO catalog_content_blobs VALUES (?, ?)", ("a" * 32, 1)
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_content_blobs VALUES (?, ?)", (b"a" * 32, 1.5)
            )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_source_revision_descriptors VALUES (?, ?, ?)",
                (0, b"default", b"s" * 32),
            )
        connection.execute(
            "INSERT INTO catalog_source_revision_descriptors VALUES (?, ?, ?)",
            (1, b"default", b"s" * 32),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_revision_descriptors VALUES (?, ?, ?)",
                (0, 0, 0),
            )
        connection.execute(
            "INSERT INTO catalog_revision_descriptors VALUES (?, ?, ?)", (1, 0, 0)
        )

        connection.execute(
            "INSERT INTO catalog_publication_generation_nodes VALUES (?)", (0,)
        )
        connection.execute(
            "INSERT INTO catalog_publication_generation_nodes VALUES (?)", (1,)
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
                "INSERT INTO catalog_source_build_base_publication_commits VALUES (?, ?)",
                (b"b" * 16, b"r" * 15),
            )
        connection.execute(
            "INSERT INTO catalog_source_build_base_publication_commits VALUES (?, ?)",
            (b"b" * 16, b"r" * 16),
        )

        connection.execute(
            "INSERT INTO catalog_title_sort_policy VALUES (?, ?, ?)",
            (1, 1, b"16.0.0"),
        )
        for policy_id, malformed_unicode_version in enumerate(
            (b"", b"v" * 33, "16.0.0"), start=2
        ):
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    "INSERT INTO catalog_title_sort_policy VALUES (?, ?, ?)",
                    (policy_id, policy_id, malformed_unicode_version),
                )

        builds = (
            (b"c" * 16, b"d" * 32, "OPEN", None),
            (b"e" * 16, b"f" * 32, "SEALED", None),
            (b"g" * 16, b"h" * 32, "ABANDONED", 2),
            (b"i" * 16, b"j" * 32, "ABANDONED", None),
        )
        for build_id, scope_key, state, sealed_at in builds:
            connection.execute(
                "INSERT INTO catalog_source_build_descriptor VALUES (?, ?, ?, ?)",
                (build_id, scope_key, 1, 1),
            )
            connection.execute(
                "INSERT INTO catalog_source_build_states VALUES (?, ?)",
                (build_id, state),
            )
            if sealed_at is not None:
                connection.execute(
                    "INSERT INTO catalog_source_build_sealed_ats VALUES (?, ?)",
                    (build_id, sealed_at),
                )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_source_build_states VALUES (?, ?)",
                (b"z" * 16, "BROKEN"),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_source_build_sealed_ats VALUES (?, ?)",
                (b"z" * 16, None),
            )
        assert connection.execute(
            "SELECT build_id, state, sealed_at FROM catalog_source_builds "
            "ORDER BY build_id"
        ).fetchall() == [
            (b"c" * 16, "OPEN", None),
            (b"i" * 16, "ABANDONED", None),
        ]

        analyses = (
            (b"k" * 16, b"K" * 16, 1, "COMPLETE", 2),
            (b"l" * 16, b"L" * 16, 2, "COMPLETE", None),
            (b"m" * 16, b"M" * 16, 3, "OPEN", 2),
            (b"n" * 16, b"N" * 16, 4, "ABANDONED", None),
        )
        for analysis_id, build_id, policy_id, state, completed_at in analyses:
            connection.execute(
                "INSERT INTO catalog_analysis_run_descriptor VALUES (?, ?, ?, ?, ?)",
                (analysis_id, build_id, policy_id, bytes([policy_id]) * 32, 1),
            )
            connection.execute(
                "INSERT INTO catalog_analysis_run_states VALUES (?, ?)",
                (analysis_id, state),
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
                    "INSERT INTO catalog_analysis_batch_receipt_stored "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        b"x" * 16,
                        b"changed_gallery",
                        invalid_page_limit + 1,
                        bytes([invalid_page_limit % 256 or 1]),
                        b"",
                        0,
                        invalid_page_limit,
                        b"next",
                        1,
                        1,
                    ),
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
                "INSERT INTO catalog_analysis_checkpoints VALUES (?, ?, ?, ?, ?, ?, ?)",
                (b"a" * 16, b"changed_gallery", 1, b"c" * 2049, 0, "OPEN", 1),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_publication_checkpoints VALUES (?, ?, ?, ?, ?, ?, ?)",
                (b"p" * 16, b"ITEMS", 1, b"c" * 2049, 0, "OPEN", 1),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_prepared_artifacts VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    b"q" * 16,
                    b"k" * 32,
                    b"a" * 32,
                    1,
                    1,
                    b"t" * 185,
                    "PENDING",
                ),
            )
    finally:
        connection.close()


def test_sqlite_recomposed_manifest_policy_enforces_both_candidate_keys() -> None:
    logical = refinement.load_logical_schema(CATALOG)
    physical_spec = refinement.load_physical_schema(PHYSICAL, logical)
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(refinement.render_sqlite_ddl(physical_spec))
        connection.execute(
            "INSERT INTO catalog_manifest_policies VALUES (?, ?, ?)", (1, 1, 1)
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_manifest_policies VALUES (?, ?, ?)", (3, 1, 1)
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO catalog_manifest_policies "
                "(manifest_policy_id, manifest_algorithm_version) VALUES (?, ?)",
                (2, 2),
            )
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
        if statement.startswith("CREATE TABLE `catalog_publication_commits`")
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
        == 33
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
    assert "`artifact_storage_key_sha256` BINARY(32) NOT NULL" not in ddl
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
    publication_receipt_ddl = next(
        statement
        for statement in statements
        if statement.startswith(
            "CREATE SQL SECURITY INVOKER VIEW `catalog_publication_receipts`"
        )
    )
    assert "AS CHAR(16) CHARSET ascii) COLLATE ascii_bin" in (publication_receipt_ddl)
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
    publication_receipt = physical_spec.relation("publication_receipt")
    assert publication_receipt is not None
    publication_state_column = next(
        column for column in publication_receipt.columns if column.attribute == "state"
    )
    assert publication_state_column.mariadb.type_name == "VARCHAR(16)"
    assert publication_state_column.mariadb.collation == "ascii_bin"
    assert publication_state_column.mariadb.nullable
    impacted_gid = physical_spec.relation("analysis_impacted_gid")
    assert impacted_gid is not None
    witness_gallery_id = next(
        column
        for column in impacted_gid.columns
        if column.attribute == "witness_gallery_id"
    )
    assert witness_gallery_id.mariadb.nullable
    assert not witness_gallery_id.sqlite.nullable
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
        connector.execute(
            "INSERT INTO catalog_gallery_observation_metadata_locals "
            "VALUES (%s, %s, %s, %s)",
            (1, 1, 1_767_225_600_000_000, 1_767_225_600_000_000),
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
    assert database.table("catalog_gallery_observation_files") is None
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
