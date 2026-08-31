from __future__ import annotations

import hashlib
import importlib.util
import sqlite3
import subprocess
import sys
import tomllib
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from h2hdb import CoreConfig
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.vnext_identity import (
    GALLERY_OBSERVATION_DURABLE_PARSER_PHASES,
    GalleryObservationMetadata,
    iter_gallery_observation_metadata_stream,
    validate_gallery_observation_metadata_parts,
)

ROOT = Path(__file__).resolve().parents[1]
CHECKER_PATH = ROOT / "verification" / "schema" / "check_contract.py"
CATALOG_PATH = ROOT / "verification" / "schema" / "catalog.toml"
LOGICAL_PATH = ROOT / "verification" / "schema" / "operational.toml"
PHYSICAL_PATH = ROOT / "verification" / "schema" / "operational_physical.toml"


def _empty_cleanup_frozen_root_digest(cleanup_id: bytes) -> bytes:
    return hashlib.sha256(
        b"h2hdb-cleanup-frozen-root-set-v1\0" + cleanup_id + bytes(2)
    ).digest()


GENERATOR_PATH = ROOT / "verification" / "schema" / "generate_operational_physical.py"
OPERATIONAL_REFINEMENT_PATH = (
    ROOT / "verification" / "schema" / "operational_refinement.py"
)


def _load_checker() -> ModuleType:
    specification = importlib.util.spec_from_file_location(
        "h2hdb_operational_schema_checker", CHECKER_PATH
    )
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


checker = _load_checker()


def _load_operational_refinement() -> ModuleType:
    specification = importlib.util.spec_from_file_location(
        "h2hdb_operational_schema_refinement", OPERATIONAL_REFINEMENT_PATH
    )
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


operational_refinement = _load_operational_refinement()
refinement = operational_refinement.refinement


def test_cleanup_frozen_root_bound_is_derived_from_every_registered_shape() -> None:
    frame_bytes = operational_refinement._cleanup_frozen_root_frame_bytes_by_target()
    assert set(frame_bytes) == set(operational_refinement._CLEANUP_TARGET_SHAPES)
    assert len(frame_bytes) == 23
    assert all(size <= 260 for size in frame_bytes.values())
    assert max(frame_bytes.values()) == 260
    assert frame_bytes["SOURCE_GALLERY_NAME_GID"] == 260


class _SQLiteReader:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def fetch_all(
        self, query: str, data: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        return self.connection.execute(query, data).fetchall()


def _schemas() -> tuple[
    Any,
    tuple[str, ...],
    Any,
    tuple[Any, ...],
]:
    logical, local_names = operational_refinement.load_combined_logical_schema(
        LOGICAL_PATH
    )
    physical = operational_refinement.load_operational_physical_schema(
        PHYSICAL_PATH,
        logical,
        local_names,
    )
    stubs = operational_refinement.load_external_stubs(PHYSICAL_PATH)
    return logical, local_names, physical, stubs


def _gallery_batch_facts(**changes: Any) -> Any:
    facts = operational_refinement.GalleryStagingBatchFacts(
        component="FILE",
        claim_generation=7,
        presented_claim_generation=7,
        owner_matches=True,
        live_outer_lease=True,
        current_outer_head=True,
        checkpoint_state="OPEN",
        cursor=0,
        presented_start_cursor=0,
        prior_request_key=None,
        presented_prior_request_key=None,
        last_page_present=False,
        next_cursor=256,
        entry_count=256,
        subtree_item_count=256,
        terminal=False,
        commits_complete=False,
        exact_page_recomputed=True,
        normalized_facts_congruent=True,
        boundary_ordered=True,
        metadata_offset_contiguous=True,
        request_key_valid=True,
        latest_receipt_exists=False,
        latest_request_key_matches=False,
        latest_request_frame_matches=False,
    )
    return replace(facts, **changes)


def _gallery_match_facts(**changes: Any) -> Any:
    facts = operational_refinement.GalleryStagingMatchFacts(
        claim_generation=7,
        presented_claim_generation=7,
        owner_matches=True,
        live_outer_lease=True,
        current_outer_head=True,
        checkpoint_state="OPEN",
        cursor=b"file-leaf-0:0",
        presented_cursor=b"file-leaf-0:0",
        prior_request_key=None,
        presented_prior_request_key=None,
        next_cursor=b"file-leaf-1:0",
        matched_count=0,
        next_matched_count=256,
        file_item_count=300,
        directory_regular_count=300,
        step_count=256,
        terminal=False,
        commits_complete=False,
        file_stream_complete=True,
        directory_stream_complete=True,
        cursor_advance_valid=True,
        file_records_exact=True,
        directory_lookups_exact=True,
        request_key_valid=True,
        latest_receipt_exists=False,
        latest_request_key_matches=False,
        latest_request_frame_matches=False,
    )
    return replace(facts, **changes)


def _metadata_stream(
    *,
    title: str = "標題",
    comment: str = "comment",
    upload_account: str = "account",
    page_count: int | None = 12,
) -> bytes:
    prefix = b"h2hdb-vnext-gallery-observation-metadata\0"
    fields = []
    for tag, value in (
        (1, title.encode("utf-8")),
        (2, comment.encode("utf-8")),
        (3, upload_account.encode("utf-8")),
    ):
        fields.append(bytes((tag,)) + len(value).to_bytes(8, "big") + value)
    presence = 0 if page_count is None else 1
    return b"".join(
        (
            prefix,
            (1).to_bytes(4, "big"),
            (123).to_bytes(8, "big"),
            *fields,
            (1000).to_bytes(8, "big"),
            (2000).to_bytes(8, "big"),
            (3000).to_bytes(8, "big"),
            (4).to_bytes(4, "big"),
            (99).to_bytes(8, "big"),
            bytes((presence,)),
            b"" if page_count is None else page_count.to_bytes(4, "big"),
        )
    )


def test_operational_contract_is_closed_world_bcnf_and_scope_separated() -> None:
    contract = checker.load_contract(LOGICAL_PATH)
    report = checker.validate_contract(contract)

    assert contract.scope == "operational_control_plane"
    assert contract.excluded_data_plane_components
    assert not contract.excluded_operational_components
    assert len(contract.relations) == 67
    assert len(report.relations) == 67
    assert not report.lossless_decompositions
    assert not report.dependency_preserving_decompositions
    assert all(not checker.bcnf_violations(value) for value in contract.relations)
    assert len(contract.external_relations) == 45
    assert {
        "canonical_value_allocation",
        "canonical_value_page",
        "canonical_value_page_descriptor",
        "canonical_value_page_parent",
        "canonical_value_identity",
        "gallery_observation_allocation_page",
        "gallery_observation_page_descriptor",
        "gallery_observation_file_filesystem",
        "gallery_identity",
        "file_name_identity",
        "gallery_observation_file_anchor",
        "gallery_observation_file_file_no",
        "gallery_observation_file_file_sha256",
        "gallery_observation_file_seal",
        "tag_term",
        "gallery_observation_metadata",
        "gallery_observation_scan",
        "source_build_gallery",
    } <= {value.name for value in contract.external_relations}
    assert {
        value.name
        for value in contract.relations
        if value.kind == "controlled_materialization"
    } == {"file_hash_cache", "operational_activation"}


def test_maintenance_gate_holder_allows_one_owner_to_hold_every_slot() -> None:
    contract = checker.load_contract(LOGICAL_PATH)
    holder = next(
        value for value in contract.relations if value.name == "maintenance_gate_holder"
    )

    assert holder.attributes == ("owner_token", "slot")
    assert holder.declared_keys == (frozenset({"slot"}),)
    assert holder.functional_dependencies == (
        checker.FunctionalDependency(frozenset({"slot"}), frozenset({"owner_token"})),
    )
    assert checker.enumerate_candidate_keys(
        holder.attributes, holder.functional_dependencies
    ) == (frozenset({"slot"}),)

    with LOGICAL_PATH.open("rb") as stream:
        gate_contract = tomllib.load(stream)["maintenance_gate_contract"]
    assert gate_contract == {
        "slot_count": 64,
        "head_relation": "maintenance_gate_head",
        "generation_relation": "maintenance_gate_generation",
        "owner_relation": "maintenance_gate_owner",
        "holder_relation": "maintenance_gate_holder",
        "authorization_rule": (
            "exact_holder_owner_and_owner_generation_equals_head_generation_"
            "and_unexpired_owner_lease"
        ),
        "shared_claim_rule": "current_shared_owner_holds_exactly_one_slot",
        "exclusive_claim_rule": (
            "current_exclusive_owner_holds_every_slot_zero_through_sixty_three"
        ),
        "reclaim_rule": (
            "replace_a_slot_only_by_transactional_cas_on_the_exact_observed_owner_"
            "after_its_generation_is_stale_or_its_lease_is_expired"
        ),
        "stale_rule": "stale_generation_or_expired_owner_authorizes_no_mutation",
        "canonical_value_rule": (
            "every canonical-value allocation, upload-claim, bounded page, and "
            "final-seal transaction holds and rechecks one shared maintenance "
            "slot; CANONICAL_VALUE cleanup holds the exclusive generation for "
            "its complete multi-phase cycle and rechecks current-head, live "
            "source-working analysis, live or uncommitted publication-candidate, "
            "and upload semantic pins before every bounded destructive batch, so "
            "no live producer can claim, write, reseal, or lose its canonical "
            "snapshot between phases"
        ),
        "history_cleanup_rule": (
            "under a newer live exclusive gate generation, keyset-delete at most "
            "the fixed batch bound of expired non-head owners after their holder "
            "slots are absent, then delete an unreferenced non-head maintenance "
            "generation only after no owner or head references it; every batch "
            "row-locks and rechecks head, owner lease, and exact generation, so "
            "current or live shared and exclusive authority always blocks"
        ),
    }

    logical, _local_names, physical, stubs = _schemas()
    del logical
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(
            operational_refinement.render_sqlite_ddl(physical, stubs)
        )
        owner = b"exclusive-owner!"
        other_owner = b"other-owner-0001"
        assert len(owner) == len(other_owner) == 16
        connection.execute(
            """
            INSERT INTO operational_maintenance_gate_generations
                (gate_generation, mode, created_at)
            VALUES (?, ?, ?)
            """,
            (1, "EXCLUSIVE", 1),
        )
        connection.executemany(
            """
            INSERT INTO operational_maintenance_gate_owners
                (owner_token, gate_generation, lease_expires_at)
            VALUES (?, ?, ?)
            """,
            ((owner, 1, 100), (other_owner, 1, 100)),
        )
        connection.executemany(
            """
            INSERT INTO operational_maintenance_gate_holders (owner_token, slot)
            VALUES (?, ?)
            """,
            ((owner, slot) for slot in range(64)),
        )
        assert connection.execute(
            "SELECT COUNT(*) FROM operational_maintenance_gate_holders "
            "WHERE owner_token = ?",
            (owner,),
        ).fetchone() == (64,)
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO operational_maintenance_gate_holders (owner_token, slot)
                VALUES (?, ?)
                """,
                (other_owner, 0),
            )
    finally:
        connection.close()


@pytest.mark.parametrize(
    "field",
    [
        "queue_history_contract",
        "canonical_hash_cache_contract",
        "operational_event_integrity_contract",
        "source_build_generation_contract",
        "cleanup_attempt_contract",
        "preparation_identity_contract",
    ],
)
def test_operational_integrity_contracts_are_closed_world(field: str) -> None:
    contract = checker.load_contract(LOGICAL_PATH)

    with pytest.raises(
        checker.ContractValidationError,
        match=f"operational contract must declare {field}",
    ):
        checker.validate_contract(replace(contract, **{field: None}))


def test_operational_greenfield_identity_and_history_shapes() -> None:
    contract = checker.load_contract(LOGICAL_PATH)
    relations = {value.name: value for value in contract.relations}

    assert "deletion_request" not in relations
    assert "hash_source_identity" not in relations
    assert "hash_fingerprint_identity" not in relations
    assert relations["source_build_generation"].declared_keys == (
        frozenset({"generation"}),
    )
    assert any(
        foreign_key.attributes == ("generation",)
        and foreign_key.relation == "ingest_generation"
        for foreign_key in relations["source_build_generation"].foreign_keys
    )
    assert all(
        foreign_key.relation != "ingest_generation_owner"
        for relation_name in ("source_build_generation", "canonical_value_upload")
        for foreign_key in relations[relation_name].foreign_keys
    )
    stream = relations["operational_event_stream"]
    assert stream.attributes == ("preparation_id", "created_at")
    assert stream.declared_keys == (frozenset({"preparation_id"}),)
    effect_seal = relations["operational_preparation_effect_seal"]
    assert effect_seal.attributes == (
        "preparation_id",
        "event_count",
        "final_chain_sha256",
        "sealed_at",
    )
    assert effect_seal.declared_keys == (frozenset({"preparation_id"}),)
    event = relations["operational_event"]
    assert "source_revision" not in event.attributes
    assert set(event.declared_keys) == {
        frozenset({"event_id"}),
        frozenset({"preparation_id", "sequence_no"}),
    }
    assert {
        "operational_consumer",
        "operational_event_ack",
        "operational_event_ack_head",
        "removed_gid_ack",
    }.isdisjoint(relations)
    deletion_generation = relations["deletion_request_generation"]
    assert deletion_generation.attributes == ("generation", "allocated_at")
    assert deletion_generation.declared_keys == (frozenset({"generation"}),)
    assert deletion_generation.functional_dependencies == (
        checker.FunctionalDependency(
            frozenset({"generation"}), frozenset({"allocated_at"})
        ),
    )
    deletion_generation_head = relations["deletion_request_generation_head"]
    assert deletion_generation_head.attributes == (
        "singleton_id",
        "current_generation",
        "updated_at",
    )
    assert deletion_generation_head.declared_keys == (frozenset({"singleton_id"}),)
    assert any(
        foreign_key.attributes == ("current_generation",)
        and foreign_key.relation == "deletion_request_generation"
        and foreign_key.referenced_attributes == ("generation",)
        for foreign_key in deletion_generation_head.foreign_keys
    )
    assert set(relations["operational_preparation"].declared_keys) == {
        frozenset({"preparation_id"}),
        frozenset(
            {
                "build_id",
                "deletion_request_generation",
                "operational_policy_id",
            }
        ),
    }
    assert any(
        foreign_key.attributes == ("deletion_request_generation",)
        and foreign_key.relation == "deletion_request_generation"
        and foreign_key.referenced_attributes == ("generation",)
        for foreign_key in relations["operational_preparation"].foreign_keys
    )
    assert set(relations["cleanup_job"].declared_keys) == {
        frozenset({"cleanup_id"}),
        frozenset({"target_key"}),
    }
    hash_observation = relations["hash_cache_observation"]
    canonical_fks = {
        (foreign_key.attributes, foreign_key.relation)
        for foreign_key in hash_observation.foreign_keys
    }
    assert canonical_fks == {
        (("source_identity_sha256",), "canonical_value_identity"),
        (("fingerprint_sha256",), "canonical_value_identity"),
    }


def test_operational_sqlite_history_and_attempts_accept_required_reuse() -> None:
    _logical, _local_names, physical, stubs = _schemas()
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(
            operational_refinement.render_sqlite_ddl(physical, stubs)
        )
        first_build = bytes([1]) * 16
        second_build = bytes([2]) * 16
        first_owner = bytes([3]) * 16
        second_owner = bytes([4]) * 16
        first_request = bytes([5]) * 16
        second_request = bytes([6]) * 16
        first_cleanup = bytes([7]) * 16
        second_cleanup = bytes([8]) * 16
        first_preparation = bytes([9]) * 16
        second_preparation = bytes([10]) * 16
        values = (
            first_build,
            second_build,
            first_owner,
            second_owner,
            first_request,
            second_request,
            first_cleanup,
            second_cleanup,
            first_preparation,
            second_preparation,
        )
        assert all(len(value) == 16 for value in values)

        connection.executemany(
            "INSERT INTO catalog_source_build_descriptor "
            "(build_id, scope_key, manifest_policy_id, created_at) "
            "VALUES (?, ?, 1, 1)",
            ((first_build, b"s" * 32), (second_build, b"t" * 32)),
        )
        connection.execute(
            "INSERT INTO operational_deletion_request_generations "
            "(generation, allocated_at) VALUES (0, 0)"
        )
        connection.execute(
            "INSERT INTO operational_deletion_request_generation_heads "
            "(singleton_id, current_generation, updated_at) VALUES (1, 0, 0)"
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE operational_deletion_request_generation_heads "
                "SET current_generation = 1, updated_at = 1 "
                "WHERE singleton_id = 1 AND current_generation = 0"
            )
        connection.execute(
            "INSERT INTO operational_deletion_request_generations "
            "(generation, allocated_at) VALUES (1, 1)"
        )
        advanced = connection.execute(
            "UPDATE operational_deletion_request_generation_heads "
            "SET current_generation = 1, updated_at = 1 "
            "WHERE singleton_id = 1 AND current_generation = 0"
        )
        assert advanced.rowcount == 1
        stale = connection.execute(
            "UPDATE operational_deletion_request_generation_heads "
            "SET current_generation = 1, updated_at = 2 "
            "WHERE singleton_id = 1 AND current_generation = 0"
        )
        assert stale.rowcount == 0
        connection.executemany(
            """
            INSERT INTO operational_ingest_generations
                (generation, started_at, completed_at)
            VALUES (?, ?, NULL)
            """,
            ((1, 1), (2, 2)),
        )
        connection.executemany(
            """
            INSERT INTO operational_ingest_generation_owners
                (generation, owner_token, claimed_at, lease_expires_at)
            VALUES (?, ?, ?, ?)
            """,
            ((1, first_owner, 1, 10), (2, second_owner, 2, 20)),
        )
        connection.execute(
            """
            INSERT INTO operational_source_build_generations (build_id, generation)
            VALUES (?, ?)
            """,
            (first_build, 1),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO operational_source_build_generations
                    (build_id, generation)
                VALUES (?, ?)
                """,
                (second_build, 1),
            )
        connection.execute(
            """
            INSERT INTO operational_source_build_generations (build_id, generation)
            VALUES (?, ?)
            """,
            (first_build, 2),
        )
        connection.execute(
            "UPDATE operational_ingest_generations "
            "SET completed_at = 3 WHERE generation = 1"
        )
        connection.execute(
            "DELETE FROM operational_ingest_generation_owners WHERE generation = 1"
        )
        assert connection.execute(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = 1"
        ).fetchone() == (first_build,)

        connection.executemany(
            """
            INSERT INTO operational_deletion_request_attempts
                (request_token, gid, requested_at)
            VALUES (?, ?, ?)
            """,
            ((first_request, 42, 1), (second_request, 42, 2)),
        )
        connection.execute(
            """
            INSERT INTO operational_deletion_request_heads (gid, request_token)
            VALUES (?, ?)
            """,
            (42, first_request),
        )
        connection.execute(
            """
            UPDATE operational_deletion_request_heads
            SET request_token = ? WHERE gid = ?
            """,
            (second_request, 42),
        )
        assert connection.execute(
            "SELECT COUNT(*) FROM operational_deletion_request_attempts"
        ).fetchone() == (2,)
        assert connection.execute("""
            SELECT request_token FROM operational_deletion_request_heads
            WHERE gid = 42
            """).fetchone() == (second_request,)
        connection.execute(
            """
            INSERT INTO operational_deletion_request_urls (request_token, url)
            VALUES (?, '')
            """,
            (first_request,),
        )

        connection.execute(
            "INSERT INTO operational_cleanup_target_kinds (target_kind) "
            "VALUES ('SOURCE_BUILD')"
        )
        target_key = operational_refinement.encode_cleanup_target_key(
            "SOURCE_BUILD", (0,)
        )
        connection.execute(
            "INSERT INTO operational_cleanup_sweep_targets "
            "(target_kind,shard_no,target_key) VALUES ('SOURCE_BUILD',0,?)",
            (target_key,),
        )
        connection.execute(
            """
            INSERT INTO operational_cleanup_jobs
                (cleanup_id, target_key, cycle_generation,
                 cycle_cutoff_at,algorithm_version,max_rows_per_transaction,
                 hash_cache_max_age_microseconds,frozen_root_count,
                 frozen_root_set_sha256,state,created_at,completed_at)
            VALUES (?, ?, 1, 1000, 2, 100, 100, 0, ?, 'OPEN', 1, NULL)
            """,
            (
                first_cleanup,
                target_key,
                _empty_cleanup_frozen_root_digest(first_cleanup),
            ),
        )
        connection.execute(
            """UPDATE operational_cleanup_jobs
               SET cleanup_id=?, cycle_generation=2, created_at=2,
                   frozen_root_set_sha256=?
               WHERE target_key=? AND cleanup_id=? AND cycle_generation=1""",
            (
                second_cleanup,
                _empty_cleanup_frozen_root_digest(second_cleanup),
                target_key,
                first_cleanup,
            ),
        )
        connection.execute(
            "INSERT INTO operational_cleanup_phases (phase,target_kind,phase_order) "
            "VALUES ('SB_GENERATION','SOURCE_BUILD',1)"
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """INSERT INTO operational_cleanup_checkpoints
                   (cleanup_id,phase,generation,cursor_bytes,deleted_count,
                    chain_sha256,state,updated_at)
                   VALUES (?,'SB_GENERATION',0,X'',0,?,'OPEN',3)""",
                (first_cleanup, bytes(32)),
            )

        connection.executemany(
            """
            INSERT INTO operational_operational_policys
                (operational_policy_id, operational_schema_version,
                 algorithm_version, max_batch_rows)
            VALUES (?, 1, ?, 100)
            """,
            ((1, 1), (2, 2)),
        )
        connection.executemany(
            """
            INSERT INTO operational_operational_event_streams
                (preparation_id, created_at)
            VALUES (?, ?)
            """,
            ((first_preparation, 1), (second_preparation, 2)),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO operational_operational_preparations
                    (preparation_id, build_id, deletion_request_generation,
                     operational_policy_id, state, prepared_at, completed_at)
                VALUES (?, ?, 2, 1, 'OPEN', 1, NULL)
                """,
                (first_preparation, first_build),
            )
        connection.executemany(
            """
            INSERT INTO operational_operational_preparations
                (preparation_id, build_id, deletion_request_generation,
                 operational_policy_id, state, prepared_at, completed_at)
            VALUES (?, ?, 1, ?, 'OPEN', ?, NULL)
            """,
            (
                (first_preparation, first_build, 1, 1),
                (second_preparation, first_build, 2, 2),
            ),
        )
    finally:
        connection.close()


def test_operational_sqlite_event_types_subtypes_and_inline_activation() -> None:
    _logical, _local_names, physical, stubs = _schemas()
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(
            operational_refinement.render_sqlite_ddl(physical, stubs)
        )
        preparation = bytes([1]) * 16
        removed_event = bytes([2]) * 16
        deletion_event = bytes([3]) * 16
        removed_request = bytes([4]) * 16
        deletion_request = bytes([5]) * 16
        connection.execute(
            "INSERT INTO catalog_source_revisions (source_revision) VALUES (1)"
        )
        connection.execute("""
            INSERT INTO operational_operational_policys
                (operational_policy_id, operational_schema_version,
                 algorithm_version, max_batch_rows)
            VALUES (1, 1, 1, 100)
            """)
        connection.execute(
            "INSERT INTO operational_operational_event_streams "
            "(preparation_id, created_at) VALUES (?, 1)",
            (preparation,),
        )
        assert (
            connection.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE name = 'operational_operational_activations'"
            ).fetchone()
            is None
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO operational_operational_events
                    (event_id, preparation_id, sequence_no, event_type,
                     event_sha256, created_at)
                VALUES (?, ?, 99, 'UNKNOWN', ?, 1)
                """,
                (bytes([99]) * 16, preparation, bytes([99]) * 32),
            )
        connection.executemany(
            """
            INSERT INTO operational_operational_events
                (event_id, preparation_id, sequence_no, event_type,
                 event_sha256, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    removed_event,
                    preparation,
                    0,
                    "REMOVED_GID",
                    bytes([10]) * 32,
                    1,
                ),
                (
                    deletion_event,
                    preparation,
                    1,
                    "DELETION_CONSUMPTION",
                    bytes([11]) * 32,
                    2,
                ),
            ),
        )
        connection.execute(
            """
            INSERT INTO operational_operational_removed_gid_events
                (event_id, gid, request_token)
            VALUES (?, 42, ?)
            """,
            (removed_event, removed_request),
        )
        connection.execute(
            """
            INSERT INTO operational_deletion_request_attempts
                (request_token, gid, requested_at)
            VALUES (?, 43, 1)
            """,
            (deletion_request,),
        )
        connection.execute(
            """
            INSERT INTO operational_operational_deletion_consumption_events
                (event_id, gid, deletion_request_token)
            VALUES (?, 43, ?)
            """,
            (deletion_event, deletion_request),
        )
        assert connection.execute("""
            SELECT COUNT(*)
            FROM operational_operational_events AS event
            JOIN catalog_publication_commits AS committed
              ON committed.preparation_id = event.preparation_id
            """).fetchone() == (0,)
        connection.execute(
            """
            INSERT INTO operational_operational_preparation_effect_seals
                (preparation_id, event_count, final_chain_sha256, sealed_at)
            VALUES (?, 2, ?, 2)
            """,
            (preparation, bytes([12]) * 32),
        )
        receipt_id = bytes([7]) * 16
        connection.execute(
            "INSERT INTO catalog_publication_commits "
            "(receipt_id, candidate_id, revision, source_revision, generation, "
            "preparation_id, operational_policy_id, artifact_policy_id, "
            "display_title_policy_id, new_galleries, changed_galleries, "
            "removed_galleries, duplicate_losers, committed_at) "
            "VALUES (?, ?, 1, 1, 1, ?, 1, 1, 1, 0, 0, 0, 0, 2)",
            (receipt_id, bytes([8]) * 16, preparation),
        )
        assert connection.execute("""
            SELECT COUNT(*)
            FROM operational_operational_events AS event
            JOIN catalog_publication_commits AS committed
              ON committed.preparation_id = event.preparation_id
            """).fetchone() == (2,)
        empty_preparation = bytes([6]) * 16
        empty_chain = bytes.fromhex(
            "e3963ad6e07ac045502ad95ddb3805ac57deea8ffbb038ddf7c538a816301e71"
        )
        connection.execute(
            "INSERT INTO operational_operational_event_streams "
            "(preparation_id, created_at) VALUES (?, 3)",
            (empty_preparation,),
        )
        connection.execute(
            """
            INSERT INTO operational_operational_preparation_effect_seals
                (preparation_id, event_count, final_chain_sha256, sealed_at)
            VALUES (?, 0, ?, 3)
            """,
            (empty_preparation, empty_chain),
        )
        assert connection.execute(
            "SELECT event_count FROM "
            "operational_operational_preparation_effect_seals "
            "WHERE preparation_id = ?",
            (empty_preparation,),
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM operational_operational_events "
            "WHERE preparation_id = ?",
            (empty_preparation,),
        ).fetchone() == (0,)
    finally:
        connection.close()


def test_generation_retention_rows_outlive_ephemeral_owner_authority() -> None:
    _logical, _local_names, physical, stubs = _schemas()
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(
            operational_refinement.render_sqlite_ddl(physical, stubs)
        )
        generation = 7
        owner = bytes([7]) * 16
        build_id = bytes([8]) * 16
        value_sha256 = bytes([9]) * 32
        connection.execute(
            "INSERT INTO catalog_source_build_descriptor "
            "(build_id, scope_key, manifest_policy_id, created_at) "
            "VALUES (?, ?, 1, 1)",
            (build_id, b"s" * 32),
        )
        connection.execute(
            "INSERT INTO catalog_canonical_value_allocation_anchors "
            "(value_sha256) VALUES (?)",
            (value_sha256,),
        )
        connection.execute(
            "INSERT INTO catalog_canonical_value_allocation_seals "
            "(value_sha256) VALUES (?)",
            (value_sha256,),
        )
        connection.execute(
            "INSERT INTO operational_ingest_generations "
            "(generation, started_at, completed_at) VALUES (?, 1, NULL)",
            (generation,),
        )
        connection.execute(
            "INSERT INTO operational_ingest_generation_owners "
            "(generation, owner_token, claimed_at, lease_expires_at) "
            "VALUES (?, ?, 1, 100)",
            (generation, owner),
        )
        connection.execute(
            "INSERT INTO operational_source_build_generations "
            "(build_id, generation) VALUES (?, ?)",
            (build_id, generation),
        )
        connection.execute(
            "INSERT INTO operational_canonical_value_uploads "
            "(generation, value_sha256) VALUES (?, ?)",
            (generation, value_sha256),
        )
        connection.execute(
            "UPDATE operational_ingest_generations SET completed_at = 2 "
            "WHERE generation = ?",
            (generation,),
        )
        connection.execute(
            "DELETE FROM operational_ingest_generation_owners WHERE generation = ?",
            (generation,),
        )
        assert connection.execute(
            "SELECT generation FROM operational_source_build_generations"
        ).fetchall() == [(generation,)]
        assert connection.execute(
            "SELECT generation FROM operational_canonical_value_uploads"
        ).fetchall() == [(generation,)]
    finally:
        connection.close()


def test_operational_external_fk_must_target_declared_candidate_key() -> None:
    contract = checker.load_contract(LOGICAL_PATH)
    external = next(
        value
        for value in contract.external_relations
        if value.name == "source_build_descriptor"
    )
    invalid_external = replace(external, declared_keys=(frozenset({"missing"}),))
    invalid = replace(
        contract,
        external_relations=tuple(
            invalid_external if value.name == external.name else value
            for value in contract.external_relations
        ),
    )

    with pytest.raises(checker.ContractValidationError, match="unknown attributes"):
        checker.validate_contract(invalid)


def test_operational_external_shapes_match_catalog_candidate_keys() -> None:
    operational = checker.load_contract(LOGICAL_PATH)
    catalog = checker.load_contract(CATALOG_PATH)
    catalog_relations = {value.name: value for value in catalog.relations}

    for external in operational.external_relations:
        target = catalog_relations[external.name]
        assert set(external.attributes) <= set(target.attributes)
        assert set(external.declared_keys) == {
            key for key in target.declared_keys if key <= set(external.attributes)
        }

    canonical = catalog_relations["canonical_value_identity"]
    assert set(canonical.attributes) == {"value_sha256", "root_page_sha256"}
    assert set(canonical.declared_keys) == {
        frozenset({"value_sha256"}),
        frozenset({"root_page_sha256"}),
    }


def test_hash_cache_canonical_domains_are_fail_closed_across_manifests() -> None:
    operational = checker.load_contract(LOGICAL_PATH)
    catalog = checker.load_contract(CATALOG_PATH)
    checker.validate_cross_manifest_contracts(catalog, operational)

    hash_cache = operational.canonical_hash_cache_contract
    assert hash_cache is not None
    drifted_operational = replace(
        operational,
        canonical_hash_cache_contract=replace(
            hash_cache,
            source_domain="filesystem_source_identity_v2",
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="hash-cache domains must be exactly",
    ):
        checker.validate_cross_manifest_contracts(catalog, drifted_operational)

    missing_catalog_seed = replace(
        catalog,
        bootstrap_seeds=tuple(
            seed
            for seed in catalog.bootstrap_seeds
            if seed.values != ("filesystem_fingerprint_v1",)
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="does not register.*filesystem_fingerprint_v1",
    ):
        checker.validate_cross_manifest_contracts(
            missing_catalog_seed,
            operational,
        )


def test_source_build_authority_is_fail_closed_across_manifests() -> None:
    operational = checker.load_contract(LOGICAL_PATH)
    catalog = checker.load_contract(CATALOG_PATH)

    expected_membership = next(
        relation
        for relation in catalog.relations
        if relation.name == "source_build_expected_gallery"
    )
    missing_gallery_identity_fk = replace(
        expected_membership,
        foreign_keys=tuple(
            foreign_key
            for foreign_key in expected_membership.foreign_keys
            if foreign_key.relation != "gallery_identity"
        ),
    )
    drifted_catalog = replace(
        catalog,
        relations=tuple(
            (
                missing_gallery_identity_fk
                if relation.name == expected_membership.name
                else relation
            )
            for relation in catalog.relations
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="must bind build and gallery identity",
    ):
        checker.validate_cross_manifest_contracts(drifted_catalog, operational)

    discovery_receipt = next(
        relation
        for relation in operational.relations
        if relation.name == "source_build_discovery_batch_receipt"
    )
    incomplete_receipt = replace(
        discovery_receipt,
        attributes=tuple(
            attribute
            for attribute in discovery_receipt.attributes
            if attribute != "next_state"
        ),
    )
    drifted_operational = replace(
        operational,
        relations=tuple(
            incomplete_receipt if relation.name == discovery_receipt.name else relation
            for relation in operational.relations
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="discovery_batch_receipt must retain complete pre/post authority",
    ):
        checker.validate_cross_manifest_contracts(catalog, drifted_operational)

    gallery_identity_target = next(
        target
        for target in catalog.retention_targets
        if target.target == "GALLERY_IDENTITY"
    )
    incomplete_target = replace(
        gallery_identity_target,
        external_blockers=tuple(
            edge
            for edge in gallery_identity_target.external_blockers
            if edge.relation != "source_build_expected_gallery"
        ),
    )
    drifted_retention = replace(
        catalog,
        retention_targets=tuple(
            (
                incomplete_target
                if target.target == gallery_identity_target.target
                else target
            )
            for target in catalog.retention_targets
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="retention must list expected source membership",
    ):
        checker.validate_cross_manifest_contracts(drifted_retention, operational)

    publication_commit_target = next(
        target
        for target in catalog.retention_targets
        if target.target == "PUBLICATION_COMMIT"
    )
    incomplete_publication_target = replace(
        publication_commit_target,
        child_phases=tuple(
            tuple(
                relation_name
                for relation_name in phase
                if relation_name != "operational_event_stream"
            )
            for phase in publication_commit_target.child_phases
        ),
    )
    drifted_publication_retention = replace(
        catalog,
        retention_targets=tuple(
            (
                incomplete_publication_target
                if target.target == publication_commit_target.target
                else target
            )
            for target in catalog.retention_targets
        ),
    )
    with pytest.raises(
        checker.ContractValidationError,
        match="exact operational preparation and transient-event children",
    ):
        checker.validate_cross_manifest_contracts(
            drifted_publication_retention, operational
        )


def test_operational_physical_manifest_is_generated_without_drift() -> None:
    completed = subprocess.run(
        [sys.executable, str(GENERATOR_PATH), "--check"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    provider_relations = operational_refinement.provider_relation_names(PHYSICAL_PATH)
    assert "schema_epoch_control" not in provider_relations
    assert len(provider_relations) == 65
    _logical, _local_names, physical, _stubs = _schemas()
    assert provider_relations == tuple(
        name for name in physical.source_slice if name != "schema_epoch_control"
    )
    position_by_relation = {
        name: position for position, name in enumerate(physical.source_slice)
    }
    for relation in physical.relations:
        for foreign_key in relation.foreign_keys:
            if (
                foreign_key.referenced_relation in position_by_relation
                and foreign_key.referenced_relation != relation.relation
            ):
                assert (
                    position_by_relation[foreign_key.referenced_relation]
                    < position_by_relation[relation.relation]
                )


def test_operational_physical_relation_member_names_are_unique() -> None:
    with PHYSICAL_PATH.open("rb") as stream:
        manifest = tomllib.load(stream)
    for relation in manifest["relation"]:
        for member in ("foreign_key", "required_index", "check"):
            names = [value["name"] for value in relation.get(member, [])]
            assert len(names) == len(set(names)), (
                f"{relation['name']} has duplicate {member} names: {names}"
            )


def test_operational_machine_obligations_and_genesis_are_closed_world() -> None:
    machine = operational_refinement.validate_operational_machine_contract(
        LOGICAL_PATH, PHYSICAL_PATH
    )

    assert len(machine.obligations) == 17
    assert len({value.obligation_id for value in machine.obligations}) == 17
    assert all(value.version == 1 for value in machine.obligations)
    assert all(value.scope.startswith("operational.") for value in machine.obligations)
    assert all(
        value.check.startswith("operational_refinement.")
        for value in machine.obligations
    )
    assert [
        value.obligation_id
        for value in machine.obligations
        if value.lifecycle == "building_only"
    ] == ["h2hdb.operational.bootstrap-genesis.v1"]
    assert any(
        value.obligation_id == "h2hdb.operational.revision-allocation.v1"
        and value.lifecycle == "ready_and_runtime"
        for value in machine.obligations
    )
    assert any(
        value.obligation_id == "h2hdb.operational.gallery-staging.v1"
        and value.lifecycle == "ready_and_runtime"
        for value in machine.obligations
    )
    assert any(
        value.obligation_id == "h2hdb.operational.download-ingest-handoff.v1"
        and value.lifecycle == "ready_and_runtime"
        for value in machine.obligations
    )
    assert all(value.lifecycle == "building_only" for value in machine.seeds)
    assert all(
        value.hook
        == "h2hdb.vnext_schema_provider.GeneratedVNextSchemaProvider.semantic_validators"
        for value in machine.obligations
    )
    assert machine.seeded_relations == (
        "revision_allocator",
        "identity_allocator",
        "deletion_request_generation",
        "deletion_request_generation_head",
        "gallery_observation_staging_request_budget",
        "cleanup_target_kind",
        "cleanup_phase",
        "cleanup_sweep_target",
    )
    assert machine.epoch_owned_relation == "schema_epoch_control"
    assert len(machine.absent_relations) == 57
    with PHYSICAL_PATH.open("rb") as stream:
        physical_document = tomllib.load(stream)
    assert len(machine.seeds) == len(physical_document.get("bootstrap_seed", ()))
    assert {
        value.seed_id
        for value in machine.seeds
        if value.relation == "revision_allocator"
    } == {
        "h2hdb.operational.revision-allocator.source.v1",
        "h2hdb.operational.revision-allocator.catalog.v1",
    }
    assert {
        tuple(cell.value for cell in value.cells)
        for value in machine.seeds
        if value.relation == "revision_allocator"
    } == {
        ("SOURCE", 1, 0),
        ("CATALOG", 1, 0),
    }
    assert {
        tuple(cell.value for cell in value.cells)
        for value in machine.seeds
        if value.relation == "identity_allocator"
    } == {
        ("GALLERY", 1, 0),
        ("TAG", 1, 0),
        ("POLICY", 1, 0),
    }
    assert {
        value.relation: tuple(cell.value for cell in value.cells)
        for value in machine.seeds
        if value.relation.startswith("deletion_request_generation")
    } == {
        "deletion_request_generation": (0, 0),
        "deletion_request_generation_head": (1, 0, 0),
    }
    assert [
        tuple(cell.value for cell in value.cells)
        for value in machine.seeds
        if value.relation == "gallery_observation_staging_request_budget"
    ] == [(1, 0)]
    queue_obligation = next(
        value
        for value in machine.obligations
        if value.obligation_id == "h2hdb.operational.queue-history.v1"
    )
    assert queue_obligation.relations == (
        "deletion_request_generation",
        "deletion_request_generation_head",
        "deletion_request_attempt",
        "deletion_request_head",
        "deletion_request_url",
        "operational_preparation",
        "operational_deletion_consumption_event",
    )


def test_cleanup_fk_descendant_and_root_codec_mutations_fail_closed() -> None:
    with LOGICAL_PATH.open("rb") as stream:
        logical = tomllib.load(stream)
    with PHYSICAL_PATH.open("rb") as stream:
        physical = tomllib.load(stream)

    missing_child = deepcopy(logical)
    observation = next(
        value
        for value in missing_child["cleanup_target"]
        if value["target_kind"] == "GALLERY_OBSERVATION"
    )
    next(
        phase
        for phase in observation["phases"]
        if "gallery_observation_metadata_local" in phase["relations"]
    )["relations"].remove("gallery_observation_metadata_local")
    with pytest.raises(ValueError, match="phase ownership"):
        operational_refinement.check_cleanup_reachability_v1(missing_child, physical)

    missing_reserved_projection = deepcopy(logical)
    candidate = next(
        value
        for value in missing_reserved_projection["cleanup_target"]
        if value["target_kind"] == "PUBLICATION_CANDIDATE"
    )
    candidate["phases"][0]["relations"].remove("catalog_publication_storage")
    with pytest.raises(
        ValueError,
        match="phase ownership|uncommitted reserved projection",
    ):
        operational_refinement.check_cleanup_reachability_v1(
            missing_reserved_projection, physical
        )

    invalid_key = deepcopy(logical)
    observation = next(
        value
        for value in invalid_key["cleanup_target"]
        if value["target_kind"] == "GALLERY_OBSERVATION"
    )
    observation["root_key"] = ["observation_id"]
    with pytest.raises(ValueError, match="machine binding drifts|candidate key"):
        operational_refinement.check_cleanup_reachability_v1(invalid_key, physical)

    string_blocker = deepcopy(logical)
    artifact = next(
        value
        for value in string_blocker["cleanup_target"]
        if value["target_kind"] == "ARTIFACT_BLOB"
    )
    artifact["retained_fk_edges"] = ["catalog_artifact_sha256.artifact_sha256"]
    with pytest.raises(ValueError, match="retained_fk_edges is invalid"):
        operational_refinement.check_cleanup_reachability_v1(string_blocker, physical)

    missing_publication_identity = deepcopy(logical)
    upload_time = next(
        value
        for value in missing_publication_identity["cleanup_target"]
        if value["target_kind"] == "GALLERY_UPLOAD_TIME"
    )
    assert upload_time["retention_roots"] == [
        "source_gallery_name_gid.gid",
        "publication_identity.gid",
        "analysis_impacted_gid_storage.gid",
    ]
    upload_time["retained_fk_edges"] = [
        edge
        for edge in upload_time["retained_fk_edges"]
        if edge["relation"] != "publication_identity"
    ]
    with pytest.raises(ValueError, match="structured FK boundary"):
        operational_refinement.check_cleanup_reachability_v1(
            missing_publication_identity, physical
        )

    missing_intermediary = deepcopy(logical)
    artifact = next(
        value
        for value in missing_intermediary["cleanup_target"]
        if value["target_kind"] == "ARTIFACT_BLOB"
    )
    artifact["phases"][0]["relations"] = ["title_sort", "artifact_blob"]
    with pytest.raises(ValueError, match="prunable intermediary coverage"):
        operational_refinement.check_cleanup_reachability_v1(
            missing_intermediary, physical
        )

    for kind in ("SOURCE_BUILD", "ANALYSIS_RUN"):
        drifted_handoff = deepcopy(logical)
        target = next(
            value
            for value in drifted_handoff["cleanup_target"]
            if value["target_kind"] == kind
        )
        target["state_root_handoff_rule"] += " without a checkpoint"
        with pytest.raises(ValueError, match="state-to-root handoff rule drifts"):
            operational_refinement.check_cleanup_reachability_v1(
                drifted_handoff,
                physical,
            )

    for kind, fragment, error in (
        (
            "SOURCE_BUILD",
            "at-most-one ABANDONED analysis retirement family",
            "successor-fence retention",
        ),
        (
            "ANALYSIS_RUN",
            "source_build_base_publication_commit.base_receipt_id",
            "base provenance retention",
        ),
        (
            "ANALYSIS_RUN",
            "globally latest source_build_generation",
            "latest analysis retirement retention",
        ),
        (
            "ANALYSIS_RUN",
            "schema-unreachable sibling family",
            "multi-analysis retirement retention",
        ),
        (
            "PUBLICATION_CANDIDATE",
            "publication_commit.candidate_id",
            "base candidate retention",
        ),
    ):
        missing_recovery_root = deepcopy(logical)
        target = next(
            value
            for value in missing_recovery_root["cleanup_target"]
            if value["target_kind"] == kind
        )
        target["retention_roots"] = [
            root for root in target["retention_roots"] if fragment not in root
        ]
        with pytest.raises(ValueError, match=error):
            operational_refinement.check_cleanup_reachability_v1(
                missing_recovery_root,
                physical,
            )


def test_cleanup_runtime_predicates_and_exact_key_codecs() -> None:
    facts = operational_refinement.PreparationCleanupFacts
    assert operational_refinement.operational_preparation_cleanup_eligible(
        facts("COMPLETE", True, True)
    )
    assert operational_refinement.operational_preparation_cleanup_eligible(
        facts("ABANDONED", True, False)
    )
    assert not operational_refinement.operational_preparation_cleanup_eligible(
        facts("COMPLETE", False, True)
    )
    assert not operational_refinement.operational_preparation_cleanup_eligible(
        facts("COMPLETE", True, False)
    )
    assert not operational_refinement.operational_preparation_cleanup_eligible(
        facts("FAILED", True, False)
    )
    assert not operational_refinement.operational_preparation_cleanup_eligible(
        facts("ABANDONED", True, True)
    )
    assert not operational_refinement.analysis_run_cleanup_eligible(
        retained_root_reachable=False, active_head_provenance=True
    )
    generation_facts = operational_refinement.SourceBuildGenerationCleanupFacts
    assert operational_refinement.source_build_generation_cleanup_eligible(
        generation_facts(True, False, False, False, False)
    )
    assert operational_refinement.source_build_generation_cleanup_eligible(
        generation_facts(False, True, False, False, False)
    )
    assert not operational_refinement.source_build_generation_cleanup_eligible(
        generation_facts(False, False, True, True, True)
    )
    assert not operational_refinement.source_build_generation_cleanup_eligible(
        generation_facts(True, False, True, False, False)
    )
    assert not operational_refinement.source_build_generation_cleanup_eligible(
        generation_facts(False, True, False, False, True)
    )
    authorize_canonical = operational_refinement.canonical_value_mutation_authorized
    assert authorize_canonical(
        shared_maintenance_slot_held=True,
        live_outer_owner_and_lease=True,
        exact_upload_claim_locked=True,
        cleanup_exclusive_cycle_active=False,
    )
    assert not authorize_canonical(
        shared_maintenance_slot_held=True,
        live_outer_owner_and_lease=True,
        exact_upload_claim_locked=True,
        cleanup_exclusive_cycle_active=True,
    )
    begin_upload = operational_refinement.canonical_value_upload_begin_authorized
    assert begin_upload(
        digest_domain="source_root_v1",
        current_head_owner_and_lease=True,
        shared_maintenance_slot_held=True,
        source_build_generation_mapping_present=False,
    )
    assert not begin_upload(
        digest_domain="tag_value_utf8_v1",
        current_head_owner_and_lease=True,
        shared_maintenance_slot_held=True,
        source_build_generation_mapping_present=False,
    )
    assert begin_upload(
        digest_domain="tag_value_utf8_v1",
        current_head_owner_and_lease=True,
        shared_maintenance_slot_held=True,
        source_build_generation_mapping_present=True,
    )
    handoff_upload = operational_refinement.canonical_value_upload_handoff_authorized
    handoff_facts = {
        "digest_domain": "source_root_v1",
        "current_head_owner_and_lease": True,
        "shared_maintenance_slot_held": True,
        "cleanup_exclusive_cycle_active": False,
        "final_identity_byte_validated": True,
        "retention_blocking_consumer_present_after_transaction": True,
        "source_build_generation_mapping_present_after_transaction": True,
        "source_scope_build_mapping_inserted_same_transaction": True,
        "exact_own_claim_locked": True,
        "own_claim_deleted": True,
        "retention_blocking_consumer_and_claim_change_same_transaction": True,
    }
    assert handoff_upload(**handoff_facts)
    for false_field in (
        "current_head_owner_and_lease",
        "shared_maintenance_slot_held",
        "final_identity_byte_validated",
        "retention_blocking_consumer_present_after_transaction",
        "source_build_generation_mapping_present_after_transaction",
        "source_scope_build_mapping_inserted_same_transaction",
        "exact_own_claim_locked",
        "own_claim_deleted",
        "retention_blocking_consumer_and_claim_change_same_transaction",
    ):
        assert not handoff_upload(**(handoff_facts | {false_field: False}))
    assert not handoff_upload(
        **(handoff_facts | {"cleanup_exclusive_cycle_active": True})
    )
    assert handoff_upload(
        **(
            handoff_facts
            | {
                "digest_domain": "tag_value_utf8_v1",
                "source_scope_build_mapping_inserted_same_transaction": False,
            }
        )
    )
    cleanup_upload = operational_refinement.canonical_value_upload_cleanup_authorized
    assert cleanup_upload(
        digest_domain="source_root_v1",
        generation_completed_or_strictly_superseded=True,
        current_or_live_generation=False,
        source_build_generation_mapping_present=False,
        exclusive_maintenance_gate_held=True,
        exact_claim_and_allocation_locked=True,
    )
    assert not cleanup_upload(
        digest_domain="tag_value_utf8_v1",
        generation_completed_or_strictly_superseded=True,
        current_or_live_generation=False,
        source_build_generation_mapping_present=False,
        exclusive_maintenance_gate_held=True,
        exact_claim_and_allocation_locked=True,
    )
    assert not cleanup_upload(
        digest_domain="source_root_v1",
        generation_completed_or_strictly_superseded=True,
        current_or_live_generation=True,
        source_build_generation_mapping_present=False,
        exclusive_maintenance_gate_held=True,
        exact_claim_and_allocation_locked=True,
    )
    assert not cleanup_upload(
        digest_domain="source_root_v1",
        generation_completed_or_strictly_superseded=True,
        current_or_live_generation=False,
        source_build_generation_mapping_present=False,
        exclusive_maintenance_gate_held=True,
        exact_claim_and_allocation_locked=False,
    )
    ingest_history = operational_refinement.ingest_generation_history_cleanup_authorized
    assert ingest_history(
        strictly_older_than_current=True,
        current_or_completed_head_reference=False,
        build_upload_or_staging_reference=False,
        owner_resume_authority=False,
        exclusive_maintenance_gate_held=True,
        rows_selected=256,
        maximum_rows=256,
    )
    assert not ingest_history(
        strictly_older_than_current=True,
        current_or_completed_head_reference=False,
        build_upload_or_staging_reference=True,
        owner_resume_authority=False,
        exclusive_maintenance_gate_held=True,
        rows_selected=1,
        maximum_rows=256,
    )
    maintenance_history = (
        operational_refinement.maintenance_generation_history_cleanup_authorized
    )
    assert maintenance_history(
        non_head_generation=True,
        owner_expired=True,
        holder_slots_absent=True,
        owner_or_head_reference_absent=True,
        newer_live_exclusive_generation=True,
        rows_selected=1,
        maximum_rows=256,
    )
    assert not maintenance_history(
        non_head_generation=True,
        owner_expired=True,
        holder_slots_absent=False,
        owner_or_head_reference_absent=True,
        newer_live_exclusive_generation=True,
        rows_selected=1,
        maximum_rows=256,
    )
    cutoff = 10_000
    max_age = 1_000
    assert operational_refinement.hash_cache_observation_cleanup_eligible(
        observed_at=cutoff - max_age,
        cycle_cutoff_at=cutoff,
        max_age_microseconds=max_age,
        exclusive_gate_held=True,
    )
    assert not operational_refinement.hash_cache_observation_cleanup_eligible(
        observed_at=cutoff - max_age + 1,
        cycle_cutoff_at=cutoff,
        max_age_microseconds=max_age,
        exclusive_gate_held=True,
    )
    assert not operational_refinement.hash_cache_observation_cleanup_eligible(
        observed_at=0,
        cycle_cutoff_at=cutoff,
        max_age_microseconds=max_age,
        exclusive_gate_held=False,
    )
    assert operational_refinement.cleanup_checkpoint_phase_matches(
        "ANALYSIS_RUN", "ANALYSIS_RUN"
    )
    assert not operational_refinement.cleanup_checkpoint_phase_matches(
        "ANALYSIS_RUN", "SOURCE_BUILD"
    )
    observation_key = operational_refinement.encode_cleanup_target_key(
        "GALLERY_OBSERVATION", (2,)
    )
    assert len(observation_key) == 32
    assert observation_key[-16:] == (2).to_bytes(8, "big") + bytes(8)
    with pytest.raises(ValueError, match="one shard"):
        operational_refinement.encode_cleanup_target_key(
            "GALLERY_OBSERVATION", (b"wrong",)
        )
    cleanup_id = operational_refinement.encode_cleanup_id("GALLERY_OBSERVATION", 2, 1)
    next_cleanup_id = operational_refinement.encode_cleanup_id(
        "GALLERY_OBSERVATION", 2, 2
    )
    assert len(cleanup_id) == 16 and cleanup_id != next_cleanup_id
    assert cleanup_id[7] == 2 and cleanup_id[-8:] == (1).to_bytes(8, "big")
    with pytest.raises(ValueError, match=r"1..2\^63-1"):
        operational_refinement.encode_cleanup_id("GALLERY_OBSERVATION", 2, 0)
    with pytest.raises(ValueError, match=r"1..2\^63-1"):
        operational_refinement.encode_cleanup_id("GALLERY_OBSERVATION", 2, 2**63)
    assert (
        operational_refinement.cleanup_replay_state(
            job_state="COMPLETE",
            job_cleanup_id=cleanup_id,
            job_cycle_generation=2,
            completion_cycle_generation=2,
            checkpoint_cleanup_id=None,
        )
        == "COMPLETE"
    )
    with pytest.raises(ValueError, match="corrupt or stale"):
        operational_refinement.cleanup_replay_state(
            job_state="OPEN",
            job_cleanup_id=next_cleanup_id,
            job_cycle_generation=2,
            completion_cycle_generation=1,
            checkpoint_cleanup_id=cleanup_id,
        )
    with pytest.raises(ValueError, match="corrupt or stale"):
        operational_refinement.cleanup_replay_state(
            job_state="OPEN",
            job_cleanup_id=next_cleanup_id,
            job_cycle_generation=2,
            completion_cycle_generation=2,
            checkpoint_cleanup_id=None,
        )
    with pytest.raises(ValueError, match="corrupt or incomplete"):
        operational_refinement.cleanup_replay_state(
            job_state="COMPLETE",
            job_cleanup_id=next_cleanup_id,
            job_cycle_generation=2,
            completion_cycle_generation=None,
            checkpoint_cleanup_id=None,
        )
    assert (
        operational_refinement.cleanup_replay_state(
            job_state="OPEN",
            job_cleanup_id=next_cleanup_id,
            job_cycle_generation=2,
            completion_cycle_generation=None,
            checkpoint_cleanup_id=None,
        )
        == "START"
    )
    assert operational_refinement.cleanup_may_resume(
        job_state="OPEN",
        job_cleanup_id=next_cleanup_id,
        job_cycle_generation=2,
        completion_cycle_generation=None,
        checkpoint_cleanup_id=next_cleanup_id,
    )


def test_gallery_staging_page_commit_and_latest_receipt_replay_are_disjoint() -> None:
    state = operational_refinement.gallery_staging_batch_state

    assert state(_gallery_batch_facts()) == "COMMIT"
    assert (
        state(
            _gallery_batch_facts(
                cursor=256,
                prior_request_key=b"request-1",
                latest_receipt_exists=True,
                latest_request_key_matches=True,
                latest_request_frame_matches=True,
                receipt_start_cursor=0,
                receipt_next_cursor=256,
            )
        )
        == "REPLAY"
    )
    assert (
        state(
            _gallery_batch_facts(
                cursor=256,
                prior_request_key=b"request-1",
                latest_receipt_exists=True,
                latest_request_key_matches=True,
                latest_request_frame_matches=False,
                receipt_start_cursor=0,
                receipt_next_cursor=256,
            )
        )
        == "REJECT"
    )
    assert state(_gallery_batch_facts(current_outer_head=False)) == "REJECT"
    assert (
        state(
            _gallery_batch_facts(
                current_outer_head=False,
                cursor=256,
                prior_request_key=b"request-1",
                latest_receipt_exists=True,
                latest_request_key_matches=True,
                latest_request_frame_matches=True,
                receipt_start_cursor=0,
                receipt_next_cursor=256,
            )
        )
        == "REJECT"
    )
    assert (
        state(
            _gallery_batch_facts(
                owner_matches=False,
                cursor=256,
                prior_request_key=b"request-1",
                latest_receipt_exists=True,
                latest_request_key_matches=True,
                latest_request_frame_matches=True,
                receipt_start_cursor=0,
                receipt_next_cursor=256,
            )
        )
        == "REJECT"
    )


def test_gallery_staging_terminal_page_and_ordering_edges_fail_closed() -> None:
    state = operational_refinement.gallery_staging_batch_state

    assert (
        state(
            _gallery_batch_facts(
                next_cursor=100,
                entry_count=100,
                subtree_item_count=100,
            )
        )
        == "REJECT"
    )
    assert (
        state(
            _gallery_batch_facts(
                next_cursor=100,
                entry_count=100,
                subtree_item_count=100,
                terminal=True,
                commits_complete=True,
            )
        )
        == "COMMIT"
    )
    assert state(_gallery_batch_facts(terminal=True, commits_complete=True)) == "COMMIT"
    assert (
        state(
            _gallery_batch_facts(
                next_cursor=0,
                entry_count=0,
                subtree_item_count=0,
                terminal=True,
                commits_complete=True,
            )
        )
        == "COMMIT"
    )
    assert (
        state(
            _gallery_batch_facts(
                cursor=256,
                presented_start_cursor=256,
                last_page_present=True,
                next_cursor=256,
                entry_count=0,
                subtree_item_count=0,
                terminal=True,
                commits_complete=True,
            )
        )
        == "REJECT"
    )
    assert state(_gallery_batch_facts(checkpoint_state="COMPLETE")) == "REJECT"
    assert state(_gallery_batch_facts(boundary_ordered=False)) == "REJECT"
    assert (
        state(
            _gallery_batch_facts(
                prior_request_key=b"current",
                presented_prior_request_key=b"stale",
            )
        )
        == "REJECT"
    )
    assert (
        state(
            _gallery_batch_facts(
                cursor=9223372036854775800,
                presented_start_cursor=9223372036854775800,
                next_cursor=9223372036854776056,
                last_page_present=True,
            )
        )
        == "REJECT"
    )


def test_gallery_staging_metadata_chunks_use_exact_byte_offsets() -> None:
    state = operational_refinement.gallery_staging_batch_state
    metadata = _gallery_batch_facts(
        component="METADATA",
        next_cursor=32768,
        entry_count=1,
        subtree_item_count=32768,
    )
    assert state(metadata) == "COMMIT"
    assert state(replace(metadata, metadata_offset_contiguous=False)) == "REJECT"
    assert (
        state(
            replace(
                metadata,
                next_cursor=12,
                subtree_item_count=12,
                terminal=True,
                commits_complete=True,
            )
        )
        == "COMMIT"
    )
    assert (
        state(
            replace(
                metadata,
                next_cursor=0,
                entry_count=0,
                subtree_item_count=0,
                terminal=True,
                commits_complete=True,
            )
        )
        == "REJECT"
    )


def test_gallery_staging_begin_takeover_allocator_and_cleanup_crash_edges() -> None:
    begin = operational_refinement.GalleryStagingBeginFacts
    begin_state = operational_refinement.gallery_staging_begin_state
    complete = dict(
        allocation_present=True,
        staging_header_present=True,
        level_zero_checkpoint_count=4,
        claim_count=1,
        match_checkpoint_count=1,
        metadata_parser_count=1,
        higher_level_checkpoint_count=0,
        request_row_count=0,
    )
    assert (
        begin_state(
            begin(
                True, True, False, False, True, **complete, transaction_committed=True
            )
        )
        == "BEGIN"
    )
    assert (
        begin_state(
            begin(True, True, True, True, False, **complete, transaction_committed=True)
        )
        == "RESUME"
    )
    assert (
        begin_state(
            begin(
                True,
                True,
                False,
                False,
                False,
                False,
                False,
                0,
                0,
                0,
                0,
                0,
                0,
                False,
            )
        )
        == "ROLLED_BACK"
    )
    assert (
        begin_state(
            begin(
                True,
                True,
                False,
                False,
                True,
                True,
                False,
                0,
                0,
                0,
                0,
                0,
                0,
                False,
            )
        )
        == "REJECT"
    )
    assert (
        begin_state(
            begin(
                False,
                True,
                False,
                False,
                True,
                **complete,
                transaction_committed=True,
            )
        )
        == "REJECT"
    )
    for missing in ("claim_count", "match_checkpoint_count", "metadata_parser_count"):
        incomplete = complete | {missing: 0}
        assert (
            begin_state(
                begin(
                    True,
                    True,
                    False,
                    False,
                    True,
                    **incomplete,
                    transaction_committed=True,
                )
            )
            == "REJECT"
        )
    assert (
        begin_state(
            begin(
                True,
                True,
                False,
                False,
                True,
                **(complete | {"request_row_count": 1}),
                transaction_committed=True,
            )
        )
        == "REJECT"
    )
    assert (
        begin_state(
            begin(
                True,
                False,
                False,
                False,
                True,
                **complete,
                transaction_committed=True,
            )
        )
        == "REJECT"
    )

    transition = operational_refinement.portable_allocator_transition
    assert transition(current_next_id=1, presented_next_id=1) == (1, 2)
    assert transition(
        current_next_id=9223372036854775806,
        presented_next_id=9223372036854775806,
    ) == (9223372036854775806, 9223372036854775807)
    assert (
        transition(
            current_next_id=9223372036854775807,
            presented_next_id=9223372036854775807,
        )
        is None
    )
    assert transition(current_next_id=2, presented_next_id=1) is None
    assert transition(current_next_id=True, presented_next_id=True) is None

    takeover = operational_refinement.gallery_staging_takeover_authorized
    assert takeover(
        staging_state="OPEN",
        live_outer_lease=True,
        current_outer_head=True,
        current_generation=2,
        presented_generation=2,
    )
    assert not takeover(
        staging_state="OPEN",
        live_outer_lease=True,
        current_outer_head=True,
        current_generation=3,
        presented_generation=2,
    )
    assert not takeover(
        staging_state="OPEN",
        live_outer_lease=True,
        current_outer_head=True,
        current_generation=9223372036854775807,
        presented_generation=9223372036854775807,
    )
    assert not takeover(
        staging_state="OPEN",
        live_outer_lease=True,
        current_outer_head=False,
        current_generation=2,
        presented_generation=2,
    )

    cleanup = operational_refinement.GalleryObservationCleanupFacts
    eligible = operational_refinement.gallery_observation_cleanup_eligible
    assert eligible(cleanup("ABANDONED", False, 3, 3, False))
    assert not eligible(cleanup("OPEN", False, 3, 3, False))
    assert not eligible(cleanup("ABANDONED", True, 3, 3, False))
    assert not eligible(cleanup("ABANDONED", False, 2, 3, False))
    assert not eligible(cleanup("ABANDONED", False, 3, 3, True))
    assert eligible(cleanup("ABSENT", False, 3, 3, False))
    assert not eligible(cleanup("ABSENT", False, 3, 3, True))


def test_gallery_staging_file_directory_lookup_is_bounded_and_receipted() -> None:
    state = operational_refinement.gallery_staging_match_state
    assert state(_gallery_match_facts()) == "COMMIT"
    terminal = _gallery_match_facts(
        cursor=b"file-leaf-1:0",
        presented_cursor=b"file-leaf-1:0",
        next_cursor=b"file-eof",
        matched_count=256,
        next_matched_count=300,
        step_count=44,
        terminal=True,
        commits_complete=True,
    )
    assert state(terminal) == "COMMIT"
    assert state(replace(terminal, directory_lookups_exact=False)) == "REJECT"
    assert state(replace(terminal, directory_regular_count=301)) == "REJECT"
    assert state(replace(terminal, step_count=257)) == "REJECT"
    replay = replace(
        terminal,
        checkpoint_state="COMPLETE",
        cursor=b"file-eof",
        matched_count=300,
        prior_request_key=b"request-2",
        latest_receipt_exists=True,
        latest_request_key_matches=True,
        latest_request_frame_matches=True,
        receipt_start_cursor=b"file-leaf-1:0",
        receipt_next_cursor=b"file-eof",
        receipt_next_matched_count=300,
    )
    assert state(replay) == "REPLAY"
    assert state(replace(replay, owner_matches=False)) == "REJECT"
    assert state(replace(replay, live_outer_lease=False)) == "REJECT"
    assert state(replace(replay, current_outer_head=False)) == "REJECT"
    assert state(replace(replay, latest_request_frame_matches=False)) == "REJECT"
    assert operational_refinement.gallery_staging_frontier_bound() == 8160
    with pytest.raises(ValueError, match="invalid"):
        operational_refinement.gallery_staging_frontier_bound(max_level=-1)


def test_gallery_staging_branch_carry_is_atomic_bounded_and_depth_limited() -> None:
    carry = operational_refinement.GalleryStagingCarryFacts
    state = operational_refinement.gallery_staging_carry_state
    full = carry(
        7,
        7,
        True,
        True,
        True,
        "FILE",
        1,
        256,
        65536,
        65536,
        True,
        True,
        True,
        True,
        True,
        True,
        False,
    )
    assert state(full) == "CARRY"
    assert state(replace(full, child_count=255)) == "REJECT"
    assert state(replace(full, level=0)) == "REJECT"
    assert state(replace(full, level=9)) == "REJECT"
    assert state(replace(full, internal_checkpoint_present=False)) == "REJECT"
    assert state(replace(full, allocation_page_associated=False)) == "REJECT"
    assert state(replace(full, owner_matches=False)) == "REJECT"
    assert state(replace(full, live_outer_lease=False)) == "REJECT"
    assert state(replace(full, current_outer_head=False)) == "REJECT"
    assert state(replace(full, encoded_subtree_item_count=65535)) == "REJECT"
    assert (
        state(
            replace(
                full,
                child_count=1,
                terminal_flush=True,
                prior_same_level_page_present=False,
            )
        )
        == "REUSE_CHILD_ROOT"
    )
    assert (
        state(
            replace(
                full,
                child_count=1,
                terminal_flush=True,
                prior_same_level_page_present=True,
            )
        )
        == "FLUSH_CARRY"
    )
    assert (
        state(
            replace(
                full,
                child_count=2,
                terminal_flush=True,
                prior_same_level_page_present=False,
            )
        )
        == "FLUSH_ROOT"
    )
    # 257 level-zero leaves produce one full level-one page plus a final
    # one-child nonroot page, then a canonical two-child level-two root.
    final_level_one = replace(
        full,
        child_count=1,
        terminal_flush=True,
        prior_same_level_page_present=True,
    )
    assert state(final_level_one) == "FLUSH_CARRY"
    assert (
        state(
            replace(
                full,
                level=2,
                child_count=2,
                terminal_flush=True,
                prior_same_level_page_present=False,
            )
        )
        == "FLUSH_ROOT"
    )


def test_gallery_branch_descriptor_validation_recomputes_count_and_order() -> None:
    descriptor = operational_refinement.GalleryPageDescriptorFact
    validate = operational_refinement.validate_gallery_branch_children
    first = descriptor(
        b"a" * 32,
        "FILE",
        0,
        256,
        (0).to_bytes(8, "big"),
        (255).to_bytes(8, "big"),
    )
    second = descriptor(
        b"b" * 32,
        "FILE",
        0,
        44,
        (256).to_bytes(8, "big"),
        (299).to_bytes(8, "big"),
    )
    assert validate(component="FILE", level=1, children=(first, second)) == 300
    assert (
        validate(
            component="FILE",
            level=1,
            children=(
                first,
                replace(second, first_key=(257).to_bytes(8, "big")),
            ),
        )
        is None
    )
    assert (
        validate(
            component="FILE",
            level=1,
            children=(first, replace(second, component="TAG")),
        )
        is None
    )
    assert (
        validate(
            component="FILE",
            level=1,
            children=tuple(first for _value in range(257)),
        )
        is None
    )
    metadata_first = replace(
        first,
        component="METADATA",
        subtree_item_count=32768,
        first_key=(0).to_bytes(8, "big"),
        last_key=(0).to_bytes(8, "big"),
    )
    metadata_second = replace(
        second,
        component="METADATA",
        subtree_item_count=12,
        first_key=(32768).to_bytes(8, "big"),
        last_key=(32768).to_bytes(8, "big"),
    )
    assert (
        validate(
            component="METADATA",
            level=1,
            children=(metadata_first, metadata_second),
        )
        == 32780
    )
    assert (
        validate(
            component="METADATA",
            level=1,
            children=(
                metadata_first,
                replace(metadata_second, first_key=(32769).to_bytes(8, "big")),
            ),
        )
        is None
    )
    assert (
        validate(
            component="FILE",
            level=1,
            children=(replace(first, subtree_item_count=0),),
        )
        is None
    )
    maximum_key = (2**64 - 1).to_bytes(8, "big")
    assert (
        validate(
            component="FILE",
            level=1,
            children=(replace(first, first_key=maximum_key, last_key=maximum_key),),
        )
        is None
    )


def test_gallery_page_request_codec_has_exact_golden_and_collision_preimage() -> None:
    encode = operational_refinement.encode_gallery_staging_page_request
    page_bytes = b"exact-page-frame"
    page_sha256 = hashlib.sha256(page_bytes).digest()
    request = encode(
        staging_id=bytes.fromhex("00112233445566778899aabbccddeeff"),
        ingest_generation=9,
        claim_generation=3,
        component="DIRECTORY",
        level=0,
        start_cursor=192,
        prior_request_sha256=bytes.fromhex("11" * 32),
        page_sha256=page_sha256,
        page_bytes=page_bytes,
        terminal=True,
    )
    assert request.hex() == (
        "68326864622d67616c6c6572792d73746167696e672d706167652d7265717565737400"
        "00000001"
        "00112233445566778899aabbccddeeff"
        "0000000000000009"
        "0000000000000003"
        "0200"
        "00000000000000c0"
        "01" + "11" * 32 + page_sha256.hex() + "00000010" + page_bytes.hex() + "01"
    )
    assert len(request) < 65792
    changed = encode(
        staging_id=bytes.fromhex("00112233445566778899aabbccddeeff"),
        ingest_generation=9,
        claim_generation=3,
        component="DIRECTORY",
        level=0,
        start_cursor=192,
        prior_request_sha256=bytes.fromhex("11" * 32),
        page_sha256=page_sha256,
        page_bytes=page_bytes,
        terminal=False,
    )
    assert changed != request
    assert operational_refinement.gallery_staging_request_sha256(changed) != (
        operational_refinement.gallery_staging_request_sha256(request)
    )
    with pytest.raises(ValueError, match="width"):
        encode(
            staging_id=b"short",
            ingest_generation=9,
            claim_generation=3,
            component="FILE",
            level=0,
            start_cursor=0,
            prior_request_sha256=None,
            page_sha256=bytes(32),
            page_bytes=b"x",
            terminal=False,
        )


def test_gallery_match_request_codec_is_domain_separated_and_subtype_exact() -> None:
    encode = operational_refinement.encode_gallery_staging_match_request
    request = encode(
        staging_id=bytes.fromhex("00112233445566778899aabbccddeeff"),
        ingest_generation=9,
        claim_generation=3,
        start_file_cursor_bytes=b"leaf:7",
        start_matched_count=256,
        prior_request_sha256=bytes.fromhex("33" * 32),
        terminal=False,
    )
    assert request.startswith(b"h2hdb-gallery-staging-match-request\0")
    assert not request.startswith(b"h2hdb-gallery-staging-page-request\0")
    assert request.hex() == (
        "68326864622d67616c6c6572792d73746167696e672d6d617463682d7265717565737400"
        "00000001"
        "00112233445566778899aabbccddeeff"
        "0000000000000009"
        "0000000000000003"
        "0006"
        "6c6561663a37"
        "0000000000000100"
        "01" + "33" * 32 + "00"
    )
    assert len(operational_refinement.gallery_staging_request_sha256(request)) == 32
    assert request != encode(
        staging_id=bytes.fromhex("00112233445566778899aabbccddeeff"),
        ingest_generation=9,
        claim_generation=3,
        start_file_cursor_bytes=b"leaf:8",
        start_matched_count=256,
        prior_request_sha256=bytes.fromhex("33" * 32),
        terminal=False,
    )
    for changed in (
        {"ingest_generation": 10},
        {"claim_generation": 4},
        {"prior_request_sha256": bytes.fromhex("44" * 32)},
        {"terminal": True},
    ):
        arguments = {
            "staging_id": bytes.fromhex("00112233445566778899aabbccddeeff"),
            "ingest_generation": 9,
            "claim_generation": 3,
            "start_file_cursor_bytes": b"leaf:7",
            "start_matched_count": 256,
            "prior_request_sha256": bytes.fromhex("33" * 32),
            "terminal": False,
        }
        arguments.update(changed)
        assert request != encode(**arguments)
    subtype = operational_refinement.gallery_staging_request_subtype_valid
    assert subtype(page_descriptor_count=1, match_descriptor_count=0)
    assert subtype(page_descriptor_count=0, match_descriptor_count=1)
    assert not subtype(page_descriptor_count=0, match_descriptor_count=0)
    assert not subtype(page_descriptor_count=1, match_descriptor_count=1)
    predecessor = operational_refinement.gallery_staging_predecessor_authorized
    staging_id = bytes.fromhex("00112233445566778899aabbccddeeff")
    assert predecessor(
        request_owner_staging_id=staging_id,
        prior_owner_staging_id=staging_id,
        exact_request_frame_names_prior=True,
        prior_has_successor=False,
    )
    assert not predecessor(
        request_owner_staging_id=staging_id,
        prior_owner_staging_id=bytes.fromhex("ffeeddccbbaa99887766554433221100"),
        exact_request_frame_names_prior=True,
        prior_has_successor=False,
    )
    assert not predecessor(
        request_owner_staging_id=staging_id,
        prior_owner_staging_id=staging_id,
        exact_request_frame_names_prior=False,
        prior_has_successor=False,
    )
    assert not predecessor(
        request_owner_staging_id=staging_id,
        prior_owner_staging_id=staging_id,
        exact_request_frame_names_prior=True,
        prior_has_successor=True,
    )
    with pytest.raises(ValueError, match="bounded"):
        encode(
            staging_id=bytes(16),
            ingest_generation=1,
            claim_generation=1,
            start_file_cursor_bytes=bytes(2049),
            start_matched_count=0,
            prior_request_sha256=None,
            terminal=False,
        )


def test_gallery_request_chunks_are_exact_contiguous_and_digest_bound() -> None:
    request = b"h2hdb-gallery-staging-page-request\0" + bytes(65530)
    chunks = operational_refinement.split_gallery_staging_request(request)
    positioned = tuple(enumerate(chunks))
    digest = hashlib.sha256(request).digest()
    assert (
        operational_refinement.validate_gallery_staging_request_chunks(
            request_sha256=digest, chunks=positioned
        )
        == request
    )
    assert len(chunks) == 3 and len(chunks[0]) == len(chunks[1]) == 32768
    assert (
        operational_refinement.validate_gallery_staging_request_chunks(
            request_sha256=digest, chunks=((0, chunks[0]), (2, chunks[2]))
        )
        is None
    )
    assert (
        operational_refinement.validate_gallery_staging_request_chunks(
            request_sha256=digest, chunks=((0, chunks[0][:-1]), (1, chunks[1]))
        )
        is None
    )
    assert (
        operational_refinement.validate_gallery_staging_request_chunks(
            request_sha256=bytes(32), chunks=positioned
        )
        is None
    )
    exact = operational_refinement.validate_gallery_staging_request_materialization
    assert exact(
        request_sha256=digest,
        chunks=positioned,
        owner_count=1,
        page_descriptor_count=1,
        match_descriptor_count=0,
        reencoded_request_bytes=request,
    )
    assert not exact(
        request_sha256=digest,
        chunks=positioned,
        owner_count=1,
        page_descriptor_count=1,
        match_descriptor_count=0,
        reencoded_request_bytes=request[:-1] + b"\1",
    )
    assert not exact(
        request_sha256=digest,
        chunks=positioned,
        owner_count=1,
        page_descriptor_count=1,
        match_descriptor_count=1,
        reencoded_request_bytes=request,
    )


def test_gallery_metadata_parser_handles_chunk_splits_and_exact_eof() -> None:
    parser = operational_refinement.GalleryMetadataParserFacts
    state = operational_refinement.gallery_metadata_parser_state
    split_utf8 = parser(
        7,
        7,
        True,
        True,
        True,
        "TITLE",
        "TITLE",
        40000,
        7232,
        b"",
        b"\xe2\x82",
        b"",
        b"",
        32768,
        True,
        True,
        False,
        False,
    )
    assert state(split_utf8) == "ADVANCE"
    split_header = replace(
        split_utf8,
        prior_phase="HEADER",
        next_phase="HEADER",
        prior_field_remaining=0,
        next_field_remaining=0,
        prior_utf8_tail=b"",
        next_utf8_tail=b"",
        next_fixed_carry=b"header-fragment",
    )
    assert state(split_header) == "ADVANCE"
    complete = replace(
        split_utf8,
        prior_phase="SCALARS",
        next_phase="DONE",
        prior_field_remaining=8,
        next_field_remaining=0,
        prior_utf8_tail=b"",
        next_utf8_tail=b"",
        prior_fixed_carry=b"",
        next_fixed_carry=b"",
        chunk_length=8,
        terminal=True,
    )
    assert state(complete) == "COMPLETE"
    assert state(replace(complete, next_utf8_tail=b"\xe2")) == "REJECT"
    assert state(replace(complete, next_fixed_carry=b"x")) == "REJECT"
    assert state(replace(complete, next_field_remaining=1)) == "REJECT"
    assert state(replace(complete, trailing_bytes=True)) == "REJECT"
    assert state(replace(complete, transition_exact=False)) == "REJECT"
    assert state(replace(complete, owner_matches=False)) == "REJECT"
    assert state(replace(complete, live_outer_lease=False)) == "REJECT"
    assert state(replace(complete, current_outer_head=False)) == "REJECT"
    assert state(replace(split_utf8, next_phase="DONE")) == "REJECT"


def test_gallery_metadata_parser_concretely_decodes_every_split_boundary() -> None:
    advance = operational_refinement.advance_gallery_metadata_parser
    initial = operational_refinement.GalleryMetadataParserState()
    payload = _metadata_stream()
    expected_scalars = {
        "gid": 123,
        "title_byte_count": len("標題".encode()),
        "comment_byte_count": len(b"comment"),
        "upload_account_byte_count": len(b"account"),
        "upload_time": 1000,
        "download_time": 2000,
        "modified_time": 3000,
        "scan_version": 4,
        "source_file_count": 99,
        "page_count_presence": 1,
        "page_count": 12,
    }
    for split in range(1, len(payload)):
        first = advance(initial, payload[:split], terminal=False)
        first_row = operational_refinement.gallery_metadata_parser_state_to_row(first)
        assert (
            operational_refinement.gallery_metadata_parser_state_from_row(first_row)
            == first
        )
        final = advance(first, payload[split:], terminal=True)
        final_row = operational_refinement.gallery_metadata_parser_state_to_row(final)
        assert (
            operational_refinement.gallery_metadata_parser_state_from_row(final_row)
            == final
        )
        assert final.phase == "DONE"
        assert dict(final.scalars) == expected_scalars
        assert not final.utf8_tail and not final.fixed_carry

    no_page_count = advance(initial, _metadata_stream(page_count=None), terminal=True)
    assert no_page_count.phase == "DONE"
    assert dict(no_page_count.scalars)["page_count_presence"] == 0
    assert "page_count" not in dict(no_page_count.scalars)

    invalid_utf8 = bytearray(_metadata_stream(title="a"))
    title_at = invalid_utf8.index(b"a", 50)
    invalid_utf8[title_at] = 0xFF
    with pytest.raises(UnicodeDecodeError):
        advance(initial, bytes(invalid_utf8), terminal=True)
    with pytest.raises(ValueError, match="truncated"):
        advance(initial, payload[:-1], terminal=True)
    with pytest.raises(ValueError, match="trailing"):
        advance(initial, payload + b"x", terminal=True)
    wrong_prefix = b"X" + payload[1:]
    with pytest.raises(ValueError, match="prefix"):
        advance(initial, wrong_prefix, terminal=True)
    gid_offset = len(b"h2hdb-vnext-gallery-observation-metadata\0") + 4
    zero_gid = payload[:gid_offset] + bytes(8) + payload[gid_offset + 8 :]
    with pytest.raises(ValueError, match="gid must be positive"):
        advance(initial, zero_gid, terminal=True)
    scan_version_marker = (4).to_bytes(4, "big") + (99).to_bytes(8, "big")
    scan_offset = payload.rindex(scan_version_marker)
    zero_scan_version = payload[:scan_offset] + bytes(4) + payload[scan_offset + 4 :]
    with pytest.raises(ValueError, match="scan_version must be positive"):
        advance(initial, zero_scan_version, terminal=True)
    forged = operational_refinement.GalleryMetadataParserState(
        phase="PAGE_COUNT_PRESENCE",
    )
    with pytest.raises(ValueError, match="phase prefix|page count"):
        advance(forged, b"\0", terminal=True)
    with pytest.raises(ValueError, match="scalar|phase prefix"):
        advance(
            operational_refinement.GalleryMetadataParserState(
                phase="DONE", scalars=(("page_count_presence", 0),)
            ),
            b"x",
            terminal=True,
        )
    for forged_remaining in (0, 6):
        with pytest.raises(ValueError, match="remaining length"):
            advance(
                operational_refinement.GalleryMetadataParserState(
                    phase="TITLE",
                    field_remaining=forged_remaining,
                    scalars=(("gid", 1), ("title_byte_count", 5)),
                ),
                b"x",
                terminal=False,
            )
    final_row = operational_refinement.gallery_metadata_parser_state_to_row(
        advance(initial, payload, terminal=True)
    )
    with pytest.raises(ValueError, match="phase prefix|page count"):
        operational_refinement.gallery_metadata_parser_state_from_row(
            {**final_row, "gid": None}
        )
    with pytest.raises(ValueError, match="carry|canonical"):
        operational_refinement.gallery_metadata_parser_state_from_row(
            {**final_row, "fixed_carry": b"a"}
        )
    with pytest.raises(ValueError, match="UTF-8"):
        operational_refinement.gallery_metadata_parser_state_from_row(
            {
                **final_row,
                "phase": "TITLE",
                "remaining_text_bytes": 1,
                "utf8_tail": b"a",
                "fixed_carry": b"",
                "gid": 123,
                "title_byte_count": 2,
                "comment_byte_count": None,
                "upload_account_byte_count": None,
                "upload_time": None,
                "download_time": None,
                "modified_time": None,
                "scan_observation_version": None,
                "source_file_count": None,
                "page_count": None,
            }
        )


@pytest.mark.parametrize("page_count", [None, 0])
def test_gallery_metadata_parser_refines_authoritative_stream_codec(
    page_count: int | None,
) -> None:
    metadata = GalleryObservationMetadata(
        gid=123,
        title="a" * 32705 + "標" + "tail",
        comment="跨頁註解",
        upload_account="account",
        upload_time=1000,
        download_time=2000,
        modified_time=3000,
        scan_observation_version=4,
        source_file_count=99,
        page_count=page_count,
    )
    payload = b"".join(iter_gallery_observation_metadata_stream(metadata))
    chunks = tuple(
        payload[offset : offset + 32768] for offset in range(0, len(payload), 32768)
    )
    assert chunks and all(chunks)
    # The exact 3-byte UTF-8 character begins in one leaf and ends in the next.
    assert chunks[0][-1:] == "標".encode()[:1]
    authoritative = validate_gallery_observation_metadata_parts(chunks)

    state = operational_refinement.GalleryMetadataParserState()
    advance = operational_refinement.advance_gallery_metadata_parser
    for index, chunk in enumerate(chunks):
        state = advance(state, chunk, terminal=index == len(chunks) - 1)
        row = operational_refinement.gallery_metadata_parser_state_to_row(state)
        assert (
            operational_refinement.gallery_metadata_parser_state_from_row(row) == state
        )

    scalars = dict(state.scalars)
    assert state.phase == "DONE"
    assert scalars == {
        "gid": authoritative.gid,
        "title_byte_count": authoritative.title_byte_count,
        "comment_byte_count": authoritative.comment_byte_count,
        "upload_account_byte_count": authoritative.upload_account_byte_count,
        "upload_time": authoritative.upload_time,
        "download_time": authoritative.download_time,
        "modified_time": authoritative.modified_time,
        "scan_version": authoritative.scan_observation_version,
        "source_file_count": authoritative.source_file_count,
        "page_count_presence": int(authoritative.page_count is not None),
        **(
            {}
            if authoritative.page_count is None
            else {"page_count": authoritative.page_count}
        ),
    }


def test_reused_gallery_staging_is_reclaimed_only_child_first_in_bounded_batches() -> (
    None
):
    cleanup = operational_refinement.GalleryObservationCleanupFacts
    state = operational_refinement.gallery_observation_cleanup_batch_state
    reused = cleanup(
        "REUSED",
        False,
        3,
        3,
        False,
        reuse_target_is_other_observation=True,
        staged_children_remaining=True,
    )
    assert (
        state(
            facts=reused,
            phase="REQUEST_RECEIPT_FRONTIER",
            rows_deleted=256,
            maximum_rows=256,
            receipt_matches=True,
        )
        == "ADVANCE"
    )
    assert (
        state(
            facts=reused,
            phase="ALLOCATION",
            rows_deleted=1,
            maximum_rows=256,
            receipt_matches=True,
        )
        == "REJECT"
    )
    drained = replace(reused, staged_children_remaining=False)
    assert (
        state(
            facts=drained,
            phase="ALLOCATION",
            rows_deleted=1,
            maximum_rows=256,
            receipt_matches=True,
        )
        == "COMPLETE"
    )
    assert (
        state(
            facts=replace(drained, reuse_target_is_other_observation=False),
            phase="ALLOCATION",
            rows_deleted=1,
            maximum_rows=256,
            receipt_matches=True,
        )
        == "REJECT"
    )
    assert (
        state(
            facts=drained,
            phase="ALLOCATION",
            rows_deleted=257,
            maximum_rows=256,
            receipt_matches=True,
        )
        == "REJECT"
    )
    assert (
        state(
            facts=drained,
            phase="ALLOCATION",
            rows_deleted=1,
            maximum_rows=256,
            receipt_matches=False,
        )
        == "REJECT"
    )


def test_successful_gallery_staging_compacts_before_source_build_cleanup() -> None:
    facts = operational_refinement.GalleryStagingCompactionFacts
    authorized = operational_refinement.gallery_staging_compaction_authorized
    sealed = facts("SEALED", True, True, True, False, True, 256, 256)
    reused = facts("REUSED", True, True, False, True, True, 256, 256)
    assert authorized(sealed)
    assert authorized(reused)
    assert not authorized(replace(sealed, staging_state="OPEN"))
    assert not authorized(replace(sealed, source_build_gallery_present=False))
    assert not authorized(replace(sealed, link_names_own_observation=False))
    assert not authorized(replace(reused, link_names_other_final_observation=False))
    assert not authorized(replace(sealed, exact_header_and_claim_locked=False))
    assert not authorized(replace(sealed, exclusive_maintenance_gate_held=False))
    assert not authorized(replace(sealed, rows_selected=257))

    with LOGICAL_PATH.open("rb") as stream:
        document = tomllib.load(stream)
    target = next(
        value
        for value in document["cleanup_target"]
        if value["target_kind"] == "GALLERY_OBSERVATION_STAGING"
    )
    compacted = {
        relation for phase in target["phases"] for relation in phase["relations"]
    }
    assert "gallery_observation_staging" in compacted
    assert "gallery_observation_allocation" not in compacted
    assert "gallery_observation_allocation_page" not in compacted
    assert "gallery_observation" not in compacted
    assert "source_build_gallery" not in compacted
    assert target["predecessor_selectors"] == [
        {
            "relation": "gallery_observation_staging_request_predecessor",
            "attribute": "request_sha256",
            "owner_relation": "gallery_observation_staging_request",
            "owner_attribute": "request_sha256",
        },
    ]
    assert target["predecessor_blockers"] == [
        {
            "relation": "gallery_observation_staging_request_predecessor",
            "incoming_attribute": "prior_request_sha256",
            "successor_attribute": "request_sha256",
            "owner_relation": "gallery_observation_staging_request",
            "owner_request_attribute": "request_sha256",
            "owner_staging_attribute": "staging_id",
            "rule": "lock both ownership-bearing request rows; an incoming edge whose successor owner differs from the selected staging blocks cleanup and is never deleted by that staging",
        }
    ]


def test_cleanup_registry_foreign_keys_are_enforced_by_sqlite_and_mariadb() -> None:
    _logical, _local_names, physical, stubs = _schemas()
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(
            operational_refinement.render_sqlite_ddl(physical, stubs)
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO operational_cleanup_target_kinds(target_kind) VALUES ('UNKNOWN')"
            )
        connection.execute(
            "INSERT INTO operational_cleanup_target_kinds(target_kind) VALUES ('SOURCE_BUILD')"
        )
        target_key = operational_refinement.encode_cleanup_target_key(
            "SOURCE_BUILD", (0,)
        )
        connection.execute(
            "INSERT INTO operational_cleanup_sweep_targets"
            "(target_kind,shard_no,target_key) VALUES ('SOURCE_BUILD',0,?)",
            (target_key,),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO operational_cleanup_sweep_targets(target_kind,shard_no,target_key) VALUES ('SOURCE_BUILD',256,?)",
                (bytes(32),),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO operational_cleanup_phases(phase,target_kind,phase_order) VALUES ('ROGUE','SOURCE_BUILD',1)"
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO operational_cleanup_jobs "
                "(cleanup_id,target_key,cycle_generation,cycle_cutoff_at,"
                "algorithm_version,max_rows_per_transaction,"
                "hash_cache_max_age_microseconds,frozen_root_count,"
                "frozen_root_set_sha256,state,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    bytes(16),
                    bytes(32),
                    1,
                    1000,
                    2,
                    100,
                    100,
                    0,
                    _empty_cleanup_frozen_root_digest(bytes(16)),
                    "OPEN",
                    0,
                ),
            )
        invalid_jobs = (
            (b"a" * 16, 2, 100, 0, "ROGUE", 0, None),
            (b"b" * 16, 2, 0, 0, "OPEN", 0, None),
            (b"c" * 16, 1, 100, 0, "OPEN", 0, None),
            (b"d" * 16, 2, 257, 0, "OPEN", 0, None),
            (b"e" * 16, 2, 100, 101, "OPEN", 0, None),
            (b"f" * 16, 2, 100, 0, "OPEN", 0, 7),
            (b"g" * 16, 2, 100, 0, "COMPLETE", 0, None),
            (b"h" * 16, 2, 100, 0, "COMPLETE", 7, 6),
        )
        for (
            cleanup_id,
            algorithm_version,
            max_rows,
            frozen_root_count,
            state,
            created_at,
            completed_at,
        ) in invalid_jobs:
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    "INSERT INTO operational_cleanup_jobs "
                    "(cleanup_id,target_key,cycle_generation,cycle_cutoff_at,"
                    "algorithm_version,max_rows_per_transaction,"
                    "hash_cache_max_age_microseconds,frozen_root_count,"
                    "frozen_root_set_sha256,state,created_at,completed_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        cleanup_id,
                        target_key,
                        1,
                        1000,
                        algorithm_version,
                        max_rows,
                        100,
                        frozen_root_count,
                        _empty_cleanup_frozen_root_digest(cleanup_id),
                        state,
                        created_at,
                        completed_at,
                    ),
                )
    finally:
        connection.close()

    mariadb = operational_refinement.render_mariadb_ddl(physical, stubs)
    joined = "\n".join(mariadb)
    assert (
        "FOREIGN KEY (`target_key`) REFERENCES `operational_cleanup_sweep_targets` (`target_key`)"
        in joined
    )
    assert (
        "FOREIGN KEY (`phase`) REFERENCES `operational_cleanup_phases` (`phase`)"
        in joined
    )
    assert "state IN ('OPEN', 'COMPLETE')" in joined
    assert (
        "CONSTRAINT `ck_cleanup_job_progress_bounds` CHECK "
        "(algorithm_version = 2 AND max_rows_per_transaction > 0 AND "
        "max_rows_per_transaction <= 256 AND frozen_root_count >= 0 AND "
        "frozen_root_count <= max_rows_per_transaction)"
    ) in joined
    assert "`frozen_root_key` VARBINARY(260) NOT NULL" in joined
    assert (
        "CONSTRAINT `ck_cleanup_cycle_root_frame_bounds` CHECK "
        "(octet_length(frozen_root_key) >= 3 AND "
        "octet_length(frozen_root_key) <= 260)"
    ) in joined
    assert "completed_at >= created_at" in joined


@pytest.mark.parametrize(
    ("table", "field", "invalid", "message"),
    [
        ("fencing_contract", "takeover_rule", "greater_or_equal", "fencing_contract"),
        (
            "download_ingest_handoff_contract",
            "handoff_kinds",
            ["DOWNLOADER"],
            "download_ingest_handoff_contract",
        ),
        ("maintenance_gate_contract", "slot_count", 63, "maintenance_gate_contract"),
        (
            "bounded_work_contract",
            "terminal_rule",
            "complete_without_receipt",
            "bounded_work_contract",
        ),
        (
            "operational_event_integrity_contract",
            "stream_rule",
            "stream_may_commit_alone",
            "operational_event_integrity_contract",
        ),
        (
            "revision_allocator_contract",
            "allocation_rule",
            "set_any_value",
            "revision_allocator_contract",
        ),
    ],
)
def test_operational_protocol_contract_mutations_fail_closed(
    table: str, field: str, invalid: object, message: str
) -> None:
    with LOGICAL_PATH.open("rb") as stream:
        logical = tomllib.load(stream)
    with PHYSICAL_PATH.open("rb") as stream:
        physical = tomllib.load(stream)
    mutated = deepcopy(logical)
    mutated[table][field] = invalid

    with pytest.raises(ValueError, match=message):
        operational_refinement.validate_operational_machine_contract_documents(
            mutated, physical
        )


def test_gallery_parser_and_audit_contract_mutations_fail_closed() -> None:
    with LOGICAL_PATH.open("rb") as stream:
        logical = tomllib.load(stream)
    with PHYSICAL_PATH.open("rb") as stream:
        physical = tomllib.load(stream)

    assert tuple(logical["gallery_staging_contract"]["durable_parser_phases"]) == (
        GALLERY_OBSERVATION_DURABLE_PARSER_PHASES
    )
    assert (
        "zero-based contiguous ordinal sequence 0..n-1"
        in logical["gallery_staging_contract"]["normalized_fact_rule"]
    )

    one_based_file_numbers = deepcopy(logical)
    one_based_file_numbers["gallery_staging_contract"]["normalized_fact_rule"] = (
        one_based_file_numbers["gallery_staging_contract"][
            "normalized_fact_rule"
        ].replace(
            "zero-based contiguous ordinal sequence 0..n-1",
            "one-based contiguous ordinal sequence 1..n",
        )
    )
    with pytest.raises(ValueError, match="exact protocol text drifts"):
        operational_refinement.check_gallery_staging_contract_v1(
            one_based_file_numbers, physical
        )

    alias_phase = deepcopy(logical)
    phases = alias_phase["gallery_staging_contract"]["durable_parser_phases"]
    phases[phases.index("UPLOAD_ACCOUNT_LENGTH")] = "LENGTH"
    with pytest.raises(ValueError, match="structural contract drifts"):
        operational_refinement.check_gallery_staging_contract_v1(alias_phase, physical)

    reordered_audit = deepcopy(logical)
    reordered_audit["gallery_staging_contract"]["scan_audit_framing"] = reordered_audit[
        "gallery_staging_contract"
    ]["scan_audit_framing"].replace(
        "raw32(FILE root_page_sha256)", "raw32(TAG root_page_sha256)", 1
    )
    with pytest.raises(ValueError, match="structural contract drifts"):
        operational_refinement.check_gallery_staging_contract_v1(
            reordered_audit, physical
        )

    physical_alias = deepcopy(physical)
    parser = next(
        relation
        for relation in physical_alias["relation"]
        if relation["name"] == "gallery_observation_staging_metadata_parser"
    )
    phase_check = next(
        check
        for check in parser["check"]
        if check["name"] == "ck_gallery_observation_staging_metadata_parser_phase"
    )
    for backend in ("sqlite_expression", "mariadb_expression"):
        phase_check[backend] = phase_check[backend][:-1] + ", 'TITLE_TEXT')"
    with pytest.raises(ValueError, match="physical phase registry is not exact"):
        operational_refinement.check_gallery_staging_contract_v1(
            logical, physical_alias
        )


def test_operational_machine_binding_and_typed_seed_mutations_fail_closed() -> None:
    with LOGICAL_PATH.open("rb") as stream:
        logical = tomllib.load(stream)
    with PHYSICAL_PATH.open("rb") as stream:
        physical = tomllib.load(stream)

    invalid_binding = deepcopy(logical)
    invalid_binding["semantic_obligation"][0]["check"] = "prose.only"
    with pytest.raises(
        ValueError, match="unregistered version/scope/lifecycle/class/check"
    ):
        operational_refinement.validate_operational_machine_contract_documents(
            invalid_binding, physical
        )

    invalid_seed = deepcopy(logical)
    invalid_seed["bootstrap_seed"][0]["value"][1]["integer"] = 0
    with pytest.raises(ValueError, match="wrong typed values"):
        operational_refinement.validate_operational_machine_contract_documents(
            invalid_seed, physical
        )

    missing_generation_scope = deepcopy(logical)
    queue_obligation = next(
        value
        for value in missing_generation_scope["semantic_obligation"]
        if value["id"] == "h2hdb.operational.queue-history.v1"
    )
    queue_obligation["relations"].remove("deletion_request_generation_head")
    with pytest.raises(ValueError, match="generation relation or description"):
        operational_refinement.validate_operational_machine_contract_documents(
            missing_generation_scope, physical
        )

    missing_preparation_authority = deepcopy(logical)
    preparation = next(
        value
        for value in missing_preparation_authority["relation"]
        if value["name"] == "operational_preparation"
    )
    preparation["foreign_keys"] = [
        value
        for value in preparation["foreign_keys"]
        if value["relation"] != "deletion_request_generation"
    ]
    with pytest.raises(ValueError, match="lacks exact deletion generation FK"):
        operational_refinement.validate_operational_machine_contract_documents(
            missing_preparation_authority, physical
        )

    invented_genesis = deepcopy(logical)
    generation_seed = next(
        value
        for value in invented_genesis["bootstrap_seed"]
        if value["relation"] == "deletion_request_generation"
    )
    generation_seed["value"][0]["integer"] = 1
    with pytest.raises(ValueError, match="wrong typed values"):
        operational_refinement.validate_operational_machine_contract_documents(
            invented_genesis, physical
        )

    old_publication_coordinate = deepcopy(logical)
    event = next(
        value
        for value in old_publication_coordinate["relation"]
        if value["name"] == "operational_event"
    )
    event["attributes"][1] = "source_revision"
    event["declared_keys"][1][0] = "source_revision"
    with pytest.raises(ValueError, match="coordinate shape drifts"):
        operational_refinement.validate_operational_machine_contract_documents(
            old_publication_coordinate, physical
        )

    missing_effect_seal_fk = deepcopy(logical)
    activation = next(
        value
        for value in missing_effect_seal_fk["relation"]
        if value["name"] == "operational_activation"
    )
    activation["foreign_keys"] = [
        value
        for value in activation["foreign_keys"]
        if not (
            value["relation"] == "publication_commit"
            and value["attributes"] == ["preparation_id"]
        )
    ]
    with pytest.raises(ValueError, match="lacks exact effect FK"):
        operational_refinement.validate_operational_machine_contract_documents(
            missing_effect_seal_fk, physical
        )

    missing_candidate_binding_seal_fk = deepcopy(logical)
    candidate_binding = next(
        value
        for value in missing_candidate_binding_seal_fk["relation"]
        if value["name"] == "publication_candidate_preparation"
    )
    candidate_binding["foreign_keys"] = [
        value
        for value in candidate_binding["foreign_keys"]
        if value["relation"] != "operational_preparation_effect_seal"
    ]
    with pytest.raises(ValueError, match="lacks exact effect FK"):
        operational_refinement.validate_operational_machine_contract_documents(
            missing_candidate_binding_seal_fk, physical
        )

    missing_effect_obligation = deepcopy(logical)
    event_obligation = next(
        value
        for value in missing_effect_obligation["semantic_obligation"]
        if value["id"] == "h2hdb.operational.event-integrity.v1"
    )
    event_obligation["relations"].remove("operational_preparation_effect_seal")
    with pytest.raises(ValueError, match="relation or description binding drifts"):
        operational_refinement.validate_operational_machine_contract_documents(
            missing_effect_obligation, physical
        )

    ephemeral_generation_parent = deepcopy(logical)
    for relation_name in ("source_build_generation", "canonical_value_upload"):
        relation = next(
            value
            for value in ephemeral_generation_parent["relation"]
            if value["name"] == relation_name
        )
        generation_fk = next(
            value
            for value in relation["foreign_keys"]
            if value["attributes"] == ["generation"]
        )
        generation_fk["relation"] = "ingest_generation_owner"
    with pytest.raises(ValueError, match="immutable history, not owner liveness"):
        operational_refinement.validate_operational_machine_contract_documents(
            ephemeral_generation_parent, physical
        )


def test_every_operational_foreign_key_has_explicit_left_prefix_access() -> None:
    with PHYSICAL_PATH.open("rb") as stream:
        physical_document = tomllib.load(stream)
    operational_refinement.validate_operational_fk_access_paths(physical_document)
    expected_fk_indexes = {
        "ix_ingest_coordination_head_fk_1",
        "ix_ingest_coordination_head_fk_2",
        "ix_download_coordination_head_fk_1",
        "ix_download_coordination_head_fk_2",
        "ix_source_build_generation_fk_1",
        "ix_maintenance_gate_head_fk_1",
        "ix_maintenance_gate_owner_fk_1",
        "ix_deletion_request_generation_head_fk_1",
        "ix_gallery_observation_staging_claim_fk_2",
        "ix_gallery_observation_staging_request_fk_1",
        "ix_gallery_observation_staging_request_page_fk_2",
        "ix_canonical_value_upload_fk_2",
        "ix_gallery_redownload_state_fk_2",
        "ix_operational_preparation_fk_3",
        "ix_operational_preparation_fk_4",
        "ix_hash_cache_observation_fk_2",
        "ix_cleanup_checkpoint_fk_2",
    }
    actual_fk_indexes = {
        str(index["name"])
        for relation in physical_document["relation"]
        for index in relation.get("required_index", [])
        if "_fk_" in str(index["name"])
    }
    assert actual_fk_indexes == expected_fk_indexes

    invalid = deepcopy(physical_document)
    head = next(
        value
        for value in invalid["relation"]
        if value["name"] == "ingest_coordination_head"
    )
    head["required_index"] = [
        value
        for value in head["required_index"]
        if value["name"] != "ix_ingest_coordination_head_fk_1"
    ]
    with pytest.raises(ValueError, match="lacks a child-side left-prefix"):
        operational_refinement.validate_operational_fk_access_paths(invalid)


def test_operational_candidate_keys_never_use_arbitrary_payloads_or_prefixes() -> None:
    contract = checker.load_contract(LOGICAL_PATH)
    physical_text = PHYSICAL_PATH.read_text()
    for relation in contract.relations:
        assert all("url" not in key for key in relation.declared_keys)
        assert all("cursor_bytes" not in key for key in relation.declared_keys)
    assert "LONGTEXT" in physical_text
    assert "BINARY(16)" in physical_text
    assert "BINARY(32)" in physical_text
    assert "[[semantic_obligation]]" in physical_text
    assert "operational_refinement.check_physical_domains_v1" in physical_text
    assert "[domain_obligations]" not in physical_text
    assert 'mariadb_type = "CHAR(64)"' not in physical_text
    assert "CHAR(32)" not in physical_text


def test_complete_operational_sqlite_fixture_physically_refines() -> None:
    logical, local_names, physical, stubs = _schemas()
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(
            operational_refinement.render_sqlite_ddl(physical, stubs)
        )
        database = refinement.introspect_sqlite(_SQLiteReader(connection))
    finally:
        connection.close()

    report = operational_refinement.compare_operational_refinement(
        logical,
        local_names,
        physical,
        PHYSICAL_PATH,
        database,
    )

    assert report.conforms, report.render()
    assert report.fully_conforms
    assert len(report.checked_relations) == 66
    assert report.pending_relations == ()
    assert database.table("h2hdb_schema_epoch") is not None
    assert database.table("operational_cleanup_batch_receipts") is None
    assert database.table("operational_operational_activations") is None
    assert database.table("operational_cleanup_checkpoints") is not None
    sqlite_index_names = {
        index.name for table in database.tables for index in table.indexes
    }
    assert {
        "ix_ingest_coordination_head_fk_1",
        "ix_ingest_coordination_head_fk_2",
        "ix_download_coordination_head_fk_1",
        "ix_download_coordination_head_fk_2",
        "ix_source_build_generation_fk_1",
        "ix_maintenance_gate_head_fk_1",
        "ix_maintenance_gate_owner_fk_1",
        "ix_deletion_request_generation_head_fk_1",
        "ix_gallery_observation_staging_claim_fk_2",
        "ix_gallery_observation_staging_request_fk_1",
        "ix_gallery_observation_staging_request_page_fk_2",
        "ix_canonical_value_upload_fk_2",
        "ix_gallery_redownload_state_fk_2",
        "ix_operational_preparation_fk_3",
        "ix_operational_preparation_fk_4",
        "ix_hash_cache_observation_fk_2",
        "ix_cleanup_checkpoint_fk_2",
    } <= sqlite_index_names
    refinement.assert_physical_refines(report, require_complete=True)


def test_operational_sqlite_typed_genesis_has_no_invented_control_facts() -> None:
    _logical, _local_names, physical, stubs = _schemas()
    machine = operational_refinement.validate_operational_machine_contract(
        LOGICAL_PATH, PHYSICAL_PATH
    )
    relation_by_name = {value.relation: value for value in physical.relations}
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(
            operational_refinement.render_sqlite_ddl(physical, stubs)
        )
        for seed in machine.seeds:
            table = relation_by_name[seed.relation].table
            assert table is not None
            columns = ", ".join(f'"{value.attribute}"' for value in seed.cells)
            placeholders = ", ".join("?" for _value in seed.cells)
            connection.execute(
                f'INSERT INTO "{table}" ({columns}) VALUES ({placeholders})',
                tuple(value.value for value in seed.cells),
            )
        assert connection.execute("""
            SELECT stream, next_revision, updated_at
            FROM operational_revision_allocators ORDER BY stream
            """).fetchall() == [("CATALOG", 1, 0), ("SOURCE", 1, 0)]
        assert connection.execute("""
            SELECT generation, allocated_at
            FROM operational_deletion_request_generations
            """).fetchall() == [(0, 0)]
        assert connection.execute("""
            SELECT singleton_id, current_generation, updated_at
            FROM operational_deletion_request_generation_heads
            """).fetchall() == [(1, 0, 0)]
        for relation_name in machine.absent_relations:
            table = relation_by_name[relation_name].table
            assert table is not None
            assert connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone() == (
                0,
            )
        connection.execute("""
            UPDATE operational_revision_allocators
            SET next_revision = 2, updated_at = 1
            WHERE stream = 'SOURCE'
            """)
        assert connection.execute("""
            SELECT next_revision, updated_at
            FROM operational_revision_allocators WHERE stream = 'SOURCE'
            """).fetchone() == (2, 1)
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("""
                UPDATE operational_revision_allocators
                SET next_revision = 0 WHERE stream = 'CATALOG'
                """)
    finally:
        connection.close()


def test_canonical_external_stub_uses_only_physical_page_tree_authority() -> None:
    _logical, _local_names, physical, stubs = _schemas()
    canonical = next(
        stub for stub in stubs if stub.relation == "canonical_value_identity"
    )
    assert canonical.columns == (
        ("value_sha256", "BLOB", "BINARY(32)"),
        ("root_page_sha256", "BLOB", "BINARY(32)"),
    )
    assert canonical.unique_keys == (("root_page_sha256",),)
    assert all(stub.relation != "canonical_value_page" for stub in stubs)
    with PHYSICAL_PATH.open("rb") as stream:
        physical_document = tomllib.load(stream)
    assert "canonical_value_page" in physical_document["external_inline_projections"]
    sqlite_ddl = operational_refinement.render_sqlite_external_stubs(stubs)
    mariadb_ddl = "\n".join(operational_refinement.render_mariadb_external_stubs(stubs))
    assert "catalog_canonical_value_pages" not in sqlite_ddl
    assert "catalog_canonical_value_pages" not in mariadb_ddl

    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(
            operational_refinement.render_sqlite_ddl(physical, stubs)
        )
        database = refinement.introspect_sqlite(_SQLiteReader(connection))
    finally:
        connection.close()
    table = database.table("catalog_canonical_value_identities")
    assert table is not None
    assert set(table.candidate_keys) == {
        frozenset({"value_sha256"}),
        frozenset({"root_page_sha256"}),
    }


def test_source_root_upload_can_bootstrap_before_build_mapping_in_sqlite() -> None:
    _logical, _local_names, physical, stubs = _schemas()
    connection = sqlite3.connect(":memory:")
    connection.execute("PRAGMA foreign_keys = ON")
    owner = b"source-root-ownr"
    build_id = b"source-root-bld1"
    value_sha256 = bytes.fromhex("51" * 32)
    assert len(owner) == len(build_id) == 16
    try:
        connection.executescript(
            operational_refinement.render_sqlite_ddl(physical, stubs)
        )
        with connection:
            connection.execute(
                "INSERT INTO operational_ingest_generations "
                "(generation, started_at, completed_at) VALUES (1, 1, NULL)"
            )
            connection.execute(
                "INSERT INTO operational_ingest_generation_owners "
                "(generation, owner_token, claimed_at, lease_expires_at) "
                "VALUES (1, ?, 1, 100)",
                (owner,),
            )
            connection.execute(
                "INSERT INTO catalog_canonical_value_allocation_anchors "
                "(value_sha256) VALUES (?)",
                (value_sha256,),
            )
            connection.execute(
                "INSERT INTO catalog_canonical_value_allocation_seals "
                "(value_sha256) VALUES (?)",
                (value_sha256,),
            )
            # The source-root claim intentionally precedes the build mapping
            # that its final identity enables; all declared FKs remain valid.
            connection.execute(
                "INSERT INTO operational_canonical_value_uploads "
                "(generation, value_sha256) VALUES (1, ?)",
                (value_sha256,),
            )
        assert connection.execute(
            "SELECT COUNT(*) FROM operational_source_build_generations"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM operational_canonical_value_uploads"
        ).fetchone() == (1,)

        with connection:
            connection.execute(
                "INSERT INTO catalog_source_build_descriptor "
                "(build_id, scope_key, manifest_policy_id, created_at) "
                "VALUES (?, ?, 1, 1)",
                (build_id, b"s" * 32),
            )
            connection.execute(
                "INSERT INTO operational_source_build_generations "
                "(build_id, generation) VALUES (?, 1)",
                (build_id,),
            )
            connection.execute(
                "DELETE FROM operational_canonical_value_uploads "
                "WHERE generation = 1 AND value_sha256 = ?",
                (value_sha256,),
            )
        assert connection.execute(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = 1"
        ).fetchone() == (build_id,)
        assert connection.execute(
            "SELECT COUNT(*) FROM operational_canonical_value_uploads"
        ).fetchone() == (0,)
    finally:
        connection.close()


def test_operational_ddl_renderers_order_local_foreign_key_parents_first() -> None:
    _logical, _local_names, physical, stubs = _schemas()
    forward_reference_order = (
        "download_ingest_consumption",
        "coordinated_ingest_completion",
        "ingest_generation",
    )
    reordered = replace(
        physical,
        source_slice=(
            *forward_reference_order,
            *(
                name
                for name in physical.source_slice
                if name not in forward_reference_order
            ),
        ),
    )

    sqlite_ddl = operational_refinement.render_sqlite_ddl(reordered, stubs)
    mariadb_ddl = "\n".join(operational_refinement.render_mariadb_ddl(reordered, stubs))
    for ddl, quote in ((sqlite_ddl, '"'), (mariadb_ddl, "`")):
        parent = ddl.index(f"CREATE TABLE {quote}operational_ingest_generations{quote}")
        assert parent < ddl.index(
            f"CREATE TABLE {quote}operational_download_ingest_consumptions{quote}"
        )
        assert parent < ddl.index(
            f"CREATE TABLE {quote}operational_coordinated_ingest_completions{quote}"
        )


def test_operational_ddl_renderers_reject_local_foreign_key_cycles() -> None:
    _logical, _local_names, physical, stubs = _schemas()
    ingest_generation = physical.relation("ingest_generation")
    assert ingest_generation is not None
    cyclic_ingest_generation = replace(
        ingest_generation,
        foreign_keys=(
            *ingest_generation.foreign_keys,
            refinement.PhysicalForeignKeySpec(
                "fk_test_ingest_generation_cycle",
                ("generation",),
                "download_ingest_consumption",
                ("ingest_generation",),
            ),
        ),
    )
    cyclic = replace(
        physical,
        relations=tuple(
            (
                cyclic_ingest_generation
                if relation.relation == "ingest_generation"
                else relation
            )
            for relation in physical.relations
        ),
    )

    with pytest.raises(ValueError, match="operational physical dependency cycle"):
        operational_refinement.render_sqlite_ddl(cyclic, stubs)
    with pytest.raises(ValueError, match="operational physical dependency cycle"):
        operational_refinement.render_mariadb_ddl(cyclic, stubs)


def test_operational_mariadb_renderer_uses_exact_binary_domains() -> None:
    _logical, _local_names, physical, stubs = _schemas()
    statements = operational_refinement.render_mariadb_ddl(physical, stubs)
    ddl = "\n".join(statements)
    assert all(
        len(identifier.encode("ascii")) <= 64
        for statement in statements
        for identifier in statement.split("`")[1::2]
    )
    holder_ddl = next(
        statement
        for statement in statements
        if statement.startswith("CREATE TABLE `operational_maintenance_gate_holders`")
    )
    generation_ddl = next(
        statement
        for statement in statements
        if statement.startswith("CREATE TABLE `operational_source_build_generations`")
    )
    preparation_ddl = next(
        statement
        for statement in statements
        if statement.startswith("CREATE TABLE `operational_operational_preparations`")
    )
    effect_seal_ddl = next(
        statement
        for statement in statements
        if statement.startswith(
            "CREATE TABLE `operational_operational_preparation_effect_seals`"
        )
    )
    event_ddl = next(
        statement
        for statement in statements
        if statement.startswith("CREATE TABLE `operational_operational_events`")
    )
    canonical_upload_ddl = next(
        statement
        for statement in statements
        if statement.startswith("CREATE TABLE `operational_canonical_value_uploads`")
    )
    deletion_generation_ddl = next(
        statement
        for statement in statements
        if statement.startswith(
            "CREATE TABLE `operational_deletion_request_generations`"
        )
    )
    deletion_generation_head_ddl = next(
        statement
        for statement in statements
        if statement.startswith(
            "CREATE TABLE `operational_deletion_request_generation_heads`"
        )
    )
    cleanup_ddl = next(
        statement
        for statement in statements
        if statement.startswith("CREATE TABLE `operational_cleanup_jobs`")
    )
    staging_checkpoint_ddl = next(
        statement
        for statement in statements
        if statement.startswith(
            "CREATE TABLE `operational_gallery_observation_staging_checkpoints`"
        )
    )
    staging_receipt_ddl = next(
        statement
        for statement in statements
        if statement.startswith(
            "CREATE TABLE `operational_gallery_observation_staging_receipts`"
        )
    )

    assert "`manifest_sha256` BINARY(32) NOT NULL" in ddl
    assert "`owner_token` BINARY(16) NOT NULL" in ddl
    assert "`url` LONGTEXT COLLATE utf8mb4_nopad_bin NOT NULL" in ddl
    assert "UNIQUE (`request_token`)" in ddl
    assert "UNIQUE (`url`)" not in ddl
    assert "ck_download_request_url" not in ddl
    assert "ck_deletion_request_url_url" not in ddl
    assert "PRIMARY KEY (`slot`)" in holder_ddl
    assert "UNIQUE (`owner_token`)" not in holder_ddl
    assert (
        "CREATE INDEX `ix_maintenance_gate_holder_owner` "
        "ON `operational_maintenance_gate_holders` (`owner_token`)"
    ) in ddl
    assert "PRIMARY KEY (`generation`)" in generation_ddl
    assert "UNIQUE (`build_id`)" not in generation_ddl
    assert (
        "FOREIGN KEY (`generation`) REFERENCES "
        "`operational_ingest_generations` (`generation`)"
    ) in generation_ddl
    assert "REFERENCES `operational_ingest_generation_owners`" not in generation_ddl
    assert (
        "FOREIGN KEY (`generation`) REFERENCES "
        "`operational_ingest_generations` (`generation`)"
    ) in canonical_upload_ddl
    assert (
        "REFERENCES `operational_ingest_generation_owners`" not in canonical_upload_ddl
    )
    assert (
        "UNIQUE (`build_id`, `deletion_request_generation`, `operational_policy_id`)"
    ) in preparation_ddl
    assert "PRIMARY KEY (`generation`)" in deletion_generation_ddl
    assert "`allocated_at` BIGINT UNSIGNED NOT NULL" in deletion_generation_ddl
    assert "PRIMARY KEY (`singleton_id`)" in deletion_generation_head_ddl
    assert (
        "FOREIGN KEY (`current_generation`) REFERENCES "
        "`operational_deletion_request_generations` (`generation`)"
    ) in deletion_generation_head_ddl
    assert (
        "FOREIGN KEY (`deletion_request_generation`) REFERENCES "
        "`operational_deletion_request_generations` (`generation`)"
    ) in preparation_ddl
    assert (
        "FOREIGN KEY (`preparation_id`) REFERENCES "
        "`operational_operational_event_streams` (`preparation_id`)"
    ) in preparation_ddl
    assert "state IN ('OPEN', 'COMPLETE', 'FAILED', 'ABANDONED')" in preparation_ddl
    assert "`event_count` BIGINT UNSIGNED NOT NULL" in effect_seal_ddl
    assert "`final_chain_sha256` BINARY(32) NOT NULL" in effect_seal_ddl
    assert "`sealed_at` BIGINT UNSIGNED NOT NULL" in effect_seal_ddl
    assert not any(
        "operational_operational_activations" in statement for statement in statements
    )
    assert "UNIQUE (`preparation_id`, `sequence_no`)" in event_ddl
    assert "`source_revision`" not in event_ddl
    assert (
        "FOREIGN KEY (`preparation_id`) REFERENCES "
        "`operational_operational_event_streams` (`preparation_id`)"
    ) in event_ddl
    assert "UNIQUE (`target_key`)" in cleanup_ddl
    assert "`cycle_generation` BIGINT UNSIGNED NOT NULL" in cleanup_ddl
    assert "CHECK (`cursor` >= 0 AND `cursor` <= 9223372036854775807)" in (
        staging_checkpoint_ddl
    )
    assert "CHECK (regular_count <= `cursor` AND" in staging_checkpoint_ddl
    assert "component <> X'46494C45' OR level <> 0" in staging_receipt_ddl
    assert "!=" not in staging_receipt_ddl
    assert (
        "CONSTRAINT `ck_operational_event_type` CHECK "
        "(event_type IN ('REMOVED_GID', 'DELETION_CONSUMPTION'))"
    ) in ddl
    assert "CREATE INDEX `ix_operational_event_revision`" not in ddl
    assert "CREATE INDEX `ix_file_hash_cache_hash`" in ddl


def test_operational_activation_is_an_inline_projection_without_sql_object() -> None:
    with LOGICAL_PATH.open("rb") as stream:
        logical = tomllib.load(stream)
    activation = next(
        relation
        for relation in logical["relation"]
        if relation["name"] == "operational_activation"
    )
    assert activation["materialization"] == {
        "authoritative": False,
        "storage": "inline_projection",
        "view_pattern": "publication_commit_activation",
        "rationale": (
            "Project activation only from one complete immutable publication "
            "commit without publishing a separate SQL object."
        ),
        "derived_from": ["publication_commit"],
        "refresh_strategy": (
            "readers inline the exact source_revision/preparation_id/"
            "operational_policy_id/committed_at projection from publication_commit"
        ),
    }
    _logical, _local_names, physical, stubs = _schemas()
    assert not any(
        "operational_operational_activations" in statement
        for statement in operational_refinement.render_mariadb_ddl(physical, stubs)
    )


def test_complete_operational_mariadb_fixture_physically_refines(
    mariadb_config: CoreConfig,
) -> None:
    logical, local_names, physical, stubs = _schemas()
    machine = operational_refinement.validate_operational_machine_contract(
        LOGICAL_PATH, PHYSICAL_PATH
    )
    relation_by_name = {value.relation: value for value in physical.relations}
    database_config = mariadb_config.database
    with MariaDBConnector(
        host=database_config.host,
        port=database_config.port,
        user=database_config.user,
        password=database_config.password,
        database=database_config.database,
    ) as connector:
        for statement in operational_refinement.render_mariadb_ddl(physical, stubs):
            connector.execute(statement)
        for seed in machine.seeds:
            table = relation_by_name[seed.relation].table
            assert table is not None
            columns = ", ".join(f"`{value.attribute}`" for value in seed.cells)
            placeholders = ", ".join("%s" for _value in seed.cells)
            connector.execute(
                f"INSERT INTO `{table}` ({columns}) VALUES ({placeholders})",
                tuple(value.value for value in seed.cells),
            )
        assert connector.fetch_all("""
            SELECT stream, next_revision, updated_at
            FROM operational_revision_allocators ORDER BY stream
            """) == [("CATALOG", 1, 0), ("SOURCE", 1, 0)]
        connector.execute("""
            UPDATE operational_revision_allocators
            SET next_revision = 2, updated_at = 1
            WHERE stream = 'SOURCE'
            """)
        assert connector.fetch_one("""
            SELECT next_revision, updated_at
            FROM operational_revision_allocators WHERE stream = 'SOURCE'
            """) == (2, 1)
        owner = b"source-root-ownr"
        build_id = b"source-root-bld1"
        value_sha256 = bytes.fromhex("51" * 32)
        connector.commit()
        with connector.transaction():
            connector.execute(
                "INSERT INTO operational_ingest_generations "
                "(generation, started_at, completed_at) VALUES (1, 1, NULL)"
            )
            connector.execute(
                "INSERT INTO operational_ingest_generation_owners "
                "(generation, owner_token, claimed_at, lease_expires_at) "
                "VALUES (1, %s, 1, 100)",
                (owner,),
            )
            connector.execute(
                "INSERT INTO catalog_canonical_value_allocation_anchors "
                "(value_sha256) VALUES (%s)",
                (value_sha256,),
            )
            connector.execute(
                "INSERT INTO catalog_canonical_value_allocation_seals "
                "(value_sha256) VALUES (%s)",
                (value_sha256,),
            )
            connector.execute(
                "INSERT INTO operational_canonical_value_uploads "
                "(generation, value_sha256) VALUES (1, %s)",
                (value_sha256,),
            )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_source_build_generations"
        ) == (0,)
        connector.commit()
        with connector.transaction():
            connector.execute(
                "INSERT INTO catalog_source_build_descriptor "
                "(build_id, scope_key, manifest_policy_id, created_at) "
                "VALUES (%s, %s, 1, 1)",
                (build_id, b"s" * 32),
            )
            connector.execute(
                "INSERT INTO operational_source_build_generations "
                "(build_id, generation) VALUES (%s, 1)",
                (build_id,),
            )
            connector.execute(
                "DELETE FROM operational_canonical_value_uploads "
                "WHERE generation = 1 AND value_sha256 = %s",
                (value_sha256,),
            )
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = 1"
        ) == (build_id,)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_canonical_value_uploads"
        ) == (0,)
        database = refinement.introspect_mariadb(connector)

    report = operational_refinement.compare_operational_refinement(
        logical,
        local_names,
        physical,
        PHYSICAL_PATH,
        database,
    )

    assert report.conforms, report.render()
    assert report.fully_conforms
    schema_epoch = database.table("h2hdb_schema_epoch")
    assert schema_epoch is not None
    assert schema_epoch.column("manifest_sha256") == refinement.ColumnShape(
        "manifest_sha256",
        "BINARY(32)",
        False,
        None,
    )
    mariadb_index_names = {
        index.name for table in database.tables for index in table.indexes
    }
    assert {
        "ix_ingest_coordination_head_fk_1",
        "ix_ingest_coordination_head_fk_2",
        "ix_download_coordination_head_fk_1",
        "ix_download_coordination_head_fk_2",
        "ix_source_build_generation_fk_1",
        "ix_maintenance_gate_head_fk_1",
        "ix_maintenance_gate_owner_fk_1",
        "ix_deletion_request_generation_head_fk_1",
        "ix_gallery_observation_staging_claim_fk_2",
        "ix_gallery_observation_staging_request_fk_1",
        "ix_gallery_observation_staging_request_page_fk_2",
        "ix_canonical_value_upload_fk_2",
        "ix_gallery_redownload_state_fk_2",
        "ix_operational_preparation_fk_3",
        "ix_operational_preparation_fk_4",
        "ix_hash_cache_observation_fk_2",
    } <= mariadb_index_names
    refinement.assert_physical_refines(report, require_complete=True)
