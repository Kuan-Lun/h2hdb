from __future__ import annotations

from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest
import test_catalog_refinement_runtime as refinement_support
import test_vnext_cleanup_repository as cleanup_support
import test_vnext_publication_repository as publication_support

import h2hdb.vnext_publication_repository as publication_module
from h2hdb import catalog_refinement
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_cleanup_repository import CleanupTargetKind
from h2hdb.vnext_maintenance_gate_repository import MaintenanceGateRepository
from h2hdb.vnext_transaction import VNextUnitOfWork


def _application_state(
    connector: SQLiteConnector,
) -> tuple[tuple[str, tuple[tuple[Any, ...], ...]], ...]:
    """Capture every application base row, including allocator/working state."""

    names = tuple(
        cast(str, row[0])
        for row in connector.fetch_all(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' "
            "AND (name LIKE 'catalog_%' OR name LIKE 'operational_%') "
            "ORDER BY name"
        )
    )
    return tuple(
        (
            name,
            tuple(sorted(connector.fetch_all(f'SELECT * FROM "{name}"'), key=repr)),
        )
        for name in names
    )


def _delete_transient_candidate_definition(
    connector: SQLiteConnector,
) -> None:
    connector.execute("PRAGMA foreign_keys = OFF")
    connector.execute(
        "DELETE FROM catalog_publication_candidates WHERE candidate_id = %s",
        (publication_support._CANDIDATE,),
    )
    connector.execute("PRAGMA foreign_keys = ON")


@pytest.mark.parametrize(
    ("corruption", "error_match"),
    (
        ("sealed-member", "anchors differ from complete retained common commits"),
        ("missing-head", "common publication head family is incomplete"),
    ),
)
def test_ready_rejects_common_commit_or_head_corruption(
    tmp_path: Path,
    corruption: str,
    error_match: str,
) -> None:
    connector = refinement_support._generated_catalog_database(
        tmp_path / f"common-{corruption}.sqlite3"
    )
    try:
        analysis_id = refinement_support._insert_active_source_head(connector)
        refinement_support._insert_active_publication(connector, analysis_id)
        validator = catalog_refinement.builtin_semantic_validators()[
            "catalog.publication-atomicity.v1"
        ]
        validator(connector)

        if corruption == "sealed-member":
            connector.execute("PRAGMA foreign_keys = OFF")
            connector.execute(
                "DELETE FROM catalog_publication_commits WHERE receipt_id = %s",
                (b"t" * 16,),
            )
            connector.execute("PRAGMA foreign_keys = ON")
        else:
            connector.execute(
                "DELETE FROM catalog_publication_commit_head_receipts "
                "WHERE channel = %s",
                (b"default",),
            )

        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match=error_match,
        ):
            validator(connector)
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("corruption", "error_match"),
    (
        ("unsealed-anchor", "anchors differ from complete retained common commits"),
        ("missing-genesis", "generation nodes differ"),
    ),
)
def test_ready_rejects_common_chain_corruption(
    tmp_path: Path,
    corruption: str,
    error_match: str,
) -> None:
    connector = refinement_support._generated_catalog_database(
        tmp_path / f"chain-{corruption}.sqlite3"
    )
    try:
        analysis_id = refinement_support._insert_active_source_head(connector)
        refinement_support._insert_active_publication(connector, analysis_id)
        connector.execute("PRAGMA foreign_keys = OFF")
        if corruption == "unsealed-anchor":
            connector.execute(
                "INSERT INTO catalog_publication_commit_anchors "
                "(receipt_id) VALUES (%s)",
                (b"u" * 16,),
            )
        else:
            connector.execute(
                "DELETE FROM catalog_publication_generation_nodes WHERE generation = 0"
            )
        connector.execute("PRAGMA foreign_keys = ON")

        validator = catalog_refinement.builtin_semantic_validators()[
            "catalog.publication-atomicity.v1"
        ]
        with pytest.raises(
            catalog_refinement.CatalogSemanticValidationError,
            match=error_match,
        ):
            validator(connector)
    finally:
        connector.close()


@pytest.mark.parametrize("with_base", [False, True], ids=["genesis", "successor"])
def test_commit_replay_is_read_only_after_transient_candidate_cleanup(
    tmp_path: Path,
    with_base: bool,
) -> None:
    connector = publication_support._generated_database(
        tmp_path / f"cleanup-replay-{with_base}.sqlite3"
    )
    try:
        gate, turn = publication_support._authorities(connector)
        publication_support._seed_candidate(connector, turn, with_base=with_base)
        committed = publication_support._commit(connector, gate, turn)

        # Candidate cleanup is allowed to remove the transient preparation graph;
        # the sealed commit member and source lineage remain replay authority.
        _delete_transient_candidate_definition(connector)
        before = _application_state(connector)

        replay = publication_support._commit(connector, gate, turn, now=101)

        assert replay.replayed
        assert replay.receipt_id == committed.receipt_id
        assert replay.candidate_id == committed.candidate_id
        assert _application_state(connector) == before
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("corruption", "error_match"),
    (
        ("provenance", "lineage|provenance"),
        ("build-base", "base|predecessor"),
    ),
)
def test_commit_replay_after_candidate_cleanup_rejects_durable_corruption(
    tmp_path: Path,
    corruption: str,
    error_match: str,
) -> None:
    connector = publication_support._generated_database(
        tmp_path / f"cleanup-replay-corrupt-{corruption}.sqlite3"
    )
    try:
        gate, turn = publication_support._authorities(connector)
        publication_support._seed_candidate(connector, turn, with_base=True)
        committed = publication_support._commit(connector, gate, turn)
        _delete_transient_candidate_definition(connector)

        if corruption == "provenance":
            connector.execute(
                "DELETE FROM catalog_source_revision_provenance "
                "WHERE source_revision = %s",
                (committed.source_revision,),
            )
        else:
            connector.execute(
                "DELETE FROM catalog_source_build_base_publication_commits "
                "WHERE build_id = %s",
                (publication_support._BUILD,),
            )
        before = _application_state(connector)

        with pytest.raises(
            publication_module.PublicationCorruptionError,
            match=error_match,
        ):
            publication_support._commit(connector, gate, turn, now=101)

        assert _application_state(connector) == before
        assert connector.fetch_one(
            "SELECT candidate_id FROM catalog_publication_commits "
            "WHERE receipt_id = %s",
            (committed.receipt_id,),
        ) == (committed.candidate_id,)
    finally:
        connector.close()


def test_inactive_commit_replay_preserves_reachable_lineage_during_cleanup(
    tmp_path: Path,
) -> None:
    connector = publication_support._generated_database(
        tmp_path / "inactive-cleanup-replay.sqlite3"
    )
    try:
        gate, turn = publication_support._authorities(connector)
        publication_support._seed_candidate(connector, turn, with_base=True)
        publication_support._commit(connector, gate, turn)

        # Generation one is now inactive. Its source build remains reachable from
        # the retained immutable commit, so child-first cleanup must preserve it
        # until publication-commit cleanup has safely retired that retry authority.
        with connector.transaction():
            MaintenanceGateRepository.release(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate,
                now=101,
            )
        with (
            connector.transaction(),
            patch(
                "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
                return_value=b"e" * 16,
            ),
        ):
            cleanup_gate = MaintenanceGateRepository.claim_exclusive(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=102,
                lease_duration=1_000_000,
            )

        preparation_cycle = cleanup_support._begin(
            connector,
            cleanup_gate,
            CleanupTargetKind.OPERATIONAL_PREPARATION,
            ord("p"),
            max_rows=32,
            now=103,
        )
        cleanup_support._drain(
            connector,
            cleanup_gate,
            preparation_cycle,
            now=104,
        )
        build_cycle = cleanup_support._begin(
            connector,
            cleanup_gate,
            CleanupTargetKind.SOURCE_BUILD,
            ord("z"),
            max_rows=32,
            now=120,
        )
        cleanup_support._drain(
            connector,
            cleanup_gate,
            build_cycle,
            now=121,
        )
        assert connector.fetch_one(
            "SELECT build_id FROM catalog_source_build_descriptor WHERE build_id = %s",
            (b"z" * 16,),
        ) == (b"z" * 16,)

        with connector.transaction():
            MaintenanceGateRepository.release(
                VNextUnitOfWork(connector, backend="sqlite"),
                cleanup_gate,
                now=150,
            )
        with (
            connector.transaction(),
            patch(
                "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
                return_value=b"r" * 16,
            ),
        ):
            replay_gate = MaintenanceGateRepository.claim_shared(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=151,
                lease_duration=1_000_000,
            )

        before = _application_state(connector)
        with connector.transaction():
            replay = publication_module.PublicationRepository.commit(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=replay_gate,
                ingest_turn=turn,
                candidate_id=b"x" * 16,
                now=152,
            )
        assert replay.replayed
        assert replay.receipt_id == b"h" * 16
        assert _application_state(connector) == before

        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "DELETE FROM catalog_publication_commits WHERE receipt_id = %s",
            (replay.receipt_id,),
        )
        connector.execute("PRAGMA foreign_keys = ON")
        corrupted = _application_state(connector)
        with pytest.raises(
            publication_module.PublicationNotReadyError,
            match="candidate is missing",
        ):
            with connector.transaction():
                publication_module.PublicationRepository.commit(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=replay_gate,
                    ingest_turn=turn,
                    candidate_id=replay.candidate_id,
                    now=153,
                )
        assert _application_state(connector) == corrupted
    finally:
        connector.close()


def test_common_head_cas_does_not_compare_commit_time() -> None:
    class ChangedRowRecorder:
        def __init__(self) -> None:
            self.mutations: list[tuple[str, tuple[Any, ...]]] = []

        def execute_affected(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> int:
            self.mutations.append((query, data))
            return 1

    connector = ChangedRowRecorder()
    work = VNextUnitOfWork(cast(Any, connector), backend="mariadb")
    base = publication_module._LockedHead(b"b" * 16, 1, 1, 1, 100)

    publication_module._advance_publication_commit_head(
        work,
        channel=b"default",
        base=base,
        receipt_id=b"n" * 16,
    )

    assert connector.mutations == [
        (
            "UPDATE catalog_publication_commit_head_receipts SET receipt_id = %s "
            "WHERE channel = %s AND receipt_id = %s",
            (b"n" * 16, b"default", b"b" * 16),
        )
    ]
    assert "committed_at" not in connector.mutations[0][0]
