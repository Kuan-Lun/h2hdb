"""Fenced absence reuse, transaction ownership, and invalidation regressions."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Callable
from contextlib import closing
from hashlib import sha256
from types import SimpleNamespace
from typing import Any, cast

import pytest
from vnext_canonical_value_fixtures import seed_canonical_allocation
from vnext_pipeline import (
    MemoryLibrary,
    MemorySource,
    drain_maintenance,
    gallery,
    initialize_database,
    run_ingest_turn,
)
from vnext_test_database import connector_backend, open_generated_database

import h2hdb._cleanup.model as cleanup_model
import h2hdb._cleanup.registry as cleanup_registry
import h2hdb._cleanup.targets.resources as cleanup_resources
from h2hdb import CoreConfig, VNextIngestFacade
from h2hdb._cleanup.eligibility import CurrentOnlyEligibilityProof
from h2hdb._cleanup.selection import CleanupSelectionRepository
from h2hdb.domain import CurrentOnlyCleanupTerminalState
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
    MaintenanceGateUnavailableError,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


def _work(connector: SQLConnector) -> VNextUnitOfWork:
    return VNextUnitOfWork(connector, backend=connector_backend(connector))


def _claim(connector: SQLConnector, *, now: int = 1) -> GateLease:
    with connector.transaction():
        return MaintenanceGateRepository.claim_exclusive(
            _work(connector), now=now, lease_duration=100_000
        )


def _select(
    connector: SQLConnector,
    lease: GateLease,
    proof: CurrentOnlyEligibilityProof | None = None,
    *,
    now: int = 2,
    cutoff: int = 100,
) -> cleanup_model.CurrentOnlyCleanupSelection:
    with connector.transaction():
        selected = CleanupSelectionRepository.next_current_only_cycle(
            _work(connector),
            gate_lease=lease,
            cycle_cutoff_at=cutoff,
            now=now,
            eligibility_proof=proof,
        )
    return selected


def _count_probes(monkeypatch: pytest.MonkeyPatch) -> Counter[str]:
    counts: Counter[str] = Counter()

    def counter(
        name: str, original: Callable[[VNextUnitOfWork], int | None]
    ) -> Callable[[VNextUnitOfWork], int | None]:
        def counted(work: VNextUnitOfWork) -> int | None:
            counts[name] += 1
            return original(work)

        return counted

    for name in (
        "_next_content_blob_candidate_shard",
        "_next_file_name_candidate_shard",
    ):
        monkeypatch.setattr(
            cleanup_resources, name, counter(name, getattr(cleanup_resources, name))
        )
    return counts


@pytest.mark.cleanup_acceptance
def test_absence_reuse_requires_fresh_exact_ownership(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    with closing(open_generated_database(db_config)) as connector:
        lease = _claim(connector)
        counts = _count_probes(monkeypatch)
        first = _select(connector, lease)
        assert first.cycle is CurrentOnlyCleanupTerminalState.DONE
        proof = first.eligibility_proof
        assert proof.absent_targets == {"CONTENT_BLOB", "FILE_NAME_IDENTITY"}
        with connector.transaction():
            renewed = MaintenanceGateRepository.renew(
                _work(connector), lease, now=10, lease_duration=200_000
            )
        second = _select(connector, renewed, proof, now=11)
        assert second.cycle is CurrentOnlyCleanupTerminalState.DONE
        assert second.eligibility_proof is proof
        assert list(counts.values()) == [1, 1]
        # A new attempt or cutoff never inherits an old absence fact.
        _select(connector, renewed, now=12)
        _select(connector, renewed, proof, now=12, cutoff=101)
        assert list(counts.values()) == [3, 3]
        with connector.transaction(), pytest.raises(TypeError, match="freshly fenced"):
            CleanupSelectionRepository.current_only_maintenance_state(
                _work(connector), cycle_cutoff_at=100, eligibility_proof=proof
            )
        with pytest.raises(MaintenanceGateUnavailableError):
            with connector.transaction():
                MaintenanceGateRepository.claim_shared(
                    _work(connector), now=12, lease_duration=100
                )
        with pytest.raises(MaintenanceGateUnavailableError):
            _select(connector, renewed, proof, now=renewed.lease_expires_at)
        replacement = _claim(connector, now=renewed.lease_expires_at)
        with pytest.raises(MaintenanceGateUnavailableError):
            _select(connector, renewed, proof, now=replacement.lease_expires_at - 1)
        _select(
            connector,
            replacement,
            proof,
            now=renewed.lease_expires_at + 1,
        )
        assert list(counts.values()) == [4, 4]


@pytest.mark.cleanup_acceptance
def test_reference_removal_invalidates_absence_before_next_priority_scan(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    initialize_database(db_config)
    source = MemorySource([gallery(1)])
    with VNextIngestFacade(db_config) as facade:
        run_ingest_turn(facade, source=source, library=MemoryLibrary(source))
        drain_maintenance(facade)
    with closing(open_generated_database(db_config)) as connector:
        lease = _claim(connector, now=10**18)
        key = sha256(b"additional-retained-file").digest()
        with connector.transaction():
            owner = connector.fetch_one(
                "SELECT gallery_id, observation_id "
                "FROM catalog_gallery_observation_allocations "
                "ORDER BY gallery_id, observation_id LIMIT 1"
            )
            connector.execute(
                "INSERT INTO catalog_file_name_identities "
                "(file_key, name_bytes) VALUES (%s, %s)",
                (key, b"additional-retained-file"),
            )
            connector.execute(
                "INSERT INTO catalog_content_blobs "
                "(file_sha256, size_bytes) VALUES (%s, %s)",
                (key, 1),
            )
            connector.execute(
                "INSERT INTO catalog_gallery_observation_file_anchors "
                "(gallery_id, observation_id, file_key) VALUES (%s, %s, %s)",
                (*owner, key),
            )
            connector.execute(
                "INSERT INTO catalog_gallery_observation_file_file_sha256s "
                "(gallery_id, observation_id, file_key, file_sha256) "
                "VALUES (%s, %s, %s, %s)",
                (*owner, key, key),
            )
        counts = _count_probes(monkeypatch)
        first = _select(connector, lease, now=10**18 + 1)
        assert first.cycle is CurrentOnlyCleanupTerminalState.DONE
        proof = first.eligibility_proof
        preserved = proof.after_committed_cleanup("CANONICAL_VALUE")
        assert preserved is proof
        assert _select(connector, lease, preserved, now=10**18 + 2).cycle is (
            CurrentOnlyCleanupTerminalState.DONE
        )
        assert list(counts.values()) == [1, 1]
        # Synthetic child-first removal models the exact references removed by
        # observation cleanup, without changing the published parent's metadata.
        with connector.transaction():
            connector.execute(
                "DELETE FROM catalog_gallery_observation_file_file_sha256s "
                "WHERE file_key = %s",
                (key,),
            )
            connector.execute(
                "DELETE FROM catalog_gallery_observation_file_anchors "
                "WHERE file_key = %s",
                (key,),
            )
        invalidated = proof.after_committed_cleanup("GALLERY_OBSERVATION")
        assert not invalidated.absent_targets
        selected = _select(connector, lease, invalidated, now=10**18 + 3)
        assert isinstance(selected.cycle, cleanup_model.CleanupCycle)
        assert (
            selected.cycle.target_kind
            is cleanup_model.CleanupTargetKind.FILE_NAME_IDENTITY
        )
        assert counts["_next_file_name_candidate_shard"] == 2
        # The immutable prior receipt is unchanged, not silently mutated.
        assert proof.absent_targets == {"CONTENT_BLOB", "FILE_NAME_IDENTITY"}


class _CommitFault(RuntimeError):
    pass


@pytest.mark.cleanup_acceptance
@pytest.mark.parametrize("after_commit", [False, True])
def test_facade_drops_selection_evidence_on_rollback_or_response_loss(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, *, after_commit: bool
) -> None:
    with closing(open_generated_database(db_config)) as connector:
        seed_canonical_allocation(
            connector,
            value_sha256=b"v" * 32,
            digest_domain=b"source_root_v1",
            byte_count=1,
            allocated_at=1,
        )
        connector_type = type(connector)
    pending = False
    injected = False
    incoming: list[CurrentOnlyEligibilityProof | None] = []
    original_select = CleanupSelectionRepository.next_current_only_cycle
    original_commit = connector_type.commit

    def select(*args: Any, **kwargs: Any) -> cleanup_model.CurrentOnlyCleanupSelection:
        nonlocal pending
        incoming.append(kwargs.get("eligibility_proof"))
        selected = original_select(*args, **kwargs)
        if not injected:
            pending = True
        return selected

    def commit(connector: SQLConnector) -> None:
        nonlocal pending, injected
        if pending:
            pending = False
            injected = True
            if after_commit:
                original_commit(connector)
            else:
                connector.rollback()
            raise _CommitFault("selection commit response failed")
        original_commit(connector)

    monkeypatch.setattr(
        CleanupSelectionRepository, "next_current_only_cycle", staticmethod(select)
    )
    monkeypatch.setattr(connector_type, "commit", commit)
    with VNextIngestFacade(db_config) as facade:
        with pytest.raises(_CommitFault, match="selection commit"):
            facade.drain_current_only_maintenance(100_000_000)
        assert incoming == [None]
        drain_maintenance(facade)
    assert incoming[1] is None
    assert injected


def test_canonical_mutation_footprint_preserves_exact_probe_relations() -> None:
    """A future new canonical mutation must not silently invalidate this proof."""

    plan = cleanup_registry._STATIC_PLANS[
        cleanup_model.CleanupTargetKind.CANONICAL_VALUE
    ]
    mutations = {
        table
        for specs in plan.phases.values()
        for spec in specs
        for statement in spec.delete_sql
        for table in re.findall(r"DELETE FROM (\w+)", statement)
    }

    class Capture:
        query = ""

        def fetch_one(self, query: str) -> None:
            self.query = query

    for probe in (
        cleanup_resources._next_file_name_candidate_shard,
        cleanup_resources._next_content_blob_candidate_shard,
    ):
        capture = Capture()
        probe(cast(Any, SimpleNamespace(connector=capture)))
        references = set(re.findall(r"(?:FROM|JOIN) (\w+)", capture.query))
        assert references
        assert not mutations & references
