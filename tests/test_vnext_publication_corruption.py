"""Public publication preparation refuses committed canonical corruption."""

from __future__ import annotations

from contextlib import closing

import pytest
from vnext_fault_harness import (
    FaultInjector,
    fault_injection,
    open_connector,
    snapshot_database,
)
from vnext_pipeline import (
    Clock,
    MemoryLibrary,
    MemorySource,
    claim_session,
    drain_maintenance,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    run_analysis,
    run_publication,
    run_source,
)
from vnext_test_database import set_check_constraints, set_foreign_key_checks

from h2hdb import CoreConfig, VNextIngestFacade
from h2hdb.vnext_canonical_value_repository import CanonicalValueCollisionError
from h2hdb.vnext_ingest_publication import _CanonicalBatchWork
from h2hdb.vnext_publication_candidate_repository import (
    PublicationCandidateConflictError,
)


@pytest.mark.mariadb_smoke
@pytest.mark.parametrize("corruption", ("payload-swap", "partial-payload"))
def test_publication_prepare_rejects_canonical_corruption_without_writes(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
) -> None:
    initialize_database(db_config)
    source = MemorySource((gallery(1001, pages=[b"one synthetic page"]),))
    library = MemoryLibrary(source)
    adapters = {library.adapter_id: library}
    observer = FaultInjector()
    with (
        fault_injection(monkeypatch, observer),
        VNextIngestFacade(db_config, clock=Clock()) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        receipt = run_source(facade, session, policy, source)
        run_analysis(facade, session, policy, receipt.build_id)
        observed: list[tuple[str, str, int]] = []
        for _ in range(128):
            issued = facade.issue_publication_step(session, policy)
            with facade.prepare_publication_step(
                issued,
                artifact_adapters=adapters,
                finalization_adapters=adapters,
                library_activation=library,
            ) as prepared:
                payload = prepared._payload
                pages = (
                    tuple(item.page for item in payload.items if item.page is not None)
                    if isinstance(payload, _CanonicalBatchWork)
                    else ()
                )
                observed.append((issued.operation, prepared._action.value, len(pages)))
                facade.commit_publication_step(session, prepared)
            if issued.operation == "BUILD_CATALOG" and len(pages) >= 2:
                break
        else:
            pytest.fail(f"fixture did not commit a catalog canonical batch: {observed}")

        first, second = pages[:2]
        assert first.page_bytes != second.page_bytes
        # Facades capture connector factories at construction. Installing the
        # observer first and seeing these real public writes proves it is live.
        assert observer.mutations > 0
        issued = facade.issue_publication_step(session, policy)
        assert issued.operation == "BUILD_CATALOG"
        with closing(open_connector(db_config)) as writer:
            # Model committed storage corruption, including a missing FK child.
            set_foreign_key_checks(writer, enabled=False)
            try:
                with writer.transaction():
                    if corruption == "payload-swap":
                        writer.execute(
                            "UPDATE catalog_canonical_value_page_payloads SET page_bytes = %s "
                            "WHERE page_sha256 = %s",
                            (second.page_bytes, first.page_sha256),
                        )
                    else:
                        writer.execute(
                            "DELETE FROM catalog_canonical_value_page_payloads "
                            "WHERE page_sha256 = %s",
                            (first.page_sha256,),
                        )
            finally:
                set_foreign_key_checks(writer, enabled=True)
        # This tiny fixture permits exact rows, including operational authority.
        before = snapshot_database(db_config)
        prior_mutations = observer.mutations
        with pytest.raises(PublicationCandidateConflictError) as rejected:
            with facade.prepare_publication_step(
                issued,
                artifact_adapters=adapters,
                finalization_adapters=adapters,
                library_activation=library,
            ):
                pytest.fail("committed canonical corruption must refuse prepare")
        assert isinstance(rejected.value.__cause__, CanonicalValueCollisionError)
        assert observer.mutations == prior_mutations
        assert snapshot_database(db_config) == before

        with closing(open_connector(db_config)) as writer, writer.transaction():
            if corruption == "partial-payload":
                writer.execute(
                    "INSERT INTO catalog_canonical_value_page_payloads "
                    "(page_sha256, page_bytes) VALUES (%s, %s)",
                    (first.page_sha256, first.page_bytes),
                )
            else:
                writer.execute(
                    "UPDATE catalog_canonical_value_page_payloads SET page_bytes = %s "
                    "WHERE page_sha256 = %s",
                    (first.page_bytes, first.page_sha256),
                )
        run_publication(facade, session, policy, library)
        facade.complete_ingest(session)
        drain_maintenance(facade)
    assert full_check(db_config).state == "READY"


@pytest.mark.mariadb_smoke
@pytest.mark.parametrize(
    ("corruption", "message"),
    (
        ("missing-stage", "checkpoint registry is incomplete or reordered"),
        ("non-prefix", "checkpoints are not prefix-complete"),
        ("invalid-state", "checkpoint has an invalid state"),
    ),
)
def test_publication_issue_rejects_checkpoint_corruption_without_writes(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
    message: str,
) -> None:
    initialize_database(db_config)
    source = MemorySource((gallery(1001, pages=[b"one synthetic page"]),))
    library = MemoryLibrary(source)
    adapters = {library.adapter_id: library}
    observer = FaultInjector()
    with (
        fault_injection(monkeypatch, observer),
        VNextIngestFacade(db_config, clock=Clock()) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        receipt = run_source(facade, session, policy, source)
        run_analysis(facade, session, policy, receipt.build_id)
        issued = facade.issue_publication_step(session, policy)
        assert issued.operation == "BEGIN"
        with facade.prepare_publication_step(
            issued,
            artifact_adapters=adapters,
            finalization_adapters=adapters,
            library_activation=library,
        ) as prepared:
            facade.commit_publication_step(session, prepared)
        assert observer.mutations > 0
        stage = (
            b"VALIDATE_DUPLICATE_LOSER"
            if corruption == "non-prefix"
            else b"BUILD_SELECTION"
        )
        with closing(open_connector(db_config)) as writer:
            with writer.read_transaction():
                original = writer.fetch_one(
                    "SELECT candidate_id, stage, generation, `cursor`, processed_count, "
                    "state, updated_at FROM catalog_publication_checkpoints WHERE stage = %s",
                    (stage,),
                )
            assert len(original) == 7 and original[5] == "OPEN"
            # Missing registry members and a non-prefix COMPLETE stage are
            # legal physical rows. Only the invalid enum needs CHECK bypass.
            if corruption == "invalid-state":
                set_check_constraints(writer, enabled=False)
            try:
                with writer.transaction():
                    if corruption == "missing-stage":
                        writer.execute(
                            "DELETE FROM catalog_publication_checkpoints "
                            "WHERE candidate_id = %s AND stage = %s",
                            original[:2],
                        )
                    else:
                        writer.execute(
                            "UPDATE catalog_publication_checkpoints SET state = %s "
                            "WHERE candidate_id = %s AND stage = %s",
                            (
                                "CORRUPT"
                                if corruption == "invalid-state"
                                else "COMPLETE",
                                *original[:2],
                            ),
                        )
            finally:
                if corruption == "invalid-state":
                    set_check_constraints(writer, enabled=True)
        before = snapshot_database(db_config)
        prior_mutations = observer.mutations
        with pytest.raises(PublicationCandidateConflictError, match=message):
            facade.issue_publication_step(session, policy)
        assert observer.mutations == prior_mutations
        assert snapshot_database(db_config) == before
        with closing(open_connector(db_config)) as writer, writer.transaction():
            if corruption == "missing-stage":
                writer.execute(
                    "INSERT INTO catalog_publication_checkpoints "
                    "(candidate_id, stage, generation, `cursor`, processed_count, state, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    original,
                )
            else:
                writer.execute(
                    "UPDATE catalog_publication_checkpoints SET state = %s "
                    "WHERE candidate_id = %s AND stage = %s",
                    (original[5], *original[:2]),
                )
        run_publication(facade, session, policy, library)
        facade.complete_ingest(session)
        drain_maintenance(facade)
    assert full_check(db_config).state == "READY"
