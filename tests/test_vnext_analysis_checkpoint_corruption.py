"""Durable checkpoint decoding rejects corruption through the public facade."""

from __future__ import annotations

from contextlib import closing

import pytest
from test_vnext_source_batches import _source_batch, _source_batch_clock
from test_vnext_source_marker import MarkerSource
from vnext_fault_harness import (
    FaultInjector,
    fault_injection,
    open_connector,
    snapshot_database,
)
from vnext_pipeline import claim_session, gallery, ingest_policy, initialize_database

from h2hdb import CoreConfig, VNextIngestFacade
from h2hdb.vnext_domains import DomainValidationError


@pytest.mark.parametrize("corruption", ("empty_cursor", "live_count_overflow"))
def test_analysis_issue_rejects_corrupt_checkpoint_without_writes(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
) -> None:
    initialize_database(db_config)
    source = MarkerSource((gallery(1001, pages=[]),))
    target_stage = (
        b"changed_gallery"
        if corruption == "empty_cursor"
        else b"validate_file_hash_decision"
    )
    observer = FaultInjector()
    with (
        fault_injection(monkeypatch, observer),
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        receipt, _ = _source_batch(facade, session, policy, source, None)
        assert observer.mutations > 0, "the public facade must use the observer"
        with facade.prepare_analysis(
            receipt.build_id, policy, max_rows=128
        ) as prepared:
            for _ in range(128):
                issued = facade.issue_analysis_step(session, prepared)
                payload = issued._payload
                assert payload is not None
                if payload.stage == target_stage:
                    analysis_id = payload.analysis_id
                    break
                local = facade.prepare_analysis_step(prepared, issued)
                facade.commit_analysis_step(session, local)
            else:
                pytest.fail("public analysis never reached the target checkpoint")

        with closing(open_connector(db_config)) as writer, writer.transaction():
            (original_cursor,) = writer.fetch_one(
                "SELECT `cursor` FROM catalog_analysis_checkpoints "
                "WHERE analysis_id = %s AND stage = %s AND state = %s",
                (analysis_id, target_stage, "OPEN"),
            )
            assert isinstance(original_cursor, bytes)
            if corruption == "empty_cursor":
                corrupted = b""
            else:
                assert len(original_cursor) == 43
                corrupted = original_cursor[:-8] + (1 << 63).to_bytes(8, "big")
            # Both values satisfy the physical byte-domain bound. Their
            # stage-specific semantics must be rejected by the runtime reader.
            assert (
                writer.execute_affected(
                    "UPDATE catalog_analysis_checkpoints SET `cursor` = %s "
                    "WHERE analysis_id = %s AND stage = %s",
                    (corrupted, analysis_id, target_stage),
                )
                == 1
            )

        before = snapshot_database(db_config)
        mutations_before = observer.mutations
        with facade.prepare_analysis(receipt.build_id, policy, max_rows=128) as resumed:
            with pytest.raises(DomainValidationError) as rejected:
                facade.issue_analysis_step(session, resumed)
            assert type(rejected.value) is DomainValidationError
            assert observer.mutations == mutations_before
            assert snapshot_database(db_config) == before

            # Restore only the deliberately corrupted test value. The same
            # facade and prepared handle must still issue the durable stage.
            with closing(open_connector(db_config)) as writer, writer.transaction():
                assert (
                    writer.execute_affected(
                        "UPDATE catalog_analysis_checkpoints SET `cursor` = %s "
                        "WHERE analysis_id = %s AND stage = %s",
                        (original_cursor, analysis_id, target_stage),
                    )
                    == 1
                )
            recovered = facade.issue_analysis_step(session, resumed)
            assert recovered._payload is not None
            assert recovered._payload.analysis_id == analysis_id
            assert recovered._payload.stage == target_stage
