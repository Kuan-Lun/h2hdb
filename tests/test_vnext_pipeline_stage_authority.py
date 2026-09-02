"""Stored-authority matrices over the fifteen analysis stages and the
adapter-issued file roles, driven through the public facades.

For every analysis stage its second page batch (a non-initial checkpoint
cursor, so the checkpoint CAS and receipt chain are both live) is interrupted
twice:

* a statement fault before its first mutation must roll the transaction back
  exactly, after which the same prepared step commits normally; and
* a lost commit response must leave the stored batch receipt with the exact
  ``page_limit`` it was issued with; the same prepared step then refuses a
  corrupted stored limit closed and, once the limit is restored, replays the
  stored batch with zero DML.

A source adapter that issues a forged artifact role (METADATA for a page
name, PAGE/OTHER for ``galleryinfo.txt``) is stored as the adapter's own
immutable observation fact (the manifest keeps the adapter role separate from
the protocol file_role classifier) and the publication then fails closed at
artifact planning: no prepared artifact, publication commit or library
activation exists, and the retry fails identically.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from vnext_fault_harness import (
    FaultInjector,
    InjectedFault,
    fault_injection,
    open_connector,
    snapshot_database,
    snapshot_difference,
)
from vnext_pipeline import (
    Clock,
    MemoryLibrary,
    MemorySource,
    claim_session,
    gallery,
    ingest_policy,
    initialize_database,
    run_ingest_turn,
    run_source,
)

from h2hdb import (
    ArtifactSourceRole,
    CoreConfig,
    DatabaseConfig,
    FileObservation,
    VNextIngestFacade,
    VNextIngestGalleryObservation,
    VNextIngestPage,
    VNextIngestSession,
    VNextPreparedAnalysis,
)
from h2hdb.vnext_analysis_repository import (
    AnalysisCorruptionError,
    AnalysisNotReadyError,
)
from h2hdb.vnext_artifact_preparation_repository import (
    ArtifactPreparationConflictError,
)

ANALYSIS_STAGES = (
    "changed_gallery",
    "changed_file_hash",
    "file_hash_decision",
    "validate_file_hash_decision",
    "impacted_gallery",
    "impacted_content",
    "content_owner_candidate",
    "validate_content_owner_candidate",
    "content_owner",
    "validate_content_owner",
    "impacted_gid",
    "gid_candidate",
    "validate_gid_candidate",
    "gid_winner",
    "validate_gid_winner",
)


def _config(path: Path) -> CoreConfig:
    return CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))


def _corpus() -> MemorySource:
    return MemorySource(
        [
            gallery(1001, pages=[b"p0-a", b"p1-a"], artists=["alice"]),
            gallery(1002, pages=[b"p0-b", b"p1-b"], artists=["bob"]),
            gallery(1003, pages=[b"p0-a", b"p1-c"], artists=["alice", "carol"]),
        ]
    )


# Stages that prepare per-gallery payloads in this corpus finish in one page
# batch; every other stage pages through several one-row batches, so the
# second batch (a non-initial checkpoint cursor) is interrupted there.
PREPARATION_STAGES = frozenset(
    {
        "impacted_content",
        "content_owner_candidate",
        "validate_content_owner_candidate",
        "gid_candidate",
        "validate_gid_candidate",
    }
)


class _AtStageBatch:
    """Run ``arm`` right before one page-batch commit of one stage.

    Preparation stages also commit canonical uploads under the stage label;
    only the ``PROCESS_BATCH`` action writes the batch receipt and checkpoint,
    so the hook counts that action alone."""

    def __init__(self, stage: str, arm: Any) -> None:
        self.stage = stage
        self.arm = arm
        self.target = 1 if stage in PREPARATION_STAGES else 2
        self.seen = 0
        self.armed = False

    def __call__(self, label: str, action: str) -> None:
        if (
            not self.armed
            and label == f"analysis.commit:{self.stage}"
            and action == "PROCESS_BATCH"
        ):
            self.seen += 1
            if self.seen == self.target:
                self.armed = True
                self.arm()


def _step(
    facade: VNextIngestFacade,
    session: VNextIngestSession,
    prepared: VNextPreparedAnalysis,
) -> Any:
    issued = facade.issue_analysis_step(session, prepared)
    local = facade.prepare_analysis_step(prepared, issued)
    return facade.commit_analysis_step(session, local)


def _run_until_fault(
    facade: VNextIngestFacade,
    session: VNextIngestSession,
    prepared: VNextPreparedAnalysis,
    boundary: _AtStageBatch,
) -> None:
    for _ in range(10_000):
        issued = facade.issue_analysis_step(session, prepared)
        local = facade.prepare_analysis_step(prepared, issued)
        payload = issued._payload
        stage = (
            payload.stage.decode("ascii")
            if payload is not None and payload.stage is not None
            else "none"
        )
        boundary(f"analysis.commit:{stage}", str(local._action))
        try:
            result = facade.commit_analysis_step(session, local)
        except InjectedFault:
            return
        assert not result.terminal, f"analysis finished before {boundary.stage!r}"
    raise AssertionError("analysis synchronization exceeded its step budget")


def _stored_receipts(config: CoreConfig) -> list[tuple[bytes, int]]:
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            return [
                (bytes(row[0]), int(row[1]))
                for row in connector.fetch_all(
                    "SELECT stage, page_limit FROM catalog_analysis_batch_receipt_stored "
                    "ORDER BY stage"
                )
            ]
    finally:
        connector.close()


def _set_stored_limit(config: CoreConfig, stage: str, page_limit: int) -> None:
    connector = open_connector(config)
    try:
        with connector.transaction():
            assert (
                connector.execute_affected(
                    "UPDATE catalog_analysis_batch_receipt_stored SET page_limit = %s "
                    "WHERE stage = %s",
                    (page_limit, stage.encode("ascii")),
                )
                == 1
            )
    finally:
        connector.close()


@pytest.mark.parametrize("stage", ANALYSIS_STAGES)
def test_every_analysis_stage_batch_rolls_back_exactly_and_then_commits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    config = _config(tmp_path / f"rollback-{stage}.sqlite3")
    initialize_database(config)
    injector = FaultInjector()
    snapshots: list[dict[str, Any]] = []

    def arm() -> None:
        snapshots.append(snapshot_database(config))
        injector.fail_before_mutation = injector.mutations + 1

    hook = _AtStageBatch(stage, arm)
    # The facade opens its connector lazily, so the injecting connector class
    # must be installed before the facade is built; the injector stays inert
    # until the stage boundary arms it.
    with fault_injection(monkeypatch, injector):
        facade = VNextIngestFacade(config, clock=Clock())
        try:
            session = claim_session(facade)
            policy = facade.ensure_policy(session, ingest_policy())
            source_receipt = run_source(facade, session, policy, _corpus())
            prepared = facade.prepare_analysis(
                source_receipt.build_id, policy, max_rows=1
            )
            with prepared:
                _run_until_fault(facade, session, prepared, hook)
                assert hook.armed and injector.fired == "before_mutation"
                (before,) = snapshots
                assert snapshot_difference(before, snapshot_database(config)) == {}
                injector.fired = None
                injector.fail_before_mutation = None
                result = _step(facade, session, prepared)
            assert result.stage == stage.encode("ascii") and not result.replayed
            assert (stage.encode("ascii"), 1) in _stored_receipts(config)
        finally:
            facade.close()


@pytest.mark.parametrize("stage", ANALYSIS_STAGES)
def test_every_analysis_stage_replays_its_stored_page_limit_and_rejects_a_corrupted_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    config = _config(tmp_path / f"stage-{stage}.sqlite3")
    initialize_database(config)
    injector = FaultInjector()

    def arm() -> None:
        injector.fail_after_commit = injector.commits + 1

    hook = _AtStageBatch(stage, arm)
    with fault_injection(monkeypatch, injector):
        facade = VNextIngestFacade(config, clock=Clock())
        try:
            session = claim_session(facade)
            policy = facade.ensure_policy(session, ingest_policy())
            source_receipt = run_source(facade, session, policy, _corpus())
            prepared = facade.prepare_analysis(
                source_receipt.build_id, policy, max_rows=1
            )
            with prepared:
                _run_until_fault(facade, session, prepared, hook)
                assert hook.armed and injector.fired == "after_commit"
                assert (stage.encode("ascii"), 1) in _stored_receipts(config)
                committed = snapshot_database(config)
                injector.fired = None
                injector.fail_after_commit = None

                # A corrupted stored limit is rejected closed by the exact
                # retry of the same prepared step, with zero DML.
                _set_stored_limit(config, stage, 2)
                corrupted = snapshot_database(config)
                mutations = injector.mutations
                with pytest.raises((AnalysisNotReadyError, AnalysisCorruptionError)):
                    _step(facade, session, prepared)
                assert injector.mutations == mutations
                assert snapshot_difference(corrupted, snapshot_database(config)) == {}

                # With the exact stored limit the same step replays durably.
                _set_stored_limit(config, stage, 1)
                mutations = injector.mutations
                result = _step(facade, session, prepared)
                assert result.replayed and result.stage == stage.encode("ascii")
                assert injector.mutations == mutations
                assert snapshot_difference(committed, snapshot_database(config)) == {}
        finally:
            facade.close()


class _ForgedRoleSource(MemorySource):
    """Issue one forged artifact role for one exact file name."""

    def __init__(
        self, galleries: Any, *, forged_name: bytes, role: ArtifactSourceRole
    ) -> None:
        super().__init__(galleries)
        self.forged_name = forged_name
        self.role = role

    def list_file_observations(
        self,
        observation: VNextIngestGalleryObservation,
        *,
        after_name_bytes: bytes | None,
        limit: int,
    ) -> VNextIngestPage[FileObservation]:
        page = super().list_file_observations(
            observation, after_name_bytes=after_name_bytes, limit=limit
        )
        items = tuple(
            FileObservation(
                name_bytes=item.name_bytes,
                content=item.content,
                artifact_role=self.role
                if item.name_bytes == self.forged_name
                else item.artifact_role,
                device=item.device,
                inode=item.inode,
                modified_ns=item.modified_ns,
                changed_ns=item.changed_ns,
            )
            for item in page.items
        )
        return VNextIngestPage(items, page.next_after, page.terminal)


def _publication_effects(config: CoreConfig) -> dict[str, int]:
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            return {
                table: int(connector.fetch_one(f"SELECT COUNT(*) FROM {table}")[0])
                for table in (
                    "catalog_prepared_artifacts",
                    "catalog_publication_commits",
                    "catalog_publication_receipts",
                    "operational_publication_candidate_preparations",
                )
            }
    finally:
        connector.close()


@pytest.mark.parametrize(
    ("forged_name", "role"),
    [
        (b"000.png", ArtifactSourceRole.METADATA),
        (b"galleryinfo.txt", ArtifactSourceRole.PAGE),
        (b"galleryinfo.txt", ArtifactSourceRole.OTHER),
    ],
)
def test_forged_file_role_fails_closed_at_artifact_planning_and_stays_closed(
    tmp_path: Path,
    forged_name: bytes,
    role: ArtifactSourceRole,
) -> None:
    config = _config(tmp_path / "forged-role.sqlite3")
    initialize_database(config)
    source = _ForgedRoleSource(
        [gallery(7001, pages=[b"p0"], artists=["forger"])],
        forged_name=forged_name,
        role=role,
    )
    library = MemoryLibrary(source)
    facade = VNextIngestFacade(config, clock=Clock())
    try:
        session = claim_session(facade)
        with pytest.raises(ArtifactPreparationConflictError) as first:
            run_ingest_turn(facade, source=source, library=library, session=session)
        assert _publication_effects(config) == {
            "catalog_prepared_artifacts": 0,
            "catalog_publication_commits": 0,
            "catalog_publication_receipts": 0,
            "operational_publication_candidate_preparations": 0,
        }
        assert library.current == {} and library.staging == {}
        closed = snapshot_database(config)
        # The same durable state refuses identically and writes nothing.
        with pytest.raises(ArtifactPreparationConflictError) as again:
            run_ingest_turn(facade, source=source, library=library, session=session)
        assert str(again.value) == str(first.value)
        assert snapshot_difference(closed, snapshot_database(config)) == {}
    finally:
        facade.close()
