"""Production source-to-publication workflows on generated SQLite and live
MariaDB 10.11.11 through the public facades only.

Every scenario ends with the complete production READY audit, so each run is
also live evidence that every wheel validator accepts the state the production
writers produced on that backend.
"""

from __future__ import annotations

import copy
import shutil
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from test_vnext_pipeline_takeover_matrix import FENCE_ERRORS
from vnext_fault_harness import (
    open_connector,
    row_counts,
    snapshot_database,
    snapshot_difference,
)
from vnext_pipeline import (
    LEASE_MICROSECONDS,
    Clock,
    IngestTurnReceipts,
    MemoryLibrary,
    MemorySource,
    catalog_view,
    claim_session,
    drain_maintenance,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
    library_view,
    run_ingest_turn,
    run_publication_recovery,
    stored_objects,
    takeover_clock,
)

from h2hdb import (
    CatalogFacetKind,
    CatalogRecentOrder,
    CatalogResourceKind,
    CatalogRevisionNotFoundError,
    CoreConfig,
    DatabaseConfig,
    VNextCatalogFacade,
    VNextCurrentOnlyMaintenanceOutcome,
    VNextDownloadQueueFacade,
    VNextIngestFacade,
    VNextIngestPolicy,
)
from h2hdb import (
    catalog_refinement as catalog_refinement_module,
)
from h2hdb import (
    vnext_cleanup_repository as cleanup_module,
)
from h2hdb import (
    vnext_publication_repository as publication_module,
)
from h2hdb.catalog_refinement import CatalogSemanticValidationError
from h2hdb.vnext_cleanup_repository import (
    CleanupBatchCommand,
    CleanupTargetKind,
    VNextCleanupRepository,
)
from h2hdb.vnext_maintenance_gate_repository import MaintenanceGateRepository
from h2hdb.vnext_operational_event_repository import OperationalEffectStateError
from h2hdb.vnext_publication_repository import PublicationHeadRaceError
from h2hdb.vnext_transaction import VNextUnitOfWork


@dataclass
class Pipeline:
    config: CoreConfig
    source: MemorySource
    library: MemoryLibrary

    def turn(
        self,
        *,
        clock: Clock | None = None,
        periodic: bool = True,
        drain: bool = True,
        policy: VNextIngestPolicy | None = None,
    ) -> tuple[IngestTurnReceipts, int]:
        facade = VNextIngestFacade(self.config, clock=clock or Clock())
        try:
            receipts = run_ingest_turn(
                facade,
                source=self.source,
                library=self.library,
                policy=policy,
                periodic=periodic,
            )
            progressed = drain_maintenance(facade) if drain else 0
        finally:
            facade.close()
        return receipts, progressed

    def view(self) -> dict[str, Any]:
        return catalog_view(self.config)

    def ready(self) -> None:
        assert full_check(self.config).state == "READY"


def _corpus() -> list[Any]:
    return [
        gallery(
            1001,
            pages=[b"p0-a", b"p1-a"],
            artists=["alice"],
            extra_tags=[("female", "glasses")],
        ),
        gallery(1002, pages=[b"p0-b"], artists=["bob"], language="japanese"),
        gallery(
            1003,
            pages=[b"p0-c", b"p1-c", b"p2-c"],
            artists=["alice", "carol"],
            other_files={b"notes.txt": b"not a page"},
            directories=(b"extras",),
        ),
    ]


@pytest.fixture
def pipeline(db_config: CoreConfig) -> Iterator[Pipeline]:
    initialize_database(db_config)
    source = MemorySource(_corpus())
    yield Pipeline(db_config, source, MemoryLibrary(source))


def _publication_gids(view: dict[str, Any]) -> list[int]:
    return sorted(int(item["publication"]["gid"]) for item in view["publications"])


def _artifact_sha(view: dict[str, Any], gid: int) -> str:
    for item in view["publications"]:
        if int(item["publication"]["gid"]) == gid:
            (artifact,) = item["artifacts"]
            return str(artifact["storage_object"]["sha256"])
    raise AssertionError(f"gid {gid} is not published")


@pytest.mark.mariadb_smoke
@pytest.mark.merge_smoke
def test_fresh_turn_publishes_every_gallery_and_passes_full_ready_audit(
    pipeline: Pipeline,
) -> None:
    receipts, progressed = pipeline.turn()

    assert receipts.source.discovered_galleries == 3
    assert receipts.source.staged_galleries == 3
    assert receipts.source.sealed and not receipts.source.replayed
    assert (
        receipts.analysis.terminal and receipts.analysis.stage == b"snapshot_manifest"
    )
    assert receipts.publication.terminal
    assert not receipts.completion.replayed
    assert progressed == 0
    view = pipeline.view()
    assert (view["revision"], view["publication_count"], view["artifact_count"]) == (
        1,
        3,
        3,
    )
    assert _publication_gids(view) == [1001, 1002, 1003]
    facets = view["facets"]
    assert [
        value["value"] for value in facets[CatalogFacetKind.LANGUAGE.value]["values"]
    ] == ["english", "japanese"]
    contributors = {
        (value["role"], value["value"])
        for value in facets[CatalogFacetKind.CONTRIBUTOR.value]["values"]
    }
    assert contributors == {
        ("artist", "alice"),
        ("artist", "bob"),
        ("artist", "carol"),
        ("uploader", "uploader"),
    }
    assert any(
        value["value"] == "glasses"
        for value in facets[CatalogFacetKind.SUBJECT.value]["values"]
    )
    for order in CatalogRecentOrder:
        assert len(view["recent"][order.value]["publications"]) == 3
    for item in view["publications"]:
        presentation = item["presentation"]
        assert presentation["page_count"] == item["publication"]["page_count"] > 0
        assert presentation["cover"] is not None
        assert presentation["thumbnail"] is not None
    # The reader-visible library carries exactly the catalog's resources.
    assert sorted(library_view(pipeline.library)) == [
        f"{gid}:{kind.value}"
        for gid in (1001, 1002, 1003)
        for kind in (CatalogResourceKind.ACQUISITION, CatalogResourceKind.THUMBNAIL)
    ]
    objects = stored_objects(pipeline.library)
    for gid in (1001, 1002, 1003):
        assert objects[f"{gid % 256:02x}/h2h-{gid}/acquisition"][0] == _artifact_sha(
            view, gid
        )
    assert pipeline.library.staging == {}
    assert len(pipeline.library.release_calls) == 6
    pipeline.ready()


def test_unchanged_source_replays_exactly_without_a_new_revision(
    pipeline: Pipeline,
) -> None:
    pipeline.turn()
    first = pipeline.view()
    renders = pipeline.library.render_calls

    receipts, progressed = pipeline.turn()

    assert receipts.source.replayed and receipts.source.sealed
    assert receipts.publication.terminal
    assert progressed == 0
    assert pipeline.library.render_calls == renders
    assert pipeline.view() == first
    assert pipeline.view()["revision"] == 1
    pipeline.ready()


def test_restarted_process_takes_over_expired_leases_and_replays(
    pipeline: Pipeline,
) -> None:
    pipeline.turn()
    first = pipeline.view()

    receipts, _progressed = pipeline.turn(clock=takeover_clock())

    assert receipts.source.replayed
    assert receipts.session.ingest_generation >= 2
    assert pipeline.view() == first
    pipeline.ready()


def test_incremental_turn_rebuilds_adds_removes_and_reconciles_library(
    pipeline: Pipeline,
) -> None:
    pipeline.turn()
    before = pipeline.view()
    old_1001 = _artifact_sha(before, 1001)
    old_1003 = _artifact_sha(before, 1003)

    pipeline.source.put(
        gallery(
            1001,
            pages=[b"p0-a", b"p1-a-modified"],
            artists=["alice"],
            extra_tags=[("female", "glasses")],
        )
    )
    pipeline.source.put(gallery(1004, pages=[b"p0-d"], artists=["dave"]))
    pipeline.source.remove(("gallery-1002",))
    receipts, progressed = pipeline.turn()

    assert not receipts.source.replayed
    view = pipeline.view()
    assert (view["revision"], view["publication_count"], view["artifact_count"]) == (
        2,
        3,
        3,
    )
    assert _publication_gids(view) == [1001, 1003, 1004]
    assert _artifact_sha(view, 1001) != old_1001
    assert _artifact_sha(view, 1003) == old_1003
    assert sorted(library_view(pipeline.library)) == [
        f"{gid}:{kind.value}"
        for gid in (1001, 1003, 1004)
        for kind in (CatalogResourceKind.ACQUISITION, CatalogResourceKind.THUMBNAIL)
    ]
    objects = stored_objects(pipeline.library)
    assert objects[f"{1001 % 256:02x}/h2h-1001/acquisition"][0] == _artifact_sha(
        view, 1001
    )
    assert not any(key.split("/")[1] == "h2h-1002" for key in objects)
    # Historical revision-1 payload is reclaimed by current-only maintenance.
    assert progressed >= 1
    pipeline.ready()
    with pytest.raises(CatalogRevisionNotFoundError):
        VNextCatalogFacade(pipeline.config).get_catalog_revision(1)


def test_removed_gallery_and_deletion_request_are_consumed_as_typed_effects(
    pipeline: Pipeline,
) -> None:
    pipeline.turn()
    queue = VNextDownloadQueueFacade(pipeline.config, clock=Clock())
    receipt = queue.request_deletion(1003, url="https://example.invalid/g/1003")
    assert receipt.created and receipt.current
    states = queue.get_candidate_states([1001, 1002, 1003, 9999])
    assert states[1003].cataloged and not states[9999].cataloged

    pipeline.source.remove(("gallery-1003",))
    receipts, _progressed = pipeline.turn()

    assert not receipts.source.replayed
    view = pipeline.view()
    assert (view["revision"], _publication_gids(view)) == (2, [1001, 1002])
    connector = open_connector(pipeline.config)
    try:
        with connector.read_transaction():
            events = connector.fetch_all(
                "SELECT event_type FROM operational_operational_events "
                "ORDER BY sequence_no"
            )
            removed = connector.fetch_all(
                "SELECT gid FROM operational_operational_removed_gid_events"
            )
            consumed = connector.fetch_all(
                "SELECT deletion_request_token "
                "FROM operational_operational_deletion_consumption_events"
            )
    finally:
        connector.close()
    # A gallery that vanished from the source is dropped by the new revision
    # and is not a queue-recorded removal; the deletion head is consumed once.
    assert [row[0] for row in events] == ["DELETION_CONSUMPTION"]
    assert removed == []
    assert consumed == [(receipt.request_token,)]
    assert not queue.get_candidate_states([1003])[1003].cataloged
    pipeline.ready()


def test_download_handoff_links_ingest_and_completes_the_request(
    pipeline: Pipeline,
) -> None:
    pipeline.turn()
    queue = VNextDownloadQueueFacade(pipeline.config, clock=Clock())
    request = queue.request_download(2001, url="https://example.invalid/g/2001")
    turn = queue.claim_download_turn(lease_duration_microseconds=LEASE_MICROSECONDS)
    pipeline.source.put(gallery(2001, pages=[b"p0-e"], artists=["erin"]))
    handoff = queue.finish_download_turn(turn, request)
    assert not queue.is_download_handoff_complete(handoff)
    assert queue.get_download_request(2001) is None

    receipts, _progressed = pipeline.turn(periodic=False)

    assert receipts.session.download_generation == handoff.download_generation
    assert receipts.session.handoff_kind == "DOWNLOADER"
    assert receipts.completion.download_generation == handoff.download_generation
    assert queue.is_download_handoff_complete(handoff)
    view = pipeline.view()
    assert (view["revision"], _publication_gids(view)) == (2, [1001, 1002, 1003, 2001])
    assert queue.list_pending_redownloads().gids == ()
    pipeline.ready()


def test_shared_content_across_three_artists_is_excluded_as_spam(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    shared = b"identical-page-bytes"
    source = MemorySource(
        [
            gallery(3001, pages=[shared], artists=["one"]),
            gallery(3002, pages=[shared], artists=["two"]),
            gallery(3003, pages=[shared], artists=["three"]),
        ]
    )
    pipeline = Pipeline(db_config, source, MemoryLibrary(source))

    pipeline.turn()

    view = pipeline.view()
    assert (view["revision"], view["publication_count"], view["artifact_count"]) == (
        1,
        0,
        0,
    )
    assert library_view(pipeline.library) == {}
    source.put(gallery(3004, pages=[b"unique"], artists=["four"]))
    pipeline.turn()
    view = pipeline.view()
    assert (view["revision"], _publication_gids(view)) == (2, [3004])
    pipeline.ready()


def test_giant_gallery_pages_through_every_bounded_source_window(
    db_config: CoreConfig,
) -> None:
    initialize_database(db_config)
    pages = [sha256(f"giant-{index}".encode()).digest() for index in range(300)]
    tags = [("character", f"c{index:03d}") for index in range(300)]
    source = MemorySource(
        [
            gallery(4001, pages=pages, artists=["giant"], extra_tags=tags),
            gallery(4002, pages=[b"small"], artists=["giant"]),
        ]
    )
    pipeline = Pipeline(db_config, source, MemoryLibrary(source))

    receipts, _progressed = pipeline.turn()

    assert receipts.source.staged_galleries == 2
    view = pipeline.view()
    assert _publication_gids(view) == [4001, 4002]
    giant = next(
        item for item in view["publications"] if int(item["publication"]["gid"]) == 4001
    )
    assert giant["publication"]["page_count"] == 300
    assert giant["presentation"]["page_count"] == 300
    subjects = view["facets"][CatalogFacetKind.SUBJECT.value]
    assert len(subjects["values"]) == 128 and subjects["next_cursor"] is not None
    counts = row_counts(snapshot_database(pipeline.config))
    assert counts["catalog_gallery_observation_file_seals"] == 303
    pipeline.ready()


class _AtBoundary:
    """Run one action the first time a fenced boundary label is observed."""

    def __init__(self, target: str, action: Callable[[], None]) -> None:
        self.target = target
        self.action = action
        self.fired = False

    def __call__(self, label: str) -> None:
        if not self.fired and label == self.target:
            self.fired = True
            self.action()


OPERATIONAL_RACE_BOUNDARIES = (
    "publication.commit:APPEND_OPERATIONAL",
    "publication.commit:SEAL_OPERATIONAL",
    "publication.commit:VALIDATE_CREATE",
    "publication.commit:COMMIT_PUBLICATION",
)


@pytest.mark.parametrize("boundary", OPERATIONAL_RACE_BOUNDARIES)
def test_deletion_request_during_publication_supersedes_the_stale_preparation(
    pipeline: Pipeline,
    boundary: str,
) -> None:
    """A deletion request that lands after the operational preparation began
    (while its effects are appended or sealed, after the candidate bound it,
    or between the commit issue and the commit itself) must not wedge or leak:
    the turn fails closed with a typed error where the guard fires, the same
    session re-prepares under the advanced generation, the stale attempt is
    ABANDONED so generic cleanup reclaims it, and the commit consumes the new
    generation exactly."""

    queue = VNextDownloadQueueFacade(pipeline.config, clock=Clock())
    receipts: list[Any] = []
    hook = _AtBoundary(
        boundary,
        lambda: receipts.append(queue.request_deletion(1001, url=None)),
    )
    facade = VNextIngestFacade(pipeline.config, clock=Clock())
    try:
        session = claim_session(facade)
        try:
            run_ingest_turn(
                facade,
                source=pipeline.source,
                library=pipeline.library,
                session=session,
                boundary=hook,
            )
        except OperationalEffectStateError, PublicationHeadRaceError:
            run_ingest_turn(
                facade,
                source=pipeline.source,
                library=pipeline.library,
                session=session,
            )
        assert hook.fired
        (receipt,) = receipts
        stale = receipt.observed_generation - 1
        preparations, bound, committed, consumed = _preparation_facts(pipeline.config)
        assert committed == [(receipt.observed_generation,)]
        assert consumed == [(receipt.request_token,)]
        assert bound in ([], [(receipt.observed_generation,)])
        assert (receipt.observed_generation, "COMPLETE") in preparations
        assert {state for generation, state in preparations if generation == stale} <= {
            "ABANDONED"
        }
        drain_maintenance(facade)
    finally:
        facade.close()
    pipeline.ready()
    # Generic cleanup reclaimed every superseded attempt.
    preparations, _bound, _committed, _consumed = _preparation_facts(pipeline.config)
    assert preparations == [(receipt.observed_generation, "COMPLETE")]
    assert pipeline.view()["publication_count"] == len(pipeline.source.galleries)


def _preparation_facts(
    config: CoreConfig,
) -> tuple[list[Any], list[Any], list[Any], list[Any]]:
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            preparations = connector.fetch_all(
                "SELECT deletion_request_generation, state "
                "FROM operational_operational_preparations "
                "ORDER BY deletion_request_generation"
            )
            bound = connector.fetch_all(
                "SELECT preparation.deletion_request_generation "
                "FROM operational_publication_candidate_preparations AS binding "
                "JOIN operational_operational_preparations AS preparation "
                "ON preparation.preparation_id = binding.preparation_id"
            )
            committed = connector.fetch_all(
                "SELECT preparation.deletion_request_generation "
                "FROM catalog_publication_commits AS published "
                "JOIN operational_operational_preparations AS preparation "
                "ON preparation.preparation_id = published.preparation_id"
            )
            consumed = connector.fetch_all(
                "SELECT deletion_request_token FROM "
                "operational_operational_deletion_consumption_events"
            )
    finally:
        connector.close()
    return preparations, bound, committed, consumed


class _StopAt(Exception):
    pass


def _abandon_turn_before(pipeline: Pipeline, label: str) -> None:
    """Run a turn and abandon it (process death) right before ``label``."""

    def boundary(seen: str) -> None:
        if seen == label:
            raise _StopAt(seen)

    facade = VNextIngestFacade(pipeline.config, clock=Clock())
    try:
        with pytest.raises(_StopAt):
            run_ingest_turn(
                facade,
                source=pipeline.source,
                library=pipeline.library,
                boundary=boundary,
            )
    finally:
        facade.close()


@pytest.mark.parametrize("first_revision", (True, False), ids=("first", "second"))
@pytest.mark.parametrize(
    "label",
    ("publication.commit:LIBRARY_ACTIVATION", "publication.commit:FINALIZE"),
)
def test_restart_after_a_sealed_commit_finalizes_the_pending_publication(
    pipeline: Pipeline,
    label: str,
    first_revision: bool,
) -> None:
    """A process that dies after the publication commit is durable (the
    receipt is DB_COMMITTED, the reader head still points at the old
    publication) must not wedge the next turn's source handoff: the restarted
    turn resumes the pending activation and finalization and converges to the
    same catalog as an uninterrupted turn."""

    reference_path = Path(pipeline.config.database.database + ".reference")
    reference: dict[str, Any] | None = None
    if not first_revision:
        pipeline.turn()
    if pipeline.config.database.sql_type == "sqlite":
        # The reference is the uninterrupted turn on a copy of the same
        # durable state.
        shutil.copyfile(pipeline.config.database.database, reference_path)
        copied = CoreConfig(
            database=DatabaseConfig(sql_type="sqlite", database=str(reference_path))
        )
        if not first_revision:
            pipeline.source.put(
                gallery(1001, pages=[b"p0-a", b"p1-a-modified"], artists=["alice"])
            )
        clone = Pipeline(
            copied, copy.deepcopy(pipeline.source), copy.deepcopy(pipeline.library)
        )
        clone.library.source = clone.source
        clone.turn()
        reference = catalog_view(copied)
    elif not first_revision:
        pipeline.source.put(
            gallery(1001, pages=[b"p0-a", b"p1-a-modified"], artists=["alice"])
        )
    _abandon_turn_before(pipeline, label)
    pipeline.turn(clock=takeover_clock())
    pipeline.ready()
    if reference is not None:
        assert pipeline.view() == reference
    assert pipeline.view()["publication_count"] == len(pipeline.source.galleries)


def test_stale_session_cannot_register_a_policy_after_takeover(
    pipeline: Pipeline,
) -> None:
    """Policy registration is an ingest write: after another process took the
    expired gate and generation over, the stale session's registration fails
    closed with its fence error and writes nothing."""

    stale = VNextIngestFacade(pipeline.config, clock=Clock())
    taker = VNextIngestFacade(pipeline.config, clock=takeover_clock())
    try:
        stale_session = claim_session(stale)
        claim_session(taker)
        before = snapshot_database(pipeline.config)
        with pytest.raises(FENCE_ERRORS):
            stale.ensure_policy(
                stale_session, ingest_policy(spam_occurrence_threshold=7)
            )
        assert snapshot_difference(before, snapshot_database(pipeline.config)) == {}
    finally:
        stale.close()
        taker.close()


SHORT_LEASE_MICROSECONDS = 8_000_000
# Live MariaDB turns take several times longer, so the lease that must
# outlive the abandoned turn (but expire before the takeover) is longer there.
MARIADB_SHORT_LEASE_MICROSECONDS = 45_000_000


def _short_lease(pipeline: Pipeline) -> int:
    if pipeline.config.database.sql_type == "mariadb":
        return MARIADB_SHORT_LEASE_MICROSECONDS
    return SHORT_LEASE_MICROSECONDS


def _abandon_turn_with_short_lease(pipeline: Pipeline, label: str) -> None:
    """Abandon a turn right before ``label`` under a short lease and wait for
    that lease to expire, so a later turn on the real clock takes over.

    (A future clock would poison later real-time turns: source-build times
    are compared against the publication base committed by that clock.)"""

    def boundary(seen: str) -> None:
        if seen == label:
            raise _StopAt(seen)

    lease = _short_lease(pipeline)
    facade = VNextIngestFacade(pipeline.config, clock=Clock())
    try:
        session = claim_session(facade, lease=lease)
        with pytest.raises(_StopAt):
            run_ingest_turn(
                facade,
                source=pipeline.source,
                library=pipeline.library,
                session=session,
                boundary=boundary,
            )
    finally:
        facade.close()
    time.sleep(lease / 1_000_000 + 0.5)


def _fresh_reference(
    pipeline: Pipeline,
    tag: str,
    *,
    policy: VNextIngestPolicy | None = None,
) -> dict[str, Any] | None:
    """The catalog a fresh database reaches from the current source (SQLite
    only; MariaDB compares publication counts)."""

    if pipeline.config.database.sql_type != "sqlite":
        return None
    path = Path(pipeline.config.database.database + f".{tag}")
    config = CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    initialize_database(config)
    source = copy.deepcopy(pipeline.source)
    fresh = Pipeline(config, source, MemoryLibrary(source))
    fresh.turn(policy=policy)
    return catalog_view(config)


@pytest.mark.parametrize("label", ("analysis.commit:content_owner",))
def test_source_change_after_an_abandoned_sealed_build_converges(
    pipeline: Pipeline,
    label: str,
) -> None:
    """A process that died after sealing a build (mid-analysis) and a source
    that changed before the restart must not wedge: the stale sealed build
    releases its working roots, its open analysis is abandoned, and the new
    snapshot publishes exactly what a fresh ingest of that snapshot publishes.

    A build abandoned after its candidate protected artifacts (mid-publication)
    converges the same way, but its orphaned storage protections can only be
    released by the artifact-release reconciliation, which no maintenance entry
    point drives yet; maintenance then reports BLOCKED until that is wired.
    That case is therefore recorded as an open policy item, not pinned here."""

    pipeline.turn()
    pipeline.source.put(gallery(1006, pages=[b"p0-f"], artists=["frank"]))
    _abandon_turn_with_short_lease(pipeline, label)
    pipeline.source.remove(("gallery-1006",))
    pipeline.source.put(gallery(1007, pages=[b"p0-g"], artists=["gina"]))
    pipeline.turn()
    pipeline.ready()
    # The finalized turn released both working roots; the stale build's
    # analysis is no longer OPEN.
    assert _stale_build_facts(pipeline.config) == {
        "working_builds": 0,
        "working_candidates": 0,
        "open_analyses": 0,
    }
    assert pipeline.view()["publication_count"] == len(pipeline.source.galleries)
    reference = _fresh_reference(pipeline, "reference")
    if reference is not None:
        assert _publication_titles(pipeline.view()) == _publication_titles(reference)


@pytest.mark.parametrize(
    "label",
    ("publication.commit:LIBRARY_ACTIVATION", "publication.commit:FINALIZE"),
)
def test_source_change_after_a_sealed_commit_recovers_before_current_snapshot(
    pipeline: Pipeline,
    label: str,
) -> None:
    """One takeover session first recovers the immutable committed snapshot,
    without rescanning or recreating it, then publishes the source that exists
    now under the same session authority."""

    pipeline.turn()
    pipeline.source.put(gallery(1006, pages=[b"p0-f"], artists=["frank"]))
    _abandon_turn_with_short_lease(pipeline, label)
    # A durable DB_COMMITTED successor is a valid READY state while the reader
    # head still names its exact PUBLISHED predecessor.
    pipeline.ready()
    pipeline.source.remove(("gallery-1006",))
    pipeline.source.put(gallery(1007, pages=[b"p0-g"], artists=["gina"]))
    receipts, _ = pipeline.turn()
    pipeline.ready()
    assert receipts.session.ingest_generation > 0
    assert pipeline.view()["publication_count"] == len(pipeline.source.galleries)
    assert pipeline.view()["revision"] == 3
    assert pipeline.library.activations[2].status.name == "COMPLETE"
    assert pipeline.library.activations[3].status.name == "COMPLETE"
    reference = _fresh_reference(pipeline, "reference")
    if reference is not None:
        assert _publication_titles(pipeline.view()) == _publication_titles(reference)


def _sqlite_pipeline(config: CoreConfig) -> Pipeline:
    initialize_database(config)
    source = MemorySource(_corpus())
    return Pipeline(config, source, MemoryLibrary(source))


def _drain_repository_cleanup_cycle(
    connector: Any,
    gate: Any,
    cycle: Any,
    *,
    clock: Clock,
) -> None:
    with connector.transaction():
        result = VNextCleanupRepository.resume_cycle(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            cycle=cycle,
            now=clock(),
        )
    for attempt in range(64):
        if result.cycle_complete:
            return
        assert result.generation is not None
        with connector.transaction():
            result = VNextCleanupRepository.advance(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                cycle=cycle,
                command=CleanupBatchCommand(
                    (attempt + 1).to_bytes(32, "big"),
                    result.generation,
                ),
                now=clock(),
            )
    raise AssertionError("cleanup cycle did not complete")


def test_second_revision_retires_commit_pins_and_replays_compacted_current(
    sqlite_config: CoreConfig,
) -> None:
    pipeline = _sqlite_pipeline(sqlite_config)
    first, first_progressed = pipeline.turn()
    assert first_progressed == 0

    connector = open_connector(sqlite_config)
    try:
        with connector.read_transaction():
            first_commit = connector.fetch_one(
                "SELECT receipt_id, candidate_id, generation "
                "FROM catalog_publication_commits WHERE revision = %s",
                (1,),
            )
    finally:
        connector.close()
    assert len(first_commit) == 3
    first_receipt = bytes(first_commit[0])
    first_candidate = bytes(first_commit[1])
    assert first_commit[2] == 1

    pipeline.source.put(
        gallery(
            1001,
            pages=[b"p0-a", b"p1-a-retention-successor"],
            artists=["alice"],
            extra_tags=[("female", "glasses")],
        )
    )
    second, pre_maintenance_progressed = pipeline.turn(drain=False)

    assert second.source.build_id != first.source.build_id
    assert pre_maintenance_progressed == 0
    connector = open_connector(sqlite_config)
    try:
        with connector.read_transaction():
            current = connector.fetch_one(
                "SELECT committed.receipt_id, committed.candidate_id, "
                "committed.generation "
                "FROM catalog_publication_commit_head_receipts AS head "
                "JOIN catalog_publication_commits AS committed "
                "ON committed.receipt_id = head.receipt_id "
                "WHERE head.channel = %s",
                (b"default",),
            )
            assert len(current) == 3
            assert current[2] == 2
            assert connector.fetch_one(
                "SELECT 1 FROM catalog_publication_commits WHERE receipt_id = %s",
                (first_receipt,),
            ) == (1,)
            assert (
                connector.fetch_all(
                    "SELECT candidate_id, base_receipt_id FROM "
                    "catalog_publication_candidate_base_publication_commits"
                )
                == []
            )
            assert connector.fetch_one(
                "SELECT base_receipt_id FROM "
                "catalog_source_build_base_publication_commits "
                "WHERE build_id = %s",
                (second.source.build_id,),
            ) == (first_receipt,)
    finally:
        connector.close()
    pipeline.ready()

    facade = VNextIngestFacade(sqlite_config, clock=Clock())
    try:
        progressed = drain_maintenance(facade)
    finally:
        facade.close()
    assert progressed >= 1

    connector = open_connector(sqlite_config)
    try:
        with connector.read_transaction():
            assert (
                connector.fetch_one(
                    "SELECT 1 FROM catalog_publication_commits WHERE receipt_id = %s",
                    (first_receipt,),
                )
                == ()
            )
            assert (
                connector.fetch_one(
                    "SELECT 1 FROM catalog_publication_candidates "
                    "WHERE candidate_id = %s",
                    (first_candidate,),
                )
                == ()
            )
            assert (
                connector.fetch_all(
                    "SELECT candidate_id, base_receipt_id FROM "
                    "catalog_publication_candidate_base_publication_commits"
                )
                == []
            )
            assert (
                connector.fetch_one(
                    "SELECT base_receipt_id FROM "
                    "catalog_source_build_base_publication_commits "
                    "WHERE build_id = %s",
                    (second.source.build_id,),
                )
                == ()
            )
            assert connector.fetch_all(
                "SELECT generation FROM catalog_publication_generation_nodes "
                "ORDER BY generation"
            ) == [(2,)]
            assert (
                connector.fetch_all(
                    "SELECT successor_generation, predecessor_generation "
                    "FROM catalog_publication_generation_successors"
                )
                == []
            )
    finally:
        connector.close()

    before_replay = pipeline.view()
    replay, replay_progressed = pipeline.turn()

    assert replay.source.replayed
    assert replay.publication.terminal
    assert replay_progressed == 0
    assert pipeline.view() == before_replay
    pipeline.ready()


@pytest.mark.merge_smoke
def test_full_check_accepts_each_durable_publication_commit_release_phase(
    sqlite_config: CoreConfig,
) -> None:
    """Every durable OPEN PCOM phase remains READY after its one-shot release."""

    pipeline = _sqlite_pipeline(sqlite_config)
    pipeline.turn()
    pipeline.source.put(
        gallery(
            1001,
            pages=[b"p0-a", b"p1-a-pcom-crash"],
            artists=["alice"],
            extra_tags=[("female", "glasses")],
        )
    )
    second, _ = pipeline.turn(drain=False)
    clock = takeover_clock()
    connector = open_connector(sqlite_config)
    try:
        predecessor = connector.fetch_one(
            "SELECT receipt_id FROM catalog_publication_commits WHERE revision = 1"
        )
        assert len(predecessor) == 1
        predecessor_receipt = bytes(predecessor[0])
        with connector.transaction():
            gate = MaintenanceGateRepository.claim_exclusive(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=clock(),
                lease_duration=LEASE_MICROSECONDS,
            )
        with connector.transaction():
            cycle = VNextCleanupRepository.begin_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                target_kind=CleanupTargetKind.PUBLICATION_COMMIT,
                shard_no=predecessor_receipt[0],
                cycle_cutoff_at=clock(),
                max_rows_per_transaction=256,
                now=clock(),
            )
        with connector.transaction():
            result = VNextCleanupRepository.resume_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                cycle=cycle,
                now=clock(),
            )

        observed_phases: set[str] = set()
        observed_states: set[tuple[str, bytes]] = set()
        completed_checked = False
        for attempt in range(40):
            checkpoint = connector.fetch_one(
                "SELECT checkpoint.phase, checkpoint.cursor_bytes, phase.phase_order "
                "FROM operational_cleanup_checkpoints AS checkpoint "
                "JOIN operational_cleanup_phases AS phase "
                "ON phase.target_kind = 'PUBLICATION_COMMIT' "
                "AND phase.phase = checkpoint.phase "
                "WHERE checkpoint.cleanup_id = %s AND checkpoint.state = 'OPEN'",
                (cycle.cleanup_id,),
            )
            build_base_absent = (
                connector.fetch_one(
                    "SELECT 1 FROM catalog_source_build_base_publication_commits "
                    "WHERE build_id = %s",
                    (second.source.build_id,),
                )
                == ()
            )
            if build_base_absent and len(checkpoint) == 3:
                phase = str(checkpoint[0])
                state_key = (phase, bytes(checkpoint[1]))
                if state_key not in observed_states:
                    report = full_check(sqlite_config)
                    assert report.state == "READY"
                    observed_phases.add(phase)
                    observed_states.add(state_key)
            if result.cycle_complete:
                assert full_check(sqlite_config).state == "READY"
                completed_checked = True
                break
            assert result.generation is not None
            with connector.transaction():
                result = VNextCleanupRepository.advance(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    cycle=cycle,
                    command=CleanupBatchCommand(
                        (attempt + 1).to_bytes(32, "big"),
                        result.generation,
                    ),
                    now=clock(),
                )
        else:
            raise AssertionError(
                "publication-commit cleanup did not retire predecessor"
            )

        assert observed_phases == {
            "PCOM_RELEASE_BUILD_BASE",
            "PCOM_PREPARATION_BINDING",
            "PCOM_PREPARATION_BATCH",
            "PCOM_PREPARATION_CHECKPOINT",
            "PCOM_PREPARATION",
            "PCOM_EVENT",
            "PCOM_FINALIZATION_MARKER",
            "PCOM_FINALIZATION_BATCH",
            "PCOM_COMMIT_EFFECT_ROOT",
            "PCOM_FINALIZATION_CHECKPOINT",
            "PCOM_ANCHOR",
        }
        assert completed_checked
    finally:
        connector.close()


@pytest.mark.parametrize(
    "corruption",
    ("frozen-authority", "orphan-anchor", "missing-orphan-anchor"),
)
@pytest.mark.merge_smoke
def test_full_check_rejects_forged_publication_commit_cleanup_proof(
    sqlite_config: CoreConfig,
    corruption: str,
) -> None:
    """A transient retirement gap requires the exact bounded OPEN authority."""

    pipeline = _sqlite_pipeline(sqlite_config)
    pipeline.turn()
    pipeline.source.put(
        gallery(
            1001,
            pages=[b"p0-a", b"p1-a-pcom-forgery"],
            artists=["alice"],
            extra_tags=[("female", "glasses")],
        )
    )
    second, _ = pipeline.turn(drain=False)
    clock = takeover_clock()
    connector = open_connector(sqlite_config)
    try:
        predecessor = connector.fetch_one(
            "SELECT receipt_id FROM catalog_publication_commits WHERE revision = 1"
        )
        assert len(predecessor) == 1
        predecessor_receipt = bytes(predecessor[0])
        with connector.transaction():
            gate = MaintenanceGateRepository.claim_exclusive(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=clock(),
                lease_duration=LEASE_MICROSECONDS,
            )
        with connector.transaction():
            cycle = VNextCleanupRepository.begin_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                target_kind=CleanupTargetKind.PUBLICATION_COMMIT,
                shard_no=predecessor_receipt[0],
                cycle_cutoff_at=clock(),
                max_rows_per_transaction=256,
                now=clock(),
            )
        with connector.transaction():
            result = VNextCleanupRepository.resume_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                cycle=cycle,
                now=clock(),
            )

        for attempt in range(40):
            checkpoint = connector.fetch_one(
                "SELECT phase, cursor_bytes FROM operational_cleanup_checkpoints "
                "WHERE cleanup_id = %s AND state = 'OPEN'",
                (cycle.cleanup_id,),
            )
            phase = str(checkpoint[0]) if checkpoint else ""
            cursor = bytes(checkpoint[1]) if checkpoint else b""
            predecessor_retained = connector.fetch_one(
                "SELECT 1 FROM catalog_publication_commits WHERE receipt_id = %s",
                (predecessor_receipt,),
            ) == (1,)
            build_base_absent = (
                connector.fetch_one(
                    "SELECT 1 FROM catalog_source_build_base_publication_commits "
                    "WHERE build_id = %s",
                    (second.source.build_id,),
                )
                == ()
            )
            if (
                corruption == "frozen-authority"
                and phase == "PCOM_RELEASE_BUILD_BASE"
                and cursor
                and predecessor_retained
                and build_base_absent
            ):
                root_rows = connector.fetch_all(
                    "SELECT frozen_root_key FROM operational_cleanup_cycle_roots "
                    "WHERE cleanup_id = %s ORDER BY frozen_root_key",
                    (cycle.cleanup_id,),
                )
                assert len(root_rows) == 1
                original_root = bytes(root_rows[0][0])
                forged_root = original_root[:24] + b"z" * 16
                with connector.transaction():
                    assert (
                        connector.execute_affected(
                            "UPDATE operational_cleanup_cycle_roots "
                            "SET frozen_root_key = %s WHERE cleanup_id = %s "
                            "AND frozen_root_key = %s",
                            (forged_root, cycle.cleanup_id, original_root),
                        )
                        == 1
                    )
                    assert (
                        connector.execute_affected(
                            "UPDATE operational_cleanup_jobs "
                            "SET frozen_root_set_sha256 = %s WHERE cleanup_id = %s",
                            (
                                cleanup_module._frozen_root_set_sha256(
                                    cycle.cleanup_id,
                                    (forged_root,),
                                ),
                                cycle.cleanup_id,
                            ),
                        )
                        == 1
                    )
                with pytest.raises(CatalogSemanticValidationError):
                    full_check(sqlite_config)
                break
            if (
                corruption == "orphan-anchor"
                and phase == "PCOM_COMMIT_EFFECT_ROOT"
                and cursor
                and not predecessor_retained
            ):
                connector.execute("PRAGMA foreign_keys = OFF")
                connector.execute(
                    "INSERT INTO catalog_publication_commit_anchors (receipt_id) "
                    "VALUES (%s)",
                    (b"z" * 16,),
                )
                connector.execute("PRAGMA foreign_keys = ON")
                with pytest.raises(CatalogSemanticValidationError):
                    full_check(sqlite_config)
                break
            if (
                corruption == "missing-orphan-anchor"
                and phase == "PCOM_COMMIT_EFFECT_ROOT"
                and cursor
                and not predecessor_retained
            ):
                connector.execute("PRAGMA foreign_keys = OFF")
                connector.execute(
                    "DELETE FROM catalog_publication_commit_anchors "
                    "WHERE receipt_id = %s",
                    (predecessor_receipt,),
                )
                connector.execute("PRAGMA foreign_keys = ON")
                with pytest.raises(CatalogSemanticValidationError):
                    full_check(sqlite_config)
                break
            assert not result.cycle_complete
            assert result.generation is not None
            with connector.transaction():
                result = VNextCleanupRepository.advance(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    cycle=cycle,
                    command=CleanupBatchCommand(
                        (attempt + 1).to_bytes(32, "big"),
                        result.generation,
                    ),
                    now=clock(),
                )
        else:
            raise AssertionError("publication-commit cleanup did not reach forgery")
    finally:
        connector.close()


@pytest.mark.merge_smoke
def test_full_check_accepts_multi_root_pcom_keyset_coverage(
    sqlite_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later PCOM cursor covers earlier frozen roots in canonical order."""

    receipt_ids = iter(
        (
            bytes.fromhex("21" + "11" * 15),
            bytes.fromhex("21" + "22" * 15),
            bytes.fromhex("31" + "33" * 15),
        )
    )
    monkeypatch.setattr(
        publication_module, "_new_receipt_id", lambda: next(receipt_ids)
    )
    pipeline = _sqlite_pipeline(sqlite_config)
    pipeline.turn(drain=False)
    pipeline.source.put(gallery(1001, pages=[b"pcom-multi-r2"], artists=["alice"]))
    pipeline.turn(drain=False)
    pipeline.source.put(gallery(1001, pages=[b"pcom-multi-r3"], artists=["alice"]))
    # Deliberately defer only the resident maintenance preflight so the public
    # writers construct the repository's supported multi-root state.  Normal
    # resident scheduling usually drains this state between turns.
    with monkeypatch.context() as maintenance_override:
        maintenance_override.setattr(
            VNextCleanupRepository,
            "current_only_maintenance_state",
            staticmethod(
                lambda _work, *, cycle_cutoff_at: (
                    cleanup_module.CatalogPublicationMaintenanceState.DONE
                )
            ),
        )
        pipeline.turn(drain=False)

    connector = open_connector(sqlite_config)
    clock = takeover_clock()
    try:
        old_receipts = connector.fetch_all(
            "SELECT receipt_id FROM catalog_publication_commits "
            "WHERE revision < 3 ORDER BY receipt_id"
        )
        assert old_receipts == [
            (bytes.fromhex("21" + "11" * 15),),
            (bytes.fromhex("21" + "22" * 15),),
        ]
        with connector.transaction():
            gate = MaintenanceGateRepository.claim_exclusive(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=clock(),
                lease_duration=LEASE_MICROSECONDS,
            )
        with connector.transaction():
            cycle = VNextCleanupRepository.begin_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                target_kind=CleanupTargetKind.PUBLICATION_COMMIT,
                shard_no=0x21,
                cycle_cutoff_at=clock(),
                max_rows_per_transaction=256,
                now=clock(),
            )
        with connector.transaction():
            result = VNextCleanupRepository.resume_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                cycle=cycle,
                now=clock(),
            )

        observed: set[str] = set()
        forged_cursor_rejected = False
        for attempt in range(40):
            checkpoint = connector.fetch_one(
                "SELECT phase, cursor_bytes FROM operational_cleanup_checkpoints "
                "WHERE cleanup_id = %s AND state = 'OPEN'",
                (cycle.cleanup_id,),
            )
            if (
                checkpoint
                and checkpoint[0]
                in {
                    "PCOM_RELEASE_BUILD_BASE",
                    "PCOM_FINALIZATION_MARKER",
                    "PCOM_FINALIZATION_BATCH",
                }
                and checkpoint[1]
            ):
                assert full_check(sqlite_config).state == "READY"
                observed.add(str(checkpoint[0]))
                if (
                    checkpoint[0] == "PCOM_RELEASE_BUILD_BASE"
                    and not forged_cursor_rejected
                ):
                    receipt = connector.fetch_one(
                        "SELECT generation, receipt_start_cursor, "
                        "receipt_prior_chain_sha256, receipt_input_sha256, "
                        "receipt_row_count, chain_sha256 "
                        "FROM operational_cleanup_checkpoints "
                        "WHERE cleanup_id = %s AND phase = %s",
                        (cycle.cleanup_id, checkpoint[0]),
                    )
                    assert len(receipt) == 6
                    original_cursor = bytes(checkpoint[1])
                    forged_cursor = cleanup_module._encode_static_cursor(
                        0,
                        (b"\xff" * 16, original_cursor[26:42]),
                    )
                    forged_chain = cleanup_module._next_chain(
                        bytes(receipt[2]),
                        str(checkpoint[0]),
                        int(receipt[0]),
                        bytes(receipt[1]),
                        forged_cursor,
                        bytes(receipt[3]),
                        int(receipt[4]),
                    )
                    connector.execute(
                        "UPDATE operational_cleanup_checkpoints "
                        "SET cursor_bytes = %s, chain_sha256 = %s "
                        "WHERE cleanup_id = %s AND phase = %s",
                        (
                            forged_cursor,
                            forged_chain,
                            cycle.cleanup_id,
                            checkpoint[0],
                        ),
                    )
                    with pytest.raises(CatalogSemanticValidationError):
                        full_check(sqlite_config)
                    connector.execute(
                        "UPDATE operational_cleanup_checkpoints "
                        "SET cursor_bytes = %s, chain_sha256 = %s "
                        "WHERE cleanup_id = %s AND phase = %s",
                        (
                            original_cursor,
                            bytes(receipt[5]),
                            cycle.cleanup_id,
                            checkpoint[0],
                        ),
                    )
                    forged_cursor_rejected = True
            if result.cycle_complete or len(observed) == 3:
                break
            assert result.generation is not None
            with connector.transaction():
                result = VNextCleanupRepository.advance(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    cycle=cycle,
                    command=CleanupBatchCommand(
                        (attempt + 1).to_bytes(32, "big"),
                        result.generation,
                    ),
                    now=clock(),
                )
        assert observed == {
            "PCOM_RELEASE_BUILD_BASE",
            "PCOM_FINALIZATION_MARKER",
            "PCOM_FINALIZATION_BATCH",
        }
        assert forged_cursor_rejected
    finally:
        connector.close()


@pytest.mark.merge_smoke
def test_full_check_accepts_each_durable_publication_generation_phase(
    sqlite_config: CoreConfig,
) -> None:
    """Only an exact OPEN PG cursor may explain a transient chain gap."""

    pipeline = _sqlite_pipeline(sqlite_config)
    pipeline.turn(drain=False)
    pipeline.source.put(
        gallery(1001, pages=[b"publication-generation-r2"], artists=["alice"])
    )
    pipeline.turn(drain=False)
    clock = takeover_clock()
    connector = open_connector(sqlite_config)
    try:
        old_receipt_row = connector.fetch_one(
            "SELECT receipt_id FROM catalog_publication_commits WHERE revision = 1"
        )
        assert len(old_receipt_row) == 1
        old_receipt = bytes(old_receipt_row[0])
        with connector.transaction():
            gate = MaintenanceGateRepository.claim_exclusive(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=clock(),
                lease_duration=LEASE_MICROSECONDS,
            )
        with connector.transaction():
            commit_cycle = VNextCleanupRepository.begin_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                target_kind=CleanupTargetKind.PUBLICATION_COMMIT,
                shard_no=old_receipt[0],
                cycle_cutoff_at=clock(),
                max_rows_per_transaction=256,
                now=clock(),
            )
        _drain_repository_cleanup_cycle(connector, gate, commit_cycle, clock=clock)
        assert full_check(sqlite_config).state == "READY"

        with connector.transaction():
            generation_cycle = VNextCleanupRepository.begin_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                target_kind=CleanupTargetKind.PUBLICATION_GENERATION,
                shard_no=0,
                cycle_cutoff_at=clock(),
                max_rows_per_transaction=256,
                now=clock(),
            )
        assert full_check(sqlite_config).state == "READY"
        connector.execute(
            "DELETE FROM catalog_publication_generation_successors "
            "WHERE successor_generation = 1"
        )
        with pytest.raises(
            CatalogSemanticValidationError,
            match="successor chain is gapped",
        ):
            catalog_refinement_module.check_publication_atomicity_v1(connector)
        connector.execute(
            "INSERT INTO catalog_publication_generation_successors "
            "(successor_generation, predecessor_generation) VALUES (1, 0)"
        )
        with connector.transaction():
            result = VNextCleanupRepository.resume_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                cycle=generation_cycle,
                now=clock(),
            )

        observed: set[tuple[str, bool]] = set()
        checked_root_forgery = False
        checked_query_bound = False
        for attempt in range(8):
            checkpoint = connector.fetch_one(
                "SELECT phase, cursor_bytes FROM operational_cleanup_checkpoints "
                "WHERE cleanup_id = %s AND state = 'OPEN'",
                (generation_cycle.cleanup_id,),
            )
            if checkpoint:
                phase = str(checkpoint[0])
                has_cursor = bool(checkpoint[1])
                observed.add((phase, has_cursor))
                assert full_check(sqlite_config).state == "READY"
                if phase == "PG_EDGE" and has_cursor and not checked_query_bound:
                    original_fetch_all = connector.fetch_all
                    pg_reads: list[tuple[str, int]] = []

                    def record_pg_read(
                        query: str,
                        data: tuple[Any, ...] = (),
                    ) -> list[tuple[Any, ...]]:
                        rows = original_fetch_all(query, data)
                        if "sweep.target_kind = 'PUBLICATION_GENERATION'" in query:
                            pg_reads.append((query, len(rows)))
                        return rows

                    with patch.object(
                        connector,
                        "fetch_all",
                        side_effect=record_pg_read,
                    ):
                        catalog_refinement_module.check_publication_atomicity_v1(
                            connector
                        )
                    assert len(pg_reads) == 1
                    assert "LIMIT 257" in pg_reads[0][0]
                    assert pg_reads[0][1] <= 256
                    checked_query_bound = True
                if phase == "PG_ROOT" and not has_cursor and not checked_root_forgery:
                    connector.execute(
                        "DELETE FROM catalog_publication_generation_nodes "
                        "WHERE generation = 0"
                    )
                    with pytest.raises(
                        CatalogSemanticValidationError,
                        match="generation nodes differ",
                    ):
                        catalog_refinement_module.check_publication_atomicity_v1(
                            connector
                        )
                    connector.execute(
                        "INSERT INTO catalog_publication_generation_nodes "
                        "(generation) VALUES (0)"
                    )
                    checked_root_forgery = True
            if result.cycle_complete:
                break
            assert result.generation is not None
            with connector.transaction():
                result = VNextCleanupRepository.advance(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    cycle=generation_cycle,
                    command=CleanupBatchCommand(
                        (attempt + 101).to_bytes(32, "big"),
                        result.generation,
                    ),
                    now=clock(),
                )
        assert result.cycle_complete
        assert checked_root_forgery and checked_query_bound
        assert observed == {
            ("PG_EDGE", False),
            ("PG_EDGE", True),
            ("PG_ROOT", False),
            ("PG_ROOT", True),
        }
        assert full_check(sqlite_config).state == "READY"
        assert connector.fetch_all(
            "SELECT generation FROM catalog_publication_generation_nodes "
            "ORDER BY generation"
        ) == [(2,)]
        assert (
            connector.fetch_all(
                "SELECT successor_generation, predecessor_generation "
                "FROM catalog_publication_generation_successors"
            )
            == []
        )
    finally:
        connector.close()


def _generation_build(config: CoreConfig, generation: int) -> bytes | None:
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            row = connector.fetch_one(
                "SELECT build_id FROM operational_source_build_generations "
                "WHERE generation = %s",
                (generation,),
            )
    finally:
        connector.close()
    if not row:
        return None
    assert len(row) == 1
    return bytes(row[0])


@pytest.mark.parametrize(
    "label",
    ("publication.commit:LIBRARY_ACTIVATION", "publication.commit:FINALIZE"),
)
def test_receipt_scoped_recovery_publishes_before_observing_changed_source(
    sqlite_config: CoreConfig,
    label: str,
) -> None:
    """A new session finalizes the old immutable commit without recreating its
    filesystem snapshot or reserving its build, then uses that same generation
    to ingest the source that exists now."""

    pipeline = _sqlite_pipeline(sqlite_config)
    pipeline.turn()
    pipeline.source.put(gallery(1006, pages=[b"p0-f"], artists=["frank"]))
    _abandon_turn_before(pipeline, label)
    pipeline.source.remove(("gallery-1006",))
    pipeline.source.put(gallery(1007, pages=[b"p0-g"], artists=["gina"]))

    facade = VNextIngestFacade(pipeline.config, clock=takeover_clock())
    try:
        session = claim_session(facade)
        assert _generation_build(pipeline.config, session.ingest_generation) is None
        recovered = run_publication_recovery(facade, session, pipeline.library)
        assert recovered is not None and recovered.terminal
        assert _generation_build(pipeline.config, session.ingest_generation) is None
        assert _publication_gids(pipeline.view()) == [1001, 1002, 1003, 1006]

        receipts = run_ingest_turn(
            facade,
            source=pipeline.source,
            library=pipeline.library,
            session=session,
        )
    finally:
        facade.close()

    assert receipts.session.ingest_generation == session.ingest_generation
    assert _generation_build(pipeline.config, session.ingest_generation) == (
        receipts.source.build_id
    )
    assert _publication_gids(pipeline.view()) == [1001, 1002, 1003, 1007]
    assert pipeline.view()["revision"] == 3
    pipeline.ready()


@pytest.mark.parametrize(
    "lost_response",
    (
        "activation-page",
        "finalization-prepare",
        "finalization-commit",
        "completion-prepare",
        "completion-commit",
    ),
)
def test_receipt_scoped_recovery_replays_external_and_database_response_loss(
    sqlite_config: CoreConfig,
    lost_response: str,
) -> None:
    pipeline = _sqlite_pipeline(sqlite_config)
    _abandon_turn_before(pipeline, "publication.commit:LIBRARY_ACTIVATION")
    clock = takeover_clock()
    facade = VNextIngestFacade(pipeline.config, clock=clock)
    session = claim_session(facade)
    adapters = {pipeline.library.adapter_id: pipeline.library}
    try:
        if lost_response == "activation-page":
            issued = facade.try_issue_publication_recovery_step(session)
            assert issued is not None and issued.operation == "LIBRARY_ACTIVATION"
            prepared = facade.prepare_publication_step(
                issued,
                artifact_adapters=adapters,
                finalization_adapters=adapters,
                library_activation=pipeline.library,
            )
            with prepared:
                facade.commit_publication_step(session, prepared)

            issued = facade.try_issue_publication_recovery_step(session)
            assert issued is not None and issued.operation == "LIBRARY_ACTIVATION"
            prepared = facade.prepare_publication_step(
                issued,
                artifact_adapters=adapters,
                finalization_adapters=adapters,
                library_activation=pipeline.library,
            )
            assert pipeline.library.activation_calls[-1] == "activate_page"
            prepared.close()
        elif lost_response == "finalization-prepare":
            for _ in range(32):
                issued = facade.try_issue_publication_recovery_step(session)
                assert issued is not None
                prepared = facade.prepare_publication_step(
                    issued,
                    artifact_adapters=adapters,
                    finalization_adapters=adapters,
                    library_activation=pipeline.library,
                )
                if issued.operation == "FINALIZE":
                    prepared.close()
                    break
                with prepared:
                    facade.commit_publication_step(session, prepared)
            else:  # pragma: no cover - bounded protocol regression guard
                raise AssertionError("recovery did not reach finalization")
        elif lost_response == "finalization-commit":
            for _ in range(32):
                issued = facade.try_issue_publication_recovery_step(session)
                assert issued is not None
                prepared = facade.prepare_publication_step(
                    issued,
                    artifact_adapters=adapters,
                    finalization_adapters=adapters,
                    library_activation=pipeline.library,
                )
                with prepared:
                    facade.commit_publication_step(session, prepared)
                if issued.operation == "FINALIZE":
                    try:
                        assert pipeline.view()["revision"] == 1
                    except CatalogRevisionNotFoundError:
                        continue
                    break
            else:  # pragma: no cover - bounded protocol regression guard
                raise AssertionError("recovery did not reach finalization")
        else:
            for _ in range(32):
                issued = facade.try_issue_publication_recovery_step(session)
                assert issued is not None
                if issued.operation == "RECOVERY_COMPLETE":
                    break
                prepared = facade.prepare_publication_step(
                    issued,
                    artifact_adapters=adapters,
                    finalization_adapters=adapters,
                    library_activation=pipeline.library,
                )
                with prepared:
                    facade.commit_publication_step(session, prepared)
            else:  # pragma: no cover - bounded protocol regression guard
                raise AssertionError("recovery did not reach completion")

            prepared = facade.prepare_publication_step(
                issued,
                artifact_adapters=adapters,
                finalization_adapters=adapters,
                library_activation=pipeline.library,
            )
            assert pipeline.library.activations[1].status.name == "COMPLETE"
            if lost_response == "completion-prepare":
                prepared.close()
            else:
                with prepared:
                    facade.commit_publication_step(session, prepared)
    finally:
        facade.close()

    restarted = VNextIngestFacade(pipeline.config, clock=clock)
    try:
        recovered = run_publication_recovery(restarted, session, pipeline.library)
        assert recovered is not None and recovered.terminal
        assert _generation_build(pipeline.config, session.ingest_generation) is None
        completion = restarted.complete_ingest(session)
    finally:
        restarted.close()
    assert completion.ingest_generation == session.ingest_generation
    assert pipeline.library.activations[1].status.name == "COMPLETE"
    pipeline.ready()


# A different spam-occurrence threshold resolves to a different analysis
# policy while the manifest (source) policy stays the same.
CHANGED_POLICY_THRESHOLD = 7


@pytest.mark.merge_smoke
@pytest.mark.parametrize("label", ("analysis.commit:content_owner",))
def test_policy_change_at_takeover_after_a_mid_analysis_crash_converges(
    pipeline: Pipeline,
    label: str,
) -> None:
    """A process dies mid-analysis and the restarted process registers a
    different analysis policy for the unchanged snapshot.  The manifest forbids
    a different-policy sibling analysis of the same build, so the takeover must
    retire the abandoned analysis and analyze a successor build of the same
    snapshot under the new policy, converging to the catalog a fresh ingest
    under that policy produces."""

    _abandon_turn_before(pipeline, label)
    changed = ingest_policy(spam_occurrence_threshold=CHANGED_POLICY_THRESHOLD)
    pipeline.turn(clock=takeover_clock(), policy=changed)
    pipeline.ready()
    assert pipeline.view()["publication_count"] == len(pipeline.source.galleries)
    reference = _fresh_reference(pipeline, "policy", policy=changed)
    if reference is not None:
        assert _publication_titles(pipeline.view()) == _publication_titles(reference)


@pytest.mark.parametrize(
    "label",
    ("publication.commit:LIBRARY_ACTIVATION", "publication.commit:FINALIZE"),
)
def test_policy_change_at_takeover_after_a_durable_commit_finalizes_then_converges(
    pipeline: Pipeline,
    label: str,
) -> None:
    """The commit is durable (DB_COMMITTED) but neither activated nor finalized
    when the process dies, and the restarted process registers a different
    analysis policy.  The pending publication is durable authority: the
    takeover session must finalize it before source I/O, then re-analyze the
    unchanged snapshot under the requested policy and publish its successor
    before that same turn returns."""

    _abandon_turn_before(pipeline, label)
    changed = ingest_policy(spam_occurrence_threshold=CHANGED_POLICY_THRESHOLD)
    receipts, _ = pipeline.turn(clock=takeover_clock(), policy=changed)
    pipeline.ready()
    assert pipeline.view()["publication_count"] == len(pipeline.source.galleries)
    facts = _policy_facts(pipeline.config)
    assert facts["head"] is not None
    assert facts["head"]["policy_id"] == receipts.policy.analysis_policy_id
    assert facts["head"]["build_id"] == receipts.source.build_id
    reference = _fresh_reference(pipeline, "policy", policy=changed)
    if reference is not None:
        assert _publication_titles(pipeline.view()) == _publication_titles(reference)


def test_content_ownership_transfer_matches_a_fresh_ingest(
    pipeline: Pipeline,
) -> None:
    """Removing three of four galleries that share identical content moves
    the content ownership (and the GID candidacy) to the untouched survivor:
    the incremental analysis must publish exactly what a fresh ingest of the
    final snapshot publishes."""

    for locator in list(pipeline.source.galleries):
        pipeline.source.remove(locator.locator)
    for index in range(4):
        pipeline.source.put(
            gallery(3000 + index, pages=[b"same-0", b"same-1"], artists=["spammer"])
        )
    pipeline.source.put(gallery(1002, pages=[b"q0"], artists=["bob"]))
    pipeline.turn()
    for index in range(1, 4):
        pipeline.source.remove((f"gallery-{3000 + index}",))
    pipeline.turn()
    pipeline.ready()
    # Identical content publishes once (its owner) plus the unrelated gallery.
    assert pipeline.view()["publication_count"] == 2
    reference = _fresh_reference(pipeline, "reference")
    if reference is not None:
        assert _publication_titles(pipeline.view()) == _publication_titles(reference)


def _publication_titles(view: dict[str, Any]) -> list[Any]:
    return sorted(
        (entry["publication"]["gid"], entry["publication"]["title"])
        for entry in view["publications"]
    )


def _stale_build_facts(config: CoreConfig) -> dict[str, int]:
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            return {
                "working_builds": int(
                    connector.fetch_one(
                        "SELECT COUNT(*) FROM operational_source_working_builds"
                    )[0]
                ),
                "working_candidates": int(
                    connector.fetch_one(
                        "SELECT COUNT(*) FROM operational_catalog_working_candidates"
                    )[0]
                ),
                "open_analyses": int(
                    connector.fetch_one(
                        "SELECT COUNT(*) FROM catalog_analysis_run_states "
                        "WHERE state = 'OPEN'"
                    )[0]
                ),
            }
    finally:
        connector.close()


def test_spam_exclusion_flip_matches_a_fresh_ingest(pipeline: Pipeline) -> None:
    """Identical content in three galleries by three different artists is
    spam (excluded); removing one gallery drops the occurrence count below
    the threshold and un-excludes the survivors.  The incremental analysis
    records the exclusion delta and publishes exactly what a fresh ingest
    of the final snapshot publishes."""

    for entry in list(pipeline.source.galleries):
        pipeline.source.remove(entry.locator)
    for index, artist in enumerate(("ann", "ben", "cid")):
        pipeline.source.put(
            gallery(4000 + index, pages=[b"dup-0", b"dup-1"], artists=[artist])
        )
    pipeline.source.put(gallery(1002, pages=[b"q0"], artists=["bob"]))
    pipeline.turn()
    pipeline.ready()
    pipeline.source.remove(("gallery-4002",))
    pipeline.turn()
    pipeline.ready()
    connector = open_connector(pipeline.config)
    try:
        with connector.read_transaction():
            deltas = int(
                connector.fetch_one(
                    "SELECT COUNT(*) FROM catalog_analysis_exclusion_delta_changes"
                )[0]
            )
    finally:
        connector.close()
    assert deltas > 0
    # The un-excluded duplicate content publishes once (its owner) plus the
    # unrelated gallery.
    assert pipeline.view()["publication_count"] == 2
    reference = _fresh_reference(pipeline, "reference")
    if reference is not None:
        assert _publication_titles(pipeline.view()) == _publication_titles(reference)


def test_seventeen_incremental_revisions_compact_and_match_a_fresh_ingest(
    pipeline: Pipeline,
) -> None:
    """The overlay chain is bounded at depth sixteen: the seventeenth
    incremental analysis compacts to a self-only depth-zero analysis.  That
    compaction must materialize every key of its build (not only the galleries
    changed since the compacted pin) and publish exactly what a fresh ingest of
    the same snapshot publishes; the chain then keeps going."""

    pipeline.turn()
    pipeline.source.remove(("gallery-1003",))
    for revision in range(2, 21):
        pipeline.source.put(
            gallery(
                1001,
                pages=[b"p0-a", b"p1-a-%02d" % revision],
                artists=["alice"],
            )
        )
        pipeline.turn()
        if revision in {18, 20}:
            pipeline.ready()
    connector = open_connector(pipeline.config)
    try:
        with connector.read_transaction():
            depths = connector.fetch_all(
                "SELECT MAX(ancestry.ancestor_depth) "
                "FROM catalog_analysis_run_completed_ats AS completed "
                "JOIN catalog_analysis_state_ancestry AS ancestry "
                "ON ancestry.analysis_id = completed.analysis_id "
                "GROUP BY completed.analysis_id, completed.completed_at "
                "ORDER BY completed.completed_at"
            )
    finally:
        connector.close()
    # Depth grows to sixteen, the seventeenth incremental analysis compacts
    # to zero, and the chain restarts from there.
    assert [int(row[0]) for row in depths] == list(range(17)) + [0, 1, 2]
    assert pipeline.view()["publication_count"] == len(pipeline.source.galleries)
    reference = _fresh_reference(pipeline, "reference")
    if reference is not None:
        assert _publication_titles(pipeline.view()) == _publication_titles(reference)


# --- analysis-policy change crash matrix ----------------------------------

POLICY_CRASH_BOUNDARIES = (
    # analysis COMPLETE, publication not yet begun
    "publication.issue",
    # candidate BEGIN durable, no batch committed
    "publication.commit:BUILD_SELECTION",
    # artifact rendered by the adapter, its protection not yet durable
    "publication.commit:PREPARE_ARTIFACT",
    # artifact protection durable, operational preparation not begun
    "publication.commit:BEGIN_OPERATIONAL",
    # operational preparation OPEN with appended effects, not sealed
    "publication.commit:SEAL_OPERATIONAL",
    # every input sealed and bound, publication commit not durable
    "publication.commit:COMMIT_PUBLICATION",
    # durable DB_COMMITTED, library not activated
    "publication.commit:LIBRARY_ACTIVATION",
    # durable DB_COMMITTED, activated, not finalized
    "publication.commit:FINALIZE",
)
DURABLE_COMMIT_BOUNDARIES = frozenset(
    {"publication.commit:LIBRARY_ACTIVATION", "publication.commit:FINALIZE"}
)


def _policy_facts(config: CoreConfig) -> dict[str, Any]:
    """Durable facts a policy takeover must leave exact: the head's revision,
    build and analysis policy, every analysis run, and the row counts that a
    retry loop could otherwise grow without bound."""

    connector = open_connector(config)
    try:
        with connector.read_transaction():
            head = connector.fetch_one(
                "SELECT receipt.revision, run.build_id, run.policy_id, "
                "build.manifest_policy_id, artifact.policy_component_sha256, "
                "committed.display_title_policy_id, "
                "display.title_sort_policy_id, committed.operational_policy_id, "
                "candidate.artifacts_required "
                "FROM catalog_publication_commit_head_receipts AS head "
                "JOIN catalog_publication_receipts AS receipt "
                "ON receipt.receipt_id = head.receipt_id "
                "JOIN catalog_publication_commits AS committed "
                "ON committed.receipt_id = head.receipt_id "
                "JOIN catalog_publication_candidates AS candidate "
                "ON candidate.candidate_id = committed.candidate_id "
                "JOIN catalog_analysis_runs AS run "
                "ON run.analysis_id = candidate.analysis_id "
                "JOIN catalog_source_builds AS build "
                "ON build.build_id = run.build_id "
                "JOIN catalog_artifact_policies AS artifact "
                "ON artifact.artifact_policy_id = committed.artifact_policy_id "
                "JOIN catalog_display_title_policies AS display "
                "ON display.display_title_policy_id = "
                "committed.display_title_policy_id"
            )
            analyses = connector.fetch_all(
                "SELECT build_id, policy_id, state FROM catalog_analysis_runs "
                "ORDER BY started_at, analysis_id"
            )
            builds = connector.fetch_all(
                "SELECT build_id, state FROM catalog_source_builds ORDER BY created_at"
            )
            working = connector.fetch_one(
                "SELECT build_id FROM operational_source_working_builds WHERE slot = 1"
            )
            counts = {
                name: int(connector.fetch_one(f"SELECT COUNT(*) FROM {table}")[0])
                for name, table in (
                    ("candidates", "catalog_publication_candidates"),
                    ("commits", "catalog_publication_commits"),
                    ("mappings", "operational_source_build_generations"),
                    ("working_candidates", "operational_catalog_working_candidates"),
                )
            }
            protected = int(
                connector.fetch_one(
                    "SELECT COUNT(*) FROM catalog_prepared_artifacts "
                    "WHERE state IN ('PENDING', 'PREPARED')"
                )[0]
            )
            reserved = connector.fetch_one(
                "SELECT COALESCE(MAX(reserved_revision), 0) "
                "FROM catalog_publication_candidates"
            )
            preparations = connector.fetch_all(
                "SELECT state, COUNT(*) FROM operational_operational_preparations "
                "GROUP BY state ORDER BY state"
            )
    finally:
        connector.close()
    return {
        "head": (
            None
            if not head
            else {
                "revision": int(head[0]),
                "build_id": bytes(head[1]),
                "policy_id": int(head[2]),
                "complete_policy": (
                    int(head[3]),
                    bytes(head[4]),
                    int(head[5]),
                    int(head[6]),
                    int(head[7]),
                    bool(head[8]),
                ),
            }
        ),
        "analyses": [(bytes(row[0]), int(row[1]), str(row[2])) for row in analyses],
        "builds": [(bytes(row[0]), str(row[1])) for row in builds],
        "working_build": None if not working else bytes(working[0]),
        "protected": protected,
        "max_reserved": int(reserved[0]),
        "preparations": {str(row[0]): int(row[1]) for row in preparations},
        **counts,
    }


def _maintenance_outcome(pipeline: Pipeline) -> VNextCurrentOnlyMaintenanceOutcome:
    """Advance current-only maintenance until it is DONE or BLOCKED."""

    facade = VNextIngestFacade(pipeline.config, clock=takeover_clock())
    try:
        for _ in range(256):
            outcome = facade.drain_current_only_maintenance(LEASE_MICROSECONDS)
            if outcome is not VNextCurrentOnlyMaintenanceOutcome.PROGRESSED:
                return outcome
    finally:
        facade.close()
    raise RuntimeError("maintenance did not settle within its attempt budget")


@pytest.mark.parametrize("first_revision", (True, False), ids=("first", "second"))
@pytest.mark.parametrize("label", POLICY_CRASH_BOUNDARIES)
def test_policy_change_crash_matrix_converges_under_the_requested_policy(
    pipeline: Pipeline,
    label: str,
    first_revision: bool,
) -> None:
    """A process dies at ``label`` and the restarted process registers a
    different analysis policy for the unchanged snapshot.

    Before any durable publication commit the takeover retires the crashed
    build (its COMPLETE or OPEN analysis can never be reused under the new
    policy), never maps the new generation to it, and publishes a successor
    build of the same snapshot under the requested policy in one turn.  After
    a durable commit the same takeover session first finalizes that immutable
    receipt without binding it to the new generation, then publishes the
    requested-policy successor before the turn returns.

    Every case checks the head's actual policy, build and revision, the exact
    analysis-run set, that no working root or OPEN analysis survives, that
    the durable commit count equals the published revision count, and that
    retrying the converged turn adds no build, analysis, candidate or commit
    and at most one generation mapping per turn.  Maintenance afterwards is
    DONE unless the crashed candidate had already protected artifacts: those
    orphaned protections await the unwired artifact-release reconciliation
    and leave maintenance BLOCKED after every turn (ingest still claims once
    maintenance has settled), which is pinned here as an open owner decision
    recorded in the README."""

    if not first_revision:
        pipeline.turn()
        pipeline.source.put(gallery(1006, pages=[b"p0-f"], artists=["frank"]))
    base = _policy_facts(pipeline.config)
    base_revision = 0 if base["head"] is None else base["head"]["revision"]
    _abandon_turn_before(pipeline, label)
    crashed = _policy_facts(pipeline.config)
    assert crashed["working_build"] is not None
    # An orphaned candidate keeps its reserved revision number: the allocator
    # is monotone and never reuses a reservation, so the converged head is
    # the next reservation after everything the crash reserved.
    expected_revision = crashed["max_reserved"] + 1
    changed = ingest_policy(spam_occurrence_threshold=CHANGED_POLICY_THRESHOLD)
    receipts, _ = pipeline.turn(clock=takeover_clock(), policy=changed, drain=False)
    requested = receipts.policy.analysis_policy_id
    facts = _policy_facts(pipeline.config)
    head = facts["head"]
    assert head is not None
    assert head["policy_id"] == requested
    assert head["revision"] == expected_revision
    assert head["revision"] > base_revision
    assert head["build_id"] == receipts.source.build_id
    assert head["build_id"] != crashed["working_build"]
    assert [
        (build_id, state)
        for build_id, policy_id, state in facts["analyses"]
        if policy_id == requested
    ] == [(head["build_id"], "COMPLETE")]
    assert not any(state == "OPEN" for _build, _policy, state in facts["analyses"])
    assert facts["working_build"] is None
    assert facts["working_candidates"] == 0
    # A durable crashed commit is already counted at crash time; the takeover
    # adds exactly the successor's commit in every case.
    assert facts["commits"] == crashed["commits"] + 1
    assert facts["mappings"] <= crashed["mappings"] + 1
    orphaned_protection = (
        crashed["protected"] > 0 and label not in DURABLE_COMMIT_BOUNDARIES
    )
    # Pinned open owner decision, not hidden: the retired build's orphaned
    # candidate keeps PENDING/PREPARED artifact protections that only the
    # unwired artifact-release reconciliation can release, so current-only
    # maintenance settles to BLOCKED instead of DONE after every turn.  A
    # claim refuses ingest only while maintenance is still ACTIONABLE; like
    # the resident, every turn below settles maintenance first.
    expected_outcome = (
        VNextCurrentOnlyMaintenanceOutcome.BLOCKED
        if orphaned_protection
        else VNextCurrentOnlyMaintenanceOutcome.DONE
    )
    assert _maintenance_outcome(pipeline) is expected_outcome, label
    # Retrying the converged turn creates nothing new: no build, analysis,
    # candidate or commit appears (bounded cleanup may reclaim the retired
    # build), and generation mappings grow by at most one per turn.
    for retry in range(1, 3):
        pipeline.turn(clock=takeover_clock(), policy=changed, drain=False)
        again = _policy_facts(pipeline.config)
        assert again["head"] == head
        assert set(again["builds"]) <= set(facts["builds"])
        assert set(again["analyses"]) <= set(facts["analyses"])
        assert again["candidates"] <= facts["candidates"]
        assert again["commits"] == facts["commits"]
        assert again["mappings"] <= facts["mappings"] + retry
        assert again["working_build"] is None
        assert again["working_candidates"] == 0
        assert _maintenance_outcome(pipeline) is expected_outcome, label
    pipeline.ready()
    reference = _fresh_reference(pipeline, "policy", policy=changed)
    if reference is not None:
        assert _publication_titles(pipeline.view()) == _publication_titles(reference)


@pytest.mark.parametrize(
    ("label", "component"),
    (
        ("publication.commit:BUILD_SELECTION", "artifact"),
        ("publication.commit:BUILD_SELECTION", "artifacts-required"),
        ("publication.commit:COMMIT_PUBLICATION", "operational"),
    ),
)
@pytest.mark.merge_smoke
def test_non_analysis_policy_change_after_crash_converges_in_one_turn(
    pipeline: Pipeline,
    label: str,
    component: str,
) -> None:
    """A successful takeover never publishes a candidate that froze an older
    non-analysis policy component.

    The three cases deliberately cover the two durable freeze points instead
    of multiplying every component by every crash label: artifact and
    artifacts-required freeze when the candidate begins, while operational
    policy freezes when its preparation is bound.  Display/title-sort share
    the candidate comparison exercised by the first two cases; their only
    currently supported algorithm version is already covered by registry and
    candidate contract tests.
    """

    _abandon_turn_before(pipeline, label)
    previous = _policy_facts(pipeline.config)
    base = ingest_policy()
    if component == "artifact":
        fingerprint = sha256(b"memory-library-policy-v2").digest()
        pipeline.library.policy_fingerprint_sha256 = fingerprint
        changed = replace(
            base,
            artifact=replace(
                base.artifact,
                policy_fingerprint_sha256=fingerprint,
            ),
        )
    elif component == "artifacts-required":
        changed = replace(base, artifacts_required=False)
    else:
        changed = replace(base, operational_max_batch_rows=64)

    receipts, _ = pipeline.turn(clock=takeover_clock(), policy=changed, drain=False)
    head = _policy_facts(pipeline.config)["head"]
    assert head is not None
    assert head["build_id"] == receipts.source.build_id
    assert head["build_id"] != previous["working_build"]
    assert head["complete_policy"] == (
        receipts.policy.manifest_policy_id,
        receipts.policy.artifact_policy_sha256,
        receipts.policy.display_title_policy_id,
        receipts.policy.title_sort_policy_id,
        receipts.policy.operational_policy_id,
        changed.artifacts_required,
    )
    pipeline.ready()


# --- bounded preparation drainage through the public facade ----------------


def _seed_superseded_preparations_of_the_working_build(
    config: CoreConfig, *, count: int
) -> bytes:
    """Seed ``count`` OPEN attempts of the current working build under the
    retained deletion generations 0 through count-1, every one superseded by
    the current head generation ``count`` that the queue facade advanced."""

    connector = open_connector(config)
    try:
        with connector.read_transaction():
            build = bytes(
                connector.fetch_one(
                    "SELECT build_id FROM operational_source_working_builds "
                    "WHERE slot = 1"
                )[0]
            )
            policy = int(
                connector.fetch_one(
                    "SELECT operational_policy_id FROM operational_operational_policys "
                    "ORDER BY operational_policy_id LIMIT 1"
                )[0]
            )
            head = int(
                connector.fetch_one(
                    "SELECT current_generation "
                    "FROM operational_deletion_request_generation_heads "
                    "WHERE singleton_id = 1"
                )[0]
            )
        assert head == count
        with connector.transaction():
            for generation in range(count):
                preparation_id = sha256(
                    b"seeded-superseded-attempt" + generation.to_bytes(8, "big")
                ).digest()[:16]
                connector.execute(
                    "INSERT INTO operational_operational_event_streams "
                    "(preparation_id, created_at) VALUES (%s, %s)",
                    (preparation_id, 1),
                )
                connector.execute(
                    "INSERT INTO operational_operational_preparations "
                    "(preparation_id, build_id, deletion_request_generation, "
                    "operational_policy_id, state, prepared_at, completed_at) "
                    "VALUES (%s, %s, %s, %s, 'OPEN', %s, NULL)",
                    (preparation_id, build, generation, policy, 1),
                )
    finally:
        connector.close()
    return build


def _preparations_of(config: CoreConfig, build_id: bytes) -> int:
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            return int(
                connector.fetch_one(
                    "SELECT COUNT(*) FROM operational_operational_preparations "
                    "WHERE build_id = %s",
                    (build_id,),
                )[0]
            )
    finally:
        connector.close()


def _advance_deletion_generation(pipeline: Pipeline, *, count: int) -> None:
    queue = VNextDownloadQueueFacade(pipeline.config, clock=Clock())
    for _ in range(count):
        queue.request_deletion(1003, url=None)


@pytest.mark.parametrize("count", (129, 257))
def test_superseded_preparations_drain_through_the_public_facade(
    pipeline: Pipeline,
    count: int,
) -> None:
    """The live build's superseded attempts drain through the public
    publication protocol: a turn that died before its operational preparation
    began, whose build then accumulated more than one page of superseded
    attempts, is taken over and emits exactly ceil(count/128) bounded
    ABANDON_SUPERSEDED commits before the single BEGIN_OPERATIONAL, publishes,
    and generic cleanup reclaims every abandoned attempt under READY."""

    pipeline.turn()
    pipeline.source.remove(("gallery-1003",))
    _abandon_turn_before(pipeline, "publication.commit:BEGIN_OPERATIONAL")
    _advance_deletion_generation(pipeline, count=count)
    build = _seed_superseded_preparations_of_the_working_build(
        pipeline.config, count=count
    )
    labels: list[str] = []
    facade = VNextIngestFacade(pipeline.config, clock=takeover_clock())
    try:
        run_ingest_turn(
            facade,
            source=pipeline.source,
            library=pipeline.library,
            boundary=labels.append,
        )
        drain_maintenance(facade)
    finally:
        facade.close()
    pages = [
        index
        for index, seen in enumerate(labels)
        if seen.endswith("ABANDON_SUPERSEDED")
    ]
    begins = [
        index
        for index, seen in enumerate(labels)
        if seen.endswith(":BEGIN_OPERATIONAL")
    ]
    assert len(pages) == -(-count // 128)
    assert len(begins) == 1 and begins[0] > pages[-1]
    # Only the committed attempt of this build survives cleanup.
    assert _preparations_of(pipeline.config, build) == 1
    pipeline.ready()
    view = pipeline.view()
    assert (view["revision"], _publication_gids(view)) == (2, [1001, 1002])
    reference = _fresh_reference(pipeline, "reference")
    if reference is not None:
        assert _publication_titles(view) == _publication_titles(reference)


@pytest.mark.parametrize("count", (129, 257))
def test_retiring_build_preparations_drain_through_the_public_facade(
    pipeline: Pipeline,
    count: int,
) -> None:
    """A retiring build's attempts drain through the public source protocol:
    a build sealed by a turn that died mid-analysis, then loaded with more
    than one page of attempts, is retired by a turn that scanned a changed
    snapshot through exactly ceil(count/128) bounded ROOT_HANDOFF commits, the
    last of which also reserves the new build; the new snapshot publishes and
    cleanup reclaims the retired build's attempts under READY."""

    pipeline.turn()
    pipeline.source.put(gallery(1006, pages=[b"p0-f"], artists=["frank"]))
    _abandon_turn_before(pipeline, "analysis.commit:content_owner")
    _advance_deletion_generation(pipeline, count=count)
    stale_build = _seed_superseded_preparations_of_the_working_build(
        pipeline.config, count=count
    )
    pipeline.source.remove(("gallery-1006",))
    pipeline.source.remove(("gallery-1003",))
    pipeline.source.put(gallery(1007, pages=[b"p0-g"], artists=["gina"]))
    labels: list[str] = []
    facade = VNextIngestFacade(pipeline.config, clock=takeover_clock())
    try:
        receipts = run_ingest_turn(
            facade,
            source=pipeline.source,
            library=pipeline.library,
            boundary=labels.append,
        )
        drain_maintenance(facade)
    finally:
        facade.close()
    # ceil(count / 128) handoff commits: each non-final page is its own
    # re-issued ROOT_HANDOFF; the final page drains the build and the same
    # transaction releases its roots and reserves the new build.
    assert labels.count("source.commit:ROOT_HANDOFF") == -(-count // 128)
    assert receipts.source.build_id != stale_build
    assert _stale_build_facts(pipeline.config) == {
        "working_builds": 0,
        "working_candidates": 0,
        "open_analyses": 0,
    }
    assert _preparations_of(pipeline.config, stale_build) == 0
    pipeline.ready()
    view = pipeline.view()
    assert (view["revision"], _publication_gids(view)) == (2, [1001, 1002, 1007])
    reference = _fresh_reference(pipeline, "reference")
    if reference is not None:
        assert _publication_titles(view) == _publication_titles(reference)
