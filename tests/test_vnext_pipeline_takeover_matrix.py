"""Lease-loss, takeover and generation-interference matrix over every fenced
mutation boundary of one production ingest turn.

A dry run records the label of every facade call that opens a fenced write
transaction (session claim, policy registration, each source action, each
analysis stage, each publication operation, completion and maintenance).  For
every distinct label the matrix then:

* ``takeover``: lets a second facade whose clock is past every lease take the
  expired maintenance gate and ingest generation over immediately before the
  original call, proves the original call fails closed with its typed
  authority error and writes nothing, and proves the new owner completes the
  same turn to the fault-free catalog with a READY audit; or
* ``deletion``: advances the deletion-request generation (a concurrent queue
  writer) immediately before the call and proves the turn still converges to
  the fault-free catalog while the publication commit consumed the new
  generation instead of a stale preparation.
"""

from __future__ import annotations

import copy
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from vnext_fault_harness import open_connector, snapshot_database, snapshot_difference
from vnext_pipeline import (
    Clock,
    MemoryLibrary,
    MemorySource,
    catalog_view,
    claim_session,
    drain_maintenance,
    full_check,
    gallery,
    initialize_database,
    library_view,
    run_ingest_turn,
    stored_objects,
    takeover_clock,
)

from h2hdb import (
    CoreConfig,
    DatabaseConfig,
    VNextDownloadQueueFacade,
    VNextIngestFacade,
    VNextIngestSession,
)
from h2hdb.vnext_analysis_repository import AnalysisNotReadyError
from h2hdb.vnext_artifact_preparation_repository import (
    ArtifactPreparationNotReadyError,
)
from h2hdb.vnext_canonical_value_repository import CanonicalValueNotReadyError
from h2hdb.vnext_cleanup_repository import CleanupUnavailableError
from h2hdb.vnext_download_ingest_repository import DownloadIngestUnavailableError
from h2hdb.vnext_gallery_staging_repository import GalleryStagingNotReadyError
from h2hdb.vnext_ingest_fence_repository import IngestFenceUnavailableError
from h2hdb.vnext_ingest_policy_repository import VNextIngestPolicyNotReadyError
from h2hdb.vnext_maintenance_gate_repository import MaintenanceGateUnavailableError
from h2hdb.vnext_operational_event_repository import OperationalEffectStateError
from h2hdb.vnext_publication_candidate_repository import (
    PublicationCandidateNotReadyError,
)
from h2hdb.vnext_publication_finalization_repository import (
    PublicationFinalizationUnavailableError,
)
from h2hdb.vnext_publication_repository import PublicationNotReadyError
from h2hdb.vnext_source_build_repository import SourceBuildNotReadyError
from h2hdb.vnext_transaction import StaleWriteError

FENCE_ERRORS: tuple[type[BaseException], ...] = (
    MaintenanceGateUnavailableError,
    IngestFenceUnavailableError,
    DownloadIngestUnavailableError,
    CanonicalValueNotReadyError,
    SourceBuildNotReadyError,
    GalleryStagingNotReadyError,
    AnalysisNotReadyError,
    PublicationCandidateNotReadyError,
    ArtifactPreparationNotReadyError,
    PublicationNotReadyError,
    PublicationFinalizationUnavailableError,
    OperationalEffectStateError,
    VNextIngestPolicyNotReadyError,
    CleanupUnavailableError,
    StaleWriteError,
)


@dataclass
class _Interference:
    """Fire one action at the k-th boundary; record the label and outcome."""

    target: str
    action: Callable[[], None]
    fired: bool = False
    seen: int = 0

    def __call__(self, label: str) -> None:
        if not self.fired and label == self.target:
            self.fired = True
            self.action()


def _corpus() -> list[Any]:
    return [
        gallery(
            1001,
            pages=[b"p0-a", b"p1-a"],
            artists=["alice"],
            extra_tags=[("female", "glasses")],
        ),
        gallery(1002, pages=[b"p0-b"], artists=["bob"], language="japanese"),
    ]


def _sqlite_config(path: Path) -> CoreConfig:
    return CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))


def _turn(
    config: CoreConfig,
    source: MemorySource,
    library: MemoryLibrary,
    *,
    clock: Clock,
    boundary: Callable[[str], None] | None = None,
    session: VNextIngestSession | None = None,
) -> None:
    facade = VNextIngestFacade(config, clock=clock)
    try:
        run_ingest_turn(
            facade,
            source=source,
            library=library,
            boundary=boundary,
            session=session,
        )
        drain_maintenance(facade, boundary=boundary)
    finally:
        facade.close()


def _boundary_labels(
    config: CoreConfig, source: MemorySource, library: MemoryLibrary
) -> list[str]:
    labels: list[str] = []
    _turn(config, source, library, clock=Clock(), boundary=labels.append)
    distinct: list[str] = []
    for label in labels:
        if label not in distinct:
            distinct.append(label)
    return distinct


class _Baseline:
    def __init__(self, tmp_path: Path) -> None:
        self.root = tmp_path
        self.baseline = tmp_path / "baseline.sqlite3"
        initialize_database(_sqlite_config(self.baseline))
        self.source = MemorySource(_corpus())
        self.library = MemoryLibrary(self.source)
        self._copies = 0

    def fresh_copy(self) -> tuple[CoreConfig, MemorySource, MemoryLibrary]:
        self._copies += 1
        path = self.root / f"point-{self._copies}.sqlite3"
        shutil.copyfile(self.baseline, path)
        source = copy.deepcopy(self.source)
        library = copy.deepcopy(self.library)
        library.source = source
        return _sqlite_config(path), source, library


@dataclass(frozen=True)
class _Reference:
    catalog: dict[str, Any]
    library: dict[str, Any]
    objects: dict[str, tuple[str, int]]


def _reference(baseline: _Baseline) -> _Reference:
    config, source, library = baseline.fresh_copy()
    _turn(config, source, library, clock=Clock())
    assert full_check(config).state == "READY"
    return _Reference(
        catalog_view(config), library_view(library), stored_objects(library)
    )


def _assert_converged(
    config: CoreConfig,
    library: MemoryLibrary,
    reference: _Reference,
    label: str,
) -> None:
    assert full_check(config).state == "READY", label
    assert catalog_view(config) == reference.catalog, label
    assert library_view(library) == reference.library, label
    assert stored_objects(library) == reference.objects, label
    assert library.staging == {}, label


def _takeover_point(
    config: CoreConfig,
    source: MemorySource,
    library: MemoryLibrary,
    label: str,
    reference: _Reference,
) -> type[BaseException] | None:
    """Take authority over right before ``label``; the original call must
    fail closed with zero writes and the new owner must converge."""

    taker = VNextIngestFacade(config, clock=takeover_clock())
    state: dict[str, Any] = {}

    def take_over() -> None:
        # The taker may legitimately reclaim the abandoned generation's
        # transient rows while claiming; the fenced original call is measured
        # against the committed state right after that.
        state["session"] = claim_session(taker)
        state["before"] = snapshot_database(config)

    hook = _Interference(label, take_over)
    observed: type[BaseException] | None = None
    try:
        try:
            _turn(config, source, library, clock=Clock(), boundary=hook)
        except FENCE_ERRORS as error:
            observed = type(error)
        except RuntimeError as error:
            # The session claim itself is the boundary: the original sees
            # ordinary contention (no session) rather than a fenced error.
            # At the maintenance boundary the original's drain reports the
            # non-progress outcome (the new owner's live SHARED lease).
            contention = (
                label == "claim" and "session was not available" in str(error)
            ) or (label == "maintenance" and "did not progress" in str(error))
            if not contention:
                raise
            observed = RuntimeError
        assert hook.fired, f"boundary {label!r} never occurred"
        if label == "maintenance":
            # A live SHARED owner blocks destructive maintenance instead of
            # raising; the original loop reports the non-progress outcome.
            assert observed in {None, RuntimeError}
        else:
            assert observed is not None, (
                f"original call at {label!r} succeeded after its authority was taken over"
            )
        after = snapshot_database(config)
        leaked = snapshot_difference(state["before"], after)
        assert leaked == {}, f"fenced call at {label!r} wrote rows: {leaked}"
        # The new owner completes the very same turn from durable state.
        try:
            run_ingest_turn(
                taker,
                source=source,
                library=library,
                session=state["session"],
            )
            drain_maintenance(taker)
        except Exception as error:
            raise AssertionError(
                f"new owner could not complete after takeover at {label!r}: "
                f"{type(error).__name__}: {error}"
            ) from error
    finally:
        taker.close()
    _assert_converged(config, library, reference, label)
    return observed


def _deletion_point(
    config: CoreConfig,
    source: MemorySource,
    library: MemoryLibrary,
    label: str,
    reference: _Reference,
) -> None:
    """Advance the deletion-request generation right before ``label``."""

    queue = VNextDownloadQueueFacade(config, clock=Clock())
    receipts: list[Any] = []

    def request_deletion() -> None:
        receipts.append(queue.request_deletion(1001, url=None))

    hook = _Interference(label, request_deletion)
    facade = VNextIngestFacade(config, clock=Clock())
    try:
        session = claim_session(facade)
        try:
            run_ingest_turn(
                facade,
                source=source,
                library=library,
                session=session,
                boundary=hook,
            )
        except FENCE_ERRORS:
            # A stale preparation or generation is rejected closed; the same
            # session re-issues from durable state and converges.
            run_ingest_turn(
                facade,
                source=source,
                library=library,
                session=session,
            )
        drain_maintenance(facade)
    finally:
        facade.close()
    assert hook.fired, f"boundary {label!r} never occurred"
    (receipt,) = receipts
    _assert_converged(config, library, reference, label)
    consumed, head, commits = _deletion_facts(config)
    assert head == (receipt.observed_generation,), label
    if consumed:
        # The request landed before the publication commit: that commit
        # consumed the advanced generation exactly, never a stale preparation.
        assert consumed == [(receipt.request_token,)], label
        assert commits == [(receipt.observed_generation,)], label
        return
    # The request landed after the commit: it stays pending, the commit
    # retains the generation it scanned, and the next publication consumes it.
    assert commits == [(receipt.observed_generation - 1,)], label
    source.put(gallery(1003, pages=[b"p0-c"], artists=["carol"]))
    later = VNextIngestFacade(config, clock=Clock())
    try:
        run_ingest_turn(later, source=source, library=library)
        drain_maintenance(later)
    finally:
        later.close()
    assert full_check(config).state == "READY", label
    consumed, head, commits = _deletion_facts(config)
    assert consumed == [(receipt.request_token,)], label
    assert head == (receipt.observed_generation,), label
    assert sorted(commits) == [
        (receipt.observed_generation - 1,),
        (receipt.observed_generation,),
    ], label


def _deletion_facts(
    config: CoreConfig,
) -> tuple[list[Any], tuple[Any, ...] | None, list[Any]]:
    connector = open_connector(config)
    try:
        with connector.read_transaction():
            consumed = connector.fetch_all(
                "SELECT deletion_request_token FROM "
                "operational_operational_deletion_consumption_events"
            )
            head = connector.fetch_one(
                "SELECT current_generation "
                "FROM operational_deletion_request_generation_heads"
            )
            commits = connector.fetch_all(
                "SELECT preparation.deletion_request_generation "
                "FROM catalog_publication_commits AS published "
                "JOIN operational_operational_preparations AS preparation "
                "ON preparation.preparation_id = published.preparation_id"
            )
    finally:
        connector.close()
    return consumed, head, commits


BUCKETS = 6


@pytest.mark.parametrize("mode", ("takeover", "deletion"))
@pytest.mark.parametrize("bucket", range(BUCKETS))
def test_sqlite_every_fenced_boundary_fails_closed_under_takeover_and_generation_interference(
    tmp_path: Path,
    mode: str,
    bucket: int,
) -> None:
    baseline = _Baseline(tmp_path)
    config, source, library = baseline.fresh_copy()
    labels = _boundary_labels(config, source, library)
    assert "claim" in labels and "complete" in labels and "maintenance" in labels
    assert any(label.startswith("source.commit:") for label in labels)
    assert any(label.startswith("analysis.commit:") for label in labels)
    assert any(label.startswith("publication.commit:") for label in labels)
    assert len(labels) >= 30
    reference = _reference(baseline)

    selected = [
        label for index, label in enumerate(labels) if index % BUCKETS == bucket
    ]
    assert selected
    observed: dict[str, str | None] = {}
    for label in selected:
        config, source, library = baseline.fresh_copy()
        if mode == "takeover":
            error = _takeover_point(config, source, library, label, reference)
            observed[label] = None if error is None else error.__name__
        else:
            if label in {"claim", "maintenance"}:
                continue
            _deletion_point(config, source, library, label, reference)
            observed[label] = "converged"
    assert observed
    if mode == "takeover":
        fenced = {
            label
            for label, name in observed.items()
            if name not in {None, "RuntimeError"}
        }
        assert fenced == {
            label for label in observed if label not in {"claim", "maintenance"}
        }
