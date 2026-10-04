"""Closed statement-fault and response-loss matrix over the production pipeline.

A dry run records every write transaction the public facades execute for one
ingest turn.  Every distinct transaction shape (its exact ordered mutation
statements) is then interrupted once before each of its statements and once
after its commit.  For each fault point the harness proves, against the real
database:

* rollback exactness: the database after a pre-mutation fault equals the
  committed state captured when the interrupted transaction began; and
* restart convergence: a fresh facade with expired-lease takeover replays the
  same turn to completion, the full production READY audit passes, and the
  public catalog and the reader-visible library equal the fault-free result.

Both native backends run the complete matrix from isolated prefix snapshots.
A separate real-clock case also samples later revisions after expired leases.
"""

from __future__ import annotations

import copy
import pickle
import time
from collections.abc import Callable, Sequence
from contextlib import closing
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

import pytest
from vnext_database_snapshot import (
    ReusableDatabaseSnapshot,
    clone_database,
    database_digest,
)
from vnext_fault_harness import (
    MAINTENANCE_GATE_TABLES,
    FaultInjector,
    FaultPoint,
    assert_exact_rollback,
    count_mutations,
    fault_points,
    run_fault_point,
    transaction_shapes,
)
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
from vnext_test_database import DatabaseFactory

from h2hdb import (
    CoreConfig,
    VNextDownloadQueueFacade,
    VNextIngestFacade,
)

BUCKETS = 8


def _fresh_corpus() -> list[Any]:
    return [
        gallery(
            1001,
            pages=[b"p0-a", b"p1-a"],
            artists=["alice"],
            extra_tags=[("female", "glasses")],
        ),
        gallery(1002, pages=[b"p0-b"], artists=["bob"], language="japanese"),
    ]


def _incremental_mutation(source: MemorySource) -> None:
    source.put(gallery(1001, pages=[b"p0-a", b"p1-a-modified"], artists=["alice"]))
    source.put(gallery(1003, pages=[b"p0-c"], artists=["carol"]))
    source.remove(("gallery-1002",))


def _cleanup_corpus() -> list[Any]:
    """A 300-page gallery whose removal leaves multi-batch cleanup work."""

    return [
        gallery(1001, pages=[b"p%03d" % index for index in range(300)]),
        gallery(1002, pages=[b"p0-b"], artists=["bob"], language="japanese"),
    ]


def _cleanup_mutation(source: MemorySource) -> None:
    source.remove(("gallery-1001",))
    source.put(gallery(1003, pages=[b"p0-c"], artists=["carol"]))


@dataclass
class Scenario:
    """One reproducible turn: a prefix that builds the starting state, then
    the target turn that the matrix interrupts."""

    name: str
    prefix_turns: int
    mutate: Callable[[MemorySource], None] | None
    deletion_request: bool = False
    corpus: Callable[[], list[Any]] = _fresh_corpus

    def build_prefix(self, config: CoreConfig) -> tuple[MemorySource, MemoryLibrary]:
        initialize_database(config)
        source = MemorySource(self.corpus())
        library = MemoryLibrary(source)
        for _ in range(self.prefix_turns):
            _turn(config, source, library, clock=Clock())
        if self.deletion_request:
            with closing(VNextDownloadQueueFacade(config, clock=Clock())) as queue:
                queue.request_deletion(1001)
        if self.mutate is not None:
            self.mutate(source)
        return source, library


SCENARIOS = {
    "fresh": Scenario("fresh", 0, None),
    "incremental": Scenario("incremental", 1, _incremental_mutation, True),
    # The removal of a 300-page gallery makes the target turn's maintenance
    # drain run multi-batch cleanup cycles (checkpoints, frozen roots, paired
    # child-first deletes) whose statements are interrupted like every other.
    "cleanup": Scenario("cleanup", 1, _cleanup_mutation, corpus=_cleanup_corpus),
}


def _turn(
    config: CoreConfig,
    source: MemorySource,
    library: MemoryLibrary,
    *,
    clock: Clock,
) -> None:
    facade = VNextIngestFacade(config, clock=clock)
    try:
        run_ingest_turn(facade, source=source, library=library)
        drain_maintenance(facade)
    finally:
        facade.close()


@dataclass(frozen=True)
class Reference:
    catalog: dict[str, Any]
    library: dict[str, Any]
    objects: dict[str, tuple[str, int]]


def _reference(
    config: CoreConfig,
    source: MemorySource,
    library: MemoryLibrary,
) -> Reference:
    return Reference(
        catalog_view(config), library_view(library), stored_objects(library)
    )


class _Baseline:
    """Restore the same immutable prefix before every complete fault replay.

    SQLite's native backup remains inexpensive. MariaDB reuses one owned
    target's DDL, rejecting schema drift before restoring every table. Public
    facades are still reopened for each fault and its restart; only fixture
    allocation changes, never mutation ordinals or the rollback/READY oracles.
    """

    def __init__(self, databases: DatabaseFactory, scenario: Scenario) -> None:
        self.databases = databases
        self.baseline = databases.config("fault-baseline")
        self.source, self.library = scenario.build_prefix(self.baseline)
        self._copies = 0
        self._names: dict[str, str] = {}
        self._reusable = (
            ReusableDatabaseSnapshot(
                databases, self.baseline, databases.config("fault-replay")
            )
            if databases.backend == "mariadb"
            else None
        )
        self._original_database = database_digest(self.baseline)
        self._original_adapters = self._adapter_digest()

    def _adapter_digest(self) -> bytes:
        # These memory adapters are trusted fixture objects, never external
        # pickles. Preservation assumes SHA-256 collision resistance, while
        # each fault's rollback oracle still compares exact database rows.
        return sha256(pickle.dumps((self.source, self.library))).digest()

    def assert_preserved(self) -> None:
        assert database_digest(self.baseline) == self._original_database
        assert self._adapter_digest() == self._original_adapters

    def fresh_copy(self) -> tuple[CoreConfig, MemorySource, MemoryLibrary]:
        if self._reusable is None:
            self._copies += 1
            name = f"point-{self._copies}"
            config = self.databases.config(name)
            self._names[config.database.database] = name
            clone_database(self.baseline, config)
        else:
            config = self._reusable.restore()
        source = copy.deepcopy(self.source)
        library = copy.deepcopy(self.library)
        library.source = source
        return config, source, library

    def discard(self, config: CoreConfig) -> None:
        if self._reusable is None:
            self.databases.release(self._names.pop(config.database.database))
        else:
            assert config is self._reusable.target
            self.databases.close_connections(config)


def _run_point(
    monkeypatch: pytest.MonkeyPatch,
    *,
    config: CoreConfig,
    source: MemorySource,
    library: MemoryLibrary,
    point: FaultPoint,
    reference: Reference,
) -> FaultInjector:
    """Interrupt the target turn at ``point``, then restart and converge."""

    injector, pre_transaction = run_fault_point(
        monkeypatch,
        config=config,
        point=point,
        workflow=lambda: _turn(config, source, library, clock=Clock()),
    )
    if point.kind == "before_mutation":
        assert_exact_rollback(
            config,
            pre_transaction,
            compensation_tables=MAINTENANCE_GATE_TABLES,
        )
    # Restart: a fresh facade whose clock is past every earlier lease takes the
    # expired authority over and replays from durable state only.
    try:
        _turn(config, source, library, clock=takeover_clock())
    except Exception as error:
        raise AssertionError(f"restart replay failed at {point}: {error!r}") from error
    assert full_check(config).state == "READY", point
    assert catalog_view(config) == reference.catalog, point
    assert library_view(library) == reference.library, point
    assert stored_objects(library) == reference.objects, point
    assert library.staging == {}, point
    return injector


def _points_for_bucket(
    points: Sequence[FaultPoint],
    bucket: int,
) -> tuple[FaultPoint, ...]:
    return tuple(point for point in points if point.shape_index % BUCKETS == bucket)


@pytest.mark.parametrize("scenario_name", sorted(SCENARIOS))
@pytest.mark.parametrize("bucket", range(BUCKETS))
def test_sqlite_every_transaction_shape_rolls_back_exactly_and_replays_to_the_same_catalog(
    database_factory: DatabaseFactory,
    monkeypatch: pytest.MonkeyPatch,
    scenario_name: str,
    bucket: int,
) -> None:
    scenario = SCENARIOS[scenario_name]
    baseline = _Baseline(database_factory, scenario)

    # Fault-free reference and the exact transaction record of the target turn.
    config, source, library = baseline.fresh_copy()
    dry_run = count_mutations(
        monkeypatch,
        lambda: _turn(config, source, library, clock=Clock()),
    )
    reference = _reference(config, source, library)
    assert full_check(config).state == "READY"
    baseline.discard(config)
    points = fault_points(dry_run)
    shapes = transaction_shapes(dry_run)
    assert len(shapes) >= 20, "the target turn must exercise many transaction shapes"
    assert {point.shape_index for point in points} == set(range(len(shapes)))
    selected = _points_for_bucket(points, bucket)
    assert selected, "every bucket must own at least one transaction shape"

    covered: set[tuple[str, int]] = set()
    for point in selected:
        config, source, library = baseline.fresh_copy()
        injector = _run_point(
            monkeypatch,
            config=config,
            source=source,
            library=library,
            point=point,
            reference=reference,
        )
        assert injector.fired == point.kind
        covered.add((point.kind, point.shape_index))
        baseline.discard(config)

    expected_shapes = {
        point.shape_index for point in points if point.shape_index % BUCKETS == bucket
    }
    assert {shape for _kind, shape in covered} == expected_shapes
    assert {kind for kind, _shape in covered} == {"before_mutation", "after_commit"}
    baseline.assert_preserved()


# Live MariaDB turns take several times longer than SQLite ones; the lease
# must outlive the interrupted turn and expire before the takeover.
SHORT_LEASE_MICROSECONDS = 45_000_000


def _short_lease_turn(
    config: CoreConfig, source: MemorySource, library: MemoryLibrary
) -> None:
    """One turn under a short lease so a later real-clock owner can take the
    interrupted authority over (a future clock would poison later real-time
    turns on the same database)."""

    facade = VNextIngestFacade(config, clock=Clock())
    try:
        session = claim_session(facade, lease=SHORT_LEASE_MICROSECONDS)
        run_ingest_turn(facade, source=source, library=library, session=session)
        drain_maintenance(facade)
    finally:
        facade.close()


def _sample_points(points: Sequence[FaultPoint], count: int) -> list[FaultPoint]:
    """A deterministic spread over transaction shapes, alternating kinds."""

    by_shape: dict[int, list[FaultPoint]] = {}
    for point in points:
        by_shape.setdefault(point.shape_index, []).append(point)
    shapes = sorted(by_shape)
    chosen: list[FaultPoint] = []
    for position in range(count):
        shape = shapes[(position * len(shapes)) // count]
        candidates = by_shape[shape]
        wanted = "after_commit" if position % 2 else "before_mutation"
        chosen.append(next((p for p in candidates if p.kind == wanted), candidates[0]))
    return chosen


def test_live_mariadb_sampled_faults_roll_back_exactly_and_converge(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Six sampled mutation ordinals (statement faults and lost commit
    responses) spread over the transaction shapes of one incremental revision,
    each injected into a later revision on live MariaDB.  Each interrupted
    revision proves exact rollback (row locks, CAS and rollback on InnoDB) and
    converges after an expired-lease takeover on the real clock."""

    initialize_database(db_config)
    source = MemorySource(_fresh_corpus())
    library = MemoryLibrary(source)
    _turn(db_config, source, library, clock=Clock())

    def revise(index: int) -> None:
        source.put(
            gallery(1001, pages=[b"p0-a", b"p1-a-%02d" % index], artists=["alice"])
        )

    revise(0)
    dry_run = count_mutations(
        monkeypatch, lambda: _turn(db_config, source, library, clock=Clock())
    )
    points = fault_points(dry_run)
    assert len(transaction_shapes(dry_run)) >= 20
    for index, point in enumerate(_sample_points(points, 6), start=1):
        revise(index)
        # A later revision's mutation ordinals are not identical to the dry
        # run's, so the interrupted transaction's pre-state is captured at
        # every transaction start; the sampled ordinal still lands inside one
        # real production transaction of the same turn.
        injector, pre_transaction = run_fault_point(
            monkeypatch,
            config=db_config,
            point=point,
            workflow=lambda: _short_lease_turn(db_config, source, library),
            capture_every_transaction=True,
        )
        assert injector.fired is not None, point
        if point.kind == "before_mutation":
            assert_exact_rollback(
                db_config,
                pre_transaction,
                compensation_tables=MAINTENANCE_GATE_TABLES,
            )
        time.sleep(SHORT_LEASE_MICROSECONDS / 1_000_000 + 0.5)
        _turn(db_config, source, library, clock=Clock())
        assert full_check(db_config).state == "READY", point
        assert catalog_view(db_config)["publication_count"] == len(source.galleries), (
            point
        )
        assert library.staging == {}, point
