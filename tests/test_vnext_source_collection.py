"""First-scan recovery retains exact sealed galleries before any source cut exists."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import replace
from hashlib import sha256
from threading import Lock
from typing import Any

import pytest
from test_vnext_source_batches import _source_batch_clock
from test_vnext_source_marker import MarkerSource
from vnext_fault_harness import (
    FaultInjector,
    InjectedFault,
    fault_injection,
    open_connector,
)
from vnext_pipeline import (
    MemorySource,
    claim_session,
    full_check,
    gallery,
    ingest_policy,
    initialize_database,
)

from h2hdb import (
    CoreConfig,
    VNextIngestFacade,
    VNextIngestGalleryObservation,
    VNextSourceManifestMismatchError,
    VNextSourcePreparationProgress,
)
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_canonical_value_repository import CanonicalValueUploadPlan
from h2hdb.vnext_source_build_repository import SourceDiscoveryPlan
from h2hdb.vnext_source_observation_spool import FrozenSourceObservationSpool


def _rows(config: CoreConfig, query: str) -> list[tuple[Any, ...]]:
    with closing(open_connector(config)) as connector, connector.read_transaction():
        return connector.fetch_all(query)


def _finish_source(
    facade: VNextIngestFacade, session: Any, policy: Any, source: MemorySource
) -> bytes:
    with facade.prepare_source(source, policy=policy) as prepared:
        for _ in range(2000):
            issued = facade.issue_source_step(session, policy, prepared)
            local = facade.prepare_source_step(prepared, issued)
            result = facade.commit_source_step(session, local)
            if result.terminal:
                assert (
                    result.source_receipt is not None and result.source_receipt.sealed
                )
                return result.source_receipt.build_id
    raise AssertionError("source collection did not complete")


@pytest.mark.parametrize("manifest_mismatch", (False, True))
def test_source_local_io_and_callbacks_remain_outside_session_lock_and_transactions(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    manifest_mismatch: bool,
) -> None:
    initialize_database(db_config)
    session_lock = Lock()
    active_transactions = 0
    local_calls: Counter[str] = Counter()
    callback_boundaries: list[tuple[bool, int]] = []

    def instrument_transaction(name: str) -> None:
        original = getattr(SQLConnector, name)

        @contextmanager
        def transaction(connector: SQLConnector) -> Iterator[None]:
            nonlocal active_transactions
            with original(connector):
                active_transactions += 1
                try:
                    yield
                finally:
                    active_transactions -= 1

        monkeypatch.setattr(SQLConnector, name, transaction)

    def instrument_local(owner: Any, name: str) -> None:
        original = getattr(owner, name)
        label = f"{owner.__name__}.{name}"

        def local(*args: Any, **kwargs: Any) -> Any:
            assert not session_lock.locked(), label
            assert active_transactions == 0, label
            local_calls[label] += 1
            return original(*args, **kwargs)

        monkeypatch.setattr(owner, name, local)

    def progress(_progress: VNextSourcePreparationProgress) -> None:
        # Progress observers intentionally suppress ordinary errors, so record
        # the boundaries and assert afterward rather than inside the callback.
        callback_boundaries.append((session_lock.locked(), active_transactions))

    for name in ("transaction", "read_transaction"):
        instrument_transaction(name)
    instrument_local(SourceDiscoveryPlan, "_page")
    instrument_local(SourceDiscoveryPlan, "close")
    instrument_local(FrozenSourceObservationSpool, "record_sealed_observation")
    instrument_local(FrozenSourceObservationSpool, "close")
    instrument_local(CanonicalValueUploadPlan, "close")
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        source = MarkerSource((gallery(1001), gallery(1002)))
        with facade.prepare_source(source, policy=policy, progress=progress) as cut:
            changed = False
            for _step in range(2000):
                with session_lock:
                    issued = facade.issue_source_step(session, policy, cut)
                prepared = facade.prepare_source_step(cut, issued)
                try:
                    with session_lock:
                        result = facade.commit_source_step(session, prepared)
                except VNextSourceManifestMismatchError:
                    assert manifest_mismatch and changed
                    assert not cut._closed
                    assert cut._snapshot._directory.exists()
                    with session_lock:
                        with pytest.raises(ValueError, match="has failed"):
                            facade.issue_source_step(session, policy, cut)
                    break
                if result.terminal:
                    assert not manifest_mismatch
                    break
                if manifest_mismatch and not changed and cut.observation_complete:
                    cut._manifest_summary = replace(
                        cut._manifest_summary,
                        manifest_sha256=sha256(b"force-terminal-mismatch").digest(),
                    )
                    changed = True
            else:
                pytest.fail("source never reached a terminal result")
        assert cut._closed
        assert not cut._snapshot._directory.exists()
        facade.complete_ingest(session)
    assert callback_boundaries and set(callback_boundaries) == {(False, 0)}
    assert local_calls["SourceDiscoveryPlan._page"] > 0
    assert local_calls["FrozenSourceObservationSpool.record_sealed_observation"] == 2
    assert local_calls["FrozenSourceObservationSpool.close"] == 1


def test_failed_prepare_cannot_retry_same_issue_as_a_partial_source_seal(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    initialize_database(db_config)
    source = MarkerSource((gallery(1001), gallery(1002), gallery(1003)))
    original = source.observe_gallery

    def observe(locator: tuple[str, ...]) -> VNextIngestGalleryObservation:
        if len(source.deep_reads) == 1:
            raise OSError("second gallery could not be read")
        return original(locator)

    monkeypatch.setattr(source, "observe_gallery", observe)
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        with facade.prepare_source(source, policy=policy) as cut:
            for _step in range(2000):
                issued = facade.issue_source_step(session, policy, cut)
                try:
                    prepared = facade.prepare_source_step(cut, issued)
                except OSError:
                    break
                facade.commit_source_step(session, prepared)
            else:
                pytest.fail("source did not attempt its second gallery")
            assert not cut._snapshot.complete and not cut._closed
            for _retry in range(2):
                with pytest.raises(ValueError, match="has failed"):
                    facade.prepare_source_step(cut, issued)
                with pytest.raises(ValueError, match="has failed"):
                    facade.issue_source_step(session, policy, cut)
            assert _rows(
                db_config, "SELECT COUNT(*) FROM catalog_source_collection_observations"
            ) == [(1,)]
            assert (
                _rows(db_config, "SELECT build_id FROM catalog_source_build_descriptor")
                == []
            )
        assert cut._closed
        facade.complete_ingest(session)
    assert full_check(db_config).state == "READY"


@pytest.mark.parametrize(
    ("boundary", "source_change"),
    [
        pytest.param("sealed_gallery", "unchanged", marks=pytest.mark.mariadb_smoke),
        ("sealed_gallery", "changed"),
        ("sealed_gallery", "deleted"),
        ("partial_next_gallery", "unchanged"),
        ("partial_next_gallery", "changed"),
        ("partial_next_gallery", "deleted"),
    ],
)
def test_first_scan_restart_reuses_sealed_gallery_and_redoes_only_unsealed_work(
    db_config: CoreConfig,
    boundary: str,
    source_change: str,
) -> None:
    initialize_database(db_config)
    values = (gallery(1001), gallery(1002))
    source = MarkerSource(values)
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        with facade.prepare_source(source, policy=policy) as prepared:
            for _ in range(2000):
                issued = facade.issue_source_step(session, policy, prepared)
                local = facade.prepare_source_step(prepared, issued)
                facade.commit_source_step(session, local)
                members = _rows(
                    db_config,
                    "SELECT gallery_id, observation_id FROM catalog_source_collection_observations",
                )
                pending = _rows(
                    db_config,
                    "SELECT state FROM operational_gallery_observation_stagings",
                )
                if boundary == "sealed_gallery" and len(members) == 1 and not pending:
                    break
                if (
                    boundary == "partial_next_gallery"
                    and len(members) == 1
                    and len(source.deep_reads) == 2
                    and local._action.value == "FILE_PAGE"
                ):
                    break
            else:
                pytest.fail("fixture did not reach first-scan interruption boundary")
            retained = tuple(members)
        assert (
            _rows(db_config, "SELECT build_id FROM catalog_source_build_descriptor")
            == []
        )
        # This unit case closes the lease to obtain another real generation;
        # ingest process tests separately exercise lease expiry after SIGKILL.
        facade.complete_ingest(session)
    assert len(retained) == 1
    first = source.deep_reads[0]
    next_locator = next(value.locator for value in values if value.locator != first)
    if source_change == "changed":
        value = source.get(next_locator)
        source.put(replace(value, modified_time=value.modified_time + 1))
    elif source_change == "deleted":
        source.remove(next_locator)
    source.deep_reads.clear()
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        build = _finish_source(facade, session, policy, source)
        facade.complete_ingest(session)
    assert Counter(source.deep_reads) == (
        Counter() if source_change == "deleted" else Counter({next_locator: 1})
    )
    members = _rows(
        db_config,
        "SELECT gallery_id, observation_id FROM catalog_source_collection_observations",
    )
    assert set(retained) <= set(members)
    assert (
        _rows(
            db_config,
            "SELECT collection_id FROM operational_source_working_collections",
        )
        == []
    )
    assert _rows(
        db_config, "SELECT build_id FROM catalog_source_collection_consumptions"
    ) == [(build,)]
    assert full_check(db_config).state == "READY"


def test_collection_without_markers_stages_once_and_attaches_exact_sealed_receipt(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initialize_database(db_config)
    source = MemorySource((gallery(1001), gallery(1002)))
    injector = FaultInjector()
    with (
        fault_injection(monkeypatch, injector),
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        build = _finish_source(facade, session, policy, source)
        facade.complete_ingest(session)
    staging_headers = [
        sql
        for sql in injector.statements
        if " ".join(sql.split()).startswith(
            "INSERT INTO operational_gallery_observation_stagings "
        )
    ]
    assert len(staging_headers) == 2, [
        sql for sql in injector.statements if "stagings" in sql
    ]
    assert _rows(
        db_config,
        "SELECT gallery_id, observation_id FROM catalog_gallery_observations ORDER BY gallery_id, observation_id",
    ) == _rows(
        db_config,
        "SELECT gallery_id, observation_id FROM catalog_source_build_galleries ORDER BY gallery_id, observation_id",
    )
    assert _rows(
        db_config, "SELECT build_id FROM catalog_source_collection_consumptions"
    ) == [(build,)]
    assert full_check(db_config).state == "READY"


@pytest.mark.parametrize("fault", ["before_consumption", "after_commit"])
def test_source_seal_and_collection_consumption_are_one_atomic_transition(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    initialize_database(db_config)
    source = MarkerSource((gallery(1001),))
    injector = FaultInjector()
    with (
        fault_injection(monkeypatch, injector),
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        with facade.prepare_source(source, policy=policy) as prepared:
            for _ in range(2000):
                issued = facade.issue_source_step(session, policy, prepared)
                local = facade.prepare_source_step(prepared, issued)
                if local._action.value == "ASSEMBLY" and _rows(
                    db_config,
                    "SELECT processed_gallery_count FROM operational_source_build_assembly_checkpoints",
                ) == [(1,)]:
                    break
                facade.commit_source_step(session, local)
            else:
                pytest.fail("fixture did not reach terminal assembly")

            def interrupt_consumption(sql: str) -> None:
                if " ".join(sql.split()).startswith(
                    "INSERT INTO catalog_source_collection_consumptions "
                ):
                    if fault == "before_consumption":
                        raise InjectedFault("before collection consumption")
                    injector.fail_after_commit = injector.commits + 1

            injector.on_before_mutation = interrupt_consumption
            with pytest.raises(InjectedFault):
                facade.commit_source_step(session, local)
            injector.on_before_mutation = None
            assert _rows(
                db_config, "SELECT state FROM catalog_source_build_states"
            ) == [("OPEN" if fault == "before_consumption" else "SEALED",)]
            assert _rows(
                db_config, "SELECT state FROM operational_source_collection_states"
            ) == [("OPEN" if fault == "before_consumption" else "CONSUMED",)]
            result = facade.commit_source_step(session, local)
            assert result.terminal and result.source_receipt is not None
            assert result.replayed == (fault == "after_commit")
        facade.complete_ingest(session)
    assert (
        _rows(
            db_config,
            "SELECT collection_id FROM operational_source_working_collections",
        )
        == []
    )
    assert full_check(db_config).state == "READY"


def test_collection_root_response_loss_reuses_exact_singleton_without_new_writes(
    db_config: CoreConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initialize_database(db_config)
    source = MarkerSource((gallery(1001),))
    injector = FaultInjector()
    with (
        fault_injection(monkeypatch, injector),
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        with facade.prepare_source(source, policy=policy) as prepared:

            def lose_root_response(sql: str) -> None:
                if " ".join(sql.split()).startswith(
                    "INSERT INTO catalog_source_collections "
                ):
                    injector.fail_after_commit = injector.commits + 1

            injector.on_before_mutation = lose_root_response
            for _ in range(100):
                issued = facade.issue_source_step(session, policy, prepared)
                local = facade.prepare_source_step(prepared, issued)
                try:
                    facade.commit_source_step(session, local)
                except InjectedFault:
                    assert local._action.value == "ROOT_HANDOFF"
                    break
            else:
                pytest.fail("collection root commit response was not lost")
            injector.on_before_mutation = None
            before = injector.mutations
            result = facade.commit_source_step(session, local)
            assert not result.terminal
            assert injector.mutations == before
            assert (
                len(
                    _rows(
                        db_config,
                        "SELECT collection_id FROM catalog_source_collections",
                    )
                )
                == 1
            )
        facade.complete_ingest(session)
    assert full_check(db_config).state == "READY"


@pytest.mark.parametrize("change", ["root", "qualification"])
def test_replacement_collection_does_not_adopt_incompatible_checkpoint(
    db_config: CoreConfig,
    change: str,
) -> None:
    initialize_database(db_config)
    source = MarkerSource((gallery(1001), gallery(1002)))
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        with facade.prepare_source(source, policy=policy) as prepared:
            for _ in range(2000):
                local = facade.prepare_source_step(
                    prepared, facade.issue_source_step(session, policy, prepared)
                )
                facade.commit_source_step(session, local)
                if len(source.deep_reads) == 2 and local._action.value == "FILE_PAGE":
                    break
            else:
                pytest.fail("fixture did not persist partial second gallery")
        facade.complete_ingest(session)
    first_collection = _rows(
        db_config, "SELECT collection_id FROM catalog_source_collections"
    )[0][0]
    if change == "root":
        source._root = ("replacement", "source")
    source.deep_reads.clear()
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(
            session, ingest_policy(artifacts_required=change != "qualification")
        )
        _finish_source(facade, session, policy, source)
        facade.complete_ingest(session)
    assert Counter(source.deep_reads) == Counter(
        value.locator for value in source.galleries
    )
    states = dict(
        _rows(
            db_config,
            "SELECT collection_id, state FROM operational_source_collection_states",
        )
    )
    assert states[first_collection] == "ABANDONED"
    assert Counter(states.values()) == Counter({"ABANDONED": 1, "CONSUMED": 1})
    assert (
        _rows(db_config, "SELECT state FROM operational_gallery_observation_stagings")
        == []
    )
    assert full_check(db_config).state == "READY"


@pytest.mark.parametrize("change", ["root", "qualification"])
def test_same_generation_cannot_replace_collection_source_or_policy(
    db_config: CoreConfig,
    change: str,
) -> None:
    from h2hdb.vnext_source_collection_repository import SourceCollectionConflictError

    initialize_database(db_config)
    source = MarkerSource((gallery(1001),))
    with (
        _source_batch_clock(db_config) as clock,
        VNextIngestFacade(db_config, clock=clock) as facade,
    ):
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy())
        with facade.prepare_source(source, policy=policy) as prepared:
            for _ in range(100):
                local = facade.prepare_source_step(
                    prepared, facade.issue_source_step(session, policy, prepared)
                )
                facade.commit_source_step(session, local)
                if local._action.value == "ROOT_HANDOFF":
                    break
            else:
                pytest.fail("fixture did not create a source collection")
        original = _rows(
            db_config,
            "SELECT collection_id, state FROM operational_source_collection_states",
        )
        if change == "root":
            source._root = ("another", "root")
        replacement = facade.ensure_policy(
            session, ingest_policy(artifacts_required=change != "qualification")
        )
        with facade.prepare_source(source, policy=replacement) as prepared:
            with pytest.raises(
                SourceCollectionConflictError,
                match="one ingest generation cannot switch",
            ):
                for _ in range(100):
                    local = facade.prepare_source_step(
                        prepared,
                        facade.issue_source_step(session, replacement, prepared),
                    )
                    facade.commit_source_step(session, local)
        assert (
            _rows(
                db_config,
                "SELECT collection_id, state FROM operational_source_collection_states",
            )
            == original
        )
        assert source.deep_reads == []
        facade.complete_ingest(session)
    assert full_check(db_config).state == "READY"
