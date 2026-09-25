"""Bounded filesystem and hash-count retirement with real gates, SQL and checkpoints.

The partial graphs omit unrelated source/identity families during setup, then
restore foreign-key enforcement before cleanup. They are finite SQL-cost and
transaction checks; they do not claim a complete READY audit or NAS wall time.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from typing import Any, Literal

import pytest
from vnext_catalog_identity_fixtures import seed_gallery_observation_file
from vnext_fault_harness import backend_of, open_connector
from vnext_pipeline import initialize_database
from vnext_publication_cleanup_fixtures import partial_publication_setup

import h2hdb.vnext_cleanup_repository as cleanup
from h2hdb import CoreConfig
from h2hdb.sql_connector import SQLConnector
from h2hdb.sql_performance import instrument_connector, measure_sql
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_maintenance_gate_repository import GateLease, MaintenanceGateRepository
from h2hdb.vnext_transaction import VNextUnitOfWork

pytestmark = [pytest.mark.cleanup_acceptance, pytest.mark.deep]

_GALLERY = 23
_PHASES = {
    "GO_FACTS": ("file_hash_occurrences",),
    "GO_FILESYSTEM_SEAL": ("seals",),
    "GO_FILESYSTEM_VALUES": (
        "devices",
        "inodes",
        "modified_nses",
        "changed_nses",
    ),
    "GO_FILESYSTEM_ANCHOR": ("anchors",),
}
_VALUES = (
    ("devices", "device"),
    ("inodes", "inode"),
    ("modified_nses", "modified_ns"),
    ("changed_nses", "changed_ns"),
)
_PREFIX = "catalog_gallery_observation_file_filesystem_"


@dataclass
class _Counter:
    queries: list[str] = field(default_factory=list)

    def record_sql_operation(
        self,
        category: Literal["sql", "connection", "transaction"],
        _elapsed: float,
        query: str,
        _rows: int,
    ) -> None:
        if category == "sql":
            self.queries.append(" ".join(query.split()))


@dataclass(frozen=True)
class _Sample:
    phase: str
    keys: tuple[bytes, ...]
    queries: tuple[str, ...]


def _seed_observation(connector: SQLConnector, *, observation: int, files: int) -> None:
    connector.execute(
        "INSERT INTO catalog_gallery_observation_allocations "
        "(gallery_id, observation_id, allocated_at) VALUES (%s, %s, 0)",
        (_GALLERY, observation),
    )
    for position in range(files):
        file_key = (position + 1).to_bytes(32, "big")
        seed_gallery_observation_file(
            connector,
            gallery_id=_GALLERY,
            observation_id=observation,
            file_no=position,
            file_key=file_key,
            file_sha256=file_key,
        )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_file_hash_occurrences "
            "(gallery_id, observation_id, file_sha256, occurrence_count) VALUES (%s, %s, %s, 1)",
            (_GALLERY, observation, file_key),
        )
        key = (_GALLERY, observation, file_key)
        connector.execute(
            f"INSERT INTO {_PREFIX}anchors "
            "(gallery_id, observation_id, file_key) VALUES (%s, %s, %s)",
            key,
        )
        for suffix, column in _VALUES:
            connector.execute(
                f"INSERT INTO {_PREFIX}{suffix} "
                f"(gallery_id, observation_id, file_key, {column}) "
                "VALUES (%s, %s, %s, %s)",
                (*key, (position + 1).to_bytes(8, "big")),
            )
        connector.execute(
            f"INSERT INTO {_PREFIX}seals "
            "(gallery_id, observation_id, file_key) VALUES (%s, %s, %s)",
            key,
        )


def _begin(
    config: CoreConfig, gate: GateLease, *, now: int = 2
) -> cleanup.CleanupCycle:
    with closing(open_connector(config)) as connector, connector.transaction():
        return cleanup.VNextCleanupRepository.begin_cycle(
            VNextUnitOfWork(connector, backend=backend_of(config)),
            gate_lease=gate,
            target_kind=cleanup.CleanupTargetKind.GALLERY_OBSERVATION,
            shard_no=_GALLERY,
            cycle_cutoff_at=100,
            max_rows_per_transaction=256,
            now=now,
        )


def _seed(config: CoreConfig, files: int) -> tuple[GateLease, cleanup.CleanupCycle]:
    initialize_database(config)
    backend = backend_of(config)
    with closing(open_connector(config)) as connector:
        with partial_publication_setup(connector, backend=backend):
            for observation in (1, 2):
                _seed_observation(connector, observation=observation, files=files)
            # The second observation is a retained sibling of the same gallery.
            connector.execute(
                "INSERT INTO catalog_source_build_galleries "
                "(build_id, gallery_id, observation_id) VALUES (%s, %s, 2)",
                (b"b" * 16, _GALLERY),
            )
        with connector.transaction():
            gate = MaintenanceGateRepository.claim_exclusive(
                VNextUnitOfWork(connector, backend=backend),
                now=1,
                lease_duration=100_000,
            )
    return gate, _begin(config, gate)


def _drain(
    config: CoreConfig,
    gate: GateLease,
    cycle: cleanup.CleanupCycle,
    monkeypatch: pytest.MonkeyPatch,
    *,
    files: int,
    now: int = 3,
) -> tuple[_Sample, ...]:
    samples: list[_Sample] = []
    original = cleanup._run_static_phase

    def observed(
        operation: cleanup._CleanupOperation,
        cursor: bytes,
        plan: cleanup._StaticTargetPlan,
        phase: str,
        **kwargs: Any,
    ) -> cleanup._Mutation:
        if phase not in _PHASES:
            return original(operation, cursor, plan, phase, **kwargs)
        counter = _Counter()
        with measure_sql(counter, observe_nested=True):
            result = original(operation, cursor, plan, phase, **kwargs)
        samples.append(_Sample(phase, result.row_keys, tuple(counter.queries)))
        return result

    all_calls = _Counter()
    with monkeypatch.context() as patch, measure_sql(all_calls, observe_nested=True):
        patch.setattr(cleanup, "_run_static_phase", observed)
        with closing(open_connector(config)) as raw:
            if isinstance(raw, SQLiteConnector):
                raw.connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999)
            connector = instrument_connector(raw)
            for step in range(64):
                with connector.transaction():
                    result = cleanup.VNextCleanupRepository.advance_current_only_cycle(
                        VNextUnitOfWork(connector, backend=backend_of(config)),
                        gate_lease=gate,
                        cycle=cycle,
                        now=now + step,
                    )
                if result[-1].cycle_complete:
                    break
            else:
                pytest.fail("observation cleanup did not finish within 64 calls")
            for suffix in ("anchors", *dict(_VALUES), "seals"):
                assert connector.fetch_one(
                    f"SELECT COUNT(*) FROM {_PREFIX}{suffix} "
                    "WHERE gallery_id = %s AND observation_id = 1",
                    (_GALLERY,),
                ) == (0,)
                assert connector.fetch_one(
                    f"SELECT COUNT(*) FROM {_PREFIX}{suffix} "
                    "WHERE gallery_id = %s AND observation_id = 2",
                    (_GALLERY,),
                ) == (files,)
            assert connector.fetch_all(
                "SELECT observation_id, COUNT(*), SUM(occurrence_count) "
                "FROM catalog_gallery_observation_file_hash_occurrences "
                "WHERE gallery_id = %s GROUP BY observation_id ORDER BY observation_id",
                (_GALLERY,),
            ) == [(2, files, files)]
            assert connector.fetch_all(
                "SELECT observation_id FROM catalog_gallery_observation_allocations "
                "WHERE gallery_id = %s ORDER BY observation_id",
                (_GALLERY,),
            ) == [(2,)]
    assert 0 < sum(len(sample.queries) for sample in samples) < len(all_calls.queries)
    return tuple(samples)


def _assert_costs(samples: tuple[_Sample, ...], *, files: int) -> None:
    for phase, relations in _PHASES.items():
        selected = tuple(sample for sample in samples if sample.phase == phase)
        keys = tuple(key for sample in selected for key in sample.keys)
        expected = files * len(relations)
        assert len(keys) == len(set(keys)) == expected
        assert all(len(sample.keys) <= 256 for sample in selected)
        assert selected[-1].keys == ()
        queries = tuple(query for sample in selected for query in sample.queries)
        # The pre-existing retirement acceptance contract, fixed before this
        # optimization: two statements per 64 rows plus 16 per transaction and
        # terminal verification. Do not fit this budget to measured samples.
        ceiling = 2 * ((expected + 63) // 64) + 16 * ((expected + 255) // 256 + 1)
        assert len(queries) <= ceiling, "SQL cost: filesystem retirement budget"
        assert all(query.count("%s") <= 900 for query in queries)
        deletes = [query for query in queries if query.startswith("DELETE ")]
        locks = [query for query in queries if "AS requested ON" in query]
        assert len(deletes) == len(locks), "SQL cost: missing bounded locking read"
        assert all(query.count(" OR ") < 64 for query in deletes)


@pytest.mark.parametrize("files", [63, 64, 65, 255, 256, 257])
def test_filesystem_cleanup_cost_and_retention_at_both_page_boundaries(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch, files: int
) -> None:
    gate, cycle = _seed(db_config, files)
    samples = _drain(db_config, gate, cycle, monkeypatch, files=files)
    _assert_costs(samples, files=files)


def test_filesystem_cleanup_repeated_cycles_reload_retention_authority(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    files = 65
    gate, cycle = _seed(db_config, files)
    identities: set[bytes] = set()
    signatures = []
    for lap in range(3):
        identities.add(cycle.cleanup_id)
        samples = _drain(
            db_config, gate, cycle, monkeypatch, files=files, now=3 + lap * 100
        )
        _assert_costs(samples, files=files)
        signatures.append(
            tuple(
                (sample.phase, len(sample.keys), len(sample.queries))
                for sample in samples
            )
        )
        if lap < 2:
            with closing(open_connector(db_config)) as connector:
                with partial_publication_setup(
                    connector, backend=backend_of(db_config)
                ):
                    _seed_observation(connector, observation=1, files=files)
            cycle = _begin(db_config, gate, now=90 + lap * 100)
    assert len(identities) == 3
    assert signatures == [signatures[0]] * 3


def test_cost_oracle_rejects_scalar_deletion_with_correct_retained_results(
    sqlite_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    files = 65
    gate, cycle = _seed(sqlite_config, files)
    original = cleanup._delete_static_key_page

    def scalar(work: VNextUnitOfWork, **kwargs: Any) -> None:
        if kwargs["phase"] in _PHASES:
            for candidate in kwargs["candidates"]:
                original(work, **{**kwargs, "candidates": (candidate,)})
        else:
            original(work, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(cleanup, "_delete_static_key_page", scalar)
        samples = _drain(sqlite_config, gate, cycle, monkeypatch, files=files)
    with pytest.raises(AssertionError, match="SQL cost"):
        _assert_costs(samples, files=files)


def test_observation_batch_failure_rolls_back_rows_and_checkpoint_then_resumes(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    files = 65
    gate, cycle = _seed(db_config, files)
    original = cleanup._delete_static_key_page
    injected = False

    def fail_after_delete(work: VNextUnitOfWork, **kwargs: Any) -> None:
        nonlocal injected
        original(work, **kwargs)
        if kwargs["phase"] == "GO_FACTS" and not injected:
            injected = True
            raise RuntimeError("injected after exact observation batch deletion")

    with (
        monkeypatch.context() as patch,
        closing(open_connector(db_config)) as connector,
    ):
        patch.setattr(cleanup, "_delete_static_key_page", fail_after_delete)
        for step in range(32):
            with connector.read_transaction():
                before = connector.fetch_all(
                    "SELECT * FROM operational_cleanup_checkpoints "
                    "WHERE cleanup_id = %s",
                    (cycle.cleanup_id,),
                )
            try:
                with connector.transaction():
                    cleanup.VNextCleanupRepository.advance_current_only_cycle(
                        VNextUnitOfWork(connector, backend=backend_of(db_config)),
                        gate_lease=gate,
                        cycle=cycle,
                        now=3 + step,
                    )
            except RuntimeError as error:
                assert str(error) == "injected after exact observation batch deletion"
                assert injected
                assert (
                    connector.fetch_all(
                        "SELECT * FROM operational_cleanup_checkpoints "
                        "WHERE cleanup_id = %s",
                        (cycle.cleanup_id,),
                    )
                    == before
                )
                assert connector.fetch_one(
                    "SELECT COUNT(*) FROM catalog_gallery_observation_file_hash_occurrences "
                    "WHERE gallery_id = %s AND observation_id = 1",
                    (_GALLERY,),
                ) == (files,)
                for suffix in ("anchors", *dict(_VALUES), "seals"):
                    assert connector.fetch_one(
                        f"SELECT COUNT(*) FROM {_PREFIX}{suffix} "
                        "WHERE gallery_id = %s AND observation_id = 1",
                        (_GALLERY,),
                    ) == (files,)
                break
        else:
            pytest.fail("filesystem deletion fault was not reached")
    samples = _drain(db_config, gate, cycle, monkeypatch, files=files, now=50)
    _assert_costs(samples, files=files)
