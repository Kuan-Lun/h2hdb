"""Artifact release samples live gate authority after waits and replays expiry.

The existing orphan-resource fixtures provide the generated SQL families.
Only their fixture insertion wrapper is adapted for the two backend sessions;
the release repositories, facade transactions and durable cleanup are real.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from typing import Any, Literal, cast

import pytest
import test_vnext_artifact_release_repository as fixtures
from vnext_fault_harness import backend_of, open_connector
from vnext_pipeline import initialize_database

from h2hdb import CoreConfig, VNextCurrentOnlyMaintenanceOutcome, VNextIngestFacade
from h2hdb.domain import CatalogResourceKind, StorageObjectKey
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_artifact_release_repository import (
    ArtifactReleaseAcknowledgement,
    ArtifactReleasePage,
    ArtifactReleaseRepository,
    ArtifactReleaseStorageEvidence,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
    MaintenanceGateUnavailableError,
)
from h2hdb.vnext_transaction import LockRank, VNextUnitOfWork

pytestmark = pytest.mark.cleanup_acceptance

_DURATION = 1_000
_CANDIDATE = b"a" * 16
type _Phase = Literal["issue", "external", "commit"]
type _Now = int | Callable[[], int]


@dataclass
class _Clock:
    now: int = 100
    samples: list[int] = field(default_factory=list)

    def __call__(self) -> int:
        self.samples.append(self.now)
        return self.now


@dataclass
class _Adapter:
    on_release: Callable[[], None] = lambda: None
    adapter_id: bytes = fixtures._ADAPTER_ID
    calls: list[tuple[StorageObjectKey, bytes, int, bytes]] = field(
        default_factory=list
    )
    tombstones: set[bytes] = field(default_factory=set)

    def release(
        self,
        storage_key: StorageObjectKey,
        expected_sha256: bytes,
        expected_size_bytes: int,
        protection_token: bytes,
    ) -> ArtifactReleaseStorageEvidence:
        self.calls.append(
            (storage_key, expected_sha256, expected_size_bytes, protection_token)
        )
        self.tombstones.add(protection_token)
        self.on_release()
        return ArtifactReleaseStorageEvidence(True)


@pytest.fixture
def orphan(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[SQLConnector, str, bytes]]:
    initialize_database(db_config)
    backend = backend_of(db_config)

    def insert_rows(
        database: SQLConnector, statements: list[tuple[str, tuple[object, ...]]]
    ) -> None:
        disable, enable = (
            ("PRAGMA foreign_keys = OFF", "PRAGMA foreign_keys = ON")
            if backend == "sqlite"
            else ("SET FOREIGN_KEY_CHECKS = 0", "SET FOREIGN_KEY_CHECKS = 1")
        )
        database.execute(disable)
        try:
            for sql, parameters in statements:
                database.execute(sql, parameters)
        finally:
            database.execute(enable)

    with closing(open_connector(db_config)) as connector:
        with monkeypatch.context() as patch:
            patch.setattr(fixtures, "_fixture_rows", insert_rows)
            # Those fixture builders use only the common SQLConnector methods.
            fixture_connector = cast(SQLiteConnector, connector)
            fixtures._seed_policy_canonical_value(fixture_connector)
            fixtures._seed_candidate(
                fixture_connector, candidate_id=_CANDIDATE, reserved_revision=1
            )
            token = fixtures._seed_resource(
                fixture_connector,
                gid=1,
                candidate_id=_CANDIDATE,
                reserved_revision=1,
                resource_kind=CatalogResourceKind.ACQUISITION,
                storage_object_sha256=b"a" * 32,
                state="PREPARED",
            )
        yield connector, backend, token


def _state(connector: SQLConnector) -> tuple[Any, ...]:
    # The facade writes through another connection. MariaDB's implicit
    # REPEATABLE READ snapshot would otherwise retain the first observation
    # across later committed release/cleanup operations.
    with connector.read_transaction():
        return connector.fetch_one(
            "SELECT state FROM catalog_prepared_artifacts WHERE candidate_id = %s",
            (_CANDIDATE,),
        )


def _claim(connector: SQLConnector, backend: str, clock: _Clock) -> GateLease:
    with connector.transaction():
        claim = MaintenanceGateRepository.lock_exclusive_claim(
            VNextUnitOfWork(connector, backend=backend)
        )
        return claim.grant(now=clock(), lease_duration=_DURATION)


def _issue(
    connector: SQLConnector, backend: str, lease: GateLease, now: _Now
) -> ArtifactReleasePage:
    with connector.transaction():
        return ArtifactReleaseRepository.issue_page(
            VNextUnitOfWork(connector, backend=backend),
            gate_lease=lease,
            now=now,
        )


def _external(
    connector: SQLConnector,
    backend: str,
    page: ArtifactReleasePage,
    adapter: _Adapter,
    now: _Now,
) -> ArtifactReleaseAcknowledgement:
    return ArtifactReleaseRepository.release_page(
        connector,
        backend=backend,
        page=page,
        adapters={adapter.adapter_id: adapter},
        now=now,
    )


def _commit(
    connector: SQLConnector,
    backend: str,
    acknowledgement: ArtifactReleaseAcknowledgement,
    now: _Now,
) -> None:
    with connector.transaction():
        receipt = ArtifactReleaseRepository.commit_page(
            VNextUnitOfWork(connector, backend=backend),
            acknowledgement=acknowledgement,
            now=now,
        )
        assert receipt.transitioned_count == 1


def _operation(
    connector: SQLConnector,
    backend: str,
    clock: _Clock,
    adapter: _Adapter,
    phase: _Phase,
    *,
    fixed_time: bool = False,
) -> Callable[[], object]:
    lease = _claim(connector, backend, clock)
    now: _Now = clock.now if fixed_time else clock
    if phase == "issue":
        return lambda: _issue(connector, backend, lease, now)
    page = _issue(connector, backend, lease, clock.now)
    if phase == "external":
        return lambda: _external(connector, backend, page, adapter, now)
    acknowledgement = _external(connector, backend, page, adapter, clock.now)
    return lambda: _commit(connector, backend, acknowledgement, now)


@pytest.mark.parametrize("phase", ["issue", "external", "commit"])
@pytest.mark.parametrize("elapsed", [_DURATION - 1, _DURATION, _DURATION + 1])
def test_artifact_phase_samples_after_gate_wait_and_rejects_expiry(
    orphan: tuple[SQLConnector, str, bytes],
    monkeypatch: pytest.MonkeyPatch,
    phase: _Phase,
    elapsed: int,
) -> None:
    connector, backend, _token = orphan
    clock, adapter = _Clock(), _Adapter()
    operation = _operation(connector, backend, clock, adapter, phase)
    clock.samples.clear()
    previous_calls = len(adapter.calls)
    lock_rows = VNextUnitOfWork.lock_rows
    delayed = False

    def lock(self: VNextUnitOfWork, *args: Any, **kwargs: Any) -> Any:
        nonlocal delayed
        result = lock_rows(self, *args, **kwargs)
        if not delayed and args[0] is LockRank.MAINTENANCE_GATE:
            delayed = True
            clock.now += elapsed
        return result

    monkeypatch.setattr(VNextUnitOfWork, "lock_rows", lock)
    if elapsed < _DURATION:
        operation()
    else:
        with pytest.raises(MaintenanceGateUnavailableError, match="expired"):
            operation()
    assert delayed
    assert clock.samples == [100 + elapsed]
    assert _state(connector) == (
        ("COMMITTED",) if phase == "commit" and elapsed < _DURATION else ("PREPARED",)
    )
    assert len(adapter.calls) == previous_calls + int(
        phase == "external" and elapsed < _DURATION
    )
    with connector.read_transaction():
        assert connector.fetch_one(
            "SELECT lease_expires_at FROM operational_maintenance_gate_owners"
        ) == (100 + _DURATION,)


@pytest.mark.parametrize("phase", ["issue", "external", "commit"])
def test_artifact_explicit_event_timestamp_remains_supported(
    orphan: tuple[SQLConnector, str, bytes], phase: _Phase
) -> None:
    connector, backend, _token = orphan
    clock, adapter = _Clock(), _Adapter()
    operation = _operation(connector, backend, clock, adapter, phase, fixed_time=True)
    clock.samples.clear()
    operation()
    assert clock.samples == []
    assert _state(connector) == (("COMMITTED",) if phase == "commit" else ("PREPARED",))


@pytest.mark.parametrize("expiry_point", ["external", "commit"])
def test_facade_artifact_expiry_retains_durable_evidence_and_converges(
    db_config: CoreConfig,
    orphan: tuple[SQLConnector, str, bytes],
    monkeypatch: pytest.MonkeyPatch,
    expiry_point: str,
) -> None:
    connector, _backend, token = orphan
    clock = _Clock()
    writes = 0
    expired = False
    transaction = SQLConnector.transaction
    commit_page = ArtifactReleaseRepository.commit_page

    def expire() -> None:
        nonlocal expired
        if not expired:
            expired = True
            clock.now += _DURATION

    @contextmanager
    def tracked_transaction(self: SQLConnector) -> Iterator[None]:
        nonlocal writes
        with transaction(self):
            writes += 1
            try:
                yield
            finally:
                writes -= 1

    def external_release() -> None:
        assert writes == 0, "adapter I/O must follow committed DB revalidation"
        if expiry_point == "external":
            expire()

    def slow_commit(*args: Any, **kwargs: Any) -> Any:
        result = commit_page(*args, **kwargs)
        if expiry_point == "commit":
            expire()
        return result

    adapter = _Adapter(on_release=external_release)
    monkeypatch.setattr(SQLConnector, "transaction", tracked_transaction)
    monkeypatch.setattr(
        ArtifactReleaseRepository, "commit_page", staticmethod(slow_commit)
    )
    with VNextIngestFacade(db_config, clock=clock) as facade:
        first = facade.drain_current_only_maintenance(
            _DURATION, artifact_release_adapters={adapter.adapter_id: adapter}
        )
        assert expired
        assert first is (
            VNextCurrentOnlyMaintenanceOutcome.CONTENDED
            if expiry_point == "external"
            else VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        )
        assert _state(connector) == (
            ("PREPARED",) if expiry_point == "external" else ("COMMITTED",)
        )
        assert adapter.tombstones == {token}
        assert len(adapter.calls) == 1
        for _attempt in range(64):
            outcome = facade.drain_current_only_maintenance(
                _DURATION, artifact_release_adapters={adapter.adapter_id: adapter}
            )
            if outcome is VNextCurrentOnlyMaintenanceOutcome.DONE:
                break
            assert outcome is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        else:
            pytest.fail("artifact expiry replay did not reach the cleanup fixed point")
        assert _state(connector) == ()
        assert adapter.tombstones == {token}
        assert len(adapter.calls) == (2 if expiry_point == "external" else 1)
        assert all(call == adapter.calls[0] for call in adapter.calls)
        with connector.read_transaction():
            assert connector.fetch_one(
                "SELECT COUNT(*) FROM operational_maintenance_gate_owners"
            ) == (0,)
        session = facade.try_claim_ingest(True, _DURATION)
        assert session is not None
        facade.complete_ingest(session)
