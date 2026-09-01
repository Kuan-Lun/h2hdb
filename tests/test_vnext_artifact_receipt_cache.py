from __future__ import annotations

import gc
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

import h2hdb.vnext_artifact_preparation_repository as artifact_repository
import h2hdb.vnext_ingest_publication as publication
from h2hdb.config_loader import CoreConfig, DatabaseConfig
from h2hdb.domain import (
    CatalogResourceKind,
    StorageObjectDescriptor,
    StorageObjectKey,
    VNextIngestAdvanceResult,
    VNextIngestSession,
)
from h2hdb.ports import ArtifactStorageAdapter
from h2hdb.repository import RepositoryContext
from h2hdb.vnext_artifact_family import PreparedArtifactFamily
from h2hdb.vnext_artifact_preparation_repository import (
    ArtifactPreparationAuthority,
    ArtifactPreparationReceipt,
    ArtifactPreparationRepository,
    ArtifactProtectionIntent,
)
from h2hdb.vnext_artifact_render import (
    ArtifactRenderConflictError,
    ArtifactRenderNotReadyError,
)
from h2hdb.vnext_identity import (
    artifact_storage_key_digest,
    encode_artifact_protection_token,
)
from h2hdb.vnext_operational_event_repository import OperationalEffectSeal


@dataclass(frozen=True, slots=True)
class _Authority:
    adapter_id: bytes
    candidate_id: bytes
    publication_key: bytes


@dataclass(frozen=True, slots=True)
class _Audit:
    authority: _Authority
    snapshot: bytes


class _TrackedReceipt:
    resource_kinds = (CatalogResourceKind.ACQUISITION,)

    def __init__(self, audit: _Audit, *, size_bytes: int) -> None:
        self.audit = audit
        self.close_count = 0
        self.size_bytes = size_bytes

    def close(self) -> None:
        self.close_count += 1


class _ArtifactHarness:
    def __init__(
        self,
        authority: _Authority,
        intent: ArtifactProtectionIntent,
    ) -> None:
        self.authority = authority
        self.intent = intent
        self.audit_snapshot = b"stable-audit"
        self.receipts: list[_TrackedReceipt] = []
        self.protected: list[ArtifactProtectionIntent] = []
        self.durable_observations: list[tuple[str, bytes, bytes, bytes]] = []
        self.source_revalidation_count = 0
        self.source_failure: tuple[type[RuntimeError], str] | None = None
        self.receipt_size_bytes = len(b"artifact")

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            ArtifactPreparationRepository,
            "audit_inputs",
            staticmethod(self.audit_inputs),
        )
        monkeypatch.setattr(
            ArtifactPreparationRepository,
            "prepare_with_storage_adapter",
            staticmethod(self.render),
        )
        monkeypatch.setattr(
            ArtifactPreparationRepository,
            "revalidate_cached_sources",
            staticmethod(self.revalidate_cached_sources),
        )
        monkeypatch.setattr(
            publication,
            "_protection_intent_from_family",
            self.intent_from_family,
        )
        monkeypatch.setattr(
            ArtifactPreparationRepository,
            "protect_prepared_artifact",
            staticmethod(self.protect),
        )
        monkeypatch.setattr(publication, "_commit_action", self.commit_action)

    def audit_inputs(self, *_args: object, **kwargs: object) -> _Audit:
        authority = cast(_Authority, kwargs["authority"])
        return _Audit(authority, self.audit_snapshot)

    def render(self, *_args: object, **kwargs: object) -> _TrackedReceipt:
        self._raise_source_failure()
        receipt = _TrackedReceipt(
            cast(_Audit, kwargs["audit"]),
            size_bytes=self.receipt_size_bytes,
        )
        self.receipts.append(receipt)
        return receipt

    def revalidate_cached_sources(self, **_kwargs: object) -> None:
        self.source_revalidation_count += 1
        self._raise_source_failure()

    def _raise_source_failure(self) -> None:
        if self.source_failure is not None:
            error_type, message = self.source_failure
            raise error_type(message)

    def intent_from_family(self, *_args: object, **_kwargs: object) -> object:
        return self.intent

    def protect(self, *_args: object, **kwargs: object) -> object:
        receipt = cast(_TrackedReceipt, kwargs["receipt"])
        assert receipt.close_count == 0
        intent = cast(ArtifactProtectionIntent, kwargs["intent"])
        self.protected.append(intent)
        return SimpleNamespace(intent=intent)

    def commit_action(
        self,
        _work: object,
        *,
        payload: object,
        **_kwargs: object,
    ) -> object:
        prepared = cast(publication._ArtifactPrepared, payload)
        receipt = cast(_TrackedReceipt, prepared.receipt)
        authority = receipt.audit.authority
        if not prepared.intents:
            self.durable_observations.append(
                (
                    "PENDING",
                    authority.candidate_id,
                    authority.publication_key,
                    receipt.audit.snapshot,
                )
            )
            return (self.intent,)
        self.durable_observations.append(
            (
                "PREPARED",
                authority.candidate_id,
                authority.publication_key,
                receipt.audit.snapshot,
            )
        )
        return SimpleNamespace(replayed=False)


def _context(path: Path) -> RepositoryContext:
    return RepositoryContext.from_config(
        CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))
    )


def _session() -> VNextIngestSession:
    return VNextIngestSession(
        gate_owner_token=b"g" * 16,
        gate_generation=1,
        gate_slot=0,
        gate_lease_expires_at=100,
        ingest_generation=1,
        ingest_owner_token=b"i" * 16,
        ingest_lease_expires_at=100,
        download_generation=None,
        handoff_owner_token=None,
        handoff_kind=None,
        consumed_at=None,
    )


def _intent_and_family(
    authority: _Authority,
) -> tuple[ArtifactProtectionIntent, PreparedArtifactFamily]:
    key = StorageObjectKey("cache-test-v1", ("1", "acquisition"))
    payload = b"artifact"
    descriptor = StorageObjectDescriptor(
        key,
        len(payload),
        sha256(payload).hexdigest(),
        datetime(2026, 9, 2, tzinfo=UTC),
    )
    key_digest = artifact_storage_key_digest(key.codec, key.segments)
    token = encode_artifact_protection_token(
        authority.candidate_id,
        authority.publication_key,
        CatalogResourceKind.ACQUISITION.value,
        key_digest,
        1,
    )
    intent = ArtifactProtectionIntent(
        authority.candidate_id,
        authority.publication_key,
        CatalogResourceKind.ACQUISITION,
        descriptor,
        key_digest,
        1,
        token,
        "PENDING",
        False,
        _capability=artifact_repository._PROTECTION_INTENT_TOKEN,
    )
    return intent, PreparedArtifactFamily(
        authority.candidate_id,
        authority.publication_key,
        CatalogResourceKind.ACQUISITION,
        key_digest,
        1,
        token,
        "PENDING",
    )


def _work(
    authority: _Authority,
    families: tuple[PreparedArtifactFamily, ...] | None,
) -> publication._ArtifactWork:
    return publication._ArtifactWork(
        cast(ArtifactPreparationAuthority, authority),
        cast(OperationalEffectSeal, SimpleNamespace(preparation_id=b"p" * 16)),
        families,
    )


def _prepare(
    machine: publication.VNextIngestPublication,
    work: publication._ArtifactWork,
) -> publication._ArtifactPrepared:
    adapter = cast(ArtifactStorageAdapter, SimpleNamespace())
    return cast(
        publication._ArtifactPrepared,
        cast(Any, machine)._VNextIngestPublication__prepare_artifact(
            work,
            {b"receipt-cache-adapter": adapter},
        ),
    )


def _prepared_step(
    session: VNextIngestSession,
    work: publication._ArtifactWork,
    prepared: publication._ArtifactPrepared,
) -> publication.VNextPreparedPublicationStep:
    issued = publication.VNextIssuedPublicationStep(
        action=publication._Action.PREPARE_ARTIFACT,
        payload=work,
        session=session,
        _token=publication._STEP_TOKEN,
    )
    return publication.VNextPreparedPublicationStep(
        issued=issued,
        action=publication._Action.PREPARE_ARTIFACT,
        payload=prepared,
        _token=publication._PREPARED_TOKEN,
    )


def _commit(
    machine: publication.VNextIngestPublication,
    session: VNextIngestSession,
    work: publication._ArtifactWork,
    prepared: publication._ArtifactPrepared,
) -> VNextIngestAdvanceResult:
    return machine.commit_step(session, _prepared_step(session, work, prepared))


def _cached(machine: publication.VNextIngestPublication) -> object | None:
    return cast(
        object | None,
        cast(Any, machine)._VNextIngestPublication__artifact_receipt,
    )


def _run_two_phase(
    context: RepositoryContext,
    harness: _ArtifactHarness,
    family: PreparedArtifactFamily,
    *,
    restart_after_pending: bool,
) -> tuple[
    tuple[VNextIngestAdvanceResult, VNextIngestAdvanceResult],
    int,
    tuple[tuple[str, bytes, bytes, bytes], ...],
]:
    before = len(harness.receipts)
    observation_before = len(harness.durable_observations)
    session = _session()
    initial = _work(harness.authority, None)
    pending = _work(harness.authority, (family,))
    machine = publication.VNextIngestPublication(context, clock=lambda: 10)

    persisted = _commit(machine, session, initial, _prepare(machine, initial))
    assert _cached(machine) is not None
    if restart_after_pending:
        del machine
        gc.collect()
        machine = publication.VNextIngestPublication(context, clock=lambda: 10)

    confirmed = _commit(machine, session, pending, _prepare(machine, pending))
    assert _cached(machine) is None
    return (
        (persisted, confirmed),
        len(harness.receipts) - before,
        tuple(harness.durable_observations[observation_before:]),
    )


def test_optional_receipt_cache_is_differentially_equivalent_and_renders_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _Authority(
        b"receipt-cache-adapter",
        b"c" * 16,
        b"p" * 32,
    )
    intent, family = _intent_and_family(authority)
    harness = _ArtifactHarness(authority, intent)
    harness.install(monkeypatch)

    optimized, optimized_renders, optimized_durable = _run_two_phase(
        _context(tmp_path / "optimized.sqlite3"),
        harness,
        family,
        restart_after_pending=False,
    )
    reference, reference_renders, reference_durable = _run_two_phase(
        _context(tmp_path / "restart-reference.sqlite3"),
        harness,
        family,
        restart_after_pending=True,
    )

    assert optimized == reference
    assert optimized_durable == reference_durable
    assert optimized_renders == 1
    assert reference_renders == 2
    assert harness.source_revalidation_count == 1
    assert harness.protected == [intent, intent]
    assert all(receipt.close_count == 1 for receipt in harness.receipts)


@pytest.mark.parametrize(
    ("error_type", "message"),
    [
        (
            ArtifactRenderNotReadyError,
            "artifact adapter could not open a sealed source member",
        ),
        (
            ArtifactRenderConflictError,
            "artifact source digest differs from sealed authority",
        ),
    ],
)
def test_cached_source_failure_matches_restart_reference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[RuntimeError],
    message: str,
) -> None:
    authority = _Authority(
        b"receipt-cache-adapter",
        b"c" * 16,
        b"p" * 32,
    )
    intent, family = _intent_and_family(authority)
    harness = _ArtifactHarness(authority, intent)
    harness.install(monkeypatch)
    session = _session()
    initial = _work(authority, None)
    pending = _work(authority, (family,))

    optimized = publication.VNextIngestPublication(
        _context(tmp_path / "optimized-source-failure.sqlite3"),
        clock=lambda: 10,
    )
    _commit(optimized, session, initial, _prepare(optimized, initial))
    optimized_receipt = harness.receipts[-1]

    reference = publication.VNextIngestPublication(
        _context(tmp_path / "restart-source-failure.sqlite3"),
        clock=lambda: 10,
    )
    _commit(reference, session, initial, _prepare(reference, initial))
    reference_receipt = harness.receipts[-1]
    del reference
    gc.collect()
    assert reference_receipt.close_count == 1
    reference = publication.VNextIngestPublication(
        _context(tmp_path / "restart-source-failure.sqlite3"),
        clock=lambda: 10,
    )

    harness.source_failure = (error_type, message)
    with pytest.raises(error_type, match=message):
        _prepare(optimized, pending)
    with pytest.raises(error_type, match=message):
        _prepare(reference, pending)

    assert optimized_receipt.close_count == 1
    assert _cached(optimized) is None
    assert _cached(reference) is None
    assert harness.source_revalidation_count == 1


def test_response_loss_after_pending_commit_does_not_retain_local_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _Authority(
        b"receipt-cache-adapter",
        b"c" * 16,
        b"p" * 32,
    )
    intent, family = _intent_and_family(authority)
    harness = _ArtifactHarness(authority, intent)
    harness.install(monkeypatch)
    context = _context(tmp_path / "response-loss.sqlite3")
    original_factory = context.SQLConnector
    lose_response = {"enabled": True}

    def connector_factory() -> Any:
        connector = original_factory()
        original_commit = connector.commit

        def commit() -> None:
            original_commit()
            if lose_response["enabled"]:
                raise ConnectionError("commit response lost")

        cast(Any, connector).commit = commit
        return connector

    machine = publication.VNextIngestPublication(
        replace(context, SQLConnector=connector_factory),
        clock=lambda: 10,
    )
    session = _session()
    initial = _work(authority, None)
    first = _prepare(machine, initial)

    with pytest.raises(ConnectionError, match="response lost"):
        _commit(machine, session, initial, first)

    assert harness.receipts[0].close_count == 1
    assert _cached(machine) is None
    lose_response["enabled"] = False
    retried = _prepare(machine, _work(authority, (family,)))
    assert len(harness.receipts) == 2
    retried.receipt_owner.close()


def test_oversized_receipt_is_closed_instead_of_retained(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _Authority(
        b"receipt-cache-adapter",
        b"c" * 16,
        b"p" * 32,
    )
    intent, family = _intent_and_family(authority)
    harness = _ArtifactHarness(authority, intent)
    harness.receipt_size_bytes = 256 * 1024 * 1024 + 1
    harness.install(monkeypatch)
    machine = publication.VNextIngestPublication(
        _context(tmp_path / "oversized-receipt.sqlite3"),
        clock=lambda: 10,
    )
    session = _session()
    initial = _work(authority, None)

    _commit(machine, session, initial, _prepare(machine, initial))

    assert _cached(machine) is None
    assert harness.receipts[0].close_count == 1
    pending = _prepare(machine, _work(authority, (family,)))
    assert len(harness.receipts) == 2
    assert harness.source_revalidation_count == 0
    pending.receipt_owner.close()


def test_cache_capacity_counts_acquisition_and_thumbnail_at_exact_boundary() -> None:
    class _CapacityReceipt:
        def __init__(self, acquisition: int, thumbnail: int | None) -> None:
            self.size_bytes = acquisition
            self.resource_kinds = (
                (CatalogResourceKind.ACQUISITION,)
                if thumbnail is None
                else (
                    CatalogResourceKind.ACQUISITION,
                    CatalogResourceKind.THUMBNAIL,
                )
            )
            self.thumbnail = thumbnail

        def resource_descriptor(
            self,
            resource_kind: CatalogResourceKind,
        ) -> object:
            assert resource_kind is CatalogResourceKind.THUMBNAIL
            assert self.thumbnail is not None
            return SimpleNamespace(size_bytes=self.thumbnail)

    capacity = 256 * 1024 * 1024
    fits = cast(Any, publication)._artifact_receipt_fits_cache

    assert fits(cast(ArtifactPreparationReceipt, _CapacityReceipt(capacity, None)))
    assert not fits(
        cast(ArtifactPreparationReceipt, _CapacityReceipt(capacity + 1, None))
    )
    assert fits(cast(ArtifactPreparationReceipt, _CapacityReceipt(capacity - 1, 1)))
    assert not fits(cast(ArtifactPreparationReceipt, _CapacityReceipt(capacity - 1, 2)))


def test_authority_drift_and_cached_audit_drift_use_reference_rerender(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _Authority(
        b"receipt-cache-adapter",
        b"c" * 16,
        b"p" * 32,
    )
    intent, family = _intent_and_family(authority)
    harness = _ArtifactHarness(authority, intent)
    harness.install(monkeypatch)
    session = _session()

    drift_machine = publication.VNextIngestPublication(
        _context(tmp_path / "authority-drift.sqlite3"),
        clock=lambda: 10,
    )
    initial = _work(authority, None)
    _commit(drift_machine, session, initial, _prepare(drift_machine, initial))
    drift_receipt = harness.receipts[-1]
    changed = replace(authority, candidate_id=b"d" * 16)
    lock = cast(Any, drift_machine)._VNextIngestPublication__artifact_receipt_lock
    with lock:
        cast(
            Any, drift_machine
        )._VNextIngestPublication__retire_mismatched_artifact_receipt(
            publication._Action.PREPARE_ARTIFACT,
            _work(changed, (family,)),
        )
    assert drift_receipt.close_count == 1
    assert _cached(drift_machine) is None

    audit_machine = publication.VNextIngestPublication(
        _context(tmp_path / "audit-drift.sqlite3"),
        clock=lambda: 10,
    )
    _commit(audit_machine, session, initial, _prepare(audit_machine, initial))
    audit_receipt = harness.receipts[-1]
    harness.audit_snapshot = b"changed-after-persist"
    pending = _work(authority, (family,))
    rerendered = _prepare(audit_machine, pending)
    rerendered_receipt = cast(_TrackedReceipt, cast(object, rerendered.receipt))

    assert audit_receipt.close_count == 1
    assert _cached(audit_machine) is None
    assert rerendered_receipt is harness.receipts[-1]
    assert rerendered_receipt.audit.snapshot == b"changed-after-persist"
    assert len(harness.receipts) == 3

    _commit(audit_machine, session, pending, rerendered)
    assert harness.durable_observations[-1] == (
        "PREPARED",
        authority.candidate_id,
        authority.publication_key,
        b"changed-after-persist",
    )
    assert harness.receipts[-1].close_count == 1


def test_cached_audit_drift_rerender_failure_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _Authority(
        b"receipt-cache-adapter",
        b"c" * 16,
        b"p" * 32,
    )
    intent, family = _intent_and_family(authority)
    harness = _ArtifactHarness(authority, intent)
    harness.install(monkeypatch)
    machine = publication.VNextIngestPublication(
        _context(tmp_path / "audit-drift-rerender-failure.sqlite3"),
        clock=lambda: 10,
    )
    session = _session()
    initial = _work(authority, None)
    _commit(machine, session, initial, _prepare(machine, initial))
    cached_receipt = harness.receipts[-1]
    harness.audit_snapshot = b"changed-after-persist"

    def fail_rerender(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("reference rerender failed")

    monkeypatch.setattr(
        ArtifactPreparationRepository,
        "prepare_with_storage_adapter",
        staticmethod(fail_rerender),
    )

    with pytest.raises(RuntimeError, match="reference rerender failed"):
        _prepare(machine, _work(authority, (family,)))

    assert cached_receipt.close_count == 1
    assert len(harness.receipts) == 1
    assert _cached(machine) is None


def test_prepared_close_and_protection_failure_release_exclusive_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _Authority(
        b"receipt-cache-adapter",
        b"c" * 16,
        b"p" * 32,
    )
    intent, family = _intent_and_family(authority)
    harness = _ArtifactHarness(authority, intent)
    harness.install(monkeypatch)
    session = _session()
    machine = publication.VNextIngestPublication(
        _context(tmp_path / "close-and-fault.sqlite3"),
        clock=lambda: 10,
    )
    initial = _work(authority, None)

    abandoned = _prepare(machine, initial)
    step = _prepared_step(session, initial, abandoned)
    step.close()
    assert harness.receipts[-1].close_count == 1
    assert _cached(machine) is None

    _commit(machine, session, initial, _prepare(machine, initial))
    cached_receipt = harness.receipts[-1]

    def fail_protection(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("storage protection failed")

    monkeypatch.setattr(
        ArtifactPreparationRepository,
        "protect_prepared_artifact",
        staticmethod(fail_protection),
    )
    with pytest.raises(RuntimeError, match="storage protection failed"):
        _prepare(machine, _work(authority, (family,)))

    assert cached_receipt.close_count == 1
    assert _cached(machine) is None
