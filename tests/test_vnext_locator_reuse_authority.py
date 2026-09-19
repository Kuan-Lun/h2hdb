"""Fault checks for the development-only locator reuse counterfactual."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from types import ModuleType

import pytest
from test_vnext_source_build_runtime_repository import (
    _frozen_fixture_summary,
    _generated_database,
    _open_build,
    _snapshot_command,
    _upload,
    _working_build_id,
)

from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueCollisionError,
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_identity import (
    CANONICAL_VALUE_CHUNK_BYTES,
    encode_source_relative_locator,
)
from h2hdb.vnext_ingest_fence_repository import IngestFenceUnavailableError, IngestTurn
from h2hdb.vnext_maintenance_gate_repository import GateLease
from h2hdb.vnext_source_build_repository import (
    DiscoveryBatch,
    ResolvedDiscoveryLocator,
    SourceBuildConflictError,
    SourceBuildRepository,
    SourceDiscoveryPlan,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


@dataclass
class _Scenario:
    probe: ModuleType
    connector: SQLiteConnector
    gate: GateLease
    turn: IngestTurn
    batch: DiscoveryBatch
    upload: CanonicalValueUploadPlan
    initial: ResolvedDiscoveryLocator

    def claim(self) -> None:
        with self.connector.transaction():
            CanonicalValueRepository.allocate(
                VNextUnitOfWork(self.connector, backend="sqlite"),
                gate_lease=self.gate,
                ingest_turn=self.turn,
                plan=self.upload,
                now=40,
            )

    def try_reuse(
        self, *, turn: IngestTurn | None = None
    ) -> ResolvedDiscoveryLocator | None:
        result = self.probe.reuse_discovery_locator(
            VNextUnitOfWork(self.connector, backend="sqlite"),
            gate_lease=self.gate,
            ingest_turn=self.turn if turn is None else turn,
            batch=self.batch,
            locator=self.batch.locators[0],
            upload_plan=self.upload,
            now=50,
        )
        assert result is None or isinstance(result, ResolvedDiscoveryLocator)
        return result

    def reuse(self, *, turn: IngestTurn | None = None) -> ResolvedDiscoveryLocator:
        result = self.try_reuse(turn=turn)
        assert result is not None
        return result

    def has_claim(self) -> bool:
        return bool(
            self.connector.fetch_one(
                "SELECT generation FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                (self.turn.generation, self.upload.value_sha256),
            )
        )


@pytest.fixture
def probe() -> ModuleType:
    name = "ingest_locator_reuse_authority_probe"
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "ingest_locator_reuse_probe.py"
    )
    spec = importlib.util.spec_from_file_location(name, script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    previous = list(sys.path)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = previous
    return module


@contextmanager
def _open_scenario(
    tmp_path: Path, probe: ModuleType, locator: tuple[str, ...]
) -> Iterator[_Scenario]:
    connector = _generated_database(tmp_path / "locator-reuse.sqlite3")
    locators = (locator,)
    try:
        summary = _frozen_fixture_summary(
            root=(),
            locators=locators,
            file_count_for_position=lambda _: 1,
            byte_count_for_position=lambda _: 1,
        )
        gate, turn, _command = _open_build(
            connector, command=_snapshot_command((), summary)
        )
        with SourceDiscoveryPlan.from_locators(locators) as plan:
            batch = SourceBuildRepository.prepare_discovery_batch(
                connector, build_id=_working_build_id(connector), plan=plan
            )
            with plan.prepare_locator_upload(batch.locators[0]) as original:
                _upload(connector, gate, turn, original, now=30)
                with connector.transaction():
                    initial = SourceBuildRepository.resolve_discovery_locator(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        batch=batch,
                        locator=batch.locators[0],
                        upload_plan=original,
                        now=34,
                    )
            with plan.prepare_locator_upload(batch.locators[0]) as upload:
                tuple(upload.iter_pages())
                with probe.candidate():
                    yield _Scenario(
                        probe, connector, gate, turn, batch, upload, initial
                    )
    finally:
        connector.close()


@pytest.fixture
def scenario(tmp_path: Path, probe: ModuleType) -> Iterator[_Scenario]:
    with _open_scenario(tmp_path, probe, ("collection", "gallery")) as result:
        yield result


def _locator_with_payload_size(size: int) -> tuple[str, ...]:
    # The codec has two u32 header fields and one u32 length per UTF-8 segment.
    # Each segment remains in its real 1..255-byte domain at every boundary.
    full_segments, tail = divmod(size - 8, 4 + 255)
    assert 5 <= tail <= 259
    locator = ("a" * 255,) * full_segments + ("b" * (tail - 4),)
    assert len(encode_source_relative_locator(locator)) == size
    return locator


@pytest.mark.parametrize("payload_size", [32767, 32768, 32769])
def test_single_page_capacity_boundary_and_repeated_reuse_preserve_authority(
    tmp_path: Path, probe: ModuleType, payload_size: int
) -> None:
    original_resolve = SourceBuildRepository.resolve_discovery_locator
    locator = _locator_with_payload_size(payload_size)
    with _open_scenario(tmp_path, probe, locator) as scenario:
        assert scenario.upload.byte_count == payload_size
        for _ in range(3):
            scenario.claim()
            assert scenario.has_claim()
            with scenario.connector.transaction():
                reused = scenario.try_reuse()
            if payload_size > CANONICAL_VALUE_CHUNK_BYTES:
                assert reused is None
                assert scenario.has_claim()
                # The rejected fast path preserves the cleanup protection claim;
                # the real multi-page upload and original handoff can still run.
                _upload(
                    scenario.connector,
                    scenario.gate,
                    scenario.turn,
                    scenario.upload,
                    now=41,
                )
                with scenario.connector.transaction():
                    reused = original_resolve(
                        VNextUnitOfWork(scenario.connector, backend="sqlite"),
                        gate_lease=scenario.gate,
                        ingest_turn=scenario.turn,
                        batch=scenario.batch,
                        locator=scenario.batch.locators[0],
                        upload_plan=scenario.upload,
                        now=50,
                    )
            assert reused is not None and reused.replayed
            assert reused.gallery_id == scenario.initial.gallery_id
            assert reused.gallery_key == scenario.initial.gallery_key
            assert not scenario.has_claim()
            assert scenario.connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_gallery_identities"
            ) == (1,)
            assert scenario.connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_canonical_value_identities "
                "WHERE value_sha256 = %s",
                (scenario.upload.value_sha256,),
            ) == (1,)


def _candidate_targets(probe: ModuleType) -> tuple[object, ...]:
    return (
        SourceBuildRepository.resolve_discovery_locator,
        probe.facade.VNextIngestFacade.prepare_source_step,
        probe.facade.VNextIngestFacade.commit_source_step,
        probe.facade._apply_source_outcome,
    )


def test_experiment_restores_every_original_after_normal_exit(
    probe: ModuleType,
) -> None:
    original = _candidate_targets(probe)
    with probe.candidate():
        assert all(
            current is not previous
            for current, previous in zip(
                _candidate_targets(probe), original, strict=True
            )
        )
    assert _candidate_targets(probe) == original


def test_partial_installation_failure_restores_prior_patches(
    probe: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = _candidate_targets(probe)
    replace_once = probe.replace_once
    calls = 0

    def fail_after_two_patches(text: str, old: str, new: str) -> str:
        nonlocal calls
        calls += 1
        if calls == 4:
            current = _candidate_targets(probe)
            assert current[0] is not original[0]
            assert current[1] is not original[1]
            assert current[2:] == original[2:]
            raise RuntimeError("simulated source drift during partial installation")
        result = replace_once(text, old, new)
        assert isinstance(result, str)
        return result

    monkeypatch.setattr(probe, "replace_once", fail_after_two_patches)
    with pytest.raises(RuntimeError, match="source drift"):
        with probe.candidate():
            pytest.fail("candidate entered after partial installation failure")
    assert calls == 4
    assert _candidate_targets(probe) == original


def test_reuse_consumes_existing_claim_atomically_and_replays_response_loss(
    scenario: _Scenario,
) -> None:
    scenario.claim()
    assert scenario.has_claim()
    with pytest.raises(OSError, match="rollback"):
        with scenario.connector.transaction():
            first = scenario.reuse()
            assert not scenario.has_claim()
            raise OSError("rollback after exact claim deletion")
    assert scenario.has_claim()

    with pytest.raises(OSError, match="response loss"):
        with scenario.connector.transaction():
            committed = scenario.reuse()
        raise OSError("response loss after successful commit")
    assert not scenario.has_claim()
    with scenario.connector.transaction():
        replay = scenario.reuse()
    assert first == committed == replay
    assert replay.replayed
    assert replay.gallery_id == scenario.initial.gallery_id
    assert scenario.connector.fetch_one(
        "SELECT COUNT(*) FROM catalog_gallery_identities"
    ) == (1,)
    assert scenario.connector.fetch_one(
        "SELECT COUNT(*) FROM catalog_canonical_value_identities "
        "WHERE value_sha256 = %s",
        (scenario.upload.value_sha256,),
    ) == (1,)


def test_reuse_rejects_stale_fence_without_consuming_live_claim(
    scenario: _Scenario,
) -> None:
    scenario.claim()
    stale = replace(scenario.turn, owner_token=b"s" * 16)
    with pytest.raises(IngestFenceUnavailableError):
        with scenario.connector.transaction():
            scenario.reuse(turn=stale)
    assert scenario.has_claim()


@pytest.mark.parametrize(
    "corruption",
    ["page_payload", "parent_edge", "identity", "allocator", "locator_leaf"],
)
def test_fresh_durable_validation_rejects_corruption_after_local_preparation(
    scenario: _Scenario, corruption: str
) -> None:
    scenario.claim()
    connector = scenario.connector
    root = scenario.upload.root_page_sha256
    with connector.transaction():
        if corruption == "page_payload":
            connector.execute(
                "UPDATE catalog_canonical_value_page_payloads SET page_bytes = %s "
                "WHERE page_sha256 = %s",
                (b"invalid canonical page", root),
            )
        elif corruption == "parent_edge":
            connector.execute(
                "INSERT INTO catalog_canonical_value_page_parents "
                "(parent_sha256, position, child_sha256) VALUES (%s, %s, %s)",
                (root, 0, root),
            )
        elif corruption == "identity":
            connector.execute(
                "UPDATE catalog_gallery_identities SET gallery_key = %s "
                "WHERE gallery_id = %s",
                (b"x" * 32, scenario.initial.gallery_id),
            )
        elif corruption == "allocator":
            connector.execute(
                "DELETE FROM operational_gallery_observation_allocators "
                "WHERE gallery_id = %s",
                (scenario.initial.gallery_id,),
            )
        else:
            connector.execute(
                "UPDATE catalog_source_locator_identity SET source_gallery_name = %s "
                "WHERE locator_sha256 = %s",
                (b"different", scenario.upload.value_sha256),
            )
    with pytest.raises((CanonicalValueCollisionError, SourceBuildConflictError)):
        with connector.transaction():
            scenario.reuse()
    assert scenario.has_claim()


def test_local_plan_tree_receipt_is_rechecked_against_durable_root(
    scenario: _Scenario,
) -> None:
    scenario.claim()
    scenario.upload._tree_receipt = replace(
        scenario.upload.tree_receipt, root_page_sha256=b"x" * 32
    )
    with pytest.raises(SourceBuildConflictError, match="sealed source locator"):
        with scenario.connector.transaction():
            scenario.reuse()
    assert scenario.has_claim()
