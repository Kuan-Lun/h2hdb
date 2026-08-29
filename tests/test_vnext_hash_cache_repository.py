from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from vnext_catalog_registry_fixtures import seed_manifest_policy

from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_hash_cache_repository import (
    FileHashCacheConflictError,
    FileHashObservationPlan,
    VNextHashCacheRepository,
)
from h2hdb.vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_source_build_repository import (
    SourceBuildManifestSummary,
    SourceBuildRepository,
    SourceRootBuildCommand,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


def _database(path: Path) -> SQLiteConnector:
    connector = SQLiteConnector(str(path))
    connector.connect()
    payload: Any = ARTIFACT["backends"]
    payload = payload["sqlite"]
    for _slice_id, statements in payload["slices"]:
        for _statement_id, _kind, _name, sql in statements:
            connector.execute(sql)
    for seed in payload["bootstrap_seeds"]:
        connector.execute(seed["sql"], seed["parameters"])
    seed_manifest_policy(connector)
    return connector


def _authorities(connector: SQLiteConnector) -> tuple[GateLease, IngestTurn]:
    with (
        connector.transaction(),
        patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=b"g" * 16,
        ),
    ):
        gate = MaintenanceGateRepository.claim_shared(
            VNextUnitOfWork(connector, backend="sqlite"),
            now=10,
            lease_duration=10_000,
        )
    with connector.transaction():
        turn = IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend="sqlite"),
            owner_token=b"i" * 16,
            now=11,
            lease_duration=10_000,
        )
    return gate, turn


def _put(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    plan: CanonicalValueUploadPlan,
    *,
    start: int,
) -> None:
    with connector.transaction():
        CanonicalValueRepository.allocate(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=start,
        )
    for page in plan.iter_pages():
        with connector.transaction():
            CanonicalValueRepository.put_page(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                plan=plan,
                prepared_page=page,
                now=start + 1,
            )
    with connector.transaction():
        CanonicalValueRepository.seal(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=start + 2,
        )


def _ready_build(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
) -> None:
    command = SourceRootBuildCommand(
        ("source",),
        SourceBuildManifestSummary.empty(),
    )
    root = command.prepare_root_upload()
    try:
        _put(connector, gate, turn, root, start=20)
        with connector.transaction():
            SourceBuildRepository.handoff_root(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                command=command,
                root_plan=root,
                now=23,
            )
    finally:
        root.close()


def _plans() -> tuple[CanonicalValueUploadPlan, CanonicalValueUploadPlan]:
    return (
        CanonicalValueUploadPlan.from_parts(
            "filesystem_source_identity_v1",
            (b"source-id-v1\0", b"/source/gallery/file.jpg"),
        ),
        CanonicalValueUploadPlan.from_parts(
            "filesystem_fingerprint_v1",
            (b"fingerprint-v1\0", b"stat-and-prefix"),
        ),
    )


def test_hash_cache_handoff_replay_exact_lookup_and_miss(tmp_path: Path) -> None:
    connector = _database(tmp_path / "hash-cache.sqlite3")
    source, fingerprint = _plans()
    try:
        gate, turn = _authorities(connector)
        _ready_build(connector, gate, turn)
        _put(connector, gate, turn, source, start=30)
        _put(connector, gate, turn, fingerprint, start=40)
        file_plan = FileHashObservationPlan.from_parts((b"file-", b"bytes"))
        with connector.transaction():
            first = VNextHashCacheRepository.handoff(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                source_plan=source,
                fingerprint_plan=fingerprint,
                file_plan=file_plan,
                observed_at=50,
                cached_at=51,
                now=52,
            )
        assert not first.replayed
        assert (
            connector.fetch_all(
                "SELECT 1 FROM operational_canonical_value_uploads "
                "WHERE value_sha256 IN (%s, %s)",
                (source.value_sha256, fingerprint.value_sha256),
            )
            == []
        )
        with connector.transaction():
            replay = VNextHashCacheRepository.handoff(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                source_plan=source,
                fingerprint_plan=fingerprint,
                file_plan=file_plan,
                observed_at=50,
                cached_at=51,
                now=53,
            )
        assert replay.replayed
        with connector.read_transaction():
            hit = VNextHashCacheRepository.lookup_exact(
                VNextUnitOfWork(connector, backend="sqlite"),
                source_plan=source,
                fingerprint_plan=fingerprint,
            )
        assert hit == replay

        missing_source = CanonicalValueUploadPlan.from_parts(
            "filesystem_source_identity_v1", (b"another",)
        )
        try:
            with connector.read_transaction():
                assert (
                    VNextHashCacheRepository.lookup_exact(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        source_plan=missing_source,
                        fingerprint_plan=fingerprint,
                    )
                    is None
                )
        finally:
            missing_source.close()
    finally:
        source.close()
        fingerprint.close()
        connector.close()


def test_hash_cache_handoff_crash_is_atomic_and_conflict_is_fail_closed(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "hash-cache-crash.sqlite3")
    source, fingerprint = _plans()
    try:
        gate, turn = _authorities(connector)
        _ready_build(connector, gate, turn)
        _put(connector, gate, turn, source, start=30)
        _put(connector, gate, turn, fingerprint, start=40)
        file_plan = FileHashObservationPlan.from_parts((b"payload",))
        with pytest.raises(RuntimeError, match="crash"):
            with connector.transaction():
                VNextHashCacheRepository.handoff(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    source_plan=source,
                    fingerprint_plan=fingerprint,
                    file_plan=file_plan,
                    observed_at=50,
                    cached_at=51,
                    now=52,
                )
                raise RuntimeError("crash")
        assert (
            connector.fetch_all("SELECT 1 FROM operational_hash_cache_observations")
            == []
        )
        assert (
            len(
                connector.fetch_all(
                    "SELECT 1 FROM operational_canonical_value_uploads "
                    "WHERE value_sha256 IN (%s, %s)",
                    (source.value_sha256, fingerprint.value_sha256),
                )
            )
            == 2
        )
        with connector.transaction():
            VNextHashCacheRepository.handoff(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                source_plan=source,
                fingerprint_plan=fingerprint,
                file_plan=file_plan,
                observed_at=50,
                cached_at=51,
                now=52,
            )
        with (
            connector.transaction(),
            pytest.raises(FileHashCacheConflictError, match="exact tuple"),
        ):
            VNextHashCacheRepository.handoff(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                source_plan=source,
                fingerprint_plan=fingerprint,
                file_plan=FileHashObservationPlan.from_parts((b"changed",)),
                observed_at=50,
                cached_at=51,
                now=53,
            )
    finally:
        source.close()
        fingerprint.close()
        connector.close()
