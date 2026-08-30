from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from vnext_catalog_registry_fixtures import seed_manifest_policy
from vnext_generated_database import open_generated_sqlite_database

from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_gallery_identity_repository import (
    GalleryIdentityConflictError,
    GalleryIdentityHandoff,
    GalleryIdentityRepository,
    SourceLocatorCommand,
)
from h2hdb.vnext_identity import gallery_key
from h2hdb.vnext_ingest_fence_repository import (
    IngestFenceRepository,
    IngestFenceUnavailableError,
    IngestTurn,
)
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


def _generated_database(path: Path) -> SQLiteConnector:
    connector = open_generated_sqlite_database(path)
    seed_manifest_policy(connector)
    return connector


def _authorities(connector: SQLiteConnector) -> tuple[GateLease, IngestTurn]:
    with connector.transaction():
        with patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=b"g" * 16,
        ):
            gate = MaintenanceGateRepository.claim_shared(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=10,
                lease_duration=1_000_000,
            )
    with connector.transaction():
        turn = IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend="sqlite"),
            owner_token=b"i" * 16,
            now=11,
            lease_duration=1_000_000,
        )
    return gate, turn


def _put_plan(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    plan: CanonicalValueUploadPlan,
    *,
    now: int,
) -> None:
    with connector.transaction():
        CanonicalValueRepository.allocate(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=now,
        )
    for page in plan.iter_pages():
        with connector.transaction():
            CanonicalValueRepository.put_page(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                plan=plan,
                prepared_page=page,
                now=now + 1,
            )
    with connector.transaction():
        CanonicalValueRepository.seal(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=now + 2,
        )


def _working_build(
    connector: SQLiteConnector,
) -> tuple[GateLease, IngestTurn, bytes, CanonicalValueUploadPlan]:
    gate, turn = _authorities(connector)
    root_command = SourceRootBuildCommand(
        ("Volumes", "資料"),
        SourceBuildManifestSummary.empty(),
    )
    root_plan = root_command.prepare_root_upload()
    _put_plan(connector, gate, turn, root_plan, now=20)
    with connector.transaction():
        handoff = SourceBuildRepository.handoff_root(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            command=root_command,
            root_plan=root_plan,
            now=23,
        )
    return gate, turn, handoff.build_id, root_plan


def _handoff(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    build_id: bytes,
    command: SourceLocatorCommand,
    plan: CanonicalValueUploadPlan,
    *,
    now: int,
) -> GalleryIdentityHandoff:
    with connector.transaction():
        return GalleryIdentityRepository.handoff_locator(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            build_id=build_id,
            command=command,
            locator_plan=plan,
            now=now,
        )


def test_locator_handoff_derives_identity_allocator_and_response_loss_replay(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "gallery-identity.sqlite3")
    gate, turn, build_id, root_plan = _working_build(connector)
    command = SourceLocatorCommand(("nested", "畫廊 A"))
    plan = command.prepare_upload()
    try:
        _put_plan(connector, gate, turn, plan, now=30)
        with patch.object(
            CanonicalValueUploadPlan,
            "iter_payload_parts",
            side_effect=AssertionError("handoff must not scan locator bytes"),
        ):
            result = _handoff(
                connector,
                gate,
                turn,
                build_id,
                command,
                plan,
                now=33,
            )
        scope = connector.fetch_one(
            "SELECT scope_key FROM catalog_source_builds WHERE build_id = %s",
            (build_id,),
        )[0]
        assert result.gallery_id == 1
        assert result.scope_key == scope
        assert result.gallery_key == gallery_key(scope, command.locator_sha256)
        assert not result.replayed
        assert connector.fetch_one(
            "SELECT source_gallery_name FROM catalog_source_locator_identity "
            "WHERE locator_sha256 = %s",
            (command.locator_sha256,),
        ) == ("畫廊 A".encode(),)
        assert connector.fetch_one(
            "SELECT gallery_id, gallery_key, scope_key, locator_sha256 "
            "FROM catalog_gallery_identities WHERE gallery_id = 1"
        ) == (
            1,
            result.gallery_key,
            scope,
            command.locator_sha256,
        )
        assert connector.fetch_one(
            "SELECT next_observation_id FROM "
            "operational_gallery_observation_allocators WHERE gallery_id = 1"
        ) == (1,)
        assert connector.fetch_one(
            "SELECT next_id FROM operational_identity_allocators WHERE stream = %s",
            ("GALLERY",),
        ) == (2,)
        assert (
            connector.fetch_all(
                "SELECT 1 FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                (turn.generation, command.locator_sha256),
            )
            == []
        )

        # A lost successful response is recoverable without reconstructing a
        # claim or consuming another surrogate ID.
        replay = _handoff(
            connector,
            gate,
            turn,
            build_id,
            command,
            plan,
            now=34,
        )
        assert replay.replayed and replay.gallery_id == result.gallery_id
        assert connector.fetch_one(
            "SELECT next_id FROM operational_identity_allocators WHERE stream = %s",
            ("GALLERY",),
        ) == (2,)
    finally:
        plan.close()
        root_plan.close()
        connector.close()


def test_locator_command_and_upload_plan_must_be_exact(tmp_path: Path) -> None:
    with pytest.raises(Exception):
        SourceLocatorCommand(())
    with pytest.raises(Exception):
        SourceLocatorCommand((".",))
    with pytest.raises(TypeError):
        SourceLocatorCommand(["gallery"])  # type: ignore[arg-type]

    connector = _generated_database(tmp_path / "wrong-plan.sqlite3")
    gate, turn, build_id, root_plan = _working_build(connector)
    command = SourceLocatorCommand(("gallery-a",))
    other = SourceLocatorCommand(("gallery-b",))
    plan = other.prepare_upload()
    try:
        _put_plan(connector, gate, turn, plan, now=30)
        with pytest.raises(GalleryIdentityConflictError, match="exact command"):
            _handoff(
                connector,
                gate,
                turn,
                build_id,
                command,
                plan,
                now=33,
            )
        assert connector.fetch_all("SELECT 1 FROM catalog_gallery_identities") == []
        assert connector.fetch_one(
            "SELECT generation FROM operational_canonical_value_uploads "
            "WHERE value_sha256 = %s",
            (plan.value_sha256,),
        ) == (turn.generation,)
    finally:
        plan.close()
        root_plan.close()
        connector.close()


def test_stale_fence_and_immutable_locator_conflict_are_zero_write(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "fenced-gallery.sqlite3")
    gate, turn, build_id, root_plan = _working_build(connector)
    command = SourceLocatorCommand(("gallery",))
    plan = command.prepare_upload()
    try:
        _put_plan(connector, gate, turn, plan, now=30)
        stale = IngestTurn(turn.generation, turn.owner_token, 1)
        with pytest.raises(IngestFenceUnavailableError):
            _handoff(
                connector,
                gate,
                stale,
                build_id,
                command,
                plan,
                now=33,
            )
        assert connector.fetch_all("SELECT 1 FROM catalog_gallery_identities") == []
        assert connector.fetch_one(
            "SELECT next_id FROM operational_identity_allocators WHERE stream = %s",
            ("GALLERY",),
        ) == (1,)

        connector.execute(
            "INSERT INTO catalog_source_locator_identity "
            "(locator_sha256, source_gallery_name) VALUES (%s, %s)",
            (command.locator_sha256, b"wrong-leaf"),
        )
        with pytest.raises(GalleryIdentityConflictError, match="locator leaf"):
            _handoff(
                connector,
                gate,
                turn,
                build_id,
                command,
                plan,
                now=34,
            )
        assert connector.fetch_all("SELECT 1 FROM catalog_gallery_identities") == []
        assert connector.fetch_one(
            "SELECT next_id FROM operational_identity_allocators WHERE stream = %s",
            ("GALLERY",),
        ) == (1,)
    finally:
        plan.close()
        root_plan.close()
        connector.close()


@pytest.mark.parametrize(
    ("method_name", "fragment"),
    (
        ("execute", "INSERT INTO catalog_source_locator_identity"),
        ("execute", "INSERT INTO catalog_gallery_identities"),
        ("execute", "INSERT INTO operational_gallery_observation_allocators"),
        ("execute_affected", "DELETE FROM operational_canonical_value_uploads"),
    ),
)
def test_handoff_rolls_back_each_major_statement_fault(
    tmp_path: Path,
    method_name: str,
    fragment: str,
) -> None:
    connector = _generated_database(tmp_path / f"fault-{method_name}.sqlite3")
    gate, turn, build_id, root_plan = _working_build(connector)
    command = SourceLocatorCommand(("fault-gallery",))
    plan = command.prepare_upload()
    try:
        _put_plan(connector, gate, turn, plan, now=30)
        original = getattr(connector, method_name)

        def fail_target(query: str, data: tuple[Any, ...] = ()) -> Any:
            if fragment in query:
                raise RuntimeError("injected gallery identity fault")
            return original(query, data)

        with patch.object(connector, method_name, side_effect=fail_target):
            with pytest.raises(RuntimeError, match="injected gallery identity fault"):
                _handoff(
                    connector,
                    gate,
                    turn,
                    build_id,
                    command,
                    plan,
                    now=33,
                )
        assert (
            connector.fetch_all("SELECT 1 FROM catalog_source_locator_identity") == []
        )
        assert connector.fetch_all("SELECT 1 FROM catalog_gallery_identities") == []
        assert (
            connector.fetch_all(
                "SELECT 1 FROM operational_gallery_observation_allocators"
            )
            == []
        )
        assert connector.fetch_one(
            "SELECT next_id FROM operational_identity_allocators WHERE stream = %s",
            ("GALLERY",),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT generation FROM operational_canonical_value_uploads "
            "WHERE value_sha256 = %s",
            (command.locator_sha256,),
        ) == (turn.generation,)
    finally:
        plan.close()
        root_plan.close()
        connector.close()


def test_gallery_identity_lookups_use_declared_keys(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "gallery-key-plans.sqlite3")
    try:
        digest = b"d" * 32
        scope = b"s" * 32
        lookups = (
            (
                "SELECT gallery_id FROM catalog_gallery_identities "
                "WHERE scope_key = %s AND locator_sha256 = %s",
                (scope, digest),
            ),
            (
                "SELECT gallery_id FROM catalog_gallery_identities "
                "WHERE gallery_key = %s",
                (digest,),
            ),
            (
                "SELECT source_gallery_name FROM catalog_source_locator_identity "
                "WHERE locator_sha256 = %s",
                (digest,),
            ),
            (
                "SELECT next_observation_id FROM "
                "operational_gallery_observation_allocators WHERE gallery_id = %s",
                (1,),
            ),
        )
        for sql, parameters in lookups:
            details = [
                str(row[3])
                for row in connector.fetch_all(
                    "EXPLAIN QUERY PLAN " + sql,
                    parameters,
                )
            ]
            assert details
            assert all("SCAN " not in detail for detail in details), details
            assert any("SEARCH " in detail for detail in details), details
    finally:
        connector.close()
