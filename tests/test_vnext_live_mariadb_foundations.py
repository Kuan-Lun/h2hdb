from __future__ import annotations

from typing import Any
from unittest.mock import patch

from h2hdb import CoreConfig
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.mariadb_connector import MariaDBConnector
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_gallery_identity_repository import (
    GalleryIdentityRepository,
    SourceLocatorCommand,
)
from h2hdb.vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_source_build_repository import (
    SourceBuildRepository,
    SourceRootBuildCommand,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


def _generated_mariadb(config: CoreConfig) -> MariaDBConnector:
    database = config.database
    connector = MariaDBConnector(
        host=database.host,
        port=database.port,
        user=database.user,
        password=database.password,
        database=database.database,
    )
    connector.connect()
    payload: Any = ARTIFACT["backends"]
    payload = payload["mariadb"]
    for _slice_id, statements in payload["slices"]:
        for _statement_id, _kind, _name, sql in statements:
            connector.execute(sql)
    for seed in payload["bootstrap_seeds"]:
        connector.execute(seed["sql"], seed["parameters"])
    connector.execute(
        "INSERT INTO catalog_manifest_policies "
        "(manifest_policy_id, manifest_algorithm_version, file_order_version) "
        "VALUES (%s, %s, %s)",
        (1, 1, 1),
    )
    return connector


def _authorities(connector: SQLConnector) -> tuple[GateLease, IngestTurn]:
    with connector.transaction():
        with patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=b"mariadb-gate-001",
        ):
            gate = MaintenanceGateRepository.claim_shared(
                VNextUnitOfWork(connector, backend="mariadb"),
                now=10,
                lease_duration=10_000,
            )
    with connector.transaction():
        turn = IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend="mariadb"),
            owner_token=b"mariadb-turn-001",
            now=11,
            lease_duration=10_000,
        )
    return gate, turn


def _put_plan(
    connector: SQLConnector,
    gate: GateLease,
    turn: IngestTurn,
    plan: CanonicalValueUploadPlan,
    *,
    now: int,
) -> None:
    with connector.transaction():
        CanonicalValueRepository.allocate(
            VNextUnitOfWork(connector, backend="mariadb"),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=now,
        )
    for page in plan.iter_pages():
        with connector.transaction():
            CanonicalValueRepository.put_page(
                VNextUnitOfWork(connector, backend="mariadb"),
                gate_lease=gate,
                ingest_turn=turn,
                plan=plan,
                prepared_page=page,
                now=now + 1,
            )
    with connector.transaction():
        CanonicalValueRepository.seal(
            VNextUnitOfWork(connector, backend="mariadb"),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=now + 2,
        )


def test_live_mariadb_canonical_source_and_gallery_identity_round_trip(
    mariadb_config: CoreConfig,
) -> None:
    connector = _generated_mariadb(mariadb_config)
    root_plan: CanonicalValueUploadPlan | None = None
    locator_plan: CanonicalValueUploadPlan | None = None
    try:
        gate, turn = _authorities(connector)
        build_id = b"mariadb-build001"
        root_command = SourceRootBuildCommand(("Volumes", "資料"), build_id, 20)
        root_plan = root_command.prepare_root_upload()
        _put_plan(connector, gate, turn, root_plan, now=20)
        with connector.transaction():
            source = SourceBuildRepository.handoff_root(
                VNextUnitOfWork(connector, backend="mariadb"),
                gate_lease=gate,
                ingest_turn=turn,
                command=root_command,
                root_plan=root_plan,
                now=23,
            )

        locator_command = SourceLocatorCommand(("nested", "畫廊 A"))
        locator_plan = locator_command.prepare_upload()
        _put_plan(connector, gate, turn, locator_plan, now=30)
        with connector.transaction():
            identity = GalleryIdentityRepository.handoff_locator(
                VNextUnitOfWork(connector, backend="mariadb"),
                gate_lease=gate,
                ingest_turn=turn,
                build_id=build_id,
                command=locator_command,
                locator_plan=locator_plan,
                now=33,
            )
        with connector.transaction():
            replay = GalleryIdentityRepository.handoff_locator(
                VNextUnitOfWork(connector, backend="mariadb"),
                gate_lease=gate,
                ingest_turn=turn,
                build_id=build_id,
                command=locator_command,
                locator_plan=locator_plan,
                now=34,
            )

        streamed: list[bytes] = []
        with connector.read_transaction():
            receipt = CanonicalValueRepository.stream_and_validate(
                VNextUnitOfWork(connector, backend="mariadb"),
                value_sha256=locator_command.locator_sha256,
                consume_provisional=streamed.append,
            )

        assert source.build_id == build_id
        assert source.generation == turn.generation
        assert identity.gallery_id == 1
        assert not identity.replayed
        assert replay.replayed
        assert replay == identity.__class__(
            identity.build_id,
            identity.gallery_id,
            identity.gallery_key,
            identity.scope_key,
            identity.locator_sha256,
            True,
        )
        assert receipt.value_sha256 == locator_command.locator_sha256
        assert b"".join(streamed) == b"".join(locator_plan.iter_payload_parts())
        assert connector.fetch_one(
            "SELECT source_gallery_name FROM catalog_source_locator_identity "
            "WHERE locator_sha256 = %s",
            (locator_command.locator_sha256,),
        ) == ("畫廊 A".encode(),)
    finally:
        if locator_plan is not None:
            locator_plan.close()
        if root_plan is not None:
            root_plan.close()
        connector.close()
