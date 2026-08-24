from __future__ import annotations

from typing import Any
from unittest.mock import patch

from vnext_catalog_registry_fixtures import seed_manifest_policy

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
from h2hdb.vnext_gallery_staging_repository import (
    BatchAttempt,
    DirectoryBatchCommand,
    DirectoryObservation,
    FileBatchCommand,
    GalleryObservationStagingRepository,
    MatchBatchCommand,
    MetadataBatchCommand,
    TagBatchCommand,
)
from h2hdb.vnext_identity import (
    GalleryObservationDirectoryFileType,
    GalleryObservationMetadata,
    encode_gallery_observation_metadata,
)
from h2hdb.vnext_ingest_fence_repository import IngestFenceRepository, IngestTurn
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_source_build_repository import (
    SourceBuildRepository,
    SourceDiscoveryPlan,
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
    seed_manifest_policy(connector)
    # A bootstrap replay may be read-only when the generated family is already
    # present; Connector/Python starts an implicit transaction for that SELECT.
    connector.commit()
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
        root_command = SourceRootBuildCommand(("Volumes", "資料"), build_id)
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

        with SourceDiscoveryPlan.from_locators((("nested", "畫廊 A"),)) as plan:
            with connector.read_transaction():
                discovery_batch = SourceBuildRepository.prepare_discovery_batch(
                    connector,
                    build_id=build_id,
                    plan=plan,
                )
            resolved = []
            for locator in discovery_batch.locators:
                with plan.prepare_locator_upload(locator) as upload:
                    _put_plan(connector, gate, turn, upload, now=30)
                    with connector.transaction():
                        resolved.append(
                            SourceBuildRepository.resolve_discovery_locator(
                                VNextUnitOfWork(connector, backend="mariadb"),
                                gate_lease=gate,
                                ingest_turn=turn,
                                batch=discovery_batch,
                                locator=locator,
                                upload_plan=upload,
                                now=33,
                            )
                        )
            with connector.transaction():
                discovery_receipt = SourceBuildRepository.commit_discovery_batch(
                    VNextUnitOfWork(connector, backend="mariadb"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    batch=discovery_batch,
                    resolved=tuple(resolved),
                    now=34,
                )
            with connector.read_transaction():
                terminal_batch = SourceBuildRepository.prepare_discovery_batch(
                    connector,
                    build_id=build_id,
                    plan=plan,
                )
            with connector.transaction():
                terminal_receipt = SourceBuildRepository.commit_discovery_batch(
                    VNextUnitOfWork(connector, backend="mariadb"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    batch=terminal_batch,
                    resolved=(),
                    now=35,
                )

        locator_command = SourceLocatorCommand(("nested", "畫廊 A"))
        locator_plan = locator_command.prepare_upload()
        _put_plan(connector, gate, turn, locator_plan, now=36)
        with connector.transaction():
            identity = GalleryIdentityRepository.handoff_locator(
                VNextUnitOfWork(connector, backend="mariadb"),
                gate_lease=gate,
                ingest_turn=turn,
                build_id=build_id,
                command=locator_command,
                locator_plan=locator_plan,
                now=39,
            )
        with connector.transaction():
            replay = GalleryIdentityRepository.handoff_locator(
                VNextUnitOfWork(connector, backend="mariadb"),
                gate_lease=gate,
                ingest_turn=turn,
                build_id=build_id,
                command=locator_command,
                locator_plan=locator_plan,
                now=40,
            )
        with connector.transaction():
            handle = GalleryObservationStagingRepository.begin_from_identity(
                VNextUnitOfWork(connector, backend="mariadb"),
                gate_lease=gate,
                ingest_turn=turn,
                identity=identity,
                now=41,
            )
        with connector.transaction():
            progress = GalleryObservationStagingRepository.begin_or_resume(
                VNextUnitOfWork(connector, backend="mariadb"),
                gate_lease=gate,
                ingest_turn=turn,
                build_id=identity.build_id,
                gallery_id=identity.gallery_id,
                now=42,
            )

        directories = tuple(
            DirectoryObservation(
                f"directory-{position:04d}".encode(),
                position,
                100 + position,
                1_000 + position,
                position,
                position,
                GalleryObservationDirectoryFileType.DIRECTORY,
            )
            for position in range(257)
        )
        with connector.transaction():
            directory_open = GalleryObservationStagingRepository.put_directories(
                VNextUnitOfWork(connector, backend="mariadb"),
                gate_lease=gate,
                ingest_turn=turn,
                handle=handle,
                command=DirectoryBatchCommand(
                    directories[:192],
                    False,
                    BatchAttempt(b"d" * 16, None),
                ),
                now=49,
            )

        component_receipts = []
        component_operations: tuple[tuple[Any, Any, int], ...] = (
            (
                GalleryObservationStagingRepository.put_files,
                FileBatchCommand((), True, BatchAttempt(b"f" * 16, None)),
                50,
            ),
            (
                GalleryObservationStagingRepository.put_directories,
                DirectoryBatchCommand(
                    directories[192:],
                    True,
                    BatchAttempt(b"e" * 16, b"d" * 16),
                ),
                51,
            ),
            (
                GalleryObservationStagingRepository.put_tags,
                TagBatchCommand((), True, BatchAttempt(b"t" * 16, None)),
                52,
            ),
            (
                GalleryObservationStagingRepository.put_metadata,
                MetadataBatchCommand(
                    encode_gallery_observation_metadata(
                        GalleryObservationMetadata(1, "", "", "", 1, 2, 3, 1, 0, 0)
                    ),
                    True,
                    BatchAttempt(b"m" * 16, None),
                ),
                53,
            ),
        )
        for operation, command, now in component_operations:
            with connector.transaction():
                component_receipts.append(
                    operation(
                        VNextUnitOfWork(connector, backend="mariadb"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        handle=handle,
                        command=command,
                        now=now,
                    )
                )
        with connector.transaction():
            match = GalleryObservationStagingRepository.match_files_to_directory(
                VNextUnitOfWork(connector, backend="mariadb"),
                gate_lease=gate,
                ingest_turn=turn,
                handle=handle,
                command=MatchBatchCommand(b"v" * 16, None),
                now=54,
            )
        with connector.transaction():
            staging_seal = GalleryObservationStagingRepository.seal(
                VNextUnitOfWork(connector, backend="mariadb"),
                gate_lease=gate,
                ingest_turn=turn,
                handle=handle,
                now=55,
            )

        streamed: list[bytes] = []
        with connector.read_transaction():
            receipt = CanonicalValueRepository.stream_and_validate(
                VNextUnitOfWork(connector, backend="mariadb"),
                value_sha256=locator_command.locator_sha256,
                consume_provisional=streamed.append,
            )
            directory_root = component_receipts[1].root_page_sha256
            assert directory_root is not None
            directory_root_child_count = connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_gallery_observation_page_children "
                "WHERE parent_sha256 = %s",
                (directory_root,),
            )

        assert source.build_id == build_id
        assert source.generation == turn.generation
        assert identity.gallery_id == 1
        assert len(resolved) == 1 and not resolved[0].replayed
        assert discovery_receipt.next_state == "OPEN"
        assert terminal_receipt.terminal and terminal_receipt.next_state == "COMPLETE"
        assert identity.replayed
        assert replay.replayed
        assert replay == identity.__class__(
            identity.build_id,
            identity.gallery_id,
            identity.gallery_key,
            identity.scope_key,
            identity.locator_sha256,
            True,
        )
        assert progress.handle == handle
        assert directory_open.state == "OPEN" and directory_open.cursor == 192
        assert all(receipt.state == "COMPLETE" for receipt in component_receipts)
        assert directory_root_child_count == (2,)
        assert match.state == "COMPLETE" and match.matched_count == 0
        assert staging_seal.state == "SEALED" and not staging_seal.replayed
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
