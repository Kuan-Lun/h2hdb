from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from shutil import copyfile
from typing import Any
from unittest.mock import patch

import pytest
from vnext_generated_database import open_generated_sqlite_database
from vnext_publication_fixtures import seed_publication_commit

from h2hdb import vnext_identity as identity
from h2hdb import vnext_ingest_publication as ingest_publication
from h2hdb.domain import (
    CatalogResourceKind,
    StorageObjectKey,
    VNextLibraryActivationCursor,
)
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_cleanup_repository import (
    CleanupBatchCommand,
    CleanupTargetKind,
    VNextCleanupRepository,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_publication_finalization_repository import (
    PublicationFinalizationAcknowledgement,
    PublicationFinalizationBatchReceipt,
    PublicationFinalizationConflictError,
    PublicationFinalizationCorruptionError,
    PublicationFinalizationPage,
    PublicationFinalizationRepository,
    PublicationFinalizationStorageEvidence,
    PublicationFinalizationUnavailableError,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

_CANDIDATE = b"c" * 16
_RECEIPT = b"r" * 16
_CHANNEL = b"default"
_ANALYSIS = b"a" * 16
_BUILD = b"b" * 16
_BASE_ANALYSIS = b"z" * 16


def _database(path: Path) -> SQLiteConnector:
    return open_generated_sqlite_database(path)


def _shared_gate(connector: SQLiteConnector) -> GateLease:
    with (
        connector.transaction(),
        patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=b"g" * 16,
        ),
    ):
        return MaintenanceGateRepository.claim_shared(
            VNextUnitOfWork(connector, backend="sqlite"),
            now=10,
            lease_duration=1_000,
        )


def _seed_prepared_artifact(
    connector: SQLiteConnector,
    *,
    gid: int,
    publication_key: bytes,
    artifact_sha256: bytes,
    storage_generation: int,
    resource_kind: CatalogResourceKind = CatalogResourceKind.ACQUISITION,
) -> bytes:
    size_bytes = 100 + storage_generation
    storage_key = StorageObjectKey(
        "opaque-v2",
        ("library", str(gid), resource_kind.value),
    )
    storage_key_sha256 = identity.artifact_storage_key_digest(
        storage_key.codec,
        storage_key.segments,
    )
    protection_token = identity.encode_artifact_protection_token(
        _CANDIDATE,
        publication_key,
        resource_kind.value,
        storage_key_sha256,
        storage_generation,
    )
    connector.execute(
        "INSERT INTO catalog_artifact_blobs "
        "(artifact_sha256, size_bytes) VALUES (%s, %s)",
        (artifact_sha256, size_bytes),
    )
    connector.execute(
        "INSERT INTO catalog_storage_object_key_identities "
        "(storage_object_key_sha256, key_codec, segment_count) "
        "VALUES (%s, %s, %s)",
        (
            storage_key_sha256,
            storage_key.codec.encode("ascii"),
            len(storage_key.segments),
        ),
    )
    for position, segment in enumerate(storage_key.segments):
        connector.execute(
            "INSERT INTO catalog_storage_object_key_segments "
            "(storage_object_key_sha256, segment_position, key_segment) "
            "VALUES (%s, %s, %s)",
            (storage_key_sha256, position, segment.encode("utf-8")),
        )
    connector.execute(
        "INSERT INTO catalog_prepared_artifacts "
        "(candidate_id, publication_key, resource_kind, "
        "storage_object_key_sha256, storage_generation, protection_token, state) "
        "VALUES (%s, %s, %s, %s, %s, %s, 'PREPARED')",
        (
            _CANDIDATE,
            publication_key,
            resource_kind.value.encode("ascii"),
            storage_key_sha256,
            storage_generation,
            protection_token,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_prepared_resource_blob "
        "(candidate_id, publication_key, resource_kind, storage_object_sha256) "
        "VALUES (%s, %s, %s, %s)",
        (
            _CANDIDATE,
            publication_key,
            resource_kind.value.encode("ascii"),
            artifact_sha256,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_prepared_storage_objects "
        "(candidate_id, publication_key, resource_kind, storage_object_sha256, "
        "size_bytes, modified_at) VALUES (%s, %s, %s, %s, %s, 1)",
        (
            _CANDIDATE,
            publication_key,
            resource_kind.value.encode("ascii"),
            artifact_sha256,
            size_bytes,
        ),
    )
    return protection_token


def _seed_publication(
    connector: SQLiteConnector,
    *,
    item_count: int,
) -> tuple[tuple[bytes, ...], tuple[bytes, ...]]:
    publication_keys = tuple(
        sorted(identity.publication_key(index + 1) for index in range(item_count))
    )
    tokens: list[bytes] = []
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        connector.execute(
            "INSERT INTO catalog_revision_descriptors "
            "(revision, publication_count, artifact_count) VALUES (1, %s, %s)",
            (item_count, item_count),
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_descriptors "
            "(source_revision, channel, snapshot_manifest_sha256) "
            "VALUES (1, %s, %s)",
            (_CHANNEL, b"s" * 32),
        )
        _seed_published_analysis_lineage(connector)
        connector.execute(
            "INSERT INTO catalog_artifact_adapter_policy "
            "(policy_fingerprint_sha256, adapter_id) VALUES (%s, %s)",
            (b"f" * 32, b"test-artifact-adapter"),
        )
        connector.execute(
            "INSERT INTO catalog_artifact_policy_semantics "
            "(policy_component_sha256, artifact_algorithm_version, "
            "policy_fingerprint_sha256) VALUES (%s, 2, %s)",
            (b"p" * 32, b"f" * 32),
        )
        connector.execute(
            "INSERT INTO catalog_artifact_policies "
            "(artifact_policy_id, policy_component_sha256) VALUES (1, %s)",
            (b"p" * 32,),
        )
        seed_publication_commit(
            connector,
            receipt_id=_RECEIPT,
            candidate_id=_CANDIDATE,
            revision=1,
            source_revision=1,
            generation=1,
            preparation_id=b"p" * 16,
            operational_policy_id=1,
            artifact_policy_id=1,
            display_title_policy_id=1,
            new_galleries=item_count,
            changed_galleries=0,
            removed_galleries=0,
            duplicate_losers=0,
            committed_at=20,
        )
        for index, publication_key in enumerate(publication_keys, start=1):
            connector.execute(
                "INSERT INTO catalog_publication_identities "
                "(publication_key, gid) VALUES (%s, %s)",
                (publication_key, index),
            )
            tokens.append(
                _seed_prepared_artifact(
                    connector,
                    gid=index,
                    publication_key=publication_key,
                    artifact_sha256=sha256(f"artifact-{index}".encode()).digest(),
                    storage_generation=index,
                )
            )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")
    assert connector.fetch_one(
        "SELECT state FROM catalog_publication_receipts WHERE receipt_id = %s",
        (_RECEIPT,),
    ) == ("DB_COMMITTED",)
    return publication_keys, tuple(tokens)


def _seed_published_analysis_lineage(connector: SQLiteConnector) -> None:
    connector.execute(
        "INSERT INTO catalog_source_build_descriptor "
        "(build_id, scope_key, manifest_policy_id, created_at) "
        "VALUES (%s, %s, 1, 10)",
        (_BUILD, b"k" * 32),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_states (build_id, state) "
        "VALUES (%s, 'SEALED')",
        (_BUILD,),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_sealed_ats (build_id, sealed_at) "
        "VALUES (%s, 11)",
        (_BUILD,),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_run_descriptor "
        "(analysis_id, build_id, policy_id, input_manifest_sha256, started_at) "
        "VALUES (%s, %s, 1, %s, 12)",
        (_ANALYSIS, _BUILD, b"i" * 32),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_run_states (analysis_id, state) "
        "VALUES (%s, 'COMPLETE')",
        (_ANALYSIS,),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_run_completed_ats "
        "(analysis_id, completed_at) VALUES (%s, 13)",
        (_ANALYSIS,),
    )
    connector.execute(
        "INSERT INTO catalog_analysis_state_ancestry "
        "(analysis_id, ancestor_depth, ancestor_analysis_id) "
        "VALUES (%s, 0, %s)",
        (_ANALYSIS, _ANALYSIS),
    )
    for state_component in sorted(identity.ANALYSIS_STATE_COMPONENTS):
        connector.execute(
            "INSERT INTO catalog_analysis_state_component_seals "
            "(analysis_id, state_component, row_count, sealed_at) "
            "VALUES (%s, %s, 0, 13)",
            (_ANALYSIS, state_component.encode("ascii")),
        )
    connector.execute(
        "INSERT INTO catalog_analysis_snapshot_manifest "
        "(analysis_id, snapshot_manifest_sha256) VALUES (%s, %s)",
        (_ANALYSIS, b"s" * 32),
    )
    connector.execute(
        "INSERT INTO catalog_publication_candidates "
        "(candidate_id, analysis_id, reserved_revision, artifact_policy_id, "
        "display_title_policy_id, artifacts_required, created_at) "
        "VALUES (%s, %s, 1, 1, 1, 1, 14)",
        (_CANDIDATE, _ANALYSIS),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_provenance "
        "(source_revision, analysis_id) VALUES (1, %s)",
        (_ANALYSIS,),
    )


def _seed_depth_zero_compaction_baseline(
    connector: SQLiteConnector,
    *,
    base_depth: int = 16,
) -> None:
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        connector.execute(
            "INSERT INTO catalog_analysis_run_descriptor "
            "(analysis_id, build_id, policy_id, input_manifest_sha256, started_at) "
            "VALUES (%s, %s, 1, %s, 1)",
            (_BASE_ANALYSIS, b"B" * 16, b"j" * 32),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_run_states (analysis_id, state) "
            "VALUES (%s, 'COMPLETE')",
            (_BASE_ANALYSIS,),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_run_completed_ats "
            "(analysis_id, completed_at) VALUES (%s, 2)",
            (_BASE_ANALYSIS,),
        )
        ancestors = (_BASE_ANALYSIS,) + tuple(
            bytes((index,)) * 16 for index in range(1, base_depth + 1)
        )
        for depth, ancestor in enumerate(ancestors):
            connector.execute(
                "INSERT INTO catalog_analysis_state_ancestry "
                "(analysis_id, ancestor_depth, ancestor_analysis_id) "
                "VALUES (%s, %s, %s)",
                (_BASE_ANALYSIS, depth, ancestor),
            )
        for state_component in sorted(identity.ANALYSIS_STATE_COMPONENTS):
            connector.execute(
                "INSERT INTO catalog_analysis_state_component_seals "
                "(analysis_id, state_component, row_count, sealed_at) "
                "VALUES (%s, %s, 0, 2)",
                (_BASE_ANALYSIS, state_component.encode("ascii")),
            )
        connector.execute(
            "INSERT INTO catalog_analysis_baselines "
            "(analysis_id, base_analysis_id) VALUES (%s, %s)",
            (_ANALYSIS, _BASE_ANALYSIS),
        )
        connector.execute(
            "INSERT INTO catalog_source_build_base_publication_commits "
            "(build_id, base_receipt_id) VALUES (%s, %s)",
            (_BUILD, b"q" * 16),
        )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")


def _seed_complete_seventeen_run_chain(
    connector: SQLiteConnector,
) -> tuple[bytes, ...]:
    analyses = tuple(
        b"z" + bytes((index,)) + bytes((index + 1,)) * 14 for index in range(16)
    ) + (_BASE_ANALYSIS,)
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        for index, analysis_id in enumerate(analyses):
            build_id = b"y" + bytes((index,)) + bytes((index + 1,)) * 14
            connector.execute(
                "INSERT INTO catalog_source_build_descriptor "
                "(build_id, scope_key, manifest_policy_id, created_at) "
                "VALUES (%s, %s, 1, 1)",
                (build_id, bytes((index + 1,)) * 32),
            )
            connector.execute(
                "INSERT INTO catalog_source_build_states (build_id, state) "
                "VALUES (%s, 'SEALED')",
                (build_id,),
            )
            connector.execute(
                "INSERT INTO catalog_source_build_sealed_ats "
                "(build_id, sealed_at) VALUES (%s, 1)",
                (build_id,),
            )
            connector.execute(
                "INSERT INTO catalog_analysis_run_descriptor "
                "(analysis_id, build_id, policy_id, input_manifest_sha256, "
                "started_at) VALUES (%s, %s, 1, %s, 1)",
                (analysis_id, build_id, bytes((index + 17,)) * 32),
            )
            connector.execute(
                "INSERT INTO catalog_analysis_run_states (analysis_id, state) "
                "VALUES (%s, 'COMPLETE')",
                (analysis_id,),
            )
            connector.execute(
                "INSERT INTO catalog_analysis_run_completed_ats "
                "(analysis_id, completed_at) VALUES (%s, 2)",
                (analysis_id,),
            )
            for state_component in sorted(identity.ANALYSIS_STATE_COMPONENTS):
                connector.execute(
                    "INSERT INTO catalog_analysis_state_component_seals "
                    "(analysis_id, state_component, row_count, sealed_at) "
                    "VALUES (%s, %s, 0, 2)",
                    (analysis_id, state_component.encode("ascii")),
                )
        for index, analysis_id in enumerate(analyses):
            suffix = tuple(reversed(analyses[: index + 1]))
            for depth, ancestor in enumerate(suffix):
                connector.execute(
                    "INSERT INTO catalog_analysis_state_ancestry "
                    "(analysis_id, ancestor_depth, ancestor_analysis_id) "
                    "VALUES (%s, %s, %s)",
                    (analysis_id, depth, ancestor),
                )
            if index:
                connector.execute(
                    "INSERT INTO catalog_analysis_baselines "
                    "(analysis_id, base_analysis_id) VALUES (%s, %s)",
                    (analysis_id, analyses[index - 1]),
                )
        connector.execute(
            "INSERT INTO catalog_analysis_baselines "
            "(analysis_id, base_analysis_id) VALUES (%s, %s)",
            (_ANALYSIS, analyses[-1]),
        )
        connector.execute(
            "INSERT INTO catalog_source_build_base_publication_commits "
            "(build_id, base_receipt_id) VALUES (%s, %s)",
            (_BUILD, b"q" * 16),
        )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")
    return analyses


def _claim_expired_shared_gate_exclusively(
    connector: SQLiteConnector,
    *,
    now: int,
) -> GateLease:
    with (
        connector.transaction(),
        patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=b"x" * 16,
        ),
    ):
        return MaintenanceGateRepository.claim_exclusive(
            VNextUnitOfWork(connector, backend="sqlite"),
            now=now,
            lease_duration=100_000,
        )


def _cleanup_analysis_shard_to_fixed_point(
    connector: SQLiteConnector,
    *,
    gate: GateLease,
    shard_no: int,
    now: int,
) -> tuple[int, ...]:
    timestamp = now
    remaining_counts: list[int] = []
    for cycle_index in range(18):
        with connector.transaction():
            cycle = VNextCleanupRepository.begin_cycle(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                target_kind=CleanupTargetKind.ANALYSIS_RUN,
                shard_no=shard_no,
                cycle_cutoff_at=100,
                max_rows_per_transaction=128,
                now=timestamp,
            )
        generation = 1
        for attempt in range(64):
            timestamp += 1
            with connector.transaction():
                result = VNextCleanupRepository.advance(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    cycle=cycle,
                    command=CleanupBatchCommand(
                        sha256(
                            b"fixed-point-cleanup"
                            + cycle_index.to_bytes(2, "big")
                            + attempt.to_bytes(2, "big")
                        ).digest(),
                        generation,
                    ),
                    now=timestamp,
                )
            if result.cycle_complete:
                break
            assert result.generation is not None
            generation = result.generation
        else:
            raise AssertionError("analysis cleanup cycle did not terminate")
        remaining = connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_analysis_run_descriptor "
            "WHERE SUBSTR(analysis_id, 1, 1) = %s AND analysis_id <> %s",
            (bytes((shard_no,)), _ANALYSIS),
        )
        assert remaining is not None
        remaining_counts.append(int(remaining[0]))
        if remaining == (0,):
            return tuple(remaining_counts)
        timestamp += 1
    raise AssertionError("analysis cleanup did not reach a fixed point")


def _issue(
    connector: SQLiteConnector,
    gate: GateLease,
    *,
    batch_key: bytes,
    page_limit: int = 128,
    now: int,
) -> PublicationFinalizationPage:
    return PublicationFinalizationRepository.issue_page(
        connector,
        backend="sqlite",
        gate_lease=gate,
        receipt_id=_RECEIPT,
        batch_key=batch_key,
        page_limit=page_limit,
        now=now,
    )


def _commit(
    connector: SQLiteConnector,
    acknowledgement: PublicationFinalizationAcknowledgement,
    *,
    now: int,
) -> PublicationFinalizationBatchReceipt:
    with connector.transaction():
        return PublicationFinalizationRepository.commit_page(
            VNextUnitOfWork(connector, backend="sqlite"),
            acknowledgement=acknowledgement,
            now=now,
        )


def _delete_transient_candidate_state(connector: SQLiteConnector) -> None:
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        connector.execute(
            "DELETE FROM catalog_prepared_artifacts WHERE candidate_id = %s",
            (_CANDIDATE,),
        )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")


class _MonotoneAdapter:
    adapter_id = b"test-artifact-adapter"

    def __init__(self, connector: SQLiteConnector | None = None) -> None:
        self.connector = connector
        self.calls: list[tuple[StorageObjectKey, bytes]] = []
        self.object_facts: list[tuple[StorageObjectKey, bytes, int, bytes]] = []
        self.tombstones: set[bytes] = set()

    def release(
        self,
        storage_key: StorageObjectKey,
        expected_sha256: bytes,
        expected_size_bytes: int,
        protection_token: bytes,
    ) -> PublicationFinalizationStorageEvidence:
        if self.connector is not None:
            with self.connector.read_transaction():
                assert self.connector.fetch_one("SELECT 1") == (1,)
        assert identity.decode_artifact_protection_token(protection_token) == (
            protection_token
        )
        assert len(expected_sha256) == 32
        assert expected_size_bytes > 0
        assert identity.artifact_storage_key_digest(
            storage_key.codec,
            storage_key.segments,
        )
        self.calls.append((storage_key, protection_token))
        self.object_facts.append(
            (
                storage_key,
                expected_sha256,
                expected_size_bytes,
                protection_token,
            )
        )
        self.tombstones.add(protection_token)
        return PublicationFinalizationStorageEvidence(True)


def test_current_batch_replays_after_cleanup_and_expired_gate(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "current-replay.sqlite3")
    try:
        _keys, tokens = _seed_publication(connector, item_count=1)
        gate = _shared_gate(connector)
        with pytest.raises(ValueError, match="capped at 128"):
            _issue(connector, gate, batch_key=b"too-large", page_limit=129, now=30)

        page = _issue(connector, gate, batch_key=b"page-1", now=30)
        assert _issue(connector, gate, batch_key=b"page-1", now=31) == page
        adapter = _MonotoneAdapter(connector)
        acknowledgement = PublicationFinalizationRepository.release_page(
            connector,
            backend="sqlite",
            page=page,
            adapters={adapter.adapter_id: adapter},
            now=32,
        )
        committed = _commit(connector, acknowledgement, now=33)
        assert committed.row_count == 1
        assert not committed.terminal
        assert [token for _locator, token in adapter.calls] == list(tokens)
        assert (
            PublicationFinalizationRepository.get_batch_receipt(
                connector,
                receipt_id=_RECEIPT,
                batch_key=b"page-1",
            )
            == committed
        )
        assert (
            PublicationFinalizationRepository.get_batch_receipt(
                connector,
                receipt_id=_RECEIPT,
                start_generation=1,
            )
            == committed
        )

        _delete_transient_candidate_state(connector)
        with (
            patch.object(
                connector,
                "execute",
                wraps=connector.execute,
            ) as execute,
            patch.object(
                connector,
                "execute_affected",
                wraps=connector.execute_affected,
            ) as execute_affected,
        ):
            replayed = _commit(connector, acknowledgement, now=5_000)
        assert replayed == committed
        execute.assert_not_called()
        execute_affected.assert_not_called()
    finally:
        connector.close()


def test_same_publication_resources_finalize_as_distinct_coordinates(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "multi-resource-finalization.sqlite3")
    try:
        publication_keys, acquisition_tokens = _seed_publication(
            connector,
            item_count=1,
        )
        connector.execute("PRAGMA foreign_keys = OFF")
        try:
            thumbnail_token = _seed_prepared_artifact(
                connector,
                gid=1,
                publication_key=publication_keys[0],
                artifact_sha256=b"t" * 32,
                storage_generation=2,
                resource_kind=CatalogResourceKind.THUMBNAIL,
            )
        finally:
            connector.execute("PRAGMA foreign_keys = ON")
        gate = _shared_gate(connector)
        page = _issue(
            connector,
            gate,
            batch_key=b"resource-page",
            page_limit=2,
            now=30,
        )
        assert tuple(item.resource_kind for item in page.items) == (
            CatalogResourceKind.ACQUISITION,
            CatalogResourceKind.THUMBNAIL,
        )
        assert (
            page.next_cursor
            == VNextLibraryActivationCursor(
                publication_keys[0],
                CatalogResourceKind.THUMBNAIL,
            ).to_bytes()
        )

        adapter = _MonotoneAdapter()
        acknowledgement = PublicationFinalizationRepository.release_page(
            connector,
            backend="sqlite",
            page=page,
            adapters={adapter.adapter_id: adapter},
            now=31,
        )
        receipt = _commit(connector, acknowledgement, now=32)
        assert receipt.row_count == 2
        assert receipt.next_processed_count == 2
        assert {token for _key, token in adapter.calls} == {
            acquisition_tokens[0],
            thumbnail_token,
        }
        assert connector.fetch_all(
            "SELECT resource_kind, state FROM catalog_prepared_artifacts "
            "WHERE candidate_id = %s ORDER BY publication_key, resource_kind",
            (_CANDIDATE,),
        ) == [
            (b"acquisition", "COMMITTED"),
            (b"thumbnail", "COMMITTED"),
        ]

        terminal_page = _issue(
            connector,
            gate,
            batch_key=b"resource-terminal",
            page_limit=2,
            now=33,
        )
        assert terminal_page.terminal and terminal_page.items == ()
        terminal_ack = PublicationFinalizationRepository.release_page(
            connector,
            backend="sqlite",
            page=terminal_page,
            adapters={},
            now=34,
        )
        assert _commit(connector, terminal_ack, now=35).terminal
    finally:
        connector.close()


def test_ingest_release_consumer_uses_sealed_storage_descriptor(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "ingest-release-descriptor.sqlite3")
    try:
        _publication_keys, tokens = _seed_publication(connector, item_count=1)
        gate = _shared_gate(connector)
        page = _issue(
            connector,
            gate,
            batch_key=b"ingest-resource-page",
            now=30,
        )
        adapter = _MonotoneAdapter()

        acknowledgement = ingest_publication._release_finalization_page(
            page,
            {adapter.adapter_id: adapter},
        )

        assert acknowledgement.page == page
        assert len(adapter.object_facts) == 1
        key, digest, size_bytes, token = adapter.object_facts[0]
        assert key == page.items[0].storage_object.key
        assert digest == bytes.fromhex(page.items[0].storage_object.sha256)
        assert size_bytes == page.items[0].storage_object.size_bytes
        assert token == tokens[0]
    finally:
        connector.close()


def test_terminal_marker_checkpoint_and_finalized_at_are_one_derived_commit(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "terminal.sqlite3")
    try:
        publication_keys, tokens = _seed_publication(connector, item_count=2)
        gate = _shared_gate(connector)
        adapter = _MonotoneAdapter()

        acknowledgements: list[PublicationFinalizationAcknowledgement] = []
        receipts: list[PublicationFinalizationBatchReceipt] = []
        now = 30
        for index in range(2):
            page = _issue(
                connector,
                gate,
                batch_key=f"page-{index}".encode(),
                page_limit=1,
                now=now,
            )
            assert len(page.items) == 1
            acknowledgement = PublicationFinalizationRepository.release_page(
                connector,
                backend="sqlite",
                page=page,
                adapters={adapter.adapter_id: adapter},
                now=now + 1,
            )
            receipt = _commit(connector, acknowledgement, now=now + 2)
            assert receipt.next_state == "OPEN"
            acknowledgements.append(acknowledgement)
            receipts.append(receipt)
            assert connector.fetch_all(
                "SELECT start_generation, batch_key "
                "FROM catalog_publication_finalization_batch_stored "
                "WHERE receipt_id = %s",
                (_RECEIPT,),
            ) == [(index + 1, f"page-{index}".encode())]
            now += 3

        with (
            patch.object(connector, "execute", wraps=connector.execute) as execute,
            patch.object(
                connector,
                "execute_affected",
                wraps=connector.execute_affected,
            ) as execute_affected,
        ):
            assert _commit(connector, acknowledgements[-1], now=now) == receipts[-1]
        execute.assert_not_called()
        execute_affected.assert_not_called()
        assert (
            PublicationFinalizationRepository.get_batch_receipt(
                connector,
                receipt_id=_RECEIPT,
                batch_key=b"page-0",
            )
            is None
        )
        assert (
            PublicationFinalizationRepository.get_batch_receipt(
                connector,
                receipt_id=_RECEIPT,
                start_generation=1,
            )
            is None
        )
        with pytest.raises(
            PublicationFinalizationConflictError,
            match="checkpoint advanced",
        ):
            _commit(connector, acknowledgements[0], now=now)

        terminal_page = _issue(
            connector,
            gate,
            batch_key=b"terminal",
            page_limit=1,
            now=now,
        )
        assert terminal_page.terminal and terminal_page.items == ()
        terminal_ack = PublicationFinalizationRepository.release_page(
            connector,
            backend="sqlite",
            page=terminal_page,
            adapters={},
            now=now + 1,
        )
        terminal = _commit(connector, terminal_ack, now=now + 2)
        assert terminal.terminal
        assert terminal.row_count == 0
        assert terminal.next_state == "COMPLETE"
        assert connector.fetch_all(
            "SELECT start_generation, batch_key "
            "FROM catalog_publication_finalization_batch_stored "
            "WHERE receipt_id = %s",
            (_RECEIPT,),
        ) == [(3, b"terminal")]
        assert connector.fetch_one(
            "SELECT generation, cursor, processed_count, state, updated_at "
            "FROM catalog_publication_finalization_checkpoints "
            "WHERE receipt_id = %s",
            (_RECEIPT,),
        ) == (
            4,
            VNextLibraryActivationCursor(
                publication_keys[-1],
                CatalogResourceKind.ACQUISITION,
            ).to_bytes(),
            2,
            "COMPLETE",
            now + 2,
        )
        assert connector.fetch_one(
            "SELECT state, committed_at, finalized_at "
            "FROM catalog_publication_receipts WHERE receipt_id = %s",
            (_RECEIPT,),
        ) == ("PUBLISHED", 20, now + 2)
        assert connector.fetch_one(
            "SELECT receipt_id FROM catalog_publication_commit_finalizations",
        ) == (_RECEIPT,)
        marker_columns = connector.fetch_all(
            "PRAGMA table_info(catalog_publication_commit_finalizations)"
        )
        assert [(row[1], row[5]) for row in marker_columns] == [("receipt_id", 1)]
        assert [token for _locator, token in adapter.calls] == list(tokens)

        _delete_transient_candidate_state(connector)
        assert _commit(connector, terminal_ack, now=5_000) == terminal
        assert connector.fetch_one(
            "SELECT state, finalized_at FROM catalog_publication_receipts "
            "WHERE receipt_id = %s",
            (_RECEIPT,),
        ) == ("PUBLISHED", now + 2)
        with pytest.raises(PublicationFinalizationUnavailableError):
            _issue(connector, gate, batch_key=b"after-terminal", now=40)
    finally:
        connector.close()


def test_terminal_handoff_prunes_only_the_published_depth_zero_working_baseline(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "terminal-baseline-prune.sqlite3")
    try:
        _seed_publication(connector, item_count=0)
        _seed_depth_zero_compaction_baseline(connector)
        gate = _shared_gate(connector)
        page = _issue(
            connector,
            gate,
            batch_key=b"terminal-prune",
            now=30,
        )
        assert page.terminal
        acknowledgement = PublicationFinalizationRepository.release_page(
            connector,
            backend="sqlite",
            page=page,
            adapters={},
            now=31,
        )
        assert connector.fetch_one(
            "SELECT base_analysis_id FROM catalog_analysis_baselines "
            "WHERE analysis_id = %s",
            (_ANALYSIS,),
        ) == (_BASE_ANALYSIS,)

        receipt = _commit(connector, acknowledgement, now=32)

        assert receipt.terminal
        assert (
            connector.fetch_one(
                "SELECT base_analysis_id FROM catalog_analysis_baselines "
                "WHERE analysis_id = %s",
                (_ANALYSIS,),
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT state FROM catalog_publication_receipts WHERE receipt_id = %s",
            (_RECEIPT,),
        ) == ("PUBLISHED",)
        assert connector.fetch_one(
            "SELECT ancestor_analysis_id "
            "FROM catalog_analysis_state_ancestry "
            "WHERE analysis_id = %s AND ancestor_depth = 0",
            (_ANALYSIS,),
        ) == (_ANALYSIS,)

        with (
            patch.object(connector, "execute", wraps=connector.execute) as execute,
            patch.object(
                connector,
                "execute_affected",
                wraps=connector.execute_affected,
            ) as execute_affected,
        ):
            assert _commit(connector, acknowledgement, now=5_000) == receipt
        execute.assert_not_called()
        execute_affected.assert_not_called()
    finally:
        connector.close()


def test_terminal_handoff_retains_a_positive_depth_immediate_baseline(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "terminal-positive-depth.sqlite3")
    try:
        _seed_publication(connector, item_count=0)
        _seed_depth_zero_compaction_baseline(connector, base_depth=0)
        connector.execute(
            "INSERT INTO catalog_analysis_state_ancestry "
            "(analysis_id, ancestor_depth, ancestor_analysis_id) "
            "VALUES (%s, 1, %s)",
            (_ANALYSIS, _BASE_ANALYSIS),
        )
        gate = _shared_gate(connector)
        page = _issue(
            connector,
            gate,
            batch_key=b"terminal-positive-depth",
            now=30,
        )
        acknowledgement = PublicationFinalizationRepository.release_page(
            connector,
            backend="sqlite",
            page=page,
            adapters={},
            now=31,
        )

        receipt = _commit(connector, acknowledgement, now=32)

        assert receipt.terminal
        assert connector.fetch_one(
            "SELECT base_analysis_id FROM catalog_analysis_baselines "
            "WHERE analysis_id = %s",
            (_ANALYSIS,),
        ) == (_BASE_ANALYSIS,)
    finally:
        connector.close()


def test_non_genesis_terminal_handoff_rejects_a_missing_baseline_zero_write(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "terminal-missing-baseline.sqlite3")
    try:
        _seed_publication(connector, item_count=0)
        _seed_depth_zero_compaction_baseline(connector)
        connector.execute(
            "DELETE FROM catalog_analysis_baselines WHERE analysis_id = %s",
            (_ANALYSIS,),
        )
        gate = _shared_gate(connector)
        page = _issue(
            connector,
            gate,
            batch_key=b"terminal-missing-baseline",
            now=30,
        )
        acknowledgement = PublicationFinalizationRepository.release_page(
            connector,
            backend="sqlite",
            page=page,
            adapters={},
            now=31,
        )

        with pytest.raises(
            PublicationFinalizationCorruptionError,
            match="lost its working baseline",
        ):
            _commit(connector, acknowledgement, now=32)

        assert (
            connector.fetch_one(
                "SELECT receipt_id FROM catalog_publication_commit_finalizations "
                "WHERE receipt_id = %s",
                (_RECEIPT,),
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT generation, state FROM "
            "catalog_publication_finalization_checkpoints WHERE receipt_id = %s",
            (_RECEIPT,),
        ) == (1, "OPEN")
        assert (
            connector.fetch_one(
                "SELECT batch_key FROM catalog_publication_finalization_batch_stored "
                "WHERE receipt_id = %s",
                (_RECEIPT,),
            )
            == ()
        )
    finally:
        connector.close()


def test_depth_sixteen_compaction_prune_releases_the_old_chain_to_fixed_point_cleanup(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "terminal-seventeen-chain-cleanup.sqlite3")
    try:
        _seed_publication(connector, item_count=0)
        old_chain = _seed_complete_seventeen_run_chain(connector)
        gate = _shared_gate(connector)
        page = _issue(
            connector,
            gate,
            batch_key=b"terminal-seventeen-chain",
            now=30,
        )
        acknowledgement = PublicationFinalizationRepository.release_page(
            connector,
            backend="sqlite",
            page=page,
            adapters={},
            now=31,
        )

        receipt = _commit(connector, acknowledgement, now=32)

        assert receipt.terminal
        assert (
            connector.fetch_one(
                "SELECT base_analysis_id FROM catalog_analysis_baselines "
                "WHERE analysis_id = %s",
                (_ANALYSIS,),
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_analysis_run_descriptor "
            "WHERE SUBSTR(analysis_id, 1, 1) = %s",
            (b"z",),
        ) == (17,)

        exclusive = _claim_expired_shared_gate_exclusively(connector, now=2_000)
        remaining_counts = _cleanup_analysis_shard_to_fixed_point(
            connector,
            gate=exclusive,
            shard_no=old_chain[0][0],
            now=2_001,
        )
        assert remaining_counts == tuple(range(16, -1, -1))

        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_analysis_run_descriptor "
            "WHERE SUBSTR(analysis_id, 1, 1) = %s",
            (b"z",),
        ) == (0,)
        assert connector.fetch_one(
            "SELECT analysis_id FROM catalog_analysis_run_descriptor "
            "WHERE analysis_id = %s",
            (_ANALYSIS,),
        ) == (_ANALYSIS,)
    finally:
        connector.close()


@pytest.mark.parametrize("fail_at", range(1, 5))
def test_terminal_baseline_prune_rolls_back_with_every_database_mutation(
    tmp_path: Path,
    fail_at: int,
) -> None:
    base_path = tmp_path / "terminal-prune-fault-base.sqlite3"
    base = _database(base_path)
    _seed_publication(base, item_count=0)
    _seed_depth_zero_compaction_baseline(base)
    gate = _shared_gate(base)
    page = _issue(base, gate, batch_key=b"terminal-prune-fault", now=30)
    acknowledgement = PublicationFinalizationRepository.release_page(
        base,
        backend="sqlite",
        page=page,
        adapters={},
        now=31,
    )
    base.close()

    fault_path = tmp_path / f"terminal-prune-fault-{fail_at}.sqlite3"
    copyfile(base_path, fault_path)
    connector = SQLiteConnector(str(fault_path))
    connector.connect()
    try:
        with pytest.raises(
            _InjectedFault, match=f"fault at finalization mutation {fail_at}"
        ):
            _commit_with_injected_fault(
                connector,
                acknowledgement,
                fail_at=fail_at,
            )
        assert connector.fetch_one(
            "SELECT base_analysis_id FROM catalog_analysis_baselines "
            "WHERE analysis_id = %s",
            (_ANALYSIS,),
        ) == (_BASE_ANALYSIS,)
        assert (
            connector.fetch_one(
                "SELECT receipt_id FROM catalog_publication_commit_finalizations "
                "WHERE receipt_id = %s",
                (_RECEIPT,),
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT generation, state FROM "
            "catalog_publication_finalization_checkpoints WHERE receipt_id = %s",
            (_RECEIPT,),
        ) == (1, "OPEN")
        assert (
            connector.fetch_one(
                "SELECT batch_key FROM catalog_publication_finalization_batch_stored "
                "WHERE receipt_id = %s",
                (_RECEIPT,),
            )
            == ()
        )
    finally:
        connector.close()


def test_missing_finalization_predecessor_rolls_back_successor_and_checkpoint(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "missing-predecessor.sqlite3")
    try:
        publication_keys, _tokens = _seed_publication(connector, item_count=2)
        gate = _shared_gate(connector)
        adapter = _MonotoneAdapter()

        first_page = _issue(
            connector,
            gate,
            batch_key=b"predecessor",
            page_limit=1,
            now=30,
        )
        first_ack = PublicationFinalizationRepository.release_page(
            connector,
            backend="sqlite",
            page=first_page,
            adapters={adapter.adapter_id: adapter},
            now=31,
        )
        _commit(connector, first_ack, now=32)
        connector.execute(
            "DELETE FROM catalog_publication_finalization_batch_stored "
            "WHERE receipt_id = %s AND start_generation = %s",
            (_RECEIPT, 1),
        )

        second_page = _issue(
            connector,
            gate,
            batch_key=b"successor",
            page_limit=1,
            now=33,
        )
        second_ack = PublicationFinalizationRepository.release_page(
            connector,
            backend="sqlite",
            page=second_page,
            adapters={adapter.adapter_id: adapter},
            now=34,
        )
        with pytest.raises(
            PublicationFinalizationCorruptionError,
            match="predecessor",
        ):
            _commit(connector, second_ack, now=35)

        assert connector.fetch_one(
            "SELECT generation, cursor, processed_count, state, updated_at "
            "FROM catalog_publication_finalization_checkpoints "
            "WHERE receipt_id = %s",
            (_RECEIPT,),
        ) == (
            2,
            VNextLibraryActivationCursor(
                publication_keys[0],
                CatalogResourceKind.ACQUISITION,
            ).to_bytes(),
            1,
            "OPEN",
            32,
        )
        assert connector.fetch_all(
            "SELECT publication_key, state FROM catalog_prepared_artifacts "
            "WHERE candidate_id = %s ORDER BY publication_key",
            (_CANDIDATE,),
        ) == [
            (publication_keys[0], "COMMITTED"),
            (publication_keys[1], "PREPARED"),
        ]
        assert connector.fetch_one(
            "SELECT COUNT(*) "
            "FROM catalog_publication_finalization_batch_stored "
            "WHERE receipt_id = %s",
            (_RECEIPT,),
        ) == (0,)
    finally:
        connector.close()


@pytest.mark.parametrize("race", ("mixed-state", "generation"))
def test_post_external_races_fail_closed(tmp_path: Path, race: str) -> None:
    connector = _database(tmp_path / f"race-{race}.sqlite3")
    try:
        publication_keys, _tokens = _seed_publication(connector, item_count=2)
        gate = _shared_gate(connector)
        first_page = _issue(
            connector,
            gate,
            batch_key=b"first-attempt",
            page_limit=2,
            now=30,
        )
        adapter = _MonotoneAdapter()
        first_ack = PublicationFinalizationRepository.release_page(
            connector,
            backend="sqlite",
            page=first_page,
            adapters={adapter.adapter_id: adapter},
            now=31,
        )

        if race == "mixed-state":
            connector.execute(
                "UPDATE catalog_prepared_artifacts SET state = 'COMMITTED' "
                "WHERE candidate_id = %s AND publication_key = %s",
                (_CANDIDATE, publication_keys[0]),
            )
            with pytest.raises(PublicationFinalizationConflictError, match="mixed"):
                _commit(connector, first_ack, now=32)
            expected_batch_count = 0
        else:
            competing_page = _issue(
                connector,
                gate,
                batch_key=b"winning-attempt",
                page_limit=2,
                now=32,
            )
            competing_ack = PublicationFinalizationRepository.release_page(
                connector,
                backend="sqlite",
                page=competing_page,
                adapters={adapter.adapter_id: adapter},
                now=33,
            )
            _commit(connector, competing_ack, now=34)
            with pytest.raises(
                PublicationFinalizationConflictError,
                match="generation already",
            ):
                _commit(connector, first_ack, now=35)
            expected_batch_count = 1

        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_publication_finalization_batch_stored "
            "WHERE receipt_id = %s",
            (_RECEIPT,),
        ) == (expected_batch_count,)
        if race == "mixed-state":
            assert connector.fetch_one(
                "SELECT generation, cursor, processed_count, state "
                "FROM catalog_publication_finalization_checkpoints "
                "WHERE receipt_id = %s",
                (_RECEIPT,),
            ) == (1, b"", 0, "OPEN")
            assert connector.fetch_all(
                "SELECT publication_key, state FROM catalog_prepared_artifacts "
                "WHERE candidate_id = %s ORDER BY publication_key",
                (_CANDIDATE,),
            ) == [
                (publication_keys[0], "COMMITTED"),
                (publication_keys[1], "PREPARED"),
            ]
    finally:
        connector.close()


class _InjectedFault(RuntimeError):
    pass


def _commit_with_injected_fault(
    connector: SQLiteConnector,
    acknowledgement: PublicationFinalizationAcknowledgement,
    *,
    fail_at: int | None,
) -> tuple[PublicationFinalizationBatchReceipt | None, int]:
    mutations = 0
    original_execute = connector.execute
    original_execute_affected = connector.execute_affected

    def before_mutation() -> None:
        nonlocal mutations
        mutations += 1
        if mutations == fail_at:
            raise _InjectedFault(f"fault at finalization mutation {mutations}")

    def execute(query: str, data: tuple[Any, ...] = ()) -> None:
        before_mutation()
        original_execute(query, data)

    def execute_affected(query: str, data: tuple[Any, ...] = ()) -> int:
        before_mutation()
        return original_execute_affected(query, data)

    receipt: PublicationFinalizationBatchReceipt | None = None
    with (
        patch.object(connector, "execute", side_effect=execute),
        patch.object(connector, "execute_affected", side_effect=execute_affected),
    ):
        with connector.transaction():
            receipt = PublicationFinalizationRepository.commit_page(
                VNextUnitOfWork(connector, backend="sqlite"),
                acknowledgement=acknowledgement,
                now=32,
            )
    return receipt, mutations


def test_every_post_external_commit_mutation_rolls_back(tmp_path: Path) -> None:
    base_path = tmp_path / "fault-base.sqlite3"
    base = _database(base_path)
    _keys, _tokens = _seed_publication(base, item_count=1)
    gate = _shared_gate(base)
    page = _issue(base, gate, batch_key=b"fault-page", now=30)
    acknowledgement = PublicationFinalizationRepository.release_page(
        base,
        backend="sqlite",
        page=page,
        adapters={b"test-artifact-adapter": _MonotoneAdapter()},
        now=31,
    )
    base.close()

    successful_path = tmp_path / "fault-success.sqlite3"
    copyfile(base_path, successful_path)
    successful = SQLiteConnector(str(successful_path))
    successful.connect()
    try:
        receipt, mutation_count = _commit_with_injected_fault(
            successful,
            acknowledgement,
            fail_at=None,
        )
        assert receipt is not None and receipt.row_count == 1
        assert mutation_count == 3
    finally:
        successful.close()

    for fail_at in range(1, mutation_count + 1):
        fault_path = tmp_path / f"fault-{fail_at}.sqlite3"
        copyfile(base_path, fault_path)
        connector = SQLiteConnector(str(fault_path))
        connector.connect()
        try:
            with pytest.raises(_InjectedFault):
                _commit_with_injected_fault(
                    connector,
                    acknowledgement,
                    fail_at=fail_at,
                )
            assert connector.fetch_one(
                "SELECT state FROM catalog_prepared_artifacts WHERE candidate_id = %s",
                (_CANDIDATE,),
            ) == ("PREPARED",)
            assert connector.fetch_one(
                "SELECT generation, cursor, processed_count, state, updated_at "
                "FROM catalog_publication_finalization_checkpoints "
                "WHERE receipt_id = %s",
                (_RECEIPT,),
            ) == (1, b"", 0, "OPEN", 20)
            assert connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_publication_finalization_batch_stored "
                "WHERE receipt_id = %s",
                (_RECEIPT,),
            ) == (0,)
        finally:
            connector.close()
