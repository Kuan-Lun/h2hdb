from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from shutil import copyfile
from typing import Any
from unittest.mock import patch

import pytest
from vnext_publication_fixtures import seed_publication_commit

from h2hdb import vnext_identity as identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_publication_finalization_repository import (
    PublicationFinalizationAcknowledgement,
    PublicationFinalizationBatchReceipt,
    PublicationFinalizationConflictError,
    PublicationFinalizationPage,
    PublicationFinalizationRepository,
    PublicationFinalizationStorageEvidence,
    PublicationFinalizationUnavailableError,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

_CANDIDATE = b"c" * 16
_RECEIPT = b"r" * 16
_CHANNEL = b"default"
_PREPARED_STAGE = b"VALIDATE_PREPARED_ARTIFACT"


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
    return connector


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


def _insert_validation_batch(
    connector: SQLiteConnector,
    *,
    start_generation: int,
    batch_key: bytes,
    start_cursor: bytes,
    start_count: int,
    next_cursor: bytes,
    row_count: int,
    committed_at: int,
) -> None:
    key = (_CANDIDATE, _PREPARED_STAGE, start_generation)
    connector.execute(
        "INSERT INTO catalog_publication_batch_receipt_anchors "
        "(candidate_id, stage, start_generation) VALUES (%s, %s, %s)",
        key,
    )
    connector.execute(
        "INSERT INTO catalog_publication_batch_receipt_coordinates "
        "(candidate_id, stage, batch_key, start_generation) "
        "VALUES (%s, %s, %s, %s)",
        (_CANDIDATE, _PREPARED_STAGE, batch_key, start_generation),
    )
    for table, column, value in (
        (
            "catalog_publication_batch_receipt_start_cursors",
            "start_cursor",
            start_cursor,
        ),
        (
            "catalog_publication_batch_receipt_start_processed_counts",
            "start_processed_count",
            start_count,
        ),
        ("catalog_publication_batch_receipt_next_cursors", "next_cursor", next_cursor),
        ("catalog_publication_batch_receipt_row_counts", "row_count", row_count),
        (
            "catalog_publication_batch_receipt_committed_ats",
            "committed_at",
            committed_at,
        ),
    ):
        connector.execute(
            f"INSERT INTO {table} "
            f"(candidate_id, stage, start_generation, {column}) "
            "VALUES (%s, %s, %s, %s)",
            (*key, value),
        )
    connector.execute(
        "INSERT INTO catalog_publication_batch_receipt_seals "
        "(candidate_id, stage, start_generation) VALUES (%s, %s, %s)",
        key,
    )


def _seed_validation_receipt(
    connector: SQLiteConnector,
    *,
    publication_keys: tuple[bytes, ...],
) -> None:
    expected_count = len(publication_keys)
    cursor = max(publication_keys, default=b"")
    if publication_keys:
        _insert_validation_batch(
            connector,
            start_generation=1,
            batch_key=b"prepared-rows",
            start_cursor=b"",
            start_count=0,
            next_cursor=cursor,
            row_count=expected_count,
            committed_at=18,
        )
        terminal_start_generation = 2
        checkpoint_generation = 3
    else:
        terminal_start_generation = 1
        checkpoint_generation = 2
    _insert_validation_batch(
        connector,
        start_generation=terminal_start_generation,
        batch_key=b"prepared-terminal",
        start_cursor=cursor,
        start_count=expected_count,
        next_cursor=cursor,
        row_count=0,
        committed_at=19,
    )
    connector.execute(
        "INSERT INTO catalog_publication_checkpoint_anchors "
        "(candidate_id, stage) VALUES (%s, %s)",
        (_CANDIDATE, _PREPARED_STAGE),
    )
    for table, column, value in (
        (
            "catalog_publication_checkpoint_generations",
            "generation",
            checkpoint_generation,
        ),
        ("catalog_publication_checkpoint_cursors", "cursor", cursor),
        (
            "catalog_publication_checkpoint_processed_counts",
            "processed_count",
            expected_count,
        ),
        ("catalog_publication_checkpoint_states", "state", "COMPLETE"),
        ("catalog_publication_checkpoint_updated_ats", "updated_at", 19),
    ):
        connector.execute(
            f"INSERT INTO {table} (candidate_id, stage, {column}) "
            "VALUES (%s, %s, %s)",
            (_CANDIDATE, _PREPARED_STAGE, value),
        )
    connector.execute(
        "INSERT INTO catalog_publication_checkpoint_seals "
        "(candidate_id, stage) VALUES (%s, %s)",
        (_CANDIDATE, _PREPARED_STAGE),
    )


def _seed_prepared_artifact(
    connector: SQLiteConnector,
    *,
    publication_key: bytes,
    artifact_sha256: bytes,
    storage_generation: int,
) -> bytes:
    size_bytes = 100 + storage_generation
    locator_components = identity.artifact_locator_components(artifact_sha256)
    locator_sha256 = identity.artifact_locator_digest(locator_components)
    protection_token = identity.encode_artifact_protection_token(
        1,
        _CANDIDATE,
        publication_key,
        artifact_sha256,
        locator_sha256,
        storage_generation,
        size_bytes,
    )
    connector.execute(
        "INSERT INTO catalog_artifact_blobs "
        "(artifact_sha256, size_bytes) VALUES (%s, %s)",
        (artifact_sha256, size_bytes),
    )
    connector.execute(
        "INSERT INTO catalog_artifact_location "
        "(artifact_sha256, artifact_locator_sha256) VALUES (%s, %s)",
        (artifact_sha256, locator_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_prepared_artifact_anchors "
        "(candidate_id, publication_key) VALUES (%s, %s)",
        (_CANDIDATE, publication_key),
    )
    for table, column, value in (
        ("catalog_prepared_artifact_sha256s", "artifact_sha256", artifact_sha256),
        (
            "catalog_prepared_artifact_storage_codec_versions",
            "storage_codec_version",
            1,
        ),
        (
            "catalog_prepared_artifact_storage_generations",
            "storage_generation",
            storage_generation,
        ),
        (
            "catalog_prepared_artifact_protection_tokens",
            "protection_token",
            protection_token,
        ),
        ("catalog_prepared_artifact_states", "state", "PREPARED"),
    ):
        connector.execute(
            f"INSERT INTO {table} (candidate_id, publication_key, {column}) "
            "VALUES (%s, %s, %s)",
            (_CANDIDATE, publication_key, value),
        )
    connector.execute(
        "INSERT INTO catalog_prepared_artifact_seals "
        "(candidate_id, publication_key) VALUES (%s, %s)",
        (_CANDIDATE, publication_key),
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
        connector.execute("INSERT INTO catalog_revision_anchors (revision) VALUES (1)")
        connector.execute(
            "INSERT INTO catalog_revision_publication_counts "
            "(revision, publication_count) VALUES (1, %s)",
            (item_count,),
        )
        connector.execute(
            "INSERT INTO catalog_revision_descriptor_seals (revision) VALUES (1)"
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_anchors "
            "(source_revision) VALUES (1)"
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_channels "
            "(source_revision, channel) VALUES (1, %s)",
            (_CHANNEL,),
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_snapshot_manifests "
            "(source_revision, snapshot_manifest_sha256) VALUES (1, %s)",
            (b"s" * 32,),
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_descriptor_seals "
            "(source_revision) VALUES (1)"
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
            tokens.append(
                _seed_prepared_artifact(
                    connector,
                    publication_key=publication_key,
                    artifact_sha256=sha256(f"artifact-{index}".encode()).digest(),
                    storage_generation=index,
                )
            )
        _seed_validation_receipt(
            connector,
            publication_keys=publication_keys,
        )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")
    assert connector.fetch_one(
        "SELECT state FROM catalog_publication_receipts WHERE receipt_id = %s",
        (_RECEIPT,),
    ) == ("DB_COMMITTED",)
    return publication_keys, tuple(tokens)


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
    tables = (
        "catalog_publication_batch_receipt_seals",
        "catalog_publication_batch_receipt_committed_ats",
        "catalog_publication_batch_receipt_row_counts",
        "catalog_publication_batch_receipt_next_cursors",
        "catalog_publication_batch_receipt_start_processed_counts",
        "catalog_publication_batch_receipt_start_cursors",
        "catalog_publication_batch_receipt_coordinates",
        "catalog_publication_batch_receipt_anchors",
        "catalog_publication_checkpoint_seals",
        "catalog_publication_checkpoint_updated_ats",
        "catalog_publication_checkpoint_states",
        "catalog_publication_checkpoint_processed_counts",
        "catalog_publication_checkpoint_cursors",
        "catalog_publication_checkpoint_generations",
        "catalog_publication_checkpoint_anchors",
        "catalog_prepared_artifact_seals",
        "catalog_prepared_artifact_states",
        "catalog_prepared_artifact_protection_tokens",
        "catalog_prepared_artifact_storage_generations",
        "catalog_prepared_artifact_storage_codec_versions",
        "catalog_prepared_artifact_sha256s",
        "catalog_prepared_artifact_anchors",
    )
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        for table in tables:
            connector.execute(
                f"DELETE FROM {table} WHERE candidate_id = %s",
                (_CANDIDATE,),
            )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")


class _MonotoneAdapter:
    adapter_id = b"managed-filesystem"

    def __init__(self, connector: SQLiteConnector | None = None) -> None:
        self.connector = connector
        self.calls: list[tuple[tuple[str, ...], bytes]] = []
        self.tombstones: set[bytes] = set()

    def release(
        self,
        locator_components: tuple[str, ...],
        protection_token: bytes,
    ) -> PublicationFinalizationStorageEvidence:
        if self.connector is not None:
            with self.connector.read_transaction():
                assert self.connector.fetch_one("SELECT 1") == (1,)
        self.calls.append((locator_components, protection_token))
        self.tombstones.add(protection_token)
        return PublicationFinalizationStorageEvidence(True)


def test_permanent_batch_replays_after_cleanup_and_expired_gate(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "permanent-replay.sqlite3")
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


def test_terminal_marker_checkpoint_and_finalized_at_are_one_derived_commit(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "terminal.sqlite3")
    try:
        publication_keys, tokens = _seed_publication(connector, item_count=2)
        gate = _shared_gate(connector)
        adapter = _MonotoneAdapter()

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
            now += 3

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
        assert connector.fetch_one(
            "SELECT generation, cursor, processed_count, state, updated_at "
            "FROM catalog_publication_finalization_checkpoints "
            "WHERE receipt_id = %s",
            (_RECEIPT,),
        ) == (4, publication_keys[-1], 2, "COMPLETE", now + 2)
        assert connector.fetch_one(
            "SELECT state, committed_at, finalized_at "
            "FROM catalog_publication_receipts WHERE receipt_id = %s",
            (_RECEIPT,),
        ) == ("PROJECTION_FINALIZED", 20, now + 2)
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
        ) == ("PROJECTION_FINALIZED", now + 2)
        with pytest.raises(PublicationFinalizationUnavailableError):
            _issue(connector, gate, batch_key=b"after-terminal", now=40)
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
                "UPDATE catalog_prepared_artifact_states SET state = 'COMMITTED' "
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
            "SELECT COUNT(*) FROM catalog_publication_finalization_batch_seals "
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
                "SELECT publication_key, state FROM catalog_prepared_artifact_states "
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
        adapters={b"managed-filesystem": _MonotoneAdapter()},
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
        assert mutation_count == 13
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
                "SELECT state FROM catalog_prepared_artifact_states "
                "WHERE candidate_id = %s",
                (_CANDIDATE,),
            ) == ("PREPARED",)
            assert connector.fetch_one(
                "SELECT generation, cursor, processed_count, state, updated_at "
                "FROM catalog_publication_finalization_checkpoints "
                "WHERE receipt_id = %s",
                (_RECEIPT,),
            ) == (1, b"", 0, "OPEN", 20)
            assert connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_publication_finalization_batch_seals "
                "WHERE receipt_id = %s",
                (_RECEIPT,),
            ) == (0,)
        finally:
            connector.close()
