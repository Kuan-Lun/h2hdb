from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from vnext_canonical_value_fixtures import seed_canonical_value
from vnext_generated_database import open_generated_sqlite_database

from h2hdb import vnext_artifact_release_repository as release_repository
from h2hdb import vnext_identity as identity
from h2hdb.config_loader import CoreConfig, DatabaseConfig
from h2hdb.domain import (
    CatalogResourceKind,
    StorageObjectKey,
    VNextLibraryActivationCursor,
)
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_artifact_release_repository import (
    ArtifactReleaseAcknowledgement,
    ArtifactReleaseCommitReceipt,
    ArtifactReleaseConflictError,
    ArtifactReleasePage,
    ArtifactReleaseRepository,
    ArtifactReleaseStorageEvidence,
    ArtifactReleaseUnavailableError,
)
from h2hdb.vnext_ingest_facade import (
    VNextCurrentOnlyMaintenanceOutcome,
    VNextIngestFacade,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

_ADAPTER_ID = b"test-artifact-adapter"
_POLICY_COMPONENT = b"p" * 32
_POLICY_FINGERPRINT = b"f" * 32


def _database(path: Path) -> SQLiteConnector:
    return open_generated_sqlite_database(path)


def _exclusive(connector: SQLiteConnector) -> GateLease:
    with (
        connector.transaction(),
        patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=b"x" * 16,
        ),
    ):
        return MaintenanceGateRepository.claim_exclusive(
            VNextUnitOfWork(connector, backend="sqlite"),
            now=1,
            lease_duration=100_000,
        )


def _fixture_rows(
    connector: SQLiteConnector,
    statements: list[tuple[str, tuple[object, ...]]],
) -> None:
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        for sql, parameters in statements:
            connector.execute(sql, parameters)
    finally:
        connector.execute("PRAGMA foreign_keys = ON")


def _seed_candidate(
    connector: SQLiteConnector,
    *,
    candidate_id: bytes,
    reserved_revision: int,
) -> None:
    policy_rows: list[tuple[str, tuple[object, ...]]] = []
    if not connector.fetch_one(
        "SELECT artifact_policy_id FROM catalog_artifact_policies "
        "WHERE artifact_policy_id = 1"
    ):
        policy_rows.extend(
            [
                (
                    "INSERT INTO catalog_artifact_adapter_policy "
                    "(policy_fingerprint_sha256, adapter_id) VALUES (%s, %s)",
                    (_POLICY_FINGERPRINT, _ADAPTER_ID),
                ),
                (
                    "INSERT INTO catalog_artifact_policy_semantics "
                    "(policy_component_sha256, artifact_algorithm_version, "
                    "policy_fingerprint_sha256) VALUES (%s, 2, %s)",
                    (_POLICY_COMPONENT, _POLICY_FINGERPRINT),
                ),
                (
                    "INSERT INTO catalog_artifact_policies "
                    "(artifact_policy_id, policy_component_sha256) VALUES (1, %s)",
                    (_POLICY_COMPONENT,),
                ),
            ]
        )
    policy_rows.append(
        (
            "INSERT INTO catalog_publication_candidates "
            "(candidate_id, analysis_id, reserved_revision, "
            "artifact_policy_id, display_title_policy_id, artifacts_required, "
            "created_at) VALUES (%s, %s, %s, 1, 1, 1, 1)",
            (candidate_id, candidate_id, reserved_revision),
        )
    )
    _fixture_rows(connector, policy_rows)


def _seed_resource(
    connector: SQLiteConnector,
    *,
    gid: int,
    candidate_id: bytes,
    reserved_revision: int,
    resource_kind: CatalogResourceKind,
    storage_object_sha256: bytes,
    state: str,
    storage_generation: int = 7,
    size_bytes: int = 99,
) -> bytes:
    publication_key = identity.publication_key(gid)
    storage_key = StorageObjectKey(
        "opaque-v2",
        ("library", str(gid), resource_kind.value),
    )
    storage_key_sha256 = identity.artifact_storage_key_digest(
        storage_key.codec,
        storage_key.segments,
    )
    token = identity.encode_artifact_protection_token(
        candidate_id,
        publication_key,
        resource_kind.value,
        storage_key_sha256,
        storage_generation,
    )
    occurrence = identity.catalog_publication_occurrence_sha256(
        reserved_revision,
        publication_key,
    )
    statements: list[tuple[str, tuple[object, ...]]] = []
    if not connector.fetch_one(
        "SELECT publication_key FROM catalog_publication_occurrence_identities "
        "WHERE revision = %s AND publication_key = %s",
        (reserved_revision, publication_key),
    ):
        statements.extend(
            [
                (
                    "INSERT INTO catalog_publication_occurrence_identities "
                    "(catalog_occurrence_sha256, revision, publication_key) "
                    "VALUES (%s, %s, %s)",
                    (occurrence, reserved_revision, publication_key),
                ),
                (
                    "INSERT INTO catalog_publication_storage "
                    "(catalog_occurrence_sha256, gallery_id, summary_sha256, "
                    "language_sha256, modified_at, source_title_sha256) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        occurrence,
                        gid,
                        b"s" * 32,
                        b"l" * 32,
                        1,
                        b"t" * 32,
                    ),
                ),
            ]
        )
    statements.extend(
        [
            (
                "INSERT INTO catalog_storage_object_key_identities "
                "(storage_object_key_sha256, key_codec, segment_count) "
                "VALUES (%s, %s, %s)",
                (
                    storage_key_sha256,
                    storage_key.codec.encode("ascii"),
                    len(storage_key.segments),
                ),
            ),
            *(
                (
                    "INSERT INTO catalog_storage_object_key_segments "
                    "(storage_object_key_sha256, segment_position, key_segment) "
                    "VALUES (%s, %s, %s)",
                    (storage_key_sha256, position, segment.encode("utf-8")),
                )
                for position, segment in enumerate(storage_key.segments)
            ),
            (
                "INSERT INTO catalog_artifact_blobs "
                "(artifact_sha256, size_bytes) VALUES (%s, %s)",
                (storage_object_sha256, size_bytes),
            ),
            (
                "INSERT INTO catalog_prepared_artifacts "
                "(candidate_id, publication_key, resource_kind, "
                "storage_object_key_sha256, storage_generation, "
                "protection_token, state) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    candidate_id,
                    publication_key,
                    resource_kind.value.encode("ascii"),
                    storage_key_sha256,
                    storage_generation,
                    token,
                    state,
                ),
            ),
            (
                "INSERT INTO catalog_prepared_resource_blob "
                "(candidate_id, publication_key, resource_kind, "
                "storage_object_sha256) VALUES (%s, %s, %s, %s)",
                (
                    candidate_id,
                    publication_key,
                    resource_kind.value.encode("ascii"),
                    storage_object_sha256,
                ),
            ),
        ]
    )
    _fixture_rows(connector, statements)
    return token


def _issue(
    connector: SQLiteConnector,
    gate: GateLease,
    *,
    cursor: bytes = b"",
    page_limit: int = 128,
    now: int = 2,
) -> ArtifactReleasePage:
    with connector.transaction():
        return ArtifactReleaseRepository.issue_page(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            cursor=cursor,
            page_limit=page_limit,
            now=now,
        )


def _commit(
    connector: SQLiteConnector,
    acknowledgement: ArtifactReleaseAcknowledgement,
    *,
    now: int,
) -> ArtifactReleaseCommitReceipt:
    with connector.transaction():
        return ArtifactReleaseRepository.commit_page(
            VNextUnitOfWork(connector, backend="sqlite"),
            acknowledgement=acknowledgement,
            now=now,
        )


class _MonotoneAdapter:
    adapter_id = _ADAPTER_ID

    def __init__(
        self,
        *,
        connector: SQLiteConnector | None = None,
        acknowledge: bool = True,
    ) -> None:
        self.connector = connector
        self.acknowledge = acknowledge
        self.calls: list[tuple[StorageObjectKey, bytes, int, bytes]] = []
        self.tombstones: set[bytes] = set()

    def release(
        self,
        storage_key: StorageObjectKey,
        expected_sha256: bytes,
        expected_size_bytes: int,
        protection_token: bytes,
    ) -> ArtifactReleaseStorageEvidence:
        if self.connector is not None:
            with self.connector.read_transaction():
                assert self.connector.fetch_one("SELECT 1") == (1,)
        self.calls.append(
            (
                storage_key,
                expected_sha256,
                expected_size_bytes,
                protection_token,
            )
        )
        if self.acknowledge:
            self.tombstones.add(protection_token)
        return ArtifactReleaseStorageEvidence(self.acknowledge)


class _LoseFirstResponseAdapter(_MonotoneAdapter):
    def __init__(self, *, connector: SQLiteConnector) -> None:
        super().__init__(connector=connector)
        self._lose_response = True

    def release(
        self,
        storage_key: StorageObjectKey,
        expected_sha256: bytes,
        expected_size_bytes: int,
        protection_token: bytes,
    ) -> ArtifactReleaseStorageEvidence:
        evidence = super().release(
            storage_key,
            expected_sha256,
            expected_size_bytes,
            protection_token,
        )
        if self._lose_response:
            self._lose_response = False
            raise RuntimeError("simulated response loss after durable tombstone")
        return evidence


def _drain_facade_to_done(
    facade: VNextIngestFacade,
    adapter: _MonotoneAdapter,
) -> tuple[VNextCurrentOnlyMaintenanceOutcome, ...]:
    outcomes: list[VNextCurrentOnlyMaintenanceOutcome] = []
    for _attempt in range(64):
        outcome = facade.drain_current_only_maintenance(
            1_000_000,
            artifact_release_adapters={adapter.adapter_id: adapter},
        )
        outcomes.append(outcome)
        if outcome is VNextCurrentOnlyMaintenanceOutcome.DONE:
            return tuple(outcomes)
        assert outcome is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
    raise AssertionError("orphan artifact release and cleanup did not converge")


def _seed_policy_canonical_value(connector: SQLiteConnector) -> None:
    seed_canonical_value(
        connector,
        value_sha256=_POLICY_COMPONENT,
        digest_domain=b"artifact_policy_v3",
        page_sha256=b"r" * 32,
        page_bytes=b"policy",
        subtree_item_count=1,
        allocated_at=1,
    )


def test_public_facade_releases_orphan_then_reaches_cleanup_fixed_point(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "facade-artifact-release.sqlite3"
    connector = _database(database_path)
    try:
        candidate_id = b"a" * 16
        _seed_policy_canonical_value(connector)
        _seed_candidate(connector, candidate_id=candidate_id, reserved_revision=1)
        token = _seed_resource(
            connector,
            gid=1,
            candidate_id=candidate_id,
            reserved_revision=1,
            resource_kind=CatalogResourceKind.ACQUISITION,
            storage_object_sha256=b"a" * 32,
            state="PREPARED",
        )
        adapter = _MonotoneAdapter(connector=connector)
        facade = VNextIngestFacade(
            CoreConfig(
                database=DatabaseConfig(
                    sql_type="sqlite",
                    database=str(database_path),
                )
            ),
            clock=iter(range(10, 10_000)).__next__,
        )

        try:
            assert (
                facade.drain_current_only_maintenance(1_000_000)
                is VNextCurrentOnlyMaintenanceOutcome.BLOCKED
            )
            assert adapter.calls == []
            assert adapter.tombstones == set()
            outcomes = _drain_facade_to_done(facade, adapter)
        finally:
            facade.close()

        assert outcomes[0] is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        assert outcomes[-1] is VNextCurrentOnlyMaintenanceOutcome.DONE
        assert adapter.tombstones == {token}
        assert len(adapter.calls) == 1
        assert (
            connector.fetch_one(
                "SELECT candidate_id FROM catalog_publication_candidates "
                "WHERE candidate_id = %s",
                (candidate_id,),
            )
            == ()
        )
        assert connector.fetch_all("PRAGMA foreign_key_check") == []
    finally:
        connector.close()


def test_public_facade_replays_release_after_tombstone_response_loss(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "facade-artifact-release-response-loss.sqlite3"
    connector = _database(database_path)
    try:
        candidate_id = b"a" * 16
        _seed_policy_canonical_value(connector)
        _seed_candidate(connector, candidate_id=candidate_id, reserved_revision=1)
        token = _seed_resource(
            connector,
            gid=1,
            candidate_id=candidate_id,
            reserved_revision=1,
            resource_kind=CatalogResourceKind.ACQUISITION,
            storage_object_sha256=b"a" * 32,
            state="PENDING",
        )
        adapter = _LoseFirstResponseAdapter(connector=connector)
        facade = VNextIngestFacade(
            CoreConfig(
                database=DatabaseConfig(
                    sql_type="sqlite",
                    database=str(database_path),
                )
            ),
            clock=iter(range(10, 10_000)).__next__,
        )

        try:
            with pytest.raises(RuntimeError, match="simulated response loss"):
                facade.drain_current_only_maintenance(
                    1_000_000,
                    artifact_release_adapters={adapter.adapter_id: adapter},
                )
            assert adapter.tombstones == {token}
            assert connector.fetch_one(
                "SELECT state FROM catalog_prepared_artifacts WHERE candidate_id = %s",
                (candidate_id,),
            ) == ("PENDING",)

            outcomes = _drain_facade_to_done(facade, adapter)
        finally:
            facade.close()

        assert outcomes[0] is VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
        assert outcomes[-1] is VNextCurrentOnlyMaintenanceOutcome.DONE
        assert len(adapter.calls) == 2
        assert adapter.calls[0] == adapter.calls[1]
        assert adapter.tombstones == {token}
        assert (
            connector.fetch_one(
                "SELECT candidate_id FROM catalog_publication_candidates "
                "WHERE candidate_id = %s",
                (candidate_id,),
            )
            == ()
        )
        assert connector.fetch_all("PRAGMA foreign_key_check") == []
    finally:
        connector.close()


def test_multi_resource_response_loss_and_commit_replay(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "artifact-release.sqlite3")
    try:
        candidate_id = b"a" * 16
        revision = 1
        publication = identity.publication_key(1)
        _seed_candidate(
            connector,
            candidate_id=candidate_id,
            reserved_revision=revision,
        )
        acquisition_token = _seed_resource(
            connector,
            gid=1,
            candidate_id=candidate_id,
            reserved_revision=revision,
            resource_kind=CatalogResourceKind.ACQUISITION,
            storage_object_sha256=b"a" * 32,
            state="PENDING",
        )
        thumbnail_token = _seed_resource(
            connector,
            gid=1,
            candidate_id=candidate_id,
            reserved_revision=revision,
            resource_kind=CatalogResourceKind.THUMBNAIL,
            storage_object_sha256=b"t" * 32,
            state="PREPARED",
            size_bytes=17,
        )
        gate = _exclusive(connector)

        first = _issue(connector, gate, page_limit=1, now=2)
        assert len(first.next_cursor) == 49
        assert first.items[0].resource_kind is CatalogResourceKind.ACQUISITION
        assert (
            first.next_cursor
            == candidate_id
            + VNextLibraryActivationCursor(
                publication,
                CatalogResourceKind.ACQUISITION,
            ).to_bytes()
        )
        retry = _issue(connector, gate, page_limit=1, now=3)
        assert retry == first

        adapter = _MonotoneAdapter(connector=connector)
        first_ack = ArtifactReleaseRepository.release_page(
            connector,
            backend="sqlite",
            page=first,
            adapters={adapter.adapter_id: adapter},
            now=4,
        )
        first_receipt = _commit(connector, first_ack, now=5)
        assert first_receipt.transitioned_count == 1
        second = _issue(
            connector,
            gate,
            cursor=first.next_cursor,
            page_limit=1,
            now=6,
        )
        assert second.items[0].resource_kind is CatalogResourceKind.THUMBNAIL
        assert tuple(
            item.protection_token for item in (*first.items, *second.items)
        ) == (acquisition_token, thumbnail_token)

        second_ack = ArtifactReleaseRepository.release_page(
            connector,
            backend="sqlite",
            page=second,
            adapters={adapter.adapter_id: adapter},
            now=7,
        )
        second_receipt = _commit(connector, second_ack, now=8)
        assert second_receipt.transitioned_count == 1
        assert [call[1:3] for call in adapter.calls] == [
            (b"a" * 32, 99),
            (b"t" * 32, 17),
        ]

        with patch.object(
            connector,
            "execute_affected",
            wraps=connector.execute_affected,
        ) as execute_affected:
            replayed = _commit(connector, second_ack, now=9)
        assert replayed.replayed
        execute_affected.assert_not_called()
        assert connector.fetch_all(
            "SELECT resource_kind, state FROM catalog_prepared_artifacts "
            "WHERE candidate_id = %s ORDER BY publication_key, resource_kind",
            (candidate_id,),
        ) == [
            (b"acquisition", "COMMITTED"),
            (b"thumbnail", "COMMITTED"),
        ]
        terminal = _issue(
            connector,
            gate,
            cursor=second.next_cursor,
            page_limit=1,
            now=10,
        )
        assert terminal.terminal and terminal.items == ()
    finally:
        connector.close()


def test_cursor_requires_a_committed_predecessor(tmp_path: Path) -> None:
    connector = _database(tmp_path / "artifact-release-cursor-authority.sqlite3")
    try:
        candidate_id = b"a" * 16
        _seed_candidate(connector, candidate_id=candidate_id, reserved_revision=1)
        _seed_resource(
            connector,
            gid=1,
            candidate_id=candidate_id,
            reserved_revision=1,
            resource_kind=CatalogResourceKind.ACQUISITION,
            storage_object_sha256=b"a" * 32,
            state="PENDING",
        )
        gate = _exclusive(connector)
        page = _issue(connector, gate, now=2)
        with pytest.raises(
            ArtifactReleaseConflictError,
            match="committed resource",
        ):
            _issue(connector, gate, cursor=page.next_cursor, now=3)

        adapter = _MonotoneAdapter()
        acknowledgement = ArtifactReleaseRepository.release_page(
            connector,
            backend="sqlite",
            page=page,
            adapters={adapter.adapter_id: adapter},
            now=4,
        )
        _commit(connector, acknowledgement, now=5)
        terminal = _issue(connector, gate, cursor=page.next_cursor, now=6)
        assert terminal.terminal
    finally:
        connector.close()


def test_same_publication_resources_commit_in_typed_lock_order(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "artifact-release-lock-order.sqlite3")
    try:
        candidate_id = b"a" * 16
        _seed_candidate(connector, candidate_id=candidate_id, reserved_revision=1)
        for kind, digest in (
            (CatalogResourceKind.ACQUISITION, b"a" * 32),
            (CatalogResourceKind.THUMBNAIL, b"t" * 32),
        ):
            _seed_resource(
                connector,
                gid=1,
                candidate_id=candidate_id,
                reserved_revision=1,
                resource_kind=kind,
                storage_object_sha256=digest,
                state="PREPARED",
            )
        gate = _exclusive(connector)
        page = _issue(connector, gate, page_limit=2, now=2)
        assert tuple(item.resource_kind for item in page.items) == (
            CatalogResourceKind.ACQUISITION,
            CatalogResourceKind.THUMBNAIL,
        )
        adapter = _MonotoneAdapter()
        acknowledgement = ArtifactReleaseRepository.release_page(
            connector,
            backend="sqlite",
            page=page,
            adapters={adapter.adapter_id: adapter},
            now=3,
        )
        assert _commit(connector, acknowledgement, now=4).transitioned_count == 2
    finally:
        connector.close()


def test_active_candidate_blocks_external_release(tmp_path: Path) -> None:
    connector = _database(tmp_path / "artifact-release-active.sqlite3")
    try:
        candidate_id = b"a" * 16
        _seed_candidate(connector, candidate_id=candidate_id, reserved_revision=1)
        _seed_resource(
            connector,
            gid=1,
            candidate_id=candidate_id,
            reserved_revision=1,
            resource_kind=CatalogResourceKind.ACQUISITION,
            storage_object_sha256=b"a" * 32,
            state="PENDING",
        )
        gate = _exclusive(connector)
        page = _issue(connector, gate, now=2)
        connector.execute(
            "INSERT INTO operational_catalog_working_candidates "
            "(slot, candidate_id, assigned_at) VALUES (1, %s, 2)",
            (candidate_id,),
        )
        adapter = _MonotoneAdapter()
        with pytest.raises(ArtifactReleaseUnavailableError):
            ArtifactReleaseRepository.release_page(
                connector,
                backend="sqlite",
                page=page,
                adapters={adapter.adapter_id: adapter},
                now=3,
            )
        assert adapter.calls == []
    finally:
        connector.close()


def test_durably_committed_candidate_is_never_issued_or_released(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "artifact-release-published.sqlite3")
    try:
        candidate_id = b"a" * 16
        _seed_candidate(connector, candidate_id=candidate_id, reserved_revision=1)
        _seed_resource(
            connector,
            gid=1,
            candidate_id=candidate_id,
            reserved_revision=1,
            resource_kind=CatalogResourceKind.ACQUISITION,
            storage_object_sha256=b"a" * 32,
            state="PREPARED",
        )
        gate = _exclusive(connector)
        issued_before_commit = _issue(connector, gate, now=2)
        assert not issued_before_commit.terminal
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO catalog_publication_commits "
                    "(receipt_id, candidate_id, revision, source_revision, "
                    "generation, preparation_id, operational_policy_id, "
                    "artifact_policy_id, display_title_policy_id, new_galleries, "
                    "changed_galleries, removed_galleries, duplicate_losers, "
                    "committed_at) VALUES (%s, %s, 1, 1, 1, %s, 1, 1, 1, "
                    "0, 0, 0, 0, 3)",
                    (b"r" * 16, candidate_id, b"o" * 16),
                )
            ],
        )
        adapter = _MonotoneAdapter()

        with pytest.raises(ArtifactReleaseUnavailableError):
            ArtifactReleaseRepository.release_page(
                connector,
                backend="sqlite",
                page=issued_before_commit,
                adapters={adapter.adapter_id: adapter},
                now=3,
            )

        assert adapter.calls == []
        assert _issue(connector, gate, now=4).terminal
    finally:
        connector.close()


@pytest.mark.parametrize("corruption", ("token", "segment", "blob"))
def test_corrupt_resource_authority_fails_closed(
    tmp_path: Path,
    corruption: str,
) -> None:
    connector = _database(tmp_path / f"artifact-release-{corruption}.sqlite3")
    try:
        candidate_id = b"a" * 16
        publication = identity.publication_key(1)
        _seed_candidate(connector, candidate_id=candidate_id, reserved_revision=1)
        _seed_resource(
            connector,
            gid=1,
            candidate_id=candidate_id,
            reserved_revision=1,
            resource_kind=CatalogResourceKind.ACQUISITION,
            storage_object_sha256=b"a" * 32,
            state="PENDING",
        )
        connector.execute("PRAGMA foreign_keys = OFF")
        try:
            if corruption == "token":
                connector.execute(
                    "UPDATE catalog_prepared_artifacts SET protection_token = %s "
                    "WHERE candidate_id = %s AND publication_key = %s",
                    (b"z" * 32, candidate_id, publication),
                )
            elif corruption == "segment":
                connector.execute(
                    "UPDATE catalog_storage_object_key_segments "
                    "SET key_segment = %s WHERE segment_position = 0",
                    (b"changed",),
                )
            else:
                connector.execute(
                    "DELETE FROM catalog_prepared_resource_blob "
                    "WHERE candidate_id = %s AND publication_key = %s",
                    (candidate_id, publication),
                )
        finally:
            connector.execute("PRAGMA foreign_keys = ON")
        gate = _exclusive(connector)
        with pytest.raises(ArtifactReleaseConflictError):
            _issue(connector, gate, now=2)
    finally:
        connector.close()


def test_forged_page_and_noncanonical_cursor_fail_closed(tmp_path: Path) -> None:
    connector = _database(tmp_path / "artifact-release-forgery.sqlite3")
    try:
        gate = _exclusive(connector)
        with pytest.raises(TypeError, match="repository-issued"):
            ArtifactReleasePage(gate, b"", b"", 1, (), True, object())
        with pytest.raises(ValueError, match="49 bytes"):
            _issue(connector, gate, cursor=b"x" * 48, now=2)
        with pytest.raises(ValueError, match="kind tag"):
            _issue(
                connector,
                gate,
                cursor=b"c" * 16 + b"p" * 32 + b"\xff",
                now=2,
            )
    finally:
        connector.close()


class _MariaPageRecorder:
    def __init__(self) -> None:
        self.query = ""
        self.parameters: tuple[object, ...] = ()

    def fetch_all(
        self,
        query: str,
        data: tuple[object, ...] = (),
    ) -> list[tuple[object, ...]]:
        self.query = query
        self.parameters = data
        return []


def test_mariadb_page_shape_uses_typed_resource_keyset() -> None:
    recorder = _MariaPageRecorder()
    gate = GateLease(
        b"g" * 16,
        1,
        GateMode.EXCLUSIVE,
        tuple(range(64)),
        1_000,
    )
    publication = b"p" * 32
    cursor = (
        b"c" * 16
        + VNextLibraryActivationCursor(
            publication,
            CatalogResourceKind.ACQUISITION,
        ).to_bytes()
    )
    with (
        patch.object(
            MaintenanceGateRepository,
            "lock_and_require_live",
            return_value=gate,
        ),
        patch.object(release_repository, "_require_cursor_authority"),
    ):
        page = ArtifactReleaseRepository.issue_page(
            VNextUnitOfWork(recorder, backend="mariadb"),  # type: ignore[arg-type]
            gate_lease=gate,
            cursor=cursor,
            page_limit=8,
            now=2,
        )
    assert page.terminal
    assert "?" not in recorder.query
    assert "LIMIT %s" in recorder.query
    assert "prepared.resource_kind > %s" in recorder.query
    assert "catalog_prepared_resource_blob" in recorder.query
    assert "operational_catalog_working_candidates" in recorder.query
    assert "catalog_publication_commits" in recorder.query
    assert recorder.parameters == (
        b"c" * 16,
        b"c" * 16,
        publication,
        publication,
        b"acquisition",
        8,
    )
