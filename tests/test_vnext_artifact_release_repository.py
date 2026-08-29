from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from h2hdb import vnext_identity as identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sql_connector import DatabaseDuplicateKeyError
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_artifact_family import (
    ArtifactFamilyCollisionError,
    load_prepared_artifact_family_by_token,
)
from h2hdb.vnext_artifact_release_repository import (
    ArtifactReleaseAcknowledgement,
    ArtifactReleaseCommitReceipt,
    ArtifactReleaseConflictError,
    ArtifactReleasePage,
    ArtifactReleaseRepository,
    ArtifactReleaseStorageEvidence,
    ArtifactReleaseUnavailableError,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    GateMode,
    MaintenanceGateRepository,
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
    return connector


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
    state: str = "ABANDONED",
) -> None:
    # Candidate lifecycle is graph-derived. Orphan eligibility needs the one
    # complete immutable candidate; working-root and commit rows are blockers.
    del state
    _fixture_rows(
        connector,
        [
            (
                "INSERT INTO catalog_publication_candidates "
                "(candidate_id, analysis_id, reserved_revision, "
                "artifact_policy_id, display_title_policy_id, "
                "artifacts_required, created_at) "
                "VALUES (%s, %s, %s, 1, 1, 1, 1)",
                (candidate_id, candidate_id, reserved_revision),
            ),
        ],
    )


def _seed_family(
    connector: SQLiteConnector,
    *,
    candidate_id: bytes,
    publication_key: bytes,
    artifact_sha256: bytes,
    state: str,
    storage_generation: int = 7,
    size_bytes: int = 99,
) -> bytes:
    components = identity.artifact_locator_components(artifact_sha256)
    locator_sha256 = identity.artifact_locator_digest(components)
    token = identity.encode_artifact_protection_token(
        1,
        candidate_id,
        publication_key,
        artifact_sha256,
        locator_sha256,
        storage_generation,
        size_bytes,
    )
    _fixture_rows(
        connector,
        [
            (
                "INSERT INTO catalog_artifact_blobs "
                "(artifact_sha256, size_bytes, artifact_locator_sha256) "
                "VALUES (%s, %s, %s)",
                (artifact_sha256, size_bytes, locator_sha256),
            ),
            (
                "INSERT INTO catalog_prepared_artifacts "
                "(candidate_id, publication_key, artifact_sha256, "
                "storage_codec_version, storage_generation, protection_token, state) "
                "VALUES (%s, %s, %s, 1, %s, %s, %s)",
                (
                    candidate_id,
                    publication_key,
                    artifact_sha256,
                    storage_generation,
                    token,
                    state,
                ),
            ),
        ],
    )
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


def test_token_lookup_revalidates_total_blob_authority(tmp_path: Path) -> None:
    connector = _database(tmp_path / "prepared-token-authority.sqlite3")
    try:
        candidate_id = b"a" * 16
        publication_key = b"p" * 32
        artifact_sha256 = b"c" * 32
        _seed_candidate(connector, candidate_id=candidate_id, reserved_revision=1)
        token = _seed_family(
            connector,
            candidate_id=candidate_id,
            publication_key=publication_key,
            artifact_sha256=artifact_sha256,
            state="PENDING",
        )
        loaded = load_prepared_artifact_family_by_token(
            connector,
            protection_token=token,
        )
        assert loaded is not None and loaded.protection_token == token
        connector.execute("PRAGMA foreign_keys = OFF")
        try:
            connector.execute(
                "UPDATE catalog_artifact_blobs SET size_bytes = size_bytes + 1 "
                "WHERE artifact_sha256 = %s",
                (artifact_sha256,),
            )
        finally:
            connector.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(ArtifactFamilyCollisionError, match="blob or locator"):
            load_prepared_artifact_family_by_token(
                connector,
                protection_token=token,
            )
    finally:
        connector.close()


def test_wide_artifact_relations_enforce_alternate_candidate_keys(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "artifact-wide-candidate-keys.sqlite3")
    try:
        components = tuple(bytes((index,)) * 32 for index in range(1, 7))
        connector.execute("PRAGMA foreign_keys = OFF")
        try:
            connector.execute(
                "INSERT INTO catalog_artifact_semantic_inputs "
                "(artifact_semantics_sha256, source_manifest_component_sha256, "
                "member_plan_component_sha256, effective_content_component_sha256, "
                "selected_component_sha256, owner_component_sha256, "
                "policy_component_sha256) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (b"s" * 32, *components),
            )
            with pytest.raises(DatabaseDuplicateKeyError):
                connector.execute(
                    "INSERT INTO catalog_artifact_semantic_inputs "
                    "(artifact_semantics_sha256, source_manifest_component_sha256, "
                    "member_plan_component_sha256, "
                    "effective_content_component_sha256, "
                    "selected_component_sha256, owner_component_sha256, "
                    "policy_component_sha256) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (b"x" * 32, *components),
                )
            connector.execute(
                "INSERT INTO catalog_artifact_blobs "
                "(artifact_sha256, size_bytes, artifact_locator_sha256) "
                "VALUES (%s, 1, %s)",
                (b"a" * 32, b"l" * 32),
            )
            with pytest.raises(DatabaseDuplicateKeyError):
                connector.execute(
                    "INSERT INTO catalog_artifact_blobs "
                    "(artifact_sha256, size_bytes, artifact_locator_sha256) "
                    "VALUES (%s, 2, %s)",
                    (b"b" * 32, b"l" * 32),
                )
        finally:
            connector.execute("PRAGMA foreign_keys = ON")
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_artifact_semantic_inputs"
        ) == (1,)
        assert connector.fetch_one("SELECT COUNT(*) FROM catalog_artifact_blobs") == (
            1,
        )
    finally:
        connector.close()


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
    adapter_id = b"managed-filesystem"

    def __init__(
        self,
        *,
        connector: SQLiteConnector | None = None,
        acknowledge: bool = True,
    ) -> None:
        self.connector = connector
        self.acknowledge = acknowledge
        self.calls: list[tuple[tuple[str, ...], bytes]] = []
        self.tombstones: set[bytes] = set()
        self.protected: set[bytes] = set()

    def release(
        self,
        locator_components: tuple[str, ...],
        protection_token: bytes,
    ) -> ArtifactReleaseStorageEvidence:
        # A nested read succeeds only because the repository committed its
        # revalidation transaction before entering this adapter call.
        if self.connector is not None:
            with self.connector.read_transaction():
                assert self.connector.fetch_one("SELECT 1") == (1,)
        self.calls.append((locator_components, protection_token))
        if self.acknowledge:
            self.protected.discard(protection_token)
            self.tombstones.add(protection_token)
        return ArtifactReleaseStorageEvidence(self.acknowledge)

    def delayed_protect(self, protection_token: bytes) -> bool:
        """Test model of the adapter's required monotone token lifecycle."""

        if protection_token in self.tombstones:
            return False
        self.protected.add(protection_token)
        return True


def test_response_loss_reuses_tokens_and_commit_replay_is_zero_write(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "artifact-release.sqlite3")
    try:
        candidate_id = b"a" * 16
        first_publication = b"a" * 32
        second_publication = b"b" * 32
        _seed_candidate(connector, candidate_id=candidate_id, reserved_revision=1)
        first_token = _seed_family(
            connector,
            candidate_id=candidate_id,
            publication_key=first_publication,
            artifact_sha256=b"c" * 32,
            state="PENDING",
        )
        second_token = _seed_family(
            connector,
            candidate_id=candidate_id,
            publication_key=second_publication,
            artifact_sha256=b"d" * 32,
            state="PREPARED",
        )
        gate = _exclusive(connector)

        page = _issue(connector, gate, page_limit=2, now=2)
        retry = _issue(connector, gate, page_limit=2, now=3)
        assert retry == page
        assert tuple(item.protection_token for item in page.items) == (
            first_token,
            second_token,
        )

        adapter = _MonotoneAdapter(connector=connector)
        acknowledgement = ArtifactReleaseRepository.release_page(
            connector,
            backend="sqlite",
            page=page,
            adapters={adapter.adapter_id: adapter},
            now=4,
        )
        assert tuple(token for _locator, token in adapter.calls) == (
            first_token,
            second_token,
        )
        assert not adapter.delayed_protect(first_token)
        assert not adapter.delayed_protect(second_token)

        committed = _commit(connector, acknowledgement, now=5)
        assert committed.transitioned_count == 2
        assert not committed.replayed
        assert connector.fetch_all(
            "SELECT publication_key, state "
            "FROM catalog_prepared_artifacts WHERE candidate_id = %s "
            "ORDER BY publication_key",
            (candidate_id,),
        ) == [
            (first_publication, "COMMITTED"),
            (second_publication, "COMMITTED"),
        ]

        with patch.object(
            connector,
            "execute_affected",
            wraps=connector.execute_affected,
        ) as execute_affected:
            replayed = _commit(connector, acknowledgement, now=6)
        assert replayed.replayed
        assert replayed.transitioned_count == 0
        execute_affected.assert_not_called()

        terminal = _issue(connector, gate, page_limit=2, now=7)
        assert terminal.terminal
        assert terminal.items == ()
    finally:
        connector.close()


def test_active_or_published_candidate_blocks_every_external_call(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "artifact-release-blockers.sqlite3")
    try:
        active_candidate = b"a" * 16
        published_candidate = b"b" * 16
        _seed_candidate(
            connector,
            candidate_id=active_candidate,
            reserved_revision=1,
        )
        _seed_candidate(
            connector,
            candidate_id=published_candidate,
            reserved_revision=2,
        )
        _seed_family(
            connector,
            candidate_id=active_candidate,
            publication_key=b"a" * 32,
            artifact_sha256=b"c" * 32,
            state="PENDING",
        )
        _seed_family(
            connector,
            candidate_id=published_candidate,
            publication_key=b"b" * 32,
            artifact_sha256=b"d" * 32,
            state="PREPARED",
        )
        _fixture_rows(
            connector,
            [
                (
                    "INSERT INTO operational_catalog_working_candidates "
                    "(slot, candidate_id, assigned_at) VALUES (1, %s, 1)",
                    (active_candidate,),
                ),
                (
                    "INSERT INTO catalog_publication_commits "
                    "(receipt_id, candidate_id, revision, source_revision, "
                    "generation, preparation_id, operational_policy_id, "
                    "artifact_policy_id, display_title_policy_id, new_galleries, "
                    "changed_galleries, removed_galleries, duplicate_losers, "
                    "committed_at) VALUES (%s, %s, 1, 1, 1, %s, 1, 1, 1, "
                    "0, 0, 0, 0, 1)",
                    (b"r" * 16, published_candidate, b"p" * 16),
                ),
            ],
        )
        gate = _exclusive(connector)
        blocked = _issue(connector, gate, now=2)
        assert blocked.terminal

        connector.execute(
            "DELETE FROM operational_catalog_working_candidates "
            "WHERE candidate_id = %s",
            (active_candidate,),
        )
        page = _issue(connector, gate, now=3)
        assert tuple(item.candidate_id for item in page.items) == (active_candidate,)

        connector.execute(
            "INSERT INTO operational_catalog_working_candidates "
            "(slot, candidate_id, assigned_at) VALUES (1, %s, 2)",
            (active_candidate,),
        )
        adapter = _MonotoneAdapter()
        with pytest.raises(ArtifactReleaseUnavailableError):
            ArtifactReleaseRepository.release_page(
                connector,
                backend="sqlite",
                page=page,
                adapters={adapter.adapter_id: adapter},
                now=4,
            )
        assert adapter.calls == []
    finally:
        connector.close()


def test_candidate_race_after_external_release_blocks_state_until_retry(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "artifact-release-race.sqlite3")
    try:
        candidate_id = b"a" * 16
        publication_key = b"p" * 32
        _seed_candidate(connector, candidate_id=candidate_id, reserved_revision=1)
        token = _seed_family(
            connector,
            candidate_id=candidate_id,
            publication_key=publication_key,
            artifact_sha256=b"c" * 32,
            state="PENDING",
        )
        gate = _exclusive(connector)
        page = _issue(connector, gate, now=2)
        adapter = _MonotoneAdapter()
        acknowledgement = ArtifactReleaseRepository.release_page(
            connector,
            backend="sqlite",
            page=page,
            adapters={adapter.adapter_id: adapter},
            now=3,
        )

        connector.execute(
            "INSERT INTO operational_catalog_working_candidates "
            "(slot, candidate_id, assigned_at) VALUES (1, %s, 4)",
            (candidate_id,),
        )
        with pytest.raises(ArtifactReleaseUnavailableError):
            _commit(connector, acknowledgement, now=5)
        assert connector.fetch_one(
            "SELECT state FROM catalog_prepared_artifacts "
            "WHERE candidate_id = %s AND publication_key = %s",
            (candidate_id, publication_key),
        ) == ("PENDING",)

        connector.execute(
            "DELETE FROM operational_catalog_working_candidates "
            "WHERE candidate_id = %s",
            (candidate_id,),
        )
        retry_page = _issue(connector, gate, now=6)
        assert retry_page.items[0].protection_token == token
        retry_acknowledgement = ArtifactReleaseRepository.release_page(
            connector,
            backend="sqlite",
            page=retry_page,
            adapters={adapter.adapter_id: adapter},
            now=7,
        )
        _commit(connector, retry_acknowledgement, now=8)
        assert [call[1] for call in adapter.calls] == [token, token]
        assert adapter.tombstones == {token}
    finally:
        connector.close()


@pytest.mark.parametrize("fail_on_update", (1, 2))
def test_every_release_state_statement_rolls_back_the_complete_page(
    tmp_path: Path,
    fail_on_update: int,
) -> None:
    connector = _database(tmp_path / f"artifact-release-fault-{fail_on_update}.sqlite3")
    try:
        candidate_id = b"a" * 16
        first_publication = b"a" * 32
        second_publication = b"b" * 32
        _seed_candidate(connector, candidate_id=candidate_id, reserved_revision=1)
        _seed_family(
            connector,
            candidate_id=candidate_id,
            publication_key=first_publication,
            artifact_sha256=b"c" * 32,
            state="PENDING",
        )
        _seed_family(
            connector,
            candidate_id=candidate_id,
            publication_key=second_publication,
            artifact_sha256=b"d" * 32,
            state="PREPARED",
        )
        gate = _exclusive(connector)
        page = _issue(connector, gate, now=2)
        adapter = _MonotoneAdapter()
        acknowledgement = ArtifactReleaseRepository.release_page(
            connector,
            backend="sqlite",
            page=page,
            adapters={adapter.adapter_id: adapter},
            now=3,
        )

        original_execute_affected = connector.execute_affected
        update_count = 0

        def faulting_execute_affected(
            query: str,
            data: tuple[Any, ...] = (),
        ) -> int:
            nonlocal update_count
            if query.startswith("UPDATE catalog_prepared_artifacts"):
                update_count += 1
                if update_count == fail_on_update:
                    raise RuntimeError("injected release CAS fault")
            return original_execute_affected(query, data)

        with (
            patch.object(
                connector,
                "execute_affected",
                side_effect=faulting_execute_affected,
            ),
            pytest.raises(RuntimeError, match="injected release CAS fault"),
        ):
            _commit(connector, acknowledgement, now=4)

        assert connector.fetch_all(
            "SELECT publication_key, state "
            "FROM catalog_prepared_artifacts WHERE candidate_id = %s "
            "ORDER BY publication_key",
            (candidate_id,),
        ) == [
            (first_publication, "PENDING"),
            (second_publication, "PREPARED"),
        ]
        completed = _commit(connector, acknowledgement, now=5)
        assert completed.transitioned_count == 2
    finally:
        connector.close()


def test_forged_capabilities_and_corrupt_token_facts_fail_closed(
    tmp_path: Path,
) -> None:
    connector = _database(tmp_path / "artifact-release-forgery.sqlite3")
    try:
        candidate_id = b"a" * 16
        publication_key = b"p" * 32
        _seed_candidate(connector, candidate_id=candidate_id, reserved_revision=1)
        _seed_family(
            connector,
            candidate_id=candidate_id,
            publication_key=publication_key,
            artifact_sha256=b"c" * 32,
            state="PENDING",
        )
        gate = _exclusive(connector)
        with pytest.raises(TypeError, match="repository-issued"):
            ArtifactReleasePage(gate, b"", b"", 1, (), True, object())

        connector.execute("PRAGMA foreign_keys = OFF")
        try:
            connector.execute(
                "UPDATE catalog_prepared_artifacts "
                "SET artifact_sha256 = %s "
                "WHERE candidate_id = %s AND publication_key = %s",
                (b"z" * 32, candidate_id, publication_key),
            )
        finally:
            connector.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(ArtifactReleaseConflictError):
            _issue(connector, gate, now=2)
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


def test_mariadb_page_shape_uses_portable_placeholders_and_keyset_predicate() -> None:
    recorder = _MariaPageRecorder()
    gate = GateLease(
        b"g" * 16,
        1,
        GateMode.EXCLUSIVE,
        tuple(range(64)),
        1_000,
    )
    cursor = b"c" * 16 + b"p" * 32
    with patch.object(
        MaintenanceGateRepository,
        "lock_and_require_live",
        return_value=gate,
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
    assert "prepared.candidate_id > %s" in recorder.query
    assert "operational_catalog_working_candidates" in recorder.query
    assert "catalog_publication_commits" in recorder.query
    assert recorder.parameters == (b"c" * 16, b"c" * 16, b"p" * 32, 8)
