from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector, SQLiteDuplicateKeyError
from h2hdb.vnext_download_ingest_repository import (
    DownloadCapabilityCollisionError,
    DownloadIngestCorruptionError,
    DownloadIngestReplayMismatchError,
    DownloadIngestRepository,
    DownloadIngestUnavailableError,
    DownloadTurn,
    HandoffKind,
)
from h2hdb.vnext_transaction import StaleWriteError, VNextUnitOfWork


class _FaultConnector(SQLiteConnector):
    fail_fragment: str | None = None
    fail_affected_fragment: str | None = None

    def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
        if self.fail_fragment is not None and self.fail_fragment in query:
            raise RuntimeError("injected coordinated transaction failure")
        super().execute(query, data)

    def execute_affected(self, query: str, data: tuple[Any, ...] = ()) -> int:
        if (
            self.fail_affected_fragment is not None
            and self.fail_affected_fragment in query
        ):
            return 0
        return super().execute_affected(query, data)


def _generated_database(
    path: Path, *, connector_type: type[SQLiteConnector] = SQLiteConnector
) -> SQLiteConnector:
    connector = connector_type(str(path))
    connector.connect()
    payload: Any = ARTIFACT["backends"]
    payload = payload["sqlite"]
    for _slice_id, statements in payload["slices"]:
        for _statement_id, _kind, _name, sql in statements:
            connector.execute(sql)
    for seed in payload["bootstrap_seeds"]:
        connector.execute(seed["sql"], seed["parameters"])
    return connector


def _claim_download(
    connector: SQLiteConnector,
    monkeypatch: pytest.MonkeyPatch,
    token: bytes,
    *,
    now: int,
    duration: int,
) -> DownloadTurn:
    monkeypatch.setattr(
        "h2hdb.vnext_download_ingest_repository._new_download_owner_token",
        lambda: token,
    )
    with connector.transaction():
        return DownloadIngestRepository.claim_download(
            VNextUnitOfWork(connector, backend="sqlite"),
            now=now,
            lease_duration=duration,
        )


def _snapshot(connector: SQLiteConnector) -> tuple[object, ...]:
    return (
        connector.fetch_all(
            "SELECT generation, started_at, completed_at "
            "FROM operational_download_generations ORDER BY generation"
        ),
        connector.fetch_all(
            "SELECT current_generation, completed_generation, last_transition_at "
            "FROM operational_download_coordination_heads"
        ),
        connector.fetch_all(
            "SELECT generation, owner_token, claimed_at "
            "FROM operational_download_generation_owners ORDER BY generation"
        ),
        connector.fetch_all(
            "SELECT generation, lease_expires_at "
            "FROM operational_download_generation_leases ORDER BY generation"
        ),
        connector.fetch_all(
            "SELECT download_generation, owner_token, handoff_kind, requested_at "
            "FROM operational_download_ingest_handoffs ORDER BY download_generation"
        ),
        connector.fetch_all(
            "SELECT download_generation, ingest_generation, consumed_at "
            "FROM operational_download_ingest_consumptions "
            "ORDER BY download_generation"
        ),
        connector.fetch_all(
            "SELECT ingest_generation, owner_token, completed_at "
            "FROM operational_coordinated_ingest_completions "
            "ORDER BY ingest_generation"
        ),
        connector.fetch_all(
            "SELECT generation, started_at, completed_at "
            "FROM operational_ingest_generations ORDER BY generation"
        ),
        connector.fetch_all(
            "SELECT current_generation, completed_generation, phase "
            "FROM operational_ingest_coordination_heads"
        ),
        connector.fetch_all(
            "SELECT generation, owner_token, claimed_at "
            "FROM operational_ingest_generation_owners ORDER BY generation"
        ),
        connector.fetch_all(
            "SELECT generation, lease_expires_at "
            "FROM operational_ingest_generation_leases ORDER BY generation"
        ),
    )


def test_live_download_handoff_moves_capability_and_exact_replay_is_zero_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connector = _generated_database(tmp_path / "download-handoff.sqlite3")
    try:
        turn = _claim_download(connector, monkeypatch, b"d" * 16, now=10, duration=50)
        assert turn == DownloadTurn(1, b"d" * 16, 60)
        assert connector.fetch_all(
            "SELECT generation, started_at, completed_at "
            "FROM operational_download_generations ORDER BY generation"
        ) == [(0, 10, 10), (1, 10, None)]

        before_resume = _snapshot(connector)
        with connector.transaction():
            assert (
                DownloadIngestRepository.resume_download(
                    VNextUnitOfWork(connector, backend="sqlite"), turn, now=20
                )
                == turn
            )
        assert _snapshot(connector) == before_resume

        with pytest.raises(DownloadIngestUnavailableError, match="awaits"):
            _claim_download(connector, monkeypatch, b"e" * 16, now=20, duration=50)
        assert _snapshot(connector) == before_resume

        with connector.transaction():
            handoff = DownloadIngestRepository.handoff_download(
                VNextUnitOfWork(connector, backend="sqlite"), turn, now=25
            )
        assert handoff.handoff_kind is HandoffKind.DOWNLOADER
        assert (
            connector.fetch_all(
                "SELECT generation FROM operational_download_generation_owners"
            )
            == []
        )
        assert (
            connector.fetch_all(
                "SELECT generation FROM operational_download_generation_leases"
            )
            == []
        )

        committed = _snapshot(connector)
        with connector.transaction():
            assert (
                DownloadIngestRepository.handoff_download(
                    VNextUnitOfWork(connector, backend="sqlite"), turn, now=25
                )
                == handoff
            )
        assert _snapshot(connector) == committed

        with pytest.raises(DownloadIngestReplayMismatchError, match="replay tuple"):
            with connector.transaction():
                DownloadIngestRepository.handoff_download(
                    VNextUnitOfWork(connector, backend="sqlite"), turn, now=26
                )
        assert _snapshot(connector) == committed
        with pytest.raises(DownloadIngestUnavailableError, match="stale"):
            with connector.transaction():
                DownloadIngestRepository.resume_download(
                    VNextUnitOfWork(connector, backend="sqlite"), turn, now=26
                )
        assert _snapshot(connector) == committed
    finally:
        connector.close()


def test_recoverable_handoff_and_exact_completion_poll(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _generated_database(tmp_path / "download-handoff-poll.sqlite3")
    try:
        turn = _claim_download(
            connector,
            monkeypatch,
            b"d" * 16,
            now=10,
            duration=100,
        )
        with connector.transaction():
            handoff = DownloadIngestRepository.handoff_download(
                VNextUnitOfWork(connector, backend="sqlite"),
                turn,
                now=20,
                recover_existing=True,
            )
        with connector.transaction():
            recovered = DownloadIngestRepository.handoff_download(
                VNextUnitOfWork(connector, backend="sqlite"),
                turn,
                now=21,
                recover_existing=True,
            )
        assert recovered == handoff

        with connector.read_transaction():
            assert not DownloadIngestRepository.is_download_handoff_complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                handoff,
            )

        monkeypatch.setattr(
            "h2hdb.vnext_download_ingest_repository._new_ingest_owner_token",
            lambda: b"i" * 16,
        )
        with connector.transaction():
            ingest_turn = DownloadIngestRepository.claim_ingest(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=30,
                lease_duration=100,
            )
        with connector.read_transaction():
            assert not DownloadIngestRepository.is_download_handoff_complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                handoff,
            )
        with connector.transaction():
            DownloadIngestRepository.complete_ingest(
                VNextUnitOfWork(connector, backend="sqlite"),
                ingest_turn,
                now=40,
            )
        with connector.read_transaction():
            assert DownloadIngestRepository.is_download_handoff_complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                handoff,
            )
    finally:
        connector.close()


def test_generated_bcnf_keys_and_closed_handoff_enum_are_enforced(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "physical-keys.sqlite3")
    try:
        connector.execute_many(
            "INSERT INTO operational_download_generations "
            "(generation, started_at, completed_at) VALUES (%s, %s, NULL)",
            [(1, 1), (2, 2)],
        )
        connector.execute_many(
            "INSERT INTO operational_ingest_generations "
            "(generation, started_at, completed_at) VALUES (%s, %s, NULL)",
            [(1, 1), (2, 2)],
        )
        with pytest.raises(SQLiteDuplicateKeyError):
            connector.execute(
                "INSERT INTO operational_download_ingest_handoffs "
                "(download_generation, owner_token, handoff_kind, requested_at) "
                "VALUES (%s, %s, %s, %s)",
                (1, b"a" * 16, "LEGACY_ALIAS", 1),
            )
        connector.execute_many(
            "INSERT INTO operational_download_ingest_handoffs "
            "(download_generation, owner_token, handoff_kind, requested_at) "
            "VALUES (%s, %s, %s, %s)",
            [
                (1, b"a" * 16, "DOWNLOADER", 1),
                (2, b"b" * 16, "EXPIRED_TAKEOVER", 2),
            ],
        )
        connector.execute(
            "INSERT INTO operational_download_ingest_consumptions "
            "(download_generation, ingest_generation, consumed_at) "
            "VALUES (%s, %s, %s)",
            (1, 1, 3),
        )
        with pytest.raises(SQLiteDuplicateKeyError):
            connector.execute(
                "INSERT INTO operational_download_ingest_consumptions "
                "(download_generation, ingest_generation, consumed_at) "
                "VALUES (%s, %s, %s)",
                (1, 2, 4),
            )
        with pytest.raises(SQLiteDuplicateKeyError):
            connector.execute(
                "INSERT INTO operational_download_ingest_consumptions "
                "(download_generation, ingest_generation, consumed_at) "
                "VALUES (%s, %s, %s)",
                (2, 1, 4),
            )

        connector.execute(
            "INSERT INTO operational_coordinated_ingest_completions "
            "(ingest_generation, owner_token, completed_at) VALUES (%s, %s, %s)",
            (1, b"i" * 16, 5),
        )
        with pytest.raises(SQLiteDuplicateKeyError):
            connector.execute(
                "INSERT INTO operational_coordinated_ingest_completions "
                "(ingest_generation, owner_token, completed_at) "
                "VALUES (%s, %s, %s)",
                (2, b"i" * 16, 6),
            )
    finally:
        connector.close()


def test_linked_ingest_consumes_once_completes_both_heads_and_replays(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connector = _generated_database(tmp_path / "linked.sqlite3")
    try:
        download = _claim_download(
            connector, monkeypatch, b"d" * 16, now=10, duration=100
        )
        with connector.transaction():
            DownloadIngestRepository.handoff_download(
                VNextUnitOfWork(connector, backend="sqlite"), download, now=20
            )
        monkeypatch.setattr(
            "h2hdb.vnext_download_ingest_repository._new_ingest_owner_token",
            lambda: b"i" * 16,
        )
        with connector.transaction():
            ingest = DownloadIngestRepository.claim_ingest(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=21,
                lease_duration=100,
            )
        assert not ingest.is_periodic
        assert ingest.download_generation == 1
        assert ingest.handoff_owner_token == b"d" * 16
        assert ingest.handoff_kind is HandoffKind.DOWNLOADER
        assert connector.fetch_all(
            "SELECT download_generation, ingest_generation, consumed_at "
            "FROM operational_download_ingest_consumptions"
        ) == [(1, 1, 21)]

        active = _snapshot(connector)
        with connector.transaction():
            assert (
                DownloadIngestRepository.resume_ingest(
                    VNextUnitOfWork(connector, backend="sqlite"), ingest, now=22
                )
                == ingest
            )
        assert _snapshot(connector) == active
        with pytest.raises(DownloadIngestReplayMismatchError, match="consumption"):
            with connector.transaction():
                DownloadIngestRepository.resume_ingest(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    replace(ingest, consumed_at=22),
                    now=22,
                )
        assert _snapshot(connector) == active
        with pytest.raises(DownloadIngestUnavailableError, match="already consumed"):
            with connector.transaction():
                DownloadIngestRepository.claim_ingest(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    now=22,
                    lease_duration=100,
                )
        assert _snapshot(connector) == active

        with connector.transaction():
            completion = DownloadIngestRepository.complete_ingest(
                VNextUnitOfWork(connector, backend="sqlite"), ingest, now=30
            )
        assert completion.download_generation == 1
        assert connector.fetch_one(
            "SELECT current_generation, completed_generation "
            "FROM operational_download_coordination_heads"
        ) == (1, 1)
        assert connector.fetch_one(
            "SELECT completed_at FROM operational_download_generations "
            "WHERE generation = 1"
        ) == (30,)
        assert connector.fetch_one(
            "SELECT current_generation, completed_generation, phase "
            "FROM operational_ingest_coordination_heads"
        ) == (1, 1, "READY")

        durable = _snapshot(connector)
        with connector.transaction():
            assert (
                DownloadIngestRepository.complete_ingest(
                    VNextUnitOfWork(connector, backend="sqlite"), ingest, now=30
                )
                == completion
            )
        assert _snapshot(connector) == durable
        with connector.transaction():
            assert (
                DownloadIngestRepository.complete_ingest(
                    VNextUnitOfWork(connector, backend="sqlite"), ingest, now=31
                )
                == completion
            )
        assert _snapshot(connector) == durable
        with pytest.raises(DownloadIngestReplayMismatchError, match="another owner"):
            with connector.transaction():
                DownloadIngestRepository.complete_ingest(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    replace(
                        ingest,
                        ingest_turn=replace(ingest.ingest_turn, owner_token=b"x" * 16),
                    ),
                    now=30,
                )
        assert _snapshot(connector) == durable

        successor = _claim_download(
            connector, monkeypatch, b"e" * 16, now=40, duration=100
        )
        assert successor.generation == 2
        after_successor = _snapshot(connector)
        with connector.transaction():
            assert (
                DownloadIngestRepository.complete_ingest(
                    VNextUnitOfWork(connector, backend="sqlite"), ingest, now=30
                )
                == completion
            )
        assert _snapshot(connector) == after_successor
    finally:
        connector.close()


def test_expired_download_takeover_is_fail_closed_and_preserves_exact_kind(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connector = _generated_database(tmp_path / "takeover.sqlite3")
    try:
        download = _claim_download(
            connector, monkeypatch, b"d" * 16, now=10, duration=10
        )
        monkeypatch.setattr(
            "h2hdb.vnext_download_ingest_repository._new_ingest_owner_token",
            lambda: b"i" * 16,
        )
        with pytest.raises(DownloadIngestUnavailableError, match="live lease"):
            with connector.transaction():
                DownloadIngestRepository.claim_ingest(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    now=19,
                    lease_duration=100,
                )

        with connector.transaction():
            ingest = DownloadIngestRepository.claim_ingest(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=20,
                lease_duration=100,
            )
        assert ingest.handoff_kind is HandoffKind.EXPIRED_TAKEOVER
        assert connector.fetch_one(
            "SELECT owner_token, handoff_kind, requested_at "
            "FROM operational_download_ingest_handoffs"
        ) == (b"d" * 16, "EXPIRED_TAKEOVER", 20)
        assert not connector.fetch_all(
            "SELECT generation FROM operational_download_generation_owners"
        )
        assert not connector.fetch_all(
            "SELECT generation FROM operational_download_generation_leases"
        )

        before = _snapshot(connector)
        with pytest.raises(DownloadIngestReplayMismatchError, match="replay tuple"):
            with connector.transaction():
                DownloadIngestRepository.handoff_download(
                    VNextUnitOfWork(connector, backend="sqlite"), download, now=20
                )
        assert _snapshot(connector) == before
    finally:
        connector.close()


def test_periodic_ingest_has_no_download_claim_and_completion_leaves_head_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connector = _generated_database(tmp_path / "periodic.sqlite3")
    try:
        monkeypatch.setattr(
            "h2hdb.vnext_download_ingest_repository._new_ingest_owner_token",
            lambda: b"p" * 16,
        )
        with connector.transaction():
            periodic = DownloadIngestRepository.claim_ingest(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=5,
                lease_duration=100,
                periodic=True,
            )
        assert periodic.is_periodic
        assert connector.fetch_one(
            "SELECT current_generation, completed_generation, last_transition_at "
            "FROM operational_download_coordination_heads"
        ) == (0, 0, 5)
        assert not connector.fetch_all(
            "SELECT download_generation FROM operational_download_ingest_handoffs"
        )
        assert not connector.fetch_all(
            "SELECT download_generation FROM operational_download_ingest_consumptions"
        )

        with pytest.raises(DownloadIngestUnavailableError, match="not quiescent"):
            _claim_download(connector, monkeypatch, b"d" * 16, now=6, duration=100)
        with connector.transaction():
            completion = DownloadIngestRepository.complete_ingest(
                VNextUnitOfWork(connector, backend="sqlite"), periodic, now=10
            )
        assert completion.download_generation is None
        assert connector.fetch_one(
            "SELECT current_generation, completed_generation, last_transition_at "
            "FROM operational_download_coordination_heads"
        ) == (0, 0, 5)

        durable = _snapshot(connector)
        with connector.transaction():
            assert (
                DownloadIngestRepository.complete_ingest(
                    VNextUnitOfWork(connector, backend="sqlite"), periodic, now=10
                )
                == completion
            )
        assert _snapshot(connector) == durable
        download = _claim_download(
            connector, monkeypatch, b"d" * 16, now=11, duration=100
        )
        assert download.generation == 1
        after_download = _snapshot(connector)
        with connector.transaction():
            assert (
                DownloadIngestRepository.complete_ingest(
                    VNextUnitOfWork(connector, backend="sqlite"), periodic, now=10
                )
                == completion
            )
        assert _snapshot(connector) == after_download
    finally:
        connector.close()


@pytest.mark.parametrize(
    "failure_fragment",
    (
        "INSERT INTO operational_download_ingest_consumptions",
        "INSERT INTO operational_coordinated_ingest_completions",
    ),
)
def test_faults_roll_back_cross_authority_transactions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_fragment: str,
) -> None:
    connector = cast(
        _FaultConnector,
        _generated_database(
            tmp_path / f"fault-{failure_fragment[-12:]}.sqlite3",
            connector_type=_FaultConnector,
        ),
    )
    try:
        download = _claim_download(
            connector, monkeypatch, b"d" * 16, now=10, duration=100
        )
        with connector.transaction():
            DownloadIngestRepository.handoff_download(
                VNextUnitOfWork(connector, backend="sqlite"), download, now=20
            )
        monkeypatch.setattr(
            "h2hdb.vnext_download_ingest_repository._new_ingest_owner_token",
            lambda: b"i" * 16,
        )
        if "consumptions" in failure_fragment:
            before = _snapshot(connector)
            connector.fail_fragment = failure_fragment
            with pytest.raises(RuntimeError, match="injected"):
                with connector.transaction():
                    DownloadIngestRepository.claim_ingest(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        now=21,
                        lease_duration=100,
                    )
            connector.fail_fragment = None
            assert _snapshot(connector) == before
            return

        with connector.transaction():
            ingest = DownloadIngestRepository.claim_ingest(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=21,
                lease_duration=100,
            )
        before = _snapshot(connector)
        connector.fail_fragment = failure_fragment
        with pytest.raises(RuntimeError, match="injected"):
            with connector.transaction():
                DownloadIngestRepository.complete_ingest(
                    VNextUnitOfWork(connector, backend="sqlite"), ingest, now=30
                )
        connector.fail_fragment = None
        assert _snapshot(connector) == before
    finally:
        connector.close()


def test_owner_transfer_and_head_cas_faults_roll_back_every_prior_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    claim_connector = cast(
        _FaultConnector,
        _generated_database(
            tmp_path / "claim-cas.sqlite3", connector_type=_FaultConnector
        ),
    )
    try:
        claim_connector.fail_affected_fragment = (
            "UPDATE operational_download_coordination_heads SET current_generation"
        )
        with pytest.raises(StaleWriteError, match="download coordination head"):
            _claim_download(
                claim_connector, monkeypatch, b"d" * 16, now=10, duration=100
            )
        assert not claim_connector.fetch_all(
            "SELECT generation FROM operational_download_generations"
        )
        assert not claim_connector.fetch_all(
            "SELECT singleton_id FROM operational_download_coordination_heads"
        )
    finally:
        claim_connector.close()

    connector = cast(
        _FaultConnector,
        _generated_database(
            tmp_path / "transfer-cas.sqlite3", connector_type=_FaultConnector
        ),
    )
    try:
        download = _claim_download(
            connector, monkeypatch, b"d" * 16, now=10, duration=100
        )
        before_handoff = _snapshot(connector)
        connector.fail_affected_fragment = (
            "DELETE FROM operational_download_generation_owners"
        )
        with pytest.raises(DownloadIngestCorruptionError, match="deletion affected 0"):
            with connector.transaction():
                DownloadIngestRepository.handoff_download(
                    VNextUnitOfWork(connector, backend="sqlite"), download, now=20
                )
        connector.fail_affected_fragment = None
        assert _snapshot(connector) == before_handoff

        with connector.transaction():
            DownloadIngestRepository.handoff_download(
                VNextUnitOfWork(connector, backend="sqlite"), download, now=20
            )
        monkeypatch.setattr(
            "h2hdb.vnext_download_ingest_repository._new_ingest_owner_token",
            lambda: b"i" * 16,
        )
        with connector.transaction():
            ingest = DownloadIngestRepository.claim_ingest(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=21,
                lease_duration=100,
            )
        before_completion = _snapshot(connector)
        connector.fail_affected_fragment = (
            "UPDATE operational_download_coordination_heads SET completed_generation"
        )
        with pytest.raises(
            StaleWriteError, match="linked download coordination completion"
        ):
            with connector.transaction():
                DownloadIngestRepository.complete_ingest(
                    VNextUnitOfWork(connector, backend="sqlite"), ingest, now=30
                )
        connector.fail_affected_fragment = None
        assert _snapshot(connector) == before_completion
    finally:
        connector.close()


def test_capability_collisions_and_corrupt_satellites_are_zero_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connector = _generated_database(tmp_path / "corruption.sqlite3")
    try:
        download = _claim_download(
            connector, monkeypatch, b"d" * 16, now=10, duration=100
        )
        connector.execute(
            "DELETE FROM operational_download_generation_leases WHERE generation = %s",
            (download.generation,),
        )
        before = _snapshot(connector)
        with pytest.raises(DownloadIngestCorruptionError, match="owner and lease"):
            with connector.transaction():
                DownloadIngestRepository.claim_ingest(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    now=20,
                    lease_duration=100,
                )
        assert _snapshot(connector) == before

        connector.execute(
            "INSERT INTO operational_download_generation_leases "
            "(generation, lease_expires_at) VALUES (%s, %s)",
            (download.generation, download.lease_expires_at),
        )
        with connector.transaction():
            DownloadIngestRepository.handoff_download(
                VNextUnitOfWork(connector, backend="sqlite"), download, now=20
            )
        monkeypatch.setattr(
            "h2hdb.vnext_download_ingest_repository._new_ingest_owner_token",
            lambda: b"i" * 16,
        )
        with connector.transaction():
            ingest = DownloadIngestRepository.claim_ingest(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=21,
                lease_duration=100,
            )
        with connector.transaction():
            DownloadIngestRepository.complete_ingest(
                VNextUnitOfWork(connector, backend="sqlite"), ingest, now=30
            )

        durable = _snapshot(connector)
        with pytest.raises(DownloadCapabilityCollisionError, match="already exists"):
            _claim_download(connector, monkeypatch, b"d" * 16, now=40, duration=100)
        assert _snapshot(connector) == durable
    finally:
        connector.close()


def test_mariadb_resume_uses_server_placeholders_and_row_locks() -> None:
    token = b"d" * 16

    class RecordingConnector:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def fetch_one(self, query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
            del data
            self.queries.append(query)
            if "operational_download_coordination_heads" in query:
                return (1, 0, 1)
            if "operational_download_generations" in query:
                return (1, None)
            if "operational_download_generation_owners" in query:
                return (token, 1)
            if "operational_download_generation_leases" in query:
                return (100,)
            if "operational_download_ingest_handoffs" in query:
                return ()
            if "operational_download_ingest_consumptions" in query:
                return ()
            raise AssertionError(query)

    connector: Any = RecordingConnector()
    turn = DownloadTurn(1, token, 100)
    assert (
        DownloadIngestRepository.resume_download(
            VNextUnitOfWork(connector, backend="mariadb"), turn, now=10
        )
        == turn
    )
    assert len(connector.queries) == 6
    assert all(query.endswith(" FOR UPDATE") for query in connector.queries)
    assert all("?" not in query for query in connector.queries)
    assert all(
        "%s" in query or "singleton_id = 1" in query for query in connector.queries
    )
