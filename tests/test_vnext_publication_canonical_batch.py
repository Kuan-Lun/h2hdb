"""Real SQL atomicity and bounded replay evidence for canonical batches."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from itertools import islice
from pathlib import Path
from typing import Any, cast

import pytest
import test_vnext_publication_canonical_fencing as fixtures

import h2hdb.vnext_ingest_publication as publication
from h2hdb import CoreConfig
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_canonical_value_family import (
    load_allocation_family,
    load_page_family,
    load_sealed_value_identity,
)
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueCollisionError,
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
    PreparedCanonicalPage,
    _seal_authorized,
)
from h2hdb.vnext_ingest_fence_repository import IngestTurn
from h2hdb.vnext_maintenance_gate_repository import GateLease
from h2hdb.vnext_publication_candidate_repository import (
    PublicationCandidateNotReadyError,
    PublicationCatalogProjectionPlan,
)
from h2hdb.vnext_transaction import LockRank, VNextUnitOfWork

_BACKENDS = [
    "sqlite",
    pytest.param("mariadb", marks=[pytest.mark.mariadb, pytest.mark.deep]),
]


@contextmanager
def _database(
    backend: str,
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> Iterator[
    tuple[SQLConnector, GateLease, IngestTurn, PublicationCatalogProjectionPlan]
]:
    context = (
        fixtures._generated_catalog_plan(tmp_path / "batch.sqlite3")
        if backend == "sqlite"
        else fixtures._generated_mariadb_catalog_plan(
            cast(CoreConfig, request.getfixturevalue("mariadb_config")),
        )
    )
    with context as values:
        yield values


@contextmanager
def _batch(
    plan: PublicationCatalogProjectionPlan,
    turn: IngestTurn,
) -> Iterator[publication._CanonicalBatchWork]:
    uploads = tuple(islice(plan.iter_canonical_value_plans(), 3))
    assert len(uploads) == 3
    owner = object()
    try:
        items = tuple(
            publication._CanonicalWork(
                upload,
                owner,
                tuple(upload.iter_pages())[0],
                publication._CanonicalStageFence(
                    fixtures._CANDIDATE,
                    publication._Action.BUILD_CATALOG,
                    plan._canonical_consumer_cursor(upload.value_sha256),
                    turn.generation,
                ),
            )
            for upload in uploads
        )
        yield publication._CanonicalBatchWork(items, owner)
    finally:
        for upload in uploads:
            upload.close()


def _snapshot(
    connector: SQLConnector, batch: publication._CanonicalBatchWork
) -> tuple[object, ...]:
    with connector.read_transaction():
        return tuple(
            (
                load_allocation_family(connector, value_sha256=item.plan.value_sha256),
                load_sealed_value_identity(
                    connector, value_sha256=item.plan.value_sha256
                ),
                load_page_family(
                    connector,
                    page_sha256=cast(PreparedCanonicalPage, item.page).page_sha256,
                ),
                connector.fetch_one(
                    "SELECT generation, value_sha256 FROM operational_canonical_value_uploads "
                    "WHERE generation = %s AND value_sha256 = %s",
                    (
                        cast(
                            publication._CanonicalStageFence, item.stage_fence
                        ).ingest_generation,
                        item.plan.value_sha256,
                    ),
                ),
            )
            for item in batch.items
        )


def _commit(
    connector: SQLConnector,
    backend: str,
    gate: GateLease,
    turn: IngestTurn,
    batch: publication._CanonicalBatchWork,
) -> tuple[bytes, ...]:
    with connector.transaction():
        return publication._commit_canonical_batch(
            VNextUnitOfWork(connector, backend=backend),
            batch=batch,
            gate=gate,
            turn=turn,
            now=120,
        )


@pytest.mark.parametrize("backend", _BACKENDS)
def test_atomic_batch_rolls_back_every_value_on_mid_batch_failure(
    backend: str,
    tmp_path: Path,
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with (
        _database(backend, tmp_path, request) as (connector, gate, turn, plan),
        _batch(plan, turn) as batch,
    ):
        before = _snapshot(connector, batch)
        sealed = 0
        original = _seal_authorized

        def fail_after_second_seal(*args: Any, **kwargs: Any) -> bytes:
            nonlocal sealed
            result = original(*args, **kwargs)
            sealed += 1
            if sealed == 2:
                raise RuntimeError("injected failure after two complete values")
            return result

        with monkeypatch.context() as fault:
            fault.setattr(publication, "_seal_authorized", fail_after_second_seal)
            with pytest.raises(RuntimeError, match="injected failure"):
                _commit(connector, backend, gate, turn, batch)
        assert sealed == 2
        assert _snapshot(connector, batch) == before
        assert _commit(connector, backend, gate, turn, batch) == tuple(
            sorted(item.plan.value_sha256 for item in batch.items)
        )
        for item in batch.items:
            chunks: list[bytes] = []
            with connector.read_transaction():
                CanonicalValueRepository.stream_and_validate(
                    VNextUnitOfWork(connector, backend=backend),
                    value_sha256=item.plan.value_sha256,
                    consume_provisional=chunks.append,
                )
            assert b"".join(chunks) == b"".join(item.plan.iter_payload_parts())


@pytest.mark.parametrize("backend", _BACKENDS)
def test_committed_response_loss_replays_from_fresh_plans_without_changing_facts(
    backend: str,
    tmp_path: Path,
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _database(backend, tmp_path, request) as (connector, gate, turn, plan):
        with _batch(plan, turn) as batch:
            original = connector.commit

            def lose_response() -> None:
                original()
                raise ConnectionError("commit completed but response lost")

            with monkeypatch.context() as fault:
                fault.setattr(connector, "commit", lose_response)
                with pytest.raises(ConnectionError, match="response lost"):
                    _commit(connector, backend, gate, turn, batch)
            durable = _snapshot(connector, batch)
            assert all(
                cast(tuple[object, ...], value)[1] is not None for value in durable
            )
        # Both upload-plan capabilities and page issuers are new after response
        # loss; no consumed in-memory cursor is used to decide completion.
        with _batch(plan, turn) as replay:
            _commit(connector, backend, gate, turn, replay)
            assert _snapshot(connector, replay) == durable


@pytest.mark.parametrize("backend", _BACKENDS)
@pytest.mark.parametrize(
    "stale", ["consumer", "candidate", "generation", "mixed_stage", "mixed_owner"]
)
def test_batch_rejects_stale_or_mixed_fences_before_any_write(
    backend: str,
    stale: str,
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> None:
    with (
        _database(backend, tmp_path, request) as (connector, gate, turn, plan),
        _batch(plan, turn) as batch,
    ):
        items = list(batch.items)
        fence = cast(publication._CanonicalStageFence, items[0].stage_fence)
        if stale == "consumer":
            with connector.transaction():
                connector.execute(
                    "UPDATE catalog_publication_checkpoints SET `cursor` = %s "
                    "WHERE candidate_id = %s AND stage = %s",
                    (
                        min(
                            cast(
                                publication._CanonicalStageFence, item.stage_fence
                            ).first_consumer_cursor
                            for item in items
                        ),
                        fixtures._CANDIDATE,
                        b"BUILD_CATALOG_PROJECTION",
                    ),
                )
        elif stale == "candidate":
            items = [
                replace(
                    item,
                    stage_fence=replace(
                        cast(publication._CanonicalStageFence, item.stage_fence),
                        candidate_id=b"x" * 16,
                    ),
                )
                for item in items
            ]
        elif stale == "generation":
            items = [
                replace(
                    item,
                    stage_fence=replace(
                        cast(publication._CanonicalStageFence, item.stage_fence),
                        ingest_generation=turn.generation + 1,
                    ),
                )
                for item in items
            ]
        elif stale == "mixed_stage":
            items[0] = replace(
                items[0],
                stage_fence=replace(
                    fence, stage_action=publication._Action.BUILD_ARTIFACT_INPUT
                ),
            )
        else:
            items[0] = replace(items[0], owner=object())
        proposed = replace(batch, items=tuple(items))
        before = _snapshot(connector, batch)
        with pytest.raises((RuntimeError, PublicationCandidateNotReadyError)):
            _commit(connector, backend, gate, turn, proposed)
        assert _snapshot(connector, batch) == before


@pytest.mark.parametrize("backend", _BACKENDS)
def test_batch_checks_corrupt_existing_page_and_rolls_back_new_values(
    backend: str,
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> None:
    with (
        _database(backend, tmp_path, request) as (connector, gate, turn, plan),
        _batch(plan, turn) as batch,
    ):
        last = sorted(batch.items, key=lambda item: item.plan.value_sha256)[-1]
        page = cast(PreparedCanonicalPage, last.page)
        with connector.transaction():
            CanonicalValueRepository.allocate(
                VNextUnitOfWork(connector, backend=backend),
                gate_lease=gate,
                ingest_turn=turn,
                plan=last.plan,
                now=120,
            )
        with connector.transaction():
            CanonicalValueRepository.put_page(
                VNextUnitOfWork(connector, backend=backend),
                gate_lease=gate,
                ingest_turn=turn,
                plan=last.plan,
                prepared_page=page,
                now=120,
            )
        with connector.transaction():
            connector.execute(
                "UPDATE catalog_canonical_value_page_payloads SET page_bytes = %s WHERE page_sha256 = %s",
                (b"corrupt bytes", page.page_sha256),
            )
        with pytest.raises(CanonicalValueCollisionError):
            _commit(connector, backend, gate, turn, batch)
        for item in batch.items:
            if item is not last:
                assert (
                    load_allocation_family(
                        connector, value_sha256=item.plan.value_sha256
                    )
                    is None
                )
                assert (
                    load_sealed_value_identity(
                        connector, value_sha256=item.plan.value_sha256
                    )
                    is None
                )


def test_batch_rejects_value_and_encoded_byte_bounds_before_authorization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args: object, **kwargs: object) -> int:
        del args, kwargs
        raise AssertionError("invalid batch must fail before authorization or SQL")

    monkeypatch.setattr(publication, "_authorize_canonical_write", forbidden)
    owner = object()
    uploads = [
        CanonicalValueUploadPlan.from_parts(
            "catalog_summary_utf8_v1", (bytes([index]) * 32768,)
        )
        for index in range(17)
    ]
    try:
        items = tuple(
            publication._CanonicalWork(
                upload,
                owner,
                tuple(upload.iter_pages())[0],
                publication._CanonicalStageFence(
                    b"c" * 16, publication._Action.BUILD_CATALOG, b"first", 7
                ),
            )
            for upload in uploads
        )
        gate, turn = fixtures._test_authorities()
        for selected, message in (
            (items, "value/page bound"),
            (items[:16], "encoded byte bound"),
        ):
            with pytest.raises(ValueError, match=message):
                publication._commit_canonical_batch(
                    cast(VNextUnitOfWork, object()),
                    batch=publication._CanonicalBatchWork(selected, owner),
                    gate=gate,
                    turn=turn,
                    now=1,
                )
    finally:
        for upload in uploads:
            upload.close()


def test_batch_claim_lock_order_follows_candidate_checkpoint_and_sorted_digests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with (
        fixtures._generated_catalog_plan(tmp_path / "order.sqlite3") as (
            connector,
            gate,
            turn,
            plan,
        ),
        _batch(plan, turn) as batch,
    ):
        locks: list[tuple[LockRank, bytes]] = []
        original = VNextUnitOfWork._record_lock

        def observe(work: VNextUnitOfWork, rank: LockRank, key: bytes) -> None:
            locks.append((rank, key))
            original(work, rank, key)

        monkeypatch.setattr(VNextUnitOfWork, "_record_lock", observe)
        _commit(
            connector,
            "sqlite",
            gate,
            turn,
            replace(batch, items=tuple(reversed(batch.items))),
        )
        claim_keys = [key for rank, key in locks if rank is LockRank.ALLOCATOR]
        assert len(claim_keys) == 2 * len(batch.items)
        assert claim_keys == sorted(claim_keys)
        assert max(rank for rank, _key in locks) is LockRank.ALLOCATOR
        assert any(rank is LockRank.CHECKPOINT for rank, _key in locks)


_REPLAY_SCRIPT = r"""
import json
import os
import signal
import sys
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_canonical_value_repository import CanonicalValueUploadPlan
from h2hdb.vnext_ingest_fence_repository import IngestTurn
from h2hdb.vnext_maintenance_gate_repository import GateLease, GateMode
from h2hdb.vnext_transaction import VNextUnitOfWork
import h2hdb.vnext_ingest_publication as publication
state = json.load(sys.stdin)
g = state["gate"]
gate = GateLease(bytes.fromhex(g[0]), g[1], GateMode(g[2]), tuple(g[3]), g[4])
t = state["turn"]
turn = IngestTurn(t[0], bytes.fromhex(t[1]), t[2])
owner = object()
items = []
for row in state["items"]:
    plan = CanonicalValueUploadPlan.from_parts(row[0], (bytes.fromhex(row[1]),))
    pages = tuple(plan.iter_pages())
    items.append(publication._CanonicalWork(plan, owner, pages[0],
        publication._CanonicalStageFence(bytes.fromhex(row[2]), publication._Action.BUILD_CATALOG, bytes.fromhex(row[3]), turn.generation)))
with SQLiteConnector(state["database"]) as connector:
    if state["phase"] == "before_commit":
        original = publication._seal_authorized
        sealed = 0
        def interrupted(*args, **kwargs):
            global sealed
            result = original(*args, **kwargs)
            sealed += 1
            if sealed == 2:
                os.kill(os.getpid(), state["signal"])
            return result
        publication._seal_authorized = interrupted
    elif state["phase"] == "after_commit":
        original = connector.commit
        def interrupted():
            original()
            os.kill(os.getpid(), state["signal"])
        connector.commit = interrupted
    with connector.transaction():
        result = publication._commit_canonical_batch(VNextUnitOfWork(connector, backend="sqlite"),
            batch=publication._CanonicalBatchWork(tuple(items), owner), gate=gate, turn=turn, now=120)
print(json.dumps([value.hex() for value in result]))
"""


@pytest.mark.skipif(
    os.name != "posix", reason="POSIX termination and SQLite crash recovery"
)
@pytest.mark.parametrize("termination", [signal.SIGTERM, signal.SIGKILL])
@pytest.mark.parametrize("phase", ["before_commit", "after_commit"])
def test_fresh_process_replays_after_real_termination_at_atomic_batch_boundaries(
    tmp_path: Path,
    termination: signal.Signals,
    phase: str,
) -> None:
    database = tmp_path / "restart.sqlite3"
    with (
        fixtures._generated_catalog_plan(database) as (connector, gate, turn, plan),
        _batch(plan, turn) as batch,
    ):
        manifest = {
            "database": str(database),
            "phase": phase,
            "signal": int(termination),
            "gate": [
                gate.owner_token.hex(),
                gate.gate_generation,
                gate.mode.value,
                gate.slots,
                gate.lease_expires_at,
            ],
            "turn": [turn.generation, turn.owner_token.hex(), turn.lease_expires_at],
            "items": [
                [
                    item.plan.digest_domain.decode("ascii"),
                    b"".join(item.plan.iter_payload_parts()).hex(),
                    cast(
                        publication._CanonicalStageFence, item.stage_fence
                    ).candidate_id.hex(),
                    cast(
                        publication._CanonicalStageFence, item.stage_fence
                    ).first_consumer_cursor.hex(),
                ]
                for item in batch.items
            ],
        }
        before = _snapshot(connector, batch)
        terminated = subprocess.run(
            [sys.executable, "-c", _REPLAY_SCRIPT],
            input=json.dumps(manifest),
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        assert terminated.returncode == -termination, terminated.stderr
        durable = _snapshot(connector, batch)
        if phase == "before_commit":
            assert durable == before
        else:
            assert all(
                cast(tuple[object, ...], value)[1] is not None for value in durable
            )
        manifest["phase"] = "replay"
        resumed = subprocess.run(
            [sys.executable, "-c", _REPLAY_SCRIPT],
            input=json.dumps(manifest),
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
        assert json.loads(resumed.stdout) == sorted(
            item.plan.value_sha256.hex() for item in batch.items
        )
        after = _snapshot(connector, batch)
        assert all(cast(tuple[object, ...], value)[1] is not None for value in after)
        if phase == "after_commit":
            assert after == durable


@pytest.mark.parametrize("payload_bytes", [8, 32768])
def test_prepare_bounds_lookahead_and_keeps_uncommitted_cursor_and_borrowers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload_bytes: int,
) -> None:
    import test_vnext_publication_canonical_linearization as linear

    values = linear._fixtures(
        tuple(bytes([index]) * payload_bytes for index in range(24))
    )
    state = linear._initial_state(values)
    harness = linear._OptimizedHarness(values, state)
    harness.install(monkeypatch)
    machine = harness.machine(tmp_path / "lookahead.sqlite3")
    authority = object()
    prepared = harness.prepare(machine, authority=authority)
    assert prepared._action is publication._Action.CANONICAL_BATCH
    batch = cast(publication._CanonicalBatchWork, prepared._payload)
    assert 2 <= len(batch.items) <= publication._MAX_CANONICAL_BATCH_VALUES
    assert (
        sum(
            len(cast(PreparedCanonicalPage, item.page).page_bytes)
            for item in batch.items
        )
        <= publication._MAX_CANONICAL_BATCH_BYTES
    )
    assert len(harness.materialized) <= publication._MAX_CANONICAL_BATCH_VALUES
    assert state.claims == set()
    assert state.pages == {}
    first_digest = batch.items[0].plan.value_sha256
    prepared.close()
    replay = harness.prepare(machine, authority=authority)
    replay_batch = cast(publication._CanonicalBatchWork, replay._payload)
    assert replay_batch.items[0].plan.value_sha256 == first_digest
    # Retirement cannot close borrowed plans before the prepared step releases
    # them, including every bounded prefetched plan behind the active cursor.
    machine.close()
    assert not harness.plans[0].closed
    replay.close()
    assert harness.plans[0].closed
    assert all(upload._closed for upload in harness.plans[0]._uploads)
