"""Real SQL batching, replay and corruption evidence for existing canonical values."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
import test_vnext_publication_canonical_batch as batch_tests
import test_vnext_publication_canonical_fencing as fixtures

import h2hdb.vnext_ingest_publication as publication
from h2hdb import CoreConfig
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_canonical_value_family import (
    CanonicalValueReadReceipt,
    load_allocation_families,
    load_sealed_value_identities,
)
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueCollisionError,
    CanonicalValuePartialFamilyError,
    CanonicalValueUploadPlan,
    PreparedCanonicalPage,
)
from h2hdb.vnext_ingest_fence_repository import IngestTurn
from h2hdb.vnext_maintenance_gate_repository import GateLease
from h2hdb.vnext_publication_candidate_repository import (
    PublicationCandidateNotReadyError,
    PublicationCatalogProjectionPlan,
)


class _Projection:
    """Independent source plan, with all database operations using real SQLite."""

    def __init__(self, count: int, consumer: bytes) -> None:
        self.payloads = tuple(
            f"sealed-batch-{index}".encode() for index in range(count)
        )
        self.consumer = consumer

    def iter_canonical_value_plans(self) -> Iterator[CanonicalValueUploadPlan]:
        for payload in self.payloads:
            yield CanonicalValueUploadPlan.from_parts(
                "source_title_utf8_v1", (payload,)
            )

    def _canonical_consumer_cursor(self, _value: bytes) -> bytes:
        return self.consumer

    def close(self) -> None:
        pass


@contextmanager
def _existing(
    tmp_path: Path,
    count: int,
    config: CoreConfig | None = None,
) -> Iterator[
    tuple[
        SQLConnector,
        GateLease,
        IngestTurn,
        _Projection,
        publication._CanonicalBatchWork,
    ]
]:
    backend = "sqlite" if config is None else "mariadb"
    context = (
        fixtures._generated_catalog_plan(tmp_path / "sealed.sqlite3")
        if config is None
        else fixtures._generated_mariadb_catalog_plan(config)
    )
    with (
        context as (
            connector,
            gate,
            turn,
            catalog,
        )
    ):
        first = next(catalog.iter_canonical_value_plans())
        try:
            projection = _Projection(
                count, catalog._canonical_consumer_cursor(first.value_sha256)
            )
        finally:
            first.close()
        uploads = tuple(projection.iter_canonical_value_plans())
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
                        projection.consumer,
                        turn.generation,
                    ),
                )
                for upload in uploads
            )
            for start in range(0, count, publication._MAX_CANONICAL_BATCH_VALUES):
                batch_tests._commit(
                    connector,
                    backend,
                    gate,
                    turn,
                    publication._CanonicalBatchWork(items[start : start + 16], owner),
                )
            with connector.read_transaction():
                sealed = load_sealed_value_identities(
                    connector,
                    value_sha256s=tuple(upload.value_sha256 for upload in uploads),
                )
            with connector.transaction():
                connector.execute(
                    "DELETE FROM operational_canonical_value_uploads WHERE generation = %s",
                    (turn.generation,),
                )
            yield (
                connector,
                gate,
                turn,
                projection,
                publication._CanonicalBatchWork(
                    tuple(
                        replace(item, sealed=sealed[item.plan.value_sha256])
                        for item in items
                    ),
                    owner,
                ),
            )
        finally:
            for upload in uploads:
                upload.close()


@contextmanager
def _count_sql(
    connector: SQLConnector, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Counter[str]]:
    counts: Counter[str] = Counter()
    with monkeypatch.context() as patch:
        for name in (
            "fetch_one",
            "fetch_all",
            "execute",
            "execute_affected",
            "execute_many",
        ):
            original = getattr(connector, name)

            def counted(
                *args: Any, _original: Any = original, _name: str = name, **kwargs: Any
            ) -> Any:
                counts[_name] += 1
                return _original(*args, **kwargs)

            patch.setattr(connector, name, counted)
        yield counts


def _cache(projection: _Projection) -> publication._PublicationPlanCache:
    return publication._PublicationPlanCache(
        action=publication._Action.BUILD_CATALOG,
        authority=object(),
        plan=cast(PublicationCatalogProjectionPlan, projection),
    )


def _prepare(
    connector: SQLConnector,
    cache: publication._PublicationPlanCache,
    owner: object,
    turn: IngestTurn,
) -> publication._CanonicalBatchWork | None:
    with connector.read_transaction():
        return publication._prepare_canonical_window(
            connector,
            cached=cache,
            owner=owner,
            candidate_id=fixtures._CANDIDATE,
            generation=turn.generation,
            checkpoint_cursor=b"",
            checkpoint_state="OPEN",
        )


def test_existing_window_and_claim_sql_calls_are_per_batch_not_per_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed: list[tuple[Counter[str], Counter[str]]] = []
    for count in (1, 16):
        directory = tmp_path / str(count)
        directory.mkdir()
        with _existing(directory, count) as (
            connector,
            gate,
            turn,
            projection,
            expected,
        ):
            cache = _cache(projection)
            try:
                with _lease(cache) as owner:
                    with _count_sql(connector, monkeypatch) as reads:
                        batch = _prepare(connector, cache, owner, turn)
                    assert batch is not None
                    assert len(batch.items) == count
                    assert all(item.sealed is not None for item in batch.items)
                    with _count_sql(connector, monkeypatch) as commits:
                        batch_tests._commit(connector, "sqlite", gate, turn, batch)
                    observed.append((reads, commits))
                with _lease(cache) as owner:
                    assert _prepare(connector, cache, owner, turn) is None
                assert cache.current_canonical_plan() is None
                assert load_sealed_value_identities(
                    connector,
                    value_sha256s=tuple(
                        item.plan.value_sha256 for item in expected.items
                    ),
                ) == {item.plan.value_sha256: item.sealed for item in expected.items}
            finally:
                cache.retire()
    assert observed[0] == observed[1]
    assert observed[0][0]["fetch_one"] == 0
    assert observed[0][1]["execute_many"] == 1


@contextmanager
def _lease(
    cache: publication._PublicationPlanCache,
) -> Iterator[publication._PublicationPlanLease]:
    lease = cache.borrow()
    try:
        yield lease
    finally:
        lease.close()


def test_seventeen_existing_values_are_claimed_in_two_bounded_windows(
    tmp_path: Path,
) -> None:
    with _existing(tmp_path, 17) as (connector, gate, turn, projection, _expected):
        cache = _cache(projection)
        sizes: list[int] = []
        try:
            while cache.current_canonical_plan() is not None:
                with _lease(cache) as owner:
                    batch = _prepare(connector, cache, owner, turn)
                    if batch is not None:
                        sizes.append(len(batch.items))
                        batch_tests._commit(connector, "sqlite", gate, turn, batch)
            assert sizes == [16, 1]
        finally:
            cache.retire()


@pytest.mark.parametrize("fault", ["rollback", "response_loss"])
@pytest.mark.parametrize("backend", batch_tests._BACKENDS)
def test_existing_claim_batch_recovers_atomically_without_rewriting_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
    backend: str,
    request: pytest.FixtureRequest,
) -> None:
    config = (
        None
        if backend == "sqlite"
        else cast(CoreConfig, request.getfixturevalue("mariadb_config"))
    )
    with _existing(tmp_path, 3, config) as (connector, gate, turn, _projection, batch):
        before = batch_tests._snapshot(connector, batch)
        original = connector.execute_many if fault == "rollback" else connector.commit

        def fail(*args: Any, **kwargs: Any) -> None:
            original(*args, **kwargs)
            raise ConnectionError("injected claim boundary failure")

        with monkeypatch.context() as patch:
            patch.setattr(
                connector, "execute_many" if fault == "rollback" else "commit", fail
            )
            with pytest.raises(ConnectionError, match="claim boundary"):
                batch_tests._commit(connector, backend, gate, turn, batch)
        durable = batch_tests._snapshot(connector, batch)
        if fault == "rollback":
            assert durable == before
        else:
            assert all(cast(tuple[Any, ...], value)[3] for value in durable)
        batch_tests._commit(connector, backend, gate, turn, batch)
        after = batch_tests._snapshot(connector, batch)
        assert tuple(cast(tuple[Any, ...], value)[:3] for value in after) == tuple(
            cast(tuple[Any, ...], value)[:3] for value in before
        )
        if fault == "response_loss":
            assert after == durable


@pytest.mark.parametrize("corruption", ["payload", "missing_anchor", "root"])
def test_existing_window_rejects_corrupt_durable_values(
    tmp_path: Path, corruption: str
) -> None:
    with _existing(tmp_path, 3) as (connector, _gate, turn, projection, batch):
        item = batch.items[-1]
        page = cast(PreparedCanonicalPage, item.page)
        # Simulate storage corruption beyond the normal FK-protected writer.
        connector.execute("PRAGMA foreign_keys = OFF")
        with connector.transaction():
            if corruption == "payload":
                connector.execute(
                    "UPDATE catalog_canonical_value_page_payloads SET page_bytes = %s WHERE page_sha256 = %s",
                    (b"corrupt", page.page_sha256),
                )
            elif corruption == "root":
                connector.execute(
                    "UPDATE catalog_canonical_value_identities SET root_page_sha256 = %s WHERE value_sha256 = %s",
                    (b"z" * 32, item.plan.value_sha256),
                )
            else:
                connector.execute(
                    "DELETE FROM catalog_canonical_value_allocation_anchors WHERE value_sha256 = %s",
                    (item.plan.value_sha256,),
                )
        cache = _cache(projection)
        try:
            with _lease(cache) as owner:
                if corruption == "missing_anchor":
                    # Claim commit independently reads the union of all family
                    # keys, including facts whose anchor has disappeared.
                    with pytest.raises(CanonicalValuePartialFamilyError):
                        batch_tests._commit(connector, "sqlite", _gate, turn, batch)
                else:
                    with pytest.raises(
                        RuntimeError, match="partial or corrupt|full tree validation"
                    ):
                        _prepare(connector, cache, owner, turn)
        finally:
            cache.retire()


def test_existing_claim_rejects_changed_root_after_preparation(tmp_path: Path) -> None:
    with _existing(tmp_path, 2) as (connector, gate, turn, _projection, batch):
        first, second = batch.items
        proposed = replace(
            batch,
            items=(
                replace(
                    first,
                    sealed=replace(
                        cast(CanonicalValueReadReceipt, first.sealed),
                        root_page_sha256=cast(
                            CanonicalValueReadReceipt, second.sealed
                        ).root_page_sha256,
                    ),
                ),
                second,
            ),
        )
        before = batch_tests._snapshot(connector, batch)
        with pytest.raises(CanonicalValueCollisionError, match="root differs"):
            batch_tests._commit(connector, "sqlite", gate, turn, proposed)
        assert batch_tests._snapshot(connector, batch) == before


def test_allocation_family_batch_rejects_129_keys_before_sql() -> None:
    with pytest.raises(ValueError, match="128"):
        load_allocation_families(
            cast(SQLConnector, object()),
            value_sha256s=tuple(index.to_bytes(32, "big") for index in range(129)),
        )


@pytest.mark.parametrize("stale", ["consumer", "generation"])
def test_existing_claims_reject_a_stale_fence_before_insertion(
    tmp_path: Path, stale: str
) -> None:
    with _existing(tmp_path, 3) as (connector, gate, turn, projection, batch):
        before = batch_tests._snapshot(connector, batch)
        if stale == "consumer":
            with connector.transaction():
                connector.execute(
                    "UPDATE catalog_publication_checkpoints SET `cursor` = %s WHERE candidate_id = %s AND stage = %s",
                    (
                        projection.consumer,
                        fixtures._CANDIDATE,
                        b"BUILD_CATALOG_PROJECTION",
                    ),
                )
        else:
            batch = replace(
                batch,
                items=tuple(
                    replace(
                        item,
                        stage_fence=replace(
                            cast(publication._CanonicalStageFence, item.stage_fence),
                            ingest_generation=turn.generation + 1,
                        ),
                    )
                    for item in batch.items
                ),
            )
        with pytest.raises((RuntimeError, PublicationCandidateNotReadyError)):
            batch_tests._commit(connector, "sqlite", gate, turn, batch)
        assert batch_tests._snapshot(connector, batch) == before
