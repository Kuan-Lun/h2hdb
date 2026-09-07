from __future__ import annotations

from collections.abc import Iterator
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest
from test_vnext_analysis_repository import (
    _authorities,
    _begin,
    _run_file_slice,
    _seed_initial_snapshot,
    _seed_preparation_facts,
)
from vnext_catalog_identity_fixtures import (
    seed_file_name_identity,
    seed_gallery_observation_file,
)
from vnext_generated_database import open_generated_sqlite_database

from h2hdb import CoreConfig, VNextDatabaseAdminFacade
from h2hdb import vnext_analysis_repository as analysis
from h2hdb import vnext_identity as identity
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_canonical_value_repository import CanonicalValueUploadPlan
from h2hdb.vnext_domains import DomainValidationError
from h2hdb.vnext_transaction import VNextUnitOfWork


def _authority() -> analysis._RunAuthority:
    return analysis._RunAuthority(
        b"a" * 16,
        b"b" * 16,
        analysis._Policy(1, 1, 1, 3, 1, 1),
        None,
        0,
    )


class _PagedConnector:
    def __init__(self, count: int) -> None:
        # Repeated hashes straddle page boundaries, and one hash is excluded.
        self.rows = sorted((bytes((index % 3,)) * 32, index) for index in range(count))
        self.source_calls = 0
        self.decision_calls = 0
        self.requested: list[tuple[bytes, ...]] = []

    def fetch_all(self, sql: str, parameters: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        if "catalog_analysis_file_hash_decision_resolved" in sql:
            self.decision_calls += 1
            assert parameters[0] == _authority().analysis_id
            requested = parameters[1:-1]
            assert len(requested) <= 128 and len(set(requested)) == len(requested)
            self.requested.append(requested)
            return [
                (parameters[0], digest, 5, 4 if digest == bytes(32) else 0, 1)
                for digest in reversed(requested)
            ]
        assert "catalog_gallery_observation_file_seals" in sql
        self.source_calls += 1
        assert parameters[-1] == 128
        after = (parameters[-4], parameters[-2]) if len(parameters) > 4 else None
        return [row for row in self.rows if after is None or row > after][:128]


def _work(connector: object, backend: str = "sqlite") -> VNextUnitOfWork:
    return VNextUnitOfWork(cast(SQLConnector, connector), backend=backend)


def _independent_digest(digests: tuple[bytes, ...]) -> bytes:
    """Small fixture oracle deliberately bypassing both production codecs."""

    payload = (
        b"h2hdb-vnext-effective-content\0"
        + (1).to_bytes(4, "big")
        + len(digests).to_bytes(8, "big")
        + b"".join(sorted(digests))
    )
    domain = b"effective_content_v1"
    return sha256(
        b"h2hdb-vnext-canonical-value\0"
        + (1).to_bytes(4, "big")
        + len(domain).to_bytes(4, "big")
        + domain
        + len(payload).to_bytes(8, "big")
        + payload
    ).digest()


@pytest.mark.parametrize("count", [0, 1, 127, 128, 129, 256, 257])
def test_batched_preparation_matches_independent_codec_with_one_source_scan(
    count: int,
) -> None:
    connector = _PagedConnector(count)
    expected = tuple(
        digest for digest, _ordinal in connector.rows if digest != bytes(32)
    )
    plan = analysis._prepare_effective_content_plan(
        _work(connector), _authority(), 1, 1
    )
    if expected:
        assert plan is not None
        with plan:
            assert plan.value_sha256 == _independent_digest(expected)
    else:
        assert plan is None
    assert connector.source_calls == count // 128 + 1
    assert connector.decision_calls == (count + 127) // 128


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [(b"a" * 16, b"x" * 32, 1, 0, 0)],
        [(b"a" * 16, b"x" * 32, 1, 0, 0)] * 2,
        [(b"a" * 16, b"x" * 32, 1, 0, 0), (b"z" * 16, b"y" * 32, 1, 0, 0)],
        [(b"a" * 16, b"x" * 32, 1, 0, 0), (b"a" * 16, b"z" * 32, 1, 0, 0)],
        [(b"a" * 16, b"x" * 32, 1, 0, 0), (b"a" * 16, b"y" * 32)],
    ],
    ids=[
        "missing-all",
        "missing-one",
        "duplicate",
        "foreign-analysis",
        "foreign-digest",
        "shape",
    ],
)
def test_decision_batch_rejects_inexact_authority(rows: list[tuple[Any, ...]]) -> None:
    connector = _PagedConnector(0)
    with (
        patch.object(connector, "fetch_all", return_value=rows),
        pytest.raises(analysis.AnalysisCorruptionError),
    ):
        analysis._resolved_decisions_for_page(
            _work(connector), _authority().analysis_id, (b"x" * 32, b"y" * 32)
        )


@pytest.mark.parametrize("count", [0, 129])
def test_decision_batch_rejects_unbounded_request_without_sql(count: int) -> None:
    connector = _PagedConnector(0)
    with pytest.raises(analysis.AnalysisCorruptionError, match="row cap"):
        analysis._resolved_decisions_for_page(
            _work(connector), _authority().analysis_id, (b"x" * 32,) * count
        )
    assert connector.decision_calls == 0


def test_decision_batch_rejects_invalid_count_domain() -> None:
    connector = _PagedConnector(0)
    with (
        patch.object(
            connector, "fetch_all", return_value=[(b"a" * 16, b"x" * 32, 0, 0, 0)]
        ),
        pytest.raises(DomainValidationError),
    ):
        analysis._resolved_decisions_for_page(
            _work(connector), _authority().analysis_id, (b"x" * 32,)
        )


@pytest.mark.parametrize(
    "rows",
    [
        [(b"x" * 32, index) for index in range(129)],
        [(b"x" * 32,)],
        [(b"x" * 32, 0), (b"x" * 32, 0)],
        [(b"x" * 32, 1), (b"x" * 32, 0)],
    ],
    ids=["over-cap", "shape", "duplicate", "reversed"],
)
def test_source_page_rejects_broken_bound_or_keyset(
    rows: list[tuple[Any, ...]],
) -> None:
    connector = _PagedConnector(0)
    fetch = connector.fetch_all

    def faulty_source(sql: str, parameters: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        if "catalog_gallery_observation_file_seals" in sql:
            return rows
        return fetch(sql, parameters)

    with (
        patch.object(connector, "fetch_all", side_effect=faulty_source),
        pytest.raises(analysis.AnalysisCorruptionError),
    ):
        tuple(
            analysis._iter_effective_content_digests(
                _work(connector), _authority(), 1, 1
            )
        )


def test_effective_spool_short_write_closes_local_payload() -> None:
    class PartialWrite(BytesIO):
        def write(self, payload: Any, /) -> int:
            return super().write(payload[:-1])

    payload = PartialWrite()
    with (
        patch.object(analysis, "TemporaryFile", return_value=payload),
        pytest.raises(OSError, match="partial write"),
    ):
        analysis._EffectiveContentSpool((b"a" * 32,))
    assert payload.closed


@pytest.mark.parametrize("fault", ["truncate", "append", "change"])
def test_effective_spool_rejects_disk_corruption(fault: str) -> None:
    digests = (b"a" * 32, b"b" * 32)
    with analysis._EffectiveContentSpool(digests) as spool:
        assert tuple(spool) == digests
        assert tuple(spool) == digests
        if fault == "truncate":
            spool._payload.truncate(63)
        elif fault == "append":
            spool._payload.seek(0, 2)
            spool._payload.write(b"c")
        else:
            spool._payload.seek(0)
            spool._payload.write(b"c")
        with pytest.raises(analysis.AnalysisCorruptionError, match="spool"):
            tuple(spool)
    assert spool._payload.closed


def test_effective_spool_interruption_discards_prefix_and_restarts_from_authority() -> (
    None
):
    payload = BytesIO()

    def interrupted() -> Iterator[bytes]:
        yield b"a" * 32
        raise InterruptedError("container stopped during local preparation")

    with (
        patch.object(analysis, "TemporaryFile", return_value=payload),
        pytest.raises(InterruptedError),
    ):
        analysis._EffectiveContentSpool(interrupted())
    assert payload.closed
    # A new process reconstructs the complete spool from durable authority.
    with analysis._EffectiveContentSpool((b"a" * 32, b"b" * 32)) as replay:
        assert tuple(replay) == (b"a" * 32, b"b" * 32)


def test_effective_plan_closes_on_reference_codec_failure() -> None:
    connector = _PagedConnector(3)
    original = CanonicalValueUploadPlan.from_parts
    plans: list[CanonicalValueUploadPlan] = []

    def record_plan(*args: Any, **kwargs: Any) -> CanonicalValueUploadPlan:
        plan = original(*args, **kwargs)
        plans.append(plan)
        return plan

    with (
        patch.object(CanonicalValueUploadPlan, "from_parts", side_effect=record_plan),
        patch.object(
            analysis, "effective_content_digest_ordered", return_value=b"z" * 32
        ),
        pytest.raises(analysis.AnalysisCorruptionError, match="registered codec"),
    ):
        analysis._prepare_effective_content_plan(_work(connector), _authority(), 1, 1)
    assert len(plans) == 1 and plans[0]._payload.closed


def _add_preparation_source(
    connector: SQLConnector, first: bytes, second: bytes
) -> None:
    _seed_preparation_facts(
        cast(Any, connector), gallery_id=1, observation_id=1, file_sha256=first
    )
    for ordinal in range(1, 257):
        name = f"bounded-{ordinal}.jpg".encode()
        key = identity.file_key(name)
        seed_file_name_identity(
            connector, file_key=key, name_bytes=name, file_role=b"CONTENT"
        )
        seed_gallery_observation_file(
            connector,
            gallery_id=1,
            observation_id=1,
            file_no=ordinal,
            file_key=key,
            file_sha256=first if ordinal % 2 == 0 else second,
        )


def _assert_backend_preparation(
    connector: SQLConnector,
    backend: str,
    analysis_id: bytes,
    build_id: bytes,
    first: bytes,
    second: bytes,
) -> bytes:
    authority = analysis._RunAuthority(
        analysis_id, build_id, analysis._Policy(1, 1, 1000, 1000, 1, 1), None, 0
    )
    expected = (first,) * 129 + (second,) * 128
    with (
        connector.read_transaction(),
        patch.object(connector, "fetch_one", wraps=connector.fetch_one) as fetched_one,
        patch.object(connector, "fetch_all", wraps=connector.fetch_all) as fetched,
    ):
        plan = analysis._prepare_effective_content_plan(
            _work(connector, backend), authority, 1, 1
        )
        assert plan is not None
        with plan:
            assert plan.value_sha256 == _independent_digest(expected)
            assert not any(
                "catalog_analysis_file_hash_decision_resolved" in call.args[0]
                for call in fetched_one.call_args_list
            ), "decision preparation performed a per-file scalar lookup"
            assert (
                sum(
                    "catalog_analysis_file_hash_decision_resolved" in call.args[0]
                    for call in fetched.call_args_list
                )
                == 3
            )
            assert (
                sum(
                    "catalog_gallery_observation_file_seals" in call.args[0]
                    for call in fetched.call_args_list
                )
                == 3
            )
            return plan.value_sha256


def _assert_backend_reloads_decisions_after_preparation(
    connector: SQLConnector,
    backend: str,
    analysis_id: bytes,
    build_id: bytes,
    first: bytes,
    second: bytes,
) -> None:
    # Deliberately remove one sealed decision between preparation snapshots.
    # A previous successful local plan must not mask this durable corruption.
    with connector.transaction():
        connector.execute(
            "DELETE FROM catalog_a_file_decision_shadow_seals "
            "WHERE analysis_id = %s AND file_sha256 = %s",
            (analysis_id, first),
        )
    try:
        with pytest.raises(analysis.AnalysisCorruptionError, match="omitted"):
            _assert_backend_preparation(
                connector, backend, analysis_id, build_id, first, second
            )
    finally:
        with connector.transaction():
            connector.execute(
                "INSERT INTO catalog_a_file_decision_shadow_seals "
                "(analysis_id, file_sha256) VALUES (%s, %s)",
                (analysis_id, first),
            )
    _assert_backend_preparation(
        connector, backend, analysis_id, build_id, first, second
    )


def test_sqlite_batched_preparation_survives_lost_local_result_and_reopen(
    tmp_path: Path,
) -> None:
    path = tmp_path / "bounded-analysis.sqlite3"
    connector = open_generated_sqlite_database(path)
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            _scope, build, first, second = _seed_initial_snapshot(connector)
            _add_preparation_source(connector, first, second)
        run = _begin(
            connector, gate, turn, build_id=build, analysis_id=b"a" * 16, now=30
        )
        _run_file_slice(
            connector, gate, turn, run.analysis_id, max_rows=128, start_now=100
        )
        expected = _assert_backend_preparation(
            connector, "sqlite", run.analysis_id, build, first, second
        )
        _assert_backend_reloads_decisions_after_preparation(
            connector, "sqlite", run.analysis_id, build, first, second
        )
    finally:
        connector.close()
    # No local spool or plan survives this reconnect. Durable rows are enough.
    reopened = SQLiteConnector(str(path))
    reopened.connect()
    try:
        assert (
            _assert_backend_preparation(
                reopened, "sqlite", run.analysis_id, build, first, second
            )
            == expected
        )
    finally:
        reopened.close()


@pytest.mark.mariadb_smoke
def test_live_mariadb_bounded_preparation_matches_reference(
    mariadb_config: CoreConfig,
) -> None:
    from test_vnext_live_mariadb_analysis_repository import (
        _authorities as maria_authorities,
    )
    from test_vnext_live_mariadb_analysis_repository import (
        _begin as maria_begin,
    )
    from test_vnext_live_mariadb_analysis_repository import (
        _connector,
        _prepare_file_decision_stage,
        _run_stage_to_completion,
    )

    VNextDatabaseAdminFacade(mariadb_config).initialize()
    connector = _connector(mariadb_config)
    connector.connect()
    try:
        gate, turn = maria_authorities(connector)
        with connector.transaction():
            _scope, build, first, second = _seed_initial_snapshot(cast(Any, connector))
            _add_preparation_source(connector, first, second)
        run = maria_begin(connector, gate, turn, build_id=build, analysis_id=b"a" * 16)
        _prepare_file_decision_stage(connector, gate, turn, run.analysis_id)
        _run_stage_to_completion(
            connector,
            gate,
            turn,
            analysis_id=run.analysis_id,
            operation=analysis.AnalysisRepository.process_file_hash_decision_batch,
            batch_prefix=b"bounded-decision-",
            start_now=300,
        )
        _assert_backend_preparation(
            connector, "mariadb", run.analysis_id, build, first, second
        )
        _assert_backend_reloads_decisions_after_preparation(
            connector, "mariadb", run.analysis_id, build, first, second
        )
    finally:
        connector.close()
