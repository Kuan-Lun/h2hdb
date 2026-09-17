"""Marker prefetch preserves the visited prefix's exact canonical authority.

These fault tests use real generated SQLite canonical trees. The observation
row wrapper isolates marker-prefix behavior; full preparation and real keyset
SQL correspondence are exercised by the analysis preparation cost tests.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import pytest
from vnext_canonical_value_fixtures import (
    seed_canonical_allocation,
    seed_canonical_page,
)
from vnext_generated_database import open_generated_sqlite_database

from h2hdb import vnext_analysis_repository as analysis
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueCollisionError,
    CanonicalValueNotReadyError,
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_identity import (
    ANALYSIS_ALREADY_UPLOADED_MARKER,
    CANONICAL_VALUE_CHUNK_BYTES,
    CanonicalValueBranchEntry,
    decode_canonical_value_page,
)
from h2hdb.vnext_transaction import VNextUnitOfWork

type _Fault = Literal[
    "identity", "allocation", "payload", "count", "parent", "domain", "digest"
]


@pytest.fixture
def connector(tmp_path: Path) -> Iterator[SQLiteConnector]:
    database = open_generated_sqlite_database(tmp_path / "marker.sqlite3")
    try:
        yield database
    finally:
        database.close()


def _store_value(
    connector: SQLiteConnector, payload: bytes, *, domain: str = "tag_value_utf8_v1"
) -> bytes:
    with (
        CanonicalValueUploadPlan.from_parts(domain, (payload,)) as plan,
        connector.transaction(),
    ):
        seed_canonical_allocation(
            connector,
            value_sha256=plan.value_sha256,
            digest_domain=plan.digest_domain,
            byte_count=plan.byte_count,
            allocated_at=1,
        )
        for prepared in plan.iter_pages():
            page = decode_canonical_value_page(prepared.page_bytes)
            seed_canonical_page(
                connector,
                page_sha256=prepared.page_sha256,
                value_sha256=plan.value_sha256,
                page_bytes=prepared.page_bytes,
                level=page.level,
                page_position=page.page_position,
                subtree_item_count=page.subtree_byte_count,
            )
            for position, entry in enumerate(page.entries):
                if isinstance(entry, CanonicalValueBranchEntry):
                    connector.execute(
                        "INSERT INTO catalog_canonical_value_page_parents "
                        "(parent_sha256, position, child_sha256) VALUES (%s, %s, %s)",
                        (prepared.page_sha256, position, entry.child_page_sha256),
                    )
        connector.execute(
            "INSERT INTO catalog_canonical_value_identities "
            "(value_sha256, root_page_sha256) VALUES (%s, %s)",
            (plan.value_sha256, plan.root_page_sha256),
        )
        return plan.value_sha256


@dataclass
class _TagRows:
    connector: SQLiteConnector
    values: Sequence[bytes]

    def fetch_one(self, sql: str, parameters: tuple[Any, ...] = ()) -> tuple[Any, ...]:
        return self.connector.fetch_one(sql, parameters)

    def fetch_all(
        self, sql: str, parameters: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        if "catalog_gallery_observation_tags" in sql:
            assert "observed.position > %s" in sql
            assert "ORDER BY observed.position LIMIT %s" in sql
            after, limit = parameters[-2:]
            assert limit == (1 if after == -1 else 128)
            return list(enumerate(self.values))[after + 1 : after + 1 + limit]
        return self.connector.fetch_all(sql, parameters)


def _has_marker(connector: SQLiteConnector, values: Sequence[bytes]) -> bool:
    with connector.read_transaction():
        return analysis._gallery_has_already_uploaded_marker(
            VNextUnitOfWork(
                cast(SQLConnector, _TagRows(connector, values)), backend="sqlite"
            ),
            1,
            1,
        )


def _corrupt_value(connector: SQLiteConnector, value: bytes, fault: _Fault) -> bytes:
    match fault:
        case "domain":
            return _store_value(connector, b"bad-domain", domain="source_title_utf8_v1")
        case "digest":
            return b"invalid digest"
    (root,) = connector.fetch_one(
        "SELECT root_page_sha256 FROM catalog_canonical_value_identities "
        "WHERE value_sha256 = %s",
        (value,),
    )
    # These deliberate corruptions must be readable even when foreign keys
    # would normally prevent an interrupted external write from creating them.
    connector.execute("PRAGMA foreign_keys = OFF")
    try:
        with connector.transaction():
            match fault:
                case "identity":
                    connector.execute(
                        "DELETE FROM catalog_canonical_value_identities "
                        "WHERE value_sha256 = %s",
                        (value,),
                    )
                case "allocation":
                    connector.execute(
                        "DELETE FROM catalog_canonical_value_allocation_seals "
                        "WHERE value_sha256 = %s",
                        (value,),
                    )
                case "payload":
                    (payload,) = connector.fetch_one(
                        "SELECT page_bytes FROM catalog_canonical_value_page_payloads "
                        "WHERE page_sha256 = %s",
                        (root,),
                    )
                    connector.execute(
                        "UPDATE catalog_canonical_value_page_payloads SET page_bytes = %s "
                        "WHERE page_sha256 = %s",
                        (payload[:-1] + bytes([payload[-1] ^ 1]), root),
                    )
                case "count":
                    connector.execute(
                        "UPDATE catalog_canonical_value_page_subtree_item_counts "
                        "SET subtree_item_count = subtree_item_count + 1 "
                        "WHERE page_sha256 = %s",
                        (root,),
                    )
                case _:
                    assert fault == "parent"
                    connector.execute(
                        "INSERT INTO catalog_canonical_value_page_parents "
                        "(parent_sha256, position, child_sha256) VALUES (%s, 0, %s)",
                        (root, root),
                    )
    finally:
        connector.execute("PRAGMA foreign_keys = ON")
    return value


@pytest.mark.parametrize(
    "fault",
    ["identity", "allocation", "payload", "count", "parent", "domain", "digest"],
)
@pytest.mark.parametrize("marker_first", [False, True])
def test_marker_prefetch_exposes_only_visited_prefix_corruption(
    connector: SQLiteConnector, fault: _Fault, marker_first: bool
) -> None:
    ordinary = _store_value(connector, b"ordinary")
    marker = _store_value(connector, ANALYSIS_ALREADY_UPLOADED_MARKER.upper())
    corrupt = _corrupt_value(connector, _store_value(connector, b"bad"), fault)
    if marker_first:
        assert _has_marker(connector, (ordinary, marker, corrupt))
    else:
        error_type: type[Exception] = {
            "identity": CanonicalValueNotReadyError,
            "allocation": CanonicalValueCollisionError,
            "payload": CanonicalValueCollisionError,
            "count": CanonicalValueCollisionError,
            "parent": CanonicalValueCollisionError,
            "domain": analysis.AnalysisCorruptionError,
            "digest": ValueError,
        }[fault]
        with pytest.raises(error_type):
            _has_marker(connector, (ordinary, corrupt, marker))


@pytest.mark.parametrize(
    "fault",
    ["identity", "allocation", "payload", "count", "parent", "domain", "digest"],
)
def test_first_marker_ignores_unvisited_corrupt_tail(
    connector: SQLiteConnector, fault: _Fault
) -> None:
    marker = _store_value(connector, ANALYSIS_ALREADY_UPLOADED_MARKER)
    corrupt = _corrupt_value(connector, _store_value(connector, b"bad"), fault)
    assert _has_marker(connector, (marker, corrupt))


@pytest.mark.parametrize("marker_position", [127, 128, 129])
def test_marker_early_exit_survives_prefetched_corrupt_tail_at_page_boundary(
    connector: SQLiteConnector, marker_position: int
) -> None:
    ordinary = _store_value(connector, b"ordinary")
    marker = _store_value(connector, ANALYSIS_ALREADY_UPLOADED_MARKER)
    corrupt = _corrupt_value(connector, _store_value(connector, b"bad"), "parent")
    assert _has_marker(connector, [ordinary] * marker_position + [marker, corrupt])


@pytest.mark.parametrize(
    "size", [0, CANONICAL_VALUE_CHUNK_BYTES, CANONICAL_VALUE_CHUNK_BYTES + 1]
)
def test_marker_prefetch_retains_streaming_for_multi_page_values(
    connector: SQLiteConnector, monkeypatch: pytest.MonkeyPatch, size: int
) -> None:
    scout = _store_value(connector, b"scout")
    ordinary = _store_value(connector, b"x" * size)
    marker = _store_value(connector, ANALYSIS_ALREADY_UPLOADED_MARKER)
    streamed: list[bytes] = []
    original = CanonicalValueRepository.stream_and_validate

    def observe(work: VNextUnitOfWork, **kwargs: Any) -> Any:
        streamed.append(kwargs["value_sha256"])
        return original(work, **kwargs)

    monkeypatch.setattr(
        CanonicalValueRepository, "stream_and_validate", staticmethod(observe)
    )
    assert _has_marker(connector, (scout, ordinary, marker))
    assert streamed == [
        scout,
        *([ordinary] if size > CANONICAL_VALUE_CHUNK_BYTES else []),
    ]


@pytest.mark.parametrize(
    "error_type", [sqlite3.OperationalError, OSError, KeyboardInterrupt]
)
def test_marker_prefetch_does_not_hide_operational_errors(
    connector: SQLiteConnector,
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[BaseException],
) -> None:
    ordinary = _store_value(connector, b"ordinary")
    marker = _store_value(connector, ANALYSIS_ALREADY_UPLOADED_MARKER)

    def fail(*_args: object, **_kwargs: object) -> dict[tuple[bytes, bytes], bytes]:
        raise error_type("injected prefetch interruption")

    monkeypatch.setattr(
        analysis, "load_and_validate_single_page_canonical_values", fail
    )
    with pytest.raises(error_type, match="injected prefetch interruption"):
        _has_marker(connector, (ordinary, marker))


def test_marker_does_not_validate_unvisited_page_cursor(
    connector: SQLiteConnector, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = _store_value(connector, ANALYSIS_ALREADY_UPLOADED_MARKER)
    ordinary = _store_value(connector, b"ordinary")
    original = _TagRows.fetch_all

    def damaged_tail_position(
        self: _TagRows, sql: str, parameters: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        rows = original(self, sql, parameters)
        if "catalog_gallery_observation_tags" in sql and parameters[-1] == 128:
            assert len(rows) == 128
            rows[-1] = ("not an integer", rows[-1][1])
        return rows

    monkeypatch.setattr(_TagRows, "fetch_all", damaged_tail_position)
    assert _has_marker(connector, [ordinary, marker, *([ordinary] * 127)])


def test_marker_prefetch_revalidates_each_new_snapshot(
    connector: SQLiteConnector,
) -> None:
    scout = _store_value(connector, b"scout")
    ordinary = _store_value(connector, b"ordinary")
    assert not _has_marker(connector, (scout, ordinary))
    _corrupt_value(connector, ordinary, "payload")
    with pytest.raises(CanonicalValueCollisionError):
        _has_marker(connector, (scout, ordinary))


def _scalar_marker_result(
    connector: SQLiteConnector,
    monkeypatch: pytest.MonkeyPatch,
    values: Sequence[bytes],
) -> bool:
    def empty_prefetch(
        _connector: SQLConnector, *, references: Sequence[tuple[bytes, bytes]]
    ) -> dict[tuple[bytes, bytes], bytes]:
        assert len(references) <= 128
        return {}

    with monkeypatch.context() as patch:
        patch.setattr(
            analysis, "load_and_validate_single_page_canonical_values", empty_prefetch
        )
        return _has_marker(connector, values)


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        pytest.param(b"already uploaded", True, id="lowercase"),
        pytest.param(b"ALREADY UPLOADED", True, id="uppercase"),
        pytest.param(b"AlReAdY UpLoAdEd", True, id="mixed-ascii-case"),
        pytest.param(b"xalready uploaded", False, id="prefix"),
        pytest.param(b"already uploadedx", False, id="suffix"),
        pytest.param(b" already uploaded", False, id="leading-space"),
        pytest.param(b"already uploaded\n", False, id="trailing-newline"),
        pytest.param(
            "ａｌｒｅａｄｙ　ｕｐｌｏａｄｅｄ".encode(), False, id="fullwidth"
        ),
        pytest.param(b"", False, id="empty"),
    ],
)
def test_marker_batch_and_scalar_paths_match_exact_ascii_oracle(
    connector: SQLiteConnector,
    monkeypatch: pytest.MonkeyPatch,
    payload: bytes,
    expected: bool,
) -> None:
    scout = _store_value(connector, b"scout")
    value = _store_value(connector, payload)
    assert _has_marker(connector, (scout, value)) is expected
    assert _scalar_marker_result(connector, monkeypatch, (scout, value)) is expected


def test_first_marker_does_not_prefetch_maximum_single_leaf_tail(
    connector: SQLiteConnector, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = _store_value(connector, ANALYSIS_ALREADY_UPLOADED_MARKER)
    values = [marker]
    for position in range(127):
        prefix = f"tail-{position:03d}:".encode()
        payload = prefix + b"x" * (CANONICAL_VALUE_CHUNK_BYTES - len(prefix))
        values.append(_store_value(connector, payload))
    (root,) = connector.fetch_one(
        "SELECT root_page_sha256 FROM catalog_canonical_value_identities "
        "WHERE value_sha256 = %s",
        (marker,),
    )
    canonical_parameters: list[tuple[Any, ...]] = []
    original_all = _TagRows.fetch_all
    original_one = _TagRows.fetch_one

    def forbidden_batch(
        _database: SQLConnector, *, references: Sequence[tuple[bytes, bytes]]
    ) -> dict[tuple[bytes, bytes], bytes]:
        pytest.fail(f"first marker prefetched {len(references)} tail values")

    def observe_all(
        self: _TagRows, sql: str, parameters: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        if "catalog_canonical_value" in sql:
            canonical_parameters.append(parameters)
        return original_all(self, sql, parameters)

    def observe_one(
        self: _TagRows, sql: str, parameters: tuple[Any, ...] = ()
    ) -> tuple[Any, ...]:
        if "catalog_canonical_value" in sql:
            canonical_parameters.append(parameters)
        return original_one(self, sql, parameters)

    monkeypatch.setattr(
        analysis, "load_and_validate_single_page_canonical_values", forbidden_batch
    )
    monkeypatch.setattr(_TagRows, "fetch_all", observe_all)
    monkeypatch.setattr(_TagRows, "fetch_one", observe_one)
    assert _has_marker(connector, values)
    assert canonical_parameters == [(marker,), (root,) * 5, (root,)]
    canonical_parameters.clear()
    assert _scalar_marker_result(connector, monkeypatch, values)
    assert canonical_parameters == [(marker,), (root,) * 5, (root,)]
