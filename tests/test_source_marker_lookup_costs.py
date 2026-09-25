"""Exact marker reuse with bounded physical work, including a degraded view plan.

The target observation is sealed by the public source workflow. Extra retained
files belong to an unsealed observation, seeded with foreign keys enabled; this
is a lookup-cost fixture, not a claim that the extra graph passes READY audit.
"""

from __future__ import annotations

from contextlib import closing
from typing import Any

import pytest
from test_vnext_source_marker import MarkerSource
from vnext_fault_harness import backend_of, open_connector
from vnext_pipeline import (
    claim_session,
    gallery,
    ingest_policy,
    initialize_database,
    run_source,
)
from vnext_publication_cleanup_fixtures import partial_publication_setup

from h2hdb import CoreConfig, VNextIngestFacade, VNextSourceCompletionMarker
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_identity import file_key
from h2hdb.vnext_source_marker_family import (
    CachedSourceObservation,
    SourceMarkerConflictError,
    _load_cached_batch,
)

_PREFIX = "catalog_gallery_observation_file_filesystem_"
_VALUES = (
    ("devices", "device"),
    ("inodes", "inode"),
    ("modified_nses", "modified_ns"),
    ("changed_nses", "changed_ns"),
)
# Fixed before measuring the candidate; independent of retained input size.
_SINGLE_MARKER_VM_BUDGET = 2048


def _seed_target(config: CoreConfig) -> VNextSourceCompletionMarker:
    initialize_database(config)
    item = gallery(1001, pages=[], artists=(), language=None)
    source = MarkerSource([item])
    with VNextIngestFacade(config) as facade:
        session = claim_session(facade)
        policy = facade.ensure_policy(session, ingest_policy(artifacts_required=False))
        run_source(facade, session, policy, source)
    return source.observe_completion_marker(item.locator)


def _binding(connector: SQLConnector) -> tuple[int, int, bytes]:
    rows = connector.fetch_all(
        "SELECT gallery_id, observation_id, file_key "
        "FROM catalog_gallery_observation_completion_marker"
    )
    assert len(rows) == 1
    gallery_id, observation_id, key = rows[0]
    assert (
        type(gallery_id) is int and type(observation_id) is int and type(key) is bytes
    )
    return gallery_id, observation_id, key


def _old_view_query(query: str) -> str:
    """Deliberately restore the old outer-joined view for a negative control."""
    if not query.startswith("SELECT observation.gallery_id"):
        return query
    start = query.index("LEFT JOIN " + _PREFIX + "anchors AS fs_anchor ")
    end = query.index("LEFT JOIN catalog_gallery_observation_file_artifact_role")
    result = (
        query[:start] + "LEFT JOIN catalog_gallery_observation_file_filesystem AS fs "
        "ON fs.gallery_id = file.gallery_id "
        "AND fs.observation_id = file.observation_id AND fs.file_key = file.file_key "
        + query[end:]
    )
    for alias, field in (
        ("fs_device", "device"),
        ("fs_inode", "inode"),
        ("fs_modified", "modified_ns"),
        ("fs_changed", "changed_ns"),
        ("fs_anchor", "file_key"),
        ("fs_seal", "file_key"),
    ):
        result = result.replace(f"{alias}.{field}", f"fs.{field}")
    return result


def _lookup(
    connector: SQLConnector, binding: tuple[int, int, bytes]
) -> dict[tuple[int, int], CachedSourceObservation]:
    return _load_cached_batch(connector, bindings=(binding,))


def test_marker_lookup_matches_sealed_source_and_view(
    db_config: CoreConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    expected = _seed_target(db_config)
    with closing(open_connector(db_config)) as connector:
        binding = _binding(connector)
        actual = _lookup(connector, binding)
        assert actual[binding[:2]].marker == expected
        original = connector.fetch_all

        def degraded(query: str, data: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
            return original(_old_view_query(query), data)

        monkeypatch.setattr(connector, "fetch_all", degraded)
        assert _lookup(connector, binding) == actual


@pytest.mark.parametrize("missing", ["anchors", "seals", *[v[0] for v in _VALUES]])
def test_marker_lookup_rejects_every_missing_filesystem_part(
    db_config: CoreConfig, missing: str
) -> None:
    _seed_target(db_config)
    with closing(open_connector(db_config)) as connector:
        with connector.read_transaction():
            binding = _binding(connector)
            assert _lookup(connector, binding)
        # Deliberate corruption may leave retained FK children. The lookup must
        # reject the missing authority after enforcement has been restored.
        with partial_publication_setup(connector, backend=backend_of(db_config)):
            connector.execute(
                f"DELETE FROM {_PREFIX}{missing} WHERE gallery_id = %s "
                "AND observation_id = %s AND file_key = %s",
                binding,
            )
        with pytest.raises(SourceMarkerConflictError, match="sealed observation"):
            _lookup(connector, binding)


def _seed_unrequested_files(
    connector: SQLConnector, binding: tuple[int, int, bytes], count: int
) -> None:
    if not count:
        return
    gallery_id, observation_id, _key = binding
    retained_observation = observation_id + 1
    digest = connector.fetch_one(
        "SELECT file_sha256 FROM catalog_gallery_observation_file_file_sha256s "
        "WHERE gallery_id = %s AND observation_id = %s AND file_key = %s",
        binding,
    )[0]
    with connector.transaction():
        connector.execute(
            "INSERT INTO catalog_gallery_observation_allocations "
            "(gallery_id, observation_id, allocated_at) VALUES (%s, %s, 1)",
            (gallery_id, retained_observation),
        )
        for start in range(0, count, 128):
            names = tuple(
                f"unrequested-{index:08d}.png".encode()
                for index in range(start, min(count, start + 128))
            )
            keys = [
                (gallery_id, retained_observation, file_key(name)) for name in names
            ]
            connector.execute_many(
                "INSERT INTO catalog_file_name_identities (file_key, name_bytes) "
                "VALUES (%s, %s)",
                [(key[2], name) for key, name in zip(keys, names, strict=True)],
            )
            for suffix, field in (
                ("anchors", None),
                ("file_nos", "file_no"),
                ("file_sha256s", "file_sha256"),
                ("artifact_role", "artifact_role"),
                ("seals", None),
            ):
                rows: list[tuple[Any, ...]] = []
                for index, key in enumerate(keys, start):
                    value = index if field == "file_no" else digest
                    if field == "artifact_role":
                        value = b"page"
                    rows.append(key if field is None else (*key, value))
                connector.execute_many(
                    f"INSERT INTO catalog_gallery_observation_file_{suffix} "
                    "(gallery_id, observation_id, file_key"
                    + ("" if field is None else ", " + field)
                    + ") VALUES (%s, %s, %s"
                    + ("" if field is None else ", %s")
                    + ")",
                    rows,
                )
            for suffix, field in (("anchors", None), *_VALUES, ("seals", None)):
                connector.execute_many(
                    f"INSERT INTO {_PREFIX}{suffix} "
                    "(gallery_id, observation_id, file_key"
                    + ("" if field is None else ", " + field)
                    + ") VALUES (%s, %s, %s"
                    + ("" if field is None else ", %s")
                    + ")",
                    [key if field is None else (*key, bytes(8)) for key in keys],
                )


def _measured_lookup(
    connector: SQLiteConnector, binding: tuple[int, int, bytes]
) -> tuple[dict[tuple[int, int], CachedSourceObservation], int]:
    steps = 0

    def progress() -> int:
        nonlocal steps
        steps += 1
        return 0

    connector.connection.set_progress_handler(progress, 1)
    try:
        result = _lookup(connector, binding)
    finally:
        connector.connection.set_progress_handler(None, 0)
    return result, steps


@pytest.mark.deep
@pytest.mark.parametrize("retained", [0, 128, 512, 4096, 32767])
def test_marker_lookup_vm_bound_and_old_view_negative_control(
    sqlite_config: CoreConfig, retained: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    expected = _seed_target(sqlite_config)
    with closing(open_connector(sqlite_config)) as connector:
        assert isinstance(connector, SQLiteConnector)
        binding = _binding(connector)
        _seed_unrequested_files(connector, binding, retained)
        assert connector.fetch_all("PRAGMA foreign_key_check") == []
        results = []
        for _cycle in range(3):
            result, steps = _measured_lookup(connector, binding)
            assert result[binding[:2]].marker == expected
            assert steps <= _SINGLE_MARKER_VM_BUDGET
            results.append(result)
        original = connector.fetch_all

        def degraded(query: str, data: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
            return original(_old_view_query(query), data)

        monkeypatch.setattr(connector, "fetch_all", degraded)
        for cycle in range(3):
            result, steps = _measured_lookup(connector, binding)
            assert result == results[cycle]
            if retained >= 128:
                assert steps > _SINGLE_MARKER_VM_BUDGET
