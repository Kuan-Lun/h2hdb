from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest
from vnext_analysis_fixtures import (
    complete_analysis_run,
    seed_analysis_component,
    seed_analysis_run,
    seed_content_owner_candidate_shadow,
    seed_content_owner_shadow,
    set_analysis_component_sealed_at,
)
from vnext_canonical_value_fixtures import seed_canonical_value
from vnext_catalog_identity_fixtures import (
    seed_file_name_identity,
    seed_gallery_identity,
    seed_gallery_observation_file,
    seed_tag_term,
)
from vnext_catalog_registry_fixtures import (
    seed_analysis_policy,
    seed_artifact_policy_semantics,
    seed_artifact_producer_fingerprint,
    seed_display_title_policy,
    seed_manifest_policy,
    seed_source_scope,
    seed_title_sort_policy,
)
from vnext_gallery_page_fixtures import (
    seed_gallery_page_bounds,
    seed_gallery_page_descriptor,
)
from vnext_manifest_fixtures import (
    seed_sealed_source_build,
    seed_snapshot_manifest,
)
from vnext_publication_fixtures import (
    seed_publication_commit,
    seed_publication_finalization,
)

import h2hdb.vnext_analysis_repository as analysis_module
from h2hdb import vnext_identity as identity
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_analysis_family import (
    AnalysisFamilyCollisionError,
    require_exact_analysis_state_components,
)
from h2hdb.vnext_analysis_repository import (
    ANALYSIS_COMPONENTS,
    AnalysisCorruptionError,
    AnalysisNotReadyError,
    AnalysisRepository,
)
from h2hdb.vnext_canonical_value_family import CanonicalValueReadReceipt
from h2hdb.vnext_canonical_value_repository import (
    CanonicalValueRepository,
    CanonicalValueUploadPlan,
)
from h2hdb.vnext_identity import (
    GalleryObservationBranchEntry,
    GalleryObservationMetadata,
    GalleryObservationNodeKind,
    build_gallery_observation_metadata_tree,
    decode_gallery_observation_page,
)
from h2hdb.vnext_ingest_fence_repository import (
    IngestFenceRepository,
    IngestFenceUnavailableError,
    IngestTurn,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_source_build_repository import (
    SourceBuildManifestSummary,
    source_build_identity,
    source_build_recovery_identity,
    source_build_snapshot_attempt_id,
)
from h2hdb.vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_EMPTY_EVENT_CHAIN = sha256(b"h2hdb-operational-event-chain-v1\0").digest()
_PRODUCER_FIELDS = (
    b"analysis-test-writer",
    b"cpython-test-abi",
    b"pillow-test-build",
    b"libjpeg-test-build",
    b"zlib-test-build",
)
_PRODUCER_FINGERPRINT = identity.artifact_producer_fingerprint_sha256(*_PRODUCER_FIELDS)
_PRODUCER_EQUIVALENCE_CLASS = identity.artifact_producer_equivalence_class(
    _PRODUCER_FINGERPRINT
)


def test_already_uploaded_marker_rejects_cross_domain_canonical_value(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-marker-domain.sqlite3")
    try:
        value = b"v" * 32
        receipt = CanonicalValueReadReceipt(
            value,
            b"effective_content_v1",
            0,
            b"r" * 32,
        )
        with (
            patch.object(connector, "fetch_all", return_value=[(value,)]),
            patch.object(
                CanonicalValueRepository,
                "stream_and_validate",
                return_value=receipt,
            ),
            pytest.raises(AnalysisCorruptionError, match="wrong digest domain"),
        ):
            analysis_module._gallery_has_already_uploaded_marker(
                VNextUnitOfWork(connector, backend="sqlite"),
                1,
                1,
            )
    finally:
        connector.close()


class _ContentComparatorConnector:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.page_limits: list[int] = []

    def fetch_all(
        self,
        _sql: str,
        parameters: tuple[Any, ...],
    ) -> list[tuple[Any, ...]]:
        last_gallery = int(parameters[-2])
        limit = int(parameters[-1])
        self.page_limits.append(limit)
        return [row for row in self.rows if int(row[1]) > last_gallery][:limit]


def _content_comparator_authority() -> analysis_module._RunAuthority:
    return analysis_module._RunAuthority(
        b"a" * 16,
        b"b" * 16,
        analysis_module._Policy(1, 1, 1, 1, 1, 1),
        None,
        0,
    )


def _stub_work(connector: object) -> VNextUnitOfWork:
    return VNextUnitOfWork(cast(SQLConnector, connector), backend="sqlite")


@pytest.mark.parametrize("deciding_index", range(6))
def test_content_comparator_uses_each_atom_in_exact_lexicographic_order(
    deciding_index: int,
) -> None:
    content = b"c" * 32
    lower: list[int | bytes] = [0, 0, 0, 1, b"s" * 32, b"l" * 32]
    higher = list(lower)
    deciding_atom = lower[deciding_index]
    if deciding_index < 4:
        assert isinstance(deciding_atom, int)
        higher[deciding_index] = deciding_atom + 1
    else:
        assert isinstance(deciding_atom, bytes)
        higher[deciding_index] = bytes((deciding_atom[0] + 1,)) * 32
    rows = [
        (content, 1, lower[0], lower[1], lower[2], *lower[3:]),
        (content, 2, higher[0], higher[1], higher[2], *higher[3:]),
    ]
    connector = _ContentComparatorConnector(rows)
    owner = analysis_module._evaluate_content_owner(
        _stub_work(connector),
        _content_comparator_authority(),
        content,
    )
    assert owner is not None and owner.owner_gallery_id == 2


def test_content_comparator_crosses_the_128_row_page_boundary() -> None:
    content = b"c" * 32
    rows = [
        (
            content,
            gallery,
            0,
            0,
            0,
            1,
            b"s" * 32,
            b"z" * 32 if gallery == 129 else b"l" * 32,
        )
        for gallery in range(1, 130)
    ]
    connector = _ContentComparatorConnector(rows)
    owner = analysis_module._evaluate_content_owner(
        _stub_work(connector),
        _content_comparator_authority(),
        content,
    )
    assert owner is not None and owner.owner_gallery_id == 129
    assert connector.page_limits == [128, 128]


class _GidComparatorConnector:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.page_limits: list[int] = []

    def fetch_all(
        self,
        sql: str,
        parameters: tuple[Any, ...],
    ) -> list[tuple[Any, ...]]:
        assert "catalog_analysis_gid_candidate_resolved" in sql
        assert "catalog_analysis_content_owner_candidate_resolved" in sql
        assert "catalog_gallery_identities" in sql
        last_gallery = int(parameters[-2])
        limit = int(parameters[-1])
        self.page_limits.append(limit)
        return [row for row in self.rows if int(row[0]) > last_gallery][:limit]


@pytest.mark.parametrize("deciding_index", range(5))
def test_gid_winner_comparator_uses_each_atom_in_exact_lexicographic_order(
    deciding_index: int,
) -> None:
    lower: list[int | bytes] = [0, 0, 0, b"s" * 32, b"l" * 32]
    higher = list(lower)
    deciding_atom = lower[deciding_index]
    if deciding_index < 3:
        assert isinstance(deciding_atom, int)
        higher[deciding_index] = deciding_atom + 1
    else:
        assert isinstance(deciding_atom, bytes)
        higher[deciding_index] = bytes((deciding_atom[0] + 1,)) * 32
    connector = _GidComparatorConnector(
        [
            (1, *lower),
            (2, *higher),
        ]
    )
    winner = analysis_module._evaluate_gid_winner(
        _stub_work(connector),
        _content_comparator_authority(),
        10_001,
    )
    assert winner == analysis_module._GidWinner(10_001, 2)
    assert connector.page_limits == [128]


def test_gid_winner_comparator_crosses_the_128_row_page_boundary() -> None:
    connector = _GidComparatorConnector(
        [
            (
                gallery_id,
                0,
                0,
                0,
                b"s" * 32,
                b"z" * 32 if gallery_id == 129 else b"l" * 32,
            )
            for gallery_id in range(1, 130)
        ]
    )
    winner = analysis_module._evaluate_gid_winner(
        _stub_work(connector),
        _content_comparator_authority(),
        10_001,
    )
    assert winner == analysis_module._GidWinner(10_001, 129)
    assert connector.page_limits == [128, 128]


class _SnapshotWinnerConnector:
    def __init__(self, rows: list[tuple[int, int, bytes]]) -> None:
        self.rows = rows
        self.queries: list[str] = []

    def fetch_all(
        self,
        sql: str,
        parameters: tuple[Any, ...],
    ) -> list[tuple[Any, ...]]:
        self.queries.append(sql)
        assert "catalog_analysis_gid_winner_resolved" in sql
        assert "catalog_analysis_gid_candidate_resolved" in sql
        assert "catalog_source_build_galleries" in sql
        assert "catalog_gallery_observation_metadata" in sql
        previous = int(parameters[-2])
        limit = int(parameters[-1])
        return [row for row in self.rows if row[0] > previous][:limit]


@pytest.mark.parametrize("row_count, expected_queries", [(1, 1), (127, 1), (128, 2)])
def test_snapshot_winners_use_bounded_set_joins_without_per_winner_queries(
    row_count: int,
    expected_queries: int,
) -> None:
    connector = _SnapshotWinnerConnector(
        [(gid, gid, bytes(((gid % 251) + 1,)) * 32) for gid in range(1, row_count + 1)]
    )
    winners = list(
        analysis_module._iter_snapshot_winners(
            _stub_work(connector),
            _content_comparator_authority(),
        )
    )
    assert len(winners) == row_count
    assert len(connector.queries) == expected_queries


class _GidKeyspaceConnector:
    def __init__(self, failures: tuple[bool, bool, bool]) -> None:
        self.failures = iter(failures)

    def fetch_one(
        self,
        sql: str,
        _parameters: tuple[Any, ...],
    ) -> tuple[int, ...]:
        assert "catalog_analysis_gid_winner" in sql
        return (1,) if next(self.failures) else ()


@pytest.mark.parametrize(
    "failures",
    [
        (True, False, False),
        (False, True, False),
        (False, False, True),
    ],
)
def test_gid_winner_terminal_keyspace_rejects_orphans_duplicates_and_noncandidates(
    failures: tuple[bool, bool, bool],
) -> None:
    connector = _GidKeyspaceConnector(failures)
    with pytest.raises(AnalysisCorruptionError, match="complete candidate-backed"):
        analysis_module._require_complete_gid_winner_keyspace(
            _stub_work(connector),
            b"a" * 16,
        )


@pytest.mark.parametrize(
    "table, materialize",
    [
        (
            "catalog_analysis_gid_candidate_shadows",
            lambda work: analysis_module._insert_gid_candidate_shadow(
                work,
                b"a" * 16,
                analysis_module._GidCandidate(7),
            ),
        ),
        (
            "catalog_analysis_gid_winner_selections",
            lambda work: analysis_module._insert_gid_winner_selection(
                work,
                b"a" * 16,
                analysis_module._GidWinner(9, 7),
            ),
        ),
    ],
)
def test_gid_narrow_materialization_response_loss_rolls_back_statement(
    tmp_path: Path,
    table: str,
    materialize: Any,
) -> None:
    connector = _generated_database(tmp_path / f"{table}.sqlite3")
    try:
        connector.execute("PRAGMA foreign_keys = OFF")
        original_execute = connector.execute

        def execute_then_fail(sql: str, data: tuple[Any, ...] = ()) -> None:
            original_execute(sql, data)
            if sql.startswith(f"INSERT INTO {table} "):
                raise RuntimeError("injected response loss")

        with pytest.raises(RuntimeError, match="response loss"):
            with connector.transaction():
                with patch.object(
                    connector,
                    "execute",
                    side_effect=execute_then_fail,
                ):
                    materialize(VNextUnitOfWork(connector, backend="sqlite"))
        assert connector.fetch_one(f"SELECT COUNT(*) FROM {table}") == (0,)
    finally:
        connector.close()


def _generated_database(path: Path) -> SQLiteConnector:
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


def _authorities(connector: SQLiteConnector) -> tuple[GateLease, IngestTurn]:
    with connector.transaction():
        with patch(
            "h2hdb.vnext_maintenance_gate_repository._new_owner_token",
            return_value=b"g" * 16,
        ):
            gate = MaintenanceGateRepository.claim_shared(
                VNextUnitOfWork(connector, backend="sqlite"),
                now=10,
                lease_duration=1_000_000,
            )
    with connector.transaction():
        turn = IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend="sqlite"),
            owner_token=b"i" * 16,
            now=11,
            lease_duration=1_000_000,
        )
    return gate, turn


def _canonical_identity(
    connector: SQLiteConnector,
    value_sha256: bytes,
    *,
    domain: bytes,
    serial: int,
) -> None:
    page_bytes = b"test-page\0" + serial.to_bytes(8, "big") + value_sha256
    page_sha256 = sha256(page_bytes).digest()
    seed_canonical_value(
        connector,
        value_sha256=value_sha256,
        digest_domain=domain,
        page_sha256=page_sha256,
        page_bytes=page_bytes,
        subtree_item_count=1,
        allocated_at=1,
    )


def _seed_root(connector: SQLiteConnector) -> bytes:
    root = b"r" * 32
    _canonical_identity(connector, root, domain=b"source_root_v1", serial=1)
    scope = seed_source_scope(
        connector,
        source_root_sha256=root,
    ).scope_key
    seed_manifest_policy(
        connector,
    )
    seed_analysis_policy(
        connector,
    )
    policy_component = identity.artifact_policy_digest(
        1,
        2048,
        _PRODUCER_FINGERPRINT,
    )
    _canonical_identity(
        connector,
        policy_component,
        domain=b"artifact_policy_v2",
        serial=4,
    )
    producer = seed_artifact_producer_fingerprint(
        connector,
        writer_id=_PRODUCER_FIELDS[0],
        python_abi=_PRODUCER_FIELDS[1],
        pillow_build=_PRODUCER_FIELDS[2],
        libjpeg_build=_PRODUCER_FIELDS[3],
        zlib_build=_PRODUCER_FIELDS[4],
    )
    assert producer.producer_fingerprint_sha256 == _PRODUCER_FINGERPRINT
    semantics = seed_artifact_policy_semantics(
        connector,
        producer_fingerprint_sha256=_PRODUCER_FINGERPRINT,
    )
    assert semantics.policy_component_sha256 == policy_component
    connector.execute(
        "INSERT INTO catalog_artifact_policies "
        "(artifact_policy_id, policy_component_sha256) VALUES (1, %s)",
        (policy_component,),
    )
    seed_title_sort_policy(
        connector,
    )
    seed_display_title_policy(
        connector,
    )
    connector.execute(
        "INSERT INTO operational_operational_policys "
        "(operational_policy_id, operational_schema_version, "
        "algorithm_version, max_batch_rows) VALUES (1, 1, 1, 128)"
    )
    for tag_id in (1, 2, 3):
        value = bytes((100 + tag_id,)) * 32
        _canonical_identity(
            connector,
            value,
            domain=b"tag_value_utf8_v1",
            serial=10 + tag_id,
        )
        seed_tag_term(
            connector,
            tag_id=tag_id,
            namespace=b"artist",
            tag_value_sha256=value,
        )
    return scope


def _seed_build(
    connector: SQLiteConnector,
    *,
    build_id: bytes,
    scope: bytes,
    manifest_byte: int,
    gallery_count: int,
    file_count: int = 0,
    byte_count: int = 0,
    base_receipt: bytes | None = None,
    created_at: int = 20,
    sealed_at: int = 21,
    manifest_sha256: bytes | None = None,
    discovery_tree_observation_sha256: bytes = b"t" * 32,
) -> None:
    seed_sealed_source_build(
        connector,
        build_id=build_id,
        scope_key=scope,
        manifest_sha256=(
            bytes((manifest_byte,)) * 32 if manifest_sha256 is None else manifest_sha256
        ),
        gallery_count=gallery_count,
        file_count=file_count,
        byte_count=byte_count,
        created_at=created_at,
        sealed_at=sealed_at,
        discovery_tree_observation_sha256=discovery_tree_observation_sha256,
    )
    connector.execute(
        "INSERT INTO catalog_source_build_channel (build_id, channel) VALUES (%s, %s)",
        (build_id, b"default"),
    )
    if base_receipt is not None:
        connector.execute(
            "INSERT INTO catalog_source_build_base_publication_commits "
            "(build_id, base_receipt_id) VALUES (%s, %s)",
            (build_id, base_receipt),
        )


def _source_build_id(
    connector: SQLiteConnector,
    *,
    scope: bytes,
    manifest_sha256: bytes,
    gallery_count: int,
    file_count: int = 0,
    byte_count: int = 0,
) -> bytes:
    source_root = connector.fetch_one(
        "SELECT source_root_sha256 FROM catalog_source_scopes WHERE scope_key = %s",
        (scope,),
    )
    assert len(source_root) == 1
    summary = SourceBuildManifestSummary(
        manifest_sha256,
        gallery_count,
        file_count,
        byte_count,
    )
    return source_build_identity(
        snapshot_attempt_id=source_build_snapshot_attempt_id(source_root[0], summary),
        scope=scope,
        manifest_policy_id=1,
    )


def _seed_published_commit(
    connector: SQLiteConnector,
    *,
    build_id: bytes,
    snapshot_manifest_sha256: bytes,
    generation: int,
    committed_at: int,
    analysis_id: bytes | None = None,
) -> bytes:
    receipt_id = bytes((64 + generation,)) * 16
    candidate_id = bytes((80 + generation,)) * 16
    preparation_id = bytes((96 + generation,)) * 16
    source_revision = generation
    revision = generation

    if analysis_id is None:
        analysis_row = connector.fetch_one(
            "SELECT analysis_id FROM catalog_analysis_runs "
            "WHERE state = 'COMPLETE' ORDER BY analysis_id LIMIT 1"
        )
        assert len(analysis_row) == 1
        analysis_id = analysis_row[0]
    connector.execute(
        "INSERT INTO catalog_source_revision_descriptors "
        "(source_revision, channel, snapshot_manifest_sha256) VALUES (%s, %s, %s)",
        (source_revision, b"default", snapshot_manifest_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_source_revision_provenance "
        "(source_revision, analysis_id) VALUES (%s, %s)",
        (source_revision, analysis_id),
    )
    connector.execute(
        "INSERT INTO catalog_publication_candidates "
        "(candidate_id, analysis_id, reserved_revision, artifact_policy_id, "
        "display_title_policy_id, artifacts_required, created_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (candidate_id, analysis_id, revision, 1, 1, 0, committed_at - 1),
    )
    connector.execute(
        "INSERT INTO catalog_revision_descriptors "
        "(revision, publication_count, artifact_count) VALUES (%s, 0, 0)",
        (revision,),
    )
    connector.execute(
        "INSERT INTO operational_operational_event_streams "
        "(preparation_id, created_at) VALUES (%s, %s)",
        (preparation_id, committed_at - 1),
    )
    connector.execute(
        "INSERT INTO operational_operational_preparations "
        "(preparation_id, build_id, deletion_request_generation, "
        "operational_policy_id, state, prepared_at, completed_at) "
        "VALUES (%s, %s, 0, 1, 'COMPLETE', %s, %s)",
        (preparation_id, build_id, committed_at - 1, committed_at),
    )
    connector.execute(
        "INSERT INTO operational_operational_preparation_effect_seals "
        "(preparation_id, event_count, final_chain_sha256, sealed_at) "
        "VALUES (%s, 0, %s, %s)",
        (preparation_id, _EMPTY_EVENT_CHAIN, committed_at),
    )
    connector.execute(
        "INSERT INTO catalog_publication_generation_nodes (generation) VALUES (%s)",
        (generation,),
    )
    connector.execute(
        "INSERT INTO catalog_publication_generation_successors "
        "(successor_generation, predecessor_generation) VALUES (%s, %s)",
        (generation, generation - 1),
    )
    seed_publication_commit(
        connector,
        receipt_id=receipt_id,
        candidate_id=candidate_id,
        revision=revision,
        source_revision=source_revision,
        generation=generation,
        preparation_id=preparation_id,
        operational_policy_id=1,
        artifact_policy_id=1,
        display_title_policy_id=1,
        new_galleries=0,
        changed_galleries=0,
        removed_galleries=0,
        duplicate_losers=0,
        committed_at=committed_at,
    )
    existing_head = connector.fetch_one(
        "SELECT receipt_id FROM catalog_publication_commit_head_receipts "
        "WHERE channel = %s",
        (b"default",),
    )
    if existing_head:
        connector.execute(
            "UPDATE catalog_publication_commit_head_receipts "
            "SET receipt_id = %s WHERE channel = %s",
            (receipt_id, b"default"),
        )
    else:
        connector.execute(
            "INSERT INTO catalog_publication_commit_head_receipts "
            "(channel, receipt_id) VALUES (%s, %s)",
            (b"default", receipt_id),
        )
    return receipt_id


def _seed_gallery(
    connector: SQLiteConnector,
    *,
    build_id: bytes,
    scope: bytes,
    gallery_id: int,
    observation_id: int,
    occurrences: tuple[tuple[bytes, int], ...],
    artists: tuple[int, ...],
    serial: int,
) -> None:
    locator = sha256(b"locator" + gallery_id.to_bytes(8, "big")).digest()
    if not connector.fetch_one(
        "SELECT 1 FROM catalog_source_locator_identity WHERE locator_sha256 = %s",
        (locator,),
    ):
        _canonical_identity(
            connector,
            locator,
            domain=b"source_relative_locator_v1",
            serial=1000 + gallery_id,
        )
        connector.execute(
            "INSERT INTO catalog_source_locator_identity "
            "(locator_sha256, source_gallery_name) VALUES (%s, %s)",
            (locator, f"gallery-{gallery_id}".encode()),
        )
        seed_gallery_identity(
            connector,
            gallery_id=gallery_id,
            gallery_key=identity.gallery_key(scope, locator),
            scope_key=scope,
            locator_sha256=locator,
        )
    if not connector.fetch_one(
        "SELECT 1 FROM catalog_source_build_expected_gallery "
        "WHERE build_id = %s AND gallery_id = %s",
        (build_id, gallery_id),
    ):
        connector.execute(
            "INSERT INTO catalog_source_build_expected_gallery "
            "(build_id, position, gallery_id) VALUES (%s, %s, %s)",
            (build_id, gallery_id - 1, gallery_id),
        )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_allocations "
        "(gallery_id, observation_id, allocated_at) VALUES (%s, %s, 1)",
        (gallery_id, observation_id),
    )
    observation = sha256(
        b"observation"
        + gallery_id.to_bytes(8, "big")
        + observation_id.to_bytes(8, "big")
    ).digest()
    _canonical_identity(
        connector,
        observation,
        domain=b"gallery_observation_v1",
        serial=serial,
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observations "
        "(gallery_id, observation_id, observation_identity_sha256) "
        "VALUES (%s, %s, %s)",
        (gallery_id, observation_id, observation),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_galleries "
        "(build_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
        (build_id, gallery_id, observation_id),
    )
    for digest, count in occurrences:
        if not connector.fetch_one(
            "SELECT 1 FROM catalog_content_blobs WHERE file_sha256 = %s",
            (digest,),
        ):
            connector.execute(
                "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
                "VALUES (%s, 1)",
                (digest,),
            )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_file_hash_occurrences "
            "(gallery_id, observation_id, file_sha256, occurrence_count) "
            "VALUES (%s, %s, %s, %s)",
            (gallery_id, observation_id, digest, count),
        )
    for tag_id in artists:
        connector.execute(
            "INSERT INTO catalog_gallery_observation_artists "
            "(gallery_id, observation_id, artist_tag_id) VALUES (%s, %s, %s)",
            (gallery_id, observation_id, tag_id),
        )


def _map_working_build(
    connector: SQLiteConnector,
    *,
    build_id: bytes,
    generation: int,
    replace: bool = False,
) -> None:
    connector.execute(
        "INSERT INTO operational_source_build_generations (build_id, generation) "
        "VALUES (%s, %s)",
        (build_id, generation),
    )
    if replace:
        connector.execute(
            "UPDATE operational_source_working_builds SET build_id = %s, assigned_at = 30 "
            "WHERE slot = 1",
            (build_id,),
        )
    else:
        connector.execute(
            "INSERT INTO operational_source_working_builds "
            "(slot, build_id, assigned_at) VALUES (1, %s, 20)",
            (build_id,),
        )


def _insert_scan_fact(
    connector: SQLiteConnector,
    *,
    gallery_id: int,
    observation_id: int,
    scan_observation_sha256: bytes,
    scan_observation_version: int,
    source_file_count: int,
) -> None:
    key = (gallery_id, observation_id)
    connector.execute(
        "INSERT INTO catalog_gallery_observation_scans "
        "(gallery_id, observation_id, scan_observation_sha256, "
        "scan_observation_version, source_file_count) VALUES (%s, %s, %s, %s, %s)",
        (*key, scan_observation_sha256, scan_observation_version, source_file_count),
    )


def _insert_stat_fact(
    connector: SQLiteConnector,
    *,
    gallery_id: int,
    observation_id: int,
    file_count: int,
    byte_count: int,
) -> None:
    key = (gallery_id, observation_id)
    connector.execute(
        "INSERT INTO catalog_gallery_observation_stat "
        "(gallery_id, observation_id, file_count, byte_count) "
        "VALUES (%s, %s, %s, %s)",
        (*key, file_count, byte_count),
    )


def _seed_preparation_facts(
    connector: SQLiteConnector,
    *,
    gallery_id: int,
    observation_id: int,
    file_sha256: bytes,
) -> None:
    """Add one exact metadata tree and one normalized CONTENT file."""

    metadata = GalleryObservationMetadata(
        10_000 + gallery_id,
        f"streamed title {gallery_id}",
        "",
        "fixture",
        100,
        101,
        102,
        1,
        1,
        1,
    )
    tree = build_gallery_observation_metadata_tree(metadata)
    bounds_by_page: dict[bytes, tuple[bytes, bytes]] = {}
    for encoded in tree.pages:
        page = decode_gallery_observation_page(encoded.page_bytes)
        seed_gallery_page_descriptor(
            connector,
            page_sha256=encoded.page_sha256,
            page_bytes=encoded.page_bytes,
            component=b"METADATA",
            level=page.level,
            subtree_item_count=page.subtree_item_count,
        )
        child_bounds: dict[bytes, tuple[bytes, bytes]] = {}
        if page.node_kind is GalleryObservationNodeKind.BRANCH:
            for position, entry in enumerate(page.entries):
                assert isinstance(entry, GalleryObservationBranchEntry)
                connector.execute(
                    "INSERT INTO catalog_gallery_observation_page_children "
                    "(parent_sha256, position, child_sha256) VALUES (%s, %s, %s)",
                    (encoded.page_sha256, position, entry.child_sha256),
                )
                child_bounds[entry.child_sha256] = bounds_by_page[entry.child_sha256]
        bounds = identity.gallery_observation_page_key_bounds(
            page,
            child_bounds=child_bounds or None,
        )
        assert bounds is not None
        seed_gallery_page_bounds(
            connector,
            page_sha256=encoded.page_sha256,
            first_key=bounds[0],
            last_key=bounds[1],
        )
        bounds_by_page[encoded.page_sha256] = bounds
    connector.execute(
        "INSERT INTO catalog_gallery_observation_tree_roots "
        "(gallery_id, observation_id, root_page_sha256) VALUES (%s, %s, %s)",
        (gallery_id, observation_id, tree.root_page_sha256),
    )
    source_gallery_name = connector.fetch_one(
        "SELECT locator.source_gallery_name "
        "FROM catalog_gallery_identities AS identity "
        "JOIN catalog_source_locator_identity AS locator "
        "ON locator.locator_sha256 = identity.locator_sha256 "
        "WHERE identity.gallery_id = %s",
        (gallery_id,),
    )[0]
    connector.execute(
        "INSERT INTO catalog_gallery_upload_times (gid, upload_time) VALUES (%s, %s)",
        (metadata.gid, metadata.upload_time),
    )
    connector.execute(
        "INSERT INTO catalog_source_gallery_name_gids (source_gallery_name, gid) "
        "VALUES (%s, %s)",
        (source_gallery_name, metadata.gid),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_source_name_accesses "
        "(gallery_id, source_gallery_name) VALUES (%s, %s)",
        (gallery_id, source_gallery_name),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_metadata_locals "
        "(gallery_id, observation_id, download_time, modified_time) "
        "VALUES (%s, %s, %s, %s)",
        (
            gallery_id,
            observation_id,
            metadata.download_time,
            metadata.modified_time,
        ),
    )
    _insert_scan_fact(
        connector,
        gallery_id=gallery_id,
        observation_id=observation_id,
        scan_observation_sha256=sha256(
            b"scan" + gallery_id.to_bytes(8, "big")
        ).digest(),
        scan_observation_version=1,
        source_file_count=1,
    )
    _insert_stat_fact(
        connector,
        gallery_id=gallery_id,
        observation_id=observation_id,
        file_count=1,
        byte_count=1,
    )
    name = f"content-{gallery_id}.jpg".encode("ascii")
    file_key = identity.file_key(name)
    seed_file_name_identity(
        connector,
        file_key=file_key,
        name_bytes=name,
        file_role=b"CONTENT",
    )
    seed_gallery_observation_file(
        connector,
        gallery_id=gallery_id,
        observation_id=observation_id,
        file_no=0,
        file_key=file_key,
        file_sha256=file_sha256,
    )


def _put_canonical_plan(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    plan: CanonicalValueUploadPlan,
    *,
    now: int,
) -> None:
    with connector.transaction():
        CanonicalValueRepository.allocate(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=now,
        )
    for page in plan.iter_pages():
        with connector.transaction():
            CanonicalValueRepository.put_page(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                plan=plan,
                prepared_page=page,
                now=now + 1,
            )
    with connector.transaction():
        CanonicalValueRepository.seal(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            plan=plan,
            now=now + 2,
        )


def _issue_preparation_authority(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    analysis_id: bytes,
    *,
    now: int,
) -> Any:
    with connector.transaction():
        return AnalysisRepository.issue_preparation_authority(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            analysis_id=analysis_id,
            now=now,
        )


def _run_prepared_gallery_stage(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    analysis_id: bytes,
    method: Any,
    *,
    gallery_ids: tuple[int, ...],
    prefix: bytes,
    start_now: int,
    upload_content: bool = False,
    replay_first: bool = False,
) -> tuple[Any, Any]:
    authority = _issue_preparation_authority(
        connector,
        gate,
        turn,
        analysis_id,
        now=start_now,
    )
    preparations = tuple(
        AnalysisRepository.prepare_gallery(
            connector,
            backend="sqlite",
            authority=authority,
            gallery_id=gallery_id,
        )
        for gallery_id in gallery_ids
    )
    try:
        if upload_content:
            for offset, preparation in enumerate(preparations):
                assert preparation.content_upload_plan is not None
                _put_canonical_plan(
                    connector,
                    gate,
                    turn,
                    preparation.content_upload_plan,
                    now=start_now + 1 + offset * 3,
                )
        first_key = prefix + b"-rows"
        with connector.transaction():
            first = method(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=analysis_id,
                batch_key=first_key,
                max_rows=128,
                preparations=preparations,
                now=start_now + 20,
            )
        assert first.row_count == len(gallery_ids)
        assert not first.terminal and first.next_state == "OPEN"
        if replay_first:
            with connector.transaction():
                with (
                    patch.object(
                        connector,
                        "execute",
                        side_effect=AssertionError("batch replay attempted DML"),
                    ),
                    patch.object(
                        connector,
                        "execute_affected",
                        side_effect=AssertionError("batch replay attempted DML"),
                    ),
                ):
                    replay = method(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        analysis_id=analysis_id,
                        batch_key=first_key,
                        max_rows=0,
                        preparations=preparations,
                        now=start_now + 21,
                    )
            assert replay.replayed
            assert (
                replay.start_generation,
                replay.start_cursor,
                replay.start_processed_count,
                replay.next_cursor,
                replay.next_processed_count,
                replay.next_state,
                replay.row_count,
                replay.terminal,
                replay.committed_generation,
                replay.committed_at,
            ) == (
                first.start_generation,
                first.start_cursor,
                first.start_processed_count,
                first.next_cursor,
                first.next_processed_count,
                first.next_state,
                first.row_count,
                first.terminal,
                first.committed_generation,
                first.committed_at,
            )
        with connector.transaction():
            terminal = method(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=analysis_id,
                batch_key=prefix + b"-terminal",
                max_rows=128,
                preparations=(),
                now=start_now + 22,
            )
        assert terminal.terminal and terminal.next_state == "COMPLETE"
        assert terminal.row_count == 0
        return first, terminal
    finally:
        for preparation in preparations:
            preparation.close()


def _run_removed_gallery_stage(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    analysis_id: bytes,
    method: Any,
    *,
    prefix: bytes,
    start_now: int,
) -> tuple[Any, Any]:
    with connector.transaction():
        first = method(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            analysis_id=analysis_id,
            batch_key=prefix + b"-rows",
            max_rows=128,
            preparations=(None,),
            now=start_now,
        )
    assert first.row_count == 1 and not first.terminal
    with connector.transaction():
        terminal = method(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            analysis_id=analysis_id,
            batch_key=prefix + b"-terminal",
            max_rows=128,
            preparations=(),
            now=start_now + 1,
        )
    assert terminal.terminal and terminal.row_count == 0
    return first, terminal


def _run_single_live_gallery_downstream(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    analysis_id: bytes,
    *,
    prefix: bytes,
    start_now: int,
) -> None:
    _run_stage(
        connector,
        gate,
        turn,
        AnalysisRepository.process_impacted_gallery_batch,
        analysis_id=analysis_id,
        prefix=prefix + b"-impact-gallery",
        max_rows=128,
        start_now=start_now,
    )
    _run_prepared_gallery_stage(
        connector,
        gate,
        turn,
        analysis_id,
        AnalysisRepository.process_impacted_content_batch,
        gallery_ids=(1,),
        prefix=prefix + b"-impact-content",
        start_now=start_now + 100,
        upload_content=True,
    )
    _run_prepared_gallery_stage(
        connector,
        gate,
        turn,
        analysis_id,
        AnalysisRepository.process_content_owner_candidate_batch,
        gallery_ids=(1,),
        prefix=prefix + b"-content-candidate",
        start_now=start_now + 200,
    )
    _run_prepared_gallery_stage(
        connector,
        gate,
        turn,
        analysis_id,
        AnalysisRepository.validate_content_owner_candidate_batch,
        gallery_ids=(1,),
        prefix=prefix + b"-validate-content-candidate",
        start_now=start_now + 300,
    )
    _run_stage(
        connector,
        gate,
        turn,
        AnalysisRepository.process_content_owner_batch,
        analysis_id=analysis_id,
        prefix=prefix + b"-content-owner",
        max_rows=128,
        start_now=start_now + 400,
    )
    _run_stage(
        connector,
        gate,
        turn,
        AnalysisRepository.validate_content_owner_batch,
        analysis_id=analysis_id,
        prefix=prefix + b"-validate-content-owner",
        max_rows=128,
        start_now=start_now + 500,
    )
    _run_stage(
        connector,
        gate,
        turn,
        AnalysisRepository.process_impacted_gid_batch,
        analysis_id=analysis_id,
        prefix=prefix + b"-impact-gid",
        max_rows=128,
        start_now=start_now + 600,
    )
    _run_prepared_gallery_stage(
        connector,
        gate,
        turn,
        analysis_id,
        AnalysisRepository.process_gid_candidate_batch,
        gallery_ids=(1,),
        prefix=prefix + b"-gid-candidate",
        start_now=start_now + 700,
    )
    _run_prepared_gallery_stage(
        connector,
        gate,
        turn,
        analysis_id,
        AnalysisRepository.validate_gid_candidate_batch,
        gallery_ids=(1,),
        prefix=prefix + b"-validate-gid-candidate",
        start_now=start_now + 800,
    )
    _run_stage(
        connector,
        gate,
        turn,
        AnalysisRepository.process_gid_winner_batch,
        analysis_id=analysis_id,
        prefix=prefix + b"-gid-winner",
        max_rows=128,
        start_now=start_now + 900,
    )
    _run_stage(
        connector,
        gate,
        turn,
        AnalysisRepository.validate_gid_winner_batch,
        analysis_id=analysis_id,
        prefix=prefix + b"-validate-gid-winner",
        max_rows=128,
        start_now=start_now + 1_000,
    )


def _prepare_upload_and_handoff_snapshot(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    analysis_id: bytes,
    *,
    now: int,
) -> bytes:
    authority = _issue_preparation_authority(
        connector,
        gate,
        turn,
        analysis_id,
        now=now,
    )
    with AnalysisRepository.prepare_snapshot_manifest(
        connector,
        backend="sqlite",
        authority=authority,
    ) as preparation:
        _put_canonical_plan(
            connector,
            gate,
            turn,
            preparation.upload_plan,
            now=now + 1,
        )
        with connector.transaction():
            return AnalysisRepository.handoff_snapshot_manifest(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                preparation=preparation,
                now=now + 4,
            )


def _begin(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    *,
    build_id: bytes,
    analysis_id: bytes,
    now: int,
) -> Any:
    with connector.transaction():
        return AnalysisRepository.begin(
            VNextUnitOfWork(connector, backend="sqlite"),
            gate_lease=gate,
            ingest_turn=turn,
            build_id=build_id,
            policy_id=1,
            proposed_analysis_id=analysis_id,
            now=now,
        )


def _run_stage(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    method: Any,
    *,
    analysis_id: bytes,
    prefix: bytes,
    max_rows: int = 1,
    start_now: int = 100,
    replay_each: bool = False,
) -> list[Any]:
    results = []
    for index in range(1000):
        with connector.transaction():
            result = method(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=analysis_id,
                batch_key=prefix + index.to_bytes(4, "big"),
                max_rows=max_rows,
                now=start_now + index,
            )
        results.append(result)
        if replay_each:
            with connector.transaction():
                with (
                    patch.object(
                        connector,
                        "execute",
                        side_effect=AssertionError("batch replay attempted DML"),
                    ),
                    patch.object(
                        connector,
                        "execute_affected",
                        side_effect=AssertionError("batch replay attempted DML"),
                    ),
                ):
                    replay = method(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        analysis_id=analysis_id,
                        batch_key=prefix + index.to_bytes(4, "big"),
                        max_rows=0,
                        now=start_now + index,
                    )
            assert replay.replayed
            assert (
                replay.start_generation,
                replay.start_cursor,
                replay.start_processed_count,
                replay.page_limit,
                replay.next_cursor,
                replay.next_processed_count,
                replay.next_state,
                replay.row_count,
                replay.terminal,
                replay.committed_generation,
                replay.committed_at,
                replay.component_sealed,
            ) == (
                result.start_generation,
                result.start_cursor,
                result.start_processed_count,
                result.page_limit,
                result.next_cursor,
                result.next_processed_count,
                result.next_state,
                result.row_count,
                result.terminal,
                result.committed_generation,
                result.committed_at,
                result.component_sealed,
            )
        if result.next_state == "COMPLETE":
            return results
    raise AssertionError("analysis stage did not converge")


def _run_file_slice(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    analysis_id: bytes,
    *,
    max_rows: int = 1,
    start_now: int = 100,
    replay_each: bool = False,
) -> None:
    _run_stage(
        connector,
        gate,
        turn,
        AnalysisRepository.process_changed_gallery_batch,
        analysis_id=analysis_id,
        prefix=b"gallery",
        max_rows=max_rows,
        start_now=start_now,
        replay_each=replay_each,
    )
    _run_stage(
        connector,
        gate,
        turn,
        AnalysisRepository.process_changed_file_hash_batch,
        analysis_id=analysis_id,
        prefix=b"hash",
        max_rows=max_rows,
        start_now=start_now + 100,
        replay_each=replay_each,
    )
    _run_stage(
        connector,
        gate,
        turn,
        AnalysisRepository.process_file_hash_decision_batch,
        analysis_id=analysis_id,
        prefix=b"decision",
        max_rows=max_rows,
        start_now=start_now + 200,
        replay_each=replay_each,
    )
    results = _run_stage(
        connector,
        gate,
        turn,
        AnalysisRepository.validate_file_hash_decision_batch,
        analysis_id=analysis_id,
        prefix=b"validate",
        max_rows=max_rows,
        start_now=start_now + 300,
        replay_each=replay_each,
    )
    assert results[-1].component_sealed


def _seed_initial_snapshot(
    connector: SQLiteConnector,
    *,
    generation: int = 1,
) -> tuple[bytes, bytes, bytes, bytes]:
    scope = _seed_root(connector)
    build = _source_build_id(
        connector,
        scope=scope,
        manifest_sha256=bytes((1,)) * 32,
        gallery_count=2,
    )
    first = b"a" * 32
    second = b"b" * 32
    _seed_build(
        connector,
        build_id=build,
        scope=scope,
        manifest_byte=1,
        gallery_count=2,
    )
    _seed_gallery(
        connector,
        build_id=build,
        scope=scope,
        gallery_id=1,
        observation_id=1,
        occurrences=((first, 2),),
        artists=(1, 2),
        serial=100,
    )
    _seed_gallery(
        connector,
        build_id=build,
        scope=scope,
        gallery_id=2,
        observation_id=1,
        occurrences=((first, 1), (second, 1)),
        artists=(2, 3),
        serial=101,
    )
    _map_working_build(connector, build_id=build, generation=generation)
    return scope, build, first, second


def test_begin_rejects_a_different_policy_for_the_same_build_zero_write(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-sole-build-policy.sqlite3")
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            _scope, build, _first, _second = _seed_initial_snapshot(connector)
            seed_analysis_policy(
                connector,
                policy_id=2,
                algorithm_version=2,
            )
        run = _begin(
            connector,
            gate,
            turn,
            build_id=build,
            analysis_id=b"A" * 16,
            now=30,
        )
        before = _logical_database_dump(connector)
        with (
            patch.object(
                connector,
                "execute",
                side_effect=AssertionError("different-policy begin attempted DML"),
            ),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("different-policy begin attempted DML"),
            ),
            pytest.raises(AnalysisCorruptionError, match="different policy"),
        ):
            with connector.transaction():
                AnalysisRepository.begin(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    build_id=build,
                    policy_id=2,
                    proposed_analysis_id=b"B" * 16,
                    now=31,
                )
        assert _logical_database_dump(connector) == before
        assert connector.fetch_all(
            "SELECT analysis_id, policy_id FROM catalog_analysis_run_descriptor "
            "WHERE build_id = %s",
            (build,),
        ) == [(run.analysis_id, 1)]
    finally:
        connector.close()


def _independent_file_oracle(
    connector: SQLiteConnector,
    build_id: bytes,
) -> dict[bytes, tuple[int, int, int]]:
    result: dict[bytes, tuple[int, int, int]] = {}
    hashes = connector.fetch_all(
        "SELECT DISTINCT occurrence.file_sha256 "
        "FROM catalog_source_build_galleries AS member "
        "JOIN catalog_gallery_observation_file_hash_occurrences AS occurrence "
        "ON occurrence.gallery_id = member.gallery_id "
        "AND occurrence.observation_id = member.observation_id "
        "WHERE member.build_id = %s ORDER BY occurrence.file_sha256",
        (build_id,),
    )
    for (digest,) in hashes:
        members = connector.fetch_all(
            "SELECT occurrence.gallery_id, occurrence.observation_id, "
            "occurrence.occurrence_count "
            "FROM catalog_source_build_galleries AS member "
            "JOIN catalog_gallery_observation_file_hash_occurrences AS occurrence "
            "ON occurrence.gallery_id = member.gallery_id "
            "AND occurrence.observation_id = member.observation_id "
            "WHERE member.build_id = %s AND occurrence.file_sha256 = %s",
            (build_id, digest),
        )
        all_artists: set[int] = set()
        maximum = 0
        occurrence_count = 0
        for gallery_id, observation_id, count in members:
            occurrence_count += int(count)
            artists = {
                int(row[0])
                for row in connector.fetch_all(
                    "SELECT artist_tag_id FROM catalog_gallery_observation_artists "
                    "WHERE gallery_id = %s AND observation_id = %s",
                    (gallery_id, observation_id),
                )
            }
            all_artists.update(artists)
            maximum = max(maximum, len(artists))
        result[bytes(digest)] = (occurrence_count, len(all_artists), maximum)
    return result


def test_abandon_is_atomic_replayable_and_preserves_generation_mapping(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-abandon.sqlite3")
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            scope, build, _first, _second = _seed_initial_snapshot(connector)
        run = _begin(
            connector,
            gate,
            turn,
            build_id=build,
            analysis_id=b"X" * 16,
            now=30,
        )
        for failing_sql in (
            "UPDATE catalog_analysis_run_states",
            "DELETE FROM operational_source_working_builds",
        ):
            original = connector.execute_affected

            def fail_statement(
                sql: str,
                parameters: tuple[object, ...] = (),
                *,
                marker: str = failing_sql,
            ) -> int:
                if sql.startswith(marker):
                    raise RuntimeError("injected abandon statement fault")
                return original(sql, parameters)

            with pytest.raises(RuntimeError, match="abandon statement fault"):
                with connector.transaction():
                    with patch.object(
                        connector,
                        "execute_affected",
                        side_effect=fail_statement,
                    ):
                        AnalysisRepository.abandon(
                            VNextUnitOfWork(connector, backend="sqlite"),
                            gate_lease=gate,
                            ingest_turn=turn,
                            analysis_id=run.analysis_id,
                            now=40,
                        )
            assert connector.fetch_one(
                "SELECT state FROM catalog_analysis_run_states WHERE analysis_id = %s",
                (run.analysis_id,),
            ) == ("OPEN",)
            assert connector.fetch_one(
                "SELECT build_id FROM operational_source_working_builds "
                "WHERE slot = %s",
                (1,),
            ) == (build,)

        with connector.transaction():
            abandoned = AnalysisRepository.abandon(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                now=41,
            )
        assert abandoned.state == "ABANDONED" and not abandoned.replayed
        assert (
            connector.fetch_one(
                "SELECT build_id FROM operational_source_working_builds WHERE slot = %s",
                (1,),
            )
            == ()
        )
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_build_generations "
            "WHERE generation = %s",
            (turn.generation,),
        ) == (build,)
        with connector.transaction():
            with (
                patch.object(
                    connector,
                    "execute",
                    side_effect=AssertionError("abandon replay attempted DML"),
                ),
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=AssertionError("abandon replay attempted DML"),
                ),
            ):
                replay = AnalysisRepository.abandon(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    now=42,
                )
        assert replay.state == "ABANDONED" and replay.replayed

        with connector.transaction():
            replacement = _source_build_id(
                connector,
                scope=scope,
                manifest_sha256=bytes((77,)) * 32,
                gallery_count=1,
            )
            _seed_build(
                connector,
                build_id=replacement,
                scope=scope,
                manifest_byte=77,
                gallery_count=1,
            )
            connector.execute(
                "INSERT INTO operational_source_working_builds "
                "(slot, build_id, assigned_at) VALUES (%s, %s, %s)",
                (1, replacement, 43),
            )
        with pytest.raises(AnalysisCorruptionError, match="retained"):
            with connector.transaction():
                AnalysisRepository.abandon(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    now=44,
                )
    finally:
        connector.close()


def _logical_database_dump(connector: SQLiteConnector) -> tuple[str, ...]:
    return tuple(connector.connection.iterdump())


def _seed_abandoned_replay_blocker(
    connector: SQLiteConnector,
    *,
    blocker: str,
    analysis_id: bytes,
    build_id: bytes,
) -> None:
    if blocker == "publication_candidate":
        connector.execute(
            "INSERT INTO catalog_publication_candidates "
            "(candidate_id, analysis_id, reserved_revision, artifact_policy_id, "
            "display_title_policy_id, artifacts_required, created_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (b"C" * 16, analysis_id, 77, 1, 1, 0, 42),
        )
        return
    if blocker == "source_revision_provenance":
        source_revision = 77
        snapshot_manifest_sha256 = b"V" * 32
        _canonical_identity(
            connector,
            snapshot_manifest_sha256,
            domain=b"source_snapshot_manifest_v1",
            serial=977,
        )
        seed_snapshot_manifest(
            connector,
            snapshot_manifest_sha256=snapshot_manifest_sha256,
            gallery_count=2,
            file_count=0,
            byte_count=0,
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_descriptors "
            "(source_revision, channel, snapshot_manifest_sha256) "
            "VALUES (%s, %s, %s)",
            (source_revision, b"default", snapshot_manifest_sha256),
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_provenance "
            "(source_revision, analysis_id) VALUES (%s, %s)",
            (source_revision, analysis_id),
        )
        return
    if blocker == "operational_preparation":
        preparation_id = b"P" * 16
        connector.execute(
            "INSERT INTO operational_operational_event_streams "
            "(preparation_id, created_at) VALUES (%s, %s)",
            (preparation_id, 42),
        )
        connector.execute(
            "INSERT INTO operational_operational_preparations "
            "(preparation_id, build_id, deletion_request_generation, "
            "operational_policy_id, state, prepared_at, completed_at) "
            "VALUES (%s, %s, 0, 1, 'OPEN', %s, NULL)",
            (preparation_id, build_id, 42),
        )
        return
    raise AssertionError(f"unknown replay blocker: {blocker}")


@pytest.mark.parametrize(
    "blocker",
    (
        "publication_candidate",
        "source_revision_provenance",
        "operational_preparation",
    ),
)
def test_abandoned_replay_rejects_terminal_blockers_without_mutation(
    tmp_path: Path,
    blocker: str,
) -> None:
    connector = _generated_database(
        tmp_path / f"analysis-abandon-replay-{blocker}.sqlite3"
    )
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            _scope, build, _first, _second = _seed_initial_snapshot(connector)
        run = _begin(
            connector,
            gate,
            turn,
            build_id=build,
            analysis_id=b"T" * 16,
            now=30,
        )
        with connector.transaction():
            abandoned = AnalysisRepository.abandon(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                now=40,
            )
        assert abandoned.state == "ABANDONED" and not abandoned.replayed

        with connector.transaction():
            replay = AnalysisRepository.abandon(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                now=41,
            )
        assert replay.state == "ABANDONED" and replay.replayed

        with connector.transaction():
            _seed_abandoned_replay_blocker(
                connector,
                blocker=blocker,
                analysis_id=run.analysis_id,
                build_id=build,
            )
        before = _logical_database_dump(connector)

        with pytest.raises(AnalysisCorruptionError, match="terminal-incompatible"):
            with connector.transaction():
                AnalysisRepository.abandon(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    now=43,
                )

        assert _logical_database_dump(connector) == before
        assert connector.fetch_one(
            "SELECT state FROM catalog_analysis_run_states WHERE analysis_id = %s",
            (run.analysis_id,),
        ) == ("ABANDONED",)
        assert (
            connector.fetch_one(
                "SELECT build_id FROM operational_source_working_builds WHERE slot = %s",
                (1,),
            )
            == ()
        )
    finally:
        connector.close()


def test_abandon_rejects_complete_and_stale_ingest_authority(tmp_path: Path) -> None:
    connector = _generated_database(tmp_path / "analysis-abandon-reject.sqlite3")
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            _scope, build, _first, _second = _seed_initial_snapshot(connector)
        run = _begin(
            connector,
            gate,
            turn,
            build_id=build,
            analysis_id=b"Y" * 16,
            now=30,
        )
        stale_turn = IngestTurn(
            turn.generation,
            b"s" * 16,
            turn.lease_expires_at,
        )
        with pytest.raises(IngestFenceUnavailableError, match="stale"):
            with connector.transaction():
                AnalysisRepository.abandon(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=stale_turn,
                    analysis_id=run.analysis_id,
                    now=31,
                )
        started_at = connector.fetch_one(
            "SELECT started_at FROM catalog_analysis_run_descriptor "
            "WHERE analysis_id = %s",
            (run.analysis_id,),
        )[0]
        with connector.transaction():
            complete_analysis_run(
                connector,
                analysis_id=run.analysis_id,
                completed_at=int(started_at) + 1,
            )
        with pytest.raises(AnalysisNotReadyError, match="COMPLETE"):
            with connector.transaction():
                AnalysisRepository.abandon(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    now=32,
                )
        assert connector.fetch_one(
            "SELECT build_id FROM operational_source_working_builds WHERE slot = %s",
            (1,),
        ) == (build,)
    finally:
        connector.close()


def test_component_terminal_receipt_requires_stage_specific_cursor_codec(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-component-codec.sqlite3")
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            _scope, build, _first, _second = _seed_initial_snapshot(connector)
        run = _begin(
            connector,
            gate,
            turn,
            build_id=build,
            analysis_id=b"K" * 16,
            now=30,
        )
        sealed_at = (
            int(
                connector.fetch_one(
                    "SELECT started_at FROM catalog_analysis_run_descriptor "
                    "WHERE analysis_id = %s",
                    (run.analysis_id,),
                )[0]
            )
            + 1
        )
        with connector.transaction():
            for component in ANALYSIS_COMPONENTS:
                seed_analysis_component(
                    connector,
                    analysis_id=run.analysis_id,
                    state_component=component,
                    row_count=2 if component == b"file_hash_decision" else 0,
                    sealed_at=sealed_at,
                    terminal_receipt=True,
                )

        stage = b"validate_file_hash_decision"

        def set_terminal_cursor(cursor: bytes) -> None:
            generation = connector.fetch_one(
                "SELECT start_generation FROM catalog_analysis_batch_receipts "
                "WHERE analysis_id = %s AND stage = %s AND row_count = %s",
                (run.analysis_id, stage, 0),
            )[0]
            connector.execute(
                "UPDATE catalog_analysis_batch_receipt_stored "
                "SET start_cursor = %s, next_cursor = %s "
                "WHERE analysis_id = %s AND stage = %s "
                "AND start_generation = %s",
                (cursor, cursor, run.analysis_id, stage, generation),
            )
            connector.execute(
                "UPDATE catalog_analysis_checkpoints SET `cursor` = %s "
                "WHERE analysis_id = %s AND stage = %s",
                (cursor, run.analysis_id, stage),
            )

        nonzero_key = b"\x01D\x01" + b"k" * 32 + (2).to_bytes(8, "big")
        with connector.transaction():
            set_terminal_cursor(nonzero_key)
            require_exact_analysis_state_components(
                connector,
                analysis_id=run.analysis_id,
                state_components=ANALYSIS_COMPONENTS,
            )

        for corrupt in (
            b"\x01G\x01" + (1).to_bytes(8, "big") + (2).to_bytes(8, "big"),
            nonzero_key[:-1],
        ):
            with pytest.raises(AnalysisFamilyCollisionError):
                with connector.transaction():
                    set_terminal_cursor(corrupt)
                    require_exact_analysis_state_components(
                        connector,
                        analysis_id=run.analysis_id,
                        state_components=ANALYSIS_COMPONENTS,
                    )
    finally:
        connector.close()


def test_depth_zero_file_overlay_matches_independent_full_oracle_and_fails_closed(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-full.sqlite3")
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            _scope, build, first, second = _seed_initial_snapshot(connector)
        run = _begin(
            connector,
            gate,
            turn,
            build_id=build,
            analysis_id=b"A" * 16,
            now=30,
        )
        assert run.overlay_depth == 0
        assert run.anchor_analysis_id == run.analysis_id
        assert run.baseline_analysis_id is None
        with (
            patch.object(
                analysis_module,
                "database_unix_microseconds",
                side_effect=AssertionError("natural replay requested DB time"),
            ),
            patch.object(
                connector,
                "execute",
                side_effect=AssertionError("natural replay attempted DML"),
            ),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("natural replay attempted DML"),
            ),
        ):
            replay = _begin(
                connector,
                gate,
                turn,
                build_id=build,
                analysis_id=b"Z" * 16,
                now=31,
            )
        assert replay.analysis_id == run.analysis_id
        assert replay.input_manifest_sha256 == run.input_manifest_sha256
        assert replay.replayed
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_analysis_runs WHERE build_id = %s",
            (build,),
        ) == (1,)

        _run_file_slice(connector, gate, turn, run.analysis_id)
        oracle = _independent_file_oracle(connector, build)
        resolved = {
            bytes(row[0]): (int(row[1]), int(row[2]), int(row[3]))
            for row in connector.fetch_all(
                "SELECT file_sha256, occurrence_count, artist_count, "
                "maximum_gallery_artist_count "
                "FROM catalog_analysis_file_hash_decision_resolved "
                "WHERE analysis_id = %s ORDER BY file_sha256",
                (run.analysis_id,),
            )
        }
        assert resolved == oracle == {first: (3, 3, 2), second: (1, 2, 2)}
        assert connector.fetch_one(
            "SELECT old_value.old_excluded, new_value.new_excluded "
            "FROM catalog_analysis_exclusion_delta_seals AS sealed "
            "JOIN catalog_analysis_exclusion_delta_old_excluded_flags AS old_value "
            "ON old_value.analysis_id = sealed.analysis_id "
            "AND old_value.file_sha256 = sealed.file_sha256 "
            "JOIN catalog_analysis_exclusion_delta_new_excluded_flags AS new_value "
            "ON new_value.analysis_id = sealed.analysis_id "
            "AND new_value.file_sha256 = sealed.file_sha256 "
            "WHERE sealed.analysis_id = %s AND sealed.file_sha256 = %s",
            (run.analysis_id, first),
        ) == (0, 1)

        with pytest.raises(AnalysisNotReadyError, match="five components"):
            with connector.transaction():
                AnalysisRepository.complete(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    now=500,
                )
        assert connector.fetch_one(
            "SELECT state, completed_at FROM catalog_analysis_runs "
            "WHERE analysis_id = %s",
            (run.analysis_id,),
        ) == ("OPEN", None)
    finally:
        connector.close()


def test_first_no_head_build_after_empty_ingest_turn_is_genesis(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-empty-turn-genesis.sqlite3")
    try:
        gate, empty_turn = _authorities(connector)
        with connector.transaction():
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                empty_turn,
                now=12,
            )
        with connector.transaction():
            first_build_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"j" * 16,
                now=13,
                lease_duration=1_000_000,
            )
            _scope, build, _first, _second = _seed_initial_snapshot(
                connector,
                generation=first_build_turn.generation,
            )

        assert first_build_turn.generation == 2
        run = _begin(
            connector,
            gate,
            first_build_turn,
            build_id=build,
            analysis_id=b"G" * 16,
            now=30,
        )
        assert run.baseline_analysis_id is None
        assert run.overlay_depth == 0
        assert not run.replayed
    finally:
        connector.close()


def test_every_batch_crash_rolls_back_receipt_checkpoint_and_seal_then_replays(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-crash.sqlite3")
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            _scope, build, _first, _second = _seed_initial_snapshot(connector)
        run = _begin(
            connector,
            gate,
            turn,
            build_id=build,
            analysis_id=b"C" * 16,
            now=30,
        )
        stages = (
            (AnalysisRepository.process_changed_gallery_batch, b"crash-gallery"),
            (AnalysisRepository.process_changed_file_hash_batch, b"crash-hash"),
            (AnalysisRepository.process_file_hash_decision_batch, b"crash-decision"),
            (AnalysisRepository.validate_file_hash_decision_batch, b"crash-validate"),
        )
        for index, (method, batch_key) in enumerate(stages):
            receipt_count = connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_analysis_batch_receipts "
                "WHERE analysis_id = %s",
                (run.analysis_id,),
            )[0]
            with pytest.raises(RuntimeError, match="injected crash"):
                with connector.transaction():
                    method(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        analysis_id=run.analysis_id,
                        batch_key=batch_key,
                        max_rows=128,
                        now=100 + index,
                    )
                    raise RuntimeError("injected crash")
            assert (
                connector.fetch_one(
                    "SELECT COUNT(*) FROM catalog_analysis_batch_receipts "
                    "WHERE analysis_id = %s",
                    (run.analysis_id,),
                )[0]
                == receipt_count
            )
            with connector.transaction():
                committed = method(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=batch_key,
                    max_rows=128,
                    now=200 + index,
                )
            assert committed.next_state == "OPEN"
            before = connector.fetch_one(
                "SELECT COUNT(*) FROM catalog_analysis_batch_receipts "
                "WHERE analysis_id = %s",
                (run.analysis_id,),
            )[0]
            with connector.transaction():
                replay = method(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=batch_key,
                    max_rows=128,
                    now=300 + index,
                )
            assert replay.replayed
            assert replay == type(replay)(
                analysis_id=committed.analysis_id,
                stage=committed.stage,
                batch_key=committed.batch_key,
                start_generation=committed.start_generation,
                start_cursor=committed.start_cursor,
                start_processed_count=committed.start_processed_count,
                page_limit=committed.page_limit,
                next_cursor=committed.next_cursor,
                next_processed_count=committed.next_processed_count,
                next_state=committed.next_state,
                row_count=committed.row_count,
                terminal=committed.terminal,
                committed_generation=committed.committed_generation,
                committed_at=committed.committed_at,
                replayed=True,
                component_sealed=committed.component_sealed,
            )
            assert (
                connector.fetch_one(
                    "SELECT COUNT(*) FROM catalog_analysis_batch_receipts "
                    "WHERE analysis_id = %s",
                    (run.analysis_id,),
                )[0]
                == before
            )
            with connector.transaction():
                terminal = method(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=batch_key + b"-terminal",
                    max_rows=128,
                    now=400 + index,
                )
            assert terminal.next_state == "COMPLETE"
            assert terminal.terminal and terminal.row_count == 0
        assert connector.fetch_one(
            "SELECT row_count FROM catalog_analysis_state_component_seals "
            "WHERE analysis_id = %s AND state_component = %s",
            (run.analysis_id, b"file_hash_decision"),
        ) == (2,)
    finally:
        connector.close()


def test_atomic_receipt_and_checkpoint_every_statement_fault_rolls_back(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-atomic-fault.sqlite3")
    receipt_table = "catalog_analysis_batch_receipt_stored"
    checkpoint_table = "catalog_analysis_checkpoints"
    mutation_tables = (receipt_table, checkpoint_table)
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            _scope, build, _first, _second = _seed_initial_snapshot(connector)
        run = _begin(
            connector,
            gate,
            turn,
            build_id=build,
            analysis_id=b"J" * 16,
            now=30,
        )
        with connector.transaction():
            first = AnalysisRepository.process_changed_gallery_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                batch_key=b"vertical-first",
                max_rows=128,
                now=100,
            )
        assert not first.terminal
        checkpoint_key = (run.analysis_id, b"changed_gallery")
        before_checkpoint = connector.fetch_one(
            "SELECT generation, cursor, processed_count, state, updated_at "
            "FROM catalog_analysis_checkpoints "
            "WHERE analysis_id = %s AND stage = %s",
            checkpoint_key,
        )
        before_receipt_count = connector.fetch_one(
            f"SELECT COUNT(*) FROM {receipt_table} "
            "WHERE analysis_id = %s AND stage = %s",
            checkpoint_key,
        )[0]
        original_execute = connector.execute
        original_execute_affected = connector.execute_affected

        for failure_at in range(1, 4):
            mutation_number = 0

            def maybe_fail(query: str, data: tuple[Any, ...] = ()) -> None:
                nonlocal mutation_number
                if any(table in query for table in mutation_tables):
                    mutation_number += 1
                    if mutation_number == failure_at:
                        raise RuntimeError(f"analysis atomic mutation {failure_at}")
                original_execute(query, data)

            def maybe_fail_affected(
                query: str,
                data: tuple[Any, ...] = (),
            ) -> int:
                nonlocal mutation_number
                if any(table in query for table in mutation_tables):
                    mutation_number += 1
                    if mutation_number == failure_at:
                        raise RuntimeError(f"analysis atomic mutation {failure_at}")
                return original_execute_affected(query, data)

            with pytest.raises(
                RuntimeError,
                match=f"analysis atomic mutation {failure_at}",
            ):
                with connector.transaction():
                    with (
                        patch.object(connector, "execute", side_effect=maybe_fail),
                        patch.object(
                            connector,
                            "execute_affected",
                            side_effect=maybe_fail_affected,
                        ),
                    ):
                        AnalysisRepository.process_changed_gallery_batch(
                            VNextUnitOfWork(connector, backend="sqlite"),
                            gate_lease=gate,
                            ingest_turn=turn,
                            analysis_id=run.analysis_id,
                            batch_key=b"vertical-terminal",
                            max_rows=128,
                            now=200,
                        )
            assert mutation_number == failure_at
            assert (
                connector.fetch_one(
                    "SELECT generation, cursor, processed_count, state, updated_at "
                    "FROM catalog_analysis_checkpoints "
                    "WHERE analysis_id = %s AND stage = %s",
                    checkpoint_key,
                )
                == before_checkpoint
            )
            assert connector.fetch_one(
                f"SELECT COUNT(*) FROM {receipt_table} "
                "WHERE analysis_id = %s AND stage = %s",
                checkpoint_key,
            ) == (before_receipt_count,)

        with connector.transaction():
            terminal = AnalysisRepository.process_changed_gallery_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                batch_key=b"vertical-terminal",
                max_rows=128,
                now=200,
            )
        assert terminal.terminal and terminal.next_state == "COMPLETE"
        with connector.transaction():
            replay = AnalysisRepository.process_changed_gallery_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                batch_key=b"vertical-terminal",
                max_rows=128,
                now=300,
            )
        assert replay.replayed
    finally:
        connector.close()


def _complete_baseline_for_incremental(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
    analysis_id: bytes,
) -> None:
    snapshot = b"m" * 32
    with connector.transaction():
        started_at = connector.fetch_one(
            "SELECT started_at FROM catalog_analysis_run_descriptor "
            "WHERE analysis_id = %s",
            (analysis_id,),
        )[0]
        seal_time = int(started_at) + 1
        for component in sorted(ANALYSIS_COMPONENTS - {b"file_hash_decision"}):
            seed_analysis_component(
                connector,
                analysis_id=analysis_id,
                state_component=component,
                row_count=0,
                sealed_at=seal_time,
                terminal_receipt=True,
            )
        _canonical_identity(
            connector,
            snapshot,
            domain=b"source_snapshot_manifest_v1",
            serial=900,
        )
        seed_snapshot_manifest(
            connector,
            snapshot_manifest_sha256=snapshot,
            gallery_count=2,
            file_count=0,
            byte_count=0,
        )
        connector.execute(
            "INSERT INTO catalog_analysis_snapshot_manifest "
            "(analysis_id, snapshot_manifest_sha256) VALUES (%s, %s)",
            (analysis_id, snapshot),
        )
        complete_analysis_run(
            connector,
            analysis_id=analysis_id,
            completed_at=seal_time + 1,
        )


def _prepare_incremental(
    connector: SQLiteConnector,
    gate: GateLease,
    turn: IngestTurn,
) -> tuple[IngestTurn, bytes, bytes, bytes, bytes]:
    with connector.transaction():
        scope, first_build, first, removed = _seed_initial_snapshot(connector)
    baseline = _begin(
        connector,
        gate,
        turn,
        build_id=first_build,
        analysis_id=b"B" * 16,
        now=30,
    )
    _run_file_slice(connector, gate, turn, baseline.analysis_id, max_rows=128)
    _complete_baseline_for_incremental(connector, gate, turn, baseline.analysis_id)

    snapshot = b"m" * 32
    with connector.transaction():
        base_receipt = _seed_published_commit(
            connector,
            build_id=first_build,
            snapshot_manifest_sha256=snapshot,
            generation=1,
            committed_at=510,
            analysis_id=baseline.analysis_id,
        )
        IngestFenceRepository.complete(
            VNextUnitOfWork(connector, backend="sqlite"),
            turn,
            now=520,
        )
    with connector.transaction():
        second_turn = IngestFenceRepository.claim(
            VNextUnitOfWork(connector, backend="sqlite"),
            owner_token=b"j" * 16,
            now=521,
            lease_duration=1_000_000,
        )

    second_tree = b"u" * 32
    second_build = _source_build_id(
        connector,
        scope=scope,
        manifest_sha256=bytes((2,)) * 32,
        gallery_count=2,
    )
    added = b"c" * 32
    with connector.transaction():
        _seed_build(
            connector,
            build_id=second_build,
            scope=scope,
            manifest_byte=2,
            gallery_count=2,
            base_receipt=base_receipt,
            created_at=522,
            sealed_at=523,
            discovery_tree_observation_sha256=second_tree,
        )
        connector.execute(
            "INSERT INTO catalog_source_build_expected_gallery "
            "(build_id, position, gallery_id) VALUES (%s, 0, 1)",
            (second_build,),
        )
        connector.execute(
            "INSERT INTO catalog_source_build_galleries "
            "(build_id, gallery_id, observation_id) VALUES (%s, 1, 1)",
            (second_build,),
        )
        _seed_gallery(
            connector,
            build_id=second_build,
            scope=scope,
            gallery_id=2,
            observation_id=2,
            occurrences=((first, 1), (added, 2)),
            artists=(2, 3),
            serial=102,
        )
        _map_working_build(
            connector,
            build_id=second_build,
            generation=second_turn.generation,
            replace=True,
        )
    return second_turn, second_build, first, removed, added


def test_incremental_overlay_has_exact_changed_shadow_tombstone_and_full_resolution(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-incremental.sqlite3")
    try:
        gate, first_turn = _authorities(connector)
        turn, build, unchanged, removed, added = _prepare_incremental(
            connector, gate, first_turn
        )
        run = _begin(
            connector,
            gate,
            turn,
            build_id=build,
            analysis_id=b"I" * 16,
            now=530,
        )
        assert run.overlay_depth == 1
        assert run.baseline_analysis_id == b"B" * 16
        assert connector.fetch_all(
            "SELECT ancestor_depth, ancestor_analysis_id "
            "FROM catalog_analysis_state_ancestry WHERE analysis_id = %s "
            "ORDER BY ancestor_depth",
            (run.analysis_id,),
        ) == [(0, run.analysis_id), (1, b"B" * 16)]
        _run_file_slice(
            connector,
            gate,
            turn,
            run.analysis_id,
            max_rows=1,
            start_now=600,
        )

        assert connector.fetch_all(
            "SELECT gallery_id, change_kind FROM catalog_analysis_changed_galleries "
            "WHERE analysis_id = %s ORDER BY gallery_id",
            (run.analysis_id,),
        ) == [(2, "REPLACED")]
        assert {
            bytes(row[0])
            for row in connector.fetch_all(
                "SELECT file_sha256 FROM catalog_analysis_changed_file_hashes "
                "WHERE analysis_id = %s",
                (run.analysis_id,),
            )
        } == {unchanged, removed, added}
        assert connector.fetch_all(
            "SELECT file_sha256 FROM catalog_analysis_file_hash_decision_shadow "
            "WHERE analysis_id = %s ORDER BY file_sha256",
            (run.analysis_id,),
        ) == [(added,)]
        assert connector.fetch_all(
            "SELECT file_sha256 FROM catalog_analysis_file_hash_decision_tombstone "
            "WHERE analysis_id = %s ORDER BY file_sha256",
            (run.analysis_id,),
        ) == [(removed,)]
        oracle = _independent_file_oracle(connector, build)
        resolved = {
            bytes(row[0]): (int(row[1]), int(row[2]), int(row[3]))
            for row in connector.fetch_all(
                "SELECT file_sha256, occurrence_count, artist_count, "
                "maximum_gallery_artist_count "
                "FROM catalog_analysis_file_hash_decision_resolved "
                "WHERE analysis_id = %s ORDER BY file_sha256",
                (run.analysis_id,),
            )
        }
        assert resolved == oracle
    finally:
        connector.close()


@pytest.mark.parametrize("corruption", ["omitted", "extra", "field"])
def test_independent_seal_rejects_omitted_extra_or_corrupt_overlay(
    tmp_path: Path,
    corruption: str,
) -> None:
    connector = _generated_database(tmp_path / f"analysis-{corruption}.sqlite3")
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            _scope, build, first, _second = _seed_initial_snapshot(connector)
        run = _begin(
            connector,
            gate,
            turn,
            build_id=build,
            analysis_id=b"X" * 16,
            now=30,
        )
        _run_stage(
            connector,
            gate,
            turn,
            AnalysisRepository.process_changed_gallery_batch,
            analysis_id=run.analysis_id,
            prefix=b"g",
            max_rows=128,
        )
        _run_stage(
            connector,
            gate,
            turn,
            AnalysisRepository.process_changed_file_hash_batch,
            analysis_id=run.analysis_id,
            prefix=b"h",
            max_rows=128,
        )
        _run_stage(
            connector,
            gate,
            turn,
            AnalysisRepository.process_file_hash_decision_batch,
            analysis_id=run.analysis_id,
            prefix=b"d",
            max_rows=128,
        )
        with connector.transaction():
            if corruption == "omitted":
                connector.execute(
                    "DELETE FROM catalog_a_file_decision_shadow_seals "
                    "WHERE analysis_id = %s AND file_sha256 = %s",
                    (run.analysis_id, first),
                )
            elif corruption == "extra":
                extra = b"z" * 32
                connector.execute(
                    "INSERT INTO catalog_content_blobs (file_sha256, size_bytes) "
                    "VALUES (%s, 1)",
                    (extra,),
                )
                connector.execute(
                    "INSERT INTO catalog_analysis_file_hash_decision_tombstone "
                    "(analysis_id, file_sha256) VALUES (%s, %s)",
                    (run.analysis_id, extra),
                )
            else:
                connector.execute(
                    "UPDATE catalog_a_file_decision_shadow_occurrences "
                    "SET occurrence_count = occurrence_count + 1 "
                    "WHERE analysis_id = %s AND file_sha256 = %s",
                    (run.analysis_id, first),
                )
        with pytest.raises(AnalysisCorruptionError, match="partial|full evaluator"):
            with connector.transaction():
                AnalysisRepository.validate_file_hash_decision_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=b"validate-corrupt",
                    max_rows=128,
                    now=500,
                )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_analysis_state_component_seals "
            "WHERE analysis_id = %s",
            (run.analysis_id,),
        ) == (0,)
    finally:
        connector.close()


def test_completed_incremental_replay_survives_safe_base_and_candidate_compaction(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-incremental-replay.sqlite3")
    try:
        gate, first_turn = _authorities(connector)
        second_turn, build, _first, _removed, _added = _prepare_incremental(
            connector, gate, first_turn
        )
        run = _begin(
            connector,
            gate,
            second_turn,
            build_id=build,
            analysis_id=b"I" * 16,
            now=530,
        )
        _run_file_slice(
            connector,
            gate,
            second_turn,
            run.analysis_id,
            max_rows=128,
            start_now=600,
        )
        with connector.transaction():
            started_at = int(
                connector.fetch_one(
                    "SELECT started_at FROM catalog_analysis_run_descriptor "
                    "WHERE analysis_id = %s",
                    (run.analysis_id,),
                )[0]
            )
            for component in sorted(ANALYSIS_COMPONENTS - {b"file_hash_decision"}):
                seed_analysis_component(
                    connector,
                    analysis_id=run.analysis_id,
                    state_component=component,
                    row_count=0,
                    sealed_at=started_at + 1,
                    terminal_receipt=True,
                )
            connector.execute(
                "INSERT INTO catalog_analysis_snapshot_manifest "
                "(analysis_id, snapshot_manifest_sha256) VALUES (%s, %s)",
                (run.analysis_id, b"m" * 32),
            )
            complete_analysis_run(
                connector,
                analysis_id=run.analysis_id,
                completed_at=started_at + 2,
            )
            receipt_id = _seed_published_commit(
                connector,
                build_id=build,
                snapshot_manifest_sha256=b"m" * 32,
                generation=second_turn.generation,
                committed_at=700,
                analysis_id=run.analysis_id,
            )
            seed_publication_finalization(
                connector,
                receipt_id=receipt_id,
                cursor=b"",
                processed_count=0,
                finalized_at=701,
            )
            assert (
                connector.execute_affected(
                    "DELETE FROM catalog_source_build_base_publication_commits "
                    "WHERE build_id = %s",
                    (build,),
                )
                == 1
            )
            candidate_id = connector.fetch_one(
                "SELECT candidate_id FROM catalog_publication_commits "
                "WHERE receipt_id = %s",
                (receipt_id,),
            )[0]
            assert (
                connector.execute_affected(
                    "DELETE FROM catalog_publication_candidates "
                    "WHERE candidate_id = %s",
                    (candidate_id,),
                )
                == 1
            )
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                second_turn,
                now=702,
            )
        with connector.transaction():
            fresh_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"k" * 16,
                now=703,
                lease_duration=1_000_000,
            )
            _map_working_build(
                connector,
                build_id=build,
                generation=fresh_turn.generation,
                replace=True,
            )

        with (
            patch.object(
                analysis_module,
                "database_unix_microseconds",
                side_effect=AssertionError("completed replay requested DB time"),
            ),
            patch.object(
                connector,
                "execute",
                side_effect=AssertionError("completed replay attempted DML"),
            ),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("completed replay attempted DML"),
            ),
        ):
            replay = _begin(
                connector,
                gate,
                fresh_turn,
                build_id=build,
                analysis_id=b"R" * 16,
                now=704,
            )

        assert replay.analysis_id == run.analysis_id
        assert replay.build_id == run.build_id
        assert replay.policy_id == run.policy_id
        assert replay.input_manifest_sha256 == run.input_manifest_sha256
        assert replay.baseline_analysis_id == run.baseline_analysis_id
        assert replay.anchor_analysis_id == run.anchor_analysis_id
        assert replay.overlay_depth == run.overlay_depth == 1
        assert replay.state == "COMPLETE"
        assert replay.replayed
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_analysis_runs WHERE build_id = %s",
            (build,),
        ) == (1,)
    finally:
        connector.close()


def test_new_analysis_rejects_incremental_build_with_lost_pinned_base(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-new-lost-build-base.sqlite3")
    try:
        gate, first_turn = _authorities(connector)
        second_turn, build, _first, _removed, _added = _prepare_incremental(
            connector,
            gate,
            first_turn,
        )
        assert (
            connector.execute_affected(
                "DELETE FROM catalog_source_build_base_publication_commits "
                "WHERE build_id = %s",
                (build,),
            )
            == 1
        )

        with pytest.raises(
            AnalysisCorruptionError,
            match="lost its pinned publication baseline",
        ):
            _begin(
                connector,
                gate,
                second_turn,
                build_id=build,
                analysis_id=b"N" * 16,
                now=530,
            )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_analysis_runs WHERE analysis_id = %s",
            (b"N" * 16,),
        ) == (0,)
    finally:
        connector.close()


def test_lost_base_fails_from_identity_without_historical_provenance_scan(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-lost-base-provenance.sqlite3")
    try:
        gate, first_turn = _authorities(connector)
        second_turn, build, _first, _removed, _added = _prepare_incremental(
            connector,
            gate,
            first_turn,
        )
        with connector.transaction():
            assert (
                connector.execute_affected(
                    "DELETE FROM catalog_source_build_base_publication_commits "
                    "WHERE build_id = %s",
                    (build,),
                )
                == 1
            )
            assert (
                connector.execute_affected(
                    "DELETE FROM catalog_source_revision_provenance "
                    "WHERE source_revision = 1"
                )
                == 1
            )

        with pytest.raises(
            AnalysisCorruptionError,
            match="lost its pinned publication baseline",
        ):
            _begin(
                connector,
                gate,
                second_turn,
                build_id=build,
                analysis_id=b"P" * 16,
                now=530,
            )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_analysis_runs WHERE analysis_id = %s",
            (b"P" * 16,),
        ) == (0,)
    finally:
        connector.close()


def test_existing_analysis_replay_rejects_lost_build_pinned_base(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-lost-build-base.sqlite3")
    try:
        gate, first_turn = _authorities(connector)
        second_turn, build, _first, _removed, _added = _prepare_incremental(
            connector,
            gate,
            first_turn,
        )
        run = _begin(
            connector,
            gate,
            second_turn,
            build_id=build,
            analysis_id=b"I" * 16,
            now=530,
        )
        assert run.baseline_analysis_id == b"B" * 16
        assert (
            connector.execute_affected(
                "DELETE FROM catalog_source_build_base_publication_commits "
                "WHERE build_id = %s",
                (build,),
            )
            == 1
        )

        with pytest.raises(
            AnalysisCorruptionError,
            match="lost its pinned publication baseline",
        ):
            _begin(
                connector,
                gate,
                second_turn,
                build_id=build,
                analysis_id=b"R" * 16,
                now=531,
            )
    finally:
        connector.close()


def test_existing_analysis_replay_rejects_base_candidate_lineage_tamper(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-base-candidate-tamper.sqlite3")
    try:
        gate, first_turn = _authorities(connector)
        second_turn, build, _first, _removed, _added = _prepare_incremental(
            connector,
            gate,
            first_turn,
        )
        run = _begin(
            connector,
            gate,
            second_turn,
            build_id=build,
            analysis_id=b"I" * 16,
            now=530,
        )
        assert run.baseline_analysis_id == b"B" * 16
        candidate = connector.fetch_one(
            "SELECT committed.candidate_id "
            "FROM catalog_source_build_base_publication_commits AS base "
            "JOIN catalog_publication_commits AS committed "
            "ON committed.receipt_id = base.base_receipt_id "
            "WHERE base.build_id = %s",
            (build,),
        )
        assert len(candidate) == 1
        assert (
            connector.execute_affected(
                "UPDATE catalog_publication_candidates "
                "SET analysis_id = %s WHERE candidate_id = %s",
                (run.analysis_id, candidate[0]),
            )
            == 1
        )

        with pytest.raises(
            AnalysisCorruptionError,
            match="candidate|provenance|lineage",
        ):
            _begin(
                connector,
                gate,
                second_turn,
                build_id=build,
                analysis_id=b"R" * 16,
                now=531,
            )
    finally:
        connector.close()


def test_arbitrary_noncanonical_incremental_build_is_rejected_without_write(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-arbitrary-id.sqlite3")
    try:
        gate, first_turn = _authorities(connector)
        second_turn, valid_build, _first, _removed, _added = _prepare_incremental(
            connector,
            gate,
            first_turn,
        )
        scope = connector.fetch_one(
            "SELECT scope_key FROM catalog_source_builds WHERE build_id = %s",
            (valid_build,),
        )[0]
        base_receipt = connector.fetch_one(
            "SELECT base_receipt_id "
            "FROM catalog_source_build_base_publication_commits "
            "WHERE build_id = %s",
            (valid_build,),
        )[0]
        arbitrary_build = b"@" * 16
        discovery_tree = b"v" * 32
        assert arbitrary_build != _source_build_id(
            connector,
            scope=scope,
            manifest_sha256=bytes((8,)) * 32,
            gallery_count=1,
        )
        with connector.transaction():
            _seed_build(
                connector,
                build_id=arbitrary_build,
                scope=scope,
                manifest_byte=8,
                gallery_count=1,
                base_receipt=base_receipt,
                created_at=522,
                sealed_at=523,
                discovery_tree_observation_sha256=discovery_tree,
            )
            assert (
                connector.execute_affected(
                    "UPDATE operational_source_build_generations "
                    "SET build_id = %s WHERE build_id = %s",
                    (arbitrary_build, valid_build),
                )
                == 1
            )
            assert (
                connector.execute_affected(
                    "UPDATE operational_source_working_builds "
                    "SET build_id = %s WHERE build_id = %s",
                    (arbitrary_build, valid_build),
                )
                == 1
            )

        with pytest.raises(
            AnalysisCorruptionError,
            match="identity",
        ):
            _begin(
                connector,
                gate,
                second_turn,
                build_id=arbitrary_build,
                analysis_id=b"X" * 16,
                now=530,
            )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_analysis_runs WHERE analysis_id = %s",
            (b"X" * 16,),
        ) == (0,)
    finally:
        connector.close()


def test_v3_base_free_analysis_begin_and_natural_replay(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-v3-base-free.sqlite3")
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            scope = _seed_root(connector)
            source_root = connector.fetch_one(
                "SELECT source_root_sha256 FROM catalog_source_scopes "
                "WHERE scope_key = %s",
                (scope,),
            )[0]
            summary = SourceBuildManifestSummary.empty()
            attempt = source_build_snapshot_attempt_id(source_root, summary)
            build = source_build_recovery_identity(
                snapshot_attempt_id=attempt,
                scope=scope,
                manifest_policy_id=1,
                created_at=20,
            )
            _seed_build(
                connector,
                build_id=build,
                scope=scope,
                manifest_byte=0,
                gallery_count=0,
                manifest_sha256=summary.manifest_sha256,
            )
            _map_working_build(
                connector,
                build_id=build,
                generation=turn.generation,
            )

        run = _begin(
            connector,
            gate,
            turn,
            build_id=build,
            analysis_id=b"V" * 16,
            now=30,
        )
        assert run.baseline_analysis_id is None
        replay = _begin(
            connector,
            gate,
            turn,
            build_id=build,
            analysis_id=b"R" * 16,
            now=31,
        )
        assert replay.replayed and replay.analysis_id == run.analysis_id
    finally:
        connector.close()


def test_v3_incremental_analysis_rejects_creation_time_and_base_tamper(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-v3-tamper.sqlite3")
    try:
        gate, first_turn = _authorities(connector)
        second_turn, canonical_build, _first, _removed, _added = _prepare_incremental(
            connector,
            gate,
            first_turn,
        )
        scope, manifest_policy_id = connector.fetch_one(
            "SELECT scope_key, manifest_policy_id FROM catalog_source_builds "
            "WHERE build_id = %s",
            (canonical_build,),
        )
        source_root = connector.fetch_one(
            "SELECT source_root_sha256 FROM catalog_source_scopes WHERE scope_key = %s",
            (scope,),
        )[0]
        base_receipt = connector.fetch_one(
            "SELECT base_receipt_id "
            "FROM catalog_source_build_base_publication_commits "
            "WHERE build_id = %s",
            (canonical_build,),
        )[0]
        summary = SourceBuildManifestSummary(bytes((9,)) * 32, 1, 0, 0)
        created_at = 522
        recovery = source_build_recovery_identity(
            snapshot_attempt_id=source_build_snapshot_attempt_id(
                source_root,
                summary,
            ),
            scope=scope,
            manifest_policy_id=manifest_policy_id,
            created_at=created_at,
        )
        with connector.transaction():
            _seed_build(
                connector,
                build_id=recovery,
                scope=scope,
                manifest_byte=9,
                gallery_count=1,
                base_receipt=base_receipt,
                created_at=created_at,
                sealed_at=created_at + 2,
            )
            assert (
                connector.execute_affected(
                    "UPDATE operational_source_build_generations "
                    "SET build_id = %s WHERE build_id = %s",
                    (recovery, canonical_build),
                )
                == 1
            )
            assert (
                connector.execute_affected(
                    "UPDATE operational_source_working_builds "
                    "SET build_id = %s, assigned_at = %s WHERE build_id = %s",
                    (recovery, created_at, canonical_build),
                )
                == 1
            )

        run = _begin(
            connector,
            gate,
            second_turn,
            build_id=recovery,
            analysis_id=b"V" * 16,
            now=530,
        )
        assert run.baseline_analysis_id == b"B" * 16
        assert _begin(
            connector,
            gate,
            second_turn,
            build_id=recovery,
            analysis_id=b"R" * 16,
            now=531,
        ).replayed
        run_count = connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_analysis_run_descriptor"
        )

        assert (
            connector.execute_affected(
                "UPDATE catalog_source_build_descriptor SET created_at = %s "
                "WHERE build_id = %s",
                (created_at + 1, recovery),
            )
            == 1
        )
        with pytest.raises(AnalysisCorruptionError, match="identity"):
            _begin(
                connector,
                gate,
                second_turn,
                build_id=recovery,
                analysis_id=b"X" * 16,
                now=532,
            )
        assert (
            connector.fetch_one("SELECT COUNT(*) FROM catalog_analysis_run_descriptor")
            == run_count
        )

        connector.execute(
            "UPDATE catalog_source_build_descriptor SET created_at = %s "
            "WHERE build_id = %s",
            (created_at, recovery),
        )
        assert (
            connector.execute_affected(
                "DELETE FROM catalog_source_build_base_publication_commits "
                "WHERE build_id = %s",
                (recovery,),
            )
            == 1
        )
        with pytest.raises(AnalysisCorruptionError, match="identity|baseline"):
            _begin(
                connector,
                gate,
                second_turn,
                build_id=recovery,
                analysis_id=b"Y" * 16,
                now=533,
            )
        assert (
            connector.fetch_one("SELECT COUNT(*) FROM catalog_analysis_run_descriptor")
            == run_count
        )
    finally:
        connector.close()


def test_v2_successor_rejects_substituted_complete_publication_base(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-v2-base-substitution.sqlite3")
    try:
        gate, first_turn = _authorities(connector)
        second_turn, canonical_build, _first, _removed, _added = _prepare_incremental(
            connector,
            gate,
            first_turn,
        )
        scope, manifest_policy_id = connector.fetch_one(
            "SELECT scope_key, manifest_policy_id FROM catalog_source_builds "
            "WHERE build_id = %s",
            (canonical_build,),
        )
        source_root = connector.fetch_one(
            "SELECT source_root_sha256 FROM catalog_source_scopes WHERE scope_key = %s",
            (scope,),
        )[0]
        first_receipt = connector.fetch_one(
            "SELECT base_receipt_id "
            "FROM catalog_source_build_base_publication_commits "
            "WHERE build_id = %s",
            (canonical_build,),
        )[0]
        manifest_summary = SourceBuildManifestSummary(
            bytes((9,)) * 32,
            1,
            0,
            0,
        )
        snapshot_attempt = source_build_snapshot_attempt_id(
            source_root,
            manifest_summary,
        )
        successor = source_build_identity(
            snapshot_attempt_id=snapshot_attempt,
            scope=scope,
            manifest_policy_id=manifest_policy_id,
        )
        with connector.transaction():
            _seed_build(
                connector,
                build_id=successor,
                scope=scope,
                manifest_byte=9,
                gallery_count=1,
                base_receipt=first_receipt,
                created_at=522,
                sealed_at=523,
            )

        assert analysis_module._derive_pinned_baseline(
            VNextUnitOfWork(connector, backend="sqlite"),
            build_id=successor,
        )[:3] == (b"B" * 16, 1, 1)

        replacement_run = _begin(
            connector,
            gate,
            second_turn,
            build_id=canonical_build,
            analysis_id=b"L" * 16,
            now=530,
        )
        _run_file_slice(
            connector,
            gate,
            second_turn,
            replacement_run.analysis_id,
            max_rows=128,
            start_now=540,
        )
        with connector.transaction():
            started_at = int(
                connector.fetch_one(
                    "SELECT started_at FROM catalog_analysis_run_descriptor "
                    "WHERE analysis_id = %s",
                    (replacement_run.analysis_id,),
                )[0]
            )
            for component in sorted(ANALYSIS_COMPONENTS - {b"file_hash_decision"}):
                seed_analysis_component(
                    connector,
                    analysis_id=replacement_run.analysis_id,
                    state_component=component,
                    row_count=0,
                    sealed_at=started_at + 1,
                    terminal_receipt=True,
                )
            connector.execute(
                "INSERT INTO catalog_analysis_snapshot_manifest "
                "(analysis_id, snapshot_manifest_sha256) VALUES (%s, %s)",
                (replacement_run.analysis_id, b"m" * 32),
            )
            complete_analysis_run(
                connector,
                analysis_id=replacement_run.analysis_id,
                completed_at=started_at + 2,
            )
            replacement_receipt = _seed_published_commit(
                connector,
                build_id=canonical_build,
                snapshot_manifest_sha256=b"m" * 32,
                generation=second_turn.generation,
                committed_at=600,
                analysis_id=replacement_run.analysis_id,
            )
            assert (
                connector.execute_affected(
                    "UPDATE catalog_source_build_base_publication_commits "
                    "SET base_receipt_id = %s WHERE build_id = %s",
                    (replacement_receipt, successor),
                )
                == 1
            )
            assert (
                connector.execute_affected(
                    "UPDATE operational_source_build_generations "
                    "SET build_id = %s WHERE build_id = %s",
                    (successor, canonical_build),
                )
                == 1
            )
            assert (
                connector.execute_affected(
                    "UPDATE operational_source_working_builds "
                    "SET build_id = %s WHERE build_id = %s",
                    (successor, canonical_build),
                )
                == 1
            )

        assert connector.fetch_one(
            "SELECT candidate_analysis.analysis_id, candidate_analysis.build_id, "
            "candidate_analysis.state, provenance_analysis.analysis_id, "
            "provenance_analysis.build_id, provenance_analysis.state "
            "FROM catalog_publication_commits AS committed "
            "JOIN catalog_publication_candidates AS candidate "
            "ON candidate.candidate_id = committed.candidate_id "
            "JOIN catalog_analysis_runs AS candidate_analysis "
            "ON candidate_analysis.analysis_id = candidate.analysis_id "
            "JOIN catalog_source_revision_provenance AS provenance "
            "ON provenance.source_revision = committed.source_revision "
            "JOIN catalog_analysis_runs AS provenance_analysis "
            "ON provenance_analysis.analysis_id = provenance.analysis_id "
            "WHERE committed.receipt_id = %s",
            (replacement_receipt,),
        ) == (
            replacement_run.analysis_id,
            canonical_build,
            "COMPLETE",
            replacement_run.analysis_id,
            canonical_build,
            "COMPLETE",
        )

        with pytest.raises(
            AnalysisCorruptionError,
            match="identity|baseline|receipt",
        ):
            _begin(
                connector,
                gate,
                second_turn,
                build_id=successor,
                analysis_id=b"S" * 16,
                now=610,
            )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_analysis_runs WHERE analysis_id = %s",
            (b"S" * 16,),
        ) == (0,)
    finally:
        connector.close()


def test_stale_source_baseline_is_rejected_before_analysis_write(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-stale.sqlite3")
    try:
        gate, first_turn = _authorities(connector)
        turn, build, _first, _removed, _added = _prepare_incremental(
            connector, gate, first_turn
        )
        with connector.transaction():
            scope = connector.fetch_one(
                "SELECT scope_key FROM catalog_source_builds WHERE build_id = %s",
                (build,),
            )[0]
            stale_head_build = _source_build_id(
                connector,
                scope=scope,
                manifest_sha256=bytes((10,)) * 32,
                gallery_count=1,
            )
            _seed_build(
                connector,
                build_id=stale_head_build,
                scope=scope,
                manifest_byte=10,
                gallery_count=1,
                created_at=525,
                sealed_at=526,
            )
            stale_head_analysis = b"T" * 16
            seed_analysis_run(
                connector,
                analysis_id=stale_head_analysis,
                build_id=stale_head_build,
                policy_id=1,
                input_manifest_sha256=analysis_module._analysis_input_digest(
                    bytes((10,)) * 32,
                    (1, 0, 0),
                    analysis_module._Policy(1, 1, 1, 1, 1, 1),
                ),
                started_at=527,
                state="COMPLETE",
                completed_at=528,
            )
            _seed_published_commit(
                connector,
                build_id=stale_head_build,
                snapshot_manifest_sha256=b"m" * 32,
                generation=2,
                committed_at=529,
                analysis_id=stale_head_analysis,
            )
        with pytest.raises(AnalysisNotReadyError, match="stale"):
            _begin(
                connector,
                gate,
                turn,
                build_id=build,
                analysis_id=b"S" * 16,
                now=530,
            )
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_analysis_runs WHERE analysis_id = %s",
            (b"S" * 16,),
        ) == (0,)
    finally:
        connector.close()


def test_large_snapshot_batch_is_hard_capped_and_resume_is_keyset_bounded(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-large.sqlite3")
    try:
        gate, turn = _authorities(connector)
        with connector.transaction():
            scope = _seed_root(connector)
            build = _source_build_id(
                connector,
                scope=scope,
                manifest_sha256=bytes((8,)) * 32,
                gallery_count=130,
            )
            _seed_build(
                connector,
                build_id=build,
                scope=scope,
                manifest_byte=8,
                gallery_count=130,
            )
            for gallery_id in range(1, 131):
                _seed_gallery(
                    connector,
                    build_id=build,
                    scope=scope,
                    gallery_id=gallery_id,
                    observation_id=1,
                    occurrences=((gallery_id.to_bytes(32, "big"), 1),),
                    artists=(),
                    serial=2000 + gallery_id,
                )
            _map_working_build(connector, build_id=build, generation=1)
        run = _begin(
            connector,
            gate,
            turn,
            build_id=build,
            analysis_id=b"Q" * 16,
            now=30,
        )
        with connector.transaction():
            first = AnalysisRepository.process_changed_gallery_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                batch_key=b"large-first",
                max_rows=128,
                now=40,
            )
        assert first.row_count == first.next_processed_count == 128
        assert first.next_state == "OPEN"
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM catalog_analysis_changed_galleries "
            "WHERE analysis_id = %s",
            (run.analysis_id,),
        ) == (128,)
        with connector.transaction():
            second = AnalysisRepository.process_changed_gallery_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                batch_key=b"large-second",
                max_rows=128,
                now=41,
            )
        assert second.row_count == 2
        assert second.next_processed_count == 130
        assert second.next_state == "OPEN"
        with connector.transaction():
            terminal = AnalysisRepository.process_changed_gallery_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                batch_key=b"large-terminal",
                max_rows=128,
                now=42,
            )
        assert terminal.row_count == 0
        assert terminal.next_processed_count == 130
        assert terminal.next_state == "COMPLETE" and terminal.terminal
        with connector.transaction():
            clamped = AnalysisRepository.process_changed_file_hash_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                batch_key=b"too-large",
                max_rows=129,
                now=43,
            )
        assert clamped.page_limit == 128
        assert clamped.row_count <= clamped.page_limit
        with connector.transaction():
            with (
                patch.object(
                    connector,
                    "execute",
                    side_effect=AssertionError("batch replay attempted DML"),
                ),
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=AssertionError("batch replay attempted DML"),
                ),
            ):
                clamped_replay = AnalysisRepository.process_changed_file_hash_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=b"too-large",
                    max_rows=0,
                    now=44,
                )
        assert clamped_replay.replayed
        assert clamped_replay.page_limit == clamped.page_limit == 128
        assert clamped_replay.row_count == clamped.row_count
        with pytest.raises(AnalysisCorruptionError, match="stored-limit evaluator"):
            with connector.transaction():
                connector.execute(
                    "UPDATE catalog_analysis_batch_receipt_stored "
                    "SET page_limit = %s WHERE analysis_id = %s AND stage = %s "
                    "AND start_generation = %s",
                    (
                        1,
                        run.analysis_id,
                        b"changed_file_hash",
                        clamped.start_generation,
                    ),
                )
                AnalysisRepository.process_changed_file_hash_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=b"too-large",
                    max_rows=7,
                    now=45,
                )
        first_hash = connector.fetch_one(
            "SELECT file_sha256 FROM catalog_analysis_changed_file_hashes "
            "WHERE analysis_id = %s ORDER BY file_sha256 LIMIT 1",
            (run.analysis_id,),
        )[0]
        with pytest.raises(AnalysisCorruptionError, match="materialization"):
            with connector.transaction():
                connector.execute(
                    "DELETE FROM catalog_analysis_changed_file_hashes "
                    "WHERE analysis_id = %s AND file_sha256 = %s",
                    (run.analysis_id, first_hash),
                )
                AnalysisRepository.process_changed_file_hash_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=b"too-large",
                    max_rows=7,
                    now=45,
                )
        with pytest.raises(ValueError, match="max_rows"):
            with connector.transaction():
                AnalysisRepository.process_changed_file_hash_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=b"fresh-zero",
                    max_rows=0,
                    now=45,
                )
    finally:
        connector.close()


def test_depth_zero_all_five_components_snapshot_handoff_and_replay(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-all-components.sqlite3")
    snapshot_preparation = None
    try:
        gate, turn = _authorities(connector)
        file_sha256 = b"e" * 32
        with connector.transaction():
            scope = _seed_root(connector)
            build = _source_build_id(
                connector,
                scope=scope,
                manifest_sha256=bytes((6,)) * 32,
                gallery_count=1,
                file_count=1,
                byte_count=1,
            )
            _seed_build(
                connector,
                build_id=build,
                scope=scope,
                manifest_byte=6,
                gallery_count=1,
                file_count=1,
                byte_count=1,
            )
            _seed_gallery(
                connector,
                build_id=build,
                scope=scope,
                gallery_id=1,
                observation_id=1,
                occurrences=((file_sha256, 1),),
                artists=(),
                serial=600,
            )
            _seed_preparation_facts(
                connector,
                gallery_id=1,
                observation_id=1,
                file_sha256=file_sha256,
            )
            _map_working_build(connector, build_id=build, generation=1)
        run = _begin(
            connector,
            gate,
            turn,
            build_id=build,
            analysis_id=b"F" * 16,
            now=30,
        )
        _run_file_slice(
            connector,
            gate,
            turn,
            run.analysis_id,
            max_rows=128,
            start_now=100,
            replay_each=True,
        )
        _run_stage(
            connector,
            gate,
            turn,
            AnalysisRepository.process_impacted_gallery_batch,
            analysis_id=run.analysis_id,
            prefix=b"all-impact-gallery",
            max_rows=128,
            start_now=500,
            replay_each=True,
        )
        _run_prepared_gallery_stage(
            connector,
            gate,
            turn,
            run.analysis_id,
            AnalysisRepository.process_impacted_content_batch,
            gallery_ids=(1,),
            prefix=b"all-impact-content",
            start_now=600,
            upload_content=True,
            replay_first=True,
        )
        _run_prepared_gallery_stage(
            connector,
            gate,
            turn,
            run.analysis_id,
            AnalysisRepository.process_content_owner_candidate_batch,
            gallery_ids=(1,),
            prefix=b"all-content-candidate",
            start_now=700,
            replay_first=True,
        )
        _run_prepared_gallery_stage(
            connector,
            gate,
            turn,
            run.analysis_id,
            AnalysisRepository.validate_content_owner_candidate_batch,
            gallery_ids=(1,),
            prefix=b"all-validate-content-candidate",
            start_now=800,
            replay_first=True,
        )
        _run_stage(
            connector,
            gate,
            turn,
            AnalysisRepository.process_content_owner_batch,
            analysis_id=run.analysis_id,
            prefix=b"all-content-owner",
            max_rows=128,
            start_now=900,
            replay_each=True,
        )
        _run_stage(
            connector,
            gate,
            turn,
            AnalysisRepository.validate_content_owner_batch,
            analysis_id=run.analysis_id,
            prefix=b"all-validate-content-owner",
            max_rows=128,
            start_now=1_000,
            replay_each=True,
        )
        _run_stage(
            connector,
            gate,
            turn,
            AnalysisRepository.process_impacted_gid_batch,
            analysis_id=run.analysis_id,
            prefix=b"all-impact-gid",
            max_rows=128,
            start_now=1_100,
            replay_each=True,
        )
        _run_prepared_gallery_stage(
            connector,
            gate,
            turn,
            run.analysis_id,
            AnalysisRepository.process_gid_candidate_batch,
            gallery_ids=(1,),
            prefix=b"all-gid-candidate",
            start_now=1_200,
            replay_first=True,
        )
        _run_prepared_gallery_stage(
            connector,
            gate,
            turn,
            run.analysis_id,
            AnalysisRepository.validate_gid_candidate_batch,
            gallery_ids=(1,),
            prefix=b"all-validate-gid-candidate",
            start_now=1_300,
            replay_first=True,
        )
        _run_stage(
            connector,
            gate,
            turn,
            AnalysisRepository.process_gid_winner_batch,
            analysis_id=run.analysis_id,
            prefix=b"all-gid-winner",
            max_rows=128,
            start_now=1_400,
            replay_each=True,
        )
        final_validation = _run_stage(
            connector,
            gate,
            turn,
            AnalysisRepository.validate_gid_winner_batch,
            analysis_id=run.analysis_id,
            prefix=b"all-validate-gid-winner",
            max_rows=128,
            start_now=1_500,
            replay_each=True,
        )
        assert final_validation[-1].component_sealed
        assert connector.fetch_all(
            "SELECT state_component, row_count "
            "FROM catalog_analysis_state_component_seals "
            "WHERE analysis_id = %s ORDER BY state_component",
            (run.analysis_id,),
        ) == [
            (b"content_owner", 1),
            (b"content_owner_candidate", 1),
            (b"file_hash_decision", 1),
            (b"gid_candidate", 1),
            (b"gid_winner", 1),
        ]
        assert connector.fetch_all(
            "SELECT file_no FROM catalog_gallery_observation_file_file_nos "
            "WHERE gallery_id = 1 AND observation_id = 1 ORDER BY file_no"
        ) == [(0,)]
        assert connector.fetch_one(
            "SELECT content_sha256 "
            "FROM catalog_analysis_content_owner_candidate_resolved "
            "WHERE analysis_id = %s AND gallery_id = 1",
            (run.analysis_id,),
        ) == (identity.effective_content_digest((file_sha256,)),)

        snapshot_authority = _issue_preparation_authority(
            connector,
            gate,
            turn,
            run.analysis_id,
            now=1_600,
        )
        snapshot_preparation = AnalysisRepository.prepare_snapshot_manifest(
            connector,
            backend="sqlite",
            authority=snapshot_authority,
        )
        _put_canonical_plan(
            connector,
            gate,
            turn,
            snapshot_preparation.upload_plan,
            now=1_610,
        )
        with connector.transaction():
            first_handoff = AnalysisRepository.handoff_snapshot_manifest(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                preparation=snapshot_preparation,
                now=1_620,
            )
        persisted_completed_at = connector.fetch_one(
            "SELECT completed_at FROM catalog_analysis_run_completed_ats "
            "WHERE analysis_id = %s",
            (run.analysis_id,),
        )[0]
        snapshot_preparation.close()
        snapshot_preparation = None
        replay_authority = _issue_preparation_authority(
            connector,
            gate,
            turn,
            run.analysis_id,
            now=1_621,
        )
        snapshot_preparation = AnalysisRepository.prepare_snapshot_manifest(
            connector,
            backend="sqlite",
            authority=replay_authority,
        )
        with (
            patch.object(
                connector,
                "execute",
                side_effect=AssertionError("write"),
            ),
            patch.object(
                connector,
                "execute_affected",
                side_effect=AssertionError("write"),
            ),
            connector.transaction(),
        ):
            replayed_handoff = AnalysisRepository.handoff_snapshot_manifest(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                preparation=snapshot_preparation,
                now=1_622,
            )
        assert replayed_handoff == first_handoff
        connector.execute(
            "UPDATE catalog_source_snapshot_manifest_identity "
            "SET file_count = 2 WHERE snapshot_manifest_sha256 = %s",
            (first_handoff,),
        )
        corrupt_authority = _issue_preparation_authority(
            connector,
            gate,
            turn,
            run.analysis_id,
            now=1_623,
        )
        corrupt_preparation = AnalysisRepository.prepare_snapshot_manifest(
            connector,
            backend="sqlite",
            authority=corrupt_authority,
        )
        try:
            with (
                patch.object(
                    connector,
                    "execute",
                    side_effect=AssertionError("write"),
                ),
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=AssertionError("write"),
                ),
                connector.transaction(),
                pytest.raises(AnalysisCorruptionError, match="sealed count family"),
            ):
                AnalysisRepository.handoff_snapshot_manifest(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    preparation=corrupt_preparation,
                    now=1_624,
                )
        finally:
            corrupt_preparation.close()
        connector.execute(
            "UPDATE catalog_source_snapshot_manifest_identity "
            "SET file_count = 1 WHERE snapshot_manifest_sha256 = %s",
            (first_handoff,),
        )
        root_page_sha256 = connector.fetch_one(
            "SELECT root_page_sha256 FROM catalog_canonical_value_identities "
            "WHERE value_sha256 = %s",
            (first_handoff,),
        )[0]
        original_page_bytes = connector.fetch_one(
            "SELECT page_bytes FROM catalog_canonical_value_page_payloads "
            "WHERE page_sha256 = %s",
            (root_page_sha256,),
        )[0]
        connector.execute(
            "UPDATE catalog_canonical_value_page_payloads SET page_bytes = %s "
            "WHERE page_sha256 = %s",
            (b"corrupt-snapshot-canonical-page", root_page_sha256),
        )
        payload_authority = _issue_preparation_authority(
            connector,
            gate,
            turn,
            run.analysis_id,
            now=1_625,
        )
        payload_preparation = AnalysisRepository.prepare_snapshot_manifest(
            connector,
            backend="sqlite",
            authority=payload_authority,
        )
        try:
            with (
                patch.object(
                    connector,
                    "execute",
                    side_effect=AssertionError("write"),
                ),
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=AssertionError("write"),
                ),
                connector.transaction(),
                pytest.raises(AnalysisCorruptionError, match="canonical payload"),
            ):
                AnalysisRepository.handoff_snapshot_manifest(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    preparation=payload_preparation,
                    now=1_626,
                )
        finally:
            payload_preparation.close()
            connector.execute(
                "UPDATE catalog_canonical_value_page_payloads SET page_bytes = %s "
                "WHERE page_sha256 = %s",
                (original_page_bytes, root_page_sha256),
            )
        component = b"gid_winner"
        original_component_time = connector.fetch_one(
            "SELECT sealed_at FROM catalog_analysis_state_component_seals "
            "WHERE analysis_id = %s AND state_component = %s",
            (run.analysis_id, component),
        )[0]
        set_analysis_component_sealed_at(
            connector,
            analysis_id=run.analysis_id,
            state_component=component,
            sealed_at=persisted_completed_at + 1,
        )
        time_authority = _issue_preparation_authority(
            connector,
            gate,
            turn,
            run.analysis_id,
            now=1_627,
        )
        time_preparation = AnalysisRepository.prepare_snapshot_manifest(
            connector,
            backend="sqlite",
            authority=time_authority,
        )
        try:
            with (
                patch.object(
                    connector,
                    "execute",
                    side_effect=AssertionError("write"),
                ),
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=AssertionError("write"),
                ),
                connector.transaction(),
                pytest.raises(
                    AnalysisCorruptionError,
                    match="completed analysis snapshot replay",
                ),
            ):
                AnalysisRepository.handoff_snapshot_manifest(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    preparation=time_preparation,
                    now=1_628,
                )
        finally:
            time_preparation.close()
            set_analysis_component_sealed_at(
                connector,
                analysis_id=run.analysis_id,
                state_component=component,
                sealed_at=original_component_time,
            )
        state_and_time = connector.fetch_one(
            "SELECT state, completed_at FROM catalog_analysis_runs "
            "WHERE analysis_id = %s",
            (run.analysis_id,),
        )
        assert state_and_time == ("COMPLETE", persisted_completed_at)
        started_at = connector.fetch_one(
            "SELECT started_at FROM catalog_analysis_run_descriptor "
            "WHERE analysis_id = %s",
            (run.analysis_id,),
        )[0]
        maximum_component_time = connector.fetch_one(
            "SELECT MAX(sealed_at) FROM catalog_analysis_state_component_seals "
            "WHERE analysis_id = %s",
            (run.analysis_id,),
        )[0]
        assert persisted_completed_at >= started_at
        assert persisted_completed_at >= maximum_component_time
        assert persisted_completed_at != 1_620
        assert connector.fetch_one(
            "SELECT snapshot_manifest_sha256 "
            "FROM catalog_analysis_snapshot_manifest WHERE analysis_id = %s",
            (run.analysis_id,),
        ) == (first_handoff,)
        assert connector.fetch_one(
            "SELECT gallery_count, file_count, byte_count "
            "FROM catalog_source_snapshot_manifest_identity "
            "WHERE snapshot_manifest_sha256 = %s",
            (first_handoff,),
        ) == (1, 1, 1)
    finally:
        if snapshot_preparation is not None:
            snapshot_preparation.close()
        connector.close()


def test_depth_one_removed_gallery_materializes_all_downstream_tombstones(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-depth-one-removed.sqlite3")
    try:
        gate, first_turn = _authorities(connector)
        file_sha256 = b"u" * 32
        with connector.transaction():
            scope = _seed_root(connector)
            baseline_build = _source_build_id(
                connector,
                scope=scope,
                manifest_sha256=bytes((4,)) * 32,
                gallery_count=1,
                file_count=1,
                byte_count=1,
            )
            _seed_build(
                connector,
                build_id=baseline_build,
                scope=scope,
                manifest_byte=4,
                gallery_count=1,
                file_count=1,
                byte_count=1,
            )
            _seed_gallery(
                connector,
                build_id=baseline_build,
                scope=scope,
                gallery_id=1,
                observation_id=1,
                occurrences=((file_sha256, 1),),
                artists=(),
                serial=800,
            )
            _seed_preparation_facts(
                connector,
                gallery_id=1,
                observation_id=1,
                file_sha256=file_sha256,
            )
            _map_working_build(connector, build_id=baseline_build, generation=1)
        baseline = _begin(
            connector,
            gate,
            first_turn,
            build_id=baseline_build,
            analysis_id=b"V" * 16,
            now=30,
        )
        _run_file_slice(
            connector,
            gate,
            first_turn,
            baseline.analysis_id,
            max_rows=128,
            start_now=100,
        )
        _run_single_live_gallery_downstream(
            connector,
            gate,
            first_turn,
            baseline.analysis_id,
            prefix=b"removed-base",
            start_now=500,
        )
        baseline_snapshot = _prepare_upload_and_handoff_snapshot(
            connector,
            gate,
            first_turn,
            baseline.analysis_id,
            now=1_700,
        )
        with connector.transaction():
            base_receipt = _seed_published_commit(
                connector,
                build_id=baseline_build,
                snapshot_manifest_sha256=baseline_snapshot,
                generation=1,
                committed_at=1_710,
                analysis_id=baseline.analysis_id,
            )
            IngestFenceRepository.complete(
                VNextUnitOfWork(connector, backend="sqlite"),
                first_turn,
                now=1_711,
            )
        with connector.transaction():
            second_turn = IngestFenceRepository.claim(
                VNextUnitOfWork(connector, backend="sqlite"),
                owner_token=b"k" * 16,
                now=1_712,
                lease_duration=1_000_000,
            )
        with connector.transaction():
            empty_build = _source_build_id(
                connector,
                scope=scope,
                manifest_sha256=SourceBuildManifestSummary.empty().manifest_sha256,
                gallery_count=0,
            )
            _seed_build(
                connector,
                build_id=empty_build,
                scope=scope,
                manifest_byte=5,
                gallery_count=0,
                base_receipt=base_receipt,
                created_at=1_713,
                sealed_at=1_714,
                manifest_sha256=SourceBuildManifestSummary.empty().manifest_sha256,
            )
            _map_working_build(
                connector,
                build_id=empty_build,
                generation=second_turn.generation,
                replace=True,
            )
        current = _begin(
            connector,
            gate,
            second_turn,
            build_id=empty_build,
            analysis_id=b"N" * 16,
            now=1_720,
        )
        assert current.overlay_depth == 1
        _run_file_slice(
            connector,
            gate,
            second_turn,
            current.analysis_id,
            max_rows=128,
            start_now=1_800,
        )
        _run_stage(
            connector,
            gate,
            second_turn,
            AnalysisRepository.process_impacted_gallery_batch,
            analysis_id=current.analysis_id,
            prefix=b"removed-impact-gallery",
            max_rows=128,
            start_now=2_300,
        )
        _run_removed_gallery_stage(
            connector,
            gate,
            second_turn,
            current.analysis_id,
            AnalysisRepository.process_impacted_content_batch,
            prefix=b"removed-impact-content",
            start_now=2_400,
        )
        _run_removed_gallery_stage(
            connector,
            gate,
            second_turn,
            current.analysis_id,
            AnalysisRepository.process_content_owner_candidate_batch,
            prefix=b"removed-content-candidate",
            start_now=2_500,
        )
        _run_removed_gallery_stage(
            connector,
            gate,
            second_turn,
            current.analysis_id,
            AnalysisRepository.validate_content_owner_candidate_batch,
            prefix=b"removed-validate-content-candidate",
            start_now=2_600,
        )
        _run_stage(
            connector,
            gate,
            second_turn,
            AnalysisRepository.process_content_owner_batch,
            analysis_id=current.analysis_id,
            prefix=b"removed-content-owner",
            max_rows=128,
            start_now=2_700,
        )
        _run_stage(
            connector,
            gate,
            second_turn,
            AnalysisRepository.validate_content_owner_batch,
            analysis_id=current.analysis_id,
            prefix=b"removed-validate-content-owner",
            max_rows=128,
            start_now=2_800,
        )
        _run_stage(
            connector,
            gate,
            second_turn,
            AnalysisRepository.process_impacted_gid_batch,
            analysis_id=current.analysis_id,
            prefix=b"removed-impact-gid",
            max_rows=128,
            start_now=2_900,
        )
        _run_removed_gallery_stage(
            connector,
            gate,
            second_turn,
            current.analysis_id,
            AnalysisRepository.process_gid_candidate_batch,
            prefix=b"removed-gid-candidate",
            start_now=3_000,
        )
        _run_removed_gallery_stage(
            connector,
            gate,
            second_turn,
            current.analysis_id,
            AnalysisRepository.validate_gid_candidate_batch,
            prefix=b"removed-validate-gid-candidate",
            start_now=3_100,
        )
        _run_stage(
            connector,
            gate,
            second_turn,
            AnalysisRepository.process_gid_winner_batch,
            analysis_id=current.analysis_id,
            prefix=b"removed-gid-winner",
            max_rows=128,
            start_now=3_200,
        )
        _run_stage(
            connector,
            gate,
            second_turn,
            AnalysisRepository.validate_gid_winner_batch,
            analysis_id=current.analysis_id,
            prefix=b"removed-validate-gid-winner",
            max_rows=128,
            start_now=3_300,
        )

        assert connector.fetch_one(
            "SELECT gallery_id "
            "FROM catalog_analysis_content_owner_candidate_tombstones "
            "WHERE analysis_id = %s",
            (current.analysis_id,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT content_sha256 FROM catalog_analysis_content_owner_tombstones "
            "WHERE analysis_id = %s",
            (current.analysis_id,),
        )
        assert connector.fetch_one(
            "SELECT gallery_id FROM catalog_analysis_gid_candidate_tombstones "
            "WHERE analysis_id = %s",
            (current.analysis_id,),
        ) == (1,)
        assert connector.fetch_one(
            "SELECT gid FROM catalog_analysis_gid_winner_tombstones "
            "WHERE analysis_id = %s",
            (current.analysis_id,),
        ) == (10_001,)
        assert connector.fetch_all(
            "SELECT state_component, row_count "
            "FROM catalog_analysis_state_component_seals "
            "WHERE analysis_id = %s ORDER BY state_component",
            (current.analysis_id,),
        ) == [
            (b"content_owner", 0),
            (b"content_owner_candidate", 0),
            (b"file_hash_decision", 0),
            (b"gid_candidate", 0),
            (b"gid_winner", 0),
        ]
        empty_snapshot = _prepare_upload_and_handoff_snapshot(
            connector,
            gate,
            second_turn,
            current.analysis_id,
            now=3_500,
        )
        assert connector.fetch_one(
            "SELECT gallery_count, file_count, byte_count "
            "FROM catalog_source_snapshot_manifest_identity "
            "WHERE snapshot_manifest_sha256 = %s",
            (empty_snapshot,),
        ) == (0, 0, 0)
    finally:
        connector.close()


def test_high_cardinality_spools_never_run_inside_mutation_transactions(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-preparation-split.sqlite3")
    gallery_preparation = None
    snapshot_preparation = None
    try:
        gate, turn = _authorities(connector)
        file_sha256 = b"f" * 32
        with connector.transaction():
            scope = _seed_root(connector)
            build = _source_build_id(
                connector,
                scope=scope,
                manifest_sha256=bytes((7,)) * 32,
                gallery_count=1,
                file_count=1,
                byte_count=1,
            )
            _seed_build(
                connector,
                build_id=build,
                scope=scope,
                manifest_byte=7,
                gallery_count=1,
                file_count=1,
                byte_count=1,
            )
            _seed_gallery(
                connector,
                build_id=build,
                scope=scope,
                gallery_id=1,
                observation_id=1,
                occurrences=((file_sha256, 1),),
                artists=(),
                serial=700,
            )
            _seed_preparation_facts(
                connector,
                gallery_id=1,
                observation_id=1,
                file_sha256=file_sha256,
            )
            _map_working_build(connector, build_id=build, generation=1)
        run = _begin(
            connector,
            gate,
            turn,
            build_id=build,
            analysis_id=b"R" * 16,
            now=30,
        )
        _run_file_slice(
            connector,
            gate,
            turn,
            run.analysis_id,
            max_rows=128,
            start_now=100,
        )
        _run_stage(
            connector,
            gate,
            turn,
            AnalysisRepository.process_impacted_gallery_batch,
            analysis_id=run.analysis_id,
            prefix=b"impact-gallery",
            max_rows=128,
            start_now=500,
        )
        with connector.transaction():
            gallery_authority = AnalysisRepository.issue_preparation_authority(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                now=600,
            )
        gallery_preparation = AnalysisRepository.prepare_gallery(
            connector,
            backend="sqlite",
            authority=gallery_authority,
            gallery_id=1,
        )
        assert gallery_preparation.content_upload_plan is not None
        _put_canonical_plan(
            connector,
            gate,
            turn,
            gallery_preparation.content_upload_plan,
            now=610,
        )

        high_cardinality_error = AssertionError(
            "high-cardinality gallery iterator entered mutation transaction"
        )
        with (
            patch.object(
                analysis_module,
                "_prepare_gallery",
                side_effect=high_cardinality_error,
            ),
            patch.object(
                analysis_module,
                "_iter_effective_content_digests",
                side_effect=high_cardinality_error,
            ),
            patch.object(
                analysis_module,
                "_iter_metadata_chunks",
                side_effect=high_cardinality_error,
            ),
            connector.transaction(),
        ):
            content_batch = AnalysisRepository.process_impacted_content_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                batch_key=b"impact-content",
                max_rows=128,
                preparations=(gallery_preparation,),
                now=620,
            )
        assert content_batch.row_count == 1 and not content_batch.terminal

        with connector.transaction():
            started_at = connector.fetch_one(
                "SELECT started_at FROM catalog_analysis_run_descriptor "
                "WHERE analysis_id = %s",
                (run.analysis_id,),
            )[0]
            seal_time = int(started_at) + 1
            for component in sorted(ANALYSIS_COMPONENTS - {b"file_hash_decision"}):
                seed_analysis_component(
                    connector,
                    analysis_id=run.analysis_id,
                    state_component=component,
                    row_count=0,
                    sealed_at=seal_time,
                    terminal_receipt=True,
                )
            snapshot_authority = AnalysisRepository.issue_preparation_authority(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                now=701,
            )
        snapshot_preparation = AnalysisRepository.prepare_snapshot_manifest(
            connector,
            backend="sqlite",
            authority=snapshot_authority,
        )
        _put_canonical_plan(
            connector,
            gate,
            turn,
            snapshot_preparation.upload_plan,
            now=710,
        )

        snapshot_error = AssertionError(
            "high-cardinality snapshot iterator entered handoff transaction"
        )
        with (
            patch.object(
                analysis_module,
                "_prepare_snapshot_manifest",
                side_effect=snapshot_error,
            ),
            patch.object(
                analysis_module,
                "_iter_snapshot_galleries",
                side_effect=snapshot_error,
            ),
            patch.object(
                analysis_module,
                "_iter_snapshot_decisions",
                side_effect=snapshot_error,
            ),
            patch.object(
                analysis_module,
                "_iter_snapshot_owners",
                side_effect=snapshot_error,
            ),
            patch.object(
                analysis_module,
                "_iter_snapshot_winners",
                side_effect=snapshot_error,
            ),
            patch.object(
                analysis_module,
                "_gallery_file_counts",
                side_effect=snapshot_error,
            ),
            connector.transaction(),
        ):
            snapshot_sha256 = AnalysisRepository.handoff_snapshot_manifest(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                preparation=snapshot_preparation,
                now=720,
            )
        assert snapshot_sha256 == snapshot_preparation.upload_plan.value_sha256
        assert connector.fetch_one(
            "SELECT state, snapshot.snapshot_manifest_sha256 "
            "FROM catalog_analysis_runs AS run "
            "JOIN catalog_analysis_snapshot_manifest AS snapshot "
            "ON snapshot.analysis_id = run.analysis_id "
            "WHERE run.analysis_id = %s",
            (run.analysis_id,),
        ) == ("COMPLETE", snapshot_sha256)
        assert (
            connector.fetch_one(
                "SELECT 1 FROM operational_canonical_value_uploads "
                "WHERE generation = %s AND value_sha256 = %s",
                (turn.generation, snapshot_sha256),
            )
            == ()
        )
    finally:
        if gallery_preparation is not None:
            gallery_preparation.close()
        if snapshot_preparation is not None:
            snapshot_preparation.close()
        connector.close()


def test_analysis_policy_loader_reads_one_atomic_policy_row(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-policy-shape.sqlite3")
    try:
        with connector.transaction():
            _seed_root(connector)
        original_fetch_one = connector.fetch_one
        queries: list[str] = []

        def recording_fetch_one(
            query: str,
            data: tuple[Any, ...] = (),
        ) -> tuple[Any, ...]:
            queries.append(query)
            return original_fetch_one(query, data)

        with patch.object(connector, "fetch_one", side_effect=recording_fetch_one):
            policy = analysis_module._load_policy(
                VNextUnitOfWork(connector, backend="sqlite"),
                1,
            )
        assert policy == analysis_module._Policy(1, 1, 1, 3, 1, 1)
        policy_queries = [
            query for query in queries if "catalog_analysis_policies" in query
        ]
        assert len(policy_queries) == 1
        query = policy_queries[0]
        assert "algorithm_version" in query
        assert "spam_artist_threshold" in query
        assert "spam_occurrence_threshold" in query
        assert "content_owner_rule_version" in query
        assert "gid_winner_rule_version" in query
        assert "FOR UPDATE" not in query.upper()
    finally:
        connector.close()


def test_analysis_policy_loader_fails_closed_for_missing_atomic_row(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-policy-missing.sqlite3")
    try:
        with connector.transaction():
            _seed_root(connector)
        connector.execute("PRAGMA foreign_keys = OFF")
        connector.execute(
            "DELETE FROM catalog_analysis_policies WHERE policy_id = %s",
            (1,),
        )
        connector.execute("PRAGMA foreign_keys = ON")
        with (
            patch.object(connector, "execute", wraps=connector.execute) as execute,
            pytest.raises(AnalysisNotReadyError, match="missing or incomplete"),
        ):
            analysis_module._load_policy(
                VNextUnitOfWork(connector, backend="sqlite"),
                1,
            )
        execute.assert_not_called()
    finally:
        connector.close()


def test_analysis_policy_loader_supports_a_distinct_atomic_policy_tuple(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-policy-distinct.sqlite3")
    try:
        with connector.transaction():
            _seed_root(connector)
            seed_analysis_policy(
                connector,
                policy_id=2,
                algorithm_version=2,
                spam_artist_threshold=2,
                spam_occurrence_threshold=4,
                content_owner_rule_version=2,
                gid_winner_rule_version=2,
            )
        assert analysis_module._load_policy(
            VNextUnitOfWork(connector, backend="sqlite"),
            2,
        ) == analysis_module._Policy(2, 2, 2, 4, 2, 2)
    finally:
        connector.close()


def _counted_batch_call(
    connector: SQLiteConnector,
    callback: Any,
    *,
    zero_dml: bool = False,
) -> tuple[int, list[str], list[str]]:
    with (
        patch.object(connector, "fetch_one", wraps=connector.fetch_one) as fetch_one,
        patch.object(connector, "fetch_all", wraps=connector.fetch_all) as fetch_all,
        patch.object(
            connector,
            "execute_affected",
            wraps=connector.execute_affected,
        ) as execute_affected,
    ):
        if zero_dml:
            with (
                patch.object(
                    connector,
                    "execute",
                    side_effect=AssertionError("batch replay attempted DML"),
                ),
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=AssertionError("batch replay attempted DML"),
                ),
            ):
                callback()
        else:
            callback()
    queries = [
        call.args[0] for call in (*fetch_one.call_args_list, *fetch_all.call_args_list)
    ]
    affected = [call.args[0] for call in execute_affected.call_args_list]
    return fetch_one.call_count + fetch_all.call_count, queries, affected


def _seed_minimal_gid_metadata(
    connector: SQLiteConnector,
    *,
    gallery_id: int,
    observation_id: int,
) -> int:
    gid = 20_000 + gallery_id
    source_name = f"batch-gallery-{gallery_id}".encode("ascii")
    connector.execute(
        "INSERT INTO catalog_gallery_upload_times (gid, upload_time) VALUES (%s, %s)",
        (gid, 1),
    )
    connector.execute(
        "INSERT INTO catalog_source_gallery_name_gids (source_gallery_name, gid) "
        "VALUES (%s, %s)",
        (source_name, gid),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_source_name_accesses "
        "(gallery_id, source_gallery_name) VALUES (%s, %s)",
        (gallery_id, source_name),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_metadata_locals "
        "(gallery_id, observation_id, download_time, modified_time) "
        "VALUES (%s, %s, 1, 1)",
        (gallery_id, observation_id),
    )
    return gid


def _impact_batch_select_profile(
    path: Path,
    page_rows: int,
    *,
    verify_terminal_orphans: bool,
    restore_claim: bool = True,
    distinct_contents: bool = True,
) -> dict[str, tuple[int, list[str], list[str]]]:
    connector = _generated_database(path)
    plans: tuple[CanonicalValueUploadPlan, ...] = ()
    try:
        gate, turn = _authorities(connector)
        analysis_id = b"Y" * 16
        total = page_rows + 1
        with connector.transaction():
            scope = _seed_root(connector)
            build = _source_build_id(
                connector,
                scope=scope,
                manifest_sha256=bytes((21,)) * 32,
                gallery_count=total,
            )
            _seed_build(
                connector,
                build_id=build,
                scope=scope,
                manifest_byte=21,
                gallery_count=total,
            )
            _map_working_build(connector, build_id=build, generation=1)
        run = _begin(
            connector,
            gate,
            turn,
            build_id=build,
            analysis_id=analysis_id,
            now=30,
        )
        connector.execute("PRAGMA foreign_keys = OFF")
        with connector.transaction():
            for gallery_id in range(1, total + 1):
                connector.execute(
                    "INSERT INTO catalog_source_build_galleries "
                    "(build_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
                    (build, gallery_id, gallery_id),
                )
                connector.execute(
                    "INSERT INTO catalog_analysis_changed_galleries "
                    "(analysis_id, gallery_id, change_kind) VALUES (%s, %s, 'ADDED')",
                    (run.analysis_id, gallery_id),
                )
            seed_analysis_component(
                connector,
                analysis_id=run.analysis_id,
                state_component=b"file_hash_decision",
                row_count=0,
                sealed_at=31,
                terminal_receipt=True,
            )
        _run_stage(
            connector,
            gate,
            turn,
            AnalysisRepository.process_impacted_gallery_batch,
            analysis_id=run.analysis_id,
            prefix=b"count-impact-gallery",
            max_rows=128,
            start_now=100,
        )
        receipt = _issue_preparation_authority(
            connector,
            gate,
            turn,
            run.analysis_id,
            now=200,
        )
        if distinct_contents:
            plans = tuple(
                CanonicalValueUploadPlan.from_parts(
                    "effective_content_v1",
                    (b"effective-content-" + gallery_id.to_bytes(8, "big"),),
                )
                for gallery_id in range(1, total + 1)
            )
        else:
            shared_plan = CanonicalValueUploadPlan.from_parts(
                "effective_content_v1",
                (b"one shared effective-content payload",),
            )
            plans = (shared_plan,) * total
        for offset, upload_plan in enumerate(dict.fromkeys(plans)):
            _put_canonical_plan(
                connector,
                gate,
                turn,
                upload_plan,
                now=210 + offset * 3,
            )
        preparations = tuple(
            analysis_module.AnalysisGalleryPreparation(
                run.analysis_id,
                build,
                gallery_id,
                gallery_id,
                20_000 + gallery_id,
                plans[gallery_id - 1].value_sha256,
                0,
                1,
                1,
                plans[gallery_id - 1],
                receipt,
                analysis_module._PREPARATION_TOKEN,
            )
            for gallery_id in range(1, total + 1)
        )
        with connector.transaction():
            first_content = AnalysisRepository.process_impacted_content_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                batch_key=b"count-content-first",
                max_rows=1,
                preparations=preparations[:1],
                now=1_000,
            )
        assert first_content.row_count == 1
        if restore_claim and not distinct_contents:
            connector.execute(
                "INSERT INTO operational_canonical_value_uploads "
                "(generation, value_sha256) VALUES (%s, %s)",
                (receipt.generation, plans[0].value_sha256),
            )

        def fresh_content() -> None:
            with connector.transaction():
                result = AnalysisRepository.process_impacted_content_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=b"count-content-page",
                    max_rows=128,
                    preparations=preparations[1:],
                    now=1_001,
                )
            assert result.row_count == page_rows and not result.replayed

        profile: dict[str, tuple[int, list[str], list[str]]] = {}
        profile["content_fresh"] = _counted_batch_call(connector, fresh_content)
        assert connector.fetch_one(
            "SELECT COUNT(*) FROM operational_canonical_value_uploads "
            "WHERE generation = %s",
            (receipt.generation,),
        ) == (0,)

        def replay_content() -> None:
            with connector.transaction():
                replay = AnalysisRepository.process_impacted_content_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=b"count-content-page",
                    max_rows=1,
                    preparations=preparations[1:],
                    now=1_002,
                )
            assert replay.replayed and replay.row_count == page_rows

        profile["content_replay"] = _counted_batch_call(
            connector,
            replay_content,
            zero_dml=True,
        )
        if verify_terminal_orphans:

            def fresh_content_terminal() -> None:
                with connector.transaction():
                    terminal = AnalysisRepository.process_impacted_content_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        analysis_id=run.analysis_id,
                        batch_key=b"count-content-terminal",
                        max_rows=128,
                        preparations=(),
                        now=1_003,
                    )
                assert terminal.terminal and not terminal.replayed

            profile["content_terminal"] = _counted_batch_call(
                connector,
                fresh_content_terminal,
            )

            def replay_content_terminal() -> None:
                with connector.transaction():
                    terminal = AnalysisRepository.process_impacted_content_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        analysis_id=run.analysis_id,
                        batch_key=b"count-content-terminal",
                        max_rows=1,
                        preparations=(),
                        now=1_004,
                    )
                assert terminal.terminal and terminal.replayed

            profile["content_terminal_replay"] = _counted_batch_call(
                connector,
                replay_content_terminal,
                zero_dml=True,
            )
            connector.execute(
                "INSERT INTO catalog_analysis_impacted_content "
                "(analysis_id, content_sha256, witness_gallery_id) "
                "VALUES (%s, %s, 1)",
                (run.analysis_id, b"o" * 32),
            )
            with (
                connector.transaction(),
                patch.object(
                    connector,
                    "execute",
                    side_effect=AssertionError("terminal replay attempted DML"),
                ),
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=AssertionError("terminal replay attempted DML"),
                ),
                pytest.raises(AnalysisCorruptionError, match="terminal keyspace"),
            ):
                AnalysisRepository.process_impacted_content_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=b"count-content-terminal",
                    max_rows=1,
                    preparations=(),
                    now=1_005,
                )
            missing_plan = CanonicalValueUploadPlan.from_parts(
                "effective_content_v1",
                (b"sealed identity whose live claim is deliberately absent",),
            )
            try:
                _put_canonical_plan(connector, gate, turn, missing_plan, now=1_006)
                connector.execute(
                    "DELETE FROM operational_canonical_value_uploads "
                    "WHERE generation = %s AND value_sha256 = %s",
                    (receipt.generation, missing_plan.value_sha256),
                )
                missing_preparation = analysis_module.AnalysisGalleryPreparation(
                    run.analysis_id,
                    build,
                    1,
                    1,
                    20_001,
                    missing_plan.value_sha256,
                    0,
                    1,
                    1,
                    missing_plan,
                    receipt,
                    analysis_module._PREPARATION_TOKEN,
                )
                with (
                    connector.transaction(),
                    pytest.raises(
                        AnalysisNotReadyError,
                        match="live-generation upload claim",
                    ),
                ):
                    analysis_module._consume_effective_content_claims(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        analysis_module._RunAuthority(
                            run.analysis_id,
                            build,
                            analysis_module._Policy(1, 1, 1, 1, 1, 1),
                            None,
                            0,
                        ),
                        (missing_preparation,),
                        preexisting_contents=frozenset(),
                    )
            finally:
                missing_plan.close()

        with connector.transaction():
            for gallery_id in range(1, total + 1):
                _seed_minimal_gid_metadata(
                    connector,
                    gallery_id=gallery_id,
                    observation_id=gallery_id,
                )
                seed_content_owner_shadow(
                    connector,
                    analysis_id=run.analysis_id,
                    content_sha256=sha256(
                        b"owner" + gallery_id.to_bytes(8, "big")
                    ).digest(),
                    owner_gallery_id=gallery_id,
                )
            seed_analysis_component(
                connector,
                analysis_id=run.analysis_id,
                state_component=b"content_owner",
                row_count=total,
                sealed_at=32,
                terminal_receipt=True,
            )
        with connector.transaction():
            first_gid = AnalysisRepository.process_impacted_gid_batch(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                analysis_id=run.analysis_id,
                batch_key=b"count-gid-first",
                max_rows=1,
                now=1_200,
            )
        assert first_gid.row_count == 1

        def fresh_gid() -> None:
            with connector.transaction():
                result = AnalysisRepository.process_impacted_gid_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=b"count-gid-page",
                    max_rows=128,
                    now=1_201,
                )
            assert result.row_count == page_rows and not result.replayed

        profile["gid_fresh"] = _counted_batch_call(connector, fresh_gid)

        def replay_gid() -> None:
            with connector.transaction():
                replay = AnalysisRepository.process_impacted_gid_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=b"count-gid-page",
                    max_rows=1,
                    now=1_202,
                )
            assert replay.replayed and replay.row_count == page_rows

        profile["gid_replay"] = _counted_batch_call(
            connector,
            replay_gid,
            zero_dml=True,
        )
        if verify_terminal_orphans:

            def fresh_gid_terminal() -> None:
                with connector.transaction():
                    terminal = AnalysisRepository.process_impacted_gid_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        analysis_id=run.analysis_id,
                        batch_key=b"count-gid-terminal",
                        max_rows=128,
                        now=1_203,
                    )
                assert terminal.terminal and not terminal.replayed

            profile["gid_terminal"] = _counted_batch_call(
                connector,
                fresh_gid_terminal,
            )

            def replay_gid_terminal() -> None:
                with connector.transaction():
                    terminal = AnalysisRepository.process_impacted_gid_batch(
                        VNextUnitOfWork(connector, backend="sqlite"),
                        gate_lease=gate,
                        ingest_turn=turn,
                        analysis_id=run.analysis_id,
                        batch_key=b"count-gid-terminal",
                        max_rows=1,
                        now=1_204,
                    )
                assert terminal.terminal and terminal.replayed

            profile["gid_terminal_replay"] = _counted_batch_call(
                connector,
                replay_gid_terminal,
                zero_dml=True,
            )
            connector.execute(
                "INSERT INTO catalog_analysis_impacted_gid_storage "
                "(analysis_id, gid) VALUES (%s, %s)",
                (run.analysis_id, 99_999),
            )
            with (
                connector.transaction(),
                patch.object(
                    connector,
                    "execute",
                    side_effect=AssertionError("terminal replay attempted DML"),
                ),
                patch.object(
                    connector,
                    "execute_affected",
                    side_effect=AssertionError("terminal replay attempted DML"),
                ),
                pytest.raises(AnalysisCorruptionError, match="terminal keyspace"),
            ):
                AnalysisRepository.process_impacted_gid_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=b"count-gid-terminal",
                    max_rows=1,
                    now=1_205,
                )
        return profile
    finally:
        for plan in dict.fromkeys(plans):
            plan.close()
        connector.close()


def test_impacted_stage_select_counts_are_constant_for_one_and_128_rows(
    tmp_path: Path,
) -> None:
    one = _impact_batch_select_profile(
        tmp_path / "analysis-impact-count-1.sqlite3",
        1,
        verify_terminal_orphans=True,
    )
    many = _impact_batch_select_profile(
        tmp_path / "analysis-impact-count-128.sqlite3",
        128,
        verify_terminal_orphans=False,
    )
    preexisting_without_claim = _impact_batch_select_profile(
        tmp_path / "analysis-impact-preexisting-no-claim.sqlite3",
        1,
        verify_terminal_orphans=False,
        restore_claim=False,
        distinct_contents=False,
    )
    for stage in ("content_fresh", "content_replay", "gid_fresh", "gid_replay"):
        assert one[stage][0] == many[stage][0], stage
    content_fresh_queries = many["content_fresh"][1]
    assert (
        sum("WITH proposed(gallery_id)" in query for query in content_fresh_queries)
        == 1
    )
    allocation_queries = [
        query
        for query in content_fresh_queries
        if "FROM catalog_canonical_value_allocation_anchors" in query
    ]
    assert len(allocation_queries) == 1
    assert allocation_queries[0].count("%s") == 128
    claim_queries = [
        query
        for query in content_fresh_queries
        if "FROM operational_canonical_value_uploads" in query
    ]
    assert len(claim_queries) == 1
    assert claim_queries[0].count("%s") == 129
    provenance_preflights = [
        query
        for query in content_fresh_queries
        if "FROM catalog_analysis_impacted_content AS impacted" in query
        and "page_or_future" in query
    ]
    assert len(provenance_preflights) == 1
    assert "SELECT %s AS key_value" not in provenance_preflights[0]
    assert provenance_preflights[0].count("impacted.content_sha256 IN (") == 1
    assert provenance_preflights[0].count("%s") == 131
    content_deletes = [
        query
        for query in many["content_fresh"][2]
        if query.startswith("DELETE FROM operational_canonical_value_uploads")
    ]
    assert len(content_deletes) == 1 and " IN (" in content_deletes[0]
    assert content_deletes[0].count("%s") == 129
    assert not any(
        query.startswith("DELETE FROM operational_canonical_value_uploads")
        for query in preexisting_without_claim["content_fresh"][2]
    )
    content_replay_queries = many["content_replay"][1]
    assert (
        sum("WITH proposed(gallery_id)" in query for query in content_replay_queries)
        == 1
    )
    assert not any(
        "catalog_canonical_value_allocation_anchors" in query
        or "operational_canonical_value_uploads" in query
        for query in content_replay_queries
    )
    for stage in ("gid_fresh", "gid_replay"):
        queries = many[stage][1]
        assert sum("WITH proposed(gallery_id)" in query for query in queries) == 1
    gid_storage_preflights = [
        query
        for query in many["gid_fresh"][1]
        if "FROM catalog_a_impacted_gid_provenance_storage" in query
        and "gallery_id IN (" in query
    ]
    assert len(gid_storage_preflights) == 1
    assert gid_storage_preflights[0].count("%s") == 129
    for stage in (
        "content_terminal",
        "content_terminal_replay",
        "gid_terminal",
        "gid_terminal_replay",
    ):
        queries = one[stage][1]
        assert sum(" AS violations LIMIT 1" in query for query in queries) == 1
        assert (
            sum(
                "_provenance AS provenance" in query and "LIMIT %s" in query
                for query in queries
            )
            == 1
        )


def test_depth_zero_impact_loaders_do_not_join_real_zero_identifier_rows(
    tmp_path: Path,
) -> None:
    connector = _generated_database(tmp_path / "analysis-zero-id-isolation.sqlite3")
    try:
        _gate, _turn = _authorities(connector)
        zero = bytes(16)
        current_build = b"C" * 16
        with connector.transaction():
            scope = _seed_root(connector)
            _seed_build(
                connector,
                build_id=zero,
                scope=scope,
                manifest_byte=22,
                gallery_count=1,
            )
            _seed_build(
                connector,
                build_id=current_build,
                scope=scope,
                manifest_byte=23,
                gallery_count=0,
            )
            _map_working_build(connector, build_id=zero, generation=1)
            seed_analysis_run(
                connector,
                analysis_id=zero,
                build_id=zero,
                policy_id=1,
                input_manifest_sha256=b"z" * 32,
                started_at=30,
            )
        connector.execute("PRAGMA foreign_keys = OFF")
        with connector.transaction():
            connector.execute(
                "INSERT INTO catalog_source_build_galleries "
                "(build_id, gallery_id, observation_id) VALUES (%s, 1, 1)",
                (zero,),
            )
            _seed_minimal_gid_metadata(
                connector,
                gallery_id=1,
                observation_id=1,
            )
            seed_content_owner_candidate_shadow(
                connector,
                analysis_id=zero,
                gallery_id=1,
                content_sha256=b"z" * 32,
                prefer_not_already_uploaded=0,
                title_scalar_count=1,
                download_time=1,
            )
            connector.execute(
                "INSERT INTO catalog_analysis_gid_candidate_shadows "
                "(analysis_id, gallery_id) VALUES (%s, 1)",
                (zero,),
            )
        authority = analysis_module._RunAuthority(
            b"N" * 16,
            current_build,
            analysis_module._Policy(1, 1, 1, 1, 1, 1),
            None,
            0,
        )
        content_page = analysis_module._load_content_impact_page(
            VNextUnitOfWork(connector, backend="sqlite"),
            authority,
            (1,),
        )
        gid_page = analysis_module._load_gid_impact_page(
            VNextUnitOfWork(connector, backend="sqlite"),
            authority,
            (1,),
        )
        assert content_page.current_observations == {1: None}
        assert content_page.old_candidates == {1: None}
        assert gid_page.old_gids == {1: None}
        assert gid_page.current_gids == {1: None}
    finally:
        connector.close()


def test_mariadb_checkpoint_lock_and_cas_keep_server_placeholders_and_row_lock() -> (
    None
):
    import h2hdb.vnext_analysis_repository as module

    class RecordingConnector:
        def __init__(self) -> None:
            self.lock_query = ""
            self.batch_lock_query = ""
            self.cas_queries: list[str] = []
            self.insert_queries: list[str] = []
            self.insert_parameters: list[tuple[Any, ...]] = []

        def fetch_one(self, query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
            if "catalog_analysis_stages" in query:
                return (b"01", b"analysis_gallery_v1")
            if "catalog_analysis_checkpoints" in query:
                self.lock_query = query
                return (2, b"\x01G\x00" + bytes(8), 0, "OPEN", 1)
            if "TIMESTAMPDIFF" in query:
                return (2,)
            return ()

        def fetch_all(
            self,
            query: str,
            data: tuple[Any, ...] = (),
        ) -> list[tuple[Any, ...]]:
            self.batch_lock_query = query
            return [(1, b"a" * 32), (1, b"b" * 32)]

        def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
            self.insert_queries.append(query)
            self.insert_parameters.append(data)

        def execute_affected(self, query: str, data: tuple[Any, ...] = ()) -> int:
            self.cas_queries.append(query)
            return 1

    connector: Any = RecordingConnector()
    work = VNextUnitOfWork(connector, backend="mariadb")
    checkpoint = module._lock_checkpoint(
        work,
        b"A" * 16,
        b"changed_gallery",
        page_limit=128,
    )
    assert connector.lock_query.endswith(" FOR UPDATE")
    assert "%s" in connector.lock_query and "?" not in connector.lock_query
    authority = module._RunAuthority(
        b"A" * 16,
        b"B" * 16,
        module._Policy(1, 1, 1, 1, 1, 1),
        None,
        0,
    )
    module._commit_batch(
        work,
        authority=authority,
        stage=b"changed_gallery",
        batch_key=b"batch",
        checkpoint=checkpoint,
        cursor=b"\x01G\x00" + bytes(8),
        row_count=0,
        terminal=True,
        now=2,
    )
    assert len(connector.insert_queries) == 1
    assert "catalog_analysis_batch_receipt_stored" in connector.insert_queries[0]
    assert connector.insert_parameters[0][6] == 128
    assert all("%s" in query and "?" not in query for query in connector.insert_queries)
    assert "catalog_analysis_checkpoints" in connector.cas_queries[0]
    assert "SET generation = %s, `cursor` = %s" in connector.cas_queries[0]
    assert (
        "processed_count = %s, state = %s, updated_at = %s"
        in (connector.cas_queries[0])
    )
    assert "AND generation = %s" in connector.cas_queries[0]
    assert (
        "DELETE FROM catalog_analysis_batch_receipt_stored"
        in (connector.cas_queries[1])
    )
    assert "start_generation = %s" in connector.cas_queries[1]
    assert all("?" not in query for query in connector.cas_queries)
    claims = work.lock_rows(
        LockRank.CHECKPOINT,
        tuple(
            sorted(
                (
                    encode_lock_key("analysis-content-upload", 1, b"a" * 32),
                    encode_lock_key("analysis-content-upload", 1, b"b" * 32),
                )
            )
        ),
        "SELECT generation, value_sha256 "
        "FROM operational_canonical_value_uploads "
        "WHERE generation = %s AND value_sha256 IN (%s, %s) "
        "ORDER BY value_sha256",
        (1, b"a" * 32, b"b" * 32),
    )
    assert claims == [(1, b"a" * 32), (1, b"b" * 32)]
    assert connector.batch_lock_query.endswith(" FOR UPDATE")
    assert "%s" in connector.batch_lock_query and "?" not in connector.batch_lock_query
