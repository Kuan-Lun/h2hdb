from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

import h2hdb.vnext_analysis_repository as analysis_module
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_analysis_repository import (
    ANALYSIS_COMPONENTS,
    AnalysisCorruptionError,
    AnalysisNotReadyError,
    AnalysisRepository,
)
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
    IngestTurn,
)
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
)
from h2hdb.vnext_transaction import VNextUnitOfWork


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
    connector.execute(
        "INSERT INTO catalog_canonical_value_allocations "
        "(value_sha256, digest_domain, byte_count, allocated_at) "
        "VALUES (%s, %s, %s, %s)",
        (value_sha256, domain, 1, 1),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_pages "
        "(page_sha256, value_sha256, page_bytes) VALUES (%s, %s, %s)",
        (page_sha256, value_sha256, page_bytes),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_page_descriptors "
        "(page_sha256, value_sha256, level, page_position, subtree_item_count) "
        "VALUES (%s, %s, 0, 0, 1)",
        (page_sha256, value_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_canonical_value_identities "
        "(value_sha256, root_page_sha256) VALUES (%s, %s)",
        (value_sha256, page_sha256),
    )


def _seed_root(connector: SQLiteConnector) -> bytes:
    root = b"r" * 32
    _canonical_identity(connector, root, domain=b"source_root_v1", serial=1)
    scope = b"s" * 32
    connector.execute(
        "INSERT INTO catalog_source_scopes "
        "(scope_key, source_provider, source_root_sha256, identity_policy_version) "
        "VALUES (%s, %s, %s, 1)",
        (scope, b"filesystem", root),
    )
    connector.execute(
        "INSERT INTO catalog_manifest_policies "
        "(manifest_policy_id, manifest_algorithm_version, file_order_version) "
        "VALUES (1, 1, 1)"
    )
    connector.execute(
        "INSERT INTO catalog_analysis_policies "
        "(policy_id, algorithm_version, spam_artist_threshold, "
        "spam_occurrence_threshold, content_owner_rule_version, "
        "gid_winner_rule_version) VALUES (1, 1, 1, 3, 1, 1)"
    )
    for tag_id in (1, 2, 3):
        value = bytes((100 + tag_id,)) * 32
        _canonical_identity(
            connector,
            value,
            domain=b"tag_value_utf8_v1",
            serial=10 + tag_id,
        )
        connector.execute(
            "INSERT INTO catalog_tag_terms "
            "(tag_id, namespace, tag_value_sha256) VALUES (%s, %s, %s)",
            (tag_id, b"artist", value),
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
    base_source: tuple[int, int] | None = None,
) -> None:
    connector.execute(
        "INSERT INTO catalog_source_builds "
        "(build_id, scope_key, manifest_policy_id, state, created_at, sealed_at) "
        "VALUES (%s, %s, 1, 'SEALED', 20, 21)",
        (build_id, scope),
    )
    connector.execute(
        "INSERT INTO catalog_source_build_channel (build_id, channel) "
        "VALUES (%s, %s)",
        (build_id, b"default"),
    )
    if base_source is not None:
        connector.execute(
            "INSERT INTO catalog_source_build_base_source "
            "(build_id, base_source_revision, base_source_generation) "
            "VALUES (%s, %s, %s)",
            (build_id, *base_source),
        )
    connector.execute(
        "INSERT INTO catalog_build_manifests "
        "(build_id, manifest_sha256, gallery_count, file_count, byte_count, computed_at) "
        "VALUES (%s, %s, %s, %s, %s, 21)",
        (
            build_id,
            bytes((manifest_byte,)) * 32,
            gallery_count,
            file_count,
            byte_count,
        ),
    )


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
        connector.execute(
            "INSERT INTO catalog_gallery_identities "
            "(gallery_id, gallery_key, scope_key, locator_sha256) "
            "VALUES (%s, %s, %s, %s)",
            (
                gallery_id,
                sha256(b"gallery" + gallery_id.to_bytes(8, "big")).digest(),
                scope,
                locator,
            ),
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
    for encoded in tree.pages:
        page = decode_gallery_observation_page(encoded.page_bytes)
        connector.execute(
            "INSERT INTO catalog_gallery_observation_pages "
            "(page_sha256, page_bytes) VALUES (%s, %s)",
            (encoded.page_sha256, encoded.page_bytes),
        )
        connector.execute(
            "INSERT INTO catalog_gallery_observation_page_descriptors "
            "(page_sha256, component, level, subtree_item_count) "
            "VALUES (%s, %s, %s, %s)",
            (
                encoded.page_sha256,
                b"METADATA",
                page.level,
                page.subtree_item_count,
            ),
        )
        if page.node_kind is GalleryObservationNodeKind.BRANCH:
            for position, entry in enumerate(page.entries):
                assert isinstance(entry, GalleryObservationBranchEntry)
                connector.execute(
                    "INSERT INTO catalog_gallery_observation_page_children "
                    "(parent_sha256, position, child_sha256) VALUES (%s, %s, %s)",
                    (encoded.page_sha256, position, entry.child_sha256),
                )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_tree_roots "
        "(gallery_id, observation_id, root_page_sha256) VALUES (%s, %s, %s)",
        (gallery_id, observation_id, tree.root_page_sha256),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_metadata "
        "(gallery_id, observation_id, gid, upload_time, download_time, modified_time) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (
            gallery_id,
            observation_id,
            metadata.gid,
            metadata.upload_time,
            metadata.download_time,
            metadata.modified_time,
        ),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_scans "
        "(gallery_id, observation_id, scan_observation_sha256, "
        "scan_observation_version, source_file_count) VALUES (%s, %s, %s, 1, 1)",
        (
            gallery_id,
            observation_id,
            sha256(b"scan" + gallery_id.to_bytes(8, "big")).digest(),
        ),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_stat "
        "(gallery_id, observation_id, file_count, byte_count) "
        "VALUES (%s, %s, 1, 1)",
        (gallery_id, observation_id),
    )
    name = f"content-{gallery_id}.jpg".encode("ascii")
    file_key = sha256(b"file-key\0" + name).digest()
    connector.execute(
        "INSERT INTO catalog_file_name_identities (file_key, name_bytes, file_role) "
        "VALUES (%s, %s, %s)",
        (file_key, name, b"CONTENT"),
    )
    connector.execute(
        "INSERT INTO catalog_gallery_observation_files "
        "(gallery_id, observation_id, file_no, file_key, file_sha256) "
        "VALUES (%s, %s, 1, %s, %s)",
        (gallery_id, observation_id, file_key, file_sha256),
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
        assert not first.terminal and first.state == "OPEN"
        if replay_first:
            with connector.transaction():
                replay = method(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=analysis_id,
                    batch_key=first_key,
                    max_rows=128,
                    preparations=(),
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
        assert terminal.terminal and terminal.state == "COMPLETE"
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
            analysis_attempt_id=analysis_id,
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
        if result.state == "COMPLETE":
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
    )
    assert results[-1].component_sealed


def _seed_initial_snapshot(
    connector: SQLiteConnector,
) -> tuple[bytes, bytes, bytes, bytes]:
    scope = _seed_root(connector)
    build = b"b" * 16
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
    _map_working_build(connector, build_id=build, generation=1)
    return scope, build, first, second


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
            "SELECT old_excluded, new_excluded "
            "FROM catalog_analysis_exclusion_deltas "
            "WHERE analysis_id = %s AND file_sha256 = %s",
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
            assert committed.state == "OPEN"
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
                committed.analysis_id,
                committed.stage,
                committed.batch_key,
                committed.start_generation,
                committed.start_cursor,
                committed.start_processed_count,
                committed.next_cursor,
                committed.next_processed_count,
                committed.next_state,
                committed.row_count,
                committed.terminal,
                committed.committed_generation,
                committed.committed_at,
                True,
                committed.component_sealed,
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
            assert terminal.state == "COMPLETE"
            assert terminal.terminal and terminal.row_count == 0
        assert connector.fetch_one(
            "SELECT row_count FROM catalog_analysis_state_component_seals "
            "WHERE analysis_id = %s AND state_component = %s",
            (run.analysis_id, b"file_hash_decision"),
        ) == (2,)
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
        for component in sorted(ANALYSIS_COMPONENTS - {b"file_hash_decision"}):
            connector.execute(
                "INSERT INTO catalog_analysis_state_component_seals "
                "(analysis_id, state_component, row_count, sealed_at) "
                "VALUES (%s, %s, 0, 500)",
                (analysis_id, component),
            )
        _canonical_identity(
            connector,
            snapshot,
            domain=b"source_snapshot_manifest_v1",
            serial=900,
        )
        connector.execute(
            "INSERT INTO catalog_source_snapshot_manifest_identity "
            "(snapshot_manifest_sha256, gallery_count, file_count, byte_count) "
            "VALUES (%s, 2, 0, 0)",
            (snapshot,),
        )
        connector.execute(
            "INSERT INTO catalog_analysis_snapshot_manifest "
            "(analysis_id, snapshot_manifest_sha256) VALUES (%s, %s)",
            (analysis_id, snapshot),
        )
        connector.execute(
            "UPDATE catalog_analysis_runs SET state = 'COMPLETE', completed_at = 501 "
            "WHERE analysis_id = %s",
            (analysis_id,),
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
        connector.execute(
            "INSERT INTO catalog_source_revisions "
            "(source_revision, channel, snapshot_manifest_sha256, published_at) "
            "VALUES (1, %s, %s, 510)",
            (b"default", snapshot),
        )
        connector.execute(
            "INSERT INTO catalog_source_revision_provenance "
            "(source_revision, analysis_id) VALUES (1, %s)",
            (baseline.analysis_id,),
        )
        connector.execute(
            "INSERT INTO catalog_source_heads "
            "(channel, source_revision, generation, advanced_at) "
            "VALUES (%s, 1, 1, 510)",
            (b"default",),
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

    second_build = b"d" * 16
    added = b"c" * 32
    with connector.transaction():
        _seed_build(
            connector,
            build_id=second_build,
            scope=scope,
            manifest_byte=2,
            gallery_count=2,
            base_source=(1, 1),
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
                    "DELETE FROM catalog_analysis_file_hash_decision_shadow "
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
                    "UPDATE catalog_analysis_file_hash_decision_shadow "
                    "SET occurrence_count = occurrence_count + 1 "
                    "WHERE analysis_id = %s AND file_sha256 = %s",
                    (run.analysis_id, first),
                )
        with pytest.raises(AnalysisCorruptionError, match="full evaluator"):
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
            connector.execute(
                "UPDATE catalog_source_heads SET generation = 2 WHERE channel = %s",
                (b"default",),
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
            build = b"L" * 16
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
        assert first.row_count == first.cumulative_row_count == 128
        assert first.state == "OPEN"
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
        assert second.cumulative_row_count == 130
        assert second.state == "OPEN"
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
        assert terminal.cumulative_row_count == 130
        assert terminal.state == "COMPLETE" and terminal.terminal
        with pytest.raises(ValueError, match="must not exceed"):
            with connector.transaction():
                AnalysisRepository.process_changed_file_hash_batch(
                    VNextUnitOfWork(connector, backend="sqlite"),
                    gate_lease=gate,
                    ingest_turn=turn,
                    analysis_id=run.analysis_id,
                    batch_key=b"too-large",
                    max_rows=129,
                    now=43,
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
            build = b"W" * 16
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
        with connector.transaction():
            replayed_handoff = AnalysisRepository.handoff_snapshot_manifest(
                VNextUnitOfWork(connector, backend="sqlite"),
                gate_lease=gate,
                ingest_turn=turn,
                preparation=snapshot_preparation,
                now=1_622,
            )
        assert replayed_handoff == first_handoff
        assert connector.fetch_one(
            "SELECT state, completed_at FROM catalog_analysis_runs "
            "WHERE analysis_id = %s",
            (run.analysis_id,),
        ) == ("COMPLETE", 1_620)
        assert connector.fetch_one(
            "SELECT snapshot_manifest_sha256 "
            "FROM catalog_analysis_snapshot_manifest WHERE analysis_id = %s",
            (run.analysis_id,),
        ) == (first_handoff,)
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
            baseline_build = b"U" * 16
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
            connector.execute(
                "INSERT INTO catalog_source_revisions "
                "(source_revision, channel, snapshot_manifest_sha256, published_at) "
                "VALUES (1, %s, %s, 1710)",
                (b"default", baseline_snapshot),
            )
            connector.execute(
                "INSERT INTO catalog_source_revision_provenance "
                "(source_revision, analysis_id) VALUES (1, %s)",
                (baseline.analysis_id,),
            )
            connector.execute(
                "INSERT INTO catalog_source_heads "
                "(channel, source_revision, generation, advanced_at) "
                "VALUES (%s, 1, 1, 1710)",
                (b"default",),
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
            empty_build = b"E" * 16
            _seed_build(
                connector,
                build_id=empty_build,
                scope=scope,
                manifest_byte=5,
                gallery_count=0,
                base_source=(1, 1),
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
            build = b"P" * 16
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
            for component in sorted(ANALYSIS_COMPONENTS - {b"file_hash_decision"}):
                connector.execute(
                    "INSERT INTO catalog_analysis_state_component_seals "
                    "(analysis_id, state_component, row_count, sealed_at) "
                    "VALUES (%s, %s, 0, 700)",
                    (run.analysis_id, component),
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


def test_mariadb_checkpoint_lock_and_cas_keep_server_placeholders_and_row_lock() -> (
    None
):
    import h2hdb.vnext_analysis_repository as module

    class RecordingConnector:
        def __init__(self) -> None:
            self.lock_query = ""
            self.cas_query = ""
            self.receipt_query = ""

        def fetch_one(self, query: str, data: tuple[Any, ...] = ()) -> tuple[Any, ...]:
            if "catalog_analysis_stages" in query:
                return (b"01", b"analysis_gallery_v1")
            self.lock_query = query
            return (1, b"\x01G\x00" + bytes(8), 0, "OPEN", 1)

        def execute(self, query: str, data: tuple[Any, ...] = ()) -> None:
            self.receipt_query = query

        def execute_affected(self, query: str, data: tuple[Any, ...] = ()) -> int:
            self.cas_query = query
            return 1

    connector: Any = RecordingConnector()
    work = VNextUnitOfWork(connector, backend="mariadb")
    checkpoint = module._lock_checkpoint(
        work,
        b"A" * 16,
        b"changed_gallery",
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
    assert "start_generation" in connector.receipt_query
    assert "next_processed_count" in connector.receipt_query
    assert "next_state" in connector.receipt_query
    assert "terminal" in connector.receipt_query
    assert "AND generation = %s" in connector.cas_query
    assert "AND cursor = %s" in connector.cas_query
    assert "AND processed_count = %s" in connector.cas_query
    assert "AND state = %s" in connector.cas_query
    assert "AND updated_at = %s" in connector.cas_query
    assert "?" not in connector.cas_query
