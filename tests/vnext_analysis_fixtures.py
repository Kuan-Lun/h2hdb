"""Narrow seal-last analysis-family fixtures shared by runtime tests."""

from __future__ import annotations

from typing import Any

from h2hdb.vnext_analysis_family import (
    ensure_analysis_run_family,
    ensure_analysis_state_component_family,
    insert_analysis_exclusion_delta_family,
    insert_analysis_run_completed_at,
)
from h2hdb.vnext_analysis_overlay_family import (
    AnalysisContentOwnerCandidateShadowFamily,
    AnalysisContentOwnerShadowFamily,
    ensure_analysis_content_owner_candidate_shadow_family,
    ensure_analysis_content_owner_shadow_family,
)

_COMPONENT_STAGE = {
    b"file_hash_decision": (b"validate_file_hash_decision", b"D", 32),
    b"content_owner_candidate": (b"validate_content_owner_candidate", b"G", 8),
    b"content_owner": (b"validate_content_owner", b"D", 32),
    b"gid_candidate": (b"validate_gid_candidate", b"G", 8),
    b"gid_winner": (b"validate_gid_winner", b"I", 8),
}


def seed_content_owner_candidate_shadow(
    connector: Any,
    *,
    analysis_id: bytes,
    gallery_id: int,
    content_sha256: bytes,
    prefer_not_already_uploaded: int,
    title_scalar_count: int,
    download_time: int,
) -> None:
    ensure_analysis_content_owner_candidate_shadow_family(
        connector,
        AnalysisContentOwnerCandidateShadowFamily(
            analysis_id,
            gallery_id,
            content_sha256,
            prefer_not_already_uploaded,
            title_scalar_count,
            download_time,
        ),
    )


def seed_content_owner_shadow(
    connector: Any,
    *,
    analysis_id: bytes,
    content_sha256: bytes,
    owner_gallery_id: int,
) -> None:
    ensure_analysis_content_owner_shadow_family(
        connector,
        AnalysisContentOwnerShadowFamily(
            analysis_id,
            content_sha256,
            owner_gallery_id,
        ),
    )


def _live_cursor(component: bytes, row_count: int) -> bytes:
    _stage, kind, key_size = _COMPONENT_STAGE[component]
    return b"\x01" + kind + b"\x00" + bytes(key_size) + row_count.to_bytes(8, "big")


def seed_analysis_run(
    connector: Any,
    *,
    analysis_id: bytes,
    build_id: bytes,
    policy_id: int,
    input_manifest_sha256: bytes,
    started_at: int,
    state: str = "OPEN",
    completed_at: int | None = None,
) -> None:
    family, created = ensure_analysis_run_family(
        connector,
        analysis_id=analysis_id,
        build_id=build_id,
        policy_id=policy_id,
        input_manifest_sha256=input_manifest_sha256,
        started_at=started_at,
    )
    if not created:
        expected_completed = completed_at if state == "COMPLETE" else None
        if family.state != state or family.completed_at != expected_completed:
            raise AssertionError("analysis run fixture replay differs")
        return
    if state == "OPEN":
        if completed_at is not None:
            raise ValueError("OPEN fixture cannot have completed_at")
        return
    if state == "COMPLETE":
        if completed_at is None:
            raise ValueError("COMPLETE fixture requires completed_at")
        insert_analysis_run_completed_at(
            connector,
            analysis_id=analysis_id,
            completed_at=completed_at,
        )
    elif state == "ABANDONED":
        if completed_at is not None:
            raise ValueError("ABANDONED fixture cannot have completed_at")
    else:
        raise ValueError("analysis fixture state is not registered")
    connector.execute(
        "UPDATE catalog_analysis_run_states SET state = %s "
        "WHERE analysis_id = %s AND state = %s",
        (state, analysis_id, "OPEN"),
    )


def seed_analysis_component(
    connector: Any,
    *,
    analysis_id: bytes,
    state_component: bytes,
    row_count: int,
    sealed_at: int,
    terminal_receipt: bool = False,
) -> None:
    if terminal_receipt:
        stage = _COMPONENT_STAGE[state_component][0]
        cursor = _live_cursor(state_component, row_count)
        checkpoint = connector.fetch_one(
            "SELECT generation FROM catalog_analysis_checkpoint_generations "
            "WHERE analysis_id = %s AND stage = %s",
            (analysis_id, stage),
        )
        if checkpoint:
            generation = int(checkpoint[0])
            connector.execute(
                "UPDATE catalog_analysis_checkpoint_cursors SET cursor = %s "
                "WHERE analysis_id = %s AND stage = %s",
                (cursor, analysis_id, stage),
            )
            connector.execute(
                "UPDATE catalog_analysis_checkpoint_states SET state = %s "
                "WHERE analysis_id = %s AND stage = %s",
                ("COMPLETE", analysis_id, stage),
            )
            connector.execute(
                "UPDATE catalog_analysis_checkpoint_updated_ats SET updated_at = %s "
                "WHERE analysis_id = %s AND stage = %s",
                (sealed_at, analysis_id, stage),
            )
            connector.execute(
                "UPDATE catalog_analysis_checkpoint_generations SET generation = %s "
                "WHERE analysis_id = %s AND stage = %s",
                (generation + 1, analysis_id, stage),
            )
        else:
            generation = 1
            key = (analysis_id, stage)
            connector.execute(
                "INSERT INTO catalog_analysis_checkpoint_anchors "
                "(analysis_id, stage) VALUES (%s, %s)",
                key,
            )
            for table, column, value in (
                ("catalog_analysis_checkpoint_generations", "generation", 2),
                ("catalog_analysis_checkpoint_cursors", "cursor", cursor),
                (
                    "catalog_analysis_checkpoint_processed_counts",
                    "processed_count",
                    0,
                ),
                ("catalog_analysis_checkpoint_states", "state", "COMPLETE"),
                (
                    "catalog_analysis_checkpoint_updated_ats",
                    "updated_at",
                    sealed_at,
                ),
            ):
                connector.execute(
                    f"INSERT INTO {table} (analysis_id, stage, {column}) "
                    "VALUES (%s, %s, %s)",
                    (*key, value),
                )
            connector.execute(
                "INSERT INTO catalog_analysis_checkpoint_seals "
                "(analysis_id, stage) VALUES (%s, %s)",
                key,
            )
        receipt_key = (analysis_id, stage, generation)
        connector.execute(
            "INSERT INTO catalog_analysis_batch_receipt_anchors "
            "(analysis_id, stage, start_generation) VALUES (%s, %s, %s)",
            receipt_key,
        )
        connector.execute(
            "INSERT INTO catalog_analysis_batch_receipt_coordinates "
            "(analysis_id, stage, batch_key, start_generation) "
            "VALUES (%s, %s, %s, %s)",
            (analysis_id, stage, b"fixture-terminal-" + state_component, generation),
        )
        for table, column, value in (
            ("catalog_analysis_batch_receipt_start_cursors", "start_cursor", cursor),
            (
                "catalog_analysis_batch_receipt_start_processed_counts",
                "start_processed_count",
                0,
            ),
            ("catalog_analysis_batch_receipt_page_limits", "page_limit", 128),
            ("catalog_analysis_batch_receipt_next_cursors", "next_cursor", cursor),
            ("catalog_analysis_batch_receipt_row_counts", "row_count", 0),
            (
                "catalog_analysis_batch_receipt_committed_ats",
                "committed_at",
                sealed_at,
            ),
        ):
            connector.execute(
                f"INSERT INTO {table} "
                f"(analysis_id, stage, start_generation, {column}) "
                "VALUES (%s, %s, %s, %s)",
                (*receipt_key, value),
            )
        connector.execute(
            "INSERT INTO catalog_analysis_batch_receipt_seals "
            "(analysis_id, stage, start_generation) VALUES (%s, %s, %s)",
            receipt_key,
        )
    ensure_analysis_state_component_family(
        connector,
        analysis_id=analysis_id,
        state_component=state_component,
        row_count=row_count,
        sealed_at=sealed_at,
    )


def set_analysis_component_live_count(
    connector: Any,
    *,
    analysis_id: bytes,
    state_component: bytes,
    row_count: int,
) -> None:
    """Keep a test component's fact, terminal receipt, and checkpoint exact."""

    stage = _COMPONENT_STAGE[state_component][0]
    cursor = _live_cursor(state_component, row_count)
    connector.execute(
        "UPDATE catalog_analysis_state_component_row_counts SET row_count = %s "
        "WHERE analysis_id = %s AND state_component = %s",
        (row_count, analysis_id, state_component),
    )
    terminal = connector.fetch_one(
        "SELECT start_generation FROM catalog_analysis_batch_receipts "
        "WHERE analysis_id = %s AND stage = %s AND row_count = %s",
        (analysis_id, stage, 0),
    )
    if len(terminal) != 1:
        raise AssertionError("analysis component fixture lacks one terminal receipt")
    receipt_key = (analysis_id, stage, int(terminal[0]))
    for table, column in (
        ("catalog_analysis_batch_receipt_start_cursors", "start_cursor"),
        ("catalog_analysis_batch_receipt_next_cursors", "next_cursor"),
    ):
        connector.execute(
            f"UPDATE {table} SET {column} = %s "
            "WHERE analysis_id = %s AND stage = %s AND start_generation = %s",
            (cursor, *receipt_key),
        )
    connector.execute(
        "UPDATE catalog_analysis_checkpoint_cursors SET cursor = %s "
        "WHERE analysis_id = %s AND stage = %s",
        (cursor, analysis_id, stage),
    )


def set_analysis_component_sealed_at(
    connector: Any,
    *,
    analysis_id: bytes,
    state_component: bytes,
    sealed_at: int,
) -> None:
    """Keep a test component's seal, terminal receipt, and checkpoint exact."""

    stage = _COMPONENT_STAGE[state_component][0]
    terminal = connector.fetch_one(
        "SELECT start_generation FROM catalog_analysis_batch_receipts "
        "WHERE analysis_id = %s AND stage = %s AND row_count = %s",
        (analysis_id, stage, 0),
    )
    if len(terminal) != 1:
        raise AssertionError("analysis component fixture lacks one terminal receipt")
    receipt_key = (analysis_id, stage, int(terminal[0]))
    connector.execute(
        "UPDATE catalog_analysis_state_component_sealed_ats SET sealed_at = %s "
        "WHERE analysis_id = %s AND state_component = %s",
        (sealed_at, analysis_id, state_component),
    )
    connector.execute(
        "UPDATE catalog_analysis_batch_receipt_committed_ats SET committed_at = %s "
        "WHERE analysis_id = %s AND stage = %s AND start_generation = %s",
        (sealed_at, *receipt_key),
    )
    connector.execute(
        "UPDATE catalog_analysis_checkpoint_updated_ats SET updated_at = %s "
        "WHERE analysis_id = %s AND stage = %s",
        (sealed_at, analysis_id, stage),
    )


def complete_analysis_run(
    connector: Any,
    *,
    analysis_id: bytes,
    completed_at: int,
) -> None:
    insert_analysis_run_completed_at(
        connector,
        analysis_id=analysis_id,
        completed_at=completed_at,
    )
    connector.execute(
        "UPDATE catalog_analysis_run_states SET state = %s "
        "WHERE analysis_id = %s AND state = %s",
        ("COMPLETE", analysis_id, "OPEN"),
    )


def seed_analysis_exclusion_delta(
    connector: Any,
    *,
    analysis_id: bytes,
    file_sha256: bytes,
    old_excluded: int,
    new_excluded: int,
) -> None:
    insert_analysis_exclusion_delta_family(
        connector,
        analysis_id=analysis_id,
        file_sha256=file_sha256,
        old_excluded=old_excluded,
        new_excluded=new_excluded,
    )
