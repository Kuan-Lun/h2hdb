"""Independent SQL audits of durable source collections and staging ownership.

These full audits use bounded result sets, not bounded total SQL work. Temporal
fencing and retention during deletion remain repository writer obligations.
"""

from __future__ import annotations

from dataclasses import dataclass

from .schema_epoch import SchemaEpochValidationError
from .sql_connector import SQLConnector


@dataclass(slots=True)
class _CleanupAudit:
    """One validator invocation's proof; never retained across read transactions."""

    connector: SQLConnector
    validated: bool = False

    def ensure(self) -> None:
        if self.validated:
            return
        from .operational_refinement import (
            check_cleanup_frozen_root_set_v1,
            check_cleanup_reachability_v1,
        )

        check_cleanup_reachability_v1(self.connector)
        check_cleanup_frozen_root_set_v1(self.connector)
        self.validated = True


def _reject(connector: SQLConnector, query: str, message: str) -> None:
    if connector.fetch_one(query + " LIMIT 1"):
        raise SchemaEpochValidationError(message)


def _require_retirement(
    connector: SQLConnector,
    collection_id: bytes,
    phase: str,
    relation_index: int,
    audit: _CleanupAudit,
) -> None:
    # Reuse the independent cleanup receipt/frozen-set audit, never its writer's
    # eligibility decision. A missing scalar needs an exact committed frontier.
    audit.ensure()
    frame = b"\x01\x01b\x00\x10" + collection_id
    rows = connector.fetch_all(
        "SELECT j.cleanup_id, p.state, p.cursor_bytes FROM operational_cleanup_jobs j "
        "JOIN operational_cleanup_sweep_targets t ON t.target_key = j.target_key "
        "JOIN operational_cleanup_cycle_roots r ON r.cleanup_id = j.cleanup_id "
        "JOIN operational_cleanup_checkpoints p ON p.cleanup_id = j.cleanup_id "
        "WHERE j.state = 'OPEN' AND t.target_kind = 'SOURCE_COLLECTION' "
        "AND r.frozen_root_key = %s AND p.phase = %s "
        "AND NOT EXISTS (SELECT 1 FROM operational_source_working_collections w WHERE w.collection_id = %s) "
        "AND NOT EXISTS (SELECT 1 FROM operational_gallery_staging_collections g WHERE g.collection_id = %s) LIMIT 2",
        (frame, phase, collection_id, collection_id),
    )
    if len(rows) != 1:
        raise SchemaEpochValidationError(
            "Collection gap has no exact cleanup authority"
        )
    cleanup_id, state, raw_cursor = rows[0]
    if state == "COMPLETE":
        return
    cursor = bytes(raw_cursor)
    if state != "OPEN" or len(cursor) < 4 or cursor[0] != 1:
        raise SchemaEpochValidationError("Collection cleanup frontier is absent")
    index = int.from_bytes(cursor[1:3], "big")
    max_index = 2 if phase == "SC_METADATA" else 1 if phase == "SC_MEMBERS" else 0
    if index > max_index or index < relation_index:
        raise SchemaEpochValidationError(
            "Collection gap is ahead of its cleanup relation"
        )
    if index > relation_index:
        return
    if (
        len(cursor) != 42
        or cursor[3:7] != b"\x02b\x00\x10"
        or cursor[23:26] != b"b\x00\x10"
        or cursor[7:23] != cursor[26:42]
    ):
        raise SchemaEpochValidationError(
            "Collection cleanup cursor is not an exact scalar frontier"
        )
    root = cursor[7:23]
    if root < collection_id or connector.fetch_one(
        "SELECT 1 FROM operational_cleanup_cycle_roots WHERE cleanup_id = %s AND frozen_root_key = %s",
        (cleanup_id, b"\x01\x01b\x00\x10" + root),
    ) != (1,):
        raise SchemaEpochValidationError(
            "Collection gap is ahead of its cleanup cursor"
        )


def check_source_collection_durable_observations_v1(connector: SQLConnector) -> None:
    """Require total collection descriptors and exact sealed member policies."""
    audit = _CleanupAudit(connector)
    last = b""
    while True:
        rows = connector.fetch_all(
            "SELECT c.collection_id, m.collection_id, q.collection_id, t.collection_id, s.collection_id, k.collection_id, k.updated_at, t.created_at "
            "FROM catalog_source_collections c "
            "LEFT JOIN catalog_source_collection_manifest_policies m ON m.collection_id = c.collection_id "
            "LEFT JOIN catalog_source_collection_qualification_policies q ON q.collection_id = c.collection_id "
            "LEFT JOIN catalog_source_collection_created_ats t ON t.collection_id = c.collection_id "
            "LEFT JOIN operational_source_collection_states s ON s.collection_id = c.collection_id "
            "LEFT JOIN operational_source_collection_claims k ON k.collection_id = c.collection_id "
            "WHERE c.collection_id > %s ORDER BY c.collection_id LIMIT 128",
            (last,),
        )
        if not rows:
            break
        for row in rows:
            collection_id = bytes(row[0])
            for value, phase, index in zip(
                row[1:6],
                ("SC_METADATA", "SC_METADATA", "SC_METADATA", "SC_STATE", "SC_CLAIM"),
                (2, 1, 0, 0, 0),
                strict=True,
            ):
                if value is None:
                    _require_retirement(connector, collection_id, phase, index, audit)
            if row[6] is not None and row[7] is not None and row[6] < row[7]:
                raise SchemaEpochValidationError(
                    "Collection claim precedes its creation"
                )
        last = bytes(rows[-1][0])
    _reject(
        connector,
        "SELECT 1 FROM catalog_source_collection_observations r "
        "JOIN catalog_source_collections c ON c.collection_id = r.collection_id "
        "JOIN catalog_source_collection_qualification_policies q ON q.collection_id = c.collection_id "
        "LEFT JOIN catalog_gallery_identities g ON g.gallery_id = r.gallery_id "
        "LEFT JOIN catalog_gallery_observations o ON o.gallery_id = r.gallery_id AND o.observation_id = r.observation_id "
        "LEFT JOIN catalog_gallery_observation_validation_policies v ON v.gallery_id = r.gallery_id AND v.observation_id = r.observation_id "
        "WHERE g.gallery_id IS NULL OR o.gallery_id IS NULL OR v.gallery_id IS NULL "
        "OR g.scope_key <> c.scope_key OR v.qualification_policy_sha256 <> q.qualification_policy_sha256",
        "Source collection member is unsealed or has a different scope or qualification policy",
    )

    _reject(
        connector,
        "SELECT 1 FROM catalog_source_collection_observations r "
        "JOIN catalog_source_collection_manifest_policies p ON p.collection_id = r.collection_id "
        "LEFT JOIN catalog_gallery_manifests m ON m.gallery_id = r.gallery_id AND m.observation_id = r.observation_id AND m.manifest_policy_id = p.manifest_policy_id "
        "WHERE m.gallery_id IS NULL",
        "Source collection member lacks its exact manifest policy",
    )


def check_source_collection_consumption_fencing_v1(connector: SQLConnector) -> None:
    """Require exact terminal consumption and working-root state correspondence."""
    _reject(
        connector,
        "SELECT 1 FROM operational_source_collection_states s "
        "LEFT JOIN catalog_source_collection_consumptions x ON x.collection_id = s.collection_id "
        "LEFT JOIN operational_source_working_collections w ON w.collection_id = s.collection_id "
        "JOIN catalog_source_collection_created_ats t ON t.collection_id = s.collection_id "
        "WHERE (s.state <> 'CONSUMED' AND x.collection_id IS NOT NULL) "
        "OR (s.state = 'OPEN' AND w.collection_id IS NULL) "
        "OR (s.state <> 'OPEN' AND w.collection_id IS NOT NULL) "
        "OR (w.collection_id IS NOT NULL AND w.assigned_at <> t.created_at)",
        "Source collection consumption or working root disagrees with its state",
    )
    audit = _CleanupAudit(connector)
    last = b""
    while True:
        rows = connector.fetch_all(
            "SELECT s.collection_id FROM operational_source_collection_states s "
            "LEFT JOIN catalog_source_collection_consumptions x ON x.collection_id = s.collection_id "
            "WHERE s.state = 'CONSUMED' AND x.collection_id IS NULL "
            "AND s.collection_id > %s ORDER BY s.collection_id LIMIT 128",
            (last,),
        )
        if not rows:
            break
        for row in rows:
            _require_retirement(connector, bytes(row[0]), "SC_MEMBERS", 0, audit)
        last = bytes(rows[-1][0])
    _reject(
        connector,
        "SELECT 1 FROM catalog_source_collection_consumptions x "
        "JOIN catalog_source_collections c ON c.collection_id = x.collection_id "
        "JOIN catalog_source_collection_manifest_policies m ON m.collection_id = c.collection_id "
        "LEFT JOIN catalog_source_builds b ON b.build_id = x.build_id "
        "WHERE b.build_id IS NULL OR b.state <> 'SEALED' OR b.scope_key <> c.scope_key "
        "OR b.manifest_policy_id <> m.manifest_policy_id",
        "Source collection consumption is not its exact sealed source policy",
    )


def check_source_collection_staging_owner_v1(connector: SQLConnector) -> None:
    """Each shared staging header has one compatible source or collection owner."""
    _reject(
        connector,
        "SELECT 1 FROM operational_gallery_observation_stagings s "
        "LEFT JOIN operational_gallery_staging_source_builds b ON b.staging_id = s.staging_id "
        "LEFT JOIN operational_gallery_staging_collections c ON c.staging_id = s.staging_id "
        "WHERE (b.staging_id IS NULL AND c.staging_id IS NULL) "
        "OR (b.staging_id IS NOT NULL AND c.staging_id IS NOT NULL)",
        "Shared gallery staging must have exactly one owner",
    )
    _reject(
        connector,
        "SELECT 1 FROM operational_gallery_staging_collections o "
        "JOIN operational_gallery_observation_stagings s ON s.staging_id = o.staging_id "
        "JOIN catalog_source_collections c ON c.collection_id = o.collection_id "
        "JOIN operational_source_collection_states t ON t.collection_id = c.collection_id "
        "JOIN operational_source_collection_claims k ON k.collection_id = c.collection_id "
        "LEFT JOIN operational_gallery_observation_staging_claims q ON q.staging_id = s.staging_id "
        "JOIN catalog_gallery_identities g ON g.gallery_id = s.gallery_id "
        "WHERE g.scope_key <> c.scope_key "
        "OR q.ingest_generation > k.ingest_generation OR t.state = 'CONSUMED'",
        "Collection staging scope or generation disagrees with its owner",
    )

    audit = _CleanupAudit(connector)
    last = b""
    while True:
        rows = connector.fetch_all(
            "SELECT s.staging_id, s.state FROM operational_gallery_staging_collections o "
            "JOIN operational_gallery_observation_stagings s ON s.staging_id = o.staging_id "
            "LEFT JOIN operational_gallery_observation_staging_claims q ON q.staging_id = s.staging_id "
            "WHERE q.staging_id IS NULL AND s.staging_id > %s ORDER BY s.staging_id LIMIT 128",
            (last,),
        )
        if not rows:
            break
        for row in rows:
            _require_staging_claim_retirement(
                connector, bytes(row[0]), str(row[1]), audit
            )
        last = bytes(rows[-1][0])


def _require_staging_claim_retirement(
    connector: SQLConnector, staging_id: bytes, state: str, audit: _CleanupAudit
) -> None:
    # In-band retirement carries its ACK in the header and deletes CLAIM last.
    # All other directly owned children must already be absent at this boundary.
    children = (
        "operational_gallery_observation_staging_checkpoints",
        "operational_gallery_observation_staging_requests",
        "operational_gallery_observation_staging_match_checkpoints",
        "operational_gallery_observation_staging_metadata_parsers",
    )
    if state in {"RETIRING_SEALED", "RETIRING_REUSED", "ABANDONED"} and all(
        not connector.fetch_one(
            f"SELECT 1 FROM {table} WHERE staging_id = %s LIMIT 1", (staging_id,)
        )
        for table in children
    ):
        return
    audit.ensure()
    rows = connector.fetch_all(
        "SELECT p.state, p.cursor_bytes, j.cleanup_id FROM operational_cleanup_jobs j "
        "JOIN operational_cleanup_sweep_targets t ON t.target_key = j.target_key "
        "JOIN operational_cleanup_cycle_roots r ON r.cleanup_id = j.cleanup_id "
        "JOIN operational_cleanup_checkpoints p ON p.cleanup_id = j.cleanup_id "
        "WHERE j.state = 'OPEN' AND t.target_kind = 'GALLERY_OBSERVATION_STAGING' "
        "AND r.frozen_root_key = %s AND p.phase = 'GOS_CLAIM' LIMIT 2",
        (b"\x01\x01b\x00\x10" + staging_id,),
    )
    if len(rows) != 1 or state not in {
        "SEALED",
        "REUSED",
        "RETIRING_SEALED",
        "RETIRING_REUSED",
    }:
        raise SchemaEpochValidationError(
            "Collection staging claim has no exact retirement authority"
        )
    phase_state, raw_cursor, cleanup_id = rows[0]
    if phase_state == "COMPLETE":
        return
    cursor = bytes(raw_cursor)
    if (
        phase_state != "OPEN"
        or len(cursor) != 42
        or cursor[:7] != b"\x01\x00\x00\x02b\x00\x10"
        or cursor[23:26] != b"b\x00\x10"
        or cursor[7:23] != cursor[26:42]
        or cursor[7:23] < staging_id
    ):
        raise SchemaEpochValidationError(
            "Collection staging claim is ahead of retirement"
        )
    if connector.fetch_one(
        "SELECT 1 FROM operational_cleanup_cycle_roots WHERE cleanup_id = %s AND frozen_root_key = %s",
        (cleanup_id, b"\x01\x01b\x00\x10" + cursor[7:23]),
    ) != (1,):
        raise SchemaEpochValidationError(
            "Collection staging claim frontier is outside frozen roots"
        )


def check_source_collection_cleanup_reachability_v1(connector: SQLConnector) -> None:
    """Audit retained roots; historical deletion safety belongs to cleanup writers."""
    _reject(
        connector,
        "SELECT 1 FROM operational_source_working_collections w "
        "LEFT JOIN catalog_source_collections c ON c.collection_id = w.collection_id "
        "LEFT JOIN operational_source_collection_states s ON s.collection_id = w.collection_id "
        "WHERE c.collection_id IS NULL OR s.state IS NULL OR s.state <> 'OPEN'",
        "Working source collection is not retained and open",
    )
    _reject(
        connector,
        "SELECT 1 FROM catalog_source_collection_observations r "
        "LEFT JOIN catalog_source_collections c ON c.collection_id = r.collection_id "
        "LEFT JOIN catalog_gallery_observations o ON o.gallery_id = r.gallery_id AND o.observation_id = r.observation_id "
        "WHERE c.collection_id IS NULL OR o.gallery_id IS NULL",
        "A retained source collection observation lost its durable authority",
    )
