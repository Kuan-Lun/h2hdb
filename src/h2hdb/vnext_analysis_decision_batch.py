"""Bounded scalar storage for one independently evaluated file-decision page.

Only fixed-width facts are batched here. Source aggregation and its independent
validation remain owned by the analysis repository. Every key union is restricted
inside each branch, so orphan children remain visible without a corpus scan.
The caller owns the fenced transaction, including the subsequent checkpoint.
"""

from collections.abc import Sequence

from .sql_connector import SQLConnector
from .vnext_analysis_family import (
    AnalysisExclusionDeltaFamily,
    AnalysisFamilyCollisionError,
    AnalysisFamilyPartialError,
    load_analysis_exclusion_delta_families,
)
from .vnext_analysis_overlay_family import AnalysisFileHashDecisionShadowFamily
from .vnext_domains import require_digest32, require_uuid16

_SHADOW_TABLES = (
    "catalog_a_file_decision_shadow_anchors",
    "catalog_a_file_decision_shadow_occurrences",
    "catalog_a_file_decision_shadow_artists",
    "catalog_a_file_decision_shadow_gallery_artist_max",
    "catalog_a_file_decision_shadow_seals",
)
_TOMBSTONE = "catalog_analysis_file_hash_decision_tombstone"


def require_file_decision_page_keys(digests: Sequence[bytes]) -> tuple[bytes, ...]:
    """Reject oversized input before allocating or querying its key set."""

    if len(digests) > 128:
        raise ValueError("file-decision page exceeds 128 keys")
    exact = tuple(
        require_digest32(digest, field="decision page key") for digest in digests
    )
    if len(set(exact)) != len(exact):
        raise ValueError("file-decision page contains duplicate keys")
    return tuple(sorted(exact))


def load_file_decision_shadow_page(
    connector: SQLConnector,
    *,
    analysis_id: bytes,
    digests: Sequence[bytes],
) -> dict[bytes, AnalysisFileHashDecisionShadowFamily]:
    analysis = require_uuid16(analysis_id, field="decision page analysis")
    keys = require_file_decision_page_keys(digests)
    if not keys:
        return {}
    placeholders = ", ".join("%s" for _key in keys)
    branches = [
        f"SELECT analysis_id, file_sha256 FROM {table} "
        f"WHERE analysis_id = %s AND file_sha256 IN ({placeholders})"
        for table in _SHADOW_TABLES
    ]
    anchor, occurrence, artist, maximum, seal = _SHADOW_TABLES
    rows = connector.fetch_all(
        "WITH family_keys(analysis_id, file_sha256) AS ("
        + " UNION ".join(branches)
        + ") SELECT k.analysis_id, k.file_sha256, a.analysis_id, "
        "o.analysis_id, o.occurrence_count, t.analysis_id, t.artist_count, "
        "m.analysis_id, m.maximum_gallery_artist_count, s.analysis_id "
        "FROM family_keys AS k "
        f"LEFT JOIN {anchor} AS a ON a.analysis_id = k.analysis_id "
        "AND a.file_sha256 = k.file_sha256 "
        f"LEFT JOIN {occurrence} AS o ON o.analysis_id = k.analysis_id "
        "AND o.file_sha256 = k.file_sha256 "
        f"LEFT JOIN {artist} AS t ON t.analysis_id = k.analysis_id "
        "AND t.file_sha256 = k.file_sha256 "
        f"LEFT JOIN {maximum} AS m ON m.analysis_id = k.analysis_id "
        "AND m.file_sha256 = k.file_sha256 "
        f"LEFT JOIN {seal} AS s ON s.analysis_id = k.analysis_id "
        "AND s.file_sha256 = k.file_sha256 ORDER BY k.file_sha256 LIMIT %s",
        (analysis, *keys) * len(_SHADOW_TABLES) + (129,),
    )
    result: dict[bytes, AnalysisFileHashDecisionShadowFamily] = {}
    if len(rows) > len(keys):
        raise AnalysisFamilyCollisionError(
            "file-decision shadow page exceeds its key set"
        )
    for row in rows:
        if (
            len(row) != 10
            or row[0] != analysis
            or row[1] not in keys
            or row[1] in result
        ):
            raise AnalysisFamilyCollisionError(
                "file-decision shadow page returned an unexpected key"
            )
        if any(row[index] != analysis for index in (2, 3, 5, 7, 9)):
            raise AnalysisFamilyPartialError("file-decision shadow family is partial")
        try:
            result[row[1]] = AnalysisFileHashDecisionShadowFamily(
                analysis, row[1], row[4], row[6], row[8]
            )
        except (TypeError, ValueError) as error:
            raise AnalysisFamilyCollisionError(
                "file-decision shadow contains invalid facts"
            ) from error
    return result


def load_file_decision_tombstone_page(
    connector: SQLConnector,
    *,
    analysis_id: bytes,
    digests: Sequence[bytes],
) -> frozenset[bytes]:
    analysis = require_uuid16(analysis_id, field="decision tombstone analysis")
    keys = require_file_decision_page_keys(digests)
    if not keys:
        return frozenset()
    placeholders = ", ".join("%s" for _key in keys)
    rows = connector.fetch_all(
        f"SELECT analysis_id, file_sha256 FROM {_TOMBSTONE} "
        f"WHERE analysis_id = %s AND file_sha256 IN ({placeholders}) "
        "ORDER BY file_sha256 LIMIT %s",
        (analysis, *keys, 129),
    )
    result: set[bytes] = set()
    for row in rows:
        if (
            len(row) != 2
            or row[0] != analysis
            or row[1] not in keys
            or row[1] in result
        ):
            raise AnalysisFamilyCollisionError(
                "file-decision tombstone page returned an unexpected key"
            )
        result.add(row[1])
    return frozenset(result)


def ensure_file_decision_materialization_page(
    connector: SQLConnector,
    *,
    analysis_id: bytes,
    deltas: Sequence[AnalysisExclusionDeltaFamily],
    shadows: Sequence[AnalysisFileHashDecisionShadowFamily],
    tombstones: Sequence[bytes],
) -> None:
    """Exact-preflight every family, then insert missing facts seal-last.

    An identical complete family is idempotent. Any differing or partial family
    fails before DML. No upsert ignores conflicts; concurrent mutation is excluded
    by the surrounding analysis checkpoint lock, and SQL errors roll back the
    entire caller transaction. There is no independently committed child batch.
    """

    analysis = require_uuid16(analysis_id, field="decision materialization analysis")
    if len(deltas) > 128 or len(shadows) > 128 or len(tombstones) > 128:
        raise ValueError("file-decision materialization exceeds 128 keys")
    keys = require_file_decision_page_keys(tuple(delta.file_sha256 for delta in deltas))
    proposed_deltas = {delta.file_sha256: delta for delta in deltas}
    shadow_keys = require_file_decision_page_keys(
        tuple(shadow.file_sha256 for shadow in shadows)
    )
    proposed_shadows = {shadow.file_sha256: shadow for shadow in shadows}
    proposed_tombstones = frozenset(require_file_decision_page_keys(tombstones))
    if (
        any(delta.analysis_id != analysis for delta in deltas)
        or any(shadow.analysis_id != analysis for shadow in shadows)
        or not set(shadow_keys).issubset(keys)
        or not proposed_tombstones.issubset(keys)
        or set(shadow_keys).intersection(proposed_tombstones)
    ):
        raise AnalysisFamilyCollisionError(
            "file-decision materialization has inconsistent keys"
        )
    existing_shadows = load_file_decision_shadow_page(
        connector, analysis_id=analysis, digests=keys
    )
    existing_tombstones = load_file_decision_tombstone_page(
        connector, analysis_id=analysis, digests=keys
    )
    existing_delta_rows = load_analysis_exclusion_delta_families(
        connector, analysis_id=analysis, file_sha256s=keys
    )
    existing_deltas = {delta.file_sha256: delta for delta in existing_delta_rows}
    if (
        len(existing_deltas) != len(existing_delta_rows)
        or any(
            proposed_deltas.get(key) != value for key, value in existing_deltas.items()
        )
        or any(
            proposed_shadows.get(key) != value
            for key, value in existing_shadows.items()
        )
        or not existing_tombstones.issubset(proposed_tombstones)
    ):
        raise AnalysisFamilyCollisionError(
            "file-decision materialization replay changed"
        )

    fresh_deltas = tuple(
        proposed_deltas[key] for key in keys if key not in existing_deltas
    )
    fresh_shadows = tuple(
        proposed_shadows[key] for key in shadow_keys if key not in existing_shadows
    )
    fresh_tombstones = tuple(sorted(proposed_tombstones - existing_tombstones))
    delta_keys = tuple((analysis, delta.file_sha256) for delta in fresh_deltas)
    shadow_rows = tuple((analysis, shadow.file_sha256) for shadow in fresh_shadows)
    # These are the complete, fixed physical families, not caller-selected SQL.
    writes: tuple[tuple[str, str, tuple[tuple[bytes | int, ...], ...]], ...] = (
        (
            "catalog_analysis_exclusion_delta_anchors",
            "analysis_id, file_sha256",
            delta_keys,
        ),
        (
            "catalog_analysis_exclusion_delta_old_excluded_flags",
            "analysis_id, file_sha256, old_excluded",
            tuple(
                (analysis, delta.file_sha256, delta.old_excluded)
                for delta in fresh_deltas
            ),
        ),
        (
            "catalog_analysis_exclusion_delta_new_excluded_flags",
            "analysis_id, file_sha256, new_excluded",
            tuple(
                (analysis, delta.file_sha256, delta.new_excluded)
                for delta in fresh_deltas
            ),
        ),
        (
            "catalog_analysis_exclusion_delta_changes",
            "analysis_id, file_sha256",
            tuple(
                (analysis, delta.file_sha256) for delta in fresh_deltas if delta.changed
            ),
        ),
        (
            "catalog_analysis_exclusion_delta_seals",
            "analysis_id, file_sha256",
            delta_keys,
        ),
        (_SHADOW_TABLES[0], "analysis_id, file_sha256", shadow_rows),
        (
            _SHADOW_TABLES[1],
            "analysis_id, file_sha256, occurrence_count",
            tuple(
                (analysis, shadow.file_sha256, shadow.occurrence_count)
                for shadow in fresh_shadows
            ),
        ),
        (
            _SHADOW_TABLES[2],
            "analysis_id, file_sha256, artist_count",
            tuple(
                (analysis, shadow.file_sha256, shadow.artist_count)
                for shadow in fresh_shadows
            ),
        ),
        (
            _SHADOW_TABLES[3],
            "analysis_id, file_sha256, maximum_gallery_artist_count",
            tuple(
                (analysis, shadow.file_sha256, shadow.maximum_gallery_artist_count)
                for shadow in fresh_shadows
            ),
        ),
        (_SHADOW_TABLES[4], "analysis_id, file_sha256", shadow_rows),
        (
            _TOMBSTONE,
            "analysis_id, file_sha256",
            tuple((analysis, key) for key in fresh_tombstones),
        ),
    )
    for table, columns, rows in writes:
        if rows:
            values = ", ".join(
                "(" + ", ".join("%s" for _value in row) + ")" for row in rows
            )
            connector.execute(
                f"INSERT INTO {table} ({columns}) VALUES {values}",
                tuple(value for row in rows for value in row),
            )
