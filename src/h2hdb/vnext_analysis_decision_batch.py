"""Bounded scalar storage for one independently evaluated file-decision page.

Only fixed-width facts are batched here. Source aggregation and its independent
validation remain owned by the analysis repository. Every family is joined against a
bounded requested-key grid, so orphan children remain visible without a corpus scan.
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
from .vnext_domains import require_digest32, require_positive_int63, require_uuid16

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


def _require_analysis_layers(analysis_ids: Sequence[bytes]) -> tuple[bytes, ...]:
    if len(analysis_ids) > 17:
        raise ValueError("file-decision layers exceed 17 analyses")
    analyses = tuple(
        require_uuid16(analysis, field="decision layer analysis")
        for analysis in analysis_ids
    )
    if len(set(analyses)) != len(analyses):
        raise ValueError("file-decision layers contain duplicate analyses")
    return tuple(sorted(analyses))


def _require_layer_result_key(
    analysis_id: object,
    digest: object,
    *,
    analyses: tuple[bytes, ...],
    keys: tuple[bytes, ...],
) -> tuple[bytes, bytes]:
    try:
        analysis = require_uuid16(analysis_id, field="decision result analysis")
        key = require_digest32(digest, field="decision result key")
    except (TypeError, ValueError) as error:
        raise AnalysisFamilyCollisionError(
            "file-decision layers returned an invalid key"
        ) from error
    if analysis not in analyses or key not in keys:
        raise AnalysisFamilyCollisionError(
            "file-decision layers returned an unexpected key"
        )
    return analysis, key


def _requested_layer_grid(
    connector: SQLConnector, analyses: tuple[bytes, ...], keys: tuple[bytes, ...]
) -> str:
    analysis_selects = " UNION ALL ".join(
        f"SELECT {connector.binary_parameter_expression(16)}" for _analysis in analyses
    )
    key_selects = " UNION ALL ".join(
        f"SELECT {connector.binary_parameter_expression(32)}" for _key in keys
    )
    return (
        f"WITH requested_analyses(analysis_id) AS ({analysis_selects}), "
        f"requested_hashes(file_sha256) AS ({key_selects}) "
    )


def load_file_decision_shadow_layers(
    connector: SQLConnector,
    *,
    analysis_ids: Sequence[bytes],
    digests: Sequence[bytes],
) -> dict[tuple[bytes, bytes], AnalysisFileHashDecisionShadowFamily]:
    """Read at most 17 layers of 128 keys with one exact-family query.

    A bounded requested-key grid drives physical primary-key point joins, avoiding
    scans of unrelated history even when a backend would scan an IN predicate.
    The grid retains absent rows: an orphan child is distinguishable from a wholly
    absent family. Nearest-layer selection and shadow/tombstone conflicts remain
    the caller's responsibility in the same fenced transaction.
    """

    analyses = _require_analysis_layers(analysis_ids)
    keys = require_file_decision_page_keys(digests)
    if not analyses or not keys:
        return {}
    anchor, occurrence, artist, maximum, seal = _SHADOW_TABLES
    maximum_rows = len(analyses) * len(keys)
    joins = " ".join(
        f"LEFT JOIN {connector.primary_key_table_reference(table)} "
        f"ON {table}.analysis_id = g.analysis_id "
        f"AND {table}.file_sha256 = h.file_sha256"
        for table in _SHADOW_TABLES
    )
    rows = connector.fetch_all(
        _requested_layer_grid(connector, analyses, keys)
        + f"SELECT g.analysis_id, h.file_sha256, {anchor}.analysis_id, "
        f"{occurrence}.analysis_id, {occurrence}.occurrence_count, "
        f"{artist}.analysis_id, {artist}.artist_count, "
        f"{maximum}.analysis_id, {maximum}.maximum_gallery_artist_count, "
        f"{seal}.analysis_id "
        "FROM requested_analyses AS g CROSS JOIN requested_hashes AS h "
        + joins
        + " ORDER BY g.analysis_id, h.file_sha256 LIMIT %s",
        (*analyses, *keys, maximum_rows + 1),
    )
    result: dict[tuple[bytes, bytes], AnalysisFileHashDecisionShadowFamily] = {}
    if len(rows) != maximum_rows:
        raise AnalysisFamilyCollisionError(
            "file-decision shadow layers disagree with their requested grid"
        )
    seen: set[tuple[bytes, bytes]] = set()
    for row in rows:
        if len(row) != 10:
            raise AnalysisFamilyCollisionError(
                "file-decision shadow layers returned an invalid row"
            )
        identity = _require_layer_result_key(
            row[0], row[1], analyses=analyses, keys=keys
        )
        if identity in seen:
            raise AnalysisFamilyCollisionError(
                "file-decision shadow layers returned a duplicate key"
            )
        seen.add(identity)
        if all(value is None for value in row[2:]):
            continue
        analysis, key = identity
        if any(row[index] != analysis for index in (2, 3, 5, 7, 9)):
            raise AnalysisFamilyPartialError("file-decision shadow family is partial")
        try:
            result[identity] = AnalysisFileHashDecisionShadowFamily(
                analysis,
                key,
                require_positive_int63(row[4], field="stored shadow occurrence count"),
                row[6],
                row[8],
            )
        except (TypeError, ValueError) as error:
            raise AnalysisFamilyCollisionError(
                "file-decision shadow contains invalid facts"
            ) from error
    return result


def load_file_decision_tombstone_layers(
    connector: SQLConnector,
    *,
    analysis_ids: Sequence[bytes],
    digests: Sequence[bytes],
) -> frozenset[tuple[bytes, bytes]]:
    """Read tombstones through the same bounded grid and complete physical PK."""

    analyses = _require_analysis_layers(analysis_ids)
    keys = require_file_decision_page_keys(digests)
    if not analyses or not keys:
        return frozenset()
    maximum_rows = len(analyses) * len(keys)
    rows = connector.fetch_all(
        _requested_layer_grid(connector, analyses, keys)
        + f"SELECT g.analysis_id, h.file_sha256, {_TOMBSTONE}.analysis_id "
        "FROM requested_analyses AS g CROSS JOIN requested_hashes AS h "
        f"LEFT JOIN {connector.primary_key_table_reference(_TOMBSTONE)} "
        f"ON {_TOMBSTONE}.analysis_id = g.analysis_id "
        f"AND {_TOMBSTONE}.file_sha256 = h.file_sha256 "
        "ORDER BY g.analysis_id, h.file_sha256 LIMIT %s",
        (*analyses, *keys, maximum_rows + 1),
    )
    result: set[tuple[bytes, bytes]] = set()
    if len(rows) != maximum_rows:
        raise AnalysisFamilyCollisionError(
            "file-decision tombstone layers disagree with their requested grid"
        )
    seen: set[tuple[bytes, bytes]] = set()
    for row in rows:
        if len(row) != 3:
            raise AnalysisFamilyCollisionError(
                "file-decision tombstone layers returned an invalid row"
            )
        identity = _require_layer_result_key(
            row[0], row[1], analyses=analyses, keys=keys
        )
        if identity in seen:
            raise AnalysisFamilyCollisionError(
                "file-decision tombstone layers returned a duplicate key"
            )
        seen.add(identity)
        if row[2] is None:
            continue
        if row[2] != identity[0]:
            raise AnalysisFamilyCollisionError(
                "file-decision tombstone layers returned a mismatched identity"
            )
        result.add(identity)
    return frozenset(result)


def load_file_decision_shadow_page(
    connector: SQLConnector,
    *,
    analysis_id: bytes,
    digests: Sequence[bytes],
) -> dict[bytes, AnalysisFileHashDecisionShadowFamily]:
    return {
        key: family
        for (_analysis, key), family in load_file_decision_shadow_layers(
            connector, analysis_ids=(analysis_id,), digests=digests
        ).items()
    }


def load_file_decision_tombstone_page(
    connector: SQLConnector,
    *,
    analysis_id: bytes,
    digests: Sequence[bytes],
) -> frozenset[bytes]:
    return frozenset(
        key
        for _analysis, key in load_file_decision_tombstone_layers(
            connector, analysis_ids=(analysis_id,), digests=digests
        )
    )


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
