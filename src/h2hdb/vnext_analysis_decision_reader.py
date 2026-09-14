"""Stream nearest-ancestor file decisions using bounded physical index seeks."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from heapq import merge
from itertools import groupby

from .sql_connector import SQLConnector
from .vnext_analysis_decision_batch import (
    load_file_decision_shadow_page,
    load_file_decision_tombstone_page,
)
from .vnext_analysis_family import AnalysisFamilyCollisionError
from .vnext_analysis_overlay_family import AnalysisFileHashDecisionShadowFamily
from .vnext_domains import require_digest32, require_uuid16

_PAGE_SIZE = 128
_MAX_ANCESTORS = 17
_KEY_TABLES = (
    "catalog_a_file_decision_shadow_anchors",
    "catalog_a_file_decision_shadow_occurrences",
    "catalog_a_file_decision_shadow_artists",
    "catalog_a_file_decision_shadow_gallery_artist_max",
    "catalog_a_file_decision_shadow_seals",
    "catalog_analysis_file_hash_decision_tombstone",
)

type _LayerRow = tuple[bytes, int, AnalysisFileHashDecisionShadowFamily | None]


def iter_resolved_file_decisions(
    connector: SQLConnector, *, ancestry: Sequence[bytes]
) -> Iterator[AnalysisFileHashDecisionShadowFamily]:
    """Merge exact layer pages, choosing the nearest shadow or tombstone.

    The repository supplies durable, validated ancestry in its pinned read
    transaction. Each layer retains at most 128 keys; neither the full resolved
    view nor a growing set of already-seen keys is materialized. Including every
    physical child in key discovery makes incomplete families fail closed.
    """

    layers = tuple(
        require_uuid16(value, field="decision ancestor") for value in ancestry
    )
    if not layers or len(layers) > _MAX_ANCESTORS or len(set(layers)) != len(layers):
        raise ValueError("decision ancestry must contain 1..17 unique layers")
    ordered = merge(
        *(
            _iter_layer(connector, analysis_id=analysis, depth=depth)
            for depth, analysis in enumerate(layers)
        ),
        key=lambda row: (row[0], row[1]),
    )
    for _digest, matches in groupby(ordered, key=lambda row: row[0]):
        _key, _depth, nearest = next(matches)
        if nearest is not None:
            yield nearest


def _iter_layer(
    connector: SQLConnector, *, analysis_id: bytes, depth: int
) -> Iterator[_LayerRow]:
    after: bytes | None = None
    while keys := _layer_key_page(connector, analysis_id=analysis_id, after=after):
        shadows = load_file_decision_shadow_page(
            connector, analysis_id=analysis_id, digests=keys
        )
        tombstones = load_file_decision_tombstone_page(
            connector, analysis_id=analysis_id, digests=keys
        )
        if shadows.keys() & tombstones or shadows.keys() | tombstones != set(keys):
            raise AnalysisFamilyCollisionError(
                "decision layer has conflicting or absent shadow/tombstone facts"
            )
        for key in keys:
            yield key, depth, shadows.get(key)
        if len(keys) < _PAGE_SIZE:
            return
        after = keys[-1]


def _layer_key_page(
    connector: SQLConnector, *, analysis_id: bytes, after: bytes | None
) -> tuple[bytes, ...]:
    # LIMIT belongs inside each physical-table branch. A key outside any
    # branch's prefix already has at least PAGE_SIZE union predecessors. The
    # composite PK can seek (analysis_id, file_sha256) before any view joins.
    predicate = "" if after is None else " AND file_sha256 > %s"
    branches: list[str] = []
    parameters: list[bytes | int] = []
    for table in _KEY_TABLES:
        branches.append(
            "SELECT file_sha256 FROM (SELECT file_sha256 "
            f"FROM {table} WHERE analysis_id = %s{predicate} "
            "ORDER BY file_sha256 LIMIT %s) AS layer_prefix"
        )
        parameters.append(analysis_id)
        if after is not None:
            parameters.append(after)
        parameters.append(_PAGE_SIZE)
    rows = connector.fetch_all(
        "SELECT file_sha256 FROM ("
        + " UNION ".join(branches)
        + ") AS layer_keys ORDER BY file_sha256 LIMIT %s",
        (*parameters, _PAGE_SIZE),
    )
    keys: list[bytes] = []
    previous = after
    for row in rows:
        if len(row) != 1 or len(keys) == _PAGE_SIZE:
            raise AnalysisFamilyCollisionError(
                "decision layer key page has invalid shape"
            )
        key = require_digest32(row[0], field="decision layer key")
        if previous is not None and key <= previous:
            raise AnalysisFamilyCollisionError("decision layer keys did not advance")
        keys.append(key)
        previous = key
    return tuple(keys)
