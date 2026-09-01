"""Build and measure a fresh SQL-backed synthetic catalog.

This developer benchmark deliberately lives outside ``src/h2hdb`` and is not
included in the wheel.  Production family protocols own every family for
which a standalone writer exists.  Relations without such a standalone
writer are populated through :class:`_ManifestBoundWriter`, which rejects any
table or column absent from the wheel-resident generated schema manifest.

The fixture is a catalog-read scalability input, not a replay of the complete
source/analysis/ingest state machine.  A full READY audit nevertheless has to
accept the resulting immutable current revision before a receipt is emitted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import time
import tomllib
import tracemalloc
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any
from unicodedata import unidata_version

import h2hdb.vnext_identity as identity
from h2hdb import (
    CatalogDiscoveryBundle,
    CatalogDiscoveryQuery,
    CatalogFacetKind,
    CoreConfig,
    DatabaseConfig,
    VNextCatalogFacade,
    VNextDatabaseAdminFacade,
)
from h2hdb.catalog_search import iter_search_lexemes
from h2hdb.config_loader import DatabaseAccessMode
from h2hdb.repository import RepositoryContext
from h2hdb.sql_connector import SQLConnector
from h2hdb.sqlite_connector import SQLiteConnector
from h2hdb.vnext_artifact_family import (
    ArtifactSemanticInputFamily,
    CatalogArtifactFamily,
    ensure_artifact_semantic_input_family,
    ensure_catalog_artifact_family,
)
from h2hdb.vnext_canonical_value_family import (
    CanonicalValuePageFamily,
    ensure_allocation_family,
    ensure_canonical_value_identity,
    ensure_exact_page_parent_edges,
    ensure_page_family,
)
from h2hdb.vnext_catalog_identity_family import (
    GalleryIdentity,
    TagTerm,
    ensure_gallery_identity,
    ensure_tag_term,
)
from h2hdb.vnext_catalog_registry_repository import ensure_source_scope
from h2hdb.vnext_publication_family import (
    CatalogContributorFamily,
    CatalogPublicationDownloadTimeFamily,
    CatalogPublicationFamily,
    CatalogPublicationTitleFamily,
    PublicationIdentityFamily,
    ensure_catalog_contributor_family,
    ensure_catalog_publication_download_time_family,
    ensure_catalog_publication_family,
    ensure_catalog_publication_title_family,
    ensure_publication_identity_family,
)
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider

FIXTURE_MODE = "manifest-bound-sql"
DEFAULT_SEED = 0x48324844425F5343
SMOKE_PUBLICATION_COUNT = 165
MANUAL_PUBLICATION_COUNT = 10_000
REVISION = 1
SOURCE_REVISION = 1
GENERATION = 1
SEARCH_QUERY = "needle"
SEARCH_MATCH_MODULUS = 5
PAGE_LIMIT = 32
FACET_LIMIT = 128
_GID_BASE = 1_000_000
_BASE_TIMESTAMP = 1_800_000_000_000_000
_MEDIA_TYPE = b"application/vnd.comicbook+zip"
_ADAPTER_ID = b"synthetic-benchmark"
_POLICY_FINGERPRINT = hashlib.sha256(b"h2hdb synthetic benchmark policy").digest()
_EMPTY_EVENT_CHAIN = hashlib.sha256(b"h2hdb-operational-event-chain-v1\0").digest()
_LANGUAGES = ("en", "es", "ja", "ko", "und", "zh-Hans", "zh-Hant")
_SUBJECTS = tuple(f"Subject {index:02d}" for index in range(12))
_CONTRIBUTORS = tuple(f"Contributor {index:02d}" for index in range(8))
_SUBJECT_NAMESPACE = b"genre"
_CONTRIBUTOR_ROLE = b"author"
_REGULAR_TITLE = "Synthetic Catalog Publication"
_MATCHING_TITLE = "Synthetic Needle Publication"
_SUMMARY = "Synthetic SQL-backed catalog scalability fixture"
_PRODUCTION_FAMILY_BINDINGS = (
    "ensure_allocation_family",
    "ensure_page_family",
    "ensure_canonical_value_identity",
    "ensure_source_scope",
    "ensure_gallery_identity",
    "ensure_publication_identity_family",
    "ensure_catalog_publication_family",
    "ensure_catalog_publication_download_time_family",
    "ensure_catalog_publication_title_family",
    "ensure_catalog_contributor_family",
    "ensure_tag_term",
    "ensure_artifact_semantic_input_family",
    "ensure_catalog_artifact_family",
)


@dataclass(frozen=True, slots=True)
class _SyntheticAssignment:
    position: int
    gid: int
    matched: bool
    title: str
    language: str
    subject: str
    contributor: str


@dataclass(slots=True)
class _ReadCounters:
    connections: int = 0
    read_transactions: int = 0
    logical_queries: int = 0
    query_shapes: dict[str, dict[str, Any]] = field(default_factory=dict)

    def reset(self) -> None:
        self.connections = 0
        self.read_transactions = 0
        self.logical_queries = 0
        self.query_shapes = {}

    def record_query(self, query: str) -> None:
        self.logical_queries += 1
        normalized = " ".join(query.split())
        digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
        existing = self.query_shapes.get(digest)
        if existing is None:
            self.query_shapes[digest] = {
                "sql_sha256": digest,
                "count": 1,
                "query_class": _query_class(normalized),
                "normalized_sql": normalized,
            }
        else:
            existing["count"] += 1

    def shape_summary(self) -> list[dict[str, Any]]:
        return sorted(
            (dict(value) for value in self.query_shapes.values()),
            key=lambda value: (-value["count"], value["sql_sha256"]),
        )

    def class_summary(self) -> dict[str, int]:
        result: dict[str, int] = {}
        for shape in self.query_shapes.values():
            label = str(shape["query_class"])
            result[label] = result.get(label, 0) + int(shape["count"])
        return dict(sorted(result.items()))


def _query_class(normalized_sql: str) -> str:
    if "catalog_canonical_value_allocation_anchors" in normalized_sql:
        return "canonical_identity"
    if normalized_sql.startswith("WITH family_keys(page_sha256)"):
        return "canonical_page_family"
    if "FROM catalog_canonical_value_page_parents" in normalized_sql:
        return "canonical_parent_edges"
    if "FROM catalog_channel_registry AS registry" in normalized_sql:
        return "catalog_head"
    if normalized_sql.startswith("SELECT policy_id FROM catalog_discovery_seals"):
        return "discovery_seal"
    if "catalog_search_postings" in normalized_sql:
        return "search_or_filtered_facet"
    return "other"


class _CountingReadOnlySQLiteConnector(SQLiteConnector):
    """Count facade-visible SQL without changing production connector code."""

    def __init__(self, database: str, counters: _ReadCounters) -> None:
        super().__init__(database=database, read_only=True)
        self._counters = counters

    def connect(self) -> None:
        self._counters.connections += 1
        super().connect()

    def begin_read(self) -> None:
        self._counters.read_transactions += 1
        super().begin_read()

    def fetch_one(
        self,
        query: str,
        data: tuple[Any, ...] = (),
    ) -> tuple[Any, ...]:
        self._counters.record_query(query)
        return super().fetch_one(query, data)

    def fetch_all(
        self,
        query: str,
        data: tuple[Any, ...] = (),
    ) -> list[tuple[Any, ...]]:
        self._counters.record_query(query)
        return super().fetch_all(query, data)


class _ManifestBoundWriter:
    """Allow benchmark INSERTs only into exact generated manifest columns."""

    def __init__(self) -> None:
        provider = GeneratedVNextSchemaProvider("sqlite")
        relations = provider.generated_definition_data.get("relations")
        if not isinstance(relations, tuple):
            raise RuntimeError("generated SQLite manifest relations are unavailable")
        columns_by_table: dict[str, frozenset[str]] = {}
        for relation in relations:
            if not isinstance(relation, dict) or relation.get("kind") != "table":
                continue
            table = relation.get("table")
            columns = relation.get("columns")
            if not isinstance(table, str) or not isinstance(columns, tuple):
                raise RuntimeError("generated SQLite relation metadata is malformed")
            physical_columns: set[str] = set()
            for column in columns:
                if (
                    not isinstance(column, tuple)
                    or len(column) != 5
                    or not isinstance(column[1], str)
                ):
                    raise RuntimeError(
                        f"generated SQLite columns for {table!r} are malformed"
                    )
                physical_columns.add(column[1])
            columns_by_table[table] = frozenset(physical_columns)
        self._columns_by_table = columns_by_table
        self.used_tables: set[str] = set()

    def insert(
        self,
        connector: SQLConnector,
        table: str,
        columns: tuple[str, ...],
        values: tuple[Any, ...],
    ) -> None:
        if len(columns) != len(values) or not columns:
            raise ValueError("manifest-bound INSERT columns and values differ")
        admitted = self._columns_by_table.get(table)
        if admitted is None:
            raise RuntimeError(f"benchmark table {table!r} is absent from the manifest")
        if len(set(columns)) != len(columns) or not set(columns) <= admitted:
            raise RuntimeError(
                f"benchmark columns for {table!r} are absent or duplicated"
            )
        self.used_tables.add(table)
        placeholders = ", ".join("%s" for _ in columns)
        column_sql = ", ".join(columns)
        connector.execute(
            f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})",
            values,
        )


def _config(path: Path, *, read_only: bool = False) -> CoreConfig:
    return CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite",
            database=str(path),
            access_mode=(
                DatabaseAccessMode.read_only
                if read_only
                else DatabaseAccessMode.read_write
            ),
        )
    )


def _assignment(position: int, seed: int) -> _SyntheticAssignment:
    digest = hashlib.sha256(
        seed.to_bytes(8, "big", signed=False) + position.to_bytes(8, "big")
    ).digest()
    matched = (position + seed) % SEARCH_MATCH_MODULUS == 0
    return _SyntheticAssignment(
        position=position,
        gid=_GID_BASE + position + 1,
        matched=matched,
        title=_MATCHING_TITLE if matched else _REGULAR_TITLE,
        language=_LANGUAGES[digest[0] % len(_LANGUAGES)],
        subject=_SUBJECTS[digest[1] % len(_SUBJECTS)],
        contributor=_CONTRIBUTORS[digest[2] % len(_CONTRIBUTORS)],
    )


def _persist_canonical_value(
    connector: SQLConnector,
    *,
    domain: str,
    payload: bytes,
    allocated_at: int,
) -> bytes:
    value_sha256 = identity.canonical_value_digest(domain, payload)
    tree = identity.build_canonical_value_tree(
        value_sha256,
        len(payload),
        (payload,),
    )
    ensure_allocation_family(
        connector,
        value_sha256=value_sha256,
        digest_domain=domain.encode("ascii"),
        byte_count=len(payload),
        allocated_at=allocated_at,
    )
    for encoded in tree.pages:
        receipt = ensure_page_family(
            connector,
            page=CanonicalValuePageFamily.from_payload(
                page_sha256=encoded.page_sha256,
                page_bytes=encoded.page_bytes,
            ),
        )
        ensure_exact_page_parent_edges(connector, receipt=receipt)
    ensure_canonical_value_identity(
        connector,
        value_sha256=value_sha256,
        root_page_sha256=tree.root_page_sha256,
    )
    return value_sha256


def _seed_catalog_policies(
    connector: SQLConnector,
    writer: _ManifestBoundWriter,
    *,
    allocated_at: int,
) -> bytes:
    writer.insert(
        connector,
        "catalog_manifest_policies",
        (
            "manifest_policy_id",
            "manifest_algorithm_version",
            "file_order_version",
        ),
        (1, 1, 1),
    )
    writer.insert(
        connector,
        "catalog_analysis_policies",
        (
            "policy_id",
            "algorithm_version",
            "spam_artist_threshold",
            "spam_occurrence_threshold",
            "content_owner_rule_version",
            "gid_winner_rule_version",
        ),
        (1, 1, 1, 1, 1, 1),
    )
    writer.insert(
        connector,
        "catalog_title_sort_policy",
        (
            "title_sort_policy_id",
            "title_sort_algorithm_version",
            "unicode_data_version",
        ),
        (1, 1, unidata_version.encode("ascii")),
    )
    writer.insert(
        connector,
        "catalog_display_title_policies",
        (
            "display_title_policy_id",
            "display_title_algorithm_version",
            "title_sort_policy_id",
        ),
        (1, 1, 1),
    )
    policy_payload = identity.encode_artifact_policy(
        2,
        _ADAPTER_ID,
        _POLICY_FINGERPRINT,
    )
    policy_component = _persist_canonical_value(
        connector,
        domain="artifact_policy_v3",
        payload=policy_payload,
        allocated_at=allocated_at,
    )
    if policy_component != identity.artifact_policy_digest(
        2,
        _ADAPTER_ID,
        _POLICY_FINGERPRINT,
    ):
        raise RuntimeError("synthetic artifact policy digest is noncongruent")
    writer.insert(
        connector,
        "catalog_artifact_adapter_policy",
        ("policy_fingerprint_sha256", "adapter_id"),
        (_POLICY_FINGERPRINT, _ADAPTER_ID),
    )
    writer.insert(
        connector,
        "catalog_artifact_policy_semantics",
        (
            "policy_component_sha256",
            "artifact_algorithm_version",
            "policy_fingerprint_sha256",
        ),
        (policy_component, 2, _POLICY_FINGERPRINT),
    )
    writer.insert(
        connector,
        "catalog_artifact_policies",
        ("artifact_policy_id", "policy_component_sha256"),
        (1, policy_component),
    )
    return policy_component


def _seed_commit_head(
    connector: SQLConnector,
    writer: _ManifestBoundWriter,
    *,
    publication_count: int,
    seed: int,
    committed_at: int,
) -> tuple[bytes, bytes]:
    receipt_id = hashlib.sha256(
        b"h2hdb benchmark receipt\0" + seed.to_bytes(8, "big")
    ).digest()[:16]
    candidate_id = hashlib.sha256(
        b"h2hdb benchmark candidate\0" + seed.to_bytes(8, "big")
    ).digest()[:16]
    preparation_id = hashlib.sha256(
        b"h2hdb benchmark preparation\0" + seed.to_bytes(8, "big")
    ).digest()[:16]
    writer.insert(
        connector,
        "catalog_revision_descriptors",
        ("revision", "publication_count", "artifact_count"),
        (REVISION, publication_count, publication_count),
    )
    writer.insert(
        connector,
        "catalog_publication_generation_nodes",
        ("generation",),
        (GENERATION,),
    )
    writer.insert(
        connector,
        "catalog_publication_generation_successors",
        ("successor_generation", "predecessor_generation"),
        (GENERATION, GENERATION - 1),
    )
    writer.insert(
        connector,
        "operational_operational_policys",
        (
            "operational_policy_id",
            "operational_schema_version",
            "algorithm_version",
            "max_batch_rows",
        ),
        (1, 1, 1, 128),
    )
    writer.insert(
        connector,
        "operational_operational_event_streams",
        ("preparation_id", "created_at"),
        (preparation_id, committed_at - 2),
    )
    writer.insert(
        connector,
        "operational_operational_preparation_effect_seals",
        ("preparation_id", "event_count", "final_chain_sha256", "sealed_at"),
        (preparation_id, 0, _EMPTY_EVENT_CHAIN, committed_at - 1),
    )
    writer.insert(
        connector,
        "catalog_publication_commit_anchors",
        ("receipt_id",),
        (receipt_id,),
    )
    finalized_at = committed_at + 1
    writer.insert(
        connector,
        "catalog_publication_finalization_checkpoints",
        (
            "receipt_id",
            "generation",
            "cursor",
            "processed_count",
            "state",
            "updated_at",
        ),
        (receipt_id, 2, b"", 0, "COMPLETE", finalized_at),
    )
    writer.insert(
        connector,
        "catalog_publication_commits",
        (
            "receipt_id",
            "candidate_id",
            "revision",
            "source_revision",
            "generation",
            "preparation_id",
            "operational_policy_id",
            "artifact_policy_id",
            "display_title_policy_id",
            "new_galleries",
            "changed_galleries",
            "removed_galleries",
            "duplicate_losers",
            "committed_at",
        ),
        (
            receipt_id,
            candidate_id,
            REVISION,
            SOURCE_REVISION,
            GENERATION,
            preparation_id,
            1,
            1,
            1,
            publication_count,
            0,
            0,
            0,
            committed_at,
        ),
    )
    writer.insert(
        connector,
        "catalog_publication_finalization_batch_stored",
        (
            "receipt_id",
            "start_generation",
            "batch_key",
            "start_cursor",
            "start_processed_count",
            "next_cursor",
            "row_count",
            "committed_at",
        ),
        (receipt_id, 1, b"terminal", b"", 0, b"", 0, finalized_at),
    )
    writer.insert(
        connector,
        "catalog_publication_commit_finalizations",
        ("receipt_id",),
        (receipt_id,),
    )
    writer.insert(
        connector,
        "catalog_publication_commit_head_receipts",
        ("channel", "receipt_id"),
        (b"default", receipt_id),
    )
    return receipt_id, candidate_id


def _seed_source_authority(
    connector: SQLConnector,
    writer: _ManifestBoundWriter,
    *,
    scope_key: bytes,
    snapshot_manifest_sha256: bytes,
    seed: int,
    committed_at: int,
) -> None:
    build_id = hashlib.sha256(
        b"h2hdb benchmark source build\0" + seed.to_bytes(8, "big")
    ).digest()[:16]
    analysis_id = hashlib.sha256(
        b"h2hdb benchmark analysis\0" + seed.to_bytes(8, "big")
    ).digest()[:16]
    input_manifest_sha256 = hashlib.sha256(
        b"h2hdb benchmark input manifest\0" + seed.to_bytes(8, "big")
    ).digest()
    writer.insert(
        connector,
        "catalog_source_build_descriptor",
        ("build_id", "scope_key", "manifest_policy_id", "created_at"),
        (build_id, scope_key, 1, committed_at - 20),
    )
    writer.insert(
        connector,
        "catalog_source_build_states",
        ("build_id", "state"),
        (build_id, "SEALED"),
    )
    writer.insert(
        connector,
        "catalog_source_build_channel",
        ("build_id", "channel"),
        (build_id, b"default"),
    )
    writer.insert(
        connector,
        "catalog_source_build_discoveries",
        (
            "build_id",
            "scan_attempt",
            "gallery_count",
            "tree_observation_sha256",
            "completed_at",
        ),
        (
            build_id,
            hashlib.sha256(build_id + b"scan").digest()[:16],
            0,
            hashlib.sha256(build_id + b"tree").digest(),
            committed_at - 19,
        ),
    )
    writer.insert(
        connector,
        "catalog_build_manifest_core",
        ("build_id", "manifest_sha256", "file_count", "byte_count"),
        (build_id, input_manifest_sha256, 0, 0),
    )
    writer.insert(
        connector,
        "catalog_source_build_sealed_ats",
        ("build_id", "sealed_at"),
        (build_id, committed_at - 18),
    )
    writer.insert(
        connector,
        "catalog_analysis_run_descriptor",
        ("analysis_id", "build_id", "policy_id", "input_manifest_sha256", "started_at"),
        (analysis_id, build_id, 1, input_manifest_sha256, committed_at - 17),
    )
    writer.insert(
        connector,
        "catalog_analysis_run_states",
        ("analysis_id", "state"),
        (analysis_id, "COMPLETE"),
    )
    writer.insert(
        connector,
        "catalog_analysis_run_completed_ats",
        ("analysis_id", "completed_at"),
        (analysis_id, committed_at - 11),
    )
    writer.insert(
        connector,
        "catalog_analysis_state_ancestry",
        ("analysis_id", "ancestor_depth", "ancestor_analysis_id"),
        (analysis_id, 0, analysis_id),
    )
    for offset, component in enumerate(sorted(identity.ANALYSIS_STATE_COMPONENTS)):
        writer.insert(
            connector,
            "catalog_analysis_state_component_seals",
            ("analysis_id", "state_component", "row_count", "sealed_at"),
            (analysis_id, component.encode("ascii"), 0, committed_at - 16 + offset),
        )
    writer.insert(
        connector,
        "catalog_analysis_snapshot_manifest",
        ("analysis_id", "snapshot_manifest_sha256"),
        (analysis_id, snapshot_manifest_sha256),
    )
    writer.insert(
        connector,
        "catalog_source_revision_descriptors",
        ("source_revision", "channel", "snapshot_manifest_sha256"),
        (SOURCE_REVISION, b"default", snapshot_manifest_sha256),
    )
    writer.insert(
        connector,
        "catalog_source_revision_provenance",
        ("source_revision", "analysis_id"),
        (SOURCE_REVISION, analysis_id),
    )


def _seed_shared_catalog_values(
    connector: SQLConnector,
    writer: _ManifestBoundWriter,
    *,
    allocated_at: int,
    policy_component_sha256: bytes,
) -> dict[str, Any]:
    source_root_payload = identity.encode_source_root(("synthetic-benchmark",))
    source_root = _persist_canonical_value(
        connector,
        domain="source_root_v1",
        payload=source_root_payload,
        allocated_at=allocated_at,
    )
    scope = ensure_source_scope(
        connector,
        source_provider=b"filesystem",
        source_root_sha256=source_root,
        identity_policy_version=1,
    ).record.scope_key
    snapshot_payload = identity.encode_source_snapshot_manifest(
        identity.SourceSnapshotPolicy(1, 1, 1, 1, 1),
        identity.SourceSnapshotCounts(0, 0, 0),
        (),
        (),
        (),
        (),
    )
    snapshot_manifest = _persist_canonical_value(
        connector,
        domain="source_snapshot_manifest_v1",
        payload=snapshot_payload,
        allocated_at=allocated_at,
    )
    writer.insert(
        connector,
        "catalog_source_snapshot_manifest_identity",
        ("snapshot_manifest_sha256", "gallery_count", "file_count", "byte_count"),
        (snapshot_manifest, 0, 0, 0),
    )

    title_values: dict[str, dict[str, bytes]] = {}
    for title in (_REGULAR_TITLE, _MATCHING_TITLE):
        raw = title.encode("utf-8")
        source_title = _persist_canonical_value(
            connector,
            domain="source_title_utf8_v1",
            payload=raw,
            allocated_at=allocated_at,
        )
        display_title = _persist_canonical_value(
            connector,
            domain="display_title_utf8_v1",
            payload=raw,
            allocated_at=allocated_at,
        )
        sort_payload = title.casefold().encode("utf-8")
        sort_title = _persist_canonical_value(
            connector,
            domain="title_sort_utf8_v1",
            payload=sort_payload,
            allocated_at=allocated_at,
        )
        writer.insert(
            connector,
            "catalog_title_sorts",
            ("title_sort_policy_id", "title_sha256", "sort_title_sha256"),
            (1, display_title, sort_title),
        )
        title_values[title] = {
            "source": source_title,
            "display": display_title,
            "sort": sort_title,
        }

    summary = _persist_canonical_value(
        connector,
        domain="catalog_summary_utf8_v1",
        payload=_SUMMARY.encode("utf-8"),
        allocated_at=allocated_at,
    )
    language_values = {
        value: _persist_canonical_value(
            connector,
            domain="catalog_language_utf8_v1",
            payload=value.encode("utf-8"),
            allocated_at=allocated_at,
        )
        for value in _LANGUAGES
    }
    contributor_values = {
        value: _persist_canonical_value(
            connector,
            domain="contributor_name_utf8_v1",
            payload=value.encode("utf-8"),
            allocated_at=allocated_at,
        )
        for value in _CONTRIBUTORS
    }
    subject_values = {
        value: _persist_canonical_value(
            connector,
            domain="tag_value_utf8_v1",
            payload=value.encode("utf-8"),
            allocated_at=allocated_at,
        )
        for value in _SUBJECTS
    }
    subject_ids = {value: index + 1 for index, value in enumerate(_SUBJECTS)}
    for value in _SUBJECTS:
        ensure_tag_term(
            connector,
            term=TagTerm(
                subject_ids[value],
                _SUBJECT_NAMESPACE,
                subject_values[value],
            ),
        )

    artifact_payload = b"\0"
    artifact_sha256 = hashlib.sha256(artifact_payload).digest()
    writer.insert(
        connector,
        "catalog_artifact_blobs",
        ("artifact_sha256", "size_bytes"),
        (artifact_sha256, len(artifact_payload)),
    )
    content_payload = identity.encode_effective_content((artifact_sha256,))
    content = _persist_canonical_value(
        connector,
        domain="effective_content_v1",
        payload=content_payload,
        allocated_at=allocated_at,
    )

    observation_sha256 = hashlib.sha256(b"synthetic observation").digest()
    source_manifest_payload = identity.encode_artifact_source_manifest(
        observation_sha256,
        1,
        1,
    )
    source_manifest = _persist_canonical_value(
        connector,
        domain="artifact_source_manifest_v1",
        payload=source_manifest_payload,
        allocated_at=allocated_at,
    )
    metadata_sha256 = hashlib.sha256(b"synthetic metadata").digest()
    member_plan_payload = identity.encode_artifact_member_plan(
        (
            identity.ArtifactMemberPlanEntry(
                source_position=0,
                source_name_bytes=b"galleryinfo.txt",
                source_file_sha256=metadata_sha256,
                source_size_bytes=1,
                source_role=identity.ArtifactMemberSourceRole.METADATA,
            ),
        )
    )
    member_plan = _persist_canonical_value(
        connector,
        domain="artifact_member_plan_v2",
        payload=member_plan_payload,
        allocated_at=allocated_at,
    )
    artifact_effective_payload = identity.encode_artifact_effective_content(
        (metadata_sha256,)
    )
    artifact_effective = _persist_canonical_value(
        connector,
        domain="artifact_effective_content_v1",
        payload=artifact_effective_payload,
        allocated_at=allocated_at,
    )

    lexeme_values: dict[bytes, bytes] = {}
    searchable_fields = (
        *(_REGULAR_TITLE.encode(), _MATCHING_TITLE.encode()),
        *(value.encode() for value in _CONTRIBUTORS),
        *(value.encode() for value in _SUBJECTS),
    )
    for lexeme in set(iter_search_lexemes(searchable_fields)):
        digest = _persist_canonical_value(
            connector,
            domain="search_lexeme_utf8_v1",
            payload=lexeme,
            allocated_at=allocated_at,
        )
        writer.insert(
            connector,
            "catalog_search_lexemes",
            ("value_sha256",),
            (digest,),
        )
        lexeme_values[lexeme] = digest

    return {
        "scope": scope,
        "snapshot_manifest": snapshot_manifest,
        "titles": title_values,
        "summary": summary,
        "languages": language_values,
        "contributors": contributor_values,
        "subjects": subject_values,
        "subject_ids": subject_ids,
        "artifact_sha256": artifact_sha256,
        "artifact_size": len(artifact_payload),
        "content": content,
        "source_manifest": source_manifest,
        "member_plan": member_plan,
        "artifact_effective": artifact_effective,
        "policy": policy_component_sha256,
        "lexemes": lexeme_values,
    }


def _seed_one_publication(
    connector: SQLConnector,
    writer: _ManifestBoundWriter,
    *,
    assignment: _SyntheticAssignment,
    shared: dict[str, Any],
    allocated_at: int,
    modified_at: int,
) -> tuple[bytes, tuple[bytes, ...]]:
    gallery_name_text = f"synthetic-{assignment.gid}"
    gallery_name = gallery_name_text.encode("utf-8")
    locator_payload = identity.encode_source_relative_locator((gallery_name_text,))
    locator = _persist_canonical_value(
        connector,
        domain="source_relative_locator_v1",
        payload=locator_payload,
        allocated_at=allocated_at,
    )
    writer.insert(
        connector,
        "catalog_source_locator_identity",
        ("locator_sha256", "source_gallery_name"),
        (locator, gallery_name),
    )
    gallery_key = identity.gallery_key(shared["scope"], locator)
    gallery_id = assignment.position + 1
    ensure_gallery_identity(
        connector,
        identity=GalleryIdentity(
            gallery_id,
            gallery_key,
            shared["scope"],
            locator,
        ),
    )
    writer.insert(
        connector,
        "catalog_gallery_upload_times",
        ("gid", "upload_time"),
        (assignment.gid, modified_at - 2),
    )
    publication_key = identity.publication_key(assignment.gid)
    ensure_publication_identity_family(
        connector,
        PublicationIdentityFamily(publication_key, assignment.gid),
    )
    writer.insert(
        connector,
        "catalog_source_gallery_name_gids",
        ("source_gallery_name", "gid"),
        (gallery_name, assignment.gid),
    )
    writer.insert(
        connector,
        "catalog_gallery_source_name_accesses",
        ("gallery_id", "source_gallery_name"),
        (gallery_id, gallery_name),
    )

    title_values = shared["titles"][assignment.title]
    writer.insert(
        connector,
        "catalog_display_title_choices",
        (
            "display_title_policy_id",
            "source_title_sha256",
            "source_gallery_name",
            "title_sha256",
        ),
        (1, title_values["source"], gallery_name, title_values["display"]),
    )
    ensure_catalog_publication_family(
        connector,
        CatalogPublicationFamily(
            revision=REVISION,
            publication_key=publication_key,
            gallery_id=gallery_id,
            summary_sha256=shared["summary"],
            language_sha256=shared["languages"][assignment.language],
            modified_at=modified_at,
            source_title_sha256=title_values["source"],
        ),
    )
    ensure_catalog_publication_download_time_family(
        connector,
        CatalogPublicationDownloadTimeFamily(
            REVISION,
            publication_key,
            modified_at - 1,
        ),
    )
    ensure_catalog_publication_title_family(
        connector,
        CatalogPublicationTitleFamily(
            REVISION,
            publication_key,
            title_values["source"],
            gallery_name,
        ),
    )
    writer.insert(
        connector,
        "catalog_publication_order",
        ("revision", "position", "publication_key"),
        (REVISION, assignment.position, publication_key),
    )
    writer.insert(
        connector,
        "catalog_publication_contents",
        ("revision", "publication_key", "content_sha256"),
        (REVISION, publication_key, shared["content"]),
    )
    ensure_catalog_contributor_family(
        connector,
        CatalogContributorFamily(
            REVISION,
            publication_key,
            0,
            shared["contributors"][assignment.contributor],
            _CONTRIBUTOR_ROLE,
        ),
    )
    writer.insert(
        connector,
        "catalog_subjects",
        ("revision", "publication_key", "position", "tag_id"),
        (
            REVISION,
            publication_key,
            0,
            shared["subject_ids"][assignment.subject],
        ),
    )

    selected_payload = identity.encode_artifact_selected(publication_key, gallery_key)
    selected = _persist_canonical_value(
        connector,
        domain="artifact_selected_v1",
        payload=selected_payload,
        allocated_at=allocated_at,
    )
    owner_payload = identity.encode_artifact_owner(
        shared["content"],
        gallery_key,
        assignment.gid,
        gallery_key,
    )
    owner = _persist_canonical_value(
        connector,
        domain="artifact_owner_v1",
        payload=owner_payload,
        allocated_at=allocated_at,
    )
    semantics_payload = identity.encode_artifact_semantics(
        shared["source_manifest"],
        shared["member_plan"],
        shared["artifact_effective"],
        selected,
        owner,
        shared["policy"],
    )
    semantics = _persist_canonical_value(
        connector,
        domain="artifact_semantics_v1",
        payload=semantics_payload,
        allocated_at=allocated_at,
    )
    ensure_artifact_semantic_input_family(
        connector,
        ArtifactSemanticInputFamily(
            artifact_semantics_sha256=semantics,
            source_manifest_component_sha256=shared["source_manifest"],
            member_plan_component_sha256=shared["member_plan"],
            effective_content_component_sha256=shared["artifact_effective"],
            selected_component_sha256=selected,
            owner_component_sha256=owner,
            policy_component_sha256=shared["policy"],
        ),
    )
    artifact_name = f"synthetic-{assignment.gid}.cbz".encode("ascii")
    ensure_catalog_artifact_family(
        connector,
        CatalogArtifactFamily(
            revision=REVISION,
            publication_key=publication_key,
            artifact_sha256=shared["artifact_sha256"],
            artifact_semantics_sha256=semantics,
            artifact_name=artifact_name,
            media_type=_MEDIA_TYPE,
            page_count=0,
        ),
    )
    storage_codec = "synthetic-benchmark-v1"
    storage_segments = ("acquisitions", f"{assignment.gid}.cbz")
    storage_key_sha256 = identity.artifact_storage_key_digest(
        storage_codec,
        storage_segments,
    )
    writer.insert(
        connector,
        "catalog_storage_object_key_identities",
        ("storage_object_key_sha256", "key_codec", "segment_count"),
        (storage_key_sha256, storage_codec.encode("ascii"), len(storage_segments)),
    )
    for segment_position, segment in enumerate(storage_segments):
        writer.insert(
            connector,
            "catalog_storage_object_key_segments",
            (
                "storage_object_key_sha256",
                "segment_position",
                "key_segment",
            ),
            (storage_key_sha256, segment_position, segment.encode("utf-8")),
        )
    writer.insert(
        connector,
        "catalog_storage_objects",
        (
            "revision",
            "publication_key",
            "resource_kind",
            "storage_object_key_sha256",
            "storage_object_sha256",
            "size_bytes",
            "modified_at",
        ),
        (
            REVISION,
            publication_key,
            b"acquisition",
            storage_key_sha256,
            shared["artifact_sha256"],
            shared["artifact_size"],
            modified_at,
        ),
    )

    field_lexemes = tuple(
        sorted(
            set(
                iter_search_lexemes(
                    (
                        assignment.title.encode("utf-8"),
                        assignment.title.encode("utf-8"),
                        assignment.contributor.encode("utf-8"),
                        assignment.subject.encode("utf-8"),
                    )
                )
            )
        )
    )
    posting_digests = tuple(shared["lexemes"][value] for value in field_lexemes)
    writer.insert(
        connector,
        "catalog_search_documents",
        ("revision", "publication_key", "row_count"),
        (REVISION, publication_key, len(posting_digests)),
    )
    for lexeme_sha256 in posting_digests:
        writer.insert(
            connector,
            "catalog_search_postings",
            ("revision", "value_sha256", "publication_key"),
            (REVISION, lexeme_sha256, publication_key),
        )
    return publication_key, posting_digests


def _seed_facets(
    connector: SQLConnector,
    writer: _ManifestBoundWriter,
    *,
    shared: dict[str, Any],
    language_counts: dict[str, int],
    subject_counts: dict[str, int],
    contributor_counts: dict[str, int],
) -> None:
    ordered_languages = sorted(_LANGUAGES, key=lambda value: value.encode("utf-8"))
    for position, value in enumerate(ordered_languages):
        writer.insert(
            connector,
            "catalog_language_facet_order",
            ("revision", "position", "language_sha256", "occurrence_count"),
            (REVISION, position, shared["languages"][value], language_counts[value]),
        )
    ordered_subjects = sorted(
        _SUBJECTS,
        key=lambda value: (
            _SUBJECT_NAMESPACE,
            value.encode("utf-8"),
            shared["subject_ids"][value],
        ),
    )
    for position, value in enumerate(ordered_subjects):
        writer.insert(
            connector,
            "catalog_subject_facet_order",
            ("revision", "position", "tag_id", "occurrence_count"),
            (
                REVISION,
                position,
                shared["subject_ids"][value],
                subject_counts[value],
            ),
        )
    ordered_contributors = sorted(
        _CONTRIBUTORS,
        key=lambda value: (
            _CONTRIBUTOR_ROLE,
            value.encode("utf-8"),
            shared["contributors"][value],
        ),
    )
    for position, value in enumerate(ordered_contributors):
        writer.insert(
            connector,
            "catalog_contributor_facet_order",
            (
                "revision",
                "position",
                "contributor_name_sha256",
                "role",
                "occurrence_count",
            ),
            (
                REVISION,
                position,
                shared["contributors"][value],
                _CONTRIBUTOR_ROLE,
                contributor_counts[value],
            ),
        )
    writer.insert(
        connector,
        "catalog_discovery_seals",
        ("revision", "policy_id"),
        (REVISION, 1),
    )


def _seed_fixture(
    database_path: Path,
    *,
    publication_count: int,
    seed: int,
) -> tuple[dict[str, Any], tuple[str, ...]]:
    writer = _ManifestBoundWriter()
    committed_at = _BASE_TIMESTAMP + seed % 1_000_000
    full_language_counts = dict.fromkeys(_LANGUAGES, 0)
    full_subject_counts = dict.fromkeys(_SUBJECTS, 0)
    full_contributor_counts = dict.fromkeys(_CONTRIBUTORS, 0)
    search_language_counts = dict.fromkeys(_LANGUAGES, 0)
    search_subject_counts = dict.fromkeys(_SUBJECTS, 0)
    search_contributor_counts = dict.fromkeys(_CONTRIBUTORS, 0)
    matching_gids: list[int] = []
    posting_count = 0

    with SQLiteConnector(str(database_path)) as connector, connector.transaction():
        policy = _seed_catalog_policies(
            connector,
            writer,
            allocated_at=committed_at,
        )
        shared = _seed_shared_catalog_values(
            connector,
            writer,
            allocated_at=committed_at,
            policy_component_sha256=policy,
        )
        _seed_source_authority(
            connector,
            writer,
            scope_key=shared["scope"],
            snapshot_manifest_sha256=shared["snapshot_manifest"],
            seed=seed,
            committed_at=committed_at,
        )
        _seed_commit_head(
            connector,
            writer,
            publication_count=publication_count,
            seed=seed,
            committed_at=committed_at,
        )
        for position in range(publication_count):
            assignment = _assignment(position, seed)
            full_language_counts[assignment.language] += 1
            full_subject_counts[assignment.subject] += 1
            full_contributor_counts[assignment.contributor] += 1
            if assignment.matched:
                matching_gids.append(assignment.gid)
                search_language_counts[assignment.language] += 1
                search_subject_counts[assignment.subject] += 1
                search_contributor_counts[assignment.contributor] += 1
            _publication_key, postings = _seed_one_publication(
                connector,
                writer,
                assignment=assignment,
                shared=shared,
                allocated_at=committed_at + position + 1,
                modified_at=committed_at + position + 10,
            )
            posting_count += len(postings)
        _seed_facets(
            connector,
            writer,
            shared=shared,
            language_counts=full_language_counts,
            subject_counts=full_subject_counts,
            contributor_counts=full_contributor_counts,
        )

    expected = {
        "revision": REVISION,
        "publication_count": publication_count,
        "artifact_count": publication_count,
        "artifact_blob_count": 1,
        "acquisition_descriptor_count": publication_count,
        "search_document_count": publication_count,
        "search_posting_count": posting_count,
        "search": {
            "query": SEARCH_QUERY,
            "publication_count": len(matching_gids),
            "ordered_gids": matching_gids,
        },
        "facets": {
            "language": search_language_counts,
            "subject": search_subject_counts,
            "contributor": search_contributor_counts,
        },
    }
    return expected, tuple(sorted(writer.used_tables))


def _facade_with_counters(
    database_path: Path,
    counters: _ReadCounters,
) -> VNextCatalogFacade:
    config = _config(database_path, read_only=True)
    facade = VNextCatalogFacade(config)
    context = replace(
        RepositoryContext.from_config(config),
        SQLConnector=lambda: _CountingReadOnlySQLiteConnector(
            str(database_path),
            counters,
        ),
    )
    # This is benchmark-only instrumentation of a private implementation
    # detail.  It avoids adding an injection seam to the production facade.
    object.__setattr__(facade, "_VNextCatalogFacade__context", context)
    return facade


def _bundle_summary(bundle: CatalogDiscoveryBundle) -> dict[str, Any]:
    cursor = bundle.page.next_cursor
    return {
        "revision": bundle.page.revision.revision,
        "publication_ids": [
            publication.publication_id for publication in bundle.page.publications
        ],
        "next_cursor": (
            None
            if cursor is None
            else {
                "revision": cursor.revision,
                "query_sha256": cursor.query_sha256,
                "position": cursor.position,
                "publication_id": cursor.publication_id,
            }
        ),
        "facets": {
            page.facet.value: [
                {
                    "value": value.value,
                    "publication_count": value.publication_count,
                    "namespace": value.namespace,
                    "role": value.role,
                }
                for value in page.values
            ]
            for page in bundle.facets
        },
    }


def _bundle_json(bundle: CatalogDiscoveryBundle) -> bytes:
    return json.dumps(
        _bundle_summary(bundle),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _bundle_digest(bundle: CatalogDiscoveryBundle) -> str:
    return hashlib.sha256(_bundle_json(bundle)).hexdigest()


def _measure_bundle(
    facade: VNextCatalogFacade,
    counters: _ReadCounters,
    *,
    query: CatalogDiscoveryQuery,
    after: Any = None,
) -> tuple[CatalogDiscoveryBundle, dict[str, Any]]:
    counters.reset()
    started = time.perf_counter_ns()
    bundle = facade.discover_publications_with_facets(
        query=query,
        after=after,
        limit=PAGE_LIMIT,
        facet_limit=FACET_LIMIT,
    )
    elapsed_ns = time.perf_counter_ns() - started
    if counters.connections != 1 or counters.read_transactions != 2:
        raise RuntimeError(
            "catalog bundle no longer uses one connection and two read transactions"
        )
    return bundle, {
        "elapsed_ns": elapsed_ns,
        "elapsed_seconds": elapsed_ns / 1_000_000_000,
        "connection_count": counters.connections,
        "read_transaction_count": counters.read_transactions,
        "logical_query_count": counters.logical_queries,
        "query_class_counts": counters.class_summary(),
        "query_shapes": counters.shape_summary(),
        "result_sha256": _bundle_digest(bundle),
        "result_json_bytes": len(_bundle_json(bundle)),
        "returned_publication_count": len(bundle.page.publications),
        "returned_facet_value_count": sum(len(page.values) for page in bundle.facets),
    }


def _measure_separate_reference(
    facade: VNextCatalogFacade,
    counters: _ReadCounters,
    *,
    query: CatalogDiscoveryQuery,
) -> tuple[CatalogDiscoveryBundle, dict[str, Any]]:
    counters.reset()
    started = time.perf_counter_ns()
    page = facade.discover_publications(query=query, limit=PAGE_LIMIT)
    facets = tuple(
        facade.list_publication_facets(
            facet=facet,
            query=query,
            limit=FACET_LIMIT,
            revision=page.revision,
        )
        for facet in CatalogFacetKind
    )
    bundle = CatalogDiscoveryBundle(page=page, facets=facets)
    elapsed_ns = time.perf_counter_ns() - started
    expected_calls = 1 + len(CatalogFacetKind)
    if (
        counters.connections != expected_calls
        or counters.read_transactions != expected_calls * 2
    ):
        raise RuntimeError(
            "separate catalog reference no longer uses one connection and two "
            "read transactions per facade call"
        )
    return bundle, {
        "elapsed_ns": elapsed_ns,
        "elapsed_seconds": elapsed_ns / 1_000_000_000,
        "connection_count": counters.connections,
        "read_transaction_count": counters.read_transactions,
        "logical_query_count": counters.logical_queries,
        "query_class_counts": counters.class_summary(),
        "query_shapes": counters.shape_summary(),
        "result_sha256": _bundle_digest(bundle),
        "result_json_bytes": len(_bundle_json(bundle)),
        "public_facade_call_count": expected_calls,
    }


def _measure_bundle_memory(
    facade: VNextCatalogFacade,
    counters: _ReadCounters,
    *,
    query: CatalogDiscoveryQuery,
) -> tuple[CatalogDiscoveryBundle, dict[str, Any]]:
    counters.reset()
    current_bytes = 0
    peak_bytes = 0
    tracemalloc.start()
    try:
        bundle = facade.discover_publications_with_facets(
            query=query,
            limit=PAGE_LIMIT,
            facet_limit=FACET_LIMIT,
        )
        current_bytes, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    if counters.connections != 1 or counters.read_transactions != 2:
        raise RuntimeError(
            "catalog memory probe no longer uses one connection and two read "
            "transactions"
        )
    return bundle, {
        "python_traced_current_bytes": current_bytes,
        "python_traced_peak_bytes": peak_bytes,
        "result_json_bytes": len(_bundle_json(bundle)),
        "connection_count": counters.connections,
        "read_transaction_count": counters.read_transactions,
        "logical_query_count": counters.logical_queries,
        "query_class_counts": counters.class_summary(),
        "query_shapes": counters.shape_summary(),
        "result_sha256": _bundle_digest(bundle),
    }


def _facet_counts(bundle: CatalogDiscoveryBundle) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for page in bundle.facets:
        result[page.facet.value] = {
            value.value: value.publication_count for value in page.values
        }
    return result


def _validate_measured_results(
    first: CatalogDiscoveryBundle,
    warm: CatalogDiscoveryBundle,
    cursor_page: CatalogDiscoveryBundle,
    *,
    expected: dict[str, Any],
) -> None:
    if first != warm:
        raise RuntimeError("first and warm bundle results differ")
    matching_gids = expected["search"]["ordered_gids"]
    expected_first = matching_gids[:PAGE_LIMIT]
    expected_cursor = matching_gids[PAGE_LIMIT : 2 * PAGE_LIMIT]
    actual_first = [publication.gid for publication in first.page.publications]
    actual_cursor = [publication.gid for publication in cursor_page.page.publications]
    if actual_first != expected_first or actual_cursor != expected_cursor:
        raise RuntimeError("search discovery pages differ from the fixed-seed oracle")
    if first.page.next_cursor is None:
        raise RuntimeError("search fixture does not provide a cursor page")
    actual_facets = _facet_counts(first)
    expected_facets = {
        family: {value: count for value, count in counts.items() if count > 0}
        for family, counts in expected["facets"].items()
    }
    if actual_facets != expected_facets:
        raise RuntimeError("search facet results differ from the fixed-seed oracle")


def _database_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json_sha256(value: dict[str, Any]) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _source_provenance() -> dict[str, Any]:
    repository_root = Path(__file__).resolve().parents[1]
    pyproject_path = repository_root / "pyproject.toml"
    with pyproject_path.open("rb") as source:
        project_data = tomllib.load(source)
    project = project_data.get("project")
    project_version = project.get("version") if isinstance(project, dict) else None
    if not isinstance(project_version, str) or not project_version:
        raise RuntimeError("pyproject.toml does not define project.version")

    source_paths = [
        pyproject_path,
        Path(__file__).resolve(),
        *(repository_root / "src" / "h2hdb").rglob("*.py"),
    ]
    relative_paths = sorted(
        {
            path.resolve().relative_to(repository_root).as_posix(): path.resolve()
            for path in source_paths
            if path.is_file()
        }.items()
    )
    digest = hashlib.sha256(b"h2hdb-benchmark-source-manifest-v1\0")
    for relative_path, absolute_path in relative_paths:
        path_bytes = relative_path.encode("utf-8")
        content = absolute_path.read_bytes()
        digest.update(len(path_bytes).to_bytes(8, "big"))
        digest.update(path_bytes)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)

    commit: str | None = None
    dirty: bool | None = None
    try:
        revision = subprocess.run(
            ("git", "rev-parse", "HEAD"),
            cwd=repository_root,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        status = subprocess.run(
            ("git", "status", "--porcelain", "--untracked-files=all"),
            cwd=repository_root,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if revision.returncode == 0:
            candidate = revision.stdout.strip()
            if len(candidate) == 40:
                commit = candidate
        if status.returncode == 0:
            dirty = bool(status.stdout)
    except OSError, subprocess.SubprocessError:
        pass
    return {
        "project_version": project_version,
        "source_manifest_algorithm": "h2hdb-benchmark-source-manifest-v1",
        "source_manifest_sha256": digest.hexdigest(),
        "source_manifest_file_count": len(relative_paths),
        "git_commit": commit,
        "git_dirty": dirty,
    }


def _actual_database_counts(path: Path) -> dict[str, int]:
    queries = {
        "publication_count": "SELECT COUNT(*) FROM catalog_publications",
        "artifact_count": "SELECT COUNT(*) FROM catalog_artifacts",
        "artifact_blob_count": "SELECT COUNT(*) FROM catalog_artifact_blobs",
        "acquisition_descriptor_count": (
            "SELECT COUNT(*) FROM catalog_storage_objects "
            "WHERE resource_kind = X'6163717569736974696F6E'"
        ),
        "search_document_count": "SELECT COUNT(*) FROM catalog_search_documents",
        "search_posting_count": "SELECT COUNT(*) FROM catalog_search_postings",
    }
    result: dict[str, int] = {}
    with SQLiteConnector(str(path), read_only=True) as connector:
        with connector.read_transaction():
            for label, query in queries.items():
                row = connector.fetch_one(query)
                if len(row) != 1 or type(row[0]) is not int:
                    raise RuntimeError(f"database count {label!r} is malformed")
                result[label] = row[0]
            if connector.fetch_all("PRAGMA foreign_key_check"):
                raise RuntimeError("synthetic fixture fails SQLite foreign_key_check")
            if connector.fetch_one("PRAGMA integrity_check") != ("ok",):
                raise RuntimeError("synthetic fixture fails SQLite integrity_check")
    return result


def _reserve_new_file(path: Path) -> None:
    if not path.parent.is_dir():
        raise FileNotFoundError(
            f"target parent directory does not exist: {path.parent}"
        )
    with path.open("xb"):
        pass


def run_scalability_benchmark(
    database_path: Path,
    *,
    receipt_path: Path,
    publication_count: int = MANUAL_PUBLICATION_COUNT,
    seed: int = DEFAULT_SEED,
) -> dict[str, Any]:
    """Build one fresh DB, measure reads, audit it, and write a JSON receipt."""

    minimum_publication_count = (PAGE_LIMIT + 1) * SEARCH_MATCH_MODULUS
    if (
        type(publication_count) is not int
        or publication_count < minimum_publication_count
    ):
        raise ValueError(
            "publication_count must be at least "
            f"{minimum_publication_count} for a nonempty search cursor page"
        )
    if type(seed) is not int or not 0 <= seed <= (1 << 64) - 1:
        raise ValueError("seed must be an unsigned 64-bit integer")
    database_path = Path(database_path).resolve()
    target_receipt = Path(receipt_path).resolve()
    if database_path == target_receipt:
        raise ValueError("database and receipt paths must differ")
    if target_receipt.exists() or target_receipt.is_symlink():
        raise FileExistsError(f"receipt already exists: {target_receipt}")
    if not target_receipt.parent.is_dir():
        raise FileNotFoundError(
            f"receipt parent directory does not exist: {target_receipt.parent}"
        )
    _reserve_new_file(database_path)

    total_started = time.perf_counter_ns()
    schema_started = time.perf_counter_ns()
    initialized = VNextDatabaseAdminFacade(_config(database_path)).initialize()
    schema_elapsed_ns = time.perf_counter_ns() - schema_started
    if initialized.state != "READY" or not initialized.transitioned_to_ready:
        raise RuntimeError("fresh benchmark database did not transition to READY")

    seed_started = time.perf_counter_ns()
    expected, manifest_tables = _seed_fixture(
        database_path,
        publication_count=publication_count,
        seed=seed,
    )
    seed_elapsed_ns = time.perf_counter_ns() - seed_started

    counters = _ReadCounters()
    facade = _facade_with_counters(database_path, counters)
    query = CatalogDiscoveryQuery(search=SEARCH_QUERY)
    first, first_metrics = _measure_bundle(facade, counters, query=query)
    warm, warm_metrics = _measure_bundle(facade, counters, query=query)
    if first.page.next_cursor is None:
        raise RuntimeError("fixed synthetic search unexpectedly lacks a cursor")
    cursor_page, cursor_metrics = _measure_bundle(
        facade,
        counters,
        query=query,
        after=first.page.next_cursor,
    )
    _validate_measured_results(first, warm, cursor_page, expected=expected)
    reference, reference_metrics = _measure_separate_reference(
        facade,
        counters,
        query=query,
    )
    if reference != first:
        raise RuntimeError(
            "separate public facade calls differ from the bundled catalog read"
        )
    memory_bundle, memory_metrics = _measure_bundle_memory(
        facade,
        counters,
        query=query,
    )
    if memory_bundle != first:
        raise RuntimeError("memory probe bundle differs from the timed search bundle")

    audit_started = time.perf_counter_ns()
    audited = VNextDatabaseAdminFacade(_config(database_path, read_only=True)).check()
    audit_elapsed_ns = time.perf_counter_ns() - audit_started
    if (
        audited.state != "READY"
        or audited.manifest_sha256 != initialized.manifest_sha256
    ):
        raise RuntimeError("full READY audit did not preserve the initialized manifest")
    actual_counts = _actual_database_counts(database_path)
    for label, actual in actual_counts.items():
        if actual != expected[label]:
            raise RuntimeError(
                f"database {label} {actual} differs from expected {expected[label]}"
            )

    matching_gids = expected["search"].pop("ordered_gids")
    expected["search"]["first_page_gids"] = matching_gids[:PAGE_LIMIT]
    expected["search"]["cursor_page_gids"] = matching_gids[PAGE_LIMIT : 2 * PAGE_LIMIT]
    total_elapsed_ns = time.perf_counter_ns() - total_started
    provenance = _source_provenance()
    core_version = provenance["project_version"]
    fixture_contract = {
        "format": "h2hdb-sqlite-catalog-scalability-v1",
        "fixture_mode": FIXTURE_MODE,
        "core_version": core_version,
        "source_manifest_sha256": provenance["source_manifest_sha256"],
        "seed": seed,
        "schema_epoch": initialized.epoch,
        "schema_version": initialized.schema_version,
        "schema_manifest_sha256": initialized.manifest_sha256,
        "expected": expected,
    }
    receipt: dict[str, Any] = {
        "format": "h2hdb-sqlite-catalog-scalability-v1",
        "fixture_mode": FIXTURE_MODE,
        "fixture_refines_complete_ingest_state_machine": False,
        "core_version": core_version,
        "source_provenance": provenance,
        "seed": seed,
        "profile": (
            "manual-10k"
            if publication_count == MANUAL_PUBLICATION_COUNT
            else "smoke"
            if publication_count == SMOKE_PUBLICATION_COUNT
            else "custom"
        ),
        "schema": {
            "epoch": initialized.epoch,
            "schema_version": initialized.schema_version,
            "state": initialized.state,
            "manifest_sha256": initialized.manifest_sha256,
            "full_ready_audit_passed": True,
        },
        "fixture": {
            "publication_count": publication_count,
            "creates_cbz_or_artwork_bytes": False,
            "production_family_bindings": list(_PRODUCTION_FAMILY_BINDINGS),
            "manifest_bound_tables": list(manifest_tables),
        },
        "expected": expected,
        "fixture_contract_sha256": _canonical_json_sha256(fixture_contract),
        "actual_database_counts": actual_counts,
        "timing": {
            "schema_initialize": {
                "elapsed_ns": schema_elapsed_ns,
                "elapsed_seconds": schema_elapsed_ns / 1_000_000_000,
            },
            "fixture_seed": {
                "elapsed_ns": seed_elapsed_ns,
                "elapsed_seconds": seed_elapsed_ns / 1_000_000_000,
            },
            "catalog_bundle_first_after_build": first_metrics,
            "catalog_bundle_warm": warm_metrics,
            "catalog_bundle_cursor_page": cursor_metrics,
            "catalog_separate_facade_reference": reference_metrics,
            "catalog_bundle_memory_probe": memory_metrics,
            "full_ready_audit": {
                "elapsed_ns": audit_elapsed_ns,
                "elapsed_seconds": audit_elapsed_ns / 1_000_000_000,
            },
            "total": {
                "elapsed_ns": total_elapsed_ns,
                "elapsed_seconds": total_elapsed_ns / 1_000_000_000,
            },
        },
        "database": {
            "path": str(database_path),
            "sha256": _database_sha256(database_path),
            "size_bytes": database_path.stat().st_size,
        },
        "receipt_path": str(target_receipt),
        "comparability": {
            "first_after_build_is_not_an_os_page_cache_cold_run": True,
            "latency_threshold_enforced": False,
            "logical_query_count_excludes_connector_setup_pragmas": True,
            "normalized_sql_shape_algorithm": "collapse-ascii-whitespace-v1",
            "fixture_contract_digest_includes_paths": False,
            "runtime_bundle_reference_exact_equality_passed": True,
            "runtime_equality_is_not_a_formal_refinement_claim": True,
            "measurement_order": [
                "catalog_bundle_first_after_build",
                "catalog_bundle_warm",
                "catalog_bundle_cursor_page",
                "catalog_separate_facade_reference",
                "catalog_bundle_memory_probe",
                "full_ready_audit",
            ],
        },
    }
    payload = json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with target_receipt.open("x", encoding="utf-8") as destination:
        destination.write(payload)
    return receipt


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        choices=("smoke", "10k"),
        default="smoke",
        help="safe smoke profile or explicit manual 10,000-publication profile",
    )
    parser.add_argument(
        "--database",
        type=Path,
        required=True,
        help="new SQLite database path; an existing path is rejected",
    )
    parser.add_argument(
        "--receipt",
        type=Path,
        required=True,
        help="new JSON receipt path; an existing path is rejected",
    )
    parser.add_argument(
        "--seed", type=lambda value: int(value, 0), default=DEFAULT_SEED
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    publication_count = (
        SMOKE_PUBLICATION_COUNT if args.profile == "smoke" else MANUAL_PUBLICATION_COUNT
    )
    receipt = run_scalability_benchmark(
        args.database,
        receipt_path=args.receipt,
        publication_count=publication_count,
        seed=args.seed,
    )
    print(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
