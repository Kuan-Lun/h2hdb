"""Bounded production refinements for the vNext catalog data plane.

READY validation independently replays the current revision's canonical search
and facet projection into a disk-backed, keyset-paged audit plan. Historical
canonical trees, artifact plans, and analysis workset derivations remain owned
by their named transaction writer hooks. The other recurring checks are bounded
to closed registries, active channel heads, and the at-most-16-deep analysis
seal chain. Sealed impacted-key families use key-first indexed anti-joins, and
publication generation history is linearly audited over the single sealed
common commit chain. Quick readiness remains epoch-only and O(1).
"""

from __future__ import annotations

__all__ = [
    "BuiltinSemanticRegistryError",
    "CatalogSemanticValidationError",
    "builtin_semantic_validators",
    "check_artifact_semantics_v1",
    "check_bootstrap_v1",
    "check_canonical_reference_domains_v1",
    "check_discovery_exactness_v1",
    "check_identity_codecs_v1",
    "check_incremental_impact_v1",
    "check_overlay_resolution_seal_v1",
    "check_physical_domains_v1",
    "check_published_baseline_prune_v1",
    "check_publication_atomicity_v1",
    "check_retention_contract_v2",
    "check_role_derivation_v1",
    "check_source_baseline_channel_v1",
    "check_state_machines_v1",
    "validate_builtin_semantic_manifest",
]

import codecs
import re
import sqlite3
import unicodedata
from collections import OrderedDict
from collections.abc import Callable, Container, Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from io import BytesIO
from tempfile import TemporaryFile
from types import MappingProxyType
from typing import Any, BinaryIO

from . import vnext_identity as identity
from ._generated_vnext_schema import ARTIFACT
from .catalog_search import (
    SEARCH_POLICY_ID,
    iter_search_field_lexemes,
    require_search_runtime_policy,
)
from .catalog_writer import validate_artifact_writer_manifest
from .domain import (
    ByteExtent,
    CatalogArtifact,
    CatalogImageResource,
    StorageObjectDescriptor,
    StorageObjectKey,
)
from .sql_connector import SQLConnector
from .vnext_canonical_value_repository import (
    CanonicalValueCollisionError,
    CanonicalValueNotReadyError,
    stream_and_validate_canonical_value,
)
from .vnext_domains import require_ascii_bytes, require_digest32
from .vnext_state_machine_contract import validate_catalog_state_machine_contract

type SemanticValidator = Callable[[SQLConnector], None]

_CANONICAL_VALIDATION_CACHE_MAX_ENTRIES = 128
_CANONICAL_VALIDATION_CACHE_MAX_VALUE_BYTES = 64 * 1024
_CANONICAL_VALIDATION_CACHE_MAX_TOTAL_BYTES = (
    _CANONICAL_VALIDATION_CACHE_MAX_ENTRIES
    * _CANONICAL_VALIDATION_CACHE_MAX_VALUE_BYTES
)


class BuiltinSemanticRegistryError(RuntimeError):
    """The generated obligation manifest differs from the wheel registry."""


class CatalogSemanticValidationError(BuiltinSemanticRegistryError):
    """Bounded catalog READY state is inconsistent with its contract."""


_SPECS = (
    (
        "catalog.identity-codecs.v1",
        "ready_and_runtime",
        "catalog_refinement.check_identity_codecs_v1",
    ),
    (
        "catalog.canonical-reference-domains.v1",
        "ready_and_runtime",
        "catalog_refinement.check_canonical_reference_domains_v1",
    ),
    (
        "catalog.source-baseline-channel.v1",
        "ready_and_runtime",
        "catalog_refinement.check_source_baseline_channel_v1",
    ),
    (
        "catalog.incremental-impact.v1",
        "ready_and_runtime",
        "catalog_refinement.check_incremental_impact_v1",
    ),
    (
        "catalog.overlay-resolution-seal.v1",
        "ready_and_runtime",
        "catalog_refinement.check_overlay_resolution_seal_v1",
    ),
    (
        "catalog.published-baseline-prune.v1",
        "ready_and_runtime",
        "catalog_refinement.check_published_baseline_prune_v1",
    ),
    (
        "catalog.artifact-semantics.v1",
        "ready_and_runtime",
        "catalog_refinement.check_artifact_semantics_v1",
    ),
    (
        "catalog.publication-atomicity.v1",
        "ready_and_runtime",
        "catalog_refinement.check_publication_atomicity_v1",
    ),
    (
        "catalog.discovery-exactness.v1",
        "ready_and_runtime",
        "catalog_refinement.check_discovery_exactness_v1",
    ),
    (
        "catalog.state-machines.v1",
        "ready_and_runtime",
        "catalog_refinement.check_state_machines_v1",
    ),
    (
        "catalog.role-derivation.v1",
        "ready_and_runtime",
        "catalog_refinement.check_role_derivation_v1",
    ),
    (
        "catalog.physical-domains.v1",
        "ready_and_runtime",
        "catalog_refinement.check_physical_domains_v1",
    ),
    ("catalog.bootstrap.v1", "building_only", "catalog_refinement.check_bootstrap_v1"),
    (
        "catalog.retention.v2",
        "ready_and_runtime",
        "catalog_refinement.check_retention_contract_v2",
    ),
    (
        "h2hdb.operational.physical-domains.v1",
        "ready_validation",
        "operational_refinement.check_physical_domains_v1",
    ),
    (
        "h2hdb.operational.epoch-manifest.v1",
        "building_to_ready",
        "operational_refinement.check_epoch_manifest_v1",
    ),
    (
        "h2hdb.operational.fencing.v1",
        "ready_and_runtime",
        "operational_refinement.check_fencing_contract_v1",
    ),
    (
        "h2hdb.operational.download-ingest-handoff.v1",
        "ready_and_runtime",
        "operational_refinement.check_download_ingest_handoff_contract_v1",
    ),
    (
        "h2hdb.operational.maintenance-gate.v1",
        "ready_and_runtime",
        "operational_refinement.check_maintenance_gate_contract_v1",
    ),
    (
        "h2hdb.operational.bounded-work.v1",
        "ready_and_runtime",
        "operational_refinement.check_bounded_work_contract_v1",
    ),
    (
        "h2hdb.operational.queue-history.v1",
        "ready_and_runtime",
        "operational_refinement.check_queue_history_contract_v1",
    ),
    (
        "h2hdb.operational.canonical-hash-cache.v1",
        "ready_and_runtime",
        "operational_refinement.check_canonical_hash_cache_contract_v1",
    ),
    (
        "h2hdb.operational.event-integrity.v1",
        "ready_and_runtime",
        "operational_refinement.check_event_integrity_contract_v1",
    ),
    (
        "h2hdb.operational.build-generation.v1",
        "ready_and_runtime",
        "operational_refinement.check_build_generation_contract_v1",
    ),
    (
        "h2hdb.operational.attempt-identity.v1",
        "ready_and_runtime",
        "operational_refinement.check_attempt_identity_contract_v1",
    ),
    (
        "h2hdb.operational.cleanup-reachability.v1",
        "ready_and_runtime",
        "operational_refinement.check_cleanup_reachability_v1",
    ),
    (
        "h2hdb.operational.cleanup-frozen-root-set.v1",
        "ready_and_runtime",
        "operational_refinement.check_cleanup_frozen_root_set_v1",
    ),
    (
        "h2hdb.operational.revision-allocation.v1",
        "ready_and_runtime",
        "operational_refinement.check_revision_allocator_contract_v1",
    ),
    (
        "h2hdb.operational.gallery-staging.v1",
        "ready_and_runtime",
        "operational_refinement.check_gallery_staging_contract_v1",
    ),
    (
        "h2hdb.operational.gallery-staging-request-budget.v1",
        "ready_and_runtime",
        "operational_refinement.check_gallery_staging_request_budget_v1",
    ),
    (
        "h2hdb.operational.bootstrap-genesis.v1",
        "building_only",
        "operational_refinement.check_bootstrap_contract_v1",
    ),
)

_EXPECTED_DATA_SEEDS = (
    ("catalog.channel.default.v1", "channel_registry", "channel", "default"),
    (
        "catalog.source-provider.filesystem.v1",
        "source_provider_registry",
        "source_provider",
        "filesystem",
    ),
    (
        "catalog.contributor-role.artist.v1",
        "contributor_role_registry",
        "role",
        "artist",
    ),
    (
        "catalog.contributor-role.author.v1",
        "contributor_role_registry",
        "role",
        "author",
    ),
    (
        "catalog.contributor-role.cosplayer.v1",
        "contributor_role_registry",
        "role",
        "cosplayer",
    ),
    (
        "catalog.contributor-role.group.v1",
        "contributor_role_registry",
        "role",
        "group",
    ),
    (
        "catalog.contributor-role.illustrator.v1",
        "contributor_role_registry",
        "role",
        "illustrator",
    ),
    (
        "catalog.contributor-role.uploader.v1",
        "contributor_role_registry",
        "role",
        "uploader",
    ),
    (
        "catalog.analysis-stage.changed-gallery.v1",
        "analysis_stage",
        "stage",
        "changed_gallery",
    ),
    (
        "catalog.analysis-stage.changed-file-hash.v1",
        "analysis_stage",
        "stage",
        "changed_file_hash",
    ),
    (
        "catalog.analysis-stage.file-hash-decision.v1",
        "analysis_stage",
        "stage",
        "file_hash_decision",
    ),
    (
        "catalog.analysis-stage.validate-file-hash-decision.v1",
        "analysis_stage",
        "stage",
        "validate_file_hash_decision",
    ),
    (
        "catalog.analysis-stage.impacted-gallery.v1",
        "analysis_stage",
        "stage",
        "impacted_gallery",
    ),
    (
        "catalog.analysis-stage.impacted-content.v1",
        "analysis_stage",
        "stage",
        "impacted_content",
    ),
    (
        "catalog.analysis-stage.content-owner-candidate.v1",
        "analysis_stage",
        "stage",
        "content_owner_candidate",
    ),
    (
        "catalog.analysis-stage.validate-content-owner-candidate.v1",
        "analysis_stage",
        "stage",
        "validate_content_owner_candidate",
    ),
    (
        "catalog.analysis-stage.content-owner.v1",
        "analysis_stage",
        "stage",
        "content_owner",
    ),
    (
        "catalog.analysis-stage.validate-content-owner.v1",
        "analysis_stage",
        "stage",
        "validate_content_owner",
    ),
    (
        "catalog.analysis-stage.impacted-gid.v1",
        "analysis_stage",
        "stage",
        "impacted_gid",
    ),
    (
        "catalog.analysis-stage.gid-candidate.v1",
        "analysis_stage",
        "stage",
        "gid_candidate",
    ),
    (
        "catalog.analysis-stage.validate-gid-candidate.v1",
        "analysis_stage",
        "stage",
        "validate_gid_candidate",
    ),
    (
        "catalog.analysis-stage.gid-winner.v1",
        "analysis_stage",
        "stage",
        "gid_winner",
    ),
    (
        "catalog.analysis-stage.validate-gid-winner.v1",
        "analysis_stage",
        "stage",
        "validate_gid_winner",
    ),
    (
        "catalog.resource-kind.acquisition.v2",
        "catalog_resource_kind",
        "resource_kind",
        "acquisition",
    ),
    (
        "catalog.resource-kind.thumbnail.v2",
        "catalog_resource_kind",
        "resource_kind",
        "thumbnail",
    ),
    (
        "catalog.digest-domain.artifact-effective-content.v1",
        "canonical_digest_policy",
        "digest_domain",
        "artifact_effective_content_v1",
    ),
    (
        "catalog.digest-domain.storage-object-key.v2",
        "canonical_digest_policy",
        "digest_domain",
        "storage_object_key_v2",
    ),
    (
        "catalog.digest-domain.artifact-member-plan.v2",
        "canonical_digest_policy",
        "digest_domain",
        "artifact_member_plan_v2",
    ),
    (
        "catalog.digest-domain.artifact-owner.v1",
        "canonical_digest_policy",
        "digest_domain",
        "artifact_owner_v1",
    ),
    (
        "catalog.digest-domain.artifact-policy.v3",
        "canonical_digest_policy",
        "digest_domain",
        "artifact_policy_v3",
    ),
    (
        "catalog.digest-domain.artifact-selected.v1",
        "canonical_digest_policy",
        "digest_domain",
        "artifact_selected_v1",
    ),
    (
        "catalog.digest-domain.artifact-semantics.v1",
        "canonical_digest_policy",
        "digest_domain",
        "artifact_semantics_v1",
    ),
    (
        "catalog.digest-domain.artifact-source-manifest.v1",
        "canonical_digest_policy",
        "digest_domain",
        "artifact_source_manifest_v1",
    ),
    (
        "catalog.digest-domain.contributor-name.v1",
        "canonical_digest_policy",
        "digest_domain",
        "contributor_name_utf8_v1",
    ),
    (
        "catalog.digest-domain.catalog-language.v1",
        "canonical_digest_policy",
        "digest_domain",
        "catalog_language_utf8_v1",
    ),
    (
        "catalog.digest-domain.catalog-summary.v1",
        "canonical_digest_policy",
        "digest_domain",
        "catalog_summary_utf8_v1",
    ),
    (
        "catalog.digest-domain.display-title.v1",
        "canonical_digest_policy",
        "digest_domain",
        "display_title_utf8_v1",
    ),
    (
        "catalog.digest-domain.effective-content.v1",
        "canonical_digest_policy",
        "digest_domain",
        "effective_content_v1",
    ),
    (
        "catalog.digest-domain.filesystem-fingerprint.v1",
        "canonical_digest_policy",
        "digest_domain",
        "filesystem_fingerprint_v1",
    ),
    (
        "catalog.digest-domain.filesystem-source-identity.v1",
        "canonical_digest_policy",
        "digest_domain",
        "filesystem_source_identity_v1",
    ),
    (
        "catalog.digest-domain.gallery-observation.v1",
        "canonical_digest_policy",
        "digest_domain",
        "gallery_observation_v1",
    ),
    (
        "catalog.digest-domain.source-relative-locator.v1",
        "canonical_digest_policy",
        "digest_domain",
        "source_relative_locator_v1",
    ),
    (
        "catalog.digest-domain.source-root.v1",
        "canonical_digest_policy",
        "digest_domain",
        "source_root_v1",
    ),
    (
        "catalog.digest-domain.source-snapshot-manifest.v1",
        "canonical_digest_policy",
        "digest_domain",
        "source_snapshot_manifest_v1",
    ),
    (
        "catalog.digest-domain.source-title.v1",
        "canonical_digest_policy",
        "digest_domain",
        "source_title_utf8_v1",
    ),
    (
        "catalog.digest-domain.tag-value.v1",
        "canonical_digest_policy",
        "digest_domain",
        "tag_value_utf8_v1",
    ),
    (
        "catalog.digest-domain.title-sort.v1",
        "canonical_digest_policy",
        "digest_domain",
        "title_sort_utf8_v1",
    ),
    (
        "catalog.digest-domain.search-lexeme.v1",
        "canonical_digest_policy",
        "digest_domain",
        "search_lexeme_utf8_v1",
    ),
)

_EXPECTED_DIGEST_DOMAINS = tuple(
    value.encode("ascii")
    for _seed_id, relation, _attribute, value in _EXPECTED_DATA_SEEDS
    if relation == "canonical_digest_policy"
)

# Wheel-resident copy of the logical manifest's closed canonical-reference
# registry.  Build-time verification compares this tuple with catalog.toml;
# production READY checks use it without depending on repository source files.
_CANONICAL_REFERENCE_ROLES = (
    ("artifact_delta_new", "artifact_semantics_sha256", b"artifact_semantics_v1"),
    ("artifact_delta_old", "artifact_semantics_sha256", b"artifact_semantics_v1"),
    ("artifact_input", "artifact_semantics_sha256", b"artifact_semantics_v1"),
    (
        "artifact_semantic_input",
        "artifact_semantics_sha256",
        b"artifact_semantics_v1",
    ),
    ("catalog_artifact", "artifact_semantics_sha256", b"artifact_semantics_v1"),
    (
        "analysis_content_owner_candidate_resolved",
        "content_sha256",
        b"effective_content_v1",
    ),
    (
        "analysis_content_owner_candidate_shadow",
        "content_sha256",
        b"effective_content_v1",
    ),
    ("analysis_content_owner_resolved", "content_sha256", b"effective_content_v1"),
    ("analysis_content_owner_shadow", "content_sha256", b"effective_content_v1"),
    (
        "analysis_content_owner_tombstone",
        "content_sha256",
        b"effective_content_v1",
    ),
    ("analysis_impacted_content", "content_sha256", b"effective_content_v1"),
    (
        "analysis_impacted_content_provenance",
        "content_sha256",
        b"effective_content_v1",
    ),
    ("catalog_publication_content", "content_sha256", b"effective_content_v1"),
    (
        "catalog_contributor",
        "contributor_name_sha256",
        b"contributor_name_utf8_v1",
    ),
    (
        "contributor_facet_order",
        "contributor_name_sha256",
        b"contributor_name_utf8_v1",
    ),
    (
        "artifact_semantic_input",
        "effective_content_component_sha256",
        b"artifact_effective_content_v1",
    ),
    ("gallery_identity", "locator_sha256", b"source_relative_locator_v1"),
    ("source_locator_identity", "locator_sha256", b"source_relative_locator_v1"),
    (
        "catalog_publication_storage",
        "language_sha256",
        b"catalog_language_utf8_v1",
    ),
    ("language_facet_order", "language_sha256", b"catalog_language_utf8_v1"),
    (
        "artifact_semantic_input",
        "member_plan_component_sha256",
        b"artifact_member_plan_v2",
    ),
    (
        "gallery_observation",
        "observation_identity_sha256",
        b"gallery_observation_v1",
    ),
    (
        "artifact_semantic_input",
        "owner_component_sha256",
        b"artifact_owner_v1",
    ),
    ("artifact_policy", "policy_component_sha256", b"artifact_policy_v3"),
    (
        "artifact_policy_semantics",
        "policy_component_sha256",
        b"artifact_policy_v3",
    ),
    (
        "artifact_semantic_input",
        "policy_component_sha256",
        b"artifact_policy_v3",
    ),
    (
        "artifact_semantic_input",
        "selected_component_sha256",
        b"artifact_selected_v1",
    ),
    ("title_sort", "sort_title_sha256", b"title_sort_utf8_v1"),
    (
        "artifact_semantic_input",
        "source_manifest_component_sha256",
        b"artifact_source_manifest_v1",
    ),
    ("source_scope", "source_root_sha256", b"source_root_v1"),
    (
        "source_snapshot_manifest_identity",
        "snapshot_manifest_sha256",
        b"source_snapshot_manifest_v1",
    ),
    (
        "catalog_publication_storage",
        "source_title_sha256",
        b"source_title_utf8_v1",
    ),
    ("display_title_choice", "source_title_sha256", b"source_title_utf8_v1"),
    (
        "catalog_publication_storage",
        "summary_sha256",
        b"catalog_summary_utf8_v1",
    ),
    ("tag_term", "tag_value_sha256", b"tag_value_utf8_v1"),
    ("display_title_choice", "title_sha256", b"display_title_utf8_v1"),
    ("title_sort", "title_sha256", b"display_title_utf8_v1"),
)
_EXPECTED_ANALYSIS_COMPONENTS = frozenset(
    {
        "file_hash_decision",
        "content_owner_candidate",
        "content_owner",
        "gid_candidate",
        "gid_winner",
    }
)
_EXPECTED_ARTIFACT_COMPONENTS = frozenset(
    {
        "source_manifest",
        "member_plan",
        "effective_content",
        "selected",
        "owner",
        "policy",
    }
)
_SAFE_IDENTIFIER = re.compile(r"[a-z][a-z0-9_]*\Z")
_MAX_ANALYSIS_OVERLAY_DEPTH = 16
_ANALYSIS_ANCESTRY_RESULT_LIMIT = _MAX_ANALYSIS_OVERLAY_DEPTH + 2
_IMPACTED_KEY_FAMILIES = (
    (
        "content",
        "content_sha256",
        "catalog_analysis_impacted_content",
        "catalog_a_impacted_content_provenance",
    ),
    (
        "gid",
        "gid",
        "catalog_analysis_impacted_gid",
        "catalog_a_impacted_gid_provenance",
    ),
)

# These are executable runtime registrations, not merely values admitted by the
# physical integer domains.  A new byte producer, display-title rule, casefold
# implementation, or Unicode table must add a wheel implementation and then
# extend the corresponding closed registry.
_SUPPORTED_ARTIFACT_ALGORITHM_VERSIONS = frozenset({2})
_RUNTIME_UNICODE_DATA_VERSION = unicodedata.unidata_version.encode("ascii")
_SUPPORTED_DISPLAY_TITLE_POLICY_ALGORITHMS = frozenset(
    {(1, 1, _RUNTIME_UNICODE_DATA_VERSION)}
)

_WIDE_POLICY_BOOTSTRAP_VALUES: Mapping[
    str, tuple[str, tuple[tuple[str, str, str, int | str], ...]]
] = MappingProxyType(
    {
        "search_policy": (
            "catalog.search-policy.discovery.v1",
            (
                ("policy_id", "uint64", "integer", 1),
                ("algorithm_version", "uint32", "integer", 2),
                ("unicode_data_version", "ascii_enum", "utf8", "16.0.0"),
                ("maximum_field_nfd_bytes", "uint32", "integer", 65536),
                ("maximum_query_nfd_bytes", "uint32", "integer", 1024),
                ("maximum_lexeme_bytes", "uint32", "integer", 64),
                ("maximum_query_lexemes", "uint32", "integer", 16),
            ),
        ),
    }
)

_WIDE_POLICY_SHAPES: Mapping[
    str,
    tuple[
        str,
        tuple[str, ...],
        tuple[str, ...],
        tuple[tuple[str, ...], ...],
        tuple[tuple[str, ...], ...],
    ],
] = MappingProxyType(
    {
        "manifest_policy": (
            "catalog_manifest_policies",
            ("manifest_policy_id", "manifest_algorithm_version", "file_order_version"),
            ("manifest_policy_id",),
            (("manifest_algorithm_version", "file_order_version"),),
            (
                ("manifest_policy_id", "manifest_algorithm_version"),
                ("manifest_policy_id", "file_order_version"),
            ),
        ),
        "analysis_policy": (
            "catalog_analysis_policies",
            (
                "policy_id",
                "algorithm_version",
                "spam_artist_threshold",
                "spam_occurrence_threshold",
                "content_owner_rule_version",
                "gid_winner_rule_version",
            ),
            ("policy_id",),
            (
                (
                    "algorithm_version",
                    "spam_artist_threshold",
                    "spam_occurrence_threshold",
                    "content_owner_rule_version",
                    "gid_winner_rule_version",
                ),
            ),
            tuple(
                ("policy_id", attribute)
                for attribute in (
                    "algorithm_version",
                    "spam_artist_threshold",
                    "spam_occurrence_threshold",
                    "content_owner_rule_version",
                    "gid_winner_rule_version",
                )
            ),
        ),
        "search_policy": (
            "catalog_search_policies",
            (
                "policy_id",
                "algorithm_version",
                "unicode_data_version",
                "maximum_field_nfd_bytes",
                "maximum_query_nfd_bytes",
                "maximum_lexeme_bytes",
                "maximum_query_lexemes",
            ),
            ("policy_id",),
            (
                (
                    "algorithm_version",
                    "unicode_data_version",
                    "maximum_field_nfd_bytes",
                    "maximum_query_nfd_bytes",
                    "maximum_lexeme_bytes",
                    "maximum_query_lexemes",
                ),
            ),
            (),
        ),
        "artifact_policy_semantics": (
            "catalog_artifact_policy_semantics",
            (
                "policy_component_sha256",
                "artifact_algorithm_version",
                "policy_fingerprint_sha256",
            ),
            ("policy_component_sha256",),
            (
                (
                    "artifact_algorithm_version",
                    "policy_fingerprint_sha256",
                ),
            ),
            (),
        ),
        "artifact_adapter_policy": (
            "catalog_artifact_adapter_policy",
            ("policy_fingerprint_sha256", "adapter_id"),
            ("policy_fingerprint_sha256",),
            (),
            (),
        ),
        "title_sort_policy": (
            "catalog_title_sort_policy",
            (
                "title_sort_policy_id",
                "title_sort_algorithm_version",
                "unicode_data_version",
            ),
            ("title_sort_policy_id",),
            (("title_sort_algorithm_version", "unicode_data_version"),),
            (
                ("title_sort_policy_id", "title_sort_algorithm_version"),
                ("title_sort_policy_id", "unicode_data_version"),
            ),
        ),
        "display_title_policy": (
            "catalog_display_title_policies",
            (
                "display_title_policy_id",
                "display_title_algorithm_version",
                "title_sort_policy_id",
            ),
            ("display_title_policy_id",),
            (("display_title_algorithm_version", "title_sort_policy_id"),),
            (
                ("display_title_policy_id", "display_title_algorithm_version"),
                ("display_title_policy_id", "title_sort_policy_id"),
            ),
        ),
        "source_scope": (
            "catalog_source_scopes",
            (
                "scope_key",
                "source_provider",
                "source_root_sha256",
                "identity_policy_version",
            ),
            ("scope_key",),
            (("source_provider", "source_root_sha256", "identity_policy_version"),),
            (
                ("scope_key", "source_provider"),
                ("scope_key", "source_root_sha256"),
                ("scope_key", "identity_policy_version"),
            ),
        ),
    }
)


@dataclass(frozen=True, slots=True)
class _SourceContext:
    channel: bytes
    source_revision: int
    generation: int
    snapshot_manifest_sha256: bytes
    analysis_id: bytes | None
    analysis_policy_id: int | None
    build_id: bytes | None
    scope_key: bytes | None


@dataclass(frozen=True, slots=True)
class _PublicationContext:
    channel: bytes
    revision: int
    generation: int
    publication_count: int
    candidate_id: bytes
    analysis_id: bytes | None
    source_revision: int
    artifact_policy_id: int
    display_title_policy_id: int


def _obligations() -> tuple[Mapping[str, Any], ...]:
    raw = ARTIFACT.get("semantic_obligations")
    if not isinstance(raw, tuple) or not all(isinstance(value, dict) for value in raw):
        raise BuiltinSemanticRegistryError(
            "generated semantic-obligation records are malformed"
        )
    return raw


def validate_builtin_semantic_manifest() -> None:
    """Require exact generated ID/lifecycle/check and writer-hook registries."""

    actual: list[tuple[str, str, str]] = []
    for value in _obligations():
        obligation_id = value.get("id")
        contract = value.get("contract")
        if not isinstance(obligation_id, str) or not isinstance(contract, Mapping):
            raise BuiltinSemanticRegistryError(
                "generated semantic-obligation record is malformed"
            )
        lifecycle = contract.get("lifecycle")
        check = contract.get("ready_check")
        if not isinstance(lifecycle, str) or not isinstance(check, str):
            raise BuiltinSemanticRegistryError(
                "generated semantic obligation lacks lifecycle/check"
            )
        actual.append((obligation_id, lifecycle, check))
    if tuple(actual) != _SPECS:
        raise BuiltinSemanticRegistryError(
            "generated semantic-obligation manifest differs from the wheel registry"
        )
    try:
        validate_artifact_writer_manifest(_obligations())
    except Exception as error:
        raise BuiltinSemanticRegistryError(
            "generated writer-hook manifest differs from the wheel registry"
        ) from error


def _provider_columns(relation: Mapping[str, object]) -> tuple[str, ...]:
    raw = relation.get("columns")
    if not isinstance(raw, tuple) or not all(
        isinstance(column, tuple) and len(column) >= 2 and isinstance(column[0], str)
        for column in raw
    ):
        return ()
    return tuple(column[0] for column in raw)


def _validate_wide_policy_provider_shapes(
    relation_by_name: Mapping[object, object], *, backend: str
) -> None:
    """Pin runtime policy authorities to their exact atomic BCNF rows."""

    for family, (
        table,
        columns,
        primary_key,
        unique_keys,
        referential_unique_keys,
    ) in _WIDE_POLICY_SHAPES.items():
        relation = relation_by_name.get(family)
        if (
            not isinstance(relation, Mapping)
            or relation.get("kind") != "table"
            or relation.get("table") != table
            or _provider_columns(relation) != columns
            or relation.get("primary_key") != primary_key
            or relation.get("unique_keys") != unique_keys
            or relation.get("referential_unique_keys") != referential_unique_keys
        ):
            raise BuiltinSemanticRegistryError(
                f"generated {backend} {family} lacks its exact wide BCNF shape"
            )


def _validate_static_catalog_contract() -> None:
    validate_builtin_semantic_manifest()
    raw = ARTIFACT.get("bootstrap_seeds")
    if not isinstance(raw, tuple):
        raise BuiltinSemanticRegistryError("generated bootstrap records are malformed")
    actual: list[tuple[str, str, str, str]] = []
    actual_stage_registries: dict[str, list[tuple[str, tuple[str, ...]]]] = {
        "analysis_stage": [],
        "publication_stage": [],
    }
    actual_wide_policy_seeds: list[
        tuple[str, str, tuple[tuple[str, str, str, int | str], ...]]
    ] = []
    generation_genesis_seen = False
    for value in raw:
        if not isinstance(value, Mapping) or value.get("source") != "data":
            continue
        seed_id = value.get("id")
        relation = value.get("relation")
        version = value.get("version")
        cells = value.get("value")
        if (
            not isinstance(seed_id, str)
            or not isinstance(relation, str)
            or version != 1
            or not isinstance(cells, tuple)
        ):
            raise BuiltinSemanticRegistryError("generated catalog seed is malformed")
        if relation in actual_stage_registries:
            if (
                len(cells) != 3
                or any(not isinstance(cell, tuple) or len(cell) != 4 for cell in cells)
                or tuple(cell[0] for cell in cells)
                != ("stage", "stage_order", "cursor_codec")
                or any(cell[1:3] != ("ascii_enum", "utf8") for cell in cells)
                or any(not isinstance(cell[3], str) for cell in cells)
            ):
                raise BuiltinSemanticRegistryError(
                    f"generated {relation.replace('_', '-')} seed is malformed"
                )
            actual_stage_registries[relation].append(
                (seed_id, tuple(cell[3] for cell in cells))
            )
            continue
        if relation in _WIDE_POLICY_BOOTSTRAP_VALUES:
            if not cells or any(
                not isinstance(cell, tuple)
                or len(cell) != 4
                or not isinstance(cell[0], str)
                or not isinstance(cell[1], str)
                or not isinstance(cell[2], str)
                or not isinstance(cell[3], int | str)
                or isinstance(cell[3], bool)
                for cell in cells
            ):
                raise BuiltinSemanticRegistryError(
                    f"generated {relation.replace('_', '-')} seed is malformed"
                )
            actual_wide_policy_seeds.append((seed_id, relation, cells))
            continue
        if relation == "publication_generation_node":
            if generation_genesis_seen or (
                seed_id != "catalog.publication-generation.genesis.v1"
                or cells != (("generation", "uint64", "integer", 0),)
            ):
                raise BuiltinSemanticRegistryError(
                    "generated publication-generation genesis seed is malformed"
                )
            generation_genesis_seen = True
            continue
        if len(cells) != 1 or not isinstance(cells[0], tuple) or len(cells[0]) != 4:
            raise BuiltinSemanticRegistryError("generated catalog seed is malformed")
        attribute, type_name, encoding, text = cells[0]
        if (
            not isinstance(attribute, str)
            or type_name != "ascii_enum"
            or encoding != "utf8"
            or not isinstance(text, str)
        ):
            raise BuiltinSemanticRegistryError(
                "generated catalog seed cell is malformed"
            )
        actual.append((seed_id, relation, attribute, text))
    if tuple(actual) != tuple(
        value
        for value in _EXPECTED_DATA_SEEDS
        if value[1] not in actual_stage_registries
    ):
        raise BuiltinSemanticRegistryError(
            "generated catalog bootstrap registry differs from executable constants"
        )
    if not generation_genesis_seen:
        raise BuiltinSemanticRegistryError(
            "generated publication-generation genesis seed is missing"
        )
    expected_wide_policy_seeds = tuple(
        (seed_id, family, cells)
        for family, (seed_id, cells) in _WIDE_POLICY_BOOTSTRAP_VALUES.items()
    )
    if tuple(actual_wide_policy_seeds) != expected_wide_policy_seeds:
        raise BuiltinSemanticRegistryError(
            "generated wide policy seeds differ from executable constants"
        )
    analysis_stage_values = (
        ("changed_gallery", 1, "analysis_gallery_v1"),
        ("changed_file_hash", 2, "analysis_digest_v1"),
        ("file_hash_decision", 3, "analysis_digest_v1"),
        ("validate_file_hash_decision", 4, "analysis_digest_live_v1"),
        ("impacted_gallery", 5, "analysis_gallery_v1"),
        ("impacted_content", 6, "analysis_gallery_v1"),
        ("content_owner_candidate", 7, "analysis_gallery_v1"),
        ("validate_content_owner_candidate", 8, "analysis_gallery_live_v1"),
        ("content_owner", 9, "analysis_digest_v1"),
        ("validate_content_owner", 10, "analysis_digest_live_v1"),
        ("impacted_gid", 11, "analysis_gallery_v1"),
        ("gid_candidate", 12, "analysis_gallery_v1"),
        ("validate_gid_candidate", 13, "analysis_gallery_live_v1"),
        ("gid_winner", 14, "analysis_gid_v1"),
        ("validate_gid_winner", 15, "analysis_gid_live_v1"),
    )
    expected_analysis_stages = tuple(
        (
            f"catalog.analysis-stage.{name.replace('_', '-')}.v1",
            (name, f"{order:02d}", cursor_codec),
        )
        for name, order, cursor_codec in analysis_stage_values
    )
    if tuple(actual_stage_registries["analysis_stage"]) != expected_analysis_stages:
        raise BuiltinSemanticRegistryError(
            "generated analysis-stage registry differs from executable constants"
        )
    publication_stage_values = (
        ("BUILD_SELECTION", 1, "publication_gallery_v1"),
        ("VALIDATE_SELECTION", 2, "publication_gallery_v1"),
        (
            "BUILD_CATALOG_PROJECTION",
            3,
            "publication_catalog_child_v1",
        ),
        (
            "VALIDATE_CATALOG_PROJECTION",
            4,
            "publication_catalog_child_v1",
        ),
        ("BUILD_ARTIFACT_INPUT", 5, "publication_key_v1"),
        ("BUILD_ARTIFACT_DELTA_OPERATION", 6, "publication_key_v1"),
        ("VALIDATE_ARTIFACT_INPUT_DELTA", 7, "publication_key_v1"),
        ("VALIDATE_PREPARED_ARTIFACT", 8, "publication_key_v1"),
        ("VALIDATE_CREATE", 9, "publication_key_v1"),
        ("VALIDATE_REBUILD", 10, "publication_key_v1"),
        ("VALIDATE_DELETE", 11, "publication_key_v1"),
        ("VALIDATE_UNCHANGED", 12, "publication_key_v1"),
        ("VALIDATE_NEW_GALLERY", 13, "publication_key_v1"),
        ("VALIDATE_CHANGED_GALLERY", 14, "publication_key_v1"),
        ("VALIDATE_REMOVED_GALLERY", 15, "publication_key_v1"),
        ("VALIDATE_DUPLICATE_LOSER", 16, "publication_gallery_v1"),
        ("FINALIZE_ARTIFACTS", 17, "publication_resource_v2"),
    )
    expected_publication_stages = tuple(
        (
            f"catalog.publication-stage.{name.lower().replace('_', '-')}.v1",
            (name, f"{order:02d}", cursor_codec),
        )
        for name, order, cursor_codec in publication_stage_values
    )
    if (
        tuple(actual_stage_registries["publication_stage"])
        != expected_publication_stages
    ):
        raise BuiltinSemanticRegistryError(
            "generated publication-stage registry differs from executable constants"
        )
    if identity.ANALYSIS_STATE_COMPONENTS != _EXPECTED_ANALYSIS_COMPONENTS:
        raise BuiltinSemanticRegistryError("analysis component codec registry drifted")
    if identity.ARTIFACT_COMPONENT_KINDS != _EXPECTED_ARTIFACT_COMPONENTS:
        raise BuiltinSemanticRegistryError("artifact component codec registry drifted")
    if not _RUNTIME_UNICODE_DATA_VERSION or len(_RUNTIME_UNICODE_DATA_VERSION) > 32:
        raise BuiltinSemanticRegistryError(
            "runtime Unicode data version is outside the catalog byte domain"
        )

    backends = ARTIFACT.get("backends")
    if not isinstance(backends, Mapping):
        raise BuiltinSemanticRegistryError("generated backend payloads are malformed")
    for backend in ("sqlite", "mariadb"):
        payload = backends.get(backend)
        relations = payload.get("relations") if isinstance(payload, Mapping) else None
        if not isinstance(relations, tuple):
            raise BuiltinSemanticRegistryError(
                f"generated {backend} relation registry is malformed"
            )
        receipt_relations = tuple(
            value
            for value in relations
            if isinstance(value, Mapping)
            and value.get("plane") == "data"
            and value.get("relation") == "publication_receipt"
        )
        if len(receipt_relations) != 1:
            raise BuiltinSemanticRegistryError(
                f"generated {backend} publication_receipt contract is not singular"
            )
        columns = receipt_relations[0].get("columns")
        if not isinstance(columns, tuple) or not any(
            isinstance(column, tuple)
            and len(column) >= 2
            and column[0] == "publication_count"
            and column[1] == "publication_count"
            for column in columns
        ):
            raise BuiltinSemanticRegistryError(
                "publication_receipt lacks the bounded authoritative "
                f"publication_count scalar on {backend}"
            )
        order_relations = tuple(
            value
            for value in relations
            if isinstance(value, Mapping)
            and value.get("plane") == "data"
            and value.get("relation") == "catalog_publication_order"
        )
        if len(order_relations) != 1:
            raise BuiltinSemanticRegistryError(
                f"generated {backend} catalog_publication_order is not singular"
            )
        order_relation = order_relations[0]
        order_columns = order_relation.get("columns")
        if (
            order_relation.get("table") != "catalog_publication_order"
            or order_relation.get("primary_key") != ("revision", "position")
            or order_relation.get("unique_keys") != (("revision", "publication_key"),)
            or not isinstance(order_columns, tuple)
            or tuple(
                column[0]
                for column in order_columns
                if isinstance(column, tuple) and len(column) >= 2
            )
            != ("revision", "position", "publication_key")
        ):
            raise BuiltinSemanticRegistryError(
                "catalog_publication_order lacks its exact BCNF paging shape "
                f"on {backend}"
            )

        relation_by_name = {
            value.get("relation"): value
            for value in relations
            if isinstance(value, Mapping) and value.get("plane") == "data"
        }
        _validate_wide_policy_provider_shapes(relation_by_name, backend=backend)
        expected_shapes = {
            "publication_identity": (
                "catalog_publication_identities",
                ("publication_key", "gid"),
                ("publication_key",),
                (("gid",),),
            ),
            "contributor_role_registry": (
                "catalog_contributor_role_registry",
                ("role",),
                ("role",),
                (),
            ),
            "prepared_artifact": (
                "catalog_prepared_artifacts",
                (
                    "candidate_id",
                    "publication_key",
                    "resource_kind",
                    "storage_object_key_sha256",
                    "storage_generation",
                    "protection_token",
                    "state",
                ),
                ("candidate_id", "publication_key", "resource_kind"),
                (("protection_token",),),
            ),
            "prepared_resource_blob": (
                "catalog_prepared_resource_blob",
                (
                    "candidate_id",
                    "publication_key",
                    "resource_kind",
                    "storage_object_sha256",
                ),
                ("candidate_id", "publication_key", "resource_kind"),
                (),
            ),
            "catalog_resource_kind": (
                "catalog_resource_kinds",
                ("resource_kind",),
                ("resource_kind",),
                (),
            ),
            "storage_object_key_identity": (
                "catalog_storage_object_key_identities",
                ("storage_object_key_sha256", "key_codec", "segment_count"),
                ("storage_object_key_sha256",),
                (),
            ),
            "storage_object_key_segment": (
                "catalog_storage_object_key_segments",
                ("storage_object_key_sha256", "segment_position", "key_segment"),
                ("storage_object_key_sha256", "segment_position"),
                (),
            ),
            "prepared_storage_object": (
                "catalog_prepared_storage_objects",
                (
                    "candidate_id",
                    "publication_key",
                    "resource_kind",
                    "storage_object_sha256",
                    "size_bytes",
                    "modified_at",
                ),
                ("candidate_id", "publication_key", "resource_kind"),
                (),
            ),
            "prepared_artifact_descriptor": (
                "catalog_prepared_artifact_descriptors",
                (
                    "candidate_id",
                    "publication_key",
                    "artifact_sha256",
                    "artifact_name",
                    "media_type",
                    "page_count",
                ),
                ("candidate_id", "publication_key"),
                (),
            ),
            "prepared_page": (
                "catalog_prepared_pages",
                (
                    "candidate_id",
                    "publication_key",
                    "resource_kind",
                    "page_index",
                    "extent_offset",
                    "extent_length",
                    "media_type",
                    "image_sha256",
                    "width",
                    "height",
                ),
                ("candidate_id", "publication_key", "page_index"),
                (),
            ),
            "prepared_thumbnail": (
                "catalog_prepared_thumbnails",
                (
                    "candidate_id",
                    "publication_key",
                    "resource_kind",
                    "extent_offset",
                    "extent_length",
                    "media_type",
                    "image_sha256",
                    "width",
                    "height",
                ),
                ("candidate_id", "publication_key"),
                (),
            ),
            "artifact_semantic_input": (
                "catalog_artifact_semantic_inputs",
                (
                    "artifact_semantics_sha256",
                    "source_manifest_component_sha256",
                    "member_plan_component_sha256",
                    "effective_content_component_sha256",
                    "selected_component_sha256",
                    "owner_component_sha256",
                    "policy_component_sha256",
                ),
                ("artifact_semantics_sha256",),
                (
                    (
                        "source_manifest_component_sha256",
                        "member_plan_component_sha256",
                        "effective_content_component_sha256",
                        "selected_component_sha256",
                        "owner_component_sha256",
                        "policy_component_sha256",
                    ),
                ),
            ),
            "artifact_blob": (
                "catalog_artifact_blobs",
                ("artifact_sha256", "size_bytes"),
                ("artifact_sha256",),
                (),
            ),
            "catalog_artifact": (
                "catalog_artifacts",
                (
                    "revision",
                    "publication_key",
                    "artifact_sha256",
                    "artifact_semantics_sha256",
                    "artifact_name",
                    "media_type",
                    "page_count",
                ),
                ("revision", "publication_key"),
                (),
            ),
            "catalog_storage_object": (
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
                ("revision", "publication_key", "resource_kind"),
                (),
            ),
            "catalog_page": (
                "catalog_pages",
                (
                    "revision",
                    "publication_key",
                    "resource_kind",
                    "page_index",
                    "extent_offset",
                    "extent_length",
                    "media_type",
                    "image_sha256",
                    "width",
                    "height",
                ),
                ("revision", "publication_key", "page_index"),
                (),
            ),
            "catalog_thumbnail": (
                "catalog_thumbnails",
                (
                    "revision",
                    "publication_key",
                    "resource_kind",
                    "extent_offset",
                    "extent_length",
                    "media_type",
                    "image_sha256",
                    "width",
                    "height",
                ),
                ("revision", "publication_key"),
                (),
            ),
            "analysis_stage": (
                "catalog_analysis_stages",
                ("stage", "stage_order", "cursor_codec"),
                ("stage",),
                (("stage_order",),),
            ),
            "analysis_snapshot_manifest": (
                "catalog_analysis_snapshot_manifest",
                ("analysis_id", "snapshot_manifest_sha256"),
                ("analysis_id",),
                (),
            ),
            "analysis_checkpoint": (
                "catalog_analysis_checkpoints",
                (
                    "analysis_id",
                    "stage",
                    "generation",
                    "cursor",
                    "processed_count",
                    "state",
                    "updated_at",
                ),
                ("analysis_id", "stage"),
                (),
            ),
            "analysis_batch_receipt": (
                "catalog_analysis_batch_receipts",
                (
                    "analysis_id",
                    "stage",
                    "batch_key",
                    "start_generation",
                    "start_cursor",
                    "start_processed_count",
                    "page_limit",
                    "next_cursor",
                    "next_processed_count",
                    "next_state",
                    "row_count",
                    "terminal",
                    "committed_generation",
                    "committed_at",
                ),
                ("analysis_id", "stage", "batch_key"),
                (
                    ("analysis_id", "stage", "start_generation"),
                    ("analysis_id", "stage", "committed_generation"),
                ),
            ),
            "publication_candidate_projection_seal": (
                "catalog_publication_candidate_projection_seals",
                ("candidate_id",),
                ("candidate_id",),
                (),
            ),
            "publication_candidate_projection": (
                "catalog_publication_candidate_projections",
                (
                    "candidate_id",
                    "create_count",
                    "rebuild_count",
                    "delete_count",
                    "new_galleries",
                    "changed_galleries",
                ),
                ("candidate_id",),
                (),
            ),
            "publication_selection": (
                "catalog_publication_selections",
                ("candidate_id", "gallery_id", "publication_key"),
                ("candidate_id", "gallery_id"),
                (("candidate_id", "publication_key"),),
            ),
            "publication_stage": (
                "catalog_publication_stages",
                ("stage", "stage_order", "cursor_codec"),
                ("stage",),
                (("stage_order",),),
            ),
            "publication_checkpoint": (
                "catalog_publication_checkpoints",
                (
                    "candidate_id",
                    "stage",
                    "generation",
                    "cursor",
                    "processed_count",
                    "state",
                    "updated_at",
                ),
                ("candidate_id", "stage"),
                (),
            ),
            "publication_batch_receipt": (
                "catalog_publication_batch_receipts",
                (
                    "candidate_id",
                    "stage",
                    "batch_key",
                    "start_generation",
                    "start_cursor",
                    "start_processed_count",
                    "next_cursor",
                    "next_processed_count",
                    "next_state",
                    "row_count",
                    "terminal",
                    "committed_generation",
                    "committed_at",
                ),
                ("candidate_id", "stage", "batch_key"),
                (
                    ("candidate_id", "stage", "start_generation"),
                    ("candidate_id", "stage", "committed_generation"),
                ),
            ),
        }
        for relation_name, (
            expected_table,
            expected_columns,
            expected_primary_key,
            expected_unique_keys,
        ) in expected_shapes.items():
            relation = relation_by_name.get(relation_name)
            columns = relation.get("columns") if isinstance(relation, Mapping) else None
            if (
                not isinstance(relation, Mapping)
                or relation.get("table") != expected_table
                or not isinstance(columns, tuple)
                or tuple(
                    column[0]
                    for column in columns
                    if isinstance(column, tuple) and len(column) >= 2
                )
                != expected_columns
                or relation.get("primary_key") != expected_primary_key
                or relation.get("unique_keys") != expected_unique_keys
            ):
                raise BuiltinSemanticRegistryError(
                    f"generated {backend} {relation_name} lacks its exact BCNF authority shape"
                )


def _as_bytes(value: object, *, field: str) -> bytes:
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, bytes):
        return value
    raise CatalogSemanticValidationError(f"{field} is not exact binary data")


def _as_int(value: object, *, field: str, positive: bool = False) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise CatalogSemanticValidationError(f"{field} is not an integer")
    if value < (1 if positive else 0) or value > (1 << 63) - 1:
        domain = "positive int63" if positive else "nonnegative int63"
        raise CatalogSemanticValidationError(f"{field} is outside {domain}")
    return value


def _one(
    connector: SQLConnector,
    query: str,
    data: tuple[Any, ...],
    *,
    detail: str,
    optional: bool = False,
) -> tuple[Any, ...] | None:
    rows = connector.fetch_all(query, data)
    if not rows and optional:
        return None
    if len(rows) != 1:
        raise CatalogSemanticValidationError(
            f"{detail} must resolve to exactly one row; observed {len(rows)}"
        )
    return rows[0]


def _validate_exact_registries(connector: SQLConnector) -> None:
    _validate_static_catalog_contract()
    channels = connector.fetch_all(
        "SELECT channel FROM catalog_channel_registry ORDER BY channel LIMIT 2"
    )
    if tuple(_as_bytes(row[0], field="channel registry value") for row in channels) != (
        b"default",
    ):
        raise CatalogSemanticValidationError(
            "channel_registry is not the exact singleton {default}"
        )
    providers = connector.fetch_all(
        "SELECT source_provider FROM catalog_source_provider_registry "
        "ORDER BY source_provider LIMIT 2"
    )
    if tuple(
        _as_bytes(row[0], field="source-provider registry value") for row in providers
    ) != (b"filesystem",):
        raise CatalogSemanticValidationError(
            "source_provider_registry is not the exact singleton {filesystem}"
        )
    resource_kinds = connector.fetch_all(
        "SELECT resource_kind FROM catalog_resource_kinds "
        "ORDER BY resource_kind LIMIT 3"
    )
    if tuple(
        _as_bytes(row[0], field="catalog resource kind") for row in resource_kinds
    ) != (b"acquisition", b"thumbnail"):
        raise CatalogSemanticValidationError(
            "catalog_resource_kind is not the exact neutral two-role registry"
        )
    _search_seed_id, search_cells = _WIDE_POLICY_BOOTSTRAP_VALUES["search_policy"]
    # A bounded scan of the singleton registry, like the other exact registries.
    search_policies = connector.fetch_all(
        "SELECT "
        + ", ".join(name for name, _domain, _storage, _value in search_cells)
        + " FROM catalog_search_policies ORDER BY policy_id LIMIT 2"
    )
    expected_search_policy = tuple(
        value.encode("ascii") if isinstance(value, str) else value
        for _name, _domain, _storage, value in search_cells
    )
    actual_search_policies = tuple(
        tuple(
            _as_bytes(cell, field=f"search policy {name}")
            if storage == "utf8"
            else _as_int(cell, field=f"search policy {name}")
            for (name, _domain, storage, _value), cell in zip(
                search_cells, row, strict=True
            )
        )
        for row in search_policies
    )
    if actual_search_policies != (expected_search_policy,):
        raise CatalogSemanticValidationError(
            "search_policy registry is not the exact bootstrap singleton"
        )
    stages = connector.fetch_all(
        "SELECT stage, stage_order, cursor_codec "
        "FROM catalog_analysis_stages ORDER BY stage_order LIMIT 16"
    )
    expected_stages = tuple(
        (name.encode("ascii"), f"{order:02d}".encode("ascii"), codec.encode("ascii"))
        for name, order, codec in (
            ("changed_gallery", 1, "analysis_gallery_v1"),
            ("changed_file_hash", 2, "analysis_digest_v1"),
            ("file_hash_decision", 3, "analysis_digest_v1"),
            ("validate_file_hash_decision", 4, "analysis_digest_live_v1"),
            ("impacted_gallery", 5, "analysis_gallery_v1"),
            ("impacted_content", 6, "analysis_gallery_v1"),
            ("content_owner_candidate", 7, "analysis_gallery_v1"),
            ("validate_content_owner_candidate", 8, "analysis_gallery_live_v1"),
            ("content_owner", 9, "analysis_digest_v1"),
            ("validate_content_owner", 10, "analysis_digest_live_v1"),
            ("impacted_gid", 11, "analysis_gallery_v1"),
            ("gid_candidate", 12, "analysis_gallery_v1"),
            ("validate_gid_candidate", 13, "analysis_gallery_live_v1"),
            ("gid_winner", 14, "analysis_gid_v1"),
            ("validate_gid_winner", 15, "analysis_gid_live_v1"),
        )
    )
    actual_stages = tuple(
        tuple(
            _as_bytes(value, field=f"analysis stage registry field {index}")
            for index, value in enumerate(row)
        )
        for row in stages
    )
    if actual_stages != expected_stages:
        raise CatalogSemanticValidationError(
            "analysis_stage is not the exact closed fifteen-stage registry"
        )
    publication_stages = connector.fetch_all(
        "SELECT stage, stage_order, cursor_codec "
        "FROM catalog_publication_stages ORDER BY stage_order LIMIT 18"
    )
    expected_publication_stages = tuple(
        (name.encode("ascii"), f"{order:02d}".encode("ascii"), codec.encode("ascii"))
        for name, order, codec in (
            ("BUILD_SELECTION", 1, "publication_gallery_v1"),
            ("VALIDATE_SELECTION", 2, "publication_gallery_v1"),
            (
                "BUILD_CATALOG_PROJECTION",
                3,
                "publication_catalog_child_v1",
            ),
            (
                "VALIDATE_CATALOG_PROJECTION",
                4,
                "publication_catalog_child_v1",
            ),
            ("BUILD_ARTIFACT_INPUT", 5, "publication_key_v1"),
            ("BUILD_ARTIFACT_DELTA_OPERATION", 6, "publication_key_v1"),
            ("VALIDATE_ARTIFACT_INPUT_DELTA", 7, "publication_key_v1"),
            ("VALIDATE_PREPARED_ARTIFACT", 8, "publication_key_v1"),
            ("VALIDATE_CREATE", 9, "publication_key_v1"),
            ("VALIDATE_REBUILD", 10, "publication_key_v1"),
            ("VALIDATE_DELETE", 11, "publication_key_v1"),
            ("VALIDATE_UNCHANGED", 12, "publication_key_v1"),
            ("VALIDATE_NEW_GALLERY", 13, "publication_key_v1"),
            ("VALIDATE_CHANGED_GALLERY", 14, "publication_key_v1"),
            ("VALIDATE_REMOVED_GALLERY", 15, "publication_key_v1"),
            ("VALIDATE_DUPLICATE_LOSER", 16, "publication_gallery_v1"),
            ("FINALIZE_ARTIFACTS", 17, "publication_resource_v2"),
        )
    )
    actual_publication_stages = tuple(
        tuple(
            _as_bytes(value, field=f"publication stage registry field {index}")
            for index, value in enumerate(row)
        )
        for row in publication_stages
    )
    if actual_publication_stages != expected_publication_stages:
        raise CatalogSemanticValidationError(
            "publication_stage is not the exact closed seventeen-stage registry"
        )
    domains = connector.fetch_all(
        "SELECT digest_domain FROM catalog_canonical_digest_policies "
        f"ORDER BY digest_domain LIMIT {len(_EXPECTED_DIGEST_DOMAINS) + 1}"
    )
    actual_domains = tuple(
        _as_bytes(row[0], field="canonical digest-domain registry value")
        for row in domains
    )
    if actual_domains != tuple(sorted(_EXPECTED_DIGEST_DOMAINS)):
        raise CatalogSemanticValidationError(
            "canonical_digest_policy is not the exact closed digest-domain registry"
        )


def _require_canonical_domain(
    connector: SQLConnector,
    value_sha256: bytes,
    expected_domain: bytes,
    *,
    detail: str,
) -> None:
    row = _one(
        connector,
        """
        SELECT allocation.digest_domain
        FROM catalog_canonical_value_identities AS identity_row
        JOIN catalog_canonical_value_allocations AS allocation
          ON allocation.value_sha256 = identity_row.value_sha256
        WHERE identity_row.value_sha256 = %s
        LIMIT 2
        """,
        (value_sha256,),
        detail=detail,
    )
    assert row is not None
    if _as_bytes(row[0], field=f"{detail} digest_domain") != expected_domain:
        raise CatalogSemanticValidationError(
            f"{detail} is sealed under the wrong canonical digest domain"
        )


def _generated_catalog_relation_name(logical_relation: str) -> str | None:
    """Resolve one trusted logical relation to its portable SQL object name."""

    object_names: set[str] = set()
    if logical_relation in ARTIFACT["data_inline_projections"]:
        return None
    for backend in ("sqlite", "mariadb"):
        relations = ARTIFACT["backends"][backend]["relations"]
        matches = tuple(
            relation
            for relation in relations
            if relation["plane"] == "data" and relation["relation"] == logical_relation
        )
        if len(matches) != 1:
            raise BuiltinSemanticRegistryError(
                f"generated schema does not uniquely map {logical_relation!r}"
            )
        object_names.add(str(matches[0]["table"]))
    if len(object_names) != 1:
        raise BuiltinSemanticRegistryError(
            f"generated backends disagree on {logical_relation!r}"
        )
    object_name = object_names.pop()
    if _SAFE_IDENTIFIER.fullmatch(object_name) is None:
        raise BuiltinSemanticRegistryError(
            f"generated schema has an unsafe relation name {object_name!r}"
        )
    return object_name


def _validate_retained_canonical_reference_domains(connector: SQLConnector) -> None:
    """Reject any retained canonical FK sealed under another digest domain."""

    for logical_relation, attribute, expected_domain in _CANONICAL_REFERENCE_ROLES:
        if _SAFE_IDENTIFIER.fullmatch(attribute) is None:
            raise BuiltinSemanticRegistryError(
                f"canonical registry has an unsafe attribute name {attribute!r}"
            )
        object_name = _generated_catalog_relation_name(logical_relation)
        if object_name is None:
            # Inline projections have no independently stored reference; their
            # source relations are covered by their own registry entries.
            continue
        invalid = connector.fetch_all(
            f"SELECT reference_row.{attribute} FROM {object_name} AS reference_row "
            "LEFT JOIN catalog_canonical_value_identities AS identity_row "
            f"ON identity_row.value_sha256 = reference_row.{attribute} "
            "LEFT JOIN catalog_canonical_value_allocations AS allocation "
            f"ON allocation.value_sha256 = reference_row.{attribute} "
            "WHERE identity_row.value_sha256 IS NULL "
            "OR allocation.value_sha256 IS NULL "
            "OR allocation.digest_domain <> %s LIMIT 1",
            (expected_domain,),
        )
        if invalid:
            raise CatalogSemanticValidationError(
                f"{logical_relation}.{attribute} is not sealed under "
                f"{expected_domain.decode('ascii')}"
            )


def _validate_live_snapshot_manifest_pins(connector: SQLConnector) -> None:
    """Require payload only for transient analysis/candidate retention pins.

    Historical analysis and source-revision bindings are compact opaque audit
    digests.  They deliberately do not participate in this query.  A digest
    becomes payload authority only while its analysis build occupies a source
    working slot or its publication candidate is uncommitted/operationally
    live.  The current published source is validated separately by
    :func:`_active_source_contexts`.
    """

    invalid = connector.fetch_all(
        """
        SELECT pinned.snapshot_manifest_sha256
        FROM (
            SELECT binding.snapshot_manifest_sha256
            FROM catalog_analysis_snapshot_manifest AS binding
            JOIN catalog_analysis_run_descriptor AS run_build
              ON run_build.analysis_id = binding.analysis_id
            JOIN operational_source_working_builds AS working_build
              ON working_build.build_id = run_build.build_id
            UNION
            SELECT binding.snapshot_manifest_sha256
            FROM catalog_publication_candidates AS candidate
            JOIN catalog_analysis_snapshot_manifest AS binding
              ON binding.analysis_id = candidate.analysis_id
            LEFT JOIN catalog_publication_commits AS committed
              ON committed.candidate_id = candidate.candidate_id
            LEFT JOIN operational_catalog_working_candidates AS working_candidate
              ON working_candidate.candidate_id = candidate.candidate_id
            WHERE committed.candidate_id IS NULL
               OR working_candidate.candidate_id IS NOT NULL
        ) AS pinned
        LEFT JOIN catalog_source_snapshot_manifest_identity AS snapshot_identity
          ON snapshot_identity.snapshot_manifest_sha256 =
             pinned.snapshot_manifest_sha256
        LEFT JOIN catalog_canonical_value_identities AS canonical_identity
          ON canonical_identity.value_sha256 = pinned.snapshot_manifest_sha256
        LEFT JOIN catalog_canonical_value_allocations AS allocation
          ON allocation.value_sha256 = pinned.snapshot_manifest_sha256
        WHERE LENGTH(pinned.snapshot_manifest_sha256) <> 32
           OR snapshot_identity.snapshot_manifest_sha256 IS NULL
           OR canonical_identity.value_sha256 IS NULL
           OR allocation.value_sha256 IS NULL
           OR allocation.digest_domain <> %s
        LIMIT 1
        """,
        (b"source_snapshot_manifest_v1",),
    )
    if invalid:
        raise CatalogSemanticValidationError(
            "live source-working analysis or publication-candidate snapshot "
            "semantic pin lacks a complete source_snapshot_manifest_v1 payload"
        )


def _require_compacted_current_source_handoff(
    connector: SQLConnector,
    *,
    channel: bytes,
    revision: int,
    generation: int,
    analysis_id: bytes,
    build_id: bytes,
) -> None:
    """Prove that a missing predecessor pin was safely handed to current state."""

    if generation <= 1:
        raise CatalogSemanticValidationError(
            "compacted current source handoff is not a successor generation"
        )
    predecessor_row = _one(
        connector,
        """
        SELECT committed.receipt_id, descriptor.channel,
               committed.preparation_id
        FROM catalog_publication_commits AS committed
        LEFT JOIN catalog_source_revision_descriptors AS descriptor
          ON descriptor.source_revision = committed.source_revision
        WHERE committed.generation = %s
        LIMIT 2
        """,
        (generation - 1,),
        detail="retained current source predecessor",
        optional=True,
    )
    if predecessor_row is not None:
        if len(predecessor_row) != 3 or any(value is None for value in predecessor_row):
            raise CatalogSemanticValidationError(
                "retained current source predecessor is malformed"
            )
        predecessor_receipt = _as_bytes(
            predecessor_row[0],
            field="retained current source predecessor receipt_id",
        )
        if len(predecessor_receipt) != 16:
            raise CatalogSemanticValidationError(
                "retained current source predecessor receipt_id is not 16 bytes"
            )
        if (
            _as_bytes(
                predecessor_row[1],
                field="retained current source predecessor channel",
            )
            != channel
        ):
            raise CatalogSemanticValidationError(
                "retained current source predecessor belongs to another channel"
            )
        predecessor_preparation = _as_bytes(
            predecessor_row[2],
            field="retained current source predecessor preparation_id",
        )
        if len(predecessor_preparation) != 16:
            raise CatalogSemanticValidationError(
                "retained current source predecessor preparation_id is not 16 bytes"
            )
        _require_open_pcom_source_build_base_release(
            connector,
            receipt_id=predecessor_receipt,
            preparation_id=predecessor_preparation,
            build_id=build_id,
        )
        return

    row = _one(
        connector,
        """
        SELECT committed.receipt_id, committed.source_revision,
               committed.generation, receipt.state, receipt.finalized_at,
               provenance.analysis_id, provenance_analysis.build_id,
               provenance_analysis.state, descriptor.channel
        FROM catalog_publication_commit_head_receipts AS head
        JOIN catalog_publication_commits AS committed
          ON committed.receipt_id = head.receipt_id
        JOIN catalog_publication_receipts AS receipt
          ON receipt.receipt_id = committed.receipt_id
        JOIN catalog_source_revision_provenance AS provenance
          ON provenance.source_revision = committed.source_revision
        JOIN catalog_analysis_runs AS provenance_analysis
          ON provenance_analysis.analysis_id = provenance.analysis_id
        JOIN catalog_source_revision_descriptors AS descriptor
          ON descriptor.source_revision = committed.source_revision
        WHERE head.channel = %s
        LIMIT 2
        """,
        (channel,),
        detail="compacted current source handoff",
    )
    assert row is not None
    receipt_id = _as_bytes(row[0], field="compacted source receipt_id")
    if len(receipt_id) != 16:
        raise CatalogSemanticValidationError(
            "compacted current source receipt_id is not 16 bytes"
        )
    _as_int(row[4], field="compacted source finalized_at")
    provenance_analysis = _as_bytes(
        row[5],
        field="compacted source provenance analysis_id",
    )
    provenance_build = _as_bytes(
        row[6],
        field="compacted source provenance build_id",
    )
    descriptor_channel = _as_bytes(
        row[8],
        field="compacted source descriptor channel",
    )
    if (
        _as_int(row[1], field="compacted source revision", positive=True) != revision
        or _as_int(row[2], field="compacted source generation", positive=True)
        != generation
        or row[3] != "PUBLISHED"
        or provenance_analysis != analysis_id
        or provenance_build != build_id
        or row[7] != "COMPLETE"
        or descriptor_channel != channel
    ):
        raise CatalogSemanticValidationError(
            "compacted current source handoff differs from its exact "
            "build, analysis, or provenance authority"
        )


def _require_open_pcom_source_build_base_release(
    connector: SQLConnector,
    *,
    receipt_id: bytes,
    preparation_id: bytes,
    build_id: bytes,
) -> None:
    """Accept one missing build pin only under its exact durable cleanup proof.

    ``PCOM_RELEASE_BUILD_BASE`` intentionally consumes the successor build's
    one-shot predecessor pin before the predecessor commit is retired.  A
    process may stop between those transactions.  The gap is therefore valid
    only while the fixed cleanup state proves the exact frozen
    receipt/preparation authority and its current phase proves that this build
    row has already been covered.
    """

    authorities = _validated_open_pcom_transitions(connector)
    phase, cursor, phase_order = _require_open_pcom_transition(
        connector,
        receipt_id=receipt_id,
        preparation_id=preparation_id,
        transitions=authorities,
    )
    if not 1 <= phase_order <= 9:
        raise CatalogSemanticValidationError(
            "current source build baseline gap is outside the predecessor-retirement "
            "phase window"
        )
    if phase_order > 1:
        return
    if phase != "PCOM_RELEASE_BUILD_BASE":
        raise CatalogSemanticValidationError(
            "current source build baseline gap has a forged cleanup phase"
        )
    if (
        len(cursor) != 42
        or cursor[:7] != b"\x01\x00\x00\x02b\x00\x10"
        or cursor[23:26] != b"b\x00\x10"
    ):
        raise CatalogSemanticValidationError(
            "current source build baseline gap lacks its exact release cursor"
        )
    cursor_receipt = cursor[7:23]
    cursor_build = cursor[26:42]
    if cursor_receipt not in authorities or (cursor_receipt, cursor_build) < (
        receipt_id,
        build_id,
    ):
        raise CatalogSemanticValidationError(
            "current source build baseline gap is not covered by its release cursor"
        )


@dataclass(frozen=True, slots=True)
class _OpenPcomTransition:
    preparation_id: bytes
    phase: str
    cursor: bytes
    phase_order: int


def _validated_open_pcom_transitions(
    connector: SQLConnector,
) -> dict[bytes, _OpenPcomTransition]:
    """Validate once and load the bounded frozen OPEN PCOM capability set."""

    from . import operational_refinement

    try:
        operational_refinement.check_cleanup_reachability_v1(connector)
        operational_refinement.check_event_integrity_contract_v1(connector)
    except (
        operational_refinement.OperationalSemanticRegistryError,
        operational_refinement.OperationalSemanticValidationError,
    ) as error:
        raise CatalogSemanticValidationError(
            "catalog retirement gap lacks valid OPEN publication-commit cleanup "
            "authority"
        ) from error

    rows = connector.fetch_all(
        """
        SELECT root.frozen_root_key, checkpoint.phase, checkpoint.cursor_bytes,
               phase.phase_order
        FROM operational_cleanup_jobs AS job
        JOIN operational_cleanup_sweep_targets AS sweep
          ON sweep.target_key = job.target_key
        JOIN operational_cleanup_checkpoints AS checkpoint
          ON checkpoint.cleanup_id = job.cleanup_id
        JOIN operational_cleanup_phases AS phase
          ON phase.target_kind = sweep.target_kind
         AND phase.phase = checkpoint.phase
        JOIN operational_cleanup_cycle_roots AS root
          ON root.cleanup_id = job.cleanup_id
        WHERE job.state = 'OPEN'
          AND sweep.target_kind = 'PUBLICATION_COMMIT'
          AND checkpoint.state = 'OPEN'
        ORDER BY root.frozen_root_key
        LIMIT 257
        """
    )
    if len(rows) > 256:
        raise CatalogSemanticValidationError(
            "OPEN publication-commit cleanup roots exceed the hard cap"
        )
    transitions: dict[bytes, _OpenPcomTransition] = {}
    for row in rows:
        frame = _as_bytes(row[0], field="OPEN PCOM frozen root")
        if (
            len(frame) != 40
            or frame[:5] != b"\x01\x02b\x00\x10"
            or frame[21:24] != b"b\x00\x10"
        ):
            raise CatalogSemanticValidationError(
                "OPEN PCOM frozen root frame is malformed"
            )
        receipt_id = frame[5:21]
        phase = row[1]
        if not isinstance(phase, str):
            raise CatalogSemanticValidationError(
                "OPEN publication-commit cleanup phase is malformed"
            )
        transition = _OpenPcomTransition(
            preparation_id=frame[24:40],
            phase=phase,
            cursor=_as_bytes(row[2], field="OPEN publication-commit cleanup cursor"),
            phase_order=_as_int(
                row[3],
                field="OPEN publication-commit cleanup phase order",
                positive=True,
            ),
        )
        if receipt_id in transitions:
            raise CatalogSemanticValidationError(
                "OPEN PCOM frozen receipt authority is duplicated"
            )
        transitions[receipt_id] = transition
    return transitions


def _require_open_pcom_transition(
    connector: SQLConnector,
    *,
    receipt_id: bytes,
    preparation_id: bytes,
    transitions: Mapping[bytes, _OpenPcomTransition] | None = None,
) -> tuple[str, bytes, int]:
    """Return an exact, fully validated OPEN PCOM transition capability."""

    authority = (
        _validated_open_pcom_transitions(connector)
        if transitions is None
        else transitions
    ).get(receipt_id)
    if authority is None or authority.preparation_id != preparation_id:
        raise CatalogSemanticValidationError(
            "catalog retirement gap is outside the exact OPEN publication-commit "
            "frozen authority"
        )
    return authority.phase, authority.cursor, authority.phase_order


def _pcom_static_cursor_covers_pair(
    cursor: bytes,
    *,
    receipt_id: bytes,
    frozen_receipts: Container[bytes],
) -> bool:
    """Prove a canonical two-UUID cursor covered ``receipt_id``.

    Static cleanup cursors are keyset positions, not per-root receipts.  A
    cursor on a later frozen receipt therefore covers every earlier frozen
    receipt as well.  Requiring its leading root to remain in the sealed set
    prevents a forged out-of-range byte string from authorizing absence.
    """

    if (
        len(cursor) != 42
        or cursor[:7] != b"\x01\x00\x00\x02b\x00\x10"
        or cursor[23:26] != b"b\x00\x10"
    ):
        return False
    cursor_receipt = cursor[7:23]
    cursor_primary = cursor[26:42]
    return (
        cursor_receipt == cursor_primary
        and cursor_receipt in frozen_receipts
        and (cursor_receipt, cursor_primary) >= (receipt_id, receipt_id)
    )


def _pcom_finalization_batch_cursor_covers(
    cursor: bytes,
    *,
    receipt_id: bytes,
    start_generation: int,
    frozen_receipts: Container[bytes],
) -> bool:
    """Recognize a phase-8 cursor that covered the exact terminal receipt row."""

    if (
        len(cursor) != 51
        or cursor[:7] != b"\x01\x00\x00\x03b\x00\x10"
        or cursor[23:26] != b"b\x00\x10"
        or cursor[42:43] != b"i"
    ):
        return False
    cursor_receipt = cursor[7:23]
    cursor_primary = cursor[26:42]
    cursor_generation = int.from_bytes(cursor[43:51], "big")
    return (
        cursor_receipt == cursor_primary
        and cursor_receipt in frozen_receipts
        and (cursor_receipt, cursor_primary, cursor_generation)
        >= (receipt_id, receipt_id, start_generation)
    )


def _decode_pcom_compound_cursor(cursor: bytes) -> tuple[bytes, bytes, bytes]:
    """Decode the three-UUID phase-nine static keyset position."""

    if (
        len(cursor) != 61
        or cursor[:7] != b"\x01\x00\x00\x03b\x00\x10"
        or cursor[23:26] != b"b\x00\x10"
        or cursor[42:45] != b"b\x00\x10"
    ):
        raise CatalogSemanticValidationError("OPEN PCOM compound cursor is malformed")
    return cursor[7:23], cursor[26:42], cursor[45:61]


def _require_open_pcom_finalization_retirement(
    connector: SQLConnector,
    *,
    receipt_id: bytes,
    preparation_id: bytes,
    checkpoint_generation: int,
    latest_present: bool,
    transitions: Mapping[bytes, _OpenPcomTransition] | None = None,
) -> None:
    """Prove intentionally absent publication-finalization authority."""

    authorities = (
        _validated_open_pcom_transitions(connector)
        if transitions is None
        else transitions
    )
    phase, cursor, phase_order = _require_open_pcom_transition(
        connector,
        receipt_id=receipt_id,
        preparation_id=preparation_id,
        transitions=authorities,
    )
    marker_covered = phase_order > 7 or (
        phase_order == 7
        and phase == "PCOM_FINALIZATION_MARKER"
        and _pcom_static_cursor_covers_pair(
            cursor,
            receipt_id=receipt_id,
            frozen_receipts=authorities,
        )
    )
    if not marker_covered:
        raise CatalogSemanticValidationError(
            "retained publication lost its finalization marker before its exact "
            "cleanup cursor"
        )
    if latest_present:
        return
    batch_covered = phase_order > 8 or (
        phase_order == 8
        and phase == "PCOM_FINALIZATION_BATCH"
        and _pcom_finalization_batch_cursor_covers(
            cursor,
            receipt_id=receipt_id,
            start_generation=checkpoint_generation - 1,
            frozen_receipts=authorities,
        )
    )
    if not batch_covered:
        raise CatalogSemanticValidationError(
            "retained publication lost its finalization batch before its exact "
            "cleanup cursor"
        )


def _require_open_pcom_compound_retirement(
    connector: SQLConnector,
    *,
    transitions: Mapping[bytes, _OpenPcomTransition] | None = None,
) -> tuple[frozenset[bytes], frozenset[bytes]]:
    """Prove and return the at-most-256 receipts deleted by OPEN phase 9."""

    authorities = (
        _validated_open_pcom_transitions(connector)
        if transitions is None
        else dict(transitions)
    )
    if not authorities:
        return frozenset(), frozenset()
    first = next(iter(authorities.values()))
    phase = first.phase
    phase_order = first.phase_order
    cursor = first.cursor
    if any(
        (item.phase, item.phase_order, item.cursor) != (phase, phase_order, cursor)
        for item in authorities.values()
    ):
        raise CatalogSemanticValidationError(
            "OPEN PCOM root set disagrees on its current checkpoint"
        )
    receipts = frozenset(authorities)
    retired: frozenset[bytes]
    expected_orphan_anchors: frozenset[bytes]
    if phase_order == 9:
        if phase != "PCOM_COMMIT_EFFECT_ROOT":
            raise CatalogSemanticValidationError(
                "OPEN PCOM compound phase/order disagrees"
            )
        if not cursor:
            return frozenset(), frozenset()
        cursor_receipt, cursor_primary, cursor_preparation = (
            _decode_pcom_compound_cursor(cursor)
        )
        cursor_authority = authorities.get(cursor_receipt)
        if (
            cursor_receipt != cursor_primary
            or cursor_authority is None
            or cursor_authority.preparation_id != cursor_preparation
        ):
            raise CatalogSemanticValidationError(
                "OPEN PCOM compound cursor is outside its frozen root set"
            )
        retired = frozenset(
            receipt_id for receipt_id in receipts if receipt_id <= cursor_receipt
        )
        expected_orphan_anchors = retired
    elif phase_order == 10:
        if phase != "PCOM_FINALIZATION_CHECKPOINT":
            raise CatalogSemanticValidationError(
                "OPEN PCOM checkpoint phase/order disagrees"
            )
        retired = receipts
        expected_orphan_anchors = receipts
    elif phase_order == 11:
        if phase != "PCOM_ANCHOR":
            raise CatalogSemanticValidationError(
                "OPEN PCOM anchor phase/order disagrees"
            )
        retired = receipts
        expected_orphan_anchors = receipts
        if cursor:
            if not _pcom_static_cursor_covers_pair(
                cursor,
                receipt_id=cursor[7:23] if len(cursor) == 42 else b"",
                frozen_receipts=receipts,
            ):
                raise CatalogSemanticValidationError(
                    "OPEN PCOM anchor cursor is malformed"
                )
            cursor_receipt = cursor[7:23]
            if cursor_receipt not in retired:
                raise CatalogSemanticValidationError(
                    "OPEN PCOM anchor cursor is outside its frozen roots"
                )
            expected_orphan_anchors = frozenset(
                receipt_id for receipt_id in retired if receipt_id > cursor_receipt
            )
    else:
        raise CatalogSemanticValidationError(
            "OPEN PCOM compound retirement is outside phases 9 through 11"
        )
    return retired, expected_orphan_anchors


def _has_open_pcom_compound_transition(connector: SQLConnector) -> bool:
    """Return a bounded hint; the full authority is validated before use."""

    rows = connector.fetch_all(
        """
        SELECT 1
        FROM operational_cleanup_jobs AS job
        JOIN operational_cleanup_sweep_targets AS sweep
          ON sweep.target_key = job.target_key
        JOIN operational_cleanup_checkpoints AS checkpoint
          ON checkpoint.cleanup_id = job.cleanup_id
        JOIN operational_cleanup_phases AS phase
          ON phase.target_kind = sweep.target_kind
         AND phase.phase = checkpoint.phase
        WHERE job.state = 'OPEN'
          AND sweep.target_kind = 'PUBLICATION_COMMIT'
          AND checkpoint.state = 'OPEN'
          AND phase.phase_order >= 9
        LIMIT 2
        """
    )
    if len(rows) > 1 or (rows and rows[0] != (1,)):
        raise CatalogSemanticValidationError(
            "OPEN PCOM compound transition hint is ambiguous"
        )
    return bool(rows)


def _active_source_contexts(connector: SQLConnector) -> tuple[_SourceContext, ...]:
    rows = connector.fetch_all("""
        SELECT registry.channel, head.source_revision, mapping.generation,
               advanced.advanced_at
        FROM catalog_channel_registry AS registry
        LEFT JOIN catalog_source_head_revisions AS head
          ON head.channel = registry.channel
        LEFT JOIN catalog_source_revision_generations AS mapping
          ON mapping.source_revision = head.source_revision
        LEFT JOIN catalog_source_head_advanced_ats AS advanced
          ON advanced.channel = registry.channel
        ORDER BY registry.channel
        LIMIT 2
        """)
    if len(rows) != 1:
        raise CatalogSemanticValidationError("source_head exceeds the channel registry")
    if all(value is None for value in rows[0][1:]):
        return ()
    if any(value is None for value in rows[0][1:]):
        raise CatalogSemanticValidationError("source_head projection is incomplete")
    contexts: list[_SourceContext] = []
    for head_row in rows:
        channel = _as_bytes(head_row[0], field="source_head.channel")
        revision = _as_int(
            head_row[1], field="source_head.source_revision", positive=True
        )
        generation = _as_int(head_row[2], field="source_head.generation", positive=True)
        _as_int(head_row[3], field="source_head.advanced_at")
        if channel != b"default":
            raise CatalogSemanticValidationError("source_head uses an unknown channel")

        revision_row = _one(
            connector,
            """
            SELECT channel, snapshot_manifest_sha256
            FROM catalog_source_revisions
            WHERE source_revision = %s
            LIMIT 2
            """,
            (revision,),
            detail="active source revision",
        )
        assert revision_row is not None
        if _as_bytes(revision_row[0], field="source_revision.channel") != channel:
            raise CatalogSemanticValidationError(
                "source_head and source_revision channels differ"
            )
        snapshot_digest = _as_bytes(
            revision_row[1], field="source_revision.snapshot_manifest_sha256"
        )
        if len(snapshot_digest) != 32:
            raise CatalogSemanticValidationError(
                "source snapshot manifest identity is not 32 bytes"
            )
        manifest_row = _one(
            connector,
            """
            SELECT gallery_count, file_count, byte_count
            FROM catalog_source_snapshot_manifest_identity
            WHERE snapshot_manifest_sha256 = %s
            LIMIT 2
            """,
            (snapshot_digest,),
            detail="active source snapshot descriptor",
        )
        assert manifest_row is not None
        for index, name in enumerate(("gallery_count", "file_count", "byte_count")):
            _as_int(manifest_row[index], field=f"source snapshot {name}")
        _require_canonical_domain(
            connector,
            snapshot_digest,
            b"source_snapshot_manifest_v1",
            detail="active source snapshot manifest",
        )

        provenance_row = _one(
            connector,
            """
            SELECT analysis_id
            FROM catalog_source_revision_provenance
            WHERE source_revision = %s
            LIMIT 2
            """,
            (revision,),
            detail="active source revision provenance",
        )
        assert provenance_row is not None
        analysis_id = _as_bytes(provenance_row[0], field="provenance.analysis_id")
        analysis_row = _one(
            connector,
            """
            SELECT build_id, policy_id, input_manifest_sha256, state
            FROM catalog_analysis_runs
            WHERE analysis_id = %s
            LIMIT 2
            """,
            (analysis_id,),
            detail="active source analysis",
        )
        assert analysis_row is not None
        build_id = _as_bytes(analysis_row[0], field="analysis_run.build_id")
        policy_id = _as_int(
            analysis_row[1], field="analysis_run.policy_id", positive=True
        )
        if len(_as_bytes(analysis_row[2], field="analysis input manifest")) != 32:
            raise CatalogSemanticValidationError(
                "active analysis input manifest is not 32 bytes"
            )
        if analysis_row[3] != "COMPLETE":
            raise CatalogSemanticValidationError(
                "active source analysis is not COMPLETE"
            )
        binding_row = _one(
            connector,
            """
            SELECT snapshot_manifest_sha256
            FROM catalog_analysis_snapshot_manifest
            WHERE analysis_id = %s
            LIMIT 2
            """,
            (analysis_id,),
            detail="active analysis snapshot output",
        )
        assert binding_row is not None
        if (
            _as_bytes(
                binding_row[0],
                field="analysis_snapshot_manifest.snapshot_manifest_sha256",
            )
            != snapshot_digest
        ):
            raise CatalogSemanticValidationError(
                "active source revision manifest differs from its analysis output"
            )

        build_row = _one(
            connector,
            """
            SELECT scope_key, state
            FROM catalog_source_builds
            WHERE build_id = %s
            LIMIT 2
            """,
            (build_id,),
            detail="active source build",
        )
        assert build_row is not None
        scope_key = _as_bytes(build_row[0], field="source_build.scope_key")
        if build_row[1] != "SEALED":
            raise CatalogSemanticValidationError("active source build is not SEALED")
        build_channel_row = _one(
            connector,
            """
            SELECT channel
            FROM catalog_source_build_channel
            WHERE build_id = %s
            LIMIT 2
            """,
            (build_id,),
            detail="active source build channel",
        )
        assert build_channel_row is not None
        if (
            _as_bytes(build_channel_row[0], field="source_build_channel.channel")
            != channel
        ):
            raise CatalogSemanticValidationError(
                "active source build is pinned to a different channel"
            )
        _one(
            connector,
            """
            SELECT core.build_id
            FROM catalog_build_manifest_core AS core
            JOIN catalog_source_build_discoveries AS discovery
              ON discovery.build_id = core.build_id
            JOIN catalog_source_build_sealed_ats AS sealed
              ON sealed.build_id = core.build_id
            WHERE core.build_id = %s
            LIMIT 2
            """,
            (build_id,),
            detail="active sealed build manifest",
        )

        scope_row = _one(
            connector,
            """
            SELECT source_provider, source_root_sha256, identity_policy_version
            FROM catalog_source_scopes
            WHERE scope_key = %s
            LIMIT 2
            """,
            (scope_key,),
            detail="active source scope",
        )
        assert scope_row is not None
        provider = _as_bytes(scope_row[0], field="source_scope.source_provider")
        source_root = _as_bytes(scope_row[1], field="source_scope.source_root_sha256")
        identity_policy_version = _as_int(
            scope_row[2], field="source_scope.identity_policy_version", positive=True
        )
        if provider != b"filesystem":
            raise CatalogSemanticValidationError(
                "active source scope uses an unknown source provider"
            )
        if (
            identity.source_scope_key(
                provider.decode("ascii"), source_root, identity_policy_version
            )
            != scope_key
        ):
            raise CatalogSemanticValidationError(
                "active source scope_key does not match its exact natural tuple"
            )
        _require_canonical_domain(
            connector,
            source_root,
            b"source_root_v1",
            detail="active source root",
        )

        base_row = _one(
            connector,
            """
            SELECT base.base_receipt_id, commit_row.revision,
                   commit_row.source_revision, commit_row.generation,
                   source_revision.channel
            FROM catalog_source_build_base_publication_commits AS base
            JOIN catalog_publication_commits AS commit_row
              ON commit_row.receipt_id = base.base_receipt_id
            JOIN catalog_source_revisions AS source_revision
              ON source_revision.source_revision = commit_row.source_revision
            WHERE base.build_id = %s
            LIMIT 2
            """,
            (build_id,),
            detail="active source build baseline",
            optional=True,
        )
        if base_row is None:
            malformed_base = _one(
                connector,
                "SELECT base_receipt_id "
                "FROM catalog_source_build_base_publication_commits "
                "WHERE build_id = %s LIMIT 2",
                (build_id,),
                detail="active source build raw baseline",
                optional=True,
            )
            if malformed_base is not None:
                raise CatalogSemanticValidationError(
                    "active source build baseline lacks its sealed predecessor"
                )
            if generation != 1:
                _require_compacted_current_source_handoff(
                    connector,
                    channel=channel,
                    revision=revision,
                    generation=generation,
                    analysis_id=analysis_id,
                    build_id=build_id,
                )
        else:
            base_commit = _base_commit_tuple(
                base_row,
                detail="active source build baseline",
            )
            assert base_commit is not None
            (
                _base_receipt_id,
                _base_catalog_revision,
                base_source_revision,
                base_generation,
                base_channel,
            ) = base_commit
            if (
                base_channel != channel
                or generation != base_generation + 1
                or revision <= base_source_revision
            ):
                raise CatalogSemanticValidationError(
                    "source_head does not advance its exact build baseline"
                )

        contexts.append(
            _SourceContext(
                channel,
                revision,
                generation,
                snapshot_digest,
                analysis_id,
                policy_id,
                build_id,
                scope_key,
            )
        )
    return tuple(contexts)


def _component_seals(
    connector: SQLConnector, analysis_id: bytes, *, detail: str
) -> tuple[tuple[bytes, int, int], ...]:
    rows = connector.fetch_all(
        """
        SELECT state_component, row_count, sealed_at
        FROM catalog_analysis_state_component_seals
        WHERE analysis_id = %s
        ORDER BY state_component
        LIMIT 6
        """,
        (analysis_id,),
    )
    result = tuple(
        (
            _as_bytes(row[0], field=f"{detail} state_component"),
            _as_int(row[1], field=f"{detail} row_count"),
            _as_int(row[2], field=f"{detail} sealed_at"),
        )
        for row in rows
    )
    expected = tuple(
        sorted(value.encode("ascii") for value in _EXPECTED_ANALYSIS_COMPONENTS)
    )
    if tuple(row[0] for row in result) != expected:
        raise CatalogSemanticValidationError(
            f"{detail} does not have the exact five immutable component seals"
        )
    return result


def _analysis_ancestry(
    connector: SQLConnector,
    analysis_id: bytes,
    *,
    policy_id: int,
    detail: str,
) -> tuple[bytes, ...]:
    rows = connector.fetch_all(
        f"""
        SELECT ancestry.ancestor_depth,
               ancestry.ancestor_analysis_id,
               ancestor.policy_id,
               ancestor.state
        FROM catalog_analysis_state_ancestry AS ancestry
        JOIN catalog_analysis_runs AS ancestor
          ON ancestor.analysis_id = ancestry.ancestor_analysis_id
        WHERE ancestry.analysis_id = %s
        ORDER BY ancestry.ancestor_depth
        LIMIT {_ANALYSIS_ANCESTRY_RESULT_LIMIT}
        """,
        (analysis_id,),
    )
    ancestors: list[bytes] = []
    for expected_depth, row in enumerate(rows):
        if _as_int(row[0], field=f"{detail} ancestor_depth") != expected_depth:
            raise CatalogSemanticValidationError(
                f"{detail} ancestry depth is not contiguous"
            )
        ancestor_id = _as_bytes(row[1], field=f"{detail} ancestor_analysis_id")
        if len(ancestor_id) != 16:
            raise CatalogSemanticValidationError(
                f"{detail} ancestor_analysis_id is not 16 bytes"
            )
        ancestors.append(ancestor_id)
        if (
            _as_int(row[2], field=f"{detail} ancestor policy", positive=True)
            != policy_id
        ):
            raise CatalogSemanticValidationError(
                f"{detail} ancestry crosses policy identities"
            )
        if row[3] != "COMPLETE":
            raise CatalogSemanticValidationError(
                f"{detail} ancestry reaches a non-COMPLETE run"
            )
    return tuple(ancestors)


def _validate_impacted_key_families(
    connector: SQLConnector,
    analysis_id: bytes,
) -> None:
    """Audit sealed impacted keys without recomputing their derivation.

    Each anti-join is rooted at one atomic impacted-key row and constrained by
    ``analysis_id``.  The minimum-witness query deliberately probes provenance
    in ``(analysis_id, key, gallery_id)`` order so both supported backends can
    use the manifest-required key-first lookup index.
    """

    for (
        family,
        key,
        impacted_table,
        provenance_table,
    ) in _IMPACTED_KEY_FAMILIES:
        incomplete_key = connector.fetch_all(
            f"""
            SELECT impacted.{key}
            FROM {impacted_table} AS impacted
            WHERE impacted.analysis_id = %s
              AND (
                NOT EXISTS (
                  SELECT 1
                  FROM {provenance_table} AS provenance
                  WHERE provenance.analysis_id = impacted.analysis_id
                    AND provenance.{key} = impacted.{key}
                )
                OR NOT EXISTS (
                  SELECT 1 FROM {provenance_table} AS witness
                  WHERE witness.analysis_id = impacted.analysis_id
                    AND witness.{key} = impacted.{key}
                    AND witness.gallery_id = impacted.witness_gallery_id
                )
              )
            LIMIT 1
            """,
            (analysis_id,),
        )
        if incomplete_key:
            raise CatalogSemanticValidationError(
                f"impacted-{family} row lacks its witness provenance"
            )

        orphan_provenance = connector.fetch_all(
            f"""
            SELECT provenance.{key}
            FROM {provenance_table} AS provenance
            WHERE provenance.analysis_id = %s
              AND NOT EXISTS (
                SELECT 1
                FROM {impacted_table} AS impacted
                WHERE impacted.analysis_id = provenance.analysis_id
                  AND impacted.{key} = provenance.{key}
              )
            LIMIT 1
            """,
            (analysis_id,),
        )
        if orphan_provenance:
            raise CatalogSemanticValidationError(
                f"impacted-{family} provenance has no atomic key row"
            )

        nonminimum_witness = connector.fetch_all(
            f"""
            SELECT impacted.{key}
            FROM {impacted_table} AS impacted
            WHERE impacted.analysis_id = %s
              AND NOT EXISTS (
                SELECT 1
                FROM {provenance_table} AS candidate
                WHERE candidate.analysis_id = impacted.analysis_id
                  AND candidate.{key} = impacted.{key}
                  AND candidate.gallery_id = impacted.witness_gallery_id
                  AND NOT EXISTS (
                    SELECT 1
                    FROM {provenance_table} AS smaller
                    WHERE smaller.analysis_id = candidate.analysis_id
                      AND smaller.{key} = candidate.{key}
                      AND smaller.gallery_id < candidate.gallery_id
                  )
              )
            LIMIT 1
            """,
            (analysis_id,),
        )
        if nonminimum_witness:
            raise CatalogSemanticValidationError(
                f"sealed impacted-{family} witness is not the minimum provenance gallery"
            )

    incomplete_gid_key_storage = connector.fetch_all(
        """
        SELECT stored.gid
        FROM catalog_analysis_impacted_gid_storage AS stored
        WHERE stored.analysis_id = %s
          AND NOT EXISTS (
            SELECT 1
            FROM catalog_a_impacted_gid_provenance AS provenance
            WHERE provenance.analysis_id = stored.analysis_id
              AND provenance.gid = stored.gid
          )
        LIMIT 1
        """,
        (analysis_id,),
    )
    if incomplete_gid_key_storage:
        raise CatalogSemanticValidationError(
            "impacted-gid key storage lacks complete derived provenance"
        )

    incomplete_gid_provenance_storage = connector.fetch_all(
        """
        SELECT stored.gallery_id
        FROM catalog_a_impacted_gid_provenance_storage AS stored
        WHERE stored.analysis_id = %s
          AND NOT EXISTS (
            SELECT 1
            FROM catalog_a_impacted_gid_provenance AS provenance
            JOIN catalog_analysis_impacted_gid_storage AS impacted
              ON impacted.analysis_id = provenance.analysis_id
             AND impacted.gid = provenance.gid
            WHERE provenance.analysis_id = stored.analysis_id
              AND provenance.gallery_id = stored.gallery_id
          )
        LIMIT 1
        """,
        (analysis_id,),
    )
    if incomplete_gid_provenance_storage:
        raise CatalogSemanticValidationError(
            "impacted-gid provenance storage lacks its identity chain or atomic key"
        )


def _validate_analysis_seal(
    connector: SQLConnector,
    analysis_id: bytes,
    *,
    expected_policy_id: int | None = None,
) -> None:
    if len(analysis_id) != 16:
        raise CatalogSemanticValidationError("sealed analysis id is not 16 bytes")
    analysis_row = _one(
        connector,
        """
        SELECT policy_id, state
        FROM catalog_analysis_runs
        WHERE analysis_id = %s
        LIMIT 2
        """,
        (analysis_id,),
        detail="sealed analysis run",
    )
    assert analysis_row is not None
    policy_id = _as_int(analysis_row[0], field="sealed analysis policy", positive=True)
    if analysis_row[1] != "COMPLETE":
        raise CatalogSemanticValidationError("sealed analysis run is not COMPLETE")
    if expected_policy_id is not None and policy_id != expected_policy_id:
        raise CatalogSemanticValidationError(
            "active analysis policy changed across its seal"
        )

    anchor_row = _one(
        connector,
        """
        SELECT anchor_analysis_id, overlay_depth
        FROM catalog_analysis_state_anchors
        WHERE analysis_id = %s
        LIMIT 2
        """,
        (analysis_id,),
        detail="sealed analysis anchor",
    )
    assert anchor_row is not None
    anchor_analysis_id = _as_bytes(anchor_row[0], field="analysis anchor id")
    overlay_depth = _as_int(anchor_row[1], field="analysis overlay depth")
    if overlay_depth > _MAX_ANALYSIS_OVERLAY_DEPTH:
        raise CatalogSemanticValidationError("analysis overlay depth exceeds 16")

    ancestors = _analysis_ancestry(
        connector,
        analysis_id,
        policy_id=policy_id,
        detail="active analysis",
    )
    if len(ancestors) != overlay_depth + 1:
        raise CatalogSemanticValidationError(
            "analysis ancestry does not exactly cover overlay_depth"
        )
    if (
        not ancestors
        or ancestors[0] != analysis_id
        or ancestors[-1] != anchor_analysis_id
    ):
        raise CatalogSemanticValidationError(
            "analysis ancestry endpoints disagree with its anchor"
        )
    if len(set(ancestors)) != len(ancestors):
        raise CatalogSemanticValidationError("analysis ancestry contains a cycle")

    compaction_baseline: bytes | None = None
    for ancestor_offset, ancestor_id in enumerate(ancestors):
        suffix = ancestors[ancestor_offset:]
        suffix_depth = len(suffix) - 1
        if ancestor_offset == 0:
            current_anchor_id = anchor_analysis_id
            current_depth = overlay_depth
            current_ancestry = ancestors
        else:
            suffix_anchor_row = _one(
                connector,
                """
                SELECT anchor_analysis_id, overlay_depth
                FROM catalog_analysis_state_anchors
                WHERE analysis_id = %s
                LIMIT 2
                """,
                (ancestor_id,),
                detail="ancestor analysis anchor",
            )
            assert suffix_anchor_row is not None
            current_anchor_id = _as_bytes(
                suffix_anchor_row[0], field="ancestor anchor id"
            )
            current_depth = _as_int(
                suffix_anchor_row[1], field="ancestor overlay depth"
            )
            current_ancestry = _analysis_ancestry(
                connector,
                ancestor_id,
                policy_id=policy_id,
                detail="ancestor analysis",
            )
        if (
            current_anchor_id != anchor_analysis_id
            or current_depth != suffix_depth
            or current_ancestry != suffix
        ):
            raise CatalogSemanticValidationError(
                "ancestor analysis does not materialize the complete parent ancestry suffix"
            )
        _component_seals(
            connector,
            ancestor_id,
            detail=f"analysis ancestor depth {ancestor_offset}",
        )

        baseline_row = _one(
            connector,
            """
            SELECT base_analysis_id
            FROM catalog_analysis_baselines
            WHERE analysis_id = %s
            LIMIT 2
            """,
            (ancestor_id,),
            detail=f"analysis ancestor depth {ancestor_offset} baseline",
            optional=True,
        )
        if suffix_depth > 0:
            if baseline_row is None:
                raise CatalogSemanticValidationError(
                    "positive-depth ancestor analysis lacks a baseline"
                )
            base_analysis_id = _as_bytes(
                baseline_row[0], field="ancestor base_analysis_id"
            )
            if base_analysis_id != suffix[1]:
                raise CatalogSemanticValidationError(
                    "ancestor analysis baseline is not its immediate sealed parent"
                )
        else:
            if current_anchor_id != ancestor_id:
                raise CatalogSemanticValidationError(
                    "depth-zero analysis must be its own anchor"
                )
            if baseline_row is not None:
                compaction_baseline = _as_bytes(
                    baseline_row[0], field="compaction base_analysis_id"
                )

    if compaction_baseline is None:
        return
    if compaction_baseline in ancestors:
        raise CatalogSemanticValidationError(
            "depth-zero compaction baseline cycles into its active ancestry"
        )
    compacted_row = _one(
        connector,
        """
        SELECT run.policy_id, run.state, anchor.overlay_depth
        FROM catalog_analysis_runs AS run
        JOIN catalog_analysis_state_anchors AS anchor
          ON anchor.analysis_id = run.analysis_id
        WHERE run.analysis_id = %s
        LIMIT 2
        """,
        (compaction_baseline,),
        detail="depth-zero compaction baseline",
    )
    assert compacted_row is not None
    compacted_policy_id = _as_int(
        compacted_row[0], field="compaction baseline policy", positive=True
    )
    if compacted_row[1] != "COMPLETE":
        raise CatalogSemanticValidationError(
            "depth-zero compaction baseline is not COMPLETE"
        )
    compacted_depth = _as_int(
        compacted_row[2], field="compaction baseline overlay depth"
    )
    if (
        compacted_policy_id == policy_id
        and compacted_depth != _MAX_ANALYSIS_OVERLAY_DEPTH
    ):
        raise CatalogSemanticValidationError(
            "same-policy depth-zero compaction did not follow a depth-16 baseline"
        )
    _component_seals(
        connector,
        compaction_baseline,
        detail="depth-zero compaction baseline",
    )


def _base_commit_tuple(
    row: tuple[Any, ...] | None,
    *,
    detail: str,
) -> tuple[bytes, int, int, int, bytes] | None:
    if row is None:
        return None
    receipt_id = _as_bytes(row[0], field=f"{detail} receipt_id")
    if len(receipt_id) != 16:
        raise CatalogSemanticValidationError(f"{detail} receipt_id is not 16 bytes")
    return (
        receipt_id,
        _as_int(row[1], field=f"{detail} catalog revision", positive=True),
        _as_int(row[2], field=f"{detail} source revision", positive=True),
        _as_int(row[3], field=f"{detail} generation", positive=True),
        _as_bytes(row[4], field=f"{detail} channel"),
    )


def _validate_publication_candidate_source(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
    analysis_id: bytes,
    source_revision: int,
    channel: bytes,
) -> None:
    source_revision_row = _one(
        connector,
        """
        SELECT revision.channel, provenance.analysis_id,
               revision.snapshot_manifest_sha256
        FROM catalog_source_revisions AS revision
        JOIN catalog_source_revision_provenance AS provenance
          ON provenance.source_revision = revision.source_revision
        WHERE revision.source_revision = %s
        LIMIT 2
        """,
        (source_revision,),
        detail="publication receipt source revision provenance",
    )
    assert source_revision_row is not None
    if _as_bytes(source_revision_row[0], field="receipt source channel") != channel:
        raise CatalogSemanticValidationError(
            "publication receipt source revision belongs to another channel"
        )
    provenance_analysis_id = _as_bytes(
        source_revision_row[1], field="receipt source provenance analysis_id"
    )
    if provenance_analysis_id != analysis_id:
        raise CatalogSemanticValidationError(
            "publication receipt source revision was not produced by its candidate analysis"
        )
    revision_snapshot_manifest = _as_bytes(
        source_revision_row[2], field="receipt source snapshot_manifest_sha256"
    )
    binding_row = _one(
        connector,
        """
        SELECT snapshot_manifest_sha256
        FROM catalog_analysis_snapshot_manifest
        WHERE analysis_id = %s
        LIMIT 2
        """,
        (analysis_id,),
        detail="publication candidate analysis snapshot output",
    )
    assert binding_row is not None
    if (
        _as_bytes(
            binding_row[0],
            field="publication analysis_snapshot_manifest.snapshot_manifest_sha256",
        )
        != revision_snapshot_manifest
    ):
        raise CatalogSemanticValidationError(
            "publication source revision manifest differs from its candidate analysis output"
        )

    build_row = _one(
        connector,
        """
        SELECT run.build_id, build.state, build_channel.channel
        FROM catalog_analysis_runs AS run
        JOIN catalog_source_builds AS build
          ON build.build_id = run.build_id
        JOIN catalog_source_build_channel AS build_channel
          ON build_channel.build_id = build.build_id
        WHERE run.analysis_id = %s
        LIMIT 2
        """,
        (analysis_id,),
        detail="publication candidate source build",
    )
    assert build_row is not None
    build_id = _as_bytes(build_row[0], field="candidate source build_id")
    if len(build_id) != 16:
        raise CatalogSemanticValidationError(
            "publication candidate source build_id is not 16 bytes"
        )
    if build_row[1] != "SEALED":
        raise CatalogSemanticValidationError(
            "publication candidate source build is not SEALED"
        )
    if _as_bytes(build_row[2], field="candidate source build channel") != channel:
        raise CatalogSemanticValidationError(
            "publication candidate source build belongs to another channel"
        )

    candidate_base = _one(
        connector,
        """
        SELECT base_receipt_id
        FROM catalog_publication_candidate_base_publication_commits
        WHERE candidate_id = %s
        LIMIT 2
        """,
        (candidate_id,),
        detail="consumed publication candidate base authority",
        optional=True,
    )
    if candidate_base is not None:
        raise CatalogSemanticValidationError(
            "PUBLISHED publication retained its consumed candidate base authority"
        )

    build_base_row = _one(
        connector,
        """
        SELECT base.base_receipt_id, commit_row.revision,
               commit_row.source_revision, commit_row.generation,
               source_revision.channel
        FROM catalog_source_build_base_publication_commits AS base
        JOIN catalog_publication_commits AS commit_row
          ON commit_row.receipt_id = base.base_receipt_id
        JOIN catalog_source_revisions AS source_revision
          ON source_revision.source_revision = commit_row.source_revision
        WHERE base.build_id = %s
        LIMIT 2
        """,
        (build_id,),
        detail="publication analysis build base source",
        optional=True,
    )
    if build_base_row is None:
        malformed_build_base = _one(
            connector,
            "SELECT base_receipt_id "
            "FROM catalog_source_build_base_publication_commits "
            "WHERE build_id = %s LIMIT 2",
            (build_id,),
            detail="publication analysis raw build baseline",
            optional=True,
        )
        if malformed_build_base is not None:
            raise CatalogSemanticValidationError(
                "publication analysis build baseline lacks its sealed predecessor"
            )
    build_base = _base_commit_tuple(
        build_base_row,
        detail="publication analysis build base commit",
    )
    expected_source_generation: int | None = None
    if build_base is not None:
        (
            _base_receipt_id,
            _base_catalog_revision,
            base_source_revision,
            base_generation,
            base_channel,
        ) = build_base
        if base_channel != channel or source_revision <= base_source_revision:
            raise CatalogSemanticValidationError(
                "publication source revision does not advance its build base commit"
            )
        expected_source_generation = base_generation + 1

    source_head_row = _one(
        connector,
        """
        SELECT head.source_revision, mapping.generation, advanced.advanced_at
        FROM catalog_source_head_revisions AS head
        LEFT JOIN catalog_source_revision_generations AS mapping
          ON mapping.source_revision = head.source_revision
        LEFT JOIN catalog_source_head_advanced_ats AS advanced
          ON advanced.channel = head.channel
        WHERE head.channel = %s
        LIMIT 2
        """,
        (channel,),
        detail="publication source head",
    )
    assert source_head_row is not None
    if source_head_row[1] is None or source_head_row[2] is None:
        raise CatalogSemanticValidationError(
            "publication source_head projection is incomplete"
        )
    head_source_revision = _as_int(
        source_head_row[0],
        field="publication source_head.source_revision",
        positive=True,
    )
    head_source_generation = _as_int(
        source_head_row[1], field="publication source_head.generation", positive=True
    )
    _as_int(source_head_row[2], field="publication source_head.advanced_at")
    if head_source_revision != source_revision:
        raise CatalogSemanticValidationError(
            "publication receipt does not match the active source-head CAS result"
        )
    if expected_source_generation is None:
        if head_source_generation == 1:
            return
        _require_compacted_current_source_handoff(
            connector,
            channel=channel,
            revision=source_revision,
            generation=head_source_generation,
            analysis_id=analysis_id,
            build_id=build_id,
        )
        return
    if head_source_generation != expected_source_generation:
        raise CatalogSemanticValidationError(
            "publication receipt does not match its retained build-base generation"
        )


def _validate_active_publication_policies(
    connector: SQLConnector,
    *,
    artifact_policy_id: int,
    display_title_policy_id: int,
) -> None:
    artifact_policy_row = _one(
        connector,
        """
        SELECT policy.policy_component_sha256,
               semantics.artifact_algorithm_version,
               semantics.policy_fingerprint_sha256,
               adapter.adapter_id
        FROM catalog_artifact_policies AS policy
        JOIN catalog_artifact_policy_semantics AS semantics
          ON semantics.policy_component_sha256 = policy.policy_component_sha256
        JOIN catalog_artifact_adapter_policy AS adapter
          ON adapter.policy_fingerprint_sha256 =
             semantics.policy_fingerprint_sha256
        WHERE policy.artifact_policy_id = %s
        LIMIT 2
        """,
        (artifact_policy_id,),
        detail="active artifact policy",
    )
    assert artifact_policy_row is not None
    policy_component = _as_bytes(
        artifact_policy_row[0], field="artifact policy component"
    )
    algorithm_version = _as_int(
        artifact_policy_row[1], field="artifact algorithm version", positive=True
    )
    policy_fingerprint = _as_bytes(
        artifact_policy_row[2], field="artifact policy fingerprint"
    )
    adapter_id = _as_bytes(artifact_policy_row[3], field="artifact adapter identifier")
    if algorithm_version not in _SUPPORTED_ARTIFACT_ALGORITHM_VERSIONS:
        raise CatalogSemanticValidationError(
            "active artifact policy uses an unregistered runtime algorithm version"
        )
    try:
        require_digest32(
            policy_fingerprint,
            field="active artifact policy fingerprint",
        )
        require_ascii_bytes(
            adapter_id,
            field="active artifact adapter identifier",
            minimum=1,
            maximum=64,
        )
    except (TypeError, ValueError) as error:
        raise CatalogSemanticValidationError(
            "active artifact adapter policy contains invalid bounded facts"
        ) from error
    if (
        identity.artifact_policy_digest(
            algorithm_version,
            adapter_id,
            policy_fingerprint,
        )
        != policy_component
    ):
        raise CatalogSemanticValidationError(
            "active artifact policy component does not match its exact tuple"
        )
    _require_canonical_domain(
        connector,
        policy_component,
        b"artifact_policy_v3",
        detail="active artifact policy component",
    )
    display_policy_row = _one(
        connector,
        """
        SELECT display.display_title_algorithm_version,
               display.title_sort_policy_id,
               sort_policy.title_sort_algorithm_version,
               sort_policy.unicode_data_version
        FROM catalog_display_title_policies AS display
        JOIN catalog_title_sort_policy AS sort_policy
          ON sort_policy.title_sort_policy_id = display.title_sort_policy_id
        WHERE display.display_title_policy_id = %s
        LIMIT 2
        """,
        (display_title_policy_id,),
        detail="active display/title-sort policy",
    )
    assert display_policy_row is not None
    policy_tuple = (
        _as_int(
            display_policy_row[0],
            field="display title algorithm version",
            positive=True,
        ),
        _as_int(
            display_policy_row[2],
            field="title sort algorithm version",
            positive=True,
        ),
        display_policy_row[3],
    )
    _as_int(
        display_policy_row[1],
        field="display title sort policy",
        positive=True,
    )
    if not isinstance(policy_tuple[2], bytes):
        raise CatalogSemanticValidationError(
            "active Unicode data version is not strict bytes"
        )
    if policy_tuple not in _SUPPORTED_DISPLAY_TITLE_POLICY_ALGORITHMS:
        raise CatalogSemanticValidationError(
            "active display/title-sort policy uses an unsupported runtime "
            "algorithm/Unicode tuple"
        )


def _publication_terminal_stage_count(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
    stage: bytes,
) -> int:
    row = _one(
        connector,
        """
        SELECT checkpoint.generation, checkpoint.`cursor`,
               checkpoint.processed_count, checkpoint.state,
               checkpoint.updated_at, receipt.start_cursor,
               receipt.start_processed_count, receipt.next_cursor,
               receipt.next_processed_count, receipt.next_state,
               receipt.row_count, receipt.terminal,
               receipt.committed_generation, receipt.committed_at
        FROM catalog_publication_checkpoints AS checkpoint
        JOIN catalog_publication_batch_receipts AS receipt
          ON receipt.candidate_id = checkpoint.candidate_id
         AND receipt.stage = checkpoint.stage
         AND receipt.committed_generation = checkpoint.generation
        WHERE checkpoint.candidate_id = %s AND checkpoint.stage = %s
        LIMIT 2
        """,
        (candidate_id, stage),
        detail=f"terminal publication stage {stage!r}",
    )
    assert row is not None
    generation = _as_int(
        row[0], field="publication checkpoint generation", positive=True
    )
    cursor = _as_bytes(row[1], field="publication checkpoint cursor")
    processed_count = _as_int(row[2], field="publication checkpoint processed_count")
    state = row[3]
    updated_at = _as_int(row[4], field="publication checkpoint updated_at")
    start_cursor = _as_bytes(row[5], field="publication receipt start_cursor")
    start_count = _as_int(row[6], field="publication receipt start_processed_count")
    next_cursor = _as_bytes(row[7], field="publication receipt next_cursor")
    next_count = _as_int(row[8], field="publication receipt next_processed_count")
    next_state = row[9]
    row_count = _as_int(row[10], field="publication receipt row_count")
    terminal = _as_int(row[11], field="publication receipt terminal")
    committed_generation = _as_int(
        row[12], field="publication receipt committed_generation", positive=True
    )
    committed_at = _as_int(row[13], field="publication receipt committed_at")
    if (
        state != "COMPLETE"
        or next_state != "COMPLETE"
        or terminal != 1
        or row_count != 0
        or start_cursor != cursor
        or next_cursor != cursor
        or start_count != processed_count
        or next_count != processed_count
        or committed_generation != generation
        or committed_at != updated_at
    ):
        raise CatalogSemanticValidationError(
            f"publication stage {stage!r} lacks its exact current terminal receipt"
        )
    return processed_count


def _validate_catalog_occurrence_storage(
    connector: SQLConnector,
    *,
    revision: int,
) -> None:
    """Boundedly reject an occurrence whose payload or gallery key is missing.

    The publication family rederives the occurrence digest on every insert and
    keyed load.  READY audits relational congruence with a one-row anti-join;
    it must not materialize and rehash an entire million-row active revision.
    """

    mismatch = connector.fetch_all(
        """
        SELECT occurrence.catalog_occurrence_sha256
        FROM catalog_publication_occurrence_identities AS occurrence
        LEFT JOIN catalog_publication_storage AS stored
          ON stored.catalog_occurrence_sha256 =
             occurrence.catalog_occurrence_sha256
        LEFT JOIN catalog_publication_download_times AS downloaded
          ON downloaded.catalog_occurrence_sha256 =
             occurrence.catalog_occurrence_sha256
        LEFT JOIN catalog_gallery_source_name_accesses AS access
          ON access.gallery_id = stored.gallery_id
        LEFT JOIN catalog_source_gallery_name_gids AS name_gid
          ON name_gid.source_gallery_name = access.source_gallery_name
        LEFT JOIN catalog_publication_identities AS derived
          ON derived.gid = name_gid.gid
        WHERE occurrence.revision = %s
          AND (
            stored.catalog_occurrence_sha256 IS NULL
            OR downloaded.catalog_occurrence_sha256 IS NULL
            OR derived.publication_key IS NULL
            OR derived.publication_key <> occurrence.publication_key
          )
        LIMIT 1
        """,
        (revision,),
    )
    if mismatch:
        raise CatalogSemanticValidationError(
            "active catalog occurrence identity/storage/download-time is not congruent"
        )


_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_CATALOG_RESOURCE_PAGE_LIMIT = 128
_CONTRIBUTOR_FACET_NAMESPACES = frozenset(
    {b"artist", b"author", b"cosplayer", b"group", b"illustrator", b"uploader"}
)


def _catalog_datetime(value: object, *, field: str) -> datetime:
    microseconds = _as_int(value, field=field)
    try:
        return _UNIX_EPOCH + timedelta(microseconds=microseconds)
    except OverflowError as error:
        raise CatalogSemanticValidationError(
            f"{field} is outside the supported datetime range"
        ) from error


def _catalog_storage_key(
    connector: SQLConnector,
    key_digest: bytes,
) -> StorageObjectKey:
    require_digest32(key_digest, field="catalog storage object key digest")
    rows = connector.fetch_all(
        "SELECT key_codec, segment_count "
        "FROM catalog_storage_object_key_identities "
        "WHERE storage_object_key_sha256 = %s LIMIT 2",
        (key_digest,),
    )
    if len(rows) != 1 or any(value is None for value in rows[0]):
        raise CatalogSemanticValidationError(
            "catalog storage object key identity is missing or duplicated"
        )
    try:
        codec = require_ascii_bytes(
            rows[0][0],
            field="catalog storage object key codec",
            minimum=1,
            maximum=64,
        ).decode("ascii")
        segment_count = _as_int(
            rows[0][1], field="catalog storage object segment_count", positive=True
        )
        if segment_count > 16:
            raise CatalogSemanticValidationError(
                "catalog storage object key exceeds sixteen segments"
            )
        segment_rows = connector.fetch_all(
            "SELECT segment_position, key_segment "
            "FROM catalog_storage_object_key_segments "
            "WHERE storage_object_key_sha256 = %s "
            "ORDER BY segment_position LIMIT 17",
            (key_digest,),
        )
        if len(segment_rows) != segment_count:
            raise CatalogSemanticValidationError(
                "catalog storage object key segment family is incomplete"
            )
        segments: list[str] = []
        for expected_position, segment_row in enumerate(segment_rows):
            if (
                _as_int(
                    segment_row[0],
                    field="catalog storage object segment position",
                )
                != expected_position
            ):
                raise CatalogSemanticValidationError(
                    "catalog storage object key segments are not dense"
                )
            segments.append(
                _as_bytes(
                    segment_row[1],
                    field="catalog storage object key segment",
                ).decode("utf-8", errors="strict")
            )
        key = StorageObjectKey(codec=codec, segments=tuple(segments))
        if identity.artifact_storage_key_digest(key.codec, key.segments) != key_digest:
            raise CatalogSemanticValidationError(
                "catalog storage object key digest is noncongruent"
            )
        return key
    except (TypeError, ValueError, UnicodeError) as error:
        if isinstance(error, CatalogSemanticValidationError):
            raise
        raise CatalogSemanticValidationError(
            "catalog storage object key violates its public bounded domain"
        ) from error


def _catalog_storage_objects(
    connector: SQLConnector,
    *,
    revision: int,
    publication_key: bytes,
) -> dict[bytes, StorageObjectDescriptor]:
    rows = connector.fetch_all(
        "SELECT stored.resource_kind, stored.storage_object_key_sha256, "
        "stored.storage_object_sha256, stored.size_bytes, stored.modified_at, "
        "blob_row.size_bytes "
        "FROM catalog_storage_objects AS stored "
        "LEFT JOIN catalog_artifact_blobs AS blob_row "
        "ON blob_row.artifact_sha256 = stored.storage_object_sha256 "
        "WHERE stored.revision = %s AND stored.publication_key = %s "
        "ORDER BY stored.resource_kind LIMIT 3",
        (revision, publication_key),
    )
    resources: dict[bytes, StorageObjectDescriptor] = {}
    for row in rows:
        if len(row) != 6 or any(value is None for value in row):
            raise CatalogSemanticValidationError(
                "catalog storage object lacks its complete key/blob authority"
            )
        kind = _as_bytes(row[0], field="catalog storage object resource_kind")
        if kind not in {b"acquisition", b"thumbnail"} or kind in resources:
            raise CatalogSemanticValidationError(
                "catalog storage object has an unknown or duplicate resource kind"
            )
        key_digest = _as_bytes(row[1], field="catalog storage object key digest")
        object_digest = _as_bytes(row[2], field="catalog storage object digest")
        try:
            require_digest32(key_digest, field="catalog storage object key digest")
            require_digest32(object_digest, field="catalog storage object digest")
            size_bytes = _as_int(
                row[3], field="catalog storage object size", positive=True
            )
            if (
                _as_int(row[5], field="catalog resource blob size", positive=True)
                != size_bytes
            ):
                raise CatalogSemanticValidationError(
                    "catalog storage object size differs from its verified blob"
                )
            key = _catalog_storage_key(connector, key_digest)
            resources[kind] = StorageObjectDescriptor(
                key=key,
                size_bytes=size_bytes,
                sha256=object_digest.hex(),
                modified_at=_catalog_datetime(
                    row[4], field="catalog storage object modified_at"
                ),
            )
        except (TypeError, ValueError, UnicodeError) as error:
            if isinstance(error, CatalogSemanticValidationError):
                raise
            raise CatalogSemanticValidationError(
                "catalog storage object violates its public bounded domain"
            ) from error
    return resources


def _validate_catalog_image_row(
    row: tuple[Any, ...],
    *,
    storage_object: StorageObjectDescriptor,
    expected_kind: bytes,
    expected_index: int | None,
    detail: str,
) -> CatalogImageResource:
    index_offset = 1 if expected_index is not None else 0
    expected_columns = 8 if expected_index is not None else 7
    if len(row) != expected_columns:
        raise CatalogSemanticValidationError(f"{detail} has an invalid row shape")
    kind = _as_bytes(row[0], field=f"{detail} resource_kind")
    if kind != expected_kind:
        raise CatalogSemanticValidationError(f"{detail} uses the wrong storage object")
    if (
        expected_index is not None
        and _as_int(row[1], field=f"{detail} page_index") != expected_index
    ):
        raise CatalogSemanticValidationError(f"{detail} indices are not dense")
    try:
        extent = ByteExtent(
            offset=_as_int(row[index_offset + 1], field=f"{detail} extent offset"),
            length=_as_int(
                row[index_offset + 2],
                field=f"{detail} extent length",
                positive=True,
            ),
        )
        media_type = _as_bytes(
            row[index_offset + 3], field=f"{detail} media_type"
        ).decode("ascii", errors="strict")
        image_digest = _as_bytes(row[index_offset + 4], field=f"{detail} image digest")
        require_digest32(image_digest, field=f"{detail} image digest")
        return CatalogImageResource(
            storage_object=storage_object,
            extent=extent,
            media_type=media_type,
            sha256=image_digest.hex(),
            width=_as_int(
                row[index_offset + 5], field=f"{detail} width", positive=True
            ),
            height=_as_int(
                row[index_offset + 6], field=f"{detail} height", positive=True
            ),
        )
    except (TypeError, ValueError, UnicodeError) as error:
        raise CatalogSemanticValidationError(
            f"{detail} violates its public bounded domain"
        ) from error


def _validate_active_catalog_resources(
    connector: SQLConnector,
    *,
    revision: int,
    expected_artifact_count: int,
) -> None:
    orphan = connector.fetch_all(
        "SELECT stored.publication_key FROM catalog_storage_objects AS stored "
        "LEFT JOIN catalog_artifacts AS artifact "
        "ON artifact.revision = stored.revision "
        "AND artifact.publication_key = stored.publication_key "
        "WHERE stored.revision = %s AND artifact.publication_key IS NULL LIMIT 1",
        (revision,),
    )
    if orphan:
        raise CatalogSemanticValidationError(
            "active catalog has presentation storage without an artifact"
        )

    after = b""
    artifact_count = 0
    while True:
        rows = connector.fetch_all(
            "SELECT artifact.publication_key, identity_row.gid, "
            "artifact.artifact_sha256, artifact.artifact_name, "
            "artifact.media_type, artifact.page_count, blob_row.size_bytes "
            "FROM catalog_artifacts AS artifact "
            "LEFT JOIN catalog_publication_identities AS identity_row "
            "ON identity_row.publication_key = artifact.publication_key "
            "LEFT JOIN catalog_artifact_blobs AS blob_row "
            "ON blob_row.artifact_sha256 = artifact.artifact_sha256 "
            "WHERE artifact.revision = %s AND artifact.publication_key > %s "
            "ORDER BY artifact.publication_key LIMIT %s",
            (revision, after, _CATALOG_RESOURCE_PAGE_LIMIT),
        )
        if not rows:
            break
        for row in rows:
            if len(row) != 7 or any(value is None for value in row):
                raise CatalogSemanticValidationError(
                    "catalog artifact lacks its complete identity/blob authority"
                )
            publication_key = _as_bytes(
                row[0], field="catalog artifact publication_key"
            )
            try:
                require_digest32(
                    publication_key, field="catalog artifact publication_key"
                )
                gid = _as_int(row[1], field="catalog artifact gid", positive=True)
                if identity.publication_key(gid) != publication_key:
                    raise CatalogSemanticValidationError(
                        "catalog artifact publication key disagrees with its GID"
                    )
                artifact_digest = _as_bytes(row[2], field="catalog artifact digest")
                require_digest32(artifact_digest, field="catalog artifact digest")
                artifact_name = _as_bytes(row[3], field="catalog artifact name").decode(
                    "utf-8", errors="strict"
                )
                media_type = _as_bytes(
                    row[4], field="catalog artifact media_type"
                ).decode("ascii", errors="strict")
                page_count = _as_int(row[5], field="catalog artifact page_count")
                if page_count > 4096:
                    raise CatalogSemanticValidationError(
                        "catalog artifact exceeds the generic page-count bound"
                    )
                artifact_size = _as_int(
                    row[6], field="catalog artifact blob size", positive=True
                )
                resources = _catalog_storage_objects(
                    connector,
                    revision=revision,
                    publication_key=publication_key,
                )
                expected_kinds = (
                    {b"acquisition", b"thumbnail"} if page_count else {b"acquisition"}
                )
                if set(resources) != expected_kinds:
                    raise CatalogSemanticValidationError(
                        "catalog artifact storage roles are incomplete or excessive"
                    )
                acquisition = resources[b"acquisition"]
                if (
                    acquisition.sha256 != artifact_digest.hex()
                    or acquisition.size_bytes != artifact_size
                ):
                    raise CatalogSemanticValidationError(
                        "catalog acquisition differs from artifact byte authority"
                    )
                CatalogArtifact(
                    artifact_id=identity.artifact_id(gid, artifact_digest).decode(
                        "ascii"
                    ),
                    name=artifact_name,
                    storage_object=acquisition,
                    media_type=media_type,
                )
            except (TypeError, ValueError, UnicodeError) as error:
                if isinstance(error, CatalogSemanticValidationError):
                    raise
                raise CatalogSemanticValidationError(
                    "catalog artifact violates its public bounded domain"
                ) from error

            page_rows = connector.fetch_all(
                "SELECT resource_kind, page_index, extent_offset, extent_length, "
                "media_type, image_sha256, width, height FROM catalog_pages "
                "WHERE revision = %s AND publication_key = %s "
                "ORDER BY page_index LIMIT 4097",
                (revision, publication_key),
            )
            if len(page_rows) != page_count:
                raise CatalogSemanticValidationError(
                    "catalog page coverage differs from artifact page_count"
                )
            for page_index, page_row in enumerate(page_rows):
                _validate_catalog_image_row(
                    page_row,
                    storage_object=acquisition,
                    expected_kind=b"acquisition",
                    expected_index=page_index,
                    detail="catalog page",
                )

            thumbnail_rows = connector.fetch_all(
                "SELECT resource_kind, extent_offset, extent_length, media_type, "
                "image_sha256, width, height FROM catalog_thumbnails "
                "WHERE revision = %s AND publication_key = %s LIMIT 2",
                (revision, publication_key),
            )
            if len(thumbnail_rows) != (1 if page_count else 0):
                raise CatalogSemanticValidationError(
                    "catalog thumbnail totality differs from artifact page_count"
                )
            if thumbnail_rows:
                thumbnail_object = resources[b"thumbnail"]
                thumbnail = _validate_catalog_image_row(
                    thumbnail_rows[0],
                    storage_object=thumbnail_object,
                    expected_kind=b"thumbnail",
                    expected_index=None,
                    detail="catalog thumbnail",
                )
                if (
                    thumbnail.extent.offset != 0
                    or thumbnail.extent.length != thumbnail_object.size_bytes
                    or thumbnail.sha256 != thumbnail_object.sha256
                ):
                    raise CatalogSemanticValidationError(
                        "catalog thumbnail is not its complete sealed object"
                    )
            artifact_count += 1
            if artifact_count > (1 << 63) - 1:
                raise CatalogSemanticValidationError(
                    "catalog artifact count exceeds signed-int63"
                )
        after = _as_bytes(rows[-1][0], field="catalog artifact keyset cursor")
    if artifact_count != expected_artifact_count:
        raise CatalogSemanticValidationError(
            "active artifact coverage differs from catalog revision artifact_count"
        )


class _CanonicalValidationCache:
    """Bound successful canonical reads to one READY-audit snapshot.

    The caller owns this cache for exactly one discovery refinement pass inside
    the schema validator's read transaction.  Values too large for the fixed
    memory budget retain the original streaming path, and eviction only
    removes an optimization: a later access validates the canonical tree
    again.
    """

    __slots__ = ("_byte_count", "_values")

    def __init__(self) -> None:
        self._values: OrderedDict[tuple[bytes, bytes], bytes] = OrderedDict()
        self._byte_count = 0

    def open(
        self,
        value_sha256: bytes,
        expected_domain: bytes,
    ) -> tuple[BinaryIO, int] | None:
        key = (value_sha256, expected_domain)
        payload = self._values.pop(key, None)
        if payload is None:
            return None
        self._values[key] = payload
        return BytesIO(payload), len(payload)

    def remember(
        self,
        value_sha256: bytes,
        expected_domain: bytes,
        spool: BinaryIO,
        *,
        byte_count: int,
    ) -> None:
        if byte_count > _CANONICAL_VALIDATION_CACHE_MAX_VALUE_BYTES:
            return
        spool.seek(0)
        payload = spool.read(byte_count + 1)
        spool.seek(0)
        if not isinstance(payload, bytes) or len(payload) != byte_count:
            raise CatalogSemanticValidationError(
                "validated canonical spool changed before bounded caching"
            )
        key = (value_sha256, expected_domain)
        previous = self._values.pop(key, None)
        if previous is not None:
            self._byte_count -= len(previous)
        while self._values and (
            len(self._values) >= _CANONICAL_VALIDATION_CACHE_MAX_ENTRIES
            or self._byte_count + byte_count
            > _CANONICAL_VALIDATION_CACHE_MAX_TOTAL_BYTES
        ):
            _evicted_key, evicted = self._values.popitem(last=False)
            self._byte_count -= len(evicted)
        self._values[key] = payload
        self._byte_count += byte_count


def _validated_canonical_spool(
    connector: SQLConnector,
    value_sha256: bytes,
    *,
    expected_domain: bytes,
    detail: str,
    cache: _CanonicalValidationCache | None = None,
) -> tuple[BinaryIO, int]:
    if (
        cache is not None
        and type(value_sha256) is bytes
        and type(expected_domain) is bytes
    ):
        cached = cache.open(value_sha256, expected_domain)
        if cached is not None:
            return cached
    spool = TemporaryFile(mode="w+b")
    written = 0

    def consume(part: bytes) -> None:
        nonlocal written
        if spool.write(part) != len(part):
            raise OSError("canonical READY spool accepted a partial write")
        written += len(part)

    try:
        receipt = stream_and_validate_canonical_value(
            connector,
            value_sha256=value_sha256,
            consume_provisional=consume,
        )
        if receipt.digest_domain != expected_domain or written != receipt.byte_count:
            raise CatalogSemanticValidationError(
                f"{detail} has the wrong domain or exact byte count"
            )
        spool.flush()
        spool.seek(0)
        if cache is not None:
            cache.remember(
                value_sha256,
                expected_domain,
                spool,
                byte_count=receipt.byte_count,
            )
        return spool, receipt.byte_count
    except (CanonicalValueCollisionError, CanonicalValueNotReadyError) as error:
        spool.close()
        raise CatalogSemanticValidationError(
            f"{detail} canonical byte authority is incomplete or corrupt"
        ) from error
    except BaseException:
        spool.close()
        raise


def _casefolded_title_spool(
    title: BinaryIO,
    *,
    byte_count: int,
) -> tuple[BinaryIO, int]:
    """Apply the pinned title-sort transform with bounded streaming memory."""

    folded = TemporaryFile(mode="w+b")
    written = 0
    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    try:
        for part in _iter_spool_chunks(title, byte_count=byte_count):
            text = decoder.decode(part, final=False)
            encoded = text.casefold().encode("utf-8")
            if encoded:
                if folded.write(encoded) != len(encoded):
                    raise OSError("title-sort READY spool accepted a partial write")
                written += len(encoded)
        tail = decoder.decode(b"", final=True)
        encoded_tail = tail.casefold().encode("utf-8")
        if encoded_tail:
            if folded.write(encoded_tail) != len(encoded_tail):
                raise OSError("title-sort READY spool accepted a partial tail write")
            written += len(encoded_tail)
        folded.flush()
        folded.seek(0)
        return folded, written
    except UnicodeError as error:
        folded.close()
        raise CatalogSemanticValidationError(
            "retained display title is not strict UTF-8"
        ) from error
    except BaseException:
        folded.close()
        raise


def _iter_spool_chunks(source: BinaryIO, *, byte_count: int) -> Iterator[bytes]:
    source.seek(0)
    remaining = byte_count
    while remaining:
        chunk = source.read(min(64 * 1024, remaining))
        if not isinstance(chunk, bytes) or not chunk:
            raise CatalogSemanticValidationError(
                "canonical READY spool ended before its validated byte count"
            )
        remaining -= len(chunk)
        yield chunk
    if source.read(1) not in {b"", None}:
        raise CatalogSemanticValidationError(
            "canonical READY spool exceeds its validated byte count"
        )


def _register_expected_search_field(
    connector: SQLConnector,
    expected: sqlite3.Connection,
    *,
    publication_key: bytes,
    parts: Iterable[bytes],
    cache: _CanonicalValidationCache,
) -> None:
    try:
        for lexeme in iter_search_field_lexemes(parts):
            value_sha256 = identity.canonical_value_digest(
                "search_lexeme_utf8_v1",
                lexeme,
            )
            already_validated = expected.execute(
                "SELECT 1 FROM validated_lexemes WHERE value_sha256 = ?",
                (sqlite3.Binary(value_sha256),),
            ).fetchone()
            if already_validated is None:
                marker = connector.fetch_all(
                    "SELECT value_sha256 FROM catalog_search_lexemes "
                    "WHERE value_sha256 = %s LIMIT 2",
                    (value_sha256,),
                )
                if marker != [(value_sha256,)]:
                    raise CatalogSemanticValidationError(
                        "derived search lexeme lacks its exact marker"
                    )
                spool, byte_count = _validated_canonical_spool(
                    connector,
                    value_sha256,
                    expected_domain=b"search_lexeme_utf8_v1",
                    detail="derived search lexeme",
                    cache=cache,
                )
                try:
                    if (
                        byte_count != len(lexeme)
                        or spool.read(byte_count + 1) != lexeme
                    ):
                        raise CatalogSemanticValidationError(
                            "derived search lexeme digest maps to different exact bytes"
                        )
                finally:
                    spool.close()
                expected.execute(
                    "INSERT INTO validated_lexemes (value_sha256) VALUES (?)",
                    (sqlite3.Binary(value_sha256),),
                )
            inserted = expected.execute(
                "INSERT OR IGNORE INTO expected_postings "
                "(publication_key, value_sha256) VALUES (?, ?)",
                (sqlite3.Binary(publication_key), sqlite3.Binary(value_sha256)),
            )
            if inserted.rowcount == 1:
                updated = expected.execute(
                    "UPDATE expected_documents SET row_count = row_count + 1 "
                    "WHERE publication_key = ?",
                    (sqlite3.Binary(publication_key),),
                )
                if updated.rowcount != 1:
                    raise CatalogSemanticValidationError(
                        "derived search document authority is missing"
                    )
    except (TypeError, ValueError, UnicodeError) as error:
        raise CatalogSemanticValidationError(
            "active search source field violates the pinned tokenizer policy"
        ) from error


def _validate_one_publication_search_projection(
    connector: SQLConnector,
    expected: sqlite3.Connection,
    *,
    revision: int,
    publication_key: bytes,
    language_sha256: bytes,
    source_title_sha256: bytes,
    display_title_sha256: bytes,
    source_gallery_name: bytes,
    cache: _CanonicalValidationCache,
) -> None:
    try:
        expected.execute(
            "INSERT INTO expected_documents (publication_key, row_count) VALUES (?, 0)",
            (sqlite3.Binary(publication_key),),
        )
    except sqlite3.IntegrityError as error:
        raise CatalogSemanticValidationError(
            "active search source repeats a publication"
        ) from error
    expected.execute(
        "INSERT INTO expected_languages (language_sha256, occurrence_count) "
        "VALUES (?, 1) ON CONFLICT(language_sha256) DO UPDATE SET "
        "occurrence_count = occurrence_count + 1",
        (sqlite3.Binary(language_sha256),),
    )
    title_spool, title_byte_count = _validated_canonical_spool(
        connector,
        source_title_sha256,
        expected_domain=b"source_title_utf8_v1",
        detail="active publication source title",
        cache=cache,
    )
    try:
        _register_expected_search_field(
            connector,
            expected,
            publication_key=publication_key,
            parts=_iter_spool_chunks(title_spool, byte_count=title_byte_count),
            cache=cache,
        )
    finally:
        title_spool.close()
    display_title_spool, display_title_byte_count = _validated_canonical_spool(
        connector,
        display_title_sha256,
        expected_domain=b"display_title_utf8_v1",
        detail="active publication display title",
        cache=cache,
    )
    try:
        if display_title_byte_count == 0:
            raise CatalogSemanticValidationError(
                "active publication display title is empty"
            )
        _register_expected_search_field(
            connector,
            expected,
            publication_key=publication_key,
            parts=_iter_spool_chunks(
                display_title_spool,
                byte_count=display_title_byte_count,
            ),
            cache=cache,
        )
    finally:
        display_title_spool.close()

    expected_position = 0
    after_position = -1
    while True:
        contributors = connector.fetch_all(
            "SELECT position, contributor_name_sha256, role "
            "FROM catalog_contributors WHERE revision = %s "
            "AND publication_key = %s AND position > %s "
            "ORDER BY position LIMIT %s",
            (
                revision,
                publication_key,
                after_position,
                _CATALOG_RESOURCE_PAGE_LIMIT,
            ),
        )
        if not contributors:
            break
        for row in contributors:
            if len(row) != 3:
                raise CatalogSemanticValidationError(
                    "active contributor search row is malformed"
                )
            position = _as_int(row[0], field="active contributor position")
            if position != expected_position:
                raise CatalogSemanticValidationError(
                    "active contributor positions are not exactly contiguous"
                )
            digest = _as_bytes(row[1], field="active contributor name digest")
            role = _as_bytes(row[2], field="active contributor role")
            if role not in _CONTRIBUTOR_FACET_NAMESPACES:
                raise CatalogSemanticValidationError(
                    "active contributor uses an unregistered facet role"
                )
            expected.execute(
                "INSERT INTO expected_contributors "
                "(contributor_name_sha256, role, occurrence_count) "
                "VALUES (?, ?, 1) ON CONFLICT(contributor_name_sha256, role) "
                "DO UPDATE SET occurrence_count = occurrence_count + 1",
                (sqlite3.Binary(digest), sqlite3.Binary(role)),
            )
            spool, byte_count = _validated_canonical_spool(
                connector,
                digest,
                expected_domain=b"contributor_name_utf8_v1",
                detail="active contributor name",
                cache=cache,
            )
            try:
                if not 1 <= byte_count <= 65_536:
                    raise CatalogSemanticValidationError(
                        "active contributor name is outside its writer bound"
                    )
                _register_expected_search_field(
                    connector,
                    expected,
                    publication_key=publication_key,
                    parts=_iter_spool_chunks(spool, byte_count=byte_count),
                    cache=cache,
                )
            finally:
                spool.close()
            expected_position += 1
            after_position = position

    expected_position = 0
    after_position = -1
    while True:
        subjects = connector.fetch_all(
            "SELECT subject.position, subject.tag_id, term.namespace, "
            "term.tag_value_sha256 "
            "FROM catalog_subjects AS subject "
            "JOIN catalog_tag_terms AS term ON term.tag_id = subject.tag_id "
            "WHERE subject.revision = %s AND subject.publication_key = %s "
            "AND subject.position > %s ORDER BY subject.position LIMIT %s",
            (
                revision,
                publication_key,
                after_position,
                _CATALOG_RESOURCE_PAGE_LIMIT,
            ),
        )
        if not subjects:
            break
        for row in subjects:
            if len(row) != 4:
                raise CatalogSemanticValidationError(
                    "active subject search row is malformed"
                )
            position = _as_int(row[0], field="active subject position")
            if position != expected_position:
                raise CatalogSemanticValidationError(
                    "active subject positions are not exactly contiguous"
                )
            tag_id = _as_int(row[1], field="active subject tag_id", positive=True)
            namespace = _as_bytes(row[2], field="active subject namespace")
            digest = _as_bytes(row[3], field="active subject value digest")
            try:
                decoded_namespace = namespace.decode("utf-8", errors="strict")
                if identity.validate_namespace(decoded_namespace) != namespace:
                    raise ValueError("tag namespace changed during validation")
            except (TypeError, ValueError, UnicodeError) as error:
                raise CatalogSemanticValidationError(
                    "active subject namespace is not exact bounded UTF-8"
                ) from error
            if (
                namespace != b"language"
                and namespace not in _CONTRIBUTOR_FACET_NAMESPACES
            ):
                expected.execute(
                    "INSERT INTO expected_subjects (tag_id, occurrence_count) "
                    "VALUES (?, 1) ON CONFLICT(tag_id) DO UPDATE SET "
                    "occurrence_count = occurrence_count + 1",
                    (tag_id,),
                )
            spool, byte_count = _validated_canonical_spool(
                connector,
                digest,
                expected_domain=b"tag_value_utf8_v1",
                detail="active subject value",
                cache=cache,
            )
            try:
                if byte_count > 65_536:
                    raise CatalogSemanticValidationError(
                        "active subject value exceeds its writer bound"
                    )
                _register_expected_search_field(
                    connector,
                    expected,
                    publication_key=publication_key,
                    parts=_iter_spool_chunks(spool, byte_count=byte_count),
                    cache=cache,
                )
            finally:
                spool.close()
            expected_position += 1
            after_position = position

    document = _one(
        connector,
        "SELECT row_count FROM catalog_search_documents "
        "WHERE revision = %s AND publication_key = %s LIMIT 2",
        (revision, publication_key),
        detail="active publication search document",
    )
    assert document is not None
    expected_count_row = expected.execute(
        "SELECT row_count FROM expected_documents WHERE publication_key = ?",
        (sqlite3.Binary(publication_key),),
    ).fetchone()
    if expected_count_row is None:
        raise CatalogSemanticValidationError(
            "expected search projection count is missing"
        )
    expected_count = _as_int(
        expected_count_row[0], field="derived search posting count"
    )
    if _as_int(document[0], field="active search document row_count") != expected_count:
        raise CatalogSemanticValidationError(
            "active search document count differs from exact tokenizer output"
        )

    after_lexeme = b""
    while True:
        actual_rows = connector.fetch_all(
            "SELECT value_sha256 FROM catalog_search_postings "
            "WHERE revision = %s AND publication_key = %s "
            "AND value_sha256 > %s ORDER BY value_sha256 LIMIT %s",
            (
                revision,
                publication_key,
                after_lexeme,
                _CATALOG_RESOURCE_PAGE_LIMIT,
            ),
        )
        expected_rows = expected.execute(
            "SELECT value_sha256 FROM expected_postings "
            "WHERE publication_key = ? AND value_sha256 > ? "
            "ORDER BY value_sha256 LIMIT ?",
            (
                sqlite3.Binary(publication_key),
                sqlite3.Binary(after_lexeme),
                _CATALOG_RESOURCE_PAGE_LIMIT,
            ),
        ).fetchall()
        actual = tuple(
            _as_bytes(row[0], field="active search posting value_sha256")
            for row in actual_rows
        )
        derived = tuple(bytes(row[0]) for row in expected_rows)
        if actual != derived:
            raise CatalogSemanticValidationError(
                "active search postings differ from exact tokenizer output"
            )
        if not actual:
            break
        after_lexeme = actual[-1]


def _compare_canonical_spools(left: BinaryIO, right: BinaryIO) -> int:
    left.seek(0)
    right.seek(0)
    while True:
        left_chunk = left.read(64 * 1024)
        right_chunk = right.read(64 * 1024)
        if not isinstance(left_chunk, bytes) or not isinstance(right_chunk, bytes):
            raise CatalogSemanticValidationError(
                "canonical facet spool returned non-bytes"
            )
        if left_chunk != right_chunk:
            return -1 if left_chunk < right_chunk else 1
        if not left_chunk:
            return 0


def _validate_language_facet_order(
    connector: SQLConnector,
    expected: sqlite3.Connection,
    *,
    revision: int,
    cache: _CanonicalValidationCache,
) -> None:
    after = -1
    expected_position = 0
    previous: tuple[BinaryIO, int, bytes] | None = None
    try:
        while True:
            rows = connector.fetch_all(
                "SELECT position, language_sha256, occurrence_count "
                "FROM catalog_language_facet_order WHERE revision = %s "
                "AND position > %s ORDER BY position LIMIT %s",
                (revision, after, _CATALOG_RESOURCE_PAGE_LIMIT),
            )
            if not rows:
                break
            for row in rows:
                if len(row) != 3:
                    raise CatalogSemanticValidationError(
                        "active language facet row is malformed"
                    )
                position = _as_int(row[0], field="active language facet position")
                if position != expected_position:
                    raise CatalogSemanticValidationError(
                        "active language facet positions are not exactly contiguous"
                    )
                digest = _as_bytes(row[1], field="active language facet digest")
                try:
                    require_digest32(digest, field="active language facet digest")
                except (TypeError, ValueError) as error:
                    raise CatalogSemanticValidationError(
                        "active language facet digest is invalid"
                    ) from error
                occurrence_count = _as_int(
                    row[2],
                    field="active language facet occurrence_count",
                    positive=True,
                )
                expected_row = expected.execute(
                    "SELECT occurrence_count FROM expected_languages "
                    "WHERE language_sha256 = ?",
                    (sqlite3.Binary(digest),),
                ).fetchone()
                if expected_row != (occurrence_count,):
                    raise CatalogSemanticValidationError(
                        "active language facet differs from exact membership"
                    )
                expected.execute(
                    "DELETE FROM expected_languages WHERE language_sha256 = ?",
                    (sqlite3.Binary(digest),),
                )
                spool, byte_count = _validated_canonical_spool(
                    connector,
                    digest,
                    expected_domain=b"catalog_language_utf8_v1",
                    detail="active language facet value",
                    cache=cache,
                )
                if not 1 <= byte_count <= 65_536:
                    spool.close()
                    raise CatalogSemanticValidationError(
                        "active language facet value is outside its writer bound"
                    )
                try:
                    spool.read(byte_count + 1).decode("utf-8", errors="strict")
                except UnicodeError as error:
                    spool.close()
                    raise CatalogSemanticValidationError(
                        "active language facet value is not strict UTF-8"
                    ) from error
                if previous is not None:
                    comparison = _compare_canonical_spools(previous[0], spool)
                    if comparison > 0 or (comparison == 0 and previous[2] >= digest):
                        spool.close()
                        raise CatalogSemanticValidationError(
                            "active language facets are not in canonical byte order"
                        )
                    previous[0].close()
                previous = (spool, byte_count, digest)
                expected_position += 1
                after = position
        if expected.execute("SELECT 1 FROM expected_languages LIMIT 1").fetchone():
            raise CatalogSemanticValidationError(
                "active language facets omit current membership"
            )
    finally:
        if previous is not None:
            previous[0].close()


def _validate_subject_facet_order(
    connector: SQLConnector,
    expected: sqlite3.Connection,
    *,
    revision: int,
    cache: _CanonicalValidationCache,
) -> None:
    after = -1
    expected_position = 0
    previous: tuple[bytes, BinaryIO, int, int] | None = None
    try:
        while True:
            rows = connector.fetch_all(
                "SELECT facet.position, facet.tag_id, facet.occurrence_count, "
                "term.namespace, term.tag_value_sha256 "
                "FROM catalog_subject_facet_order AS facet "
                "LEFT JOIN catalog_tag_terms AS term ON term.tag_id = facet.tag_id "
                "WHERE facet.revision = %s AND facet.position > %s "
                "ORDER BY facet.position LIMIT %s",
                (revision, after, _CATALOG_RESOURCE_PAGE_LIMIT),
            )
            if not rows:
                break
            for row in rows:
                if len(row) != 5 or row[3] is None or row[4] is None:
                    raise CatalogSemanticValidationError(
                        "active subject facet lacks its tag authority"
                    )
                position = _as_int(row[0], field="active subject facet position")
                if position != expected_position:
                    raise CatalogSemanticValidationError(
                        "active subject facet positions are not exactly contiguous"
                    )
                tag_id = _as_int(
                    row[1], field="active subject facet tag_id", positive=True
                )
                occurrence_count = _as_int(
                    row[2],
                    field="active subject facet occurrence_count",
                    positive=True,
                )
                expected_row = expected.execute(
                    "SELECT occurrence_count FROM expected_subjects WHERE tag_id = ?",
                    (tag_id,),
                ).fetchone()
                if expected_row != (occurrence_count,):
                    raise CatalogSemanticValidationError(
                        "active subject facet differs from exact membership"
                    )
                expected.execute(
                    "DELETE FROM expected_subjects WHERE tag_id = ?",
                    (tag_id,),
                )
                namespace = _as_bytes(row[3], field="active subject facet namespace")
                if len(namespace) > 128:
                    raise CatalogSemanticValidationError(
                        "active subject facet namespace exceeds its bound"
                    )
                digest = _as_bytes(row[4], field="active subject facet value digest")
                try:
                    require_digest32(digest, field="active subject facet value digest")
                except (TypeError, ValueError) as error:
                    raise CatalogSemanticValidationError(
                        "active subject facet value digest is invalid"
                    ) from error
                spool, byte_count = _validated_canonical_spool(
                    connector,
                    digest,
                    expected_domain=b"tag_value_utf8_v1",
                    detail="active subject facet value",
                    cache=cache,
                )
                if previous is not None:
                    if previous[0] < namespace:
                        comparison = -1
                    elif previous[0] > namespace:
                        comparison = 1
                    else:
                        comparison = _compare_canonical_spools(previous[1], spool)
                        if comparison == 0:
                            comparison = -1 if previous[3] < tag_id else 1
                    if comparison >= 0:
                        spool.close()
                        raise CatalogSemanticValidationError(
                            "active subject facets are not in canonical byte order"
                        )
                    previous[1].close()
                previous = (namespace, spool, byte_count, tag_id)
                expected_position += 1
                after = position
        if expected.execute("SELECT 1 FROM expected_subjects LIMIT 1").fetchone():
            raise CatalogSemanticValidationError(
                "active subject facets omit current membership"
            )
    finally:
        if previous is not None:
            previous[1].close()


def _validate_contributor_facet_order(
    connector: SQLConnector,
    expected: sqlite3.Connection,
    *,
    revision: int,
    cache: _CanonicalValidationCache,
) -> None:
    after = -1
    expected_position = 0
    previous: tuple[bytes, BinaryIO, int, bytes] | None = None
    try:
        while True:
            rows = connector.fetch_all(
                "SELECT position, contributor_name_sha256, role, occurrence_count "
                "FROM catalog_contributor_facet_order WHERE revision = %s "
                "AND position > %s ORDER BY position LIMIT %s",
                (revision, after, _CATALOG_RESOURCE_PAGE_LIMIT),
            )
            if not rows:
                break
            for row in rows:
                if len(row) != 4:
                    raise CatalogSemanticValidationError(
                        "active contributor facet row is malformed"
                    )
                position = _as_int(row[0], field="active contributor facet position")
                if position != expected_position:
                    raise CatalogSemanticValidationError(
                        "active contributor facet positions are not exactly contiguous"
                    )
                digest = _as_bytes(row[1], field="active contributor facet digest")
                role = _as_bytes(row[2], field="active contributor facet role")
                try:
                    require_digest32(digest, field="active contributor facet digest")
                    require_ascii_bytes(
                        role,
                        field="active contributor facet role",
                        minimum=1,
                        maximum=64,
                    )
                except (TypeError, ValueError) as error:
                    raise CatalogSemanticValidationError(
                        "active contributor facet identity is invalid"
                    ) from error
                occurrence_count = _as_int(
                    row[3],
                    field="active contributor facet occurrence_count",
                    positive=True,
                )
                expected_row = expected.execute(
                    "SELECT occurrence_count FROM expected_contributors "
                    "WHERE contributor_name_sha256 = ? AND role = ?",
                    (sqlite3.Binary(digest), sqlite3.Binary(role)),
                ).fetchone()
                if expected_row != (occurrence_count,):
                    raise CatalogSemanticValidationError(
                        "active contributor facet differs from exact membership"
                    )
                expected.execute(
                    "DELETE FROM expected_contributors "
                    "WHERE contributor_name_sha256 = ? AND role = ?",
                    (sqlite3.Binary(digest), sqlite3.Binary(role)),
                )
                spool, byte_count = _validated_canonical_spool(
                    connector,
                    digest,
                    expected_domain=b"contributor_name_utf8_v1",
                    detail="active contributor facet value",
                    cache=cache,
                )
                if previous is not None:
                    if previous[0] < role:
                        comparison = -1
                    elif previous[0] > role:
                        comparison = 1
                    else:
                        comparison = _compare_canonical_spools(previous[1], spool)
                        if comparison == 0:
                            comparison = -1 if previous[3] < digest else 1
                    if comparison >= 0:
                        spool.close()
                        raise CatalogSemanticValidationError(
                            "active contributor facets are not in canonical byte order"
                        )
                    previous[1].close()
                previous = (role, spool, byte_count, digest)
                expected_position += 1
                after = position
        if expected.execute("SELECT 1 FROM expected_contributors LIMIT 1").fetchone():
            raise CatalogSemanticValidationError(
                "active contributor facets omit current membership"
            )
    finally:
        if previous is not None:
            previous[1].close()


def _validate_active_discovery_projection(
    connector: SQLConnector,
    *,
    revision: int,
    display_title_policy_id: int,
    expected_publication_count: int,
) -> None:
    """Audit the sealed current discovery projection after transient cleanup.

    Every database query returns at most one mismatch witness or one fixed-size
    keyset page.  This full READY check may visit the complete current revision;
    the separate epoch-only readiness probe remains O(1).
    """

    try:
        require_search_runtime_policy()
    except RuntimeError as error:
        raise CatalogSemanticValidationError(
            "catalog search runtime policy differs from the sealed index policy"
        ) from error

    seal = _one(
        connector,
        "SELECT policy_id FROM catalog_discovery_seals WHERE revision = %s LIMIT 2",
        (revision,),
        detail="active discovery seal",
    )
    assert seal is not None
    if _as_int(seal[0], field="active discovery policy", positive=True) != (
        SEARCH_POLICY_ID
    ):
        raise CatalogSemanticValidationError(
            "active discovery seal uses an unexpected search policy"
        )

    missing_document = connector.fetch_all(
        "SELECT publication.publication_key "
        "FROM catalog_publications AS publication "
        "LEFT JOIN catalog_search_documents AS document "
        "ON document.revision = publication.revision "
        "AND document.publication_key = publication.publication_key "
        "WHERE publication.revision = %s AND document.publication_key IS NULL "
        "LIMIT 1",
        (revision,),
    )
    extra_document = connector.fetch_all(
        "SELECT document.publication_key "
        "FROM catalog_search_documents AS document "
        "LEFT JOIN catalog_publications AS publication "
        "ON publication.revision = document.revision "
        "AND publication.publication_key = document.publication_key "
        "WHERE document.revision = %s AND publication.publication_key IS NULL "
        "LIMIT 1",
        (revision,),
    )
    if missing_document or extra_document:
        raise CatalogSemanticValidationError(
            "active search documents do not exactly cover current publications"
        )

    cache = _CanonicalValidationCache()
    expected = sqlite3.connect("")
    try:
        expected.executescript("""
            PRAGMA temp_store = FILE;
            CREATE TABLE expected_postings (
                publication_key BLOB NOT NULL,
                value_sha256 BLOB NOT NULL,
                PRIMARY KEY (publication_key, value_sha256)
            ) WITHOUT ROWID;
            CREATE TABLE validated_lexemes (
                value_sha256 BLOB PRIMARY KEY
            ) WITHOUT ROWID;
            CREATE TABLE expected_documents (
                publication_key BLOB PRIMARY KEY,
                row_count INTEGER NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE expected_languages (
                language_sha256 BLOB PRIMARY KEY,
                occurrence_count INTEGER NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE expected_subjects (
                tag_id INTEGER PRIMARY KEY,
                occurrence_count INTEGER NOT NULL
            );
            CREATE TABLE expected_contributors (
                contributor_name_sha256 BLOB NOT NULL,
                role BLOB NOT NULL,
                occurrence_count INTEGER NOT NULL,
                PRIMARY KEY (contributor_name_sha256, role)
            ) WITHOUT ROWID;
            """)
        after_publication = b""
        publication_count = 0
        while True:
            publications = connector.fetch_all(
                "SELECT publication.publication_key, publication.language_sha256, "
                "title.source_title_sha256, title.source_gallery_name, "
                "display.title_sha256 "
                "FROM catalog_publications AS publication "
                "JOIN catalog_publication_titles AS title "
                "ON title.revision = publication.revision "
                "AND title.publication_key = publication.publication_key "
                "JOIN catalog_display_title_choices AS display "
                "ON display.display_title_policy_id = %s "
                "AND display.source_title_sha256 = title.source_title_sha256 "
                "AND display.source_gallery_name = title.source_gallery_name "
                "WHERE publication.revision = %s "
                "AND publication.publication_key > %s "
                "ORDER BY publication.publication_key LIMIT %s",
                (
                    display_title_policy_id,
                    revision,
                    after_publication,
                    _CATALOG_RESOURCE_PAGE_LIMIT,
                ),
            )
            if not publications:
                break
            for publication in publications:
                if len(publication) != 5:
                    raise CatalogSemanticValidationError(
                        "active search source publication is malformed"
                    )
                publication_key = _as_bytes(
                    publication[0], field="active search source publication_key"
                )
                language_sha256 = _as_bytes(
                    publication[1], field="active search source language digest"
                )
                source_title_sha256 = _as_bytes(
                    publication[2], field="active search source title digest"
                )
                source_gallery_name = _as_bytes(
                    publication[3], field="active search source gallery name"
                )
                display_title_sha256 = _as_bytes(
                    publication[4], field="active search display title digest"
                )
                try:
                    require_digest32(
                        publication_key,
                        field="active search source publication_key",
                    )
                    require_digest32(
                        language_sha256,
                        field="active search source language digest",
                    )
                    require_digest32(
                        source_title_sha256,
                        field="active search source title digest",
                    )
                    require_digest32(
                        display_title_sha256,
                        field="active search display title digest",
                    )
                    if not 1 <= len(source_gallery_name) <= 255:
                        raise ValueError("source gallery name is outside 1..255 bytes")
                    source_gallery_name.decode("utf-8", errors="strict")
                except (TypeError, ValueError, UnicodeError) as error:
                    raise CatalogSemanticValidationError(
                        "active search source publication violates bounded domains"
                    ) from error
                _validate_one_publication_search_projection(
                    connector,
                    expected,
                    revision=revision,
                    publication_key=publication_key,
                    language_sha256=language_sha256,
                    source_title_sha256=source_title_sha256,
                    display_title_sha256=display_title_sha256,
                    source_gallery_name=source_gallery_name,
                    cache=cache,
                )
                publication_count += 1
                if publication_count > (1 << 63) - 1:
                    raise CatalogSemanticValidationError(
                        "active search source count exceeds signed-int63"
                    )
                after_publication = publication_key
        if publication_count != expected_publication_count:
            raise CatalogSemanticValidationError(
                "active search source count differs from catalog revision"
            )
        _validate_language_facet_order(
            connector,
            expected,
            revision=revision,
            cache=cache,
        )
        _validate_subject_facet_order(
            connector,
            expected,
            revision=revision,
            cache=cache,
        )
        _validate_contributor_facet_order(
            connector,
            expected,
            revision=revision,
            cache=cache,
        )
    finally:
        expected.close()

    orphan_posting = connector.fetch_all(
        "SELECT posting.publication_key "
        "FROM catalog_search_postings AS posting "
        "LEFT JOIN catalog_search_documents AS document "
        "ON document.revision = posting.revision "
        "AND document.publication_key = posting.publication_key "
        "LEFT JOIN catalog_search_lexemes AS lexeme "
        "ON lexeme.value_sha256 = posting.value_sha256 "
        "WHERE posting.revision = %s "
        "AND (document.publication_key IS NULL OR lexeme.value_sha256 IS NULL) "
        "LIMIT 1",
        (revision,),
    )
    if orphan_posting:
        raise CatalogSemanticValidationError(
            "active search posting lacks document or lexeme authority"
        )


def _prepared_storage_objects(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
    publication_key: bytes,
) -> dict[bytes, StorageObjectDescriptor]:
    rows = connector.fetch_all(
        "SELECT prepared.resource_kind, prepared.storage_object_key_sha256, "
        "prepared.storage_generation, prepared.protection_token, prepared.state, "
        "binding.storage_object_sha256, stored.storage_object_sha256, "
        "stored.size_bytes, stored.modified_at, blob_row.size_bytes "
        "FROM catalog_prepared_artifacts AS prepared "
        "LEFT JOIN catalog_prepared_resource_blob AS binding "
        "ON binding.candidate_id = prepared.candidate_id "
        "AND binding.publication_key = prepared.publication_key "
        "AND binding.resource_kind = prepared.resource_kind "
        "LEFT JOIN catalog_prepared_storage_objects AS stored "
        "ON stored.candidate_id = prepared.candidate_id "
        "AND stored.publication_key = prepared.publication_key "
        "AND stored.resource_kind = prepared.resource_kind "
        "LEFT JOIN catalog_artifact_blobs AS blob_row "
        "ON blob_row.artifact_sha256 = binding.storage_object_sha256 "
        "WHERE prepared.candidate_id = %s AND prepared.publication_key = %s "
        "ORDER BY prepared.resource_kind LIMIT 3",
        (candidate_id, publication_key),
    )
    resources: dict[bytes, StorageObjectDescriptor] = {}
    for row in rows:
        if len(row) != 10 or any(value is None for value in row):
            raise CatalogSemanticValidationError(
                "prepared resource lacks its complete storage/blob authority"
            )
        kind = _as_bytes(row[0], field="prepared resource_kind")
        if kind not in {b"acquisition", b"thumbnail"} or kind in resources:
            raise CatalogSemanticValidationError(
                "prepared resource has an unknown or duplicate resource kind"
            )
        key_digest = _as_bytes(row[1], field="prepared storage key digest")
        generation = _as_int(row[2], field="prepared storage generation")
        token = _as_bytes(row[3], field="prepared protection token")
        if row[4] != "COMMITTED":
            raise CatalogSemanticValidationError(
                "published candidate contains a non-COMMITTED prepared resource"
            )
        binding_digest = _as_bytes(row[5], field="prepared resource blob digest")
        stored_digest = _as_bytes(row[6], field="prepared storage object digest")
        try:
            require_digest32(key_digest, field="prepared storage key digest")
            require_digest32(token, field="prepared protection token")
            require_digest32(binding_digest, field="prepared resource blob digest")
            require_digest32(stored_digest, field="prepared storage object digest")
            if binding_digest != stored_digest:
                raise CatalogSemanticValidationError(
                    "prepared storage object differs from its pre-protection binding"
                )
            expected_token = identity.encode_artifact_protection_token(
                candidate_id,
                publication_key,
                kind.decode("ascii"),
                key_digest,
                generation,
            )
            if token != expected_token:
                raise CatalogSemanticValidationError(
                    "prepared protection token disagrees with durable resource facts"
                )
            size_bytes = _as_int(
                row[7], field="prepared storage object size", positive=True
            )
            if (
                _as_int(row[9], field="prepared resource blob size", positive=True)
                != size_bytes
            ):
                raise CatalogSemanticValidationError(
                    "prepared storage object size differs from its verified blob"
                )
            resources[kind] = StorageObjectDescriptor(
                key=_catalog_storage_key(connector, key_digest),
                size_bytes=size_bytes,
                sha256=stored_digest.hex(),
                modified_at=_catalog_datetime(
                    row[8], field="prepared storage object modified_at"
                ),
            )
        except (TypeError, ValueError, UnicodeError) as error:
            if isinstance(error, CatalogSemanticValidationError):
                raise
            raise CatalogSemanticValidationError(
                "prepared resource violates its public bounded domain"
            ) from error
    return resources


def _validate_prepared_resource_families(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
    expected_publication_count: int,
) -> None:
    orphan = connector.fetch_all(
        "SELECT prepared.publication_key "
        "FROM catalog_prepared_artifacts AS prepared "
        "LEFT JOIN catalog_prepared_artifact_descriptors AS descriptor "
        "ON descriptor.candidate_id = prepared.candidate_id "
        "AND descriptor.publication_key = prepared.publication_key "
        "WHERE prepared.candidate_id = %s "
        "AND descriptor.publication_key IS NULL LIMIT 1",
        (candidate_id,),
    )
    if orphan:
        raise CatalogSemanticValidationError(
            "prepared resource lacks its publication descriptor"
        )

    after = b""
    publication_count = 0
    while True:
        rows = connector.fetch_all(
            "SELECT descriptor.publication_key, identity_row.gid, "
            "descriptor.artifact_sha256, descriptor.artifact_name, "
            "descriptor.media_type, descriptor.page_count, blob_row.size_bytes "
            "FROM catalog_prepared_artifact_descriptors AS descriptor "
            "LEFT JOIN catalog_publication_identities AS identity_row "
            "ON identity_row.publication_key = descriptor.publication_key "
            "LEFT JOIN catalog_artifact_blobs AS blob_row "
            "ON blob_row.artifact_sha256 = descriptor.artifact_sha256 "
            "WHERE descriptor.candidate_id = %s "
            "AND descriptor.publication_key > %s "
            "ORDER BY descriptor.publication_key LIMIT %s",
            (candidate_id, after, _CATALOG_RESOURCE_PAGE_LIMIT),
        )
        if not rows:
            break
        for row in rows:
            if len(row) != 7 or any(value is None for value in row):
                raise CatalogSemanticValidationError(
                    "prepared artifact lacks its complete identity/blob authority"
                )
            publication_key = _as_bytes(
                row[0], field="prepared artifact publication_key"
            )
            try:
                require_digest32(
                    publication_key, field="prepared artifact publication_key"
                )
                gid = _as_int(row[1], field="prepared artifact gid", positive=True)
                if identity.publication_key(gid) != publication_key:
                    raise CatalogSemanticValidationError(
                        "prepared artifact publication key disagrees with its GID"
                    )
                artifact_digest = _as_bytes(row[2], field="prepared artifact digest")
                require_digest32(artifact_digest, field="prepared artifact digest")
                artifact_name = _as_bytes(
                    row[3], field="prepared artifact name"
                ).decode("utf-8", errors="strict")
                media_type = _as_bytes(
                    row[4], field="prepared artifact media_type"
                ).decode("ascii", errors="strict")
                page_count = _as_int(row[5], field="prepared artifact page_count")
                if page_count > 4096:
                    raise CatalogSemanticValidationError(
                        "prepared artifact exceeds the generic page-count bound"
                    )
                artifact_size = _as_int(
                    row[6], field="prepared artifact blob size", positive=True
                )
                resources = _prepared_storage_objects(
                    connector,
                    candidate_id=candidate_id,
                    publication_key=publication_key,
                )
                expected_kinds = (
                    {b"acquisition", b"thumbnail"} if page_count else {b"acquisition"}
                )
                if set(resources) != expected_kinds:
                    raise CatalogSemanticValidationError(
                        "prepared artifact resource roles are incomplete or excessive"
                    )
                acquisition = resources[b"acquisition"]
                if (
                    acquisition.sha256 != artifact_digest.hex()
                    or acquisition.size_bytes != artifact_size
                ):
                    raise CatalogSemanticValidationError(
                        "prepared acquisition differs from artifact byte authority"
                    )
                CatalogArtifact(
                    artifact_id=identity.artifact_id(gid, artifact_digest).decode(
                        "ascii"
                    ),
                    name=artifact_name,
                    storage_object=acquisition,
                    media_type=media_type,
                )
            except (TypeError, ValueError, UnicodeError) as error:
                if isinstance(error, CatalogSemanticValidationError):
                    raise
                raise CatalogSemanticValidationError(
                    "prepared artifact violates its public bounded domain"
                ) from error

            page_rows = connector.fetch_all(
                "SELECT resource_kind, page_index, extent_offset, extent_length, "
                "media_type, image_sha256, width, height "
                "FROM catalog_prepared_pages WHERE candidate_id = %s "
                "AND publication_key = %s ORDER BY page_index LIMIT 4097",
                (candidate_id, publication_key),
            )
            if len(page_rows) != page_count:
                raise CatalogSemanticValidationError(
                    "prepared page coverage differs from artifact page_count"
                )
            for page_index, page_row in enumerate(page_rows):
                _validate_catalog_image_row(
                    page_row,
                    storage_object=acquisition,
                    expected_kind=b"acquisition",
                    expected_index=page_index,
                    detail="prepared page",
                )

            thumbnail_rows = connector.fetch_all(
                "SELECT resource_kind, extent_offset, extent_length, media_type, "
                "image_sha256, width, height FROM catalog_prepared_thumbnails "
                "WHERE candidate_id = %s AND publication_key = %s LIMIT 2",
                (candidate_id, publication_key),
            )
            if len(thumbnail_rows) != (1 if page_count else 0):
                raise CatalogSemanticValidationError(
                    "prepared thumbnail totality differs from artifact page_count"
                )
            if thumbnail_rows:
                thumbnail_object = resources[b"thumbnail"]
                thumbnail = _validate_catalog_image_row(
                    thumbnail_rows[0],
                    storage_object=thumbnail_object,
                    expected_kind=b"thumbnail",
                    expected_index=None,
                    detail="prepared thumbnail",
                )
                if (
                    thumbnail.extent.offset != 0
                    or thumbnail.extent.length != thumbnail_object.size_bytes
                    or thumbnail.sha256 != thumbnail_object.sha256
                ):
                    raise CatalogSemanticValidationError(
                        "prepared thumbnail is not its complete sealed object"
                    )
            publication_count += 1
            if publication_count > (1 << 63) - 1:
                raise CatalogSemanticValidationError(
                    "prepared artifact count exceeds signed-int63"
                )
        after = _as_bytes(rows[-1][0], field="prepared artifact keyset cursor")
    if publication_count != expected_publication_count:
        raise CatalogSemanticValidationError(
            "prepared artifact coverage differs from its terminal publication count"
        )


def _active_publication_contexts(
    connector: SQLConnector,
) -> tuple[_PublicationContext, ...]:
    rows = connector.fetch_all("""
        SELECT registry.channel, head.revision, head.generation,
               head.committed_at
        FROM catalog_channel_registry AS registry
        LEFT JOIN catalog_publication_commit_heads AS head
          ON head.channel = registry.channel
        ORDER BY registry.channel
        LIMIT 2
        """)
    if len(rows) != 1:
        raise CatalogSemanticValidationError(
            "publication_head exceeds the channel registry"
        )
    if all(value is None for value in rows[0][1:]):
        return ()
    if any(value is None for value in rows[0][1:]):
        raise CatalogSemanticValidationError(
            "publication_head projection is incomplete"
        )
    contexts: list[_PublicationContext] = []
    for head_row in rows:
        channel = _as_bytes(head_row[0], field="publication_head.channel")
        revision = _as_int(
            head_row[1], field="publication_head.revision", positive=True
        )
        generation = _as_int(
            head_row[2], field="publication_head.generation", positive=True
        )
        _as_int(head_row[3], field="publication_head.advanced_at")
        if channel != b"default":
            raise CatalogSemanticValidationError(
                "publication_head uses an unknown channel"
            )
        revision_row = _one(
            connector,
            """
            SELECT publication_count, artifact_count
            FROM catalog_revisions
            WHERE revision = %s
            LIMIT 2
            """,
            (revision,),
            detail="active catalog revision",
        )
        assert revision_row is not None
        publication_count = _as_int(
            revision_row[0], field="catalog_revision.publication_count"
        )
        artifact_count = _as_int(
            revision_row[1], field="catalog_revision.artifact_count"
        )
        if artifact_count not in {0, publication_count}:
            raise CatalogSemanticValidationError(
                "catalog revision artifact_count is neither zero nor publication_count"
            )
        _validate_catalog_occurrence_storage(
            connector,
            revision=revision,
        )
        _validate_active_catalog_resources(
            connector,
            revision=revision,
            expected_artifact_count=artifact_count,
        )
        receipt_row = _one(
            connector,
            """
            SELECT receipt_id, source_revision, channel, artifact_policy_id,
                   display_title_policy_id,
                   publication_count, new_galleries, changed_galleries,
                   removed_galleries, duplicate_losers, state
            FROM catalog_publication_receipts
            WHERE revision = %s
            LIMIT 2
            """,
            (revision,),
            detail="active publication receipt",
        )
        assert receipt_row is not None
        receipt_id = _as_bytes(receipt_row[0], field="publication receipt_id")
        if len(receipt_id) != 16:
            raise CatalogSemanticValidationError(
                "active publication receipt_id is not 16 bytes"
            )
        source_revision = _as_int(
            receipt_row[1], field="publication receipt source_revision", positive=True
        )
        receipt_channel = _as_bytes(receipt_row[2], field="publication receipt channel")
        artifact_policy_id = _as_int(
            receipt_row[3],
            field="publication receipt artifact_policy_id",
            positive=True,
        )
        display_title_policy_id = _as_int(
            receipt_row[4],
            field="publication receipt display_title_policy_id",
            positive=True,
        )
        receipt_publication_count = _as_int(
            receipt_row[5], field="publication receipt publication_count"
        )
        if receipt_channel != channel:
            raise CatalogSemanticValidationError(
                "active publication receipt does not match its head attempt"
            )
        if receipt_publication_count != publication_count:
            raise CatalogSemanticValidationError(
                "publication receipt and catalog revision publication_count differ"
            )
        receipt_result_scalars = tuple(
            _as_int(receipt_row[index], field=f"publication receipt {name}")
            for index, name in enumerate(
                (
                    "new_galleries",
                    "changed_galleries",
                    "removed_galleries",
                    "duplicate_losers",
                ),
                start=6,
            )
        )
        receipt_state = receipt_row[10]
        if receipt_state != "PUBLISHED":
            raise CatalogSemanticValidationError(
                "active publication receipt is not PUBLISHED"
            )

        commit_candidate_row = _one(
            connector,
            """
            SELECT candidate_id
            FROM catalog_publication_commits
            WHERE receipt_id = %s
            LIMIT 2
            """,
            (receipt_id,),
            detail="active permanent publication candidate mapping",
        )
        assert commit_candidate_row is not None
        candidate_id = _as_bytes(
            commit_candidate_row[0], field="permanent publication candidate_id"
        )
        if len(candidate_id) != 16:
            raise CatalogSemanticValidationError(
                "permanent publication candidate_id is not 16 bytes"
            )
        consumed_candidate_base = _one(
            connector,
            """
            SELECT base_receipt_id
            FROM catalog_publication_candidate_base_publication_commits
            WHERE candidate_id = %s
            LIMIT 2
            """,
            (candidate_id,),
            detail="active consumed publication candidate base authority",
            optional=True,
        )
        if consumed_candidate_base is not None:
            raise CatalogSemanticValidationError(
                "PUBLISHED publication retained its consumed candidate base authority"
            )

        candidate_row = _one(
            connector,
            """
            SELECT candidate_id, analysis_id, artifact_policy_id,
                   display_title_policy_id
            FROM catalog_publication_candidates
            WHERE candidate_id = %s
            LIMIT 2
            """,
            (candidate_id,),
            detail="active publication candidate",
            optional=True,
        )
        if candidate_row is None:
            _validate_active_publication_policies(
                connector,
                artifact_policy_id=artifact_policy_id,
                display_title_policy_id=display_title_policy_id,
            )
            contexts.append(
                _PublicationContext(
                    channel,
                    revision,
                    generation,
                    publication_count,
                    candidate_id,
                    None,
                    source_revision,
                    artifact_policy_id,
                    display_title_policy_id,
                )
            )
            continue
        assert candidate_row is not None
        transient_candidate_id = _as_bytes(
            candidate_row[0], field="publication candidate_id"
        )
        analysis_id = _as_bytes(candidate_row[1], field="publication analysis_id")
        if transient_candidate_id != candidate_id or len(analysis_id) != 16:
            raise CatalogSemanticValidationError(
                "active publication candidate identities are not 16 bytes"
            )
        if (
            _as_int(candidate_row[2], field="candidate artifact policy", positive=True)
            != artifact_policy_id
            or _as_int(
                candidate_row[3], field="candidate display policy", positive=True
            )
            != display_title_policy_id
        ):
            raise CatalogSemanticValidationError(
                "active publication candidate and immutable receipt disagree"
            )
        projection_seal_row = _one(
            connector,
            """
            SELECT candidate_id
            FROM catalog_publication_candidate_projection_seals
            WHERE candidate_id = %s
            LIMIT 2
            """,
            (candidate_id,),
            detail="active publication candidate projection seal",
        )
        assert projection_seal_row is not None
        if (
            _as_bytes(
                projection_seal_row[0], field="publication projection certification"
            )
            != candidate_id
        ):
            raise CatalogSemanticValidationError(
                "publication projection certification key disagrees with its candidate"
            )
        stage_counts = {
            stage: _publication_terminal_stage_count(
                connector, candidate_id=candidate_id, stage=stage
            )
            for stage in (
                b"VALIDATE_SELECTION",
                b"VALIDATE_ARTIFACT_INPUT_DELTA",
                b"VALIDATE_PREPARED_ARTIFACT",
                b"VALIDATE_CREATE",
                b"VALIDATE_REBUILD",
                b"VALIDATE_DELETE",
                b"VALIDATE_UNCHANGED",
                b"VALIDATE_NEW_GALLERY",
                b"VALIDATE_CHANGED_GALLERY",
                b"VALIDATE_REMOVED_GALLERY",
                b"VALIDATE_DUPLICATE_LOSER",
            )
        }
        if stage_counts[b"VALIDATE_SELECTION"] != publication_count:
            raise CatalogSemanticValidationError(
                "terminal selection receipt and catalog publication_count differ"
            )
        create_count = stage_counts[b"VALIDATE_CREATE"]
        rebuild_count = stage_counts[b"VALIDATE_REBUILD"]
        delete_count = stage_counts[b"VALIDATE_DELETE"]
        unchanged_count = stage_counts[b"VALIDATE_UNCHANGED"]
        prepared_count = stage_counts[b"VALIDATE_PREPARED_ARTIFACT"]
        artifact_input_count = stage_counts[b"VALIDATE_ARTIFACT_INPUT_DELTA"]
        if prepared_count != create_count + rebuild_count:
            raise CatalogSemanticValidationError(
                "terminal prepared-artifact count differs from CREATE plus REBUILD"
            )
        _validate_prepared_resource_families(
            connector,
            candidate_id=candidate_id,
            expected_publication_count=prepared_count,
        )
        if artifact_input_count != create_count + rebuild_count + unchanged_count:
            raise CatalogSemanticValidationError(
                "terminal artifact-input count differs from CREATE plus REBUILD "
                "plus UNCHANGED"
            )
        result_counts = (
            stage_counts[b"VALIDATE_NEW_GALLERY"],
            stage_counts[b"VALIDATE_CHANGED_GALLERY"],
            stage_counts[b"VALIDATE_REMOVED_GALLERY"],
            stage_counts[b"VALIDATE_DUPLICATE_LOSER"],
        )
        if result_counts != receipt_result_scalars:
            raise CatalogSemanticValidationError(
                "terminal publication-result receipts and permanent commit differ"
            )
        projection_row = _one(
            connector,
            """
            SELECT create_count, rebuild_count, delete_count,
                   new_galleries, changed_galleries
            FROM catalog_publication_candidate_projections
            WHERE candidate_id = %s
            LIMIT 2
            """,
            (candidate_id,),
            detail="derived publication candidate projection",
        )
        assert projection_row is not None
        if tuple(
            _as_int(value, field="derived publication projection count")
            for value in projection_row
        ) != (
            create_count,
            rebuild_count,
            delete_count,
            result_counts[0],
            result_counts[1],
        ):
            raise CatalogSemanticValidationError(
                "derived projection does not equal its fixed current terminal receipts"
            )
        final_checkpoint = _one(
            connector,
            """
            SELECT processed_count, state
            FROM catalog_publication_finalization_checkpoints
            WHERE receipt_id = %s
            LIMIT 2
            """,
            (receipt_id,),
            detail="permanent artifact finalization checkpoint",
        )
        assert final_checkpoint is not None
        _as_int(final_checkpoint[0], field="permanent finalization processed_count")
        if final_checkpoint[1] != "COMPLETE":
            raise CatalogSemanticValidationError(
                "published commit lacks a COMPLETE resource finalization checkpoint"
            )
        _validate_analysis_seal(connector, analysis_id)
        _validate_publication_candidate_source(
            connector,
            candidate_id=candidate_id,
            analysis_id=analysis_id,
            source_revision=source_revision,
            channel=channel,
        )

        _validate_active_publication_policies(
            connector,
            artifact_policy_id=artifact_policy_id,
            display_title_policy_id=display_title_policy_id,
        )

        contexts.append(
            _PublicationContext(
                channel,
                revision,
                generation,
                publication_count,
                candidate_id,
                analysis_id,
                source_revision,
                artifact_policy_id,
                display_title_policy_id,
            )
        )
    return tuple(contexts)


@dataclass(frozen=True, slots=True)
class _OpenPublicationGenerationTransition:
    """Exact bounded rows absent under one validated OPEN PG cleanup."""

    canonical_floor: int
    missing_nodes: frozenset[int]
    missing_edge_successors: frozenset[int]


def _decode_publication_generation_cursor(
    cursor: bytes,
) -> tuple[int, int, int]:
    """Decode one canonical static cursor containing two uint63 keys."""

    if (
        len(cursor) != 22
        or cursor[:1] != b"\x01"
        or cursor[3:5] != b"\x02i"
        or cursor[13:14] != b"i"
    ):
        raise CatalogSemanticValidationError(
            "OPEN publication-generation cleanup cursor is malformed"
        )
    relation_index = int.from_bytes(cursor[1:3], "big")
    root_generation = int.from_bytes(cursor[5:13], "big")
    primary_generation = int.from_bytes(cursor[14:22], "big")
    _as_int(root_generation, field="publication-generation cursor root")
    _as_int(primary_generation, field="publication-generation cursor primary key")
    return relation_index, root_generation, primary_generation


def _validated_open_publication_generation_transition(
    connector: SQLConnector,
    *,
    retained_commit_floor: int | None,
    retained_commit_tip: int | None,
) -> _OpenPublicationGenerationTransition | None:
    """Load the exact bounded PG transition that may break the visible chain."""

    rows = connector.fetch_all(
        """
        SELECT root.frozen_root_key, sweep.shard_no, checkpoint.phase,
               checkpoint.cursor_bytes, phase.phase_order
        FROM operational_cleanup_jobs AS job
        JOIN operational_cleanup_sweep_targets AS sweep
          ON sweep.target_key = job.target_key
        JOIN operational_cleanup_checkpoints AS checkpoint
          ON checkpoint.cleanup_id = job.cleanup_id
        JOIN operational_cleanup_phases AS phase
          ON phase.target_kind = sweep.target_kind
         AND phase.phase = checkpoint.phase
        LEFT JOIN operational_cleanup_cycle_roots AS root
          ON root.cleanup_id = job.cleanup_id
        WHERE job.state = 'OPEN'
          AND sweep.target_kind = 'PUBLICATION_GENERATION'
          AND checkpoint.state = 'OPEN'
        ORDER BY root.frozen_root_key
        LIMIT 257
        """
    )
    if not rows:
        return None
    if len(rows) > 256:
        raise CatalogSemanticValidationError(
            "OPEN publication-generation cleanup roots exceed the hard cap"
        )

    from . import operational_refinement

    try:
        operational_refinement.check_cleanup_reachability_v1(connector)
    except (
        operational_refinement.OperationalSemanticRegistryError,
        operational_refinement.OperationalSemanticValidationError,
    ) as error:
        raise CatalogSemanticValidationError(
            "publication-generation gap lacks valid OPEN cleanup authority"
        ) from error

    shard_no = _as_int(rows[0][1], field="publication-generation cleanup shard")
    if not 0 <= shard_no <= 255:
        raise CatalogSemanticValidationError(
            "OPEN publication-generation cleanup shard is outside 0..255"
        )
    phase = rows[0][2]
    cursor = _as_bytes(rows[0][3], field="publication-generation cleanup cursor")
    phase_order = _as_int(
        rows[0][4], field="publication-generation cleanup phase order", positive=True
    )
    if any(row[1:] != rows[0][1:] for row in rows[1:]):
        raise CatalogSemanticValidationError(
            "OPEN publication-generation roots disagree on their checkpoint"
        )
    if (phase, phase_order) not in {("PG_EDGE", 1), ("PG_ROOT", 2)}:
        raise CatalogSemanticValidationError(
            "OPEN publication-generation phase/order disagrees"
        )

    frozen: list[int] = []
    for row in rows:
        if row[0] is None:
            if len(rows) != 1:
                raise CatalogSemanticValidationError(
                    "OPEN publication-generation NULL root is ambiguous"
                )
            continue
        frame = _as_bytes(row[0], field="OPEN publication-generation frozen root")
        if len(frame) != 11 or frame[:3] != b"\x01\x01i":
            raise CatalogSemanticValidationError(
                "OPEN publication-generation frozen root frame is malformed"
            )
        frozen.append(
            _as_int(
                int.from_bytes(frame[3:11], "big"),
                field="OPEN publication-generation frozen root",
            )
        )
    if not frozen:
        if cursor:
            raise CatalogSemanticValidationError(
                "empty publication-generation cleanup advanced a cursor"
            )
        return None
    if len(set(frozen)) != len(frozen) or any(
        right != left + 1 for left, right in zip(frozen, frozen[1:], strict=False)
    ):
        raise CatalogSemanticValidationError(
            "OPEN publication-generation roots are not one contiguous prefix"
        )
    if shard_no != frozen[0] % 256:
        raise CatalogSemanticValidationError(
            "OPEN publication-generation cleanup slot does not match its frozen "
            "prefix floor"
        )
    if (
        retained_commit_floor is None
        or retained_commit_tip is None
        or retained_commit_floor <= 1
        or frozen[-1] >= retained_commit_floor
    ):
        raise CatalogSemanticValidationError(
            "OPEN publication-generation roots cross retained commit authority"
        )

    internal_edges = frozenset(frozen[1:])
    boundary_successor = frozen[-1] + 1
    if phase == "PG_EDGE":
        if not cursor:
            return _OpenPublicationGenerationTransition(
                frozen[0], frozenset(), frozenset()
            )
        index, root_generation, primary_generation = (
            _decode_publication_generation_cursor(cursor)
        )
        if index == 0:
            if (
                root_generation != primary_generation
                or root_generation not in frozen[1:]
            ):
                raise CatalogSemanticValidationError(
                    "OPEN PG_EDGE cursor is outside its internal frozen edges"
                )
            missing_edges = frozenset(
                generation for generation in frozen[1:] if generation <= root_generation
            )
        elif index == 1:
            if (
                root_generation != frozen[-1]
                or primary_generation != boundary_successor
            ):
                raise CatalogSemanticValidationError(
                    "OPEN PG_EDGE boundary cursor is outside its frozen prefix"
                )
            missing_edges = internal_edges | {boundary_successor}
        else:
            raise CatalogSemanticValidationError(
                "OPEN PG_EDGE cursor names an unknown relation"
            )
        return _OpenPublicationGenerationTransition(
            frozen[0], frozenset(), missing_edges
        )

    missing_edges = internal_edges | {boundary_successor}
    if not cursor:
        missing_nodes: frozenset[int] = frozenset()
    else:
        index, root_generation, primary_generation = (
            _decode_publication_generation_cursor(cursor)
        )
        if (
            index != 0
            or root_generation != primary_generation
            or root_generation not in frozen
        ):
            raise CatalogSemanticValidationError(
                "OPEN PG_ROOT cursor is outside its frozen prefix"
            )
        missing_nodes = frozenset(
            generation for generation in frozen if generation <= root_generation
        )
    return _OpenPublicationGenerationTransition(frozen[0], missing_nodes, missing_edges)


def _validate_publication_generation_nodes(
    connector: SQLConnector,
    *,
    floor: int | None,
    tip: int | None,
    transition: _OpenPublicationGenerationTransition | None = None,
) -> int:
    """Stream one contiguous retained interval and return its actual floor.

    Publication-commit cleanup and generation-prefix cleanup are distinct
    bounded transactions.  A crash between them may therefore leave a
    structurally valid, unreferenced prefix below the oldest retained commit.
    That conservative residue grants no publication authority and the next
    generation cleanup can remove it.  The interval must still be exact from
    its actual floor through the commit tip; gaps and nodes above the tip are
    corruption.
    """

    expected_tip = 0 if tip is None else tip
    actual_floor: int | None = None
    expected: int | None = None
    missing = frozenset() if transition is None else transition.missing_nodes
    after = -1
    while True:
        rows = connector.fetch_all(
            "SELECT generation FROM catalog_publication_generation_nodes "
            "WHERE generation > %s ORDER BY generation LIMIT %s",
            (after, _CATALOG_RESOURCE_PAGE_LIMIT),
        )
        for row in rows:
            generation = _as_int(row[0], field="publication generation node")
            if actual_floor is None:
                actual_floor = generation
                if transition is not None:
                    expected = transition.canonical_floor
                else:
                    expected = generation
                    if floor is None:
                        valid_floor = generation == 0
                    elif floor == 1:
                        # Generation zero cannot become cleanup-eligible while the
                        # genesis publication commit remains retained.
                        valid_floor = generation == 0
                    else:
                        valid_floor = generation <= floor
                    if not valid_floor:
                        raise CatalogSemanticValidationError(
                            "publication generation nodes differ from the retained "
                            "compacted window: the interval starts after its oldest "
                            "retained commit"
                        )
            assert expected is not None
            while expected in missing:
                expected += 1
            if generation != expected:
                raise CatalogSemanticValidationError(
                    "publication generation nodes differ from the retained "
                    "compacted window"
                )
            expected += 1
            after = generation
        if len(rows) < _CATALOG_RESOURCE_PAGE_LIMIT:
            break
    while expected is not None and expected in missing:
        expected += 1
    if actual_floor is None or expected != expected_tip + 1:
        raise CatalogSemanticValidationError(
            "publication generation nodes differ from the retained compacted window"
        )
    return transition.canonical_floor if transition is not None else actual_floor


def _validate_publication_generation_edges(
    connector: SQLConnector,
    *,
    floor: int | None,
    tip: int | None,
    transition: _OpenPublicationGenerationTransition | None = None,
) -> None:
    """Stream the unique adjacency chain with a fixed-size keyset page."""

    if floor is None or tip is None or floor == tip:
        expected_successor: int | None = None
    else:
        expected_successor = floor + 1
    missing = frozenset() if transition is None else transition.missing_edge_successors
    after = -1
    while True:
        rows = connector.fetch_all(
            "SELECT successor_generation, predecessor_generation "
            "FROM catalog_publication_generation_successors "
            "WHERE successor_generation > %s "
            "ORDER BY successor_generation LIMIT %s",
            (after, _CATALOG_RESOURCE_PAGE_LIMIT),
        )
        for row in rows:
            while expected_successor in missing:
                assert expected_successor is not None
                expected_successor += 1
            successor = _as_int(
                row[0], field="publication successor generation", positive=True
            )
            predecessor = _as_int(row[1], field="publication predecessor generation")
            if (
                expected_successor is None
                or successor != expected_successor
                or predecessor != successor - 1
            ):
                raise CatalogSemanticValidationError(
                    "publication generation successor chain is gapped, forked, or "
                    "crosses the compacted floor"
                )
            assert expected_successor is not None
            expected_successor += 1
            after = successor
        if len(rows) < _CATALOG_RESOURCE_PAGE_LIMIT:
            break
    if expected_successor is not None:
        assert tip is not None
        while expected_successor in missing:
            expected_successor += 1
        if expected_successor != tip + 1:
            raise CatalogSemanticValidationError(
                "publication generation successor chain is gapped, forked, or "
                "crosses the compacted floor"
            )


def _validate_publication_generation_history(connector: SQLConnector) -> None:
    """Validate the compactable retained window in fixed-size keyset pages."""

    orphan_anchor_rows = connector.fetch_all(
        "SELECT anchor.receipt_id FROM catalog_publication_commit_anchors AS anchor "
        "LEFT JOIN catalog_publication_commits AS committed "
        "ON committed.receipt_id = anchor.receipt_id "
        "WHERE committed.receipt_id IS NULL "
        "ORDER BY anchor.receipt_id LIMIT 257"
    )
    missing_anchor_rows = connector.fetch_all(
        "SELECT 1 FROM catalog_publication_commits AS committed "
        "LEFT JOIN catalog_publication_commit_anchors AS anchor "
        "ON anchor.receipt_id = committed.receipt_id "
        "WHERE anchor.receipt_id IS NULL LIMIT 1"
    )
    if missing_anchor_rows:
        raise CatalogSemanticValidationError(
            "publication anchors differ from complete retained common commits"
        )
    if len(orphan_anchor_rows) > 256:
        raise CatalogSemanticValidationError(
            "publication anchor retirement exceeds its bounded frozen root set"
        )
    actual_orphans = frozenset(
        _as_bytes(row[0], field="retiring publication anchor receipt_id")
        for row in orphan_anchor_rows
    )
    if any(len(receipt_id) != 16 for receipt_id in actual_orphans):
        raise CatalogSemanticValidationError(
            "retiring publication anchor receipt_id is not 16 bytes"
        )
    pcom_transitions: dict[bytes, _OpenPcomTransition] | None = None
    has_open_compound_retirement = _has_open_pcom_compound_transition(connector)
    if actual_orphans and not has_open_compound_retirement:
        raise CatalogSemanticValidationError(
            "publication anchors differ from complete retained common commits"
        )
    if has_open_compound_retirement:
        pcom_transitions = _validated_open_pcom_transitions(connector)
        _retired, expected_orphans = _require_open_pcom_compound_retirement(
            connector,
            transitions=pcom_transitions,
        )
        if actual_orphans != expected_orphans:
            raise CatalogSemanticValidationError(
                "publication orphan anchors differ from their exact OPEN PCOM "
                "cursor authority"
            )

    heads = connector.fetch_all(
        "SELECT registry.channel, head.receipt_id, commit_row.generation, "
        "source_revision.channel "
        "FROM catalog_channel_registry AS registry "
        "LEFT JOIN catalog_publication_commit_head_receipts AS head "
        "ON head.channel = registry.channel "
        "LEFT JOIN catalog_publication_commits AS commit_row "
        "ON commit_row.receipt_id = head.receipt_id "
        "LEFT JOIN catalog_source_revisions AS source_revision "
        "ON source_revision.source_revision = commit_row.source_revision "
        "ORDER BY registry.channel LIMIT 2"
    )
    if len(heads) != 1 or _as_bytes(heads[0][0], field="head channel") != b"default":
        raise CatalogSemanticValidationError(
            "common publication head exceeds the channel registry"
        )
    if any(value is None for value in heads[0][1:]):
        if not all(value is None for value in heads[0][1:]):
            raise CatalogSemanticValidationError(
                "common publication head family is incomplete"
            )
        head_receipt: bytes | None = None
        head_generation: int | None = None
    else:
        head_receipt = _as_bytes(heads[0][1], field="head receipt_id")
        head_generation = _as_int(heads[0][2], field="head generation", positive=True)
        head_source_channel = _as_bytes(heads[0][3], field="head source channel")
        if len(head_receipt) != 16 or head_source_channel != b"default":
            raise CatalogSemanticValidationError(
                "common publication head does not name a retained PUBLISHED commit"
            )

    commit_count = 0
    floor: int | None = None
    tip: int | None = None
    tip_receipt: bytes | None = None
    tip_candidate: bytes | None = None
    tip_is_published = False
    previous_generation: int | None = None
    head_seen = False
    head_is_published = False
    last_published_receipt: bytes | None = None
    unfinished_count = 0
    after_generation = -1
    while True:
        rows = connector.fetch_all(
            "SELECT committed.receipt_id, committed.candidate_id, "
            "committed.revision, committed.source_revision, committed.generation, "
            "committed.committed_at, committed.preparation_id, base.base_receipt_id, "
            "checkpoint.generation, checkpoint.`cursor`, checkpoint.processed_count, "
            "checkpoint.state, checkpoint.updated_at, marker.receipt_id, "
            "latest.start_cursor, latest.start_processed_count, "
            "latest.next_cursor, latest.next_processed_count, latest.next_state, "
            "latest.row_count, latest.terminal, latest.committed_generation, "
            "latest.committed_at "
            "FROM catalog_publication_commits AS committed "
            "LEFT JOIN catalog_publication_candidate_base_publication_commits AS base "
            "ON base.candidate_id = committed.candidate_id "
            "LEFT JOIN catalog_publication_finalization_checkpoints AS checkpoint "
            "ON checkpoint.receipt_id = committed.receipt_id "
            "LEFT JOIN catalog_publication_commit_finalizations AS marker "
            "ON marker.receipt_id = committed.receipt_id "
            "LEFT JOIN catalog_publication_finalization_batch_receipts AS latest "
            "ON latest.receipt_id = committed.receipt_id "
            "AND latest.committed_generation = checkpoint.generation "
            "WHERE committed.generation > %s "
            "ORDER BY committed.generation LIMIT %s",
            (after_generation, _CATALOG_RESOURCE_PAGE_LIMIT),
        )
        for row in rows:
            receipt_id = _as_bytes(row[0], field="publication chain receipt_id")
            candidate_id = _as_bytes(row[1], field="publication chain candidate_id")
            if len(receipt_id) != 16 or len(candidate_id) != 16:
                raise CatalogSemanticValidationError(
                    "publication chain UUID identity is not 16 bytes"
                )
            _as_int(row[2], field="publication chain revision", positive=True)
            _as_int(row[3], field="publication chain source revision", positive=True)
            publication_generation = _as_int(
                row[4], field="publication chain generation", positive=True
            )
            committed_at = _as_int(row[5], field="publication chain committed_at")
            if (
                previous_generation is not None
                and publication_generation <= previous_generation
            ):
                raise CatalogSemanticValidationError(
                    "retained publication commit generations are not strictly increasing"
                )
            previous_generation = publication_generation
            after_generation = publication_generation
            floor = publication_generation if floor is None else floor
            tip = publication_generation
            commit_count += 1

            preparation_id = _as_bytes(row[6], field="publication chain preparation_id")
            if len(preparation_id) != 16:
                raise CatalogSemanticValidationError(
                    "publication chain preparation_id is not 16 bytes"
                )
            candidate_base = (
                None
                if row[7] is None
                else _as_bytes(
                    row[7], field="publication chain candidate base receipt_id"
                )
            )
            if candidate_base is not None and len(candidate_base) != 16:
                raise CatalogSemanticValidationError(
                    "publication chain candidate base receipt_id is not 16 bytes"
                )
            if any(value is None for value in row[8:13]):
                raise CatalogSemanticValidationError(
                    "retained publication commits and permanent finalization "
                    "checkpoints differ"
                )
            checkpoint_generation = _as_int(
                row[8], field="finalization checkpoint generation", positive=True
            )
            cursor = _as_bytes(row[9], field="finalization checkpoint cursor")
            processed_count = _as_int(
                row[10], field="finalization checkpoint processed_count"
            )
            state = row[11]
            updated_at = _as_int(row[12], field="finalization checkpoint updated_at")
            marker_present = row[13] is not None
            if updated_at < committed_at or state not in {"OPEN", "COMPLETE"}:
                raise CatalogSemanticValidationError(
                    "permanent finalization checkpoint precedes its commit or has "
                    "an invalid state"
                )
            latest_values = row[14:23]
            if all(value is None for value in latest_values):
                latest: tuple[Any, ...] | None = None
            elif any(value is None for value in latest_values):
                raise CatalogSemanticValidationError(
                    "latest permanent finalization receipt is incomplete"
                )
            else:
                latest = tuple(latest_values)
            retiring_finalization = state == "COMPLETE" and not marker_present
            if retiring_finalization:
                if pcom_transitions is None:
                    pcom_transitions = _validated_open_pcom_transitions(connector)
                _require_open_pcom_finalization_retirement(
                    connector,
                    receipt_id=receipt_id,
                    preparation_id=preparation_id,
                    checkpoint_generation=checkpoint_generation,
                    latest_present=latest is not None,
                    transitions=pcom_transitions,
                )
            published = False
            if checkpoint_generation == 1:
                if latest is not None or state != "OPEN" or marker_present:
                    raise CatalogSemanticValidationError(
                        "initial permanent finalization checkpoint is not exact OPEN "
                        "genesis"
                    )
            else:
                if latest is None:
                    if not retiring_finalization:
                        raise CatalogSemanticValidationError(
                            "advanced permanent finalization checkpoint lacks its "
                            "exact receipt"
                        )
                    published = True
                else:
                    start_cursor = _as_bytes(
                        latest[0], field="finalization start_cursor"
                    )
                    start_count = _as_int(
                        latest[1], field="finalization start_processed_count"
                    )
                    next_cursor = _as_bytes(latest[2], field="finalization next_cursor")
                    next_count = _as_int(
                        latest[3], field="finalization next_processed_count"
                    )
                    next_state = latest[4]
                    row_count = _as_int(latest[5], field="finalization row_count")
                    terminal = _as_int(latest[6], field="finalization terminal")
                    committed_generation = _as_int(
                        latest[7],
                        field="finalization committed_generation",
                        positive=True,
                    )
                    receipt_time = _as_int(latest[8], field="finalization committed_at")
                    if (
                        next_cursor != cursor
                        or next_count != processed_count
                        or committed_generation != checkpoint_generation
                        or receipt_time != updated_at
                    ):
                        raise CatalogSemanticValidationError(
                            "latest permanent finalization receipt and checkpoint are "
                            "incongruent"
                        )
                    if state == "OPEN":
                        if (
                            marker_present
                            or terminal != 0
                            or row_count <= 0
                            or next_state != "OPEN"
                            or next_count != start_count + row_count
                        ):
                            raise CatalogSemanticValidationError(
                                "OPEN finalization checkpoint has marker or "
                                "nonterminal receipt drift"
                            )
                    elif (
                        (not marker_present and not retiring_finalization)
                        or terminal != 1
                        or row_count != 0
                        or next_state != "COMPLETE"
                        or start_cursor != next_cursor
                        or start_count != next_count
                    ):
                        raise CatalogSemanticValidationError(
                            "COMPLETE finalization checkpoint lacks its exact terminal "
                            "marker/receipt"
                        )
                    else:
                        published = True
            if published and candidate_base is not None:
                raise CatalogSemanticValidationError(
                    "PUBLISHED publication retained its consumed candidate base "
                    "authority"
                )
            if not published:
                unfinished_count += 1
            elif unfinished_count:
                raise CatalogSemanticValidationError(
                    "a retained publication before the chain tip is not PUBLISHED"
                )
            else:
                last_published_receipt = receipt_id
            if head_receipt == receipt_id:
                head_seen = True
                head_is_published = published
                if head_generation != publication_generation:
                    raise CatalogSemanticValidationError(
                        "common publication head is not an exact PUBLISHED generation"
                    )
            tip_receipt = receipt_id
            tip_candidate = candidate_id
            tip_is_published = published
        if len(rows) < _CATALOG_RESOURCE_PAGE_LIMIT:
            break

    generation_transition = _validated_open_publication_generation_transition(
        connector,
        retained_commit_floor=floor,
        retained_commit_tip=tip,
    )
    retained_node_floor = _validate_publication_generation_nodes(
        connector,
        floor=floor,
        tip=tip,
        transition=generation_transition,
    )
    _validate_publication_generation_edges(
        connector,
        floor=retained_node_floor,
        tip=tip,
        transition=generation_transition,
    )

    if commit_count == 0:
        if head_receipt is not None:
            raise CatalogSemanticValidationError(
                "genesis publication catalog unexpectedly has a common head"
            )
        return
    assert tip is not None and tip_receipt is not None and tip_candidate is not None
    if head_receipt is None:
        if commit_count != 1 or floor != 1 or tip != 1 or tip_is_published:
            raise CatalogSemanticValidationError(
                "only one reader-invisible DB_COMMITTED genesis may precede a head"
            )
        _validate_invisible_publication_working_roots(
            connector,
            candidate_id=tip_candidate,
        )
        return
    if not head_seen:
        raise CatalogSemanticValidationError(
            "common publication head does not name a retained PUBLISHED commit"
        )
    assert head_generation is not None
    if not head_is_published:
        raise CatalogSemanticValidationError(
            "common publication head is not an exact PUBLISHED generation"
        )
    if tip_receipt == head_receipt:
        if not tip_is_published:
            raise CatalogSemanticValidationError(
                "common publication head points at an unfinished commit"
            )
    elif unfinished_count == 1 and not tip_is_published:
        if tip != head_generation + 1 or last_published_receipt != head_receipt:
            raise CatalogSemanticValidationError(
                "reader-invisible DB_COMMITTED commit is not the head's exact successor"
            )
        _validate_invisible_publication_working_roots(
            connector,
            candidate_id=tip_candidate,
        )
    else:
        raise CatalogSemanticValidationError(
            "publication history has more than one commit beyond its common head"
        )


def _validate_invisible_publication_working_roots(
    connector: SQLConnector,
    *,
    candidate_id: bytes,
) -> None:
    """Require the exact roots that make one invisible successor recoverable."""

    row = _one(
        connector,
        """
        SELECT candidate.candidate_id, catalog_working.candidate_id,
               run.build_id, source_working.build_id,
               build_channel.channel, head.receipt_id
        FROM catalog_publication_candidates AS candidate
        JOIN catalog_analysis_run_descriptor AS run
          ON run.analysis_id = candidate.analysis_id
        JOIN catalog_source_build_channel AS build_channel
          ON build_channel.build_id = run.build_id
        LEFT JOIN catalog_publication_commit_head_receipts AS head
          ON head.channel = build_channel.channel
        LEFT JOIN operational_catalog_working_candidates AS catalog_working
          ON catalog_working.candidate_id = candidate.candidate_id
        LEFT JOIN operational_source_working_builds AS source_working
          ON source_working.build_id = run.build_id
        WHERE candidate.candidate_id = %s
        LIMIT 2
        """,
        (candidate_id,),
        detail="reader-invisible publication working roots",
    )
    assert row is not None
    values = tuple(
        _as_bytes(value, field="reader-invisible publication working root")
        for value in row[:4]
    )
    if values[0] != candidate_id or values[1] != candidate_id or values[2] != values[3]:
        raise CatalogSemanticValidationError(
            "reader-invisible DB_COMMITTED successor lacks both exact working roots"
        )
    channel = _as_bytes(row[4], field="reader-invisible publication channel")
    if channel != b"default":
        raise CatalogSemanticValidationError(
            "reader-invisible DB_COMMITTED successor belongs to another channel"
        )
    expected_base = (
        None
        if row[5] is None
        else _as_bytes(row[5], field="reader-invisible publication head receipt_id")
    )
    if expected_base is not None and len(expected_base) != 16:
        raise CatalogSemanticValidationError(
            "reader-invisible publication head receipt_id is not 16 bytes"
        )

    candidate_base_row = _one(
        connector,
        "SELECT base_receipt_id "
        "FROM catalog_publication_candidate_base_publication_commits "
        "WHERE candidate_id = %s LIMIT 2",
        (candidate_id,),
        detail="reader-invisible publication candidate base",
        optional=True,
    )
    build_base_row = _one(
        connector,
        "SELECT base_receipt_id "
        "FROM catalog_source_build_base_publication_commits "
        "WHERE build_id = %s LIMIT 2",
        (values[2],),
        detail="reader-invisible publication source-build base",
        optional=True,
    )
    candidate_base = (
        None
        if candidate_base_row is None
        else _as_bytes(
            candidate_base_row[0],
            field="reader-invisible publication candidate base receipt_id",
        )
    )
    build_base = (
        None
        if build_base_row is None
        else _as_bytes(
            build_base_row[0],
            field="reader-invisible publication source-build base receipt_id",
        )
    )
    if candidate_base != expected_base or build_base != expected_base:
        raise CatalogSemanticValidationError(
            "reader-invisible DB_COMMITTED successor lacks its exact head base "
            "authority"
        )


def _validate_identity_codec_vectors() -> None:
    vectors = (
        (
            identity.gallery_key(
                bytes(range(32)),
                bytes.fromhex(
                    "2b054506682936127fd916b15c50a29ea0e15e96d4e0f249ce68a88bdd3d1ae3"
                ),
            ),
            "7e5c25a4144d31c0e6cfc3fc5380b1b65659356ca1d1602dea4503c45485975f",
            "gallery-key.v1",
        ),
        (
            identity.publication_key((1 << 63) - 1),
            "ef9a3bbaa67483f863e6aa50c1c8f2b97969a6acf1b21d6ea77df181e3bb0fd2",
            "publication-key.v1",
        ),
        (
            identity.publication_selection_occurrence_sha256(
                bytes(range(16)), bytes(range(32, 64))
            ),
            "2ff17d9d79c889c594c82864f71d4799a172199db2c823ee919bc0f5ab9928fa",
            "publication-selection-occurrence.v1",
        ),
        (
            identity.catalog_publication_occurrence_sha256(1, bytes(range(32, 64))),
            "b22702b4189d823ff834f6dd335f342aeaa563af2d3fe8c26c16a1b6e9a9d697",
            "catalog-publication-occurrence.v1",
        ),
        (
            identity.file_key(b"galleryinfo.txt"),
            "941b3ae3880d76dc3de825346febfb62b7c9c11111fd3a9260cb2622d6bab138",
            "file-key.v1",
        ),
        (
            identity.source_scope_key("filesystem", bytes(range(32)), 1),
            "25a68fc28a95244a3d8b5c00b26cec4f5b8c1d6975fab341aac08ceffa6c042e",
            "source-scope-key.v1",
        ),
        (
            identity.canonical_value_digest("tag_value_utf8_v1", b"test"),
            "57b3c6a25f0aa803d2bcd2b220100d07af70368277af7b077c6ae1e98f762809",
            "canonical-value.v1",
        ),
    )
    for actual, expected_hex, name in vectors:
        if actual.hex() != expected_hex:
            raise CatalogSemanticValidationError(f"production {name} codec drifted")


def _validate_artifact_codec_vectors() -> None:
    if identity.artifact_policy_digest(
        2, b"fixture-adapter", bytes((3,)) * 32
    ).hex() != ("7b7f57411c129ea4532155aeca464d45b5c79fc71122c6ceda6fb4cb8e6c57c4"):
        raise CatalogSemanticValidationError("artifact policy codec drifted")
    storage_key = (
        "catalog-fixture-v2",
        ("acquisitions", "aa", "b", "opaque.bin"),
    )
    if identity.artifact_storage_key_digest(*storage_key).hex() != (
        "d5bd0ef1e33e58ce9af8b1cd52dad5083146e27bc404a2d99bc8c9561551b29b"
    ):
        raise CatalogSemanticValidationError("artifact storage-key codec drifted")
    protection = identity.encode_artifact_protection_token(
        bytes((0x11,)) * 16,
        bytes((0x22,)) * 32,
        "acquisition",
        identity.artifact_storage_key_digest(*storage_key),
        9,
    )
    if (
        len(protection) != 32
        or protection.hex()
        != ("ee626b6b4d714d41b4507f4fb4fdb9b65dd95957a42d6907a76af8c54fbc0c8b")
        or identity.decode_artifact_protection_token(protection) != protection
    ):
        raise CatalogSemanticValidationError("artifact protection codec drifted")
    if identity.artifact_semantics_digest(
        *(bytes((value,)) * 32 for value in range(1, 7))
    ).hex() != ("24e1140357d6956ded50b48db8ee90171c7eff0b1179c4cf3636cfaf3dda2047"):
        raise CatalogSemanticValidationError("artifact six-component codec drifted")
    member_plan = bytes.fromhex(
        "68326864622d766e6578742d61727469666163742d6d656d6265722d706c616e0000000002000000000000000200000000000000000000000f67616c6c657279696e666f2e74787401010101010101010101010101010101010101010101010101010101010101010000000000000007000000000000000002000000073030312e6a70670202020202020202020202020202020202020202020202020202020202020202000000000000000901"
    )
    entries = identity.decode_artifact_member_plan(member_plan)
    if identity.encode_artifact_member_plan(entries) != member_plan:
        raise CatalogSemanticValidationError(
            "artifact member-plan closed enum codec drifted"
        )


def _validate_retained_title_sort_identities(connector: SQLConnector) -> None:
    after_policy_id = 0
    after_title_sha256 = b""
    cache = _CanonicalValidationCache()
    while True:
        rows = connector.fetch_all(
            """
            SELECT title_sort.title_sort_policy_id, title_sort.title_sha256,
                   title_sort.sort_title_sha256,
                   policy.title_sort_algorithm_version,
                   policy.unicode_data_version
            FROM catalog_title_sorts AS title_sort
            LEFT JOIN catalog_title_sort_policy AS policy
              ON policy.title_sort_policy_id = title_sort.title_sort_policy_id
            WHERE title_sort.title_sort_policy_id > %s
               OR (title_sort.title_sort_policy_id = %s
                   AND title_sort.title_sha256 > %s)
            ORDER BY title_sort.title_sort_policy_id, title_sort.title_sha256
            LIMIT %s
            """,
            (
                after_policy_id,
                after_policy_id,
                after_title_sha256,
                _CATALOG_RESOURCE_PAGE_LIMIT,
            ),
        )
        if not rows:
            break
        for row in rows:
            policy_id = _as_int(row[0], field="retained title-sort policy_id")
            title_sha256 = _as_bytes(row[1], field="retained title-sort title_sha256")
            sort_title_sha256 = _as_bytes(
                row[2], field="retained title-sort sort_title_sha256"
            )
            if row[3] is None or row[4] is None:
                raise CatalogSemanticValidationError(
                    "retained title sort has no exact runtime policy"
                )
            algorithm_version = _as_int(
                row[3],
                field="retained title-sort algorithm version",
                positive=True,
            )
            unicode_version = _as_bytes(
                row[4], field="retained title-sort Unicode version"
            )
            if (algorithm_version, unicode_version) != (
                1,
                _RUNTIME_UNICODE_DATA_VERSION,
            ):
                raise CatalogSemanticValidationError(
                    "retained title sort uses an unsupported runtime policy"
                )

            title_spool, title_byte_count = _validated_canonical_spool(
                connector,
                title_sha256,
                expected_domain=b"display_title_utf8_v1",
                detail="retained display title",
                cache=cache,
            )
            sort_spool: BinaryIO | None = None
            expected_spool: BinaryIO | None = None
            try:
                sort_spool, _sort_byte_count = _validated_canonical_spool(
                    connector,
                    sort_title_sha256,
                    expected_domain=b"title_sort_utf8_v1",
                    detail="retained title-sort value",
                    cache=cache,
                )
                expected_spool, _expected_byte_count = _casefolded_title_spool(
                    title_spool,
                    byte_count=title_byte_count,
                )
                if _compare_canonical_spools(expected_spool, sort_spool) != 0:
                    raise CatalogSemanticValidationError(
                        "retained title sort differs from exact Unicode casefold"
                    )
            finally:
                title_spool.close()
                if sort_spool is not None:
                    sort_spool.close()
                if expected_spool is not None:
                    expected_spool.close()

            after_policy_id = policy_id
            after_title_sha256 = title_sha256


def check_identity_codecs_v1(connector: SQLConnector) -> None:
    """Validate executable codecs and retained reproducible identities."""

    _validate_exact_registries(connector)
    _validate_identity_codec_vectors()
    _validate_retained_title_sort_identities(connector)
    _active_source_contexts(connector)


def check_canonical_reference_domains_v1(connector: SQLConnector) -> None:
    """Validate the closed registry and every retained canonical reference."""

    _validate_exact_registries(connector)
    _validate_retained_canonical_reference_domains(connector)
    _active_source_contexts(connector)
    _validate_live_snapshot_manifest_pins(connector)
    _active_publication_contexts(connector)


def check_source_baseline_channel_v1(connector: SQLConnector) -> None:
    """Validate the bounded active source head/provenance/build chain."""

    _validate_exact_registries(connector)
    for context in _active_source_contexts(connector):
        if context.analysis_id is not None:
            _validate_analysis_seal(
                connector,
                context.analysis_id,
                expected_policy_id=context.analysis_policy_id,
            )


def check_incremental_impact_v1(connector: SQLConnector) -> None:
    """Require exact active analysis seals and complete impacted-key families."""

    _validate_exact_registries(connector)
    analysis_ids = {
        context.analysis_id
        for context in _active_source_contexts(connector)
        if context.analysis_id is not None
    }
    analysis_ids.update(
        context.analysis_id
        for context in _active_publication_contexts(connector)
        if context.analysis_id is not None
    )
    for analysis_id in sorted(analysis_ids):
        _validate_analysis_seal(connector, analysis_id)
        _validate_impacted_key_families(connector, analysis_id)


def check_overlay_resolution_seal_v1(connector: SQLConnector) -> None:
    """Validate the bounded active ancestry and exact five-component seal set."""

    check_incremental_impact_v1(connector)


def check_published_baseline_prune_v1(connector: SQLConnector) -> None:
    """Require safe-ACK pruning without weakening live ancestry authority."""

    _validate_exact_registries(connector)
    retained_depth_zero = connector.fetch_all(
        """
        SELECT committed.receipt_id
        FROM catalog_publication_commit_finalizations AS finalized
        JOIN catalog_publication_commits AS committed
          ON committed.receipt_id = finalized.receipt_id
        JOIN catalog_source_revision_provenance AS provenance
          ON provenance.source_revision = committed.source_revision
        JOIN catalog_analysis_state_anchors AS anchor
          ON anchor.analysis_id = provenance.analysis_id
        JOIN catalog_analysis_baselines AS baseline
          ON baseline.analysis_id = provenance.analysis_id
        WHERE anchor.overlay_depth = 0
        LIMIT 1
        """
    )
    if retained_depth_zero:
        raise CatalogSemanticValidationError(
            "published depth-zero analysis retained its working baseline"
        )

    missing_published_parent = connector.fetch_all(
        """
        SELECT committed.receipt_id
        FROM catalog_publication_commit_finalizations AS finalized
        JOIN catalog_publication_commits AS committed
          ON committed.receipt_id = finalized.receipt_id
        JOIN catalog_source_revision_provenance AS provenance
          ON provenance.source_revision = committed.source_revision
        JOIN catalog_analysis_state_anchors AS anchor
          ON anchor.analysis_id = provenance.analysis_id
        LEFT JOIN catalog_analysis_baselines AS baseline
          ON baseline.analysis_id = provenance.analysis_id
        WHERE anchor.overlay_depth > 0
          AND baseline.analysis_id IS NULL
        LIMIT 1
        """
    )
    if missing_published_parent:
        raise CatalogSemanticValidationError(
            "positive-depth published analysis lost its ancestry baseline"
        )

    missing_working_parent = connector.fetch_all(
        """
        SELECT run.analysis_id
        FROM operational_source_working_builds AS working
        JOIN catalog_analysis_run_descriptor AS run
          ON run.build_id = working.build_id
        JOIN catalog_source_build_base_publication_commits AS source_base
          ON source_base.build_id = run.build_id
        LEFT JOIN catalog_analysis_baselines AS baseline
          ON baseline.analysis_id = run.analysis_id
        WHERE baseline.analysis_id IS NULL
          AND NOT EXISTS (
            SELECT 1
            FROM catalog_source_revision_provenance AS provenance
            JOIN catalog_publication_commits AS committed
              ON committed.source_revision = provenance.source_revision
            JOIN catalog_publication_commit_finalizations AS finalized
              ON finalized.receipt_id = committed.receipt_id
            WHERE provenance.analysis_id = run.analysis_id)
        LIMIT 1
        """
    )
    if missing_working_parent:
        raise CatalogSemanticValidationError(
            "non-genesis working analysis lacks its exact baseline authority"
        )


def check_artifact_semantics_v1(connector: SQLConnector) -> None:
    """Validate executable artifact codecs and active policy/candidate seals."""

    _validate_exact_registries(connector)
    _validate_artifact_codec_vectors()
    _active_publication_contexts(connector)


def check_publication_atomicity_v1(connector: SQLConnector) -> None:
    """Validate each active head's immutable revision/receipt/candidate chain."""

    _validate_exact_registries(connector)
    _validate_publication_generation_history(connector)
    _active_publication_contexts(connector)


def check_discovery_exactness_v1(connector: SQLConnector) -> None:
    """Recompute the current sealed search and facet projection exactly."""

    _validate_exact_registries(connector)
    rows = connector.fetch_all("""
        SELECT registry.channel, head.revision,
               committed.display_title_policy_id
        FROM catalog_channel_registry AS registry
        LEFT JOIN catalog_publication_commit_heads AS head
          ON head.channel = registry.channel
        LEFT JOIN catalog_publication_commits AS committed
          ON committed.receipt_id = head.receipt_id
        ORDER BY registry.channel
        LIMIT 2
        """)
    if len(rows) != 1:
        raise CatalogSemanticValidationError(
            "discovery head exceeds the channel registry"
        )
    channel = _as_bytes(rows[0][0], field="discovery channel")
    if channel != b"default":
        raise CatalogSemanticValidationError("discovery uses an unknown channel")
    if rows[0][1] is None:
        if rows[0][2] is not None:
            raise CatalogSemanticValidationError(
                "discovery title policy exists without a current revision"
            )
        return
    revision = _as_int(rows[0][1], field="discovery revision", positive=True)
    display_title_policy_id = _as_int(
        rows[0][2],
        field="discovery display title policy",
        positive=True,
    )
    revision_row = _one(
        connector,
        "SELECT publication_count, artifact_count FROM catalog_revisions "
        "WHERE revision = %s LIMIT 2",
        (revision,),
        detail="active discovery revision",
    )
    assert revision_row is not None
    publication_count = _as_int(
        revision_row[0], field="active discovery publication_count"
    )
    artifact_count = _as_int(revision_row[1], field="active discovery artifact_count")
    if artifact_count not in {0, publication_count}:
        raise CatalogSemanticValidationError(
            "catalog revision artifact_count is neither zero nor publication_count"
        )
    _validate_active_discovery_projection(
        connector,
        revision=revision,
        display_title_policy_id=display_title_policy_id,
        expected_publication_count=publication_count,
    )


def check_state_machines_v1(connector: SQLConnector) -> None:
    """Validate active state-machine terminals and their exact seal markers."""

    validate_catalog_state_machine_contract()
    _validate_exact_registries(connector)
    for context in _active_source_contexts(connector):
        if context.analysis_id is not None:
            _validate_analysis_seal(connector, context.analysis_id)
    _active_publication_contexts(connector)


_FILE_FAMILY_MEMBER_TABLES = (
    "catalog_gallery_observation_file_file_nos",
    "catalog_gallery_observation_file_file_sha256s",
    "catalog_gallery_observation_file_artifact_role",
    "catalog_gallery_observation_file_seals",
)


def _validate_file_family_totality(connector: SQLConnector) -> None:
    """Prove every retained file anchor has one complete sealed family."""

    after_gallery_id = 0
    after_observation_id = 0
    after_file_key = b""
    while True:
        rows = connector.fetch_all(
            """
            SELECT anchor.gallery_id, anchor.observation_id, anchor.file_key,
                   file_no.file_no, file_sha.file_sha256,
                   stored_role.artifact_role, sealed.file_key,
                   name.file_key, name.name_bytes
            FROM catalog_gallery_observation_file_anchors AS anchor
            LEFT JOIN catalog_gallery_observation_file_file_nos AS file_no
              ON file_no.gallery_id = anchor.gallery_id
             AND file_no.observation_id = anchor.observation_id
             AND file_no.file_key = anchor.file_key
            LEFT JOIN catalog_gallery_observation_file_file_sha256s AS file_sha
              ON file_sha.gallery_id = anchor.gallery_id
             AND file_sha.observation_id = anchor.observation_id
             AND file_sha.file_key = anchor.file_key
            LEFT JOIN catalog_gallery_observation_file_artifact_role AS stored_role
              ON stored_role.gallery_id = anchor.gallery_id
             AND stored_role.observation_id = anchor.observation_id
             AND stored_role.file_key = anchor.file_key
            LEFT JOIN catalog_gallery_observation_file_seals AS sealed
              ON sealed.gallery_id = anchor.gallery_id
             AND sealed.observation_id = anchor.observation_id
             AND sealed.file_key = anchor.file_key
            LEFT JOIN catalog_file_name_identities AS name
              ON name.file_key = anchor.file_key
            WHERE (anchor.gallery_id, anchor.observation_id, anchor.file_key)
                  > (%s, %s, %s)
            ORDER BY anchor.gallery_id, anchor.observation_id, anchor.file_key
            LIMIT %s
            """,
            (
                after_gallery_id,
                after_observation_id,
                after_file_key,
                _CATALOG_RESOURCE_PAGE_LIMIT,
            ),
        )
        if not rows:
            break
        for row in rows:
            gallery_id = _as_int(
                row[0], field="retained file family gallery_id", positive=True
            )
            observation_id = _as_int(
                row[1], field="retained file family observation_id", positive=True
            )
            file_key = _as_bytes(row[2], field="retained file family file_key")
            if any(value is None for value in row[3:]):
                raise CatalogSemanticValidationError(
                    "retained observation file has an incomplete sealed family"
                )
            _as_int(row[3], field="retained file family file_no")
            file_sha256 = _as_bytes(row[4], field="retained file family file_sha256")
            artifact_role = _as_bytes(
                row[5], field="retained file family artifact_role"
            )
            sealed_file_key = _as_bytes(
                row[6], field="retained file family sealed file_key"
            )
            name_file_key = _as_bytes(
                row[7], field="retained file family name file_key"
            )
            name_bytes = _as_bytes(row[8], field="retained file family name_bytes")
            try:
                expected_key = identity.file_key(name_bytes)
            except (TypeError, ValueError) as error:
                raise CatalogSemanticValidationError(
                    "retained observation file has an invalid file-name identity"
                ) from error
            if (
                len(file_sha256) != 32
                or file_key != sealed_file_key
                or file_key != name_file_key
                or file_key != expected_key
                or artifact_role not in {b"metadata", b"page", b"other"}
            ):
                raise CatalogSemanticValidationError(
                    "retained observation file has invalid immutable facts"
                )
            after_gallery_id = gallery_id
            after_observation_id = observation_id
            after_file_key = file_key

    # Foreign keys normally make these subset checks redundant.  READY must
    # still reject a database damaged while foreign-key enforcement was off.
    for table in _FILE_FAMILY_MEMBER_TABLES:
        after_gallery_id = 0
        after_observation_id = 0
        after_file_key = b""
        while True:
            rows = connector.fetch_all(
                f"""
                SELECT member.gallery_id, member.observation_id, member.file_key,
                       anchor.file_key
                FROM {table} AS member
                LEFT JOIN catalog_gallery_observation_file_anchors AS anchor
                  ON anchor.gallery_id = member.gallery_id
                 AND anchor.observation_id = member.observation_id
                 AND anchor.file_key = member.file_key
                WHERE (member.gallery_id, member.observation_id, member.file_key)
                      > (%s, %s, %s)
                ORDER BY member.gallery_id, member.observation_id, member.file_key
                LIMIT %s
                """,
                (
                    after_gallery_id,
                    after_observation_id,
                    after_file_key,
                    _CATALOG_RESOURCE_PAGE_LIMIT,
                ),
            )
            if not rows:
                break
            for row in rows:
                gallery_id = _as_int(
                    row[0], field="retained file member gallery_id", positive=True
                )
                observation_id = _as_int(
                    row[1], field="retained file member observation_id", positive=True
                )
                file_key = _as_bytes(row[2], field="retained file member file_key")
                if row[3] is None:
                    raise CatalogSemanticValidationError(
                        "retained observation file member has no anchor authority"
                    )
                anchor_file_key = _as_bytes(
                    row[3], field="retained file member anchor file_key"
                )
                if anchor_file_key != file_key:
                    raise CatalogSemanticValidationError(
                        "retained observation file member disagrees with its anchor"
                    )
                after_gallery_id = gallery_id
                after_observation_id = observation_id
                after_file_key = file_key


def _iter_derived_file_hash_occurrences(
    connector: SQLConnector,
) -> Iterator[tuple[int, int, bytes, int]]:
    """Derive CONTENT hash multiplicities with a constant-memory keyset scan."""

    after_gallery_id = 0
    after_observation_id = 0
    after_file_sha256 = b""
    after_file_key = b""
    current_key: tuple[int, int, bytes] | None = None
    current_count = 0
    while True:
        rows = connector.fetch_all(
            """
            SELECT file_sha.gallery_id, file_sha.observation_id,
                   file_sha.file_sha256, file_sha.file_key
            FROM catalog_gallery_observation_file_file_sha256s AS file_sha
            JOIN catalog_gallery_observation_file_seals AS sealed
              ON sealed.gallery_id = file_sha.gallery_id
             AND sealed.observation_id = file_sha.observation_id
             AND sealed.file_key = file_sha.file_key
            JOIN catalog_file_name_identities AS name
              ON name.file_key = file_sha.file_key
            WHERE name.name_bytes <> %s
              AND (file_sha.gallery_id, file_sha.observation_id,
                   file_sha.file_sha256, file_sha.file_key) > (%s, %s, %s, %s)
            ORDER BY file_sha.gallery_id, file_sha.observation_id,
                     file_sha.file_sha256, file_sha.file_key
            LIMIT %s
            """,
            (
                b"galleryinfo.txt",
                after_gallery_id,
                after_observation_id,
                after_file_sha256,
                after_file_key,
                _CATALOG_RESOURCE_PAGE_LIMIT,
            ),
        )
        if not rows:
            break
        for row in rows:
            gallery_id = _as_int(
                row[0], field="derived hash occurrence gallery_id", positive=True
            )
            observation_id = _as_int(
                row[1], field="derived hash occurrence observation_id", positive=True
            )
            file_sha256 = _as_bytes(row[2], field="derived hash occurrence file_sha256")
            file_key = _as_bytes(row[3], field="derived hash occurrence file_key")
            key = (gallery_id, observation_id, file_sha256)
            if current_key is not None and key != current_key:
                yield (*current_key, current_count)
                current_count = 0
            current_key = key
            current_count += 1
            after_gallery_id = gallery_id
            after_observation_id = observation_id
            after_file_sha256 = file_sha256
            after_file_key = file_key
    if current_key is not None:
        yield (*current_key, current_count)


def _iter_stored_file_hash_occurrences(
    connector: SQLConnector,
) -> Iterator[tuple[int, int, bytes, int]]:
    after_gallery_id = 0
    after_observation_id = 0
    after_file_sha256 = b""
    while True:
        rows = connector.fetch_all(
            """
            SELECT gallery_id, observation_id, file_sha256, occurrence_count
            FROM catalog_gallery_observation_file_hash_occurrences
            WHERE (gallery_id, observation_id, file_sha256) > (%s, %s, %s)
            ORDER BY gallery_id, observation_id, file_sha256
            LIMIT %s
            """,
            (
                after_gallery_id,
                after_observation_id,
                after_file_sha256,
                _CATALOG_RESOURCE_PAGE_LIMIT,
            ),
        )
        if not rows:
            return
        for row in rows:
            gallery_id = _as_int(
                row[0], field="stored hash occurrence gallery_id", positive=True
            )
            observation_id = _as_int(
                row[1], field="stored hash occurrence observation_id", positive=True
            )
            file_sha256 = _as_bytes(row[2], field="stored hash occurrence file_sha256")
            occurrence_count = _as_int(
                row[3], field="stored hash occurrence count", positive=True
            )
            yield gallery_id, observation_id, file_sha256, occurrence_count
            after_gallery_id = gallery_id
            after_observation_id = observation_id
            after_file_sha256 = file_sha256


def check_role_derivation_v1(connector: SQLConnector) -> None:
    """Validate the classifier and every retained file-role materialization.

    The full READY audit is allowed to be linear in retained catalog data, but
    each query and Python working set remains hard-capped.  The separate
    ``ready`` epoch probe does not call this validator.
    """

    _validate_exact_registries(connector)
    _validate_identity_codec_vectors()
    if (
        identity.file_role(b"galleryinfo.txt") != b"METADATA"
        or identity.file_role(b"GalleryInfo.txt") != b"CONTENT"
        or identity.file_role(b"image.jpg") != b"CONTENT"
    ):
        raise CatalogSemanticValidationError("exact file-role classifier drifted")

    _validate_file_family_totality(connector)

    expected = _iter_derived_file_hash_occurrences(connector)
    stored = _iter_stored_file_hash_occurrences(connector)
    while True:
        expected_row = next(expected, None)
        stored_row = next(stored, None)
        if expected_row is None and stored_row is None:
            break
        if expected_row != stored_row:
            raise CatalogSemanticValidationError(
                "retained file-hash occurrences differ from exact CONTENT roles"
            )


def check_physical_domains_v1(connector: SQLConnector) -> None:
    """Validate executable byte bounds plus bounded active scalar domains."""

    _validate_exact_registries(connector)
    if (
        identity.CANONICAL_VALUE_PAGE_MAXIMUM_BYTES != 65536
        or identity.CANONICAL_VALUE_CHUNK_BYTES != 32768
        or identity.GALLERY_OBSERVATION_PAGE_MAXIMUM_BYTES != 65536
        or identity.FILESYSTEM_STAT_FINGERPRINT_BYTES != 40
        or identity.ARTIFACT_STORAGE_KEY_MAXIMUM_BYTES != 4096
    ):
        raise CatalogSemanticValidationError(
            "production physical-domain constants drifted"
        )
    fingerprint = identity.encode_filesystem_stat_fingerprint(
        device=1,
        inode=2,
        size_bytes=3,
        modified_ns=-4,
        changed_ns=5,
    )
    if len(fingerprint) != 40 or identity.decode_filesystem_stat_fingerprint(
        fingerprint
    ) != (1, 2, 3, -4, 5):
        raise CatalogSemanticValidationError(
            "filesystem fingerprint codec is not the exact 40-byte domain"
        )
    _active_source_contexts(connector)
    _active_publication_contexts(connector)


def _data_absent_relations() -> tuple[str, ...]:
    contracts = ARTIFACT.get("bootstrap_contracts")
    if not isinstance(contracts, tuple):
        raise BuiltinSemanticRegistryError(
            "generated bootstrap contracts are malformed"
        )
    matches = tuple(
        value
        for value in contracts
        if isinstance(value, Mapping) and value.get("source") == "data"
    )
    if len(matches) != 1:
        raise BuiltinSemanticRegistryError(
            "generated data bootstrap contract is not singular"
        )
    raw = matches[0].get("absent_relations")
    data_relations = ARTIFACT.get("data_relations")
    if (
        not isinstance(raw, tuple)
        or not all(isinstance(value, str) for value in raw)
        or not isinstance(data_relations, tuple)
        or not all(isinstance(value, str) for value in data_relations)
    ):
        raise BuiltinSemanticRegistryError(
            "generated data bootstrap absence registry is malformed"
        )
    physical_relations = _physical_data_tables()
    bootstrap_seeds = ARTIFACT.get("bootstrap_seeds")
    if not isinstance(bootstrap_seeds, tuple):
        raise BuiltinSemanticRegistryError(
            "generated bootstrap seed registry is malformed"
        )
    seeded_relations = {
        value.get("relation")
        for value in bootstrap_seeds
        if isinstance(value, Mapping) and value.get("source") == "data"
    }
    if not all(isinstance(value, str) for value in seeded_relations):
        raise BuiltinSemanticRegistryError(
            "generated data bootstrap seed relation is malformed"
        )
    expected = tuple(
        value
        for value in data_relations
        if value in physical_relations and value not in seeded_relations
    )
    if raw != expected:
        raise BuiltinSemanticRegistryError(
            "generated data bootstrap absence registry is not closed-world"
        )
    return raw


def _physical_data_tables() -> Mapping[str, str]:
    backends = ARTIFACT.get("backends")
    if not isinstance(backends, Mapping):
        raise BuiltinSemanticRegistryError("generated backend payloads are malformed")
    result: dict[str, str] = {}
    for backend in ("sqlite", "mariadb"):
        payload = backends.get(backend)
        relations = payload.get("relations") if isinstance(payload, Mapping) else None
        if not isinstance(relations, tuple):
            raise BuiltinSemanticRegistryError(
                f"generated {backend} relation registry is malformed"
            )
        current: dict[str, str] = {}
        for value in relations:
            if (
                not isinstance(value, Mapping)
                or value.get("plane") != "data"
                or value.get("kind") != "table"
            ):
                continue
            relation = value.get("relation")
            table = value.get("table")
            if (
                not isinstance(relation, str)
                or not isinstance(table, str)
                or _SAFE_IDENTIFIER.fullmatch(table) is None
                or relation in current
            ):
                raise BuiltinSemanticRegistryError(
                    f"generated {backend} data relation registry is malformed"
                )
            current[relation] = table
        if not result:
            result = current
        elif result != current:
            raise BuiltinSemanticRegistryError(
                "generated backend data relation names differ"
            )
    return result


def check_bootstrap_v1(connector: SQLConnector) -> None:
    """Validate BUILDING-only catalog seeds and fresh data-plane absence."""

    _validate_exact_registries(connector)
    tables = _physical_data_tables()
    for relation in _data_absent_relations():
        if relation in {"analysis_stage", "publication_stage"}:
            # These logical views are necessarily populated by their provider-
            # seeded vertical registry families on a greenfield catalog.
            continue
        table = tables.get(relation)
        if table is None:
            raise BuiltinSemanticRegistryError(
                f"generated bootstrap relation {relation!r} lacks a physical table"
            )
        if connector.fetch_all(f"SELECT 1 FROM {table} LIMIT 1"):
            raise CatalogSemanticValidationError(
                f"greenfield catalog relation {relation!r} is not empty"
            )


def check_retention_contract_v2(connector: SQLConnector) -> None:
    """Validate bounded active-head retention blockers and seal reachability."""

    _validate_exact_registries(connector)
    _validate_publication_generation_history(connector)
    source_contexts = _active_source_contexts(connector)
    _validate_live_snapshot_manifest_pins(connector)
    for context in source_contexts:
        if context.analysis_id is not None:
            _validate_analysis_seal(connector, context.analysis_id)
    publication_contexts = _active_publication_contexts(connector)
    source_by_channel = {context.channel: context for context in source_contexts}
    for publication in publication_contexts:
        source = source_by_channel.get(publication.channel)
        if source is None:
            raise CatalogSemanticValidationError(
                "active publication head has no retained source head"
            )
        if publication.source_revision > source.source_revision:
            raise CatalogSemanticValidationError(
                "publication receipt references a source revision beyond its active head"
            )


def builtin_semantic_validators() -> Mapping[str, SemanticValidator]:
    """Return the exact recurring catalog validator registry.

    ``catalog.bootstrap.v1`` is intentionally absent: its lifecycle is
    ``building_only`` and the schema provider rejects it as an unexpected READY
    callback. :func:`check_bootstrap_v1` remains directly executable for the
    BUILDING/genesis path.
    """

    _validate_static_catalog_contract()
    validators: Mapping[str, SemanticValidator] = MappingProxyType(
        {
            "catalog.identity-codecs.v1": check_identity_codecs_v1,
            "catalog.canonical-reference-domains.v1": check_canonical_reference_domains_v1,
            "catalog.source-baseline-channel.v1": check_source_baseline_channel_v1,
            "catalog.incremental-impact.v1": check_incremental_impact_v1,
            "catalog.overlay-resolution-seal.v1": check_overlay_resolution_seal_v1,
            "catalog.published-baseline-prune.v1": check_published_baseline_prune_v1,
            "catalog.artifact-semantics.v1": check_artifact_semantics_v1,
            "catalog.publication-atomicity.v1": check_publication_atomicity_v1,
            "catalog.discovery-exactness.v1": check_discovery_exactness_v1,
            "catalog.state-machines.v1": check_state_machines_v1,
            "catalog.role-derivation.v1": check_role_derivation_v1,
            "catalog.physical-domains.v1": check_physical_domains_v1,
            "catalog.retention.v2": check_retention_contract_v2,
        }
    )
    expected = tuple(
        obligation_id
        for obligation_id, lifecycle, _ready_check in _SPECS
        if obligation_id.startswith("catalog.") and lifecycle == "ready_and_runtime"
    )
    if tuple(validators) != expected:
        raise BuiltinSemanticRegistryError(
            "catalog production validator registry differs from the obligation manifest"
        )
    return validators
