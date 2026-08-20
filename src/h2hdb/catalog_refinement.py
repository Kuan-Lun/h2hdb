"""Bounded production refinements for the vNext catalog data plane.

READY validation deliberately does not replay high-cardinality canonical trees,
artifact plans, analysis workset derivations, or revision projections. Those
properties belong to the named transaction writer hooks. The recurring checks
here are read-only and otherwise bounded to closed registries, active channel
heads, and the at-most-16-deep analysis seal chain. Sealed impacted-key families
are audited with key-first indexed anti-joins, and publication generation
history is linearly audited over the single sealed common commit chain. Quick
readiness remains epoch-only and O(1).
"""

from __future__ import annotations

__all__ = [
    "BuiltinSemanticRegistryError",
    "CatalogSemanticValidationError",
    "builtin_semantic_validators",
    "check_artifact_semantics_v1",
    "check_bootstrap_v1",
    "check_canonical_reference_domains_v1",
    "check_identity_codecs_v1",
    "check_incremental_impact_v1",
    "check_overlay_resolution_seal_v1",
    "check_physical_domains_v1",
    "check_publication_atomicity_v1",
    "check_retention_contract_v1",
    "check_role_derivation_v1",
    "check_source_baseline_channel_v1",
    "check_state_machines_v1",
    "validate_builtin_semantic_manifest",
]

import re
import unicodedata
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from hashlib import sha256
from types import MappingProxyType
from typing import Any

from . import vnext_identity as identity
from ._generated_vnext_schema import ARTIFACT
from .catalog_writer import validate_artifact_writer_manifest
from .sql_connector import SQLConnector

type SemanticValidator = Callable[[SQLConnector], None]


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
        "catalog.retention.v1",
        "ready_and_runtime",
        "catalog_refinement.check_retention_contract_v1",
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
        "catalog.digest-domain.artifact-effective-content.v1",
        "canonical_digest_policy",
        "digest_domain",
        "artifact_effective_content_v1",
    ),
    (
        "catalog.digest-domain.artifact-locator.v1",
        "canonical_digest_policy",
        "digest_domain",
        "artifact_locator_bytes_v1",
    ),
    (
        "catalog.digest-domain.artifact-member-plan.v1",
        "canonical_digest_policy",
        "digest_domain",
        "artifact_member_plan_v1",
    ),
    (
        "catalog.digest-domain.artifact-owner.v1",
        "canonical_digest_policy",
        "digest_domain",
        "artifact_owner_v1",
    ),
    (
        "catalog.digest-domain.artifact-policy.v2",
        "canonical_digest_policy",
        "digest_domain",
        "artifact_policy_v2",
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
)

_EXPECTED_DIGEST_DOMAINS = tuple(
    value.encode("ascii")
    for _seed_id, relation, _attribute, value in _EXPECTED_DATA_SEEDS
    if relation == "canonical_digest_policy"
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
        "catalog_a_impacted_content_anchors",
        "catalog_a_impacted_content_provenance",
        "catalog_a_impacted_content_witnesses",
        "catalog_a_impacted_content_seals",
    ),
    (
        "gid",
        "gid",
        "catalog_a_impacted_gid_anchors",
        "catalog_a_impacted_gid_provenance",
        "catalog_a_impacted_gid_witnesses",
        "catalog_a_impacted_gid_seals",
    ),
)

# These are executable runtime registrations, not merely values admitted by the
# physical integer domains.  A new byte producer, display-title rule, casefold
# implementation, or Unicode table must add a wheel implementation and then
# extend the corresponding closed registry.
_SUPPORTED_ARTIFACT_ALGORITHM_VERSIONS = frozenset({1})
_RUNTIME_UNICODE_DATA_VERSION = unicodedata.unidata_version.encode("ascii")
_SUPPORTED_DISPLAY_TITLE_POLICY_ALGORITHMS = frozenset(
    {(1, 1, _RUNTIME_UNICODE_DATA_VERSION)}
)

_VERTICAL_POLICY_FAMILIES = (
    "manifest_policy",
    "analysis_policy",
    "artifact_zip_writer_policy",
    "artifact_storage_codec",
    "artifact_policy_semantics",
    "title_sort_policy",
    "display_title_policy",
    "source_scope",
)

_VERTICAL_POLICY_BOOTSTRAP_VALUES: Mapping[
    str, tuple[str, tuple[tuple[str, str, str, int | str], ...]]
] = MappingProxyType(
    {
        "artifact_zip_writer_policy": (
            "catalog.artifact-zip-writer-policy.v1",
            (
                ("artifact_algorithm_version", "uint32", "integer", 1),
                ("zip_codec_version", "uint32", "integer", 1),
                ("compression_method", "uint32", "integer", 8),
                ("compression_level", "uint32", "integer", 9),
                ("dos_date", "uint32", "integer", 33),
                ("dos_time", "uint32", "integer", 0),
                ("unix_mode", "uint32", "integer", 33188),
                ("general_purpose_flags", "uint32", "integer", 2048),
                ("create_system", "uint32", "integer", 3),
                ("archive_name_codec_version", "uint32", "integer", 1),
                ("artifact_name_codec_version", "uint32", "integer", 1),
            ),
        ),
        "artifact_storage_codec": (
            "catalog.artifact-storage-codec.managed-filesystem.v1",
            (
                ("storage_codec_version", "uint32", "integer", 1),
                ("adapter_id", "ascii_enum", "utf8", "managed-filesystem"),
                ("locator_codec_version", "uint32", "integer", 1),
                ("protection_token_codec_version", "uint32", "integer", 1),
            ),
        ),
    }
)


def _expected_vertical_policy_bootstrap() -> (
    Mapping[
        str, tuple[tuple[str, str, tuple[tuple[str, str, str, int | str], ...]], ...]
    ]
):
    """Return the exact physical seed fanout for the closed policy registries."""

    result: dict[
        str, list[tuple[str, str, tuple[tuple[str, str, str, int | str], ...]]]
    ] = {family: [] for family in _VERTICAL_POLICY_FAMILIES}
    for family, (seed_id, values) in _VERTICAL_POLICY_BOOTSTRAP_VALUES.items():
        key = values[0]
        facts = values[1:]

        def add(
            component: str, cells: tuple[tuple[str, str, str, int | str], ...]
        ) -> None:
            relation = f"{family}_{component}"
            result[family].append((f"{seed_id}.{relation}", relation, cells))

        add("anchor", (key,))
        for fact in facts:
            add(fact[0], (key, fact))
        if family != "artifact_storage_codec":
            add("identity", (*facts, key))
        add("seal", (key,))
    return MappingProxyType(
        {family: tuple(records) for family, records in result.items()}
    )


_EXPECTED_VERTICAL_POLICY_BOOTSTRAP = _expected_vertical_policy_bootstrap()

# family -> (legacy view table, semantic key, (fact relation, fact attribute),
#            natural-identity relation or None).  Referential unique keys on
# facts exist only to support the identity-to-fact congruence FKs; they are not
# semantic candidate keys.
_VERTICAL_POLICY_SHAPES: Mapping[
    str,
    tuple[
        str,
        str,
        tuple[tuple[str, str], ...],
        str | None,
    ],
] = MappingProxyType(
    {
        "manifest_policy": (
            "catalog_manifest_policies",
            "manifest_policy_id",
            (
                (
                    "manifest_policy_manifest_algorithm_version",
                    "manifest_algorithm_version",
                ),
                ("manifest_policy_file_order_version", "file_order_version"),
            ),
            "manifest_policy_identity",
        ),
        "analysis_policy": (
            "catalog_analysis_policies",
            "policy_id",
            (
                ("analysis_policy_algorithm_version", "algorithm_version"),
                (
                    "analysis_policy_spam_artist_threshold",
                    "spam_artist_threshold",
                ),
                (
                    "analysis_policy_spam_occurrence_threshold",
                    "spam_occurrence_threshold",
                ),
                (
                    "analysis_policy_content_owner_rule_version",
                    "content_owner_rule_version",
                ),
                (
                    "analysis_policy_gid_winner_rule_version",
                    "gid_winner_rule_version",
                ),
            ),
            "analysis_policy_identity",
        ),
        "artifact_zip_writer_policy": (
            "catalog_artifact_zip_writer_policies",
            "artifact_algorithm_version",
            tuple(
                (f"artifact_zip_writer_policy_{attribute}", attribute)
                for attribute in (
                    "zip_codec_version",
                    "compression_method",
                    "compression_level",
                    "dos_date",
                    "dos_time",
                    "unix_mode",
                    "general_purpose_flags",
                    "create_system",
                    "archive_name_codec_version",
                    "artifact_name_codec_version",
                )
            ),
            "artifact_zip_writer_policy_identity",
        ),
        "artifact_storage_codec": (
            "catalog_artifact_storage_codecs",
            "storage_codec_version",
            (
                ("artifact_storage_codec_adapter_id", "adapter_id"),
                (
                    "artifact_storage_codec_locator_codec_version",
                    "locator_codec_version",
                ),
                (
                    "artifact_storage_codec_protection_token_codec_version",
                    "protection_token_codec_version",
                ),
            ),
            None,
        ),
        "artifact_policy_semantics": (
            "catalog_artifact_policy_semantics",
            "policy_component_sha256",
            (
                (
                    "artifact_policy_semantics_artifact_algorithm_version",
                    "artifact_algorithm_version",
                ),
                (
                    "artifact_policy_semantics_max_image_short_side",
                    "max_image_short_side",
                ),
                (
                    "artifact_policy_semantics_producer_fingerprint_sha256",
                    "producer_fingerprint_sha256",
                ),
            ),
            "artifact_policy_semantics_identity",
        ),
        "title_sort_policy": (
            "catalog_title_sort_policy",
            "title_sort_policy_id",
            (
                (
                    "title_sort_policy_algorithm_version",
                    "title_sort_algorithm_version",
                ),
                (
                    "title_sort_policy_unicode_data_version",
                    "unicode_data_version",
                ),
            ),
            "title_sort_policy_identity",
        ),
        "display_title_policy": (
            "catalog_display_title_policies",
            "display_title_policy_id",
            (
                (
                    "display_title_policy_algorithm_version",
                    "display_title_algorithm_version",
                ),
                (
                    "display_title_policy_title_sort_policy_id",
                    "title_sort_policy_id",
                ),
            ),
            "display_title_policy_identity",
        ),
        "source_scope": (
            "catalog_source_scopes",
            "scope_key",
            (
                ("source_scope_source_provider", "source_provider"),
                ("source_scope_source_root_sha256", "source_root_sha256"),
                (
                    "source_scope_identity_policy_version",
                    "identity_policy_version",
                ),
            ),
            "source_scope_identity",
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


def _vertical_policy_table(relation: str) -> str:
    if relation.endswith("_identity"):
        return f"catalog_{relation[:-1]}ies"
    if relation.endswith("s"):
        return f"catalog_{relation}"
    return f"catalog_{relation}s"


def _provider_columns(relation: Mapping[str, object]) -> tuple[str, ...]:
    raw = relation.get("columns")
    if not isinstance(raw, tuple) or not all(
        isinstance(column, tuple) and len(column) >= 2 and isinstance(column[0], str)
        for column in raw
    ):
        return ()
    return tuple(column[0] for column in raw)


def _provider_fk_edges(
    relation: Mapping[str, object],
) -> frozenset[tuple[tuple[str, ...], str, tuple[str, ...]]]:
    raw = relation.get("foreign_keys")
    if not isinstance(raw, tuple):
        return frozenset()
    result: set[tuple[tuple[str, ...], str, tuple[str, ...]]] = set()
    for edge in raw:
        if (
            not isinstance(edge, tuple)
            or len(edge) != 4
            or not isinstance(edge[1], tuple)
            or not isinstance(edge[2], str)
            or not isinstance(edge[3], tuple)
            or not all(isinstance(value, str) for value in (*edge[1], *edge[3]))
        ):
            return frozenset()
        result.add((edge[1], edge[2], edge[3]))
    return frozenset(result)


def _validate_vertical_policy_provider_shapes(
    relation_by_name: Mapping[object, object], *, backend: str
) -> None:
    """Pin the eight runtime policy authorities to their narrow base graph."""

    for family, (
        view_table,
        key,
        facts,
        identity_relation,
    ) in _VERTICAL_POLICY_SHAPES.items():
        anchor_name = f"{family}_anchor"
        seal_name = f"{family}_seal"
        anchor_table = _vertical_policy_table(anchor_name)
        seal_table = _vertical_policy_table(seal_name)
        natural_key = tuple(attribute for _relation, attribute in facts)

        anchor = relation_by_name.get(anchor_name)
        if (
            not isinstance(anchor, Mapping)
            or anchor.get("kind") != "table"
            or anchor.get("table") != anchor_table
            or _provider_columns(anchor) != (key,)
            or anchor.get("primary_key") != (key,)
            or anchor.get("unique_keys") != ()
            or anchor.get("referential_unique_keys") != ()
        ):
            raise BuiltinSemanticRegistryError(
                f"generated {backend} {anchor_name} lacks its exact anchor shape"
            )

        required_seal_edges: set[tuple[tuple[str, ...], str, tuple[str, ...]]] = {
            ((key,), anchor_table, (key,))
        }
        for fact_relation, fact in facts:
            fact_table = _vertical_policy_table(fact_relation)
            fact_shape = relation_by_name.get(fact_relation)
            expected_semantic_unique = (
                (("adapter_id",),)
                if fact_relation == "artifact_storage_codec_adapter_id"
                else ()
            )
            expected_referential_unique = (
                () if identity_relation is None else ((key, fact),)
            )
            if (
                not isinstance(fact_shape, Mapping)
                or fact_shape.get("kind") != "table"
                or fact_shape.get("table") != fact_table
                or _provider_columns(fact_shape) != (key, fact)
                or fact_shape.get("primary_key") != (key,)
                or fact_shape.get("unique_keys") != expected_semantic_unique
                or fact_shape.get("referential_unique_keys")
                != expected_referential_unique
                or ((key,), anchor_table, (key,)) not in _provider_fk_edges(fact_shape)
            ):
                raise BuiltinSemanticRegistryError(
                    f"generated {backend} {fact_relation} lacks its exact fact shape"
                )
            required_seal_edges.add(((key,), fact_table, (key,)))

        if identity_relation is not None:
            identity_table = _vertical_policy_table(identity_relation)
            identity_shape = relation_by_name.get(identity_relation)
            if (
                not isinstance(identity_shape, Mapping)
                or identity_shape.get("kind") != "table"
                or identity_shape.get("table") != identity_table
                or _provider_columns(identity_shape) != (*natural_key, key)
                or identity_shape.get("primary_key") != natural_key
                or identity_shape.get("unique_keys") != ((key,),)
                or identity_shape.get("referential_unique_keys") != ()
            ):
                raise BuiltinSemanticRegistryError(
                    f"generated {backend} {identity_relation} lacks its exact identity shape"
                )
            identity_edges = _provider_fk_edges(identity_shape)
            required_identity_edges = {
                ((key,), anchor_table, (key,)),
                *(
                    (
                        (key, fact),
                        _vertical_policy_table(fact_relation),
                        (key, fact),
                    )
                    for fact_relation, fact in facts
                ),
            }
            if not required_identity_edges.issubset(identity_edges):
                raise BuiltinSemanticRegistryError(
                    f"generated {backend} {identity_relation} lacks congruence FKs"
                )
            required_seal_edges.add(((key,), identity_table, (key,)))

        seal = relation_by_name.get(seal_name)
        if (
            not isinstance(seal, Mapping)
            or seal.get("kind") != "table"
            or seal.get("table") != seal_table
            or _provider_columns(seal) != (key,)
            or seal.get("primary_key") != (key,)
            or seal.get("unique_keys") != ()
            or seal.get("referential_unique_keys") != ()
            or _provider_fk_edges(seal) != frozenset(required_seal_edges)
        ):
            raise BuiltinSemanticRegistryError(
                f"generated {backend} {seal_name} lacks total-participation FKs"
            )

        view = relation_by_name.get(family)
        expected_view_unique = (
            (("adapter_id",),) if family == "artifact_storage_codec" else (natural_key,)
        )
        if (
            not isinstance(view, Mapping)
            or view.get("kind") != "view"
            or view.get("table") != view_table
            or _provider_columns(view) != (key, *natural_key)
            or view.get("primary_key") != (key,)
            or view.get("unique_keys") != expected_view_unique
            or view.get("referential_unique_keys") != ()
            or ((key,), seal_table, (key,)) not in _provider_fk_edges(view)
        ):
            raise BuiltinSemanticRegistryError(
                f"generated {backend} {family} is not the exact sealed read view"
            )


def _validate_static_catalog_contract() -> None:
    validate_builtin_semantic_manifest()
    raw = ARTIFACT.get("bootstrap_seeds")
    if not isinstance(raw, tuple):
        raise BuiltinSemanticRegistryError("generated bootstrap records are malformed")
    actual: list[tuple[str, str, str, str]] = []
    actual_stage_registries: dict[str, list[tuple[str, str, tuple[str, ...]]]] = {
        "analysis_stage": [],
        "publication_stage": [],
    }
    actual_vertical_policy_seeds: dict[
        str,
        list[
            tuple[
                str,
                str,
                tuple[tuple[str, str, str, int | str], ...],
            ]
        ],
    ] = {family: [] for family in _VERTICAL_POLICY_FAMILIES}
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
        stage_family = next(
            (
                family
                for family in actual_stage_registries
                if relation.startswith(f"{family}_")
            ),
            None,
        )
        if stage_family is not None:
            component = relation.removeprefix(f"{stage_family}_")
            expected_attributes = {
                "anchor": ("stage",),
                "order": ("stage", "stage_order"),
                "cursor_codec": ("stage", "cursor_codec"),
                "seal": ("stage",),
            }.get(component)
            if (
                expected_attributes is None
                or len(cells) != len(expected_attributes)
                or any(not isinstance(cell, tuple) or len(cell) != 4 for cell in cells)
                or tuple(cell[0] for cell in cells) != expected_attributes
                or any(cell[1:3] != ("ascii_enum", "utf8") for cell in cells)
                or any(not isinstance(cell[3], str) for cell in cells)
            ):
                raise BuiltinSemanticRegistryError(
                    f"generated {relation.replace('_', '-')} seed is malformed"
                )
            actual_stage_registries[stage_family].append(
                (seed_id, component, tuple(cell[3] for cell in cells))
            )
            continue
        vertical_policy_family = next(
            (
                family
                for family in _VERTICAL_POLICY_FAMILIES
                if relation.startswith(f"{family}_")
            ),
            None,
        )
        if vertical_policy_family is not None:
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
            actual_vertical_policy_seeds[vertical_policy_family].append(
                (seed_id, relation, cells)
            )
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
    if {
        family: tuple(records)
        for family, records in actual_vertical_policy_seeds.items()
    } != _EXPECTED_VERTICAL_POLICY_BOOTSTRAP:
        raise BuiltinSemanticRegistryError(
            "generated vertical policy seed fanout differs from executable constants"
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
        record
        for name, order, cursor_codec in analysis_stage_values
        for record in (
            (
                f"catalog.analysis-stage.{name.replace('_', '-')}.v1.analysis_stage_anchor",
                "anchor",
                (name,),
            ),
            (
                f"catalog.analysis-stage.{name.replace('_', '-')}.v1.analysis_stage_order",
                "order",
                (name, f"{order:02d}"),
            ),
            (
                f"catalog.analysis-stage.{name.replace('_', '-')}.v1.analysis_stage_cursor_codec",
                "cursor_codec",
                (name, cursor_codec),
            ),
            (
                f"catalog.analysis-stage.{name.replace('_', '-')}.v1.analysis_stage_seal",
                "seal",
                (name,),
            ),
        )
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
        ("FINALIZE_ARTIFACTS", 17, "publication_key_v1"),
    )
    expected_publication_stages = tuple(
        record
        for name, order, cursor_codec in publication_stage_values
        for record in (
            (
                f"catalog.publication-stage.{name.lower().replace('_', '-')}.v1.publication_stage_anchor",
                "anchor",
                (name,),
            ),
            (
                f"catalog.publication-stage.{name.lower().replace('_', '-')}.v1.publication_stage_order",
                "order",
                (name, f"{order:02d}"),
            ),
            (
                f"catalog.publication-stage.{name.lower().replace('_', '-')}.v1.publication_stage_cursor_codec",
                "cursor_codec",
                (name, cursor_codec),
            ),
            (
                f"catalog.publication-stage.{name.lower().replace('_', '-')}.v1.publication_stage_seal",
                "seal",
                (name,),
            ),
        )
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
        _validate_vertical_policy_provider_shapes(relation_by_name, backend=backend)
        expected_shapes = {
            "artifact_producer_fingerprint": (
                "catalog_artifact_producer_fingerprints",
                (
                    "producer_fingerprint_sha256",
                    "artifact_algorithm_version",
                    "producer_equivalence_class",
                    "writer_id",
                    "python_abi",
                    "pillow_build",
                    "libjpeg_build",
                    "zlib_build",
                ),
                ("producer_fingerprint_sha256",),
                (
                    ("producer_equivalence_class",),
                    (
                        "writer_id",
                        "python_abi",
                        "pillow_build",
                        "libjpeg_build",
                        "zlib_build",
                    ),
                ),
            ),
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
                    "artifact_sha256",
                    "storage_codec_version",
                    "storage_generation",
                    "protection_token",
                    "state",
                ),
                ("candidate_id", "publication_key"),
                (("protection_token",),),
            ),
            "artifact_location": (
                "catalog_artifact_location",
                ("artifact_sha256", "artifact_locator_sha256"),
                ("artifact_sha256",),
                (("artifact_locator_sha256",),),
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
    zip_policies = connector.fetch_all(
        "SELECT seal.artifact_algorithm_version, zip.zip_codec_version, "
        "method.compression_method, level.compression_level, dos_date_fact.dos_date, "
        "dos_time_fact.dos_time, mode.unix_mode, flags.general_purpose_flags, "
        "create_system_fact.create_system, archive.archive_name_codec_version, "
        "artifact.artifact_name_codec_version, identity.zip_codec_version, "
        "identity.compression_method, identity.compression_level, "
        "identity.dos_date, identity.dos_time, identity.unix_mode, "
        "identity.general_purpose_flags, identity.create_system, "
        "identity.archive_name_codec_version, "
        "identity.artifact_name_codec_version "
        "FROM catalog_artifact_zip_writer_policy_seals AS seal "
        "JOIN catalog_artifact_zip_writer_policy_zip_codec_versions AS zip "
        "ON zip.artifact_algorithm_version = seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_compression_methods AS method "
        "ON method.artifact_algorithm_version = seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_compression_levels AS level "
        "ON level.artifact_algorithm_version = seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_dos_dates AS dos_date_fact "
        "ON dos_date_fact.artifact_algorithm_version = seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_dos_times AS dos_time_fact "
        "ON dos_time_fact.artifact_algorithm_version = seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_unix_modes AS mode "
        "ON mode.artifact_algorithm_version = seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_general_purpose_flags AS flags "
        "ON flags.artifact_algorithm_version = seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_create_systems AS create_system_fact "
        "ON create_system_fact.artifact_algorithm_version = "
        "seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_archive_name_codec_versions "
        "AS archive ON archive.artifact_algorithm_version = "
        "seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_artifact_name_codec_versions "
        "AS artifact ON artifact.artifact_algorithm_version = "
        "seal.artifact_algorithm_version "
        "JOIN catalog_artifact_zip_writer_policy_identities AS identity "
        "ON identity.artifact_algorithm_version = seal.artifact_algorithm_version "
        "ORDER BY seal.artifact_algorithm_version LIMIT 2"
    )
    normalized_zip_policies = tuple(
        tuple(_as_int(cell, field="artifact ZIP policy") for cell in row)
        for row in zip_policies
    )
    expected_zip_policy = (1, 1, 8, 9, 33, 0, 33188, 2048, 3, 1, 1)
    if normalized_zip_policies != (expected_zip_policy + expected_zip_policy[1:],):
        raise CatalogSemanticValidationError(
            "artifact_zip_writer_policy is not the exact v1 singleton"
        )
    storage_codecs = connector.fetch_all(
        "SELECT seal.storage_codec_version, adapter.adapter_id, "
        "locator.locator_codec_version, token.protection_token_codec_version "
        "FROM catalog_artifact_storage_codec_seals AS seal "
        "JOIN catalog_artifact_storage_codec_adapter_ids AS adapter "
        "ON adapter.storage_codec_version = seal.storage_codec_version "
        "JOIN catalog_artifact_storage_codec_locator_codec_versions AS locator "
        "ON locator.storage_codec_version = seal.storage_codec_version "
        "JOIN catalog_artifact_storage_codec_protection_token_codec_versions "
        "AS token ON token.storage_codec_version = seal.storage_codec_version "
        "ORDER BY seal.storage_codec_version LIMIT 2"
    )
    if len(storage_codecs) != 1 or (
        _as_int(storage_codecs[0][0], field="storage codec version"),
        _as_bytes(storage_codecs[0][1], field="storage adapter id"),
        _as_int(storage_codecs[0][2], field="locator codec version"),
        _as_int(storage_codecs[0][3], field="protection token codec version"),
    ) != (1, b"managed-filesystem", 1, 1):
        raise CatalogSemanticValidationError(
            "artifact_storage_codec is not the exact managed-filesystem v1 singleton"
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
            ("FINALIZE_ARTIFACTS", 17, "publication_key_v1"),
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
        raise CatalogSemanticValidationError(
            "source_head vertical family is incomplete"
        )
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
            optional=True,
        )
        if provenance_row is None:
            # Publication commits and source descriptors are permanent; their
            # transient provenance/analysis/build graph may be removed by the
            # verified cleanup protocol.  Absence is therefore not corruption.
            contexts.append(
                _SourceContext(
                    channel,
                    revision,
                    generation,
                    snapshot_digest,
                    None,
                    None,
                    None,
                    None,
                )
            )
            continue
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
            "SELECT build_id FROM catalog_build_manifests WHERE build_id = %s LIMIT 2",
            (build_id,),
            detail="active sealed build manifest",
        )

        scope_row = _one(
            connector,
            """
            SELECT provider.source_provider, root.source_root_sha256,
                   policy.identity_policy_version,
                   identity.source_provider, identity.source_root_sha256,
                   identity.identity_policy_version
            FROM catalog_source_scope_seals AS seal
            JOIN catalog_source_scope_source_providers AS provider
              ON provider.scope_key = seal.scope_key
            JOIN catalog_source_scope_source_root_sha256s AS root
              ON root.scope_key = seal.scope_key
            JOIN catalog_source_scope_identity_policy_versions AS policy
              ON policy.scope_key = seal.scope_key
            JOIN catalog_source_scope_identities AS identity
              ON identity.scope_key = seal.scope_key
            WHERE seal.scope_key = %s
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
        identity_tuple = (
            _as_bytes(scope_row[3], field="source_scope_identity.source_provider"),
            _as_bytes(scope_row[4], field="source_scope_identity.source_root_sha256"),
            _as_int(
                scope_row[5],
                field="source_scope_identity.identity_policy_version",
                positive=True,
            ),
        )
        if identity_tuple != (provider, source_root, identity_policy_version):
            raise CatalogSemanticValidationError(
                "active source scope facts disagree with its natural identity"
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
            SELECT base.base_receipt_id, catalog.revision,
                   source.source_revision, generation.generation,
                   source_channel.channel
            FROM catalog_source_build_base_publication_commits AS base
            JOIN catalog_publication_commit_seals AS seal
              ON seal.receipt_id = base.base_receipt_id
            JOIN catalog_publication_commit_catalog_revisions AS catalog
              ON catalog.receipt_id = base.base_receipt_id
            JOIN catalog_publication_commit_source_revisions AS source
              ON source.receipt_id = base.base_receipt_id
            JOIN catalog_publication_commit_generations AS generation
              ON generation.receipt_id = base.base_receipt_id
            JOIN catalog_source_revision_channels AS source_channel
              ON source_channel.source_revision = source.source_revision
            WHERE base.build_id = %s
            LIMIT 2
            """,
            (build_id,),
            detail="active source build baseline",
            optional=True,
        )
        if base_row is None:
            if generation != 1:
                raise CatalogSemanticValidationError(
                    "non-genesis source_head lacks its build baseline"
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

    Each anti-join is rooted at one physical family member and constrained by
    ``analysis_id``.  The minimum-witness query deliberately probes provenance
    in ``(analysis_id, key, gallery_id)`` order so both supported backends can
    use the manifest-required key-first lookup index.
    """

    for (
        family,
        key,
        anchor_table,
        provenance_table,
        witness_table,
        seal_table,
    ) in _IMPACTED_KEY_FAMILIES:
        incomplete_anchor = connector.fetch_all(
            f"""
            SELECT anchor.{key}
            FROM {anchor_table} AS anchor
            WHERE anchor.analysis_id = %s
              AND (
                NOT EXISTS (
                  SELECT 1
                  FROM {provenance_table} AS provenance
                  WHERE provenance.analysis_id = anchor.analysis_id
                    AND provenance.{key} = anchor.{key}
                )
                OR NOT EXISTS (
                  SELECT 1
                  FROM {witness_table} AS witness
                  WHERE witness.analysis_id = anchor.analysis_id
                    AND witness.{key} = anchor.{key}
                )
                OR NOT EXISTS (
                  SELECT 1
                  FROM {seal_table} AS seal
                  WHERE seal.analysis_id = anchor.analysis_id
                    AND seal.{key} = anchor.{key}
                )
              )
            LIMIT 1
            """,
            (analysis_id,),
        )
        if incomplete_anchor:
            raise CatalogSemanticValidationError(
                f"sealed impacted-{family} anchor lacks provenance, witness, or seal"
            )

        orphan_provenance = connector.fetch_all(
            f"""
            SELECT provenance.{key}
            FROM {provenance_table} AS provenance
            WHERE provenance.analysis_id = %s
              AND NOT EXISTS (
                SELECT 1
                FROM {anchor_table} AS anchor
                WHERE anchor.analysis_id = provenance.analysis_id
                  AND anchor.{key} = provenance.{key}
              )
            LIMIT 1
            """,
            (analysis_id,),
        )
        if orphan_provenance:
            raise CatalogSemanticValidationError(
                f"sealed impacted-{family} provenance has no anchor"
            )

        incomplete_witness = connector.fetch_all(
            f"""
            SELECT witness.{key}
            FROM {witness_table} AS witness
            WHERE witness.analysis_id = %s
              AND (
                NOT EXISTS (
                  SELECT 1
                  FROM {anchor_table} AS anchor
                  WHERE anchor.analysis_id = witness.analysis_id
                    AND anchor.{key} = witness.{key}
                )
                OR NOT EXISTS (
                  SELECT 1
                  FROM {provenance_table} AS provenance
                  WHERE provenance.analysis_id = witness.analysis_id
                    AND provenance.{key} = witness.{key}
                    AND provenance.gallery_id = witness.witness_gallery_id
                )
              )
            LIMIT 1
            """,
            (analysis_id,),
        )
        if incomplete_witness:
            raise CatalogSemanticValidationError(
                f"sealed impacted-{family} witness lacks its anchor or provenance"
            )

        incomplete_seal = connector.fetch_all(
            f"""
            SELECT seal.{key}
            FROM {seal_table} AS seal
            WHERE seal.analysis_id = %s
              AND (
                NOT EXISTS (
                  SELECT 1
                  FROM {anchor_table} AS anchor
                  WHERE anchor.analysis_id = seal.analysis_id
                    AND anchor.{key} = seal.{key}
                )
                OR NOT EXISTS (
                  SELECT 1
                  FROM {witness_table} AS witness
                  WHERE witness.analysis_id = seal.analysis_id
                    AND witness.{key} = seal.{key}
                )
              )
            LIMIT 1
            """,
            (analysis_id,),
        )
        if incomplete_seal:
            raise CatalogSemanticValidationError(
                f"sealed impacted-{family} seal lacks its anchor or witness"
            )

        nonminimum_witness = connector.fetch_all(
            f"""
            SELECT witness.{key}
            FROM {witness_table} AS witness
            WHERE witness.analysis_id = %s
              AND NOT EXISTS (
                SELECT 1
                FROM {provenance_table} AS candidate
                WHERE candidate.analysis_id = witness.analysis_id
                  AND candidate.{key} = witness.{key}
                  AND candidate.gallery_id = witness.witness_gallery_id
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

    candidate_base = _base_commit_tuple(
        _one(
            connector,
            """
            SELECT base.base_receipt_id, catalog.revision,
                   source.source_revision, generation.generation,
                   source_channel.channel
            FROM catalog_publication_candidate_base_publication_commits AS base
            JOIN catalog_publication_commit_seals AS seal
              ON seal.receipt_id = base.base_receipt_id
            JOIN catalog_publication_commit_catalog_revisions AS catalog
              ON catalog.receipt_id = base.base_receipt_id
            JOIN catalog_publication_commit_source_revisions AS source
              ON source.receipt_id = base.base_receipt_id
            JOIN catalog_publication_commit_generations AS generation
              ON generation.receipt_id = base.base_receipt_id
            JOIN catalog_source_revision_channels AS source_channel
              ON source_channel.source_revision = source.source_revision
            WHERE base.candidate_id = %s
            LIMIT 2
            """,
            (candidate_id,),
            detail="publication candidate base source",
            optional=True,
        ),
        detail="publication candidate base commit",
    )
    build_base = _base_commit_tuple(
        _one(
            connector,
            """
            SELECT base.base_receipt_id, catalog.revision,
                   source.source_revision, generation.generation,
                   source_channel.channel
            FROM catalog_source_build_base_publication_commits AS base
            JOIN catalog_publication_commit_seals AS seal
              ON seal.receipt_id = base.base_receipt_id
            JOIN catalog_publication_commit_catalog_revisions AS catalog
              ON catalog.receipt_id = base.base_receipt_id
            JOIN catalog_publication_commit_source_revisions AS source
              ON source.receipt_id = base.base_receipt_id
            JOIN catalog_publication_commit_generations AS generation
              ON generation.receipt_id = base.base_receipt_id
            JOIN catalog_source_revision_channels AS source_channel
              ON source_channel.source_revision = source.source_revision
            WHERE base.build_id = %s
            LIMIT 2
            """,
            (build_id,),
            detail="publication analysis build base source",
            optional=True,
        ),
        detail="publication analysis build base commit",
    )
    if candidate_base != build_base:
        raise CatalogSemanticValidationError(
            "publication candidate base-receipt CAS differs from its analysis build"
        )

    if candidate_base is None:
        expected_source_generation = 1
    else:
        (
            _base_receipt_id,
            _base_catalog_revision,
            base_source_revision,
            base_generation,
            base_channel,
        ) = candidate_base
        if base_channel != channel or source_revision <= base_source_revision:
            raise CatalogSemanticValidationError(
                "publication source revision does not advance its candidate base commit"
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
            "publication source_head vertical family is incomplete"
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
    if (
        head_source_revision != source_revision
        or head_source_generation != expected_source_generation
    ):
        raise CatalogSemanticValidationError(
            "publication receipt does not match the active source-head CAS result"
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
               algorithm.artifact_algorithm_version,
               side.max_image_short_side,
               fingerprint.producer_fingerprint_sha256,
               producer.producer_equivalence_class,
               producer.writer_id, producer.python_abi,
               producer.pillow_build, producer.libjpeg_build,
               producer.zlib_build,
               identity.artifact_algorithm_version,
               identity.max_image_short_side,
               identity.producer_fingerprint_sha256
        FROM catalog_artifact_policies AS policy
        JOIN catalog_artifact_policy_semantics_seals AS seal
          ON seal.policy_component_sha256 = policy.policy_component_sha256
        JOIN catalog_artifact_policy_semantics_artifact_algorithm_versions AS algorithm
          ON algorithm.policy_component_sha256 = seal.policy_component_sha256
        JOIN catalog_artifact_policy_semantics_max_image_short_sides AS side
          ON side.policy_component_sha256 = seal.policy_component_sha256
        JOIN catalog_artifact_policy_semantics_producer_fingerprint_sha256s
             AS fingerprint
          ON fingerprint.policy_component_sha256 = seal.policy_component_sha256
        JOIN catalog_artifact_policy_semantics_identities AS identity
          ON identity.policy_component_sha256 = seal.policy_component_sha256
        JOIN catalog_artifact_producer_fingerprints AS producer
          ON producer.producer_fingerprint_sha256 =
             fingerprint.producer_fingerprint_sha256
         AND producer.artifact_algorithm_version =
             algorithm.artifact_algorithm_version
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
    max_short_side = _as_int(
        artifact_policy_row[2], field="artifact max short side", positive=True
    )
    producer_fingerprint = _as_bytes(
        artifact_policy_row[3], field="artifact producer fingerprint"
    )
    producer_fields = tuple(
        _as_bytes(value, field="artifact producer build field")
        for value in artifact_policy_row[5:10]
    )
    semantics_identity = (
        _as_int(
            artifact_policy_row[10],
            field="artifact semantics identity algorithm version",
            positive=True,
        ),
        _as_int(
            artifact_policy_row[11],
            field="artifact semantics identity max short side",
            positive=True,
        ),
        _as_bytes(
            artifact_policy_row[12],
            field="artifact semantics identity producer fingerprint",
        ),
    )
    if semantics_identity != (
        algorithm_version,
        max_short_side,
        producer_fingerprint,
    ):
        raise CatalogSemanticValidationError(
            "active artifact policy facts disagree with its natural identity"
        )
    if algorithm_version not in _SUPPORTED_ARTIFACT_ALGORITHM_VERSIONS:
        raise CatalogSemanticValidationError(
            "active artifact policy uses an unregistered runtime algorithm version"
        )
    if max_short_side > (1 << 32) - 1:
        raise CatalogSemanticValidationError(
            "active artifact policy resize bound exceeds uint32"
        )
    if (
        identity.artifact_producer_fingerprint_sha256(*producer_fields)
        != producer_fingerprint
    ):
        raise CatalogSemanticValidationError(
            "active artifact producer fingerprint does not match its exact build tuple"
        )
    producer_equivalence_class = _as_bytes(
        artifact_policy_row[4], field="artifact producer equivalence class"
    )
    if producer_equivalence_class != identity.artifact_producer_equivalence_class(
        producer_fingerprint
    ):
        raise CatalogSemanticValidationError(
            "active artifact producer equivalence class is not repository-certified"
        )
    if (
        identity.artifact_policy_digest(
            algorithm_version,
            max_short_side,
            producer_fingerprint,
        )
        != policy_component
    ):
        raise CatalogSemanticValidationError(
            "active artifact policy component does not match its exact tuple"
        )
    _require_canonical_domain(
        connector,
        policy_component,
        b"artifact_policy_v2",
        detail="active artifact policy component",
    )
    display_policy_row = _one(
        connector,
        """
        SELECT display_algorithm.display_title_algorithm_version,
               display_sort.title_sort_policy_id,
               sort_algorithm.title_sort_algorithm_version,
               sort_unicode.unicode_data_version,
               display_identity.display_title_algorithm_version,
               display_identity.title_sort_policy_id,
               sort_identity.title_sort_algorithm_version,
               sort_identity.unicode_data_version
        FROM catalog_display_title_policy_seals AS display_seal
        JOIN catalog_display_title_policy_algorithm_versions AS display_algorithm
          ON display_algorithm.display_title_policy_id =
             display_seal.display_title_policy_id
        JOIN catalog_display_title_policy_title_sort_policy_ids AS display_sort
          ON display_sort.display_title_policy_id =
             display_seal.display_title_policy_id
        JOIN catalog_display_title_policy_identities AS display_identity
          ON display_identity.display_title_policy_id =
             display_seal.display_title_policy_id
        JOIN catalog_title_sort_policy_seals AS sort_seal
          ON sort_seal.title_sort_policy_id = display_sort.title_sort_policy_id
        JOIN catalog_title_sort_policy_algorithm_versions AS sort_algorithm
          ON sort_algorithm.title_sort_policy_id = sort_seal.title_sort_policy_id
        JOIN catalog_title_sort_policy_unicode_data_versions AS sort_unicode
          ON sort_unicode.title_sort_policy_id = sort_seal.title_sort_policy_id
        JOIN catalog_title_sort_policy_identities AS sort_identity
          ON sort_identity.title_sort_policy_id = sort_seal.title_sort_policy_id
        WHERE display_seal.display_title_policy_id = %s
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
    title_sort_policy_id = _as_int(
        display_policy_row[1],
        field="display title sort policy",
        positive=True,
    )
    if not isinstance(policy_tuple[2], bytes):
        raise CatalogSemanticValidationError(
            "active Unicode data version is not strict bytes"
        )
    display_identity = (
        _as_int(
            display_policy_row[4],
            field="display title identity algorithm version",
            positive=True,
        ),
        _as_int(
            display_policy_row[5],
            field="display title identity sort policy",
            positive=True,
        ),
    )
    sort_identity = (
        _as_int(
            display_policy_row[6],
            field="title sort identity algorithm version",
            positive=True,
        ),
        display_policy_row[7],
    )
    if not isinstance(sort_identity[1], bytes):
        raise CatalogSemanticValidationError(
            "active Unicode identity version is not strict bytes"
        )
    if display_identity != (policy_tuple[0], title_sort_policy_id) or (
        sort_identity != policy_tuple[1:]
    ):
        raise CatalogSemanticValidationError(
            "active display/title-sort facts disagree with natural identities"
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
        SELECT checkpoint.generation, checkpoint.cursor,
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


def _active_publication_contexts(
    connector: SQLConnector,
) -> tuple[_PublicationContext, ...]:
    rows = connector.fetch_all("""
        SELECT registry.channel, head.revision, mapping.generation,
               advanced.advanced_at
        FROM catalog_channel_registry AS registry
        LEFT JOIN catalog_publication_head_revisions AS head
          ON head.channel = registry.channel
        LEFT JOIN catalog_revision_generations AS mapping
          ON mapping.revision = head.revision
        LEFT JOIN catalog_publication_head_advanced_ats AS advanced
          ON advanced.channel = registry.channel
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
            "publication_head vertical family is incomplete"
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
            SELECT publication_count
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
        if receipt_state not in {"DB_COMMITTED", "PROJECTION_FINALIZED"}:
            raise CatalogSemanticValidationError(
                "active publication receipt is not committed"
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
        finalized_count = _as_int(
            final_checkpoint[0], field="permanent finalization processed_count"
        )
        expected_final_state = (
            "COMPLETE" if receipt_state == "PROJECTION_FINALIZED" else "OPEN"
        )
        if (
            final_checkpoint[1] != expected_final_state
            or finalized_count > prepared_count
            or (
                expected_final_state == "COMPLETE" and finalized_count != prepared_count
            )
        ):
            raise CatalogSemanticValidationError(
                "permanent finalization checkpoint disagrees with prepared receipt count"
            )
        _validate_analysis_seal(connector, analysis_id)
        _validate_publication_candidate_source(
            connector,
            candidate_id=candidate_id,
            analysis_id=analysis_id,
            source_revision=source_revision,
            channel=channel,
        )

        base_catalog = _base_commit_tuple(
            _one(
                connector,
                """
            SELECT base.base_receipt_id, catalog.revision,
                   source.source_revision, generation.generation,
                   source_channel.channel
            FROM catalog_publication_candidate_base_publication_commits AS base
            JOIN catalog_publication_commit_seals AS seal
              ON seal.receipt_id = base.base_receipt_id
            JOIN catalog_publication_commit_catalog_revisions AS catalog
              ON catalog.receipt_id = base.base_receipt_id
            JOIN catalog_publication_commit_source_revisions AS source
              ON source.receipt_id = base.base_receipt_id
            JOIN catalog_publication_commit_generations AS generation
              ON generation.receipt_id = base.base_receipt_id
            JOIN catalog_source_revision_channels AS source_channel
              ON source_channel.source_revision = source.source_revision
            WHERE base.candidate_id = %s
            LIMIT 2
            """,
                (candidate_id,),
                detail="publication candidate base commit",
                optional=True,
            ),
            detail="publication candidate base commit",
        )
        if base_catalog is None:
            if generation != 1:
                raise CatalogSemanticValidationError(
                    "non-genesis publication_head lacks its candidate baseline"
                )
        else:
            (
                _base_receipt_id,
                base_revision,
                _base_source_revision,
                base_generation,
                base_channel,
            ) = base_catalog
            if (
                base_channel != channel
                or generation != base_generation + 1
                or revision <= base_revision
            ):
                raise CatalogSemanticValidationError(
                    "publication_head does not advance its exact candidate baseline"
                )

        artifact_policy_row = _one(
            connector,
            """
            SELECT policy.policy_component_sha256,
                   algorithm.artifact_algorithm_version,
                   side.max_image_short_side,
                   fingerprint.producer_fingerprint_sha256,
                   producer.producer_equivalence_class,
                   producer.writer_id, producer.python_abi,
                   producer.pillow_build, producer.libjpeg_build,
                   producer.zlib_build,
                   identity.artifact_algorithm_version,
                   identity.max_image_short_side,
                   identity.producer_fingerprint_sha256
            FROM catalog_artifact_policies AS policy
            JOIN catalog_artifact_policy_semantics_seals AS seal
              ON seal.policy_component_sha256 = policy.policy_component_sha256
            JOIN catalog_artifact_policy_semantics_artifact_algorithm_versions
                 AS algorithm
              ON algorithm.policy_component_sha256 = seal.policy_component_sha256
            JOIN catalog_artifact_policy_semantics_max_image_short_sides AS side
              ON side.policy_component_sha256 = seal.policy_component_sha256
            JOIN catalog_artifact_policy_semantics_producer_fingerprint_sha256s
                 AS fingerprint
              ON fingerprint.policy_component_sha256 = seal.policy_component_sha256
            JOIN catalog_artifact_policy_semantics_identities AS identity
              ON identity.policy_component_sha256 = seal.policy_component_sha256
            JOIN catalog_artifact_producer_fingerprints AS producer
              ON producer.producer_fingerprint_sha256 =
                 fingerprint.producer_fingerprint_sha256
             AND producer.artifact_algorithm_version =
                 algorithm.artifact_algorithm_version
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
        max_short_side = _as_int(
            artifact_policy_row[2], field="artifact max short side", positive=True
        )
        producer_fingerprint = _as_bytes(
            artifact_policy_row[3], field="artifact producer fingerprint"
        )
        producer_fields = tuple(
            _as_bytes(value, field="artifact producer build field")
            for value in artifact_policy_row[5:10]
        )
        semantics_identity = (
            _as_int(
                artifact_policy_row[10],
                field="artifact semantics identity algorithm version",
                positive=True,
            ),
            _as_int(
                artifact_policy_row[11],
                field="artifact semantics identity max short side",
                positive=True,
            ),
            _as_bytes(
                artifact_policy_row[12],
                field="artifact semantics identity producer fingerprint",
            ),
        )
        if semantics_identity != (
            algorithm_version,
            max_short_side,
            producer_fingerprint,
        ):
            raise CatalogSemanticValidationError(
                "active artifact policy facts disagree with its natural identity"
            )
        if algorithm_version not in _SUPPORTED_ARTIFACT_ALGORITHM_VERSIONS:
            raise CatalogSemanticValidationError(
                "active artifact policy uses an unregistered runtime algorithm version"
            )
        if max_short_side > (1 << 32) - 1:
            raise CatalogSemanticValidationError(
                "active artifact policy resize bound exceeds uint32"
            )
        if (
            identity.artifact_producer_fingerprint_sha256(*producer_fields)
            != producer_fingerprint
        ):
            raise CatalogSemanticValidationError(
                "active artifact producer fingerprint does not match its exact build tuple"
            )
        producer_equivalence_class = _as_bytes(
            artifact_policy_row[4], field="artifact producer equivalence class"
        )
        if producer_equivalence_class != identity.artifact_producer_equivalence_class(
            producer_fingerprint
        ):
            raise CatalogSemanticValidationError(
                "active artifact producer equivalence class is not repository-certified"
            )
        if (
            identity.artifact_policy_digest(
                algorithm_version,
                max_short_side,
                producer_fingerprint,
            )
            != policy_component
        ):
            raise CatalogSemanticValidationError(
                "active artifact policy component does not match its exact tuple"
            )
        _require_canonical_domain(
            connector,
            policy_component,
            b"artifact_policy_v2",
            detail="active artifact policy component",
        )
        display_policy_row = _one(
            connector,
            """
            SELECT display_algorithm.display_title_algorithm_version,
                   display_sort.title_sort_policy_id,
                   sort_algorithm.title_sort_algorithm_version,
                   sort_unicode.unicode_data_version,
                   display_identity.display_title_algorithm_version,
                   display_identity.title_sort_policy_id,
                   sort_identity.title_sort_algorithm_version,
                   sort_identity.unicode_data_version
            FROM catalog_display_title_policy_seals AS display_seal
            JOIN catalog_display_title_policy_algorithm_versions
                 AS display_algorithm
              ON display_algorithm.display_title_policy_id =
                 display_seal.display_title_policy_id
            JOIN catalog_display_title_policy_title_sort_policy_ids
                 AS display_sort
              ON display_sort.display_title_policy_id =
                 display_seal.display_title_policy_id
            JOIN catalog_display_title_policy_identities AS display_identity
              ON display_identity.display_title_policy_id =
                 display_seal.display_title_policy_id
            JOIN catalog_title_sort_policy_seals AS sort_seal
              ON sort_seal.title_sort_policy_id = display_sort.title_sort_policy_id
            JOIN catalog_title_sort_policy_algorithm_versions AS sort_algorithm
              ON sort_algorithm.title_sort_policy_id = sort_seal.title_sort_policy_id
            JOIN catalog_title_sort_policy_unicode_data_versions AS sort_unicode
              ON sort_unicode.title_sort_policy_id = sort_seal.title_sort_policy_id
            JOIN catalog_title_sort_policy_identities AS sort_identity
              ON sort_identity.title_sort_policy_id = sort_seal.title_sort_policy_id
            WHERE display_seal.display_title_policy_id = %s
            LIMIT 2
            """,
            (display_title_policy_id,),
            detail="active display/title-sort policy",
        )
        assert display_policy_row is not None
        display_algorithm_version = _as_int(
            display_policy_row[0],
            field="display title algorithm version",
            positive=True,
        )
        title_sort_algorithm_version = _as_int(
            display_policy_row[2],
            field="title sort algorithm version",
            positive=True,
        )
        unicode_data_version_raw = display_policy_row[3]
        if not isinstance(unicode_data_version_raw, bytes):
            raise CatalogSemanticValidationError(
                "active Unicode data version is not strict bytes"
            )
        unicode_data_version = unicode_data_version_raw
        display_identity = (
            _as_int(
                display_policy_row[4],
                field="display title identity algorithm version",
                positive=True,
            ),
            _as_int(
                display_policy_row[5],
                field="display title identity sort policy",
                positive=True,
            ),
        )
        sort_identity = (
            _as_int(
                display_policy_row[6],
                field="title sort identity algorithm version",
                positive=True,
            ),
            display_policy_row[7],
        )
        title_sort_policy_id = _as_int(
            display_policy_row[1],
            field="display title sort policy",
            positive=True,
        )
        if not isinstance(sort_identity[1], bytes):
            raise CatalogSemanticValidationError(
                "active Unicode identity version is not strict bytes"
            )
        if display_identity != (
            display_algorithm_version,
            title_sort_policy_id,
        ) or sort_identity != (
            title_sort_algorithm_version,
            unicode_data_version,
        ):
            raise CatalogSemanticValidationError(
                "active display/title-sort facts disagree with natural identities"
            )
        if (
            display_algorithm_version,
            title_sort_algorithm_version,
            unicode_data_version,
        ) not in _SUPPORTED_DISPLAY_TITLE_POLICY_ALGORITHMS:
            raise CatalogSemanticValidationError(
                "active display/title-sort policy uses an unsupported runtime "
                "algorithm/Unicode tuple"
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


def _validate_publication_generation_history(connector: SQLConnector) -> None:
    unsealed = connector.fetch_all("""
        SELECT anchor.receipt_id
        FROM catalog_publication_commit_anchors AS anchor
        LEFT JOIN catalog_publication_commit_seals AS seal
          ON seal.receipt_id = anchor.receipt_id
        WHERE seal.receipt_id IS NULL
        ORDER BY anchor.receipt_id
        LIMIT 2
        """)
    if unsealed:
        raise CatalogSemanticValidationError(
            "publication history contains an unsealed commit anchor"
        )

    commits = connector.fetch_all("""
        SELECT receipt_id, candidate_id, revision, source_revision, generation,
               committed_at
        FROM catalog_publication_commits
        ORDER BY generation
        """)
    sealed = connector.fetch_all("""
        SELECT receipt_id
        FROM catalog_publication_commit_seals
        ORDER BY receipt_id
        """)
    commit_receipts = {
        _as_bytes(row[0], field="publication chain receipt_id") for row in commits
    }
    sealed_receipts = {
        _as_bytes(row[0], field="publication seal receipt_id") for row in sealed
    }
    if commit_receipts != sealed_receipts or len(commit_receipts) != len(commits):
        raise CatalogSemanticValidationError(
            "publication seal set differs from complete common commits"
        )

    expected_generations = tuple(range(1, len(commits) + 1))
    actual_generations: list[int] = []
    for row in commits:
        if len(row) != 6:
            raise CatalogSemanticValidationError(
                "publication commit history row is malformed"
            )
        receipt_id = _as_bytes(row[0], field="publication chain receipt_id")
        candidate_id = _as_bytes(row[1], field="publication chain candidate_id")
        if len(receipt_id) != 16 or len(candidate_id) != 16:
            raise CatalogSemanticValidationError(
                "publication chain UUID identity is not 16 bytes"
            )
        _as_int(row[2], field="publication chain revision", positive=True)
        _as_int(row[3], field="publication chain source revision", positive=True)
        actual_generations.append(
            _as_int(row[4], field="publication chain generation", positive=True)
        )
        _as_int(row[5], field="publication chain committed_at")
    if tuple(actual_generations) != expected_generations:
        raise CatalogSemanticValidationError(
            "sealed publication commit generations are not contiguous from one"
        )

    nodes = tuple(
        _as_int(row[0], field="publication generation node")
        for row in connector.fetch_all("""
            SELECT generation
            FROM catalog_publication_generation_nodes
            ORDER BY generation
            """)
    )
    if nodes != tuple(range(0, len(commits) + 1)):
        raise CatalogSemanticValidationError(
            "publication generation nodes differ from genesis plus sealed commits"
        )
    edges = tuple(
        (
            _as_int(row[0], field="publication successor generation", positive=True),
            _as_int(row[1], field="publication predecessor generation"),
        )
        for row in connector.fetch_all("""
            SELECT successor_generation, predecessor_generation
            FROM catalog_publication_generation_successors
            ORDER BY successor_generation
            """)
    )
    if edges != tuple(
        (generation, generation - 1) for generation in expected_generations
    ):
        raise CatalogSemanticValidationError(
            "publication generation successor chain is gapped or forked"
        )

    orphan_source_descriptors = connector.fetch_all("""
        SELECT descriptor.source_revision
        FROM catalog_source_revision_descriptor_seals AS descriptor
        LEFT JOIN catalog_publication_commit_source_revisions AS member
          ON member.source_revision = descriptor.source_revision
        LEFT JOIN catalog_publication_commit_seals AS seal
          ON seal.receipt_id = member.receipt_id
        WHERE seal.receipt_id IS NULL
        ORDER BY descriptor.source_revision
        LIMIT 2
        """)
    if orphan_source_descriptors:
        raise CatalogSemanticValidationError(
            "sealed source descriptor lacks its sealed publication commit"
        )

    finalization_rows = connector.fetch_all("""
        SELECT committed.receipt_id, committed.committed_at,
               checkpoint.generation, checkpoint.cursor,
               checkpoint.processed_count, checkpoint.state,
               checkpoint.updated_at, marker.receipt_id
        FROM catalog_publication_commit_seals AS commit_seal
        JOIN catalog_publication_commit_committed_ats AS committed
          ON committed.receipt_id = commit_seal.receipt_id
        JOIN catalog_publication_finalization_checkpoints AS checkpoint
          ON checkpoint.receipt_id = commit_seal.receipt_id
        LEFT JOIN catalog_publication_commit_finalizations AS marker
          ON marker.receipt_id = commit_seal.receipt_id
        ORDER BY committed.receipt_id
        """)
    if len(finalization_rows) != len(commits):
        raise CatalogSemanticValidationError(
            "sealed publication commits and permanent finalization checkpoints differ"
        )
    for row in finalization_rows:
        if len(row) != 8:
            raise CatalogSemanticValidationError(
                "permanent finalization checkpoint row is malformed"
            )
        receipt_id = _as_bytes(row[0], field="finalization receipt_id")
        commit_time = _as_int(row[1], field="publication commit time")
        generation = _as_int(
            row[2], field="finalization checkpoint generation", positive=True
        )
        cursor = _as_bytes(row[3], field="finalization checkpoint cursor")
        processed_count = _as_int(
            row[4], field="finalization checkpoint processed_count"
        )
        state = row[5]
        updated_at = _as_int(row[6], field="finalization checkpoint updated_at")
        marker_present = row[7] is not None
        if updated_at < commit_time or state not in {"OPEN", "COMPLETE"}:
            raise CatalogSemanticValidationError(
                "permanent finalization checkpoint precedes its commit or has "
                "an invalid state"
            )
        latest = _one(
            connector,
            """
            SELECT start_cursor, start_processed_count, next_cursor,
                   next_processed_count, next_state, row_count, terminal,
                   committed_generation, committed_at
            FROM catalog_publication_finalization_batch_receipts
            WHERE receipt_id = %s AND committed_generation = %s
            LIMIT 2
            """,
            (receipt_id, generation),
            detail="latest permanent finalization receipt",
            optional=True,
        )
        if generation == 1:
            if latest is not None or state != "OPEN" or marker_present:
                raise CatalogSemanticValidationError(
                    "initial permanent finalization checkpoint is not exact OPEN genesis"
                )
            continue
        if latest is None:
            raise CatalogSemanticValidationError(
                "advanced permanent finalization checkpoint lacks its exact receipt"
            )
        start_cursor = _as_bytes(latest[0], field="finalization start_cursor")
        start_count = _as_int(latest[1], field="finalization start_processed_count")
        next_cursor = _as_bytes(latest[2], field="finalization next_cursor")
        next_count = _as_int(latest[3], field="finalization next_processed_count")
        next_state = latest[4]
        row_count = _as_int(latest[5], field="finalization row_count")
        terminal = _as_int(latest[6], field="finalization terminal")
        committed_generation = _as_int(
            latest[7], field="finalization committed_generation", positive=True
        )
        receipt_time = _as_int(latest[8], field="finalization committed_at")
        if (
            next_cursor != cursor
            or next_count != processed_count
            or committed_generation != generation
            or receipt_time != updated_at
        ):
            raise CatalogSemanticValidationError(
                "latest permanent finalization receipt and checkpoint are incongruent"
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
                    "OPEN finalization checkpoint has marker or nonterminal receipt drift"
                )
        elif (
            not marker_present
            or terminal != 1
            or row_count != 0
            or next_state != "COMPLETE"
            or start_cursor != next_cursor
            or start_count != next_count
        ):
            raise CatalogSemanticValidationError(
                "COMPLETE finalization checkpoint lacks its exact terminal marker/receipt"
            )

    heads = connector.fetch_all("""
        SELECT registry.channel, head.receipt_id, generation.generation,
               source_channel.channel
        FROM catalog_channel_registry AS registry
        LEFT JOIN catalog_publication_commit_head_receipts AS head
          ON head.channel = registry.channel
        LEFT JOIN catalog_publication_commit_generations AS generation
          ON generation.receipt_id = head.receipt_id
        LEFT JOIN catalog_publication_commit_source_revisions AS source
          ON source.receipt_id = head.receipt_id
        LEFT JOIN catalog_source_revision_channels AS source_channel
          ON source_channel.source_revision = source.source_revision
        ORDER BY registry.channel
        LIMIT 2
        """)
    if len(heads) != 1 or _as_bytes(heads[0][0], field="head channel") != b"default":
        raise CatalogSemanticValidationError(
            "common publication head exceeds the channel registry"
        )
    if not commits:
        if any(value is not None for value in heads[0][1:]):
            raise CatalogSemanticValidationError(
                "genesis publication catalog unexpectedly has a common head"
            )
        return
    if any(value is None for value in heads[0][1:]):
        raise CatalogSemanticValidationError(
            "common publication head family is incomplete"
        )
    head_receipt = _as_bytes(heads[0][1], field="head receipt_id")
    head_generation = _as_int(heads[0][2], field="head generation", positive=True)
    head_source_channel = _as_bytes(heads[0][3], field="head source channel")
    if (
        head_receipt != _as_bytes(commits[-1][0], field="tip receipt_id")
        or head_generation != len(commits)
        or head_source_channel != b"default"
    ):
        raise CatalogSemanticValidationError(
            "common publication head does not name the unique maximum chain tip"
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
    if identity.artifact_policy_digest(1, 2048, bytes((3,)) * 32).hex() != (
        "055021f55a25bb338b14aa4423b3fee9f8f87ff9ea442e4283ae89db88f47a60"
    ):
        raise CatalogSemanticValidationError("artifact policy codec drifted")
    producer = identity.artifact_producer_fingerprint_sha256(
        b"writer", b"cp314", b"pillow", b"libjpeg", b"zlib"
    )
    if producer.hex() != (
        "7c12521923b06e72b031807d2d2d82b5bee38afafd408595b5d29ed31cfe892c"
    ):
        raise CatalogSemanticValidationError("artifact producer codec drifted")
    if identity.artifact_name(7) != b"h2h-7.cbz":
        raise CatalogSemanticValidationError("artifact name codec drifted")
    locator_components = identity.artifact_locator_components(bytes((0xAA,)) * 32)
    if locator_components != (
        "sha256",
        "aa",
        "a" * 64 + ".cbz",
    ) or identity.artifact_locator_digest(locator_components).hex() != (
        "1400187c75cbf5721168d71ac3a15ad2d5decca92ce4dbf2c77c036b32f21f57"
    ):
        raise CatalogSemanticValidationError("artifact locator codec drifted")
    protection = identity.encode_artifact_protection_token(
        1,
        bytes((0x11,)) * 16,
        bytes((0x22,)) * 32,
        bytes((0x33,)) * 32,
        bytes((0x44,)) * 32,
        7,
        9,
    )
    if (
        len(protection) != 184
        or identity.decode_artifact_protection_token(protection).receipt_id.hex()
        != "be24a65b2ded7965b31c3c317bc61cbf"
    ):
        raise CatalogSemanticValidationError("artifact protection codec drifted")
    if identity.artifact_semantics_digest(
        *(bytes((value,)) * 32 for value in range(1, 7))
    ).hex() != ("24e1140357d6956ded50b48db8ee90171c7eff0b1179c4cf3636cfaf3dda2047"):
        raise CatalogSemanticValidationError("artifact six-component codec drifted")
    zip_comment = identity.encode_zip_comment(bytes((1,)) * 32, bytes((3,)) * 32)
    if sha256(zip_comment).hexdigest() != (
        "3acf99d73b12b308c807b543d62d43941cf8a530b0fadfc915bf735d614b59d0"
    ):
        raise CatalogSemanticValidationError("artifact ZIP-comment envelope drifted")
    member_plan = bytes.fromhex(
        "68326864622d766e6578742d61727469666163742d6d656d6265722d706c616e000000000100000000000000020000000000000000000000000f67616c6c657279696e666f2e747874000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f000000000000007b0000010000001e303030303030303030303030303030305f5f6d657461646174612e7478740000000000000000010000000009494d4147452e474946fffefdfcfbfaf9f8f7f6f5f4f3f2f1f0efeeedecebeae9e8e7e6e5e4e3e2e1e0000000000000000001010001"
    )
    entries = identity.decode_artifact_member_plan(member_plan)
    if identity.encode_artifact_member_plan(entries) != member_plan:
        raise CatalogSemanticValidationError(
            "artifact member-plan closed enum codec drifted"
        )


def check_identity_codecs_v1(connector: SQLConnector) -> None:
    """Validate executable codec vectors and active scalar identities."""

    _validate_exact_registries(connector)
    _validate_identity_codec_vectors()
    _active_source_contexts(connector)


def check_canonical_reference_domains_v1(connector: SQLConnector) -> None:
    """Validate the closed domain registry and active canonical references."""

    _validate_exact_registries(connector)
    _active_source_contexts(connector)
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


def check_state_machines_v1(connector: SQLConnector) -> None:
    """Validate active state-machine terminals and their exact seal markers."""

    _validate_exact_registries(connector)
    for context in _active_source_contexts(connector):
        if context.analysis_id is not None:
            _validate_analysis_seal(connector, context.analysis_id)
    _active_publication_contexts(connector)


def check_role_derivation_v1(connector: SQLConnector) -> None:
    """Validate the executable exact-name role classifier and file-key codec."""

    _validate_exact_registries(connector)
    _validate_identity_codec_vectors()
    if (
        identity.file_role(b"galleryinfo.txt") != b"METADATA"
        or identity.file_role(b"GalleryInfo.txt") != b"CONTENT"
        or identity.file_role(b"image.jpg") != b"CONTENT"
    ):
        raise CatalogSemanticValidationError("exact file-role classifier drifted")


def check_physical_domains_v1(connector: SQLConnector) -> None:
    """Validate executable byte bounds plus bounded active scalar domains."""

    _validate_exact_registries(connector)
    if (
        identity.CANONICAL_VALUE_PAGE_MAXIMUM_BYTES != 65536
        or identity.CANONICAL_VALUE_CHUNK_BYTES != 32768
        or identity.GALLERY_OBSERVATION_PAGE_MAXIMUM_BYTES != 65536
        or identity.FILESYSTEM_STAT_FINGERPRINT_BYTES != 40
        or identity.ARTIFACT_LOCATOR_MAXIMUM_BYTES != 4096
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


def check_retention_contract_v1(connector: SQLConnector) -> None:
    """Validate bounded active-head retention blockers and seal reachability."""

    _validate_exact_registries(connector)
    source_contexts = _active_source_contexts(connector)
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
            "catalog.artifact-semantics.v1": check_artifact_semantics_v1,
            "catalog.publication-atomicity.v1": check_publication_atomicity_v1,
            "catalog.state-machines.v1": check_state_machines_v1,
            "catalog.role-derivation.v1": check_role_derivation_v1,
            "catalog.physical-domains.v1": check_physical_domains_v1,
            "catalog.retention.v1": check_retention_contract_v1,
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
