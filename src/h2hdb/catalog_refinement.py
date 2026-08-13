"""Fail-closed manifest boundary for future vNext semantic validators.

The generated schema declares 26 machine obligations.  Their production
repository checks are not wired yet, so this module validates only the exact
wheel manifest and refuses to manufacture validators from names, sample rows,
or caller assertions.  Structural and bootstrap validation remain implemented
by :mod:`h2hdb.vnext_schema_provider` and :mod:`h2hdb.schema_epoch`.
"""

from __future__ import annotations

__all__ = [
    "BuiltinSemanticRegistryError",
    "builtin_semantic_validators",
    "validate_builtin_semantic_manifest",
]

from collections.abc import Callable, Mapping
from typing import Any, NoReturn

from ._generated_vnext_schema import ARTIFACT
from .catalog_writer import validate_artifact_writer_manifest
from .sql_connector import SQLConnector

type SemanticValidator = Callable[[SQLConnector], None]


class BuiltinSemanticRegistryError(RuntimeError):
    """The generated obligations lack exact installed production validators."""


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


def builtin_semantic_validators() -> NoReturn:
    """Refuse READY until each obligation has a real repository implementation."""

    validate_builtin_semantic_manifest()
    raise BuiltinSemanticRegistryError(
        "production vNext semantic validators and writer callsites are not wired"
    )
