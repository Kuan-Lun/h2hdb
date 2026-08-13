"""Closed-world registry for vNext transaction writer hooks.

The formal contract names these hooks, but the production vNext repositories
do not exist yet.  This module therefore validates only the generated
name/version manifest and deliberately refuses dispatch.  A generic callback
that trusts caller-supplied booleans or projections would be a READY bypass,
not executable refinement evidence.
"""

from __future__ import annotations

__all__ = [
    "BUILTIN_WRITER_HOOKS",
    "WriterHook",
    "WriterHookUnavailableError",
    "resolve_writer_hook",
    "validate_artifact_writer_manifest",
]

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, NoReturn


class WriterHookUnavailableError(RuntimeError):
    """A formal writer hook has no production transaction implementation."""


@dataclass(frozen=True, slots=True)
class WriterHook:
    obligation_id: str
    name: str
    version: int


_SPECS: tuple[tuple[str, str], ...] = (
    ("catalog.identity-codecs.v1", "catalog_writer.validate_identity_codecs"),
    (
        "catalog.canonical-reference-domains.v1",
        "catalog_writer.validate_canonical_reference_domain",
    ),
    (
        "catalog.source-baseline-channel.v1",
        "catalog_writer.validate_source_baseline_channel_cas",
    ),
    (
        "catalog.incremental-impact.v1",
        "catalog_writer.validate_incremental_impact_freeze",
    ),
    (
        "catalog.overlay-resolution-seal.v1",
        "catalog_writer.validate_overlay_component_transition",
    ),
    (
        "catalog.artifact-semantics.v1",
        "catalog_writer.validate_artifact_semantics",
    ),
    (
        "catalog.publication-atomicity.v1",
        "catalog_writer.validate_publication_transition",
    ),
    ("catalog.state-machines.v1", "catalog_writer.validate_state_transition"),
    ("catalog.role-derivation.v1", "catalog_writer.validate_file_role"),
    ("catalog.physical-domains.v1", "catalog_writer.validate_physical_domain"),
    ("catalog.bootstrap.v1", "schema_epoch.write_catalog_bootstrap"),
    (
        "catalog.retention.v1",
        "catalog_writer.validate_retention_transition",
    ),
    (
        "h2hdb.operational.physical-domains.v1",
        "operational_writer.validate_physical_domains",
    ),
    (
        "h2hdb.operational.epoch-manifest.v1",
        "schema_epoch.validate_operational_manifest",
    ),
    (
        "h2hdb.operational.fencing.v1",
        "operational_writer.validate_ingest_fencing",
    ),
    (
        "h2hdb.operational.maintenance-gate.v1",
        "operational_writer.validate_maintenance_gate",
    ),
    (
        "h2hdb.operational.bounded-work.v1",
        "operational_writer.validate_bounded_work",
    ),
    (
        "h2hdb.operational.queue-history.v1",
        "operational_writer.validate_queue_history",
    ),
    (
        "h2hdb.operational.canonical-hash-cache.v1",
        "operational_writer.validate_canonical_hash_cache",
    ),
    (
        "h2hdb.operational.event-integrity.v1",
        "operational_writer.validate_event_integrity",
    ),
    (
        "h2hdb.operational.build-generation.v1",
        "operational_writer.validate_build_generation",
    ),
    (
        "h2hdb.operational.attempt-identity.v1",
        "operational_writer.validate_attempt_identity",
    ),
    (
        "h2hdb.operational.cleanup-reachability.v1",
        "operational_writer.validate_cleanup_reachability",
    ),
    (
        "h2hdb.operational.revision-allocation.v1",
        "operational_writer.validate_revision_allocation",
    ),
    (
        "h2hdb.operational.gallery-staging.v1",
        "operational_writer.validate_gallery_staging",
    ),
    (
        "h2hdb.operational.bootstrap-genesis.v1",
        "schema_epoch.write_operational_bootstrap",
    ),
)

BUILTIN_WRITER_HOOKS = tuple(
    WriterHook(obligation_id, name, 1) for obligation_id, name in _SPECS
)


def resolve_writer_hook(name: str, version: int) -> NoReturn:
    """Fail closed until the named hook is wired at a real repository callsite."""

    known = any(
        hook.name == name and hook.version == version for hook in BUILTIN_WRITER_HOOKS
    )
    detail = "known but not wired" if known else "unknown"
    raise WriterHookUnavailableError(
        f"vNext writer hook {name!r} v{version} is {detail}; production dispatch "
        "is unavailable"
    )


def validate_artifact_writer_manifest(obligations: Sequence[Mapping[str, Any]]) -> None:
    """Require exact generated IDs, hook names, versions, and ordering."""

    expected = tuple(
        (hook.obligation_id, hook.name, hook.version) for hook in BUILTIN_WRITER_HOOKS
    )
    actual: list[tuple[str, str, int]] = []
    for value in obligations:
        contract = value.get("contract")
        if not isinstance(contract, Mapping):
            raise WriterHookUnavailableError(
                "generated semantic obligation lacks contract payload"
            )
        obligation_id = value.get("id")
        name = contract.get("writer_hook")
        version = contract.get("writer_hook_version")
        if (
            not isinstance(obligation_id, str)
            or not isinstance(name, str)
            or not isinstance(version, int)
            or isinstance(version, bool)
        ):
            raise WriterHookUnavailableError(
                "generated writer-hook manifest record is malformed"
            )
        actual.append((obligation_id, name, version))
    if tuple(actual) != expected:
        raise WriterHookUnavailableError(
            "generated writer-hook manifest differs from the wheel registry"
        )
