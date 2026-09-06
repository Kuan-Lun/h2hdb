"""Durable completion-marker bindings and constant-size observation reuse.

The source adapter owns the promise that a stable completion marker denotes an
unchanged completed gallery. This repository binds that promise to one already
verified immutable observation; no marker digest creates observation authority.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .domain import VNextSourceCompletionMarker
from .vnext_canonical_value_repository import (
    load_and_validate_single_page_canonical_values,
)
from .vnext_catalog_identity_family import load_gallery_identities
from .vnext_domains import require_uuid16
from .vnext_gallery_staging_repository import (
    GalleryStagingSeal,
    _authorize_outer,
    _lock_and_require_working_build,
)
from .vnext_identity import (
    gallery_key,
    iter_source_relative_locator_payload,
    iter_source_root_payload,
    source_relative_locator_digest,
    source_root_digest,
    source_scope_key,
)
from .vnext_ingest_fence_repository import IngestTurn
from .vnext_maintenance_gate_repository import GateLease
from .vnext_manifest_family import (
    ensure_gallery_manifest_family,
    load_gallery_manifest_family,
    load_source_build_family,
)
from .vnext_source_marker_family import (
    CachedSourceObservation,
    SourceMarkerConflictError,
    SourceMarkerUnavailableError,
    _compare_canonical,
    _load_cached,
    _load_cached_batch,
)
from .vnext_transaction import VNextUnitOfWork

_BINDING = "catalog_gallery_observation_completion_marker"

__all__ = [
    "CachedSourceObservation",
    "SourceMarkerConflictError",
    "SourceMarkerRepository",
    "SourceMarkerUnavailableError",
]


class SourceMarkerRepository:
    @staticmethod
    def lookup(
        connector: Any,
        *,
        source_root_components: tuple[str, ...],
        locator_components: tuple[str, ...],
        marker: VNextSourceCompletionMarker,
    ) -> CachedSourceObservation | None:
        """Load the newest retained binding by exact source identity and marker."""

        return SourceMarkerRepository.lookup_batch(
            connector,
            source_root_components=source_root_components,
            probes=((locator_components, marker),),
        )[0]

    @staticmethod
    def lookup_batch(
        connector: Any,
        *,
        source_root_components: tuple[str, ...],
        probes: tuple[tuple[tuple[str, ...], VNextSourceCompletionMarker], ...],
    ) -> tuple[CachedSourceObservation | None, ...]:
        """Resolve at most 128 probes, validating the shared root only once."""

        if type(probes) is not tuple or len(probes) > 128:
            raise ValueError("source marker lookup accepts at most 128 probes")
        if not probes:
            return ()
        for _locator, marker in probes:
            marker.__post_init__()
        root = source_root_digest(source_root_components)
        scope = source_scope_key("filesystem", root, 1)
        scope_row = connector.fetch_one(
            "SELECT source_provider, source_root_sha256, identity_policy_version "
            "FROM catalog_source_scopes WHERE scope_key = %s",
            (scope,),
        )
        if not scope_row:
            return (None,) * len(probes)
        if scope_row != (b"filesystem", root, 1):
            raise SourceMarkerConflictError("source marker scope identity differs")
        _compare_canonical(
            connector,
            root,
            b"source_root_v1",
            iter_source_root_payload(source_root_components),
        )
        locators = tuple(
            source_relative_locator_digest("source_relative_locator_v1", parts)
            for parts, _marker in probes
        )
        slots = ", ".join("%s" for _ in locators)
        rows = connector.fetch_all(
            "SELECT identity.locator_sha256, identity.gallery_id, identity.gallery_key, "
            "binding.observation_id, binding.file_key "
            "FROM catalog_gallery_identities AS identity "
            f"LEFT JOIN {_BINDING} AS binding ON binding.gallery_id = identity.gallery_id "
            f"AND binding.observation_id = (SELECT latest.observation_id FROM {_BINDING} AS latest "
            "WHERE latest.gallery_id = identity.gallery_id ORDER BY latest.observation_id DESC LIMIT 1) "
            f"WHERE identity.scope_key = %s AND identity.locator_sha256 IN ({slots})",
            (scope, *locators),
        )
        by_locator = {row[0]: row for row in rows}
        if len(by_locator) != len(rows):
            raise SourceMarkerConflictError("source marker gallery natural key repeats")
        bound_rows = tuple(row for row in rows if row[3] is not None)
        cached_observations = _load_cached_batch(
            connector, bindings=tuple((row[1], row[3], row[4]) for row in bound_rows)
        )
        locator_payloads = load_and_validate_single_page_canonical_values(
            connector,
            references=tuple(
                (row[0], b"source_relative_locator_v1") for row in bound_rows
            ),
        )
        result: list[CachedSourceObservation | None] = []
        for locator, (parts, marker) in zip(locators, probes, strict=True):
            row = by_locator.get(locator)
            if row is None:
                result.append(None)
                continue
            if len(row) != 5 or row[2] != gallery_key(scope, locator):
                raise SourceMarkerConflictError(
                    "source marker gallery identity differs"
                )
            if row[3] is None:
                result.append(None)
                continue
            payload = locator_payloads.get((locator, b"source_relative_locator_v1"))
            if payload is None:
                _compare_canonical(
                    connector,
                    locator,
                    b"source_relative_locator_v1",
                    iter_source_relative_locator_payload(parts),
                )
            else:
                _compare_payload_parts(
                    payload, iter_source_relative_locator_payload(parts)
                )
            cached = cached_observations.get((row[1], row[3]))
            if cached is None:
                raise SourceMarkerConflictError("retained marker binding disappeared")
            result.append(cached if cached.marker == marker else None)
        return tuple(result)

    @staticmethod
    def reuse(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        ingest_turn: IngestTurn,
        build_id: bytes,
        cached: CachedSourceObservation,
        now: int,
    ) -> GalleryStagingSeal:
        """Link exact durable authority without replaying observation pages."""

        if type(cached) is not CachedSourceObservation:
            raise TypeError("cached must be an issued CachedSourceObservation")
        cached.__post_init__()
        build_id = require_uuid16(build_id, field="marker reuse build_id")
        generation = _authorize_outer(work, gate_lease, ingest_turn, now=now)
        scope, _state = _lock_and_require_working_build(
            work, generation=generation, build_id=build_id
        )
        _require_member(work.connector, build_id, cached.gallery_id, scope)
        durable = _load_cached(work.connector, cached.gallery_id, cached.observation_id)
        if durable is None:
            raise SourceMarkerUnavailableError(
                "cached observation is no longer retained"
            )
        if durable != cached:
            raise SourceMarkerConflictError(
                "cached observation differs from durable authority"
            )
        if work.connector.fetch_one(
            "SELECT 1 FROM operational_gallery_observation_stagings WHERE build_id = %s LIMIT 1",
            (build_id,),
        ):
            raise SourceMarkerConflictError(
                "marker reuse cannot bypass pending staging"
            )
        existing = work.connector.fetch_one(
            "SELECT observation_id FROM catalog_source_build_galleries WHERE build_id = %s AND gallery_id = %s",
            (build_id, cached.gallery_id),
        )
        if existing and existing != (cached.observation_id,):
            raise SourceMarkerConflictError(
                "marker reuse conflicts with source membership"
            )
        build = load_source_build_family(work.connector, build_id=build_id)
        if build is None:
            raise SourceMarkerUnavailableError("marker reuse build disappeared")
        if (
            existing
            and load_gallery_manifest_family(
                work.connector,
                gallery_id=cached.gallery_id,
                observation_id=cached.observation_id,
                manifest_policy_id=build.manifest_policy_id,
            )
            is None
        ):
            raise SourceMarkerConflictError(
                "replayed marker reuse lost its gallery manifest"
            )
        ensure_gallery_manifest_family(
            work,
            gallery_id=cached.gallery_id,
            observation_id=cached.observation_id,
            manifest_policy_id=build.manifest_policy_id,
        )
        if not existing:
            work.connector.execute(
                "INSERT INTO catalog_source_build_galleries (build_id, gallery_id, observation_id) VALUES (%s, %s, %s)",
                (build_id, cached.gallery_id, cached.observation_id),
            )
        return GalleryStagingSeal(
            build_id,
            cached.gallery_id,
            cached.observation_id,
            cached.observation_identity_sha256,
            "REUSED",
            bool(existing),
        )


def _compare_payload_parts(payload: bytes, parts: Iterable[bytes]) -> None:
    offset = 0
    for part in parts:
        end = offset + len(part)
        if end > len(payload) or payload[offset:end] != part:
            raise SourceMarkerConflictError("source canonical preimage differs")
        offset = end
    if offset != len(payload):
        raise SourceMarkerConflictError("source canonical preimage EOF differs")


def _require_member(
    connector: Any, build_id: bytes, gallery_id: int, scope: bytes
) -> None:
    identities = load_gallery_identities(connector, gallery_ids=(gallery_id,))
    identity = identities.get(gallery_id)
    if (
        identity is None
        or identity.scope_key != scope
        or not connector.fetch_one(
            "SELECT 1 FROM catalog_source_build_expected_gallery WHERE build_id = %s AND gallery_id = %s",
            (build_id, gallery_id),
        )
    ):
        raise SourceMarkerConflictError(
            "marker gallery is outside the exact source build"
        )
