"""Resume one sealed working source cut before looking for newer source work.

Preparation checks immutable qualification rows in bounded read transactions.
The short commit only rechecks scalar authority and maps the new fenced turn to
that same cut. No filesystem bytes, caller-selected build, or new schema is used.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from .database_performance import database_phase
from .domain import (
    SourceBatchBaseline,
    VNextIngestSourceReceipt,
    VNextResolvedIngestPolicy,
)
from .repository import RepositoryContext
from .source_errors import VNextSourceChangedError
from .sql_connector import SQLConnector
from .vnext_canonical_value_repository import _authorize
from .vnext_catalog_registry_repository import load_source_scope
from .vnext_domains import (
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from .vnext_identity import (
    iter_source_root_payload,
    source_root_digest,
    source_scope_key,
)
from .vnext_ingest_fence_repository import IngestTurn
from .vnext_ingest_policy_repository import VNextIngestPolicyRepository
from .vnext_maintenance_gate_repository import GateLease
from .vnext_source_batch_repository import _head_receipt, _published_build
from .vnext_source_build_repository import (
    SourceBuildConflictError,
    SourceBuildManifestSummary,
    _load_build_manifest_summary,
    _load_source_build_or_conflict,
    _lock_source_head,
    _require_checkpoint_pair,
    _require_latest_source_generation_authority,
    _require_locked_working_root_coherence,
    _sole_analysis_of_build,
    _SourceBuildPolicyAuthority,
    _validate_build_base_source,
    require_source_build_publication_identity,
    working_build_policy_matches,
)
from .vnext_source_marker_family import _compare_canonical
from .vnext_transaction import LockRank, VNextUnitOfWork, encode_lock_key

_TOKEN = object()
_PAGE_SIZE = 128


@dataclass(frozen=True, slots=True)
class _ResumeAuthority:
    build_id: bytes
    assigned_at: int
    scope_key: bytes
    qualification_policy_sha256: bytes
    summary: SourceBuildManifestSummary
    baseline: SourceBatchBaseline
    latest_generation: int
    catalog_working: tuple[Any, ...]
    analysis: tuple[bytes, int, str] | None


@dataclass(frozen=True, slots=True, init=False)
class VNextPreparedSourceResume:
    """Opaque qualification proof for a still-current sealed working cut."""

    _authority: _ResumeAuthority
    _policy: VNextResolvedIngestPolicy
    _root: tuple[str, ...]
    _token: object

    def __init__(
        self,
        *,
        authority: _ResumeAuthority,
        policy: VNextResolvedIngestPolicy,
        root: tuple[str, ...],
        _token: object,
    ) -> None:
        if _token is not _TOKEN:
            raise TypeError("use VNextIngestFacade.prepare_source_resume")
        object.__setattr__(self, "_authority", authority)
        object.__setattr__(self, "_policy", policy)
        object.__setattr__(self, "_root", root)
        object.__setattr__(self, "_token", _token)


def _qualification_policy_digest(policy: VNextResolvedIngestPolicy) -> bytes:
    return sha256(
        b"h2hdb-source-qualification-policy-v1\0"
        + policy.artifact_policy_sha256
        + bytes((int(policy.policy.artifacts_required),))
    ).digest()


def _read_authority(
    connector: SQLConnector,
    *,
    root: tuple[str, ...],
    policy: VNextResolvedIngestPolicy,
) -> _ResumeAuthority | None:
    working = connector.fetch_one(
        "SELECT slot, build_id, assigned_at FROM operational_source_working_builds "
        "WHERE slot = %s",
        (1,),
    )
    catalog = connector.fetch_one(
        "SELECT slot, candidate_id, assigned_at "
        "FROM operational_catalog_working_candidates WHERE slot = %s",
        (1,),
    )
    _require_locked_working_root_coherence(
        connector,
        source_working=working,
        catalog_working=catalog,
    )
    if not working:
        return None
    if len(working) != 3 or working[0] != 1:
        raise SourceBuildConflictError("source resume working root is malformed")
    build_id = require_uuid16(working[1], field="resume build_id")
    assigned = require_int63(working[2], field="resume assigned_at")
    build = _load_source_build_or_conflict(connector, build_id)
    if build.created_at != assigned or build.state == "ABANDONED":
        raise SourceBuildConflictError("source resume working authority differs")
    if build.state == "OPEN":
        return None
    if build.state != "SEALED":
        raise SourceBuildConflictError("source resume state is invalid")
    root_digest = source_root_digest(root)
    scope_key = source_scope_key("filesystem", root_digest, 1)
    if (
        build.scope_key != scope_key
        or build.manifest_policy_id != policy.manifest_policy_id
    ):
        return None
    scope = load_source_scope(connector, scope_key)
    if (
        scope.source_provider,
        scope.source_root_sha256,
        scope.identity_policy_version,
    ) != (
        b"filesystem",
        root_digest,
        1,
    ):
        raise SourceBuildConflictError("source resume scope authority differs")
    require_source_build_publication_identity(connector, build_id=build_id)
    _require_checkpoint_pair(connector, build_id=build_id, build_state="SEALED")
    receipt = _head_receipt(connector)
    published = None if receipt is None else _published_build(connector, receipt)
    baseline = SourceBatchBaseline(
        receipt,
        None if published is None else published.build_id,
        scope_key,
    )
    if baseline.build_id == build_id:
        # A published cut is not pending work. Resuming it indefinitely would
        # prevent later inventories from ever being observed.
        return None
    if (
        _validate_build_base_source(
            connector,
            build_id=build_id,
            require_lineage=True,
        )
        != baseline.receipt_id
    ):
        return None
    analysis = _sole_analysis_of_build(connector, build_id=build_id)
    if analysis is not None:
        if analysis[2] not in {"OPEN", "COMPLETE"}:
            raise SourceBuildConflictError("source resume analysis is not live")
        if not working_build_policy_matches(
            connector,
            analysis_policy_id=analysis[1],
            catalog_working=catalog,
            policy=_SourceBuildPolicyAuthority.from_resolved(policy),
        ):
            return None
    latest = connector.fetch_one(
        "SELECT generation, build_id FROM operational_source_build_generations "
        "ORDER BY generation DESC LIMIT 1",
    )
    if len(latest) != 2 or latest[1] != build_id:
        raise SourceBuildConflictError("source resume lost its latest generation")
    return _ResumeAuthority(
        build_id,
        assigned,
        scope_key,
        _qualification_policy_digest(policy),
        _load_build_manifest_summary(connector, build_id=build_id),
        baseline,
        require_positive_int63(latest[0], field="resume latest generation"),
        catalog,
        analysis,
    )


def prepare_source_resume(
    context: RepositoryContext,
    *,
    backend: str,
    policy: VNextResolvedIngestPolicy,
    root: tuple[str, ...],
) -> VNextPreparedSourceResume | None:
    if type(root) is not tuple:
        raise TypeError("source_root_components must be an exact tuple")
    source_root_digest(root)
    with context.SQLConnector() as connector:
        with database_phase("resume_authority"), connector.read_transaction():
            trusted = VNextIngestPolicyRepository.require_exact(
                VNextUnitOfWork(connector, backend=backend),
                policy,
            )
            authority = _read_authority(connector, root=root, policy=trusted)
            if authority is None:
                return None
            _compare_canonical(
                connector,
                source_root_digest(root),
                b"source_root_v1",
                iter_source_root_payload(root),
            )
        qualification = authority.qualification_policy_sha256
        after_gallery = 0
        checked = 0
        while True:
            with (
                database_phase("resume_qualification_page"),
                connector.read_transaction(),
            ):
                rows = connector.fetch_all(
                    "SELECT member.gallery_id, member.observation_id, "
                    "qualification.qualification_policy_sha256 "
                    "FROM catalog_source_build_galleries AS member "
                    "LEFT JOIN catalog_gallery_observation_validation_policies AS qualification "
                    "ON qualification.gallery_id = member.gallery_id "
                    "AND qualification.observation_id = member.observation_id "
                    "WHERE member.build_id = %s AND member.gallery_id > %s "
                    "ORDER BY member.gallery_id LIMIT 128",
                    (authority.build_id, after_gallery),
                )
            if not rows:
                break
            if len(rows) > _PAGE_SIZE:
                raise SourceBuildConflictError(
                    "source resume qualification page exceeds bound"
                )
            for row in rows:
                if len(row) != 3:
                    raise SourceBuildConflictError(
                        "source resume qualification row is malformed"
                    )
                gallery = require_positive_int63(row[0], field="resume gallery_id")
                require_positive_int63(row[1], field="resume observation_id")
                if gallery <= after_gallery:
                    raise SourceBuildConflictError(
                        "source resume qualification order changed"
                    )
                if row[2] is None:
                    raise SourceBuildConflictError(
                        "source resume qualification authority is missing"
                    )
                if (
                    require_digest32(row[2], field="resume qualification policy")
                    != qualification
                ):
                    return None
                after_gallery = gallery
                checked += 1
            if checked > authority.summary.gallery_count:
                raise SourceBuildConflictError(
                    "source resume membership exceeds sealed count"
                )
        if checked != authority.summary.gallery_count:
            raise SourceBuildConflictError(
                "source resume membership differs from sealed count"
            )
        with database_phase("resume_authority_recheck"), connector.read_transaction():
            if _read_authority(connector, root=root, policy=trusted) != authority:
                raise VNextSourceChangedError(
                    "source working cut changed during resume preparation"
                )
    return VNextPreparedSourceResume(
        authority=authority, policy=trusted, root=root, _token=_TOKEN
    )


def commit_source_resume(
    work: VNextUnitOfWork,
    *,
    gate: GateLease,
    turn: IngestTurn,
    prepared: VNextPreparedSourceResume,
    now: int,
) -> VNextIngestSourceReceipt:
    if type(prepared) is not VNextPreparedSourceResume or prepared._token is not _TOKEN:
        raise TypeError("prepared must be issued by prepare_source_resume")
    generation = _authorize(work, gate, turn, now=now)
    trusted = VNextIngestPolicyRepository.require_exact(work, prepared._policy)
    authority = prepared._authority
    working = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("source-working-build", 1),
        "SELECT slot, build_id, assigned_at FROM operational_source_working_builds "
        "WHERE slot = %s",
        (1,),
    )
    catalog = work.lock_row(
        LockRank.WORKING_ROOT,
        encode_lock_key("source-working-candidate", 1),
        "SELECT slot, candidate_id, assigned_at FROM operational_catalog_working_candidates "
        "WHERE slot = %s",
        (1,),
    )
    base = _lock_source_head(work, b"default")
    mapping = work.connector.fetch_one(
        "SELECT build_id FROM operational_source_build_generations WHERE generation = %s",
        (generation,),
    )
    current = _read_authority(work.connector, root=prepared._root, policy=trusted)
    if current is None:
        raise VNextSourceChangedError("source working cut is no longer resumable")
    expected_generation = generation if mapping else authority.latest_generation
    if (
        current.build_id != authority.build_id
        or current.assigned_at != authority.assigned_at
        or current.scope_key != authority.scope_key
        or current.qualification_policy_sha256 != authority.qualification_policy_sha256
        or current.summary != authority.summary
        or current.baseline != authority.baseline
        or current.catalog_working != authority.catalog_working
        or current.analysis != authority.analysis
        or current.latest_generation != expected_generation
        or (mapping and mapping != (authority.build_id,))
    ):
        raise VNextSourceChangedError("source working cut changed before resume commit")
    if generation < authority.latest_generation:
        raise SourceBuildConflictError(
            "source resume cannot use an older ingest generation"
        )
    if not mapping:
        _require_latest_source_generation_authority(
            work.connector,
            generation=generation,
            source_working=working,
            catalog_working=catalog,
            base_source=base,
        )
        work.connector.execute(
            "INSERT INTO operational_source_build_generations (build_id, generation) VALUES (%s, %s)",
            (authority.build_id, generation),
        )
    count = authority.summary.gallery_count
    return VNextIngestSourceReceipt(
        build_id=authority.build_id,
        discovered_galleries=count,
        staged_galleries=count,
        sealed=True,
        replayed=True,
    )
