"""Current-only maintenance classification, resume priority, and target selection."""

from __future__ import annotations

from collections.abc import Callable

from h2hdb._cleanup import cycle as _cycle
from h2hdb._cleanup import model as _model
from h2hdb._cleanup import plan as _plan
from h2hdb._cleanup.eligibility import CurrentOnlyEligibilityProof
from h2hdb._cleanup.model import (
    _CLEANUP_ALGORITHM_VERSION,
    _JOB_TABLE,
    _MAX_BATCH_ROWS,
    _SWEEP_TABLE,
    CatalogPublicationMaintenanceState,
    CleanupCorruptionError,
    CleanupCycle,
    CleanupTargetKind,
    CurrentOnlyCleanupSelection,
)
from h2hdb._cleanup.plan import _StaticTargetPlan
from h2hdb._cleanup.registry import (
    _CURRENT_ONLY_OPEN_ORDER_SQL,
    _CURRENT_ONLY_TARGET_PRIORITY,
    _MAINTENANCE_TARGET_PRIORITY,
    _STATIC_PLANS,
)
from h2hdb._cleanup.targets import resources as _resources_targets
from h2hdb._cleanup.targets.publication import _CATALOG_PUBLICATION_ELIGIBILITY
from h2hdb.database_performance import database_phase
from h2hdb.domain import CurrentOnlyCleanupTerminalState
from h2hdb.vnext_domains import (
    require_bounded_bytes,
    require_digest32,
    require_int63,
    require_positive_int63,
    require_uuid16,
)
from h2hdb.vnext_maintenance_gate_repository import GateLease
from h2hdb.vnext_transaction import VNextUnitOfWork


def _current_only_terminal_state(
    work: VNextUnitOfWork,
) -> CurrentOnlyCleanupTerminalState:
    """Classify blocked payload after this transaction found no cleanup work."""

    with database_phase("maintenance_blocked_publication"):
        blocked = _catalog_publication_payload_is_blocked(work)
    if not blocked:
        with database_phase("maintenance_blocked_candidate"):
            blocked = _publication_candidate_payload_is_blocked(work)
    return (
        CurrentOnlyCleanupTerminalState.BLOCKED
        if blocked
        else CurrentOnlyCleanupTerminalState.DONE
    )


def _load_open_current_only_cycle(work: VNextUnitOfWork) -> CleanupCycle | None:
    row = work.connector.fetch_one(
        f"""
        SELECT sweep.target_kind, sweep.shard_no, sweep.target_key,
               job.cleanup_id, job.cycle_generation, job.cycle_cutoff_at,
               job.algorithm_version, job.max_rows_per_transaction,
               job.hash_cache_max_age_microseconds
        FROM {_SWEEP_TABLE} AS sweep
        JOIN {_JOB_TABLE} AS job ON job.target_key = sweep.target_key
        WHERE job.state = 'OPEN'
        ORDER BY {_CURRENT_ONLY_OPEN_ORDER_SQL}, sweep.shard_no
        LIMIT 1
        """
    )
    if not row:
        return None
    if len(row) != 9:
        raise CleanupCorruptionError(
            "current-only interrupted-cycle probe returned an invalid shape"
        )
    try:
        kind = CleanupTargetKind(
            _model._as_text(row[0], field="current-only interrupted target_kind")
        )
    except ValueError as error:
        raise CleanupCorruptionError(
            "current-only interrupted cycle has an unknown target"
        ) from error
    if kind not in _MAINTENANCE_TARGET_PRIORITY:
        raise CleanupCorruptionError(
            "current-only interrupted cycle has an excluded target"
        )
    shard = _model._require_shard(row[1])
    if require_int63(row[6], field="cleanup algorithm_version") != (
        _CLEANUP_ALGORITHM_VERSION
    ):
        raise CleanupCorruptionError("cleanup algorithm_version is unsupported")
    cycle = CleanupCycle(
        cleanup_id=require_uuid16(row[3], field="stored cleanup_id"),
        target_kind=kind,
        shard_no=shard,
        target_key=require_digest32(row[2], field="stored target_key"),
        cycle_generation=require_positive_int63(
            row[4], field="stored cycle_generation"
        ),
        cycle_cutoff_at=require_int63(row[5], field="stored cycle_cutoff_at"),
        max_rows_per_transaction=_model._require_batch_bound(row[7]),
        hash_cache_max_age_microseconds=require_int63(
            row[8], field="stored hash_cache_max_age_microseconds"
        ),
    )
    if cycle.hash_cache_max_age_microseconds != 0:
        raise CleanupCorruptionError(
            "current-only OPEN cycle has an invalid hash-cache age policy"
        )
    return cycle


def _static_candidate_shard(plan: _StaticTargetPlan, value: object) -> int:
    if plan.shard_width is None:
        return require_int63(value, field="cleanup candidate integer shard") % 256
    if isinstance(value, str):
        payload = value.encode("utf-8", errors="strict")
    else:
        payload = require_bounded_bytes(
            value,
            field="cleanup candidate byte shard",
            minimum=1,
            maximum=1024,
        )
    if not payload:
        raise CleanupCorruptionError("cleanup candidate shard value is empty")
    if plan.variable_width_shard:
        if len(payload) > plan.shard_width:
            raise CleanupCorruptionError("cleanup candidate shard value is too wide")
    elif len(payload) != plan.shard_width:
        raise CleanupCorruptionError("cleanup candidate shard width is invalid")
    return payload[0]


def _next_static_candidate_shard(
    work: VNextUnitOfWork,
    plan: _StaticTargetPlan,
    *,
    cycle_cutoff_at: int,
) -> int | None:
    root_table = _plan._identifier(plan.root_table)
    shard_column = _plan._identifier(plan.shard_column)
    order = ", ".join(f"r.{_plan._identifier(column)}" for column in plan.root_key)
    parameters: tuple[object, ...] = (cycle_cutoff_at,) if plan.uses_cutoff else ()
    row = work.connector.fetch_one(
        f"SELECT r.{shard_column} FROM {root_table} AS r "
        f"WHERE ({plan.eligibility}) ORDER BY {order} LIMIT 1",
        parameters,
    )
    if not row:
        return None
    if len(row) != 1:
        raise CleanupCorruptionError(
            f"{plan.kind.value} candidate probe returned an invalid shape"
        )
    return _static_candidate_shard(plan, row[0])


def _next_current_only_candidate(
    work: VNextUnitOfWork,
    *,
    cycle_cutoff_at: int,
    eligibility_proof: CurrentOnlyEligibilityProof | None = None,
) -> tuple[tuple[CleanupTargetKind, int] | None, CurrentOnlyEligibilityProof | None]:
    dynamic = {
        CleanupTargetKind.ARTIFACT_BLOB: _resources_targets._next_artifact_blob_candidate_shard,
        CleanupTargetKind.PUBLICATION_IDENTITY: (
            _resources_targets._next_publication_identity_candidate_shard
        ),
        CleanupTargetKind.FILE_NAME_IDENTITY: _resources_targets._next_file_name_candidate_shard,
        CleanupTargetKind.CONTENT_BLOB: _resources_targets._next_content_blob_candidate_shard,
    }
    for kind in _CURRENT_ONLY_TARGET_PRIORITY:
        with database_phase("maintenance_eligibility", target=kind.value) as phase:
            if (
                eligibility_proof is not None
                and kind.value in eligibility_proof.absent_targets
            ):
                phase.describe(candidate_found=False, reused_absence=True)
                continue
            plan = _STATIC_PLANS.get(kind)
            if plan is not None:
                shard = _next_static_candidate_shard(
                    work, plan, cycle_cutoff_at=cycle_cutoff_at
                )
            else:
                shard = dynamic[kind](work)
            phase.describe(candidate_found=shard is not None)
        if shard is not None:
            return (kind, shard), eligibility_proof
        if eligibility_proof is not None:
            eligibility_proof = eligibility_proof.observes_absence(kind.value)
    return None, eligibility_proof


def _catalog_publication_payload_is_blocked(work: VNextUnitOfWork) -> bool:
    row = work.connector.fetch_one(
        """
        SELECT 1
        FROM catalog_publication_occurrence_identities AS publication
        JOIN catalog_publication_commit_head_receipts AS head
          ON head.channel = %s
        JOIN catalog_publication_commits AS current
          ON current.receipt_id = head.receipt_id
        WHERE publication.revision < current.revision
        LIMIT 1
        """,
        (b"default",),
    )
    if row and row != (1,):
        raise CleanupCorruptionError(
            "current-only blocked-payload probe returned an invalid shape"
        )
    return bool(row)


def _publication_candidate_payload_is_blocked(work: VNextUnitOfWork) -> bool:
    row = work.connector.fetch_one("""
        SELECT 1
        FROM catalog_publication_candidates AS candidate
        WHERE NOT EXISTS (
            SELECT 1 FROM operational_catalog_working_candidates working
            WHERE working.candidate_id = candidate.candidate_id)
          AND EXISTS (
            SELECT 1 FROM catalog_prepared_artifacts prepared
            WHERE prepared.candidate_id = candidate.candidate_id
              AND prepared.state IN ('PENDING', 'PREPARED'))
        LIMIT 1
        """)
    if row and row != (1,):
        raise CleanupCorruptionError(
            "current-only blocked-candidate probe returned an invalid shape"
        )
    return bool(row)


class CleanupSelectionRepository:
    """Classify and select current-only work within the caller-owned transaction."""

    @staticmethod
    def catalog_publication_maintenance_state(
        work: VNextUnitOfWork,
    ) -> CatalogPublicationMaintenanceState:
        """Optimistically classify current-only work without changing the gate.

        DONE and BLOCKED are safe hints: a concurrent publication/finalization
        merely defers cleanup to a later resident poll.  ACTIONABLE callers
        must still claim EXCLUSIVE and recheck under the gate before deleting.
        """

        interrupted = work.connector.fetch_one(
            f"SELECT 1 FROM {_SWEEP_TABLE} AS sweep "
            f"JOIN {_JOB_TABLE} AS job ON job.target_key = sweep.target_key "
            "WHERE sweep.target_kind = %s AND job.state = 'OPEN' LIMIT 1",
            (CleanupTargetKind.CATALOG_PUBLICATION.value,),
        )
        if interrupted and interrupted != (1,):
            raise CleanupCorruptionError(
                "catalog cleanup interrupted-cycle probe returned an invalid shape"
            )
        if interrupted:
            return CatalogPublicationMaintenanceState.ACTIONABLE

        payload = work.connector.fetch_one(
            "SELECT 1 FROM catalog_publication_occurrence_identities AS publication "
            "JOIN catalog_publication_commit_head_receipts AS head "
            "ON head.channel = %s "
            "JOIN catalog_publication_commits AS current "
            "ON current.receipt_id = head.receipt_id "
            "WHERE publication.revision < current.revision LIMIT 1",
            (b"default",),
        )
        if payload and payload != (1,):
            raise CleanupCorruptionError(
                "catalog cleanup preflight returned an invalid shape"
            )
        if not payload:
            return CatalogPublicationMaintenanceState.DONE

        current = work.connector.fetch_one(
            "SELECT receipt.state, receipt.finalized_at "
            "FROM catalog_publication_commit_head_receipts AS head "
            "JOIN catalog_publication_receipts AS receipt "
            "ON receipt.receipt_id = head.receipt_id "
            "WHERE head.channel = %s",
            (b"default",),
        )
        if not current:
            return CatalogPublicationMaintenanceState.BLOCKED
        if len(current) != 2:
            raise CleanupCorruptionError(
                "catalog cleanup current-receipt probe returned an invalid shape"
            )
        state = _model._as_text(
            current[0], field="catalog cleanup current receipt state"
        )
        if state != "PUBLISHED" or current[1] is None:
            return CatalogPublicationMaintenanceState.BLOCKED
        require_int63(current[1], field="catalog cleanup current finalized_at")

        candidate = work.connector.fetch_one(
            "SELECT 1 FROM catalog_publication_occurrence_identities AS r "
            f"WHERE ({_CATALOG_PUBLICATION_ELIGIBILITY}) LIMIT 1"
        )
        if candidate and candidate != (1,):
            raise CleanupCorruptionError(
                "catalog cleanup actionable probe returned an invalid shape"
            )
        if candidate:
            return CatalogPublicationMaintenanceState.ACTIONABLE
        return CatalogPublicationMaintenanceState.BLOCKED

    @staticmethod
    def catalog_publication_next_maintenance_shard(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        now: int,
    ) -> int | None:
        """Return the next actionable shard, prioritizing interrupted work."""

        timestamp = require_int63(now, field="catalog cleanup next-shard now")
        _cycle._require_exclusive_gate(work, gate_lease, now=timestamp)
        _cycle._validate_strategy_seeds(work, CleanupTargetKind.CATALOG_PUBLICATION)

        interrupted = work.connector.fetch_one(
            f"SELECT sweep.shard_no FROM {_SWEEP_TABLE} AS sweep "
            f"JOIN {_JOB_TABLE} AS job ON job.target_key = sweep.target_key "
            "WHERE sweep.target_kind = %s AND job.state = 'OPEN' "
            "ORDER BY sweep.shard_no LIMIT 1",
            (CleanupTargetKind.CATALOG_PUBLICATION.value,),
        )
        if interrupted:
            if len(interrupted) != 1:
                raise CleanupCorruptionError(
                    "catalog cleanup interrupted-shard probe returned an invalid shape"
                )
            return _model._require_shard(interrupted[0])

        candidate = work.connector.fetch_one(
            "SELECT r.publication_key "
            "FROM catalog_publication_occurrence_identities AS r "
            f"WHERE ({_CATALOG_PUBLICATION_ELIGIBILITY}) "
            "ORDER BY r.publication_key LIMIT 1"
        )
        if not candidate:
            return None
        if len(candidate) != 1:
            raise CleanupCorruptionError(
                "catalog cleanup next-candidate probe returned an invalid shape"
            )
        publication_key = require_digest32(
            candidate[0], field="catalog cleanup candidate publication_key"
        )
        return publication_key[0]

    @staticmethod
    def catalog_publication_maintenance_required(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        now: int,
    ) -> bool:
        """Return whether historical payload or an interrupted shard remains."""

        timestamp = require_int63(now, field="catalog cleanup preflight now")
        _cycle._require_exclusive_gate(work, gate_lease, now=timestamp)
        return (
            CleanupSelectionRepository.catalog_publication_maintenance_state(work)
            is not CatalogPublicationMaintenanceState.DONE
        )

    @staticmethod
    def has_open_current_only_cycle(work: VNextUnitOfWork) -> bool:
        """Preserve interrupted-cycle priority before scheduling resource I/O."""

        return _load_open_current_only_cycle(work) is not None

    @staticmethod
    def current_only_maintenance_state(
        work: VNextUnitOfWork,
        *,
        cycle_cutoff_at: int,
        gate_lease: GateLease | None = None,
        now: int | Callable[[], int] | None = None,
        eligibility_proof: CurrentOnlyEligibilityProof | None = None,
    ) -> CatalogPublicationMaintenanceState:
        """Classify the catalog/resource fixed point across 22 of 23 targets.

        New ``HASH_CACHE_OBSERVATION`` work is deliberately excluded.  It is a
        file-derived, age-based cache policy rather than retained catalog state;
        including it with a zero max age would make every resident idle poll
        delete newly observed cache rows and prevent a stable fixed point.  An
        already OPEN hash-cache cycle is nevertheless surfaced as ACTIONABLE
        so the sole serialized cleanup authority can always be handed off and
        completed after process loss.

        A lease-free call is an optimistic read-only hint.  Supplying the
        exact EXCLUSIVE lease and ``now`` performs the same exact probes under
        the destructive maintenance fence.
        """

        cutoff = require_int63(
            cycle_cutoff_at, field="current-only maintenance cycle_cutoff_at"
        )
        if (gate_lease is None) != (now is None):
            raise TypeError("gate_lease and now must be supplied together")
        if eligibility_proof is not None and gate_lease is None:
            raise TypeError("eligibility evidence requires a freshly fenced call")
        if gate_lease is not None:
            assert now is not None
            _cycle._require_exclusive_gate(work, gate_lease, now=now)
            eligibility_proof = CurrentOnlyEligibilityProof.under_validated_gate(
                gate_lease, cutoff, eligibility_proof
            )

        with database_phase("maintenance_open_cycle"):
            open_cycle = _load_open_current_only_cycle(work)
        if open_cycle is not None:
            return CatalogPublicationMaintenanceState.ACTIONABLE
        candidate, _proof = _next_current_only_candidate(
            work, cycle_cutoff_at=cutoff, eligibility_proof=eligibility_proof
        )
        if candidate is not None:
            return CatalogPublicationMaintenanceState.ACTIONABLE
        return CatalogPublicationMaintenanceState(
            _current_only_terminal_state(work).value
        )

    @staticmethod
    def next_current_only_cycle(
        work: VNextUnitOfWork,
        *,
        gate_lease: GateLease,
        cycle_cutoff_at: int,
        now: int | Callable[[], int],
        eligibility_proof: CurrentOnlyEligibilityProof | None = None,
    ) -> CurrentOnlyCleanupSelection:
        """Select work or classify its absence in the same fenced transaction.

        The exact candidate probe and cycle creation share the EXCLUSIVE
        transaction.  A completed cycle is followed by a new call, which
        restarts the priority scan from the first target and therefore reaches
        a dependency fixed point without walking all 5,888 target shards.  A
        previously opened hash-cache cycle is resumed as a liveness handoff;
        this path never starts new hash-cache work. A terminal result includes
        blocked-payload classification and needs no second candidate scan.
        It is valid only while the exact EXCLUSIVE lease remains live; callers
        must freshly validate release before reporting completion and must not
        reuse it for a subsequent SHARED claim.
        """

        cutoff = require_int63(
            cycle_cutoff_at, field="current-only maintenance cycle_cutoff_at"
        )
        timestamp = _cycle._require_exclusive_gate(work, gate_lease, now=now)
        proof = CurrentOnlyEligibilityProof.under_validated_gate(
            gate_lease, cutoff, eligibility_proof
        )

        interrupted = _load_open_current_only_cycle(work)
        if interrupted is not None:
            _cycle._validate_strategy_seeds(work, interrupted.target_kind)
            return CurrentOnlyCleanupSelection(interrupted, proof)

        candidate, selected_proof = _next_current_only_candidate(
            work, cycle_cutoff_at=cutoff, eligibility_proof=proof
        )
        assert selected_proof is not None
        if candidate is None:
            return CurrentOnlyCleanupSelection(
                _current_only_terminal_state(work), selected_proof
            )
        kind, shard = candidate
        cycle = _cycle._begin_cycle_under_exclusive(
            work,
            kind=kind,
            shard=shard,
            cutoff=cutoff,
            max_rows=_MAX_BATCH_ROWS,
            max_age=0,
            now=timestamp,
        )
        return CurrentOnlyCleanupSelection(cycle, selected_proof)
