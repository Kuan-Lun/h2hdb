"""Own one bounded current-only attempt and its lease and adapter lifecycle."""

from __future__ import annotations

from collections.abc import Callable, Mapping

from .._cleanup.cycle import CleanupBatchResult, CleanupCycleRepository
from .._cleanup.eligibility import CurrentOnlyEligibilityProof
from .._cleanup.model import (
    CatalogPublicationMaintenanceState,
    CleanupCycle,
    CurrentOnlyCleanupSelection,
)
from .._cleanup.selection import CleanupSelectionRepository
from ..database_performance import DatabasePerformance, DatabaseSpan, database_phase
from ..domain import CurrentOnlyCleanupTerminalState, VNextCurrentOnlyMaintenanceOutcome
from ..ports import ArtifactReleaseAdapter
from ..repository import RepositoryContext
from ..sql_connector import SQLConnector
from ..vnext_artifact_release_repository import ArtifactReleaseRepository
from ..vnext_domains import require_int63, require_positive_int63
from ..vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
    MaintenanceGateTokenCollisionError,
    MaintenanceGateUnavailableError,
)
from ..vnext_transaction import VNextUnitOfWork

_CURRENT_ONLY_BATCHES_PER_ATTEMPT = 16
_CURRENT_ONLY_ARTIFACT_RELEASE_PAGE_LIMIT = 1


class CurrentOnlyMaintenance:
    """Keep authority and absence evidence local to one bounded public call.

    The facade retains the context lifecycle. Each attempt owns its connector,
    lease, committed progress and compensation; adapter I/O is always outside
    managed transactions. Nothing durable or attempt-local escapes this owner.
    """

    __slots__ = ("_backend", "_clock", "_context", "_performance")

    def __init__(
        self,
        context: RepositoryContext,
        *,
        clock: Callable[[], int],
        performance: DatabasePerformance,
    ) -> None:
        self._context = context
        self._backend = context.sql_type
        self._clock = clock
        self._performance = performance

    def run(
        self,
        lease_duration_microseconds: int,
        *,
        artifact_release_adapters: Mapping[bytes, ArtifactReleaseAdapter] | None = None,
    ) -> VNextCurrentOnlyMaintenanceOutcome:
        """Validate inputs and report one complete bounded maintenance attempt."""

        if artifact_release_adapters is not None and not isinstance(
            artifact_release_adapters, Mapping
        ):
            raise TypeError("artifact_release_adapters must be a mapping")
        duration = require_positive_int63(
            lease_duration_microseconds,
            field="current-only maintenance lease_duration_microseconds",
        )
        with self._performance.operation(
            "current_only_cleanup",
            lease_duration_us=duration,
            max_batches=_CURRENT_ONLY_BATCHES_PER_ATTEMPT,
        ) as performance:
            outcome = self._run(
                duration,
                performance=performance,
                artifact_release_adapters=artifact_release_adapters,
            )
            performance.describe(outcome=outcome.value)
            return outcome

    def _run(
        self,
        duration: int,
        *,
        performance: DatabaseSpan,
        artifact_release_adapters: Mapping[bytes, ArtifactReleaseAdapter] | None,
    ) -> VNextCurrentOnlyMaintenanceOutcome:
        committed_logical_rows = 0
        performance.describe(committed_batches=0, committed_logical_rows=0)
        cycle_cutoff_at = require_int63(
            self._clock(), field="current-only maintenance cycle cutoff"
        )
        with self._context.SQLConnector() as connector:
            with database_phase(
                "initial_state", transaction_outcome="unconfirmed"
            ) as step:
                with connector.read_transaction():
                    work = VNextUnitOfWork(connector, backend=self._backend)
                    with database_phase("maintenance_artifact_hint"):
                        resource_pending = (
                            artifact_release_adapters is not None
                            and not CleanupSelectionRepository.has_open_current_only_cycle(
                                work
                            )
                            and ArtifactReleaseRepository.has_pending_release(work)
                        )
                    maintenance_state = (
                        None
                        if resource_pending
                        else CleanupSelectionRepository.current_only_maintenance_state(
                            work,
                            cycle_cutoff_at=cycle_cutoff_at,
                        )
                    )
                step.describe(
                    transaction_outcome="committed",
                    state=(
                        "RESOURCE_PENDING"
                        if maintenance_state is None
                        else maintenance_state.value
                    ),
                )
            if maintenance_state is CatalogPublicationMaintenanceState.DONE:
                performance.describe(quiet=True)
                return VNextCurrentOnlyMaintenanceOutcome.DONE
            if (
                maintenance_state is CatalogPublicationMaintenanceState.BLOCKED
                and artifact_release_adapters is None
            ):
                return VNextCurrentOnlyMaintenanceOutcome.BLOCKED

            try:
                with database_phase(
                    "gate_claim", transaction_outcome="unconfirmed"
                ) as step:
                    with connector.transaction():
                        with database_phase("gate_lock", action="claim"):
                            claim = MaintenanceGateRepository.lock_exclusive_claim(
                                VNextUnitOfWork(connector, backend=self._backend)
                            )
                        timestamp = require_int63(
                            self._clock(), field="current-only maintenance claim now"
                        )
                        lease = claim.grant(now=timestamp, lease_duration=duration)
                        step.describe(
                            lease_remaining_us=lease.lease_expires_at - timestamp
                        )
                    step.describe(transaction_outcome="committed")
            except MaintenanceGateTokenCollisionError:
                raise
            except MaintenanceGateUnavailableError:
                return VNextCurrentOnlyMaintenanceOutcome.CONTENDED

            progressed = False
            try:
                lease = self._renew_lease(connector, lease, duration=duration)
                released_artifact_page = (
                    (
                        resource_pending
                        or maintenance_state
                        is CatalogPublicationMaintenanceState.BLOCKED
                    )
                    and artifact_release_adapters is not None
                    and self._release_artifact_page(
                        connector,
                        lease,
                        artifact_release_adapters=artifact_release_adapters,
                    )
                )
                if released_artifact_page:
                    progressed = True
                    performance.describe(committed_artifact_pages=1)
                    outcome = VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
                else:
                    advanced_batches = 0
                    remaining: CatalogPublicationMaintenanceState | None = None
                    eligibility_proof: CurrentOnlyEligibilityProof | None = None
                    while advanced_batches < _CURRENT_ONLY_BATCHES_PER_ATTEMPT:
                        lease = self._renew_lease(connector, lease, duration=duration)
                        selection = self._next_cycle(
                            connector,
                            lease,
                            cycle_cutoff_at=cycle_cutoff_at,
                            eligibility_proof=eligibility_proof,
                        )
                        # The helper returns only after its selection transaction
                        # commits. Rollback or lost COMMIT response cannot publish
                        # new absence evidence into this attempt's local state.
                        eligibility_proof = selection.eligibility_proof
                        cycle = selection.cycle
                        if isinstance(cycle, CurrentOnlyCleanupTerminalState):
                            remaining = CatalogPublicationMaintenanceState(cycle.value)
                            break
                        while advanced_batches < _CURRENT_ONLY_BATCHES_PER_ATTEMPT:
                            lease = self._renew_lease(
                                connector, lease, duration=duration
                            )
                            results = self._advance_shard(connector, lease, cycle=cycle)
                            eligibility_proof = (
                                eligibility_proof.after_committed_cleanup(
                                    cycle.target_kind.value
                                )
                            )
                            advanced_batches += 1
                            progressed = True
                            committed_logical_rows += sum(
                                result.row_count
                                for result in results
                                if not result.replayed
                            )
                            performance.describe(
                                committed_batches=advanced_batches,
                                committed_logical_rows=committed_logical_rows,
                            )
                            if results[-1].cycle_complete:
                                break
                        if not results[-1].cycle_complete:
                            break
                        # A later target can release a foreign-key blocker for
                        # an earlier one, so every completed cycle restarts the
                        # exact priority scan.
                    if remaining is None:
                        # Reaching the batch budget leaves the fixed point
                        # unknown. A terminal selection already classified it
                        # in its fenced transaction, so only that path avoids
                        # repeating the complete candidate scan. Fresh release
                        # below still rejects an expired or replaced owner.
                        lease = self._renew_lease(connector, lease, duration=duration)
                        remaining = self._state(
                            connector,
                            lease,
                            cycle_cutoff_at=cycle_cutoff_at,
                            eligibility_proof=eligibility_proof,
                        )
                    if remaining is CatalogPublicationMaintenanceState.DONE:
                        outcome = VNextCurrentOnlyMaintenanceOutcome.DONE
                    elif advanced_batches:
                        outcome = VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
                    else:
                        outcome = VNextCurrentOnlyMaintenanceOutcome.BLOCKED
                self._release_lease(connector, lease)
            except MaintenanceGateTokenCollisionError as error:
                self._release_after_failure(connector, lease, cause=error)
                raise
            except MaintenanceGateUnavailableError as error:
                # A bounded SQL operation or COMMIT can outlast any lease.
                # Committed checkpoints remain authoritative; the next call
                # must obtain a new capability and resume them. Never continue
                # this attempt with an expired or replaced owner, nor claim
                # DONE without completing the final live release.
                self._release_after_failure(connector, lease, cause=error)
                return (
                    VNextCurrentOnlyMaintenanceOutcome.PROGRESSED
                    if progressed
                    else VNextCurrentOnlyMaintenanceOutcome.CONTENDED
                )
            except BaseException as error:
                self._release_after_failure(connector, lease, cause=error)
                raise
            return outcome

    def _release_artifact_page(
        self,
        connector: SQLConnector,
        lease: GateLease,
        *,
        artifact_release_adapters: Mapping[bytes, ArtifactReleaseAdapter],
    ) -> bool:
        """Release one bounded orphan page across the database/I/O boundary."""

        with database_phase(
            "artifact_issue", transaction_outcome="unconfirmed"
        ) as step:
            with connector.transaction():
                work = VNextUnitOfWork(connector, backend=self._backend)
                # Another owner can leave an interrupted cycle between the
                # optimistic hint and our EXCLUSIVE claim. Resume it before
                # scheduling new resource work, even if the hint found orphans.
                page = (
                    None
                    if CleanupSelectionRepository.has_open_current_only_cycle(work)
                    else ArtifactReleaseRepository.issue_page(
                        work,
                        gate_lease=lease,
                        page_limit=_CURRENT_ONLY_ARTIFACT_RELEASE_PAGE_LIMIT,
                        now=self._clock,
                    )
                )
            step.describe(
                transaction_outcome="committed", interrupted_cycle=page is None
            )
            if page is None:
                return False
            step.describe(terminal=page.terminal)
        if page.terminal:
            return False

        with database_phase("artifact_release"):
            acknowledgement = ArtifactReleaseRepository.release_page(
                connector,
                backend=self._backend,
                page=page,
                adapters=artifact_release_adapters,
                now=self._clock,
            )
        with database_phase(
            "artifact_acknowledge", transaction_outcome="unconfirmed"
        ) as step:
            with connector.transaction():
                receipt = ArtifactReleaseRepository.commit_page(
                    VNextUnitOfWork(connector, backend=self._backend),
                    acknowledgement=acknowledgement,
                    now=self._clock,
                )
            step.describe(
                transaction_outcome="committed",
                committed_items=receipt.transitioned_count,
                replayed=receipt.replayed,
            )
        return True

    def _state(
        self,
        connector: SQLConnector,
        lease: GateLease,
        *,
        cycle_cutoff_at: int,
        eligibility_proof: CurrentOnlyEligibilityProof | None,
    ) -> CatalogPublicationMaintenanceState:
        with database_phase("final_state", transaction_outcome="unconfirmed") as step:
            with connector.transaction():
                work = VNextUnitOfWork(connector, backend=self._backend)
                state = CleanupSelectionRepository.current_only_maintenance_state(
                    work,
                    cycle_cutoff_at=cycle_cutoff_at,
                    gate_lease=lease,
                    now=self._clock,
                    eligibility_proof=eligibility_proof,
                )
            step.describe(transaction_outcome="committed", state=state.value)
        return state

    def _next_cycle(
        self,
        connector: SQLConnector,
        lease: GateLease,
        *,
        cycle_cutoff_at: int,
        eligibility_proof: CurrentOnlyEligibilityProof | None,
    ) -> CurrentOnlyCleanupSelection:
        with database_phase("next_cycle", transaction_outcome="unconfirmed") as step:
            with connector.transaction():
                work = VNextUnitOfWork(connector, backend=self._backend)
                selection = CleanupSelectionRepository.next_current_only_cycle(
                    work,
                    gate_lease=lease,
                    cycle_cutoff_at=cycle_cutoff_at,
                    now=self._clock,
                    eligibility_proof=eligibility_proof,
                )
            cycle = selection.cycle
            step.describe(
                transaction_outcome="committed", found=isinstance(cycle, CleanupCycle)
            )
            if isinstance(cycle, CleanupCycle):
                step.describe(target=cycle.target_kind.value, shard=cycle.shard_no)
            else:
                step.describe(state=cycle.value)
        return selection

    def _advance_shard(
        self,
        connector: SQLConnector,
        lease: GateLease,
        *,
        cycle: CleanupCycle,
    ) -> tuple[CleanupBatchResult, ...]:
        with database_phase(
            "cleanup_batch",
            target=cycle.target_kind.value,
            shard=cycle.shard_no,
            cycle_generation=cycle.cycle_generation,
            max_logical_rows=cycle.max_rows_per_transaction,
            transaction_outcome="unconfirmed",
            committed_logical_rows=0,
        ) as step:
            with connector.transaction():
                work = VNextUnitOfWork(connector, backend=self._backend)
                result = CleanupCycleRepository.advance_current_only_cycle(
                    work,
                    gate_lease=lease,
                    cycle=cycle,
                    now=self._clock,
                )
            step.describe(
                transaction_outcome="committed",
                committed_logical_rows=sum(
                    item.row_count for item in result if not item.replayed
                ),
                phases=len(result),
                cycle_complete=result[-1].cycle_complete,
            )
        return result

    def _renew_lease(
        self,
        connector: SQLConnector,
        lease: GateLease,
        *,
        duration: int,
    ) -> GateLease:
        """Renew near half-life; following transactions recheck live authority.

        This is only renewal scheduling, never an authorization cache. An expired
        lease cannot be revived, and each bounded operation must still acquire
        the gate with its own fresh timestamp before accessing cleanup state.
        """

        now = require_int63(
            self._clock(),
            field="current-only maintenance renewal now",
        )
        if lease.lease_expires_at - now > duration // 2:
            return lease
        with database_phase("gate_renew", transaction_outcome="unconfirmed") as step:
            with connector.transaction():
                with database_phase("gate_lock", action="renew"):
                    locked = MaintenanceGateRepository.lock_for_renewal(
                        VNextUnitOfWork(connector, backend=self._backend), lease
                    )
                timestamp = require_int63(
                    self._clock(), field="current-only maintenance locked renewal now"
                )
                step.describe(lease_remaining_us=lease.lease_expires_at - timestamp)
                renewed = locked.renew(now=timestamp, lease_duration=duration)
                step.describe(
                    renewed_lease_remaining_us=renewed.lease_expires_at - timestamp
                )
            step.describe(transaction_outcome="committed")
        return renewed

    def _release_lease(self, connector: SQLConnector, lease: GateLease) -> None:
        with database_phase("gate_release", transaction_outcome="unconfirmed") as step:
            with connector.transaction():
                with database_phase("gate_lock", action="release"):
                    locked = MaintenanceGateRepository.lock_for_renewal(
                        VNextUnitOfWork(connector, backend=self._backend), lease
                    )
                timestamp = require_int63(
                    self._clock(), field="current-only maintenance release now"
                )
                step.describe(lease_remaining_us=lease.lease_expires_at - timestamp)
                locked.release(now=timestamp)
            step.describe(transaction_outcome="committed")

    def _release_after_failure(
        self,
        connector: SQLConnector,
        lease: GateLease,
        *,
        cause: BaseException,
    ) -> None:
        try:
            self._release_lease(connector, lease)
        except MaintenanceGateTokenCollisionError as error:
            raise error from cause
        except MaintenanceGateUnavailableError:
            # A process crash likewise loses this capability; expiry/takeover
            # plus durable cleanup checkpoints make a later call safe.
            return
        except BaseException as error:
            raise error from cause
