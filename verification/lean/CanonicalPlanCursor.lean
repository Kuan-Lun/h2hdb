import Std

/-!
# Canonical publication-plan cursor refinement

This module compares two selectors over one immutable, ordered canonical upload
plan and one freshly read durable snapshot:

* `referenceScan` starts at the first plan entry on every bounded step;
* `optimizedScan` starts after a process-local prefix whose exact sealed
  preimages and either required claims or durable first-consumer progress were
  already validated.

The durable model retains the production decision order.  Before a BUILD
entry's durable first-consumer cursor, a value with no exact claim is allocated
first.  A claimed, unsealed value writes the first missing ordered page,
rejects a conflicting page, and seals only after every page is exact.  Once
the BUILD checkpoint has durably crossed that first consumer, the consumed
claim may be absent and the exact sealed preimage remains complete.  VALIDATE
starts only after BUILD is terminal and independently validates exact sealed
preimages without recreating consumed claims.  Malformed, partial, or
conflicting durable facts reject instead of becoming work.

A BUILD consumer commit can therefore delete a claim while the optimized
cursor stutters at or beyond that entry: the same transaction advances the
durable first-consumer checkpoint, preserving completion.  Separately, an
active sealed value that still needs a claim may retain its observed scalar
receipt only as a fail-closed guard.  That scalar is not byte authority:
allocate rollback or response-loss retry performs the full stream validation
again, even when the receipt is unchanged.  A full stream can therefore reject
changed preimage bytes that retain the same scalar receipt.

Every prepared allocate is also re-authorized against fresh generation, stage,
and durable first-consumer checkpoint inputs.  The theorem additionally takes
opaque repository-key equality as an explicit mathematical scope assumption;
it does not assert that production rereads that key.  A delayed duplicate that
arrives after consumption or a modeled authority change returns `rejected 0`,
meaning zero publication writes.

Lean lists are immutable values, so both selectors receive the same plan value.
`CompletePrefix` is the explicit cache-validity assumption.  Its preservation
theorem assumes exact completion facts are monotonic for the same repository
authority.  A generation, stage, candidate, or other authority change is not
covered by that assumption and must discard the process-local cursor; the
companion TLA+ model checks that reset together with crash, response-loss
replay, and stale-fence rejection.

The equivalence and work theorems are unbounded over plan length, page count,
and cursor trace length.  They do not prove that Python iteration, SQL reads,
transactions, hash validation, or either database backend refines this model.
-/

namespace H2HDB.Verification.CanonicalPlanCursor

inductive SealState where
  | absent
  /-- The stored tree and exact canonical preimage match this plan entry. -/
  | exact (receipt : Nat)
  /-- A full stream found different bytes, even if the scalar receipt is equal. -/
  | conflicting (receipt : Nat)
deriving DecidableEq, Repr

inductive ClaimState where
  | absent
  /-- The claim matches the cache's exact ingest-generation authority. -/
  | exact
  | conflicting
deriving DecidableEq, Repr

inductive PageState where
  | absent
  | exact
  | conflicting
deriving DecidableEq, Repr

structure PlanEntry where
  /-- Number of pages in the deterministic ordered upload plan. -/
  pageCount : Nat
  /-- Durable BUILD child cursor of this value's unique first consumer. -/
  firstConsumer : Nat
deriving DecidableEq, Repr

structure CanonicalPlan where
  /-- Ordering is semantic: the first incomplete entry owns the next action. -/
  entries : List PlanEntry
deriving DecidableEq, Repr

structure DurableEntry where
  sealState : SealState
  claim : ClaimState
  /-- Page coordinates have the same order as their immutable plan entry. -/
  pages : List PageState
deriving DecidableEq, Repr

inductive SelectionStage where
  | build
  | validate
deriving DecidableEq, Repr

structure SelectionAuthority where
  stage : SelectionStage
  /-- Durable BUILD checkpoint, represented by its ordered cursor rank. -/
  buildCheckpoint : Nat
deriving DecidableEq, Repr

inductive CanonicalAction where
  | allocate (planIndex : Nat)
  | putPage (planIndex pageIndex : Nat)
  | seal (planIndex : Nat)
deriving DecidableEq, Repr

inductive EntryInspection where
  | reject
  | complete
  | action (next : CanonicalAction)
deriving DecidableEq, Repr

inductive ScanOutcome where
  | reject
  | terminal
  | action (next : CanonicalAction)
deriving DecidableEq, Repr

structure ScanReceipt where
  outcome : ScanOutcome
  /-- First entry not skipped as an exact required-or-consumed value. -/
  nextCursor : Nat
deriving DecidableEq, Repr

def inspectPages (planIndex : Nat) : Nat → List PageState → EntryInspection
  | _pageIndex, [] => .action (.seal planIndex)
  | pageIndex, .absent :: _tail => .action (.putPage planIndex pageIndex)
  | pageIndex, .exact :: tail => inspectPages planIndex (pageIndex + 1) tail
  | _pageIndex, .conflicting :: _tail => .reject

theorem inspect_pages_ne_complete
    (planIndex pageIndex : Nat)
    (pages : List PageState) :
    inspectPages planIndex pageIndex pages ≠ .complete := by
  induction pages generalizing pageIndex with
  | nil => simp [inspectPages]
  | cons page pages induction =>
      cases page with
      | absent => simp [inspectPages]
      | exact => simpa [inspectPages] using induction (pageIndex + 1)
      | conflicting => simp [inspectPages]

/--
BUILD crosses a value only after its first consumer commits.  VALIDATE is
admitted only after BUILD terminal, so every value is already consumed.
-/
def ConsumerCommitted
    (authority : SelectionAuthority)
    (plan : PlanEntry) : Prop :=
  authority.stage = .validate ∨ plan.firstConsumer ≤ authority.buildCheckpoint

instance consumerCommittedDecidable
    (authority : SelectionAuthority)
    (plan : PlanEntry) : Decidable (ConsumerCommitted authority plan) := by
  unfold ConsumerCommitted
  infer_instance

/-- Exact production-shaped decision for one plan entry and durable snapshot. -/
def inspectEntry
    (planIndex : Nat)
    (authority : SelectionAuthority)
    (plan : PlanEntry)
    (durable : DurableEntry) : EntryInspection :=
  if durable.pages.length != plan.pageCount then
    .reject
  else
    match durable.sealState with
    | .conflicting _receipt => .reject
    | .exact _receipt =>
        match durable.claim with
        | .absent =>
            if ConsumerCommitted authority plan then
              .complete
            else
              .action (.allocate planIndex)
        | .exact => .complete
        | .conflicting => .reject
    | .absent =>
        if ConsumerCommitted authority plan then
          .reject
        else
          match durable.claim with
          | .absent => .action (.allocate planIndex)
          | .exact => inspectPages planIndex 0 durable.pages
          | .conflicting => .reject

/-- Scan until the first rejection/action, counting only complete entries. -/
def scan : Nat → SelectionAuthority → List PlanEntry → List DurableEntry → ScanReceipt
  | cursor, _authority, [], [] => ⟨.terminal, cursor⟩
  | cursor, authority, plan :: plans, durable :: durables =>
      match inspectEntry cursor authority plan durable with
      | .reject => ⟨.reject, cursor⟩
      | .action next => ⟨.action next, cursor⟩
      | .complete => scan (cursor + 1) authority plans durables
  | cursor, _authority, _plans, _durables => ⟨.reject, cursor⟩

/-- The exhaustive selector used as the behavioral reference. -/
def referenceScan
    (plan : CanonicalPlan)
    (snapshot : List DurableEntry)
    (authority : SelectionAuthority) : ScanReceipt :=
  scan 0 authority plan.entries snapshot

/-- The optimized selector starts only after a validated exact prefix. -/
def optimizedScan
    (plan : CanonicalPlan)
    (snapshot : List DurableEntry)
    (authority : SelectionAuthority)
    (cursor : Nat) : ScanReceipt :=
  scan cursor authority (plan.entries.drop cursor) (snapshot.drop cursor)

/-- Exact seal plus a required claim or durable consumed/validation authority. -/
def EntryComplete
    (authority : SelectionAuthority)
    (plan : PlanEntry)
    (durable : DurableEntry) : Prop :=
  durable.pages.length = plan.pageCount ∧
    (∃ receipt, durable.sealState = .exact receipt) ∧
    (durable.claim = .exact ∨
      (durable.claim = .absent ∧ ConsumerCommitted authority plan))

theorem inspect_entry_complete_iff
    (planIndex : Nat)
    (authority : SelectionAuthority)
    (plan : PlanEntry)
    (durable : DurableEntry) :
    inspectEntry planIndex authority plan durable = .complete ↔
      EntryComplete authority plan durable := by
  simp only [inspectEntry, EntryComplete]
  split <;> rename_i pageShape
  · simp_all
  · cases durable.sealState <;> cases durable.claim <;>
      by_cases committed : ConsumerCommitted authority plan <;>
      simp_all [inspect_pages_ne_complete]

/-! ## Observed receipt is not byte authority -/

structure ObservedSealedReceipt where
  activeIndex : Nat
  /-- Scalar tree/root receipt observed beside a full preimage validation. -/
  receipt : Nat
deriving DecidableEq, Repr

/-- Any sealed scalar state still requires a fresh full-preimage stream. -/
def FullStreamValidationRequired (durable : DurableEntry) : Prop :=
  match durable.sealState with
  | .absent => False
  | .exact _receipt => True
  | .conflicting _receipt => True

/--
An observed scalar receipt may reject a mismatch before work, but a match still
delegates to `inspectEntry`, whose `SealState` is the result of the new full
stream.  No branch treats the observation as proof of the preimage bytes.
-/
def inspectEntryWithObservedReceipt
    (planIndex : Nat)
    (authority : SelectionAuthority)
    (plan : PlanEntry)
    (durable : DurableEntry)
    (observed : Option ObservedSealedReceipt) : EntryInspection :=
  match observed with
  | none => inspectEntry planIndex authority plan durable
  | some remembered =>
      if remembered.activeIndex != planIndex then
        inspectEntry planIndex authority plan durable
      else
        match durable.sealState with
        | .absent => .reject
        | .exact freshReceipt =>
            if remembered.receipt = freshReceipt then
              inspectEntry planIndex authority plan durable
            else
              .reject
        | .conflicting freshReceipt =>
            if remembered.receipt = freshReceipt then
              inspectEntry planIndex authority plan durable
            else
              .reject

theorem exact_retry_requires_full_stream_again
    (receipt : Nat)
    (before after : DurableEntry)
    (beforeReceipt : before.sealState = .exact receipt)
    (afterReceipt : after.sealState = .exact receipt) :
    FullStreamValidationRequired before ∧
      FullStreamValidationRequired after := by
  simp [FullStreamValidationRequired, beforeReceipt, afterReceipt]

theorem same_receipt_conflicting_preimage_rejects
    (planIndex receipt : Nat)
    (authority : SelectionAuthority)
    (plan : PlanEntry)
    (durable : DurableEntry)
    (sameScalarDifferentBytes : durable.sealState = .conflicting receipt) :
    inspectEntryWithObservedReceipt planIndex authority plan durable
      (some ⟨planIndex, receipt⟩) = .reject := by
  simp [inspectEntryWithObservedReceipt, sameScalarDifferentBytes,
    inspectEntry]

theorem changed_or_missing_observed_receipt_rejects
    (planIndex observedReceipt : Nat)
    (authority : SelectionAuthority)
    (plan : PlanEntry)
    (durable : DurableEntry)
    (missing : durable.sealState = .absent) :
    inspectEntryWithObservedReceipt planIndex authority plan durable
      (some ⟨planIndex, observedReceipt⟩) = .reject := by
  simp [inspectEntryWithObservedReceipt, missing]

/-! ## Fresh commit fencing for prepared allocate work -/

inductive AllocateCommitResult where
  | committed
  | rejected (publicationWrites : Nat)
deriving DecidableEq, Repr

/--
The opaque repository key scopes the mathematical theorem to one candidate and
repository authority.  Its equality is an explicit input assumption, not a
claim that the production commit rereads that key.  Full-stream validation is
prepare-time evidence whose preservation relies on sealed-byte immutability;
this pure decision models only the fresh generation/stage/checkpoint fence and
does not establish a transaction or SQL refinement.
-/
def commitPreparedAllocate
    (preparedRepositoryKey freshRepositoryKey : Nat)
    (preparedGeneration freshGeneration : Nat)
    (freshAuthority : SelectionAuthority)
    (plan : PlanEntry) : AllocateCommitResult :=
  if preparedRepositoryKey = freshRepositoryKey ∧
      preparedGeneration = freshGeneration ∧
      freshAuthority.stage = .build ∧
      ¬ConsumerCommitted freshAuthority plan then
    .committed
  else
    .rejected 0

theorem delayed_allocate_after_consumer_is_zero_write_rejected
    (preparedRepositoryKey freshRepositoryKey : Nat)
    (preparedGeneration freshGeneration : Nat)
    (freshAuthority : SelectionAuthority)
    (plan : PlanEntry)
    (consumed : ConsumerCommitted freshAuthority plan) :
    commitPreparedAllocate
      preparedRepositoryKey freshRepositoryKey
      preparedGeneration freshGeneration freshAuthority plan = .rejected 0 := by
  simp [commitPreparedAllocate, consumed]

theorem changed_allocate_authority_is_zero_write_rejected
    (preparedRepositoryKey freshRepositoryKey : Nat)
    (preparedGeneration freshGeneration : Nat)
    (freshAuthority : SelectionAuthority)
    (plan : PlanEntry)
    (changed : preparedRepositoryKey ≠ freshRepositoryKey ∨
      preparedGeneration ≠ freshGeneration ∨
      freshAuthority.stage ≠ .build) :
    commitPreparedAllocate
      preparedRepositoryKey freshRepositoryKey
      preparedGeneration freshGeneration freshAuthority plan = .rejected 0 := by
  unfold commitPreparedAllocate
  split <;> simp_all

theorem build_consumed_value_does_not_reallocate_claim
    (planIndex receipt : Nat)
    (authority : SelectionAuthority)
    (plan : PlanEntry)
    (durable : DurableEntry)
    (build : authority.stage = .build)
    (consumed : plan.firstConsumer ≤ authority.buildCheckpoint)
    (pageShape : durable.pages.length = plan.pageCount)
    (sealed : durable.sealState = .exact receipt)
    (claimConsumed : durable.claim = .absent) :
    inspectEntry planIndex authority plan durable = .complete := by
  simp [inspectEntry, pageShape, sealed, claimConsumed,
    ConsumerCommitted, build, consumed]

theorem validation_does_not_recreate_consumed_claim
    (planIndex receipt : Nat)
    (authority : SelectionAuthority)
    (plan : PlanEntry)
    (durable : DurableEntry)
    (validation : authority.stage = .validate)
    (pageShape : durable.pages.length = plan.pageCount)
    (sealed : durable.sealState = .exact receipt)
    (claimConsumed : durable.claim = .absent) :
    inspectEntry planIndex authority plan durable = .complete := by
  simp [inspectEntry, pageShape, sealed, claimConsumed,
    ConsumerCommitted, validation]

/--
The first `count` entries have exact sealed preimages plus either exact required
claims or durable consumed/VALIDATE authority.  Shape mismatch fails closed.
-/
def CompletePrefix :
    Nat → SelectionAuthority → List PlanEntry → List DurableEntry → Prop
  | 0, _authority, _plans, _durables => True
  | count + 1, authority, plan :: plans, durable :: durables =>
      EntryComplete authority plan durable ∧
        CompletePrefix count authority plans durables
  | _count, _authority, _plans, _durables => False

/--
Fresh durable state for the same plan authority preserves every previously
complete entry while the BUILD checkpoint moves monotonically or VALIDATE is
entered after BUILD terminal.  A generation, candidate, or plan change is
deliberately not such progress and clears the cache.
-/
def CompletionMonotone :
    SelectionAuthority → SelectionAuthority →
      List PlanEntry → List DurableEntry → List DurableEntry → Prop
  | _oldAuthority, _newAuthority, [], [], [] => True
  | oldAuthority, newAuthority, plan :: plans, old :: olds, new :: news =>
      (EntryComplete oldAuthority plan old →
        EntryComplete newAuthority plan new) ∧
      CompletionMonotone oldAuthority newAuthority plans olds news
  | _oldAuthority, _newAuthority, _plans, _old, _new => False

theorem complete_prefix_survives_authorized_progress
    (count : Nat)
    (oldAuthority newAuthority : SelectionAuthority)
    (plans : List PlanEntry)
    (oldSnapshot newSnapshot : List DurableEntry)
    (complete : CompletePrefix count oldAuthority plans oldSnapshot)
    (monotone : CompletionMonotone
      oldAuthority newAuthority plans oldSnapshot newSnapshot) :
    CompletePrefix count newAuthority plans newSnapshot := by
  induction count generalizing plans oldSnapshot newSnapshot with
  | zero => simp [CompletePrefix]
  | succ count induction =>
      cases plans with
      | nil => simp [CompletePrefix] at complete
      | cons plan plans =>
          cases oldSnapshot with
          | nil => simp [CompletePrefix] at complete
          | cons old olds =>
              cases newSnapshot with
              | nil => simp [CompletionMonotone] at monotone
              | cons new news =>
                  simp only [CompletePrefix] at complete ⊢
                  simp only [CompletionMonotone] at monotone
                  exact ⟨monotone.1 complete.1,
                    induction plans olds news complete.2 monotone.2⟩

theorem scan_skips_exact_complete_prefix
    (count start : Nat)
    (authority : SelectionAuthority)
    (plans : List PlanEntry)
    (snapshot : List DurableEntry)
    (complete : CompletePrefix count authority plans snapshot) :
    scan start authority plans snapshot =
      scan (start + count) authority
        (plans.drop count) (snapshot.drop count) := by
  induction count generalizing start plans snapshot with
  | zero => simp
  | succ count induction =>
      cases plans with
      | nil => simp [CompletePrefix] at complete
      | cons plan plans =>
          cases snapshot with
          | nil => simp [CompletePrefix] at complete
          | cons durable snapshot =>
              simp only [CompletePrefix] at complete
              have inspected :
                  inspectEntry start authority plan durable = .complete :=
                (inspect_entry_complete_iff
                  start authority plan durable).2 complete.1
              rw [show scan start authority (plan :: plans) (durable :: snapshot) =
                scan (start + 1) authority plans snapshot by
                  simp [scan, inspected]]
              rw [induction (start := start + 1) plans snapshot complete.2]
              congr 1 <;> omega

/-- The cursor selector returns the same rejection, action, terminal, and cursor. -/
theorem reference_scan_equals_optimized_scan
    (plan : CanonicalPlan)
    (snapshot : List DurableEntry)
    (authority : SelectionAuthority)
    (cursor : Nat)
    (complete : CompletePrefix cursor authority plan.entries snapshot) :
    referenceScan plan snapshot authority =
      optimizedScan plan snapshot authority cursor := by
  simpa [referenceScan, optimizedScan] using
    scan_skips_exact_complete_prefix
      cursor 0 authority plan.entries snapshot complete

theorem next_action_is_unchanged
    (plan : CanonicalPlan)
    (snapshot : List DurableEntry)
    (authority : SelectionAuthority)
    (cursor : Nat)
    (next : CanonicalAction)
    (complete : CompletePrefix cursor authority plan.entries snapshot) :
    (referenceScan plan snapshot authority).outcome = .action next ↔
      (optimizedScan plan snapshot authority cursor).outcome = .action next := by
  rw [reference_scan_equals_optimized_scan
    plan snapshot authority cursor complete]

theorem terminal_result_is_unchanged
    (plan : CanonicalPlan)
    (snapshot : List DurableEntry)
    (authority : SelectionAuthority)
    (cursor : Nat)
    (complete : CompletePrefix cursor authority plan.entries snapshot) :
    (referenceScan plan snapshot authority).outcome = .terminal ↔
      (optimizedScan plan snapshot authority cursor).outcome = .terminal := by
  rw [reference_scan_equals_optimized_scan
    plan snapshot authority cursor complete]

theorem rejection_result_is_unchanged
    (plan : CanonicalPlan)
    (snapshot : List DurableEntry)
    (authority : SelectionAuthority)
    (cursor : Nat)
    (complete : CompletePrefix cursor authority plan.entries snapshot) :
    (referenceScan plan snapshot authority).outcome = .reject ↔
      (optimizedScan plan snapshot authority cursor).outcome = .reject := by
  rw [reference_scan_equals_optimized_scan
    plan snapshot authority cursor complete]

theorem scan_cursor_is_monotonic
    (start : Nat)
    (authority : SelectionAuthority)
    (plans : List PlanEntry)
    (snapshot : List DurableEntry) :
    start ≤ (scan start authority plans snapshot).nextCursor := by
  induction plans generalizing start snapshot with
  | nil => cases snapshot <;> simp [scan]
  | cons plan plans induction =>
      cases snapshot with
      | nil => simp [scan]
      | cons durable snapshot =>
          cases inspected : inspectEntry start authority plan durable with
          | reject => simp [scan, inspected]
          | action next => simp [scan, inspected]
          | complete =>
              simp only [scan, inspected]
              have bound := induction (start + 1) snapshot
              omega

theorem scan_cursor_is_bounded
    (start : Nat)
    (authority : SelectionAuthority)
    (plans : List PlanEntry)
    (snapshot : List DurableEntry) :
    (scan start authority plans snapshot).nextCursor ≤ start + plans.length := by
  induction plans generalizing start snapshot with
  | nil => cases snapshot <;> simp [scan]
  | cons plan plans induction =>
      cases snapshot with
      | nil => simp [scan]
      | cons durable snapshot =>
          cases inspected : inspectEntry start authority plan durable with
          | reject => simp [scan, inspected]
          | action next => simp [scan, inspected]
          | complete =>
              simp only [scan, inspected, List.length_cons]
              have bound := induction (start + 1) snapshot
              omega

/-- Every crossed index had an exact seal and required-or-consumed authority. -/
theorem scan_crosses_only_exact_complete_entries
    (start : Nat)
    (authority : SelectionAuthority)
    (plans : List PlanEntry)
    (snapshot : List DurableEntry) :
    CompletePrefix
      ((scan start authority plans snapshot).nextCursor - start)
      authority
      plans
      snapshot := by
  induction plans generalizing start snapshot with
  | nil => cases snapshot <;> simp [scan, CompletePrefix]
  | cons plan plans induction =>
      cases snapshot with
      | nil => simp [scan, CompletePrefix]
      | cons durable snapshot =>
          cases inspected : inspectEntry start authority plan durable with
          | reject => simp [scan, inspected, CompletePrefix]
          | action next => simp [scan, inspected, CompletePrefix]
          | complete =>
              simp only [scan, inspected]
              have head :=
                (inspect_entry_complete_iff
                  start authority plan durable).1 inspected
              have tail := induction (start + 1) snapshot
              have bound := scan_cursor_is_monotonic
                (start + 1) authority plans snapshot
              have countEquality :
                  (scan (start + 1) authority plans snapshot).nextCursor - start =
                    ((scan (start + 1) authority plans snapshot).nextCursor -
                      (start + 1)) + 1 := by
                omega
              rw [countEquality]
              exact ⟨head, tail⟩

theorem optimized_cursor_stays_within_plan
    (plan : CanonicalPlan)
    (snapshot : List DurableEntry)
    (authority : SelectionAuthority)
    (cursor : Nat)
    (withinPlan : cursor ≤ plan.entries.length) :
    (optimizedScan plan snapshot authority cursor).nextCursor ≤
      plan.entries.length := by
  have bounded := scan_cursor_is_bounded
    cursor authority (plan.entries.drop cursor) (snapshot.drop cursor)
  simp only [List.length_drop] at bounded
  simp only [optimizedScan]
  omega

/-- Entries validated by one scan are exactly its half-open cursor interval. -/
def ValidatedDuring (start : Nat) (receipt : ScanReceipt) (index : Nat) : Prop :=
  start ≤ index ∧ index < receipt.nextCursor

/--
A completed prefix entry is not streamed again within one matching cache epoch.
This says nothing about the still-active entry: every aborted or retried sealed
allocate streams that active entry again.
-/
theorem consecutive_cursor_scans_do_not_revalidate_crossed_prefix
    (firstStart secondStart : Nat)
    (first second : ScanReceipt)
    (index : Nat)
    (handoff : first.nextCursor ≤ secondStart) :
    ¬(ValidatedDuring firstStart first index ∧
      ValidatedDuring secondStart second index) := by
  intro both
  exact Nat.not_lt_of_ge (Nat.le_trans handoff both.2.1) both.1.2

/-- Exact accounting for any composed process-local cursor epoch. -/
structure ValidationAccount where
  startCursor : Nat
  nextCursor : Nat
  work : Nat
  monotonic : startCursor ≤ nextCursor
  exact : work = nextCursor - startCursor

def scanAccount
    (start : Nat)
    (authority : SelectionAuthority)
    (plans : List PlanEntry)
    (snapshot : List DurableEntry) : ValidationAccount where
  startCursor := start
  nextCursor := (scan start authority plans snapshot).nextCursor
  work := (scan start authority plans snapshot).nextCursor - start
  monotonic := scan_cursor_is_monotonic start authority plans snapshot
  exact := rfl

/-- Adjacent scans compose without recounting an already validated index. -/
def ValidationAccount.compose
    (first second : ValidationAccount)
    (continuous : first.nextCursor = second.startCursor) : ValidationAccount where
  startCursor := first.startCursor
  nextCursor := second.nextCursor
  work := first.work + second.work
  monotonic := by
    have firstMonotonic := first.monotonic
    have secondMonotonic := second.monotonic
    omega
  exact := by
    have firstExact := first.exact
    have secondExact := second.exact
    have firstMonotonic := first.monotonic
    have secondMonotonic := second.monotonic
    omega

theorem composed_cursor_work_is_linear
    (plan : CanonicalPlan)
    (account : ValidationAccount)
    (startsAtBeginning : account.startCursor = 0)
    (withinPlan : account.nextCursor ≤ plan.entries.length) :
    account.work ≤ plan.entries.length := by
  have exact := account.exact
  have monotonic := account.monotonic
  omega

/-!
Successful-progress accounting charges one stream for every newly crossed
canonical entry and one additional stream for every required allocation that
actually commits.  An aborted or rejected attempt does not make successful
progress and must stream again on retry, so arbitrary retry storms are
intentionally outside this bound.
-/
structure SuccessfulProgressValidationAccount where
  cursorWork : ValidationAccount
  successfulRequiredAllocations : Nat
  totalValidationWork : Nat
  exact : totalValidationWork =
    cursorWork.work + successfulRequiredAllocations

theorem successful_progress_validation_work_is_linear
    (plan : CanonicalPlan)
    (account : SuccessfulProgressValidationAccount)
    (startsAtBeginning : account.cursorWork.startCursor = 0)
    (withinPlan : account.cursorWork.nextCursor ≤ plan.entries.length) :
    account.totalValidationWork ≤
      plan.entries.length + account.successfulRequiredAllocations := by
  have prefixBound := composed_cursor_work_is_linear
    plan account.cursorWork startsAtBeginning withinPlan
  have exact := account.exact
  omega

end H2HDB.Verification.CanonicalPlanCursor
