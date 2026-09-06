import Std

/-!
# Bounded atomic canonical upload batches

This model covers grouping complete single-leaf canonical values under one
fresh maintenance/ingest/candidate fence. It models all-or-none database commit
and exact replay; it does not prove SQLite/MariaDB transaction durability,
Python exceptions or SIGTERM/SIGKILL, SQL family validators, SHA-256, or disk
plan I/O. Runtime refinement is in test_vnext_publication_canonical_batch.py:
real database mid-batch rollback, committed response loss, stale/mixed fences,
corrupt persisted page rejection, byte/value bounds, sorted claim locks, and
fresh subprocess replay after termination before/after SQLite commit.

The abstract store records fully validated sealed membership. Existing corrupt
facts must fail validation and cannot enter the accepted transition. Grouping
does not weaken that premise. The production normalized single-leaf path adds
at most twelve facts per value (five allocation, one claim, five page, one
identity); no branch edges are possible. The cache retains at most sixteen
upload plans, each selected batch has at most 256 KiB of encoded pages, and a
large lookahead plan remains disk-backed and uses the individual page protocol.

Canonical claim locks now have global rank 50, after candidate checkpoint rank
40. Scalar ROOT/LOCATOR/analysis/publication put/seal callers return immediately
after those locks. Batch values are ordered by their fixed-width digest lock
keys; page and seal may reacquire the same key but never an earlier key/rank.
The production read/lock order and database behavior require runtime evidence.
-/

namespace H2HDB.Verification.CanonicalAtomicBatch

def MaxValues : Nat := 16
def MaxEncodedBytes : Nat := 262144

structure BoundedBatch where
  values : List Nat
  encodedSizes : List Nat
  onePagePerValue : encodedSizes.length = values.length
  nonempty : 0 < values.length
  valueBound : values.length ≤ MaxValues
  byteBound : encodedSizes.sum ≤ MaxEncodedBytes

theorem batch_page_and_value_counts_are_bounded (batch : BoundedBatch) :
    batch.values.length ≤ 16 ∧ batch.encodedSizes.length ≤ 16 := by
  constructor
  · exact batch.valueBound
  · rw [batch.onePagePerValue]
    exact batch.valueBound

theorem batch_encoded_bytes_are_bounded (batch : BoundedBatch) :
    batch.encodedSizes.sum ≤ 262144 := batch.byteBound

theorem single_leaf_normalized_inserts_are_bounded (batch : BoundedBatch) :
    12 * batch.values.length ≤ 192 := by
  have bound := batch.valueBound
  simp only [MaxValues] at bound
  omega

abbrev SealedStore := Nat → Bool

def sealOne (before : SealedStore) (value : Nat) : SealedStore :=
  fun observed => decide (observed = value) || before observed

def sealBatch (before : SealedStore) (values : List Nat) : SealedStore :=
  fun observed => decide (observed ∈ values) || before observed

theorem grouped_sealing_equals_individual_sealing
    (before : SealedStore) (values : List Nat) :
    sealBatch before values = values.foldr (fun value store => sealOne store value) before := by
  induction values with
  | nil => funext observed; simp [sealBatch]
  | cons value tail ih =>
    simp only [List.foldr_cons, ← ih]
    funext observed
    simp [sealBatch, sealOne, Bool.or_assoc]

theorem exact_batch_replay_is_idempotent
    (before : SealedStore) (values : List Nat) :
    sealBatch (sealBatch before values) values = sealBatch before values := by
  funext observed
  by_cases present : observed ∈ values <;> simp [sealBatch, present]

/-- Authorization, exact preimages, and SQL atomicity are explicit premises. -/
def finish (before : SealedStore) (values : List Nat)
    (freshFence allFactsExact committed : Bool) : SealedStore :=
  if freshFence && allFactsExact && committed then sealBatch before values else before

theorem stale_fence_performs_no_durable_write
    (before : SealedStore) (values : List Nat) (exact committed : Bool) :
    finish before values false exact committed = before := by simp [finish]

theorem corrupt_facts_perform_no_durable_write
    (before : SealedStore) (values : List Nat) (fresh committed : Bool) :
    finish before values fresh false committed = before := by simp [finish]

theorem precommit_failure_preserves_the_entire_store
    (before : SealedStore) (values : List Nat) (fresh exact : Bool) :
    finish before values fresh exact false = before := by simp [finish]

theorem response_loss_after_commit_keeps_every_sealed_value
    (before : SealedStore) (values : List Nat) (value : Nat)
    (included : value ∈ values) :
    finish before values true true true value = true := by
  simp [finish, sealBatch, included]

theorem fresh_process_exact_replay_after_response_loss
    (before : SealedStore) (values : List Nat) :
    finish (finish before values true true true) values true true true =
      finish before values true true true := by
  simpa [finish] using exact_batch_replay_is_idempotent before values

/-- Prefetch itself cannot certify or advance the committed validation prefix. -/
def prepareLookahead (cursor : Nat) (_pending : List Nat) : Nat := cursor

theorem preparation_does_not_advance_uncommitted_cursor
    (cursor : Nat) (pending : List Nat) : prepareLookahead cursor pending = cursor := rfl

theorem candidate_checkpoint_precedes_canonical_claim : (40 : Nat) < 50 := by decide

theorem sorted_claim_key_steps_never_descend
    (previous following : Nat) (sorted : previous ≤ following) :
    (50 : Nat) ≤ 50 ∧ previous ≤ following := ⟨Nat.le_refl 50, sorted⟩

end H2HDB.Verification.CanonicalAtomicBatch
