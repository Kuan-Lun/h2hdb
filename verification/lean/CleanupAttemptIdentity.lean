import Std

/-!
# Cleanup successor attempt identity

Production keeps one cleanup job per fixed `(targetKind, shard)` slot.  Once
that job is COMPLETE, the writer locks the slot, deletes the exact observed
job with a compare-and-swap predicate, and inserts its successor in the same
transaction.  The successor generation is the natural-number successor and
its cleanup ID is derived again from the fixed slot and that new generation.

This model proves the pure, unbounded part of that transition: every
successful successor strictly increases the generation, is internally
consistent with the derivation function, and cannot retain the predecessor's
ID.  The production codec stores the generation in the final eight bytes;
runtime codec tests connect that concrete encoding to this injective record
model.  SQL locking, compare-and-swap success, transaction atomicity and
rollback are established by runtime and backend tests, not by this theorem.
-/

namespace H2HDB.Verification.CleanupAttemptIdentity

/-- Abstract image of the injective production cleanup-ID codec. -/
structure CleanupId where
  targetKindTag : Nat
  shard : Fin 256
  generation : Nat
deriving DecidableEq, Repr

/-- Re-derive an attempt ID from its fixed slot and cycle generation. -/
def deriveCleanupId
    (targetKindTag : Nat) (shard : Fin 256) (generation : Nat) : CleanupId :=
  { targetKindTag, shard, generation }

/-- The retained row created by one successful successor transition. -/
structure CleanupJob where
  targetKindTag : Nat
  shard : Fin 256
  cycleGeneration : Nat
  cleanupId : CleanupId
deriving DecidableEq, Repr

/-- Construct the next row only after the exact predecessor CAS succeeds. -/
def beginSuccessor
    (targetKindTag : Nat) (shard : Fin 256) (currentGeneration : Nat) :
    CleanupJob :=
  let generation := Nat.succ currentGeneration
  {
    targetKindTag
    shard
    cycleGeneration := generation
    cleanupId := deriveCleanupId targetKindTag shard generation
  }

def HasDerivedIdentity (job : CleanupJob) : Prop :=
  job.cleanupId =
    deriveCleanupId job.targetKindTag job.shard job.cycleGeneration

theorem successful_successor_strictly_increases_generation
    (targetKindTag : Nat) (shard : Fin 256) (currentGeneration : Nat) :
    currentGeneration <
      (beginSuccessor targetKindTag shard currentGeneration).cycleGeneration := by
  exact Nat.lt_succ_self currentGeneration

theorem successful_successor_rederives_identity
    (targetKindTag : Nat) (shard : Fin 256) (currentGeneration : Nat) :
    HasDerivedIdentity (beginSuccessor targetKindTag shard currentGeneration) := by
  rfl

theorem successful_successor_has_fresh_identity
    (targetKindTag : Nat) (shard : Fin 256) (currentGeneration : Nat) :
    (beginSuccessor targetKindTag shard currentGeneration).cleanupId ≠
      deriveCleanupId targetKindTag shard currentGeneration := by
  intro same
  have generationsAreEqual := congrArg CleanupId.generation same
  simp [beginSuccessor, deriveCleanupId] at generationsAreEqual

end H2HDB.Verification.CleanupAttemptIdentity
