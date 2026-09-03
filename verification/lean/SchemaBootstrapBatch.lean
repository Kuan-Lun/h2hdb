import Std

/-!
# Bounded schema-bootstrap batch refinement

The production epoch runner preserves the generated seed order and combines
only adjacent rows whose SQL text is byte-identical.  Every batch is hard
bounded.  This model proves that batch counters and statement boundaries do not
change the durable row result relative to the former row-at-a-time evaluator.

`Seed` abstracts one checksum-bound idempotent INSERT fact.  A duplicate-free
`List Seed` abstracts the exact durable facts accepted by the generated validator. A
failed batch contributes no facts; response loss may contribute the whole batch
before the process-local cursor is lost.  The replay theorem proves that
reapplying the complete generated list after any already-applied subset cannot
change the final fact set, provided the subset contains only generated facts.

The theorems are unbounded over seed-list length, statement changes, batch
limit, and replay-prefix size.  They assume the production SQL statements have
the idempotent no-op conflict semantics checked by `SchemaSeedStatement`; they
do not prove Python grouping, connector transaction behavior, generated SQL, or
either database backend refines this model.  Differential and live-backend
tests supply that implementation evidence.
-/

namespace H2HDB.Verification.SchemaBootstrapBatch

structure Seed where
  statement : Nat
  row : Nat
deriving DecidableEq, Repr

abbrev Durable := List Seed

/-- The per-key production validator accepts one exact ordered singleton. -/
def ExactOrderedRows (actual expected : List Seed) : Prop :=
  actual = expected

theorem exact_singleton_rejects_missing (expected : Seed) :
    ¬ ExactOrderedRows [] [expected] := by
  simp [ExactOrderedRows]

theorem exact_singleton_rejects_duplicate (expected : Seed) :
    ¬ ExactOrderedRows [expected, expected] [expected] := by
  simp [ExactOrderedRows]

theorem exact_singleton_rejects_changed_row
    (actual expected : Seed)
    (different : actual ≠ expected) :
    ¬ ExactOrderedRows [actual] [expected] := by
  simpa [ExactOrderedRows] using different

/-- Seeded-relation validation is exact multiset equality, like `Counter`. -/
def ExactSeedMultiset (actual expected : List Seed) : Prop :=
  ∀ candidate, actual.count candidate = expected.count candidate

theorem exact_seed_multiset_rejects_count_difference
    (actual expected : List Seed)
    (candidate : Seed)
    (different : actual.count candidate ≠ expected.count candidate) :
    ¬ ExactSeedMultiset actual expected := by
  intro exact
  exact different (exact candidate)

def durableInsert (seed : Seed) (durable : Durable) : Durable :=
  if seed ∈ durable then durable else seed :: durable

theorem mem_durable_insert
    (candidate seed : Seed)
    (durable : Durable) :
    candidate ∈ durableInsert seed durable ↔
      candidate = seed ∨ candidate ∈ durable := by
  by_cases present : seed ∈ durable
  · simp only [durableInsert, present, if_pos]
    constructor
    · exact Or.inr
    · intro observed
      rcases observed with equal | existing
      · simpa [equal] using present
      · exact existing
  · simp [durableInsert, present]

def applyReference : List Seed → Durable → Durable
  | [], durable => durable
  | seed :: tail, durable => applyReference tail (durableInsert seed durable)

/--
The result evaluator used by bounded batching. `currentStatement` and
`remaining` affect only where a connector call ends; every seed is still
applied in exact generated order.
-/
def applyBatched : Nat → Option Nat → Nat → List Seed → Durable → Durable
  | _limit, _currentStatement, _remaining, [], durable => durable
  | limit, currentStatement, remaining, seed :: tail, durable =>
      let continues := currentStatement == some seed.statement && remaining > 0
      let nextRemaining := if continues then remaining - 1 else limit - 1
      applyBatched
        limit
        (some seed.statement)
        nextRemaining
        tail
        (durableInsert seed durable)

theorem batched_result_equals_row_reference
    (limit remaining : Nat)
    (currentStatement : Option Nat)
    (seeds : List Seed)
    (durable : Durable) :
    applyBatched limit currentStatement remaining seeds durable =
      applyReference seeds durable := by
  induction seeds generalizing currentStatement remaining durable with
  | nil => rfl
  | cons seed tail inductionHypothesis =>
      simp only [applyBatched, applyReference]
      exact inductionHypothesis _ _ _

theorem mem_apply_reference
    (candidate : Seed)
    (seeds : List Seed)
    (durable : Durable) :
    candidate ∈ applyReference seeds durable ↔
      candidate ∈ durable ∨ candidate ∈ seeds := by
  induction seeds generalizing durable with
  | nil => simp [applyReference]
  | cons seed tail inductionHypothesis =>
      simp [applyReference, inductionHypothesis, mem_durable_insert,
        or_assoc, or_left_comm, or_comm]

def DurablyEquivalent (left right : Durable) : Prop :=
  ∀ candidate, candidate ∈ left ↔ candidate ∈ right

/--
Crash or response loss may leave any subset of generated idempotent facts
durable. Replaying the complete manifest then has the same result as one clean
row-at-a-time execution.
-/
theorem replay_after_generated_subset_equals_clean_execution
    (seeds alreadyApplied : List Seed)
    (durable : Durable)
    (subset : ∀ seed, seed ∈ alreadyApplied → seed ∈ seeds) :
    DurablyEquivalent
      (applyReference seeds (applyReference alreadyApplied durable))
      (applyReference seeds durable) := by
  intro candidate
  simp only [mem_apply_reference]
  constructor
  · intro observed
    rcases observed with (existing | applied) | generated
    · exact Or.inl existing
    · exact Or.inr (subset candidate applied)
    · exact Or.inr generated
  · intro expected
    rcases expected with existing | generated
    · exact Or.inl (Or.inl existing)
    · exact Or.inr generated

/-- Every committed generated prefix is a generated subset, so replaying the
complete manifest converges to the same durable facts as a clean execution. -/
theorem replay_after_generated_prefix_equals_clean_execution
    (generatedPrefix remainder : List Seed)
    (durable : Durable) :
    DurablyEquivalent
      (applyReference
        (generatedPrefix ++ remainder)
        (applyReference generatedPrefix durable))
      (applyReference (generatedPrefix ++ remainder) durable) := by
  apply replay_after_generated_subset_equals_clean_execution
  intro seed member
  simp only [List.mem_append]
  exact Or.inl member

theorem batched_replay_after_generated_subset_equals_reference
    (limit remaining : Nat)
    (currentStatement : Option Nat)
    (seeds alreadyApplied : List Seed)
    (durable : Durable)
    (subset : ∀ seed, seed ∈ alreadyApplied → seed ∈ seeds) :
    DurablyEquivalent
      (applyBatched
          limit
          currentStatement
          remaining
          seeds
          (applyReference alreadyApplied durable))
      (applyReference seeds durable) := by
  rw [batched_result_equals_row_reference]
  exact replay_after_generated_subset_equals_clean_execution
    seeds alreadyApplied durable subset

end H2HDB.Verification.SchemaBootstrapBatch
