import Std

/-!
# Exact-key publication cleanup SQL costs

This model constructs the key chunks and their two client SQL operations: one
locking SELECT and one exact-primary-key DELETE. The chunk capacity is computed
from the runtime's 64-key and 900-bind limits, including the locking LIMIT bind.
It does not take a SQL-cost bound as an assumption. The positive-capacity premise
is admission of a query shape: Python rejects shapes that cannot fit one key.

Keys here are already selected, deduplicated and ordered immutable row keys.
Frozen-root loading, candidate selection, cursor coverage, terminal probes,
gate/receipt/checkpoint work and transaction overhead are outside this mutation
projection and must be counted separately by implementation tests. These
theorems do not prove SQL semantics, physical lock acquisition order, Python
refinement, rows examined, network latency or elapsed time. Real SQLite/MariaDB
tests supply separate finite correspondence and safety evidence.
-/

namespace H2HDB.Verification.PublicationCleanupBatch

def capacity (fixedBinds orderedArity : Nat) : Nat :=
  min 64 ((900 - fixedBinds - 1) / orderedArity)

def chunkRounds {α : Type} (width : Nat) : Nat → List α → List (List α)
  | 0, _ => []
  | rounds + 1, keys =>
      if keys.isEmpty then []
      else keys.take width :: chunkRounds width rounds (keys.drop width)

def chunks {α : Type} (width : Nat) (keys : List α) : List (List α) :=
  chunkRounds width (keys.length / width + 1) keys

inductive Query where
  | lockingSelect
  | exactDelete
  deriving DecidableEq, Repr

def mutationTrace {α : Type} (pages : List (List α)) : List Query :=
  pages.flatMap fun _ => [.lockingSelect, .exactDelete]

theorem capacity_is_hard_capped (fixedBinds orderedArity : Nat) :
    capacity fixedBinds orderedArity ≤ 64 := by
  exact Nat.min_le_left _ _

theorem admitted_lock_query_fits_bind_budget
    (fixedBinds orderedArity rows : Nat)
    (fixedFits : fixedBinds + 1 ≤ 900)
    (rowsFit : rows ≤ capacity fixedBinds orderedArity) :
    fixedBinds + rows * orderedArity + 1 ≤ 900 := by
  have cap := Nat.min_le_right 64 ((900 - fixedBinds - 1) / orderedArity)
  have product := Nat.mul_le_mul_right orderedArity (Nat.le_trans rowsFit cap)
  have divided := Nat.div_mul_le_self (900 - fixedBinds - 1) orderedArity
  omega

theorem constructed_pages_are_bounded {α : Type}
    (width rounds : Nat) (keys : List α) :
    ∀ page ∈ chunkRounds width rounds keys, page.length ≤ width := by
  induction rounds generalizing keys with
  | zero => simp [chunkRounds]
  | succ rounds ih =>
      simp only [chunkRounds]
      split
      · simp
      · intro page member
        simp only [List.mem_cons] at member
        rcases member with equal | later
        · subst page
          simp only [List.length_take]
          exact Nat.min_le_left _ _
        · exact ih (keys.drop width) page later

theorem exact_delete_fits_bind_budget
    (fixedBinds orderedArity primaryArity rows : Nat)
    (fixedFits : fixedBinds + 1 ≤ 900)
    (primarySubset : primaryArity ≤ orderedArity)
    (rowsFit : rows ≤ capacity fixedBinds orderedArity) :
    rows * primaryArity ≤ 900 := by
  have locked := admitted_lock_query_fits_bind_budget
    fixedBinds orderedArity rows fixedFits rowsFit
  have subset := Nat.mul_le_mul_left rows primarySubset
  omega

theorem constructed_pages_are_nonempty {α : Type}
    (width rounds : Nat) (keys : List α) (positive : 0 < width) :
    ∀ page ∈ chunkRounds width rounds keys, 0 < page.length := by
  induction rounds generalizing keys with
  | zero => simp [chunkRounds]
  | succ rounds ih =>
      simp only [chunkRounds]
      split
      · simp
      · rename_i nonempty
        intro page member
        simp only [List.mem_cons] at member
        rcases member with equal | later
        · subst page
          simp only [List.length_take]
          have keyPositive : 0 < keys.length := by
            cases keys <;> simp_all
          omega
        · exact ih (keys.drop width) page later

theorem adequate_rounds_preserve_every_key {α : Type}
    (width rounds : Nat) (keys : List α)
    (adequate : keys.length ≤ width * rounds) :
    (chunkRounds width rounds keys).flatten = keys := by
  induction rounds generalizing keys with
  | zero =>
      have empty : keys = [] := List.length_eq_zero_iff.mp (by omega)
      simp [chunkRounds, empty]
  | succ rounds ih =>
      simp only [chunkRounds]
      split
      · rename_i empty
        have equal : keys = [] := List.isEmpty_iff.mp empty
        simp [equal]
      · have rest : (keys.drop width).length ≤ width * rounds := by
          simp only [List.length_drop]
          rw [Nat.mul_succ] at adequate
          omega
        simp only [List.flatten_cons, ih (keys.drop width) rest]
        exact List.take_append_drop width keys

theorem constructed_chunks_preserve_every_key {α : Type}
    (width : Nat) (keys : List α) (positive : 0 < width) :
    (chunks width keys).flatten = keys := by
  apply adequate_rounds_preserve_every_key
  have remainder := Nat.mod_lt keys.length positive
  have partition := Nat.mod_add_div keys.length width
  rw [Nat.mul_succ]
  omega

theorem round_count_bounds_constructed_chunks {α : Type}
    (width rounds : Nat) (keys : List α) :
    (chunkRounds width rounds keys).length ≤ rounds := by
  induction rounds generalizing keys with
  | zero => simp [chunkRounds]
  | succ rounds ih =>
      simp only [chunkRounds]
      split
      · simp
      · simp only [List.length_cons]
        exact Nat.succ_le_succ (ih (keys.drop width))

theorem exactly_two_queries_per_constructed_chunk {α : Type}
    (pages : List (List α)) :
    (mutationTrace pages).length = 2 * pages.length := by
  induction pages with
  | nil => rfl
  | cons page rest ih => simp [mutationTrace] at ih ⊢; omega

theorem mutation_queries_have_derived_batch_bound {α : Type}
    (width : Nat) (keys : List α) :
    (mutationTrace (chunks width keys)).length ≤ 2 * (keys.length / width + 1) := by
  rw [exactly_two_queries_per_constructed_chunk]
  exact Nat.mul_le_mul_left 2 (round_count_bounds_constructed_chunks _ _ _)

theorem repeated_cycles_repeat_actual_mutation_work {α : Type}
    (cycles : Nat) (pages : List (List α)) :
    ((List.replicate cycles (mutationTrace pages)).flatten).length =
      cycles * (2 * pages.length) := by
  induction cycles with
  | zero => simp
  | succ count ih =>
      simp [List.replicate_succ, exactly_two_queries_per_constructed_chunk, ih,
        Nat.succ_mul, Nat.add_comm]

end H2HDB.Verification.PublicationCleanupBatch

/-- Executable finite traces used by real connector cost-correspondence tests. -/
def main : IO Unit := do
  let stdout ← IO.getStdout
  for fixed in [5, 515, 516, 835] do
    for arity in [2, 3, 4, 5, 6, 11, 128] do
      for rows in [0, 1, 4, 5, 6, 49, 50, 51, 57, 58, 59, 63, 64, 65, 255, 256, 257] do
        let width := H2HDB.Verification.PublicationCleanupBatch.capacity fixed arity
        if 0 < width then
          let pages := H2HDB.Verification.PublicationCleanupBatch.chunks width (List.range rows)
          let sql := H2HDB.Verification.PublicationCleanupBatch.mutationTrace pages
          stdout.putStrLn s!"{fixed},{arity},{rows},{width},{pages.length},{sql.length}"
