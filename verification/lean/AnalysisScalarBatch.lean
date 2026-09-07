import Std

/-!
The scalar family protocol and the per-column SQL batch assign exactly the same
fixed-width facts. Each Row is ONE normalized family, identified by
(family-kind, analysis/hash identity), not all families belonging to a file hash.
The Nat identity is an injective abstraction of (analysis_id, file_sha256).
Delta and shadow therefore have independent anchors and seals. Tombstone uses its
own family kind and existence field. Unused fields of a family are always absent.
An existing family is admissible only when wholly absent or exactly equal to
every proposed field. This agrees with each family's independent SQL preflight.
The theorem does not establish the separate cross-family shadow/tombstone
exclusion rule or refusal of an unexpected family with no proposed Row; those
remain runtime comparison obligations. Each processed hash has a delta Row, so
distinct projected hash identities give the repository's checkpoint increment.
A transaction commits all families together with that checkpoint; an abort or
stale checkpoint keeps the old durable state.

This model covers normalized scalar storage, not source aggregation. SQL union
queries must expose every orphan child; domain checks, fencing, isolation, insert
conflicts and transaction rollback must refine the model. Runtime tests exercise
those assumptions on SQLite and MariaDB. Nothing here proves Python allocation,
SQL query plans, physical crash durability, or a bound on one hash's source fanout.
-/

namespace H2HDB.Verification.AnalysisScalarBatch

inductive Field where
  | anchor | occurrence | artist | maximum | oldExcluded | newExcluded | change | seal
  | tombstone
  deriving DecidableEq

inductive FamilyKind where
  | delta | shadow | tombstone
  deriving DecidableEq

abbrev Key := FamilyKind × Nat
abbrev Columns := Field → Key → Option Nat

structure Row where
  key : Key
  values : Field → Option Nat

def writeRow (store : Columns) (row : Row) : Columns :=
  fun field key => if key = row.key then row.values field else store field key

def writeColumn (column : Key → Option Nat) (field : Field) (row : Row) :
    Key → Option Nat :=
  fun key => if key = row.key then row.values field else column key

def scalar (store : Columns) (rows : List Row) : Columns :=
  rows.foldl writeRow store

def batch (store : Columns) (rows : List Row) : Columns :=
  fun field => rows.foldl (fun column row => writeColumn column field row) (store field)

theorem column_projection_of_scalar
    (rows : List Row) (store : Columns) (field : Field) :
    scalar store rows field = batch store rows field := by
  induction rows generalizing store with
  | nil => rfl
  | cons row rows ih =>
      change scalar (writeRow store row) rows field =
        rows.foldl (fun column item => writeColumn column field item)
          (writeColumn (store field) field row)
      exact ih (writeRow store row)

/-- Grouping fixed columns across a page equals writing each complete row. -/
theorem scalar_and_batch_store_are_equal (store : Columns) (rows : List Row) :
    scalar store rows = batch store rows := by
  funext field
  exact column_projection_of_scalar rows store field

def absent (store : Columns) (key : Key) : Prop :=
  ∀ field, store field key = none

def compatible (store : Columns) (row : Row) : Prop :=
  absent store row.key ∨ ∀ field, store field row.key = row.values field

/-- Both orphan children and incomplete sealed families fail exact preflight. -/
theorem partial_or_collision_is_rejected
    (store : Columns) (row : Row)
    (present : ∃ field, store field row.key ≠ none)
    (different : ∃ field, store field row.key ≠ row.values field) :
    ¬ compatible store row := by
  intro accepted
  rcases accepted with empty | exact
  · obtain ⟨field, nonempty⟩ := present
    exact nonempty (empty field)
  · obtain ⟨field, mismatch⟩ := different
    exact mismatch (exact field)

/-- Replaying an exact complete family changes no normalized facts. -/
theorem exact_row_replay_preserves_store
    (store : Columns) (row : Row)
    (exact : ∀ field, store field row.key = row.values field) :
    writeRow store row = store := by
  funext field key
  by_cases equality : key = row.key
  · simp [writeRow, equality, exact]
  · simp [writeRow, equality]

theorem exact_page_replay_preserves_store
    (store : Columns) (rows : List Row)
    (exact : ∀ row ∈ rows, ∀ field, store field row.key = row.values field) :
    batch store rows = store := by
  rw [← scalar_and_batch_store_are_equal]
  induction rows with
  | nil => rfl
  | cons row rows ih =>
      have head := exact row (by simp)
      have tail : ∀ item ∈ rows, ∀ field, store field item.key = item.values field := by
        intro item member
        exact exact item (by simp [member])
      simp only [scalar, List.foldl_cons, exact_row_replay_preserves_store store row head]
      exact ih tail

structure Durable where
  facts : Columns
  checkpoint : Nat

def processedHashCount (rows : List Row) : Nat :=
  (rows.map (fun row => row.key.2)).eraseDups.length

/-- A commit exposes the complete batch and checkpoint in one durable state. -/
def finish (before : Durable) (rows : List Row) (expected : Nat)
    (transactionSucceeded : Bool) : Durable :=
  if transactionSucceeded && before.checkpoint == expected then
    { facts := batch before.facts rows, checkpoint := before.checkpoint + processedHashCount rows }
  else before

theorem abort_preserves_facts_and_checkpoint
    (before : Durable) (rows : List Row) (expected : Nat) :
    finish before rows expected false = before := by
  simp [finish]

theorem stale_checkpoint_cannot_commit
    (before : Durable) (rows : List Row) (expected : Nat)
    (stale : before.checkpoint ≠ expected) :
    finish before rows expected true = before := by
  simp [finish, stale]

theorem successful_checkpoint_has_every_scalar_fact
    (before : Durable) (rows : List Row) :
    (finish before rows before.checkpoint true).facts = scalar before.facts rows ∧
    (finish before rows before.checkpoint true).checkpoint = before.checkpoint + processedHashCount rows := by
  simp [finish, scalar_and_batch_store_are_equal]

/-- One logical proposal per hash; excludes repeated SQL keys and allocations. -/
theorem scalar_page_payload_is_bounded
    (rowCount : Nat) (bounded : rowCount ≤ 128) :
    rowCount * (16 + 32 + 5 * 8) ≤ 11264 := by omega

end H2HDB.Verification.AnalysisScalarBatch
