import Std

/-!
# Bounded catalog-child hydration refinement

Contributor and subject rows are already ordered by the immutable SQL
coordinate `(publication_key, position)`.  Production takes a prefix of at
most 128 rows, decodes that page with a page-local canonical cache, and resumes
strictly after its last coordinate.  This model proves that one such bounded
prefix step preserves the exact unpaged decoded sequence and never changes its
order.  Repeated application therefore refines the reference scan while
keeping every SQL result and canonical-prefetch batch bounded.

The theorems are unbounded over row and decoded-value types and over the number
of child rows.  They assume SQL returns the exact strictly ordered suffix named
by the keyset cursor.  They do not prove Python loop progress, SQL ordering,
canonical page validation, or SQLite/MariaDB query plans; runtime differential,
boundary-corruption, and backend tests provide that evidence.
-/

namespace H2HDB.Verification.CatalogChildHydration

def pageLimit : Nat := 128

def nextPage (rows : List Row) : List Row :=
  rows.take pageLimit

def remainingAfterPage (rows : List Row) : List Row :=
  rows.drop pageLimit

theorem next_page_is_hard_bounded (rows : List Row) :
    (nextPage rows).length ≤ pageLimit := by
  simpa [nextPage] using Nat.min_le_left pageLimit rows.length

theorem page_local_canonical_batch_is_hard_bounded
    (reference : Row → Reference)
    (rows : List Row) :
    ((nextPage rows).map reference).length ≤ pageLimit := by
  simpa using next_page_is_hard_bounded rows

theorem page_then_tail_preserves_exact_decoded_order
    (decode : Row → Value)
    (rows : List Row) :
    (nextPage rows).map decode ++
        (remainingAfterPage rows).map decode = rows.map decode := by
  simp [nextPage, remainingAfterPage]

structure HydrationState (Value Row : Type) where
  emitted : List Value
  remaining : List Row

def step
    (decode : Row → Value)
    (state : HydrationState Value Row) : HydrationState Value Row :=
  {
    emitted := state.emitted ++ (nextPage state.remaining).map decode
    remaining := remainingAfterPage state.remaining
  }

def RefinesReference
    (decode : Row → Value)
    (reference : List Value)
    (state : HydrationState Value Row) : Prop :=
  state.emitted ++ state.remaining.map decode = reference

theorem bounded_keyset_step_preserves_reference
    (decode : Row → Value)
    (reference : List Value)
    (state : HydrationState Value Row)
    (refines : RefinesReference decode reference state) :
    RefinesReference decode reference (step decode state) := by
  unfold RefinesReference step
  simp only
  rw [List.append_assoc, page_then_tail_preserves_exact_decoded_order]
  exact refines

theorem empty_tail_is_exact_completion
    (decode : Row → Value)
    (reference : List Value)
    (state : HydrationState Value Row)
    (refines : RefinesReference decode reference state)
    (done : state.remaining = []) :
    state.emitted = reference := by
  unfold RefinesReference at refines
  rw [done] at refines
  simpa using refines

end H2HDB.Verification.CatalogChildHydration
