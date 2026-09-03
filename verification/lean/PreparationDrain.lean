import Std

/-!
# Bounded superseded-preparation drainage

An operational preparation attempt of a build that can never publish (its
policy or deletion generation was superseded, or its build is being retired)
is drained in bounded pages.  Production reads the durable *position*: the
least `(state, preparation_id)` still matching the drain predicate, by one
index seek per drain state; one page is the first `pageLimit` matching rows of
that state in `preparation_id` order, and the transaction abandons exactly
those rows.

This model abstracts the matching rows of one state as a strictly ascending
list of keys (the physical index order).  It proves the pure properties the
runtime protocol relies on:

* a page is hard bounded by `pageLimit`;
* a committed page strictly shrinks the matching set, so the drain measure
  is a well-founded progress measure;
* draining `n` rows takes exactly `⌈n / pageLimit⌉` committed pages, and is
  not finished one page earlier;
* every key of a committed page is strictly below every key that still
  matches afterwards, so the next durable position is strictly greater than
  the position the page started from (the liveness fence);
* committing a page removes exactly its keys from the matching set, and
  committing the same page twice (a lost commit response replayed) changes
  nothing more than committing it once.

The theorems are unbounded over row counts, keys and page limit.  They assume
the SQL page is the exact `pageLimit`-prefix of the ascending matching keys,
which the query-plan and 129/257-row tests establish per backend.  They do not
prove SQL ordering, transaction atomicity, index usage, or that Python
implements this model; runtime, fault and live-backend tests supply that
evidence.
-/

namespace H2HDB.Verification.PreparationDrain

/-- The hard cap of one drainage page. -/
def pageLimit : Nat := 128

/-- One page: the least `pageLimit` matching keys in seek order. -/
def page (matching : List Nat) : List Nat :=
  matching.take pageLimit

/-- The keys still matching after that page committed. -/
def afterPage (matching : List Nat) : List Nat :=
  matching.drop pageLimit

theorem page_is_hard_bounded (matching : List Nat) :
    (page matching).length ≤ pageLimit := by
  simpa [page] using Nat.min_le_left pageLimit matching.length

theorem length_positive_of_nonempty (matching : List Nat) (nonempty : matching ≠ []) :
    0 < matching.length := by
  cases matching with
  | nil => exact absurd rfl nonempty
  | cons _ _ => simp

theorem committed_page_strictly_shrinks_the_measure
    (matching : List Nat) (nonempty : matching ≠ []) :
    (afterPage matching).length < matching.length := by
  have positive := length_positive_of_nonempty matching nonempty
  simp only [afterPage, List.length_drop, pageLimit]
  omega

/-- The keys still matching after `pages` committed pages. -/
def afterPages : Nat → List Nat → List Nat
  | 0, matching => matching
  | pages + 1, matching => afterPages pages (afterPage matching)

/-- The exact number of committed pages that drain `count` matching rows. -/
def pagesNeeded (count : Nat) : Nat :=
  (count + pageLimit - 1) / pageLimit

theorem after_pages_is_drop (pages : Nat) (matching : List Nat) :
    afterPages pages matching = matching.drop (pageLimit * pages) := by
  induction pages generalizing matching with
  | zero => simp [afterPages]
  | succ pages ih =>
    rw [afterPages, ih, afterPage, List.drop_drop, Nat.mul_succ, Nat.add_comm]

theorem drained_after_pages_needed (matching : List Nat) :
    afterPages (pagesNeeded matching.length) matching = [] := by
  rw [after_pages_is_drop, List.drop_eq_nil_iff]
  simp only [pagesNeeded, pageLimit]
  omega

theorem not_drained_one_page_earlier
    (matching : List Nat) (nonempty : matching ≠ []) :
    afterPages (pagesNeeded matching.length - 1) matching ≠ [] := by
  have positive := length_positive_of_nonempty matching nonempty
  rw [after_pages_is_drop, Ne, List.drop_eq_nil_iff]
  simp only [pagesNeeded, pageLimit]
  omega

/--
The liveness fence: every key of the committed page is strictly below every
key that still matches afterwards.  The next durable position (the least
remaining key) is therefore strictly greater than the position the committed
page started from (its least key).
-/
theorem committed_page_is_strictly_below_every_remaining_key
    (matching : List Nat) (ascending : matching.Pairwise (· < ·))
    (committed : Nat) (inPage : committed ∈ page matching)
    (remaining : Nat) (afterwards : remaining ∈ afterPage matching) :
    committed < remaining := by
  have split := List.take_append_drop pageLimit matching
  rw [← split] at ascending
  exact (List.pairwise_append.1 ascending).2.2 committed inPage remaining afterwards

/-- A durable row: its key and whether it is already ABANDONED. -/
structure Row where
  key : Nat
  abandoned : Bool
deriving DecidableEq, Repr

/-- The keys of the rows that still match the drain predicate. -/
def matching (rows : List Row) : List Nat :=
  (rows.filter (fun row => !row.abandoned)).map Row.key

/-- Commit one page: abandon every row whose key is in the page. -/
def commit (rows : List Row) (keys : List Nat) : List Row :=
  rows.map (fun row => if row.key ∈ keys then { row with abandoned := true } else row)

theorem commit_removes_exactly_the_page_keys (rows : List Row) (keys : List Nat) :
    matching (commit rows keys) = (matching rows).filter (fun key => !decide (key ∈ keys)) := by
  induction rows with
  | nil => rfl
  | cons row rest ih =>
    by_cases member : row.key ∈ keys <;> by_cases gone : row.abandoned <;>
      simp [matching, commit, member, gone] at ih ⊢ <;>
      simpa [matching, commit] using ih

theorem commit_is_idempotent (rows : List Row) (keys : List Nat) :
    commit (commit rows keys) keys = commit rows keys := by
  simp only [commit, List.map_map]
  apply List.map_congr_left
  intro row _
  by_cases member : row.key ∈ keys <;> simp [member]

theorem abandoned_rows_stay_abandoned (rows : List Row) (keys : List Nat)
    (key : Nat) (present : { key := key, abandoned := true } ∈ rows) :
    { key := key, abandoned := true } ∈ commit rows keys := by
  simp only [commit, List.mem_map]
  refine ⟨{ key := key, abandoned := true }, present, ?_⟩
  by_cases member : key ∈ keys <;> simp [member]

end H2HDB.Verification.PreparationDrain
