import Std

/-!
# Bounded DIRECTORY matching

A FILE batch carries its canonical ordinal order. A DIRECTORY traversal uses a
separate name-sorted view, partitions it into disjoint child ranges and validates
leaf matches. The original batch, not the traversal order, forms the receipt.

The equivalence theorem below quantifies over arbitrary finite partitions and
checks; it proves that grouping does not omit, add or weaken any target check.
The permutation theorem covers an arbitrary reordering, including byte-name
sorting, without assuming FILE ordinals sort by name. The interval theorem
establishes unique routing for disjoint ordered sibling ranges. A failure in any
partition propagates; the receipt theorem preserves original FILE order.

Production must establish all hypotheses from one transaction's durable root,
canonical decoded bytes and their SHA-256/normalized descriptors, exact sibling
membership and strict nonoverlapping bounds. This file does not establish that
Python implements the permutation/partition, that SQL sees a pinned snapshot,
that SHA-256 is collision-free, or that a database transaction is crash-atomic.
Those are separate codec, differential, corruption, rollback and backend tests.
The resource bound is for logical serialized page/routing bytes, not Python
objects, native driver allocation or process RSS; it does not bound total pages
visited or whole-gallery size.
-/

namespace H2HDB.Verification.DirectoryBatchMatching

abbrev Name := Nat

def accepts (check : Name → Bool) (targets : List Name) : Bool :=
  targets.all check

def grouped (check : Name → Bool) (groups : List (List Name)) : Bool :=
  groups.all (accepts check)

theorem partition_preserves_every_exact_match
    (check : Name → Bool) (groups : List (List Name)) :
    grouped check groups = accepts check groups.flatten := by
  induction groups with
  | nil => rfl
  | cons group rest ih =>
      simp [grouped, accepts, List.all_append] at *
      exact congrArg (fun value => group.all check && value) ih

theorem name_sort_preserves_validation
    (check : Name → Bool) (original sorted : List Name)
    (reordering : original.Perm sorted) :
    accepts check original = accepts check sorted := by
  apply Bool.eq_iff_iff.mpr
  simp only [accepts, List.all_eq_true]
  constructor
  · intro exact name member
    exact exact name ((List.Perm.mem_iff reordering).mpr member)
  · intro exact name member
    exact exact name ((List.Perm.mem_iff reordering).mp member)

theorem grouped_permutation_equals_file_reference
    (check : Name → Bool) (original : List Name) (groups : List (List Name))
    (partition : original.Perm groups.flatten) :
    grouped check groups = accepts check original := by
  rw [partition_preserves_every_exact_match]
  exact (name_sort_preserves_validation check original groups.flatten partition).symm

theorem missing_target_fails_closed
    (check : Name → Bool) (targets : List Name) (name : Name)
    (present : name ∈ targets) (missing : check name = false) :
    accepts check targets = false := by
  cases result : accepts check targets with
  | false => rfl
  | true =>
      have exact := (List.all_eq_true.mp result) name present
      rw [missing] at exact
      contradiction

structure Range where
  first : Name
  last : Name

def within (range : Range) (name : Name) : Prop :=
  range.first ≤ name ∧ name ≤ range.last

theorem disjoint_sibling_ranges_route_at_most_once
    (left right : Range) (name : Name)
    (ordered : left.last < right.first) :
    ¬ (within left name ∧ within right name) := by
  intro both
  have leftLast := both.1.2
  have rightFirst := both.2.1
  exact (Nat.not_lt_of_ge (Nat.le_trans rightFirst leftLast)) ordered

/-- Validation changes no row sequence: the caller encodes its original batch. -/
def receiptRows (original : List Name) (_validatedTraversal : List Name) : List Name :=
  original

theorem receipt_keeps_original_file_order
    (original traversal : List Name) : receiptRows original traversal = original := rfl

theorem active_path_logical_bytes_bounded
    (branchFrames targets pageBytes : Nat)
    (depthBound : branchFrames ≤ 8)
    (targetBound : targets ≤ 256)
    (pageBound : pageBytes ≤ 65536) :
    branchFrames * (256 * (32 + 8 + 255 + 255)) +
        targets * 255 + pageBytes ≤ 1257216 := by omega

end H2HDB.Verification.DirectoryBatchMatching
