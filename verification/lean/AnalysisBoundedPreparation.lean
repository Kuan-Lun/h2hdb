import Std

/-!
# Bounded analysis lookup and spool refinement

A source list contains digest identities in canonical order, including repeated
digests. A lookup returns either a missing decision or a policy-derived keep
boolean. The batch theorem requires equality with durable authority on every
source digest; production validates exact analysis/digest membership, rejects
duplicates, and validates scalar domains before using a bounded page map.

The partition theorem proves that source page boundaries preserve the complete
ordered result and propagate missing decisions. The spool is local to one
independent preparation snapshot: after consuming the source, count and both
codec traversals read that same ordered sequence. No theorem authorizes reuse
between a construction stage and its independent validation stage.

These theorems are unbounded over source length, repetitions, page boundaries,
lookup functions and codec functions. The logical byte bounds cover fixed-width
scalar payloads, not Python objects, protocol encoding, driver allocation or RSS.
SQL extraction, pinned snapshots, disk I/O, checksum collision resistance and
the concrete codecs are implementation assumptions checked separately by runtime
differential, fault and backend tests; Lean does not prove those effects.
-/

namespace H2HDB.Verification.AnalysisBoundedPreparation

abbrev Digest := Nat
abbrev Lookup := Digest → Option Bool

def select (lookup : Lookup) : List Digest → Option (List Digest)
  | [] => some []
  | digest :: tail => do
      let keep ← lookup digest
      let selected ← select lookup tail
      pure (if keep then digest :: selected else selected)

theorem exact_page_lookup_equals_per_file_reference
    (authority batch : Lookup)
    (source : List Digest)
    (exact : ∀ digest ∈ source, batch digest = authority digest) :
    select batch source = select authority source := by
  induction source with
  | nil => rfl
  | cons digest tail ih =>
      have headExact := exact digest (by simp)
      have tailExact : ∀ value ∈ tail, batch value = authority value := by
        intro value member
        exact exact value (by simp [member])
      simp only [select, headExact, ih tailExact]

theorem missing_decision_fails_closed
    (lookup : Lookup)
    (digest : Digest)
    (tail : List Digest)
    (missing : lookup digest = none) :
    select lookup (digest :: tail) = none := by
  simp [select, missing]

theorem select_append
    (lookup : Lookup)
    (left right : List Digest) :
    select lookup (left ++ right) = (do
      let first ← select lookup left
      let second ← select lookup right
      pure (first ++ second)) := by
  induction left with
  | nil => simp [select]
  | cons digest tail ih =>
      simp only [List.cons_append, select, ih]
      cases decision : lookup digest with
      | none => simp
      | some keep =>
          cases first : select lookup tail with
          | none => simp
          | some values =>
              cases second : select lookup right with
              | none => simp
              | some remaining => cases keep <;> simp

def selectPages (lookup : Lookup) : List (List Digest) → Option (List Digest)
  | [] => some []
  | page :: tail => do
      let selected ← select lookup page
      let following ← selectPages lookup tail
      pure (selected ++ following)

theorem page_partition_preserves_order_duplicates_and_failure
    (lookup : Lookup)
    (pages : List (List Digest)) :
    selectPages lookup pages = select lookup pages.flatten := by
  induction pages with
  | nil => rfl
  | cons page tail ih =>
      simp only [selectPages, List.flatten_cons, select_append, ih]

/-- Records appended to disk are kept in exact source order. -/
def spoolPages : List Digest → List (List Digest) → List Digest
  | preceding, [] => preceding
  | preceding, page :: tail => spoolPages (preceding ++ page) tail

theorem disk_spool_contains_exact_ordered_records
    (preceding : List Digest)
    (pages : List (List Digest)) :
    spoolPages preceding pages = preceding ++ pages.flatten := by
  induction pages generalizing preceding with
  | nil => simp [spoolPages]
  | cons page tail ih =>
      simp [spoolPages, ih, List.append_assoc]

theorem spool_count_and_each_codec_equal_source
    (pages : List (List Digest))
    (codec : Nat → List Digest → Nat) :
    let spool := spoolPages [] pages
    spool.length = pages.flatten.length ∧
      codec spool.length spool = codec pages.flatten.length pages.flatten := by
  simp [disk_spool_contains_exact_ordered_records]

theorem source_page_logical_bytes_bounded
    (rows : Nat)
    (bounded : rows ≤ 128) :
    rows * 40 ≤ 5120 := by omega

theorem decision_page_with_rejection_sentinel_logical_bytes_bounded
    (rows : Nat)
    (bounded : rows ≤ 129) :
    rows * 72 ≤ 9288 := by omega

/-- Qualification selects whole observations before any global file analysis.
    The original gallery list remains available as complete source membership. -/
def qualifiedSourceDigests : List (Bool × List Digest) → List Digest
  | [] => []
  | (accepted, digests) :: tail =>
      if accepted then digests ++ qualifiedSourceDigests tail
      else qualifiedSourceDigests tail

theorem rejected_gallery_contributes_no_digests
    (digests : List Digest) (remaining : List (Bool × List Digest)) :
    qualifiedSourceDigests ((false, digests) :: remaining) =
      qualifiedSourceDigests remaining := by
  simp [qualifiedSourceDigests]

theorem accepted_gallery_preserves_all_ordered_digests
    (digests : List Digest) (remaining : List (Bool × List Digest)) :
    qualifiedSourceDigests ((true, digests) :: remaining) =
      digests ++ qualifiedSourceDigests remaining := by
  simp [qualifiedSourceDigests]

theorem qualification_preserves_append
    (left right : List (Bool × List Digest)) :
    qualifiedSourceDigests (left ++ right) =
      qualifiedSourceDigests left ++ qualifiedSourceDigests right := by
  induction left with
  | nil => simp [qualifiedSourceDigests]
  | cons head tail ih =>
      rcases head with ⟨accepted, digests⟩
      cases accepted <;> simp [qualifiedSourceDigests, ih, List.append_assoc]

end H2HDB.Verification.AnalysisBoundedPreparation
