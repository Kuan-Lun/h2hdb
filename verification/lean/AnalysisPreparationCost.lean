import Std

/-!
# Stage-specific preparation cost

This model constructs the read trace of a marker scan: a single-leaf canonical
value requires a descriptor read, a page read and a parent-edge read. It counts
client SQL calls, not rows examined, query-plan complexity, CPU, elapsed time or
network round trips. Long canonical trees require a different per-value trace.

The input booleans say whether each validated tag equals the marker. A true tag
stops the scan; repeated tags are revalidated on each visit. Gallery batches and
the construction/independent-validation stages do not share a cache. These are
explicit model choices, checked against real Python preparation in the separate
finite SQL correspondence tests. Lean alone does not prove that correspondence.

Content preparation still needs marker reads. GID preparation has a distinct
capability and omits that dependency. The stage selector below models that
algorithmic choice; finite SQL tests establish correspondence with Python.
The zero-cost theorem concerns canonical tag reads only: full metadata stream
validation, qualification, membership and fencing remain required. Metadata
pages can grow with input bytes; no theorem below claims constant total SQL.
-/

namespace H2HDB.Verification.AnalysisPreparationCost

inductive Read where
  | descriptor
  | page
  | parentEdges
  deriving DecidableEq, Repr

def singleLeafRead : List Read := [.descriptor, .page, .parentEdges]

def markerScan : List Bool → List Read
  | [] => []
  | matched :: remaining =>
      singleLeafRead ++ if matched then [] else markerScan remaining

def visitedTags : List Bool → Nat
  | [] => 0
  | matched :: remaining => 1 + if matched then 0 else visitedTags remaining

theorem marker_scan_counts_actual_visits (tags : List Bool) :
    (markerScan tags).length = 3 * visitedTags tags := by
  induction tags with
  | nil => rfl
  | cons matched remaining ih =>
      cases matched <;> simp [markerScan, visitedTags, singleLeafRead, ih] <;> omega

theorem no_marker_visits_every_tag (tags : Nat) :
    visitedTags (List.replicate tags false) = tags := by
  induction tags with
  | zero => rfl
  | succ count ih => simp [List.replicate_succ, visitedTags, ih, Nat.add_comm]

theorem first_marker_stops_later_reads (preceding : Nat) (tail : List Bool) :
    visitedTags (List.replicate preceding false ++ true :: tail) = preceding + 1 := by
  induction preceding with
  | zero => simp [visitedTags]
  | succ count ih =>
      simp [List.replicate_succ, visitedTags, ih, Nat.add_comm]

def stageTrace : List (List Bool) → List Read
  | [] => []
  | gallery :: remaining => markerScan gallery ++ stageTrace remaining

def stageVisits : List (List Bool) → Nat
  | [] => 0
  | gallery :: remaining => visitedTags gallery + stageVisits remaining

theorem gallery_costs_are_additive (galleries : List (List Bool)) :
    (stageTrace galleries).length = 3 * stageVisits galleries := by
  induction galleries with
  | nil => rfl
  | cons gallery remaining ih =>
      simp [stageTrace, stageVisits, marker_scan_counts_actual_visits, ih, Nat.mul_add]

theorem stage_trace_append (left right : List (List Bool)) :
    stageTrace (left ++ right) = stageTrace left ++ stageTrace right := by
  induction left with
  | nil => simp [stageTrace]
  | cons gallery remaining ih => simp [stageTrace, ih, List.append_assoc]

def pagedTrace : List (List (List Bool)) → List Read
  | [] => []
  | page :: remaining => stageTrace page ++ pagedTrace remaining

theorem bounded_batches_do_not_remove_total_work
    (pages : List (List (List Bool))) :
    pagedTrace pages = stageTrace pages.flatten := by
  induction pages with
  | nil => rfl
  | cons page remaining ih => simp [pagedTrace, stage_trace_append, ih]

def repeatedMarkerStages (galleries : List (List Bool)) : List Read :=
  stageTrace galleries ++ stageTrace galleries

theorem independent_validation_repeats_preparation_cost
    (galleries : List (List Bool)) :
    (repeatedMarkerStages galleries).length = 6 * stageVisits galleries := by
  simp [repeatedMarkerStages, gallery_costs_are_additive]
  omega

theorem uniform_gallery_visits (galleries tags : Nat) :
    stageVisits (List.replicate galleries (List.replicate tags false)) =
      galleries * tags := by
  induction galleries with
  | zero => simp [stageVisits]
  | succ count ih =>
      simp [List.replicate_succ, stageVisits, no_marker_visits_every_tag, ih,
        Nat.succ_mul, Nat.add_comm]

theorem repeated_marker_stages_have_six_queries_per_gallery_tag (galleries tags : Nat) :
    (repeatedMarkerStages
      (List.replicate galleries (List.replicate tags false))).length =
      6 * galleries * tags := by
  simp [independent_validation_repeats_preparation_cost, uniform_gallery_visits,
    Nat.mul_assoc]

inductive PreparationKind where
  | content
  | gid

def canonicalTagReads (kind : PreparationKind) (tags : List Bool) : List Read :=
  match kind with
  | .content => markerScan tags
  | .gid => []

def preparationTagTrace (kind : PreparationKind) : List (List Bool) → List Read
  | [] => []
  | gallery :: remaining =>
      canonicalTagReads kind gallery ++ preparationTagTrace kind remaining

theorem gid_preparation_does_not_read_canonical_tags (galleries : List (List Bool)) :
    preparationTagTrace .gid galleries = [] := by
  induction galleries with
  | nil => rfl
  | cons gallery remaining ih => simp [preparationTagTrace, canonicalTagReads, ih]

theorem content_preparation_retains_marker_validation (galleries : List (List Bool)) :
    preparationTagTrace .content galleries = stageTrace galleries := by
  induction galleries with
  | nil => rfl
  | cons gallery remaining ih =>
      simp [preparationTagTrace, canonicalTagReads, stageTrace, ih]

def independentGidStages (galleries : List (List Bool)) : List Read :=
  preparationTagTrace .gid galleries ++ preparationTagTrace .gid galleries

theorem independent_gid_stages_have_zero_canonical_tag_queries
    (galleries : List (List Bool)) :
    (independentGidStages galleries).length = 0 := by
  simp [independentGidStages, gid_preparation_does_not_read_canonical_tags]

theorem gid_canonical_tag_cost_is_independent_of_tag_payloads
    (left right : List (List Bool)) :
    independentGidStages left = independentGidStages right := by
  simp [independentGidStages, gid_preparation_does_not_read_canonical_tags]

end H2HDB.Verification.AnalysisPreparationCost

/-- Executable model samples; tests compare these with measured client SQL. -/
def main : IO Unit := do
  for galleries in [0, 1, 2] do
    for tags in [0, 1, 3, 127, 128, 129] do
      let source := List.replicate galleries (List.replicate tags false)
      let cost := H2HDB.Verification.AnalysisPreparationCost.independentGidStages source
      IO.println s!"uniform,{galleries},{tags},{cost.length}"
  for preceding in [0, 1, 127, 128] do
    let source := List.replicate preceding false ++ [true, false, false]
    let cost := H2HDB.Verification.AnalysisPreparationCost.markerScan source
    IO.println s!"marker,1,{preceding},{cost.length}"
