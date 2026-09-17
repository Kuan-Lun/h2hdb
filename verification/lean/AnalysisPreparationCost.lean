import Std

/-!
# Stage-specific preparation cost

The scalar reference constructs the former marker scan trace: a single-leaf
canonical value requires descriptor, page and parent-edge reads. It counts
client SQL calls, not rows examined, query-plan complexity, CPU, elapsed time or
network round trips. Long canonical trees require a different per-value trace.

The input booleans say whether each validated tag equals the marker. A true tag
stops the scan; repeated tags are revalidated on each visit. Gallery batches and
the construction/independent-validation stages do not share a cache. These are
explicit scalar reference choices. The current content implementation instead
uses the ContentBatches trace, connected to Python by separate finite SQL
correspondence tests. Lean alone does not prove that correspondence.

Content preparation still needs marker validation; the ContentBatches namespace
below models its bounded prefetch implementation. GID preparation has a distinct
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

theorem reference_content_marker_trace (galleries : List (List Bool)) :
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


/-!
The content model starts with a one-row scout and scalar validation. A first
marker, first corruption, or empty input stops before any batch payload read.
After an unmatched first tag, it constructs keyset pages of the remaining tags
with `take 128`/`drop 128`. Each page performs one tag-list query. A nonempty canonical prefetch performs
one identity-family query and, when any single-page value is present, one
page-family query and one parent-edge query. Multi-page values retain their
scalar identity plus page/edge-pair tree traversal. A recognized prefetch
validation failure discards all prefetched results and validates visited tags
in order; marker and corruption both stop the scalar visit prefix. Database
errors are outside this successful-query/family-validation-failure model.

The scalar page count is a mathematical tree-shape input, not a cost-bound
assumption. Its trace is explicitly constructed with two reads per tree page.
Fault constructors cover identity, root-page, edge and final-payload validation
failure phases; they do not enumerate every possible database corruption.
-/
namespace ContentBatches

inductive Query where
  | tagList
  | identity
  | pages
  | edges
  deriving DecidableEq, Repr

inductive Tag where
  | leaf (marker : Bool)
  | tree (pages : Nat)
  | badIdentity
  | badPage
  | badEdges
  | badPayload
  deriving DecidableEq, Repr

inductive Outcome where
  | next
  | marker
  | corrupt
  deriving DecidableEq, Repr

structure Result where
  queries : List Query
  outcome : Outcome
  deriving Repr

structure Prefetch where
  queries : List Query
  usable : Bool
  deriving Repr

def scalarTrace : Tag → List Query
  | .leaf _ => [.identity, .pages, .edges]
  | .tree pages => .identity :: (List.replicate pages [.pages, .edges]).flatten
  | .badIdentity => [.identity]
  | .badPage => [.identity, .pages]
  | .badEdges | .badPayload => [.identity, .pages, .edges]

def outcome : Tag → Outcome
  | .leaf true => .marker
  | .leaf false | .tree _ => .next
  | _ => .corrupt

def isSingle : Tag → Bool
  | .tree _ => false
  | _ => true

def prefetch (tags : List Tag) : Prefetch :=
  if tags.isEmpty then ⟨[], true⟩
  else if .badIdentity ∈ tags then ⟨[.identity], false⟩
  else if tags.any isSingle then
    if .badPage ∈ tags then ⟨[.identity, .pages], false⟩
    else if .badEdges ∈ tags ∨ .badPayload ∈ tags then
      ⟨[.identity, .pages, .edges], false⟩
    else ⟨[.identity, .pages, .edges], true⟩
  else ⟨[.identity], true⟩

def visitTrace (usable : Bool) (tag : Tag) : List Query :=
  if usable && isSingle tag then [] else scalarTrace tag

def visit (usable : Bool) : List Tag → Result
  | [] => ⟨[], .next⟩
  | tag :: remaining =>
      let own := visitTrace usable tag
      match outcome tag with
      | .next =>
          let later := visit usable remaining
          ⟨own ++ later.queries, later.outcome⟩
      | terminal => ⟨own, terminal⟩

def batch (tags : List Tag) : Result :=
  let prepared := prefetch tags
  let visited := visit prepared.usable tags
  ⟨prepared.queries ++ visited.queries, visited.outcome⟩

/-- Fuel is the exact maximum number of keyset calls, including possible EOF. -/
def scanRounds : Nat → List Tag → Result
  | 0, _ => ⟨[], .next⟩
  | rounds + 1, tags =>
      let page := tags.take 128
      let current := batch page
      let own := .tagList :: current.queries
      if current.outcome == .next && page.length == 128 then
        let later := scanRounds rounds (tags.drop 128)
        ⟨own ++ later.queries, later.outcome⟩
      else ⟨own, current.outcome⟩

def batchedScan (tags : List Tag) : Result := scanRounds (tags.length / 128 + 1) tags

/-- A first-row scout avoids speculative payload reads for a leading marker. -/
def scan : List Tag → Result
  | [] => ⟨[.tagList], .next⟩
  | tag :: tail =>
      let own := .tagList :: scalarTrace tag
      match outcome tag with
      | .next =>
          let later := batchedScan tail
          ⟨own ++ later.queries, later.outcome⟩
      | terminal => ⟨own, terminal⟩

def scalarBudget : List Tag → Nat
  | [] => 0
  | tag :: tail => (scalarTrace tag).length + scalarBudget tail

theorem scalar_tree_trace_counts_pages (pages : Nat) :
    (scalarTrace (.tree pages)).length = 1 + 2 * pages := by
  induction pages with
  | zero => rfl
  | succ n ih =>
      simp [scalarTrace, List.replicate_succ] at ih ⊢
      omega

theorem prefetch_cost_at_most_three (tags : List Tag) :
    (prefetch tags).queries.length ≤ 3 := by
  unfold prefetch
  repeat' first | split | simp_all

theorem visit_cost_at_most_scalar (usable : Bool) (tags : List Tag) :
    (visit usable tags).queries.length ≤ scalarBudget tags := by
  induction tags with
  | nil => simp [visit, scalarBudget]
  | cons tag remaining ih =>
      have own : (visitTrace usable tag).length ≤ (scalarTrace tag).length := by
        unfold visitTrace
        split <;> simp
      cases result : outcome tag <;>
        simp [visit, result, scalarBudget] <;> omega

theorem batch_cost_at_most_prefetch_plus_scalar (tags : List Tag) :
    (batch tags).queries.length ≤ 3 + scalarBudget tags := by
  have p := prefetch_cost_at_most_three tags
  have v := visit_cost_at_most_scalar (prefetch tags).usable tags
  simpa [batch] using Nat.add_le_add p v

theorem scalar_budget_append (left right : List Tag) :
    scalarBudget (left ++ right) = scalarBudget left + scalarBudget right := by
  induction left with
  | nil => simp [scalarBudget]
  | cons tag tail ih => simp [scalarBudget, ih, Nat.add_assoc]

theorem scalar_budget_page_partition (tags : List Tag) :
    scalarBudget (tags.take 128) + scalarBudget (tags.drop 128) = scalarBudget tags := by
  rw [← scalar_budget_append, List.take_append_drop]

theorem general_scan_bound (rounds : Nat) (tags : List Tag) :
    (scanRounds rounds tags).queries.length ≤ 4 * rounds + scalarBudget tags := by
  induction rounds generalizing tags with
  | zero => simp [scanRounds]
  | succ rounds ih =>
      have b := batch_cost_at_most_prefetch_plus_scalar (tags.take 128)
      have rest := ih (tags.drop 128)
      have partition := scalar_budget_page_partition tags
      simp only [scanRounds]
      split <;> simp only [List.length_append, List.length_cons] <;> omega

theorem every_constructed_page_is_hard_capped (tags : List Tag) :
    (tags.take 128).length ≤ 128 := by
  simp only [List.length_take]
  exact Nat.min_le_left _ _

theorem chosen_rounds_cover_all_tags (tags : List Tag) :
    tags.length < 128 * (tags.length / 128 + 1) := by
  have modBound := Nat.mod_lt tags.length (by decide : 0 < 128)
  have quotient := Nat.mod_add_div tags.length 128
  omega

/-- The computed fuel cannot hide work: any extra fuel emits the same trace. -/
theorem adequate_rounds_are_stable (rounds extra : Nat) (tags : List Tag)
    (adequate : tags.length < 128 * rounds) :
    scanRounds (rounds + extra) tags = scanRounds rounds tags := by
  induction rounds generalizing tags with
  | zero => omega
  | succ rounds ih =>
      simp only [Nat.succ_add, scanRounds]
      split
      · have pageFull : (tags.take 128).length = 128 := by
          simp_all only [Bool.and_eq_true, beq_iff_eq]
        have remaining : (tags.drop 128).length < 128 * rounds := by
          simp only [List.length_take] at pageFull
          simp only [List.length_drop]
          omega
        rw [ih (tags.drop 128) remaining]
      · rfl

theorem chosen_batch_scan_cannot_be_truncated (extra : Nat) (tags : List Tag) :
    scanRounds (tags.length / 128 + 1 + extra) tags = batchedScan tags := by
  exact adequate_rounds_are_stable _ _ _ (chosen_rounds_cover_all_tags tags)

theorem prefetch_preserves_ordered_validation_outcome (usable : Bool) (tags : List Tag) :
    (visit usable tags).outcome = (visit false tags).outcome := by
  induction tags with
  | nil => rfl
  | cons tag remaining ih =>
      cases found : outcome tag <;> simp [visit, found, ih]

theorem marker_prefix_ignores_later_corruption (usable : Bool) (tail : List Tag) :
    (visit usable (.leaf true :: tail)).outcome = .marker := by
  simp [visit, outcome]

theorem corrupt_prefix_cannot_be_hidden_by_later_marker (usable : Bool) (tail : List Tag) :
    (visit usable (.badPayload :: tail)).outcome = .corrupt := by
  simp [visit, outcome]

/-- Direct upper bound derived from emitted operations; no input cost premise. -/
theorem batched_content_scan_bound (tags : List Tag) :
    (batchedScan tags).queries.length ≤ 4 * (tags.length / 128 + 1) + scalarBudget tags :=
  general_scan_bound _ _

/-- Single-page successful prefetch removes all per-tag SQL validation visits. -/
theorem leaf_visit_issues_no_sql (tags : List Bool) :
    (visit true (tags.map Tag.leaf)).queries = [] := by
  induction tags with
  | nil => rfl
  | cons tag tail ih => cases tag <;> simp [visit, visitTrace, isSingle, outcome, ih]

theorem leaf_prefetch_is_usable (tags : List Bool) :
    (prefetch (tags.map Tag.leaf)).usable = true := by
  cases tags with
  | nil => rfl
  | cons tag tail => simp [prefetch, isSingle]

theorem leaf_batch_cost_at_most_three (tags : List Bool) :
    (batch (tags.map Tag.leaf)).queries.length ≤ 3 := by
  simp [batch, leaf_prefetch_is_usable, leaf_visit_issues_no_sql]
  exact prefetch_cost_at_most_three _

theorem leaf_scan_bound (rounds : Nat) (tags : List Bool) :
    (scanRounds rounds (tags.map Tag.leaf)).queries.length ≤ 4 * rounds := by
  induction rounds generalizing tags with
  | zero => simp [scanRounds]
  | succ rounds ih =>
      have b := leaf_batch_cost_at_most_three (tags.take 128)
      have rest := ih (tags.drop 128)
      simp only [List.map_take, List.map_drop] at b rest
      simp only [scanRounds]
      split <;> simp only [List.length_append, List.length_cons] <;> omega

theorem single_leaf_batch_scan_bound (tags : List Bool) :
    (batchedScan (tags.map Tag.leaf)).queries.length ≤ 4 * (tags.length / 128 + 1) := by
  simpa [batchedScan] using leaf_scan_bound (tags.length / 128 + 1) tags

/-- The scout costs one list query and the first value's scalar tree trace. -/
theorem content_scan_bound (tag : Tag) (tail : List Tag) :
    (scan (tag :: tail)).queries.length ≤
      1 + (scalarTrace tag).length + 4 * (tail.length / 128 + 1) + scalarBudget tail := by
  have later := batched_content_scan_bound tail
  cases found : outcome tag <;>
    simp only [scan, found, List.length_append, List.length_cons] <;> omega

theorem single_leaf_content_scan_bound (first : Bool) (tail : List Bool) :
    (scan ((first :: tail).map Tag.leaf)).queries.length ≤
      4 + 4 * (tail.length / 128 + 1) := by
  have later := single_leaf_batch_scan_bound tail
  cases first <;> simp [scan, outcome, scalarTrace] <;> omega

theorem empty_content_scan_issues_one_list_query :
    (scan []).queries = [.tagList] := rfl

theorem first_marker_scout_never_reads_tail (tail : List Tag) :
    (scan (.leaf true :: tail)).queries = [.tagList, .identity, .pages, .edges] ∧
      (scan (.leaf true :: tail)).outcome = .marker := by
  simp [scan, outcome, scalarTrace]

/-- No cross-gallery or cross-stage memoization is present in the algorithm. -/
def stageTrace : List (List Tag) → List Query
  | [] => []
  | gallery :: remaining => (scan gallery).queries ++ stageTrace remaining

def independentContentStages (galleries : List (List Tag)) : List Query :=
  stageTrace galleries ++ stageTrace galleries ++ stageTrace galleries

theorem three_content_stages_repeat_the_actual_trace (galleries : List (List Tag)) :
    (independentContentStages galleries).length = 3 * (stageTrace galleries).length := by
  simp [independentContentStages]
  omega

def listQueries (queries : List Query) : Nat :=
  (queries.filter (· == .tagList)).length

def canonicalQueries (queries : List Query) : Nat :=
  (queries.filter (· != .tagList)).length

end ContentBatches

end H2HDB.Verification.AnalysisPreparationCost

/-- Executable model samples; tests compare these with measured client SQL. -/
def main (args : List String) : IO Unit := do
  if args == ["--content"] then
    let report := fun (kind : String) (input : Nat)
        (tags : List H2HDB.Verification.AnalysisPreparationCost.ContentBatches.Tag) => do
      let queries := (H2HDB.Verification.AnalysisPreparationCost.ContentBatches.scan tags).queries
      let lists := H2HDB.Verification.AnalysisPreparationCost.ContentBatches.listQueries queries
      let canonical := H2HDB.Verification.AnalysisPreparationCost.ContentBatches.canonicalQueries queries
      IO.println s!"{kind},{input},{lists},{canonical}"
    for tags in [0, 1, 127, 128, 129, 130, 256, 512, 1024] do
      report "uniform" tags (List.replicate tags (.leaf false))
    for preceding in [0, 1, 127, 128, 129] do
      report "marker" preceding (List.replicate preceding (.leaf false) ++
        [.leaf true, .leaf false, .leaf false])
    for bytes in [32769, 65536] do
      report "multileaf" bytes [.tree 3]
      report "mixed" bytes [.leaf false, .tree 3]
    report "fault-before-marker" 0 [.leaf false, .badPage, .leaf true]
    report "marker-before-fault" 0 [.leaf false, .leaf true, .badPage]
    return
  for galleries in [0, 1, 2] do
    for tags in [0, 1, 3, 127, 128, 129] do
      let source := List.replicate galleries (List.replicate tags false)
      let cost := H2HDB.Verification.AnalysisPreparationCost.independentGidStages source
      IO.println s!"uniform,{galleries},{tags},{cost.length}"
  for preceding in [0, 1, 127, 128] do
    let source := List.replicate preceding false ++ [true, false, false]
    let cost := H2HDB.Verification.AnalysisPreparationCost.markerScan source
    IO.println s!"marker,1,{preceding},{cost.length}"
