import Std

/-!
# Exact artifact delta classification

This module specifies the four artifact operations independently of the SQL
schema.  An artifact key is in the work set exactly when it exists in either
the old or the new semantic-input map.  Presence and complete semantic-input
equality then determine one and only one operation:

* absent -> present: `create`;
* present -> absent: `delete`;
* present -> equal present: `unchanged`;
* present -> different present: `rebuild`.

The theorems are unbounded over keys and values.  Runtime refinement still has
to prove that SQL old/new rows encode `ArtifactInput` exactly and that its
operation relation contains precisely this pointwise result.
-/

namespace H2HDB.Verification.ArtifactDelta

inductive ArtifactOperation where
  | create
  | rebuild
  | delete
  | unchanged
deriving DecidableEq, Repr

/--
Every value that can change an artifact's bytes, identity, or publication
membership belongs to the semantic input.  Optional fields remain explicit so
absence cannot be confused with an empty digest or gallery identifier.
-/
structure ArtifactInput
    (Manifest MemberPlan Content Gallery Policy : Type) where
  sourceManifest : Manifest
  /-- Exact ordered source/generated entries, exclusions, names, and transforms. -/
  memberPlan : MemberPlan
  effectiveContent : Option Content
  selected : Bool
  owner : Option Gallery
  policy : Policy
deriving DecidableEq, Repr

/-- Classify one key in the union of the old and new artifact maps. -/
def classify [DecidableEq Input]
    (oldInput newInput : Option Input) : Option ArtifactOperation :=
  match oldInput, newInput with
  | none, none => none
  | none, some _ => some .create
  | some _, none => some .delete
  | some oldValue, some newValue =>
      if oldValue = newValue then some .unchanged else some .rebuild

theorem classify_none_iff [DecidableEq Input]
    (oldInput newInput : Option Input) :
    classify oldInput newInput = none ↔
      oldInput = none ∧ newInput = none := by
  cases oldInput with
  | none => cases newInput <;> simp [classify]
  | some oldValue =>
      cases newInput with
      | none => simp [classify]
      | some newValue =>
          by_cases same : oldValue = newValue
          · simp [classify, same]
          · simp [classify, same]

theorem classify_create_iff [DecidableEq Input]
    (oldInput newInput : Option Input) :
    classify oldInput newInput = some .create ↔
      oldInput = none ∧ ∃ newValue, newInput = some newValue := by
  cases oldInput with
  | none => cases newInput <;> simp [classify]
  | some oldValue =>
      cases newInput with
      | none => simp [classify]
      | some newValue =>
          by_cases same : oldValue = newValue
          · simp [classify, same]
          · simp [classify, same]

theorem classify_delete_iff [DecidableEq Input]
    (oldInput newInput : Option Input) :
    classify oldInput newInput = some .delete ↔
      (∃ oldValue, oldInput = some oldValue) ∧ newInput = none := by
  cases oldInput with
  | none => cases newInput <;> simp [classify]
  | some oldValue =>
      cases newInput with
      | none => simp [classify]
      | some newValue =>
          by_cases same : oldValue = newValue <;> simp [classify, same]

theorem classify_unchanged_iff [DecidableEq Input]
    (oldInput newInput : Option Input) :
    classify oldInput newInput = some .unchanged ↔
      ∃ value, oldInput = some value ∧ newInput = some value := by
  cases oldInput with
  | none => cases newInput <;> simp [classify]
  | some oldValue =>
      cases newInput with
      | none => simp [classify]
      | some newValue =>
          by_cases same : oldValue = newValue
          · simp [classify, same]
          · have reverse : newValue ≠ oldValue := fun equal => same equal.symm
            simp [classify, same, reverse]

theorem classify_rebuild_iff [DecidableEq Input]
    (oldInput newInput : Option Input) :
    classify oldInput newInput = some .rebuild ↔
      ∃ oldValue newValue,
        oldInput = some oldValue ∧
          newInput = some newValue ∧ oldValue ≠ newValue := by
  cases oldInput with
  | none => cases newInput <;> simp [classify]
  | some oldValue =>
      cases newInput with
      | none => simp [classify]
      | some newValue =>
          by_cases same : oldValue = newValue <;> simp [classify, same]

/--
A member-plan change can never be hidden by an equal aggregate content digest
or by equal selection/owner/policy fields: the complete semantic input differs,
so two present artifacts classify strictly as `rebuild`.
-/
theorem member_plan_difference_forces_rebuild
    [DecidableEq (ArtifactInput Manifest MemberPlan Content Gallery Policy)]
    (oldInput newInput : ArtifactInput Manifest MemberPlan Content Gallery Policy)
    (changed : oldInput.memberPlan ≠ newInput.memberPlan) :
    classify (some oldInput) (some newInput) = some .rebuild := by
  have different : oldInput ≠ newInput := by
    intro same
    exact changed (congrArg ArtifactInput.memberPlan same)
  simp [classify, different]

theorem classify_is_total_on_union [DecidableEq Input]
    (oldInput newInput : Option Input)
    (inUnion : oldInput ≠ none ∨ newInput ≠ none) :
    ∃ operation, classify oldInput newInput = some operation := by
  cases oldInput <;> cases newInput <;> simp_all [classify]
  split <;> simp_all

theorem classify_is_single_valued [DecidableEq Input]
    (oldInput newInput : Option Input)
    (left right : ArtifactOperation)
    (leftResult : classify oldInput newInput = some left)
    (rightResult : classify oldInput newInput = some right) :
    left = right := by
  rw [leftResult] at rightResult
  exact Option.some.inj rightResult

abbrev ArtifactMap (Key Input : Type) := Key → Option Input

def operationAt [DecidableEq Input]
    (oldArtifacts newArtifacts : ArtifactMap Key Input)
    (key : Key) : Option ArtifactOperation :=
  classify (oldArtifacts key) (newArtifacts key)

theorem operation_map_domain_is_exact_union [DecidableEq Input]
    (oldArtifacts newArtifacts : ArtifactMap Key Input)
    (key : Key) :
    operationAt oldArtifacts newArtifacts key ≠ none ↔
      oldArtifacts key ≠ none ∨ newArtifacts key ≠ none := by
  cases oldValue : oldArtifacts key with
  | none =>
      cases newValue : newArtifacts key with
      | none => simp [operationAt, classify, oldValue, newValue]
      | some value => simp [operationAt, classify, oldValue, newValue]
  | some oldInput =>
      cases newValue : newArtifacts key with
      | none => simp [operationAt, classify, oldValue, newValue]
      | some newInput =>
          by_cases same : oldInput = newInput <;>
            simp [operationAt, classify, oldValue, newValue, same]

/-- Only create and rebuild require preparing new artifact bytes. -/
def requiresPreparation : ArtifactOperation → Bool
  | .create | .rebuild => true
  | .delete | .unchanged => false

theorem preparation_iff_create_or_rebuild
    (operation : ArtifactOperation) :
    requiresPreparation operation = true ↔
      operation = .create ∨ operation = .rebuild := by
  cases operation <;> simp [requiresPreparation]

end H2HDB.Verification.ArtifactDelta
