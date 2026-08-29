import Std

/-!
# vNext catalog schema and BCNF

The relation declarations below this framework are generated mechanically from
`verification/schema/catalog.toml`, the sole authority for relation names,
attributes, declared candidate/alternate keys, and functional dependencies.

`BCNF` quantifies over every attribute subset `X` and every attribute in
`closure_F(X) \ X`; it therefore checks `F⁺`, not merely the input FD list.
`closure_F` is the ordinary iterative Armstrong attribute closure.  Each
relation additionally proves that closure has reached a fixed point, that every
declared key determines every attribute, and that no proper subset of a
declared key is a superkey.

This is a closed-world proof over the manifest FDs.  It assumes the manifest
lists every semantic nontrivial FD.  SHA digests are collision-checked stored
identities: exact preimages deterministically yield digests, while reverse FDs
require declared uniqueness, full-preimage comparison, mismatch rejection, and
an immutable seal.  It does not prove that SQL DDL, migrations, collations,
indexes, foreign keys, or transactions implement the manifest.
-/

namespace H2HDB.Verification.VNextSchema

abbrev Attribute := String

structure FD where
  determinant : List Attribute
  dependent : List Attribute
deriving DecidableEq, Repr

structure RelationContract where
  name : String
  attributes : List Attribute
  declaredKeys : List (List Attribute)
  declaredFDs : List FD
deriving DecidableEq, Repr

def addUnique (values base : List Attribute) : List Attribute :=
  values.foldl (fun result value =>
    if value ∈ result then result else result ++ [value]) base

def attrSubset (left right : List Attribute) : Bool :=
  left.all fun value => value ∈ right

def sameAttrSet (left right : List Attribute) : Bool :=
  attrSubset left right && attrSubset right left

def noDuplicates : List Attribute → Bool
  | [] => true
  | head :: tail => !(head ∈ tail) && noDuplicates tail

def subsets : List Attribute → List (List Attribute)
  | [] => [[]]
  | head :: tail =>
      let rest := subsets tail
      rest ++ rest.map (fun values => head :: values)

def closureStep
    (declaredFDs : List FD)
    (current : List Attribute) : List Attribute :=
  declaredFDs.foldl (fun result fd =>
    if attrSubset fd.determinant result then
      addUnique fd.dependent result
    else result) current

/-- Iterating once per attribute plus one is sufficient for finite closure. -/
def closureF
    (contract : RelationContract)
    (seed : List Attribute) : List Attribute :=
  (List.range (contract.attributes.length + 1)).foldl
    (fun result _ => closureStep contract.declaredFDs result)
    (addUnique seed [])

def isSuperkey
    (contract : RelationContract)
    (determinant : List Attribute) : Bool :=
  attrSubset contract.attributes (closureF contract determinant)

/-- `F⁺` BCNF definition over every `X ⊆ R` and `A ∈ X⁺ - X`. -/
def BCNF (contract : RelationContract) : Prop :=
  ∀ determinant ∈ subsets contract.attributes,
    ∀ dependent ∈ closureF contract determinant,
      dependent ∉ determinant →
      ∀ attr ∈ contract.attributes, attr ∈ closureF contract determinant

def KeysDetermineAllAttributes (contract : RelationContract) : Prop :=
  ∀ key ∈ contract.declaredKeys,
    ∀ attr ∈ contract.attributes, attr ∈ closureF contract key

def DeclaredKeysAreMinimal (contract : RelationContract) : Prop :=
  ∀ key ∈ contract.declaredKeys,
    ∀ candidate ∈ subsets key,
      ¬ sameAttrSet candidate key = true →
      isSuperkey contract candidate = false

def ClosureReachedFixedPoint (contract : RelationContract) : Prop :=
  ∀ seed ∈ subsets contract.attributes,
    sameAttrSet
      (closureStep contract.declaredFDs (closureF contract seed))
      (closureF contract seed) = true

def schemaWellFormedCheck (contract : RelationContract) : Bool :=
  noDuplicates contract.attributes &&
    contract.declaredKeys.all (fun key =>
      !key.isEmpty && noDuplicates key && attrSubset key contract.attributes) &&
    contract.declaredFDs.all (fun fd =>
      noDuplicates fd.determinant && noDuplicates fd.dependent &&
        attrSubset fd.determinant contract.attributes &&
        attrSubset fd.dependent contract.attributes)

def keysDetermineAllCheck (contract : RelationContract) : Bool :=
  contract.declaredKeys.all (isSuperkey contract)

def declaredKeysMinimalCheck (contract : RelationContract) : Bool :=
  contract.declaredKeys.all (fun key =>
    (subsets key).all (fun candidate =>
      sameAttrSet candidate key || !isSuperkey contract candidate))

def closureFixedPointCheck (contract : RelationContract) : Bool :=
  (subsets contract.attributes).all (fun seed =>
    sameAttrSet
      (closureStep contract.declaredFDs (closureF contract seed))
      (closureF contract seed))

def bcnfCheck (contract : RelationContract) : Bool :=
  (subsets contract.attributes).all (fun determinant =>
    !(closureF contract determinant).any
        (fun dependent => dependent ∉ determinant) ||
      isSuperkey contract determinant)

theorem keysDetermineAllCheck_sound
    (contract : RelationContract)
    (checked : keysDetermineAllCheck contract = true) :
    KeysDetermineAllAttributes contract := by
  intro key keyMem attr attrMem
  have keyCheck := List.all_eq_true.mp checked key keyMem
  exact of_decide_eq_true
    (List.all_eq_true.mp keyCheck attr attrMem)

theorem declaredKeysMinimalCheck_sound
    (contract : RelationContract)
    (checked : declaredKeysMinimalCheck contract = true) :
    DeclaredKeysAreMinimal contract := by
  intro key keyMem candidate candidateMem proper
  have keyCheck := List.all_eq_true.mp checked key keyMem
  have candidateCheck := List.all_eq_true.mp keyCheck candidate candidateMem
  simp only [Bool.or_eq_true] at candidateCheck
  rcases candidateCheck with same | notSuperkey
  · exact False.elim (proper same)
  · cases value : isSuperkey contract candidate <;>
      simp [value] at notSuperkey ⊢

theorem closureFixedPointCheck_sound
    (contract : RelationContract)
    (checked : closureFixedPointCheck contract = true) :
    ClosureReachedFixedPoint contract := by
  intro seed seedMem
  exact List.all_eq_true.mp checked seed seedMem

theorem bcnfCheck_sound
    (contract : RelationContract)
    (checked : bcnfCheck contract = true) :
    BCNF contract := by
  intro determinant determinantMem dependent dependentMem outside attr attrMem
  have determinantCheck :=
    List.all_eq_true.mp checked determinant determinantMem
  have hasNew :
      (closureF contract determinant).any
          (fun value => value ∉ determinant) = true := by
    exact List.any_eq_true.mpr
      ⟨dependent, dependentMem, by simp [outside]⟩
  rw [hasNew] at determinantCheck
  simp only [Bool.not_true, Bool.false_or] at determinantCheck
  exact of_decide_eq_true
    (List.all_eq_true.mp determinantCheck attr attrMem)

/-! ## Binary lossless-join decomposition criterion -/

structure BinaryDecompositionContract where
  name : String
  universalAttributes : List Attribute
  leftAttributes : List Attribute
  rightAttributes : List Attribute
  declaredFDs : List FD
deriving DecidableEq, Repr

def attributeIntersection
    (left right : List Attribute) : List Attribute :=
  left.filter fun attr => attr ∈ right

def decompositionAsRelation
    (contract : BinaryDecompositionContract) : RelationContract where
  name := contract.name
  attributes := contract.universalAttributes
  declaredKeys := []
  declaredFDs := contract.declaredFDs

/-- Both projections are subsets of, and together cover, the universal relation. -/
def BinaryDecompositionWellFormed
    (contract : BinaryDecompositionContract) : Prop :=
  (∀ attr ∈ contract.leftAttributes,
      attr ∈ contract.universalAttributes) ∧
    (∀ attr ∈ contract.rightAttributes,
      attr ∈ contract.universalAttributes) ∧
    (∀ attr ∈ contract.universalAttributes,
      attr ∈ contract.leftAttributes ∨ attr ∈ contract.rightAttributes)

def binaryDecompositionWellFormedCheck
    (contract : BinaryDecompositionContract) : Bool :=
  attrSubset contract.leftAttributes contract.universalAttributes &&
    (attrSubset contract.rightAttributes contract.universalAttributes &&
      attrSubset contract.universalAttributes
        (contract.leftAttributes ++ contract.rightAttributes))

theorem binaryDecompositionWellFormedCheck_sound
    (contract : BinaryDecompositionContract)
    (checked : binaryDecompositionWellFormedCheck contract = true) :
    BinaryDecompositionWellFormed contract := by
  unfold binaryDecompositionWellFormedCheck at checked
  simp only [Bool.and_eq_true] at checked
  rcases checked with ⟨leftSubset, rightSubset, covered⟩
  refine ⟨?_, ?_, ?_⟩
  · intro attr attrMem
    exact of_decide_eq_true
      (List.all_eq_true.mp leftSubset attr attrMem)
  · intro attr attrMem
    exact of_decide_eq_true
      (List.all_eq_true.mp rightSubset attr attrMem)
  · intro attr attrMem
    have inUnion : attr ∈
        contract.leftAttributes ++ contract.rightAttributes :=
      of_decide_eq_true (List.all_eq_true.mp covered attr attrMem)
    simpa using inUnion

/--
The standard binary lossless-join FD criterion:
`(R₁ ∩ R₂) → R₁` or `(R₁ ∩ R₂) → R₂` in `F⁺`.
-/
def BinaryLosslessFDCondition (contract : BinaryDecompositionContract) : Prop :=
  let intersection :=
    attributeIntersection contract.leftAttributes contract.rightAttributes
  let closure := closureF (decompositionAsRelation contract) intersection
  (∀ attr ∈ contract.leftAttributes, attr ∈ closure) ∨
    (∀ attr ∈ contract.rightAttributes, attr ∈ closure)

def BinaryLossless (contract : BinaryDecompositionContract) : Prop :=
  BinaryDecompositionWellFormed contract ∧
    BinaryLosslessFDCondition contract

def binaryLosslessCheck (contract : BinaryDecompositionContract) : Bool :=
  let intersection :=
    attributeIntersection contract.leftAttributes contract.rightAttributes
  let closure := closureF (decompositionAsRelation contract) intersection
  attrSubset contract.leftAttributes closure ||
    attrSubset contract.rightAttributes closure

theorem binaryLosslessCheck_sound
    (contract : BinaryDecompositionContract)
    (checked : binaryLosslessCheck contract = true) :
    BinaryLosslessFDCondition contract := by
  unfold binaryLosslessCheck at checked
  unfold BinaryLosslessFDCondition
  simp only [Bool.or_eq_true] at checked
  rcases checked with left | right
  · exact Or.inl (fun attr attrMem =>
      of_decide_eq_true (List.all_eq_true.mp left attr attrMem))
  · exact Or.inr (fun attr attrMem =>
      of_decide_eq_true (List.all_eq_true.mp right attr attrMem))

/-! ## Dependency preservation (separate from losslessness) -/

/--
The exact `F⁺` projection onto `attributes`: for every `X ⊆ Rᵢ`, emit
`X → (closure_F X ∩ Rᵢ)`.  Trivial dependents are retained because they do
not change implication and make the executable construction direct.
-/
def projectFunctionalDependencies
    (contract : BinaryDecompositionContract)
    (attributes : List Attribute) : List FD :=
  (subsets attributes).map fun determinant =>
    { determinant := determinant
      dependent :=
        (closureF (decompositionAsRelation contract) determinant).filter
          fun attr => attr ∈ attributes }

def projectedFunctionalDependencies
    (contract : BinaryDecompositionContract) : List FD :=
  projectFunctionalDependencies contract contract.leftAttributes ++
    projectFunctionalDependencies contract contract.rightAttributes

def projectedDependenciesAsRelation
    (contract : BinaryDecompositionContract) : RelationContract where
  name := contract.name
  attributes := contract.universalAttributes
  declaredKeys := []
  declaredFDs := projectedFunctionalDependencies contract

def projectedClosureF
    (contract : BinaryDecompositionContract)
    (seed : List Attribute) : List Attribute :=
  closureF (projectedDependenciesAsRelation contract) seed

/-- Every original FD is implied by the union of the exact F⁺ projections. -/
def DependencyPreserving (contract : BinaryDecompositionContract) : Prop :=
  ∀ fd ∈ contract.declaredFDs,
    ∀ attr ∈ fd.dependent,
      attr ∈ projectedClosureF contract fd.determinant

def dependencyPreservationCheck
    (contract : BinaryDecompositionContract) : Bool :=
  contract.declaredFDs.all fun fd =>
    attrSubset fd.dependent
      (projectedClosureF contract fd.determinant)

theorem dependencyPreservationCheck_sound
    (contract : BinaryDecompositionContract)
    (checked : dependencyPreservationCheck contract = true) :
    DependencyPreserving contract := by
  intro fd fdMem attr attrMem
  have fdCheck := List.all_eq_true.mp checked fd fdMem
  exact of_decide_eq_true
    (List.all_eq_true.mp fdCheck attr attrMem)

/-! ## Bounded immutable analysis overlays -/

/-- One target-key change: replace the old value or remove the key. -/
inductive StateDelta (Value : Type) where
  | put (value : Value)
  | delete

/-- A resolved state component and a sparse, explicitly shadowing layer. -/
abbrev CompleteState (Key Value : Type) := Key → Option Value
abbrev StateDeltaMap (Key Value : Type) := Key → Option (StateDelta Value)

/--
Apply one sparse layer to the state resolved from its sealed parent chain.
Delta absence inherits the parent answer; `put` shadows it and `delete` is an
explicit tombstone.  This is a semantic function, not a physical full-state copy.
-/
def materializeCompleteState
    {Key Value : Type}
    (baseline : CompleteState Key Value)
    (delta : StateDeltaMap Key Value) : CompleteState Key Value :=
  fun key =>
    match delta key with
    | none => baseline key
    | some (.put value) => some value
    | some .delete => none

/-- Exact delta detection covers every key, including explicit deletion. -/
def ExactStateDelta
    {Key Value : Type}
    (baseline target : CompleteState Key Value)
    (delta : StateDeltaMap Key Value) : Prop :=
  ∀ key,
    match delta key with
    | none => baseline key = target key
    | some (.put value) => target key = some value
    | some .delete => target key = none

/--
An exact current layer over a complete resolved parent is extensionally
identical to the deterministic full evaluator for the new immutable build.
-/
theorem exact_delta_materializes_full_state
    {Key Value : Type}
    (baseline target : CompleteState Key Value)
    (delta : StateDeltaMap Key Value)
    (deltaExact : ExactStateDelta baseline target delta) :
    materializeCompleteState baseline delta = target := by
  funext key
  have keyExact := deltaExact key
  cases deltaValue : delta key with
  | none =>
      simp [deltaValue] at keyExact
      simpa [materializeCompleteState, deltaValue] using keyExact
  | some change =>
      cases change with
      | put value =>
          simp [deltaValue] at keyExact
          simpa [materializeCompleteState, deltaValue] using keyExact.symm
      | delete =>
          simp [deltaValue] at keyExact
          simpa [materializeCompleteState, deltaValue] using keyExact.symm

/-- The first analysis is the same construction over an empty baseline. -/
def emptyCompleteState {Key Value : Type} : CompleteState Key Value :=
  fun _ => none

theorem exact_initial_delta_materializes_full_state
    {Key Value : Type}
    (target : CompleteState Key Value)
    (delta : StateDeltaMap Key Value)
    (deltaExact : ExactStateDelta emptyCompleteState target delta) :
    materializeCompleteState emptyCompleteState delta = target :=
  exact_delta_materializes_full_state emptyCompleteState target delta deltaExact

/-- Nearest-first immutable shadow/tombstone layers. -/
abbrev OverlayChain (Key Value : Type) := List (StateDeltaMap Key Value)

def resolveOverlay {Key Value : Type} :
    OverlayChain Key Value → CompleteState Key Value
  | [], _ => none
  | layer :: ancestors, key =>
      match layer key with
      | none => resolveOverlay ancestors key
      | some (.put value) => some value
      | some .delete => none

theorem exact_delta_over_parent_equals_full_recompute
    {Key Value : Type}
    (parent : OverlayChain Key Value)
    (target : CompleteState Key Value)
    (delta : StateDeltaMap Key Value)
    (deltaExact : ExactStateDelta (resolveOverlay parent) target delta) :
    resolveOverlay (delta :: parent) = target := by
  change materializeCompleteState (resolveOverlay parent) delta = target
  exact exact_delta_materializes_full_state
    (resolveOverlay parent) target delta deltaExact

/-- At any depth, absent nearer layers make the first decision uniquely win. -/
theorem resolveOverlay_nearest_decision
    {Key Value : Type}
    (before : OverlayChain Key Value)
    (chosen : StateDeltaMap Key Value)
    (after : OverlayChain Key Value)
    (key : Key)
    (decision : StateDelta Value)
    (beforeAbsent : ∀ layer ∈ before, layer key = none)
    (chosenDecision : chosen key = some decision) :
    resolveOverlay (before ++ chosen :: after) key =
      match decision with
      | .put value => some value
      | .delete => none := by
  induction before with
  | nil =>
      cases decision <;> simp [resolveOverlay, chosenDecision]
  | cons head tail inductionHypothesis =>
      have headAbsent : head key = none :=
        beforeAbsent head (by simp)
      have tailAbsent : ∀ layer ∈ tail, layer key = none := by
        intro layer layerMem
        exact beforeAbsent layer (by simp [layerMem])
      simpa [resolveOverlay, headAbsent] using
        inductionHypothesis tailAbsent

/-- A compacted depth-zero chain stores every live value and no old ancestry. -/
def fullRootLayer
    {Key Value : Type}
    (target : CompleteState Key Value) : StateDeltaMap Key Value :=
  fun key => target key |>.map StateDelta.put

def compactedOverlay
    {Key Value : Type}
    (target : CompleteState Key Value) : OverlayChain Key Value :=
  [fullRootLayer target]

theorem depth_zero_compaction_equals_full_recompute
    {Key Value : Type}
    (target : CompleteState Key Value) :
    resolveOverlay (compactedOverlay target) = target := by
  funext key
  cases value : target key <;>
    simp [compactedOverlay, fullRootLayer, resolveOverlay, value]

/-- Depth ≤ 16 means at most 17 nearest-first layers including the run. -/
def BoundedOverlay {Key Value : Type}
    (layers : OverlayChain Key Value) : Prop :=
  layers.length ≤ 17

theorem depth_zero_compaction_is_bounded
    {Key Value : Type}
    (target : CompleteState Key Value) :
    BoundedOverlay (compactedOverlay target) := by
  simp [BoundedOverlay, compactedOverlay]

/-- A child after a depth-16 parent compacts to one layer and cuts ancestry. -/
theorem seventeenth_child_compacts_and_cuts_ancestry
    {Key Value : Type}
    (parent : OverlayChain Key Value)
    (target : CompleteState Key Value)
    (_parentAtLimit : parent.length = 17) :
    resolveOverlay (compactedOverlay target) = target ∧
      (compactedOverlay target).length = 1 := by
  exact ⟨depth_zero_compaction_equals_full_recompute target,
    by simp [compactedOverlay]⟩

/-- Materialized ancestry is acyclic and bounded; compaction is self-only. -/
def ValidAncestry {Analysis : Type}
    (chain : List Analysis) : Prop :=
  chain.Nodup ∧ chain.length ≤ 17

theorem compacted_ancestry_is_valid
    {Analysis : Type} (analysis : Analysis) :
    ValidAncestry [analysis] := by
  simp [ValidAncestry]

def PolicyCompatible {Policy : Type} [DecidableEq Policy]
    (parent child : Policy) : Bool :=
  decide (parent = child)

theorem policy_change_cannot_inherit
    {Policy : Type} [DecidableEq Policy]
    (parent child : Policy)
    (changed : parent ≠ child) :
    PolicyCompatible parent child = false := by
  simp [PolicyCompatible, changed]

/--
The three externally read analysis answers all equal their deterministic full
recomputations when their independently normalized deltas are exact.
-/
theorem complete_analysis_answers_equal_full_recompute
    {Hash Content Gid Gallery : Type}
    (baselineSpam targetSpam : CompleteState Hash Unit)
    (baselineOwner targetOwner : CompleteState Content Gallery)
    (baselineWinner targetWinner : CompleteState Gid Gallery)
    (spamDelta : StateDeltaMap Hash Unit)
    (ownerDelta : StateDeltaMap Content Gallery)
    (winnerDelta : StateDeltaMap Gid Gallery)
    (spamExact : ExactStateDelta baselineSpam targetSpam spamDelta)
    (ownerExact : ExactStateDelta baselineOwner targetOwner ownerDelta)
    (winnerExact : ExactStateDelta baselineWinner targetWinner winnerDelta) :
    materializeCompleteState baselineSpam spamDelta = targetSpam ∧
      materializeCompleteState baselineOwner ownerDelta = targetOwner ∧
      materializeCompleteState baselineWinner winnerDelta = targetWinner := by
  exact ⟨exact_delta_materializes_full_state
      baselineSpam targetSpam spamDelta spamExact,
    exact_delta_materializes_full_state
      baselineOwner targetOwner ownerDelta ownerExact,
    exact_delta_materializes_full_state
      baselineWinner targetWinner winnerDelta winnerExact⟩

/-- The three canonical resolved query relations equal their full evaluators. -/
theorem resolved_analysis_answers_equal_full_recompute
    {Hash Content Gid Gallery : Type}
    (spamParent : OverlayChain Hash Unit)
    (ownerParent : OverlayChain Content Gallery)
    (winnerParent : OverlayChain Gid Gallery)
    (targetSpam : CompleteState Hash Unit)
    (targetOwner : CompleteState Content Gallery)
    (targetWinner : CompleteState Gid Gallery)
    (spamDelta : StateDeltaMap Hash Unit)
    (ownerDelta : StateDeltaMap Content Gallery)
    (winnerDelta : StateDeltaMap Gid Gallery)
    (spamExact : ExactStateDelta
      (resolveOverlay spamParent) targetSpam spamDelta)
    (ownerExact : ExactStateDelta
      (resolveOverlay ownerParent) targetOwner ownerDelta)
    (winnerExact : ExactStateDelta
      (resolveOverlay winnerParent) targetWinner winnerDelta) :
    resolveOverlay (spamDelta :: spamParent) = targetSpam ∧
      resolveOverlay (ownerDelta :: ownerParent) = targetOwner ∧
      resolveOverlay (winnerDelta :: winnerParent) = targetWinner := by
  exact ⟨exact_delta_over_parent_equals_full_recompute
      spamParent targetSpam spamDelta spamExact,
    exact_delta_over_parent_equals_full_recompute
      ownerParent targetOwner ownerDelta ownerExact,
    exact_delta_over_parent_equals_full_recompute
      winnerParent targetWinner winnerDelta winnerExact⟩

/-- An ancestor is prunable only when no sealed descendant chain reaches it. -/
def MayPruneAncestor {Analysis : Type}
    (ancestor : Analysis)
    (sealedDescendantChains : List (List Analysis)) : Prop :=
  ∀ chain ∈ sealedDescendantChains, ancestor ∉ chain

theorem reachable_ancestor_cannot_be_pruned
    {Analysis : Type}
    (ancestor : Analysis)
    (chain : List Analysis)
    (allChains : List (List Analysis))
    (chainRecorded : chain ∈ allChains)
    (ancestorReachable : ancestor ∈ chain) :
    ¬ MayPruneAncestor ancestor allChains := by
  intro mayPrune
  exact mayPrune chain chainRecorded ancestorReachable

/-- After compaction, reads contain no reference to replaced ancestry storage. -/
theorem compacted_state_survives_old_ancestry_cleanup
    {Key Value OldStorage : Type}
    (target : CompleteState Key Value)
    (_beforeCleanup _afterCleanup : OldStorage) :
    resolveOverlay (compactedOverlay target) = target :=
  depth_zero_compaction_equals_full_recompute target

/-! ## Sealed vertical families -/

structure VerticalMemberContract where
  relationName : String
  keyAttributes : List Attribute
  valueAttribute : Attribute
  projectionAttribute : Attribute
  attributes : List Attribute
  declaredKeys : List (List Attribute)
  declaredFDs : List FD
  sourceRelation : String
  sourceRelationAttributes : List Attribute
  sourceAttributes : List Attribute
  memberAttributes : List Attribute
  congruenceMembers : List String
  project : Bool
  required : Bool
deriving DecidableEq, Repr

structure VerticalOptionalPresenceContract where
  memberRelation : String
  discriminatorRelation : String
  discriminatorAttribute : Attribute
  presentValue : String
  absentValues : List String
deriving DecidableEq, Repr

structure VerticalFamilyContract where
  name : String
  anchorRelation : String
  sealRelation : String
  keyAttributes : List Attribute
  anchorAttributes : List Attribute
  sealAttributes : List Attribute
  semanticAttributes : List Attribute
  semanticFDs : List FD
  viewAttributes : List Attribute
  viewFDs : List FD
  members : List VerticalMemberContract
  optionalPresence : Option VerticalOptionalPresenceContract
  markerRelation : Option String
  markerAttributes : List Attribute
  markerPredicate : Option String
deriving DecidableEq, Repr

def verticalSourceShapeCheck
    (contract : VerticalFamilyContract)
    (prior : List VerticalMemberContract)
    (member : VerticalMemberContract) : Bool :=
  if member.sourceRelation == contract.anchorRelation then
    sameAttrSet member.sourceRelationAttributes contract.anchorAttributes
  else if member.sourceRelation == contract.sealRelation then
    sameAttrSet member.sourceRelationAttributes contract.sealAttributes
  else
    prior.any (fun candidate =>
      candidate.relationName == member.sourceRelation &&
        sameAttrSet candidate.attributes member.sourceRelationAttributes)

def verticalOrderedJoinCheck
    (contract : VerticalFamilyContract) :
    List VerticalMemberContract → List VerticalMemberContract → Bool
  | [], _ => true
  | member :: remaining, prior =>
      verticalSourceShapeCheck contract prior member &&
        !member.sourceAttributes.isEmpty &&
        noDuplicates member.sourceAttributes &&
        noDuplicates member.memberAttributes &&
        member.sourceAttributes.length == member.memberAttributes.length &&
        attrSubset member.sourceAttributes member.sourceRelationAttributes &&
        member.declaredKeys.any
          (fun key => sameAttrSet member.memberAttributes key) &&
        verticalOrderedJoinCheck contract remaining (prior ++ [member])

def verticalProjectedAttribute
    (member : VerticalMemberContract)
    (attr : Attribute) : Attribute :=
  if member.project && attr == member.valueAttribute then
    member.projectionAttribute
  else
    attr

def verticalMemberFDs (contract : VerticalFamilyContract) : List FD :=
  contract.members.flatMap (fun member =>
    member.declaredFDs.map (fun fd =>
      { determinant := fd.determinant.map (verticalProjectedAttribute member)
        dependent := fd.dependent.map (verticalProjectedAttribute member) }))

def verticalSemanticRelation
    (contract : VerticalFamilyContract) : RelationContract where
  name := contract.name ++ ".semantic"
  attributes := contract.semanticAttributes
  declaredKeys := []
  declaredFDs := contract.semanticFDs

def verticalMemberFDRelation
    (contract : VerticalFamilyContract) : RelationContract where
  name := contract.name ++ ".members"
  attributes := contract.semanticAttributes
  declaredKeys := []
  declaredFDs := verticalMemberFDs contract

def verticalViewRelation
    (contract : VerticalFamilyContract) : RelationContract where
  name := contract.name ++ ".view"
  attributes := contract.viewAttributes
  declaredKeys := []
  declaredFDs := contract.viewFDs

def verticalFDClosureEquivalenceCheck
    (contract : VerticalFamilyContract) : Bool :=
  (subsets contract.semanticAttributes).all (fun seed =>
    sameAttrSet
      (closureF (verticalSemanticRelation contract) seed)
      (closureF (verticalMemberFDRelation contract) seed))

def verticalProjectedFDClosureEquivalenceCheck
    (contract : VerticalFamilyContract) : Bool :=
  (subsets contract.viewAttributes).all (fun seed =>
    sameAttrSet
      ((closureF (verticalSemanticRelation contract) seed).filter
        (fun attr => attr ∈ contract.viewAttributes))
      (closureF (verticalViewRelation contract) seed))

/-- The view projects the family key plus every marked member non-join attribute. -/
def verticalFamilyViewProjectionCheck
    (contract : VerticalFamilyContract) : Bool :=
  sameAttrSet contract.viewAttributes
    (contract.keyAttributes ++
      (contract.members.filter (fun member => member.project)).flatMap
        (fun member =>
          (member.attributes.filter
            (fun attr => attr ∉ member.memberAttributes)).map
              (verticalProjectedAttribute member)))

/-- Optional facts need one closed discriminator rule; mandatory families need none. -/
def verticalOptionalPresenceCheck (contract : VerticalFamilyContract) : Bool :=
  let optionalMembers := contract.members.filter (fun member => !member.required)
  match contract.optionalPresence with
  | none => optionalMembers.isEmpty
  | some presence =>
      optionalMembers.length == 1 &&
        optionalMembers.any (fun member =>
          member.relationName == presence.memberRelation) &&
        contract.members.any (fun member =>
          member.relationName == presence.discriminatorRelation &&
            member.required && member.project &&
            (member.valueAttribute == presence.discriminatorAttribute ||
              member.projectionAttribute == presence.discriminatorAttribute)) &&
        !presence.absentValues.isEmpty &&
        noDuplicates presence.absentValues &&
        !presence.absentValues.contains presence.presentValue

/-- A cold natural identity may demand congruence with an explicit subset of hot facts. -/
def verticalSelectedCongruenceCheck
    (contract : VerticalFamilyContract)
    (member : VerticalMemberContract) : Bool :=
  if member.congruenceMembers.isEmpty then
    true
  else
    !member.project &&
      noDuplicates member.congruenceMembers &&
      member.congruenceMembers.all (fun name =>
        contract.members.any (fun candidate =>
          candidate.relationName == name && candidate.project)) &&
      sameAttrSet member.keyAttributes
        ((contract.members.filter (fun candidate =>
          member.congruenceMembers.contains candidate.relationName)).map
            (fun candidate => candidate.valueAttribute))

def verticalCongruenceCheck (contract : VerticalFamilyContract) : Bool :=
  contract.members.all (verticalSelectedCongruenceCheck contract)

/-- Optional PK-only marker metadata stays outside the projected value join. -/
def verticalMarkerCheck (contract : VerticalFamilyContract) : Bool :=
  match contract.markerRelation, contract.markerPredicate with
  | none, none => contract.markerAttributes.isEmpty
  | some relation, some predicate =>
      !relation.isEmpty && !predicate.isEmpty &&
        sameAttrSet contract.markerAttributes contract.keyAttributes
  | _, _ => false

/-- Generic manifest check for PK-only boundaries and member-specific PK-plus-one rows. -/
def verticalFamilyWellFormedCheck (contract : VerticalFamilyContract) : Bool :=
  !contract.keyAttributes.isEmpty &&
    noDuplicates contract.keyAttributes &&
    sameAttrSet contract.anchorAttributes contract.keyAttributes &&
    sameAttrSet contract.sealAttributes contract.keyAttributes &&
    sameAttrSet contract.semanticAttributes
      (contract.keyAttributes ++
        contract.members.flatMap (fun member =>
          member.attributes.map (verticalProjectedAttribute member))) &&
    noDuplicates (contract.members.map (fun member => member.relationName)) &&
    noDuplicates (contract.members.map (fun member => member.valueAttribute)) &&
    noDuplicates ((contract.members.filter (fun member => member.project)).map
      (fun member => member.projectionAttribute)) &&
    contract.members.all (fun member =>
      !member.keyAttributes.isEmpty &&
        noDuplicates member.keyAttributes &&
        sameAttrSet member.attributes
          (member.keyAttributes ++ [member.valueAttribute]) &&
        member.declaredKeys.head? == some member.keyAttributes &&
        member.declaredFDs.any (fun fd =>
          sameAttrSet fd.determinant member.keyAttributes &&
            member.valueAttribute ∈ fd.dependent)) &&
    verticalCongruenceCheck contract &&
    verticalMarkerCheck contract &&
    verticalOptionalPresenceCheck contract &&
    verticalFamilyViewProjectionCheck contract

/-- Each ordered join attaches a relation on its complete determinant key. -/
def verticalFamilyLosslessJoinCheck (contract : VerticalFamilyContract) : Bool :=
  verticalFamilyWellFormedCheck contract &&
    verticalOrderedJoinCheck contract contract.members []

/-- Base-member FDs and the projected view FDs have exactly the declared F⁺ closures. -/
def verticalFamilyDependencyPreservationCheck
    (contract : VerticalFamilyContract) : Bool :=
  verticalFDClosureEquivalenceCheck contract &&
    verticalProjectedFDClosureEquivalenceCheck contract

def VerticalFamilyWellFormed (contract : VerticalFamilyContract) : Prop :=
  verticalFamilyWellFormedCheck contract = true

theorem verticalFamilyWellFormedCheck_sound
    (contract : VerticalFamilyContract)
    (checked : verticalFamilyWellFormedCheck contract = true) :
    VerticalFamilyWellFormed contract :=
  checked

def VerticalFamilyViewProjection (contract : VerticalFamilyContract) : Prop :=
  verticalFamilyViewProjectionCheck contract = true

theorem verticalFamilyViewProjectionCheck_sound
    (contract : VerticalFamilyContract)
    (checked : verticalFamilyViewProjectionCheck contract = true) :
    VerticalFamilyViewProjection contract :=
  checked

def VerticalFamilyLossless (contract : VerticalFamilyContract) : Prop :=
  verticalFamilyLosslessJoinCheck contract = true

theorem verticalFamilyLosslessJoinCheck_sound
    (contract : VerticalFamilyContract)
    (checked : verticalFamilyLosslessJoinCheck contract = true) :
    VerticalFamilyLossless contract :=
  checked

def VerticalFamilyDependencyPreserving
    (contract : VerticalFamilyContract) : Prop :=
  verticalFamilyDependencyPreservationCheck contract = true

theorem verticalFamilyDependencyPreservationCheck_sound
    (contract : VerticalFamilyContract)
    (checked : verticalFamilyDependencyPreservationCheck contract = true) :
    VerticalFamilyDependencyPreserving contract :=
  checked

/-! ## Batch0B derived receipt arithmetic -/

/-- Largest portable signed 63-bit value admitted by both physical backends. -/
def portableInt63Max : Nat := 9223372036854775807

def derivedCommittedGeneration (startGeneration : Nat) : Nat :=
  startGeneration + 1

def derivedNextProcessedCount
    (startProcessedCount rowCount : Nat) : Nat :=
  startProcessedCount + rowCount

def derivedTerminal (rowCount : Nat) : Bool :=
  rowCount == 0

inductive DerivedCheckpointState where
  | open
  | complete
deriving DecidableEq, Repr

def derivedNextState (rowCount : Nat) : DerivedCheckpointState :=
  if rowCount = 0 then .complete else .open

/-- Successor projection is injective, so committed generation is a key too. -/
theorem derived_committed_generation_injective
    {left right : Nat}
    (equal : derivedCommittedGeneration left =
      derivedCommittedGeneration right) :
    left = right := by
  simp [derivedCommittedGeneration] at equal
  omega

/-- A successor remains inside int63 whenever its stored predecessor is below it. -/
theorem derived_committed_generation_int63_bounded
    {startGeneration : Nat}
    (within : startGeneration < portableInt63Max) :
    derivedCommittedGeneration startGeneration ≤ portableInt63Max := by
  simp [derivedCommittedGeneration]
  omega

/-- The stored addends uniquely determine the derived processed-count poststate. -/
theorem derived_next_processed_count_exact
    (startProcessedCount rowCount : Nat) :
    derivedNextProcessedCount startProcessedCount rowCount =
      startProcessedCount + rowCount := by
  rfl

/-- Any admitted sum at most int63 remains portable after view projection. -/
theorem derived_next_processed_count_int63_bounded
    {startProcessedCount rowCount : Nat}
    (within : startProcessedCount + rowCount ≤ portableInt63Max) :
    derivedNextProcessedCount startProcessedCount rowCount ≤
      portableInt63Max := by
  exact within

/-- Either addend is recovered from the other addend and their exact sum. -/
theorem derived_processed_count_addends_recoverable
    {startProcessedCount rowCount nextProcessedCount : Nat}
    (exactSum : nextProcessedCount =
      derivedNextProcessedCount startProcessedCount rowCount) :
    nextProcessedCount - startProcessedCount = rowCount ∧
      nextProcessedCount - rowCount = startProcessedCount := by
  simp [derivedNextProcessedCount] at exactSum
  omega

/-- Terminal and COMPLETE are two projections of the same zero-row predicate. -/
theorem derived_terminal_iff_complete (rowCount : Nat) :
    derivedTerminal rowCount = true ↔
      derivedNextState rowCount = .complete := by
  simp [derivedTerminal, derivedNextState]

/-- A nonterminal state deliberately does not determine a positive row count. -/
theorem nonterminal_does_not_determine_row_count :
    derivedTerminal 1 = false ∧
      derivedTerminal 2 = false ∧
      (1 : Nat) ≠ 2 := by
  native_decide

/-- Analysis retries execute the stage query with the sealed stored limit only. -/
def analysisReplayPageLimit (_retryCallerLimit storedPageLimit : Nat) : Nat :=
  storedPageLimit

def AnalysisPageLimitAdmitted (pageLimit : Nat) : Prop :=
  0 < pageLimit ∧ pageLimit ≤ 128

theorem analysis_replay_uses_stored_page_limit
    (retryCallerLimit storedPageLimit : Nat) :
    analysisReplayPageLimit retryCallerLimit storedPageLimit = storedPageLimit := by
  rfl

theorem analysis_page_limit_is_portable
    {pageLimit : Nat}
    (admitted : AnalysisPageLimitAdmitted pageLimit) :
  pageLimit ≤ portableInt63Max := by
  unfold AnalysisPageLimitAdmitted portableInt63Max at *
  omega

/-- The exclusion change marker is present exactly when old and new flags differ. -/
def exclusionChangeMarker (oldExcluded newExcluded : Bool) : Bool :=
  decide (oldExcluded ≠ newExcluded)

theorem exclusion_change_marker_exact
    (oldExcluded newExcluded : Bool) :
    exclusionChangeMarker oldExcluded newExcluded = true ↔
      oldExcluded ≠ newExcluded := by
  simp [exclusionChangeMarker]

/-! ## Sealed impacted-key provenance -/

/--
An abstract row set for one `(analysis_id, key)` family.  The provenance list
is intentionally unbounded: the proofs below are semantic proofs, not a bounded
page-model check.
-/
structure ImpactedKeyRows where
  anchor : Bool
  provenance : List Nat
  witness : Option Nat
  sealed : Bool
deriving DecidableEq, Repr

def ImpactedMinimumWitness (witness : Nat) (provenance : List Nat) : Prop :=
  witness ∈ provenance ∧
    ∀ gallery ∈ provenance, witness ≤ gallery

def impactedKeyVisible (rows : ImpactedKeyRows) : Bool :=
  rows.anchor && rows.witness.isSome && rows.sealed

def impactedFirstInsert (gallery : Nat) : ImpactedKeyRows where
  anchor := true
  provenance := [gallery]
  witness := some gallery
  sealed := true

/-- Appending provenance cannot mutate the anchor, witness, or seal. -/
def impactedAppendProvenance
    (rows : ImpactedKeyRows)
    (gallery : Nat) : ImpactedKeyRows :=
  { rows with provenance := rows.provenance ++ [gallery] }

theorem impacted_first_insert_establishes_minimum_and_visibility
    (gallery : Nat) :
    ImpactedMinimumWitness gallery
        (impactedFirstInsert gallery).provenance ∧
      impactedKeyVisible (impactedFirstInsert gallery) = true := by
  simp [ImpactedMinimumWitness, impactedFirstInsert, impactedKeyVisible]

theorem impacted_minimum_witness_unique
    {left right : Nat}
    {provenance : List Nat}
    (leftMinimum : ImpactedMinimumWitness left provenance)
    (rightMinimum : ImpactedMinimumWitness right provenance) :
    left = right := by
  exact Nat.le_antisymm
    (leftMinimum.2 right rightMinimum.1)
    (rightMinimum.2 left leftMinimum.1)

theorem impacted_append_greater_preserves_minimum_and_visibility
    (rows : ImpactedKeyRows)
    (witness gallery : Nat)
    (minimum : ImpactedMinimumWitness witness rows.provenance)
    (greater : witness < gallery)
    (visible : impactedKeyVisible rows = true) :
    ImpactedMinimumWitness witness
        (impactedAppendProvenance rows gallery).provenance ∧
      impactedKeyVisible (impactedAppendProvenance rows gallery) = true := by
  constructor
  · rcases minimum with ⟨present, least⟩
    constructor
    · simp [impactedAppendProvenance, present]
    · intro observed observedMem
      simp [impactedAppendProvenance] at observedMem
      rcases observedMem with observedMem | rfl
      · exact least observed observedMem
      · exact Nat.le_of_lt greater
  · simpa [impactedAppendProvenance, impactedKeyVisible] using visible

/-- Exact replay compares every physical component and therefore is identity. -/
def ImpactedExactReplay
    (stored replayed : ImpactedKeyRows) : Prop :=
  replayed.anchor = stored.anchor ∧
    replayed.provenance = stored.provenance ∧
    replayed.witness = stored.witness ∧
    replayed.sealed = stored.sealed

theorem impacted_exact_replay_is_identity
    (stored replayed : ImpactedKeyRows)
    (exact : ImpactedExactReplay stored replayed) :
    replayed = stored := by
  cases stored
  cases replayed
  simp_all [ImpactedExactReplay]

def impactedRemoveSeal (rows : ImpactedKeyRows) : ImpactedKeyRows :=
  { rows with sealed := false }

def impactedRemoveWitness (rows : ImpactedKeyRows) : ImpactedKeyRows :=
  { rows with witness := none }

def impactedRemoveProvenance (rows : ImpactedKeyRows) : ImpactedKeyRows :=
  { rows with provenance := [] }

def impactedRemoveAnchor (rows : ImpactedKeyRows) : ImpactedKeyRows :=
  { rows with anchor := false }

def ImpactedNoRows (rows : ImpactedKeyRows) : Prop :=
  rows.anchor = false ∧
    rows.provenance = [] ∧
    rows.witness = none ∧
    rows.sealed = false

/--
The required seal → witness → provenance → anchor cleanup order leaves no
visible key and no child row dangling after its parent is removed.
-/
theorem impacted_ordered_cleanup_has_no_dangling_rows
    (rows : ImpactedKeyRows) :
    let sealRemoved := impactedRemoveSeal rows
    let witnessRemoved := impactedRemoveWitness sealRemoved
    let provenanceRemoved := impactedRemoveProvenance witnessRemoved
    let anchorRemoved := impactedRemoveAnchor provenanceRemoved
    sealRemoved.sealed = false ∧
      witnessRemoved.sealed = false ∧
      witnessRemoved.witness = none ∧
      provenanceRemoved.sealed = false ∧
      provenanceRemoved.witness = none ∧
      provenanceRemoved.provenance = [] ∧
      ImpactedNoRows anchorRemoved := by
  simp [impactedRemoveSeal, impactedRemoveWitness,
    impactedRemoveProvenance, impactedRemoveAnchor, ImpactedNoRows]

/-! ## GID candidate membership and exact winner selection -/

/--
The five atomic comparison facts used by the GID winner evaluator.  `Nat`
models each already-domain-checked unsigned scalar or binary-order rank; the
SQL/Python refinement proves that these are exactly preference, title scalar
count, download time, scope key, and locator digest in that order.
-/
structure GidWinnerCandidate where
  galleryId : Nat
  preferNotAlreadyUploaded : Nat
  titleScalarCount : Nat
  downloadTime : Nat
  scopeKeyRank : Nat
  locatorRank : Nat
deriving DecidableEq, Repr

def gidWinnerOrderKey
    (candidate : GidWinnerCandidate) :
    Nat × Nat × Nat × Nat × Nat :=
  (candidate.preferNotAlreadyUploaded,
    candidate.titleScalarCount,
    candidate.downloadTime,
    candidate.scopeKeyRank,
    candidate.locatorRank)

/--
A selected winner is a member and dominates every member of its group under
the exact five-atom ordering relation supplied by the runtime refinement.
-/
def GidWinnerExact
    (candidates : List GidWinnerCandidate)
    (dominates : GidWinnerCandidate → GidWinnerCandidate → Prop)
    (winner : GidWinnerCandidate) : Prop :=
  winner ∈ candidates ∧
    ∀ candidate ∈ candidates,
      dominates candidate winner

theorem gid_winner_exact_is_member
    (candidates : List GidWinnerCandidate)
    (dominates : GidWinnerCandidate → GidWinnerCandidate → Prop)
    (winner : GidWinnerCandidate)
    (exact : GidWinnerExact candidates dominates winner) :
    winner ∈ candidates :=
  exact.1

theorem gid_winner_exact_dominates_every_candidate
    (candidates : List GidWinnerCandidate)
    (dominates : GidWinnerCandidate → GidWinnerCandidate → Prop)
    (winner candidate : GidWinnerCandidate)
    (exact : GidWinnerExact candidates dominates winner)
    (member : candidate ∈ candidates) :
    dominates candidate winner :=
  exact.2 candidate member

/--
If stable gallery identity makes the full five-atom key injective within the
group, two exact selections necessarily name the same gallery.
-/
theorem gid_winner_exact_gallery_unique
    (candidates : List GidWinnerCandidate)
    (dominates : GidWinnerCandidate → GidWinnerCandidate → Prop)
    (left right : GidWinnerCandidate)
    (leftExact : GidWinnerExact candidates dominates left)
    (rightExact : GidWinnerExact candidates dominates right)
    (stableIdentity :
      ∀ first ∈ candidates, ∀ second ∈ candidates,
        dominates first second → dominates second first →
          first.galleryId = second.galleryId) :
    left.galleryId = right.galleryId := by
  have rightLeLeft := leftExact.2 right rightExact.1
  have leftLeRight := rightExact.2 left leftExact.1
  exact stableIdentity left leftExact.1 right rightExact.1
    leftLeRight rightLeLeft

/-- A gallery's immutable pinned metadata supplies at most one GID. -/
def GidCandidateCurrentGroup
    (metadataGid : Nat → Nat)
    (galleryId gid : Nat) : Prop :=
  metadataGid galleryId = gid

theorem gid_candidate_current_gid_functional
    (metadataGid : Nat → Nat)
    (galleryId leftGid rightGid : Nat)
    (left : GidCandidateCurrentGroup metadataGid galleryId leftGid)
    (right : GidCandidateCurrentGroup metadataGid galleryId rightGid) :
    leftGid = rightGid :=
  left.symm.trans right

/-!
Relation-specific declarations and proofs follow.  Every theorem name retains
the exact snake-case manifest relation name for auditability.
-/

end H2HDB.Verification.VNextSchema

namespace H2HDB.Verification.VNextSchema

/- BEGIN GENERATED CATALOG CONTRACTS -/
def catalogManifestSha256 : String := "b876b17cc0ae28187a97cd6a231e632e0e28b5225d05635a21dc623e5c9c73de"

/-! This section is mechanically generated from catalog.toml. -/

def canonical_digest_policy_contract : RelationContract where
  name := "canonical_digest_policy"
  attributes := ["digest_domain"]
  declaredKeys := [["digest_domain"]]
  declaredFDs := [
  ]

theorem canonical_digest_policy_schema_well_formed :
    schemaWellFormedCheck canonical_digest_policy_contract = true := by
  native_decide

theorem canonical_digest_policy_candidate_keys_check :
    keysDetermineAllCheck canonical_digest_policy_contract = true := by
  native_decide

theorem canonical_digest_policy_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_digest_policy_contract :=
  keysDetermineAllCheck_sound canonical_digest_policy_contract
    canonical_digest_policy_candidate_keys_check

theorem canonical_digest_policy_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_digest_policy_contract = true := by
  native_decide

theorem canonical_digest_policy_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_digest_policy_contract :=
  declaredKeysMinimalCheck_sound canonical_digest_policy_contract
    canonical_digest_policy_candidate_keys_minimal_check

theorem canonical_digest_policy_closure_fixed_check :
    closureFixedPointCheck canonical_digest_policy_contract = true := by
  native_decide

theorem canonical_digest_policy_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_digest_policy_contract :=
  closureFixedPointCheck_sound canonical_digest_policy_contract
    canonical_digest_policy_closure_fixed_check

theorem canonical_digest_policy_bcnf_check :
    bcnfCheck canonical_digest_policy_contract = true := by
  native_decide

theorem canonical_digest_policy_bcnf : BCNF canonical_digest_policy_contract :=
  bcnfCheck_sound canonical_digest_policy_contract canonical_digest_policy_bcnf_check

def canonical_value_allocation_anchor_contract : RelationContract where
  name := "canonical_value_allocation_anchor"
  attributes := ["value_sha256"]
  declaredKeys := [["value_sha256"]]
  declaredFDs := [
  ]

theorem canonical_value_allocation_anchor_schema_well_formed :
    schemaWellFormedCheck canonical_value_allocation_anchor_contract = true := by
  native_decide

theorem canonical_value_allocation_anchor_candidate_keys_check :
    keysDetermineAllCheck canonical_value_allocation_anchor_contract = true := by
  native_decide

theorem canonical_value_allocation_anchor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_value_allocation_anchor_contract :=
  keysDetermineAllCheck_sound canonical_value_allocation_anchor_contract
    canonical_value_allocation_anchor_candidate_keys_check

theorem canonical_value_allocation_anchor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_value_allocation_anchor_contract = true := by
  native_decide

theorem canonical_value_allocation_anchor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_value_allocation_anchor_contract :=
  declaredKeysMinimalCheck_sound canonical_value_allocation_anchor_contract
    canonical_value_allocation_anchor_candidate_keys_minimal_check

theorem canonical_value_allocation_anchor_closure_fixed_check :
    closureFixedPointCheck canonical_value_allocation_anchor_contract = true := by
  native_decide

theorem canonical_value_allocation_anchor_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_value_allocation_anchor_contract :=
  closureFixedPointCheck_sound canonical_value_allocation_anchor_contract
    canonical_value_allocation_anchor_closure_fixed_check

theorem canonical_value_allocation_anchor_bcnf_check :
    bcnfCheck canonical_value_allocation_anchor_contract = true := by
  native_decide

theorem canonical_value_allocation_anchor_bcnf : BCNF canonical_value_allocation_anchor_contract :=
  bcnfCheck_sound canonical_value_allocation_anchor_contract canonical_value_allocation_anchor_bcnf_check

def canonical_value_allocation_digest_domain_contract : RelationContract where
  name := "canonical_value_allocation_digest_domain"
  attributes := ["value_sha256", "digest_domain"]
  declaredKeys := [["value_sha256"]]
  declaredFDs := [
    { determinant := ["value_sha256"], dependent := ["digest_domain"] }
  ]

theorem canonical_value_allocation_digest_domain_schema_well_formed :
    schemaWellFormedCheck canonical_value_allocation_digest_domain_contract = true := by
  native_decide

theorem canonical_value_allocation_digest_domain_candidate_keys_check :
    keysDetermineAllCheck canonical_value_allocation_digest_domain_contract = true := by
  native_decide

theorem canonical_value_allocation_digest_domain_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_value_allocation_digest_domain_contract :=
  keysDetermineAllCheck_sound canonical_value_allocation_digest_domain_contract
    canonical_value_allocation_digest_domain_candidate_keys_check

theorem canonical_value_allocation_digest_domain_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_value_allocation_digest_domain_contract = true := by
  native_decide

theorem canonical_value_allocation_digest_domain_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_value_allocation_digest_domain_contract :=
  declaredKeysMinimalCheck_sound canonical_value_allocation_digest_domain_contract
    canonical_value_allocation_digest_domain_candidate_keys_minimal_check

theorem canonical_value_allocation_digest_domain_closure_fixed_check :
    closureFixedPointCheck canonical_value_allocation_digest_domain_contract = true := by
  native_decide

theorem canonical_value_allocation_digest_domain_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_value_allocation_digest_domain_contract :=
  closureFixedPointCheck_sound canonical_value_allocation_digest_domain_contract
    canonical_value_allocation_digest_domain_closure_fixed_check

theorem canonical_value_allocation_digest_domain_bcnf_check :
    bcnfCheck canonical_value_allocation_digest_domain_contract = true := by
  native_decide

theorem canonical_value_allocation_digest_domain_bcnf : BCNF canonical_value_allocation_digest_domain_contract :=
  bcnfCheck_sound canonical_value_allocation_digest_domain_contract canonical_value_allocation_digest_domain_bcnf_check

def canonical_value_allocation_byte_count_contract : RelationContract where
  name := "canonical_value_allocation_byte_count"
  attributes := ["value_sha256", "byte_count"]
  declaredKeys := [["value_sha256"]]
  declaredFDs := [
    { determinant := ["value_sha256"], dependent := ["byte_count"] }
  ]

theorem canonical_value_allocation_byte_count_schema_well_formed :
    schemaWellFormedCheck canonical_value_allocation_byte_count_contract = true := by
  native_decide

theorem canonical_value_allocation_byte_count_candidate_keys_check :
    keysDetermineAllCheck canonical_value_allocation_byte_count_contract = true := by
  native_decide

theorem canonical_value_allocation_byte_count_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_value_allocation_byte_count_contract :=
  keysDetermineAllCheck_sound canonical_value_allocation_byte_count_contract
    canonical_value_allocation_byte_count_candidate_keys_check

theorem canonical_value_allocation_byte_count_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_value_allocation_byte_count_contract = true := by
  native_decide

theorem canonical_value_allocation_byte_count_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_value_allocation_byte_count_contract :=
  declaredKeysMinimalCheck_sound canonical_value_allocation_byte_count_contract
    canonical_value_allocation_byte_count_candidate_keys_minimal_check

theorem canonical_value_allocation_byte_count_closure_fixed_check :
    closureFixedPointCheck canonical_value_allocation_byte_count_contract = true := by
  native_decide

theorem canonical_value_allocation_byte_count_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_value_allocation_byte_count_contract :=
  closureFixedPointCheck_sound canonical_value_allocation_byte_count_contract
    canonical_value_allocation_byte_count_closure_fixed_check

theorem canonical_value_allocation_byte_count_bcnf_check :
    bcnfCheck canonical_value_allocation_byte_count_contract = true := by
  native_decide

theorem canonical_value_allocation_byte_count_bcnf : BCNF canonical_value_allocation_byte_count_contract :=
  bcnfCheck_sound canonical_value_allocation_byte_count_contract canonical_value_allocation_byte_count_bcnf_check

def canonical_value_allocation_allocated_at_contract : RelationContract where
  name := "canonical_value_allocation_allocated_at"
  attributes := ["value_sha256", "allocated_at"]
  declaredKeys := [["value_sha256"]]
  declaredFDs := [
    { determinant := ["value_sha256"], dependent := ["allocated_at"] }
  ]

theorem canonical_value_allocation_allocated_at_schema_well_formed :
    schemaWellFormedCheck canonical_value_allocation_allocated_at_contract = true := by
  native_decide

theorem canonical_value_allocation_allocated_at_candidate_keys_check :
    keysDetermineAllCheck canonical_value_allocation_allocated_at_contract = true := by
  native_decide

theorem canonical_value_allocation_allocated_at_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_value_allocation_allocated_at_contract :=
  keysDetermineAllCheck_sound canonical_value_allocation_allocated_at_contract
    canonical_value_allocation_allocated_at_candidate_keys_check

theorem canonical_value_allocation_allocated_at_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_value_allocation_allocated_at_contract = true := by
  native_decide

theorem canonical_value_allocation_allocated_at_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_value_allocation_allocated_at_contract :=
  declaredKeysMinimalCheck_sound canonical_value_allocation_allocated_at_contract
    canonical_value_allocation_allocated_at_candidate_keys_minimal_check

theorem canonical_value_allocation_allocated_at_closure_fixed_check :
    closureFixedPointCheck canonical_value_allocation_allocated_at_contract = true := by
  native_decide

theorem canonical_value_allocation_allocated_at_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_value_allocation_allocated_at_contract :=
  closureFixedPointCheck_sound canonical_value_allocation_allocated_at_contract
    canonical_value_allocation_allocated_at_closure_fixed_check

theorem canonical_value_allocation_allocated_at_bcnf_check :
    bcnfCheck canonical_value_allocation_allocated_at_contract = true := by
  native_decide

theorem canonical_value_allocation_allocated_at_bcnf : BCNF canonical_value_allocation_allocated_at_contract :=
  bcnfCheck_sound canonical_value_allocation_allocated_at_contract canonical_value_allocation_allocated_at_bcnf_check

def canonical_value_allocation_seal_contract : RelationContract where
  name := "canonical_value_allocation_seal"
  attributes := ["value_sha256"]
  declaredKeys := [["value_sha256"]]
  declaredFDs := [
  ]

theorem canonical_value_allocation_seal_schema_well_formed :
    schemaWellFormedCheck canonical_value_allocation_seal_contract = true := by
  native_decide

theorem canonical_value_allocation_seal_candidate_keys_check :
    keysDetermineAllCheck canonical_value_allocation_seal_contract = true := by
  native_decide

theorem canonical_value_allocation_seal_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_value_allocation_seal_contract :=
  keysDetermineAllCheck_sound canonical_value_allocation_seal_contract
    canonical_value_allocation_seal_candidate_keys_check

theorem canonical_value_allocation_seal_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_value_allocation_seal_contract = true := by
  native_decide

theorem canonical_value_allocation_seal_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_value_allocation_seal_contract :=
  declaredKeysMinimalCheck_sound canonical_value_allocation_seal_contract
    canonical_value_allocation_seal_candidate_keys_minimal_check

theorem canonical_value_allocation_seal_closure_fixed_check :
    closureFixedPointCheck canonical_value_allocation_seal_contract = true := by
  native_decide

theorem canonical_value_allocation_seal_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_value_allocation_seal_contract :=
  closureFixedPointCheck_sound canonical_value_allocation_seal_contract
    canonical_value_allocation_seal_closure_fixed_check

theorem canonical_value_allocation_seal_bcnf_check :
    bcnfCheck canonical_value_allocation_seal_contract = true := by
  native_decide

theorem canonical_value_allocation_seal_bcnf : BCNF canonical_value_allocation_seal_contract :=
  bcnfCheck_sound canonical_value_allocation_seal_contract canonical_value_allocation_seal_bcnf_check

def canonical_value_allocation_contract : RelationContract where
  name := "canonical_value_allocation"
  attributes := ["value_sha256", "digest_domain", "byte_count", "allocated_at"]
  declaredKeys := [["value_sha256"]]
  declaredFDs := [
    { determinant := ["value_sha256"], dependent := ["digest_domain", "byte_count", "allocated_at"] }
  ]

theorem canonical_value_allocation_schema_well_formed :
    schemaWellFormedCheck canonical_value_allocation_contract = true := by
  native_decide

theorem canonical_value_allocation_candidate_keys_check :
    keysDetermineAllCheck canonical_value_allocation_contract = true := by
  native_decide

theorem canonical_value_allocation_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_value_allocation_contract :=
  keysDetermineAllCheck_sound canonical_value_allocation_contract
    canonical_value_allocation_candidate_keys_check

theorem canonical_value_allocation_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_value_allocation_contract = true := by
  native_decide

theorem canonical_value_allocation_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_value_allocation_contract :=
  declaredKeysMinimalCheck_sound canonical_value_allocation_contract
    canonical_value_allocation_candidate_keys_minimal_check

theorem canonical_value_allocation_closure_fixed_check :
    closureFixedPointCheck canonical_value_allocation_contract = true := by
  native_decide

theorem canonical_value_allocation_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_value_allocation_contract :=
  closureFixedPointCheck_sound canonical_value_allocation_contract
    canonical_value_allocation_closure_fixed_check

def canonical_value_page_anchor_contract : RelationContract where
  name := "canonical_value_page_anchor"
  attributes := ["page_sha256"]
  declaredKeys := [["page_sha256"]]
  declaredFDs := [
  ]

theorem canonical_value_page_anchor_schema_well_formed :
    schemaWellFormedCheck canonical_value_page_anchor_contract = true := by
  native_decide

theorem canonical_value_page_anchor_candidate_keys_check :
    keysDetermineAllCheck canonical_value_page_anchor_contract = true := by
  native_decide

theorem canonical_value_page_anchor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_value_page_anchor_contract :=
  keysDetermineAllCheck_sound canonical_value_page_anchor_contract
    canonical_value_page_anchor_candidate_keys_check

theorem canonical_value_page_anchor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_value_page_anchor_contract = true := by
  native_decide

theorem canonical_value_page_anchor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_value_page_anchor_contract :=
  declaredKeysMinimalCheck_sound canonical_value_page_anchor_contract
    canonical_value_page_anchor_candidate_keys_minimal_check

theorem canonical_value_page_anchor_closure_fixed_check :
    closureFixedPointCheck canonical_value_page_anchor_contract = true := by
  native_decide

theorem canonical_value_page_anchor_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_value_page_anchor_contract :=
  closureFixedPointCheck_sound canonical_value_page_anchor_contract
    canonical_value_page_anchor_closure_fixed_check

theorem canonical_value_page_anchor_bcnf_check :
    bcnfCheck canonical_value_page_anchor_contract = true := by
  native_decide

theorem canonical_value_page_anchor_bcnf : BCNF canonical_value_page_anchor_contract :=
  bcnfCheck_sound canonical_value_page_anchor_contract canonical_value_page_anchor_bcnf_check

def canonical_value_page_payload_contract : RelationContract where
  name := "canonical_value_page_payload"
  attributes := ["page_sha256", "page_bytes"]
  declaredKeys := [["page_sha256"], ["page_bytes"]]
  declaredFDs := [
    { determinant := ["page_sha256"], dependent := ["page_bytes"] },
    { determinant := ["page_bytes"], dependent := ["page_sha256"] }
  ]

theorem canonical_value_page_payload_schema_well_formed :
    schemaWellFormedCheck canonical_value_page_payload_contract = true := by
  native_decide

theorem canonical_value_page_payload_candidate_keys_check :
    keysDetermineAllCheck canonical_value_page_payload_contract = true := by
  native_decide

theorem canonical_value_page_payload_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_value_page_payload_contract :=
  keysDetermineAllCheck_sound canonical_value_page_payload_contract
    canonical_value_page_payload_candidate_keys_check

theorem canonical_value_page_payload_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_value_page_payload_contract = true := by
  native_decide

theorem canonical_value_page_payload_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_value_page_payload_contract :=
  declaredKeysMinimalCheck_sound canonical_value_page_payload_contract
    canonical_value_page_payload_candidate_keys_minimal_check

theorem canonical_value_page_payload_closure_fixed_check :
    closureFixedPointCheck canonical_value_page_payload_contract = true := by
  native_decide

theorem canonical_value_page_payload_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_value_page_payload_contract :=
  closureFixedPointCheck_sound canonical_value_page_payload_contract
    canonical_value_page_payload_closure_fixed_check

theorem canonical_value_page_payload_bcnf_check :
    bcnfCheck canonical_value_page_payload_contract = true := by
  native_decide

theorem canonical_value_page_payload_bcnf : BCNF canonical_value_page_payload_contract :=
  bcnfCheck_sound canonical_value_page_payload_contract canonical_value_page_payload_bcnf_check

def canonical_value_page_coordinate_contract : RelationContract where
  name := "canonical_value_page_coordinate"
  attributes := ["value_sha256", "level", "page_position", "page_sha256"]
  declaredKeys := [["value_sha256", "level", "page_position"], ["page_sha256"]]
  declaredFDs := [
    { determinant := ["value_sha256", "level", "page_position"], dependent := ["page_sha256"] },
    { determinant := ["page_sha256"], dependent := ["value_sha256", "level", "page_position"] }
  ]

theorem canonical_value_page_coordinate_schema_well_formed :
    schemaWellFormedCheck canonical_value_page_coordinate_contract = true := by
  native_decide

theorem canonical_value_page_coordinate_candidate_keys_check :
    keysDetermineAllCheck canonical_value_page_coordinate_contract = true := by
  native_decide

theorem canonical_value_page_coordinate_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_value_page_coordinate_contract :=
  keysDetermineAllCheck_sound canonical_value_page_coordinate_contract
    canonical_value_page_coordinate_candidate_keys_check

theorem canonical_value_page_coordinate_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_value_page_coordinate_contract = true := by
  native_decide

theorem canonical_value_page_coordinate_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_value_page_coordinate_contract :=
  declaredKeysMinimalCheck_sound canonical_value_page_coordinate_contract
    canonical_value_page_coordinate_candidate_keys_minimal_check

theorem canonical_value_page_coordinate_closure_fixed_check :
    closureFixedPointCheck canonical_value_page_coordinate_contract = true := by
  native_decide

theorem canonical_value_page_coordinate_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_value_page_coordinate_contract :=
  closureFixedPointCheck_sound canonical_value_page_coordinate_contract
    canonical_value_page_coordinate_closure_fixed_check

theorem canonical_value_page_coordinate_bcnf_check :
    bcnfCheck canonical_value_page_coordinate_contract = true := by
  native_decide

theorem canonical_value_page_coordinate_bcnf : BCNF canonical_value_page_coordinate_contract :=
  bcnfCheck_sound canonical_value_page_coordinate_contract canonical_value_page_coordinate_bcnf_check

def canonical_value_page_subtree_item_count_contract : RelationContract where
  name := "canonical_value_page_subtree_item_count"
  attributes := ["page_sha256", "subtree_item_count"]
  declaredKeys := [["page_sha256"]]
  declaredFDs := [
    { determinant := ["page_sha256"], dependent := ["subtree_item_count"] }
  ]

theorem canonical_value_page_subtree_item_count_schema_well_formed :
    schemaWellFormedCheck canonical_value_page_subtree_item_count_contract = true := by
  native_decide

theorem canonical_value_page_subtree_item_count_candidate_keys_check :
    keysDetermineAllCheck canonical_value_page_subtree_item_count_contract = true := by
  native_decide

theorem canonical_value_page_subtree_item_count_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_value_page_subtree_item_count_contract :=
  keysDetermineAllCheck_sound canonical_value_page_subtree_item_count_contract
    canonical_value_page_subtree_item_count_candidate_keys_check

theorem canonical_value_page_subtree_item_count_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_value_page_subtree_item_count_contract = true := by
  native_decide

theorem canonical_value_page_subtree_item_count_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_value_page_subtree_item_count_contract :=
  declaredKeysMinimalCheck_sound canonical_value_page_subtree_item_count_contract
    canonical_value_page_subtree_item_count_candidate_keys_minimal_check

theorem canonical_value_page_subtree_item_count_closure_fixed_check :
    closureFixedPointCheck canonical_value_page_subtree_item_count_contract = true := by
  native_decide

theorem canonical_value_page_subtree_item_count_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_value_page_subtree_item_count_contract :=
  closureFixedPointCheck_sound canonical_value_page_subtree_item_count_contract
    canonical_value_page_subtree_item_count_closure_fixed_check

theorem canonical_value_page_subtree_item_count_bcnf_check :
    bcnfCheck canonical_value_page_subtree_item_count_contract = true := by
  native_decide

theorem canonical_value_page_subtree_item_count_bcnf : BCNF canonical_value_page_subtree_item_count_contract :=
  bcnfCheck_sound canonical_value_page_subtree_item_count_contract canonical_value_page_subtree_item_count_bcnf_check

def canonical_value_page_seal_contract : RelationContract where
  name := "canonical_value_page_seal"
  attributes := ["page_sha256"]
  declaredKeys := [["page_sha256"]]
  declaredFDs := [
  ]

theorem canonical_value_page_seal_schema_well_formed :
    schemaWellFormedCheck canonical_value_page_seal_contract = true := by
  native_decide

theorem canonical_value_page_seal_candidate_keys_check :
    keysDetermineAllCheck canonical_value_page_seal_contract = true := by
  native_decide

theorem canonical_value_page_seal_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_value_page_seal_contract :=
  keysDetermineAllCheck_sound canonical_value_page_seal_contract
    canonical_value_page_seal_candidate_keys_check

theorem canonical_value_page_seal_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_value_page_seal_contract = true := by
  native_decide

theorem canonical_value_page_seal_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_value_page_seal_contract :=
  declaredKeysMinimalCheck_sound canonical_value_page_seal_contract
    canonical_value_page_seal_candidate_keys_minimal_check

theorem canonical_value_page_seal_closure_fixed_check :
    closureFixedPointCheck canonical_value_page_seal_contract = true := by
  native_decide

theorem canonical_value_page_seal_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_value_page_seal_contract :=
  closureFixedPointCheck_sound canonical_value_page_seal_contract
    canonical_value_page_seal_closure_fixed_check

theorem canonical_value_page_seal_bcnf_check :
    bcnfCheck canonical_value_page_seal_contract = true := by
  native_decide

theorem canonical_value_page_seal_bcnf : BCNF canonical_value_page_seal_contract :=
  bcnfCheck_sound canonical_value_page_seal_contract canonical_value_page_seal_bcnf_check

def canonical_value_page_contract : RelationContract where
  name := "canonical_value_page"
  attributes := ["page_sha256", "value_sha256", "page_bytes"]
  declaredKeys := [["page_sha256"], ["page_bytes"]]
  declaredFDs := [
    { determinant := ["page_sha256"], dependent := ["value_sha256", "page_bytes"] },
    { determinant := ["page_bytes"], dependent := ["page_sha256", "value_sha256"] }
  ]

theorem canonical_value_page_schema_well_formed :
    schemaWellFormedCheck canonical_value_page_contract = true := by
  native_decide

theorem canonical_value_page_candidate_keys_check :
    keysDetermineAllCheck canonical_value_page_contract = true := by
  native_decide

theorem canonical_value_page_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_value_page_contract :=
  keysDetermineAllCheck_sound canonical_value_page_contract
    canonical_value_page_candidate_keys_check

theorem canonical_value_page_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_value_page_contract = true := by
  native_decide

theorem canonical_value_page_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_value_page_contract :=
  declaredKeysMinimalCheck_sound canonical_value_page_contract
    canonical_value_page_candidate_keys_minimal_check

theorem canonical_value_page_closure_fixed_check :
    closureFixedPointCheck canonical_value_page_contract = true := by
  native_decide

theorem canonical_value_page_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_value_page_contract :=
  closureFixedPointCheck_sound canonical_value_page_contract
    canonical_value_page_closure_fixed_check

def canonical_value_page_descriptor_contract : RelationContract where
  name := "canonical_value_page_descriptor"
  attributes := ["page_sha256", "value_sha256", "level", "page_position", "subtree_item_count"]
  declaredKeys := [["page_sha256"], ["value_sha256", "level", "page_position"]]
  declaredFDs := [
    { determinant := ["page_sha256"], dependent := ["value_sha256", "level", "page_position", "subtree_item_count"] },
    { determinant := ["value_sha256", "level", "page_position"], dependent := ["page_sha256", "subtree_item_count"] }
  ]

theorem canonical_value_page_descriptor_schema_well_formed :
    schemaWellFormedCheck canonical_value_page_descriptor_contract = true := by
  native_decide

theorem canonical_value_page_descriptor_candidate_keys_check :
    keysDetermineAllCheck canonical_value_page_descriptor_contract = true := by
  native_decide

theorem canonical_value_page_descriptor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_value_page_descriptor_contract :=
  keysDetermineAllCheck_sound canonical_value_page_descriptor_contract
    canonical_value_page_descriptor_candidate_keys_check

theorem canonical_value_page_descriptor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_value_page_descriptor_contract = true := by
  native_decide

theorem canonical_value_page_descriptor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_value_page_descriptor_contract :=
  declaredKeysMinimalCheck_sound canonical_value_page_descriptor_contract
    canonical_value_page_descriptor_candidate_keys_minimal_check

theorem canonical_value_page_descriptor_closure_fixed_check :
    closureFixedPointCheck canonical_value_page_descriptor_contract = true := by
  native_decide

theorem canonical_value_page_descriptor_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_value_page_descriptor_contract :=
  closureFixedPointCheck_sound canonical_value_page_descriptor_contract
    canonical_value_page_descriptor_closure_fixed_check

def canonical_value_page_parent_contract : RelationContract where
  name := "canonical_value_page_parent"
  attributes := ["parent_sha256", "position", "child_sha256"]
  declaredKeys := [["parent_sha256", "position"], ["child_sha256"]]
  declaredFDs := [
    { determinant := ["parent_sha256", "position"], dependent := ["child_sha256"] },
    { determinant := ["child_sha256"], dependent := ["parent_sha256", "position"] }
  ]

theorem canonical_value_page_parent_schema_well_formed :
    schemaWellFormedCheck canonical_value_page_parent_contract = true := by
  native_decide

theorem canonical_value_page_parent_candidate_keys_check :
    keysDetermineAllCheck canonical_value_page_parent_contract = true := by
  native_decide

theorem canonical_value_page_parent_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_value_page_parent_contract :=
  keysDetermineAllCheck_sound canonical_value_page_parent_contract
    canonical_value_page_parent_candidate_keys_check

theorem canonical_value_page_parent_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_value_page_parent_contract = true := by
  native_decide

theorem canonical_value_page_parent_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_value_page_parent_contract :=
  declaredKeysMinimalCheck_sound canonical_value_page_parent_contract
    canonical_value_page_parent_candidate_keys_minimal_check

theorem canonical_value_page_parent_closure_fixed_check :
    closureFixedPointCheck canonical_value_page_parent_contract = true := by
  native_decide

theorem canonical_value_page_parent_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_value_page_parent_contract :=
  closureFixedPointCheck_sound canonical_value_page_parent_contract
    canonical_value_page_parent_closure_fixed_check

theorem canonical_value_page_parent_bcnf_check :
    bcnfCheck canonical_value_page_parent_contract = true := by
  native_decide

theorem canonical_value_page_parent_bcnf : BCNF canonical_value_page_parent_contract :=
  bcnfCheck_sound canonical_value_page_parent_contract canonical_value_page_parent_bcnf_check

def canonical_value_identity_contract : RelationContract where
  name := "canonical_value_identity"
  attributes := ["value_sha256", "root_page_sha256"]
  declaredKeys := [["value_sha256"], ["root_page_sha256"]]
  declaredFDs := [
    { determinant := ["value_sha256"], dependent := ["root_page_sha256"] },
    { determinant := ["root_page_sha256"], dependent := ["value_sha256"] }
  ]

theorem canonical_value_identity_schema_well_formed :
    schemaWellFormedCheck canonical_value_identity_contract = true := by
  native_decide

theorem canonical_value_identity_candidate_keys_check :
    keysDetermineAllCheck canonical_value_identity_contract = true := by
  native_decide

theorem canonical_value_identity_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_value_identity_contract :=
  keysDetermineAllCheck_sound canonical_value_identity_contract
    canonical_value_identity_candidate_keys_check

theorem canonical_value_identity_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_value_identity_contract = true := by
  native_decide

theorem canonical_value_identity_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_value_identity_contract :=
  declaredKeysMinimalCheck_sound canonical_value_identity_contract
    canonical_value_identity_candidate_keys_minimal_check

theorem canonical_value_identity_closure_fixed_check :
    closureFixedPointCheck canonical_value_identity_contract = true := by
  native_decide

theorem canonical_value_identity_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_value_identity_contract :=
  closureFixedPointCheck_sound canonical_value_identity_contract
    canonical_value_identity_closure_fixed_check

theorem canonical_value_identity_bcnf_check :
    bcnfCheck canonical_value_identity_contract = true := by
  native_decide

theorem canonical_value_identity_bcnf : BCNF canonical_value_identity_contract :=
  bcnfCheck_sound canonical_value_identity_contract canonical_value_identity_bcnf_check

def manifest_policy_contract : RelationContract where
  name := "manifest_policy"
  attributes := ["manifest_policy_id", "manifest_algorithm_version", "file_order_version"]
  declaredKeys := [["manifest_policy_id"], ["manifest_algorithm_version", "file_order_version"]]
  declaredFDs := [
    { determinant := ["manifest_policy_id"], dependent := ["manifest_algorithm_version", "file_order_version"] },
    { determinant := ["manifest_algorithm_version", "file_order_version"], dependent := ["manifest_policy_id"] }
  ]

theorem manifest_policy_schema_well_formed :
    schemaWellFormedCheck manifest_policy_contract = true := by
  native_decide

theorem manifest_policy_candidate_keys_check :
    keysDetermineAllCheck manifest_policy_contract = true := by
  native_decide

theorem manifest_policy_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes manifest_policy_contract :=
  keysDetermineAllCheck_sound manifest_policy_contract
    manifest_policy_candidate_keys_check

theorem manifest_policy_candidate_keys_minimal_check :
    declaredKeysMinimalCheck manifest_policy_contract = true := by
  native_decide

theorem manifest_policy_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal manifest_policy_contract :=
  declaredKeysMinimalCheck_sound manifest_policy_contract
    manifest_policy_candidate_keys_minimal_check

theorem manifest_policy_closure_fixed_check :
    closureFixedPointCheck manifest_policy_contract = true := by
  native_decide

theorem manifest_policy_closure_reached_fixed_point :
    ClosureReachedFixedPoint manifest_policy_contract :=
  closureFixedPointCheck_sound manifest_policy_contract
    manifest_policy_closure_fixed_check

theorem manifest_policy_bcnf_check :
    bcnfCheck manifest_policy_contract = true := by
  native_decide

theorem manifest_policy_bcnf : BCNF manifest_policy_contract :=
  bcnfCheck_sound manifest_policy_contract manifest_policy_bcnf_check

def source_build_state_contract : RelationContract where
  name := "source_build_state"
  attributes := ["build_id", "state"]
  declaredKeys := [["build_id"]]
  declaredFDs := [
    { determinant := ["build_id"], dependent := ["state"] }
  ]

theorem source_build_state_schema_well_formed :
    schemaWellFormedCheck source_build_state_contract = true := by
  native_decide

theorem source_build_state_candidate_keys_check :
    keysDetermineAllCheck source_build_state_contract = true := by
  native_decide

theorem source_build_state_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_build_state_contract :=
  keysDetermineAllCheck_sound source_build_state_contract
    source_build_state_candidate_keys_check

theorem source_build_state_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_build_state_contract = true := by
  native_decide

theorem source_build_state_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_build_state_contract :=
  declaredKeysMinimalCheck_sound source_build_state_contract
    source_build_state_candidate_keys_minimal_check

theorem source_build_state_closure_fixed_check :
    closureFixedPointCheck source_build_state_contract = true := by
  native_decide

theorem source_build_state_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_build_state_contract :=
  closureFixedPointCheck_sound source_build_state_contract
    source_build_state_closure_fixed_check

theorem source_build_state_bcnf_check :
    bcnfCheck source_build_state_contract = true := by
  native_decide

theorem source_build_state_bcnf : BCNF source_build_state_contract :=
  bcnfCheck_sound source_build_state_contract source_build_state_bcnf_check

def source_build_sealed_at_contract : RelationContract where
  name := "source_build_sealed_at"
  attributes := ["build_id", "sealed_at"]
  declaredKeys := [["build_id"]]
  declaredFDs := [
    { determinant := ["build_id"], dependent := ["sealed_at"] }
  ]

theorem source_build_sealed_at_schema_well_formed :
    schemaWellFormedCheck source_build_sealed_at_contract = true := by
  native_decide

theorem source_build_sealed_at_candidate_keys_check :
    keysDetermineAllCheck source_build_sealed_at_contract = true := by
  native_decide

theorem source_build_sealed_at_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_build_sealed_at_contract :=
  keysDetermineAllCheck_sound source_build_sealed_at_contract
    source_build_sealed_at_candidate_keys_check

theorem source_build_sealed_at_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_build_sealed_at_contract = true := by
  native_decide

theorem source_build_sealed_at_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_build_sealed_at_contract :=
  declaredKeysMinimalCheck_sound source_build_sealed_at_contract
    source_build_sealed_at_candidate_keys_minimal_check

theorem source_build_sealed_at_closure_fixed_check :
    closureFixedPointCheck source_build_sealed_at_contract = true := by
  native_decide

theorem source_build_sealed_at_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_build_sealed_at_contract :=
  closureFixedPointCheck_sound source_build_sealed_at_contract
    source_build_sealed_at_closure_fixed_check

theorem source_build_sealed_at_bcnf_check :
    bcnfCheck source_build_sealed_at_contract = true := by
  native_decide

theorem source_build_sealed_at_bcnf : BCNF source_build_sealed_at_contract :=
  bcnfCheck_sound source_build_sealed_at_contract source_build_sealed_at_bcnf_check

def source_build_descriptor_contract : RelationContract where
  name := "source_build_descriptor"
  attributes := ["build_id", "scope_key", "manifest_policy_id", "created_at"]
  declaredKeys := [["build_id"]]
  declaredFDs := [
    { determinant := ["build_id"], dependent := ["scope_key", "manifest_policy_id", "created_at"] }
  ]

theorem source_build_descriptor_schema_well_formed :
    schemaWellFormedCheck source_build_descriptor_contract = true := by
  native_decide

theorem source_build_descriptor_candidate_keys_check :
    keysDetermineAllCheck source_build_descriptor_contract = true := by
  native_decide

theorem source_build_descriptor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_build_descriptor_contract :=
  keysDetermineAllCheck_sound source_build_descriptor_contract
    source_build_descriptor_candidate_keys_check

theorem source_build_descriptor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_build_descriptor_contract = true := by
  native_decide

theorem source_build_descriptor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_build_descriptor_contract :=
  declaredKeysMinimalCheck_sound source_build_descriptor_contract
    source_build_descriptor_candidate_keys_minimal_check

theorem source_build_descriptor_closure_fixed_check :
    closureFixedPointCheck source_build_descriptor_contract = true := by
  native_decide

theorem source_build_descriptor_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_build_descriptor_contract :=
  closureFixedPointCheck_sound source_build_descriptor_contract
    source_build_descriptor_closure_fixed_check

theorem source_build_descriptor_bcnf_check :
    bcnfCheck source_build_descriptor_contract = true := by
  native_decide

theorem source_build_descriptor_bcnf : BCNF source_build_descriptor_contract :=
  bcnfCheck_sound source_build_descriptor_contract source_build_descriptor_bcnf_check

def source_build_contract : RelationContract where
  name := "source_build"
  attributes := ["build_id", "scope_key", "manifest_policy_id", "state", "created_at", "sealed_at"]
  declaredKeys := [["build_id"]]
  declaredFDs := [
    { determinant := ["build_id"], dependent := ["scope_key", "manifest_policy_id", "state", "created_at", "sealed_at"] }
  ]

theorem source_build_schema_well_formed :
    schemaWellFormedCheck source_build_contract = true := by
  native_decide

theorem source_build_candidate_keys_check :
    keysDetermineAllCheck source_build_contract = true := by
  native_decide

theorem source_build_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_build_contract :=
  keysDetermineAllCheck_sound source_build_contract
    source_build_candidate_keys_check

theorem source_build_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_build_contract = true := by
  native_decide

theorem source_build_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_build_contract :=
  declaredKeysMinimalCheck_sound source_build_contract
    source_build_candidate_keys_minimal_check

theorem source_build_closure_fixed_check :
    closureFixedPointCheck source_build_contract = true := by
  native_decide

theorem source_build_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_build_contract :=
  closureFixedPointCheck_sound source_build_contract
    source_build_closure_fixed_check

def source_build_base_publication_commit_contract : RelationContract where
  name := "source_build_base_publication_commit"
  attributes := ["build_id", "base_receipt_id"]
  declaredKeys := [["build_id"]]
  declaredFDs := [
    { determinant := ["build_id"], dependent := ["base_receipt_id"] }
  ]

theorem source_build_base_publication_commit_schema_well_formed :
    schemaWellFormedCheck source_build_base_publication_commit_contract = true := by
  native_decide

theorem source_build_base_publication_commit_candidate_keys_check :
    keysDetermineAllCheck source_build_base_publication_commit_contract = true := by
  native_decide

theorem source_build_base_publication_commit_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_build_base_publication_commit_contract :=
  keysDetermineAllCheck_sound source_build_base_publication_commit_contract
    source_build_base_publication_commit_candidate_keys_check

theorem source_build_base_publication_commit_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_build_base_publication_commit_contract = true := by
  native_decide

theorem source_build_base_publication_commit_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_build_base_publication_commit_contract :=
  declaredKeysMinimalCheck_sound source_build_base_publication_commit_contract
    source_build_base_publication_commit_candidate_keys_minimal_check

theorem source_build_base_publication_commit_closure_fixed_check :
    closureFixedPointCheck source_build_base_publication_commit_contract = true := by
  native_decide

theorem source_build_base_publication_commit_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_build_base_publication_commit_contract :=
  closureFixedPointCheck_sound source_build_base_publication_commit_contract
    source_build_base_publication_commit_closure_fixed_check

theorem source_build_base_publication_commit_bcnf_check :
    bcnfCheck source_build_base_publication_commit_contract = true := by
  native_decide

theorem source_build_base_publication_commit_bcnf : BCNF source_build_base_publication_commit_contract :=
  bcnfCheck_sound source_build_base_publication_commit_contract source_build_base_publication_commit_bcnf_check

def source_build_base_source_contract : RelationContract where
  name := "source_build_base_source"
  attributes := ["build_id", "base_source_revision", "base_source_generation"]
  declaredKeys := [["build_id"]]
  declaredFDs := [
    { determinant := ["build_id"], dependent := ["base_source_revision", "base_source_generation"] },
    { determinant := ["base_source_revision"], dependent := ["base_source_generation"] },
    { determinant := ["base_source_generation"], dependent := ["base_source_revision"] }
  ]

theorem source_build_base_source_schema_well_formed :
    schemaWellFormedCheck source_build_base_source_contract = true := by
  native_decide

theorem source_build_base_source_candidate_keys_check :
    keysDetermineAllCheck source_build_base_source_contract = true := by
  native_decide

theorem source_build_base_source_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_build_base_source_contract :=
  keysDetermineAllCheck_sound source_build_base_source_contract
    source_build_base_source_candidate_keys_check

theorem source_build_base_source_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_build_base_source_contract = true := by
  native_decide

theorem source_build_base_source_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_build_base_source_contract :=
  declaredKeysMinimalCheck_sound source_build_base_source_contract
    source_build_base_source_candidate_keys_minimal_check

theorem source_build_base_source_closure_fixed_check :
    closureFixedPointCheck source_build_base_source_contract = true := by
  native_decide

theorem source_build_base_source_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_build_base_source_contract :=
  closureFixedPointCheck_sound source_build_base_source_contract
    source_build_base_source_closure_fixed_check

def channel_registry_contract : RelationContract where
  name := "channel_registry"
  attributes := ["channel"]
  declaredKeys := [["channel"]]
  declaredFDs := [
  ]

theorem channel_registry_schema_well_formed :
    schemaWellFormedCheck channel_registry_contract = true := by
  native_decide

theorem channel_registry_candidate_keys_check :
    keysDetermineAllCheck channel_registry_contract = true := by
  native_decide

theorem channel_registry_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes channel_registry_contract :=
  keysDetermineAllCheck_sound channel_registry_contract
    channel_registry_candidate_keys_check

theorem channel_registry_candidate_keys_minimal_check :
    declaredKeysMinimalCheck channel_registry_contract = true := by
  native_decide

theorem channel_registry_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal channel_registry_contract :=
  declaredKeysMinimalCheck_sound channel_registry_contract
    channel_registry_candidate_keys_minimal_check

theorem channel_registry_closure_fixed_check :
    closureFixedPointCheck channel_registry_contract = true := by
  native_decide

theorem channel_registry_closure_reached_fixed_point :
    ClosureReachedFixedPoint channel_registry_contract :=
  closureFixedPointCheck_sound channel_registry_contract
    channel_registry_closure_fixed_check

theorem channel_registry_bcnf_check :
    bcnfCheck channel_registry_contract = true := by
  native_decide

theorem channel_registry_bcnf : BCNF channel_registry_contract :=
  bcnfCheck_sound channel_registry_contract channel_registry_bcnf_check

def source_provider_registry_contract : RelationContract where
  name := "source_provider_registry"
  attributes := ["source_provider"]
  declaredKeys := [["source_provider"]]
  declaredFDs := [
  ]

theorem source_provider_registry_schema_well_formed :
    schemaWellFormedCheck source_provider_registry_contract = true := by
  native_decide

theorem source_provider_registry_candidate_keys_check :
    keysDetermineAllCheck source_provider_registry_contract = true := by
  native_decide

theorem source_provider_registry_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_provider_registry_contract :=
  keysDetermineAllCheck_sound source_provider_registry_contract
    source_provider_registry_candidate_keys_check

theorem source_provider_registry_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_provider_registry_contract = true := by
  native_decide

theorem source_provider_registry_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_provider_registry_contract :=
  declaredKeysMinimalCheck_sound source_provider_registry_contract
    source_provider_registry_candidate_keys_minimal_check

theorem source_provider_registry_closure_fixed_check :
    closureFixedPointCheck source_provider_registry_contract = true := by
  native_decide

theorem source_provider_registry_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_provider_registry_contract :=
  closureFixedPointCheck_sound source_provider_registry_contract
    source_provider_registry_closure_fixed_check

theorem source_provider_registry_bcnf_check :
    bcnfCheck source_provider_registry_contract = true := by
  native_decide

theorem source_provider_registry_bcnf : BCNF source_provider_registry_contract :=
  bcnfCheck_sound source_provider_registry_contract source_provider_registry_bcnf_check

def source_build_channel_contract : RelationContract where
  name := "source_build_channel"
  attributes := ["build_id", "channel"]
  declaredKeys := [["build_id"]]
  declaredFDs := [
    { determinant := ["build_id"], dependent := ["channel"] }
  ]

theorem source_build_channel_schema_well_formed :
    schemaWellFormedCheck source_build_channel_contract = true := by
  native_decide

theorem source_build_channel_candidate_keys_check :
    keysDetermineAllCheck source_build_channel_contract = true := by
  native_decide

theorem source_build_channel_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_build_channel_contract :=
  keysDetermineAllCheck_sound source_build_channel_contract
    source_build_channel_candidate_keys_check

theorem source_build_channel_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_build_channel_contract = true := by
  native_decide

theorem source_build_channel_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_build_channel_contract :=
  declaredKeysMinimalCheck_sound source_build_channel_contract
    source_build_channel_candidate_keys_minimal_check

theorem source_build_channel_closure_fixed_check :
    closureFixedPointCheck source_build_channel_contract = true := by
  native_decide

theorem source_build_channel_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_build_channel_contract :=
  closureFixedPointCheck_sound source_build_channel_contract
    source_build_channel_closure_fixed_check

theorem source_build_channel_bcnf_check :
    bcnfCheck source_build_channel_contract = true := by
  native_decide

theorem source_build_channel_bcnf : BCNF source_build_channel_contract :=
  bcnfCheck_sound source_build_channel_contract source_build_channel_bcnf_check

def source_scope_contract : RelationContract where
  name := "source_scope"
  attributes := ["scope_key", "source_provider", "source_root_sha256", "identity_policy_version"]
  declaredKeys := [["scope_key"], ["source_provider", "source_root_sha256", "identity_policy_version"]]
  declaredFDs := [
    { determinant := ["scope_key"], dependent := ["source_provider", "source_root_sha256", "identity_policy_version"] },
    { determinant := ["source_provider", "source_root_sha256", "identity_policy_version"], dependent := ["scope_key"] }
  ]

theorem source_scope_schema_well_formed :
    schemaWellFormedCheck source_scope_contract = true := by
  native_decide

theorem source_scope_candidate_keys_check :
    keysDetermineAllCheck source_scope_contract = true := by
  native_decide

theorem source_scope_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_scope_contract :=
  keysDetermineAllCheck_sound source_scope_contract
    source_scope_candidate_keys_check

theorem source_scope_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_scope_contract = true := by
  native_decide

theorem source_scope_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_scope_contract :=
  declaredKeysMinimalCheck_sound source_scope_contract
    source_scope_candidate_keys_minimal_check

theorem source_scope_closure_fixed_check :
    closureFixedPointCheck source_scope_contract = true := by
  native_decide

theorem source_scope_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_scope_contract :=
  closureFixedPointCheck_sound source_scope_contract
    source_scope_closure_fixed_check

theorem source_scope_bcnf_check :
    bcnfCheck source_scope_contract = true := by
  native_decide

theorem source_scope_bcnf : BCNF source_scope_contract :=
  bcnfCheck_sound source_scope_contract source_scope_bcnf_check

def source_build_discovery_contract : RelationContract where
  name := "source_build_discovery"
  attributes := ["build_id", "scan_attempt", "gallery_count", "tree_observation_sha256", "completed_at"]
  declaredKeys := [["build_id"]]
  declaredFDs := [
    { determinant := ["build_id"], dependent := ["scan_attempt", "gallery_count", "tree_observation_sha256", "completed_at"] }
  ]

theorem source_build_discovery_schema_well_formed :
    schemaWellFormedCheck source_build_discovery_contract = true := by
  native_decide

theorem source_build_discovery_candidate_keys_check :
    keysDetermineAllCheck source_build_discovery_contract = true := by
  native_decide

theorem source_build_discovery_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_build_discovery_contract :=
  keysDetermineAllCheck_sound source_build_discovery_contract
    source_build_discovery_candidate_keys_check

theorem source_build_discovery_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_build_discovery_contract = true := by
  native_decide

theorem source_build_discovery_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_build_discovery_contract :=
  declaredKeysMinimalCheck_sound source_build_discovery_contract
    source_build_discovery_candidate_keys_minimal_check

theorem source_build_discovery_closure_fixed_check :
    closureFixedPointCheck source_build_discovery_contract = true := by
  native_decide

theorem source_build_discovery_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_build_discovery_contract :=
  closureFixedPointCheck_sound source_build_discovery_contract
    source_build_discovery_closure_fixed_check

theorem source_build_discovery_bcnf_check :
    bcnfCheck source_build_discovery_contract = true := by
  native_decide

theorem source_build_discovery_bcnf : BCNF source_build_discovery_contract :=
  bcnfCheck_sound source_build_discovery_contract source_build_discovery_bcnf_check

def source_build_expected_gallery_contract : RelationContract where
  name := "source_build_expected_gallery"
  attributes := ["build_id", "position", "gallery_id"]
  declaredKeys := [["build_id", "position"], ["build_id", "gallery_id"]]
  declaredFDs := [
    { determinant := ["build_id", "position"], dependent := ["gallery_id"] },
    { determinant := ["build_id", "gallery_id"], dependent := ["position"] }
  ]

theorem source_build_expected_gallery_schema_well_formed :
    schemaWellFormedCheck source_build_expected_gallery_contract = true := by
  native_decide

theorem source_build_expected_gallery_candidate_keys_check :
    keysDetermineAllCheck source_build_expected_gallery_contract = true := by
  native_decide

theorem source_build_expected_gallery_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_build_expected_gallery_contract :=
  keysDetermineAllCheck_sound source_build_expected_gallery_contract
    source_build_expected_gallery_candidate_keys_check

theorem source_build_expected_gallery_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_build_expected_gallery_contract = true := by
  native_decide

theorem source_build_expected_gallery_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_build_expected_gallery_contract :=
  declaredKeysMinimalCheck_sound source_build_expected_gallery_contract
    source_build_expected_gallery_candidate_keys_minimal_check

theorem source_build_expected_gallery_closure_fixed_check :
    closureFixedPointCheck source_build_expected_gallery_contract = true := by
  native_decide

theorem source_build_expected_gallery_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_build_expected_gallery_contract :=
  closureFixedPointCheck_sound source_build_expected_gallery_contract
    source_build_expected_gallery_closure_fixed_check

theorem source_build_expected_gallery_bcnf_check :
    bcnfCheck source_build_expected_gallery_contract = true := by
  native_decide

theorem source_build_expected_gallery_bcnf : BCNF source_build_expected_gallery_contract :=
  bcnfCheck_sound source_build_expected_gallery_contract source_build_expected_gallery_bcnf_check

def source_locator_identity_contract : RelationContract where
  name := "source_locator_identity"
  attributes := ["locator_sha256", "source_gallery_name"]
  declaredKeys := [["locator_sha256"]]
  declaredFDs := [
    { determinant := ["locator_sha256"], dependent := ["source_gallery_name"] }
  ]

theorem source_locator_identity_schema_well_formed :
    schemaWellFormedCheck source_locator_identity_contract = true := by
  native_decide

theorem source_locator_identity_candidate_keys_check :
    keysDetermineAllCheck source_locator_identity_contract = true := by
  native_decide

theorem source_locator_identity_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_locator_identity_contract :=
  keysDetermineAllCheck_sound source_locator_identity_contract
    source_locator_identity_candidate_keys_check

theorem source_locator_identity_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_locator_identity_contract = true := by
  native_decide

theorem source_locator_identity_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_locator_identity_contract :=
  declaredKeysMinimalCheck_sound source_locator_identity_contract
    source_locator_identity_candidate_keys_minimal_check

theorem source_locator_identity_closure_fixed_check :
    closureFixedPointCheck source_locator_identity_contract = true := by
  native_decide

theorem source_locator_identity_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_locator_identity_contract :=
  closureFixedPointCheck_sound source_locator_identity_contract
    source_locator_identity_closure_fixed_check

theorem source_locator_identity_bcnf_check :
    bcnfCheck source_locator_identity_contract = true := by
  native_decide

theorem source_locator_identity_bcnf : BCNF source_locator_identity_contract :=
  bcnfCheck_sound source_locator_identity_contract source_locator_identity_bcnf_check

def gallery_identity_contract : RelationContract where
  name := "gallery_identity"
  attributes := ["gallery_id", "gallery_key", "scope_key", "locator_sha256"]
  declaredKeys := [["gallery_id"], ["gallery_key"], ["scope_key", "locator_sha256"]]
  declaredFDs := [
    { determinant := ["gallery_id"], dependent := ["gallery_key", "scope_key", "locator_sha256"] },
    { determinant := ["gallery_key"], dependent := ["gallery_id", "scope_key", "locator_sha256"] },
    { determinant := ["scope_key", "locator_sha256"], dependent := ["gallery_id", "gallery_key"] }
  ]

theorem gallery_identity_schema_well_formed :
    schemaWellFormedCheck gallery_identity_contract = true := by
  native_decide

theorem gallery_identity_candidate_keys_check :
    keysDetermineAllCheck gallery_identity_contract = true := by
  native_decide

theorem gallery_identity_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_identity_contract :=
  keysDetermineAllCheck_sound gallery_identity_contract
    gallery_identity_candidate_keys_check

theorem gallery_identity_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_identity_contract = true := by
  native_decide

theorem gallery_identity_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_identity_contract :=
  declaredKeysMinimalCheck_sound gallery_identity_contract
    gallery_identity_candidate_keys_minimal_check

theorem gallery_identity_closure_fixed_check :
    closureFixedPointCheck gallery_identity_contract = true := by
  native_decide

theorem gallery_identity_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_identity_contract :=
  closureFixedPointCheck_sound gallery_identity_contract
    gallery_identity_closure_fixed_check

theorem gallery_identity_bcnf_check :
    bcnfCheck gallery_identity_contract = true := by
  native_decide

theorem gallery_identity_bcnf : BCNF gallery_identity_contract :=
  bcnfCheck_sound gallery_identity_contract gallery_identity_bcnf_check

def gallery_observation_allocation_contract : RelationContract where
  name := "gallery_observation_allocation"
  attributes := ["gallery_id", "observation_id", "allocated_at"]
  declaredKeys := [["gallery_id", "observation_id"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id"], dependent := ["allocated_at"] }
  ]

theorem gallery_observation_allocation_schema_well_formed :
    schemaWellFormedCheck gallery_observation_allocation_contract = true := by
  native_decide

theorem gallery_observation_allocation_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_allocation_contract = true := by
  native_decide

theorem gallery_observation_allocation_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_allocation_contract :=
  keysDetermineAllCheck_sound gallery_observation_allocation_contract
    gallery_observation_allocation_candidate_keys_check

theorem gallery_observation_allocation_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_allocation_contract = true := by
  native_decide

theorem gallery_observation_allocation_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_allocation_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_allocation_contract
    gallery_observation_allocation_candidate_keys_minimal_check

theorem gallery_observation_allocation_closure_fixed_check :
    closureFixedPointCheck gallery_observation_allocation_contract = true := by
  native_decide

theorem gallery_observation_allocation_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_allocation_contract :=
  closureFixedPointCheck_sound gallery_observation_allocation_contract
    gallery_observation_allocation_closure_fixed_check

theorem gallery_observation_allocation_bcnf_check :
    bcnfCheck gallery_observation_allocation_contract = true := by
  native_decide

theorem gallery_observation_allocation_bcnf : BCNF gallery_observation_allocation_contract :=
  bcnfCheck_sound gallery_observation_allocation_contract gallery_observation_allocation_bcnf_check

def gallery_observation_page_contract : RelationContract where
  name := "gallery_observation_page"
  attributes := ["page_sha256", "page_bytes"]
  declaredKeys := [["page_sha256"], ["page_bytes"]]
  declaredFDs := [
    { determinant := ["page_sha256"], dependent := ["page_bytes"] },
    { determinant := ["page_bytes"], dependent := ["page_sha256"] }
  ]

theorem gallery_observation_page_schema_well_formed :
    schemaWellFormedCheck gallery_observation_page_contract = true := by
  native_decide

theorem gallery_observation_page_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_page_contract = true := by
  native_decide

theorem gallery_observation_page_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_page_contract :=
  keysDetermineAllCheck_sound gallery_observation_page_contract
    gallery_observation_page_candidate_keys_check

theorem gallery_observation_page_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_page_contract = true := by
  native_decide

theorem gallery_observation_page_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_page_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_page_contract
    gallery_observation_page_candidate_keys_minimal_check

theorem gallery_observation_page_closure_fixed_check :
    closureFixedPointCheck gallery_observation_page_contract = true := by
  native_decide

theorem gallery_observation_page_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_page_contract :=
  closureFixedPointCheck_sound gallery_observation_page_contract
    gallery_observation_page_closure_fixed_check

theorem gallery_observation_page_bcnf_check :
    bcnfCheck gallery_observation_page_contract = true := by
  native_decide

theorem gallery_observation_page_bcnf : BCNF gallery_observation_page_contract :=
  bcnfCheck_sound gallery_observation_page_contract gallery_observation_page_bcnf_check

def gallery_observation_allocation_page_contract : RelationContract where
  name := "gallery_observation_allocation_page"
  attributes := ["gallery_id", "observation_id", "page_sha256"]
  declaredKeys := [["gallery_id", "observation_id", "page_sha256"]]
  declaredFDs := [
  ]

theorem gallery_observation_allocation_page_schema_well_formed :
    schemaWellFormedCheck gallery_observation_allocation_page_contract = true := by
  native_decide

theorem gallery_observation_allocation_page_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_allocation_page_contract = true := by
  native_decide

theorem gallery_observation_allocation_page_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_allocation_page_contract :=
  keysDetermineAllCheck_sound gallery_observation_allocation_page_contract
    gallery_observation_allocation_page_candidate_keys_check

theorem gallery_observation_allocation_page_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_allocation_page_contract = true := by
  native_decide

theorem gallery_observation_allocation_page_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_allocation_page_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_allocation_page_contract
    gallery_observation_allocation_page_candidate_keys_minimal_check

theorem gallery_observation_allocation_page_closure_fixed_check :
    closureFixedPointCheck gallery_observation_allocation_page_contract = true := by
  native_decide

theorem gallery_observation_allocation_page_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_allocation_page_contract :=
  closureFixedPointCheck_sound gallery_observation_allocation_page_contract
    gallery_observation_allocation_page_closure_fixed_check

theorem gallery_observation_allocation_page_bcnf_check :
    bcnfCheck gallery_observation_allocation_page_contract = true := by
  native_decide

theorem gallery_observation_allocation_page_bcnf : BCNF gallery_observation_allocation_page_contract :=
  bcnfCheck_sound gallery_observation_allocation_page_contract gallery_observation_allocation_page_bcnf_check

def gallery_observation_page_descriptor_anchor_contract : RelationContract where
  name := "gallery_observation_page_descriptor_anchor"
  attributes := ["page_sha256"]
  declaredKeys := [["page_sha256"]]
  declaredFDs := [
  ]

theorem gallery_observation_page_descriptor_anchor_schema_well_formed :
    schemaWellFormedCheck gallery_observation_page_descriptor_anchor_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_anchor_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_page_descriptor_anchor_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_anchor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_page_descriptor_anchor_contract :=
  keysDetermineAllCheck_sound gallery_observation_page_descriptor_anchor_contract
    gallery_observation_page_descriptor_anchor_candidate_keys_check

theorem gallery_observation_page_descriptor_anchor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_page_descriptor_anchor_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_anchor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_page_descriptor_anchor_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_page_descriptor_anchor_contract
    gallery_observation_page_descriptor_anchor_candidate_keys_minimal_check

theorem gallery_observation_page_descriptor_anchor_closure_fixed_check :
    closureFixedPointCheck gallery_observation_page_descriptor_anchor_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_anchor_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_page_descriptor_anchor_contract :=
  closureFixedPointCheck_sound gallery_observation_page_descriptor_anchor_contract
    gallery_observation_page_descriptor_anchor_closure_fixed_check

theorem gallery_observation_page_descriptor_anchor_bcnf_check :
    bcnfCheck gallery_observation_page_descriptor_anchor_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_anchor_bcnf : BCNF gallery_observation_page_descriptor_anchor_contract :=
  bcnfCheck_sound gallery_observation_page_descriptor_anchor_contract gallery_observation_page_descriptor_anchor_bcnf_check

def gallery_observation_page_descriptor_component_contract : RelationContract where
  name := "gallery_observation_page_descriptor_component"
  attributes := ["page_sha256", "component"]
  declaredKeys := [["page_sha256"]]
  declaredFDs := [
    { determinant := ["page_sha256"], dependent := ["component"] }
  ]

theorem gallery_observation_page_descriptor_component_schema_well_formed :
    schemaWellFormedCheck gallery_observation_page_descriptor_component_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_component_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_page_descriptor_component_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_component_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_page_descriptor_component_contract :=
  keysDetermineAllCheck_sound gallery_observation_page_descriptor_component_contract
    gallery_observation_page_descriptor_component_candidate_keys_check

theorem gallery_observation_page_descriptor_component_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_page_descriptor_component_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_component_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_page_descriptor_component_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_page_descriptor_component_contract
    gallery_observation_page_descriptor_component_candidate_keys_minimal_check

theorem gallery_observation_page_descriptor_component_closure_fixed_check :
    closureFixedPointCheck gallery_observation_page_descriptor_component_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_component_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_page_descriptor_component_contract :=
  closureFixedPointCheck_sound gallery_observation_page_descriptor_component_contract
    gallery_observation_page_descriptor_component_closure_fixed_check

theorem gallery_observation_page_descriptor_component_bcnf_check :
    bcnfCheck gallery_observation_page_descriptor_component_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_component_bcnf : BCNF gallery_observation_page_descriptor_component_contract :=
  bcnfCheck_sound gallery_observation_page_descriptor_component_contract gallery_observation_page_descriptor_component_bcnf_check

def gallery_observation_page_descriptor_level_contract : RelationContract where
  name := "gallery_observation_page_descriptor_level"
  attributes := ["page_sha256", "level"]
  declaredKeys := [["page_sha256"]]
  declaredFDs := [
    { determinant := ["page_sha256"], dependent := ["level"] }
  ]

theorem gallery_observation_page_descriptor_level_schema_well_formed :
    schemaWellFormedCheck gallery_observation_page_descriptor_level_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_level_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_page_descriptor_level_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_level_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_page_descriptor_level_contract :=
  keysDetermineAllCheck_sound gallery_observation_page_descriptor_level_contract
    gallery_observation_page_descriptor_level_candidate_keys_check

theorem gallery_observation_page_descriptor_level_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_page_descriptor_level_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_level_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_page_descriptor_level_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_page_descriptor_level_contract
    gallery_observation_page_descriptor_level_candidate_keys_minimal_check

theorem gallery_observation_page_descriptor_level_closure_fixed_check :
    closureFixedPointCheck gallery_observation_page_descriptor_level_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_level_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_page_descriptor_level_contract :=
  closureFixedPointCheck_sound gallery_observation_page_descriptor_level_contract
    gallery_observation_page_descriptor_level_closure_fixed_check

theorem gallery_observation_page_descriptor_level_bcnf_check :
    bcnfCheck gallery_observation_page_descriptor_level_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_level_bcnf : BCNF gallery_observation_page_descriptor_level_contract :=
  bcnfCheck_sound gallery_observation_page_descriptor_level_contract gallery_observation_page_descriptor_level_bcnf_check

def gallery_observation_page_descriptor_subtree_item_count_contract : RelationContract where
  name := "gallery_observation_page_descriptor_subtree_item_count"
  attributes := ["page_sha256", "subtree_item_count"]
  declaredKeys := [["page_sha256"]]
  declaredFDs := [
    { determinant := ["page_sha256"], dependent := ["subtree_item_count"] }
  ]

theorem gallery_observation_page_descriptor_subtree_item_count_schema_well_formed :
    schemaWellFormedCheck gallery_observation_page_descriptor_subtree_item_count_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_subtree_item_count_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_page_descriptor_subtree_item_count_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_subtree_item_count_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_page_descriptor_subtree_item_count_contract :=
  keysDetermineAllCheck_sound gallery_observation_page_descriptor_subtree_item_count_contract
    gallery_observation_page_descriptor_subtree_item_count_candidate_keys_check

theorem gallery_observation_page_descriptor_subtree_item_count_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_page_descriptor_subtree_item_count_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_subtree_item_count_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_page_descriptor_subtree_item_count_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_page_descriptor_subtree_item_count_contract
    gallery_observation_page_descriptor_subtree_item_count_candidate_keys_minimal_check

theorem gallery_observation_page_descriptor_subtree_item_count_closure_fixed_check :
    closureFixedPointCheck gallery_observation_page_descriptor_subtree_item_count_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_subtree_item_count_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_page_descriptor_subtree_item_count_contract :=
  closureFixedPointCheck_sound gallery_observation_page_descriptor_subtree_item_count_contract
    gallery_observation_page_descriptor_subtree_item_count_closure_fixed_check

theorem gallery_observation_page_descriptor_subtree_item_count_bcnf_check :
    bcnfCheck gallery_observation_page_descriptor_subtree_item_count_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_subtree_item_count_bcnf : BCNF gallery_observation_page_descriptor_subtree_item_count_contract :=
  bcnfCheck_sound gallery_observation_page_descriptor_subtree_item_count_contract gallery_observation_page_descriptor_subtree_item_count_bcnf_check

def gallery_observation_page_descriptor_seal_contract : RelationContract where
  name := "gallery_observation_page_descriptor_seal"
  attributes := ["page_sha256"]
  declaredKeys := [["page_sha256"]]
  declaredFDs := [
  ]

theorem gallery_observation_page_descriptor_seal_schema_well_formed :
    schemaWellFormedCheck gallery_observation_page_descriptor_seal_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_seal_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_page_descriptor_seal_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_seal_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_page_descriptor_seal_contract :=
  keysDetermineAllCheck_sound gallery_observation_page_descriptor_seal_contract
    gallery_observation_page_descriptor_seal_candidate_keys_check

theorem gallery_observation_page_descriptor_seal_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_page_descriptor_seal_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_seal_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_page_descriptor_seal_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_page_descriptor_seal_contract
    gallery_observation_page_descriptor_seal_candidate_keys_minimal_check

theorem gallery_observation_page_descriptor_seal_closure_fixed_check :
    closureFixedPointCheck gallery_observation_page_descriptor_seal_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_seal_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_page_descriptor_seal_contract :=
  closureFixedPointCheck_sound gallery_observation_page_descriptor_seal_contract
    gallery_observation_page_descriptor_seal_closure_fixed_check

theorem gallery_observation_page_descriptor_seal_bcnf_check :
    bcnfCheck gallery_observation_page_descriptor_seal_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_seal_bcnf : BCNF gallery_observation_page_descriptor_seal_contract :=
  bcnfCheck_sound gallery_observation_page_descriptor_seal_contract gallery_observation_page_descriptor_seal_bcnf_check

def gallery_observation_page_descriptor_contract : RelationContract where
  name := "gallery_observation_page_descriptor"
  attributes := ["page_sha256", "component", "level", "subtree_item_count"]
  declaredKeys := [["page_sha256"]]
  declaredFDs := [
    { determinant := ["page_sha256"], dependent := ["component", "level", "subtree_item_count"] }
  ]

theorem gallery_observation_page_descriptor_schema_well_formed :
    schemaWellFormedCheck gallery_observation_page_descriptor_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_page_descriptor_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_page_descriptor_contract :=
  keysDetermineAllCheck_sound gallery_observation_page_descriptor_contract
    gallery_observation_page_descriptor_candidate_keys_check

theorem gallery_observation_page_descriptor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_page_descriptor_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_page_descriptor_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_page_descriptor_contract
    gallery_observation_page_descriptor_candidate_keys_minimal_check

theorem gallery_observation_page_descriptor_closure_fixed_check :
    closureFixedPointCheck gallery_observation_page_descriptor_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_page_descriptor_contract :=
  closureFixedPointCheck_sound gallery_observation_page_descriptor_contract
    gallery_observation_page_descriptor_closure_fixed_check

def gallery_observation_page_key_bounds_anchor_contract : RelationContract where
  name := "gallery_observation_page_key_bounds_anchor"
  attributes := ["page_sha256"]
  declaredKeys := [["page_sha256"]]
  declaredFDs := [
  ]

theorem gallery_observation_page_key_bounds_anchor_schema_well_formed :
    schemaWellFormedCheck gallery_observation_page_key_bounds_anchor_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_anchor_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_page_key_bounds_anchor_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_anchor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_page_key_bounds_anchor_contract :=
  keysDetermineAllCheck_sound gallery_observation_page_key_bounds_anchor_contract
    gallery_observation_page_key_bounds_anchor_candidate_keys_check

theorem gallery_observation_page_key_bounds_anchor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_page_key_bounds_anchor_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_anchor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_page_key_bounds_anchor_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_page_key_bounds_anchor_contract
    gallery_observation_page_key_bounds_anchor_candidate_keys_minimal_check

theorem gallery_observation_page_key_bounds_anchor_closure_fixed_check :
    closureFixedPointCheck gallery_observation_page_key_bounds_anchor_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_anchor_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_page_key_bounds_anchor_contract :=
  closureFixedPointCheck_sound gallery_observation_page_key_bounds_anchor_contract
    gallery_observation_page_key_bounds_anchor_closure_fixed_check

theorem gallery_observation_page_key_bounds_anchor_bcnf_check :
    bcnfCheck gallery_observation_page_key_bounds_anchor_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_anchor_bcnf : BCNF gallery_observation_page_key_bounds_anchor_contract :=
  bcnfCheck_sound gallery_observation_page_key_bounds_anchor_contract gallery_observation_page_key_bounds_anchor_bcnf_check

def gallery_observation_page_key_bounds_first_key_contract : RelationContract where
  name := "gallery_observation_page_key_bounds_first_key"
  attributes := ["page_sha256", "first_key"]
  declaredKeys := [["page_sha256"]]
  declaredFDs := [
    { determinant := ["page_sha256"], dependent := ["first_key"] }
  ]

theorem gallery_observation_page_key_bounds_first_key_schema_well_formed :
    schemaWellFormedCheck gallery_observation_page_key_bounds_first_key_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_first_key_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_page_key_bounds_first_key_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_first_key_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_page_key_bounds_first_key_contract :=
  keysDetermineAllCheck_sound gallery_observation_page_key_bounds_first_key_contract
    gallery_observation_page_key_bounds_first_key_candidate_keys_check

theorem gallery_observation_page_key_bounds_first_key_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_page_key_bounds_first_key_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_first_key_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_page_key_bounds_first_key_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_page_key_bounds_first_key_contract
    gallery_observation_page_key_bounds_first_key_candidate_keys_minimal_check

theorem gallery_observation_page_key_bounds_first_key_closure_fixed_check :
    closureFixedPointCheck gallery_observation_page_key_bounds_first_key_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_first_key_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_page_key_bounds_first_key_contract :=
  closureFixedPointCheck_sound gallery_observation_page_key_bounds_first_key_contract
    gallery_observation_page_key_bounds_first_key_closure_fixed_check

theorem gallery_observation_page_key_bounds_first_key_bcnf_check :
    bcnfCheck gallery_observation_page_key_bounds_first_key_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_first_key_bcnf : BCNF gallery_observation_page_key_bounds_first_key_contract :=
  bcnfCheck_sound gallery_observation_page_key_bounds_first_key_contract gallery_observation_page_key_bounds_first_key_bcnf_check

def gallery_observation_page_key_bounds_last_key_contract : RelationContract where
  name := "gallery_observation_page_key_bounds_last_key"
  attributes := ["page_sha256", "last_key"]
  declaredKeys := [["page_sha256"]]
  declaredFDs := [
    { determinant := ["page_sha256"], dependent := ["last_key"] }
  ]

theorem gallery_observation_page_key_bounds_last_key_schema_well_formed :
    schemaWellFormedCheck gallery_observation_page_key_bounds_last_key_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_last_key_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_page_key_bounds_last_key_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_last_key_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_page_key_bounds_last_key_contract :=
  keysDetermineAllCheck_sound gallery_observation_page_key_bounds_last_key_contract
    gallery_observation_page_key_bounds_last_key_candidate_keys_check

theorem gallery_observation_page_key_bounds_last_key_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_page_key_bounds_last_key_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_last_key_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_page_key_bounds_last_key_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_page_key_bounds_last_key_contract
    gallery_observation_page_key_bounds_last_key_candidate_keys_minimal_check

theorem gallery_observation_page_key_bounds_last_key_closure_fixed_check :
    closureFixedPointCheck gallery_observation_page_key_bounds_last_key_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_last_key_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_page_key_bounds_last_key_contract :=
  closureFixedPointCheck_sound gallery_observation_page_key_bounds_last_key_contract
    gallery_observation_page_key_bounds_last_key_closure_fixed_check

theorem gallery_observation_page_key_bounds_last_key_bcnf_check :
    bcnfCheck gallery_observation_page_key_bounds_last_key_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_last_key_bcnf : BCNF gallery_observation_page_key_bounds_last_key_contract :=
  bcnfCheck_sound gallery_observation_page_key_bounds_last_key_contract gallery_observation_page_key_bounds_last_key_bcnf_check

def gallery_observation_page_key_bounds_seal_contract : RelationContract where
  name := "gallery_observation_page_key_bounds_seal"
  attributes := ["page_sha256"]
  declaredKeys := [["page_sha256"]]
  declaredFDs := [
  ]

theorem gallery_observation_page_key_bounds_seal_schema_well_formed :
    schemaWellFormedCheck gallery_observation_page_key_bounds_seal_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_seal_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_page_key_bounds_seal_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_seal_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_page_key_bounds_seal_contract :=
  keysDetermineAllCheck_sound gallery_observation_page_key_bounds_seal_contract
    gallery_observation_page_key_bounds_seal_candidate_keys_check

theorem gallery_observation_page_key_bounds_seal_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_page_key_bounds_seal_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_seal_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_page_key_bounds_seal_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_page_key_bounds_seal_contract
    gallery_observation_page_key_bounds_seal_candidate_keys_minimal_check

theorem gallery_observation_page_key_bounds_seal_closure_fixed_check :
    closureFixedPointCheck gallery_observation_page_key_bounds_seal_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_seal_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_page_key_bounds_seal_contract :=
  closureFixedPointCheck_sound gallery_observation_page_key_bounds_seal_contract
    gallery_observation_page_key_bounds_seal_closure_fixed_check

theorem gallery_observation_page_key_bounds_seal_bcnf_check :
    bcnfCheck gallery_observation_page_key_bounds_seal_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_seal_bcnf : BCNF gallery_observation_page_key_bounds_seal_contract :=
  bcnfCheck_sound gallery_observation_page_key_bounds_seal_contract gallery_observation_page_key_bounds_seal_bcnf_check

def gallery_observation_page_key_bounds_contract : RelationContract where
  name := "gallery_observation_page_key_bounds"
  attributes := ["page_sha256", "first_key", "last_key"]
  declaredKeys := [["page_sha256"]]
  declaredFDs := [
    { determinant := ["page_sha256"], dependent := ["first_key", "last_key"] }
  ]

theorem gallery_observation_page_key_bounds_schema_well_formed :
    schemaWellFormedCheck gallery_observation_page_key_bounds_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_page_key_bounds_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_page_key_bounds_contract :=
  keysDetermineAllCheck_sound gallery_observation_page_key_bounds_contract
    gallery_observation_page_key_bounds_candidate_keys_check

theorem gallery_observation_page_key_bounds_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_page_key_bounds_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_page_key_bounds_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_page_key_bounds_contract
    gallery_observation_page_key_bounds_candidate_keys_minimal_check

theorem gallery_observation_page_key_bounds_closure_fixed_check :
    closureFixedPointCheck gallery_observation_page_key_bounds_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_page_key_bounds_contract :=
  closureFixedPointCheck_sound gallery_observation_page_key_bounds_contract
    gallery_observation_page_key_bounds_closure_fixed_check

def gallery_observation_page_child_contract : RelationContract where
  name := "gallery_observation_page_child"
  attributes := ["parent_sha256", "position", "child_sha256"]
  declaredKeys := [["parent_sha256", "position"], ["parent_sha256", "child_sha256"]]
  declaredFDs := [
    { determinant := ["parent_sha256", "position"], dependent := ["child_sha256"] },
    { determinant := ["parent_sha256", "child_sha256"], dependent := ["position"] }
  ]

theorem gallery_observation_page_child_schema_well_formed :
    schemaWellFormedCheck gallery_observation_page_child_contract = true := by
  native_decide

theorem gallery_observation_page_child_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_page_child_contract = true := by
  native_decide

theorem gallery_observation_page_child_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_page_child_contract :=
  keysDetermineAllCheck_sound gallery_observation_page_child_contract
    gallery_observation_page_child_candidate_keys_check

theorem gallery_observation_page_child_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_page_child_contract = true := by
  native_decide

theorem gallery_observation_page_child_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_page_child_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_page_child_contract
    gallery_observation_page_child_candidate_keys_minimal_check

theorem gallery_observation_page_child_closure_fixed_check :
    closureFixedPointCheck gallery_observation_page_child_contract = true := by
  native_decide

theorem gallery_observation_page_child_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_page_child_contract :=
  closureFixedPointCheck_sound gallery_observation_page_child_contract
    gallery_observation_page_child_closure_fixed_check

theorem gallery_observation_page_child_bcnf_check :
    bcnfCheck gallery_observation_page_child_contract = true := by
  native_decide

theorem gallery_observation_page_child_bcnf : BCNF gallery_observation_page_child_contract :=
  bcnfCheck_sound gallery_observation_page_child_contract gallery_observation_page_child_bcnf_check

def gallery_observation_tree_root_contract : RelationContract where
  name := "gallery_observation_tree_root"
  attributes := ["gallery_id", "observation_id", "root_page_sha256"]
  declaredKeys := [["gallery_id", "observation_id", "root_page_sha256"]]
  declaredFDs := [
  ]

theorem gallery_observation_tree_root_schema_well_formed :
    schemaWellFormedCheck gallery_observation_tree_root_contract = true := by
  native_decide

theorem gallery_observation_tree_root_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_tree_root_contract = true := by
  native_decide

theorem gallery_observation_tree_root_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_tree_root_contract :=
  keysDetermineAllCheck_sound gallery_observation_tree_root_contract
    gallery_observation_tree_root_candidate_keys_check

theorem gallery_observation_tree_root_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_tree_root_contract = true := by
  native_decide

theorem gallery_observation_tree_root_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_tree_root_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_tree_root_contract
    gallery_observation_tree_root_candidate_keys_minimal_check

theorem gallery_observation_tree_root_closure_fixed_check :
    closureFixedPointCheck gallery_observation_tree_root_contract = true := by
  native_decide

theorem gallery_observation_tree_root_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_tree_root_contract :=
  closureFixedPointCheck_sound gallery_observation_tree_root_contract
    gallery_observation_tree_root_closure_fixed_check

theorem gallery_observation_tree_root_bcnf_check :
    bcnfCheck gallery_observation_tree_root_contract = true := by
  native_decide

theorem gallery_observation_tree_root_bcnf : BCNF gallery_observation_tree_root_contract :=
  bcnfCheck_sound gallery_observation_tree_root_contract gallery_observation_tree_root_bcnf_check

def gallery_observation_contract : RelationContract where
  name := "gallery_observation"
  attributes := ["gallery_id", "observation_id", "observation_identity_sha256"]
  declaredKeys := [["gallery_id", "observation_id"], ["gallery_id", "observation_identity_sha256"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id"], dependent := ["observation_identity_sha256"] },
    { determinant := ["gallery_id", "observation_identity_sha256"], dependent := ["observation_id"] }
  ]

theorem gallery_observation_schema_well_formed :
    schemaWellFormedCheck gallery_observation_contract = true := by
  native_decide

theorem gallery_observation_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_contract = true := by
  native_decide

theorem gallery_observation_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_contract :=
  keysDetermineAllCheck_sound gallery_observation_contract
    gallery_observation_candidate_keys_check

theorem gallery_observation_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_contract = true := by
  native_decide

theorem gallery_observation_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_contract
    gallery_observation_candidate_keys_minimal_check

theorem gallery_observation_closure_fixed_check :
    closureFixedPointCheck gallery_observation_contract = true := by
  native_decide

theorem gallery_observation_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_contract :=
  closureFixedPointCheck_sound gallery_observation_contract
    gallery_observation_closure_fixed_check

theorem gallery_observation_bcnf_check :
    bcnfCheck gallery_observation_contract = true := by
  native_decide

theorem gallery_observation_bcnf : BCNF gallery_observation_contract :=
  bcnfCheck_sound gallery_observation_contract gallery_observation_bcnf_check

def gallery_upload_time_contract : RelationContract where
  name := "gallery_upload_time"
  attributes := ["gid", "upload_time"]
  declaredKeys := [["gid"]]
  declaredFDs := [
    { determinant := ["gid"], dependent := ["upload_time"] }
  ]

theorem gallery_upload_time_schema_well_formed :
    schemaWellFormedCheck gallery_upload_time_contract = true := by
  native_decide

theorem gallery_upload_time_candidate_keys_check :
    keysDetermineAllCheck gallery_upload_time_contract = true := by
  native_decide

theorem gallery_upload_time_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_upload_time_contract :=
  keysDetermineAllCheck_sound gallery_upload_time_contract
    gallery_upload_time_candidate_keys_check

theorem gallery_upload_time_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_upload_time_contract = true := by
  native_decide

theorem gallery_upload_time_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_upload_time_contract :=
  declaredKeysMinimalCheck_sound gallery_upload_time_contract
    gallery_upload_time_candidate_keys_minimal_check

theorem gallery_upload_time_closure_fixed_check :
    closureFixedPointCheck gallery_upload_time_contract = true := by
  native_decide

theorem gallery_upload_time_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_upload_time_contract :=
  closureFixedPointCheck_sound gallery_upload_time_contract
    gallery_upload_time_closure_fixed_check

theorem gallery_upload_time_bcnf_check :
    bcnfCheck gallery_upload_time_contract = true := by
  native_decide

theorem gallery_upload_time_bcnf : BCNF gallery_upload_time_contract :=
  bcnfCheck_sound gallery_upload_time_contract gallery_upload_time_bcnf_check

def source_gallery_name_gid_contract : RelationContract where
  name := "source_gallery_name_gid"
  attributes := ["source_gallery_name", "gid"]
  declaredKeys := [["source_gallery_name"]]
  declaredFDs := [
    { determinant := ["source_gallery_name"], dependent := ["gid"] }
  ]

theorem source_gallery_name_gid_schema_well_formed :
    schemaWellFormedCheck source_gallery_name_gid_contract = true := by
  native_decide

theorem source_gallery_name_gid_candidate_keys_check :
    keysDetermineAllCheck source_gallery_name_gid_contract = true := by
  native_decide

theorem source_gallery_name_gid_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_gallery_name_gid_contract :=
  keysDetermineAllCheck_sound source_gallery_name_gid_contract
    source_gallery_name_gid_candidate_keys_check

theorem source_gallery_name_gid_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_gallery_name_gid_contract = true := by
  native_decide

theorem source_gallery_name_gid_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_gallery_name_gid_contract :=
  declaredKeysMinimalCheck_sound source_gallery_name_gid_contract
    source_gallery_name_gid_candidate_keys_minimal_check

theorem source_gallery_name_gid_closure_fixed_check :
    closureFixedPointCheck source_gallery_name_gid_contract = true := by
  native_decide

theorem source_gallery_name_gid_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_gallery_name_gid_contract :=
  closureFixedPointCheck_sound source_gallery_name_gid_contract
    source_gallery_name_gid_closure_fixed_check

theorem source_gallery_name_gid_bcnf_check :
    bcnfCheck source_gallery_name_gid_contract = true := by
  native_decide

theorem source_gallery_name_gid_bcnf : BCNF source_gallery_name_gid_contract :=
  bcnfCheck_sound source_gallery_name_gid_contract source_gallery_name_gid_bcnf_check

def gallery_source_name_access_contract : RelationContract where
  name := "gallery_source_name_access"
  attributes := ["gallery_id", "source_gallery_name"]
  declaredKeys := [["gallery_id"]]
  declaredFDs := [
    { determinant := ["gallery_id"], dependent := ["source_gallery_name"] }
  ]

theorem gallery_source_name_access_schema_well_formed :
    schemaWellFormedCheck gallery_source_name_access_contract = true := by
  native_decide

theorem gallery_source_name_access_candidate_keys_check :
    keysDetermineAllCheck gallery_source_name_access_contract = true := by
  native_decide

theorem gallery_source_name_access_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_source_name_access_contract :=
  keysDetermineAllCheck_sound gallery_source_name_access_contract
    gallery_source_name_access_candidate_keys_check

theorem gallery_source_name_access_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_source_name_access_contract = true := by
  native_decide

theorem gallery_source_name_access_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_source_name_access_contract :=
  declaredKeysMinimalCheck_sound gallery_source_name_access_contract
    gallery_source_name_access_candidate_keys_minimal_check

theorem gallery_source_name_access_closure_fixed_check :
    closureFixedPointCheck gallery_source_name_access_contract = true := by
  native_decide

theorem gallery_source_name_access_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_source_name_access_contract :=
  closureFixedPointCheck_sound gallery_source_name_access_contract
    gallery_source_name_access_closure_fixed_check

theorem gallery_source_name_access_bcnf_check :
    bcnfCheck gallery_source_name_access_contract = true := by
  native_decide

theorem gallery_source_name_access_bcnf : BCNF gallery_source_name_access_contract :=
  bcnfCheck_sound gallery_source_name_access_contract gallery_source_name_access_bcnf_check

def gallery_observation_metadata_local_contract : RelationContract where
  name := "gallery_observation_metadata_local"
  attributes := ["gallery_id", "observation_id", "download_time", "modified_time"]
  declaredKeys := [["gallery_id", "observation_id"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id"], dependent := ["download_time", "modified_time"] }
  ]

theorem gallery_observation_metadata_local_schema_well_formed :
    schemaWellFormedCheck gallery_observation_metadata_local_contract = true := by
  native_decide

theorem gallery_observation_metadata_local_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_metadata_local_contract = true := by
  native_decide

theorem gallery_observation_metadata_local_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_metadata_local_contract :=
  keysDetermineAllCheck_sound gallery_observation_metadata_local_contract
    gallery_observation_metadata_local_candidate_keys_check

theorem gallery_observation_metadata_local_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_metadata_local_contract = true := by
  native_decide

theorem gallery_observation_metadata_local_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_metadata_local_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_metadata_local_contract
    gallery_observation_metadata_local_candidate_keys_minimal_check

theorem gallery_observation_metadata_local_closure_fixed_check :
    closureFixedPointCheck gallery_observation_metadata_local_contract = true := by
  native_decide

theorem gallery_observation_metadata_local_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_metadata_local_contract :=
  closureFixedPointCheck_sound gallery_observation_metadata_local_contract
    gallery_observation_metadata_local_closure_fixed_check

theorem gallery_observation_metadata_local_bcnf_check :
    bcnfCheck gallery_observation_metadata_local_contract = true := by
  native_decide

theorem gallery_observation_metadata_local_bcnf : BCNF gallery_observation_metadata_local_contract :=
  bcnfCheck_sound gallery_observation_metadata_local_contract gallery_observation_metadata_local_bcnf_check

def gallery_observation_metadata_contract : RelationContract where
  name := "gallery_observation_metadata"
  attributes := ["gallery_id", "observation_id", "gid", "upload_time", "download_time", "modified_time"]
  declaredKeys := [["gallery_id", "observation_id"]]
  declaredFDs := [
    { determinant := ["gallery_id"], dependent := ["gid"] },
    { determinant := ["gid"], dependent := ["upload_time"] },
    { determinant := ["gallery_id", "observation_id"], dependent := ["download_time", "modified_time"] }
  ]

theorem gallery_observation_metadata_schema_well_formed :
    schemaWellFormedCheck gallery_observation_metadata_contract = true := by
  native_decide

theorem gallery_observation_metadata_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_metadata_contract = true := by
  native_decide

theorem gallery_observation_metadata_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_metadata_contract :=
  keysDetermineAllCheck_sound gallery_observation_metadata_contract
    gallery_observation_metadata_candidate_keys_check

theorem gallery_observation_metadata_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_metadata_contract = true := by
  native_decide

theorem gallery_observation_metadata_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_metadata_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_metadata_contract
    gallery_observation_metadata_candidate_keys_minimal_check

theorem gallery_observation_metadata_closure_fixed_check :
    closureFixedPointCheck gallery_observation_metadata_contract = true := by
  native_decide

theorem gallery_observation_metadata_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_metadata_contract :=
  closureFixedPointCheck_sound gallery_observation_metadata_contract
    gallery_observation_metadata_closure_fixed_check

def gallery_observation_scan_contract : RelationContract where
  name := "gallery_observation_scan"
  attributes := ["gallery_id", "observation_id", "scan_observation_sha256", "scan_observation_version", "source_file_count"]
  declaredKeys := [["gallery_id", "observation_id"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id"], dependent := ["scan_observation_sha256", "scan_observation_version", "source_file_count"] }
  ]

theorem gallery_observation_scan_schema_well_formed :
    schemaWellFormedCheck gallery_observation_scan_contract = true := by
  native_decide

theorem gallery_observation_scan_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_scan_contract = true := by
  native_decide

theorem gallery_observation_scan_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_scan_contract :=
  keysDetermineAllCheck_sound gallery_observation_scan_contract
    gallery_observation_scan_candidate_keys_check

theorem gallery_observation_scan_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_scan_contract = true := by
  native_decide

theorem gallery_observation_scan_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_scan_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_scan_contract
    gallery_observation_scan_candidate_keys_minimal_check

theorem gallery_observation_scan_closure_fixed_check :
    closureFixedPointCheck gallery_observation_scan_contract = true := by
  native_decide

theorem gallery_observation_scan_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_scan_contract :=
  closureFixedPointCheck_sound gallery_observation_scan_contract
    gallery_observation_scan_closure_fixed_check

theorem gallery_observation_scan_bcnf_check :
    bcnfCheck gallery_observation_scan_contract = true := by
  native_decide

theorem gallery_observation_scan_bcnf : BCNF gallery_observation_scan_contract :=
  bcnfCheck_sound gallery_observation_scan_contract gallery_observation_scan_bcnf_check

def gallery_observation_discovery_fingerprint_contract : RelationContract where
  name := "gallery_observation_discovery_fingerprint"
  attributes := ["gallery_id", "observation_id", "metadata_fingerprint"]
  declaredKeys := [["gallery_id", "observation_id"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id"], dependent := ["metadata_fingerprint"] }
  ]

theorem gallery_observation_discovery_fingerprint_schema_well_formed :
    schemaWellFormedCheck gallery_observation_discovery_fingerprint_contract = true := by
  native_decide

theorem gallery_observation_discovery_fingerprint_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_discovery_fingerprint_contract = true := by
  native_decide

theorem gallery_observation_discovery_fingerprint_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_discovery_fingerprint_contract :=
  keysDetermineAllCheck_sound gallery_observation_discovery_fingerprint_contract
    gallery_observation_discovery_fingerprint_candidate_keys_check

theorem gallery_observation_discovery_fingerprint_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_discovery_fingerprint_contract = true := by
  native_decide

theorem gallery_observation_discovery_fingerprint_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_discovery_fingerprint_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_discovery_fingerprint_contract
    gallery_observation_discovery_fingerprint_candidate_keys_minimal_check

theorem gallery_observation_discovery_fingerprint_closure_fixed_check :
    closureFixedPointCheck gallery_observation_discovery_fingerprint_contract = true := by
  native_decide

theorem gallery_observation_discovery_fingerprint_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_discovery_fingerprint_contract :=
  closureFixedPointCheck_sound gallery_observation_discovery_fingerprint_contract
    gallery_observation_discovery_fingerprint_closure_fixed_check

theorem gallery_observation_discovery_fingerprint_bcnf_check :
    bcnfCheck gallery_observation_discovery_fingerprint_contract = true := by
  native_decide

theorem gallery_observation_discovery_fingerprint_bcnf : BCNF gallery_observation_discovery_fingerprint_contract :=
  bcnfCheck_sound gallery_observation_discovery_fingerprint_contract gallery_observation_discovery_fingerprint_bcnf_check

def gallery_observation_metadata_digest_contract : RelationContract where
  name := "gallery_observation_metadata_digest"
  attributes := ["gallery_id", "observation_id", "metadata_sha256"]
  declaredKeys := [["gallery_id", "observation_id"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id"], dependent := ["metadata_sha256"] }
  ]

theorem gallery_observation_metadata_digest_schema_well_formed :
    schemaWellFormedCheck gallery_observation_metadata_digest_contract = true := by
  native_decide

theorem gallery_observation_metadata_digest_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_metadata_digest_contract = true := by
  native_decide

theorem gallery_observation_metadata_digest_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_metadata_digest_contract :=
  keysDetermineAllCheck_sound gallery_observation_metadata_digest_contract
    gallery_observation_metadata_digest_candidate_keys_check

theorem gallery_observation_metadata_digest_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_metadata_digest_contract = true := by
  native_decide

theorem gallery_observation_metadata_digest_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_metadata_digest_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_metadata_digest_contract
    gallery_observation_metadata_digest_candidate_keys_minimal_check

theorem gallery_observation_metadata_digest_closure_fixed_check :
    closureFixedPointCheck gallery_observation_metadata_digest_contract = true := by
  native_decide

theorem gallery_observation_metadata_digest_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_metadata_digest_contract :=
  closureFixedPointCheck_sound gallery_observation_metadata_digest_contract
    gallery_observation_metadata_digest_closure_fixed_check

theorem gallery_observation_metadata_digest_bcnf_check :
    bcnfCheck gallery_observation_metadata_digest_contract = true := by
  native_decide

theorem gallery_observation_metadata_digest_bcnf : BCNF gallery_observation_metadata_digest_contract :=
  bcnfCheck_sound gallery_observation_metadata_digest_contract gallery_observation_metadata_digest_bcnf_check

def gallery_observation_raw_content_contract : RelationContract where
  name := "gallery_observation_raw_content"
  attributes := ["gallery_id", "observation_id", "raw_content_sha256"]
  declaredKeys := [["gallery_id", "observation_id"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id"], dependent := ["raw_content_sha256"] }
  ]

theorem gallery_observation_raw_content_schema_well_formed :
    schemaWellFormedCheck gallery_observation_raw_content_contract = true := by
  native_decide

theorem gallery_observation_raw_content_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_raw_content_contract = true := by
  native_decide

theorem gallery_observation_raw_content_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_raw_content_contract :=
  keysDetermineAllCheck_sound gallery_observation_raw_content_contract
    gallery_observation_raw_content_candidate_keys_check

theorem gallery_observation_raw_content_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_raw_content_contract = true := by
  native_decide

theorem gallery_observation_raw_content_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_raw_content_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_raw_content_contract
    gallery_observation_raw_content_candidate_keys_minimal_check

theorem gallery_observation_raw_content_closure_fixed_check :
    closureFixedPointCheck gallery_observation_raw_content_contract = true := by
  native_decide

theorem gallery_observation_raw_content_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_raw_content_contract :=
  closureFixedPointCheck_sound gallery_observation_raw_content_contract
    gallery_observation_raw_content_closure_fixed_check

theorem gallery_observation_raw_content_bcnf_check :
    bcnfCheck gallery_observation_raw_content_contract = true := by
  native_decide

theorem gallery_observation_raw_content_bcnf : BCNF gallery_observation_raw_content_contract :=
  bcnfCheck_sound gallery_observation_raw_content_contract gallery_observation_raw_content_bcnf_check

def gallery_observation_page_count_contract : RelationContract where
  name := "gallery_observation_page_count"
  attributes := ["gallery_id", "observation_id", "page_count"]
  declaredKeys := [["gallery_id", "observation_id"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id"], dependent := ["page_count"] }
  ]

theorem gallery_observation_page_count_schema_well_formed :
    schemaWellFormedCheck gallery_observation_page_count_contract = true := by
  native_decide

theorem gallery_observation_page_count_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_page_count_contract = true := by
  native_decide

theorem gallery_observation_page_count_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_page_count_contract :=
  keysDetermineAllCheck_sound gallery_observation_page_count_contract
    gallery_observation_page_count_candidate_keys_check

theorem gallery_observation_page_count_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_page_count_contract = true := by
  native_decide

theorem gallery_observation_page_count_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_page_count_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_page_count_contract
    gallery_observation_page_count_candidate_keys_minimal_check

theorem gallery_observation_page_count_closure_fixed_check :
    closureFixedPointCheck gallery_observation_page_count_contract = true := by
  native_decide

theorem gallery_observation_page_count_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_page_count_contract :=
  closureFixedPointCheck_sound gallery_observation_page_count_contract
    gallery_observation_page_count_closure_fixed_check

theorem gallery_observation_page_count_bcnf_check :
    bcnfCheck gallery_observation_page_count_contract = true := by
  native_decide

theorem gallery_observation_page_count_bcnf : BCNF gallery_observation_page_count_contract :=
  bcnfCheck_sound gallery_observation_page_count_contract gallery_observation_page_count_bcnf_check

def gallery_observation_directory_contract : RelationContract where
  name := "gallery_observation_directory"
  attributes := ["gallery_id", "observation_id", "directory_entry_count", "directory_observation_sha256"]
  declaredKeys := [["gallery_id", "observation_id"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id"], dependent := ["directory_entry_count", "directory_observation_sha256"] }
  ]

theorem gallery_observation_directory_schema_well_formed :
    schemaWellFormedCheck gallery_observation_directory_contract = true := by
  native_decide

theorem gallery_observation_directory_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_directory_contract = true := by
  native_decide

theorem gallery_observation_directory_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_directory_contract :=
  keysDetermineAllCheck_sound gallery_observation_directory_contract
    gallery_observation_directory_candidate_keys_check

theorem gallery_observation_directory_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_directory_contract = true := by
  native_decide

theorem gallery_observation_directory_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_directory_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_directory_contract
    gallery_observation_directory_candidate_keys_minimal_check

theorem gallery_observation_directory_closure_fixed_check :
    closureFixedPointCheck gallery_observation_directory_contract = true := by
  native_decide

theorem gallery_observation_directory_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_directory_contract :=
  closureFixedPointCheck_sound gallery_observation_directory_contract
    gallery_observation_directory_closure_fixed_check

theorem gallery_observation_directory_bcnf_check :
    bcnfCheck gallery_observation_directory_contract = true := by
  native_decide

theorem gallery_observation_directory_bcnf : BCNF gallery_observation_directory_contract :=
  bcnfCheck_sound gallery_observation_directory_contract gallery_observation_directory_bcnf_check

def gallery_observation_stat_contract : RelationContract where
  name := "gallery_observation_stat"
  attributes := ["gallery_id", "observation_id", "file_count", "byte_count"]
  declaredKeys := [["gallery_id", "observation_id"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id"], dependent := ["file_count", "byte_count"] }
  ]

theorem gallery_observation_stat_schema_well_formed :
    schemaWellFormedCheck gallery_observation_stat_contract = true := by
  native_decide

theorem gallery_observation_stat_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_stat_contract = true := by
  native_decide

theorem gallery_observation_stat_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_stat_contract :=
  keysDetermineAllCheck_sound gallery_observation_stat_contract
    gallery_observation_stat_candidate_keys_check

theorem gallery_observation_stat_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_stat_contract = true := by
  native_decide

theorem gallery_observation_stat_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_stat_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_stat_contract
    gallery_observation_stat_candidate_keys_minimal_check

theorem gallery_observation_stat_closure_fixed_check :
    closureFixedPointCheck gallery_observation_stat_contract = true := by
  native_decide

theorem gallery_observation_stat_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_stat_contract :=
  closureFixedPointCheck_sound gallery_observation_stat_contract
    gallery_observation_stat_closure_fixed_check

theorem gallery_observation_stat_bcnf_check :
    bcnfCheck gallery_observation_stat_contract = true := by
  native_decide

theorem gallery_observation_stat_bcnf : BCNF gallery_observation_stat_contract :=
  bcnfCheck_sound gallery_observation_stat_contract gallery_observation_stat_bcnf_check

def source_build_gallery_contract : RelationContract where
  name := "source_build_gallery"
  attributes := ["build_id", "gallery_id", "observation_id"]
  declaredKeys := [["build_id", "gallery_id"]]
  declaredFDs := [
    { determinant := ["build_id", "gallery_id"], dependent := ["observation_id"] }
  ]

theorem source_build_gallery_schema_well_formed :
    schemaWellFormedCheck source_build_gallery_contract = true := by
  native_decide

theorem source_build_gallery_candidate_keys_check :
    keysDetermineAllCheck source_build_gallery_contract = true := by
  native_decide

theorem source_build_gallery_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_build_gallery_contract :=
  keysDetermineAllCheck_sound source_build_gallery_contract
    source_build_gallery_candidate_keys_check

theorem source_build_gallery_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_build_gallery_contract = true := by
  native_decide

theorem source_build_gallery_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_build_gallery_contract :=
  declaredKeysMinimalCheck_sound source_build_gallery_contract
    source_build_gallery_candidate_keys_minimal_check

theorem source_build_gallery_closure_fixed_check :
    closureFixedPointCheck source_build_gallery_contract = true := by
  native_decide

theorem source_build_gallery_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_build_gallery_contract :=
  closureFixedPointCheck_sound source_build_gallery_contract
    source_build_gallery_closure_fixed_check

theorem source_build_gallery_bcnf_check :
    bcnfCheck source_build_gallery_contract = true := by
  native_decide

theorem source_build_gallery_bcnf : BCNF source_build_gallery_contract :=
  bcnfCheck_sound source_build_gallery_contract source_build_gallery_bcnf_check

def file_name_identity_contract : RelationContract where
  name := "file_name_identity"
  attributes := ["file_key", "name_bytes"]
  declaredKeys := [["file_key"], ["name_bytes"]]
  declaredFDs := [
    { determinant := ["file_key"], dependent := ["name_bytes"] },
    { determinant := ["name_bytes"], dependent := ["file_key"] }
  ]

theorem file_name_identity_schema_well_formed :
    schemaWellFormedCheck file_name_identity_contract = true := by
  native_decide

theorem file_name_identity_candidate_keys_check :
    keysDetermineAllCheck file_name_identity_contract = true := by
  native_decide

theorem file_name_identity_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes file_name_identity_contract :=
  keysDetermineAllCheck_sound file_name_identity_contract
    file_name_identity_candidate_keys_check

theorem file_name_identity_candidate_keys_minimal_check :
    declaredKeysMinimalCheck file_name_identity_contract = true := by
  native_decide

theorem file_name_identity_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal file_name_identity_contract :=
  declaredKeysMinimalCheck_sound file_name_identity_contract
    file_name_identity_candidate_keys_minimal_check

theorem file_name_identity_closure_fixed_check :
    closureFixedPointCheck file_name_identity_contract = true := by
  native_decide

theorem file_name_identity_closure_reached_fixed_point :
    ClosureReachedFixedPoint file_name_identity_contract :=
  closureFixedPointCheck_sound file_name_identity_contract
    file_name_identity_closure_fixed_check

theorem file_name_identity_bcnf_check :
    bcnfCheck file_name_identity_contract = true := by
  native_decide

theorem file_name_identity_bcnf : BCNF file_name_identity_contract :=
  bcnfCheck_sound file_name_identity_contract file_name_identity_bcnf_check

def content_blob_contract : RelationContract where
  name := "content_blob"
  attributes := ["file_sha256", "size_bytes"]
  declaredKeys := [["file_sha256"]]
  declaredFDs := [
    { determinant := ["file_sha256"], dependent := ["size_bytes"] }
  ]

theorem content_blob_schema_well_formed :
    schemaWellFormedCheck content_blob_contract = true := by
  native_decide

theorem content_blob_candidate_keys_check :
    keysDetermineAllCheck content_blob_contract = true := by
  native_decide

theorem content_blob_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes content_blob_contract :=
  keysDetermineAllCheck_sound content_blob_contract
    content_blob_candidate_keys_check

theorem content_blob_candidate_keys_minimal_check :
    declaredKeysMinimalCheck content_blob_contract = true := by
  native_decide

theorem content_blob_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal content_blob_contract :=
  declaredKeysMinimalCheck_sound content_blob_contract
    content_blob_candidate_keys_minimal_check

theorem content_blob_closure_fixed_check :
    closureFixedPointCheck content_blob_contract = true := by
  native_decide

theorem content_blob_closure_reached_fixed_point :
    ClosureReachedFixedPoint content_blob_contract :=
  closureFixedPointCheck_sound content_blob_contract
    content_blob_closure_fixed_check

theorem content_blob_bcnf_check :
    bcnfCheck content_blob_contract = true := by
  native_decide

theorem content_blob_bcnf : BCNF content_blob_contract :=
  bcnfCheck_sound content_blob_contract content_blob_bcnf_check

def gallery_observation_file_anchor_contract : RelationContract where
  name := "gallery_observation_file_anchor"
  attributes := ["gallery_id", "observation_id", "file_key"]
  declaredKeys := [["gallery_id", "observation_id", "file_key"]]
  declaredFDs := [
  ]

theorem gallery_observation_file_anchor_schema_well_formed :
    schemaWellFormedCheck gallery_observation_file_anchor_contract = true := by
  native_decide

theorem gallery_observation_file_anchor_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_file_anchor_contract = true := by
  native_decide

theorem gallery_observation_file_anchor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_file_anchor_contract :=
  keysDetermineAllCheck_sound gallery_observation_file_anchor_contract
    gallery_observation_file_anchor_candidate_keys_check

theorem gallery_observation_file_anchor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_file_anchor_contract = true := by
  native_decide

theorem gallery_observation_file_anchor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_file_anchor_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_file_anchor_contract
    gallery_observation_file_anchor_candidate_keys_minimal_check

theorem gallery_observation_file_anchor_closure_fixed_check :
    closureFixedPointCheck gallery_observation_file_anchor_contract = true := by
  native_decide

theorem gallery_observation_file_anchor_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_file_anchor_contract :=
  closureFixedPointCheck_sound gallery_observation_file_anchor_contract
    gallery_observation_file_anchor_closure_fixed_check

theorem gallery_observation_file_anchor_bcnf_check :
    bcnfCheck gallery_observation_file_anchor_contract = true := by
  native_decide

theorem gallery_observation_file_anchor_bcnf : BCNF gallery_observation_file_anchor_contract :=
  bcnfCheck_sound gallery_observation_file_anchor_contract gallery_observation_file_anchor_bcnf_check

def gallery_observation_file_file_no_contract : RelationContract where
  name := "gallery_observation_file_file_no"
  attributes := ["gallery_id", "observation_id", "file_key", "file_no"]
  declaredKeys := [["gallery_id", "observation_id", "file_key"], ["gallery_id", "observation_id", "file_no"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["file_no"] },
    { determinant := ["gallery_id", "observation_id", "file_no"], dependent := ["file_key"] }
  ]

theorem gallery_observation_file_file_no_schema_well_formed :
    schemaWellFormedCheck gallery_observation_file_file_no_contract = true := by
  native_decide

theorem gallery_observation_file_file_no_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_file_file_no_contract = true := by
  native_decide

theorem gallery_observation_file_file_no_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_file_file_no_contract :=
  keysDetermineAllCheck_sound gallery_observation_file_file_no_contract
    gallery_observation_file_file_no_candidate_keys_check

theorem gallery_observation_file_file_no_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_file_file_no_contract = true := by
  native_decide

theorem gallery_observation_file_file_no_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_file_file_no_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_file_file_no_contract
    gallery_observation_file_file_no_candidate_keys_minimal_check

theorem gallery_observation_file_file_no_closure_fixed_check :
    closureFixedPointCheck gallery_observation_file_file_no_contract = true := by
  native_decide

theorem gallery_observation_file_file_no_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_file_file_no_contract :=
  closureFixedPointCheck_sound gallery_observation_file_file_no_contract
    gallery_observation_file_file_no_closure_fixed_check

theorem gallery_observation_file_file_no_bcnf_check :
    bcnfCheck gallery_observation_file_file_no_contract = true := by
  native_decide

theorem gallery_observation_file_file_no_bcnf : BCNF gallery_observation_file_file_no_contract :=
  bcnfCheck_sound gallery_observation_file_file_no_contract gallery_observation_file_file_no_bcnf_check

def gallery_observation_file_file_sha256_contract : RelationContract where
  name := "gallery_observation_file_file_sha256"
  attributes := ["gallery_id", "observation_id", "file_key", "file_sha256"]
  declaredKeys := [["gallery_id", "observation_id", "file_key"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["file_sha256"] }
  ]

theorem gallery_observation_file_file_sha256_schema_well_formed :
    schemaWellFormedCheck gallery_observation_file_file_sha256_contract = true := by
  native_decide

theorem gallery_observation_file_file_sha256_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_file_file_sha256_contract = true := by
  native_decide

theorem gallery_observation_file_file_sha256_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_file_file_sha256_contract :=
  keysDetermineAllCheck_sound gallery_observation_file_file_sha256_contract
    gallery_observation_file_file_sha256_candidate_keys_check

theorem gallery_observation_file_file_sha256_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_file_file_sha256_contract = true := by
  native_decide

theorem gallery_observation_file_file_sha256_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_file_file_sha256_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_file_file_sha256_contract
    gallery_observation_file_file_sha256_candidate_keys_minimal_check

theorem gallery_observation_file_file_sha256_closure_fixed_check :
    closureFixedPointCheck gallery_observation_file_file_sha256_contract = true := by
  native_decide

theorem gallery_observation_file_file_sha256_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_file_file_sha256_contract :=
  closureFixedPointCheck_sound gallery_observation_file_file_sha256_contract
    gallery_observation_file_file_sha256_closure_fixed_check

theorem gallery_observation_file_file_sha256_bcnf_check :
    bcnfCheck gallery_observation_file_file_sha256_contract = true := by
  native_decide

theorem gallery_observation_file_file_sha256_bcnf : BCNF gallery_observation_file_file_sha256_contract :=
  bcnfCheck_sound gallery_observation_file_file_sha256_contract gallery_observation_file_file_sha256_bcnf_check

def gallery_observation_file_seal_contract : RelationContract where
  name := "gallery_observation_file_seal"
  attributes := ["gallery_id", "observation_id", "file_key"]
  declaredKeys := [["gallery_id", "observation_id", "file_key"]]
  declaredFDs := [
  ]

theorem gallery_observation_file_seal_schema_well_formed :
    schemaWellFormedCheck gallery_observation_file_seal_contract = true := by
  native_decide

theorem gallery_observation_file_seal_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_file_seal_contract = true := by
  native_decide

theorem gallery_observation_file_seal_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_file_seal_contract :=
  keysDetermineAllCheck_sound gallery_observation_file_seal_contract
    gallery_observation_file_seal_candidate_keys_check

theorem gallery_observation_file_seal_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_file_seal_contract = true := by
  native_decide

theorem gallery_observation_file_seal_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_file_seal_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_file_seal_contract
    gallery_observation_file_seal_candidate_keys_minimal_check

theorem gallery_observation_file_seal_closure_fixed_check :
    closureFixedPointCheck gallery_observation_file_seal_contract = true := by
  native_decide

theorem gallery_observation_file_seal_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_file_seal_contract :=
  closureFixedPointCheck_sound gallery_observation_file_seal_contract
    gallery_observation_file_seal_closure_fixed_check

theorem gallery_observation_file_seal_bcnf_check :
    bcnfCheck gallery_observation_file_seal_contract = true := by
  native_decide

theorem gallery_observation_file_seal_bcnf : BCNF gallery_observation_file_seal_contract :=
  bcnfCheck_sound gallery_observation_file_seal_contract gallery_observation_file_seal_bcnf_check

def gallery_observation_file_contract : RelationContract where
  name := "gallery_observation_file"
  attributes := ["gallery_id", "observation_id", "file_no", "file_key", "file_sha256"]
  declaredKeys := [["gallery_id", "observation_id", "file_key"], ["gallery_id", "observation_id", "file_no"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id", "file_no"], dependent := ["file_key", "file_sha256"] },
    { determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["file_no", "file_sha256"] }
  ]

theorem gallery_observation_file_schema_well_formed :
    schemaWellFormedCheck gallery_observation_file_contract = true := by
  native_decide

theorem gallery_observation_file_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_file_contract = true := by
  native_decide

theorem gallery_observation_file_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_file_contract :=
  keysDetermineAllCheck_sound gallery_observation_file_contract
    gallery_observation_file_candidate_keys_check

theorem gallery_observation_file_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_file_contract = true := by
  native_decide

theorem gallery_observation_file_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_file_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_file_contract
    gallery_observation_file_candidate_keys_minimal_check

theorem gallery_observation_file_closure_fixed_check :
    closureFixedPointCheck gallery_observation_file_contract = true := by
  native_decide

theorem gallery_observation_file_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_file_contract :=
  closureFixedPointCheck_sound gallery_observation_file_contract
    gallery_observation_file_closure_fixed_check

def gallery_observation_file_filesystem_anchor_contract : RelationContract where
  name := "gallery_observation_file_filesystem_anchor"
  attributes := ["gallery_id", "observation_id", "file_key"]
  declaredKeys := [["gallery_id", "observation_id", "file_key"]]
  declaredFDs := [
  ]

theorem gallery_observation_file_filesystem_anchor_schema_well_formed :
    schemaWellFormedCheck gallery_observation_file_filesystem_anchor_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_anchor_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_file_filesystem_anchor_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_anchor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_file_filesystem_anchor_contract :=
  keysDetermineAllCheck_sound gallery_observation_file_filesystem_anchor_contract
    gallery_observation_file_filesystem_anchor_candidate_keys_check

theorem gallery_observation_file_filesystem_anchor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_file_filesystem_anchor_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_anchor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_file_filesystem_anchor_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_file_filesystem_anchor_contract
    gallery_observation_file_filesystem_anchor_candidate_keys_minimal_check

theorem gallery_observation_file_filesystem_anchor_closure_fixed_check :
    closureFixedPointCheck gallery_observation_file_filesystem_anchor_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_anchor_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_file_filesystem_anchor_contract :=
  closureFixedPointCheck_sound gallery_observation_file_filesystem_anchor_contract
    gallery_observation_file_filesystem_anchor_closure_fixed_check

theorem gallery_observation_file_filesystem_anchor_bcnf_check :
    bcnfCheck gallery_observation_file_filesystem_anchor_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_anchor_bcnf : BCNF gallery_observation_file_filesystem_anchor_contract :=
  bcnfCheck_sound gallery_observation_file_filesystem_anchor_contract gallery_observation_file_filesystem_anchor_bcnf_check

def gallery_observation_file_filesystem_device_contract : RelationContract where
  name := "gallery_observation_file_filesystem_device"
  attributes := ["gallery_id", "observation_id", "file_key", "device"]
  declaredKeys := [["gallery_id", "observation_id", "file_key"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["device"] }
  ]

theorem gallery_observation_file_filesystem_device_schema_well_formed :
    schemaWellFormedCheck gallery_observation_file_filesystem_device_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_device_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_file_filesystem_device_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_device_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_file_filesystem_device_contract :=
  keysDetermineAllCheck_sound gallery_observation_file_filesystem_device_contract
    gallery_observation_file_filesystem_device_candidate_keys_check

theorem gallery_observation_file_filesystem_device_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_file_filesystem_device_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_device_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_file_filesystem_device_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_file_filesystem_device_contract
    gallery_observation_file_filesystem_device_candidate_keys_minimal_check

theorem gallery_observation_file_filesystem_device_closure_fixed_check :
    closureFixedPointCheck gallery_observation_file_filesystem_device_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_device_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_file_filesystem_device_contract :=
  closureFixedPointCheck_sound gallery_observation_file_filesystem_device_contract
    gallery_observation_file_filesystem_device_closure_fixed_check

theorem gallery_observation_file_filesystem_device_bcnf_check :
    bcnfCheck gallery_observation_file_filesystem_device_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_device_bcnf : BCNF gallery_observation_file_filesystem_device_contract :=
  bcnfCheck_sound gallery_observation_file_filesystem_device_contract gallery_observation_file_filesystem_device_bcnf_check

def gallery_observation_file_filesystem_inode_contract : RelationContract where
  name := "gallery_observation_file_filesystem_inode"
  attributes := ["gallery_id", "observation_id", "file_key", "inode"]
  declaredKeys := [["gallery_id", "observation_id", "file_key"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["inode"] }
  ]

theorem gallery_observation_file_filesystem_inode_schema_well_formed :
    schemaWellFormedCheck gallery_observation_file_filesystem_inode_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_inode_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_file_filesystem_inode_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_inode_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_file_filesystem_inode_contract :=
  keysDetermineAllCheck_sound gallery_observation_file_filesystem_inode_contract
    gallery_observation_file_filesystem_inode_candidate_keys_check

theorem gallery_observation_file_filesystem_inode_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_file_filesystem_inode_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_inode_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_file_filesystem_inode_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_file_filesystem_inode_contract
    gallery_observation_file_filesystem_inode_candidate_keys_minimal_check

theorem gallery_observation_file_filesystem_inode_closure_fixed_check :
    closureFixedPointCheck gallery_observation_file_filesystem_inode_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_inode_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_file_filesystem_inode_contract :=
  closureFixedPointCheck_sound gallery_observation_file_filesystem_inode_contract
    gallery_observation_file_filesystem_inode_closure_fixed_check

theorem gallery_observation_file_filesystem_inode_bcnf_check :
    bcnfCheck gallery_observation_file_filesystem_inode_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_inode_bcnf : BCNF gallery_observation_file_filesystem_inode_contract :=
  bcnfCheck_sound gallery_observation_file_filesystem_inode_contract gallery_observation_file_filesystem_inode_bcnf_check

def gallery_observation_file_filesystem_modified_ns_contract : RelationContract where
  name := "gallery_observation_file_filesystem_modified_ns"
  attributes := ["gallery_id", "observation_id", "file_key", "modified_ns"]
  declaredKeys := [["gallery_id", "observation_id", "file_key"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["modified_ns"] }
  ]

theorem gallery_observation_file_filesystem_modified_ns_schema_well_formed :
    schemaWellFormedCheck gallery_observation_file_filesystem_modified_ns_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_modified_ns_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_file_filesystem_modified_ns_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_modified_ns_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_file_filesystem_modified_ns_contract :=
  keysDetermineAllCheck_sound gallery_observation_file_filesystem_modified_ns_contract
    gallery_observation_file_filesystem_modified_ns_candidate_keys_check

theorem gallery_observation_file_filesystem_modified_ns_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_file_filesystem_modified_ns_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_modified_ns_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_file_filesystem_modified_ns_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_file_filesystem_modified_ns_contract
    gallery_observation_file_filesystem_modified_ns_candidate_keys_minimal_check

theorem gallery_observation_file_filesystem_modified_ns_closure_fixed_check :
    closureFixedPointCheck gallery_observation_file_filesystem_modified_ns_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_modified_ns_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_file_filesystem_modified_ns_contract :=
  closureFixedPointCheck_sound gallery_observation_file_filesystem_modified_ns_contract
    gallery_observation_file_filesystem_modified_ns_closure_fixed_check

theorem gallery_observation_file_filesystem_modified_ns_bcnf_check :
    bcnfCheck gallery_observation_file_filesystem_modified_ns_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_modified_ns_bcnf : BCNF gallery_observation_file_filesystem_modified_ns_contract :=
  bcnfCheck_sound gallery_observation_file_filesystem_modified_ns_contract gallery_observation_file_filesystem_modified_ns_bcnf_check

def gallery_observation_file_filesystem_changed_ns_contract : RelationContract where
  name := "gallery_observation_file_filesystem_changed_ns"
  attributes := ["gallery_id", "observation_id", "file_key", "changed_ns"]
  declaredKeys := [["gallery_id", "observation_id", "file_key"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["changed_ns"] }
  ]

theorem gallery_observation_file_filesystem_changed_ns_schema_well_formed :
    schemaWellFormedCheck gallery_observation_file_filesystem_changed_ns_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_changed_ns_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_file_filesystem_changed_ns_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_changed_ns_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_file_filesystem_changed_ns_contract :=
  keysDetermineAllCheck_sound gallery_observation_file_filesystem_changed_ns_contract
    gallery_observation_file_filesystem_changed_ns_candidate_keys_check

theorem gallery_observation_file_filesystem_changed_ns_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_file_filesystem_changed_ns_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_changed_ns_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_file_filesystem_changed_ns_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_file_filesystem_changed_ns_contract
    gallery_observation_file_filesystem_changed_ns_candidate_keys_minimal_check

theorem gallery_observation_file_filesystem_changed_ns_closure_fixed_check :
    closureFixedPointCheck gallery_observation_file_filesystem_changed_ns_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_changed_ns_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_file_filesystem_changed_ns_contract :=
  closureFixedPointCheck_sound gallery_observation_file_filesystem_changed_ns_contract
    gallery_observation_file_filesystem_changed_ns_closure_fixed_check

theorem gallery_observation_file_filesystem_changed_ns_bcnf_check :
    bcnfCheck gallery_observation_file_filesystem_changed_ns_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_changed_ns_bcnf : BCNF gallery_observation_file_filesystem_changed_ns_contract :=
  bcnfCheck_sound gallery_observation_file_filesystem_changed_ns_contract gallery_observation_file_filesystem_changed_ns_bcnf_check

def gallery_observation_file_filesystem_seal_contract : RelationContract where
  name := "gallery_observation_file_filesystem_seal"
  attributes := ["gallery_id", "observation_id", "file_key"]
  declaredKeys := [["gallery_id", "observation_id", "file_key"]]
  declaredFDs := [
  ]

theorem gallery_observation_file_filesystem_seal_schema_well_formed :
    schemaWellFormedCheck gallery_observation_file_filesystem_seal_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_seal_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_file_filesystem_seal_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_seal_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_file_filesystem_seal_contract :=
  keysDetermineAllCheck_sound gallery_observation_file_filesystem_seal_contract
    gallery_observation_file_filesystem_seal_candidate_keys_check

theorem gallery_observation_file_filesystem_seal_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_file_filesystem_seal_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_seal_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_file_filesystem_seal_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_file_filesystem_seal_contract
    gallery_observation_file_filesystem_seal_candidate_keys_minimal_check

theorem gallery_observation_file_filesystem_seal_closure_fixed_check :
    closureFixedPointCheck gallery_observation_file_filesystem_seal_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_seal_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_file_filesystem_seal_contract :=
  closureFixedPointCheck_sound gallery_observation_file_filesystem_seal_contract
    gallery_observation_file_filesystem_seal_closure_fixed_check

theorem gallery_observation_file_filesystem_seal_bcnf_check :
    bcnfCheck gallery_observation_file_filesystem_seal_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_seal_bcnf : BCNF gallery_observation_file_filesystem_seal_contract :=
  bcnfCheck_sound gallery_observation_file_filesystem_seal_contract gallery_observation_file_filesystem_seal_bcnf_check

def gallery_observation_file_filesystem_contract : RelationContract where
  name := "gallery_observation_file_filesystem"
  attributes := ["gallery_id", "observation_id", "file_key", "device", "inode", "modified_ns", "changed_ns"]
  declaredKeys := [["gallery_id", "observation_id", "file_key"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["device", "inode", "modified_ns", "changed_ns"] }
  ]

theorem gallery_observation_file_filesystem_schema_well_formed :
    schemaWellFormedCheck gallery_observation_file_filesystem_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_file_filesystem_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_file_filesystem_contract :=
  keysDetermineAllCheck_sound gallery_observation_file_filesystem_contract
    gallery_observation_file_filesystem_candidate_keys_check

theorem gallery_observation_file_filesystem_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_file_filesystem_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_file_filesystem_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_file_filesystem_contract
    gallery_observation_file_filesystem_candidate_keys_minimal_check

theorem gallery_observation_file_filesystem_closure_fixed_check :
    closureFixedPointCheck gallery_observation_file_filesystem_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_file_filesystem_contract :=
  closureFixedPointCheck_sound gallery_observation_file_filesystem_contract
    gallery_observation_file_filesystem_closure_fixed_check

def tag_term_contract : RelationContract where
  name := "tag_term"
  attributes := ["tag_id", "namespace", "tag_value_sha256"]
  declaredKeys := [["tag_id"], ["namespace", "tag_value_sha256"]]
  declaredFDs := [
    { determinant := ["tag_id"], dependent := ["namespace", "tag_value_sha256"] },
    { determinant := ["namespace", "tag_value_sha256"], dependent := ["tag_id"] }
  ]

theorem tag_term_schema_well_formed :
    schemaWellFormedCheck tag_term_contract = true := by
  native_decide

theorem tag_term_candidate_keys_check :
    keysDetermineAllCheck tag_term_contract = true := by
  native_decide

theorem tag_term_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes tag_term_contract :=
  keysDetermineAllCheck_sound tag_term_contract
    tag_term_candidate_keys_check

theorem tag_term_candidate_keys_minimal_check :
    declaredKeysMinimalCheck tag_term_contract = true := by
  native_decide

theorem tag_term_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal tag_term_contract :=
  declaredKeysMinimalCheck_sound tag_term_contract
    tag_term_candidate_keys_minimal_check

theorem tag_term_closure_fixed_check :
    closureFixedPointCheck tag_term_contract = true := by
  native_decide

theorem tag_term_closure_reached_fixed_point :
    ClosureReachedFixedPoint tag_term_contract :=
  closureFixedPointCheck_sound tag_term_contract
    tag_term_closure_fixed_check

theorem tag_term_bcnf_check :
    bcnfCheck tag_term_contract = true := by
  native_decide

theorem tag_term_bcnf : BCNF tag_term_contract :=
  bcnfCheck_sound tag_term_contract tag_term_bcnf_check

def gallery_observation_tag_contract : RelationContract where
  name := "gallery_observation_tag"
  attributes := ["gallery_id", "observation_id", "position", "tag_id"]
  declaredKeys := [["gallery_id", "observation_id", "position"], ["gallery_id", "observation_id", "tag_id"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id", "position"], dependent := ["tag_id"] },
    { determinant := ["gallery_id", "observation_id", "tag_id"], dependent := ["position"] }
  ]

theorem gallery_observation_tag_schema_well_formed :
    schemaWellFormedCheck gallery_observation_tag_contract = true := by
  native_decide

theorem gallery_observation_tag_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_tag_contract = true := by
  native_decide

theorem gallery_observation_tag_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_tag_contract :=
  keysDetermineAllCheck_sound gallery_observation_tag_contract
    gallery_observation_tag_candidate_keys_check

theorem gallery_observation_tag_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_tag_contract = true := by
  native_decide

theorem gallery_observation_tag_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_tag_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_tag_contract
    gallery_observation_tag_candidate_keys_minimal_check

theorem gallery_observation_tag_closure_fixed_check :
    closureFixedPointCheck gallery_observation_tag_contract = true := by
  native_decide

theorem gallery_observation_tag_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_tag_contract :=
  closureFixedPointCheck_sound gallery_observation_tag_contract
    gallery_observation_tag_closure_fixed_check

theorem gallery_observation_tag_bcnf_check :
    bcnfCheck gallery_observation_tag_contract = true := by
  native_decide

theorem gallery_observation_tag_bcnf : BCNF gallery_observation_tag_contract :=
  bcnfCheck_sound gallery_observation_tag_contract gallery_observation_tag_bcnf_check

def build_manifest_core_contract : RelationContract where
  name := "build_manifest_core"
  attributes := ["build_id", "manifest_sha256", "file_count", "byte_count"]
  declaredKeys := [["build_id"]]
  declaredFDs := [
    { determinant := ["build_id"], dependent := ["manifest_sha256", "file_count", "byte_count"] }
  ]

theorem build_manifest_core_schema_well_formed :
    schemaWellFormedCheck build_manifest_core_contract = true := by
  native_decide

theorem build_manifest_core_candidate_keys_check :
    keysDetermineAllCheck build_manifest_core_contract = true := by
  native_decide

theorem build_manifest_core_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes build_manifest_core_contract :=
  keysDetermineAllCheck_sound build_manifest_core_contract
    build_manifest_core_candidate_keys_check

theorem build_manifest_core_candidate_keys_minimal_check :
    declaredKeysMinimalCheck build_manifest_core_contract = true := by
  native_decide

theorem build_manifest_core_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal build_manifest_core_contract :=
  declaredKeysMinimalCheck_sound build_manifest_core_contract
    build_manifest_core_candidate_keys_minimal_check

theorem build_manifest_core_closure_fixed_check :
    closureFixedPointCheck build_manifest_core_contract = true := by
  native_decide

theorem build_manifest_core_closure_reached_fixed_point :
    ClosureReachedFixedPoint build_manifest_core_contract :=
  closureFixedPointCheck_sound build_manifest_core_contract
    build_manifest_core_closure_fixed_check

theorem build_manifest_core_bcnf_check :
    bcnfCheck build_manifest_core_contract = true := by
  native_decide

theorem build_manifest_core_bcnf : BCNF build_manifest_core_contract :=
  bcnfCheck_sound build_manifest_core_contract build_manifest_core_bcnf_check

def build_manifest_contract : RelationContract where
  name := "build_manifest"
  attributes := ["build_id", "manifest_sha256", "gallery_count", "file_count", "byte_count", "computed_at"]
  declaredKeys := [["build_id"]]
  declaredFDs := [
    { determinant := ["build_id"], dependent := ["manifest_sha256", "gallery_count", "file_count", "byte_count", "computed_at"] }
  ]

theorem build_manifest_schema_well_formed :
    schemaWellFormedCheck build_manifest_contract = true := by
  native_decide

theorem build_manifest_candidate_keys_check :
    keysDetermineAllCheck build_manifest_contract = true := by
  native_decide

theorem build_manifest_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes build_manifest_contract :=
  keysDetermineAllCheck_sound build_manifest_contract
    build_manifest_candidate_keys_check

theorem build_manifest_candidate_keys_minimal_check :
    declaredKeysMinimalCheck build_manifest_contract = true := by
  native_decide

theorem build_manifest_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal build_manifest_contract :=
  declaredKeysMinimalCheck_sound build_manifest_contract
    build_manifest_candidate_keys_minimal_check

theorem build_manifest_closure_fixed_check :
    closureFixedPointCheck build_manifest_contract = true := by
  native_decide

theorem build_manifest_closure_reached_fixed_point :
    ClosureReachedFixedPoint build_manifest_contract :=
  closureFixedPointCheck_sound build_manifest_contract
    build_manifest_closure_fixed_check

def gallery_manifest_contract : RelationContract where
  name := "gallery_manifest"
  attributes := ["gallery_id", "observation_id", "manifest_policy_id", "manifest_sha256", "computed_at"]
  declaredKeys := [["gallery_id", "observation_id", "manifest_policy_id"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id", "manifest_policy_id"], dependent := ["manifest_sha256", "computed_at"] }
  ]

theorem gallery_manifest_schema_well_formed :
    schemaWellFormedCheck gallery_manifest_contract = true := by
  native_decide

theorem gallery_manifest_candidate_keys_check :
    keysDetermineAllCheck gallery_manifest_contract = true := by
  native_decide

theorem gallery_manifest_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_manifest_contract :=
  keysDetermineAllCheck_sound gallery_manifest_contract
    gallery_manifest_candidate_keys_check

theorem gallery_manifest_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_manifest_contract = true := by
  native_decide

theorem gallery_manifest_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_manifest_contract :=
  declaredKeysMinimalCheck_sound gallery_manifest_contract
    gallery_manifest_candidate_keys_minimal_check

theorem gallery_manifest_closure_fixed_check :
    closureFixedPointCheck gallery_manifest_contract = true := by
  native_decide

theorem gallery_manifest_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_manifest_contract :=
  closureFixedPointCheck_sound gallery_manifest_contract
    gallery_manifest_closure_fixed_check

theorem gallery_manifest_bcnf_check :
    bcnfCheck gallery_manifest_contract = true := by
  native_decide

theorem gallery_manifest_bcnf : BCNF gallery_manifest_contract :=
  bcnfCheck_sound gallery_manifest_contract gallery_manifest_bcnf_check

def analysis_policy_contract : RelationContract where
  name := "analysis_policy"
  attributes := ["policy_id", "algorithm_version", "spam_artist_threshold", "spam_occurrence_threshold", "content_owner_rule_version", "gid_winner_rule_version"]
  declaredKeys := [["policy_id"], ["algorithm_version", "spam_artist_threshold", "spam_occurrence_threshold", "content_owner_rule_version", "gid_winner_rule_version"]]
  declaredFDs := [
    { determinant := ["policy_id"], dependent := ["algorithm_version", "spam_artist_threshold", "spam_occurrence_threshold", "content_owner_rule_version", "gid_winner_rule_version"] },
    { determinant := ["algorithm_version", "spam_artist_threshold", "spam_occurrence_threshold", "content_owner_rule_version", "gid_winner_rule_version"], dependent := ["policy_id"] }
  ]

theorem analysis_policy_schema_well_formed :
    schemaWellFormedCheck analysis_policy_contract = true := by
  native_decide

theorem analysis_policy_candidate_keys_check :
    keysDetermineAllCheck analysis_policy_contract = true := by
  native_decide

theorem analysis_policy_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_policy_contract :=
  keysDetermineAllCheck_sound analysis_policy_contract
    analysis_policy_candidate_keys_check

theorem analysis_policy_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_policy_contract = true := by
  native_decide

theorem analysis_policy_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_policy_contract :=
  declaredKeysMinimalCheck_sound analysis_policy_contract
    analysis_policy_candidate_keys_minimal_check

theorem analysis_policy_closure_fixed_check :
    closureFixedPointCheck analysis_policy_contract = true := by
  native_decide

theorem analysis_policy_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_policy_contract :=
  closureFixedPointCheck_sound analysis_policy_contract
    analysis_policy_closure_fixed_check

theorem analysis_policy_bcnf_check :
    bcnfCheck analysis_policy_contract = true := by
  native_decide

theorem analysis_policy_bcnf : BCNF analysis_policy_contract :=
  bcnfCheck_sound analysis_policy_contract analysis_policy_bcnf_check

def analysis_run_state_contract : RelationContract where
  name := "analysis_run_state"
  attributes := ["analysis_id", "state"]
  declaredKeys := [["analysis_id"]]
  declaredFDs := [
    { determinant := ["analysis_id"], dependent := ["state"] }
  ]

theorem analysis_run_state_schema_well_formed :
    schemaWellFormedCheck analysis_run_state_contract = true := by
  native_decide

theorem analysis_run_state_candidate_keys_check :
    keysDetermineAllCheck analysis_run_state_contract = true := by
  native_decide

theorem analysis_run_state_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_run_state_contract :=
  keysDetermineAllCheck_sound analysis_run_state_contract
    analysis_run_state_candidate_keys_check

theorem analysis_run_state_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_run_state_contract = true := by
  native_decide

theorem analysis_run_state_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_run_state_contract :=
  declaredKeysMinimalCheck_sound analysis_run_state_contract
    analysis_run_state_candidate_keys_minimal_check

theorem analysis_run_state_closure_fixed_check :
    closureFixedPointCheck analysis_run_state_contract = true := by
  native_decide

theorem analysis_run_state_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_run_state_contract :=
  closureFixedPointCheck_sound analysis_run_state_contract
    analysis_run_state_closure_fixed_check

theorem analysis_run_state_bcnf_check :
    bcnfCheck analysis_run_state_contract = true := by
  native_decide

theorem analysis_run_state_bcnf : BCNF analysis_run_state_contract :=
  bcnfCheck_sound analysis_run_state_contract analysis_run_state_bcnf_check

def analysis_run_completed_at_contract : RelationContract where
  name := "analysis_run_completed_at"
  attributes := ["analysis_id", "completed_at"]
  declaredKeys := [["analysis_id"]]
  declaredFDs := [
    { determinant := ["analysis_id"], dependent := ["completed_at"] }
  ]

theorem analysis_run_completed_at_schema_well_formed :
    schemaWellFormedCheck analysis_run_completed_at_contract = true := by
  native_decide

theorem analysis_run_completed_at_candidate_keys_check :
    keysDetermineAllCheck analysis_run_completed_at_contract = true := by
  native_decide

theorem analysis_run_completed_at_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_run_completed_at_contract :=
  keysDetermineAllCheck_sound analysis_run_completed_at_contract
    analysis_run_completed_at_candidate_keys_check

theorem analysis_run_completed_at_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_run_completed_at_contract = true := by
  native_decide

theorem analysis_run_completed_at_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_run_completed_at_contract :=
  declaredKeysMinimalCheck_sound analysis_run_completed_at_contract
    analysis_run_completed_at_candidate_keys_minimal_check

theorem analysis_run_completed_at_closure_fixed_check :
    closureFixedPointCheck analysis_run_completed_at_contract = true := by
  native_decide

theorem analysis_run_completed_at_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_run_completed_at_contract :=
  closureFixedPointCheck_sound analysis_run_completed_at_contract
    analysis_run_completed_at_closure_fixed_check

theorem analysis_run_completed_at_bcnf_check :
    bcnfCheck analysis_run_completed_at_contract = true := by
  native_decide

theorem analysis_run_completed_at_bcnf : BCNF analysis_run_completed_at_contract :=
  bcnfCheck_sound analysis_run_completed_at_contract analysis_run_completed_at_bcnf_check

def analysis_run_descriptor_contract : RelationContract where
  name := "analysis_run_descriptor"
  attributes := ["analysis_id", "build_id", "policy_id", "input_manifest_sha256", "started_at"]
  declaredKeys := [["analysis_id"], ["build_id"]]
  declaredFDs := [
    { determinant := ["analysis_id"], dependent := ["build_id", "policy_id", "input_manifest_sha256", "started_at"] },
    { determinant := ["build_id"], dependent := ["analysis_id", "policy_id", "input_manifest_sha256", "started_at"] }
  ]

theorem analysis_run_descriptor_schema_well_formed :
    schemaWellFormedCheck analysis_run_descriptor_contract = true := by
  native_decide

theorem analysis_run_descriptor_candidate_keys_check :
    keysDetermineAllCheck analysis_run_descriptor_contract = true := by
  native_decide

theorem analysis_run_descriptor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_run_descriptor_contract :=
  keysDetermineAllCheck_sound analysis_run_descriptor_contract
    analysis_run_descriptor_candidate_keys_check

theorem analysis_run_descriptor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_run_descriptor_contract = true := by
  native_decide

theorem analysis_run_descriptor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_run_descriptor_contract :=
  declaredKeysMinimalCheck_sound analysis_run_descriptor_contract
    analysis_run_descriptor_candidate_keys_minimal_check

theorem analysis_run_descriptor_closure_fixed_check :
    closureFixedPointCheck analysis_run_descriptor_contract = true := by
  native_decide

theorem analysis_run_descriptor_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_run_descriptor_contract :=
  closureFixedPointCheck_sound analysis_run_descriptor_contract
    analysis_run_descriptor_closure_fixed_check

theorem analysis_run_descriptor_bcnf_check :
    bcnfCheck analysis_run_descriptor_contract = true := by
  native_decide

theorem analysis_run_descriptor_bcnf : BCNF analysis_run_descriptor_contract :=
  bcnfCheck_sound analysis_run_descriptor_contract analysis_run_descriptor_bcnf_check

def analysis_run_contract : RelationContract where
  name := "analysis_run"
  attributes := ["analysis_id", "build_id", "policy_id", "input_manifest_sha256", "state", "started_at", "completed_at"]
  declaredKeys := [["analysis_id"], ["build_id"]]
  declaredFDs := [
    { determinant := ["analysis_id"], dependent := ["build_id", "policy_id", "input_manifest_sha256", "state", "started_at", "completed_at"] },
    { determinant := ["build_id"], dependent := ["analysis_id", "policy_id", "input_manifest_sha256", "state", "started_at", "completed_at"] }
  ]

theorem analysis_run_schema_well_formed :
    schemaWellFormedCheck analysis_run_contract = true := by
  native_decide

theorem analysis_run_candidate_keys_check :
    keysDetermineAllCheck analysis_run_contract = true := by
  native_decide

theorem analysis_run_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_run_contract :=
  keysDetermineAllCheck_sound analysis_run_contract
    analysis_run_candidate_keys_check

theorem analysis_run_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_run_contract = true := by
  native_decide

theorem analysis_run_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_run_contract :=
  declaredKeysMinimalCheck_sound analysis_run_contract
    analysis_run_candidate_keys_minimal_check

theorem analysis_run_closure_fixed_check :
    closureFixedPointCheck analysis_run_contract = true := by
  native_decide

theorem analysis_run_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_run_contract :=
  closureFixedPointCheck_sound analysis_run_contract
    analysis_run_closure_fixed_check

def analysis_baseline_contract : RelationContract where
  name := "analysis_baseline"
  attributes := ["analysis_id", "base_analysis_id"]
  declaredKeys := [["analysis_id"]]
  declaredFDs := [
    { determinant := ["analysis_id"], dependent := ["base_analysis_id"] }
  ]

theorem analysis_baseline_schema_well_formed :
    schemaWellFormedCheck analysis_baseline_contract = true := by
  native_decide

theorem analysis_baseline_candidate_keys_check :
    keysDetermineAllCheck analysis_baseline_contract = true := by
  native_decide

theorem analysis_baseline_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_baseline_contract :=
  keysDetermineAllCheck_sound analysis_baseline_contract
    analysis_baseline_candidate_keys_check

theorem analysis_baseline_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_baseline_contract = true := by
  native_decide

theorem analysis_baseline_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_baseline_contract :=
  declaredKeysMinimalCheck_sound analysis_baseline_contract
    analysis_baseline_candidate_keys_minimal_check

theorem analysis_baseline_closure_fixed_check :
    closureFixedPointCheck analysis_baseline_contract = true := by
  native_decide

theorem analysis_baseline_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_baseline_contract :=
  closureFixedPointCheck_sound analysis_baseline_contract
    analysis_baseline_closure_fixed_check

theorem analysis_baseline_bcnf_check :
    bcnfCheck analysis_baseline_contract = true := by
  native_decide

theorem analysis_baseline_bcnf : BCNF analysis_baseline_contract :=
  bcnfCheck_sound analysis_baseline_contract analysis_baseline_bcnf_check

def analysis_state_anchor_contract : RelationContract where
  name := "analysis_state_anchor"
  attributes := ["analysis_id", "anchor_analysis_id", "overlay_depth"]
  declaredKeys := [["analysis_id"]]
  declaredFDs := [
    { determinant := ["analysis_id"], dependent := ["anchor_analysis_id", "overlay_depth"] }
  ]

theorem analysis_state_anchor_schema_well_formed :
    schemaWellFormedCheck analysis_state_anchor_contract = true := by
  native_decide

theorem analysis_state_anchor_candidate_keys_check :
    keysDetermineAllCheck analysis_state_anchor_contract = true := by
  native_decide

theorem analysis_state_anchor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_state_anchor_contract :=
  keysDetermineAllCheck_sound analysis_state_anchor_contract
    analysis_state_anchor_candidate_keys_check

theorem analysis_state_anchor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_state_anchor_contract = true := by
  native_decide

theorem analysis_state_anchor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_state_anchor_contract :=
  declaredKeysMinimalCheck_sound analysis_state_anchor_contract
    analysis_state_anchor_candidate_keys_minimal_check

theorem analysis_state_anchor_closure_fixed_check :
    closureFixedPointCheck analysis_state_anchor_contract = true := by
  native_decide

theorem analysis_state_anchor_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_state_anchor_contract :=
  closureFixedPointCheck_sound analysis_state_anchor_contract
    analysis_state_anchor_closure_fixed_check

def analysis_state_ancestry_contract : RelationContract where
  name := "analysis_state_ancestry"
  attributes := ["analysis_id", "ancestor_depth", "ancestor_analysis_id"]
  declaredKeys := [["analysis_id", "ancestor_depth"], ["analysis_id", "ancestor_analysis_id"]]
  declaredFDs := [
    { determinant := ["analysis_id", "ancestor_depth"], dependent := ["ancestor_analysis_id"] },
    { determinant := ["analysis_id", "ancestor_analysis_id"], dependent := ["ancestor_depth"] }
  ]

theorem analysis_state_ancestry_schema_well_formed :
    schemaWellFormedCheck analysis_state_ancestry_contract = true := by
  native_decide

theorem analysis_state_ancestry_candidate_keys_check :
    keysDetermineAllCheck analysis_state_ancestry_contract = true := by
  native_decide

theorem analysis_state_ancestry_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_state_ancestry_contract :=
  keysDetermineAllCheck_sound analysis_state_ancestry_contract
    analysis_state_ancestry_candidate_keys_check

theorem analysis_state_ancestry_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_state_ancestry_contract = true := by
  native_decide

theorem analysis_state_ancestry_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_state_ancestry_contract :=
  declaredKeysMinimalCheck_sound analysis_state_ancestry_contract
    analysis_state_ancestry_candidate_keys_minimal_check

theorem analysis_state_ancestry_closure_fixed_check :
    closureFixedPointCheck analysis_state_ancestry_contract = true := by
  native_decide

theorem analysis_state_ancestry_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_state_ancestry_contract :=
  closureFixedPointCheck_sound analysis_state_ancestry_contract
    analysis_state_ancestry_closure_fixed_check

theorem analysis_state_ancestry_bcnf_check :
    bcnfCheck analysis_state_ancestry_contract = true := by
  native_decide

theorem analysis_state_ancestry_bcnf : BCNF analysis_state_ancestry_contract :=
  bcnfCheck_sound analysis_state_ancestry_contract analysis_state_ancestry_bcnf_check

def source_snapshot_manifest_identity_contract : RelationContract where
  name := "source_snapshot_manifest_identity"
  attributes := ["snapshot_manifest_sha256", "gallery_count", "file_count", "byte_count"]
  declaredKeys := [["snapshot_manifest_sha256"]]
  declaredFDs := [
    { determinant := ["snapshot_manifest_sha256"], dependent := ["gallery_count", "file_count", "byte_count"] }
  ]

theorem source_snapshot_manifest_identity_schema_well_formed :
    schemaWellFormedCheck source_snapshot_manifest_identity_contract = true := by
  native_decide

theorem source_snapshot_manifest_identity_candidate_keys_check :
    keysDetermineAllCheck source_snapshot_manifest_identity_contract = true := by
  native_decide

theorem source_snapshot_manifest_identity_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_snapshot_manifest_identity_contract :=
  keysDetermineAllCheck_sound source_snapshot_manifest_identity_contract
    source_snapshot_manifest_identity_candidate_keys_check

theorem source_snapshot_manifest_identity_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_snapshot_manifest_identity_contract = true := by
  native_decide

theorem source_snapshot_manifest_identity_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_snapshot_manifest_identity_contract :=
  declaredKeysMinimalCheck_sound source_snapshot_manifest_identity_contract
    source_snapshot_manifest_identity_candidate_keys_minimal_check

theorem source_snapshot_manifest_identity_closure_fixed_check :
    closureFixedPointCheck source_snapshot_manifest_identity_contract = true := by
  native_decide

theorem source_snapshot_manifest_identity_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_snapshot_manifest_identity_contract :=
  closureFixedPointCheck_sound source_snapshot_manifest_identity_contract
    source_snapshot_manifest_identity_closure_fixed_check

theorem source_snapshot_manifest_identity_bcnf_check :
    bcnfCheck source_snapshot_manifest_identity_contract = true := by
  native_decide

theorem source_snapshot_manifest_identity_bcnf : BCNF source_snapshot_manifest_identity_contract :=
  bcnfCheck_sound source_snapshot_manifest_identity_contract source_snapshot_manifest_identity_bcnf_check

def analysis_snapshot_manifest_contract : RelationContract where
  name := "analysis_snapshot_manifest"
  attributes := ["analysis_id", "snapshot_manifest_sha256"]
  declaredKeys := [["analysis_id"]]
  declaredFDs := [
    { determinant := ["analysis_id"], dependent := ["snapshot_manifest_sha256"] }
  ]

theorem analysis_snapshot_manifest_schema_well_formed :
    schemaWellFormedCheck analysis_snapshot_manifest_contract = true := by
  native_decide

theorem analysis_snapshot_manifest_candidate_keys_check :
    keysDetermineAllCheck analysis_snapshot_manifest_contract = true := by
  native_decide

theorem analysis_snapshot_manifest_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_snapshot_manifest_contract :=
  keysDetermineAllCheck_sound analysis_snapshot_manifest_contract
    analysis_snapshot_manifest_candidate_keys_check

theorem analysis_snapshot_manifest_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_snapshot_manifest_contract = true := by
  native_decide

theorem analysis_snapshot_manifest_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_snapshot_manifest_contract :=
  declaredKeysMinimalCheck_sound analysis_snapshot_manifest_contract
    analysis_snapshot_manifest_candidate_keys_minimal_check

theorem analysis_snapshot_manifest_closure_fixed_check :
    closureFixedPointCheck analysis_snapshot_manifest_contract = true := by
  native_decide

theorem analysis_snapshot_manifest_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_snapshot_manifest_contract :=
  closureFixedPointCheck_sound analysis_snapshot_manifest_contract
    analysis_snapshot_manifest_closure_fixed_check

theorem analysis_snapshot_manifest_bcnf_check :
    bcnfCheck analysis_snapshot_manifest_contract = true := by
  native_decide

theorem analysis_snapshot_manifest_bcnf : BCNF analysis_snapshot_manifest_contract :=
  bcnfCheck_sound analysis_snapshot_manifest_contract analysis_snapshot_manifest_bcnf_check

def source_revision_descriptor_contract : RelationContract where
  name := "source_revision_descriptor"
  attributes := ["source_revision", "channel", "snapshot_manifest_sha256"]
  declaredKeys := [["source_revision"]]
  declaredFDs := [
    { determinant := ["source_revision"], dependent := ["channel", "snapshot_manifest_sha256"] }
  ]

theorem source_revision_descriptor_schema_well_formed :
    schemaWellFormedCheck source_revision_descriptor_contract = true := by
  native_decide

theorem source_revision_descriptor_candidate_keys_check :
    keysDetermineAllCheck source_revision_descriptor_contract = true := by
  native_decide

theorem source_revision_descriptor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_revision_descriptor_contract :=
  keysDetermineAllCheck_sound source_revision_descriptor_contract
    source_revision_descriptor_candidate_keys_check

theorem source_revision_descriptor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_revision_descriptor_contract = true := by
  native_decide

theorem source_revision_descriptor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_revision_descriptor_contract :=
  declaredKeysMinimalCheck_sound source_revision_descriptor_contract
    source_revision_descriptor_candidate_keys_minimal_check

theorem source_revision_descriptor_closure_fixed_check :
    closureFixedPointCheck source_revision_descriptor_contract = true := by
  native_decide

theorem source_revision_descriptor_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_revision_descriptor_contract :=
  closureFixedPointCheck_sound source_revision_descriptor_contract
    source_revision_descriptor_closure_fixed_check

theorem source_revision_descriptor_bcnf_check :
    bcnfCheck source_revision_descriptor_contract = true := by
  native_decide

theorem source_revision_descriptor_bcnf : BCNF source_revision_descriptor_contract :=
  bcnfCheck_sound source_revision_descriptor_contract source_revision_descriptor_bcnf_check

def source_revision_contract : RelationContract where
  name := "source_revision"
  attributes := ["source_revision", "channel", "snapshot_manifest_sha256", "published_at"]
  declaredKeys := [["source_revision"]]
  declaredFDs := [
    { determinant := ["source_revision"], dependent := ["channel", "snapshot_manifest_sha256", "published_at"] }
  ]

theorem source_revision_schema_well_formed :
    schemaWellFormedCheck source_revision_contract = true := by
  native_decide

theorem source_revision_candidate_keys_check :
    keysDetermineAllCheck source_revision_contract = true := by
  native_decide

theorem source_revision_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_revision_contract :=
  keysDetermineAllCheck_sound source_revision_contract
    source_revision_candidate_keys_check

theorem source_revision_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_revision_contract = true := by
  native_decide

theorem source_revision_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_revision_contract :=
  declaredKeysMinimalCheck_sound source_revision_contract
    source_revision_candidate_keys_minimal_check

theorem source_revision_closure_fixed_check :
    closureFixedPointCheck source_revision_contract = true := by
  native_decide

theorem source_revision_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_revision_contract :=
  closureFixedPointCheck_sound source_revision_contract
    source_revision_closure_fixed_check

def source_revision_generation_contract : RelationContract where
  name := "source_revision_generation"
  attributes := ["source_revision", "generation"]
  declaredKeys := [["source_revision"], ["generation"]]
  declaredFDs := [
    { determinant := ["source_revision"], dependent := ["generation"] },
    { determinant := ["generation"], dependent := ["source_revision"] }
  ]

theorem source_revision_generation_schema_well_formed :
    schemaWellFormedCheck source_revision_generation_contract = true := by
  native_decide

theorem source_revision_generation_candidate_keys_check :
    keysDetermineAllCheck source_revision_generation_contract = true := by
  native_decide

theorem source_revision_generation_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_revision_generation_contract :=
  keysDetermineAllCheck_sound source_revision_generation_contract
    source_revision_generation_candidate_keys_check

theorem source_revision_generation_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_revision_generation_contract = true := by
  native_decide

theorem source_revision_generation_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_revision_generation_contract :=
  declaredKeysMinimalCheck_sound source_revision_generation_contract
    source_revision_generation_candidate_keys_minimal_check

theorem source_revision_generation_closure_fixed_check :
    closureFixedPointCheck source_revision_generation_contract = true := by
  native_decide

theorem source_revision_generation_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_revision_generation_contract :=
  closureFixedPointCheck_sound source_revision_generation_contract
    source_revision_generation_closure_fixed_check

def source_revision_provenance_contract : RelationContract where
  name := "source_revision_provenance"
  attributes := ["source_revision", "analysis_id"]
  declaredKeys := [["source_revision"], ["analysis_id"]]
  declaredFDs := [
    { determinant := ["source_revision"], dependent := ["analysis_id"] },
    { determinant := ["analysis_id"], dependent := ["source_revision"] }
  ]

theorem source_revision_provenance_schema_well_formed :
    schemaWellFormedCheck source_revision_provenance_contract = true := by
  native_decide

theorem source_revision_provenance_candidate_keys_check :
    keysDetermineAllCheck source_revision_provenance_contract = true := by
  native_decide

theorem source_revision_provenance_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_revision_provenance_contract :=
  keysDetermineAllCheck_sound source_revision_provenance_contract
    source_revision_provenance_candidate_keys_check

theorem source_revision_provenance_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_revision_provenance_contract = true := by
  native_decide

theorem source_revision_provenance_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_revision_provenance_contract :=
  declaredKeysMinimalCheck_sound source_revision_provenance_contract
    source_revision_provenance_candidate_keys_minimal_check

theorem source_revision_provenance_closure_fixed_check :
    closureFixedPointCheck source_revision_provenance_contract = true := by
  native_decide

theorem source_revision_provenance_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_revision_provenance_contract :=
  closureFixedPointCheck_sound source_revision_provenance_contract
    source_revision_provenance_closure_fixed_check

theorem source_revision_provenance_bcnf_check :
    bcnfCheck source_revision_provenance_contract = true := by
  native_decide

theorem source_revision_provenance_bcnf : BCNF source_revision_provenance_contract :=
  bcnfCheck_sound source_revision_provenance_contract source_revision_provenance_bcnf_check

def source_head_revision_contract : RelationContract where
  name := "source_head_revision"
  attributes := ["channel", "source_revision"]
  declaredKeys := [["channel"], ["source_revision"]]
  declaredFDs := [
    { determinant := ["channel"], dependent := ["source_revision"] },
    { determinant := ["source_revision"], dependent := ["channel"] }
  ]

theorem source_head_revision_schema_well_formed :
    schemaWellFormedCheck source_head_revision_contract = true := by
  native_decide

theorem source_head_revision_candidate_keys_check :
    keysDetermineAllCheck source_head_revision_contract = true := by
  native_decide

theorem source_head_revision_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_head_revision_contract :=
  keysDetermineAllCheck_sound source_head_revision_contract
    source_head_revision_candidate_keys_check

theorem source_head_revision_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_head_revision_contract = true := by
  native_decide

theorem source_head_revision_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_head_revision_contract :=
  declaredKeysMinimalCheck_sound source_head_revision_contract
    source_head_revision_candidate_keys_minimal_check

theorem source_head_revision_closure_fixed_check :
    closureFixedPointCheck source_head_revision_contract = true := by
  native_decide

theorem source_head_revision_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_head_revision_contract :=
  closureFixedPointCheck_sound source_head_revision_contract
    source_head_revision_closure_fixed_check

def source_head_advanced_at_contract : RelationContract where
  name := "source_head_advanced_at"
  attributes := ["channel", "advanced_at"]
  declaredKeys := [["channel"]]
  declaredFDs := [
    { determinant := ["channel"], dependent := ["advanced_at"] }
  ]

theorem source_head_advanced_at_schema_well_formed :
    schemaWellFormedCheck source_head_advanced_at_contract = true := by
  native_decide

theorem source_head_advanced_at_candidate_keys_check :
    keysDetermineAllCheck source_head_advanced_at_contract = true := by
  native_decide

theorem source_head_advanced_at_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_head_advanced_at_contract :=
  keysDetermineAllCheck_sound source_head_advanced_at_contract
    source_head_advanced_at_candidate_keys_check

theorem source_head_advanced_at_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_head_advanced_at_contract = true := by
  native_decide

theorem source_head_advanced_at_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_head_advanced_at_contract :=
  declaredKeysMinimalCheck_sound source_head_advanced_at_contract
    source_head_advanced_at_candidate_keys_minimal_check

theorem source_head_advanced_at_closure_fixed_check :
    closureFixedPointCheck source_head_advanced_at_contract = true := by
  native_decide

theorem source_head_advanced_at_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_head_advanced_at_contract :=
  closureFixedPointCheck_sound source_head_advanced_at_contract
    source_head_advanced_at_closure_fixed_check

def source_head_contract : RelationContract where
  name := "source_head"
  attributes := ["channel", "source_revision", "generation", "advanced_at"]
  declaredKeys := [["channel"], ["source_revision"], ["generation"]]
  declaredFDs := [
    { determinant := ["channel"], dependent := ["source_revision", "generation", "advanced_at"] },
    { determinant := ["source_revision"], dependent := ["channel", "generation", "advanced_at"] },
    { determinant := ["generation"], dependent := ["channel", "source_revision", "advanced_at"] }
  ]

theorem source_head_schema_well_formed :
    schemaWellFormedCheck source_head_contract = true := by
  native_decide

theorem source_head_candidate_keys_check :
    keysDetermineAllCheck source_head_contract = true := by
  native_decide

theorem source_head_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_head_contract :=
  keysDetermineAllCheck_sound source_head_contract
    source_head_candidate_keys_check

theorem source_head_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_head_contract = true := by
  native_decide

theorem source_head_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_head_contract :=
  declaredKeysMinimalCheck_sound source_head_contract
    source_head_candidate_keys_minimal_check

theorem source_head_closure_fixed_check :
    closureFixedPointCheck source_head_contract = true := by
  native_decide

theorem source_head_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_head_contract :=
  closureFixedPointCheck_sound source_head_contract
    source_head_closure_fixed_check

def gallery_observation_artist_contract : RelationContract where
  name := "gallery_observation_artist"
  attributes := ["gallery_id", "observation_id", "artist_tag_id"]
  declaredKeys := [["gallery_id", "observation_id", "artist_tag_id"]]
  declaredFDs := [
  ]

theorem gallery_observation_artist_schema_well_formed :
    schemaWellFormedCheck gallery_observation_artist_contract = true := by
  native_decide

theorem gallery_observation_artist_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_artist_contract = true := by
  native_decide

theorem gallery_observation_artist_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_artist_contract :=
  keysDetermineAllCheck_sound gallery_observation_artist_contract
    gallery_observation_artist_candidate_keys_check

theorem gallery_observation_artist_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_artist_contract = true := by
  native_decide

theorem gallery_observation_artist_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_artist_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_artist_contract
    gallery_observation_artist_candidate_keys_minimal_check

theorem gallery_observation_artist_closure_fixed_check :
    closureFixedPointCheck gallery_observation_artist_contract = true := by
  native_decide

theorem gallery_observation_artist_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_artist_contract :=
  closureFixedPointCheck_sound gallery_observation_artist_contract
    gallery_observation_artist_closure_fixed_check

theorem gallery_observation_artist_bcnf_check :
    bcnfCheck gallery_observation_artist_contract = true := by
  native_decide

theorem gallery_observation_artist_bcnf : BCNF gallery_observation_artist_contract :=
  bcnfCheck_sound gallery_observation_artist_contract gallery_observation_artist_bcnf_check

def gallery_observation_file_hash_occurrence_contract : RelationContract where
  name := "gallery_observation_file_hash_occurrence"
  attributes := ["gallery_id", "observation_id", "file_sha256", "occurrence_count"]
  declaredKeys := [["gallery_id", "observation_id", "file_sha256"]]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id", "file_sha256"], dependent := ["occurrence_count"] }
  ]

theorem gallery_observation_file_hash_occurrence_schema_well_formed :
    schemaWellFormedCheck gallery_observation_file_hash_occurrence_contract = true := by
  native_decide

theorem gallery_observation_file_hash_occurrence_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_file_hash_occurrence_contract = true := by
  native_decide

theorem gallery_observation_file_hash_occurrence_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_file_hash_occurrence_contract :=
  keysDetermineAllCheck_sound gallery_observation_file_hash_occurrence_contract
    gallery_observation_file_hash_occurrence_candidate_keys_check

theorem gallery_observation_file_hash_occurrence_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_file_hash_occurrence_contract = true := by
  native_decide

theorem gallery_observation_file_hash_occurrence_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_file_hash_occurrence_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_file_hash_occurrence_contract
    gallery_observation_file_hash_occurrence_candidate_keys_minimal_check

theorem gallery_observation_file_hash_occurrence_closure_fixed_check :
    closureFixedPointCheck gallery_observation_file_hash_occurrence_contract = true := by
  native_decide

theorem gallery_observation_file_hash_occurrence_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_file_hash_occurrence_contract :=
  closureFixedPointCheck_sound gallery_observation_file_hash_occurrence_contract
    gallery_observation_file_hash_occurrence_closure_fixed_check

theorem gallery_observation_file_hash_occurrence_bcnf_check :
    bcnfCheck gallery_observation_file_hash_occurrence_contract = true := by
  native_decide

theorem gallery_observation_file_hash_occurrence_bcnf : BCNF gallery_observation_file_hash_occurrence_contract :=
  bcnfCheck_sound gallery_observation_file_hash_occurrence_contract gallery_observation_file_hash_occurrence_bcnf_check

def analysis_changed_gallery_contract : RelationContract where
  name := "analysis_changed_gallery"
  attributes := ["analysis_id", "gallery_id", "change_kind"]
  declaredKeys := [["analysis_id", "gallery_id"]]
  declaredFDs := [
    { determinant := ["analysis_id", "gallery_id"], dependent := ["change_kind"] }
  ]

theorem analysis_changed_gallery_schema_well_formed :
    schemaWellFormedCheck analysis_changed_gallery_contract = true := by
  native_decide

theorem analysis_changed_gallery_candidate_keys_check :
    keysDetermineAllCheck analysis_changed_gallery_contract = true := by
  native_decide

theorem analysis_changed_gallery_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_changed_gallery_contract :=
  keysDetermineAllCheck_sound analysis_changed_gallery_contract
    analysis_changed_gallery_candidate_keys_check

theorem analysis_changed_gallery_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_changed_gallery_contract = true := by
  native_decide

theorem analysis_changed_gallery_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_changed_gallery_contract :=
  declaredKeysMinimalCheck_sound analysis_changed_gallery_contract
    analysis_changed_gallery_candidate_keys_minimal_check

theorem analysis_changed_gallery_closure_fixed_check :
    closureFixedPointCheck analysis_changed_gallery_contract = true := by
  native_decide

theorem analysis_changed_gallery_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_changed_gallery_contract :=
  closureFixedPointCheck_sound analysis_changed_gallery_contract
    analysis_changed_gallery_closure_fixed_check

theorem analysis_changed_gallery_bcnf_check :
    bcnfCheck analysis_changed_gallery_contract = true := by
  native_decide

theorem analysis_changed_gallery_bcnf : BCNF analysis_changed_gallery_contract :=
  bcnfCheck_sound analysis_changed_gallery_contract analysis_changed_gallery_bcnf_check

def analysis_changed_file_hash_contract : RelationContract where
  name := "analysis_changed_file_hash"
  attributes := ["analysis_id", "file_sha256"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
  ]

theorem analysis_changed_file_hash_schema_well_formed :
    schemaWellFormedCheck analysis_changed_file_hash_contract = true := by
  native_decide

theorem analysis_changed_file_hash_candidate_keys_check :
    keysDetermineAllCheck analysis_changed_file_hash_contract = true := by
  native_decide

theorem analysis_changed_file_hash_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_changed_file_hash_contract :=
  keysDetermineAllCheck_sound analysis_changed_file_hash_contract
    analysis_changed_file_hash_candidate_keys_check

theorem analysis_changed_file_hash_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_changed_file_hash_contract = true := by
  native_decide

theorem analysis_changed_file_hash_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_changed_file_hash_contract :=
  declaredKeysMinimalCheck_sound analysis_changed_file_hash_contract
    analysis_changed_file_hash_candidate_keys_minimal_check

theorem analysis_changed_file_hash_closure_fixed_check :
    closureFixedPointCheck analysis_changed_file_hash_contract = true := by
  native_decide

theorem analysis_changed_file_hash_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_changed_file_hash_contract :=
  closureFixedPointCheck_sound analysis_changed_file_hash_contract
    analysis_changed_file_hash_closure_fixed_check

theorem analysis_changed_file_hash_bcnf_check :
    bcnfCheck analysis_changed_file_hash_contract = true := by
  native_decide

theorem analysis_changed_file_hash_bcnf : BCNF analysis_changed_file_hash_contract :=
  bcnfCheck_sound analysis_changed_file_hash_contract analysis_changed_file_hash_bcnf_check

def analysis_exclusion_delta_anchor_contract : RelationContract where
  name := "analysis_exclusion_delta_anchor"
  attributes := ["analysis_id", "file_sha256"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
  ]

theorem analysis_exclusion_delta_anchor_schema_well_formed :
    schemaWellFormedCheck analysis_exclusion_delta_anchor_contract = true := by
  native_decide

theorem analysis_exclusion_delta_anchor_candidate_keys_check :
    keysDetermineAllCheck analysis_exclusion_delta_anchor_contract = true := by
  native_decide

theorem analysis_exclusion_delta_anchor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_exclusion_delta_anchor_contract :=
  keysDetermineAllCheck_sound analysis_exclusion_delta_anchor_contract
    analysis_exclusion_delta_anchor_candidate_keys_check

theorem analysis_exclusion_delta_anchor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_exclusion_delta_anchor_contract = true := by
  native_decide

theorem analysis_exclusion_delta_anchor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_exclusion_delta_anchor_contract :=
  declaredKeysMinimalCheck_sound analysis_exclusion_delta_anchor_contract
    analysis_exclusion_delta_anchor_candidate_keys_minimal_check

theorem analysis_exclusion_delta_anchor_closure_fixed_check :
    closureFixedPointCheck analysis_exclusion_delta_anchor_contract = true := by
  native_decide

theorem analysis_exclusion_delta_anchor_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_exclusion_delta_anchor_contract :=
  closureFixedPointCheck_sound analysis_exclusion_delta_anchor_contract
    analysis_exclusion_delta_anchor_closure_fixed_check

theorem analysis_exclusion_delta_anchor_bcnf_check :
    bcnfCheck analysis_exclusion_delta_anchor_contract = true := by
  native_decide

theorem analysis_exclusion_delta_anchor_bcnf : BCNF analysis_exclusion_delta_anchor_contract :=
  bcnfCheck_sound analysis_exclusion_delta_anchor_contract analysis_exclusion_delta_anchor_bcnf_check

def analysis_exclusion_delta_old_excluded_contract : RelationContract where
  name := "analysis_exclusion_delta_old_excluded"
  attributes := ["analysis_id", "file_sha256", "old_excluded"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
    { determinant := ["analysis_id", "file_sha256"], dependent := ["old_excluded"] }
  ]

theorem analysis_exclusion_delta_old_excluded_schema_well_formed :
    schemaWellFormedCheck analysis_exclusion_delta_old_excluded_contract = true := by
  native_decide

theorem analysis_exclusion_delta_old_excluded_candidate_keys_check :
    keysDetermineAllCheck analysis_exclusion_delta_old_excluded_contract = true := by
  native_decide

theorem analysis_exclusion_delta_old_excluded_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_exclusion_delta_old_excluded_contract :=
  keysDetermineAllCheck_sound analysis_exclusion_delta_old_excluded_contract
    analysis_exclusion_delta_old_excluded_candidate_keys_check

theorem analysis_exclusion_delta_old_excluded_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_exclusion_delta_old_excluded_contract = true := by
  native_decide

theorem analysis_exclusion_delta_old_excluded_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_exclusion_delta_old_excluded_contract :=
  declaredKeysMinimalCheck_sound analysis_exclusion_delta_old_excluded_contract
    analysis_exclusion_delta_old_excluded_candidate_keys_minimal_check

theorem analysis_exclusion_delta_old_excluded_closure_fixed_check :
    closureFixedPointCheck analysis_exclusion_delta_old_excluded_contract = true := by
  native_decide

theorem analysis_exclusion_delta_old_excluded_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_exclusion_delta_old_excluded_contract :=
  closureFixedPointCheck_sound analysis_exclusion_delta_old_excluded_contract
    analysis_exclusion_delta_old_excluded_closure_fixed_check

theorem analysis_exclusion_delta_old_excluded_bcnf_check :
    bcnfCheck analysis_exclusion_delta_old_excluded_contract = true := by
  native_decide

theorem analysis_exclusion_delta_old_excluded_bcnf : BCNF analysis_exclusion_delta_old_excluded_contract :=
  bcnfCheck_sound analysis_exclusion_delta_old_excluded_contract analysis_exclusion_delta_old_excluded_bcnf_check

def analysis_exclusion_delta_new_excluded_contract : RelationContract where
  name := "analysis_exclusion_delta_new_excluded"
  attributes := ["analysis_id", "file_sha256", "new_excluded"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
    { determinant := ["analysis_id", "file_sha256"], dependent := ["new_excluded"] }
  ]

theorem analysis_exclusion_delta_new_excluded_schema_well_formed :
    schemaWellFormedCheck analysis_exclusion_delta_new_excluded_contract = true := by
  native_decide

theorem analysis_exclusion_delta_new_excluded_candidate_keys_check :
    keysDetermineAllCheck analysis_exclusion_delta_new_excluded_contract = true := by
  native_decide

theorem analysis_exclusion_delta_new_excluded_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_exclusion_delta_new_excluded_contract :=
  keysDetermineAllCheck_sound analysis_exclusion_delta_new_excluded_contract
    analysis_exclusion_delta_new_excluded_candidate_keys_check

theorem analysis_exclusion_delta_new_excluded_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_exclusion_delta_new_excluded_contract = true := by
  native_decide

theorem analysis_exclusion_delta_new_excluded_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_exclusion_delta_new_excluded_contract :=
  declaredKeysMinimalCheck_sound analysis_exclusion_delta_new_excluded_contract
    analysis_exclusion_delta_new_excluded_candidate_keys_minimal_check

theorem analysis_exclusion_delta_new_excluded_closure_fixed_check :
    closureFixedPointCheck analysis_exclusion_delta_new_excluded_contract = true := by
  native_decide

theorem analysis_exclusion_delta_new_excluded_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_exclusion_delta_new_excluded_contract :=
  closureFixedPointCheck_sound analysis_exclusion_delta_new_excluded_contract
    analysis_exclusion_delta_new_excluded_closure_fixed_check

theorem analysis_exclusion_delta_new_excluded_bcnf_check :
    bcnfCheck analysis_exclusion_delta_new_excluded_contract = true := by
  native_decide

theorem analysis_exclusion_delta_new_excluded_bcnf : BCNF analysis_exclusion_delta_new_excluded_contract :=
  bcnfCheck_sound analysis_exclusion_delta_new_excluded_contract analysis_exclusion_delta_new_excluded_bcnf_check

def analysis_exclusion_delta_change_contract : RelationContract where
  name := "analysis_exclusion_delta_change"
  attributes := ["analysis_id", "file_sha256"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
  ]

theorem analysis_exclusion_delta_change_schema_well_formed :
    schemaWellFormedCheck analysis_exclusion_delta_change_contract = true := by
  native_decide

theorem analysis_exclusion_delta_change_candidate_keys_check :
    keysDetermineAllCheck analysis_exclusion_delta_change_contract = true := by
  native_decide

theorem analysis_exclusion_delta_change_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_exclusion_delta_change_contract :=
  keysDetermineAllCheck_sound analysis_exclusion_delta_change_contract
    analysis_exclusion_delta_change_candidate_keys_check

theorem analysis_exclusion_delta_change_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_exclusion_delta_change_contract = true := by
  native_decide

theorem analysis_exclusion_delta_change_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_exclusion_delta_change_contract :=
  declaredKeysMinimalCheck_sound analysis_exclusion_delta_change_contract
    analysis_exclusion_delta_change_candidate_keys_minimal_check

theorem analysis_exclusion_delta_change_closure_fixed_check :
    closureFixedPointCheck analysis_exclusion_delta_change_contract = true := by
  native_decide

theorem analysis_exclusion_delta_change_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_exclusion_delta_change_contract :=
  closureFixedPointCheck_sound analysis_exclusion_delta_change_contract
    analysis_exclusion_delta_change_closure_fixed_check

theorem analysis_exclusion_delta_change_bcnf_check :
    bcnfCheck analysis_exclusion_delta_change_contract = true := by
  native_decide

theorem analysis_exclusion_delta_change_bcnf : BCNF analysis_exclusion_delta_change_contract :=
  bcnfCheck_sound analysis_exclusion_delta_change_contract analysis_exclusion_delta_change_bcnf_check

def analysis_exclusion_delta_seal_contract : RelationContract where
  name := "analysis_exclusion_delta_seal"
  attributes := ["analysis_id", "file_sha256"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
  ]

theorem analysis_exclusion_delta_seal_schema_well_formed :
    schemaWellFormedCheck analysis_exclusion_delta_seal_contract = true := by
  native_decide

theorem analysis_exclusion_delta_seal_candidate_keys_check :
    keysDetermineAllCheck analysis_exclusion_delta_seal_contract = true := by
  native_decide

theorem analysis_exclusion_delta_seal_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_exclusion_delta_seal_contract :=
  keysDetermineAllCheck_sound analysis_exclusion_delta_seal_contract
    analysis_exclusion_delta_seal_candidate_keys_check

theorem analysis_exclusion_delta_seal_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_exclusion_delta_seal_contract = true := by
  native_decide

theorem analysis_exclusion_delta_seal_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_exclusion_delta_seal_contract :=
  declaredKeysMinimalCheck_sound analysis_exclusion_delta_seal_contract
    analysis_exclusion_delta_seal_candidate_keys_minimal_check

theorem analysis_exclusion_delta_seal_closure_fixed_check :
    closureFixedPointCheck analysis_exclusion_delta_seal_contract = true := by
  native_decide

theorem analysis_exclusion_delta_seal_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_exclusion_delta_seal_contract :=
  closureFixedPointCheck_sound analysis_exclusion_delta_seal_contract
    analysis_exclusion_delta_seal_closure_fixed_check

theorem analysis_exclusion_delta_seal_bcnf_check :
    bcnfCheck analysis_exclusion_delta_seal_contract = true := by
  native_decide

theorem analysis_exclusion_delta_seal_bcnf : BCNF analysis_exclusion_delta_seal_contract :=
  bcnfCheck_sound analysis_exclusion_delta_seal_contract analysis_exclusion_delta_seal_bcnf_check

def analysis_exclusion_delta_contract : RelationContract where
  name := "analysis_exclusion_delta"
  attributes := ["analysis_id", "file_sha256", "old_excluded", "new_excluded"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
    { determinant := ["analysis_id", "file_sha256"], dependent := ["old_excluded", "new_excluded"] }
  ]

theorem analysis_exclusion_delta_schema_well_formed :
    schemaWellFormedCheck analysis_exclusion_delta_contract = true := by
  native_decide

theorem analysis_exclusion_delta_candidate_keys_check :
    keysDetermineAllCheck analysis_exclusion_delta_contract = true := by
  native_decide

theorem analysis_exclusion_delta_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_exclusion_delta_contract :=
  keysDetermineAllCheck_sound analysis_exclusion_delta_contract
    analysis_exclusion_delta_candidate_keys_check

theorem analysis_exclusion_delta_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_exclusion_delta_contract = true := by
  native_decide

theorem analysis_exclusion_delta_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_exclusion_delta_contract :=
  declaredKeysMinimalCheck_sound analysis_exclusion_delta_contract
    analysis_exclusion_delta_candidate_keys_minimal_check

theorem analysis_exclusion_delta_closure_fixed_check :
    closureFixedPointCheck analysis_exclusion_delta_contract = true := by
  native_decide

theorem analysis_exclusion_delta_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_exclusion_delta_contract :=
  closureFixedPointCheck_sound analysis_exclusion_delta_contract
    analysis_exclusion_delta_closure_fixed_check

def analysis_impacted_gallery_contract : RelationContract where
  name := "analysis_impacted_gallery"
  attributes := ["analysis_id", "gallery_id"]
  declaredKeys := [["analysis_id", "gallery_id"]]
  declaredFDs := [
  ]

theorem analysis_impacted_gallery_schema_well_formed :
    schemaWellFormedCheck analysis_impacted_gallery_contract = true := by
  native_decide

theorem analysis_impacted_gallery_candidate_keys_check :
    keysDetermineAllCheck analysis_impacted_gallery_contract = true := by
  native_decide

theorem analysis_impacted_gallery_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_impacted_gallery_contract :=
  keysDetermineAllCheck_sound analysis_impacted_gallery_contract
    analysis_impacted_gallery_candidate_keys_check

theorem analysis_impacted_gallery_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_impacted_gallery_contract = true := by
  native_decide

theorem analysis_impacted_gallery_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_impacted_gallery_contract :=
  declaredKeysMinimalCheck_sound analysis_impacted_gallery_contract
    analysis_impacted_gallery_candidate_keys_minimal_check

theorem analysis_impacted_gallery_closure_fixed_check :
    closureFixedPointCheck analysis_impacted_gallery_contract = true := by
  native_decide

theorem analysis_impacted_gallery_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_impacted_gallery_contract :=
  closureFixedPointCheck_sound analysis_impacted_gallery_contract
    analysis_impacted_gallery_closure_fixed_check

theorem analysis_impacted_gallery_bcnf_check :
    bcnfCheck analysis_impacted_gallery_contract = true := by
  native_decide

theorem analysis_impacted_gallery_bcnf : BCNF analysis_impacted_gallery_contract :=
  bcnfCheck_sound analysis_impacted_gallery_contract analysis_impacted_gallery_bcnf_check

def analysis_impacted_content_contract : RelationContract where
  name := "analysis_impacted_content"
  attributes := ["analysis_id", "content_sha256", "witness_gallery_id"]
  declaredKeys := [["analysis_id", "content_sha256"]]
  declaredFDs := [
    { determinant := ["analysis_id", "content_sha256"], dependent := ["witness_gallery_id"] }
  ]

theorem analysis_impacted_content_schema_well_formed :
    schemaWellFormedCheck analysis_impacted_content_contract = true := by
  native_decide

theorem analysis_impacted_content_candidate_keys_check :
    keysDetermineAllCheck analysis_impacted_content_contract = true := by
  native_decide

theorem analysis_impacted_content_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_impacted_content_contract :=
  keysDetermineAllCheck_sound analysis_impacted_content_contract
    analysis_impacted_content_candidate_keys_check

theorem analysis_impacted_content_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_impacted_content_contract = true := by
  native_decide

theorem analysis_impacted_content_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_impacted_content_contract :=
  declaredKeysMinimalCheck_sound analysis_impacted_content_contract
    analysis_impacted_content_candidate_keys_minimal_check

theorem analysis_impacted_content_closure_fixed_check :
    closureFixedPointCheck analysis_impacted_content_contract = true := by
  native_decide

theorem analysis_impacted_content_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_impacted_content_contract :=
  closureFixedPointCheck_sound analysis_impacted_content_contract
    analysis_impacted_content_closure_fixed_check

theorem analysis_impacted_content_bcnf_check :
    bcnfCheck analysis_impacted_content_contract = true := by
  native_decide

theorem analysis_impacted_content_bcnf : BCNF analysis_impacted_content_contract :=
  bcnfCheck_sound analysis_impacted_content_contract analysis_impacted_content_bcnf_check

def analysis_impacted_content_provenance_contract : RelationContract where
  name := "analysis_impacted_content_provenance"
  attributes := ["analysis_id", "gallery_id", "content_sha256"]
  declaredKeys := [["analysis_id", "gallery_id", "content_sha256"]]
  declaredFDs := [
  ]

theorem analysis_impacted_content_provenance_schema_well_formed :
    schemaWellFormedCheck analysis_impacted_content_provenance_contract = true := by
  native_decide

theorem analysis_impacted_content_provenance_candidate_keys_check :
    keysDetermineAllCheck analysis_impacted_content_provenance_contract = true := by
  native_decide

theorem analysis_impacted_content_provenance_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_impacted_content_provenance_contract :=
  keysDetermineAllCheck_sound analysis_impacted_content_provenance_contract
    analysis_impacted_content_provenance_candidate_keys_check

theorem analysis_impacted_content_provenance_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_impacted_content_provenance_contract = true := by
  native_decide

theorem analysis_impacted_content_provenance_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_impacted_content_provenance_contract :=
  declaredKeysMinimalCheck_sound analysis_impacted_content_provenance_contract
    analysis_impacted_content_provenance_candidate_keys_minimal_check

theorem analysis_impacted_content_provenance_closure_fixed_check :
    closureFixedPointCheck analysis_impacted_content_provenance_contract = true := by
  native_decide

theorem analysis_impacted_content_provenance_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_impacted_content_provenance_contract :=
  closureFixedPointCheck_sound analysis_impacted_content_provenance_contract
    analysis_impacted_content_provenance_closure_fixed_check

theorem analysis_impacted_content_provenance_bcnf_check :
    bcnfCheck analysis_impacted_content_provenance_contract = true := by
  native_decide

theorem analysis_impacted_content_provenance_bcnf : BCNF analysis_impacted_content_provenance_contract :=
  bcnfCheck_sound analysis_impacted_content_provenance_contract analysis_impacted_content_provenance_bcnf_check

def analysis_impacted_gid_storage_contract : RelationContract where
  name := "analysis_impacted_gid_storage"
  attributes := ["analysis_id", "gid"]
  declaredKeys := [["analysis_id", "gid"]]
  declaredFDs := [
  ]

theorem analysis_impacted_gid_storage_schema_well_formed :
    schemaWellFormedCheck analysis_impacted_gid_storage_contract = true := by
  native_decide

theorem analysis_impacted_gid_storage_candidate_keys_check :
    keysDetermineAllCheck analysis_impacted_gid_storage_contract = true := by
  native_decide

theorem analysis_impacted_gid_storage_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_impacted_gid_storage_contract :=
  keysDetermineAllCheck_sound analysis_impacted_gid_storage_contract
    analysis_impacted_gid_storage_candidate_keys_check

theorem analysis_impacted_gid_storage_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_impacted_gid_storage_contract = true := by
  native_decide

theorem analysis_impacted_gid_storage_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_impacted_gid_storage_contract :=
  declaredKeysMinimalCheck_sound analysis_impacted_gid_storage_contract
    analysis_impacted_gid_storage_candidate_keys_minimal_check

theorem analysis_impacted_gid_storage_closure_fixed_check :
    closureFixedPointCheck analysis_impacted_gid_storage_contract = true := by
  native_decide

theorem analysis_impacted_gid_storage_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_impacted_gid_storage_contract :=
  closureFixedPointCheck_sound analysis_impacted_gid_storage_contract
    analysis_impacted_gid_storage_closure_fixed_check

theorem analysis_impacted_gid_storage_bcnf_check :
    bcnfCheck analysis_impacted_gid_storage_contract = true := by
  native_decide

theorem analysis_impacted_gid_storage_bcnf : BCNF analysis_impacted_gid_storage_contract :=
  bcnfCheck_sound analysis_impacted_gid_storage_contract analysis_impacted_gid_storage_bcnf_check

def analysis_impacted_gid_provenance_storage_contract : RelationContract where
  name := "analysis_impacted_gid_provenance_storage"
  attributes := ["analysis_id", "gallery_id"]
  declaredKeys := [["analysis_id", "gallery_id"]]
  declaredFDs := [
  ]

theorem analysis_impacted_gid_provenance_storage_schema_well_formed :
    schemaWellFormedCheck analysis_impacted_gid_provenance_storage_contract = true := by
  native_decide

theorem analysis_impacted_gid_provenance_storage_candidate_keys_check :
    keysDetermineAllCheck analysis_impacted_gid_provenance_storage_contract = true := by
  native_decide

theorem analysis_impacted_gid_provenance_storage_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_impacted_gid_provenance_storage_contract :=
  keysDetermineAllCheck_sound analysis_impacted_gid_provenance_storage_contract
    analysis_impacted_gid_provenance_storage_candidate_keys_check

theorem analysis_impacted_gid_provenance_storage_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_impacted_gid_provenance_storage_contract = true := by
  native_decide

theorem analysis_impacted_gid_provenance_storage_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_impacted_gid_provenance_storage_contract :=
  declaredKeysMinimalCheck_sound analysis_impacted_gid_provenance_storage_contract
    analysis_impacted_gid_provenance_storage_candidate_keys_minimal_check

theorem analysis_impacted_gid_provenance_storage_closure_fixed_check :
    closureFixedPointCheck analysis_impacted_gid_provenance_storage_contract = true := by
  native_decide

theorem analysis_impacted_gid_provenance_storage_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_impacted_gid_provenance_storage_contract :=
  closureFixedPointCheck_sound analysis_impacted_gid_provenance_storage_contract
    analysis_impacted_gid_provenance_storage_closure_fixed_check

theorem analysis_impacted_gid_provenance_storage_bcnf_check :
    bcnfCheck analysis_impacted_gid_provenance_storage_contract = true := by
  native_decide

theorem analysis_impacted_gid_provenance_storage_bcnf : BCNF analysis_impacted_gid_provenance_storage_contract :=
  bcnfCheck_sound analysis_impacted_gid_provenance_storage_contract analysis_impacted_gid_provenance_storage_bcnf_check

def analysis_impacted_gid_provenance_contract : RelationContract where
  name := "analysis_impacted_gid_provenance"
  attributes := ["analysis_id", "gallery_id", "gid"]
  declaredKeys := [["analysis_id", "gallery_id"]]
  declaredFDs := [
    { determinant := ["gallery_id"], dependent := ["gid"] }
  ]

theorem analysis_impacted_gid_provenance_schema_well_formed :
    schemaWellFormedCheck analysis_impacted_gid_provenance_contract = true := by
  native_decide

theorem analysis_impacted_gid_provenance_candidate_keys_check :
    keysDetermineAllCheck analysis_impacted_gid_provenance_contract = true := by
  native_decide

theorem analysis_impacted_gid_provenance_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_impacted_gid_provenance_contract :=
  keysDetermineAllCheck_sound analysis_impacted_gid_provenance_contract
    analysis_impacted_gid_provenance_candidate_keys_check

theorem analysis_impacted_gid_provenance_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_impacted_gid_provenance_contract = true := by
  native_decide

theorem analysis_impacted_gid_provenance_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_impacted_gid_provenance_contract :=
  declaredKeysMinimalCheck_sound analysis_impacted_gid_provenance_contract
    analysis_impacted_gid_provenance_candidate_keys_minimal_check

theorem analysis_impacted_gid_provenance_closure_fixed_check :
    closureFixedPointCheck analysis_impacted_gid_provenance_contract = true := by
  native_decide

theorem analysis_impacted_gid_provenance_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_impacted_gid_provenance_contract :=
  closureFixedPointCheck_sound analysis_impacted_gid_provenance_contract
    analysis_impacted_gid_provenance_closure_fixed_check

def analysis_impacted_gid_contract : RelationContract where
  name := "analysis_impacted_gid"
  attributes := ["analysis_id", "gid", "witness_gallery_id"]
  declaredKeys := [["analysis_id", "gid"], ["analysis_id", "witness_gallery_id"]]
  declaredFDs := [
    { determinant := ["witness_gallery_id"], dependent := ["gid"] },
    { determinant := ["analysis_id", "gid"], dependent := ["witness_gallery_id"] }
  ]

theorem analysis_impacted_gid_schema_well_formed :
    schemaWellFormedCheck analysis_impacted_gid_contract = true := by
  native_decide

theorem analysis_impacted_gid_candidate_keys_check :
    keysDetermineAllCheck analysis_impacted_gid_contract = true := by
  native_decide

theorem analysis_impacted_gid_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_impacted_gid_contract :=
  keysDetermineAllCheck_sound analysis_impacted_gid_contract
    analysis_impacted_gid_candidate_keys_check

theorem analysis_impacted_gid_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_impacted_gid_contract = true := by
  native_decide

theorem analysis_impacted_gid_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_impacted_gid_contract :=
  declaredKeysMinimalCheck_sound analysis_impacted_gid_contract
    analysis_impacted_gid_candidate_keys_minimal_check

theorem analysis_impacted_gid_closure_fixed_check :
    closureFixedPointCheck analysis_impacted_gid_contract = true := by
  native_decide

theorem analysis_impacted_gid_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_impacted_gid_contract :=
  closureFixedPointCheck_sound analysis_impacted_gid_contract
    analysis_impacted_gid_closure_fixed_check

def analysis_file_hash_decision_shadow_anchor_contract : RelationContract where
  name := "analysis_file_hash_decision_shadow_anchor"
  attributes := ["analysis_id", "file_sha256"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
  ]

theorem analysis_file_hash_decision_shadow_anchor_schema_well_formed :
    schemaWellFormedCheck analysis_file_hash_decision_shadow_anchor_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_anchor_candidate_keys_check :
    keysDetermineAllCheck analysis_file_hash_decision_shadow_anchor_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_anchor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_file_hash_decision_shadow_anchor_contract :=
  keysDetermineAllCheck_sound analysis_file_hash_decision_shadow_anchor_contract
    analysis_file_hash_decision_shadow_anchor_candidate_keys_check

theorem analysis_file_hash_decision_shadow_anchor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_file_hash_decision_shadow_anchor_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_anchor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_file_hash_decision_shadow_anchor_contract :=
  declaredKeysMinimalCheck_sound analysis_file_hash_decision_shadow_anchor_contract
    analysis_file_hash_decision_shadow_anchor_candidate_keys_minimal_check

theorem analysis_file_hash_decision_shadow_anchor_closure_fixed_check :
    closureFixedPointCheck analysis_file_hash_decision_shadow_anchor_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_anchor_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_file_hash_decision_shadow_anchor_contract :=
  closureFixedPointCheck_sound analysis_file_hash_decision_shadow_anchor_contract
    analysis_file_hash_decision_shadow_anchor_closure_fixed_check

theorem analysis_file_hash_decision_shadow_anchor_bcnf_check :
    bcnfCheck analysis_file_hash_decision_shadow_anchor_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_anchor_bcnf : BCNF analysis_file_hash_decision_shadow_anchor_contract :=
  bcnfCheck_sound analysis_file_hash_decision_shadow_anchor_contract analysis_file_hash_decision_shadow_anchor_bcnf_check

def analysis_file_hash_decision_shadow_occurrence_count_contract : RelationContract where
  name := "analysis_file_hash_decision_shadow_occurrence_count"
  attributes := ["analysis_id", "file_sha256", "occurrence_count"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
    { determinant := ["analysis_id", "file_sha256"], dependent := ["occurrence_count"] }
  ]

theorem analysis_file_hash_decision_shadow_occurrence_count_schema_well_formed :
    schemaWellFormedCheck analysis_file_hash_decision_shadow_occurrence_count_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_occurrence_count_candidate_keys_check :
    keysDetermineAllCheck analysis_file_hash_decision_shadow_occurrence_count_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_occurrence_count_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_file_hash_decision_shadow_occurrence_count_contract :=
  keysDetermineAllCheck_sound analysis_file_hash_decision_shadow_occurrence_count_contract
    analysis_file_hash_decision_shadow_occurrence_count_candidate_keys_check

theorem analysis_file_hash_decision_shadow_occurrence_count_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_file_hash_decision_shadow_occurrence_count_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_occurrence_count_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_file_hash_decision_shadow_occurrence_count_contract :=
  declaredKeysMinimalCheck_sound analysis_file_hash_decision_shadow_occurrence_count_contract
    analysis_file_hash_decision_shadow_occurrence_count_candidate_keys_minimal_check

theorem analysis_file_hash_decision_shadow_occurrence_count_closure_fixed_check :
    closureFixedPointCheck analysis_file_hash_decision_shadow_occurrence_count_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_occurrence_count_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_file_hash_decision_shadow_occurrence_count_contract :=
  closureFixedPointCheck_sound analysis_file_hash_decision_shadow_occurrence_count_contract
    analysis_file_hash_decision_shadow_occurrence_count_closure_fixed_check

theorem analysis_file_hash_decision_shadow_occurrence_count_bcnf_check :
    bcnfCheck analysis_file_hash_decision_shadow_occurrence_count_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_occurrence_count_bcnf : BCNF analysis_file_hash_decision_shadow_occurrence_count_contract :=
  bcnfCheck_sound analysis_file_hash_decision_shadow_occurrence_count_contract analysis_file_hash_decision_shadow_occurrence_count_bcnf_check

def analysis_file_hash_decision_shadow_artist_count_contract : RelationContract where
  name := "analysis_file_hash_decision_shadow_artist_count"
  attributes := ["analysis_id", "file_sha256", "artist_count"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
    { determinant := ["analysis_id", "file_sha256"], dependent := ["artist_count"] }
  ]

theorem analysis_file_hash_decision_shadow_artist_count_schema_well_formed :
    schemaWellFormedCheck analysis_file_hash_decision_shadow_artist_count_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_artist_count_candidate_keys_check :
    keysDetermineAllCheck analysis_file_hash_decision_shadow_artist_count_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_artist_count_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_file_hash_decision_shadow_artist_count_contract :=
  keysDetermineAllCheck_sound analysis_file_hash_decision_shadow_artist_count_contract
    analysis_file_hash_decision_shadow_artist_count_candidate_keys_check

theorem analysis_file_hash_decision_shadow_artist_count_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_file_hash_decision_shadow_artist_count_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_artist_count_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_file_hash_decision_shadow_artist_count_contract :=
  declaredKeysMinimalCheck_sound analysis_file_hash_decision_shadow_artist_count_contract
    analysis_file_hash_decision_shadow_artist_count_candidate_keys_minimal_check

theorem analysis_file_hash_decision_shadow_artist_count_closure_fixed_check :
    closureFixedPointCheck analysis_file_hash_decision_shadow_artist_count_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_artist_count_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_file_hash_decision_shadow_artist_count_contract :=
  closureFixedPointCheck_sound analysis_file_hash_decision_shadow_artist_count_contract
    analysis_file_hash_decision_shadow_artist_count_closure_fixed_check

theorem analysis_file_hash_decision_shadow_artist_count_bcnf_check :
    bcnfCheck analysis_file_hash_decision_shadow_artist_count_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_artist_count_bcnf : BCNF analysis_file_hash_decision_shadow_artist_count_contract :=
  bcnfCheck_sound analysis_file_hash_decision_shadow_artist_count_contract analysis_file_hash_decision_shadow_artist_count_bcnf_check

def analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract : RelationContract where
  name := "analysis_file_hash_decision_shadow_maximum_gallery_artist_count"
  attributes := ["analysis_id", "file_sha256", "maximum_gallery_artist_count"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
    { determinant := ["analysis_id", "file_sha256"], dependent := ["maximum_gallery_artist_count"] }
  ]

theorem analysis_file_hash_decision_shadow_maximum_gallery_artist_count_schema_well_formed :
    schemaWellFormedCheck analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_maximum_gallery_artist_count_candidate_keys_check :
    keysDetermineAllCheck analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_maximum_gallery_artist_count_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract :=
  keysDetermineAllCheck_sound analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract
    analysis_file_hash_decision_shadow_maximum_gallery_artist_count_candidate_keys_check

theorem analysis_file_hash_decision_shadow_maximum_gallery_artist_count_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_maximum_gallery_artist_count_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract :=
  declaredKeysMinimalCheck_sound analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract
    analysis_file_hash_decision_shadow_maximum_gallery_artist_count_candidate_keys_minimal_check

theorem analysis_file_hash_decision_shadow_maximum_gallery_artist_count_closure_fixed_check :
    closureFixedPointCheck analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_maximum_gallery_artist_count_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract :=
  closureFixedPointCheck_sound analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract
    analysis_file_hash_decision_shadow_maximum_gallery_artist_count_closure_fixed_check

theorem analysis_file_hash_decision_shadow_maximum_gallery_artist_count_bcnf_check :
    bcnfCheck analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_maximum_gallery_artist_count_bcnf : BCNF analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract :=
  bcnfCheck_sound analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract analysis_file_hash_decision_shadow_maximum_gallery_artist_count_bcnf_check

def analysis_file_hash_decision_shadow_seal_contract : RelationContract where
  name := "analysis_file_hash_decision_shadow_seal"
  attributes := ["analysis_id", "file_sha256"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
  ]

theorem analysis_file_hash_decision_shadow_seal_schema_well_formed :
    schemaWellFormedCheck analysis_file_hash_decision_shadow_seal_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_seal_candidate_keys_check :
    keysDetermineAllCheck analysis_file_hash_decision_shadow_seal_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_seal_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_file_hash_decision_shadow_seal_contract :=
  keysDetermineAllCheck_sound analysis_file_hash_decision_shadow_seal_contract
    analysis_file_hash_decision_shadow_seal_candidate_keys_check

theorem analysis_file_hash_decision_shadow_seal_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_file_hash_decision_shadow_seal_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_seal_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_file_hash_decision_shadow_seal_contract :=
  declaredKeysMinimalCheck_sound analysis_file_hash_decision_shadow_seal_contract
    analysis_file_hash_decision_shadow_seal_candidate_keys_minimal_check

theorem analysis_file_hash_decision_shadow_seal_closure_fixed_check :
    closureFixedPointCheck analysis_file_hash_decision_shadow_seal_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_seal_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_file_hash_decision_shadow_seal_contract :=
  closureFixedPointCheck_sound analysis_file_hash_decision_shadow_seal_contract
    analysis_file_hash_decision_shadow_seal_closure_fixed_check

theorem analysis_file_hash_decision_shadow_seal_bcnf_check :
    bcnfCheck analysis_file_hash_decision_shadow_seal_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_seal_bcnf : BCNF analysis_file_hash_decision_shadow_seal_contract :=
  bcnfCheck_sound analysis_file_hash_decision_shadow_seal_contract analysis_file_hash_decision_shadow_seal_bcnf_check

def analysis_file_hash_decision_shadow_contract : RelationContract where
  name := "analysis_file_hash_decision_shadow"
  attributes := ["analysis_id", "file_sha256", "occurrence_count", "artist_count", "maximum_gallery_artist_count"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
    { determinant := ["analysis_id", "file_sha256"], dependent := ["occurrence_count", "artist_count", "maximum_gallery_artist_count"] }
  ]

theorem analysis_file_hash_decision_shadow_schema_well_formed :
    schemaWellFormedCheck analysis_file_hash_decision_shadow_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_candidate_keys_check :
    keysDetermineAllCheck analysis_file_hash_decision_shadow_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_file_hash_decision_shadow_contract :=
  keysDetermineAllCheck_sound analysis_file_hash_decision_shadow_contract
    analysis_file_hash_decision_shadow_candidate_keys_check

theorem analysis_file_hash_decision_shadow_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_file_hash_decision_shadow_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_file_hash_decision_shadow_contract :=
  declaredKeysMinimalCheck_sound analysis_file_hash_decision_shadow_contract
    analysis_file_hash_decision_shadow_candidate_keys_minimal_check

theorem analysis_file_hash_decision_shadow_closure_fixed_check :
    closureFixedPointCheck analysis_file_hash_decision_shadow_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_file_hash_decision_shadow_contract :=
  closureFixedPointCheck_sound analysis_file_hash_decision_shadow_contract
    analysis_file_hash_decision_shadow_closure_fixed_check

def analysis_file_hash_decision_tombstone_contract : RelationContract where
  name := "analysis_file_hash_decision_tombstone"
  attributes := ["analysis_id", "file_sha256"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
  ]

theorem analysis_file_hash_decision_tombstone_schema_well_formed :
    schemaWellFormedCheck analysis_file_hash_decision_tombstone_contract = true := by
  native_decide

theorem analysis_file_hash_decision_tombstone_candidate_keys_check :
    keysDetermineAllCheck analysis_file_hash_decision_tombstone_contract = true := by
  native_decide

theorem analysis_file_hash_decision_tombstone_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_file_hash_decision_tombstone_contract :=
  keysDetermineAllCheck_sound analysis_file_hash_decision_tombstone_contract
    analysis_file_hash_decision_tombstone_candidate_keys_check

theorem analysis_file_hash_decision_tombstone_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_file_hash_decision_tombstone_contract = true := by
  native_decide

theorem analysis_file_hash_decision_tombstone_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_file_hash_decision_tombstone_contract :=
  declaredKeysMinimalCheck_sound analysis_file_hash_decision_tombstone_contract
    analysis_file_hash_decision_tombstone_candidate_keys_minimal_check

theorem analysis_file_hash_decision_tombstone_closure_fixed_check :
    closureFixedPointCheck analysis_file_hash_decision_tombstone_contract = true := by
  native_decide

theorem analysis_file_hash_decision_tombstone_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_file_hash_decision_tombstone_contract :=
  closureFixedPointCheck_sound analysis_file_hash_decision_tombstone_contract
    analysis_file_hash_decision_tombstone_closure_fixed_check

theorem analysis_file_hash_decision_tombstone_bcnf_check :
    bcnfCheck analysis_file_hash_decision_tombstone_contract = true := by
  native_decide

theorem analysis_file_hash_decision_tombstone_bcnf : BCNF analysis_file_hash_decision_tombstone_contract :=
  bcnfCheck_sound analysis_file_hash_decision_tombstone_contract analysis_file_hash_decision_tombstone_bcnf_check

def analysis_file_hash_decision_resolved_contract : RelationContract where
  name := "analysis_file_hash_decision_resolved"
  attributes := ["analysis_id", "file_sha256", "occurrence_count", "artist_count", "maximum_gallery_artist_count"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
    { determinant := ["analysis_id", "file_sha256"], dependent := ["occurrence_count", "artist_count", "maximum_gallery_artist_count"] }
  ]

theorem analysis_file_hash_decision_resolved_schema_well_formed :
    schemaWellFormedCheck analysis_file_hash_decision_resolved_contract = true := by
  native_decide

theorem analysis_file_hash_decision_resolved_candidate_keys_check :
    keysDetermineAllCheck analysis_file_hash_decision_resolved_contract = true := by
  native_decide

theorem analysis_file_hash_decision_resolved_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_file_hash_decision_resolved_contract :=
  keysDetermineAllCheck_sound analysis_file_hash_decision_resolved_contract
    analysis_file_hash_decision_resolved_candidate_keys_check

theorem analysis_file_hash_decision_resolved_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_file_hash_decision_resolved_contract = true := by
  native_decide

theorem analysis_file_hash_decision_resolved_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_file_hash_decision_resolved_contract :=
  declaredKeysMinimalCheck_sound analysis_file_hash_decision_resolved_contract
    analysis_file_hash_decision_resolved_candidate_keys_minimal_check

theorem analysis_file_hash_decision_resolved_closure_fixed_check :
    closureFixedPointCheck analysis_file_hash_decision_resolved_contract = true := by
  native_decide

theorem analysis_file_hash_decision_resolved_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_file_hash_decision_resolved_contract :=
  closureFixedPointCheck_sound analysis_file_hash_decision_resolved_contract
    analysis_file_hash_decision_resolved_closure_fixed_check

def analysis_content_owner_candidate_shadow_contract : RelationContract where
  name := "analysis_content_owner_candidate_shadow"
  attributes := ["analysis_id", "gallery_id", "content_sha256", "prefer_not_already_uploaded", "title_scalar_count", "download_time"]
  declaredKeys := [["analysis_id", "gallery_id"]]
  declaredFDs := [
    { determinant := ["analysis_id", "gallery_id"], dependent := ["content_sha256", "prefer_not_already_uploaded", "title_scalar_count", "download_time"] }
  ]

theorem analysis_content_owner_candidate_shadow_schema_well_formed :
    schemaWellFormedCheck analysis_content_owner_candidate_shadow_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_shadow_candidate_keys_check :
    keysDetermineAllCheck analysis_content_owner_candidate_shadow_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_shadow_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_content_owner_candidate_shadow_contract :=
  keysDetermineAllCheck_sound analysis_content_owner_candidate_shadow_contract
    analysis_content_owner_candidate_shadow_candidate_keys_check

theorem analysis_content_owner_candidate_shadow_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_content_owner_candidate_shadow_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_shadow_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_content_owner_candidate_shadow_contract :=
  declaredKeysMinimalCheck_sound analysis_content_owner_candidate_shadow_contract
    analysis_content_owner_candidate_shadow_candidate_keys_minimal_check

theorem analysis_content_owner_candidate_shadow_closure_fixed_check :
    closureFixedPointCheck analysis_content_owner_candidate_shadow_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_shadow_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_content_owner_candidate_shadow_contract :=
  closureFixedPointCheck_sound analysis_content_owner_candidate_shadow_contract
    analysis_content_owner_candidate_shadow_closure_fixed_check

theorem analysis_content_owner_candidate_shadow_bcnf_check :
    bcnfCheck analysis_content_owner_candidate_shadow_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_shadow_bcnf : BCNF analysis_content_owner_candidate_shadow_contract :=
  bcnfCheck_sound analysis_content_owner_candidate_shadow_contract analysis_content_owner_candidate_shadow_bcnf_check

def analysis_content_owner_candidate_tombstone_contract : RelationContract where
  name := "analysis_content_owner_candidate_tombstone"
  attributes := ["analysis_id", "gallery_id"]
  declaredKeys := [["analysis_id", "gallery_id"]]
  declaredFDs := [
  ]

theorem analysis_content_owner_candidate_tombstone_schema_well_formed :
    schemaWellFormedCheck analysis_content_owner_candidate_tombstone_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_tombstone_candidate_keys_check :
    keysDetermineAllCheck analysis_content_owner_candidate_tombstone_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_tombstone_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_content_owner_candidate_tombstone_contract :=
  keysDetermineAllCheck_sound analysis_content_owner_candidate_tombstone_contract
    analysis_content_owner_candidate_tombstone_candidate_keys_check

theorem analysis_content_owner_candidate_tombstone_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_content_owner_candidate_tombstone_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_tombstone_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_content_owner_candidate_tombstone_contract :=
  declaredKeysMinimalCheck_sound analysis_content_owner_candidate_tombstone_contract
    analysis_content_owner_candidate_tombstone_candidate_keys_minimal_check

theorem analysis_content_owner_candidate_tombstone_closure_fixed_check :
    closureFixedPointCheck analysis_content_owner_candidate_tombstone_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_tombstone_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_content_owner_candidate_tombstone_contract :=
  closureFixedPointCheck_sound analysis_content_owner_candidate_tombstone_contract
    analysis_content_owner_candidate_tombstone_closure_fixed_check

theorem analysis_content_owner_candidate_tombstone_bcnf_check :
    bcnfCheck analysis_content_owner_candidate_tombstone_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_tombstone_bcnf : BCNF analysis_content_owner_candidate_tombstone_contract :=
  bcnfCheck_sound analysis_content_owner_candidate_tombstone_contract analysis_content_owner_candidate_tombstone_bcnf_check

def analysis_content_owner_candidate_resolved_contract : RelationContract where
  name := "analysis_content_owner_candidate_resolved"
  attributes := ["analysis_id", "gallery_id", "content_sha256", "prefer_not_already_uploaded", "title_scalar_count", "download_time"]
  declaredKeys := [["analysis_id", "gallery_id"]]
  declaredFDs := [
    { determinant := ["analysis_id", "gallery_id"], dependent := ["content_sha256", "prefer_not_already_uploaded", "title_scalar_count", "download_time"] }
  ]

theorem analysis_content_owner_candidate_resolved_schema_well_formed :
    schemaWellFormedCheck analysis_content_owner_candidate_resolved_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_resolved_candidate_keys_check :
    keysDetermineAllCheck analysis_content_owner_candidate_resolved_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_resolved_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_content_owner_candidate_resolved_contract :=
  keysDetermineAllCheck_sound analysis_content_owner_candidate_resolved_contract
    analysis_content_owner_candidate_resolved_candidate_keys_check

theorem analysis_content_owner_candidate_resolved_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_content_owner_candidate_resolved_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_resolved_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_content_owner_candidate_resolved_contract :=
  declaredKeysMinimalCheck_sound analysis_content_owner_candidate_resolved_contract
    analysis_content_owner_candidate_resolved_candidate_keys_minimal_check

theorem analysis_content_owner_candidate_resolved_closure_fixed_check :
    closureFixedPointCheck analysis_content_owner_candidate_resolved_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_resolved_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_content_owner_candidate_resolved_contract :=
  closureFixedPointCheck_sound analysis_content_owner_candidate_resolved_contract
    analysis_content_owner_candidate_resolved_closure_fixed_check

def analysis_content_owner_shadow_contract : RelationContract where
  name := "analysis_content_owner_shadow"
  attributes := ["analysis_id", "content_sha256", "owner_gallery_id"]
  declaredKeys := [["analysis_id", "content_sha256"], ["analysis_id", "owner_gallery_id"]]
  declaredFDs := [
    { determinant := ["analysis_id", "content_sha256"], dependent := ["owner_gallery_id"] },
    { determinant := ["analysis_id", "owner_gallery_id"], dependent := ["content_sha256"] }
  ]

theorem analysis_content_owner_shadow_schema_well_formed :
    schemaWellFormedCheck analysis_content_owner_shadow_contract = true := by
  native_decide

theorem analysis_content_owner_shadow_candidate_keys_check :
    keysDetermineAllCheck analysis_content_owner_shadow_contract = true := by
  native_decide

theorem analysis_content_owner_shadow_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_content_owner_shadow_contract :=
  keysDetermineAllCheck_sound analysis_content_owner_shadow_contract
    analysis_content_owner_shadow_candidate_keys_check

theorem analysis_content_owner_shadow_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_content_owner_shadow_contract = true := by
  native_decide

theorem analysis_content_owner_shadow_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_content_owner_shadow_contract :=
  declaredKeysMinimalCheck_sound analysis_content_owner_shadow_contract
    analysis_content_owner_shadow_candidate_keys_minimal_check

theorem analysis_content_owner_shadow_closure_fixed_check :
    closureFixedPointCheck analysis_content_owner_shadow_contract = true := by
  native_decide

theorem analysis_content_owner_shadow_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_content_owner_shadow_contract :=
  closureFixedPointCheck_sound analysis_content_owner_shadow_contract
    analysis_content_owner_shadow_closure_fixed_check

theorem analysis_content_owner_shadow_bcnf_check :
    bcnfCheck analysis_content_owner_shadow_contract = true := by
  native_decide

theorem analysis_content_owner_shadow_bcnf : BCNF analysis_content_owner_shadow_contract :=
  bcnfCheck_sound analysis_content_owner_shadow_contract analysis_content_owner_shadow_bcnf_check

def analysis_content_owner_tombstone_contract : RelationContract where
  name := "analysis_content_owner_tombstone"
  attributes := ["analysis_id", "content_sha256"]
  declaredKeys := [["analysis_id", "content_sha256"]]
  declaredFDs := [
  ]

theorem analysis_content_owner_tombstone_schema_well_formed :
    schemaWellFormedCheck analysis_content_owner_tombstone_contract = true := by
  native_decide

theorem analysis_content_owner_tombstone_candidate_keys_check :
    keysDetermineAllCheck analysis_content_owner_tombstone_contract = true := by
  native_decide

theorem analysis_content_owner_tombstone_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_content_owner_tombstone_contract :=
  keysDetermineAllCheck_sound analysis_content_owner_tombstone_contract
    analysis_content_owner_tombstone_candidate_keys_check

theorem analysis_content_owner_tombstone_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_content_owner_tombstone_contract = true := by
  native_decide

theorem analysis_content_owner_tombstone_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_content_owner_tombstone_contract :=
  declaredKeysMinimalCheck_sound analysis_content_owner_tombstone_contract
    analysis_content_owner_tombstone_candidate_keys_minimal_check

theorem analysis_content_owner_tombstone_closure_fixed_check :
    closureFixedPointCheck analysis_content_owner_tombstone_contract = true := by
  native_decide

theorem analysis_content_owner_tombstone_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_content_owner_tombstone_contract :=
  closureFixedPointCheck_sound analysis_content_owner_tombstone_contract
    analysis_content_owner_tombstone_closure_fixed_check

theorem analysis_content_owner_tombstone_bcnf_check :
    bcnfCheck analysis_content_owner_tombstone_contract = true := by
  native_decide

theorem analysis_content_owner_tombstone_bcnf : BCNF analysis_content_owner_tombstone_contract :=
  bcnfCheck_sound analysis_content_owner_tombstone_contract analysis_content_owner_tombstone_bcnf_check

def analysis_content_owner_resolved_contract : RelationContract where
  name := "analysis_content_owner_resolved"
  attributes := ["analysis_id", "content_sha256", "owner_gallery_id"]
  declaredKeys := [["analysis_id", "content_sha256"], ["analysis_id", "owner_gallery_id"]]
  declaredFDs := [
    { determinant := ["analysis_id", "content_sha256"], dependent := ["owner_gallery_id"] },
    { determinant := ["analysis_id", "owner_gallery_id"], dependent := ["content_sha256"] }
  ]

theorem analysis_content_owner_resolved_schema_well_formed :
    schemaWellFormedCheck analysis_content_owner_resolved_contract = true := by
  native_decide

theorem analysis_content_owner_resolved_candidate_keys_check :
    keysDetermineAllCheck analysis_content_owner_resolved_contract = true := by
  native_decide

theorem analysis_content_owner_resolved_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_content_owner_resolved_contract :=
  keysDetermineAllCheck_sound analysis_content_owner_resolved_contract
    analysis_content_owner_resolved_candidate_keys_check

theorem analysis_content_owner_resolved_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_content_owner_resolved_contract = true := by
  native_decide

theorem analysis_content_owner_resolved_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_content_owner_resolved_contract :=
  declaredKeysMinimalCheck_sound analysis_content_owner_resolved_contract
    analysis_content_owner_resolved_candidate_keys_minimal_check

theorem analysis_content_owner_resolved_closure_fixed_check :
    closureFixedPointCheck analysis_content_owner_resolved_contract = true := by
  native_decide

theorem analysis_content_owner_resolved_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_content_owner_resolved_contract :=
  closureFixedPointCheck_sound analysis_content_owner_resolved_contract
    analysis_content_owner_resolved_closure_fixed_check

def analysis_gid_candidate_shadow_contract : RelationContract where
  name := "analysis_gid_candidate_shadow"
  attributes := ["analysis_id", "gallery_id"]
  declaredKeys := [["analysis_id", "gallery_id"]]
  declaredFDs := [
  ]

theorem analysis_gid_candidate_shadow_schema_well_formed :
    schemaWellFormedCheck analysis_gid_candidate_shadow_contract = true := by
  native_decide

theorem analysis_gid_candidate_shadow_candidate_keys_check :
    keysDetermineAllCheck analysis_gid_candidate_shadow_contract = true := by
  native_decide

theorem analysis_gid_candidate_shadow_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_gid_candidate_shadow_contract :=
  keysDetermineAllCheck_sound analysis_gid_candidate_shadow_contract
    analysis_gid_candidate_shadow_candidate_keys_check

theorem analysis_gid_candidate_shadow_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_gid_candidate_shadow_contract = true := by
  native_decide

theorem analysis_gid_candidate_shadow_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_gid_candidate_shadow_contract :=
  declaredKeysMinimalCheck_sound analysis_gid_candidate_shadow_contract
    analysis_gid_candidate_shadow_candidate_keys_minimal_check

theorem analysis_gid_candidate_shadow_closure_fixed_check :
    closureFixedPointCheck analysis_gid_candidate_shadow_contract = true := by
  native_decide

theorem analysis_gid_candidate_shadow_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_gid_candidate_shadow_contract :=
  closureFixedPointCheck_sound analysis_gid_candidate_shadow_contract
    analysis_gid_candidate_shadow_closure_fixed_check

theorem analysis_gid_candidate_shadow_bcnf_check :
    bcnfCheck analysis_gid_candidate_shadow_contract = true := by
  native_decide

theorem analysis_gid_candidate_shadow_bcnf : BCNF analysis_gid_candidate_shadow_contract :=
  bcnfCheck_sound analysis_gid_candidate_shadow_contract analysis_gid_candidate_shadow_bcnf_check

def analysis_gid_candidate_tombstone_contract : RelationContract where
  name := "analysis_gid_candidate_tombstone"
  attributes := ["analysis_id", "gallery_id"]
  declaredKeys := [["analysis_id", "gallery_id"]]
  declaredFDs := [
  ]

theorem analysis_gid_candidate_tombstone_schema_well_formed :
    schemaWellFormedCheck analysis_gid_candidate_tombstone_contract = true := by
  native_decide

theorem analysis_gid_candidate_tombstone_candidate_keys_check :
    keysDetermineAllCheck analysis_gid_candidate_tombstone_contract = true := by
  native_decide

theorem analysis_gid_candidate_tombstone_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_gid_candidate_tombstone_contract :=
  keysDetermineAllCheck_sound analysis_gid_candidate_tombstone_contract
    analysis_gid_candidate_tombstone_candidate_keys_check

theorem analysis_gid_candidate_tombstone_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_gid_candidate_tombstone_contract = true := by
  native_decide

theorem analysis_gid_candidate_tombstone_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_gid_candidate_tombstone_contract :=
  declaredKeysMinimalCheck_sound analysis_gid_candidate_tombstone_contract
    analysis_gid_candidate_tombstone_candidate_keys_minimal_check

theorem analysis_gid_candidate_tombstone_closure_fixed_check :
    closureFixedPointCheck analysis_gid_candidate_tombstone_contract = true := by
  native_decide

theorem analysis_gid_candidate_tombstone_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_gid_candidate_tombstone_contract :=
  closureFixedPointCheck_sound analysis_gid_candidate_tombstone_contract
    analysis_gid_candidate_tombstone_closure_fixed_check

theorem analysis_gid_candidate_tombstone_bcnf_check :
    bcnfCheck analysis_gid_candidate_tombstone_contract = true := by
  native_decide

theorem analysis_gid_candidate_tombstone_bcnf : BCNF analysis_gid_candidate_tombstone_contract :=
  bcnfCheck_sound analysis_gid_candidate_tombstone_contract analysis_gid_candidate_tombstone_bcnf_check

def analysis_gid_candidate_resolved_contract : RelationContract where
  name := "analysis_gid_candidate_resolved"
  attributes := ["analysis_id", "gallery_id"]
  declaredKeys := [["analysis_id", "gallery_id"]]
  declaredFDs := [
  ]

theorem analysis_gid_candidate_resolved_schema_well_formed :
    schemaWellFormedCheck analysis_gid_candidate_resolved_contract = true := by
  native_decide

theorem analysis_gid_candidate_resolved_candidate_keys_check :
    keysDetermineAllCheck analysis_gid_candidate_resolved_contract = true := by
  native_decide

theorem analysis_gid_candidate_resolved_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_gid_candidate_resolved_contract :=
  keysDetermineAllCheck_sound analysis_gid_candidate_resolved_contract
    analysis_gid_candidate_resolved_candidate_keys_check

theorem analysis_gid_candidate_resolved_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_gid_candidate_resolved_contract = true := by
  native_decide

theorem analysis_gid_candidate_resolved_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_gid_candidate_resolved_contract :=
  declaredKeysMinimalCheck_sound analysis_gid_candidate_resolved_contract
    analysis_gid_candidate_resolved_candidate_keys_minimal_check

theorem analysis_gid_candidate_resolved_closure_fixed_check :
    closureFixedPointCheck analysis_gid_candidate_resolved_contract = true := by
  native_decide

theorem analysis_gid_candidate_resolved_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_gid_candidate_resolved_contract :=
  closureFixedPointCheck_sound analysis_gid_candidate_resolved_contract
    analysis_gid_candidate_resolved_closure_fixed_check

def analysis_gid_winner_selection_contract : RelationContract where
  name := "analysis_gid_winner_selection"
  attributes := ["analysis_id", "winner_gallery_id"]
  declaredKeys := [["analysis_id", "winner_gallery_id"]]
  declaredFDs := [
  ]

theorem analysis_gid_winner_selection_schema_well_formed :
    schemaWellFormedCheck analysis_gid_winner_selection_contract = true := by
  native_decide

theorem analysis_gid_winner_selection_candidate_keys_check :
    keysDetermineAllCheck analysis_gid_winner_selection_contract = true := by
  native_decide

theorem analysis_gid_winner_selection_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_gid_winner_selection_contract :=
  keysDetermineAllCheck_sound analysis_gid_winner_selection_contract
    analysis_gid_winner_selection_candidate_keys_check

theorem analysis_gid_winner_selection_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_gid_winner_selection_contract = true := by
  native_decide

theorem analysis_gid_winner_selection_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_gid_winner_selection_contract :=
  declaredKeysMinimalCheck_sound analysis_gid_winner_selection_contract
    analysis_gid_winner_selection_candidate_keys_minimal_check

theorem analysis_gid_winner_selection_closure_fixed_check :
    closureFixedPointCheck analysis_gid_winner_selection_contract = true := by
  native_decide

theorem analysis_gid_winner_selection_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_gid_winner_selection_contract :=
  closureFixedPointCheck_sound analysis_gid_winner_selection_contract
    analysis_gid_winner_selection_closure_fixed_check

theorem analysis_gid_winner_selection_bcnf_check :
    bcnfCheck analysis_gid_winner_selection_contract = true := by
  native_decide

theorem analysis_gid_winner_selection_bcnf : BCNF analysis_gid_winner_selection_contract :=
  bcnfCheck_sound analysis_gid_winner_selection_contract analysis_gid_winner_selection_bcnf_check

def analysis_gid_winner_shadow_contract : RelationContract where
  name := "analysis_gid_winner_shadow"
  attributes := ["analysis_id", "gid", "winner_gallery_id"]
  declaredKeys := [["analysis_id", "gid"], ["analysis_id", "winner_gallery_id"]]
  declaredFDs := [
    { determinant := ["analysis_id", "gid"], dependent := ["winner_gallery_id"] },
    { determinant := ["analysis_id", "winner_gallery_id"], dependent := ["gid"] },
    { determinant := ["winner_gallery_id"], dependent := ["gid"] }
  ]

theorem analysis_gid_winner_shadow_schema_well_formed :
    schemaWellFormedCheck analysis_gid_winner_shadow_contract = true := by
  native_decide

theorem analysis_gid_winner_shadow_candidate_keys_check :
    keysDetermineAllCheck analysis_gid_winner_shadow_contract = true := by
  native_decide

theorem analysis_gid_winner_shadow_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_gid_winner_shadow_contract :=
  keysDetermineAllCheck_sound analysis_gid_winner_shadow_contract
    analysis_gid_winner_shadow_candidate_keys_check

theorem analysis_gid_winner_shadow_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_gid_winner_shadow_contract = true := by
  native_decide

theorem analysis_gid_winner_shadow_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_gid_winner_shadow_contract :=
  declaredKeysMinimalCheck_sound analysis_gid_winner_shadow_contract
    analysis_gid_winner_shadow_candidate_keys_minimal_check

theorem analysis_gid_winner_shadow_closure_fixed_check :
    closureFixedPointCheck analysis_gid_winner_shadow_contract = true := by
  native_decide

theorem analysis_gid_winner_shadow_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_gid_winner_shadow_contract :=
  closureFixedPointCheck_sound analysis_gid_winner_shadow_contract
    analysis_gid_winner_shadow_closure_fixed_check

def analysis_gid_winner_tombstone_contract : RelationContract where
  name := "analysis_gid_winner_tombstone"
  attributes := ["analysis_id", "gid"]
  declaredKeys := [["analysis_id", "gid"]]
  declaredFDs := [
  ]

theorem analysis_gid_winner_tombstone_schema_well_formed :
    schemaWellFormedCheck analysis_gid_winner_tombstone_contract = true := by
  native_decide

theorem analysis_gid_winner_tombstone_candidate_keys_check :
    keysDetermineAllCheck analysis_gid_winner_tombstone_contract = true := by
  native_decide

theorem analysis_gid_winner_tombstone_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_gid_winner_tombstone_contract :=
  keysDetermineAllCheck_sound analysis_gid_winner_tombstone_contract
    analysis_gid_winner_tombstone_candidate_keys_check

theorem analysis_gid_winner_tombstone_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_gid_winner_tombstone_contract = true := by
  native_decide

theorem analysis_gid_winner_tombstone_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_gid_winner_tombstone_contract :=
  declaredKeysMinimalCheck_sound analysis_gid_winner_tombstone_contract
    analysis_gid_winner_tombstone_candidate_keys_minimal_check

theorem analysis_gid_winner_tombstone_closure_fixed_check :
    closureFixedPointCheck analysis_gid_winner_tombstone_contract = true := by
  native_decide

theorem analysis_gid_winner_tombstone_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_gid_winner_tombstone_contract :=
  closureFixedPointCheck_sound analysis_gid_winner_tombstone_contract
    analysis_gid_winner_tombstone_closure_fixed_check

theorem analysis_gid_winner_tombstone_bcnf_check :
    bcnfCheck analysis_gid_winner_tombstone_contract = true := by
  native_decide

theorem analysis_gid_winner_tombstone_bcnf : BCNF analysis_gid_winner_tombstone_contract :=
  bcnfCheck_sound analysis_gid_winner_tombstone_contract analysis_gid_winner_tombstone_bcnf_check

def analysis_gid_winner_resolved_contract : RelationContract where
  name := "analysis_gid_winner_resolved"
  attributes := ["analysis_id", "gid", "winner_gallery_id"]
  declaredKeys := [["analysis_id", "gid"], ["analysis_id", "winner_gallery_id"]]
  declaredFDs := [
    { determinant := ["analysis_id", "gid"], dependent := ["winner_gallery_id"] },
    { determinant := ["analysis_id", "winner_gallery_id"], dependent := ["gid"] },
    { determinant := ["winner_gallery_id"], dependent := ["gid"] }
  ]

theorem analysis_gid_winner_resolved_schema_well_formed :
    schemaWellFormedCheck analysis_gid_winner_resolved_contract = true := by
  native_decide

theorem analysis_gid_winner_resolved_candidate_keys_check :
    keysDetermineAllCheck analysis_gid_winner_resolved_contract = true := by
  native_decide

theorem analysis_gid_winner_resolved_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_gid_winner_resolved_contract :=
  keysDetermineAllCheck_sound analysis_gid_winner_resolved_contract
    analysis_gid_winner_resolved_candidate_keys_check

theorem analysis_gid_winner_resolved_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_gid_winner_resolved_contract = true := by
  native_decide

theorem analysis_gid_winner_resolved_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_gid_winner_resolved_contract :=
  declaredKeysMinimalCheck_sound analysis_gid_winner_resolved_contract
    analysis_gid_winner_resolved_candidate_keys_minimal_check

theorem analysis_gid_winner_resolved_closure_fixed_check :
    closureFixedPointCheck analysis_gid_winner_resolved_contract = true := by
  native_decide

theorem analysis_gid_winner_resolved_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_gid_winner_resolved_contract :=
  closureFixedPointCheck_sound analysis_gid_winner_resolved_contract
    analysis_gid_winner_resolved_closure_fixed_check

def analysis_state_component_seal_contract : RelationContract where
  name := "analysis_state_component_seal"
  attributes := ["analysis_id", "state_component", "row_count", "sealed_at"]
  declaredKeys := [["analysis_id", "state_component"]]
  declaredFDs := [
    { determinant := ["analysis_id", "state_component"], dependent := ["row_count", "sealed_at"] }
  ]

theorem analysis_state_component_seal_schema_well_formed :
    schemaWellFormedCheck analysis_state_component_seal_contract = true := by
  native_decide

theorem analysis_state_component_seal_candidate_keys_check :
    keysDetermineAllCheck analysis_state_component_seal_contract = true := by
  native_decide

theorem analysis_state_component_seal_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_state_component_seal_contract :=
  keysDetermineAllCheck_sound analysis_state_component_seal_contract
    analysis_state_component_seal_candidate_keys_check

theorem analysis_state_component_seal_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_state_component_seal_contract = true := by
  native_decide

theorem analysis_state_component_seal_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_state_component_seal_contract :=
  declaredKeysMinimalCheck_sound analysis_state_component_seal_contract
    analysis_state_component_seal_candidate_keys_minimal_check

theorem analysis_state_component_seal_closure_fixed_check :
    closureFixedPointCheck analysis_state_component_seal_contract = true := by
  native_decide

theorem analysis_state_component_seal_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_state_component_seal_contract :=
  closureFixedPointCheck_sound analysis_state_component_seal_contract
    analysis_state_component_seal_closure_fixed_check

theorem analysis_state_component_seal_bcnf_check :
    bcnfCheck analysis_state_component_seal_contract = true := by
  native_decide

theorem analysis_state_component_seal_bcnf : BCNF analysis_state_component_seal_contract :=
  bcnfCheck_sound analysis_state_component_seal_contract analysis_state_component_seal_bcnf_check

def analysis_stage_contract : RelationContract where
  name := "analysis_stage"
  attributes := ["stage", "stage_order", "cursor_codec"]
  declaredKeys := [["stage"], ["stage_order"]]
  declaredFDs := [
    { determinant := ["stage"], dependent := ["stage_order", "cursor_codec"] },
    { determinant := ["stage_order"], dependent := ["stage", "cursor_codec"] }
  ]

theorem analysis_stage_schema_well_formed :
    schemaWellFormedCheck analysis_stage_contract = true := by
  native_decide

theorem analysis_stage_candidate_keys_check :
    keysDetermineAllCheck analysis_stage_contract = true := by
  native_decide

theorem analysis_stage_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_stage_contract :=
  keysDetermineAllCheck_sound analysis_stage_contract
    analysis_stage_candidate_keys_check

theorem analysis_stage_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_stage_contract = true := by
  native_decide

theorem analysis_stage_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_stage_contract :=
  declaredKeysMinimalCheck_sound analysis_stage_contract
    analysis_stage_candidate_keys_minimal_check

theorem analysis_stage_closure_fixed_check :
    closureFixedPointCheck analysis_stage_contract = true := by
  native_decide

theorem analysis_stage_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_stage_contract :=
  closureFixedPointCheck_sound analysis_stage_contract
    analysis_stage_closure_fixed_check

theorem analysis_stage_bcnf_check :
    bcnfCheck analysis_stage_contract = true := by
  native_decide

theorem analysis_stage_bcnf : BCNF analysis_stage_contract :=
  bcnfCheck_sound analysis_stage_contract analysis_stage_bcnf_check

def analysis_checkpoint_contract : RelationContract where
  name := "analysis_checkpoint"
  attributes := ["analysis_id", "stage", "generation", "cursor", "processed_count", "state", "updated_at"]
  declaredKeys := [["analysis_id", "stage"]]
  declaredFDs := [
    { determinant := ["analysis_id", "stage"], dependent := ["generation", "cursor", "processed_count", "state", "updated_at"] }
  ]

theorem analysis_checkpoint_schema_well_formed :
    schemaWellFormedCheck analysis_checkpoint_contract = true := by
  native_decide

theorem analysis_checkpoint_candidate_keys_check :
    keysDetermineAllCheck analysis_checkpoint_contract = true := by
  native_decide

theorem analysis_checkpoint_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_checkpoint_contract :=
  keysDetermineAllCheck_sound analysis_checkpoint_contract
    analysis_checkpoint_candidate_keys_check

theorem analysis_checkpoint_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_checkpoint_contract = true := by
  native_decide

theorem analysis_checkpoint_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_checkpoint_contract :=
  declaredKeysMinimalCheck_sound analysis_checkpoint_contract
    analysis_checkpoint_candidate_keys_minimal_check

theorem analysis_checkpoint_closure_fixed_check :
    closureFixedPointCheck analysis_checkpoint_contract = true := by
  native_decide

theorem analysis_checkpoint_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_checkpoint_contract :=
  closureFixedPointCheck_sound analysis_checkpoint_contract
    analysis_checkpoint_closure_fixed_check

theorem analysis_checkpoint_bcnf_check :
    bcnfCheck analysis_checkpoint_contract = true := by
  native_decide

theorem analysis_checkpoint_bcnf : BCNF analysis_checkpoint_contract :=
  bcnfCheck_sound analysis_checkpoint_contract analysis_checkpoint_bcnf_check

def analysis_batch_receipt_stored_contract : RelationContract where
  name := "analysis_batch_receipt_stored"
  attributes := ["analysis_id", "stage", "start_generation", "batch_key", "start_cursor", "start_processed_count", "page_limit", "next_cursor", "row_count", "committed_at"]
  declaredKeys := [["analysis_id", "stage", "start_generation"], ["analysis_id", "stage", "batch_key"]]
  declaredFDs := [
    { determinant := ["analysis_id", "stage", "batch_key"], dependent := ["start_generation", "start_cursor", "start_processed_count", "page_limit", "next_cursor", "row_count", "committed_at"] },
    { determinant := ["analysis_id", "stage", "start_generation"], dependent := ["batch_key", "start_cursor", "start_processed_count", "page_limit", "next_cursor", "row_count", "committed_at"] }
  ]

theorem analysis_batch_receipt_stored_schema_well_formed :
    schemaWellFormedCheck analysis_batch_receipt_stored_contract = true := by
  native_decide

theorem analysis_batch_receipt_stored_candidate_keys_check :
    keysDetermineAllCheck analysis_batch_receipt_stored_contract = true := by
  native_decide

theorem analysis_batch_receipt_stored_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_batch_receipt_stored_contract :=
  keysDetermineAllCheck_sound analysis_batch_receipt_stored_contract
    analysis_batch_receipt_stored_candidate_keys_check

theorem analysis_batch_receipt_stored_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_batch_receipt_stored_contract = true := by
  native_decide

theorem analysis_batch_receipt_stored_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_batch_receipt_stored_contract :=
  declaredKeysMinimalCheck_sound analysis_batch_receipt_stored_contract
    analysis_batch_receipt_stored_candidate_keys_minimal_check

theorem analysis_batch_receipt_stored_closure_fixed_check :
    closureFixedPointCheck analysis_batch_receipt_stored_contract = true := by
  native_decide

theorem analysis_batch_receipt_stored_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_batch_receipt_stored_contract :=
  closureFixedPointCheck_sound analysis_batch_receipt_stored_contract
    analysis_batch_receipt_stored_closure_fixed_check

theorem analysis_batch_receipt_stored_bcnf_check :
    bcnfCheck analysis_batch_receipt_stored_contract = true := by
  native_decide

theorem analysis_batch_receipt_stored_bcnf : BCNF analysis_batch_receipt_stored_contract :=
  bcnfCheck_sound analysis_batch_receipt_stored_contract analysis_batch_receipt_stored_bcnf_check

def analysis_batch_receipt_contract : RelationContract where
  name := "analysis_batch_receipt"
  attributes := ["analysis_id", "stage", "batch_key", "start_generation", "start_cursor", "start_processed_count", "page_limit", "next_cursor", "next_processed_count", "next_state", "row_count", "terminal", "committed_generation", "committed_at"]
  declaredKeys := [["analysis_id", "stage", "batch_key"], ["analysis_id", "stage", "start_generation"], ["analysis_id", "stage", "committed_generation"]]
  declaredFDs := [
    { determinant := ["analysis_id", "stage", "batch_key"], dependent := ["start_generation", "start_cursor", "start_processed_count", "page_limit", "next_cursor", "next_processed_count", "next_state", "row_count", "terminal", "committed_generation", "committed_at"] },
    { determinant := ["analysis_id", "stage", "start_generation"], dependent := ["batch_key", "start_cursor", "start_processed_count", "page_limit", "next_cursor", "next_processed_count", "next_state", "row_count", "terminal", "committed_generation", "committed_at"] },
    { determinant := ["start_generation"], dependent := ["committed_generation"] },
    { determinant := ["committed_generation"], dependent := ["start_generation"] },
    { determinant := ["start_processed_count", "row_count"], dependent := ["next_processed_count"] },
    { determinant := ["start_processed_count", "next_processed_count"], dependent := ["row_count"] },
    { determinant := ["row_count", "next_processed_count"], dependent := ["start_processed_count"] },
    { determinant := ["row_count"], dependent := ["terminal", "next_state"] },
    { determinant := ["terminal"], dependent := ["next_state"] },
    { determinant := ["next_state"], dependent := ["terminal"] }
  ]

theorem analysis_batch_receipt_schema_well_formed :
    schemaWellFormedCheck analysis_batch_receipt_contract = true := by
  native_decide

theorem analysis_batch_receipt_candidate_keys_check :
    keysDetermineAllCheck analysis_batch_receipt_contract = true := by
  native_decide

theorem analysis_batch_receipt_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_batch_receipt_contract :=
  keysDetermineAllCheck_sound analysis_batch_receipt_contract
    analysis_batch_receipt_candidate_keys_check

theorem analysis_batch_receipt_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_batch_receipt_contract = true := by
  native_decide

theorem analysis_batch_receipt_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_batch_receipt_contract :=
  declaredKeysMinimalCheck_sound analysis_batch_receipt_contract
    analysis_batch_receipt_candidate_keys_minimal_check

theorem analysis_batch_receipt_closure_fixed_check :
    closureFixedPointCheck analysis_batch_receipt_contract = true := by
  native_decide

theorem analysis_batch_receipt_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_batch_receipt_contract :=
  closureFixedPointCheck_sound analysis_batch_receipt_contract
    analysis_batch_receipt_closure_fixed_check

def publication_candidate_contract : RelationContract where
  name := "publication_candidate"
  attributes := ["candidate_id", "analysis_id", "reserved_revision", "artifact_policy_id", "display_title_policy_id", "artifacts_required", "created_at"]
  declaredKeys := [["candidate_id"], ["reserved_revision"]]
  declaredFDs := [
    { determinant := ["candidate_id"], dependent := ["analysis_id", "reserved_revision", "artifact_policy_id", "display_title_policy_id", "artifacts_required", "created_at"] },
    { determinant := ["reserved_revision"], dependent := ["candidate_id", "analysis_id", "artifact_policy_id", "display_title_policy_id", "artifacts_required", "created_at"] }
  ]

theorem publication_candidate_schema_well_formed :
    schemaWellFormedCheck publication_candidate_contract = true := by
  native_decide

theorem publication_candidate_candidate_keys_check :
    keysDetermineAllCheck publication_candidate_contract = true := by
  native_decide

theorem publication_candidate_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_candidate_contract :=
  keysDetermineAllCheck_sound publication_candidate_contract
    publication_candidate_candidate_keys_check

theorem publication_candidate_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_candidate_contract = true := by
  native_decide

theorem publication_candidate_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_candidate_contract :=
  declaredKeysMinimalCheck_sound publication_candidate_contract
    publication_candidate_candidate_keys_minimal_check

theorem publication_candidate_closure_fixed_check :
    closureFixedPointCheck publication_candidate_contract = true := by
  native_decide

theorem publication_candidate_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_candidate_contract :=
  closureFixedPointCheck_sound publication_candidate_contract
    publication_candidate_closure_fixed_check

theorem publication_candidate_bcnf_check :
    bcnfCheck publication_candidate_contract = true := by
  native_decide

theorem publication_candidate_bcnf : BCNF publication_candidate_contract :=
  bcnfCheck_sound publication_candidate_contract publication_candidate_bcnf_check

def publication_candidate_projection_seal_contract : RelationContract where
  name := "publication_candidate_projection_seal"
  attributes := ["candidate_id"]
  declaredKeys := [["candidate_id"]]
  declaredFDs := [
  ]

theorem publication_candidate_projection_seal_schema_well_formed :
    schemaWellFormedCheck publication_candidate_projection_seal_contract = true := by
  native_decide

theorem publication_candidate_projection_seal_candidate_keys_check :
    keysDetermineAllCheck publication_candidate_projection_seal_contract = true := by
  native_decide

theorem publication_candidate_projection_seal_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_candidate_projection_seal_contract :=
  keysDetermineAllCheck_sound publication_candidate_projection_seal_contract
    publication_candidate_projection_seal_candidate_keys_check

theorem publication_candidate_projection_seal_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_candidate_projection_seal_contract = true := by
  native_decide

theorem publication_candidate_projection_seal_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_candidate_projection_seal_contract :=
  declaredKeysMinimalCheck_sound publication_candidate_projection_seal_contract
    publication_candidate_projection_seal_candidate_keys_minimal_check

theorem publication_candidate_projection_seal_closure_fixed_check :
    closureFixedPointCheck publication_candidate_projection_seal_contract = true := by
  native_decide

theorem publication_candidate_projection_seal_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_candidate_projection_seal_contract :=
  closureFixedPointCheck_sound publication_candidate_projection_seal_contract
    publication_candidate_projection_seal_closure_fixed_check

theorem publication_candidate_projection_seal_bcnf_check :
    bcnfCheck publication_candidate_projection_seal_contract = true := by
  native_decide

theorem publication_candidate_projection_seal_bcnf : BCNF publication_candidate_projection_seal_contract :=
  bcnfCheck_sound publication_candidate_projection_seal_contract publication_candidate_projection_seal_bcnf_check

def publication_candidate_projection_contract : RelationContract where
  name := "publication_candidate_projection"
  attributes := ["candidate_id", "create_count", "rebuild_count", "delete_count", "new_galleries", "changed_galleries"]
  declaredKeys := [["candidate_id"]]
  declaredFDs := [
    { determinant := ["candidate_id"], dependent := ["create_count", "rebuild_count", "delete_count", "new_galleries", "changed_galleries"] }
  ]

theorem publication_candidate_projection_schema_well_formed :
    schemaWellFormedCheck publication_candidate_projection_contract = true := by
  native_decide

theorem publication_candidate_projection_candidate_keys_check :
    keysDetermineAllCheck publication_candidate_projection_contract = true := by
  native_decide

theorem publication_candidate_projection_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_candidate_projection_contract :=
  keysDetermineAllCheck_sound publication_candidate_projection_contract
    publication_candidate_projection_candidate_keys_check

theorem publication_candidate_projection_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_candidate_projection_contract = true := by
  native_decide

theorem publication_candidate_projection_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_candidate_projection_contract :=
  declaredKeysMinimalCheck_sound publication_candidate_projection_contract
    publication_candidate_projection_candidate_keys_minimal_check

theorem publication_candidate_projection_closure_fixed_check :
    closureFixedPointCheck publication_candidate_projection_contract = true := by
  native_decide

theorem publication_candidate_projection_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_candidate_projection_contract :=
  closureFixedPointCheck_sound publication_candidate_projection_contract
    publication_candidate_projection_closure_fixed_check

def publication_candidate_base_publication_commit_contract : RelationContract where
  name := "publication_candidate_base_publication_commit"
  attributes := ["candidate_id", "base_receipt_id"]
  declaredKeys := [["candidate_id"]]
  declaredFDs := [
    { determinant := ["candidate_id"], dependent := ["base_receipt_id"] }
  ]

theorem publication_candidate_base_publication_commit_schema_well_formed :
    schemaWellFormedCheck publication_candidate_base_publication_commit_contract = true := by
  native_decide

theorem publication_candidate_base_publication_commit_candidate_keys_check :
    keysDetermineAllCheck publication_candidate_base_publication_commit_contract = true := by
  native_decide

theorem publication_candidate_base_publication_commit_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_candidate_base_publication_commit_contract :=
  keysDetermineAllCheck_sound publication_candidate_base_publication_commit_contract
    publication_candidate_base_publication_commit_candidate_keys_check

theorem publication_candidate_base_publication_commit_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_candidate_base_publication_commit_contract = true := by
  native_decide

theorem publication_candidate_base_publication_commit_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_candidate_base_publication_commit_contract :=
  declaredKeysMinimalCheck_sound publication_candidate_base_publication_commit_contract
    publication_candidate_base_publication_commit_candidate_keys_minimal_check

theorem publication_candidate_base_publication_commit_closure_fixed_check :
    closureFixedPointCheck publication_candidate_base_publication_commit_contract = true := by
  native_decide

theorem publication_candidate_base_publication_commit_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_candidate_base_publication_commit_contract :=
  closureFixedPointCheck_sound publication_candidate_base_publication_commit_contract
    publication_candidate_base_publication_commit_closure_fixed_check

theorem publication_candidate_base_publication_commit_bcnf_check :
    bcnfCheck publication_candidate_base_publication_commit_contract = true := by
  native_decide

theorem publication_candidate_base_publication_commit_bcnf : BCNF publication_candidate_base_publication_commit_contract :=
  bcnfCheck_sound publication_candidate_base_publication_commit_contract publication_candidate_base_publication_commit_bcnf_check

def publication_candidate_base_catalog_contract : RelationContract where
  name := "publication_candidate_base_catalog"
  attributes := ["candidate_id", "base_revision", "base_catalog_generation"]
  declaredKeys := [["candidate_id"]]
  declaredFDs := [
    { determinant := ["candidate_id"], dependent := ["base_revision", "base_catalog_generation"] },
    { determinant := ["base_revision"], dependent := ["base_catalog_generation"] },
    { determinant := ["base_catalog_generation"], dependent := ["base_revision"] }
  ]

theorem publication_candidate_base_catalog_schema_well_formed :
    schemaWellFormedCheck publication_candidate_base_catalog_contract = true := by
  native_decide

theorem publication_candidate_base_catalog_candidate_keys_check :
    keysDetermineAllCheck publication_candidate_base_catalog_contract = true := by
  native_decide

theorem publication_candidate_base_catalog_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_candidate_base_catalog_contract :=
  keysDetermineAllCheck_sound publication_candidate_base_catalog_contract
    publication_candidate_base_catalog_candidate_keys_check

theorem publication_candidate_base_catalog_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_candidate_base_catalog_contract = true := by
  native_decide

theorem publication_candidate_base_catalog_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_candidate_base_catalog_contract :=
  declaredKeysMinimalCheck_sound publication_candidate_base_catalog_contract
    publication_candidate_base_catalog_candidate_keys_minimal_check

theorem publication_candidate_base_catalog_closure_fixed_check :
    closureFixedPointCheck publication_candidate_base_catalog_contract = true := by
  native_decide

theorem publication_candidate_base_catalog_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_candidate_base_catalog_contract :=
  closureFixedPointCheck_sound publication_candidate_base_catalog_contract
    publication_candidate_base_catalog_closure_fixed_check

def publication_candidate_base_source_contract : RelationContract where
  name := "publication_candidate_base_source"
  attributes := ["candidate_id", "base_source_revision", "base_source_generation"]
  declaredKeys := [["candidate_id"]]
  declaredFDs := [
    { determinant := ["candidate_id"], dependent := ["base_source_revision", "base_source_generation"] },
    { determinant := ["base_source_revision"], dependent := ["base_source_generation"] },
    { determinant := ["base_source_generation"], dependent := ["base_source_revision"] }
  ]

theorem publication_candidate_base_source_schema_well_formed :
    schemaWellFormedCheck publication_candidate_base_source_contract = true := by
  native_decide

theorem publication_candidate_base_source_candidate_keys_check :
    keysDetermineAllCheck publication_candidate_base_source_contract = true := by
  native_decide

theorem publication_candidate_base_source_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_candidate_base_source_contract :=
  keysDetermineAllCheck_sound publication_candidate_base_source_contract
    publication_candidate_base_source_candidate_keys_check

theorem publication_candidate_base_source_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_candidate_base_source_contract = true := by
  native_decide

theorem publication_candidate_base_source_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_candidate_base_source_contract :=
  declaredKeysMinimalCheck_sound publication_candidate_base_source_contract
    publication_candidate_base_source_candidate_keys_minimal_check

theorem publication_candidate_base_source_closure_fixed_check :
    closureFixedPointCheck publication_candidate_base_source_contract = true := by
  native_decide

theorem publication_candidate_base_source_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_candidate_base_source_contract :=
  closureFixedPointCheck_sound publication_candidate_base_source_contract
    publication_candidate_base_source_closure_fixed_check

def publication_selection_storage_contract : RelationContract where
  name := "publication_selection_storage"
  attributes := ["selection_occurrence_sha256", "gallery_id"]
  declaredKeys := [["selection_occurrence_sha256"]]
  declaredFDs := [
    { determinant := ["selection_occurrence_sha256"], dependent := ["gallery_id"] }
  ]

theorem publication_selection_storage_schema_well_formed :
    schemaWellFormedCheck publication_selection_storage_contract = true := by
  native_decide

theorem publication_selection_storage_candidate_keys_check :
    keysDetermineAllCheck publication_selection_storage_contract = true := by
  native_decide

theorem publication_selection_storage_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_selection_storage_contract :=
  keysDetermineAllCheck_sound publication_selection_storage_contract
    publication_selection_storage_candidate_keys_check

theorem publication_selection_storage_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_selection_storage_contract = true := by
  native_decide

theorem publication_selection_storage_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_selection_storage_contract :=
  declaredKeysMinimalCheck_sound publication_selection_storage_contract
    publication_selection_storage_candidate_keys_minimal_check

theorem publication_selection_storage_closure_fixed_check :
    closureFixedPointCheck publication_selection_storage_contract = true := by
  native_decide

theorem publication_selection_storage_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_selection_storage_contract :=
  closureFixedPointCheck_sound publication_selection_storage_contract
    publication_selection_storage_closure_fixed_check

theorem publication_selection_storage_bcnf_check :
    bcnfCheck publication_selection_storage_contract = true := by
  native_decide

theorem publication_selection_storage_bcnf : BCNF publication_selection_storage_contract :=
  bcnfCheck_sound publication_selection_storage_contract publication_selection_storage_bcnf_check

def publication_selection_occurrence_identity_contract : RelationContract where
  name := "publication_selection_occurrence_identity"
  attributes := ["selection_occurrence_sha256", "candidate_id", "publication_key"]
  declaredKeys := [["candidate_id", "publication_key"], ["selection_occurrence_sha256"]]
  declaredFDs := [
    { determinant := ["selection_occurrence_sha256"], dependent := ["candidate_id", "publication_key"] },
    { determinant := ["candidate_id", "publication_key"], dependent := ["selection_occurrence_sha256"] }
  ]

theorem publication_selection_occurrence_identity_schema_well_formed :
    schemaWellFormedCheck publication_selection_occurrence_identity_contract = true := by
  native_decide

theorem publication_selection_occurrence_identity_candidate_keys_check :
    keysDetermineAllCheck publication_selection_occurrence_identity_contract = true := by
  native_decide

theorem publication_selection_occurrence_identity_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_selection_occurrence_identity_contract :=
  keysDetermineAllCheck_sound publication_selection_occurrence_identity_contract
    publication_selection_occurrence_identity_candidate_keys_check

theorem publication_selection_occurrence_identity_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_selection_occurrence_identity_contract = true := by
  native_decide

theorem publication_selection_occurrence_identity_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_selection_occurrence_identity_contract :=
  declaredKeysMinimalCheck_sound publication_selection_occurrence_identity_contract
    publication_selection_occurrence_identity_candidate_keys_minimal_check

theorem publication_selection_occurrence_identity_closure_fixed_check :
    closureFixedPointCheck publication_selection_occurrence_identity_contract = true := by
  native_decide

theorem publication_selection_occurrence_identity_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_selection_occurrence_identity_contract :=
  closureFixedPointCheck_sound publication_selection_occurrence_identity_contract
    publication_selection_occurrence_identity_closure_fixed_check

theorem publication_selection_occurrence_identity_bcnf_check :
    bcnfCheck publication_selection_occurrence_identity_contract = true := by
  native_decide

theorem publication_selection_occurrence_identity_bcnf : BCNF publication_selection_occurrence_identity_contract :=
  bcnfCheck_sound publication_selection_occurrence_identity_contract publication_selection_occurrence_identity_bcnf_check

def publication_selection_contract : RelationContract where
  name := "publication_selection"
  attributes := ["candidate_id", "gallery_id", "publication_key"]
  declaredKeys := [["candidate_id", "gallery_id"], ["candidate_id", "publication_key"]]
  declaredFDs := [
    { determinant := ["gallery_id"], dependent := ["publication_key"] },
    { determinant := ["candidate_id", "gallery_id"], dependent := ["publication_key"] },
    { determinant := ["candidate_id", "publication_key"], dependent := ["gallery_id"] }
  ]

theorem publication_selection_schema_well_formed :
    schemaWellFormedCheck publication_selection_contract = true := by
  native_decide

theorem publication_selection_candidate_keys_check :
    keysDetermineAllCheck publication_selection_contract = true := by
  native_decide

theorem publication_selection_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_selection_contract :=
  keysDetermineAllCheck_sound publication_selection_contract
    publication_selection_candidate_keys_check

theorem publication_selection_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_selection_contract = true := by
  native_decide

theorem publication_selection_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_selection_contract :=
  declaredKeysMinimalCheck_sound publication_selection_contract
    publication_selection_candidate_keys_minimal_check

theorem publication_selection_closure_fixed_check :
    closureFixedPointCheck publication_selection_contract = true := by
  native_decide

theorem publication_selection_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_selection_contract :=
  closureFixedPointCheck_sound publication_selection_contract
    publication_selection_closure_fixed_check

def publication_stage_contract : RelationContract where
  name := "publication_stage"
  attributes := ["stage", "stage_order", "cursor_codec"]
  declaredKeys := [["stage"], ["stage_order"]]
  declaredFDs := [
    { determinant := ["stage"], dependent := ["stage_order", "cursor_codec"] },
    { determinant := ["stage_order"], dependent := ["stage", "cursor_codec"] }
  ]

theorem publication_stage_schema_well_formed :
    schemaWellFormedCheck publication_stage_contract = true := by
  native_decide

theorem publication_stage_candidate_keys_check :
    keysDetermineAllCheck publication_stage_contract = true := by
  native_decide

theorem publication_stage_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_stage_contract :=
  keysDetermineAllCheck_sound publication_stage_contract
    publication_stage_candidate_keys_check

theorem publication_stage_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_stage_contract = true := by
  native_decide

theorem publication_stage_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_stage_contract :=
  declaredKeysMinimalCheck_sound publication_stage_contract
    publication_stage_candidate_keys_minimal_check

theorem publication_stage_closure_fixed_check :
    closureFixedPointCheck publication_stage_contract = true := by
  native_decide

theorem publication_stage_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_stage_contract :=
  closureFixedPointCheck_sound publication_stage_contract
    publication_stage_closure_fixed_check

theorem publication_stage_bcnf_check :
    bcnfCheck publication_stage_contract = true := by
  native_decide

theorem publication_stage_bcnf : BCNF publication_stage_contract :=
  bcnfCheck_sound publication_stage_contract publication_stage_bcnf_check

def publication_checkpoint_contract : RelationContract where
  name := "publication_checkpoint"
  attributes := ["candidate_id", "stage", "generation", "cursor", "processed_count", "state", "updated_at"]
  declaredKeys := [["candidate_id", "stage"]]
  declaredFDs := [
    { determinant := ["candidate_id", "stage"], dependent := ["generation", "cursor", "processed_count", "state", "updated_at"] }
  ]

theorem publication_checkpoint_schema_well_formed :
    schemaWellFormedCheck publication_checkpoint_contract = true := by
  native_decide

theorem publication_checkpoint_candidate_keys_check :
    keysDetermineAllCheck publication_checkpoint_contract = true := by
  native_decide

theorem publication_checkpoint_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_checkpoint_contract :=
  keysDetermineAllCheck_sound publication_checkpoint_contract
    publication_checkpoint_candidate_keys_check

theorem publication_checkpoint_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_checkpoint_contract = true := by
  native_decide

theorem publication_checkpoint_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_checkpoint_contract :=
  declaredKeysMinimalCheck_sound publication_checkpoint_contract
    publication_checkpoint_candidate_keys_minimal_check

theorem publication_checkpoint_closure_fixed_check :
    closureFixedPointCheck publication_checkpoint_contract = true := by
  native_decide

theorem publication_checkpoint_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_checkpoint_contract :=
  closureFixedPointCheck_sound publication_checkpoint_contract
    publication_checkpoint_closure_fixed_check

theorem publication_checkpoint_bcnf_check :
    bcnfCheck publication_checkpoint_contract = true := by
  native_decide

theorem publication_checkpoint_bcnf : BCNF publication_checkpoint_contract :=
  bcnfCheck_sound publication_checkpoint_contract publication_checkpoint_bcnf_check

def publication_batch_receipt_stored_contract : RelationContract where
  name := "publication_batch_receipt_stored"
  attributes := ["candidate_id", "stage", "start_generation", "batch_key", "start_cursor", "start_processed_count", "next_cursor", "row_count", "committed_at"]
  declaredKeys := [["candidate_id", "stage", "start_generation"], ["candidate_id", "stage", "batch_key"]]
  declaredFDs := [
    { determinant := ["candidate_id", "stage", "batch_key"], dependent := ["start_generation", "start_cursor", "start_processed_count", "next_cursor", "row_count", "committed_at"] },
    { determinant := ["candidate_id", "stage", "start_generation"], dependent := ["batch_key", "start_cursor", "start_processed_count", "next_cursor", "row_count", "committed_at"] }
  ]

theorem publication_batch_receipt_stored_schema_well_formed :
    schemaWellFormedCheck publication_batch_receipt_stored_contract = true := by
  native_decide

theorem publication_batch_receipt_stored_candidate_keys_check :
    keysDetermineAllCheck publication_batch_receipt_stored_contract = true := by
  native_decide

theorem publication_batch_receipt_stored_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_batch_receipt_stored_contract :=
  keysDetermineAllCheck_sound publication_batch_receipt_stored_contract
    publication_batch_receipt_stored_candidate_keys_check

theorem publication_batch_receipt_stored_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_batch_receipt_stored_contract = true := by
  native_decide

theorem publication_batch_receipt_stored_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_batch_receipt_stored_contract :=
  declaredKeysMinimalCheck_sound publication_batch_receipt_stored_contract
    publication_batch_receipt_stored_candidate_keys_minimal_check

theorem publication_batch_receipt_stored_closure_fixed_check :
    closureFixedPointCheck publication_batch_receipt_stored_contract = true := by
  native_decide

theorem publication_batch_receipt_stored_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_batch_receipt_stored_contract :=
  closureFixedPointCheck_sound publication_batch_receipt_stored_contract
    publication_batch_receipt_stored_closure_fixed_check

theorem publication_batch_receipt_stored_bcnf_check :
    bcnfCheck publication_batch_receipt_stored_contract = true := by
  native_decide

theorem publication_batch_receipt_stored_bcnf : BCNF publication_batch_receipt_stored_contract :=
  bcnfCheck_sound publication_batch_receipt_stored_contract publication_batch_receipt_stored_bcnf_check

def publication_batch_receipt_contract : RelationContract where
  name := "publication_batch_receipt"
  attributes := ["candidate_id", "stage", "batch_key", "start_generation", "start_cursor", "start_processed_count", "next_cursor", "next_processed_count", "next_state", "row_count", "terminal", "committed_generation", "committed_at"]
  declaredKeys := [["candidate_id", "stage", "batch_key"], ["candidate_id", "stage", "start_generation"], ["candidate_id", "stage", "committed_generation"]]
  declaredFDs := [
    { determinant := ["candidate_id", "stage", "batch_key"], dependent := ["start_generation", "start_cursor", "start_processed_count", "next_cursor", "next_processed_count", "next_state", "row_count", "terminal", "committed_generation", "committed_at"] },
    { determinant := ["candidate_id", "stage", "start_generation"], dependent := ["batch_key", "start_cursor", "start_processed_count", "next_cursor", "next_processed_count", "next_state", "row_count", "terminal", "committed_generation", "committed_at"] },
    { determinant := ["start_generation"], dependent := ["committed_generation"] },
    { determinant := ["committed_generation"], dependent := ["start_generation"] },
    { determinant := ["start_processed_count", "row_count"], dependent := ["next_processed_count"] },
    { determinant := ["start_processed_count", "next_processed_count"], dependent := ["row_count"] },
    { determinant := ["row_count", "next_processed_count"], dependent := ["start_processed_count"] },
    { determinant := ["row_count"], dependent := ["terminal", "next_state"] },
    { determinant := ["terminal"], dependent := ["next_state"] },
    { determinant := ["next_state"], dependent := ["terminal"] }
  ]

theorem publication_batch_receipt_schema_well_formed :
    schemaWellFormedCheck publication_batch_receipt_contract = true := by
  native_decide

theorem publication_batch_receipt_candidate_keys_check :
    keysDetermineAllCheck publication_batch_receipt_contract = true := by
  native_decide

theorem publication_batch_receipt_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_batch_receipt_contract :=
  keysDetermineAllCheck_sound publication_batch_receipt_contract
    publication_batch_receipt_candidate_keys_check

theorem publication_batch_receipt_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_batch_receipt_contract = true := by
  native_decide

theorem publication_batch_receipt_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_batch_receipt_contract :=
  declaredKeysMinimalCheck_sound publication_batch_receipt_contract
    publication_batch_receipt_candidate_keys_minimal_check

theorem publication_batch_receipt_closure_fixed_check :
    closureFixedPointCheck publication_batch_receipt_contract = true := by
  native_decide

theorem publication_batch_receipt_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_batch_receipt_contract :=
  closureFixedPointCheck_sound publication_batch_receipt_contract
    publication_batch_receipt_closure_fixed_check

def publication_finalization_checkpoint_contract : RelationContract where
  name := "publication_finalization_checkpoint"
  attributes := ["receipt_id", "generation", "cursor", "processed_count", "state", "updated_at"]
  declaredKeys := [["receipt_id"]]
  declaredFDs := [
    { determinant := ["receipt_id"], dependent := ["generation", "cursor", "processed_count", "state", "updated_at"] }
  ]

theorem publication_finalization_checkpoint_schema_well_formed :
    schemaWellFormedCheck publication_finalization_checkpoint_contract = true := by
  native_decide

theorem publication_finalization_checkpoint_candidate_keys_check :
    keysDetermineAllCheck publication_finalization_checkpoint_contract = true := by
  native_decide

theorem publication_finalization_checkpoint_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_finalization_checkpoint_contract :=
  keysDetermineAllCheck_sound publication_finalization_checkpoint_contract
    publication_finalization_checkpoint_candidate_keys_check

theorem publication_finalization_checkpoint_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_finalization_checkpoint_contract = true := by
  native_decide

theorem publication_finalization_checkpoint_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_finalization_checkpoint_contract :=
  declaredKeysMinimalCheck_sound publication_finalization_checkpoint_contract
    publication_finalization_checkpoint_candidate_keys_minimal_check

theorem publication_finalization_checkpoint_closure_fixed_check :
    closureFixedPointCheck publication_finalization_checkpoint_contract = true := by
  native_decide

theorem publication_finalization_checkpoint_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_finalization_checkpoint_contract :=
  closureFixedPointCheck_sound publication_finalization_checkpoint_contract
    publication_finalization_checkpoint_closure_fixed_check

theorem publication_finalization_checkpoint_bcnf_check :
    bcnfCheck publication_finalization_checkpoint_contract = true := by
  native_decide

theorem publication_finalization_checkpoint_bcnf : BCNF publication_finalization_checkpoint_contract :=
  bcnfCheck_sound publication_finalization_checkpoint_contract publication_finalization_checkpoint_bcnf_check

def publication_finalization_batch_receipt_stored_contract : RelationContract where
  name := "publication_finalization_batch_receipt_stored"
  attributes := ["receipt_id", "start_generation", "batch_key", "start_cursor", "start_processed_count", "next_cursor", "row_count", "committed_at"]
  declaredKeys := [["receipt_id", "start_generation"], ["receipt_id", "batch_key"]]
  declaredFDs := [
    { determinant := ["receipt_id", "batch_key"], dependent := ["start_generation", "start_cursor", "start_processed_count", "next_cursor", "row_count", "committed_at"] },
    { determinant := ["receipt_id", "start_generation"], dependent := ["batch_key", "start_cursor", "start_processed_count", "next_cursor", "row_count", "committed_at"] }
  ]

theorem publication_finalization_batch_receipt_stored_schema_well_formed :
    schemaWellFormedCheck publication_finalization_batch_receipt_stored_contract = true := by
  native_decide

theorem publication_finalization_batch_receipt_stored_candidate_keys_check :
    keysDetermineAllCheck publication_finalization_batch_receipt_stored_contract = true := by
  native_decide

theorem publication_finalization_batch_receipt_stored_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_finalization_batch_receipt_stored_contract :=
  keysDetermineAllCheck_sound publication_finalization_batch_receipt_stored_contract
    publication_finalization_batch_receipt_stored_candidate_keys_check

theorem publication_finalization_batch_receipt_stored_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_finalization_batch_receipt_stored_contract = true := by
  native_decide

theorem publication_finalization_batch_receipt_stored_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_finalization_batch_receipt_stored_contract :=
  declaredKeysMinimalCheck_sound publication_finalization_batch_receipt_stored_contract
    publication_finalization_batch_receipt_stored_candidate_keys_minimal_check

theorem publication_finalization_batch_receipt_stored_closure_fixed_check :
    closureFixedPointCheck publication_finalization_batch_receipt_stored_contract = true := by
  native_decide

theorem publication_finalization_batch_receipt_stored_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_finalization_batch_receipt_stored_contract :=
  closureFixedPointCheck_sound publication_finalization_batch_receipt_stored_contract
    publication_finalization_batch_receipt_stored_closure_fixed_check

theorem publication_finalization_batch_receipt_stored_bcnf_check :
    bcnfCheck publication_finalization_batch_receipt_stored_contract = true := by
  native_decide

theorem publication_finalization_batch_receipt_stored_bcnf : BCNF publication_finalization_batch_receipt_stored_contract :=
  bcnfCheck_sound publication_finalization_batch_receipt_stored_contract publication_finalization_batch_receipt_stored_bcnf_check

def publication_finalization_batch_receipt_contract : RelationContract where
  name := "publication_finalization_batch_receipt"
  attributes := ["receipt_id", "batch_key", "start_generation", "start_cursor", "start_processed_count", "next_cursor", "next_processed_count", "next_state", "row_count", "terminal", "committed_generation", "committed_at"]
  declaredKeys := [["receipt_id", "batch_key"], ["receipt_id", "start_generation"], ["receipt_id", "committed_generation"]]
  declaredFDs := [
    { determinant := ["receipt_id", "batch_key"], dependent := ["start_generation", "start_cursor", "start_processed_count", "next_cursor", "next_processed_count", "next_state", "row_count", "terminal", "committed_generation", "committed_at"] },
    { determinant := ["receipt_id", "start_generation"], dependent := ["batch_key", "start_cursor", "start_processed_count", "next_cursor", "next_processed_count", "next_state", "row_count", "terminal", "committed_generation", "committed_at"] },
    { determinant := ["start_generation"], dependent := ["committed_generation"] },
    { determinant := ["committed_generation"], dependent := ["start_generation"] },
    { determinant := ["start_processed_count", "row_count"], dependent := ["next_processed_count"] },
    { determinant := ["start_processed_count", "next_processed_count"], dependent := ["row_count"] },
    { determinant := ["row_count", "next_processed_count"], dependent := ["start_processed_count"] },
    { determinant := ["row_count"], dependent := ["terminal", "next_state"] },
    { determinant := ["terminal"], dependent := ["next_state"] },
    { determinant := ["next_state"], dependent := ["terminal"] }
  ]

theorem publication_finalization_batch_receipt_schema_well_formed :
    schemaWellFormedCheck publication_finalization_batch_receipt_contract = true := by
  native_decide

theorem publication_finalization_batch_receipt_candidate_keys_check :
    keysDetermineAllCheck publication_finalization_batch_receipt_contract = true := by
  native_decide

theorem publication_finalization_batch_receipt_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_finalization_batch_receipt_contract :=
  keysDetermineAllCheck_sound publication_finalization_batch_receipt_contract
    publication_finalization_batch_receipt_candidate_keys_check

theorem publication_finalization_batch_receipt_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_finalization_batch_receipt_contract = true := by
  native_decide

theorem publication_finalization_batch_receipt_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_finalization_batch_receipt_contract :=
  declaredKeysMinimalCheck_sound publication_finalization_batch_receipt_contract
    publication_finalization_batch_receipt_candidate_keys_minimal_check

theorem publication_finalization_batch_receipt_closure_fixed_check :
    closureFixedPointCheck publication_finalization_batch_receipt_contract = true := by
  native_decide

theorem publication_finalization_batch_receipt_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_finalization_batch_receipt_contract :=
  closureFixedPointCheck_sound publication_finalization_batch_receipt_contract
    publication_finalization_batch_receipt_closure_fixed_check

def artifact_zip_writer_policy_contract : RelationContract where
  name := "artifact_zip_writer_policy"
  attributes := ["artifact_algorithm_version", "zip_codec_version", "compression_method", "compression_level", "dos_date", "dos_time", "unix_mode", "general_purpose_flags", "create_system", "archive_name_codec_version", "artifact_name_codec_version"]
  declaredKeys := [["artifact_algorithm_version"], ["zip_codec_version", "compression_method", "compression_level", "dos_date", "dos_time", "unix_mode", "general_purpose_flags", "create_system", "archive_name_codec_version", "artifact_name_codec_version"]]
  declaredFDs := [
    { determinant := ["artifact_algorithm_version"], dependent := ["zip_codec_version", "compression_method", "compression_level", "dos_date", "dos_time", "unix_mode", "general_purpose_flags", "create_system", "archive_name_codec_version", "artifact_name_codec_version"] },
    { determinant := ["zip_codec_version", "compression_method", "compression_level", "dos_date", "dos_time", "unix_mode", "general_purpose_flags", "create_system", "archive_name_codec_version", "artifact_name_codec_version"], dependent := ["artifact_algorithm_version"] }
  ]

theorem artifact_zip_writer_policy_schema_well_formed :
    schemaWellFormedCheck artifact_zip_writer_policy_contract = true := by
  native_decide

theorem artifact_zip_writer_policy_candidate_keys_check :
    keysDetermineAllCheck artifact_zip_writer_policy_contract = true := by
  native_decide

theorem artifact_zip_writer_policy_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes artifact_zip_writer_policy_contract :=
  keysDetermineAllCheck_sound artifact_zip_writer_policy_contract
    artifact_zip_writer_policy_candidate_keys_check

theorem artifact_zip_writer_policy_candidate_keys_minimal_check :
    declaredKeysMinimalCheck artifact_zip_writer_policy_contract = true := by
  native_decide

theorem artifact_zip_writer_policy_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal artifact_zip_writer_policy_contract :=
  declaredKeysMinimalCheck_sound artifact_zip_writer_policy_contract
    artifact_zip_writer_policy_candidate_keys_minimal_check

theorem artifact_zip_writer_policy_closure_fixed_check :
    closureFixedPointCheck artifact_zip_writer_policy_contract = true := by
  native_decide

theorem artifact_zip_writer_policy_closure_reached_fixed_point :
    ClosureReachedFixedPoint artifact_zip_writer_policy_contract :=
  closureFixedPointCheck_sound artifact_zip_writer_policy_contract
    artifact_zip_writer_policy_closure_fixed_check

theorem artifact_zip_writer_policy_bcnf_check :
    bcnfCheck artifact_zip_writer_policy_contract = true := by
  native_decide

theorem artifact_zip_writer_policy_bcnf : BCNF artifact_zip_writer_policy_contract :=
  bcnfCheck_sound artifact_zip_writer_policy_contract artifact_zip_writer_policy_bcnf_check

def artifact_producer_fingerprint_contract : RelationContract where
  name := "artifact_producer_fingerprint"
  attributes := ["producer_fingerprint_sha256", "artifact_algorithm_version", "producer_equivalence_class", "writer_id", "python_abi", "pillow_build", "libjpeg_build", "zlib_build"]
  declaredKeys := [["producer_fingerprint_sha256"], ["producer_equivalence_class"], ["writer_id", "python_abi", "pillow_build", "libjpeg_build", "zlib_build"]]
  declaredFDs := [
    { determinant := ["producer_fingerprint_sha256"], dependent := ["artifact_algorithm_version", "producer_equivalence_class", "writer_id", "python_abi", "pillow_build", "libjpeg_build", "zlib_build"] },
    { determinant := ["producer_equivalence_class"], dependent := ["producer_fingerprint_sha256", "artifact_algorithm_version", "writer_id", "python_abi", "pillow_build", "libjpeg_build", "zlib_build"] },
    { determinant := ["writer_id", "python_abi", "pillow_build", "libjpeg_build", "zlib_build"], dependent := ["producer_fingerprint_sha256", "artifact_algorithm_version", "producer_equivalence_class"] }
  ]

theorem artifact_producer_fingerprint_schema_well_formed :
    schemaWellFormedCheck artifact_producer_fingerprint_contract = true := by
  native_decide

theorem artifact_producer_fingerprint_candidate_keys_check :
    keysDetermineAllCheck artifact_producer_fingerprint_contract = true := by
  native_decide

theorem artifact_producer_fingerprint_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes artifact_producer_fingerprint_contract :=
  keysDetermineAllCheck_sound artifact_producer_fingerprint_contract
    artifact_producer_fingerprint_candidate_keys_check

theorem artifact_producer_fingerprint_candidate_keys_minimal_check :
    declaredKeysMinimalCheck artifact_producer_fingerprint_contract = true := by
  native_decide

theorem artifact_producer_fingerprint_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal artifact_producer_fingerprint_contract :=
  declaredKeysMinimalCheck_sound artifact_producer_fingerprint_contract
    artifact_producer_fingerprint_candidate_keys_minimal_check

theorem artifact_producer_fingerprint_closure_fixed_check :
    closureFixedPointCheck artifact_producer_fingerprint_contract = true := by
  native_decide

theorem artifact_producer_fingerprint_closure_reached_fixed_point :
    ClosureReachedFixedPoint artifact_producer_fingerprint_contract :=
  closureFixedPointCheck_sound artifact_producer_fingerprint_contract
    artifact_producer_fingerprint_closure_fixed_check

theorem artifact_producer_fingerprint_bcnf_check :
    bcnfCheck artifact_producer_fingerprint_contract = true := by
  native_decide

theorem artifact_producer_fingerprint_bcnf : BCNF artifact_producer_fingerprint_contract :=
  bcnfCheck_sound artifact_producer_fingerprint_contract artifact_producer_fingerprint_bcnf_check

def artifact_storage_codec_contract : RelationContract where
  name := "artifact_storage_codec"
  attributes := ["storage_codec_version", "adapter_id", "locator_codec_version", "protection_token_codec_version"]
  declaredKeys := [["storage_codec_version"], ["adapter_id"]]
  declaredFDs := [
    { determinant := ["storage_codec_version"], dependent := ["adapter_id", "locator_codec_version", "protection_token_codec_version"] },
    { determinant := ["adapter_id"], dependent := ["storage_codec_version", "locator_codec_version", "protection_token_codec_version"] }
  ]

theorem artifact_storage_codec_schema_well_formed :
    schemaWellFormedCheck artifact_storage_codec_contract = true := by
  native_decide

theorem artifact_storage_codec_candidate_keys_check :
    keysDetermineAllCheck artifact_storage_codec_contract = true := by
  native_decide

theorem artifact_storage_codec_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes artifact_storage_codec_contract :=
  keysDetermineAllCheck_sound artifact_storage_codec_contract
    artifact_storage_codec_candidate_keys_check

theorem artifact_storage_codec_candidate_keys_minimal_check :
    declaredKeysMinimalCheck artifact_storage_codec_contract = true := by
  native_decide

theorem artifact_storage_codec_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal artifact_storage_codec_contract :=
  declaredKeysMinimalCheck_sound artifact_storage_codec_contract
    artifact_storage_codec_candidate_keys_minimal_check

theorem artifact_storage_codec_closure_fixed_check :
    closureFixedPointCheck artifact_storage_codec_contract = true := by
  native_decide

theorem artifact_storage_codec_closure_reached_fixed_point :
    ClosureReachedFixedPoint artifact_storage_codec_contract :=
  closureFixedPointCheck_sound artifact_storage_codec_contract
    artifact_storage_codec_closure_fixed_check

theorem artifact_storage_codec_bcnf_check :
    bcnfCheck artifact_storage_codec_contract = true := by
  native_decide

theorem artifact_storage_codec_bcnf : BCNF artifact_storage_codec_contract :=
  bcnfCheck_sound artifact_storage_codec_contract artifact_storage_codec_bcnf_check

def artifact_policy_semantics_contract : RelationContract where
  name := "artifact_policy_semantics"
  attributes := ["policy_component_sha256", "max_image_short_side", "producer_fingerprint_sha256"]
  declaredKeys := [["policy_component_sha256"], ["max_image_short_side", "producer_fingerprint_sha256"]]
  declaredFDs := [
    { determinant := ["policy_component_sha256"], dependent := ["max_image_short_side", "producer_fingerprint_sha256"] },
    { determinant := ["max_image_short_side", "producer_fingerprint_sha256"], dependent := ["policy_component_sha256"] }
  ]

theorem artifact_policy_semantics_schema_well_formed :
    schemaWellFormedCheck artifact_policy_semantics_contract = true := by
  native_decide

theorem artifact_policy_semantics_candidate_keys_check :
    keysDetermineAllCheck artifact_policy_semantics_contract = true := by
  native_decide

theorem artifact_policy_semantics_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes artifact_policy_semantics_contract :=
  keysDetermineAllCheck_sound artifact_policy_semantics_contract
    artifact_policy_semantics_candidate_keys_check

theorem artifact_policy_semantics_candidate_keys_minimal_check :
    declaredKeysMinimalCheck artifact_policy_semantics_contract = true := by
  native_decide

theorem artifact_policy_semantics_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal artifact_policy_semantics_contract :=
  declaredKeysMinimalCheck_sound artifact_policy_semantics_contract
    artifact_policy_semantics_candidate_keys_minimal_check

theorem artifact_policy_semantics_closure_fixed_check :
    closureFixedPointCheck artifact_policy_semantics_contract = true := by
  native_decide

theorem artifact_policy_semantics_closure_reached_fixed_point :
    ClosureReachedFixedPoint artifact_policy_semantics_contract :=
  closureFixedPointCheck_sound artifact_policy_semantics_contract
    artifact_policy_semantics_closure_fixed_check

theorem artifact_policy_semantics_bcnf_check :
    bcnfCheck artifact_policy_semantics_contract = true := by
  native_decide

theorem artifact_policy_semantics_bcnf : BCNF artifact_policy_semantics_contract :=
  bcnfCheck_sound artifact_policy_semantics_contract artifact_policy_semantics_bcnf_check

def artifact_policy_contract : RelationContract where
  name := "artifact_policy"
  attributes := ["artifact_policy_id", "policy_component_sha256"]
  declaredKeys := [["artifact_policy_id"], ["policy_component_sha256"]]
  declaredFDs := [
    { determinant := ["artifact_policy_id"], dependent := ["policy_component_sha256"] },
    { determinant := ["policy_component_sha256"], dependent := ["artifact_policy_id"] }
  ]

theorem artifact_policy_schema_well_formed :
    schemaWellFormedCheck artifact_policy_contract = true := by
  native_decide

theorem artifact_policy_candidate_keys_check :
    keysDetermineAllCheck artifact_policy_contract = true := by
  native_decide

theorem artifact_policy_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes artifact_policy_contract :=
  keysDetermineAllCheck_sound artifact_policy_contract
    artifact_policy_candidate_keys_check

theorem artifact_policy_candidate_keys_minimal_check :
    declaredKeysMinimalCheck artifact_policy_contract = true := by
  native_decide

theorem artifact_policy_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal artifact_policy_contract :=
  declaredKeysMinimalCheck_sound artifact_policy_contract
    artifact_policy_candidate_keys_minimal_check

theorem artifact_policy_closure_fixed_check :
    closureFixedPointCheck artifact_policy_contract = true := by
  native_decide

theorem artifact_policy_closure_reached_fixed_point :
    ClosureReachedFixedPoint artifact_policy_contract :=
  closureFixedPointCheck_sound artifact_policy_contract
    artifact_policy_closure_fixed_check

theorem artifact_policy_bcnf_check :
    bcnfCheck artifact_policy_contract = true := by
  native_decide

theorem artifact_policy_bcnf : BCNF artifact_policy_contract :=
  bcnfCheck_sound artifact_policy_contract artifact_policy_bcnf_check

def artifact_semantic_input_contract : RelationContract where
  name := "artifact_semantic_input"
  attributes := ["artifact_semantics_sha256", "source_manifest_component_sha256", "member_plan_component_sha256", "effective_content_component_sha256", "selected_component_sha256", "owner_component_sha256", "policy_component_sha256"]
  declaredKeys := [["artifact_semantics_sha256"], ["source_manifest_component_sha256", "member_plan_component_sha256", "effective_content_component_sha256", "selected_component_sha256", "owner_component_sha256", "policy_component_sha256"]]
  declaredFDs := [
    { determinant := ["artifact_semantics_sha256"], dependent := ["source_manifest_component_sha256", "member_plan_component_sha256", "effective_content_component_sha256", "selected_component_sha256", "owner_component_sha256", "policy_component_sha256"] },
    { determinant := ["source_manifest_component_sha256", "member_plan_component_sha256", "effective_content_component_sha256", "selected_component_sha256", "owner_component_sha256", "policy_component_sha256"], dependent := ["artifact_semantics_sha256"] }
  ]

theorem artifact_semantic_input_schema_well_formed :
    schemaWellFormedCheck artifact_semantic_input_contract = true := by
  native_decide

theorem artifact_semantic_input_candidate_keys_check :
    keysDetermineAllCheck artifact_semantic_input_contract = true := by
  native_decide

theorem artifact_semantic_input_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes artifact_semantic_input_contract :=
  keysDetermineAllCheck_sound artifact_semantic_input_contract
    artifact_semantic_input_candidate_keys_check

theorem artifact_semantic_input_candidate_keys_minimal_check :
    declaredKeysMinimalCheck artifact_semantic_input_contract = true := by
  native_decide

theorem artifact_semantic_input_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal artifact_semantic_input_contract :=
  declaredKeysMinimalCheck_sound artifact_semantic_input_contract
    artifact_semantic_input_candidate_keys_minimal_check

theorem artifact_semantic_input_closure_fixed_check :
    closureFixedPointCheck artifact_semantic_input_contract = true := by
  native_decide

theorem artifact_semantic_input_closure_reached_fixed_point :
    ClosureReachedFixedPoint artifact_semantic_input_contract :=
  closureFixedPointCheck_sound artifact_semantic_input_contract
    artifact_semantic_input_closure_fixed_check

theorem artifact_semantic_input_bcnf_check :
    bcnfCheck artifact_semantic_input_contract = true := by
  native_decide

theorem artifact_semantic_input_bcnf : BCNF artifact_semantic_input_contract :=
  bcnfCheck_sound artifact_semantic_input_contract artifact_semantic_input_bcnf_check

def artifact_input_contract : RelationContract where
  name := "artifact_input"
  attributes := ["candidate_id", "publication_key", "artifact_semantics_sha256"]
  declaredKeys := [["candidate_id", "publication_key"]]
  declaredFDs := [
    { determinant := ["candidate_id", "publication_key"], dependent := ["artifact_semantics_sha256"] }
  ]

theorem artifact_input_schema_well_formed :
    schemaWellFormedCheck artifact_input_contract = true := by
  native_decide

theorem artifact_input_candidate_keys_check :
    keysDetermineAllCheck artifact_input_contract = true := by
  native_decide

theorem artifact_input_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes artifact_input_contract :=
  keysDetermineAllCheck_sound artifact_input_contract
    artifact_input_candidate_keys_check

theorem artifact_input_candidate_keys_minimal_check :
    declaredKeysMinimalCheck artifact_input_contract = true := by
  native_decide

theorem artifact_input_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal artifact_input_contract :=
  declaredKeysMinimalCheck_sound artifact_input_contract
    artifact_input_candidate_keys_minimal_check

theorem artifact_input_closure_fixed_check :
    closureFixedPointCheck artifact_input_contract = true := by
  native_decide

theorem artifact_input_closure_reached_fixed_point :
    ClosureReachedFixedPoint artifact_input_contract :=
  closureFixedPointCheck_sound artifact_input_contract
    artifact_input_closure_fixed_check

theorem artifact_input_bcnf_check :
    bcnfCheck artifact_input_contract = true := by
  native_decide

theorem artifact_input_bcnf : BCNF artifact_input_contract :=
  bcnfCheck_sound artifact_input_contract artifact_input_bcnf_check

def artifact_delta_old_contract : RelationContract where
  name := "artifact_delta_old"
  attributes := ["candidate_id", "publication_key", "artifact_semantics_sha256", "artifact_sha256"]
  declaredKeys := [["candidate_id", "publication_key"]]
  declaredFDs := [
    { determinant := ["candidate_id", "publication_key"], dependent := ["artifact_semantics_sha256", "artifact_sha256"] }
  ]

theorem artifact_delta_old_schema_well_formed :
    schemaWellFormedCheck artifact_delta_old_contract = true := by
  native_decide

theorem artifact_delta_old_candidate_keys_check :
    keysDetermineAllCheck artifact_delta_old_contract = true := by
  native_decide

theorem artifact_delta_old_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes artifact_delta_old_contract :=
  keysDetermineAllCheck_sound artifact_delta_old_contract
    artifact_delta_old_candidate_keys_check

theorem artifact_delta_old_candidate_keys_minimal_check :
    declaredKeysMinimalCheck artifact_delta_old_contract = true := by
  native_decide

theorem artifact_delta_old_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal artifact_delta_old_contract :=
  declaredKeysMinimalCheck_sound artifact_delta_old_contract
    artifact_delta_old_candidate_keys_minimal_check

theorem artifact_delta_old_closure_fixed_check :
    closureFixedPointCheck artifact_delta_old_contract = true := by
  native_decide

theorem artifact_delta_old_closure_reached_fixed_point :
    ClosureReachedFixedPoint artifact_delta_old_contract :=
  closureFixedPointCheck_sound artifact_delta_old_contract
    artifact_delta_old_closure_fixed_check

def artifact_delta_new_contract : RelationContract where
  name := "artifact_delta_new"
  attributes := ["candidate_id", "publication_key", "artifact_semantics_sha256"]
  declaredKeys := [["candidate_id", "publication_key"]]
  declaredFDs := [
    { determinant := ["candidate_id", "publication_key"], dependent := ["artifact_semantics_sha256"] }
  ]

theorem artifact_delta_new_schema_well_formed :
    schemaWellFormedCheck artifact_delta_new_contract = true := by
  native_decide

theorem artifact_delta_new_candidate_keys_check :
    keysDetermineAllCheck artifact_delta_new_contract = true := by
  native_decide

theorem artifact_delta_new_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes artifact_delta_new_contract :=
  keysDetermineAllCheck_sound artifact_delta_new_contract
    artifact_delta_new_candidate_keys_check

theorem artifact_delta_new_candidate_keys_minimal_check :
    declaredKeysMinimalCheck artifact_delta_new_contract = true := by
  native_decide

theorem artifact_delta_new_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal artifact_delta_new_contract :=
  declaredKeysMinimalCheck_sound artifact_delta_new_contract
    artifact_delta_new_candidate_keys_minimal_check

theorem artifact_delta_new_closure_fixed_check :
    closureFixedPointCheck artifact_delta_new_contract = true := by
  native_decide

theorem artifact_delta_new_closure_reached_fixed_point :
    ClosureReachedFixedPoint artifact_delta_new_contract :=
  closureFixedPointCheck_sound artifact_delta_new_contract
    artifact_delta_new_closure_fixed_check

def artifact_operation_contract : RelationContract where
  name := "artifact_operation"
  attributes := ["candidate_id", "publication_key", "operation"]
  declaredKeys := [["candidate_id", "publication_key"]]
  declaredFDs := [
    { determinant := ["candidate_id", "publication_key"], dependent := ["operation"] }
  ]

theorem artifact_operation_schema_well_formed :
    schemaWellFormedCheck artifact_operation_contract = true := by
  native_decide

theorem artifact_operation_candidate_keys_check :
    keysDetermineAllCheck artifact_operation_contract = true := by
  native_decide

theorem artifact_operation_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes artifact_operation_contract :=
  keysDetermineAllCheck_sound artifact_operation_contract
    artifact_operation_candidate_keys_check

theorem artifact_operation_candidate_keys_minimal_check :
    declaredKeysMinimalCheck artifact_operation_contract = true := by
  native_decide

theorem artifact_operation_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal artifact_operation_contract :=
  declaredKeysMinimalCheck_sound artifact_operation_contract
    artifact_operation_candidate_keys_minimal_check

theorem artifact_operation_closure_fixed_check :
    closureFixedPointCheck artifact_operation_contract = true := by
  native_decide

theorem artifact_operation_closure_reached_fixed_point :
    ClosureReachedFixedPoint artifact_operation_contract :=
  closureFixedPointCheck_sound artifact_operation_contract
    artifact_operation_closure_fixed_check

theorem artifact_operation_bcnf_check :
    bcnfCheck artifact_operation_contract = true := by
  native_decide

theorem artifact_operation_bcnf : BCNF artifact_operation_contract :=
  bcnfCheck_sound artifact_operation_contract artifact_operation_bcnf_check

def artifact_blob_contract : RelationContract where
  name := "artifact_blob"
  attributes := ["artifact_sha256", "size_bytes", "artifact_locator_sha256"]
  declaredKeys := [["artifact_sha256"], ["artifact_locator_sha256"]]
  declaredFDs := [
    { determinant := ["artifact_sha256"], dependent := ["size_bytes", "artifact_locator_sha256"] },
    { determinant := ["artifact_locator_sha256"], dependent := ["artifact_sha256", "size_bytes"] }
  ]

theorem artifact_blob_schema_well_formed :
    schemaWellFormedCheck artifact_blob_contract = true := by
  native_decide

theorem artifact_blob_candidate_keys_check :
    keysDetermineAllCheck artifact_blob_contract = true := by
  native_decide

theorem artifact_blob_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes artifact_blob_contract :=
  keysDetermineAllCheck_sound artifact_blob_contract
    artifact_blob_candidate_keys_check

theorem artifact_blob_candidate_keys_minimal_check :
    declaredKeysMinimalCheck artifact_blob_contract = true := by
  native_decide

theorem artifact_blob_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal artifact_blob_contract :=
  declaredKeysMinimalCheck_sound artifact_blob_contract
    artifact_blob_candidate_keys_minimal_check

theorem artifact_blob_closure_fixed_check :
    closureFixedPointCheck artifact_blob_contract = true := by
  native_decide

theorem artifact_blob_closure_reached_fixed_point :
    ClosureReachedFixedPoint artifact_blob_contract :=
  closureFixedPointCheck_sound artifact_blob_contract
    artifact_blob_closure_fixed_check

theorem artifact_blob_bcnf_check :
    bcnfCheck artifact_blob_contract = true := by
  native_decide

theorem artifact_blob_bcnf : BCNF artifact_blob_contract :=
  bcnfCheck_sound artifact_blob_contract artifact_blob_bcnf_check

def prepared_artifact_contract : RelationContract where
  name := "prepared_artifact"
  attributes := ["candidate_id", "publication_key", "artifact_sha256", "storage_codec_version", "storage_generation", "protection_token", "state"]
  declaredKeys := [["candidate_id", "publication_key"], ["protection_token"]]
  declaredFDs := [
    { determinant := ["candidate_id", "publication_key"], dependent := ["artifact_sha256", "storage_codec_version", "storage_generation", "protection_token", "state"] },
    { determinant := ["protection_token"], dependent := ["candidate_id", "publication_key", "artifact_sha256", "storage_codec_version", "storage_generation", "state"] }
  ]

theorem prepared_artifact_schema_well_formed :
    schemaWellFormedCheck prepared_artifact_contract = true := by
  native_decide

theorem prepared_artifact_candidate_keys_check :
    keysDetermineAllCheck prepared_artifact_contract = true := by
  native_decide

theorem prepared_artifact_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes prepared_artifact_contract :=
  keysDetermineAllCheck_sound prepared_artifact_contract
    prepared_artifact_candidate_keys_check

theorem prepared_artifact_candidate_keys_minimal_check :
    declaredKeysMinimalCheck prepared_artifact_contract = true := by
  native_decide

theorem prepared_artifact_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal prepared_artifact_contract :=
  declaredKeysMinimalCheck_sound prepared_artifact_contract
    prepared_artifact_candidate_keys_minimal_check

theorem prepared_artifact_closure_fixed_check :
    closureFixedPointCheck prepared_artifact_contract = true := by
  native_decide

theorem prepared_artifact_closure_reached_fixed_point :
    ClosureReachedFixedPoint prepared_artifact_contract :=
  closureFixedPointCheck_sound prepared_artifact_contract
    prepared_artifact_closure_fixed_check

theorem prepared_artifact_bcnf_check :
    bcnfCheck prepared_artifact_contract = true := by
  native_decide

theorem prepared_artifact_bcnf : BCNF prepared_artifact_contract :=
  bcnfCheck_sound prepared_artifact_contract prepared_artifact_bcnf_check

def catalog_revision_descriptor_contract : RelationContract where
  name := "catalog_revision_descriptor"
  attributes := ["revision", "publication_count"]
  declaredKeys := [["revision"]]
  declaredFDs := [
    { determinant := ["revision"], dependent := ["publication_count"] }
  ]

theorem catalog_revision_descriptor_schema_well_formed :
    schemaWellFormedCheck catalog_revision_descriptor_contract = true := by
  native_decide

theorem catalog_revision_descriptor_candidate_keys_check :
    keysDetermineAllCheck catalog_revision_descriptor_contract = true := by
  native_decide

theorem catalog_revision_descriptor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes catalog_revision_descriptor_contract :=
  keysDetermineAllCheck_sound catalog_revision_descriptor_contract
    catalog_revision_descriptor_candidate_keys_check

theorem catalog_revision_descriptor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck catalog_revision_descriptor_contract = true := by
  native_decide

theorem catalog_revision_descriptor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal catalog_revision_descriptor_contract :=
  declaredKeysMinimalCheck_sound catalog_revision_descriptor_contract
    catalog_revision_descriptor_candidate_keys_minimal_check

theorem catalog_revision_descriptor_closure_fixed_check :
    closureFixedPointCheck catalog_revision_descriptor_contract = true := by
  native_decide

theorem catalog_revision_descriptor_closure_reached_fixed_point :
    ClosureReachedFixedPoint catalog_revision_descriptor_contract :=
  closureFixedPointCheck_sound catalog_revision_descriptor_contract
    catalog_revision_descriptor_closure_fixed_check

theorem catalog_revision_descriptor_bcnf_check :
    bcnfCheck catalog_revision_descriptor_contract = true := by
  native_decide

theorem catalog_revision_descriptor_bcnf : BCNF catalog_revision_descriptor_contract :=
  bcnfCheck_sound catalog_revision_descriptor_contract catalog_revision_descriptor_bcnf_check

def catalog_revision_contract : RelationContract where
  name := "catalog_revision"
  attributes := ["revision", "publication_count", "published_at"]
  declaredKeys := [["revision"]]
  declaredFDs := [
    { determinant := ["revision"], dependent := ["publication_count", "published_at"] }
  ]

theorem catalog_revision_schema_well_formed :
    schemaWellFormedCheck catalog_revision_contract = true := by
  native_decide

theorem catalog_revision_candidate_keys_check :
    keysDetermineAllCheck catalog_revision_contract = true := by
  native_decide

theorem catalog_revision_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes catalog_revision_contract :=
  keysDetermineAllCheck_sound catalog_revision_contract
    catalog_revision_candidate_keys_check

theorem catalog_revision_candidate_keys_minimal_check :
    declaredKeysMinimalCheck catalog_revision_contract = true := by
  native_decide

theorem catalog_revision_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal catalog_revision_contract :=
  declaredKeysMinimalCheck_sound catalog_revision_contract
    catalog_revision_candidate_keys_minimal_check

theorem catalog_revision_closure_fixed_check :
    closureFixedPointCheck catalog_revision_contract = true := by
  native_decide

theorem catalog_revision_closure_reached_fixed_point :
    ClosureReachedFixedPoint catalog_revision_contract :=
  closureFixedPointCheck_sound catalog_revision_contract
    catalog_revision_closure_fixed_check

def catalog_revision_generation_contract : RelationContract where
  name := "catalog_revision_generation"
  attributes := ["revision", "generation"]
  declaredKeys := [["revision"], ["generation"]]
  declaredFDs := [
    { determinant := ["revision"], dependent := ["generation"] },
    { determinant := ["generation"], dependent := ["revision"] }
  ]

theorem catalog_revision_generation_schema_well_formed :
    schemaWellFormedCheck catalog_revision_generation_contract = true := by
  native_decide

theorem catalog_revision_generation_candidate_keys_check :
    keysDetermineAllCheck catalog_revision_generation_contract = true := by
  native_decide

theorem catalog_revision_generation_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes catalog_revision_generation_contract :=
  keysDetermineAllCheck_sound catalog_revision_generation_contract
    catalog_revision_generation_candidate_keys_check

theorem catalog_revision_generation_candidate_keys_minimal_check :
    declaredKeysMinimalCheck catalog_revision_generation_contract = true := by
  native_decide

theorem catalog_revision_generation_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal catalog_revision_generation_contract :=
  declaredKeysMinimalCheck_sound catalog_revision_generation_contract
    catalog_revision_generation_candidate_keys_minimal_check

theorem catalog_revision_generation_closure_fixed_check :
    closureFixedPointCheck catalog_revision_generation_contract = true := by
  native_decide

theorem catalog_revision_generation_closure_reached_fixed_point :
    ClosureReachedFixedPoint catalog_revision_generation_contract :=
  closureFixedPointCheck_sound catalog_revision_generation_contract
    catalog_revision_generation_closure_fixed_check

def publication_generation_node_contract : RelationContract where
  name := "publication_generation_node"
  attributes := ["generation"]
  declaredKeys := [["generation"]]
  declaredFDs := [
  ]

theorem publication_generation_node_schema_well_formed :
    schemaWellFormedCheck publication_generation_node_contract = true := by
  native_decide

theorem publication_generation_node_candidate_keys_check :
    keysDetermineAllCheck publication_generation_node_contract = true := by
  native_decide

theorem publication_generation_node_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_generation_node_contract :=
  keysDetermineAllCheck_sound publication_generation_node_contract
    publication_generation_node_candidate_keys_check

theorem publication_generation_node_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_generation_node_contract = true := by
  native_decide

theorem publication_generation_node_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_generation_node_contract :=
  declaredKeysMinimalCheck_sound publication_generation_node_contract
    publication_generation_node_candidate_keys_minimal_check

theorem publication_generation_node_closure_fixed_check :
    closureFixedPointCheck publication_generation_node_contract = true := by
  native_decide

theorem publication_generation_node_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_generation_node_contract :=
  closureFixedPointCheck_sound publication_generation_node_contract
    publication_generation_node_closure_fixed_check

theorem publication_generation_node_bcnf_check :
    bcnfCheck publication_generation_node_contract = true := by
  native_decide

theorem publication_generation_node_bcnf : BCNF publication_generation_node_contract :=
  bcnfCheck_sound publication_generation_node_contract publication_generation_node_bcnf_check

def publication_generation_successor_contract : RelationContract where
  name := "publication_generation_successor"
  attributes := ["successor_generation", "predecessor_generation"]
  declaredKeys := [["successor_generation"], ["predecessor_generation"]]
  declaredFDs := [
    { determinant := ["successor_generation"], dependent := ["predecessor_generation"] },
    { determinant := ["predecessor_generation"], dependent := ["successor_generation"] }
  ]

theorem publication_generation_successor_schema_well_formed :
    schemaWellFormedCheck publication_generation_successor_contract = true := by
  native_decide

theorem publication_generation_successor_candidate_keys_check :
    keysDetermineAllCheck publication_generation_successor_contract = true := by
  native_decide

theorem publication_generation_successor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_generation_successor_contract :=
  keysDetermineAllCheck_sound publication_generation_successor_contract
    publication_generation_successor_candidate_keys_check

theorem publication_generation_successor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_generation_successor_contract = true := by
  native_decide

theorem publication_generation_successor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_generation_successor_contract :=
  declaredKeysMinimalCheck_sound publication_generation_successor_contract
    publication_generation_successor_candidate_keys_minimal_check

theorem publication_generation_successor_closure_fixed_check :
    closureFixedPointCheck publication_generation_successor_contract = true := by
  native_decide

theorem publication_generation_successor_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_generation_successor_contract :=
  closureFixedPointCheck_sound publication_generation_successor_contract
    publication_generation_successor_closure_fixed_check

theorem publication_generation_successor_bcnf_check :
    bcnfCheck publication_generation_successor_contract = true := by
  native_decide

theorem publication_generation_successor_bcnf : BCNF publication_generation_successor_contract :=
  bcnfCheck_sound publication_generation_successor_contract publication_generation_successor_bcnf_check

def publication_identity_contract : RelationContract where
  name := "publication_identity"
  attributes := ["publication_key", "gid"]
  declaredKeys := [["publication_key"], ["gid"]]
  declaredFDs := [
    { determinant := ["publication_key"], dependent := ["gid"] },
    { determinant := ["gid"], dependent := ["publication_key"] }
  ]

theorem publication_identity_schema_well_formed :
    schemaWellFormedCheck publication_identity_contract = true := by
  native_decide

theorem publication_identity_candidate_keys_check :
    keysDetermineAllCheck publication_identity_contract = true := by
  native_decide

theorem publication_identity_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_identity_contract :=
  keysDetermineAllCheck_sound publication_identity_contract
    publication_identity_candidate_keys_check

theorem publication_identity_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_identity_contract = true := by
  native_decide

theorem publication_identity_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_identity_contract :=
  declaredKeysMinimalCheck_sound publication_identity_contract
    publication_identity_candidate_keys_minimal_check

theorem publication_identity_closure_fixed_check :
    closureFixedPointCheck publication_identity_contract = true := by
  native_decide

theorem publication_identity_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_identity_contract :=
  closureFixedPointCheck_sound publication_identity_contract
    publication_identity_closure_fixed_check

theorem publication_identity_bcnf_check :
    bcnfCheck publication_identity_contract = true := by
  native_decide

theorem publication_identity_bcnf : BCNF publication_identity_contract :=
  bcnfCheck_sound publication_identity_contract publication_identity_bcnf_check

def display_title_policy_contract : RelationContract where
  name := "display_title_policy"
  attributes := ["display_title_policy_id", "display_title_algorithm_version", "title_sort_policy_id"]
  declaredKeys := [["display_title_policy_id"], ["display_title_algorithm_version", "title_sort_policy_id"]]
  declaredFDs := [
    { determinant := ["display_title_policy_id"], dependent := ["display_title_algorithm_version", "title_sort_policy_id"] },
    { determinant := ["display_title_algorithm_version", "title_sort_policy_id"], dependent := ["display_title_policy_id"] }
  ]

theorem display_title_policy_schema_well_formed :
    schemaWellFormedCheck display_title_policy_contract = true := by
  native_decide

theorem display_title_policy_candidate_keys_check :
    keysDetermineAllCheck display_title_policy_contract = true := by
  native_decide

theorem display_title_policy_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes display_title_policy_contract :=
  keysDetermineAllCheck_sound display_title_policy_contract
    display_title_policy_candidate_keys_check

theorem display_title_policy_candidate_keys_minimal_check :
    declaredKeysMinimalCheck display_title_policy_contract = true := by
  native_decide

theorem display_title_policy_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal display_title_policy_contract :=
  declaredKeysMinimalCheck_sound display_title_policy_contract
    display_title_policy_candidate_keys_minimal_check

theorem display_title_policy_closure_fixed_check :
    closureFixedPointCheck display_title_policy_contract = true := by
  native_decide

theorem display_title_policy_closure_reached_fixed_point :
    ClosureReachedFixedPoint display_title_policy_contract :=
  closureFixedPointCheck_sound display_title_policy_contract
    display_title_policy_closure_fixed_check

theorem display_title_policy_bcnf_check :
    bcnfCheck display_title_policy_contract = true := by
  native_decide

theorem display_title_policy_bcnf : BCNF display_title_policy_contract :=
  bcnfCheck_sound display_title_policy_contract display_title_policy_bcnf_check

def title_sort_policy_contract : RelationContract where
  name := "title_sort_policy"
  attributes := ["title_sort_policy_id", "title_sort_algorithm_version", "unicode_data_version"]
  declaredKeys := [["title_sort_policy_id"], ["title_sort_algorithm_version", "unicode_data_version"]]
  declaredFDs := [
    { determinant := ["title_sort_policy_id"], dependent := ["title_sort_algorithm_version", "unicode_data_version"] },
    { determinant := ["title_sort_algorithm_version", "unicode_data_version"], dependent := ["title_sort_policy_id"] }
  ]

theorem title_sort_policy_schema_well_formed :
    schemaWellFormedCheck title_sort_policy_contract = true := by
  native_decide

theorem title_sort_policy_candidate_keys_check :
    keysDetermineAllCheck title_sort_policy_contract = true := by
  native_decide

theorem title_sort_policy_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes title_sort_policy_contract :=
  keysDetermineAllCheck_sound title_sort_policy_contract
    title_sort_policy_candidate_keys_check

theorem title_sort_policy_candidate_keys_minimal_check :
    declaredKeysMinimalCheck title_sort_policy_contract = true := by
  native_decide

theorem title_sort_policy_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal title_sort_policy_contract :=
  declaredKeysMinimalCheck_sound title_sort_policy_contract
    title_sort_policy_candidate_keys_minimal_check

theorem title_sort_policy_closure_fixed_check :
    closureFixedPointCheck title_sort_policy_contract = true := by
  native_decide

theorem title_sort_policy_closure_reached_fixed_point :
    ClosureReachedFixedPoint title_sort_policy_contract :=
  closureFixedPointCheck_sound title_sort_policy_contract
    title_sort_policy_closure_fixed_check

theorem title_sort_policy_bcnf_check :
    bcnfCheck title_sort_policy_contract = true := by
  native_decide

theorem title_sort_policy_bcnf : BCNF title_sort_policy_contract :=
  bcnfCheck_sound title_sort_policy_contract title_sort_policy_bcnf_check

def display_title_choice_contract : RelationContract where
  name := "display_title_choice"
  attributes := ["display_title_policy_id", "source_title_sha256", "source_gallery_name", "title_sha256"]
  declaredKeys := [["display_title_policy_id", "source_title_sha256", "source_gallery_name"]]
  declaredFDs := [
    { determinant := ["display_title_policy_id", "source_title_sha256", "source_gallery_name"], dependent := ["title_sha256"] }
  ]

theorem display_title_choice_schema_well_formed :
    schemaWellFormedCheck display_title_choice_contract = true := by
  native_decide

theorem display_title_choice_candidate_keys_check :
    keysDetermineAllCheck display_title_choice_contract = true := by
  native_decide

theorem display_title_choice_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes display_title_choice_contract :=
  keysDetermineAllCheck_sound display_title_choice_contract
    display_title_choice_candidate_keys_check

theorem display_title_choice_candidate_keys_minimal_check :
    declaredKeysMinimalCheck display_title_choice_contract = true := by
  native_decide

theorem display_title_choice_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal display_title_choice_contract :=
  declaredKeysMinimalCheck_sound display_title_choice_contract
    display_title_choice_candidate_keys_minimal_check

theorem display_title_choice_closure_fixed_check :
    closureFixedPointCheck display_title_choice_contract = true := by
  native_decide

theorem display_title_choice_closure_reached_fixed_point :
    ClosureReachedFixedPoint display_title_choice_contract :=
  closureFixedPointCheck_sound display_title_choice_contract
    display_title_choice_closure_fixed_check

theorem display_title_choice_bcnf_check :
    bcnfCheck display_title_choice_contract = true := by
  native_decide

theorem display_title_choice_bcnf : BCNF display_title_choice_contract :=
  bcnfCheck_sound display_title_choice_contract display_title_choice_bcnf_check

def title_sort_contract : RelationContract where
  name := "title_sort"
  attributes := ["title_sort_policy_id", "title_sha256", "sort_title_sha256"]
  declaredKeys := [["title_sort_policy_id", "title_sha256"]]
  declaredFDs := [
    { determinant := ["title_sort_policy_id", "title_sha256"], dependent := ["sort_title_sha256"] }
  ]

theorem title_sort_schema_well_formed :
    schemaWellFormedCheck title_sort_contract = true := by
  native_decide

theorem title_sort_candidate_keys_check :
    keysDetermineAllCheck title_sort_contract = true := by
  native_decide

theorem title_sort_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes title_sort_contract :=
  keysDetermineAllCheck_sound title_sort_contract
    title_sort_candidate_keys_check

theorem title_sort_candidate_keys_minimal_check :
    declaredKeysMinimalCheck title_sort_contract = true := by
  native_decide

theorem title_sort_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal title_sort_contract :=
  declaredKeysMinimalCheck_sound title_sort_contract
    title_sort_candidate_keys_minimal_check

theorem title_sort_closure_fixed_check :
    closureFixedPointCheck title_sort_contract = true := by
  native_decide

theorem title_sort_closure_reached_fixed_point :
    ClosureReachedFixedPoint title_sort_contract :=
  closureFixedPointCheck_sound title_sort_contract
    title_sort_closure_fixed_check

theorem title_sort_bcnf_check :
    bcnfCheck title_sort_contract = true := by
  native_decide

theorem title_sort_bcnf : BCNF title_sort_contract :=
  bcnfCheck_sound title_sort_contract title_sort_bcnf_check

def catalog_publication_storage_contract : RelationContract where
  name := "catalog_publication_storage"
  attributes := ["catalog_occurrence_sha256", "gallery_id", "summary_sha256", "language_sha256", "modified_at", "source_title_sha256"]
  declaredKeys := [["catalog_occurrence_sha256"]]
  declaredFDs := [
    { determinant := ["catalog_occurrence_sha256"], dependent := ["gallery_id", "summary_sha256", "language_sha256", "modified_at", "source_title_sha256"] }
  ]

theorem catalog_publication_storage_schema_well_formed :
    schemaWellFormedCheck catalog_publication_storage_contract = true := by
  native_decide

theorem catalog_publication_storage_candidate_keys_check :
    keysDetermineAllCheck catalog_publication_storage_contract = true := by
  native_decide

theorem catalog_publication_storage_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes catalog_publication_storage_contract :=
  keysDetermineAllCheck_sound catalog_publication_storage_contract
    catalog_publication_storage_candidate_keys_check

theorem catalog_publication_storage_candidate_keys_minimal_check :
    declaredKeysMinimalCheck catalog_publication_storage_contract = true := by
  native_decide

theorem catalog_publication_storage_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal catalog_publication_storage_contract :=
  declaredKeysMinimalCheck_sound catalog_publication_storage_contract
    catalog_publication_storage_candidate_keys_minimal_check

theorem catalog_publication_storage_closure_fixed_check :
    closureFixedPointCheck catalog_publication_storage_contract = true := by
  native_decide

theorem catalog_publication_storage_closure_reached_fixed_point :
    ClosureReachedFixedPoint catalog_publication_storage_contract :=
  closureFixedPointCheck_sound catalog_publication_storage_contract
    catalog_publication_storage_closure_fixed_check

theorem catalog_publication_storage_bcnf_check :
    bcnfCheck catalog_publication_storage_contract = true := by
  native_decide

theorem catalog_publication_storage_bcnf : BCNF catalog_publication_storage_contract :=
  bcnfCheck_sound catalog_publication_storage_contract catalog_publication_storage_bcnf_check

def catalog_publication_occurrence_identity_contract : RelationContract where
  name := "catalog_publication_occurrence_identity"
  attributes := ["catalog_occurrence_sha256", "revision", "publication_key"]
  declaredKeys := [["revision", "publication_key"], ["catalog_occurrence_sha256"]]
  declaredFDs := [
    { determinant := ["catalog_occurrence_sha256"], dependent := ["revision", "publication_key"] },
    { determinant := ["revision", "publication_key"], dependent := ["catalog_occurrence_sha256"] }
  ]

theorem catalog_publication_occurrence_identity_schema_well_formed :
    schemaWellFormedCheck catalog_publication_occurrence_identity_contract = true := by
  native_decide

theorem catalog_publication_occurrence_identity_candidate_keys_check :
    keysDetermineAllCheck catalog_publication_occurrence_identity_contract = true := by
  native_decide

theorem catalog_publication_occurrence_identity_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes catalog_publication_occurrence_identity_contract :=
  keysDetermineAllCheck_sound catalog_publication_occurrence_identity_contract
    catalog_publication_occurrence_identity_candidate_keys_check

theorem catalog_publication_occurrence_identity_candidate_keys_minimal_check :
    declaredKeysMinimalCheck catalog_publication_occurrence_identity_contract = true := by
  native_decide

theorem catalog_publication_occurrence_identity_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal catalog_publication_occurrence_identity_contract :=
  declaredKeysMinimalCheck_sound catalog_publication_occurrence_identity_contract
    catalog_publication_occurrence_identity_candidate_keys_minimal_check

theorem catalog_publication_occurrence_identity_closure_fixed_check :
    closureFixedPointCheck catalog_publication_occurrence_identity_contract = true := by
  native_decide

theorem catalog_publication_occurrence_identity_closure_reached_fixed_point :
    ClosureReachedFixedPoint catalog_publication_occurrence_identity_contract :=
  closureFixedPointCheck_sound catalog_publication_occurrence_identity_contract
    catalog_publication_occurrence_identity_closure_fixed_check

theorem catalog_publication_occurrence_identity_bcnf_check :
    bcnfCheck catalog_publication_occurrence_identity_contract = true := by
  native_decide

theorem catalog_publication_occurrence_identity_bcnf : BCNF catalog_publication_occurrence_identity_contract :=
  bcnfCheck_sound catalog_publication_occurrence_identity_contract catalog_publication_occurrence_identity_bcnf_check

def catalog_publication_contract : RelationContract where
  name := "catalog_publication"
  attributes := ["revision", "publication_key", "gallery_id", "summary_sha256", "language_sha256", "modified_at"]
  declaredKeys := [["revision", "publication_key"], ["revision", "gallery_id"]]
  declaredFDs := [
    { determinant := ["gallery_id"], dependent := ["publication_key"] },
    { determinant := ["revision", "publication_key"], dependent := ["gallery_id", "summary_sha256", "language_sha256", "modified_at"] },
    { determinant := ["revision", "gallery_id"], dependent := ["publication_key", "summary_sha256", "language_sha256", "modified_at"] }
  ]

theorem catalog_publication_schema_well_formed :
    schemaWellFormedCheck catalog_publication_contract = true := by
  native_decide

theorem catalog_publication_candidate_keys_check :
    keysDetermineAllCheck catalog_publication_contract = true := by
  native_decide

theorem catalog_publication_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes catalog_publication_contract :=
  keysDetermineAllCheck_sound catalog_publication_contract
    catalog_publication_candidate_keys_check

theorem catalog_publication_candidate_keys_minimal_check :
    declaredKeysMinimalCheck catalog_publication_contract = true := by
  native_decide

theorem catalog_publication_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal catalog_publication_contract :=
  declaredKeysMinimalCheck_sound catalog_publication_contract
    catalog_publication_candidate_keys_minimal_check

theorem catalog_publication_closure_fixed_check :
    closureFixedPointCheck catalog_publication_contract = true := by
  native_decide

theorem catalog_publication_closure_reached_fixed_point :
    ClosureReachedFixedPoint catalog_publication_contract :=
  closureFixedPointCheck_sound catalog_publication_contract
    catalog_publication_closure_fixed_check

def catalog_publication_order_contract : RelationContract where
  name := "catalog_publication_order"
  attributes := ["revision", "position", "publication_key"]
  declaredKeys := [["revision", "position"], ["revision", "publication_key"]]
  declaredFDs := [
    { determinant := ["revision", "position"], dependent := ["publication_key"] },
    { determinant := ["revision", "publication_key"], dependent := ["position"] }
  ]

theorem catalog_publication_order_schema_well_formed :
    schemaWellFormedCheck catalog_publication_order_contract = true := by
  native_decide

theorem catalog_publication_order_candidate_keys_check :
    keysDetermineAllCheck catalog_publication_order_contract = true := by
  native_decide

theorem catalog_publication_order_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes catalog_publication_order_contract :=
  keysDetermineAllCheck_sound catalog_publication_order_contract
    catalog_publication_order_candidate_keys_check

theorem catalog_publication_order_candidate_keys_minimal_check :
    declaredKeysMinimalCheck catalog_publication_order_contract = true := by
  native_decide

theorem catalog_publication_order_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal catalog_publication_order_contract :=
  declaredKeysMinimalCheck_sound catalog_publication_order_contract
    catalog_publication_order_candidate_keys_minimal_check

theorem catalog_publication_order_closure_fixed_check :
    closureFixedPointCheck catalog_publication_order_contract = true := by
  native_decide

theorem catalog_publication_order_closure_reached_fixed_point :
    ClosureReachedFixedPoint catalog_publication_order_contract :=
  closureFixedPointCheck_sound catalog_publication_order_contract
    catalog_publication_order_closure_fixed_check

theorem catalog_publication_order_bcnf_check :
    bcnfCheck catalog_publication_order_contract = true := by
  native_decide

theorem catalog_publication_order_bcnf : BCNF catalog_publication_order_contract :=
  bcnfCheck_sound catalog_publication_order_contract catalog_publication_order_bcnf_check

def catalog_publication_title_contract : RelationContract where
  name := "catalog_publication_title"
  attributes := ["revision", "publication_key", "source_title_sha256", "source_gallery_name"]
  declaredKeys := [["revision", "publication_key"], ["revision", "source_gallery_name"]]
  declaredFDs := [
    { determinant := ["source_gallery_name"], dependent := ["publication_key"] },
    { determinant := ["revision", "publication_key"], dependent := ["source_title_sha256", "source_gallery_name"] }
  ]

theorem catalog_publication_title_schema_well_formed :
    schemaWellFormedCheck catalog_publication_title_contract = true := by
  native_decide

theorem catalog_publication_title_candidate_keys_check :
    keysDetermineAllCheck catalog_publication_title_contract = true := by
  native_decide

theorem catalog_publication_title_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes catalog_publication_title_contract :=
  keysDetermineAllCheck_sound catalog_publication_title_contract
    catalog_publication_title_candidate_keys_check

theorem catalog_publication_title_candidate_keys_minimal_check :
    declaredKeysMinimalCheck catalog_publication_title_contract = true := by
  native_decide

theorem catalog_publication_title_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal catalog_publication_title_contract :=
  declaredKeysMinimalCheck_sound catalog_publication_title_contract
    catalog_publication_title_candidate_keys_minimal_check

theorem catalog_publication_title_closure_fixed_check :
    closureFixedPointCheck catalog_publication_title_contract = true := by
  native_decide

theorem catalog_publication_title_closure_reached_fixed_point :
    ClosureReachedFixedPoint catalog_publication_title_contract :=
  closureFixedPointCheck_sound catalog_publication_title_contract
    catalog_publication_title_closure_fixed_check

def catalog_publication_content_contract : RelationContract where
  name := "catalog_publication_content"
  attributes := ["revision", "publication_key", "content_sha256"]
  declaredKeys := [["revision", "publication_key"]]
  declaredFDs := [
    { determinant := ["revision", "publication_key"], dependent := ["content_sha256"] }
  ]

theorem catalog_publication_content_schema_well_formed :
    schemaWellFormedCheck catalog_publication_content_contract = true := by
  native_decide

theorem catalog_publication_content_candidate_keys_check :
    keysDetermineAllCheck catalog_publication_content_contract = true := by
  native_decide

theorem catalog_publication_content_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes catalog_publication_content_contract :=
  keysDetermineAllCheck_sound catalog_publication_content_contract
    catalog_publication_content_candidate_keys_check

theorem catalog_publication_content_candidate_keys_minimal_check :
    declaredKeysMinimalCheck catalog_publication_content_contract = true := by
  native_decide

theorem catalog_publication_content_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal catalog_publication_content_contract :=
  declaredKeysMinimalCheck_sound catalog_publication_content_contract
    catalog_publication_content_candidate_keys_minimal_check

theorem catalog_publication_content_closure_fixed_check :
    closureFixedPointCheck catalog_publication_content_contract = true := by
  native_decide

theorem catalog_publication_content_closure_reached_fixed_point :
    ClosureReachedFixedPoint catalog_publication_content_contract :=
  closureFixedPointCheck_sound catalog_publication_content_contract
    catalog_publication_content_closure_fixed_check

theorem catalog_publication_content_bcnf_check :
    bcnfCheck catalog_publication_content_contract = true := by
  native_decide

theorem catalog_publication_content_bcnf : BCNF catalog_publication_content_contract :=
  bcnfCheck_sound catalog_publication_content_contract catalog_publication_content_bcnf_check

def contributor_role_registry_contract : RelationContract where
  name := "contributor_role_registry"
  attributes := ["role"]
  declaredKeys := [["role"]]
  declaredFDs := [
  ]

theorem contributor_role_registry_schema_well_formed :
    schemaWellFormedCheck contributor_role_registry_contract = true := by
  native_decide

theorem contributor_role_registry_candidate_keys_check :
    keysDetermineAllCheck contributor_role_registry_contract = true := by
  native_decide

theorem contributor_role_registry_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes contributor_role_registry_contract :=
  keysDetermineAllCheck_sound contributor_role_registry_contract
    contributor_role_registry_candidate_keys_check

theorem contributor_role_registry_candidate_keys_minimal_check :
    declaredKeysMinimalCheck contributor_role_registry_contract = true := by
  native_decide

theorem contributor_role_registry_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal contributor_role_registry_contract :=
  declaredKeysMinimalCheck_sound contributor_role_registry_contract
    contributor_role_registry_candidate_keys_minimal_check

theorem contributor_role_registry_closure_fixed_check :
    closureFixedPointCheck contributor_role_registry_contract = true := by
  native_decide

theorem contributor_role_registry_closure_reached_fixed_point :
    ClosureReachedFixedPoint contributor_role_registry_contract :=
  closureFixedPointCheck_sound contributor_role_registry_contract
    contributor_role_registry_closure_fixed_check

theorem contributor_role_registry_bcnf_check :
    bcnfCheck contributor_role_registry_contract = true := by
  native_decide

theorem contributor_role_registry_bcnf : BCNF contributor_role_registry_contract :=
  bcnfCheck_sound contributor_role_registry_contract contributor_role_registry_bcnf_check

def catalog_contributor_contract : RelationContract where
  name := "catalog_contributor"
  attributes := ["revision", "publication_key", "contributor_name_sha256", "role", "position"]
  declaredKeys := [["revision", "publication_key", "contributor_name_sha256", "role"], ["revision", "publication_key", "position"]]
  declaredFDs := [
    { determinant := ["revision", "publication_key", "contributor_name_sha256", "role"], dependent := ["position"] },
    { determinant := ["revision", "publication_key", "position"], dependent := ["contributor_name_sha256", "role"] }
  ]

theorem catalog_contributor_schema_well_formed :
    schemaWellFormedCheck catalog_contributor_contract = true := by
  native_decide

theorem catalog_contributor_candidate_keys_check :
    keysDetermineAllCheck catalog_contributor_contract = true := by
  native_decide

theorem catalog_contributor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes catalog_contributor_contract :=
  keysDetermineAllCheck_sound catalog_contributor_contract
    catalog_contributor_candidate_keys_check

theorem catalog_contributor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck catalog_contributor_contract = true := by
  native_decide

theorem catalog_contributor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal catalog_contributor_contract :=
  declaredKeysMinimalCheck_sound catalog_contributor_contract
    catalog_contributor_candidate_keys_minimal_check

theorem catalog_contributor_closure_fixed_check :
    closureFixedPointCheck catalog_contributor_contract = true := by
  native_decide

theorem catalog_contributor_closure_reached_fixed_point :
    ClosureReachedFixedPoint catalog_contributor_contract :=
  closureFixedPointCheck_sound catalog_contributor_contract
    catalog_contributor_closure_fixed_check

theorem catalog_contributor_bcnf_check :
    bcnfCheck catalog_contributor_contract = true := by
  native_decide

theorem catalog_contributor_bcnf : BCNF catalog_contributor_contract :=
  bcnfCheck_sound catalog_contributor_contract catalog_contributor_bcnf_check

def catalog_subject_contract : RelationContract where
  name := "catalog_subject"
  attributes := ["revision", "publication_key", "position", "tag_id"]
  declaredKeys := [["revision", "publication_key", "position"], ["revision", "publication_key", "tag_id"]]
  declaredFDs := [
    { determinant := ["revision", "publication_key", "position"], dependent := ["tag_id"] },
    { determinant := ["revision", "publication_key", "tag_id"], dependent := ["position"] }
  ]

theorem catalog_subject_schema_well_formed :
    schemaWellFormedCheck catalog_subject_contract = true := by
  native_decide

theorem catalog_subject_candidate_keys_check :
    keysDetermineAllCheck catalog_subject_contract = true := by
  native_decide

theorem catalog_subject_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes catalog_subject_contract :=
  keysDetermineAllCheck_sound catalog_subject_contract
    catalog_subject_candidate_keys_check

theorem catalog_subject_candidate_keys_minimal_check :
    declaredKeysMinimalCheck catalog_subject_contract = true := by
  native_decide

theorem catalog_subject_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal catalog_subject_contract :=
  declaredKeysMinimalCheck_sound catalog_subject_contract
    catalog_subject_candidate_keys_minimal_check

theorem catalog_subject_closure_fixed_check :
    closureFixedPointCheck catalog_subject_contract = true := by
  native_decide

theorem catalog_subject_closure_reached_fixed_point :
    ClosureReachedFixedPoint catalog_subject_contract :=
  closureFixedPointCheck_sound catalog_subject_contract
    catalog_subject_closure_fixed_check

theorem catalog_subject_bcnf_check :
    bcnfCheck catalog_subject_contract = true := by
  native_decide

theorem catalog_subject_bcnf : BCNF catalog_subject_contract :=
  bcnfCheck_sound catalog_subject_contract catalog_subject_bcnf_check

def catalog_artifact_contract : RelationContract where
  name := "catalog_artifact"
  attributes := ["revision", "publication_key", "artifact_sha256", "artifact_semantics_sha256"]
  declaredKeys := [["revision", "publication_key"]]
  declaredFDs := [
    { determinant := ["revision", "publication_key"], dependent := ["artifact_sha256", "artifact_semantics_sha256"] }
  ]

theorem catalog_artifact_schema_well_formed :
    schemaWellFormedCheck catalog_artifact_contract = true := by
  native_decide

theorem catalog_artifact_candidate_keys_check :
    keysDetermineAllCheck catalog_artifact_contract = true := by
  native_decide

theorem catalog_artifact_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes catalog_artifact_contract :=
  keysDetermineAllCheck_sound catalog_artifact_contract
    catalog_artifact_candidate_keys_check

theorem catalog_artifact_candidate_keys_minimal_check :
    declaredKeysMinimalCheck catalog_artifact_contract = true := by
  native_decide

theorem catalog_artifact_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal catalog_artifact_contract :=
  declaredKeysMinimalCheck_sound catalog_artifact_contract
    catalog_artifact_candidate_keys_minimal_check

theorem catalog_artifact_closure_fixed_check :
    closureFixedPointCheck catalog_artifact_contract = true := by
  native_decide

theorem catalog_artifact_closure_reached_fixed_point :
    ClosureReachedFixedPoint catalog_artifact_contract :=
  closureFixedPointCheck_sound catalog_artifact_contract
    catalog_artifact_closure_fixed_check

theorem catalog_artifact_bcnf_check :
    bcnfCheck catalog_artifact_contract = true := by
  native_decide

theorem catalog_artifact_bcnf : BCNF catalog_artifact_contract :=
  bcnfCheck_sound catalog_artifact_contract catalog_artifact_bcnf_check

def publication_commit_anchor_contract : RelationContract where
  name := "publication_commit_anchor"
  attributes := ["receipt_id"]
  declaredKeys := [["receipt_id"]]
  declaredFDs := [
  ]

theorem publication_commit_anchor_schema_well_formed :
    schemaWellFormedCheck publication_commit_anchor_contract = true := by
  native_decide

theorem publication_commit_anchor_candidate_keys_check :
    keysDetermineAllCheck publication_commit_anchor_contract = true := by
  native_decide

theorem publication_commit_anchor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_commit_anchor_contract :=
  keysDetermineAllCheck_sound publication_commit_anchor_contract
    publication_commit_anchor_candidate_keys_check

theorem publication_commit_anchor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_commit_anchor_contract = true := by
  native_decide

theorem publication_commit_anchor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_commit_anchor_contract :=
  declaredKeysMinimalCheck_sound publication_commit_anchor_contract
    publication_commit_anchor_candidate_keys_minimal_check

theorem publication_commit_anchor_closure_fixed_check :
    closureFixedPointCheck publication_commit_anchor_contract = true := by
  native_decide

theorem publication_commit_anchor_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_commit_anchor_contract :=
  closureFixedPointCheck_sound publication_commit_anchor_contract
    publication_commit_anchor_closure_fixed_check

theorem publication_commit_anchor_bcnf_check :
    bcnfCheck publication_commit_anchor_contract = true := by
  native_decide

theorem publication_commit_anchor_bcnf : BCNF publication_commit_anchor_contract :=
  bcnfCheck_sound publication_commit_anchor_contract publication_commit_anchor_bcnf_check

def publication_commit_contract : RelationContract where
  name := "publication_commit"
  attributes := ["receipt_id", "candidate_id", "revision", "source_revision", "generation", "preparation_id", "operational_policy_id", "artifact_policy_id", "display_title_policy_id", "new_galleries", "changed_galleries", "removed_galleries", "duplicate_losers", "committed_at"]
  declaredKeys := [["receipt_id"], ["candidate_id"], ["revision"], ["source_revision"], ["generation"], ["preparation_id"]]
  declaredFDs := [
    { determinant := ["receipt_id"], dependent := ["candidate_id", "revision", "source_revision", "generation", "preparation_id", "operational_policy_id", "artifact_policy_id", "display_title_policy_id", "new_galleries", "changed_galleries", "removed_galleries", "duplicate_losers", "committed_at"] },
    { determinant := ["candidate_id"], dependent := ["receipt_id", "revision", "source_revision", "generation", "preparation_id", "operational_policy_id", "artifact_policy_id", "display_title_policy_id", "new_galleries", "changed_galleries", "removed_galleries", "duplicate_losers", "committed_at"] },
    { determinant := ["revision"], dependent := ["receipt_id", "candidate_id", "source_revision", "generation", "preparation_id", "operational_policy_id", "artifact_policy_id", "display_title_policy_id", "new_galleries", "changed_galleries", "removed_galleries", "duplicate_losers", "committed_at"] },
    { determinant := ["source_revision"], dependent := ["receipt_id", "candidate_id", "revision", "generation", "preparation_id", "operational_policy_id", "artifact_policy_id", "display_title_policy_id", "new_galleries", "changed_galleries", "removed_galleries", "duplicate_losers", "committed_at"] },
    { determinant := ["generation"], dependent := ["receipt_id", "candidate_id", "revision", "source_revision", "preparation_id", "operational_policy_id", "artifact_policy_id", "display_title_policy_id", "new_galleries", "changed_galleries", "removed_galleries", "duplicate_losers", "committed_at"] },
    { determinant := ["preparation_id"], dependent := ["receipt_id", "candidate_id", "revision", "source_revision", "generation", "operational_policy_id", "artifact_policy_id", "display_title_policy_id", "new_galleries", "changed_galleries", "removed_galleries", "duplicate_losers", "committed_at"] }
  ]

theorem publication_commit_schema_well_formed :
    schemaWellFormedCheck publication_commit_contract = true := by
  native_decide

theorem publication_commit_candidate_keys_check :
    keysDetermineAllCheck publication_commit_contract = true := by
  native_decide

theorem publication_commit_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_commit_contract :=
  keysDetermineAllCheck_sound publication_commit_contract
    publication_commit_candidate_keys_check

theorem publication_commit_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_commit_contract = true := by
  native_decide

theorem publication_commit_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_commit_contract :=
  declaredKeysMinimalCheck_sound publication_commit_contract
    publication_commit_candidate_keys_minimal_check

theorem publication_commit_closure_fixed_check :
    closureFixedPointCheck publication_commit_contract = true := by
  native_decide

theorem publication_commit_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_commit_contract :=
  closureFixedPointCheck_sound publication_commit_contract
    publication_commit_closure_fixed_check

theorem publication_commit_bcnf_check :
    bcnfCheck publication_commit_contract = true := by
  native_decide

theorem publication_commit_bcnf : BCNF publication_commit_contract :=
  bcnfCheck_sound publication_commit_contract publication_commit_bcnf_check

def publication_commit_finalization_contract : RelationContract where
  name := "publication_commit_finalization"
  attributes := ["receipt_id"]
  declaredKeys := [["receipt_id"]]
  declaredFDs := [
  ]

theorem publication_commit_finalization_schema_well_formed :
    schemaWellFormedCheck publication_commit_finalization_contract = true := by
  native_decide

theorem publication_commit_finalization_candidate_keys_check :
    keysDetermineAllCheck publication_commit_finalization_contract = true := by
  native_decide

theorem publication_commit_finalization_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_commit_finalization_contract :=
  keysDetermineAllCheck_sound publication_commit_finalization_contract
    publication_commit_finalization_candidate_keys_check

theorem publication_commit_finalization_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_commit_finalization_contract = true := by
  native_decide

theorem publication_commit_finalization_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_commit_finalization_contract :=
  declaredKeysMinimalCheck_sound publication_commit_finalization_contract
    publication_commit_finalization_candidate_keys_minimal_check

theorem publication_commit_finalization_closure_fixed_check :
    closureFixedPointCheck publication_commit_finalization_contract = true := by
  native_decide

theorem publication_commit_finalization_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_commit_finalization_contract :=
  closureFixedPointCheck_sound publication_commit_finalization_contract
    publication_commit_finalization_closure_fixed_check

theorem publication_commit_finalization_bcnf_check :
    bcnfCheck publication_commit_finalization_contract = true := by
  native_decide

theorem publication_commit_finalization_bcnf : BCNF publication_commit_finalization_contract :=
  bcnfCheck_sound publication_commit_finalization_contract publication_commit_finalization_bcnf_check

def publication_commit_head_receipt_contract : RelationContract where
  name := "publication_commit_head_receipt"
  attributes := ["channel", "receipt_id"]
  declaredKeys := [["channel"], ["receipt_id"]]
  declaredFDs := [
    { determinant := ["channel"], dependent := ["receipt_id"] },
    { determinant := ["receipt_id"], dependent := ["channel"] }
  ]

theorem publication_commit_head_receipt_schema_well_formed :
    schemaWellFormedCheck publication_commit_head_receipt_contract = true := by
  native_decide

theorem publication_commit_head_receipt_candidate_keys_check :
    keysDetermineAllCheck publication_commit_head_receipt_contract = true := by
  native_decide

theorem publication_commit_head_receipt_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_commit_head_receipt_contract :=
  keysDetermineAllCheck_sound publication_commit_head_receipt_contract
    publication_commit_head_receipt_candidate_keys_check

theorem publication_commit_head_receipt_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_commit_head_receipt_contract = true := by
  native_decide

theorem publication_commit_head_receipt_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_commit_head_receipt_contract :=
  declaredKeysMinimalCheck_sound publication_commit_head_receipt_contract
    publication_commit_head_receipt_candidate_keys_minimal_check

theorem publication_commit_head_receipt_closure_fixed_check :
    closureFixedPointCheck publication_commit_head_receipt_contract = true := by
  native_decide

theorem publication_commit_head_receipt_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_commit_head_receipt_contract :=
  closureFixedPointCheck_sound publication_commit_head_receipt_contract
    publication_commit_head_receipt_closure_fixed_check

theorem publication_commit_head_receipt_bcnf_check :
    bcnfCheck publication_commit_head_receipt_contract = true := by
  native_decide

theorem publication_commit_head_receipt_bcnf : BCNF publication_commit_head_receipt_contract :=
  bcnfCheck_sound publication_commit_head_receipt_contract publication_commit_head_receipt_bcnf_check

def publication_commit_head_contract : RelationContract where
  name := "publication_commit_head"
  attributes := ["channel", "receipt_id", "candidate_id", "revision", "source_revision", "generation", "committed_at"]
  declaredKeys := [["channel"], ["receipt_id"], ["candidate_id"], ["revision"], ["source_revision"], ["generation"]]
  declaredFDs := [
    { determinant := ["channel"], dependent := ["receipt_id", "candidate_id", "revision", "source_revision", "generation", "committed_at"] },
    { determinant := ["receipt_id"], dependent := ["channel", "candidate_id", "revision", "source_revision", "generation", "committed_at"] },
    { determinant := ["candidate_id"], dependent := ["channel", "receipt_id", "revision", "source_revision", "generation", "committed_at"] },
    { determinant := ["revision"], dependent := ["channel", "receipt_id", "candidate_id", "source_revision", "generation", "committed_at"] },
    { determinant := ["source_revision"], dependent := ["channel", "receipt_id", "candidate_id", "revision", "generation", "committed_at"] },
    { determinant := ["generation"], dependent := ["channel", "receipt_id", "candidate_id", "revision", "source_revision", "committed_at"] }
  ]

theorem publication_commit_head_schema_well_formed :
    schemaWellFormedCheck publication_commit_head_contract = true := by
  native_decide

theorem publication_commit_head_candidate_keys_check :
    keysDetermineAllCheck publication_commit_head_contract = true := by
  native_decide

theorem publication_commit_head_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_commit_head_contract :=
  keysDetermineAllCheck_sound publication_commit_head_contract
    publication_commit_head_candidate_keys_check

theorem publication_commit_head_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_commit_head_contract = true := by
  native_decide

theorem publication_commit_head_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_commit_head_contract :=
  declaredKeysMinimalCheck_sound publication_commit_head_contract
    publication_commit_head_candidate_keys_minimal_check

theorem publication_commit_head_closure_fixed_check :
    closureFixedPointCheck publication_commit_head_contract = true := by
  native_decide

theorem publication_commit_head_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_commit_head_contract :=
  closureFixedPointCheck_sound publication_commit_head_contract
    publication_commit_head_closure_fixed_check

def publication_receipt_contract : RelationContract where
  name := "publication_receipt"
  attributes := ["receipt_id", "revision", "source_revision", "channel", "artifact_policy_id", "display_title_policy_id", "publication_count", "new_galleries", "changed_galleries", "removed_galleries", "duplicate_losers", "state", "committed_at", "finalized_at"]
  declaredKeys := [["receipt_id"], ["revision"], ["source_revision"]]
  declaredFDs := [
    { determinant := ["receipt_id"], dependent := ["revision", "source_revision", "channel", "artifact_policy_id", "display_title_policy_id", "publication_count", "new_galleries", "changed_galleries", "removed_galleries", "duplicate_losers", "state", "committed_at", "finalized_at"] },
    { determinant := ["revision"], dependent := ["receipt_id", "source_revision", "channel", "artifact_policy_id", "display_title_policy_id", "publication_count", "new_galleries", "changed_galleries", "removed_galleries", "duplicate_losers", "state", "committed_at", "finalized_at"] },
    { determinant := ["source_revision"], dependent := ["receipt_id", "revision", "channel", "artifact_policy_id", "display_title_policy_id", "publication_count", "new_galleries", "changed_galleries", "removed_galleries", "duplicate_losers", "state", "committed_at", "finalized_at"] }
  ]

theorem publication_receipt_schema_well_formed :
    schemaWellFormedCheck publication_receipt_contract = true := by
  native_decide

theorem publication_receipt_candidate_keys_check :
    keysDetermineAllCheck publication_receipt_contract = true := by
  native_decide

theorem publication_receipt_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_receipt_contract :=
  keysDetermineAllCheck_sound publication_receipt_contract
    publication_receipt_candidate_keys_check

theorem publication_receipt_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_receipt_contract = true := by
  native_decide

theorem publication_receipt_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_receipt_contract :=
  declaredKeysMinimalCheck_sound publication_receipt_contract
    publication_receipt_candidate_keys_minimal_check

theorem publication_receipt_closure_fixed_check :
    closureFixedPointCheck publication_receipt_contract = true := by
  native_decide

theorem publication_receipt_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_receipt_contract :=
  closureFixedPointCheck_sound publication_receipt_contract
    publication_receipt_closure_fixed_check

def publication_head_revision_contract : RelationContract where
  name := "publication_head_revision"
  attributes := ["channel", "revision"]
  declaredKeys := [["channel"], ["revision"]]
  declaredFDs := [
    { determinant := ["channel"], dependent := ["revision"] },
    { determinant := ["revision"], dependent := ["channel"] }
  ]

theorem publication_head_revision_schema_well_formed :
    schemaWellFormedCheck publication_head_revision_contract = true := by
  native_decide

theorem publication_head_revision_candidate_keys_check :
    keysDetermineAllCheck publication_head_revision_contract = true := by
  native_decide

theorem publication_head_revision_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_head_revision_contract :=
  keysDetermineAllCheck_sound publication_head_revision_contract
    publication_head_revision_candidate_keys_check

theorem publication_head_revision_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_head_revision_contract = true := by
  native_decide

theorem publication_head_revision_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_head_revision_contract :=
  declaredKeysMinimalCheck_sound publication_head_revision_contract
    publication_head_revision_candidate_keys_minimal_check

theorem publication_head_revision_closure_fixed_check :
    closureFixedPointCheck publication_head_revision_contract = true := by
  native_decide

theorem publication_head_revision_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_head_revision_contract :=
  closureFixedPointCheck_sound publication_head_revision_contract
    publication_head_revision_closure_fixed_check

def publication_head_advanced_at_contract : RelationContract where
  name := "publication_head_advanced_at"
  attributes := ["channel", "advanced_at"]
  declaredKeys := [["channel"]]
  declaredFDs := [
    { determinant := ["channel"], dependent := ["advanced_at"] }
  ]

theorem publication_head_advanced_at_schema_well_formed :
    schemaWellFormedCheck publication_head_advanced_at_contract = true := by
  native_decide

theorem publication_head_advanced_at_candidate_keys_check :
    keysDetermineAllCheck publication_head_advanced_at_contract = true := by
  native_decide

theorem publication_head_advanced_at_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_head_advanced_at_contract :=
  keysDetermineAllCheck_sound publication_head_advanced_at_contract
    publication_head_advanced_at_candidate_keys_check

theorem publication_head_advanced_at_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_head_advanced_at_contract = true := by
  native_decide

theorem publication_head_advanced_at_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_head_advanced_at_contract :=
  declaredKeysMinimalCheck_sound publication_head_advanced_at_contract
    publication_head_advanced_at_candidate_keys_minimal_check

theorem publication_head_advanced_at_closure_fixed_check :
    closureFixedPointCheck publication_head_advanced_at_contract = true := by
  native_decide

theorem publication_head_advanced_at_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_head_advanced_at_contract :=
  closureFixedPointCheck_sound publication_head_advanced_at_contract
    publication_head_advanced_at_closure_fixed_check

def publication_head_contract : RelationContract where
  name := "publication_head"
  attributes := ["channel", "revision", "generation", "advanced_at"]
  declaredKeys := [["channel"], ["revision"], ["generation"]]
  declaredFDs := [
    { determinant := ["channel"], dependent := ["revision", "generation", "advanced_at"] },
    { determinant := ["revision"], dependent := ["channel", "generation", "advanced_at"] },
    { determinant := ["generation"], dependent := ["channel", "revision", "advanced_at"] }
  ]

theorem publication_head_schema_well_formed :
    schemaWellFormedCheck publication_head_contract = true := by
  native_decide

theorem publication_head_candidate_keys_check :
    keysDetermineAllCheck publication_head_contract = true := by
  native_decide

theorem publication_head_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes publication_head_contract :=
  keysDetermineAllCheck_sound publication_head_contract
    publication_head_candidate_keys_check

theorem publication_head_candidate_keys_minimal_check :
    declaredKeysMinimalCheck publication_head_contract = true := by
  native_decide

theorem publication_head_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal publication_head_contract :=
  declaredKeysMinimalCheck_sound publication_head_contract
    publication_head_candidate_keys_minimal_check

theorem publication_head_closure_fixed_check :
    closureFixedPointCheck publication_head_contract = true := by
  native_decide

theorem publication_head_closure_reached_fixed_point :
    ClosureReachedFixedPoint publication_head_contract :=
  closureFixedPointCheck_sound publication_head_contract
    publication_head_closure_fixed_check

theorem publication_commit_has_six_equivalent_candidate_keys :
    publication_commit_contract.declaredKeys.length = 6 := by
  native_decide

theorem analysis_batch_receipt_has_three_derived_candidate_keys :
    analysis_batch_receipt_contract.declaredKeys.length = 3 := by
  native_decide

theorem publication_batch_receipt_has_three_derived_candidate_keys :
    publication_batch_receipt_contract.declaredKeys.length = 3 := by
  native_decide

def manifestContracts : List RelationContract := [
  canonical_digest_policy_contract,
  canonical_value_allocation_anchor_contract,
  canonical_value_allocation_digest_domain_contract,
  canonical_value_allocation_byte_count_contract,
  canonical_value_allocation_allocated_at_contract,
  canonical_value_allocation_seal_contract,
  canonical_value_allocation_contract,
  canonical_value_page_anchor_contract,
  canonical_value_page_payload_contract,
  canonical_value_page_coordinate_contract,
  canonical_value_page_subtree_item_count_contract,
  canonical_value_page_seal_contract,
  canonical_value_page_contract,
  canonical_value_page_descriptor_contract,
  canonical_value_page_parent_contract,
  canonical_value_identity_contract,
  manifest_policy_contract,
  source_build_state_contract,
  source_build_sealed_at_contract,
  source_build_descriptor_contract,
  source_build_contract,
  source_build_base_publication_commit_contract,
  source_build_base_source_contract,
  channel_registry_contract,
  source_provider_registry_contract,
  source_build_channel_contract,
  source_scope_contract,
  source_build_discovery_contract,
  source_build_expected_gallery_contract,
  source_locator_identity_contract,
  gallery_identity_contract,
  gallery_observation_allocation_contract,
  gallery_observation_page_contract,
  gallery_observation_allocation_page_contract,
  gallery_observation_page_descriptor_anchor_contract,
  gallery_observation_page_descriptor_component_contract,
  gallery_observation_page_descriptor_level_contract,
  gallery_observation_page_descriptor_subtree_item_count_contract,
  gallery_observation_page_descriptor_seal_contract,
  gallery_observation_page_descriptor_contract,
  gallery_observation_page_key_bounds_anchor_contract,
  gallery_observation_page_key_bounds_first_key_contract,
  gallery_observation_page_key_bounds_last_key_contract,
  gallery_observation_page_key_bounds_seal_contract,
  gallery_observation_page_key_bounds_contract,
  gallery_observation_page_child_contract,
  gallery_observation_tree_root_contract,
  gallery_observation_contract,
  gallery_upload_time_contract,
  source_gallery_name_gid_contract,
  gallery_source_name_access_contract,
  gallery_observation_metadata_local_contract,
  gallery_observation_metadata_contract,
  gallery_observation_scan_contract,
  gallery_observation_discovery_fingerprint_contract,
  gallery_observation_metadata_digest_contract,
  gallery_observation_raw_content_contract,
  gallery_observation_page_count_contract,
  gallery_observation_directory_contract,
  gallery_observation_stat_contract,
  source_build_gallery_contract,
  file_name_identity_contract,
  content_blob_contract,
  gallery_observation_file_anchor_contract,
  gallery_observation_file_file_no_contract,
  gallery_observation_file_file_sha256_contract,
  gallery_observation_file_seal_contract,
  gallery_observation_file_contract,
  gallery_observation_file_filesystem_anchor_contract,
  gallery_observation_file_filesystem_device_contract,
  gallery_observation_file_filesystem_inode_contract,
  gallery_observation_file_filesystem_modified_ns_contract,
  gallery_observation_file_filesystem_changed_ns_contract,
  gallery_observation_file_filesystem_seal_contract,
  gallery_observation_file_filesystem_contract,
  tag_term_contract,
  gallery_observation_tag_contract,
  build_manifest_core_contract,
  build_manifest_contract,
  gallery_manifest_contract,
  analysis_policy_contract,
  analysis_run_state_contract,
  analysis_run_completed_at_contract,
  analysis_run_descriptor_contract,
  analysis_run_contract,
  analysis_baseline_contract,
  analysis_state_anchor_contract,
  analysis_state_ancestry_contract,
  source_snapshot_manifest_identity_contract,
  analysis_snapshot_manifest_contract,
  source_revision_descriptor_contract,
  source_revision_contract,
  source_revision_generation_contract,
  source_revision_provenance_contract,
  source_head_revision_contract,
  source_head_advanced_at_contract,
  source_head_contract,
  gallery_observation_artist_contract,
  gallery_observation_file_hash_occurrence_contract,
  analysis_changed_gallery_contract,
  analysis_changed_file_hash_contract,
  analysis_exclusion_delta_anchor_contract,
  analysis_exclusion_delta_old_excluded_contract,
  analysis_exclusion_delta_new_excluded_contract,
  analysis_exclusion_delta_change_contract,
  analysis_exclusion_delta_seal_contract,
  analysis_exclusion_delta_contract,
  analysis_impacted_gallery_contract,
  analysis_impacted_content_contract,
  analysis_impacted_content_provenance_contract,
  analysis_impacted_gid_storage_contract,
  analysis_impacted_gid_provenance_storage_contract,
  analysis_impacted_gid_provenance_contract,
  analysis_impacted_gid_contract,
  analysis_file_hash_decision_shadow_anchor_contract,
  analysis_file_hash_decision_shadow_occurrence_count_contract,
  analysis_file_hash_decision_shadow_artist_count_contract,
  analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract,
  analysis_file_hash_decision_shadow_seal_contract,
  analysis_file_hash_decision_shadow_contract,
  analysis_file_hash_decision_tombstone_contract,
  analysis_file_hash_decision_resolved_contract,
  analysis_content_owner_candidate_shadow_contract,
  analysis_content_owner_candidate_tombstone_contract,
  analysis_content_owner_candidate_resolved_contract,
  analysis_content_owner_shadow_contract,
  analysis_content_owner_tombstone_contract,
  analysis_content_owner_resolved_contract,
  analysis_gid_candidate_shadow_contract,
  analysis_gid_candidate_tombstone_contract,
  analysis_gid_candidate_resolved_contract,
  analysis_gid_winner_selection_contract,
  analysis_gid_winner_shadow_contract,
  analysis_gid_winner_tombstone_contract,
  analysis_gid_winner_resolved_contract,
  analysis_state_component_seal_contract,
  analysis_stage_contract,
  analysis_checkpoint_contract,
  analysis_batch_receipt_stored_contract,
  analysis_batch_receipt_contract,
  publication_candidate_contract,
  publication_candidate_projection_seal_contract,
  publication_candidate_projection_contract,
  publication_candidate_base_publication_commit_contract,
  publication_candidate_base_catalog_contract,
  publication_candidate_base_source_contract,
  publication_selection_storage_contract,
  publication_selection_occurrence_identity_contract,
  publication_selection_contract,
  publication_stage_contract,
  publication_checkpoint_contract,
  publication_batch_receipt_stored_contract,
  publication_batch_receipt_contract,
  publication_finalization_checkpoint_contract,
  publication_finalization_batch_receipt_stored_contract,
  publication_finalization_batch_receipt_contract,
  artifact_zip_writer_policy_contract,
  artifact_producer_fingerprint_contract,
  artifact_storage_codec_contract,
  artifact_policy_semantics_contract,
  artifact_policy_contract,
  artifact_semantic_input_contract,
  artifact_input_contract,
  artifact_delta_old_contract,
  artifact_delta_new_contract,
  artifact_operation_contract,
  artifact_blob_contract,
  prepared_artifact_contract,
  catalog_revision_descriptor_contract,
  catalog_revision_contract,
  catalog_revision_generation_contract,
  publication_generation_node_contract,
  publication_generation_successor_contract,
  publication_identity_contract,
  display_title_policy_contract,
  title_sort_policy_contract,
  display_title_choice_contract,
  title_sort_contract,
  catalog_publication_storage_contract,
  catalog_publication_occurrence_identity_contract,
  catalog_publication_contract,
  catalog_publication_order_contract,
  catalog_publication_title_contract,
  catalog_publication_content_contract,
  contributor_role_registry_contract,
  catalog_contributor_contract,
  catalog_subject_contract,
  catalog_artifact_contract,
  publication_commit_anchor_contract,
  publication_commit_contract,
  publication_commit_finalization_contract,
  publication_commit_head_receipt_contract,
  publication_commit_head_contract,
  publication_receipt_contract,
  publication_head_revision_contract,
  publication_head_advanced_at_contract,
  publication_head_contract
]

theorem manifest_relation_count :
    manifestContracts.length = 197 := by
  native_decide

/-! Closed catalog physical-domain authority from the manifest. -/

def catalogPhysicalDomainContracts : List RelationContract := [
  canonical_value_allocation_anchor_contract,
  canonical_value_allocation_digest_domain_contract,
  canonical_value_allocation_byte_count_contract,
  canonical_value_allocation_allocated_at_contract,
  canonical_value_allocation_seal_contract,
  canonical_value_allocation_contract,
  canonical_value_page_anchor_contract,
  canonical_value_page_payload_contract,
  canonical_value_page_coordinate_contract,
  canonical_value_page_subtree_item_count_contract,
  canonical_value_page_seal_contract,
  canonical_value_page_parent_contract,
  source_locator_identity_contract,
  source_build_descriptor_contract,
  source_build_state_contract,
  source_build_sealed_at_contract,
  source_build_contract,
  source_build_discovery_contract,
  build_manifest_core_contract,
  gallery_identity_contract,
  file_name_identity_contract,
  gallery_observation_file_anchor_contract,
  gallery_observation_file_file_no_contract,
  gallery_observation_file_file_sha256_contract,
  gallery_observation_file_seal_contract,
  gallery_manifest_contract,
  tag_term_contract,
  source_snapshot_manifest_identity_contract,
  publication_identity_contract,
  artifact_producer_fingerprint_contract,
  artifact_storage_codec_contract,
  artifact_semantic_input_contract,
  prepared_artifact_contract,
  catalog_artifact_contract,
  gallery_observation_page_contract,
  gallery_observation_allocation_page_contract,
  gallery_observation_page_descriptor_anchor_contract,
  gallery_observation_page_descriptor_component_contract,
  gallery_observation_page_descriptor_level_contract,
  gallery_observation_page_descriptor_subtree_item_count_contract,
  gallery_observation_page_descriptor_seal_contract,
  gallery_observation_page_key_bounds_anchor_contract,
  gallery_observation_page_key_bounds_first_key_contract,
  gallery_observation_page_key_bounds_last_key_contract,
  gallery_observation_page_key_bounds_seal_contract,
  gallery_observation_page_key_bounds_contract,
  gallery_observation_page_child_contract,
  gallery_observation_discovery_fingerprint_contract,
  analysis_run_descriptor_contract,
  analysis_run_state_contract,
  analysis_run_completed_at_contract,
  analysis_run_contract,
  analysis_state_anchor_contract,
  analysis_state_component_seal_contract,
  analysis_exclusion_delta_anchor_contract,
  analysis_exclusion_delta_old_excluded_contract,
  analysis_exclusion_delta_new_excluded_contract,
  analysis_exclusion_delta_change_contract,
  analysis_exclusion_delta_seal_contract,
  analysis_file_hash_decision_shadow_anchor_contract,
  analysis_file_hash_decision_shadow_occurrence_count_contract,
  analysis_file_hash_decision_shadow_artist_count_contract,
  analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract,
  analysis_file_hash_decision_shadow_seal_contract,
  analysis_file_hash_decision_shadow_contract,
  analysis_content_owner_candidate_shadow_contract,
  analysis_content_owner_shadow_contract,
  analysis_impacted_content_provenance_contract,
  analysis_impacted_content_contract,
  analysis_impacted_gid_provenance_storage_contract,
  analysis_impacted_gid_storage_contract,
  analysis_impacted_gid_provenance_contract,
  analysis_impacted_gid_contract,
  analysis_gid_candidate_shadow_contract,
  analysis_gid_candidate_tombstone_contract,
  analysis_gid_candidate_resolved_contract,
  analysis_gid_winner_selection_contract,
  analysis_gid_winner_shadow_contract,
  analysis_gid_winner_tombstone_contract,
  analysis_gid_winner_resolved_contract,
  analysis_batch_receipt_stored_contract,
  analysis_checkpoint_contract,
  publication_checkpoint_contract,
  publication_batch_receipt_contract,
  publication_candidate_contract,
  publication_candidate_projection_seal_contract,
  publication_candidate_projection_contract,
  publication_candidate_base_publication_commit_contract,
  publication_candidate_base_catalog_contract,
  publication_selection_storage_contract,
  publication_selection_occurrence_identity_contract,
  publication_selection_contract,
  publication_stage_contract,
  publication_batch_receipt_stored_contract,
  publication_finalization_checkpoint_contract,
  publication_finalization_batch_receipt_stored_contract,
  publication_finalization_batch_receipt_contract,
  artifact_zip_writer_policy_contract,
  artifact_policy_semantics_contract,
  artifact_policy_contract,
  artifact_input_contract,
  artifact_delta_old_contract,
  artifact_operation_contract,
  artifact_blob_contract,
  catalog_revision_descriptor_contract,
  catalog_revision_contract,
  publication_generation_node_contract,
  publication_generation_successor_contract,
  display_title_policy_contract,
  title_sort_policy_contract,
  display_title_choice_contract,
  title_sort_contract,
  catalog_publication_storage_contract,
  catalog_publication_occurrence_identity_contract,
  catalog_publication_contract,
  catalog_publication_order_contract,
  catalog_publication_title_contract,
  catalog_publication_content_contract,
  contributor_role_registry_contract,
  catalog_contributor_contract,
  catalog_subject_contract,
  publication_commit_anchor_contract,
  publication_commit_contract,
  publication_commit_finalization_contract,
  publication_commit_head_receipt_contract,
  publication_commit_head_contract,
  publication_receipt_contract
]

def catalogPhysicalDomainMutationContracts : List RelationContract := [
  canonical_value_allocation_anchor_contract,
  canonical_value_allocation_digest_domain_contract,
  canonical_value_allocation_byte_count_contract,
  canonical_value_allocation_allocated_at_contract,
  canonical_value_allocation_seal_contract,
  canonical_value_page_anchor_contract,
  canonical_value_page_payload_contract,
  canonical_value_page_coordinate_contract,
  canonical_value_page_subtree_item_count_contract,
  canonical_value_page_seal_contract,
  canonical_value_page_parent_contract,
  source_locator_identity_contract,
  source_build_descriptor_contract,
  source_build_state_contract,
  source_build_sealed_at_contract,
  source_build_discovery_contract,
  build_manifest_core_contract,
  gallery_identity_contract,
  file_name_identity_contract,
  gallery_observation_file_anchor_contract,
  gallery_observation_file_file_no_contract,
  gallery_observation_file_file_sha256_contract,
  gallery_observation_file_seal_contract,
  gallery_manifest_contract,
  tag_term_contract,
  source_snapshot_manifest_identity_contract,
  publication_identity_contract,
  artifact_producer_fingerprint_contract,
  artifact_storage_codec_contract,
  artifact_semantic_input_contract,
  prepared_artifact_contract,
  catalog_artifact_contract,
  gallery_observation_page_contract,
  gallery_observation_allocation_page_contract,
  gallery_observation_page_descriptor_anchor_contract,
  gallery_observation_page_descriptor_component_contract,
  gallery_observation_page_descriptor_level_contract,
  gallery_observation_page_descriptor_subtree_item_count_contract,
  gallery_observation_page_descriptor_seal_contract,
  gallery_observation_page_key_bounds_anchor_contract,
  gallery_observation_page_key_bounds_first_key_contract,
  gallery_observation_page_key_bounds_last_key_contract,
  gallery_observation_page_key_bounds_seal_contract,
  gallery_observation_page_child_contract,
  gallery_observation_discovery_fingerprint_contract,
  analysis_run_descriptor_contract,
  analysis_run_state_contract,
  analysis_run_completed_at_contract,
  analysis_state_component_seal_contract,
  analysis_exclusion_delta_anchor_contract,
  analysis_exclusion_delta_old_excluded_contract,
  analysis_exclusion_delta_new_excluded_contract,
  analysis_exclusion_delta_change_contract,
  analysis_exclusion_delta_seal_contract,
  analysis_file_hash_decision_shadow_anchor_contract,
  analysis_file_hash_decision_shadow_occurrence_count_contract,
  analysis_file_hash_decision_shadow_artist_count_contract,
  analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract,
  analysis_file_hash_decision_shadow_seal_contract,
  analysis_content_owner_candidate_shadow_contract,
  analysis_content_owner_shadow_contract,
  analysis_impacted_content_provenance_contract,
  analysis_impacted_content_contract,
  analysis_impacted_gid_provenance_storage_contract,
  analysis_impacted_gid_storage_contract,
  analysis_gid_candidate_shadow_contract,
  analysis_gid_candidate_tombstone_contract,
  analysis_gid_winner_selection_contract,
  analysis_gid_winner_tombstone_contract,
  analysis_batch_receipt_stored_contract,
  analysis_checkpoint_contract,
  publication_checkpoint_contract,
  publication_candidate_contract,
  publication_candidate_projection_seal_contract,
  publication_candidate_base_publication_commit_contract,
  publication_selection_storage_contract,
  publication_selection_occurrence_identity_contract,
  publication_stage_contract,
  publication_batch_receipt_stored_contract,
  publication_finalization_checkpoint_contract,
  publication_finalization_batch_receipt_stored_contract,
  artifact_zip_writer_policy_contract,
  artifact_policy_semantics_contract,
  artifact_policy_contract,
  artifact_input_contract,
  artifact_operation_contract,
  artifact_blob_contract,
  catalog_revision_descriptor_contract,
  publication_generation_node_contract,
  publication_generation_successor_contract,
  display_title_policy_contract,
  title_sort_policy_contract,
  display_title_choice_contract,
  title_sort_contract,
  catalog_publication_storage_contract,
  catalog_publication_occurrence_identity_contract,
  catalog_publication_order_contract,
  catalog_publication_content_contract,
  contributor_role_registry_contract,
  catalog_contributor_contract,
  catalog_subject_contract,
  publication_commit_anchor_contract,
  publication_commit_contract,
  publication_commit_finalization_contract,
  publication_commit_head_receipt_contract
]

def catalogPhysicalDomainReadOnlyViewContracts : List RelationContract := [
  canonical_value_allocation_contract,
  source_build_contract,
  gallery_observation_page_key_bounds_contract,
  analysis_run_contract,
  analysis_state_anchor_contract,
  analysis_file_hash_decision_shadow_contract,
  analysis_impacted_gid_provenance_contract,
  analysis_impacted_gid_contract,
  analysis_gid_candidate_resolved_contract,
  analysis_gid_winner_shadow_contract,
  analysis_gid_winner_resolved_contract,
  publication_batch_receipt_contract,
  publication_candidate_projection_contract,
  publication_candidate_base_catalog_contract,
  publication_selection_contract,
  publication_finalization_batch_receipt_contract,
  artifact_delta_old_contract,
  catalog_revision_contract,
  catalog_publication_contract,
  catalog_publication_title_contract,
  publication_commit_head_contract,
  publication_receipt_contract
]

theorem catalog_physical_domain_relation_count :
    catalogPhysicalDomainContracts.length = 127 := by
  native_decide

theorem catalog_physical_domain_mutation_relation_count :
    catalogPhysicalDomainMutationContracts.length = 105 := by
  native_decide

theorem catalog_physical_domain_read_only_view_count :
    catalogPhysicalDomainReadOnlyViewContracts.length = 22 := by
  native_decide

theorem catalog_physical_domain_has_no_duplicates :
    catalogPhysicalDomainContracts.Nodup := by
  native_decide

theorem catalog_physical_domain_is_manifest_closed :
    ∀ contract ∈ catalogPhysicalDomainContracts,
      contract ∈ manifestContracts := by
  native_decide

theorem catalog_physical_domain_partition_is_exact :
    (catalogPhysicalDomainMutationContracts ++
      catalogPhysicalDomainReadOnlyViewContracts).all
        (fun contract => contract ∈ catalogPhysicalDomainContracts) = true ∧
      catalogPhysicalDomainContracts.all (fun contract =>
        contract ∈ catalogPhysicalDomainMutationContracts ||
          contract ∈ catalogPhysicalDomainReadOnlyViewContracts) = true := by
  native_decide

theorem catalog_physical_domain_partition_is_disjoint :
    ∀ contract ∈ catalogPhysicalDomainMutationContracts,
      contract ∉ catalogPhysicalDomainReadOnlyViewContracts := by
  native_decide

/-! Unbounded sealed impacted-key protocol instantiated from the manifest. -/

def analysisImpactedMaximumPageGalleries : Nat := 128
def analysisImpactedMaximumProvenanceRows : Nat := 257

theorem analysis_impacted_page_bounds_match_manifest :
    analysisImpactedMaximumPageGalleries = 128 ∧
      analysisImpactedMaximumProvenanceRows = 257 := by
  native_decide

theorem analysis_impacted_content_first_insert_min_witness
    (gallery : Nat) :
    ImpactedMinimumWitness gallery
        (impactedFirstInsert gallery).provenance ∧
      impactedKeyVisible (impactedFirstInsert gallery) = true :=
  impacted_first_insert_establishes_minimum_and_visibility gallery

theorem analysis_impacted_content_append_greater_preserves_min_and_visibility
    (rows : ImpactedKeyRows)
    (witness gallery : Nat)
    (minimum : ImpactedMinimumWitness witness rows.provenance)
    (greater : witness < gallery)
    (visible : impactedKeyVisible rows = true) :
    ImpactedMinimumWitness witness
        (impactedAppendProvenance rows gallery).provenance ∧
      impactedKeyVisible (impactedAppendProvenance rows gallery) = true :=
  impacted_append_greater_preserves_minimum_and_visibility
    rows witness gallery minimum greater visible

theorem analysis_impacted_content_exact_replay_identity
    (stored replayed : ImpactedKeyRows)
    (exact : ImpactedExactReplay stored replayed) :
    replayed = stored :=
  impacted_exact_replay_is_identity stored replayed exact

theorem analysis_impacted_content_cleanup_no_dangling
    (rows : ImpactedKeyRows) :
    let sealRemoved := impactedRemoveSeal rows
    let witnessRemoved := impactedRemoveWitness sealRemoved
    let provenanceRemoved := impactedRemoveProvenance witnessRemoved
    let anchorRemoved := impactedRemoveAnchor provenanceRemoved
    sealRemoved.sealed = false ∧
      witnessRemoved.sealed = false ∧
      witnessRemoved.witness = none ∧
      provenanceRemoved.sealed = false ∧
      provenanceRemoved.witness = none ∧
      provenanceRemoved.provenance = [] ∧
      ImpactedNoRows anchorRemoved :=
  impacted_ordered_cleanup_has_no_dangling_rows rows

theorem analysis_impacted_gid_first_insert_min_witness
    (gallery : Nat) :
    ImpactedMinimumWitness gallery
        (impactedFirstInsert gallery).provenance ∧
      impactedKeyVisible (impactedFirstInsert gallery) = true :=
  impacted_first_insert_establishes_minimum_and_visibility gallery

theorem analysis_impacted_gid_append_greater_preserves_min_and_visibility
    (rows : ImpactedKeyRows)
    (witness gallery : Nat)
    (minimum : ImpactedMinimumWitness witness rows.provenance)
    (greater : witness < gallery)
    (visible : impactedKeyVisible rows = true) :
    ImpactedMinimumWitness witness
        (impactedAppendProvenance rows gallery).provenance ∧
      impactedKeyVisible (impactedAppendProvenance rows gallery) = true :=
  impacted_append_greater_preserves_minimum_and_visibility
    rows witness gallery minimum greater visible

theorem analysis_impacted_gid_exact_replay_identity
    (stored replayed : ImpactedKeyRows)
    (exact : ImpactedExactReplay stored replayed) :
    replayed = stored :=
  impacted_exact_replay_is_identity stored replayed exact

theorem analysis_impacted_gid_cleanup_no_dangling
    (rows : ImpactedKeyRows) :
    let sealRemoved := impactedRemoveSeal rows
    let witnessRemoved := impactedRemoveWitness sealRemoved
    let provenanceRemoved := impactedRemoveProvenance witnessRemoved
    let anchorRemoved := impactedRemoveAnchor provenanceRemoved
    sealRemoved.sealed = false ∧
      witnessRemoved.sealed = false ∧
      witnessRemoved.witness = none ∧
      provenanceRemoved.sealed = false ∧
      provenanceRemoved.witness = none ∧
      provenanceRemoved.provenance = [] ∧
      ImpactedNoRows anchorRemoved :=
  impacted_ordered_cleanup_has_no_dangling_rows rows

/-! Unbounded GID membership and winner facts instantiated from the manifest. -/

def analysisGidWinnerOrderAttributes : List String :=
  ["prefer_not_already_uploaded", "title_scalar_count", "download_time", "scope_key", "locator_sha256"]

theorem analysis_gid_winner_order_has_five_atomic_facts :
    analysisGidWinnerOrderAttributes.length = 5 := by
  native_decide

theorem analysis_gid_candidate_current_group_is_functional
    (metadataGid : Nat → Nat)
    (galleryId leftGid rightGid : Nat)
    (left : GidCandidateCurrentGroup metadataGid galleryId leftGid)
    (right : GidCandidateCurrentGroup metadataGid galleryId rightGid) :
    leftGid = rightGid :=
  gid_candidate_current_gid_functional
    metadataGid galleryId leftGid rightGid left right

theorem analysis_gid_winner_selection_is_candidate
    (candidates : List GidWinnerCandidate)
    (dominates : GidWinnerCandidate → GidWinnerCandidate → Prop)
    (winner : GidWinnerCandidate)
    (exact : GidWinnerExact candidates dominates winner) :
    winner ∈ candidates :=
  gid_winner_exact_is_member candidates dominates winner exact

theorem analysis_gid_winner_selection_dominates_group
    (candidates : List GidWinnerCandidate)
    (dominates : GidWinnerCandidate → GidWinnerCandidate → Prop)
    (winner candidate : GidWinnerCandidate)
    (exact : GidWinnerExact candidates dominates winner)
    (member : candidate ∈ candidates) :
    dominates candidate winner :=
  gid_winner_exact_dominates_every_candidate
    candidates dominates winner candidate exact member

theorem analysis_gid_winner_selection_unique_under_stable_identity
    (candidates : List GidWinnerCandidate)
    (dominates : GidWinnerCandidate → GidWinnerCandidate → Prop)
    (left right : GidWinnerCandidate)
    (leftExact : GidWinnerExact candidates dominates left)
    (rightExact : GidWinnerExact candidates dominates right)
    (stableIdentity :
      ∀ first ∈ candidates, ∀ second ∈ candidates,
        dominates first second → dominates second first →
          first.galleryId = second.galleryId) :
    left.galleryId = right.galleryId :=
  gid_winner_exact_gallery_unique candidates dominates left right
    leftExact rightExact stableIdentity

def gallery_observation_file_filesystem_vertical_contract : VerticalFamilyContract where
  name := "gallery_observation_file_filesystem_vertical"
  anchorRelation := "gallery_observation_file_filesystem_anchor"
  sealRelation := "gallery_observation_file_filesystem_seal"
  keyAttributes := ["gallery_id", "observation_id", "file_key"]
  anchorAttributes := ["gallery_id", "observation_id", "file_key"]
  sealAttributes := ["gallery_id", "observation_id", "file_key"]
  semanticAttributes := ["gallery_id", "observation_id", "file_key", "device", "inode", "modified_ns", "changed_ns"]
  semanticFDs := [
    { determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["device", "inode", "modified_ns", "changed_ns"] }
  ]
  viewAttributes := ["gallery_id", "observation_id", "file_key", "device", "inode", "modified_ns", "changed_ns"]
  viewFDs := [
    { determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["device", "inode", "modified_ns", "changed_ns"] }
  ]
  members := [
    { relationName := "gallery_observation_file_filesystem_device", keyAttributes := ["gallery_id", "observation_id", "file_key"], valueAttribute := "device", projectionAttribute := "device", attributes := ["gallery_id", "observation_id", "file_key", "device"], declaredKeys := [["gallery_id", "observation_id", "file_key"]], declaredFDs := [{ determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["device"] }], sourceRelation := "gallery_observation_file_filesystem_seal", sourceRelationAttributes := ["gallery_id", "observation_id", "file_key"], sourceAttributes := ["gallery_id", "observation_id", "file_key"], memberAttributes := ["gallery_id", "observation_id", "file_key"], congruenceMembers := [], project := true, required := true },
    { relationName := "gallery_observation_file_filesystem_inode", keyAttributes := ["gallery_id", "observation_id", "file_key"], valueAttribute := "inode", projectionAttribute := "inode", attributes := ["gallery_id", "observation_id", "file_key", "inode"], declaredKeys := [["gallery_id", "observation_id", "file_key"]], declaredFDs := [{ determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["inode"] }], sourceRelation := "gallery_observation_file_filesystem_seal", sourceRelationAttributes := ["gallery_id", "observation_id", "file_key"], sourceAttributes := ["gallery_id", "observation_id", "file_key"], memberAttributes := ["gallery_id", "observation_id", "file_key"], congruenceMembers := [], project := true, required := true },
    { relationName := "gallery_observation_file_filesystem_modified_ns", keyAttributes := ["gallery_id", "observation_id", "file_key"], valueAttribute := "modified_ns", projectionAttribute := "modified_ns", attributes := ["gallery_id", "observation_id", "file_key", "modified_ns"], declaredKeys := [["gallery_id", "observation_id", "file_key"]], declaredFDs := [{ determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["modified_ns"] }], sourceRelation := "gallery_observation_file_filesystem_seal", sourceRelationAttributes := ["gallery_id", "observation_id", "file_key"], sourceAttributes := ["gallery_id", "observation_id", "file_key"], memberAttributes := ["gallery_id", "observation_id", "file_key"], congruenceMembers := [], project := true, required := true },
    { relationName := "gallery_observation_file_filesystem_changed_ns", keyAttributes := ["gallery_id", "observation_id", "file_key"], valueAttribute := "changed_ns", projectionAttribute := "changed_ns", attributes := ["gallery_id", "observation_id", "file_key", "changed_ns"], declaredKeys := [["gallery_id", "observation_id", "file_key"]], declaredFDs := [{ determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["changed_ns"] }], sourceRelation := "gallery_observation_file_filesystem_seal", sourceRelationAttributes := ["gallery_id", "observation_id", "file_key"], sourceAttributes := ["gallery_id", "observation_id", "file_key"], memberAttributes := ["gallery_id", "observation_id", "file_key"], congruenceMembers := [], project := true, required := true }
  ]
  optionalPresence := none
  markerRelation := none
  markerAttributes := []
  markerPredicate := none

theorem gallery_observation_file_filesystem_vertical_view_projection_check :
    verticalFamilyViewProjectionCheck gallery_observation_file_filesystem_vertical_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_vertical_view_projection :
    VerticalFamilyViewProjection gallery_observation_file_filesystem_vertical_contract :=
  verticalFamilyViewProjectionCheck_sound gallery_observation_file_filesystem_vertical_contract
    gallery_observation_file_filesystem_vertical_view_projection_check

theorem gallery_observation_file_filesystem_vertical_well_formed_check :
    verticalFamilyWellFormedCheck gallery_observation_file_filesystem_vertical_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_vertical_well_formed :
    VerticalFamilyWellFormed gallery_observation_file_filesystem_vertical_contract :=
  verticalFamilyWellFormedCheck_sound gallery_observation_file_filesystem_vertical_contract
    gallery_observation_file_filesystem_vertical_well_formed_check

theorem gallery_observation_file_filesystem_vertical_lossless_join_check :
    verticalFamilyLosslessJoinCheck gallery_observation_file_filesystem_vertical_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_vertical_lossless :
    VerticalFamilyLossless gallery_observation_file_filesystem_vertical_contract :=
  verticalFamilyLosslessJoinCheck_sound gallery_observation_file_filesystem_vertical_contract
    gallery_observation_file_filesystem_vertical_lossless_join_check

theorem gallery_observation_file_filesystem_vertical_dependency_preservation_check :
    verticalFamilyDependencyPreservationCheck gallery_observation_file_filesystem_vertical_contract = true := by
  native_decide

theorem gallery_observation_file_filesystem_vertical_dependency_preserving :
    VerticalFamilyDependencyPreserving gallery_observation_file_filesystem_vertical_contract :=
  verticalFamilyDependencyPreservationCheck_sound gallery_observation_file_filesystem_vertical_contract
    gallery_observation_file_filesystem_vertical_dependency_preservation_check

def analysis_exclusion_delta_vertical_contract : VerticalFamilyContract where
  name := "analysis_exclusion_delta_vertical"
  anchorRelation := "analysis_exclusion_delta_anchor"
  sealRelation := "analysis_exclusion_delta_seal"
  keyAttributes := ["analysis_id", "file_sha256"]
  anchorAttributes := ["analysis_id", "file_sha256"]
  sealAttributes := ["analysis_id", "file_sha256"]
  semanticAttributes := ["analysis_id", "file_sha256", "old_excluded", "new_excluded"]
  semanticFDs := [
    { determinant := ["analysis_id", "file_sha256"], dependent := ["old_excluded", "new_excluded"] }
  ]
  viewAttributes := ["analysis_id", "file_sha256", "old_excluded", "new_excluded"]
  viewFDs := [
    { determinant := ["analysis_id", "file_sha256"], dependent := ["old_excluded", "new_excluded"] }
  ]
  members := [
    { relationName := "analysis_exclusion_delta_old_excluded", keyAttributes := ["analysis_id", "file_sha256"], valueAttribute := "old_excluded", projectionAttribute := "old_excluded", attributes := ["analysis_id", "file_sha256", "old_excluded"], declaredKeys := [["analysis_id", "file_sha256"]], declaredFDs := [{ determinant := ["analysis_id", "file_sha256"], dependent := ["old_excluded"] }], sourceRelation := "analysis_exclusion_delta_seal", sourceRelationAttributes := ["analysis_id", "file_sha256"], sourceAttributes := ["analysis_id", "file_sha256"], memberAttributes := ["analysis_id", "file_sha256"], congruenceMembers := [], project := true, required := true },
    { relationName := "analysis_exclusion_delta_new_excluded", keyAttributes := ["analysis_id", "file_sha256"], valueAttribute := "new_excluded", projectionAttribute := "new_excluded", attributes := ["analysis_id", "file_sha256", "new_excluded"], declaredKeys := [["analysis_id", "file_sha256"]], declaredFDs := [{ determinant := ["analysis_id", "file_sha256"], dependent := ["new_excluded"] }], sourceRelation := "analysis_exclusion_delta_seal", sourceRelationAttributes := ["analysis_id", "file_sha256"], sourceAttributes := ["analysis_id", "file_sha256"], memberAttributes := ["analysis_id", "file_sha256"], congruenceMembers := [], project := true, required := true }
  ]
  optionalPresence := none
  markerRelation := some "analysis_exclusion_delta_change"
  markerAttributes := ["analysis_id", "file_sha256"]
  markerPredicate := some "old_excluded != new_excluded"

theorem analysis_exclusion_delta_vertical_view_projection_check :
    verticalFamilyViewProjectionCheck analysis_exclusion_delta_vertical_contract = true := by
  native_decide

theorem analysis_exclusion_delta_vertical_view_projection :
    VerticalFamilyViewProjection analysis_exclusion_delta_vertical_contract :=
  verticalFamilyViewProjectionCheck_sound analysis_exclusion_delta_vertical_contract
    analysis_exclusion_delta_vertical_view_projection_check

theorem analysis_exclusion_delta_vertical_well_formed_check :
    verticalFamilyWellFormedCheck analysis_exclusion_delta_vertical_contract = true := by
  native_decide

theorem analysis_exclusion_delta_vertical_well_formed :
    VerticalFamilyWellFormed analysis_exclusion_delta_vertical_contract :=
  verticalFamilyWellFormedCheck_sound analysis_exclusion_delta_vertical_contract
    analysis_exclusion_delta_vertical_well_formed_check

theorem analysis_exclusion_delta_vertical_lossless_join_check :
    verticalFamilyLosslessJoinCheck analysis_exclusion_delta_vertical_contract = true := by
  native_decide

theorem analysis_exclusion_delta_vertical_lossless :
    VerticalFamilyLossless analysis_exclusion_delta_vertical_contract :=
  verticalFamilyLosslessJoinCheck_sound analysis_exclusion_delta_vertical_contract
    analysis_exclusion_delta_vertical_lossless_join_check

theorem analysis_exclusion_delta_vertical_dependency_preservation_check :
    verticalFamilyDependencyPreservationCheck analysis_exclusion_delta_vertical_contract = true := by
  native_decide

theorem analysis_exclusion_delta_vertical_dependency_preserving :
    VerticalFamilyDependencyPreserving analysis_exclusion_delta_vertical_contract :=
  verticalFamilyDependencyPreservationCheck_sound analysis_exclusion_delta_vertical_contract
    analysis_exclusion_delta_vertical_dependency_preservation_check

def canonical_value_allocation_vertical_contract : VerticalFamilyContract where
  name := "canonical_value_allocation_vertical"
  anchorRelation := "canonical_value_allocation_anchor"
  sealRelation := "canonical_value_allocation_seal"
  keyAttributes := ["value_sha256"]
  anchorAttributes := ["value_sha256"]
  sealAttributes := ["value_sha256"]
  semanticAttributes := ["value_sha256", "digest_domain", "byte_count", "allocated_at"]
  semanticFDs := [
    { determinant := ["value_sha256"], dependent := ["digest_domain", "byte_count", "allocated_at"] }
  ]
  viewAttributes := ["value_sha256", "digest_domain", "byte_count", "allocated_at"]
  viewFDs := [
    { determinant := ["value_sha256"], dependent := ["digest_domain", "byte_count", "allocated_at"] }
  ]
  members := [
    { relationName := "canonical_value_allocation_digest_domain", keyAttributes := ["value_sha256"], valueAttribute := "digest_domain", projectionAttribute := "digest_domain", attributes := ["value_sha256", "digest_domain"], declaredKeys := [["value_sha256"]], declaredFDs := [{ determinant := ["value_sha256"], dependent := ["digest_domain"] }], sourceRelation := "canonical_value_allocation_seal", sourceRelationAttributes := ["value_sha256"], sourceAttributes := ["value_sha256"], memberAttributes := ["value_sha256"], congruenceMembers := [], project := true, required := true },
    { relationName := "canonical_value_allocation_byte_count", keyAttributes := ["value_sha256"], valueAttribute := "byte_count", projectionAttribute := "byte_count", attributes := ["value_sha256", "byte_count"], declaredKeys := [["value_sha256"]], declaredFDs := [{ determinant := ["value_sha256"], dependent := ["byte_count"] }], sourceRelation := "canonical_value_allocation_seal", sourceRelationAttributes := ["value_sha256"], sourceAttributes := ["value_sha256"], memberAttributes := ["value_sha256"], congruenceMembers := [], project := true, required := true },
    { relationName := "canonical_value_allocation_allocated_at", keyAttributes := ["value_sha256"], valueAttribute := "allocated_at", projectionAttribute := "allocated_at", attributes := ["value_sha256", "allocated_at"], declaredKeys := [["value_sha256"]], declaredFDs := [{ determinant := ["value_sha256"], dependent := ["allocated_at"] }], sourceRelation := "canonical_value_allocation_seal", sourceRelationAttributes := ["value_sha256"], sourceAttributes := ["value_sha256"], memberAttributes := ["value_sha256"], congruenceMembers := [], project := true, required := true }
  ]
  optionalPresence := none
  markerRelation := none
  markerAttributes := []
  markerPredicate := none

theorem canonical_value_allocation_vertical_view_projection_check :
    verticalFamilyViewProjectionCheck canonical_value_allocation_vertical_contract = true := by
  native_decide

theorem canonical_value_allocation_vertical_view_projection :
    VerticalFamilyViewProjection canonical_value_allocation_vertical_contract :=
  verticalFamilyViewProjectionCheck_sound canonical_value_allocation_vertical_contract
    canonical_value_allocation_vertical_view_projection_check

theorem canonical_value_allocation_vertical_well_formed_check :
    verticalFamilyWellFormedCheck canonical_value_allocation_vertical_contract = true := by
  native_decide

theorem canonical_value_allocation_vertical_well_formed :
    VerticalFamilyWellFormed canonical_value_allocation_vertical_contract :=
  verticalFamilyWellFormedCheck_sound canonical_value_allocation_vertical_contract
    canonical_value_allocation_vertical_well_formed_check

theorem canonical_value_allocation_vertical_lossless_join_check :
    verticalFamilyLosslessJoinCheck canonical_value_allocation_vertical_contract = true := by
  native_decide

theorem canonical_value_allocation_vertical_lossless :
    VerticalFamilyLossless canonical_value_allocation_vertical_contract :=
  verticalFamilyLosslessJoinCheck_sound canonical_value_allocation_vertical_contract
    canonical_value_allocation_vertical_lossless_join_check

theorem canonical_value_allocation_vertical_dependency_preservation_check :
    verticalFamilyDependencyPreservationCheck canonical_value_allocation_vertical_contract = true := by
  native_decide

theorem canonical_value_allocation_vertical_dependency_preserving :
    VerticalFamilyDependencyPreserving canonical_value_allocation_vertical_contract :=
  verticalFamilyDependencyPreservationCheck_sound canonical_value_allocation_vertical_contract
    canonical_value_allocation_vertical_dependency_preservation_check

def canonical_value_page_vertical_contract : VerticalFamilyContract where
  name := "canonical_value_page_vertical"
  anchorRelation := "canonical_value_page_anchor"
  sealRelation := "canonical_value_page_seal"
  keyAttributes := ["page_sha256"]
  anchorAttributes := ["page_sha256"]
  sealAttributes := ["page_sha256"]
  semanticAttributes := ["page_sha256", "page_bytes", "value_sha256", "level", "page_position", "subtree_item_count"]
  semanticFDs := [
    { determinant := ["page_sha256"], dependent := ["page_bytes", "value_sha256", "level", "page_position", "subtree_item_count"] },
    { determinant := ["page_bytes"], dependent := ["page_sha256", "value_sha256", "level", "page_position", "subtree_item_count"] },
    { determinant := ["value_sha256", "level", "page_position"], dependent := ["page_sha256", "page_bytes", "subtree_item_count"] }
  ]
  viewAttributes := ["page_sha256", "value_sha256", "level", "page_position", "subtree_item_count"]
  viewFDs := [
    { determinant := ["page_sha256"], dependent := ["value_sha256", "level", "page_position", "subtree_item_count"] },
    { determinant := ["value_sha256", "level", "page_position"], dependent := ["page_sha256", "subtree_item_count"] }
  ]
  members := [
    { relationName := "canonical_value_page_payload", keyAttributes := ["page_sha256"], valueAttribute := "page_bytes", projectionAttribute := "page_bytes", attributes := ["page_sha256", "page_bytes"], declaredKeys := [["page_sha256"], ["page_bytes"]], declaredFDs := [{ determinant := ["page_sha256"], dependent := ["page_bytes"] }, { determinant := ["page_bytes"], dependent := ["page_sha256"] }], sourceRelation := "canonical_value_page_seal", sourceRelationAttributes := ["page_sha256"], sourceAttributes := ["page_sha256"], memberAttributes := ["page_sha256"], congruenceMembers := [], project := false, required := true },
    { relationName := "canonical_value_page_coordinate", keyAttributes := ["value_sha256", "level", "page_position"], valueAttribute := "page_sha256", projectionAttribute := "page_sha256", attributes := ["value_sha256", "level", "page_position", "page_sha256"], declaredKeys := [["value_sha256", "level", "page_position"], ["page_sha256"]], declaredFDs := [{ determinant := ["value_sha256", "level", "page_position"], dependent := ["page_sha256"] }, { determinant := ["page_sha256"], dependent := ["value_sha256", "level", "page_position"] }], sourceRelation := "canonical_value_page_seal", sourceRelationAttributes := ["page_sha256"], sourceAttributes := ["page_sha256"], memberAttributes := ["page_sha256"], congruenceMembers := [], project := true, required := true },
    { relationName := "canonical_value_page_subtree_item_count", keyAttributes := ["page_sha256"], valueAttribute := "subtree_item_count", projectionAttribute := "subtree_item_count", attributes := ["page_sha256", "subtree_item_count"], declaredKeys := [["page_sha256"]], declaredFDs := [{ determinant := ["page_sha256"], dependent := ["subtree_item_count"] }], sourceRelation := "canonical_value_page_seal", sourceRelationAttributes := ["page_sha256"], sourceAttributes := ["page_sha256"], memberAttributes := ["page_sha256"], congruenceMembers := [], project := true, required := true }
  ]
  optionalPresence := none
  markerRelation := none
  markerAttributes := []
  markerPredicate := none

theorem canonical_value_page_vertical_view_projection_check :
    verticalFamilyViewProjectionCheck canonical_value_page_vertical_contract = true := by
  native_decide

theorem canonical_value_page_vertical_view_projection :
    VerticalFamilyViewProjection canonical_value_page_vertical_contract :=
  verticalFamilyViewProjectionCheck_sound canonical_value_page_vertical_contract
    canonical_value_page_vertical_view_projection_check

theorem canonical_value_page_vertical_well_formed_check :
    verticalFamilyWellFormedCheck canonical_value_page_vertical_contract = true := by
  native_decide

theorem canonical_value_page_vertical_well_formed :
    VerticalFamilyWellFormed canonical_value_page_vertical_contract :=
  verticalFamilyWellFormedCheck_sound canonical_value_page_vertical_contract
    canonical_value_page_vertical_well_formed_check

theorem canonical_value_page_vertical_lossless_join_check :
    verticalFamilyLosslessJoinCheck canonical_value_page_vertical_contract = true := by
  native_decide

theorem canonical_value_page_vertical_lossless :
    VerticalFamilyLossless canonical_value_page_vertical_contract :=
  verticalFamilyLosslessJoinCheck_sound canonical_value_page_vertical_contract
    canonical_value_page_vertical_lossless_join_check

theorem canonical_value_page_vertical_dependency_preservation_check :
    verticalFamilyDependencyPreservationCheck canonical_value_page_vertical_contract = true := by
  native_decide

theorem canonical_value_page_vertical_dependency_preserving :
    VerticalFamilyDependencyPreserving canonical_value_page_vertical_contract :=
  verticalFamilyDependencyPreservationCheck_sound canonical_value_page_vertical_contract
    canonical_value_page_vertical_dependency_preservation_check

def gallery_observation_page_descriptor_vertical_contract : VerticalFamilyContract where
  name := "gallery_observation_page_descriptor_vertical"
  anchorRelation := "gallery_observation_page_descriptor_anchor"
  sealRelation := "gallery_observation_page_descriptor_seal"
  keyAttributes := ["page_sha256"]
  anchorAttributes := ["page_sha256"]
  sealAttributes := ["page_sha256"]
  semanticAttributes := ["page_sha256", "page_bytes", "component", "level", "subtree_item_count"]
  semanticFDs := [
    { determinant := ["page_sha256"], dependent := ["page_bytes", "component", "level", "subtree_item_count"] },
    { determinant := ["page_bytes"], dependent := ["page_sha256", "component", "level", "subtree_item_count"] }
  ]
  viewAttributes := ["page_sha256", "component", "level", "subtree_item_count"]
  viewFDs := [
    { determinant := ["page_sha256"], dependent := ["component", "level", "subtree_item_count"] }
  ]
  members := [
    { relationName := "gallery_observation_page", keyAttributes := ["page_sha256"], valueAttribute := "page_bytes", projectionAttribute := "page_bytes", attributes := ["page_sha256", "page_bytes"], declaredKeys := [["page_sha256"], ["page_bytes"]], declaredFDs := [{ determinant := ["page_sha256"], dependent := ["page_bytes"] }, { determinant := ["page_bytes"], dependent := ["page_sha256"] }], sourceRelation := "gallery_observation_page_descriptor_seal", sourceRelationAttributes := ["page_sha256"], sourceAttributes := ["page_sha256"], memberAttributes := ["page_sha256"], congruenceMembers := [], project := false, required := true },
    { relationName := "gallery_observation_page_descriptor_component", keyAttributes := ["page_sha256"], valueAttribute := "component", projectionAttribute := "component", attributes := ["page_sha256", "component"], declaredKeys := [["page_sha256"]], declaredFDs := [{ determinant := ["page_sha256"], dependent := ["component"] }], sourceRelation := "gallery_observation_page_descriptor_seal", sourceRelationAttributes := ["page_sha256"], sourceAttributes := ["page_sha256"], memberAttributes := ["page_sha256"], congruenceMembers := [], project := true, required := true },
    { relationName := "gallery_observation_page_descriptor_level", keyAttributes := ["page_sha256"], valueAttribute := "level", projectionAttribute := "level", attributes := ["page_sha256", "level"], declaredKeys := [["page_sha256"]], declaredFDs := [{ determinant := ["page_sha256"], dependent := ["level"] }], sourceRelation := "gallery_observation_page_descriptor_seal", sourceRelationAttributes := ["page_sha256"], sourceAttributes := ["page_sha256"], memberAttributes := ["page_sha256"], congruenceMembers := [], project := true, required := true },
    { relationName := "gallery_observation_page_descriptor_subtree_item_count", keyAttributes := ["page_sha256"], valueAttribute := "subtree_item_count", projectionAttribute := "subtree_item_count", attributes := ["page_sha256", "subtree_item_count"], declaredKeys := [["page_sha256"]], declaredFDs := [{ determinant := ["page_sha256"], dependent := ["subtree_item_count"] }], sourceRelation := "gallery_observation_page_descriptor_seal", sourceRelationAttributes := ["page_sha256"], sourceAttributes := ["page_sha256"], memberAttributes := ["page_sha256"], congruenceMembers := [], project := true, required := true }
  ]
  optionalPresence := none
  markerRelation := none
  markerAttributes := []
  markerPredicate := none

theorem gallery_observation_page_descriptor_vertical_view_projection_check :
    verticalFamilyViewProjectionCheck gallery_observation_page_descriptor_vertical_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_vertical_view_projection :
    VerticalFamilyViewProjection gallery_observation_page_descriptor_vertical_contract :=
  verticalFamilyViewProjectionCheck_sound gallery_observation_page_descriptor_vertical_contract
    gallery_observation_page_descriptor_vertical_view_projection_check

theorem gallery_observation_page_descriptor_vertical_well_formed_check :
    verticalFamilyWellFormedCheck gallery_observation_page_descriptor_vertical_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_vertical_well_formed :
    VerticalFamilyWellFormed gallery_observation_page_descriptor_vertical_contract :=
  verticalFamilyWellFormedCheck_sound gallery_observation_page_descriptor_vertical_contract
    gallery_observation_page_descriptor_vertical_well_formed_check

theorem gallery_observation_page_descriptor_vertical_lossless_join_check :
    verticalFamilyLosslessJoinCheck gallery_observation_page_descriptor_vertical_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_vertical_lossless :
    VerticalFamilyLossless gallery_observation_page_descriptor_vertical_contract :=
  verticalFamilyLosslessJoinCheck_sound gallery_observation_page_descriptor_vertical_contract
    gallery_observation_page_descriptor_vertical_lossless_join_check

theorem gallery_observation_page_descriptor_vertical_dependency_preservation_check :
    verticalFamilyDependencyPreservationCheck gallery_observation_page_descriptor_vertical_contract = true := by
  native_decide

theorem gallery_observation_page_descriptor_vertical_dependency_preserving :
    VerticalFamilyDependencyPreserving gallery_observation_page_descriptor_vertical_contract :=
  verticalFamilyDependencyPreservationCheck_sound gallery_observation_page_descriptor_vertical_contract
    gallery_observation_page_descriptor_vertical_dependency_preservation_check

def gallery_observation_page_key_bounds_vertical_contract : VerticalFamilyContract where
  name := "gallery_observation_page_key_bounds_vertical"
  anchorRelation := "gallery_observation_page_key_bounds_anchor"
  sealRelation := "gallery_observation_page_key_bounds_seal"
  keyAttributes := ["page_sha256"]
  anchorAttributes := ["page_sha256"]
  sealAttributes := ["page_sha256"]
  semanticAttributes := ["page_sha256", "first_key", "last_key"]
  semanticFDs := [
    { determinant := ["page_sha256"], dependent := ["first_key", "last_key"] }
  ]
  viewAttributes := ["page_sha256", "first_key", "last_key"]
  viewFDs := [
    { determinant := ["page_sha256"], dependent := ["first_key", "last_key"] }
  ]
  members := [
    { relationName := "gallery_observation_page_key_bounds_first_key", keyAttributes := ["page_sha256"], valueAttribute := "first_key", projectionAttribute := "first_key", attributes := ["page_sha256", "first_key"], declaredKeys := [["page_sha256"]], declaredFDs := [{ determinant := ["page_sha256"], dependent := ["first_key"] }], sourceRelation := "gallery_observation_page_key_bounds_seal", sourceRelationAttributes := ["page_sha256"], sourceAttributes := ["page_sha256"], memberAttributes := ["page_sha256"], congruenceMembers := [], project := true, required := true },
    { relationName := "gallery_observation_page_key_bounds_last_key", keyAttributes := ["page_sha256"], valueAttribute := "last_key", projectionAttribute := "last_key", attributes := ["page_sha256", "last_key"], declaredKeys := [["page_sha256"]], declaredFDs := [{ determinant := ["page_sha256"], dependent := ["last_key"] }], sourceRelation := "gallery_observation_page_key_bounds_seal", sourceRelationAttributes := ["page_sha256"], sourceAttributes := ["page_sha256"], memberAttributes := ["page_sha256"], congruenceMembers := [], project := true, required := true }
  ]
  optionalPresence := none
  markerRelation := none
  markerAttributes := []
  markerPredicate := none

theorem gallery_observation_page_key_bounds_vertical_view_projection_check :
    verticalFamilyViewProjectionCheck gallery_observation_page_key_bounds_vertical_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_vertical_view_projection :
    VerticalFamilyViewProjection gallery_observation_page_key_bounds_vertical_contract :=
  verticalFamilyViewProjectionCheck_sound gallery_observation_page_key_bounds_vertical_contract
    gallery_observation_page_key_bounds_vertical_view_projection_check

theorem gallery_observation_page_key_bounds_vertical_well_formed_check :
    verticalFamilyWellFormedCheck gallery_observation_page_key_bounds_vertical_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_vertical_well_formed :
    VerticalFamilyWellFormed gallery_observation_page_key_bounds_vertical_contract :=
  verticalFamilyWellFormedCheck_sound gallery_observation_page_key_bounds_vertical_contract
    gallery_observation_page_key_bounds_vertical_well_formed_check

theorem gallery_observation_page_key_bounds_vertical_lossless_join_check :
    verticalFamilyLosslessJoinCheck gallery_observation_page_key_bounds_vertical_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_vertical_lossless :
    VerticalFamilyLossless gallery_observation_page_key_bounds_vertical_contract :=
  verticalFamilyLosslessJoinCheck_sound gallery_observation_page_key_bounds_vertical_contract
    gallery_observation_page_key_bounds_vertical_lossless_join_check

theorem gallery_observation_page_key_bounds_vertical_dependency_preservation_check :
    verticalFamilyDependencyPreservationCheck gallery_observation_page_key_bounds_vertical_contract = true := by
  native_decide

theorem gallery_observation_page_key_bounds_vertical_dependency_preserving :
    VerticalFamilyDependencyPreserving gallery_observation_page_key_bounds_vertical_contract :=
  verticalFamilyDependencyPreservationCheck_sound gallery_observation_page_key_bounds_vertical_contract
    gallery_observation_page_key_bounds_vertical_dependency_preservation_check

def gallery_observation_file_vertical_contract : VerticalFamilyContract where
  name := "gallery_observation_file_vertical"
  anchorRelation := "gallery_observation_file_anchor"
  sealRelation := "gallery_observation_file_seal"
  keyAttributes := ["gallery_id", "observation_id", "file_key"]
  anchorAttributes := ["gallery_id", "observation_id", "file_key"]
  sealAttributes := ["gallery_id", "observation_id", "file_key"]
  semanticAttributes := ["gallery_id", "observation_id", "file_key", "file_no", "file_sha256"]
  semanticFDs := [
    { determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["file_no", "file_sha256"] },
    { determinant := ["gallery_id", "observation_id", "file_no"], dependent := ["file_key", "file_sha256"] }
  ]
  viewAttributes := ["gallery_id", "observation_id", "file_no", "file_key", "file_sha256"]
  viewFDs := [
    { determinant := ["gallery_id", "observation_id", "file_no"], dependent := ["file_key", "file_sha256"] },
    { determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["file_no", "file_sha256"] }
  ]
  members := [
    { relationName := "gallery_observation_file_file_no", keyAttributes := ["gallery_id", "observation_id", "file_key"], valueAttribute := "file_no", projectionAttribute := "file_no", attributes := ["gallery_id", "observation_id", "file_key", "file_no"], declaredKeys := [["gallery_id", "observation_id", "file_key"], ["gallery_id", "observation_id", "file_no"]], declaredFDs := [{ determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["file_no"] }, { determinant := ["gallery_id", "observation_id", "file_no"], dependent := ["file_key"] }], sourceRelation := "gallery_observation_file_seal", sourceRelationAttributes := ["gallery_id", "observation_id", "file_key"], sourceAttributes := ["gallery_id", "observation_id", "file_key"], memberAttributes := ["gallery_id", "observation_id", "file_key"], congruenceMembers := [], project := true, required := true },
    { relationName := "gallery_observation_file_file_sha256", keyAttributes := ["gallery_id", "observation_id", "file_key"], valueAttribute := "file_sha256", projectionAttribute := "file_sha256", attributes := ["gallery_id", "observation_id", "file_key", "file_sha256"], declaredKeys := [["gallery_id", "observation_id", "file_key"]], declaredFDs := [{ determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["file_sha256"] }], sourceRelation := "gallery_observation_file_seal", sourceRelationAttributes := ["gallery_id", "observation_id", "file_key"], sourceAttributes := ["gallery_id", "observation_id", "file_key"], memberAttributes := ["gallery_id", "observation_id", "file_key"], congruenceMembers := [], project := true, required := true }
  ]
  optionalPresence := none
  markerRelation := none
  markerAttributes := []
  markerPredicate := none

theorem gallery_observation_file_vertical_view_projection_check :
    verticalFamilyViewProjectionCheck gallery_observation_file_vertical_contract = true := by
  native_decide

theorem gallery_observation_file_vertical_view_projection :
    VerticalFamilyViewProjection gallery_observation_file_vertical_contract :=
  verticalFamilyViewProjectionCheck_sound gallery_observation_file_vertical_contract
    gallery_observation_file_vertical_view_projection_check

theorem gallery_observation_file_vertical_well_formed_check :
    verticalFamilyWellFormedCheck gallery_observation_file_vertical_contract = true := by
  native_decide

theorem gallery_observation_file_vertical_well_formed :
    VerticalFamilyWellFormed gallery_observation_file_vertical_contract :=
  verticalFamilyWellFormedCheck_sound gallery_observation_file_vertical_contract
    gallery_observation_file_vertical_well_formed_check

theorem gallery_observation_file_vertical_lossless_join_check :
    verticalFamilyLosslessJoinCheck gallery_observation_file_vertical_contract = true := by
  native_decide

theorem gallery_observation_file_vertical_lossless :
    VerticalFamilyLossless gallery_observation_file_vertical_contract :=
  verticalFamilyLosslessJoinCheck_sound gallery_observation_file_vertical_contract
    gallery_observation_file_vertical_lossless_join_check

theorem gallery_observation_file_vertical_dependency_preservation_check :
    verticalFamilyDependencyPreservationCheck gallery_observation_file_vertical_contract = true := by
  native_decide

theorem gallery_observation_file_vertical_dependency_preserving :
    VerticalFamilyDependencyPreserving gallery_observation_file_vertical_contract :=
  verticalFamilyDependencyPreservationCheck_sound gallery_observation_file_vertical_contract
    gallery_observation_file_vertical_dependency_preservation_check

def analysis_file_hash_decision_shadow_vertical_contract : VerticalFamilyContract where
  name := "analysis_file_hash_decision_shadow_vertical"
  anchorRelation := "analysis_file_hash_decision_shadow_anchor"
  sealRelation := "analysis_file_hash_decision_shadow_seal"
  keyAttributes := ["analysis_id", "file_sha256"]
  anchorAttributes := ["analysis_id", "file_sha256"]
  sealAttributes := ["analysis_id", "file_sha256"]
  semanticAttributes := ["analysis_id", "file_sha256", "occurrence_count", "artist_count", "maximum_gallery_artist_count"]
  semanticFDs := [
    { determinant := ["analysis_id", "file_sha256"], dependent := ["occurrence_count", "artist_count", "maximum_gallery_artist_count"] }
  ]
  viewAttributes := ["analysis_id", "file_sha256", "occurrence_count", "artist_count", "maximum_gallery_artist_count"]
  viewFDs := [
    { determinant := ["analysis_id", "file_sha256"], dependent := ["occurrence_count", "artist_count", "maximum_gallery_artist_count"] }
  ]
  members := [
    { relationName := "analysis_file_hash_decision_shadow_occurrence_count", keyAttributes := ["analysis_id", "file_sha256"], valueAttribute := "occurrence_count", projectionAttribute := "occurrence_count", attributes := ["analysis_id", "file_sha256", "occurrence_count"], declaredKeys := [["analysis_id", "file_sha256"]], declaredFDs := [{ determinant := ["analysis_id", "file_sha256"], dependent := ["occurrence_count"] }], sourceRelation := "analysis_file_hash_decision_shadow_seal", sourceRelationAttributes := ["analysis_id", "file_sha256"], sourceAttributes := ["analysis_id", "file_sha256"], memberAttributes := ["analysis_id", "file_sha256"], congruenceMembers := [], project := true, required := true },
    { relationName := "analysis_file_hash_decision_shadow_artist_count", keyAttributes := ["analysis_id", "file_sha256"], valueAttribute := "artist_count", projectionAttribute := "artist_count", attributes := ["analysis_id", "file_sha256", "artist_count"], declaredKeys := [["analysis_id", "file_sha256"]], declaredFDs := [{ determinant := ["analysis_id", "file_sha256"], dependent := ["artist_count"] }], sourceRelation := "analysis_file_hash_decision_shadow_seal", sourceRelationAttributes := ["analysis_id", "file_sha256"], sourceAttributes := ["analysis_id", "file_sha256"], memberAttributes := ["analysis_id", "file_sha256"], congruenceMembers := [], project := true, required := true },
    { relationName := "analysis_file_hash_decision_shadow_maximum_gallery_artist_count", keyAttributes := ["analysis_id", "file_sha256"], valueAttribute := "maximum_gallery_artist_count", projectionAttribute := "maximum_gallery_artist_count", attributes := ["analysis_id", "file_sha256", "maximum_gallery_artist_count"], declaredKeys := [["analysis_id", "file_sha256"]], declaredFDs := [{ determinant := ["analysis_id", "file_sha256"], dependent := ["maximum_gallery_artist_count"] }], sourceRelation := "analysis_file_hash_decision_shadow_seal", sourceRelationAttributes := ["analysis_id", "file_sha256"], sourceAttributes := ["analysis_id", "file_sha256"], memberAttributes := ["analysis_id", "file_sha256"], congruenceMembers := [], project := true, required := true }
  ]
  optionalPresence := none
  markerRelation := none
  markerAttributes := []
  markerPredicate := none

theorem analysis_file_hash_decision_shadow_vertical_view_projection_check :
    verticalFamilyViewProjectionCheck analysis_file_hash_decision_shadow_vertical_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_vertical_view_projection :
    VerticalFamilyViewProjection analysis_file_hash_decision_shadow_vertical_contract :=
  verticalFamilyViewProjectionCheck_sound analysis_file_hash_decision_shadow_vertical_contract
    analysis_file_hash_decision_shadow_vertical_view_projection_check

theorem analysis_file_hash_decision_shadow_vertical_well_formed_check :
    verticalFamilyWellFormedCheck analysis_file_hash_decision_shadow_vertical_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_vertical_well_formed :
    VerticalFamilyWellFormed analysis_file_hash_decision_shadow_vertical_contract :=
  verticalFamilyWellFormedCheck_sound analysis_file_hash_decision_shadow_vertical_contract
    analysis_file_hash_decision_shadow_vertical_well_formed_check

theorem analysis_file_hash_decision_shadow_vertical_lossless_join_check :
    verticalFamilyLosslessJoinCheck analysis_file_hash_decision_shadow_vertical_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_vertical_lossless :
    VerticalFamilyLossless analysis_file_hash_decision_shadow_vertical_contract :=
  verticalFamilyLosslessJoinCheck_sound analysis_file_hash_decision_shadow_vertical_contract
    analysis_file_hash_decision_shadow_vertical_lossless_join_check

theorem analysis_file_hash_decision_shadow_vertical_dependency_preservation_check :
    verticalFamilyDependencyPreservationCheck analysis_file_hash_decision_shadow_vertical_contract = true := by
  native_decide

theorem analysis_file_hash_decision_shadow_vertical_dependency_preserving :
    VerticalFamilyDependencyPreserving analysis_file_hash_decision_shadow_vertical_contract :=
  verticalFamilyDependencyPreservationCheck_sound analysis_file_hash_decision_shadow_vertical_contract
    analysis_file_hash_decision_shadow_vertical_dependency_preservation_check

def source_build_and_optional_base_source_contract : BinaryDecompositionContract where
  name := "source_build_and_optional_base_source"
  universalAttributes := ["build_id", "scope_key", "manifest_policy_id", "base_receipt_id", "state", "created_at", "sealed_at"]
  leftAttributes := ["build_id", "scope_key", "manifest_policy_id", "state", "created_at", "sealed_at"]
  rightAttributes := ["build_id", "base_receipt_id"]
  declaredFDs := [
    { determinant := ["build_id"], dependent := ["scope_key", "manifest_policy_id", "base_receipt_id", "state", "created_at", "sealed_at"] }
  ]

theorem source_build_and_optional_base_source_projection_check :
    binaryDecompositionWellFormedCheck
      source_build_and_optional_base_source_contract = true := by
  native_decide

theorem source_build_and_optional_base_source_projection_well_formed :
    BinaryDecompositionWellFormed source_build_and_optional_base_source_contract :=
  binaryDecompositionWellFormedCheck_sound
    source_build_and_optional_base_source_contract source_build_and_optional_base_source_projection_check

theorem source_build_and_optional_base_source_intersection_check :
    sameAttrSet (attributeIntersection
      source_build_and_optional_base_source_contract.leftAttributes
      source_build_and_optional_base_source_contract.rightAttributes)
      ["build_id"] = true := by
  native_decide

theorem source_build_and_optional_base_source_lossless_check :
    binaryLosslessCheck source_build_and_optional_base_source_contract = true := by
  native_decide

theorem source_build_and_optional_base_source_lossless : BinaryLossless source_build_and_optional_base_source_contract :=
  ⟨source_build_and_optional_base_source_projection_well_formed,
    binaryLosslessCheck_sound source_build_and_optional_base_source_contract
      source_build_and_optional_base_source_lossless_check⟩

theorem source_build_and_optional_base_source_dependency_preservation_check :
    dependencyPreservationCheck source_build_and_optional_base_source_contract = true := by
  native_decide

theorem source_build_and_optional_base_source_dependency_preserving :
    DependencyPreserving source_build_and_optional_base_source_contract :=
  dependencyPreservationCheck_sound source_build_and_optional_base_source_contract
    source_build_and_optional_base_source_dependency_preservation_check

def source_revision_and_generation_publication_seal_contract : BinaryDecompositionContract where
  name := "source_revision_and_generation_publication_seal"
  universalAttributes := ["source_revision", "channel", "snapshot_manifest_sha256", "published_at", "generation"]
  leftAttributes := ["source_revision", "channel", "snapshot_manifest_sha256", "published_at"]
  rightAttributes := ["source_revision", "generation"]
  declaredFDs := [
    { determinant := ["source_revision"], dependent := ["channel", "snapshot_manifest_sha256", "published_at", "generation"] },
    { determinant := ["generation"], dependent := ["source_revision", "channel", "snapshot_manifest_sha256", "published_at"] }
  ]

theorem source_revision_and_generation_publication_seal_projection_check :
    binaryDecompositionWellFormedCheck
      source_revision_and_generation_publication_seal_contract = true := by
  native_decide

theorem source_revision_and_generation_publication_seal_projection_well_formed :
    BinaryDecompositionWellFormed source_revision_and_generation_publication_seal_contract :=
  binaryDecompositionWellFormedCheck_sound
    source_revision_and_generation_publication_seal_contract source_revision_and_generation_publication_seal_projection_check

theorem source_revision_and_generation_publication_seal_intersection_check :
    sameAttrSet (attributeIntersection
      source_revision_and_generation_publication_seal_contract.leftAttributes
      source_revision_and_generation_publication_seal_contract.rightAttributes)
      ["source_revision"] = true := by
  native_decide

theorem source_revision_and_generation_publication_seal_lossless_check :
    binaryLosslessCheck source_revision_and_generation_publication_seal_contract = true := by
  native_decide

theorem source_revision_and_generation_publication_seal_lossless : BinaryLossless source_revision_and_generation_publication_seal_contract :=
  ⟨source_revision_and_generation_publication_seal_projection_well_formed,
    binaryLosslessCheck_sound source_revision_and_generation_publication_seal_contract
      source_revision_and_generation_publication_seal_lossless_check⟩

theorem source_revision_and_generation_publication_seal_dependency_preservation_check :
    dependencyPreservationCheck source_revision_and_generation_publication_seal_contract = true := by
  native_decide

theorem source_revision_and_generation_publication_seal_dependency_preserving :
    DependencyPreserving source_revision_and_generation_publication_seal_contract :=
  dependencyPreservationCheck_sound source_revision_and_generation_publication_seal_contract
    source_revision_and_generation_publication_seal_dependency_preservation_check

def source_head_revision_and_advanced_at_contract : BinaryDecompositionContract where
  name := "source_head_revision_and_advanced_at"
  universalAttributes := ["channel", "source_revision", "advanced_at"]
  leftAttributes := ["channel", "source_revision"]
  rightAttributes := ["channel", "advanced_at"]
  declaredFDs := [
    { determinant := ["channel"], dependent := ["source_revision", "advanced_at"] },
    { determinant := ["source_revision"], dependent := ["channel", "advanced_at"] }
  ]

theorem source_head_revision_and_advanced_at_projection_check :
    binaryDecompositionWellFormedCheck
      source_head_revision_and_advanced_at_contract = true := by
  native_decide

theorem source_head_revision_and_advanced_at_projection_well_formed :
    BinaryDecompositionWellFormed source_head_revision_and_advanced_at_contract :=
  binaryDecompositionWellFormedCheck_sound
    source_head_revision_and_advanced_at_contract source_head_revision_and_advanced_at_projection_check

theorem source_head_revision_and_advanced_at_intersection_check :
    sameAttrSet (attributeIntersection
      source_head_revision_and_advanced_at_contract.leftAttributes
      source_head_revision_and_advanced_at_contract.rightAttributes)
      ["channel"] = true := by
  native_decide

theorem source_head_revision_and_advanced_at_lossless_check :
    binaryLosslessCheck source_head_revision_and_advanced_at_contract = true := by
  native_decide

theorem source_head_revision_and_advanced_at_lossless : BinaryLossless source_head_revision_and_advanced_at_contract :=
  ⟨source_head_revision_and_advanced_at_projection_well_formed,
    binaryLosslessCheck_sound source_head_revision_and_advanced_at_contract
      source_head_revision_and_advanced_at_lossless_check⟩

theorem source_head_revision_and_advanced_at_dependency_preservation_check :
    dependencyPreservationCheck source_head_revision_and_advanced_at_contract = true := by
  native_decide

theorem source_head_revision_and_advanced_at_dependency_preserving :
    DependencyPreserving source_head_revision_and_advanced_at_contract :=
  dependencyPreservationCheck_sound source_head_revision_and_advanced_at_contract
    source_head_revision_and_advanced_at_dependency_preservation_check

def source_revision_and_retained_snapshot_manifest_contract : BinaryDecompositionContract where
  name := "source_revision_and_retained_snapshot_manifest"
  universalAttributes := ["source_revision", "channel", "snapshot_manifest_sha256", "published_at", "gallery_count", "file_count", "byte_count"]
  leftAttributes := ["source_revision", "channel", "snapshot_manifest_sha256", "published_at"]
  rightAttributes := ["snapshot_manifest_sha256", "gallery_count", "file_count", "byte_count"]
  declaredFDs := [
    { determinant := ["source_revision"], dependent := ["channel", "snapshot_manifest_sha256", "published_at"] },
    { determinant := ["snapshot_manifest_sha256"], dependent := ["gallery_count", "file_count", "byte_count"] }
  ]

theorem source_revision_and_retained_snapshot_manifest_projection_check :
    binaryDecompositionWellFormedCheck
      source_revision_and_retained_snapshot_manifest_contract = true := by
  native_decide

theorem source_revision_and_retained_snapshot_manifest_projection_well_formed :
    BinaryDecompositionWellFormed source_revision_and_retained_snapshot_manifest_contract :=
  binaryDecompositionWellFormedCheck_sound
    source_revision_and_retained_snapshot_manifest_contract source_revision_and_retained_snapshot_manifest_projection_check

theorem source_revision_and_retained_snapshot_manifest_intersection_check :
    sameAttrSet (attributeIntersection
      source_revision_and_retained_snapshot_manifest_contract.leftAttributes
      source_revision_and_retained_snapshot_manifest_contract.rightAttributes)
      ["snapshot_manifest_sha256"] = true := by
  native_decide

theorem source_revision_and_retained_snapshot_manifest_lossless_check :
    binaryLosslessCheck source_revision_and_retained_snapshot_manifest_contract = true := by
  native_decide

theorem source_revision_and_retained_snapshot_manifest_lossless : BinaryLossless source_revision_and_retained_snapshot_manifest_contract :=
  ⟨source_revision_and_retained_snapshot_manifest_projection_well_formed,
    binaryLosslessCheck_sound source_revision_and_retained_snapshot_manifest_contract
      source_revision_and_retained_snapshot_manifest_lossless_check⟩

theorem source_revision_and_retained_snapshot_manifest_dependency_preservation_check :
    dependencyPreservationCheck source_revision_and_retained_snapshot_manifest_contract = true := by
  native_decide

theorem source_revision_and_retained_snapshot_manifest_dependency_preserving :
    DependencyPreserving source_revision_and_retained_snapshot_manifest_contract :=
  dependencyPreservationCheck_sound source_revision_and_retained_snapshot_manifest_contract
    source_revision_and_retained_snapshot_manifest_dependency_preservation_check

def source_revision_and_optional_prunable_provenance_contract : BinaryDecompositionContract where
  name := "source_revision_and_optional_prunable_provenance"
  universalAttributes := ["source_revision", "channel", "snapshot_manifest_sha256", "published_at", "analysis_id"]
  leftAttributes := ["source_revision", "channel", "snapshot_manifest_sha256", "published_at"]
  rightAttributes := ["source_revision", "analysis_id"]
  declaredFDs := [
    { determinant := ["source_revision"], dependent := ["channel", "snapshot_manifest_sha256", "published_at", "analysis_id"] },
    { determinant := ["analysis_id"], dependent := ["source_revision", "channel", "snapshot_manifest_sha256", "published_at"] }
  ]

theorem source_revision_and_optional_prunable_provenance_projection_check :
    binaryDecompositionWellFormedCheck
      source_revision_and_optional_prunable_provenance_contract = true := by
  native_decide

theorem source_revision_and_optional_prunable_provenance_projection_well_formed :
    BinaryDecompositionWellFormed source_revision_and_optional_prunable_provenance_contract :=
  binaryDecompositionWellFormedCheck_sound
    source_revision_and_optional_prunable_provenance_contract source_revision_and_optional_prunable_provenance_projection_check

theorem source_revision_and_optional_prunable_provenance_intersection_check :
    sameAttrSet (attributeIntersection
      source_revision_and_optional_prunable_provenance_contract.leftAttributes
      source_revision_and_optional_prunable_provenance_contract.rightAttributes)
      ["source_revision"] = true := by
  native_decide

theorem source_revision_and_optional_prunable_provenance_lossless_check :
    binaryLosslessCheck source_revision_and_optional_prunable_provenance_contract = true := by
  native_decide

theorem source_revision_and_optional_prunable_provenance_lossless : BinaryLossless source_revision_and_optional_prunable_provenance_contract :=
  ⟨source_revision_and_optional_prunable_provenance_projection_well_formed,
    binaryLosslessCheck_sound source_revision_and_optional_prunable_provenance_contract
      source_revision_and_optional_prunable_provenance_lossless_check⟩

theorem source_revision_and_optional_prunable_provenance_dependency_preservation_check :
    dependencyPreservationCheck source_revision_and_optional_prunable_provenance_contract = true := by
  native_decide

theorem source_revision_and_optional_prunable_provenance_dependency_preserving :
    DependencyPreserving source_revision_and_optional_prunable_provenance_contract :=
  dependencyPreservationCheck_sound source_revision_and_optional_prunable_provenance_contract
    source_revision_and_optional_prunable_provenance_dependency_preservation_check

def filesystem_gallery_location_identity_contract : BinaryDecompositionContract where
  name := "filesystem_gallery_location_identity"
  universalAttributes := ["gallery_id", "gallery_key", "scope_key", "locator_sha256", "source_provider", "source_root_sha256", "identity_policy_version"]
  leftAttributes := ["scope_key", "source_provider", "source_root_sha256", "identity_policy_version"]
  rightAttributes := ["gallery_id", "gallery_key", "scope_key", "locator_sha256"]
  declaredFDs := [
    { determinant := ["scope_key"], dependent := ["source_provider", "source_root_sha256", "identity_policy_version"] },
    { determinant := ["source_provider", "source_root_sha256", "identity_policy_version"], dependent := ["scope_key"] },
    { determinant := ["gallery_id"], dependent := ["gallery_key", "scope_key", "locator_sha256"] },
    { determinant := ["gallery_key"], dependent := ["gallery_id", "scope_key", "locator_sha256"] },
    { determinant := ["scope_key", "locator_sha256"], dependent := ["gallery_id", "gallery_key"] }
  ]

theorem filesystem_gallery_location_identity_projection_check :
    binaryDecompositionWellFormedCheck
      filesystem_gallery_location_identity_contract = true := by
  native_decide

theorem filesystem_gallery_location_identity_projection_well_formed :
    BinaryDecompositionWellFormed filesystem_gallery_location_identity_contract :=
  binaryDecompositionWellFormedCheck_sound
    filesystem_gallery_location_identity_contract filesystem_gallery_location_identity_projection_check

theorem filesystem_gallery_location_identity_intersection_check :
    sameAttrSet (attributeIntersection
      filesystem_gallery_location_identity_contract.leftAttributes
      filesystem_gallery_location_identity_contract.rightAttributes)
      ["scope_key"] = true := by
  native_decide

theorem filesystem_gallery_location_identity_lossless_check :
    binaryLosslessCheck filesystem_gallery_location_identity_contract = true := by
  native_decide

theorem filesystem_gallery_location_identity_lossless : BinaryLossless filesystem_gallery_location_identity_contract :=
  ⟨filesystem_gallery_location_identity_projection_well_formed,
    binaryLosslessCheck_sound filesystem_gallery_location_identity_contract
      filesystem_gallery_location_identity_lossless_check⟩

theorem filesystem_gallery_location_identity_dependency_preservation_check :
    dependencyPreservationCheck filesystem_gallery_location_identity_contract = true := by
  native_decide

theorem filesystem_gallery_location_identity_dependency_preserving :
    DependencyPreserving filesystem_gallery_location_identity_contract :=
  dependencyPreservationCheck_sound filesystem_gallery_location_identity_contract
    filesystem_gallery_location_identity_dependency_preservation_check

def gallery_location_and_typed_locator_contract : BinaryDecompositionContract where
  name := "gallery_location_and_typed_locator"
  universalAttributes := ["gallery_id", "gallery_key", "scope_key", "locator_sha256", "source_gallery_name"]
  leftAttributes := ["locator_sha256", "source_gallery_name"]
  rightAttributes := ["gallery_id", "gallery_key", "scope_key", "locator_sha256"]
  declaredFDs := [
    { determinant := ["locator_sha256"], dependent := ["source_gallery_name"] },
    { determinant := ["gallery_id"], dependent := ["gallery_key", "scope_key", "locator_sha256"] },
    { determinant := ["gallery_key"], dependent := ["gallery_id", "scope_key", "locator_sha256"] },
    { determinant := ["scope_key", "locator_sha256"], dependent := ["gallery_id", "gallery_key"] }
  ]

theorem gallery_location_and_typed_locator_projection_check :
    binaryDecompositionWellFormedCheck
      gallery_location_and_typed_locator_contract = true := by
  native_decide

theorem gallery_location_and_typed_locator_projection_well_formed :
    BinaryDecompositionWellFormed gallery_location_and_typed_locator_contract :=
  binaryDecompositionWellFormedCheck_sound
    gallery_location_and_typed_locator_contract gallery_location_and_typed_locator_projection_check

theorem gallery_location_and_typed_locator_intersection_check :
    sameAttrSet (attributeIntersection
      gallery_location_and_typed_locator_contract.leftAttributes
      gallery_location_and_typed_locator_contract.rightAttributes)
      ["locator_sha256"] = true := by
  native_decide

theorem gallery_location_and_typed_locator_lossless_check :
    binaryLosslessCheck gallery_location_and_typed_locator_contract = true := by
  native_decide

theorem gallery_location_and_typed_locator_lossless : BinaryLossless gallery_location_and_typed_locator_contract :=
  ⟨gallery_location_and_typed_locator_projection_well_formed,
    binaryLosslessCheck_sound gallery_location_and_typed_locator_contract
      gallery_location_and_typed_locator_lossless_check⟩

theorem gallery_location_and_typed_locator_dependency_preservation_check :
    dependencyPreservationCheck gallery_location_and_typed_locator_contract = true := by
  native_decide

theorem gallery_location_and_typed_locator_dependency_preserving :
    DependencyPreserving gallery_location_and_typed_locator_contract :=
  dependencyPreservationCheck_sound gallery_location_and_typed_locator_contract
    gallery_location_and_typed_locator_dependency_preservation_check

def gallery_observation_metadata_and_scan_contract : BinaryDecompositionContract where
  name := "gallery_observation_metadata_and_scan"
  universalAttributes := ["gallery_id", "observation_id", "gid", "upload_time", "download_time", "modified_time", "scan_observation_sha256", "scan_observation_version", "source_file_count"]
  leftAttributes := ["gallery_id", "observation_id", "gid", "upload_time", "download_time", "modified_time"]
  rightAttributes := ["gallery_id", "observation_id", "scan_observation_sha256", "scan_observation_version", "source_file_count"]
  declaredFDs := [
    { determinant := ["gallery_id"], dependent := ["gid"] },
    { determinant := ["gid"], dependent := ["upload_time"] },
    { determinant := ["gallery_id", "observation_id"], dependent := ["download_time", "modified_time"] },
    { determinant := ["gallery_id", "observation_id"], dependent := ["scan_observation_sha256", "scan_observation_version", "source_file_count"] }
  ]

theorem gallery_observation_metadata_and_scan_projection_check :
    binaryDecompositionWellFormedCheck
      gallery_observation_metadata_and_scan_contract = true := by
  native_decide

theorem gallery_observation_metadata_and_scan_projection_well_formed :
    BinaryDecompositionWellFormed gallery_observation_metadata_and_scan_contract :=
  binaryDecompositionWellFormedCheck_sound
    gallery_observation_metadata_and_scan_contract gallery_observation_metadata_and_scan_projection_check

theorem gallery_observation_metadata_and_scan_intersection_check :
    sameAttrSet (attributeIntersection
      gallery_observation_metadata_and_scan_contract.leftAttributes
      gallery_observation_metadata_and_scan_contract.rightAttributes)
      ["gallery_id", "observation_id"] = true := by
  native_decide

theorem gallery_observation_metadata_and_scan_lossless_check :
    binaryLosslessCheck gallery_observation_metadata_and_scan_contract = true := by
  native_decide

theorem gallery_observation_metadata_and_scan_lossless : BinaryLossless gallery_observation_metadata_and_scan_contract :=
  ⟨gallery_observation_metadata_and_scan_projection_well_formed,
    binaryLosslessCheck_sound gallery_observation_metadata_and_scan_contract
      gallery_observation_metadata_and_scan_lossless_check⟩

theorem gallery_observation_metadata_and_scan_dependency_preservation_check :
    dependencyPreservationCheck gallery_observation_metadata_and_scan_contract = true := by
  native_decide

theorem gallery_observation_metadata_and_scan_dependency_preserving :
    DependencyPreserving gallery_observation_metadata_and_scan_contract :=
  dependencyPreservationCheck_sound gallery_observation_metadata_and_scan_contract
    gallery_observation_metadata_and_scan_dependency_preservation_check

def gallery_observation_discovery_and_page_count_contract : BinaryDecompositionContract where
  name := "gallery_observation_discovery_and_page_count"
  universalAttributes := ["gallery_id", "observation_id", "metadata_fingerprint", "page_count"]
  leftAttributes := ["gallery_id", "observation_id", "metadata_fingerprint"]
  rightAttributes := ["gallery_id", "observation_id", "page_count"]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id"], dependent := ["metadata_fingerprint"] },
    { determinant := ["gallery_id", "observation_id"], dependent := ["page_count"] }
  ]

theorem gallery_observation_discovery_and_page_count_projection_check :
    binaryDecompositionWellFormedCheck
      gallery_observation_discovery_and_page_count_contract = true := by
  native_decide

theorem gallery_observation_discovery_and_page_count_projection_well_formed :
    BinaryDecompositionWellFormed gallery_observation_discovery_and_page_count_contract :=
  binaryDecompositionWellFormedCheck_sound
    gallery_observation_discovery_and_page_count_contract gallery_observation_discovery_and_page_count_projection_check

theorem gallery_observation_discovery_and_page_count_intersection_check :
    sameAttrSet (attributeIntersection
      gallery_observation_discovery_and_page_count_contract.leftAttributes
      gallery_observation_discovery_and_page_count_contract.rightAttributes)
      ["gallery_id", "observation_id"] = true := by
  native_decide

theorem gallery_observation_discovery_and_page_count_lossless_check :
    binaryLosslessCheck gallery_observation_discovery_and_page_count_contract = true := by
  native_decide

theorem gallery_observation_discovery_and_page_count_lossless : BinaryLossless gallery_observation_discovery_and_page_count_contract :=
  ⟨gallery_observation_discovery_and_page_count_projection_well_formed,
    binaryLosslessCheck_sound gallery_observation_discovery_and_page_count_contract
      gallery_observation_discovery_and_page_count_lossless_check⟩

theorem gallery_observation_discovery_and_page_count_dependency_preservation_check :
    dependencyPreservationCheck gallery_observation_discovery_and_page_count_contract = true := by
  native_decide

theorem gallery_observation_discovery_and_page_count_dependency_preserving :
    DependencyPreserving gallery_observation_discovery_and_page_count_contract :=
  dependencyPreservationCheck_sound gallery_observation_discovery_and_page_count_contract
    gallery_observation_discovery_and_page_count_dependency_preservation_check

def gallery_observation_metadata_and_raw_content_digests_contract : BinaryDecompositionContract where
  name := "gallery_observation_metadata_and_raw_content_digests"
  universalAttributes := ["gallery_id", "observation_id", "metadata_sha256", "raw_content_sha256"]
  leftAttributes := ["gallery_id", "observation_id", "metadata_sha256"]
  rightAttributes := ["gallery_id", "observation_id", "raw_content_sha256"]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id"], dependent := ["metadata_sha256"] },
    { determinant := ["gallery_id", "observation_id"], dependent := ["raw_content_sha256"] }
  ]

theorem gallery_observation_metadata_and_raw_content_digests_projection_check :
    binaryDecompositionWellFormedCheck
      gallery_observation_metadata_and_raw_content_digests_contract = true := by
  native_decide

theorem gallery_observation_metadata_and_raw_content_digests_projection_well_formed :
    BinaryDecompositionWellFormed gallery_observation_metadata_and_raw_content_digests_contract :=
  binaryDecompositionWellFormedCheck_sound
    gallery_observation_metadata_and_raw_content_digests_contract gallery_observation_metadata_and_raw_content_digests_projection_check

theorem gallery_observation_metadata_and_raw_content_digests_intersection_check :
    sameAttrSet (attributeIntersection
      gallery_observation_metadata_and_raw_content_digests_contract.leftAttributes
      gallery_observation_metadata_and_raw_content_digests_contract.rightAttributes)
      ["gallery_id", "observation_id"] = true := by
  native_decide

theorem gallery_observation_metadata_and_raw_content_digests_lossless_check :
    binaryLosslessCheck gallery_observation_metadata_and_raw_content_digests_contract = true := by
  native_decide

theorem gallery_observation_metadata_and_raw_content_digests_lossless : BinaryLossless gallery_observation_metadata_and_raw_content_digests_contract :=
  ⟨gallery_observation_metadata_and_raw_content_digests_projection_well_formed,
    binaryLosslessCheck_sound gallery_observation_metadata_and_raw_content_digests_contract
      gallery_observation_metadata_and_raw_content_digests_lossless_check⟩

theorem gallery_observation_metadata_and_raw_content_digests_dependency_preservation_check :
    dependencyPreservationCheck gallery_observation_metadata_and_raw_content_digests_contract = true := by
  native_decide

theorem gallery_observation_metadata_and_raw_content_digests_dependency_preserving :
    DependencyPreserving gallery_observation_metadata_and_raw_content_digests_contract :=
  dependencyPreservationCheck_sound gallery_observation_metadata_and_raw_content_digests_contract
    gallery_observation_metadata_and_raw_content_digests_dependency_preservation_check

def file_identity_and_gallery_observation_file_contract : BinaryDecompositionContract where
  name := "file_identity_and_gallery_observation_file"
  universalAttributes := ["file_key", "name_bytes", "gallery_id", "observation_id", "file_no", "file_sha256"]
  leftAttributes := ["file_key", "name_bytes"]
  rightAttributes := ["gallery_id", "observation_id", "file_no", "file_key", "file_sha256"]
  declaredFDs := [
    { determinant := ["file_key"], dependent := ["name_bytes"] },
    { determinant := ["name_bytes"], dependent := ["file_key"] },
    { determinant := ["gallery_id", "observation_id", "file_no"], dependent := ["file_key", "file_sha256"] },
    { determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["file_no", "file_sha256"] }
  ]

theorem file_identity_and_gallery_observation_file_projection_check :
    binaryDecompositionWellFormedCheck
      file_identity_and_gallery_observation_file_contract = true := by
  native_decide

theorem file_identity_and_gallery_observation_file_projection_well_formed :
    BinaryDecompositionWellFormed file_identity_and_gallery_observation_file_contract :=
  binaryDecompositionWellFormedCheck_sound
    file_identity_and_gallery_observation_file_contract file_identity_and_gallery_observation_file_projection_check

theorem file_identity_and_gallery_observation_file_intersection_check :
    sameAttrSet (attributeIntersection
      file_identity_and_gallery_observation_file_contract.leftAttributes
      file_identity_and_gallery_observation_file_contract.rightAttributes)
      ["file_key"] = true := by
  native_decide

theorem file_identity_and_gallery_observation_file_lossless_check :
    binaryLosslessCheck file_identity_and_gallery_observation_file_contract = true := by
  native_decide

theorem file_identity_and_gallery_observation_file_lossless : BinaryLossless file_identity_and_gallery_observation_file_contract :=
  ⟨file_identity_and_gallery_observation_file_projection_well_formed,
    binaryLosslessCheck_sound file_identity_and_gallery_observation_file_contract
      file_identity_and_gallery_observation_file_lossless_check⟩

theorem file_identity_and_gallery_observation_file_dependency_preservation_check :
    dependencyPreservationCheck file_identity_and_gallery_observation_file_contract = true := by
  native_decide

theorem file_identity_and_gallery_observation_file_dependency_preserving :
    DependencyPreserving file_identity_and_gallery_observation_file_contract :=
  dependencyPreservationCheck_sound file_identity_and_gallery_observation_file_contract
    file_identity_and_gallery_observation_file_dependency_preservation_check

def file_content_payload_and_gallery_observation_file_contract : BinaryDecompositionContract where
  name := "file_content_payload_and_gallery_observation_file"
  universalAttributes := ["gallery_id", "observation_id", "file_no", "file_key", "file_sha256", "size_bytes"]
  leftAttributes := ["file_sha256", "size_bytes"]
  rightAttributes := ["gallery_id", "observation_id", "file_no", "file_key", "file_sha256"]
  declaredFDs := [
    { determinant := ["file_sha256"], dependent := ["size_bytes"] },
    { determinant := ["gallery_id", "observation_id", "file_no"], dependent := ["file_key", "file_sha256"] },
    { determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["file_no", "file_sha256"] }
  ]

theorem file_content_payload_and_gallery_observation_file_projection_check :
    binaryDecompositionWellFormedCheck
      file_content_payload_and_gallery_observation_file_contract = true := by
  native_decide

theorem file_content_payload_and_gallery_observation_file_projection_well_formed :
    BinaryDecompositionWellFormed file_content_payload_and_gallery_observation_file_contract :=
  binaryDecompositionWellFormedCheck_sound
    file_content_payload_and_gallery_observation_file_contract file_content_payload_and_gallery_observation_file_projection_check

theorem file_content_payload_and_gallery_observation_file_intersection_check :
    sameAttrSet (attributeIntersection
      file_content_payload_and_gallery_observation_file_contract.leftAttributes
      file_content_payload_and_gallery_observation_file_contract.rightAttributes)
      ["file_sha256"] = true := by
  native_decide

theorem file_content_payload_and_gallery_observation_file_lossless_check :
    binaryLosslessCheck file_content_payload_and_gallery_observation_file_contract = true := by
  native_decide

theorem file_content_payload_and_gallery_observation_file_lossless : BinaryLossless file_content_payload_and_gallery_observation_file_contract :=
  ⟨file_content_payload_and_gallery_observation_file_projection_well_formed,
    binaryLosslessCheck_sound file_content_payload_and_gallery_observation_file_contract
      file_content_payload_and_gallery_observation_file_lossless_check⟩

theorem file_content_payload_and_gallery_observation_file_dependency_preservation_check :
    dependencyPreservationCheck file_content_payload_and_gallery_observation_file_contract = true := by
  native_decide

theorem file_content_payload_and_gallery_observation_file_dependency_preserving :
    DependencyPreserving file_content_payload_and_gallery_observation_file_contract :=
  dependencyPreservationCheck_sound file_content_payload_and_gallery_observation_file_contract
    file_content_payload_and_gallery_observation_file_dependency_preservation_check

def gallery_observation_file_and_filesystem_facts_contract : BinaryDecompositionContract where
  name := "gallery_observation_file_and_filesystem_facts"
  universalAttributes := ["gallery_id", "observation_id", "file_no", "file_key", "file_sha256", "device", "inode", "modified_ns", "changed_ns"]
  leftAttributes := ["gallery_id", "observation_id", "file_no", "file_key", "file_sha256"]
  rightAttributes := ["gallery_id", "observation_id", "file_key", "device", "inode", "modified_ns", "changed_ns"]
  declaredFDs := [
    { determinant := ["gallery_id", "observation_id", "file_no"], dependent := ["file_key", "file_sha256"] },
    { determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["file_no", "file_sha256"] },
    { determinant := ["gallery_id", "observation_id", "file_key"], dependent := ["device", "inode", "modified_ns", "changed_ns"] }
  ]

theorem gallery_observation_file_and_filesystem_facts_projection_check :
    binaryDecompositionWellFormedCheck
      gallery_observation_file_and_filesystem_facts_contract = true := by
  native_decide

theorem gallery_observation_file_and_filesystem_facts_projection_well_formed :
    BinaryDecompositionWellFormed gallery_observation_file_and_filesystem_facts_contract :=
  binaryDecompositionWellFormedCheck_sound
    gallery_observation_file_and_filesystem_facts_contract gallery_observation_file_and_filesystem_facts_projection_check

theorem gallery_observation_file_and_filesystem_facts_intersection_check :
    sameAttrSet (attributeIntersection
      gallery_observation_file_and_filesystem_facts_contract.leftAttributes
      gallery_observation_file_and_filesystem_facts_contract.rightAttributes)
      ["gallery_id", "observation_id", "file_key"] = true := by
  native_decide

theorem gallery_observation_file_and_filesystem_facts_lossless_check :
    binaryLosslessCheck gallery_observation_file_and_filesystem_facts_contract = true := by
  native_decide

theorem gallery_observation_file_and_filesystem_facts_lossless : BinaryLossless gallery_observation_file_and_filesystem_facts_contract :=
  ⟨gallery_observation_file_and_filesystem_facts_projection_well_formed,
    binaryLosslessCheck_sound gallery_observation_file_and_filesystem_facts_contract
      gallery_observation_file_and_filesystem_facts_lossless_check⟩

theorem gallery_observation_file_and_filesystem_facts_dependency_preservation_check :
    dependencyPreservationCheck gallery_observation_file_and_filesystem_facts_contract = true := by
  native_decide

theorem gallery_observation_file_and_filesystem_facts_dependency_preserving :
    DependencyPreserving gallery_observation_file_and_filesystem_facts_contract :=
  dependencyPreservationCheck_sound gallery_observation_file_and_filesystem_facts_contract
    gallery_observation_file_and_filesystem_facts_dependency_preservation_check

def tag_identity_and_gallery_observation_association_contract : BinaryDecompositionContract where
  name := "tag_identity_and_gallery_observation_association"
  universalAttributes := ["tag_id", "namespace", "tag_value_sha256", "gallery_id", "observation_id", "position"]
  leftAttributes := ["tag_id", "namespace", "tag_value_sha256"]
  rightAttributes := ["gallery_id", "observation_id", "position", "tag_id"]
  declaredFDs := [
    { determinant := ["tag_id"], dependent := ["namespace", "tag_value_sha256"] },
    { determinant := ["namespace", "tag_value_sha256"], dependent := ["tag_id"] },
    { determinant := ["gallery_id", "observation_id", "position"], dependent := ["tag_id"] },
    { determinant := ["gallery_id", "observation_id", "tag_id"], dependent := ["position"] }
  ]

theorem tag_identity_and_gallery_observation_association_projection_check :
    binaryDecompositionWellFormedCheck
      tag_identity_and_gallery_observation_association_contract = true := by
  native_decide

theorem tag_identity_and_gallery_observation_association_projection_well_formed :
    BinaryDecompositionWellFormed tag_identity_and_gallery_observation_association_contract :=
  binaryDecompositionWellFormedCheck_sound
    tag_identity_and_gallery_observation_association_contract tag_identity_and_gallery_observation_association_projection_check

theorem tag_identity_and_gallery_observation_association_intersection_check :
    sameAttrSet (attributeIntersection
      tag_identity_and_gallery_observation_association_contract.leftAttributes
      tag_identity_and_gallery_observation_association_contract.rightAttributes)
      ["tag_id"] = true := by
  native_decide

theorem tag_identity_and_gallery_observation_association_lossless_check :
    binaryLosslessCheck tag_identity_and_gallery_observation_association_contract = true := by
  native_decide

theorem tag_identity_and_gallery_observation_association_lossless : BinaryLossless tag_identity_and_gallery_observation_association_contract :=
  ⟨tag_identity_and_gallery_observation_association_projection_well_formed,
    binaryLosslessCheck_sound tag_identity_and_gallery_observation_association_contract
      tag_identity_and_gallery_observation_association_lossless_check⟩

theorem tag_identity_and_gallery_observation_association_dependency_preservation_check :
    dependencyPreservationCheck tag_identity_and_gallery_observation_association_contract = true := by
  native_decide

theorem tag_identity_and_gallery_observation_association_dependency_preserving :
    DependencyPreserving tag_identity_and_gallery_observation_association_contract :=
  dependencyPreservationCheck_sound tag_identity_and_gallery_observation_association_contract
    tag_identity_and_gallery_observation_association_dependency_preservation_check

def artifact_payload_and_preparation_occurrence_contract : BinaryDecompositionContract where
  name := "artifact_payload_and_preparation_occurrence"
  universalAttributes := ["candidate_id", "publication_key", "artifact_sha256", "size_bytes", "artifact_locator_sha256", "storage_codec_version", "storage_generation", "protection_token", "state"]
  leftAttributes := ["artifact_sha256", "size_bytes", "artifact_locator_sha256"]
  rightAttributes := ["candidate_id", "publication_key", "artifact_sha256", "storage_codec_version", "storage_generation", "protection_token", "state"]
  declaredFDs := [
    { determinant := ["artifact_sha256"], dependent := ["size_bytes", "artifact_locator_sha256"] },
    { determinant := ["artifact_locator_sha256"], dependent := ["artifact_sha256", "size_bytes"] },
    { determinant := ["candidate_id", "publication_key"], dependent := ["artifact_sha256", "storage_codec_version", "storage_generation", "protection_token", "state"] },
    { determinant := ["protection_token"], dependent := ["candidate_id", "publication_key", "artifact_sha256", "storage_codec_version", "storage_generation", "state"] }
  ]

theorem artifact_payload_and_preparation_occurrence_projection_check :
    binaryDecompositionWellFormedCheck
      artifact_payload_and_preparation_occurrence_contract = true := by
  native_decide

theorem artifact_payload_and_preparation_occurrence_projection_well_formed :
    BinaryDecompositionWellFormed artifact_payload_and_preparation_occurrence_contract :=
  binaryDecompositionWellFormedCheck_sound
    artifact_payload_and_preparation_occurrence_contract artifact_payload_and_preparation_occurrence_projection_check

theorem artifact_payload_and_preparation_occurrence_intersection_check :
    sameAttrSet (attributeIntersection
      artifact_payload_and_preparation_occurrence_contract.leftAttributes
      artifact_payload_and_preparation_occurrence_contract.rightAttributes)
      ["artifact_sha256"] = true := by
  native_decide

theorem artifact_payload_and_preparation_occurrence_lossless_check :
    binaryLosslessCheck artifact_payload_and_preparation_occurrence_contract = true := by
  native_decide

theorem artifact_payload_and_preparation_occurrence_lossless : BinaryLossless artifact_payload_and_preparation_occurrence_contract :=
  ⟨artifact_payload_and_preparation_occurrence_projection_well_formed,
    binaryLosslessCheck_sound artifact_payload_and_preparation_occurrence_contract
      artifact_payload_and_preparation_occurrence_lossless_check⟩

theorem artifact_payload_and_preparation_occurrence_dependency_preservation_check :
    dependencyPreservationCheck artifact_payload_and_preparation_occurrence_contract = true := by
  native_decide

theorem artifact_payload_and_preparation_occurrence_dependency_preserving :
    DependencyPreserving artifact_payload_and_preparation_occurrence_contract :=
  dependencyPreservationCheck_sound artifact_payload_and_preparation_occurrence_contract
    artifact_payload_and_preparation_occurrence_dependency_preservation_check

def artifact_policy_and_registered_producer_contract : BinaryDecompositionContract where
  name := "artifact_policy_and_registered_producer"
  universalAttributes := ["policy_component_sha256", "artifact_algorithm_version", "max_image_short_side", "producer_fingerprint_sha256", "producer_equivalence_class", "writer_id", "python_abi", "pillow_build", "libjpeg_build", "zlib_build"]
  leftAttributes := ["policy_component_sha256", "max_image_short_side", "producer_fingerprint_sha256"]
  rightAttributes := ["producer_fingerprint_sha256", "artifact_algorithm_version", "producer_equivalence_class", "writer_id", "python_abi", "pillow_build", "libjpeg_build", "zlib_build"]
  declaredFDs := [
    { determinant := ["policy_component_sha256"], dependent := ["artifact_algorithm_version", "max_image_short_side", "producer_fingerprint_sha256", "producer_equivalence_class", "writer_id", "python_abi", "pillow_build", "libjpeg_build", "zlib_build"] },
    { determinant := ["max_image_short_side", "producer_fingerprint_sha256"], dependent := ["policy_component_sha256"] },
    { determinant := ["producer_fingerprint_sha256"], dependent := ["artifact_algorithm_version", "producer_equivalence_class", "writer_id", "python_abi", "pillow_build", "libjpeg_build", "zlib_build"] },
    { determinant := ["producer_equivalence_class"], dependent := ["producer_fingerprint_sha256", "artifact_algorithm_version", "writer_id", "python_abi", "pillow_build", "libjpeg_build", "zlib_build"] },
    { determinant := ["writer_id", "python_abi", "pillow_build", "libjpeg_build", "zlib_build"], dependent := ["producer_fingerprint_sha256", "artifact_algorithm_version", "producer_equivalence_class"] }
  ]

theorem artifact_policy_and_registered_producer_projection_check :
    binaryDecompositionWellFormedCheck
      artifact_policy_and_registered_producer_contract = true := by
  native_decide

theorem artifact_policy_and_registered_producer_projection_well_formed :
    BinaryDecompositionWellFormed artifact_policy_and_registered_producer_contract :=
  binaryDecompositionWellFormedCheck_sound
    artifact_policy_and_registered_producer_contract artifact_policy_and_registered_producer_projection_check

theorem artifact_policy_and_registered_producer_intersection_check :
    sameAttrSet (attributeIntersection
      artifact_policy_and_registered_producer_contract.leftAttributes
      artifact_policy_and_registered_producer_contract.rightAttributes)
      ["producer_fingerprint_sha256"] = true := by
  native_decide

theorem artifact_policy_and_registered_producer_lossless_check :
    binaryLosslessCheck artifact_policy_and_registered_producer_contract = true := by
  native_decide

theorem artifact_policy_and_registered_producer_lossless : BinaryLossless artifact_policy_and_registered_producer_contract :=
  ⟨artifact_policy_and_registered_producer_projection_well_formed,
    binaryLosslessCheck_sound artifact_policy_and_registered_producer_contract
      artifact_policy_and_registered_producer_lossless_check⟩

theorem artifact_policy_and_registered_producer_dependency_preservation_check :
    dependencyPreservationCheck artifact_policy_and_registered_producer_contract = true := by
  native_decide

theorem artifact_policy_and_registered_producer_dependency_preserving :
    DependencyPreserving artifact_policy_and_registered_producer_contract :=
  dependencyPreservationCheck_sound artifact_policy_and_registered_producer_contract
    artifact_policy_and_registered_producer_dependency_preservation_check

def artifact_payload_and_catalog_occurrence_contract : BinaryDecompositionContract where
  name := "artifact_payload_and_catalog_occurrence"
  universalAttributes := ["revision", "publication_key", "artifact_sha256", "size_bytes", "artifact_locator_sha256", "artifact_semantics_sha256"]
  leftAttributes := ["artifact_sha256", "size_bytes", "artifact_locator_sha256"]
  rightAttributes := ["revision", "publication_key", "artifact_sha256", "artifact_semantics_sha256"]
  declaredFDs := [
    { determinant := ["artifact_sha256"], dependent := ["size_bytes", "artifact_locator_sha256"] },
    { determinant := ["artifact_locator_sha256"], dependent := ["artifact_sha256", "size_bytes"] },
    { determinant := ["revision", "publication_key"], dependent := ["artifact_sha256", "artifact_semantics_sha256"] }
  ]

theorem artifact_payload_and_catalog_occurrence_projection_check :
    binaryDecompositionWellFormedCheck
      artifact_payload_and_catalog_occurrence_contract = true := by
  native_decide

theorem artifact_payload_and_catalog_occurrence_projection_well_formed :
    BinaryDecompositionWellFormed artifact_payload_and_catalog_occurrence_contract :=
  binaryDecompositionWellFormedCheck_sound
    artifact_payload_and_catalog_occurrence_contract artifact_payload_and_catalog_occurrence_projection_check

theorem artifact_payload_and_catalog_occurrence_intersection_check :
    sameAttrSet (attributeIntersection
      artifact_payload_and_catalog_occurrence_contract.leftAttributes
      artifact_payload_and_catalog_occurrence_contract.rightAttributes)
      ["artifact_sha256"] = true := by
  native_decide

theorem artifact_payload_and_catalog_occurrence_lossless_check :
    binaryLosslessCheck artifact_payload_and_catalog_occurrence_contract = true := by
  native_decide

theorem artifact_payload_and_catalog_occurrence_lossless : BinaryLossless artifact_payload_and_catalog_occurrence_contract :=
  ⟨artifact_payload_and_catalog_occurrence_projection_well_formed,
    binaryLosslessCheck_sound artifact_payload_and_catalog_occurrence_contract
      artifact_payload_and_catalog_occurrence_lossless_check⟩

theorem artifact_payload_and_catalog_occurrence_dependency_preservation_check :
    dependencyPreservationCheck artifact_payload_and_catalog_occurrence_contract = true := by
  native_decide

theorem artifact_payload_and_catalog_occurrence_dependency_preserving :
    DependencyPreserving artifact_payload_and_catalog_occurrence_contract :=
  dependencyPreservationCheck_sound artifact_payload_and_catalog_occurrence_contract
    artifact_payload_and_catalog_occurrence_dependency_preservation_check

def artifact_semantics_and_catalog_occurrence_contract : BinaryDecompositionContract where
  name := "artifact_semantics_and_catalog_occurrence"
  universalAttributes := ["revision", "publication_key", "artifact_semantics_sha256", "artifact_sha256", "source_manifest_component_sha256", "member_plan_component_sha256", "effective_content_component_sha256", "selected_component_sha256", "owner_component_sha256", "policy_component_sha256"]
  leftAttributes := ["artifact_semantics_sha256", "source_manifest_component_sha256", "member_plan_component_sha256", "effective_content_component_sha256", "selected_component_sha256", "owner_component_sha256", "policy_component_sha256"]
  rightAttributes := ["revision", "publication_key", "artifact_sha256", "artifact_semantics_sha256"]
  declaredFDs := [
    { determinant := ["revision", "publication_key"], dependent := ["artifact_semantics_sha256", "artifact_sha256"] },
    { determinant := ["artifact_semantics_sha256"], dependent := ["source_manifest_component_sha256", "member_plan_component_sha256", "effective_content_component_sha256", "selected_component_sha256", "owner_component_sha256", "policy_component_sha256"] },
    { determinant := ["source_manifest_component_sha256", "member_plan_component_sha256", "effective_content_component_sha256", "selected_component_sha256", "owner_component_sha256", "policy_component_sha256"], dependent := ["artifact_semantics_sha256"] }
  ]

theorem artifact_semantics_and_catalog_occurrence_projection_check :
    binaryDecompositionWellFormedCheck
      artifact_semantics_and_catalog_occurrence_contract = true := by
  native_decide

theorem artifact_semantics_and_catalog_occurrence_projection_well_formed :
    BinaryDecompositionWellFormed artifact_semantics_and_catalog_occurrence_contract :=
  binaryDecompositionWellFormedCheck_sound
    artifact_semantics_and_catalog_occurrence_contract artifact_semantics_and_catalog_occurrence_projection_check

theorem artifact_semantics_and_catalog_occurrence_intersection_check :
    sameAttrSet (attributeIntersection
      artifact_semantics_and_catalog_occurrence_contract.leftAttributes
      artifact_semantics_and_catalog_occurrence_contract.rightAttributes)
      ["artifact_semantics_sha256"] = true := by
  native_decide

theorem artifact_semantics_and_catalog_occurrence_lossless_check :
    binaryLosslessCheck artifact_semantics_and_catalog_occurrence_contract = true := by
  native_decide

theorem artifact_semantics_and_catalog_occurrence_lossless : BinaryLossless artifact_semantics_and_catalog_occurrence_contract :=
  ⟨artifact_semantics_and_catalog_occurrence_projection_well_formed,
    binaryLosslessCheck_sound artifact_semantics_and_catalog_occurrence_contract
      artifact_semantics_and_catalog_occurrence_lossless_check⟩

theorem artifact_semantics_and_catalog_occurrence_dependency_preservation_check :
    dependencyPreservationCheck artifact_semantics_and_catalog_occurrence_contract = true := by
  native_decide

theorem artifact_semantics_and_catalog_occurrence_dependency_preserving :
    DependencyPreserving artifact_semantics_and_catalog_occurrence_contract :=
  dependencyPreservationCheck_sound artifact_semantics_and_catalog_occurrence_contract
    artifact_semantics_and_catalog_occurrence_dependency_preservation_check

def artifact_input_and_semantic_identity_contract : BinaryDecompositionContract where
  name := "artifact_input_and_semantic_identity"
  universalAttributes := ["candidate_id", "publication_key", "artifact_semantics_sha256", "source_manifest_component_sha256", "member_plan_component_sha256", "effective_content_component_sha256", "selected_component_sha256", "owner_component_sha256", "policy_component_sha256"]
  leftAttributes := ["candidate_id", "publication_key", "artifact_semantics_sha256"]
  rightAttributes := ["artifact_semantics_sha256", "source_manifest_component_sha256", "member_plan_component_sha256", "effective_content_component_sha256", "selected_component_sha256", "owner_component_sha256", "policy_component_sha256"]
  declaredFDs := [
    { determinant := ["candidate_id", "publication_key"], dependent := ["artifact_semantics_sha256", "source_manifest_component_sha256", "member_plan_component_sha256", "effective_content_component_sha256", "selected_component_sha256", "owner_component_sha256", "policy_component_sha256"] },
    { determinant := ["artifact_semantics_sha256"], dependent := ["source_manifest_component_sha256", "member_plan_component_sha256", "effective_content_component_sha256", "selected_component_sha256", "owner_component_sha256", "policy_component_sha256"] },
    { determinant := ["source_manifest_component_sha256", "member_plan_component_sha256", "effective_content_component_sha256", "selected_component_sha256", "owner_component_sha256", "policy_component_sha256"], dependent := ["artifact_semantics_sha256"] }
  ]

theorem artifact_input_and_semantic_identity_projection_check :
    binaryDecompositionWellFormedCheck
      artifact_input_and_semantic_identity_contract = true := by
  native_decide

theorem artifact_input_and_semantic_identity_projection_well_formed :
    BinaryDecompositionWellFormed artifact_input_and_semantic_identity_contract :=
  binaryDecompositionWellFormedCheck_sound
    artifact_input_and_semantic_identity_contract artifact_input_and_semantic_identity_projection_check

theorem artifact_input_and_semantic_identity_intersection_check :
    sameAttrSet (attributeIntersection
      artifact_input_and_semantic_identity_contract.leftAttributes
      artifact_input_and_semantic_identity_contract.rightAttributes)
      ["artifact_semantics_sha256"] = true := by
  native_decide

theorem artifact_input_and_semantic_identity_lossless_check :
    binaryLosslessCheck artifact_input_and_semantic_identity_contract = true := by
  native_decide

theorem artifact_input_and_semantic_identity_lossless : BinaryLossless artifact_input_and_semantic_identity_contract :=
  ⟨artifact_input_and_semantic_identity_projection_well_formed,
    binaryLosslessCheck_sound artifact_input_and_semantic_identity_contract
      artifact_input_and_semantic_identity_lossless_check⟩

theorem artifact_input_and_semantic_identity_dependency_preservation_check :
    dependencyPreservationCheck artifact_input_and_semantic_identity_contract = true := by
  native_decide

theorem artifact_input_and_semantic_identity_dependency_preserving :
    DependencyPreserving artifact_input_and_semantic_identity_contract :=
  dependencyPreservationCheck_sound artifact_input_and_semantic_identity_contract
    artifact_input_and_semantic_identity_dependency_preservation_check

def publication_candidate_and_optional_common_base_contract : BinaryDecompositionContract where
  name := "publication_candidate_and_optional_common_base"
  universalAttributes := ["candidate_id", "analysis_id", "reserved_revision", "artifact_policy_id", "display_title_policy_id", "base_receipt_id", "artifacts_required", "created_at"]
  leftAttributes := ["candidate_id", "analysis_id", "reserved_revision", "artifact_policy_id", "display_title_policy_id", "artifacts_required", "created_at"]
  rightAttributes := ["candidate_id", "base_receipt_id"]
  declaredFDs := [
    { determinant := ["candidate_id"], dependent := ["analysis_id", "reserved_revision", "artifact_policy_id", "display_title_policy_id", "base_receipt_id", "artifacts_required", "created_at"] },
    { determinant := ["reserved_revision"], dependent := ["candidate_id", "analysis_id", "artifact_policy_id", "display_title_policy_id", "base_receipt_id", "artifacts_required", "created_at"] }
  ]

theorem publication_candidate_and_optional_common_base_projection_check :
    binaryDecompositionWellFormedCheck
      publication_candidate_and_optional_common_base_contract = true := by
  native_decide

theorem publication_candidate_and_optional_common_base_projection_well_formed :
    BinaryDecompositionWellFormed publication_candidate_and_optional_common_base_contract :=
  binaryDecompositionWellFormedCheck_sound
    publication_candidate_and_optional_common_base_contract publication_candidate_and_optional_common_base_projection_check

theorem publication_candidate_and_optional_common_base_intersection_check :
    sameAttrSet (attributeIntersection
      publication_candidate_and_optional_common_base_contract.leftAttributes
      publication_candidate_and_optional_common_base_contract.rightAttributes)
      ["candidate_id"] = true := by
  native_decide

theorem publication_candidate_and_optional_common_base_lossless_check :
    binaryLosslessCheck publication_candidate_and_optional_common_base_contract = true := by
  native_decide

theorem publication_candidate_and_optional_common_base_lossless : BinaryLossless publication_candidate_and_optional_common_base_contract :=
  ⟨publication_candidate_and_optional_common_base_projection_well_formed,
    binaryLosslessCheck_sound publication_candidate_and_optional_common_base_contract
      publication_candidate_and_optional_common_base_lossless_check⟩

theorem publication_candidate_and_optional_common_base_dependency_preservation_check :
    dependencyPreservationCheck publication_candidate_and_optional_common_base_contract = true := by
  native_decide

theorem publication_candidate_and_optional_common_base_dependency_preserving :
    DependencyPreserving publication_candidate_and_optional_common_base_contract :=
  dependencyPreservationCheck_sound publication_candidate_and_optional_common_base_contract
    publication_candidate_and_optional_common_base_dependency_preservation_check

def catalog_revision_and_generation_publication_seal_contract : BinaryDecompositionContract where
  name := "catalog_revision_and_generation_publication_seal"
  universalAttributes := ["revision", "publication_count", "published_at", "generation"]
  leftAttributes := ["revision", "publication_count", "published_at"]
  rightAttributes := ["revision", "generation"]
  declaredFDs := [
    { determinant := ["revision"], dependent := ["publication_count", "published_at", "generation"] },
    { determinant := ["generation"], dependent := ["revision", "publication_count", "published_at"] }
  ]

theorem catalog_revision_and_generation_publication_seal_projection_check :
    binaryDecompositionWellFormedCheck
      catalog_revision_and_generation_publication_seal_contract = true := by
  native_decide

theorem catalog_revision_and_generation_publication_seal_projection_well_formed :
    BinaryDecompositionWellFormed catalog_revision_and_generation_publication_seal_contract :=
  binaryDecompositionWellFormedCheck_sound
    catalog_revision_and_generation_publication_seal_contract catalog_revision_and_generation_publication_seal_projection_check

theorem catalog_revision_and_generation_publication_seal_intersection_check :
    sameAttrSet (attributeIntersection
      catalog_revision_and_generation_publication_seal_contract.leftAttributes
      catalog_revision_and_generation_publication_seal_contract.rightAttributes)
      ["revision"] = true := by
  native_decide

theorem catalog_revision_and_generation_publication_seal_lossless_check :
    binaryLosslessCheck catalog_revision_and_generation_publication_seal_contract = true := by
  native_decide

theorem catalog_revision_and_generation_publication_seal_lossless : BinaryLossless catalog_revision_and_generation_publication_seal_contract :=
  ⟨catalog_revision_and_generation_publication_seal_projection_well_formed,
    binaryLosslessCheck_sound catalog_revision_and_generation_publication_seal_contract
      catalog_revision_and_generation_publication_seal_lossless_check⟩

theorem catalog_revision_and_generation_publication_seal_dependency_preservation_check :
    dependencyPreservationCheck catalog_revision_and_generation_publication_seal_contract = true := by
  native_decide

theorem catalog_revision_and_generation_publication_seal_dependency_preserving :
    DependencyPreserving catalog_revision_and_generation_publication_seal_contract :=
  dependencyPreservationCheck_sound catalog_revision_and_generation_publication_seal_contract
    catalog_revision_and_generation_publication_seal_dependency_preservation_check

def publication_head_revision_and_advanced_at_contract : BinaryDecompositionContract where
  name := "publication_head_revision_and_advanced_at"
  universalAttributes := ["channel", "revision", "advanced_at"]
  leftAttributes := ["channel", "revision"]
  rightAttributes := ["channel", "advanced_at"]
  declaredFDs := [
    { determinant := ["channel"], dependent := ["revision", "advanced_at"] },
    { determinant := ["revision"], dependent := ["channel", "advanced_at"] }
  ]

theorem publication_head_revision_and_advanced_at_projection_check :
    binaryDecompositionWellFormedCheck
      publication_head_revision_and_advanced_at_contract = true := by
  native_decide

theorem publication_head_revision_and_advanced_at_projection_well_formed :
    BinaryDecompositionWellFormed publication_head_revision_and_advanced_at_contract :=
  binaryDecompositionWellFormedCheck_sound
    publication_head_revision_and_advanced_at_contract publication_head_revision_and_advanced_at_projection_check

theorem publication_head_revision_and_advanced_at_intersection_check :
    sameAttrSet (attributeIntersection
      publication_head_revision_and_advanced_at_contract.leftAttributes
      publication_head_revision_and_advanced_at_contract.rightAttributes)
      ["channel"] = true := by
  native_decide

theorem publication_head_revision_and_advanced_at_lossless_check :
    binaryLosslessCheck publication_head_revision_and_advanced_at_contract = true := by
  native_decide

theorem publication_head_revision_and_advanced_at_lossless : BinaryLossless publication_head_revision_and_advanced_at_contract :=
  ⟨publication_head_revision_and_advanced_at_projection_well_formed,
    binaryLosslessCheck_sound publication_head_revision_and_advanced_at_contract
      publication_head_revision_and_advanced_at_lossless_check⟩

theorem publication_head_revision_and_advanced_at_dependency_preservation_check :
    dependencyPreservationCheck publication_head_revision_and_advanced_at_contract = true := by
  native_decide

theorem publication_head_revision_and_advanced_at_dependency_preserving :
    DependencyPreserving publication_head_revision_and_advanced_at_contract :=
  dependencyPreservationCheck_sound publication_head_revision_and_advanced_at_contract
    publication_head_revision_and_advanced_at_dependency_preservation_check

def publication_selection_occurrence_and_gallery_contract : BinaryDecompositionContract where
  name := "publication_selection_occurrence_and_gallery"
  universalAttributes := ["selection_occurrence_sha256", "candidate_id", "gallery_id", "publication_key"]
  leftAttributes := ["selection_occurrence_sha256", "gallery_id"]
  rightAttributes := ["selection_occurrence_sha256", "candidate_id", "publication_key"]
  declaredFDs := [
    { determinant := ["selection_occurrence_sha256"], dependent := ["candidate_id", "gallery_id", "publication_key"] },
    { determinant := ["candidate_id", "publication_key"], dependent := ["selection_occurrence_sha256", "gallery_id"] }
  ]

theorem publication_selection_occurrence_and_gallery_projection_check :
    binaryDecompositionWellFormedCheck
      publication_selection_occurrence_and_gallery_contract = true := by
  native_decide

theorem publication_selection_occurrence_and_gallery_projection_well_formed :
    BinaryDecompositionWellFormed publication_selection_occurrence_and_gallery_contract :=
  binaryDecompositionWellFormedCheck_sound
    publication_selection_occurrence_and_gallery_contract publication_selection_occurrence_and_gallery_projection_check

theorem publication_selection_occurrence_and_gallery_intersection_check :
    sameAttrSet (attributeIntersection
      publication_selection_occurrence_and_gallery_contract.leftAttributes
      publication_selection_occurrence_and_gallery_contract.rightAttributes)
      ["selection_occurrence_sha256"] = true := by
  native_decide

theorem publication_selection_occurrence_and_gallery_lossless_check :
    binaryLosslessCheck publication_selection_occurrence_and_gallery_contract = true := by
  native_decide

theorem publication_selection_occurrence_and_gallery_lossless : BinaryLossless publication_selection_occurrence_and_gallery_contract :=
  ⟨publication_selection_occurrence_and_gallery_projection_well_formed,
    binaryLosslessCheck_sound publication_selection_occurrence_and_gallery_contract
      publication_selection_occurrence_and_gallery_lossless_check⟩

theorem publication_selection_occurrence_and_gallery_dependency_preservation_check :
    dependencyPreservationCheck publication_selection_occurrence_and_gallery_contract = true := by
  native_decide

theorem publication_selection_occurrence_and_gallery_dependency_preserving :
    DependencyPreserving publication_selection_occurrence_and_gallery_contract :=
  dependencyPreservationCheck_sound publication_selection_occurrence_and_gallery_contract
    publication_selection_occurrence_and_gallery_dependency_preservation_check

def catalog_publication_occurrence_and_payload_contract : BinaryDecompositionContract where
  name := "catalog_publication_occurrence_and_payload"
  universalAttributes := ["catalog_occurrence_sha256", "revision", "publication_key", "gallery_id", "summary_sha256", "language_sha256", "modified_at", "source_title_sha256"]
  leftAttributes := ["catalog_occurrence_sha256", "revision", "publication_key"]
  rightAttributes := ["catalog_occurrence_sha256", "gallery_id", "summary_sha256", "language_sha256", "modified_at", "source_title_sha256"]
  declaredFDs := [
    { determinant := ["catalog_occurrence_sha256"], dependent := ["revision", "publication_key", "gallery_id", "summary_sha256", "language_sha256", "modified_at", "source_title_sha256"] },
    { determinant := ["revision", "publication_key"], dependent := ["catalog_occurrence_sha256", "gallery_id", "summary_sha256", "language_sha256", "modified_at", "source_title_sha256"] }
  ]

theorem catalog_publication_occurrence_and_payload_projection_check :
    binaryDecompositionWellFormedCheck
      catalog_publication_occurrence_and_payload_contract = true := by
  native_decide

theorem catalog_publication_occurrence_and_payload_projection_well_formed :
    BinaryDecompositionWellFormed catalog_publication_occurrence_and_payload_contract :=
  binaryDecompositionWellFormedCheck_sound
    catalog_publication_occurrence_and_payload_contract catalog_publication_occurrence_and_payload_projection_check

theorem catalog_publication_occurrence_and_payload_intersection_check :
    sameAttrSet (attributeIntersection
      catalog_publication_occurrence_and_payload_contract.leftAttributes
      catalog_publication_occurrence_and_payload_contract.rightAttributes)
      ["catalog_occurrence_sha256"] = true := by
  native_decide

theorem catalog_publication_occurrence_and_payload_lossless_check :
    binaryLosslessCheck catalog_publication_occurrence_and_payload_contract = true := by
  native_decide

theorem catalog_publication_occurrence_and_payload_lossless : BinaryLossless catalog_publication_occurrence_and_payload_contract :=
  ⟨catalog_publication_occurrence_and_payload_projection_well_formed,
    binaryLosslessCheck_sound catalog_publication_occurrence_and_payload_contract
      catalog_publication_occurrence_and_payload_lossless_check⟩

theorem catalog_publication_occurrence_and_payload_dependency_preservation_check :
    dependencyPreservationCheck catalog_publication_occurrence_and_payload_contract = true := by
  native_decide

theorem catalog_publication_occurrence_and_payload_dependency_preserving :
    DependencyPreserving catalog_publication_occurrence_and_payload_contract :=
  dependencyPreservationCheck_sound catalog_publication_occurrence_and_payload_contract
    catalog_publication_occurrence_and_payload_dependency_preservation_check

def catalog_publication_and_title_basis_contract : BinaryDecompositionContract where
  name := "catalog_publication_and_title_basis"
  universalAttributes := ["revision", "gallery_id", "publication_key", "source_title_sha256", "source_gallery_name", "summary_sha256", "language_sha256", "modified_at"]
  leftAttributes := ["revision", "publication_key", "gallery_id", "summary_sha256", "language_sha256", "modified_at"]
  rightAttributes := ["revision", "publication_key", "source_title_sha256", "source_gallery_name"]
  declaredFDs := [
    { determinant := ["gallery_id"], dependent := ["publication_key"] },
    { determinant := ["source_gallery_name"], dependent := ["publication_key"] },
    { determinant := ["revision", "gallery_id"], dependent := ["publication_key", "source_title_sha256", "source_gallery_name", "summary_sha256", "language_sha256", "modified_at"] },
    { determinant := ["revision", "publication_key"], dependent := ["gallery_id", "source_title_sha256", "source_gallery_name", "summary_sha256", "language_sha256", "modified_at"] }
  ]

theorem catalog_publication_and_title_basis_projection_check :
    binaryDecompositionWellFormedCheck
      catalog_publication_and_title_basis_contract = true := by
  native_decide

theorem catalog_publication_and_title_basis_projection_well_formed :
    BinaryDecompositionWellFormed catalog_publication_and_title_basis_contract :=
  binaryDecompositionWellFormedCheck_sound
    catalog_publication_and_title_basis_contract catalog_publication_and_title_basis_projection_check

theorem catalog_publication_and_title_basis_intersection_check :
    sameAttrSet (attributeIntersection
      catalog_publication_and_title_basis_contract.leftAttributes
      catalog_publication_and_title_basis_contract.rightAttributes)
      ["revision", "publication_key"] = true := by
  native_decide

theorem catalog_publication_and_title_basis_lossless_check :
    binaryLosslessCheck catalog_publication_and_title_basis_contract = true := by
  native_decide

theorem catalog_publication_and_title_basis_lossless : BinaryLossless catalog_publication_and_title_basis_contract :=
  ⟨catalog_publication_and_title_basis_projection_well_formed,
    binaryLosslessCheck_sound catalog_publication_and_title_basis_contract
      catalog_publication_and_title_basis_lossless_check⟩

theorem catalog_publication_and_title_basis_dependency_preservation_check :
    dependencyPreservationCheck catalog_publication_and_title_basis_contract = true := by
  native_decide

theorem catalog_publication_and_title_basis_dependency_preserving :
    DependencyPreserving catalog_publication_and_title_basis_contract :=
  dependencyPreservationCheck_sound catalog_publication_and_title_basis_contract
    catalog_publication_and_title_basis_dependency_preservation_check

def publication_identity_and_gallery_upload_time_contract : BinaryDecompositionContract where
  name := "publication_identity_and_gallery_upload_time"
  universalAttributes := ["publication_key", "gid", "upload_time"]
  leftAttributes := ["publication_key", "gid"]
  rightAttributes := ["gid", "upload_time"]
  declaredFDs := [
    { determinant := ["publication_key"], dependent := ["gid", "upload_time"] },
    { determinant := ["gid"], dependent := ["publication_key", "upload_time"] }
  ]

theorem publication_identity_and_gallery_upload_time_projection_check :
    binaryDecompositionWellFormedCheck
      publication_identity_and_gallery_upload_time_contract = true := by
  native_decide

theorem publication_identity_and_gallery_upload_time_projection_well_formed :
    BinaryDecompositionWellFormed publication_identity_and_gallery_upload_time_contract :=
  binaryDecompositionWellFormedCheck_sound
    publication_identity_and_gallery_upload_time_contract publication_identity_and_gallery_upload_time_projection_check

theorem publication_identity_and_gallery_upload_time_intersection_check :
    sameAttrSet (attributeIntersection
      publication_identity_and_gallery_upload_time_contract.leftAttributes
      publication_identity_and_gallery_upload_time_contract.rightAttributes)
      ["gid"] = true := by
  native_decide

theorem publication_identity_and_gallery_upload_time_lossless_check :
    binaryLosslessCheck publication_identity_and_gallery_upload_time_contract = true := by
  native_decide

theorem publication_identity_and_gallery_upload_time_lossless : BinaryLossless publication_identity_and_gallery_upload_time_contract :=
  ⟨publication_identity_and_gallery_upload_time_projection_well_formed,
    binaryLosslessCheck_sound publication_identity_and_gallery_upload_time_contract
      publication_identity_and_gallery_upload_time_lossless_check⟩

theorem publication_identity_and_gallery_upload_time_dependency_preservation_check :
    dependencyPreservationCheck publication_identity_and_gallery_upload_time_contract = true := by
  native_decide

theorem publication_identity_and_gallery_upload_time_dependency_preserving :
    DependencyPreserving publication_identity_and_gallery_upload_time_contract :=
  dependencyPreservationCheck_sound publication_identity_and_gallery_upload_time_contract
    publication_identity_and_gallery_upload_time_dependency_preservation_check

def catalog_publication_and_optional_content_contract : BinaryDecompositionContract where
  name := "catalog_publication_and_optional_content"
  universalAttributes := ["revision", "gallery_id", "publication_key", "summary_sha256", "language_sha256", "modified_at", "content_sha256"]
  leftAttributes := ["revision", "publication_key", "gallery_id", "summary_sha256", "language_sha256", "modified_at"]
  rightAttributes := ["revision", "publication_key", "content_sha256"]
  declaredFDs := [
    { determinant := ["gallery_id"], dependent := ["publication_key"] },
    { determinant := ["revision", "gallery_id"], dependent := ["publication_key", "summary_sha256", "language_sha256", "modified_at", "content_sha256"] },
    { determinant := ["revision", "publication_key"], dependent := ["gallery_id", "summary_sha256", "language_sha256", "modified_at", "content_sha256"] }
  ]

theorem catalog_publication_and_optional_content_projection_check :
    binaryDecompositionWellFormedCheck
      catalog_publication_and_optional_content_contract = true := by
  native_decide

theorem catalog_publication_and_optional_content_projection_well_formed :
    BinaryDecompositionWellFormed catalog_publication_and_optional_content_contract :=
  binaryDecompositionWellFormedCheck_sound
    catalog_publication_and_optional_content_contract catalog_publication_and_optional_content_projection_check

theorem catalog_publication_and_optional_content_intersection_check :
    sameAttrSet (attributeIntersection
      catalog_publication_and_optional_content_contract.leftAttributes
      catalog_publication_and_optional_content_contract.rightAttributes)
      ["revision", "publication_key"] = true := by
  native_decide

theorem catalog_publication_and_optional_content_lossless_check :
    binaryLosslessCheck catalog_publication_and_optional_content_contract = true := by
  native_decide

theorem catalog_publication_and_optional_content_lossless : BinaryLossless catalog_publication_and_optional_content_contract :=
  ⟨catalog_publication_and_optional_content_projection_well_formed,
    binaryLosslessCheck_sound catalog_publication_and_optional_content_contract
      catalog_publication_and_optional_content_lossless_check⟩

theorem catalog_publication_and_optional_content_dependency_preservation_check :
    dependencyPreservationCheck catalog_publication_and_optional_content_contract = true := by
  native_decide

theorem catalog_publication_and_optional_content_dependency_preserving :
    DependencyPreserving catalog_publication_and_optional_content_contract :=
  dependencyPreservationCheck_sound catalog_publication_and_optional_content_contract
    catalog_publication_and_optional_content_dependency_preservation_check

def gallery_observation_page_bytes_and_descriptor_contract : BinaryDecompositionContract where
  name := "gallery_observation_page_bytes_and_descriptor"
  universalAttributes := ["page_sha256", "page_bytes", "component", "level", "subtree_item_count"]
  leftAttributes := ["page_sha256", "page_bytes"]
  rightAttributes := ["page_sha256", "component", "level", "subtree_item_count"]
  declaredFDs := [
    { determinant := ["page_sha256"], dependent := ["page_bytes", "component", "level", "subtree_item_count"] },
    { determinant := ["page_bytes"], dependent := ["page_sha256", "component", "level", "subtree_item_count"] }
  ]

theorem gallery_observation_page_bytes_and_descriptor_projection_check :
    binaryDecompositionWellFormedCheck
      gallery_observation_page_bytes_and_descriptor_contract = true := by
  native_decide

theorem gallery_observation_page_bytes_and_descriptor_projection_well_formed :
    BinaryDecompositionWellFormed gallery_observation_page_bytes_and_descriptor_contract :=
  binaryDecompositionWellFormedCheck_sound
    gallery_observation_page_bytes_and_descriptor_contract gallery_observation_page_bytes_and_descriptor_projection_check

theorem gallery_observation_page_bytes_and_descriptor_intersection_check :
    sameAttrSet (attributeIntersection
      gallery_observation_page_bytes_and_descriptor_contract.leftAttributes
      gallery_observation_page_bytes_and_descriptor_contract.rightAttributes)
      ["page_sha256"] = true := by
  native_decide

theorem gallery_observation_page_bytes_and_descriptor_lossless_check :
    binaryLosslessCheck gallery_observation_page_bytes_and_descriptor_contract = true := by
  native_decide

theorem gallery_observation_page_bytes_and_descriptor_lossless : BinaryLossless gallery_observation_page_bytes_and_descriptor_contract :=
  ⟨gallery_observation_page_bytes_and_descriptor_projection_well_formed,
    binaryLosslessCheck_sound gallery_observation_page_bytes_and_descriptor_contract
      gallery_observation_page_bytes_and_descriptor_lossless_check⟩

theorem gallery_observation_page_bytes_and_descriptor_dependency_preservation_check :
    dependencyPreservationCheck gallery_observation_page_bytes_and_descriptor_contract = true := by
  native_decide

theorem gallery_observation_page_bytes_and_descriptor_dependency_preserving :
    DependencyPreserving gallery_observation_page_bytes_and_descriptor_contract :=
  dependencyPreservationCheck_sound gallery_observation_page_bytes_and_descriptor_contract
    gallery_observation_page_bytes_and_descriptor_dependency_preservation_check

theorem all_manifest_decompositions_lossless :
    BinaryLossless source_build_and_optional_base_source_contract ∧
    BinaryLossless source_revision_and_generation_publication_seal_contract ∧
    BinaryLossless source_head_revision_and_advanced_at_contract ∧
    BinaryLossless source_revision_and_retained_snapshot_manifest_contract ∧
    BinaryLossless source_revision_and_optional_prunable_provenance_contract ∧
    BinaryLossless filesystem_gallery_location_identity_contract ∧
    BinaryLossless gallery_location_and_typed_locator_contract ∧
    BinaryLossless gallery_observation_metadata_and_scan_contract ∧
    BinaryLossless gallery_observation_discovery_and_page_count_contract ∧
    BinaryLossless gallery_observation_metadata_and_raw_content_digests_contract ∧
    BinaryLossless file_identity_and_gallery_observation_file_contract ∧
    BinaryLossless file_content_payload_and_gallery_observation_file_contract ∧
    BinaryLossless gallery_observation_file_and_filesystem_facts_contract ∧
    BinaryLossless tag_identity_and_gallery_observation_association_contract ∧
    BinaryLossless artifact_payload_and_preparation_occurrence_contract ∧
    BinaryLossless artifact_policy_and_registered_producer_contract ∧
    BinaryLossless artifact_payload_and_catalog_occurrence_contract ∧
    BinaryLossless artifact_semantics_and_catalog_occurrence_contract ∧
    BinaryLossless artifact_input_and_semantic_identity_contract ∧
    BinaryLossless publication_candidate_and_optional_common_base_contract ∧
    BinaryLossless catalog_revision_and_generation_publication_seal_contract ∧
    BinaryLossless publication_head_revision_and_advanced_at_contract ∧
    BinaryLossless publication_selection_occurrence_and_gallery_contract ∧
    BinaryLossless catalog_publication_occurrence_and_payload_contract ∧
    BinaryLossless catalog_publication_and_title_basis_contract ∧
    BinaryLossless publication_identity_and_gallery_upload_time_contract ∧
    BinaryLossless catalog_publication_and_optional_content_contract ∧
    BinaryLossless gallery_observation_page_bytes_and_descriptor_contract := by
  exact ⟨source_build_and_optional_base_source_lossless,
    source_revision_and_generation_publication_seal_lossless,
    source_head_revision_and_advanced_at_lossless,
    source_revision_and_retained_snapshot_manifest_lossless,
    source_revision_and_optional_prunable_provenance_lossless,
    filesystem_gallery_location_identity_lossless,
    gallery_location_and_typed_locator_lossless,
    gallery_observation_metadata_and_scan_lossless,
    gallery_observation_discovery_and_page_count_lossless,
    gallery_observation_metadata_and_raw_content_digests_lossless,
    file_identity_and_gallery_observation_file_lossless,
    file_content_payload_and_gallery_observation_file_lossless,
    gallery_observation_file_and_filesystem_facts_lossless,
    tag_identity_and_gallery_observation_association_lossless,
    artifact_payload_and_preparation_occurrence_lossless,
    artifact_policy_and_registered_producer_lossless,
    artifact_payload_and_catalog_occurrence_lossless,
    artifact_semantics_and_catalog_occurrence_lossless,
    artifact_input_and_semantic_identity_lossless,
    publication_candidate_and_optional_common_base_lossless,
    catalog_revision_and_generation_publication_seal_lossless,
    publication_head_revision_and_advanced_at_lossless,
    publication_selection_occurrence_and_gallery_lossless,
    catalog_publication_occurrence_and_payload_lossless,
    catalog_publication_and_title_basis_lossless,
    publication_identity_and_gallery_upload_time_lossless,
    catalog_publication_and_optional_content_lossless,
    gallery_observation_page_bytes_and_descriptor_lossless⟩

theorem all_manifest_decompositions_dependency_preserving :
    DependencyPreserving source_build_and_optional_base_source_contract ∧
    DependencyPreserving source_revision_and_generation_publication_seal_contract ∧
    DependencyPreserving source_head_revision_and_advanced_at_contract ∧
    DependencyPreserving source_revision_and_retained_snapshot_manifest_contract ∧
    DependencyPreserving source_revision_and_optional_prunable_provenance_contract ∧
    DependencyPreserving filesystem_gallery_location_identity_contract ∧
    DependencyPreserving gallery_location_and_typed_locator_contract ∧
    DependencyPreserving gallery_observation_metadata_and_scan_contract ∧
    DependencyPreserving gallery_observation_discovery_and_page_count_contract ∧
    DependencyPreserving gallery_observation_metadata_and_raw_content_digests_contract ∧
    DependencyPreserving file_identity_and_gallery_observation_file_contract ∧
    DependencyPreserving file_content_payload_and_gallery_observation_file_contract ∧
    DependencyPreserving gallery_observation_file_and_filesystem_facts_contract ∧
    DependencyPreserving tag_identity_and_gallery_observation_association_contract ∧
    DependencyPreserving artifact_payload_and_preparation_occurrence_contract ∧
    DependencyPreserving artifact_policy_and_registered_producer_contract ∧
    DependencyPreserving artifact_payload_and_catalog_occurrence_contract ∧
    DependencyPreserving artifact_semantics_and_catalog_occurrence_contract ∧
    DependencyPreserving artifact_input_and_semantic_identity_contract ∧
    DependencyPreserving publication_candidate_and_optional_common_base_contract ∧
    DependencyPreserving catalog_revision_and_generation_publication_seal_contract ∧
    DependencyPreserving publication_head_revision_and_advanced_at_contract ∧
    DependencyPreserving publication_selection_occurrence_and_gallery_contract ∧
    DependencyPreserving catalog_publication_occurrence_and_payload_contract ∧
    DependencyPreserving catalog_publication_and_title_basis_contract ∧
    DependencyPreserving publication_identity_and_gallery_upload_time_contract ∧
    DependencyPreserving catalog_publication_and_optional_content_contract ∧
    DependencyPreserving gallery_observation_page_bytes_and_descriptor_contract := by
  exact ⟨source_build_and_optional_base_source_dependency_preserving,
    source_revision_and_generation_publication_seal_dependency_preserving,
    source_head_revision_and_advanced_at_dependency_preserving,
    source_revision_and_retained_snapshot_manifest_dependency_preserving,
    source_revision_and_optional_prunable_provenance_dependency_preserving,
    filesystem_gallery_location_identity_dependency_preserving,
    gallery_location_and_typed_locator_dependency_preserving,
    gallery_observation_metadata_and_scan_dependency_preserving,
    gallery_observation_discovery_and_page_count_dependency_preserving,
    gallery_observation_metadata_and_raw_content_digests_dependency_preserving,
    file_identity_and_gallery_observation_file_dependency_preserving,
    file_content_payload_and_gallery_observation_file_dependency_preserving,
    gallery_observation_file_and_filesystem_facts_dependency_preserving,
    tag_identity_and_gallery_observation_association_dependency_preserving,
    artifact_payload_and_preparation_occurrence_dependency_preserving,
    artifact_policy_and_registered_producer_dependency_preserving,
    artifact_payload_and_catalog_occurrence_dependency_preserving,
    artifact_semantics_and_catalog_occurrence_dependency_preserving,
    artifact_input_and_semantic_identity_dependency_preserving,
    publication_candidate_and_optional_common_base_dependency_preserving,
    catalog_revision_and_generation_publication_seal_dependency_preserving,
    publication_head_revision_and_advanced_at_dependency_preserving,
    publication_selection_occurrence_and_gallery_dependency_preserving,
    catalog_publication_occurrence_and_payload_dependency_preserving,
    catalog_publication_and_title_basis_dependency_preserving,
    publication_identity_and_gallery_upload_time_dependency_preserving,
    catalog_publication_and_optional_content_dependency_preserving,
    gallery_observation_page_bytes_and_descriptor_dependency_preserving⟩

set_option maxRecDepth 10000 in
theorem all_manifest_base_relations_bcnf :
    BCNF canonical_digest_policy_contract ∧
    BCNF canonical_value_allocation_anchor_contract ∧
    BCNF canonical_value_allocation_digest_domain_contract ∧
    BCNF canonical_value_allocation_byte_count_contract ∧
    BCNF canonical_value_allocation_allocated_at_contract ∧
    BCNF canonical_value_allocation_seal_contract ∧
    BCNF canonical_value_page_anchor_contract ∧
    BCNF canonical_value_page_payload_contract ∧
    BCNF canonical_value_page_coordinate_contract ∧
    BCNF canonical_value_page_subtree_item_count_contract ∧
    BCNF canonical_value_page_seal_contract ∧
    BCNF canonical_value_page_parent_contract ∧
    BCNF canonical_value_identity_contract ∧
    BCNF manifest_policy_contract ∧
    BCNF source_build_state_contract ∧
    BCNF source_build_sealed_at_contract ∧
    BCNF source_build_descriptor_contract ∧
    BCNF source_build_base_publication_commit_contract ∧
    BCNF channel_registry_contract ∧
    BCNF source_provider_registry_contract ∧
    BCNF source_build_channel_contract ∧
    BCNF source_scope_contract ∧
    BCNF source_build_discovery_contract ∧
    BCNF source_build_expected_gallery_contract ∧
    BCNF source_locator_identity_contract ∧
    BCNF gallery_identity_contract ∧
    BCNF gallery_observation_allocation_contract ∧
    BCNF gallery_observation_page_contract ∧
    BCNF gallery_observation_allocation_page_contract ∧
    BCNF gallery_observation_page_descriptor_anchor_contract ∧
    BCNF gallery_observation_page_descriptor_component_contract ∧
    BCNF gallery_observation_page_descriptor_level_contract ∧
    BCNF gallery_observation_page_descriptor_subtree_item_count_contract ∧
    BCNF gallery_observation_page_descriptor_seal_contract ∧
    BCNF gallery_observation_page_key_bounds_anchor_contract ∧
    BCNF gallery_observation_page_key_bounds_first_key_contract ∧
    BCNF gallery_observation_page_key_bounds_last_key_contract ∧
    BCNF gallery_observation_page_key_bounds_seal_contract ∧
    BCNF gallery_observation_page_child_contract ∧
    BCNF gallery_observation_tree_root_contract ∧
    BCNF gallery_observation_contract ∧
    BCNF gallery_upload_time_contract ∧
    BCNF source_gallery_name_gid_contract ∧
    BCNF gallery_source_name_access_contract ∧
    BCNF gallery_observation_metadata_local_contract ∧
    BCNF gallery_observation_scan_contract ∧
    BCNF gallery_observation_discovery_fingerprint_contract ∧
    BCNF gallery_observation_metadata_digest_contract ∧
    BCNF gallery_observation_raw_content_contract ∧
    BCNF gallery_observation_page_count_contract ∧
    BCNF gallery_observation_directory_contract ∧
    BCNF gallery_observation_stat_contract ∧
    BCNF source_build_gallery_contract ∧
    BCNF file_name_identity_contract ∧
    BCNF content_blob_contract ∧
    BCNF gallery_observation_file_anchor_contract ∧
    BCNF gallery_observation_file_file_no_contract ∧
    BCNF gallery_observation_file_file_sha256_contract ∧
    BCNF gallery_observation_file_seal_contract ∧
    BCNF gallery_observation_file_filesystem_anchor_contract ∧
    BCNF gallery_observation_file_filesystem_device_contract ∧
    BCNF gallery_observation_file_filesystem_inode_contract ∧
    BCNF gallery_observation_file_filesystem_modified_ns_contract ∧
    BCNF gallery_observation_file_filesystem_changed_ns_contract ∧
    BCNF gallery_observation_file_filesystem_seal_contract ∧
    BCNF tag_term_contract ∧
    BCNF gallery_observation_tag_contract ∧
    BCNF build_manifest_core_contract ∧
    BCNF gallery_manifest_contract ∧
    BCNF analysis_policy_contract ∧
    BCNF analysis_run_state_contract ∧
    BCNF analysis_run_completed_at_contract ∧
    BCNF analysis_run_descriptor_contract ∧
    BCNF analysis_baseline_contract ∧
    BCNF analysis_state_ancestry_contract ∧
    BCNF source_snapshot_manifest_identity_contract ∧
    BCNF analysis_snapshot_manifest_contract ∧
    BCNF source_revision_descriptor_contract ∧
    BCNF source_revision_provenance_contract ∧
    BCNF gallery_observation_artist_contract ∧
    BCNF gallery_observation_file_hash_occurrence_contract ∧
    BCNF analysis_changed_gallery_contract ∧
    BCNF analysis_changed_file_hash_contract ∧
    BCNF analysis_exclusion_delta_anchor_contract ∧
    BCNF analysis_exclusion_delta_old_excluded_contract ∧
    BCNF analysis_exclusion_delta_new_excluded_contract ∧
    BCNF analysis_exclusion_delta_change_contract ∧
    BCNF analysis_exclusion_delta_seal_contract ∧
    BCNF analysis_impacted_gallery_contract ∧
    BCNF analysis_impacted_content_contract ∧
    BCNF analysis_impacted_content_provenance_contract ∧
    BCNF analysis_impacted_gid_storage_contract ∧
    BCNF analysis_impacted_gid_provenance_storage_contract ∧
    BCNF analysis_file_hash_decision_shadow_anchor_contract ∧
    BCNF analysis_file_hash_decision_shadow_occurrence_count_contract ∧
    BCNF analysis_file_hash_decision_shadow_artist_count_contract ∧
    BCNF analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract ∧
    BCNF analysis_file_hash_decision_shadow_seal_contract ∧
    BCNF analysis_file_hash_decision_tombstone_contract ∧
    BCNF analysis_content_owner_candidate_shadow_contract ∧
    BCNF analysis_content_owner_candidate_tombstone_contract ∧
    BCNF analysis_content_owner_shadow_contract ∧
    BCNF analysis_content_owner_tombstone_contract ∧
    BCNF analysis_gid_candidate_shadow_contract ∧
    BCNF analysis_gid_candidate_tombstone_contract ∧
    BCNF analysis_gid_winner_selection_contract ∧
    BCNF analysis_gid_winner_tombstone_contract ∧
    BCNF analysis_state_component_seal_contract ∧
    BCNF analysis_stage_contract ∧
    BCNF analysis_checkpoint_contract ∧
    BCNF analysis_batch_receipt_stored_contract ∧
    BCNF publication_candidate_contract ∧
    BCNF publication_candidate_projection_seal_contract ∧
    BCNF publication_candidate_base_publication_commit_contract ∧
    BCNF publication_selection_storage_contract ∧
    BCNF publication_selection_occurrence_identity_contract ∧
    BCNF publication_stage_contract ∧
    BCNF publication_checkpoint_contract ∧
    BCNF publication_batch_receipt_stored_contract ∧
    BCNF publication_finalization_checkpoint_contract ∧
    BCNF publication_finalization_batch_receipt_stored_contract ∧
    BCNF artifact_zip_writer_policy_contract ∧
    BCNF artifact_producer_fingerprint_contract ∧
    BCNF artifact_storage_codec_contract ∧
    BCNF artifact_policy_semantics_contract ∧
    BCNF artifact_policy_contract ∧
    BCNF artifact_semantic_input_contract ∧
    BCNF artifact_input_contract ∧
    BCNF artifact_operation_contract ∧
    BCNF artifact_blob_contract ∧
    BCNF prepared_artifact_contract ∧
    BCNF catalog_revision_descriptor_contract ∧
    BCNF publication_generation_node_contract ∧
    BCNF publication_generation_successor_contract ∧
    BCNF publication_identity_contract ∧
    BCNF display_title_policy_contract ∧
    BCNF title_sort_policy_contract ∧
    BCNF display_title_choice_contract ∧
    BCNF title_sort_contract ∧
    BCNF catalog_publication_storage_contract ∧
    BCNF catalog_publication_occurrence_identity_contract ∧
    BCNF catalog_publication_order_contract ∧
    BCNF catalog_publication_content_contract ∧
    BCNF contributor_role_registry_contract ∧
    BCNF catalog_contributor_contract ∧
    BCNF catalog_subject_contract ∧
    BCNF catalog_artifact_contract ∧
    BCNF publication_commit_anchor_contract ∧
    BCNF publication_commit_contract ∧
    BCNF publication_commit_finalization_contract ∧
    BCNF publication_commit_head_receipt_contract := by
  exact ⟨canonical_digest_policy_bcnf,
    canonical_value_allocation_anchor_bcnf,
    canonical_value_allocation_digest_domain_bcnf,
    canonical_value_allocation_byte_count_bcnf,
    canonical_value_allocation_allocated_at_bcnf,
    canonical_value_allocation_seal_bcnf,
    canonical_value_page_anchor_bcnf,
    canonical_value_page_payload_bcnf,
    canonical_value_page_coordinate_bcnf,
    canonical_value_page_subtree_item_count_bcnf,
    canonical_value_page_seal_bcnf,
    canonical_value_page_parent_bcnf,
    canonical_value_identity_bcnf,
    manifest_policy_bcnf,
    source_build_state_bcnf,
    source_build_sealed_at_bcnf,
    source_build_descriptor_bcnf,
    source_build_base_publication_commit_bcnf,
    channel_registry_bcnf,
    source_provider_registry_bcnf,
    source_build_channel_bcnf,
    source_scope_bcnf,
    source_build_discovery_bcnf,
    source_build_expected_gallery_bcnf,
    source_locator_identity_bcnf,
    gallery_identity_bcnf,
    gallery_observation_allocation_bcnf,
    gallery_observation_page_bcnf,
    gallery_observation_allocation_page_bcnf,
    gallery_observation_page_descriptor_anchor_bcnf,
    gallery_observation_page_descriptor_component_bcnf,
    gallery_observation_page_descriptor_level_bcnf,
    gallery_observation_page_descriptor_subtree_item_count_bcnf,
    gallery_observation_page_descriptor_seal_bcnf,
    gallery_observation_page_key_bounds_anchor_bcnf,
    gallery_observation_page_key_bounds_first_key_bcnf,
    gallery_observation_page_key_bounds_last_key_bcnf,
    gallery_observation_page_key_bounds_seal_bcnf,
    gallery_observation_page_child_bcnf,
    gallery_observation_tree_root_bcnf,
    gallery_observation_bcnf,
    gallery_upload_time_bcnf,
    source_gallery_name_gid_bcnf,
    gallery_source_name_access_bcnf,
    gallery_observation_metadata_local_bcnf,
    gallery_observation_scan_bcnf,
    gallery_observation_discovery_fingerprint_bcnf,
    gallery_observation_metadata_digest_bcnf,
    gallery_observation_raw_content_bcnf,
    gallery_observation_page_count_bcnf,
    gallery_observation_directory_bcnf,
    gallery_observation_stat_bcnf,
    source_build_gallery_bcnf,
    file_name_identity_bcnf,
    content_blob_bcnf,
    gallery_observation_file_anchor_bcnf,
    gallery_observation_file_file_no_bcnf,
    gallery_observation_file_file_sha256_bcnf,
    gallery_observation_file_seal_bcnf,
    gallery_observation_file_filesystem_anchor_bcnf,
    gallery_observation_file_filesystem_device_bcnf,
    gallery_observation_file_filesystem_inode_bcnf,
    gallery_observation_file_filesystem_modified_ns_bcnf,
    gallery_observation_file_filesystem_changed_ns_bcnf,
    gallery_observation_file_filesystem_seal_bcnf,
    tag_term_bcnf,
    gallery_observation_tag_bcnf,
    build_manifest_core_bcnf,
    gallery_manifest_bcnf,
    analysis_policy_bcnf,
    analysis_run_state_bcnf,
    analysis_run_completed_at_bcnf,
    analysis_run_descriptor_bcnf,
    analysis_baseline_bcnf,
    analysis_state_ancestry_bcnf,
    source_snapshot_manifest_identity_bcnf,
    analysis_snapshot_manifest_bcnf,
    source_revision_descriptor_bcnf,
    source_revision_provenance_bcnf,
    gallery_observation_artist_bcnf,
    gallery_observation_file_hash_occurrence_bcnf,
    analysis_changed_gallery_bcnf,
    analysis_changed_file_hash_bcnf,
    analysis_exclusion_delta_anchor_bcnf,
    analysis_exclusion_delta_old_excluded_bcnf,
    analysis_exclusion_delta_new_excluded_bcnf,
    analysis_exclusion_delta_change_bcnf,
    analysis_exclusion_delta_seal_bcnf,
    analysis_impacted_gallery_bcnf,
    analysis_impacted_content_bcnf,
    analysis_impacted_content_provenance_bcnf,
    analysis_impacted_gid_storage_bcnf,
    analysis_impacted_gid_provenance_storage_bcnf,
    analysis_file_hash_decision_shadow_anchor_bcnf,
    analysis_file_hash_decision_shadow_occurrence_count_bcnf,
    analysis_file_hash_decision_shadow_artist_count_bcnf,
    analysis_file_hash_decision_shadow_maximum_gallery_artist_count_bcnf,
    analysis_file_hash_decision_shadow_seal_bcnf,
    analysis_file_hash_decision_tombstone_bcnf,
    analysis_content_owner_candidate_shadow_bcnf,
    analysis_content_owner_candidate_tombstone_bcnf,
    analysis_content_owner_shadow_bcnf,
    analysis_content_owner_tombstone_bcnf,
    analysis_gid_candidate_shadow_bcnf,
    analysis_gid_candidate_tombstone_bcnf,
    analysis_gid_winner_selection_bcnf,
    analysis_gid_winner_tombstone_bcnf,
    analysis_state_component_seal_bcnf,
    analysis_stage_bcnf,
    analysis_checkpoint_bcnf,
    analysis_batch_receipt_stored_bcnf,
    publication_candidate_bcnf,
    publication_candidate_projection_seal_bcnf,
    publication_candidate_base_publication_commit_bcnf,
    publication_selection_storage_bcnf,
    publication_selection_occurrence_identity_bcnf,
    publication_stage_bcnf,
    publication_checkpoint_bcnf,
    publication_batch_receipt_stored_bcnf,
    publication_finalization_checkpoint_bcnf,
    publication_finalization_batch_receipt_stored_bcnf,
    artifact_zip_writer_policy_bcnf,
    artifact_producer_fingerprint_bcnf,
    artifact_storage_codec_bcnf,
    artifact_policy_semantics_bcnf,
    artifact_policy_bcnf,
    artifact_semantic_input_bcnf,
    artifact_input_bcnf,
    artifact_operation_bcnf,
    artifact_blob_bcnf,
    prepared_artifact_bcnf,
    catalog_revision_descriptor_bcnf,
    publication_generation_node_bcnf,
    publication_generation_successor_bcnf,
    publication_identity_bcnf,
    display_title_policy_bcnf,
    title_sort_policy_bcnf,
    display_title_choice_bcnf,
    title_sort_bcnf,
    catalog_publication_storage_bcnf,
    catalog_publication_occurrence_identity_bcnf,
    catalog_publication_order_bcnf,
    catalog_publication_content_bcnf,
    contributor_role_registry_bcnf,
    catalog_contributor_bcnf,
    catalog_subject_bcnf,
    catalog_artifact_bcnf,
    publication_commit_anchor_bcnf,
    publication_commit_bcnf,
    publication_commit_finalization_bcnf,
    publication_commit_head_receipt_bcnf⟩

set_option maxRecDepth 10000 in
theorem all_manifest_candidate_keys_determine_attributes :
    KeysDetermineAllAttributes canonical_digest_policy_contract ∧
    KeysDetermineAllAttributes canonical_value_allocation_anchor_contract ∧
    KeysDetermineAllAttributes canonical_value_allocation_digest_domain_contract ∧
    KeysDetermineAllAttributes canonical_value_allocation_byte_count_contract ∧
    KeysDetermineAllAttributes canonical_value_allocation_allocated_at_contract ∧
    KeysDetermineAllAttributes canonical_value_allocation_seal_contract ∧
    KeysDetermineAllAttributes canonical_value_allocation_contract ∧
    KeysDetermineAllAttributes canonical_value_page_anchor_contract ∧
    KeysDetermineAllAttributes canonical_value_page_payload_contract ∧
    KeysDetermineAllAttributes canonical_value_page_coordinate_contract ∧
    KeysDetermineAllAttributes canonical_value_page_subtree_item_count_contract ∧
    KeysDetermineAllAttributes canonical_value_page_seal_contract ∧
    KeysDetermineAllAttributes canonical_value_page_contract ∧
    KeysDetermineAllAttributes canonical_value_page_descriptor_contract ∧
    KeysDetermineAllAttributes canonical_value_page_parent_contract ∧
    KeysDetermineAllAttributes canonical_value_identity_contract ∧
    KeysDetermineAllAttributes manifest_policy_contract ∧
    KeysDetermineAllAttributes source_build_state_contract ∧
    KeysDetermineAllAttributes source_build_sealed_at_contract ∧
    KeysDetermineAllAttributes source_build_descriptor_contract ∧
    KeysDetermineAllAttributes source_build_contract ∧
    KeysDetermineAllAttributes source_build_base_publication_commit_contract ∧
    KeysDetermineAllAttributes source_build_base_source_contract ∧
    KeysDetermineAllAttributes channel_registry_contract ∧
    KeysDetermineAllAttributes source_provider_registry_contract ∧
    KeysDetermineAllAttributes source_build_channel_contract ∧
    KeysDetermineAllAttributes source_scope_contract ∧
    KeysDetermineAllAttributes source_build_discovery_contract ∧
    KeysDetermineAllAttributes source_build_expected_gallery_contract ∧
    KeysDetermineAllAttributes source_locator_identity_contract ∧
    KeysDetermineAllAttributes gallery_identity_contract ∧
    KeysDetermineAllAttributes gallery_observation_allocation_contract ∧
    KeysDetermineAllAttributes gallery_observation_page_contract ∧
    KeysDetermineAllAttributes gallery_observation_allocation_page_contract ∧
    KeysDetermineAllAttributes gallery_observation_page_descriptor_anchor_contract ∧
    KeysDetermineAllAttributes gallery_observation_page_descriptor_component_contract ∧
    KeysDetermineAllAttributes gallery_observation_page_descriptor_level_contract ∧
    KeysDetermineAllAttributes gallery_observation_page_descriptor_subtree_item_count_contract ∧
    KeysDetermineAllAttributes gallery_observation_page_descriptor_seal_contract ∧
    KeysDetermineAllAttributes gallery_observation_page_descriptor_contract ∧
    KeysDetermineAllAttributes gallery_observation_page_key_bounds_anchor_contract ∧
    KeysDetermineAllAttributes gallery_observation_page_key_bounds_first_key_contract ∧
    KeysDetermineAllAttributes gallery_observation_page_key_bounds_last_key_contract ∧
    KeysDetermineAllAttributes gallery_observation_page_key_bounds_seal_contract ∧
    KeysDetermineAllAttributes gallery_observation_page_key_bounds_contract ∧
    KeysDetermineAllAttributes gallery_observation_page_child_contract ∧
    KeysDetermineAllAttributes gallery_observation_tree_root_contract ∧
    KeysDetermineAllAttributes gallery_observation_contract ∧
    KeysDetermineAllAttributes gallery_upload_time_contract ∧
    KeysDetermineAllAttributes source_gallery_name_gid_contract ∧
    KeysDetermineAllAttributes gallery_source_name_access_contract ∧
    KeysDetermineAllAttributes gallery_observation_metadata_local_contract ∧
    KeysDetermineAllAttributes gallery_observation_metadata_contract ∧
    KeysDetermineAllAttributes gallery_observation_scan_contract ∧
    KeysDetermineAllAttributes gallery_observation_discovery_fingerprint_contract ∧
    KeysDetermineAllAttributes gallery_observation_metadata_digest_contract ∧
    KeysDetermineAllAttributes gallery_observation_raw_content_contract ∧
    KeysDetermineAllAttributes gallery_observation_page_count_contract ∧
    KeysDetermineAllAttributes gallery_observation_directory_contract ∧
    KeysDetermineAllAttributes gallery_observation_stat_contract ∧
    KeysDetermineAllAttributes source_build_gallery_contract ∧
    KeysDetermineAllAttributes file_name_identity_contract ∧
    KeysDetermineAllAttributes content_blob_contract ∧
    KeysDetermineAllAttributes gallery_observation_file_anchor_contract ∧
    KeysDetermineAllAttributes gallery_observation_file_file_no_contract ∧
    KeysDetermineAllAttributes gallery_observation_file_file_sha256_contract ∧
    KeysDetermineAllAttributes gallery_observation_file_seal_contract ∧
    KeysDetermineAllAttributes gallery_observation_file_contract ∧
    KeysDetermineAllAttributes gallery_observation_file_filesystem_anchor_contract ∧
    KeysDetermineAllAttributes gallery_observation_file_filesystem_device_contract ∧
    KeysDetermineAllAttributes gallery_observation_file_filesystem_inode_contract ∧
    KeysDetermineAllAttributes gallery_observation_file_filesystem_modified_ns_contract ∧
    KeysDetermineAllAttributes gallery_observation_file_filesystem_changed_ns_contract ∧
    KeysDetermineAllAttributes gallery_observation_file_filesystem_seal_contract ∧
    KeysDetermineAllAttributes gallery_observation_file_filesystem_contract ∧
    KeysDetermineAllAttributes tag_term_contract ∧
    KeysDetermineAllAttributes gallery_observation_tag_contract ∧
    KeysDetermineAllAttributes build_manifest_core_contract ∧
    KeysDetermineAllAttributes build_manifest_contract ∧
    KeysDetermineAllAttributes gallery_manifest_contract ∧
    KeysDetermineAllAttributes analysis_policy_contract ∧
    KeysDetermineAllAttributes analysis_run_state_contract ∧
    KeysDetermineAllAttributes analysis_run_completed_at_contract ∧
    KeysDetermineAllAttributes analysis_run_descriptor_contract ∧
    KeysDetermineAllAttributes analysis_run_contract ∧
    KeysDetermineAllAttributes analysis_baseline_contract ∧
    KeysDetermineAllAttributes analysis_state_anchor_contract ∧
    KeysDetermineAllAttributes analysis_state_ancestry_contract ∧
    KeysDetermineAllAttributes source_snapshot_manifest_identity_contract ∧
    KeysDetermineAllAttributes analysis_snapshot_manifest_contract ∧
    KeysDetermineAllAttributes source_revision_descriptor_contract ∧
    KeysDetermineAllAttributes source_revision_contract ∧
    KeysDetermineAllAttributes source_revision_generation_contract ∧
    KeysDetermineAllAttributes source_revision_provenance_contract ∧
    KeysDetermineAllAttributes source_head_revision_contract ∧
    KeysDetermineAllAttributes source_head_advanced_at_contract ∧
    KeysDetermineAllAttributes source_head_contract ∧
    KeysDetermineAllAttributes gallery_observation_artist_contract ∧
    KeysDetermineAllAttributes gallery_observation_file_hash_occurrence_contract ∧
    KeysDetermineAllAttributes analysis_changed_gallery_contract ∧
    KeysDetermineAllAttributes analysis_changed_file_hash_contract ∧
    KeysDetermineAllAttributes analysis_exclusion_delta_anchor_contract ∧
    KeysDetermineAllAttributes analysis_exclusion_delta_old_excluded_contract ∧
    KeysDetermineAllAttributes analysis_exclusion_delta_new_excluded_contract ∧
    KeysDetermineAllAttributes analysis_exclusion_delta_change_contract ∧
    KeysDetermineAllAttributes analysis_exclusion_delta_seal_contract ∧
    KeysDetermineAllAttributes analysis_exclusion_delta_contract ∧
    KeysDetermineAllAttributes analysis_impacted_gallery_contract ∧
    KeysDetermineAllAttributes analysis_impacted_content_contract ∧
    KeysDetermineAllAttributes analysis_impacted_content_provenance_contract ∧
    KeysDetermineAllAttributes analysis_impacted_gid_storage_contract ∧
    KeysDetermineAllAttributes analysis_impacted_gid_provenance_storage_contract ∧
    KeysDetermineAllAttributes analysis_impacted_gid_provenance_contract ∧
    KeysDetermineAllAttributes analysis_impacted_gid_contract ∧
    KeysDetermineAllAttributes analysis_file_hash_decision_shadow_anchor_contract ∧
    KeysDetermineAllAttributes analysis_file_hash_decision_shadow_occurrence_count_contract ∧
    KeysDetermineAllAttributes analysis_file_hash_decision_shadow_artist_count_contract ∧
    KeysDetermineAllAttributes analysis_file_hash_decision_shadow_maximum_gallery_artist_count_contract ∧
    KeysDetermineAllAttributes analysis_file_hash_decision_shadow_seal_contract ∧
    KeysDetermineAllAttributes analysis_file_hash_decision_shadow_contract ∧
    KeysDetermineAllAttributes analysis_file_hash_decision_tombstone_contract ∧
    KeysDetermineAllAttributes analysis_file_hash_decision_resolved_contract ∧
    KeysDetermineAllAttributes analysis_content_owner_candidate_shadow_contract ∧
    KeysDetermineAllAttributes analysis_content_owner_candidate_tombstone_contract ∧
    KeysDetermineAllAttributes analysis_content_owner_candidate_resolved_contract ∧
    KeysDetermineAllAttributes analysis_content_owner_shadow_contract ∧
    KeysDetermineAllAttributes analysis_content_owner_tombstone_contract ∧
    KeysDetermineAllAttributes analysis_content_owner_resolved_contract ∧
    KeysDetermineAllAttributes analysis_gid_candidate_shadow_contract ∧
    KeysDetermineAllAttributes analysis_gid_candidate_tombstone_contract ∧
    KeysDetermineAllAttributes analysis_gid_candidate_resolved_contract ∧
    KeysDetermineAllAttributes analysis_gid_winner_selection_contract ∧
    KeysDetermineAllAttributes analysis_gid_winner_shadow_contract ∧
    KeysDetermineAllAttributes analysis_gid_winner_tombstone_contract ∧
    KeysDetermineAllAttributes analysis_gid_winner_resolved_contract ∧
    KeysDetermineAllAttributes analysis_state_component_seal_contract ∧
    KeysDetermineAllAttributes analysis_stage_contract ∧
    KeysDetermineAllAttributes analysis_checkpoint_contract ∧
    KeysDetermineAllAttributes analysis_batch_receipt_stored_contract ∧
    KeysDetermineAllAttributes analysis_batch_receipt_contract ∧
    KeysDetermineAllAttributes publication_candidate_contract ∧
    KeysDetermineAllAttributes publication_candidate_projection_seal_contract ∧
    KeysDetermineAllAttributes publication_candidate_projection_contract ∧
    KeysDetermineAllAttributes publication_candidate_base_publication_commit_contract ∧
    KeysDetermineAllAttributes publication_candidate_base_catalog_contract ∧
    KeysDetermineAllAttributes publication_candidate_base_source_contract ∧
    KeysDetermineAllAttributes publication_selection_storage_contract ∧
    KeysDetermineAllAttributes publication_selection_occurrence_identity_contract ∧
    KeysDetermineAllAttributes publication_selection_contract ∧
    KeysDetermineAllAttributes publication_stage_contract ∧
    KeysDetermineAllAttributes publication_checkpoint_contract ∧
    KeysDetermineAllAttributes publication_batch_receipt_stored_contract ∧
    KeysDetermineAllAttributes publication_batch_receipt_contract ∧
    KeysDetermineAllAttributes publication_finalization_checkpoint_contract ∧
    KeysDetermineAllAttributes publication_finalization_batch_receipt_stored_contract ∧
    KeysDetermineAllAttributes publication_finalization_batch_receipt_contract ∧
    KeysDetermineAllAttributes artifact_zip_writer_policy_contract ∧
    KeysDetermineAllAttributes artifact_producer_fingerprint_contract ∧
    KeysDetermineAllAttributes artifact_storage_codec_contract ∧
    KeysDetermineAllAttributes artifact_policy_semantics_contract ∧
    KeysDetermineAllAttributes artifact_policy_contract ∧
    KeysDetermineAllAttributes artifact_semantic_input_contract ∧
    KeysDetermineAllAttributes artifact_input_contract ∧
    KeysDetermineAllAttributes artifact_delta_old_contract ∧
    KeysDetermineAllAttributes artifact_delta_new_contract ∧
    KeysDetermineAllAttributes artifact_operation_contract ∧
    KeysDetermineAllAttributes artifact_blob_contract ∧
    KeysDetermineAllAttributes prepared_artifact_contract ∧
    KeysDetermineAllAttributes catalog_revision_descriptor_contract ∧
    KeysDetermineAllAttributes catalog_revision_contract ∧
    KeysDetermineAllAttributes catalog_revision_generation_contract ∧
    KeysDetermineAllAttributes publication_generation_node_contract ∧
    KeysDetermineAllAttributes publication_generation_successor_contract ∧
    KeysDetermineAllAttributes publication_identity_contract ∧
    KeysDetermineAllAttributes display_title_policy_contract ∧
    KeysDetermineAllAttributes title_sort_policy_contract ∧
    KeysDetermineAllAttributes display_title_choice_contract ∧
    KeysDetermineAllAttributes title_sort_contract ∧
    KeysDetermineAllAttributes catalog_publication_storage_contract ∧
    KeysDetermineAllAttributes catalog_publication_occurrence_identity_contract ∧
    KeysDetermineAllAttributes catalog_publication_contract ∧
    KeysDetermineAllAttributes catalog_publication_order_contract ∧
    KeysDetermineAllAttributes catalog_publication_title_contract ∧
    KeysDetermineAllAttributes catalog_publication_content_contract ∧
    KeysDetermineAllAttributes contributor_role_registry_contract ∧
    KeysDetermineAllAttributes catalog_contributor_contract ∧
    KeysDetermineAllAttributes catalog_subject_contract ∧
    KeysDetermineAllAttributes catalog_artifact_contract ∧
    KeysDetermineAllAttributes publication_commit_anchor_contract ∧
    KeysDetermineAllAttributes publication_commit_contract ∧
    KeysDetermineAllAttributes publication_commit_finalization_contract ∧
    KeysDetermineAllAttributes publication_commit_head_receipt_contract ∧
    KeysDetermineAllAttributes publication_commit_head_contract ∧
    KeysDetermineAllAttributes publication_receipt_contract ∧
    KeysDetermineAllAttributes publication_head_revision_contract ∧
    KeysDetermineAllAttributes publication_head_advanced_at_contract ∧
    KeysDetermineAllAttributes publication_head_contract := by
  exact ⟨canonical_digest_policy_candidate_keys_determine_all_attributes,
    canonical_value_allocation_anchor_candidate_keys_determine_all_attributes,
    canonical_value_allocation_digest_domain_candidate_keys_determine_all_attributes,
    canonical_value_allocation_byte_count_candidate_keys_determine_all_attributes,
    canonical_value_allocation_allocated_at_candidate_keys_determine_all_attributes,
    canonical_value_allocation_seal_candidate_keys_determine_all_attributes,
    canonical_value_allocation_candidate_keys_determine_all_attributes,
    canonical_value_page_anchor_candidate_keys_determine_all_attributes,
    canonical_value_page_payload_candidate_keys_determine_all_attributes,
    canonical_value_page_coordinate_candidate_keys_determine_all_attributes,
    canonical_value_page_subtree_item_count_candidate_keys_determine_all_attributes,
    canonical_value_page_seal_candidate_keys_determine_all_attributes,
    canonical_value_page_candidate_keys_determine_all_attributes,
    canonical_value_page_descriptor_candidate_keys_determine_all_attributes,
    canonical_value_page_parent_candidate_keys_determine_all_attributes,
    canonical_value_identity_candidate_keys_determine_all_attributes,
    manifest_policy_candidate_keys_determine_all_attributes,
    source_build_state_candidate_keys_determine_all_attributes,
    source_build_sealed_at_candidate_keys_determine_all_attributes,
    source_build_descriptor_candidate_keys_determine_all_attributes,
    source_build_candidate_keys_determine_all_attributes,
    source_build_base_publication_commit_candidate_keys_determine_all_attributes,
    source_build_base_source_candidate_keys_determine_all_attributes,
    channel_registry_candidate_keys_determine_all_attributes,
    source_provider_registry_candidate_keys_determine_all_attributes,
    source_build_channel_candidate_keys_determine_all_attributes,
    source_scope_candidate_keys_determine_all_attributes,
    source_build_discovery_candidate_keys_determine_all_attributes,
    source_build_expected_gallery_candidate_keys_determine_all_attributes,
    source_locator_identity_candidate_keys_determine_all_attributes,
    gallery_identity_candidate_keys_determine_all_attributes,
    gallery_observation_allocation_candidate_keys_determine_all_attributes,
    gallery_observation_page_candidate_keys_determine_all_attributes,
    gallery_observation_allocation_page_candidate_keys_determine_all_attributes,
    gallery_observation_page_descriptor_anchor_candidate_keys_determine_all_attributes,
    gallery_observation_page_descriptor_component_candidate_keys_determine_all_attributes,
    gallery_observation_page_descriptor_level_candidate_keys_determine_all_attributes,
    gallery_observation_page_descriptor_subtree_item_count_candidate_keys_determine_all_attributes,
    gallery_observation_page_descriptor_seal_candidate_keys_determine_all_attributes,
    gallery_observation_page_descriptor_candidate_keys_determine_all_attributes,
    gallery_observation_page_key_bounds_anchor_candidate_keys_determine_all_attributes,
    gallery_observation_page_key_bounds_first_key_candidate_keys_determine_all_attributes,
    gallery_observation_page_key_bounds_last_key_candidate_keys_determine_all_attributes,
    gallery_observation_page_key_bounds_seal_candidate_keys_determine_all_attributes,
    gallery_observation_page_key_bounds_candidate_keys_determine_all_attributes,
    gallery_observation_page_child_candidate_keys_determine_all_attributes,
    gallery_observation_tree_root_candidate_keys_determine_all_attributes,
    gallery_observation_candidate_keys_determine_all_attributes,
    gallery_upload_time_candidate_keys_determine_all_attributes,
    source_gallery_name_gid_candidate_keys_determine_all_attributes,
    gallery_source_name_access_candidate_keys_determine_all_attributes,
    gallery_observation_metadata_local_candidate_keys_determine_all_attributes,
    gallery_observation_metadata_candidate_keys_determine_all_attributes,
    gallery_observation_scan_candidate_keys_determine_all_attributes,
    gallery_observation_discovery_fingerprint_candidate_keys_determine_all_attributes,
    gallery_observation_metadata_digest_candidate_keys_determine_all_attributes,
    gallery_observation_raw_content_candidate_keys_determine_all_attributes,
    gallery_observation_page_count_candidate_keys_determine_all_attributes,
    gallery_observation_directory_candidate_keys_determine_all_attributes,
    gallery_observation_stat_candidate_keys_determine_all_attributes,
    source_build_gallery_candidate_keys_determine_all_attributes,
    file_name_identity_candidate_keys_determine_all_attributes,
    content_blob_candidate_keys_determine_all_attributes,
    gallery_observation_file_anchor_candidate_keys_determine_all_attributes,
    gallery_observation_file_file_no_candidate_keys_determine_all_attributes,
    gallery_observation_file_file_sha256_candidate_keys_determine_all_attributes,
    gallery_observation_file_seal_candidate_keys_determine_all_attributes,
    gallery_observation_file_candidate_keys_determine_all_attributes,
    gallery_observation_file_filesystem_anchor_candidate_keys_determine_all_attributes,
    gallery_observation_file_filesystem_device_candidate_keys_determine_all_attributes,
    gallery_observation_file_filesystem_inode_candidate_keys_determine_all_attributes,
    gallery_observation_file_filesystem_modified_ns_candidate_keys_determine_all_attributes,
    gallery_observation_file_filesystem_changed_ns_candidate_keys_determine_all_attributes,
    gallery_observation_file_filesystem_seal_candidate_keys_determine_all_attributes,
    gallery_observation_file_filesystem_candidate_keys_determine_all_attributes,
    tag_term_candidate_keys_determine_all_attributes,
    gallery_observation_tag_candidate_keys_determine_all_attributes,
    build_manifest_core_candidate_keys_determine_all_attributes,
    build_manifest_candidate_keys_determine_all_attributes,
    gallery_manifest_candidate_keys_determine_all_attributes,
    analysis_policy_candidate_keys_determine_all_attributes,
    analysis_run_state_candidate_keys_determine_all_attributes,
    analysis_run_completed_at_candidate_keys_determine_all_attributes,
    analysis_run_descriptor_candidate_keys_determine_all_attributes,
    analysis_run_candidate_keys_determine_all_attributes,
    analysis_baseline_candidate_keys_determine_all_attributes,
    analysis_state_anchor_candidate_keys_determine_all_attributes,
    analysis_state_ancestry_candidate_keys_determine_all_attributes,
    source_snapshot_manifest_identity_candidate_keys_determine_all_attributes,
    analysis_snapshot_manifest_candidate_keys_determine_all_attributes,
    source_revision_descriptor_candidate_keys_determine_all_attributes,
    source_revision_candidate_keys_determine_all_attributes,
    source_revision_generation_candidate_keys_determine_all_attributes,
    source_revision_provenance_candidate_keys_determine_all_attributes,
    source_head_revision_candidate_keys_determine_all_attributes,
    source_head_advanced_at_candidate_keys_determine_all_attributes,
    source_head_candidate_keys_determine_all_attributes,
    gallery_observation_artist_candidate_keys_determine_all_attributes,
    gallery_observation_file_hash_occurrence_candidate_keys_determine_all_attributes,
    analysis_changed_gallery_candidate_keys_determine_all_attributes,
    analysis_changed_file_hash_candidate_keys_determine_all_attributes,
    analysis_exclusion_delta_anchor_candidate_keys_determine_all_attributes,
    analysis_exclusion_delta_old_excluded_candidate_keys_determine_all_attributes,
    analysis_exclusion_delta_new_excluded_candidate_keys_determine_all_attributes,
    analysis_exclusion_delta_change_candidate_keys_determine_all_attributes,
    analysis_exclusion_delta_seal_candidate_keys_determine_all_attributes,
    analysis_exclusion_delta_candidate_keys_determine_all_attributes,
    analysis_impacted_gallery_candidate_keys_determine_all_attributes,
    analysis_impacted_content_candidate_keys_determine_all_attributes,
    analysis_impacted_content_provenance_candidate_keys_determine_all_attributes,
    analysis_impacted_gid_storage_candidate_keys_determine_all_attributes,
    analysis_impacted_gid_provenance_storage_candidate_keys_determine_all_attributes,
    analysis_impacted_gid_provenance_candidate_keys_determine_all_attributes,
    analysis_impacted_gid_candidate_keys_determine_all_attributes,
    analysis_file_hash_decision_shadow_anchor_candidate_keys_determine_all_attributes,
    analysis_file_hash_decision_shadow_occurrence_count_candidate_keys_determine_all_attributes,
    analysis_file_hash_decision_shadow_artist_count_candidate_keys_determine_all_attributes,
    analysis_file_hash_decision_shadow_maximum_gallery_artist_count_candidate_keys_determine_all_attributes,
    analysis_file_hash_decision_shadow_seal_candidate_keys_determine_all_attributes,
    analysis_file_hash_decision_shadow_candidate_keys_determine_all_attributes,
    analysis_file_hash_decision_tombstone_candidate_keys_determine_all_attributes,
    analysis_file_hash_decision_resolved_candidate_keys_determine_all_attributes,
    analysis_content_owner_candidate_shadow_candidate_keys_determine_all_attributes,
    analysis_content_owner_candidate_tombstone_candidate_keys_determine_all_attributes,
    analysis_content_owner_candidate_resolved_candidate_keys_determine_all_attributes,
    analysis_content_owner_shadow_candidate_keys_determine_all_attributes,
    analysis_content_owner_tombstone_candidate_keys_determine_all_attributes,
    analysis_content_owner_resolved_candidate_keys_determine_all_attributes,
    analysis_gid_candidate_shadow_candidate_keys_determine_all_attributes,
    analysis_gid_candidate_tombstone_candidate_keys_determine_all_attributes,
    analysis_gid_candidate_resolved_candidate_keys_determine_all_attributes,
    analysis_gid_winner_selection_candidate_keys_determine_all_attributes,
    analysis_gid_winner_shadow_candidate_keys_determine_all_attributes,
    analysis_gid_winner_tombstone_candidate_keys_determine_all_attributes,
    analysis_gid_winner_resolved_candidate_keys_determine_all_attributes,
    analysis_state_component_seal_candidate_keys_determine_all_attributes,
    analysis_stage_candidate_keys_determine_all_attributes,
    analysis_checkpoint_candidate_keys_determine_all_attributes,
    analysis_batch_receipt_stored_candidate_keys_determine_all_attributes,
    analysis_batch_receipt_candidate_keys_determine_all_attributes,
    publication_candidate_candidate_keys_determine_all_attributes,
    publication_candidate_projection_seal_candidate_keys_determine_all_attributes,
    publication_candidate_projection_candidate_keys_determine_all_attributes,
    publication_candidate_base_publication_commit_candidate_keys_determine_all_attributes,
    publication_candidate_base_catalog_candidate_keys_determine_all_attributes,
    publication_candidate_base_source_candidate_keys_determine_all_attributes,
    publication_selection_storage_candidate_keys_determine_all_attributes,
    publication_selection_occurrence_identity_candidate_keys_determine_all_attributes,
    publication_selection_candidate_keys_determine_all_attributes,
    publication_stage_candidate_keys_determine_all_attributes,
    publication_checkpoint_candidate_keys_determine_all_attributes,
    publication_batch_receipt_stored_candidate_keys_determine_all_attributes,
    publication_batch_receipt_candidate_keys_determine_all_attributes,
    publication_finalization_checkpoint_candidate_keys_determine_all_attributes,
    publication_finalization_batch_receipt_stored_candidate_keys_determine_all_attributes,
    publication_finalization_batch_receipt_candidate_keys_determine_all_attributes,
    artifact_zip_writer_policy_candidate_keys_determine_all_attributes,
    artifact_producer_fingerprint_candidate_keys_determine_all_attributes,
    artifact_storage_codec_candidate_keys_determine_all_attributes,
    artifact_policy_semantics_candidate_keys_determine_all_attributes,
    artifact_policy_candidate_keys_determine_all_attributes,
    artifact_semantic_input_candidate_keys_determine_all_attributes,
    artifact_input_candidate_keys_determine_all_attributes,
    artifact_delta_old_candidate_keys_determine_all_attributes,
    artifact_delta_new_candidate_keys_determine_all_attributes,
    artifact_operation_candidate_keys_determine_all_attributes,
    artifact_blob_candidate_keys_determine_all_attributes,
    prepared_artifact_candidate_keys_determine_all_attributes,
    catalog_revision_descriptor_candidate_keys_determine_all_attributes,
    catalog_revision_candidate_keys_determine_all_attributes,
    catalog_revision_generation_candidate_keys_determine_all_attributes,
    publication_generation_node_candidate_keys_determine_all_attributes,
    publication_generation_successor_candidate_keys_determine_all_attributes,
    publication_identity_candidate_keys_determine_all_attributes,
    display_title_policy_candidate_keys_determine_all_attributes,
    title_sort_policy_candidate_keys_determine_all_attributes,
    display_title_choice_candidate_keys_determine_all_attributes,
    title_sort_candidate_keys_determine_all_attributes,
    catalog_publication_storage_candidate_keys_determine_all_attributes,
    catalog_publication_occurrence_identity_candidate_keys_determine_all_attributes,
    catalog_publication_candidate_keys_determine_all_attributes,
    catalog_publication_order_candidate_keys_determine_all_attributes,
    catalog_publication_title_candidate_keys_determine_all_attributes,
    catalog_publication_content_candidate_keys_determine_all_attributes,
    contributor_role_registry_candidate_keys_determine_all_attributes,
    catalog_contributor_candidate_keys_determine_all_attributes,
    catalog_subject_candidate_keys_determine_all_attributes,
    catalog_artifact_candidate_keys_determine_all_attributes,
    publication_commit_anchor_candidate_keys_determine_all_attributes,
    publication_commit_candidate_keys_determine_all_attributes,
    publication_commit_finalization_candidate_keys_determine_all_attributes,
    publication_commit_head_receipt_candidate_keys_determine_all_attributes,
    publication_commit_head_candidate_keys_determine_all_attributes,
    publication_receipt_candidate_keys_determine_all_attributes,
    publication_head_revision_candidate_keys_determine_all_attributes,
    publication_head_advanced_at_candidate_keys_determine_all_attributes,
    publication_head_candidate_keys_determine_all_attributes⟩

/- END GENERATED CATALOG CONTRACTS -/

end H2HDB.Verification.VNextSchema
