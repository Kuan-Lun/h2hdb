import Std

/-!
# vNext operational schema and BCNF

The relation declarations below this framework are generated mechanically from
`verification/schema/operational.toml`, the sole authority for relation names,
attributes, declared candidate/alternate keys, and functional dependencies.

`BCNF` quantifies over every attribute subset `X` and every attribute in
`closure_F(X) \ X`; it therefore checks `F⁺`, not merely the input FD list.
`closure_F` is the ordinary iterative Armstrong attribute closure.  Each
relation additionally proves that closure has reached a fixed point, that every
declared key determines every attribute, and that no proper subset of a
declared key is a superkey.

This is a closed-world proof over the manifest FDs.  It assumes the manifest
lists every semantic nontrivial FD.  It also assumes hash identifiers are
collision-free for canonical bytes and canonical encoders/policies are
deterministic.  It does not prove that SQL DDL, migrations, collations, indexes,
foreign keys, or transactions implement the manifest.
-/

namespace H2HDB.Verification.OperationalSchema

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

/-!
Relation-specific declarations and proofs follow.  Every theorem name retains
the exact snake-case manifest relation name for auditability.
-/

end H2HDB.Verification.OperationalSchema

namespace H2HDB.Verification.OperationalSchema

/-!
## Maintenance-gate lifecycle refinement

The holder relation has `slot` as its sole key.  Owner generation and lease
remain normalized in the owner relation and are joined here before a claim can
authorize work.  Replacement models an exact-owner compare-and-swap after the
observed claim is stale or expired.
-/

structure MaintenanceGateOwner (Owner : Type) where
  ownerToken : Owner
  gateGeneration : Nat
  leaseExpiresAt : Nat

structure MaintenanceGateClaim (Owner Slot : Type) where
  ownerToken : Owner
  slot : Slot

inductive MaintenanceGateMode where
  | shared
  | exclusive
deriving DecidableEq

def MaintenanceClaimAuthorized
    (currentGeneration now : Nat)
    (owner : MaintenanceGateOwner Owner)
    (claim : MaintenanceGateClaim Owner Slot) : Prop :=
  claim.ownerToken = owner.ownerToken ∧
    owner.gateGeneration = currentGeneration ∧
    now < owner.leaseExpiresAt

def MayReplaceMaintenanceClaim
    (currentGeneration now : Nat)
    (expectedOwner : Owner)
    (owner : MaintenanceGateOwner Owner)
    (claim : MaintenanceGateClaim Owner Slot) : Prop :=
  claim.ownerToken = expectedOwner ∧
    claim.ownerToken = owner.ownerToken ∧
    (owner.gateGeneration ≠ currentGeneration ∨
      owner.leaseExpiresAt ≤ now)

theorem stale_maintenance_generation_cannot_authorize
    (stale : owner.gateGeneration ≠ currentGeneration) :
    ¬ MaintenanceClaimAuthorized currentGeneration now owner claim := by
  intro authorized
  exact stale authorized.2.1

theorem expired_maintenance_lease_cannot_authorize
    (expired : owner.leaseExpiresAt ≤ now) :
    ¬ MaintenanceClaimAuthorized currentGeneration now owner claim := by
  intro authorized
  exact (Nat.not_lt_of_ge expired) authorized.2.2

theorem maintenance_replacement_requires_exact_observed_owner
    (replacement : MayReplaceMaintenanceClaim
      currentGeneration now expectedOwner owner claim) :
    claim.ownerToken = expectedOwner :=
  replacement.1

theorem maintenance_replacement_requires_stale_or_expired
    (replacement : MayReplaceMaintenanceClaim
      currentGeneration now expectedOwner owner claim) :
    owner.gateGeneration ≠ currentGeneration ∨
      owner.leaseExpiresAt ≤ now :=
  replacement.2.2

theorem current_unexpired_maintenance_claim_cannot_be_replaced
    (current : owner.gateGeneration = currentGeneration)
    (unexpired : now < owner.leaseExpiresAt) :
    ¬ MayReplaceMaintenanceClaim
      currentGeneration now expectedOwner owner claim := by
  intro replacement
  rcases replacement.2.2 with stale | expired
  · exact stale current
  · exact (Nat.not_lt_of_ge expired) unexpired

def SlotHasSingleOwner (claims : List (MaintenanceGateClaim Owner Slot)) : Prop :=
  ∀ left, left ∈ claims → ∀ right, right ∈ claims →
    left.slot = right.slot → left.ownerToken = right.ownerToken

def ExclusiveMaintenanceClaims
    (owner : Owner) (slots : List Slot) :
    List (MaintenanceGateClaim Owner Slot) :=
  slots.map fun slot => { ownerToken := owner, slot := slot }

def OwnerHoldsEveryMaintenanceSlot
    (owner : MaintenanceGateOwner Owner)
    (slots : List Slot)
    (claims : List (MaintenanceGateClaim Owner Slot)) : Prop :=
  ∀ slot, slot ∈ slots →
    MaintenanceGateClaim.mk owner.ownerToken slot ∈ claims

def ExclusiveMaintenanceAuthorized
    (currentGeneration now : Nat)
    (mode : MaintenanceGateMode)
    (slots : List Slot)
    (owner : MaintenanceGateOwner Owner)
    (claims : List (MaintenanceGateClaim Owner Slot)) : Prop :=
  mode = .exclusive ∧
    owner.gateGeneration = currentGeneration ∧
    now < owner.leaseExpiresAt ∧
    OwnerHoldsEveryMaintenanceSlot owner slots claims

theorem exclusive_claims_preserve_slot_single_owner
    (owner : Owner) (slots : List Slot) :
    SlotHasSingleOwner (ExclusiveMaintenanceClaims owner slots) := by
  intro left leftMember right rightMember _sameSlot
  simp only [ExclusiveMaintenanceClaims, List.mem_map] at leftMember rightMember
  obtain ⟨leftSlot, _leftSlotMember, rfl⟩ := leftMember
  obtain ⟨rightSlot, _rightSlotMember, rfl⟩ := rightMember
  rfl

theorem exclusive_claims_cover_arbitrary_slot_collection
    (owner : Owner) (slots : List Slot) (slot : Slot)
    (slotMember : slot ∈ slots) :
    { ownerToken := owner, slot := slot } ∈
      ExclusiveMaintenanceClaims owner slots := by
  exact List.mem_map_of_mem
    (f := fun value : Slot => MaintenanceGateClaim.mk owner value) slotMember

theorem generated_exclusive_claims_authorize_arbitrary_slot_collection
    (owner : MaintenanceGateOwner Owner)
    (slots : List Slot)
    (current : owner.gateGeneration = currentGeneration)
    (unexpired : now < owner.leaseExpiresAt) :
    ExclusiveMaintenanceAuthorized currentGeneration now .exclusive slots owner
      (ExclusiveMaintenanceClaims owner.ownerToken slots) := by
  exact ⟨rfl, current, unexpired, fun slot slotMember =>
    exclusive_claims_cover_arbitrary_slot_collection
      owner.ownerToken slots slot slotMember⟩

theorem shared_mode_cannot_authorize_exclusive_maintenance :
    ¬ ExclusiveMaintenanceAuthorized currentGeneration now .shared slots
      owner claims := by
  intro authorized
  cases authorized.1

theorem exclusive_claim_count_equals_slot_count
    (owner : Owner) (slots : List Slot) :
    (ExclusiveMaintenanceClaims owner slots).length = slots.length := by
  simp [ExclusiveMaintenanceClaims]

def maintenanceGateSlots : List Nat := List.range 64

theorem maintenance_gate_has_exactly_64_slots :
    maintenanceGateSlots.length = 64 := by
  native_decide

theorem maintenance_gate_slots_are_bounded
    (slotMember : slot ∈ maintenanceGateSlots) : slot < 64 := by
  exact List.mem_range.mp slotMember

theorem exclusive_owner_can_hold_every_maintenance_slot
    (owner : Owner) (slotMember : slot ∈ maintenanceGateSlots) :
    { ownerToken := owner, slot := slot } ∈
      ExclusiveMaintenanceClaims owner maintenanceGateSlots :=
  exclusive_claims_cover_arbitrary_slot_collection
    owner maintenanceGateSlots slot slotMember

theorem current_unexpired_exclusive_owner_is_authorized_for_all_64_slots
    (owner : MaintenanceGateOwner Owner)
    (current : owner.gateGeneration = currentGeneration)
    (unexpired : now < owner.leaseExpiresAt) :
    ExclusiveMaintenanceAuthorized currentGeneration now .exclusive
      maintenanceGateSlots owner
      (ExclusiveMaintenanceClaims owner.ownerToken maintenanceGateSlots) :=
  generated_exclusive_claims_authorize_arbitrary_slot_collection
    owner maintenanceGateSlots current unexpired

theorem exclusive_owner_can_hold_distinct_slots (owner : Owner) :
    ∃ first second : MaintenanceGateClaim Owner Nat,
      first ∈ ExclusiveMaintenanceClaims owner maintenanceGateSlots ∧
      second ∈ ExclusiveMaintenanceClaims owner maintenanceGateSlots ∧
      first.ownerToken = owner ∧ second.ownerToken = owner ∧
      first.slot ≠ second.slot := by
  refine ⟨MaintenanceGateClaim.mk owner 0,
    MaintenanceGateClaim.mk owner 1, ?_, ?_, rfl, rfl, ?_⟩
  · apply exclusive_owner_can_hold_every_maintenance_slot
    native_decide
  · apply exclusive_owner_can_hold_every_maintenance_slot
    native_decide
  · change (0 : Nat) ≠ 1
    decide

/-!
## Operational integrity refinement

These definitions make the lifecycle obligations that cannot be expressed by
keys and foreign keys executable.  They are unbounded over their type and list
parameters; the concrete SQL implementations must refine the atomic predicates.
-/

structure BuildGenerationReservation (Build : Type) where
  generation : Nat
  build : Build

def CanReserveBuild
    (reservations : List (BuildGenerationReservation Build))
    (generation : Nat) (build : Build) : Prop :=
  ∀ existing, existing ∈ reservations →
    existing.generation = generation → existing.build = build

def GenerationHasNoBuild
    (reservations : List (BuildGenerationReservation Build))
    (generation : Nat) : Prop :=
  ∀ existing, existing ∈ reservations →
    existing.generation ≠ generation

theorem first_build_reservation_is_admitted
    (generation : Nat) (build : Build) :
    CanReserveBuild [] generation build := by
  intro existing member
  cases member

theorem second_distinct_build_in_same_generation_is_rejected
    (existing : BuildGenerationReservation Build)
    (member : existing ∈ reservations)
    (sameGeneration : existing.generation = generation)
    (differentBuild : existing.build ≠ proposedBuild) :
    ¬ CanReserveBuild reservations generation proposedBuild := by
  intro reservation
  exact differentBuild (reservation existing member sameGeneration)

theorem strictly_newer_generation_may_resume_same_build
    (oldGeneration newGeneration : Nat) (build : Build)
    (newer : oldGeneration < newGeneration) :
    CanReserveBuild
      [{ generation := oldGeneration, build := build }]
      newGeneration build := by
  intro existing member sameGeneration
  simp only [List.mem_singleton] at member
  subst existing
  exact False.elim ((Nat.ne_of_lt newer) sameGeneration)

theorem no_build_generation_has_no_reservation
    (Build : Type) (generation : Nat) :
    GenerationHasNoBuild (Build := Build) [] generation := by
  intro existing member
  cases member

structure DeletionRequestAttempt (Token : Type) where
  requestToken : Token
  gid : Nat

structure DeletionRequestHead (Token : Type) where
  gid : Nat
  requestToken : Token

structure DeletionConsumption (Token : Type) where
  gid : Nat
  requestToken : Token

def DeletionHeadMatchesAttempt
    (attempt : DeletionRequestAttempt Token)
    (head : DeletionRequestHead Token) : Prop :=
  head.requestToken = attempt.requestToken ∧ head.gid = attempt.gid

def DeletionConsumptionMatchesAttempt
    (attempt : DeletionRequestAttempt Token)
    (consumption : DeletionConsumption Token) : Prop :=
  consumption.requestToken = attempt.requestToken ∧
    consumption.gid = attempt.gid

theorem rotating_or_removing_head_preserves_consumed_attempt
    (valid : DeletionConsumptionMatchesAttempt attempt consumption)
    (_newHead : Option (DeletionRequestHead Token)) :
    DeletionConsumptionMatchesAttempt attempt consumption :=
  valid

def OptionalDeletionUrl (Payload : Type) := Option Payload

theorem deletion_url_may_be_absent :
    ∃ value : OptionalDeletionUrl Payload, value = none :=
  ⟨none, rfl⟩

theorem deletion_url_may_preserve_exact_empty_payload
    (emptyPayload : Payload) :
    ∃ value : OptionalDeletionUrl Payload, value = some emptyPayload :=
  ⟨some emptyPayload, rfl⟩

structure CanonicalPreimage (Digest Policy Payload : Type) where
  digest : Digest
  policy : Policy
  payload : Payload

def CanonicalReferenceValid
    (expectedPolicy : Policy) (digest : Digest)
    (stored : CanonicalPreimage Digest Policy Payload) : Prop :=
  stored.digest = digest ∧ stored.policy = expectedPolicy

def CanonicalConflictCompatible
    (digest : Digest) (policy : Policy) (payload : Payload)
    (stored : CanonicalPreimage Digest Policy Payload) : Prop :=
  stored.digest = digest →
    stored.policy = policy ∧ stored.payload = payload

theorem accepted_canonical_digest_conflict_requires_exact_preimage
    (compatible : CanonicalConflictCompatible digest policy payload stored)
    (sameDigest : stored.digest = digest) :
    stored.policy = policy ∧ stored.payload = payload :=
  compatible sameDigest

theorem distinct_hash_roles_cannot_accept_one_policy
    (separated : sourcePolicy ≠ fingerprintPolicy)
    (sourceValid : CanonicalReferenceValid sourcePolicy digest stored)
    (fingerprintValid : CanonicalReferenceValid fingerprintPolicy digest stored) :
    False :=
  separated (sourceValid.2.symm.trans fingerprintValid.2)

structure CleanupCycle (Target Cleanup : Type) where
  target : Target
  cycleGeneration : Nat
  cleanupId : Cleanup

structure CleanupCycleId (Kind : Type) where
  kind : Kind
  shard : Fin 256
  generation : Nat
deriving DecidableEq

def EncodeCleanupCycleId
    (kind : Kind) (shard : Fin 256) (generation : Nat) :
    CleanupCycleId Kind :=
  { kind := kind, shard := shard, generation := generation }

theorem distinct_cleanup_generation_has_distinct_deterministic_id
    (different : firstGeneration ≠ secondGeneration) :
    EncodeCleanupCycleId kind shard firstGeneration ≠
      EncodeCleanupCycleId kind shard secondGeneration := by
  intro equalId
  exact different (congrArg CleanupCycleId.generation equalId)

def CleanupMutationAuthorized
    (current presented : CleanupCycle Target Cleanup) : Prop :=
  current.target = presented.target ∧
    current.cycleGeneration = presented.cycleGeneration ∧
    current.cleanupId = presented.cleanupId

theorem rotated_cleanup_id_rejects_stale_checkpoint_mutation
    (rotated : current.cleanupId ≠ stale.cleanupId) :
    ¬ CleanupMutationAuthorized current stale := by
  intro authorized
  exact rotated authorized.2.2

def CleanupCompletionReplayAuthorized
    (jobComplete : Bool) (jobGeneration completionGeneration : Nat) : Prop :=
  jobComplete = true ∧ jobGeneration = completionGeneration

theorem stale_cleanup_completion_generation_cannot_replay_complete
    (stale : jobGeneration ≠ completionGeneration) :
    ¬ CleanupCompletionReplayAuthorized true jobGeneration completionGeneration := by
  intro authorized
  exact stale authorized.2

theorem open_cleanup_job_cannot_replay_complete :
    ¬ CleanupCompletionReplayAuthorized false jobGeneration completionGeneration := by
  intro authorized
  exact Bool.false_ne_true authorized.1

structure PreparationIdentity (Build Policy : Type) where
  build : Build
  deletionGeneration : Nat
  policy : Policy

def SamePreparationNaturalIdentity
    (left right : PreparationIdentity Build Policy) : Prop :=
  left.build = right.build ∧
    left.deletionGeneration = right.deletionGeneration ∧
    left.policy = right.policy

theorem policy_change_requires_distinct_preparation
    (differentPolicy : first.policy ≠ second.policy) :
    ¬ SamePreparationNaturalIdentity first second := by
  intro sameIdentity
  exact differentPolicy sameIdentity.2.2

inductive OperationalEventKind where
  | removedGid
  | deletionConsumption
deriving DecidableEq

structure OperationalSubtypeRows where
  hasRemovedGid : Bool
  hasDeletionConsumption : Bool

def ExactOperationalSubtypeRows
    (kind : OperationalEventKind) (rows : OperationalSubtypeRows) : Prop :=
  match kind with
  | .removedGid =>
      rows.hasRemovedGid = true ∧ rows.hasDeletionConsumption = false
  | .deletionConsumption =>
      rows.hasRemovedGid = false ∧ rows.hasDeletionConsumption = true

def ExactlyOneSubtypeRow (rows : OperationalSubtypeRows) : Prop :=
  (rows.hasRemovedGid = true ∧ rows.hasDeletionConsumption = false) ∨
    (rows.hasRemovedGid = false ∧ rows.hasDeletionConsumption = true)

theorem subtype_type_match_implies_exactly_one
    (evidence : ExactOperationalSubtypeRows kind rows) :
    ExactlyOneSubtypeRow rows := by
  cases kind with
  | removedGid => exact Or.inl evidence
  | deletionConsumption => exact Or.inr evidence

theorem removed_gid_subtype_is_exact_and_type_matched :
    ExactOperationalSubtypeRows .removedGid
      { hasRemovedGid := true, hasDeletionConsumption := false } := by
  exact ⟨rfl, rfl⟩

theorem deletion_consumption_subtype_is_exact_and_type_matched :
    ExactOperationalSubtypeRows .deletionConsumption
      { hasRemovedGid := false, hasDeletionConsumption := true } := by
  exact ⟨rfl, rfl⟩

structure OperationalEventCoordinate where
  sourceRevision : Nat
  sequenceNo : Nat

def AcknowledgesEveryExistingEventThrough
    (allEvents acknowledgedEvents : List OperationalEventCoordinate)
    (target : OperationalEventCoordinate) : Prop :=
  ∀ event, event ∈ allEvents →
    event.sourceRevision = target.sourceRevision →
    event.sequenceNo ≤ target.sequenceNo →
    event ∈ acknowledgedEvents

def AckHighWaterAdvance
    (oldHead targetEvent : OperationalEventCoordinate)
    (allEvents acknowledgedEvents : List OperationalEventCoordinate) : Prop :=
  targetEvent.sourceRevision = oldHead.sourceRevision ∧
    oldHead.sequenceNo ≤ targetEvent.sequenceNo ∧
    targetEvent ∈ allEvents ∧
    AcknowledgesEveryExistingEventThrough
      allEvents acknowledgedEvents targetEvent

theorem ack_high_water_advance_stays_in_revision
    (advance : AckHighWaterAdvance
      oldHead targetEvent allEvents acknowledgedEvents) :
    targetEvent.sourceRevision = oldHead.sourceRevision :=
  advance.1

theorem ack_high_water_advance_is_monotone
    (advance : AckHighWaterAdvance
      oldHead targetEvent allEvents acknowledgedEvents) :
    oldHead.sequenceNo ≤ targetEvent.sequenceNo :=
  advance.2.1

theorem ack_high_water_target_event_exists
    (advance : AckHighWaterAdvance
      oldHead targetEvent allEvents acknowledgedEvents) :
    targetEvent ∈ allEvents :=
  advance.2.2.1

theorem ack_high_water_covers_every_preceding_existing_event
    (advance : AckHighWaterAdvance
      oldHead targetEvent allEvents acknowledgedEvents)
    (eventMember : event ∈ allEvents)
    (sameRevision : event.sourceRevision = targetEvent.sourceRevision)
    (atOrBefore : event.sequenceNo ≤ targetEvent.sequenceNo) :
    event ∈ acknowledgedEvents :=
  advance.2.2.2 event eventMember sameRevision atOrBefore

theorem ack_high_water_regression_is_rejected
    (regression : targetEvent.sequenceNo < oldHead.sequenceNo) :
    ¬ AckHighWaterAdvance
      oldHead targetEvent allEvents acknowledgedEvents := by
  intro advance
  exact (Nat.not_lt_of_ge advance.2.1) regression

/-!
## Bounded gallery-staging refinement

This supporting model states the protocol properties enforced by the concrete
request codec, parser transition, physical checks, and writer hook.  It is
unbounded over identifiers and cursors; the base-256 cardinality theorem is an
exact arithmetic fact for the four registered components and eight internal
levels.
-/

def GalleryClaimAuthorized
    (currentClaim presentedClaim : Nat) (liveOwner : Prop) : Prop :=
  liveOwner ∧ currentClaim = presentedClaim

theorem stale_gallery_claim_cannot_mutate
    (stale : currentClaim ≠ presentedClaim) :
    ¬ GalleryClaimAuthorized currentClaim presentedClaim liveOwner := by
  intro authorized
  exact stale authorized.2

def ContiguousGalleryCursorAdvance
    (current next itemCount : Nat) : Prop :=
  next = current + itemCount

theorem gallery_cursor_advance_cannot_skip
    (advance : ContiguousGalleryCursorAdvance current next itemCount) :
    next = current + itemCount :=
  advance

def GalleryLatestReceiptReplay
    (liveOwner exactRequest : Prop)
    (currentCursor storedNextCursor : Nat) : Prop :=
  liveOwner ∧ exactRequest ∧ currentCursor = storedNextCursor

theorem gallery_response_loss_replay_requires_exact_latest_request
    (replay : GalleryLatestReceiptReplay
      liveOwner exactRequest currentCursor storedNextCursor) :
    exactRequest :=
  replay.2.1

theorem stale_owner_cannot_replay_gallery_receipt
    (stale : ¬ liveOwner) :
    ¬ GalleryLatestReceiptReplay
      liveOwner exactRequest currentCursor storedNextCursor := by
  intro replay
  exact stale replay.1

structure GallerySealFacts where
  fileComplete : Prop
  tagComplete : Prop
  directoryComplete : Prop
  metadataComplete : Prop
  rootsConsistent : Prop

def GalleryFinalVisible (facts : GallerySealFacts) : Prop :=
  facts.fileComplete ∧ facts.tagComplete ∧ facts.directoryComplete ∧
    facts.metadataComplete ∧ facts.rootsConsistent

theorem gallery_final_visibility_implies_all_streams_complete
    (visible : GalleryFinalVisible facts) :
    facts.fileComplete ∧ facts.tagComplete ∧ facts.directoryComplete ∧
      facts.metadataComplete :=
  ⟨visible.1, visible.2.1, visible.2.2.1, visible.2.2.2.1⟩

theorem gallery_final_visibility_implies_root_consistency
    (visible : GalleryFinalVisible facts) : facts.rootsConsistent :=
  visible.2.2.2.2

def galleryFrontierCardinalityBound : Nat := 4 * 8 * 255

theorem gallery_frontier_cardinality_is_8160 :
    galleryFrontierCardinalityBound = 8160 := by
  native_decide

def PortableAllocatorAdvance (current presented : Nat) : Option (Nat × Nat) :=
  if current = presented ∧ 1 ≤ current ∧ current < 9223372036854775807 then
    some (current, current + 1)
  else
    none

theorem exhausted_allocator_sentinel_fails_closed :
    PortableAllocatorAdvance 9223372036854775807 9223372036854775807 = none := by
  native_decide

/-!
## Machine obligations and bootstrap genesis

These values are generated from the typed machine records in operational.toml.
Allocator rows plus cleanup kind/phase registries are provider-owned genesis.
Coordination, maintenance, owner, lease, event, queue, cache, and work facts are
absent until their first transaction creates real state.  Schema epoch control
is initialized independently by SchemaEpochCatalog.
-/

structure RevisionAllocatorGenesis where
  stream : String
  nextRevision : Nat
  updatedAt : Nat
deriving DecidableEq, Repr

def CleanupEligible (reachableFromRetentionRoot : Target → Prop)
    (target : Target) : Prop := ¬ reachableFromRetentionRoot target

theorem reachable_retention_root_is_never_cleanup_eligible
    (reachable : reachableFromRetentionRoot target) :
    ¬ CleanupEligible reachableFromRetentionRoot target := by
  intro eligible
  exact eligible reachable

structure CleanupPhaseOrder where
  childOrder : Nat
  parentOrder : Nat

def ChildBeforeParent (phase : CleanupPhaseOrder) : Prop :=
  phase.childOrder < phase.parentOrder

theorem parent_phase_cannot_precede_its_child
    (ordered : ChildBeforeParent phase) :
    ¬ phase.parentOrder ≤ phase.childOrder := by
  exact Nat.not_le_of_lt ordered

def RevisionAllocatorCurrentValid (state : RevisionAllocatorGenesis) : Prop :=
  1 ≤ state.nextRevision

def RevisionAllocatorAdvance
    (oldState newState : RevisionAllocatorGenesis)
    (allocatedRevision : Nat) : Prop :=
  allocatedRevision = oldState.nextRevision ∧
    newState.stream = oldState.stream ∧
    newState.nextRevision = oldState.nextRevision + 1 ∧
    oldState.updatedAt ≤ newState.updatedAt

theorem revision_allocator_advance_returns_exact_old_next_revision
    (advance : RevisionAllocatorAdvance oldState newState allocatedRevision) :
    allocatedRevision = oldState.nextRevision :=
  advance.1

theorem revision_allocator_advance_is_exactly_monotone
    (advance : RevisionAllocatorAdvance oldState newState allocatedRevision) :
    oldState.nextRevision < newState.nextRevision := by
  unfold RevisionAllocatorAdvance at advance
  rw [advance.2.2.1]
  omega

theorem revision_allocator_advance_preserves_current_validity
    (valid : RevisionAllocatorCurrentValid oldState)
    (advance : RevisionAllocatorAdvance oldState newState allocatedRevision) :
    RevisionAllocatorCurrentValid newState := by
  unfold RevisionAllocatorCurrentValid at valid ⊢
  unfold RevisionAllocatorAdvance at advance
  rw [advance.2.2.1]
  omega

def sourceRevisionAllocatorGenesis : RevisionAllocatorGenesis :=
  { stream := "SOURCE",
    nextRevision := 1, updatedAt := 0 }

def catalogRevisionAllocatorGenesis : RevisionAllocatorGenesis :=
  { stream := "CATALOG",
    nextRevision := 1, updatedAt := 0 }

def operationalBootstrapAllocatorRows : List RevisionAllocatorGenesis :=
  [sourceRevisionAllocatorGenesis, catalogRevisionAllocatorGenesis]

theorem operational_bootstrap_has_exactly_two_allocator_rows :
    operationalBootstrapAllocatorRows.length = 2 := by
  native_decide

theorem operational_bootstrap_allocator_streams_are_distinct :
    sourceRevisionAllocatorGenesis.stream ≠
      catalogRevisionAllocatorGenesis.stream := by
  native_decide

theorem operational_bootstrap_allocators_start_at_revision_one :
    sourceRevisionAllocatorGenesis.nextRevision = 1 ∧
      catalogRevisionAllocatorGenesis.nextRevision = 1 := by
  native_decide

theorem operational_bootstrap_allocator_timestamps_start_at_zero :
    sourceRevisionAllocatorGenesis.updatedAt = 0 ∧
      catalogRevisionAllocatorGenesis.updatedAt = 0 := by
  native_decide

theorem ready_validation_accepts_a_legitimately_advanced_allocator :
    RevisionAllocatorCurrentValid
      { stream := "SOURCE", nextRevision := 2,
        updatedAt := 1 } := by
  change (1 : Nat) ≤ 2
  decide

def operationalSemanticObligationIds : List String :=
  ["h2hdb.operational.physical-domains.v1", "h2hdb.operational.epoch-manifest.v1", "h2hdb.operational.fencing.v1", "h2hdb.operational.maintenance-gate.v1", "h2hdb.operational.bounded-work.v1", "h2hdb.operational.queue-history.v1", "h2hdb.operational.canonical-hash-cache.v1", "h2hdb.operational.event-integrity.v1", "h2hdb.operational.build-generation.v1", "h2hdb.operational.attempt-identity.v1", "h2hdb.operational.cleanup-reachability.v1", "h2hdb.operational.revision-allocation.v1", "h2hdb.operational.gallery-staging.v1", "h2hdb.operational.bootstrap-genesis.v1"]

theorem operational_semantic_obligation_ids_are_unique :
    operationalSemanticObligationIds.Nodup := by
  native_decide

theorem operational_semantic_obligation_count :
    operationalSemanticObligationIds.length = 14 := by
  native_decide

def operationalBuildingOnlyObligationIds : List String :=
  ["h2hdb.operational.bootstrap-genesis.v1"]

def operationalReadyObligationIds : List String :=
  ["h2hdb.operational.physical-domains.v1", "h2hdb.operational.epoch-manifest.v1", "h2hdb.operational.fencing.v1", "h2hdb.operational.maintenance-gate.v1", "h2hdb.operational.bounded-work.v1", "h2hdb.operational.queue-history.v1", "h2hdb.operational.canonical-hash-cache.v1", "h2hdb.operational.event-integrity.v1", "h2hdb.operational.build-generation.v1", "h2hdb.operational.attempt-identity.v1", "h2hdb.operational.cleanup-reachability.v1", "h2hdb.operational.revision-allocation.v1", "h2hdb.operational.gallery-staging.v1"]

theorem bootstrap_genesis_is_the_only_building_only_obligation :
    operationalBuildingOnlyObligationIds =
      ["h2hdb.operational.bootstrap-genesis.v1"] := by
  native_decide

theorem building_only_and_ready_obligations_are_disjoint :
    ∀ obligation ∈ operationalBuildingOnlyObligationIds,
      obligation ∉ operationalReadyObligationIds := by
  native_decide

def operationalBootstrapAbsentRelations : List String :=
  ["ingest_generation", "ingest_coordination_head", "ingest_generation_owner", "ingest_generation_lease", "ingest_generation_handoff", "source_build_generation", "maintenance_gate_generation", "maintenance_gate_head", "maintenance_gate_owner", "maintenance_gate_holder", "maintenance_work_state", "source_working_build", "catalog_working_candidate", "gallery_observation_allocator", "gallery_observation_staging", "gallery_observation_staging_claim", "gallery_observation_staging_checkpoint", "gallery_observation_staging_request", "gallery_observation_staging_request_chunk", "gallery_observation_staging_request_owner", "gallery_observation_staging_request_predecessor", "gallery_observation_staging_page_request", "gallery_observation_staging_request_page", "gallery_observation_staging_receipt", "gallery_observation_staging_frontier", "gallery_observation_staging_match_checkpoint", "gallery_observation_staging_match_request", "gallery_observation_staging_match_receipt", "gallery_observation_staging_metadata_parser", "canonical_value_upload", "download_request", "deletion_request_attempt", "deletion_request_url", "deletion_request_head", "removed_gid", "gallery_redownload_state", "operational_policy", "operational_preparation", "operational_preparation_checkpoint", "operational_preparation_batch_receipt", "operational_activation", "operational_event", "operational_removed_gid_event", "operational_deletion_consumption_event", "operational_consumer", "operational_event_ack", "operational_event_ack_head", "removed_gid_ack", "hash_cache_observation", "file_hash_cache", "cleanup_job", "cleanup_checkpoint", "cleanup_batch_receipt", "cleanup_completion"]

theorem operational_bootstrap_has_no_invented_active_control_facts :
    ∀ relation ∈ ["ingest_generation", "ingest_coordination_head", "ingest_generation_owner", "ingest_generation_lease", "maintenance_gate_generation", "maintenance_gate_head", "maintenance_gate_owner", "maintenance_gate_holder", "operational_event", "download_request", "cleanup_job"],
      relation ∈ operationalBootstrapAbsentRelations := by
  native_decide

theorem revision_allocator_is_seeded_not_absent :
    "revision_allocator" ∉ operationalBootstrapAbsentRelations := by
  native_decide

theorem schema_epoch_control_is_epoch_owned_not_absent :
    "schema_epoch_control" ∉ operationalBootstrapAbsentRelations := by
  native_decide

/- BEGIN GENERATED OPERATIONAL CONTRACTS -/
def operationalManifestSha256 : String := "6b6e438921eb621a8050abd00e82b7421a215c329b23c4cd9ad41e3f249f396c"

/-! This section is mechanically generated from operational.toml. -/

def schema_epoch_control_contract : RelationContract where
  name := "schema_epoch_control"
  attributes := ["singleton_id", "epoch", "schema_version", "state", "manifest_sha256", "started_at", "ready_at"]
  declaredKeys := [["singleton_id"]]
  declaredFDs := [
    { determinant := ["singleton_id"], dependent := ["epoch", "schema_version", "state", "manifest_sha256", "started_at", "ready_at"] }
  ]

theorem schema_epoch_control_schema_well_formed :
    schemaWellFormedCheck schema_epoch_control_contract = true := by
  native_decide

theorem schema_epoch_control_candidate_keys_check :
    keysDetermineAllCheck schema_epoch_control_contract = true := by
  native_decide

theorem schema_epoch_control_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes schema_epoch_control_contract :=
  keysDetermineAllCheck_sound schema_epoch_control_contract
    schema_epoch_control_candidate_keys_check

theorem schema_epoch_control_candidate_keys_minimal_check :
    declaredKeysMinimalCheck schema_epoch_control_contract = true := by
  native_decide

theorem schema_epoch_control_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal schema_epoch_control_contract :=
  declaredKeysMinimalCheck_sound schema_epoch_control_contract
    schema_epoch_control_candidate_keys_minimal_check

theorem schema_epoch_control_closure_fixed_check :
    closureFixedPointCheck schema_epoch_control_contract = true := by
  native_decide

theorem schema_epoch_control_closure_reached_fixed_point :
    ClosureReachedFixedPoint schema_epoch_control_contract :=
  closureFixedPointCheck_sound schema_epoch_control_contract
    schema_epoch_control_closure_fixed_check

theorem schema_epoch_control_bcnf_check :
    bcnfCheck schema_epoch_control_contract = true := by
  native_decide

theorem schema_epoch_control_bcnf : BCNF schema_epoch_control_contract :=
  bcnfCheck_sound schema_epoch_control_contract schema_epoch_control_bcnf_check

def ingest_generation_contract : RelationContract where
  name := "ingest_generation"
  attributes := ["generation", "started_at", "completed_at"]
  declaredKeys := [["generation"]]
  declaredFDs := [
    { determinant := ["generation"], dependent := ["started_at", "completed_at"] }
  ]

theorem ingest_generation_schema_well_formed :
    schemaWellFormedCheck ingest_generation_contract = true := by
  native_decide

theorem ingest_generation_candidate_keys_check :
    keysDetermineAllCheck ingest_generation_contract = true := by
  native_decide

theorem ingest_generation_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes ingest_generation_contract :=
  keysDetermineAllCheck_sound ingest_generation_contract
    ingest_generation_candidate_keys_check

theorem ingest_generation_candidate_keys_minimal_check :
    declaredKeysMinimalCheck ingest_generation_contract = true := by
  native_decide

theorem ingest_generation_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal ingest_generation_contract :=
  declaredKeysMinimalCheck_sound ingest_generation_contract
    ingest_generation_candidate_keys_minimal_check

theorem ingest_generation_closure_fixed_check :
    closureFixedPointCheck ingest_generation_contract = true := by
  native_decide

theorem ingest_generation_closure_reached_fixed_point :
    ClosureReachedFixedPoint ingest_generation_contract :=
  closureFixedPointCheck_sound ingest_generation_contract
    ingest_generation_closure_fixed_check

theorem ingest_generation_bcnf_check :
    bcnfCheck ingest_generation_contract = true := by
  native_decide

theorem ingest_generation_bcnf : BCNF ingest_generation_contract :=
  bcnfCheck_sound ingest_generation_contract ingest_generation_bcnf_check

def ingest_coordination_head_contract : RelationContract where
  name := "ingest_coordination_head"
  attributes := ["singleton_id", "current_generation", "completed_generation", "phase", "last_transition_at"]
  declaredKeys := [["singleton_id"]]
  declaredFDs := [
    { determinant := ["singleton_id"], dependent := ["current_generation", "completed_generation", "phase", "last_transition_at"] }
  ]

theorem ingest_coordination_head_schema_well_formed :
    schemaWellFormedCheck ingest_coordination_head_contract = true := by
  native_decide

theorem ingest_coordination_head_candidate_keys_check :
    keysDetermineAllCheck ingest_coordination_head_contract = true := by
  native_decide

theorem ingest_coordination_head_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes ingest_coordination_head_contract :=
  keysDetermineAllCheck_sound ingest_coordination_head_contract
    ingest_coordination_head_candidate_keys_check

theorem ingest_coordination_head_candidate_keys_minimal_check :
    declaredKeysMinimalCheck ingest_coordination_head_contract = true := by
  native_decide

theorem ingest_coordination_head_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal ingest_coordination_head_contract :=
  declaredKeysMinimalCheck_sound ingest_coordination_head_contract
    ingest_coordination_head_candidate_keys_minimal_check

theorem ingest_coordination_head_closure_fixed_check :
    closureFixedPointCheck ingest_coordination_head_contract = true := by
  native_decide

theorem ingest_coordination_head_closure_reached_fixed_point :
    ClosureReachedFixedPoint ingest_coordination_head_contract :=
  closureFixedPointCheck_sound ingest_coordination_head_contract
    ingest_coordination_head_closure_fixed_check

theorem ingest_coordination_head_bcnf_check :
    bcnfCheck ingest_coordination_head_contract = true := by
  native_decide

theorem ingest_coordination_head_bcnf : BCNF ingest_coordination_head_contract :=
  bcnfCheck_sound ingest_coordination_head_contract ingest_coordination_head_bcnf_check

def ingest_generation_owner_contract : RelationContract where
  name := "ingest_generation_owner"
  attributes := ["generation", "owner_token", "claimed_at"]
  declaredKeys := [["generation"], ["owner_token"]]
  declaredFDs := [
    { determinant := ["generation"], dependent := ["owner_token", "claimed_at"] },
    { determinant := ["owner_token"], dependent := ["generation", "claimed_at"] }
  ]

theorem ingest_generation_owner_schema_well_formed :
    schemaWellFormedCheck ingest_generation_owner_contract = true := by
  native_decide

theorem ingest_generation_owner_candidate_keys_check :
    keysDetermineAllCheck ingest_generation_owner_contract = true := by
  native_decide

theorem ingest_generation_owner_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes ingest_generation_owner_contract :=
  keysDetermineAllCheck_sound ingest_generation_owner_contract
    ingest_generation_owner_candidate_keys_check

theorem ingest_generation_owner_candidate_keys_minimal_check :
    declaredKeysMinimalCheck ingest_generation_owner_contract = true := by
  native_decide

theorem ingest_generation_owner_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal ingest_generation_owner_contract :=
  declaredKeysMinimalCheck_sound ingest_generation_owner_contract
    ingest_generation_owner_candidate_keys_minimal_check

theorem ingest_generation_owner_closure_fixed_check :
    closureFixedPointCheck ingest_generation_owner_contract = true := by
  native_decide

theorem ingest_generation_owner_closure_reached_fixed_point :
    ClosureReachedFixedPoint ingest_generation_owner_contract :=
  closureFixedPointCheck_sound ingest_generation_owner_contract
    ingest_generation_owner_closure_fixed_check

theorem ingest_generation_owner_bcnf_check :
    bcnfCheck ingest_generation_owner_contract = true := by
  native_decide

theorem ingest_generation_owner_bcnf : BCNF ingest_generation_owner_contract :=
  bcnfCheck_sound ingest_generation_owner_contract ingest_generation_owner_bcnf_check

def ingest_generation_lease_contract : RelationContract where
  name := "ingest_generation_lease"
  attributes := ["generation", "lease_expires_at"]
  declaredKeys := [["generation"]]
  declaredFDs := [
    { determinant := ["generation"], dependent := ["lease_expires_at"] }
  ]

theorem ingest_generation_lease_schema_well_formed :
    schemaWellFormedCheck ingest_generation_lease_contract = true := by
  native_decide

theorem ingest_generation_lease_candidate_keys_check :
    keysDetermineAllCheck ingest_generation_lease_contract = true := by
  native_decide

theorem ingest_generation_lease_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes ingest_generation_lease_contract :=
  keysDetermineAllCheck_sound ingest_generation_lease_contract
    ingest_generation_lease_candidate_keys_check

theorem ingest_generation_lease_candidate_keys_minimal_check :
    declaredKeysMinimalCheck ingest_generation_lease_contract = true := by
  native_decide

theorem ingest_generation_lease_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal ingest_generation_lease_contract :=
  declaredKeysMinimalCheck_sound ingest_generation_lease_contract
    ingest_generation_lease_candidate_keys_minimal_check

theorem ingest_generation_lease_closure_fixed_check :
    closureFixedPointCheck ingest_generation_lease_contract = true := by
  native_decide

theorem ingest_generation_lease_closure_reached_fixed_point :
    ClosureReachedFixedPoint ingest_generation_lease_contract :=
  closureFixedPointCheck_sound ingest_generation_lease_contract
    ingest_generation_lease_closure_fixed_check

theorem ingest_generation_lease_bcnf_check :
    bcnfCheck ingest_generation_lease_contract = true := by
  native_decide

theorem ingest_generation_lease_bcnf : BCNF ingest_generation_lease_contract :=
  bcnfCheck_sound ingest_generation_lease_contract ingest_generation_lease_bcnf_check

def ingest_generation_handoff_contract : RelationContract where
  name := "ingest_generation_handoff"
  attributes := ["generation", "requested_at"]
  declaredKeys := [["generation"]]
  declaredFDs := [
    { determinant := ["generation"], dependent := ["requested_at"] }
  ]

theorem ingest_generation_handoff_schema_well_formed :
    schemaWellFormedCheck ingest_generation_handoff_contract = true := by
  native_decide

theorem ingest_generation_handoff_candidate_keys_check :
    keysDetermineAllCheck ingest_generation_handoff_contract = true := by
  native_decide

theorem ingest_generation_handoff_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes ingest_generation_handoff_contract :=
  keysDetermineAllCheck_sound ingest_generation_handoff_contract
    ingest_generation_handoff_candidate_keys_check

theorem ingest_generation_handoff_candidate_keys_minimal_check :
    declaredKeysMinimalCheck ingest_generation_handoff_contract = true := by
  native_decide

theorem ingest_generation_handoff_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal ingest_generation_handoff_contract :=
  declaredKeysMinimalCheck_sound ingest_generation_handoff_contract
    ingest_generation_handoff_candidate_keys_minimal_check

theorem ingest_generation_handoff_closure_fixed_check :
    closureFixedPointCheck ingest_generation_handoff_contract = true := by
  native_decide

theorem ingest_generation_handoff_closure_reached_fixed_point :
    ClosureReachedFixedPoint ingest_generation_handoff_contract :=
  closureFixedPointCheck_sound ingest_generation_handoff_contract
    ingest_generation_handoff_closure_fixed_check

theorem ingest_generation_handoff_bcnf_check :
    bcnfCheck ingest_generation_handoff_contract = true := by
  native_decide

theorem ingest_generation_handoff_bcnf : BCNF ingest_generation_handoff_contract :=
  bcnfCheck_sound ingest_generation_handoff_contract ingest_generation_handoff_bcnf_check

def source_build_generation_contract : RelationContract where
  name := "source_build_generation"
  attributes := ["build_id", "generation"]
  declaredKeys := [["generation"]]
  declaredFDs := [
    { determinant := ["generation"], dependent := ["build_id"] }
  ]

theorem source_build_generation_schema_well_formed :
    schemaWellFormedCheck source_build_generation_contract = true := by
  native_decide

theorem source_build_generation_candidate_keys_check :
    keysDetermineAllCheck source_build_generation_contract = true := by
  native_decide

theorem source_build_generation_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_build_generation_contract :=
  keysDetermineAllCheck_sound source_build_generation_contract
    source_build_generation_candidate_keys_check

theorem source_build_generation_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_build_generation_contract = true := by
  native_decide

theorem source_build_generation_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_build_generation_contract :=
  declaredKeysMinimalCheck_sound source_build_generation_contract
    source_build_generation_candidate_keys_minimal_check

theorem source_build_generation_closure_fixed_check :
    closureFixedPointCheck source_build_generation_contract = true := by
  native_decide

theorem source_build_generation_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_build_generation_contract :=
  closureFixedPointCheck_sound source_build_generation_contract
    source_build_generation_closure_fixed_check

theorem source_build_generation_bcnf_check :
    bcnfCheck source_build_generation_contract = true := by
  native_decide

theorem source_build_generation_bcnf : BCNF source_build_generation_contract :=
  bcnfCheck_sound source_build_generation_contract source_build_generation_bcnf_check

def maintenance_gate_generation_contract : RelationContract where
  name := "maintenance_gate_generation"
  attributes := ["gate_generation", "mode", "created_at"]
  declaredKeys := [["gate_generation"]]
  declaredFDs := [
    { determinant := ["gate_generation"], dependent := ["mode", "created_at"] }
  ]

theorem maintenance_gate_generation_schema_well_formed :
    schemaWellFormedCheck maintenance_gate_generation_contract = true := by
  native_decide

theorem maintenance_gate_generation_candidate_keys_check :
    keysDetermineAllCheck maintenance_gate_generation_contract = true := by
  native_decide

theorem maintenance_gate_generation_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes maintenance_gate_generation_contract :=
  keysDetermineAllCheck_sound maintenance_gate_generation_contract
    maintenance_gate_generation_candidate_keys_check

theorem maintenance_gate_generation_candidate_keys_minimal_check :
    declaredKeysMinimalCheck maintenance_gate_generation_contract = true := by
  native_decide

theorem maintenance_gate_generation_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal maintenance_gate_generation_contract :=
  declaredKeysMinimalCheck_sound maintenance_gate_generation_contract
    maintenance_gate_generation_candidate_keys_minimal_check

theorem maintenance_gate_generation_closure_fixed_check :
    closureFixedPointCheck maintenance_gate_generation_contract = true := by
  native_decide

theorem maintenance_gate_generation_closure_reached_fixed_point :
    ClosureReachedFixedPoint maintenance_gate_generation_contract :=
  closureFixedPointCheck_sound maintenance_gate_generation_contract
    maintenance_gate_generation_closure_fixed_check

theorem maintenance_gate_generation_bcnf_check :
    bcnfCheck maintenance_gate_generation_contract = true := by
  native_decide

theorem maintenance_gate_generation_bcnf : BCNF maintenance_gate_generation_contract :=
  bcnfCheck_sound maintenance_gate_generation_contract maintenance_gate_generation_bcnf_check

def maintenance_gate_head_contract : RelationContract where
  name := "maintenance_gate_head"
  attributes := ["singleton_id", "gate_generation", "updated_at"]
  declaredKeys := [["singleton_id"]]
  declaredFDs := [
    { determinant := ["singleton_id"], dependent := ["gate_generation", "updated_at"] }
  ]

theorem maintenance_gate_head_schema_well_formed :
    schemaWellFormedCheck maintenance_gate_head_contract = true := by
  native_decide

theorem maintenance_gate_head_candidate_keys_check :
    keysDetermineAllCheck maintenance_gate_head_contract = true := by
  native_decide

theorem maintenance_gate_head_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes maintenance_gate_head_contract :=
  keysDetermineAllCheck_sound maintenance_gate_head_contract
    maintenance_gate_head_candidate_keys_check

theorem maintenance_gate_head_candidate_keys_minimal_check :
    declaredKeysMinimalCheck maintenance_gate_head_contract = true := by
  native_decide

theorem maintenance_gate_head_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal maintenance_gate_head_contract :=
  declaredKeysMinimalCheck_sound maintenance_gate_head_contract
    maintenance_gate_head_candidate_keys_minimal_check

theorem maintenance_gate_head_closure_fixed_check :
    closureFixedPointCheck maintenance_gate_head_contract = true := by
  native_decide

theorem maintenance_gate_head_closure_reached_fixed_point :
    ClosureReachedFixedPoint maintenance_gate_head_contract :=
  closureFixedPointCheck_sound maintenance_gate_head_contract
    maintenance_gate_head_closure_fixed_check

theorem maintenance_gate_head_bcnf_check :
    bcnfCheck maintenance_gate_head_contract = true := by
  native_decide

theorem maintenance_gate_head_bcnf : BCNF maintenance_gate_head_contract :=
  bcnfCheck_sound maintenance_gate_head_contract maintenance_gate_head_bcnf_check

def maintenance_gate_owner_contract : RelationContract where
  name := "maintenance_gate_owner"
  attributes := ["owner_token", "gate_generation", "lease_expires_at"]
  declaredKeys := [["owner_token"]]
  declaredFDs := [
    { determinant := ["owner_token"], dependent := ["gate_generation", "lease_expires_at"] }
  ]

theorem maintenance_gate_owner_schema_well_formed :
    schemaWellFormedCheck maintenance_gate_owner_contract = true := by
  native_decide

theorem maintenance_gate_owner_candidate_keys_check :
    keysDetermineAllCheck maintenance_gate_owner_contract = true := by
  native_decide

theorem maintenance_gate_owner_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes maintenance_gate_owner_contract :=
  keysDetermineAllCheck_sound maintenance_gate_owner_contract
    maintenance_gate_owner_candidate_keys_check

theorem maintenance_gate_owner_candidate_keys_minimal_check :
    declaredKeysMinimalCheck maintenance_gate_owner_contract = true := by
  native_decide

theorem maintenance_gate_owner_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal maintenance_gate_owner_contract :=
  declaredKeysMinimalCheck_sound maintenance_gate_owner_contract
    maintenance_gate_owner_candidate_keys_minimal_check

theorem maintenance_gate_owner_closure_fixed_check :
    closureFixedPointCheck maintenance_gate_owner_contract = true := by
  native_decide

theorem maintenance_gate_owner_closure_reached_fixed_point :
    ClosureReachedFixedPoint maintenance_gate_owner_contract :=
  closureFixedPointCheck_sound maintenance_gate_owner_contract
    maintenance_gate_owner_closure_fixed_check

theorem maintenance_gate_owner_bcnf_check :
    bcnfCheck maintenance_gate_owner_contract = true := by
  native_decide

theorem maintenance_gate_owner_bcnf : BCNF maintenance_gate_owner_contract :=
  bcnfCheck_sound maintenance_gate_owner_contract maintenance_gate_owner_bcnf_check

def maintenance_gate_holder_contract : RelationContract where
  name := "maintenance_gate_holder"
  attributes := ["owner_token", "slot"]
  declaredKeys := [["slot"]]
  declaredFDs := [
    { determinant := ["slot"], dependent := ["owner_token"] }
  ]

theorem maintenance_gate_holder_schema_well_formed :
    schemaWellFormedCheck maintenance_gate_holder_contract = true := by
  native_decide

theorem maintenance_gate_holder_candidate_keys_check :
    keysDetermineAllCheck maintenance_gate_holder_contract = true := by
  native_decide

theorem maintenance_gate_holder_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes maintenance_gate_holder_contract :=
  keysDetermineAllCheck_sound maintenance_gate_holder_contract
    maintenance_gate_holder_candidate_keys_check

theorem maintenance_gate_holder_candidate_keys_minimal_check :
    declaredKeysMinimalCheck maintenance_gate_holder_contract = true := by
  native_decide

theorem maintenance_gate_holder_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal maintenance_gate_holder_contract :=
  declaredKeysMinimalCheck_sound maintenance_gate_holder_contract
    maintenance_gate_holder_candidate_keys_minimal_check

theorem maintenance_gate_holder_closure_fixed_check :
    closureFixedPointCheck maintenance_gate_holder_contract = true := by
  native_decide

theorem maintenance_gate_holder_closure_reached_fixed_point :
    ClosureReachedFixedPoint maintenance_gate_holder_contract :=
  closureFixedPointCheck_sound maintenance_gate_holder_contract
    maintenance_gate_holder_closure_fixed_check

theorem maintenance_gate_holder_bcnf_check :
    bcnfCheck maintenance_gate_holder_contract = true := by
  native_decide

theorem maintenance_gate_holder_bcnf : BCNF maintenance_gate_holder_contract :=
  bcnfCheck_sound maintenance_gate_holder_contract maintenance_gate_holder_bcnf_check

def maintenance_work_state_contract : RelationContract where
  name := "maintenance_work_state"
  attributes := ["singleton_id", "accumulated_work", "last_evaluated_at", "last_optimized_at"]
  declaredKeys := [["singleton_id"]]
  declaredFDs := [
    { determinant := ["singleton_id"], dependent := ["accumulated_work", "last_evaluated_at", "last_optimized_at"] }
  ]

theorem maintenance_work_state_schema_well_formed :
    schemaWellFormedCheck maintenance_work_state_contract = true := by
  native_decide

theorem maintenance_work_state_candidate_keys_check :
    keysDetermineAllCheck maintenance_work_state_contract = true := by
  native_decide

theorem maintenance_work_state_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes maintenance_work_state_contract :=
  keysDetermineAllCheck_sound maintenance_work_state_contract
    maintenance_work_state_candidate_keys_check

theorem maintenance_work_state_candidate_keys_minimal_check :
    declaredKeysMinimalCheck maintenance_work_state_contract = true := by
  native_decide

theorem maintenance_work_state_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal maintenance_work_state_contract :=
  declaredKeysMinimalCheck_sound maintenance_work_state_contract
    maintenance_work_state_candidate_keys_minimal_check

theorem maintenance_work_state_closure_fixed_check :
    closureFixedPointCheck maintenance_work_state_contract = true := by
  native_decide

theorem maintenance_work_state_closure_reached_fixed_point :
    ClosureReachedFixedPoint maintenance_work_state_contract :=
  closureFixedPointCheck_sound maintenance_work_state_contract
    maintenance_work_state_closure_fixed_check

theorem maintenance_work_state_bcnf_check :
    bcnfCheck maintenance_work_state_contract = true := by
  native_decide

theorem maintenance_work_state_bcnf : BCNF maintenance_work_state_contract :=
  bcnfCheck_sound maintenance_work_state_contract maintenance_work_state_bcnf_check

def source_working_build_contract : RelationContract where
  name := "source_working_build"
  attributes := ["slot", "build_id", "assigned_at"]
  declaredKeys := [["slot"], ["build_id"]]
  declaredFDs := [
    { determinant := ["slot"], dependent := ["build_id", "assigned_at"] },
    { determinant := ["build_id"], dependent := ["slot", "assigned_at"] }
  ]

theorem source_working_build_schema_well_formed :
    schemaWellFormedCheck source_working_build_contract = true := by
  native_decide

theorem source_working_build_candidate_keys_check :
    keysDetermineAllCheck source_working_build_contract = true := by
  native_decide

theorem source_working_build_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_working_build_contract :=
  keysDetermineAllCheck_sound source_working_build_contract
    source_working_build_candidate_keys_check

theorem source_working_build_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_working_build_contract = true := by
  native_decide

theorem source_working_build_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_working_build_contract :=
  declaredKeysMinimalCheck_sound source_working_build_contract
    source_working_build_candidate_keys_minimal_check

theorem source_working_build_closure_fixed_check :
    closureFixedPointCheck source_working_build_contract = true := by
  native_decide

theorem source_working_build_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_working_build_contract :=
  closureFixedPointCheck_sound source_working_build_contract
    source_working_build_closure_fixed_check

theorem source_working_build_bcnf_check :
    bcnfCheck source_working_build_contract = true := by
  native_decide

theorem source_working_build_bcnf : BCNF source_working_build_contract :=
  bcnfCheck_sound source_working_build_contract source_working_build_bcnf_check

def catalog_working_candidate_contract : RelationContract where
  name := "catalog_working_candidate"
  attributes := ["slot", "candidate_id", "assigned_at"]
  declaredKeys := [["slot"], ["candidate_id"]]
  declaredFDs := [
    { determinant := ["slot"], dependent := ["candidate_id", "assigned_at"] },
    { determinant := ["candidate_id"], dependent := ["slot", "assigned_at"] }
  ]

theorem catalog_working_candidate_schema_well_formed :
    schemaWellFormedCheck catalog_working_candidate_contract = true := by
  native_decide

theorem catalog_working_candidate_candidate_keys_check :
    keysDetermineAllCheck catalog_working_candidate_contract = true := by
  native_decide

theorem catalog_working_candidate_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes catalog_working_candidate_contract :=
  keysDetermineAllCheck_sound catalog_working_candidate_contract
    catalog_working_candidate_candidate_keys_check

theorem catalog_working_candidate_candidate_keys_minimal_check :
    declaredKeysMinimalCheck catalog_working_candidate_contract = true := by
  native_decide

theorem catalog_working_candidate_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal catalog_working_candidate_contract :=
  declaredKeysMinimalCheck_sound catalog_working_candidate_contract
    catalog_working_candidate_candidate_keys_minimal_check

theorem catalog_working_candidate_closure_fixed_check :
    closureFixedPointCheck catalog_working_candidate_contract = true := by
  native_decide

theorem catalog_working_candidate_closure_reached_fixed_point :
    ClosureReachedFixedPoint catalog_working_candidate_contract :=
  closureFixedPointCheck_sound catalog_working_candidate_contract
    catalog_working_candidate_closure_fixed_check

theorem catalog_working_candidate_bcnf_check :
    bcnfCheck catalog_working_candidate_contract = true := by
  native_decide

theorem catalog_working_candidate_bcnf : BCNF catalog_working_candidate_contract :=
  bcnfCheck_sound catalog_working_candidate_contract catalog_working_candidate_bcnf_check

def revision_allocator_contract : RelationContract where
  name := "revision_allocator"
  attributes := ["stream", "next_revision", "updated_at"]
  declaredKeys := [["stream"]]
  declaredFDs := [
    { determinant := ["stream"], dependent := ["next_revision", "updated_at"] }
  ]

theorem revision_allocator_schema_well_formed :
    schemaWellFormedCheck revision_allocator_contract = true := by
  native_decide

theorem revision_allocator_candidate_keys_check :
    keysDetermineAllCheck revision_allocator_contract = true := by
  native_decide

theorem revision_allocator_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes revision_allocator_contract :=
  keysDetermineAllCheck_sound revision_allocator_contract
    revision_allocator_candidate_keys_check

theorem revision_allocator_candidate_keys_minimal_check :
    declaredKeysMinimalCheck revision_allocator_contract = true := by
  native_decide

theorem revision_allocator_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal revision_allocator_contract :=
  declaredKeysMinimalCheck_sound revision_allocator_contract
    revision_allocator_candidate_keys_minimal_check

theorem revision_allocator_closure_fixed_check :
    closureFixedPointCheck revision_allocator_contract = true := by
  native_decide

theorem revision_allocator_closure_reached_fixed_point :
    ClosureReachedFixedPoint revision_allocator_contract :=
  closureFixedPointCheck_sound revision_allocator_contract
    revision_allocator_closure_fixed_check

theorem revision_allocator_bcnf_check :
    bcnfCheck revision_allocator_contract = true := by
  native_decide

theorem revision_allocator_bcnf : BCNF revision_allocator_contract :=
  bcnfCheck_sound revision_allocator_contract revision_allocator_bcnf_check

def identity_allocator_contract : RelationContract where
  name := "identity_allocator"
  attributes := ["stream", "next_id", "updated_at"]
  declaredKeys := [["stream"]]
  declaredFDs := [
    { determinant := ["stream"], dependent := ["next_id", "updated_at"] }
  ]

theorem identity_allocator_schema_well_formed :
    schemaWellFormedCheck identity_allocator_contract = true := by
  native_decide

theorem identity_allocator_candidate_keys_check :
    keysDetermineAllCheck identity_allocator_contract = true := by
  native_decide

theorem identity_allocator_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes identity_allocator_contract :=
  keysDetermineAllCheck_sound identity_allocator_contract
    identity_allocator_candidate_keys_check

theorem identity_allocator_candidate_keys_minimal_check :
    declaredKeysMinimalCheck identity_allocator_contract = true := by
  native_decide

theorem identity_allocator_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal identity_allocator_contract :=
  declaredKeysMinimalCheck_sound identity_allocator_contract
    identity_allocator_candidate_keys_minimal_check

theorem identity_allocator_closure_fixed_check :
    closureFixedPointCheck identity_allocator_contract = true := by
  native_decide

theorem identity_allocator_closure_reached_fixed_point :
    ClosureReachedFixedPoint identity_allocator_contract :=
  closureFixedPointCheck_sound identity_allocator_contract
    identity_allocator_closure_fixed_check

theorem identity_allocator_bcnf_check :
    bcnfCheck identity_allocator_contract = true := by
  native_decide

theorem identity_allocator_bcnf : BCNF identity_allocator_contract :=
  bcnfCheck_sound identity_allocator_contract identity_allocator_bcnf_check

def gallery_observation_allocator_contract : RelationContract where
  name := "gallery_observation_allocator"
  attributes := ["gallery_id", "next_observation_id", "updated_at"]
  declaredKeys := [["gallery_id"]]
  declaredFDs := [
    { determinant := ["gallery_id"], dependent := ["next_observation_id", "updated_at"] }
  ]

theorem gallery_observation_allocator_schema_well_formed :
    schemaWellFormedCheck gallery_observation_allocator_contract = true := by
  native_decide

theorem gallery_observation_allocator_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_allocator_contract = true := by
  native_decide

theorem gallery_observation_allocator_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_allocator_contract :=
  keysDetermineAllCheck_sound gallery_observation_allocator_contract
    gallery_observation_allocator_candidate_keys_check

theorem gallery_observation_allocator_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_allocator_contract = true := by
  native_decide

theorem gallery_observation_allocator_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_allocator_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_allocator_contract
    gallery_observation_allocator_candidate_keys_minimal_check

theorem gallery_observation_allocator_closure_fixed_check :
    closureFixedPointCheck gallery_observation_allocator_contract = true := by
  native_decide

theorem gallery_observation_allocator_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_allocator_contract :=
  closureFixedPointCheck_sound gallery_observation_allocator_contract
    gallery_observation_allocator_closure_fixed_check

theorem gallery_observation_allocator_bcnf_check :
    bcnfCheck gallery_observation_allocator_contract = true := by
  native_decide

theorem gallery_observation_allocator_bcnf : BCNF gallery_observation_allocator_contract :=
  bcnfCheck_sound gallery_observation_allocator_contract gallery_observation_allocator_bcnf_check

def gallery_observation_staging_contract : RelationContract where
  name := "gallery_observation_staging"
  attributes := ["staging_id", "build_id", "gallery_id", "observation_id", "state", "created_at", "sealed_at"]
  declaredKeys := [["staging_id"], ["build_id", "gallery_id"], ["gallery_id", "observation_id"]]
  declaredFDs := [
    { determinant := ["staging_id"], dependent := ["build_id", "gallery_id", "observation_id", "state", "created_at", "sealed_at"] },
    { determinant := ["build_id", "gallery_id"], dependent := ["staging_id", "observation_id", "state", "created_at", "sealed_at"] },
    { determinant := ["gallery_id", "observation_id"], dependent := ["staging_id", "build_id", "state", "created_at", "sealed_at"] }
  ]

theorem gallery_observation_staging_schema_well_formed :
    schemaWellFormedCheck gallery_observation_staging_contract = true := by
  native_decide

theorem gallery_observation_staging_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_staging_contract = true := by
  native_decide

theorem gallery_observation_staging_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_staging_contract :=
  keysDetermineAllCheck_sound gallery_observation_staging_contract
    gallery_observation_staging_candidate_keys_check

theorem gallery_observation_staging_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_staging_contract = true := by
  native_decide

theorem gallery_observation_staging_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_staging_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_staging_contract
    gallery_observation_staging_candidate_keys_minimal_check

theorem gallery_observation_staging_closure_fixed_check :
    closureFixedPointCheck gallery_observation_staging_contract = true := by
  native_decide

theorem gallery_observation_staging_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_staging_contract :=
  closureFixedPointCheck_sound gallery_observation_staging_contract
    gallery_observation_staging_closure_fixed_check

theorem gallery_observation_staging_bcnf_check :
    bcnfCheck gallery_observation_staging_contract = true := by
  native_decide

theorem gallery_observation_staging_bcnf : BCNF gallery_observation_staging_contract :=
  bcnfCheck_sound gallery_observation_staging_contract gallery_observation_staging_bcnf_check

def gallery_observation_staging_claim_contract : RelationContract where
  name := "gallery_observation_staging_claim"
  attributes := ["staging_id", "ingest_generation", "claim_generation", "updated_at"]
  declaredKeys := [["staging_id"]]
  declaredFDs := [
    { determinant := ["staging_id"], dependent := ["ingest_generation", "claim_generation", "updated_at"] }
  ]

theorem gallery_observation_staging_claim_schema_well_formed :
    schemaWellFormedCheck gallery_observation_staging_claim_contract = true := by
  native_decide

theorem gallery_observation_staging_claim_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_staging_claim_contract = true := by
  native_decide

theorem gallery_observation_staging_claim_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_staging_claim_contract :=
  keysDetermineAllCheck_sound gallery_observation_staging_claim_contract
    gallery_observation_staging_claim_candidate_keys_check

theorem gallery_observation_staging_claim_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_staging_claim_contract = true := by
  native_decide

theorem gallery_observation_staging_claim_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_staging_claim_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_staging_claim_contract
    gallery_observation_staging_claim_candidate_keys_minimal_check

theorem gallery_observation_staging_claim_closure_fixed_check :
    closureFixedPointCheck gallery_observation_staging_claim_contract = true := by
  native_decide

theorem gallery_observation_staging_claim_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_staging_claim_contract :=
  closureFixedPointCheck_sound gallery_observation_staging_claim_contract
    gallery_observation_staging_claim_closure_fixed_check

theorem gallery_observation_staging_claim_bcnf_check :
    bcnfCheck gallery_observation_staging_claim_contract = true := by
  native_decide

theorem gallery_observation_staging_claim_bcnf : BCNF gallery_observation_staging_claim_contract :=
  bcnfCheck_sound gallery_observation_staging_claim_contract gallery_observation_staging_claim_bcnf_check

def gallery_observation_staging_checkpoint_contract : RelationContract where
  name := "gallery_observation_staging_checkpoint"
  attributes := ["staging_id", "component", "level", "cursor", "regular_count", "state", "updated_at"]
  declaredKeys := [["staging_id", "component", "level"]]
  declaredFDs := [
    { determinant := ["staging_id", "component", "level"], dependent := ["cursor", "regular_count", "state", "updated_at"] }
  ]

theorem gallery_observation_staging_checkpoint_schema_well_formed :
    schemaWellFormedCheck gallery_observation_staging_checkpoint_contract = true := by
  native_decide

theorem gallery_observation_staging_checkpoint_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_staging_checkpoint_contract = true := by
  native_decide

theorem gallery_observation_staging_checkpoint_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_staging_checkpoint_contract :=
  keysDetermineAllCheck_sound gallery_observation_staging_checkpoint_contract
    gallery_observation_staging_checkpoint_candidate_keys_check

theorem gallery_observation_staging_checkpoint_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_staging_checkpoint_contract = true := by
  native_decide

theorem gallery_observation_staging_checkpoint_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_staging_checkpoint_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_staging_checkpoint_contract
    gallery_observation_staging_checkpoint_candidate_keys_minimal_check

theorem gallery_observation_staging_checkpoint_closure_fixed_check :
    closureFixedPointCheck gallery_observation_staging_checkpoint_contract = true := by
  native_decide

theorem gallery_observation_staging_checkpoint_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_staging_checkpoint_contract :=
  closureFixedPointCheck_sound gallery_observation_staging_checkpoint_contract
    gallery_observation_staging_checkpoint_closure_fixed_check

theorem gallery_observation_staging_checkpoint_bcnf_check :
    bcnfCheck gallery_observation_staging_checkpoint_contract = true := by
  native_decide

theorem gallery_observation_staging_checkpoint_bcnf : BCNF gallery_observation_staging_checkpoint_contract :=
  bcnfCheck_sound gallery_observation_staging_checkpoint_contract gallery_observation_staging_checkpoint_bcnf_check

def gallery_observation_staging_request_contract : RelationContract where
  name := "gallery_observation_staging_request"
  attributes := ["request_sha256"]
  declaredKeys := [["request_sha256"]]
  declaredFDs := [
  ]

theorem gallery_observation_staging_request_schema_well_formed :
    schemaWellFormedCheck gallery_observation_staging_request_contract = true := by
  native_decide

theorem gallery_observation_staging_request_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_staging_request_contract = true := by
  native_decide

theorem gallery_observation_staging_request_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_staging_request_contract :=
  keysDetermineAllCheck_sound gallery_observation_staging_request_contract
    gallery_observation_staging_request_candidate_keys_check

theorem gallery_observation_staging_request_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_staging_request_contract = true := by
  native_decide

theorem gallery_observation_staging_request_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_staging_request_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_staging_request_contract
    gallery_observation_staging_request_candidate_keys_minimal_check

theorem gallery_observation_staging_request_closure_fixed_check :
    closureFixedPointCheck gallery_observation_staging_request_contract = true := by
  native_decide

theorem gallery_observation_staging_request_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_staging_request_contract :=
  closureFixedPointCheck_sound gallery_observation_staging_request_contract
    gallery_observation_staging_request_closure_fixed_check

theorem gallery_observation_staging_request_bcnf_check :
    bcnfCheck gallery_observation_staging_request_contract = true := by
  native_decide

theorem gallery_observation_staging_request_bcnf : BCNF gallery_observation_staging_request_contract :=
  bcnfCheck_sound gallery_observation_staging_request_contract gallery_observation_staging_request_bcnf_check

def gallery_observation_staging_request_chunk_contract : RelationContract where
  name := "gallery_observation_staging_request_chunk"
  attributes := ["request_sha256", "position", "request_bytes"]
  declaredKeys := [["request_sha256", "position"]]
  declaredFDs := [
    { determinant := ["request_sha256", "position"], dependent := ["request_bytes"] }
  ]

theorem gallery_observation_staging_request_chunk_schema_well_formed :
    schemaWellFormedCheck gallery_observation_staging_request_chunk_contract = true := by
  native_decide

theorem gallery_observation_staging_request_chunk_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_staging_request_chunk_contract = true := by
  native_decide

theorem gallery_observation_staging_request_chunk_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_staging_request_chunk_contract :=
  keysDetermineAllCheck_sound gallery_observation_staging_request_chunk_contract
    gallery_observation_staging_request_chunk_candidate_keys_check

theorem gallery_observation_staging_request_chunk_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_staging_request_chunk_contract = true := by
  native_decide

theorem gallery_observation_staging_request_chunk_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_staging_request_chunk_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_staging_request_chunk_contract
    gallery_observation_staging_request_chunk_candidate_keys_minimal_check

theorem gallery_observation_staging_request_chunk_closure_fixed_check :
    closureFixedPointCheck gallery_observation_staging_request_chunk_contract = true := by
  native_decide

theorem gallery_observation_staging_request_chunk_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_staging_request_chunk_contract :=
  closureFixedPointCheck_sound gallery_observation_staging_request_chunk_contract
    gallery_observation_staging_request_chunk_closure_fixed_check

theorem gallery_observation_staging_request_chunk_bcnf_check :
    bcnfCheck gallery_observation_staging_request_chunk_contract = true := by
  native_decide

theorem gallery_observation_staging_request_chunk_bcnf : BCNF gallery_observation_staging_request_chunk_contract :=
  bcnfCheck_sound gallery_observation_staging_request_chunk_contract gallery_observation_staging_request_chunk_bcnf_check

def gallery_observation_staging_request_owner_contract : RelationContract where
  name := "gallery_observation_staging_request_owner"
  attributes := ["request_sha256", "staging_id"]
  declaredKeys := [["request_sha256"]]
  declaredFDs := [
    { determinant := ["request_sha256"], dependent := ["staging_id"] }
  ]

theorem gallery_observation_staging_request_owner_schema_well_formed :
    schemaWellFormedCheck gallery_observation_staging_request_owner_contract = true := by
  native_decide

theorem gallery_observation_staging_request_owner_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_staging_request_owner_contract = true := by
  native_decide

theorem gallery_observation_staging_request_owner_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_staging_request_owner_contract :=
  keysDetermineAllCheck_sound gallery_observation_staging_request_owner_contract
    gallery_observation_staging_request_owner_candidate_keys_check

theorem gallery_observation_staging_request_owner_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_staging_request_owner_contract = true := by
  native_decide

theorem gallery_observation_staging_request_owner_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_staging_request_owner_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_staging_request_owner_contract
    gallery_observation_staging_request_owner_candidate_keys_minimal_check

theorem gallery_observation_staging_request_owner_closure_fixed_check :
    closureFixedPointCheck gallery_observation_staging_request_owner_contract = true := by
  native_decide

theorem gallery_observation_staging_request_owner_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_staging_request_owner_contract :=
  closureFixedPointCheck_sound gallery_observation_staging_request_owner_contract
    gallery_observation_staging_request_owner_closure_fixed_check

theorem gallery_observation_staging_request_owner_bcnf_check :
    bcnfCheck gallery_observation_staging_request_owner_contract = true := by
  native_decide

theorem gallery_observation_staging_request_owner_bcnf : BCNF gallery_observation_staging_request_owner_contract :=
  bcnfCheck_sound gallery_observation_staging_request_owner_contract gallery_observation_staging_request_owner_bcnf_check

def gallery_observation_staging_request_predecessor_contract : RelationContract where
  name := "gallery_observation_staging_request_predecessor"
  attributes := ["request_sha256", "prior_request_sha256"]
  declaredKeys := [["request_sha256"], ["prior_request_sha256"]]
  declaredFDs := [
    { determinant := ["request_sha256"], dependent := ["prior_request_sha256"] },
    { determinant := ["prior_request_sha256"], dependent := ["request_sha256"] }
  ]

theorem gallery_observation_staging_request_predecessor_schema_well_formed :
    schemaWellFormedCheck gallery_observation_staging_request_predecessor_contract = true := by
  native_decide

theorem gallery_observation_staging_request_predecessor_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_staging_request_predecessor_contract = true := by
  native_decide

theorem gallery_observation_staging_request_predecessor_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_staging_request_predecessor_contract :=
  keysDetermineAllCheck_sound gallery_observation_staging_request_predecessor_contract
    gallery_observation_staging_request_predecessor_candidate_keys_check

theorem gallery_observation_staging_request_predecessor_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_staging_request_predecessor_contract = true := by
  native_decide

theorem gallery_observation_staging_request_predecessor_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_staging_request_predecessor_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_staging_request_predecessor_contract
    gallery_observation_staging_request_predecessor_candidate_keys_minimal_check

theorem gallery_observation_staging_request_predecessor_closure_fixed_check :
    closureFixedPointCheck gallery_observation_staging_request_predecessor_contract = true := by
  native_decide

theorem gallery_observation_staging_request_predecessor_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_staging_request_predecessor_contract :=
  closureFixedPointCheck_sound gallery_observation_staging_request_predecessor_contract
    gallery_observation_staging_request_predecessor_closure_fixed_check

theorem gallery_observation_staging_request_predecessor_bcnf_check :
    bcnfCheck gallery_observation_staging_request_predecessor_contract = true := by
  native_decide

theorem gallery_observation_staging_request_predecessor_bcnf : BCNF gallery_observation_staging_request_predecessor_contract :=
  bcnfCheck_sound gallery_observation_staging_request_predecessor_contract gallery_observation_staging_request_predecessor_bcnf_check

def gallery_observation_staging_page_request_contract : RelationContract where
  name := "gallery_observation_staging_page_request"
  attributes := ["request_sha256", "staging_id", "component", "level", "start_cursor", "terminal"]
  declaredKeys := [["request_sha256"], ["staging_id", "component", "level", "start_cursor"]]
  declaredFDs := [
    { determinant := ["request_sha256"], dependent := ["staging_id", "component", "level", "start_cursor", "terminal"] },
    { determinant := ["staging_id", "component", "level", "start_cursor"], dependent := ["request_sha256", "terminal"] }
  ]

theorem gallery_observation_staging_page_request_schema_well_formed :
    schemaWellFormedCheck gallery_observation_staging_page_request_contract = true := by
  native_decide

theorem gallery_observation_staging_page_request_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_staging_page_request_contract = true := by
  native_decide

theorem gallery_observation_staging_page_request_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_staging_page_request_contract :=
  keysDetermineAllCheck_sound gallery_observation_staging_page_request_contract
    gallery_observation_staging_page_request_candidate_keys_check

theorem gallery_observation_staging_page_request_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_staging_page_request_contract = true := by
  native_decide

theorem gallery_observation_staging_page_request_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_staging_page_request_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_staging_page_request_contract
    gallery_observation_staging_page_request_candidate_keys_minimal_check

theorem gallery_observation_staging_page_request_closure_fixed_check :
    closureFixedPointCheck gallery_observation_staging_page_request_contract = true := by
  native_decide

theorem gallery_observation_staging_page_request_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_staging_page_request_contract :=
  closureFixedPointCheck_sound gallery_observation_staging_page_request_contract
    gallery_observation_staging_page_request_closure_fixed_check

theorem gallery_observation_staging_page_request_bcnf_check :
    bcnfCheck gallery_observation_staging_page_request_contract = true := by
  native_decide

theorem gallery_observation_staging_page_request_bcnf : BCNF gallery_observation_staging_page_request_contract :=
  bcnfCheck_sound gallery_observation_staging_page_request_contract gallery_observation_staging_page_request_bcnf_check

def gallery_observation_staging_request_page_contract : RelationContract where
  name := "gallery_observation_staging_request_page"
  attributes := ["request_sha256", "page_sha256"]
  declaredKeys := [["request_sha256"]]
  declaredFDs := [
    { determinant := ["request_sha256"], dependent := ["page_sha256"] }
  ]

theorem gallery_observation_staging_request_page_schema_well_formed :
    schemaWellFormedCheck gallery_observation_staging_request_page_contract = true := by
  native_decide

theorem gallery_observation_staging_request_page_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_staging_request_page_contract = true := by
  native_decide

theorem gallery_observation_staging_request_page_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_staging_request_page_contract :=
  keysDetermineAllCheck_sound gallery_observation_staging_request_page_contract
    gallery_observation_staging_request_page_candidate_keys_check

theorem gallery_observation_staging_request_page_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_staging_request_page_contract = true := by
  native_decide

theorem gallery_observation_staging_request_page_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_staging_request_page_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_staging_request_page_contract
    gallery_observation_staging_request_page_candidate_keys_minimal_check

theorem gallery_observation_staging_request_page_closure_fixed_check :
    closureFixedPointCheck gallery_observation_staging_request_page_contract = true := by
  native_decide

theorem gallery_observation_staging_request_page_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_staging_request_page_contract :=
  closureFixedPointCheck_sound gallery_observation_staging_request_page_contract
    gallery_observation_staging_request_page_closure_fixed_check

theorem gallery_observation_staging_request_page_bcnf_check :
    bcnfCheck gallery_observation_staging_request_page_contract = true := by
  native_decide

theorem gallery_observation_staging_request_page_bcnf : BCNF gallery_observation_staging_request_page_contract :=
  bcnfCheck_sound gallery_observation_staging_request_page_contract gallery_observation_staging_request_page_bcnf_check

def gallery_observation_staging_receipt_contract : RelationContract where
  name := "gallery_observation_staging_receipt"
  attributes := ["staging_id", "component", "level", "request_sha256", "committed_at"]
  declaredKeys := [["staging_id", "component", "level"], ["request_sha256"]]
  declaredFDs := [
    { determinant := ["staging_id", "component", "level"], dependent := ["request_sha256", "committed_at"] },
    { determinant := ["request_sha256"], dependent := ["staging_id", "component", "level", "committed_at"] }
  ]

theorem gallery_observation_staging_receipt_schema_well_formed :
    schemaWellFormedCheck gallery_observation_staging_receipt_contract = true := by
  native_decide

theorem gallery_observation_staging_receipt_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_staging_receipt_contract = true := by
  native_decide

theorem gallery_observation_staging_receipt_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_staging_receipt_contract :=
  keysDetermineAllCheck_sound gallery_observation_staging_receipt_contract
    gallery_observation_staging_receipt_candidate_keys_check

theorem gallery_observation_staging_receipt_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_staging_receipt_contract = true := by
  native_decide

theorem gallery_observation_staging_receipt_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_staging_receipt_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_staging_receipt_contract
    gallery_observation_staging_receipt_candidate_keys_minimal_check

theorem gallery_observation_staging_receipt_closure_fixed_check :
    closureFixedPointCheck gallery_observation_staging_receipt_contract = true := by
  native_decide

theorem gallery_observation_staging_receipt_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_staging_receipt_contract :=
  closureFixedPointCheck_sound gallery_observation_staging_receipt_contract
    gallery_observation_staging_receipt_closure_fixed_check

theorem gallery_observation_staging_receipt_bcnf_check :
    bcnfCheck gallery_observation_staging_receipt_contract = true := by
  native_decide

theorem gallery_observation_staging_receipt_bcnf : BCNF gallery_observation_staging_receipt_contract :=
  bcnfCheck_sound gallery_observation_staging_receipt_contract gallery_observation_staging_receipt_bcnf_check

def gallery_observation_staging_frontier_contract : RelationContract where
  name := "gallery_observation_staging_frontier"
  attributes := ["request_sha256", "position"]
  declaredKeys := [["request_sha256"]]
  declaredFDs := [
    { determinant := ["request_sha256"], dependent := ["position"] }
  ]

theorem gallery_observation_staging_frontier_schema_well_formed :
    schemaWellFormedCheck gallery_observation_staging_frontier_contract = true := by
  native_decide

theorem gallery_observation_staging_frontier_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_staging_frontier_contract = true := by
  native_decide

theorem gallery_observation_staging_frontier_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_staging_frontier_contract :=
  keysDetermineAllCheck_sound gallery_observation_staging_frontier_contract
    gallery_observation_staging_frontier_candidate_keys_check

theorem gallery_observation_staging_frontier_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_staging_frontier_contract = true := by
  native_decide

theorem gallery_observation_staging_frontier_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_staging_frontier_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_staging_frontier_contract
    gallery_observation_staging_frontier_candidate_keys_minimal_check

theorem gallery_observation_staging_frontier_closure_fixed_check :
    closureFixedPointCheck gallery_observation_staging_frontier_contract = true := by
  native_decide

theorem gallery_observation_staging_frontier_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_staging_frontier_contract :=
  closureFixedPointCheck_sound gallery_observation_staging_frontier_contract
    gallery_observation_staging_frontier_closure_fixed_check

theorem gallery_observation_staging_frontier_bcnf_check :
    bcnfCheck gallery_observation_staging_frontier_contract = true := by
  native_decide

theorem gallery_observation_staging_frontier_bcnf : BCNF gallery_observation_staging_frontier_contract :=
  bcnfCheck_sound gallery_observation_staging_frontier_contract gallery_observation_staging_frontier_bcnf_check

def gallery_observation_staging_match_checkpoint_contract : RelationContract where
  name := "gallery_observation_staging_match_checkpoint"
  attributes := ["staging_id", "file_cursor_bytes", "matched_count", "state", "updated_at"]
  declaredKeys := [["staging_id"]]
  declaredFDs := [
    { determinant := ["staging_id"], dependent := ["file_cursor_bytes", "matched_count", "state", "updated_at"] }
  ]

theorem gallery_observation_staging_match_checkpoint_schema_well_formed :
    schemaWellFormedCheck gallery_observation_staging_match_checkpoint_contract = true := by
  native_decide

theorem gallery_observation_staging_match_checkpoint_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_staging_match_checkpoint_contract = true := by
  native_decide

theorem gallery_observation_staging_match_checkpoint_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_staging_match_checkpoint_contract :=
  keysDetermineAllCheck_sound gallery_observation_staging_match_checkpoint_contract
    gallery_observation_staging_match_checkpoint_candidate_keys_check

theorem gallery_observation_staging_match_checkpoint_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_staging_match_checkpoint_contract = true := by
  native_decide

theorem gallery_observation_staging_match_checkpoint_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_staging_match_checkpoint_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_staging_match_checkpoint_contract
    gallery_observation_staging_match_checkpoint_candidate_keys_minimal_check

theorem gallery_observation_staging_match_checkpoint_closure_fixed_check :
    closureFixedPointCheck gallery_observation_staging_match_checkpoint_contract = true := by
  native_decide

theorem gallery_observation_staging_match_checkpoint_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_staging_match_checkpoint_contract :=
  closureFixedPointCheck_sound gallery_observation_staging_match_checkpoint_contract
    gallery_observation_staging_match_checkpoint_closure_fixed_check

theorem gallery_observation_staging_match_checkpoint_bcnf_check :
    bcnfCheck gallery_observation_staging_match_checkpoint_contract = true := by
  native_decide

theorem gallery_observation_staging_match_checkpoint_bcnf : BCNF gallery_observation_staging_match_checkpoint_contract :=
  bcnfCheck_sound gallery_observation_staging_match_checkpoint_contract gallery_observation_staging_match_checkpoint_bcnf_check

def gallery_observation_staging_match_request_contract : RelationContract where
  name := "gallery_observation_staging_match_request"
  attributes := ["request_sha256", "staging_id", "start_file_cursor_bytes", "start_matched_count", "terminal"]
  declaredKeys := [["request_sha256"], ["staging_id", "start_matched_count"], ["staging_id", "start_file_cursor_bytes"]]
  declaredFDs := [
    { determinant := ["request_sha256"], dependent := ["staging_id", "start_file_cursor_bytes", "start_matched_count", "terminal"] },
    { determinant := ["staging_id", "start_matched_count"], dependent := ["request_sha256", "start_file_cursor_bytes", "terminal"] },
    { determinant := ["staging_id", "start_file_cursor_bytes"], dependent := ["request_sha256", "start_matched_count", "terminal"] }
  ]

theorem gallery_observation_staging_match_request_schema_well_formed :
    schemaWellFormedCheck gallery_observation_staging_match_request_contract = true := by
  native_decide

theorem gallery_observation_staging_match_request_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_staging_match_request_contract = true := by
  native_decide

theorem gallery_observation_staging_match_request_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_staging_match_request_contract :=
  keysDetermineAllCheck_sound gallery_observation_staging_match_request_contract
    gallery_observation_staging_match_request_candidate_keys_check

theorem gallery_observation_staging_match_request_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_staging_match_request_contract = true := by
  native_decide

theorem gallery_observation_staging_match_request_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_staging_match_request_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_staging_match_request_contract
    gallery_observation_staging_match_request_candidate_keys_minimal_check

theorem gallery_observation_staging_match_request_closure_fixed_check :
    closureFixedPointCheck gallery_observation_staging_match_request_contract = true := by
  native_decide

theorem gallery_observation_staging_match_request_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_staging_match_request_contract :=
  closureFixedPointCheck_sound gallery_observation_staging_match_request_contract
    gallery_observation_staging_match_request_closure_fixed_check

theorem gallery_observation_staging_match_request_bcnf_check :
    bcnfCheck gallery_observation_staging_match_request_contract = true := by
  native_decide

theorem gallery_observation_staging_match_request_bcnf : BCNF gallery_observation_staging_match_request_contract :=
  bcnfCheck_sound gallery_observation_staging_match_request_contract gallery_observation_staging_match_request_bcnf_check

def gallery_observation_staging_match_receipt_contract : RelationContract where
  name := "gallery_observation_staging_match_receipt"
  attributes := ["staging_id", "request_sha256", "committed_at"]
  declaredKeys := [["staging_id"], ["request_sha256"]]
  declaredFDs := [
    { determinant := ["staging_id"], dependent := ["request_sha256", "committed_at"] },
    { determinant := ["request_sha256"], dependent := ["staging_id", "committed_at"] }
  ]

theorem gallery_observation_staging_match_receipt_schema_well_formed :
    schemaWellFormedCheck gallery_observation_staging_match_receipt_contract = true := by
  native_decide

theorem gallery_observation_staging_match_receipt_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_staging_match_receipt_contract = true := by
  native_decide

theorem gallery_observation_staging_match_receipt_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_staging_match_receipt_contract :=
  keysDetermineAllCheck_sound gallery_observation_staging_match_receipt_contract
    gallery_observation_staging_match_receipt_candidate_keys_check

theorem gallery_observation_staging_match_receipt_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_staging_match_receipt_contract = true := by
  native_decide

theorem gallery_observation_staging_match_receipt_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_staging_match_receipt_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_staging_match_receipt_contract
    gallery_observation_staging_match_receipt_candidate_keys_minimal_check

theorem gallery_observation_staging_match_receipt_closure_fixed_check :
    closureFixedPointCheck gallery_observation_staging_match_receipt_contract = true := by
  native_decide

theorem gallery_observation_staging_match_receipt_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_staging_match_receipt_contract :=
  closureFixedPointCheck_sound gallery_observation_staging_match_receipt_contract
    gallery_observation_staging_match_receipt_closure_fixed_check

theorem gallery_observation_staging_match_receipt_bcnf_check :
    bcnfCheck gallery_observation_staging_match_receipt_contract = true := by
  native_decide

theorem gallery_observation_staging_match_receipt_bcnf : BCNF gallery_observation_staging_match_receipt_contract :=
  bcnfCheck_sound gallery_observation_staging_match_receipt_contract gallery_observation_staging_match_receipt_bcnf_check

def gallery_observation_staging_metadata_parser_contract : RelationContract where
  name := "gallery_observation_staging_metadata_parser"
  attributes := ["staging_id", "phase", "fixed_carry", "remaining_text_bytes", "utf8_tail", "gid", "title_byte_count", "comment_byte_count", "upload_account_byte_count", "upload_time", "download_time", "modified_time", "scan_observation_version", "source_file_count", "page_count", "updated_at"]
  declaredKeys := [["staging_id"]]
  declaredFDs := [
    { determinant := ["staging_id"], dependent := ["phase", "fixed_carry", "remaining_text_bytes", "utf8_tail", "gid", "title_byte_count", "comment_byte_count", "upload_account_byte_count", "upload_time", "download_time", "modified_time", "scan_observation_version", "source_file_count", "page_count", "updated_at"] }
  ]

theorem gallery_observation_staging_metadata_parser_schema_well_formed :
    schemaWellFormedCheck gallery_observation_staging_metadata_parser_contract = true := by
  native_decide

theorem gallery_observation_staging_metadata_parser_candidate_keys_check :
    keysDetermineAllCheck gallery_observation_staging_metadata_parser_contract = true := by
  native_decide

theorem gallery_observation_staging_metadata_parser_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_observation_staging_metadata_parser_contract :=
  keysDetermineAllCheck_sound gallery_observation_staging_metadata_parser_contract
    gallery_observation_staging_metadata_parser_candidate_keys_check

theorem gallery_observation_staging_metadata_parser_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_observation_staging_metadata_parser_contract = true := by
  native_decide

theorem gallery_observation_staging_metadata_parser_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_observation_staging_metadata_parser_contract :=
  declaredKeysMinimalCheck_sound gallery_observation_staging_metadata_parser_contract
    gallery_observation_staging_metadata_parser_candidate_keys_minimal_check

theorem gallery_observation_staging_metadata_parser_closure_fixed_check :
    closureFixedPointCheck gallery_observation_staging_metadata_parser_contract = true := by
  native_decide

theorem gallery_observation_staging_metadata_parser_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_observation_staging_metadata_parser_contract :=
  closureFixedPointCheck_sound gallery_observation_staging_metadata_parser_contract
    gallery_observation_staging_metadata_parser_closure_fixed_check

theorem gallery_observation_staging_metadata_parser_bcnf_check :
    bcnfCheck gallery_observation_staging_metadata_parser_contract = true := by
  native_decide

theorem gallery_observation_staging_metadata_parser_bcnf : BCNF gallery_observation_staging_metadata_parser_contract :=
  bcnfCheck_sound gallery_observation_staging_metadata_parser_contract gallery_observation_staging_metadata_parser_bcnf_check

def canonical_value_upload_contract : RelationContract where
  name := "canonical_value_upload"
  attributes := ["generation", "value_sha256"]
  declaredKeys := [["generation", "value_sha256"]]
  declaredFDs := [
  ]

theorem canonical_value_upload_schema_well_formed :
    schemaWellFormedCheck canonical_value_upload_contract = true := by
  native_decide

theorem canonical_value_upload_candidate_keys_check :
    keysDetermineAllCheck canonical_value_upload_contract = true := by
  native_decide

theorem canonical_value_upload_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes canonical_value_upload_contract :=
  keysDetermineAllCheck_sound canonical_value_upload_contract
    canonical_value_upload_candidate_keys_check

theorem canonical_value_upload_candidate_keys_minimal_check :
    declaredKeysMinimalCheck canonical_value_upload_contract = true := by
  native_decide

theorem canonical_value_upload_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal canonical_value_upload_contract :=
  declaredKeysMinimalCheck_sound canonical_value_upload_contract
    canonical_value_upload_candidate_keys_minimal_check

theorem canonical_value_upload_closure_fixed_check :
    closureFixedPointCheck canonical_value_upload_contract = true := by
  native_decide

theorem canonical_value_upload_closure_reached_fixed_point :
    ClosureReachedFixedPoint canonical_value_upload_contract :=
  closureFixedPointCheck_sound canonical_value_upload_contract
    canonical_value_upload_closure_fixed_check

theorem canonical_value_upload_bcnf_check :
    bcnfCheck canonical_value_upload_contract = true := by
  native_decide

theorem canonical_value_upload_bcnf : BCNF canonical_value_upload_contract :=
  bcnfCheck_sound canonical_value_upload_contract canonical_value_upload_bcnf_check

def download_request_contract : RelationContract where
  name := "download_request"
  attributes := ["gid", "url", "request_token", "requested_at"]
  declaredKeys := [["gid"], ["request_token"]]
  declaredFDs := [
    { determinant := ["gid"], dependent := ["url", "request_token", "requested_at"] },
    { determinant := ["request_token"], dependent := ["gid", "url", "requested_at"] }
  ]

theorem download_request_schema_well_formed :
    schemaWellFormedCheck download_request_contract = true := by
  native_decide

theorem download_request_candidate_keys_check :
    keysDetermineAllCheck download_request_contract = true := by
  native_decide

theorem download_request_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes download_request_contract :=
  keysDetermineAllCheck_sound download_request_contract
    download_request_candidate_keys_check

theorem download_request_candidate_keys_minimal_check :
    declaredKeysMinimalCheck download_request_contract = true := by
  native_decide

theorem download_request_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal download_request_contract :=
  declaredKeysMinimalCheck_sound download_request_contract
    download_request_candidate_keys_minimal_check

theorem download_request_closure_fixed_check :
    closureFixedPointCheck download_request_contract = true := by
  native_decide

theorem download_request_closure_reached_fixed_point :
    ClosureReachedFixedPoint download_request_contract :=
  closureFixedPointCheck_sound download_request_contract
    download_request_closure_fixed_check

theorem download_request_bcnf_check :
    bcnfCheck download_request_contract = true := by
  native_decide

theorem download_request_bcnf : BCNF download_request_contract :=
  bcnfCheck_sound download_request_contract download_request_bcnf_check

def deletion_request_attempt_contract : RelationContract where
  name := "deletion_request_attempt"
  attributes := ["request_token", "gid", "requested_at"]
  declaredKeys := [["request_token"]]
  declaredFDs := [
    { determinant := ["request_token"], dependent := ["gid", "requested_at"] }
  ]

theorem deletion_request_attempt_schema_well_formed :
    schemaWellFormedCheck deletion_request_attempt_contract = true := by
  native_decide

theorem deletion_request_attempt_candidate_keys_check :
    keysDetermineAllCheck deletion_request_attempt_contract = true := by
  native_decide

theorem deletion_request_attempt_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes deletion_request_attempt_contract :=
  keysDetermineAllCheck_sound deletion_request_attempt_contract
    deletion_request_attempt_candidate_keys_check

theorem deletion_request_attempt_candidate_keys_minimal_check :
    declaredKeysMinimalCheck deletion_request_attempt_contract = true := by
  native_decide

theorem deletion_request_attempt_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal deletion_request_attempt_contract :=
  declaredKeysMinimalCheck_sound deletion_request_attempt_contract
    deletion_request_attempt_candidate_keys_minimal_check

theorem deletion_request_attempt_closure_fixed_check :
    closureFixedPointCheck deletion_request_attempt_contract = true := by
  native_decide

theorem deletion_request_attempt_closure_reached_fixed_point :
    ClosureReachedFixedPoint deletion_request_attempt_contract :=
  closureFixedPointCheck_sound deletion_request_attempt_contract
    deletion_request_attempt_closure_fixed_check

theorem deletion_request_attempt_bcnf_check :
    bcnfCheck deletion_request_attempt_contract = true := by
  native_decide

theorem deletion_request_attempt_bcnf : BCNF deletion_request_attempt_contract :=
  bcnfCheck_sound deletion_request_attempt_contract deletion_request_attempt_bcnf_check

def deletion_request_url_contract : RelationContract where
  name := "deletion_request_url"
  attributes := ["request_token", "url"]
  declaredKeys := [["request_token"]]
  declaredFDs := [
    { determinant := ["request_token"], dependent := ["url"] }
  ]

theorem deletion_request_url_schema_well_formed :
    schemaWellFormedCheck deletion_request_url_contract = true := by
  native_decide

theorem deletion_request_url_candidate_keys_check :
    keysDetermineAllCheck deletion_request_url_contract = true := by
  native_decide

theorem deletion_request_url_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes deletion_request_url_contract :=
  keysDetermineAllCheck_sound deletion_request_url_contract
    deletion_request_url_candidate_keys_check

theorem deletion_request_url_candidate_keys_minimal_check :
    declaredKeysMinimalCheck deletion_request_url_contract = true := by
  native_decide

theorem deletion_request_url_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal deletion_request_url_contract :=
  declaredKeysMinimalCheck_sound deletion_request_url_contract
    deletion_request_url_candidate_keys_minimal_check

theorem deletion_request_url_closure_fixed_check :
    closureFixedPointCheck deletion_request_url_contract = true := by
  native_decide

theorem deletion_request_url_closure_reached_fixed_point :
    ClosureReachedFixedPoint deletion_request_url_contract :=
  closureFixedPointCheck_sound deletion_request_url_contract
    deletion_request_url_closure_fixed_check

theorem deletion_request_url_bcnf_check :
    bcnfCheck deletion_request_url_contract = true := by
  native_decide

theorem deletion_request_url_bcnf : BCNF deletion_request_url_contract :=
  bcnfCheck_sound deletion_request_url_contract deletion_request_url_bcnf_check

def deletion_request_head_contract : RelationContract where
  name := "deletion_request_head"
  attributes := ["gid", "request_token"]
  declaredKeys := [["gid"], ["request_token"]]
  declaredFDs := [
    { determinant := ["gid"], dependent := ["request_token"] },
    { determinant := ["request_token"], dependent := ["gid"] }
  ]

theorem deletion_request_head_schema_well_formed :
    schemaWellFormedCheck deletion_request_head_contract = true := by
  native_decide

theorem deletion_request_head_candidate_keys_check :
    keysDetermineAllCheck deletion_request_head_contract = true := by
  native_decide

theorem deletion_request_head_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes deletion_request_head_contract :=
  keysDetermineAllCheck_sound deletion_request_head_contract
    deletion_request_head_candidate_keys_check

theorem deletion_request_head_candidate_keys_minimal_check :
    declaredKeysMinimalCheck deletion_request_head_contract = true := by
  native_decide

theorem deletion_request_head_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal deletion_request_head_contract :=
  declaredKeysMinimalCheck_sound deletion_request_head_contract
    deletion_request_head_candidate_keys_minimal_check

theorem deletion_request_head_closure_fixed_check :
    closureFixedPointCheck deletion_request_head_contract = true := by
  native_decide

theorem deletion_request_head_closure_reached_fixed_point :
    ClosureReachedFixedPoint deletion_request_head_contract :=
  closureFixedPointCheck_sound deletion_request_head_contract
    deletion_request_head_closure_fixed_check

theorem deletion_request_head_bcnf_check :
    bcnfCheck deletion_request_head_contract = true := by
  native_decide

theorem deletion_request_head_bcnf : BCNF deletion_request_head_contract :=
  bcnfCheck_sound deletion_request_head_contract deletion_request_head_bcnf_check

def removed_gid_contract : RelationContract where
  name := "removed_gid"
  attributes := ["gid"]
  declaredKeys := [["gid"]]
  declaredFDs := [
  ]

theorem removed_gid_schema_well_formed :
    schemaWellFormedCheck removed_gid_contract = true := by
  native_decide

theorem removed_gid_candidate_keys_check :
    keysDetermineAllCheck removed_gid_contract = true := by
  native_decide

theorem removed_gid_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes removed_gid_contract :=
  keysDetermineAllCheck_sound removed_gid_contract
    removed_gid_candidate_keys_check

theorem removed_gid_candidate_keys_minimal_check :
    declaredKeysMinimalCheck removed_gid_contract = true := by
  native_decide

theorem removed_gid_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal removed_gid_contract :=
  declaredKeysMinimalCheck_sound removed_gid_contract
    removed_gid_candidate_keys_minimal_check

theorem removed_gid_closure_fixed_check :
    closureFixedPointCheck removed_gid_contract = true := by
  native_decide

theorem removed_gid_closure_reached_fixed_point :
    ClosureReachedFixedPoint removed_gid_contract :=
  closureFixedPointCheck_sound removed_gid_contract
    removed_gid_closure_fixed_check

theorem removed_gid_bcnf_check :
    bcnfCheck removed_gid_contract = true := by
  native_decide

theorem removed_gid_bcnf : BCNF removed_gid_contract :=
  bcnfCheck_sound removed_gid_contract removed_gid_bcnf_check

def gallery_redownload_state_contract : RelationContract where
  name := "gallery_redownload_state"
  attributes := ["gallery_id", "redownload_at", "through_source_revision", "updated_at"]
  declaredKeys := [["gallery_id"]]
  declaredFDs := [
    { determinant := ["gallery_id"], dependent := ["redownload_at", "through_source_revision", "updated_at"] }
  ]

theorem gallery_redownload_state_schema_well_formed :
    schemaWellFormedCheck gallery_redownload_state_contract = true := by
  native_decide

theorem gallery_redownload_state_candidate_keys_check :
    keysDetermineAllCheck gallery_redownload_state_contract = true := by
  native_decide

theorem gallery_redownload_state_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes gallery_redownload_state_contract :=
  keysDetermineAllCheck_sound gallery_redownload_state_contract
    gallery_redownload_state_candidate_keys_check

theorem gallery_redownload_state_candidate_keys_minimal_check :
    declaredKeysMinimalCheck gallery_redownload_state_contract = true := by
  native_decide

theorem gallery_redownload_state_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal gallery_redownload_state_contract :=
  declaredKeysMinimalCheck_sound gallery_redownload_state_contract
    gallery_redownload_state_candidate_keys_minimal_check

theorem gallery_redownload_state_closure_fixed_check :
    closureFixedPointCheck gallery_redownload_state_contract = true := by
  native_decide

theorem gallery_redownload_state_closure_reached_fixed_point :
    ClosureReachedFixedPoint gallery_redownload_state_contract :=
  closureFixedPointCheck_sound gallery_redownload_state_contract
    gallery_redownload_state_closure_fixed_check

theorem gallery_redownload_state_bcnf_check :
    bcnfCheck gallery_redownload_state_contract = true := by
  native_decide

theorem gallery_redownload_state_bcnf : BCNF gallery_redownload_state_contract :=
  bcnfCheck_sound gallery_redownload_state_contract gallery_redownload_state_bcnf_check

def operational_policy_contract : RelationContract where
  name := "operational_policy"
  attributes := ["operational_policy_id", "operational_schema_version", "algorithm_version", "max_batch_rows"]
  declaredKeys := [["operational_policy_id"], ["operational_schema_version", "algorithm_version", "max_batch_rows"]]
  declaredFDs := [
    { determinant := ["operational_policy_id"], dependent := ["operational_schema_version", "algorithm_version", "max_batch_rows"] },
    { determinant := ["operational_schema_version", "algorithm_version", "max_batch_rows"], dependent := ["operational_policy_id"] }
  ]

theorem operational_policy_schema_well_formed :
    schemaWellFormedCheck operational_policy_contract = true := by
  native_decide

theorem operational_policy_candidate_keys_check :
    keysDetermineAllCheck operational_policy_contract = true := by
  native_decide

theorem operational_policy_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes operational_policy_contract :=
  keysDetermineAllCheck_sound operational_policy_contract
    operational_policy_candidate_keys_check

theorem operational_policy_candidate_keys_minimal_check :
    declaredKeysMinimalCheck operational_policy_contract = true := by
  native_decide

theorem operational_policy_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal operational_policy_contract :=
  declaredKeysMinimalCheck_sound operational_policy_contract
    operational_policy_candidate_keys_minimal_check

theorem operational_policy_closure_fixed_check :
    closureFixedPointCheck operational_policy_contract = true := by
  native_decide

theorem operational_policy_closure_reached_fixed_point :
    ClosureReachedFixedPoint operational_policy_contract :=
  closureFixedPointCheck_sound operational_policy_contract
    operational_policy_closure_fixed_check

theorem operational_policy_bcnf_check :
    bcnfCheck operational_policy_contract = true := by
  native_decide

theorem operational_policy_bcnf : BCNF operational_policy_contract :=
  bcnfCheck_sound operational_policy_contract operational_policy_bcnf_check

def operational_preparation_contract : RelationContract where
  name := "operational_preparation"
  attributes := ["preparation_id", "build_id", "deletion_request_generation", "operational_policy_id", "state", "prepared_at", "completed_at"]
  declaredKeys := [["preparation_id"], ["build_id", "deletion_request_generation", "operational_policy_id"]]
  declaredFDs := [
    { determinant := ["preparation_id"], dependent := ["build_id", "deletion_request_generation", "operational_policy_id", "state", "prepared_at", "completed_at"] },
    { determinant := ["build_id", "deletion_request_generation", "operational_policy_id"], dependent := ["preparation_id", "state", "prepared_at", "completed_at"] }
  ]

theorem operational_preparation_schema_well_formed :
    schemaWellFormedCheck operational_preparation_contract = true := by
  native_decide

theorem operational_preparation_candidate_keys_check :
    keysDetermineAllCheck operational_preparation_contract = true := by
  native_decide

theorem operational_preparation_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes operational_preparation_contract :=
  keysDetermineAllCheck_sound operational_preparation_contract
    operational_preparation_candidate_keys_check

theorem operational_preparation_candidate_keys_minimal_check :
    declaredKeysMinimalCheck operational_preparation_contract = true := by
  native_decide

theorem operational_preparation_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal operational_preparation_contract :=
  declaredKeysMinimalCheck_sound operational_preparation_contract
    operational_preparation_candidate_keys_minimal_check

theorem operational_preparation_closure_fixed_check :
    closureFixedPointCheck operational_preparation_contract = true := by
  native_decide

theorem operational_preparation_closure_reached_fixed_point :
    ClosureReachedFixedPoint operational_preparation_contract :=
  closureFixedPointCheck_sound operational_preparation_contract
    operational_preparation_closure_fixed_check

theorem operational_preparation_bcnf_check :
    bcnfCheck operational_preparation_contract = true := by
  native_decide

theorem operational_preparation_bcnf : BCNF operational_preparation_contract :=
  bcnfCheck_sound operational_preparation_contract operational_preparation_bcnf_check

def operational_preparation_checkpoint_contract : RelationContract where
  name := "operational_preparation_checkpoint"
  attributes := ["preparation_id", "phase", "generation", "cursor_bytes", "processed_count", "chain_sha256", "state", "updated_at"]
  declaredKeys := [["preparation_id", "phase"]]
  declaredFDs := [
    { determinant := ["preparation_id", "phase"], dependent := ["generation", "cursor_bytes", "processed_count", "chain_sha256", "state", "updated_at"] }
  ]

theorem operational_preparation_checkpoint_schema_well_formed :
    schemaWellFormedCheck operational_preparation_checkpoint_contract = true := by
  native_decide

theorem operational_preparation_checkpoint_candidate_keys_check :
    keysDetermineAllCheck operational_preparation_checkpoint_contract = true := by
  native_decide

theorem operational_preparation_checkpoint_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes operational_preparation_checkpoint_contract :=
  keysDetermineAllCheck_sound operational_preparation_checkpoint_contract
    operational_preparation_checkpoint_candidate_keys_check

theorem operational_preparation_checkpoint_candidate_keys_minimal_check :
    declaredKeysMinimalCheck operational_preparation_checkpoint_contract = true := by
  native_decide

theorem operational_preparation_checkpoint_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal operational_preparation_checkpoint_contract :=
  declaredKeysMinimalCheck_sound operational_preparation_checkpoint_contract
    operational_preparation_checkpoint_candidate_keys_minimal_check

theorem operational_preparation_checkpoint_closure_fixed_check :
    closureFixedPointCheck operational_preparation_checkpoint_contract = true := by
  native_decide

theorem operational_preparation_checkpoint_closure_reached_fixed_point :
    ClosureReachedFixedPoint operational_preparation_checkpoint_contract :=
  closureFixedPointCheck_sound operational_preparation_checkpoint_contract
    operational_preparation_checkpoint_closure_fixed_check

theorem operational_preparation_checkpoint_bcnf_check :
    bcnfCheck operational_preparation_checkpoint_contract = true := by
  native_decide

theorem operational_preparation_checkpoint_bcnf : BCNF operational_preparation_checkpoint_contract :=
  bcnfCheck_sound operational_preparation_checkpoint_contract operational_preparation_checkpoint_bcnf_check

def operational_preparation_batch_receipt_contract : RelationContract where
  name := "operational_preparation_batch_receipt"
  attributes := ["preparation_id", "phase", "batch_key", "start_cursor", "next_cursor", "input_sha256", "output_sha256", "row_count", "committed_generation", "committed_at"]
  declaredKeys := [["preparation_id", "phase", "batch_key"], ["preparation_id", "phase", "start_cursor"]]
  declaredFDs := [
    { determinant := ["preparation_id", "phase", "batch_key"], dependent := ["start_cursor", "next_cursor", "input_sha256", "output_sha256", "row_count", "committed_generation", "committed_at"] },
    { determinant := ["preparation_id", "phase", "start_cursor"], dependent := ["batch_key", "next_cursor", "input_sha256", "output_sha256", "row_count", "committed_generation", "committed_at"] }
  ]

theorem operational_preparation_batch_receipt_schema_well_formed :
    schemaWellFormedCheck operational_preparation_batch_receipt_contract = true := by
  native_decide

theorem operational_preparation_batch_receipt_candidate_keys_check :
    keysDetermineAllCheck operational_preparation_batch_receipt_contract = true := by
  native_decide

theorem operational_preparation_batch_receipt_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes operational_preparation_batch_receipt_contract :=
  keysDetermineAllCheck_sound operational_preparation_batch_receipt_contract
    operational_preparation_batch_receipt_candidate_keys_check

theorem operational_preparation_batch_receipt_candidate_keys_minimal_check :
    declaredKeysMinimalCheck operational_preparation_batch_receipt_contract = true := by
  native_decide

theorem operational_preparation_batch_receipt_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal operational_preparation_batch_receipt_contract :=
  declaredKeysMinimalCheck_sound operational_preparation_batch_receipt_contract
    operational_preparation_batch_receipt_candidate_keys_minimal_check

theorem operational_preparation_batch_receipt_closure_fixed_check :
    closureFixedPointCheck operational_preparation_batch_receipt_contract = true := by
  native_decide

theorem operational_preparation_batch_receipt_closure_reached_fixed_point :
    ClosureReachedFixedPoint operational_preparation_batch_receipt_contract :=
  closureFixedPointCheck_sound operational_preparation_batch_receipt_contract
    operational_preparation_batch_receipt_closure_fixed_check

theorem operational_preparation_batch_receipt_bcnf_check :
    bcnfCheck operational_preparation_batch_receipt_contract = true := by
  native_decide

theorem operational_preparation_batch_receipt_bcnf : BCNF operational_preparation_batch_receipt_contract :=
  bcnfCheck_sound operational_preparation_batch_receipt_contract operational_preparation_batch_receipt_bcnf_check

def operational_activation_contract : RelationContract where
  name := "operational_activation"
  attributes := ["source_revision", "preparation_id", "operational_policy_id", "activated_at"]
  declaredKeys := [["source_revision"], ["preparation_id"]]
  declaredFDs := [
    { determinant := ["source_revision"], dependent := ["preparation_id", "operational_policy_id", "activated_at"] },
    { determinant := ["preparation_id"], dependent := ["source_revision", "operational_policy_id", "activated_at"] }
  ]

theorem operational_activation_schema_well_formed :
    schemaWellFormedCheck operational_activation_contract = true := by
  native_decide

theorem operational_activation_candidate_keys_check :
    keysDetermineAllCheck operational_activation_contract = true := by
  native_decide

theorem operational_activation_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes operational_activation_contract :=
  keysDetermineAllCheck_sound operational_activation_contract
    operational_activation_candidate_keys_check

theorem operational_activation_candidate_keys_minimal_check :
    declaredKeysMinimalCheck operational_activation_contract = true := by
  native_decide

theorem operational_activation_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal operational_activation_contract :=
  declaredKeysMinimalCheck_sound operational_activation_contract
    operational_activation_candidate_keys_minimal_check

theorem operational_activation_closure_fixed_check :
    closureFixedPointCheck operational_activation_contract = true := by
  native_decide

theorem operational_activation_closure_reached_fixed_point :
    ClosureReachedFixedPoint operational_activation_contract :=
  closureFixedPointCheck_sound operational_activation_contract
    operational_activation_closure_fixed_check

theorem operational_activation_bcnf_check :
    bcnfCheck operational_activation_contract = true := by
  native_decide

theorem operational_activation_bcnf : BCNF operational_activation_contract :=
  bcnfCheck_sound operational_activation_contract operational_activation_bcnf_check

def operational_event_contract : RelationContract where
  name := "operational_event"
  attributes := ["event_id", "source_revision", "sequence_no", "event_type", "event_sha256", "created_at"]
  declaredKeys := [["event_id"], ["source_revision", "sequence_no"]]
  declaredFDs := [
    { determinant := ["event_id"], dependent := ["source_revision", "sequence_no", "event_type", "event_sha256", "created_at"] },
    { determinant := ["source_revision", "sequence_no"], dependent := ["event_id", "event_type", "event_sha256", "created_at"] }
  ]

theorem operational_event_schema_well_formed :
    schemaWellFormedCheck operational_event_contract = true := by
  native_decide

theorem operational_event_candidate_keys_check :
    keysDetermineAllCheck operational_event_contract = true := by
  native_decide

theorem operational_event_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes operational_event_contract :=
  keysDetermineAllCheck_sound operational_event_contract
    operational_event_candidate_keys_check

theorem operational_event_candidate_keys_minimal_check :
    declaredKeysMinimalCheck operational_event_contract = true := by
  native_decide

theorem operational_event_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal operational_event_contract :=
  declaredKeysMinimalCheck_sound operational_event_contract
    operational_event_candidate_keys_minimal_check

theorem operational_event_closure_fixed_check :
    closureFixedPointCheck operational_event_contract = true := by
  native_decide

theorem operational_event_closure_reached_fixed_point :
    ClosureReachedFixedPoint operational_event_contract :=
  closureFixedPointCheck_sound operational_event_contract
    operational_event_closure_fixed_check

theorem operational_event_bcnf_check :
    bcnfCheck operational_event_contract = true := by
  native_decide

theorem operational_event_bcnf : BCNF operational_event_contract :=
  bcnfCheck_sound operational_event_contract operational_event_bcnf_check

def operational_removed_gid_event_contract : RelationContract where
  name := "operational_removed_gid_event"
  attributes := ["event_id", "gid", "request_token"]
  declaredKeys := [["event_id"], ["request_token"]]
  declaredFDs := [
    { determinant := ["event_id"], dependent := ["gid", "request_token"] },
    { determinant := ["request_token"], dependent := ["event_id", "gid"] }
  ]

theorem operational_removed_gid_event_schema_well_formed :
    schemaWellFormedCheck operational_removed_gid_event_contract = true := by
  native_decide

theorem operational_removed_gid_event_candidate_keys_check :
    keysDetermineAllCheck operational_removed_gid_event_contract = true := by
  native_decide

theorem operational_removed_gid_event_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes operational_removed_gid_event_contract :=
  keysDetermineAllCheck_sound operational_removed_gid_event_contract
    operational_removed_gid_event_candidate_keys_check

theorem operational_removed_gid_event_candidate_keys_minimal_check :
    declaredKeysMinimalCheck operational_removed_gid_event_contract = true := by
  native_decide

theorem operational_removed_gid_event_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal operational_removed_gid_event_contract :=
  declaredKeysMinimalCheck_sound operational_removed_gid_event_contract
    operational_removed_gid_event_candidate_keys_minimal_check

theorem operational_removed_gid_event_closure_fixed_check :
    closureFixedPointCheck operational_removed_gid_event_contract = true := by
  native_decide

theorem operational_removed_gid_event_closure_reached_fixed_point :
    ClosureReachedFixedPoint operational_removed_gid_event_contract :=
  closureFixedPointCheck_sound operational_removed_gid_event_contract
    operational_removed_gid_event_closure_fixed_check

theorem operational_removed_gid_event_bcnf_check :
    bcnfCheck operational_removed_gid_event_contract = true := by
  native_decide

theorem operational_removed_gid_event_bcnf : BCNF operational_removed_gid_event_contract :=
  bcnfCheck_sound operational_removed_gid_event_contract operational_removed_gid_event_bcnf_check

def operational_deletion_consumption_event_contract : RelationContract where
  name := "operational_deletion_consumption_event"
  attributes := ["event_id", "gid", "deletion_request_token"]
  declaredKeys := [["event_id"], ["deletion_request_token"]]
  declaredFDs := [
    { determinant := ["event_id"], dependent := ["gid", "deletion_request_token"] },
    { determinant := ["deletion_request_token"], dependent := ["event_id", "gid"] }
  ]

theorem operational_deletion_consumption_event_schema_well_formed :
    schemaWellFormedCheck operational_deletion_consumption_event_contract = true := by
  native_decide

theorem operational_deletion_consumption_event_candidate_keys_check :
    keysDetermineAllCheck operational_deletion_consumption_event_contract = true := by
  native_decide

theorem operational_deletion_consumption_event_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes operational_deletion_consumption_event_contract :=
  keysDetermineAllCheck_sound operational_deletion_consumption_event_contract
    operational_deletion_consumption_event_candidate_keys_check

theorem operational_deletion_consumption_event_candidate_keys_minimal_check :
    declaredKeysMinimalCheck operational_deletion_consumption_event_contract = true := by
  native_decide

theorem operational_deletion_consumption_event_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal operational_deletion_consumption_event_contract :=
  declaredKeysMinimalCheck_sound operational_deletion_consumption_event_contract
    operational_deletion_consumption_event_candidate_keys_minimal_check

theorem operational_deletion_consumption_event_closure_fixed_check :
    closureFixedPointCheck operational_deletion_consumption_event_contract = true := by
  native_decide

theorem operational_deletion_consumption_event_closure_reached_fixed_point :
    ClosureReachedFixedPoint operational_deletion_consumption_event_contract :=
  closureFixedPointCheck_sound operational_deletion_consumption_event_contract
    operational_deletion_consumption_event_closure_fixed_check

theorem operational_deletion_consumption_event_bcnf_check :
    bcnfCheck operational_deletion_consumption_event_contract = true := by
  native_decide

theorem operational_deletion_consumption_event_bcnf : BCNF operational_deletion_consumption_event_contract :=
  bcnfCheck_sound operational_deletion_consumption_event_contract operational_deletion_consumption_event_bcnf_check

def operational_consumer_contract : RelationContract where
  name := "operational_consumer"
  attributes := ["consumer_id", "consumer_name"]
  declaredKeys := [["consumer_id"], ["consumer_name"]]
  declaredFDs := [
    { determinant := ["consumer_id"], dependent := ["consumer_name"] },
    { determinant := ["consumer_name"], dependent := ["consumer_id"] }
  ]

theorem operational_consumer_schema_well_formed :
    schemaWellFormedCheck operational_consumer_contract = true := by
  native_decide

theorem operational_consumer_candidate_keys_check :
    keysDetermineAllCheck operational_consumer_contract = true := by
  native_decide

theorem operational_consumer_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes operational_consumer_contract :=
  keysDetermineAllCheck_sound operational_consumer_contract
    operational_consumer_candidate_keys_check

theorem operational_consumer_candidate_keys_minimal_check :
    declaredKeysMinimalCheck operational_consumer_contract = true := by
  native_decide

theorem operational_consumer_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal operational_consumer_contract :=
  declaredKeysMinimalCheck_sound operational_consumer_contract
    operational_consumer_candidate_keys_minimal_check

theorem operational_consumer_closure_fixed_check :
    closureFixedPointCheck operational_consumer_contract = true := by
  native_decide

theorem operational_consumer_closure_reached_fixed_point :
    ClosureReachedFixedPoint operational_consumer_contract :=
  closureFixedPointCheck_sound operational_consumer_contract
    operational_consumer_closure_fixed_check

theorem operational_consumer_bcnf_check :
    bcnfCheck operational_consumer_contract = true := by
  native_decide

theorem operational_consumer_bcnf : BCNF operational_consumer_contract :=
  bcnfCheck_sound operational_consumer_contract operational_consumer_bcnf_check

def operational_event_ack_contract : RelationContract where
  name := "operational_event_ack"
  attributes := ["consumer_id", "event_id", "acked_at"]
  declaredKeys := [["consumer_id", "event_id"]]
  declaredFDs := [
    { determinant := ["consumer_id", "event_id"], dependent := ["acked_at"] }
  ]

theorem operational_event_ack_schema_well_formed :
    schemaWellFormedCheck operational_event_ack_contract = true := by
  native_decide

theorem operational_event_ack_candidate_keys_check :
    keysDetermineAllCheck operational_event_ack_contract = true := by
  native_decide

theorem operational_event_ack_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes operational_event_ack_contract :=
  keysDetermineAllCheck_sound operational_event_ack_contract
    operational_event_ack_candidate_keys_check

theorem operational_event_ack_candidate_keys_minimal_check :
    declaredKeysMinimalCheck operational_event_ack_contract = true := by
  native_decide

theorem operational_event_ack_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal operational_event_ack_contract :=
  declaredKeysMinimalCheck_sound operational_event_ack_contract
    operational_event_ack_candidate_keys_minimal_check

theorem operational_event_ack_closure_fixed_check :
    closureFixedPointCheck operational_event_ack_contract = true := by
  native_decide

theorem operational_event_ack_closure_reached_fixed_point :
    ClosureReachedFixedPoint operational_event_ack_contract :=
  closureFixedPointCheck_sound operational_event_ack_contract
    operational_event_ack_closure_fixed_check

theorem operational_event_ack_bcnf_check :
    bcnfCheck operational_event_ack_contract = true := by
  native_decide

theorem operational_event_ack_bcnf : BCNF operational_event_ack_contract :=
  bcnfCheck_sound operational_event_ack_contract operational_event_ack_bcnf_check

def operational_event_ack_head_contract : RelationContract where
  name := "operational_event_ack_head"
  attributes := ["consumer_id", "source_revision", "through_sequence_no", "updated_at"]
  declaredKeys := [["consumer_id", "source_revision"]]
  declaredFDs := [
    { determinant := ["consumer_id", "source_revision"], dependent := ["through_sequence_no", "updated_at"] }
  ]

theorem operational_event_ack_head_schema_well_formed :
    schemaWellFormedCheck operational_event_ack_head_contract = true := by
  native_decide

theorem operational_event_ack_head_candidate_keys_check :
    keysDetermineAllCheck operational_event_ack_head_contract = true := by
  native_decide

theorem operational_event_ack_head_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes operational_event_ack_head_contract :=
  keysDetermineAllCheck_sound operational_event_ack_head_contract
    operational_event_ack_head_candidate_keys_check

theorem operational_event_ack_head_candidate_keys_minimal_check :
    declaredKeysMinimalCheck operational_event_ack_head_contract = true := by
  native_decide

theorem operational_event_ack_head_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal operational_event_ack_head_contract :=
  declaredKeysMinimalCheck_sound operational_event_ack_head_contract
    operational_event_ack_head_candidate_keys_minimal_check

theorem operational_event_ack_head_closure_fixed_check :
    closureFixedPointCheck operational_event_ack_head_contract = true := by
  native_decide

theorem operational_event_ack_head_closure_reached_fixed_point :
    ClosureReachedFixedPoint operational_event_ack_head_contract :=
  closureFixedPointCheck_sound operational_event_ack_head_contract
    operational_event_ack_head_closure_fixed_check

theorem operational_event_ack_head_bcnf_check :
    bcnfCheck operational_event_ack_head_contract = true := by
  native_decide

theorem operational_event_ack_head_bcnf : BCNF operational_event_ack_head_contract :=
  bcnfCheck_sound operational_event_ack_head_contract operational_event_ack_head_bcnf_check

def removed_gid_ack_contract : RelationContract where
  name := "removed_gid_ack"
  attributes := ["consumer_id", "gid", "through_source_revision", "acked_at"]
  declaredKeys := [["consumer_id", "gid"]]
  declaredFDs := [
    { determinant := ["consumer_id", "gid"], dependent := ["through_source_revision", "acked_at"] }
  ]

theorem removed_gid_ack_schema_well_formed :
    schemaWellFormedCheck removed_gid_ack_contract = true := by
  native_decide

theorem removed_gid_ack_candidate_keys_check :
    keysDetermineAllCheck removed_gid_ack_contract = true := by
  native_decide

theorem removed_gid_ack_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes removed_gid_ack_contract :=
  keysDetermineAllCheck_sound removed_gid_ack_contract
    removed_gid_ack_candidate_keys_check

theorem removed_gid_ack_candidate_keys_minimal_check :
    declaredKeysMinimalCheck removed_gid_ack_contract = true := by
  native_decide

theorem removed_gid_ack_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal removed_gid_ack_contract :=
  declaredKeysMinimalCheck_sound removed_gid_ack_contract
    removed_gid_ack_candidate_keys_minimal_check

theorem removed_gid_ack_closure_fixed_check :
    closureFixedPointCheck removed_gid_ack_contract = true := by
  native_decide

theorem removed_gid_ack_closure_reached_fixed_point :
    ClosureReachedFixedPoint removed_gid_ack_contract :=
  closureFixedPointCheck_sound removed_gid_ack_contract
    removed_gid_ack_closure_fixed_check

theorem removed_gid_ack_bcnf_check :
    bcnfCheck removed_gid_ack_contract = true := by
  native_decide

theorem removed_gid_ack_bcnf : BCNF removed_gid_ack_contract :=
  bcnfCheck_sound removed_gid_ack_contract removed_gid_ack_bcnf_check

def hash_cache_observation_contract : RelationContract where
  name := "hash_cache_observation"
  attributes := ["source_identity_sha256", "fingerprint_sha256", "observed_at"]
  declaredKeys := [["source_identity_sha256", "fingerprint_sha256"]]
  declaredFDs := [
    { determinant := ["source_identity_sha256", "fingerprint_sha256"], dependent := ["observed_at"] }
  ]

theorem hash_cache_observation_schema_well_formed :
    schemaWellFormedCheck hash_cache_observation_contract = true := by
  native_decide

theorem hash_cache_observation_candidate_keys_check :
    keysDetermineAllCheck hash_cache_observation_contract = true := by
  native_decide

theorem hash_cache_observation_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes hash_cache_observation_contract :=
  keysDetermineAllCheck_sound hash_cache_observation_contract
    hash_cache_observation_candidate_keys_check

theorem hash_cache_observation_candidate_keys_minimal_check :
    declaredKeysMinimalCheck hash_cache_observation_contract = true := by
  native_decide

theorem hash_cache_observation_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal hash_cache_observation_contract :=
  declaredKeysMinimalCheck_sound hash_cache_observation_contract
    hash_cache_observation_candidate_keys_minimal_check

theorem hash_cache_observation_closure_fixed_check :
    closureFixedPointCheck hash_cache_observation_contract = true := by
  native_decide

theorem hash_cache_observation_closure_reached_fixed_point :
    ClosureReachedFixedPoint hash_cache_observation_contract :=
  closureFixedPointCheck_sound hash_cache_observation_contract
    hash_cache_observation_closure_fixed_check

theorem hash_cache_observation_bcnf_check :
    bcnfCheck hash_cache_observation_contract = true := by
  native_decide

theorem hash_cache_observation_bcnf : BCNF hash_cache_observation_contract :=
  bcnfCheck_sound hash_cache_observation_contract hash_cache_observation_bcnf_check

def file_hash_cache_contract : RelationContract where
  name := "file_hash_cache"
  attributes := ["source_identity_sha256", "fingerprint_sha256", "file_sha256", "cached_at"]
  declaredKeys := [["source_identity_sha256", "fingerprint_sha256"]]
  declaredFDs := [
    { determinant := ["source_identity_sha256", "fingerprint_sha256"], dependent := ["file_sha256", "cached_at"] }
  ]

theorem file_hash_cache_schema_well_formed :
    schemaWellFormedCheck file_hash_cache_contract = true := by
  native_decide

theorem file_hash_cache_candidate_keys_check :
    keysDetermineAllCheck file_hash_cache_contract = true := by
  native_decide

theorem file_hash_cache_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes file_hash_cache_contract :=
  keysDetermineAllCheck_sound file_hash_cache_contract
    file_hash_cache_candidate_keys_check

theorem file_hash_cache_candidate_keys_minimal_check :
    declaredKeysMinimalCheck file_hash_cache_contract = true := by
  native_decide

theorem file_hash_cache_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal file_hash_cache_contract :=
  declaredKeysMinimalCheck_sound file_hash_cache_contract
    file_hash_cache_candidate_keys_minimal_check

theorem file_hash_cache_closure_fixed_check :
    closureFixedPointCheck file_hash_cache_contract = true := by
  native_decide

theorem file_hash_cache_closure_reached_fixed_point :
    ClosureReachedFixedPoint file_hash_cache_contract :=
  closureFixedPointCheck_sound file_hash_cache_contract
    file_hash_cache_closure_fixed_check

theorem file_hash_cache_bcnf_check :
    bcnfCheck file_hash_cache_contract = true := by
  native_decide

theorem file_hash_cache_bcnf : BCNF file_hash_cache_contract :=
  bcnfCheck_sound file_hash_cache_contract file_hash_cache_bcnf_check

def cleanup_target_kind_contract : RelationContract where
  name := "cleanup_target_kind"
  attributes := ["target_kind"]
  declaredKeys := [["target_kind"]]
  declaredFDs := [
  ]

theorem cleanup_target_kind_schema_well_formed :
    schemaWellFormedCheck cleanup_target_kind_contract = true := by
  native_decide

theorem cleanup_target_kind_candidate_keys_check :
    keysDetermineAllCheck cleanup_target_kind_contract = true := by
  native_decide

theorem cleanup_target_kind_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes cleanup_target_kind_contract :=
  keysDetermineAllCheck_sound cleanup_target_kind_contract
    cleanup_target_kind_candidate_keys_check

theorem cleanup_target_kind_candidate_keys_minimal_check :
    declaredKeysMinimalCheck cleanup_target_kind_contract = true := by
  native_decide

theorem cleanup_target_kind_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal cleanup_target_kind_contract :=
  declaredKeysMinimalCheck_sound cleanup_target_kind_contract
    cleanup_target_kind_candidate_keys_minimal_check

theorem cleanup_target_kind_closure_fixed_check :
    closureFixedPointCheck cleanup_target_kind_contract = true := by
  native_decide

theorem cleanup_target_kind_closure_reached_fixed_point :
    ClosureReachedFixedPoint cleanup_target_kind_contract :=
  closureFixedPointCheck_sound cleanup_target_kind_contract
    cleanup_target_kind_closure_fixed_check

theorem cleanup_target_kind_bcnf_check :
    bcnfCheck cleanup_target_kind_contract = true := by
  native_decide

theorem cleanup_target_kind_bcnf : BCNF cleanup_target_kind_contract :=
  bcnfCheck_sound cleanup_target_kind_contract cleanup_target_kind_bcnf_check

def cleanup_sweep_target_contract : RelationContract where
  name := "cleanup_sweep_target"
  attributes := ["target_kind", "shard_no", "target_key"]
  declaredKeys := [["target_kind", "shard_no"], ["target_key"]]
  declaredFDs := [
    { determinant := ["target_kind", "shard_no"], dependent := ["target_key"] },
    { determinant := ["target_key"], dependent := ["target_kind", "shard_no"] }
  ]

theorem cleanup_sweep_target_schema_well_formed :
    schemaWellFormedCheck cleanup_sweep_target_contract = true := by
  native_decide

theorem cleanup_sweep_target_candidate_keys_check :
    keysDetermineAllCheck cleanup_sweep_target_contract = true := by
  native_decide

theorem cleanup_sweep_target_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes cleanup_sweep_target_contract :=
  keysDetermineAllCheck_sound cleanup_sweep_target_contract
    cleanup_sweep_target_candidate_keys_check

theorem cleanup_sweep_target_candidate_keys_minimal_check :
    declaredKeysMinimalCheck cleanup_sweep_target_contract = true := by
  native_decide

theorem cleanup_sweep_target_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal cleanup_sweep_target_contract :=
  declaredKeysMinimalCheck_sound cleanup_sweep_target_contract
    cleanup_sweep_target_candidate_keys_minimal_check

theorem cleanup_sweep_target_closure_fixed_check :
    closureFixedPointCheck cleanup_sweep_target_contract = true := by
  native_decide

theorem cleanup_sweep_target_closure_reached_fixed_point :
    ClosureReachedFixedPoint cleanup_sweep_target_contract :=
  closureFixedPointCheck_sound cleanup_sweep_target_contract
    cleanup_sweep_target_closure_fixed_check

theorem cleanup_sweep_target_bcnf_check :
    bcnfCheck cleanup_sweep_target_contract = true := by
  native_decide

theorem cleanup_sweep_target_bcnf : BCNF cleanup_sweep_target_contract :=
  bcnfCheck_sound cleanup_sweep_target_contract cleanup_sweep_target_bcnf_check

def cleanup_phase_contract : RelationContract where
  name := "cleanup_phase"
  attributes := ["phase", "target_kind", "phase_order"]
  declaredKeys := [["phase"], ["target_kind", "phase_order"]]
  declaredFDs := [
    { determinant := ["phase"], dependent := ["target_kind", "phase_order"] },
    { determinant := ["target_kind", "phase_order"], dependent := ["phase"] }
  ]

theorem cleanup_phase_schema_well_formed :
    schemaWellFormedCheck cleanup_phase_contract = true := by
  native_decide

theorem cleanup_phase_candidate_keys_check :
    keysDetermineAllCheck cleanup_phase_contract = true := by
  native_decide

theorem cleanup_phase_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes cleanup_phase_contract :=
  keysDetermineAllCheck_sound cleanup_phase_contract
    cleanup_phase_candidate_keys_check

theorem cleanup_phase_candidate_keys_minimal_check :
    declaredKeysMinimalCheck cleanup_phase_contract = true := by
  native_decide

theorem cleanup_phase_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal cleanup_phase_contract :=
  declaredKeysMinimalCheck_sound cleanup_phase_contract
    cleanup_phase_candidate_keys_minimal_check

theorem cleanup_phase_closure_fixed_check :
    closureFixedPointCheck cleanup_phase_contract = true := by
  native_decide

theorem cleanup_phase_closure_reached_fixed_point :
    ClosureReachedFixedPoint cleanup_phase_contract :=
  closureFixedPointCheck_sound cleanup_phase_contract
    cleanup_phase_closure_fixed_check

theorem cleanup_phase_bcnf_check :
    bcnfCheck cleanup_phase_contract = true := by
  native_decide

theorem cleanup_phase_bcnf : BCNF cleanup_phase_contract :=
  bcnfCheck_sound cleanup_phase_contract cleanup_phase_bcnf_check

def cleanup_job_contract : RelationContract where
  name := "cleanup_job"
  attributes := ["cleanup_id", "target_key", "cycle_generation", "cycle_cutoff_at", "algorithm_version", "max_rows_per_transaction", "hash_cache_max_age_microseconds", "state", "created_at", "completed_at"]
  declaredKeys := [["cleanup_id"], ["target_key"]]
  declaredFDs := [
    { determinant := ["cleanup_id"], dependent := ["target_key", "cycle_generation", "cycle_cutoff_at", "algorithm_version", "max_rows_per_transaction", "hash_cache_max_age_microseconds", "state", "created_at", "completed_at"] },
    { determinant := ["target_key"], dependent := ["cleanup_id", "cycle_generation", "cycle_cutoff_at", "algorithm_version", "max_rows_per_transaction", "hash_cache_max_age_microseconds", "state", "created_at", "completed_at"] }
  ]

theorem cleanup_job_schema_well_formed :
    schemaWellFormedCheck cleanup_job_contract = true := by
  native_decide

theorem cleanup_job_candidate_keys_check :
    keysDetermineAllCheck cleanup_job_contract = true := by
  native_decide

theorem cleanup_job_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes cleanup_job_contract :=
  keysDetermineAllCheck_sound cleanup_job_contract
    cleanup_job_candidate_keys_check

theorem cleanup_job_candidate_keys_minimal_check :
    declaredKeysMinimalCheck cleanup_job_contract = true := by
  native_decide

theorem cleanup_job_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal cleanup_job_contract :=
  declaredKeysMinimalCheck_sound cleanup_job_contract
    cleanup_job_candidate_keys_minimal_check

theorem cleanup_job_closure_fixed_check :
    closureFixedPointCheck cleanup_job_contract = true := by
  native_decide

theorem cleanup_job_closure_reached_fixed_point :
    ClosureReachedFixedPoint cleanup_job_contract :=
  closureFixedPointCheck_sound cleanup_job_contract
    cleanup_job_closure_fixed_check

theorem cleanup_job_bcnf_check :
    bcnfCheck cleanup_job_contract = true := by
  native_decide

theorem cleanup_job_bcnf : BCNF cleanup_job_contract :=
  bcnfCheck_sound cleanup_job_contract cleanup_job_bcnf_check

def cleanup_checkpoint_contract : RelationContract where
  name := "cleanup_checkpoint"
  attributes := ["cleanup_id", "phase", "generation", "cursor_bytes", "deleted_count", "chain_sha256", "state", "updated_at"]
  declaredKeys := [["cleanup_id", "phase"]]
  declaredFDs := [
    { determinant := ["cleanup_id", "phase"], dependent := ["generation", "cursor_bytes", "deleted_count", "chain_sha256", "state", "updated_at"] }
  ]

theorem cleanup_checkpoint_schema_well_formed :
    schemaWellFormedCheck cleanup_checkpoint_contract = true := by
  native_decide

theorem cleanup_checkpoint_candidate_keys_check :
    keysDetermineAllCheck cleanup_checkpoint_contract = true := by
  native_decide

theorem cleanup_checkpoint_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes cleanup_checkpoint_contract :=
  keysDetermineAllCheck_sound cleanup_checkpoint_contract
    cleanup_checkpoint_candidate_keys_check

theorem cleanup_checkpoint_candidate_keys_minimal_check :
    declaredKeysMinimalCheck cleanup_checkpoint_contract = true := by
  native_decide

theorem cleanup_checkpoint_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal cleanup_checkpoint_contract :=
  declaredKeysMinimalCheck_sound cleanup_checkpoint_contract
    cleanup_checkpoint_candidate_keys_minimal_check

theorem cleanup_checkpoint_closure_fixed_check :
    closureFixedPointCheck cleanup_checkpoint_contract = true := by
  native_decide

theorem cleanup_checkpoint_closure_reached_fixed_point :
    ClosureReachedFixedPoint cleanup_checkpoint_contract :=
  closureFixedPointCheck_sound cleanup_checkpoint_contract
    cleanup_checkpoint_closure_fixed_check

theorem cleanup_checkpoint_bcnf_check :
    bcnfCheck cleanup_checkpoint_contract = true := by
  native_decide

theorem cleanup_checkpoint_bcnf : BCNF cleanup_checkpoint_contract :=
  bcnfCheck_sound cleanup_checkpoint_contract cleanup_checkpoint_bcnf_check

def cleanup_batch_receipt_contract : RelationContract where
  name := "cleanup_batch_receipt"
  attributes := ["cleanup_id", "phase", "batch_key", "start_cursor", "next_cursor", "input_sha256", "output_sha256", "row_count", "committed_generation", "committed_at"]
  declaredKeys := [["cleanup_id", "phase"]]
  declaredFDs := [
    { determinant := ["cleanup_id", "phase"], dependent := ["batch_key", "start_cursor", "next_cursor", "input_sha256", "output_sha256", "row_count", "committed_generation", "committed_at"] }
  ]

theorem cleanup_batch_receipt_schema_well_formed :
    schemaWellFormedCheck cleanup_batch_receipt_contract = true := by
  native_decide

theorem cleanup_batch_receipt_candidate_keys_check :
    keysDetermineAllCheck cleanup_batch_receipt_contract = true := by
  native_decide

theorem cleanup_batch_receipt_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes cleanup_batch_receipt_contract :=
  keysDetermineAllCheck_sound cleanup_batch_receipt_contract
    cleanup_batch_receipt_candidate_keys_check

theorem cleanup_batch_receipt_candidate_keys_minimal_check :
    declaredKeysMinimalCheck cleanup_batch_receipt_contract = true := by
  native_decide

theorem cleanup_batch_receipt_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal cleanup_batch_receipt_contract :=
  declaredKeysMinimalCheck_sound cleanup_batch_receipt_contract
    cleanup_batch_receipt_candidate_keys_minimal_check

theorem cleanup_batch_receipt_closure_fixed_check :
    closureFixedPointCheck cleanup_batch_receipt_contract = true := by
  native_decide

theorem cleanup_batch_receipt_closure_reached_fixed_point :
    ClosureReachedFixedPoint cleanup_batch_receipt_contract :=
  closureFixedPointCheck_sound cleanup_batch_receipt_contract
    cleanup_batch_receipt_closure_fixed_check

theorem cleanup_batch_receipt_bcnf_check :
    bcnfCheck cleanup_batch_receipt_contract = true := by
  native_decide

theorem cleanup_batch_receipt_bcnf : BCNF cleanup_batch_receipt_contract :=
  bcnfCheck_sound cleanup_batch_receipt_contract cleanup_batch_receipt_bcnf_check

def cleanup_completion_contract : RelationContract where
  name := "cleanup_completion"
  attributes := ["target_key", "cycle_generation", "final_chain_sha256", "deleted_count"]
  declaredKeys := [["target_key"]]
  declaredFDs := [
    { determinant := ["target_key"], dependent := ["cycle_generation", "final_chain_sha256", "deleted_count"] }
  ]

theorem cleanup_completion_schema_well_formed :
    schemaWellFormedCheck cleanup_completion_contract = true := by
  native_decide

theorem cleanup_completion_candidate_keys_check :
    keysDetermineAllCheck cleanup_completion_contract = true := by
  native_decide

theorem cleanup_completion_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes cleanup_completion_contract :=
  keysDetermineAllCheck_sound cleanup_completion_contract
    cleanup_completion_candidate_keys_check

theorem cleanup_completion_candidate_keys_minimal_check :
    declaredKeysMinimalCheck cleanup_completion_contract = true := by
  native_decide

theorem cleanup_completion_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal cleanup_completion_contract :=
  declaredKeysMinimalCheck_sound cleanup_completion_contract
    cleanup_completion_candidate_keys_minimal_check

theorem cleanup_completion_closure_fixed_check :
    closureFixedPointCheck cleanup_completion_contract = true := by
  native_decide

theorem cleanup_completion_closure_reached_fixed_point :
    ClosureReachedFixedPoint cleanup_completion_contract :=
  closureFixedPointCheck_sound cleanup_completion_contract
    cleanup_completion_closure_fixed_check

theorem cleanup_completion_bcnf_check :
    bcnfCheck cleanup_completion_contract = true := by
  native_decide

theorem cleanup_completion_bcnf : BCNF cleanup_completion_contract :=
  bcnfCheck_sound cleanup_completion_contract cleanup_completion_bcnf_check

def manifestContracts : List RelationContract := [
  schema_epoch_control_contract,
  ingest_generation_contract,
  ingest_coordination_head_contract,
  ingest_generation_owner_contract,
  ingest_generation_lease_contract,
  ingest_generation_handoff_contract,
  source_build_generation_contract,
  maintenance_gate_generation_contract,
  maintenance_gate_head_contract,
  maintenance_gate_owner_contract,
  maintenance_gate_holder_contract,
  maintenance_work_state_contract,
  source_working_build_contract,
  catalog_working_candidate_contract,
  revision_allocator_contract,
  identity_allocator_contract,
  gallery_observation_allocator_contract,
  gallery_observation_staging_contract,
  gallery_observation_staging_claim_contract,
  gallery_observation_staging_checkpoint_contract,
  gallery_observation_staging_request_contract,
  gallery_observation_staging_request_chunk_contract,
  gallery_observation_staging_request_owner_contract,
  gallery_observation_staging_request_predecessor_contract,
  gallery_observation_staging_page_request_contract,
  gallery_observation_staging_request_page_contract,
  gallery_observation_staging_receipt_contract,
  gallery_observation_staging_frontier_contract,
  gallery_observation_staging_match_checkpoint_contract,
  gallery_observation_staging_match_request_contract,
  gallery_observation_staging_match_receipt_contract,
  gallery_observation_staging_metadata_parser_contract,
  canonical_value_upload_contract,
  download_request_contract,
  deletion_request_attempt_contract,
  deletion_request_url_contract,
  deletion_request_head_contract,
  removed_gid_contract,
  gallery_redownload_state_contract,
  operational_policy_contract,
  operational_preparation_contract,
  operational_preparation_checkpoint_contract,
  operational_preparation_batch_receipt_contract,
  operational_activation_contract,
  operational_event_contract,
  operational_removed_gid_event_contract,
  operational_deletion_consumption_event_contract,
  operational_consumer_contract,
  operational_event_ack_contract,
  operational_event_ack_head_contract,
  removed_gid_ack_contract,
  hash_cache_observation_contract,
  file_hash_cache_contract,
  cleanup_target_kind_contract,
  cleanup_sweep_target_contract,
  cleanup_phase_contract,
  cleanup_job_contract,
  cleanup_checkpoint_contract,
  cleanup_batch_receipt_contract,
  cleanup_completion_contract
]

theorem manifest_relation_count :
    manifestContracts.length = 60 := by
  native_decide

theorem all_manifest_relations_bcnf :
    BCNF schema_epoch_control_contract ∧
    BCNF ingest_generation_contract ∧
    BCNF ingest_coordination_head_contract ∧
    BCNF ingest_generation_owner_contract ∧
    BCNF ingest_generation_lease_contract ∧
    BCNF ingest_generation_handoff_contract ∧
    BCNF source_build_generation_contract ∧
    BCNF maintenance_gate_generation_contract ∧
    BCNF maintenance_gate_head_contract ∧
    BCNF maintenance_gate_owner_contract ∧
    BCNF maintenance_gate_holder_contract ∧
    BCNF maintenance_work_state_contract ∧
    BCNF source_working_build_contract ∧
    BCNF catalog_working_candidate_contract ∧
    BCNF revision_allocator_contract ∧
    BCNF identity_allocator_contract ∧
    BCNF gallery_observation_allocator_contract ∧
    BCNF gallery_observation_staging_contract ∧
    BCNF gallery_observation_staging_claim_contract ∧
    BCNF gallery_observation_staging_checkpoint_contract ∧
    BCNF gallery_observation_staging_request_contract ∧
    BCNF gallery_observation_staging_request_chunk_contract ∧
    BCNF gallery_observation_staging_request_owner_contract ∧
    BCNF gallery_observation_staging_request_predecessor_contract ∧
    BCNF gallery_observation_staging_page_request_contract ∧
    BCNF gallery_observation_staging_request_page_contract ∧
    BCNF gallery_observation_staging_receipt_contract ∧
    BCNF gallery_observation_staging_frontier_contract ∧
    BCNF gallery_observation_staging_match_checkpoint_contract ∧
    BCNF gallery_observation_staging_match_request_contract ∧
    BCNF gallery_observation_staging_match_receipt_contract ∧
    BCNF gallery_observation_staging_metadata_parser_contract ∧
    BCNF canonical_value_upload_contract ∧
    BCNF download_request_contract ∧
    BCNF deletion_request_attempt_contract ∧
    BCNF deletion_request_url_contract ∧
    BCNF deletion_request_head_contract ∧
    BCNF removed_gid_contract ∧
    BCNF gallery_redownload_state_contract ∧
    BCNF operational_policy_contract ∧
    BCNF operational_preparation_contract ∧
    BCNF operational_preparation_checkpoint_contract ∧
    BCNF operational_preparation_batch_receipt_contract ∧
    BCNF operational_activation_contract ∧
    BCNF operational_event_contract ∧
    BCNF operational_removed_gid_event_contract ∧
    BCNF operational_deletion_consumption_event_contract ∧
    BCNF operational_consumer_contract ∧
    BCNF operational_event_ack_contract ∧
    BCNF operational_event_ack_head_contract ∧
    BCNF removed_gid_ack_contract ∧
    BCNF hash_cache_observation_contract ∧
    BCNF file_hash_cache_contract ∧
    BCNF cleanup_target_kind_contract ∧
    BCNF cleanup_sweep_target_contract ∧
    BCNF cleanup_phase_contract ∧
    BCNF cleanup_job_contract ∧
    BCNF cleanup_checkpoint_contract ∧
    BCNF cleanup_batch_receipt_contract ∧
    BCNF cleanup_completion_contract := by
  exact ⟨schema_epoch_control_bcnf,
    ingest_generation_bcnf,
    ingest_coordination_head_bcnf,
    ingest_generation_owner_bcnf,
    ingest_generation_lease_bcnf,
    ingest_generation_handoff_bcnf,
    source_build_generation_bcnf,
    maintenance_gate_generation_bcnf,
    maintenance_gate_head_bcnf,
    maintenance_gate_owner_bcnf,
    maintenance_gate_holder_bcnf,
    maintenance_work_state_bcnf,
    source_working_build_bcnf,
    catalog_working_candidate_bcnf,
    revision_allocator_bcnf,
    identity_allocator_bcnf,
    gallery_observation_allocator_bcnf,
    gallery_observation_staging_bcnf,
    gallery_observation_staging_claim_bcnf,
    gallery_observation_staging_checkpoint_bcnf,
    gallery_observation_staging_request_bcnf,
    gallery_observation_staging_request_chunk_bcnf,
    gallery_observation_staging_request_owner_bcnf,
    gallery_observation_staging_request_predecessor_bcnf,
    gallery_observation_staging_page_request_bcnf,
    gallery_observation_staging_request_page_bcnf,
    gallery_observation_staging_receipt_bcnf,
    gallery_observation_staging_frontier_bcnf,
    gallery_observation_staging_match_checkpoint_bcnf,
    gallery_observation_staging_match_request_bcnf,
    gallery_observation_staging_match_receipt_bcnf,
    gallery_observation_staging_metadata_parser_bcnf,
    canonical_value_upload_bcnf,
    download_request_bcnf,
    deletion_request_attempt_bcnf,
    deletion_request_url_bcnf,
    deletion_request_head_bcnf,
    removed_gid_bcnf,
    gallery_redownload_state_bcnf,
    operational_policy_bcnf,
    operational_preparation_bcnf,
    operational_preparation_checkpoint_bcnf,
    operational_preparation_batch_receipt_bcnf,
    operational_activation_bcnf,
    operational_event_bcnf,
    operational_removed_gid_event_bcnf,
    operational_deletion_consumption_event_bcnf,
    operational_consumer_bcnf,
    operational_event_ack_bcnf,
    operational_event_ack_head_bcnf,
    removed_gid_ack_bcnf,
    hash_cache_observation_bcnf,
    file_hash_cache_bcnf,
    cleanup_target_kind_bcnf,
    cleanup_sweep_target_bcnf,
    cleanup_phase_bcnf,
    cleanup_job_bcnf,
    cleanup_checkpoint_bcnf,
    cleanup_batch_receipt_bcnf,
    cleanup_completion_bcnf⟩

theorem all_manifest_candidate_keys_determine_attributes :
    KeysDetermineAllAttributes schema_epoch_control_contract ∧
    KeysDetermineAllAttributes ingest_generation_contract ∧
    KeysDetermineAllAttributes ingest_coordination_head_contract ∧
    KeysDetermineAllAttributes ingest_generation_owner_contract ∧
    KeysDetermineAllAttributes ingest_generation_lease_contract ∧
    KeysDetermineAllAttributes ingest_generation_handoff_contract ∧
    KeysDetermineAllAttributes source_build_generation_contract ∧
    KeysDetermineAllAttributes maintenance_gate_generation_contract ∧
    KeysDetermineAllAttributes maintenance_gate_head_contract ∧
    KeysDetermineAllAttributes maintenance_gate_owner_contract ∧
    KeysDetermineAllAttributes maintenance_gate_holder_contract ∧
    KeysDetermineAllAttributes maintenance_work_state_contract ∧
    KeysDetermineAllAttributes source_working_build_contract ∧
    KeysDetermineAllAttributes catalog_working_candidate_contract ∧
    KeysDetermineAllAttributes revision_allocator_contract ∧
    KeysDetermineAllAttributes identity_allocator_contract ∧
    KeysDetermineAllAttributes gallery_observation_allocator_contract ∧
    KeysDetermineAllAttributes gallery_observation_staging_contract ∧
    KeysDetermineAllAttributes gallery_observation_staging_claim_contract ∧
    KeysDetermineAllAttributes gallery_observation_staging_checkpoint_contract ∧
    KeysDetermineAllAttributes gallery_observation_staging_request_contract ∧
    KeysDetermineAllAttributes gallery_observation_staging_request_chunk_contract ∧
    KeysDetermineAllAttributes gallery_observation_staging_request_owner_contract ∧
    KeysDetermineAllAttributes gallery_observation_staging_request_predecessor_contract ∧
    KeysDetermineAllAttributes gallery_observation_staging_page_request_contract ∧
    KeysDetermineAllAttributes gallery_observation_staging_request_page_contract ∧
    KeysDetermineAllAttributes gallery_observation_staging_receipt_contract ∧
    KeysDetermineAllAttributes gallery_observation_staging_frontier_contract ∧
    KeysDetermineAllAttributes gallery_observation_staging_match_checkpoint_contract ∧
    KeysDetermineAllAttributes gallery_observation_staging_match_request_contract ∧
    KeysDetermineAllAttributes gallery_observation_staging_match_receipt_contract ∧
    KeysDetermineAllAttributes gallery_observation_staging_metadata_parser_contract ∧
    KeysDetermineAllAttributes canonical_value_upload_contract ∧
    KeysDetermineAllAttributes download_request_contract ∧
    KeysDetermineAllAttributes deletion_request_attempt_contract ∧
    KeysDetermineAllAttributes deletion_request_url_contract ∧
    KeysDetermineAllAttributes deletion_request_head_contract ∧
    KeysDetermineAllAttributes removed_gid_contract ∧
    KeysDetermineAllAttributes gallery_redownload_state_contract ∧
    KeysDetermineAllAttributes operational_policy_contract ∧
    KeysDetermineAllAttributes operational_preparation_contract ∧
    KeysDetermineAllAttributes operational_preparation_checkpoint_contract ∧
    KeysDetermineAllAttributes operational_preparation_batch_receipt_contract ∧
    KeysDetermineAllAttributes operational_activation_contract ∧
    KeysDetermineAllAttributes operational_event_contract ∧
    KeysDetermineAllAttributes operational_removed_gid_event_contract ∧
    KeysDetermineAllAttributes operational_deletion_consumption_event_contract ∧
    KeysDetermineAllAttributes operational_consumer_contract ∧
    KeysDetermineAllAttributes operational_event_ack_contract ∧
    KeysDetermineAllAttributes operational_event_ack_head_contract ∧
    KeysDetermineAllAttributes removed_gid_ack_contract ∧
    KeysDetermineAllAttributes hash_cache_observation_contract ∧
    KeysDetermineAllAttributes file_hash_cache_contract ∧
    KeysDetermineAllAttributes cleanup_target_kind_contract ∧
    KeysDetermineAllAttributes cleanup_sweep_target_contract ∧
    KeysDetermineAllAttributes cleanup_phase_contract ∧
    KeysDetermineAllAttributes cleanup_job_contract ∧
    KeysDetermineAllAttributes cleanup_checkpoint_contract ∧
    KeysDetermineAllAttributes cleanup_batch_receipt_contract ∧
    KeysDetermineAllAttributes cleanup_completion_contract := by
  exact ⟨schema_epoch_control_candidate_keys_determine_all_attributes,
    ingest_generation_candidate_keys_determine_all_attributes,
    ingest_coordination_head_candidate_keys_determine_all_attributes,
    ingest_generation_owner_candidate_keys_determine_all_attributes,
    ingest_generation_lease_candidate_keys_determine_all_attributes,
    ingest_generation_handoff_candidate_keys_determine_all_attributes,
    source_build_generation_candidate_keys_determine_all_attributes,
    maintenance_gate_generation_candidate_keys_determine_all_attributes,
    maintenance_gate_head_candidate_keys_determine_all_attributes,
    maintenance_gate_owner_candidate_keys_determine_all_attributes,
    maintenance_gate_holder_candidate_keys_determine_all_attributes,
    maintenance_work_state_candidate_keys_determine_all_attributes,
    source_working_build_candidate_keys_determine_all_attributes,
    catalog_working_candidate_candidate_keys_determine_all_attributes,
    revision_allocator_candidate_keys_determine_all_attributes,
    identity_allocator_candidate_keys_determine_all_attributes,
    gallery_observation_allocator_candidate_keys_determine_all_attributes,
    gallery_observation_staging_candidate_keys_determine_all_attributes,
    gallery_observation_staging_claim_candidate_keys_determine_all_attributes,
    gallery_observation_staging_checkpoint_candidate_keys_determine_all_attributes,
    gallery_observation_staging_request_candidate_keys_determine_all_attributes,
    gallery_observation_staging_request_chunk_candidate_keys_determine_all_attributes,
    gallery_observation_staging_request_owner_candidate_keys_determine_all_attributes,
    gallery_observation_staging_request_predecessor_candidate_keys_determine_all_attributes,
    gallery_observation_staging_page_request_candidate_keys_determine_all_attributes,
    gallery_observation_staging_request_page_candidate_keys_determine_all_attributes,
    gallery_observation_staging_receipt_candidate_keys_determine_all_attributes,
    gallery_observation_staging_frontier_candidate_keys_determine_all_attributes,
    gallery_observation_staging_match_checkpoint_candidate_keys_determine_all_attributes,
    gallery_observation_staging_match_request_candidate_keys_determine_all_attributes,
    gallery_observation_staging_match_receipt_candidate_keys_determine_all_attributes,
    gallery_observation_staging_metadata_parser_candidate_keys_determine_all_attributes,
    canonical_value_upload_candidate_keys_determine_all_attributes,
    download_request_candidate_keys_determine_all_attributes,
    deletion_request_attempt_candidate_keys_determine_all_attributes,
    deletion_request_url_candidate_keys_determine_all_attributes,
    deletion_request_head_candidate_keys_determine_all_attributes,
    removed_gid_candidate_keys_determine_all_attributes,
    gallery_redownload_state_candidate_keys_determine_all_attributes,
    operational_policy_candidate_keys_determine_all_attributes,
    operational_preparation_candidate_keys_determine_all_attributes,
    operational_preparation_checkpoint_candidate_keys_determine_all_attributes,
    operational_preparation_batch_receipt_candidate_keys_determine_all_attributes,
    operational_activation_candidate_keys_determine_all_attributes,
    operational_event_candidate_keys_determine_all_attributes,
    operational_removed_gid_event_candidate_keys_determine_all_attributes,
    operational_deletion_consumption_event_candidate_keys_determine_all_attributes,
    operational_consumer_candidate_keys_determine_all_attributes,
    operational_event_ack_candidate_keys_determine_all_attributes,
    operational_event_ack_head_candidate_keys_determine_all_attributes,
    removed_gid_ack_candidate_keys_determine_all_attributes,
    hash_cache_observation_candidate_keys_determine_all_attributes,
    file_hash_cache_candidate_keys_determine_all_attributes,
    cleanup_target_kind_candidate_keys_determine_all_attributes,
    cleanup_sweep_target_candidate_keys_determine_all_attributes,
    cleanup_phase_candidate_keys_determine_all_attributes,
    cleanup_job_candidate_keys_determine_all_attributes,
    cleanup_checkpoint_candidate_keys_determine_all_attributes,
    cleanup_batch_receipt_candidate_keys_determine_all_attributes,
    cleanup_completion_candidate_keys_determine_all_attributes⟩

/- END GENERATED OPERATIONAL CONTRACTS -/

end H2HDB.Verification.OperationalSchema
