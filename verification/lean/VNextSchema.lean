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
lists every semantic nontrivial FD.  It also assumes hash identifiers are
collision-free for canonical bytes and canonical encoders/policies are
deterministic.  It does not prove that SQL DDL, migrations, collations, indexes,
foreign keys, or transactions implement the manifest.
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

/-!
Relation-specific declarations and proofs follow.  Every theorem name retains
the exact snake-case manifest relation name for auditability.
-/

end H2HDB.Verification.VNextSchema

namespace H2HDB.Verification.VNextSchema

/- BEGIN GENERATED CATALOG CONTRACTS -/
def catalogManifestSha256 : String := "0f9187bb3991468114af2910ffb431428515a2ae1af6175fa79e387f4f8408e7"

/-! This section is mechanically generated from catalog.toml. -/

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

def source_build_contract : RelationContract where
  name := "source_build"
  attributes := ["build_id", "scope_key", "manifest_policy_id", "base_source_revision", "state", "created_at", "sealed_at"]
  declaredKeys := [["build_id"]]
  declaredFDs := [
    { determinant := ["build_id"], dependent := ["scope_key", "manifest_policy_id", "base_source_revision", "state", "created_at", "sealed_at"] }
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

theorem source_build_bcnf_check :
    bcnfCheck source_build_contract = true := by
  native_decide

theorem source_build_bcnf : BCNF source_build_contract :=
  bcnfCheck_sound source_build_contract source_build_bcnf_check

def gallery_identity_contract : RelationContract where
  name := "gallery_identity"
  attributes := ["gallery_key", "gallery_name"]
  declaredKeys := [["gallery_key"], ["gallery_name"]]
  declaredFDs := [
    { determinant := ["gallery_key"], dependent := ["gallery_name"] },
    { determinant := ["gallery_name"], dependent := ["gallery_key"] }
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

def source_gallery_contract : RelationContract where
  name := "source_gallery"
  attributes := ["gallery_id", "build_id", "gallery_key", "source_locator", "gid", "metadata_observation_sha256", "source_complete"]
  declaredKeys := [["gallery_id"], ["build_id", "gallery_key"], ["build_id", "source_locator"]]
  declaredFDs := [
    { determinant := ["gallery_id"], dependent := ["build_id", "gallery_key", "source_locator", "gid", "metadata_observation_sha256", "source_complete"] },
    { determinant := ["build_id", "gallery_key"], dependent := ["gallery_id", "source_locator", "gid", "metadata_observation_sha256", "source_complete"] },
    { determinant := ["build_id", "source_locator"], dependent := ["gallery_id", "gallery_key", "gid", "metadata_observation_sha256", "source_complete"] }
  ]

theorem source_gallery_schema_well_formed :
    schemaWellFormedCheck source_gallery_contract = true := by
  native_decide

theorem source_gallery_candidate_keys_check :
    keysDetermineAllCheck source_gallery_contract = true := by
  native_decide

theorem source_gallery_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_gallery_contract :=
  keysDetermineAllCheck_sound source_gallery_contract
    source_gallery_candidate_keys_check

theorem source_gallery_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_gallery_contract = true := by
  native_decide

theorem source_gallery_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_gallery_contract :=
  declaredKeysMinimalCheck_sound source_gallery_contract
    source_gallery_candidate_keys_minimal_check

theorem source_gallery_closure_fixed_check :
    closureFixedPointCheck source_gallery_contract = true := by
  native_decide

theorem source_gallery_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_gallery_contract :=
  closureFixedPointCheck_sound source_gallery_contract
    source_gallery_closure_fixed_check

theorem source_gallery_bcnf_check :
    bcnfCheck source_gallery_contract = true := by
  native_decide

theorem source_gallery_bcnf : BCNF source_gallery_contract :=
  bcnfCheck_sound source_gallery_contract source_gallery_bcnf_check

def file_name_identity_contract : RelationContract where
  name := "file_name_identity"
  attributes := ["file_key", "name_bytes", "role"]
  declaredKeys := [["file_key"], ["name_bytes"]]
  declaredFDs := [
    { determinant := ["file_key"], dependent := ["name_bytes", "role"] },
    { determinant := ["name_bytes"], dependent := ["file_key", "role"] }
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

def source_file_contract : RelationContract where
  name := "source_file"
  attributes := ["file_observation_id", "gallery_id", "file_no", "file_key", "mtime_ns", "file_sha256"]
  declaredKeys := [["file_observation_id"], ["gallery_id", "file_no"], ["gallery_id", "file_key"]]
  declaredFDs := [
    { determinant := ["file_observation_id"], dependent := ["gallery_id", "file_no", "file_key", "mtime_ns", "file_sha256"] },
    { determinant := ["gallery_id", "file_no"], dependent := ["file_observation_id", "file_key", "mtime_ns", "file_sha256"] },
    { determinant := ["gallery_id", "file_key"], dependent := ["file_observation_id", "file_no", "mtime_ns", "file_sha256"] }
  ]

theorem source_file_schema_well_formed :
    schemaWellFormedCheck source_file_contract = true := by
  native_decide

theorem source_file_candidate_keys_check :
    keysDetermineAllCheck source_file_contract = true := by
  native_decide

theorem source_file_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_file_contract :=
  keysDetermineAllCheck_sound source_file_contract
    source_file_candidate_keys_check

theorem source_file_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_file_contract = true := by
  native_decide

theorem source_file_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_file_contract :=
  declaredKeysMinimalCheck_sound source_file_contract
    source_file_candidate_keys_minimal_check

theorem source_file_closure_fixed_check :
    closureFixedPointCheck source_file_contract = true := by
  native_decide

theorem source_file_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_file_contract :=
  closureFixedPointCheck_sound source_file_contract
    source_file_closure_fixed_check

theorem source_file_bcnf_check :
    bcnfCheck source_file_contract = true := by
  native_decide

theorem source_file_bcnf : BCNF source_file_contract :=
  bcnfCheck_sound source_file_contract source_file_bcnf_check

def tag_term_contract : RelationContract where
  name := "tag_term"
  attributes := ["tag_id", "namespace", "normalized_value", "display_value"]
  declaredKeys := [["tag_id"], ["namespace", "normalized_value"]]
  declaredFDs := [
    { determinant := ["tag_id"], dependent := ["namespace", "normalized_value", "display_value"] },
    { determinant := ["namespace", "normalized_value"], dependent := ["tag_id", "display_value"] }
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

def source_gallery_tag_contract : RelationContract where
  name := "source_gallery_tag"
  attributes := ["gallery_id", "position", "tag_id"]
  declaredKeys := [["gallery_id", "position"]]
  declaredFDs := [
    { determinant := ["gallery_id", "position"], dependent := ["tag_id"] }
  ]

theorem source_gallery_tag_schema_well_formed :
    schemaWellFormedCheck source_gallery_tag_contract = true := by
  native_decide

theorem source_gallery_tag_candidate_keys_check :
    keysDetermineAllCheck source_gallery_tag_contract = true := by
  native_decide

theorem source_gallery_tag_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes source_gallery_tag_contract :=
  keysDetermineAllCheck_sound source_gallery_tag_contract
    source_gallery_tag_candidate_keys_check

theorem source_gallery_tag_candidate_keys_minimal_check :
    declaredKeysMinimalCheck source_gallery_tag_contract = true := by
  native_decide

theorem source_gallery_tag_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal source_gallery_tag_contract :=
  declaredKeysMinimalCheck_sound source_gallery_tag_contract
    source_gallery_tag_candidate_keys_minimal_check

theorem source_gallery_tag_closure_fixed_check :
    closureFixedPointCheck source_gallery_tag_contract = true := by
  native_decide

theorem source_gallery_tag_closure_reached_fixed_point :
    ClosureReachedFixedPoint source_gallery_tag_contract :=
  closureFixedPointCheck_sound source_gallery_tag_contract
    source_gallery_tag_closure_fixed_check

theorem source_gallery_tag_bcnf_check :
    bcnfCheck source_gallery_tag_contract = true := by
  native_decide

theorem source_gallery_tag_bcnf : BCNF source_gallery_tag_contract :=
  bcnfCheck_sound source_gallery_tag_contract source_gallery_tag_bcnf_check

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

theorem build_manifest_bcnf_check :
    bcnfCheck build_manifest_contract = true := by
  native_decide

theorem build_manifest_bcnf : BCNF build_manifest_contract :=
  bcnfCheck_sound build_manifest_contract build_manifest_bcnf_check

def gallery_manifest_contract : RelationContract where
  name := "gallery_manifest"
  attributes := ["gallery_id", "manifest_sha256", "content_sha256", "metadata_sha256", "content_file_count", "content_byte_count", "computed_at"]
  declaredKeys := [["gallery_id"]]
  declaredFDs := [
    { determinant := ["gallery_id"], dependent := ["manifest_sha256", "content_sha256", "metadata_sha256", "content_file_count", "content_byte_count", "computed_at"] }
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

def analysis_run_contract : RelationContract where
  name := "analysis_run"
  attributes := ["analysis_id", "build_id", "policy_id", "input_manifest_sha256", "state", "started_at", "completed_at"]
  declaredKeys := [["analysis_id"], ["build_id", "policy_id", "input_manifest_sha256"]]
  declaredFDs := [
    { determinant := ["analysis_id"], dependent := ["build_id", "policy_id", "input_manifest_sha256", "state", "started_at", "completed_at"] },
    { determinant := ["build_id", "policy_id", "input_manifest_sha256"], dependent := ["analysis_id", "state", "started_at", "completed_at"] }
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

theorem analysis_run_bcnf_check :
    bcnfCheck analysis_run_contract = true := by
  native_decide

theorem analysis_run_bcnf : BCNF analysis_run_contract :=
  bcnfCheck_sound analysis_run_contract analysis_run_bcnf_check

def analysis_gallery_artist_contract : RelationContract where
  name := "analysis_gallery_artist"
  attributes := ["analysis_id", "gallery_id", "artist_tag_id"]
  declaredKeys := [["analysis_id", "gallery_id", "artist_tag_id"]]
  declaredFDs := [
  ]

theorem analysis_gallery_artist_schema_well_formed :
    schemaWellFormedCheck analysis_gallery_artist_contract = true := by
  native_decide

theorem analysis_gallery_artist_candidate_keys_check :
    keysDetermineAllCheck analysis_gallery_artist_contract = true := by
  native_decide

theorem analysis_gallery_artist_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_gallery_artist_contract :=
  keysDetermineAllCheck_sound analysis_gallery_artist_contract
    analysis_gallery_artist_candidate_keys_check

theorem analysis_gallery_artist_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_gallery_artist_contract = true := by
  native_decide

theorem analysis_gallery_artist_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_gallery_artist_contract :=
  declaredKeysMinimalCheck_sound analysis_gallery_artist_contract
    analysis_gallery_artist_candidate_keys_minimal_check

theorem analysis_gallery_artist_closure_fixed_check :
    closureFixedPointCheck analysis_gallery_artist_contract = true := by
  native_decide

theorem analysis_gallery_artist_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_gallery_artist_contract :=
  closureFixedPointCheck_sound analysis_gallery_artist_contract
    analysis_gallery_artist_closure_fixed_check

theorem analysis_gallery_artist_bcnf_check :
    bcnfCheck analysis_gallery_artist_contract = true := by
  native_decide

theorem analysis_gallery_artist_bcnf : BCNF analysis_gallery_artist_contract :=
  bcnfCheck_sound analysis_gallery_artist_contract analysis_gallery_artist_bcnf_check

def analysis_gallery_file_hash_occurrence_contract : RelationContract where
  name := "analysis_gallery_file_hash_occurrence"
  attributes := ["analysis_id", "gallery_id", "file_sha256", "occurrence_count"]
  declaredKeys := [["analysis_id", "gallery_id", "file_sha256"]]
  declaredFDs := [
    { determinant := ["analysis_id", "gallery_id", "file_sha256"], dependent := ["occurrence_count"] }
  ]

theorem analysis_gallery_file_hash_occurrence_schema_well_formed :
    schemaWellFormedCheck analysis_gallery_file_hash_occurrence_contract = true := by
  native_decide

theorem analysis_gallery_file_hash_occurrence_candidate_keys_check :
    keysDetermineAllCheck analysis_gallery_file_hash_occurrence_contract = true := by
  native_decide

theorem analysis_gallery_file_hash_occurrence_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_gallery_file_hash_occurrence_contract :=
  keysDetermineAllCheck_sound analysis_gallery_file_hash_occurrence_contract
    analysis_gallery_file_hash_occurrence_candidate_keys_check

theorem analysis_gallery_file_hash_occurrence_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_gallery_file_hash_occurrence_contract = true := by
  native_decide

theorem analysis_gallery_file_hash_occurrence_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_gallery_file_hash_occurrence_contract :=
  declaredKeysMinimalCheck_sound analysis_gallery_file_hash_occurrence_contract
    analysis_gallery_file_hash_occurrence_candidate_keys_minimal_check

theorem analysis_gallery_file_hash_occurrence_closure_fixed_check :
    closureFixedPointCheck analysis_gallery_file_hash_occurrence_contract = true := by
  native_decide

theorem analysis_gallery_file_hash_occurrence_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_gallery_file_hash_occurrence_contract :=
  closureFixedPointCheck_sound analysis_gallery_file_hash_occurrence_contract
    analysis_gallery_file_hash_occurrence_closure_fixed_check

theorem analysis_gallery_file_hash_occurrence_bcnf_check :
    bcnfCheck analysis_gallery_file_hash_occurrence_contract = true := by
  native_decide

theorem analysis_gallery_file_hash_occurrence_bcnf : BCNF analysis_gallery_file_hash_occurrence_contract :=
  bcnfCheck_sound analysis_gallery_file_hash_occurrence_contract analysis_gallery_file_hash_occurrence_bcnf_check

def analysis_file_hash_artist_contribution_contract : RelationContract where
  name := "analysis_file_hash_artist_contribution"
  attributes := ["analysis_id", "file_sha256", "artist_tag_id", "gallery_id", "occurrence_count"]
  declaredKeys := [["analysis_id", "file_sha256", "artist_tag_id", "gallery_id"]]
  declaredFDs := [
    { determinant := ["analysis_id", "file_sha256", "artist_tag_id", "gallery_id"], dependent := ["occurrence_count"] }
  ]

theorem analysis_file_hash_artist_contribution_schema_well_formed :
    schemaWellFormedCheck analysis_file_hash_artist_contribution_contract = true := by
  native_decide

theorem analysis_file_hash_artist_contribution_candidate_keys_check :
    keysDetermineAllCheck analysis_file_hash_artist_contribution_contract = true := by
  native_decide

theorem analysis_file_hash_artist_contribution_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_file_hash_artist_contribution_contract :=
  keysDetermineAllCheck_sound analysis_file_hash_artist_contribution_contract
    analysis_file_hash_artist_contribution_candidate_keys_check

theorem analysis_file_hash_artist_contribution_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_file_hash_artist_contribution_contract = true := by
  native_decide

theorem analysis_file_hash_artist_contribution_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_file_hash_artist_contribution_contract :=
  declaredKeysMinimalCheck_sound analysis_file_hash_artist_contribution_contract
    analysis_file_hash_artist_contribution_candidate_keys_minimal_check

theorem analysis_file_hash_artist_contribution_closure_fixed_check :
    closureFixedPointCheck analysis_file_hash_artist_contribution_contract = true := by
  native_decide

theorem analysis_file_hash_artist_contribution_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_file_hash_artist_contribution_contract :=
  closureFixedPointCheck_sound analysis_file_hash_artist_contribution_contract
    analysis_file_hash_artist_contribution_closure_fixed_check

theorem analysis_file_hash_artist_contribution_bcnf_check :
    bcnfCheck analysis_file_hash_artist_contribution_contract = true := by
  native_decide

theorem analysis_file_hash_artist_contribution_bcnf : BCNF analysis_file_hash_artist_contribution_contract :=
  bcnfCheck_sound analysis_file_hash_artist_contribution_contract analysis_file_hash_artist_contribution_bcnf_check

def analysis_file_hash_artist_stat_contract : RelationContract where
  name := "analysis_file_hash_artist_stat"
  attributes := ["analysis_id", "file_sha256", "artist_tag_id", "occurrence_count", "gallery_count"]
  declaredKeys := [["analysis_id", "file_sha256", "artist_tag_id"]]
  declaredFDs := [
    { determinant := ["analysis_id", "file_sha256", "artist_tag_id"], dependent := ["occurrence_count", "gallery_count"] }
  ]

theorem analysis_file_hash_artist_stat_schema_well_formed :
    schemaWellFormedCheck analysis_file_hash_artist_stat_contract = true := by
  native_decide

theorem analysis_file_hash_artist_stat_candidate_keys_check :
    keysDetermineAllCheck analysis_file_hash_artist_stat_contract = true := by
  native_decide

theorem analysis_file_hash_artist_stat_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_file_hash_artist_stat_contract :=
  keysDetermineAllCheck_sound analysis_file_hash_artist_stat_contract
    analysis_file_hash_artist_stat_candidate_keys_check

theorem analysis_file_hash_artist_stat_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_file_hash_artist_stat_contract = true := by
  native_decide

theorem analysis_file_hash_artist_stat_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_file_hash_artist_stat_contract :=
  declaredKeysMinimalCheck_sound analysis_file_hash_artist_stat_contract
    analysis_file_hash_artist_stat_candidate_keys_minimal_check

theorem analysis_file_hash_artist_stat_closure_fixed_check :
    closureFixedPointCheck analysis_file_hash_artist_stat_contract = true := by
  native_decide

theorem analysis_file_hash_artist_stat_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_file_hash_artist_stat_contract :=
  closureFixedPointCheck_sound analysis_file_hash_artist_stat_contract
    analysis_file_hash_artist_stat_closure_fixed_check

theorem analysis_file_hash_artist_stat_bcnf_check :
    bcnfCheck analysis_file_hash_artist_stat_contract = true := by
  native_decide

theorem analysis_file_hash_artist_stat_bcnf : BCNF analysis_file_hash_artist_stat_contract :=
  bcnfCheck_sound analysis_file_hash_artist_stat_contract analysis_file_hash_artist_stat_bcnf_check

def excluded_file_hash_evidence_contract : RelationContract where
  name := "excluded_file_hash_evidence"
  attributes := ["analysis_id", "file_sha256", "artist_count", "occurrence_count", "evidence_sha256"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
    { determinant := ["analysis_id", "file_sha256"], dependent := ["artist_count", "occurrence_count", "evidence_sha256"] }
  ]

theorem excluded_file_hash_evidence_schema_well_formed :
    schemaWellFormedCheck excluded_file_hash_evidence_contract = true := by
  native_decide

theorem excluded_file_hash_evidence_candidate_keys_check :
    keysDetermineAllCheck excluded_file_hash_evidence_contract = true := by
  native_decide

theorem excluded_file_hash_evidence_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes excluded_file_hash_evidence_contract :=
  keysDetermineAllCheck_sound excluded_file_hash_evidence_contract
    excluded_file_hash_evidence_candidate_keys_check

theorem excluded_file_hash_evidence_candidate_keys_minimal_check :
    declaredKeysMinimalCheck excluded_file_hash_evidence_contract = true := by
  native_decide

theorem excluded_file_hash_evidence_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal excluded_file_hash_evidence_contract :=
  declaredKeysMinimalCheck_sound excluded_file_hash_evidence_contract
    excluded_file_hash_evidence_candidate_keys_minimal_check

theorem excluded_file_hash_evidence_closure_fixed_check :
    closureFixedPointCheck excluded_file_hash_evidence_contract = true := by
  native_decide

theorem excluded_file_hash_evidence_closure_reached_fixed_point :
    ClosureReachedFixedPoint excluded_file_hash_evidence_contract :=
  closureFixedPointCheck_sound excluded_file_hash_evidence_contract
    excluded_file_hash_evidence_closure_fixed_check

theorem excluded_file_hash_evidence_bcnf_check :
    bcnfCheck excluded_file_hash_evidence_contract = true := by
  native_decide

theorem excluded_file_hash_evidence_bcnf : BCNF excluded_file_hash_evidence_contract :=
  bcnfCheck_sound excluded_file_hash_evidence_contract excluded_file_hash_evidence_bcnf_check

def excluded_file_hash_contract : RelationContract where
  name := "excluded_file_hash"
  attributes := ["analysis_id", "file_sha256"]
  declaredKeys := [["analysis_id", "file_sha256"]]
  declaredFDs := [
  ]

theorem excluded_file_hash_schema_well_formed :
    schemaWellFormedCheck excluded_file_hash_contract = true := by
  native_decide

theorem excluded_file_hash_candidate_keys_check :
    keysDetermineAllCheck excluded_file_hash_contract = true := by
  native_decide

theorem excluded_file_hash_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes excluded_file_hash_contract :=
  keysDetermineAllCheck_sound excluded_file_hash_contract
    excluded_file_hash_candidate_keys_check

theorem excluded_file_hash_candidate_keys_minimal_check :
    declaredKeysMinimalCheck excluded_file_hash_contract = true := by
  native_decide

theorem excluded_file_hash_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal excluded_file_hash_contract :=
  declaredKeysMinimalCheck_sound excluded_file_hash_contract
    excluded_file_hash_candidate_keys_minimal_check

theorem excluded_file_hash_closure_fixed_check :
    closureFixedPointCheck excluded_file_hash_contract = true := by
  native_decide

theorem excluded_file_hash_closure_reached_fixed_point :
    ClosureReachedFixedPoint excluded_file_hash_contract :=
  closureFixedPointCheck_sound excluded_file_hash_contract
    excluded_file_hash_closure_fixed_check

theorem excluded_file_hash_bcnf_check :
    bcnfCheck excluded_file_hash_contract = true := by
  native_decide

theorem excluded_file_hash_bcnf : BCNF excluded_file_hash_contract :=
  bcnfCheck_sound excluded_file_hash_contract excluded_file_hash_bcnf_check

def analysis_content_owner_candidate_contract : RelationContract where
  name := "analysis_content_owner_candidate"
  attributes := ["analysis_id", "content_sha256", "gallery_id", "priority_key", "candidate_sha256"]
  declaredKeys := [["analysis_id", "gallery_id"]]
  declaredFDs := [
    { determinant := ["analysis_id", "gallery_id"], dependent := ["content_sha256", "priority_key", "candidate_sha256"] }
  ]

theorem analysis_content_owner_candidate_schema_well_formed :
    schemaWellFormedCheck analysis_content_owner_candidate_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_candidate_keys_check :
    keysDetermineAllCheck analysis_content_owner_candidate_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_content_owner_candidate_contract :=
  keysDetermineAllCheck_sound analysis_content_owner_candidate_contract
    analysis_content_owner_candidate_candidate_keys_check

theorem analysis_content_owner_candidate_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_content_owner_candidate_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_content_owner_candidate_contract :=
  declaredKeysMinimalCheck_sound analysis_content_owner_candidate_contract
    analysis_content_owner_candidate_candidate_keys_minimal_check

theorem analysis_content_owner_candidate_closure_fixed_check :
    closureFixedPointCheck analysis_content_owner_candidate_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_content_owner_candidate_contract :=
  closureFixedPointCheck_sound analysis_content_owner_candidate_contract
    analysis_content_owner_candidate_closure_fixed_check

theorem analysis_content_owner_candidate_bcnf_check :
    bcnfCheck analysis_content_owner_candidate_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_bcnf : BCNF analysis_content_owner_candidate_contract :=
  bcnfCheck_sound analysis_content_owner_candidate_contract analysis_content_owner_candidate_bcnf_check

def analysis_content_owner_contract : RelationContract where
  name := "analysis_content_owner"
  attributes := ["analysis_id", "content_sha256", "owner_gallery_id", "decision_sha256"]
  declaredKeys := [["analysis_id", "content_sha256"]]
  declaredFDs := [
    { determinant := ["analysis_id", "content_sha256"], dependent := ["owner_gallery_id", "decision_sha256"] }
  ]

theorem analysis_content_owner_schema_well_formed :
    schemaWellFormedCheck analysis_content_owner_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_keys_check :
    keysDetermineAllCheck analysis_content_owner_contract = true := by
  native_decide

theorem analysis_content_owner_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_content_owner_contract :=
  keysDetermineAllCheck_sound analysis_content_owner_contract
    analysis_content_owner_candidate_keys_check

theorem analysis_content_owner_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_content_owner_contract = true := by
  native_decide

theorem analysis_content_owner_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_content_owner_contract :=
  declaredKeysMinimalCheck_sound analysis_content_owner_contract
    analysis_content_owner_candidate_keys_minimal_check

theorem analysis_content_owner_closure_fixed_check :
    closureFixedPointCheck analysis_content_owner_contract = true := by
  native_decide

theorem analysis_content_owner_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_content_owner_contract :=
  closureFixedPointCheck_sound analysis_content_owner_contract
    analysis_content_owner_closure_fixed_check

theorem analysis_content_owner_bcnf_check :
    bcnfCheck analysis_content_owner_contract = true := by
  native_decide

theorem analysis_content_owner_bcnf : BCNF analysis_content_owner_contract :=
  bcnfCheck_sound analysis_content_owner_contract analysis_content_owner_bcnf_check

def analysis_gid_candidate_contract : RelationContract where
  name := "analysis_gid_candidate"
  attributes := ["analysis_id", "gid", "gallery_id", "priority_key", "candidate_sha256"]
  declaredKeys := [["analysis_id", "gallery_id"]]
  declaredFDs := [
    { determinant := ["analysis_id", "gallery_id"], dependent := ["gid", "priority_key", "candidate_sha256"] }
  ]

theorem analysis_gid_candidate_schema_well_formed :
    schemaWellFormedCheck analysis_gid_candidate_contract = true := by
  native_decide

theorem analysis_gid_candidate_candidate_keys_check :
    keysDetermineAllCheck analysis_gid_candidate_contract = true := by
  native_decide

theorem analysis_gid_candidate_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_gid_candidate_contract :=
  keysDetermineAllCheck_sound analysis_gid_candidate_contract
    analysis_gid_candidate_candidate_keys_check

theorem analysis_gid_candidate_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_gid_candidate_contract = true := by
  native_decide

theorem analysis_gid_candidate_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_gid_candidate_contract :=
  declaredKeysMinimalCheck_sound analysis_gid_candidate_contract
    analysis_gid_candidate_candidate_keys_minimal_check

theorem analysis_gid_candidate_closure_fixed_check :
    closureFixedPointCheck analysis_gid_candidate_contract = true := by
  native_decide

theorem analysis_gid_candidate_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_gid_candidate_contract :=
  closureFixedPointCheck_sound analysis_gid_candidate_contract
    analysis_gid_candidate_closure_fixed_check

theorem analysis_gid_candidate_bcnf_check :
    bcnfCheck analysis_gid_candidate_contract = true := by
  native_decide

theorem analysis_gid_candidate_bcnf : BCNF analysis_gid_candidate_contract :=
  bcnfCheck_sound analysis_gid_candidate_contract analysis_gid_candidate_bcnf_check

def analysis_gid_winner_contract : RelationContract where
  name := "analysis_gid_winner"
  attributes := ["analysis_id", "gid", "winner_gallery_id", "decision_sha256"]
  declaredKeys := [["analysis_id", "gid"]]
  declaredFDs := [
    { determinant := ["analysis_id", "gid"], dependent := ["winner_gallery_id", "decision_sha256"] }
  ]

theorem analysis_gid_winner_schema_well_formed :
    schemaWellFormedCheck analysis_gid_winner_contract = true := by
  native_decide

theorem analysis_gid_winner_candidate_keys_check :
    keysDetermineAllCheck analysis_gid_winner_contract = true := by
  native_decide

theorem analysis_gid_winner_candidate_keys_determine_all_attributes :
    KeysDetermineAllAttributes analysis_gid_winner_contract :=
  keysDetermineAllCheck_sound analysis_gid_winner_contract
    analysis_gid_winner_candidate_keys_check

theorem analysis_gid_winner_candidate_keys_minimal_check :
    declaredKeysMinimalCheck analysis_gid_winner_contract = true := by
  native_decide

theorem analysis_gid_winner_declared_keys_are_candidate_keys :
    DeclaredKeysAreMinimal analysis_gid_winner_contract :=
  declaredKeysMinimalCheck_sound analysis_gid_winner_contract
    analysis_gid_winner_candidate_keys_minimal_check

theorem analysis_gid_winner_closure_fixed_check :
    closureFixedPointCheck analysis_gid_winner_contract = true := by
  native_decide

theorem analysis_gid_winner_closure_reached_fixed_point :
    ClosureReachedFixedPoint analysis_gid_winner_contract :=
  closureFixedPointCheck_sound analysis_gid_winner_contract
    analysis_gid_winner_closure_fixed_check

theorem analysis_gid_winner_bcnf_check :
    bcnfCheck analysis_gid_winner_contract = true := by
  native_decide

theorem analysis_gid_winner_bcnf : BCNF analysis_gid_winner_contract :=
  bcnfCheck_sound analysis_gid_winner_contract analysis_gid_winner_bcnf_check

def analysis_checkpoint_contract : RelationContract where
  name := "analysis_checkpoint"
  attributes := ["analysis_id", "stage", "generation", "cursor", "output_sha256", "state", "updated_at"]
  declaredKeys := [["analysis_id", "stage"]]
  declaredFDs := [
    { determinant := ["analysis_id", "stage"], dependent := ["generation", "cursor", "output_sha256", "state", "updated_at"] }
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

def analysis_batch_receipt_contract : RelationContract where
  name := "analysis_batch_receipt"
  attributes := ["analysis_id", "stage", "batch_key", "input_sha256", "output_sha256", "row_count", "committed_at"]
  declaredKeys := [["analysis_id", "stage", "batch_key"]]
  declaredFDs := [
    { determinant := ["analysis_id", "stage", "batch_key"], dependent := ["input_sha256", "output_sha256", "row_count", "committed_at"] }
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

theorem analysis_batch_receipt_bcnf_check :
    bcnfCheck analysis_batch_receipt_contract = true := by
  native_decide

theorem analysis_batch_receipt_bcnf : BCNF analysis_batch_receipt_contract :=
  bcnfCheck_sound analysis_batch_receipt_contract analysis_batch_receipt_bcnf_check

def publication_candidate_contract : RelationContract where
  name := "publication_candidate"
  attributes := ["candidate_id", "analysis_id", "reserved_revision", "state", "created_at", "sealed_at"]
  declaredKeys := [["candidate_id"], ["reserved_revision"]]
  declaredFDs := [
    { determinant := ["candidate_id"], dependent := ["analysis_id", "reserved_revision", "state", "created_at", "sealed_at"] },
    { determinant := ["reserved_revision"], dependent := ["candidate_id", "analysis_id", "state", "created_at", "sealed_at"] }
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

def publication_selection_contract : RelationContract where
  name := "publication_selection"
  attributes := ["candidate_id", "gallery_id", "publication_key", "item_sha256"]
  declaredKeys := [["candidate_id", "gallery_id"], ["candidate_id", "publication_key"]]
  declaredFDs := [
    { determinant := ["candidate_id", "gallery_id"], dependent := ["publication_key", "item_sha256"] },
    { determinant := ["candidate_id", "publication_key"], dependent := ["gallery_id", "item_sha256"] }
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

theorem publication_selection_bcnf_check :
    bcnfCheck publication_selection_contract = true := by
  native_decide

theorem publication_selection_bcnf : BCNF publication_selection_contract :=
  bcnfCheck_sound publication_selection_contract publication_selection_bcnf_check

def publication_checkpoint_contract : RelationContract where
  name := "publication_checkpoint"
  attributes := ["candidate_id", "stage", "generation", "cursor", "input_sha256", "output_sha256", "state", "updated_at"]
  declaredKeys := [["candidate_id", "stage"]]
  declaredFDs := [
    { determinant := ["candidate_id", "stage"], dependent := ["generation", "cursor", "input_sha256", "output_sha256", "state", "updated_at"] }
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

def publication_batch_receipt_contract : RelationContract where
  name := "publication_batch_receipt"
  attributes := ["candidate_id", "stage", "batch_key", "input_sha256", "output_sha256", "row_count", "committed_at"]
  declaredKeys := [["candidate_id", "stage", "batch_key"]]
  declaredFDs := [
    { determinant := ["candidate_id", "stage", "batch_key"], dependent := ["input_sha256", "output_sha256", "row_count", "committed_at"] }
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

theorem publication_batch_receipt_bcnf_check :
    bcnfCheck publication_batch_receipt_contract = true := by
  native_decide

theorem publication_batch_receipt_bcnf : BCNF publication_batch_receipt_contract :=
  bcnfCheck_sound publication_batch_receipt_contract publication_batch_receipt_bcnf_check

def artifact_blob_contract : RelationContract where
  name := "artifact_blob"
  attributes := ["artifact_sha256", "size_bytes"]
  declaredKeys := [["artifact_sha256"]]
  declaredFDs := [
    { determinant := ["artifact_sha256"], dependent := ["size_bytes"] }
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
  attributes := ["candidate_id", "publication_key", "artifact_name", "artifact_sha256", "protection_token", "state"]
  declaredKeys := [["candidate_id", "publication_key", "artifact_name"]]
  declaredFDs := [
    { determinant := ["candidate_id", "publication_key", "artifact_name"], dependent := ["artifact_sha256", "protection_token", "state"] }
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

def catalog_revision_contract : RelationContract where
  name := "catalog_revision"
  attributes := ["revision", "candidate_id", "publication_sha256", "item_count", "published_at"]
  declaredKeys := [["revision"], ["candidate_id"]]
  declaredFDs := [
    { determinant := ["revision"], dependent := ["candidate_id", "publication_sha256", "item_count", "published_at"] },
    { determinant := ["candidate_id"], dependent := ["revision", "publication_sha256", "item_count", "published_at"] }
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

theorem catalog_revision_bcnf_check :
    bcnfCheck catalog_revision_contract = true := by
  native_decide

theorem catalog_revision_bcnf : BCNF catalog_revision_contract :=
  bcnfCheck_sound catalog_revision_contract catalog_revision_bcnf_check

def catalog_publication_contract : RelationContract where
  name := "catalog_publication"
  attributes := ["revision", "publication_key", "gallery_id", "publication_id", "gid", "title", "language", "item_sha256"]
  declaredKeys := [["revision", "publication_key"], ["revision", "gallery_id"], ["revision", "publication_id"], ["revision", "gid"]]
  declaredFDs := [
    { determinant := ["revision", "publication_key"], dependent := ["gallery_id", "publication_id", "gid", "title", "language", "item_sha256"] },
    { determinant := ["revision", "gallery_id"], dependent := ["publication_key", "publication_id", "gid", "title", "language", "item_sha256"] },
    { determinant := ["revision", "publication_id"], dependent := ["publication_key", "gallery_id", "gid", "title", "language", "item_sha256"] },
    { determinant := ["revision", "gid"], dependent := ["publication_key", "gallery_id", "publication_id", "title", "language", "item_sha256"] }
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

theorem catalog_publication_bcnf_check :
    bcnfCheck catalog_publication_contract = true := by
  native_decide

theorem catalog_publication_bcnf : BCNF catalog_publication_contract :=
  bcnfCheck_sound catalog_publication_contract catalog_publication_bcnf_check

def catalog_contributor_contract : RelationContract where
  name := "catalog_contributor"
  attributes := ["revision", "publication_key", "position", "tag_id", "role", "sort_as"]
  declaredKeys := [["revision", "publication_key", "position"]]
  declaredFDs := [
    { determinant := ["revision", "publication_key", "position"], dependent := ["tag_id", "role", "sort_as"] }
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
  declaredKeys := [["revision", "publication_key", "position"]]
  declaredFDs := [
    { determinant := ["revision", "publication_key", "position"], dependent := ["tag_id"] }
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
  attributes := ["revision", "publication_key", "artifact_name", "artifact_sha256", "locator"]
  declaredKeys := [["revision", "publication_key", "artifact_name"]]
  declaredFDs := [
    { determinant := ["revision", "publication_key", "artifact_name"], dependent := ["artifact_sha256", "locator"] }
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

def publication_receipt_contract : RelationContract where
  name := "publication_receipt"
  attributes := ["receipt_id", "revision", "candidate_id", "source_manifest_sha256", "publication_sha256", "state", "committed_at", "finalized_at"]
  declaredKeys := [["receipt_id"], ["revision"], ["candidate_id"]]
  declaredFDs := [
    { determinant := ["receipt_id"], dependent := ["revision", "candidate_id", "source_manifest_sha256", "publication_sha256", "state", "committed_at", "finalized_at"] },
    { determinant := ["revision"], dependent := ["receipt_id", "candidate_id", "source_manifest_sha256", "publication_sha256", "state", "committed_at", "finalized_at"] },
    { determinant := ["candidate_id"], dependent := ["receipt_id", "revision", "source_manifest_sha256", "publication_sha256", "state", "committed_at", "finalized_at"] }
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

theorem publication_receipt_bcnf_check :
    bcnfCheck publication_receipt_contract = true := by
  native_decide

theorem publication_receipt_bcnf : BCNF publication_receipt_contract :=
  bcnfCheck_sound publication_receipt_contract publication_receipt_bcnf_check

def publication_head_contract : RelationContract where
  name := "publication_head"
  attributes := ["channel", "revision", "generation", "advanced_at"]
  declaredKeys := [["channel"]]
  declaredFDs := [
    { determinant := ["channel"], dependent := ["revision", "generation", "advanced_at"] }
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

theorem publication_head_bcnf_check :
    bcnfCheck publication_head_contract = true := by
  native_decide

theorem publication_head_bcnf : BCNF publication_head_contract :=
  bcnfCheck_sound publication_head_contract publication_head_bcnf_check

def manifestContracts : List RelationContract := [
  manifest_policy_contract,
  source_build_contract,
  gallery_identity_contract,
  source_gallery_contract,
  file_name_identity_contract,
  content_blob_contract,
  source_file_contract,
  tag_term_contract,
  source_gallery_tag_contract,
  build_manifest_contract,
  gallery_manifest_contract,
  analysis_policy_contract,
  analysis_run_contract,
  analysis_gallery_artist_contract,
  analysis_gallery_file_hash_occurrence_contract,
  analysis_file_hash_artist_contribution_contract,
  analysis_file_hash_artist_stat_contract,
  excluded_file_hash_evidence_contract,
  excluded_file_hash_contract,
  analysis_content_owner_candidate_contract,
  analysis_content_owner_contract,
  analysis_gid_candidate_contract,
  analysis_gid_winner_contract,
  analysis_checkpoint_contract,
  analysis_batch_receipt_contract,
  publication_candidate_contract,
  publication_selection_contract,
  publication_checkpoint_contract,
  publication_batch_receipt_contract,
  artifact_blob_contract,
  prepared_artifact_contract,
  catalog_revision_contract,
  catalog_publication_contract,
  catalog_contributor_contract,
  catalog_subject_contract,
  catalog_artifact_contract,
  publication_receipt_contract,
  publication_head_contract
]

theorem manifest_relation_count :
    manifestContracts.length = 38 := by
  native_decide

def gallery_identity_and_source_occurrence_contract : BinaryDecompositionContract where
  name := "gallery_identity_and_source_occurrence"
  universalAttributes := ["gallery_key", "gallery_name", "gallery_id", "build_id", "source_locator", "gid", "metadata_observation_sha256", "source_complete"]
  leftAttributes := ["gallery_key", "gallery_name"]
  rightAttributes := ["gallery_id", "build_id", "gallery_key", "source_locator", "gid", "metadata_observation_sha256", "source_complete"]
  declaredFDs := [
    { determinant := ["gallery_key"], dependent := ["gallery_name"] },
    { determinant := ["gallery_name"], dependent := ["gallery_key"] },
    { determinant := ["gallery_id"], dependent := ["build_id", "gallery_key", "source_locator", "gid", "metadata_observation_sha256", "source_complete"] },
    { determinant := ["build_id", "gallery_key"], dependent := ["gallery_id", "source_locator", "gid", "metadata_observation_sha256", "source_complete"] },
    { determinant := ["build_id", "source_locator"], dependent := ["gallery_id", "gallery_key", "gid", "metadata_observation_sha256", "source_complete"] }
  ]

theorem gallery_identity_and_source_occurrence_projection_check :
    binaryDecompositionWellFormedCheck
      gallery_identity_and_source_occurrence_contract = true := by
  native_decide

theorem gallery_identity_and_source_occurrence_projection_well_formed :
    BinaryDecompositionWellFormed gallery_identity_and_source_occurrence_contract :=
  binaryDecompositionWellFormedCheck_sound
    gallery_identity_and_source_occurrence_contract gallery_identity_and_source_occurrence_projection_check

theorem gallery_identity_and_source_occurrence_intersection_check :
    sameAttrSet (attributeIntersection
      gallery_identity_and_source_occurrence_contract.leftAttributes
      gallery_identity_and_source_occurrence_contract.rightAttributes)
      ["gallery_key"] = true := by
  native_decide

theorem gallery_identity_and_source_occurrence_lossless_check :
    binaryLosslessCheck gallery_identity_and_source_occurrence_contract = true := by
  native_decide

theorem gallery_identity_and_source_occurrence_lossless : BinaryLossless gallery_identity_and_source_occurrence_contract :=
  ⟨gallery_identity_and_source_occurrence_projection_well_formed,
    binaryLosslessCheck_sound gallery_identity_and_source_occurrence_contract
      gallery_identity_and_source_occurrence_lossless_check⟩

theorem gallery_identity_and_source_occurrence_dependency_preservation_check :
    dependencyPreservationCheck gallery_identity_and_source_occurrence_contract = true := by
  native_decide

theorem gallery_identity_and_source_occurrence_dependency_preserving :
    DependencyPreserving gallery_identity_and_source_occurrence_contract :=
  dependencyPreservationCheck_sound gallery_identity_and_source_occurrence_contract
    gallery_identity_and_source_occurrence_dependency_preservation_check

def file_identity_and_source_occurrence_contract : BinaryDecompositionContract where
  name := "file_identity_and_source_occurrence"
  universalAttributes := ["file_key", "name_bytes", "role", "file_observation_id", "gallery_id", "file_no", "mtime_ns", "file_sha256"]
  leftAttributes := ["file_key", "name_bytes", "role"]
  rightAttributes := ["file_observation_id", "gallery_id", "file_no", "file_key", "mtime_ns", "file_sha256"]
  declaredFDs := [
    { determinant := ["file_key"], dependent := ["name_bytes", "role"] },
    { determinant := ["name_bytes"], dependent := ["file_key", "role"] },
    { determinant := ["file_observation_id"], dependent := ["gallery_id", "file_no", "file_key", "mtime_ns", "file_sha256"] },
    { determinant := ["gallery_id", "file_no"], dependent := ["file_observation_id", "file_key", "mtime_ns", "file_sha256"] },
    { determinant := ["gallery_id", "file_key"], dependent := ["file_observation_id", "file_no", "mtime_ns", "file_sha256"] }
  ]

theorem file_identity_and_source_occurrence_projection_check :
    binaryDecompositionWellFormedCheck
      file_identity_and_source_occurrence_contract = true := by
  native_decide

theorem file_identity_and_source_occurrence_projection_well_formed :
    BinaryDecompositionWellFormed file_identity_and_source_occurrence_contract :=
  binaryDecompositionWellFormedCheck_sound
    file_identity_and_source_occurrence_contract file_identity_and_source_occurrence_projection_check

theorem file_identity_and_source_occurrence_intersection_check :
    sameAttrSet (attributeIntersection
      file_identity_and_source_occurrence_contract.leftAttributes
      file_identity_and_source_occurrence_contract.rightAttributes)
      ["file_key"] = true := by
  native_decide

theorem file_identity_and_source_occurrence_lossless_check :
    binaryLosslessCheck file_identity_and_source_occurrence_contract = true := by
  native_decide

theorem file_identity_and_source_occurrence_lossless : BinaryLossless file_identity_and_source_occurrence_contract :=
  ⟨file_identity_and_source_occurrence_projection_well_formed,
    binaryLosslessCheck_sound file_identity_and_source_occurrence_contract
      file_identity_and_source_occurrence_lossless_check⟩

theorem file_identity_and_source_occurrence_dependency_preservation_check :
    dependencyPreservationCheck file_identity_and_source_occurrence_contract = true := by
  native_decide

theorem file_identity_and_source_occurrence_dependency_preserving :
    DependencyPreserving file_identity_and_source_occurrence_contract :=
  dependencyPreservationCheck_sound file_identity_and_source_occurrence_contract
    file_identity_and_source_occurrence_dependency_preservation_check

def file_content_payload_and_source_occurrence_contract : BinaryDecompositionContract where
  name := "file_content_payload_and_source_occurrence"
  universalAttributes := ["file_observation_id", "gallery_id", "file_no", "file_key", "mtime_ns", "file_sha256", "size_bytes"]
  leftAttributes := ["file_sha256", "size_bytes"]
  rightAttributes := ["file_observation_id", "gallery_id", "file_no", "file_key", "mtime_ns", "file_sha256"]
  declaredFDs := [
    { determinant := ["file_sha256"], dependent := ["size_bytes"] },
    { determinant := ["file_observation_id"], dependent := ["gallery_id", "file_no", "file_key", "mtime_ns", "file_sha256"] },
    { determinant := ["gallery_id", "file_no"], dependent := ["file_observation_id", "file_key", "mtime_ns", "file_sha256"] },
    { determinant := ["gallery_id", "file_key"], dependent := ["file_observation_id", "file_no", "mtime_ns", "file_sha256"] }
  ]

theorem file_content_payload_and_source_occurrence_projection_check :
    binaryDecompositionWellFormedCheck
      file_content_payload_and_source_occurrence_contract = true := by
  native_decide

theorem file_content_payload_and_source_occurrence_projection_well_formed :
    BinaryDecompositionWellFormed file_content_payload_and_source_occurrence_contract :=
  binaryDecompositionWellFormedCheck_sound
    file_content_payload_and_source_occurrence_contract file_content_payload_and_source_occurrence_projection_check

theorem file_content_payload_and_source_occurrence_intersection_check :
    sameAttrSet (attributeIntersection
      file_content_payload_and_source_occurrence_contract.leftAttributes
      file_content_payload_and_source_occurrence_contract.rightAttributes)
      ["file_sha256"] = true := by
  native_decide

theorem file_content_payload_and_source_occurrence_lossless_check :
    binaryLosslessCheck file_content_payload_and_source_occurrence_contract = true := by
  native_decide

theorem file_content_payload_and_source_occurrence_lossless : BinaryLossless file_content_payload_and_source_occurrence_contract :=
  ⟨file_content_payload_and_source_occurrence_projection_well_formed,
    binaryLosslessCheck_sound file_content_payload_and_source_occurrence_contract
      file_content_payload_and_source_occurrence_lossless_check⟩

theorem file_content_payload_and_source_occurrence_dependency_preservation_check :
    dependencyPreservationCheck file_content_payload_and_source_occurrence_contract = true := by
  native_decide

theorem file_content_payload_and_source_occurrence_dependency_preserving :
    DependencyPreserving file_content_payload_and_source_occurrence_contract :=
  dependencyPreservationCheck_sound file_content_payload_and_source_occurrence_contract
    file_content_payload_and_source_occurrence_dependency_preservation_check

def tag_identity_and_gallery_association_contract : BinaryDecompositionContract where
  name := "tag_identity_and_gallery_association"
  universalAttributes := ["tag_id", "namespace", "normalized_value", "display_value", "gallery_id", "position"]
  leftAttributes := ["tag_id", "namespace", "normalized_value", "display_value"]
  rightAttributes := ["gallery_id", "position", "tag_id"]
  declaredFDs := [
    { determinant := ["tag_id"], dependent := ["namespace", "normalized_value", "display_value"] },
    { determinant := ["namespace", "normalized_value"], dependent := ["tag_id", "display_value"] },
    { determinant := ["gallery_id", "position"], dependent := ["tag_id"] }
  ]

theorem tag_identity_and_gallery_association_projection_check :
    binaryDecompositionWellFormedCheck
      tag_identity_and_gallery_association_contract = true := by
  native_decide

theorem tag_identity_and_gallery_association_projection_well_formed :
    BinaryDecompositionWellFormed tag_identity_and_gallery_association_contract :=
  binaryDecompositionWellFormedCheck_sound
    tag_identity_and_gallery_association_contract tag_identity_and_gallery_association_projection_check

theorem tag_identity_and_gallery_association_intersection_check :
    sameAttrSet (attributeIntersection
      tag_identity_and_gallery_association_contract.leftAttributes
      tag_identity_and_gallery_association_contract.rightAttributes)
      ["tag_id"] = true := by
  native_decide

theorem tag_identity_and_gallery_association_lossless_check :
    binaryLosslessCheck tag_identity_and_gallery_association_contract = true := by
  native_decide

theorem tag_identity_and_gallery_association_lossless : BinaryLossless tag_identity_and_gallery_association_contract :=
  ⟨tag_identity_and_gallery_association_projection_well_formed,
    binaryLosslessCheck_sound tag_identity_and_gallery_association_contract
      tag_identity_and_gallery_association_lossless_check⟩

theorem tag_identity_and_gallery_association_dependency_preservation_check :
    dependencyPreservationCheck tag_identity_and_gallery_association_contract = true := by
  native_decide

theorem tag_identity_and_gallery_association_dependency_preserving :
    DependencyPreserving tag_identity_and_gallery_association_contract :=
  dependencyPreservationCheck_sound tag_identity_and_gallery_association_contract
    tag_identity_and_gallery_association_dependency_preservation_check

def artifact_payload_and_preparation_occurrence_contract : BinaryDecompositionContract where
  name := "artifact_payload_and_preparation_occurrence"
  universalAttributes := ["candidate_id", "publication_key", "artifact_name", "artifact_sha256", "size_bytes", "protection_token", "state"]
  leftAttributes := ["artifact_sha256", "size_bytes"]
  rightAttributes := ["candidate_id", "publication_key", "artifact_name", "artifact_sha256", "protection_token", "state"]
  declaredFDs := [
    { determinant := ["artifact_sha256"], dependent := ["size_bytes"] },
    { determinant := ["candidate_id", "publication_key", "artifact_name"], dependent := ["artifact_sha256", "protection_token", "state"] }
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

def artifact_payload_and_catalog_occurrence_contract : BinaryDecompositionContract where
  name := "artifact_payload_and_catalog_occurrence"
  universalAttributes := ["revision", "publication_key", "artifact_name", "artifact_sha256", "size_bytes", "locator"]
  leftAttributes := ["artifact_sha256", "size_bytes"]
  rightAttributes := ["revision", "publication_key", "artifact_name", "artifact_sha256", "locator"]
  declaredFDs := [
    { determinant := ["artifact_sha256"], dependent := ["size_bytes"] },
    { determinant := ["revision", "publication_key", "artifact_name"], dependent := ["artifact_sha256", "locator"] }
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

def excluded_file_hash_evidence_and_membership_contract : BinaryDecompositionContract where
  name := "excluded_file_hash_evidence_and_membership"
  universalAttributes := ["analysis_id", "file_sha256", "artist_count", "occurrence_count", "evidence_sha256"]
  leftAttributes := ["analysis_id", "file_sha256", "artist_count", "occurrence_count", "evidence_sha256"]
  rightAttributes := ["analysis_id", "file_sha256"]
  declaredFDs := [
    { determinant := ["analysis_id", "file_sha256"], dependent := ["artist_count", "occurrence_count", "evidence_sha256"] }
  ]

theorem excluded_file_hash_evidence_and_membership_projection_check :
    binaryDecompositionWellFormedCheck
      excluded_file_hash_evidence_and_membership_contract = true := by
  native_decide

theorem excluded_file_hash_evidence_and_membership_projection_well_formed :
    BinaryDecompositionWellFormed excluded_file_hash_evidence_and_membership_contract :=
  binaryDecompositionWellFormedCheck_sound
    excluded_file_hash_evidence_and_membership_contract excluded_file_hash_evidence_and_membership_projection_check

theorem excluded_file_hash_evidence_and_membership_intersection_check :
    sameAttrSet (attributeIntersection
      excluded_file_hash_evidence_and_membership_contract.leftAttributes
      excluded_file_hash_evidence_and_membership_contract.rightAttributes)
      ["analysis_id", "file_sha256"] = true := by
  native_decide

theorem excluded_file_hash_evidence_and_membership_lossless_check :
    binaryLosslessCheck excluded_file_hash_evidence_and_membership_contract = true := by
  native_decide

theorem excluded_file_hash_evidence_and_membership_lossless : BinaryLossless excluded_file_hash_evidence_and_membership_contract :=
  ⟨excluded_file_hash_evidence_and_membership_projection_well_formed,
    binaryLosslessCheck_sound excluded_file_hash_evidence_and_membership_contract
      excluded_file_hash_evidence_and_membership_lossless_check⟩

theorem excluded_file_hash_evidence_and_membership_dependency_preservation_check :
    dependencyPreservationCheck excluded_file_hash_evidence_and_membership_contract = true := by
  native_decide

theorem excluded_file_hash_evidence_and_membership_dependency_preserving :
    DependencyPreserving excluded_file_hash_evidence_and_membership_contract :=
  dependencyPreservationCheck_sound excluded_file_hash_evidence_and_membership_contract
    excluded_file_hash_evidence_and_membership_dependency_preservation_check

theorem all_manifest_decompositions_lossless :
    BinaryLossless gallery_identity_and_source_occurrence_contract ∧
    BinaryLossless file_identity_and_source_occurrence_contract ∧
    BinaryLossless file_content_payload_and_source_occurrence_contract ∧
    BinaryLossless tag_identity_and_gallery_association_contract ∧
    BinaryLossless artifact_payload_and_preparation_occurrence_contract ∧
    BinaryLossless artifact_payload_and_catalog_occurrence_contract ∧
    BinaryLossless excluded_file_hash_evidence_and_membership_contract := by
  exact ⟨gallery_identity_and_source_occurrence_lossless,
    file_identity_and_source_occurrence_lossless,
    file_content_payload_and_source_occurrence_lossless,
    tag_identity_and_gallery_association_lossless,
    artifact_payload_and_preparation_occurrence_lossless,
    artifact_payload_and_catalog_occurrence_lossless,
    excluded_file_hash_evidence_and_membership_lossless⟩

theorem all_manifest_decompositions_dependency_preserving :
    DependencyPreserving gallery_identity_and_source_occurrence_contract ∧
    DependencyPreserving file_identity_and_source_occurrence_contract ∧
    DependencyPreserving file_content_payload_and_source_occurrence_contract ∧
    DependencyPreserving tag_identity_and_gallery_association_contract ∧
    DependencyPreserving artifact_payload_and_preparation_occurrence_contract ∧
    DependencyPreserving artifact_payload_and_catalog_occurrence_contract ∧
    DependencyPreserving excluded_file_hash_evidence_and_membership_contract := by
  exact ⟨gallery_identity_and_source_occurrence_dependency_preserving,
    file_identity_and_source_occurrence_dependency_preserving,
    file_content_payload_and_source_occurrence_dependency_preserving,
    tag_identity_and_gallery_association_dependency_preserving,
    artifact_payload_and_preparation_occurrence_dependency_preserving,
    artifact_payload_and_catalog_occurrence_dependency_preserving,
    excluded_file_hash_evidence_and_membership_dependency_preserving⟩

theorem all_manifest_relations_bcnf :
    BCNF manifest_policy_contract ∧
    BCNF source_build_contract ∧
    BCNF gallery_identity_contract ∧
    BCNF source_gallery_contract ∧
    BCNF file_name_identity_contract ∧
    BCNF content_blob_contract ∧
    BCNF source_file_contract ∧
    BCNF tag_term_contract ∧
    BCNF source_gallery_tag_contract ∧
    BCNF build_manifest_contract ∧
    BCNF gallery_manifest_contract ∧
    BCNF analysis_policy_contract ∧
    BCNF analysis_run_contract ∧
    BCNF analysis_gallery_artist_contract ∧
    BCNF analysis_gallery_file_hash_occurrence_contract ∧
    BCNF analysis_file_hash_artist_contribution_contract ∧
    BCNF analysis_file_hash_artist_stat_contract ∧
    BCNF excluded_file_hash_evidence_contract ∧
    BCNF excluded_file_hash_contract ∧
    BCNF analysis_content_owner_candidate_contract ∧
    BCNF analysis_content_owner_contract ∧
    BCNF analysis_gid_candidate_contract ∧
    BCNF analysis_gid_winner_contract ∧
    BCNF analysis_checkpoint_contract ∧
    BCNF analysis_batch_receipt_contract ∧
    BCNF publication_candidate_contract ∧
    BCNF publication_selection_contract ∧
    BCNF publication_checkpoint_contract ∧
    BCNF publication_batch_receipt_contract ∧
    BCNF artifact_blob_contract ∧
    BCNF prepared_artifact_contract ∧
    BCNF catalog_revision_contract ∧
    BCNF catalog_publication_contract ∧
    BCNF catalog_contributor_contract ∧
    BCNF catalog_subject_contract ∧
    BCNF catalog_artifact_contract ∧
    BCNF publication_receipt_contract ∧
    BCNF publication_head_contract := by
  exact ⟨manifest_policy_bcnf,
    source_build_bcnf,
    gallery_identity_bcnf,
    source_gallery_bcnf,
    file_name_identity_bcnf,
    content_blob_bcnf,
    source_file_bcnf,
    tag_term_bcnf,
    source_gallery_tag_bcnf,
    build_manifest_bcnf,
    gallery_manifest_bcnf,
    analysis_policy_bcnf,
    analysis_run_bcnf,
    analysis_gallery_artist_bcnf,
    analysis_gallery_file_hash_occurrence_bcnf,
    analysis_file_hash_artist_contribution_bcnf,
    analysis_file_hash_artist_stat_bcnf,
    excluded_file_hash_evidence_bcnf,
    excluded_file_hash_bcnf,
    analysis_content_owner_candidate_bcnf,
    analysis_content_owner_bcnf,
    analysis_gid_candidate_bcnf,
    analysis_gid_winner_bcnf,
    analysis_checkpoint_bcnf,
    analysis_batch_receipt_bcnf,
    publication_candidate_bcnf,
    publication_selection_bcnf,
    publication_checkpoint_bcnf,
    publication_batch_receipt_bcnf,
    artifact_blob_bcnf,
    prepared_artifact_bcnf,
    catalog_revision_bcnf,
    catalog_publication_bcnf,
    catalog_contributor_bcnf,
    catalog_subject_bcnf,
    catalog_artifact_bcnf,
    publication_receipt_bcnf,
    publication_head_bcnf⟩

theorem all_manifest_candidate_keys_determine_attributes :
    KeysDetermineAllAttributes manifest_policy_contract ∧
    KeysDetermineAllAttributes source_build_contract ∧
    KeysDetermineAllAttributes gallery_identity_contract ∧
    KeysDetermineAllAttributes source_gallery_contract ∧
    KeysDetermineAllAttributes file_name_identity_contract ∧
    KeysDetermineAllAttributes content_blob_contract ∧
    KeysDetermineAllAttributes source_file_contract ∧
    KeysDetermineAllAttributes tag_term_contract ∧
    KeysDetermineAllAttributes source_gallery_tag_contract ∧
    KeysDetermineAllAttributes build_manifest_contract ∧
    KeysDetermineAllAttributes gallery_manifest_contract ∧
    KeysDetermineAllAttributes analysis_policy_contract ∧
    KeysDetermineAllAttributes analysis_run_contract ∧
    KeysDetermineAllAttributes analysis_gallery_artist_contract ∧
    KeysDetermineAllAttributes analysis_gallery_file_hash_occurrence_contract ∧
    KeysDetermineAllAttributes analysis_file_hash_artist_contribution_contract ∧
    KeysDetermineAllAttributes analysis_file_hash_artist_stat_contract ∧
    KeysDetermineAllAttributes excluded_file_hash_evidence_contract ∧
    KeysDetermineAllAttributes excluded_file_hash_contract ∧
    KeysDetermineAllAttributes analysis_content_owner_candidate_contract ∧
    KeysDetermineAllAttributes analysis_content_owner_contract ∧
    KeysDetermineAllAttributes analysis_gid_candidate_contract ∧
    KeysDetermineAllAttributes analysis_gid_winner_contract ∧
    KeysDetermineAllAttributes analysis_checkpoint_contract ∧
    KeysDetermineAllAttributes analysis_batch_receipt_contract ∧
    KeysDetermineAllAttributes publication_candidate_contract ∧
    KeysDetermineAllAttributes publication_selection_contract ∧
    KeysDetermineAllAttributes publication_checkpoint_contract ∧
    KeysDetermineAllAttributes publication_batch_receipt_contract ∧
    KeysDetermineAllAttributes artifact_blob_contract ∧
    KeysDetermineAllAttributes prepared_artifact_contract ∧
    KeysDetermineAllAttributes catalog_revision_contract ∧
    KeysDetermineAllAttributes catalog_publication_contract ∧
    KeysDetermineAllAttributes catalog_contributor_contract ∧
    KeysDetermineAllAttributes catalog_subject_contract ∧
    KeysDetermineAllAttributes catalog_artifact_contract ∧
    KeysDetermineAllAttributes publication_receipt_contract ∧
    KeysDetermineAllAttributes publication_head_contract := by
  exact ⟨manifest_policy_candidate_keys_determine_all_attributes,
    source_build_candidate_keys_determine_all_attributes,
    gallery_identity_candidate_keys_determine_all_attributes,
    source_gallery_candidate_keys_determine_all_attributes,
    file_name_identity_candidate_keys_determine_all_attributes,
    content_blob_candidate_keys_determine_all_attributes,
    source_file_candidate_keys_determine_all_attributes,
    tag_term_candidate_keys_determine_all_attributes,
    source_gallery_tag_candidate_keys_determine_all_attributes,
    build_manifest_candidate_keys_determine_all_attributes,
    gallery_manifest_candidate_keys_determine_all_attributes,
    analysis_policy_candidate_keys_determine_all_attributes,
    analysis_run_candidate_keys_determine_all_attributes,
    analysis_gallery_artist_candidate_keys_determine_all_attributes,
    analysis_gallery_file_hash_occurrence_candidate_keys_determine_all_attributes,
    analysis_file_hash_artist_contribution_candidate_keys_determine_all_attributes,
    analysis_file_hash_artist_stat_candidate_keys_determine_all_attributes,
    excluded_file_hash_evidence_candidate_keys_determine_all_attributes,
    excluded_file_hash_candidate_keys_determine_all_attributes,
    analysis_content_owner_candidate_candidate_keys_determine_all_attributes,
    analysis_content_owner_candidate_keys_determine_all_attributes,
    analysis_gid_candidate_candidate_keys_determine_all_attributes,
    analysis_gid_winner_candidate_keys_determine_all_attributes,
    analysis_checkpoint_candidate_keys_determine_all_attributes,
    analysis_batch_receipt_candidate_keys_determine_all_attributes,
    publication_candidate_candidate_keys_determine_all_attributes,
    publication_selection_candidate_keys_determine_all_attributes,
    publication_checkpoint_candidate_keys_determine_all_attributes,
    publication_batch_receipt_candidate_keys_determine_all_attributes,
    artifact_blob_candidate_keys_determine_all_attributes,
    prepared_artifact_candidate_keys_determine_all_attributes,
    catalog_revision_candidate_keys_determine_all_attributes,
    catalog_publication_candidate_keys_determine_all_attributes,
    catalog_contributor_candidate_keys_determine_all_attributes,
    catalog_subject_candidate_keys_determine_all_attributes,
    catalog_artifact_candidate_keys_determine_all_attributes,
    publication_receipt_candidate_keys_determine_all_attributes,
    publication_head_candidate_keys_determine_all_attributes⟩

/- END GENERATED CATALOG CONTRACTS -/

end H2HDB.Verification.VNextSchema
