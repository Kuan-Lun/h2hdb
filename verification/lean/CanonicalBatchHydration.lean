import Std

/-!
# Bounded canonical batch hydration refinement

The reference catalog reader validates each canonical reference separately.
The optimized reader may first cache an exact single-page result for a bounded
batch and falls back to the reference reader for every cache miss, including
all multi-page values. `ExactCache` requires every cache hit to equal the
reference authority, including its digest domain and validated payload.

The theorems are unbounded over references, domains, payloads, failures, and
authority functions. The executable implementation separately hard-caps each
SQL batch at 128 references; `bounded_batch_observational_equivalence` carries
that production precondition explicitly. These theorems do not prove Python
cache construction, SQL row decoding, page hashing, transaction snapshots, or
SQLite/MariaDB refinement. Differential and backend tests cover those
boundaries.
-/

namespace H2HDB.Verification.CanonicalBatchHydration

structure Reference where
  digest : Nat
  domain : Nat
deriving DecidableEq, Repr

abbrev Payload := List Nat

inductive ReadError where
  | missing
  | incomplete
  | collision
deriving DecidableEq, Repr

abbrev ReadResult := Except ReadError Payload
abbrev Authority := Reference → ReadResult
abbrev Cache := Reference → Option Payload

def referenceRead (authority : Authority) (reference : Reference) : ReadResult :=
  authority reference

def optimizedRead
    (authority : Authority)
    (cache : Cache)
    (reference : Reference) : ReadResult :=
  match cache reference with
  | none => referenceRead authority reference
  | some payload => .ok payload

def ExactCache (authority : Authority) (cache : Cache) : Prop :=
  ∀ reference payload, cache reference = some payload →
    authority reference = .ok payload

theorem exact_cache_hit_or_fallback_equals_reference
    (authority : Authority)
    (cache : Cache)
    (exact : ExactCache authority cache)
    (reference : Reference) :
    optimizedRead authority cache reference = referenceRead authority reference := by
  unfold optimizedRead
  cases cached : cache reference with
  | none => rfl
  | some payload =>
      simp only
      exact (exact reference payload cached).symm

def referenceHydrate
    (authority : Authority)
    (references : List Reference) : List ReadResult :=
  references.map (referenceRead authority)

def optimizedHydrate
    (authority : Authority)
    (cache : Cache)
    (references : List Reference) : List ReadResult :=
  references.map (optimizedRead authority cache)

theorem batch_hydration_observationally_equals_per_value_reference
    (authority : Authority)
    (cache : Cache)
    (references : List Reference)
    (exact : ExactCache authority cache) :
    optimizedHydrate authority cache references =
      referenceHydrate authority references := by
  simp only [optimizedHydrate, referenceHydrate, List.map_inj_left]
  intro reference _member
  exact exact_cache_hit_or_fallback_equals_reference authority cache exact reference

def BoundedBatch (references : List Reference) : Prop :=
  references.length ≤ 128

theorem bounded_batch_observational_equivalence
    (authority : Authority)
    (cache : Cache)
    (references : List Reference)
    (exact : ExactCache authority cache)
    (_bounded : BoundedBatch references) :
    optimizedHydrate authority cache references =
      referenceHydrate authority references :=
  batch_hydration_observationally_equals_per_value_reference
    authority cache references exact

theorem multi_page_cache_miss_uses_exact_reference
    (authority : Authority)
    (cache : Cache)
    (reference : Reference)
    (notCached : cache reference = none) :
    optimizedRead authority cache reference = referenceRead authority reference := by
  simp [optimizedRead, notCached]

end H2HDB.Verification.CanonicalBatchHydration
