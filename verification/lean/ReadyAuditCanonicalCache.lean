import Std

/-!
# Snapshot-scoped canonical READY-audit cache

`authority` is the immutable canonical-value result visible in one database
read snapshot. A cache may retain or evict any entries, but every retained hit
must equal that authority. The theorems prove that hits, misses, arbitrary
access sequences, and failure-preserving updates have the same observable
result as revalidating the snapshot every time. This is a semantic model only:
even an always-miss implementation satisfies these equivalence theorems.

These results are unbounded over key and payload types under the stated
snapshot and cache-soundness assumptions. They do not prove that Python's LRU
implementation enforces the bounds, that SQL/Python canonical validation is
correct, that a connector supplies one immutable transaction snapshot, or that
production code refines this model. Differential, corruption, bound, and full
READY-audit tests provide that separate implementation evidence. The concrete
state transitions, derived storage bounds, and miss costs are modeled separately
in `ReadyAuditCacheCost.lean`; storage bounds are not assumed in this structure.
-/

namespace H2HDB.Verification.ReadyAuditCanonicalCache

structure SnapshotCache
    (Key Payload : Type)
    (authority : Key → Option Payload) where
  lookup : Key → Option Payload
  sound : ∀ key payload, lookup key = some payload →
    authority key = some payload

def readWithCache
    {Key Payload : Type}
    (authority : Key → Option Payload)
    (cache : SnapshotCache Key Payload authority)
    (key : Key) : Option Payload :=
  match cache.lookup key with
  | some payload => some payload
  | none => authority key

theorem cache_hit_equals_snapshot
    {Key Payload : Type}
    (authority : Key → Option Payload)
    (cache : SnapshotCache Key Payload authority)
    (key : Key)
    (payload : Payload)
    (hit : cache.lookup key = some payload) :
    readWithCache authority cache key = authority key := by
  rw [readWithCache, hit]
  exact (cache.sound key payload hit).symm

theorem cache_miss_revalidates_snapshot
    {Key Payload : Type}
    (authority : Key → Option Payload)
    (cache : SnapshotCache Key Payload authority)
    (key : Key)
    (miss : cache.lookup key = none) :
    readWithCache authority cache key = authority key := by
  simp [readWithCache, miss]

theorem every_cached_read_equals_snapshot
    {Key Payload : Type}
    (authority : Key → Option Payload)
    (cache : SnapshotCache Key Payload authority)
    (key : Key) :
    readWithCache authority cache key = authority key := by
  cases hit : cache.lookup key with
  | none => exact cache_miss_revalidates_snapshot authority cache key hit
  | some payload =>
      exact cache_hit_equals_snapshot authority cache key payload hit

def readSequence
    {Key Payload : Type}
    (authority : Key → Option Payload)
    (cache : SnapshotCache Key Payload authority)
    (keys : List Key) : List (Option Payload) :=
  keys.map (readWithCache authority cache)

theorem arbitrary_access_sequence_equals_revalidation
    {Key Payload : Type}
    (authority : Key → Option Payload)
    (cache : SnapshotCache Key Payload authority)
    (keys : List Key) :
    readSequence authority cache keys = keys.map authority := by
  unfold readSequence
  apply List.map_congr_left
  intro key _membership
  exact every_cached_read_equals_snapshot authority cache key

inductive ValidationResult (Payload : Type) where
  | success (payload : Payload)
  | failure

def cacheAfterValidation
    {Key Payload : Type}
    {authority : Key → Option Payload}
    (before afterSuccess : SnapshotCache Key Payload authority)
    (result : ValidationResult Payload) : SnapshotCache Key Payload authority :=
  match result with
  | .success _payload => afterSuccess
  | .failure => before

theorem failed_validation_does_not_change_cache
    {Key Payload : Type}
    {authority : Key → Option Payload}
    (before afterSuccess : SnapshotCache Key Payload authority) :
    cacheAfterValidation before afterSuccess (.failure) = before := by
  rfl

def alwaysMissCache
    {Key Payload : Type}
    (authority : Key → Option Payload) : SnapshotCache Key Payload authority where
  lookup := fun _ => none
  sound := by
    intro key payload hit
    cases hit

-- Semantic correctness alone cannot rule out revalidating every read.
theorem always_miss_still_satisfies_semantic_equivalence
    {Key Payload : Type}
    (authority : Key → Option Payload)
    (keys : List Key) :
    readSequence authority (alwaysMissCache authority) keys = keys.map authority := by
  exact arbitrary_access_sequence_equals_revalidation authority
    (alwaysMissCache authority) keys

end H2HDB.Verification.ReadyAuditCanonicalCache
