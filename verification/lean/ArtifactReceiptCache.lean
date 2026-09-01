import Std

/-!
# Disposable artifact-receipt cache refinement

This module compares the reference publication step, which renders again after
the durable PENDING intent commit, with an optimized step that may consume one
process-local receipt.  A cache hit requires the exact authority and ordered
resource-family key plus the exact result of a fresh input audit.  Missing,
mismatched, crashed, response-lost, or explicitly closed caches fall back to
the reference render.  Before an exact hit may be consumed, the implementation
also reopens and hashes the same selected sealed source members as the reference
renderer; an unavailable or drifted source rejects both paths.

`PublicObservation` deliberately contains only the durable/public confirmation
result.  Render count and process-local cache ownership are outside that
authorized observable quotient; `secondPhaseRenderCount` states the intended
work difference separately.  The cache never replaces the fresh audit or the
durable PENDING check.

The equivalence theorems are unbounded over authority components, audit values,
and ordered family lists.  They assume the adapter ID plus policy fingerprint
fully identifies deterministic storage-key, render, and presentation semantics,
and that an exact cached receipt remains open and unchanged.  They do not prove
that Python ownership, SQL transactions, filesystem spools, storage adapters,
SQLite, or MariaDB refine this model; differential, source-fault, lifecycle,
and response-loss tests provide separate executable evidence.
-/

namespace H2HDB.Verification.ArtifactReceiptCache

structure Authority where
  candidate : Nat
  publication : Nat
  generation : Nat
  adapter : Nat
deriving DecidableEq, Repr

structure CacheKey where
  authority : Authority
  /-- The exact, ordered durable resource-family identity. -/
  families : List Nat
deriving DecidableEq, Repr

structure Receipt where
  key : CacheKey
  freshAudit : Nat
  byteReceipt : Nat
deriving DecidableEq, Repr

/-- Pure stand-in for the verified renderer on one freshly audited input. -/
def render (key : CacheKey) (freshAudit : Nat) : Receipt :=
  ⟨key, freshAudit,
    key.authority.candidate + key.authority.publication +
      key.authority.generation + key.authority.adapter +
      key.families.length + freshAudit⟩

inductive DurableState where
  | absent
  | pending
  | prepared
deriving DecidableEq, Repr

structure DurableSnapshot where
  key : CacheKey
  state : DurableState
deriving DecidableEq, Repr

inductive PublicObservation where
  | rejected
  | confirmed (byteReceipt : Nat)
deriving DecidableEq, Repr

/-- Confirmation still checks fresh durable PENDING authority and exact bytes. -/
def confirm
    (key : CacheKey)
    (freshAudit : Nat)
    (durable : DurableSnapshot)
    (receipt : Receipt) : PublicObservation :=
  if durable.key = key ∧ durable.state = .pending ∧
      receipt = render key freshAudit then
    .confirmed receipt.byteReceipt
  else
    .rejected

/-- Reference behavior always renders after observing durable PENDING state. -/
def reference
    (key : CacheKey)
    (freshAudit : Nat)
    (durable : DurableSnapshot) : PublicObservation :=
  confirm key freshAudit durable (render key freshAudit)

structure CacheEntry where
  key : CacheKey
  receipt : Receipt
deriving DecidableEq, Repr

/-- A hit includes the fresh audit; authority/family equality alone is not enough. -/
def CacheValid
    (entry : CacheEntry)
    (key : CacheKey)
    (freshAudit : Nat) : Prop :=
  entry.key = key ∧ entry.receipt = render key freshAudit

instance cacheValidDecidable
    (entry : CacheEntry)
    (key : CacheKey)
    (freshAudit : Nat) : Decidable (CacheValid entry key freshAudit) := by
  unfold CacheValid
  infer_instance

/-- Invalid caches are disposed and take the same rerender branch as a miss. -/
def optimized
    (cache : Option CacheEntry)
    (key : CacheKey)
    (freshAudit : Nat)
    (durable : DurableSnapshot) : PublicObservation :=
  match cache with
  | none => reference key freshAudit durable
  | some entry =>
      if CacheValid entry key freshAudit then
        confirm key freshAudit durable entry.receipt
      else
        reference key freshAudit durable

theorem optional_cache_observationally_equals_reference
    (cache : Option CacheEntry)
    (key : CacheKey)
    (freshAudit : Nat)
    (durable : DurableSnapshot) :
    optimized cache key freshAudit durable =
      reference key freshAudit durable := by
  cases cache with
  | none => rfl
  | some entry =>
      by_cases valid : CacheValid entry key freshAudit
      · simp only [optimized, valid, if_pos]
        rw [valid.2]
        rfl
      · simp [optimized, valid]

def secondPhaseRenderCount
    (cache : Option CacheEntry)
    (key : CacheKey)
    (freshAudit : Nat) : Nat :=
  match cache with
  | none => 1
  | some entry => if CacheValid entry key freshAudit then 0 else 1

theorem exact_cache_hit_renders_once_over_both_phases
    (entry : CacheEntry)
    (key : CacheKey)
    (freshAudit : Nat)
    (valid : CacheValid entry key freshAudit) :
    1 + secondPhaseRenderCount (some entry) key freshAudit = 1 := by
  simp [secondPhaseRenderCount, valid]

theorem missing_cache_rerenders_but_preserves_observation
    (key : CacheKey)
    (freshAudit : Nat)
    (durable : DurableSnapshot) :
    1 + secondPhaseRenderCount none key freshAudit = 2 ∧
      optimized none key freshAudit durable =
        reference key freshAudit durable := by
  simp [secondPhaseRenderCount, optimized]

theorem mismatched_cache_rerenders_but_preserves_observation
    (entry : CacheEntry)
    (key : CacheKey)
    (freshAudit : Nat)
    (durable : DurableSnapshot)
    (invalid : ¬ CacheValid entry key freshAudit) :
    1 + secondPhaseRenderCount (some entry) key freshAudit = 2 ∧
      optimized (some entry) key freshAudit durable =
        reference key freshAudit durable := by
  simp [secondPhaseRenderCount, optimized, invalid]

inductive SourceProbe where
  | verified
  | rejected
deriving DecidableEq, Repr

/-- The reference only reaches its encoder after the sealed source-byte probe. -/
def referenceAfterSourceProbe
    (probe : SourceProbe)
    (key : CacheKey)
    (freshAudit : Nat)
    (durable : DurableSnapshot) : PublicObservation :=
  match probe with
  | .verified => reference key freshAudit durable
  | .rejected => .rejected

/-- A cache hit is likewise unavailable until the identical probe succeeds. -/
def optimizedAfterSourceProbe
    (probe : SourceProbe)
    (cache : Option CacheEntry)
    (key : CacheKey)
    (freshAudit : Nat)
    (durable : DurableSnapshot) : PublicObservation :=
  match probe with
  | .verified => optimized cache key freshAudit durable
  | .rejected => .rejected

theorem source_probe_preserves_optional_cache_equivalence
    (probe : SourceProbe)
    (cache : Option CacheEntry)
    (key : CacheKey)
    (freshAudit : Nat)
    (durable : DurableSnapshot) :
    optimizedAfterSourceProbe probe cache key freshAudit durable =
      referenceAfterSourceProbe probe key freshAudit durable := by
  cases probe <;>
    simp [
      optimizedAfterSourceProbe,
      referenceAfterSourceProbe,
      optional_cache_observationally_equals_reference,
    ]

theorem rejected_source_probe_never_consumes_cache
    (cache : Option CacheEntry)
    (key : CacheKey)
    (freshAudit : Nat)
    (durable : DurableSnapshot) :
    optimizedAfterSourceProbe .rejected cache key freshAudit durable =
      .rejected := by
  rfl

inductive PersistReply where
  | pendingAcknowledged
  | alreadyPrepared
  | responseLost
  | rejected
deriving DecidableEq, Repr

/-- Ownership transfers only after an acknowledged exact PENDING commit. -/
def retainAfterPersist
    (reply : PersistReply)
    (key : CacheKey)
    (receipt : Receipt) : Option CacheEntry :=
  match reply with
  | .pendingAcknowledged => some ⟨key, receipt⟩
  | .alreadyPrepared => none
  | .responseLost => none
  | .rejected => none

theorem response_loss_retains_no_cache
    (key : CacheKey)
    (receipt : Receipt) :
    retainAfterPersist .responseLost key receipt = none := by
  rfl

theorem non_pending_reply_retains_no_cache
    (reply : PersistReply)
    (key : CacheKey)
    (receipt : Receipt)
    (notPending : reply ≠ .pendingAcknowledged) :
    retainAfterPersist reply key receipt = none := by
  cases reply <;> simp_all [retainAfterPersist]

/-- Resource capacity is an optimization boundary, never durable authority. -/
def retainBoundedAfterPersist
    (reply : PersistReply)
    (key : CacheKey)
    (receipt : Receipt)
    (resourceBytes capacityBytes : Nat) : Option CacheEntry :=
  if resourceBytes ≤ capacityBytes then
    retainAfterPersist reply key receipt
  else
    none

theorem oversized_receipt_retains_no_cache
    (reply : PersistReply)
    (key : CacheKey)
    (receipt : Receipt)
    (resourceBytes capacityBytes : Nat)
    (oversized : capacityBytes < resourceBytes) :
    retainBoundedAfterPersist reply key receipt resourceBytes capacityBytes =
      none := by
  simp [retainBoundedAfterPersist, Nat.not_le_of_lt oversized]

/-- A fresh issued authority/family mismatch disposes the optional cache. -/
def retireMismatched
    (cache : Option CacheEntry)
    (freshKey : CacheKey) : Option CacheEntry :=
  match cache with
  | none => none
  | some entry => if entry.key = freshKey then some entry else none

theorem authority_or_family_drift_evicts
    (entry : CacheEntry)
    (freshKey : CacheKey)
    (changed : entry.key ≠ freshKey) :
    retireMismatched (some entry) freshKey = none := by
  simp [retireMismatched, changed]

/-- Crash, restart, cache loss, and explicit facade disposal have no authority. -/
def discardCache (_cache : Option CacheEntry) : Option CacheEntry := none

theorem discard_rerenders_with_reference_observation
    (cache : Option CacheEntry)
    (key : CacheKey)
    (freshAudit : Nat)
    (durable : DurableSnapshot) :
    secondPhaseRenderCount (discardCache cache) key freshAudit = 1 ∧
      optimized (discardCache cache) key freshAudit durable =
        reference key freshAudit durable := by
  simp [discardCache, secondPhaseRenderCount, optimized]

end H2HDB.Verification.ArtifactReceiptCache
