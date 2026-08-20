#!/usr/bin/env python3
"""Generate and drift-check Lean BCNF proofs from operational.toml."""

from __future__ import annotations

import argparse
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from verification.lean import generate_schema_proof as catalog_generator
else:
    import generate_schema_proof as catalog_generator

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "verification" / "schema" / "operational.toml"
CATALOG_LEAN = ROOT / "verification" / "lean" / "VNextSchema.lean"
LEAN_FILE = ROOT / "verification" / "lean" / "OperationalSchema.lean"

MAINTENANCE_GATE_MODEL = r"""/-!
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

"""

OPERATIONAL_INTEGRITY_MODEL = r"""/-!
## Operational integrity refinement

These definitions make the lifecycle obligations that cannot be expressed by
keys and foreign keys executable.  They are unbounded over their type and list
parameters; the concrete SQL implementations must refine the atomic predicates.
-/

inductive DownloadHandoffKind where
  | downloader
  | expiredTakeover
deriving DecidableEq

structure DownloadAuthority (Token : Type) where
  generation : Nat
  ownerToken : Token
  leaseExpiresAt : Nat

structure DownloadHandoff (Token : Type) where
  generation : Nat
  ownerToken : Token
  kind : DownloadHandoffKind
  requestedAt : Nat

def DownloadHandoffAuthorized
    (now : Nat) (authority : DownloadAuthority Token)
    (handoff : DownloadHandoff Token) : Prop :=
  handoff.generation = authority.generation ∧
    handoff.ownerToken = authority.ownerToken ∧
    ((handoff.kind = .downloader ∧ now < authority.leaseExpiresAt) ∨
      (handoff.kind = .expiredTakeover ∧ authority.leaseExpiresAt ≤ now))

def MutableDownloadAuthorityRemoved
    (owner : Option Token) (lease : Option Nat) : Prop :=
  owner = none ∧ lease = none

def DownloadHandoffCommit
    (now : Nat) (authority : DownloadAuthority Token)
    (handoff : DownloadHandoff Token)
    (remainingOwner : Option Token) (remainingLease : Option Nat) : Prop :=
  DownloadHandoffAuthorized now authority handoff ∧
    MutableDownloadAuthorityRemoved remainingOwner remainingLease

theorem committed_download_handoff_removes_owner_and_lease
    (commit : DownloadHandoffCommit now authority handoff owner lease) :
    owner = none ∧ lease = none :=
  commit.2

theorem live_downloader_handoff_requires_exact_capability
    (authorized : DownloadHandoffAuthorized now authority handoff) :
    handoff.ownerToken = authority.ownerToken :=
  authorized.2.1

structure DownloadIngestConsumption where
  downloadGeneration : Nat
  ingestGeneration : Nat

def DownloadConsumptionOneToOne
    (consumptions : List DownloadIngestConsumption) : Prop :=
  (∀ first second, first ∈ consumptions → second ∈ consumptions →
    first.downloadGeneration = second.downloadGeneration → first = second) ∧
  (∀ first second, first ∈ consumptions → second ∈ consumptions →
    first.ingestGeneration = second.ingestGeneration → first = second)

theorem one_download_handoff_cannot_feed_two_ingest_generations
    (unique : DownloadConsumptionOneToOne consumptions)
    (firstMember : first ∈ consumptions) (secondMember : second ∈ consumptions)
    (sameDownload : first.downloadGeneration = second.downloadGeneration) :
    first = second :=
  unique.1 first second firstMember secondMember sameDownload

structure CoordinatedIngestCompletion where
  ingestGeneration : Nat
  completedAt : Nat

def LinkedDownloadCompletion
    (consumption : DownloadIngestConsumption)
    (completion : CoordinatedIngestCompletion)
    (downloadCompletedGeneration : Nat) : Prop :=
  completion.ingestGeneration = consumption.ingestGeneration ∧
    downloadCompletedGeneration = consumption.downloadGeneration

theorem linked_ingest_completion_advances_exact_download_generation
    (linked : LinkedDownloadCompletion consumption completion completed) :
    completed = consumption.downloadGeneration :=
  linked.2

def ExactCoordinatedReplay
    (stored presented : CoordinatedIngestCompletion) : Prop :=
  stored = presented

theorem coordinated_response_loss_replay_rejects_mismatch
    (mismatch : stored ≠ presented) :
    ¬ ExactCoordinatedReplay stored presented :=
  mismatch

structure BuildGenerationReservation (Build : Type) where
  generation : Nat
  build : Build

def BuildReservationWriterAuthorized
    (currentGeneration ownerGeneration now leaseExpiresAt : Nat)
    (reservation : BuildGenerationReservation Build) : Prop :=
  reservation.generation = currentGeneration ∧
    reservation.generation = ownerGeneration ∧
    now < leaseExpiresAt

def BuildReservationHistoryBacked
    (history : List Nat) (reservation : BuildGenerationReservation Build) : Prop :=
  reservation.generation ∈ history

theorem build_reservation_requires_current_live_owner
    (authorized : BuildReservationWriterAuthorized
      currentGeneration ownerGeneration now leaseExpiresAt reservation) :
    reservation.generation = currentGeneration ∧
      reservation.generation = ownerGeneration ∧ now < leaseExpiresAt :=
  authorized

theorem completed_owner_removal_preserves_generation_history_authority
    (backed : BuildReservationHistoryBacked history reservation)
    (_remainingOwner : Option Nat) :
    BuildReservationHistoryBacked history reservation :=
  backed

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

structure DeletionRequestGenerationFact where
  generation : Nat
  allocatedAt : Nat

structure DeletionRequestGenerationHead where
  currentGeneration : Nat
  updatedAt : Nat

def DeletionGenerationHeadBacked
    (history : List DeletionRequestGenerationFact)
    (head : DeletionRequestGenerationHead) : Prop :=
  ∃ fact, fact ∈ history ∧ fact.generation = head.currentGeneration

def DeletionGenerationAdvance
    (oldHead : DeletionRequestGenerationHead)
    (presentedGeneration : Nat)
    (newFact : DeletionRequestGenerationFact)
    (newHead : DeletionRequestGenerationHead) : Prop :=
  presentedGeneration = oldHead.currentGeneration ∧
    oldHead.currentGeneration < 9223372036854775807 ∧
    newFact.generation = oldHead.currentGeneration + 1 ∧
    newHead.currentGeneration = newFact.generation ∧
    oldHead.updatedAt ≤ newFact.allocatedAt ∧
    newFact.allocatedAt ≤ newHead.updatedAt

theorem deletion_generation_advance_allocates_exact_successor
    (advance : DeletionGenerationAdvance
      oldHead presentedGeneration newFact newHead) :
    newFact.generation = oldHead.currentGeneration + 1 :=
  advance.2.2.1

theorem deletion_generation_advance_exact_cas_rejects_stale_writer
    (stale : presentedGeneration ≠ oldHead.currentGeneration) :
    ¬ DeletionGenerationAdvance
      oldHead presentedGeneration newFact newHead := by
  intro advance
  exact stale advance.1

theorem deletion_generation_exhaustion_fails_closed
    (exhausted : oldHead.currentGeneration = 9223372036854775807) :
    ¬ DeletionGenerationAdvance
      oldHead presentedGeneration newFact newHead := by
  intro advance
  unfold DeletionGenerationAdvance at advance
  rw [exhausted] at advance
  omega

theorem deletion_generation_advance_backs_new_head_with_inserted_history
    (advance : DeletionGenerationAdvance
      oldHead presentedGeneration newFact newHead) :
    DeletionGenerationHeadBacked (newFact :: history) newHead := by
  refine ⟨newFact, by simp, ?_⟩
  exact advance.2.2.2.1.symm

def DeletionPreparationGenerationCurrent
    (preparedGeneration : Nat)
    (head : DeletionRequestGenerationHead) : Prop :=
  preparedGeneration = head.currentGeneration

theorem changed_deletion_generation_rejects_prepared_publication
    (changed : preparedGeneration ≠ head.currentGeneration) :
    ¬ DeletionPreparationGenerationCurrent preparedGeneration head :=
  changed

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
  preparationId : Nat
  sequenceNo : Nat

def PreparationStreamBeginAtomic
    (streamInserted preparationInserted checkpointsInserted : Prop) : Prop :=
  streamInserted ↔ preparationInserted ∧ checkpointsInserted

theorem failed_preparation_begin_leaves_no_standalone_stream
    (atomic : PreparationStreamBeginAtomic
      streamInserted preparationInserted checkpointsInserted)
    (noPreparation : ¬ preparationInserted) :
    ¬ streamInserted := by
  intro stream
  exact noPreparation (atomic.mp stream).1

structure OperationalEffectSealModel (Digest : Type) where
  eventCount : Nat
  finalChain : Digest

def ContiguousOperationalEffectCoordinates
    (events : List OperationalEventCoordinate) (eventCount : Nat) : Prop :=
  ∀ sequence, sequence < eventCount ↔
    ∃ event, event ∈ events ∧ event.sequenceNo = sequence

def OperationalEffectSealValid
    (events : List OperationalEventCoordinate)
    (computedFinalChain : Digest)
    (effectSeal : OperationalEffectSealModel Digest) : Prop :=
  ContiguousOperationalEffectCoordinates events effectSeal.eventCount ∧
    effectSeal.finalChain = computedFinalChain

theorem zero_event_effect_seal_is_valid
    (emptyChain : Digest) :
    OperationalEffectSealValid [] emptyChain
      { eventCount := 0, finalChain := emptyChain } := by
  constructor
  · intro sequence
    simp
  · rfl

def OperationalActivationAuthorized
    (preparationComplete effectSealPresent deletionGenerationCurrent : Prop) : Prop :=
  preparationComplete ∧ effectSealPresent ∧ deletionGenerationCurrent

theorem operational_activation_requires_only_scalar_authority
    (complete : preparationComplete)
    (sealed : effectSealPresent)
    (current : deletionGenerationCurrent) :
    OperationalActivationAuthorized
      preparationComplete effectSealPresent deletionGenerationCurrent :=
  ⟨complete, sealed, current⟩

def OperationalEventVisible
    (eventPreparation activatedPreparation : Nat) : Prop :=
  eventPreparation = activatedPreparation

theorem event_without_matching_activation_is_invisible
    (unactivated : eventPreparation ≠ activatedPreparation) :
    ¬ OperationalEventVisible eventPreparation activatedPreparation :=
  unactivated

def AcknowledgesEveryExistingEventThrough
    (allEvents acknowledgedEvents : List OperationalEventCoordinate)
    (target : OperationalEventCoordinate) : Prop :=
  ∀ event, event ∈ allEvents →
    event.preparationId = target.preparationId →
    event.sequenceNo ≤ target.sequenceNo →
    event ∈ acknowledgedEvents

def AckHighWaterAdvance
    (oldHead targetEvent : OperationalEventCoordinate)
    (allEvents acknowledgedEvents : List OperationalEventCoordinate) : Prop :=
  targetEvent.preparationId = oldHead.preparationId ∧
    oldHead.sequenceNo ≤ targetEvent.sequenceNo ∧
    targetEvent ∈ allEvents ∧
    AcknowledgesEveryExistingEventThrough
      allEvents acknowledgedEvents targetEvent

theorem ack_high_water_advance_stays_in_preparation
    (advance : AckHighWaterAdvance
      oldHead targetEvent allEvents acknowledgedEvents) :
    targetEvent.preparationId = oldHead.preparationId :=
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
    (samePreparation : event.preparationId = targetEvent.preparationId)
    (atOrBefore : event.sequenceNo ≤ targetEvent.sequenceNo) :
    event ∈ acknowledgedEvents :=
  advance.2.2.2 event eventMember samePreparation atOrBefore

theorem ack_high_water_regression_is_rejected
    (regression : targetEvent.sequenceNo < oldHead.sequenceNo) :
    ¬ AckHighWaterAdvance
      oldHead targetEvent allEvents acknowledgedEvents := by
  intro advance
  exact (Nat.not_lt_of_ge advance.2.1) regression

"""

GALLERY_STAGING_MODEL = r"""/-!
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

"""


def _machine_contract_model(manifest: dict[str, object]) -> str:
    raw_obligations = manifest.get("semantic_obligation")
    raw_seeds = manifest.get("bootstrap_seed")
    raw_cleanup_targets = manifest.get("cleanup_target")
    bootstrap = manifest.get("bootstrap_contract")
    if (
        not isinstance(raw_obligations, list)
        or not all(isinstance(value, dict) for value in raw_obligations)
        or not isinstance(raw_seeds, list)
        or not all(isinstance(value, dict) for value in raw_seeds)
        or not isinstance(raw_cleanup_targets, list)
        or not all(isinstance(value, dict) for value in raw_cleanup_targets)
        or not isinstance(bootstrap, dict)
    ):
        raise ValueError("operational machine contracts are missing")
    obligation_ids = [str(value.get("id")) for value in raw_obligations]
    if len(obligation_ids) != 15 or len(obligation_ids) != len(set(obligation_ids)):
        raise ValueError("operational semantic-obligation IDs are incomplete")
    obligation_lifecycles = {
        str(value.get("id")): str(value.get("lifecycle")) for value in raw_obligations
    }
    building_only_obligations = [
        value
        for value in obligation_ids
        if obligation_lifecycles[value] == "building_only"
    ]
    ready_obligations = [
        value
        for value in obligation_ids
        if obligation_lifecycles[value] != "building_only"
    ]
    if building_only_obligations != ["h2hdb.operational.bootstrap-genesis.v1"] or any(
        value not in {"ready_validation", "building_to_ready", "ready_and_runtime"}
        for value in (obligation_lifecycles[item] for item in ready_obligations)
    ):
        raise ValueError("operational semantic-obligation lifecycle partition drifts")
    obligations_by_id = {str(value.get("id")): value for value in raw_obligations}
    cleanup_targets_by_kind = {
        str(value.get("target_kind")): value for value in raw_cleanup_targets
    }
    candidate_cleanup = cleanup_targets_by_kind.get("PUBLICATION_CANDIDATE")
    if candidate_cleanup is None or candidate_cleanup.get("semantic_blockers") != [
        {
            "relation": "prepared_artifact_state",
            "attributes": ["candidate_id"],
            "root_attributes": ["candidate_id"],
            "blocking_predicate": "state IN ('PENDING','PREPARED')",
            "nonblocking_state": "COMMITTED",
            "semantic_obligation_id": "catalog.retention.v1",
            "release_obligation_id": "catalog.artifact-semantics.v1",
        }
    ]:
        raise ValueError(
            "publication-candidate cleanup release semantic blocker drifts"
        )
    expected_generation_obligation_relations = {
        "h2hdb.operational.bounded-work.v1": (
            "operational_event_stream",
            "operational_preparation",
            "operational_preparation_checkpoint",
            "operational_preparation_batch_receipt",
            "operational_preparation_effect_seal",
            "operational_event",
            "operational_removed_gid_event",
            "operational_deletion_consumption_event",
            "cleanup_checkpoint",
            "cleanup_batch_receipt",
        ),
        "h2hdb.operational.queue-history.v1": (
            "deletion_request_generation",
            "deletion_request_generation_head",
            "deletion_request_attempt",
            "deletion_request_head",
            "deletion_request_url",
            "operational_preparation",
            "operational_deletion_consumption_event",
        ),
        "h2hdb.operational.attempt-identity.v1": (
            "cleanup_job",
            "operational_preparation",
            "operational_policy",
            "deletion_request_generation",
            "deletion_request_generation_head",
        ),
        "h2hdb.operational.bootstrap-genesis.v1": (
            "revision_allocator",
            "identity_allocator",
            "deletion_request_generation",
            "deletion_request_generation_head",
        ),
        "h2hdb.operational.event-integrity.v1": (
            "operational_event_stream",
            "operational_preparation",
            "operational_preparation_effect_seal",
            "publication_candidate_preparation",
            "operational_activation",
            "operational_event",
            "operational_removed_gid_event",
            "operational_deletion_consumption_event",
            "operational_event_ack",
            "operational_event_ack_head",
        ),
        "h2hdb.operational.cleanup-reachability.v1": (
            "cleanup_target_kind",
            "cleanup_phase",
            "cleanup_job",
            "cleanup_checkpoint",
            "source_build_descriptor_seal",
            "publication_candidate_anchor",
            "publication_candidate_definition_seal",
            "publication_candidate_analysis_id",
            "publication_candidate_reserved_revision",
            "publication_candidate_artifact_policy_id",
            "publication_candidate_display_title_policy_id",
            "publication_candidate_artifacts_required",
            "publication_candidate_created_at",
            "analysis_snapshot_manifest",
            "source_revision",
            "catalog_revision",
            "canonical_value_identity",
            "content_blob",
            "operational_event_stream",
            "operational_preparation",
            "operational_preparation_effect_seal",
            "publication_candidate_preparation",
            "operational_activation",
            "operational_event",
            "operational_removed_gid_event",
            "operational_deletion_consumption_event",
            "operational_event_ack",
            "operational_event_ack_head",
        ),
    }
    for (
        obligation_id,
        expected_relations,
    ) in expected_generation_obligation_relations.items():
        if tuple(obligations_by_id[obligation_id].get("relations", [])) != (
            expected_relations
        ):
            raise ValueError(
                f"operational obligation {obligation_id!r} relation binding drifts"
            )
    seed_by_id = {str(value.get("id")): value for value in raw_seeds}
    expected_seed_ids = {
        "h2hdb.operational.revision-allocator.source.v1": "SOURCE",
        "h2hdb.operational.revision-allocator.catalog.v1": "CATALOG",
    }
    deletion_generation_seed_ids = {
        "h2hdb.operational.deletion-request-generation.genesis.v1",
        "h2hdb.operational.deletion-request-generation-head.genesis.v1",
    }
    if not (set(expected_seed_ids) | deletion_generation_seed_ids) <= set(seed_by_id):
        raise ValueError("operational bootstrap authority seeds are incomplete")

    def seed_values(seed_id: str) -> tuple[str, int, int]:
        raw_seed = seed_by_id[seed_id]
        if raw_seed.get("lifecycle") != "building_only":
            raise ValueError(f"bootstrap seed {seed_id!r} is not BUILDING-only")
        raw_cells = raw_seed.get("value")
        if not isinstance(raw_cells, list) or not all(
            isinstance(value, dict) for value in raw_cells
        ):
            raise ValueError(f"bootstrap seed {seed_id!r} lacks typed cells")
        values: dict[str, str | int] = {}
        for cell in raw_cells:
            attribute = cell.get("attribute")
            if not isinstance(attribute, str):
                raise ValueError(f"bootstrap seed {seed_id!r} has an invalid cell")
            value = cell.get("text", cell.get("integer"))
            if not isinstance(value, (str, int)) or isinstance(value, bool):
                raise ValueError(f"bootstrap seed {seed_id!r} has an invalid value")
            values[attribute] = value
        result = (
            str(values.get("stream")),
            int(values.get("next_revision", -1)),
            int(values.get("updated_at", -1)),
        )
        if result != (expected_seed_ids[seed_id], 1, 0):
            raise ValueError(f"bootstrap seed {seed_id!r} has unsafe genesis values")
        return result

    source = seed_values("h2hdb.operational.revision-allocator.source.v1")
    catalog = seed_values("h2hdb.operational.revision-allocator.catalog.v1")

    def integer_seed_values(
        seed_id: str,
        expected_relation: str,
        expected_attributes: tuple[str, ...],
        expected_types: tuple[str, ...],
    ) -> tuple[int, ...]:
        raw_seed = seed_by_id[seed_id]
        if (
            raw_seed.get("lifecycle") != "building_only"
            or raw_seed.get("relation") != expected_relation
        ):
            raise ValueError(f"bootstrap seed {seed_id!r} authority binding drifts")
        raw_cells = raw_seed.get("value")
        if not isinstance(raw_cells, list) or not all(
            isinstance(value, dict) for value in raw_cells
        ):
            raise ValueError(f"bootstrap seed {seed_id!r} lacks typed cells")
        if tuple(value.get("attribute") for value in raw_cells) != expected_attributes:
            raise ValueError(f"bootstrap seed {seed_id!r} attribute order drifts")
        if tuple(value.get("type") for value in raw_cells) != expected_types or any(
            not isinstance(value.get("integer"), int)
            or isinstance(value.get("integer"), bool)
            for value in raw_cells
        ):
            raise ValueError(f"bootstrap seed {seed_id!r} integer type drifts")
        return tuple(int(value["integer"]) for value in raw_cells)

    deletion_generation = integer_seed_values(
        "h2hdb.operational.deletion-request-generation.genesis.v1",
        "deletion_request_generation",
        ("generation", "allocated_at"),
        ("uint64", "unix_microseconds"),
    )
    deletion_generation_head = integer_seed_values(
        "h2hdb.operational.deletion-request-generation-head.genesis.v1",
        "deletion_request_generation_head",
        ("singleton_id", "current_generation", "updated_at"),
        ("uint64", "uint64", "unix_microseconds"),
    )
    if deletion_generation != (0, 0) or deletion_generation_head != (1, 0, 0):
        raise ValueError("deletion generation genesis is not the exact real zero fact")
    raw_absent = bootstrap.get("absent_relations")
    if not isinstance(raw_absent, list) or not all(
        isinstance(value, str) and value for value in raw_absent
    ):
        raise ValueError("bootstrap absent relation set is missing")
    absent = list(raw_absent)
    critical_absent = [
        "ingest_generation",
        "ingest_coordination_head",
        "ingest_generation_owner",
        "ingest_generation_lease",
        "maintenance_gate_generation",
        "maintenance_gate_head",
        "maintenance_gate_owner",
        "maintenance_gate_holder",
        "operational_event_stream",
        "operational_preparation_effect_seal",
        "operational_event",
        "download_request",
        "deletion_request_attempt",
        "deletion_request_url",
        "deletion_request_head",
        "operational_preparation",
        "cleanup_job",
    ]
    if not set(critical_absent) <= set(absent):
        raise ValueError("bootstrap invents an active control-plane fact")
    if any(
        relation in absent
        for relation in (
            "revision_allocator",
            "deletion_request_generation",
            "deletion_request_generation_head",
            "schema_epoch_control",
        )
    ):
        raise ValueError("bootstrap ownership partition is inconsistent")
    if (
        bootstrap.get("seed_validation_lifecycle") != "building_only"
        or bootstrap.get("absence_validation_lifecycle") != "building_only"
    ):
        raise ValueError("bootstrap validation lifecycle must be BUILDING-only")

    return f"""/-!
## Machine obligations and bootstrap genesis

These values are generated from the typed machine records in operational.toml.
Allocator rows, the real deletion generation-zero empty-queue history/head,
and cleanup kind/phase registries are provider-owned genesis.  Deletion attempts
and per-gid heads, coordination, maintenance, owner, lease, preparation stream,
effect seal, activation, event, cache, and work facts are absent until their
first transaction creates real state.  Schema
epoch control is initialized independently by SchemaEpochCatalog.
-/

structure RevisionAllocatorGenesis where
  stream : String
  nextRevision : Nat
  updatedAt : Nat
deriving DecidableEq, Repr

structure DeletionGenerationGenesis where
  generation : Nat
  allocatedAt : Nat
deriving DecidableEq, Repr

structure DeletionGenerationHeadGenesis where
  singletonId : Nat
  currentGeneration : Nat
  updatedAt : Nat
deriving DecidableEq, Repr

def CleanupEligible (reachableFromRetentionRoot : Target → Prop)
    (target : Target) : Prop := ¬ reachableFromRetentionRoot target

theorem reachable_retention_root_is_never_cleanup_eligible
    (reachable : reachableFromRetentionRoot target) :
    ¬ CleanupEligible reachableFromRetentionRoot target := by
  intro eligible
  exact eligible reachable

inductive PreparedArtifactCleanupState where
  | pending
  | prepared
  | committed
deriving DecidableEq

def PreparedArtifactBlocksCandidateCleanup :
    PreparedArtifactCleanupState → Bool
  | .pending => true
  | .prepared => true
  | .committed => false

theorem pending_prepared_artifact_blocks_candidate_cleanup :
    PreparedArtifactBlocksCandidateCleanup .pending = true := by
  native_decide

theorem prepared_prepared_artifact_blocks_candidate_cleanup :
    PreparedArtifactBlocksCandidateCleanup .prepared = true := by
  native_decide

theorem committed_prepared_artifact_does_not_block_candidate_cleanup :
    PreparedArtifactBlocksCandidateCleanup .committed = false := by
  native_decide

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
  {{ stream := {catalog_generator.lean_string(source[0])},
    nextRevision := {source[1]}, updatedAt := {source[2]} }}

def catalogRevisionAllocatorGenesis : RevisionAllocatorGenesis :=
  {{ stream := {catalog_generator.lean_string(catalog[0])},
    nextRevision := {catalog[1]}, updatedAt := {catalog[2]} }}

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

def deletionGenerationGenesis : DeletionGenerationGenesis :=
  {{ generation := {deletion_generation[0]},
    allocatedAt := {deletion_generation[1]} }}

def deletionGenerationHeadGenesis : DeletionGenerationHeadGenesis :=
  {{ singletonId := {deletion_generation_head[0]},
    currentGeneration := {deletion_generation_head[1]},
    updatedAt := {deletion_generation_head[2]} }}

theorem deletion_generation_zero_is_a_real_history_fact :
    deletionGenerationGenesis.generation = 0 ∧
      deletionGenerationGenesis.allocatedAt = 0 := by
  native_decide

theorem deletion_generation_genesis_head_is_the_exact_singleton_reference :
    deletionGenerationHeadGenesis.singletonId = 1 ∧
      deletionGenerationHeadGenesis.currentGeneration =
        deletionGenerationGenesis.generation ∧
      deletionGenerationHeadGenesis.updatedAt = 0 := by
  native_decide

theorem ready_validation_accepts_a_legitimately_advanced_allocator :
    RevisionAllocatorCurrentValid
      {{ stream := "SOURCE", nextRevision := 2,
        updatedAt := 1 }} := by
  change (1 : Nat) ≤ 2
  decide

def operationalSemanticObligationIds : List String :=
  {catalog_generator.lean_list(obligation_ids)}

theorem operational_semantic_obligation_ids_are_unique :
    operationalSemanticObligationIds.Nodup := by
  native_decide

theorem operational_semantic_obligation_count :
    operationalSemanticObligationIds.length = {len(obligation_ids)} := by
  native_decide

def operationalBuildingOnlyObligationIds : List String :=
  {catalog_generator.lean_list(building_only_obligations)}

def operationalReadyObligationIds : List String :=
  {catalog_generator.lean_list(ready_obligations)}

theorem bootstrap_genesis_is_the_only_building_only_obligation :
    operationalBuildingOnlyObligationIds =
      ["h2hdb.operational.bootstrap-genesis.v1"] := by
  native_decide

theorem building_only_and_ready_obligations_are_disjoint :
    ∀ obligation ∈ operationalBuildingOnlyObligationIds,
      obligation ∉ operationalReadyObligationIds := by
  native_decide

def operationalBootstrapAbsentRelations : List String :=
  {catalog_generator.lean_list(absent)}

theorem operational_bootstrap_has_no_invented_active_control_facts :
    ∀ relation ∈ {catalog_generator.lean_list(critical_absent)},
      relation ∈ operationalBootstrapAbsentRelations := by
  native_decide

theorem revision_allocator_is_seeded_not_absent :
    "revision_allocator" ∉ operationalBootstrapAbsentRelations := by
  native_decide

theorem deletion_generation_authority_is_seeded_not_absent :
    "deletion_request_generation" ∉ operationalBootstrapAbsentRelations ∧
      "deletion_request_generation_head" ∉
        operationalBootstrapAbsentRelations := by
  native_decide

theorem schema_epoch_control_is_epoch_owned_not_absent :
    "schema_epoch_control" ∉ operationalBootstrapAbsentRelations := by
  native_decide

"""


def render() -> str:
    with MANIFEST.open("rb") as stream:
        manifest = tomllib.load(stream)
    catalog_source = CATALOG_LEAN.read_text(encoding="utf-8")
    framework, _generated = catalog_source.split(
        catalog_generator.BEGIN,
        1,
    )
    framework = framework.replace(
        "H2HDB.Verification.VNextSchema",
        "H2HDB.Verification.OperationalSchema",
    )
    framework = framework.replace(
        "# vNext catalog schema and BCNF",
        "# vNext operational schema and BCNF",
    ).replace(
        "`verification/schema/catalog.toml`",
        "`verification/schema/operational.toml`",
    )
    generated = catalog_generator.render(MANIFEST.read_bytes())
    generated = (
        generated.replace(
            "GENERATED CATALOG CONTRACTS",
            "GENERATED OPERATIONAL CONTRACTS",
        )
        .replace(
            "catalogManifestSha256",
            "operationalManifestSha256",
        )
        .replace(
            "mechanically generated from catalog.toml",
            "mechanically generated from operational.toml",
        )
    )
    return (
        framework
        + MAINTENANCE_GATE_MODEL
        + OPERATIONAL_INTEGRITY_MODEL
        + GALLERY_STAGING_MODEL
        + _machine_contract_model(manifest)
        + generated
        + "\n\nend H2HDB.Verification.OperationalSchema\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args()
    expected = render()
    actual = LEAN_FILE.read_text(encoding="utf-8") if LEAN_FILE.exists() else ""
    if arguments.check:
        if actual != expected:
            raise SystemExit(
                "OperationalSchema.lean is stale; run "
                "verification/lean/generate_operational_schema_proof.py"
            )
    else:
        LEAN_FILE.write_text(expected, encoding="utf-8")


if __name__ == "__main__":
    main()
