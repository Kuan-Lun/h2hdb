---------------------------- MODULE GalleryStaging ----------------------------
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************
This is a provider-neutral, bounded model of the vNext gallery staging
protocol.  It models database-visible protocol facts, not byte codecs, hash
collision resistance, SQL isolation, or a particular connector.  A successful
TLC run exhausts only the finite constants selected by the companion cfg.

The outer lease and the staging claim are distinct fences.  The staging claim
stores exactly an ingest generation and a monotonically increasing claim
generation; its owner and lease are obtained by joining the live outer tuple.
Every staging mutation (including an exact replay) requires both fences.  A
staging identity and its header are allocated once.  Observation
identifiers come from a monotone allocator and remain in the allocation
history even when a staging reuses an already sealed observation.

A leaf or branch page commit stores a request digest plus the complete exact
request bytes.  The model deliberately collides all request digests.  A
response may be lost after COMMIT; retrying the same bytes is an observational
REPLAY, while different bytes for that slot are rejected.  The level-one
branch is the bounded abstraction of flushing a durable carry frontier.
***************************************************************************)

CONSTANTS
    Owners,
    Stagings,
    Headers,
    RequestBytes,
    CommitRequestBytes,
    RequestDigests,
    Values,
    SourceRootValue,
    Components,
    FileComponent,
    TagComponent,
    DirectoryComponent,
    MetadataComponent,
    NoOwner,
    NoHeader,
    NoValue,
    NoObservation,
    NoReplayFingerprint,
    SeedObservation,
    MaxTime,
    LeaseSpan,
    MaxOuterGeneration,
    MaxClaimGeneration,
    MaxObservation,
    MaxPagePosition,
    MetadataUnits

ASSUME /\ Owners # {}
       /\ Stagings # {}
       /\ Headers # {}
       /\ Cardinality(RequestBytes) >= 2
       /\ CommitRequestBytes # {}
       /\ CommitRequestBytes \subseteq RequestBytes
       /\ CommitRequestBytes # RequestBytes
       /\ RequestDigests # {}
       /\ \E left, right \in RequestBytes :
            /\ left # right
       /\ Values # {}
       /\ SourceRootValue \in Values
       /\ Values \ {SourceRootValue} # {}
       /\ Components = {
            FileComponent, TagComponent, DirectoryComponent, MetadataComponent
          }
       /\ Cardinality(Components) = 4
       /\ NoOwner \notin Owners
       /\ NoHeader \notin Headers
       /\ NoValue \notin Values
       /\ NoObservation \notin 0..MaxObservation
       /\ SeedObservation = 0
       /\ MaxTime \in Nat
       /\ LeaseSpan \in Nat \ {0}
       /\ MaxOuterGeneration \in Nat \ {0}
       /\ MaxClaimGeneration \in Nat \ {0}
       /\ MaxObservation \in Nat \ {0}
       /\ MaxPagePosition \in Nat
       /\ MetadataUnits \in Nat \ {0}

OuterGenerations == 0..MaxOuterGeneration
ClaimGenerations == 0..MaxClaimGeneration
ObservationIds == 1..MaxObservation
AllObservations == ObservationIds \cup {SeedObservation}
Positions == 0..MaxPagePosition
Levels == {"Leaf", "Branch"}
PageRecords ==
    Stagings \X Components \X Levels \X Positions
    \X RequestDigests \X RequestBytes
UploadClaims ==
    OuterGenerations \X Values
DomainValues == Values \ {SourceRootValue}

\* Each RequestBytes atom stands for the complete framed request (fences,
\* component/level/cursor, prior request, page digest, and terminal intent),
\* not merely the page digest.  Hashes are deliberately maximally colliding;
\* safety must therefore follow from comparison of the complete request bytes.
RequestDigest(requestBytes) ==
    CHOOSE digest \in RequestDigests : TRUE
StagingPhases == {"UNALLOCATED", "OPEN", "SEALED", "REUSED", "DELETED"}
EventKinds == {
    "INIT", "CLOCK", "OUTER_CLAIM",
    "OUTER_TAKEOVER", "BEGIN", "TAKEOVER", "ALLOCATE", "LEAF_COMMIT",
    "LEAF_COMMIT_RESPONSE_LOST", "REUSE_CHILD_ROOT", "BRANCH_COMMIT",
    "REPLAY", "CHANGED_REQUEST_REJECT",
    "STALE_MUTATION_REJECT", "STALE_REPLAY_REJECT", "PARSER_ADVANCE",
    "MATCH_NEW", "MATCH_REUSE", "SEAL_NEW", "FINALIZE_REUSE",
    "REUSE_PAGE_CLEANUP", "SOURCE_BUILD_RESUME",
    "SOURCE_BUILD_MAPPING_CLEANUP",
    "SOURCE_BUILD_MAPPING_CLEANUP_REJECT", "GENERATION_COMPLETE",
    "UPLOAD_ALLOCATE_CLAIM", "UPLOAD_IDENTITY_SEAL",
    "UPLOAD_CONSUME_RELEASE",
    "MAINTENANCE_ENTER", "MAINTENANCE_EXIT",
    "STALE_UPLOAD_CLEANUP", "EXTERNAL_BLOCKER_ADD",
    "EXTERNAL_BLOCKER_REMOVE", "CONSUMER_CLEANUP",
    "ORPHAN_IDENTITY_CLEANUP",
    "STAGING_CLEANUP", "VALUE_GC"
}

VARIABLES
    clock,
    outerOwner,
    outerGeneration,
    outerExpiry,

    stagingAllocated,
    stagingDeleted,
    stagingPhase,
    stagingHeader,
    allocatedHeader,
    claimIngestGeneration,
    claimGeneration,

    nextObservation,
    everAllocatedObservations,
    observationOwner,
    stagingObservation,
    observationSealed,

    pageCommits,
    nextLeafPosition,
    carryPending,
    componentRootComplete,
    metadataParserOffset,
    metadataParserComplete,

    matchComplete,
    matchObservation,
    finalObservation,
    reuseCleanupRemaining,

    canonicalPresent,
    canonicalIdentityPresent,
    canonicalConsumers,
    externalRetentionBlockers,
    canonicalUploads,
    everCanonicalUploads,
    sourceMappedGenerations,
    completedGenerations,
    maintenanceExclusive,

    lastEvent,
    lastAttemptAccepted,
    lastAttemptWasStale,
    lastAttemptWasReplay,
    replayFingerprint,
    lastCleanupAccepted,
    lastCleanupWasOpen,
    lastCleanupWasMaintenance,
    lastUploadCleanupAccepted,
    lastUploadCleanupWasMaintenance,
    lastUploadCleanupWasCurrent,
    lastUploadCleanupWasEligible,
    lastUploadCleanupSourceMappingRetained,
    lastUploadCleanupMadeGcEligible,
    lastUploadCleanupGeneration,
    lastUploadCleanupValue,
    lastIdentityCleanupAccepted,
    lastIdentityCleanupWasMaintenance,
    lastIdentityCleanupHadUpload,
    lastIdentityCleanupHadConsumer,
    lastIdentityCleanupMadeGcEligible,
    lastIdentityCleanupValue,
    lastConsumerCleanupAccepted,
    lastConsumerCleanupWasMaintenance,
    lastConsumerCleanupHadExternalBlocker,
    lastConsumerCleanupWasCurrent,
    lastConsumerCleanupWasEligible,
    lastConsumerCleanupValue,
    lastMappingCleanupAccepted,
    lastMappingCleanupWasMaintenance,
    lastMappingCleanupWasCurrent,
    lastMappingCleanupHadExternalBlocker,
    lastMappingCleanupGeneration,
    lastValueGcAccepted,
    lastValueGcHadUpload,
    lastValueGcWasMaintenance,
    lastReuseCleanupRemoved

ProcessVariables == <<clock>>
OuterFenceVariables == <<outerOwner, outerGeneration, outerExpiry>>
StagingAllocationVariables ==
    <<stagingAllocated, stagingDeleted, stagingPhase, stagingHeader,
      allocatedHeader, claimIngestGeneration, claimGeneration>>
ObservationVariables ==
    <<nextObservation, everAllocatedObservations, observationOwner,
      stagingObservation, observationSealed>>
PageVariables ==
    <<pageCommits, nextLeafPosition, carryPending, componentRootComplete,
      metadataParserOffset, metadataParserComplete>>
FinalizationVariables ==
    <<matchComplete, matchObservation, finalObservation,
      reuseCleanupRemaining>>
CanonicalVariables ==
    <<canonicalPresent, canonicalIdentityPresent,
      canonicalConsumers,
      externalRetentionBlockers,
      canonicalUploads, everCanonicalUploads,
      sourceMappedGenerations, completedGenerations, maintenanceExclusive>>
ProtocolAuditVariables ==
    <<lastEvent, lastAttemptAccepted, lastAttemptWasStale,
      lastAttemptWasReplay, replayFingerprint>>
StagingCleanupAuditVariables ==
    <<lastCleanupAccepted, lastCleanupWasOpen,
      lastCleanupWasMaintenance>>
UploadCleanupAuditVariables ==
    <<lastUploadCleanupAccepted, lastUploadCleanupWasMaintenance,
      lastUploadCleanupWasCurrent, lastUploadCleanupWasEligible,
      lastUploadCleanupSourceMappingRetained,
      lastUploadCleanupMadeGcEligible, lastUploadCleanupGeneration,
      lastUploadCleanupValue>>
IdentityCleanupAuditVariables ==
    <<lastIdentityCleanupAccepted, lastIdentityCleanupWasMaintenance,
      lastIdentityCleanupHadUpload, lastIdentityCleanupHadConsumer,
      lastIdentityCleanupMadeGcEligible, lastIdentityCleanupValue>>
ConsumerCleanupAuditVariables ==
    <<lastConsumerCleanupAccepted, lastConsumerCleanupWasMaintenance,
      lastConsumerCleanupHadExternalBlocker,
      lastConsumerCleanupWasCurrent,
      lastConsumerCleanupWasEligible,
      lastConsumerCleanupValue>>
MappingCleanupAuditVariables ==
    <<lastMappingCleanupAccepted, lastMappingCleanupWasMaintenance,
      lastMappingCleanupWasCurrent,
      lastMappingCleanupHadExternalBlocker,
      lastMappingCleanupGeneration>>
CleanupAuditVariables ==
    <<StagingCleanupAuditVariables, UploadCleanupAuditVariables,
      IdentityCleanupAuditVariables, ConsumerCleanupAuditVariables,
      MappingCleanupAuditVariables>>
ValueGcAuditVariables ==
    <<lastValueGcAccepted, lastValueGcHadUpload,
      lastValueGcWasMaintenance>>
AuditVariables ==
    <<ProtocolAuditVariables, CleanupAuditVariables, ValueGcAuditVariables,
      lastReuseCleanupRemoved>>

DurableProtocolVariables ==
    <<OuterFenceVariables, StagingAllocationVariables, ObservationVariables,
      PageVariables, FinalizationVariables, CanonicalVariables>>
vars == <<ProcessVariables, DurableProtocolVariables, AuditVariables>>

DurableFingerprint ==
    <<outerOwner, outerGeneration, outerExpiry,
      stagingAllocated, stagingDeleted, stagingPhase, stagingHeader,
      allocatedHeader, claimIngestGeneration, claimGeneration,
      nextObservation, everAllocatedObservations, observationOwner,
      stagingObservation, observationSealed, pageCommits, nextLeafPosition,
      carryPending, componentRootComplete, metadataParserOffset,
      metadataParserComplete, matchComplete, matchObservation,
      finalObservation, reuseCleanupRemaining, canonicalPresent,
      canonicalIdentityPresent, canonicalConsumers,
      externalRetentionBlockers,
      canonicalUploads, everCanonicalUploads,
      sourceMappedGenerations,
      completedGenerations, maintenanceExclusive>>

OuterLeaseTupleIsLive ==
    /\ outerOwner # NoOwner
    /\ clock < outerExpiry

OuterLeaseIsLive ==
    /\ ~maintenanceExclusive
    /\ OuterLeaseTupleIsLive
    /\ outerGeneration \notin completedGenerations
OuterAuthorized(owner, generation) ==
    /\ OuterLeaseIsLive
    /\ owner = outerOwner
    /\ generation = outerGeneration

StagingClaimHasLiveOuter(staging) ==
    /\ staging \in stagingAllocated \ stagingDeleted
    /\ claimIngestGeneration[staging] = outerGeneration
    /\ OuterLeaseTupleIsLive

FullyAuthorized(staging, owner, outerGen, claimGen) ==
    /\ OuterAuthorized(owner, outerGen)
    /\ staging \in stagingAllocated \ stagingDeleted
    /\ claimIngestGeneration[staging] = outerGen
    /\ claimGen = claimGeneration[staging]

(***************************************************************************
All presented fence tuples fall into two equivalence classes for the checked
safety predicates: the exact current tuple and every unequal/stale tuple.  The
accepted actions below use the exact class; RejectStalePageAttempt represents
the other class.  This avoids enumerating many guard-equivalent token values.
***************************************************************************)

CurrentStagingAuthorized(staging) ==
    FullyAuthorized(
        staging, outerOwner, outerGeneration, claimGeneration[staging])

StaleTupleExists(staging) ==
    \E owner \in Owners,
       outerGen \in OuterGenerations,
       claimGen \in ClaimGenerations :
        ~FullyAuthorized(staging, owner, outerGen, claimGen)

StagingPageRecords(staging) ==
    {entry \in pageCommits : entry[1] = staging}

SlotRequestBytes(staging, component, level, position) ==
    {requestBytes \in RequestBytes :
        <<staging, component, level, position,
          RequestDigest(requestBytes), requestBytes>> \in pageCommits}

PageSlotCommitted(staging, component, level, position) ==
    SlotRequestBytes(staging, component, level, position) # {}

AllComponentRootsComplete(staging) ==
    \A component \in Components : componentRootComplete[staging][component]

AllCarriesFlushed(staging) ==
    \A component \in Components : ~carryPending[staging][component]

(***************************************************************************
The production protocol owns four disjoint component checkpoints.  This model
uses their canonical generation order to avoid exploring schedule permutations
that cannot change any checked predicate.  It therefore does not claim to
check arbitrary cross-component scheduling.
***************************************************************************)

PriorComponentRootsComplete(staging, component) ==
    IF component = FileComponent
    THEN TRUE
    ELSE IF component = TagComponent
         THEN componentRootComplete[staging][FileComponent]
         ELSE IF component = DirectoryComponent
              THEN /\ componentRootComplete[staging][FileComponent]
                   /\ componentRootComplete[staging][TagComponent]
              ELSE /\ component = MetadataComponent
                   /\ componentRootComplete[staging][FileComponent]
                   /\ componentRootComplete[staging][TagComponent]
                   /\ componentRootComplete[staging][DirectoryComponent]

ValueHasUpload(value) ==
    \E upload \in canonicalUploads : upload[2] = value

ValueHasConsumer(value) ==
    \E consumer \in canonicalConsumers : consumer[2] = value

ValueGcEligible(value) ==
    /\ value \in canonicalPresent
    /\ value \notin canonicalIdentityPresent
    /\ ~ValueHasConsumer(value)
    /\ ~ValueHasUpload(value)

UploadClaimIsCurrent(upload) ==
    /\ upload \in UploadClaims
    /\ upload[1] = outerGeneration

UploadClaimIsStrictlySuperseded(upload) ==
    /\ upload \in UploadClaims
    /\ upload[1] < outerGeneration

UploadCleanupEligible(upload) ==
    /\ upload \in canonicalUploads
    /\ \/ upload[1] \in completedGenerations
       \/ UploadClaimIsStrictlySuperseded(upload)

AllocatedPrefix ==
    IF nextObservation = 1 THEN {} ELSE 1..(nextObservation - 1)

RecordEvent(kind, accepted, stale, replay) ==
    /\ lastEvent' = kind
    /\ lastAttemptAccepted' = accepted
    /\ lastAttemptWasStale' = stale
    /\ lastAttemptWasReplay' = replay
    /\ replayFingerprint' =
        IF replay THEN DurableFingerprint ELSE NoReplayFingerprint

RecordCurrentAuthorizedEvent(kind, staging, replay) ==
    RecordEvent(
        kind,
        TRUE,
        ~CurrentStagingAuthorized(staging),
        replay
    )

Init ==
    /\ clock = 0
    /\ outerOwner = NoOwner
    /\ outerGeneration = 0
    /\ outerExpiry = 0

    /\ stagingAllocated = {}
    /\ stagingDeleted = {}
    /\ stagingPhase = [staging \in Stagings |-> "UNALLOCATED"]
    /\ stagingHeader = [staging \in Stagings |-> NoHeader]
    /\ allocatedHeader = [staging \in Stagings |-> NoHeader]
    /\ claimIngestGeneration = [staging \in Stagings |-> 0]
    /\ claimGeneration = [staging \in Stagings |-> 0]

    /\ nextObservation = 1
    /\ everAllocatedObservations = {}
    /\ observationOwner =
        [observation \in ObservationIds |-> CHOOSE staging \in Stagings : TRUE]
    /\ stagingObservation =
        [staging \in Stagings |-> NoObservation]
    /\ observationSealed = {SeedObservation}

    /\ pageCommits = {}
    /\ nextLeafPosition =
        [staging \in Stagings |-> [component \in Components |-> 0]]
    /\ carryPending =
        [staging \in Stagings |-> [component \in Components |-> FALSE]]
    /\ componentRootComplete =
        [staging \in Stagings |-> [component \in Components |-> FALSE]]
    /\ metadataParserOffset = [staging \in Stagings |-> 0]
    /\ metadataParserComplete = [staging \in Stagings |-> FALSE]

    /\ matchComplete = [staging \in Stagings |-> FALSE]
    /\ matchObservation =
        [staging \in Stagings |-> NoObservation]
    /\ finalObservation =
        [staging \in Stagings |-> NoObservation]
    /\ reuseCleanupRemaining = [staging \in Stagings |-> {}]

    /\ canonicalPresent = {}
    /\ canonicalIdentityPresent = {}
    /\ canonicalConsumers = {}
    /\ externalRetentionBlockers = {}
    /\ canonicalUploads = {}
    /\ everCanonicalUploads = {}
    /\ sourceMappedGenerations = {}
    /\ completedGenerations = {}
    /\ maintenanceExclusive = FALSE

    /\ lastEvent = "INIT"
    /\ lastAttemptAccepted = FALSE
    /\ lastAttemptWasStale = FALSE
    /\ lastAttemptWasReplay = FALSE
    /\ replayFingerprint = NoReplayFingerprint
    /\ lastCleanupAccepted = FALSE
    /\ lastCleanupWasOpen = FALSE
    /\ lastCleanupWasMaintenance = FALSE
    /\ lastUploadCleanupAccepted = FALSE
    /\ lastUploadCleanupWasMaintenance = FALSE
    /\ lastUploadCleanupWasCurrent = FALSE
    /\ lastUploadCleanupWasEligible = FALSE
    /\ lastUploadCleanupSourceMappingRetained = FALSE
    /\ lastUploadCleanupMadeGcEligible = FALSE
    /\ lastUploadCleanupGeneration = 0
    /\ lastUploadCleanupValue = NoValue
    /\ lastIdentityCleanupAccepted = FALSE
    /\ lastIdentityCleanupWasMaintenance = FALSE
    /\ lastIdentityCleanupHadUpload = FALSE
    /\ lastIdentityCleanupHadConsumer = FALSE
    /\ lastIdentityCleanupMadeGcEligible = FALSE
    /\ lastIdentityCleanupValue = NoValue
    /\ lastConsumerCleanupAccepted = FALSE
    /\ lastConsumerCleanupWasMaintenance = FALSE
    /\ lastConsumerCleanupHadExternalBlocker = FALSE
    /\ lastConsumerCleanupWasCurrent = FALSE
    /\ lastConsumerCleanupWasEligible = FALSE
    /\ lastConsumerCleanupValue = NoValue
    /\ lastMappingCleanupAccepted = FALSE
    /\ lastMappingCleanupWasMaintenance = FALSE
    /\ lastMappingCleanupWasCurrent = FALSE
    /\ lastMappingCleanupHadExternalBlocker = FALSE
    /\ lastMappingCleanupGeneration = 0
    /\ lastValueGcAccepted = FALSE
    /\ lastValueGcHadUpload = FALSE
    /\ lastValueGcWasMaintenance = FALSE
    /\ lastReuseCleanupRemoved = 0

(***************************************************************************
Outer lease lifecycle.  Expiry and takeover do not erase either durable
fence.  A takeover changes the generation and makes the old tuple stale.
Process availability itself is outside this model.
***************************************************************************)

Tick ==
    /\ clock < MaxTime
    /\ clock' = clock + 1
    /\ RecordEvent("CLOCK", FALSE, FALSE, FALSE)
    /\ UNCHANGED DurableProtocolVariables
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

ClaimOuter(owner) ==
    /\ owner \in Owners
    /\ ~maintenanceExclusive
    /\ outerOwner = NoOwner
    /\ outerGeneration < MaxOuterGeneration
    /\ outerOwner' = owner
    /\ outerGeneration' = outerGeneration + 1
    /\ outerExpiry' = clock + LeaseSpan
    /\ RecordEvent("OUTER_CLAIM", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<StagingAllocationVariables, ObservationVariables,
                   PageVariables, FinalizationVariables, CanonicalVariables>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

TakeoverOuter(owner) ==
    /\ owner \in Owners
    /\ ~maintenanceExclusive
    /\ outerOwner # NoOwner
    /\ clock >= outerExpiry
    /\ outerGeneration < MaxOuterGeneration
    /\ outerOwner' = owner
    /\ outerGeneration' = outerGeneration + 1
    /\ outerExpiry' = clock + LeaseSpan
    /\ RecordEvent("OUTER_TAKEOVER", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<StagingAllocationVariables, ObservationVariables,
                   PageVariables, FinalizationVariables, CanonicalVariables>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

(***************************************************************************
sourceMappedGenerations is the bounded projection of
source_build_generation.generation.  It is deliberately independent of
staging identities and canonical upload rows.  The source-root identity is a
bootstrap exception: its generation/value upload claim precedes the source
scope, build, and mapping.  Once the identity is sealed, one atomic consumer
handoff installs the source scope/build/mapping and releases the exact claim;
later domain-value uploads and staging work require that mapping.  Stale
upload cleanup never removes a mapping that exists.
***************************************************************************)

ResumeCurrentSourceBuildGeneration ==
    /\ OuterLeaseIsLive
    /\ outerGeneration \notin sourceMappedGenerations
    /\ \E priorGeneration \in sourceMappedGenerations :
        /\ priorGeneration < outerGeneration
        /\ <<priorGeneration, SourceRootValue>> \in canonicalConsumers
    /\ sourceMappedGenerations' =
        sourceMappedGenerations \cup {outerGeneration}
    /\ RecordEvent("SOURCE_BUILD_RESUME", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables, PageVariables,
                   FinalizationVariables>>
    /\ UNCHANGED
        <<canonicalPresent, canonicalIdentityPresent, canonicalConsumers,
          externalRetentionBlockers, canonicalUploads,
          everCanonicalUploads, completedGenerations,
          maintenanceExclusive>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

GenerationHasRetainedStaging(generation) ==
    \E staging \in stagingAllocated \ stagingDeleted :
        claimIngestGeneration[staging] = generation

CleanupSourceBuildGenerationMapping(generation) ==
    LET wasCurrent == generation = outerGeneration
        hadExternalBlocker ==
            \E blocker \in externalRetentionBlockers :
                blocker[2] = SourceRootValue
    IN
    /\ maintenanceExclusive
    /\ generation \in sourceMappedGenerations
    /\ \/ generation \in completedGenerations
       \/ generation < outerGeneration
    /\ ~GenerationHasRetainedStaging(generation)
    /\ ~wasCurrent
    /\ ~hadExternalBlocker
    /\ ~\E upload \in canonicalUploads :
        upload[1] = generation
    /\ ~\E consumer \in canonicalConsumers :
        /\ consumer[1] = generation
        /\ consumer[2] \in DomainValues
    /\ sourceMappedGenerations' =
        sourceMappedGenerations \ {generation}
    /\ lastMappingCleanupAccepted' = TRUE
    /\ lastMappingCleanupWasMaintenance' = maintenanceExclusive
    /\ lastMappingCleanupWasCurrent' = wasCurrent
    /\ lastMappingCleanupHadExternalBlocker' = hadExternalBlocker
    /\ lastMappingCleanupGeneration' = generation
    /\ RecordEvent("SOURCE_BUILD_MAPPING_CLEANUP", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables, PageVariables,
                   FinalizationVariables>>
    /\ UNCHANGED
        <<canonicalPresent, canonicalIdentityPresent, canonicalConsumers,
          externalRetentionBlockers, canonicalUploads,
          everCanonicalUploads, completedGenerations,
          maintenanceExclusive>>
    /\ UNCHANGED
        <<StagingCleanupAuditVariables, UploadCleanupAuditVariables,
          IdentityCleanupAuditVariables, ConsumerCleanupAuditVariables,
          ValueGcAuditVariables, lastReuseCleanupRemoved>>

RejectExternallyBlockedSourceBuildMappingCleanup(generation) ==
    LET hadExternalBlocker ==
        \E blocker \in externalRetentionBlockers :
            blocker[2] = SourceRootValue
    IN
    /\ maintenanceExclusive
    /\ generation \in sourceMappedGenerations
    /\ hadExternalBlocker
    /\ lastMappingCleanupAccepted' = FALSE
    /\ lastMappingCleanupWasMaintenance' = maintenanceExclusive
    /\ lastMappingCleanupWasCurrent' = (generation = outerGeneration)
    /\ lastMappingCleanupHadExternalBlocker' = hadExternalBlocker
    /\ lastMappingCleanupGeneration' = generation
    /\ RecordEvent(
        "SOURCE_BUILD_MAPPING_CLEANUP_REJECT", FALSE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED DurableProtocolVariables
    /\ UNCHANGED
        <<StagingCleanupAuditVariables, UploadCleanupAuditVariables,
          IdentityCleanupAuditVariables, ConsumerCleanupAuditVariables,
          ValueGcAuditVariables, lastReuseCleanupRemoved>>

CompleteCurrentGeneration ==
    /\ ~maintenanceExclusive
    /\ OuterLeaseTupleIsLive
    /\ outerGeneration \in sourceMappedGenerations
    /\ outerGeneration \notin completedGenerations
    /\ completedGenerations' = completedGenerations \cup {outerGeneration}
    /\ RecordEvent("GENERATION_COMPLETE", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables, PageVariables,
                   FinalizationVariables>>
    /\ UNCHANGED
        <<canonicalPresent, canonicalIdentityPresent, canonicalConsumers,
          externalRetentionBlockers, canonicalUploads, everCanonicalUploads,
          sourceMappedGenerations, maintenanceExclusive>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

(***************************************************************************
Immutable staging allocation/header and the replaceable staging claim.
***************************************************************************)

BeginStaging(staging, header) ==
    /\ OuterLeaseIsLive
    /\ outerGeneration \in sourceMappedGenerations
    /\ staging \in Stagings \ stagingAllocated
    /\ header \in Headers
    /\ stagingAllocated' = stagingAllocated \cup {staging}
    /\ stagingPhase' = [stagingPhase EXCEPT ![staging] = "OPEN"]
    /\ stagingHeader' = [stagingHeader EXCEPT ![staging] = header]
    /\ allocatedHeader' = [allocatedHeader EXCEPT ![staging] = header]
    /\ claimIngestGeneration' =
        [claimIngestGeneration EXCEPT ![staging] = outerGeneration]
    /\ claimGeneration' = [claimGeneration EXCEPT ![staging] = 1]
    /\ RecordEvent("BEGIN", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED OuterFenceVariables
    /\ UNCHANGED stagingDeleted
    /\ UNCHANGED <<ObservationVariables, PageVariables,
                   FinalizationVariables, CanonicalVariables>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

TakeoverStaging(staging) ==
    /\ OuterLeaseIsLive
    /\ outerGeneration \in sourceMappedGenerations
    /\ staging \in stagingAllocated \ stagingDeleted
    /\ stagingPhase[staging] = "OPEN"
    /\ claimIngestGeneration[staging] < outerGeneration
    /\ claimGeneration[staging] < MaxClaimGeneration
    /\ claimIngestGeneration' =
        [claimIngestGeneration EXCEPT ![staging] = outerGeneration]
    /\ claimGeneration' =
        [claimGeneration EXCEPT ![staging] = @ + 1]
    /\ RecordEvent("TAKEOVER", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED OuterFenceVariables
    /\ UNCHANGED
        <<stagingAllocated, stagingDeleted, stagingPhase, stagingHeader,
          allocatedHeader>>
    /\ UNCHANGED <<ObservationVariables, PageVariables,
                   FinalizationVariables, CanonicalVariables>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

(***************************************************************************
Observation allocation is monotone.  An allocated number and its first owner
are never cleared, including the provisional allocation of a reuse staging.
***************************************************************************)

AllocateObservation(staging) ==
    /\ CurrentStagingAuthorized(staging)
    /\ stagingPhase[staging] = "OPEN"
    /\ stagingObservation[staging] = NoObservation
    /\ nextObservation \in ObservationIds
    /\ stagingObservation' =
        [stagingObservation EXCEPT ![staging] = nextObservation]
    /\ observationOwner' =
        [observationOwner EXCEPT ![nextObservation] = staging]
    /\ everAllocatedObservations' =
        everAllocatedObservations \cup {nextObservation}
    /\ nextObservation' = nextObservation + 1
    /\ RecordCurrentAuthorizedEvent("ALLOCATE", staging, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables>>
    /\ UNCHANGED observationSealed
    /\ UNCHANGED <<PageVariables, FinalizationVariables, CanonicalVariables>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

(***************************************************************************
Page protocol.  A singleton terminal carry reuses its only child as the root
and writes no unary branch.  A bounded carry of two or more children commits a
branch root.  Deep checks at most two leaves; the production 257-leaf case
(unary non-root level followed by a two-child higher root) is outside these
finite constants.  A leaf commit can lose the response after the exact request
has become durable.
***************************************************************************)

CommitLeafPage(staging, component, requestBytes, loseResponse) ==
    LET position == nextLeafPosition[staging][component]
        record ==
            <<staging, component, "Leaf", position,
              RequestDigest(requestBytes), requestBytes>>
    IN
    /\ CurrentStagingAuthorized(staging)
    /\ stagingPhase[staging] = "OPEN"
    /\ stagingObservation[staging] # NoObservation
    /\ component \in Components
    /\ requestBytes \in CommitRequestBytes
    /\ loseResponse \in BOOLEAN
    /\ position \in Positions
    /\ loseResponse =>
        /\ component = FileComponent
        /\ position = 0
    /\ PriorComponentRootsComplete(staging, component)
    /\ ~componentRootComplete[staging][component]
    /\ ~PageSlotCommitted(staging, component, "Leaf", position)
    /\ pageCommits' = pageCommits \cup {record}
    /\ nextLeafPosition' =
        [nextLeafPosition EXCEPT ![staging][component] = @ + 1]
    /\ carryPending' =
        [carryPending EXCEPT ![staging][component] = TRUE]
    /\ RecordCurrentAuthorizedEvent(
        IF loseResponse
        THEN "LEAF_COMMIT_RESPONSE_LOST"
        ELSE "LEAF_COMMIT",
        staging, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables>>
    /\ UNCHANGED
        <<componentRootComplete, metadataParserOffset,
          metadataParserComplete>>
    /\ UNCHANGED <<FinalizationVariables, CanonicalVariables>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

ReuseOnlyChildAsRoot(staging, component) ==
    /\ CurrentStagingAuthorized(staging)
    /\ stagingPhase[staging] = "OPEN"
    /\ component \in Components
    /\ carryPending[staging][component]
    /\ nextLeafPosition[staging][component] = 1
    /\ ~componentRootComplete[staging][component]
    /\ ~PageSlotCommitted(staging, component, "Branch", 0)
    /\ carryPending' =
        [carryPending EXCEPT ![staging][component] = FALSE]
    /\ componentRootComplete' =
        [componentRootComplete EXCEPT ![staging][component] = TRUE]
    /\ RecordCurrentAuthorizedEvent("REUSE_CHILD_ROOT", staging, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables>>
    /\ UNCHANGED
        <<pageCommits, nextLeafPosition, metadataParserOffset,
          metadataParserComplete>>
    /\ UNCHANGED <<FinalizationVariables, CanonicalVariables>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

CommitBranchPage(staging, component, requestBytes) ==
    LET record ==
        <<staging, component, "Branch", 0,
          RequestDigest(requestBytes), requestBytes>>
    IN
    /\ CurrentStagingAuthorized(staging)
    /\ stagingPhase[staging] = "OPEN"
    /\ component \in Components
    /\ requestBytes \in CommitRequestBytes
    /\ carryPending[staging][component]
    /\ nextLeafPosition[staging][component] >= 2
    /\ ~componentRootComplete[staging][component]
    /\ ~PageSlotCommitted(staging, component, "Branch", 0)
    /\ pageCommits' = pageCommits \cup {record}
    /\ carryPending' =
        [carryPending EXCEPT ![staging][component] = FALSE]
    /\ componentRootComplete' =
        [componentRootComplete EXCEPT ![staging][component] = TRUE]
    /\ RecordCurrentAuthorizedEvent("BRANCH_COMMIT", staging, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables>>
    /\ UNCHANGED
        <<nextLeafPosition, metadataParserOffset, metadataParserComplete>>
    /\ UNCHANGED <<FinalizationVariables, CanonicalVariables>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

(***************************************************************************
Request-token equality, replay immutability, and stale fencing do not depend
on component semantics.  Their fault probes therefore use FILE/Leaf/0 as one
representative committed slot; all component slots still share the same
commit relation and exact-slot uniqueness invariant.
***************************************************************************)

ReplayCommittedPage(staging, component, level, position, requestBytes) ==
    /\ CurrentStagingAuthorized(staging)
    /\ stagingPhase[staging] \in {"OPEN", "SEALED", "REUSED"}
    /\ component \in Components
    /\ level \in Levels
    /\ position \in Positions
    /\ requestBytes \in RequestBytes
    /\ component = FileComponent
    /\ level = "Leaf"
    /\ position = 0
    /\ <<staging, component, level, position,
          RequestDigest(requestBytes), requestBytes>> \in pageCommits
    /\ RecordCurrentAuthorizedEvent("REPLAY", staging, TRUE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED DurableProtocolVariables
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

RejectChangedPageRequest(staging, component, level, position, requestBytes) ==
    /\ CurrentStagingAuthorized(staging)
    /\ component \in Components
    /\ level \in Levels
    /\ position \in Positions
    /\ requestBytes \in RequestBytes
    /\ component = FileComponent
    /\ level = "Leaf"
    /\ position = 0
    /\ PageSlotCommitted(staging, component, level, position)
    /\ requestBytes \notin
        SlotRequestBytes(staging, component, level, position)
    /\ RecordEvent("CHANGED_REQUEST_REJECT", FALSE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED DurableProtocolVariables
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

RejectStalePageAttempt(staging, component, level, position, requestBytes) ==
    LET exactReplay ==
        <<staging, component, level, position,
          RequestDigest(requestBytes), requestBytes>> \in pageCommits
    IN
    /\ staging \in stagingAllocated \ stagingDeleted
    /\ component \in Components
    /\ level \in Levels
    /\ position \in Positions
    /\ requestBytes \in RequestBytes
    /\ component = FileComponent
    /\ level = "Leaf"
    /\ position = 0
    /\ StaleTupleExists(staging)
    /\ RecordEvent(
        IF exactReplay
        THEN "STALE_REPLAY_REJECT"
        ELSE "STALE_MUTATION_REJECT",
        FALSE, TRUE, exactReplay)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED DurableProtocolVariables
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

(***************************************************************************
The metadata parser advances monotonically.  Sealing and reuse both require
all four (configured) roots, an empty carry frontier, parser completion, and
a completed new/reuse match.
***************************************************************************)

AdvanceMetadataParser(staging) ==
    /\ CurrentStagingAuthorized(staging)
    /\ stagingPhase[staging] = "OPEN"
    /\ PageSlotCommitted(staging, MetadataComponent, "Leaf", 0)
    /\ metadataParserOffset[staging] < MetadataUnits
    /\ metadataParserOffset' =
        [metadataParserOffset EXCEPT ![staging] = @ + 1]
    /\ metadataParserComplete' =
        [metadataParserComplete EXCEPT
            ![staging] = (metadataParserOffset[staging] + 1 = MetadataUnits)]
    /\ RecordCurrentAuthorizedEvent("PARSER_ADVANCE", staging, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables>>
    /\ UNCHANGED
        <<pageCommits, nextLeafPosition, carryPending,
          componentRootComplete>>
    /\ UNCHANGED <<FinalizationVariables, CanonicalVariables>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

CompleteNewMatch(staging) ==
    /\ CurrentStagingAuthorized(staging)
    /\ stagingPhase[staging] = "OPEN"
    /\ stagingObservation[staging] # NoObservation
    /\ AllComponentRootsComplete(staging)
    /\ AllCarriesFlushed(staging)
    /\ metadataParserComplete[staging]
    /\ ~matchComplete[staging]
    /\ matchComplete' = [matchComplete EXCEPT ![staging] = TRUE]
    /\ matchObservation' =
        [matchObservation EXCEPT ![staging] = stagingObservation[staging]]
    /\ RecordCurrentAuthorizedEvent("MATCH_NEW", staging, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables, PageVariables>>
    /\ UNCHANGED <<finalObservation, reuseCleanupRemaining>>
    /\ UNCHANGED CanonicalVariables
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

CompleteReuseMatch(staging, reusedObservation) ==
    /\ CurrentStagingAuthorized(staging)
    /\ stagingPhase[staging] = "OPEN"
    /\ stagingObservation[staging] # NoObservation
    /\ reusedObservation \in observationSealed
    /\ reusedObservation # stagingObservation[staging]
    /\ AllComponentRootsComplete(staging)
    /\ AllCarriesFlushed(staging)
    /\ metadataParserComplete[staging]
    /\ ~matchComplete[staging]
    /\ matchComplete' = [matchComplete EXCEPT ![staging] = TRUE]
    /\ matchObservation' =
        [matchObservation EXCEPT ![staging] = reusedObservation]
    /\ RecordCurrentAuthorizedEvent("MATCH_REUSE", staging, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables, PageVariables>>
    /\ UNCHANGED <<finalObservation, reuseCleanupRemaining>>
    /\ UNCHANGED CanonicalVariables
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

SealNewObservation(staging) ==
    LET observation == stagingObservation[staging]
    IN
    /\ CurrentStagingAuthorized(staging)
    /\ stagingPhase[staging] = "OPEN"
    /\ matchComplete[staging]
    /\ matchObservation[staging] = observation
    /\ observation \in everAllocatedObservations
    /\ AllComponentRootsComplete(staging)
    /\ AllCarriesFlushed(staging)
    /\ metadataParserComplete[staging]
    /\ stagingPhase' = [stagingPhase EXCEPT ![staging] = "SEALED"]
    /\ observationSealed' = observationSealed \cup {observation}
    /\ finalObservation' =
        [finalObservation EXCEPT ![staging] = observation]
    /\ RecordCurrentAuthorizedEvent("SEAL_NEW", staging, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED OuterFenceVariables
    /\ UNCHANGED
        <<stagingAllocated, stagingDeleted, stagingHeader, allocatedHeader,
          claimIngestGeneration, claimGeneration>>
    /\ UNCHANGED
        <<nextObservation, everAllocatedObservations, observationOwner,
          stagingObservation>>
    /\ UNCHANGED PageVariables
    /\ UNCHANGED <<matchComplete, matchObservation, reuseCleanupRemaining>>
    /\ UNCHANGED CanonicalVariables
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

FinalizeReuse(staging) ==
    /\ CurrentStagingAuthorized(staging)
    /\ stagingPhase[staging] = "OPEN"
    /\ matchComplete[staging]
    /\ matchObservation[staging] \in observationSealed
    /\ matchObservation[staging] # stagingObservation[staging]
    /\ AllComponentRootsComplete(staging)
    /\ AllCarriesFlushed(staging)
    /\ metadataParserComplete[staging]
    /\ stagingPhase' = [stagingPhase EXCEPT ![staging] = "REUSED"]
    /\ finalObservation' =
        [finalObservation EXCEPT ![staging] = matchObservation[staging]]
    /\ reuseCleanupRemaining' =
        [reuseCleanupRemaining EXCEPT ![staging] = StagingPageRecords(staging)]
    /\ RecordCurrentAuthorizedEvent("FINALIZE_REUSE", staging, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED OuterFenceVariables
    /\ UNCHANGED
        <<stagingAllocated, stagingDeleted, stagingHeader, allocatedHeader,
          claimIngestGeneration, claimGeneration>>
    /\ UNCHANGED ObservationVariables
    /\ UNCHANGED PageVariables
    /\ UNCHANGED <<matchComplete, matchObservation>>
    /\ UNCHANGED CanonicalVariables
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

CleanupOneReusePage(staging, entry) ==
    /\ CurrentStagingAuthorized(staging)
    /\ stagingPhase[staging] = "REUSED"
    /\ entry \in reuseCleanupRemaining[staging]
    /\ entry \in pageCommits
    /\ pageCommits' = pageCommits \ {entry}
    /\ reuseCleanupRemaining' =
        [reuseCleanupRemaining EXCEPT ![staging] = @ \ {entry}]
    /\ lastReuseCleanupRemoved' = 1
    /\ RecordCurrentAuthorizedEvent("REUSE_PAGE_CLEANUP", staging, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables>>
    /\ UNCHANGED
        <<nextLeafPosition, carryPending, componentRootComplete,
          metadataParserOffset, metadataParserComplete>>
    /\ UNCHANGED <<matchComplete, matchObservation, finalObservation>>
    /\ UNCHANGED CanonicalVariables
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables>>

(***************************************************************************
canonicalUploads has the exact canonical_value_upload key shape:
<<ingest generation, value sha>>.  It carries no staging, owner, lease, or
staging-claim columns.  The owner/lease fence is obtained from the generation.
When a canonical value is absent, allocation and insertion of that exact
generation/value claim occur in one transition.  The same transition also
models a later generation claiming an already-present value.  SourceRootValue
is the explicit pre-mapping bootstrap exception; every domain value requires
the source-build mapping.  Neither path depends on a staging row.  Identity
seal retains its exact claim.  A separate handoff transition inserts a
durable, retention-blocking external consumer and releases only that exact
claim atomically; a phase-owned type/dictionary row alone is deliberately not
such a consumer.  For the source root, source_build_generation is the exact
retention-blocking consumer installed in that same transaction.  A domain
consumer is the bounded abstraction of an external catalog/reference edge.

Staging cleanup is intentionally independent of this relation.  A completed
or strictly superseded generation claim can instead be removed under exclusive
maintenance after rechecking that its generation is not the current
coordination head, regardless of lease expiry.  Removing the last claim
immediately enables value GC even though the source-build mapping remains
retained.  Scheduler progress beyond enablement is not asserted without a
fairness assumption.
***************************************************************************)

AllocateAndClaimCanonicalValue(value) ==
    LET upload == <<outerGeneration, value>>
    IN
    /\ OuterLeaseIsLive
    /\ value \in Values
    /\ \/ /\ value = SourceRootValue
          /\ outerGeneration \notin sourceMappedGenerations
       \/ /\ value \in DomainValues
          /\ outerGeneration \in sourceMappedGenerations
    /\ upload \notin everCanonicalUploads
    /\ canonicalPresent' = canonicalPresent \cup {value}
    /\ canonicalUploads' = canonicalUploads \cup {upload}
    /\ everCanonicalUploads' = everCanonicalUploads \cup {upload}
    /\ RecordEvent("UPLOAD_ALLOCATE_CLAIM", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables, PageVariables,
                   FinalizationVariables>>
    /\ UNCHANGED
        <<canonicalIdentityPresent, canonicalConsumers,
          externalRetentionBlockers,
          sourceMappedGenerations,
          completedGenerations,
          maintenanceExclusive>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

SealCanonicalIdentity(value) ==
    LET upload == <<outerGeneration, value>>
    IN
    /\ OuterLeaseIsLive
    /\ \/ /\ value = SourceRootValue
          /\ outerGeneration \notin sourceMappedGenerations
       \/ /\ value \in DomainValues
          /\ outerGeneration \in sourceMappedGenerations
    /\ upload \in canonicalUploads
    /\ value \notin canonicalIdentityPresent
    /\ canonicalIdentityPresent' = canonicalIdentityPresent \cup {value}
    /\ RecordEvent("UPLOAD_IDENTITY_SEAL", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables, PageVariables,
                   FinalizationVariables>>
    /\ UNCHANGED
        <<canonicalPresent, canonicalConsumers, externalRetentionBlockers,
          canonicalUploads,
          everCanonicalUploads, sourceMappedGenerations,
          completedGenerations, maintenanceExclusive>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

ConsumeAndReleaseCanonicalUpload(value) ==
    LET upload == <<outerGeneration, value>>
    IN
    /\ OuterLeaseIsLive
    /\ upload \in canonicalUploads
    /\ value \in canonicalIdentityPresent
    /\ upload \notin canonicalConsumers
    /\ \/ /\ value = SourceRootValue
          /\ outerGeneration \notin sourceMappedGenerations
       \/ /\ value \in DomainValues
          /\ outerGeneration \in sourceMappedGenerations
    /\ canonicalConsumers' = canonicalConsumers \cup {upload}
    /\ canonicalUploads' = canonicalUploads \ {upload}
    /\ UNCHANGED externalRetentionBlockers
    /\ sourceMappedGenerations' =
        IF value = SourceRootValue
        THEN sourceMappedGenerations \cup {outerGeneration}
        ELSE sourceMappedGenerations
    /\ RecordEvent("UPLOAD_CONSUME_RELEASE", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables, PageVariables,
                   FinalizationVariables>>
    /\ UNCHANGED
        <<canonicalPresent, canonicalIdentityPresent,
          everCanonicalUploads, completedGenerations,
          maintenanceExclusive>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

EnterMaintenance ==
    /\ ~maintenanceExclusive
    /\ maintenanceExclusive' = TRUE
    /\ RecordEvent("MAINTENANCE_ENTER", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables, PageVariables,
                   FinalizationVariables>>
    /\ UNCHANGED
        <<canonicalPresent, canonicalIdentityPresent, canonicalConsumers,
          externalRetentionBlockers, canonicalUploads, everCanonicalUploads,
          sourceMappedGenerations, completedGenerations>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

ExitMaintenance ==
    /\ maintenanceExclusive
    /\ maintenanceExclusive' = FALSE
    /\ RecordEvent("MAINTENANCE_EXIT", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables, PageVariables,
                   FinalizationVariables>>
    /\ UNCHANGED
        <<canonicalPresent, canonicalIdentityPresent, canonicalConsumers,
          externalRetentionBlockers, canonicalUploads, everCanonicalUploads,
          sourceMappedGenerations, completedGenerations>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

CleanupStaleCanonicalUpload(upload) ==
    LET generation == upload[1]
        value == upload[2]
        wasEligible == UploadCleanupEligible(upload)
        wasCurrent == UploadClaimIsCurrent(upload)
        sourceMappingWasPresent == generation \in sourceMappedGenerations
        remaining == canonicalUploads \ {upload}
        madeGcEligible ==
            /\ value \in canonicalPresent
            /\ value \notin canonicalIdentityPresent
            /\ ~ValueHasConsumer(value)
            /\ ~\E other \in remaining : other[2] = value
    IN
    /\ maintenanceExclusive
    /\ upload \in canonicalUploads
    /\ wasEligible
    /\ ~wasCurrent
    /\ canonicalUploads' = remaining
    /\ lastUploadCleanupAccepted' = TRUE
    /\ lastUploadCleanupWasMaintenance' = maintenanceExclusive
    /\ lastUploadCleanupWasCurrent' = wasCurrent
    /\ lastUploadCleanupWasEligible' = wasEligible
    /\ lastUploadCleanupSourceMappingRetained' = sourceMappingWasPresent
    /\ lastUploadCleanupMadeGcEligible' = madeGcEligible
    /\ lastUploadCleanupGeneration' = generation
    /\ lastUploadCleanupValue' = value
    /\ RecordEvent("STALE_UPLOAD_CLEANUP", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables, PageVariables,
                   FinalizationVariables>>
    /\ UNCHANGED
        <<canonicalPresent, canonicalIdentityPresent, canonicalConsumers,
          externalRetentionBlockers, everCanonicalUploads,
          sourceMappedGenerations,
          completedGenerations, maintenanceExclusive>>
    /\ UNCHANGED
        <<StagingCleanupAuditVariables, IdentityCleanupAuditVariables,
          ConsumerCleanupAuditVariables, MappingCleanupAuditVariables,
          ValueGcAuditVariables,
          lastReuseCleanupRemoved>>

CleanupCompletedCanonicalUpload(upload) ==
    /\ upload[1] \in completedGenerations
    /\ CleanupStaleCanonicalUpload(upload)

CleanupSupersededCanonicalUpload(upload) ==
    /\ UploadClaimIsStrictlySuperseded(upload)
    /\ CleanupStaleCanonicalUpload(upload)

AddExternalRetentionBlocker(consumer) ==
    /\ consumer \in canonicalConsumers
    /\ consumer \notin externalRetentionBlockers
    /\ externalRetentionBlockers' =
        externalRetentionBlockers \cup {consumer}
    /\ RecordEvent("EXTERNAL_BLOCKER_ADD", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables, PageVariables,
                   FinalizationVariables>>
    /\ UNCHANGED
        <<canonicalPresent, canonicalIdentityPresent, canonicalConsumers,
          canonicalUploads, everCanonicalUploads, sourceMappedGenerations,
          completedGenerations, maintenanceExclusive>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

RemoveExternalRetentionBlocker(consumer) ==
    /\ consumer \in externalRetentionBlockers
    /\ externalRetentionBlockers' =
        externalRetentionBlockers \ {consumer}
    /\ RecordEvent("EXTERNAL_BLOCKER_REMOVE", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables, PageVariables,
                   FinalizationVariables>>
    /\ UNCHANGED
        <<canonicalPresent, canonicalIdentityPresent, canonicalConsumers,
          canonicalUploads, everCanonicalUploads, sourceMappedGenerations,
          completedGenerations, maintenanceExclusive>>
    /\ UNCHANGED <<CleanupAuditVariables, ValueGcAuditVariables,
                   lastReuseCleanupRemoved>>

CleanupCanonicalConsumer(consumer) ==
    LET value == consumer[2]
        hadExternalBlocker == consumer \in externalRetentionBlockers
        wasCurrent == consumer[1] = outerGeneration
        wasEligible ==
            \/ consumer[1] \in completedGenerations
            \/ consumer[1] < outerGeneration
    IN
    /\ maintenanceExclusive
    /\ consumer \in canonicalConsumers
    /\ ~hadExternalBlocker
    /\ wasEligible
    /\ ~wasCurrent
    /\ \/ value \in DomainValues
       \/ /\ value = SourceRootValue
          /\ sourceMappedGenerations = {}
    /\ canonicalConsumers' = canonicalConsumers \ {consumer}
    /\ lastConsumerCleanupAccepted' = TRUE
    /\ lastConsumerCleanupWasMaintenance' = maintenanceExclusive
    /\ lastConsumerCleanupHadExternalBlocker' = hadExternalBlocker
    /\ lastConsumerCleanupWasCurrent' = wasCurrent
    /\ lastConsumerCleanupWasEligible' = wasEligible
    /\ lastConsumerCleanupValue' = value
    /\ RecordEvent("CONSUMER_CLEANUP", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables, PageVariables,
                   FinalizationVariables>>
    /\ UNCHANGED
        <<canonicalPresent, canonicalIdentityPresent,
          externalRetentionBlockers, canonicalUploads,
          everCanonicalUploads, sourceMappedGenerations,
          completedGenerations, maintenanceExclusive>>
    /\ UNCHANGED
        <<StagingCleanupAuditVariables, UploadCleanupAuditVariables,
          IdentityCleanupAuditVariables, MappingCleanupAuditVariables,
          ValueGcAuditVariables,
          lastReuseCleanupRemoved>>

CleanupOrphanCanonicalIdentity(value) ==
    LET hadUpload == ValueHasUpload(value)
        hadConsumer == ValueHasConsumer(value)
        madeGcEligible ==
            /\ value \in canonicalPresent
            /\ ~hadUpload
            /\ ~hadConsumer
    IN
    /\ maintenanceExclusive
    /\ value \in canonicalIdentityPresent
    /\ ~hadUpload
    /\ ~hadConsumer
    /\ canonicalIdentityPresent' = canonicalIdentityPresent \ {value}
    /\ lastIdentityCleanupAccepted' = TRUE
    /\ lastIdentityCleanupWasMaintenance' = maintenanceExclusive
    /\ lastIdentityCleanupHadUpload' = hadUpload
    /\ lastIdentityCleanupHadConsumer' = hadConsumer
    /\ lastIdentityCleanupMadeGcEligible' = madeGcEligible
    /\ lastIdentityCleanupValue' = value
    /\ RecordEvent("ORPHAN_IDENTITY_CLEANUP", TRUE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables, PageVariables,
                   FinalizationVariables>>
    /\ UNCHANGED
        <<canonicalPresent, canonicalConsumers, externalRetentionBlockers,
          canonicalUploads, everCanonicalUploads, sourceMappedGenerations,
          completedGenerations, maintenanceExclusive>>
    /\ UNCHANGED
        <<StagingCleanupAuditVariables, UploadCleanupAuditVariables,
          ConsumerCleanupAuditVariables, MappingCleanupAuditVariables,
          ValueGcAuditVariables,
          lastReuseCleanupRemoved>>

AttemptStagingCleanup(staging) ==
    LET wasOpen == stagingPhase[staging] = "OPEN"
        terminal == stagingPhase[staging] \in {"SEALED", "REUSED"}
        reuseClean ==
            stagingPhase[staging] # "REUSED"
            \/ reuseCleanupRemaining[staging] = {}
        accepted ==
            /\ staging \in stagingAllocated \ stagingDeleted
            /\ terminal
            /\ reuseClean
    IN
    /\ staging \in stagingAllocated \ stagingDeleted
    /\ maintenanceExclusive
    /\ stagingDeleted' =
        IF accepted THEN stagingDeleted \cup {staging} ELSE stagingDeleted
    /\ stagingPhase' =
        IF accepted
        THEN [stagingPhase EXCEPT ![staging] = "DELETED"]
        ELSE stagingPhase
    /\ lastCleanupAccepted' = accepted
    /\ lastCleanupWasOpen' = wasOpen
    /\ lastCleanupWasMaintenance' = maintenanceExclusive
    /\ RecordEvent("STAGING_CLEANUP", FALSE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED OuterFenceVariables
    /\ UNCHANGED
        <<stagingAllocated, stagingHeader, allocatedHeader,
          claimIngestGeneration, claimGeneration>>
    /\ UNCHANGED <<ObservationVariables, PageVariables,
                   FinalizationVariables, CanonicalVariables>>
    /\ UNCHANGED
        <<UploadCleanupAuditVariables, IdentityCleanupAuditVariables,
          ConsumerCleanupAuditVariables, MappingCleanupAuditVariables,
          ValueGcAuditVariables,
          lastReuseCleanupRemoved>>

AttemptCanonicalValueGc(value) ==
    LET hadUpload == ValueHasUpload(value)
        accepted ==
            /\ value \in canonicalPresent
            /\ value \notin canonicalIdentityPresent
            /\ ~ValueHasConsumer(value)
            /\ ~hadUpload
    IN
    /\ value \in Values
    /\ maintenanceExclusive
    /\ canonicalPresent' =
        IF accepted THEN canonicalPresent \ {value} ELSE canonicalPresent
    /\ lastValueGcAccepted' = accepted
    /\ lastValueGcHadUpload' = hadUpload
    /\ lastValueGcWasMaintenance' = maintenanceExclusive
    /\ RecordEvent("VALUE_GC", FALSE, FALSE, FALSE)
    /\ UNCHANGED ProcessVariables
    /\ UNCHANGED <<OuterFenceVariables, StagingAllocationVariables,
                   ObservationVariables, PageVariables,
                   FinalizationVariables>>
    /\ UNCHANGED
        <<canonicalIdentityPresent, canonicalConsumers,
          externalRetentionBlockers,
          canonicalUploads, everCanonicalUploads,
          sourceMappedGenerations, completedGenerations,
          maintenanceExclusive>>
    /\ UNCHANGED <<CleanupAuditVariables, lastReuseCleanupRemoved>>

Next ==
    \/ Tick
    \/ \E owner \in Owners : ClaimOuter(owner)
    \/ \E owner \in Owners : TakeoverOuter(owner)
    \/ ResumeCurrentSourceBuildGeneration
    \/ CompleteCurrentGeneration
    \/ \E generation \in OuterGenerations :
        CleanupSourceBuildGenerationMapping(generation)
    \/ \E generation \in OuterGenerations :
        RejectExternallyBlockedSourceBuildMappingCleanup(generation)
    \/ \E staging \in Stagings,
          header \in Headers :
        BeginStaging(staging, header)
    \/ \E staging \in Stagings : TakeoverStaging(staging)
    \/ \E staging \in Stagings : AllocateObservation(staging)
    \/ \E staging \in Stagings,
          component \in Components :
        ReuseOnlyChildAsRoot(staging, component)
    \/ \E staging \in Stagings,
          component \in Components,
          requestBytes \in RequestBytes,
          loseResponse \in BOOLEAN :
        CommitLeafPage(staging, component, requestBytes, loseResponse)
    \/ \E staging \in Stagings,
          component \in Components,
          requestBytes \in RequestBytes :
        CommitBranchPage(staging, component, requestBytes)
    \/ \E staging \in Stagings,
          component \in Components,
          level \in Levels,
          position \in Positions,
          requestBytes \in RequestBytes :
        ReplayCommittedPage(staging, component, level, position, requestBytes)
    \/ \E staging \in Stagings,
          component \in Components,
          level \in Levels,
          position \in Positions,
          requestBytes \in RequestBytes :
        RejectChangedPageRequest(
            staging, component, level, position, requestBytes)
    \/ \E staging \in Stagings,
          component \in Components,
          level \in Levels,
          position \in Positions,
          requestBytes \in RequestBytes :
        RejectStalePageAttempt(
            staging, component, level, position, requestBytes)
    \/ \E staging \in Stagings : AdvanceMetadataParser(staging)
    \/ \E staging \in Stagings : CompleteNewMatch(staging)
    \/ \E staging \in Stagings,
          observation \in AllObservations :
        CompleteReuseMatch(staging, observation)
    \/ \E staging \in Stagings : SealNewObservation(staging)
    \/ \E staging \in Stagings : FinalizeReuse(staging)
    \/ \E staging \in Stagings,
          entry \in PageRecords :
        CleanupOneReusePage(staging, entry)
    \/ \E value \in Values : AllocateAndClaimCanonicalValue(value)
    \/ \E value \in Values : SealCanonicalIdentity(value)
    \/ \E value \in Values : ConsumeAndReleaseCanonicalUpload(value)
    \/ EnterMaintenance
    \/ ExitMaintenance
    \/ \E upload \in UploadClaims :
        CleanupCompletedCanonicalUpload(upload)
    \/ \E upload \in UploadClaims :
        CleanupSupersededCanonicalUpload(upload)
    \/ \E consumer \in UploadClaims :
        AddExternalRetentionBlocker(consumer)
    \/ \E consumer \in UploadClaims :
        RemoveExternalRetentionBlocker(consumer)
    \/ \E consumer \in UploadClaims : CleanupCanonicalConsumer(consumer)
    \/ \E value \in Values : CleanupOrphanCanonicalIdentity(value)
    \/ \E staging \in Stagings : AttemptStagingCleanup(staging)
    \/ \E value \in Values : AttemptCanonicalValueGc(value)

Spec == Init /\ [][Next]_vars

(***************************************************************************
Safety invariants.  These are finite-state TLC obligations for a selected cfg;
they are not claims about unbounded pages, galleries, clocks, or identities.
***************************************************************************)

TypeOK ==
    /\ clock \in 0..MaxTime
    /\ outerOwner \in Owners \cup {NoOwner}
    /\ outerGeneration \in OuterGenerations
    /\ outerExpiry \in 0..(MaxTime + LeaseSpan)
    /\ stagingAllocated \subseteq Stagings
    /\ stagingDeleted \subseteq stagingAllocated
    /\ stagingPhase \in [Stagings -> StagingPhases]
    /\ stagingHeader \in [Stagings -> Headers \cup {NoHeader}]
    /\ allocatedHeader \in [Stagings -> Headers \cup {NoHeader}]
    /\ claimIngestGeneration \in [Stagings -> OuterGenerations]
    /\ claimGeneration \in [Stagings -> ClaimGenerations]
    /\ nextObservation \in 1..(MaxObservation + 1)
    /\ everAllocatedObservations \subseteq ObservationIds
    /\ observationOwner \in [ObservationIds -> Stagings]
    /\ stagingObservation \in
        [Stagings -> ObservationIds \cup {NoObservation}]
    /\ observationSealed \subseteq AllObservations
    /\ pageCommits \subseteq PageRecords
    /\ nextLeafPosition \in
        [Stagings -> [Components -> 0..(MaxPagePosition + 1)]]
    /\ carryPending \in [Stagings -> [Components -> BOOLEAN]]
    /\ componentRootComplete \in
        [Stagings -> [Components -> BOOLEAN]]
    /\ metadataParserOffset \in [Stagings -> 0..MetadataUnits]
    /\ metadataParserComplete \in [Stagings -> BOOLEAN]
    /\ matchComplete \in [Stagings -> BOOLEAN]
    /\ matchObservation \in
        [Stagings -> AllObservations \cup {NoObservation}]
    /\ finalObservation \in
        [Stagings -> AllObservations \cup {NoObservation}]
    /\ reuseCleanupRemaining \in [Stagings -> SUBSET PageRecords]
    /\ canonicalPresent \subseteq Values
    /\ canonicalIdentityPresent \subseteq canonicalPresent
    /\ canonicalConsumers \subseteq UploadClaims
    /\ externalRetentionBlockers \subseteq canonicalConsumers
    /\ canonicalUploads \subseteq UploadClaims
    /\ everCanonicalUploads \subseteq UploadClaims
    /\ sourceMappedGenerations \subseteq 1..MaxOuterGeneration
    /\ completedGenerations \subseteq 1..MaxOuterGeneration
    /\ maintenanceExclusive \in BOOLEAN
    /\ lastEvent \in EventKinds
    /\ lastAttemptAccepted \in BOOLEAN
    /\ lastAttemptWasStale \in BOOLEAN
    /\ lastAttemptWasReplay \in BOOLEAN
    /\ lastCleanupAccepted \in BOOLEAN
    /\ lastCleanupWasOpen \in BOOLEAN
    /\ lastCleanupWasMaintenance \in BOOLEAN
    /\ lastUploadCleanupAccepted \in BOOLEAN
    /\ lastUploadCleanupWasMaintenance \in BOOLEAN
    /\ lastUploadCleanupWasCurrent \in BOOLEAN
    /\ lastUploadCleanupWasEligible \in BOOLEAN
    /\ lastUploadCleanupSourceMappingRetained \in BOOLEAN
    /\ lastUploadCleanupMadeGcEligible \in BOOLEAN
    /\ lastUploadCleanupGeneration \in OuterGenerations
    /\ lastUploadCleanupValue \in Values \cup {NoValue}
    /\ lastIdentityCleanupAccepted \in BOOLEAN
    /\ lastIdentityCleanupWasMaintenance \in BOOLEAN
    /\ lastIdentityCleanupHadUpload \in BOOLEAN
    /\ lastIdentityCleanupHadConsumer \in BOOLEAN
    /\ lastIdentityCleanupMadeGcEligible \in BOOLEAN
    /\ lastIdentityCleanupValue \in Values \cup {NoValue}
    /\ lastConsumerCleanupAccepted \in BOOLEAN
    /\ lastConsumerCleanupWasMaintenance \in BOOLEAN
    /\ lastConsumerCleanupHadExternalBlocker \in BOOLEAN
    /\ lastConsumerCleanupWasCurrent \in BOOLEAN
    /\ lastConsumerCleanupWasEligible \in BOOLEAN
    /\ lastConsumerCleanupValue \in Values \cup {NoValue}
    /\ lastMappingCleanupAccepted \in BOOLEAN
    /\ lastMappingCleanupWasMaintenance \in BOOLEAN
    /\ lastMappingCleanupWasCurrent \in BOOLEAN
    /\ lastMappingCleanupHadExternalBlocker \in BOOLEAN
    /\ lastMappingCleanupGeneration \in OuterGenerations
    /\ lastValueGcAccepted \in BOOLEAN
    /\ lastValueGcHadUpload \in BOOLEAN
    /\ lastValueGcWasMaintenance \in BOOLEAN
    /\ lastReuseCleanupRemoved \in {0, 1}

ImmutableStagingAllocationAndHeader ==
    /\ \A staging \in stagingAllocated :
        /\ stagingHeader[staging] \in Headers
        /\ stagingHeader[staging] = allocatedHeader[staging]
        /\ stagingPhase[staging] # "UNALLOCATED"
        /\ claimIngestGeneration[staging] \in 1..MaxOuterGeneration
        /\ claimGeneration[staging] \in 1..MaxClaimGeneration
    /\ \A staging \in Stagings \ stagingAllocated :
        /\ stagingHeader[staging] = NoHeader
        /\ allocatedHeader[staging] = NoHeader
        /\ stagingPhase[staging] = "UNALLOCATED"
        /\ claimIngestGeneration[staging] = 0
        /\ claimGeneration[staging] = 0
    /\ \A staging \in stagingDeleted : stagingPhase[staging] = "DELETED"

ObservationAllocationIsMonotonicAndUnique ==
    /\ everAllocatedObservations = AllocatedPrefix
    /\ \A observation \in everAllocatedObservations :
        /\ observationOwner[observation] \in stagingAllocated
        /\ stagingObservation[observationOwner[observation]] = observation
    /\ \A left, right \in Stagings :
        IF /\ stagingObservation[left] # NoObservation
           /\ stagingObservation[left] = stagingObservation[right]
        THEN left = right
        ELSE TRUE
    /\ observationSealed \ {SeedObservation}
        \subseteq everAllocatedObservations

ExactPageSlotIdentity ==
    /\ \A staging \in Stagings,
          component \in Components,
          level \in Levels,
          position \in Positions :
        Cardinality(
            SlotRequestBytes(staging, component, level, position)) <= 1
    /\ \A staging \in stagingAllocated \ stagingDeleted,
          component \in Components :
        IF stagingPhase[staging] \in {"OPEN", "SEALED"}
        THEN
            /\ \A position \in Positions :
                IF position < nextLeafPosition[staging][component]
                THEN PageSlotCommitted(
                    staging, component, "Leaf", position)
                ELSE TRUE
            /\ componentRootComplete[staging][component]
               => IF nextLeafPosition[staging][component] = 1
                  THEN ~PageSlotCommitted(
                      staging, component, "Branch", 0)
                  ELSE PageSlotCommitted(
                      staging, component, "Branch", 0)
        ELSE TRUE

ExactRequestBytesNotDigestAlone ==
    \A staging \in Stagings,
       component \in Components,
       level \in Levels,
       position \in Positions :
        \A committedBytes \in
               SlotRequestBytes(staging, component, level, position),
           candidateBytes \in RequestBytes :
            IF /\ candidateBytes # committedBytes
               /\ RequestDigest(candidateBytes) = RequestDigest(committedBytes)
            THEN candidateBytes \notin
                SlotRequestBytes(staging, component, level, position)
            ELSE TRUE

ComponentRootsHaveMinimalHeight ==
    \A staging \in stagingAllocated \ stagingDeleted,
       component \in Components :
        IF componentRootComplete[staging][component]
        THEN
            /\ nextLeafPosition[staging][component] >= 1
            /\ (PageSlotCommitted(staging, component, "Branch", 0) <=>
                    nextLeafPosition[staging][component] >= 2)
        ELSE TRUE

StaleOwnerCannotMutateOrReplay ==
    /\ ~(lastAttemptAccepted /\ lastAttemptWasStale)
    /\ ~(lastAttemptAccepted
         /\ lastAttemptWasReplay
         /\ lastAttemptWasStale)

ReplayIsObservational ==
    lastAttemptWasReplay => replayFingerprint = DurableFingerprint

ResponseLossLeavesCommitDurable ==
    lastEvent = "LEAF_COMMIT_RESPONSE_LOST" =>
        \E staging \in Stagings,
           requestBytes \in RequestBytes :
            <<staging, FileComponent, "Leaf", 0,
              RequestDigest(requestBytes), requestBytes>> \in pageCommits

ChangedRequestIsRejected ==
    lastEvent = "CHANGED_REQUEST_REJECT" => ~lastAttemptAccepted

SealOnlyAfterCompleteRootsParserAndMatch ==
    \A staging \in Stagings :
        IF stagingPhase[staging] \in {"SEALED", "REUSED"}
        THEN
            /\ AllComponentRootsComplete(staging)
            /\ AllCarriesFlushed(staging)
            /\ metadataParserComplete[staging]
            /\ matchComplete[staging]
            /\ matchObservation[staging] = finalObservation[staging]
            /\ finalObservation[staging] \in observationSealed
        ELSE TRUE

ReuseNeverSealsOrDeletesProvisionalObservation ==
    \A staging \in Stagings :
        IF stagingPhase[staging] = "REUSED"
        THEN
            /\ finalObservation[staging] # stagingObservation[staging]
            /\ finalObservation[staging] \in observationSealed
            /\ stagingObservation[staging] \notin observationSealed
            /\ reuseCleanupRemaining[staging]
                \subseteq StagingPageRecords(staging)
        ELSE TRUE

BoundedReuseCleanup ==
    lastEvent = "REUSE_PAGE_CLEANUP" => lastReuseCleanupRemoved = 1

CleanupNeverDeletesLiveStagingOrUpload ==
    /\ ~(lastCleanupAccepted /\ lastCleanupWasOpen)
    /\ (lastCleanupAccepted => lastCleanupWasMaintenance)
    /\ ~(lastUploadCleanupAccepted
         /\ lastUploadCleanupWasCurrent)
    /\ ~(lastIdentityCleanupAccepted
         /\ (lastIdentityCleanupHadUpload \/
             lastIdentityCleanupHadConsumer))
    /\ ~(lastConsumerCleanupAccepted
         /\ lastConsumerCleanupHadExternalBlocker)

CanonicalUploadClaimBlocksGc ==
    /\ \A upload \in canonicalUploads : upload[2] \in canonicalPresent
    /\ \A consumer \in canonicalConsumers :
        consumer[2] \in canonicalIdentityPresent
    /\ canonicalUploads \subseteq everCanonicalUploads
    /\ canonicalConsumers \subseteq everCanonicalUploads
    /\ \A generation \in OuterGenerations,
          value \in Values :
        Cardinality(
            {upload \in canonicalUploads :
                /\ upload[1] = generation
                /\ upload[2] = value}) <= 1
    /\ ~(lastValueGcAccepted /\ lastValueGcHadUpload)
    /\ (lastValueGcAccepted => lastValueGcWasMaintenance)

CanonicalUploadBootstrapAndMappingOrder ==
    /\ \A generation \in sourceMappedGenerations :
        \E sourceConsumer \in canonicalConsumers :
            /\ sourceConsumer[1] <= generation
            /\ sourceConsumer[2] = SourceRootValue
    /\ \A upload \in everCanonicalUploads :
        upload[2] \in DomainValues =>
            \E sourceUpload \in everCanonicalUploads :
                /\ sourceUpload[1] <= upload[1]
                /\ sourceUpload[2] = SourceRootValue

CanonicalUploadHandoffIsAtomic ==
    IF lastEvent = "UPLOAD_CONSUME_RELEASE"
    THEN
        /\ \E consumer \in canonicalConsumers :
            /\ consumer[1] = outerGeneration
            /\ consumer[2] \in canonicalIdentityPresent
            /\ consumer \notin canonicalUploads
    ELSE TRUE

StaleCanonicalUploadCleanupIsFenced ==
    IF lastEvent = "STALE_UPLOAD_CLEANUP"
    THEN
        /\ lastUploadCleanupAccepted
        /\ lastUploadCleanupWasMaintenance
        /\ ~lastUploadCleanupWasCurrent
        /\ lastUploadCleanupWasEligible
        /\ (lastUploadCleanupSourceMappingRetained <=>
                lastUploadCleanupGeneration \in sourceMappedGenerations)
        /\ lastUploadCleanupValue \in Values
    ELSE TRUE

StaleCanonicalUploadCleanupEnablesValueGc ==
    IF /\ lastEvent = "STALE_UPLOAD_CLEANUP"
       /\ lastUploadCleanupMadeGcEligible
    THEN ValueGcEligible(lastUploadCleanupValue)
    ELSE TRUE

OrphanCanonicalIdentityCleanupIsFenced ==
    IF lastEvent = "ORPHAN_IDENTITY_CLEANUP"
    THEN
        /\ lastIdentityCleanupAccepted
        /\ lastIdentityCleanupWasMaintenance
        /\ ~lastIdentityCleanupHadUpload
        /\ ~lastIdentityCleanupHadConsumer
        /\ lastIdentityCleanupValue \in Values
    ELSE TRUE

OrphanCanonicalIdentityCleanupEnablesValueGc ==
    IF /\ lastEvent = "ORPHAN_IDENTITY_CLEANUP"
       /\ lastIdentityCleanupMadeGcEligible
    THEN ValueGcEligible(lastIdentityCleanupValue)
    ELSE TRUE

CanonicalConsumerCleanupIsFenced ==
    IF lastEvent = "CONSUMER_CLEANUP"
    THEN
        /\ lastConsumerCleanupAccepted
        /\ lastConsumerCleanupWasMaintenance
        /\ ~lastConsumerCleanupHadExternalBlocker
        /\ ~lastConsumerCleanupWasCurrent
        /\ lastConsumerCleanupWasEligible
        /\ lastConsumerCleanupValue \in Values
    ELSE TRUE

ExternalRetentionBlockerPreventsCleanupAndGc ==
    /\ externalRetentionBlockers \subseteq canonicalConsumers
    /\ \A blocker \in externalRetentionBlockers :
        /\ blocker[2] \in canonicalIdentityPresent
        /\ blocker[2] \in canonicalPresent

SourceBuildMappingCleanupIsFenced ==
    IF lastEvent = "SOURCE_BUILD_MAPPING_CLEANUP"
    THEN
        /\ lastMappingCleanupAccepted
        /\ lastMappingCleanupWasMaintenance
        /\ ~lastMappingCleanupWasCurrent
        /\ ~lastMappingCleanupHadExternalBlocker
        /\ \A upload \in canonicalUploads :
            upload[1] # lastMappingCleanupGeneration
    ELSE IF lastEvent = "SOURCE_BUILD_MAPPING_CLEANUP_REJECT"
    THEN
        /\ ~lastMappingCleanupAccepted
        /\ lastMappingCleanupWasMaintenance
        /\ lastMappingCleanupHadExternalBlocker
    ELSE TRUE

Safety ==
    /\ TypeOK
    /\ ImmutableStagingAllocationAndHeader
    /\ ObservationAllocationIsMonotonicAndUnique
    /\ ExactPageSlotIdentity
    /\ ExactRequestBytesNotDigestAlone
    /\ ComponentRootsHaveMinimalHeight
    /\ StaleOwnerCannotMutateOrReplay
    /\ ReplayIsObservational
    /\ ResponseLossLeavesCommitDurable
    /\ ChangedRequestIsRejected
    /\ SealOnlyAfterCompleteRootsParserAndMatch
    /\ ReuseNeverSealsOrDeletesProvisionalObservation
    /\ BoundedReuseCleanup
    /\ CleanupNeverDeletesLiveStagingOrUpload
    /\ CanonicalUploadClaimBlocksGc
    /\ CanonicalUploadBootstrapAndMappingOrder
    /\ CanonicalUploadHandoffIsAtomic
    /\ StaleCanonicalUploadCleanupIsFenced
    /\ StaleCanonicalUploadCleanupEnablesValueGc
    /\ OrphanCanonicalIdentityCleanupIsFenced
    /\ OrphanCanonicalIdentityCleanupEnablesValueGc
    /\ CanonicalConsumerCleanupIsFenced
    /\ ExternalRetentionBlockerPreventsCleanupAndGc
    /\ SourceBuildMappingCleanupIsFenced

(***************************************************************************
SafetyView is a fingerprint quotient only for the exact invariant-only Small
profile.  Audit variables never enable an action or determine a durable
successor: they only record the latest fault/cleanup evidence.  Safe states
with the same process and durable variables therefore have congruent durable
successors.  Including Safety itself is essential: a state that violates any
configured Small invariant cannot be merged with a safe state having the same
durable fingerprint.

Do not use this view for GalleryStagingWitness, liveness, audit-event
reachability, or any future specification whose guards, durable updates, or
properties read audit history.  Such a specification must retain those audit
variables or extend the view with a congruent property-preserving projection.
***************************************************************************)

SafetyView == <<clock, DurableFingerprint, Safety>>

=============================================================================
