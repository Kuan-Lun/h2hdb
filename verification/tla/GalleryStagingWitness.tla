------------------------ MODULE GalleryStagingWitness ------------------------
EXTENDS GalleryStaging

(***************************************************************************
This is a deterministic bounded reachability witness, not a safety proof.
Weak fairness requires its only enabled phase transition eventually to run;
the companion liveness property reaches all child-first cleanup chains:

  * mapped domain value: claim -> seal -> consumer handoff -> consumer row ->
    identity -> allocation GC;
  * source-root bootstrap: claim before mapping -> seal -> atomic mapping and
    consumer handoff -> mapping -> consumer row -> identity -> allocation GC;
  * a sealed gallery staging compacts under the still-live lease; after
    takeover and every other mapping blocker is removed, a source-root
    external blocker alone rejects mapping cleanup; removing that blocker
    immediately enables the source mapping/value cleanup chain.

The exhaustive safety evidence remains GalleryStagingSmall.cfg.
***************************************************************************)

VARIABLE witnessPhase

WitnessOwner == CHOOSE owner \in Owners : TRUE
WitnessStaging == CHOOSE staging \in Stagings : TRUE
WitnessHeader == CHOOSE header \in Headers : TRUE
WitnessRequest == CHOOSE requestBytes \in CommitRequestBytes : TRUE
WitnessDomainValue == CHOOSE value \in DomainValues : TRUE
WitnessGeneration == 1

WitnessVars == <<vars, witnessPhase>>

WitnessInit ==
    /\ Init
    /\ witnessPhase = 0

AtPhase(phase, action) ==
    /\ witnessPhase = phase
    /\ action
    /\ witnessPhase' = phase + 1

WitnessNext ==
    \/ AtPhase(0, ClaimOuter(WitnessOwner))
    \/ AtPhase(1, AllocateAndClaimCanonicalValue(SourceRootValue))
    \/ AtPhase(2, SealCanonicalIdentity(SourceRootValue))
    \/ AtPhase(3, ConsumeAndReleaseCanonicalUpload(SourceRootValue))
    \/ AtPhase(4, BeginStaging(WitnessStaging, WitnessHeader))
    \/ AtPhase(5, AllocateObservation(WitnessStaging))
    \/ AtPhase(6,
        CommitLeafPage(
            WitnessStaging, FileComponent, WitnessRequest, FALSE))
    \/ AtPhase(7,
        ReuseOnlyChildAsRoot(WitnessStaging, FileComponent))
    \/ AtPhase(8,
        CommitLeafPage(
            WitnessStaging, TagComponent, WitnessRequest, FALSE))
    \/ AtPhase(9,
        ReuseOnlyChildAsRoot(WitnessStaging, TagComponent))
    \/ AtPhase(10,
        CommitLeafPage(
            WitnessStaging, DirectoryComponent, WitnessRequest, FALSE))
    \/ AtPhase(11,
        ReuseOnlyChildAsRoot(WitnessStaging, DirectoryComponent))
    \/ AtPhase(12,
        CommitLeafPage(
            WitnessStaging, MetadataComponent, WitnessRequest, FALSE))
    \/ AtPhase(13,
        ReuseOnlyChildAsRoot(WitnessStaging, MetadataComponent))
    \/ AtPhase(14, AdvanceMetadataParser(WitnessStaging))
    \/ AtPhase(15, CompleteNewMatch(WitnessStaging))
    \/ AtPhase(16, SealNewObservation(WitnessStaging))
    \/ AtPhase(17,
        AllocateAndClaimCanonicalValue(WitnessDomainValue))
    \/ AtPhase(18, SealCanonicalIdentity(WitnessDomainValue))
    \/ AtPhase(19,
        ConsumeAndReleaseCanonicalUpload(WitnessDomainValue))
    \/ AtPhase(20, CompleteCurrentGeneration)
    \/ AtPhase(21,
        AddExternalRetentionBlocker(
            <<WitnessGeneration, SourceRootValue>>))
    \/ AtPhase(22, EnterMaintenance)
    \/ AtPhase(23, AttemptStagingCleanup(WitnessStaging))
    \/ AtPhase(24, Tick)
    \/ AtPhase(25, ExitMaintenance)
    \/ AtPhase(26, TakeoverOuter(WitnessOwner))
    \/ AtPhase(27, EnterMaintenance)
    \/ AtPhase(28,
        CleanupCanonicalConsumer(
            <<WitnessGeneration, WitnessDomainValue>>))
    \/ AtPhase(29, CleanupOrphanCanonicalIdentity(WitnessDomainValue))
    \/ AtPhase(30, AttemptCanonicalValueGc(WitnessDomainValue))
    \* At this point gen1 is non-current, has no retained staging/upload/domain
    \* consumer, and is otherwise mapping-cleanup eligible.  The external
    \* source-root blocker is therefore the decisive rejection reason.
    \/ AtPhase(31,
        RejectExternallyBlockedSourceBuildMappingCleanup(WitnessGeneration))
    \/ AtPhase(32,
        RemoveExternalRetentionBlocker(
            <<WitnessGeneration, SourceRootValue>>))
    \/ AtPhase(33,
        CleanupSourceBuildGenerationMapping(WitnessGeneration))
    \/ AtPhase(34,
        CleanupCanonicalConsumer(
            <<WitnessGeneration, SourceRootValue>>))
    \/ AtPhase(35, CleanupOrphanCanonicalIdentity(SourceRootValue))
    \/ AtPhase(36, AttemptCanonicalValueGc(SourceRootValue))
    \/ /\ witnessPhase = 37
       /\ UNCHANGED WitnessVars

WitnessSpec ==
    /\ WitnessInit
    /\ [][WitnessNext]_WitnessVars
    /\ WF_WitnessVars(WitnessNext)

WitnessTargetReached == <> (witnessPhase = 37)

=============================================================================
