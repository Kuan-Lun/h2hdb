----------------------------- MODULE CatalogCore -----------------------------
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************
This is a provider-neutral model of the durable catalog lifecycle owned by
h2hdb core.  It deliberately says nothing about CBZ files, filesystem paths,
or a particular artifact provider.  The companion filesystem projection model
lives in h2hdb-ingest.

The model makes client generation tokens explicit.  A crash does not erase a
lease or any durable build state; after expiry, another owner can take over at
a new generation.  Publication is one atomic transition that records the
published revision and flips the database head together.
***************************************************************************)

CONSTANTS
    Owners,
    Builds,
    Artifacts,
    Revisions,
    NoOwner,
    NoBuild,
    NoArtifact,
    NoRevision,
    MaxTime,
    LeaseSpan,
    MaxGeneration

ASSUME /\ Owners # {}
       /\ Builds # {}
       /\ Artifacts # {}
       /\ Revisions # {}
       /\ NoOwner \notin Owners
       /\ NoBuild \notin Builds
       /\ NoArtifact \notin Artifacts
       /\ NoRevision \notin Revisions
       /\ MaxTime \in Nat
       /\ LeaseSpan \in Nat \ {0}
       /\ MaxGeneration \in Nat \ {0}

BuildPhases == {"Empty", "Staging", "Sealed", "Published", "Abandoned"}

Generations == 0..MaxGeneration

VARIABLES
    clock,
    runningOwners,
    leaseOwner,
    leaseGeneration,
    leaseExpiry,

    buildExists,
    buildPhase,
    stagedInput,
    sourceSealed,
    spamAnalysisDone,
    fullAnalysisDone,
    selectedArtifacts,
    preparedArtifacts,
    protectedByBuild,
    affectedOldArtifacts,
    rebuiltArtifacts,
    artifactPresent,

    publishedRevisions,
    revisionBuild,
    buildRevision,
    dbHead,
    activeBuild,
    activeArtifacts,

    lastMutationAccepted,
    lastMutationWasStale,
    lastMutationWasPublish,
    lastGcAccepted,
    lastGcWasActive,
    lastGcWasProtected

LeaseVariables ==
    <<clock, runningOwners, leaseOwner, leaseGeneration, leaseExpiry>>

BuildVariables ==
    <<buildExists, buildPhase, stagedInput, sourceSealed,
      spamAnalysisDone, fullAnalysisDone, selectedArtifacts,
      preparedArtifacts, protectedByBuild, affectedOldArtifacts,
      rebuiltArtifacts, artifactPresent>>

PublicationVariables ==
    <<publishedRevisions, revisionBuild, buildRevision, dbHead,
      activeBuild, activeArtifacts>>

MutationAuditVariables ==
    <<lastMutationAccepted, lastMutationWasStale, lastMutationWasPublish>>

GcAuditVariables == <<lastGcAccepted, lastGcWasActive, lastGcWasProtected>>

AuditVariables == <<MutationAuditVariables, GcAuditVariables>>

vars == <<LeaseVariables, BuildVariables, PublicationVariables, AuditVariables>>

ProtectedArtifacts ==
    UNION {protectedByBuild[b] : b \in buildExists}

LeaseIsLive ==
    /\ leaseOwner # NoOwner
    /\ leaseOwner \in runningOwners
    /\ clock < leaseExpiry

Authorized(owner, generation) ==
    /\ LeaseIsLive
    /\ owner = leaseOwner
    /\ generation = leaseGeneration

AcceptedAttempt(kind, owner, generation) ==
    /\ lastMutationAccepted' = TRUE
    /\ lastMutationWasStale' = ~Authorized(owner, generation)
    /\ lastMutationWasPublish' = (kind = "Publish")

BuildReadyToPublish(build) ==
    /\ build \in buildExists
    /\ buildPhase[build] = "Sealed"
    /\ sourceSealed[build]
    /\ spamAnalysisDone[build]
    /\ fullAnalysisDone[build]
    /\ selectedArtifacts[build] \subseteq preparedArtifacts[build]
    /\ selectedArtifacts[build] \subseteq protectedByBuild[build]
    /\ affectedOldArtifacts[build] \subseteq rebuiltArtifacts[build]
    /\ affectedOldArtifacts[build] \subseteq protectedByBuild[build]

Init ==
    /\ clock = 0
    /\ runningOwners = Owners
    /\ leaseOwner = NoOwner
    /\ leaseGeneration = 0
    /\ leaseExpiry = 0

    /\ buildExists = {}
    /\ buildPhase = [b \in Builds |-> "Empty"]
    /\ stagedInput = [b \in Builds |-> FALSE]
    /\ sourceSealed = [b \in Builds |-> FALSE]
    /\ spamAnalysisDone = [b \in Builds |-> FALSE]
    /\ fullAnalysisDone = [b \in Builds |-> FALSE]
    /\ selectedArtifacts = [b \in Builds |-> {}]
    /\ preparedArtifacts = [b \in Builds |-> {}]
    /\ protectedByBuild = [b \in Builds |-> {}]
    /\ affectedOldArtifacts = [b \in Builds |-> {}]
    /\ rebuiltArtifacts = [b \in Builds |-> {}]
    /\ artifactPresent = {}

    /\ publishedRevisions = {}
    /\ revisionBuild = [r \in Revisions |-> NoBuild]
    /\ buildRevision = [b \in Builds |-> NoRevision]
    /\ dbHead = NoRevision
    /\ activeBuild = NoBuild
    /\ activeArtifacts = {}

    /\ lastMutationAccepted = FALSE
    /\ lastMutationWasStale = FALSE
    /\ lastMutationWasPublish = FALSE
    /\ lastGcAccepted = FALSE
    /\ lastGcWasActive = FALSE
    /\ lastGcWasProtected = FALSE

(***************************************************************************
Lease and process lifecycle.  Crash only changes process availability; all
database state, including the old lease generation, remains durable.
***************************************************************************)

Tick ==
    /\ clock < MaxTime
    /\ clock' = clock + 1
    /\ UNCHANGED <<runningOwners, leaseOwner, leaseGeneration, leaseExpiry>>
    /\ UNCHANGED BuildVariables
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED AuditVariables

Crash(owner) ==
    /\ owner \in runningOwners
    /\ runningOwners' = runningOwners \ {owner}
    /\ UNCHANGED <<clock, leaseOwner, leaseGeneration, leaseExpiry>>
    /\ UNCHANGED BuildVariables
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED AuditVariables

Restart(owner) ==
    /\ owner \in Owners \ runningOwners
    /\ runningOwners' = runningOwners \cup {owner}
    /\ UNCHANGED <<clock, leaseOwner, leaseGeneration, leaseExpiry>>
    /\ UNCHANGED BuildVariables
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED AuditVariables

Claim(owner) ==
    /\ owner \in runningOwners
    /\ leaseOwner = NoOwner
    /\ leaseGeneration < MaxGeneration
    /\ leaseOwner' = owner
    /\ leaseGeneration' = leaseGeneration + 1
    /\ leaseExpiry' = clock + LeaseSpan
    /\ UNCHANGED <<clock, runningOwners>>
    /\ UNCHANGED BuildVariables
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED AuditVariables

Renew(owner, generation) ==
    /\ Authorized(owner, generation)
    /\ leaseExpiry' = clock + LeaseSpan
    /\ AcceptedAttempt("Renew", owner, generation)
    /\ UNCHANGED <<clock, runningOwners, leaseOwner, leaseGeneration>>
    /\ UNCHANGED BuildVariables
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED GcAuditVariables

ExpireLease ==
    /\ leaseOwner # NoOwner
    /\ clock >= leaseExpiry
    /\ leaseOwner' = NoOwner
    /\ leaseExpiry' = clock
    /\ UNCHANGED <<clock, runningOwners, leaseGeneration>>
    /\ UNCHANGED BuildVariables
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED AuditVariables

Takeover(owner) ==
    /\ owner \in runningOwners
    /\ leaseOwner # NoOwner
    /\ owner # leaseOwner
    /\ clock >= leaseExpiry
    /\ leaseGeneration < MaxGeneration
    /\ leaseOwner' = owner
    /\ leaseGeneration' = leaseGeneration + 1
    /\ leaseExpiry' = clock + LeaseSpan
    /\ UNCHANGED <<clock, runningOwners>>
    /\ UNCHANGED BuildVariables
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED AuditVariables

(***************************************************************************
A stale caller may attempt any fenced mutation.  Rejection is observable in
the audit record, but no durable lifecycle state changes.
***************************************************************************)

RejectStaleMutation(isPublish, owner, generation) ==
    /\ isPublish \in BOOLEAN
    /\ owner \in Owners
    /\ generation \in Generations
    /\ ~Authorized(owner, generation)
    /\ lastMutationAccepted' = FALSE
    /\ lastMutationWasStale' = TRUE
    /\ lastMutationWasPublish' = isPublish
    /\ UNCHANGED LeaseVariables
    /\ UNCHANGED BuildVariables
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED GcAuditVariables

(***************************************************************************
Build staging, immutable seal, ordered spam/full analysis, and provider-neutral
artifact preparation/protection.
***************************************************************************)

BeginBuild(build, owner, generation) ==
    /\ Authorized(owner, generation)
    /\ build \in Builds \ buildExists
    /\ buildExists' = buildExists \cup {build}
    /\ buildPhase' = [buildPhase EXCEPT ![build] = "Staging"]
    /\ stagedInput' = [stagedInput EXCEPT ![build] = FALSE]
    /\ sourceSealed' = [sourceSealed EXCEPT ![build] = FALSE]
    /\ spamAnalysisDone' = [spamAnalysisDone EXCEPT ![build] = FALSE]
    /\ fullAnalysisDone' = [fullAnalysisDone EXCEPT ![build] = FALSE]
    /\ selectedArtifacts' = [selectedArtifacts EXCEPT ![build] = {}]
    /\ preparedArtifacts' = [preparedArtifacts EXCEPT ![build] = {}]
    /\ protectedByBuild' = [protectedByBuild EXCEPT ![build] = {}]
    /\ affectedOldArtifacts' = [affectedOldArtifacts EXCEPT ![build] = {}]
    /\ rebuiltArtifacts' = [rebuiltArtifacts EXCEPT ![build] = {}]
    /\ AcceptedAttempt("Begin", owner, generation)
    /\ UNCHANGED LeaseVariables
    /\ UNCHANGED artifactPresent
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED GcAuditVariables

StageInput(build, owner, generation) ==
    /\ Authorized(owner, generation)
    /\ build \in buildExists
    /\ buildPhase[build] = "Staging"
    /\ ~sourceSealed[build]
    /\ ~stagedInput[build]
    /\ stagedInput' = [stagedInput EXCEPT ![build] = TRUE]
    /\ AcceptedAttempt("Stage", owner, generation)
    /\ UNCHANGED LeaseVariables
    /\ UNCHANGED
        <<buildExists, buildPhase, sourceSealed, spamAnalysisDone,
          fullAnalysisDone, selectedArtifacts, preparedArtifacts,
          protectedByBuild, affectedOldArtifacts, rebuiltArtifacts,
          artifactPresent>>
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED GcAuditVariables

\* A newly staged gallery changes a prior deduplication/ownership decision.
\* The displaced old artifact becomes both selected and explicitly affected;
\* publication will remain disabled until it is rebuilt and protected.
StageNewGalleryChangedDecision(build, oldArtifact, owner, generation) ==
    /\ Authorized(owner, generation)
    /\ build \in buildExists
    /\ oldArtifact \in Artifacts
    /\ buildPhase[build] = "Staging"
    /\ ~sourceSealed[build]
    /\ oldArtifact \notin affectedOldArtifacts[build]
    /\ stagedInput' = [stagedInput EXCEPT ![build] = TRUE]
    /\ selectedArtifacts' =
        [selectedArtifacts EXCEPT ![build] = @ \cup {oldArtifact}]
    /\ affectedOldArtifacts' =
        [affectedOldArtifacts EXCEPT ![build] = @ \cup {oldArtifact}]
    /\ AcceptedAttempt("Stage", owner, generation)
    /\ UNCHANGED LeaseVariables
    /\ UNCHANGED
        <<buildExists, buildPhase, sourceSealed, spamAnalysisDone,
          fullAnalysisDone, preparedArtifacts, protectedByBuild,
          rebuiltArtifacts, artifactPresent>>
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED GcAuditVariables

SealBuild(build, owner, generation) ==
    /\ Authorized(owner, generation)
    /\ build \in buildExists
    /\ buildPhase[build] = "Staging"
    /\ stagedInput[build]
    /\ buildPhase' = [buildPhase EXCEPT ![build] = "Sealed"]
    /\ sourceSealed' = [sourceSealed EXCEPT ![build] = TRUE]
    /\ AcceptedAttempt("Seal", owner, generation)
    /\ UNCHANGED LeaseVariables
    /\ UNCHANGED
        <<buildExists, stagedInput, spamAnalysisDone, fullAnalysisDone,
          selectedArtifacts, preparedArtifacts, protectedByBuild,
          affectedOldArtifacts, rebuiltArtifacts, artifactPresent>>
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED GcAuditVariables

RunSpamAnalysis(build, owner, generation) ==
    /\ Authorized(owner, generation)
    /\ build \in buildExists
    /\ buildPhase[build] = "Sealed"
    /\ sourceSealed[build]
    /\ ~spamAnalysisDone[build]
    /\ spamAnalysisDone' = [spamAnalysisDone EXCEPT ![build] = TRUE]
    /\ AcceptedAttempt("SpamAnalysis", owner, generation)
    /\ UNCHANGED LeaseVariables
    /\ UNCHANGED
        <<buildExists, buildPhase, stagedInput, sourceSealed,
          fullAnalysisDone, selectedArtifacts, preparedArtifacts,
          protectedByBuild, affectedOldArtifacts, rebuiltArtifacts,
          artifactPresent>>
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED GcAuditVariables

RunFullAnalysis(build, owner, generation) ==
    /\ Authorized(owner, generation)
    /\ build \in buildExists
    /\ buildPhase[build] = "Sealed"
    /\ spamAnalysisDone[build]
    /\ ~fullAnalysisDone[build]
    /\ fullAnalysisDone' = [fullAnalysisDone EXCEPT ![build] = TRUE]
    /\ AcceptedAttempt("FullAnalysis", owner, generation)
    /\ UNCHANGED LeaseVariables
    /\ UNCHANGED
        <<buildExists, buildPhase, stagedInput, sourceSealed,
          spamAnalysisDone, selectedArtifacts, preparedArtifacts,
          protectedByBuild, affectedOldArtifacts, rebuiltArtifacts,
          artifactPresent>>
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED GcAuditVariables

SelectArtifact(build, artifact, owner, generation) ==
    /\ Authorized(owner, generation)
    /\ build \in buildExists
    /\ artifact \in Artifacts
    /\ buildPhase[build] = "Sealed"
    /\ fullAnalysisDone[build]
    /\ artifact \notin selectedArtifacts[build]
    /\ selectedArtifacts' =
        [selectedArtifacts EXCEPT ![build] = @ \cup {artifact}]
    /\ AcceptedAttempt("SelectArtifact", owner, generation)
    /\ UNCHANGED LeaseVariables
    /\ UNCHANGED
        <<buildExists, buildPhase, stagedInput, sourceSealed,
          spamAnalysisDone, fullAnalysisDone, preparedArtifacts,
          protectedByBuild, affectedOldArtifacts, rebuiltArtifacts,
          artifactPresent>>
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED GcAuditVariables

PrepareArtifact(build, artifact, owner, generation) ==
    /\ Authorized(owner, generation)
    /\ build \in buildExists
    /\ artifact \in selectedArtifacts[build]
    /\ fullAnalysisDone[build]
    /\ artifact \notin preparedArtifacts[build]
    /\ preparedArtifacts' =
        [preparedArtifacts EXCEPT ![build] = @ \cup {artifact}]
    /\ artifactPresent' = artifactPresent \cup {artifact}
    /\ AcceptedAttempt("PrepareArtifact", owner, generation)
    /\ UNCHANGED LeaseVariables
    /\ UNCHANGED
        <<buildExists, buildPhase, stagedInput, sourceSealed,
          spamAnalysisDone, fullAnalysisDone, selectedArtifacts,
          protectedByBuild, affectedOldArtifacts, rebuiltArtifacts>>
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED GcAuditVariables

RebuildAffectedArtifact(build, artifact, owner, generation) ==
    /\ Authorized(owner, generation)
    /\ build \in buildExists
    /\ artifact \in affectedOldArtifacts[build]
    /\ fullAnalysisDone[build]
    /\ artifact \notin rebuiltArtifacts[build]
    /\ preparedArtifacts' =
        [preparedArtifacts EXCEPT ![build] = @ \cup {artifact}]
    /\ rebuiltArtifacts' =
        [rebuiltArtifacts EXCEPT ![build] = @ \cup {artifact}]
    /\ artifactPresent' = artifactPresent \cup {artifact}
    /\ AcceptedAttempt("RebuildArtifact", owner, generation)
    /\ UNCHANGED LeaseVariables
    /\ UNCHANGED
        <<buildExists, buildPhase, stagedInput, sourceSealed,
          spamAnalysisDone, fullAnalysisDone, selectedArtifacts,
          protectedByBuild, affectedOldArtifacts>>
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED GcAuditVariables

ProtectArtifact(build, artifact, owner, generation) ==
    /\ Authorized(owner, generation)
    /\ build \in buildExists
    /\ artifact \in preparedArtifacts[build]
    /\ artifact \in artifactPresent
    /\ artifact \notin protectedByBuild[build]
    /\ protectedByBuild' =
        [protectedByBuild EXCEPT ![build] = @ \cup {artifact}]
    /\ AcceptedAttempt("ProtectArtifact", owner, generation)
    /\ UNCHANGED LeaseVariables
    /\ UNCHANGED
        <<buildExists, buildPhase, stagedInput, sourceSealed,
          spamAnalysisDone, fullAnalysisDone, selectedArtifacts,
          preparedArtifacts, affectedOldArtifacts, rebuiltArtifacts,
          artifactPresent>>
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED GcAuditVariables

(***************************************************************************
Publication and garbage collection.  PublishCatalogBuild is deliberately one
TLA+ step: there is no state in which a published revision and dbHead disagree.
***************************************************************************)

PublishCatalogBuild(build, revision, owner, generation) ==
    /\ Authorized(owner, generation)
    /\ BuildReadyToPublish(build)
    /\ revision \in Revisions \ publishedRevisions
    /\ revisionBuild[revision] = NoBuild
    /\ buildRevision[build] = NoRevision
    /\ buildPhase' = [buildPhase EXCEPT ![build] = "Published"]
    /\ publishedRevisions' = publishedRevisions \cup {revision}
    /\ revisionBuild' = [revisionBuild EXCEPT ![revision] = build]
    /\ buildRevision' = [buildRevision EXCEPT ![build] = revision]
    /\ dbHead' = revision
    /\ activeBuild' = build
    /\ activeArtifacts' = selectedArtifacts[build]
    /\ AcceptedAttempt("Publish", owner, generation)
    /\ UNCHANGED LeaseVariables
    /\ UNCHANGED
        <<buildExists, stagedInput, sourceSealed, spamAnalysisDone,
          fullAnalysisDone, selectedArtifacts, preparedArtifacts,
          protectedByBuild, affectedOldArtifacts, rebuiltArtifacts,
          artifactPresent>>
    /\ UNCHANGED GcAuditVariables

AbandonBuild(build, owner, generation) ==
    /\ Authorized(owner, generation)
    /\ build \in buildExists
    /\ build # activeBuild
    /\ buildPhase[build] \notin {"Published", "Abandoned"}
    /\ buildPhase' = [buildPhase EXCEPT ![build] = "Abandoned"]
    /\ AcceptedAttempt("Abandon", owner, generation)
    /\ UNCHANGED LeaseVariables
    /\ UNCHANGED
        <<buildExists, stagedInput, sourceSealed, spamAnalysisDone,
          fullAnalysisDone, selectedArtifacts, preparedArtifacts,
          protectedByBuild, affectedOldArtifacts, rebuiltArtifacts,
          artifactPresent>>
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED GcAuditVariables

ReleaseAbandonedProtection(build, owner, generation) ==
    /\ Authorized(owner, generation)
    /\ build \in buildExists
    /\ buildPhase[build] = "Abandoned"
    /\ protectedByBuild[build] # {}
    /\ protectedByBuild' = [protectedByBuild EXCEPT ![build] = {}]
    /\ AcceptedAttempt("ReleaseProtection", owner, generation)
    /\ UNCHANGED LeaseVariables
    /\ UNCHANGED
        <<buildExists, buildPhase, stagedInput, sourceSealed,
          spamAnalysisDone, fullAnalysisDone, selectedArtifacts,
          preparedArtifacts, affectedOldArtifacts, rebuiltArtifacts,
          artifactPresent>>
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED GcAuditVariables

GarbageCollectBuild(build, owner, generation) ==
    /\ Authorized(owner, generation)
    /\ build \in buildExists
    /\ buildPhase[build] = "Abandoned"
    /\ protectedByBuild[build] = {}
    /\ buildRevision[build] = NoRevision
    /\ build # activeBuild
    /\ buildExists' = buildExists \ {build}
    /\ AcceptedAttempt("GarbageCollectBuild", owner, generation)
    /\ UNCHANGED LeaseVariables
    /\ UNCHANGED
        <<buildPhase, stagedInput, sourceSealed, spamAnalysisDone,
          fullAnalysisDone, selectedArtifacts, preparedArtifacts,
          protectedByBuild, affectedOldArtifacts, rebuiltArtifacts,
          artifactPresent>>
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED GcAuditVariables

AttemptArtifactGc(artifact) ==
    LET wasActive == artifact \in activeArtifacts
        wasProtected == artifact \in ProtectedArtifacts
        accepted ==
            /\ artifact \in artifactPresent
            /\ ~wasActive
            /\ ~wasProtected
    IN
    /\ artifact \in Artifacts
    /\ lastGcAccepted' = accepted
    /\ lastGcWasActive' = wasActive
    /\ lastGcWasProtected' = wasProtected
    /\ artifactPresent' =
        IF accepted THEN artifactPresent \ {artifact} ELSE artifactPresent
    /\ UNCHANGED LeaseVariables
    /\ UNCHANGED
        <<buildExists, buildPhase, stagedInput, sourceSealed,
          spamAnalysisDone, fullAnalysisDone, selectedArtifacts,
          preparedArtifacts, protectedByBuild, affectedOldArtifacts,
          rebuiltArtifacts>>
    /\ UNCHANGED PublicationVariables
    /\ UNCHANGED MutationAuditVariables

Next ==
    \/ Tick
    \/ ExpireLease
    \/ \E owner \in Owners : Crash(owner)
    \/ \E owner \in Owners : Restart(owner)
    \/ \E owner \in Owners : Claim(owner)
    \/ \E owner \in Owners : Takeover(owner)
    \/ \E owner \in Owners, generation \in Generations :
        Renew(owner, generation)
    \/ \E isPublish \in BOOLEAN,
          owner \in Owners,
          generation \in Generations :
        RejectStaleMutation(isPublish, owner, generation)
    \/ \E build \in Builds,
          owner \in Owners,
          generation \in Generations :
        BeginBuild(build, owner, generation)
    \/ \E build \in Builds,
          owner \in Owners,
          generation \in Generations :
        StageInput(build, owner, generation)
    \/ \E build \in Builds,
          artifact \in Artifacts,
          owner \in Owners,
          generation \in Generations :
        StageNewGalleryChangedDecision(build, artifact, owner, generation)
    \/ \E build \in Builds,
          owner \in Owners,
          generation \in Generations :
        SealBuild(build, owner, generation)
    \/ \E build \in Builds,
          owner \in Owners,
          generation \in Generations :
        RunSpamAnalysis(build, owner, generation)
    \/ \E build \in Builds,
          owner \in Owners,
          generation \in Generations :
        RunFullAnalysis(build, owner, generation)
    \/ \E build \in Builds,
          artifact \in Artifacts,
          owner \in Owners,
          generation \in Generations :
        SelectArtifact(build, artifact, owner, generation)
    \/ \E build \in Builds,
          artifact \in Artifacts,
          owner \in Owners,
          generation \in Generations :
        PrepareArtifact(build, artifact, owner, generation)
    \/ \E build \in Builds,
          artifact \in Artifacts,
          owner \in Owners,
          generation \in Generations :
        RebuildAffectedArtifact(build, artifact, owner, generation)
    \/ \E build \in Builds,
          artifact \in Artifacts,
          owner \in Owners,
          generation \in Generations :
        ProtectArtifact(build, artifact, owner, generation)
    \/ \E build \in Builds,
          revision \in Revisions,
          owner \in Owners,
          generation \in Generations :
        PublishCatalogBuild(build, revision, owner, generation)
    \/ \E build \in Builds,
          owner \in Owners,
          generation \in Generations :
        AbandonBuild(build, owner, generation)
    \/ \E build \in Builds,
          owner \in Owners,
          generation \in Generations :
        ReleaseAbandonedProtection(build, owner, generation)
    \/ \E build \in Builds,
          owner \in Owners,
          generation \in Generations :
        GarbageCollectBuild(build, owner, generation)
    \/ \E artifact \in Artifacts : AttemptArtifactGc(artifact)

Spec == Init /\ [][Next]_vars

(***************************************************************************
Safety properties checked by CatalogCoreSmall.cfg.
***************************************************************************)

TypeOK ==
    /\ clock \in 0..MaxTime
    /\ runningOwners \subseteq Owners
    /\ leaseOwner \in Owners \cup {NoOwner}
    /\ leaseGeneration \in Generations
    /\ leaseExpiry \in 0..(MaxTime + LeaseSpan)
    /\ buildExists \subseteq Builds
    /\ buildPhase \in [Builds -> BuildPhases]
    /\ stagedInput \in [Builds -> BOOLEAN]
    /\ sourceSealed \in [Builds -> BOOLEAN]
    /\ spamAnalysisDone \in [Builds -> BOOLEAN]
    /\ fullAnalysisDone \in [Builds -> BOOLEAN]
    /\ selectedArtifacts \in [Builds -> SUBSET Artifacts]
    /\ preparedArtifacts \in [Builds -> SUBSET Artifacts]
    /\ protectedByBuild \in [Builds -> SUBSET Artifacts]
    /\ affectedOldArtifacts \in [Builds -> SUBSET Artifacts]
    /\ rebuiltArtifacts \in [Builds -> SUBSET Artifacts]
    /\ artifactPresent \subseteq Artifacts
    /\ publishedRevisions \subseteq Revisions
    /\ revisionBuild \in [Revisions -> Builds \cup {NoBuild}]
    /\ buildRevision \in [Builds -> Revisions \cup {NoRevision}]
    /\ dbHead \in Revisions \cup {NoRevision}
    /\ activeBuild \in Builds \cup {NoBuild}
    /\ activeArtifacts \subseteq Artifacts
    /\ lastMutationAccepted \in BOOLEAN
    /\ lastMutationWasStale \in BOOLEAN
    /\ lastMutationWasPublish \in BOOLEAN
    /\ lastGcAccepted \in BOOLEAN
    /\ lastGcWasActive \in BOOLEAN
    /\ lastGcWasProtected \in BOOLEAN

StaleGenerationCannotMutate ==
    ~(lastMutationAccepted /\ lastMutationWasStale)

StaleGenerationCannotPublish ==
    ~(lastMutationWasPublish
      /\ lastMutationAccepted
      /\ lastMutationWasStale)

HeadPointsOnlyToSealedAndAnalyzedBuild ==
    IF dbHead = NoRevision
    THEN TRUE
    ELSE
        /\ activeBuild \in buildExists
        /\ sourceSealed[activeBuild]
        /\ spamAnalysisDone[activeBuild]
        /\ fullAnalysisDone[activeBuild]
        /\ buildPhase[activeBuild] = "Published"

ActiveAndProtectedArtifactsCannotBeGc ==
    /\ activeArtifacts \subseteq artifactPresent
    /\ ProtectedArtifacts \subseteq artifactPresent
    /\ ~lastGcAccepted
       \/ /\ ~lastGcWasActive
          /\ ~lastGcWasProtected

DatabaseHeadMatchesPublishedRevision ==
    /\ ((dbHead = NoRevision) = (activeBuild = NoBuild))
    /\ IF dbHead = NoRevision
       THEN publishedRevisions = {}
       ELSE
           /\ dbHead \in publishedRevisions
           /\ revisionBuild[dbHead] = activeBuild
           /\ buildRevision[activeBuild] = dbHead
    /\ \A revision \in publishedRevisions :
        IF revisionBuild[revision] \in buildExists
        THEN
            /\ buildRevision[revisionBuild[revision]] = revision
            /\ buildPhase[revisionBuild[revision]] = "Published"
        ELSE FALSE

PublishedArtifactsWerePreparedAndProtected ==
    /\ activeArtifacts \subseteq ProtectedArtifacts
    /\ \A build \in buildExists :
        IF buildPhase[build] = "Published"
        THEN
            /\ selectedArtifacts[build] \subseteq preparedArtifacts[build]
            /\ selectedArtifacts[build] \subseteq protectedByBuild[build]
        ELSE TRUE

ChangedDecisionArtifactsReadyBeforePublish ==
    \A build \in buildExists :
        IF buildPhase[build] = "Published"
        THEN
            /\ affectedOldArtifacts[build] \subseteq rebuiltArtifacts[build]
            /\ affectedOldArtifacts[build] \subseteq protectedByBuild[build]
        ELSE TRUE

Safety ==
    /\ TypeOK
    /\ StaleGenerationCannotMutate
    /\ StaleGenerationCannotPublish
    /\ HeadPointsOnlyToSealedAndAnalyzedBuild
    /\ ActiveAndProtectedArtifactsCannotBeGc
    /\ DatabaseHeadMatchesPublishedRevision
    /\ PublishedArtifactsWerePreparedAndProtected
    /\ ChangedDecisionArtifactsReadyBeforePublish

=============================================================================
