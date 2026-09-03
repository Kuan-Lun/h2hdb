----------------------------- MODULE PolicyTakeover -----------------------------
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************
Finite crash/retry model of complete ingest-policy convergence at takeover.

Each Policies atom denotes the complete resolved ingest policy tuple:
manifest, analysis, artifact, display-title, title-sort and operational policy
identities plus artifacts-required.  Equality in this model is therefore
exact tuple equality, not analysis-policy equality alone.

Each build carries the complete policy of its sole analysis/publication path,
its analysis state, and its publication commit state.  A crash takeover claims
a fresh ingest generation and may request any complete policy.  If the prior
publication is already DB_COMMITTED, receipt-scoped recovery finalizes it
before the new session may hand off (and therefore observe) a source.  Recovery
does not create a generation-to-build mapping.  The same generation then maps
the current source: it reuses the PUBLISHED head only when the complete policy
matches, otherwise it creates a successor under the requested policy.

Consequently a successful synchronization return is never a hidden deferral:
the current PUBLISHED head has the exact requested policy.  Crashes may delay
that return, and may leave an older immutable publication to recover first,
but do not change the meaning of success.

TLC exhausts only the configured policy atoms and generation/build bounds.
The model abstracts receipt recovery and source/publication transactions as
atomic actions.  It does not establish SQL, adapter, filesystem, drainage or
cleanup refinement; the production recovery checks and crash matrix are the
separate executable evidence.
***************************************************************************)

CONSTANTS Policies, MaxGeneration, MaxBuilds

ASSUME /\ Cardinality(Policies) >= 2
       /\ MaxGeneration \in Nat \ {0}
       /\ MaxBuilds \in Nat \ {0}

NoPolicy == 0
ASSUME NoPolicy \notin Policies

AnalysisStates == {"NONE", "OPEN", "COMPLETE", "ABANDONED"}
CommitStates == {"NONE", "DB_COMMITTED", "PUBLISHED"}

VARIABLES
    generation,    \* the live ingest generation
    requested,     \* the live session's complete requested policy
    builds,        \* [1..count -> [policy, analysis, commit]]
    count,         \* number of builds ever created
    working,       \* the sole source working build (0 = none)
    mapping,       \* generation -> build (0 = unmapped)
    head,          \* the current PUBLISHED head build (0 = none)
    recoveryOwed,  \* this session inherited a DB_COMMITTED publication
    returned       \* this synchronization returned success

vars ==
    <<generation, requested, builds, count, working, mapping, head,
      recoveryOwed, returned>>

Build(b) == builds[b]

HeadPolicy == IF head = 0 THEN NoPolicy ELSE Build(head).policy

PendingBuilds ==
    {b \in 1..count : Build(b).commit = "DB_COMMITTED"}

NoPending == PendingBuilds = {}

Init ==
    /\ generation = 1
    /\ requested \in Policies
    /\ builds = <<>>
    /\ count = 0
    /\ working = 0
    /\ mapping = [g \in 1..MaxGeneration |-> 0]
    /\ head = 0
    /\ recoveryOwed = FALSE
    /\ returned = FALSE

NewBuild(policy) ==
    [policy |-> policy, analysis |-> "NONE", commit |-> "NONE"]

Append(record) ==
    [b \in 1..(count + 1) |-> IF b = count + 1 THEN record ELSE Build(b)]

DurableCommit(b) == Build(b).commit \in {"DB_COMMITTED", "PUBLISHED"}

ForeignPolicy(b) == Build(b).policy \notin {NoPolicy, requested}

Mapped == mapping[generation]

\* A crash/restart claims a fresh generation and carries no source mapping.
\* If the crash inherited a durable unfinalized publication, this session
\* owes receipt-scoped recovery before it is allowed to hand off a source.
Takeover(policy) ==
    /\ ~returned
    /\ generation < MaxGeneration
    /\ generation' = generation + 1
    /\ requested' = policy
    /\ recoveryOwed' = ~NoPending
    /\ returned' = FALSE
    /\ UNCHANGED <<builds, count, working, mapping, head>>

\* A later synchronization call after a successful one keeps the requested
\* policy.  A successful return cannot leave DB_COMMITTED work, so no recovery
\* debt is carried into this fresh generation.
NextSync ==
    /\ returned
    /\ generation < MaxGeneration
    /\ generation' = generation + 1
    /\ recoveryOwed' = FALSE
    /\ returned' = FALSE
    /\ UNCHANGED <<requested, builds, count, working, mapping, head>>

\* Finalize the inherited immutable commit without binding this session's
\* generation to its old build.  The source working root belongs to the old
\* receipt and is released by finalization.
RecoverPending ==
    /\ recoveryOwed
    /\ mapping[generation] = 0
    /\ Cardinality(PendingBuilds) = 1
    /\ LET b == CHOOSE candidate \in PendingBuilds : TRUE
       IN  /\ builds' = [builds EXCEPT ![b].commit = "PUBLISHED"]
           /\ head' = b
    /\ working' = 0
    /\ recoveryOwed' = FALSE
    /\ UNCHANGED <<generation, requested, count, mapping, returned>>

\* Retire a foreign-policy working build and create its successor in the same
\* transaction.  OPEN analysis becomes ABANDONED; other immutable terminal
\* facts remain unchanged.  A PUBLISHED head remains the head while its source
\* working slot is replaced.
Retire(b) ==
    /\ count < MaxBuilds
    /\ builds' =
        [Append(NewBuild(NoPolicy)) EXCEPT
            ![b].analysis =
                IF Build(b).analysis = "OPEN"
                THEN "ABANDONED"
                ELSE Build(b).analysis]
    /\ count' = count + 1
    /\ working' = count + 1
    /\ mapping' = [mapping EXCEPT ![generation] = count + 1]
    /\ UNCHANGED
        <<generation, requested, head, recoveryOwed, returned>>

\* Source handoff is forbidden while an inherited receipt is unrecovered or
\* any DB_COMMITTED publication exists.  Reuse requires exact complete-policy
\* equality; otherwise this generation receives a successor build.
Handoff ==
    /\ ~returned
    /\ ~recoveryOwed
    /\ NoPending
    /\ mapping[generation] = 0
    /\ \/ /\ working = 0
          /\ \/ /\ head # 0
                /\ HeadPolicy = requested
                /\ mapping' = [mapping EXCEPT ![generation] = head]
                /\ working' = head
                /\ UNCHANGED <<builds, count>>
             \/ /\ (head = 0) \/ (HeadPolicy # requested)
                /\ count < MaxBuilds
                /\ builds' = Append(NewBuild(NoPolicy))
                /\ count' = count + 1
                /\ working' = count + 1
                /\ mapping' = [mapping EXCEPT ![generation] = count + 1]
       \/ /\ working # 0
          /\ ~ForeignPolicy(working)
          /\ mapping' = [mapping EXCEPT ![generation] = working]
          /\ UNCHANGED <<builds, count, working>>
       \/ /\ working # 0
          /\ ForeignPolicy(working)
          /\ Retire(working)
    /\ UNCHANGED <<generation, requested, head, recoveryOwed, returned>>

BeginAnalysis ==
    /\ ~returned
    /\ Mapped # 0
    /\ working = Mapped
    /\ Build(Mapped).analysis = "NONE"
    /\ builds' =
        [builds EXCEPT
            ![Mapped].analysis = "OPEN",
            ![Mapped].policy = requested]
    /\ UNCHANGED
        <<generation, requested, count, working, mapping, head,
          recoveryOwed, returned>>

CompleteAnalysis ==
    /\ ~returned
    /\ Mapped # 0
    /\ working = Mapped
    /\ Build(Mapped).analysis = "OPEN"
    /\ Build(Mapped).policy = requested
    /\ builds' = [builds EXCEPT ![Mapped].analysis = "COMPLETE"]
    /\ UNCHANGED
        <<generation, requested, count, working, mapping, head,
          recoveryOwed, returned>>

CommitPublication ==
    /\ ~returned
    /\ Mapped # 0
    /\ working = Mapped
    /\ Build(Mapped).analysis = "COMPLETE"
    /\ Build(Mapped).commit = "NONE"
    /\ Build(Mapped).policy = requested
    /\ builds' = [builds EXCEPT ![Mapped].commit = "DB_COMMITTED"]
    /\ UNCHANGED
        <<generation, requested, count, working, mapping, head,
          recoveryOwed, returned>>

\* Ordinary same-session finalization of the build mapped by this generation.
FinalizeMapped ==
    /\ ~returned
    /\ Mapped # 0
    /\ working = Mapped
    /\ Build(Mapped).commit = "DB_COMMITTED"
    /\ Build(Mapped).policy = requested
    /\ builds' = [builds EXCEPT ![Mapped].commit = "PUBLISHED"]
    /\ head' = Mapped
    /\ working' = 0
    /\ UNCHANGED
        <<generation, requested, count, mapping, recoveryOwed, returned>>

\* An unchanged snapshot under the exact complete policy replays the current
\* head and releases its transient working root without a new commit.
ReplayPublished ==
    /\ ~returned
    /\ Mapped # 0
    /\ Mapped = head
    /\ working = Mapped
    /\ Build(Mapped).commit = "PUBLISHED"
    /\ Build(Mapped).policy = requested
    /\ working' = 0
    /\ UNCHANGED
        <<generation, requested, builds, count, mapping, head,
          recoveryOwed, returned>>

\* This is the public synchronization success boundary.  It deliberately has
\* no branch that can return an older-policy head after recovering it.
ReturnSuccess ==
    /\ ~returned
    /\ ~recoveryOwed
    /\ NoPending
    /\ working = 0
    /\ Mapped # 0
    /\ Mapped = head
    /\ HeadPolicy = requested
    /\ returned' = TRUE
    /\ UNCHANGED
        <<generation, requested, builds, count, working, mapping, head,
          recoveryOwed>>

Terminal ==
    /\ generation = MaxGeneration
    /\ returned
    /\ UNCHANGED vars

Progress ==
    RecoverPending \/ Handoff \/ BeginAnalysis \/ CompleteAnalysis
    \/ CommitPublication \/ FinalizeMapped \/ ReplayPublished
    \/ ReturnSuccess \/ NextSync

Next == Progress \/ (\E p \in Policies : Takeover(p)) \/ Terminal

Spec == Init /\ [][Next]_vars /\ SF_vars(Progress)

TypeOK ==
    /\ generation \in 1..MaxGeneration
    /\ requested \in Policies
    /\ count \in 0..MaxBuilds
    /\ \A b \in 1..count :
        /\ Build(b).policy \in Policies \cup {NoPolicy}
        /\ Build(b).analysis \in AnalysisStates
        /\ Build(b).commit \in CommitStates
    /\ working \in 0..count
    /\ head \in 0..count
    /\ \A g \in 1..MaxGeneration : mapping[g] \in 0..count
    /\ recoveryOwed \in BOOLEAN
    /\ returned \in BOOLEAN

\* Each build acquires exactly one complete policy before analysis begins.
OnePolicyPerBuild ==
    \A b \in 1..count :
        (Build(b).analysis = "NONE") => (Build(b).policy = NoPolicy)

CommitRequiresCompleteAnalysis ==
    \A b \in 1..count :
        DurableCommit(b) => Build(b).analysis = "COMPLETE"

AtMostOnePendingCommit == Cardinality(PendingBuilds) <= 1

\* A takeover that owes recovery has not consumed the new generation's source
\* mapping.  Handoff remains disabled until RecoverPending clears the debt.
RecoveryPrecedesSourceHandoff ==
    recoveryOwed => mapping[generation] = 0

\* Any live mapping is analyzable under the complete requested policy.  A
\* foreign durable build is recovered receipt-by-receipt and is never mapped
\* into the takeover generation.
MappingIsAnalyzable ==
    Mapped # 0 => ~ForeignPolicy(Mapped)

RetiredBuildNeverPublishes ==
    \A b \in 1..count :
        (Build(b).analysis = "ABANDONED") => (Build(b).commit = "NONE")

\* Recovery creates no mapping or build, so at most one source build/mapping
\* is added per generation even when an old publication is finalized first.
MappingsBounded ==
    /\ Cardinality(
        {g \in 1..MaxGeneration : mapping[g] # 0}
       ) <= generation
    /\ count <= generation

\* Public success means the exact requested complete policy is already the
\* current head.  This is the safety property that rules out hidden deferral.
SuccessfulReturnHasRequestedPolicy ==
    returned =>
        /\ head # 0
        /\ Mapped = head
        /\ HeadPolicy = requested
        /\ NoPending

\* Takeovers are finite under MaxGeneration.  Strong fairness of Progress then
\* makes the final synchronization return with its requested policy current.
ConvergesBeforeReturn == <>[](returned /\ HeadPolicy = requested)

=============================================================================
