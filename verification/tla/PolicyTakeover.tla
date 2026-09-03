----------------------------- MODULE PolicyTakeover -----------------------------
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************
Finite crash/retry model of an analysis-policy change at an ingest takeover.

Each build carries the policy of its sole analysis run (the manifest forbids
a different-policy sibling analysis of one build), the analysis state, and
its publication commit state.  A takeover claims the next ingest generation
and may resolve another requested policy at any point of the lifecycle.  The
source handoff of the new generation must then never map that generation to
a build whose analysis the analysis stage would refuse forever: a build whose
analysis is OPEN, or COMPLETE without a durable commit, under a foreign
policy is retired before any mapping is written and a successor build of the
same snapshot is created under the requested policy; a foreign-policy build
whose commit is already durable is replayed so the publication finalizes
first, and the requested policy is applied by the following generation.

TLC exhausts only the configured policies and generation bound.  The model
does not establish the SQL retirement, drainage, cleanup or publication
transactions, nor that Python implements it; the crash matrix and the
fresh-ingest differential tests are the runtime evidence.
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
    generation,   \* the live ingest generation
    requested,    \* the live session's requested analysis policy
    builds,       \* [1..count -> [policy, analysis, commit]]
    count,        \* number of builds ever created
    working,      \* the sole source working build (0 = none)
    mapping,      \* generation -> build (0 = unmapped)
    head          \* the published head build (0 = none)

vars == <<generation, requested, builds, count, working, mapping, head>>

Build(b) == builds[b]

HeadPolicy == IF head = 0 THEN NoPolicy ELSE builds[head].policy

Init ==
    /\ generation = 1
    /\ requested \in Policies
    /\ builds = <<>>
    /\ count = 0
    /\ working = 0
    /\ mapping = [g \in 1..MaxGeneration |-> 0]
    /\ head = 0

NewBuild(policy) == [policy |-> policy, analysis |-> "NONE", commit |-> "NONE"]

Append(record) == [b \in 1..(count + 1) |-> IF b = count + 1 THEN record ELSE builds[b]]

\* A crash and restart: the next generation, possibly under another policy.
\* Takeovers stop two generations before the bound so that the bounded
\* exploration can still observe the deferred successor of a durable replay.
Takeover(policy) ==
    /\ generation + 2 <= MaxGeneration
    /\ generation' = generation + 1
    /\ requested' = policy
    /\ UNCHANGED <<builds, count, working, mapping, head>>

\* A completed turn is followed by the next generation's turn.
NextTurn ==
    /\ working = 0
    /\ mapping[generation] # 0
    /\ generation < MaxGeneration
    /\ generation' = generation + 1
    /\ UNCHANGED <<requested, builds, count, working, mapping, head>>

DurableCommit(b) == Build(b).commit \in {"DB_COMMITTED", "PUBLISHED"}

ForeignPolicy(b) == Build(b).policy \notin {NoPolicy, requested}

\* Retire the working build and create the successor in one transaction.
Retire(b) ==
    /\ count < MaxBuilds
    /\ builds' = [Append(NewBuild(NoPolicy)) EXCEPT ![b].analysis =
                    IF Build(b).analysis = "OPEN" THEN "ABANDONED" ELSE Build(b).analysis]
    /\ count' = count + 1
    /\ working' = count + 1
    /\ mapping' = [mapping EXCEPT ![generation] = count + 1]
    /\ UNCHANGED <<generation, requested, head>>

\* The source handoff of an unmapped generation.
Handoff ==
    /\ mapping[generation] = 0
    /\ \/ /\ working = 0
          /\ \/ /\ HeadPolicy = requested
                /\ mapping' = [mapping EXCEPT ![generation] = head]
                /\ working' = head
                /\ UNCHANGED <<builds, count>>
             \/ /\ HeadPolicy # requested
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
          /\ DurableCommit(working)
          /\ mapping' = [mapping EXCEPT ![generation] = working]
          /\ UNCHANGED <<builds, count, working>>
       \/ /\ working # 0
          /\ ForeignPolicy(working)
          /\ ~DurableCommit(working)
          /\ Retire(working)
    /\ UNCHANGED <<generation, requested, head>>

Mapped == mapping[generation]

BeginAnalysis ==
    /\ Mapped # 0
    /\ Build(Mapped).analysis = "NONE"
    /\ builds' = [builds EXCEPT ![Mapped].analysis = "OPEN", ![Mapped].policy = requested]
    /\ UNCHANGED <<generation, requested, count, working, mapping, head>>

CompleteAnalysis ==
    /\ Mapped # 0
    /\ Build(Mapped).analysis = "OPEN"
    /\ Build(Mapped).policy = requested
    /\ builds' = [builds EXCEPT ![Mapped].analysis = "COMPLETE"]
    /\ UNCHANGED <<generation, requested, count, working, mapping, head>>

\* Publication of the mapped build: only its own-policy analysis, or a
\* foreign-policy analysis whose commit is already durable, may proceed.
CommitPublication ==
    /\ Mapped # 0
    /\ Build(Mapped).analysis = "COMPLETE"
    /\ Build(Mapped).commit = "NONE"
    /\ Build(Mapped).policy = requested
    /\ builds' = [builds EXCEPT ![Mapped].commit = "DB_COMMITTED"]
    /\ UNCHANGED <<generation, requested, count, working, mapping, head>>

Finalize ==
    /\ Mapped # 0
    /\ Build(Mapped).commit = "DB_COMMITTED"
    /\ builds' = [builds EXCEPT ![Mapped].commit = "PUBLISHED"]
    /\ head' = Mapped
    /\ working' = 0
    /\ UNCHANGED <<generation, requested, count, mapping>>

\* An unchanged snapshot replays the published head: the turn completes and
\* releases the working root without a new candidate or commit.
ReplayPublished ==
    /\ Mapped # 0
    /\ Build(Mapped).commit = "PUBLISHED"
    /\ working = Mapped
    /\ working' = 0
    /\ UNCHANGED <<generation, requested, builds, count, mapping, head>>

Terminal ==
    /\ generation = MaxGeneration
    /\ working = 0
    /\ mapping[generation] # 0
    /\ UNCHANGED vars

Progress ==
    Handoff \/ BeginAnalysis \/ CompleteAnalysis \/ CommitPublication \/ Finalize
    \/ ReplayPublished \/ NextTurn

Next == Progress \/ (\E p \in Policies : Takeover(p)) \/ Terminal

Spec == Init /\ [][Next]_vars /\ SF_vars(Progress)

TypeOK ==
    /\ generation \in 1..MaxGeneration
    /\ requested \in Policies
    /\ count \in 0..MaxBuilds
    /\ \A b \in 1..count : /\ Build(b).policy \in Policies \cup {NoPolicy}
                           /\ Build(b).analysis \in AnalysisStates
                           /\ Build(b).commit \in CommitStates
    /\ working \in 0..count
    /\ head \in 0..count
    /\ \A g \in 1..MaxGeneration : mapping[g] \in 0..count

\* Each build has at most one analysis policy for its whole life.
OnePolicyPerBuild ==
    \A b \in 1..count : Build(b).analysis = "NONE" => Build(b).policy = NoPolicy

\* A durable commit belongs to a COMPLETE analysis.
CommitRequiresCompleteAnalysis ==
    \A b \in 1..count : DurableCommit(b) => Build(b).analysis = "COMPLETE"

\* The live generation never names a build the analysis stage would refuse
\* forever: a foreign-policy mapping exists only for a durable replay.
MappingIsAnalyzable ==
    Mapped # 0 => (~ForeignPolicy(Mapped) \/ DurableCommit(Mapped))

\* A retired build never becomes the head.
RetiredBuildNeverPublishes ==
    \A b \in 1..count :
        (Build(b).analysis = "ABANDONED") => Build(b).commit = "NONE"

\* Generation mappings are one per generation; builds are bounded by the
\* generations that could create them (the head reuse creates none).
MappingsBounded ==
    /\ Cardinality({g \in 1..MaxGeneration : mapping[g] # 0}) <= generation
    /\ count <= generation

\* Takeovers stop before the bound; the head then converges to the
\* requested policy and stays there.
Converges == <>[](HeadPolicy = requested)

=============================================================================
