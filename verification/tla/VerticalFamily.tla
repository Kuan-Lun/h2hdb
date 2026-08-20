--------------------------- MODULE VerticalFamily ---------------------------
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************
This is a finite, provider-neutral protocol model for a sealed vertical
family.  It models one PK-only anchor, multiple independently staged member
slots, a PK-only completion seal, response-loss replay, crashes, and
child-first cleanup.  One distinguished member relation is shared across
family keys; an external reference may retain that shared row after all
family links to it have been removed.

The visible wide relation is abstracted as keys in phase SEALED.  A key can
enter that phase only after its anchor and every member slot are durable, and
cleanup leaves SEALED before removing any member link or anchor.  Member and
seal retries after response loss are observational: they change only client
response bookkeeping, never the durable database fingerprint.

A successful TLC run exhausts only the finite Keys and Members selected by a
companion cfg.  It is not an unbounded proof and does not establish that
Python writers, generated SQL views, foreign keys, transactions, isolation,
or either database backend refine this model.
***************************************************************************)

CONSTANTS
    Keys,
    Members,
    SharedMember

ASSUME /\ Cardinality(Keys) >= 2
       /\ Cardinality(Members) >= 2
       /\ SharedMember \in Members

KeyPhases == {"EMPTY", "STAGING", "SEALED", "CLEANING", "DELETED"}
MemberSlots == Keys \X Members

\* Every key shares the distinguished member row.  Other member rows are
\* key-local.  Tuple arity keeps the two row-identity domains disjoint.
MemberRow(key, member) ==
    IF member = SharedMember
    THEN <<member>>
    ELSE <<key, member>>

MemberRows ==
    {MemberRow(key, member) : key \in Keys, member \in Members}

SharedRow == <<SharedMember>>

MemberRequest(key, member) == <<"MEMBER", key, member>>
SealRequest(key) == <<"SEAL", key>>
NoResponse == <<>>
Requests ==
    {MemberRequest(key, member) : key \in Keys, member \in Members}
    \cup {SealRequest(key) : key \in Keys}

EventKinds == {
    "INIT", "ANCHOR_WRITE", "MEMBER_WRITE", "MEMBER_RESPONSE_LOST",
    "MEMBER_REPLAY", "SEAL_WRITE", "SEAL_RESPONSE_LOST", "SEAL_REPLAY",
    "CRASH", "RESTART", "EXTERNAL_REFERENCE_ADD",
    "EXTERNAL_REFERENCE_REMOVE", "CLEANUP_BEGIN", "CLEANUP_MEMBER",
    "CLEANUP_ANCHOR", "CLEANUP_ORPHAN_MEMBER"
}

VARIABLES
    processUp,
    keyPhase,
    everSealed,
    memberLinks,
    memberRows,
    sharedExternalReference,
    lostResponse,
    lastEvent,
    replaySnapshot,
    crashSnapshot

DatabaseVariables ==
    <<keyPhase, everSealed, memberLinks, memberRows,
      sharedExternalReference>>

ClientVariables == <<processUp, lostResponse>>
AuditVariables == <<lastEvent, replaySnapshot, crashSnapshot>>
vars == <<DatabaseVariables, ClientVariables, AuditVariables>>

DurableFingerprint == DatabaseVariables

AnchoredKeys ==
    {key \in Keys : keyPhase[key] \in {"STAGING", "SEALED", "CLEANING"}}

VisibleKeys == {key \in Keys : keyPhase[key] = "SEALED"}
CleanupStartedKeys ==
    {key \in Keys : keyPhase[key] \in {"CLEANING", "DELETED"}}
CleanupCompletedKeys == {key \in Keys : keyPhase[key] = "DELETED"}

SlotsForKey(key) == {slot \in memberLinks : slot[1] = key}

LinksForRow(row) ==
    {slot \in memberLinks : MemberRow(slot[1], slot[2]) = row}

AllMembersPresent(key) ==
    /\ key \in AnchoredKeys
    /\ \A member \in Members :
        /\ <<key, member>> \in memberLinks
        /\ MemberRow(key, member) \in memberRows

PartialKeys ==
    {key \in AnchoredKeys : ~AllMembersPresent(key)}

Init ==
    /\ processUp = TRUE
    /\ keyPhase = [key \in Keys |-> "EMPTY"]
    /\ everSealed = {}
    /\ memberLinks = {}
    /\ memberRows = {}
    /\ sharedExternalReference = FALSE
    /\ lostResponse = NoResponse
    /\ lastEvent = "INIT"
    /\ replaySnapshot = DurableFingerprint
    /\ crashSnapshot = DurableFingerprint

WriteAnchor(key) ==
    /\ processUp
    /\ keyPhase[key] = "EMPTY"
    /\ keyPhase' = [keyPhase EXCEPT ![key] = "STAGING"]
    /\ lastEvent' = "ANCHOR_WRITE"
    /\ UNCHANGED
        <<everSealed, memberLinks, memberRows, sharedExternalReference,
          ClientVariables, replaySnapshot, crashSnapshot>>

WriteMember(key, member, responseLost) ==
    LET slot == <<key, member>>
        row == MemberRow(key, member)
        request == MemberRequest(key, member)
    IN
    /\ processUp
    /\ keyPhase[key] = "STAGING"
    /\ slot \notin memberLinks
    /\ ~responseLost \/ lostResponse = NoResponse
    /\ memberLinks' = memberLinks \cup {slot}
    /\ memberRows' = memberRows \cup {row}
    /\ lostResponse' = IF responseLost THEN request ELSE lostResponse
    /\ lastEvent' =
        IF responseLost THEN "MEMBER_RESPONSE_LOST" ELSE "MEMBER_WRITE"
    /\ UNCHANGED
        <<keyPhase, everSealed, sharedExternalReference, processUp,
          replaySnapshot, crashSnapshot>>

ReplayMember(key, member) ==
    LET slot == <<key, member>>
        request == MemberRequest(key, member)
    IN
    /\ processUp
    /\ lostResponse = request
    /\ keyPhase[key] \in {"STAGING", "SEALED"}
    /\ slot \in memberLinks
    /\ MemberRow(key, member) \in memberRows
    /\ lostResponse' = NoResponse
    /\ lastEvent' = "MEMBER_REPLAY"
    /\ replaySnapshot' = DurableFingerprint
    /\ UNCHANGED <<DatabaseVariables, processUp, crashSnapshot>>

WriteSeal(key, responseLost) ==
    LET request == SealRequest(key)
    IN
    /\ processUp
    /\ keyPhase[key] = "STAGING"
    /\ key \notin everSealed
    /\ AllMembersPresent(key)
    /\ ~responseLost \/ lostResponse = NoResponse
    /\ keyPhase' = [keyPhase EXCEPT ![key] = "SEALED"]
    /\ everSealed' = everSealed \cup {key}
    /\ lostResponse' = IF responseLost THEN request ELSE lostResponse
    /\ lastEvent' =
        IF responseLost THEN "SEAL_RESPONSE_LOST" ELSE "SEAL_WRITE"
    /\ UNCHANGED
        <<memberLinks, memberRows, sharedExternalReference, processUp,
          replaySnapshot, crashSnapshot>>

ReplaySeal(key) ==
    /\ processUp
    /\ lostResponse = SealRequest(key)
    /\ keyPhase[key] = "SEALED"
    /\ key \in everSealed
    /\ lostResponse' = NoResponse
    /\ lastEvent' = "SEAL_REPLAY"
    /\ replaySnapshot' = DurableFingerprint
    /\ UNCHANGED <<DatabaseVariables, processUp, crashSnapshot>>

Crash ==
    /\ processUp
    /\ processUp' = FALSE
    /\ lastEvent' = "CRASH"
    /\ crashSnapshot' = DurableFingerprint
    /\ UNCHANGED <<DatabaseVariables, lostResponse, replaySnapshot>>

Restart ==
    /\ ~processUp
    /\ processUp' = TRUE
    /\ lastEvent' = "RESTART"
    /\ UNCHANGED
        <<DatabaseVariables, lostResponse, replaySnapshot, crashSnapshot>>

AddExternalReference ==
    /\ processUp
    /\ SharedRow \in memberRows
    /\ ~sharedExternalReference
    /\ sharedExternalReference' = TRUE
    /\ lastEvent' = "EXTERNAL_REFERENCE_ADD"
    /\ UNCHANGED
        <<keyPhase, everSealed, memberLinks, memberRows,
          ClientVariables, replaySnapshot, crashSnapshot>>

RemoveExternalReference ==
    /\ processUp
    /\ sharedExternalReference
    /\ sharedExternalReference' = FALSE
    /\ lastEvent' = "EXTERNAL_REFERENCE_REMOVE"
    /\ UNCHANGED
        <<keyPhase, everSealed, memberLinks, memberRows,
          ClientVariables, replaySnapshot, crashSnapshot>>

BeginCleanup(key) ==
    /\ processUp
    /\ keyPhase[key] \in {"STAGING", "SEALED"}
    /\ keyPhase' = [keyPhase EXCEPT ![key] = "CLEANING"]
    \* Cleanup invalidates every outstanding client response before children.
    /\ lostResponse' = NoResponse
    /\ lastEvent' = "CLEANUP_BEGIN"
    /\ UNCHANGED
        <<everSealed, memberLinks, memberRows, sharedExternalReference,
          processUp, replaySnapshot, crashSnapshot>>

CleanupMember(key, member) ==
    LET slot == <<key, member>>
        row == MemberRow(key, member)
        remainingLinks == memberLinks \ {slot}
        rowStillReferenced ==
            \E remaining \in remainingLinks :
                MemberRow(remaining[1], remaining[2]) = row
        externallyReferenced ==
            /\ row = SharedRow
            /\ sharedExternalReference
    IN
    /\ processUp
    /\ keyPhase[key] = "CLEANING"
    /\ slot \in memberLinks
    /\ memberLinks' = remainingLinks
    /\ memberRows' =
        IF rowStillReferenced \/ externallyReferenced
        THEN memberRows
        ELSE memberRows \ {row}
    /\ lastEvent' = "CLEANUP_MEMBER"
    /\ UNCHANGED
        <<keyPhase, everSealed, sharedExternalReference,
          ClientVariables, replaySnapshot, crashSnapshot>>

CleanupAnchor(key) ==
    /\ processUp
    /\ keyPhase[key] = "CLEANING"
    /\ SlotsForKey(key) = {}
    /\ keyPhase' = [keyPhase EXCEPT ![key] = "DELETED"]
    /\ lastEvent' = "CLEANUP_ANCHOR"
    /\ UNCHANGED
        <<everSealed, memberLinks, memberRows, sharedExternalReference,
          ClientVariables, replaySnapshot, crashSnapshot>>

CleanupOrphanMember(row) ==
    /\ processUp
    /\ row \in memberRows
    /\ LinksForRow(row) = {}
    /\ ~(row = SharedRow /\ sharedExternalReference)
    /\ memberRows' = memberRows \ {row}
    /\ lastEvent' = "CLEANUP_ORPHAN_MEMBER"
    /\ UNCHANGED
        <<keyPhase, everSealed, memberLinks, sharedExternalReference,
          ClientVariables, replaySnapshot, crashSnapshot>>

Next ==
    \/ Crash
    \/ Restart
    \/ \E key \in Keys : WriteAnchor(key)
    \/ \E key \in Keys, member \in Members :
        WriteMember(key, member, FALSE)
    \/ \E key \in Keys, member \in Members :
        WriteMember(key, member, TRUE)
    \/ \E key \in Keys, member \in Members : ReplayMember(key, member)
    \/ \E key \in Keys : WriteSeal(key, FALSE)
    \/ \E key \in Keys : WriteSeal(key, TRUE)
    \/ \E key \in Keys : ReplaySeal(key)
    \/ AddExternalReference
    \/ RemoveExternalReference
    \/ \E key \in Keys : BeginCleanup(key)
    \/ \E key \in Keys, member \in Members : CleanupMember(key, member)
    \/ \E key \in Keys : CleanupAnchor(key)
    \/ \E row \in MemberRows : CleanupOrphanMember(row)

Spec == Init /\ [][Next]_vars

(***************************************************************************
Safety properties checked by VerticalFamilySmall.cfg.  These are bounded TLC
properties over the selected constants, not claims about SQL refinement.
***************************************************************************)

TypeOK ==
    /\ processUp \in BOOLEAN
    /\ keyPhase \in [Keys -> KeyPhases]
    /\ everSealed \subseteq Keys
    /\ memberLinks \subseteq MemberSlots
    /\ memberRows \subseteq MemberRows
    /\ sharedExternalReference \in BOOLEAN
    /\ lostResponse \in Requests \cup {NoResponse}
    /\ lastEvent \in EventKinds

VisibleImpliesAllMembers ==
    \A key \in VisibleKeys : AllMembersPresent(key)

PartialNeverVisible == PartialKeys \cap VisibleKeys = {}

SealOnlyIfTotal ==
    \A key \in VisibleKeys :
        /\ key \in AnchoredKeys
        /\ key \in everSealed
        /\ AllMembersPresent(key)

SealImmutable ==
    /\ VisibleKeys \subseteq everSealed
    /\ \A key \in everSealed :
        keyPhase[key] \in {"SEALED", "CLEANING", "DELETED"}

ReplayObservational ==
    lastEvent \in {"MEMBER_REPLAY", "SEAL_REPLAY"} =>
        DurableFingerprint = replaySnapshot

CrashPreservesDurableState ==
    lastEvent = "CRASH" => DurableFingerprint = crashSnapshot

CleanupNeverLeavesVisiblePartial ==
    /\ CleanupStartedKeys \cap VisibleKeys = {}
    /\ \A key \in CleanupCompletedKeys :
        /\ key \notin AnchoredKeys
        /\ key \notin VisibleKeys
        /\ SlotsForKey(key) = {}

ChildFirstCleanup ==
    \A key \in CleanupCompletedKeys :
        /\ key \in CleanupStartedKeys
        /\ SlotsForKey(key) = {}

SharedMemberRetention ==
    /\ \A row \in MemberRows :
        LinksForRow(row) # {} => row \in memberRows
    /\ sharedExternalReference => SharedRow \in memberRows

Safety ==
    /\ TypeOK
    /\ VisibleImpliesAllMembers
    /\ PartialNeverVisible
    /\ SealOnlyIfTotal
    /\ SealImmutable
    /\ ReplayObservational
    /\ CrashPreservesDurableState
    /\ CleanupNeverLeavesVisiblePartial
    /\ ChildFirstCleanup
    /\ SharedMemberRetention

(***************************************************************************
SafetyView is a fingerprint quotient only for the invariant-only Small
profile.  Audit variables never enable an action; they only remember the
pre-state of the latest replay or crash.  Including Safety prevents TLC from
merging a violating audit state with a safe state that has the same durable
and client state.  Do not use this view for liveness or event reachability.
***************************************************************************)

SafetyView == <<DatabaseVariables, ClientVariables, Safety>>

=============================================================================
