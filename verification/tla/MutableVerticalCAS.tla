------------------------- MODULE MutableVerticalCAS -------------------------
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************
This is a finite, provider-neutral safety model for a mutable owner/stage
checkpoint decomposed into narrow member relations and for the immutable
batch receipts that advance it.  A checkpoint key is an exact
<<owner, stage>> pair.  Its durable tuple contains generation, processed
count, last batch key, and terminal state.  A receipt records the matching
start generation/count, the positive server-clamped page limit, row count,
derived committed generation/next count, and whether the server-derived page
was the terminal empty page.  Replay accepts any admitted retry caller limit
but reruns and compares through the stored immutable page limit; the retry
value is never durable or query authority.

CreateCheckpoint and CommitBatch are single atomic database transitions.
CommitBatch publishes the receipt anchor, every receipt member, its seal, and
the checkpoint poststate together.  There is no modeled action that exposes a
receipt without its complete tuple or advances a visible checkpoint without
the matching receipt.  Response loss changes only client bookkeeping; exact
retry returns the stored receipt without changing the durable fingerprint.

Cleanup first removes the checkpoint seal and all of its receipt seals in one
visibility transition.  It then removes hidden receipt members and anchors,
followed by hidden checkpoint members and anchor, in a fixed child-first order
that avoids exploring irrelevant deletion permutations.  A crash may occur
between cleanup actions, but no partial tuple is visible then.

A successful TLC run exhausts only the finite constants selected by a
companion cfg.  It is bounded safety evidence, not an unbounded proof.  It
does not establish liveness or refinement by Python, SQL, transactions,
isolation levels, generated views, SQLite, or MariaDB.
***************************************************************************)

CONSTANTS
    Owners,
    Stages,
    BatchKeys,
    NoBatchKey,
    RowCounts,
    PageLimits,
    DefaultPageLimit,
    MaxGeneration,
    MaxCount,
    MaxPageLimit

ASSUME /\ Owners # {}
       /\ Stages # {}
       /\ BatchKeys # {}
       /\ NoBatchKey \notin BatchKeys
       /\ MaxGeneration \in Nat \ {0}
       /\ MaxCount \in Nat
       /\ MaxPageLimit \in Nat \ {0}
       /\ RowCounts \subseteq 0..MaxCount
       /\ 0 \in RowCounts
       /\ PageLimits \subseteq 1..MaxPageLimit
       /\ DefaultPageLimit \in PageLimits

Generations == 0..MaxGeneration
Counts == 0..MaxCount
OwnerStages == Owners \X Stages

CheckpointMembers == {
    "GENERATION", "COUNT", "LAST_BATCH", "TERMINAL"
}
ReceiptMembers == {
    "START_GENERATION", "START_COUNT", "PAGE_LIMIT", "ROW_COUNT",
    "COMMITTED_GENERATION", "NEXT_COUNT", "TERMINAL"
}

CheckpointSlots == OwnerStages \X CheckpointMembers

ReceiptId(key, batchKey) == <<key, batchKey>>
ReceiptIds == {
    ReceiptId(key, batchKey) :
        key \in OwnerStages, batchKey \in BatchKeys
}
ReceiptSlots == ReceiptIds \X ReceiptMembers

NoResponse == <<>>
BatchRequest(key, batchKey, rowCount) ==
    <<"BATCH", key, batchKey, rowCount>>
Requests == {
    BatchRequest(key, batchKey, rowCount) :
        key \in OwnerStages,
        batchKey \in BatchKeys,
        rowCount \in RowCounts
}

EventKinds == {
    "INIT", "CHECKPOINT_CREATE", "BATCH_COMMIT",
    "BATCH_RESPONSE_LOST", "REPLAY", "REPLAY_RESPONSE_LOST",
    "CHANGED_RETRY_REJECTED", "CRASH", "RESTART",
    "CLEANUP_BEGIN", "CLEANUP_RECEIPT_MEMBER",
    "CLEANUP_RECEIPT_ANCHOR", "CLEANUP_CHECKPOINT_MEMBER",
    "CLEANUP_CHECKPOINT_ANCHOR"
}

VARIABLES
    processUp,
    lostResponse,

    checkpointAnchors,
    checkpointMemberRows,
    checkpointSeals,
    checkpointGeneration,
    checkpointCount,
    checkpointLastBatch,
    checkpointTerminal,

    receiptAnchors,
    receiptMemberRows,
    receiptSeals,
    receiptStartGeneration,
    receiptStartCount,
    receiptPageLimit,
    receiptRowCount,
    receiptCommittedGeneration,
    receiptNextCount,
    receiptTerminal,

    cleanupStarted,
    cleanupCompleted,

    lastEvent,
    replaySnapshot,
    crashSnapshot

CheckpointPhysicalVariables ==
    <<checkpointAnchors, checkpointMemberRows, checkpointSeals>>
CheckpointValueVariables ==
    <<checkpointGeneration, checkpointCount,
      checkpointLastBatch, checkpointTerminal>>
ReceiptPhysicalVariables ==
    <<receiptAnchors, receiptMemberRows, receiptSeals>>
ReceiptValueVariables ==
    <<receiptStartGeneration, receiptStartCount, receiptPageLimit, receiptRowCount,
      receiptCommittedGeneration, receiptNextCount, receiptTerminal>>
CleanupVariables == <<cleanupStarted, cleanupCompleted>>
DatabaseVariables ==
    <<CheckpointPhysicalVariables, CheckpointValueVariables,
      ReceiptPhysicalVariables, ReceiptValueVariables, CleanupVariables>>
ClientVariables == <<processUp, lostResponse>>
AuditVariables == <<lastEvent, replaySnapshot, crashSnapshot>>
vars == <<DatabaseVariables, ClientVariables, AuditVariables>>

DurableFingerprint == DatabaseVariables

CheckpointSlotsFor(key) ==
    {slot \in checkpointMemberRows : slot[1] = key}

ReceiptSlotsFor(receipt) ==
    {slot \in receiptMemberRows : slot[1] = receipt}

ReceiptKey(receipt) == receipt[1]
ReceiptBatchKey(receipt) == receipt[2]

StoredReceiptsFor(key) ==
    {receipt \in receiptAnchors : ReceiptKey(receipt) = key}

VisibleReceiptsFor(key) ==
    {receipt \in receiptSeals : ReceiptKey(receipt) = key}

AllCheckpointMembersPresent(key) ==
    \A member \in CheckpointMembers :
        <<key, member>> \in checkpointMemberRows

AllReceiptMembersPresent(receipt) ==
    \A member \in ReceiptMembers :
        <<receipt, member>> \in receiptMemberRows

CheckpointIsGenesis(key) ==
    /\ checkpointGeneration[key] = 0
    /\ checkpointCount[key] = 0
    /\ checkpointLastBatch[key] = NoBatchKey
    /\ ~checkpointTerminal[key]

ReceiptIsLatestFor(key, receipt) ==
    /\ receipt \in VisibleReceiptsFor(key)
    /\ \A other \in VisibleReceiptsFor(key) :
        receiptCommittedGeneration[other]
            <= receiptCommittedGeneration[receipt]

CheckpointMatchesReceipt(key, receipt) ==
    /\ checkpointGeneration[key] = receiptCommittedGeneration[receipt]
    /\ checkpointCount[key] = receiptNextCount[receipt]
    /\ checkpointLastBatch[key] = ReceiptBatchKey(receipt)
    /\ checkpointTerminal[key] = receiptTerminal[receipt]

CheckpointMatchesLatestReceipt(key) ==
    LET receipts == VisibleReceiptsFor(key)
    IN IF receipts = {}
       THEN CheckpointIsGenesis(key)
       ELSE \E receipt \in receipts :
                /\ ReceiptIsLatestFor(key, receipt)
                /\ CheckpointMatchesReceipt(key, receipt)

Init ==
    /\ processUp = TRUE
    /\ lostResponse = NoResponse

    /\ checkpointAnchors = {}
    /\ checkpointMemberRows = {}
    /\ checkpointSeals = {}
    /\ checkpointGeneration = [key \in OwnerStages |-> 0]
    /\ checkpointCount = [key \in OwnerStages |-> 0]
    /\ checkpointLastBatch = [key \in OwnerStages |-> NoBatchKey]
    /\ checkpointTerminal = [key \in OwnerStages |-> FALSE]

    /\ receiptAnchors = {}
    /\ receiptMemberRows = {}
    /\ receiptSeals = {}
    /\ receiptStartGeneration = [receipt \in ReceiptIds |-> 0]
    /\ receiptStartCount = [receipt \in ReceiptIds |-> 0]
    /\ receiptPageLimit = [receipt \in ReceiptIds |-> DefaultPageLimit]
    /\ receiptRowCount = [receipt \in ReceiptIds |-> 0]
    /\ receiptCommittedGeneration = [receipt \in ReceiptIds |-> 0]
    /\ receiptNextCount = [receipt \in ReceiptIds |-> 0]
    /\ receiptTerminal = [receipt \in ReceiptIds |-> FALSE]

    /\ cleanupStarted = {}
    /\ cleanupCompleted = {}

    /\ lastEvent = "INIT"
    /\ replaySnapshot = DurableFingerprint
    /\ crashSnapshot = DurableFingerprint

(***************************************************************************
Checkpoint creation publishes the entire genesis tuple and its seal in one
atomic transition.  Batch commit likewise writes one complete immutable
receipt and advances every checkpoint member in the same transition.
***************************************************************************)

CreateCheckpoint(key) ==
    /\ processUp
    /\ key \notin checkpointAnchors
    /\ key \notin cleanupCompleted
    /\ checkpointAnchors' = checkpointAnchors \cup {key}
    /\ checkpointMemberRows' =
        checkpointMemberRows
            \cup {<<key, member>> : member \in CheckpointMembers}
    /\ checkpointSeals' = checkpointSeals \cup {key}
    /\ checkpointGeneration' =
        [checkpointGeneration EXCEPT ![key] = 0]
    /\ checkpointCount' = [checkpointCount EXCEPT ![key] = 0]
    /\ checkpointLastBatch' =
        [checkpointLastBatch EXCEPT ![key] = NoBatchKey]
    /\ checkpointTerminal' =
        [checkpointTerminal EXCEPT ![key] = FALSE]
    /\ lastEvent' = "CHECKPOINT_CREATE"
    /\ UNCHANGED
        <<ReceiptPhysicalVariables, ReceiptValueVariables,
          CleanupVariables, ClientVariables, replaySnapshot, crashSnapshot>>

CommitBatch(key, batchKey, rowCount, pageLimit, responseLost) ==
    LET receipt == ReceiptId(key, batchKey)
        request == BatchRequest(key, batchKey, rowCount)
        committedGeneration == checkpointGeneration[key] + 1
        nextCount == checkpointCount[key] + rowCount
        terminal == rowCount = 0
    IN
    /\ processUp
    /\ pageLimit \in PageLimits
    /\ key \in checkpointSeals
    /\ key \notin cleanupStarted
    /\ AllCheckpointMembersPresent(key)
    /\ ~checkpointTerminal[key]
    /\ checkpointGeneration[key] < MaxGeneration
    /\ nextCount \leq MaxCount
    /\ receipt \notin receiptAnchors
    /\ lostResponse = NoResponse
    /\ receiptAnchors' = receiptAnchors \cup {receipt}
    /\ receiptMemberRows' =
        receiptMemberRows
            \cup {<<receipt, member>> : member \in ReceiptMembers}
    /\ receiptSeals' = receiptSeals \cup {receipt}
    /\ receiptStartGeneration' =
        [receiptStartGeneration EXCEPT
            ![receipt] = checkpointGeneration[key]]
    /\ receiptStartCount' =
        [receiptStartCount EXCEPT ![receipt] = checkpointCount[key]]
    /\ receiptPageLimit' =
        [receiptPageLimit EXCEPT ![receipt] = pageLimit]
    /\ receiptRowCount' =
        [receiptRowCount EXCEPT ![receipt] = rowCount]
    /\ receiptCommittedGeneration' =
        [receiptCommittedGeneration EXCEPT
            ![receipt] = committedGeneration]
    /\ receiptNextCount' =
        [receiptNextCount EXCEPT ![receipt] = nextCount]
    /\ receiptTerminal' =
        [receiptTerminal EXCEPT ![receipt] = terminal]
    /\ checkpointGeneration' =
        [checkpointGeneration EXCEPT ![key] = committedGeneration]
    /\ checkpointCount' =
        [checkpointCount EXCEPT ![key] = nextCount]
    /\ checkpointLastBatch' =
        [checkpointLastBatch EXCEPT ![key] = batchKey]
    /\ checkpointTerminal' =
        [checkpointTerminal EXCEPT ![key] = terminal]
    /\ lostResponse' = IF responseLost THEN request ELSE NoResponse
    /\ lastEvent' =
        IF responseLost THEN "BATCH_RESPONSE_LOST" ELSE "BATCH_COMMIT"
    /\ UNCHANGED
        <<CheckpointPhysicalVariables, CleanupVariables,
          processUp, replaySnapshot, crashSnapshot>>

(***************************************************************************
An exact retry may follow a successful response or a lost response.  The
stored immutable receipt is the response authority even if a later batch has
advanced the checkpoint.  A changed row count under the same batch key is
rejected.  Both outcomes are observational with respect to durable state.
***************************************************************************)

ReplayBatch(key, batchKey, rowCount, retryPageLimit, responseLost) ==
    LET receipt == ReceiptId(key, batchKey)
        request == BatchRequest(key, batchKey, rowCount)
    IN
    /\ processUp
    /\ retryPageLimit \in PageLimits
    /\ receipt \in receiptSeals
    /\ receiptRowCount[receipt] = rowCount
    /\ lostResponse \in {NoResponse, request}
    /\ lostResponse' = IF responseLost THEN request ELSE NoResponse
    /\ lastEvent' =
        IF responseLost THEN "REPLAY_RESPONSE_LOST" ELSE "REPLAY"
    /\ replaySnapshot' = DurableFingerprint
    /\ UNCHANGED <<DatabaseVariables, processUp, crashSnapshot>>

RejectChangedRetry(key, batchKey, rowCount, retryPageLimit) ==
    LET receipt == ReceiptId(key, batchKey)
        request == BatchRequest(key, batchKey, rowCount)
    IN
    /\ processUp
    /\ retryPageLimit \in PageLimits
    /\ receipt \in receiptSeals
    /\ receiptRowCount[receipt] # rowCount
    /\ lostResponse \in {NoResponse, request}
    /\ lostResponse' = NoResponse
    /\ lastEvent' = "CHANGED_RETRY_REJECTED"
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

(***************************************************************************
Cleanup removes visibility first.  Receipt members use a canonical order;
receipts are removed oldest generation first.  Checkpoint members are removed
only after every child receipt anchor has gone.
***************************************************************************)

BeginCleanup(key) ==
    /\ processUp
    /\ key \in checkpointSeals
    /\ key \notin cleanupStarted
    /\ checkpointSeals' = checkpointSeals \ {key}
    /\ receiptSeals' =
        receiptSeals
            \ {receipt \in receiptSeals : ReceiptKey(receipt) = key}
    /\ cleanupStarted' = cleanupStarted \cup {key}
    /\ lostResponse' = NoResponse
    /\ lastEvent' = "CLEANUP_BEGIN"
    /\ UNCHANGED
        <<checkpointAnchors, checkpointMemberRows,
          CheckpointValueVariables,
          receiptAnchors, receiptMemberRows, ReceiptValueVariables,
          cleanupCompleted, processUp, replaySnapshot, crashSnapshot>>

PriorReceiptMembersRemoved(receipt, member) ==
    IF member = "START_GENERATION"
    THEN TRUE
    ELSE IF member = "START_COUNT"
         THEN <<receipt, "START_GENERATION">> \notin receiptMemberRows
         ELSE IF member = "PAGE_LIMIT"
              THEN /\ <<receipt, "START_GENERATION">>
                         \notin receiptMemberRows
                   /\ <<receipt, "START_COUNT">>
                         \notin receiptMemberRows
              ELSE IF member = "ROW_COUNT"
                   THEN /\ <<receipt, "START_GENERATION">>
                              \notin receiptMemberRows
                        /\ <<receipt, "START_COUNT">>
                              \notin receiptMemberRows
                        /\ <<receipt, "PAGE_LIMIT">>
                              \notin receiptMemberRows
                   ELSE IF member = "COMMITTED_GENERATION"
                        THEN /\ <<receipt, "START_GENERATION">>
                                   \notin receiptMemberRows
                             /\ <<receipt, "START_COUNT">>
                                   \notin receiptMemberRows
                             /\ <<receipt, "PAGE_LIMIT">>
                                   \notin receiptMemberRows
                             /\ <<receipt, "ROW_COUNT">>
                                   \notin receiptMemberRows
                        ELSE IF member = "NEXT_COUNT"
                             THEN /\ <<receipt, "START_GENERATION">>
                                        \notin receiptMemberRows
                                  /\ <<receipt, "START_COUNT">>
                                        \notin receiptMemberRows
                                  /\ <<receipt, "PAGE_LIMIT">>
                                        \notin receiptMemberRows
                                  /\ <<receipt, "ROW_COUNT">>
                                        \notin receiptMemberRows
                                  /\ <<receipt, "COMMITTED_GENERATION">>
                                        \notin receiptMemberRows
                             ELSE /\ member = "TERMINAL"
                                  /\ \A prior \in
                                         ReceiptMembers \ {"TERMINAL"} :
                                         <<receipt, prior>>
                                             \notin receiptMemberRows

EarlierReceiptsRemoved(receipt) ==
    \A other \in StoredReceiptsFor(ReceiptKey(receipt)) :
        receiptStartGeneration[other]
            < receiptStartGeneration[receipt]
            => other \notin receiptAnchors

CleanupReceiptMember(receipt, member) ==
    /\ processUp
    /\ ReceiptKey(receipt) \in cleanupStarted
    /\ receipt \in receiptAnchors
    /\ receipt \notin receiptSeals
    /\ <<receipt, member>> \in receiptMemberRows
    /\ EarlierReceiptsRemoved(receipt)
    /\ PriorReceiptMembersRemoved(receipt, member)
    /\ receiptMemberRows' = receiptMemberRows \ {<<receipt, member>>}
    /\ lastEvent' = "CLEANUP_RECEIPT_MEMBER"
    /\ UNCHANGED
        <<CheckpointPhysicalVariables, CheckpointValueVariables,
          receiptAnchors, receiptSeals, ReceiptValueVariables,
          CleanupVariables, ClientVariables, replaySnapshot, crashSnapshot>>

CleanupReceiptAnchor(receipt) ==
    /\ processUp
    /\ ReceiptKey(receipt) \in cleanupStarted
    /\ receipt \in receiptAnchors
    /\ receipt \notin receiptSeals
    /\ ReceiptSlotsFor(receipt) = {}
    /\ EarlierReceiptsRemoved(receipt)
    /\ receiptAnchors' = receiptAnchors \ {receipt}
    /\ lastEvent' = "CLEANUP_RECEIPT_ANCHOR"
    /\ UNCHANGED
        <<CheckpointPhysicalVariables, CheckpointValueVariables,
          receiptMemberRows, receiptSeals, ReceiptValueVariables,
          CleanupVariables, ClientVariables, replaySnapshot, crashSnapshot>>

PriorCheckpointMembersRemoved(key, member) ==
    IF member = "GENERATION"
    THEN TRUE
    ELSE IF member = "COUNT"
         THEN <<key, "GENERATION">> \notin checkpointMemberRows
         ELSE IF member = "LAST_BATCH"
              THEN /\ <<key, "GENERATION">> \notin checkpointMemberRows
                   /\ <<key, "COUNT">> \notin checkpointMemberRows
              ELSE /\ member = "TERMINAL"
                   /\ \A prior \in CheckpointMembers \ {"TERMINAL"} :
                        <<key, prior>> \notin checkpointMemberRows

CleanupCheckpointMember(key, member) ==
    /\ processUp
    /\ key \in cleanupStarted
    /\ key \notin checkpointSeals
    /\ StoredReceiptsFor(key) = {}
    /\ <<key, member>> \in checkpointMemberRows
    /\ PriorCheckpointMembersRemoved(key, member)
    /\ checkpointMemberRows' = checkpointMemberRows \ {<<key, member>>}
    /\ lastEvent' = "CLEANUP_CHECKPOINT_MEMBER"
    /\ UNCHANGED
        <<checkpointAnchors, checkpointSeals, CheckpointValueVariables,
          ReceiptPhysicalVariables, ReceiptValueVariables,
          CleanupVariables, ClientVariables, replaySnapshot, crashSnapshot>>

CleanupCheckpointAnchor(key) ==
    /\ processUp
    /\ key \in cleanupStarted
    /\ key \in checkpointAnchors
    /\ key \notin checkpointSeals
    /\ CheckpointSlotsFor(key) = {}
    /\ StoredReceiptsFor(key) = {}
    /\ checkpointAnchors' = checkpointAnchors \ {key}
    /\ cleanupStarted' = cleanupStarted \ {key}
    /\ cleanupCompleted' = cleanupCompleted \cup {key}
    /\ lastEvent' = "CLEANUP_CHECKPOINT_ANCHOR"
    /\ UNCHANGED
        <<checkpointMemberRows, checkpointSeals, CheckpointValueVariables,
          ReceiptPhysicalVariables, ReceiptValueVariables,
          ClientVariables, replaySnapshot, crashSnapshot>>

Next ==
    \/ Crash
    \/ Restart
    \/ \E key \in OwnerStages : CreateCheckpoint(key)
    \/ \E key \in OwnerStages,
          batchKey \in BatchKeys,
          rowCount \in RowCounts,
          pageLimit \in PageLimits :
        CommitBatch(key, batchKey, rowCount, pageLimit, FALSE)
    \/ \E key \in OwnerStages,
          batchKey \in BatchKeys,
          rowCount \in RowCounts,
          pageLimit \in PageLimits :
        CommitBatch(key, batchKey, rowCount, pageLimit, TRUE)
    \/ \E key \in OwnerStages,
          batchKey \in BatchKeys,
          rowCount \in RowCounts,
          retryPageLimit \in PageLimits :
        ReplayBatch(key, batchKey, rowCount, retryPageLimit, FALSE)
    \/ \E key \in OwnerStages,
          batchKey \in BatchKeys,
          rowCount \in RowCounts,
          retryPageLimit \in PageLimits :
        ReplayBatch(key, batchKey, rowCount, retryPageLimit, TRUE)
    \/ \E key \in OwnerStages,
          batchKey \in BatchKeys,
          rowCount \in RowCounts,
          retryPageLimit \in PageLimits :
        RejectChangedRetry(key, batchKey, rowCount, retryPageLimit)
    \/ \E key \in OwnerStages : BeginCleanup(key)
    \/ \E receipt \in ReceiptIds, member \in ReceiptMembers :
        CleanupReceiptMember(receipt, member)
    \/ \E receipt \in ReceiptIds : CleanupReceiptAnchor(receipt)
    \/ \E key \in OwnerStages, member \in CheckpointMembers :
        CleanupCheckpointMember(key, member)
    \/ \E key \in OwnerStages : CleanupCheckpointAnchor(key)

Spec == Init /\ [][Next]_vars

(***************************************************************************
Safety properties checked by the finite companion profiles.  They do not
claim liveness, SQL atomicity, or backend refinement.
***************************************************************************)

TypeOK ==
    /\ processUp \in BOOLEAN
    /\ lostResponse \in Requests \cup {NoResponse}
    /\ checkpointAnchors \subseteq OwnerStages
    /\ checkpointMemberRows \subseteq CheckpointSlots
    /\ checkpointSeals \subseteq OwnerStages
    /\ checkpointGeneration \in [OwnerStages -> Generations]
    /\ checkpointCount \in [OwnerStages -> Counts]
    /\ checkpointLastBatch
        \in [OwnerStages -> BatchKeys \cup {NoBatchKey}]
    /\ checkpointTerminal \in [OwnerStages -> BOOLEAN]
    /\ receiptAnchors \subseteq ReceiptIds
    /\ receiptMemberRows \subseteq ReceiptSlots
    /\ receiptSeals \subseteq ReceiptIds
    /\ receiptStartGeneration \in [ReceiptIds -> Generations]
    /\ receiptStartCount \in [ReceiptIds -> Counts]
    /\ receiptPageLimit \in [ReceiptIds -> PageLimits]
    /\ receiptRowCount \in [ReceiptIds -> RowCounts]
    /\ receiptCommittedGeneration \in [ReceiptIds -> Generations]
    /\ receiptNextCount \in [ReceiptIds -> Counts]
    /\ receiptTerminal \in [ReceiptIds -> BOOLEAN]
    /\ cleanupStarted \subseteq OwnerStages
    /\ cleanupCompleted \subseteq OwnerStages
    /\ cleanupStarted \cap cleanupCompleted = {}
    /\ lastEvent \in EventKinds

VisibleTuplesAreTotal ==
    /\ \A key \in checkpointSeals :
        /\ key \in checkpointAnchors
        /\ AllCheckpointMembersPresent(key)
    /\ \A receipt \in receiptSeals :
        /\ receipt \in receiptAnchors
        /\ AllReceiptMembersPresent(receipt)
        /\ ReceiptKey(receipt) \in checkpointSeals
    /\ \A key \in checkpointAnchors :
        ~AllCheckpointMembersPresent(key) => key \notin checkpointSeals
    /\ \A receipt \in receiptAnchors :
        ~AllReceiptMembersPresent(receipt) => receipt \notin receiptSeals

CheckpointEqualsLatestReceiptPoststate ==
    \A key \in checkpointSeals :
        /\ StoredReceiptsFor(key) = VisibleReceiptsFor(key)
        /\ CheckpointMatchesLatestReceipt(key)

ReceiptGenerationsAreUnique ==
    \A key \in OwnerStages :
        \A left \in StoredReceiptsFor(key) :
            \A right \in StoredReceiptsFor(key) :
                /\ (receiptStartGeneration[left]
                        = receiptStartGeneration[right]
                    => left = right)
                /\ (receiptCommittedGeneration[left]
                        = receiptCommittedGeneration[right]
                    => left = right)

ReceiptEquations ==
    \A receipt \in receiptAnchors :
        /\ receiptPageLimit[receipt] \in PageLimits
        /\ receiptCommittedGeneration[receipt]
            = receiptStartGeneration[receipt] + 1
        /\ receiptNextCount[receipt]
            = receiptStartCount[receipt] + receiptRowCount[receipt]
        /\ (receiptTerminal[receipt] <=> receiptRowCount[receipt] = 0)

AtomicCommitHasNoPublishedHalf ==
    /\ VisibleTuplesAreTotal
    /\ CheckpointEqualsLatestReceiptPoststate
    /\ \A receipt \in receiptSeals :
        receiptCommittedGeneration[receipt]
            <= checkpointGeneration[ReceiptKey(receipt)]

ReplayIsObservational ==
    lastEvent \in {
        "REPLAY", "REPLAY_RESPONSE_LOST", "CHANGED_RETRY_REJECTED"
    } => DurableFingerprint = replaySnapshot

ReplayUsesStoredPageLimit ==
    lastEvent \in {
        "REPLAY", "REPLAY_RESPONSE_LOST", "CHANGED_RETRY_REJECTED"
    } =>
        /\ DurableFingerprint = replaySnapshot
        /\ \A receipt \in receiptSeals :
            receiptPageLimit[receipt] \in PageLimits

CrashPreservesDurableState ==
    lastEvent = "CRASH" => DurableFingerprint = crashSnapshot

CrashLeavesNoTornTuple ==
    lastEvent = "CRASH" =>
        /\ VisibleTuplesAreTotal
        /\ CheckpointEqualsLatestReceiptPoststate

CleanupIsChildFirst ==
    /\ cleanupStarted \subseteq checkpointAnchors
    /\ \A key \in cleanupStarted :
        /\ key \notin checkpointSeals
        /\ VisibleReceiptsFor(key) = {}
    /\ \A receipt \in receiptAnchors :
        /\ ReceiptKey(receipt) \in checkpointAnchors
        /\ ReceiptSlotsFor(receipt) \subseteq receiptMemberRows
    /\ \A receipt \in ReceiptIds \ receiptAnchors :
        ReceiptSlotsFor(receipt) = {}
    /\ \A key \in cleanupCompleted :
        /\ key \notin checkpointAnchors
        /\ key \notin checkpointSeals
        /\ CheckpointSlotsFor(key) = {}
        /\ StoredReceiptsFor(key) = {}
    /\ \A key \in OwnerStages \ checkpointAnchors :
        /\ key \notin checkpointSeals
        /\ CheckpointSlotsFor(key) = {}
        /\ StoredReceiptsFor(key) = {}

Safety ==
    /\ TypeOK
    /\ VisibleTuplesAreTotal
    /\ CheckpointEqualsLatestReceiptPoststate
    /\ ReceiptGenerationsAreUnique
    /\ ReceiptEquations
    /\ AtomicCommitHasNoPublishedHalf
    /\ ReplayIsObservational
    /\ ReplayUsesStoredPageLimit
    /\ CrashPreservesDurableState
    /\ CrashLeavesNoTornTuple
    /\ CleanupIsChildFirst

(***************************************************************************
SafetyView is a fingerprint quotient only for invariant checking.  Audit
variables never enable an action; they retain only the pre-state of the most
recent replay or crash.  Including Safety prevents a violating audit state
from being merged with a safe one that has the same durable/client state.
Do not use this view as liveness or event-reachability evidence.
***************************************************************************)

SafetyView == <<DatabaseVariables, ClientVariables, Safety>>

=============================================================================
