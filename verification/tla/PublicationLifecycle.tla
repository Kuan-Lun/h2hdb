------------------------ MODULE PublicationLifecycle ------------------------
EXTENDS Naturals, FiniteSets, Sequences, TLC

(***************************************************************************
Finite provider-neutral safety model for the Batch8 publication authority.
One atomic publish transaction creates the fresh source descriptor, next
generation node and successor edge, receipt anchor, all thirteen mandatory
one-value members, an OPEN commit-owned permanent finalization checkpoint,
the commit seal, and the single common receipt head.  Candidate lifecycle,
old revision/generation relations, both old heads, publication receipt, and
operational activation are derived views; there is no stored candidate state,
candidate sealed_at, finalization timestamp, or mutable activation row.

PublishStatements exposes transaction-local prefix progress.  A prefix crash
rolls every statement back.  Finalization separates an issue read, an external
idempotent release, and the permanent receipt/checkpoint CAS commit.  Its
PK-only marker is written only with the terminal empty batch and COMPLETE
checkpoint poststate; sealed permanent receipts remain replayable after
candidate-local cleanup and response loss.

TLC exhausts only the finite companion-cfg instance.  This is bounded safety
evidence, not an unbounded proof.  It does not establish liveness or Python,
SQL, isolation, SQLite, MariaDB, or implementation refinement.
***************************************************************************)

CONSTANTS Candidates, BatchKeys, NoCandidate, NoRevision,
          MaxGeneration, MaxFinalizationGeneration

ASSUME /\ Candidates # {}
       /\ BatchKeys # {}
       /\ NoCandidate \notin Candidates
       /\ MaxGeneration \in Nat \ {0}
       /\ MaxFinalizationGeneration \in Nat \ {0, 1}

CandidatePhases == {"EMPTY", "OPEN", "SEALED", "PUBLISHED"}
CheckpointStates == {"OPEN", "COMPLETE"}
Generations == 1..MaxGeneration
NodeGenerations == 0..MaxGeneration
FinalizationGenerations == 1..MaxFinalizationGeneration
Times == 0..(MaxGeneration + MaxFinalizationGeneration)

SourceRevision(c) == <<"SOURCE_REVISION", c>>
CatalogRevision(c) == <<"CATALOG_REVISION", c>>
Preparation(c) == <<"PREPARATION", c>>
OperationalPolicy(c) == <<"OPERATIONAL_POLICY", c>>

CommitMembers == {
    "CANDIDATE", "CATALOG_REVISION", "SOURCE_REVISION", "GENERATION",
    "OPERATIONAL_PREPARATION", "OPERATIONAL_POLICY", "ARTIFACT_POLICY",
    "DISPLAY_TITLE_POLICY", "NEW_GALLERIES", "CHANGED_GALLERIES",
    "REMOVED_GALLERIES", "DUPLICATE_LOSERS", "COMMITTED_AT"
}
CommitRowsFor(c) == {<<c, member>> : member \in CommitMembers}

FinalizeId(c, key) == <<c, key>>
FinalizeIds == {
    FinalizeId(c, key) : c \in Candidates, key \in BatchKeys
}
NoResponse == <<>>
PublishRequest(c) == <<"PUBLISH", c>>
FinalizeRequest(c, key) == <<"FINALIZE", c, key>>
Requests ==
    {PublishRequest(c) : c \in Candidates}
    \cup {FinalizeRequest(c, key) : c \in Candidates, key \in BatchKeys}

PublishStatements == <<
    "INSERT_SOURCE_DESCRIPTOR", "INSERT_GENERATION_NODE",
    "INSERT_GENERATION_SUCCESSOR", "INSERT_COMMIT_ANCHOR",
    "INSERT_13_COMMIT_MEMBERS", "INSERT_FINALIZATION_CHECKPOINT_FAMILY",
    "INSERT_COMMIT_SEAL_LAST", "CAS_COMMON_HEAD_RECEIPT"
>>
PublishStatementCount == Len(PublishStatements)

EventKinds == {
    "INIT", "PREPARE_CATALOG_DESCRIPTOR", "SEAL_CANDIDATE",
    "PUBLISH_BEGIN", "PUBLISH_STATEMENT", "PUBLISH_COMMIT",
    "PUBLISH_RESPONSE_LOST", "PUBLISH_REPLAY",
    "PUBLISH_REPLAY_RESPONSE_LOST", "STALE_HEAD_CAS_REJECTED",
    "PUBLISH_PREFIX_CRASH", "IDLE_CRASH", "RESTART",
    "FINALIZE_ISSUE", "FINALIZE_EXTERNAL_RELEASE", "FINALIZE_COMMIT",
    "FINALIZE_RESPONSE_LOST", "FINALIZE_REPLAY",
    "FINALIZE_REPLAY_RESPONSE_LOST", "CANDIDATE_CLEANUP"
}

VARIABLES
    processUp, lostResponse,
    preparedCatalogDescriptors, preparedOperationalEffects,
    preparedOptionalRows, candidateDefinitions, projectionSeals,
    candidateBaseReceipt,
    sourceDescriptors, commitAnchors, commitMemberRows, commitSeals,
    commitGeneration, generationNodes, generationSuccessors,
    commonHeadReceipt,
    finalizedOptionalRows, receiptFinalizations,
    finalizationCheckpoints, checkpointGeneration, checkpointCursor,
    checkpointProcessedCount, checkpointState, checkpointUpdatedAt,
    finalizationBatchReceipts, batchStartGeneration,
    batchStartCursor, batchNextCursor,
    batchStartProcessedCount, batchNextProcessedCount, batchRowIncluded,
    batchTerminal, batchCommittedGeneration, batchCommittedAt,
    batchNextState,
    finalizeAttempt, finalizeIncludeRow, finalizeTerminal,
    releasedFinalizationBatches,
    txCandidate, txPrefix, lastEvent, txStartSnapshot,
    replaySnapshot, staleSnapshot, crashSnapshot

PreparationVariables ==
    <<preparedCatalogDescriptors, preparedOperationalEffects,
      preparedOptionalRows>>
CandidateVariables ==
    <<candidateDefinitions, projectionSeals, candidateBaseReceipt>>
CommitVariables ==
    <<sourceDescriptors, commitAnchors, commitMemberRows, commitSeals,
      commitGeneration, generationNodes, generationSuccessors,
      commonHeadReceipt>>
CheckpointVariables ==
    <<finalizationCheckpoints, checkpointGeneration, checkpointCursor,
      checkpointProcessedCount, checkpointState, checkpointUpdatedAt>>
BatchVariables ==
    <<finalizationBatchReceipts, batchStartGeneration,
      batchStartCursor, batchNextCursor,
      batchStartProcessedCount, batchNextProcessedCount, batchRowIncluded,
      batchTerminal, batchCommittedGeneration, batchCommittedAt,
      batchNextState>>
FinalizationVariables ==
    <<finalizedOptionalRows, receiptFinalizations,
      CheckpointVariables, BatchVariables>>
DatabaseVariables ==
    <<PreparationVariables, CandidateVariables, CommitVariables,
      FinalizationVariables>>
TransactionVariables == <<txCandidate, txPrefix>>
FinalizerControlVariables ==
    <<finalizeAttempt, finalizeIncludeRow, finalizeTerminal,
      releasedFinalizationBatches>>
ClientVariables == <<processUp, lostResponse, FinalizerControlVariables>>
AuditVariables ==
    <<lastEvent, txStartSnapshot, replaySnapshot,
      staleSnapshot, crashSnapshot>>
vars ==
    <<DatabaseVariables, TransactionVariables, ClientVariables,
      AuditVariables>>
DurableFingerprint == DatabaseVariables

BaseGeneration(c) ==
    IF candidateBaseReceipt[c] = NoCandidate
    THEN 0
    ELSE commitGeneration[candidateBaseReceipt[c]]
CurrentGeneration ==
    IF commonHeadReceipt = NoCandidate
    THEN 0
    ELSE commitGeneration[commonHeadReceipt]
CandidatePhase(c) ==
    IF c \in commitSeals
    THEN "PUBLISHED"
    ELSE IF c \in projectionSeals
         THEN "SEALED"
         ELSE IF c \in candidateDefinitions THEN "OPEN" ELSE "EMPTY"
DerivedActivations == {
    <<SourceRevision(c), Preparation(c), OperationalPolicy(c),
      commitGeneration[c]>> : c \in commitSeals
}
PublishedSourceRevisions == {SourceRevision(c) : c \in commitSeals}
PublishedCatalogRevisions == {CatalogRevision(c) : c \in commitSeals}

ExactCommit(c) ==
    /\ c \in commitAnchors
    /\ c \in commitSeals
    /\ CommitRowsFor(c) \subseteq commitMemberRows
    /\ SourceRevision(c) \in sourceDescriptors
    /\ commitGeneration[c] \in Generations
    /\ commitGeneration[c] \in generationNodes
    /\ <<commitGeneration[c], commitGeneration[c] - 1>>
        \in generationSuccessors

PublicationReady(c) ==
    LET next == BaseGeneration(c) + 1
    IN /\ CandidatePhase(c) = "SEALED"
       /\ c \in preparedCatalogDescriptors
       /\ c \in preparedOperationalEffects
       /\ candidateBaseReceipt[c] = commonHeadReceipt
       /\ c \notin commitSeals
       /\ SourceRevision(c) \notin sourceDescriptors
       /\ next \in Generations
       /\ next \notin generationNodes
       /\ ~\E edge \in generationSuccessors : edge[2] = BaseGeneration(c)

FinalizationBatchesFor(c) ==
    {batch \in finalizationBatchReceipts : batch[1] = c}
LatestFinalizationBatch(c, batch) ==
    /\ batch \in FinalizationBatchesFor(c)
    /\ \A other \in FinalizationBatchesFor(c) :
        batchCommittedGeneration[other] <= batchCommittedGeneration[batch]
CheckpointMatchesBatch(c, batch) ==
    /\ checkpointGeneration[c] = batchCommittedGeneration[batch]
    /\ checkpointCursor[c] = batchNextCursor[batch]
    /\ checkpointProcessedCount[c] = batchNextProcessedCount[batch]
    /\ checkpointState[c] = batchNextState[batch]
    /\ checkpointUpdatedAt[c] = batchCommittedAt[batch]
ExactTerminalBatch(c, batch) ==
    /\ batch \in FinalizationBatchesFor(c)
    /\ LatestFinalizationBatch(c, batch)
    /\ CheckpointMatchesBatch(c, batch)
    /\ batchTerminal[batch]
    /\ ~batchRowIncluded[batch]
    /\ batchStartCursor[batch] = batchNextCursor[batch]
    /\ batchNextState[batch] = "COMPLETE"
FinalizationVisible(c) ==
    /\ c \in receiptFinalizations
    /\ c \in finalizationCheckpoints
    /\ \E batch \in FinalizationBatchesFor(c) :
        ExactTerminalBatch(c, batch)
ReceiptState(c) ==
    IF FinalizationVisible(c)
    THEN "PROJECTION_FINALIZED"
    ELSE IF c \in commitSeals /\ checkpointState[c] = "OPEN"
         THEN "DB_COMMITTED"
         ELSE "NONE"
ReceiptFinalizedAt(c) ==
    IF FinalizationVisible(c)
    THEN batchCommittedAt[
        CHOOSE batch \in FinalizationBatchesFor(c) :
            ExactTerminalBatch(c, batch)]
    ELSE 0

Init ==
    /\ processUp = TRUE
    /\ lostResponse = NoResponse
    /\ preparedCatalogDescriptors = {}
    /\ preparedOperationalEffects = {}
    /\ preparedOptionalRows = {}
    /\ candidateDefinitions = {}
    /\ projectionSeals = {}
    /\ candidateBaseReceipt = [c \in Candidates |-> NoCandidate]
    /\ sourceDescriptors = {}
    /\ commitAnchors = {}
    /\ commitMemberRows = {}
    /\ commitSeals = {}
    /\ commitGeneration = [c \in Candidates |-> 0]
    /\ generationNodes = {0}
    /\ generationSuccessors = {}
    /\ commonHeadReceipt = NoCandidate
    /\ finalizedOptionalRows = {}
    /\ receiptFinalizations = {}
    /\ finalizationCheckpoints = {}
    /\ checkpointGeneration = [c \in Candidates |-> 1]
    /\ checkpointCursor = [c \in Candidates |-> 0]
    /\ checkpointProcessedCount = [c \in Candidates |-> 0]
    /\ checkpointState = [c \in Candidates |-> "OPEN"]
    /\ checkpointUpdatedAt = [c \in Candidates |-> 0]
    /\ finalizationBatchReceipts = {}
    /\ batchStartGeneration = [batch \in FinalizeIds |-> 1]
    /\ batchStartCursor = [batch \in FinalizeIds |-> 0]
    /\ batchNextCursor = [batch \in FinalizeIds |-> 0]
    /\ batchStartProcessedCount = [batch \in FinalizeIds |-> 0]
    /\ batchNextProcessedCount = [batch \in FinalizeIds |-> 0]
    /\ batchRowIncluded = [batch \in FinalizeIds |-> FALSE]
    /\ batchTerminal = [batch \in FinalizeIds |-> FALSE]
    /\ batchCommittedGeneration = [batch \in FinalizeIds |-> 1]
    /\ batchCommittedAt = [batch \in FinalizeIds |-> 0]
    /\ batchNextState = [batch \in FinalizeIds |-> "OPEN"]
    /\ finalizeAttempt = NoResponse
    /\ finalizeIncludeRow = FALSE
    /\ finalizeTerminal = FALSE
    /\ releasedFinalizationBatches = {}
    /\ txCandidate = NoCandidate
    /\ txPrefix = 0
    /\ lastEvent = "INIT"
    /\ txStartSnapshot = DurableFingerprint
    /\ replaySnapshot = DurableFingerprint
    /\ staleSnapshot = DurableFingerprint
    /\ crashSnapshot = DurableFingerprint

PrepareCatalogDescriptor(c, hasOptionalRow) ==
    /\ processUp
    /\ txCandidate = NoCandidate
    /\ lostResponse = NoResponse
    /\ CandidatePhase(c) = "EMPTY"
    /\ c \notin preparedCatalogDescriptors
    /\ hasOptionalRow \in BOOLEAN
    /\ preparedCatalogDescriptors' = preparedCatalogDescriptors \cup {c}
    /\ preparedOperationalEffects' = preparedOperationalEffects \cup {c}
    /\ preparedOptionalRows' =
        IF hasOptionalRow THEN preparedOptionalRows \cup {c}
        ELSE preparedOptionalRows
    /\ candidateDefinitions' = candidateDefinitions \cup {c}
    /\ candidateBaseReceipt' =
        [candidateBaseReceipt EXCEPT ![c] = commonHeadReceipt]
    /\ lastEvent' = "PREPARE_CATALOG_DESCRIPTOR"
    /\ UNCHANGED
        <<projectionSeals, CommitVariables, FinalizationVariables,
          TransactionVariables, ClientVariables, txStartSnapshot,
          replaySnapshot, staleSnapshot, crashSnapshot>>

SealCandidate(c) ==
    /\ processUp
    /\ txCandidate = NoCandidate
    /\ lostResponse = NoResponse
    /\ CandidatePhase(c) = "OPEN"
    /\ c \in preparedCatalogDescriptors
    /\ c \in preparedOperationalEffects
    /\ projectionSeals' = projectionSeals \cup {c}
    /\ lastEvent' = "SEAL_CANDIDATE"
    /\ UNCHANGED
        <<PreparationVariables, candidateDefinitions, candidateBaseReceipt,
          CommitVariables, FinalizationVariables,
          TransactionVariables, ClientVariables, txStartSnapshot,
          replaySnapshot, staleSnapshot, crashSnapshot>>

BeginPublish(c) ==
    /\ processUp
    /\ txCandidate = NoCandidate
    /\ lostResponse = NoResponse
    /\ PublicationReady(c)
    /\ txCandidate' = c
    /\ txPrefix' = 0
    /\ txStartSnapshot' = DurableFingerprint
    /\ lastEvent' = "PUBLISH_BEGIN"
    /\ UNCHANGED
        <<DatabaseVariables, ClientVariables,
          replaySnapshot, staleSnapshot, crashSnapshot>>

StagePublishStatement ==
    /\ processUp
    /\ txCandidate \in Candidates
    /\ txPrefix < PublishStatementCount
    /\ txPrefix' = txPrefix + 1
    /\ lastEvent' = "PUBLISH_STATEMENT"
    /\ UNCHANGED
        <<DatabaseVariables, txCandidate, ClientVariables,
          txStartSnapshot, replaySnapshot, staleSnapshot, crashSnapshot>>

CommitPublish(responseLost) ==
    LET c == txCandidate
        generation == BaseGeneration(c) + 1
    IN /\ processUp
       /\ c \in Candidates
       /\ txPrefix = PublishStatementCount
       /\ lostResponse = NoResponse
       /\ responseLost \in BOOLEAN
       /\ PublicationReady(c)
       /\ sourceDescriptors' = sourceDescriptors \cup {SourceRevision(c)}
       /\ commitAnchors' = commitAnchors \cup {c}
       /\ commitMemberRows' = commitMemberRows \cup CommitRowsFor(c)
       /\ commitSeals' = commitSeals \cup {c}
       /\ commitGeneration' = [commitGeneration EXCEPT ![c] = generation]
       /\ generationNodes' = generationNodes \cup {generation}
       /\ generationSuccessors' =
            generationSuccessors \cup {<<generation, generation - 1>>}
       /\ commonHeadReceipt' = c
       /\ finalizationCheckpoints' = finalizationCheckpoints \cup {c}
       /\ checkpointGeneration' =
            [checkpointGeneration EXCEPT ![c] = 1]
       /\ checkpointCursor' = [checkpointCursor EXCEPT ![c] = 0]
       /\ checkpointProcessedCount' =
            [checkpointProcessedCount EXCEPT ![c] = 0]
       /\ checkpointState' = [checkpointState EXCEPT ![c] = "OPEN"]
       /\ checkpointUpdatedAt' =
            [checkpointUpdatedAt EXCEPT ![c] = generation]
       /\ txCandidate' = NoCandidate
       /\ txPrefix' = 0
       /\ lostResponse' =
            IF responseLost THEN PublishRequest(c) ELSE NoResponse
       /\ lastEvent' =
            IF responseLost THEN "PUBLISH_RESPONSE_LOST" ELSE "PUBLISH_COMMIT"
       /\ UNCHANGED
            <<PreparationVariables, CandidateVariables,
              finalizedOptionalRows, receiptFinalizations, BatchVariables,
              processUp, FinalizerControlVariables, txStartSnapshot,
              replaySnapshot, staleSnapshot, crashSnapshot>>

RejectStaleHeadCAS(c) ==
    /\ processUp
    /\ txCandidate = NoCandidate
    /\ CandidatePhase(c) = "SEALED"
    /\ candidateBaseReceipt[c] # commonHeadReceipt
    /\ lastEvent' = "STALE_HEAD_CAS_REJECTED"
    /\ staleSnapshot' = DurableFingerprint
    /\ UNCHANGED
        <<DatabaseVariables, TransactionVariables, ClientVariables,
          txStartSnapshot, replaySnapshot, crashSnapshot>>

ReplayPublication(c, responseLost) ==
    /\ processUp
    /\ txCandidate = NoCandidate
    /\ lostResponse = PublishRequest(c)
    /\ responseLost \in BOOLEAN
    /\ ExactCommit(c)
    /\ CandidatePhase(c) = "PUBLISHED"
    /\ lostResponse' = IF responseLost THEN PublishRequest(c) ELSE NoResponse
    /\ lastEvent' =
        IF responseLost THEN "PUBLISH_REPLAY_RESPONSE_LOST"
        ELSE "PUBLISH_REPLAY"
    /\ replaySnapshot' = DurableFingerprint
    /\ UNCHANGED
        <<DatabaseVariables, TransactionVariables, processUp,
          FinalizerControlVariables, txStartSnapshot,
          staleSnapshot, crashSnapshot>>

CrashDuringPublish ==
    /\ processUp
    /\ txCandidate \in Candidates
    /\ processUp' = FALSE
    /\ txCandidate' = NoCandidate
    /\ txPrefix' = 0
    /\ lastEvent' = "PUBLISH_PREFIX_CRASH"
    /\ crashSnapshot' = txStartSnapshot
    /\ UNCHANGED
        <<DatabaseVariables, lostResponse, FinalizerControlVariables,
          txStartSnapshot,
          replaySnapshot, staleSnapshot>>

CrashIdle ==
    /\ processUp
    /\ txCandidate = NoCandidate
    /\ processUp' = FALSE
    /\ finalizeAttempt' = NoResponse
    /\ finalizeIncludeRow' = FALSE
    /\ finalizeTerminal' = FALSE
    /\ lastEvent' = "IDLE_CRASH"
    /\ UNCHANGED
        <<DatabaseVariables, TransactionVariables, lostResponse,
          releasedFinalizationBatches,
          txStartSnapshot, replaySnapshot, staleSnapshot, crashSnapshot>>

Restart ==
    /\ ~processUp
    /\ processUp' = TRUE
    /\ lastEvent' = "RESTART"
    /\ UNCHANGED
        <<DatabaseVariables, TransactionVariables, lostResponse,
          FinalizerControlVariables,
          txStartSnapshot, replaySnapshot, staleSnapshot, crashSnapshot>>

IssueFinalization(c, key, includeRow, terminal) ==
    LET batch == FinalizeId(c, key)
        committedGeneration == checkpointGeneration[c] + 1
    IN /\ processUp
       /\ txCandidate = NoCandidate
       /\ lostResponse = NoResponse
       /\ finalizeAttempt = NoResponse
       /\ ExactCommit(c)
       /\ CandidatePhase(c) = "PUBLISHED"
       /\ c \in finalizationCheckpoints
       /\ c \notin receiptFinalizations
       /\ checkpointState[c] = "OPEN"
       /\ batch \notin finalizationBatchReceipts
       /\ includeRow \in BOOLEAN
       /\ terminal \in BOOLEAN
       /\ includeRow \/ terminal
       /\ (includeRow =>
            /\ c \in preparedOptionalRows
            /\ c \notin finalizedOptionalRows)
       /\ (terminal =>
            /\ ~includeRow
            /\ (c \in preparedOptionalRows <=> c \in finalizedOptionalRows))
       /\ committedGeneration \in FinalizationGenerations
       /\ finalizeAttempt' = batch
       /\ finalizeIncludeRow' = includeRow
       /\ finalizeTerminal' = terminal
       /\ lastEvent' = "FINALIZE_ISSUE"
       /\ UNCHANGED
            <<DatabaseVariables, TransactionVariables, processUp, lostResponse,
              releasedFinalizationBatches, txStartSnapshot, replaySnapshot,
              staleSnapshot, crashSnapshot>>

ReleaseFinalization ==
    /\ processUp
    /\ txCandidate = NoCandidate
    /\ lostResponse = NoResponse
    /\ finalizeAttempt \in FinalizeIds
    /\ finalizeIncludeRow
    /\ ~finalizeTerminal
    /\ releasedFinalizationBatches' =
        releasedFinalizationBatches \cup {finalizeAttempt}
    /\ lastEvent' = "FINALIZE_EXTERNAL_RELEASE"
    /\ UNCHANGED
        <<DatabaseVariables, TransactionVariables, processUp, lostResponse,
          finalizeAttempt, finalizeIncludeRow, finalizeTerminal,
          txStartSnapshot, replaySnapshot, staleSnapshot, crashSnapshot>>

CommitFinalization(responseLost) ==
    LET batch == finalizeAttempt
        c == batch[1]
        key == batch[2]
        startGeneration == checkpointGeneration[c]
        committedGeneration == startGeneration + 1
        startCursor == checkpointCursor[c]
        startProcessed == checkpointProcessedCount[c]
        includeRow == finalizeIncludeRow
        terminal == finalizeTerminal
        nextProcessed == startProcessed + IF includeRow THEN 1 ELSE 0
        nextCursor == IF includeRow THEN nextProcessed ELSE startCursor
        nextState == IF terminal THEN "COMPLETE" ELSE "OPEN"
        committedAt == commitGeneration[c] + committedGeneration
    IN /\ processUp
       /\ txCandidate = NoCandidate
       /\ lostResponse = NoResponse
       /\ batch \in FinalizeIds
       /\ ExactCommit(c)
       /\ CandidatePhase(c) = "PUBLISHED"
       /\ c \in finalizationCheckpoints
       /\ c \notin receiptFinalizations
       /\ checkpointState[c] = "OPEN"
       /\ batch \notin finalizationBatchReceipts
       /\ responseLost \in BOOLEAN
       /\ (includeRow => batch \in releasedFinalizationBatches)
       /\ (includeRow =>
            /\ c \in preparedOptionalRows
            /\ c \notin finalizedOptionalRows)
       /\ (terminal =>
            /\ ~includeRow
            /\ (c \in preparedOptionalRows <=> c \in finalizedOptionalRows))
       /\ committedGeneration \in FinalizationGenerations
       /\ finalizedOptionalRows' =
            IF includeRow THEN finalizedOptionalRows \cup {c}
            ELSE finalizedOptionalRows
       /\ finalizationCheckpoints' = finalizationCheckpoints \cup {c}
       /\ checkpointGeneration' =
            [checkpointGeneration EXCEPT ![c] = committedGeneration]
       /\ checkpointCursor' =
            [checkpointCursor EXCEPT ![c] = nextCursor]
       /\ checkpointProcessedCount' =
            [checkpointProcessedCount EXCEPT ![c] = nextProcessed]
       /\ checkpointState' = [checkpointState EXCEPT ![c] = nextState]
       /\ checkpointUpdatedAt' =
            [checkpointUpdatedAt EXCEPT ![c] = committedAt]
       /\ finalizationBatchReceipts' = finalizationBatchReceipts \cup {batch}
       /\ batchStartGeneration' =
            [batchStartGeneration EXCEPT ![batch] = startGeneration]
       /\ batchStartCursor' =
            [batchStartCursor EXCEPT ![batch] = startCursor]
       /\ batchNextCursor' =
            [batchNextCursor EXCEPT ![batch] = nextCursor]
       /\ batchStartProcessedCount' =
            [batchStartProcessedCount EXCEPT ![batch] = startProcessed]
       /\ batchNextProcessedCount' =
            [batchNextProcessedCount EXCEPT ![batch] = nextProcessed]
       /\ batchRowIncluded' =
            [batchRowIncluded EXCEPT ![batch] = includeRow]
       /\ batchTerminal' = [batchTerminal EXCEPT ![batch] = terminal]
       /\ batchCommittedGeneration' =
            [batchCommittedGeneration EXCEPT ![batch] = committedGeneration]
       /\ batchCommittedAt' =
            [batchCommittedAt EXCEPT ![batch] = committedAt]
       /\ batchNextState' = [batchNextState EXCEPT ![batch] = nextState]
       /\ receiptFinalizations' =
            IF terminal THEN receiptFinalizations \cup {c}
            ELSE receiptFinalizations
       /\ finalizeAttempt' = NoResponse
       /\ finalizeIncludeRow' = FALSE
       /\ finalizeTerminal' = FALSE
       /\ lostResponse' =
            IF responseLost THEN FinalizeRequest(c, key) ELSE NoResponse
       /\ lastEvent' =
            IF responseLost THEN "FINALIZE_RESPONSE_LOST"
            ELSE "FINALIZE_COMMIT"
       /\ UNCHANGED
            <<PreparationVariables, CandidateVariables, CommitVariables,
              TransactionVariables, processUp, releasedFinalizationBatches,
              txStartSnapshot,
              replaySnapshot, staleSnapshot, crashSnapshot>>

ReplayFinalization(c, key, responseLost) ==
    LET batch == FinalizeId(c, key)
    IN /\ processUp
       /\ txCandidate = NoCandidate
       /\ lostResponse = FinalizeRequest(c, key)
       /\ batch \in finalizationBatchReceipts
       /\ responseLost \in BOOLEAN
       /\ lostResponse' =
            IF responseLost THEN FinalizeRequest(c, key) ELSE NoResponse
       /\ lastEvent' =
            IF responseLost THEN "FINALIZE_REPLAY_RESPONSE_LOST"
            ELSE "FINALIZE_REPLAY"
       /\ replaySnapshot' = DurableFingerprint
       /\ UNCHANGED
            <<DatabaseVariables, TransactionVariables, processUp,
              FinalizerControlVariables, txStartSnapshot,
              staleSnapshot, crashSnapshot>>

CleanupCandidate(c) ==
    /\ processUp
    /\ txCandidate = NoCandidate
    /\ finalizeAttempt = NoResponse
    /\ CandidatePhase(c) = "PUBLISHED"
    /\ c \in receiptFinalizations
    /\ c \in candidateDefinitions
    /\ candidateDefinitions' = candidateDefinitions \ {c}
    /\ projectionSeals' = projectionSeals \ {c}
    /\ candidateBaseReceipt' =
        [candidateBaseReceipt EXCEPT ![c] = NoCandidate]
    /\ preparedCatalogDescriptors' = preparedCatalogDescriptors \ {c}
    /\ preparedOperationalEffects' = preparedOperationalEffects \ {c}
    /\ preparedOptionalRows' = preparedOptionalRows \ {c}
    /\ lastEvent' = "CANDIDATE_CLEANUP"
    /\ UNCHANGED
        <<CommitVariables, FinalizationVariables, TransactionVariables,
          ClientVariables, txStartSnapshot,
          replaySnapshot, staleSnapshot, crashSnapshot>>

Next ==
    \/ \E c \in Candidates, hasOptionalRow \in BOOLEAN :
        PrepareCatalogDescriptor(c, hasOptionalRow)
    \/ \E c \in Candidates : SealCandidate(c)
    \/ \E c \in Candidates : BeginPublish(c)
    \/ StagePublishStatement
    \/ \E responseLost \in BOOLEAN : CommitPublish(responseLost)
    \/ \E c \in Candidates : RejectStaleHeadCAS(c)
    \/ \E c \in Candidates, responseLost \in BOOLEAN :
        ReplayPublication(c, responseLost)
    \/ CrashDuringPublish
    \/ CrashIdle
    \/ Restart
    \/ \E c \in Candidates, key \in BatchKeys,
          includeRow \in BOOLEAN, terminal \in BOOLEAN :
        IssueFinalization(c, key, includeRow, terminal)
    \/ ReleaseFinalization
    \/ \E responseLost \in BOOLEAN : CommitFinalization(responseLost)
    \/ \E c \in Candidates, key \in BatchKeys,
          responseLost \in BOOLEAN :
        ReplayFinalization(c, key, responseLost)
    \/ \E c \in Candidates : CleanupCandidate(c)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ processUp \in BOOLEAN
    /\ lostResponse \in Requests \cup {NoResponse}
    /\ preparedCatalogDescriptors \subseteq Candidates
    /\ preparedOperationalEffects \subseteq Candidates
    /\ preparedOptionalRows \subseteq Candidates
    /\ candidateDefinitions \subseteq Candidates
    /\ projectionSeals \subseteq Candidates
    /\ \A c \in Candidates : CandidatePhase(c) \in CandidatePhases
    /\ candidateBaseReceipt \in [Candidates -> Candidates \cup {NoCandidate}]
    /\ sourceDescriptors
        \subseteq {SourceRevision(c) : c \in Candidates}
    /\ commitAnchors \subseteq Candidates
    /\ commitMemberRows \subseteq Candidates \X CommitMembers
    /\ commitSeals \subseteq Candidates
    /\ commitGeneration \in [Candidates -> 0..MaxGeneration]
    /\ generationNodes \subseteq NodeGenerations
    /\ generationSuccessors \subseteq Generations \X NodeGenerations
    /\ commonHeadReceipt \in Candidates \cup {NoCandidate}
    /\ finalizedOptionalRows \subseteq Candidates
    /\ receiptFinalizations \subseteq Candidates
    /\ finalizationCheckpoints \subseteq Candidates
    /\ checkpointGeneration
        \in [Candidates -> 1..MaxFinalizationGeneration]
    /\ checkpointCursor \in [Candidates -> 0..1]
    /\ checkpointProcessedCount \in [Candidates -> 0..1]
    /\ checkpointState \in [Candidates -> CheckpointStates]
    /\ checkpointUpdatedAt \in [Candidates -> Times]
    /\ finalizationBatchReceipts \subseteq FinalizeIds
    /\ batchStartGeneration
        \in [FinalizeIds -> 1..MaxFinalizationGeneration]
    /\ batchStartCursor \in [FinalizeIds -> 0..1]
    /\ batchNextCursor \in [FinalizeIds -> 0..1]
    /\ batchStartProcessedCount \in [FinalizeIds -> 0..1]
    /\ batchNextProcessedCount \in [FinalizeIds -> 0..1]
    /\ batchRowIncluded \in [FinalizeIds -> BOOLEAN]
    /\ batchTerminal \in [FinalizeIds -> BOOLEAN]
    /\ batchCommittedGeneration
        \in [FinalizeIds -> 1..MaxFinalizationGeneration]
    /\ batchCommittedAt \in [FinalizeIds -> Times]
    /\ batchNextState \in [FinalizeIds -> CheckpointStates]
    /\ finalizeAttempt \in FinalizeIds \cup {NoResponse}
    /\ finalizeIncludeRow \in BOOLEAN
    /\ finalizeTerminal \in BOOLEAN
    /\ releasedFinalizationBatches \subseteq FinalizeIds
    /\ txCandidate \in Candidates \cup {NoCandidate}
    /\ txPrefix \in 0..PublishStatementCount
    /\ lastEvent \in EventKinds

CommitSealIsTotal ==
    \A c \in Candidates : c \in commitSeals <=> ExactCommit(c)

CommitHasThirteenMandatoryMembers ==
    /\ Cardinality(CommitMembers) = 13
    /\ \A c \in commitSeals : Cardinality(CommitRowsFor(c)) = 13

CommitEquivalentKeysAreUnique ==
    \A left, right \in commitSeals :
        \/ left = right
        \/ /\ SourceRevision(left) # SourceRevision(right)
           /\ CatalogRevision(left) # CatalogRevision(right)
           /\ Preparation(left) # Preparation(right)
           /\ commitGeneration[left] # commitGeneration[right]

GenerationChainIsExact ==
    /\ generationNodes =
        {0} \cup {commitGeneration[c] : c \in commitSeals}
    /\ generationSuccessors = {
        <<commitGeneration[c], commitGeneration[c] - 1>> : c \in commitSeals
       }
    /\ \A edge \in generationSuccessors : edge[1] = edge[2] + 1

CommonHeadIsMaximumTip ==
    IF commitSeals = {}
    THEN commonHeadReceipt = NoCandidate
    ELSE /\ commonHeadReceipt \in commitSeals
         /\ \A c \in commitSeals :
            commitGeneration[c] <= commitGeneration[commonHeadReceipt]
         /\ ~\E edge \in generationSuccessors :
            edge[2] = commitGeneration[commonHeadReceipt]

PublishedCandidateIffSealedCommonCommit ==
    \A c \in Candidates :
        CandidatePhase(c) = "PUBLISHED" <=> c \in commitSeals

CandidateLifecycleIsGraphDerived ==
    /\ projectionSeals \subseteq candidateDefinitions
    /\ candidateDefinitions = preparedCatalogDescriptors
    /\ candidateDefinitions = preparedOperationalEffects
    /\ \A c \in Candidates :
        /\ (CandidatePhase(c) = "EMPTY" <=>
            c \notin candidateDefinitions /\ c \notin commitSeals)
        /\ (CandidatePhase(c) = "OPEN" <=>
            c \in candidateDefinitions /\ c \notin projectionSeals)
        /\ (CandidatePhase(c) = "SEALED" <=>
            c \in projectionSeals /\ c \notin commitSeals)

CommitOwnsPermanentCheckpoint ==
    finalizationCheckpoints = commitSeals

DescriptorIsNotPublishedBeforeCommitSeal ==
    /\ PublishedSourceRevisions =
        {SourceRevision(c) : c \in commitSeals}
    /\ PublishedCatalogRevisions =
        {CatalogRevision(c) : c \in commitSeals}

DerivedActivationIffSealedCommit ==
    \A c \in Candidates :
        c \in commitSeals
        <=> <<SourceRevision(c), Preparation(c), OperationalPolicy(c),
              commitGeneration[c]>> \in DerivedActivations

NoTornPublicVisibility ==
    /\ CommitSealIsTotal
    /\ PublishedCandidateIffSealedCommonCommit
    /\ CandidateLifecycleIsGraphDerived
    /\ CommitOwnsPermanentCheckpoint
    /\ DescriptorIsNotPublishedBeforeCommitSeal
    /\ DerivedActivationIffSealedCommit

ReplayIsObservational ==
    lastEvent \in {"PUBLISH_REPLAY", "PUBLISH_REPLAY_RESPONSE_LOST",
                    "FINALIZE_REPLAY", "FINALIZE_REPLAY_RESPONSE_LOST"}
    => DurableFingerprint = replaySnapshot

StaleHeadCASHasZeroDurableWrites ==
    lastEvent = "STALE_HEAD_CAS_REJECTED"
    => DurableFingerprint = staleSnapshot

TransactionPrefixIsRollbackOnly ==
    txCandidate \in Candidates => DurableFingerprint = txStartSnapshot

StatementPrefixCrashRollsBack ==
    lastEvent = "PUBLISH_PREFIX_CRASH"
    => DurableFingerprint = crashSnapshot

FinalizationBatchEquations ==
    \A batch \in finalizationBatchReceipts :
        /\ batch[1] \in commitSeals
        /\ batchCommittedGeneration[batch] = batchStartGeneration[batch] + 1
        /\ batchNextProcessedCount[batch] =
            batchStartProcessedCount[batch]
            + IF batchRowIncluded[batch] THEN 1 ELSE 0
        /\ batchNextCursor[batch] =
            IF batchRowIncluded[batch]
            THEN batchNextProcessedCount[batch]
            ELSE batchStartCursor[batch]
        /\ (batchTerminal[batch] => ~batchRowIncluded[batch])
        /\ (batchTerminal[batch] =>
            batchStartCursor[batch] = batchNextCursor[batch])
        /\ (~batchTerminal[batch] => batchRowIncluded[batch])
        /\ batchNextState[batch] =
            IF batchTerminal[batch] THEN "COMPLETE" ELSE "OPEN"

ExternalReleasePrecedesCommittedRow ==
    \A batch \in finalizationBatchReceipts :
        batchRowIncluded[batch] => batch \in releasedFinalizationBatches

FinalizationIsAppendOnlyAndDerived ==
    /\ receiptFinalizations \subseteq commitSeals
    /\ \A c \in Candidates :
        /\ (ReceiptState(c) = "PROJECTION_FINALIZED"
            <=> c \in receiptFinalizations)
        /\ (c \in receiptFinalizations <=>
            \E batch \in FinalizationBatchesFor(c) :
                ExactTerminalBatch(c, batch))
        /\ (c \in receiptFinalizations =>
            /\ ReceiptFinalizedAt(c) # 0
            /\ checkpointState[c] = "COMPLETE"
            /\ checkpointUpdatedAt[c] = ReceiptFinalizedAt(c))

FinalizationCoherence ==
    /\ FinalizationBatchEquations
    /\ ExternalReleasePrecedesCommittedRow
    /\ FinalizationIsAppendOnlyAndDerived
    /\ \A c \in finalizationCheckpoints :
        LET batches == FinalizationBatchesFor(c)
        IN IF batches = {}
           THEN /\ checkpointGeneration[c] = 1
                /\ checkpointCursor[c] = 0
                /\ checkpointProcessedCount[c] = 0
                /\ checkpointState[c] = "OPEN"
                /\ checkpointUpdatedAt[c] = commitGeneration[c]
                /\ c \notin receiptFinalizations
           ELSE \E batch \in batches :
                /\ LatestFinalizationBatch(c, batch)
                /\ CheckpointMatchesBatch(c, batch)
                /\ \A other \in batches :
                    LatestFinalizationBatch(c, other) => other = batch

FinalizationReplaySurvivesCandidateCleanup ==
    \A c \in receiptFinalizations :
        c \notin candidateDefinitions =>
            /\ c \in commitSeals
            /\ c \in finalizationCheckpoints
            /\ \E batch \in FinalizationBatchesFor(c) :
                ExactTerminalBatch(c, batch)

ResponseLossHasPermanentReplayAuthority ==
    /\ \A c \in Candidates :
        lostResponse = PublishRequest(c) => ExactCommit(c)
    /\ \A c \in Candidates, key \in BatchKeys :
        lostResponse = FinalizeRequest(c, key) =>
            FinalizeId(c, key) \in finalizationBatchReceipts

SafetyView ==
    <<DatabaseVariables, TransactionVariables, ClientVariables, lastEvent>>

=============================================================================
