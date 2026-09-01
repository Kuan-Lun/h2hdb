------------------------- MODULE CanonicalPlanCursor -------------------------
EXTENDS Naturals, FiniteSets, Sequences, TLC

(***************************************************************************
Finite refinement model for publication canonical-plan cursor caching.

Plan is the immutable candidate/digest order used by both selectors.
ConsumerPlan is the independently ordered durable BUILD-child authority: a
BUILD transaction atomically consumes the exact-generation claim of its next
first consumer and advances buildCheckpoint.  A value before that checkpoint
is complete with an exact sealed preimage even though its consumed claim is
absent.  VALIDATE is entered only after BUILD is terminal and independently
checks every sealed preimage without recreating claims.

The reference selector starts at Plan[1] on every issue.  The optimized
selector starts at a process-local cursor only while its stage and generation
cache key still match.  It advances solely across entries observed complete
by a fresh durable read and full preimage stream.  A crash discards the cache.
An ingest-fence change makes the cache key stale and the next issue restarts
at Plan[1].

A scalar sealed receipt is deliberately not byte authority.  The preimage
state is independent, and an adversarial step can change bytes while retaining
the exact scalar receipt.  Every issue of an active sealed ALLOCATE performs a
new full stream; abort/retry performs it again.  The model has no stream-skipping
memo.  The successful-progress accounting therefore bounds completed-prefix
validations plus successful required allocations, while arbitrary aborted
retry work is explicitly outside that bound.

Two prepared copies of the same sealed ALLOCATE may coexist.  Each copy carries
the result of its prepare-time full stream.  The bounded model fixes the
candidate plan and assumes supported writers keep sealed receipt/preimage bytes
immutable while prepared work is outstanding; the corruption action is
therefore enabled only when no prepared work exists.  Commit does not stream
again: it freshly fences only generation, BUILD stage, and the first-consumer
checkpoint.  Once consumed or stale the delayed copy rejects with zero
publication writes, so it cannot resurrect the deleted claim.

Each ordinary action uses a transaction-local statement prefix.  Prefix crash
changes no modeled publication state.  An exact committed action may lose its
response and replay observationally.  Reference and optimized durable worlds
are updated independently so their agreement is an invariant rather than an
assumption of CommitAction.

TLC exhausts only the finite companion Small profile.  This is bounded safety
evidence, not an unbounded proof.  It does not establish liveness or refinement
by Python, SQL, hashing, transaction isolation, SQLite, or MariaDB.  The Lean
companion supplies the unbounded selector and linear successful-progress
theorems over its explicit assumptions.
***************************************************************************)

CONSTANTS FirstValue, SecondValue, NoValue, NoAction, RejectAction,
          MaxGeneration, TxStatementCount

Values == {FirstValue, SecondValue}
Plan == <<FirstValue, SecondValue>>
ConsumerPlan == <<SecondValue, FirstValue>>

ASSUME /\ FirstValue # SecondValue
       /\ NoValue \notin Values
       /\ NoAction # RejectAction
       /\ MaxGeneration \in Nat \ {0}
       /\ TxStatementCount \in Nat \ {0}

Positions == 1..Len(Plan)
Generations == 1..MaxGeneration
Stages == {"BUILD", "VALIDATE", "DONE"}
CacheStages == Stages \cup {"NONE"}
StoragePhases == 0..2
ReceiptStates == 0..2
NoReceipt == 0
ExactReceipt == 1
ChangedReceipt == 2
PreimageStates == 0..2
NoPreimage == 0
ExactPreimage == 1
ChangedPreimage == 2
ActionKinds == {
    "ALLOCATE", "PAGE", "SEAL", "CONSUME",
    "START_VALIDATE", "COMPLETE"
}
CanonicalActionKinds == {"ALLOCATE", "PAGE", "SEAL"}
Actions ==
    (CanonicalActionKinds \X Values)
    \cup ({"CONSUME"} \X Values)
    \cup {
        <<"START_VALIDATE", NoValue>>,
        <<"COMPLETE", NoValue>>
    }
Decisions == Actions \cup {NoAction, RejectAction}
NoRequest == <<>>
Requests == Generations \X Actions

EventKinds == {
    "INIT", "ISSUE", "TX_STATEMENT", "COMMIT",
    "COMMIT_RESPONSE_LOST", "REPLAY", "REPLAY_RESPONSE_LOST",
    "FENCE_ADVANCE", "STALE_FENCE_REJECTED", "CRASH", "RESTART",
    "TX_ABORT", "CONCURRENT_ALLOCATE_PREPARED",
    "DELAYED_ALLOCATE_COMMIT", "DELAYED_ALLOCATE_REJECTED",
    "DELAYED_ALLOCATE_AFTER_CONSUMER_REJECTED",
    "SAME_RECEIPT_PREIMAGE_CORRUPTED",
    "TERMINAL_OBSERVED", "REJECTION_OBSERVED"
}

VARIABLES
    processUp, currentGeneration,
    stageRef, stageOpt, buildCheckpointRef, buildCheckpointOpt,
    storageRef, storageOpt,
    receiptRef, receiptOpt, preimageRef, preimageOpt,
    claimsRef, claimsOpt,
    pendingRef, pendingOpt, pendingGeneration,
    pendingFullStreamRef, pendingFullStreamOpt, txPrefix,
    delayedRef, delayedOpt, delayedGeneration,
    delayedFullStreamRef, delayedFullStreamOpt,
    lostRef, lostOpt,
    cacheStage, cacheGeneration, cursor,
    validatedPositions, validationCount, successfulRequiredAllocations,
    lastEvent, lastFaultValue,
    txStartSnapshot, replaySnapshot, staleSnapshot,
    delayedRejectSnapshot, crashSnapshot

ReferenceDatabaseVariables ==
    <<stageRef, buildCheckpointRef,
      storageRef, receiptRef, preimageRef, claimsRef>>
OptimizedDatabaseVariables ==
    <<stageOpt, buildCheckpointOpt,
      storageOpt, receiptOpt, preimageOpt, claimsOpt>>
DatabaseVariables ==
    <<ReferenceDatabaseVariables, OptimizedDatabaseVariables>>
TransactionVariables ==
    <<pendingRef, pendingOpt, pendingGeneration,
      pendingFullStreamRef, pendingFullStreamOpt, txPrefix>>
DelayedVariables ==
    <<delayedRef, delayedOpt, delayedGeneration,
      delayedFullStreamRef, delayedFullStreamOpt>>
ClientVariables ==
    <<processUp, lostRef, lostOpt,
      cacheStage, cacheGeneration, cursor,
      validatedPositions, validationCount, successfulRequiredAllocations>>
AuditVariables ==
    <<lastEvent, lastFaultValue,
      txStartSnapshot, replaySnapshot, staleSnapshot,
      delayedRejectSnapshot, crashSnapshot>>
vars ==
    <<currentGeneration, DatabaseVariables, TransactionVariables,
      DelayedVariables, ClientVariables, AuditVariables>>

DurableFingerprint == DatabaseVariables

ConsumerPosition(value) ==
    CHOOSE position \in Positions : ConsumerPlan[position] = value

HasCurrentClaim(claims, generation, value) ==
    <<generation, value>> \in claims

ConsumerCommitted(stage, checkpoint, value) ==
    \/ stage = "VALIDATE"
    \/ stage = "DONE"
    \/ ConsumerPosition(value) <= checkpoint

EntryComplete(
        stage, checkpoint, storage, receipts, preimages,
        claims, generation, value) ==
    /\ storage[value] = 2
    /\ receipts[value] = ExactReceipt
    /\ preimages[value] = ExactPreimage
    /\ \/ ConsumerCommitted(stage, checkpoint, value)
       \/ HasCurrentClaim(claims, generation, value)

IncompletePositionsFrom(
        start, stage, checkpoint, storage, receipts, preimages,
        claims, generation) ==
    {position \in start..Len(Plan) :
        ~EntryComplete(
            stage, checkpoint, storage, receipts, preimages,
            claims, generation, Plan[position])}

FirstIncompleteFrom(
        start, stage, checkpoint, storage, receipts, preimages,
        claims, generation) ==
    LET positions == IncompletePositionsFrom(
            start, stage, checkpoint, storage, receipts, preimages,
            claims, generation)
    IN CHOOSE position \in positions :
        \A other \in positions : position <= other

ScanCursor(
        start, stage, checkpoint, storage, receipts, preimages,
        claims, generation) ==
    LET positions == IncompletePositionsFrom(
            start, stage, checkpoint, storage, receipts, preimages,
            claims, generation)
    IN IF positions = {}
       THEN Len(Plan) + 1
       ELSE FirstIncompleteFrom(
            start, stage, checkpoint, storage, receipts, preimages,
            claims, generation)

EntryDecision(
        stage, checkpoint, storage, receipts, preimages,
        claims, generation, value) ==
    IF storage[value] = 2 /\
        \/ receipts[value] # ExactReceipt
        \/ preimages[value] # ExactPreimage
    THEN RejectAction
    ELSE IF ConsumerCommitted(stage, checkpoint, value)
    THEN RejectAction
    ELSE IF ~HasCurrentClaim(claims, generation, value)
         THEN <<"ALLOCATE", value>>
         ELSE IF storage[value] = 0
              THEN <<"PAGE", value>>
              ELSE IF storage[value] = 1
                   THEN <<"SEAL", value>>
                   ELSE NoAction

CanonicalDecisionFrom(
        start, stage, checkpoint, storage, receipts, preimages,
        claims, generation) ==
    LET nextCursor == ScanCursor(
            start, stage, checkpoint, storage, receipts, preimages,
            claims, generation)
    IN IF nextCursor = Len(Plan) + 1
       THEN NoAction
       ELSE EntryDecision(
            stage, checkpoint, storage, receipts, preimages,
            claims, generation, Plan[nextCursor])

FullDecisionFrom(
        start, stage, checkpoint, storage, receipts, preimages,
        claims, generation) ==
    LET canonical == CanonicalDecisionFrom(
            start, stage, checkpoint, storage, receipts, preimages,
            claims, generation)
    IN IF canonical # NoAction
       THEN canonical
       ELSE IF stage = "BUILD"
            THEN IF checkpoint < Len(ConsumerPlan)
                 THEN <<"CONSUME", ConsumerPlan[checkpoint + 1]>>
                 ELSE <<"START_VALIDATE", NoValue>>
            ELSE IF stage = "VALIDATE"
                 THEN <<"COMPLETE", NoValue>>
                 ELSE NoAction

CacheKeyMatches ==
    /\ cacheStage = stageOpt
    /\ cacheGeneration = currentGeneration

EffectiveCursor == IF CacheKeyMatches THEN cursor ELSE 1

ReferenceDecision ==
    FullDecisionFrom(
        1, stageRef, buildCheckpointRef,
        storageRef, receiptRef, preimageRef,
        claimsRef, currentGeneration)

OptimizedDecision ==
    FullDecisionFrom(
        EffectiveCursor, stageOpt, buildCheckpointOpt,
        storageOpt, receiptOpt, preimageOpt,
        claimsOpt, currentGeneration)

OptimizedScanCursor ==
    ScanCursor(
        EffectiveCursor, stageOpt, buildCheckpointOpt,
        storageOpt, receiptOpt, preimageOpt,
        claimsOpt, currentGeneration)

NewValidatedPositions == EffectiveCursor..(OptimizedScanCursor - 1)

ActionKind(action) == action[1]
ActionValue(action) == action[2]

SealedAllocate(storage, action) ==
    IF action \in Actions
    THEN /\ ActionKind(action) = "ALLOCATE"
         /\ storage[ActionValue(action)] = 2
    ELSE FALSE

PreparedStreamValid(storage, action, fullStreamValidated) ==
    IF SealedAllocate(storage, action)
    THEN fullStreamValidated
    ELSE TRUE

ActionEnabled(
        stage, checkpoint, storage, receipts, preimages,
        claims, generation, action) ==
    LET kind == ActionKind(action)
        value == ActionValue(action)
    IN CASE kind = "ALLOCATE" ->
                /\ value \in Values
                /\ ~ConsumerCommitted(stage, checkpoint, value)
                /\ ~HasCurrentClaim(claims, generation, value)
                /\ \/ storage[value] # 2
                   \/ /\ receipts[value] = ExactReceipt
                      /\ preimages[value] = ExactPreimage
       [] kind = "PAGE" ->
                /\ value \in Values
                /\ ~ConsumerCommitted(stage, checkpoint, value)
                /\ HasCurrentClaim(claims, generation, value)
                /\ storage[value] = 0
       [] kind = "SEAL" ->
                /\ value \in Values
                /\ ~ConsumerCommitted(stage, checkpoint, value)
                /\ HasCurrentClaim(claims, generation, value)
                /\ storage[value] = 1
                /\ receipts[value] = NoReceipt
                /\ preimages[value] = NoPreimage
       [] kind = "CONSUME" ->
                /\ stage = "BUILD"
                /\ checkpoint < Len(ConsumerPlan)
                /\ value = ConsumerPlan[checkpoint + 1]
                /\ storage[value] = 2
                /\ receipts[value] = ExactReceipt
                /\ preimages[value] = ExactPreimage
                /\ HasCurrentClaim(claims, generation, value)
       [] kind = "START_VALIDATE" ->
                /\ stage = "BUILD"
                /\ checkpoint = Len(ConsumerPlan)
                /\ value = NoValue
       [] kind = "COMPLETE" ->
                /\ stage = "VALIDATE"
                /\ value = NoValue
       [] OTHER -> FALSE

ApplyStorage(storage, action) ==
    LET kind == ActionKind(action)
        value == ActionValue(action)
    IN IF kind = "PAGE"
       THEN [storage EXCEPT ![value] = 1]
       ELSE IF kind = "SEAL"
            THEN [storage EXCEPT ![value] = 2]
            ELSE storage

ApplyReceipt(receipts, action) ==
    LET kind == ActionKind(action)
        value == ActionValue(action)
    IN IF kind = "SEAL"
       THEN [receipts EXCEPT ![value] = ExactReceipt]
       ELSE receipts

ApplyPreimage(preimages, action) ==
    LET kind == ActionKind(action)
        value == ActionValue(action)
    IN IF kind = "SEAL"
       THEN [preimages EXCEPT ![value] = ExactPreimage]
       ELSE preimages

ApplyClaims(claims, generation, action) ==
    LET kind == ActionKind(action)
        value == ActionValue(action)
    IN IF kind = "ALLOCATE"
       THEN claims \cup {<<generation, value>>}
       ELSE IF kind = "CONSUME"
            THEN claims \ {<<generation, value>>}
            ELSE claims

ApplyCheckpoint(checkpoint, action) ==
    IF ActionKind(action) = "CONSUME" THEN checkpoint + 1 ELSE checkpoint

ApplyStage(stage, action) ==
    IF ActionKind(action) = "START_VALIDATE"
    THEN "VALIDATE"
    ELSE IF ActionKind(action) = "COMPLETE" THEN "DONE" ELSE stage

DelayedAllocateCommitAdmissible(
        stage, checkpoint, storage, receipts, preimages,
        generation, action, preparedGeneration, fullStreamValidated) ==
    LET value == ActionValue(action)
    IN /\ action \in CanonicalActionKinds \X Values
       /\ ActionKind(action) = "ALLOCATE"
       /\ preparedGeneration = generation
       /\ stage = "BUILD"
       /\ ~ConsumerCommitted(stage, checkpoint, value)
       /\ storage[value] = 2
       /\ receipts[value] = ExactReceipt
       /\ preimages[value] = ExactPreimage
       /\ fullStreamValidated

Init ==
    /\ processUp = TRUE
    /\ currentGeneration = 1
    /\ stageRef = "BUILD"
    /\ stageOpt = "BUILD"
    /\ buildCheckpointRef = 0
    /\ buildCheckpointOpt = 0
    /\ storageRef = [value \in Values |-> 0]
    /\ storageOpt = [value \in Values |-> 0]
    /\ receiptRef = [value \in Values |-> NoReceipt]
    /\ receiptOpt = [value \in Values |-> NoReceipt]
    /\ preimageRef = [value \in Values |-> NoPreimage]
    /\ preimageOpt = [value \in Values |-> NoPreimage]
    /\ claimsRef = {}
    /\ claimsOpt = {}
    /\ pendingRef = NoAction
    /\ pendingOpt = NoAction
    /\ pendingGeneration = 1
    /\ pendingFullStreamRef = FALSE
    /\ pendingFullStreamOpt = FALSE
    /\ txPrefix = 0
    /\ delayedRef = NoAction
    /\ delayedOpt = NoAction
    /\ delayedGeneration = 1
    /\ delayedFullStreamRef = FALSE
    /\ delayedFullStreamOpt = FALSE
    /\ lostRef = NoRequest
    /\ lostOpt = NoRequest
    /\ cacheStage = "NONE"
    /\ cacheGeneration = 0
    /\ cursor = 1
    /\ validatedPositions = {}
    /\ validationCount = 0
    /\ successfulRequiredAllocations = 0
    /\ lastEvent = "INIT"
    /\ lastFaultValue = NoValue
    /\ txStartSnapshot = DurableFingerprint
    /\ replaySnapshot = DurableFingerprint
    /\ staleSnapshot = DurableFingerprint
    /\ delayedRejectSnapshot = DurableFingerprint
    /\ crashSnapshot = DurableFingerprint

IssueAction ==
    LET reference == ReferenceDecision
        optimized == OptimizedDecision
        cacheMatches == CacheKeyMatches
    IN /\ processUp
       /\ pendingRef = NoAction
       /\ pendingOpt = NoAction
       /\ lostRef = NoRequest
       /\ lostOpt = NoRequest
       /\ reference \in Actions
       /\ optimized \in Actions
       /\ pendingRef' = reference
       /\ pendingOpt' = optimized
       /\ pendingGeneration' = currentGeneration
       /\ pendingFullStreamRef' = SealedAllocate(storageRef, reference)
       /\ pendingFullStreamOpt' = SealedAllocate(storageOpt, optimized)
       /\ txPrefix' = 0
       /\ cacheStage' = stageOpt
       /\ cacheGeneration' = currentGeneration
       /\ cursor' = OptimizedScanCursor
       /\ validatedPositions' =
            IF cacheMatches
            THEN validatedPositions \cup NewValidatedPositions
            ELSE NewValidatedPositions
       /\ validationCount' =
            IF cacheMatches
            THEN validationCount + Cardinality(NewValidatedPositions)
            ELSE Cardinality(NewValidatedPositions)
       /\ txStartSnapshot' = DurableFingerprint
       /\ lastEvent' = "ISSUE"
       /\ lastFaultValue' = NoValue
       /\ UNCHANGED
            <<currentGeneration, DatabaseVariables, DelayedVariables,
              processUp, lostRef, lostOpt, successfulRequiredAllocations,
              replaySnapshot, staleSnapshot,
              delayedRejectSnapshot, crashSnapshot>>

CaptureConcurrentPreparedAllocate ==
    /\ processUp
    /\ pendingRef \in Actions
    /\ pendingOpt \in Actions
    /\ delayedRef = NoAction
    /\ delayedOpt = NoAction
    /\ SealedAllocate(storageRef, pendingRef)
    /\ SealedAllocate(storageOpt, pendingOpt)
    /\ pendingFullStreamRef
    /\ pendingFullStreamOpt
    /\ delayedRef' = pendingRef
    /\ delayedOpt' = pendingOpt
    /\ delayedGeneration' = pendingGeneration
    /\ delayedFullStreamRef' = pendingFullStreamRef
    /\ delayedFullStreamOpt' = pendingFullStreamOpt
    /\ lastEvent' = "CONCURRENT_ALLOCATE_PREPARED"
    /\ lastFaultValue' = NoValue
    /\ UNCHANGED
        <<currentGeneration, DatabaseVariables, TransactionVariables,
          ClientVariables, txStartSnapshot, replaySnapshot, staleSnapshot,
          delayedRejectSnapshot, crashSnapshot>>

StageTransaction ==
    /\ processUp
    /\ pendingRef \in Actions
    /\ pendingOpt \in Actions
    /\ txPrefix < TxStatementCount
    /\ txPrefix' = txPrefix + 1
    /\ lastEvent' = "TX_STATEMENT"
    /\ lastFaultValue' = NoValue
    /\ UNCHANGED
        <<currentGeneration, DatabaseVariables,
          pendingRef, pendingOpt, pendingGeneration,
          pendingFullStreamRef, pendingFullStreamOpt,
          DelayedVariables, ClientVariables, txStartSnapshot,
          replaySnapshot, staleSnapshot,
          delayedRejectSnapshot, crashSnapshot>>

CommitAction(responseLost) ==
    /\ processUp
    /\ pendingRef \in Actions
    /\ pendingOpt \in Actions
    /\ pendingGeneration = currentGeneration
    /\ txPrefix = TxStatementCount
    /\ PreparedStreamValid(
        storageRef, pendingRef, pendingFullStreamRef)
    /\ PreparedStreamValid(
        storageOpt, pendingOpt, pendingFullStreamOpt)
    /\ ActionEnabled(
        stageRef, buildCheckpointRef,
        storageRef, receiptRef, preimageRef,
        claimsRef, currentGeneration, pendingRef)
    /\ ActionEnabled(
        stageOpt, buildCheckpointOpt,
        storageOpt, receiptOpt, preimageOpt,
        claimsOpt, currentGeneration, pendingOpt)
    /\ storageRef' = ApplyStorage(storageRef, pendingRef)
    /\ storageOpt' = ApplyStorage(storageOpt, pendingOpt)
    /\ receiptRef' = ApplyReceipt(receiptRef, pendingRef)
    /\ receiptOpt' = ApplyReceipt(receiptOpt, pendingOpt)
    /\ preimageRef' = ApplyPreimage(preimageRef, pendingRef)
    /\ preimageOpt' = ApplyPreimage(preimageOpt, pendingOpt)
    /\ claimsRef' = ApplyClaims(claimsRef, currentGeneration, pendingRef)
    /\ claimsOpt' = ApplyClaims(claimsOpt, currentGeneration, pendingOpt)
    /\ buildCheckpointRef' =
        ApplyCheckpoint(buildCheckpointRef, pendingRef)
    /\ buildCheckpointOpt' =
        ApplyCheckpoint(buildCheckpointOpt, pendingOpt)
    /\ stageRef' = ApplyStage(stageRef, pendingRef)
    /\ stageOpt' = ApplyStage(stageOpt, pendingOpt)
    /\ lostRef' =
        IF responseLost
        THEN <<pendingGeneration, pendingRef>>
        ELSE NoRequest
    /\ lostOpt' =
        IF responseLost
        THEN <<pendingGeneration, pendingOpt>>
        ELSE NoRequest
    /\ successfulRequiredAllocations' =
        successfulRequiredAllocations +
          IF SealedAllocate(storageOpt, pendingOpt) THEN 1 ELSE 0
    /\ pendingRef' = NoAction
    /\ pendingOpt' = NoAction
    /\ pendingFullStreamRef' = FALSE
    /\ pendingFullStreamOpt' = FALSE
    /\ txPrefix' = 0
    /\ lastEvent' =
        IF responseLost THEN "COMMIT_RESPONSE_LOST" ELSE "COMMIT"
    /\ lastFaultValue' = NoValue
    /\ UNCHANGED
        <<currentGeneration, pendingGeneration, processUp,
          DelayedVariables,
          cacheStage, cacheGeneration, cursor,
          validatedPositions, validationCount,
          txStartSnapshot, replaySnapshot, staleSnapshot,
          delayedRejectSnapshot, crashSnapshot>>

ReplayLost(responseLost) ==
    /\ processUp
    /\ pendingRef = NoAction
    /\ pendingOpt = NoAction
    /\ lostRef \in Requests
    /\ lostOpt \in Requests
    /\ lostRef' = IF responseLost THEN lostRef ELSE NoRequest
    /\ lostOpt' = IF responseLost THEN lostOpt ELSE NoRequest
    /\ replaySnapshot' = DurableFingerprint
    /\ lastEvent' =
        IF responseLost THEN "REPLAY_RESPONSE_LOST" ELSE "REPLAY"
    /\ lastFaultValue' = NoValue
    /\ UNCHANGED
        <<currentGeneration, DatabaseVariables, TransactionVariables,
          DelayedVariables,
          processUp, cacheStage, cacheGeneration, cursor,
          validatedPositions, validationCount, successfulRequiredAllocations,
          txStartSnapshot, staleSnapshot,
          delayedRejectSnapshot, crashSnapshot>>

AdvanceFence ==
    /\ processUp
    /\ currentGeneration < MaxGeneration
    /\ currentGeneration' = currentGeneration + 1
    /\ lastEvent' = "FENCE_ADVANCE"
    /\ lastFaultValue' = NoValue
    /\ UNCHANGED
        <<DatabaseVariables, TransactionVariables, DelayedVariables,
          ClientVariables,
          txStartSnapshot, replaySnapshot, staleSnapshot,
          delayedRejectSnapshot, crashSnapshot>>

RejectStaleFence ==
    /\ processUp
    /\ pendingRef \in Actions
    /\ pendingOpt \in Actions
    /\ pendingGeneration # currentGeneration
    /\ staleSnapshot' = DurableFingerprint
    /\ pendingRef' = NoAction
    /\ pendingOpt' = NoAction
    /\ pendingFullStreamRef' = FALSE
    /\ pendingFullStreamOpt' = FALSE
    /\ txPrefix' = 0
    /\ lastEvent' = "STALE_FENCE_REJECTED"
    /\ lastFaultValue' = NoValue
    /\ UNCHANGED
        <<currentGeneration, DatabaseVariables, pendingGeneration,
          DelayedVariables, ClientVariables,
          txStartSnapshot, replaySnapshot,
          delayedRejectSnapshot, crashSnapshot>>

AbortTransaction ==
    /\ processUp
    /\ pendingRef \in Actions
    /\ pendingOpt \in Actions
    /\ pendingRef' = NoAction
    /\ pendingOpt' = NoAction
    /\ pendingFullStreamRef' = FALSE
    /\ pendingFullStreamOpt' = FALSE
    /\ txPrefix' = 0
    /\ lastEvent' = "TX_ABORT"
    /\ lastFaultValue' = NoValue
    /\ UNCHANGED
        <<currentGeneration, DatabaseVariables, pendingGeneration,
          DelayedVariables, ClientVariables, txStartSnapshot,
          replaySnapshot, staleSnapshot,
          delayedRejectSnapshot, crashSnapshot>>

(***************************************************************************
A delayed duplicate combines its prepare-time stream evidence with the
supported-writer sealed-state immutability assumption.  At commit, only the
generation, BUILD stage, and first-consumer checkpoint are fresh fences.  Union
is a zero-write replay if the exact claim already exists; otherwise it is the
one successful required allocation charged to successful-progress work.  This
operator does not claim a commit-time stream or Python/SQL refinement.
***************************************************************************)
CommitDelayedAllocate ==
    LET value == ActionValue(delayedRef)
        claimAlreadyExists ==
            HasCurrentClaim(claimsRef, currentGeneration, value)
    IN /\ processUp
       /\ pendingRef = NoAction
       /\ pendingOpt = NoAction
       /\ delayedRef \in Actions
       /\ delayedOpt \in Actions
       /\ DelayedAllocateCommitAdmissible(
            stageRef, buildCheckpointRef,
            storageRef, receiptRef, preimageRef,
            currentGeneration, delayedRef,
            delayedGeneration, delayedFullStreamRef)
       /\ DelayedAllocateCommitAdmissible(
            stageOpt, buildCheckpointOpt,
            storageOpt, receiptOpt, preimageOpt,
            currentGeneration, delayedOpt,
            delayedGeneration, delayedFullStreamOpt)
       /\ claimsRef' = claimsRef \cup {<<currentGeneration, value>>}
       /\ claimsOpt' = claimsOpt \cup {
            <<currentGeneration, ActionValue(delayedOpt)>>}
       /\ delayedRef' = NoAction
       /\ delayedOpt' = NoAction
       /\ delayedFullStreamRef' = FALSE
       /\ delayedFullStreamOpt' = FALSE
       /\ successfulRequiredAllocations' =
            successfulRequiredAllocations +
              IF claimAlreadyExists THEN 0 ELSE 1
       /\ lastEvent' = "DELAYED_ALLOCATE_COMMIT"
       /\ lastFaultValue' = NoValue
       /\ UNCHANGED
            <<currentGeneration,
              stageRef, stageOpt,
              buildCheckpointRef, buildCheckpointOpt,
              storageRef, storageOpt,
              receiptRef, receiptOpt, preimageRef, preimageOpt,
              TransactionVariables, delayedGeneration,
              processUp, lostRef, lostOpt,
              cacheStage, cacheGeneration, cursor,
              validatedPositions, validationCount,
              txStartSnapshot, replaySnapshot, staleSnapshot,
              delayedRejectSnapshot, crashSnapshot>>

RejectDelayedAllocateAfterConsumer ==
    LET value == ActionValue(delayedRef)
    IN /\ processUp
       /\ pendingRef = NoAction
       /\ pendingOpt = NoAction
       /\ delayedRef \in Actions
       /\ delayedOpt \in Actions
       /\ delayedGeneration = currentGeneration
       /\ stageRef = "BUILD"
       /\ stageOpt = "BUILD"
       /\ ConsumerPosition(value) <= buildCheckpointRef
       /\ ConsumerPosition(ActionValue(delayedOpt)) <= buildCheckpointOpt
       /\ delayedRejectSnapshot' = DurableFingerprint
       /\ delayedRef' = NoAction
       /\ delayedOpt' = NoAction
       /\ delayedFullStreamRef' = FALSE
       /\ delayedFullStreamOpt' = FALSE
       /\ lastEvent' = "DELAYED_ALLOCATE_AFTER_CONSUMER_REJECTED"
       /\ lastFaultValue' = value
       /\ UNCHANGED
            <<currentGeneration, DatabaseVariables, TransactionVariables,
              delayedGeneration, ClientVariables,
              txStartSnapshot, replaySnapshot, staleSnapshot, crashSnapshot>>

RejectDelayedAllocate ==
    /\ processUp
    /\ pendingRef = NoAction
    /\ pendingOpt = NoAction
    /\ delayedRef \in Actions
    /\ delayedOpt \in Actions
    /\ \/ ~DelayedAllocateCommitAdmissible(
            stageRef, buildCheckpointRef,
            storageRef, receiptRef, preimageRef,
            currentGeneration, delayedRef,
            delayedGeneration, delayedFullStreamRef)
       \/ ~DelayedAllocateCommitAdmissible(
            stageOpt, buildCheckpointOpt,
            storageOpt, receiptOpt, preimageOpt,
            currentGeneration, delayedOpt,
            delayedGeneration, delayedFullStreamOpt)
    /\ delayedRejectSnapshot' = DurableFingerprint
    /\ delayedRef' = NoAction
    /\ delayedOpt' = NoAction
    /\ delayedFullStreamRef' = FALSE
    /\ delayedFullStreamOpt' = FALSE
    /\ lastEvent' = "DELAYED_ALLOCATE_REJECTED"
    /\ lastFaultValue' = NoValue
    /\ UNCHANGED
        <<currentGeneration, DatabaseVariables, TransactionVariables,
          delayedGeneration, ClientVariables,
          txStartSnapshot, replaySnapshot, staleSnapshot, crashSnapshot>>

(***************************************************************************
Adversarial fault: page/preimage bytes change while the scalar receipt stays
exact.  It is allowed only without prepared work, so the next issue must run a
new full stream and reject in both worlds.  Valid production transitions keep
sealed bytes immutable.
***************************************************************************)
CorruptActivePreimageSameReceipt ==
    LET position == OptimizedScanCursor
        value == Plan[position]
    IN /\ processUp
       /\ pendingRef = NoAction
       /\ pendingOpt = NoAction
       /\ delayedRef = NoAction
       /\ delayedOpt = NoAction
       /\ lostRef = NoRequest
       /\ lostOpt = NoRequest
       /\ position \in Positions
       /\ storageRef[value] = 2
       /\ storageOpt[value] = 2
       /\ receiptRef[value] = ExactReceipt
       /\ receiptOpt[value] = ExactReceipt
       /\ preimageRef[value] = ExactPreimage
       /\ preimageOpt[value] = ExactPreimage
       /\ preimageRef' =
            [preimageRef EXCEPT ![value] = ChangedPreimage]
       /\ preimageOpt' =
            [preimageOpt EXCEPT ![value] = ChangedPreimage]
       /\ lastEvent' = "SAME_RECEIPT_PREIMAGE_CORRUPTED"
       /\ lastFaultValue' = value
       /\ UNCHANGED
            <<currentGeneration,
              stageRef, stageOpt,
              buildCheckpointRef, buildCheckpointOpt,
              storageRef, storageOpt, receiptRef, receiptOpt,
              claimsRef, claimsOpt,
              TransactionVariables, DelayedVariables, ClientVariables,
              txStartSnapshot, replaySnapshot, staleSnapshot,
              delayedRejectSnapshot, crashSnapshot>>

Crash ==
    /\ processUp
    /\ processUp' = FALSE
    /\ pendingRef' = NoAction
    /\ pendingOpt' = NoAction
    /\ pendingFullStreamRef' = FALSE
    /\ pendingFullStreamOpt' = FALSE
    /\ txPrefix' = 0
    /\ delayedRef' = NoAction
    /\ delayedOpt' = NoAction
    /\ delayedFullStreamRef' = FALSE
    /\ delayedFullStreamOpt' = FALSE
    /\ cacheStage' = "NONE"
    /\ cacheGeneration' = 0
    /\ cursor' = 1
    /\ validatedPositions' = {}
    /\ validationCount' = 0
    /\ crashSnapshot' = DurableFingerprint
    /\ lastEvent' = "CRASH"
    /\ lastFaultValue' = NoValue
    /\ UNCHANGED
        <<currentGeneration, DatabaseVariables,
          pendingGeneration, delayedGeneration,
          lostRef, lostOpt, successfulRequiredAllocations,
          txStartSnapshot, replaySnapshot, staleSnapshot,
          delayedRejectSnapshot>>

Restart ==
    /\ ~processUp
    /\ processUp' = TRUE
    /\ lastEvent' = "RESTART"
    /\ lastFaultValue' = NoValue
    /\ UNCHANGED
        <<currentGeneration, DatabaseVariables, TransactionVariables,
          DelayedVariables,
          lostRef, lostOpt, cacheStage, cacheGeneration, cursor,
          validatedPositions, validationCount, successfulRequiredAllocations,
          txStartSnapshot, replaySnapshot, staleSnapshot,
          delayedRejectSnapshot, crashSnapshot>>

ObserveTerminal ==
    LET cacheMatches == CacheKeyMatches
    IN /\ processUp
       /\ pendingRef = NoAction
       /\ pendingOpt = NoAction
       /\ lostRef = NoRequest
       /\ lostOpt = NoRequest
       /\ ReferenceDecision = NoAction
       /\ OptimizedDecision = NoAction
       /\ cacheStage' = stageOpt
       /\ cacheGeneration' = currentGeneration
       /\ cursor' = OptimizedScanCursor
       /\ validatedPositions' =
            IF cacheMatches
            THEN validatedPositions \cup NewValidatedPositions
            ELSE NewValidatedPositions
       /\ validationCount' =
            IF cacheMatches
            THEN validationCount + Cardinality(NewValidatedPositions)
            ELSE Cardinality(NewValidatedPositions)
       /\ lastEvent' = "TERMINAL_OBSERVED"
       /\ lastFaultValue' = NoValue
       /\ UNCHANGED
            <<currentGeneration, DatabaseVariables, TransactionVariables,
              DelayedVariables,
              processUp, lostRef, lostOpt, successfulRequiredAllocations,
              txStartSnapshot, replaySnapshot, staleSnapshot,
              delayedRejectSnapshot, crashSnapshot>>

ObserveRejection ==
    /\ processUp
    /\ pendingRef = NoAction
    /\ pendingOpt = NoAction
    /\ ReferenceDecision = RejectAction
    /\ OptimizedDecision = RejectAction
    /\ lastEvent' = "REJECTION_OBSERVED"
    /\ UNCHANGED
        <<currentGeneration, DatabaseVariables, TransactionVariables,
          DelayedVariables, ClientVariables, lastFaultValue,
          txStartSnapshot, replaySnapshot, staleSnapshot,
          delayedRejectSnapshot, crashSnapshot>>

Next ==
    \/ IssueAction
    \/ CaptureConcurrentPreparedAllocate
    \/ StageTransaction
    \/ CommitAction(FALSE)
    \/ CommitAction(TRUE)
    \/ ReplayLost(FALSE)
    \/ ReplayLost(TRUE)
    \/ AdvanceFence
    \/ RejectStaleFence
    \/ AbortTransaction
    \/ CommitDelayedAllocate
    \/ RejectDelayedAllocateAfterConsumer
    \/ RejectDelayedAllocate
    \/ CorruptActivePreimageSameReceipt
    \/ Crash
    \/ Restart
    \/ ObserveTerminal
    \/ ObserveRejection

Spec == Init /\ [][Next]_vars

(***************************************************************************
Finite safety properties.  Decision equivalence is the TLA refinement bridge;
Lean proves its list-level analogue without a finite plan bound.
***************************************************************************)

TypeOK ==
    /\ processUp \in BOOLEAN
    /\ currentGeneration \in Generations
    /\ stageRef \in Stages
    /\ stageOpt \in Stages
    /\ buildCheckpointRef \in 0..Len(ConsumerPlan)
    /\ buildCheckpointOpt \in 0..Len(ConsumerPlan)
    /\ storageRef \in [Values -> StoragePhases]
    /\ storageOpt \in [Values -> StoragePhases]
    /\ receiptRef \in [Values -> ReceiptStates]
    /\ receiptOpt \in [Values -> ReceiptStates]
    /\ preimageRef \in [Values -> PreimageStates]
    /\ preimageOpt \in [Values -> PreimageStates]
    /\ claimsRef \subseteq Generations \X Values
    /\ claimsOpt \subseteq Generations \X Values
    /\ pendingRef \in Actions \cup {NoAction}
    /\ pendingOpt \in Actions \cup {NoAction}
    /\ pendingGeneration \in Generations
    /\ pendingFullStreamRef \in BOOLEAN
    /\ pendingFullStreamOpt \in BOOLEAN
    /\ txPrefix \in 0..TxStatementCount
    /\ delayedRef \in Actions \cup {NoAction}
    /\ delayedOpt \in Actions \cup {NoAction}
    /\ delayedGeneration \in Generations
    /\ delayedFullStreamRef \in BOOLEAN
    /\ delayedFullStreamOpt \in BOOLEAN
    /\ lostRef \in Requests \cup {NoRequest}
    /\ lostOpt \in Requests \cup {NoRequest}
    /\ cacheStage \in CacheStages
    /\ cacheGeneration \in 0..MaxGeneration
    /\ cursor \in 1..(Len(Plan) + 1)
    /\ validatedPositions \subseteq Positions
    /\ validationCount \in 0..Len(Plan)
    /\ successfulRequiredAllocations \in
        0..(MaxGeneration * Len(Plan))
    /\ lastEvent \in EventKinds
    /\ lastFaultValue \in Values \cup {NoValue}

DurableWorldsAgree ==
    ReferenceDatabaseVariables = OptimizedDatabaseVariables

ReferenceAndCursorSelectSameNextAction ==
    ReferenceDecision = OptimizedDecision

ReferenceAndCursorHaveSameTerminalResult ==
    (ReferenceDecision = NoAction) <=> (OptimizedDecision = NoAction)

PendingActionsAgree ==
    /\ pendingRef = pendingOpt
    /\ lostRef = lostOpt
    /\ delayedRef = delayedOpt

CursorPrefixIsFreshlyJustified ==
    CacheKeyMatches =>
        \A position \in 1..(cursor - 1) :
            EntryComplete(
                stageOpt, buildCheckpointOpt,
                storageOpt, receiptOpt, preimageOpt,
                claimsOpt, currentGeneration, Plan[position])

ValidatedPositionsAreOneMonotonicPrefix ==
    CacheKeyMatches =>
        /\ validatedPositions = 1..(cursor - 1)
        /\ validationCount = Cardinality(validatedPositions)

(***************************************************************************
The first term counts each newly sealed-and-complete prefix element at most
once per matching cache epoch.  The second charges exactly one full stream for
each successful sealed allocation.  Aborted attempts stream again but are not
successful progress, so no bound over an arbitrary retry schedule is claimed.
***************************************************************************)
SuccessfulProgressValidationWorkIsLinear ==
    validationCount + successfulRequiredAllocations <=
        Len(Plan) + successfulRequiredAllocations

SuccessfulRequiredAllocationsAreBounded ==
    successfulRequiredAllocations <= MaxGeneration * Len(Plan)

EveryPreparedSealedAllocateWasFreshlyStreamed ==
    /\ (pendingRef \in Actions /\ SealedAllocate(storageRef, pendingRef) =>
            pendingFullStreamRef)
    /\ (pendingOpt \in Actions /\ SealedAllocate(storageOpt, pendingOpt) =>
            pendingFullStreamOpt)
    /\ (delayedRef \in Actions => delayedFullStreamRef)
    /\ (delayedOpt \in Actions => delayedFullStreamOpt)

SameReceiptChangedPreimageIsRejected ==
    lastEvent = "SAME_RECEIPT_PREIMAGE_CORRUPTED" =>
        /\ lastFaultValue \in Values
        /\ receiptRef[lastFaultValue] = ExactReceipt
        /\ receiptOpt[lastFaultValue] = ExactReceipt
        /\ preimageRef[lastFaultValue] = ChangedPreimage
        /\ preimageOpt[lastFaultValue] = ChangedPreimage
        /\ ReferenceDecision = RejectAction
        /\ OptimizedDecision = RejectAction

CurrentConsumerClaimDeletionAndCheckpointAreAtomic ==
    \A position \in 1..buildCheckpointOpt :
        <<currentGeneration, ConsumerPlan[position]>> \notin claimsOpt

TransactionPrefixHasNoPublicationWrites ==
    pendingRef \in Actions => DurableFingerprint = txStartSnapshot

StatementPrefixCrashRollsBack ==
    lastEvent = "CRASH" => DurableFingerprint = crashSnapshot

ResponseLossReplayIsObservational ==
    lastEvent \in {"REPLAY", "REPLAY_RESPONSE_LOST"}
        => DurableFingerprint = replaySnapshot

StaleFenceHasZeroPublicationWrites ==
    lastEvent = "STALE_FENCE_REJECTED"
        => DurableFingerprint = staleSnapshot

DelayedConsumedOrStaleAllocateHasZeroWrites ==
    lastEvent \in {
        "DELAYED_ALLOCATE_REJECTED",
        "DELAYED_ALLOCATE_AFTER_CONSUMER_REJECTED"
    }
        => DurableFingerprint = delayedRejectSnapshot

DelayedAllocateAfterConsumerFenceIsExplicit ==
    lastEvent = "DELAYED_ALLOCATE_AFTER_CONSUMER_REJECTED" =>
        /\ lastFaultValue \in Values
        /\ delayedGeneration = currentGeneration
        /\ stageOpt = "BUILD"
        /\ ConsumerPosition(lastFaultValue) <= buildCheckpointOpt
        /\ <<currentGeneration, lastFaultValue>> \notin claimsOpt

ResponseLossHasExactReplayAuthority ==
    /\ (lostRef # NoRequest => lostRef \in Requests)
    /\ (lostOpt # NoRequest => lostOpt \in Requests)

SafetyView ==
    vars

=============================================================================
