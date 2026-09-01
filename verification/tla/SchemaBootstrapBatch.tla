------------------------ MODULE SchemaBootstrapBatch ------------------------
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************
Finite crash/replay model for bounded batching of checksum-bound idempotent
schema bootstrap facts.  The process preserves generated order, stops a batch
at an SQL-statement boundary, and never includes more than BatchLimit rows.

A failed prepared batch writes nothing.  A response-lost commit may make the
whole batch durable before the process cursor is reset.  Replay starts from the
first generated fact and duplicate INSERTs are no-ops.  READY is reachable only
after every generated fact is durable.  The model does not establish Python,
connector, SQL, transaction, or backend refinement; runtime differential and
SQLite/MariaDB tests provide that evidence.
***************************************************************************)

CONSTANTS SeedCount, BatchLimit, StatementSplit

ASSUME /\ SeedCount \in Nat \ {0}
       /\ BatchLimit \in Nat \ {0}
       /\ StatementSplit \in 1..SeedCount

Seeds == 1..SeedCount
Modes == {"IDLE", "PREPARED", "LOST"}
EpochStates == {"BUILDING", "READY"}

StatementOf(seed) == IF seed <= StatementSplit THEN 1 ELSE 2

CandidateEnds(start) ==
    {finish \in start..(IF start + BatchLimit - 1 <= SeedCount
                         THEN start + BatchLimit - 1
                         ELSE SeedCount) :
        \A seed \in start..finish : StatementOf(seed) = StatementOf(start)}

BatchEnd(start) ==
    CHOOSE finish \in CandidateEnds(start) :
        \A other \in CandidateEnds(start) : other <= finish

VARIABLES
    epochState,
    mode,
    cursor,
    preparedStart,
    preparedEnd,
    durable,
    lastEvent

vars == <<
    epochState, mode, cursor, preparedStart, preparedEnd, durable, lastEvent
>>

Init ==
    /\ epochState = "BUILDING"
    /\ mode = "IDLE"
    /\ cursor = 1
    /\ preparedStart = 0
    /\ preparedEnd = 0
    /\ durable = {}
    /\ lastEvent = "INIT"

PrepareBatch ==
    /\ epochState = "BUILDING"
    /\ mode = "IDLE"
    /\ cursor \in Seeds
    /\ mode' = "PREPARED"
    /\ preparedStart' = cursor
    /\ preparedEnd' = BatchEnd(cursor)
    /\ lastEvent' = "PREPARE"
    /\ UNCHANGED <<epochState, cursor, durable>>

CommitDelivered ==
    /\ epochState = "BUILDING"
    /\ mode = "PREPARED"
    /\ durable' = durable \cup (preparedStart..preparedEnd)
    /\ cursor' = preparedEnd + 1
    /\ mode' = "IDLE"
    /\ preparedStart' = 0
    /\ preparedEnd' = 0
    /\ lastEvent' = "COMMIT_DELIVERED"
    /\ UNCHANGED epochState

CommitResponseLost ==
    /\ epochState = "BUILDING"
    /\ mode = "PREPARED"
    /\ durable' = durable \cup (preparedStart..preparedEnd)
    /\ mode' = "LOST"
    /\ lastEvent' = "COMMIT_RESPONSE_LOST"
    /\ UNCHANGED <<epochState, cursor, preparedStart, preparedEnd>>

AbortPrepared ==
    /\ epochState = "BUILDING"
    /\ mode = "PREPARED"
    /\ mode' = "IDLE"
    /\ cursor' = 1
    /\ preparedStart' = 0
    /\ preparedEnd' = 0
    /\ lastEvent' = "BATCH_ROLLBACK"
    /\ UNCHANGED <<epochState, durable>>

ReplayLost ==
    /\ epochState = "BUILDING"
    /\ mode = "LOST"
    /\ mode' = "IDLE"
    /\ cursor' = 1
    /\ preparedStart' = 0
    /\ preparedEnd' = 0
    /\ lastEvent' = "REPLAY_RESPONSE_LOSS"
    /\ UNCHANGED <<epochState, durable>>

Crash ==
    /\ epochState = "BUILDING"
    /\ mode' = "IDLE"
    /\ cursor' = 1
    /\ preparedStart' = 0
    /\ preparedEnd' = 0
    /\ lastEvent' = "CRASH_RESET"
    /\ UNCHANGED <<epochState, durable>>

PublishReady ==
    /\ epochState = "BUILDING"
    /\ mode = "IDLE"
    /\ cursor = SeedCount + 1
    /\ durable = Seeds
    /\ epochState' = "READY"
    /\ lastEvent' = "READY_A"
    /\ UNCHANGED <<mode, cursor, preparedStart, preparedEnd, durable>>

ReadyProbe ==
    /\ epochState = "READY"
    /\ lastEvent' = IF lastEvent = "READY_A" THEN "READY_B" ELSE "READY_A"
    /\ UNCHANGED <<epochState, mode, cursor, preparedStart, preparedEnd, durable>>

Next ==
    \/ PrepareBatch
    \/ CommitDelivered
    \/ CommitResponseLost
    \/ AbortPrepared
    \/ ReplayLost
    \/ Crash
    \/ PublishReady
    \/ ReadyProbe

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ epochState \in EpochStates
    /\ mode \in Modes
    /\ cursor \in 1..(SeedCount + 1)
    /\ preparedStart \in 0..SeedCount
    /\ preparedEnd \in 0..SeedCount
    /\ durable \subseteq Seeds

PreparedBatchIsBounded ==
    mode = "PREPARED" =>
        /\ preparedStart \in Seeds
        /\ preparedEnd \in preparedStart..SeedCount
        /\ preparedEnd - preparedStart + 1 <= BatchLimit
        /\ \A seed \in preparedStart..preparedEnd :
            StatementOf(seed) = StatementOf(preparedStart)

NonpreparedHasNoBatch ==
    mode = "IDLE" => preparedStart = 0 /\ preparedEnd = 0

DurableFactsAreAnOrderedPrefix ==
    \A seed \in durable : \A earlier \in 1..seed : earlier \in durable

ReadyHasExactGeneratedFacts ==
    epochState = "READY" => durable = Seeds

ReadyIsTerminal ==
    epochState = "READY" => mode = "IDLE" /\ cursor = SeedCount + 1

=============================================================================
