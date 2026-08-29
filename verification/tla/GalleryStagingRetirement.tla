---------------------- MODULE GalleryStagingRetirement ----------------------
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************
This is a finite, provider-neutral model of the greenfield in-band staging
retirement and emergency request-budget protocol.  One scalar slot represents
the database candidate key build_id -> staging_id.  phaseRows represents the
seven child-first SQL phases; phase four is the request-identity family and is
therefore exactly congruent with the seeded global budget counter.

Seal delivery and implicit ACK are deliberately separate transitions.  A
response-loss recovery first delivers the reconstructed seal while retaining
ordinary SEALED/REUSED retry authority.  Only a following shared-fenced source
advance may change it to RETIRING_* and delete the first child batch atomically.
An exclusive generic cleanup transition is retained as a crash-recovery
backstop.  This bounded model does not establish Python/SQL refinement,
database isolation, byte/digest correctness, or unbounded capacity.
***************************************************************************)

CONSTANTS MaxBudget, MaxGeneration

ASSUME /\ MaxBudget \in Nat \ {0}
       /\ MaxGeneration \in Nat \ {0}

Phases == 1..7
Slots == {
    "EMPTY", "OPEN", "SEALED", "REUSED",
    "RETIRING_SEALED", "RETIRING_REUSED"
}
TerminalSlots == {"SEALED", "REUSED"}
RetiringSlots == {"RETIRING_SEALED", "RETIRING_REUSED"}
GateModes == {"SHARED", "EXCLUSIVE"}

VARIABLES
    slot,
    gateMode,
    liveGeneration,
    claimGeneration,
    sealDelivered,
    implicitAcked,
    phaseRows,
    budgetCount,
    lastEvent,
    lastDeletedPhase,
    lastDeleteSkippedEarlier,
    lastRetryAccepted,
    lastSecondBeginAccepted,
    lastAckWasShared

vars == <<
    slot, gateMode, liveGeneration, claimGeneration, sealDelivered,
    implicitAcked, phaseRows, budgetCount, lastEvent, lastDeletedPhase,
    lastDeleteSkippedEarlier, lastRetryAccepted,
    lastSecondBeginAccepted, lastAckWasShared
>>

FirstNonempty(rows) ==
    IF {p \in Phases : rows[p] > 0} = {}
    THEN 0
    ELSE CHOOSE p \in Phases :
        /\ rows[p] > 0
        /\ \A earlier \in Phases : earlier < p => rows[earlier] = 0

InitialPhaseRows == [p \in Phases |-> IF p \in {5, 6, 7} THEN 1 ELSE 0]

Init ==
    /\ slot = "EMPTY"
    /\ gateMode = "SHARED"
    /\ liveGeneration = 1
    /\ claimGeneration = 0
    /\ sealDelivered = FALSE
    /\ implicitAcked = FALSE
    /\ phaseRows = [p \in Phases |-> 0]
    /\ budgetCount = 0
    /\ lastEvent = "INIT"
    /\ lastDeletedPhase = 0
    /\ lastDeleteSkippedEarlier = FALSE
    /\ lastRetryAccepted = FALSE
    /\ lastSecondBeginAccepted = FALSE
    /\ lastAckWasShared = FALSE

ResetDeleteAudit ==
    /\ lastDeletedPhase' = 0
    /\ lastDeleteSkippedEarlier' = FALSE

ResetCallAudit ==
    /\ lastRetryAccepted' = FALSE
    /\ lastSecondBeginAccepted' = FALSE
    /\ lastAckWasShared' = FALSE

Begin ==
    /\ slot = "EMPTY"
    /\ gateMode = "SHARED"
    /\ slot' = "OPEN"
    /\ phaseRows' = InitialPhaseRows
    /\ budgetCount' = 0
    /\ sealDelivered' = FALSE
    /\ implicitAcked' = FALSE
    /\ UNCHANGED <<gateMode, liveGeneration, claimGeneration>>
    /\ lastEvent' = "BEGIN"
    /\ ResetDeleteAudit
    /\ ResetCallAudit

ConcurrentSecondBeginReject ==
    /\ slot # "EMPTY"
    /\ UNCHANGED <<slot, gateMode, liveGeneration, claimGeneration,
                    sealDelivered, implicitAcked, phaseRows, budgetCount>>
    /\ lastEvent' = "SECOND_BEGIN_REJECT"
    /\ ResetDeleteAudit
    /\ lastRetryAccepted' = FALSE
    /\ lastSecondBeginAccepted' = FALSE
    /\ lastAckWasShared' = FALSE

ReserveRequest ==
    /\ slot = "OPEN"
    /\ gateMode = "SHARED"
    /\ budgetCount < MaxBudget
    /\ phaseRows' = [phaseRows EXCEPT
        ![1] = @ + 1,
        ![2] = @ + 1,
        ![3] = @ + 1,
        ![4] = @ + 1]
    /\ budgetCount' = budgetCount + 1
    /\ UNCHANGED <<slot, gateMode, liveGeneration, claimGeneration,
                    sealDelivered, implicitAcked>>
    /\ lastEvent' = "RESERVE"
    /\ ResetDeleteAudit
    /\ ResetCallAudit

CapacityReject ==
    /\ slot = "OPEN"
    /\ budgetCount = MaxBudget
    /\ UNCHANGED <<slot, gateMode, liveGeneration, claimGeneration,
                    sealDelivered, implicitAcked, phaseRows, budgetCount>>
    /\ lastEvent' = "CAPACITY_REJECT"
    /\ ResetDeleteAudit
    /\ ResetCallAudit

SealNew ==
    /\ slot = "OPEN"
    /\ gateMode = "SHARED"
    /\ slot' = "SEALED"
    /\ sealDelivered' = FALSE
    /\ implicitAcked' = FALSE
    /\ UNCHANGED <<gateMode, liveGeneration, claimGeneration,
                    phaseRows, budgetCount>>
    /\ lastEvent' = "SEAL_RESPONSE_LOST"
    /\ ResetDeleteAudit
    /\ ResetCallAudit

SealReused ==
    /\ slot = "OPEN"
    /\ gateMode = "SHARED"
    /\ slot' = "REUSED"
    /\ sealDelivered' = FALSE
    /\ implicitAcked' = FALSE
    /\ UNCHANGED <<gateMode, liveGeneration, claimGeneration,
                    phaseRows, budgetCount>>
    /\ lastEvent' = "REUSE_RESPONSE_LOST"
    /\ ResetDeleteAudit
    /\ ResetCallAudit

DeliverFreshSeal ==
    /\ slot \in TerminalSlots
    /\ ~sealDelivered
    /\ sealDelivered' = TRUE
    /\ UNCHANGED <<slot, gateMode, liveGeneration, claimGeneration,
                    implicitAcked, phaseRows, budgetCount>>
    /\ lastEvent' = "DELIVER_FRESH_SEAL"
    /\ ResetDeleteAudit
    /\ ResetCallAudit

RecoverLostSeal ==
    /\ slot \in TerminalSlots
    /\ ~sealDelivered
    /\ sealDelivered' = TRUE
    /\ UNCHANGED <<slot, gateMode, liveGeneration, claimGeneration,
                    implicitAcked, phaseRows, budgetCount>>
    /\ lastEvent' = "STAGING_RECOVER"
    /\ ResetDeleteAudit
    /\ ResetCallAudit

ImplicitAck ==
    LET phase == FirstNonempty(phaseRows) IN
    /\ slot \in TerminalSlots
    /\ gateMode = "SHARED"
    /\ sealDelivered
    /\ phase \in Phases
    /\ phaseRows' = [phaseRows EXCEPT ![phase] = @ - 1]
    /\ budgetCount' = IF phase = 4 THEN budgetCount - 1 ELSE budgetCount
    /\ slot' = IF phase = 7
        THEN "EMPTY"
        ELSE IF slot = "SEALED" THEN "RETIRING_SEALED"
        ELSE "RETIRING_REUSED"
    /\ sealDelivered' = IF phase = 7 THEN FALSE ELSE sealDelivered
    /\ implicitAcked' = (phase # 7)
    /\ UNCHANGED <<gateMode, liveGeneration, claimGeneration>>
    /\ lastEvent' = "IMPLICIT_ACK"
    /\ lastDeletedPhase' = phase
    /\ lastDeleteSkippedEarlier' =
        \E earlier \in Phases : earlier < phase /\ phaseRows[earlier] > 0
    /\ lastRetryAccepted' = FALSE
    /\ lastSecondBeginAccepted' = FALSE
    /\ lastAckWasShared' = TRUE

RetireBatch ==
    LET phase == FirstNonempty(phaseRows) IN
    /\ slot \in RetiringSlots
    /\ gateMode = "SHARED"
    /\ implicitAcked
    /\ phase \in Phases
    /\ phaseRows' = [phaseRows EXCEPT ![phase] = @ - 1]
    /\ budgetCount' = IF phase = 4 THEN budgetCount - 1 ELSE budgetCount
    /\ slot' = IF phase = 7 THEN "EMPTY" ELSE slot
    /\ sealDelivered' = IF phase = 7 THEN FALSE ELSE sealDelivered
    /\ implicitAcked' = IF phase = 7 THEN FALSE ELSE implicitAcked
    /\ UNCHANGED <<gateMode, liveGeneration, claimGeneration>>
    /\ lastEvent' = "RETIRE_BATCH"
    /\ lastDeletedPhase' = phase
    /\ lastDeleteSkippedEarlier' =
        \E earlier \in Phases : earlier < phase /\ phaseRows[earlier] > 0
    /\ lastRetryAccepted' = FALSE
    /\ lastSecondBeginAccepted' = FALSE
    /\ lastAckWasShared' = FALSE

GenericCleanupBatch ==
    LET phase == FirstNonempty(phaseRows) IN
    /\ slot \in TerminalSlots \cup RetiringSlots
    /\ gateMode = "EXCLUSIVE"
    /\ phase \in Phases
    /\ phaseRows' = [phaseRows EXCEPT ![phase] = @ - 1]
    /\ budgetCount' = IF phase = 4 THEN budgetCount - 1 ELSE budgetCount
    /\ slot' = IF phase = 7 THEN "EMPTY" ELSE slot
    /\ sealDelivered' = IF phase = 7 THEN FALSE ELSE sealDelivered
    /\ implicitAcked' = IF phase = 7 THEN FALSE ELSE implicitAcked
    /\ UNCHANGED <<gateMode, liveGeneration, claimGeneration>>
    /\ lastEvent' = "GENERIC_CLEANUP"
    /\ lastDeletedPhase' = phase
    /\ lastDeleteSkippedEarlier' =
        \E earlier \in Phases : earlier < phase /\ phaseRows[earlier] > 0
    /\ lastRetryAccepted' = FALSE
    /\ lastSecondBeginAccepted' = FALSE
    /\ lastAckWasShared' = FALSE

OldRetryRejected ==
    /\ slot \in RetiringSlots
    /\ UNCHANGED <<slot, gateMode, liveGeneration, claimGeneration,
                    sealDelivered, implicitAcked, phaseRows, budgetCount>>
    /\ lastEvent' = "OLD_RETRY_REJECT"
    /\ ResetDeleteAudit
    /\ lastRetryAccepted' = FALSE
    /\ lastSecondBeginAccepted' = FALSE
    /\ lastAckWasShared' = FALSE

Takeover ==
    /\ slot \in TerminalSlots \cup RetiringSlots
    /\ gateMode = "SHARED"
    /\ liveGeneration < MaxGeneration
    /\ liveGeneration' = liveGeneration + 1
    /\ claimGeneration' = claimGeneration + 1
    /\ UNCHANGED <<slot, gateMode, sealDelivered, implicitAcked,
                    phaseRows, budgetCount>>
    /\ lastEvent' = "TAKEOVER"
    /\ ResetDeleteAudit
    /\ ResetCallAudit

EnterExclusive ==
    /\ gateMode = "SHARED"
    /\ gateMode' = "EXCLUSIVE"
    /\ UNCHANGED <<slot, liveGeneration, claimGeneration, sealDelivered,
                    implicitAcked, phaseRows, budgetCount>>
    /\ lastEvent' = "ENTER_EXCLUSIVE"
    /\ ResetDeleteAudit
    /\ ResetCallAudit

EnterShared ==
    /\ gateMode = "EXCLUSIVE"
    /\ gateMode' = "SHARED"
    /\ UNCHANGED <<slot, liveGeneration, claimGeneration, sealDelivered,
                    implicitAcked, phaseRows, budgetCount>>
    /\ lastEvent' = "ENTER_SHARED"
    /\ ResetDeleteAudit
    /\ ResetCallAudit

Next ==
    \/ Begin
    \/ ConcurrentSecondBeginReject
    \/ ReserveRequest
    \/ CapacityReject
    \/ SealNew
    \/ SealReused
    \/ DeliverFreshSeal
    \/ RecoverLostSeal
    \/ ImplicitAck
    \/ RetireBatch
    \/ GenericCleanupBatch
    \/ OldRetryRejected
    \/ Takeover
    \/ EnterExclusive
    \/ EnterShared

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ slot \in Slots
    /\ gateMode \in GateModes
    /\ liveGeneration \in 1..MaxGeneration
    /\ claimGeneration \in Nat
    /\ sealDelivered \in BOOLEAN
    /\ implicitAcked \in BOOLEAN
    /\ phaseRows \in [Phases -> Nat]
    /\ budgetCount \in 0..MaxBudget
    /\ lastDeletedPhase \in 0..7
    /\ lastDeleteSkippedEarlier \in BOOLEAN
    /\ lastRetryAccepted \in BOOLEAN
    /\ lastSecondBeginAccepted \in BOOLEAN
    /\ lastAckWasShared \in BOOLEAN

BudgetExactlyMatchesRetainedRequests == budgetCount = phaseRows[4]

RetiringRequiresDeliveredImplicitAck ==
    slot \in RetiringSlots => sealDelivered /\ implicitAcked

ImplicitAckUsesSharedFence ==
    lastEvent = "IMPLICIT_ACK" => lastAckWasShared

RecoveryPrecedesAck ==
    lastEvent = "STAGING_RECOVER" => slot \in TerminalSlots /\ ~implicitAcked

OldRetryCannotMutateRetiring ==
    lastEvent = "OLD_RETRY_REJECT" => ~lastRetryAccepted

OneBuildSlotRejectsConcurrentBegin ==
    lastEvent = "SECOND_BEGIN_REJECT" => ~lastSecondBeginAccepted

DeletionIsChildFirst == ~lastDeleteSkippedEarlier

RootDeletionRequiresEmptyChildren ==
    lastDeletedPhase = 7 => \A phase \in 1..6 : phaseRows[phase] = 0

=============================================================================
