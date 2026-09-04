----------------------- MODULE PytestProcessSupervision -----------------------
EXTENDS Integers

(***************************************************************************)
(* Finite safety model for the sequential pytest profile runner.  Each     *)
(* phase creates a fresh start-gated supervisor and process-tree owner.     *)
(* PhaseLimit ranges over the one-phase server-crash profile, the two-phase *)
(* merge profile, and the three-phase deep profile in the companion cfg.    *)
(* MaxTick models an explicitly configured aggregate deadline; the real     *)
(* deep profile has no deadline unless its caller supplies one.             *)
(*                                                                         *)
(* `treeEmptyProven` distinguishes a synchronous empty-tree query from the  *)
(* Windows kill-on-last-Job-handle contract.  The latter is allowed only on *)
(* the fail-closed infrastructure exit 125.  Docker daemon resources are    *)
(* outside this operating-system process-tree model.                        *)
(***************************************************************************)

CONSTANTS MaxTick, MaxPhases

VARIABLES lifecycle,
          phaseLimit,
          phaseIndex,
          owned,
          gateOpen,
          active,
          cause,
          requestedExitCode,
          terminationFailed,
          treeEmptyProven,
          phaseExitCode,
          priorPhaseExitCode,
          priorTreeEmptyProven,
          profileExitCode,
          tick

vars == <<lifecycle, phaseLimit, phaseIndex, owned, gateOpen, active, cause,
          requestedExitCode, terminationFailed, treeEmptyProven,
          phaseExitCode, priorPhaseExitCode, priorTreeEmptyProven,
          profileExitCode, tick>>

Lifecycles == {"unstarted", "gated", "owned", "running", "cleanup",
               "empty", "returned", "profile-returned"}
Causes == {"none", "clean", "failure", "survivor", "timeout",
           "interrupt", "establishment", "owner-close"}
InterruptExitCodes == {129, 130, 143, 149}
SemanticExitCodes == {0, 7, 124} \union InterruptExitCodes
ExitCodes == {-1, 0, 7, 124, 125, 129, 130, 143, 149}

Init ==
    /\ lifecycle = "unstarted"
    /\ phaseLimit \in 1..MaxPhases
    /\ phaseIndex = 1
    /\ owned = FALSE
    /\ gateOpen = FALSE
    /\ active = 0
    /\ cause = "none"
    /\ requestedExitCode = -1
    /\ terminationFailed = FALSE
    /\ treeEmptyProven = FALSE
    /\ phaseExitCode = -1
    /\ priorPhaseExitCode = -1
    /\ priorTreeEmptyProven = FALSE
    /\ profileExitCode = -1
    /\ tick = 0

CreateGatedSupervisor ==
    /\ lifecycle = "unstarted"
    /\ tick < MaxTick
    /\ lifecycle' = "gated"
    /\ active' = 1
    /\ UNCHANGED <<phaseLimit, phaseIndex, owned, gateOpen, cause,
                    requestedExitCode, terminationFailed, treeEmptyProven,
                    phaseExitCode, priorPhaseExitCode,
                    priorTreeEmptyProven, profileExitCode, tick>>

AssignOwner ==
    /\ lifecycle = "gated"
    /\ lifecycle' = "owned"
    /\ owned' = TRUE
    /\ UNCHANGED <<phaseLimit, phaseIndex, gateOpen, active, cause,
                    requestedExitCode, terminationFailed, treeEmptyProven,
                    phaseExitCode, priorPhaseExitCode,
                    priorTreeEmptyProven, profileExitCode, tick>>

OpenStartGate ==
    /\ lifecycle = "owned"
    /\ tick < MaxTick
    /\ lifecycle' = "running"
    /\ gateOpen' = TRUE
    /\ active' = 2
    /\ UNCHANGED <<phaseLimit, phaseIndex, owned, cause,
                    requestedExitCode, terminationFailed, treeEmptyProven,
                    phaseExitCode, priorPhaseExitCode,
                    priorTreeEmptyProven, profileExitCode, tick>>

PreStartBudgetExpires ==
    /\ lifecycle = "unstarted"
    /\ tick = MaxTick
    /\ lifecycle' = "empty"
    /\ cause' = "timeout"
    /\ requestedExitCode' = 124
    /\ treeEmptyProven' = TRUE
    /\ phaseExitCode' = 124
    /\ UNCHANGED <<phaseLimit, phaseIndex, owned, gateOpen, active,
                    terminationFailed, priorPhaseExitCode,
                    priorTreeEmptyProven, profileExitCode, tick>>

EstablishmentFails ==
    /\ lifecycle \in {"gated", "owned"}
    /\ lifecycle' = "cleanup"
    /\ cause' = "establishment"
    /\ requestedExitCode' = 125
    /\ UNCHANGED <<phaseLimit, phaseIndex, owned, gateOpen, active,
                    terminationFailed, treeEmptyProven, phaseExitCode,
                    priorPhaseExitCode, priorTreeEmptyProven,
                    profileExitCode, tick>>

EstablishmentBudgetExpires ==
    /\ lifecycle \in {"gated", "owned"}
    /\ tick = MaxTick
    /\ lifecycle' = "cleanup"
    /\ cause' = "timeout"
    /\ requestedExitCode' = 124
    /\ UNCHANGED <<phaseLimit, phaseIndex, owned, gateOpen, active,
                    terminationFailed, treeEmptyProven, phaseExitCode,
                    priorPhaseExitCode, priorTreeEmptyProven,
                    profileExitCode, tick>>

CleanExit ==
    /\ lifecycle = "running"
    /\ lifecycle' = "empty"
    /\ active' = 0
    /\ cause' = "clean"
    /\ requestedExitCode' = 0
    /\ treeEmptyProven' = TRUE
    /\ phaseExitCode' = 0
    /\ UNCHANGED <<phaseLimit, phaseIndex, owned, gateOpen,
                    terminationFailed, priorPhaseExitCode,
                    priorTreeEmptyProven, profileExitCode, tick>>

FailureExit ==
    /\ lifecycle = "running"
    /\ lifecycle' = "empty"
    /\ active' = 0
    /\ cause' = "failure"
    /\ requestedExitCode' = 7
    /\ treeEmptyProven' = TRUE
    /\ phaseExitCode' = 7
    /\ UNCHANGED <<phaseLimit, phaseIndex, owned, gateOpen,
                    terminationFailed, priorPhaseExitCode,
                    priorTreeEmptyProven, profileExitCode, tick>>

LeaderExitWithSurvivor ==
    /\ lifecycle = "running"
    /\ lifecycle' = "cleanup"
    /\ active' = 1
    /\ cause' = "survivor"
    /\ requestedExitCode' = 125
    /\ UNCHANGED <<phaseLimit, phaseIndex, owned, gateOpen,
                    terminationFailed, treeEmptyProven, phaseExitCode,
                    priorPhaseExitCode, priorTreeEmptyProven,
                    profileExitCode, tick>>

Timeout ==
    /\ lifecycle = "running"
    /\ lifecycle' = "cleanup"
    /\ cause' = "timeout"
    /\ requestedExitCode' = 124
    /\ UNCHANGED <<phaseLimit, phaseIndex, owned, gateOpen, active,
                    terminationFailed, treeEmptyProven, phaseExitCode,
                    priorPhaseExitCode, priorTreeEmptyProven,
                    profileExitCode, tick>>

Interrupt ==
    /\ lifecycle \in {"gated", "owned", "running"}
    /\ \E code \in InterruptExitCodes:
        /\ lifecycle' = "cleanup"
        /\ cause' = "interrupt"
        /\ requestedExitCode' = code
        /\ UNCHANGED <<phaseLimit, phaseIndex, owned, gateOpen, active,
                        terminationFailed, treeEmptyProven, phaseExitCode,
                        priorPhaseExitCode, priorTreeEmptyProven,
                        profileExitCode, tick>>

TerminationSucceeds ==
    /\ lifecycle = "cleanup"
    /\ tick <= MaxTick
    /\ lifecycle' = "empty"
    /\ active' = 0
    /\ treeEmptyProven' = TRUE
    /\ phaseExitCode' = requestedExitCode
    /\ UNCHANGED <<phaseLimit, phaseIndex, owned, gateOpen, cause,
                    requestedExitCode, terminationFailed,
                    priorPhaseExitCode, priorTreeEmptyProven,
                    profileExitCode, tick>>

TerminationFails ==
    /\ lifecycle = "cleanup"
    /\ tick < MaxTick
    /\ terminationFailed' = TRUE
    /\ UNCHANGED <<lifecycle, phaseLimit, phaseIndex, owned, gateOpen,
                    active, cause, requestedExitCode, treeEmptyProven,
                    phaseExitCode, priorPhaseExitCode,
                    priorTreeEmptyProven, profileExitCode, tick>>

TaskkillSucceeds ==
    /\ lifecycle = "cleanup"
    /\ terminationFailed
    /\ tick <= MaxTick
    /\ lifecycle' = "empty"
    /\ active' = 0
    /\ treeEmptyProven' = TRUE
    /\ phaseExitCode' = requestedExitCode
    /\ UNCHANGED <<phaseLimit, phaseIndex, owned, gateOpen, cause,
                    requestedExitCode, terminationFailed,
                    priorPhaseExitCode, priorTreeEmptyProven,
                    profileExitCode, tick>>

(***************************************************************************)
(* These transitions abstract Windows kill-on-last-Job-handle at runner    *)
(* process exit.  They deliberately do not create an empty-query receipt,  *)
(* so only infrastructure-failure exit 125 can be published.               *)
(***************************************************************************)
TaskkillFailsAndOwnerCloses ==
    /\ lifecycle = "cleanup"
    /\ terminationFailed
    /\ tick <= MaxTick
    /\ lifecycle' = "empty"
    /\ active' = 0
    /\ treeEmptyProven' = FALSE
    /\ phaseExitCode' = 125
    /\ UNCHANGED <<phaseLimit, phaseIndex, owned, gateOpen, cause,
                    requestedExitCode, terminationFailed,
                    priorPhaseExitCode, priorTreeEmptyProven,
                    profileExitCode, tick>>

CleanupDeadlineExpires ==
    /\ lifecycle = "cleanup"
    /\ tick = MaxTick
    /\ lifecycle' = "empty"
    /\ active' = 0
    /\ treeEmptyProven' = FALSE
    /\ phaseExitCode' = 125
    /\ UNCHANGED <<phaseLimit, phaseIndex, owned, gateOpen, cause,
                    requestedExitCode, terminationFailed,
                    priorPhaseExitCode, priorTreeEmptyProven,
                    profileExitCode, tick>>

PublishPhaseResult ==
    /\ lifecycle = "empty"
    /\ lifecycle' = "returned"
    /\ owned' = FALSE
    /\ gateOpen' = FALSE
    /\ UNCHANGED <<phaseLimit, phaseIndex, active, cause,
                    requestedExitCode, terminationFailed, treeEmptyProven,
                    phaseExitCode, priorPhaseExitCode,
                    priorTreeEmptyProven, profileExitCode, tick>>

OwnerCloseFails ==
    /\ lifecycle = "empty"
    /\ owned
    /\ lifecycle' = "returned"
    /\ cause' = "owner-close"
    /\ phaseExitCode' = 125
    /\ UNCHANGED <<phaseLimit, phaseIndex, owned, gateOpen, active,
                    requestedExitCode, terminationFailed, treeEmptyProven,
                    priorPhaseExitCode, priorTreeEmptyProven,
                    profileExitCode, tick>>

StartNextPhase ==
    /\ lifecycle = "returned"
    /\ phaseIndex < phaseLimit
    /\ phaseExitCode = 0
    /\ treeEmptyProven
    /\ tick < MaxTick
    /\ lifecycle' = "unstarted"
    /\ phaseIndex' = phaseIndex + 1
    /\ owned' = FALSE
    /\ gateOpen' = FALSE
    /\ active' = 0
    /\ cause' = "none"
    /\ requestedExitCode' = -1
    /\ terminationFailed' = FALSE
    /\ treeEmptyProven' = FALSE
    /\ phaseExitCode' = -1
    /\ priorPhaseExitCode' = phaseExitCode
    /\ priorTreeEmptyProven' = treeEmptyProven
    /\ UNCHANGED <<phaseLimit, profileExitCode, tick>>

BetweenPhaseBudgetExpires ==
    /\ lifecycle = "returned"
    /\ phaseIndex < phaseLimit
    /\ phaseExitCode = 0
    /\ treeEmptyProven
    /\ tick = MaxTick
    /\ lifecycle' = "profile-returned"
    /\ profileExitCode' = 124
    /\ UNCHANGED <<phaseLimit, phaseIndex, owned, gateOpen, active, cause,
                    requestedExitCode, terminationFailed, treeEmptyProven,
                    phaseExitCode, priorPhaseExitCode,
                    priorTreeEmptyProven, tick>>

FinishProfile ==
    /\ lifecycle = "returned"
    /\ \/ phaseIndex = phaseLimit
       \/ phaseExitCode /= 0
    /\ lifecycle' = "profile-returned"
    /\ profileExitCode' = phaseExitCode
    /\ UNCHANGED <<phaseLimit, phaseIndex, owned, gateOpen, active, cause,
                    requestedExitCode, terminationFailed, treeEmptyProven,
                    phaseExitCode, priorPhaseExitCode,
                    priorTreeEmptyProven, tick>>

AdvanceTime ==
    /\ lifecycle \in {"unstarted", "gated", "owned", "running",
                       "cleanup", "returned"}
    /\ tick < MaxTick
    /\ tick' = tick + 1
    /\ UNCHANGED <<lifecycle, phaseLimit, phaseIndex, owned, gateOpen,
                    active, cause, requestedExitCode, terminationFailed,
                    treeEmptyProven, phaseExitCode, priorPhaseExitCode,
                    priorTreeEmptyProven, profileExitCode>>

Next ==
    \/ CreateGatedSupervisor
    \/ AssignOwner
    \/ OpenStartGate
    \/ PreStartBudgetExpires
    \/ EstablishmentFails
    \/ EstablishmentBudgetExpires
    \/ CleanExit
    \/ FailureExit
    \/ LeaderExitWithSurvivor
    \/ Timeout
    \/ Interrupt
    \/ TerminationSucceeds
    \/ TerminationFails
    \/ TaskkillSucceeds
    \/ TaskkillFailsAndOwnerCloses
    \/ CleanupDeadlineExpires
    \/ PublishPhaseResult
    \/ OwnerCloseFails
    \/ StartNextPhase
    \/ BetweenPhaseBudgetExpires
    \/ FinishProfile
    \/ AdvanceTime

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ lifecycle \in Lifecycles
    /\ phaseLimit \in 1..MaxPhases
    /\ phaseIndex \in 1..MaxPhases
    /\ owned \in BOOLEAN
    /\ gateOpen \in BOOLEAN
    /\ active \in 0..2
    /\ cause \in Causes
    /\ requestedExitCode \in ExitCodes
    /\ terminationFailed \in BOOLEAN
    /\ treeEmptyProven \in BOOLEAN
    /\ phaseExitCode \in ExitCodes
    /\ priorPhaseExitCode \in ExitCodes
    /\ priorTreeEmptyProven \in BOOLEAN
    /\ profileExitCode \in ExitCodes
    /\ tick \in 0..MaxTick

GateRequiresOwnership == gateOpen => owned
TargetRequiresOwnership == active = 2 => owned /\ gateOpen
PhaseNeverExceedsProfile == phaseIndex <= phaseLimit
LaterPhaseRequiresCleanEmptyPredecessor ==
    phaseIndex > 1 => priorPhaseExitCode = 0 /\ priorTreeEmptyProven
SemanticPhaseReceiptRequiresEmptyProof ==
    lifecycle \in {"returned", "profile-returned"}
        /\ phaseExitCode \in SemanticExitCodes => treeEmptyProven
ProfileReturnHasNoOwnedTree == lifecycle = "profile-returned" => active = 0
SuccessfulProfileRunsEveryPhase ==
    lifecycle = "profile-returned" /\ profileExitCode = 0 =>
        phaseIndex = phaseLimit /\ phaseExitCode = 0 /\ treeEmptyProven
EarlyProfileReturnIsFailure ==
    lifecycle = "profile-returned" /\ phaseIndex < phaseLimit =>
        profileExitCode /= 0
SurvivorCannotSucceed ==
    lifecycle \in {"returned", "profile-returned"} /\ cause = "survivor" =>
        phaseExitCode = 125
OwnerCloseFailureCannotSucceed ==
    lifecycle \in {"returned", "profile-returned"} /\ cause = "owner-close" =>
        phaseExitCode = 125
TimeoutReceiptIsExact == phaseExitCode = 124 => cause = "timeout"
InterruptReceiptIsExact ==
    phaseExitCode \in InterruptExitCodes => cause = "interrupt"
ProfileTimeoutReceiptIsExact ==
    lifecycle = "profile-returned" /\ profileExitCode = 124 =>
        \/ phaseExitCode = 124 /\ cause = "timeout"
        \/ phaseExitCode = 0 /\ phaseIndex < phaseLimit /\ tick = MaxTick
ProfileInterruptReceiptIsExact ==
    lifecycle = "profile-returned" /\ profileExitCode \in InterruptExitCodes =>
        phaseExitCode = profileExitCode /\ cause = "interrupt"
DeadlineNeverExtends == tick <= MaxTick

=============================================================================
