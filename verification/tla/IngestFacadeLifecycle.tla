---------------------- MODULE IngestFacadeLifecycle ----------------------
EXTENDS Naturals, TLC

(***************************************************************************
Finite interleaving model for one disposable receipt and an ingest facade.
The receipt has exactly one abstract owner.  Install and take may linearize on
either side of close; close must prevent cache resurrection and must not close
a receipt whose ownership already moved to a prepared-step borrower.

TLC explores only this finite ownership abstraction.  It does not establish
Python lock/Event behavior, resource destructor effects, or transaction and
adapter semantics.  Runtime barrier-driven race tests cover the implementation
interleavings, while the Lean model proves the transition algebra independently
of any finite state bound.
***************************************************************************)

Owners == {"CALLER", "CACHE", "BORROWER", "RELEASED"}

VARIABLES open, owner, releaseCount, heartbeat

vars == <<open, owner, releaseCount, heartbeat>>

Init ==
    /\ open = TRUE
    /\ owner = "CALLER"
    /\ releaseCount = 0
    /\ heartbeat = FALSE

Install ==
    /\ owner = "CALLER"
    /\ IF open
          THEN /\ owner' = "CACHE"
               /\ UNCHANGED releaseCount
          ELSE /\ owner' = "RELEASED"
               /\ releaseCount' = releaseCount + 1
    /\ UNCHANGED <<open, heartbeat>>

Take ==
    /\ open
    /\ owner = "CACHE"
    /\ owner' = "BORROWER"
    /\ UNCHANGED <<open, releaseCount, heartbeat>>

Close ==
    /\ open' = FALSE
    /\ IF owner = "CACHE"
          THEN /\ owner' = "RELEASED"
               /\ releaseCount' = releaseCount + 1
          ELSE /\ UNCHANGED <<owner, releaseCount>>
    /\ UNCHANGED heartbeat

ReleaseCaller ==
    /\ owner \in {"CALLER", "BORROWER"}
    /\ owner' = "RELEASED"
    /\ releaseCount' = releaseCount + 1
    /\ UNCHANGED <<open, heartbeat>>

TerminalProbe ==
    /\ owner = "RELEASED"
    /\ heartbeat' = ~heartbeat
    /\ UNCHANGED <<open, owner, releaseCount>>

Next == Install \/ Take \/ Close \/ ReleaseCaller \/ TerminalProbe

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ open \in BOOLEAN
    /\ owner \in Owners
    /\ releaseCount \in Nat
    /\ heartbeat \in BOOLEAN

ExactlyOneOwner == owner \in Owners

AtMostOneRelease == releaseCount <= 1

ClosedFacadeOwnsNoCache == ~open => owner # "CACHE"

ResourceConserved ==
    releaseCount + (IF owner = "RELEASED" THEN 0 ELSE 1) = 1

=============================================================================
