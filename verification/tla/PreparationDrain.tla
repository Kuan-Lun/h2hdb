---------------------------- MODULE PreparationDrain ----------------------------
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************
Finite crash/retry model of the bounded superseded-preparation drainage.

Durable rows are 1..RowCount in index (seek) order; `abandoned` is the set
already ABANDONED.  The driver issues the durable position (the least
matching row), then commits one page: the first PageLimit matching rows at or
after that position.  Between the two steps the process may crash, the commit
response may be lost, or a delayed retry may carry a stale position.  A commit
reloads the durable position and refuses a stale one with zero writes.

TLC exhausts only the configured RowCount and PageLimit.  The model does not
establish SQL ordering, index usage, transaction atomicity, or that Python
implements it; the Lean theorems are unbounded for the abstract page
arithmetic, and the runtime, fault and live-backend tests cover the rest.
***************************************************************************)

CONSTANTS RowCount, PageLimit

ASSUME /\ RowCount \in Nat \ {0}
       /\ PageLimit \in Nat \ {0}

Rows == 1..RowCount

PagesNeeded == (RowCount + PageLimit - 1) \div PageLimit

VARIABLES
    abandoned,      \* durable: rows already ABANDONED
    issued,         \* the position the driver holds (0 = none)
    stale,          \* positions of pages whose commit response was lost
    lastCommitted,  \* the position of the most recently committed page (0 = none)
    lastPage,       \* the rows that page abandoned
    commits         \* number of committed pages

vars == <<abandoned, issued, stale, lastCommitted, lastPage, commits>>

Matching == Rows \ abandoned

Position == IF Matching = {} THEN 0 ELSE CHOOSE r \in Matching : \A q \in Matching : r <= q

\* The first PageLimit matching rows at or after `pos`.
Page(pos) ==
    {r \in Matching : r >= pos /\ Cardinality({q \in Matching : pos <= q /\ q < r}) < PageLimit}

Init ==
    /\ abandoned = {}
    /\ issued = 0
    /\ stale = {}
    /\ lastCommitted = 0
    /\ lastPage = {}
    /\ commits = 0

\* Read the durable position in one transaction.
Issue ==
    /\ Matching # {}
    /\ issued' = Position
    /\ UNCHANGED <<abandoned, stale, lastCommitted, lastPage, commits>>

\* Commit the page from the issued position; the position is reloaded first.
Commit ==
    /\ issued # 0
    /\ issued = Position
    /\ abandoned' = abandoned \cup Page(issued)
    /\ lastPage' = Page(issued)
    /\ lastCommitted' = issued
    /\ commits' = commits + 1
    /\ issued' = 0
    /\ UNCHANGED stale

\* The page committed but its response was lost: the driver keeps (or a
\* delayed retry later re-presents) the old position.
LoseResponse ==
    /\ issued # 0
    /\ issued = Position
    /\ abandoned' = abandoned \cup Page(issued)
    /\ lastPage' = Page(issued)
    /\ lastCommitted' = issued
    /\ commits' = commits + 1
    /\ stale' = stale \cup {issued}
    /\ issued' = 0

\* A delayed retry re-presents a lost page's position: refused, zero writes.
StaleRetry ==
    /\ \E pos \in stale : pos # Position
    /\ UNCHANGED vars

\* The process dies between issue and commit; the driver re-issues later.
Crash ==
    /\ issued # 0
    /\ issued' = 0
    /\ UNCHANGED <<abandoned, stale, lastCommitted, lastPage, commits>>

Terminal ==
    /\ Matching = {}
    /\ UNCHANGED vars

Next == Issue \/ Commit \/ LoseResponse \/ StaleRetry \/ Crash \/ Terminal

Spec == Init /\ [][Next]_vars /\ WF_vars(Issue) /\ SF_vars(Commit)

TypeOK ==
    /\ abandoned \subseteq Rows
    /\ issued \in 0..RowCount
    /\ stale \subseteq Rows
    /\ lastCommitted \in 0..RowCount
    /\ lastPage \subseteq Rows
    /\ commits \in 0..PagesNeeded

\* Every committed page is hard bounded.
PageIsHardBounded == Cardinality(lastPage) <= PageLimit

\* A committed page only ever abandoned rows that were still matching, and
\* they stay abandoned: no row is abandoned twice and none is revived.
AbandonedIsMonotone == lastPage \subseteq abandoned

\* The liveness fence: after a committed page the next durable position is
\* strictly past the position that page started from.
PositionStrictlyAdvances ==
    (lastCommitted # 0 /\ Matching # {}) => Position > lastCommitted

\* No page from a stale position is ever written: a stale position is never
\* the durable position again once its page committed.
StalePositionNeverCurrent == \A pos \in stale : pos # Position

\* Draining never takes more committed pages than ceil(RowCount / PageLimit),
\* whatever crashes, lost responses and retries interleave.
CommitsAreBounded == commits <= PagesNeeded

\* Every behavior that keeps issuing drains the build.
Drains == <>[](Matching = {})

=============================================================================
