---------------------- MODULE CatalogChildHydration ----------------------
EXTENDS Naturals, Sequences, TLC

(***************************************************************************
Finite operational model for contributor/subject keyset hydration.  Durable
child rows are abstracted as the strictly ordered sequence 1..ChildCount.
Each transition reads only the suffix strictly after `cursor`, appends at most
PageLimit rows in source order, and advances to the exact last row returned.

TLC exhausts only the configured finite child count and page limit.  The model
does not establish Python loop behavior, SQL collation/order, canonical-value
validation, or SQLite/MariaDB refinement; unbounded Lean sequence refinement
and runtime backend/differential tests cover those separate boundaries.
***************************************************************************)

CONSTANTS ChildCount, PageLimit

ASSUME /\ ChildCount \in Nat \ {0}
       /\ PageLimit \in Nat \ {0}

Source == [position \in 1..ChildCount |-> position]

VARIABLES cursor, emitted, lastBatchSize, complete, heartbeat

vars == <<cursor, emitted, lastBatchSize, complete, heartbeat>>

Init ==
    /\ cursor = 0
    /\ emitted = <<>>
    /\ lastBatchSize = 0
    /\ complete = FALSE
    /\ heartbeat = FALSE

HydratePage ==
    /\ cursor < ChildCount
    /\ LET finish ==
               IF cursor + PageLimit <= ChildCount
               THEN cursor + PageLimit
               ELSE ChildCount
           batch == SubSeq(Source, cursor + 1, finish)
       IN /\ emitted' = emitted \o batch
          /\ cursor' = finish
          /\ lastBatchSize' = Len(batch)
          /\ complete' = (finish = ChildCount)
          /\ UNCHANGED heartbeat

TerminalProbe ==
    /\ complete
    /\ heartbeat' = ~heartbeat
    /\ UNCHANGED <<cursor, emitted, lastBatchSize, complete>>

Next == HydratePage \/ TerminalProbe

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ cursor \in 0..ChildCount
    /\ emitted \in Seq(1..ChildCount)
    /\ lastBatchSize \in 0..PageLimit
    /\ complete \in BOOLEAN
    /\ heartbeat \in BOOLEAN

EveryFetchIsHardBounded == lastBatchSize <= PageLimit

EmittedRowsAreExactOrderedPrefix ==
    emitted = IF cursor = 0 THEN <<>> ELSE SubSeq(Source, 1, cursor)

CursorEqualsEmittedLength == cursor = Len(emitted)

CompletionIsExact == complete <=> cursor = ChildCount

CompletedResultEqualsReference == complete => emitted = Source

=============================================================================
