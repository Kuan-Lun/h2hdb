----------------------- MODULE SchemaArtifactDecode -----------------------
EXTENDS Naturals, TLC

(***************************************************************************)
(* Finite acceptance model for the generated schema resource trust         *)
(* boundary.  TLC checks only the explicit InputKinds below.  This model    *)
(* does not prove Python, JSON, SHA-256, zlib, importlib.resources, or       *)
(* filesystem/zip behavior refines these predicates.  Runtime differential  *)
(* and fault tests provide that evidence.                                   *)
(***************************************************************************)

NoInput == "none"
Valid == "valid"
InputKinds == {
    Valid,
    "compressed-corrupt",
    "raw-corrupt",
    "truncated",
    "trailing",
    "compressed-oversize",
    "raw-bomb",
    "node-overflow",
    "depth-overflow",
    "duplicate-key",
    "noncanonical-json",
    "unknown-tag",
    "type-invalid"
}

VARIABLES phase, candidate, accepted

vars == <<phase, candidate, accepted>>

SizeOK(input) == input /= "compressed-oversize"
CompressedDigestOK(input) == input /= "compressed-corrupt"
StreamComplete(input) == input /= "truncated"
NoTrailingData(input) == input /= "trailing"
RawBoundOK(input) == input /= "raw-bomb"
RawDigestOK(input) == input /= "raw-corrupt"
NodeBoundOK(input) == input /= "node-overflow"
DepthBoundOK(input) == input /= "depth-overflow"
CanonicalJSON(input) ==
    input \notin {"duplicate-key", "noncanonical-json"}
ClosedTypeSurface(input) ==
    input \notin {"unknown-tag", "type-invalid"}

AllChecksPass(input) ==
    /\ SizeOK(input)
    /\ CompressedDigestOK(input)
    /\ StreamComplete(input)
    /\ NoTrailingData(input)
    /\ RawBoundOK(input)
    /\ RawDigestOK(input)
    /\ NodeBoundOK(input)
    /\ DepthBoundOK(input)
    /\ CanonicalJSON(input)
    /\ ClosedTypeSurface(input)

Init ==
    /\ phase = "idle"
    /\ candidate = NoInput
    /\ accepted = FALSE

Choose ==
    /\ phase = "idle"
    /\ \E input \in InputKinds:
        /\ candidate' = input
        /\ phase' = "checking"
        /\ accepted' = FALSE

Verify ==
    /\ phase = "checking"
    /\ phase' = "done"
    /\ accepted' = AllChecksPass(candidate)
    /\ UNCHANGED candidate

Done ==
    /\ phase = "done"
    /\ UNCHANGED vars

Next == Choose \/ Verify \/ Done

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in {"idle", "checking", "done"}
    /\ candidate \in InputKinds \cup {NoInput}
    /\ accepted \in BOOLEAN

InvalidNeverAccepted == accepted => candidate = Valid

AcceptanceMatchesChecks ==
    (phase = "done") => (accepted = AllChecksPass(candidate))

ValidAcceptedAtCompletion ==
    (phase = "done" /\ candidate = Valid) => accepted

=============================================================================
