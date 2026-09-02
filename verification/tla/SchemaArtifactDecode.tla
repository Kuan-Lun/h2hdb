----------------------- MODULE SchemaArtifactDecode -----------------------
EXTENDS Naturals, TLC

(***************************************************************************)
(* Finite acceptance model for both generated-resource trust boundaries.   *)
(* Generic/build checks include the abstract opcode and memo preflight. The *)
(* production runtime may omit that repeated scan only under PinnedCohort,  *)
(* the explicit abstraction that size/digest authentication selected the   *)
(* exact resource previously accepted by build/distribution checks. TLC     *)
(* checks only the finite InputKinds below. It does not prove Python,        *)
(* pickle, pickletools, SHA-256 (including collision resistance),           *)
(* importlib.resources, or filesystem/zip behavior refines these            *)
(* predicates. Differential and fault tests provide that evidence.          *)
(***************************************************************************)

NoInput == "none"
NoMode == "none"
Valid == "valid"
BuildMode == "build"
RuntimeMode == "runtime"
Modes == {BuildMode, RuntimeMode}
InputKinds == {
    Valid,
    "resource-digest-corrupt",
    "truncated",
    "trailing",
    "resource-oversize",
    "protocol-invalid",
    "frame-overflow",
    "opcode-overflow",
    "expanded-byte-overflow",
    "scalar-overflow",
    "work-overflow",
    "forbidden-opcode",
    "node-overflow",
    "memo-dag-overflow",
    "depth-overflow",
    "noncanonical-order",
    "dictionary-key-invalid",
    "cycle",
    "shared-mutable",
    "type-invalid"
}

VARIABLES phase, mode, candidate, accepted

vars == <<phase, mode, candidate, accepted>>

SizeOK(input) == input /= "resource-oversize"
DigestOK(input) == input /= "resource-digest-corrupt"
StreamComplete(input) == input /= "truncated"
NoTrailingData(input) == input /= "trailing"
ProtocolOK(input) == input /= "protocol-invalid"
FrameBoundOK(input) == input /= "frame-overflow"
OpcodeBoundOK(input) == input /= "opcode-overflow"
ExpandedByteBoundOK(input) == input /= "expanded-byte-overflow"
ScalarBoundOK(input) == input /= "scalar-overflow"
ConstructionWorkBoundOK(input) == input /= "work-overflow"
OpcodeSurfaceOK(input) == input /= "forbidden-opcode"
NodeBoundOK(input) == input \notin {"node-overflow", "memo-dag-overflow"}
DepthBoundOK(input) == input /= "depth-overflow"
CanonicalOrder(input) == input /= "noncanonical-order"
AcyclicOwnedTree(input) == input \notin {"cycle", "shared-mutable"}
ClosedTypeSurface(input) ==
    input \notin {"dictionary-key-invalid", "type-invalid"}

FullPreflightChecksPass(input) ==
    /\ SizeOK(input)
    /\ DigestOK(input)
    /\ StreamComplete(input)
    /\ NoTrailingData(input)
    /\ ProtocolOK(input)
    /\ FrameBoundOK(input)
    /\ OpcodeBoundOK(input)
    /\ ExpandedByteBoundOK(input)
    /\ ScalarBoundOK(input)
    /\ ConstructionWorkBoundOK(input)
    /\ OpcodeSurfaceOK(input)
    /\ NodeBoundOK(input)
    /\ DepthBoundOK(input)
    /\ CanonicalOrder(input)
    /\ AcyclicOwnedTree(input)
    /\ ClosedTypeSurface(input)

PinnedCohort(input) == input = Valid

RuntimeChecksPass(input) ==
    /\ PinnedCohort(input)
    /\ SizeOK(input)
    /\ DigestOK(input)
    /\ StreamComplete(input)
    /\ NoTrailingData(input)
    /\ ProtocolOK(input)
    /\ NodeBoundOK(input)
    /\ DepthBoundOK(input)
    /\ CanonicalOrder(input)
    /\ AcyclicOwnedTree(input)
    /\ ClosedTypeSurface(input)

ChecksFor(modeValue, input) ==
    IF modeValue = BuildMode
    THEN FullPreflightChecksPass(input)
    ELSE RuntimeChecksPass(input)

Init ==
    /\ phase = "idle"
    /\ mode = NoMode
    /\ candidate = NoInput
    /\ accepted = FALSE

Choose ==
    /\ phase = "idle"
    /\ \E modeValue \in Modes, input \in InputKinds:
        /\ mode' = modeValue
        /\ candidate' = input
        /\ phase' = "checking"
        /\ accepted' = FALSE

Verify ==
    /\ phase = "checking"
    /\ phase' = "done"
    /\ accepted' = ChecksFor(mode, candidate)
    /\ UNCHANGED <<mode, candidate>>

Done ==
    /\ phase = "done"
    /\ UNCHANGED vars

Next == Choose \/ Verify \/ Done

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in {"idle", "checking", "done"}
    /\ mode \in Modes \cup {NoMode}
    /\ candidate \in InputKinds \cup {NoInput}
    /\ accepted \in BOOLEAN

InvalidNeverAccepted == accepted => candidate = Valid

AcceptanceMatchesChecks ==
    (phase = "done") => (accepted = ChecksFor(mode, candidate))

RuntimeAcceptanceRequiresPinnedCohort ==
    (accepted /\ mode = RuntimeMode) => PinnedCohort(candidate)

BuildAcceptanceIncludesFullPreflight ==
    (accepted /\ mode = BuildMode) => FullPreflightChecksPass(candidate)

ValidAcceptedAtCompletion ==
    (phase = "done" /\ candidate = Valid) => accepted

=============================================================================
