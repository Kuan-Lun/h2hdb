import Std

/-!
# Verified artifact-source slice isolation

Core stages every selected artifact source into one aggregate immutable spool.
Each logical member has its own cursor, while a short critical section makes
the shared physical seek-and-read one indivisible operation.  The model below
states the authorized observation: reading a locked slice yields exactly the
same bounded extent as reading an independent immutable source, and mapping
that operation over any finite ordered member list preserves all result bytes.

The theorems are unbounded over spool bytes, offsets, lengths, and member lists.
They model a correctly implemented mutual-exclusion critical section as the
pure `lockedRead`; they do not prove Python's `threading.Lock`, file-object
cursor behavior, adapter scheduling, or filesystem effects.  A deterministic
overlap regression and the real-corpus worker-count differential test cover
those implementation boundaries.
-/

namespace H2HDB.Verification.ArtifactSourceSliceIsolation

structure Extent where
  offset : Nat
  length : Nat
deriving DecidableEq, Repr

abbrev ByteSpool := List Nat

def independentRead (spool : ByteSpool) (extent : Extent) : List Nat :=
  (spool.drop extent.offset).take extent.length

/-- One completed physical seek-and-read critical section. -/
def lockedRead (spool : ByteSpool) (extent : Extent) : List Nat :=
  (spool.drop extent.offset).take extent.length

theorem one_locked_slice_equals_independent_source
    (spool : ByteSpool)
    (extent : Extent) :
    lockedRead spool extent = independentRead spool extent := by
  rfl

def referenceMembers
    (spool : ByteSpool)
    (extents : List Extent) : List (List Nat) :=
  extents.map (independentRead spool)

def lockedMembers
    (spool : ByteSpool)
    (extents : List Extent) : List (List Nat) :=
  extents.map (lockedRead spool)

theorem all_locked_member_bytes_equal_independent_reference
    (spool : ByteSpool)
    (extents : List Extent) :
    lockedMembers spool extents = referenceMembers spool extents := by
  rfl

def BoundedMembers (extents : List Extent) : Prop :=
  extents.length ≤ 4097

theorem bounded_renderer_input_preserves_exact_member_bytes
    (spool : ByteSpool)
    (extents : List Extent)
    (_bounded : BoundedMembers extents) :
    lockedMembers spool extents = referenceMembers spool extents :=
  all_locked_member_bytes_equal_independent_reference spool extents

end H2HDB.Verification.ArtifactSourceSliceIsolation
