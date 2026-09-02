import Std

/-!
# Generated schema artifact decode refinement

This model represents the closed primitive tree carried by the generated
schema resource.  A runtime decode is admitted only when its finite envelope
is within the shipped byte/node/depth caps and the decoder returns the exact
source tree.  Under that explicit exactness assumption, every schema
projection is unchanged.

The theorems are unbounded over encoded values, artifact trees, and projection
result types.  They do not prove that Python's JSON parser, zlib, package
resources, SHA-256 implementation, or handwritten runtime codec establishes
the exact-decode premise.  Runtime structural differential, property, fault,
zipimport, wheel, and resource-bound tests provide that refinement evidence.
-/

namespace H2HDB.Verification.SchemaArtifactDecode

mutual
inductive ArtifactTree where
  | null
  | boolean (value : Bool)
  | integer (value : Int)
  | string (value : String)
  | bytes (value : List UInt8)
  | list (values : ArtifactTrees)
  | tuple (values : ArtifactTrees)
  | dictionary (entries : ArtifactEntries)

inductive ArtifactTrees where
  | nil
  | cons (head : ArtifactTree) (tail : ArtifactTrees)

inductive ArtifactEntries where
  | nil
  | cons (key : String) (value : ArtifactTree) (tail : ArtifactEntries)
end

mutual
inductive CanonicalWire where
  | null
  | boolean (value : Bool)
  | integer (value : Int)
  | string (value : String)
  | taggedBytes (value : List UInt8)
  | nativeList (values : CanonicalWires)
  | taggedTuple (values : CanonicalWires)
  | nativeDictionary (entries : CanonicalEntries)

inductive CanonicalWires where
  | nil
  | cons (head : CanonicalWire) (tail : CanonicalWires)

inductive CanonicalEntries where
  | nil
  | cons (key : String) (value : CanonicalWire) (tail : CanonicalEntries)
end

mutual
def encodeTree : ArtifactTree → CanonicalWire
  | .null => .null
  | .boolean value => .boolean value
  | .integer value => .integer value
  | .string value => .string value
  | .bytes value => .taggedBytes value
  | .list values => .nativeList (encodeTrees values)
  | .tuple values => .taggedTuple (encodeTrees values)
  | .dictionary entries => .nativeDictionary (encodeEntries entries)

def encodeTrees : ArtifactTrees → CanonicalWires
  | .nil => .nil
  | .cons head tail => .cons (encodeTree head) (encodeTrees tail)

def encodeEntries : ArtifactEntries → CanonicalEntries
  | .nil => .nil
  | .cons key value tail =>
      .cons key (encodeTree value) (encodeEntries tail)
end

mutual
def decodeTree : CanonicalWire → ArtifactTree
  | .null => .null
  | .boolean value => .boolean value
  | .integer value => .integer value
  | .string value => .string value
  | .taggedBytes value => .bytes value
  | .nativeList values => .list (decodeTrees values)
  | .taggedTuple values => .tuple (decodeTrees values)
  | .nativeDictionary entries => .dictionary (decodeEntries entries)

def decodeTrees : CanonicalWires → ArtifactTrees
  | .nil => .nil
  | .cons head tail => .cons (decodeTree head) (decodeTrees tail)

def decodeEntries : CanonicalEntries → ArtifactEntries
  | .nil => .nil
  | .cons key value tail =>
      .cons key (decodeTree value) (decodeEntries tail)
end

mutual
theorem reference_codec_roundtrip (tree : ArtifactTree) :
    decodeTree (encodeTree tree) = tree := by
  cases tree <;>
    simp [encodeTree, decodeTree, reference_trees_roundtrip,
      reference_entries_roundtrip]

theorem reference_trees_roundtrip (trees : ArtifactTrees) :
    decodeTrees (encodeTrees trees) = trees := by
  cases trees <;>
    simp [encodeTrees, decodeTrees, reference_codec_roundtrip,
      reference_trees_roundtrip]

theorem reference_entries_roundtrip (entries : ArtifactEntries) :
    decodeEntries (encodeEntries entries) = entries := by
  cases entries <;>
    simp [encodeEntries, decodeEntries, reference_codec_roundtrip,
      reference_entries_roundtrip]
end

theorem list_and_tuple_tags_are_distinct (values : ArtifactTrees) :
    encodeTree (.list values) ≠ encodeTree (.tuple values) := by
  intro contradiction
  cases contradiction

theorem string_and_bytes_tags_are_distinct
    (stringValue : String)
    (byteValue : List UInt8) :
    encodeTree (.string stringValue) ≠ encodeTree (.bytes byteValue) := by
  intro contradiction
  cases contradiction

structure Envelope where
  compressedBytes : Nat
  rawBytes : Nat
  nodes : Nat
  depth : Nat
deriving DecidableEq, Repr

def maxCompressedBytes : Nat := 2 * 1024 * 1024
def maxRawBytes : Nat := 32 * 1024 * 1024
def maxNodes : Nat := 1000000
def maxDepth : Nat := 64

def WithinBounds (envelope : Envelope) : Prop :=
  envelope.compressedBytes ≤ maxCompressedBytes ∧
    envelope.rawBytes ≤ maxRawBytes ∧
    envelope.nodes ≤ maxNodes ∧
    envelope.depth ≤ maxDepth

def SuccessfulExactDecode
    (decode : Encoded → Option ArtifactTree)
    (wire : Encoded)
    (source : ArtifactTree) : Prop :=
  decode wire = some source

def Admitted
    (decode : Encoded → Option ArtifactTree)
    (wire : Encoded)
    (source : ArtifactTree)
    (envelope : Envelope) : Prop :=
  WithinBounds envelope ∧ SuccessfulExactDecode decode wire source

theorem admitted_payload_is_bounded
    (decode : Encoded → Option ArtifactTree)
    (wire : Encoded)
    (source : ArtifactTree)
    (envelope : Envelope)
    (admitted : Admitted decode wire source envelope) :
    WithinBounds envelope := by
  exact admitted.1

theorem successful_exact_decode_preserves_projection
    (decode : Encoded → Option ArtifactTree)
    (wire : Encoded)
    (source : ArtifactTree)
    (project : ArtifactTree → Projection)
    (exact : SuccessfulExactDecode decode wire source) :
    (decode wire).map project = some (project source) := by
  simp [SuccessfulExactDecode] at exact
  simp [exact]

theorem admitted_decode_preserves_schema_projection
    (decode : Encoded → Option ArtifactTree)
    (wire : Encoded)
    (source : ArtifactTree)
    (envelope : Envelope)
    (project : ArtifactTree → Projection)
    (admitted : Admitted decode wire source envelope) :
    (decode wire).map project = some (project source) := by
  exact successful_exact_decode_preserves_projection
    decode wire source project admitted.2

end H2HDB.Verification.SchemaArtifactDecode
