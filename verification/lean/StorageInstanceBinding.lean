import Std

/-!
# Immutable storage-instance binding

The database begins without a storage-instance binding.  A first bind records
the UUID supplied by the external storage adapter.  Once present, an exact
response-loss replay returns the retained UUID and a different proposal is
rejected without changing it.

This model proves the unbounded pure transition algebra over abstract UUID
values.  The production runtime checks the concrete non-nil 16-byte domain,
locks the exact READY schema-epoch singleton, and commits or rolls back the SQL
insert atomically.  Filesystem-marker durability and ordering remain an ingest
adapter responsibility and are deliberately outside this core model.
-/

namespace H2HDB.Verification.StorageInstanceBinding

abbrev StorageInstanceUuid := Nat

inductive BindResult where
  | bound (uuid : StorageInstanceUuid)
  | mismatch (retained proposed : StorageInstanceUuid)
deriving DecidableEq, Repr

def bind
    (current : Option StorageInstanceUuid)
    (proposed : StorageInstanceUuid) :
    Option StorageInstanceUuid × BindResult :=
  match current with
  | none => (some proposed, .bound proposed)
  | some retained =>
      if retained = proposed then
        (some retained, .bound retained)
      else
        (some retained, .mismatch retained proposed)

theorem first_bind_records_proposal (proposed : StorageInstanceUuid) :
    bind none proposed = (some proposed, .bound proposed) := by
  rfl

theorem exact_replay_preserves_binding (uuid : StorageInstanceUuid) :
    bind (some uuid) uuid = (some uuid, .bound uuid) := by
  simp [bind]

theorem mismatch_preserves_existing
    (retained proposed : StorageInstanceUuid)
    (different : retained ≠ proposed) :
    bind (some retained) proposed =
      (some retained, .mismatch retained proposed) := by
  simp [bind, different]

theorem successful_bind_never_rebinds
    (retained proposed : StorageInstanceUuid) :
    (bind (some retained) proposed).1 = some retained := by
  by_cases same : retained = proposed <;> simp [bind, same]

end H2HDB.Verification.StorageInstanceBinding
