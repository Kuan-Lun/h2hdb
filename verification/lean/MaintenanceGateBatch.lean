import Std

/-!
Bounded maintenance-gate materialization refinement. The input is one exact
locked snapshot of already validated holder/owner rows. SQL LEFT JOIN decoding,
duplicate/orphan rejection, head serialization, MariaDB gap locks and Python
allocation sizes are runtime obligations, not conclusions of this model.
The fixed slot domain is independent of the number of galleries or images.
-/

namespace H2HDB.Verification.MaintenanceGateBatch

abbrev Slot := Fin 64

structure Owner where
  token : Nat
  generation : Nat
  expires : Nat
deriving DecidableEq, Repr

abbrev Row := Slot × Owner

def pointRead : List Row → Slot → Option Owner
  | [], _slot => none
  | (key, owner) :: rows, slot =>
      if slot = key then some owner else pointRead rows slot

def materialize (rows : List Row) : Slot → Option Owner :=
  rows.foldr
    (fun row index slot => if slot = row.1 then some row.2 else index slot)
    (fun _slot => none)

theorem batch_lookup_equals_point_read (rows : List Row) (slot : Slot) :
    materialize rows slot = pointRead rows slot := by
  induction rows with
  | nil => rfl
  | cons row rows induction =>
      cases row
      simp [materialize, pointRead, materialize] at induction ⊢
      split <;> simp_all

def referenceRead (rows : List Row) (slots : List Slot) : List (Option Owner) :=
  slots.map (pointRead rows)

def batchRead (rows : List Row) (slots : List Slot) : List (Option Owner) :=
  slots.map (materialize rows)

theorem all_slots_preserve_exact_authority (rows : List Row) (slots : List Slot) :
    batchRead rows slots = referenceRead rows slots := by
  simp only [batchRead, referenceRead, List.map_inj_left]
  intro slot _member
  exact batch_lookup_equals_point_read rows slot

theorem every_authorization_decision_is_preserved
    (rows : List Row) (slots : List Slot)
    (authorize : List (Option Owner) → Bool) :
    authorize (batchRead rows slots) = authorize (referenceRead rows slots) := by
  rw [all_slots_preserve_exact_authority]

theorem result_size_is_fixed_by_slot_domain
    (rows : List Row) (slots : List Slot) (bounded : slots.length = 64) :
    (batchRead rows slots).length = 64 := by
  simpa [batchRead] using bounded

end H2HDB.Verification.MaintenanceGateBatch
