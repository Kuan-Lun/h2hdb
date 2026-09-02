import Std

/-!
# Ingest-facade process-local lifecycle

This model isolates the ownership protocol used when an ingest facade closes
while a disposable receipt is being installed or taken.  A resource begins at
the installing caller, may move to the facade cache or one borrower, and is
eventually released exactly once.  Closing rejects future calls, releases a
cache-owned resource, and deliberately leaves a resource already transferred
to a caller-owned prepared step alone.

The theorems are unbounded over the pre-existing release count.  They prove the
pure transition algebra only.  They do not prove Python lock/Event semantics,
destructor scheduling, adapter behavior, or filesystem cleanup; runtime race
tests exercise those implementation boundaries.
-/

namespace H2HDB.Verification.IngestFacadeLifecycle

inductive ResourceOwner where
  | caller
  | cache
  | borrower
  | released
deriving DecidableEq, Repr

structure State where
  isOpen : Bool
  owner : ResourceOwner
  releaseCount : Nat
deriving DecidableEq, Repr

def closeFacade (state : State) : State :=
  match state.owner with
  | .cache =>
      { isOpen := false
        owner := .released
        releaseCount := state.releaseCount + 1 }
  | _ => { state with isOpen := false }

def install (state : State) : State :=
  match state.owner with
  | .caller =>
      if state.isOpen then
        { state with owner := .cache }
      else
        { state with
          owner := .released
          releaseCount := state.releaseCount + 1 }
  | _ => state

def take (state : State) : State :=
  if state.isOpen then
    match state.owner with
    | .cache => { state with owner := .borrower }
    | _ => state
  else
    state

inductive CallResult where
  | accepted
  | rejected
deriving DecidableEq, Repr

def call (state : State) : CallResult :=
  if state.isOpen then .accepted else .rejected

theorem close_is_idempotent (state : State) :
    closeFacade (closeFacade state) = closeFacade state := by
  rcases state with ⟨isOpen, owner, releaseCount⟩
  cases owner <;> rfl

theorem call_after_close_is_rejected (state : State) :
    call (closeFacade state) = .rejected := by
  rcases state with ⟨isOpen, owner, releaseCount⟩
  cases owner <;> rfl

theorem close_wins_install_and_releases_once
    (state : State)
    (closed : state.isOpen = false)
    (owned : state.owner = .caller) :
    (install state).owner = .released ∧
      (install state).releaseCount = state.releaseCount + 1 := by
  simp [install, closed, owned]

theorem install_then_close_releases_once
    (state : State)
    (opened : state.isOpen = true)
    (owned : state.owner = .caller) :
    (closeFacade (install state)).owner = .released ∧
      (closeFacade (install state)).releaseCount = state.releaseCount + 1 := by
  simp [install, closeFacade, opened, owned]

theorem take_then_close_preserves_borrower_ownership
    (state : State)
    (opened : state.isOpen = true)
    (cached : state.owner = .cache) :
    (closeFacade (take state)).owner = .borrower ∧
      (closeFacade (take state)).releaseCount = state.releaseCount := by
  simp [take, closeFacade, opened, cached]

end H2HDB.Verification.IngestFacadeLifecycle
