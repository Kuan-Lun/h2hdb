import Std

/-!
# Runtime-owned bounded MariaDB leases

The pool reserves capacity before opening a physical session and releases that
reservation only after physical close succeeds. Pending opens, checked-out
leases, idle sessions, closing transports, and quarantined close failures all
count. Waiting callers have a separate hard bound. Return after session reset
moves a checked-out lease to idle without changing the physical occupancy.

The unbounded theorems prove this counter protocol and preservation of an
abstract durable observation for every pool transition. They assume mutually
exclusive admission/return bookkeeping and correct COM_RESET_CONNECTION and
physical-close results. They do not prove Python Condition/thread/process
semantics, driver behavior, SQL atomicity, crash durability, or replay. Fake
race/failure tests and live MariaDB session/response-loss tests provide separate
implementation evidence; pipeline crash/replay tests retain that responsibility.
-/

namespace H2HDB.Verification.MariaDBLeasePool

structure State where
  opening : Nat := 0
  idle : Nat := 0
  leased : Nat := 0
  closing : Nat := 0
  quarantined : Nat := 0
  waiting : Nat := 0
  durable : List Nat := []
deriving DecidableEq, Repr

def occupied (s : State) : Nat :=
  s.opening + s.idle + s.leased + s.closing + s.quarantined

def Safe (capacity waiterLimit : Nat) (s : State) : Prop :=
  occupied s ≤ capacity ∧ s.waiting ≤ waiterLimit

inductive Step (capacity waiterLimit : Nat) : State → State → Prop where
  | reserve (s : State) (room : occupied s < capacity) :
      Step capacity waiterLimit s {s with opening := s.opening + 1}
  | opened (s : State) (present : 0 < s.opening) :
      Step capacity waiterLimit s
        {s with opening := s.opening - 1, leased := s.leased + 1}
  | openFailed (s : State) (present : 0 < s.opening) :
      Step capacity waiterLimit s {s with opening := s.opening - 1}
  | borrow (s : State) (present : 0 < s.idle) :
      Step capacity waiterLimit s
        {s with idle := s.idle - 1, leased := s.leased + 1}
  | resetReturned (s : State) (present : 0 < s.leased) :
      Step capacity waiterLimit s
        {s with leased := s.leased - 1, idle := s.idle + 1}
  | discard (s : State) (present : 0 < s.leased) :
      Step capacity waiterLimit s
        {s with leased := s.leased - 1, closing := s.closing + 1}
  | closeIdle (s : State) (present : 0 < s.idle) :
      Step capacity waiterLimit s
        {s with idle := s.idle - 1, closing := s.closing + 1}
  | closed (s : State) (present : 0 < s.closing) :
      Step capacity waiterLimit s {s with closing := s.closing - 1}
  | closeFailed (s : State) (present : 0 < s.closing) :
      Step capacity waiterLimit s
        {s with closing := s.closing - 1, quarantined := s.quarantined + 1}
  | retryQuarantine (s : State) (present : 0 < s.quarantined) :
      Step capacity waiterLimit s
        {s with quarantined := s.quarantined - 1, closing := s.closing + 1}
  | wait (s : State) (room : s.waiting < waiterLimit) :
      Step capacity waiterLimit s {s with waiting := s.waiting + 1}
  | wake (s : State) (present : 0 < s.waiting) :
      Step capacity waiterLimit s {s with waiting := s.waiting - 1}
  | reject (s : State) : Step capacity waiterLimit s s

theorem every_transition_preserves_bounds
    {capacity waiterLimit : Nat} {before after : State}
    (safe : Safe capacity waiterLimit before)
    (step : Step capacity waiterLimit before after) :
    Safe capacity waiterLimit after := by
  cases step <;> simp_all [Safe, occupied] <;> omega

theorem pool_transitions_do_not_change_durable_observation
    {capacity waiterLimit : Nat} {before after : State}
    (step : Step capacity waiterLimit before after) :
    after.durable = before.durable := by
  cases step <;> rfl

inductive Reachable (capacity waiterLimit : Nat) (initial : State) : State → Prop where
  | initial : Reachable capacity waiterLimit initial initial
  | next {before after : State} :
      Reachable capacity waiterLimit initial before →
      Step capacity waiterLimit before after →
      Reachable capacity waiterLimit initial after

theorem all_reachable_states_are_bounded
    {capacity waiterLimit : Nat} {initial reached : State}
    (safe : Safe capacity waiterLimit initial)
    (reachable : Reachable capacity waiterLimit initial reached) :
    Safe capacity waiterLimit reached := by
  induction reachable with
  | initial => exact safe
  | next _ step ih => exact every_transition_preserves_bounds ih step

theorem all_reachable_states_preserve_durable_observation
    {capacity waiterLimit : Nat} {initial reached : State}
    (reachable : Reachable capacity waiterLimit initial reached) :
    reached.durable = initial.durable := by
  induction reachable with
  | initial => rfl
  | next _ step ih =>
      exact (pool_transitions_do_not_change_durable_observation step).trans ih

end H2HDB.Verification.MariaDBLeasePool
