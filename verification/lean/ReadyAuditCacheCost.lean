import Std

/-!
# Concrete READY-audit LRU transition and cost model

Entries are stored oldest first. A hit removes and appends its entry; a miss
performs one validation, then successful admissible values evict oldest entries
until the charged byte budget has room. Failed validations and oversized values do not
change retained entries. Keys abstract the complete (digest, expected-domain)
pair, and byte lengths abstract already validated immutable payloads. Each
admitted key also pays a fixed 512-byte bookkeeping allowance. The entry count
is derived from that positive charge, not independently limited to 128.
The allowance bounds logical accounting, not interpreter object sizes or RSS.

Capacity theorems below follow from these executable transitions, starting with
the empty list; bounds are not fields assumed by a cache structure. Validation
cost is one symbolic unit per miss. Multiplying it by a SQL cost is justified
only when every miss has that exact SQL cost (the implementation correspondence
tests use successfully validated single-leaf values). General canonical trees,
failed validation, SQL execution plans, wall time, and NAS I/O are not covered
by that fixed-cost assumption. Python refinement is separate finite test
evidence, not a theorem about the Python interpreter.
-/

namespace H2HDB.Verification.ReadyAuditCacheCost

structure Limits where
  entryCharge : Nat
  valueBytes : Nat
  totalBytes : Nat
  deriving Repr

def productionLimits : Limits := ⟨512, 65536, 8388608⟩

structure Entry where
  key : Nat
  bytes : Nat
  deriving Repr, DecidableEq, BEq

abbrev Cache := List Entry

def retainedBytes : Cache → Nat
  | [] => 0
  | entry :: rest => entry.bytes + retainedBytes rest

theorem retainedBytes_append (left right : Cache) :
    retainedBytes (left ++ right) = retainedBytes left + retainedBytes right := by
  induction left with
  | nil => simp [retainedBytes]
  | cons entry rest ih => simp [retainedBytes, ih, Nat.add_assoc]

def chargedBytes (limits : Limits) (cache : Cache) : Nat :=
  retainedBytes cache + cache.length * limits.entryCharge

theorem chargedBytes_append (limits : Limits) (left right : Cache) :
    chargedBytes limits (left ++ right) =
      chargedBytes limits left + chargedBytes limits right := by
  simp only [chargedBytes, retainedBytes_append, List.length_append, Nat.add_mul]
  omega

def extract (key : Nat) : Cache → Option Entry × Cache
  | [] => (none, [])
  | entry :: rest =>
      if entry.key = key then (some entry, rest)
      else
        let (found, remaining) := extract key rest
        (found, entry :: remaining)

theorem extract_none (key : Nat) (cache rest : Cache)
    (absent : extract key cache = (none, rest)) : rest = cache := by
  induction cache generalizing rest with
  | nil => simpa [extract] using absent.symm
  | cons entry tail ih =>
      simp only [extract] at absent
      split at absent
      · simp at absent
      · cases found : extract key tail with
        | mk hit remaining =>
            simp only [found] at absent
            cases hit with
            | none =>
                cases absent
                exact congrArg (entry :: ·) (ih remaining found)
            | some value => simp at absent

theorem extract_some_accounting (key : Nat) (cache rest : Cache) (value : Entry)
    (found : extract key cache = (some value, rest)) :
    rest.length + 1 = cache.length ∧
      retainedBytes rest + value.bytes = retainedBytes cache := by
  induction cache generalizing rest with
  | nil => simp [extract] at found
  | cons entry tail ih =>
      simp only [extract] at found
      split at found
      · cases found
        simp [retainedBytes, Nat.add_comm]
      · cases extracted : extract key tail with
        | mk hit remaining =>
            simp only [extracted] at found
            cases hit with
            | none => simp at found
            | some value' =>
                cases found
                obtain ⟨count, bytes⟩ := ih remaining extracted
                simp only [List.length_cons, retainedBytes]
                constructor <;> omega

def trim (limits : Limits) (incomingBytes : Nat) : Cache → Cache
  | [] => []
  | entry :: rest =>
      if chargedBytes limits (entry :: rest) + incomingBytes + limits.entryCharge ≤
          limits.totalBytes then
        entry :: rest
      else trim limits incomingBytes rest

def Bounded (limits : Limits) (cache : Cache) : Prop :=
  chargedBytes limits cache ≤ limits.totalBytes

theorem empty_bounded (limits : Limits) : Bounded limits [] := by
  simp [Bounded, chargedBytes, retainedBytes]

theorem trim_makes_room (limits : Limits) (incomingBytes : Nat) (cache : Cache)
    (admissible : incomingBytes + limits.entryCharge ≤ limits.totalBytes) :
      chargedBytes limits (trim limits incomingBytes cache) +
        incomingBytes + limits.entryCharge ≤
        limits.totalBytes := by
  induction cache with
  | nil => simpa [trim, chargedBytes, retainedBytes] using admissible
  | cons entry rest ih =>
      simp only [trim]
      split
      · assumption
      · exact ih

def remember (limits : Limits) (cache : Cache) (value : Entry) : Cache :=
  if value.bytes > limits.valueBytes ∨
      value.bytes + limits.entryCharge > limits.totalBytes then cache
  else trim limits value.bytes (extract value.key cache).2 ++ [value]

theorem remember_preserves_capacity (limits : Limits) (cache : Cache) (value : Entry)
    (bounded : Bounded limits cache) :
    Bounded limits (remember limits cache value) := by
  unfold remember
  split
  · exact bounded
  · rename_i admissible
    have valueBound : value.bytes + limits.entryCharge ≤ limits.totalBytes := by omega
    have room := trim_makes_room limits value.bytes
      (extract value.key cache).2 valueBound
    simp only [Bounded, chargedBytes, retainedBytes_append, retainedBytes,
      List.length_append, List.length_singleton, Nat.add_mul, Nat.one_mul,
      Nat.add_zero] at *
    omega

structure Access where
  key : Nat
  bytes : Nat
  valid : Bool
  deriving Repr

structure ReadResult where
  cache : Cache
  hit : Bool
  deriving Repr

def read (limits : Limits) (cache : Cache) (access : Access) : ReadResult :=
  match extract access.key cache with
  | (some value, rest) => ⟨rest ++ [value], true⟩
  | (none, _) =>
      ⟨if access.valid then remember limits cache ⟨access.key, access.bytes⟩
        else cache, false⟩

def validationUnits (result : ReadResult) : Nat := if result.hit then 0 else 1

theorem read_preserves_capacity (limits : Limits) (cache : Cache) (access : Access)
    (bounded : Bounded limits cache) :
    Bounded limits (read limits cache access).cache := by
  unfold read
  cases extracted : extract access.key cache with
  | mk hit rest =>
      cases hit with
      | none =>
          simp only
          split
          · exact remember_preserves_capacity limits cache _ bounded
          · exact bounded
      | some value =>
          obtain ⟨count, bytes⟩ := extract_some_accounting access.key cache rest value extracted
          simp only [Bounded, chargedBytes, List.length_append, List.length_singleton,
            retainedBytes_append, retainedBytes, Nat.add_zero]
          rw [count, bytes]
          exact bounded

theorem one_read_validation_units (result : ReadResult) :
    validationUnits result ≤ 1 := by
  unfold validationUnits
  split <;> omega

theorem failed_miss_preserves_cache (limits : Limits) (cache rest : Cache)
    (key bytes : Nat) (absent : extract key cache = (none, rest)) :
    (read limits cache ⟨key, bytes, false⟩).cache = cache := by
  simp [read, absent]

theorem oversized_miss_preserves_cache (limits : Limits) (cache rest : Cache)
    (key bytes : Nat) (absent : extract key cache = (none, rest))
    (oversized : bytes > limits.valueBytes) :
    (read limits cache ⟨key, bytes, true⟩).cache = cache := by
  simp [read, absent, remember, oversized]

theorem unaffordable_miss_preserves_cache (limits : Limits) (cache rest : Cache)
    (key bytes : Nat) (absent : extract key cache = (none, rest))
    (unaffordable : bytes + limits.entryCharge > limits.totalBytes) :
    (read limits cache ⟨key, bytes, true⟩).cache = cache := by
  simp [read, absent, remember, unaffordable]

structure TraceResult where
  cache : Cache
  misses : Nat
  deriving Repr

def run (limits : Limits) (cache : Cache) : List Access → TraceResult
  | [] => ⟨cache, 0⟩
  | access :: rest =>
      let next := read limits cache access
      let tail := run limits next.cache rest
      ⟨tail.cache, validationUnits next + tail.misses⟩

theorem trace_preserves_capacity (limits : Limits) (cache : Cache) (accesses : List Access)
    (bounded : Bounded limits cache) :
    Bounded limits (run limits cache accesses).cache := by
  induction accesses generalizing cache with
  | nil => exact bounded
  | cons access rest ih =>
      exact ih _ (read_preserves_capacity limits cache access bounded)

theorem trace_validation_units_le_reads
    (limits : Limits) (cache : Cache) (accesses : List Access) :
    (run limits cache accesses).misses ≤ accesses.length := by
  induction accesses generalizing cache with
  | nil => simp [run]
  | cons access rest ih =>
      have tail := ih (read limits cache access).cache
      have head := one_read_validation_units (read limits cache access)
      simp only [run, List.length_cons]
      omega

theorem production_trace_capacity (accesses : List Access) :
    (run productionLimits [] accesses).cache.length ≤ 16384 ∧
      chargedBytes productionLimits (run productionLimits [] accesses).cache ≤ 8388608 := by
  have budget := trace_preserves_capacity productionLimits [] accesses
    (empty_bounded productionLimits)
  have charged := budget
  simp only [Bounded, chargedBytes, productionLimits] at charged
  constructor
  · simp only [productionLimits]
    omega
  · exact budget

def sqlUnits (costPerMiss : Nat) (result : TraceResult) : Nat :=
  costPerMiss * result.misses

def runFixedSqlCost (limits : Limits) (costPerMiss : Nat) (cache : Cache) :
    List Access → Nat
  | [] => 0
  | access :: rest =>
      let next := read limits cache access
      (if next.hit then 0 else costPerMiss) +
        runFixedSqlCost limits costPerMiss next.cache rest

theorem fixed_cost_sql_is_cost_times_misses
    (limits : Limits) (cache : Cache) (accesses : List Access) (costPerMiss : Nat) :
    runFixedSqlCost limits costPerMiss cache accesses =
      sqlUnits costPerMiss (run limits cache accesses) := by
  induction accesses generalizing cache with
  | nil => simp [runFixedSqlCost, sqlUnits, run]
  | cons access rest ih =>
      simp only [runFixedSqlCost, run, sqlUnits, Nat.mul_add]
      rw [ih]
      unfold validationUnits
      split <;> simp [sqlUnits]

theorem fixed_cost_sql_units_le_reads
    (limits : Limits) (cache : Cache) (accesses : List Access) (costPerMiss : Nat) :
    sqlUnits costPerMiss (run limits cache accesses) ≤ costPerMiss * accesses.length := by
  exact Nat.mul_le_mul_left costPerMiss (trace_validation_units_le_reads limits cache accesses)

def RetainsKey (cache : Cache) (key : Nat) : Prop :=
  ∃ value ∈ cache, value.key = key

theorem retained_key_extracts (key : Nat) (cache : Cache)
    (present : RetainsKey cache key) :
    ∃ value rest, extract key cache = (some value, rest) := by
  induction cache with
  | nil => simp [RetainsKey] at present
  | cons entry tail ih =>
      by_cases same : entry.key = key
      · exact ⟨entry, tail, by simp [extract, same]⟩
      · obtain ⟨value, member, keyEq⟩ := present
        simp only [List.mem_cons] at member
        rcases member with equal | member
        · subst value
          exact False.elim (same keyEq)
        · obtain ⟨hit, rest, found⟩ := ih ⟨value, member, keyEq⟩
          exact ⟨hit, entry :: rest, by simp [extract, same, found]⟩

theorem hit_reorders_retained_entries (key : Nat) (cache rest : Cache) (value : Entry)
    (found : extract key cache = (some value, rest)) :
    (rest ++ [value]).Perm cache := by
  induction cache generalizing rest with
  | nil => simp [extract] at found
  | cons entry tail ih =>
      simp only [extract] at found
      split at found
      · cases found
        exact List.perm_append_singleton value tail
      · cases extracted : extract key tail with
        | mk hit remaining =>
            simp only [extracted] at found
            cases hit with
            | none => simp at found
            | some value' =>
                cases found
                exact List.Perm.cons entry (ih remaining extracted)

/-- After a working set is retained, any length or ordering of accesses confined
    to it requires no validation. This premise concerns concrete initial entries,
    not a claimed cost bound; hits preserve those entries by permutation. It does
    not assume or prove that an arbitrary cold working set fits the byte budget. -/
theorem retained_working_set_requires_no_validation
    (limits : Limits) (cache : Cache) (accesses : List Access)
    (retained : ∀ access ∈ accesses, RetainsKey cache access.key) :
    (run limits cache accesses).misses = 0 := by
  induction accesses generalizing cache with
  | nil => simp [run]
  | cons access rest ih =>
      obtain ⟨value, remaining, found⟩ :=
        retained_key_extracts access.key cache (retained access (by simp))
      have perm := hit_reorders_retained_entries access.key cache remaining value found
      have tailRetained : ∀ item ∈ rest, RetainsKey (remaining ++ [value]) item.key := by
        intro item member
        obtain ⟨entry, entryMember, keyEq⟩ := retained item (by simp [member])
        exact ⟨entry, perm.mem_iff.mpr entryMember, keyEq⟩
      simpa [run, read, found, validationUnits] using ih (remaining ++ [value]) tailRetained

def cycle (workingSet laps : Nat) : List Access :=
  (List.replicate laps ((List.range workingSet).map fun key => Access.mk key 1 true)).flatten

/- These are finite regression witnesses, not a universal LRU
   working-set theorem. All accesses succeed, values occupy one byte, and the
   charged ceiling never causes an eviction. The former 128-entry cliff is gone.
   Working sets larger than the actual charged budget can still thrash. -/
set_option maxRecDepth 100000 in
set_option maxHeartbeats 4000000 in
theorem finite_127_three_laps_warm :
    (run productionLimits [] (cycle 127 3)).misses = 127 := by decide

set_option maxRecDepth 100000 in
set_option maxHeartbeats 4000000 in
theorem finite_128_three_laps_warm :
    (run productionLimits [] (cycle 128 3)).misses = 128 := by decide

set_option maxRecDepth 100000 in
set_option maxHeartbeats 4000000 in
theorem finite_129_three_laps_warm :
    (run productionLimits [] (cycle 129 3)).misses = 129 := by decide

-- A small real-budget boundary counterexample keeps the remaining limitation
-- explicit without kernel-evaluating a 16,385-key production trace.
theorem charged_budget_can_still_thrash :
    (run ⟨512, 1, 1026⟩ [] (cycle 3 3)).misses = 9 := by decide

private def parseNat (value : String) : IO Nat :=
  match value.toNat? with
  | some number => pure number
  | none => throw (IO.userError s!"expected natural number: {value}")

private def parseAccess (value : String) : IO Access := do
  match value.splitOn ":" with
  | [key, bytes, valid] =>
      unless valid = "0" || valid = "1" do
        throw (IO.userError "access success must be 0 or 1")
      pure ⟨← parseNat key, ← parseNat bytes, valid == "1"⟩
  | _ => throw (IO.userError "access must be KEY:BYTES:SUCCESS")

/-- Executable finite refinement oracle. One oldest-first state row per read:
    hit,cumulative_misses,symbolic_sql_units,entry_count,byte_count,key:bytes|...
    The runner requires a positive entry charge, as production uses 512 bytes.
    Values whose charge cannot fit the total budget are validated but bypassed.
    Inputs and outputs contain no database authority or payload bytes. -/
def runCli (args : List String) : IO Unit := do
  match args with
  | entryCharge :: valueLimit :: byteLimit :: cost :: accesses =>
      let limits := Limits.mk (← parseNat entryCharge) (← parseNat valueLimit)
        (← parseNat byteLimit)
      unless limits.entryCharge > 0 do
        throw (IO.userError "limits require positive entry charge")
      let costPerMiss ← parseNat cost
      let mut cache : Cache := []
      let mut misses := 0
      for encoded in accesses do
        let next := read limits cache (← parseAccess encoded)
        cache := next.cache
        misses := misses + validationUnits next
        let entries := "|".intercalate (cache.map fun entry => s!"{entry.key}:{entry.bytes}")
        let hit := if next.hit then 1 else 0
        IO.println s!"{hit},{misses},{costPerMiss * misses},{cache.length},{retainedBytes cache},{entries}"
  | _ => throw (IO.userError
      "usage: lean --run ReadyAuditCacheCost.lean ENTRY_CHARGE VALUE_BYTES TOTAL_BYTES COST_PER_MISS KEY:BYTES:SUCCESS ...")

end H2HDB.Verification.ReadyAuditCacheCost

/-- Multiple finite scenarios reuse one model compilation; each starts empty. -/
def main (args : List String) : IO Unit := do
  for scenario in args.splitOn "--next-scenario" do
    H2HDB.Verification.ReadyAuditCacheCost.runCli scenario
