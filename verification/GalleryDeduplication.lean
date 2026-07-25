import Std

/-!
# Stable gallery-content deduplication

This file is a small formal Lean model of the final reconciliation pass
for gallery CBZ ownership.  It deliberately models the *completed snapshot*,
not transient states while chunks are being inserted.

The selection rule is lexicographic:

1. a claimant with the greatest priority wins;
2. among equal-priority claimants, an existing owner that still claims this
   hash wins;
3. otherwise the greatest gallery id is the deterministic tie-breaker.

The implementation below spells this order as three selection stages instead
of defining a custom lexicographic order.  This makes the intended semantics
visible in both the executable definition and the proofs.

Here "stable" is intentionally relative to the supplied `prior` owner map.
Preserving a still-eligible incumbent on an exact priority tie is a stated
semantic rule, so results need not be history-independent across *different*
valid prior maps.  For any fixed prior map, however, the final result is
independent of scan/list order; and feeding that result back as the next prior
map is a fixed point.
-/

namespace GalleryDeduplication

abbrev GalleryId := Nat
abbrev ContentHash := Nat
abbrev Priority := Nat

structure Gallery where
  id : GalleryId
  effectiveHash : Option ContentHash
  priority : Priority
deriving DecidableEq, Repr

/--
A completed scan snapshot.  Encoding the effective hash and priority in one
record, together with `uniqueIds`, states that an id has at most one effective
hash and one priority in a snapshot.
-/
structure Snapshot where
  galleries : List Gallery
  uniqueIds :
    ∀ {a b : Gallery},
      a ∈ galleries → b ∈ galleries → a.id = b.id → a = b

abbrev ExistingOwners := ContentHash → Option GalleryId

def Claims (g : Gallery) (hash : ContentHash) : Prop :=
  g.effectiveHash = some hash

def Eligible (snapshot : Snapshot) (hash : ContentHash) (id : GalleryId) : Prop :=
  ∃ g ∈ snapshot.galleries, g.id = id ∧ Claims g hash

def eligibleGalleries (snapshot : Snapshot) (hash : ContentHash) : List Gallery :=
  snapshot.galleries.filter fun g => g.effectiveHash == some hash

def topPriority (snapshot : Snapshot) (hash : ContentHash) : Option Priority :=
  ((eligibleGalleries snapshot hash).map Gallery.priority).max?

def EligibleAtPriority
    (snapshot : Snapshot)
    (hash : ContentHash)
    (priority : Priority)
    (id : GalleryId) : Prop :=
  ∃ g ∈ snapshot.galleries,
    g.id = id ∧ Claims g hash ∧ g.priority = priority

def topGalleryIds
    (snapshot : Snapshot)
    (hash : ContentHash)
    (priority : Priority) : List GalleryId :=
  ((eligibleGalleries snapshot hash).filter fun g => g.priority == priority).map Gallery.id

def fallbackWinner
    (snapshot : Snapshot)
    (hash : ContentHash)
    (priority : Priority) : Option GalleryId :=
  (topGalleryIds snapshot hash priority).max?

noncomputable def winner
    (snapshot : Snapshot)
    (prior : ExistingOwners)
    (hash : ContentHash) : Option GalleryId := by
  classical
  exact
    match topPriority snapshot hash with
    | none => none
    | some priority =>
        match prior hash with
        | some owner =>
            if EligibleAtPriority snapshot hash priority owner then
              some owner
            else
              fallbackWinner snapshot hash priority
        | none => fallbackWinner snapshot hash priority

/--
Warnings are modeled as a relation so the theorem states exactly which row is
allowed: the loser must claim a hash, the target must be that hash's final
winner, and winners never warn about themselves.
-/
structure Reconciliation where
  owner : ContentHash → Option GalleryId
  warning : GalleryId → GalleryId → Prop

theorem reconciliation_ext
    {first second : Reconciliation}
    (owners : first.owner = second.owner)
    (warnings : first.warning = second.warning) :
    first = second := by
  cases first
  cases second
  simp_all

noncomputable def reconcile
    (snapshot : Snapshot)
    (prior : ExistingOwners) : Reconciliation where
  owner := winner snapshot prior
  warning := fun loser kept =>
    ∃ g ∈ snapshot.galleries,
      g.id = loser ∧
      ∃ hash,
        Claims g hash ∧
        winner snapshot prior hash = some kept ∧
        loser ≠ kept

/-! ## Basic finite-maximum and membership lemmas -/

theorem max?_eq_of_perm {xs ys : List Nat} (perm : xs.Perm ys) :
    xs.max? = ys.max? := by
  cases hxs : xs.max? with
  | none =>
      have hnil : xs = [] := List.max?_eq_none_iff.mp hxs
      subst xs
      have : ys = [] := perm.nil_eq.symm
      simp [this]
  | some maximum =>
      symm
      apply (List.max?_eq_some_iff).mpr
      have hmax := (List.max?_eq_some_iff.mp hxs)
      constructor
      · exact perm.mem_iff.mp hmax.1
      · intro value hvalue
        exact hmax.2 value (perm.mem_iff.mpr hvalue)

@[simp]
theorem mem_eligibleGalleries
    {snapshot : Snapshot} {hash : ContentHash} {g : Gallery} :
    g ∈ eligibleGalleries snapshot hash ↔
      g ∈ snapshot.galleries ∧ Claims g hash := by
  simp [eligibleGalleries, Claims]

@[simp]
theorem mem_topGalleryIds
    {snapshot : Snapshot}
    {hash : ContentHash}
    {priority : Priority}
    {id : GalleryId} :
    id ∈ topGalleryIds snapshot hash priority ↔
      EligibleAtPriority snapshot hash priority id := by
  simp only [topGalleryIds, List.mem_map, List.mem_filter, mem_eligibleGalleries]
  constructor
  · rintro ⟨g, ⟨⟨hg, hclaims⟩, hpriority⟩, rfl⟩
    exact ⟨g, hg, rfl, hclaims, by simpa using hpriority⟩
  · rintro ⟨g, hg, rfl, hclaims, hpriority⟩
    exact ⟨g, ⟨⟨hg, hclaims⟩, by simp [hpriority]⟩, rfl⟩

theorem eligible_of_eligibleAtPriority
    {snapshot : Snapshot}
    {hash : ContentHash}
    {priority : Priority}
    {id : GalleryId}
    (eligible : EligibleAtPriority snapshot hash priority id) :
    Eligible snapshot hash id := by
  rcases eligible with ⟨g, hg, hid, hclaims, _⟩
  exact ⟨g, hg, hid, hclaims⟩

theorem topPriority_mem
    {snapshot : Snapshot}
    {hash : ContentHash}
    {priority : Priority}
    (isTop : topPriority snapshot hash = some priority) :
    ∃ g ∈ snapshot.galleries,
      Claims g hash ∧ g.priority = priority := by
  have hmem :
      priority ∈ (eligibleGalleries snapshot hash).map Gallery.priority :=
    List.max?_mem isTop
  rcases List.mem_map.mp hmem with ⟨g, hg, rfl⟩
  exact ⟨g, (mem_eligibleGalleries.mp hg).1,
    (mem_eligibleGalleries.mp hg).2, rfl⟩

theorem topPriority_exists_of_claimant
    {snapshot : Snapshot}
    {hash : ContentHash}
    {g : Gallery}
    (inSnapshot : g ∈ snapshot.galleries)
    (claims : Claims g hash) :
    ∃ priority, topPriority snapshot hash = some priority := by
  have hmem :
      g.priority ∈ (eligibleGalleries snapshot hash).map Gallery.priority := by
    exact List.mem_map.mpr
      ⟨g, mem_eligibleGalleries.mpr ⟨inSnapshot, claims⟩, rfl⟩
  exact Option.isSome_iff_exists.mp (List.isSome_max?_of_mem hmem)

theorem topPriority_maximal
    {snapshot : Snapshot}
    {hash : ContentHash}
    {priority : Priority}
    (isTop : topPriority snapshot hash = some priority)
    {g : Gallery}
    (inSnapshot : g ∈ snapshot.galleries)
    (claims : Claims g hash) :
    g.priority ≤ priority := by
  have hmem :
      g.priority ∈ (eligibleGalleries snapshot hash).map Gallery.priority := by
    apply List.mem_map.mpr
    exact ⟨g, mem_eligibleGalleries.mpr ⟨inSnapshot, claims⟩, rfl⟩
  exact (List.max?_eq_some_iff.mp isTop).2 g.priority hmem

theorem fallbackWinner_eligibleAtPriority
    {snapshot : Snapshot}
    {hash : ContentHash}
    {priority : Priority}
    {id : GalleryId}
    (wins : fallbackWinner snapshot hash priority = some id) :
    EligibleAtPriority snapshot hash priority id := by
  exact mem_topGalleryIds.mp (List.max?_mem wins)

theorem fallbackWinner_maximalId
    {snapshot : Snapshot}
    {hash : ContentHash}
    {priority : Priority}
    {winnerId contenderId : GalleryId}
    (wins : fallbackWinner snapshot hash priority = some winnerId)
    (eligible : EligibleAtPriority snapshot hash priority contenderId) :
    contenderId ≤ winnerId := by
  exact
    (List.max?_eq_some_iff.mp wins).2 contenderId
      (mem_topGalleryIds.mpr eligible)

theorem fallbackWinner_exists_of_topPriority
    {snapshot : Snapshot}
    {hash : ContentHash}
    {priority : Priority}
    (isTop : topPriority snapshot hash = some priority) :
    ∃ id, fallbackWinner snapshot hash priority = some id := by
  rcases topPriority_mem isTop with ⟨g, hg, hclaims, hpriority⟩
  have hmem : g.id ∈ topGalleryIds snapshot hash priority :=
    mem_topGalleryIds.mpr ⟨g, hg, rfl, hclaims, hpriority⟩
  have hisSome :
      (fallbackWinner snapshot hash priority).isSome := by
    exact List.isSome_max?_of_mem hmem
  exact Option.isSome_iff_exists.mp hisSome

/-! ## Winner semantics -/

theorem existing_top_owner_wins
    {snapshot : Snapshot}
    {prior : ExistingOwners}
    {hash : ContentHash}
    {priority : Priority}
    {owner : GalleryId}
    (isTop : topPriority snapshot hash = some priority)
    (wasOwner : prior hash = some owner)
    (stillEligible : EligibleAtPriority snapshot hash priority owner) :
    winner snapshot prior hash = some owner := by
  simp [winner, isTop, wasOwner, stillEligible]

theorem winner_is_top
    {snapshot : Snapshot}
    {prior : ExistingOwners}
    {hash : ContentHash}
    {winnerId : GalleryId}
    (wins : winner snapshot prior hash = some winnerId) :
    ∃ priority,
      topPriority snapshot hash = some priority ∧
      EligibleAtPriority snapshot hash priority winnerId := by
  unfold winner at wins
  generalize htop : topPriority snapshot hash = top at wins
  cases top with
  | none => simp at wins
  | some priority =>
      refine ⟨priority, rfl, ?_⟩
      generalize howner : prior hash = oldOwner at wins
      cases oldOwner with
      | none =>
          exact fallbackWinner_eligibleAtPriority wins
      | some owner =>
          by_cases stillEligible :
              EligibleAtPriority snapshot hash priority owner
          · simp [stillEligible] at wins
            subst winnerId
            exact stillEligible
          · simp [stillEligible] at wins
            exact fallbackWinner_eligibleAtPriority wins

theorem winner_eligible
    {snapshot : Snapshot}
    {prior : ExistingOwners}
    {hash : ContentHash}
    {winnerId : GalleryId}
    (wins : winner snapshot prior hash = some winnerId) :
    Eligible snapshot hash winnerId := by
  rcases winner_is_top wins with ⟨_, _, eligible⟩
  exact eligible_of_eligibleAtPriority eligible

/--
The winning gallery has maximal priority among every gallery claiming the
hash.  The witness also records that the returned id names an actual claimant.
-/
theorem winner_maximal
    {snapshot : Snapshot}
    {prior : ExistingOwners}
    {hash : ContentHash}
    {winnerId : GalleryId}
    (wins : winner snapshot prior hash = some winnerId) :
    ∃ winningGallery ∈ snapshot.galleries,
      winningGallery.id = winnerId ∧
      Claims winningGallery hash ∧
      ∀ contender ∈ snapshot.galleries,
        Claims contender hash →
        contender.priority ≤ winningGallery.priority := by
  rcases winner_is_top wins with ⟨priority, isTop, eligible⟩
  rcases eligible with ⟨winningGallery, hg, hid, hclaims, hpriority⟩
  refine ⟨winningGallery, hg, hid, hclaims, ?_⟩
  intro contender hcontender hclaimsContender
  rw [hpriority]
  exact topPriority_maximal isTop hcontender hclaimsContender

/-- A hash has at most one owner in the reconciled state. -/
theorem owner_unique
    {snapshot : Snapshot}
    {prior : ExistingOwners}
    {hash : ContentHash}
    {first second : GalleryId}
    (firstOwns : (reconcile snapshot prior).owner hash = some first)
    (secondOwns : (reconcile snapshot prior).owner hash = some second) :
    first = second := by
  rw [firstOwns] at secondOwns
  exact Option.some.inj secondOwns

/-- A gallery can own at most one effective content hash. -/
theorem gallery_owns_at_most_one_hash
    {snapshot : Snapshot}
    {prior : ExistingOwners}
    {firstHash secondHash : ContentHash}
    {id : GalleryId}
    (ownsFirst : (reconcile snapshot prior).owner firstHash = some id)
    (ownsSecond : (reconcile snapshot prior).owner secondHash = some id) :
    firstHash = secondHash := by
  have firstEligible :
      Eligible snapshot firstHash id :=
    winner_eligible (by simpa [reconcile] using ownsFirst)
  have secondEligible :
      Eligible snapshot secondHash id :=
    winner_eligible (by simpa [reconcile] using ownsSecond)
  rcases firstEligible with ⟨firstGallery, firstIn, firstId, firstClaims⟩
  rcases secondEligible with ⟨secondGallery, secondIn, secondId, secondClaims⟩
  have sameGallery : firstGallery = secondGallery :=
    snapshot.uniqueIds firstIn secondIn (firstId.trans secondId.symm)
  subst secondGallery
  rw [Claims] at firstClaims secondClaims
  exact Option.some.inj (firstClaims.symm.trans secondClaims)

/--
When there is no eligible existing owner, the deterministic tie-breaker is the
greatest id among the top-priority galleries.
-/
theorem fallback_tie_break_is_maximal_id
    {snapshot : Snapshot}
    {prior : ExistingOwners}
    {hash : ContentHash}
    {priority : Priority}
    {winnerId : GalleryId}
    (isTop : topPriority snapshot hash = some priority)
    (noEligibleOwner :
      ∀ owner, prior hash = some owner →
        ¬EligibleAtPriority snapshot hash priority owner)
    (wins : winner snapshot prior hash = some winnerId) :
    ∀ contenderId,
      EligibleAtPriority snapshot hash priority contenderId →
      contenderId ≤ winnerId := by
  have fallbackWins :
      fallbackWinner snapshot hash priority = some winnerId := by
    cases howner : prior hash with
    | none =>
        simpa [winner, isTop, howner] using wins
    | some owner =>
        have notEligible := noEligibleOwner owner howner
        simpa [winner, isTop, howner, notEligible] using wins
  intro contenderId eligible
  exact fallbackWinner_maximalId fallbackWins eligible

/-! ## Warning semantics -/

theorem warning_correct
    {snapshot : Snapshot}
    {prior : ExistingOwners}
    {loser kept : GalleryId} :
    (reconcile snapshot prior).warning loser kept ↔
      ∃ g ∈ snapshot.galleries,
        g.id = loser ∧
        ∃ hash,
          Claims g hash ∧
          (reconcile snapshot prior).owner hash = some kept ∧
          loser ≠ kept := by
  rfl

theorem warning_target_unique
    {snapshot : Snapshot}
    {prior : ExistingOwners}
    {loser firstTarget secondTarget : GalleryId}
    (firstWarning : (reconcile snapshot prior).warning loser firstTarget)
    (secondWarning : (reconcile snapshot prior).warning loser secondTarget) :
    firstTarget = secondTarget := by
  rcases firstWarning with
    ⟨firstGallery, firstIn, firstId, firstHash,
      firstClaims, firstOwner, _⟩
  rcases secondWarning with
    ⟨secondGallery, secondIn, secondId, secondHash,
      secondClaims, secondOwner, _⟩
  have sameGallery : firstGallery = secondGallery :=
    snapshot.uniqueIds firstIn secondIn (firstId.trans secondId.symm)
  subst secondGallery
  have sameHash : firstHash = secondHash := by
    rw [Claims] at firstClaims secondClaims
    exact Option.some.inj (firstClaims.symm.trans secondClaims)
  subst secondHash
  rw [firstOwner] at secondOwner
  exact Option.some.inj secondOwner

/-- A final owner cannot simultaneously be a duplicate-warning loser. -/
theorem winner_has_no_warning
    {snapshot : Snapshot}
    {prior : ExistingOwners}
    {hash : ContentHash}
    {winnerId target : GalleryId}
    (owns : (reconcile snapshot prior).owner hash = some winnerId) :
    ¬(reconcile snapshot prior).warning winnerId target := by
  rintro ⟨loserGallery, loserIn, loserId, loserHash,
    loserClaims, loserOwner, differs⟩
  have winnerEligible :
      Eligible snapshot hash winnerId :=
    winner_eligible (by simpa [reconcile] using owns)
  rcases winnerEligible with
    ⟨winnerGallery, winnerIn, winnerIdEq, winnerClaims⟩
  have sameGallery : winnerGallery = loserGallery :=
    snapshot.uniqueIds winnerIn loserIn (winnerIdEq.trans loserId.symm)
  subst loserGallery
  have sameHash : hash = loserHash := by
    rw [Claims] at winnerClaims loserClaims
    exact Option.some.inj (winnerClaims.symm.trans loserClaims)
  subst loserHash
  have winnerOwns :
      winner snapshot prior hash = some winnerId := by
    simpa [reconcile] using owns
  rw [winnerOwns] at loserOwner
  exact differs (Option.some.inj loserOwner)

/--
Every hash-bearing non-winner has exactly one warning, and its target is the
final owner rather than an intermediate owner.
-/
theorem nonwinner_warns_exactly_final_winner
    {snapshot : Snapshot}
    {prior : ExistingOwners}
    {g : Gallery}
    {hash : ContentHash}
    {kept : GalleryId}
    (inSnapshot : g ∈ snapshot.galleries)
    (claims : Claims g hash)
    (finalOwner : (reconcile snapshot prior).owner hash = some kept)
    (loses : g.id ≠ kept) :
    ∀ target,
      (reconcile snapshot prior).warning g.id target ↔ target = kept := by
  have directWarning :
      (reconcile snapshot prior).warning g.id kept :=
    ⟨g, inSnapshot, rfl, hash, claims, finalOwner, loses⟩
  intro target
  constructor
  · intro warning
    exact warning_target_unique warning directWarning
  · intro targetEq
    simpa [targetEq] using directWarning

theorem hashless_not_eligible
    {snapshot : Snapshot}
    {g : Gallery}
    (inSnapshot : g ∈ snapshot.galleries)
    (hashless : g.effectiveHash = none)
    (hash : ContentHash) :
    ¬Eligible snapshot hash g.id := by
  rintro ⟨other, otherIn, sameId, otherClaims⟩
  have sameGallery : other = g :=
    snapshot.uniqueIds otherIn inSnapshot sameId
  subst other
  rw [Claims, hashless] at otherClaims
  contradiction

theorem hashless_has_no_warning
    {snapshot : Snapshot}
    {prior : ExistingOwners}
    {g : Gallery}
    (inSnapshot : g ∈ snapshot.galleries)
    (hashless : g.effectiveHash = none)
    (kept : GalleryId) :
    ¬(reconcile snapshot prior).warning g.id kept := by
  rintro ⟨other, otherIn, sameId, hash, otherClaims, _, _⟩
  have sameGallery : other = g :=
    snapshot.uniqueIds otherIn inSnapshot sameId
  subst other
  rw [Claims, hashless] at otherClaims
  contradiction

/--
The filesystem rule induced by final reconciliation: a hashless gallery is
eligible for a CBZ, while a hash-bearing gallery is eligible exactly when it is
the final owner of that hash.
-/
def CBZEligible
    (snapshot : Snapshot)
    (prior : ExistingOwners)
    (g : Gallery) : Prop :=
  g ∈ snapshot.galleries ∧
    match g.effectiveHash with
    | none => True
    | some hash => (reconcile snapshot prior).owner hash = some g.id

theorem cbzEligible_iff_hashless_or_final_winner
    {snapshot : Snapshot}
    {prior : ExistingOwners}
    {g : Gallery} :
    CBZEligible snapshot prior g ↔
      g ∈ snapshot.galleries ∧
      (g.effectiveHash = none ∨
        ∃ hash,
          Claims g hash ∧
          (reconcile snapshot prior).owner hash = some g.id) := by
  cases hhash : g.effectiveHash with
  | none =>
      simp [CBZEligible, Claims, hhash]
  | some hash =>
      simp [CBZEligible, Claims, hhash]

/-! ## Idempotence -/

theorem winner_exists_of_topPriority
    {snapshot : Snapshot}
    {prior : ExistingOwners}
    {hash : ContentHash}
    {priority : Priority}
    (isTop : topPriority snapshot hash = some priority) :
    ∃ id, winner snapshot prior hash = some id := by
  unfold winner
  rw [isTop]
  cases howner : prior hash with
  | none => exact fallbackWinner_exists_of_topPriority isTop
  | some owner =>
      by_cases eligible : EligibleAtPriority snapshot hash priority owner
      · exact ⟨owner, by simp [eligible]⟩
      · simpa [eligible] using fallbackWinner_exists_of_topPriority isTop

/-- Every claimed hash has exactly one final owner. -/
theorem claimed_hash_has_unique_winner
    {snapshot : Snapshot}
    {prior : ExistingOwners}
    {hash : ContentHash}
    {claimant : Gallery}
    (inSnapshot : claimant ∈ snapshot.galleries)
    (claims : Claims claimant hash) :
    ∃ id,
      (reconcile snapshot prior).owner hash = some id ∧
      ∀ other,
        (reconcile snapshot prior).owner hash = some other →
        other = id := by
  rcases topPriority_exists_of_claimant inSnapshot claims with
    ⟨priority, isTop⟩
  rcases winner_exists_of_topPriority (prior := prior) isTop with
    ⟨id, wins⟩
  refine ⟨id, by simpa [reconcile] using wins, ?_⟩
  intro other otherOwns
  exact owner_unique otherOwns (by simpa [reconcile] using wins)

theorem winner_idempotent
    (snapshot : Snapshot)
    (prior : ExistingOwners)
    (hash : ContentHash) :
    winner snapshot (winner snapshot prior) hash =
      winner snapshot prior hash := by
  cases htop : topPriority snapshot hash with
  | none =>
      simp [winner, htop]
  | some priority =>
      rcases winner_exists_of_topPriority (prior := prior) htop with
        ⟨winnerId, wins⟩
      have winnerTop :
          EligibleAtPriority snapshot hash priority winnerId := by
        rcases winner_is_top wins with ⟨actualPriority, actualTop, eligible⟩
        rw [htop] at actualTop
        have : actualPriority = priority := (Option.some.inj actualTop).symm
        simpa [this] using eligible
      rw [wins]
      exact existing_top_owner_wins htop wins winnerTop

theorem stable_reconciliation_idempotent
    (snapshot : Snapshot)
    (prior : ExistingOwners) :
    reconcile snapshot (reconcile snapshot prior).owner =
      reconcile snapshot prior := by
  apply reconciliation_ext
  · funext hash
    exact winner_idempotent snapshot prior hash
  · funext loser kept
    apply propext
    constructor
    · rintro ⟨g, hg, hid, hash, hclaims, howner, hne⟩
      exact ⟨g, hg, hid, hash, hclaims,
        (winner_idempotent snapshot prior hash).symm.trans howner, hne⟩
    · rintro ⟨g, hg, hid, hash, hclaims, howner, hne⟩
      exact ⟨g, hg, hid, hash, hclaims,
        (winner_idempotent snapshot prior hash).trans howner, hne⟩

/-! ## Input-order independence -/

theorem eligibleGalleries_perm
    {first second : Snapshot}
    (perm : first.galleries.Perm second.galleries)
    (hash : ContentHash) :
    (eligibleGalleries first hash).Perm
      (eligibleGalleries second hash) := by
  exact perm.filter fun g => g.effectiveHash == some hash

theorem topPriority_eq_of_perm
    {first second : Snapshot}
    (perm : first.galleries.Perm second.galleries)
    (hash : ContentHash) :
    topPriority first hash = topPriority second hash := by
  apply max?_eq_of_perm
  exact (eligibleGalleries_perm perm hash).map Gallery.priority

theorem eligibleAtPriority_iff_of_perm
    {first second : Snapshot}
    (perm : first.galleries.Perm second.galleries)
    (hash : ContentHash)
    (priority : Priority)
    (id : GalleryId) :
    EligibleAtPriority first hash priority id ↔
      EligibleAtPriority second hash priority id := by
  constructor
  · rintro ⟨g, hg, rest⟩
    exact ⟨g, perm.mem_iff.mp hg, rest⟩
  · rintro ⟨g, hg, rest⟩
    exact ⟨g, perm.mem_iff.mpr hg, rest⟩

theorem topGalleryIds_perm
    {first second : Snapshot}
    (perm : first.galleries.Perm second.galleries)
    (hash : ContentHash)
    (priority : Priority) :
    (topGalleryIds first hash priority).Perm
      (topGalleryIds second hash priority) := by
  apply List.Perm.map
  apply List.Perm.filter
  exact eligibleGalleries_perm perm hash

theorem fallbackWinner_eq_of_perm
    {first second : Snapshot}
    (perm : first.galleries.Perm second.galleries)
    (hash : ContentHash)
    (priority : Priority) :
    fallbackWinner first hash priority =
      fallbackWinner second hash priority := by
  exact max?_eq_of_perm (topGalleryIds_perm perm hash priority)

theorem winner_eq_of_perm
    {first second : Snapshot}
    (perm : first.galleries.Perm second.galleries)
    (prior : ExistingOwners)
    (hash : ContentHash) :
    winner first prior hash = winner second prior hash := by
  have topEq := topPriority_eq_of_perm perm hash
  cases htop : topPriority first hash with
  | none =>
      have htopSecond : topPriority second hash = none := topEq ▸ htop
      simp [winner, htop, htopSecond]
  | some priority =>
      have htopSecond :
          topPriority second hash = some priority := topEq ▸ htop
      cases howner : prior hash with
      | none =>
          simp [winner, htop, htopSecond, howner,
            fallbackWinner_eq_of_perm perm hash priority]
      | some owner =>
          have eligibleIff :=
            eligibleAtPriority_iff_of_perm perm hash priority owner
          by_cases eligibleFirst :
              EligibleAtPriority first hash priority owner
          · have eligibleSecond := eligibleIff.mp eligibleFirst
            simp [winner, htop, htopSecond, howner,
              eligibleFirst, eligibleSecond]
          · have eligibleSecond : ¬EligibleAtPriority second hash priority owner :=
              fun h => eligibleFirst (eligibleIff.mpr h)
            simp [winner, htop, htopSecond, howner,
              eligibleFirst, eligibleSecond,
              fallbackWinner_eq_of_perm perm hash priority]

theorem stable_reconciliation_permutation_invariant
    {first second : Snapshot}
    (perm : first.galleries.Perm second.galleries)
    (prior : ExistingOwners) :
    reconcile first prior = reconcile second prior := by
  apply reconciliation_ext
  · funext hash
    exact winner_eq_of_perm perm prior hash
  · funext loser kept
    apply propext
    constructor
    · rintro ⟨g, hg, hid, hash, hclaims, howner, hne⟩
      exact ⟨g, perm.mem_iff.mp hg, hid, hash, hclaims,
        (winner_eq_of_perm perm prior hash).symm.trans howner, hne⟩
    · rintro ⟨g, hg, hid, hash, hclaims, howner, hne⟩
      exact ⟨g, perm.mem_iff.mpr hg, hid, hash, hclaims,
        (winner_eq_of_perm perm prior hash).trans howner, hne⟩

end GalleryDeduplication
