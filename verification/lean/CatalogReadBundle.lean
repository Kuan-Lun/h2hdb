import Std

/-!
# Catalog read connector reuse and discovery-bundle refinement

The production facade performs one pinned read transaction and one subsequent
fresh head-fence transaction.  Reusing the same physical connector changes
only connection ownership: the two transaction observations remain distinct.
`executeRead` therefore records connector layout but derives its public result
solely from the pinned snapshot, its payload, and the fresh fence observation.

`singleSnapshotBundle` reads a discovery page and the LANGUAGE, SUBJECT, and
CONTRIBUTOR first facet pages from one immutable revision.  The independent
reference reads the same four projections separately.  When every independent
read observes the stable pinned head, both bundles are equal.  Any different
fresh fence head rejects, so no stale payload can be returned successfully.

These theorems are unbounded over revision numbers and catalog payload values.
They assume revision payloads are immutable and both physical layouts supply
the stated fresh transaction observations.  They do not prove connector reset,
Python context-manager behavior, SQL snapshot isolation, query correctness, or
SQLite/MariaDB refinement; production regression and backend integration tests
remain required for those boundaries.
-/

namespace H2HDB.Verification.CatalogReadBundle

inductive ConnectorLayout where
  | reused
  | separate
deriving DecidableEq, Repr

structure RevisionPayload where
  discoveryPage : Nat
  languageFacet : Nat
  subjectFacet : Nat
  contributorFacet : Nat
deriving DecidableEq, Repr

structure DiscoveryBundle where
  revision : Nat
  discoveryPage : Nat
  languageFacet : Nat
  subjectFacet : Nat
  contributorFacet : Nat
deriving DecidableEq, Repr

inductive ReadOutcome where
  | success (bundle : DiscoveryBundle)
  | rejected
deriving DecidableEq, Repr

structure ReadExecution where
  layout : ConnectorLayout
  pinnedHead : Nat
  freshFenceHead : Nat
  payload : DiscoveryBundle
deriving DecidableEq, Repr

/-- Connector identity is not catalog authority; the fresh head observation is. -/
def executeRead (execution : ReadExecution) : ReadOutcome :=
  if execution.freshFenceHead = execution.pinnedHead then
    .success execution.payload
  else
    .rejected

def withLayout
    (layout : ConnectorLayout)
    (pinnedHead freshFenceHead : Nat)
    (payload : DiscoveryBundle) : ReadExecution :=
  ⟨layout, pinnedHead, freshFenceHead, payload⟩

theorem reused_connector_equals_two_connectors_under_same_observations
    (pinnedHead freshFenceHead : Nat)
    (payload : DiscoveryBundle) :
    executeRead (withLayout .reused pinnedHead freshFenceHead payload) =
      executeRead (withLayout .separate pinnedHead freshFenceHead payload) := by
  rfl

/-- One physical connector still supplies two logically fresh observations. -/
theorem sequential_fresh_transactions_preserve_reference_result
    (layoutA layoutB : ConnectorLayout)
    (pinnedHead freshFenceHead : Nat)
    (payload : DiscoveryBundle) :
    executeRead (withLayout layoutA pinnedHead freshFenceHead payload) =
      executeRead (withLayout layoutB pinnedHead freshFenceHead payload) := by
  rfl

def singleSnapshotBundle
    (catalog : Nat → RevisionPayload)
    (pinnedHead : Nat) : DiscoveryBundle :=
  let payload := catalog pinnedHead
  ⟨pinnedHead,
    payload.discoveryPage,
    payload.languageFacet,
    payload.subjectFacet,
    payload.contributorFacet⟩

def fourIndependentReads
    (catalog : Nat → RevisionPayload)
    (pageHead languageHead subjectHead contributorHead : Nat) :
    DiscoveryBundle :=
  ⟨pageHead,
    (catalog pageHead).discoveryPage,
    (catalog languageHead).languageFacet,
    (catalog subjectHead).subjectFacet,
    (catalog contributorHead).contributorFacet⟩

theorem single_snapshot_bundle_equals_four_reads_at_stable_head
    (catalog : Nat → RevisionPayload)
    (pinnedHead pageHead languageHead subjectHead contributorHead : Nat)
    (pageStable : pageHead = pinnedHead)
    (languageStable : languageHead = pinnedHead)
    (subjectStable : subjectHead = pinnedHead)
    (contributorStable : contributorHead = pinnedHead) :
    singleSnapshotBundle catalog pinnedHead =
      fourIndependentReads catalog pageHead languageHead subjectHead
        contributorHead := by
  subst pageHead
  subst languageHead
  subst subjectHead
  subst contributorHead
  rfl

def bundledRead
    (layout : ConnectorLayout)
    (catalog : Nat → RevisionPayload)
    (pinnedHead freshFenceHead : Nat) : ReadOutcome :=
  executeRead
    (withLayout layout pinnedHead freshFenceHead
      (singleSnapshotBundle catalog pinnedHead))

theorem stable_bundle_succeeds_with_exact_reference
    (layout : ConnectorLayout)
    (catalog : Nat → RevisionPayload)
    (pinnedHead : Nat) :
    bundledRead layout catalog pinnedHead pinnedHead =
      .success (fourIndependentReads catalog pinnedHead pinnedHead pinnedHead
        pinnedHead) := by
  simp [bundledRead, executeRead, withLayout, singleSnapshotBundle,
    fourIndependentReads]

theorem advanced_head_has_zero_stale_success
    (layout : ConnectorLayout)
    (catalog : Nat → RevisionPayload)
    (pinnedHead freshFenceHead : Nat)
    (advanced : freshFenceHead ≠ pinnedHead) :
    bundledRead layout catalog pinnedHead freshFenceHead = .rejected := by
  simp [bundledRead, executeRead, withLayout, advanced]

theorem successful_read_implies_exact_fresh_head
    (execution : ReadExecution)
    (bundle : DiscoveryBundle)
    (succeeded : executeRead execution = .success bundle) :
    execution.freshFenceHead = execution.pinnedHead := by
  unfold executeRead at succeeded
  split at succeeded
  · assumption
  · cases succeeded

theorem successful_bundle_cannot_return_stale_payload
    (layout : ConnectorLayout)
    (catalog : Nat → RevisionPayload)
    (pinnedHead freshFenceHead : Nat)
    (bundle : DiscoveryBundle)
    (succeeded : bundledRead layout catalog pinnedHead freshFenceHead =
      .success bundle) :
    freshFenceHead = pinnedHead ∧
      bundle = singleSnapshotBundle catalog pinnedHead := by
  by_cases fence : freshFenceHead = pinnedHead
  · simp [bundledRead, executeRead, withLayout, fence] at succeeded
    exact ⟨fence, succeeded.symm⟩
  · simp [bundledRead, executeRead, withLayout, fence] at succeeded

end H2HDB.Verification.CatalogReadBundle
