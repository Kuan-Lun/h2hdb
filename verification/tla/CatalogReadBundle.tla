------------------------- MODULE CatalogReadBundle -------------------------
EXTENDS Naturals, Sequences, TLC

(***************************************************************************
Finite safety model for catalog read connector reuse and the discovery
bundle.  One request reads a page plus the LANGUAGE, SUBJECT, and CONTRIBUTOR
facets from a pinned snapshot, while a reference performs the four reads in
sequence.  Reused-connector and two-connector layouts receive the same pinned
and fresh-fence observations and must therefore return the same result.

AdvanceHead may occur between any two read actions.  The second transaction's
fresh head observation decides success; once it differs from the pinned head,
both layouts reject and staleSuccesses remains zero.  Under a stable head, all
four independent payloads equal the single-snapshot bundle.

TLC exhausts only the configured finite revisions and action phases.  The
model assumes immutable revision payloads and that a second transaction on a
reused connector observes FreshHead.  It does not establish Python connector
lifecycle, context-manager behavior, SQL snapshot isolation, query semantics,
or SQLite/MariaDB refinement.
***************************************************************************)

CONSTANT MaxRevision

ASSUME MaxRevision \in Nat \ {0}

Revisions == 1..MaxRevision
Phases == {
    "IDLE", "PINNED", "BUNDLE_PAGE", "BUNDLE_LANGUAGE",
    "BUNDLE_SUBJECT", "BUNDLE_CONTRIBUTOR", "PAGE", "LANGUAGE",
    "SUBJECT", "CONTRIBUTOR"
}
Outcomes == {"NONE", "SUCCESS", "REJECT"}

PayloadAt(revision) ==
    <<revision * 10 + 1,
      revision * 10 + 2,
      revision * 10 + 3,
      revision * 10 + 4>>

VARIABLES
    head,
    phase,
    pinnedHead,
    freshHead,
    stable,
    bundle,
    independentPage,
    independentLanguage,
    independentSubject,
    independentContributor,
    reusedOutcome,
    separateOutcome,
    staleSuccesses,
    lastEvent

vars == <<
    head, phase, pinnedHead, freshHead, stable, bundle,
    independentPage, independentLanguage, independentSubject,
    independentContributor, reusedOutcome, separateOutcome,
    staleSuccesses, lastEvent
>>

Init ==
    /\ head = 1
    /\ phase = "IDLE"
    /\ pinnedHead = 0
    /\ freshHead = 0
    /\ stable = TRUE
    /\ bundle = <<0, 0, 0, 0>>
    /\ independentPage = 0
    /\ independentLanguage = 0
    /\ independentSubject = 0
    /\ independentContributor = 0
    /\ reusedOutcome = "NONE"
    /\ separateOutcome = "NONE"
    /\ staleSuccesses = 0
    /\ lastEvent = "INIT"

BeginRead ==
    /\ phase = "IDLE"
    /\ phase' = "PINNED"
    /\ pinnedHead' = head
    /\ freshHead' = 0
    /\ stable' = TRUE
    /\ bundle' = <<0, 0, 0, 0>>
    /\ independentPage' = 0
    /\ independentLanguage' = 0
    /\ independentSubject' = 0
    /\ independentContributor' = 0
    /\ reusedOutcome' = "NONE"
    /\ separateOutcome' = "NONE"
    /\ lastEvent' = "BEGIN_FIRST_TRANSACTION"
    /\ UNCHANGED <<head, staleSuccesses>>

ReadBundle ==
    /\ phase = "PINNED"
    /\ phase' = "BUNDLE_PAGE"
    /\ bundle' = [bundle EXCEPT ![1] = PayloadAt(pinnedHead)[1]]
    /\ lastEvent' = "READ_PINNED_BUNDLE_PAGE"
    /\ UNCHANGED <<head, pinnedHead, freshHead, stable, independentPage,
        independentLanguage, independentSubject, independentContributor,
        reusedOutcome, separateOutcome, staleSuccesses>>

ReadBundleLanguage ==
    /\ phase = "BUNDLE_PAGE"
    /\ phase' = "BUNDLE_LANGUAGE"
    /\ bundle' = [bundle EXCEPT ![2] = PayloadAt(pinnedHead)[2]]
    /\ lastEvent' = "READ_PINNED_BUNDLE_LANGUAGE"
    /\ UNCHANGED <<head, pinnedHead, freshHead, stable, independentPage,
        independentLanguage, independentSubject, independentContributor,
        reusedOutcome, separateOutcome, staleSuccesses>>

ReadBundleSubject ==
    /\ phase = "BUNDLE_LANGUAGE"
    /\ phase' = "BUNDLE_SUBJECT"
    /\ bundle' = [bundle EXCEPT ![3] = PayloadAt(pinnedHead)[3]]
    /\ lastEvent' = "READ_PINNED_BUNDLE_SUBJECT"
    /\ UNCHANGED <<head, pinnedHead, freshHead, stable, independentPage,
        independentLanguage, independentSubject, independentContributor,
        reusedOutcome, separateOutcome, staleSuccesses>>

ReadBundleContributor ==
    /\ phase = "BUNDLE_SUBJECT"
    /\ phase' = "BUNDLE_CONTRIBUTOR"
    /\ bundle' = [bundle EXCEPT ![4] = PayloadAt(pinnedHead)[4]]
    /\ lastEvent' = "READ_PINNED_BUNDLE_CONTRIBUTOR"
    /\ UNCHANGED <<head, pinnedHead, freshHead, stable, independentPage,
        independentLanguage, independentSubject, independentContributor,
        reusedOutcome, separateOutcome, staleSuccesses>>

ReadIndependentPage ==
    /\ phase = "BUNDLE_CONTRIBUTOR"
    /\ phase' = "PAGE"
    /\ independentPage' = PayloadAt(head)[1]
    /\ lastEvent' = "READ_INDEPENDENT_PAGE"
    /\ UNCHANGED <<head, pinnedHead, freshHead, stable, bundle,
        independentLanguage, independentSubject, independentContributor,
        reusedOutcome, separateOutcome, staleSuccesses>>

ReadIndependentLanguage ==
    /\ phase = "PAGE"
    /\ phase' = "LANGUAGE"
    /\ independentLanguage' = PayloadAt(head)[2]
    /\ lastEvent' = "READ_INDEPENDENT_LANGUAGE"
    /\ UNCHANGED <<head, pinnedHead, freshHead, stable, bundle,
        independentPage, independentSubject, independentContributor,
        reusedOutcome, separateOutcome, staleSuccesses>>

ReadIndependentSubject ==
    /\ phase = "LANGUAGE"
    /\ phase' = "SUBJECT"
    /\ independentSubject' = PayloadAt(head)[3]
    /\ lastEvent' = "READ_INDEPENDENT_SUBJECT"
    /\ UNCHANGED <<head, pinnedHead, freshHead, stable, bundle,
        independentPage, independentLanguage, independentContributor,
        reusedOutcome, separateOutcome, staleSuccesses>>

ReadIndependentContributor ==
    /\ phase = "SUBJECT"
    /\ phase' = "CONTRIBUTOR"
    /\ independentContributor' = PayloadAt(head)[4]
    /\ lastEvent' = "READ_INDEPENDENT_CONTRIBUTOR"
    /\ UNCHANGED <<head, pinnedHead, freshHead, stable, bundle,
        independentPage, independentLanguage, independentSubject,
        reusedOutcome, separateOutcome, staleSuccesses>>

ReadFreshFence ==
    /\ phase = "CONTRIBUTOR"
    /\ phase' = "IDLE"
    /\ freshHead' = head
    /\ reusedOutcome' = IF head = pinnedHead THEN "SUCCESS" ELSE "REJECT"
    /\ separateOutcome' = IF head = pinnedHead THEN "SUCCESS" ELSE "REJECT"
    /\ staleSuccesses' =
        IF head # pinnedHead /\
           (reusedOutcome' = "SUCCESS" \/ separateOutcome' = "SUCCESS")
        THEN staleSuccesses + 1
        ELSE staleSuccesses
    /\ lastEvent' = "READ_SECOND_FRESH_TRANSACTION"
    /\ UNCHANGED <<head, pinnedHead, stable, bundle, independentPage,
        independentLanguage, independentSubject, independentContributor>>

AdvanceHead ==
    /\ head < MaxRevision
    /\ head' = head + 1
    /\ stable' = IF phase = "IDLE" THEN stable ELSE FALSE
    /\ lastEvent' = "ADVANCE_HEAD"
    /\ UNCHANGED <<phase, pinnedHead, freshHead, bundle, independentPage,
        independentLanguage, independentSubject, independentContributor,
        reusedOutcome, separateOutcome, staleSuccesses>>

Next ==
    \/ BeginRead
    \/ ReadBundle
    \/ ReadBundleLanguage
    \/ ReadBundleSubject
    \/ ReadBundleContributor
    \/ ReadIndependentPage
    \/ ReadIndependentLanguage
    \/ ReadIndependentSubject
    \/ ReadIndependentContributor
    \/ ReadFreshFence
    \/ AdvanceHead

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ head \in Revisions
    /\ phase \in Phases
    /\ pinnedHead \in 0..MaxRevision
    /\ freshHead \in 0..MaxRevision
    /\ stable \in BOOLEAN
    /\ bundle \in Seq(Nat)
    /\ Len(bundle) = 4
    /\ independentPage \in Nat
    /\ independentLanguage \in Nat
    /\ independentSubject \in Nat
    /\ independentContributor \in Nat
    /\ reusedOutcome \in Outcomes
    /\ separateOutcome \in Outcomes
    /\ staleSuccesses \in Nat

ConnectorLayoutsAgree == reusedOutcome = separateOutcome

BundlePageDonePhases == {
    "BUNDLE_PAGE", "BUNDLE_LANGUAGE", "BUNDLE_SUBJECT",
    "BUNDLE_CONTRIBUTOR", "PAGE", "LANGUAGE", "SUBJECT", "CONTRIBUTOR",
    "IDLE"
}

BundleLanguageDonePhases == {
    "BUNDLE_LANGUAGE", "BUNDLE_SUBJECT", "BUNDLE_CONTRIBUTOR", "PAGE",
    "LANGUAGE", "SUBJECT", "CONTRIBUTOR", "IDLE"
}

BundleSubjectDonePhases == {
    "BUNDLE_SUBJECT", "BUNDLE_CONTRIBUTOR", "PAGE", "LANGUAGE", "SUBJECT",
    "CONTRIBUTOR", "IDLE"
}

BundleContributorDonePhases == {
    "BUNDLE_CONTRIBUTOR", "PAGE", "LANGUAGE", "SUBJECT", "CONTRIBUTOR",
    "IDLE"
}

BundleUsesOnePinnedSnapshot ==
    pinnedHead # 0 =>
        /\ phase \in BundlePageDonePhases
            => bundle[1] = PayloadAt(pinnedHead)[1]
        /\ phase \in BundleLanguageDonePhases
            => bundle[2] = PayloadAt(pinnedHead)[2]
        /\ phase \in BundleSubjectDonePhases
            => bundle[3] = PayloadAt(pinnedHead)[3]
        /\ phase \in BundleContributorDonePhases
            => bundle[4] = PayloadAt(pinnedHead)[4]

StableIndependentReadsMatchPinned ==
    stable /\ pinnedHead # 0 =>
        /\ phase \in {"PAGE", "LANGUAGE", "SUBJECT", "CONTRIBUTOR", "IDLE"}
            => independentPage = PayloadAt(pinnedHead)[1]
        /\ phase \in {"LANGUAGE", "SUBJECT", "CONTRIBUTOR", "IDLE"}
            => independentLanguage = PayloadAt(pinnedHead)[2]
        /\ phase \in {"SUBJECT", "CONTRIBUTOR", "IDLE"}
            => independentSubject = PayloadAt(pinnedHead)[3]
        /\ phase \in {"CONTRIBUTOR", "IDLE"}
            => independentContributor = PayloadAt(pinnedHead)[4]

SuccessfulBundleEqualsFourStableReads ==
    reusedOutcome = "SUCCESS" =>
        bundle = <<
            independentPage,
            independentLanguage,
            independentSubject,
            independentContributor
        >>

FreshFenceControlsOutcome ==
    reusedOutcome # "NONE" =>
        ((freshHead = pinnedHead) <=> (reusedOutcome = "SUCCESS"))

HeadAdvancementHasZeroStaleSuccess == staleSuccesses = 0

SuccessfulReadHasExactFreshHead ==
    reusedOutcome = "SUCCESS" => freshHead = pinnedHead

SuccessfulReadHadStableHead == reusedOutcome = "SUCCESS" => stable

=============================================================================
