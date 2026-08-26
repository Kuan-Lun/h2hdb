# Formal verification

This directory specifies the only production schema shipped by this package:
the greenfield epoch-2 database. It does not describe, adopt, or silently
replace an earlier database. The numbered migration runner and hand-written
legacy schema have been removed; the generated provider resolves every
recurring semantic validator and production writer binding.

## Current contract

The generated contract currently contains:

- 306 data-plane base relations checked as BCNF, plus 73 executable logical
  views and 38 reusable sealed vertical families;
- 29 explicitly checked lossless and dependency-preserving decompositions;
- an exact 272-relation catalog physical-domain closure, split into 218
  mutation relations and 54 read-only views;
- 75 operational BCNF base relations plus one derived activation view for
  fencing, downloader-to-ingest handoff, staging, allocation, receipts,
  maintenance, queues, caches, and bounded cleanup;
- 27 versioned semantic obligations: 12 data-plane and 15 operational; and
- 4,913 typed bootstrap rows per backend, including the real deletion-request
  generation-zero history/head and 18 cleanup target kinds
  expanded into 256 fixed shards each.

There are no declared BCNF exceptions among base tables. BCNF does not impose
the narrower product layout: a separate closed-world gate requires every
ordinary physical `catalog_*` base table to be its semantic primary key plus at
most one atomic non-key column. It reports 294 narrow bases and 12 exact
approved-wide BCNF recompositions: seven gallery-linear current-state rows plus
gallery identity, artifact semantic input, prepared artifact, catalog artifact
occurrence, and artifact blob with its mandatory locator. Every nontrivial
determinant is a candidate key. Views are excluded and may
deliberately expose denormalized read shapes. The counts above are checked from
the manifests rather than copied into the runtime provider by hand.

Full `SchemaAdmin.check()` deliberately scans the single sealed publication
generation chain to prove exact node/edge/commit-set equality, successor
arithmetic, absence of forks, gaps and orphans, and that the common receipt
head is the maximum tip. It does not scan content, projection, artifact, queue,
or event rows. The hot `check_readiness()` probe remains epoch-only and O(1),
while fresh publication and replay validate only the locked chain tip locally.

The generated provider is intentionally fail-closed: it cannot return a
`SchemaEpochDefinition` if a recurring obligation lacks a trusted wheel-owned
validator or exact production writer binding. The wheel now binds all 25
recurring obligations to closed families of real public repository methods.
The two physical-domain bindings additionally install closed domain-guard tuples
and distinguish caller-owned transactions from the schema-epoch runner. The
public `migrate`, `check`, and `ready` commands all enter this schema-epoch
boundary; none executes a numbered migration.

## Verification layers

The layers prove different things and are not interchangeable.

- `schema/catalog.toml` and `schema/operational.toml` declare relations,
  candidate keys, functional dependencies, foreign keys, lifecycle rules,
  codecs, bootstrap state, and machine obligations.
- `schema/check_contract.py` independently enumerates attribute closures and
  candidate keys, checks BCNF, validates declared decompositions, and applies
  closed-world checks to identity, staging, retention, and bootstrap metadata.
- `lean/VNextSchema.lean` and `lean/OperationalSchema.lean` prove BCNF for the
  exact generated FD contracts; the data-plane proof also establishes the
  unbounded minimum-witness, greater-only provenance append, exact replay, and
  child-first cleanup theorems for both impacted-key families.
  `lean/ArtifactDelta.lean` and `GalleryDeduplication.lean` prove the listed
  abstract delta and deduplication theorems.
- `schema/physical.toml` and `schema/operational_physical.toml` give complete
  SQLite and MariaDB realizations. The refinement code introspects object kind,
  columns, types, nullability, collation, keys, FKs, checks, indexes, and view
  definitions. Deterministic generators reject drift.
- `tla/CatalogCore.tla` finitely explores catalog lifecycle, fencing,
  publication, artifacts, and GC. `tla/GalleryStaging.tla` finitely explores
  gallery staging, exact-request replay, takeover, bounded carry, canonical
  upload handoff, maintenance exclusion, and phased cleanup.
- `invariants.toml` indexes evidence for every semantic-obligation ID. Missing
  production refinement, fault, or cross-backend integration evidence is a
  machine-readable blocker, not an implicit success.

Lean theorems are unbounded over their stated mathematical inputs, subject to
their assumptions. TLC exhausts only the finite constants in the selected
configuration. Neither tool proves that Python, SQL, a filesystem, or an
external codec implements the model; executable refinement and fault tests are
separate requirements.

## Identity and bounded payloads

Canonical identity is domain separated and version framed. The database stores
an owner-scoped canonical-value graph:

1. `canonical_value_allocation` fixes the digest domain and exact byte count;
2. bounded immutable pages and parent descriptors carry the payload;
3. the final identity row binds the canonical digest to the unique root; and
4. conflicts require digest recomputation plus exact byte comparison.

Pages are at most 65,536 bytes. Writers use two-pass or externally spooled,
ordered iterators so the declared length and digest are known without one
unbounded bind. Publication summary, language, and artifact locator are
canonical references rather than duplicated unbounded SQL values. Monolithic
encode/decode helpers are reference or convenience oracles only when the
contract supplies a streaming production path.

Gallery observations use a distinct page graph with four exact components:
FILE, TAG, DIRECTORY, and METADATA. Page descriptors omit facts derivable from
the page digest, preventing hidden non-key FDs. FILE and DIRECTORY agreement is
established by a bounded, durable exact lookup protocol; METADATA parsing keeps
bounded resumable state across arbitrary chunk boundaries. Final observation
visibility occurs only after every component root, normalized-row congruence,
metadata parse, and match receipt is complete.

The persisted metadata-parser phase registry is closed and case-sensitive;
runtime decoder aliases are never durable values. DIRECTORY, METADATA, and scan
audit digests use fixed domain-separated frames and fixed FILE/TAG/METADATA/
DIRECTORY ordering. They are diagnostic only and never authorize parser
completion, observation reuse, membership, response-loss replay, or sealing.

Source-build discovery persists the complete expected gallery membership from
one provider-private typed spool through bounded checkpoint/receipt CAS. A
separate bounded assembly evaluator exact-merges that membership with final
gallery links and immutable observation statistics. Only its empty terminal
receipt supplies the O(1) gallery/file/byte counters used to seal the build;
discovery and manifest digests remain audit-only.

SHA-256 is modeled only as a collision-checked stored identity. Exact
preimages deterministically yield digests; the
reverse stored FD exists only through uniqueness, full-preimage comparison,
mismatch rejection, and an immutable completion seal.

## Fencing, replay, and cleanup

Operational mutations join the current generation, owner, unexpired lease, and
per-staging claim. A takeover uses a strictly newer generation. Page and match
requests persist an exact bounded request preimage in chunks; the digest alone
never authorizes response-loss replay. New commits, exact replay, and rejection
are disjoint transitions.

Downloader coordination is independent of the ingest fence. A repository-
issued 16-byte capability lives in normalized download owner/lease rows until
either the live downloader hands it off or ingest takes over an expired lease;
that transaction moves the capability into immutable handoff history and
removes both mutable satellites. One handoff maps one-to-one to one ingest
generation. Linked completion records its durable receipt, completes ingest,
and advances the download completed head in one transaction, while periodic
ingest is admitted only under quiescent download authority and never fabricates
a download link. Retry compares the complete retained handoff, consumption,
and completion tuples; no phase string or caller digest is authority.

Canonical upload claims are keyed by `(generation, value_sha256)`. Both these
claims and `source_build_generation` reference immutable `ingest_generation`
history, not ephemeral owner rows. Their allocation and mutation transactions
still lock and validate the exact current head, matching owner, unexpired
lease, and maintenance authority; the FK is retention, never authorization.
This lets a completed owner and lease be removed while residual cleanup
authority remains.

Sealing an identity retains the claim until the first retention-blocking
external consumer is inserted in the same transaction that releases the claim.
A phase-owned dictionary or type row alone does not qualify. This closes the
seal-to-consumer GC window, including first source-root bootstrap. Completed or
strictly superseded generations have an independent bounded claim sweep.

Operational effects are built in a durable preparation-scoped stream while
invisible. Begin inserts the stream, preparation, and initial checkpoints in
one transaction, so a failed begin cannot leave an undiscoverable stream. Each
bounded transaction writes a contiguous base-event range and
exactly one typed subtype, advances the exact digest chain, and commits its
receipt and checkpoint CAS together. After an empty terminal receipt, an
immutable `(event_count, final_chain_sha256)` seal is written last; zero events
use the registered empty-chain digest. Publication then performs only scalar
checks—COMPLETE preparation, exact seal and policy, and current deletion
generation—before atomically sealing one common publication commit and swapping
its single receipt head. Operational activation is a read-only view derived
from the sealed commit's source revision, preparation, policy, and commit time;
there is no independent activation mutation. Readers and acknowledgement
writers reach events through activation;
ack heads are preparation-scoped and advance by bounded contiguous evidence.

Activated COMPLETE preparation control rows may be compacted while the stream,
seal, events, subtypes, activation, and acknowledgements outlive source
storage. Unactivated COMPLETE work remains a publication/retry root. Only an
ABANDONED preparation with no activation or acknowledgement authority can have
its entire invisible stream removed child-first.

Cleanup is a fixed 18-by-256 shard control plane. Each shard reuses one current
job and latest completion generation; deterministic int63 identities prevent
ABA without unbounded attempt history. Candidate selection is keyset bounded,
child-first phases are closed against the catalog and operational FK graphs,
and every destructive cycle requires the exclusive maintenance gate. Shared
observation pages use allocation associations plus exact incoming-FK blockers;
canonical pages are owner scoped.

Catalog revision descriptors, common commits, and source lineage remain as
O(revision) audit. User-facing catalog payload is current-only: readers fence
and recheck the current finalized head, while fixed-shard cleanup removes fully
finalized prior payload only after the new head is also projection-finalized
and no live candidate/build predecessor pins it.

## Honest production boundary

The closed-world coverage command still exits nonzero, but production evidence
is no longer absent. The index names exact vNext repository entrypoints, direct
SQLite workflows, focused rollback and corruption tests, two live MariaDB
workflows, and MariaDB SQL-shape recorders. Each recorder is explicitly only
supporting evidence; it is never described as live MariaDB execution.

The 25 installed writer bindings close runtime ownership for every declared
repository family. Nine physical-boundary tests verify the exact production
families and reject representative forged values before SQL or event derivation.
They do not erase the remaining exhaustive fault and cross-backend gaps.

The default generated provider now completes initialize, replay, read-only full
check, readiness, and public open on fresh SQLite and live MariaDB, validating
all 4,913 bootstrap rows per backend. This closes the catalog and operational
bootstrap
runtime/integration claims, while their row-by-row corruption and partial-commit
fault matrices remain explicit blockers.

In particular, schema/Lean/TLC success does not by itself authorize `READY`.
The generated provider authorizes it only after its exact wheel-owned validators
and all 25 production method families resolve and pass bounded checks. The
strict evidence gate separately remains nonzero until its reported fault and
integration blockers are discharged.

FD completeness is also a domain-audit assumption. The checker and Lean can
prove consequences of the declared FD set; they cannot infer an omitted real
FD from a column name, digest rationale, or business policy. A semantic design
change must first declare all resulting FDs, then regenerate and recheck both
logical and physical artifacts.

That limitation also applies to schema coverage: the manifest proof alone does
not prove that a Python package contains no second hand-written schema. The
source and wheel schema-surface gate separately rejects production SQL relation
identifiers outside the physical manifests and the epoch-control relation.

## Commands

Run the deterministic schema and proof checks with the repository environment:

```bash
uv run --no-sync python scripts/verify-formal.py schema
uv run --no-sync python scripts/verify-formal.py lean
uv run --no-sync python scripts/verify-formal.py coverage --validate-only
```

`coverage --validate-only` rejects an invalid closed-world contract but exits
zero after reporting valid, explicit production blockers. Plain `coverage` is
the strict production-readiness gate and is expected to fail while those
blockers remain. `all` retains the strict behavior and does not accept
`--validate-only`.

TLC 1.7.4, its JAR checksum, and the fallback container digest are pinned in
`tools.lock.toml`:

```bash
uv run --no-sync python scripts/fetch-formal-tools.py
uv run --no-sync python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar
```

The default command runs every `*Small.cfg` profile and must drain each state
queue to zero. A clean partial run is not evidence. The larger profiles are
manual only:

```bash
uv run --no-sync python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar --deep
```

Report Deep simulation as a smoke test unless its exact invocation performed
exhaustive model checking.
