# Formal verification

This directory specifies the only production schema shipped by this package:
the greenfield epoch-3 database. It does not describe, adopt, or silently
replace an earlier database. The numbered migration runner and hand-written
legacy schema have been removed; the generated provider resolves every
recurring semantic validator and production writer binding.

## Current contract

The generated contract currently contains:

- 152 data-plane base relations checked as BCNF, plus 46 executable logical
  projections (33 SQL views and 13 inline projections) and 8 reusable sealed
  vertical families;
- 29 explicitly checked lossless and dependency-preserving decompositions;
- an exact 128-relation catalog physical-domain closure, split into 106
  mutation relations and 22 read-only relations;
- 66 operational BCNF base relations, including epoch control, plus one inline
  activation projection and no operational SQL view for fencing,
  downloader-to-ingest handoff, staging, allocation, receipts, maintenance,
  queues, caches, and bounded cleanup;
- 218 tables and 33 SQL views, for exactly 251 SQL objects across the complete
  epoch;
- 30 versioned semantic obligations: 13 data-plane and 17 operational; and
- every generated typed bootstrap row per backend, including the real
  deletion-request generation-zero history/head and all cleanup target kinds
  expanded into 256 fixed shards each.

There are no declared BCNF exceptions among base tables. BCNF does not impose
the narrower product layout: a separate closed-world gate requires every
ordinary physical `catalog_*` base table to be its semantic primary key plus at
most one atomic non-key column. It reports 113 narrow bases and 39 exact
reviewed-wide BCNF relations. Thirty selected families replace 190 former
physical relations with 36 bases under the explicit capacity contract. Three
authorities (`file_name_identity`, `tag_term`, and `catalog_contributor`) retain
the exact complete shape of a former widest member and are therefore
capacity-neutral while their redundant companion tables disappear. Every
nontrivial determinant is a candidate key. Logical projections are excluded and
may deliberately expose denormalized read shapes. The counts above are checked
from the manifests rather than copied into the runtime provider by hand.

Full `SchemaAdmin.check()` deliberately scans the single publication generation
chain to prove exact adjacency through the maximum commit, successor arithmetic,
and absence of forks, gaps, or nodes beyond the tip. Publication-commit deletion
and generation compaction are separate bounded transactions, so a crash may
leave a conservative contiguous prefix below the oldest commit; that prefix
grants no publication authority and later maintenance removes it. During an
OPEN generation cycle, the audit admits a missing edge or node only when the
sealed contiguous-prefix roots, registered PG phase, and canonical keyset
cursor prove that exact bounded deletion; completion is accepted only after the
remaining suffix is contiguous again. It does not scan content, projection,
artifact, queue, or event rows. The hot
`check_readiness()` probe remains epoch-only and O(1), while fresh publication
and replay validate only the locked chain tip locally.

The generated provider is intentionally fail-closed: it cannot return a
`SchemaEpochDefinition` if a recurring obligation lacks a trusted wheel-owned
validator or exact production writer binding. The wheel now binds all 28
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
  `lean/ArtifactDelta.lean`, `lean/ArtifactReceiptCache.lean`,
  `lean/ArtifactSourceSliceIsolation.lean`,
  `lean/CanonicalBatchHydration.lean`, `lean/CatalogChildHydration.lean`,
  `lean/CanonicalPlanCursor.lean`, `lean/IngestFacadeLifecycle.lean`,
  `lean/CatalogReadBundle.lean`, `lean/ReadyAuditCanonicalCache.lean`,
  `lean/SchemaBootstrapBatch.lean`, `lean/PreparationDrain.lean`, and
  `GalleryDeduplication.lean` prove the
  listed abstract delta, source-revalidated and capacity-bounded disposable
  receipt-cache observational equivalence,
  shared-spool slice versus independent-source byte equivalence,
  scalar-versus-batched canonical hydration equivalence, bounded ordered
  contributor/subject keyset hydration, ingest-facade close/cache ownership,
  canonical selector and catalog connector-layout equivalence, stable-head discovery-bundle
  equivalence, snapshot-scoped READY-audit cache equivalence and hard bounds,
  bounded-bootstrap result/replay equivalence, bounded preparation-drainage
  page arithmetic (hard-bounded pages, a strictly decreasing measure, exactly
  `ceil(n/128)` committed pages, a strictly advancing durable position, and
  idempotent page replay), and deduplication theorems.
- `schema/physical.toml` and `schema/operational_physical.toml` give complete
  SQLite and MariaDB realizations. The refinement code introspects object kind,
  columns, types, nullability, collation, keys, FKs, checks, indexes, and view
  definitions. Deterministic generators reject drift.
- `tla/CatalogCore.tla` finitely explores catalog lifecycle, fencing,
  publication, artifacts, and GC. `tla/GalleryStaging.tla` finitely explores
  gallery staging, exact-request replay, takeover, bounded carry, canonical
  upload handoff, maintenance exclusion, and phased cleanup.
  `tla/CanonicalPlanCursor.tla` finitely explores stage-plan cache reset,
  first-consumer claim handoff, transaction-prefix rollback, response loss,
  stale fencing, same-receipt preimage corruption, and delayed concurrent
  allocate rejection after durable consumption.
  `tla/CatalogReadBundle.tla` finitely explores reused versus separate catalog
  read connectors, one-snapshot discovery bundles, stable independent reads,
  and zero stale success across head advancement.
  `tla/CatalogChildHydration.tla` finitely explores hard-capped ordered child
  pages, exact cursor advancement, and reference-result completion.
  `tla/IngestFacadeLifecycle.tla` finitely explores close/install/take ownership
  interleavings, idempotent release, and cache non-resurrection after close.
  `tla/SchemaBootstrapBatch.tla` finitely explores bounded, statement-aligned
  bootstrap batches through rollback, crash, lost commit responses, replay,
  and the exact durable-fact precondition for `READY`.
  `tla/PreparationDrain.tla` finitely explores the superseded-preparation
  drainage through issue, commit, lost commit responses, stale-position
  retries and crashes: bounded pages, a strictly advancing durable position,
  at most `ceil(rows/limit)` committed pages, and fair drainage.
  `tla/PolicyTakeover.tla` finitely explores a complete ingest-policy change
  at a takeover across every lifecycle point. Each policy atom represents the
  manifest, analysis, artifact, display-title, title-sort and operational
  identities plus artifacts-required. An inherited `DB_COMMITTED`
  publication is recovered before source handoff without consuming the new
  generation's mapping; a successful synchronization return is permitted
  only after the exact requested policy is the current `PUBLISHED` head.
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
unbounded bind. Publication summary and language are canonical references;
artifact storage keys are bounded neutral values derived solely from GID and
are not stored as canonical payloads or concrete filesystem paths. Monolithic
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
there is no independent activation mutation. These events are
publication-owned current/retry control, not an OPDS requirement or a durable
delivery log, and the schema has no event-consumer registry or acknowledgement
relations.

The current publication, a reader-invisible `DB_COMMITTED` exact successor, and
replayable COMPLETE work retain their exact preparation, stream, seal, typed
events, and source-build lineage. The candidate-base row is a one-shot CAS
authority: terminal activation consumes it in the same transaction that moves
the reader head and releases both initial working roots. Generic preparation
cleanup removes only unbound ABANDONED work. Once a finalized non-head
publication is unreachable, its dedicated frozen cleanup releases the safe
build-base pin and candidate binding, removes the COMPLETE preparation control,
atomically removes each exactly matching subtype/base-event pair under a
durable `(receipt_id, preparation_id, sequence_no)` cursor, and atomically
removes the commit, effect seal, and stream before the final checkpoint and
anchor. There is no retained lifetime cross-revision event history.

Cleanup is a fixed 23-by-256 control plane. Each target slot reuses one current
job and latest completion generation; deterministic int63 identities prevent
ABA without unbounded attempt history. Candidate selection is keyset bounded,
child-first phases are closed against the catalog and operational FK graphs,
and every destructive cycle requires the exclusive maintenance gate. Shared
observation pages use allocation associations plus exact incoming-FK blockers;
canonical pages are owner scoped.

Catalog revision descriptors, common commits, generation nodes, and source
lineage are not lifetime audit history. They may remain while a current build or
candidate still pins its predecessor, then fixed-shard cleanup removes the
unreachable prefix after the replacement head is PUBLISHED. Full READY audit is
valid after every durable publication-commit and publication-generation phase,
including PCOM multi-root keyset positions and the crash window before the next
generation cleanup. It accepts an intact conservative prefix between cycles;
within PG_EDGE or PG_ROOT, only exact OPEN frozen-root/cursor authority may
explain a gap. It still rejects forged or missing anchors, out-of-set cursors,
forks, unexplained gaps, and nodes past the tip. A two-revision public-facade regression
then reaches the cleanup fixed point, verifies both one-shot base pins and the
old generation edge/node are gone, replays the compacted current head, and runs
the full READY audit. User-facing catalog payload remains current-only; current
incremental analysis/build ancestry is retained only while the active overlay
still references it.

## Honest production boundary

The strict closed-world coverage command (`scripts/verify-formal.py coverage`)
still exits nonzero. Exactly four evidence layers remain blocked, all describing
one missing production workflow: current-only maintenance does not yet drive an
abandoned candidate's protected artifacts through the external adapter's
terminal tombstone acknowledgement and then into database cleanup. The
repository-level bounded release protocol and its SQLite fault/integration
tests exist, but automatically deleting external bytes is intentionally not
wired without explicit product authorization.

The four layers are fault and integration evidence for
`h2hdb.operational.maintenance-gate.v1` and
`h2hdb.operational.cleanup-reachability.v1`. `scripts/check-full.sh` therefore
runs the `--validate-only` contract gate, which proves that the evidence index
is well formed and every cited symbol resolves, rather than claiming the strict
production-readiness gate passed. Plain `coverage` remains the strict bar and
fails while those four layers are blocked.

The end-to-end workflow and liveness evidence runs only through the public
facades on a fresh temporary database per case. The fault matrices are
deliberately lower-level: they inject invalid values and corruption at the
rendered SQL boundary and at the writer-binding guards, and they seed some
fixtures with foreign keys disabled, so they are not themselves pure
facade-only runs.

The evidence inventory records what an executable check establishes when it is
run; it is not the per-merge execution manifest. Plain `pytest` selects
`not deep` and does not enable a live service, but has no aggregate wall-clock
deadline. The canonical bounded merge runner, `scripts/run-pytest.py merge`,
gives its SQLite
`not deep and not mariadb` phase and its single-worker live-MariaDB
`mariadb_smoke and mariadb and not deep` phase one shared 300-second hard
deadline, including termination and reaping of the pytest/xdist POSIX process
group; on Windows, interrupted runs use `taskkill /T`. A test that deliberately
creates a detached operating-system session is outside that process-group
guarantee. Testcontainers/Ryuk cleanup inside the Docker daemon may complete
after the runner exits and is not evidence covered by that deadline. The
exact-tree release receipt attests only that bounded profile.
High-cost SQLite and non-smoke live-MariaDB matrices are marked `deep`; they
remain executable through `scripts/check-pytest-deep.sh`, but their existence
does not mean that every merge ran them.

Executable evidence added, and what it establishes:

- A statement-fault and response-loss matrix interrupts every distinct write
  transaction shape of a production turn (fresh, incremental with a pending
  deletion request and a download handoff, and a cleanup-heavy 300-page
  removal) before each mutation and after each commit, proves exact rollback
  against a full snapshot, and proves that a restarted owner converges to the
  byte-identical catalog under the full READY audit.
- A takeover and generation-interference matrix takes the expired maintenance
  gate and ingest generation over before every fenced boundary and races
  every boundary against a concurrent deletion-request writer.
- Per-stage analysis authority (rollback, stored `page_limit` replay,
  corrupted-limit rejection), bootstrap seed omission, a representative-column
  corruption and foreign-row rejection with interrupted-batch resume, a
  SHA-256 collision fixture at the canonical-value, file-name and page-staging
  seams, a physical-domain matrix over every column of the reachable manifest
  relations on both backends, an identity-corruption matrix with the
  production audit and the production turns as oracles, and authority
  exhaustion, expiry-takeover, response-loss and policy-replacement races
  through the facades.
- An ingest-policy crash matrix: for a first and a later revision, a turn
  dies at each of nine boundaries (analysis complete before publication,
  candidate begin durable, artifact input sealed, operational preparation
  open, artifact rendered but not persisted, every external protection
  durable, every input bound before the commit, durable commit before
  activation, activated before finalization) and the restarted process
  requests another analysis policy. Every case checks the head's actual
  policy, build and revision, the
  exact analysis-run set, that no working root or open analysis survives,
  that durable commits equal published revisions, that retrying the
  converged turn creates no build, analysis, candidate or commit and at most
  one generation mapping per turn, terminal release of any orphan protection,
  maintenance reaching `DONE`, the READY audit and equality with a fresh
  ingest under the requested policy. Before a durable commit the crashed build
  is retired (a COMPLETE analysis without a commit stays an immutable terminal
  fact that cleanup reclaims) before any generation mapping is written. After
  a durable commit, the restarted
  synchronization first finalizes the pending immutable receipt without a
  source mapping, then observes the current filesystem and publishes the
  requested-policy successor in that same session before returning. The
  matrix varies the analysis component; a separate parameterized repository
  test varies artifact, display-title, title-sort, operational and
  artifacts-required components and proves that each mismatch prevents head
  reuse. The matrix also exposed and fixed a liveness bug: replaying a
  published self-only depth-zero analysis (a policy change or a depth-16
  compaction) after finalization pruned its working baseline failed as
  corruption while the build's base pin remained.
- Publication activation has a matching finite TLA+ model and concrete SQLite
  rollback evidence. The model keeps DB_COMMITTED reader-invisible, requires
  its exact current predecessor authority, and places finalization marker,
  reader-head CAS, candidate-base consumption, and both initial working-root
  releases in one terminal action. A fault immediately after the production
  base delete proves the concrete SQL transaction restores every authority.
- Generated SQLite DDL now gives every base column a closed storage-class
  domain and gives every MariaDB `UNSIGNED` column the same nonnegative lower
  bound. A complete static contract plus direct DDL negatives cover all 867
  columns, including relations without a production writer. The manual deep
  matrix remains useful for backend normalization observations, not for finding
  unguarded SQLite storage domains.
- A central executable state-machine contract exhausts all declared finite
  lifecycle transitions and terminal timestamp-presence combinations, scans
  the source tree for the exact 24 lifecycle DML sites and 47 public writer
  entrypoints, and is invoked by the production READY audit. The generated DDL
  guards five enum families; publication-finalization checkpoint enum validity
  is deliberately enforced by its repository decoder and READY audit because
  that table has no enum CHECK.
- Both preparation drains (the live build's superseded attempts and a
  retiring build's attempts) page from a durable position: the least
  `(state, preparation_id)` still matching, read by one seek per drain state
  on the manifest index `ix_operational_preparation_drain_seek`
  `(build_id, state, preparation_id)`; a page is one single-state index range
  from that position capped at 128 rows. The position travels between issue
  and commit and is not trusted: the page reloads it and a stale copy fails
  closed with zero writes, and both orchestrators refuse a position that did
  not advance strictly past the page they committed. Public-facade E2E drains
  129 and 257 seeded attempts in exactly `ceil(n/128)` bounded commits on
  SQLite and live MariaDB; the repository tests pin `EXPLAIN QUERY PLAN` and
  MariaDB `EXPLAIN` to one range seek on that index with no table scan.
- The available live MariaDB 10.11.11 deep profile runs the same facade
  workflows, the liveness regressions, the policy crash matrix, the authority
  races, the interrupted seed batch, the two bounded preparation drains
  (facade-level and repository-level), the physical matrix (one corpus) and
  six sampled statement faults. The bounded release profile does not run that
  matrix: it runs only five representative `mariadb_smoke` cases covering the
  generated epoch end to end, one fresh public-facade pipeline plus full READY
  audit, representative cleanup crash states, public-facade orphan release
  through cleanup while preserving current resources, and catalog-reader
  discovery, facets and presentation over real rows.
  Both profiles require `H2HDB_TEST_MARIADB=1`, which their canonical runners
  set; plain `pytest` and the check-fast commit hook leave it unset.

Explicit assumptions and limits (each is also recorded on the evidence):

- SHA-256 collision resistance is assumed only for file content blobs, whose
  identity is digest plus byte count; every other stored identity is accepted
  only after full preimage comparison, and the collision fixture proves those
  writers fail closed. Nothing claims SHA-256 is collision-free.
- SQLite applies type affinity before CHECK evaluation, so values that normalize
  to the declared storage class are accepted as that normalized value. MariaDB
  fixed-width `BINARY(n)` columns may pad a shorter value with zero bytes. Both
  backend-specific normalizations are pinned by the deep matrix and are not
  classified as missing domain guards.
- External storage and the core database do not share one transaction. If a
  process stops after a terminal storage tombstone but before its database
  acknowledgement, current-only maintenance replays the same exact protection
  token. The adapter contract requires that replay to be idempotent; the rare
  recovery cost is limited to the affected bounded release page.
- Three manifest relations have no production writer and rest in no runtime
  corpus; direct generated-DDL tests insert valid rows and reject wrong storage
  classes and negative portable-unsigned values for them.
- Every TLC result is a finite model check of the declared small profile; the
  Lean theorems are unbounded only for the abstract models they state.

## Synchronization policy semantics

The public meaning of a successful synchronization is exact and has no
deferred-policy outcome: when `synchronize_once` returns normally, the current
`PUBLISHED` catalog head was produced under the complete policy requested by
that call.

If a crashed predecessor already left an immutable `DB_COMMITTED` publication,
the new session drives receipt-scoped library activation and finalization
before it constructs or scans the filesystem source. That recovery deliberately
does not bind the new ingest generation to the old build. The same session then
observes the source that exists now and reuses a current build only if all seven
policy components match; otherwise it creates and publishes a successor. One
call can therefore publish the old durable revision and its requested-policy
successor, but it cannot return success between those two outcomes.

This replaces the former hidden one-turn deferral and the related changed-
snapshot failure behavior. A caller no longer has to retry manually, interpret
a new `POLICY_DEFERRED` result, or recreate the old filesystem snapshot merely
to finish durable database work. The old commit remains authoritative and is
completed idempotently from its receipt; response loss may repeat bounded
adapter/database steps without changing the successful-return contract.

FD completeness remains a domain-audit assumption. The checker and Lean can
prove consequences of the declared FD set; they cannot infer an omitted real
FD from a column name, digest rationale, or business policy. A semantic design
change must first declare all resulting FDs, then regenerate and recheck both
logical and physical artifacts.

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

The preserved capacity benchmark JSON binds the benchmark script, deterministic SHA-256
row generators, fixed seed, 5,000-row batch size, insertion order, MariaDB
10.11.11, a 16,384-byte InnoDB page, each measured relation's exact physical
shape hash, and the tables' engine, row format, and collation. Only the pinned
Testcontainers execution path is accepted, so direct-host evidence cannot be
mislabelled with the container image. It deliberately
does not bind whole physical-manifest hashes: that would create a cycle when a
new operational capacity relation is generated. The final receipt binds the
reviewed measurements to both complete generated physical manifests and to the
capacity plan. The manual profile is too expensive for every commit or release
gate; the inexpensive receipt drift check remains in the schema gate.

The staging profile first fills the table to 1.5 million different-domain
random request keys and staging UUIDs in 300 commits, measures that full-cap
state, then child-free
deletes the entire fill in 300 bounded commits. It refills the same untruncated
table with different request and staging-UUID domains to the accepted
1.5-million-row budget,
measures it, then appends to 1.8 million rows for a rejected diagnostic.
It separately measures 300,019 exact-shape `source_scope` rows and 50,000
maximum-width registry rows. Re-measure after any covered relation shape,
benchmark protocol, or storage-setting change, review the JSON, then generate
the receipt:

```bash
uv run --no-sync python verification/schema/measure_capacity_mariadb.py \
  --output /tmp/h2hdb-capacity-measurement.json
uv run --no-sync python \
  verification/schema/generate_capacity_measurement_receipt.py \
  --measurement /tmp/h2hdb-capacity-measurement.json
```

The generator preserves the reviewed raw JSON as
`schema/capacity_measurement.json`, binds its exact hash into the generated
receipt, and makes the inexpensive schema gate reproduce the receipt from that
raw evidence byte-for-byte. The generator rejects a measurement made with
another benchmark script, relation shape, server version, InnoDB page size,
table storage setting, seed, row distribution, insertion order, or row count.
Measured DATA plus index bytes live only in the receipt, so changing a result
cannot silently author schema or runtime limits in `catalog.toml`. This is
manually reviewed empirical evidence, not a cryptographic attestation of who
ran MariaDB; trusted CI signing would be required for an anti-forgery claim.

The measured `artifact_producer_fingerprint` registry is also checked against
the complete six-relation bounded registry set. An executable conservative
clustered-row plus all-secondary-index width score must show that every
unmeasured 50,000-row registry is no wider than the measured relation; widening
any unmeasured registry past it invalidates the receipt gate.

The bounded cleanup protocol has a separate generated-physical width guard.
It charges maximum column encodings (including text charset width and variable
length prefixes), NULL bitmaps, a conservative clustered-record envelope, and
every deduplicated secondary index with its primary-key suffix. The receipt
records the resulting score, per-row account, and headroom for `cleanup_job`,
`cleanup_cycle_root`, and `cleanup_checkpoint`; widening any shape beyond its
account invalidates the receipt. This is soft-cap sizing evidence, not a claim that an InnoDB
tablespace, MVCC history, or filesystem high-water mark has an unbounded hard
maximum.

The accepted staging capacity is the 1,500,000-row executable emergency budget,
with the larger of the full pre-delete fill and post-delete/refill allocations
multiplied by the policy safety ratio 5/4 before comparison with decimal 400
MB. The multiplier is conservative
acceptance policy, not a promise that an `.ibd` high-water mark or MVCC history
can never exceed the live-row estimate. Normal ingest does not accumulate that
many rows: after a seal outcome is acknowledged, the live shared ingest fence
runs bounded child-first `STAGING_RETIRE` transactions before admitting the next
gallery. Exclusive current-only cleanup is a recovery backstop. The synthetic
300,000 IDs times five rows only exercises random key/index distribution at the
hard budget; it does not assert five requests per gallery. Likewise, the stated
average of 50 files per gallery is scenario context and never derives a request
bound.

`source_scope` is a retained planning peak rather than the staging emergency
cap: 300,000 gallery scopes plus the conservative 19-row reachable build
lineage (a depth-16 current chain of 17, one latest ABANDONED build that needs a
newer generation before cleanup, and one working successor). Its exact primary,
natural unique, three referential unique, and root index shape is measured
directly at 300,019 rows and receives the same 5/4
safety ratio. The one-million-gallery value remains unaccepted stress context,
not a staging row formula or accepted sizing bound.

The largest non-measured current-only control family is bounded at 285 rows:
19 simultaneously reachable analysis runs times 15 stages. Component seals are
bounded at 95 rows. A finalized current-head publication candidate can coexist
with one working candidate until the next exclusive fixed-point cleanup, so
candidate checkpoints and receipts are bounded at 2 times 16, or 32 rows. The
receipt conservatively accounts one decimal megabyte per row for this class,
yielding a 285,000,000-byte sizing bound.

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
