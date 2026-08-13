# Formal verification

This directory specifies the proposed greenfield vNext database epoch. It does
not describe, migrate, or silently replace the currently deployed schema.
Production keeps using the legacy repositories until the explicit vNext admin
operation and every recurring semantic validator are available.

## Current contract

The generated contract currently contains:

- 115 data-plane BCNF relations, including five executable overlay views;
- 23 explicitly checked lossless and dependency-preserving decompositions;
- 60 operational BCNF relations for fencing, staging, allocation, receipts,
  maintenance, queues, activation, caches, and bounded cleanup;
- 26 versioned semantic obligations: 12 data-plane and 14 operational; and
- 3,944 typed bootstrap rows per backend, including 15 cleanup target kinds
  expanded into 256 fixed shards each.

There are no declared BCNF exceptions. The counts above are checked from the
manifests rather than copied into the runtime provider by hand.

The generated provider is intentionally fail-closed. It can expose and audit
the generated data, but it cannot return a `SchemaEpochDefinition` while a
recurring obligation lacks a trusted wheel-owned validator. The ordinary
repository facade is not routed to vNext. Explicit `epoch-v2-*` admin commands
exist so an eventual cutover cannot be confused with a legacy migration.

## Verification layers

The layers prove different things and are not interchangeable.

- `schema/catalog.toml` and `schema/operational.toml` declare relations,
  candidate keys, functional dependencies, foreign keys, lifecycle rules,
  codecs, bootstrap state, and machine obligations.
- `schema/check_contract.py` independently enumerates attribute closures and
  candidate keys, checks BCNF, validates declared decompositions, and applies
  closed-world checks to identity, staging, retention, and bootstrap metadata.
- `lean/VNextSchema.lean` and `lean/OperationalSchema.lean` prove BCNF for the
  exact generated FD contracts. `lean/ArtifactDelta.lean` and
  `GalleryDeduplication.lean` prove the listed abstract delta and deduplication
  theorems.
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

SHA-256 collision freedom remains an explicit identity-model assumption. Exact
preimage comparison reduces the operational risk but is not a mathematical
proof that collisions cannot occur.

## Fencing, replay, and cleanup

Operational mutations join the current generation, owner, unexpired lease, and
per-staging claim. A takeover uses a strictly newer generation. Page and match
requests persist an exact bounded request preimage in chunks; the digest alone
never authorizes response-loss replay. New commits, exact replay, and rejection
are disjoint transitions.

Canonical upload claims are keyed by `(generation, value_sha256)`. Sealing an
identity retains the claim until the first retention-blocking external consumer
is inserted in the same transaction that releases the claim. A phase-owned
dictionary or type row alone does not qualify. This closes the
seal-to-consumer GC window, including first source-root bootstrap. Completed or
strictly superseded generations have an independent bounded claim sweep.

Cleanup is a fixed 15-by-256 shard control plane. Each shard reuses one current
job and latest completion generation; deterministic int63 identities prevent
ABA without unbounded attempt history. Candidate selection is keyset bounded,
child-first phases are closed against the catalog and operational FK graphs,
and every destructive cycle requires the exclusive maintenance gate. Shared
observation pages use allocation associations plus exact incoming-FK blockers;
canonical pages are owner scoped.

Catalog revision history and source revision descriptors are deliberately
retained when no durable reader-pin protocol exists. The contract does not
claim safe pruning merely because a row appears inactive.

## Honest production boundary

The closed-world coverage command currently exits nonzero while production
vNext repository callsites, crash matrices, and SQLite/MariaDB end-to-end
workflows are absent. That is the required result: generated hook names,
caller-supplied booleans, sample queries, and manifest checks do not discharge a
same-transaction writer obligation.

In particular, schema/Lean/TLC success does not by itself authorize `READY`.
The runtime provider must remain unavailable until every recurring obligation
has a bounded validator and every mutation path invokes its writer hook inside
the committing transaction.

FD completeness is also a domain-audit assumption. The checker and Lean can
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

TLC 1.7.4, its JAR checksum, and the fallback container digest are pinned in
`tools.lock.toml`:

```bash
uv run --no-sync python scripts/fetch-formal-tools.py
uv run --no-sync python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar
```

The default command runs every `*Small.cfg` profile and must drain each state
queue to zero. A clean partial run is not evidence. The larger profiles are
manual/nightly only:

```bash
uv run --no-sync python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar --deep
```

Report Deep simulation as a smoke test unless its exact invocation performed
exhaustive model checking.
