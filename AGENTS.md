# AGENTS.md

Guidance for coding agents working in the `h2hdb` core repository.

## Scope

This package owns the SQLite/MariaDB connectors, greenfield schema epoch,
normalized catalog and operational relations, bounded transactions, durable
coordination state, and public application facades. The supported schema is
epoch 2/version 1: 125 catalog BCNF relations, 25 lossless and
dependency-preserving decompositions, and 76 operational BCNF relations.

Core must not depend on Pillow, FastAPI, OPDS types, `hbrowser`, filesystem
scanning, gallery parsing, or concrete CBZ/object-storage behavior. Those
belong to sibling repositories or consumer adapters. Consumers use
`VNextDatabaseAdminFacade`, `VNextCatalogFacade`, and
`VNextDownloadQueueFacade` rather than connector, repository, generated-schema,
or table internals.

## Environment and commands

Use the repository's independent uv virtual environment. This repository is
not part of a uv workspace, and `uv.lock` must remain ignored.

```bash
uv pip install -e ".[dev]"
uv run --no-sync black --check src tests scripts
uv run --no-sync ruff check src tests scripts
uv run --no-sync mypy src tests scripts
uv run --no-sync pytest
uv run --no-sync python -m build
```

Use `./scripts/rebuild-env.sh` after toolchain changes. Use
`./scripts/rebuild-multirepo-integration.sh` for the isolated cross-repository
editable-install smoke test.

## Manifest-first schema workflow

The logical authoring surfaces are
`verification/schema/catalog.toml` and
`verification/schema/operational.toml`. The catalog manifest is closed-world
over its declared functional dependencies. An omitted semantic dependency
invalidates the real design claim even if the executable checker passes.

Make schema changes in this order:

1. Add or change every relation, key, functional dependency, decomposition,
   bootstrap fact, semantic obligation, and materialization rationale in the
   logical manifests.
2. Regenerate `physical.toml`, `operational_physical.toml`, and the catalog and
   operational Lean schema files with their repository generators.
3. Regenerate the wheel-resident `_generated_vnext_schema.py` provider artifact.
4. Implement or update exact runtime validators, recurring writer bindings,
   repositories, and fault/integration evidence named by the manifests.
5. Run the schema, Lean, coverage-metadata, runtime, and backend checks before
   treating the new manifest as usable.

Run drift and proof checks with:

```bash
uv run --no-sync python scripts/verify-formal.py coverage --validate-only
uv run --no-sync python scripts/verify-formal.py schema
uv run --no-sync python scripts/verify-formal.py lean
uv run --no-sync python scripts/fetch-formal-tools.py
uv run --no-sync python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar
```

Do not hand-edit generated physical manifests, generated Lean schema files, or
the generated runtime-provider artifact. A relation-count change must be
reflected consistently in manifests, checks, and documentation.

## Formal verification

`verification/invariants.toml` is a closed-world evidence index for every
machine `semantic_obligation` ID declared by the catalog and operational
contracts. New IDs require real FD, Lean, TLA+, runtime-refinement, fault, or
integration evidence as appropriate. The gate rejects missing IDs, stale
evidence symbols, and claims that finite TLC exploration are unbounded proofs.

The required metadata check uses `coverage --validate-only`: it rejects an
invalid evidence contract while reporting production blockers. Plain
`coverage` is the strict production-readiness gate and remains nonzero until
all blockers are discharged. Do not describe successful schema generation,
Lean checks, or coverage metadata validation as strict production coverage.

Use `--deep` only for the larger manual/nightly TLA+ profile. TLC exhausts
reachable states only for the selected finite constants. Lean theorems are
unbounded over their stated mathematical inputs and assumptions, but do not by
themselves establish that Python, SQL, transactions, or filesystem effects
refine the model.

## Architecture rules

- Public consumers use the three `VNext*Facade` classes and immutable values
  exported from `h2hdb`. Protocols live in `ports.py`; neutral data lives in
  `domain.py`.
- Backend-specific behavior stays behind `SQLConnector`. Write common SQL once
  with `%s` placeholders; SQLite translates them to `?`.
- Every cross-table workflow shares one connector and one managed transaction.
  Internal connector/unit-of-work methods may coordinate repositories;
  consumers may not call them.
- SQLite writes use `BEGIN IMMEDIATE`; MariaDB uses row/advisory locks where
  fencing, allocation, or epoch serialization requires them. Read-only mode is
  connector-enforced.
- Never accept caller digests, derived identifiers, counts, cursors, names,
  generations, leases, or tokens as authority. Recompute or load exact durable
  authority and fail closed on mismatch.
- Arbitrary-length data uses bounded canonical pages and streaming validation.
  Batches are hard-capped, keyset-paged, idempotent, and response-loss safe.
- Immutable identity, history, event, receipt, and publication facts are not
  updated in place. Mutable state is isolated in normalized heads, owners,
  leases, checkpoints, and explicit state-machine relations.
- Publication and coordinated completion validate sealed scalar state inside a
  short transaction. They must not scan an unbounded source, projection,
  artifact, queue, or event set.
- Exact attempt/generation/token fencing must prevent a delayed retry from
  completing or mutating replacement work.
- Cleanup is bounded, child-first, and reachability checked. It must retain any
  identity or history still referenced by active work, publication, pending
  effects, or protection claims.
- Catalog pagination across calls pins the `CatalogRevision` returned by the
  first read. Nonblank search remains unavailable until a normalized,
  revision-pinned index is added to the manifest.
- Do not derive `redownload_required` from transient joins until the manifest
  defines durable revision-scoped authority and replay semantics.
- The artifact-preparation repository accepts a typed storage adapter. Concrete
  filesystem/object-storage behavior and the missing complete public ingest
  orchestration belong in the consumer integration.

## Schema epoch rules

Only this repository owns schema. The CLI exposes only `migrate`, `check`, and
`ready` for epoch 2/version 1.

`migrate` admits a truly empty database, writes a checksum-bound `BUILDING`
marker, applies idempotent generated DDL/bootstrap slices, validates the exact
object/seed/obligation manifests, and transitions to `READY`. An interrupted
run may resume only the same manifest-bound `BUILDING` epoch. A `READY` rerun
validates rather than mutates the data-plane schema.

There is no numbered v1-v7 upgrade, schema adoption, compatibility view, or
dual-write path. Do not add one. A previous, foreign, or drifted database must
be rejected; rebuilding starts from a new empty database.

`check` performs the complete `READY` audit in a read transaction. `ready` is
the O(1) read-only epoch/version/manifest probe. Provider blockers must fail
before opening or mutating a database. Consumers never initialize schema.

Test every shared schema, transaction, connector, validator, or repository
change on SQLite. MariaDB cases use testcontainers and are enabled with
`H2HDB_TEST_MARIADB=1`; report exactly when Docker is unavailable.

## Working-tree discipline

Preserve pre-existing uncommitted changes. Do not commit, push, publish, remove
user files, or rewrite history unless the user explicitly requests it. Update
README and agent guidance whenever architecture or schema policy makes them
stale.
