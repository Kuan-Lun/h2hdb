# AGENTS.md

Guidance for coding agents working in the `h2hdb` core repository.

## Scope

This package owns database connectors and transactions, domain models, schema
migrations, the durable download queue, lease/token fencing, the database gate,
and the revision catalog projection. It exports the public protocols
`CatalogReader`, `CatalogPublisher`, `CatalogBuildCoordinator`,
`CatalogBuildAnalyzer`, `CatalogBuildProjectionCoordinator`,
`DownloadCoordinator`, and `DatabaseAdmin`.

Core must not depend on Pillow, FastAPI, OPDS types, `hbrowser`, filesystem
scanning, gallery parsing, or CBZ behavior. Those belong to sibling repos.
Downloader, Komga, ingest, and OPDS must use exported public APIs rather than
connector/repository/table internals.

## Environment and commands

Use the repository's independent uv virtual environment. This repository is not
part of a uv workspace, and `uv.lock` must remain ignored.

```bash
uv pip install -e ".[dev]"
uv run --no-sync black --check src tests scripts
uv run --no-sync ruff check src tests scripts
uv run --no-sync mypy src tests scripts
uv run --no-sync pytest
uv run --no-sync python -m build
```

Use `./scripts/rebuild-env.sh` after toolchain changes. Use
`./scripts/rebuild-multirepo-integration.sh` for the isolated cross-repo
editable-install smoke test.

## Formal verification

The executable vNext specifications live in `verification/`; they do not
describe the currently deployed schema. Run the required checks with the
repository-pinned Lean toolchain and checksum-pinned TLC release:

```bash
uv run --no-sync python scripts/verify-formal.py coverage --validate-only
uv run --no-sync python scripts/verify-formal.py schema
uv run --no-sync python scripts/verify-formal.py lean
uv run --no-sync python scripts/fetch-formal-tools.py
uv run --no-sync python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar
```

`verification/invariants.toml` is a closed-world evidence index for every
machine `semantic_obligation` ID declared by the data and operational
contracts. New IDs must add real FD/Lean/TLA/runtime-refinement/fault/integration
coverage; the gate rejects missing IDs, stale evidence symbols, and any claim
that finite TLC exploration is an unbounded proof.

The required CI check uses `coverage --validate-only`: it still rejects every
invalid contract, ID, evidence symbol, and status while reporting production
blockers. Plain `coverage` is the strict production-readiness gate and remains
nonzero until all reported blockers are discharged. `all` retains that strict
behavior and does not accept `--validate-only`.

Use `--deep` only for the larger manual/nightly TLA+ profile; the default
`Small` profile is the finite required check. TLC success exhausts reachable
states only for the selected constants. Lean theorems are unbounded over their
stated mathematical inputs, but depend on their explicit assumptions and do
not establish that Python, SQL, transactions, or filesystem behavior refines
the model.

The BCNF proof is closed-world over exactly the functional dependencies in
`verification/schema/catalog.toml`. Any design change must first add every
semantic FD to that manifest, regenerate/check the Lean proof, and then update
the SQL design. An omitted semantic FD invalidates the real-world BCNF claim
even when the executable check passes. Keep all intended materializations and
their refresh rationale explicit in the manifest.

## Architecture rules

- `H2HDB` in `service.py` is the concrete public facade. Protocols live in
  `ports.py`; neutral immutable data lives in `domain.py`.
- Backend-specific behavior stays behind `SQLConnector`. Write common SQL once
  with `%s` placeholders; SQLite translates them to `?`.
- Cross-table workflows share one connector and managed transaction. Internal
  connector-accepting methods may coordinate core repositories; consumers may
  not call them.
- SQLite writes use `BEGIN IMMEDIATE`; MariaDB uses row locks where fencing or
  revision allocation requires serialization.
- Read-only mode must be enforced by the connector, not merely by convention.
- Request completion and lease transitions remain token/generation fenced.
- Published catalog rows and `catalog_revision_history` descriptors are
  immutable per revision. A publish requires a live `GalleryIngestTurn`; insert
  the complete new snapshot and history descriptor before advancing the
  `catalog_revision` pointer in the same fenced transaction.
- Durable source builds use an independent UUID plus an exact opaque scope key;
  ingest generation is fencing metadata, never build identity. Source-file
  chunks are keyed by stable file-name digests rather than traversal position.
  Scanner-facing plural header/file/completion calls must keep a bounded
  multi-gallery batch in one transaction; never reintroduce one transaction per
  gallery. Giant-gallery completion uses durable counters, not a file-row scan.
  Persist source locators and filesystem observations needed for restart and
  later validation. Scan-observation digests are not canonical source manifests.
  Seal may validate bounded gallery/batch state, but a final pointer-publish
  transaction must not rescan source files.
- Catalog projection builds reserve revisions through the shared allocator;
  legacy publication must use the same allocator. Candidate revision rows are
  invisible until history insertion and pointer publication. Artifact and
  selection checkpoints are hard-capped, payload-receipted CAS mutations.
  Joint publication validates only O(1) sealed state while swapping source and
  catalog pointers in one transaction. Persist a recovery receipt before
  returning; a new live ingest turn may finalize a matching active published
  receipt without inheriting the old owner token.
- Operational source cutover is prepared by a build-scoped, hard-capped
  keyset state machine after projection completion and before sealing. A
  deletion-generation race must refresh the same invisible preparation even
  when the build is already sealed; final publication validates only scalar
  readiness and activates effects with the pointer transaction. Downloader
  operations use the active source build only when its exact revision has a
  matching operational activation, otherwise they use the legacy authority.
- Removed-GID completion and deletion consumption remain exact-token fenced.
  Runtime redownload timestamps are stable rows outside immutable source
  builds. Legacy snapshot publication must not mutate canonical authority once
  a source build has been activated.
- Prune abandoned and unchanged-reuse projection candidates in bounded,
  child-first transactions. Never cascade-delete a reserved revision, and
  never remove its source history or publication receipt during orphan cleanup.
- A finalized inactive published projection may be compacted child-first:
  remove build-scoped projection rows, receipt, and descriptor while retaining
  immutable historical catalog revision rows. Only then may source cleanup
  begin. Cleanup discovery must exclude active, working, and pending-receipt
  builds. Operational activations/events have no foreign key to prunable build
  rows because unacknowledged events outlive source storage.
- Prune inactive source builds only with bounded child-first batches. Never
  cascade-delete a multi-million-row source build in one transaction.
- Pagination over multiple calls must pin the `CatalogRevision` returned by the
  first read.

## Schema rules

Only this repository owns or migrates schema. Add forward-only numbered
migrations in `migrations.py`; never couple schema version to package version.
Migrations must be idempotent enough to recover safely from interrupted DDL.
Consumers only call `check_compatibility()` and must fail clearly when the
database is outside the supported schema range.

Test every shared schema, transaction, migration, or connector change on
SQLite. MariaDB cases use testcontainers and are enabled with
`H2HDB_TEST_MARIADB=1`; report exactly when Docker is unavailable.

## Working-tree discipline

Preserve pre-existing uncommitted changes and do not commit, push, publish, or
rewrite history unless the user explicitly requests it. Update README and agent
guidance when an architecture change makes them stale.
