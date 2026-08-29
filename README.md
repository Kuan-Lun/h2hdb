# h2hdb

`h2hdb` is the database and coordination core for the H2HDB multi-repository
system. It owns the SQLite/MariaDB schema, bounded transactional workflows, and
backend-neutral application facades. Komga and OPDS use the catalog facade;
ingest uses the transaction-owning ingest facade and downloader uses the queue
facade.

It deliberately does **not** scan files, parse `galleryinfo.txt`, manipulate
images, choose filesystem paths, serve HTTP, serialize OPDS documents, or
depend on `hbrowser`. Those responsibilities belong to consumer adapters and
sibling packages.

## Greenfield BCNF schema

The current schema is a clean epoch-2 design:

- 160 catalog data-plane base relations checked as BCNF, plus 49 generated
  logical views for read-oriented projections. Eleven remaining sealed vertical
  families are reserved for relations whose partial construction is itself
  observable workflow state.
- 28 declared decompositions, each checked as lossless and
  dependency-preserving.
- 70 operational control-plane base relations checked as BCNF, one of which is
  the separately created epoch-control relation, plus one derived activation
  view. The complete epoch therefore contains 160 + 70 = 230 base tables and
  50 logical views, or 280 schema objects in those two manifests.
- Operational events are current/retry publication control rather than an OPDS
  delivery log. Unreachable finalized non-head snapshots are retired by bounded
  compound cleanup; no event-consumer registry, per-event acknowledgement, or
  cross-revision event history is retained.
- Relative to the former 75-base operational schema, two lease tables are
  folded into their BCNF owner relations, the staging-request owner is replaced
  by one bounded budget authority, four unreachable event-delivery scaffold
  relations are removed, and one frozen cleanup-root relation is added. The net
  operational reduction is five base tables.
- One generated physical schema for SQLite and MariaDB, with backend-specific
  SQL rendered from the same closed-world manifests.
- A separate physical-width gate requires each ordinary `catalog_*` base table
  to have its semantic primary key plus at most one atomic non-key value. It
  reports 122 narrow bases and 38 exact reviewed-wide BCNF relations. Twenty-seven
  selected families replace 178 former physical relations with 32 base relations;
  each new wide table is covered by the capacity contract. Logical views are
  excluded from the width policy.
- The closed catalog physical-domain authority contains exactly 151 relations:
  114 mutation relations and 37 read-only views. The complete publication graph
  is inside that closure, including current-only finalization replay state.
- The generated provider installs exactly 5,838 typed bootstrap rows per
  backend, including all 22 fixed 256-shard cleanup ranges.

The logical sources of truth are
[`verification/schema/catalog.toml`](verification/schema/catalog.toml) and
[`verification/schema/operational.toml`](verification/schema/operational.toml).
They declare functional dependencies, keys, decompositions, bootstrap facts,
and semantic obligations. Deterministic generators derive the physical
manifests, Lean schema proofs, and the wheel-resident runtime provider. Generated
SQL is not a second schema-authoring surface.

This is a greenfield cutover. There is no v1-v7 upgrade or adoption path, no
legacy-epoch compatibility layer, and no dual write. Read-only logical views
inside epoch 2 are deliberate read models. A previous or foreign database must
be replaced with an empty database and rebuilt from source data.

The numbered migration runner, monolithic `H2HDB` facade, and their hand-written
legacy repositories are not shipped. In particular, the old
`catalog_build_discoveries` relation is not part of the package or generated
schema. Production SQL relation names are checked against the two physical
manifests in both source and built-wheel verification, so a second hand-written
`catalog_*` or `operational_*` schema cannot silently bypass the manifest audit.

## Schema epoch

The active identity is `epoch=2`, `schema_version=1`. The singleton
`h2hdb_schema_epoch` row binds the exact generated DDL, bootstrap-seed, and
semantic-obligation manifests into one durable checksum.

Initialization follows a fail-closed state machine:

1. A truly empty database is admitted and recorded as `BUILDING`.
2. Idempotent generated DDL and bootstrap rows are applied in deterministic
   slices.
3. The complete object set, bootstrap facts, and activation obligations are
   validated.
4. The exact manifest is atomically marked `READY`.

If construction is interrupted, rerunning `migrate` may resume only the same
checksum-matching `BUILDING` epoch. A `READY` rerun validates the exact current
epoch. Drift, an unknown control residue, or any other non-empty database is
rejected without adoption or destructive repair.

The command name `migrate` is retained as the administration interface, but it
constructs or resumes this single manifest-bound greenfield epoch; it does not
run numbered historical migrations.

## Core responsibilities

- MariaDB and SQLite connectors, read/write transactions, and connector-enforced
  read-only access.
- Manifest-bound schema construction, full validation, and O(1) readiness
  checks.
- Normalized catalog identities, retained revision/commit audit descriptors,
  publication preparation, and current-head-only catalog reads.
- Durable download-to-ingest handoff, exact attempt/lease fencing, and
  coordinated completion.
- Bounded source-build, analysis, publication, cleanup, maintenance-gate,
  canonical-value, event, and hash-cache workflows.

Cross-table workflows use one connector and one managed transaction. Mutable
authority is represented by normalized heads, generations, leases, seals, and
receipts rather than by caller-supplied counts, cursors, digests, names, or
tokens. Long-running work advances through bounded, replayable batches; pointer
publication validates sealed scalar state in a short transaction.

## Installation

This repository uses a `src` layout and an independent uv environment. It is
not part of a uv workspace, and `uv.lock` is intentionally ignored.

Rebuild the repository-local Python and Markdown tool environments with:

```bash
./scripts/rebuild-env.sh
```

## Configuration

```json
{
  "database": {
    "sql_type": "sqlite",
    "database": "/var/lib/h2hdb/catalog.sqlite3",
    "access_mode": "read-write"
  },
  "maintenance": {
    "optimize_enabled": true
  },
  "logger": {
    "level": "INFO",
    "file": null
  }
}
```

For MariaDB, also set `host`, `port`, `user`, `password`, and `database`.
The supported MariaDB baseline is 10.11.11, including Synology's
10.11.11-1551 package build. The integration gate pins the upstream
`mariadb:10.11.11` image and verifies the server version before creating its
test database.
Read-only consumers should use `"access_mode": "read-only"` and a database
account limited to the metadata/read privileges required by schema validation
and application reads.

JSON string values consisting exactly of `${ENV_NAME}` are resolved from the
process environment before validation. Variable names must match
`[A-Za-z_][A-Za-z0-9_]*`; missing or invalid variables stop startup without
including their values in the error. Inline interpolation such as
`db-${INSTANCE}` is deliberately unsupported, and unknown JSON fields are
rejected.

## Schema administration

The CLI exposes exactly three operations:

```bash
uv run --no-sync python -m h2hdb migrate --config config.json
uv run --no-sync python -m h2hdb check --config config.json
uv run --no-sync python -m h2hdb ready --config config.json
```

Choose the operation from database state:

| Database state or caller | Operation |
| --- | --- |
| Truly empty database | Run `migrate` to construct epoch 2/version 1 |
| Matching interrupted `BUILDING` epoch | Rerun `migrate` to resume |
| Matching `READY` epoch | Run read-only `check` for the full audit |
| Consumer startup | Run `check`; never initialize schema |
| Frequent readiness probe | Run the O(1) read-only `ready` check |
| Previous, foreign, or drifted schema | Create a new empty database and rebuild |

The wheel-resident generated provider must resolve every required runtime
validator and recurring writer binding before it opens or mutates a database;
the public administration API does not accept a substitute provider. `check`
holds a read transaction while validating the complete `READY` schema;
`ready` validates only the exact epoch/version/manifest marker.

Applications can use the same administration boundary directly:

```python
from h2hdb import VNextDatabaseAdminFacade, load_config

config = load_config("config.json")
admin = VNextDatabaseAdminFacade(config)
admin.initialize()  # deployment init job only
admin.check()  # full read-only audit
admin.check_readiness()  # lightweight probe
```

## Public application API

Consumers should import the public facades and immutable domain values from
`h2hdb`; they must not import connector, repository, generated-schema, or table
implementation modules.

Current-head catalog reads use `open_database`, which performs the full
manifest-bound `READY` audit before returning a `VNextCatalogFacade`. The
returned descriptor fences one call and is accepted only while it still equals
the current head; a head advance makes an older descriptor fail closed:

```python
from h2hdb import load_config, open_database

catalog = open_database(load_config("readonly-config.json"))
revision = catalog.get_catalog_revision()
page = catalog.list_publications(
    revision=revision,
    offset=0,
    limit=50,
    require_artifact=True,
)
publication = catalog.get_publication("42", revision=revision)
```

Download request creation, bounded listing, and exact-request completion use
`VNextDownloadQueueFacade`:

```python
from h2hdb import VNextDownloadQueueFacade, load_config

queue = VNextDownloadQueueFacade(load_config("writer-config.json"))
request = queue.request_download(42, "https://example.invalid/gallery/42")
pending = queue.list_download_requests(limit=100)
queue.complete_download_request(request)
```

Each facade call owns a fresh connection and one bounded read or write
transaction. Repository methods that accept connectors or units of work remain
internal coordination surfaces.

`VNextIngestFacade.prepare_source()` consumes the source adapter once, outside
every database transaction, and freezes the exact observation pages in a
private disk-backed spool. The manifest preflight and later bounded staging
steps therefore read the same immutable bytes even if the live source changes
mid-run; closing the prepared-source handle removes the temporary spool.

After `complete_ingest()` releases its SHARED gate lease, resident integrations
call `VNextIngestFacade.drain_current_only_maintenance()`. Each cleanup
transaction selects at most 256 logical cleanup keys/families under a renewable
EXCLUSIVE lease; each selected key executes only a schema-fixed bounded set of
physical deletes. One public attempt advances at most 16 cleanup batches. The
typed result is `DONE`,
`PROGRESSED`, `BLOCKED`, or `CONTENDED`; residents immediately retry
`PROGRESSED`, while blocked/contended attempts use the ordinary poll cadence.
Every result retains no caller capability, and durable shard checkpoints make
response-loss replay safe. Cleanup retains the prior payload until the new
current receipt is fully `PROJECTION_FINALIZED` and no live
publication-candidate or source-build predecessor pins it.

### Deliberate current limits

- A nonblank catalog search query fails closed until a normalized,
  current-head search index is part of the manifest and reader contract.
- The durable contract needed to derive `CatalogPublication.redownload_required`
  for the current revision is not closed. Readers therefore do not infer it from
  transient operational rows.
- Core defines and orchestrates the typed artifact-preparation/storage
  boundary, but concrete filesystem and object-storage behavior remains in the
  consumer adapter.

## Verification

The fast, read-only gate and the complete repository gate are:

```bash
./scripts/check-fast.sh
./scripts/check-full.sh
```

`verify-formal.py schema` checks manifest validity and generator drift for the
physical, Lean, operational-refinement, and runtime-provider artifacts. The
Lean target proves the declared closed-world BCNF and decomposition statements
under their explicit assumptions; it does not by itself prove SQL/runtime
refinement.

`coverage --validate-only` validates the evidence-index structure while still
reporting unresolved production blockers. Plain `coverage` is the strict
production-readiness gate and must remain nonzero until every reported blocker
has real evidence. Schema and Lean success must not be presented as strict
coverage success.

SQLite tests run locally. Set `H2HDB_TEST_MARIADB=1` with a working Docker
daemon to include the pinned MariaDB 10.11.11 testcontainer cases.

The distribution boundary can be checked with:

```bash
uv run --no-sync python scripts/build-and-verify-distributions.py \
  --output-directory /path/to/empty/output-directory
```

It builds in a fresh temporary directory, verifies the wheel's closed schema
surface and removed-module boundary, and confirms that the installed CLI
exposes only `migrate`, `check`, and `ready`.

### Local release gate

Install the versioned Git hooks once per clone:

```bash
./scripts/install-git-hooks.sh
```

The installer refuses to disable an existing hooks path or executable legacy
hook; compose those hooks explicitly before switching this clone to `.githooks`.
VS Code's built-in Git and command-line Git honor the installed hooks. GitHub
web edits and clones where the installer has not run do not.
The installer, merge workflow, and pre-push gate resolve the primary branch via
`scripts/detect-primary-branch.sh`; no branch name is hard-coded.

Ordinary task commits run only `check-fast.sh`. Every merge candidate runs the
complete exact-index release gate before the merge commit is created. The gate
validates task-level version impact and dependency-audit evidence, then runs:

- Ruff lint/format, strict mypy, and markdownlint-cli2;
- coverage-contract and generated-schema drift checks;
- the source and wheel schema-surface checks;
- Lean proofs and the required small TLC profiles;
- the complete SQLite and MariaDB 10.11.11 test suite; and
- the installed-distribution boundary check.

The gate requires the development environment, Docker for MariaDB, the Lean
toolchain declared by `lean-toolchain`, and either host Java or Docker for TLC.
Deep TLC remains an explicit manual check. A successful gate writes a local,
non-versioned receipt under the repository's Git metadata and binds it to the
exact committed tree and project version. The push proceeds only when that
receipt is valid, so retrying the same commit does not rerun the suite.

To verify a clean task `HEAD`, or compare an already merged tree with an explicit
task base, run:

```bash
uv run --no-sync python scripts/release-gate.py run
uv run --no-sync python scripts/release-gate.py run --base <task-base>
```

An integration script may validate a staged merge candidate before creating
the merge commit:

```bash
git merge --no-ff --no-commit <task-branch>
uv run --no-sync python scripts/release-gate.py run --index
```

The index must exactly match the working tree and contain no unresolved entries.
The receipt is keyed by `git write-tree`, so the resulting merge commit reuses
it as long as committing does not change the candidate tree. The normal task
workflow runs this through `scripts/git-flow-merge.sh`.

A version-changing task must first audit all direct Python and Node dependencies,
including latest releases outside current upper bounds:

```bash
.venv/bin/python scripts/audit-dependencies.py \
  --review-note "reviewed release notes and compatibility evidence"
```

The committed audit evidence is validated inside the same release gate; it is
not a second release receipt and does not replace the complete tests.

GitHub-hosted formal verification is manual-only. The PyPI workflow rechecks
the version transition, builds and smoke-tests the distributions, and publishes
them; the expensive correctness evidence is owned by the local release gate.

## Multi-repository development

The repositories remain independent projects. For an isolated editable-install
smoke environment, run:

```bash
./scripts/rebuild-multirepo-integration.sh
```

Consumers resolve from the configured package index by default; the checked-out
core is the only implicit local source. To exercise an explicit unpublished
artifact or source, pass it by package name, for example:

```bash
./scripts/rebuild-multirepo-integration.sh \
  --source h2hdb-ingest=/tmp/h2hdb_ingest.whl \
  --source h2hdb-opds='git+https://github.com/Kuan-Lun/h2hdb-opds.git@ref'
```

See [`docs/multi-repo-deployment.md`](docs/multi-repo-deployment.md) for the
database ownership, initialization, and consumer-adapter deployment boundary.

## License

GNU General Public License version 3 (GPLv3). See `LICENSE` for the complete
terms.
