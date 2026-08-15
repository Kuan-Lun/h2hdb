# h2hdb

`h2hdb` is the database and coordination core for the H2HDB multi-repository
system. It owns the SQLite/MariaDB schema, bounded transactional workflows, and
backend-neutral application facades used by ingest, download, Komga, and OPDS
consumers.

It deliberately does **not** scan files, parse `galleryinfo.txt`, manipulate
images, choose filesystem paths, serve HTTP, serialize OPDS documents, or
depend on `hbrowser`. Those responsibilities belong to consumer adapters and
sibling packages.

## Greenfield BCNF schema

The current schema is a clean epoch-2 design:

- 125 catalog data-plane relations, each checked as BCNF.
- 25 declared decompositions, each checked as lossless and
  dependency-preserving.
- 76 operational control-plane relations, each checked as BCNF. This count
  includes the epoch-control relation; the generated CREATE-only provider owns
  the other 75.
- One generated physical schema for SQLite and MariaDB, with backend-specific
  SQL rendered from the same closed-world manifests.

The logical sources of truth are
[`verification/schema/catalog.toml`](verification/schema/catalog.toml) and
[`verification/schema/operational.toml`](verification/schema/operational.toml).
They declare functional dependencies, keys, decompositions, bootstrap facts,
and semantic obligations. Deterministic generators derive the physical
manifests, Lean schema proofs, and the wheel-resident runtime provider. Generated
SQL is not a second schema-authoring surface.

This is a greenfield cutover. There is no v1-v7 upgrade or adoption path, no
compatibility view, and no dual write. A previous or foreign database must be
replaced with an empty database and rebuilt from source data.

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
- Normalized catalog identities, immutable revisions, publication preparation,
  and revision-pinned reads.
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

```bash
uv venv --python 3.14
uv pip install -e ".[dev]"
```

Rebuild the local environment after toolchain changes with:

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

The default generated provider must resolve every required runtime validator
and recurring writer binding before it opens or mutates a database. `check`
holds a read transaction while validating the complete `READY` schema;
`ready` validates only the exact epoch/version/manifest marker.

Applications can use the same administration boundary directly:

```python
from h2hdb import VNextDatabaseAdminFacade, load_config

config = load_config("config.json")
admin = VNextDatabaseAdminFacade(config)
admin.initialize()       # deployment init job only
admin.check()            # full read-only audit
admin.check_readiness()  # lightweight probe
```

## Public application API

Consumers should import the public facades and immutable domain values from
`h2hdb`; they must not import connector, repository, generated-schema, or table
implementation modules.

Revision-pinned catalog reads use `open_database`, which performs the full
manifest-bound `READY` audit before returning a `VNextCatalogFacade`:

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

### Deliberate current limits

- A nonblank catalog search query fails closed until a normalized,
  revision-pinned search index is part of the manifest and reader contract.
- The durable contract needed to derive `CatalogPublication.redownload_required`
  for a pinned revision is not closed. Readers therefore do not infer it from
  transient operational rows.
- Core defines the typed artifact-preparation/storage boundary, but a concrete
  filesystem or object-storage adapter and complete public ingest orchestration
  are not shipped here; the consumer integration must supply them.

## Verification

The schema workflow and implementation checks are:

```bash
uv run --no-sync python scripts/verify-formal.py coverage --validate-only
uv run --no-sync python scripts/verify-formal.py schema
uv run --no-sync python scripts/verify-formal.py lean
uv run --no-sync black --check src tests scripts
uv run --no-sync ruff check src tests scripts
uv run --no-sync mypy src tests scripts
uv run --no-sync pytest
uv run --no-sync python -m build
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
daemon to include MariaDB testcontainer cases.

The distribution boundary can be checked with:

```bash
uv run --no-sync python scripts/build-and-verify-distributions.py \
  --output-directory /path/to/empty/output-directory
```

It builds in a fresh temporary directory, verifies the wheel, and confirms that
the installed CLI exposes only `migrate`, `check`, and `ready`.

## Multi-repository development

The repositories remain independent projects. For an isolated editable-install
smoke environment, run:

```bash
./scripts/rebuild-multirepo-integration.sh
```

See [`docs/multi-repo-deployment.md`](docs/multi-repo-deployment.md) for the
database ownership, initialization, and consumer-adapter deployment boundary.

## License

GNU Affero General Public License v3 or later. See `LICENSE`.
