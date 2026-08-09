# h2hdb

`h2hdb` is the database and coordination core for the H2HDB multi-repository
system. It owns the durable schema and exposes backend-neutral public ports to
ingest, download, Komga, and OPDS consumers.

It deliberately does **not** scan files, parse `galleryinfo.txt`, manipulate
images or CBZ files, serve HTTP, serialize OPDS documents, or depend on
`hbrowser`. Those responsibilities live in sibling packages.

## Core responsibilities

- MariaDB and SQLite connectors, transactions, and read-only access.
- Forward-only, versioned schema migrations.
- Durable download queue and token-fenced download/ingest leases.
- Database maintenance gate and scheduling state. Core public operations own
  participation; consumers do not wrap calls themselves.
- Neutral catalog domain models and revision-based projection.
- Public ports: `CatalogReader`, `CatalogPublisher`, `CatalogBuildCoordinator`,
  `CatalogBuildAnalyzer`, `CatalogBuildProjectionCoordinator`,
  `DownloadCoordinator`, and `DatabaseAdmin`.

The catalog projection consists of `catalog_publications`,
`catalog_contributors`, `catalog_subjects`, `catalog_artifacts`, and the
singleton `catalog_revision` pointer. Immutable descriptors for every published
revision live in `catalog_revision_history`. A complete snapshot and its history
entry are inserted before the pointer advances in one transaction. The publish
is fenced by a live ingest lease, unchanged projections reuse the current
revision, and readers only see fully published revisions. Every newly published
publication records its exact canonical `source_gallery_name` and, when content
exists, `content_sha256`, so ingest can preserve deduplication incumbents.

Large filesystem imports use the separate durable source-build workflow. A
UUID `CatalogBuild` is bound to an opaque source/config `scope_key` and a live,
token-fenced ingest turn. Discovery, gallery headers, unordered file chunks,
gallery completions, final analysis, and the global file-hash cache are written
in bounded idempotent batches. The plural header, file-chunk, and completion
operations commit one bounded multi-gallery scanner batch in one fenced
transaction; callers do not need one transaction per gallery. A gallery keeps
an O(1) durable staged-file counter, so completing even a giant gallery does
not count or materialize all of its file rows.

Discovery persists a scope-unique gallery name, root-relative source locator,
and optional metadata fingerprint. Source files persist their relative locator,
digest, and optional full stat observation. Scan-observation digests and their
version are deliberately distinct from the historical canonical source
manifest. `CatalogBuildAnalyzer` exposes hard-capped keyset pages and durable,
idempotent batches for the ordered `SOURCE_MANIFESTS`, `FILE_SPAM`,
`CONTENT_DIGESTS`, `CONTENT_OWNERS`, `GID_WINNERS`, and `FINAL_ANALYSES`
reducers. It preserves duplicate file occurrences and emits explicit empty
gallery sentinels, so ingest can derive the historical manifest and effective
content digest without hydrating the corpus. Core persists the resulting facts
but leaves Python `casefold`, title-length, and winner policy decisions to
ingest. Gallery
source pages return explicit lightweight `CatalogSourceGalleryRecord` values,
while files use a separate `(casefold(name), name, key)` keyset page. They never
silently return a partially hydrated `GallerySourceRecord`.

Candidate pages prefer selected incumbents from the active source build. Before
the first source build is active, they fall back to the current legacy catalog
projection using exact `content_sha256` or exact GID; deterministic binary
gallery-name ordering resolves an invalid legacy projection containing more
than one match. This fallback cannot recover a content incumbent from a legacy
publication whose nullable `content_sha256` was never recorded.

Partial source builds and reserved catalog revisions are invisible to readers.
`CatalogBuildProjectionCoordinator` exposes independent, hard-capped keyset
pages for selected galleries and their files. Prepared artifact receipts are
written in bounded plural batches, remain provisional until a compare-and-swap
page checkpoint protects them, and then feed bounded publication-selection
batches directly into the reserved revision. No operation constructs the full
publication list or hydrates every file in a gallery.

After final filesystem validation and projection staging reaches `COMPLETE`,
the caller repeatedly invokes `prepare_catalog_build_operations()` before
sealing. Each call advances at most 1,000 rows through keyset-paged time
normalization, removed-GID events, and exact-token deletion-marker
consumptions. The returned `CatalogBuildOperationalState.complete` is the
durable readiness gate. A deletion request racing publication raises the
public, retryable `CatalogOperationalGenerationStaleError`; the same `SEALED`
build can refresh its invisible preparation and retry instead of being
abandoned.

After the source and projection descriptors are sealed,
`publish_catalog_build_with_projection()` performs one short,
live-turn-fenced transaction: it verifies only scalar sealed/prepared state,
inserts immutable history/receipt and operational activation rows, swaps both
pointers, atomically accounts maintenance work, and clears the working build.
It does not scan source, projection, or operational effect rows. A lost commit
response returns the same durable receipt on retry without double-accounting.
`DB_COMMITTED` receipts pin published artifact pages (including gallery name,
GID, and upload time) so a later live lease can finish filesystem recovery and
idempotently acknowledge `PROJECTION_FINALIZED`. Unchanged projections reuse
the current catalog revision; their unused reserved rows are removed only by
bounded child-first cleanup.

The older `publish_catalog_build()` source-only API remains available for
non-user-facing compatibility workflows, performs the same bounded operational
preparation, and rejects a build with an unfinished projection. Once a build
and its matching activation row are the active source revision,
`DownloadCoordinator` reads catalog membership, pending redownload state,
removed-GID events, deletion markers, and stable redownload runtime from that
source authority. Older active source builds without a v5 activation row fall
back to the legacy tables. Legacy `publish_snapshot()` is rejected after any
source-build activation so the two authorities cannot diverge.

Removed-GID events are prepared per build but remain invisible until the same
transaction activates their source revision. Exact request-token completion
advances a per-GID revision acknowledgement, so retry and response loss cannot
complete a replacement request. Deletion requests use their own token plus a
singleton generation fence; consuming one build's token never consumes a
later re-request. Mutable accepted-submission times live in the stable
`catalog_gallery_redownload_times` table rather than immutable source rows.

The compatibility SQL view `todelete_rm_commands` follows the same exact
operational activation gate. With an active source authority, it resolves
filesystem paths from that build's durable `source_locator` and includes
effective explicit deletion markers, older copies of a duplicate GID, and the
persisted duplicate-page deletion flag. Exact-token consumptions suppress only
the request they consumed. Before activation (including a pre-v5 source pointer
without its matching activation row), the view falls back to legacy
`todelete_galleries`/`galleries_names`. Its single `cmd` column remains safely
single-quote escaped and uses `rm -rf --` so paths beginning with `-` are not
parsed as options.

Source-build history is deliberately retained only while its immutable source
rows remain. `list_catalog_build_cleanup_candidates()` lists abandoned builds,
source-only inactive builds, and inactive projections whose receipt is already
`PROJECTION_FINALIZED`; active, working, and pending-receipt builds are never
listed. For a projected build, call `prune_catalog_build_projection()` until
complete, then call `prune_catalog_build()` until complete. Both are bounded
and child-first. The first step deletes only build-scoped staging rows,
receipt, and descriptor; immutable historical `catalog_*` revision rows remain
readable. The second removes the old source revision and its multi-million-row
source copy. Activation/event rows deliberately have no foreign key to the
prunable build because unacknowledged removed-GID events must survive cleanup.

## Installation

This repository uses a src layout: the import package is `h2hdb`, while its
source lives under `src/h2hdb`.

```bash
uv venv --python 3.14
uv pip install -e ".[dev]"
```

`uv.lock` is intentionally ignored. Rebuild the local environment with:

```bash
./scripts/rebuild-env.sh
```

## Configuration and schema administration

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

JSON string values that consist exactly of `${ENV_NAME}` are resolved from the
process environment before validation, including values nested inside objects
and arrays. Variable names must match `[A-Za-z_][A-Za-z0-9_]*`; a missing or
invalid name stops startup without including the environment value in the
error. Inline interpolation is deliberately unsupported, so strings such as
`db-${INSTANCE}` remain literal. For example, keep a MariaDB password out of
the file with `"password": "${H2HDB_RW_DB_PASSWORD}"`.
Unknown JSON fields are still rejected after placeholder resolution.

For MariaDB, set `host`, `port`, `user`, `password`, and `database`. Consumers
such as OPDS should use `"access_mode": "read-only"`; their database account
needs `SELECT` plus `SHOW VIEW` so compatibility checks can validate critical
view definitions without write privileges.

Only the core administration command migrates current, versioned schema. Its
three runtime operations are:

```bash
uv run --no-sync python -m h2hdb migrate --config config.json
uv run --no-sync python -m h2hdb check --config config.json
uv run --no-sync python -m h2hdb ready --config config.json
```

Choose the operation from database state, not from container lifecycle:

| Database state | Operation |
| --- | --- |
| Empty | Run `python -m h2hdb migrate` once |
| Non-empty and has the supported `h2hdb_schema_migrations` ledger | Run the same forward-only `migrate` command |
| Consumer startup | Run `python -m h2hdb check`; never migrate |
| Container readiness probe | Run `python -m h2hdb ready` |

`migrate` deliberately refuses a non-empty database without
`h2hdb_schema_migrations`. There is no old-schema recognition, adoption,
backfill, config fallback, or upgrade command. Replace an old database with an
empty one and rebuild it through the current ingest workflow. A database that
already has the current ledger may use later forward-only migrations as they
are added.

The schema version is stored in the append-only `h2hdb_schema_migrations`
ledger and is independent of the Python package version. Consumer startup must
call `check_compatibility()`; it must not migrate.
The lightweight `ready` command checks only that ledger and deliberately does
not wait on the database maintenance gate. It is suitable for a frequently
repeated orchestrator probe; `check` remains the full schema-structure audit.
The later ingest `bootstrap-catalog.py` command is a separate data operation: it
does not create schema or write the schema ledger.

## Public API

Create the facade from `CoreConfig` or `load_config()`:

```python
from h2hdb import H2HDB, load_config

database = H2HDB(load_config("config.json"))
database.check_compatibility()
page = database.list_publications(limit=50)
historical = database.get_catalog_revision(1)
historical_page = database.list_publications(revision=historical, limit=50)
acquirable_page = database.list_publications(
    revision=historical,
    limit=50,
    require_artifact=True,
)
```

`get_catalog_revision()` resolves the current pointer;
`get_catalog_revision(revision_number)` loads a durable historical descriptor
or raises `CatalogRevisionNotFoundError`.

`get_publications_by_artifact_names(..., revision=descriptor)` participates in
the same revision pinning contract. `require_artifact=True` applies the artifact
predicate to both page rows and `total`, which lets acquisition-feed consumers
paginate without post-filtering.

Every supported revision persists exact `source_gallery_name`; consumers never
infer it from mutable canonical tables. `content_sha256` is nullable only for a
current-domain reason: a gallery can have no non-galleryinfo content after
exclusions.

Consumers should type against the exported protocols and domain models. Do not
import connector, repository, or table implementation modules from another
repository.

## Multi-repository development

The repositories remain independent projects; this is not a uv workspace. To
create an isolated editable-install integration environment for all local
packages, run:

```bash
./scripts/rebuild-multirepo-integration.sh
```

The script uses `uv venv` and `uv pip install -e` for each repository and does
not create or consume a lock file. See
[`docs/multi-repo-deployment.md`](docs/multi-repo-deployment.md) for the
container split, fresh database initialization, shared database, and CBZ mount
rules.

## Verification

```bash
uv run --no-sync black --check src tests scripts
uv run --no-sync ruff check src tests scripts
uv run --no-sync mypy src tests scripts
uv run --no-sync pytest
uv run --no-sync python -m build
uv run --no-sync python scripts/build-and-verify-distributions.py \
  --output-directory /path/to/empty/output-directory
```

The final command always builds in a fresh temporary directory. It verifies the
wheel boundary and confirms that an installation taken from that wheel exposes
only the `migrate`, `check`, and `ready` CLI operations. It copies artifacts to
the requested empty output directory only after every check passes.

SQLite runs locally. Set `H2HDB_TEST_MARIADB=1` with a running Docker daemon to
include the MariaDB testcontainer cases.

## License

GNU Affero General Public License v3 or later. See `LICENSE`.
