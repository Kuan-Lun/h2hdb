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
- Public ports: `CatalogReader`, `CatalogPublisher`, `DownloadCoordinator`, and
  `DatabaseAdmin`.

The catalog projection consists of `catalog_publications`,
`catalog_contributors`, `catalog_subjects`, `catalog_artifacts`, and the
singleton `catalog_revision` pointer. Immutable descriptors for every published
revision live in `catalog_revision_history`. A complete snapshot and its history
entry are inserted before the pointer advances in one transaction. The publish
is fenced by a live ingest lease, unchanged projections reuse the current
revision, and readers only see fully published revisions. Every newly published
publication records its exact canonical `source_gallery_name` and, when content
exists, `content_sha256`, so ingest can preserve deduplication incumbents.

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
two runtime operations are:

```bash
uv run --no-sync python -m h2hdb migrate --config config.json
uv run --no-sync python -m h2hdb check --config config.json
```

Choose the operation from database state, not from container lifecycle:

| Database state | Operation |
| --- | --- |
| Empty | Run `python -m h2hdb migrate` once |
| Non-empty and has the supported `h2hdb_schema_migrations` ledger | Run the same forward-only `migrate` command |
| Consumer startup | Run `python -m h2hdb check`; never migrate |

`migrate` deliberately refuses a non-empty database without
`h2hdb_schema_migrations`. There is no old-schema recognition, adoption,
backfill, config fallback, or upgrade command. Replace an old database with an
empty one and rebuild it through the current ingest workflow. A database that
already has the current ledger may use later forward-only migrations as they
are added.

The schema version is stored in the append-only `h2hdb_schema_migrations`
ledger and is independent of the Python package version. Consumer startup must
call `check_compatibility()`; it must not migrate.
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
only the `migrate` and `check` CLI operations. It copies artifacts to the
requested empty output directory only after every check passes.

SQLite runs locally. Set `H2HDB_TEST_MARIADB=1` with a running Docker daemon to
include the MariaDB testcontainer cases.

## License

GNU Affero General Public License v3 or later. See `LICENSE`.
