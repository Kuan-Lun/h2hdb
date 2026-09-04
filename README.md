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

## What this package provides

- One generated epoch-3/schema-v2 schema for SQLite and MariaDB.
- Safe initialization, full schema auditing, and lightweight readiness probes.
- Current-catalog discovery with Unicode-normalized search, exact facets,
  keyset pagination, and fixed recently uploaded/downloaded windows.
- Single-publication, acquisition, cover, thumbnail, and ordered-page metadata
  through immutable backend-neutral values.
- Durable download, ingest, publication, cleanup, lease, and retry coordination
  through public facades.

Catalog readers only see the current publication head. A caller-supplied
revision or cursor is checked against durable database authority and fails
closed if the catalog advances or the value was forged.

## Compatibility model

The active database identity is `epoch=3`, `schema_version=2`. This is a
greenfield contract: schema v2 does not upgrade or adopt schema v1 or any older
database, provide compatibility views, retain old list APIs, or dual-write old
and new shapes. Replace an earlier database with a truly empty database and
rebuild it from source through the current ingest integration.

`migrate` constructs or resumes only this checksum-bound schema. An interrupted
matching `BUILDING` run can resume; a previous, foreign, drifted, or otherwise
non-empty database is rejected rather than adopted or repaired in place.

The logical schema sources are
[`verification/schema/catalog.toml`](verification/schema/catalog.toml) and
[`verification/schema/operational.toml`](verification/schema/operational.toml).
Generated artifacts and executable checks are authoritative for relation and
bootstrap details, so this README does not copy counts that can drift.

## Installation

`h2hdb` requires Python 3.14 or later. Install the published package into each
core administration or consumer environment:

```bash
python -m pip install h2hdb
```

For development from a source checkout, rebuild the repository-local Python
and Markdown tool environments with:

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
python -m h2hdb migrate --config config.json
python -m h2hdb check --config config.json
python -m h2hdb ready --config config.json
```

Choose the operation from database state:

| Database state or caller | Operation |
| --- | --- |
| Truly empty database | Run `migrate` to construct epoch 3/schema v2 |
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

The generated schema is shipped as a small Python loader plus a raw, bounded
protocol-5 pickle resource; the wheel or sdist compressor handles distribution
compression. The resource is part of the same trusted code cohort as the
loader, which authenticates its fixed name, exact size, and SHA-256 digest
before parsing it. Generator drift checks, schema-surface scans, and fresh
distribution gates apply a bounded abstract opcode interpreter to that exact
digest; the interpreter excludes globals, callables, classes, persistent IDs,
extensions, out-of-band buffers, mutable aliases, non-string dictionary keys,
and memo graphs whose unfolded tree exceeds the byte/node/depth caps. The
production loader does not repeat that development-time opcode proof after the
fixed resource has matched its loader-pinned identity, size, and digest. It
still uses a restricted unpickler followed by closed
type/order/node/depth/cycle/ownership validation. This fixed, wheel-owned path
is not a generic untrusted-pickle API. The eager `ARTIFACT` contract
preserves exact values, dictionary order, and list/tuple/bytes/bool/int types;
deduplicated immutable object identity is only a storage optimization and is
not an API guarantee. A plain `import h2hdb` does not load this resource;
explicit schema-provider use decodes it once per process. The generator
preserves an existing authenticated blob when its canonical logical payload is
unchanged, insulating committed output from compatible pickler differences.

Applications can use the same administration boundary directly:

```python
from h2hdb import VNextDatabaseAdminFacade, load_config

config = load_config("config.json")
admin = VNextDatabaseAdminFacade(config)
admin.initialize()  # deployment init job only
admin.check()  # full read-only audit
admin.check_readiness()  # lightweight probe
# A storage-owning integration supplies its durable filesystem/object-store UUID.
admin.bind_storage_instance(storage_instance_uuid)
```

## Public application API

Consumers should import the public facades and immutable domain values from
`h2hdb`; they must not import connector, repository, generated-schema, or table
implementation modules.

`bind_storage_instance()` is a one-time immutable database-to-storage binding.
The first call stores the integration-supplied non-nil 16-byte UUID; an exact
retry is write-free, while a different UUID fails closed. There is no rebind,
unbind, migration, or path-derived identity compatibility surface.

Current-head catalog reads use `open_database`, which performs the full
manifest-bound `READY` audit before returning a `VNextCatalogFacade`. The
`CatalogRevision` returned by `get_catalog_revision()` can fence subsequent
calls and is accepted only while it still equals the current head; a head
advance makes an older descriptor fail closed:

```python
from h2hdb import (
    CatalogDiscoveryQuery,
    CatalogFacetKind,
    CatalogRecentOrder,
    load_config,
    open_database,
)

catalog = open_database(load_config("readonly-config.json"))
revision = catalog.get_catalog_revision()
query = CatalogDiscoveryQuery(search="example title")
page = catalog.discover_publications(
    query=query,
    limit=50,
    revision=revision,
)
languages = catalog.list_publication_facets(
    facet=CatalogFacetKind.LANGUAGE,
    query=query,
    limit=50,
    revision=revision,
)
recent = catalog.list_recent_publications(
    order=CatalogRecentOrder.UPLOADED,
    revision=revision,
)
publication_id = "urn:h2h:gallery:42"
publication = catalog.get_publication(publication_id, revision=revision)
presentation = catalog.get_publication_presentation(
    publication_id,
    revision=revision,
)
```

`discover_publications()` uses seek cursors and accepts normalized AND search
plus exact language, subject, and contributor filters. Search is backed only by
the revision-scoped SQL index; it does not hydrate every publication to match in
Python. `list_publication_facets()` exposes exact language, subject, and
contributor counts under the other active filters. `list_recent_publications()`
has no caller limit or cursor: it returns the complete fixed window of at most
128 acquisition-bearing publications in uploaded or downloaded order.

Acquisitions and images are exposed as immutable, backend-neutral descriptors.
The acquisition descriptor carries a download name, media type, and opaque
storage-object identity. Presentation reads expose cover, thumbnail, page count,
and individual page descriptors with byte extent, media type, digest, and image
dimensions. Consumers resolve those descriptors through their own storage
adapter; core neither assumes a CBZ layout nor opens image/archive bytes.

Download request creation, bounded listing, and exact-request completion use
`VNextDownloadQueueFacade`:

```python
from h2hdb import VNextDownloadQueueFacade, load_config

queue = VNextDownloadQueueFacade(load_config("writer-config.json"))
request = queue.request_download(42, "https://example.invalid/gallery/42")
pending = queue.list_download_requests(limit=100)
queue.complete_download_request(request)
```

Each facade call owns fresh database connections and bounded transactions.
Catalog reads use a pinned snapshot and then a fresh current-head fence before
returning, so a concurrent head advance fails closed. Repository methods that
accept connectors or units of work remain internal coordination surfaces.

`VNextIngestFacade.prepare_source()` consumes the source adapter once, outside
every database transaction, and freezes the exact observation pages in a
private disk-backed spool. The manifest preflight and later bounded staging
steps therefore read the same immutable bytes even if the live source changes
mid-run; closing the prepared-source handle removes the temporary spool.

After `complete_ingest()` releases its SHARED gate lease, resident integrations
call `VNextIngestFacade.drain_current_only_maintenance()` with their artifact
release-adapter registry. If an unpublished abandoned candidate still protects
external resources and blocks database cleanup, one attempt terminally releases
one of them outside every database transaction and then commits its
acknowledgement.
The next attempt resumes the existing database cleanup fixed point. A lost
adapter response repeats the same idempotent protection-token tombstone; it
does not rebuild the catalog, release a current-publication token, or remove
reader-visible bytes. Callers without external artifacts may omit the registry
and retain the database-only behavior.

Each cleanup transaction selects at most 256 logical cleanup keys/families
under a renewable EXCLUSIVE lease; each selected key executes only a
schema-fixed bounded set of physical deletes. One public attempt advances at
most 16 cleanup batches. The typed result is `DONE`,
`PROGRESSED`, `BLOCKED`, or `CONTENDED`; residents immediately retry
`PROGRESSED`, while blocked/contended attempts use the ordinary poll cadence.
Every result retains no caller capability, and durable shard checkpoints make
response-loss replay safe. Cleanup retains the prior payload until the new
current receipt is fully `PUBLISHED` and no live
publication-candidate or source-build predecessor pins it.

### Byte ownership and current limits

The ingest integration owns concrete archive and artwork bytes: it renders,
stores, protects, resolves, and eventually releases them through its adapters.
Core owns only transactional coordination and sealed neutral descriptors; it
does not choose storage paths, mandate CBZ/ZIP, decode artwork, or perform
filesystem/object-storage I/O.

The durable contract needed to derive
`CatalogPublication.redownload_required` for the current revision is not
closed. Readers therefore do not infer it from transient operational rows.

## Deployment

The repositories remain independent packages; they are not a shared uv
workspace. See
[`docs/multi-repo-deployment.md`](docs/multi-repo-deployment.md) for database
ownership, clean initialization, credentials, startup order, descriptor
resolution, and backup boundaries.

## Development and verification

Repository contributors should read [`AGENTS.md`](AGENTS.md) before changing
code or schema. The local fast check and bounded release check are:

```bash
./scripts/check-fast.sh
./scripts/check-full.sh
```

The release check covers formatting, typing, generated-schema drift, schema
surface, formal evidence, the installed distribution, and a pytest merge
profile with a 300-second aggregate hard deadline. Its canonical runner is
`scripts/run-pytest.py merge`: the first phase selects
`not deep and not mariadb` with automatic xdist workers; the second uses one
worker and `H2HDB_TEST_MARIADB=1` for only
`mariadb_smoke and mariadb and not deep` against pinned MariaDB 10.11.11. Docker is
therefore required for the release check's MariaDB smoke phase. The deadline
includes termination and reaping of the pytest/xdist POSIX process group; on
Windows, interrupted runs use `taskkill /T`. A test that deliberately creates a
detached operating-system session is outside that process-group guarantee.
Testcontainers/Ryuk cleanup inside the Docker daemon can finish after the runner
exits and is not claimed by that deadline.

Plain `pytest` defaults to `not deep` with automatic bounded xdist workers and
does not enable the live service, but that direct command has no aggregate
wall-clock deadline. Use `scripts/run-pytest.py merge` when the five-minute
ceiling must be enforced.
Run `scripts/check-pytest-deep.sh` explicitly for the full non-MariaDB suite
followed by the full live-MariaDB suite. That manual profile has no default
timeout and requires Docker. Deep matrix results are not part of the
exact-tree release receipt and must not be reported as though every merge ran
them.

Run `scripts/check-mariadb-server-crash-deep.sh` for the separately bounded
MariaDB 10.11.11 server-crash case. It sends `SIGKILL` only to its uniquely
named disposable database container, restarts MariaDB on the same uniquely
named volume, and cleans up those exact resources. Docker, the host kernel, and
the physical storage remain alive, so this is server-process crash evidence,
not host or guest power-loss evidence.

### Manual disposable-VM power-cut experiment

`scripts/storage-guest-powercut.py` provides a deliberately manual two-stage
SQLite storage-binding experiment. The external whole-guest hard stop is never
performed by any pytest gate. A deep-only regression test exercises the
harness protocol with an ordinary process restart; it is excluded from the
bounded merge profile and is not power-cut evidence. Run `prepare` inside a
disposable POSIX VM, passing an absolute path for a new, dedicated state
directory on storage that survives a guest restart:

```bash
.venv/bin/python scripts/storage-guest-powercut.py prepare \
  --state-directory /var/lib/h2hdb-powercut/case-001
```

After it prints `H2HDB_GUEST_POWERCUT_READY`, hard-stop the entire guest from
the hypervisor without asking the guest OS to shut down. Reboot the same guest
with the same storage attached, then run:

```bash
.venv/bin/python scripts/storage-guest-powercut.py verify \
  --state-directory /var/lib/h2hdb-powercut/case-001
```

The harness refuses to prepare an existing directory or verify one containing
unexpected entries. It verifies the full schema, SQLite integrity, foreign
keys, the response-lost storage-binding commit, exact replay, and rejection of
a different storage UUID. The tool never powers off the VM itself. Killing
only the `prepare` process and restarting it validates the harness protocol,
but is **not** guest power-cut evidence. Even an external guest hard stop does
not reproduce loss of the physical host, storage controller, or their caches,
and this core-only experiment does not cover CBZ or other ingest filesystem
artifacts.

For an isolated editable-install smoke containing explicit consumer sources,
run:

```bash
./scripts/rebuild-multirepo-integration.sh
```

Consumers otherwise resolve from the configured package index. To exercise an
unpublished wheel, Git ref, or local project, pass it explicitly by package
name:

```bash
./scripts/rebuild-multirepo-integration.sh \
  --source h2hdb-ingest=/tmp/h2hdb_ingest.whl \
  --source h2hdb-opds='git+https://github.com/Kuan-Lun/h2hdb-opds.git@ref'
```

## License

GNU General Public License version 3 (GPLv3). See `LICENSE` for the complete
terms.
