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

- One generated epoch-3/schema-v7 schema for SQLite and MariaDB.
- Safe initialization, full schema auditing, and lightweight readiness probes.
- Durable ingest startup and periodic audit scheduling, including crash recovery.
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

The active database identity is `epoch=3`, `schema_version=7`. Runtime accepts
only this exact generated schema. It provides no compatibility views, old list
APIs, dual writes, or automatic migration of earlier databases. The exact
schema-6 database has a one-use offline conversion path described below; it
preserves source, catalog, queue and artifact facts and all external CBZs.
Schema 5 and earlier still require a fresh database rebuilt through ingest.

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
| Truly empty database | Run `migrate` to construct epoch 3/schema v7 |
| Matching interrupted `BUILDING` epoch | Rerun `migrate` to resume |
| Matching `READY` epoch | `migrate` reports `already_ready` using marker admission; `check` performs the full audit |
| Ingest process startup | Use the managed audit session to select quick or full validation |
| Explicit complete audit | Run `check`; `open_database()` also retains this full-audit contract |
| Frequent readiness probe | Run the O(1) read-only `ready` check |
| Exact READY schema 6 | Stop consumers and run the one-use offline converter |
| Other previous, foreign, or drifted schema | Create a new empty database and rebuild |

The wheel-resident generated provider must resolve every required runtime
validator and recurring writer binding before it opens or mutates a database;
the public administration API does not accept a substitute provider. `check`
holds a read transaction while validating the complete `READY` schema;
`ready` validates only the exact epoch/version/manifest marker.

`migrate` is provisioning, not an implicit audit of an existing database. A
matching `READY` control has a read-only, fixed-cost admission path and reports
`outcome=already_ready`, `audit=not_performed`. Empty databases and matching
interrupted `BUILDING` epochs execute all generated DDL/bootstrap slices and
complete the structural, bootstrap and activation semantic checks before
transitioning to `READY`. Each slice still validates its exact objects; fresh
closed-world inventories bracket final validation instead of scanning the
entire namespace after every DDL statement. Recovery never reuses an inventory
from a previous attempt. Invalid controls, foreign manifests, unreadable state
and database errors fail closed; a failed probe is never treated as empty.

The public `initialize()` result is `SchemaProvisioningReport`.
Its `outcome` is `SchemaProvisioningOutcome.CREATED`, `.RESUMED` or
`.ALREADY_READY`. Only the first two outcomes carry a `SchemaEpochReport` in
`activation_audit`; the last carries `None`. An exact marker does not detect
later data-plane/schema drift. Explicit `check()` and `open_database()` still
perform complete audits; quick admission never returns a fabricated full-audit
report. An administration script requiring a complete audit must call `check`.

Python callers that previously interpreted `initialize()` as a full audit must
explicitly call `admin.initialize(); report = admin.check()` and consume that
`SchemaEpochReport`. Callers that only provision should consume the new
`SchemaProvisioningReport`.

### Managed ingest audit scheduling

`VNextDatabaseAdminFacade.start_ingest_runtime()` reserves a generation/token
and selects the startup check using core-owned mutable scheduling state. It
performs a full audit when no successful baseline exists, after an unclean
ingest termination, when the validator version changes, when the schedule is
due, or when explicitly forced. A clean restart within the interval uses the
fixed-cost readiness probe. An active predecessor lease blocks a new runtime;
expired ownership may be replaced, and the superseded process cannot record a
successful audit or a clean exit.

`check_ingest_runtime_if_due()` belongs between ingest sessions, including idle
polls. The interval is the larger of the configured minimum (default seven
days) and the last complete audit's measured duration multiplied by the
configured factor (default 100). The interval starts at successful completion,
not at the start of a possibly long audit. Ordinary batch completion does not
pretend to be a complete database audit or reset this deadline.

The first source catch-up has a separate, persistent scheduling grace period.
`mark_initial_catchup_complete()` accepts a one-time scheduling hint from the
source adapter after its last deferred batch. Galleries still downloading do
not postpone it indefinitely. Until then, periodic audits are deferred; crash,
validator-change and forced audits still run. The first catch-up completion
starts a full waiting interval without changing the real last-audit timestamp.
Later batches and restarts cannot renew that grace period.
The completion condition is the first empty deferred backlog, not a frozen
inventory of the initially present folders. If completed downloads keep arriving
faster than ingest can process them, this initial exemption can remain active;
manual `check`, crash recovery and validator-change audits remain available.

`renew_ingest_runtime()` maintains the process lease. During a full SQLite
read audit it does not require a competing write heartbeat; completion uses a
fresh exact-token check and discards a result if another runtime took over.
`finish_ingest_runtime()` is a clean-exit acknowledgement, to be called only
after work, heartbeats and resource cleanup succeed. SIGKILL or an unhandled
failure leaves an unclean session. This detects ingest process termination;
it is not a claim to detect every downloader, operating-system or database-server
failure. The scheduling row is not a permanent assertion that later database
contents are healthy; transaction, publication and reader validation remain
independent responsibilities.
An in-progress synchronous full audit is not immediately cancellable. A graceful
stop waits for it to return; a forced termination leaves the session unclean and
requires another full audit after its lease expires.

### One-use schema-6 conversion

Use the new core environment and this release's
[`scripts/upgrade-audit-schema.py`](scripts/upgrade-audit-schema.py):

```bash
python scripts/upgrade-audit-schema.py --config /path/to/core-config.json --consumers-stopped
```

Before running it, stop ingest, downloader, OPDS, Komga synchronization and all
other database clients, and take a verifiable database backup. The flag records
the operator's acknowledgement; an advisory schema lock alone cannot prove
that application clients are stopped. Keep the same environment variables
required by the database configuration. The tool only supports the exact
schema-6 manifests and the additive schema-7 provider shipped with this release.

It validates the retained schema, adds the generated scheduling relation,
performs all READY semantic checks on the existing data, and commits the new
READY marker together with the real audit completion time and duration. The
first ingest startup can therefore use that completed baseline instead of
immediately repeating the converter's full audit. This is an offline full audit
and can take as long as a manual `check`; it does not re-render or read CBZ bytes.
A conversion-only checksum
makes ordinary `migrate`, `ready` and consumers reject partial conversions.
On interruption, rerun the same tool. It checks the existing prefix instead
of dropping tables. SQLite conversion is atomic; MariaDB DDL can commit before
the final transaction, so the converter explicitly supports that retained
prefix. Repeating the tool on the converted schema performs a full audit and
reports `already_converted`.

Do not delete the existing database, library, CBZs or source folders for this
conversion. To return to old software after conversion, restore the pre-upgrade
database backup before restarting old consumers; runtime does not downgrade the
schema. The converter itself never changes external media.

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
provisioned = admin.initialize()  # deployment provisioning only
print(provisioned.outcome, provisioned.activation_audit)
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

`discover_publications()` uses seek cursors and combines every supplied predicate
with AND. `search` matches display/source titles, contributors and tag values;
`title` restricts the same token rules to display/source titles. The two scopes
share a total budget of sixteen lexemes. `gid` is an exact positive gallery ID;
a numeric `search` remains a text query. `subjects` is a tuple of up to sixteen
exact namespace/value filters, canonically sorted and deduplicated. It replaces
the former single `subject` argument. Language and contributor remain exact
filters.

`uploaded` and `downloaded` accept `CatalogTimestampRange(start=..., end=...)`:
timezone-aware bounds are normalized to UTC, the start is inclusive and the end
exclusive. They use the immutable gallery upload time and published occurrence
download time. `pages` accepts `CatalogPageCountRange(minimum=40, maximum=200)`,
with inclusive bounds on the sealed artifact's actual page count. Page bounds
are in 0..4096; a publication without an artifact never matches a page filter,
while a sealed zero-page artifact can match zero. Either range can omit one
bound, but empty or reversed ranges are rejected.

Search is backed by revision-scoped SQL postings, with title postings sealed as
an exact subset of the complete search document. Filtering happens before
bounded hydration; all predicates participate in cursor validation and the
query digest. Core accepts typed values; consumer applications own search-box
syntax. `list_publication_facets()` exposes exact language, subject, and
contributor counts while ignoring all selected filters of the requested family
and preserving every other predicate. `list_recent_publications()`
has no caller limit or cursor: it returns the complete fixed window of at most
128 acquisition-bearing publications in uploaded or downloaded order.

`list_tag_values()` pages an exact namespace in latest-upload order, breaking
ties by the exact UTF-8 tag value. `list_tag_publications()` pages an exact
`CatalogTagFilter` in uploaded-time descending, casefolded title ascending,
publication-identity ascending order. Both use sealed ordering and seek cursors,
with at most 128 results per page. `list_tag_values_with_publications()` returns
a `CatalogTagBundle`: its `page` is the same tag directory, and `publications`
contains each visible tag's first ranked publication in matching order. Shared
publications are hydrated once per page; no later publication is substituted
when the first has no acquisition or image. Consumers can use these descriptors
to illustrate directory entries without reading each tag separately.

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

`VNextIngestFacade.prepare_source()` freezes the exact source snapshot outside
write transactions in a private disk-backed spool. Adapters may provide a
completion marker whose unchanged bytes and complete stat tuple promise that
the completed gallery is unchanged. A first complete scan binds that marker to
the sealed observation; subsequent matching probes reuse its verified immutable
descriptor and normalized facts without reopening gallery content or copying
database page trees. Marker filename, content digest, size, device, inode,
modification/change nanoseconds, and observation interpretation version must
all match. The marker binding stores only its observation key and file key;
existing FILE, filesystem and content relations remain the sole owners of the
six fingerprint facts. Missing or changed markers use the full scan path.
Reuse rechecks durable authority under the live ingest fence before linking
the observation to a new build. Cleanup may evict an unreferenced binding and
its observation; that cache miss requires preparation again. No marker grants
authority to caller-supplied observation IDs or audit checksums.

`prepare_source(adapter, max_new_galleries=1000)` admits a cumulative batch.
It first validates the adapter's complete locator inventory, retains every
still-present member of the current published source, and selects at most
1,000 new galleries in canonical locator order. Existing changed galleries are
refreshed in the same batch and do not consume this allowance; missing galleries
are removed. An unchanged completion-marker cache avoids reading image bytes,
but a cache entry from an unpublished attempt does not count as published
membership. `VNextPreparedSource.deferred_gallery_count` reports remaining new
galleries so a resident can publish the batch and immediately start another
fresh inventory. This count is scheduling information, never database authority.
The current published source is the restart checkpoint, including duplicate
losers; a stale publication baseline fails before source handoff. Each admitted
cut still completes the ordinary analysis, artifact, validation and atomic
publication workflow. Omitting the limit admits the full inventory. No schema
change or separate persistent cursor is required. A finite, stable source drains
in successive batches; new source changes are discovered on subsequent passes.

`prepare_source(..., progress=observer)` optionally reports immutable
`VNextSourcePreparationProgress` values during local preparation. The observer
receives the current operation and absolute completed/total gallery counts;
`total=None` means discovery has not reached exact EOF. Operations distinguish
inventory transfer, discovery ordering, cumulative batch selection, batch
ordering, old inventory cleanup, and observation freezing. Selection counts all
checked inventory entries; freezing counts only admitted galleries. Callbacks
run outside catalog database transactions and should update in-memory counters
promptly. Ordinary observer exceptions do not change ingest results. These
counts are diagnostic observations, not durable restart or commit authority.
Discovery ordering consumes fixed-size keyset pages on the same temporary
connection before writing their positions, so its own read cursor cannot block
rollback-journal cache spill for a large inventory.

Analysis and publication calls emit progress through the standard
`h2hdb.ingest_performance` logger and the application's handlers.
The default `logger.level` remains `info`. INFO records describe the activity,
elapsed time and measured database work in readable sentences. Detailed
`ingest_db_performance` records, including issue/prepare/commit timing, processed
records, query counts, connection time and transaction time, belong to DEBUG.
Summaries also appear at 60-second checkpoints when a facade call returns.
SQL measurement only updates in-memory counters; handlers run after the outer
facade call releases its transactions and internal locks. A long-running call
is reported on completion, not by a background heartbeat: integrations should
retain their progress reporter to show an operation blocked inside a call.
Stage `wall_seconds` includes time between calls; `call_seconds` sums the calls
themselves. `other_seconds` includes Python, private scratch I/O, adapters, and
scheduling, and must not be interpreted as CPU time alone. SQL counters measure
connector calls, so `execute_many` counts once; connection calls are leases and
do not imply new TCP connections. Driver-internal setup queries belong to
connection time. These diagnostics cover the calling thread, not adapter worker
threads or source scanning. Sequential calls share a bounded stage accumulator.
Nested and overlapping calls produce separate `scope=nested` or
`scope=concurrent` records. Parent call time excludes nested call time; nested
records wait for the outer call to finish, with at most 64 deferred records and
an explicit omitted-record count. Expired or copied scopes cannot attribute
another thread or task's work to a completed call.

File-decision validation has a separately measured local preparation operation.
It announces its start and completion at INFO and reports elapsed time and
galleries read at 60-second safe boundaries outside catalog transactions.
The preparation cost must be included when comparing validation performance;
parent call time excludes this nested work. A single blocked database call
still needs the integration's progress reporter.

Setting the application's core logger configuration to `{"level": "debug"}`
adds structured stage and per-call completion records and the five query
fingerprints with the highest total SQL time. Each `query_top` entry identifies
the fingerprint and names its
`calls`, total `seconds`, `returned_rows`, and single-call `max_seconds`; entries
are separated by semicolons. This distinguishes repeated short queries from a
single slow call. Returned rows are the connector's result count, not the database
engine's examined rows; detecting an
index scan still requires a query plan. `operation`, `generation`, and `phase`
identify the corresponding facade call, including `PREPARE_SNAPSHOT` preparation.
A fingerprint is the first 16 hexadecimal characters of SHA-256 over the SQL
template's UTF-8 bytes; query statistics keep
at most 64 templates plus an overflow bucket per call. To locate a fingerprint,
take the assembled SQL template from the installed version's source, preserving
whitespace and `%s` placeholders, and calculate
`hashlib.sha256(sql.encode("utf-8")).hexdigest()[:16]`. Parameters are excluded.
SQL text, parameters, credentials, and authority tokens are never logged.
Configuration and handler
thresholds both apply; ordinary diagnostic clock, recorder, or handler failures
do not change commit or retry results. Operation labels come from the
orchestrator's validated state, without diagnostic inspection of caller handles.

### Audit and current-only cleanup diagnostics

Full schema audits and current-only cleanup use the standard
`h2hdb.database_performance` logger and the application's handlers. Records have
the prefix `database_performance`, a space, and JSON with `schema=1`.
The core CLI automatically routes these records through its configured console
and optional file logger, using the same `logger.level` setting.
The default INFO level records completed operations, failures, the five retained phases
with the highest exclusive elapsed time, and periodic snapshots of a long
operation's active phase. A full `schema_check` also announces its start.
Quick `check_readiness()` probes produce no performance records. Successful
`schema_initialize` calls that only admit an existing READY marker and idle
cleanup probes are DEBUG-only unless they last at least the reporting interval.

Set the core configuration's `logger.level` to `"DEBUG"` to add completed phase
records and query fingerprint statistics; an ingest configuration nests this
under `core.logger.level`. Both the core setting and application handler level
must admit DEBUG. This adds diagnostics without changing schema validation,
cleanup bounds, or lease duration. The operation names distinguish
`schema_initialize`, `schema_check`, and `current_only_cleanup`.

| Record detail | Meaning |
| --- | --- |
| `labels.audit`, `labels.result` | Initialization distinguishes `activation` from `not_performed`, with `created`, `resumed`, or `already_ready`; a full check is `ready` |
| `phase=semantic_validator` | `labels.validator` is the exact semantic obligation ID; `labels.lifecycle` distinguishes `BUILDING_TO_READY` from `READY_REVALIDATION` |
| Structural phases | Provider resolution, schema admission, inventories before/after validation, global structure, and activation-only bootstrap work |
| `phase=cleanup_batch` | Target, shard, cycle generation, logical row limit, and transaction result for one batch |
| `phase=cleanup_phase` | Inner checkpoint phase and attempted logical rows; these observations alone do not establish a committed deletion |
| `phase=gate_lock` | Elapsed lock acquisition; enclosing claim, renewal, release, or validation records report `lease_remaining_us` from the same fresh post-lock clock used for authorization |
| `query_top` | At DEBUG, the five retained query-statistic entries with the highest total time, including the `other` bucket when applicable, with counts, total/maximum seconds, and returned rows |

`sql_calls`, `sql_seconds`, `read_rows`, connection counters, and transaction
boundary counters include nested phases. Sum the `exclusive_*` counters instead
when reconciling separate phases; the root operation also retains its own
exclusive work. `elapsed_seconds` includes nested elapsed time, while
`exclusive_seconds` subtracts it. Elapsed time outside connector calls includes
Python work, scratch I/O, adapter work, and scheduling; it is not a CPU sample.
SQL duration is measured at the client and can include server execution, lock
waits, and transport. It cannot separate those causes. Returned rows do not
measure examined rows, and `execute_many` remains one connector call.

`transaction_outcome=committed` and `committed_logical_rows` are recorded only
after the transaction context returns successfully. Inner
`attempted_logical_rows` can describe work subsequently rolled back. A lost
COMMIT response remains `unconfirmed` even if the server committed; use the
durable checkpoint on retry to resolve it. Logical rows are workflow keys,
not the total physical child rows deleted. Telemetry is never a lease,
publication receipt, or proof of cleanup completion.

SQL observers only update memory. Completed phase records wait for the outer
operation to release its connector and transaction. A separate process-wide
daemon emits roughly 60-second progress snapshots without using the operation's
database connection or running handlers on its transaction thread. A blocked
SQL call contributes to SQL totals only when it returns; the active phase's
elapsed time continues to show the wait. Correlate by `operation_id` and order
by `sequence`: a sampled heartbeat can reach the handler after a later terminal
record. A delayed or failed handler can delay or omit a heartbeat, so it is not
a liveness guarantee or a cancellation deadline.

Each operation retains at most 256 completed phase records, prioritizing failures
and long phases, and reports `omitted_phase_records`; discarded phase details
do not discard operation totals. Nesting is capped at 64 with `omitted_depth`.
Query statistics retain the first 64 distinct fingerprints per span and combine
later unseen fingerprints into `other`. `query_top` ranks those retained entries;
it is not a global top-five query ranking when the fingerprint capacity is
exceeded. One daemon tracks at most 64 simultaneous operations; exhausted
heartbeat registration capacity is explicitly labelled. SQL templates, parameters,
credentials, owner tokens, and exception messages are not logged. Use the installed source
and the fingerprint calculation above to identify a query template.

### Performance investigations

Performance investigations use disposable synthetic data before changing runtime
behavior. The checkout-only probes below measure the production implementation;
they do not accept an existing database or server address. MariaDB runs use a
private MariaDB 10.11.11 Testcontainer. They are manual tools, not additional
merge gates or production-load tests:

- `scripts/investigate-maintenance-performance.py` records the new production
  audit/cleanup diagnostics while independently counting physical connector calls.
  It checks exact SQL/returned-row totals and retained fingerprint counts against
  the operation records. Source staging is observed through the test driver's
  action boundaries; it is not part of the new production audit/cleanup logger.
  `--group all --repeats 2` covers gallery/page row windows around 128, shared tag
  counts, raw comments near 32/64 KiB, retained history, policy compaction, and
  natural analysis depths 0 through 16 followed by compaction. Raw comment sizes
  are not encoded source canonical sizes or evidence of cache-budget occupancy.
  Each revision checks publication facts, three full audits, cleanup DONE and
  the next ingest claim; a final full READY audit checks the state after cleanup.
  Repeated source fixtures and unchanged audit repeats must retain their exact
  SQL counts. `--level warning`, `info`, and `debug` support controlled logger
  comparisons; fast read-only DONE probes intentionally omit INFO records.
  Memory adapters omit producer-marker reuse, filesystem work and actual archive
  rendering. The in-memory log sink excludes remote/disk handler latency.
  Reports retain all measured query groups within a hard budget, deduplicate SQL
  templates in `sql_texts`, and save measured source bytes beside the JSON in a
  `.sources` directory. Budget overflow invalidates the run. These finite
  measurements do not establish NAS latency or universal complexity bounds.
- `scripts/ingest_pipeline_probe.py` varies one source-shape dimension at a time
  and measures source preparation/persistence, analysis, and catalog publication.
  It verifies published results and a full READY audit. In-memory adapters and
  metadata-only publication deliberately exclude image decoding, CBZ rendering,
  and filesystem activation. Shapes allow up to 256 galleries with aggregate
  limits of 4096 pages, 4 MiB of comments, and 32768 tags. Public catalog checks
  traverse keyset pages and compare all expected publications. Each operation
  separates SQL, connection, transaction, and remaining time; the final READY
  audit records its existing validators separately. These timings are scoped
  observations, not proof that local proportions match a remote deployment.
- `scripts/ingest_growth_cleanup_probe.py --mode idle` measures two maintenance
  calls at the cleanup fixed point followed by an ingest claim. Candidate-target
  attribution distinguishes expensive absence checks from actual deletion work.
  On SQLite, `sqlite_progress_operations_estimate` samples progress callbacks
  with a 100-operation quantum, including possible statement preparation work.
  It is approximate work evidence, not an exact instruction or examined-row
  count; zero can mean work below the sampling quantum. MariaDB leaves these
  SQLite-only fields null. Claim-completion cleanup is measured separately from
  the idle sequence. Gallery count is capped at 128 with at most 512 total pages;
  consecutive idle calls and the claim run without report serialization between
  them. Separate diagnostic replays do not count toward these baseline timings.
- `scripts/ingest_growth_hash_probe.py` isolates file-decision validation while
  varying active hashes and unselected source history. Its source-history axis
  is not analysis-overlay depth; it records SQLite query plans or MariaDB
  ANALYZE/EXPLAIN and handler counters.

For example, start with the maintenance baseline, then run source and idle cases:

```sh
.venv/bin/python scripts/investigate-maintenance-performance.py \
  --backend sqlite --group baseline --repeats 2 --output /tmp/maintenance-sqlite.json
.venv/bin/python scripts/ingest_pipeline_probe.py \
  --backend sqlite --vary all --repeats 3 --output /tmp/pipeline-sqlite.json
.venv/bin/python scripts/ingest_growth_cleanup_probe.py \
  --backend sqlite --mode idle --galleries 8 --pages-per-gallery 1 \
  --revisions 2 --output-directory /tmp/idle-maintenance
.venv/bin/python scripts/ingest_growth_hash_probe.py \
  --backend sqlite --hashes 64 129 256 --history 0 4 \
  --output /tmp/hash-validation.json
```

Use `--backend mariadb` for the isolated live backend. Pipeline/hash output paths
must be new files; cleanup creates a separate run directory. Keep reports outside
the checkout. The tools use cooperative POSIX alarms, not hard process deadlines;
native calls and container startup/teardown can exceed the requested timeout.

Compare cases with the same backend and all but one input dimension fixed.
Record query counts, returned rows, actual source shape, and correctness along
with elapsed time; repeat fresh fixtures to assess timing variability. Source
provenance and measurement boundaries belong with every saved report. Returned
rows are not examined rows, nested timings overlap their enclosing operation,
and profiling itself adds overhead. Local synthetic timings cannot predict NAS
completion time. A completed report means its correctness checks passed, not
that a performance target was met. Keep wall-clock thresholds out of ordinary
regression tests; use deterministic work bounds when the contract justifies them.

Both pipeline and idle-maintenance probes accept `--mariadb-diagnostics` only
with their private MariaDB backend. Pipeline diagnostics capture Performance
Schema statement-digest deltas outside each timed phase. Maintenance diagnostics
replay captured candidate SELECTs separately from the idle baseline and retain
EXPLAIN/ANALYZE plans, session handler counters, and global row-lock counters.
These additional queries have their own cost. Keep diagnostic and ordinary
timings distinct, and serialize benchmarks when comparing wall time.

The pipeline probe's `--audit-cache-control` performs three complete READY audits
on the same privately published fixture: the current canonical cache, a control
with a derived 512-entry ceiling, then the restored current cache. The control
increases the per-entry bookkeeping charge; the 64 KiB per-value and 8 MiB total
charged-byte budgets stay fixed. The production charge is 512 bytes plus each
payload, giving an empty-value ceiling of 16,384 entries; nonempty values reduce
that ceiling. Charged bytes are logical accounting, not measured process RSS.
Every pass runs the original validators and
checks the same public catalog result. This isolates cache eviction costs without
changing production configuration or accepting an existing database. Cache
hit/miss/eviction counters observe the original decisions without changing LRU
order. These are warm synthetic audits, not measurements of NAS startup.

For a running MariaDB deployment, the separate manual
`scripts/collect_mariadb_performance.py` reads two existing statistics snapshots
over a short window. It accepts a core or ingest configuration file and uses a
separate connection with no default schema and a read-only session. It never
initializes/audits/scans the catalog, executes caller-supplied SQL, enables
instrumentation, resets counters, or runs ANALYZE against production data.
For example, copy this standalone script into the existing ingest Python
environment and run during the analysis stage:

```sh
python /tmp/collect_mariadb_performance.py \
  --config /config/h2hdb-ingest.json --config-kind ingest \
  --seconds 60 --output /tmp/mariadb-performance.json
```

The output must be a new file in an existing directory. Credentials and
exception text are excluded. Digest output contains normalized SQL prefixes,
not bound values; it is limited to 10000 digests and 2048 characters per prefix.
Pair the window with existing `ingest_db_performance` DEBUG records to compare
client calls with server statement costs. Digest statistics include all users
of the selected schema, while global counters include all server activity and
the collector's own small traffic. Server elapsed times can overlap and are not
CPU times or wall-clock percentages. The post-window SELECT-1 samples include
driver, server and transport overhead; they are not pure network latency.

The report explicitly flags disabled/inaccessible instrumentation, overflow,
counter decreases and changing counter sets. Missing statistics never mean
zero query cost. MariaDB's
[Performance Schema](https://mariadb.com/docs/server/reference/sql-statements/administrative-sql-statements/system-tables/performance-schema/performance-schema-overview)
may be disabled; enabling it requires server-start configuration. The collector
does not change that setting. Even with complete snapshots, a hidden counter
reset followed by sufficient regrowth may evade detection. This manual collector
is excluded from automatic live-service tests and merge gates.

Catalog preparation validates common tag values in batches of at most 128 and
retains their bytes in a private disk cache for that plan. BUILD and VALIDATE
prepare separate plans and independently revalidate their source snapshots.
Private scratch writes share one transaction, and failed preparation discards
the entire plan. Durable catalog child writes and comparisons remain bounded to
128 children while grouping compatible SQL operations. Already bounded canonical
scalars register directly, while unknown-length metadata uses a disk spool;
both inputs share exact domain, length and payload collision checks. Search
lexemes are deduplicated within each bounded field before registration. Typed
plan records centralize decoding for catalog writers and validators, and upload
time comparison independently checks immutable GID authority. These optimizations do
not change the schema or artifact format and require no data rebuild.

File-decision validation reads the selected sealed source observations once in
bounded pages and computes independent expected counts using a private disk
sort. Scratch input writes share one local transaction without extending any
catalog transaction. It retains authenticated fixed-width records for the
lifetime of that analysis preparation. Each validation page merges those
expected keys with fresh ancestor and actual key prefixes issued by the
repository, then compares the exact scalar families under the existing
generation, lease and checkpoint fences. Source facts are not
reaggregated for every validation page. The plan is disposable: interruption
discards it, and restarting reconstructs it from the sealed source before
resuming durable checkpoints. It adds no database relation or restart cleanup
protocol. Full READY audit still derives decisions independently from source
SQL; a retained plan relies on legal writers preserving sealed observations.
It does not promise per-commit detection of unmanaged SQL edits to those facts.
Ancestor key discovery uses bounded physical primary-key pages instead of
sorting the entire resolved view. Its safe candidate superset can include an
ancestral key already removed by a tombstone; such a key contributes no live
source count. Scalar resolution joins a bounded grid of requested hashes and
at most 17 ancestry layers to the physical primary keys; it does not rescan the
resolved view for each page. The connector owns backend-specific access paths,
and commit rechecks the current candidate prefix before comparing values.

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
schema-fixed bounded set of physical deletes. Consecutive empty phases share
one fenced transaction, preserving their individual durable receipts, and the
transaction ends after the first nonempty deletion batch. Exclusive gate holder
changes use bounded multi-row mutations. One public attempt advances at most
16 cleanup transactions; it renews the lease when needed instead of at every
empty phase. The typed result is `DONE`,
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
`mariadb_smoke and mariadb and not deep` against pinned MariaDB 10.11.11. Docker
is therefore required for the release check's MariaDB smoke phase. The deadline
includes termination and reaping of the owned pytest/xdist process tree and the
handoff between phases. POSIX uses a new session/process group. Windows assigns
a start-gated supervisor to a kill-on-close Job Object before pytest can start;
`taskkill /T` is only a bounded fallback when Job termination fails. Every phase
gets a fresh owner, and the next phase cannot start until the previous phase has
an empty-tree receipt. A test that deliberately creates a detached POSIX
operating-system session is outside that platform's process-group guarantee.
Testcontainers/Ryuk cleanup inside the Docker daemon can finish after the runner
exits and is not part of the deadline or owned process-tree evidence.

An independent `windows-latest` target exercises the real Job Object, timeout,
console-break, forced-parent-exit, descendant, virtual-environment redirector,
and multi-phase handoff behavior without starting MariaDB. The finite
`PytestProcessSupervision` model checks one-, two-, and three-phase profiles and
the fail-closed result rules. Neither target claims that terminating pytest also
removes Docker containers or volumes.

Plain `pytest` defaults to `not deep` with automatic bounded xdist workers and
does not enable the live service, but that direct command has no aggregate
wall-clock deadline. Use `scripts/run-pytest.py merge` when the five-minute
ceiling must be enforced.
Run `scripts/check-pytest-deep.sh` explicitly for the full non-MariaDB suite
followed by the full live-MariaDB suite. That manual profile has no default
timeout and requires Docker. Deep matrix results are not part of the
exact-tree release receipt and must not be reported as though every merge ran
them.

### Opt-in deployment acceptance

`scripts/check-deployment-acceptance.py` tests the supplied deployment's actual
ingest and OPDS Compose commands, dependencies, read-only mounts, wrappers, and
healthchecks against disposable MariaDB 10.11.11. It reads only the explicit
Compose file and public wrappers; it does not load deployment env files,
credentials, configurations, or media. Supply already-built local role images
from that deployment Dockerfile and a Python environment containing the tested
ingest/core cohort and Pillow for synthetic fixture generation.

The host must support POSIX descriptor-relative file access. Evidence readers
reject symbolic links and special files before exporting container-written data:

```bash
.venv/bin/python scripts/check-deployment-acceptance.py \
  --deployment-root /path/to/deployment \
  --fixture-python /path/to/cohort/.venv/bin/python \
  --context desktop-linux \
  --ingest-image local/acceptance-ingest:tested \
  --opds-image local/acceptance-opds:tested \
  --mariadb-image mariadb:10.11.11 \
  --output /tmp/h2hdb-acceptance-small \
  --base-count 2 --append-count 2 --pages 2 --http-artifacts
```

The script pins local image identities, replaces production resources with
unique labeled resources and synthetic reader/writer accounts, and publishes no
host ports. The fixture uses deterministic unique images and writes
`galleryinfo.txt` last. The independent oracle verifies exact catalog membership,
page identity and raster content, archive bytes, and unchanged file identity.
Restart, append, and optional marker lifecycle scenarios require completed work
and the expected catalog; a replayed COMPLETE receipt does not prove new analysis.
This diagnostic harness explicitly enables DEBUG in its disposable configuration
to verify completed work and non-replayed analysis from the measured diagnostic
records. It does not parse human INFO sentences as a machine protocol or change
the production INFO default. Its timings include diagnostic logging overhead;
use a separate INFO run to assess ordinary operational output.
`--http-artifacts` also downloads the first and last GID CBZs through OPDS after
each scenario, checking search identity, byte size, SHA-256, and Range responses.
Only HTTP 503 with the exact `library_activating` JSON contract, `Retry-After: 1`
and `Cache-Control: no-store` permits a repeated request. Every such response and
wait remains in the result, including deadline failures; the request retains its
original revision and Range. Search, full download, Range verification and waits
share a 45-second probe budget inside the existing 60-second command timeout.
The command timeout bounds blocking I/O; the probe checks its budget at I/O
boundaries. Unknown 503 responses, other errors and incorrect bytes fail without
retrying the scenario.

The default 2+2 galleries provide a short development loop for repeated startup
audits and per-record SQL/journal work. Dedicated fixtures test shared-image
selection and 16/17-value, 128/129-row and byte-budget boundaries. After a group
of optimizations, run increasing `--base-count` values sequentially with the same
page profile to measure growth; small-run timings alone do not establish scaling.
For three additional equal append rounds in the same resident, add
`--growth-batches 3 --base-count 2 --append-count 2 --pages 129 --image-profile small`.
The original fresh, restart and append scenarios still run first; the growth
rounds then reach 6, 8 and 10 galleries. Each round prepares its complete source
collection outside the visible source and exposes it with one directory rename.
Reports retain actual resident generation counts, completed phase durations,
core stage SQL costs and independent source/catalog/CBZ verification. They require
non-replayed validation beyond 128 keys and complete source, analysis and
publication evidence; they do not require validation to revisit every current
hash. An input round with multiple publications is reported as multiple batches.
`--growth-batches` defaults to zero, accepts at most three and requires more than
128 new image pages per round. It adds no work to ordinary pytest or merge runs.
The dev-only fixture manifest is now schema 2 to record collection paths; old
synthetic fixtures must be regenerated. Production data and CBZ formats are
unaffected. Phase totals contain core stages and must not be added to their
nested stage totals; process-wide probe counters remain separately cumulative.
`--instrumented` adds test-only startup, SQL, render, and explicit
Python fsync observations. Run it separately from the uninstrumented baseline;
its observer cost is not free. `--faults --instrumented` additionally interrupts
a real durable library installation with SIGTERM and SIGKILL, checks OPDS fencing,
and verifies restart recovery. These tests do not simulate machine power loss.
They remain opt-in and are never launched by ordinary pytest or the merge gate.

The output contains command logs, pinned cohort/platform metadata, scenario and
oracle results, and a label-scoped Docker cleanup receipt. Consumers stop before
the database; their final log tails are saved and checked before resource removal.
Synthetic fixtures are removed after verified Docker cleanup unless
`--keep-fixtures` is supplied;
failed cleanup preserves their paths for diagnosis. Successful functional checks
are distinct from acceptable latency: local Docker timings do not establish a NAS
SLO or coverage of a hundred-thousand-gallery corpus. Test helpers themselves have
offline unit tests; set `H2HDB_ACCEPTANCE_PYTHON` to the explicit cohort interpreter
when running `tests/test_deployment_acceptance_fixture.py` to also exercise its
real SQLite ingest and byte oracle.

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
