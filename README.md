# h2hdb

`h2hdb` stores the shared catalog and download coordination data for an H2HDB
library. It supports SQLite and MariaDB and provides commands to initialize and
check the database.

Use this package when administering the database or reading the catalog from
Python. To import galleries, browse books, or run downloads, choose the
application for that task:

| Task | Application |
| --- | --- |
| Import downloaded galleries and prepare a reading library | [h2hdb-ingest](https://github.com/Kuan-Lun/h2hdb-ingest) |
| Browse and download books with an OPDS reader | [h2hdb-opds](https://github.com/Kuan-Lun/h2hdb-opds) |
| Synchronize the published library with Komga | [h2hdb-komga](https://github.com/Kuan-Lun/h2hdb-komga) |
| Process gallery download requests | [h2hdb-downloader](https://github.com/Kuan-Lun/h2hdb-downloader) |

The applications share one database. Installing Core alone does not import
files, produce CBZs, or start a web server.

## Install

Requires Python 3.14 or later. SQLite support is included. For a MariaDB
installation, the supported baseline is MariaDB 10.11.11, including Synology
package build 10.11.11-1551.

Install into a virtual environment:

```bash
python3.14 -m venv .venv
source .venv/bin/activate
python -m pip install h2hdb
python -m h2hdb --help
```

On Windows, create the environment with `py -3.14 -m venv .venv` and activate
it with `.venv\Scripts\Activate.ps1` in PowerShell. Run subsequent `python`
commands in that environment.

If another H2HDB application manages your environment, use the Core version
installed with that application. When upgrading a shared deployment, select
application versions that support the same database schema.

## Set up a database

### SQLite

Save this as `core-writer.json`:

```json
{
  "database": {
    "sql_type": "sqlite",
    "database": "catalog.sqlite3",
    "access_mode": "read-write"
  },
  "logger": {
    "level": "INFO",
    "file": null
  }
}
```

This example creates `catalog.sqlite3` in the current working directory. For a
service, use an absolute path in a persistent directory. Create its parent
directory first and ensure the process can write there.

Initialize the empty database, then verify it:

```bash
python -m h2hdb migrate --config core-writer.json
python -m h2hdb check --config core-writer.json
```

A successful initialization reports `outcome=created` and `state=READY`. The
database is ready for ingest; it does not yet contain a published library.
Follow the ingest application's setup instructions to import your galleries.

### MariaDB

Create an empty database and a dedicated writer account on your MariaDB server.
The account used for initialization needs permission to create the schema's
tables, views, and indexes, as well as read and write its data. Save the
connection settings as `core-writer.json`:

```json
{
  "database": {
    "sql_type": "mariadb",
    "host": "127.0.0.1",
    "port": 3306,
    "user": "h2hdb_writer",
    "password": "${H2HDB_DB_PASSWORD}",
    "database": "h2hdb",
    "access_mode": "read-write"
  },
  "logger": {
    "level": "INFO",
    "file": null
  }
}
```

Provide `H2HDB_DB_PASSWORD` through the environment of the process running the
command, then run the same `migrate` and `check` commands as for SQLite.
Initialization creates tables inside the named database; it does not provision
MariaDB, create the database itself, or create accounts.

### Reader configuration and secrets

For catalog readers, create `core-reader.json` with the same database location
and `"access_mode": "read-only"`. On MariaDB, also use a dedicated reader account
with the read and metadata privileges required to inspect the schema. Keep the
writer credentials in ingest, downloader, and administration environments.

A JSON string equal to `${ENV_NAME}` is replaced with that environment
variable's value. Missing variables stop startup. Embedded placeholders such as
`"library-${INSTANCE}"` are unsupported. Unknown configuration fields are
rejected, so check spelling when a file fails validation.

Core configuration has `database`, optional `maintenance`, and optional `logger`
sections. Application configuration files may wrap Core settings in another
section; use the relevant application's example for those commands.

## Check and maintain the installation

| Command | When to use it | What success means |
| --- | --- | --- |
| `python -m h2hdb migrate --config core-writer.json` | Initialize an empty database or resume an interrupted initialization | The database was created or resumed, or its existing READY marker was accepted |
| `python -m h2hdb check --config core-reader.json` | Run a full database audit | The current schema and stored catalog/coordination facts passed validation |
| `python -m h2hdb ready --config core-reader.json` | Run a frequent readiness probe | The database has the expected READY marker, schema version, and manifest |

`ready` is a quick, read-only check. It does not audit all stored data or verify
that external media files are present. `check` performs the full database audit
and may take substantially longer on a large library; it does not decode CBZs
or images.

Rerunning `migrate` on an already initialized database reports
`outcome=already_ready` and `audit=not_performed`. Use `check` when you need a full
audit. If initialization was interrupted, rerun `migrate` with the same software
and configuration; it resumes only a matching unfinished initialization.

The ingest application manages its own startup and periodic audit schedule.
After an unclean stop, a validator change, or a due audit, startup may require a
full check. A quick restart result is not evidence of a new full audit.

Logs go to the console by default. Set `logger.file` to save them to a file and
`logger.level` to `"DEBUG"` for more detailed timing and query statistics. During
a long full audit, progress records identify the active phase. Timing records
help diagnose a delay; they do not establish that the audit has finished.

INFO diagnostics cover source preparation and action costs, ingest claims,
cleanup candidate selection and audit scans. Long-operation progress includes a
pending connector call's category, fingerprint and age; completed counters do
not include that call yet. Bounded slow-call samples retain expensive queries
even after the detailed query-statistics capacity is reached. No query parameters
or source payloads are logged. Phase totals expose repeated small costs that a
list of the slowest individual phases can miss.

The initial `source_prepare` record reports inventory size with
`observation_complete=false`; it does not yet know admitted file/gallery counts.
The terminal `source_step` INFO record reports admitted files and galleries,
discovered/staged galleries, deferred/waiting counts, and completed inventory
observation. Its correlation ID matches the source preparation and stage summary.

INFO also reports cumulative time for the first 64 SQL query families and an
explicit overflow total. Pure placeholder `IN` lists share one family regardless
of their parameter count; quoted text, expressions and subqueries remain distinct.
The `query_fingerprint_algorithm` label identifies this diagnostic normalization;
execution SQL and parameters are unchanged. This can expose thousands of
individually fast calls; it is not a complete ranking of every query shape.
Transaction timings distinguish
`begin`, `begin_read`, `commit` and `rollback`, including failed calls. They measure
client elapsed time, not database lock waits or filesystem flush time separately.

Publication INFO summaries also attribute artifact input audits, source copying
and revalidation, rendering, output verification and storage protection. Each
operation reports completed calls, failures, inclusive/exclusive wall time and
actual logical read/write bytes; cache lookup includes hits, misses and
invalidations. Exclusive time subtracts nested local-work spans, but these spans
can still contain SQL and adapter measurements. Do not add the different layers
or interpret logical transfers as physical disk I/O. An unfinished step has no
completed local-work summary yet; existing progress records identify its active
operation. Ingest's adapter and image-worker summaries provide the filesystem
and codec details that Core intentionally does not interpret.

`sql_calls` counts connector method calls, not server statements or network
round trips. `read_rows` counts returned rows, not examined rows. Client SQL time
includes driver, transport, execution and waits. Use the manual cost probes to
measure backend work across input sizes; a fixed page size or SQL-call count is
not evidence of bounded database scanning.

Current-only cleanup tests each canonical reverse reference with its own indexed
equality and preserves the full live-publication checks. A no-work selection
classifies DONE or BLOCKED in the same exclusive transaction; it avoids a second
candidate scan only while that exact lease remains live. Final release still
checks the lease, and the next ingest claim performs a fresh check. Reaching the
cleanup batch budget still requires the final full state check.

Role audit pagination includes equivalent tuple and expanded seek predicates in
one query so SQLite and MariaDB can use their composite indexes. Manual role and
cleanup probes compare actual engine work with historical or deliberately slow
queries while requiring identical results. The MariaDB cleanup diagnostic also
compares three alternating executions of the current canonical candidate query
and the historical test predicate. Its title-cache visit target applies to the
probe's unique-title, single-policy fixture; it is not an arbitrary-data or NAS
latency guarantee.

For changes to analysis ancestry validation or unchanged artifact descriptors,
run `python scripts/run-pytest.py performance-acceptance` explicitly with Docker
available. It runs serial SQLite and MariaDB 10.11.11 experiments, including deep
cases; it is separate from the bounded merge receipt. Do not run other tests or
benchmarks concurrently when interpreting elapsed times. The matrix includes
127/128/129 source-page boundaries across 19 actual publication/cleanup rounds,
ancestry depth through compaction, descriptor copy boundaries through 4096 pages,
and fixed additions of 100 galleries to different retained catalogs. Public-path
experiments exercise the facade's issue/prepare/commit orchestration and verify
publication, cleanup and READY; writer-only inputs are reported separately.

Historical implementations serve as negative controls for operation-count
regressions. JSON reports under pytest's temporary directory retain alternating
baseline/candidate elapsed samples, exact-output checks and source hashes. An
elapsed-time improvement is an experimental result, not a timing assertion in
the merge gate. This profile does not exercise source filesystem bytes, real
image encoding, OPDS HTTP reads or deployment mounts: use the ingest source
snapshot probes and the separate instrumented deployment acceptance for those.
Passing one phase or a small correctness case does not complete this matrix or
establish NAS throughput.

The development catch-up probe compares publication batch sizes on disposable
databases while forbidding deep reads of unchanged source markers:

```bash
.venv/bin/python scripts/ingest_batch_scaling_probe.py \
  --case 256:64:64 --case 256:256:64 --output /tmp/catchup-sqlite.json
```

Cases are `galleries:publication_batch:pages_per_gallery`. Each turn checks the
public catalog and cleanup `DONE`; each case ends with a full READY audit. Use
`--backend mariadb` explicitly for a local MariaDB 10.11.11 testcontainer, and
`--artifacts` for neutral in-memory artifacts. These synthetic pages do not
exercise image decoding or real archive I/O. The separate development-only
`ingest_locator_reuse_probe.py --case 64:8:4 --output /tmp/locator-reuse.json`
compares a scoped locator-reuse counterfactual with the actual implementation,
restoring the original methods afterward. It is not installed runtime behavior.
Query-count savings and small local timings do not establish a full-library
completion target; input size, page distribution, duplicate patterns, retained
history and storage latency all matter.

Changed-file-hash analysis traverses the sealed changed-gallery set and its
accepted current/baseline hash occurrences once per local preparation. It sorts
and deduplicates them in a disk plan outside write transactions, then commits
authenticated pages of at most 128 hashes. Restart rebuilds that disposable plan
from database facts at the durable cursor; it does not hash source images.
The isolated `scripts/analysis_changed_hash_probe.py` exercises dense changes and
sparse changes among unrelated galleries, and records SQLite VM work or MariaDB
handler counters and query plans. Its source-call totals cover changed-gallery
member and occurrence reads, excluding the earlier change-detection stage and
later commits. These query fixtures do not perform a full READY audit or measure
whole-ingest throughput.

`VNextIngestFacade.prepare_source()` accepts `max_new_galleries=None` to admit
all complete galleries before global analysis and publication. This changes
publication frequency, not the bounded size of database or adapter operations.
The caller owns any temporary source-byte storage and must account for its
capacity independently of this admission limit.

An exact fresh completion marker and the current qualification policy now allow
reuse of any retained, sealed source observation, including an unpublished one
after restart. This replaces the previous artifact-enabled rule that deeply
observed every unpublished gallery again. Reuse preserves immutable hashes and
qualification evidence; it does not prove that live files remain available.
Artifact preparation still verifies the bytes it actually reads against the
sealed observation. Source adapters must honor their completion-marker contract.

After an artifact-source failure, callers can pass
`reobserve_gallery_locators=(locator_a, locator_b)` to bypass reuse for up to 128
distinct exact locators. Accumulate unresolved failed locators across retries so
alternating failures do not reintroduce an earlier stale observation. These hints
only request fresh observation and qualification; they cannot insert inventory
members, bypass the new-gallery budget, or authorize publication. To refresh
every admitted gallery, pass `reuse_sealed_observations=False` with an empty hint
tuple. A resident may use that explicit fallback when its bounded retry hints
overflow. A deferred gallery still uses only its currently published observation;
unpublished cache entries cannot replace that fallback.

`prepare_source_resume(adapter, policy=...)` requires the source adapter. It
checks an unpublished sealed working source cut, including its qualification and
policy authority, in read pages of at most 128 galleries. Outside the database
transaction, it rereads each existing gallery's completion marker and requires
an exact match. This is O(G + marker bytes) source work for G existing galleries;
it performs SQL and marker I/O, without enumerating newly arrived galleries or
deep-reading image files. Missing marker evidence, a marker mismatch, or a
deferred source returns `None` and requires ordinary fresh source preparation.
Other source or database errors still propagate. A successful check returns an
opaque preparation. `commit_source_resume(session, prepared)`
rechecks the current working cut and publication baseline in a short transaction
and maps the renewed ingest generation to that same build. Preparation belongs
outside the caller's heartbeat lock; only commit belongs inside it. A published
head is never returned as pending work.

This lets a consumer finish interrupted analysis and prepared artifacts before
inventorying newly arrived galleries. It does not recover the original deferred
or waiting counts; those were process-local inventory facts. The consumer must
request a fresh inventory after publication and must not report catch-up complete
from the resumed receipt alone. A source-byte failure or policy change requires
fresh observation instead. Dropping a failed gallery after global analysis could
change spam and duplicate selection for other galleries, so a changed cut can
still require a new analysis and rendering of candidate-owned artifacts.

`prepare_source()` prepares discovery and a lazy observation handle. Subsequent
issue/prepare/commit steps observe one gallery outside the heartbeat lock and
persist its canonical pages, qualification and sealed observation before
observing the next gallery. An observation collection owns these checkpoints
before the full source manifest exists. Source assembly then attaches those
exact observations and atomically consumes the collection when the cut seals.
It does not create intermediate analysis or publication batches.
Each ingest session binds one source root and collection policy. Changing the
root, manifest policy or qualification policy requires a new ingest session;
attempting to replace them within the same generation fails closed.

After an interrupted first scan, a fresh inventory rechecks completion markers
and reuses matching durable observations. Changed markers or qualification
policies require new checks. An unfinished gallery's old staging is retired in
bounded steps before re-observation; completed observations remain protected.
At most one gallery's unsealed deep checks are lost per interruption when the
adapter supplies stable completion markers. An adapter without such evidence
must observe its galleries again; a checkpoint alone cannot prove unchanged
external bytes. Neither recovery contract establishes a full-library time budget.

The prepared handle's `observation_complete` becomes true after the selected
inventory is complete. Its `gallery_count`, `deferred_gallery_count` and
`waiting_gallery_count` properties reject premature reads. This changes the
public preparation contract; callers must drive the source steps before reading
these counts. Existing source observations and artifact formats remain intact,
but schema 7 requires the offline conversion below.

A preparation error or cancellation permanently invalidates that prepared
handle; a terminal manifest mismatch does the same. Leave its context or close
it outside the session-renewal lock, then start a fresh scan. Its completed
durable gallery checkpoints remain eligible for reuse. Retrying an invalidated
handle cannot turn a partial inventory into a completed source cut.

In the normal two-step artifact preparation, the initial live-source pass fills
one gallery-local verified spool. Post-render source verification reads this
spool, not the live source. The subsequent protection step reopens and verifies
live sources when it reuses the prepared receipt. Consequently, a cache hit
normally still involves two live reads of each executable source member overall.
Receipts above the 256 MiB cache limit, restarts and retries can repeat rendering
or add reads; logical read counts do not reveal physical HDD reads.

## Upgrade or restore a database

This release uses **epoch 3, schema version 8**. `migrate` initializes this
schema; it does not automatically upgrade older databases. Back up the database
and its matching library storage before changing the deployed application set.

### Convert an exact schema-7 database

The one-time offline converter preserves the catalog, source observations,
download queue, and external media. It accepts the exact supported schema-7
database and runs with the corresponding schema-8 software. It adds observation
collection state and moves existing staging ownership into explicit bindings;
staging identities and their canonical pages are preserved.

1. Stop ingest, downloader, OPDS, Komga synchronization, and any other database
   clients.
2. Take a verifiable database backup. Retain the matching library storage.
3. Obtain the source checkout for the schema-8 Core release you are installing.
   The converter is a checkout script, not a `python -m h2hdb` subcommand.
4. From that checkout, use the matching Core Python environment and a read-write
   Core configuration to run:

   ```bash
   python scripts/upgrade-source-collection-schema.py \
     --config /path/to/core-writer.json --consumers-stopped
   ```

5. Resume compatible applications only after the conversion completes
   successfully.

`--consumers-stopped` acknowledges that you stopped the clients; it does not stop
them for you. The conversion includes a full database audit and can take as long
as `check`. It does not re-render CBZs. If interrupted, leave clients stopped and
rerun the same converter. Repeating a completed conversion performs a full audit
and reports `already_converted`.

Do not delete source folders, CBZs, or the existing database for this conversion.
To return to the old software, restore the pre-upgrade database backup first;
there is no automatic downgrade.

For schema 6, first use the `upgrade-audit-schema.py` script from the Core 0.40.0
checkout and its environment to reach schema 7, then use this converter. The old
6-to-7 entry point is not part of the schema-8 checkout, and normal runtime does
not fall back to older schemas. Keep all clients stopped throughout both
conversions and retain the original database/library backup.

### Other old or incompatible databases

Schema 5 and earlier have no supported in-place conversion. Preserve the old
database and original downloaded source galleries, create a separate empty
database, and use ingest to rebuild the catalog and a matching generated library.
Keep the old database/library pair available until the replacement is verified.
The rebuilt catalog does not automatically recover old download requests or
operational history; re-enter any requests you still need.

An unexpected schema mismatch can also mean an application is using the wrong
Core version or database path. Confirm those settings before deciding a rebuild
is necessary. Do not use `migrate` to repair a damaged database in place.

## Read the catalog from Python

For scripts that use the library directly, import the public API from `h2hdb`.
This example searches a library already published by ingest:

```python
from contextlib import closing

from h2hdb import CatalogDiscoveryQuery, load_config, open_database

with closing(open_database(load_config("core-reader.json"))) as catalog:
    revision = catalog.get_catalog_revision()
    page = catalog.discover_publications(
        query=CatalogDiscoveryQuery(search="example title"),
        limit=50,
        revision=revision,
    )
    for publication in page.publications:
        print(publication.publication_id, publication.title)
```

`open_database()` performs a full audit. Discovery pages contain at most 128
publications. To continue, pass the returned `page.next_cursor` as `after` with
the same query and revision; `None` means there is no next page. If ingest
publishes a newer revision during browsing, fetch the current revision and
restart pagination instead of reusing the old cursor.

The public entry points cover these tasks:

| Entry point | Purpose |
| --- | --- |
| `VNextDatabaseAdminFacade` | Initialize, audit, and probe the database |
| `VNextCatalogFacade` | Read publications, search results, facets, tags, recent books, and image/acquisition descriptions |
| `VNextDownloadQueueFacade` | Submit and manage download requests |
| `VNextIngestFacade` | Coordinate an ingest integration |

Search combines its words and supplied filters with AND. Language, contributor,
and subject filters match exact values. Upload/download time ranges include
the start and exclude the end; page-count ranges include both ends and use the
prepared artifact's page count. Catalog results describe the current publication
only, and image/acquisition descriptions must be resolved by the application's
storage adapter.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| Configuration fails before connecting | Check JSON syntax, field names, environment variables, and that this is a Core configuration file |
| SQLite cannot open or write the database | Check the absolute path, parent directory, and process permissions |
| MariaDB connection or permission error | Check server availability, database name, credentials, and account privileges |
| Database is not READY after interrupted setup | Resume `migrate` with the same version and configuration |
| Schema or manifest mismatch | Check the installed application versions and the upgrade instructions above |
| `ready` succeeds but the library is empty | Complete the first ingest publication; readiness alone does not populate the catalog |
| Full `check` fails | Retain the error and backups; investigate the mismatch before restoring or rebuilding |
| Python catalog revision or cursor becomes invalid | Reload the current revision and restart the query |

For support, include the installed package versions, database backend, command,
and redacted error in the [issue tracker](https://github.com/Kuan-Lun/h2hdb/issues).
Do not include credentials or an unredacted configuration file.

For optional local assessment, see the [verification guide](verification/README.md)
and [catalog benchmark guide](benchmarks/README.md). These tools are separate
from checking your own database.

## License

[GNU General Public License version 3](LICENSE).
