# AGENTS.md

Guidance for coding agents working in this repository.

## Project Overview

H2HDB is a MariaDB- or SQLite-backed database/cataloguer for H@H
(Hentai@Home) comic collections. It scans a download folder for gallery
directories containing `galleryinfo.txt`, records gallery metadata and file
hashes in a database, and can compress galleries into CBZ files.

The main entry point is:

```bash
uv run python -m h2hdb --config [json-path]
```

Python must be run through `uv run` so commands use the project virtual
environment and dependency versions. The Python version requirement is defined
by `requires-python` in `pyproject.toml`.

## Common Commands

```bash
uv pip install -e ".[dev]"
uv run ruff check src/h2hdb
uv run black src/h2hdb
uv run mypy src/h2hdb
uv run pymarkdownlnt fix .
uv run pytest
```

If the virtual environment breaks after a Python upgrade or similar toolchain
change, rebuild it with:

```bash
./scripts/rebuild-env.sh
```

## Testing

The test suite is under `tests/` and uses pytest.

- Most tests are parametrized over both `mariadb` and `sqlite` through the
  `db_config` fixture in `tests/conftest.py`.
- SQLite tests use a temporary file, not `:memory:`, because H2HDB methods open
  independent connections.
- MariaDB tests use `testcontainers` and require a running Docker daemon.
- `tests/test_sqlite_connector.py` covers the SQLite connection layer directly.

Run targeted tests when working on a narrow change, but run `uv run pytest`
before finishing changes that affect shared behavior, schema logic, SQL
connectors, or ingest flows.

## Architecture Notes

This project is pre-1.0. Architecture descriptions document the current design,
not a permanent contract. If a change intentionally replaces one of these
patterns, update the affected docs in the same change.

`H2HDB` in `src/h2hdb/h2hdb_h2hdb.py` is the public API and acts as a facade
over focused repository objects. Table and view concerns live in
`table_*.py`/`view_*.py` modules. Shared repository dependencies and SQL helper
methods live in `src/h2hdb/repository.py`.

Cross-table dependencies should be explicit constructor arguments, not inherited
through a mixin chain. When adding new gallery metadata, create a focused
`table_*.py` repository, add its table creation method, implement insert/get
behavior, instantiate it in `H2HDB.__init__()`, and register it in
`create_main_tables()`.

## SQL Rules

`src/h2hdb/sql_connector.py` defines the connector interface and shared
exception types. The concrete connectors are:

- `src/h2hdb/mariadb_connector.py`
- `src/h2hdb/sqlite_connector.py`

Write ordinary queries once where possible, using `%s` placeholders. The SQLite
connector translates `%s` to `?` internally. Backend dispatch with
`match self.config.database.sql_type.lower()` should be limited to places where
SQL truly differs, such as DDL, date arithmetic, optimization commands,
collation checks, name-column generation, and SQLite FTS5 support.

Long gallery or file names must use the name-column helpers in
`BaseRepository`. MariaDB splits indexed names across fixed-width columns to
respect InnoDB prefix limits; SQLite stores the same logical name in one `TEXT`
column. Do not introduce a direct single-column indexed long-name key that
bypasses those helpers.

SQLite has no native `FULLTEXT` index. MariaDB full-text tables should be
mirrored with SQLite FTS5 virtual tables and sync triggers using the existing
`BaseRepository._create_sqlite_fts5_sync` pattern.

## Configuration

Configuration is defined in `src/h2hdb/config_loader.py` using pydantic models
with `extra="forbid"`. Validated enum values live in `src/h2hdb/settings.py`.

Database backends are currently `mariadb` and `sqlite`. For MariaDB, the
database fields identify the server and database name. For SQLite, the
`database` field is the database file path.

## Concurrency and CBZ Handling

Gallery metadata is inserted with batched SQL. File-byte hashing uses a bounded
`ThreadPoolExecutor` in `src/h2hdb/table_files_dbids.py`;
`h2h.file_hash_workers` controls the worker limit. Hash workers only read files
and compute digests; hash catalog lookups and gallery catalog writes stay on
the main thread. The only background database writer is the resident loop's
ingest-lease heartbeat; it uses independent short connections and only updates
the coordination singleton.

The main entry point is resident and keeps a 30-minute periodic synchronization
deadline, but its wait is split into five-second coordination polls. The
singleton `gallery_ingest_state` row serializes h2hdb-downloader deep-download
turns with database-ingest turns through `INGEST_REQUESTED`, `INGESTING`,
`READY`, and `DOWNLOADING`. A newly seeded row starts at `INGEST_REQUESTED`, so
the catalog completes one baseline scan before a downloader can claim a turn.

`claim_download_turn()` increments `generation` and returns an owner-token
fenced `DownloadTurn`; only that live token can renew its lease or idempotently
call `request_gallery_ingest()`. `ensure_download_request()` atomically creates
a request only when absent and otherwise preserves the current token, allowing a
related-gallery download to reuse work without fencing out a queued root. One
turn may cover a single independent root or a batch of complete root traversals
governed by the downloader's accepted-submission soft threshold. Each root and
its full related-tag cascade remain indivisible; the downloader counts unique
GIDs for which H@H accepted a submission, checks the threshold only after the
root returns, and does not advance it for a zero-submission root. Between batch
roots,
`complete_download_request_in_turn()` exact-deletes a successful request only
while that live turn still owns `DOWNLOADING`;
`complete_missing_download_request_in_turn()` applies the same turn and request
token fences before atomically adding the removed marker. A soft-threshold or
snapshot-exhaustion boundary calls `request_gallery_ingest()` once. Single-root
callers instead use
`finish_download_turn()` or `finish_missing_download_turn()` so their request
mutation and explicit handoff share one transaction. A direct missing lookup
uses `complete_missing_download_request()` with the same request-token fence; a
later successful lookup clears the stale marker with
`clear_removed_gallery_gid()`, and replaying an older completion cannot restore
it. Once any handoff for a turn is already committed, later finish calls are
idempotent no-ops and must not perform success or missing mutations. Downloader
callers wait for `completed_generation >= turn.generation` before claiming the
next batch or independent root.
The main loop atomically claims requested ingestion, an expired downloader or
ingester lease, or a due periodic scan. A fresh `DOWNLOADING` lease always
blocks a periodic scan. It acknowledges a generation and returns the row to
`READY` only after `synchronize_once()` converges to
`needs_immediate_rescan == False` and scheduled maintenance succeeds. Failed
synchronization or maintenance leaves `INGESTING` for lease-based recovery. A
60-second background heartbeat renews the 300-second ingest lease throughout
synchronization and, for MariaDB, scheduled maintenance. SQLite `BUSY` and
`LOCKED` heartbeat errors are retried only within a conservative monotonic
lease deadline; token, phase, and database-clock expiry failures remain
terminal. Immediately before SQLite maintenance, h2hdb renews synchronously and
stops the heartbeat, allowing `VACUUM`'s exclusive database lock to fence all
coordination writers. If SQLite actually optimized, completion may tolerate an
expired timestamp only while the same generation and owner token remain; a
replacement claimant wins the row-lock race and the old completion fails.
Another resident that encounters SQLite `BUSY` or `LOCKED` while claiming
treats the turn as temporarily unavailable and continues its five-second poll;
errors after a claim are not swallowed. Other heartbeat failures or expired
leases prevent acknowledgement.

Explicit handoff provenance is persisted as the downloader generation and
owner token through `INGEST_REQUESTED`, `INGESTING`, and the following `READY`
state; the next download claim clears it. This makes handoff retries
idempotent, while a downloader recovered only because its lease expired cannot
later claim that it explicitly handed off.

Coordination transitions use the same managed-transaction pattern as other
cross-table operations: MariaDB locks the singleton row with
`SELECT ... FOR UPDATE`, while SQLite serializes writers with
`BEGIN IMMEDIATE`. Lease timestamps come from the database clock and are
computed only after the row lock is acquired. `create_main_tables()` creates
and seeds this additive singleton idempotently.

Completed removal and changed-gallery batches add work to the singleton
`database_maintenance_state` row; new galleries do not count. Automatic
optimization requires the configured accumulated-work and minimum-interval
thresholds, then filters
MariaDB base tables by both absolute `DATA_FREE` bytes and free-space ratio;
SQLite uses freelist pages for the same space test. An evaluation timestamp is
persisted before inspection/DDL so a failed or unnecessary attempt is not
retried every loop. Successful optimization subtracts the evaluated work
snapshot rather than zeroing the counter, preserving increments from a
concurrent scan. This additive singleton schema is created and seeded
idempotently by `create_main_tables()`; it requires no separate migration.

MariaDB maintenance and cooperating clients use
`H2HDB.database_gate(timeout_seconds=...)`, backed by a database-specific
server-wide named lock. The lock-holder connection stays open for the full
context; SQL inside the context may use the repositories' ordinary short
connections. A wait timeout is a logging interval and retries indefinitely,
while a named-lock error still raises. Keep network requests, downloads, and
sleeps outside this gate. SQLite currently treats the application gate as a
no-op and relies on its own database locking.

CBZ compression uses the shared `multiprocessing.Pool` owned by
`synchronize_once` in `src/h2hdb/h2hdb_h2hdb.py`. With CBZ output enabled,
progress chunks scale as `POOL_CPU_LIMIT * 16`, clamped to 64–500 galleries.
After each chunk's metadata insert, the provisional CBZ pass considers only
galleries whose database rows are new, creates their missing CBZ files, and
preserves any existing CBZ files. Changed galleries and existing output wait
for the authoritative final exclusion set.

After all insertion chunks finish, the pipeline freezes one final exclusion set
and runs one stable global deduplication reconciliation across all current
galleries. That reconciliation exact-syncs content ownership and duplicate
warnings, deletes loser CBZ files, and repairs final
winners against the final exclusions. This final pass is the only pass that
rebuilds existing CBZ files. A still-valid incumbent wins an exact priority tie
for the same hash; otherwise the database gallery ID is the deterministic
tie-breaker.

Gallery change detection stores a cheap SHA-256 source-layout token in
`gallery_source_manifests`, so a source-file add, delete, or rename marks a
gallery as changed without rereading image contents. Before a changed gallery's
database rows are refreshed, its name is stored in `pending_cbz_rebuilds`. This
state survives runs with CBZ output disabled and process interruption. The
stable final CBZ pass forces pending galleries to rebuild and clears their
entries after successful reconciliation; final deduplication losers are
cleared after their CBZ files are removed.

Each created or rebuilt CBZ also stores a versioned input-layout manifest in
the ZIP archive comment. It is derived from raw source filenames and their
catalogued SHA-256 values. Final reconciliation compares this marker as well as
member names, so rebuilding the database does not lose the ability to detect a
normalized rename or filename swap. A missing or mismatched marker causes one
conservative rebuild. This marker is not CBZ integrity verification: H2HDB does
not hash CBZ bytes, read member contents for scrubbing, or maintain an
integrity baseline.

Transient ownership and CBZ state during a run may be inconsistent, but a
successful `main`/`synchronize_once` return must converge to the final
snapshot. Do not weaken that property by making reconciliation chunk-local.
The exact-tie result is stable relative to prior ownership, not canonical
across different prior histories.

`compress_gallery_to_cbz.py` is imported lazily inside the method that needs it
to avoid a hard Pillow import at package import time. Preserve that lazy-import
behavior when touching compression code.

## Gallery Deletion and Download Requests

`todelete_gallery_candidates` derives live database gallery IDs only from
explicit `todelete_gids`, older folders with the same GID, and
`duplicate_hash_in_gallery`. Each successful `synchronize_once()` exact-syncs
that view into `todelete_galleries`, which backs `todelete_rm_commands`.
Full-content equality across different GIDs is not a raw-folder deletion
reason.

Publishing a deletion candidate must not enqueue a download. Only a later scan
that confirms the folder is absent may delete the gallery row. A GID is
enqueued atomically only after all of its active deletion candidates are gone,
so candidates removed across separate administrator runs still produce one
request. `pending_gallery_removals` also journals interrupted metadata writes,
so a pending folder that still exists must be recovered without enqueueing.

`todownload_gids` is the durable request queue, not an in-flight table. Requests
carry a UUID token. Re-enqueueing a GID replaces the token, and completion
deletes only a matching `(gid, request_token)` so an older worker cannot clear a
newer request. Failed and cancelled downloads leave the row intact.

One coordination download turn may cover one independent root or a batch of
root requests governed by the downloader's accepted-submission soft threshold,
but each root's complete deep traversal remains indivisible. The threshold
counts unique H@H-accepted submissions, is checked after each root returns, and
therefore may be exceeded by the final root by any amount; a root that produces
no accepted submission does not advance it. Failed, cancelled, and interrupted
traversals
keep their root row retryable and may explicitly request ingest for any complete
published files.
Successful intermediate batch roots use
`complete_download_request_in_turn()`; confirmed-missing roots use
`complete_missing_download_request_in_turn()`. Both first fence the live turn,
then conditionally mutate only the exact request token while retaining
`DOWNLOADING` for the next root. A newer token makes those request and missing
mutations no-ops. At the soft submission boundary, snapshot exhaustion, or a
controlled stop, the downloader hands off once. If it is terminated before that
handoff,
lease expiry lets h2hdb recover and scan; already completed root mutations are
durable, while the unfinished root remains queued. Single-root callers retain
the atomic `finish_download_turn()` and `finish_missing_download_turn()`
handoffs. A finish call made after the same turn already used the generic
failure handoff performs no request or removed-marker mutations.

Cross-table enqueue/delete operations use `with connector.transaction():`.
MariaDB suppresses per-statement auto-commit inside that block; SQLite uses
`BEGIN IMMEDIATE` and enables foreign keys on every connection. Do not open a
second repository connection inside a managed transaction.

## Tooling and Style

Follow SOLID principles and the existing local patterns. Keep changes scoped to
the feature or bug being addressed.

The IDE save pipeline and Claude Stop-hook pipeline are intentionally kept in
sync. If changing Python formatting, linting, type-checking, Markdown
formatting, or tool versions, update all relevant locations together:

- `.vscode/settings.json`
- `mypy.ini`
- `[tool.ruff.lint]` in `pyproject.toml`
- `.claude/hooks/finalize-python.sh`
- `.claude/hooks/finalize-markdown.sh`
- `.claude/settings.local.json`
- the `dev` dependencies in `pyproject.toml`

Tool versions should be changed in `pyproject.toml`, not through system-wide
installs.

Ruff `E2xx` whitespace rules are preview-only for the configured Ruff version.
Do not assume the CLI or hook will report every whitespace issue an IDE
extension might flag separately.

## Documentation Sync

`CLAUDE.md` is the source document this file was derived from. Keep both files
consistent when changing project workflow, architecture patterns, testing
expectations, or tooling behavior. Routine use of an already documented pattern
does not require a docs update; replacing the pattern itself does.
