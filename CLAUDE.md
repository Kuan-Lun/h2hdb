# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository.

## What this is

H2HDB is a MariaDB- or SQLite-backed database/cataloguer for H@H
(Hentai@Home) comic collections. It scans a download folder for galleries
(each gallery = a folder containing a `galleryinfo.txt`), records metadata
(GID, title, tags, upload account, times, file hashes) into the database, and
optionally compresses each gallery into a CBZ file. Entry point:
`python -m h2hdb --config [json-path]` (see `src/h2hdb/__main__.py`).

Requires Python >= 3.14.

## Communication

- Claude 必須以繁體中文回答所有對話內容，不論使用者以何種語言提問；程式碼、指令、檔名、專有名詞等仍維持原文。

## Common commands

Environment is managed with `uv`.

```bash
uv pip install -e ".[dev]"
uv run ruff check src/h2hdb     # lint
uv run black src/h2hdb          # format
uv run mypy src/h2hdb           # type-check (strict mode, see mypy.ini)
uv run pymarkdownlnt fix .      # markdown autofix
uv run pytest                   # run the test suite (see Testing below)
```

Always run Python through `uv run` (e.g. `uv run python -m h2hdb ...`) so it
resolves to the project venv's interpreter and dependency versions.

A Claude Code Stop hook already runs this pipeline automatically after each
turn — see `.claude/hooks/finalize-python.sh` (black → ruff --fix → black →
mypy, scoped to `src/h2hdb`) and `.claude/hooks/finalize-markdown.sh`
(pymarkdown fix → ruff format --preview on embedded code blocks), registered
in `.claude/settings.local.json`. It mirrors the VS Code on-save pipeline in
`.vscode/settings.json`. Tool versions for both paths come from the single
`dev` extra in `pyproject.toml` — bump versions there, not via a system-wide
install.

If the venv breaks (e.g. after a Python version upgrade — mypyc extension
module errors), nuke and rebuild it with `./scripts/rebuild-env.sh`.

### Testing

`tests/` holds the test suite (pytest). Most tests are parametrized over both
backends via the `db_config` fixture in `tests/conftest.py`
(`test_xxx[mariadb]` / `test_xxx[sqlite]`):

- The `sqlite` param uses stdlib `sqlite3` against a temp file (never
  `:memory:` — every H2HDB method opens its own connection, and SQLite's
  in-memory databases are connection-scoped, so an in-memory DB would lose all
  data between calls).
- The `mariadb` param uses `testcontainers` to start a throwaway MariaDB
  container per test run — this needs a running Docker daemon, but otherwise
  requires no manual setup (no need to point it at a real instance).

`tests/test_sqlite_connector.py` covers the `SQLiteConnector` connection layer
directly (not parametrized, since it's backend-specific by definition).

## Architecture

This project is pre-1.0 and the sections below describe today's design, not a
contract to preserve. If a change intentionally replaces one of these patterns,
update or delete the stale part of this doc in the same change rather than
working around it.

### Repository-based class composition

`H2HDB` (`src/h2hdb/h2hdb_h2hdb.py`) is the public API and acts as a facade.
It owns focused repository objects for table and view concerns instead of
inheriting those concerns through a mixin chain. Shared dependencies live in
`RepositoryContext` (`src/h2hdb/repository.py`): validated config, logger, SQL
connector factory, SQL connection params, and the MariaDB index-prefix limit.
Repositories inherit only `BaseRepository`, which exposes those shared
dependencies and SQL helper methods.

Table and view repositories still live in `table_*.py`/`view_*.py` modules.
When a repository's tables and views form one cohesive domain concept (e.g.
`todelete_queue.py`, `duplicated_hashes.py`), the module drops the prefix
instead of being split across a matching `table_*.py`/`view_*.py` pair.
Cross-table dependencies must be constructor-injected explicitly; for example
most gallery metadata repositories receive the gallery ID repository because
their tables foreign-key into `galleries_dbids`. `H2HDB.__init__()` is the
authoritative wiring map. `create_main_tables()` is the authoritative schema
creation order.

Convention for adding a new piece of gallery metadata: create a focused
repository in a `table_*.py` module with a `_create_*_table` method plus
insert/get methods, inject any sibling repositories it needs, instantiate it in
`H2HDB.__init__()`, and register its table creation method in
`create_main_tables()`. Keep the facade's public method names stable where
practical, but avoid adding new behavior through inheritance.

### SQL abstraction

`sql_connector.py` defines an abstract `SQLConnector` interface
(connect/close/execute/fetch/begin/commit/rollback) plus a small exception
hierarchy for key/table/configuration errors. Two concrete implementations exist:
`mariadb_connector.py` (wraps `mysql-connector-python`, wire-protocol
compatible with MariaDB) and `sqlite_connector.py` (wraps stdlib `sqlite3`).
Every repository method opens a connector via
`with self.SQLConnector() as connector:` and writes its query as plain SQL with
`%s` placeholders (the canonical placeholder style across the whole codebase —
`SQLiteConnector` translates `%s` to `sqlite3`'s `?` internally; nothing else
needs to know about that difference).

Normal repository writes retain their existing per-statement commit behavior.
Workflows that must atomically coordinate multiple tables use
`with connector.transaction():`; MariaDB suppresses its per-statement
auto-commit inside that block, while SQLite uses `BEGIN IMMEDIATE`. SQLite
connections always enable foreign-key enforcement. Do not call repository
methods that open another connector from inside a managed transaction; use a
connector-accepting internal method instead.

Most query bodies are identical across both backends and are written once,
unconditionally. A `match self.config.database.sql_type.lower(): ...` dispatch
is only used where the two backends genuinely need different SQL: DDL
(`CREATE TABLE`/`CREATE VIEW`), anything that calls the name-column generators
below, and a small set of backend-specific statements (date arithmetic,
`OPTIMIZE TABLE` vs `VACUUM`, character-set/collation checks — search for
`case "sqlite":` to find them all). Adding a third backend means adding a third
`case` only at those sites, not everywhere `SQLConnector` is used.

### Long names vs MariaDB index limits

MariaDB's InnoDB engine limits indexed key prefixes to 191 bytes
(`RepositoryContext.mariadb_index_prefix_limit`). Gallery and file names can
exceed that, so on the MariaDB backend long names get split into multiple
fixed-width columns and the index is defined across all of them together — see
`_mariadb_split_name_based_on_limit` and friends in `repository.py`. SQLite has
no such limit, so the SQLite backend stores the same long name in a single
unsplit `TEXT` column (`BaseRepository.sqlite_name_columns`). Both generators
return the same `(column_names, ddl_fragment)` shape, so the surrounding
`WHERE`/`SELECT`/`INSERT` code that builds its query from `column_names` doesn't
need to know which backend it's running against, or how many columns the name
was split into. Any new table keyed by a long name must go through one of these
two generators — don't key it on a single unsplit `CHAR(255)`/`VARCHAR` column
directly.

SQLite also has no `FULLTEXT` index; tables that declare one on MariaDB get a
mirrored FTS5 virtual table + sync triggers on SQLite instead
(`BaseRepository._create_sqlite_fts5_sync`) — same searchable capability,
different mechanism.

### Configuration

`config_loader.py` defines the pydantic config model tree (`extra="forbid"`
throughout) and `load_config()`, which loads from a `--config` JSON path or
falls back to all defaults. Validated enum fields (CBZ grouping/sort, log
level) live in `settings.py`.

### Concurrency

Gallery metadata is written with batched SQL (`_insert_rows` and friends),
not per-gallery concurrent writes. File-byte hashing uses a bounded
`ThreadPoolExecutor` in `table_files_dbids.py`; `h2h.file_hash_workers`
controls the worker limit, and workers only read files and compute digests.
Hash catalog lookups and gallery catalog writes remain on the main thread. The
only background database writer is the resident loop's ingest-lease heartbeat;
it uses independent short connections and only updates the coordination
singleton.

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
turn may cover a single independent root or a bounded batch of complete root
traversals. Between batch roots,
`complete_download_request_in_turn()` exact-deletes a successful request only
while that live turn still owns `DOWNLOADING`;
`complete_missing_download_request_in_turn()` applies the same turn and request
token fences before atomically adding the removed marker. The final batch
boundary calls `request_gallery_ingest()` once. Single-root callers instead use
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

CPU-bound CBZ compression across *galleries* is dispatched to a shared
`multiprocessing.Pool` via `run_in_parallel` in `cbz_files.py`.
`synchronize_once` in `h2hdb_h2hdb.py` owns that pool's lifetime when
`cbz_path` is configured. `POOL_CPU_LIMIT`/`CPU_NUM` live in
`h2hdb_h2hdb.py` alongside it.

When CBZ output is enabled, the ingest pipeline uses CPU-scaled progress chunks:
`POOL_CPU_LIMIT * 16`, clamped to 64–500 galleries. It computes provisional
duplicate/spam exclusions at run start. After each chunk's metadata insert, the
provisional CBZ pass considers only galleries whose database rows are new,
creates their missing CBZ files, and preserves any existing CBZ files. Changed
galleries and existing output wait for the authoritative final exclusion set;
this prevents a fresh database from rebuilding valid output against an
incomplete provisional set.

After all insertion chunks finish, the pipeline freezes the final exclusion
set and performs one stable, global deduplication reconciliation across all
current galleries. It exact-syncs content ownership and duplicate warnings,
deletes loser CBZ files, and repairs final winners against
the final exclusions. This final pass is the only pass that rebuilds existing
CBZ files. For equal priority claims, a still-valid incumbent for that same hash
remains the owner; otherwise the database gallery ID is the deterministic
tie-breaker. Intermediate ownership and provisional CBZ output may be
inconsistent, but a successful `main`/`synchronize_once` return has converged
to the final snapshot. This result is stable relative to the prior owner used
for exact ties; it is not a history-independent canonical assignment.

### Gallery deletion and durable download requests

`todelete_gallery_candidates` computes live deletion candidates by database
gallery ID from explicit `todelete_gids`, older folders with the same GID, and
`duplicate_hash_in_gallery`. A successful `synchronize_once()` exact-syncs
those IDs into `todelete_galleries`; `todelete_rm_commands` is generated from
that active snapshot. Full-content equality across different GIDs is not a raw
folder deletion reason. It may still select one CBZ content owner.

Publishing a candidate does not enqueue a download. Administrators may execute
the removal command days later. Only a later filesystem scan that confirms the
folder is absent deletes its gallery row. Its GID is atomically enqueued only
after every active deletion candidate for that GID has disappeared, so
candidates removed in separate administrator runs still produce one request.
`pending_gallery_removals` remains an interruption journal for metadata writes;
present pending folders are recovered without creating download requests.

`todownload_gids` is a durable request table, not an in-flight log. Each row has
an immutable UUID request token exposed through `DownloadRequest`. Enqueuing an
existing GID replaces its token (while a blank URL preserves an existing
non-blank URL), and completion conditionally deletes by both GID and token.
This prevents an older worker from acknowledging a newer request. Failed or
cancelled downloads must leave their request row intact.

One coordination download turn may cover one independent root or a bounded
batch of root requests, but each root's complete deep traversal remains
indivisible. Failed, cancelled, and interrupted traversals keep their root row
retryable and may explicitly request ingest for any complete published files.
Successful intermediate batch roots use
`complete_download_request_in_turn()`; confirmed-missing roots use
`complete_missing_download_request_in_turn()`. Both first fence the live turn,
then conditionally mutate only the exact request token while retaining
`DOWNLOADING` for the next root. A newer token makes those request and missing
mutations no-ops. At the batch boundary, snapshot exhaustion, or a controlled
stop, the downloader hands off once. If it is terminated before that handoff,
lease expiry lets h2hdb recover and scan; already completed root mutations are
durable, while the unfinished root remains queued. Single-root callers retain
the atomic `finish_download_turn()` and `finish_missing_download_turn()`
handoffs. A finish call made after the same turn already used the generic
failure handoff performs no request or removed-marker mutations.

### CBZ compression

`compress_gallery_to_cbz.py` is imported lazily (inside the method that needs
it) to avoid a hard Pillow dependency at import time — keep that lazy-import
convention if you touch it. It resizes images and bundles them into a `.cbz`,
skipping images whose hash is in a duplicate/spam exclusion list computed from
a view defined in `h2hdb_h2hdb.py`.

Gallery change detection stores a cheap SHA-256 source-layout token in
`gallery_source_manifests`; it does not reread image contents to build that
token. A source-file add, delete, or rename therefore marks the gallery as
changed even when `galleryinfo.txt` and all image bytes are unchanged. Before a
changed gallery's database rows are refreshed, its name is stored in
`pending_cbz_rebuilds`. That pending state survives runs with CBZ output
disabled and process interruption. The stable final CBZ pass forces those
galleries to rebuild, then clears completed pending entries; entries for final
deduplication losers are cleared after their CBZ files are removed.

Every created or rebuilt CBZ stores a separate, versioned input-layout manifest
in the ZIP archive comment. The manifest is derived from raw source filenames
and the source SHA-256 values already catalogued during ingest, so it requires
no extra file reads. Final reconciliation compares both member names and this
marker. Consequently, deleting the database does not lose the information
needed to detect a normalized rename or filename swap after the database is
rebuilt. A missing, malformed, or mismatched marker triggers one conservative
rebuild. This is build-input metadata, not CBZ integrity verification: H2HDB
does not hash CBZ bytes, scrub member contents, or maintain a known-good CBZ
baseline.

## Keeping this file in sync

Routine use of an existing pattern needs no doc update — e.g. adding one more
`table_*.py` repository that follows the documented convention doesn't make the
Architecture section stale, since it already points at the code instead of
enumerating repositories. Update or delete the affected paragraph only when a
change replaces the *pattern itself* — e.g. repository composition is dropped,
the SQL-dispatch repetition is centralized, the key-splitting scheme changes.
Do that update in the same change, not a separate docs pass; a stale
Architecture section is worse than no Architecture section, since it actively
misleads the next session instead of just being silent.

## Design Principles

- Follow SOLID principles: single responsibility, open/closed, Liskov
  substitution, interface segregation, dependency inversion.

## Code Style

- **Sync obligation for tooling configuration:** the IDE save pipeline and the
  Stop hook pipeline are kept in lockstep across the locations below. Any
  change to one of them requires matching updates to the others in the same
  change.
  - Python formatting/lint/type-check:
    [.vscode/settings.json](.vscode/settings.json) (`[python]` block),
    [mypy.ini](mypy.ini) (strict mode), the `[tool.ruff.lint]` section of
    [pyproject.toml](pyproject.toml), all auto-discovered by both the IDE and
    `uv run`, and the shared implementation at
    [.claude/hooks/finalize-python.sh](.claude/hooks/finalize-python.sh),
    registered as a Claude Stop hook in
    [.claude/settings.local.json](.claude/settings.local.json).
  - Markdown formatting: [.vscode/settings.json](.vscode/settings.json)
    (`[markdown]` block), the shared implementation at
    [.claude/hooks/finalize-markdown.sh](.claude/hooks/finalize-markdown.sh),
    and the same Claude Stop-hook registration in
    [.claude/settings.local.json](.claude/settings.local.json).
  - Tool versions: the `dev` group of `[project.optional-dependencies]` in
    [pyproject.toml](pyproject.toml) pins `black`, `ruff`, `mypy`, and
    `pymarkdownlnt`. Both the IDE pipeline (when invoked via `uv run`) and the
    Stop-hook scripts resolve to these venv-installed versions, so bumping any
    of them must be done here — not via Homebrew or any other system-wide
    install.
- Ruff's `E2xx` whitespace rules (e.g. `E271`/`E272`
  multiple-spaces-before/after-keyword) are preview-only in this Ruff version
  and stay off even with `select = ["E", ...]` unless `preview = true` is set.
  Don't be surprised if the CLI/hook misses a whitespace nit that an IDE
  extension flags separately.
- Python version range: refer to `requires-python` in
  [pyproject.toml](pyproject.toml)
- **Comments:** default to none. Only add one when the *why* isn't obvious
  from the code itself (a hidden constraint, a non-obvious invariant, a
  workaround for a specific bug). Never frame a comment around the current
  change, refactor, or task ("moved here for X", "changed from Y to Z",
  "added for the Z flow") — write it as a timeless statement of the
  constraint, since that context rots as the codebase evolves but the
  underlying constraint doesn't. Prefer one line over a multi-line block.
