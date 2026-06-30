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
(connect/close/execute/fetch/commit/rollback) plus a small exception hierarchy
for key/table/configuration errors. Two concrete implementations exist:
`mariadb_connector.py` (wraps `mysql-connector-python`, wire-protocol
compatible with MariaDB) and `sqlite_connector.py` (wraps stdlib `sqlite3`).
Every repository method opens a connector via
`with self.SQLConnector() as connector:` and writes its query as plain SQL with
`%s` placeholders (the canonical placeholder style across the whole codebase —
`SQLiteConnector` translates `%s` to `sqlite3`'s `?` internally; nothing else
needs to know about that difference).

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
not per-gallery concurrent writes. The one parallelism primitive left is
CPU-bound work across *galleries* — CBZ compression and CBZ-staleness
checks — dispatched to a `multiprocessing.Pool` via `run_in_parallel` in
`cbz_files.py` (its only caller). `insert_h2h_download` in `h2hdb_h2hdb.py`
owns the pool's lifetime: it creates one `Pool(POOL_CPU_LIMIT)` (only when
`cbz_path` is configured) and reuses it for every gallery chunk plus the
final staleness pass within that call, instead of spawning a fresh batch of
worker processes per chunk. `POOL_CPU_LIMIT`/`CPU_NUM` live in
`h2hdb_h2hdb.py` alongside it. The main ingest pipeline is built on this:
scan folders → refresh CBZ files → sort galleries → insert + compress to CBZ
in chunks (excluding spam images detected via duplicate-hash views) →
refresh file hashes → sleep/retry if new galleries were found.

### CBZ compression

`compress_gallery_to_cbz.py` is imported lazily (inside the method that needs
it) to avoid a hard Pillow dependency at import time — keep that lazy-import
convention if you touch it. It resizes images and bundles them into a `.cbz`,
skipping images whose hash is in a duplicate/spam exclusion list computed from
a view defined in `h2hdb_h2hdb.py`.

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
