# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

H2HDB is a MariaDB- or SQLite-backed database/cataloguer for H@H (Hentai@Home) comic collections.
It scans a download folder for galleries (each gallery = a folder containing a `galleryinfo.txt`),
records metadata (GID, title, tags, upload account, times, file hashes) into the database, and
optionally compresses each gallery into a CBZ file. Entry point:
`python -m h2hdb --config [json-path]` (see `src/h2hdb/__main__.py`).

Requires Python >= 3.14.

## Common commands

Environment is managed with `uv`.

```bash
uv pip install -e ".[dev]"      # install package + dev deps (black, ruff, mypy, pymarkdownlnt, pytest, testcontainers)
uv run ruff check src/h2hdb     # lint
uv run black src/h2hdb          # format
uv run mypy src/h2hdb           # type-check (strict mode, see mypy.ini)
uv run pymarkdownlnt fix .      # markdown autofix
uv run pytest                   # run the test suite (see Testing below)
```

Always run Python through `uv run` (e.g. `uv run python -m h2hdb ...`) so it resolves to the
project venv's interpreter and dependency versions.

A Claude Code Stop hook already runs this pipeline automatically after each turn — see
`.claude/hooks/finalize-python.sh` (black → ruff --fix → black → mypy, scoped to `src/h2hdb`) and
`.claude/hooks/finalize-markdown.sh` (pymarkdown fix → ruff format --preview on embedded code
blocks), registered in `.claude/settings.local.json`. It mirrors the VS Code on-save pipeline in
`.vscode/settings.json`. Tool versions for both paths come from the single `dev` extra in
`pyproject.toml` — bump versions there, not via a system-wide install.

If the venv breaks (e.g. after a Python version upgrade — mypyc extension module errors), nuke and
rebuild it with `./scripts/rebuild-env.sh`.

### Testing

`tests/` holds the test suite (pytest). Most tests are parametrized over both backends via the
`db_config` fixture in `tests/conftest.py` (`test_xxx[mariadb]` / `test_xxx[sqlite]`):

- The `sqlite` param uses stdlib `sqlite3` against a temp file (never `:memory:` — every H2HDB
  method opens its own connection, and SQLite's in-memory databases are connection-scoped, so an
  in-memory DB would lose all data between calls).
- The `mariadb` param uses `testcontainers` to start a throwaway MariaDB container per test run —
  this needs a running Docker daemon, but otherwise requires no manual setup (no need to point it
  at a real instance).

`tests/test_sqlite_connector.py` covers the `SQLiteConnector` connection layer directly (not
parametrized, since it's backend-specific by definition).

## Architecture

This project is pre-1.0 and the sections below describe today's design, not a contract to
preserve. The whole mixin/SQL-dispatch shape may be replaced as part of the SOLID-driven refactor
(see Design Principles) — if you're mid-refactor and a statement here conflicts with where you're
intentionally taking the code, the code wins; update or delete the stale part of this doc in the
same change rather than working around it.

### Mixin-based class composition

`H2HDB` (`src/h2hdb/h2hdb_h2hdb.py`) is the public API. It is not one class — it's assembled by
multiple inheritance from a set of mixins, each living in its own `table_*.py`/`view_*.py` module
and covering one schema concern (one table or view) plus that concern's CRUD methods. Every mixin
ultimately inherits `H2HDBAbstract` (`h2hdb_spec.py`), which holds the shared state (`config`,
`logger`, `SQLConnector`, `mariadb_index_prefix_limit`) and declares the abstract interface `H2HDB`
must fully implement. Higher-level mixins (e.g. one composing several table-level mixins into a SQL
view) sit on top of the table-level ones. Read the `class H2HDB(...)` declaration in
`h2hdb_h2hdb.py` for the current, authoritative list of mixins — don't rely on a list here, it goes
stale the moment a mixin is added, split, or renamed.

Convention for adding a new piece of gallery metadata: create a new `table_*.py` mixin with a
`_create_*_table` method plus insert/get methods, have it inherit from the root gallery-id mixin
(in `table_gids.py` — almost every other table foreign-keys into the table it owns) and
`H2HDBAbstract`, then add it to `H2HDB`'s base classes and to `create_main_tables()`.

### SQL abstraction

`sql_connector.py` defines an abstract `SQLConnector` interface (connect/close/execute/fetch/commit/
rollback) plus a small exception hierarchy for key/table/configuration errors. Two concrete
implementations exist: `mariadb_connector.py` (wraps `mysql-connector-python`, wire-protocol
compatible with MariaDB) and `sqlite_connector.py` (wraps stdlib `sqlite3`). Every mixin method
opens a connector via `with self.SQLConnector() as connector:` and writes its query as plain SQL
with `%s` placeholders (the canonical placeholder style across the whole codebase —
`SQLiteConnector` translates `%s` to `sqlite3`'s `?` internally; nothing else needs to know about
that difference).

Most query bodies are identical across both backends and are written once, unconditionally. A
`match self.config.database.sql_type.lower(): case "mariadb": ... case "sqlite": ...` dispatch is
only used where the two backends genuinely need different SQL: DDL (`CREATE TABLE`/`CREATE VIEW`),
anything that calls the name-column generators below, and a small set of backend-specific
statements (date arithmetic, `OPTIMIZE TABLE` vs `VACUUM`, character-set/collation checks — search
for `case "sqlite":` to find them all). Adding a third backend means adding a third `case` only at
those sites, not everywhere `SQLConnector` is used.

### Long names vs MariaDB index limits

MariaDB's InnoDB engine limits indexed key prefixes to 191 bytes
(`H2HDBAbstract.mariadb_index_prefix_limit`). Gallery and file names can exceed that, so on the
MariaDB backend long names get split into multiple fixed-width columns and the index is defined
across all of them together — see `_mariadb_split_name_based_on_limit` and friends in
`h2hdb_spec.py`. SQLite has no such limit, so the SQLite backend stores the same long name in a
single unsplit `TEXT` column (`H2HDBAbstract.sqlite_name_columns`). Both generators return the same
`(column_names, ddl_fragment)` shape, so the surrounding `WHERE`/`SELECT`/`INSERT` code that builds
its query from `column_names` doesn't need to know which backend it's running against, or how many
columns the name was split into. Any new table keyed by a long name must go through one of these
two generators — don't key it on a single unsplit `CHAR(255)`/`VARCHAR` column directly.

SQLite also has no `FULLTEXT` index; tables that declare one on MariaDB get a mirrored FTS5
virtual table + sync triggers on SQLite instead (`H2HDBAbstract._create_sqlite_fts5_sync`) — same
searchable capability, different mechanism.

### Configuration

`config_loader.py` defines the pydantic config model tree (`extra="forbid"` throughout) and
`load_config()`, which loads from a `--config` JSON path or falls back to all defaults. Validated
enum fields (CBZ grouping/sort, log level) live in `settings.py`.

### Concurrency

`threading_tools.py` provides two parallelism primitives used throughout `h2hdb_h2hdb.py`: a bounded
thread pool gated by a semaphore, used to fire off several independent SQL writes for one gallery
concurrently, and a `multiprocessing.Pool` helper used to process multiple *galleries* in parallel.
The main ingest pipeline (`insert_h2h_download` in `h2hdb_h2hdb.py`) is built on these: scan folders
→ refresh CBZ files → sort galleries → insert + compress to CBZ in chunks (excluding spam images
detected via duplicate-hash views) → refresh file hashes → sleep/retry if new galleries were found.

### CBZ compression

`compress_gallery_to_cbz.py` is imported lazily (inside the method that needs it) to avoid a hard
Pillow dependency at import time — keep that lazy-import convention if you touch it. It resizes
images and bundles them into a `.cbz`, skipping images whose hash is in a duplicate/spam exclusion
list computed from a view defined in `h2hdb_h2hdb.py`.

## Keeping this file in sync

Routine use of an existing pattern needs no doc update — e.g. adding one more `table_*.py` mixin
that follows the documented convention doesn't make the Architecture section stale, since it
already points at the code instead of enumerating mixins. Update or delete the affected paragraph
only when a change replaces the *pattern itself* — e.g. mixin composition is dropped, the
SQL-dispatch repetition is centralized, the key-splitting scheme changes. Do that update in the
same change, not a separate docs pass; a stale Architecture section is worse than no Architecture
section, since it actively misleads the next session instead of just being silent.

## Design Principles

- Follow SOLID principles: single responsibility, open/closed, Liskov substitution, interface segregation, dependency inversion.

## Code Style

- **Sync obligation for tooling configuration:** the IDE save pipeline and the Stop hook pipeline are kept in lockstep across the locations below. Any change to one of them requires matching updates to the others in the same change.
  - Python formatting/lint/type-check: [.vscode/settings.json](.vscode/settings.json) (`[python]` block), [mypy.ini](mypy.ini) (strict mode), the `[tool.ruff.lint]` section of [pyproject.toml](pyproject.toml), all auto-discovered by both the IDE and `uv run`, and the shared implementation at [.claude/hooks/finalize-python.sh](.claude/hooks/finalize-python.sh), registered as a Claude Stop hook in [.claude/settings.local.json](.claude/settings.local.json).
  - Markdown formatting: [.vscode/settings.json](.vscode/settings.json) (`[markdown]` block), the shared implementation at [.claude/hooks/finalize-markdown.sh](.claude/hooks/finalize-markdown.sh), and the same Claude Stop-hook registration in [.claude/settings.local.json](.claude/settings.local.json).
  - Tool versions: the `dev` group of `[project.optional-dependencies]` in [pyproject.toml](pyproject.toml) pins `black`, `ruff`, `mypy`, and `pymarkdownlnt`. Both the IDE pipeline (when invoked via `uv run`) and the Stop-hook scripts resolve to these venv-installed versions, so bumping any of them must be done here — not via Homebrew or any other system-wide install.
- Ruff's `E2xx` whitespace rules (e.g. `E271`/`E272` multiple-spaces-before/after-keyword) are preview-only in this Ruff version and stay off even with `select = ["E", ...]` unless `preview = true` is set — don't be surprised if the CLI/hook misses a whitespace nit that an IDE extension flags separately.
- Python version range: refer to `requires-python` in [pyproject.toml](pyproject.toml)
