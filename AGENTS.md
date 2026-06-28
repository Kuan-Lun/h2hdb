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

`src/h2hdb/threading_tools.py` provides the bounded thread-pool and
multi-process helpers used by the ingest pipeline in `src/h2hdb/h2hdb_h2hdb.py`.
Be careful when changing insert, compression, or hash-refresh behavior because
the pipeline processes galleries in chunks and runs independent SQL writes
concurrently.

`compress_gallery_to_cbz.py` is imported lazily inside the method that needs it
to avoid a hard Pillow import at package import time. Preserve that lazy-import
behavior when touching compression code.

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
