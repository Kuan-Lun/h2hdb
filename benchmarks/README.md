# Measure catalog reads and ingest database work

Use this optional benchmark to compare catalog read performance on your own
machine with a reproducible synthetic library. It creates a new SQLite database
and a JSON report; it never opens or replaces an existing database.

The catalog benchmark measures queries only. It does not import your galleries,
create CBZs or images, or measure OPDS downloads, MariaDB, or NAS performance.
For normal installation, start with the [main guide](../README.md).

## Prepare the environment

Run from a source checkout of the version you want to measure. You need
Python 3.14 or later and the package installed in a virtual environment:

```bash
python3.14 -m venv .venv
source .venv/bin/activate
python -m pip install .
```

On Windows, use `py -3.14` to create the environment and
`.venv\Scripts\Activate.ps1` to activate it in PowerShell.

Choose an existing output directory with room for a new database and report.
Both output filenames must be unused. Each repeat needs new filenames; do not
point this tool at your library database.

## Run a measurement

Start with the small, 165-publication profile:

```bash
python -m benchmarks.sqlite_catalog_scalability \
  --profile smoke \
  --database /path/to/results/catalog-smoke.sqlite3 \
  --receipt /path/to/results/catalog-smoke.json
```

For a larger comparison, use the 10,000-publication profile:

```bash
python -m benchmarks.sqlite_catalog_scalability \
  --profile 10k \
  --database /path/to/results/catalog-10k.sqlite3 \
  --receipt /path/to/results/catalog-10k.json
```

Replace `/path/to/results` with your output directory. Both commands use the
same fixed random seed by default. `--seed` selects another seed; retain it with
your results so comparisons use equivalent inputs. Use `--help` for all options.

Before saving a successful report, the tool checks the generated database with
the full Core audit and SQLite integrity/foreign-key checks, and verifies that
the measured reads return the expected results. An error or missing report is
not a successful measurement.

## Read the report

The JSON records:

- The package/source identity, seed, database size, and database checksum.
- Fixture creation and full-audit time.
- The first read after creation, repeated warm reads, a cursor page, and a
  comparison using separate public catalog calls.
- Query, connection, and transaction counts, serialized result size, and a
  separate Python traced-memory measurement.

Compare runs with the same profile and seed. Keep the software version, hardware,
storage, and concurrent machine load with the report. Compare query counts and
result correctness as well as elapsed time.

The first read uses a fresh connection, but the operating system may already
cache the database. It is not a cold-disk result. Python traced memory is not the
whole process's memory use. A successful report has no latency threshold and
does not promise that a real library will meet a particular response time.

## After the run

Keep the report and its matching database if you need to inspect or reproduce a
result. Otherwise, remove only the synthetic files you selected for this run.
If the command refuses an existing path, choose new filenames for the next run.
For additional evidence and its limits, see the
[verification guide](../verification/README.md).

## Measure ingest database work

To investigate import cost, use the separate manual acceptance tool from a
development checkout. Prepare its dependencies using the
[verification setup](../verification/README.md#prepare-a-separate-checkout).
Run measurements one at a time, without competing benchmarks or test suites.
The following small cases help check the tool and page boundaries; they are not
a representative full-library performance assessment:

```bash
.venv/bin/python scripts/check-ingest-database-performance.py \
  --backend sqlite --case 3:1:63 --case 3:1:64 --case 3:1:65 \
  --case 3:1:127 --case 3:1:128 --case 3:1:129 \
  --replacement-case 127:3 --replacement-case 128:3 \
  --replacement-case 129:3 \
  --output /tmp/ingest-cost-sqlite.json
```

`--case` uses `galleries:publication_batch:pages_per_gallery`.
`--replacement-case` uses `pages:cycles` to include replacement and retirement
cleanup. These workflows use synthetic data, check the published catalog and
run the full database audit. They do not read your library.

MariaDB requires a working Docker daemon and explicit opt-in. This command
creates a disposable MariaDB 10.11.11 container:

```bash
.venv/bin/python scripts/check-ingest-database-performance.py \
  --backend mariadb --allow-mariadb --case 3:1:65 \
  --replacement-case 129:3 --output /tmp/ingest-cost-mariadb.json
```

Read the report as well as the exit status: **0** means the selected cost
checks passed, **1** means a measured cost violated its budget, and **2** means
the evidence is incomplete or execution failed. A completed measurement can
fail acceptance. Passing tests of the measurement tool does not override a
failing cost report.

Keep the report's input dimensions, source identity, operation counts and
elapsed samples together. SQL calls count connector methods; returned rows
are not rows examined by the server. These measurements exclude image
qualification, CBZ production and physical storage I/O. They cannot certify
the full-library objectives of 132,046 galleries, at most 24 hours of non-CBZ
work (12 desired), or at most seven days including CBZ production.

For source-file and image costs, use `scripts/check-source-cost.py` in an
explicitly supplied [ingest checkout](https://github.com/Kuan-Lun/h2hdb-ingest).
Its `scripts/check-library-cleanup-cost.py` measures library-journal query work.
Use each tool's `--help` and that checkout's environment. Those reports cover
different work and do not replace a complete lifecycle measurement.

Other focused experiments live in [`scripts/`](../scripts/). Start with the
tool's docstring and `--help` to select inputs and understand its scope:

| Question | Tool |
| --- | --- |
| How does publication batch size affect database work? | `ingest_batch_scaling_probe.py` |
| How does source catch-up compare with its fixed cost model? | `source_catchup_cost_model.py` |
| What does current-only artifact release cost? | `current_only_release_probe.py` |
| How much work selects changed hashes or decision keys? | `analysis_changed_hash_probe.py`, `analysis_decision_key_probe.py` |
| How can two ingest versions be measured on another machine? | `build-source-performance-bundle.py` |

These are scoped diagnostic tools. Their results do not establish deployment
throughput or replace the acceptance report above. The separate
`.venv/bin/python scripts/run-pytest.py performance-acceptance` profile checks
database performance scenarios on SQLite and disposable MariaDB with Docker;
it is outside the bounded merge profile.

## Convert an old diagnostic log

If you need to analyze schema-1 database performance JSONL with the current
schema-2 format, run the offline converter from a checkout:

```bash
.venv/bin/python scripts/normalize-database-performance-log.py old.jsonl new.jsonl
```

The output path must be new. The input is preserved and no database is opened.
Complete raw JSON records and `database_performance` log lines are accepted;
HTML exports must first be reconstructed into complete log lines. The converter
preserves recorded totals and displayed query families, but cannot recover
omitted identities or convert ingest's human-readable text. A failure can leave
a partial output, so only use the result after a successful exit.
