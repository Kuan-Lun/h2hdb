# Measure SQLite catalog reads

Use this optional benchmark to compare catalog read performance on your own
machine with a reproducible synthetic library. It creates a new SQLite database
and a JSON report; it never opens or replaces an existing database.

The benchmark measures catalog queries only. It does not import your galleries,
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
