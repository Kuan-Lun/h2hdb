# SQL catalog scalability fixture

`sqlite_catalog_scalability.py` creates a fresh epoch-3 SQLite catalog for
read-path scalability measurements. It never opens, migrates, or overwrites an
existing database. Both target parent directories must already exist, and both
the database and receipt paths must be new.

The fixture uses a fixed seed, normalized production family bindings where a
standalone writer exists, and a benchmark-only manifest-bound SQL writer for
the remaining relations. Every directly written table and column must be
present in the wheel-resident generated SQLite manifest. The finished database
must pass the public full READY audit, SQLite foreign-key check, and integrity
check before the JSON receipt is written.

The fixture contains neutral acquisition descriptors but creates no CBZ,
artwork, or object-storage bytes. It is a catalog-reader scalability fixture,
not a refinement of the complete ingest state machine.

Run the small profile used by the automated test:

```console
.venv/bin/python -m benchmarks.sqlite_catalog_scalability \
  --profile smoke \
  --database /private/tmp/h2hdb-catalog-smoke.sqlite3 \
  --receipt /private/tmp/h2hdb-catalog-smoke.json
```

Run the manual 10,000-publication profile:

```console
.venv/bin/python -m benchmarks.sqlite_catalog_scalability \
  --profile 10k \
  --database /private/tmp/h2hdb-catalog-10k.sqlite3 \
  --receipt /private/tmp/h2hdb-catalog-10k.json
```

The receipt records schema and source provenance, fixed-seed result oracles,
database SHA-256 and byte size, setup and full-audit time, first-after-build and
warm bundled reads, a cursor page, a separately invoked public-facade reference,
query/connection/transaction counts, normalized SQL shape digests and counts,
serialized result bytes, and a separate Python traced-memory probe. The
first-after-build sample opens a fresh database
connection but is not an operating-system page-cache-cold measurement. The
receipt records the exact measurement order and enforces no host latency
threshold.

The fixture contract digest excludes absolute database and receipt paths. Its
source manifest digest covers `pyproject.toml`, this benchmark tool, and every
Python source path and byte under `src/h2hdb`. Runtime equality between the
bundled call and the four separate public facade calls is an executable
regression check, not a substitute for the repository's formal refinement
theorems. `verification/lean/CanonicalBatchHydration.lean` proves that an exact
single-page cache hit or a streaming fallback preserves every per-reference
read result under the production batch bound.
