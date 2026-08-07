# Multi-repository deployment

The former `python -m h2hdb` resident process is split by runtime responsibility.
For a deployment that enables OPDS, one former resident container becomes two
long-running containers:

1. `h2hdb-ingest` scans the gallery mount, reconciles CBZ files, and publishes
   complete catalog snapshots.
2. `h2hdb-opds` serves the published projection over HTTP.

Core is a library used by both processes. Its `python -m h2hdb` command is an
administration command, not a third resident service. Run it as an init job for
schema migration or compatibility checks. Existing downloader and Komga
containers remain separate and are not included in the two-container count.

## Database ownership

There is one core-owned database. Its canonical gallery, file, tag, time, queue,
and coordination tables are authoritative. Core also owns the revisioned
catalog projection used by OPDS.

Ingest receives read-write credentials. OPDS connects to that same database in
read-only mode. For MariaDB, give OPDS a dedicated database account with only
`SELECT` and `SHOW VIEW` privileges; the latter lets core validate critical view
definitions during the read-only compatibility check. For SQLite, mount the
same database file read-only in the OPDS container. Do not create a second
writable OPDS database.

Ingest and OPDS must see the immutable artifact-store mount at the same absolute
container path because catalog revisions store physical artifact paths. Komga
must scan only the separate current friendly CBZ library, never the immutable
artifact store.

## CBZ storage lifecycle and mounts

The two CBZ roots are not interchangeable:

- `cbz_path` is a replaceable, current-only projection with friendly filenames
  for Komga. Ingest owns it read-write. Komga scans this root, but OPDS and the
  downloader do not need it.
- `artifact_store_path` is the durable, content-addressed source of every CBZ
  published by a catalog revision. It also contains ingest's publication lock
  and reconciliation state. Ingest needs read-write access. OPDS must mount the
  same host directory read-only at the same absolute container path and set
  `artifact_root` to that path. Komga and the downloader must not scan or write
  this root.

For example, a deployment using `/hentai/comics` for the Komga library and
`/hentai/comics-artifacts` for published artifacts needs mounts equivalent to:

```yaml
services:
  ingest:
    volumes:
      - /host/gallery-downloads:/hentai/download:ro
      - /host/komga-comics:/hentai/comics:rw
      - /host/h2hdb-artifacts:/hentai/comics-artifacts:rw
  opds:
    volumes:
      - /host/h2hdb-artifacts:/hentai/comics-artifacts:ro
  komga:
    volumes:
      - /host/komga-comics:/data/hentai/comics:ro
```

The OPDS config must use `artifact_root: /hentai/comics-artifacts`; mounting the
same host directory at a different path is insufficient because catalog rows
contain the ingest-visible absolute path. Both containers also need compatible
numeric UID/GID or an explicit ACL so OPDS can read ingest-created owner-only
files.

Back up the database and artifact store as one publication set. Losing
`cbz_path` is recoverable from current artifacts, but losing the artifact store
breaks current and historical acquisition URLs. Old artifacts are retained for
pinned historical revisions, so capacity grows with actual CBZ changes. There
is currently no revision-retention or artifact-garbage-collection command; do
not manually delete content-addressed files while catalog revisions reference
them. The current projection is an independent copy rather than a hard link:
software writing through a Komga filename therefore cannot mutate immutable
history.

## Fresh initialization

The current architecture starts with an empty database. Before replacing an
older deployment, stop all writers and take a database and filesystem backup.
Then point the current services at a new empty database and initialize it:

```bash
python -m h2hdb migrate --config core.json
```

The runtime refuses every non-empty database without the current migration
ledger. It has no schema-adoption or old-database upgrade path. Numbered
migrations remain the initializer for an empty database and the mechanism for
future forward-only changes to a current database.

Verify compatibility before starting consumers:

```bash
python -m h2hdb check --config core.json
```

While consumers are still stopped, run the ingest-owned bootstrap. This is a
distinct second phase: it performs no DDL and never writes the schema ledger.
It rebuilds canonical rows from the gallery filesystem and publishes the
initial projection:

```bash
python scripts/bootstrap-catalog.py \
  --config ingest.json
```

Run that command from the `h2hdb-ingest` checkout.

Then start the two steady-state residents:

```bash
h2hdb-ingest --config ingest.json
h2hdb-opds --config opds.json
```

If OPDS is not enabled, only ingest replaces the former H2HDB resident. The
downloader and Komga processes continue using the core public API.

## CBZ filenames

Content-addressed physical names such as `gid-sha256.cbz` are an ingest storage
choice, not an OPDS 2.0 requirement. They live in the immutable artifact store
so historical OPDS revisions remain downloadable. OPDS sends the friendly
catalog artifact name in `Content-Disposition`.

Ingest separately maintains a current friendly CBZ library for Komga. A rebuild
atomically replaces that independent copy, so Komga sees one current book while
the artifact store retains history.

## Local multi-repository verification

The repositories remain independent and do not form a uv workspace. From the
core repository, build a disposable environment containing all seven editable
installs and run a real SQLite public-API smoke test with:

```bash
./scripts/rebuild-multirepo-integration.sh
```

The script does not create or consume `uv.lock`.
