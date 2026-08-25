# Multi-repository deployment

`h2hdb` is a shared core library and schema administrator, not a resident
service. Long-running behavior belongs to sibling integrations. Komga and OPDS
consume the epoch-2 catalog facade; ingest uses the transaction-owning ingest
facade and downloader uses the queue facade. No sibling may query `catalog_*`
or operational tables directly.

## Database ownership

There is one epoch-2/version-1 database. Its 340 catalog BCNF base relations,
78 intentional catalog views, 75 operational BCNF base relations, and one
operational activation view are generated for both SQLite and MariaDB from the
same logical manifests. One of the 75 operational bases is the separately
created epoch-control relation, so the database has 340 + 75 = 415 base tables.
The catalog graph has 49 sealed vertical families, 27 checked decompositions,
335 narrow bases plus five intentional wide BCNF recompositions, and an exact
295-relation physical authority closure (239 mutation relations plus 56
read-only views). Each backend receives exactly 4,644 typed bootstrap rows.

Ingest and coordination workers receive read-write credentials. Catalog-serving
consumers use read-only credentials and `VNextCatalogFacade`. For SQLite, mount
the same database file read-only in read-only containers. For MariaDB, use a
dedicated account with the metadata/read privileges required by the full schema
check and application reads. Do not create a second writable projection
database.

The MariaDB compatibility baseline is server version 10.11.11, including the
Synology 10.11.11-1551 package build. Release integration tests pin the
upstream `mariadb:10.11.11` image and reject a different server version before
schema initialization.

Only a deployment init job runs schema construction. Consumer containers run a
full check at startup and may use the lightweight readiness probe separately.

## Fresh initialization

The greenfield schema has no v1-v7 upgrade/adoption path, compatibility view,
or dual-write period. Before replacing any earlier database, stop all writers
and take the backups required by that deployment. Point the current services at
a truly empty database, then run:

```bash
python -m h2hdb migrate --config core-writer.json
```

This constructs `h2hdb_schema_epoch` with `epoch=2`, `schema_version=1`, and a
checksum-bound `BUILDING` state; applies the generated SQLite or MariaDB DDL and
bootstrap facts; validates the exact manifests; and atomically marks the epoch
`READY`.

If the init job crashes, rerun the same command. It resumes only a matching
`BUILDING` epoch. A previous, foreign, drifted, or malformed non-empty database
is rejected without adoption or destructive repair.

Verify the complete schema from a read-only configuration before starting
consumers:

```bash
python -m h2hdb check --config core-reader.json
```

Use the O(1) epoch/version/manifest probe for frequent container readiness:

```bash
python -m h2hdb ready --config core-reader.json
```

`migrate`, `check`, and `ready` are the only core CLI operations. Despite its
name, `migrate` constructs or resumes the single manifest-bound greenfield
epoch; it does not execute numbered historical migrations.

The core wheel contains neither `H2HDB` nor `MigrationRunner`, and it contains
no numbered-migration module or legacy hand-written schema repositories. Do not
work around that boundary by pinning an epoch-2 deployment to a mixed set of
core versions.

## Consumer boundaries

Applications import these public entry points from `h2hdb`:

- `VNextDatabaseAdminFacade` for initialization, full checks, and readiness.
- `VNextCatalogFacade` for revision-pinned catalog reads.
- `VNextDownloadQueueFacade` for normalized request/list/complete operations.

Repository classes that accept a connector or unit of work are internal
coordination surfaces. A sibling repository must not depend on physical table
names, generated SQL, or a private repository method.

The integration boundary has three explicit limits:

- Nonblank search fails closed until a normalized revision-pinned search index
  is part of the schema and reader contract.
- `CatalogPublication.redownload_required` has no closed durable
  revision-scoped derivation contract, so consumers must not infer it from
  transient operational state.
- Core provides typed artifact preparation and storage protocols, while the
  concrete filesystem/object-storage adapter remains a consumer
  responsibility. Core alone does not perform filesystem or artifact I/O.

## Artifact storage and mounts

An ingest integration that publishes artifacts must implement the registered
typed storage adapter. That adapter owns the mapping from core's verified,
content-addressed artifact identity to a concrete filesystem or object-store
locator and must acknowledge exact byte protection without changing the
verified archive.

If a filesystem-backed adapter is used, keep these conceptual roots separate:

- A replaceable current library with friendly filenames for Komga.
- An immutable, content-addressed artifact store used for published revisions.

Ingest needs write access to both. Catalog-serving consumers need read access
only to the artifact store when their acquisition adapter serves those bytes.
Komga scans only the replaceable current library. Mount paths, permissions,
locator encoding, reconciliation, and recovery are part of the concrete
consumer adapter; do not hard-code them in core or reconstruct a locator from a
catalog display name.

Back up the database and protected artifact store as one publication set. An
adapter must not delete content still retained by a catalog revision or a
durable protection claim.

## Startup sequence

A deployment integration follows this order:

1. Create a truly empty SQLite database or MariaDB schema.
2. Run core `migrate` with read-write credentials.
3. Run core `check` with the same read-only configuration consumers will use.
4. Run the ingest consumer with its concrete source/artifact adapters to
   populate and publish data through bounded vNext workflows.
5. Start catalog readers, download workers, and other consumers only after the
   required initial publication exists.
6. Use core `ready` for frequent liveness/readiness probes; keep full `check`
   as the stronger startup or deployment audit.

Schema `READY` means the exact database contract is present; it does not mean
source data or artifacts have already been ingested.

## Local multi-repository verification

The repositories remain independent and do not form a uv workspace. From the
core repository, build a disposable environment containing the local editable
installs with:

```bash
./scripts/rebuild-multirepo-integration.sh
```

The script does not create or consume `uv.lock`. Its smoke installs every
public consumer, including `h2hdb-ingest` and `h2hdb-downloader`, and
supplements—but does not replace—schema/Lean checks, strict coverage evidence,
or live MariaDB integration tests.
