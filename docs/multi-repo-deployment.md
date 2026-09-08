# Multi-repository deployment

`h2hdb` is a shared core library and schema administrator, not a resident
service. Long-running behavior belongs to sibling integrations. Komga and OPDS
consume the epoch-3/schema-v6 catalog facade; ingest uses the
transaction-owning ingest facade and downloader uses the queue facade. No
sibling may query `catalog_*` or operational tables directly.

## Database ownership

There is one epoch-3/schema-v6 database. Catalog and operational relations are
generated for both SQLite and MariaDB from the same closed-world logical
manifests. Those manifests and their executable schema reports are the
authority for relation shapes, projections, bootstrap facts, decompositions,
and semantic obligations; deployment documentation intentionally does not copy
counts that would drift as the schema evolves.

Schema v6 includes revision-scoped discovery order, normalized search postings,
facet order/count authority, acquisition descriptors, and presentation
descriptors. Operational events remain publication-owned current/retry state,
not OPDS history or a durable delivery queue. Bounded current-only cleanup
retires unreachable finalized non-head state while retaining identities and
objects protected by live work or published revisions.

Source observations carry an immutable qualification result and policy digest
inside their canonical metadata. Core checks the normalized facts against that
metadata before sealing. Rejected galleries remain in the complete source
snapshot with all their file observations; analysis excludes them from spam
counts, content ownership, and GID selection. A rejected gallery cannot suppress
a valid alternative with the same GID. Repairing the source completion marker
or changing the artifact policy causes requalification, and the next publication
can restore the gallery. An unchanged rejected observation can reuse its cached
result under the same policy.

Image decoding and the distinction between invalid source data and temporary
resource failures belong to ingest adapters. Core does not decode images or
turn arbitrary adapter failures into a permanent rejection. Qualification
children follow their observation's reachability: older analysis ancestry may
keep them after a publication is retired, and bounded cleanup removes them only
after the observation becomes unreachable.

An older publication commit can be reclaimed while a newer incremental
analysis still needs the older analysis and source provenance. If that source
snapshot appears again, ingest verifies the retained descriptor against the
completed analysis output and the newer published head, then creates a new
build based on that head. It preserves the referenced ancestry and never
replays the old publication. This also works after a process restart and needs
no schema migration or manual removal of provenance.

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

The greenfield schema has no upgrade/adoption path, compatibility view, legacy
read API, or dual-write period. Schema v5 and older databases cannot be opened
or migrated in place by schema v6. Before replacing an earlier database, stop all
writers and take the backups required by that deployment. Create a truly empty
database, rebuild it from source through the current ingest integration, and run:

```bash
python -m h2hdb migrate --config core-writer.json
```

This constructs `h2hdb_schema_epoch` with `epoch=3`, `schema_version=6`, and a
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
no numbered-migration module, old list API, or legacy hand-written schema
repository. All producers and consumers in one deployment must use the same
schema-v6 public contract; mixed schema versions are unsupported.

## Consumer boundaries

Applications import these public entry points from `h2hdb`:

- `VNextDatabaseAdminFacade` for initialization, full checks, readiness, and
  the immutable external-storage instance binding.
- `VNextCatalogFacade` for current-head catalog reads; a descriptor is accepted
  only while it still exactly equals that head. Its public discovery surface is
  `discover_publications()`, `list_publication_facets()`,
  `list_recent_publications()`, `list_tag_values()`,
  `list_tag_publications()`, single-publication reads, and presentation/page
  reads.
- `VNextDownloadQueueFacade` for normalized request/list/complete operations.
- `VNextIngestFacade.drain_current_only_maintenance()` after ingest completion
  for renewable, response-loss-safe current-catalog cleanup. Filesystem-facing
  integrations pass their `ArtifactReleaseAdapter` registry so abandoned,
  unpublished protection tokens are terminally released before candidate
  metadata cleanup; artifact-free callers may omit it. Its typed outcome
  distinguishes `PROGRESSED` (retry immediately) from `BLOCKED`/`CONTENDED`
  (retry on the ordinary resident poll cadence) and terminal `DONE`.

Repository classes that accept a connector or unit of work are internal
coordination surfaces. A sibling repository must not depend on physical table
names, generated SQL, or a private repository method.

Catalog discovery is SQL-indexed and revision-scoped. Nonblank search uses the
pinned normalization/token policy and AND-matches its lexemes; exact language,
subject, and contributor filters can be combined with it. Facet pages report
exact publication counts under search and the other active facet families.
Recent uploaded and downloaded reads return a complete fixed window of at most
128 acquisition-bearing publications and do not accept a caller limit or
cursor.

`CatalogPublication.redownload_required` still has no closed durable
revision-scoped derivation contract, so consumers must not infer it from
transient operational state.

## Acquisition and presentation storage

Core stores immutable, format-neutral descriptors. An acquisition descriptor
contains its download name, media type, digest-bound opaque storage-object key,
size, and modification time. Presentation descriptors identify cover,
thumbnail, and ordered page resources using the same opaque storage-object
identity plus byte extents, media types, digests, and image dimensions. A key's
codec and segments belong to the adapter; core does not turn them into a path.

The ingest integration owns the concrete archive and artwork bytes. Its
adapters render, store, protect, resolve, reconcile, and release those bytes.
They may choose CBZ, another archive format, standalone images, packed objects,
a filesystem, or object storage without changing the core schema. Core never
opens ZIP/CBZ files, decodes images, requires a shared mount, or makes a
filesystem/object-storage call inside a database transaction.

Catalog-serving consumers receive neutral descriptors and resolve them through
the deployment's storage adapter. Mount paths, permissions, atomic replacement,
recovery, HTTP delivery, and range serving remain integration concerns. Back up
the database and adapter-owned protected objects as one publication set; an
adapter must not delete bytes retained by a published revision or durable
protection claim.

## Startup sequence

A deployment integration follows this order:

1. Create a truly empty SQLite database or MariaDB schema.
2. Run core `migrate` with read-write credentials.
3. Run core `check` with the same read-only configuration consumers will use.
4. Have the storage-owning integration durably create or load its non-nil
   16-byte storage UUID, then call
   `VNextDatabaseAdminFacade.bind_storage_instance()`. The first call binds the
   database; an exact restart is write-free and a different storage root fails
   before maintenance or ingest work.
5. Run the ingest consumer with its concrete source, acquisition, and
   presentation adapters to populate and publish data through bounded vNext
   workflows.
6. Start catalog readers, download workers, and other consumers only after the
   required initial publication exists.
7. Use core `ready` for frequent liveness/readiness probes; keep full `check`
   as the stronger startup or deployment audit.

Schema `READY` means the exact database contract is present; it does not mean
source data or acquisition/presentation bytes have already been ingested.

Resident integrations can pass `max_new_galleries` to `prepare_source()` and
publish cumulative source batches. Every batch inventories the current source
again, incorporates removals and refreshes previously admitted galleries, and
admits a bounded number of new galleries. Previously published source membership
is retained even when deduplication excluded a gallery from the visible catalog.
The prepared handle's `deferred_gallery_count` requests another immediate batch;
it must not be treated as a persisted queue or a publication receipt. Completion
of a gallery, analysis, or CBZ alone does not make it readable: each batch still
uses the full sealed publication and library activation protocol. OPDS discovers
the new current head without restarting. Current-only resource links can expire
at a later publication; consumers that download complete archives before reading
do not require historical catalog revisions.

## Local multi-repository verification

The repositories remain independent and do not form a uv workspace. From the
core repository, build a disposable environment containing the local editable
installs with:

```bash
./scripts/rebuild-multirepo-integration.sh
```

The script does not create or consume `uv.lock`. It installs the checked-out
core and resolves every public consumer from the configured package index. A
wheel, Git URL/ref, archive URL, or local project path is used only when passed
explicitly with `--source PACKAGE=SOURCE`; no sibling checkout is discovered
implicitly. The smoke supplements—but does not replace—schema/Lean checks,
strict coverage evidence, or live MariaDB integration tests.

Tag browsing is an immutable revision projection. `catalog_tag_publication_order`
assigns dense positions within each exact tag by uploaded time descending,
casefolded UTF-8 display-title sort bytes ascending, then publication identity.
`catalog_tag_directory_order` assigns dense positions within each namespace by
its tags' latest member uploaded time descending, then exact UTF-8 value bytes.
Both tables contain semantic keys and one atomic value; namespace and tag ID
authority remain normalized in `catalog_tag_terms`. Candidate disk preparation
computes these orders once. Bounded publication batches copy and independently
validate them before sealing; READY reconstructs membership and order after
transient candidate cleanup. Readers seek indexed positions with capped pages
and validate cursor membership against the current revision. Cleanup releases
each directory row before its latest member publication order and subject rows;
a retained historical descriptor does not pin obsolete directory values.
Schema v5 requires rebuilding older schema databases from an empty database.
