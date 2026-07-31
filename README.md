# H2HDB

## Description

The `H2HDB` is a comprehensive database for organising and managing H@H comic
collections. It offers a streamlined way to catalogue your comics, providing
key information such as GID (Gallery ID), title, tags and more, ensuring your
collection is always organised and accessible.

---

## Features

- [x] Add new galleries to the database
- [x] Comporess H@H's galleries to a folder
- [x] Record the removed GIDs in a separate list
- [x] Coordinate bounded downloader and database-ingest turns
- [ ] Write document (need?)

---

## Installation and Usage

1. Install [uv](https://docs.astral.sh/uv/getting-started/installation/).
   It manages the Python version and dependencies for you.
1. Install the required packages.

    ```bash
    uv pip install h2hdb
    ```

1. Run the script.

    ```bash
    uv run python -m h2hdb --config [json-path]
    ```

### Config

```json
{
    "h2h": {
        "download_path": "download",
        "cbz_max_size": 768,
        "cbz_grouping": "flat",
        "cbz_sort": "no"
    },
    "database": {
        "sql_type": "mariadb",
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "password",
        "database": "h2h"
    },
    "maintenance": {
        "optimize_enabled": true,
        "min_interval_seconds": 604800,
        "min_work_units": 1000,
        "min_data_free_bytes": 268435456,
        "min_data_free_ratio": 0.2,
        "lock_wait_seconds": 300
    },
    "logger": {
        "level": "INFO"
    }
}
```

- `h2h.download_path`: H@H download path. The default is `download`.
- `h2h.cbz_path`: directory for CBZ output. Unset (the default) disables CBZ
  output entirely; if given, it must be a non-empty path (`""` is rejected).
- `h2h.cbz_max_size`: maximum image size. The default is `768`.
- `h2h.cbz_grouping`: `flat`, `date-yyyy`, `date-yyyy-mm`, or
  `date-yyyy-mm-dd`. The default is `flat`.
- `h2h.cbz_sort`: `no`, `upload_time`, `download_time`, `gid`, `title`,
  `pages`, or `pages+[num]`. The default is `no`.
- `h2h.file_hash_workers`: maximum number of files read and hashed
  concurrently. The default is the smaller of `4` and the available CPU count;
  set it to `1` for serial hashing. Valid values are `1`–`32`.
- `database.sql_type`: `mariadb` or `sqlite`. The default is `mariadb`.
  Existing config files that still use `mysql` must update this field.
- `database.host`, `database.port`, `database.user`, and `database.password`
  are only used for `mariadb`.
- `database.database`: for `mariadb`, this is the database name. For `sqlite`,
  this is the path to the database file.
- `maintenance.optimize_enabled`: enables automatic optimization in the
  resident main loop. Manual `H2HDB.optimize_database()` calls remain
  unconditional.
- `maintenance.min_interval_seconds`: minimum time between automatic
  optimization evaluations. The default is seven days (`604800`).
- `maintenance.min_work_units`: changed or removed galleries accumulated
  before an evaluation. New galleries do not count. The default is `1000`.
- `maintenance.min_data_free_bytes` and `maintenance.min_data_free_ratio`:
  minimum reclaimable space a table (or the SQLite database) must satisfy.
  Both thresholds must pass; the defaults are 256 MiB and 20%.
- `maintenance.lock_wait_seconds`: one wait interval for the MariaDB
  cross-process database gate. The default is 300 seconds. A timeout is logged
  and retried rather than terminating the caller.
- `logger.level`: one of `NOTSET`, `DEBUG`, `INFO`, `WARNING`, `ERROR`, or
  `CRITICAL`.

The main entry point remains resident and keeps its 30-minute periodic scan
deadline, but it now polls the durable `gallery_ingest_state` every five seconds
while waiting. A live downloader lease defers that scan; otherwise an ingest
request wakes h2hdb without waiting for the old uninterruptible 30-minute
sleep.

The singleton coordination row moves through
`INGEST_REQUESTED → INGESTING → READY → DOWNLOADING`. A newly added row starts
at `INGEST_REQUESTED`, so an upgraded or new installation completes one
baseline scan before a downloader can claim `READY`. Downloader integrations
use these public methods:

- `claim_download_turn(lease_seconds=...)` returns a generation-fenced
  `DownloadTurn`, or `None` while h2hdb owns the turn.
- `renew_download_turn(turn, lease_seconds=...)` extends a live downloader
  lease and returns whether that token still owns the generation.
- `ensure_download_request(gid, url="")` atomically reuses an existing durable
  request without replacing its token, or creates one and reports that it was
  newly created.
- `complete_download_request_in_turn(turn, request)` exact-deletes a completed
  root request while its fenced turn remains in `DOWNLOADING` for more roots.
- `complete_missing_download_request_in_turn(turn, request, gid)` performs the
  equivalent live-turn-fenced exact deletion and missing-marker write.
- `request_gallery_ingest(turn)` idempotently hands the generation to h2hdb.
- `finish_download_turn(turn, request)` atomically hands off a successful root
  traversal and conditionally deletes only that request token.
- `finish_missing_download_turn(turn, request, gid)` atomically fences and hands
  off a coordinated lookup; only while that exact request token is still current
  does it record the GID as removed and delete the request.
- `complete_missing_download_request(request, gid)` records a confirmed missing
  gallery and deletes a direct request in one transaction, only while that exact
  token is still current.
- `clear_removed_gallery_gid(gid)` clears a prior missing result after a later
  lookup finds the gallery again.
- `get_gallery_ingest_state()` exposes the durable phase and
  `completed_generation`; a downloader may start its next batch or independent
  root only after `completed_generation >= turn.generation`.

A fresh `DOWNLOADING` lease prevents h2hdb from starting a scan until the
downloader requests handoff; a periodic deadline does not override it. If the
downloader terminates, h2hdb takes over after the lease expires and ingests any
complete gallery folders already published. An ingest acknowledgement is
written only after repeated `synchronize_once()` calls converge to a pass with
no new or changed galleries and scheduled maintenance succeeds. A background
heartbeat renews the ingest lease throughout synchronization and MariaDB
maintenance. SQLite lock contention is retried only within the current lease;
before SQLite maintenance h2hdb renews once and stops the heartbeat so
`VACUUM`'s exclusive lock can fence competing coordination writers. A
successful SQLite optimization can acknowledge after the timestamp expires
only if its generation and owner token are still current. Another resident
treats SQLite lock contention while claiming as temporarily unavailable and
keeps polling. Owner tokens fence stale downloader and h2hdb processes from
renewing or completing a newer turn. Persisted handoff provenance distinguishes
an explicit live-token handoff from an expired downloader lease recovered by
h2hdb, so a recovered stale token cannot later report success.

A download turn may cover one independent root or a bounded batch of
`todownload_gids` roots; every root's complete deep traversal remains an
indivisible unit. Between roots, a batch uses the live-turn-fenced
`complete_download_request_in_turn()` or
`complete_missing_download_request_in_turn()` operation to persist completed
work without handing off `DOWNLOADING`. At its root-count boundary, snapshot
exhaustion, cancellation, or failure, it calls `request_gallery_ingest()` once.
The single-root APIs retain `finish_download_turn()` and
`finish_missing_download_turn()`, where final request mutation and handoff share
one transaction.

All request completion remains exact-token conditional. If the same GID was
already re-enqueued with a newer token, success deletion is a no-op and a stale
missing result writes no marker. A later successful lookup calls
`clear_removed_gallery_gid()` to repair a prior marker; replaying the older
completion cannot restore it. Interrupting an in-turn completion commits both
its fenced request mutation and missing marker or neither. If a downloader
stops before handoff, the durable `DOWNLOADING` lease eventually expires so
h2hdb scans already published files; completed roots stay settled while the
unfinished root remains queued. If h2hdb itself stops in `INGESTING`, its lease
recovery repeats synchronization until convergence before acknowledging the
generation.

Completed removal and changed-gallery batches add work to the singleton
`database_maintenance_state` row. Automatic optimization is evaluated only
after both the work and time thresholds pass, and MariaDB runs `OPTIMIZE TABLE`
only for base tables that also pass both reclaimable-space thresholds. h2hdb
clients can wrap short database work in `H2HDB.database_gate()` so it waits
while maintenance owns the same MariaDB named lock.

H2HDB records a source-filename manifest for each gallery. Adding, deleting, or
renaming a source file marks that gallery's CBZ for rebuilding. This pending
state remains in the database if CBZ output is disabled or a run is interrupted.
During ingestion, the provisional pass creates only missing CBZ files for new
galleries and preserves existing CBZ files. After all galleries have been
processed, one final pass uses the stable exclusion set to perform any required
rebuilds. Created CBZ files carry a small input-layout marker in their ZIP
comment, allowing the final pass to detect normalized renames and filename
swaps even after the database has been deleted and rebuilt. H2HDB does not hash
or scrub CBZ file contents.

---

## Q & A

- Why are some images missing from the CBZ-files?

`H2HDB` does not compress images that are considered spam according to certain
rules. If you encounter any images that you believe should have been included,
please report the issue.

- Why are some images in some CBZ files and not in other CBZ-files?

`H2HDB` learns the spam rule from the previous CBZ files. If you kill the CBZ
files containing these images, the new CBZ files will not contain these images.

---

## Credits

The project was created by [Kuan-Lun Wang](https://www.klwang.tw/home/).

---

## License

This project is distributed under the terms of the GNU General Public Licence
(GPL). For detailed licence terms, see the `LICENSE` file included in this
distribution.
