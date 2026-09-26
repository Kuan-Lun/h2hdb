#!/usr/bin/env python3
"""Finish only retained schema-8 cleanup work before the offline conversion.

The converter runs this file in a separate process using the checksum-verified
Core 0.42.2 wheel. No old runtime is imported into the schema-9 process. This
worker never creates cleanup cycles or discovers additional cleanup candidates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import select
import stat
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from time import perf_counter
from typing import Any, Literal, cast

from h2hdb import CoreConfig, DatabaseAccessMode, load_config
from h2hdb.database_clock import database_unix_microseconds
from h2hdb.repository import RepositoryContext
from h2hdb.schema_epoch import (
    MariaDBAdvisorySchemaEpochGate,
    MariaDBSchemaEpochCatalog,
    SchemaEpochRunner,
    SQLiteSchemaEpochCatalog,
    mariadb_schema_epoch_gate_name,
)
from h2hdb.sql_connector import SQLConnector
from h2hdb.vnext_cleanup_repository import (
    _CLEANUP_ALGORITHM_VERSION,
    CleanupCycle,
    CleanupRetentionBlockedError,
    CleanupTargetKind,
    VNextCleanupRepository,
    _cycle_from_job_row,
    _require_complete_job,
    _validate_strategy_seeds,
)
from h2hdb.vnext_domains import require_int63
from h2hdb.vnext_maintenance_gate_repository import (
    GateLease,
    MaintenanceGateRepository,
    MaintenanceGateUnavailableError,
)
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider
from h2hdb.vnext_transaction import VNextUnitOfWork

_AFFECTED = (
    "GALLERY_UPLOAD_TIME",
    "CATALOG_PUBLICATION",
    "GALLERY_OBSERVATION",
    "PUBLICATION_CANDIDATE",
)
_MANIFESTS = {
    "sqlite": "0d1f17682f461388b159df7d53b017a8fb8bc7af7dafaf395946513c4be7717b",
    "mariadb": "09ff699ec5d63785cda128a709d30fbe5e3edab3e0e685475d941b890c6e2426",
}
_LEASE_MICROSECONDS = 60_000_000


class WorkerError(RuntimeError):
    """A controlled error code safe to report without configuration contents."""


def _require_owner(owner_fd: int) -> None:
    if os.name != "posix":
        raise WorkerError("worker_requires_posix_use_docker")
    if owner_fd < 3 or not stat.S_ISFIFO(os.fstat(owner_fd).st_mode):
        raise WorkerError("invalid_owner_pipe")
    if select.select((owner_fd,), (), (), 0)[0]:
        if os.read(owner_fd, 1):
            raise WorkerError("unexpected_owner_pipe_data")
        raise WorkerError("converter_owner_exited")


@contextmanager
def _schema_gate(connector: SQLConnector, backend: str) -> Iterator[None]:
    if backend == "mariadb":
        database = connector.fetch_one("SELECT DATABASE()")
        connector.commit()
        if len(database) != 1 or not isinstance(database[0], str):
            raise WorkerError("database_identity_unavailable")
        with MariaDBAdvisorySchemaEpochGate().acquire_named(
            connector, mariadb_schema_epoch_gate_name(database[0])
        ):
            yield
        return
    if connector.fetch_one("PRAGMA locking_mode = EXCLUSIVE") != ("exclusive",):
        raise WorkerError("sqlite_exclusive_mode_unavailable")
    with connector.transaction():
        connector.fetch_one("SELECT singleton_id FROM h2hdb_schema_epoch")
    # Connection-level EXCLUSIVE locking spans each bounded batch transaction.
    yield


def _require_schema8(connector: SQLConnector, backend: str) -> None:
    provider = GeneratedVNextSchemaProvider(
        backend=cast("Literal['sqlite', 'mariadb']", backend)
    )
    definition = provider.definition
    if (definition.epoch, definition.schema_version) != (
        3,
        8,
    ) or definition.manifest_sha256 != _MANIFESTS[backend]:
        raise WorkerError("worker_requires_exact_core_0_42_2_schema8")
    catalog = (
        SQLiteSchemaEpochCatalog()
        if backend == "sqlite"
        else MariaDBSchemaEpochCatalog()
    )
    catalog.validate_control_table(connector)
    state = SchemaEpochRunner(gate=None, catalog=catalog)._read_and_validate_control(
        connector, definition, definition.manifest_sha256
    )
    if state != "READY":
        raise WorkerError("cleanup_requires_schema8_ready")


def _existing_affected_cycle(work: VNextUnitOfWork) -> CleanupCycle | None:
    row = work.connector.fetch_one(
        "SELECT sweep.target_kind, sweep.shard_no, sweep.target_key, "
        "job.cleanup_id, job.cycle_generation, job.cycle_cutoff_at, "
        "job.algorithm_version, job.max_rows_per_transaction, "
        "job.hash_cache_max_age_microseconds "
        "FROM operational_cleanup_sweep_targets AS sweep "
        "JOIN operational_cleanup_jobs AS job ON job.target_key = sweep.target_key "
        "WHERE job.state = 'OPEN' AND sweep.target_kind IN (%s, %s, %s, %s) "
        "ORDER BY sweep.target_key LIMIT 1",
        _AFFECTED,
    )
    if not row:
        return None
    if (
        len(row) != 9
        or row[6] != _CLEANUP_ALGORITHM_VERSION
        or row[0] not in _AFFECTED
        or row[8] != 0
    ):
        raise WorkerError("invalid_retained_cleanup_cycle")
    cycle = CleanupCycle(
        cleanup_id=row[3],
        target_kind=CleanupTargetKind(row[0]),
        shard_no=row[1],
        target_key=row[2],
        cycle_generation=row[4],
        cycle_cutoff_at=row[5],
        max_rows_per_transaction=row[7],
        hash_cache_max_age_microseconds=row[8],
    )
    _validate_strategy_seeds(work, cycle.target_kind)
    return cycle


def _root_count(connector: SQLConnector, cycle: CleanupCycle) -> int:
    roots = connector.fetch_all(
        "SELECT frozen_root_key FROM operational_cleanup_cycle_roots "
        "WHERE cleanup_id = %s LIMIT 257",
        (cycle.cleanup_id,),
    )
    if len(roots) > 256:
        raise WorkerError("retained_cleanup_root_bound_exceeded")
    return len(roots)


def _progress(event: str, started: float, **fields: object) -> None:
    print(
        json.dumps(
            {
                "event": event,
                "elapsed_seconds": round(perf_counter() - started, 3),
                **fields,
            },
            separators=(",", ":"),
        ),
        file=sys.stderr,
        flush=True,
    )


def _completed_gid_job_proof(
    connector: SQLConnector, *, owner_fd: int
) -> dict[str, object]:
    """Validate retained old authority before the converter retires these rows."""

    rows = connector.fetch_all(
        "SELECT sweep.shard_no, sweep.target_key, job.cleanup_id, "
        "job.cycle_generation, job.cycle_cutoff_at, job.algorithm_version, "
        "job.max_rows_per_transaction, job.hash_cache_max_age_microseconds, "
        "job.frozen_root_count, job.frozen_root_set_sha256, job.state, "
        "job.created_at, job.completed_at, job.final_chain_sha256, "
        "job.final_deleted_count FROM operational_cleanup_sweep_targets AS sweep "
        "JOIN operational_cleanup_jobs AS job ON job.target_key = sweep.target_key "
        "WHERE sweep.target_kind = 'GALLERY_UPLOAD_TIME' "
        "ORDER BY sweep.shard_no LIMIT 257"
    )
    if len(rows) > 256:
        raise WorkerError("completed_gid_job_bound_exceeded")
    previous_shard = -1
    for row in rows:
        _require_owner(owner_fd)
        if len(row) != 15 or row[10] != "COMPLETE":
            raise WorkerError("gid_job_not_complete")
        cycle = _cycle_from_job_row(
            CleanupTargetKind("GALLERY_UPLOAD_TIME"), row[0], row[1:]
        )
        _require_complete_job(row[1:])
        require_int63(row[11], field="cleanup created_at")
        if cycle.shard_no <= previous_shard or cycle.hash_cache_max_age_microseconds:
            raise WorkerError("invalid_completed_gid_job_policy")
        previous_shard = cycle.shard_no
        for table in (
            "operational_cleanup_checkpoints",
            "operational_cleanup_cycle_roots",
        ):
            if connector.fetch_one(
                f"SELECT 1 FROM {table} WHERE cleanup_id = %s LIMIT 1",
                (cycle.cleanup_id,),
            ):
                raise WorkerError("completed_gid_job_retains_children")
    encoded = json.dumps(
        rows, separators=(",", ":"), default=lambda value: value.hex()
    ).encode("ascii")
    return {
        "completed_gid_job_count": len(rows),
        "completed_gid_jobs_sha256": hashlib.sha256(
            b"h2hdb-schema8-completed-gid-jobs-v1\0" + encoded
        ).hexdigest(),
    }


def _drain(
    connector: SQLConnector, *, backend: str, owner_fd: int, started: float
) -> dict[str, Any]:
    cycles: list[dict[str, Any]] = []
    summary: dict[str, Any] = {
        "status": "no_affected_open_cycle",
        "cycles": cycles,
        "frozen_root_count": 0,
        "rows_deleted": 0,
        "steps": 0,
        "blocker": None,
    }
    lease: GateLease | None = None
    try:
        _require_owner(owner_fd)
        with connector.transaction():
            work = VNextUnitOfWork(connector, backend=backend)
            cycle = _existing_affected_cycle(work)
            if cycle is None:
                return summary
            claim = MaintenanceGateRepository.lock_exclusive_claim(work)
            lease = claim.grant(
                now=database_unix_microseconds(work),
                lease_duration=_LEASE_MICROSECONDS,
            )
        while cycle is not None:
            _require_owner(owner_fd)
            current: dict[str, Any] = {
                "cleanup_id": cycle.cleanup_id.hex(),
                "target_kind": cycle.target_kind.value,
                "frozen_root_count": _root_count(connector, cycle),
                "rows_deleted": 0,
                "steps": 0,
            }
            connector.commit()
            cycles.append(current)
            summary["frozen_root_count"] += current["frozen_root_count"]
            _progress("retained_cycle", started, **current)
            while True:
                _require_owner(owner_fd)
                with connector.transaction():
                    work = VNextUnitOfWork(connector, backend=backend)
                    renewal = MaintenanceGateRepository.lock_for_renewal(work, lease)
                    lease = renewal.renew(
                        now=database_unix_microseconds(work),
                        lease_duration=_LEASE_MICROSECONDS,
                    )
                _require_owner(owner_fd)
                with connector.transaction():
                    work = VNextUnitOfWork(connector, backend=backend)
                    results = VNextCleanupRepository.advance_current_only_cycle(
                        work,
                        gate_lease=lease,
                        cycle=cycle,
                        now=lambda: database_unix_microseconds(work),
                    )
                rows = sum(result.row_count for result in results)
                current["rows_deleted"] += rows
                current["steps"] += 1
                summary["rows_deleted"] += rows
                summary["steps"] += 1
                _progress(
                    "cleanup_batch_committed",
                    started,
                    cleanup_id=cycle.cleanup_id.hex(),
                    target_kind=cycle.target_kind.value,
                    phase=results[-1].phase,
                    row_count=rows,
                    cycle_complete=results[-1].cycle_complete,
                )
                if results[-1].cycle_complete:
                    break
            with connector.transaction():
                if _root_count(connector, cycle) or connector.fetch_one(
                    "SELECT 1 FROM operational_cleanup_checkpoints "
                    "WHERE cleanup_id = %s LIMIT 1",
                    (cycle.cleanup_id,),
                ):
                    raise WorkerError("completed_cycle_retained_frontier")
                work = VNextUnitOfWork(connector, backend=backend)
                cycle = _existing_affected_cycle(work)
        summary["status"] = "complete"
        return summary
    except (CleanupRetentionBlockedError, MaintenanceGateUnavailableError) as error:
        summary["status"] = "blocked"
        summary["blocker"] = type(error).__name__
        return summary
    finally:
        if lease is not None:
            with connector.transaction():
                work = VNextUnitOfWork(connector, backend=backend)
                renewal = MaintenanceGateRepository.lock_for_renewal(work, lease)
                renewal.release(now=database_unix_microseconds(work))


def finish(config: CoreConfig, *, owner_fd: int) -> dict[str, Any]:
    _require_owner(owner_fd)
    if config.database.access_mode is not DatabaseAccessMode.read_write:
        raise WorkerError("cleanup_requires_read_write_configuration")
    started = perf_counter()
    context = RepositoryContext.from_config(
        config.model_copy(
            update={"logger": config.logger.model_copy(update={"file": None})}
        )
    )
    try:
        with context.SQLConnector() as connector:
            with _schema_gate(connector, context.sql_type):
                with connector.transaction():
                    _require_schema8(connector, context.sql_type)
                result = _drain(
                    connector,
                    backend=context.sql_type,
                    owner_fd=owner_fd,
                    started=started,
                )
                if result["status"] != "blocked":
                    with connector.transaction():
                        result.update(
                            _completed_gid_job_proof(connector, owner_fd=owner_fd)
                        )
                result["elapsed_seconds"] = round(perf_counter() - started, 3)
                return result
    finally:
        context.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--consumers-stopped", action="store_true", required=True)
    parser.add_argument("--owner-fd", type=int, required=True)
    args = parser.parse_args()
    try:
        result = finish(load_config(args.config), owner_fd=args.owner_fd)
    except Exception as error:
        result = {"status": "error", "error": type(error).__name__}
        if isinstance(error, WorkerError):
            result["code"] = str(error)
        print(json.dumps(result, separators=(",", ":")), flush=True)
        raise SystemExit(2) from None
    print(json.dumps(result, separators=(",", ":")), flush=True)
    raise SystemExit(1 if result["status"] == "blocked" else 0)


if __name__ == "__main__":
    main()
