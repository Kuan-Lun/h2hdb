"""The isolated historical worker advances retained work, never new cycles."""

from __future__ import annotations

import json
import os
import runpy
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).parents[1] / "scripts/finish-schema8-cleanup.py"
_CHILD = r"""
import json, os, pathlib, runpy, sys
sys.path.insert(0, sys.argv[1])
from h2hdb import CoreConfig, VNextDatabaseAdminFacade
from h2hdb.repository import RepositoryContext
from h2hdb.vnext_cleanup_repository import CleanupTargetKind, VNextCleanupRepository
from h2hdb.vnext_maintenance_gate_repository import MaintenanceGateRepository
from h2hdb.vnext_transaction import VNextUnitOfWork
from h2hdb.vnext_identity import publication_key
module = runpy.run_path(sys.argv[2])
config = CoreConfig.model_validate({"database": {"sql_type": "sqlite", "database": sys.argv[3]}})
VNextDatabaseAdminFacade(config).initialize()
context = RepositoryContext.from_config(config)
case = sys.argv[4]
with context.SQLConnector() as connector:
    with connector.transaction():
        connector.execute_many("INSERT INTO catalog_gallery_upload_times VALUES (%s, %s)", [(23, 123), (279, 124), (24, 125)])
    if case != "none":
        with connector.transaction():
            work = VNextUnitOfWork(connector, backend="sqlite")
            lease = MaintenanceGateRepository.claim_exclusive(work, now=1, lease_duration=100000)
        with connector.transaction():
            cycle = VNextCleanupRepository.begin_cycle(
                VNextUnitOfWork(connector, backend="sqlite"), gate_lease=lease,
                target_kind=CleanupTargetKind.GALLERY_UPLOAD_TIME, shard_no=23,
                cycle_cutoff_at=100, max_rows_per_transaction=1, now=2)
        with connector.transaction():
            MaintenanceGateRepository.release(VNextUnitOfWork(connector, backend="sqlite"), lease, now=3)
        if case == "blocked":
            with connector.transaction():
                connector.execute("INSERT INTO catalog_publication_identities VALUES (%s, %s)", (publication_key(23), 23))
    before = connector.fetch_all("SELECT target_key, cycle_generation FROM operational_cleanup_jobs ORDER BY target_key")
context.close()
read_fd, write_fd = os.pipe()
try:
    if case == "configured_log_file":
        unwanted_log = pathlib.Path(sys.argv[3]).parent / "missing-log-parent" / "core.log"
        config = config.model_copy(update={"logger": config.logger.model_copy(update={"file": unwanted_log})})
    if case.startswith("completed_"):
        module["finish"](config, owner_fd=read_fd)
        context = RepositoryContext.from_config(config)
        with context.SQLConnector() as connector:
            changes = {
                "completed_bad_id": ("cleanup_id", b"z" * 16),
                "completed_bad_chain": ("final_chain_sha256", b"short"),
                "completed_bad_count": ("final_deleted_count", -1),
                "completed_bad_created": ("created_at", -1),
                "completed_bad_algorithm": ("algorithm_version", 3),
            }
            if case in changes:
                column, value = changes[case]
                connector.execute("PRAGMA ignore_check_constraints = ON")
                with connector.transaction():
                    connector.execute(f"UPDATE operational_cleanup_jobs SET {column} = %s", (value,))
            if case == "completed_retained_root":
                connector.execute("INSERT INTO operational_cleanup_cycle_roots VALUES (%s, %s)", (cycle.cleanup_id, b"retained"))
            if case == "completed_retained_checkpoint":
                connector.execute("INSERT INTO operational_cleanup_checkpoints (cleanup_id, phase, generation, cursor_bytes, deleted_count, chain_sha256, state, updated_at) VALUES (%s, 'GUT_ROOT', 1, %s, 0, %s, 'OPEN', 999)", (cycle.cleanup_id, b"", b"q" * 32))
            protected_before = connector.fetch_all("SELECT * FROM operational_cleanup_jobs")
        context.close()
    if case == "owner_dead":
        os.close(write_fd)
        write_fd = None
    if case in {"interrupted", "mid_owner_dead"}:
        original_progress = module["_drain"].__globals__["_progress"]
        def interrupt(event, started, **fields):
            global write_fd
            original_progress(event, started, **fields)
            if event == "cleanup_batch_committed":
                if case == "mid_owner_dead":
                    os.close(write_fd)
                    write_fd = None
                    return
                raise RuntimeError("injected committed-batch response loss")
        module["_drain"].__globals__["_progress"] = interrupt
    try:
        first = module["finish"](config, owner_fd=read_fd)
    except Exception as error:
        first = {"error": type(error).__name__, "code": str(error)}
    if case == "configured_log_file":
        assert not unwanted_log.exists()
        assert not unwanted_log.parent.exists()
        config = config.model_copy(update={"logger": config.logger.model_copy(update={"file": None})})
    if case == "interrupted":
        module["_drain"].__globals__["_progress"] = original_progress
        second = module["finish"](config, owner_fd=read_fd)
    else:
        second = None
finally:
    os.close(read_fd)
    if write_fd is not None:
        os.close(write_fd)
context = RepositoryContext.from_config(config)
with context.SQLConnector() as connector:
    remaining = connector.fetch_all("SELECT gid FROM catalog_gallery_upload_times ORDER BY gid")
    after = connector.fetch_all("SELECT target_key, cycle_generation FROM operational_cleanup_jobs ORDER BY target_key")
    states = connector.fetch_all("SELECT state FROM operational_cleanup_jobs ORDER BY target_key")
    roots = connector.fetch_all("SELECT frozen_root_key FROM operational_cleanup_cycle_roots")
    if case.startswith("completed_"):
        assert connector.fetch_all("SELECT * FROM operational_cleanup_jobs") == protected_before
context.close()
assert before == after, (before, after)
print(json.dumps({"first": first, "second": second, "remaining": remaining, "states": states, "roots": len(roots)}))
"""


def test_worker_rejects_dead_or_invalid_owner_before_database_access() -> None:
    module = runpy.run_path(str(_SCRIPT))
    if os.name != "posix":
        with pytest.raises(
            module["WorkerError"], match="worker_requires_posix_use_docker"
        ):
            module["_require_owner"](0)
        return
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    try:
        with pytest.raises(module["WorkerError"], match="converter_owner_exited"):
            module["_require_owner"](read_fd)
    finally:
        os.close(read_fd)
    with pytest.raises(module["WorkerError"], match="invalid_owner_pipe"):
        module["_require_owner"](0)


@pytest.mark.deep
@pytest.mark.skipif(
    os.name != "posix",
    reason="Historical offline worker requires POSIX or its Docker bundle",
)
@pytest.mark.parametrize(
    "case",
    (
        "none",
        "complete",
        "owner_dead",
        "interrupted",
        "blocked",
        "mid_owner_dead",
        "completed_valid",
        "completed_bad_id",
        "completed_bad_chain",
        "completed_bad_count",
        "completed_bad_created",
        "completed_bad_algorithm",
        "completed_retained_root",
        "completed_retained_checkpoint",
        "configured_log_file",
    ),
)
def test_isolated_schema8_worker_preserves_unselected_roots_and_cycle_generation(
    tmp_path: Path, case: str
) -> None:
    wheel = os.environ.get("H2HDB_SCHEMA8_WHEEL")
    if wheel is None:
        pytest.skip("Requires explicitly supplied historical Core 0.42.2 wheel")
    completed = subprocess.run(
        (
            sys.executable,
            "-I",
            "-B",
            "-c",
            _CHILD,
            str(Path(wheel).resolve()),
            str(_SCRIPT),
            str(tmp_path / "schema8.sqlite3"),
            case,
        ),
        check=True,
        text=True,
        capture_output=True,
        timeout=60,
    )
    result = json.loads(completed.stdout)
    if case.startswith("completed_"):
        assert result["remaining"] == [[24], [279]]
        assert result["states"] == [["COMPLETE"]]
        assert result["roots"] == (1 if case == "completed_retained_root" else 0)
        if case == "completed_valid":
            assert result["first"]["status"] == "no_affected_open_cycle"
            assert result["first"]["completed_gid_job_count"] == 1
            assert len(result["first"]["completed_gid_jobs_sha256"]) == 64
        else:
            assert "error" in result["first"]
    elif case in {"none", "owner_dead", "blocked"}:
        assert result["remaining"] == [[23], [24], [279]]
        if case == "none":
            assert result["first"]["status"] == "no_affected_open_cycle"
            assert result["first"]["completed_gid_job_count"] == 0
            assert result["states"] == []
        elif case == "owner_dead":
            assert result["first"]["code"] == "converter_owner_exited"
            assert result["states"] == [["OPEN"]]
        else:
            assert result["first"]["status"] == "blocked"
            assert result["first"]["blocker"] == "CleanupRetentionBlockedError"
            assert result["states"] == [["OPEN"]]
    elif case == "mid_owner_dead":
        assert result["first"]["code"] == "converter_owner_exited"
        assert result["remaining"] == [[24], [279]]
        assert result["states"] == [["OPEN"]]
        assert result["roots"] == 1
    else:
        # Only the retained frozen root is processed. Same-shard 279 and another
        # shard 24 remain eligible but the worker must never start a new cycle.
        assert result["remaining"] == [[24], [279]]
        assert result["states"] == [["COMPLETE"]]
        assert result["roots"] == 0
        if case in {"complete", "configured_log_file"}:
            assert result["first"]["status"] == "complete"
            assert result["first"]["rows_deleted"] == 1
            assert result["first"]["completed_gid_job_count"] == 1
        else:
            assert result["first"]["error"] == "RuntimeError"
            assert result["second"]["status"] == "complete"
            assert result["second"]["rows_deleted"] == 0
