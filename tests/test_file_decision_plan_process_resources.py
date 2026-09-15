"""A killed local preparation must not leave named scratch for restart cleanup."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

_CHILD = r"""
import json
import os
import signal
import sys
from pathlib import Path

from h2hdb import vnext_analysis_repository as repository
from h2hdb import vnext_file_decision_validation_plan as plans

phase, scratch, ready = sys.argv[1:]
original_connect = plans.sqlite3.connect
connections = []

def connect(*args, **kwargs):
    connection = original_connect(*args, **kwargs)
    connections.append(connection)
    return connection

plans.sqlite3.connect = connect
authority = repository.AnalysisPreparationAuthority(
    b'a' * 16, b'b' * 16, 1, 1, b'm' * 32, (), repository._PREPARATION_TOKEN
)

def pause(evidence):
    evidence['named_scratch'] = sorted(path.name for path in Path(scratch).iterdir())
    staging = Path(ready).with_suffix('.staging')
    staging.write_text(json.dumps(evidence))
    staging.replace(ready)
    signal.pause()
    raise RuntimeError('child unexpectedly resumed')

def galleries():
    yield plans.FileDecisionSourceGallery(
        1, 1, (1,), ((value.to_bytes(32, 'big'), 1) for value in range(65536))
    )
    if phase == 'building':
        connection = connections[0]
        size = connection.execute('PRAGMA page_count').fetchone()[0]
        size *= connection.execute('PRAGMA page_size').fetchone()[0]
        cache_kib = -connection.execute('PRAGMA cache_size').fetchone()[0]
        pause({
            'database_bytes': size,
            'cache_bytes': cache_kib * 1024,
            'database_path': connection.execute('PRAGMA database_list').fetchone()[2],
        })

plan = plans.build_file_decision_validation_plan(authority, galleries())
pause({
    'row_count': plan.row_count,
    'payload_links': os.fstat(plan._payload.fileno()).st_nlink,
    'payload_bytes': os.fstat(plan._payload.fileno()).st_size,
})
"""


@pytest.mark.skipif(os.name != "posix", reason="SIGKILL and unlink evidence are POSIX")
@pytest.mark.parametrize("phase", ["building", "retained"])
def test_sigkill_discards_sort_and_retained_plan_without_restart_cleanup(
    tmp_path: Path, phase: str
) -> None:
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    ready = tmp_path / "ready.json"
    environment = {
        **os.environ,
        "SQLITE_TMPDIR": str(scratch),
        "TMPDIR": str(scratch),
        "TEMP": str(scratch),
        "TMP": str(scratch),
    }
    with (tmp_path / "child.log").open("w+") as output:
        child = subprocess.Popen(
            [sys.executable, "-c", _CHILD, phase, str(scratch), str(ready)],
            env=environment,
            stdout=output,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 15
            while not ready.exists() and child.poll() is None:
                assert time.monotonic() < deadline, (
                    "preparation never reached kill point"
                )
                time.sleep(0.02)
            output.seek(0)
            assert ready.exists(), output.read()
            evidence = json.loads(ready.read_text())
            assert evidence["named_scratch"] == []
            if phase == "building":
                assert evidence["database_bytes"] > evidence["cache_bytes"]
                assert evidence["database_path"] == ""
            else:
                assert evidence["row_count"] == 65536
                assert evidence["payload_links"] == 0
                assert evidence["payload_bytes"] == 65536 * 88
            child.kill()
            assert child.wait(timeout=5) == -signal.SIGKILL
            assert list(scratch.iterdir()) == []
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
