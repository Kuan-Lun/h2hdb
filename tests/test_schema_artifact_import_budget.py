from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import TypedDict, cast

import pytest

ROOT = Path(__file__).resolve().parents[1]
GENERATED_LOADER = ROOT / "src" / "h2hdb" / "_generated_vnext_schema.py"
GENERATED_RESOURCE = ROOT / "src" / "h2hdb" / "_generated_vnext_schema.bin"
MAX_IMPORT_RSS_BYTES = 256 * 1024 * 1024
MAX_IMPORT_ELAPSED_SECONDS = 10.0


class _ProbeResult(TypedDict):
    elapsed: float
    max_rss: int


_PROBES = {
    "package": """
import sys
import h2hdb
assert "h2hdb._generated_vnext_schema" not in sys.modules
""",
    "artifact": """
from h2hdb._generated_vnext_schema import ARTIFACT
assert ARTIFACT["epoch"] == 3
assert ARTIFACT["backends"]["sqlite"]["relations"]
""",
    "provider": """
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider
provider = GeneratedVNextSchemaProvider("sqlite")
assert provider.generated_definition_data["relations"]
""",
}


def _rss_bytes(value: int) -> int:
    return value if sys.platform == "darwin" else value * 1024


def _run_probe(
    *, source: str, cache: Path | None, dont_write_bytecode: bool
) -> _ProbeResult:
    environment = os.environ.copy()
    environment["PYTHONHASHSEED"] = "0"
    environment["PYTHONPATH"] = str(ROOT / "src")
    if cache is None:
        environment.pop("PYTHONPYCACHEPREFIX", None)
    else:
        environment["PYTHONPYCACHEPREFIX"] = str(cache)
    if dont_write_bytecode:
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
    else:
        environment.pop("PYTHONDONTWRITEBYTECODE", None)
    wrapper = f"""
import json
import resource
import time
started = time.perf_counter()
{source}
print(json.dumps({{
    "elapsed": time.perf_counter() - started,
    "max_rss": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
}}))
"""
    completed = subprocess.run(
        [sys.executable, "-c", wrapper],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    result = cast(_ProbeResult, json.loads(completed.stdout))
    result["max_rss"] = _rss_bytes(result["max_rss"])
    return result


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="the stdlib resource.RUSAGE_SELF import budget probe is POSIX-only",
)
@pytest.mark.parametrize("probe_name", tuple(_PROBES))
def test_fresh_imports_stay_within_broad_resource_budget(
    probe_name: str, tmp_path: Path
) -> None:
    source = _PROBES[probe_name]
    cache = tmp_path / "pycache"

    cold = _run_probe(source=source, cache=cache, dont_write_bytecode=False)
    warm = _run_probe(source=source, cache=cache, dont_write_bytecode=False)
    no_bytecode = _run_probe(
        source=source,
        cache=tmp_path / "no-bytecode-pycache",
        dont_write_bytecode=True,
    )

    for result in (cold, warm, no_bytecode):
        assert result["max_rss"] < MAX_IMPORT_RSS_BYTES
        assert result["elapsed"] < MAX_IMPORT_ELAPSED_SECONDS


def test_generated_loader_and_resource_have_hard_distribution_size_caps() -> None:
    assert GENERATED_LOADER.stat().st_size < 1024 * 1024
    assert GENERATED_RESOURCE.stat().st_size < 8 * 1024 * 1024
