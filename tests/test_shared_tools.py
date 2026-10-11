from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.skipif(
    os.name != "posix", reason="The repository shell gate is POSIX-only"
)
@pytest.mark.parametrize("damage", ("modified", "missing", "unexpected"))
def test_fast_gate_rejects_damaged_snapshot_before_quality_tools(
    tmp_path: Path, damage: str
) -> None:
    repository = tmp_path / "checkout"
    scripts = repository / "scripts"
    scripts.mkdir(parents=True)
    shutil.copy2(_ROOT / "scripts/check-fast.sh", scripts / "check-fast.sh")
    shutil.copytree(_ROOT / "scripts/_shared", scripts / "_shared")
    bundle = scripts / "_shared/h2h_dev_tools"
    if damage == "modified":
        with (bundle / "dependency_manifest.py").open("a", encoding="utf-8") as stream:
            stream.write("\nraise AssertionError('modified snapshot must not run')\n")
    elif damage == "missing":
        (bundle / "dependency_manifest.py").unlink()
    else:
        (bundle / "unexpected.py").write_text("unexpected = True\n", encoding="utf-8")
    marker = repository / "quality-tool-started"
    executables = {
        ".venv/bin/python": f'exec {shlex.quote(sys.executable)} "$@"\n',
        ".venv/bin/ruff": f"touch {shlex.quote(str(marker))}\n",
        ".venv/bin/mypy": f"touch {shlex.quote(str(marker))}\n",
        "node_modules/.bin/markdownlint-cli2": f"touch {shlex.quote(str(marker))}\n",
    }
    for name, body in executables.items():
        path = repository / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("#!/usr/bin/env bash\n" + body, encoding="utf-8")
        path.chmod(0o755)
    environment = {
        name: value for name, value in os.environ.items() if not name.startswith("GIT_")
    }
    environment.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
    subprocess.run(
        ("git", "init", "-b", "main"),
        cwd=repository,
        env=environment,
        capture_output=True,
        check=True,
        timeout=30,
    )

    result = subprocess.run(
        ("bash", "scripts/check-fast.sh"),
        cwd=repository,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )

    assert result.returncode != 0
    assert "h2h-dev-tools" in result.stderr
    assert not marker.exists()
