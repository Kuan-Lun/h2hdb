#!/usr/bin/env python3
"""Export a build-first, synthetic Ingest qualification calibration bundle.

This dev-only exporter never starts Docker, accesses a database, or copies a
gallery. Explicit wheels are checked against explicit runtime checkouts. The
same probe source measures both arms; no repository metadata is manufactured.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import tomllib
import zipfile
from email.parser import BytesParser
from pathlib import Path
from typing import Any

PROBES = (
    "probe-qualification-phases.py",
    "check-source-cost.py",
    "probe-source-io.py",
    "_probe_environment.py",
    # Imported only for its bounded process ownership primitives, not pytest.
    "run-pytest.py",
)
ARMS = ("baseline", "candidate")
RUN_ORDER = ("baseline", "candidate", "candidate", "baseline")
PHASE_SECONDS = 300
CONTAINER_SECONDS = 4 * (PHASE_SECONDS + 30) + 60
RUNTIME = Path(__file__).with_name("source_performance_bundle.py")


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def regular_bytes(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"Expected a regular, nonsymlink input: {path}")
    return path.read_bytes()


def wheel_inputs(path: Path, name: str) -> tuple[dict[str, Any], dict[str, bytes]]:
    data = regular_bytes(path)
    if not re.fullmatch(r"[A-Za-z0-9_.+-]+\.whl", path.name):
        raise ValueError("Wheel filename must be a simple .whl basename")
    package = name.replace("-", "_")
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            raise ValueError("Wheel has duplicate members")
        metadata_paths = [n for n in names if n.endswith(".dist-info/METADATA")]
        if len(metadata_paths) != 1:
            raise ValueError("Wheel must contain one distribution metadata record")
        metadata = BytesParser().parsebytes(archive.read(metadata_paths[0]))
        version = str(metadata.get("Version", ""))
        if canonical(str(metadata.get("Name", ""))) != name or not version:
            raise ValueError(f"Wheel must describe {name}")
        sources = {
            n.removeprefix(package + "/"): archive.read(n)
            for n in names
            if n.startswith(package + "/") and n.endswith(".py")
        }
        if not sources or any(
            str(Path(n)) != n or ".." in Path(n).parts for n in sources
        ):
            raise ValueError("Wheel Python source inventory is empty or malformed")
    return (
        {
            "name": name,
            "version": version,
            "filename": path.name,
            "sha256": digest(data),
            "requires_dist": metadata.get_all("Requires-Dist", []),
            "python_sources": {n: digest(b) for n, b in sorted(sources.items())},
        },
        sources,
    )


def checkout_inputs(root: Path, sources: dict[str, bytes], version: str) -> bytes:
    manifest = regular_bytes(root / "pyproject.toml")
    project = tomllib.loads(manifest.decode())["project"]
    if canonical(project["name"]) != "h2hdb-ingest" or project["version"] != version:
        raise ValueError("Ingest wheel and checkout project identity differ")
    package = root / "src" / "h2hdb_ingest"
    files = {
        p.relative_to(package).as_posix(): regular_bytes(p)
        for p in sorted(package.rglob("*.py"))
    }
    if files != sources:
        raise ValueError(
            "Ingest wheel Python sources differ from the supplied checkout"
        )
    return manifest


def checkout_revision(root: Path) -> dict[str, Any]:
    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(root), *args],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        ).stdout.strip()

    try:
        if Path(git("rev-parse", "--show-toplevel")).resolve() != root.resolve():
            return {
                "commit": None,
                "dirty": None,
                "scope": "source export nested inside another Git checkout",
            }
        return {
            "commit": git("rev-parse", "HEAD"),
            "dirty": bool(git("status", "--porcelain")),
        }
    except OSError, subprocess.SubprocessError:
        return {
            "commit": None,
            "dirty": None,
            "scope": "source export without Git metadata",
        }


def dockerfile(arms: dict[str, Any], core: dict[str, Any]) -> str:
    lines = [
        "ARG PYTHON_IMAGE=python:3.14",
        "FROM ${PYTHON_IMAGE}",
        "ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1",
        "COPY payload /opt/bundle",
        "RUN mkdir /opt/bundle/installed",
    ]
    for arm in ARMS:
        wheel = arms[arm]["wheel"]["filename"]
        lines.append(
            f"RUN python -m venv /opt/{arm}"
            f" && /opt/{arm}/bin/python -m pip install --no-cache-dir"
            f" --report /opt/bundle/installed/{arm}-resolution.json"
            f" /opt/bundle/wheels/{core['filename']}"
            f" /opt/bundle/{arm}/wheels/{wheel}"
            f" && /opt/{arm}/bin/python -m pip check"
        )
    lines.extend(
        (
            "RUN python /opt/bundle/runtime.py verify-build",
            "RUN command -v timeout && chmod -R a+rX /opt/bundle /opt/baseline /opt/candidate",
            'ENTRYPOINT ["timeout", "--signal=TERM", "--kill-after=10s", '
            f'"{CONTAINER_SECONDS}s", "python", "/opt/bundle/runtime.py", "run"]',
            "",
        )
    )
    return "\n".join(lines)


def run_script(identity: str) -> str:
    return f"""#!/bin/sh
# Always build a standalone image, then create a fresh synthetic-only container.
set -eu
cd -- "$(dirname -- "$0")"
if [ "$#" -ne 1 ]; then
    echo "Usage: sh run.sh NEW_RESULT_DIRECTORY" >&2
    exit 2
fi
mkdir -- "$1"
result=$(cd -- "$1" && pwd -P)
mkdir "$result/scratch" "$result/reports"
image=h2hdb-source-calibration:{identity}
cleanup() {{
    if [ -f "$result/container.id" ]; then
        cid=$(cat "$result/container.id")
        case "$cid" in
            ''|*[!0-9a-f]*) ;;
            *) docker rm -f "$cid" >/dev/null 2>&1 || true ;;
        esac
    fi
    rm -rf -- "$result/scratch"
}}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP
docker build --pull --no-cache --tag "$image" .
docker image inspect --format '{{{{json .}}}}' "$image" > "$result/image.json"
docker run --rm --init --pull=never --network none --read-only \\
    --cap-drop ALL --security-opt no-new-privileges \\
    --user "$(id -u):$(id -g)" --cidfile "$result/container.id" \\
    --mount "type=bind,src=$result/scratch,dst=/tmp" \\
    --mount "type=bind,src=$result/reports,dst=/results" "$image"
"""


def build_bundle(
    *,
    output: Path,
    baseline_checkout: Path,
    baseline_wheel: Path,
    candidate_checkout: Path,
    candidate_wheel: Path,
    core_wheel: Path,
    probe_checkout: Path,
) -> dict[str, Any]:
    """Validate all inputs before creating a new, credential-free build context."""
    if output.exists() or output.is_symlink():
        raise ValueError("Output must be a new directory")
    core, _ = wheel_inputs(core_wheel, "h2hdb")
    probes = {name: regular_bytes(probe_checkout / "scripts" / name) for name in PROBES}
    arms: dict[str, Any] = {}
    contents: dict[str, bytes] = {
        "runtime.py": regular_bytes(RUNTIME),
        f"wheels/{core_wheel.name}": regular_bytes(core_wheel),
    }
    for arm, checkout, wheel in (
        ("baseline", baseline_checkout, baseline_wheel),
        ("candidate", candidate_checkout, candidate_wheel),
    ):
        metadata, sources = wheel_inputs(wheel, "h2hdb-ingest")
        project = checkout_inputs(checkout, sources, metadata["version"])
        arms[arm] = {"wheel": metadata, "source_checkout": checkout_revision(checkout)}
        contents[f"{arm}/pyproject.toml"] = project
        contents[f"{arm}/wheels/{wheel.name}"] = regular_bytes(wheel)
        contents.update({f"{arm}/src/h2hdb_ingest/{n}": b for n, b in sources.items()})
        contents.update({f"{arm}/scripts/{n}": b for n, b in probes.items()})
    manifest = {
        "schema": 1,
        "kind": "synthetic-source-qualification-calibration",
        "core": core,
        "arms": arms,
        "probe_checkout": checkout_revision(probe_checkout),
        "probe_files": {n: digest(b) for n, b in probes.items()},
        "files": {n: digest(b) for n, b in sorted(contents.items())},
        "run_order": RUN_ORDER,
        "phase_timeout_seconds": PHASE_SECONDS,
        "container_timeout_seconds": CONTAINER_SECONDS,
        "scope": {
            "fixture": "Fixed seed 47029; JPEG/PNG/GIF/WEBP, 256/1536 edges, four pages, 1/4 workers; identical encoded manifests required across arms and repetitions",
            "probe": "Same qualification attribution probe in both arms; measures complete source observation including FILE receipts",
            "git": "Original revisions are export metadata only; no .git is copied or invented",
            "limits": "Synthetic warm-cache calibration only; no database, production source, full ingest, CBZ, Compose deployment, physical disk throughput or whole-library SLA",
            "pytest": "No pytest installed or executed; run-pytest.py is an imported process ownership helper",
            "comparison": "Observed timings only; instrumentation overhead, benefits and whole-job targets are not established by successful execution",
        },
    }
    encoded = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    output.mkdir(parents=True)
    try:
        for name, value in contents.items():
            target = output / "payload" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(value)
        (output / "payload" / "manifest.json").write_bytes(encoded)
        (output / "Dockerfile").write_text(dockerfile(arms, core))
        (output / ".dockerignore").write_text(
            "*\n!Dockerfile\n!payload/\n!payload/**\n"
        )
        (output / "run.sh").write_text(run_script(digest(encoded)[:16]))
        (output / "run.sh").chmod(0o755)
    except BaseException:
        shutil.rmtree(output)
        raise
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "output",
        "baseline-checkout",
        "baseline-wheel",
        "candidate-checkout",
        "candidate-wheel",
        "core-wheel",
        "probe-checkout",
    ):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    try:
        build_bundle(**vars(args))
    except (OSError, ValueError, KeyError, zipfile.BadZipFile) as error:
        parser.error(str(error))
    print(f"Bundle ready: {args.output}; run sh run.sh NEW_RESULT_DIRECTORY there")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
