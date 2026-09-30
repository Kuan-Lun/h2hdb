"""Runtime copied into the dev-only source calibration image by its exporter."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import platform
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ARMS = ("baseline", "candidate")
ROOT = Path(__file__).resolve().parent
RESULTS = Path("/results")
TIME_FIELDS = (
    "qualification_wall_ns",
    "complete_observation_wall_ns",
    "process_cpu_ns",
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError("Expected a JSON object")
    return value


def write_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + ".pending")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"
    )
    temporary.replace(path)


def verified_manifest(root: Path) -> dict[str, Any]:
    manifest = read_json(root / "manifest.json")
    if (
        manifest.get("schema") != 1
        or manifest.get("kind") != "synthetic-source-qualification-calibration"
    ):
        raise ValueError("Unknown bundle format")
    for name, expected in manifest["files"].items():
        path = root / name
        if (
            Path(name).is_absolute()
            or ".." in Path(name).parts
            or path.is_symlink()
            or sha256(path) != expected
        ):
            raise ValueError("Bundle input differs from its exported fingerprint")
    return manifest


def installed_evidence(root: Path, arm: str) -> dict[str, Any]:
    """Executed by the arm's isolated interpreter, never by the host Python."""
    manifest = verified_manifest(root)
    packages = {}
    for name, expected in (
        ("h2hdb", manifest["core"]),
        ("h2hdb-ingest", manifest["arms"][arm]["wheel"]),
    ):
        distribution = importlib.metadata.distribution(name)
        if distribution.version != expected["version"]:
            raise ValueError("Installed wheel version differs from the bundle")
        module = name.replace("-", "_")
        package = Path(str(distribution.locate_file(module)))
        actual = {
            p.relative_to(package).as_posix(): sha256(p)
            for p in sorted(package.rglob("*.py"))
        }
        if actual != expected["python_sources"]:
            raise ValueError("Installed runtime sources differ from the supplied wheel")
        direct = json.loads(distribution.read_text("direct_url.json") or "{}")
        if (
            direct.get("archive_info", {}).get("hashes", {}).get("sha256")
            != expected["sha256"]
        ):
            raise ValueError("Installed distribution lacks exact wheel provenance")
        packages[name] = {
            "version": distribution.version,
            "wheel_sha256": expected["sha256"],
            "python_sources": actual,
            "direct_url": direct,
        }
    return {
        "python": sys.version,
        "platform": platform.platform(),
        "packages": packages,
        "distributions": {
            str(d.metadata["Name"]).lower().replace("_", "-"): d.version
            for d in importlib.metadata.distributions()
        },
    }


def verify_build(root: Path) -> dict[str, Any]:
    verified_manifest(root)
    evidence = {}
    for arm in ARMS:
        result = subprocess.run(
            [
                f"/opt/{arm}/bin/python",
                "-I",
                str(root / "runtime.py"),
                "inspect-arm",
                "--arm",
                arm,
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
        evidence[arm] = json.loads(result.stdout)
        resolution = read_json(root / "installed" / f"{arm}-resolution.json")
        evidence[arm]["pip_resolution"] = resolution
    dependencies = [
        {k: v for k, v in evidence[arm]["distributions"].items() if k != "h2hdb-ingest"}
        for arm in ARMS
    ]
    if dependencies[0] != dependencies[1]:
        raise ValueError("Comparison arms resolved different dependencies")
    write_json(root / "installed" / "verified.json", evidence)
    return evidence


def compare(reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Require matched fixture bytes before describing observed timing changes."""
    grouped: dict[tuple[str, int], dict[str, list[dict[str, Any]]]] = {}
    fingerprints: dict[tuple[str, int], str] = {}
    expected_cases = {
        (f"{codec}-{edge}", workers)
        for codec in ("JPEG", "PNG", "GIF", "WEBP")
        for edge in (256, 1536)
        for workers in (1, 4)
    }
    counts = dict.fromkeys(ARMS, 0)
    for run in reports:
        arm, report = run["arm"], run["report"]
        if arm not in ARMS or report.get("status") != "completed":
            raise ValueError("Incomplete calibration run")
        counts[arm] += 1
        seen = set()
        for case in report["cases"]:
            key = (case["label"], case["workers"])
            if (
                key not in expected_cases
                or key in seen
                or case.get("attribution_status") != "complete"
            ):
                raise ValueError(
                    "Calibration case identity or attribution is incomplete"
                )
            seen.add(key)
            fingerprint = case["fixture"]["manifest_sha256"]
            if fingerprints.setdefault(key, fingerprint) != fingerprint:
                raise ValueError("Baseline/candidate fixture bytes differ")
            if any(
                type(case[field]) is not int or case[field] <= 0
                for field in TIME_FIELDS
            ):
                raise ValueError("Missing positive calibration timing")
            grouped.setdefault(key, {name: [] for name in ARMS})[arm].append(case)
        if seen != expected_cases:
            raise ValueError("Calibration matrix omits expected fixture dimensions")
    if counts != dict.fromkeys(ARMS, 2):
        raise ValueError("Calibration needs two completed observations per arm")
    summary = []
    for (label, workers), arms in sorted(grouped.items()):
        metrics = {}
        for field in TIME_FIELDS:
            values = {arm: [case[field] for case in arms[arm]] for arm in ARMS}
            medians = {arm: statistics.median(values[arm]) for arm in ARMS}
            metrics[field] = {
                "samples": values,
                "median": medians,
                "candidate_change_fraction": medians["candidate"] / medians["baseline"]
                - 1,
            }
        summary.append(
            {
                "label": label,
                "workers": workers,
                "fixture_sha256": fingerprints[(label, workers)],
                "measurements": metrics,
            }
        )
    return summary


def run(root: Path, results: Path) -> int:
    manifest = verified_manifest(root)
    report: dict[str, Any] = {
        "schema": 1,
        "status": "incomplete",
        "started_utc": datetime.now(UTC).isoformat(),
        "manifest": manifest,
        "installed": read_json(root / "installed" / "verified.json"),
        "scope": manifest["scope"],
        "acceptance": "not_assessed",
        "runs": [],
    }
    runs = []
    try:
        for index, arm in enumerate(manifest["run_order"]):
            destination = results / f"{index + 1}-{arm}.json"
            command = [
                f"/opt/{arm}/bin/python",
                "-I",
                str(root / arm / "scripts" / "probe-qualification-phases.py"),
                "--output",
                str(destination),
                "--timeout-seconds",
                str(manifest["phase_timeout_seconds"]),
            ]
            started = time.monotonic()
            with (results / f"{index + 1}-{arm}.log").open("w") as log:
                completed = subprocess.run(
                    command,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    timeout=manifest["phase_timeout_seconds"] + 30,
                    check=False,
                )
            report["runs"].append(
                {
                    "arm": arm,
                    "command": command,
                    "returncode": completed.returncode,
                    "supervised_seconds": time.monotonic() - started,
                    "report": destination.name,
                }
            )
            if completed.returncode != 0:
                raise ValueError(
                    f"Qualification probe failed for {arm}; see retained report/log"
                )
            measured = read_json(destination)
            runs.append({"arm": arm, "report": measured})
            write_json(results / "summary.json", report)
        report["comparison"] = compare(runs)
        report["status"] = "completed"
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        subprocess.SubprocessError,
    ) as error:
        report["error"] = f"{type(error).__name__}: {error}"
    finally:
        report["finished_utc"] = datetime.now(UTC).isoformat()
        write_json(results / "summary.json", report)
    return 0 if report["status"] == "completed" else 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("verify-build", "inspect-arm", "run"))
    parser.add_argument("--arm", choices=ARMS)
    args = parser.parse_args()
    if args.operation == "verify-build":
        verify_build(ROOT)
    elif args.operation == "inspect-arm":
        if args.arm is None:
            parser.error("inspect-arm requires --arm")
        print(json.dumps(installed_evidence(ROOT, args.arm)))
    else:
        return run(ROOT, RESULTS)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
