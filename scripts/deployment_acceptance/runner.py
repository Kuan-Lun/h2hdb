"""Disposable Compose acceptance against local images; a POSIX host is required."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import secrets
import shutil
import sys
import tempfile
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .compose import (
    OWNER_LABEL,
    SERVICES,
    DatabaseCredentials,
    PreparedDeployment,
    prepare_deployment,
)
from .evidence import (
    EvidenceError,
    export_evidence,
    read_evidence_json,
    require_evidence_support,
)
from .execution import Commands
from .report import (
    assess_probe_measurement,
    read_probe_events,
    summarize_log,
    summarize_probe,
)


def write_json(path: Path, value: object) -> None:
    temporary = path.with_name(path.name + ".pending")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def compare_reuse(before: dict[str, Any], after: dict[str, Any]) -> None:
    """Unchanged files must retain identity as well as bytes, across restarts."""
    fields = (
        "key",
        "generation",
        "pages",
        "page_sha256",
        "thumbnail_key",
        "thumbnail_sha256",
        "sha256",
        "byte_length",
        "mtime_ns",
        "ctime_ns",
        "device",
        "inode",
    )
    previous = {row["gid"]: row for row in before["artifacts"]}
    current = {row["gid"]: row for row in after["artifacts"]}
    if previous.keys() != current.keys():
        raise AssertionError("Unchanged restart changed publication membership")
    for gid, old in previous.items():
        if any(old[field] != current[gid][field] for field in fields):
            raise AssertionError(f"Unchanged restart rewrote artifact for GID {gid}")


class Acceptance:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.project = "h2hdb-acceptance-" + secrets.token_hex(6)
        self.commands = Commands(
            context=args.context,
            output=args.output.resolve(),
            seconds=args.deadline_seconds,
        )
        self.root = Path(tempfile.mkdtemp(prefix=self.project + "-"))
        self.prepared: PreparedDeployment | None = None
        self.credentials = DatabaseCredentials(
            *(secrets.token_hex(16) for _ in range(3))
        )
        self.report: dict[str, Any] = {
            "schema": 1,
            "project": self.project,
            "fixture_root": str(self.root),
            "started_utc": datetime.now(UTC).isoformat(),
            "scenarios": [],
            "mode": "instrumented" if args.instrumented else "baseline",
            "limitations": [
                "Synthetic local files and Docker storage do not reproduce NAS latency or its full data distribution.",
                "Only ingest and OPDS roles are exercised; downloader and Komga are outside this run.",
                "No absolute production latency SLO was supplied; successful correctness checks do not imply acceptable performance.",
                "work_wall_seconds is observed scenario elapsed time including setup action and polling; it is an upper bound, not pure ingest CPU time.",
                "Successful oracle full READY audit and byte verification are separate; earlier oracle attempts are itemized in intermediate_oracle_seconds and can affect caches.",
            ],
        }

    @property
    def deployment(self) -> PreparedDeployment:
        if self.prepared is None:
            raise RuntimeError("Deployment has not been prepared")
        return self.prepared

    def compose(
        self, arguments: Sequence[str], *, check: bool = True, timeout: float = 60
    ) -> str:
        return self.commands.compose(
            self.project,
            self.deployment.compose_path,
            arguments,
            check=check,
            timeout=timeout,
        )

    def helper(self, arguments: Sequence[str], *, check: bool = True) -> str:
        return self.compose(
            [
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "--entrypoint",
                "env",
                "-e",
                "PYTHONPATH=/acceptance",
                "-e",
                "H2HDB_DATABASE_READER_USER=acceptance_reader",
                "-e",
                f"H2HDB_DATABASE_READER_PASSWORD={self.credentials.reader_password}",
                SERVICES["ingest"],
                "-u",
                "H2HDB_ACCEPTANCE_PROBE_DIR",
                "-u",
                "H2HDB_ACCEPTANCE_CONTROL_DIR",
                "python",
                "-m",
                "deployment_acceptance.fixture",
                *arguments,
            ],
            check=check,
            timeout=600,
        )

    def generate(self, count: int, gid: int, *, marker: str = "complete") -> None:
        # Generation occurs in an explicit supplied interpreter; ingest still sees a RO source mount.
        self.commands.run(
            [
                str(self.args.fixture_python),
                str(Path(__file__).with_name("fixture.py")),
                "generate",
                "--root",
                str(self.deployment.source_dir),
                "--count",
                str(count),
                "--start-gid",
                str(gid),
                "--pages",
                str(self.args.pages),
                "--profile",
                self.args.image_profile,
                "--marker",
                marker,
            ],
            timeout=600,
        )

    def change(self, gid: int, generation: int, marker: str) -> None:
        self.commands.run(
            [
                str(self.args.fixture_python),
                str(Path(__file__).with_name("fixture.py")),
                "change",
                "--root",
                str(self.deployment.source_dir),
                "--gid",
                str(gid),
                "--generation",
                str(generation),
                "--marker",
                marker,
            ],
            timeout=60,
        )

    def logs(self, since: str) -> str:
        return self.compose(
            ["logs", "--no-color", "--timestamps", "--since", since, SERVICES["ingest"]]
        )

    def statuses(self) -> list[dict[str, Any]]:
        result = self.compose(["ps", "--all", "--format", "json"])
        return [
            json.loads(line) for line in result.splitlines() if line.startswith("{")
        ]

    def wait(
        self, predicate: Callable[[], bool], description: str, *, seconds: float = 600
    ) -> None:
        deadline = min(time.monotonic() + seconds, self.commands.deadline - 45)
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(2)
        raise TimeoutError(f"Timed out waiting for {description}")

    def http(self, *, fenced: bool = False) -> dict[str, int]:
        code = """import json, urllib.request, urllib.error
result = {}
for path in ('/health', '/opds/v2'):
    try:
        with urllib.request.urlopen('http://127.0.0.1:8000' + path, timeout=5) as response:
            result[path] = response.status
            response.read()
    except urllib.error.HTTPError as error:
        result[path] = error.code
print(json.dumps(result))
"""
        response = self.compose(
            [
                "exec",
                "-T",
                SERVICES["opds"],
                "env",
                "-u",
                "H2HDB_ACCEPTANCE_PROBE_DIR",
                "-u",
                "H2HDB_ACCEPTANCE_CONTROL_DIR",
                "python",
                "-c",
                code,
            ]
        )
        result: dict[str, int] = json.loads(response)
        if result != {"/health": 200, "/opds/v2": 503 if fenced else 200}:
            raise AssertionError(f"Unexpected OPDS responses: {result}")
        return result

    def verify(self, name: str) -> dict[str, Any]:
        self.helper(
            [
                "verify",
                "--config",
                "/h2hdb-config/h2hdb-reader-config.json",
                "--render-config",
                "/h2hdb-config/h2hdb-ingest.json",
                "--source",
                "/hentai/download",
                "--library",
                "/hentai/library",
                "--expected-manifest",
                "/hentai/download/.acceptance-manifest.json",
                "--output",
                f"/acceptance-evidence/oracle-{name}.json",
            ]
        )
        filename = f"oracle-{name}.json"
        result = read_evidence_json(self.deployment.evidence_dir, filename)
        write_json(self.commands.output / filename, result)
        return result

    def verify_http_artifacts(self, oracle: dict[str, Any]) -> list[dict[str, Any]]:
        """Check both an existing and the highest-GID artifact through OPDS."""
        chosen = {
            row["gid"]: row for row in (oracle["artifacts"][0], oracle["artifacts"][-1])
        }
        reports = []
        for row in chosen.values():
            result = self.compose(
                [
                    "exec",
                    "-T",
                    SERVICES["opds"],
                    "env",
                    "-u",
                    "H2HDB_ACCEPTANCE_PROBE_DIR",
                    "-u",
                    "H2HDB_ACCEPTANCE_CONTROL_DIR",
                    "python",
                    "/acceptance/http_probe.py",
                    "--base-url",
                    "http://127.0.0.1:8000",
                    "--gid",
                    str(row["gid"]),
                    "--sha256",
                    row["sha256"],
                    "--size",
                    str(row["byte_length"]),
                    # Reserve time to report exhaustion before the outer
                    # command hard timeout, which also bounds blocking I/O.
                    "--deadline-seconds",
                    "45",
                ]
            )
            reports.append(json.loads(result))
        return reports

    def phase(
        self,
        name: str,
        action: Callable[[], object],
        *,
        require_analysis: bool = False,
        before: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        since = datetime.now(UTC).isoformat()
        started = time.monotonic()
        record: dict[str, Any] = {
            "name": name,
            "started_utc": since,
            "status": "running",
        }
        self.report["scenarios"].append(record)
        action()
        last_completion = 0
        oracle: dict[str, Any] | None = None

        def completed() -> bool:
            nonlocal last_completion, oracle
            log = self.logs(since)
            count = summarize_log(log)["completed_batches"]
            if "Traceback (most recent call last)" in log or "[ERROR]" in log:
                raise AssertionError(f"Runtime error during {name}; see command logs")
            if count <= last_completion:
                return False
            last_completion = count
            measured = time.monotonic() - started
            oracle_started = time.monotonic()
            try:
                oracle = self.verify(name)
            except RuntimeError as error:
                # A completed durable work may predate this source generation. Wait for a NEW completion.
                record.setdefault("intermediate_oracle_mismatches", []).append(
                    str(error)
                )
                record["intermediate_oracle_seconds"] = (
                    record.get("intermediate_oracle_seconds", 0.0)
                    + time.monotonic()
                    - oracle_started
                )
                return False
            record["work_wall_seconds"] = measured
            record["log"] = summarize_log(log)
            (self.commands.output / f"ingest-{name}.log").write_text(log)
            return True

        self.wait(
            completed,
            f"{name}: completed work and exact expected catalog",
            seconds=self.args.phase_seconds,
        )
        if oracle is None:
            raise AssertionError("No independent oracle evidence")
        if require_analysis and not record["log"]["real_analysis_observed"]:
            raise AssertionError(f"{name} did not exercise non-replayed analysis")
        if require_analysis and not record["log"]["target_analysis_observed"]:
            raise AssertionError(f"{name} did not exercise validate_file_hash_decision")
        if before is not None:
            compare_reuse(before, oracle)
            if record["log"]["cbz_render_operations"]:
                raise AssertionError("Unchanged restart rendered CBZs")
        self.wait(
            lambda: all(
                any(
                    row.get("Service") == service and row.get("Health") == "healthy"
                    for row in self.statuses()
                )
                for service in SERVICES.values()
            ),
            "original Compose healthchecks",
            seconds=180,
        )
        record.update(
            oracle={key: value for key, value in oracle.items() if key != "artifacts"},
            http=self.http(),
        )
        if self.args.http_artifacts:
            record["http_artifacts"] = self.verify_http_artifacts(oracle)
        record["status"] = "passed"
        write_json(self.commands.output / "report.json", self.report)
        print(
            json.dumps(
                {
                    key: record[key]
                    for key in ("name", "status", "work_wall_seconds", "oracle")
                }
            ),
            flush=True,
        )
        return oracle

    def fault(self, signal: str, gid: int) -> None:
        token = signal.lower() + "-" + secrets.token_hex(4)
        arm = self.deployment.control_dir / "arm.json"
        write_json(
            arm, {"operation": "library.commit_pending_installs", "token": token}
        )
        self.generate(1, gid)

        def reached() -> bool:
            events, _ = read_probe_events(
                self.deployment.evidence_dir.glob("probe-*.jsonl")
            )
            return any(
                event.get("event") == "fault_reached" and event.get("token") == token
                for event in events
            )

        self.wait(
            reached,
            f"durable activation fault gate before {signal}",
            seconds=self.args.phase_seconds,
        )
        arm.unlink()
        if not (
            self.deployment.library_dir / ".h2hdb-coordination" / "ACTIVATING"
        ).exists():
            raise AssertionError("Fault did not reach a fenced activation")
        evidence: dict[str, Any] = {
            "signal": signal,
            "token": token,
            "fenced_http": self.http(fenced=True),
        }
        container = next(
            row for row in self.statuses() if row.get("Service") == SERVICES["ingest"]
        )
        identity = container["ID"]

        def state() -> dict[str, Any]:
            result: dict[str, Any] = json.loads(
                self.commands.run(
                    [
                        *self.commands.docker,
                        "inspect",
                        "--format",
                        "{{json .State}}",
                        identity,
                    ]
                )
            )
            return result

        evidence["before"] = state()
        self.report.setdefault("faults", []).append(evidence)
        self.compose(["kill", "--signal", signal, SERVICES["ingest"]])
        if signal == "SIGTERM":
            (self.deployment.control_dir / f"release-{token}").touch()
            self.wait(
                lambda: any(
                    row.get("Service") == SERVICES["ingest"]
                    and row.get("State") == "exited"
                    for row in self.statuses()
                ),
                "graceful ingest exit",
                seconds=120,
            )
        evidence["terminated"] = state()
        if evidence["terminated"].get("Running") or evidence["terminated"].get(
            "ExitCode"
        ) != (0 if signal == "SIGTERM" else 137):
            raise AssertionError(
                f"Unexpected termination state after {signal}: {evidence['terminated']}"
            )
        # Compose kill may suppress auto-restart; explicit start retains the original restart policy.
        self.compose(["start", SERVICES["ingest"]])
        evidence["restarted"] = state()
        if not evidence["restarted"].get("Running") or evidence["restarted"].get(
            "StartedAt"
        ) == evidence["before"].get("StartedAt"):
            raise AssertionError("Fault test did not establish a new running process")

    def run(self) -> int:
        try:
            self.commands.assert_fresh_project(self.project)
            images = {
                role: self.commands.image(getattr(self.args, role + "_image"))
                for role in ("ingest", "opds", "mariadb")
            }
            self.report["images"] = images
            self.report["image_platforms"] = {
                role: self.commands.run(
                    [
                        *self.commands.docker,
                        "image",
                        "inspect",
                        "--format",
                        "{{.Architecture}}/{{.Os}}",
                        identity,
                    ]
                ).strip()
                for role, identity in images.items()
            }
            version_code = "import importlib.metadata as m,json,platform,pathlib; print(json.dumps({'python':platform.python_version(),'platform':platform.platform(),'packages':{d.metadata['Name']:d.version for d in m.distributions()},'cohort':json.loads(pathlib.Path('/opt/h2hdb-cohort.json').read_text())}))"
            self.report["docker"] = self.commands.run(
                [*self.commands.docker, "version", "--format", "json"]
            )
            self.prepared = prepare_deployment(
                self.args.deployment_root,
                self.root,
                project=self.project,
                images=images,
                credentials=self.credentials,
                docker=self.commands.docker,
                instrumented=self.args.instrumented,
                resident={
                    "publication_batch_galleries": max(
                        self.args.base_count, self.args.append_count
                    ),
                    "source_quiet_seconds": 1,
                    "source_probe_interval_seconds": 1,
                    "source_max_wait_seconds": 3600,
                    "progress_log_interval_seconds": 10,
                },
            )
            self.report["deployment"] = self.deployment.report
            self.report["test_source_sha256"] = {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(Path(__file__).parent.glob("*.py"))
            }
            self.report["installed_cohorts"] = {
                role: json.loads(
                    self.commands.run(
                        [
                            *self.commands.docker,
                            "run",
                            "--rm",
                            "--network",
                            "none",
                            "--read-only",
                            "--label",
                            f"{OWNER_LABEL}={self.project}",
                            "--entrypoint",
                            "python",
                            images[role],
                            "-c",
                            version_code,
                        ]
                    )
                )
                for role in SERVICES
            }
            package = self.deployment.probe_dir / "deployment_acceptance"
            package.mkdir()
            for filename in ("__init__.py", "fixture.py", "probe.py"):
                shutil.copy2(Path(__file__).with_name(filename), package / filename)
            shutil.copy2(
                Path(__file__).with_name("sitecustomize.py"),
                self.deployment.probe_dir / "sitecustomize.py",
            )
            shutil.copy2(
                Path(__file__).with_name("probe.py"),
                self.deployment.probe_dir / "probe.py",
            )
            shutil.copy2(
                Path(__file__).with_name("http_probe.py"),
                self.deployment.probe_dir / "http_probe.py",
            )
            self.compose(["config", "--quiet"])
            self.generate(self.args.base_count, 1_000_001)
            self.compose(["up", "-d", "--no-build", "--pull", "never", "database"])
            self.wait(
                lambda: any(
                    row.get("Service") == "database" and row.get("Health") == "healthy"
                    for row in self.statuses()
                ),
                "disposable MariaDB",
                seconds=120,
            )
            initial = self.phase(
                "fresh",
                lambda: self.compose(
                    ["up", "-d", "--no-build", "--pull", "never", *SERVICES.values()]
                ),
                require_analysis=True,
            )
            self.phase(
                "unchanged-restart",
                lambda: self.compose(
                    ["restart", "--timeout", "30", *SERVICES.values()], timeout=90
                ),
                before=initial,
            )
            next_gid = 1_000_001 + self.args.base_count
            self.phase(
                "append",
                lambda: self.generate(self.args.append_count, next_gid),
                require_analysis=True,
            )
            next_gid += self.args.append_count
            if self.args.lifecycle:

                def pending() -> None:
                    self.change(1_000_001, 2, "pending")
                    self.generate(1, next_gid)

                self.phase(
                    "pending-existing-and-ready-new", pending, require_analysis=True
                )
                self.phase(
                    "complete-existing",
                    lambda: self.change(1_000_001, 2, "complete"),
                    require_analysis=True,
                )
                next_gid += 1

                def missing() -> None:
                    self.generate(1, next_gid, marker="missing")
                    self.generate(1, next_gid + 1)

                self.phase(
                    "missing-new-marker-and-ready-new", missing, require_analysis=True
                )
                self.phase(
                    "complete-new-marker",
                    lambda: self.change(next_gid, 1, "complete"),
                    require_analysis=True,
                )
                next_gid += 2
            if self.args.faults:
                for signal in ("SIGTERM", "SIGKILL"):
                    self.phase(
                        "recover-" + signal.lower(),
                        lambda: self.fault(signal, next_gid),
                        require_analysis=True,
                    )
                    next_gid += 1
            self.report["correctness_status"] = "passed"
            return 0
        except (Exception, KeyboardInterrupt) as error:
            self.report.update(
                correctness_status="failed", failure=f"{type(error).__name__}: {error}"
            )
            for scenario in self.report["scenarios"]:
                if scenario.get("status") == "running":
                    scenario.update(status="failed", failure=self.report["failure"])
            print(self.report["failure"], file=sys.stderr, flush=True)
            return 1
        finally:
            if self.prepared is not None:
                try:
                    self.report["cleanup"] = self.commands.cleanup(
                        self.project, self.deployment.compose_path
                    )
                except Exception as error:
                    self.report["cleanup"] = {
                        "verified_empty": False,
                        "error": str(error),
                    }
                try:
                    # Export through held directory descriptors before parsing;
                    # container-created links must never become host file reads.
                    exported = self.commands.output / "evidence"
                    files = export_evidence(self.deployment.evidence_dir, exported)
                    events, damaged = read_probe_events(exported.glob("probe-*.jsonl"))
                    summary = summarize_probe(events)
                    self.report["probe"] = summary
                    self.report["damaged_probe_lines"] = damaged
                    self.report["evidence"] = {"status": "exported", "files": files}
                    self.report["measurement"] = assess_probe_measurement(
                        summary, damaged, instrumented=self.args.instrumented
                    )
                    if not self.report.get("cleanup", {}).get("verified_empty"):
                        self.report["measurement"] = {
                            "status": "failed",
                            "issues": [
                                "Container cleanup was not verified before evidence collection"
                            ],
                        }
                except Exception as error:
                    detail = f"{type(error).__name__}: {error}"
                    self.report["evidence"] = {"status": "failed", "error": detail}
                    self.report["measurement"] = {
                        "status": "failed",
                        "issues": [detail],
                    }
            self.report["fixture_retained"] = True
            if (
                self.report.get("cleanup", {}).get("verified_empty")
                and self.report.get("evidence", {}).get("status") == "exported"
                and not self.args.keep_fixtures
            ):
                try:
                    shutil.rmtree(self.root)
                    self.report["fixture_retained"] = False
                except OSError as error:
                    self.report["fixture_cleanup_error"] = str(error)
            self.report["finished_utc"] = datetime.now(UTC).isoformat()
            write_json(self.commands.output / "report.json", self.report)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("deployment-root", "fixture-python", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--context", required=True)
    for role in ("ingest", "opds", "mariadb"):
        parser.add_argument(f"--{role}-image", required=True)
    parser.add_argument("--base-count", type=int, default=2)
    parser.add_argument("--append-count", type=int, default=2)
    parser.add_argument("--pages", type=int, default=2)
    parser.add_argument(
        "--image-profile", choices=("small", "large", "mixed"), default="mixed"
    )
    parser.add_argument("--deadline-seconds", type=float, default=7200)
    parser.add_argument("--phase-seconds", type=float, default=1800)
    parser.add_argument("--instrumented", action="store_true")
    parser.add_argument("--lifecycle", action="store_true")
    parser.add_argument("--faults", action="store_true")
    parser.add_argument(
        "--http-artifacts",
        action="store_true",
        help="After each scenario, verify first and last GID CBZ downloads and Range through OPDS",
    )
    parser.add_argument(
        "--keep-fixtures",
        action="store_true",
        help="Retain synthetic source, CBZs and configuration after Docker cleanup for diagnosis",
    )
    args = parser.parse_args(argv)
    if min(args.base_count, args.append_count, args.pages, args.phase_seconds) <= 0:
        parser.error("Fixture dimensions and phase timeout must be positive")
    if max(args.base_count, args.append_count) > 1_000_000 or args.pages > 4096:
        parser.error("Counts must be <= 1,000,000 and pages <= 4096")
    if (
        not all(
            math.isfinite(value)
            for value in (args.phase_seconds, args.deadline_seconds)
        )
        or args.deadline_seconds <= 60
    ):
        parser.error("Timeouts must be finite and deadline must exceed 60 seconds")
    if args.faults and not args.instrumented:
        parser.error("Fault scenarios require explicit instrumentation")
    try:
        require_evidence_support()
    except EvidenceError as error:
        parser.error(str(error))
    result = Acceptance(args)
    code = result.run()
    if (
        not result.report.get("cleanup", {}).get("verified_empty")
        or result.report.get("evidence", {}).get("status") != "exported"
        or result.report.get("measurement", {}).get("status")
        not in {"passed", "not_requested"}
        or (
            args.instrumented
            and result.report.get("measurement", {}).get("status") != "passed"
        )
        or result.report.get("fixture_cleanup_error")
    ):
        return 1
    return code
