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
from .growth import growth_evidence
from .probe_stream import stream_lifetime_evidence
from .report import (
    assess_probe_measurement,
    claim_handoff,
    cleanup_completion,
    probe_cursor,
    read_probe_events,
    summarize_log,
    summarize_probe,
)

_OBSERVER_BOOTSTRAP = """import json, os, pathlib, runpy, sys, tempfile
result_path = pathlib.Path(sys.argv[1])
sys.argv = ['deployment_acceptance.http_observer', *sys.argv[2:]]
try:
    runpy.run_module('deployment_acceptance.http_observer', run_name='__main__')
except BaseException as error:
    if isinstance(error, SystemExit) and error.code in (None, 0):
        raise
    if not result_path.exists():
        value = {'status': 'failed', 'stage': 'observer-launch',
                 'failure': 'Observer bootstrap failed: ' + type(error).__name__}
        with tempfile.NamedTemporaryFile(mode='w', dir=result_path.parent, delete=False) as stream:
            json.dump(value, stream, sort_keys=True)
            stream.write('\\n')
            stream.flush()
            os.fsync(stream.fileno())
            temporary = stream.name
        os.replace(temporary, result_path)
    raise
"""


def http_observer_command(
    arguments: Sequence[str],
    *,
    result_path: Path,
    package_root: Path = Path("/acceptance"),
    python: str = "python",
) -> list[str]:
    """A detached Docker exec has no environment from the role's main command."""
    return [
        "env",
        "-u",
        "H2HDB_ACCEPTANCE_PROBE_DIR",
        "-u",
        "H2HDB_ACCEPTANCE_CONTROL_DIR",
        f"PYTHONPATH={package_root}",
        python,
        "-c",
        _OBSERVER_BOOTSTRAP,
        str(result_path),
        *arguments,
    ]


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


def cleanup_fault_evidence(
    events: Sequence[dict[str, Any]],
    reached: dict[str, Any],
    *,
    prior_generation: int,
) -> dict[str, Any]:
    """Validate the exact committed shard that caused this process to pause."""
    generation = reached.get("completed_ingest_generation")
    cause = reached.get("committed_shard_sequence")
    if (
        reached.get("operation") != "core.cleanup.committed_nonempty_shard"
        or reached.get("fault_injection") is not True
        or type(generation) is not int
        or generation <= prior_generation
        or reached.get("required_after_ingest_generation") != prior_generation
        or type(cause) is not int
        or not 0 < cause < reached["sequence"]
    ):
        raise AssertionError(
            "Cleanup fault lacks its fresh generation and exact committed cause"
        )
    shards = [
        event
        for event in events
        if event["process_instance"] == reached["process_instance"]
        and event["sequence"] == cause
    ]
    if len(shards) != 1:
        raise AssertionError("Cleanup fault must identify exactly one committed shard")
    shard = shards[0]
    if (
        shard.get("event") != "cleanup_shard_committed"
        or shard.get("operation") != "core.cleanup.committed_nonempty_shard"
        or type(shard.get("row_count")) is not int
        or shard["row_count"] <= 0
        or shard.get("cycle_complete") is not False
        or type(shard.get("after_ingest_generation")) is not int
        or shard["after_ingest_generation"] != generation
    ):
        raise AssertionError(
            "Cleanup fault cause is not fresh nonempty committed work with work remaining"
        )
    completions = [
        event
        for event in events
        if event["process_instance"] == reached["process_instance"]
        and event["sequence"] < cause
        and event.get("event") == "ingest_completed"
        and event.get("ingest_generation") == generation
        and event.get("publication_terminal") is True
        and event.get("replayed") is False
    ]
    if len(completions) != 1:
        raise AssertionError(
            "Cleanup fault cause has no exact fresh published-ingest completion"
        )
    return {
        "process_instance": reached["process_instance"],
        "ingest_generation": generation,
        "required_after_ingest_generation": prior_generation,
        "completion_sequence": completions[0]["sequence"],
        "committed_shard_sequence": cause,
        "fault_sequence": reached["sequence"],
        "row_count": shard["row_count"],
        "cycle_complete": False,
    }


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
        self._last_oracle: dict[str, Any] | None = None
        self._http_observer_token: str | None = None
        self._http_observer_reached: dict[str, Any] | None = None
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
            "completion_contract": (
                "publication-and-current-only-cleanup"
                if args.instrumented
                else "publication-only-functional-baseline"
            ),
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

    def growth_rounds(self, next_gid: int) -> int:
        """Keep the existing scenarios, then add separately measured equal inputs."""
        for ordinal in range(1, self.args.growth_batches + 1):
            name = f"growth-append-{ordinal}"

            def append() -> None:
                self.commands.run(
                    [
                        str(self.args.fixture_python),
                        str(Path(__file__).with_name("fixture.py")),
                        "append-collection",
                        "--root",
                        str(self.deployment.source_dir),
                        "--count",
                        str(self.args.append_count),
                        "--start-gid",
                        str(next_gid),
                        "--pages",
                        str(self.args.pages),
                        "--profile",
                        self.args.image_profile,
                        "--collection",
                        name,
                    ],
                    timeout=600,
                )

            self.phase(
                name,
                append,
                require_analysis=True,
                growth_galleries=next_gid - 1_000_001 + self.args.append_count,
            )
            next_gid += self.args.append_count
        return next_gid

    def logs(self, since: str) -> str:
        return self.compose(
            ["logs", "--no-color", "--timestamps", "--since", since, SERVICES["ingest"]]
        )

    def probe_events(self) -> list[dict[str, Any]]:
        events, _damaged = read_probe_events(
            self.deployment.evidence_dir.glob("probe-*.jsonl")
        )
        return events

    def await_cleanup(self, record: dict[str, Any], boundary: dict[str, int]) -> None:
        def completed() -> bool:
            evidence = cleanup_completion(
                self.probe_events(),
                boundary,
                verified_after=record["verification_probe_cursor"],
            )
            if evidence is None:
                return False
            record["catalog_cleanup"] = evidence
            return True

        self.wait(
            completed,
            f"{record['name']}: fresh published ingest followed by current-only cleanup DONE",
            seconds=self.args.phase_seconds,
        )
        previous = self.report["scenarios"][:-1]
        if previous:
            prior = previous[-1].get("catalog_cleanup")
            if prior is not None and prior.get("status") == "passed":
                record["previous_cleanup_to_claim"] = claim_handoff(
                    prior, record["catalog_cleanup"]
                )
                previous[-1]["next_ingest_claim"] = record["previous_cleanup_to_claim"]

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

    def start_http_observer(self, oracle: dict[str, Any]) -> str:
        artifact = min(oracle["artifacts"], key=lambda row: row["gid"])
        token = "stream-" + secrets.token_hex(8)
        seconds = self.args.phase_seconds + 120
        self._http_observer_token = token
        write_json(
            self.deployment.control_dir / "stream-arm.json",
            {
                "token": token,
                "byte_length": artifact["byte_length"],
                "deadline_seconds": seconds,
            },
        )
        self.compose(
            [
                "exec",
                "-d",
                SERVICES["opds"],
                *http_observer_command(
                    [
                        "--base-url",
                        "http://127.0.0.1:8000",
                        "--gid",
                        str(artifact["gid"]),
                        "--sha256",
                        artifact["sha256"],
                        "--size",
                        str(artifact["byte_length"]),
                        "--revision",
                        str(oracle["revision"]),
                        "--control-directory",
                        "/acceptance-control",
                        "--evidence-directory",
                        "/acceptance-evidence",
                        "--token",
                        token,
                        "--deadline-seconds",
                        str(seconds),
                    ],
                    result_path=Path("/acceptance-evidence")
                    / f"http-result-{token}.json",
                ),
            ]
        )

        def ready() -> bool:
            failed = self.deployment.evidence_dir / f"http-result-{token}.json"
            if failed.exists():
                result = read_evidence_json(failed.parent, failed.name)
                if result.get("status") != "passed":
                    raise AssertionError(
                        f"Concurrent HTTP observer failed before ready: {result}"
                    )
            path = self.deployment.evidence_dir / f"http-ready-{token}.json"
            if not path.exists():
                return False
            result = read_evidence_json(path.parent, path.name)
            if result.get("status") != "ready":
                raise AssertionError(
                    "Concurrent HTTP observer did not acquire its held stream"
                )
            gates = [
                event
                for event in self.probe_events()
                if event.get("token") == token
                and str(event.get("event", "")).startswith("stream_gate_")
            ]
            if not gates:
                return False
            if len(gates) != 1 or gates[0]["event"] != "stream_gate_reached":
                raise AssertionError(
                    "Archive descriptor was not held before source mutation"
                )
            self._http_observer_reached = gates[0]
            return True

        self.wait(ready, "OPDS observer holds a real archive descriptor", seconds=60)
        return token

    def release_http_observer(self) -> None:
        token = getattr(self, "_http_observer_token", None)
        if token is not None:
            (self.deployment.control_dir / "stream-arm.json").unlink(missing_ok=True)
            (self.deployment.control_dir / f"stream-release-{token}").touch()

    def finish_http_observer(
        self, token: str, oracle: dict[str, Any]
    ) -> dict[str, Any]:
        artifact = min(oracle["artifacts"], key=lambda row: row["gid"])
        self.release_http_observer()
        write_json(
            self.deployment.control_dir / f"finish-{token}.json",
            {
                "gid": artifact["gid"],
                "revision": oracle["revision"],
                "sha256": artifact["sha256"],
                "byte_length": artifact["byte_length"],
            },
        )
        result: dict[str, Any] = {}

        def completed() -> bool:
            nonlocal result
            path = self.deployment.evidence_dir / f"http-result-{token}.json"
            if not path.exists():
                return False
            result = read_evidence_json(path.parent, path.name)
            if result.get("status") != "passed":
                raise AssertionError(f"Concurrent HTTP observer failed: {result}")
            return True

        self.wait(
            completed, "OPDS held stream and new revision verification", seconds=60
        )
        lifetime = stream_lifetime_evidence(self.probe_events(), token)
        reached = self._http_observer_reached
        if reached is None or any(
            lifetime["reached"][field] != reached[field]
            for field in ("process_instance", "sequence")
        ):
            raise AssertionError(
                "Completed stream differs from the pre-mutation descriptor gate"
            )
        cleanup = self.report["scenarios"][-1]["catalog_cleanup"]
        lifetime["cleanup_done_before_release"] = {
            key: cleanup[key]
            for key in ("process_instance", "ingest_generation", "done_sequence")
        }
        lifetime["ordering_evidence"] = (
            "Host observed the held descriptor before input mutation, then observed cleanup DONE before creating the release file. Cross-process clocks are not compared."
        )
        result["descriptor_lifetime"] = lifetime
        replaced = result["held_download_sha256"] != result["new_download_sha256"]
        result["archive_replaced"] = replaced
        scenario = self.report["scenarios"][-1]["name"]
        if scenario == "complete-existing" and (
            not replaced or lifetime["released"]["links"] != 0
        ):
            raise AssertionError(
                "Changed-archive scenario must keep reading its unlinked old inode while publishing different bytes"
            )
        result["unlinked_old_archive_verified"] = (
            replaced and lifetime["released"]["links"] == 0
        )
        self._http_observer_token = None
        self._http_observer_reached = None
        return result

    def phase(
        self,
        name: str,
        action: Callable[[], object],
        *,
        require_analysis: bool = False,
        before: dict[str, Any] | None = None,
        growth_galleries: int | None = None,
    ) -> dict[str, Any]:
        since = datetime.now(UTC).isoformat()
        started = time.monotonic()
        instrumented = self.args.instrumented
        boundary = probe_cursor(self.probe_events()) if instrumented else {}
        record: dict[str, Any] = {
            "name": name,
            "started_utc": since,
            "status": "running",
        }
        self.report["scenarios"].append(record)
        previous_oracle = getattr(self, "_last_oracle", None)
        observer = (
            self.start_http_observer(previous_oracle)
            if self.args.concurrent_http
            and previous_oracle is not None
            and before is None
            else None
        )
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
            record["publication_observed_wall_seconds"] = measured
            record["log"] = summarize_log(log)
            (self.commands.output / f"ingest-{name}.log").write_text(log)
            if growth_galleries is not None:
                record["growth"] = growth_evidence(
                    log,
                    record["log"],
                    oracle,
                    expected_galleries=growth_galleries,
                    pages_per_gallery=self.args.pages,
                )
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
        if instrumented:
            record["verification_probe_cursor"] = probe_cursor(self.probe_events())
            self.await_cleanup(record, boundary)
            record["work_wall_seconds"] = time.monotonic() - started
            verification_started = time.monotonic()
            after_cleanup = self.verify(name + "-after-cleanup")
            record["post_cleanup_oracle_seconds"] = (
                time.monotonic() - verification_started
            )
            compare_reuse(oracle, after_cleanup)
            if oracle["revision"] != after_cleanup["revision"]:
                raise AssertionError(
                    "Cleanup changed the verified current catalog revision"
                )
            oracle = after_cleanup
        else:
            record["catalog_cleanup"] = {
                "status": "not_observed",
                "reason": "Functional baseline has no lifecycle instrumentation; it is not cleanup acceptance.",
            }
        if observer is not None:
            record["concurrent_http"] = self.finish_http_observer(observer, oracle)
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
        self._last_oracle = oracle
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

    def fault(self, signal: str, gid: int, *, cleanup: bool = False) -> None:
        token = (
            ("cleanup-" if cleanup else "activation-")
            + signal.lower()
            + "-"
            + secrets.token_hex(4)
        )
        arm = self.deployment.control_dir / "arm.json"
        operation = (
            "core.cleanup.committed_nonempty_shard"
            if cleanup
            else "library.commit_pending_installs"
        )
        prior_generation = max(
            (
                row.get("catalog_cleanup", {}).get("ingest_generation", 0)
                for row in self.report["scenarios"]
            ),
            default=0,
        )
        declaration: dict[str, object] = {"operation": operation, "token": token}
        if cleanup:
            declaration["after_ingest_generation"] = prior_generation
        write_json(arm, declaration)
        self.generate(1, gid)
        committed_cleanup: dict[str, Any] | None = None

        def reached() -> bool:
            nonlocal committed_cleanup
            events, _ = read_probe_events(
                self.deployment.evidence_dir.glob("probe-*.jsonl")
            )
            reached_event = next(
                (
                    event
                    for event in events
                    if event.get("event") == "fault_reached"
                    and event.get("token") == token
                ),
                None,
            )
            if reached_event is None:
                return False
            if cleanup:
                committed_cleanup = cleanup_fault_evidence(
                    events,
                    reached_event,
                    prior_generation=prior_generation,
                )
            return True

        self.wait(
            reached,
            f"durable {operation} fault gate before {signal}",
            seconds=self.args.phase_seconds,
        )
        arm.unlink()
        if (
            not cleanup
            and not (
                self.deployment.library_dir / ".h2hdb-coordination" / "ACTIVATING"
            ).exists()
        ):
            raise AssertionError("Fault did not reach a fenced activation")
        evidence: dict[str, Any] = {
            "signal": signal,
            "token": token,
            "operation": operation,
            "http_at_fault": self.http(fenced=not cleanup),
        }
        if cleanup:
            evidence["committed_cleanup"] = committed_cleanup
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
                # This diagnostic harness proves which analysis was executed.
                # Production and prepare_deployment defaults remain INFO.
                log_level="debug",
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
            for filename in (
                "__init__.py",
                "fixture.py",
                "probe.py",
                "http_observer.py",
                "http_probe.py",
                "probe_stream.py",
            ):
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
                Path(__file__).with_name("probe_stream.py"),
                self.deployment.probe_dir / "probe_stream.py",
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
            next_gid = self.growth_rounds(next_gid)
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
            if self.args.cleanup_faults:
                for signal in ("SIGTERM", "SIGKILL"):
                    self.phase(
                        "recover-cleanup-" + signal.lower(),
                        lambda: self.fault(signal, next_gid, cleanup=True),
                        require_analysis=True,
                    )
                    next_gid += 1
                self.phase(
                    "post-cleanup-fault-handoff",
                    lambda: self.generate(1, next_gid),
                    require_analysis=True,
                )
                sentinel = self.report["scenarios"][-1]
                sentinel["purpose"] = (
                    "Verify that the last cleanup-fault recovery permits the next real ingest claim."
                )
                sentinel["next_ingest_claim"] = {
                    "status": "not_requested",
                    "reason": "This final sentinel proves the preceding scenario handoff; no later input was requested.",
                }
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
                    self.release_http_observer()
                except Exception as error:
                    self.report["observer_release_error"] = str(error)
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
    parser.add_argument(
        "--growth-batches",
        type=int,
        choices=range(4),
        default=0,
        help="Additional equal append rounds (0..3); requires >128 new image pages per round",
    )
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
        "--concurrent-http",
        action="store_true",
        help="Hold an OPDS archive descriptor and read throughout publication and cleanup; requires instrumentation",
    )
    parser.add_argument(
        "--cleanup-faults",
        action="store_true",
        help="Kill real ingest after a nonempty cleanup shard commits with work remaining; requires instrumentation",
    )
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
    if (
        args.faults or args.cleanup_faults or args.concurrent_http
    ) and not args.instrumented:
        parser.error("Fault scenarios require explicit instrumentation")
    if args.growth_batches and args.append_count * args.pages <= 128:
        parser.error("Growth rounds require append-count * pages greater than 128")
    if args.concurrent_http and args.phase_seconds > 3480:
        parser.error(
            "Concurrent HTTP requires phase-seconds <= 3480 for its bounded observer lifetime"
        )
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
        or result.report.get("observer_release_error")
    ):
        return 1
    return code
