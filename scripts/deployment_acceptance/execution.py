"""Bounded Docker commands with explicit context and label-scoped cleanup."""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from .compose import OWNER_LABEL

_RESOURCES = (
    ("containers", ("ps", "-aq"), ("rm", "-f")),
    ("networks", ("network", "ls", "-q"), ("network", "rm")),
    ("volumes", ("volume", "ls", "-q"), ("volume", "rm")),
)
_PROJECT_LABEL = "com.docker.compose.project"


class CleanupError(RuntimeError):
    """A cleanup failure with a reviewable receipt, including any known residue."""

    def __init__(self, receipt: dict[str, Any]) -> None:
        self.receipt = receipt
        super().__init__(f"Acceptance cleanup was incomplete: {receipt}")


def _output(value: bytes | str | None) -> str:
    return value.decode(errors="replace") if isinstance(value, bytes) else value or ""


def _require_project(project: str) -> None:
    if re.fullmatch(r"h2hdb-acceptance-[a-z0-9][a-z0-9-]{0,47}", project) is None:
        raise ValueError("Test project must use its dedicated namespace")


class Commands:
    def __init__(self, *, context: str, output: Path, seconds: float) -> None:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", context):
            raise ValueError("An explicit Docker context name is required")
        if seconds <= 60:
            raise ValueError(
                "The test deadline must reserve at least 60 seconds for cleanup"
            )
        self.docker = ["docker", "--context", context]
        self.output = output
        self.started = time.monotonic()
        self.deadline = self.started + seconds
        self.serial = 0
        self._admitted_projects: set[str] = set()
        output.mkdir(parents=True, exist_ok=False)

    def run(
        self,
        command: Sequence[str],
        *,
        check: bool = True,
        cleanup: bool = False,
        timeout: float = 60,
    ) -> str:
        """Journal every attempted call, including timeout, signal and launch error.

        Normal calls reserve 45 seconds for cleanup. If that reserve is exhausted,
        cleanup still attempts each owned resource with at most one second per
        command; it never silently reports an unverified empty tree.
        """
        if timeout <= 0:
            raise ValueError("Command timeout must be positive")
        remaining = self.deadline - time.monotonic() - (0 if cleanup else 45)
        if remaining <= 0 and not cleanup:
            raise TimeoutError("Acceptance deadline reached; cleanup time is reserved")
        effective_timeout = min(timeout, max(1, remaining))
        self.serial += 1
        started = time.monotonic()
        path = self.output / f"command-{self.serial:05d}.log"
        record: dict[str, Any] = {
            "command": list(command),
            "cleanup": cleanup,
            "timeout_seconds": effective_timeout,
            "output": path.name,
        }
        text = ""
        try:
            completed = subprocess.run(
                list(command),
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                check=False,
                timeout=effective_timeout,
            )
            text = completed.stdout + completed.stderr
            record["returncode"] = completed.returncode
        except BaseException as error:
            if isinstance(error, subprocess.TimeoutExpired):
                text = _output(error.stdout) + _output(error.stderr)
            record["exception"] = type(error).__name__
            record["error"] = str(error)
            raise
        finally:
            record["seconds"] = time.monotonic() - started
            path.write_text(text, encoding="utf-8")
            with (self.output / "commands.jsonl").open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record) + "\n")
        if check and completed.returncode:
            raise RuntimeError(
                f"Command exited {completed.returncode}; see {path}: {text[-2500:]}"
            )
        return text

    def compose(
        self,
        project: str,
        compose_path: Path,
        arguments: Sequence[str],
        **options: Any,
    ) -> str:
        _require_project(project)
        return self.run(
            [
                *self.docker,
                "compose",
                "--env-file",
                os.devnull,
                "--project-name",
                project,
                "--project-directory",
                str(compose_path.parent),
                "--file",
                str(compose_path),
                *arguments,
            ],
            **options,
        )

    def image(self, reference: str) -> str:
        result = self.run(
            [*self.docker, "image", "inspect", "--format", "{{.Id}}", "--", reference]
        ).strip()
        if re.fullmatch(r"sha256:[0-9a-f]{64}", result) is None:
            raise ValueError(
                f"Image reference did not resolve to one immutable local ID: {reference}"
            )
        return result

    def _list(
        self,
        arguments: Sequence[str],
        *,
        label: str,
        project: str,
        cleanup: bool = False,
    ) -> list[str]:
        result = self.run(
            [*self.docker, *arguments, "--filter", f"label={label}={project}"],
            cleanup=cleanup,
            timeout=5 if cleanup else 30,
        ).split()
        if any(
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", item) is None for item in result
        ):
            raise ValueError("Docker returned a malformed resource identifier")
        return result

    def assert_fresh_project(self, project: str) -> None:
        _require_project(project)
        for kind, arguments, _remove in _RESOURCES:
            for label in (OWNER_LABEL, _PROJECT_LABEL):
                if self._list(arguments, label=label, project=project):
                    raise ValueError(
                        f"Refusing to reuse pre-existing test {kind} ({label})"
                    )
        self._admitted_projects.add(project)

    def cleanup(self, project: str, compose_path: Path) -> dict[str, Any]:
        """Attempt every owned resource even after independent cleanup failures."""
        _require_project(project)
        if project not in self._admitted_projects:
            raise ValueError("Cleanup requires a successful fresh-project admission")
        errors: list[dict[str, str]] = []
        notes: list[dict[str, str]] = []
        removed: dict[str, list[str]] = {kind: [] for kind, _, _ in _RESOURCES}
        remaining: dict[str, list[str] | None] = {}
        foreign: dict[str, list[str] | None] = {}

        def failure(
            phase: str, error: BaseException, *, diagnostic: bool = False
        ) -> None:
            item = {"phase": phase, "error": f"{type(error).__name__}: {error}"}
            (notes if diagnostic else errors).append(item)

        try:
            self.compose(
                project,
                compose_path,
                ["logs", "--no-color", "--timestamps"],
                cleanup=True,
                timeout=5,
            )
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
            failure("logs", error, diagnostic=True)

        # Compose down acts on project labels. Do not let it remove an unrelated
        # resource carrying this project label without our explicit owner label.
        for kind, arguments, _remove in _RESOURCES:
            try:
                owned = self._list(
                    arguments, label=OWNER_LABEL, project=project, cleanup=True
                )
                project_resources = self._list(
                    arguments, label=_PROJECT_LABEL, project=project, cleanup=True
                )
                foreign[kind] = sorted(set(project_resources) - set(owned))
            except (
                OSError,
                RuntimeError,
                ValueError,
                subprocess.TimeoutExpired,
            ) as error:
                foreign[kind] = None
                failure(f"down ownership check: {kind}", error, diagnostic=True)
        if all(items == [] for items in foreign.values()):
            try:
                self.compose(
                    project,
                    compose_path,
                    ["down", "--volumes", "--timeout", "5"],
                    cleanup=True,
                    timeout=10,
                )
            except (
                OSError,
                RuntimeError,
                ValueError,
                subprocess.TimeoutExpired,
            ) as error:
                failure("compose down", error, diagnostic=True)
        else:
            notes.append(
                {
                    "phase": "compose down",
                    "error": "Skipped because project ownership is foreign or unverified",
                }
            )

        for kind, arguments, remove in _RESOURCES:
            try:
                owned = self._list(
                    arguments, label=OWNER_LABEL, project=project, cleanup=True
                )
            except (
                OSError,
                RuntimeError,
                ValueError,
                subprocess.TimeoutExpired,
            ) as error:
                failure(f"list owned {kind}", error)
                continue
            for identity in owned:
                try:
                    self.run(
                        [*self.docker, *remove, "--", identity], cleanup=True, timeout=5
                    )
                    removed[kind].append(identity)
                except (
                    OSError,
                    RuntimeError,
                    ValueError,
                    subprocess.TimeoutExpired,
                ) as error:
                    failure(f"remove {kind} {identity}", error)

        for kind, arguments, _remove in _RESOURCES:
            try:
                remaining[kind] = self._list(
                    arguments, label=OWNER_LABEL, project=project, cleanup=True
                )
            except (
                OSError,
                RuntimeError,
                ValueError,
                subprocess.TimeoutExpired,
            ) as error:
                remaining[kind] = None
                failure(f"verify owned {kind}", error)
        verified = all(items == [] for items in remaining.values())
        receipt = {
            "verified_empty": verified,
            "remaining": remaining,
            "removed": removed,
            "foreign_project_resources": foreign,
            "errors": errors,
            "diagnostics": notes,
        }
        (self.output / "cleanup.json").write_text(
            json.dumps(receipt, indent=2) + "\n", encoding="utf-8"
        )
        if not verified or errors:
            raise CleanupError(receipt)
        return receipt
