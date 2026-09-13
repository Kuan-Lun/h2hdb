"""Derive a disposable acceptance deployment from the real Compose model.

This module never starts Docker resources. Only the explicit Compose YAML and
three public runtime wrappers are read from the supplied deployment directory.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

OWNER_LABEL = "io.h2hdb.deployment-acceptance"
DATABASE_NAME = "h2hdb_acceptance"
WRITER_USER = "acceptance_writer"
READER_USER = "acceptance_reader"
SERVICES = {"ingest": "h2hdb-ingest", "opds": "h2hdb-opds"}
WRAPPERS = {
    "main.sh": "/opt/h2hdb-main.sh",
    "wait-opds.py": "/opt/h2hdb-wait-opds.py",
    "publication-ready.sh": "/opt/h2hdb-publication-ready.sh",
}
_SERVICE_FIELDS = frozenset(
    {
        "build",
        "command",
        "container_name",
        "depends_on",
        "env_file",
        "environment",
        "healthcheck",
        "image",
        "init",
        "networks",
        "ports",
        "pull_policy",
        "read_only",
        "restart",
        "stop_grace_period",
        "tmpfs",
        "user",
        "volumes",
    }
)
_TARGETS = {
    "ingest": {
        "/h2hdb-config": True,
        "/hentai/download": True,
        "/hentai/library": False,
    },
    "opds": {
        "/h2hdb-config": True,
        "/hentai/comics": True,
        "/h2hdb-coordination": True,
    },
}


@dataclass(frozen=True)
class DatabaseCredentials:
    writer_password: str
    reader_password: str
    root_password: str

    def validate(self) -> None:
        values = (self.writer_password, self.reader_password, self.root_password)
        if len(set(values)) != 3 or any(
            re.fullmatch(r"[A-Za-z0-9_-]{16,128}", value) is None for value in values
        ):
            raise ValueError(
                "Use three distinct synthetic alphanumeric passwords (16..128 characters)"
            )


@dataclass(frozen=True)
class PreparedDeployment:
    compose_path: Path
    config_dir: Path
    source_dir: Path
    library_dir: Path
    probe_dir: Path
    evidence_dir: Path
    control_dir: Path
    report: dict[str, Any]


def load_deployment(
    deployment_root: Path, *, docker: Sequence[str] = ("docker",)
) -> dict[str, Any]:
    """Parse YAML with Compose without interpolation or env-file resolution."""
    if not docker:
        raise ValueError("An explicit Docker CLI command is required")
    executable = shutil.which(docker[0])
    if executable is None:
        raise ValueError("Docker CLI is unavailable")
    source = deployment_root.resolve() / "docker-compose.yml"
    if source.is_symlink() or not source.is_file():
        raise ValueError("Deployment Compose source must be a regular file")
    command = [
        executable,
        *docker[1:],
        "compose",
        "--env-file",
        os.devnull,
        "--project-name",
        "h2hdb-acceptance-parse",
        "--project-directory",
        tempfile.gettempdir(),
        "--file",
        str(source),
        "config",
        "--format",
        "json",
        "--no-interpolate",
        "--no-env-resolution",
        "--no-path-resolution",
    ]
    result = subprocess.run(
        command,
        cwd=tempfile.gettempdir(),
        env={"PATH": os.defpath},
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if result.returncode:
        # Do not print a future CLI's rendered environment or arbitrary stderr.
        raise ValueError(
            f"Safe Compose parsing failed (exit {result.returncode}); required isolation flags must be supported"
        )
    value = json.loads(result.stdout)
    if not isinstance(value, dict):
        raise ValueError("Compose did not return an object")
    return value


def _fixture_root(path: Path) -> Path:
    root = path.resolve()
    temporary_roots = {Path(tempfile.gettempdir()).resolve(), Path("/tmp").resolve()}
    if not any(
        root != parent and root.is_relative_to(parent) for parent in temporary_roots
    ):
        raise ValueError("Acceptance fixtures must be inside a temporary directory")
    if path.is_symlink():
        raise ValueError("Fixture root must not be a symlink")
    return root


def _inside(root: Path, path: Path) -> Path:
    if not path.is_relative_to(root) or path.resolve() != path:
        raise ValueError(
            "Acceptance bind source escapes fixture root or follows a symlink"
        )
    return path


def _bind(source: Path, target: str, readonly: bool = True) -> dict[str, Any]:
    return {
        "type": "bind",
        "source": str(source),
        "target": target,
        "read_only": readonly,
        "bind": {"create_host_path": False},
    }


def _source_bindings(service: Mapping[str, Any], role: str) -> dict[str, str]:
    bindings: dict[str, str] = {}
    volumes = service.get("volumes")
    if not isinstance(volumes, list):
        raise ValueError(f"{role} must retain explicit bind mounts")
    for volume in volumes:
        if not isinstance(volume, dict):
            raise ValueError(f"{role} uses an unsupported volume declaration")
        target = volume.get("target")
        if target not in _TARGETS[role] or target in bindings:
            raise ValueError(f"{role} has an unknown or duplicated bind target")
        if (
            volume.get("type") != "bind"
            or volume.get("read_only", False) is not _TARGETS[role][target]
            or volume.get("bind") != {"create_host_path": False}
            or set(volume) - {"type", "source", "target", "read_only", "bind"}
        ):
            raise ValueError(f"{role} bind policy changed for {target}")
        source = volume.get("source")
        if not isinstance(source, str) or not source.startswith("/") or "$" in source:
            raise ValueError(f"{role} requires absolute uninterpolated bind sources")
        bindings[target] = source
    if set(bindings) != set(_TARGETS[role]):
        raise ValueError(f"{role} is missing a required mount")
    return bindings


def _validate_original(model: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    if any(
        key not in {"name", "version", "services", "networks"}
        and not key.startswith("x-")
        for key in model
    ):
        raise ValueError("Unexpected deployment-level resources")
    raw = model.get("services")
    if not isinstance(raw, dict):
        raise ValueError("Compose services are missing")
    chosen: dict[str, dict[str, Any]] = {}
    bindings: dict[str, dict[str, str]] = {}
    for role, name in SERVICES.items():
        service = raw.get(name)
        if not isinstance(service, dict) or set(service) - _SERVICE_FIELDS:
            raise ValueError(f"Unsupported service fields for {name}")
        command = service.get("command")
        argv = shlex.split(command) if isinstance(command, str) else command
        if argv != ["bash", "/opt/h2hdb-main.sh", role]:
            raise ValueError(f"Unexpected deployment command for {name}")
        if service.get("read_only") is not True or service.get("init") is not True:
            raise ValueError(f"{name} must retain read-only root and init")
        health = service.get("healthcheck")
        if (
            not isinstance(health, dict)
            or not health.get("test")
            or health.get("disable")
        ):
            raise ValueError(f"{name} requires its deployment healthcheck")
        expected_dependencies = (
            {}
            if role == "ingest"
            else {SERVICES["ingest"]: {"condition": "service_started"}}
        )
        actual_dependencies = copy.deepcopy(service.get("depends_on", {}))
        if isinstance(actual_dependencies, dict):
            for dependency in actual_dependencies.values():
                if isinstance(dependency, dict) and dependency.get("required") is True:
                    dependency.pop("required")
        if actual_dependencies != expected_dependencies:
            raise ValueError(f"{name} startup dependency contract changed")
        bindings[role] = _source_bindings(service, role)
        chosen[role] = service
    library = PurePosixPath(bindings["ingest"]["/hentai/library"])
    if (
        bindings["opds"]["/h2hdb-config"] != bindings["ingest"]["/h2hdb-config"]
        or PurePosixPath(bindings["opds"]["/hentai/comics"]) != library / "current"
        or PurePosixPath(bindings["opds"]["/h2hdb-coordination"])
        != library / ".h2hdb-coordination"
    ):
        raise ValueError("Deployment source/library sharing contract changed")
    return chosen


def derive_compose(
    model: Mapping[str, Any],
    fixture_root: Path,
    *,
    project: str,
    images: Mapping[str, str],
    credentials: DatabaseCredentials,
    uid: int = 65534,
    gid: int = 65534,
    instrumented: bool = False,
) -> dict[str, Any]:
    """Copy the real role contract and replace every external resource boundary."""
    root = _fixture_root(fixture_root)
    credentials.validate()
    if re.fullmatch(r"h2hdb-acceptance-[a-z0-9][a-z0-9-]{0,47}", project) is None:
        raise ValueError("Use a unique h2hdb-acceptance-* project name")
    if (
        type(uid) is not int
        or type(gid) is not int
        or not 1 <= uid < 2**31
        or not 1 <= gid < 2**31
    ):
        raise ValueError("Acceptance role UID/GID must be non-root positive integers")
    if set(images) != {"ingest", "opds", "mariadb"} or any(
        re.fullmatch(r"sha256:[0-9a-f]{64}", image) is None for image in images.values()
    ):
        raise ValueError(
            "Pin exactly ingest, opds and mariadb to local immutable image IDs"
        )
    chosen = _validate_original(model)
    labels = {OWNER_LABEL: project}
    paths = {
        "ingest": {
            "/h2hdb-config": root / "config",
            "/hentai/download": root / "source",
            "/hentai/library": root / "library",
        },
        "opds": {
            "/h2hdb-config": root / "config",
            "/hentai/comics": root / "library/current",
            "/h2hdb-coordination": root / "library/.h2hdb-coordination",
        },
    }
    services: dict[str, Any] = {}
    for role, name in SERVICES.items():
        service = copy.deepcopy(chosen[role])
        for field in ("build", "container_name", "env_file", "ports"):
            service.pop(field, None)
        environment = {
            "HOME": "/tmp/h2hdb-home",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUNBUFFERED": "1",
            "TZ": "Asia/Taipei",
            "H2HDB_DATABASE_HOST": "database",
            "H2HDB_DATABASE_PORT": "3306",
            "H2HDB_DATABASE_NAME": DATABASE_NAME,
        }
        if instrumented:
            environment.update(
                {
                    "PYTHONPATH": "/acceptance",
                    "H2HDB_ACCEPTANCE_PROBE_DIR": "/acceptance-evidence",
                }
            )
            if role == "ingest":
                environment["H2HDB_ACCEPTANCE_CONTROL_DIR"] = "/acceptance-control"
        if role == "ingest":
            environment.update(
                {
                    "H2HDB_DATABASE_WRITER_USER": WRITER_USER,
                    "H2HDB_DATABASE_WRITER_PASSWORD": credentials.writer_password,
                }
            )
        else:
            environment.update(
                {
                    "H2HDB_DATABASE_READER_USER": READER_USER,
                    "H2HDB_DATABASE_READER_PASSWORD": credentials.reader_password,
                    "H2HDB_OPDS_PUBLIC_BASE_URL": "http://h2hdb-opds:8000",
                    "H2HDB_OPDS_USERNAME": "acceptance-unused",
                    "H2HDB_OPDS_PASSWORD": credentials.reader_password,
                }
            )
        service.update(
            {
                "image": images[role],
                "pull_policy": "never",
                "environment": environment,
                "user": f"{uid}:{gid}",
                "networks": {"isolated": None},
                "labels": labels.copy(),
                "tmpfs": [
                    f"/tmp:rw,nosuid,nodev,noexec,size=2g,uid={uid},gid={gid},mode=1770"
                ],
                "volumes": [
                    *(
                        _bind(_inside(root, source), target, _TARGETS[role][target])
                        for target, source in paths[role].items()
                    ),
                    *(
                        _bind(_inside(root, root / "runtime" / filename), target)
                        for filename, target in WRAPPERS.items()
                    ),
                    _bind(_inside(root, root / "probes"), "/acceptance"),
                    _bind(
                        _inside(root, root / "evidence"), "/acceptance-evidence", False
                    ),
                    _bind(_inside(root, root / "control"), "/acceptance-control"),
                ],
            }
        )
        services[name] = service
    services["database"] = {
        "image": images["mariadb"],
        "pull_policy": "never",
        "labels": labels.copy(),
        "environment": {
            "MARIADB_ROOT_PASSWORD": credentials.root_password,
            "MARIADB_DATABASE": DATABASE_NAME,
            "MARIADB_USER": WRITER_USER,
            "MARIADB_PASSWORD": credentials.writer_password,
        },
        "networks": {"isolated": None},
        "volumes": [
            {"type": "volume", "source": "database-data", "target": "/var/lib/mysql"},
            _bind(_inside(root, root / "database-init"), "/docker-entrypoint-initdb.d"),
        ],
        "healthcheck": {
            "test": ["CMD", "healthcheck.sh", "--connect", "--innodb_initialized"],
            "interval": "2s",
            "timeout": "3s",
            "retries": 40,
        },
        "restart": "no",
    }
    result = {
        "name": project,
        "services": services,
        "networks": {"isolated": {"internal": True, "labels": labels.copy()}},
        "volumes": {"database-data": {"labels": labels.copy()}},
    }
    validate_isolation(result, root, project=project)
    return result


def validate_isolation(
    model: Mapping[str, Any], fixture_root: Path, *, project: str
) -> None:
    """Fail closed before any executor receives the derived model."""
    root = _fixture_root(fixture_root)
    if model.get("name") != project or set(model) != {
        "name",
        "services",
        "networks",
        "volumes",
    }:
        raise ValueError("Unexpected isolated deployment shape")
    labels = {OWNER_LABEL: project}
    if model.get("networks") != {
        "isolated": {"internal": True, "labels": labels}
    } or model.get("volumes") != {"database-data": {"labels": labels}}:
        raise ValueError("Acceptance network/volume escaped its project")
    services = model.get("services")
    if not isinstance(services, dict) or set(services) != {
        *SERVICES.values(),
        "database",
    }:
        raise ValueError("Unexpected isolated service set")
    for name, service in services.items():
        if not isinstance(service, dict) or set(service) - (
            _SERVICE_FIELDS | {"labels"}
        ):
            raise ValueError("Unsupported isolated service fields")
        if any(
            field in service
            for field in ("build", "container_name", "env_file", "ports")
        ):
            raise ValueError("External deployment configuration survived isolation")
        if (
            service.get("networks") != {"isolated": None}
            or service.get("labels") != labels
            or service.get("pull_policy") != "never"
        ):
            raise ValueError("Isolated service ownership/network/pull contract changed")
        if (
            not isinstance(service.get("image"), str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", service["image"]) is None
        ):
            raise ValueError("An isolated image must remain pinned")
        if name == "database":
            expected_binds = {
                "/docker-entrypoint-initdb.d": (root / "database-init", True)
            }
        else:
            role = "ingest" if name == SERVICES["ingest"] else "opds"
            role_paths = (
                {
                    "/h2hdb-config": "config",
                    "/hentai/download": "source",
                    "/hentai/library": "library",
                }
                if role == "ingest"
                else {
                    "/h2hdb-config": "config",
                    "/hentai/comics": "library/current",
                    "/h2hdb-coordination": "library/.h2hdb-coordination",
                }
            )
            expected_binds = {
                target: (root / relative, _TARGETS[role][target])
                for target, relative in role_paths.items()
            }
            expected_binds.update(
                {
                    target: (root / "runtime" / filename, True)
                    for filename, target in WRAPPERS.items()
                }
            )
            expected_binds.update(
                {
                    "/acceptance": (root / "probes", True),
                    "/acceptance-evidence": (root / "evidence", False),
                    "/acceptance-control": (root / "control", True),
                }
            )
            environment = service.get("environment")
            if (
                not isinstance(environment, dict)
                or environment.get("H2HDB_DATABASE_HOST") != "database"
                or environment.get("H2HDB_DATABASE_NAME") != DATABASE_NAME
            ):
                raise ValueError("Role database access escaped the test database")
            if role == "opds" and any(
                "WRITER" in key or "ROOT" in key for key in environment
            ):
                raise ValueError("Reader received writer/root credentials")
        volumes = service.get("volumes")
        if not isinstance(volumes, list):
            raise ValueError("Isolated mounts must be explicit")
        seen: set[str] = set()
        for volume in volumes:
            if not isinstance(volume, dict) or not isinstance(
                volume.get("target"), str
            ):
                raise ValueError("Malformed isolated mount")
            target = volume["target"]
            if target in seen:
                raise ValueError("Duplicated isolated mount")
            seen.add(target)
            if volume.get("type") == "volume":
                if name != "database" or volume != {
                    "type": "volume",
                    "source": "database-data",
                    "target": "/var/lib/mysql",
                }:
                    raise ValueError("Unknown acceptance volume")
            elif volume.get("type") == "bind":
                source = _inside(root, Path(volume["source"]))
                expected = expected_binds.get(target)
                if (
                    expected is None
                    or volume != _bind(source, target, expected[1])
                    or source != expected[0]
                ):
                    raise ValueError(
                        "Acceptance bind target/source/access contract changed"
                    )
            else:
                raise ValueError("Unknown acceptance mount type")
        expected_targets = set(expected_binds) | (
            {"/var/lib/mysql"} if name == "database" else set()
        )
        if seen != expected_targets:
            raise ValueError("An isolated mount is missing")


def _configurations(
    log_level: str, resident: Mapping[str, object] | None
) -> dict[str, Any]:
    if log_level not in {"info", "debug"}:
        raise ValueError("Acceptance logging must be info or explicit debug")
    common = {
        "sql_type": "mariadb",
        "host": "${H2HDB_DATABASE_HOST}",
        "port": "${H2HDB_DATABASE_PORT}",
        "database": "${H2HDB_DATABASE_NAME}",
    }
    writer = {
        "database": {
            **common,
            "user": "${H2HDB_DATABASE_WRITER_USER}",
            "password": "${H2HDB_DATABASE_WRITER_PASSWORD}",
        },
        "logger": {"level": log_level},
        "maintenance": {"optimize_enabled": False},
    }
    reader = {
        "database": {
            **common,
            "user": "${H2HDB_DATABASE_READER_USER}",
            "password": "${H2HDB_DATABASE_READER_PASSWORD}",
            "access_mode": "read-only",
        },
        "logger": {"level": log_level},
        "maintenance": {"optimize_enabled": False},
    }
    settings: dict[str, object] = {
        "source_quiet_seconds": 0.05,
        "source_probe_interval_seconds": 0.05,
        "source_max_wait_seconds": 10,
        "poll_seconds": 1,
        "lease_seconds": 60,
        "heartbeat_seconds": 10,
        "max_rows": 128,
        "progress_log_interval_seconds": 1,
    }
    if resident is not None:
        if set(resident) - (set(settings) | {"publication_batch_galleries"}):
            raise ValueError("Unknown resident acceptance setting")
        settings.update(resident)
    return {
        "h2hdb-config.json": writer,
        "h2hdb-reader-config.json": reader,
        "h2hdb-ingest.json": {
            "core": writer,
            "paths": {
                "download_path": "/hentai/download",
                "library_path": "/hentai/library",
                "page_render_workers": 1,
            },
            "resident": settings,
        },
        "h2hdb-opds.json": {
            "core": reader,
            "library_root": "/hentai/comics",
            "coordination_root": "/h2hdb-coordination",
            "public_base_url": "http://h2hdb-opds:8000",
            "server": {"host": "0.0.0.0", "port": 8000},
        },
    }


def prepare_deployment(
    deployment_root: Path,
    fixture_root: Path,
    *,
    project: str,
    images: Mapping[str, str],
    credentials: DatabaseCredentials,
    docker: Sequence[str] = ("docker",),
    uid: int = 65534,
    gid: int = 65534,
    log_level: str = "info",
    resident: Mapping[str, object] | None = None,
    instrumented: bool = False,
) -> PreparedDeployment:
    """Write new synthetic inputs and a derived model; never execute containers."""
    root = _fixture_root(fixture_root)
    model = load_deployment(deployment_root, docker=docker)
    derived = derive_compose(
        model,
        root,
        project=project,
        images=images,
        credentials=credentials,
        uid=uid,
        gid=gid,
        instrumented=instrumented,
    )
    configurations = _configurations(log_level, resident)
    wrapper_bytes = {}
    for name in WRAPPERS:
        source = deployment_root / "runtime" / name
        if source.is_symlink() or not source.is_file():
            raise ValueError("Runtime wrapper must be a regular file")
        wrapper_bytes[name] = source.read_bytes()
    directories = (
        "config",
        "source",
        "library",
        "library/current",
        "library/current/acquisitions",
        "library/current/artwork",
        "library/.h2hdb-coordination",
        "runtime",
        "probes",
        "evidence",
        "control",
        "database-init",
    )
    root.mkdir(parents=True, exist_ok=True)
    root.chmod(0o755)
    for name in directories:
        directory = _inside(root, root / name)
        directory.mkdir(parents=True, exist_ok=True)
        directory.chmod(
            0o777 if name.startswith("library") or name == "evidence" else 0o755
        )
    for name, content in wrapper_bytes.items():
        with (root / "runtime" / name).open("xb") as stream:
            stream.write(content)
    for name, content in configurations.items():
        with (root / "config" / name).open("x", encoding="utf-8") as stream:
            json.dump(content, stream, indent=2)
            stream.write("\n")
    with (root / "database-init" / "reader.sql").open("x", encoding="utf-8") as stream:
        stream.write(
            f"CREATE USER '{READER_USER}'@'%' IDENTIFIED BY '{credentials.reader_password}';\nGRANT SELECT, SHOW VIEW ON `{DATABASE_NAME}`.* TO '{READER_USER}'@'%';\n"
        )
    compose_path = root / "compose.json"
    with compose_path.open("x", encoding="utf-8") as stream:
        json.dump(derived, stream, indent=2)
        stream.write("\n")
    report = {
        "deployment_compose_sha256": hashlib.sha256(
            (deployment_root / "docker-compose.yml").read_bytes()
        ).hexdigest(),
        "runtime_sha256": {
            name: hashlib.sha256(content).hexdigest()
            for name, content in wrapper_bytes.items()
        },
        "removed_services": sorted(set(model["services"]) - set(SERVICES.values())),
        "image_ids": dict(images),
        "project": project,
        "overrides": {
            name: sorted(
                field
                for field in set(model["services"][name])
                | set(derived["services"][name])
                if model["services"][name].get(field)
                != derived["services"][name].get(field)
            )
            for name in SERVICES.values()
        },
        "top_level_overrides": sorted(
            {"name", "networks", "volumes"}
            | {key for key in model if key.startswith("x-")}
        ),
        "added_services": ["database"],
        "log_level": log_level,
        "instrumented": instrumented,
        "configuration": {
            "synthetic": True,
            "reader_database_grant": "SELECT, SHOW VIEW",
            "resident": configurations["h2hdb-ingest.json"]["resident"],
            "maintenance_optimize_enabled": False,
        },
    }
    return PreparedDeployment(
        compose_path,
        root / "config",
        root / "source",
        root / "library",
        root / "probes",
        root / "evidence",
        root / "control",
        report,
    )
