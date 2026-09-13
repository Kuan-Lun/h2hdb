from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


def _load_module() -> ModuleType:
    source = (
        Path(__file__).resolve().parents[1] / "scripts/deployment_acceptance/compose.py"
    )
    spec = importlib.util.spec_from_file_location(
        "deployment_acceptance_compose_under_test", source
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


compose = _load_module()

PROJECT = "h2hdb-acceptance-unit-test"
IMAGES = {
    role: "sha256:" + value * 64
    for role, value in (("ingest", "a"), ("opds", "b"), ("mariadb", "c"))
}
CREDENTIALS = compose.DatabaseCredentials("w" * 32, "r" * 32, "s" * 32)


def model() -> dict[str, Any]:
    def mount(source: str, target: str, readonly: bool = True) -> dict[str, Any]:
        return {
            "type": "bind",
            "source": source,
            "target": target,
            "read_only": readonly,
            "bind": {"create_host_path": False},
        }

    services = {}
    for role, name in compose.SERVICES.items():
        services[name] = {
            "command": ["bash", "/opt/h2hdb-main.sh", role],
            "image": f"production/{role}:latest",
            "build": {"context": "/production", "pull": True},
            "pull_policy": "build",
            "container_name": f"production-{role}",
            "env_file": [{"path": "/production/secret.env", "required": True}],
            "environment": {"PRIVATE": "never inherit"},
            "read_only": True,
            "init": True,
            "networks": {"production": None},
            "user": "${MEDIA_UID}:${MEDIA_GID}",
            "tmpfs": ["/tmp:uid=${MEDIA_UID},gid=${MEDIA_GID}"],
            "restart": "unless-stopped",
            "healthcheck": {
                "test": ["CMD", "true"],
                "interval": "30s",
                "start_period": "2m",
            },
        }
    services["h2hdb-ingest"]["volumes"] = [
        mount("/production/config", "/h2hdb-config"),
        mount("/production/source", "/hentai/download"),
        mount("/production/library", "/hentai/library", False),
    ]
    services["h2hdb-opds"].update(
        {
            "depends_on": {
                "h2hdb-ingest": {"condition": "service_started", "required": True}
            },
            "ports": [{"target": 8000, "published": "62300", "host_ip": "127.0.0.1"}],
            "volumes": [
                mount("/production/config", "/h2hdb-config"),
                mount("/production/library/current", "/hentai/comics"),
                mount("/production/library/.h2hdb-coordination", "/h2hdb-coordination"),
            ],
        }
    )
    services["komga"] = {"image": "not-used", "env_file": ["/private/komga.env"]}
    return {
        "name": "production",
        "services": services,
        "networks": {
            "production": {
                "name": "production",
                "ipam": {"config": [{"subnet": "172.30.93.0/24"}]},
            }
        },
    }


def derive(
    tmp_path: Path, source: dict[str, Any] | None = None, **kwargs: Any
) -> dict[str, Any]:
    result: dict[str, Any] = compose.derive_compose(
        model() if source is None else source,
        tmp_path / "fixture",
        project=PROJECT,
        images=IMAGES,
        credentials=CREDENTIALS,
        **kwargs,
    )
    return result


def test_derivation_preserves_runtime_contract_and_removes_external_resources(
    tmp_path: Path,
) -> None:
    original = model()
    before = copy.deepcopy(original)
    isolated = derive(tmp_path, original)
    assert original == before
    assert set(isolated["services"]) == {"database", "h2hdb-ingest", "h2hdb-opds"}
    for name in compose.SERVICES.values():
        service = isolated["services"][name]
        for field in ("command", "healthcheck", "restart", "read_only", "init"):
            assert service[field] == original["services"][name][field]
        assert service.get("depends_on") == original["services"][name].get("depends_on")
        assert not ({"build", "container_name", "env_file", "ports"} & set(service))
        assert service["pull_policy"] == "never"
        assert "PRIVATE" not in service["environment"]
        for volume in service["volumes"]:
            assert Path(volume["source"]).is_relative_to(tmp_path / "fixture")
            assert volume["bind"] == {"create_host_path": False}
        assert all(
            volume["read_only"]
            for volume in service["volumes"]
            if volume["target"]
            in {
                "/h2hdb-config",
                "/hentai/download",
                "/hentai/comics",
                "/h2hdb-coordination",
                "/acceptance",
                "/acceptance-control",
            }
        )
    assert "production" not in json.dumps(isolated)
    assert isolated["networks"]["isolated"]["internal"] is True
    assert isolated["volumes"] == {
        "database-data": {"labels": {compose.OWNER_LABEL: PROJECT}}
    }


@pytest.mark.parametrize(
    "field",
    [
        "privileged",
        "network_mode",
        "pid",
        "devices",
        "secrets",
        "configs",
        "entrypoint",
        "volumes_from",
        "extends",
        "extra_hosts",
    ],
)
def test_unknown_service_capabilities_fail_closed(tmp_path: Path, field: str) -> None:
    original = model()
    original["services"]["h2hdb-ingest"][field] = "unsafe"
    with pytest.raises(ValueError, match="Unsupported service fields"):
        derive(tmp_path, original)


@pytest.mark.parametrize(
    "change",
    ["unknown", "duplicate", "writable-source", "create-host", "different-library"],
)
def test_mount_contract_drift_is_rejected(tmp_path: Path, change: str) -> None:
    original = model()
    mounts = original["services"]["h2hdb-ingest"]["volumes"]
    if change == "unknown":
        mounts.append({**mounts[0], "target": "/var/run/docker.sock"})
    elif change == "duplicate":
        mounts.append(mounts[0])
    elif change == "writable-source":
        mounts[1]["read_only"] = False
    elif change == "create-host":
        mounts[0]["bind"]["create_host_path"] = True
    else:
        original["services"]["h2hdb-opds"]["volumes"][1]["source"] = "/other/current"
    with pytest.raises(ValueError):
        derive(tmp_path, original)


def test_actual_startup_contract_changes_are_not_hidden_by_fixture(
    tmp_path: Path,
) -> None:
    original = model()
    original["services"]["h2hdb-opds"]["depends_on"]["h2hdb-ingest"]["condition"] = (
        "service_healthy"
    )
    with pytest.raises(ValueError, match="dependency contract"):
        derive(tmp_path, original)


def test_compose_expanded_extensions_are_inert_and_commands_can_be_strings(
    tmp_path: Path,
) -> None:
    original = model()
    original["x-python-runtime"] = {"env_file": "/production/never-read.env"}
    for role, name in compose.SERVICES.items():
        original["services"][name]["command"] = f"bash /opt/h2hdb-main.sh {role}"
    isolated = derive(tmp_path, original)
    assert "x-python-runtime" not in isolated
    assert (
        isolated["services"]["h2hdb-ingest"]["command"]
        == "bash /opt/h2hdb-main.sh ingest"
    )
    original = model()
    original["services"]["h2hdb-ingest"]["command"][-1] = "bootstrap"
    with pytest.raises(ValueError, match="Unexpected deployment command"):
        derive(tmp_path, original)


def test_mutable_images_and_non_temporary_roots_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="immutable image IDs"):
        compose.derive_compose(
            model(),
            tmp_path,
            project=PROJECT,
            images={**IMAGES, "ingest": "image:latest"},
            credentials=CREDENTIALS,
        )
    with pytest.raises(ValueError, match="temporary directory"):
        compose.derive_compose(
            model(),
            Path("/production/library"),
            project=PROJECT,
            images=IMAGES,
            credentials=CREDENTIALS,
        )
    with pytest.raises(ValueError, match="project name"):
        compose.derive_compose(
            model(),
            tmp_path,
            project="production",
            images=IMAGES,
            credentials=CREDENTIALS,
        )


def test_symlink_bind_escape_and_later_model_escape_are_rejected(
    tmp_path: Path,
) -> None:
    root = tmp_path / "fixture"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "library").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="escapes fixture root"):
        derive(tmp_path)
    (root / "library").unlink()
    isolated = derive(tmp_path)
    isolated["services"]["h2hdb-opds"]["volumes"][0]["source"] = str(outside)
    with pytest.raises(ValueError, match="escapes fixture root"):
        compose.validate_isolation(isolated, root, project=PROJECT)


def test_instrumentation_is_explicit_and_reader_credentials_are_separate(
    tmp_path: Path,
) -> None:
    for instrumented in (False, True):
        isolated = derive(tmp_path, instrumented=instrumented)
        for role in compose.SERVICES.values():
            environment = isolated["services"][role]["environment"]
            assert ("PYTHONPATH" in environment) is instrumented
            assert ("H2HDB_ACCEPTANCE_PROBE_DIR" in environment) is instrumented
        reader = isolated["services"]["h2hdb-opds"]["environment"]
        writer = isolated["services"]["h2hdb-ingest"]["environment"]
        assert "H2HDB_ACCEPTANCE_CONTROL_DIR" not in reader
        assert ("H2HDB_ACCEPTANCE_CONTROL_DIR" in writer) is instrumented
        assert "H2HDB_DATABASE_WRITER_PASSWORD" not in reader
        assert "H2HDB_DATABASE_READER_PASSWORD" not in writer
        assert CREDENTIALS.writer_password not in json.dumps(reader)
        assert CREDENTIALS.root_password not in json.dumps(reader)


@pytest.mark.parametrize(
    "change",
    [
        "unknown-target",
        "writable-reader",
        "reader-secret",
        "mutable-image",
        "missing-mount",
    ],
)
def test_executor_revalidation_rejects_later_safety_drift(
    tmp_path: Path, change: str
) -> None:
    isolated = derive(tmp_path)
    reader = isolated["services"]["h2hdb-opds"]
    if change == "unknown-target":
        reader["volumes"][0]["target"] = "/other"
    elif change == "writable-reader":
        reader["volumes"][1]["read_only"] = False
    elif change == "reader-secret":
        reader["environment"]["H2HDB_DATABASE_WRITER_PASSWORD"] = (
            CREDENTIALS.writer_password
        )
    elif change == "mutable-image":
        reader["image"] = "image:latest"
    else:
        reader["volumes"].pop()
    with pytest.raises(ValueError):
        compose.validate_isolation(isolated, tmp_path / "fixture", project=PROJECT)


def test_sql_initialization_passwords_cannot_introduce_statements(
    tmp_path: Path,
) -> None:
    credentials = compose.DatabaseCredentials(
        "'; DROP DATABASE test; --", "r" * 32, "s" * 32
    )
    with pytest.raises(ValueError, match="synthetic alphanumeric passwords"):
        compose.derive_compose(
            model(), tmp_path, project=PROJECT, images=IMAGES, credentials=credentials
        )


def test_safe_parser_disables_dotenv_interpolation_and_does_not_inherit_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "docker-compose.yml").write_text("services: {}\n")
    (tmp_path / ".env").write_text("THIS_MUST_NOT_BE_READ=secret\n")
    monkeypatch.setenv("COMPOSE_FILE", "/production/compose.yml")
    monkeypatch.setenv("H2HDB_DATABASE_PASSWORD", "production-secret")
    monkeypatch.setattr(compose.shutil, "which", lambda _name: "/usr/bin/docker")

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert command[:4] == ["/usr/bin/docker", "--context", "testing", "compose"]
        assert command[command.index("--env-file") + 1] == os.devnull
        assert {
            "--no-interpolate",
            "--no-env-resolution",
            "--no-path-resolution",
        } <= set(command)
        assert kwargs["env"] == {"PATH": os.defpath}
        assert kwargs["timeout"] == 30
        assert "up" not in command and "inspect" not in command
        return subprocess.CompletedProcess(command, 0, json.dumps(model()), "")

    monkeypatch.setattr(compose.subprocess, "run", run)
    assert (
        compose.load_deployment(tmp_path, docker=("docker", "--context", "testing"))
        == model()
    )


def test_parser_failure_does_not_retry_without_safety_flags_or_echo_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "docker-compose.yml").write_text("services: {}\n")
    monkeypatch.setattr(compose.shutil, "which", lambda _name: "/usr/bin/docker")
    calls = []

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 1, "secret", "secret")

    monkeypatch.setattr(compose.subprocess, "run", run)
    with pytest.raises(ValueError, match="Safe Compose parsing failed") as error:
        compose.load_deployment(tmp_path)
    assert len(calls) == 1
    assert "secret" not in str(error.value)


def test_prepare_copies_wrapper_bytes_and_writes_only_synthetic_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    (deployment / "docker-compose.yml").write_text("public compose fixture\n")
    (deployment / "runtime").mkdir()
    for name in compose.WRAPPERS:
        (deployment / "runtime" / name).write_bytes(b"original bytes\r\n")
    monkeypatch.setattr(compose, "load_deployment", lambda *_args, **_kwargs: model())
    result = compose.prepare_deployment(
        deployment,
        tmp_path / "fixture",
        project=PROJECT,
        images=IMAGES,
        credentials=CREDENTIALS,
    )
    assert result.compose_path.is_file()
    for name in compose.WRAPPERS:
        assert (
            result.compose_path.parent / "runtime" / name
        ).read_bytes() == b"original bytes\r\n"
        assert (
            result.report["runtime_sha256"][name]
            == hashlib.sha256(b"original bytes\r\n").hexdigest()
        )
    assert result.report["removed_services"] == ["komga"]
    assert result.report["log_level"] == "info"
    assert result.report["instrumented"] is False
    assert "environment" in result.report["overrides"]["h2hdb-opds"]
    reader = json.loads((result.config_dir / "h2hdb-reader-config.json").read_text())
    assert reader["database"]["password"] == "${H2HDB_DATABASE_READER_PASSWORD}"
    assert reader["database"]["access_mode"] == "read-only"
    assert "WRITER" not in json.dumps(reader)
    assert CREDENTIALS.reader_password not in json.dumps(reader)
    assert CREDENTIALS.reader_password not in json.dumps(result.report)
    sql = (result.compose_path.parent / "database-init/reader.sql").read_text()
    assert "GRANT SELECT, SHOW VIEW ON `h2hdb_acceptance`.*" in sql
    assert "GRANT ALL" not in sql
    assert result.library_dir.stat().st_mode & 0o777 == 0o777
    assert result.evidence_dir.stat().st_mode & 0o777 == 0o777
    assert result.control_dir.stat().st_mode & 0o777 == 0o755
    with pytest.raises(FileExistsError):
        compose.prepare_deployment(
            deployment,
            tmp_path / "fixture",
            project=PROJECT,
            images=IMAGES,
            credentials=CREDENTIALS,
        )
