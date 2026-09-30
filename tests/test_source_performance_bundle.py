"""Portable source calibration exports must not depend on running deployment state."""

from __future__ import annotations

import copy
import importlib.util
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest


def module(name: str) -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), path)
    assert spec is not None and spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


@pytest.fixture
def builder() -> ModuleType:
    return module("build-source-performance-bundle")


def wheel(root: Path, name: str, version: str, content: bytes) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    package = name.replace("-", "_")
    result = root / f"{package}-{version}-py3-none-any.whl"
    with zipfile.ZipFile(result, "w") as archive:
        archive.writestr(f"{package}/__init__.py", content)
        archive.writestr(
            f"{package}-{version}.dist-info/METADATA",
            f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n",
        )
    return result


def inputs(tmp_path: Path, builder: ModuleType) -> dict[str, Path]:
    values = {"output": tmp_path / "bundle"}
    for arm, version in (("baseline", "0.30.1"), ("candidate", "0.30.2")):
        checkout = tmp_path / arm
        package = checkout / "src" / "h2hdb_ingest"
        package.mkdir(parents=True)
        content = f'VERSION = "{version}"\n'.encode()
        (package / "__init__.py").write_bytes(content)
        (checkout / "pyproject.toml").write_text(
            f'[project]\nname = "h2hdb-ingest"\nversion = "{version}"\n'
        )
        # Deliberately sensitive/unrelated files must stay out of the export.
        (checkout / ".env").write_text("DO_NOT_COPY=private\n")
        (checkout / "source-photo.jpg").write_bytes(b"private gallery")
        scripts = checkout / "scripts"
        scripts.mkdir()
        for name in builder.PROBES:
            (scripts / name).write_text(f"# {arm} {name}\n")
        values[arm + "_checkout"] = checkout
        values[arm + "_wheel"] = wheel(
            tmp_path / "wheels" / arm, "h2hdb-ingest", version, content
        )
    values["core_wheel"] = wheel(
        tmp_path / "wheels", "h2hdb", "0.45.0", b"CORE = True\n"
    )
    values["probe_checkout"] = values["candidate_checkout"]
    return values


def test_nested_export_does_not_borrow_parent_git_identity(
    tmp_path: Path, builder: ModuleType
) -> None:
    subprocess.run(["git", "init", str(tmp_path)], check=True, capture_output=True)
    export = tmp_path / "export"
    export.mkdir()
    result = builder.checkout_revision(export)
    assert result["commit"] is None
    assert result["dirty"] is None
    assert result["scope"] == "source export nested inside another Git checkout"


def test_export_checks_exact_sources_and_uses_one_probe_snapshot(
    tmp_path: Path, builder: ModuleType
) -> None:
    values = inputs(tmp_path, builder)
    manifest = builder.build_bundle(**values)
    root = values["output"]
    assert manifest["arms"]["baseline"]["wheel"]["version"] == "0.30.1"
    assert manifest["arms"]["candidate"]["wheel"]["version"] == "0.30.2"
    for name in builder.PROBES:
        before = (root / "payload" / "baseline" / "scripts" / name).read_bytes()
        after = (root / "payload" / "candidate" / "scripts" / name).read_bytes()
        assert (
            before
            == after
            == (values["probe_checkout"] / "scripts" / name).read_bytes()
        )
    assert not list(root.rglob(".env"))
    assert not list(root.rglob("*.jpg"))
    assert not list(root.rglob(".git"))
    dockerfile = (root / "Dockerfile").read_text()
    assert "--no-deps" not in dockerfile
    assert "-m pytest" not in dockerfile
    assert dockerfile.count("-m pip check") == 2
    assert dockerfile.count("--report") == 2
    assert "verify-build" in dockerfile
    assert "--kill-after=10s" in dockerfile
    runtime = module("source_performance_bundle")
    assert runtime.verified_manifest(root / "payload")["core"] == manifest["core"]
    (
        root / "payload" / "baseline" / "src" / "h2hdb_ingest" / "__init__.py"
    ).write_bytes(b"changed")
    with pytest.raises(ValueError, match="fingerprint"):
        runtime.verified_manifest(root / "payload")


@pytest.mark.parametrize(
    "mutation", ["source", "version", "wheel-name", "missing-probe", "symlink"]
)
def test_invalid_inputs_fail_before_creating_output(
    tmp_path: Path, builder: ModuleType, mutation: str
) -> None:
    values = inputs(tmp_path, builder)
    source = values["candidate_checkout"] / "src" / "h2hdb_ingest" / "__init__.py"
    if mutation == "source":
        source.write_bytes(b"different runtime")
    elif mutation == "version":
        (values["candidate_checkout"] / "pyproject.toml").write_text(
            '[project]\nname="h2hdb-ingest"\nversion="0.99.0"\n'
        )
    elif mutation == "wheel-name":
        values["candidate_wheel"] = values["core_wheel"]
    elif mutation == "missing-probe":
        (values["probe_checkout"] / "scripts" / builder.PROBES[0]).unlink()
    else:
        source.unlink()
        source.symlink_to(
            values["baseline_checkout"] / "src" / "h2hdb_ingest" / "__init__.py"
        )
    with pytest.raises(ValueError):
        builder.build_bundle(**values)
    assert not values["output"].exists()


def test_export_never_overwrites_existing_output(
    tmp_path: Path, builder: ModuleType
) -> None:
    values = inputs(tmp_path, builder)
    values["output"].mkdir()
    marker = values["output"] / "keep"
    marker.write_bytes(b"existing")
    with pytest.raises(ValueError, match="new directory"):
        builder.build_bundle(**values)
    assert marker.read_bytes() == b"existing"


@pytest.mark.parametrize("mutation", [None, "version", "source", "wheel-origin"])
def test_installed_arm_requires_version_source_and_wheel_provenance(
    tmp_path: Path,
    builder: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str | None,
) -> None:
    values = inputs(tmp_path, builder)
    manifest = builder.build_bundle(**values)
    root = values["output"] / "payload"
    installed = tmp_path / "installed"
    packages = {}
    for name, expected in (
        ("h2hdb", manifest["core"]),
        ("h2hdb-ingest", manifest["arms"]["baseline"]["wheel"]),
    ):
        package = name.replace("-", "_")
        target = installed / package
        target.mkdir(parents=True)
        content = (
            b"CORE = True\n"
            if name == "h2hdb"
            else (root / "baseline" / "src" / package / "__init__.py").read_bytes()
        )
        (target / "__init__.py").write_bytes(content)
        direct = {"archive_info": {"hashes": {"sha256": expected["sha256"]}}}
        direct_text = json.dumps(direct)
        packages[name] = SimpleNamespace(
            version=expected["version"],
            metadata={"Name": name},
            locate_file=lambda module: installed / module,
            read_text=lambda _name, text=direct_text: text,
        )
    if mutation == "version":
        packages["h2hdb-ingest"].version = "0.99.0"
    elif mutation == "source":
        (installed / "h2hdb_ingest" / "__init__.py").write_bytes(b"wrong runtime")
    elif mutation == "wheel-origin":
        packages["h2hdb-ingest"].read_text = lambda _name: "{}"
    runtime = module("source_performance_bundle")
    monkeypatch.setattr(
        runtime.importlib.metadata, "distribution", packages.__getitem__
    )
    monkeypatch.setattr(
        runtime.importlib.metadata, "distributions", lambda: packages.values()
    )
    if mutation is None:
        evidence = runtime.installed_evidence(root, "baseline")
        assert (
            evidence["packages"]["h2hdb-ingest"]["wheel_sha256"]
            == manifest["arms"]["baseline"]["wheel"]["sha256"]
        )
    else:
        with pytest.raises(ValueError):
            runtime.installed_evidence(root, "baseline")


def reports() -> list[dict[str, Any]]:
    cases = [
        {
            "label": f"{codec}-{edge}",
            "workers": workers,
            "attribution_status": "complete",
            "fixture": {"manifest_sha256": f"{codec}-{edge}"},
            "qualification_wall_ns": 80,
            "complete_observation_wall_ns": 100,
            "process_cpu_ns": 50,
        }
        for codec in ("JPEG", "PNG", "GIF", "WEBP")
        for edge in (256, 1536)
        for workers in (1, 4)
    ]
    return [
        {"arm": arm, "report": {"status": "completed", "cases": copy.deepcopy(cases)}}
        for arm in ("baseline", "candidate", "candidate", "baseline")
    ]


def test_compare_reports_raw_change_without_claiming_sla() -> None:
    runtime = module("source_performance_bundle")
    values = reports()
    for run in values:
        if run["arm"] == "candidate":
            for case in run["report"]["cases"]:
                case["complete_observation_wall_ns"] = 75
    result = runtime.compare(values)
    assert len(result) == 16
    assert all(
        row["measurements"]["complete_observation_wall_ns"]["candidate_change_fraction"]
        == -0.25
        for row in result
    )
    assert not any("passed" in row or "sla" in row for row in result)


@pytest.mark.parametrize(
    "mutation", ["fixture", "case", "repeat", "attribution", "timing", "duplicate"]
)
def test_compare_rejects_unmatched_or_incomplete_evidence(mutation: str) -> None:
    runtime = module("source_performance_bundle")
    values = reports()
    case = values[1]["report"]["cases"][0]
    if mutation == "fixture":
        case["fixture"]["manifest_sha256"] = "changed"
    elif mutation == "case":
        values[1]["report"]["cases"].pop()
    elif mutation == "repeat":
        values.pop()
    elif mutation == "attribution":
        case["attribution_status"] = "incomplete"
    elif mutation == "timing":
        case["complete_observation_wall_ns"] = float("nan")
    else:
        values[1]["report"]["cases"].append(copy.deepcopy(case))
    with pytest.raises(ValueError):
        runtime.compare(values)


@pytest.mark.parametrize("build_exit,run_exit", [(0, 0), (8, 0), (0, 2)])
def test_entrypoint_builds_before_fresh_run_and_cleans_only_own_container(
    tmp_path: Path, builder: ModuleType, build_exit: int, run_exit: int
) -> None:
    values = inputs(tmp_path, builder)
    builder.build_bundle(**values)
    root = values["output"]
    commands = tmp_path / "commands.jsonl"
    binaries = tmp_path / "bin"
    binaries.mkdir()
    executable = binaries / "docker"
    executable.write_text(
        f"#!{sys.executable}\n"
        "import json,pathlib,sys\n"
        f"p=pathlib.Path({str(commands)!r})\n"
        "with p.open('a') as stream: stream.write(json.dumps(sys.argv[1:])+'\\n')\n"
        "args=sys.argv[1:]\n"
        f"if args[0]=='build': sys.exit({build_exit})\n"
        "if args[:2]==['image','inspect']: print('{}')\n"
        "if args[0]=='run':\n"
        "    pathlib.Path(args[args.index('--cidfile')+1]).write_text('a'*64)\n"
        f"    sys.exit({run_exit})\n"
    )
    executable.chmod(0o755)
    completed = subprocess.run(
        ["sh", str(root / "run.sh"), "results"],
        env={**os.environ, "PATH": str(binaries) + os.pathsep + os.environ["PATH"]},
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == (build_exit or run_exit)
    invoked = [json.loads(line) for line in commands.read_text().splitlines()]
    assert invoked[0][:3] == ["build", "--pull", "--no-cache"]
    assert not (root / "results" / "scratch").exists()
    if build_exit:
        assert len(invoked) == 1
    else:
        run = next(cmd for cmd in invoked if cmd[0] == "run")
        assert {"--rm", "--init", "--read-only", "--network", "none"} <= set(run)
        assert not any("compose" in cmd or "exec" in cmd for cmd in invoked)
        assert all(
            str(root / "results") in run[i + 1]
            for i, arg in enumerate(run)
            if arg == "--mount"
        )
        assert invoked[-1] == ["rm", "-f", "a" * 64]
    again = subprocess.run(
        ["sh", str(root / "run.sh"), "results"],
        env={**os.environ, "PATH": str(binaries) + os.pathsep + os.environ["PATH"]},
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert again.returncode != 0
    assert len(commands.read_text().splitlines()) == len(invoked)
