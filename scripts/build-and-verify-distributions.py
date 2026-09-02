#!/usr/bin/env python3
"""Build fresh distributions and verify the installed-package boundary."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

FORBIDDEN_LEGACY_MODULE_MEMBERS = frozenset(
    {
        "h2hdb/canonical_repository.py",
        "h2hdb/catalog_analysis_repository.py",
        "h2hdb/catalog_build_repository.py",
        "h2hdb/catalog_operational_repository.py",
        "h2hdb/catalog_projection_build_repository.py",
        "h2hdb/catalog_repository.py",
        "h2hdb/gallery_deduplication.py",
        "h2hdb/hash_dict.py",
        "h2hdb/information.py",
        "h2hdb/migrations.py",
        "h2hdb/service.py",
        "h2hdb/table_comments.py",
        "h2hdb/table_database_maintenance.py",
        "h2hdb/table_database_setting.py",
        "h2hdb/table_files_dbids.py",
        "h2hdb/table_gallery_ingest_coordination.py",
        "h2hdb/table_gallery_source_manifests.py",
        "h2hdb/table_gids.py",
        "h2hdb/table_removed_gids.py",
        "h2hdb/table_tags.py",
        "h2hdb/table_times.py",
        "h2hdb/table_titles.py",
        "h2hdb/table_uploadaccounts.py",
        "h2hdb/todelete_queue.py",
        "h2hdb/todownload_queue.py",
        "h2hdb/view_ginfo.py",
    }
)
REQUIRED_SCHEMA_ARTIFACT_MEMBERS = (
    "h2hdb/_generated_vnext_schema.py",
    "h2hdb/_generated_vnext_schema.bin",
    "h2hdb/_schema_artifact_codec.py",
)


def _run(*command: str, cwd: Path, env: dict[str, str] | None = None) -> str:
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        rendered_command = " ".join(command)
        raise RuntimeError(
            f"Command failed with exit code {completed.returncode}: "
            f"{rendered_command}\nstdout:\n{completed.stdout}\nstderr:\n"
            f"{completed.stderr}"
        )
    return completed.stdout


def _single_artifact(directory: Path, pattern: str) -> Path:
    matches = sorted(directory.glob(pattern))
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected exactly one {pattern!r} artifact in {directory}; "
            f"found {[path.name for path in matches]}."
        )
    return matches[0]


def _forbidden_legacy_members(member_names: list[str]) -> list[str]:
    forbidden_parts = tuple(
        PurePosixPath(member).parts for member in FORBIDDEN_LEGACY_MODULE_MEMBERS
    )
    return sorted(
        name
        for name in member_names
        if any(
            PurePosixPath(name).parts[-len(parts) :] == parts
            for parts in forbidden_parts
        )
    )


def _verify_wheel_archive(wheel: Path) -> None:
    with zipfile.ZipFile(wheel) as archive:
        member_names = archive.namelist()
        forbidden_members = [
            name
            for name in member_names
            if "scripts" in PurePosixPath(name).parts or "bootstrap" in name.casefold()
        ]
        forbidden_legacy_members = _forbidden_legacy_members(member_names)
        missing_schema_members = sorted(
            set(REQUIRED_SCHEMA_ARTIFACT_MEMBERS) - set(member_names)
        )
        schema_sizes = {
            member: archive.getinfo(member).file_size
            for member in REQUIRED_SCHEMA_ARTIFACT_MEMBERS
            if member in member_names
        }

    if forbidden_members:
        raise RuntimeError(
            f"The wheel contains source-only administration files: {forbidden_members}."
        )
    if forbidden_legacy_members:
        raise RuntimeError(
            f"The wheel contains removed legacy modules: {forbidden_legacy_members}."
        )
    if missing_schema_members:
        raise RuntimeError(
            f"The wheel is missing schema artifact members: {missing_schema_members}."
        )
    if schema_sizes["h2hdb/_generated_vnext_schema.py"] >= 1024 * 1024:
        raise RuntimeError("The wheel schema artifact loader exceeds 1 MiB.")
    if schema_sizes["h2hdb/_generated_vnext_schema.bin"] >= 2 * 1024 * 1024:
        raise RuntimeError("The wheel compressed schema artifact exceeds 2 MiB.")


def _verify_sdist_archive(sdist: Path) -> None:
    with tarfile.open(sdist, mode="r:gz") as archive:
        member_names = archive.getnames()
        forbidden_legacy_members = _forbidden_legacy_members(member_names)
        missing_schema_members = sorted(
            required
            for required in REQUIRED_SCHEMA_ARTIFACT_MEMBERS
            if not any(
                PurePosixPath(name).parts[-len(PurePosixPath(required).parts) :]
                == PurePosixPath(required).parts
                for name in member_names
            )
        )
    if forbidden_legacy_members:
        raise RuntimeError(
            f"The sdist contains removed legacy modules: {forbidden_legacy_members}."
        )
    if missing_schema_members:
        raise RuntimeError(
            f"The sdist is missing schema artifact members: {missing_schema_members}."
        )


def _verify_direct_wheel_zipimport(wheel: Path, scratch: Path) -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(wheel.resolve())
    probe_source = """
import json
import sys

import h2hdb

artifact_loaded_on_package_import = "h2hdb._generated_vnext_schema" in sys.modules
from h2hdb._generated_vnext_schema import ARTIFACT

print(json.dumps({
    "package_file": h2hdb.__file__,
    "artifact_loaded_on_package_import": artifact_loaded_on_package_import,
    "artifact_epoch": ARTIFACT["epoch"],
    "sqlite_relations": len(ARTIFACT["backends"]["sqlite"]["relations"]),
}))
"""
    probe = json.loads(
        _run(
            sys.executable,
            "-c",
            probe_source,
            cwd=scratch,
            env=environment,
        )
    )
    if str(wheel.resolve()) not in str(probe["package_file"]):
        raise RuntimeError("Direct wheel probe did not import from the wheel archive.")
    if probe["artifact_loaded_on_package_import"]:
        raise RuntimeError(
            "Direct wheel package import eagerly loaded the schema artifact."
        )
    if probe["artifact_epoch"] != 3 or probe["sqlite_relations"] <= 0:
        raise RuntimeError(
            "Direct wheel zipimport could not decode the schema resource."
        )


def _verify_installed_cli(wheel: Path, scratch: Path) -> None:
    pip_environment = scratch / "pip-venv"
    _run(
        sys.executable,
        "-m",
        "venv",
        str(pip_environment),
        cwd=scratch,
    )
    if os.name == "nt":
        pip_python = pip_environment / "Scripts" / "python.exe"
    else:
        pip_python = pip_environment / "bin" / "python"
    install_root = scratch / "installed-wheel"
    _run(
        str(pip_python),
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        "--no-deps",
        "--target",
        str(install_root),
        str(wheel),
        cwd=scratch,
    )

    environment = os.environ.copy()
    existing_python_path = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = os.pathsep.join(
        path for path in (str(install_root), existing_python_path) if path
    )
    probe_source = """
import importlib.util
import inspect
import json
import sys
from pathlib import Path

import h2hdb
from h2hdb import __main__ as h2hdb_cli

artifact_loaded_on_package_import = "h2hdb._generated_vnext_schema" in sys.modules
from h2hdb._generated_vnext_schema import ARTIFACT
from h2hdb.vnext_schema_provider import GeneratedVNextSchemaProvider

provider = GeneratedVNextSchemaProvider("sqlite")

print(json.dumps({
    "package_path": str(Path(h2hdb.__file__).resolve()),
    "environment_value": h2hdb.resolve_environment_placeholders(
        "${H2HDB_DISTRIBUTION_PROBE}"
    ),
    "artifact_loaded_on_package_import": artifact_loaded_on_package_import,
    "artifact_epoch": ARTIFACT["epoch"],
    "provider_relation_count": len(provider.generated_definition_data["relations"]),
    "forbidden_public_exports": sorted(
        name for name in ("H2HDB", "MigrationRunner") if name in vars(h2hdb)
    ),
    "forbidden_legacy_modules": sorted(
        name
        for name in ("h2hdb.migrations", "h2hdb.service")
        if importlib.util.find_spec(name) is not None
    ),
    "provider_injection_surfaces": sorted(
        name
        for name, operation in (
            ("VNextDatabaseAdminFacade.initialize", h2hdb.VNextDatabaseAdminFacade.initialize),
            ("VNextDatabaseAdminFacade.check", h2hdb.VNextDatabaseAdminFacade.check),
            ("VNextDatabaseAdminFacade.check_readiness", h2hdb.VNextDatabaseAdminFacade.check_readiness),
            ("open_database", h2hdb.open_database),
            ("h2hdb.__main__.main", h2hdb_cli.main),
        )
        if "provider" in inspect.signature(operation).parameters
    ),
}))
"""
    environment["H2HDB_DISTRIBUTION_PROBE"] = "resolved-from-installed-wheel"
    probe = json.loads(
        _run(
            sys.executable,
            "-c",
            probe_source,
            cwd=scratch,
            env=environment,
        )
    )
    installed_package = Path(str(probe["package_path"]))
    if not installed_package.is_relative_to(install_root.resolve()):
        raise RuntimeError(
            f"Package probe imported {installed_package}, not the installed wheel."
        )
    if probe["environment_value"] != "resolved-from-installed-wheel":
        raise RuntimeError(
            "Installed wheel did not expose the environment placeholder resolver."
        )
    if probe["artifact_loaded_on_package_import"]:
        raise RuntimeError(
            "Plain installed-package import eagerly loaded the schema artifact."
        )
    if probe["artifact_epoch"] != 3 or probe["provider_relation_count"] <= 0:
        raise RuntimeError(
            "Installed wheel could not decode its generated schema artifact."
        )
    if probe["forbidden_public_exports"]:
        raise RuntimeError(
            "Installed wheel exposes removed legacy API names: "
            f"{probe['forbidden_public_exports']}."
        )
    if probe["forbidden_legacy_modules"]:
        raise RuntimeError(
            "Installed wheel still contains removed legacy modules: "
            f"{probe['forbidden_legacy_modules']}."
        )
    if probe["provider_injection_surfaces"]:
        raise RuntimeError(
            "Installed wheel exposes caller-injected schema providers: "
            f"{probe['provider_injection_surfaces']}."
        )
    help_text = _run(
        sys.executable,
        "-m",
        "h2hdb",
        "--help",
        cwd=scratch,
        env=environment,
    )
    supported_commands = ("migrate", "check", "ready")
    expected_commands = f"{{{','.join(supported_commands)}}}"
    if expected_commands not in help_text:
        raise RuntimeError(f"Unexpected h2hdb CLI commands:\n{help_text}")


def _copy_verified_artifacts(
    *, wheel: Path, sdist: Path, output_directory: Path
) -> None:
    output_directory.mkdir(parents=True, exist_ok=True)
    existing_artifacts = sorted(
        (*output_directory.glob("*.whl"), *output_directory.glob("*.tar.gz"))
    )
    if existing_artifacts:
        raise RuntimeError(
            "Output directory already contains distribution artifacts; use an empty "
            f"directory instead: {[path.name for path in existing_artifacts]}."
        )
    shutil.copy2(wheel, output_directory / wheel.name)
    shutil.copy2(sdist, output_directory / sdist.name)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build and verify the h2hdb sdist and wheel boundaries."
    )
    parser.add_argument("--output-directory", required=True, type=Path)
    args = parser.parse_args()

    repository_root = Path(__file__).resolve().parent.parent
    with tempfile.TemporaryDirectory(prefix="h2hdb-distributions-") as temporary:
        scratch = Path(temporary)
        build_directory = scratch / "build"
        _run(
            sys.executable,
            "-m",
            "build",
            "--outdir",
            str(build_directory),
            cwd=repository_root,
        )
        wheel = _single_artifact(build_directory, "*.whl")
        sdist = _single_artifact(build_directory, "*.tar.gz")
        _verify_wheel_archive(wheel)
        _verify_sdist_archive(sdist)
        _run(
            sys.executable,
            str(repository_root / "scripts" / "verify-schema-surface.py"),
            "--wheel",
            str(wheel),
            cwd=repository_root,
        )
        _verify_direct_wheel_zipimport(wheel, scratch)
        _verify_installed_cli(wheel, scratch)
        _copy_verified_artifacts(
            wheel=wheel,
            sdist=sdist,
            output_directory=args.output_directory.resolve(),
        )

    print("Verified fresh h2hdb distributions and supported wheel CLI commands.")


if __name__ == "__main__":
    main()
