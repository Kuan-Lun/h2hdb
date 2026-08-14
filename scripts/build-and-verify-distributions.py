#!/usr/bin/env python3
"""Build fresh distributions and verify the installed-package boundary."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import venv
import zipfile
from pathlib import Path, PurePosixPath


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


def _verify_wheel_archive(wheel: Path) -> None:
    with zipfile.ZipFile(wheel) as archive:
        member_names = archive.namelist()
        forbidden_members = [
            name
            for name in member_names
            if "scripts" in PurePosixPath(name).parts or "bootstrap" in name.casefold()
        ]

    if forbidden_members:
        raise RuntimeError(
            "The wheel contains source-only administration files: "
            f"{forbidden_members}."
        )


def _verify_installed_cli(wheel: Path, scratch: Path) -> None:
    pip_environment = scratch / "pip-venv"
    venv.EnvBuilder(with_pip=True).create(pip_environment)
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
import json
from pathlib import Path

import h2hdb

print(json.dumps({
    "package_path": str(Path(h2hdb.__file__).resolve()),
    "environment_value": h2hdb.resolve_environment_placeholders(
        "${H2HDB_DISTRIBUTION_PROBE}"
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
    help_text = _run(
        sys.executable,
        "-m",
        "h2hdb",
        "--help",
        cwd=scratch,
        env=environment,
    )
    supported_commands = (
        "migrate",
        "check",
        "ready",
        "epoch-v2-initialize",
        "epoch-v2-check",
        "epoch-v2-ready",
    )
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
        _verify_installed_cli(wheel, scratch)
        _copy_verified_artifacts(
            wheel=wheel,
            sdist=sdist,
            output_directory=args.output_directory.resolve(),
        )

    print("Verified fresh h2hdb distributions and supported wheel CLI commands.")


if __name__ == "__main__":
    main()
