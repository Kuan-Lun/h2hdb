"""Install a CI dependency subset using the project's declared requirements."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
from pathlib import Path


def _package_name(requirement: str) -> str:
    match = re.match(r"[A-Za-z0-9][A-Za-z0-9._-]*", requirement)
    if match is None:
        raise ValueError(f"Invalid dependency name: {requirement!r}")
    return re.sub(r"[-_.]+", "-", match[0]).lower()


def requirements_for(
    manifest: Path, packages: list[str], *, pytest_plugins: bool
) -> list[str]:
    with manifest.open("rb") as stream:
        document = tomllib.load(stream)
    project = document["project"]
    declared = [
        *project.get("dependencies", []),
        *project.get("optional-dependencies", {}).get("dev", []),
    ]
    requirements: list[str] = []
    for package in packages:
        matches = [
            requirement
            for requirement in declared
            if _package_name(requirement) == _package_name(package)
        ]
        if not matches:
            raise ValueError(f"Dependency is not declared in {manifest}: {package}")
        requirements.extend(matches)
    if pytest_plugins:
        requirements.extend(
            document["tool"]["pytest"]["ini_options"]["required_plugins"]
        )
    return list(dict.fromkeys(requirements))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("packages", nargs="+", help="runtime or dev dependency names")
    parser.add_argument(
        "--pytest-plugins",
        action="store_true",
        help="also install every plugin required by the repository pytest settings",
    )
    arguments = parser.parse_args()
    manifest = Path(__file__).resolve().parents[1] / "pyproject.toml"
    try:
        requirements = requirements_for(
            manifest, arguments.packages, pytest_plugins=arguments.pytest_plugins
        )
    except (KeyError, OSError, ValueError) as error:
        parser.error(str(error))
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--upgrade", *requirements],
        check=True,
    )


if __name__ == "__main__":
    main()
