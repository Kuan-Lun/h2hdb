#!/usr/bin/env python3
"""Audit every direct Python and Node dependency and write a review receipt."""

from __future__ import annotations

import argparse
import datetime as dt
import functools
import hashlib
import json
import subprocess
import sys
import tomllib
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, cast

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.version import Version


def _dependency_manifest(document: dict[str, Any], package: dict[str, Any]) -> bytes:
    project = document.get("project", {})
    manifest = {
        "build": document.get("build-system", {}).get("requires", []),
        "dependency-groups": document.get("dependency-groups", {}),
        "optional": project.get("optional-dependencies", {}),
        "runtime": project.get("dependencies", []),
        "node-dev": package.get("devDependencies", {}),
    }
    return json.dumps(
        manifest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _python_requirements(document: dict[str, Any]) -> list[tuple[str, str]]:
    project = document.get("project", {})
    grouped: list[tuple[str, list[str]]] = [
        ("build", document.get("build-system", {}).get("requires", [])),
        ("runtime", project.get("dependencies", [])),
    ]
    grouped.extend(
        (f"optional:{name}", requirements)
        for name, requirements in project.get("optional-dependencies", {}).items()
    )
    grouped.extend(
        (f"group:{name}", requirements)
        for name, requirements in document.get("dependency-groups", {}).items()
    )
    return [
        (group, requirement)
        for group, requirements in grouped
        for requirement in requirements
        if isinstance(requirement, str)
    ]


@functools.cache
def _pypi_json(name: str) -> dict[str, Any]:
    normalized = urllib.parse.quote(name, safe="")
    request = urllib.request.Request(
        f"https://pypi.org/pypi/{normalized}/json",
        headers={"User-Agent": "h2h-dependency-audit/1"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise ValueError(f"PyPI returned a non-object payload for {name}")
    return cast(dict[str, Any], payload)


def _supports_python_314(files: list[dict[str, Any]]) -> bool:
    for file in files:
        if file.get("yanked"):
            continue
        requires_python = file.get("requires_python")
        if not requires_python or Version("3.14") in SpecifierSet(requires_python):
            return True
    return False


def _audit_python(group: str, declared: str) -> dict[str, Any]:
    requirement = Requirement(declared)
    if requirement.url is not None:
        return {
            "declared": declared,
            "group": group,
            "latest": None,
            "latest_satisfies": None,
            "name": requirement.name,
            "note": "direct URL/ref requires manual upstream review",
            "python_3_14": None,
        }
    payload = _pypi_json(requirement.name)
    latest = str(payload["info"]["version"])
    return {
        "declared": declared,
        "group": group,
        "latest": latest,
        "latest_satisfies": Version(latest) in requirement.specifier,
        "name": requirement.name,
        "python_3_14": _supports_python_314(payload["releases"].get(latest, [])),
        "url": payload["info"].get("project_url") or payload["info"].get("package_url"),
    }


def _npm_view_versions(specification: str) -> tuple[str, ...]:
    completed = subprocess.run(
        ("npm", "view", specification, "version", "--json"),
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    payload = json.loads(completed.stdout)
    if isinstance(payload, str):
        return (payload,)
    if isinstance(payload, list) and all(isinstance(value, str) for value in payload):
        return tuple(payload)
    raise ValueError(f"npm returned invalid version data for {specification}")


def _audit_node(name: str, declared: str) -> dict[str, Any]:
    latest_versions = _npm_view_versions(name)
    if len(latest_versions) != 1:
        raise ValueError(f"npm returned more than one latest version for {name}")
    latest = latest_versions[0]
    satisfying_versions = _npm_view_versions(f"{name}@{declared}")
    return {
        "declared": declared,
        "latest": latest,
        "latest_satisfies": latest in satisfying_versions,
        "name": name,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--review-note", required=True)
    parser.add_argument(
        "--output",
        default=".release/dependency-audit.json",
        type=Path,
    )
    arguments = parser.parse_args()
    if not arguments.review_note.strip():
        parser.error("--review-note must not be empty")

    with Path("pyproject.toml").open("rb") as stream:
        document = tomllib.load(stream)
    package = json.loads(Path("package.json").read_text())
    python_audit = [
        _audit_python(group, requirement)
        for group, requirement in _python_requirements(document)
    ]
    node_audit = [
        _audit_node(name, declared)
        for name, declared in package.get("devDependencies", {}).items()
    ]
    receipt = {
        "checked_at": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat(),
        "manifest_sha256": hashlib.sha256(
            _dependency_manifest(document, package)
        ).hexdigest(),
        "node": node_audit,
        "project_version": document["project"]["version"],
        "python": python_audit,
        "review": {
            "note": arguments.review_note.strip(),
            "status": "reviewed",
        },
        "schema": "h2h.dependency-audit.v1",
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, subprocess.CalledProcessError, ValueError) as error:
        print(f"audit-dependencies: {error}", file=sys.stderr)
        raise SystemExit(1) from error
