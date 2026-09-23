#!/usr/bin/env python3
"""Package an exact checkout wheel and the offline converter for Docker Compose."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import tarfile
import tempfile
import tomllib
import zipfile
from email.parser import BytesParser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONVERTER = "upgrade-source-collection-schema.py"


def _require_exact_wheel(wheel: Path, *, root: Path, version: str) -> None:
    with zipfile.ZipFile(wheel) as archive:
        metadata = archive.read(f"h2hdb-{version}.dist-info/METADATA")
        message = BytesParser().parsebytes(metadata)
        if message["Name"] != "h2hdb" or message["Version"] != version:
            raise ValueError("Wheel distribution differs from the checkout version")
        sources = {
            path.relative_to(root / "src").as_posix(): path.read_bytes()
            for path in (root / "src" / "h2hdb").rglob("*")
            if path.is_file() and "__pycache__" not in path.parts
        }
        installed = {
            name: archive.read(name)
            for name in archive.namelist()
            if name.startswith("h2hdb/") and not name.endswith("/")
        }
    if installed != sources:
        raise ValueError("Wheel runtime files differ from the checkout")


def _dockerfile(version: str, wheel_name: str) -> str:
    return f"""FROM python:3.14
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
LABEL io.h2hdb.tool="offline-schema7-to8" io.h2hdb.core.version="{version}"
COPY {wheel_name} /opt/h2hdb-upgrade/
RUN python -m pip install --no-cache-dir /opt/h2hdb-upgrade/{wheel_name} \\
    && python -m pip check \\
    && python -I -c "from importlib.metadata import version; assert version('h2hdb') == '{version}'" \\
    && rm /opt/h2hdb-upgrade/{wheel_name} \\
    && chmod 1777 /tmp
COPY {CONVERTER} /opt/h2hdb-upgrade/
RUN chmod 0755 /opt /opt/h2hdb-upgrade \\
    && chmod 0444 /opt/h2hdb-upgrade/{CONVERTER}
USER 65534:65534
RUN python -I /opt/h2hdb-upgrade/{CONVERTER} --help >/dev/null
ENTRYPOINT ["python", "-I", "-u", "/opt/h2hdb-upgrade/{CONVERTER}"]
CMD ["--help"]
"""


def _compose(version: str, deployment_root: str, network: str) -> str:
    def literal(value: str) -> str:
        # JSON quotes YAML syntax, while Compose separately requires $$ for a
        # literal dollar even inside quoted scalars. UID/GID below stay dynamic.
        return json.dumps(value.replace("$", "$$"))

    config = literal(str(Path(deployment_root) / "config"))
    database_env = literal(str(Path(deployment_root) / "env/database.env"))
    writer_env = literal(str(Path(deployment_root) / "env/writer.env"))
    return f"""name: h2hdb-schema8-upgrade
services:
  upgrade:
    build:
      context: .
      dockerfile: Dockerfile
    image: h2hdb-schema8-upgrade:{version}
    user: "${{MEDIA_UID:?set MEDIA_UID in deployment .env}}:${{MEDIA_GID:?set MEDIA_GID in deployment .env}}"
    init: true
    read_only: true
    cap_drop:
      - ALL
    security_opt:
      - no-new-privileges:true
    env_file:
      - {database_env}
      - {writer_env}
    volumes:
      - type: bind
        source: {config}
        target: /h2hdb-config
        read_only: true
        bind:
          create_host_path: false
      - type: volume
        target: /tmp
    networks:
      - h2hdb-backend
    restart: "no"
networks:
  h2hdb-backend:
    external: true
    name: {literal(network)}
"""


def build_bundle(
    *,
    root: Path,
    wheel: Path,
    output: Path,
    deployment_root: str,
    network: str,
) -> None:
    version = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
    _require_exact_wheel(wheel, root=root, version=version)
    if not Path(deployment_root).is_absolute() or not network:
        raise ValueError(
            "An absolute deployment root and an existing network are required"
        )
    if output.exists():
        raise FileExistsError(output)
    wheel_name = f"h2hdb-{version}-py3-none-any.whl"
    files = {
        wheel_name: wheel.read_bytes(),
        CONVERTER: (root / "scripts" / CONVERTER).read_bytes(),
        "Dockerfile": _dockerfile(version, wheel_name).encode(),
        "compose.yaml": _compose(version, deployment_root, network).encode(),
        ".dockerignore": (f"*\n!Dockerfile\n!{wheel_name}\n!{CONVERTER}\n").encode(),
    }
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()
    provenance = {
        "core_version": version,
        "checkout_commit": commit,
        "supported_upgrade": "exact schema 7 to schema 8; resume the same conversion",
        "files_sha256": {
            name: hashlib.sha256(data).hexdigest() for name, data in files.items()
        },
        "verification": "Every wheel runtime file matches this checkout. Bundle creation does not build an image or connect to a database.",
    }
    files["provenance.json"] = (json.dumps(provenance, indent=2) + "\n").encode()
    with tempfile.TemporaryDirectory(prefix="h2hdb-upgrade-bundle-") as temporary:
        directory = Path(temporary) / f"h2hdb-schema7-to8-docker-{version}"
        directory.mkdir(mode=0o755)
        directory.chmod(0o755)
        for name, data in files.items():
            path = directory / name
            path.write_bytes(data)
            path.chmod(0o644)
        with (
            output.open("xb") as stream,
            tarfile.open(fileobj=stream, mode="w:gz") as archive,
        ):
            archive.add(directory, arcname=directory.name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--deployment-root", required=True)
    parser.add_argument("--network", required=True)
    args = parser.parse_args()
    build_bundle(
        root=ROOT,
        wheel=args.wheel,
        output=args.output,
        deployment_root=args.deployment_root,
        network=args.network,
    )
    print(args.output)


if __name__ == "__main__":
    main()
