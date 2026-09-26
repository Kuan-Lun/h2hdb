"""Offline schema conversion bundles authenticate both schema generations."""

from __future__ import annotations

import gzip
import hashlib
import json
import runpy
import tarfile
import zipfile
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).parents[1]
_SCRIPT = _ROOT / "scripts" / "build-observation-upload-time-upgrade-bundle.py"


@pytest.fixture
def builder() -> dict[str, Any]:
    return runpy.run_path(str(_SCRIPT))


def _old_schema() -> bytes:
    fixture = _ROOT / "tests" / "fixtures" / "schema8-provider.bin.gz"
    with gzip.open(fixture, "rb") as stream:
        return stream.read(4_429_650)


def _source_wheel(path: Path, raw: bytes, *, version: str = "0.42.2") -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "h2hdb-0.42.2.dist-info/METADATA",
            f"Name: h2hdb\nVersion: {version}\n",
        )
        archive.writestr("h2hdb/_generated_vnext_schema.bin", raw)


def test_source_fixture_is_bound_to_its_generated_provenance(
    builder: dict[str, Any],
) -> None:
    raw = _old_schema()
    provenance = json.loads(
        (_ROOT / "tests/fixtures/schema8-provider.provenance.json").read_text()
    )
    assert provenance["core_version"] == "0.42.2"
    assert provenance["raw_size"] == len(raw) == builder["SOURCE_SCHEMA_SIZE"]
    assert (
        provenance["raw_sha256"]
        == hashlib.sha256(raw).hexdigest()
        == builder["SOURCE_SCHEMA_SHA256"]
    )
    compressed = (_ROOT / "tests/fixtures/schema8-provider.bin.gz").read_bytes()
    assert provenance["gzip_sha256"] == hashlib.sha256(compressed).hexdigest()
    assert compressed[4:8] == b"\0" * 4


@pytest.mark.parametrize("damage", ("version", "size", "checksum", "duplicate"))
def test_bundle_rejects_unpinned_source_inputs(
    tmp_path: Path, builder: dict[str, Any], damage: str
) -> None:
    wheel = tmp_path / "source.whl"
    raw = _old_schema()
    if damage == "size":
        raw = raw[:-1]
    elif damage == "checksum":
        raw = raw[:-1] + bytes([raw[-1] ^ 1])
    _source_wheel(wheel, raw, version="0.42.1" if damage == "version" else "0.42.2")
    if damage == "duplicate":
        with (
            zipfile.ZipFile(wheel, "a") as archive,
            pytest.warns(UserWarning, match="Duplicate name"),
        ):
            archive.writestr("h2hdb-0.42.2.dist-info/METADATA", "Name: h2hdb\n")
    with pytest.raises(ValueError):
        builder["_source_schema_bytes"](wheel.read_bytes())


def test_bundle_contains_readable_pinned_input_and_explicit_media_identity(
    tmp_path: Path, builder: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "checkout"
    package = root / "src" / "h2hdb"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text('"""Bundle test fixture."""\n')
    (root / "pyproject.toml").write_text('[project]\nversion = "0.43.0"\n')
    (root / "scripts").mkdir()
    (root / "scripts" / builder["CONVERTER"]).write_text("print('fixture')\n")
    (root / "scripts" / builder["SOURCE_CLEANUP"]).write_text(
        "print('cleanup fixture')\n"
    )
    wheel, source_wheel = tmp_path / "target.whl", tmp_path / "source.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.write(package / "__init__.py", "h2hdb/__init__.py")
        archive.writestr(
            "h2hdb-0.43.0.dist-info/METADATA", "Name: h2hdb\nVersion: 0.43.0\n"
        )
    _source_wheel(source_wheel, _old_schema())
    monkeypatch.setattr(
        builder["subprocess"], "check_output", lambda *_args, **_kwargs: "a" * 40
    )
    output = tmp_path / "upgrade.tar.gz"
    builder["build_bundle"](
        root=root,
        wheel=wheel,
        source_wheel=source_wheel,
        output=output,
        deployment_root="/a deployment/$literal",
        network="existing-$network",
    )
    with tarfile.open(output) as archive:
        members = archive.getmembers()
        assert all(item.isdir() or item.isfile() for item in members)
        assert all(item.mode == (0o755 if item.isdir() else 0o644) for item in members)
        files = {}
        for member in members:
            if member.isfile():
                stream = archive.extractfile(member)
                assert stream is not None
                files[Path(member.name).name] = stream.read()
    assert files[builder["SOURCE_SCHEMA"]] == _old_schema()
    dockerfile = files["Dockerfile"].decode()
    assert "chmod 0444 /opt/h2hdb-upgrade/*" in dockerfile
    assert "schema8-provider.bin" in dockerfile
    assert files[builder["SOURCE_WHEEL"]] == source_wheel.read_bytes()
    assert (
        files[builder["SOURCE_WHEEL_DIGEST"]].decode().strip()
        == hashlib.sha256(source_wheel.read_bytes()).hexdigest()
    )
    assert dockerfile.index("USER 65534:65534") < dockerfile.index(" --help")
    compose = files["compose.yaml"].decode()
    assert "${MEDIA_UID:?" in compose and "${MEDIA_GID:?" in compose
    assert "/a deployment/$$literal/config" in compose
    assert 'name: "existing-$$network"' in compose
    provenance = json.loads(files["provenance.json"])
    assert provenance["source_version"] == "0.42.2"
    for name, digest in provenance["files_sha256"].items():
        assert hashlib.sha256(files[name]).hexdigest() == digest
    with pytest.raises(FileExistsError):
        builder["build_bundle"](
            root=root,
            wheel=wheel,
            source_wheel=source_wheel,
            output=output,
            deployment_root="/deployment",
            network="network",
        )
