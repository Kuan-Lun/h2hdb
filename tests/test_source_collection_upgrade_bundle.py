from __future__ import annotations

import importlib.util
import json
import os
import tarfile
import zipfile
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture
def builder() -> ModuleType:
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts/build-source-collection-upgrade-bundle.py"
    )
    spec = importlib.util.spec_from_file_location("upgrade_bundle_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fixture(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "checkout"
    package = root / "src/h2hdb"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("VALUE = 1\n")
    (root / "pyproject.toml").write_text('[project]\nversion = "0.41.1"\n')
    scripts = root / "scripts"
    scripts.mkdir()
    converter = scripts / "upgrade-source-collection-schema.py"
    converter.write_text('print("fixture converter")\n')
    converter.chmod(0o600)
    wheel = tmp_path / "fixture.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("h2hdb/__init__.py", (package / "__init__.py").read_bytes())
        archive.writestr(
            "h2hdb-0.41.1.dist-info/METADATA",
            "Metadata-Version: 2.4\nName: h2hdb\nVersion: 0.41.1\n",
        )
    return root, wheel


def test_bundle_normalizes_restrictive_source_permissions_and_excludes_secrets(
    tmp_path: Path, builder: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, wheel = _fixture(tmp_path)
    (root / ".env").write_text("DO_NOT_PACKAGE=secret\n")
    monkeypatch.setattr(
        builder.subprocess, "check_output", lambda *_a, **_k: "commit\n"
    )
    output = tmp_path / "bundle.tar.gz"
    original_umask = os.umask(0o077)
    try:
        builder.build_bundle(
            root=root,
            wheel=wheel,
            output=output,
            deployment_root="/deployment/config with spaces",
            network="existing-network",
        )
    finally:
        os.umask(original_umask)
    with tarfile.open(output) as archive:
        entries = archive.getmembers()
        assert len(entries) == 7
        assert all(
            entry.mode == (0o755 if entry.isdir() else 0o644) for entry in entries
        )
        assert not any(entry.name.endswith("/.env") for entry in entries)
        prefix = "h2hdb-schema7-to8-docker-0.41.1/"
        manifest = archive.extractfile(prefix + "provenance.json")
        assert manifest is not None
        provenance = json.load(manifest)
        assert provenance["core_version"] == "0.41.1"
        assert set(provenance["files_sha256"]) == {
            "h2hdb-0.41.1-py3-none-any.whl",
            "upgrade-source-collection-schema.py",
            "Dockerfile",
            "compose.yaml",
            ".dockerignore",
        }


def test_bundle_rejects_a_stale_wheel_before_creating_output(
    tmp_path: Path, builder: ModuleType
) -> None:
    root, wheel = _fixture(tmp_path)
    (root / "src/h2hdb/__init__.py").write_text("VALUE = 2\n")
    output = tmp_path / "bundle.tar.gz"
    with pytest.raises(ValueError, match="runtime files differ"):
        builder.build_bundle(
            root=root,
            wheel=wheel,
            output=output,
            deployment_root="/deployment",
            network="existing-network",
        )
    assert not output.exists()


def test_compose_preserves_literal_dollars_and_runtime_user_interpolation(
    builder: ModuleType,
) -> None:
    compose = builder._compose(
        "0.41.1", "/deployment/$archive-${name}", "database-$network-${name}"
    )
    assert 'source: "/deployment/$$archive-$${name}/config"' in compose
    assert '"/deployment/$$archive-$${name}/env/database.env"' in compose
    assert '"/deployment/$$archive-$${name}/env/writer.env"' in compose
    assert 'name: "database-$$network-$${name}"' in compose
    assert (
        'user: "${MEDIA_UID:?set MEDIA_UID in deployment .env}:'
        '${MEDIA_GID:?set MEDIA_GID in deployment .env}"'
    ) in compose


def test_bundle_refuses_to_overwrite_an_existing_delivery(
    tmp_path: Path, builder: ModuleType
) -> None:
    root, wheel = _fixture(tmp_path)
    output = tmp_path / "bundle.tar.gz"
    output.write_bytes(b"existing delivery")
    with pytest.raises(FileExistsError):
        builder.build_bundle(
            root=root,
            wheel=wheel,
            output=output,
            deployment_root="/deployment",
            network="existing-network",
        )
    assert output.read_bytes() == b"existing delivery"
