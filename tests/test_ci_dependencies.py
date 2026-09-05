from __future__ import annotations

import runpy
import subprocess
import sys
from pathlib import Path

import pytest

_INSTALLER = Path(__file__).parents[1] / "scripts" / "install-ci-dependencies.py"


def _fixture_installer(tmp_path: Path, manifest: str) -> Path:
    repository = tmp_path / "checkout"
    scripts = repository / "scripts"
    scripts.mkdir(parents=True)
    installer = scripts / _INSTALLER.name
    installer.write_bytes(_INSTALLER.read_bytes())
    (repository / "pyproject.toml").write_text(manifest, encoding="utf-8")
    return installer


def _capture_pip(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    calls: list[list[str]] = []

    def run(command: list[str], *, check: bool) -> subprocess.CompletedProcess[str]:
        assert check is True
        calls.append(command)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(subprocess, "run", run)
    return calls


@pytest.mark.parametrize("pytest_plugins", [False, True])
def test_ci_install_uses_manifest_constraints_and_required_plugins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, pytest_plugins: bool
) -> None:
    installer = _fixture_installer(
        tmp_path,
        """
[project]
dependencies = ['example-package[extra]>=2; python_version >= "3.14"']
[project.optional-dependencies]
dev = ["pytest>=99,!=99.1", "hypothesis>=88,<89", "unrelated-dev-tool>=42"]
[tool.pytest.ini_options]
required_plugins = ["pytest-xdist>=77,<78", "pytest-custom>=66"]
""",
    )
    calls = _capture_pip(monkeypatch)
    arguments = [str(installer), "pytest", "hypothesis", "Example_Package"]
    if pytest_plugins:
        arguments.append("--pytest-plugins")
    monkeypatch.setattr(sys, "argv", arguments)
    monkeypatch.chdir(tmp_path)

    runpy.run_path(str(installer), run_name="__main__")

    expected = [
        "pytest>=99,!=99.1",
        "hypothesis>=88,<89",
        'example-package[extra]>=2; python_version >= "3.14"',
    ]
    if pytest_plugins:
        expected.extend(["pytest-xdist>=77,<78", "pytest-custom>=66"])
    assert calls == [[sys.executable, "-m", "pip", "install", "--upgrade", *expected]]


def test_ci_install_preserves_intersecting_requirements(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    installer = _fixture_installer(
        tmp_path,
        """
[project]
dependencies = ["pytest>=9"]
[project.optional-dependencies]
dev = ["pytest<10"]
[tool.pytest.ini_options]
required_plugins = ["pytest>=9"]
""",
    )
    calls = _capture_pip(monkeypatch)
    monkeypatch.setattr(sys, "argv", [str(installer), "--pytest-plugins", "pytest"])

    runpy.run_path(str(installer), run_name="__main__")

    assert calls == [
        [sys.executable, "-m", "pip", "install", "--upgrade", "pytest>=9", "pytest<10"]
    ]


@pytest.mark.parametrize(
    "arguments", [["pytest"], ["--pytest-plugins", "pytest-xdist"]]
)
def test_ci_install_rejects_missing_declarations_before_installing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, arguments: list[str]
) -> None:
    installer = _fixture_installer(
        tmp_path,
        '[project.optional-dependencies]\ndev = ["pytest-xdist>=77"]\n',
    )
    calls = _capture_pip(monkeypatch)
    monkeypatch.setattr(sys, "argv", [str(installer), *arguments])

    with pytest.raises(SystemExit) as error:
        runpy.run_path(str(installer), run_name="__main__")

    assert error.value.code == 2
    assert not calls


def test_ci_install_propagates_pip_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    installer = _fixture_installer(
        tmp_path, '[project.optional-dependencies]\ndev = ["pytest>=99"]\n'
    )

    def fail(command: list[str], *, check: bool) -> subprocess.CompletedProcess[str]:
        assert check is True
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(subprocess, "run", fail)
    monkeypatch.setattr(sys, "argv", [str(installer), "pytest"])

    with pytest.raises(subprocess.CalledProcessError):
        runpy.run_path(str(installer), run_name="__main__")
