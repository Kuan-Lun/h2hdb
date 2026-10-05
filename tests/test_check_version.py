from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
CHECK_VERSION = ROOT / "scripts" / "check-version.py"


def _load_module(name: str, path: Path) -> ModuleType:
    specification = importlib.util.spec_from_file_location(name, path)
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


policy = _load_module("h2hdb_check_version", CHECK_VERSION)


def test_candidate_version_must_use_exactly_three_parts() -> None:
    assert policy._parse_version("0.23.1") == (0, 23, 1)
    with pytest.raises(ValueError, match="must use X.Y.Z"):
        policy._parse_version("0.23.0.12")


def test_post_one_feature_uses_semantic_minor_version() -> None:
    assert policy._expected_version((1, 4, 2), breaking=False, feature=True) == (
        1,
        5,
        0,
    )


def test_index_candidate_uses_merge_task_ref_before_merge_head_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKFLOW_MERGE_TASK_REF", "task/example")
    calls: list[tuple[str, ...]] = []

    def git(*arguments: str, input_text: str | None = None) -> str:
        assert input_text is None
        calls.append(arguments)
        if arguments == (
            "rev-parse",
            "--verify",
            "task/example^{commit}",
        ):
            return "task-commit"
        assert arguments == ("write-tree",)
        return "candidate-tree"

    monkeypatch.setattr(policy, "_git", git)
    arguments = argparse.Namespace(index=True, base=None, candidate="HEAD")

    assert policy._candidate(arguments) == (
        "HEAD",
        "candidate-tree",
        "HEAD..task-commit",
    )
    assert calls == [
        ("rev-parse", "--verify", "task/example^{commit}"),
        ("write-tree",),
    ]


@pytest.mark.parametrize(
    "removed_path",
    (
        "scripts/upgrade-observation-upload-time-schema.py",
        "scripts/build-observation-upload-time-upgrade-bundle.py",
        "scripts/finish-schema8-cleanup.py",
    ),
)
@pytest.mark.parametrize("target_version", ("0.43.0", "0.43.1", "0.44.0"))
def test_retired_offline_commands_require_a_breaking_release(
    monkeypatch: pytest.MonkeyPatch, removed_path: str, target_version: str
) -> None:
    base = tomllib.loads((ROOT / "pyproject.toml").read_text())
    base["project"]["version"] = "0.43.0"
    candidate = copy.deepcopy(base)
    candidate["project"]["version"] = target_version
    monkeypatch.setattr(sys, "argv", [str(CHECK_VERSION)])
    monkeypatch.setattr(
        policy, "_candidate", lambda _args: ("base", "candidate", "task")
    )
    monkeypatch.setattr(
        policy,
        "_load_toml",
        lambda tree: base if tree in {"base", "release-commit^1"} else candidate,
    )

    def git(*arguments: str) -> str:
        if arguments in {
            (
                "diff",
                "--no-renames",
                "--name-only",
                "--diff-filter=ACDMRT",
                "base",
                "candidate",
            ),
            (
                "diff",
                "--no-renames",
                "--name-only",
                "--diff-filter=ACDMRT",
                "release-commit^1",
                "release-commit",
            ),
        }:
            return removed_path
        if arguments == ("rev-list", "task"):
            return "release-commit"
        if arguments == ("show", "-s", "--format=%B", "release-commit"):
            return "refactor(upgrade)!: retire completed offline conversion tooling"
        assert arguments == ("log", "--format=%B%x00", "task")
        return "refactor(upgrade)!: retire completed offline conversion tooling"

    audits: list[str] = []
    monkeypatch.setattr(policy, "_git", git)
    monkeypatch.setattr(
        policy, "_validate_audit", lambda _tree, _doc, version: audits.append(version)
    )
    if target_version == "0.44.0":
        assert policy.main() == 0
        assert audits == [target_version]
    else:
        message = (
            "release surface changed without a project version bump"
            if target_version == "0.43.0"
            else "expected project version 0.44.0"
        )
        with pytest.raises(ValueError, match=message):
            policy.main()
        assert not audits


def _history_git(root: Path, *arguments: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), *arguments],
        stdin=subprocess.DEVNULL,
        encoding="utf-8",
        stderr=subprocess.STDOUT,
        timeout=5,
    ).strip()


def _commit_files(root: Path, changes: dict[str, str | None], message: str) -> None:
    for name, content in changes.items():
        path = root / name
        if content is None:
            path.unlink()
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
    _history_git(root, "add", "--all")
    _history_git(root, "commit", "-qm", message)


def _history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, version: str = "0.45.2"
) -> list[str]:
    # Isolate every Git subprocess, including the policy's own history reads.
    # Clear command-level config/identity/repository overrides before git init.
    for name in tuple(os.environ):
        if name.startswith("GIT_"):
            monkeypatch.delenv(name)
    for name, value in {
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_ALLOW_PROTOCOL": "file",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_EDITOR": "true",
    }.items():
        monkeypatch.setenv(name, value)
    _history_git(tmp_path, "init", "-q")
    _history_git(tmp_path, "config", "user.name", "Version policy test")
    _history_git(tmp_path, "config", "user.email", "version@example.invalid")
    # Do not run user-installed global hooks inside this isolated test history.
    _history_git(tmp_path, "config", "core.hooksPath", str(tmp_path / "no-hooks"))
    _commit_files(
        tmp_path,
        {
            "pyproject.toml": (
                '[project]\nname = "sample"\n'
                f'version = "{version}"\nrequires-python = ">=3.14"\n'
                '[project.optional-dependencies]\ndev = ["pytest>=9"]\n'
                "[tool.h2h.version]\nrelease-paths = "
                '["src/**", "scripts/upgrade-*.py"]\n'
            ),
            "src/sample.py": "VALUE = 1\n",
            "scripts/upgrade-sample.py": "print('public offline command')\n",
        },
        "chore: initial fixture",
    )
    base = _history_git(tmp_path, "rev-parse", "HEAD")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", [str(CHECK_VERSION), "--base", base])
    audits: list[str] = []
    monkeypatch.setattr(
        policy, "_validate_audit", lambda _tree, _doc, value: audits.append(value)
    )
    return audits


def _bump(root: Path, original: str, target: str) -> None:
    path = root / "pyproject.toml"
    _commit_files(
        root,
        {"pyproject.toml": path.read_text().replace(original, target)},
        f"chore(release): bump version to {target}",
    )


@pytest.mark.parametrize("source", ["global", "system", "environment"])
def test_history_isolates_hostile_signing_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    unavailable_gpg = tmp_path / "簽章 tools" / "unavailable-fixture-gpg"
    config = tmp_path / "hostile.gitconfig"
    content = (
        "[commit]\n    gpgsign = true\n"
        f"[gpg]\n    program = {json.dumps(unavailable_gpg.as_posix(), ensure_ascii=False)}\n"
    )
    config.write_text(content, encoding="utf-8")
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith("GIT_")
    }
    environment.update(
        GIT_CONFIG_NOSYSTEM="0" if source == "system" else "1",
        GIT_CONFIG_SYSTEM=str(config) if source == "system" else os.devnull,
        GIT_CONFIG_GLOBAL=str(config) if source == "global" else os.devnull,
        GIT_ALLOW_PROTOCOL="file",
        GIT_TERMINAL_PROMPT="0",
        LC_ALL="C",
    )
    if source == "environment":
        environment.update(
            GIT_CONFIG_COUNT="2",
            GIT_CONFIG_KEY_0="commit.gpgsign",
            GIT_CONFIG_VALUE_0="true",
            GIT_CONFIG_KEY_1="gpg.program",
            GIT_CONFIG_VALUE_1=str(unavailable_gpg),
        )
    control = tmp_path / "unisolated"
    control.mkdir()
    subprocess.run(
        ["git", "-C", str(control), "init", "-q"],
        env=environment,
        stdin=subprocess.DEVNULL,
        check=True,
        capture_output=True,
        encoding="utf-8",
        timeout=5,
    )
    configured_program = subprocess.check_output(
        ["git", "-C", str(control), "config", "--get", "gpg.program"],
        env=environment,
        stdin=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        timeout=5,
    ).strip()
    expected_program = (
        str(unavailable_gpg) if source == "environment" else unavailable_gpg.as_posix()
    )
    assert configured_program == expected_program
    rejected = subprocess.run(
        [
            "git",
            "-C",
            str(control),
            "-c",
            "user.name=Version policy test",
            "-c",
            "user.email=version@example.invalid",
            "-c",
            f"core.hooksPath={tmp_path / 'no-hooks'}",
            "commit",
            "--allow-empty",
            "-qm",
            "test: unisolated signing control",
        ],
        env=environment,
        stdin=subprocess.DEVNULL,
        check=False,
        capture_output=True,
        encoding="utf-8",
        timeout=5,
    )
    assert rejected.returncode != 0
    assert unavailable_gpg.name in rejected.stderr
    assert "failed to sign" in rejected.stderr

    for name in tuple(os.environ):
        if name.startswith("GIT_"):
            monkeypatch.delenv(name)
    for name, value in environment.items():
        if name.startswith("GIT_"):
            monkeypatch.setenv(name, value)
    isolated = tmp_path / "isolated"
    isolated.mkdir()
    audits = _history(isolated, monkeypatch)
    _commit_files(isolated, {"src/sample.py": "VALUE = 2\n"}, "fix: correct runtime")
    _bump(isolated, "0.45.2", "0.45.3")
    assert policy.main() == 0
    assert audits == ["0.45.3"]
    assert _history_git(isolated, "rev-list", "--count", "HEAD") == "3"
    assert _history_git(isolated, "log", "-1", "--format=%an <%ae>") == (
        "Version policy test <version@example.invalid>"
    )
    assert config.read_text(encoding="utf-8") == content
    assert not unavailable_gpg.exists()


@pytest.mark.parametrize(
    ("base", "development_message", "target"),
    [
        ("0.45.2", "fix!: change developer report protocol", "0.45.3"),
        (
            "0.45.2",
            "fix: revise developer reports\n\nBREAKING CHANGE: report fields changed",
            "0.45.3",
        ),
        ("1.4.2", "feat: add a developer analysis mode", "1.4.3"),
    ],
)
def test_development_signals_do_not_raise_runtime_fix_severity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    base: str,
    development_message: str,
    target: str,
) -> None:
    audits = _history(tmp_path, monkeypatch, base)
    _commit_files(
        tmp_path, {"scripts/developer-probe.py": "print('v2')\n"}, development_message
    )
    _commit_files(tmp_path, {"src/sample.py": "VALUE = 2\n"}, "fix: correct runtime")
    _bump(tmp_path, base, target)
    assert policy.main() == 0
    assert audits == [target]


@pytest.mark.parametrize(
    ("base", "message", "target"),
    [
        ("0.45.2", "fix!: change runtime protocol", "0.46.0"),
        ("1.4.2", "feat!: change runtime protocol", "2.0.0"),
        ("1.4.2", "feat: add a runtime feature", "1.5.0"),
        (
            "0.45.2",
            "fix: change runtime protocol\n\nBREAKING CHANGE: remove public field",
            "0.46.0",
        ),
    ],
)
def test_runtime_messages_preserve_breaking_and_feature_severity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    base: str,
    message: str,
    target: str,
) -> None:
    audits = _history(tmp_path, monkeypatch, base)
    _commit_files(tmp_path, {"src/sample.py": "VALUE = 2\n"}, message)
    _bump(tmp_path, base, target)
    assert policy.main() == 0
    assert audits == [target]


@pytest.mark.parametrize("move", [False, True])
@pytest.mark.parametrize("target", ["0.45.3", "0.46.0"])
def test_retired_or_moved_public_script_keeps_breaking_signal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, move: bool, target: str
) -> None:
    audits = _history(tmp_path, monkeypatch)
    content = (tmp_path / "scripts/upgrade-sample.py").read_text()
    changes: dict[str, str | None] = {"scripts/upgrade-sample.py": None}
    if move:
        changes["scripts/developer-archive.py"] = content
    # Removing the declaration together with the public command cannot hide it.
    changes["pyproject.toml"] = (
        (tmp_path / "pyproject.toml")
        .read_text()
        .replace(', "scripts/upgrade-*.py"', "")
    )
    _commit_files(tmp_path, changes, "refactor!: retire public offline command")
    _bump(tmp_path, "0.45.2", target)
    if target == "0.46.0":
        assert policy.main() == 0
        assert audits == [target]
    else:
        with pytest.raises(ValueError, match="expected project version 0.46.0"):
            policy.main()
        assert audits == []


def test_development_only_breaking_change_does_not_require_project_bump(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    audits = _history(tmp_path, monkeypatch)
    _commit_files(
        tmp_path, {"scripts/developer-probe.py": "print('v2')\n"}, "fix!: report v2"
    )
    assert policy.main() == 0
    assert audits == []


def test_breaking_runtime_metadata_requires_lane_bump(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    audits = _history(tmp_path, monkeypatch)
    _commit_files(
        tmp_path,
        {
            "pyproject.toml": (tmp_path / "pyproject.toml")
            .read_text()
            .replace('requires-python = ">=3.14"', 'requires-python = ">=3.15"')
        },
        "build!: raise supported Python baseline",
    )
    _bump(tmp_path, "0.45.2", "0.45.3")
    with pytest.raises(ValueError, match="expected project version 0.46.0"):
        policy.main()
    assert audits == []


def test_unknown_path_is_not_silently_classified_as_development(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _history(tmp_path, monkeypatch)
    _commit_files(tmp_path, {"unclassified.conf": "new\n"}, "fix!: unknown surface")
    with pytest.raises(ValueError, match="unclassified version impact paths"):
        policy.main()


@pytest.mark.parametrize(
    ("base", "message", "target"),
    [
        ("0.45.2", "fix!: change public protocol", "0.46.0"),
        ("1.4.2", "feat: add public feature", "1.5.0"),
    ],
)
def test_earlier_release_message_keeps_severity_after_later_runtime_fix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    base: str,
    message: str,
    target: str,
) -> None:
    audits = _history(tmp_path, monkeypatch, base)
    _commit_files(tmp_path, {"src/sample.py": "VALUE = 2\n"}, message)
    _commit_files(tmp_path, {"src/sample.py": "VALUE = 3\n"}, "fix: follow-up")
    _bump(tmp_path, base, target)
    assert policy.main() == 0
    assert audits == [target]


def test_development_dependency_metadata_signal_does_not_raise_runtime_severity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    audits = _history(tmp_path, monkeypatch)
    _commit_files(
        tmp_path,
        {
            "pyproject.toml": (tmp_path / "pyproject.toml")
            .read_text()
            .replace('dev = ["pytest>=9"]', 'dev = ["pytest>=10"]')
        },
        "build!: change developer dependency contract",
    )
    _commit_files(tmp_path, {"src/sample.py": "VALUE = 2\n"}, "fix: correct runtime")
    _bump(tmp_path, "0.45.2", "0.45.3")
    assert policy.main() == 0
    assert audits == ["0.45.3"]
