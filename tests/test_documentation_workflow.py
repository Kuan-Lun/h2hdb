from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]


def _run(repository: Path, *command: str) -> subprocess.CompletedProcess[str]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("GIT_") and key != "WORKFLOW_MERGE_TASK_REF"
    }
    environment.update(
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_CONFIG_NOSYSTEM="1",
        DOCUMENTATION_WORKFLOW_TRACE=str(repository.parent / "checks.log"),
    )
    return subprocess.run(
        command,
        cwd=repository,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )


def _git(repository: Path, *arguments: str) -> str:
    result = _run(repository, "git", *arguments)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout.strip()


def _write(repository: Path, name: str, content: str) -> None:
    path = repository / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _commit(repository: Path, message: str) -> None:
    _git(repository, "add", ".")
    _git(repository, "commit", "-m", message)


def _repository(tmp_path: Path, *, install_hooks: bool = True) -> Path:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    for name, value in (
        ("user.name", "Documentation Workflow"),
        ("user.email", "documentation@example.invalid"),
        ("commit.gpgsign", "false"),
        ("workflow.primaryBranch", "main"),
    ):
        _git(repository, "config", name, value)
    for name in (
        "scripts/detect-primary-branch.sh",
        "scripts/git-flow-merge.sh",
        "scripts/check_change_scope.py",
        "scripts/check-docs.py",
        "scripts/check-version.py",
        "scripts/release-gate.py",
        ".githooks/run-release-gate",
        ".githooks/pre-commit",
        ".githooks/pre-merge-commit",
        ".githooks/commit-msg",
        ".markdownlint-cli2.jsonc",
    ):
        destination = repository / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(_ROOT / name, destination)
    _write(repository, ".gitignore", ".venv/\nnode_modules/\n__pycache__/\n")
    _write(repository, "README.md", "# Fixture\n")
    _write(
        repository,
        "pyproject.toml",
        '[project]\nname = "workflow-fixture"\nversion = "0.1.0"\n'
        '[tool.h2h.version]\nrelease-paths = ["src/**"]\n',
    )
    _write(
        repository,
        ".venv/bin/python",
        f'#!/usr/bin/env bash\nexec {shlex.quote(sys.executable)} "$@"\n',
    )
    (repository / ".venv/bin/python").chmod(0o755)
    (repository / "node_modules").symlink_to(
        _ROOT / "node_modules", target_is_directory=True
    )
    for profile in ("fast", "full"):
        path = repository / "scripts" / f"check-{profile}.sh"
        path.write_text(
            "#!/usr/bin/env bash\nset -euo pipefail\n"
            f'printf "{profile}\\n" >> "$DOCUMENTATION_WORKFLOW_TRACE"\n'
            f'if [[ "${{DOCUMENTATION_FAIL_PROFILE:-}}" == "{profile}" ]]; '
            "then exit 42; fi\n",
            encoding="utf-8",
        )
        path.chmod(0o755)
    _write(
        repository,
        "scripts/review-code.py",
        "import os, sys\nfrom pathlib import Path\n"
        "with Path(os.environ['DOCUMENTATION_WORKFLOW_TRACE']).open('a') as log:\n"
        "    log.write('review-' + sys.argv[1] + '\\n')\n",
    )
    _commit(repository, "chore: seed isolated workflow")
    if install_hooks:
        _git(repository, "config", "core.hooksPath", ".githooks")
    _git(repository, "switch", "-c", "docs/task")
    return repository


def _checks(repository: Path) -> list[str]:
    path = repository.parent / "checks.log"
    return path.read_text(encoding="utf-8").splitlines() if path.exists() else []


def test_documentation_commit_checks_staged_content_without_program_tools(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path)
    _write(repository, "README.md", "# Staged documentation\n")
    _git(repository, "add", "README.md")
    _write(repository, "README.md", "# Unstaged\n\n### Invalid heading jump\n")
    _write(repository, "tests/unstaged.py", "not valid Python !\n")

    _git(repository, "commit", "-m", "docs: update guide")

    assert _checks(repository) == []
    assert _git(repository, "show", "HEAD:README.md") == "# Staged documentation"


@pytest.mark.parametrize(
    "path", ("tests/example.py", "scripts/example.py", "AGENTS.md")
)
def test_mixed_commit_dispatches_to_fast_profile(tmp_path: Path, path: str) -> None:
    repository = _repository(tmp_path)
    _write(repository, "README.md", "# Updated guide\n")
    _write(repository, path, "changed\n")

    _commit(repository, "chore: update implementation and guide")

    assert _checks(repository) == ["fast"]


def test_invalid_documentation_blocks_commit_without_program_checks(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path)
    before = _git(repository, "rev-parse", "HEAD")
    _write(repository, "README.md", "# Guide\n\n### Invalid heading jump\n")
    _git(repository, "add", "README.md")

    result = _run(repository, "git", "commit", "-m", "docs: invalid guide")

    assert result.returncode != 0
    assert "MD001" in result.stdout + result.stderr
    assert _git(repository, "rev-parse", "HEAD") == before
    assert _checks(repository) == []


@pytest.mark.parametrize("classifier", ("raise SystemExit(23)\n", "print('unknown')\n"))
def test_classifier_failure_or_unknown_profile_blocks_commit(
    tmp_path: Path, classifier: str
) -> None:
    repository = _repository(tmp_path)
    before = _git(repository, "rev-parse", "HEAD")
    _write(repository, "README.md", "# Updated guide\n")
    _git(repository, "add", "README.md")
    _write(repository, "scripts/check_change_scope.py", classifier)

    result = _run(repository, "git", "commit", "-m", "docs: update guide")

    assert result.returncode != 0
    assert _git(repository, "rev-parse", "HEAD") == before
    assert _checks(repository) == []


@pytest.mark.parametrize("code_first", (False, True))
@pytest.mark.parametrize("advance_primary", (False, True))
def test_merge_dispatch_uses_complete_candidate_not_last_commit(
    tmp_path: Path, code_first: bool, advance_primary: bool
) -> None:
    repository = _repository(tmp_path)
    if code_first:
        _write(repository, "tests/example.py", "value = 1\n")
        _commit(repository, "test: add example")
    _write(repository, "README.md", "# Updated guide\n")
    _commit(repository, "docs: update guide")
    if advance_primary:
        _git(repository, "switch", "main")
        _git(repository, "switch", "-c", "docs/parallel")
        _write(repository, "docs/parallel.md", "# Parallel guide\n")
        _commit(repository, "docs: add parallel guide")
        result = _run(repository, "scripts/git-flow-merge.sh")
        assert result.returncode == 0, result.stdout + result.stderr
        _git(repository, "switch", "docs/task")
    (repository.parent / "checks.log").write_text("", encoding="utf-8")

    result = _run(repository, "scripts/git-flow-merge.sh")

    assert result.returncode == 0, result.stdout + result.stderr
    assert _checks(repository) == (
        ["review-run", "review-verify", "full", "review-verify"] if code_first else []
    )
    assert _git(repository, "branch", "--show-current") == "main"
    assert (
        len(_git(repository, "rev-list", "--parents", "-n", "1", "HEAD").split()) == 3
    )
    assert not _git(repository, "branch", "--list", "docs/task")
    assert _git(repository, "status", "--porcelain") == ""


@pytest.mark.parametrize("failure", ("documentation", "full"))
def test_failed_selected_merge_profile_aborts_and_retains_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    repository = _repository(tmp_path, install_hooks=False)
    primary = _git(repository, "rev-parse", "main")
    if failure == "documentation":
        _write(repository, "README.md", "# Guide\n\n### Invalid heading jump\n")
    else:
        _write(repository, "tests/example.py", "value = 1\n")
        monkeypatch.setenv("DOCUMENTATION_FAIL_PROFILE", "full")
    _commit(repository, "test: construct failing candidate")
    task = _git(repository, "rev-parse", "HEAD")
    _git(repository, "config", "core.hooksPath", ".githooks")

    result = _run(repository, "scripts/git-flow-merge.sh")

    assert result.returncode != 0
    assert "task branch was retained" in result.stdout + result.stderr
    assert _git(repository, "rev-parse", "main") == primary
    assert _git(repository, "rev-parse", "docs/task") == task
    assert _git(repository, "branch", "--show-current") == "docs/task"
    assert _git(repository, "status", "--porcelain") == ""
    assert not (repository / ".git/MERGE_HEAD").exists()
    assert _checks(repository) == (
        ["review-run", "review-verify", "full"] if failure == "full" else []
    )
    if failure == "documentation":
        assert "MD001" in result.stdout + result.stderr
