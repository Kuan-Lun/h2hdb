from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class GitRepository:
    path: Path
    primary: str
    environment: dict[str, str]

    def run(
        self, *arguments: str, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            arguments,
            cwd=self.path,
            env=self.environment,
            check=check,
            capture_output=True,
            text=True,
            timeout=15,
        )

    def git(self, *arguments: str) -> str:
        return self.run("git", *arguments).stdout.strip()

    def commit(self, name: str) -> str:
        (self.path / name).write_text(name, encoding="utf-8")
        self.git("add", name)
        self.git("commit", "-m", f"test: add {name}")
        return self.git("rev-parse", "HEAD")

    def install(self) -> None:
        self.run("bash", "scripts/install-git-hooks.sh")

    def merge_task(self) -> str:
        self.git("switch", "-c", "task/example")
        self.commit("task-change")
        self.git("switch", self.primary)
        self.git("merge", "--no-ff", "--no-edit", "task/example")
        self.git("branch", "-d", "task/example")
        return self.git("rev-parse", "HEAD")


@pytest.fixture(params=["main", "master", "trunk"])
def repository(tmp_path: Path, request: pytest.FixtureRequest) -> GitRepository:
    primary = str(request.param)
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith("GIT_")
    }
    environment.update(
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_ALLOW_PROTOCOL="file",
        GIT_TERMINAL_PROMPT="0",
        GIT_EDITOR="true",
    )
    local = tmp_path / "local"
    local.mkdir()
    repo = GitRepository(local, primary, environment)
    repo.git("init", "--initial-branch", primary)
    repo.git("config", "user.name", "Workflow Test")
    repo.git("config", "user.email", "workflow@example.invalid")
    repo.git("config", "workflow.primaryBranch", primary)
    # Exercise the real installer and rebase hook without unrelated release gates.
    for relative in (
        "scripts/detect-primary-branch.sh",
        "scripts/install-git-hooks.sh",
        ".githooks/pre-rebase",
    ):
        destination = local / relative
        destination.parent.mkdir(exist_ok=True)
        shutil.copy2(ROOT / relative, destination)
    repo.git("add", ".")
    repo.git("commit", "-m", "test: initialize workflow fixture")
    remote = tmp_path / "origin.git"
    repo.git("clone", "--bare", ".", str(remote))
    repo.git("remote", "add", "origin", str(remote))
    repo.git("fetch", "origin")
    repo.git("branch", "--set-upstream-to", f"origin/{primary}")
    repo.install()
    return repo


def test_installer_repairs_unsafe_settings_idempotently(
    repository: GitRepository,
) -> None:
    repository.git("config", "pull.rebase", "true")
    repository.git("config", f"branch.{repository.primary}.rebase", "merges")
    repository.git("config", "pull.ff", "false")
    repository.git("config", f"branch.{repository.primary}.mergeOptions", "--ff")

    repository.install()
    repository.install()

    expected = {
        "core.hooksPath": ".githooks",
        "pull.rebase": "false",
        "pull.ff": "only",
        f"branch.{repository.primary}.rebase": "false",
        f"branch.{repository.primary}.mergeOptions": "--no-ff",
    }
    for setting, value in expected.items():
        assert repository.git("config", "--local", "--get", setting) == value


def test_pull_preserves_exact_merge_after_task_branch_deletion(
    repository: GitRepository,
) -> None:
    merged = repository.merge_task()
    parents = repository.git("show", "-s", "--format=%P", merged)
    assert len(parents.split()) == 2

    repository.git("pull", "--tags", "origin", repository.primary)

    assert repository.git("rev-parse", "HEAD") == merged
    assert repository.git("show", "-s", "--format=%P", "HEAD") == parents


@pytest.mark.parametrize(
    "arguments",
    [
        ("rebase", "HEAD~1"),
        ("pull", "--rebase", "--tags", "origin"),
        (
            "-c",
            "pull.rebase=true",
            "-c",
            "branch.{primary}.rebase=true",
            "-c",
            "pull.ff=false",
            "pull",
        ),
    ],
)
def test_primary_rebase_is_rejected_without_changing_merge(
    repository: GitRepository, arguments: tuple[str, ...]
) -> None:
    merged = repository.merge_task()
    command = tuple(
        argument.format(primary=repository.primary) for argument in arguments
    )

    result = repository.run("git", *command, check=False)

    assert result.returncode != 0
    assert "Rebasing primary is not allowed" in result.stderr
    assert repository.git("rev-parse", repository.primary) == merged
    assert repository.git("rev-parse", "HEAD") == merged


@pytest.mark.parametrize("qualified", [False, True])
def test_explicit_primary_rebase_from_task_is_rejected(
    repository: GitRepository, qualified: bool
) -> None:
    merged = repository.merge_task()
    repository.git("switch", "-c", "task/next")
    branch = f"refs/heads/{repository.primary}" if qualified else repository.primary

    result = repository.run(
        "git", "rebase", f"{repository.primary}~1", branch, check=False
    )

    assert result.returncode != 0
    assert "Rebasing primary is not allowed" in result.stderr
    assert repository.git("rev-parse", repository.primary) == merged


def test_task_branch_can_rebase_onto_new_primary(repository: GitRepository) -> None:
    repository.git("switch", "-c", "task/rebase")
    previous_task = repository.commit("task-rebase-change")
    repository.git("switch", repository.primary)
    new_primary = repository.merge_task()
    repository.git("switch", "task/rebase")

    repository.git("rebase", repository.primary)

    assert repository.git("rev-parse", repository.primary) == new_primary
    assert repository.git("rev-parse", "HEAD") != previous_task
    assert repository.git("rev-parse", "HEAD~1") == new_primary
    assert (repository.path / "task-rebase-change").read_text() == "task-rebase-change"


def test_divergent_pull_stops_without_rewriting_local_merge(
    repository: GitRepository, tmp_path: Path
) -> None:
    merged = repository.merge_task()
    peer_path = tmp_path / "peer"
    repository.git("clone", str(tmp_path / "origin.git"), str(peer_path))
    peer = GitRepository(peer_path, repository.primary, repository.environment)
    peer.git("config", "user.name", "Workflow Test")
    peer.git("config", "user.email", "workflow@example.invalid")
    peer.commit("remote-change")
    peer.git("push", "origin", repository.primary)

    result = repository.run("git", "pull", check=False)

    assert result.returncode != 0
    assert repository.git("rev-parse", "HEAD") == merged
    assert len(repository.git("show", "-s", "--format=%P", "HEAD").split()) == 2
