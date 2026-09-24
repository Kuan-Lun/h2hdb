from __future__ import annotations

import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TASK_BRANCH = "task/review"

REVIEWER = """import json
import os
import signal
import subprocess
import sys
from pathlib import Path

def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()

assert sys.argv[1:] == ["run", "--index"]
assert git("branch", "--show-current") == "master"
assert not git("diff", "--name-only")
assert not git("ls-files", "--others", "--exclude-standard")
record = {
    "phase": "review",
    "tree": git("write-tree"),
    "base": git("rev-parse", "HEAD"),
    "task": git("rev-parse", "MERGE_HEAD"),
    "task_ref": os.environ["WORKFLOW_MERGE_TASK_REF"],
}
log = Path(git("rev-parse", "--git-common-dir")) / "review-flow.jsonl"
def interrupted(signum, frame):
    with log.open("a") as stream:
        stream.write(json.dumps({"phase": "review-terminated"}) + "\\n")
    sys.exit(1)

if os.environ.get("TEST_REVIEW_WAIT"):
    signal.signal(signal.SIGTERM, interrupted)
with log.open("a") as stream:
    stream.write(json.dumps(record) + "\\n")
if os.environ.get("TEST_REVIEW_WAIT"):
    while True:
        signal.pause()
if os.environ.get("TEST_REVIEW_SIGNAL"):
    os.kill(os.getppid(), signal.SIGTERM)
    sys.exit(1)
if os.environ.get("TEST_REVIEW_FAIL"):
    sys.exit(1)
"""

HOOK = """import json
import os
import subprocess
import sys
from pathlib import Path

def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()

if not Path(git("rev-parse", "--git-path", "MERGE_HEAD")).exists():
    sys.exit(0)
phase = sys.argv[1]
log = Path(git("rev-parse", "--git-common-dir")) / "review-flow.jsonl"
records = [json.loads(line) for line in log.read_text().splitlines()]
tree = git("write-tree")
assert records[0]["phase"] == "review"
assert records[0]["tree"] == tree
with log.open("a") as stream:
    stream.write(json.dumps({
        "phase": phase,
        "tree": tree,
        "task_ref": os.environ["WORKFLOW_MERGE_TASK_REF"],
    }) + "\\n")
if phase == "gate" and os.environ.get("TEST_GATE_FAIL"):
    sys.exit(1)
"""


def _environment(**overrides: str) -> dict[str, str]:
    # Hooks run tests with repository-local Git variables; fixture repositories
    # must never inherit an index or worktree belonging to the real checkout.
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith("GIT_")
    }
    environment.update(
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_CONFIG_NOSYSTEM="1",
        GIT_TERMINAL_PROMPT="0",
    )
    environment.update(overrides)
    return environment


def _git(repository: Path, *arguments: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repository), *arguments],
        env=_environment(),
        text=True,
        stderr=subprocess.PIPE,
        timeout=30,
    ).strip()


def _write(path: Path, content: str, *, executable: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    if executable:
        path.chmod(0o755)


@dataclass(frozen=True)
class MergeFixture:
    primary: Path
    task: Path
    primary_head: str
    task_head: str

    def run(self, **environment: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", "scripts/git-flow-merge.sh"],
            cwd=self.task,
            env=_environment(**environment),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=30,
            check=False,
        )

    def records(self) -> list[dict[str, str]]:
        log = self.primary / ".git" / "review-flow.jsonl"
        if not log.exists():
            return []
        return [json.loads(line) for line in log.read_text().splitlines()]

    def assert_retained(self) -> None:
        assert _git(self.primary, "rev-parse", "master") == self.primary_head
        assert _git(self.task, "rev-parse", TASK_BRANCH) == self.task_head
        assert _git(self.task, "branch", "--show-current") == TASK_BRANCH
        assert not _git(self.task, "status", "--porcelain=v1")
        assert not _git(self.primary, "status", "--porcelain=v1")
        for worktree in {self.primary, self.task}:
            merge_head = Path(
                _git(
                    worktree,
                    "rev-parse",
                    "--path-format=absolute",
                    "--git-path",
                    "MERGE_HEAD",
                )
            )
            assert not merge_head.exists()


def _fixture(
    tmp_path: Path,
    *,
    separate_worktree: bool = False,
    conflict: bool = False,
    reviewer: bool = True,
    interpreter: bool = True,
) -> MergeFixture:
    primary = tmp_path / "primary"
    primary.mkdir()
    _git(primary, "init", "--initial-branch=master")
    _git(primary, "config", "user.name", "Review Flow Test")
    _git(primary, "config", "user.email", "review-flow@example.invalid")
    _write(primary / ".gitignore", ".venv/\n")
    _write(primary / "shared.txt", "base\n")
    for name in ("git-flow-merge.sh", "detect-primary-branch.sh"):
        path = primary / "scripts" / name
        path.parent.mkdir(exist_ok=True)
        shutil.copy2(ROOT / "scripts" / name, path)
    if reviewer:
        _write(primary / "scripts" / "review-code.py", REVIEWER)
    for name, phase in (
        ("pre-commit", "gate"),
        ("pre-merge-commit", "unexpected-automatic-merge-gate"),
        ("commit-msg", "commit-message"),
    ):
        _write(
            primary / ".githooks" / name,
            "#!/usr/bin/env bash\n"
            f"exec {shlex.quote(sys.executable)} -c {shlex.quote(HOOK)} {phase}\n",
            executable=True,
        )
    _git(primary, "add", ".")
    _git(primary, "commit", "-m", "chore: initialize fixture")
    _git(primary, "branch", TASK_BRANCH)
    _write(primary / ("shared.txt" if conflict else "primary.txt"), "primary\n")
    _git(primary, "add", ".")
    _git(primary, "commit", "-m", "feat: update primary")
    primary_head = _git(primary, "rev-parse", "HEAD")
    task = tmp_path / "task" if separate_worktree else primary
    if separate_worktree:
        _git(primary, "worktree", "add", str(task), TASK_BRANCH)
    else:
        _git(primary, "switch", TASK_BRANCH)
    _write(task / ("shared.txt" if conflict else "task.txt"), "task\n")
    _git(task, "add", ".")
    _git(task, "commit", "-m", "feat: update task")
    task_head = _git(task, "rev-parse", "HEAD")
    if interpreter:
        for worktree in {primary, task}:
            _write(
                worktree / ".venv" / "bin" / "python",
                f'#!/usr/bin/env bash\nexec {shlex.quote(sys.executable)} "$@"\n',
                executable=True,
            )
    _git(task, "config", "core.hooksPath", ".githooks")
    return MergeFixture(primary, task, primary_head, task_head)


@pytest.mark.parametrize("separate_worktree", [False, True])
def test_merge_reviews_divergent_candidate_before_gate_and_cleans_task(
    tmp_path: Path, separate_worktree: bool
) -> None:
    fixture = _fixture(tmp_path, separate_worktree=separate_worktree)
    task_tree = _git(fixture.task, "rev-parse", "HEAD^{tree}")

    result = fixture.run()

    assert result.returncode == 0, result.stdout
    merge_tree = _git(fixture.primary, "rev-parse", "HEAD^{tree}")
    records = fixture.records()
    assert [record["phase"] for record in records] == [
        "review",
        "gate",
        "commit-message",
    ]
    assert all(record["tree"] == merge_tree for record in records)
    assert all(record["task_ref"] == TASK_BRANCH for record in records)
    assert records[0]["base"] == fixture.primary_head
    assert records[0]["task"] == fixture.task_head
    assert merge_tree != task_tree
    assert _git(fixture.primary, "show", "HEAD:primary.txt") == "primary"
    assert _git(fixture.primary, "show", "HEAD:task.txt") == "task"
    assert _git(fixture.primary, "rev-parse", "HEAD^1") == fixture.primary_head
    assert _git(fixture.primary, "rev-parse", "HEAD^2") == fixture.task_head
    assert _git(fixture.primary, "branch", "--show-current") == "master"
    assert not _git(fixture.primary, "branch", "--list", TASK_BRANCH)
    assert not _git(fixture.primary, "status", "--porcelain=v1")
    if separate_worktree:
        assert not fixture.task.exists()
        assert str(fixture.task) not in _git(fixture.primary, "worktree", "list")


@pytest.mark.parametrize("separate_worktree", [False, True])
@pytest.mark.parametrize("failure", ["review", "gate"])
def test_failed_review_or_gate_aborts_merge_and_preserves_task(
    tmp_path: Path, separate_worktree: bool, failure: str
) -> None:
    fixture = _fixture(tmp_path, separate_worktree=separate_worktree)

    result = fixture.run(**{f"TEST_{failure.upper()}_FAIL": "1"})

    assert result.returncode != 0
    assert "task branch was retained" in result.stdout
    fixture.assert_retained()
    assert [record["phase"] for record in fixture.records()] == (
        ["review"] if failure == "review" else ["review", "gate"]
    )


@pytest.mark.parametrize("missing", ["reviewer", "interpreter"])
def test_missing_review_prerequisites_abort_without_advancing_primary(
    tmp_path: Path, missing: str
) -> None:
    fixture = _fixture(
        tmp_path, reviewer=missing != "reviewer", interpreter=missing != "interpreter"
    )

    result = fixture.run()

    assert result.returncode != 0
    assert (
        "review-code.py is missing"
        if missing == "reviewer"
        else "scripts/rebuild-env.sh"
    ) in result.stdout
    fixture.assert_retained()
    assert fixture.records() == []


@pytest.mark.parametrize("separate_worktree", [False, True])
def test_merge_conflict_aborts_before_review(
    tmp_path: Path, separate_worktree: bool
) -> None:
    fixture = _fixture(tmp_path, separate_worktree=separate_worktree, conflict=True)

    result = fixture.run()

    assert result.returncode != 0
    assert "merge failed" in result.stdout
    fixture.assert_retained()
    assert fixture.records() == []


@pytest.mark.skipif(os.name == "nt", reason="POSIX signal propagation contract")
def test_interrupted_review_aborts_merge_and_restores_task(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    result = fixture.run(TEST_REVIEW_SIGNAL="1")

    assert result.returncode == 143, result.stdout
    fixture.assert_retained()
    assert [record["phase"] for record in fixture.records()] == ["review"]


@pytest.mark.skipif(os.name == "nt", reason="POSIX signal propagation contract")
@pytest.mark.parametrize("interruption", [signal.SIGTERM, signal.SIGHUP, signal.SIGINT])
def test_wrapper_forwards_signal_before_aborting_pending_review(
    tmp_path: Path, interruption: signal.Signals
) -> None:
    fixture = _fixture(tmp_path)
    with subprocess.Popen(
        ["bash", "scripts/git-flow-merge.sh"],
        cwd=fixture.task,
        env=_environment(TEST_REVIEW_WAIT="1"),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    ) as process:
        try:
            deadline = time.monotonic() + 10
            while not fixture.records():
                assert process.poll() is None
                assert time.monotonic() < deadline, "reviewer did not start"
                time.sleep(0.01)
            process.send_signal(interruption)
            output, _ = process.communicate(timeout=10)
        finally:
            if process.poll() is None:
                process.terminate()
                process.communicate(timeout=10)

    assert process.returncode == 128 + interruption, output
    fixture.assert_retained()
    assert [record["phase"] for record in fixture.records()] == [
        "review",
        "review-terminated",
    ]
