from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

FAKE_CODEX = r'''
import json
import os
import subprocess
import sys
import time
from pathlib import Path

if sys.argv[1:] == ["--version"]:
    print("codex-cli test-fixture")
    raise SystemExit(0)

state = Path(os.environ["FAKE_CODEX_STATE"])
with (state / "calls.jsonl").open("a", encoding="utf-8") as stream:
    stream.write(json.dumps({"args": sys.argv[1:], "prompt": sys.stdin.read()}) + "\n")

mode = os.environ.get("FAKE_CODEX_MODE", "pass")
if mode in {"hold-pass", "hold-failure"}:
    (state / "held").write_text("review started", encoding="utf-8")
    while not (state / "release").exists():
        time.sleep(0.01)
    if mode == "hold-failure":
        raise SystemExit(7)
if mode == "nonzero":
    raise SystemExit(7)
if mode == "timeout":
    child_code = """
import fcntl
import os
import sys
import time
from pathlib import Path
state = Path(sys.argv[1])
lock = (state / 'child-lock').open('a', encoding='utf-8')
fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
(state / 'child-pid').write_text(str(os.getpid()), encoding='utf-8')
while True:
    time.sleep(0.1)
"""
    subprocess.Popen([sys.executable, "-c", child_code, str(state)])
    while not (state / "child-pid").exists():
        time.sleep(0.01)
    time.sleep(120)
    raise SystemExit(0)
if mode == "change-tree":
    Path("feature.py").write_text("value = 2\n", encoding="utf-8")
    subprocess.run(["git", "add", "feature.py"], check=True)

output = Path(sys.argv[sys.argv.index("--output-last-message") + 1])
if mode == "missing":
    raise SystemExit(0)
if mode == "malformed":
    output.write_text("not JSON", encoding="utf-8")
    raise SystemExit(0)

result = {"verdict": "pass", "summary": "Reviewed the candidate.", "findings": []}
if mode in {"p0", "p1", "p2", "contradictory"}:
    result["verdict"] = "pass" if mode == "contradictory" else "changes_requested"
    result["findings"] = [{
        "priority": 1 if mode == "contradictory" else int(mode[1]),
        "title": "Changed behavior loses data",
        "body": "The changed path discards a required value.",
        "file": "feature.py",
        "line_start": 1,
        "line_end": 1,
    }]
elif mode == "incomplete":
    result["verdict"] = "incomplete"
elif mode == "empty-summary":
    result["summary"] = "  "
elif mode == "unknown-verdict":
    result["verdict"] = "approved"
elif mode == "missing-findings":
    del result["findings"]
output.write_text(json.dumps(result), encoding="utf-8")
print(json.dumps({"type": "turn.completed"}))
'''


@dataclass(frozen=True)
class ReviewRepository:
    path: Path
    state: Path
    environment: dict[str, str]

    def git(self, *arguments: str) -> str:
        return subprocess.run(
            ("git", *arguments),
            cwd=self.path,
            env=self.environment,
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        ).stdout.strip()

    def review(
        self, *arguments: str, mode: str = "pass"
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            (sys.executable, "scripts/review-code.py", *arguments),
            cwd=self.path,
            env={**self.environment, "FAKE_CODEX_MODE": mode},
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )

    def calls(self) -> list[dict[str, object]]:
        path = self.state / "calls.jsonl"
        if not path.exists():
            return []
        return [
            json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
        ]

    def commit_file(self, name: str, content: str) -> str:
        (self.path / name).write_text(content, encoding="utf-8")
        self.git("add", name)
        self.git("commit", "-m", f"test: update {name}")
        return self.git("rev-parse", "HEAD")


@pytest.fixture
def review_repository(tmp_path: Path) -> ReviewRepository:
    if os.name != "posix":
        pytest.skip("The fake Codex executable and process-group oracle require POSIX")
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
    local = tmp_path / "repository"
    local.mkdir()
    state = tmp_path / "fake-codex-state"
    state.mkdir()
    executable_directory = tmp_path / "bin"
    executable_directory.mkdir()
    executable = executable_directory / "codex"
    executable.write_text(f"#!{sys.executable}\n{FAKE_CODEX}", encoding="utf-8")
    executable.chmod(0o755)
    environment["PATH"] = (
        str(executable_directory) + os.pathsep + environment.get("PATH", "")
    )
    environment["FAKE_CODEX_STATE"] = str(state)
    repo = ReviewRepository(local, state, environment)
    repo.git("init", "--initial-branch", "main")
    repo.git("config", "user.name", "Review Test")
    repo.git("config", "user.email", "review@example.invalid")
    repo.git("config", "workflow.primaryBranch", "main")
    for relative in ("scripts/review-code.py", "scripts/detect-primary-branch.sh"):
        destination = local / relative
        destination.parent.mkdir(exist_ok=True)
        shutil.copy2(ROOT / relative, destination)
    (local / "AGENTS.md").write_text(
        "# Review policy\n\nReport correctness defects.\n", encoding="utf-8"
    )
    repo.git("add", ".")
    repo.git("commit", "-m", "test: initialize review fixture")
    repo.git("switch", "-c", "task/example")
    repo.commit_file("feature.py", "value = 1\n")
    repo.git("switch", "main")
    repo.commit_file("primary.txt", "Concurrent primary work.\n")
    repo.git("merge", "--no-ff", "--no-commit", "task/example")
    return repo


def _assert_passed(result: subprocess.CompletedProcess[str]) -> None:
    assert result.returncode == 0, result.stdout + result.stderr


def _assert_blocked(result: subprocess.CompletedProcess[str]) -> None:
    assert result.returncode != 0, result.stdout + result.stderr


@contextmanager
def _held_review(repo: ReviewRepository, mode: str) -> Iterator[subprocess.Popen[str]]:
    process = subprocess.Popen(
        (sys.executable, "scripts/review-code.py", "run", "--index"),
        cwd=repo.path,
        env={**repo.environment, "FAKE_CODEX_MODE": mode},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 5
        while not (repo.state / "held").exists() and time.monotonic() < deadline:
            assert process.poll() is None, "The review exited before synchronization"
            time.sleep(0.01)
        assert (repo.state / "held").exists(), "The fake reviewer never started"
        yield process
    finally:
        (repo.state / "release").write_text("release review", encoding="utf-8")
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.communicate(timeout=5)


def test_review_binds_the_real_merge_candidate_and_verifies_after_commit(
    review_repository: ReviewRepository,
) -> None:
    repo = review_repository
    base = repo.git("rev-parse", "HEAD")
    tree = repo.git("write-tree")
    _assert_passed(repo.review("run", "--index", "--model", "fixture-model"))
    calls = repo.calls()
    assert len(calls) == 1
    arguments = calls[0]["args"]
    assert isinstance(arguments, list)
    assert arguments[0] == "exec"
    assert arguments[arguments.index("--sandbox") + 1] == "read-only"
    assert arguments[arguments.index("--model") + 1] == "fixture-model"
    assert {
        "--json",
        "--output-schema",
        "--output-last-message",
        "--ephemeral",
        "-",
    } <= set(arguments)
    prompt = str(calls[0]["prompt"])
    assert base in prompt
    assert tree in prompt
    _assert_passed(repo.review("verify", "--index", mode="nonzero"))
    repo.git("commit", "-m", "Merge task/example")
    _assert_passed(repo.review("verify", "--revision", "HEAD", mode="nonzero"))
    assert repo.calls() == calls, "Offline verification must never invoke Codex"
    assert repo.git("status", "--porcelain") == ""


@pytest.mark.parametrize(
    "mode",
    [
        "p0",
        "p1",
        "p2",
        "contradictory",
        "incomplete",
        "malformed",
        "missing",
        "nonzero",
        "empty-summary",
        "unknown-verdict",
        "missing-findings",
    ],
)
def test_review_fails_closed_without_a_complete_clean_result(
    review_repository: ReviewRepository, mode: str
) -> None:
    repo = review_repository
    _assert_blocked(repo.review("run", "--index", mode=mode))
    _assert_blocked(repo.review("verify", "--index"))
    assert len(repo.calls()) == 1


@pytest.mark.parametrize("untracked", [False, True])
def test_review_rejects_dirty_worktrees_before_invoking_codex(
    review_repository: ReviewRepository, untracked: bool
) -> None:
    repo = review_repository
    name = "untracked.txt" if untracked else "primary.txt"
    (repo.path / name).write_text("Unreviewed local changes.\n", encoding="utf-8")
    _assert_blocked(repo.review("run", "--index"))
    assert repo.calls() == []


@pytest.mark.parametrize("name", ["feature.py", "AGENTS.md", "scripts/review-code.py"])
def test_review_receipt_does_not_cover_changed_code_policy_or_runner(
    review_repository: ReviewRepository, name: str
) -> None:
    repo = review_repository
    _assert_passed(repo.review("run", "--index"))
    path = repo.path / name
    with path.open("a", encoding="utf-8") as stream:
        stream.write("\n# Candidate changed after review.\n")
    repo.git("add", name)
    _assert_blocked(repo.review("verify", "--index"))
    assert len(repo.calls()) == 1


def test_review_receipt_does_not_cover_a_new_base_with_the_same_tree(
    review_repository: ReviewRepository,
) -> None:
    repo = review_repository
    _assert_passed(repo.review("run", "--index"))
    candidate_tree = repo.git("write-tree")
    old_base = repo.git("rev-parse", "HEAD")
    replacement = repo.git(
        "commit-tree",
        "HEAD^{tree}",
        "-p",
        old_base,
        "-m",
        "test: advance primary metadata",
    )
    repo.git("update-ref", "refs/heads/main", replacement, old_base)
    assert repo.git("write-tree") == candidate_tree
    _assert_blocked(repo.review("verify", "--index"))
    assert len(repo.calls()) == 1


def test_review_index_requires_a_pending_merge(
    review_repository: ReviewRepository,
) -> None:
    repo = review_repository
    repo.git("merge", "--abort")
    _assert_blocked(repo.review("run", "--index"))
    assert repo.calls() == []


def test_explicit_failed_rerun_invalidates_the_previous_pass(
    review_repository: ReviewRepository,
) -> None:
    repo = review_repository
    _assert_passed(repo.review("run", "--index"))
    _assert_blocked(repo.review("run", "--index", mode="p1"))
    _assert_blocked(repo.review("verify", "--index"))
    assert len(repo.calls()) == 2


@pytest.mark.parametrize("index", [False, True])
def test_review_verification_rejects_a_different_expected_tree(
    review_repository: ReviewRepository, index: bool
) -> None:
    repo = review_repository
    candidate_tree = repo.git("write-tree")
    other_tree = repo.git("rev-parse", "HEAD^{tree}")
    assert candidate_tree != other_tree
    _assert_passed(repo.review("run", "--index"))
    if not index:
        repo.git("commit", "-m", "Merge task/example")
    scope = ("--index",) if index else ("--revision", "HEAD")
    _assert_passed(repo.review("verify", *scope, "--expected-tree", candidate_tree))
    _assert_blocked(repo.review("verify", *scope, "--expected-tree", other_tree))
    _assert_passed(repo.review("verify", *scope, "--expected-tree", candidate_tree))
    assert len(repo.calls()) == 1


@pytest.mark.parametrize("failure", ["dirty-worktree", "invalid-timeout"])
def test_failed_review_preflight_invalidates_the_previous_pass(
    review_repository: ReviewRepository, failure: str
) -> None:
    repo = review_repository
    _assert_passed(repo.review("run", "--index"))
    _assert_passed(repo.review("verify", "--index"))
    path = repo.path / "primary.txt"
    original = path.read_bytes()
    arguments = ("--timeout-seconds", "0") if failure == "invalid-timeout" else ()
    if failure == "dirty-worktree":
        path.write_text("Unstaged change blocks review.\n", encoding="utf-8")
    _assert_blocked(repo.review("run", "--index", *arguments))
    path.write_bytes(original)
    assert repo.git("diff", "--name-only") == ""
    _assert_blocked(repo.review("verify", "--index"))
    assert len(repo.calls()) == 1, "Preflight failures must occur before invoking Codex"


@pytest.mark.parametrize("mode", ["hold-pass", "hold-failure"])
def test_concurrent_review_rejects_a_second_owner_without_invoking_codex(
    review_repository: ReviewRepository, mode: str
) -> None:
    repo = review_repository
    _assert_passed(repo.review("run", "--index"))
    with _held_review(repo, mode) as owner:
        _assert_blocked(repo.review("verify", "--index"))
        started = time.monotonic()
        competing = repo.review("run", "--index")
        assert time.monotonic() - started < 5, (
            "A competing run must not wait for its owner"
        )
        _assert_blocked(competing)
        assert "already running" in (competing.stdout + competing.stderr).lower()
        assert len(repo.calls()) == 2, "A competing run must not invoke Codex"
        _assert_blocked(repo.review("verify", "--index"))
        (repo.state / "release").write_text("release review", encoding="utf-8")
        stdout, stderr = owner.communicate(timeout=5)
        if mode == "hold-pass":
            assert owner.returncode == 0, stdout + stderr
            _assert_passed(repo.review("verify", "--index"))
        else:
            assert owner.returncode != 0, stdout + stderr
            _assert_blocked(repo.review("verify", "--index"))
    _assert_passed(repo.review("run", "--index"))
    _assert_passed(repo.review("verify", "--index"))
    assert len(repo.calls()) == 3


def test_terminated_review_releases_its_lock_without_a_passing_receipt(
    review_repository: ReviewRepository,
) -> None:
    repo = review_repository
    _assert_passed(repo.review("run", "--index"))
    with _held_review(repo, "hold-pass") as owner:
        _assert_blocked(repo.review("verify", "--index"))
        owner.terminate()
        stdout, stderr = owner.communicate(timeout=5)
        assert owner.returncode != 0, stdout + stderr
        _assert_blocked(repo.review("verify", "--index"))
    _assert_passed(repo.review("run", "--index"))
    _assert_passed(repo.review("verify", "--index"))
    assert len(repo.calls()) == 3


def test_candidate_changes_during_review_cannot_receive_a_passing_receipt(
    review_repository: ReviewRepository,
) -> None:
    repo = review_repository
    _assert_blocked(repo.review("run", "--index", mode="change-tree"))
    _assert_blocked(repo.review("verify", "--index"))
    assert len(repo.calls()) == 1


def test_review_can_explicitly_cover_an_existing_commit(
    review_repository: ReviewRepository,
) -> None:
    repo = review_repository
    repo.git("merge", "--abort")
    _assert_passed(repo.review("run"))
    _assert_passed(repo.review("verify"))
    assert len(repo.calls()) == 1


def _child_released_lock(state: Path) -> bool:
    import fcntl

    # The fake descendant keeps this descriptor locked for its entire lifetime.
    # Process exit releases the lock even before an orphaned zombie is reaped.
    with (state / "child-lock").open("a", encoding="utf-8") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
    return True


def test_review_timeout_terminates_the_codex_descendant(
    review_repository: ReviewRepository,
) -> None:
    repo = review_repository
    child_pid: int | None = None
    try:
        started = time.monotonic()
        result = repo.review("run", "--index", "--timeout-seconds", "1", mode="timeout")
        elapsed = time.monotonic() - started
        pid_path = repo.state / "child-pid"
        assert pid_path.exists(), "The timeout oracle must exercise a real descendant"
        child_pid = int(pid_path.read_text(encoding="utf-8"))
        _assert_blocked(result)
        assert elapsed < 10, result.stdout + result.stderr
        deadline = time.monotonic() + 2
        while not _child_released_lock(repo.state) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert _child_released_lock(repo.state), "The Codex descendant survived timeout"
        _assert_blocked(repo.review("verify", "--index"))
    finally:
        if child_pid is not None and not _child_released_lock(repo.state):
            try:
                os.kill(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
