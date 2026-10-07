from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCOPE = ROOT / "scripts" / "check_change_scope.py"
DOCS = ROOT / "scripts" / "check-docs.py"


@dataclass(frozen=True)
class DocumentationRepository:
    root: Path
    environment: dict[str, str]

    def run(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            arguments,
            cwd=self.root,
            env=self.environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )

    def git(self, *arguments: str) -> str:
        result = self.run("git", *arguments)
        assert result.returncode == 0, result.stderr
        return result.stdout.strip()

    def stage(self, name: str, content: str | bytes) -> None:
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content.encode() if isinstance(content, str) else content)
        self.git("add", "--", name)

    def scope(self, *arguments: str) -> str:
        result = self.run(sys.executable, str(SCOPE), *(arguments or ("--index",)))
        assert result.returncode == 0, result.stderr
        return result.stdout.strip()

    def lint_stub(self, body: str) -> Path:
        tools = self.root / "node_modules" / ".bin"
        tools.mkdir(parents=True)
        script = tools / "lint_stub.py"
        script.write_text(body, encoding="utf-8")
        if os.name == "nt":
            executable = tools / "markdownlint-cli2.cmd"
            executable.write_text(
                f'@"{sys.executable}" "{script}"\r\n', encoding="utf-8"
            )
        else:
            executable = tools / "markdownlint-cli2"
            executable.write_text(f"#!{sys.executable}\n{body}", encoding="utf-8")
            executable.chmod(0o755)
        return executable


@pytest.fixture
def documentation_repository(tmp_path: Path) -> DocumentationRepository:
    environment = os.environ.copy()
    local_names = subprocess.run(
        ("git", "rev-parse", "--local-env-vars"),
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    ).stdout.splitlines()
    for name in local_names:
        environment.pop(name, None)
    environment.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_SYSTEM=os.devnull)
    repository = DocumentationRepository(tmp_path, environment)
    repository.git("init", "-b", "main")
    repository.git("config", "user.name", "Documentation Check Test")
    repository.git("config", "user.email", "documentation@example.invalid")
    repository.git("config", "core.fileMode", "true")
    repository.stage("README.md", "# Original\n")
    repository.stage("docs/guide.md", "# Guide\n")
    repository.stage("src/runtime.py", "value = 1\n")
    repository.stage(".markdownlint-cli2.jsonc", '{"globs": ["**/*.md"]}\n')
    repository.git("commit", "-m", "test: seed documentation fixture")
    return repository


@pytest.mark.parametrize(
    "name",
    (
        "README.md",
        "docs/guide.md",
        "docs/nested/new.md",
        "docs/- spaced name.md",
        "benchmarks/README.md",
        "verification/README.md",
    ),
)
def test_registered_regular_text_uses_documentation_profile(
    documentation_repository: DocumentationRepository, name: str
) -> None:
    documentation_repository.stage(name, "# Updated\n")
    assert documentation_repository.scope() == "documentation"


def test_newline_in_git_filename_is_not_a_second_path(
    documentation_repository: DocumentationRepository,
) -> None:
    if os.name == "nt":
        pytest.skip("Windows filenames cannot contain newlines")
    documentation_repository.stage("docs/first\nsecond.md", "# Updated\n")
    assert documentation_repository.scope() == "documentation"


@pytest.mark.parametrize(
    "name",
    (
        "src/runtime.py",
        "unknown.md",
        "docs/run.py",
        "scripts/check.py",
        "tests/example.py",
        "pyproject.toml",
        ".markdownlint-cli2.jsonc",
        "verification/schema/catalog.toml",
        "AGENTS.md",
        "CLAUDE.md",
        "docs/nested/AGENTS.md",
        "docs/CLAUDE.md",
    ),
)
def test_control_code_and_unknown_paths_require_full_profile(
    documentation_repository: DocumentationRepository, name: str
) -> None:
    documentation_repository.stage("README.md", "# Updated\n")
    documentation_repository.stage(name, "changed\n")
    assert documentation_repository.scope() == "full"


@pytest.mark.parametrize("content", (b"binary\0payload", b"\xff\xfe"))
def test_non_text_markdown_requires_full_profile(
    documentation_repository: DocumentationRepository, content: bytes
) -> None:
    documentation_repository.stage("README.md", content)
    assert documentation_repository.scope() == "full"


def test_empty_diff_requires_full_profile(
    documentation_repository: DocumentationRepository,
) -> None:
    assert documentation_repository.scope() == "full"


def test_document_deletion_uses_documentation_profile(
    documentation_repository: DocumentationRepository,
) -> None:
    documentation_repository.git("rm", "README.md")
    assert documentation_repository.scope() == "documentation"


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    (
        ("docs/guide.md", "docs/renamed.md", "documentation"),
        ("src/runtime.py", "docs/runtime.md", "full"),
        ("docs/guide.md", "src/guide.md", "full"),
    ),
)
def test_rename_checks_both_endpoints(
    documentation_repository: DocumentationRepository,
    old: str,
    new: str,
    expected: str,
) -> None:
    documentation_repository.git("mv", old, new)
    assert documentation_repository.scope() == expected


def test_executable_bit_requires_full_profile(
    documentation_repository: DocumentationRepository,
) -> None:
    documentation_repository.git("update-index", "--chmod=+x", "README.md")
    assert documentation_repository.scope() == "full"


def test_symlink_requires_full_profile(
    documentation_repository: DocumentationRepository,
) -> None:
    repository = documentation_repository
    object_id = repository.git("rev-parse", "HEAD:README.md")
    repository.git("update-index", "--cacheinfo", f"120000,{object_id},README.md")
    assert repository.scope() == "full"


def test_classifier_uses_index_and_complete_base_comparison(
    documentation_repository: DocumentationRepository,
) -> None:
    repository = documentation_repository
    original = repository.git("rev-parse", "HEAD")
    repository.stage("src/runtime.py", "value = 2\n")
    (repository.root / "src/runtime.py").write_text("value = 1\n", encoding="utf-8")
    assert repository.scope() == "full"
    repository.git("commit", "-m", "fix: change runtime")
    repository.stage("README.md", "# Updated\n")
    repository.git("commit", "-m", "docs: explain runtime")
    assert repository.scope("--candidate", "HEAD", "--base", original) == "full"
    assert repository.scope("--candidate", "HEAD", "--base", "HEAD^") == "documentation"


def test_invalid_git_reference_cannot_report_documentation(
    documentation_repository: DocumentationRepository,
) -> None:
    result = documentation_repository.run(
        sys.executable, str(SCOPE), "--candidate", "missing-ref", "--base", "HEAD"
    )
    assert result.returncode != 0
    assert result.stdout == ""
    assert "Git operation failed" in result.stderr


def test_document_checker_lints_exact_index_and_configuration(
    documentation_repository: DocumentationRepository,
) -> None:
    repository = documentation_repository
    repository.stage("README.md", "# Staged\n")
    (repository.root / "README.md").write_text("WORKTREE ONLY\n", encoding="utf-8")
    (repository.root / ".markdownlint-cli2.jsonc").write_text(
        "WORKTREE CONFIG\n", encoding="utf-8"
    )
    repository.lint_stub(
        "from pathlib import Path\n"
        "assert Path('README.md').read_text() == '# Staged\\n'\n"
        "assert Path('docs/guide.md').read_text() == '# Guide\\n'\n"
        "assert Path('.markdownlint-cli2.jsonc').read_text() == "
        '\'{"globs": ["**/*.md"]}\\n\'\n'
        "assert not Path('src/runtime.py').exists()\n"
    )
    result = repository.run(sys.executable, str(DOCS), "--index")
    assert result.returncode == 0, result.stderr
    assert "Documentation checks passed for tree" in result.stdout


@pytest.mark.parametrize("target", ("index", "revision"))
def test_lint_failure_blocks_documentation_acceptance(
    documentation_repository: DocumentationRepository, target: str
) -> None:
    repository = documentation_repository
    repository.stage("README.md", "# Changed\n")
    repository.lint_stub("raise SystemExit(23)\n")
    arguments = ["--index"]
    if target == "revision":
        repository.git("commit", "-m", "docs: change readme")
        arguments = ["--candidate", "HEAD", "--base", "HEAD^"]
    result = repository.run(sys.executable, str(DOCS), *arguments)
    assert result.returncode != 0
    assert "Markdown check failed" in result.stderr
    assert "Documentation checks passed" not in result.stdout


def test_missing_markdown_tool_blocks_documentation_acceptance(
    documentation_repository: DocumentationRepository,
) -> None:
    repository = documentation_repository
    repository.stage("README.md", "# Changed\n")
    result = repository.run(sys.executable, str(DOCS), "--index")
    assert result.returncode != 0
    assert "Missing repository-local Markdown tool" in result.stderr


def test_diff_whitespace_failure_blocks_before_lint(
    documentation_repository: DocumentationRepository,
) -> None:
    repository = documentation_repository
    repository.stage("README.md", "# Trailing whitespace  \n")
    repository.lint_stub("raise AssertionError('lint must not run')\n")
    result = repository.run(sys.executable, str(DOCS), "--index")
    assert result.returncode != 0
    assert "Git operation failed" in result.stderr
    assert "README.md:1" in result.stderr
    assert "lint must not run" not in result.stderr


@pytest.mark.parametrize("target", ("index", "revision"))
def test_candidate_change_during_lint_blocks_acceptance(
    documentation_repository: DocumentationRepository, target: str
) -> None:
    repository = documentation_repository
    previous = repository.git("rev-parse", "HEAD")
    repository.stage("README.md", "# Changed\n")
    arguments = ["--index"]
    command: tuple[str, ...]
    if target == "revision":
        repository.git("commit", "-m", "docs: change readme")
        repository.git("branch", "candidate")
        arguments = ["--candidate", "candidate", "--base", previous]
        command = ("git", "update-ref", "refs/heads/candidate", previous)
    else:
        command = ("git", "read-tree", previous)
    repository.lint_stub(
        "import subprocess\n"
        f"subprocess.run({command!r}, cwd={str(repository.root)!r}, check=True)\n"
    )
    result = repository.run(sys.executable, str(DOCS), *arguments)
    assert result.returncode != 0
    assert "Candidate changed" in result.stderr


def test_non_documentation_changes_never_start_markdown_only_gate(
    documentation_repository: DocumentationRepository,
) -> None:
    repository = documentation_repository
    repository.stage("src/runtime.py", "value = 3\n")
    marker = repository.root / "lint-started"
    repository.lint_stub(
        f"from pathlib import Path\nPath({str(marker)!r}).write_text('unexpected')\n"
    )
    result = repository.run(sys.executable, str(DOCS), "--index")
    assert result.returncode != 0
    assert "not a documentation-only change" in result.stderr
    assert not marker.exists()


def test_deleted_document_is_absent_from_lint_snapshot(
    documentation_repository: DocumentationRepository,
) -> None:
    repository = documentation_repository
    repository.git("rm", "README.md")
    repository.lint_stub(
        "from pathlib import Path\n"
        "assert not Path('README.md').exists()\n"
        "assert Path('docs/guide.md').exists()\n"
    )
    result = repository.run(sys.executable, str(DOCS), "--index")
    assert result.returncode == 0, result.stderr


def test_document_export_preserves_special_filename(
    documentation_repository: DocumentationRepository,
) -> None:
    if os.name == "nt":
        pytest.skip("Windows filenames cannot contain newlines")
    repository = documentation_repository
    name = "docs/space and\nnewline.md"
    repository.stage(name, "# Special\n")
    repository.lint_stub(
        "from pathlib import Path\n"
        f"assert Path({name!r}).read_text() == '# Special\\n'\n"
    )
    result = repository.run(sys.executable, str(DOCS), "--index")
    assert result.returncode == 0, result.stderr


def test_missing_candidate_configuration_blocks_checks(
    documentation_repository: DocumentationRepository,
) -> None:
    repository = documentation_repository
    repository.git("rm", ".markdownlint-cli2.jsonc")
    repository.git("commit", "-m", "test: omit config")
    repository.stage("README.md", "# Changed\n")
    repository.lint_stub("raise AssertionError('lint must not run')\n")
    result = repository.run(sys.executable, str(DOCS), "--index")
    assert result.returncode != 0
    assert "Candidate has no regular" in result.stderr


def test_docs_profile_does_not_trust_candidate_config_in_worktree(
    documentation_repository: DocumentationRepository,
) -> None:
    repository = documentation_repository
    repository.stage("README.md", "# Changed\n")
    repository.stage(
        ".markdownlint-cli2.jsonc", json.dumps({"config": {"default": False}})
    )
    assert repository.scope() == "full"


@pytest.mark.parametrize("target", ("index", "revision"))
def test_base_change_during_lint_blocks_acceptance(
    documentation_repository: DocumentationRepository, target: str
) -> None:
    repository = documentation_repository
    repository.git("branch", "baseline")
    repository.stage("README.md", "# Changed\n")
    candidate_tree = repository.git("write-tree")
    changed_base = repository.git(
        "commit-tree", candidate_tree, "-p", "HEAD", "-m", "docs: alternate base"
    )
    repository.lint_stub(
        "import subprocess\n"
        "subprocess.run(('git', 'update-ref', 'refs/heads/baseline', "
        f"{changed_base!r}), "
        f"cwd={str(repository.root)!r}, check=True)\n"
    )
    arguments = ["--index"] if target == "index" else ["--candidate", candidate_tree]
    result = repository.run(sys.executable, str(DOCS), *arguments, "--base", "baseline")
    assert result.returncode != 0
    assert "Base changed" in result.stderr
    assert "Documentation checks passed" not in result.stdout
