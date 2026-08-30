from pathlib import Path

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def test_publish_workflow_requires_the_exact_default_branch_ref() -> None:
    workflow = (_REPOSITORY_ROOT / ".github/workflows/publish.yml").read_text(
        encoding="utf-8"
    )

    assert "github.ref ==" in workflow
    assert (
        "format('refs/heads/{0}', github.event.repository.default_branch)" in workflow
    )
    assert "github.ref_name" not in workflow


def test_publish_workflow_compares_against_the_push_before_revision() -> None:
    workflow = (_REPOSITORY_ROOT / ".github/workflows/publish.yml").read_text(
        encoding="utf-8"
    )

    assert "BEFORE_SHA: ${{ github.event.before }}" in workflow
    assert "HEAD~1" not in workflow
