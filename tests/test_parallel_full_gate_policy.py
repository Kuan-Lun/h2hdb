import tomllib
from pathlib import Path

import pytest
from conftest import (
    MARIADB_XDIST_GROUP,
    claim_live_mariadb_container,
    live_mariadb_group_marker_required,
    live_mariadb_xdist_group,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def test_live_mariadb_group_covers_direct_and_transitive_fixtures() -> None:
    assert live_mariadb_xdist_group(("mariadb_config",), {}) == MARIADB_XDIST_GROUP
    assert (
        live_mariadb_xdist_group(
            ("generated_mariadb", "mariadb_config", "mariadb_container"),
            {},
        )
        == MARIADB_XDIST_GROUP
    )


def test_live_mariadb_group_covers_indirect_db_config_parameter() -> None:
    assert (
        live_mariadb_xdist_group(
            ("db_config",),
            {"db_config": "mariadb"},
        )
        == MARIADB_XDIST_GROUP
    )
    assert (
        live_mariadb_xdist_group(
            ("db_config",),
            {"db_config": "sqlite"},
        )
        is None
    )


def test_live_mariadb_group_does_not_classify_string_only_backend_cases() -> None:
    assert live_mariadb_xdist_group((), {"backend": "mariadb"}) is None
    assert live_mariadb_xdist_group(("sqlite_config",), {}) is None


def test_live_mariadb_group_rejects_a_conflicting_existing_group() -> None:
    matching_marker = pytest.mark.xdist_group(name=MARIADB_XDIST_GROUP).mark
    conflicting_marker = pytest.mark.xdist_group(name="other-live-service").mark

    assert live_mariadb_group_marker_required(())
    assert not live_mariadb_group_marker_required((matching_marker,))
    with pytest.raises(ValueError, match="other-live-service"):
        live_mariadb_group_marker_required((conflicting_marker,))


def test_live_mariadb_container_claim_is_process_exclusive(tmp_path: Path) -> None:
    claim = claim_live_mariadb_container(tmp_path, "test-run")

    assert claim.is_dir()
    with pytest.raises(RuntimeError, match="single xdist worker group"):
        claim_live_mariadb_container(tmp_path, "test-run")


def test_pytest_xdist_is_a_required_bounded_development_dependency() -> None:
    pyproject = tomllib.loads(
        (REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )

    assert (
        "pytest-xdist>=3.8.0,<4.0.0"
        in pyproject["project"]["optional-dependencies"]["dev"]
    )
    assert pyproject["tool"]["pytest"]["ini_options"]["required_plugins"] == [
        "pytest-xdist>=3.8.0,<4.0.0"
    ]


def test_full_gate_pins_bounded_xdist_and_timing_policy() -> None:
    full_gate = (REPOSITORY_ROOT / "scripts" / "check-full.sh").read_text(
        encoding="utf-8"
    )

    assert "readonly pytest_workers=4" in full_gate
    assert '--numprocesses="$pytest_workers"' in full_gate
    assert "--dist=loadgroup" in full_gate
    assert "--max-worker-restart=0" in full_gate
    assert "--durations=50" in full_gate
    assert 'local started_at="$SECONDS"' in full_gate
    assert '"$((SECONDS - started_at))"' in full_gate
