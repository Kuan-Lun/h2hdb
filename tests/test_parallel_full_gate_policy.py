import subprocess
import tomllib
from pathlib import Path
from unittest.mock import Mock

import pytest
from conftest import (
    AUTO_WORKER_CAP,
    MACOS_PERFORMANCE_CORE_SYSCTL,
    MACOS_SYSCTL_TIMEOUT_SECONDS,
    MARIADB_XDIST_GROUP,
    OVERRIDE_WORKER_CAP,
    claim_live_mariadb_container,
    live_mariadb_group_marker_required,
    live_mariadb_xdist_group,
    macos_performance_core_count,
    select_pytest_worker_count,
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


@pytest.mark.parametrize(
    ("system", "performance_cores", "process_cpus", "expected"),
    (
        ("Darwin", 10, 14, 10),
        ("Darwin", 10, 4, 4),
        ("Darwin", 6, 12, 6),
        ("Darwin", None, 32, AUTO_WORKER_CAP),
        ("Linux", None, 4, 4),
        ("Linux", None, 32, AUTO_WORKER_CAP),
        ("Linux", None, None, 1),
        ("Linux", None, 0, 1),
    ),
)
def test_auto_worker_policy_uses_performance_or_affinity_aware_counts(
    system: str,
    performance_cores: int | None,
    process_cpus: int | None,
    expected: int,
) -> None:
    assert (
        select_pytest_worker_count(
            override=None,
            system=system,
            macos_performance_cores=performance_cores,
            process_cpus=process_cpus,
        )
        == expected
    )


def test_worker_override_is_explicit_and_not_silently_clamped() -> None:
    assert (
        select_pytest_worker_count(
            override="10",
            system="Darwin",
            macos_performance_cores=6,
            process_cpus=6,
        )
        == 10
    )
    assert (
        select_pytest_worker_count(
            override=str(OVERRIDE_WORKER_CAP),
            system="Linux",
            macos_performance_cores=None,
            process_cpus=2,
        )
        == OVERRIDE_WORKER_CAP
    )


@pytest.mark.parametrize("override", ("", "invalid", "0", "17"))
def test_worker_override_rejects_invalid_or_out_of_range_values(
    override: str,
) -> None:
    with pytest.raises(ValueError, match="H2HDB_PYTEST_WORKERS"):
        select_pytest_worker_count(
            override=override,
            system="Darwin",
            macos_performance_cores=10,
            process_cpus=10,
        )


def test_macos_performance_core_probe_uses_the_bounded_sysctl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = Mock(
        return_value=subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="10\n",
            stderr="",
        )
    )
    monkeypatch.setattr(subprocess, "run", run)

    assert macos_performance_core_count() == 10
    run.assert_called_once_with(
        ["/usr/sbin/sysctl", "-n", MACOS_PERFORMANCE_CORE_SYSCTL],
        check=False,
        capture_output=True,
        text=True,
        timeout=MACOS_SYSCTL_TIMEOUT_SECONDS,
    )


def test_macos_performance_core_probe_rejects_nonzero_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        subprocess,
        "run",
        Mock(
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=1,
                stdout="10\n",
                stderr="unknown oid",
            )
        ),
    )

    assert macos_performance_core_count() is None


def test_macos_performance_core_probe_rejects_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        subprocess,
        "run",
        Mock(side_effect=subprocess.TimeoutExpired(cmd="sysctl", timeout=2)),
    )

    assert macos_performance_core_count() is None


@pytest.mark.parametrize("stdout", ("not-a-count\n", "0\n", "-1\n", ""))
def test_macos_performance_core_probe_rejects_invalid_output(
    monkeypatch: pytest.MonkeyPatch,
    stdout: str,
) -> None:
    monkeypatch.setattr(
        subprocess,
        "run",
        Mock(
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=stdout,
                stderr="",
            )
        ),
    )

    assert macos_performance_core_count() is None


def test_full_gate_pins_bounded_xdist_and_timing_policy() -> None:
    full_gate = (REPOSITORY_ROOT / "scripts" / "check-full.sh").read_text(
        encoding="utf-8"
    )

    assert "--numprocesses=auto" in full_gate
    assert "--dist=loadgroup" in full_gate
    assert "--max-worker-restart=0" in full_gate
    assert "--durations=50" in full_gate
    assert 'local started_at="$SECONDS"' in full_gate
    assert '"$((SECONDS - started_at))"' in full_gate
