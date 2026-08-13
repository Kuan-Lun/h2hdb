"""Run the repository-owned executable formal verification targets."""

from __future__ import annotations

import argparse
import hashlib
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path
from typing import Literal

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
VERIFICATION_ROOT = REPOSITORY_ROOT / "verification"
TOOLS_LOCK = VERIFICATION_ROOT / "tools.lock.toml"
COVERAGE_MANIFEST = VERIFICATION_ROOT / "invariants.toml"
TlaRuntime = Literal["auto", "host", "docker"]


def _run(command: list[str], *, cwd: Path = REPOSITORY_ROOT) -> None:
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=cwd, check=True)


def verify_lean() -> None:
    files = sorted(VERIFICATION_ROOT.rglob("*.lean"))
    if not files:
        raise RuntimeError("No Lean verification files were found")
    for path in files:
        _run(["lean", "--error=warning", str(path.relative_to(REPOSITORY_ROOT))])


def verify_coverage(*, validate_only: bool = False) -> None:
    checker = VERIFICATION_ROOT / "check_coverage.py"
    if not checker.is_file() or not COVERAGE_MANIFEST.is_file():
        raise RuntimeError("The formal invariant coverage gate is incomplete")
    command = [sys.executable, str(checker), str(COVERAGE_MANIFEST)]
    if validate_only:
        command.append("--validate-only")
    _run(command)


def verify_schema() -> None:
    checker = VERIFICATION_ROOT / "schema" / "check_contract.py"
    contract = VERIFICATION_ROOT / "schema" / "catalog.toml"
    operational_contract = VERIFICATION_ROOT / "schema" / "operational.toml"
    generator = VERIFICATION_ROOT / "lean" / "generate_schema_proof.py"
    physical_generator = VERIFICATION_ROOT / "schema" / "generate_physical.py"
    operational_generator = (
        VERIFICATION_ROOT / "schema" / "generate_operational_physical.py"
    )
    operational_physical = VERIFICATION_ROOT / "schema" / "operational_physical.toml"
    operational_refinement = VERIFICATION_ROOT / "schema" / "operational_refinement.py"
    operational_lean_generator = (
        VERIFICATION_ROOT / "lean" / "generate_operational_schema_proof.py"
    )
    provider_generator = (
        REPOSITORY_ROOT / "scripts" / "generate-vnext-schema-provider.py"
    )
    if (
        not checker.is_file()
        or not contract.is_file()
        or not operational_contract.is_file()
    ):
        raise RuntimeError("The executable schema contract is incomplete")
    if (
        not generator.is_file()
        or not physical_generator.is_file()
        or not provider_generator.is_file()
    ):
        raise RuntimeError("A schema/physical generator is missing")
    _run([sys.executable, str(generator), "--check"])
    _run([sys.executable, str(physical_generator), "--check"])
    _run([sys.executable, str(operational_lean_generator), "--check"])
    _run([sys.executable, str(operational_generator), "--check"])
    _run(
        [
            sys.executable,
            str(operational_refinement),
            str(operational_contract),
            str(operational_physical),
        ]
    )
    _run([sys.executable, str(checker), str(contract)])
    _run([sys.executable, str(checker), str(operational_contract)])
    _run([sys.executable, str(provider_generator), "--check"])


def _host_java_available() -> bool:
    java = shutil.which("java")
    if java is None:
        return False
    result = subprocess.run(
        [java, "-version"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def _locked_runtime_image() -> str:
    with TOOLS_LOCK.open("rb") as stream:
        document = tomllib.load(stream)
    value = document.get("tla_plus", {}).get("tlc", {}).get("runtime_image")
    if (
        not isinstance(value, str)
        or re.fullmatch(r"[^@]+@sha256:[0-9a-f]{64}", value) is None
    ):
        raise RuntimeError(
            f"tla_plus.tlc.runtime_image in {TOOLS_LOCK} must be digest-pinned"
        )
    return value


def _validate_tlc_lock() -> None:
    with TOOLS_LOCK.open("rb") as stream:
        document = tomllib.load(stream)
    tlc = document.get("tla_plus", {}).get("tlc")
    if not isinstance(tlc, dict):
        raise RuntimeError(f"Missing [tla_plus.tlc] in {TOOLS_LOCK}")
    if tlc.get("version") != "1.7.4":
        raise RuntimeError(f"Unsupported TLC version in {TOOLS_LOCK}")
    _locked_runtime_image()


def _verify_tla_jar(tla_jar: Path) -> None:
    with TOOLS_LOCK.open("rb") as stream:
        document = tomllib.load(stream)
    expected = document.get("tla_plus", {}).get("tlc", {}).get("sha256")
    if (
        not isinstance(expected, str)
        or re.fullmatch(r"[0-9a-fA-F]{64}", expected) is None
    ):
        raise RuntimeError(f"tla_plus.tlc.sha256 in {TOOLS_LOCK} is invalid")
    digest = hashlib.sha256()
    with tla_jar.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    actual = digest.hexdigest()
    if actual != expected.lower():
        raise RuntimeError(
            f"TLC checksum mismatch: expected={expected.lower()} actual={actual}"
        )


def _run_tlc_with_host_java(
    tla_jar: Path, config: Path, module: Path, metadata_directory: Path
) -> None:
    _run(
        [
            "java",
            "-XX:+UseParallelGC",
            "-Xmx4g",
            "-cp",
            str(tla_jar.resolve()),
            "tlc2.TLC",
            "-workers",
            "auto",
            "-metadir",
            str(metadata_directory),
            "-config",
            config.name,
            module.name,
        ],
        cwd=config.parent,
    )


def _run_tlc_with_docker(tla_jar: Path, config: Path, module: Path) -> None:
    if shutil.which("docker") is None:
        raise RuntimeError("Neither a working Java runtime nor Docker is available")
    container_repository = Path("/workspace")
    container_tla_directory = container_repository / "verification" / "tla"
    _run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--read-only",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,size=512m",
            "--mount",
            f"type=bind,src={REPOSITORY_ROOT},dst={container_repository},readonly",
            "--mount",
            f"type=bind,src={tla_jar.resolve()},dst=/tools/tla2tools.jar,readonly",
            "--workdir",
            str(container_tla_directory),
            _locked_runtime_image(),
            "java",
            "-XX:+UseParallelGC",
            "-Xmx4g",
            "-cp",
            "/tools/tla2tools.jar",
            "tlc2.TLC",
            "-workers",
            "auto",
            "-metadir",
            f"/tmp/tlc/{config.stem}",
            "-config",
            config.name,
            module.name,
        ]
    )


def verify_tla(tla_jar: Path, *, deep: bool, runtime: TlaRuntime) -> None:
    if not tla_jar.is_file():
        raise FileNotFoundError(f"TLC JAR does not exist: {tla_jar}")
    _validate_tlc_lock()
    _verify_tla_jar(tla_jar)
    profile_name = "Deep" if deep else "Small"
    configs = sorted((VERIFICATION_ROOT / "tla").glob(f"*{profile_name}.cfg"))
    if not configs:
        raise RuntimeError("No TLA+ model configurations were found")
    use_host_java = runtime == "host" or (runtime == "auto" and _host_java_available())
    if runtime == "host" and not _host_java_available():
        raise RuntimeError("The requested host Java runtime is not available")
    for config in configs:
        module_stem = config.name.removesuffix(f"{profile_name}.cfg")
        if not module_stem:
            raise RuntimeError(f"Cannot infer a TLA+ module from {config.name}")
        module = config.with_name(f"{module_stem}.tla")
        if not module.is_file():
            raise RuntimeError(f"Missing TLA+ module for {config.name}")
        if use_host_java:
            with tempfile.TemporaryDirectory(prefix="h2hdb-tlc-") as state_directory:
                _run_tlc_with_host_java(
                    tla_jar, config, module, Path(state_directory) / config.stem
                )
        else:
            _run_tlc_with_docker(tla_jar, config, module)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "target",
        choices=("coverage", "lean", "schema", "tla", "all"),
        help="verification layer to execute",
    )
    parser.add_argument(
        "--tla-jar",
        type=Path,
        help="path to the checksum-verified tla2tools.jar",
    )
    parser.add_argument(
        "--deep",
        action="store_true",
        help="execute the larger manual/nightly TLA+ profile",
    )
    parser.add_argument(
        "--tla-runtime",
        choices=("auto", "host", "docker"),
        default="auto",
        help="run TLC with host Java or the digest-pinned Docker fallback",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="validate coverage metadata without treating production blockers as success",
    )
    return parser.parse_args()


def main() -> None:
    arguments = _arguments()
    if arguments.validate_only and arguments.target != "coverage":
        raise SystemExit("--validate-only is only valid with the coverage target")
    if arguments.target in {"coverage", "all"}:
        verify_coverage(validate_only=arguments.validate_only)
    if arguments.target in {"lean", "all"}:
        verify_lean()
    if arguments.target in {"schema", "all"}:
        verify_schema()
    if arguments.target in {"tla", "all"}:
        if arguments.tla_jar is None:
            raise SystemExit("--tla-jar is required for the TLA+ target")
        verify_tla(
            arguments.tla_jar,
            deep=arguments.deep,
            runtime=arguments.tla_runtime,
        )


if __name__ == "__main__":
    main()
