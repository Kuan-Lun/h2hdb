from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "convert-schema-admin.py"


def _load_converter() -> ModuleType:
    specification = importlib.util.spec_from_file_location(
        "h2hdb_convert_schema_admin", _SCRIPT
    )
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


converter = _load_converter()


def test_converter_preserves_literal_arguments_and_comments() -> None:
    source = (
        "#!/bin/sh\n# Explicit full-audit admin job.\nset -eu\n\n"
        "python3.14 -m h2hdb migrate --config 'config dir/database.json'\n"
    )
    converted = converter.convert_script(source)
    assert converted.startswith(
        "#!/bin/sh\n# Explicit full-audit admin job.\nset -eu\n\n"
    )
    assert converted.endswith(
        "python3.14 -m h2hdb migrate --config 'config dir/database.json' && "
        "python3.14 -m h2hdb check --config 'config dir/database.json'\n"
    )


@pytest.mark.parametrize(
    "source",
    [
        "# no command\n",
        "python -m h2hdb check --config config.json\n",
        "exec python -m h2hdb migrate --config config.json\n",
        "python -m h2hdb migrate --config $CONFIG\n",
        "python -m h2hdb migrate --config '$(touch sentinel)'\n",
        "python -m h2hdb migrate --config `pwd`\n",
        "python -m h2hdb migrate --config config.json; true\n",
        "python -m h2hdb migrate --config config.json | tee log\n",
        "python -m h2hdb migrate --config config.json > log\n",
        "python -m h2hdb migrate --config config*.json\n",
        "if true; then python -m h2hdb migrate --config config.json; fi\n",
        "python -m h2hdb migrate --config config.json\n" * 2,
        "python -m h2hdb migrate --config 'unfinished\n",
        "python -m h2hdb migrate --config config.json\nset -e\n",
    ],
)
def test_converter_rejects_unknown_shell_flow(source: str) -> None:
    with pytest.raises(ValueError):
        converter.convert_script(source)


def test_converter_creates_only_new_output_and_preserves_input(tmp_path: Path) -> None:
    source = tmp_path / "old.sh"
    source.write_text("python -m h2hdb migrate --config config.json\n")
    original = source.read_bytes()
    target = tmp_path / "new.sh"

    assert converter.main(["--input", str(source), "--output", str(target)]) == 0
    assert " && python -m h2hdb check " in target.read_text()
    assert source.read_bytes() == original
    for existing in (source, target):
        before = existing.read_bytes()
        with pytest.raises(SystemExit) as failure:
            converter.main(["--input", str(source), "--output", str(existing)])
        assert failure.value.code == 2
        assert existing.read_bytes() == before


@pytest.mark.parametrize("migrate_status", [0, 7])
@pytest.mark.skipif(
    sys.platform == "win32", reason="shell AND-list contract needs POSIX sh"
)
def test_converted_command_preserves_failure_and_checks_only_after_success(
    tmp_path: Path, migrate_status: int
) -> None:
    # This synthetic interpreter records arguments; it never opens any database.
    interpreter = tmp_path / "python3"
    trace = tmp_path / "trace.txt"
    interpreter.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$3\" >> '{trace}'\n"
        f'if [ "$3" = migrate ]; then exit {migrate_status}; fi\n'
        "exit 3\n"
    )
    interpreter.chmod(0o700)
    source = f"{interpreter} -m h2hdb migrate --config config.json\n"
    converted = converter.convert_script(source)
    completed = subprocess.run(
        ["/bin/sh", "-c", converted], capture_output=True, text=True, check=False
    )
    assert completed.returncode == (3 if migrate_status == 0 else migrate_status)
    assert trace.read_text().splitlines() == (
        ["migrate", "check"] if migrate_status == 0 else ["migrate"]
    )
