"""Test mount entry point; Python must not silently ignore a broken probe."""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path


def _install() -> None:
    if (
        "H2HDB_ACCEPTANCE_PROBE_DIR" not in os.environ
        and "H2HDB_ACCEPTANCE_CONTROL_DIR" not in os.environ
    ):
        return
    path = Path(__file__).with_name("probe.py")
    spec = importlib.util.spec_from_file_location("_h2hdb_acceptance_probe", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("acceptance probe module is unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    module.install()


try:
    _install()
except Exception as error:
    # site.py otherwise prints an Exception and continues with an unobserved app.
    raise SystemExit("H2HDB_ACCEPTANCE_PROBE_INSTALL_FAILED") from error
