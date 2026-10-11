"""Atomic JSON progress reports for checkout-only performance probes."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


def write_report(output: Path, report: dict[str, Any]) -> None:
    """Replace complete JSON atomically; interruption keeps the previous report."""
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=output.parent,
            prefix=".report-",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            json.dump(report, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
