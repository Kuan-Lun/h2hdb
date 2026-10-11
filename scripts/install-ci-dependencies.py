#!/usr/bin/env python3
"""Install CI tools using this repository's dependency declarations."""

from pathlib import Path

from _shared.h2h_dev_tools.ci_dependencies import main

if __name__ == "__main__":
    main(repository=Path(__file__).resolve().parents[1])
