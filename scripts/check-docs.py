#!/usr/bin/env python3
"""Run the repository's generated shared development tool."""

import runpy

if __name__ == "__main__":
    runpy.run_module("_shared.h2h_dev_tools.documentation", run_name="__main__")
