#!/usr/bin/env bash
# Nuke .venv + caches and recreate from scratch with full dev dependencies.
#
# Use when the venv is corrupted — e.g. after a Python version upgrade you
# see errors like:
#   ModuleNotFoundError: No module named '30fcd23745efe32ce681__mypyc'
# This is black/mypy's mypyc-compiled extension failing to locate its
# internal module, and the fix is a clean reinstall.
set -euo pipefail
cd "$(dirname "$0")/.."

rm -rf .venv uv.lock .mypy_cache .ruff_cache .pytest_cache
find . -type d -name __pycache__ -exec rm -rf {} +
uv cache clean --force
uv venv --python 3.14
uv pip install -e ".[dev]"
