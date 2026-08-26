#!/usr/bin/env bash
# Recreate this repository's independent editable-install environment.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

command -v uv >/dev/null || {
    printf 'rebuild-env: uv is required\n' >&2
    exit 1
}
command -v npm >/dev/null || {
    printf 'rebuild-env: npm is required\n' >&2
    exit 1
}

# Resolve from pyproject.toml and package.json; neither lockfile is an input.
uv venv --clear --python 3.14 .venv
uv pip install --refresh --python .venv/bin/python \
    --upgrade --reinstall -e ".[dev]"
npm install --package-lock=false
