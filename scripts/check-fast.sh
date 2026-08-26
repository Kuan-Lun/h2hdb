#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repository_root"

[[ -x .venv/bin/ruff && -x .venv/bin/mypy ]] || {
    printf 'check-fast: run scripts/rebuild-env.sh first\n' >&2
    exit 1
}
[[ -x node_modules/.bin/markdownlint-cli2 ]] || {
    printf 'check-fast: run scripts/rebuild-env.sh first\n' >&2
    exit 1
}

.venv/bin/ruff check --no-cache .
.venv/bin/ruff format --no-cache --check .
.venv/bin/mypy --no-incremental --cache-dir=/dev/null
node_modules/.bin/markdownlint-cli2
