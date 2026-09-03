#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repository_root"

[[ -x .venv/bin/python ]] || {
    printf 'check-pytest-deep: run scripts/rebuild-env.sh first\n' >&2
    exit 1
}

printf '%s\n' \
    'Running the complete SQLite and MariaDB pytest profiles.' \
    'This manual deep check is intentionally outside the merge/release gate.'
exec .venv/bin/python scripts/run-pytest.py deep
