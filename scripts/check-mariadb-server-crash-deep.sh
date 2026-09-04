#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repository_root"

[[ -x .venv/bin/python ]] || {
    printf 'check-mariadb-server-crash-deep: run scripts/rebuild-env.sh first\n' >&2
    exit 1
}

printf '%s\n' \
    'Running isolated MariaDB server SIGKILL recovery evidence.' \
    'Docker and its host remain alive; this is not a host power-loss test.'
exec .venv/bin/python scripts/run-pytest.py \
    mariadb-server-crash --budget-seconds 300
