#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repository_root"

run_timed_stage() {
    local stage_name="$1"
    shift
    local started_at="$SECONDS"
    local status

    printf '\n==> %s\n' "$stage_name"
    if "$@"; then
        status=0
    else
        status=$?
    fi
    printf '<== %s: %ss (exit %s)\n' \
        "$stage_name" "$((SECONDS - started_at))" "$status"
    return "$status"
}

run_timed_stage "fast checks" scripts/check-fast.sh
run_timed_stage \
    "formal coverage gate" \
    .venv/bin/python scripts/verify-formal.py coverage
run_timed_stage \
    "formal schema" \
    .venv/bin/python scripts/verify-formal.py schema
run_timed_stage \
    "schema surface" \
    .venv/bin/python scripts/verify-schema-surface.py
run_timed_stage \
    "Lean" \
    .venv/bin/python scripts/verify-formal.py lean

run_timed_stage \
    "pytest (SQLite and MariaDB, auto workers)" \
    env H2HDB_TEST_MARIADB=1 PYTHONDONTWRITEBYTECODE=1 \
    .venv/bin/pytest \
    --numprocesses=auto \
    --dist=loadgroup \
    --max-worker-restart=0 \
    --durations=50

run_timed_stage \
    "formal tool availability" \
    .venv/bin/python scripts/fetch-formal-tools.py
run_timed_stage \
    "TLA+ small profiles" \
    .venv/bin/python scripts/verify-formal.py tla \
    --tla-jar .formal-tools/tla2tools-1.7.4.jar

distribution_root="$(mktemp -d "${TMPDIR:-/tmp}/h2hdb-distributions.XXXXXX")"
trap 'rm -rf "$distribution_root"' EXIT
run_timed_stage \
    "distribution boundary" \
    .venv/bin/python scripts/build-and-verify-distributions.py \
    --output-directory "$distribution_root/dist"
