#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repository_root"

scripts/check-fast.sh
.venv/bin/python scripts/verify-formal.py coverage --validate-only
.venv/bin/python scripts/verify-formal.py schema
.venv/bin/python scripts/verify-schema-surface.py
.venv/bin/python scripts/verify-formal.py lean

H2HDB_TEST_MARIADB=1 .venv/bin/pytest

.venv/bin/python scripts/fetch-formal-tools.py
.venv/bin/python scripts/verify-formal.py tla \
    --tla-jar .formal-tools/tla2tools-1.7.4.jar

distribution_root="$(mktemp -d "${TMPDIR:-/tmp}/h2hdb-distributions.XXXXXX")"
trap 'rm -rf "$distribution_root"' EXIT
.venv/bin/python scripts/build-and-verify-distributions.py \
    --output-directory "$distribution_root/dist"
