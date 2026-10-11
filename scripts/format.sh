#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repository_root"

.venv/bin/ruff check --fix --extend-exclude scripts/_shared .
.venv/bin/ruff format --extend-exclude scripts/_shared .
node_modules/.bin/markdownlint-cli2 --fix
