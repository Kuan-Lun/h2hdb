#!/usr/bin/env bash
# Build one disposable integration venv from independent editable installs.
# This intentionally does not use a uv workspace, uv sync, or a lock file.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_REPO="$(cd "$SCRIPT_DIR/.." && pwd)"
REPOS_ROOT="$(cd "$CORE_REPO/.." && pwd)"
VENV_DIR="$CORE_REPO/.integration-venv"
PYTHON="$VENV_DIR/bin/python"

REPOSITORIES=(
    "$CORE_REPO"
    "$REPOS_ROOT/h2h-galleryinfo-parser.clone"
    "$REPOS_ROOT/hbrowser.clone"
    "$REPOS_ROOT/h2hdb-downloader.clone"
    "$REPOS_ROOT/h2hdb-ingest.clone"
    "$REPOS_ROOT/h2hdb-komga.clone"
    "$REPOS_ROOT/h2hdb-opds.clone"
)

for repository in "${REPOSITORIES[@]}"; do
    if [[ ! -f "$repository/pyproject.toml" ]]; then
        echo "Missing integration repository: $repository" >&2
        exit 1
    fi
done

uv venv --clear --python 3.14 "$VENV_DIR"

EDITABLES=()
for repository in "${REPOSITORIES[@]}"; do
    EDITABLES+=(--editable "$repository")
done
uv pip install --python "$PYTHON" "${EDITABLES[@]}"

"$PYTHON" "$CORE_REPO/scripts/smoke-multirepo.py"
