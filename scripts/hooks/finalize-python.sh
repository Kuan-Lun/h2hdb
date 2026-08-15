#!/usr/bin/env bash
# Fast Python finalizer for humans and coding agents.
set -eu
trap 'exit 2' ERR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

PY_FILES=()
while IFS= read -r -d '' file; do
    if [[ -f "$file" ]]; then
        PY_FILES+=("$file")
    fi
done < <(
    git diff --name-only --diff-filter=ACMR -z HEAD -- '*.py' '*.pyi'
)
while IFS= read -r -d '' file; do
    if [[ -f "$file" ]]; then
        PY_FILES+=("$file")
    fi
done < <(git ls-files --others --exclude-standard -z -- '*.py' '*.pyi')

if [[ ${#PY_FILES[@]} -eq 0 ]]; then
    exit 0
fi

uv run --no-sync black --check "${PY_FILES[@]}" >&2
uv run --no-sync ruff check "${PY_FILES[@]}" >&2
