#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

existing_hooks_path="$(git config --get core.hooksPath || true)"
if [[ -n "$existing_hooks_path" && "$existing_hooks_path" != ".githooks" ]]; then
    echo "Refusing to replace existing core.hooksPath: $existing_hooks_path" >&2
    exit 1
fi

if [[ -z "$existing_hooks_path" ]]; then
    default_hooks_directory="$(git rev-parse --git-path hooks)"
    shopt -s nullglob
    for existing_hook in "$default_hooks_directory"/*; do
        if [[ -f "$existing_hook" \
            && -x "$existing_hook" \
            && "$existing_hook" != *.sample ]]; then
            echo "Refusing to disable existing Git hook: $existing_hook" >&2
            exit 1
        fi
    done
fi

git config --local core.hooksPath .githooks
echo "Installed h2hdb Git hooks from .githooks for this clone."
