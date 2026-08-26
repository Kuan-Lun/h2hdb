#!/usr/bin/env bash

set -euo pipefail

repository_root="$(git rev-parse --show-toplevel)"
cd "$repository_root"

configured_primary="$(git config --local --get workflow.primaryBranch || true)"
if [[ -n "$configured_primary" ]]; then
    git show-ref --verify --quiet "refs/heads/$configured_primary" || {
        printf 'Configured primary branch does not exist: %s\n' \
            "$configured_primary" >&2
        exit 1
    }
    printf '%s\n' "$configured_primary"
    exit 0
fi

remote_head="$(
    git symbolic-ref --quiet --short refs/remotes/origin/HEAD 2>/dev/null \
        || true
)"
if [[ -n "$remote_head" ]]; then
    primary="${remote_head#origin/}"
    if git show-ref --verify --quiet "refs/heads/$primary"; then
        printf '%s\n' "$primary"
        exit 0
    fi
fi

candidates=()
for candidate in main master; do
    if git show-ref --verify --quiet "refs/heads/$candidate"; then
        candidates+=("$candidate")
    fi
done

if (( ${#candidates[@]} == 1 )); then
    printf '%s\n' "${candidates[0]}"
    exit 0
fi

if (( ${#candidates[@]} > 1 )); then
    printf '%s\n' \
        'Primary branch is ambiguous; set git config --local workflow.primaryBranch.' \
        >&2
else
    printf '%s\n' \
        'Cannot detect primary branch; set git config --local workflow.primaryBranch.' \
        >&2
fi
exit 1
