#!/usr/bin/env bash

set -euo pipefail

fail() {
    printf 'git-flow-merge: %s\n' "$1" >&2
    exit 1
}

assert_clean() {
    local worktree=$1

    [[ -z "$(git -C "$worktree" status --porcelain=v1 --untracked-files=all)" ]] \
        || fail "worktree is not clean: $worktree"
}

assert_no_operation() {
    local worktree=$1 git_path

    for name in MERGE_HEAD CHERRY_PICK_HEAD REVERT_HEAD REBASE_HEAD; do
        git_path="$(git -C "$worktree" rev-parse --path-format=absolute --git-path "$name")"
        [[ ! -e "$git_path" ]] || fail "Git operation is active: $name"
    done
}

abort_merge_if_needed() {
    local worktree=$1 merge_head

    merge_head="$(git -C "$worktree" rev-parse --path-format=absolute --git-path MERGE_HEAD)"
    if [[ -e "$merge_head" ]]; then
        git -C "$worktree" merge --abort || {
            printf 'git-flow-merge: could not abort merge in %s\n' "$worktree" >&2
            return 1
        }
    fi
}

restore_after_failure() {
    local status=$?

    trap - EXIT HUP INT TERM
    if (( status != 0 )); then
        if [[ -n "$reviewer_pid" ]]; then
            # wait is interruptible; stop review before discarding its candidate.
            kill -TERM "$reviewer_pid" 2>/dev/null || true
            wait "$reviewer_pid" || true
        fi
        if abort_merge_if_needed "$merge_worktree"; then
            # A signal or cleanup failure after the commit must not undo success.
            if [[ "$merge_worktree" == "$repository_root" ]] \
                && [[ "$(git -C "$merge_worktree" rev-parse HEAD)" == "$primary_head" ]]; then
                git -C "$repository_root" switch "$task_branch" || {
                    printf 'git-flow-merge: task branch retained; could not switch to %s\n' \
                        "$task_branch" >&2
                }
            fi
        fi
    fi
    exit "$status"
}

review_candidate() {
    cd "$merge_worktree" || fail "cannot enter merge worktree: $merge_worktree"
    [[ -f scripts/review-code.py ]] || fail 'scripts/review-code.py is missing'
    if [[ -x .venv/bin/python ]]; then
        exec .venv/bin/python scripts/review-code.py run --index
    fi
    if [[ -x .venv/Scripts/python.exe ]]; then
        exec .venv/Scripts/python.exe scripts/review-code.py run --index
    fi
    fail 'create the local development environment with scripts/rebuild-env.sh'
}

repository_root="$(git rev-parse --show-toplevel)"
cd "$repository_root"

primary="$(scripts/detect-primary-branch.sh)"
task_branch="$(git branch --show-current)"
[[ -n "$task_branch" ]] || fail 'detached HEAD is not supported'
[[ "$task_branch" != "$primary" ]] \
    || fail 'run this script from the task branch'

assert_clean "$repository_root"
assert_no_operation "$repository_root"

git merge-base "$primary" "$task_branch" >/dev/null \
    || fail "task branch and primary have no common ancestor: $primary"
[[ "$(git rev-list --count "$primary..$task_branch")" -gt 0 ]] \
    || fail 'task branch has no commits to merge'

hooks_path="$(git config --local --get core.hooksPath || true)"
[[ "$hooks_path" == '.githooks' ]] \
    || fail 'repository hooks are not installed; run scripts/install-git-hooks.sh'
[[ -x .githooks/pre-merge-commit ]] \
    || fail '.githooks/pre-merge-commit is missing or not executable'
[[ -x .githooks/pre-commit ]] \
    || fail '.githooks/pre-commit is missing or not executable'

primary_worktree=''
candidate_worktree=''
candidate_branch=''
while IFS= read -r line || [[ -n "$line" ]]; do
    case "$line" in
        'worktree '*)
            candidate_worktree="${line#worktree }"
            candidate_branch=''
            ;;
        'branch refs/heads/'*)
            candidate_branch="${line#branch refs/heads/}"
            if [[ "$candidate_branch" == "$primary" ]]; then
                primary_worktree="$candidate_worktree"
            fi
            ;;
    esac
done < <(git worktree list --porcelain)

if [[ -n "$primary_worktree" ]]; then
    assert_clean "$primary_worktree"
    assert_no_operation "$primary_worktree"
    merge_worktree="$primary_worktree"
else
    merge_worktree="$repository_root"
fi

primary_head="$(git rev-parse "$primary")"
reviewer_pid=''
trap restore_after_failure EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ -z "$primary_worktree" ]]; then
    git switch "$primary"
fi

export WORKFLOW_MERGE_TASK_REF="$task_branch"
if ! git -C "$merge_worktree" merge --no-ff --no-commit "$task_branch"; then
    fail 'merge failed; task branch was retained'
fi
review_candidate &
reviewer_pid=$!
if ! wait "$reviewer_pid"; then
    reviewer_pid=''
    fail 'code review failed; task branch was retained'
fi
reviewer_pid=''
# The existing pre-commit hook delegates pending merges to the same release gate
# as pre-merge-commit. Review remains outside both offline hooks.
if ! git -C "$merge_worktree" commit --no-edit; then
    fail 'merge commit or release gate failed; task branch was retained'
fi

cd "$merge_worktree"
if [[ "$repository_root" != "$merge_worktree" ]]; then
    assert_clean "$repository_root"
    git -C "$merge_worktree" worktree remove "$repository_root" \
        || fail "merge succeeded, but task worktree remains: $repository_root"
fi

git -C "$merge_worktree" branch -d "$task_branch" \
    || fail "merge succeeded, but task branch remains: $task_branch"

merge_commit="$(git -C "$merge_worktree" rev-parse HEAD)"
printf 'Merged %s into %s at %s\n' \
    "$task_branch" "$primary" "$merge_commit"
