#!/usr/bin/env bash
# Stop-hook: clean up markdown files after Claude finishes responding.
# Two passes:
#   1. pymarkdown fix    — best-effort autofix for mechanical issues like
#                          missing blank lines around lists. Many lints
#                          (e.g. MD013 line length on Chinese paragraphs)
#                          are *not* auto-fixable; pymarkdown silently
#                          leaves them alone, so this step never blocks.
#   2. ruff format       — preview mode formats Python code blocks embedded
#       --preview          inside fenced ```python sections. This is the
#                          markdown counterpart to running black on .py
#                          files: it ensures example code in docs follows
#                          the same style as the rest of the codebase.
#                          A non-zero exit here means a code block has a
#                          parse error — that's a real signal worth surfacing.
#
# Why a Stop hook (not PostToolUse): markdown is rarely the focus of an
# edit, and chaining a fixer onto every file write would slow Claude down
# without much benefit. The Stop time check is enough.
#
# Exit codes:
#   0 — everything passed (or there were lints that aren't auto-fixable)
#   2 — ruff format hit a parse error in a Python code block

set -uo pipefail

# Collect markdown files at project root and one level deep, excluding
# noise from venvs, caches, and dot-directories.
MD_FILES=()
while IFS= read -r f; do
    MD_FILES+=("$f")
done < <(
    find . -maxdepth 2 -type f -name "*.md" \
        -not -path "./.venv/*" \
        -not -path "./node_modules/*" \
        -not -path "./.pytest_cache/*" \
        -not -path "./.*" \
        | sort
)

if [ ${#MD_FILES[@]} -eq 0 ]; then
    exit 0
fi

# Pass 1: pymarkdown fix. Always non-blocking; tool-level errors are
# unlikely and if they happen they'd cascade into the ruff pass too.
uv run pymarkdown fix "${MD_FILES[@]}" >/dev/null 2>&1 || true

# Pass 2: ruff format --preview. Surface parse errors via exit 2.
if ! uv run ruff format --preview "${MD_FILES[@]}" >&2; then
    exit 2
fi
