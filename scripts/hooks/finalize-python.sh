#!/usr/bin/env bash
# Stop-hook: run formatters and type checker on the whole project before
# Claude finishes responding. Mirrors VS Code's on-save pipeline:
#   1. Black                 — pre-format pass; also catches syntax errors.
#   2. Ruff `--fix`          — auto-fixes safe lints (imports, pyupgrade, …).
#   3. Black                 — second pass: re-format whatever ruff rewrote
#                              so the file is guaranteed black-stable, even
#                              when ruff's UP rules produce code that black
#                              would reformat (single-pipeline convergence).
#   4. Mypy                  — type check on the final formatted code.
#
# Why a Stop hook (not PostToolUse): incremental edits routinely produce
# transient broken states (e.g. add import, then add usage in next edit).
# Running checkers between every edit would block legitimate workflows.
# At Stop time the codebase is supposed to be coherent, so a full check is
# the right gate.
#
# Error handling: any tool failing causes the script to exit 2, which makes
# Claude Code surface the captured stderr to the model on the next turn.
# stderr from a successful tool is invisible to Claude (Stop hooks only
# forward stderr on non-zero exit), so the noisy "All done! ✨ 🍰 ✨" lines
# from black don't pollute the transcript.

set -eu
trap 'exit 2' ERR

PATHS=(src/h2hdb)

uv run black "${PATHS[@]}" >&2
uv run ruff check --fix "${PATHS[@]}" >&2
uv run black "${PATHS[@]}" >&2
uv run mypy "${PATHS[@]}" >&2
