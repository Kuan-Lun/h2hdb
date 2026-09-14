#!/usr/bin/env python3
"""Offline, one-use conversion of a narrowly supported schema admin shell script."""

from __future__ import annotations

import argparse
import re
import shlex
from collections.abc import Sequence
from pathlib import Path

_PYTHON = re.compile(r"(?:[A-Za-z0-9_./-]+/)?python(?:3(?:\.[0-9]+)?)?\Z")
_PREAMBLES = frozenset({"set -e", "set -eu", "set -euo pipefail"})


def convert_script(source: str) -> str:
    """Convert exactly one literal, standalone migrate call; reject other flow.

    No input is executed or expanded. The sole command becomes an AND list,
    preserving its failure exit status and running check only after success.
    """

    converted: list[str] = []
    commands = 0
    for number, line in enumerate(source.splitlines(keepends=True), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            converted.append(line)
            continue
        if stripped in _PREAMBLES and commands == 0:
            converted.append(line)
            continue
        if any(character in stripped for character in "$`~*?[\\"):
            raise ValueError(f"line {number}: shell expansions/escapes are unsupported")
        lexer = shlex.shlex(stripped, posix=True, punctuation_chars=";&|<>()")
        lexer.whitespace_split = True
        lexer.commenters = ""
        try:
            tokens = list(lexer)
        except ValueError as error:
            raise ValueError(f"line {number}: invalid shell quoting") from error
        if (
            len(tokens) != 6
            or _PYTHON.fullmatch(tokens[0]) is None
            or tokens[1:5] != ["-m", "h2hdb", "migrate", "--config"]
            or not tokens[5]
            or tokens[5].startswith("-")
        ):
            raise ValueError(
                f"line {number}: expected a standalone literal "
                "python -m h2hdb migrate --config PATH command"
            )
        commands += 1
        if commands > 1:
            raise ValueError("exactly one migrate command is supported")
        checked = tokens.copy()
        checked[3] = "check"
        indent = line[: len(line) - len(line.lstrip())]
        newline = "\r\n" if line.endswith("\r\n") else "\n"
        converted.append(
            f"{indent}{shlex.join(tokens)} && {shlex.join(checked)}{newline}"
        )
    if commands != 1:
        raise ValueError("exactly one migrate command is required")
    return "".join(converted)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument(
        "--output", type=Path, required=True, help="new file; never overwritten"
    )
    args = parser.parse_args(argv)
    try:
        converted = convert_script(args.input.read_text(encoding="utf-8"))
        # Exclusive creation rejects existing paths, symlinks and in-place edits.
        with args.output.open("x", encoding="utf-8", newline="") as output:
            output.write(converted)
    except (OSError, UnicodeError, ValueError) as error:
        parser.error(str(error))
    print(f"Created {args.output}; review before use. The input was not changed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
