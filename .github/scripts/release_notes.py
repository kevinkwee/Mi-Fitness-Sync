"""Print the changelog section for one version, so a release can use it as its notes."""

from __future__ import annotations

import re
import sys
from pathlib import Path

CHANGELOG_PATH = Path(__file__).resolve().parents[2] / "CHANGELOG.md"


def extract_section(version: str, changelog: str) -> str:
    """Return the body of the section for a version.

    Reading stops at the next version heading or at a reference-style link definition, so a
    definition placed inside a section truncates the notes.
    """
    pattern = rf"^## \[{re.escape(version)}\][^\n]*\n(.*?)(?=^## \[[^\]]*\]|^\[[^\]]+\]:|\Z)"
    match = re.search(pattern, changelog, re.MULTILINE | re.DOTALL)
    return match.group(1).strip() if match else ""


def main(argv: list[str]) -> int:
    """Print the notes for the version named by the first argument, when the changelog has them."""
    arguments = argv[1:]
    if not arguments:
        print("usage: release_notes.py <tag>", file=sys.stderr)
        return 2
    version = arguments[0].removeprefix("v")
    notes = extract_section(version, CHANGELOG_PATH.read_text(encoding="utf-8"))
    # A bare print() writes a newline, which makes the notes file non-empty and defeats the
    # workflow's fallback to generated notes.
    if notes:
        print(notes)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
