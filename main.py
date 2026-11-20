"""Run the CLI from a checkout without installing the package."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from mi_fitness_sync.cli.app import main  # noqa: E402 - imported once src is on sys.path

if __name__ == "__main__":
    raise SystemExit(main())
