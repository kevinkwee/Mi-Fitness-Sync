"""Unified application directory layout.

All persistent data lives under a single root folder::

    ~/.mi_fitness_sync/
    ├── auth/          # auth tokens, session data
    ├── cache/
    │   └── fds/       # FDS binary cache
    └── exports/       # default export output path
"""

from __future__ import annotations

from pathlib import Path

APP_DIR = Path.home() / ".mi_fitness_sync"


def get_auth_dir() -> Path:
    path = APP_DIR / "auth"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_cache_dir() -> Path:
    path = APP_DIR / "cache"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_exports_dir() -> Path:
    path = APP_DIR / "exports"
    path.mkdir(parents=True, exist_ok=True)
    return path
