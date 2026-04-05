from __future__ import annotations

from pathlib import Path


APP_DIR = Path.home() / ".mi_fitness_sync"


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_auth_dir() -> Path:
    return _ensure_dir(APP_DIR / "auth")


def get_cache_dir() -> Path:
    return _ensure_dir(APP_DIR / "cache")


def get_exports_dir() -> Path:
    return _ensure_dir(APP_DIR / "exports")


def get_strava_dir() -> Path:
    return _ensure_dir(APP_DIR / "strava")
