from __future__ import annotations

import json
import os
import stat
from dataclasses import asdict, dataclass
from pathlib import Path

from mi_fitness_sync.paths import get_strava_dir


@dataclass(slots=True)
class StravaTokenState:
    client_id: str
    client_secret: str
    access_token: str
    refresh_token: str
    expires_at: int
    athlete_id: int | None
    created_at: str
    updated_at: str


DEFAULT_TOKEN_PATH = get_strava_dir() / "tokens.json"


def resolve_token_path(token_path: str | None = None) -> Path:
    if token_path:
        return Path(token_path).expanduser().resolve()
    return DEFAULT_TOKEN_PATH


def save_tokens(state: StravaTokenState, token_path: str | None = None) -> Path:
    path = resolve_token_path(token_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(state), indent=2, sort_keys=True), encoding="utf-8")
    try:
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass  # Best-effort; Windows ACLs don't support POSIX modes
    return path


def load_tokens(token_path: str | None = None) -> StravaTokenState | None:
    path = resolve_token_path(token_path)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return StravaTokenState(**payload)


def delete_tokens(token_path: str | None = None) -> Path:
    path = resolve_token_path(token_path)
    if path.exists():
        path.unlink()
    return path
