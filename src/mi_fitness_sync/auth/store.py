from __future__ import annotations

import json
import stat
from dataclasses import asdict
from pathlib import Path

from mi_fitness_sync.auth.state import AuthState
from mi_fitness_sync.paths import get_auth_dir


DEFAULT_STATE_PATH = get_auth_dir() / "auth.json"


def resolve_state_path(state_path: str | None = None) -> Path:
    if state_path:
        return Path(state_path).expanduser().resolve()
    return DEFAULT_STATE_PATH


def save_state(state: AuthState, state_path: str | None = None) -> Path:
    path = resolve_state_path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(state), indent=2, sort_keys=True), encoding="utf-8")
    try:
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass  # Best-effort; Windows ACLs don't support POSIX modes
    return path


def load_state(state_path: str | None = None) -> AuthState | None:
    path = resolve_state_path(state_path)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return AuthState(**payload)


def delete_state(state_path: str | None = None) -> Path:
    path = resolve_state_path(state_path)
    if path.exists():
        path.unlink()
    return path
