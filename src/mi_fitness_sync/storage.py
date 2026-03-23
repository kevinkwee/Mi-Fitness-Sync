from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_STATE_PATH = Path.home() / ".mi-fitness-strava-sync" / "auth.json"
STATE_PATH_ENV_VAR = "MI_FITNESS_AUTH_PATH"


@dataclass(slots=True)
class AuthState:
    email: str
    user_id: str
    c_user_id: str
    service_id: str
    pass_token: str
    service_token: str
    ssecurity: str
    psecurity: str | None
    auto_login_url: str
    device_id: str
    slh: str | None
    ph: str | None
    sts_cookie_header: str
    cookies: list[dict[str, Any]]
    created_at: str
    updated_at: str


def resolve_state_path(state_path: str | None = None) -> Path:
    if state_path:
        return Path(state_path).expanduser().resolve()
    env_value = os.environ.get(STATE_PATH_ENV_VAR)
    if env_value:
        return Path(env_value).expanduser().resolve()
    return DEFAULT_STATE_PATH


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def save_state(state: AuthState, state_path: str | None = None) -> Path:
    path = resolve_state_path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(state), indent=2, sort_keys=True), encoding="utf-8")
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
