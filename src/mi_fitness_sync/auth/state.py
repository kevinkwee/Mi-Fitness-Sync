from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


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


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()
