from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from mi_fitness_sync.storage import AuthState


@pytest.fixture
def auth_state() -> AuthState:
    return AuthState(
        email="user@example.com",
        user_id="123456",
        c_user_id="c-user-123",
        service_id="miothealth",
        pass_token="pass-token",
        service_token="service-token",
        ssecurity="MDEyMzQ1Njc4OWFiY2RlZg==",
        psecurity=None,
        auto_login_url="https://example.com/sts",
        device_id="DEVICE123",
        slh=None,
        ph=None,
        sts_cookie_header="serviceToken=service-token; cUserId=c-user-123",
        cookies=[
            {"name": "uLocale", "value": "en_US"},
            {"name": "serviceToken", "value": "service-token"},
        ],
        created_at="2026-03-24T00:00:00+00:00",
        updated_at="2026-03-24T00:00:00+00:00",
    )
