from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from mi_fitness_sync.activity.models import Activity, ActivityDetail, ActivitySample, TrackPoint
from mi_fitness_sync.auth.state import AuthState


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


@pytest.fixture
def sample_activity_detail() -> ActivityDetail:
    activity = Activity(
        activity_id="sid:key:1",
        sid="sid",
        key="key",
        category="outdoor_run",
        sport_type=1,
        title="Morning Run",
        start_time=1717200000,
        end_time=1717203600,
        duration_seconds=3600,
        distance_meters=10000,
        calories=700,
        steps=12000,
        sync_state="server",
        next_key=None,
        raw_record={"sid": "sid", "key": "key"},
        raw_report={"name": "Morning Run"},
    )
    return ActivityDetail(
        activity=activity,
        detail_sid="sid",
        detail_key="key",
        detail_time=1717200000,
        zone_name="UTC",
        zone_offset_seconds=0,
        track_points=[
            TrackPoint(
                timestamp=1717200000,
                latitude=1.0,
                longitude=2.0,
                altitude_meters=10.0,
                speed_mps=2.5,
                distance_meters=0.0,
                heart_rate=120,
                cadence=160,
                raw_point={},
            )
        ],
        samples=[
            ActivitySample(
                timestamp=1717200000,
                start_time=1717200000,
                end_time=1717200000,
                duration_seconds=0,
                heart_rate=120,
                cadence=160,
                speed_mps=2.5,
                distance_meters=0.0,
                altitude_meters=10.0,
                steps=100,
                calories=10,
                raw_sample={},
            )
        ],
        sport_report=None,
        recovery_rate=None,
        raw_fitness_item={},
        raw_detail={},
    )
