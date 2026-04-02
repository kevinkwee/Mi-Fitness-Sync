from __future__ import annotations

from mi_fitness_sync.activity.models import Activity, ActivityDetail, ActivitySample


def test_summary_calories_preferred_over_sample_calories():
    activity = Activity(
        activity_id="sid-1:key-1:1717200000",
        sid="sid-1",
        key="key-1",
        category="strength_training",
        sport_type=22,
        title="Strength Training",
        start_time=1717200000,
        end_time=1717203600,
        duration_seconds=3600,
        distance_meters=0,
        calories=321,
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={},
        raw_report={},
    )
    detail = ActivityDetail(
        activity=activity,
        detail_sid="sid-1",
        detail_key="fds_sport_record",
        detail_time=1717200000,
        zone_name="UTC",
        zone_offset_seconds=0,
        track_points=[],
        samples=[
            ActivitySample(
                timestamp=1717200000,
                start_time=None,
                end_time=None,
                duration_seconds=None,
                heart_rate=100,
                cadence=None,
                speed_mps=None,
                distance_meters=None,
                altitude_meters=None,
                steps=None,
                calories=150,
                raw_sample={},
            ),
            ActivitySample(
                timestamp=1717203600,
                start_time=None,
                end_time=None,
                duration_seconds=None,
                heart_rate=130,
                cadence=None,
                speed_mps=None,
                distance_meters=None,
                altitude_meters=None,
                steps=None,
                calories=372,
                raw_sample={},
            ),
        ],
        sport_report=None,
        recovery_rate=None,
        raw_fitness_item={},
        raw_detail={},
    )

    assert detail.total_calories == 321


def test_detail_still_prefers_activity_summary_calories():
    activity = Activity(
        activity_id="sid-1:key-1:1717200000",
        sid="sid-1",
        key="key-1",
        category="outdoor_run",
        sport_type=1,
        title="Run",
        start_time=1717200000,
        end_time=1717200060,
        duration_seconds=60,
        distance_meters=500,
        calories=42,
        steps=800,
        sync_state="server",
        next_key=None,
        raw_record={},
        raw_report={},
    )
    detail = ActivityDetail(
        activity=activity,
        detail_sid="sid-1",
        detail_key="key-1",
        detail_time=1717200000,
        zone_name="UTC",
        zone_offset_seconds=0,
        track_points=[],
        samples=[
            ActivitySample(
                timestamp=1717200060,
                start_time=None,
                end_time=None,
                duration_seconds=None,
                heart_rate=120,
                cadence=None,
                speed_mps=None,
                distance_meters=None,
                altitude_meters=None,
                steps=None,
                calories=99,
                raw_sample={},
            ),
        ],
        sport_report=None,
        recovery_rate=None,
        raw_fitness_item={},
        raw_detail={},
    )

    assert detail.total_calories == 42


def test_sample_distance_used_when_summary_missing():
    activity = Activity(
        activity_id="sid-1:key-1:1717200000",
        sid="sid-1",
        key="key-1",
        category="outdoor_run",
        sport_type=1,
        title="Run",
        start_time=1717200000,
        end_time=1717200060,
        duration_seconds=60,
        distance_meters=None,
        calories=None,
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={},
        raw_report={},
    )
    detail = ActivityDetail(
        activity=activity,
        detail_sid="sid-1",
        detail_key="fds_sport_record",
        detail_time=1717200000,
        zone_name="UTC",
        zone_offset_seconds=0,
        track_points=[],
        samples=[
            ActivitySample(
                timestamp=1717200060,
                start_time=None,
                end_time=None,
                duration_seconds=None,
                heart_rate=None,
                cadence=None,
                speed_mps=None,
                distance_meters=550.0,
                altitude_meters=None,
                steps=None,
                calories=None,
                raw_sample={},
            ),
        ],
        sport_report=None,
        recovery_rate=None,
        raw_fitness_item={},
        raw_detail={},
    )

    assert detail.total_distance_meters == 550.0