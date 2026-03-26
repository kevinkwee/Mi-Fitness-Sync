from __future__ import annotations

import json

import pytest

from mi_fitness_sync.activities import (
    ACTIVITY_LIST_ENDPOINT,
    Activity,
    MiFitnessActivitiesClient,
    _build_fds_suffix,
    parse_activity_id,
    parse_cli_time,
    render_activities_table,
)
from mi_fitness_sync.exceptions import MiFitnessError
from mi_fitness_sync.region_mapping import region_for_country_code


def test_parse_cli_time_accepts_unix_seconds():
    assert parse_cli_time("1717200000") == 1717200000


def test_parse_cli_time_accepts_iso8601_utc():
    assert parse_cli_time("2024-01-01T00:00:00Z") == 1704067200


def test_collect_cookie_values_fills_locale_and_user_id(auth_state):
    client = MiFitnessActivitiesClient(auth_state)

    assert client._cookie_values["locale"] == "en_US"
    assert client._cookie_values["userId"] == auth_state.user_id


def test_region_for_country_code_maps_id_to_sg():
    assert region_for_country_code("ID") == "sg"


def test_region_for_country_code_rejects_unknown_country_code():
    with pytest.raises(MiFitnessError, match="Unsupported Mi Fitness country override: ZZ."):
        region_for_country_code("ZZ")


def test_get_activity_list_endpoint_uses_country_override(auth_state):
    client = MiFitnessActivitiesClient(auth_state, country_code="ID")

    assert client._get_activity_list_endpoint() == ACTIVITY_LIST_ENDPOINT.replace("://", "://sg.", 1)


def test_parse_activity_builds_expected_fields(auth_state):
    client = MiFitnessActivitiesClient(auth_state)
    record = {
        "sid": "sid-1",
        "key": "key-1",
        "time": 1717200000,
        "category": "outdoor_run",
        "value": json.dumps(
            {
                "sport_type": 1,
                "start_time": 1717200000,
                "end_time": 1717203600,
                "duration": 3600,
                "distance": 10000,
                "calories": 700,
                "steps": 12000,
                "name": "Morning Run",
            }
        ),
    }

    activity = client._parse_activity(record, "next-token")

    assert activity.activity_id == "sid-1:key-1:1717200000"
    assert activity.title == "Morning Run"
    assert activity.category == "outdoor_run"
    assert activity.sync_state == "server"
    assert activity.distance_meters == 10000
    assert activity.next_key == "next-token"


def test_render_activities_table_handles_empty_list():
    assert render_activities_table([]) == "No activities matched the requested time window."


def test_parse_activity_id_round_trips_list_format():
    assert parse_activity_id("sid-1:key-1:1717200000") == ("sid-1", "key-1", 1717200000)


def test_get_activity_detail_normalizes_track_points_and_samples(auth_state, monkeypatch):
    client = MiFitnessActivitiesClient(auth_state)
    activity = Activity(
        activity_id="sid-1:key-1:1717200000",
        sid="sid-1",
        key="key-1",
        category="outdoor_run",
        sport_type=1,
        title="Morning Run",
        start_time=1717200000,
        end_time=1717200060,
        duration_seconds=60,
        distance_meters=500.0,
        calories=42,
        steps=800,
        sync_state="server",
        next_key=None,
        raw_record={"sid": "sid-1", "key": "key-1"},
        raw_report={"name": "Morning Run"},
    )

    fitness_item = {
        "sid": "sid-1",
        "key": "key-1",
        "time": 1717200000,
        "zone_name": "UTC",
        "zone_offset": 0,
        "value": json.dumps(
            {
                "gps_records": [
                    {"time": 1717200000, "latitude": 1.1, "longitude": 2.2, "altitude": 10.0},
                    {"time": 1717200060, "latitude": 1.2, "longitude": 2.3, "altitude": 12.0},
                ],
                "sport_records": [
                    {
                        "startTime": 1717200000,
                        "endTime": 1717200000,
                        "hr": 120,
                        "distance": 0,
                        "speed": 2.0,
                        "cadence": 160,
                    },
                    {
                        "startTime": 1717200060,
                        "endTime": 1717200060,
                        "hr": 125,
                        "distance": 500,
                        "speed": 3.2,
                        "cadence": 165,
                        "calories": 42,
                    },
                ],
            }
        ),
    }

    monkeypatch.setattr(client, "_get_activity_detail_item", lambda selected_activity: fitness_item)

    detail = client.get_activity_detail(activity)

    assert detail.detail_key == "key-1"
    assert detail.zone_name == "UTC"
    assert len(detail.track_points) == 2
    assert len(detail.samples) == 2
    assert detail.track_points[-1].distance_meters == 500.0
    assert detail.track_points[-1].heart_rate == 125
    assert detail.total_distance_meters == 500.0
    assert detail.total_calories == 42


def test_build_fds_suffix_matches_android_server_key_format():
    assert _build_fds_suffix(
        sid="882963223",
        timestamp=1774263950,
        timezone_offset=28,
        sport_type=22,
        file_type=0,
    ) == "jh7BaRzY_KdJjWE97g_CXNigsrsLabYEzCPg"


def test_build_fds_suffix_strength_training_proto_type_28():
    """Regression: FDS suffix for proto_type=28 (strength training) uses correct genDataTypeByte."""
    assert _build_fds_suffix(
        sid="882963223",
        timestamp=1774241243,
        timezone_offset=28,
        sport_type=28,
        file_type=0,
    ) == "28XAaRzw_KdJjWE97g_CXNigsrsLabYEzCPg"


def test_get_activity_detail_falls_back_to_timeline_samples(auth_state, monkeypatch):
    client = MiFitnessActivitiesClient(auth_state)
    activity = Activity(
        activity_id="sid-1:key-1:1717200000",
        sid="sid-1",
        key="key-1",
        category="outdoor_run",
        sport_type=1,
        title="Morning Run",
        start_time=1717200000,
        end_time=1717200060,
        duration_seconds=60,
        distance_meters=500,
        calories=42,
        steps=800,
        sync_state="server",
        next_key=None,
        raw_record={"sid": "sid-1", "key": "key-1", "time": 1717200000},
        raw_report={"name": "Morning Run"},
    )

    monkeypatch.setattr(client, "_try_get_fds_download_map", lambda selected_activity: {"key": {"url": "https://example.com"}})
    monkeypatch.setattr(client, "_get_activity_detail_item", lambda selected_activity: {})
    monkeypatch.setattr(
        client,
        "_get_activity_timeline_items",
        lambda selected_activity: [
            {"sid": "sid-1", "time": 1717200000, "key": "heart_rate", "value": 120, "zone_name": "UTC", "zone_offset": 0},
            {"sid": "sid-1", "time": 1717200000, "key": "steps", "value": 100, "zone_name": "UTC", "zone_offset": 0},
            {"sid": "sid-1", "time": 1717200060, "key": "heart_rate", "value": 125, "zone_name": "UTC", "zone_offset": 0},
            {"sid": "sid-1", "time": 1717200060, "key": "calories", "value": 42, "zone_name": "UTC", "zone_offset": 0},
        ],
    )

    detail = client.get_activity_detail(activity)

    assert detail.detail_key == "fitness_data_timeline"
    assert detail.zone_name == "UTC"
    assert len(detail.track_points) == 0
    assert [sample.heart_rate for sample in detail.samples] == [120, 125]
    assert detail.raw_detail["fds_downloads"]["key"]["url"] == "https://example.com"


def test_extract_timeline_samples_filters_out_of_range_timestamps():
    from mi_fitness_sync.activities import _extract_timeline_samples

    items = [
        # Within activity range
        {"sid": "s1", "time": 1717200000, "key": "heart_rate", "value": '{"time":1717200010,"bpm":120}'},
        # Within activity range
        {"sid": "s1", "time": 1717200000, "key": "heart_rate", "value": '{"time":1717200050,"bpm":130}'},
        # Outside range: 20 days earlier (metric payload time matters)
        {"sid": "s1", "time": 1717200000, "key": "heart_rate", "value": '{"time":1715500000,"bpm":80}'},
        # Outside range: way after activity
        {"sid": "s1", "time": 1717200000, "key": "heart_rate", "value": '{"time":1717290000,"bpm":70}'},
    ]

    samples = _extract_timeline_samples(
        items,
        sid="s1",
        activity_start=1717200000,
        activity_end=1717200060,
    )

    timestamps = [s.timestamp for s in samples]
    # Only the samples whose metric payload time falls within [start-60, end+60] should remain
    assert 1717200010 in timestamps
    assert 1717200050 in timestamps
    assert 1715500000 not in timestamps
    assert 1717290000 not in timestamps


def test_extract_timeline_samples_uses_metric_payload_time():
    from mi_fitness_sync.activities import _extract_timeline_samples

    items = [
        # Item time is a bucket, but metric payload has the actual measurement time
        {"sid": "s1", "time": 1717200000, "key": "heart_rate", "value": '{"time":1717200030,"bpm":140}'},
    ]

    samples = _extract_timeline_samples(items, sid="s1")

    assert len(samples) == 1
    assert samples[0].timestamp == 1717200030
    assert samples[0].heart_rate == 140


def test_build_fds_suffix_uses_underscore_separator():
    """Verify the FDS suffix uses '_' separator matching Android's LOCALE_REPORTED_SERVER_SEPARATOR."""
    result = _build_fds_suffix(
        sid="test-sid",
        timestamp=1000000,
        timezone_offset=0,
        sport_type=1,
        file_type=0,
    )
    parts = result.split("_")
    # Should have exactly 2 parts: base64(keyBytes) and base64(sha1(sid))
    # The sha1 hash base64 itself may contain '_' (url-safe base64), so check first part is key bytes
    assert not result.startswith(":")
    assert ":" not in result.split("_", 1)[0]


def test_timeline_fallback_detail_prefers_sample_calories_over_summary():
    """Regression: when detail comes from timeline fallback, sample-derived
    calories must take precedence over the stale activity summary value."""
    from mi_fitness_sync.activities import ActivityDetail, ActivitySample

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
        calories=321,  # stale summary value
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={},
        raw_report={},
    )
    detail = ActivityDetail(
        activity=activity,
        detail_sid="sid-1",
        detail_key="fitness_data_timeline",  # timeline fallback
        detail_time=1717200000,
        zone_name="UTC",
        zone_offset_seconds=0,
        track_points=[],
        samples=[
            ActivitySample(
                timestamp=1717200000, start_time=None, end_time=None,
                duration_seconds=None, heart_rate=100, cadence=None,
                speed_mps=None, distance_meters=None, altitude_meters=None,
                steps=None, calories=150, raw_sample={},
            ),
            ActivitySample(
                timestamp=1717203600, start_time=None, end_time=None,
                duration_seconds=None, heart_rate=130, cadence=None,
                speed_mps=None, distance_meters=None, altitude_meters=None,
                steps=None, calories=372, raw_sample={},
            ),
        ],
        raw_fitness_item={"source": "fitness_data_timeline"},
        raw_detail={"source": "fitness_data_timeline"},
    )

    # Sample value (372) must win over stale summary (321)
    assert detail.total_calories == 372


def test_non_timeline_detail_still_prefers_activity_summary_calories():
    """Non-timeline detail should still prefer activity summary calories."""
    from mi_fitness_sync.activities import ActivityDetail, ActivitySample

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
        detail_key="key-1",  # NOT timeline fallback
        detail_time=1717200000,
        zone_name="UTC",
        zone_offset_seconds=0,
        track_points=[],
        samples=[
            ActivitySample(
                timestamp=1717200060, start_time=None, end_time=None,
                duration_seconds=None, heart_rate=120, cadence=None,
                speed_mps=None, distance_meters=None, altitude_meters=None,
                steps=None, calories=99, raw_sample={},
            ),
        ],
        raw_fitness_item={},
        raw_detail={},
    )

    # Summary value should win for non-timeline detail
    assert detail.total_calories == 42


def test_timeline_fallback_detail_prefers_sample_distance_over_summary():
    """When detail comes from timeline fallback, sample-derived distance
    must take precedence over the stale summary value."""
    from mi_fitness_sync.activities import ActivityDetail, ActivitySample

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
        distance_meters=400,  # stale summary
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
        detail_key="fitness_data_timeline",
        detail_time=1717200000,
        zone_name="UTC",
        zone_offset_seconds=0,
        track_points=[],
        samples=[
            ActivitySample(
                timestamp=1717200060, start_time=None, end_time=None,
                duration_seconds=None, heart_rate=None, cadence=None,
                speed_mps=None, distance_meters=550.0, altitude_meters=None,
                steps=None, calories=None, raw_sample={},
            ),
        ],
        raw_fitness_item={"source": "fitness_data_timeline"},
        raw_detail={"source": "fitness_data_timeline"},
    )

    assert detail.total_distance_meters == 550.0
