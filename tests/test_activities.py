from __future__ import annotations

import json

import pytest

from mi_fitness_sync.activities import (
    ACTIVITY_LIST_ENDPOINT,
    Activity,
    MiFitnessActivitiesClient,
    TrackPoint,
    ActivitySample,
    _build_fds_suffix,
    _merge_fds_samples_into_track_points,
    _merge_samples_into_track_points,
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


def test_get_activity_by_id_distinguishes_same_sid_different_timestamps(auth_state, monkeypatch):
    """Two activities sharing the same sid+key but different timestamps must resolve independently."""
    from mi_fitness_sync.activities import ActivityPage

    client = MiFitnessActivitiesClient(auth_state)

    activity_a = Activity(
        activity_id="882963223:strength_training:1774351918",
        sid="882963223",
        key="strength_training",
        category="strength_training",
        sport_type=22,
        title="Strength Training A",
        start_time=1774351918,
        end_time=1774355518,
        duration_seconds=3600,
        distance_meters=None,
        calories=200,
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={"sid": "882963223", "key": "strength_training", "time": 1774351918},
        raw_report={},
    )

    activity_b = Activity(
        activity_id="882963223:strength_training:1774241243",
        sid="882963223",
        key="strength_training",
        category="strength_training",
        sport_type=22,
        title="Strength Training B",
        start_time=1774241243,
        end_time=1774244843,
        duration_seconds=3600,
        distance_meters=None,
        calories=150,
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={"sid": "882963223", "key": "strength_training", "time": 1774241243},
        raw_report={},
    )

    page = ActivityPage(activities=[activity_a, activity_b], has_more=False, next_key=None)
    monkeypatch.setattr(client, "_fetch_activity_page", lambda **kwargs: page)

    result_a = client.get_activity_by_id("882963223:strength_training:1774351918")
    assert result_a.activity_id == "882963223:strength_training:1774351918"
    assert result_a.title == "Strength Training A"

    result_b = client.get_activity_by_id("882963223:strength_training:1774241243")
    assert result_b.activity_id == "882963223:strength_training:1774241243"
    assert result_b.title == "Strength Training B"


def test_get_activity_detail_item_distinguishes_same_sid_different_timestamps(auth_state, monkeypatch):
    """_get_activity_detail_item must match the correct fitness data item by time when sid+key overlap."""
    from mi_fitness_sync.activities import FitnessDataPage

    client = MiFitnessActivitiesClient(auth_state)

    activity = Activity(
        activity_id="882963223:strength_training:1774241243",
        sid="882963223",
        key="strength_training",
        category="strength_training",
        sport_type=22,
        title="Strength Training B",
        start_time=1774241243,
        end_time=1774244843,
        duration_seconds=3600,
        distance_meters=None,
        calories=150,
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={"sid": "882963223", "key": "strength_training", "time": 1774241243},
        raw_report={},
    )

    # Two fitness data items sharing sid+key but with different time values
    fitness_item_wrong = {
        "sid": "882963223",
        "key": "strength_training",
        "time": 1774351918,
        "value": '{"sport_records": []}',
    }
    fitness_item_correct = {
        "sid": "882963223",
        "key": "strength_training",
        "time": 1774241243,
        "value": '{"sport_records": []}',
    }

    page = FitnessDataPage(items=[fitness_item_wrong, fitness_item_correct], has_more=False, next_key=None)
    monkeypatch.setattr(client, "_fetch_fitness_data_page", lambda **kwargs: page)

    result = client._get_activity_detail_item(activity)
    assert result["time"] == 1774241243


def test_get_activity_detail_item_paginates_to_find_matching_timestamp(auth_state, monkeypatch):
    """Regression: correct detail item is on the second page (has_more=True on page 1)."""
    from mi_fitness_sync.activities import FitnessDataPage

    client = MiFitnessActivitiesClient(auth_state)

    activity = Activity(
        activity_id="882963223:strength_training:1774241243",
        sid="882963223",
        key="strength_training",
        category="strength_training",
        sport_type=22,
        title="Strength Training B",
        start_time=1774241243,
        end_time=1774244843,
        duration_seconds=3600,
        distance_meters=None,
        calories=150,
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={"sid": "882963223", "key": "strength_training", "time": 1774241243},
        raw_report={},
    )

    # Page 1: wrong timestamp — matches sid+key but NOT time
    page1_item = {
        "sid": "882963223",
        "key": "strength_training",
        "time": 1774351918,
        "value": '{"sport_records": []}',
    }
    page1 = FitnessDataPage(items=[page1_item], has_more=True, next_key="page2-token")

    # Page 2: correct timestamp
    page2_item = {
        "sid": "882963223",
        "key": "strength_training",
        "time": 1774241243,
        "value": '{"sport_records": []}',
    }
    page2 = FitnessDataPage(items=[page2_item], has_more=False, next_key=None)

    pages = {"__first__": page1, "page2-token": page2}

    def fake_fetch(**kwargs):
        token = kwargs.get("next_key")
        return pages[token] if token else pages["__first__"]

    monkeypatch.setattr(client, "_fetch_fitness_data_page", fake_fetch)

    result = client._get_activity_detail_item(activity)
    assert result["time"] == 1774241243


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

    monkeypatch.setattr(client, "_try_get_fds_download_map", lambda selected_activity: {})
    monkeypatch.setattr(client, "_try_download_fds_sport_samples", lambda activity, fds: [])
    monkeypatch.setattr(client, "_try_download_fds_gps_track_points", lambda activity, fds: [])
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


def test_get_activity_detail_uses_fds_samples_as_primary(auth_state, monkeypatch):
    """When FDS samples are available, they replace JSON detail samples regardless of count."""
    from mi_fitness_sync.activities import ActivitySample

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

    fds_samples = [
        ActivitySample(
            timestamp=1717200000, start_time=1717200000, end_time=1717200000,
            duration_seconds=1, heart_rate=118, cadence=None, speed_mps=None,
            distance_meters=None, altitude_meters=None, steps=None, calories=None,
            raw_sample={"source": "fds_sport_record"},
        ),
    ]

    fitness_item = {
        "sid": "sid-1",
        "key": "key-1",
        "time": 1717200000,
        "zone_name": "UTC",
        "zone_offset": 0,
        "value": json.dumps({
            "sport_records": [
                {"startTime": 1717200000, "endTime": 1717200000, "hr": 120, "distance": 0},
                {"startTime": 1717200030, "endTime": 1717200030, "hr": 122, "distance": 250},
                {"startTime": 1717200060, "endTime": 1717200060, "hr": 125, "distance": 500},
            ],
        }),
    }

    monkeypatch.setattr(client, "_try_get_fds_download_map", lambda a: {})
    monkeypatch.setattr(client, "_try_download_fds_sport_samples", lambda a, f: fds_samples)
    monkeypatch.setattr(client, "_try_download_fds_gps_track_points", lambda a, f: [])
    monkeypatch.setattr(client, "_get_activity_detail_item", lambda a: fitness_item)

    detail = client.get_activity_detail(activity)

    # FDS samples replace JSON samples even though JSON has more (3 vs 1)
    assert len(detail.samples) == 1
    assert detail.samples[0].heart_rate == 118
    assert detail.samples[0].raw_sample["source"] == "fds_sport_record"


def test_get_activity_detail_fds_only_when_no_json(auth_state, monkeypatch):
    """When JSON detail is empty but FDS samples are available, use FDS as sole source."""
    from mi_fitness_sync.activities import ActivitySample

    client = MiFitnessActivitiesClient(auth_state)
    activity = Activity(
        activity_id="sid-1:key-1:1717200000",
        sid="sid-1",
        key="key-1",
        category="strength_training",
        sport_type=22,
        title="Strength",
        start_time=1717200000,
        end_time=1717200060,
        duration_seconds=60,
        distance_meters=None,
        calories=None,
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={"sid": "sid-1", "key": "key-1", "time": 1717200000, "zone_name": "UTC", "zone_offset": 0},
        raw_report={},
    )

    fds_samples = [
        ActivitySample(
            timestamp=1717200000, start_time=1717200000, end_time=1717200000,
            duration_seconds=1, heart_rate=100, cadence=None, speed_mps=None,
            distance_meters=None, altitude_meters=None, steps=None, calories=None,
            raw_sample={"source": "fds_sport_record"},
        ),
    ]

    monkeypatch.setattr(client, "_try_get_fds_download_map", lambda a: {})
    monkeypatch.setattr(client, "_try_download_fds_sport_samples", lambda a, f: fds_samples)
    monkeypatch.setattr(client, "_try_download_fds_gps_track_points", lambda a, f: [])
    monkeypatch.setattr(client, "_get_activity_detail_item", lambda a: {})

    detail = client.get_activity_detail(activity)

    assert detail.detail_key == "fds_sport_record"
    assert len(detail.samples) == 1
    assert len(detail.track_points) == 0


def test_get_activity_detail_raises_when_no_data(auth_state, monkeypatch):
    """When both JSON detail and FDS fail, raise MiFitnessError."""
    client = MiFitnessActivitiesClient(auth_state)
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
        raw_record={"sid": "sid-1", "key": "key-1", "time": 1717200000},
        raw_report={},
    )

    monkeypatch.setattr(client, "_try_get_fds_download_map", lambda a: {})
    monkeypatch.setattr(client, "_try_download_fds_sport_samples", lambda a, f: [])
    monkeypatch.setattr(client, "_try_download_fds_gps_track_points", lambda a, f: [])
    monkeypatch.setattr(client, "_get_activity_detail_item", lambda a: {})

    with pytest.raises(MiFitnessError, match="Could not find a detail payload"):
        client.get_activity_detail(activity)


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


def test_summary_calories_preferred_over_sample_calories():
    """Activity summary calories should always be preferred over sample-derived values."""
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
        sport_report=None,
        recovery_rate=None,
        raw_fitness_item={},
        raw_detail={},
    )

    # Summary value (321) wins over sample value (372)
    assert detail.total_calories == 321


def test_detail_still_prefers_activity_summary_calories():
    """Detail should always prefer activity summary calories."""
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
        sport_report=None,
        recovery_rate=None,
        raw_fitness_item={},
        raw_detail={},
    )

    # Summary value should win for non-timeline detail
    assert detail.total_calories == 42


def test_sample_distance_used_when_summary_missing():
    """When activity summary distance is missing, sample-derived distance is used."""
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
        distance_meters=None,  # no summary distance
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
                timestamp=1717200060, start_time=None, end_time=None,
                duration_seconds=None, heart_rate=None, cadence=None,
                speed_mps=None, distance_meters=550.0, altitude_meters=None,
                steps=None, calories=None, raw_sample={},
            ),
        ],
        sport_report=None,
        recovery_rate=None,
        raw_fitness_item={},
        raw_detail={},
    )

    assert detail.total_distance_meters == 550.0


# ---------------------------------------------------------------------------
# GPS track point / sport sample merge
# ---------------------------------------------------------------------------


class TestMergeFdsSamplesIntoTrackPoints:
    def test_merges_hr_and_cadence_by_timestamp(self):
        track_points = [
            TrackPoint(timestamp=1000, latitude=31.2, longitude=121.5, altitude_meters=50.0,
                       speed_mps=5.0, distance_meters=None, heart_rate=None, cadence=None, raw_point={}),
            TrackPoint(timestamp=1001, latitude=31.201, longitude=121.501, altitude_meters=51.0,
                       speed_mps=5.1, distance_meters=None, heart_rate=None, cadence=None, raw_point={}),
        ]
        samples = [
            ActivitySample(timestamp=1000, start_time=1000, end_time=1000, duration_seconds=1,
                           heart_rate=120, cadence=80, speed_mps=None, distance_meters=None,
                           altitude_meters=None, steps=None, calories=None, raw_sample={}),
            ActivitySample(timestamp=1001, start_time=1001, end_time=1001, duration_seconds=1,
                           heart_rate=125, cadence=82, speed_mps=None, distance_meters=None,
                           altitude_meters=None, steps=None, calories=None, raw_sample={}),
        ]

        _merge_fds_samples_into_track_points(track_points, samples)

        assert track_points[0].heart_rate == 120
        assert track_points[0].cadence == 80
        assert track_points[1].heart_rate == 125
        assert track_points[1].cadence == 82

    def test_no_overwrite_existing_values(self):
        track_points = [
            TrackPoint(timestamp=1000, latitude=31.2, longitude=121.5, altitude_meters=50.0,
                       speed_mps=5.0, distance_meters=None, heart_rate=115, cadence=None, raw_point={}),
        ]
        samples = [
            ActivitySample(timestamp=1000, start_time=1000, end_time=1000, duration_seconds=1,
                           heart_rate=120, cadence=80, speed_mps=None, distance_meters=None,
                           altitude_meters=None, steps=None, calories=None, raw_sample={}),
        ]

        _merge_fds_samples_into_track_points(track_points, samples)

        assert track_points[0].heart_rate == 115  # NOT overwritten
        assert track_points[0].cadence == 80

    def test_unmatched_timestamps_left_alone(self):
        track_points = [
            TrackPoint(timestamp=1000, latitude=31.2, longitude=121.5, altitude_meters=None,
                       speed_mps=None, distance_meters=None, heart_rate=None, cadence=None, raw_point={}),
        ]
        samples = [
            ActivitySample(timestamp=9999, start_time=9999, end_time=9999, duration_seconds=1,
                           heart_rate=150, cadence=90, speed_mps=None, distance_meters=None,
                           altitude_meters=None, steps=None, calories=None, raw_sample={}),
        ]

        _merge_fds_samples_into_track_points(track_points, samples)

        assert track_points[0].heart_rate is None

    def test_empty_inputs(self):
        _merge_fds_samples_into_track_points([], [])  # should not raise
