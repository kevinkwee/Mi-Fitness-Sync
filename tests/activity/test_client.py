from __future__ import annotations

import json

import pytest

from mi_fitness_sync.activity.client import ACTIVITY_LIST_ENDPOINT, MiFitnessActivitiesClient
from mi_fitness_sync.activity.models import Activity, ActivityPage, ActivitySample, FitnessDataPage
from mi_fitness_sync.exceptions import MiFitnessError


def test_collect_cookie_values_fills_locale_and_user_id(auth_state):
    client = MiFitnessActivitiesClient(auth_state)

    assert client._cookie_values["locale"] == "en_US"
    assert client._cookie_values["userId"] == auth_state.user_id


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


def test_get_activity_by_id_distinguishes_same_sid_different_timestamps(auth_state, monkeypatch):
    client = MiFitnessActivitiesClient(auth_state)

    activity_a = Activity(
        activity_id="123456789:strength_training:1700000001",
        sid="123456789",
        key="strength_training",
        category="strength_training",
        sport_type=22,
        title="Strength Training A",
        start_time=1700000001,
        end_time=1700003601,
        duration_seconds=3600,
        distance_meters=None,
        calories=200,
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={"sid": "123456789", "key": "strength_training", "time": 1700000001},
        raw_report={},
    )

    activity_b = Activity(
        activity_id="123456789:strength_training:1700000002",
        sid="123456789",
        key="strength_training",
        category="strength_training",
        sport_type=22,
        title="Strength Training B",
        start_time=1700000002,
        end_time=1700003602,
        duration_seconds=3600,
        distance_meters=None,
        calories=150,
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={"sid": "123456789", "key": "strength_training", "time": 1700000002},
        raw_report={},
    )

    page = ActivityPage(activities=[activity_a, activity_b], has_more=False, next_key=None)
    monkeypatch.setattr(client, "_fetch_activity_page", lambda **kwargs: page)

    result_a = client.get_activity_by_id("123456789:strength_training:1700000001")
    result_b = client.get_activity_by_id("123456789:strength_training:1700000002")

    assert result_a.activity_id == "123456789:strength_training:1700000001"
    assert result_a.title == "Strength Training A"
    assert result_b.activity_id == "123456789:strength_training:1700000002"
    assert result_b.title == "Strength Training B"


def test_get_activity_detail_item_distinguishes_same_sid_different_timestamps(auth_state, monkeypatch):
    client = MiFitnessActivitiesClient(auth_state)

    activity = Activity(
        activity_id="123456789:strength_training:1700000002",
        sid="123456789",
        key="strength_training",
        category="strength_training",
        sport_type=22,
        title="Strength Training B",
        start_time=1700000002,
        end_time=1700003602,
        duration_seconds=3600,
        distance_meters=None,
        calories=150,
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={"sid": "123456789", "key": "strength_training", "time": 1700000002},
        raw_report={},
    )

    fitness_item_wrong = {
        "sid": "123456789",
        "key": "strength_training",
        "time": 1700000001,
        "value": '{"sport_records": []}',
    }
    fitness_item_correct = {
        "sid": "123456789",
        "key": "strength_training",
        "time": 1700000002,
        "value": '{"sport_records": []}',
    }

    page = FitnessDataPage(items=[fitness_item_wrong, fitness_item_correct], has_more=False, next_key=None)
    monkeypatch.setattr(client, "_fetch_fitness_data_page", lambda **kwargs: page)

    result = client._get_activity_detail_item(activity)
    assert result["time"] == 1700000002


def test_get_activity_detail_item_paginates_to_find_matching_timestamp(auth_state, monkeypatch):
    client = MiFitnessActivitiesClient(auth_state)

    activity = Activity(
        activity_id="123456789:strength_training:1700000002",
        sid="123456789",
        key="strength_training",
        category="strength_training",
        sport_type=22,
        title="Strength Training B",
        start_time=1700000002,
        end_time=1700003602,
        duration_seconds=3600,
        distance_meters=None,
        calories=150,
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={"sid": "123456789", "key": "strength_training", "time": 1700000002},
        raw_report={},
    )

    page1_item = {
        "sid": "123456789",
        "key": "strength_training",
        "time": 1700000001,
        "value": '{"sport_records": []}',
    }
    page1 = FitnessDataPage(items=[page1_item], has_more=True, next_key="page2-token")

    page2_item = {
        "sid": "123456789",
        "key": "strength_training",
        "time": 1700000002,
        "value": '{"sport_records": []}',
    }
    page2 = FitnessDataPage(items=[page2_item], has_more=False, next_key=None)

    pages = {"__first__": page1, "page2-token": page2}

    def fake_fetch(**kwargs):
        token = kwargs.get("next_key")
        return pages[token] if token else pages["__first__"]

    monkeypatch.setattr(client, "_fetch_fitness_data_page", fake_fetch)

    result = client._get_activity_detail_item(activity)
    assert result["time"] == 1700000002


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


def test_get_activity_detail_uses_fds_samples_as_primary(auth_state, monkeypatch):
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
            timestamp=1717200000,
            start_time=1717200000,
            end_time=1717200000,
            duration_seconds=1,
            heart_rate=118,
            cadence=None,
            speed_mps=None,
            distance_meters=None,
            altitude_meters=None,
            steps=None,
            calories=None,
            raw_sample={"source": "fds_sport_record"},
        ),
    ]

    fitness_item = {
        "sid": "sid-1",
        "key": "key-1",
        "time": 1717200000,
        "zone_name": "UTC",
        "zone_offset": 0,
        "value": json.dumps(
            {
                "sport_records": [
                    {"startTime": 1717200000, "endTime": 1717200000, "hr": 120, "distance": 0},
                    {"startTime": 1717200030, "endTime": 1717200030, "hr": 122, "distance": 250},
                    {"startTime": 1717200060, "endTime": 1717200060, "hr": 125, "distance": 500},
                ],
            }
        ),
    }

    monkeypatch.setattr(client, "_try_get_fds_download_map", lambda a: {})
    monkeypatch.setattr(client, "_try_download_fds_sport_samples", lambda a, f: fds_samples)
    monkeypatch.setattr(client, "_try_download_fds_gps_track_points", lambda a, f: [])
    monkeypatch.setattr(client, "_get_activity_detail_item", lambda a: fitness_item)

    detail = client.get_activity_detail(activity)

    assert len(detail.samples) == 1
    assert detail.samples[0].heart_rate == 118
    assert detail.samples[0].raw_sample["source"] == "fds_sport_record"


def test_get_activity_detail_fds_only_when_no_json(auth_state, monkeypatch):
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
            timestamp=1717200000,
            start_time=1717200000,
            end_time=1717200000,
            duration_seconds=1,
            heart_rate=100,
            cadence=None,
            speed_mps=None,
            distance_meters=None,
            altitude_meters=None,
            steps=None,
            calories=None,
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


# ---------------------------------------------------------------------------
# list_activities client-side time filtering
# ---------------------------------------------------------------------------


def _make_activity(start_time: int) -> Activity:
    return Activity(
        activity_id=f"sid:key:{start_time}",
        sid="sid",
        key="key",
        category="outdoor_run",
        sport_type=1,
        title="Run",
        start_time=start_time,
        end_time=start_time + 3600,
        duration_seconds=3600,
        distance_meters=5000,
        calories=300,
        steps=6000,
        sync_state="server",
        next_key=None,
        raw_record={"sid": "sid", "key": "key", "time": start_time},
        raw_report={},
    )


def test_list_activities_filters_by_end_time(auth_state, monkeypatch):
    """--until should exclude activities whose start_time exceeds end_time."""
    client = MiFitnessActivitiesClient(auth_state)

    activities_from_api = [
        _make_activity(1000),
        _make_activity(2000),
        _make_activity(3000),
    ]
    page = ActivityPage(activities=activities_from_api, has_more=False, next_key=None)
    monkeypatch.setattr(client, "_fetch_activity_page", lambda **kwargs: page)

    result = client.list_activities(start_time=None, end_time=2000, limit=20)

    assert len(result) == 2
    assert result[0].start_time == 1000
    assert result[1].start_time == 2000


def test_list_activities_filters_by_start_time(auth_state, monkeypatch):
    """--since should exclude activities whose start_time is before start_time."""
    client = MiFitnessActivitiesClient(auth_state)

    activities_from_api = [
        _make_activity(1000),
        _make_activity(2000),
        _make_activity(3000),
    ]
    page = ActivityPage(activities=activities_from_api, has_more=False, next_key=None)
    monkeypatch.setattr(client, "_fetch_activity_page", lambda **kwargs: page)

    result = client.list_activities(start_time=2000, end_time=None, limit=20)

    assert len(result) == 2
    assert result[0].start_time == 2000
    assert result[1].start_time == 3000


def test_list_activities_filters_by_both_start_and_end_time(auth_state, monkeypatch):
    """--since + --until should keep only activities within the range."""
    client = MiFitnessActivitiesClient(auth_state)

    activities_from_api = [
        _make_activity(1000),
        _make_activity(2000),
        _make_activity(3000),
        _make_activity(4000),
    ]
    page = ActivityPage(activities=activities_from_api, has_more=False, next_key=None)
    monkeypatch.setattr(client, "_fetch_activity_page", lambda **kwargs: page)

    result = client.list_activities(start_time=2000, end_time=3000, limit=20)

    assert len(result) == 2
    assert result[0].start_time == 2000
    assert result[1].start_time == 3000


def test_list_activities_no_filter_returns_all(auth_state, monkeypatch):
    """No --since/--until should return all activities."""
    client = MiFitnessActivitiesClient(auth_state)

    activities_from_api = [
        _make_activity(1000),
        _make_activity(2000),
        _make_activity(3000),
    ]
    page = ActivityPage(activities=activities_from_api, has_more=False, next_key=None)
    monkeypatch.setattr(client, "_fetch_activity_page", lambda **kwargs: page)

    result = client.list_activities(start_time=None, end_time=None, limit=20)

    assert len(result) == 3


def test_list_activities_filter_respects_limit(auth_state, monkeypatch):
    """Limit should be enforced after filtering."""
    client = MiFitnessActivitiesClient(auth_state)

    activities_from_api = [
        _make_activity(1000),
        _make_activity(2000),
        _make_activity(3000),
    ]
    page = ActivityPage(activities=activities_from_api, has_more=False, next_key=None)
    monkeypatch.setattr(client, "_fetch_activity_page", lambda **kwargs: page)

    result = client.list_activities(start_time=None, end_time=3000, limit=2)

    assert len(result) == 2


def test_list_activities_paginates_when_filtering_reduces_count(auth_state, monkeypatch):
    """Should fetch additional pages when filtering reduces the count below limit."""
    client = MiFitnessActivitiesClient(auth_state)

    page1 = ActivityPage(
        activities=[_make_activity(500), _make_activity(2000)],
        has_more=True,
        next_key="page2",
    )
    page2 = ActivityPage(
        activities=[_make_activity(600), _make_activity(3000)],
        has_more=False,
        next_key=None,
    )

    def fake_fetch(**kwargs):
        if kwargs.get("next_key") == "page2":
            return page2
        return page1

    monkeypatch.setattr(client, "_fetch_activity_page", fake_fetch)

    # Only activities with start_time >= 1000 should pass
    result = client.list_activities(start_time=1000, end_time=None, limit=20)

    assert len(result) == 2
    assert result[0].start_time == 2000
    assert result[1].start_time == 3000


def test_list_activities_includes_activity_with_none_start_time(auth_state, monkeypatch):
    """Activities missing start_time should not be excluded by filters."""
    client = MiFitnessActivitiesClient(auth_state)

    no_start = Activity(
        activity_id="sid:key:0",
        sid="sid",
        key="key",
        category="outdoor_run",
        sport_type=1,
        title="Run",
        start_time=None,
        end_time=None,
        duration_seconds=None,
        distance_meters=None,
        calories=None,
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={"sid": "sid", "key": "key", "time": 0},
        raw_report={},
    )
    page = ActivityPage(activities=[no_start, _make_activity(2000)], has_more=False, next_key=None)
    monkeypatch.setattr(client, "_fetch_activity_page", lambda **kwargs: page)

    result = client.list_activities(start_time=1000, end_time=3000, limit=20)

    assert len(result) == 2
