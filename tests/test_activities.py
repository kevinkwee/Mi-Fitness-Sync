from __future__ import annotations

import json

import pytest

from mi_fitness_sync.activities import (
    ACTIVITY_LIST_ENDPOINT,
    MiFitnessActivitiesClient,
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
