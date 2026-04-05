from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from mi_fitness_sync.exceptions import StravaError
from mi_fitness_sync.strava.client import StravaClient
from mi_fitness_sync.strava.store import StravaTokenState


def _make_token_state(*, expires_at: int | None = None) -> StravaTokenState:
    return StravaTokenState(
        client_id="12345",
        client_secret="secret123",
        access_token="access-abc",
        refresh_token="refresh-xyz",
        expires_at=expires_at or int(time.time()) + 3600,
        athlete_id=42,
        created_at="2026-04-01T00:00:00+00:00",
        updated_at="2026-04-01T00:00:00+00:00",
    )


@patch("mi_fitness_sync.strava.client.time.sleep")
@patch("mi_fitness_sync.strava.client.requests")
def test_upload_activity_success(mock_requests, mock_sleep):
    upload_response = MagicMock()
    upload_response.status_code = 201
    upload_response.json.return_value = {"id": 123, "id_str": "123"}

    status_response = MagicMock()
    status_response.status_code = 200
    status_response.json.return_value = {
        "id": 123,
        "activity_id": 456789,
        "error": None,
        "status": "Your activity is ready.",
    }

    mock_requests.post.return_value = upload_response
    mock_requests.get.return_value = status_response

    client = StravaClient(_make_token_state())
    result = client.upload_activity(b"fake-fit-data", sport_type="Run")

    assert result["activity_id"] == 456789
    mock_requests.post.assert_called_once()
    post_kwargs = mock_requests.post.call_args[1]
    assert post_kwargs["data"]["data_type"] == "fit"
    assert post_kwargs["data"]["sport_type"] == "Run"


@patch("mi_fitness_sync.strava.client.time.sleep")
@patch("mi_fitness_sync.strava.client.requests")
def test_upload_activity_processing_error(mock_requests, mock_sleep):
    upload_response = MagicMock()
    upload_response.status_code = 201
    upload_response.json.return_value = {"id": 123, "id_str": "123"}

    status_response = MagicMock()
    status_response.status_code = 200
    status_response.json.return_value = {
        "id": 123,
        "activity_id": None,
        "error": "duplicate of activity 999",
        "status": "There was an error processing your activity.",
    }

    mock_requests.post.return_value = upload_response
    mock_requests.get.return_value = status_response

    client = StravaClient(_make_token_state())
    with pytest.raises(StravaError, match="duplicate"):
        client.upload_activity(b"fake-fit-data")


@patch("mi_fitness_sync.strava.client.requests")
def test_upload_http_error_raises(mock_requests):
    upload_response = MagicMock()
    upload_response.status_code = 401
    upload_response.text = "Unauthorized"
    mock_requests.post.return_value = upload_response

    client = StravaClient(_make_token_state())
    with pytest.raises(StravaError, match="Upload failed"):
        client.upload_activity(b"fake-fit-data")


@patch("mi_fitness_sync.strava.client.save_tokens")
@patch("mi_fitness_sync.strava.client.refresh_access_token")
@patch("mi_fitness_sync.strava.client.time.sleep")
@patch("mi_fitness_sync.strava.client.requests")
def test_auto_refresh_on_expired_token(mock_requests, mock_sleep, mock_refresh, mock_save):
    mock_refresh.return_value = {
        "access_token": "new-access",
        "refresh_token": "new-refresh",
        "expires_at": int(time.time()) + 7200,
    }

    upload_response = MagicMock()
    upload_response.status_code = 201
    upload_response.json.return_value = {"id": 1, "id_str": "1"}

    status_response = MagicMock()
    status_response.status_code = 200
    status_response.json.return_value = {"activity_id": 999, "error": None}

    mock_requests.post.return_value = upload_response
    mock_requests.get.return_value = status_response

    expired_state = _make_token_state(expires_at=int(time.time()) - 100)
    client = StravaClient(expired_state)
    result = client.upload_activity(b"data")

    mock_refresh.assert_called_once()
    mock_save.assert_called_once()
    assert result["activity_id"] == 999


@patch("mi_fitness_sync.strava.client.requests")
def test_upload_no_sport_type(mock_requests):
    upload_response = MagicMock()
    upload_response.status_code = 201
    upload_response.json.return_value = {"id": 1, "id_str": "1"}
    mock_requests.post.return_value = upload_response

    status_response = MagicMock()
    status_response.status_code = 200
    status_response.json.return_value = {"activity_id": 100, "error": None}
    mock_requests.get.return_value = status_response

    with patch("mi_fitness_sync.strava.client.time.sleep"):
        client = StravaClient(_make_token_state())
        client.upload_activity(b"data")

    post_data = mock_requests.post.call_args[1]["data"]
    assert "sport_type" not in post_data


@patch("mi_fitness_sync.strava.client.requests")
def test_list_activities_returns_results(mock_requests):
    activities = [
        {"id": 1, "name": "Morning Run", "start_date": "2026-06-01T00:00:00Z", "sport_type": "Run"},
        {"id": 2, "name": "Afternoon Ride", "start_date": "2026-06-01T00:10:00Z", "sport_type": "Ride"},
    ]
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = activities
    mock_requests.get.return_value = response

    client = StravaClient(_make_token_state())
    result = client.list_activities(after=1000, before=2000)

    assert result == activities
    mock_requests.get.assert_called_once()
    call_kwargs = mock_requests.get.call_args
    assert call_kwargs[1]["params"] == {"after": 1000, "before": 2000, "per_page": 30}


@patch("mi_fitness_sync.strava.client.requests")
def test_list_activities_http_error_raises(mock_requests):
    response = MagicMock()
    response.status_code = 401
    response.text = "Unauthorized"
    mock_requests.get.return_value = response

    client = StravaClient(_make_token_state())
    with pytest.raises(StravaError, match="Failed to list Strava activities"):
        client.list_activities(after=1000, before=2000)


@patch("mi_fitness_sync.strava.client.requests")
def test_list_activities_empty_list(mock_requests):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = []
    mock_requests.get.return_value = response

    client = StravaClient(_make_token_state())
    result = client.list_activities(after=1000, before=2000)

    assert result == []
