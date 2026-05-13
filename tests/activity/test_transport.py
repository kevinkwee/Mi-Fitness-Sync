from __future__ import annotations

from unittest.mock import MagicMock, patch

from mi_fitness_sync.activity.transport import ActivityTransport


def test_request_json_retries_retryable_http_status(auth_state):
    session = MagicMock()

    retry_response = MagicMock()
    retry_response.status_code = 429
    retry_response.ok = False
    retry_response.text = "retry later"
    retry_response.headers = {"Retry-After": "0"}

    success_response = MagicMock()
    success_response.status_code = 200
    success_response.ok = True
    success_response.text = "encrypted-body"
    success_response.headers = {}

    session.get.side_effect = [retry_response, success_response]
    transport = ActivityTransport(session, auth_state, timeout=10, country_code="ID")

    with (
        patch("mi_fitness_sync.activity.transport.generate_nonce", return_value="nonce"),
        patch("mi_fitness_sync.activity.transport.encrypt_query_params", return_value={"data": "encrypted"}),
        patch("mi_fitness_sync.activity.transport.decrypt_response_payload", return_value={"code": 0, "result": {"ok": True}}),
        patch("mi_fitness_sync.activity.transport.time.sleep") as sleep_mock,
    ):
        payload = transport.request_json(
            endpoint="https://sg.hlth.io.mi.com/app/v1/data/get_sport_records_by_time",
            path="/app/v1/data/get_sport_records_by_time",
            request_payload={"limit": 1},
            request_label="Mi Fitness activity request",
        )

    assert payload["result"]["ok"] is True
    assert session.get.call_count == 2
    sleep_mock.assert_called_once()
    assert session.get.call_args_list[0].kwargs["timeout"] == 10
    assert session.get.call_args_list[1].kwargs["timeout"] == 10
