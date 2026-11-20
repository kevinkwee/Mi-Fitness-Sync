from __future__ import annotations

import http.client
import socket
import threading
from http.server import HTTPServer
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pytest

from mi_fitness_sync.exceptions import StravaAuthError
from mi_fitness_sync.strava.auth import (
    STRAVA_AUTH_URL,
    STRAVA_DEAUTHORIZE_URL,
    _make_callback_handler,
    _OAuthResult,
    build_authorization_url,
    exchange_token,
    refresh_access_token,
    revoke_access_token,
    run_oauth_flow,
)


def test_build_authorization_url_includes_required_params() -> None:
    url = build_authorization_url("123", "http://localhost:5478/callback", "test-state")

    parsed = urlparse(url)
    params = parse_qs(parsed.query)

    assert parsed.scheme == "https"
    assert STRAVA_AUTH_URL.split("://", 1)[1].startswith(parsed.netloc + parsed.path)
    assert params["client_id"] == ["123"]
    assert params["redirect_uri"] == ["http://localhost:5478/callback"]
    assert params["response_type"] == ["code"]
    assert params["state"] == ["test-state"]
    assert "activity:write" in params["scope"][0]


@patch("mi_fitness_sync.strava.auth.requests.post")
def test_exchange_token_success(mock_post: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": "access-abc",
        "refresh_token": "refresh-xyz",
        "expires_at": 1700000000,
        "athlete": {"id": 42},
    }
    mock_post.return_value = mock_response

    result = exchange_token("123", "secret", "auth-code")

    assert result["access_token"] == "access-abc"
    mock_post.assert_called_once()
    call_data = mock_post.call_args[1]["data"]
    assert call_data["grant_type"] == "authorization_code"
    assert call_data["code"] == "auth-code"


@patch("mi_fitness_sync.strava.auth.requests.post")
def test_exchange_token_failure_raises(mock_post: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_response.text = "Unauthorized"
    mock_post.return_value = mock_response

    with pytest.raises(StravaAuthError, match="Token exchange failed"):
        exchange_token("123", "secret", "bad-code")


@patch("mi_fitness_sync.strava.auth.requests.post")
def test_refresh_access_token_success(mock_post: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": "new-access",
        "refresh_token": "new-refresh",
        "expires_at": 1700010000,
    }
    mock_post.return_value = mock_response

    result = refresh_access_token("123", "secret", "refresh-xyz")

    assert result["access_token"] == "new-access"
    call_data = mock_post.call_args[1]["data"]
    assert call_data["grant_type"] == "refresh_token"
    assert call_data["refresh_token"] == "refresh-xyz"


@patch("mi_fitness_sync.strava.auth.requests.post")
def test_refresh_access_token_failure_raises(mock_post: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = "Bad request"
    mock_post.return_value = mock_response

    with pytest.raises(StravaAuthError, match="Token refresh failed"):
        refresh_access_token("123", "secret", "bad-refresh")


@patch("mi_fitness_sync.strava.auth.requests.post")
def test_revoke_access_token_success(mock_post: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"access_token": "access-abc"}
    mock_post.return_value = mock_response

    revoke_access_token("access-abc")

    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args
    assert call_kwargs[1]["data"]["access_token"] == "access-abc"
    assert STRAVA_DEAUTHORIZE_URL in call_kwargs[0] or call_kwargs[0][0] == STRAVA_DEAUTHORIZE_URL


@patch("mi_fitness_sync.strava.auth.requests.post")
def test_revoke_access_token_failure_raises(mock_post: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_response.text = "Unauthorized"
    mock_post.return_value = mock_response

    with pytest.raises(StravaAuthError, match="Token revocation failed"):
        revoke_access_token("bad-token")


# ---------------------------------------------------------------------------
# Callback handler tests (real HTTP, ephemeral port)
# ---------------------------------------------------------------------------


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("localhost", 0))
        return int(sock.getsockname()[1])


def _serve_one_request(result: _OAuthResult, port: int) -> tuple[HTTPServer, threading.Thread]:
    """Start a real HTTP server on *port* and handle exactly one request."""
    handler_cls = _make_callback_handler(result)
    server = HTTPServer(("localhost", port), handler_cls)
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()
    return server, thread


def test_callback_handler_success() -> None:
    port = _find_free_port()
    result = _OAuthResult(expected_state="good-state")
    server, thread = _serve_one_request(result, port)

    conn = http.client.HTTPConnection("localhost", port, timeout=5)
    conn.request("GET", "/callback?code=abc123&state=good-state")
    resp = conn.getresponse()
    conn.close()
    thread.join(timeout=5)
    server.server_close()

    assert resp.status == 200
    assert result.code == "abc123"
    assert result.error is None
    assert result.received.is_set()


def test_callback_handler_state_mismatch() -> None:
    port = _find_free_port()
    result = _OAuthResult(expected_state="expected")
    server, thread = _serve_one_request(result, port)

    conn = http.client.HTTPConnection("localhost", port, timeout=5)
    conn.request("GET", "/callback?code=abc&state=wrong")
    resp = conn.getresponse()
    conn.close()
    thread.join(timeout=5)
    server.server_close()

    assert resp.status == 400
    assert result.code is None
    assert "State mismatch" in (result.error or "")


def test_callback_handler_denied() -> None:
    port = _find_free_port()
    result = _OAuthResult(expected_state="s")
    server, thread = _serve_one_request(result, port)

    conn = http.client.HTTPConnection("localhost", port, timeout=5)
    conn.request("GET", "/callback?error=access_denied")
    resp = conn.getresponse()
    conn.close()
    thread.join(timeout=5)
    server.server_close()

    assert resp.status == 200
    assert result.error == "access_denied"
    assert result.code is None


def test_callback_handler_non_callback_path() -> None:
    port = _find_free_port()
    result = _OAuthResult(expected_state="s")
    server, thread = _serve_one_request(result, port)

    conn = http.client.HTTPConnection("localhost", port, timeout=5)
    conn.request("GET", "/other")
    resp = conn.getresponse()
    conn.close()
    thread.join(timeout=5)
    server.server_close()

    assert resp.status == 404
    assert not result.received.is_set()


# ---------------------------------------------------------------------------
# run_oauth_flow integration tests
# ---------------------------------------------------------------------------


@patch("mi_fitness_sync.strava.auth.exchange_token")
@patch("mi_fitness_sync.strava.auth.webbrowser")
def test_run_oauth_flow_success(mock_browser: MagicMock, mock_exchange: MagicMock) -> None:
    mock_exchange.return_value = {
        "access_token": "at",
        "refresh_token": "rt",
        "expires_at": 9999,
        "athlete": {"id": 1},
    }
    port = _find_free_port()

    def simulate_callback(url: str) -> None:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        state = params["state"][0]
        conn = http.client.HTTPConnection("localhost", port, timeout=5)
        conn.request("GET", f"/callback?code=auth-code-ok&state={state}")
        conn.getresponse()
        conn.close()

    mock_browser.open.side_effect = simulate_callback

    result = run_oauth_flow("cid", "csecret", port=port)

    assert result["access_token"] == "at"
    mock_exchange.assert_called_once_with("cid", "csecret", "auth-code-ok")


@patch("mi_fitness_sync.strava.auth.webbrowser")
def test_run_oauth_flow_state_mismatch(mock_browser: MagicMock) -> None:
    port = _find_free_port()

    def send_wrong_state(url: str) -> None:
        conn = http.client.HTTPConnection("localhost", port, timeout=5)
        conn.request("GET", "/callback?code=c&state=wrong")
        conn.getresponse()
        conn.close()

    mock_browser.open.side_effect = send_wrong_state

    with pytest.raises(StravaAuthError, match="State mismatch"):
        run_oauth_flow("cid", "csecret", port=port)


@patch("mi_fitness_sync.strava.auth.webbrowser")
def test_run_oauth_flow_denied(mock_browser: MagicMock) -> None:
    port = _find_free_port()

    def send_denied(url: str) -> None:
        conn = http.client.HTTPConnection("localhost", port, timeout=5)
        conn.request("GET", "/callback?error=access_denied")
        conn.getresponse()
        conn.close()

    mock_browser.open.side_effect = send_denied

    with pytest.raises(StravaAuthError, match="access_denied"):
        run_oauth_flow("cid", "csecret", port=port)


def test_run_oauth_flow_port_bind_failure() -> None:
    with (
        patch(
            "mi_fitness_sync.strava.auth.HTTPServer",
            side_effect=OSError("Address already in use"),
        ),
        pytest.raises(StravaAuthError, match="Could not start OAuth callback server"),
    ):
        run_oauth_flow("cid", "csecret", port=5478)


@patch("mi_fitness_sync.strava.auth.webbrowser")
@patch("mi_fitness_sync.strava.auth.HTTPServer")
def test_run_oauth_flow_timeout(mock_server_cls: MagicMock, mock_browser: MagicMock) -> None:
    mock_server_cls.return_value = MagicMock()

    with patch("mi_fitness_sync.strava.auth._OAuthResult") as mock_result_cls:
        instance = MagicMock()
        instance.received.wait.return_value = False
        instance.received.is_set.return_value = False
        mock_result_cls.return_value = instance

        with pytest.raises(StravaAuthError, match="timed out"):
            run_oauth_flow("cid", "csecret", port=15478)


@patch("mi_fitness_sync.strava.auth.requests.post")
def test_exchange_token_non_json_body_raises(mock_post: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("no json")
    mock_post.return_value = mock_response

    with pytest.raises(StravaAuthError, match="not JSON"):
        exchange_token("123", "secret", "auth-code")


@patch("mi_fitness_sync.strava.auth.requests.post")
def test_exchange_token_non_object_body_raises(mock_post: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = ["access_token"]
    mock_post.return_value = mock_response

    with pytest.raises(StravaAuthError, match="not an object"):
        exchange_token("123", "secret", "auth-code")


@patch("mi_fitness_sync.strava.auth.requests.post")
def test_refresh_access_token_non_json_body_raises(mock_post: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("no json")
    mock_post.return_value = mock_response

    with pytest.raises(StravaAuthError, match="not JSON"):
        refresh_access_token("123", "secret", "refresh-xyz")


@patch("mi_fitness_sync.strava.auth.requests.post")
def test_refresh_access_token_non_object_body_raises(mock_post: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = 42
    mock_post.return_value = mock_response

    with pytest.raises(StravaAuthError, match="not an object"):
        refresh_access_token("123", "secret", "refresh-xyz")
