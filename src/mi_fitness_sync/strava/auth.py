from __future__ import annotations

import logging
import secrets
import threading
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import requests

from mi_fitness_sync.exceptions import StravaAuthError

logger = logging.getLogger(__name__)

STRAVA_AUTH_URL = "https://www.strava.com/oauth/authorize"
STRAVA_TOKEN_URL = "https://www.strava.com/oauth/token"  # noqa: S105 - a URL, not a credential
STRAVA_DEAUTHORIZE_URL = "https://www.strava.com/oauth/deauthorize"
REQUIRED_SCOPE = "activity:write,activity:read_all,read_all"


def _parse_token_response(response: requests.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError:
        msg = "Strava token endpoint returned a body that was not JSON."
        raise StravaAuthError(msg) from None
    if not isinstance(payload, dict):
        msg = "Strava token endpoint returned JSON that was not an object."
        raise StravaAuthError(msg)
    return payload


def build_authorization_url(client_id: str, redirect_uri: str, state: str) -> str:
    params = urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "approval_prompt": "auto",
            "scope": REQUIRED_SCOPE,
            "state": state,
        }
    )
    return f"{STRAVA_AUTH_URL}?{params}"


def exchange_token(client_id: str, client_secret: str, code: str) -> dict[str, Any]:
    response = requests.post(
        STRAVA_TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "grant_type": "authorization_code",
        },
        timeout=30,
    )
    if response.status_code != HTTPStatus.OK:
        msg = f"Token exchange failed (HTTP {response.status_code})."
        raise StravaAuthError(msg)
    return _parse_token_response(response)


def refresh_access_token(client_id: str, client_secret: str, refresh_token: str) -> dict[str, Any]:
    response = requests.post(
        STRAVA_TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        timeout=30,
    )
    if response.status_code != HTTPStatus.OK:
        msg = f"Token refresh failed (HTTP {response.status_code})."
        raise StravaAuthError(msg)
    return _parse_token_response(response)


def revoke_access_token(access_token: str) -> None:
    """Revoke a Strava access token via the deauthorize endpoint."""
    response = requests.post(
        STRAVA_DEAUTHORIZE_URL,
        data={"access_token": access_token},
        timeout=30,
    )
    if response.status_code != HTTPStatus.OK:
        msg = f"Token revocation failed (HTTP {response.status_code})."
        raise StravaAuthError(msg)


class _OAuthResult:
    __slots__ = ("code", "error", "expected_state", "received")

    def __init__(self, expected_state: str) -> None:
        self.code: str | None = None
        self.error: str | None = None
        self.expected_state = expected_state
        self.received = threading.Event()


def _make_callback_handler(result: _OAuthResult) -> type:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if not parsed.path.rstrip("/").endswith("/callback"):
                self._respond(404, "Not found.")
                return

            params = parse_qs(parsed.query)
            if "error" in params:
                result.error = params["error"][0]
                self._respond(200, "Authorization denied. You can close this window.")
                result.received.set()
                return

            code = params.get("code", [None])[0]
            state = params.get("state", [None])[0]

            if state != result.expected_state:
                result.error = "State mismatch. Possible CSRF attack."
                self._respond(400, "Authorization failed: state mismatch.")
                result.received.set()
                return

            if not code:
                result.error = "No authorization code received."
                self._respond(400, "Authorization failed: missing code.")
                result.received.set()
                return

            result.code = code
            self._respond(200, "Authorization successful! You can close this window.")
            result.received.set()

        def _respond(self, status: int, message: str) -> None:
            body = (
                f"<html><body><h2>{message}</h2>"
                "<p>Return to your terminal to continue.</p></body></html>"
            ).encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - the base class names it format
            logger.debug(format, *args)

    return Handler


def run_oauth_flow(client_id: str, client_secret: str, *, port: int = 5478) -> dict[str, Any]:
    """Drive the full OAuth2 authorization-code flow.

    Opens a browser for Strava authorization, starts a local HTTP callback
    server, exchanges the authorization code for tokens, and returns the
    raw Strava token response dict.
    """
    state = secrets.token_urlsafe(32)
    redirect_uri = f"http://localhost:{port}/callback"
    auth_url = build_authorization_url(client_id, redirect_uri, state)

    result = _OAuthResult(expected_state=state)
    handler_cls = _make_callback_handler(result)

    try:
        server = HTTPServer(("localhost", port), handler_cls)
    except OSError as exc:
        msg = f"Could not start OAuth callback server on port {port}: {exc}"
        raise StravaAuthError(msg) from exc

    server_thread = threading.Thread(target=server.handle_request, daemon=True)
    server_thread.start()

    print("Opening browser for Strava authorization...")
    print("If the browser does not open, visit this URL manually:")
    print(auth_url)
    webbrowser.open(auth_url)

    try:
        result.received.wait(timeout=120)
    finally:
        server.server_close()

    if not result.received.is_set():
        msg = "OAuth callback timed out after 120 seconds."
        raise StravaAuthError(msg)

    if result.error:
        msg = f"OAuth authorization failed: {result.error}"
        raise StravaAuthError(msg)

    if not result.code:
        msg = "No authorization code received."
        raise StravaAuthError(msg)

    return exchange_token(client_id, client_secret, result.code)
