from __future__ import annotations

import logging
import time
from dataclasses import replace

import requests

from mi_fitness_sync.auth.state import utc_now_iso
from mi_fitness_sync.exceptions import StravaError
from mi_fitness_sync.strava.auth import refresh_access_token
from mi_fitness_sync.strava.store import StravaTokenState, save_tokens

logger = logging.getLogger(__name__)

STRAVA_UPLOADS_URL = "https://www.strava.com/api/v3/uploads"
_TOKEN_EXPIRY_MARGIN_SECONDS = 60
_POLL_INTERVAL_SECONDS = 2
_MAX_POLL_ATTEMPTS = 30


class StravaClient:
    def __init__(self, token_state: StravaTokenState, *, token_path: str | None = None):
        self._state = token_state
        self._token_path = token_path

    def _ensure_valid_token(self) -> None:
        if time.time() >= self._state.expires_at - _TOKEN_EXPIRY_MARGIN_SECONDS:
            self._refresh()

    def _refresh(self) -> None:
        logger.debug("Refreshing Strava access token")
        data = refresh_access_token(
            self._state.client_id,
            self._state.client_secret,
            self._state.refresh_token,
        )
        self._state = replace(
            self._state,
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            expires_at=data["expires_at"],
            updated_at=utc_now_iso(),
        )
        save_tokens(self._state, self._token_path)

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._state.access_token}"}

    def upload_activity(
        self,
        fit_bytes: bytes,
        *,
        sport_type: str | None = None,
        external_id: str | None = None,
    ) -> dict:
        """Upload a FIT file to Strava and poll until processing completes.

        Returns the final upload status dict which includes ``activity_id``
        on success.
        """
        self._ensure_valid_token()

        files = {"file": ("activity.fit", fit_bytes, "application/octet-stream")}
        data: dict[str, str] = {"data_type": "fit"}
        if sport_type:
            data["sport_type"] = sport_type
        if external_id:
            data["external_id"] = external_id

        response = requests.post(
            STRAVA_UPLOADS_URL,
            headers=self._auth_headers(),
            files=files,
            data=data,
            timeout=60,
        )
        if response.status_code not in (200, 201):
            raise StravaError(f"Upload failed (HTTP {response.status_code}): {response.text}")

        upload = response.json()
        upload_id = upload.get("id_str") or str(upload.get("id", ""))
        if not upload_id:
            raise StravaError("Upload response did not contain an upload ID.")

        return self._poll_upload(upload_id)

    def _poll_upload(self, upload_id: str) -> dict:
        for _ in range(_MAX_POLL_ATTEMPTS):
            time.sleep(_POLL_INTERVAL_SECONDS)
            self._ensure_valid_token()

            response = requests.get(
                f"{STRAVA_UPLOADS_URL}/{upload_id}",
                headers=self._auth_headers(),
                timeout=30,
            )
            if response.status_code != 200:
                raise StravaError(f"Upload status check failed (HTTP {response.status_code}).")

            status = response.json()
            error = status.get("error")
            activity_id = status.get("activity_id")

            if error:
                raise StravaError(f"Strava processing error: {error}")
            if activity_id is not None:
                return status

        raise StravaError("Upload processing timed out — activity may still be processing on Strava.")
