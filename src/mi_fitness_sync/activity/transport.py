from __future__ import annotations

import json
import logging
from typing import Any

import requests

from mi_fitness_sync.activity.crypto import decrypt_response_payload, encrypt_query_params, generate_nonce
from mi_fitness_sync.auth.state import AuthState
from mi_fitness_sync.config import USER_AGENT
from mi_fitness_sync.exceptions import XiaomiApiError
from mi_fitness_sync.activity.region_mapping import region_for_country_code


logger = logging.getLogger(__name__)

ACTIVITY_LIST_ENDPOINT = "https://hlth.io.mi.com/app/v1/data/get_sport_records_by_time"
FITNESS_DATA_TIME_ENDPOINT = "https://hlth.io.mi.com/app/v1/data/get_fitness_data_by_time"
FDS_DOWNLOAD_URL_ENDPOINT = "https://hlth.io.mi.com/healthapp/service/gen_download_url"
REGION_BY_IP_ENDPOINT = "https://region.hlth.io.mi.com/app/v1/public/user_region_by_ip"
REGION_BY_IP_AUTH_KEY = "rwelJuWBFJxmbMKD"


def collect_cookie_values(auth_state: AuthState) -> dict[str, str]:
    cookie_values: dict[str, str] = {}
    for cookie in auth_state.cookies:
        name = cookie.get("name")
        value = cookie.get("value")
        if isinstance(name, str) and isinstance(value, str) and name not in cookie_values:
            cookie_values[name] = value

    if "locale" not in cookie_values:
        u_locale = cookie_values.get("uLocale")
        if u_locale:
            cookie_values["locale"] = u_locale

    user_id = cookie_values.get("userId")
    if not user_id and auth_state.user_id:
        cookie_values["userId"] = str(auth_state.user_id)

    return cookie_values


class ActivityTransport:
    def __init__(
        self,
        session: requests.Session,
        auth_state: AuthState,
        *,
        timeout: int,
        country_code: str | None,
    ):
        self._session = session
        self._auth_state = auth_state
        self._timeout = timeout
        self._region_override = region_for_country_code(country_code)
        self._resolved_region: str | None = None
        self._cookie_values = collect_cookie_values(auth_state)
        self._session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "User-Agent": USER_AGENT,
            }
        )

    @property
    def cookie_values(self) -> dict[str, str]:
        return dict(self._cookie_values)

    def build_cookie_header(self) -> str:
        cookies = {
            "serviceToken": self._auth_state.service_token,
            "cUserId": self._auth_state.c_user_id,
        }
        for name in ("userId", "locale"):
            value = self._cookie_values.get(name)
            if value:
                cookies[name] = value
        return "; ".join(f"{name}={value}" for name, value in cookies.items())

    def build_request_headers(self) -> dict[str, str]:
        return {
            "Cookie": self.build_cookie_header(),
            "region_tag": self.get_region(),
        }

    def get_activity_list_endpoint(self) -> str:
        return self._regionalize_endpoint(ACTIVITY_LIST_ENDPOINT)

    def get_fitness_data_time_endpoint(self) -> str:
        return self._regionalize_endpoint(FITNESS_DATA_TIME_ENDPOINT)

    def get_fds_download_url_endpoint(self) -> str:
        return self._regionalize_endpoint(FDS_DOWNLOAD_URL_ENDPOINT)

    def get_region(self) -> str:
        if self._region_override:
            return self._region_override
        if self._resolved_region:
            return self._resolved_region

        try:
            response = self._session.get(
                REGION_BY_IP_ENDPOINT,
                headers={
                    "Cookie": f"auth_key={REGION_BY_IP_AUTH_KEY}",
                    "RegionTag": "ignore",
                },
                timeout=self._timeout,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError):
            self._resolved_region = "cn"
            return self._resolved_region

        result = payload.get("result") if isinstance(payload, dict) else None
        region = result.get("region") if isinstance(result, dict) else None
        self._resolved_region = self.normalize_region(region) or "cn"
        return self._resolved_region

    def request_json(
        self,
        *,
        endpoint: str,
        path: str,
        request_payload: dict[str, Any],
        request_label: str,
        signature_path: str | None = None,
    ) -> dict[str, Any]:
        nonce = generate_nonce(0)
        params = encrypt_query_params(
            method="GET",
            path=path,
            params={"data": json.dumps(request_payload, separators=(",", ":"))},
            nonce=nonce,
            ssecurity=self._auth_state.ssecurity,
            signature_path=signature_path,
        )

        response = self._session.get(
            endpoint,
            params=params,
            headers=self.build_request_headers(),
            timeout=self._timeout,
        )

        if response.status_code == 401:
            raise XiaomiApiError(f"{request_label} was rejected with 401 auth err.")
        if not response.ok:
            raise XiaomiApiError(
                f"{request_label} failed with HTTP {response.status_code}.",
                payload={"response_text": response.text[:500]},
            )

        payload = decrypt_response_payload(response.text, nonce, self._auth_state.ssecurity)
        if payload.get("code") != 0:
            raise XiaomiApiError(
                payload.get("message") or f"{request_label} API returned an error.",
                code=payload.get("code"),
                payload=payload,
            )
        return payload

    @staticmethod
    def normalize_region(value: str | None) -> str | None:
        if not value:
            return None
        normalized = value.strip().lower()
        return normalized or None

    def _regionalize_endpoint(self, endpoint: str) -> str:
        region = self.get_region()
        if region == "cn":
            return endpoint
        return endpoint.replace("://", f"://{region}.", 1)