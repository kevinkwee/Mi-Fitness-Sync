from __future__ import annotations

import base64
import hashlib
import json
import os
import struct
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import requests

from mi_fitness_sync.exceptions import MiFitnessError, XiaomiApiError
from mi_fitness_sync.region_mapping import region_for_country_code
from mi_fitness_sync.storage import AuthState


ACTIVITY_LIST_ENDPOINT = "https://hlth.io.mi.com/app/v1/data/get_sport_records_by_time"
REGION_BY_IP_ENDPOINT = "https://region.hlth.io.mi.com/app/v1/public/user_region_by_ip"
REGION_BY_IP_AUTH_KEY = "rwelJuWBFJxmbMKD"
DEFAULT_PAGE_SIZE = 50
DEFAULT_TIMEOUT_SECONDS = 30


def parse_cli_time(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.astimezone()
        return int(parsed.timestamp())


def format_terminal_time(timestamp: int | None) -> str:
    if not timestamp:
        return "-"
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def format_duration(seconds: int | None) -> str:
    if seconds is None:
        return "-"
    minutes, sec = divmod(max(seconds, 0), 60)
    hours, minute = divmod(minutes, 60)
    return f"{hours:02d}:{minute:02d}:{sec:02d}"


def format_distance_km(distance_meters: int | None) -> str:
    if distance_meters is None:
        return "-"
    return f"{distance_meters / 1000:.2f}"


def format_title(category: str | None, sport_type: int | None, report: dict[str, Any]) -> str:
    for key in ("course_name", "desc", "name", "title"):
        value = report.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    if isinstance(category, str) and category.strip():
        return category.replace("_", " ").replace("-", " ").title()
    if sport_type is not None:
        return f"Sport {sport_type}"
    return "Unknown"


def _b64decode(value: str) -> bytes:
    return base64.b64decode(value)


def _b64encode(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _signed_nonce(ssecurity: str, nonce: str) -> str:
    digest = hashlib.sha256(_b64decode(ssecurity) + _b64decode(nonce)).digest()
    return _b64encode(digest)


def _sha1_signature(parts: list[str]) -> str:
    return _b64encode(hashlib.sha1("&".join(parts).encode("utf-8")).digest())


class _Rc4Cipher:
    def __init__(self, key: bytes):
        if len(key) != 32:
            raise ValueError("RC4 key must be 32 bytes.")
        self._state = list(range(256))
        j = 0
        for index in range(256):
            j = (j + self._state[index] + key[index % len(key)]) & 0xFF
            self._state[index], self._state[j] = self._state[j], self._state[index]
        self._i = 0
        self._j = 0
        self.apply(bytes(1024))

    def apply(self, data: bytes) -> bytes:
        output = bytearray(data)
        for index, value in enumerate(output):
            self._i = (self._i + 1) & 0xFF
            state_i = self._state[self._i]
            self._j = (self._j + state_i) & 0xFF
            self._state[self._i], self._state[self._j] = self._state[self._j], self._state[self._i]
            output[index] = value ^ self._state[(self._state[self._i] + state_i) & 0xFF]
        return bytes(output)


@dataclass(slots=True)
class ActivityPage:
    activities: list["Activity"]
    has_more: bool
    next_key: str | None


@dataclass(slots=True)
class Activity:
    activity_id: str
    sid: str
    key: str
    category: str | None
    sport_type: int | None
    title: str
    start_time: int | None
    end_time: int | None
    duration_seconds: int | None
    distance_meters: int | None
    calories: int | None
    steps: int | None
    sync_state: str | None
    next_key: str | None
    raw_record: dict[str, Any]
    raw_report: dict[str, Any]

    def to_json_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["start_time_local"] = format_terminal_time(self.start_time)
        payload["end_time_local"] = format_terminal_time(self.end_time)
        payload["duration"] = format_duration(self.duration_seconds)
        payload["distance_km"] = None if self.distance_meters is None else round(self.distance_meters / 1000, 3)
        return payload


class MiFitnessActivitiesClient:
    def __init__(
        self,
        auth_state: AuthState,
        *,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        country_code: str | None = None,
    ):
        self._auth_state = auth_state
        self._timeout = timeout
        self._region_override = region_for_country_code(country_code)
        self._resolved_region: str | None = None
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "User-Agent": "Mi-Fitness-Sync/0.1",
            }
        )
        self._cookie_values = self._collect_cookie_values()

    def list_activities(
        self,
        *,
        start_time: int | None,
        end_time: int | None,
        limit: int,
        category: str | None = None,
    ) -> list[Activity]:
        activities: list[Activity] = []
        next_key: str | None = None

        while len(activities) < limit:
            page_limit = min(DEFAULT_PAGE_SIZE, limit - len(activities))
            page = self._fetch_activity_page(
                start_time=start_time,
                end_time=end_time,
                limit=page_limit,
                category=category,
                next_key=next_key,
            )
            activities.extend(page.activities)
            if not page.has_more or not page.next_key:
                break
            next_key = page.next_key

        return activities

    def _fetch_activity_page(
        self,
        *,
        start_time: int | None,
        end_time: int | None,
        limit: int,
        category: str | None,
        next_key: str | None,
    ) -> ActivityPage:
        request_payload: dict[str, Any] = {
            "reverse": True,
            "limit": limit,
        }
        if start_time is not None and start_time > 0:
            request_payload["startTime"] = start_time
        if end_time is not None and end_time > 0:
            request_payload["endTime"] = end_time
        if category:
            request_payload["category"] = category
        if next_key:
            request_payload["next_key"] = next_key

        nonce = self._generate_nonce(0)
        params = self._encrypt_query_params(
            method="GET",
            path="/app/v1/data/get_sport_records_by_time",
            params={"data": json.dumps(request_payload, separators=(",", ":"))},
            nonce=nonce,
            ssecurity=self._auth_state.ssecurity,
        )

        response = self._session.get(
            self._get_activity_list_endpoint(),
            params=params,
            headers=self._build_request_headers(),
            timeout=self._timeout,
        )

        if response.status_code == 401:
            raise XiaomiApiError("Mi Fitness activity request was rejected with 401 auth err.")
        if not response.ok:
            raise XiaomiApiError(
                f"Mi Fitness activity request failed with HTTP {response.status_code}.",
                payload={"response_text": response.text[:500]},
            )

        payload = self._decrypt_response_payload(response.text, nonce, self._auth_state.ssecurity)
        if payload.get("code") != 0:
            raise XiaomiApiError(
                payload.get("message") or "Mi Fitness activity API returned an error.",
                code=payload.get("code"),
                payload=payload,
            )

        result = payload.get("result") or {}
        raw_records = result.get("sport_records") or []
        activities = [self._parse_activity(record, result.get("next_key")) for record in raw_records]
        return ActivityPage(
            activities=activities,
            has_more=bool(result.get("has_more")),
            next_key=result.get("next_key") or None,
        )

    def _build_cookie_header(self) -> str:
        cookies = {
            "serviceToken": self._auth_state.service_token,
            "cUserId": self._auth_state.c_user_id,
        }
        for name in ("userId", "locale"):
            value = self._cookie_values.get(name)
            if value:
                cookies[name] = value
        return "; ".join(f"{name}={value}" for name, value in cookies.items())

    def _build_request_headers(self) -> dict[str, str]:
        headers = {"Cookie": self._build_cookie_header()}
        region = self._get_region()
        if region != "cn":
            headers["region_tag"] = region
        return headers

    def _get_activity_list_endpoint(self) -> str:
        region = self._get_region()
        if region == "cn":
            return ACTIVITY_LIST_ENDPOINT
        return ACTIVITY_LIST_ENDPOINT.replace("://", f"://{region}.", 1)

    def _get_region(self) -> str:
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
        self._resolved_region = self._normalize_region(region) or "cn"
        return self._resolved_region

    def _normalize_region(self, value: str | None) -> str | None:
        if not value:
            return None
        normalized = value.strip().lower()
        return normalized or None

    def _collect_cookie_values(self) -> dict[str, str]:
        cookie_values: dict[str, str] = {}
        for cookie in self._auth_state.cookies:
            name = cookie.get("name")
            value = cookie.get("value")
            if isinstance(name, str) and isinstance(value, str) and name not in cookie_values:
                cookie_values[name] = value

        if "locale" not in cookie_values:
            u_locale = cookie_values.get("uLocale")
            if u_locale:
                cookie_values["locale"] = u_locale

        user_id = cookie_values.get("userId")
        if not user_id and self._auth_state.user_id:
            cookie_values["userId"] = str(self._auth_state.user_id)

        return cookie_values

    def _decrypt_response_payload(self, body: str, nonce: str, ssecurity: str) -> dict[str, Any]:
        rc4_key = _b64decode(_signed_nonce(ssecurity, nonce))
        decrypted = _Rc4Cipher(rc4_key).apply(_b64decode(body)).decode("utf-8")
        return json.loads(decrypted)

    def _encrypt_query_params(
        self,
        *,
        method: str,
        path: str,
        params: dict[str, str],
        nonce: str,
        ssecurity: str,
    ) -> dict[str, str]:
        signed_nonce = _signed_nonce(ssecurity, nonce)
        signature_input = {key: value for key, value in params.items() if key and value}
        signature_input["rc4_hash__"] = self._build_signature(method, path, signature_input, signed_nonce)

        cipher = _Rc4Cipher(_b64decode(signed_nonce))
        encrypted_params = {
            key: _b64encode(cipher.apply(value.encode("utf-8")))
            for key, value in sorted(signature_input.items())
        }
        encrypted_params["signature"] = self._build_signature(method, path, encrypted_params, signed_nonce)
        encrypted_params["_nonce"] = nonce
        return encrypted_params

    def _build_signature(self, method: str, path: str, params: dict[str, str], signed_nonce: str) -> str:
        uri_path = urlparse(path).path
        parts = [method.upper(), uri_path]
        for key in sorted(params):
            parts.append(f"{key}={params[key]}")
        parts.append(signed_nonce)
        return _sha1_signature(parts)

    def _generate_nonce(self, time_diff_ms: int) -> str:
        payload = struct.pack(
            ">q",
            struct.unpack(">q", os.urandom(8))[0],
        )
        payload += struct.pack(">i", int((time.time() * 1000 + time_diff_ms) // 60000))
        return _b64encode(payload)

    def _parse_activity(self, record: dict[str, Any], next_key: str | None) -> Activity:
        raw_report = record.get("value")
        if isinstance(raw_report, str) and raw_report:
            try:
                report = json.loads(raw_report)
            except json.JSONDecodeError:
                report = {}
        else:
            report = {}

        sport_type = report.get("sport_type")
        if not isinstance(sport_type, int):
            sport_type = None

        start_time = report.get("start_time")
        if not isinstance(start_time, int):
            start_time = record.get("time") if isinstance(record.get("time"), int) else None

        end_time = report.get("end_time") if isinstance(report.get("end_time"), int) else None
        duration_seconds = report.get("duration") if isinstance(report.get("duration"), int) else None
        distance_meters = report.get("distance") if isinstance(report.get("distance"), int) else None
        calories = report.get("calories") if isinstance(report.get("calories"), int) else None
        steps = report.get("steps") if isinstance(report.get("steps"), int) else None

        sid = str(record.get("sid") or "")
        key = str(record.get("key") or "")
        time_value = record.get("time")
        if not isinstance(time_value, int):
            time_value = start_time or 0
        activity_id = f"{sid}:{key}:{time_value}"

        sync_state = None
        if record.get("deleted") is True:
            sync_state = "deleted"
        elif sid and key:
            sync_state = "server"

        category = record.get("category") if isinstance(record.get("category"), str) else None

        return Activity(
            activity_id=activity_id,
            sid=sid,
            key=key,
            category=category,
            sport_type=sport_type,
            title=format_title(category, sport_type, report),
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration_seconds,
            distance_meters=distance_meters,
            calories=calories,
            steps=steps,
            sync_state=sync_state,
            next_key=next_key,
            raw_record=record,
            raw_report=report,
        )


def render_activities_table(activities: list[Activity]) -> str:
    if not activities:
        return "No activities matched the requested time window."

    headers = [
        "ID",
        "Start",
        "Title",
        "Type",
        "Duration",
        "Km",
        "Cal",
        "Steps",
        "State",
    ]
    rows = []
    for activity in activities:
        rows.append(
            [
                activity.activity_id,
                format_terminal_time(activity.start_time),
                activity.title,
                "-" if activity.sport_type is None else str(activity.sport_type),
                format_duration(activity.duration_seconds),
                format_distance_km(activity.distance_meters),
                "-" if activity.calories is None else str(activity.calories),
                "-" if activity.steps is None else str(activity.steps),
                activity.sync_state or "-",
            ]
        )

    widths = [len(header) for header in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    def format_row(values: list[str]) -> str:
        return "  ".join(value.ljust(widths[index]) for index, value in enumerate(values))

    output = [format_row(headers), format_row(["-" * width for width in widths])]
    output.extend(format_row(row) for row in rows)
    return "\n".join(output)
