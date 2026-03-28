from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import struct
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import requests

from mi_fitness_sync.exceptions import MiFitnessError, XiaomiApiError
from mi_fitness_sync.fds_parser import (
    RecoveryRateData,
    SportReport,
    download_and_parse_gps_record,
    download_and_parse_recovery_rate,
    download_and_parse_sport_record,
    download_and_parse_sport_report,
)
from mi_fitness_sync.region_mapping import region_for_country_code
from mi_fitness_sync.storage import AuthState


logger = logging.getLogger(__name__)

ACTIVITY_LIST_ENDPOINT = "https://hlth.io.mi.com/app/v1/data/get_sport_records_by_time"
FITNESS_DATA_TIME_ENDPOINT = "https://hlth.io.mi.com/app/v1/data/get_fitness_data_by_time"
FDS_DOWNLOAD_URL_ENDPOINT = "https://hlth.io.mi.com/healthapp/service/gen_download_url"
REGION_BY_IP_ENDPOINT = "https://region.hlth.io.mi.com/app/v1/public/user_region_by_ip"
REGION_BY_IP_AUTH_KEY = "rwelJuWBFJxmbMKD"
DEFAULT_PAGE_SIZE = 50
DEFAULT_TIMEOUT_SECONDS = 30
DETAIL_DATA_KEY = "huami_sport_record"
ACTIVITY_ID_SEARCH_WINDOW_SECONDS = 86400
FDS_SPORT_RECORD_FILE_TYPE = 0
FDS_SPORT_REPORT_FILE_TYPE = 1
FDS_GPS_FILE_TYPE = 2
FDS_RECOVERY_RATE_FILE_TYPE = 3


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
class FitnessDataPage:
    items: list[dict[str, Any]]
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


@dataclass(slots=True)
class ActivitySample:
    timestamp: int
    start_time: int | None
    end_time: int | None
    duration_seconds: int | None
    heart_rate: int | None
    cadence: int | None
    speed_mps: float | None
    distance_meters: float | None
    altitude_meters: float | None
    steps: int | None
    calories: int | None
    raw_sample: dict[str, Any]

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "timestamp_local": format_terminal_time(self.timestamp),
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds,
            "heart_rate": self.heart_rate,
            "cadence": self.cadence,
            "speed_mps": self.speed_mps,
            "distance_meters": self.distance_meters,
            "altitude_meters": self.altitude_meters,
            "steps": self.steps,
            "calories": self.calories,
            "raw_sample": self.raw_sample,
        }


@dataclass(slots=True)
class TrackPoint:
    timestamp: int
    latitude: float | None
    longitude: float | None
    altitude_meters: float | None
    speed_mps: float | None
    distance_meters: float | None
    heart_rate: int | None
    cadence: int | None
    raw_point: dict[str, Any]

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "timestamp_local": format_terminal_time(self.timestamp),
            "latitude": self.latitude,
            "longitude": self.longitude,
            "altitude_meters": self.altitude_meters,
            "speed_mps": self.speed_mps,
            "distance_meters": self.distance_meters,
            "heart_rate": self.heart_rate,
            "cadence": self.cadence,
            "raw_point": self.raw_point,
        }


@dataclass(slots=True)
class ActivityDetail:
    activity: Activity
    detail_sid: str
    detail_key: str
    detail_time: int
    zone_name: str | None
    zone_offset_seconds: int | None
    track_points: list[TrackPoint]
    samples: list[ActivitySample]
    sport_report: SportReport | None
    recovery_rate: RecoveryRateData | None
    raw_fitness_item: dict[str, Any]
    raw_detail: dict[str, Any]

    @property
    def start_time(self) -> int:
        return self.activity.start_time or self.detail_time

    @property
    def end_time(self) -> int:
        if self.activity.end_time is not None:
            return self.activity.end_time
        if self.samples:
            return self.samples[-1].timestamp
        if self.track_points:
            return self.track_points[-1].timestamp
        return self.detail_time

    @property
    def total_duration_seconds(self) -> int:
        if self.activity.duration_seconds is not None:
            return self.activity.duration_seconds
        return max(self.end_time - self.start_time, 0)

    @property
    def total_distance_meters(self) -> float:
        if self.activity.distance_meters is not None:
            return float(self.activity.distance_meters)
        distances = [point.distance_meters for point in self.track_points if point.distance_meters is not None]
        if distances:
            return max(distances)
        sample_distances = [sample.distance_meters for sample in self.samples if sample.distance_meters is not None]
        if sample_distances:
            return max(sample_distances)
        return 0.0

    @property
    def total_calories(self) -> int | None:
        if self.activity.calories is not None:
            return self.activity.calories
        sample_calories = [sample.calories for sample in self.samples if sample.calories is not None]
        if sample_calories:
            return max(sample_calories)
        return None

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "activity": self.activity.to_json_dict(),
            "detail_sid": self.detail_sid,
            "detail_key": self.detail_key,
            "detail_time": self.detail_time,
            "zone_name": self.zone_name,
            "zone_offset_seconds": self.zone_offset_seconds,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.total_duration_seconds,
            "distance_meters": self.total_distance_meters,
            "calories": self.total_calories,
            "track_points": [point.to_json_dict() for point in self.track_points],
            "samples": [sample.to_json_dict() for sample in self.samples],
            "sport_report": asdict(self.sport_report) if self.sport_report else None,
            "recovery_rate": asdict(self.recovery_rate) if self.recovery_rate else None,
            "raw_fitness_item": self.raw_fitness_item,
            "raw_detail": self.raw_detail,
        }


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

    def get_activity_by_id(
        self,
        activity_id: str,
        *,
        search_window_seconds: int = ACTIVITY_ID_SEARCH_WINDOW_SECONDS,
    ) -> Activity:
        sid, key, time_value = parse_activity_id(activity_id)
        start_time = max(time_value - search_window_seconds, 0)
        end_time = time_value + search_window_seconds
        next_key: str | None = None

        while True:
            page = self._fetch_activity_page(
                start_time=start_time,
                end_time=end_time,
                limit=DEFAULT_PAGE_SIZE,
                category=None,
                next_key=next_key,
            )
            for activity in page.activities:
                if activity.activity_id == activity_id:
                    return activity
            if not page.has_more or not page.next_key:
                break
            next_key = page.next_key

        raise MiFitnessError(f"Could not find activity {activity_id} in Mi Fitness for the surrounding time window.")

    def get_activity_detail(self, activity_or_id: Activity | str) -> ActivityDetail:
        activity = activity_or_id if isinstance(activity_or_id, Activity) else self.get_activity_by_id(activity_or_id)
        logger.debug("get_activity_detail: resolved activity %s (sid=%s, key=%s, start_time=%s)",
                     activity.activity_id, activity.sid, activity.key, activity.start_time)

        fds_downloads = self._try_get_fds_download_map(activity)
        fds_samples = self._try_download_fds_sport_samples(activity, fds_downloads)
        fds_track_points = self._try_download_fds_gps_track_points(activity, fds_downloads)
        fds_sport_report = self._try_download_fds_sport_report(activity, fds_downloads)
        fds_recovery_rate = self._try_download_fds_recovery_rate(activity, fds_downloads)

        # Merge FDS sport sample HR/cadence into GPS track points by timestamp
        if fds_track_points and fds_samples:
            _merge_fds_samples_into_track_points(fds_track_points, fds_samples)

        fitness_item = self._get_activity_detail_item(activity)
        if fitness_item:
            logger.debug("get_activity_detail: using JSON detail item (sid=%s, key=%s, time=%s)",
                         fitness_item.get("sid"), fitness_item.get("key"), fitness_item.get("time"))
            detail = self._build_activity_detail_from_item(activity, fitness_item, fds_downloads)
            if fds_samples:
                detail.samples = fds_samples
            # Prefer FDS GPS track points (per-second, higher quality)
            if fds_track_points:
                detail.track_points = fds_track_points
            if fds_sport_report:
                detail.sport_report = fds_sport_report
            if fds_recovery_rate:
                detail.recovery_rate = fds_recovery_rate
            return detail

        if fds_samples or fds_track_points:
            logger.debug("get_activity_detail: no JSON detail found, using FDS-only data "
                         "(%d samples, %d track points)", len(fds_samples), len(fds_track_points))
            return ActivityDetail(
                activity=activity,
                detail_sid=activity.sid,
                detail_key="fds_sport_record",
                detail_time=activity.start_time or activity.raw_record.get("time") or 0,
                zone_name=_coerce_str(activity.raw_record.get("zone_name")),
                zone_offset_seconds=_coerce_int(activity.raw_record.get("zone_offset")),
                track_points=fds_track_points,
                samples=fds_samples,
                sport_report=fds_sport_report,
                recovery_rate=fds_recovery_rate,
                raw_fitness_item={"source": "fds_sport_record"},
                raw_detail={"source": "fds_sport_record", "fds_downloads": fds_downloads},
            )

        logger.warning("get_activity_detail: no detail data found for %s — "
                       "fds_downloads had %d entries, JSON detail item was empty, "
                       "FDS sport samples=%d, FDS GPS points=%d",
                       activity.activity_id, len(fds_downloads),
                       len(fds_samples), len(fds_track_points))
        raise MiFitnessError(
            f"Could not find a detail payload for activity {activity.activity_id} in Mi Fitness. "
            "The workout summary exists, but neither the JSON detail nor FDS binary data was available."
        )

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
        region = self._get_region()
        headers = {
            "Cookie": self._build_cookie_header(),
            "region_tag": region,
        }
        return headers

    def _get_activity_list_endpoint(self) -> str:
        region = self._get_region()
        if region == "cn":
            return ACTIVITY_LIST_ENDPOINT
        return ACTIVITY_LIST_ENDPOINT.replace("://", f"://{region}.", 1)

    def _get_fitness_data_time_endpoint(self) -> str:
        region = self._get_region()
        if region == "cn":
            return FITNESS_DATA_TIME_ENDPOINT
        return FITNESS_DATA_TIME_ENDPOINT.replace("://", f"://{region}.", 1)

    def _get_fds_download_url_endpoint(self) -> str:
        region = self._get_region()
        if region == "cn":
            return FDS_DOWNLOAD_URL_ENDPOINT
        return FDS_DOWNLOAD_URL_ENDPOINT.replace("://", f"://{region}.", 1)

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
        signature_path: str | None = None,
    ) -> dict[str, str]:
        signed_nonce = _signed_nonce(ssecurity, nonce)
        signature_input = {key: value for key, value in params.items() if key and value}
        signature_input["rc4_hash__"] = self._build_signature(method, signature_path or path, signature_input, signed_nonce)

        cipher = _Rc4Cipher(_b64decode(signed_nonce))
        encrypted_params = {
            key: _b64encode(cipher.apply(value.encode("utf-8")))
            for key, value in sorted(signature_input.items())
        }
        encrypted_params["signature"] = self._build_signature(method, signature_path or path, encrypted_params, signed_nonce)
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
        payload = os.urandom(8)
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

    def _get_activity_detail_item(self, activity: Activity) -> dict[str, Any]:
        start_time = activity.start_time
        if start_time is None:
            raise MiFitnessError(f"Activity {activity.activity_id} does not have a start_time, so detail retrieval cannot be bounded.")

        end_time = activity.end_time
        if end_time is None:
            if activity.duration_seconds is not None:
                end_time = start_time + activity.duration_seconds
            else:
                end_time = start_time + ACTIVITY_ID_SEARCH_WINDOW_SECONDS

        record_time = activity.raw_record.get("time")
        logger.debug("_get_activity_detail_item: querying key=%s, start_time=%s, end_time=%s, "
                     "record_time=%s for %s",
                     DETAIL_DATA_KEY, start_time, end_time, record_time, activity.activity_id)

        total_items = 0
        next_key: str | None = None
        while True:
            page = self._fetch_fitness_data_page(
                key=DETAIL_DATA_KEY,
                start_time=start_time,
                end_time=end_time,
                next_key=next_key,
            )
            total_items += len(page.items)
            for item in page.items:
                if (
                    str(item.get("sid") or "") == activity.sid
                    and str(item.get("key") or "") == activity.key
                    and (record_time is None or item.get("time") == record_time)
                ):
                    logger.debug("_get_activity_detail_item: matched item (sid=%s, key=%s, time=%s) "
                                 "after scanning %d items",
                                 item.get("sid"), item.get("key"), item.get("time"), total_items)
                    return item
            if not page.has_more or not page.next_key:
                break
            next_key = page.next_key

        logger.debug("_get_activity_detail_item: no match found among %d items for %s "
                     "(looking for sid=%s, key=%s, record_time=%s)",
                     total_items, activity.activity_id, activity.sid, activity.key, record_time)
        return {}

    def _build_activity_detail_from_item(
        self,
        activity: Activity,
        fitness_item: dict[str, Any],
        fds_downloads: dict[str, dict[str, Any]],
    ) -> ActivityDetail:
        raw_value = fitness_item.get("value")
        if not isinstance(raw_value, str) or not raw_value.strip():
            raise MiFitnessError(f"Mi Fitness detail payload for {activity.activity_id} did not include a usable value blob.")

        try:
            raw_detail = json.loads(raw_value)
        except json.JSONDecodeError as exc:
            raise MiFitnessError(f"Mi Fitness detail payload for {activity.activity_id} was not valid JSON: {exc}.") from exc

        if isinstance(raw_detail, dict) and fds_downloads:
            raw_detail = {**raw_detail, "fds_downloads": fds_downloads}

        track_points = _extract_track_points(raw_detail)
        samples = _extract_activity_samples(raw_detail)
        _merge_samples_into_track_points(track_points, samples)

        detail_time = fitness_item.get("time") if isinstance(fitness_item.get("time"), int) else activity.start_time or 0
        zone_offset_seconds = fitness_item.get("zone_offset") if isinstance(fitness_item.get("zone_offset"), int) else None
        zone_name = fitness_item.get("zone_name") if isinstance(fitness_item.get("zone_name"), str) else None

        return ActivityDetail(
            activity=activity,
            detail_sid=str(fitness_item.get("sid") or activity.sid),
            detail_key=str(fitness_item.get("key") or activity.key),
            detail_time=detail_time,
            zone_name=zone_name,
            zone_offset_seconds=zone_offset_seconds,
            track_points=track_points,
            samples=samples,
            sport_report=None,
            recovery_rate=None,
            raw_fitness_item=fitness_item,
            raw_detail=raw_detail,
        )

    def _try_get_fds_download_map(self, activity: Activity) -> dict[str, dict[str, Any]]:
        try:
            result = self._get_fds_download_map(activity)
            logger.debug("_try_get_fds_download_map: got %d entries for %s — keys: %s",
                         len(result), activity.activity_id, list(result.keys()))
            return result
        except (MiFitnessError, XiaomiApiError, requests.RequestException, ValueError) as exc:
            logger.warning("_try_get_fds_download_map: FDS metadata request failed for %s: %s",
                           activity.activity_id, exc)
            return {}

    def _try_download_fds_sport_samples(
        self, activity: Activity, fds_downloads: dict[str, dict[str, Any]],
    ) -> list[ActivitySample]:
        """Attempt to download, decrypt, and parse the FDS sport record binary.

        Returns parsed per-second ActivitySamples, or an empty list on failure.
        """
        if not fds_downloads:
            logger.debug("_try_download_fds_sport_samples: skipped — no FDS downloads for %s",
                         activity.activity_id)
            return []

        proto_type = _coerce_int(activity.raw_report.get("proto_type"))
        timestamp = _coerce_int(activity.raw_record.get("time")) or activity.start_time
        timezone_offset = _coerce_int(activity.raw_report.get("timezone"))
        if proto_type is None or timestamp is None or timezone_offset is None or not activity.sid:
            logger.debug("_try_download_fds_sport_samples: missing fields for %s — "
                         "proto_type=%s, timestamp=%s, timezone_offset=%s, sid=%s",
                         activity.activity_id, proto_type, timestamp, timezone_offset, activity.sid)
            return []

        record_suffix = _build_fds_suffix(
            sid=activity.sid,
            timestamp=timestamp,
            timezone_offset=timezone_offset,
            sport_type=proto_type,
            file_type=FDS_SPORT_RECORD_FILE_TYPE,
        )
        logger.debug("_try_download_fds_sport_samples: computed suffix=%s for %s",
                     record_suffix, activity.activity_id)

        fds_entry = _find_fds_entry(fds_downloads, record_suffix, timestamp)
        if fds_entry is None:
            logger.debug("_try_download_fds_sport_samples: no FDS entry matched key '%s_%s' in %s",
                         record_suffix, timestamp, list(fds_downloads.keys()))
            return []

        try:
            sport_samples = download_and_parse_sport_record(
                self._session, fds_entry, proto_type, timeout=self._timeout,
            )
        except Exception:
            logger.warning("_try_download_fds_sport_samples: download/parse failed for %s",
                           activity.activity_id, exc_info=True)
            return []
        logger.debug("_try_download_fds_sport_samples: parsed %d samples for %s",
                     len(sport_samples), activity.activity_id)

        return [
            ActivitySample(
                timestamp=s.timestamp,
                start_time=s.timestamp,
                end_time=s.timestamp,
                duration_seconds=1,
                heart_rate=s.heart_rate,
                cadence=s.cadence,
                speed_mps=None,
                distance_meters=float(s.distance) if s.distance is not None else None,
                altitude_meters=None,
                steps=s.steps,
                calories=s.calories,
                raw_sample={"source": "fds_sport_record"},
            )
            for s in sport_samples
        ]

    def _try_download_fds_sport_report(
        self, activity: Activity, fds_downloads: dict[str, dict[str, Any]],
    ) -> SportReport | None:
        """Attempt to download, decrypt, and parse the FDS sport report binary.

        Returns a SportReport, or None on failure.
        """
        if not fds_downloads:
            return None

        proto_type = _coerce_int(activity.raw_report.get("proto_type"))
        timestamp = _coerce_int(activity.raw_record.get("time")) or activity.start_time
        timezone_offset = _coerce_int(activity.raw_report.get("timezone"))
        if proto_type is None or timestamp is None or timezone_offset is None or not activity.sid:
            logger.debug("_try_download_fds_sport_report: missing fields for %s", activity.activity_id)
            return None

        report_suffix = _build_fds_suffix(
            sid=activity.sid,
            timestamp=timestamp,
            timezone_offset=timezone_offset,
            sport_type=proto_type,
            file_type=FDS_SPORT_REPORT_FILE_TYPE,
        )

        fds_entry = _find_fds_entry(fds_downloads, report_suffix, timestamp)
        if fds_entry is None:
            logger.debug("_try_download_fds_sport_report: no FDS entry for suffix=%s in %s",
                         report_suffix, list(fds_downloads.keys()))
            return None

        try:
            report = download_and_parse_sport_report(
                self._session, fds_entry, proto_type, timeout=self._timeout,
            )
        except Exception:
            logger.warning("_try_download_fds_sport_report: download/parse failed for %s",
                           activity.activity_id, exc_info=True)
            return None

        if report:
            logger.debug("_try_download_fds_sport_report: parsed report for %s "
                         "(calories=%s, avg_hr=%s, max_hr=%s, distance=%s)",
                         activity.activity_id, report.calories, report.avg_hr,
                         report.max_hr, report.distance)
        return report

    def _try_download_fds_gps_track_points(
        self, activity: Activity, fds_downloads: dict[str, dict[str, Any]],
    ) -> list[TrackPoint]:
        """Attempt to download, decrypt, and parse the FDS GPS binary.

        Returns GPS track points, or an empty list on failure.
        """
        if not fds_downloads:
            logger.debug("_try_download_fds_gps_track_points: skipped — no FDS downloads for %s",
                         activity.activity_id)
            return []

        proto_type = _coerce_int(activity.raw_report.get("proto_type"))
        timestamp = _coerce_int(activity.raw_record.get("time")) or activity.start_time
        timezone_offset = _coerce_int(activity.raw_report.get("timezone"))
        if proto_type is None or timestamp is None or timezone_offset is None or not activity.sid:
            logger.debug("_try_download_fds_gps_track_points: missing fields for %s — "
                         "proto_type=%s, timestamp=%s, timezone_offset=%s, sid=%s",
                         activity.activity_id, proto_type, timestamp, timezone_offset, activity.sid)
            return []

        gps_suffix = _build_fds_suffix(
            sid=activity.sid,
            timestamp=timestamp,
            timezone_offset=timezone_offset,
            sport_type=proto_type,
            file_type=FDS_GPS_FILE_TYPE,
        )
        logger.debug("_try_download_fds_gps_track_points: computed suffix=%s for %s",
                     gps_suffix, activity.activity_id)

        fds_entry = _find_fds_entry(fds_downloads, gps_suffix, timestamp)
        if fds_entry is None:
            logger.debug("_try_download_fds_gps_track_points: no FDS entry matched key '%s_%s' in %s",
                         gps_suffix, timestamp, list(fds_downloads.keys()))
            return []

        try:
            gps_samples = download_and_parse_gps_record(
                self._session, fds_entry, timeout=self._timeout,
            )
        except Exception:
            logger.warning("_try_download_fds_gps_track_points: download/parse failed for %s",
                           activity.activity_id, exc_info=True)
            return []
        logger.debug("_try_download_fds_gps_track_points: parsed %d track points for %s",
                     len(gps_samples), activity.activity_id)

        return [
            TrackPoint(
                timestamp=g.timestamp,
                latitude=g.latitude,
                longitude=g.longitude,
                altitude_meters=g.altitude,
                speed_mps=g.speed,
                distance_meters=None,
                heart_rate=None,
                cadence=None,
                raw_point={"source": "fds_gps"},
            )
            for g in gps_samples
        ]

    def _try_download_fds_recovery_rate(
        self, activity: Activity, fds_downloads: dict[str, dict[str, Any]],
    ) -> RecoveryRateData | None:
        """Attempt to download, decrypt, and parse the FDS recovery rate binary (fileType=3).

        Returns a RecoveryRateData, or None on failure.
        """
        if not fds_downloads:
            return None

        proto_type = _coerce_int(activity.raw_report.get("proto_type"))
        timestamp = _coerce_int(activity.raw_record.get("time")) or activity.start_time
        timezone_offset = _coerce_int(activity.raw_report.get("timezone"))
        if proto_type is None or timestamp is None or timezone_offset is None or not activity.sid:
            logger.debug("_try_download_fds_recovery_rate: missing fields for %s", activity.activity_id)
            return None

        recovery_suffix = _build_fds_suffix(
            sid=activity.sid,
            timestamp=timestamp,
            timezone_offset=timezone_offset,
            sport_type=proto_type,
            file_type=FDS_RECOVERY_RATE_FILE_TYPE,
        )

        fds_entry = _find_fds_entry(fds_downloads, recovery_suffix, timestamp)
        if fds_entry is None:
            logger.debug("_try_download_fds_recovery_rate: no FDS entry for suffix=%s in %s",
                         recovery_suffix, list(fds_downloads.keys()))
            return None

        try:
            data = download_and_parse_recovery_rate(
                self._session, fds_entry, timeout=self._timeout,
            )
        except Exception:
            logger.warning("_try_download_fds_recovery_rate: download/parse failed for %s",
                           activity.activity_id, exc_info=True)
            return None

        if data:
            logger.debug("_try_download_fds_recovery_rate: parsed recovery rate for %s "
                         "(heartRate=%d, recoverRate=%.1f, samples=%d)",
                         activity.activity_id, data.heart_rate,
                         data.recover_rate, len(data.rate_samples))
        return data

    def _get_fds_download_map(self, activity: Activity) -> dict[str, dict[str, Any]]:
        timestamp = _coerce_int(activity.raw_record.get("time")) or activity.start_time
        proto_type = _coerce_int(activity.raw_report.get("proto_type"))
        timezone_offset = _coerce_int(activity.raw_report.get("timezone"))
        if timestamp is None or proto_type is None or timezone_offset is None or not activity.sid:
            return {}

        request_items = [
            self._build_fds_request_item(activity.sid, timestamp, timezone_offset, proto_type, FDS_SPORT_RECORD_FILE_TYPE),
            self._build_fds_request_item(activity.sid, timestamp, timezone_offset, proto_type, FDS_SPORT_REPORT_FILE_TYPE),
            self._build_fds_request_item(activity.sid, timestamp, timezone_offset, proto_type, FDS_GPS_FILE_TYPE),
            self._build_fds_request_item(activity.sid, timestamp, timezone_offset, proto_type, FDS_RECOVERY_RATE_FILE_TYPE),
        ]
        request_payload = {"did": activity.sid, "items": request_items}
        nonce = self._generate_nonce(0)
        params = self._encrypt_query_params(
            method="GET",
            path="/healthapp/service/gen_download_url",
            signature_path="/service/gen_download_url",
            params={"data": json.dumps(request_payload, separators=(",", ":"))},
            nonce=nonce,
            ssecurity=self._auth_state.ssecurity,
        )

        response = self._session.get(
            self._get_fds_download_url_endpoint(),
            params=params,
            headers=self._build_request_headers(),
            timeout=self._timeout,
        )

        if response.status_code == 401:
            raise XiaomiApiError("Mi Fitness FDS metadata request was rejected with 401 auth err.")
        if not response.ok:
            raise XiaomiApiError(
                f"Mi Fitness FDS metadata request failed with HTTP {response.status_code}.",
                payload={"response_text": response.text[:500]},
            )

        payload = self._decrypt_response_payload(response.text, nonce, self._auth_state.ssecurity)
        if payload.get("code") != 0:
            raise XiaomiApiError(
                payload.get("message") or "Mi Fitness FDS metadata API returned an error.",
                code=payload.get("code"),
                payload=payload,
            )

        result = payload.get("result")
        logger.debug("_get_fds_download_map: raw decrypted payload keys=%s, result=%s",
                     list(payload.keys()), result)
        if not isinstance(result, dict):
            return {}
        return {
            key: value
            for key, value in result.items()
            if isinstance(key, str) and isinstance(value, dict)
        }

    def _build_fds_request_item(
        self,
        sid: str,
        timestamp: int,
        timezone_offset: int,
        sport_type: int,
        file_type: int,
    ) -> dict[str, Any]:
        suffix = _build_fds_suffix(
            sid=sid,
            timestamp=timestamp,
            timezone_offset=timezone_offset,
            sport_type=sport_type,
            file_type=file_type,
        )
        return {"timestamp": timestamp, "suffix": suffix}

    def _fetch_fitness_data_page(
        self,
        *,
        key: str,
        start_time: int | None,
        end_time: int | None,
        next_key: str | None,
    ) -> FitnessDataPage:
        request_payload: dict[str, Any] = {"key": key, "reverse": True}
        if start_time is not None and start_time > 0:
            request_payload["startTime"] = start_time
        if end_time is not None and end_time > 0:
            request_payload["endTime"] = end_time
        if next_key:
            request_payload["next_key"] = next_key

        nonce = self._generate_nonce(0)
        params = self._encrypt_query_params(
            method="GET",
            path="/app/v1/data/get_fitness_data_by_time",
            params={"data": json.dumps(request_payload, separators=(",", ":"))},
            nonce=nonce,
            ssecurity=self._auth_state.ssecurity,
        )

        response = self._session.get(
            self._get_fitness_data_time_endpoint(),
            params=params,
            headers=self._build_request_headers(),
            timeout=self._timeout,
        )

        if response.status_code == 401:
            raise XiaomiApiError("Mi Fitness activity detail request was rejected with 401 auth err.")
        if not response.ok:
            raise XiaomiApiError(
                f"Mi Fitness activity detail request failed with HTTP {response.status_code}.",
                payload={"response_text": response.text[:500]},
            )

        payload = self._decrypt_response_payload(response.text, nonce, self._auth_state.ssecurity)
        if payload.get("code") != 0:
            raise XiaomiApiError(
                payload.get("message") or "Mi Fitness activity detail API returned an error.",
                code=payload.get("code"),
                payload=payload,
            )

        result = payload.get("result") or {}
        raw_items = result.get("data_list") or []
        items = [item for item in raw_items if isinstance(item, dict)]
        return FitnessDataPage(items=items, has_more=bool(result.get("has_more")), next_key=result.get("next_key") or None)


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


def parse_activity_id(value: str) -> tuple[str, str, int]:
    sid, separator, remainder = value.partition(":")
    if not separator:
        raise MiFitnessError(
            "Activity IDs must use the list-activities format sid:key:time."
        )
    key, separator, time_value = remainder.rpartition(":")
    if not separator or not sid or not key:
        raise MiFitnessError(
            "Activity IDs must use the list-activities format sid:key:time."
        )
    try:
        return sid, key, int(time_value)
    except ValueError as exc:
        raise MiFitnessError(
            "Activity IDs must use the list-activities format sid:key:time."
        ) from exc


def _extract_track_points(payload: Any) -> list[TrackPoint]:
    """Extract GPS track points from the huami_sport_record value.

    The payload is a dict whose top-level values are lists of per-second
    records.  GPS points live in lists where items carry ``latitude``,
    ``longitude``, and a timestamp (``timestamp`` or ``time``).
    """
    if not isinstance(payload, dict):
        return []

    track_points: list[TrackPoint] = []
    for value in payload.values():
        if not isinstance(value, list):
            continue
        for point in value:
            if not isinstance(point, dict):
                continue
            if "latitude" not in point or "longitude" not in point:
                continue
            timestamp = _coerce_int(point.get("timestamp"))
            if timestamp is None:
                timestamp = _coerce_int(point.get("time"))
            latitude = _coerce_float(point.get("latitude"))
            longitude = _coerce_float(point.get("longitude"))
            if timestamp is None or latitude is None or longitude is None:
                continue
            track_points.append(
                TrackPoint(
                    timestamp=timestamp,
                    latitude=latitude,
                    longitude=longitude,
                    altitude_meters=_coerce_float(point.get("altitude")),
                    speed_mps=_coerce_float(point.get("speed") or point.get("locationSpeed")),
                    distance_meters=None,
                    heart_rate=None,
                    cadence=None,
                    raw_point=point,
                )
            )
    track_points.sort(key=lambda point: point.timestamp)
    return _dedupe_track_points(track_points)


def _extract_activity_samples(payload: Any) -> list[ActivitySample]:
    """Extract per-second activity samples from the huami_sport_record value.

    The payload is a dict whose top-level values are lists of per-second
    records.  Sample records carry timing fields (``startTime``/``endTime``
    or ``start_time``/``end_time``) and metric fields (HR, distance, etc.).
    """
    if not isinstance(payload, dict):
        return []

    samples: list[ActivitySample] = []
    for value in payload.values():
        if not isinstance(value, list):
            continue
        for sample in value:
            if not isinstance(sample, dict):
                continue
            start_time = _coerce_int(sample.get("startTime") or sample.get("start_time"))
            end_time = _coerce_int(sample.get("endTime") or sample.get("end_time"))
            timestamp = end_time or start_time
            if timestamp is None:
                continue
            duration_seconds = _coerce_int(sample.get("duration"))
            if duration_seconds is None and start_time is not None and end_time is not None:
                duration_seconds = max(end_time - start_time, 0)
            samples.append(
                ActivitySample(
                    timestamp=timestamp,
                    start_time=start_time,
                    end_time=end_time,
                    duration_seconds=duration_seconds,
                    heart_rate=_coerce_int(sample.get("hr") or sample.get("heartRate")),
                    cadence=_coerce_int(
                        sample.get("cadence")
                        or sample.get("cycleCadence")
                        or sample.get("jumpFrequency")
                        or sample.get("rowingCadence")
                    ),
                    speed_mps=_coerce_float(sample.get("speed") or sample.get("avgSpeed") or sample.get("locationSpeed")),
                    distance_meters=_coerce_float(sample.get("distance") or sample.get("newDistance") or sample.get("runDistance")),
                    altitude_meters=_coerce_float(sample.get("altitude") or sample.get("height")),
                    steps=_coerce_int(sample.get("steps") or sample.get("newSteps") or sample.get("totalSteps")),
                    calories=_coerce_int(sample.get("calories") or sample.get("newCalories") or sample.get("activeCalories")),
                    raw_sample=sample,
                )
            )
    samples.sort(key=lambda sample: sample.timestamp)
    return _dedupe_samples(samples)


def _merge_samples_into_track_points(track_points: list[TrackPoint], samples: list[ActivitySample]) -> None:
    if not track_points or not samples:
        return

    sample_index = 0
    for point in track_points:
        while sample_index + 1 < len(samples) and samples[sample_index + 1].timestamp <= point.timestamp:
            sample_index += 1
        candidates = [samples[sample_index]]
        if sample_index + 1 < len(samples):
            candidates.append(samples[sample_index + 1])
        sample = min(candidates, key=lambda candidate: abs(candidate.timestamp - point.timestamp))
        if abs(sample.timestamp - point.timestamp) > 5:
            continue
        point.distance_meters = sample.distance_meters
        point.heart_rate = sample.heart_rate
        point.cadence = sample.cadence
        if point.speed_mps is None:
            point.speed_mps = sample.speed_mps
        if point.altitude_meters is None:
            point.altitude_meters = sample.altitude_meters


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value))
        except ValueError:
            return None
    return None


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _coerce_str(value: Any) -> str | None:
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    return None


def _find_fds_entry(
    fds_downloads: dict[str, dict[str, Any]],
    suffix: str,
    timestamp: int,
) -> dict[str, Any] | None:
    """Find an FDS result entry by exact server key.

    The FDS response map is keyed by ``suffix_timestamp`` per
    ``FDSItem.toServerKey()`` in the Android app.
    """
    server_key = f"{suffix}_{timestamp}"
    return fds_downloads.get(server_key)


def _base64url_no_padding(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _build_fds_suffix(
    *,
    sid: str,
    timestamp: int,
    timezone_offset: int,
    sport_type: int,
    file_type: int,
) -> str:
    tz_in_15_minutes = timezone_offset & 0xFF
    data_type_byte = ((1 << 7) + (sport_type << 2) + file_type) & 0xFF
    server_key = struct.pack("<I", int(timestamp)) + bytes((tz_in_15_minutes, data_type_byte))
    sid_hash = hashlib.sha1(sid.encode("utf-8")).digest()
    return f"{_base64url_no_padding(server_key)}_{_base64url_no_padding(sid_hash)}"


def _dedupe_track_points(points: list[TrackPoint]) -> list[TrackPoint]:
    deduped: list[TrackPoint] = []
    seen: set[tuple[int, float | None, float | None]] = set()
    for point in points:
        key = (point.timestamp, point.latitude, point.longitude)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(point)
    return deduped


def _dedupe_samples(samples: list[ActivitySample]) -> list[ActivitySample]:
    deduped: list[ActivitySample] = []
    seen: set[tuple[int, int | None, int | None]] = set()
    for sample in samples:
        key = (sample.timestamp, sample.start_time, sample.end_time)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(sample)
    return deduped


def _merge_fds_samples_into_track_points(
    track_points: list[TrackPoint], samples: list[ActivitySample],
) -> None:
    """Merge HR/cadence from FDS sport samples into FDS GPS track points.

    Modifies *track_points* in-place. Only fills in fields that are None on
    the track point (GPS data doesn't carry HR or cadence).
    """
    sample_map: dict[int, ActivitySample] = {s.timestamp: s for s in samples}
    for tp in track_points:
        sample = sample_map.get(tp.timestamp)
        if sample is None:
            continue
        if tp.heart_rate is None and sample.heart_rate is not None:
            tp.heart_rate = sample.heart_rate
        if tp.cadence is None and sample.cadence is not None:
            tp.cadence = sample.cadence
        if tp.altitude_meters is None and sample.altitude_meters is not None:
            tp.altitude_meters = sample.altitude_meters
