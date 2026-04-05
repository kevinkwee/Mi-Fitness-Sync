from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import requests

from mi_fitness_sync.activity import transport as activity_transport
from mi_fitness_sync.activity.crypto import build_signature, decrypt_response_payload, encrypt_query_params, generate_nonce
from mi_fitness_sync.activity.fds import ActivityFdsService
from mi_fitness_sync.activity.formatting import format_title
from mi_fitness_sync.activity.models import Activity, ActivityDetail, ActivityPage, FitnessDataPage
from mi_fitness_sync.activity.transport import (
    ActivityTransport,
    collect_cookie_values,
)
from mi_fitness_sync.activity.utils import (
    coerce_int,
    coerce_str,
    extract_activity_samples,
    extract_track_points,
    merge_fds_samples_into_track_points,
    merge_samples_into_track_points,
    parse_activity_id,
)
from mi_fitness_sync.auth.state import AuthState
from mi_fitness_sync.exceptions import MiFitnessError
from mi_fitness_sync.fds.cache import DEFAULT_CACHE_DIR, FdsCache


logger = logging.getLogger(__name__)

ACTIVITY_LIST_ENDPOINT = activity_transport.ACTIVITY_LIST_ENDPOINT
DEFAULT_PAGE_SIZE = 20
DEFAULT_TIMEOUT_SECONDS = 30
DETAIL_DATA_KEY = "huami_sport_record"
ACTIVITY_ID_SEARCH_WINDOW_SECONDS = 86400


class MiFitnessActivitiesClient:
    def __init__(
        self,
        auth_state: AuthState,
        *,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        country_code: str | None = None,
        cache_dir: Path | str = DEFAULT_CACHE_DIR,
        no_cache: bool = False,
    ):
        self._auth_state = auth_state
        self._timeout = timeout
        self._session = requests.Session()
        self._transport = ActivityTransport(
            self._session,
            auth_state,
            timeout=timeout,
            country_code=country_code,
        )
        self._cookie_values = self._transport.cookie_values
        self._cache: FdsCache | None = None if no_cache else FdsCache(cache_dir)
        self._fds_service = ActivityFdsService(
            self._session,
            self._transport,
            timeout=timeout,
            cache=self._cache,
        )

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
            for activity in page.activities:
                if start_time is not None and activity.start_time is not None and activity.start_time < start_time:
                    continue
                if end_time is not None and activity.start_time is not None and activity.start_time > end_time:
                    continue
                activities.append(activity)
                if len(activities) >= limit:
                    break
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
        _, _, time_value = parse_activity_id(activity_id)
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
        fds_downloads = self._try_get_fds_download_map(activity)
        logger.debug(
            "get_activity_detail: FDS download map has %d entries for %s",
            len(fds_downloads),
            activity.activity_id,
        )
        fds_samples = self._try_download_fds_sport_samples(activity, fds_downloads)
        fds_track_points = self._try_download_fds_gps_track_points(activity, fds_downloads)
        fds_sport_report = self._try_download_fds_sport_report(activity, fds_downloads)
        fds_recovery_rate = self._try_download_fds_recovery_rate(activity, fds_downloads)

        if fds_track_points and fds_samples:
            merge_fds_samples_into_track_points(fds_track_points, fds_samples)

        fitness_item = self._get_activity_detail_item(activity)
        if fitness_item:
            detail = self._build_activity_detail_from_item(activity, fitness_item, fds_downloads)
            if fds_samples:
                detail.samples = fds_samples
            if fds_track_points:
                detail.track_points = fds_track_points
            if fds_sport_report:
                detail.sport_report = fds_sport_report
            if fds_recovery_rate:
                detail.recovery_rate = fds_recovery_rate
            return detail

        if fds_samples or fds_track_points:
            return ActivityDetail(
                activity=activity,
                detail_sid=activity.sid,
                detail_key="fds_sport_record",
                detail_time=activity.start_time or activity.raw_record.get("time") or 0,
                zone_name=coerce_str(activity.raw_record.get("zone_name")),
                zone_offset_seconds=coerce_int(activity.raw_record.get("zone_offset")),
                track_points=fds_track_points,
                samples=fds_samples,
                sport_report=fds_sport_report,
                recovery_rate=fds_recovery_rate,
                raw_fitness_item={"source": "fds_sport_record"},
                raw_detail={"source": "fds_sport_record", "fds_downloads": fds_downloads},
            )

        raise MiFitnessError(
            f"Could not find a detail payload for activity {activity.activity_id} in Mi Fitness. "
            "The workout summary exists, but neither the JSON detail nor FDS binary data was available. "
            "Run with --verbose for diagnostics."
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
        request_payload: dict[str, Any] = {"reverse": True, "limit": limit}
        if start_time is not None and start_time > 0:
            request_payload["startTime"] = start_time
        if end_time is not None and end_time > 0:
            request_payload["endTime"] = end_time
        if category:
            request_payload["category"] = category
        if next_key:
            request_payload["next_key"] = next_key

        payload = self._transport.request_json(
            endpoint=self._transport.get_activity_list_endpoint(),
            path="/app/v1/data/get_sport_records_by_time",
            request_payload=request_payload,
            request_label="Mi Fitness activity request",
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
        return self._transport.build_cookie_header()

    def _build_request_headers(self) -> dict[str, str]:
        return self._transport.build_request_headers()

    def _get_activity_list_endpoint(self) -> str:
        return self._transport.get_activity_list_endpoint()

    def _get_fitness_data_time_endpoint(self) -> str:
        return self._transport.get_fitness_data_time_endpoint()

    def _get_fds_download_url_endpoint(self) -> str:
        return self._transport.get_fds_download_url_endpoint()

    def _get_region(self) -> str:
        return self._transport.get_region()

    def _normalize_region(self, value: str | None) -> str | None:
        return self._transport.normalize_region(value)

    def _collect_cookie_values(self) -> dict[str, str]:
        return collect_cookie_values(self._auth_state)

    def _decrypt_response_payload(self, body: str, nonce: str, ssecurity: str) -> dict[str, Any]:
        return decrypt_response_payload(body, nonce, ssecurity)

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
        return encrypt_query_params(
            method=method,
            path=path,
            params=params,
            nonce=nonce,
            ssecurity=ssecurity,
            signature_path=signature_path,
        )

    def _build_signature(self, method: str, path: str, params: dict[str, str], signed_nonce: str) -> str:
        return build_signature(method, path, params, signed_nonce)

    def _generate_nonce(self, time_diff_ms: int) -> str:
        return generate_nonce(time_diff_ms)

    def _parse_activity(self, record: dict[str, Any], next_key: str | None) -> Activity:
        raw_report = record.get("value")
        if isinstance(raw_report, str) and raw_report:
            try:
                report = json.loads(raw_report)
            except json.JSONDecodeError:
                report = {}
        else:
            report = {}

        sport_type = report.get("sport_type") if isinstance(report.get("sport_type"), int) else None
        start_time = report.get("start_time") if isinstance(report.get("start_time"), int) else None
        if start_time is None and isinstance(record.get("time"), int):
            start_time = record.get("time")
        end_time = report.get("end_time") if isinstance(report.get("end_time"), int) else None
        duration_seconds = report.get("duration") if isinstance(report.get("duration"), int) else None
        distance_meters = report.get("distance") if isinstance(report.get("distance"), int) else None
        calories = report.get("calories") if isinstance(report.get("calories"), int) else None
        steps = report.get("steps") if isinstance(report.get("steps"), int) else None
        sid = str(record.get("sid") or "")
        key = str(record.get("key") or "")
        time_value = record.get("time") if isinstance(record.get("time"), int) else start_time or 0
        sync_state = "deleted" if record.get("deleted") is True else "server" if sid and key else None
        category = record.get("category") if isinstance(record.get("category"), str) else None
        return Activity(
            activity_id=f"{sid}:{key}:{time_value}",
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
            end_time = start_time + (activity.duration_seconds or ACTIVITY_ID_SEARCH_WINDOW_SECONDS)
        record_time = activity.raw_record.get("time")
        next_key: str | None = None
        while True:
            page = self._fetch_fitness_data_page(
                key=DETAIL_DATA_KEY,
                start_time=start_time,
                end_time=end_time,
                next_key=next_key,
            )
            for item in page.items:
                if (
                    str(item.get("sid") or "") == activity.sid
                    and str(item.get("key") or "") == activity.key
                    and (record_time is None or item.get("time") == record_time)
                ):
                    return item
            if not page.has_more or not page.next_key:
                break
            next_key = page.next_key
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

        track_points = extract_track_points(raw_detail)
        samples = extract_activity_samples(raw_detail)
        merge_samples_into_track_points(track_points, samples)
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
        return self._fds_service.try_get_download_map(activity)

    def _try_download_fds_sport_samples(self, activity: Activity, fds_downloads: dict[str, dict[str, Any]]):
        return self._fds_service.try_download_sport_samples(activity, fds_downloads)

    def _try_download_fds_sport_report(self, activity: Activity, fds_downloads: dict[str, dict[str, Any]]):
        return self._fds_service.try_download_sport_report(activity, fds_downloads)

    def _try_download_fds_gps_track_points(self, activity: Activity, fds_downloads: dict[str, dict[str, Any]]):
        return self._fds_service.try_download_gps_track_points(activity, fds_downloads)

    def _try_download_fds_recovery_rate(self, activity: Activity, fds_downloads: dict[str, dict[str, Any]]):
        return self._fds_service.try_download_recovery_rate(activity, fds_downloads)

    def _get_fds_download_map(self, activity: Activity) -> dict[str, dict[str, Any]]:
        return self._fds_service.get_download_map(activity)

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
        payload = self._transport.request_json(
            endpoint=self._transport.get_fitness_data_time_endpoint(),
            path="/app/v1/data/get_fitness_data_by_time",
            request_payload=request_payload,
            request_label="Mi Fitness activity detail request",
        )
        result = payload.get("result") or {}
        raw_items = result.get("data_list") or []
        items = [item for item in raw_items if isinstance(item, dict)]
        return FitnessDataPage(items=items, has_more=bool(result.get("has_more")), next_key=result.get("next_key") or None)
