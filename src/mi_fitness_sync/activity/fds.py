from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import requests

from mi_fitness_sync.activity.models import Activity, ActivitySample, TrackPoint
from mi_fitness_sync.activity.transport import ActivityTransport
from mi_fitness_sync.activity.utils import coerce_int, build_fds_suffix, find_fds_entry
from mi_fitness_sync.exceptions import MiFitnessError, XiaomiApiError
from mi_fitness_sync.fds.cache import FdsCache
from mi_fitness_sync.fds.gps_records import download_and_parse_gps_record
from mi_fitness_sync.fds.recovery_rate import RecoveryRateData, download_and_parse_recovery_rate
from mi_fitness_sync.fds.sport_records import download_and_parse_sport_record
from mi_fitness_sync.fds.sport_reports import SportReport, download_and_parse_sport_report


logger = logging.getLogger(__name__)

FDS_SPORT_RECORD_FILE_TYPE = 0
FDS_SPORT_REPORT_FILE_TYPE = 1
FDS_GPS_FILE_TYPE = 2
FDS_RECOVERY_RATE_FILE_TYPE = 3


@dataclass(slots=True, frozen=True)
class ActivityFdsContext:
    sid: str
    proto_type: int
    timestamp: int
    timezone_offset: int


class ActivityFdsService:
    def __init__(
        self,
        session: requests.Session,
        transport: ActivityTransport,
        *,
        timeout: int,
        cache: FdsCache | None,
    ):
        self._session = session
        self._transport = transport
        self._timeout = timeout
        self._cache = cache

    def try_get_download_map(self, activity: Activity) -> dict[str, dict[str, Any]]:
        try:
            result = self.get_download_map(activity)
            logger.debug(
                "try_get_download_map: got %d entries for %s — keys: %s",
                len(result),
                activity.activity_id,
                list(result.keys()),
            )
            return result
        except (MiFitnessError, XiaomiApiError, requests.RequestException, ValueError) as exc:
            logger.warning(
                "try_get_download_map: FDS metadata request failed for %s: %s",
                activity.activity_id,
                exc,
            )
            return {}

    def try_download_sport_samples(
        self,
        activity: Activity,
        fds_downloads: dict[str, dict[str, Any]],
    ) -> list[ActivitySample]:
        if not fds_downloads:
            logger.debug("try_download_sport_samples: skipped — no FDS downloads for %s", activity.activity_id)
            return []

        context = self._build_context(activity)
        if context is None:
            logger.debug("try_download_sport_samples: missing FDS context for %s", activity.activity_id)
            return []

        record_suffix = build_fds_suffix(
            sid=context.sid,
            timestamp=context.timestamp,
            timezone_offset=context.timezone_offset,
            sport_type=context.proto_type,
            file_type=FDS_SPORT_RECORD_FILE_TYPE,
        )
        fds_entry = find_fds_entry(fds_downloads, record_suffix, context.timestamp)
        if fds_entry is None:
            logger.debug("try_download_sport_samples: no FDS entry matched suffix=%s", record_suffix)
            return []

        cache_key = f"{context.sid}_{FDS_SPORT_RECORD_FILE_TYPE}_{record_suffix}_{context.timestamp}"
        try:
            sport_samples = download_and_parse_sport_record(
                self._session,
                fds_entry,
                context.proto_type,
                timeout=self._timeout,
                cache=self._cache,
                cache_key=cache_key,
            )
        except Exception:
            logger.warning("try_download_sport_samples: download/parse failed for %s", activity.activity_id, exc_info=True)
            return []

        return [
            ActivitySample(
                timestamp=sample.timestamp,
                start_time=sample.timestamp,
                end_time=sample.timestamp,
                duration_seconds=1,
                heart_rate=sample.heart_rate,
                cadence=sample.cadence,
                speed_mps=None,
                distance_meters=float(sample.distance) if sample.distance is not None else None,
                altitude_meters=None,
                steps=sample.steps,
                calories=sample.calories,
                raw_sample={"source": "fds_sport_record"},
            )
            for sample in sport_samples
        ]

    def try_download_sport_report(
        self,
        activity: Activity,
        fds_downloads: dict[str, dict[str, Any]],
    ) -> SportReport | None:
        if not fds_downloads:
            return None

        context = self._build_context(activity)
        if context is None:
            logger.debug("try_download_sport_report: missing FDS context for %s", activity.activity_id)
            return None

        report_suffix = build_fds_suffix(
            sid=context.sid,
            timestamp=context.timestamp,
            timezone_offset=context.timezone_offset,
            sport_type=context.proto_type,
            file_type=FDS_SPORT_REPORT_FILE_TYPE,
        )
        fds_entry = find_fds_entry(fds_downloads, report_suffix, context.timestamp)
        if fds_entry is None:
            logger.debug("try_download_sport_report: no FDS entry matched suffix=%s", report_suffix)
            return None

        cache_key = f"{context.sid}_{FDS_SPORT_REPORT_FILE_TYPE}_{report_suffix}_{context.timestamp}"
        try:
            return download_and_parse_sport_report(
                self._session,
                fds_entry,
                context.proto_type,
                timeout=self._timeout,
                cache=self._cache,
                cache_key=cache_key,
            )
        except Exception:
            logger.warning("try_download_sport_report: download/parse failed for %s", activity.activity_id, exc_info=True)
            return None

    def try_download_gps_track_points(
        self,
        activity: Activity,
        fds_downloads: dict[str, dict[str, Any]],
    ) -> list[TrackPoint]:
        if not fds_downloads:
            logger.debug("try_download_gps_track_points: skipped — no FDS downloads for %s", activity.activity_id)
            return []

        context = self._build_context(activity)
        if context is None:
            logger.debug("try_download_gps_track_points: missing FDS context for %s", activity.activity_id)
            return []

        gps_suffix = build_fds_suffix(
            sid=context.sid,
            timestamp=context.timestamp,
            timezone_offset=context.timezone_offset,
            sport_type=context.proto_type,
            file_type=FDS_GPS_FILE_TYPE,
        )
        fds_entry = find_fds_entry(fds_downloads, gps_suffix, context.timestamp)
        if fds_entry is None:
            logger.debug("try_download_gps_track_points: no FDS entry matched suffix=%s", gps_suffix)
            return []

        cache_key = f"{context.sid}_{FDS_GPS_FILE_TYPE}_{gps_suffix}_{context.timestamp}"
        try:
            gps_samples = download_and_parse_gps_record(
                self._session,
                fds_entry,
                timeout=self._timeout,
                cache=self._cache,
                cache_key=cache_key,
            )
        except Exception:
            logger.warning("try_download_gps_track_points: download/parse failed for %s", activity.activity_id, exc_info=True)
            return []

        return [
            TrackPoint(
                timestamp=sample.timestamp,
                latitude=sample.latitude,
                longitude=sample.longitude,
                altitude_meters=sample.altitude,
                speed_mps=sample.speed,
                distance_meters=None,
                heart_rate=None,
                cadence=None,
                raw_point={"source": "fds_gps"},
            )
            for sample in gps_samples
        ]

    def try_download_recovery_rate(
        self,
        activity: Activity,
        fds_downloads: dict[str, dict[str, Any]],
    ) -> RecoveryRateData | None:
        if not fds_downloads:
            return None

        context = self._build_context(activity)
        if context is None:
            logger.debug("try_download_recovery_rate: missing FDS context for %s", activity.activity_id)
            return None

        recovery_suffix = build_fds_suffix(
            sid=context.sid,
            timestamp=context.timestamp,
            timezone_offset=context.timezone_offset,
            sport_type=context.proto_type,
            file_type=FDS_RECOVERY_RATE_FILE_TYPE,
        )
        fds_entry = find_fds_entry(fds_downloads, recovery_suffix, context.timestamp)
        if fds_entry is None:
            logger.debug("try_download_recovery_rate: no FDS entry matched suffix=%s", recovery_suffix)
            return None

        cache_key = f"{context.sid}_{FDS_RECOVERY_RATE_FILE_TYPE}_{recovery_suffix}_{context.timestamp}"
        try:
            return download_and_parse_recovery_rate(
                self._session,
                fds_entry,
                timeout=self._timeout,
                cache=self._cache,
                cache_key=cache_key,
            )
        except Exception:
            logger.warning("try_download_recovery_rate: download/parse failed for %s", activity.activity_id, exc_info=True)
            return None

    def get_download_map(self, activity: Activity) -> dict[str, dict[str, Any]]:
        context = self._build_context(activity)
        if context is None:
            return {}

        request_payload = {
            "did": context.sid,
            "items": [
                self._build_request_item(context, FDS_SPORT_RECORD_FILE_TYPE),
                self._build_request_item(context, FDS_SPORT_REPORT_FILE_TYPE),
                self._build_request_item(context, FDS_GPS_FILE_TYPE),
                self._build_request_item(context, FDS_RECOVERY_RATE_FILE_TYPE),
            ],
        }
        payload = self._transport.request_json(
            endpoint=self._transport.get_fds_download_url_endpoint(),
            path="/healthapp/service/gen_download_url",
            signature_path="/service/gen_download_url",
            request_payload=request_payload,
            request_label="Mi Fitness FDS metadata request",
        )
        result = payload.get("result")
        if not isinstance(result, dict):
            return {}
        return {
            key: value
            for key, value in result.items()
            if isinstance(key, str) and isinstance(value, dict)
        }

    def _build_context(self, activity: Activity) -> ActivityFdsContext | None:
        sid = activity.sid
        timestamp = coerce_int(activity.raw_record.get("time")) or activity.start_time
        proto_type = coerce_int(activity.raw_report.get("proto_type"))
        timezone_offset = coerce_int(activity.raw_report.get("timezone"))
        if not sid or timestamp is None or proto_type is None or timezone_offset is None:
            return None
        return ActivityFdsContext(
            sid=sid,
            proto_type=proto_type,
            timestamp=timestamp,
            timezone_offset=timezone_offset,
        )

    @staticmethod
    def _build_request_item(context: ActivityFdsContext, file_type: int) -> dict[str, Any]:
        suffix = build_fds_suffix(
            sid=context.sid,
            timestamp=context.timestamp,
            timezone_offset=context.timezone_offset,
            sport_type=context.proto_type,
            file_type=file_type,
        )
        return {"timestamp": context.timestamp, "suffix": suffix}