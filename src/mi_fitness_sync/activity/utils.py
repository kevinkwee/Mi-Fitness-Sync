from __future__ import annotations

import base64
import hashlib
import struct
from typing import Any

from mi_fitness_sync.activity.formatting import format_distance_km, format_duration, format_terminal_time
from mi_fitness_sync.activity.models import Activity, ActivitySample, TrackPoint
from mi_fitness_sync.exceptions import MiFitnessError


def render_activities_table(
    activities: list[Activity],
    strava_status: dict[str, bool] | None = None,
) -> str:
    if not activities:
        return "No activities matched the requested time window."

    headers = ["ID", "Start", "Title", "Type", "Duration", "Km", "Cal", "Steps", "State"]
    if strava_status is not None:
        headers.append("Strava")
    rows = []
    for activity in activities:
        row = [
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
        if strava_status is not None:
            matched = strava_status.get(activity.activity_id)
            row.append("\u2713" if matched else ("\u2717" if matched is False else "-"))
        rows.append(row)

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
        raise MiFitnessError("Activity IDs must use the list-activities format sid:key:time.")
    key, separator, time_value = remainder.rpartition(":")
    if not separator or not sid or not key:
        raise MiFitnessError("Activity IDs must use the list-activities format sid:key:time.")
    try:
        return sid, key, int(time_value)
    except ValueError as exc:
        raise MiFitnessError("Activity IDs must use the list-activities format sid:key:time.") from exc


def extract_track_points(payload: Any) -> list[TrackPoint]:
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
            timestamp = coerce_int(point.get("timestamp"))
            if timestamp is None:
                timestamp = coerce_int(point.get("time"))
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


def extract_activity_samples(payload: Any) -> list[ActivitySample]:
    if not isinstance(payload, dict):
        return []

    samples: list[ActivitySample] = []
    for value in payload.values():
        if not isinstance(value, list):
            continue
        for sample in value:
            if not isinstance(sample, dict):
                continue
            start_time = coerce_int(sample.get("startTime") or sample.get("start_time"))
            end_time = coerce_int(sample.get("endTime") or sample.get("end_time"))
            timestamp = end_time or start_time
            if timestamp is None:
                continue
            duration_seconds = coerce_int(sample.get("duration"))
            if duration_seconds is None and start_time is not None and end_time is not None:
                duration_seconds = max(end_time - start_time, 0)
            samples.append(
                ActivitySample(
                    timestamp=timestamp,
                    start_time=start_time,
                    end_time=end_time,
                    duration_seconds=duration_seconds,
                    heart_rate=coerce_int(sample.get("hr") or sample.get("heartRate")),
                    cadence=coerce_int(
                        sample.get("cadence")
                        or sample.get("cycleCadence")
                        or sample.get("jumpFrequency")
                        or sample.get("rowingCadence")
                    ),
                    speed_mps=_coerce_float(sample.get("speed") or sample.get("avgSpeed") or sample.get("locationSpeed")),
                    distance_meters=_coerce_float(sample.get("distance") or sample.get("newDistance") or sample.get("runDistance")),
                    altitude_meters=_coerce_float(sample.get("altitude") or sample.get("height")),
                    steps=coerce_int(sample.get("steps") or sample.get("newSteps") or sample.get("totalSteps")),
                    calories=coerce_int(sample.get("calories") or sample.get("newCalories") or sample.get("activeCalories")),
                    raw_sample=sample,
                )
            )
    samples.sort(key=lambda sample: sample.timestamp)
    return _dedupe_samples(samples)


def merge_samples_into_track_points(track_points: list[TrackPoint], samples: list[ActivitySample]) -> None:
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


def coerce_int(value: Any) -> int | None:
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


def coerce_str(value: Any) -> str | None:
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    return None


def find_fds_entry(fds_downloads: dict[str, dict[str, Any]], suffix: str, timestamp: int) -> dict[str, Any] | None:
    server_key = f"{suffix}_{timestamp}"
    return fds_downloads.get(server_key)


def _base64url_no_padding(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def build_fds_suffix(*, sid: str, timestamp: int, timezone_offset: int, sport_type: int, file_type: int) -> str:
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


def merge_fds_samples_into_track_points(track_points: list[TrackPoint], samples: list[ActivitySample]) -> None:
    sample_map: dict[int, ActivitySample] = {sample.timestamp: sample for sample in samples}
    for track_point in track_points:
        sample = sample_map.get(track_point.timestamp)
        if sample is None:
            continue
        if track_point.heart_rate is None and sample.heart_rate is not None:
            track_point.heart_rate = sample.heart_rate
        if track_point.cadence is None and sample.cadence is not None:
            track_point.cadence = sample.cadence
        if track_point.altitude_meters is None and sample.altitude_meters is not None:
            track_point.altitude_meters = sample.altitude_meters
