from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from mi_fitness_sync.activity.formatting import format_duration, format_terminal_time
from mi_fitness_sync.fds.recovery_rate import RecoveryRateData
from mi_fitness_sync.fds.sport_reports import SportReport


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
