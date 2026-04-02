from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


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


def format_distance_km(distance_meters: int | float | None) -> str:
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
