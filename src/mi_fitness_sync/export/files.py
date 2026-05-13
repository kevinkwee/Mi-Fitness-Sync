from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mi_fitness_sync.activity.models import Activity
from mi_fitness_sync.exceptions import MiFitnessError


def sanitize_export_title(title: str) -> str:
    """Convert a title into a filesystem-safe ASCII-ish stem."""
    sanitized = re.sub(r"[^\w]", "", title.replace(" ", "_"))
    return sanitized or "activity"


def sanitize_activity_id(activity_id: str) -> str:
    """Convert an activity id into a filesystem-safe suffix."""
    sanitized = re.sub(r"[^\w]", "", activity_id.replace(":", "_"))
    return sanitized or "activity"


def activity_local_datetime(
    activity: Activity,
    *,
    zone_offset_seconds: int | None = None,
) -> datetime:
    """Return the activity start time as a local datetime when possible."""
    timestamp = activity.start_time
    if timestamp is None:
        raw_time = activity.raw_record.get("time")
        if isinstance(raw_time, int):
            timestamp = raw_time
    if timestamp is None:
        raise MiFitnessError(f"Activity {activity.activity_id} does not have a usable start time.")

    resolved_offset = zone_offset_seconds
    if resolved_offset is None:
        raw_timezone = activity.raw_report.get("timezone")
        if isinstance(raw_timezone, int):
            resolved_offset = raw_timezone

    tz = timezone(timedelta(seconds=resolved_offset)) if resolved_offset is not None else None
    utc_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    if tz is not None:
        return utc_dt.astimezone(tz)
    return utc_dt.astimezone()


def default_export_path(
    base_dir: Path,
    activity: Activity,
    file_format: str,
    *,
    compress: bool,
    zone_offset_seconds: int | None = None,
    include_activity_id: bool = False,
) -> Path:
    """Build the default output path for a rendered export."""
    safe_title = sanitize_export_title(activity.title)
    date_str = activity_local_datetime(activity, zone_offset_seconds=zone_offset_seconds).strftime("%Y%m%d_%H%M%S")
    suffix = f".{file_format}.gz" if compress else f".{file_format}"

    if include_activity_id:
        safe_activity_id = sanitize_activity_id(activity.activity_id)
        filename = f"{safe_title}_{date_str}_{safe_activity_id}{suffix}"
    else:
        filename = f"{safe_title}_{date_str}{suffix}"

    return base_dir / filename
