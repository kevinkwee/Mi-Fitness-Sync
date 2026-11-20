"""WGS-84 coordinate validity for exported tracks."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mi_fitness_sync.activity.models import TrackPoint

MIN_LATITUDE_DEGREES = -90.0
MAX_LATITUDE_DEGREES = 90.0
MIN_LONGITUDE_DEGREES = -180.0
MAX_LONGITUDE_DEGREES = 180.0


def is_valid_coordinate(latitude: float | None, longitude: float | None) -> bool:
    """Report whether a coordinate pair is a usable WGS-84 position."""
    if latitude is None or longitude is None:
        return False
    return _in_range(latitude, longitude)


def gps_position(point: TrackPoint) -> tuple[float, float] | None:
    """Return the (latitude, longitude) pair when the point has a usable position."""
    latitude, longitude = point.latitude, point.longitude
    if latitude is None or longitude is None:
        return None
    if not _in_range(latitude, longitude):
        return None
    return latitude, longitude


def _in_range(latitude: float, longitude: float) -> bool:
    return (
        MIN_LATITUDE_DEGREES <= latitude <= MAX_LATITUDE_DEGREES
        and MIN_LONGITUDE_DEGREES <= longitude < MAX_LONGITUDE_DEGREES
    )
