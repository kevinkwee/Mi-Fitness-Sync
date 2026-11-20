from __future__ import annotations

import pytest

from mi_fitness_sync.activity.models import TrackPoint
from mi_fitness_sync.export.geo import gps_position, is_valid_coordinate


def _make_point(latitude: float | None, longitude: float | None) -> TrackPoint:
    return TrackPoint(
        timestamp=1717200000,
        latitude=latitude,
        longitude=longitude,
        altitude_meters=None,
        speed_mps=None,
        distance_meters=None,
        heart_rate=None,
        cadence=None,
        raw_point={},
    )


class TestIsValidCoordinate:
    @pytest.mark.parametrize(
        ("latitude", "longitude"),
        [
            (0.0, 0.0),
            (90.0, 0.0),
            (-90.0, 0.0),
            (0.0, -180.0),
            (0.0, 179.9999999),
        ],
    )
    def test_accepts_bounded_positions(self, latitude: float, longitude: float) -> None:
        assert is_valid_coordinate(latitude, longitude) is True

    @pytest.mark.parametrize(
        ("latitude", "longitude"),
        [
            (None, 0.0),
            (0.0, None),
            (None, None),
            (90.0001, 0.0),
            (-90.0001, 0.0),
            (0.0, 180.0),
            (0.0, -180.0001),
        ],
    )
    def test_rejects_out_of_range_positions(
        self, latitude: float | None, longitude: float | None
    ) -> None:
        assert is_valid_coordinate(latitude, longitude) is False


class TestGpsPosition:
    def test_returns_the_pair_for_a_valid_point(self) -> None:
        assert gps_position(_make_point(1.5, 103.5)) == (1.5, 103.5)

    def test_returns_none_when_latitude_is_missing(self) -> None:
        assert gps_position(_make_point(None, 103.5)) is None

    def test_returns_none_when_longitude_is_missing(self) -> None:
        assert gps_position(_make_point(1.5, None)) is None

    def test_returns_none_when_longitude_is_exactly_180(self) -> None:
        assert gps_position(_make_point(1.5, 180.0)) is None
