from __future__ import annotations

import math

import pytest

from mi_fitness_sync.activity.models import TrackPoint
from mi_fitness_sync.export.smoothing import (
    _apply_savgol,
    _distance_tolerance,
    _fix_outliers,
    _savgol_coeffs,
    haversine,
    smooth_track,
    total_haversine_distance,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _tp(
    ts: int,
    lat: float | None,
    lon: float | None,
    *,
    alt: float | None = None,
    hr: int | None = None,
    cad: int | None = None,
    spd: float | None = None,
    dist: float | None = None,
) -> TrackPoint:
    return TrackPoint(
        timestamp=ts,
        latitude=lat,
        longitude=lon,
        altitude_meters=alt,
        speed_mps=spd,
        distance_meters=dist,
        heart_rate=hr,
        cadence=cad,
        raw_point={},
    )


def _linear_track(n: int, start_lat: float = 1.0, start_lon: float = 103.0, dlat: float = 0.001, dlon: float = 0.001) -> list[TrackPoint]:
    """Generate *n* evenly-spaced points along a straight line."""
    return [
        _tp(1000 + i * 10, start_lat + i * dlat, start_lon + i * dlon)
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# haversine
# ---------------------------------------------------------------------------


class TestHaversine:
    def test_same_point_is_zero(self):
        assert haversine(0.0, 0.0, 0.0, 0.0) == 0.0

    def test_known_short_distance(self):
        # ~111 km per degree of latitude at the equator
        d = haversine(0.0, 0.0, 1.0, 0.0)
        assert 110_000 < d < 112_000

    def test_symmetric(self):
        d1 = haversine(1.3, 103.8, 1.31, 103.81)
        d2 = haversine(1.31, 103.81, 1.3, 103.8)
        assert d1 == pytest.approx(d2)


# ---------------------------------------------------------------------------
# total_haversine_distance
# ---------------------------------------------------------------------------


class TestTotalHaversineDistance:
    def test_empty_list(self):
        assert total_haversine_distance([]) == 0.0

    def test_single_point(self):
        assert total_haversine_distance([_tp(0, 1.0, 2.0)]) == 0.0

    def test_two_points(self):
        pts = [_tp(0, 0.0, 0.0), _tp(1, 1.0, 0.0)]
        d = total_haversine_distance(pts)
        assert d > 0

    def test_skips_none_coords(self):
        pts = [_tp(0, 1.0, 2.0), _tp(1, None, None), _tp(2, 1.001, 2.001)]
        d = total_haversine_distance(pts)
        assert d > 0


# ---------------------------------------------------------------------------
# smooth_track – edge cases
# ---------------------------------------------------------------------------


class TestSmoothTrackEdgeCases:
    def test_no_gps_points_returns_original(self):
        pts = [_tp(0, None, None), _tp(1, None, None)]
        result = smooth_track(pts, 1000.0)
        assert result is pts

    def test_single_gps_point_returns_original(self):
        pts = [_tp(0, 1.0, 2.0)]
        result = smooth_track(pts, 1000.0)
        assert result is pts

    def test_zero_target_returns_original(self):
        pts = _linear_track(10)
        result = smooth_track(pts, 0.0)
        assert result is pts

    def test_negative_target_returns_original(self):
        pts = _linear_track(10)
        result = smooth_track(pts, -500.0)
        assert result is pts

    def test_matching_distance_returns_original(self):
        pts = _linear_track(5)
        d = total_haversine_distance(pts)
        result = smooth_track(pts, d)
        assert result is pts

    def test_within_tolerance_returns_original(self):
        pts = _linear_track(5)
        d = total_haversine_distance(pts)
        # Small absolute offset well within the 20 m floor
        result = smooth_track(pts, d + 5.0)
        assert result is pts


# ---------------------------------------------------------------------------
# smooth_track – smoothing behaviour
# ---------------------------------------------------------------------------


class TestSmoothTrackBehaviour:
    def _noisy_track(self, n: int = 40) -> list[TrackPoint]:
        """Straight northward track with perpendicular east-west oscillation."""
        return [
            _tp(
                1000 + i * 10,
                1.0 + i * 0.001,
                103.0 + 0.0005 * (1 if i % 2 == 0 else -1),
                hr=120 + i,
                cad=160,
            )
            for i in range(n)
        ]

    def _clean_distance(self, n: int = 40) -> float:
        clean = [_tp(1000 + i * 10, 1.0 + i * 0.001, 103.0) for i in range(n)]
        return total_haversine_distance(clean)

    def test_smoothing_reduces_distance_toward_target(self):
        pts = self._noisy_track(40)
        noisy_distance = total_haversine_distance(pts)
        target = self._clean_distance(40)
        assert noisy_distance > target * 1.01  # noise inflates distance

        result = smooth_track(pts, target)
        smoothed_distance = total_haversine_distance(result)
        assert abs(smoothed_distance - target) < abs(noisy_distance - target)

    def test_preserves_timestamps(self):
        pts = self._noisy_track(20)
        target = self._clean_distance(20)
        result = smooth_track(pts, target)
        assert [p.timestamp for p in result] == [p.timestamp for p in pts]

    def test_preserves_heart_rate(self):
        pts = self._noisy_track(20)
        target = self._clean_distance(20)
        result = smooth_track(pts, target)
        assert [p.heart_rate for p in result] == [p.heart_rate for p in pts]

    def test_preserves_cadence(self):
        pts = self._noisy_track(20)
        target = self._clean_distance(20)
        result = smooth_track(pts, target)
        assert [p.cadence for p in result] == [p.cadence for p in pts]

    def test_preserves_altitude(self):
        pts = [
            _tp(1000 + i * 10, 1.0 + i * 0.001, 103.0 + 0.0005 * (1 if i % 2 == 0 else -1), alt=10.0 + i)
            for i in range(20)
        ]
        clean = [_tp(1000 + i * 10, 1.0 + i * 0.001, 103.0) for i in range(20)]
        target = total_haversine_distance(clean)
        result = smooth_track(pts, target)
        assert [p.altitude_meters for p in result] == [p.altitude_meters for p in pts]

    def test_gps_shorter_than_target_returns_fixed_only(self):
        """When GPS distance < target, SG smoothing would make it worse, so skip it."""
        pts = _linear_track(10)
        d = total_haversine_distance(pts)
        result = smooth_track(pts, d * 2.0)
        result_d = total_haversine_distance(result)
        assert result_d == pytest.approx(d, rel=0.01)

    def test_moderate_target_selects_best_window(self):
        """Regression: a target 90-95 % of noisy distance must not return the original."""
        pts = self._noisy_track(40)
        noisy_distance = total_haversine_distance(pts)
        # Set target to ~95 % of noisy distance — moderate reduction
        target = noisy_distance * 0.95
        assert noisy_distance > target  # sanity

        result = smooth_track(pts, target)
        smoothed_distance = total_haversine_distance(result)
        # Smoothed result must be closer to target than the original
        assert abs(smoothed_distance - target) < abs(noisy_distance - target)


# ---------------------------------------------------------------------------
# _fix_outliers
# ---------------------------------------------------------------------------


class TestFixOutliers:
    def test_no_outliers_unchanged(self):
        pts = _linear_track(5)
        result = _fix_outliers(pts, 50.0)
        for a, b in zip(pts, result):
            assert a.latitude == b.latitude
            assert a.longitude == b.longitude

    def test_outlier_interpolated(self):
        pts = [
            _tp(0, 1.0, 103.0),
            _tp(10, 1.001, 103.001),
            _tp(11, 50.0, 50.0),  # huge jump in 1 second → outlier
            _tp(20, 1.002, 103.002),
            _tp(30, 1.003, 103.003),
        ]
        result = _fix_outliers(pts, 50.0)
        # The outlier point should have been interpolated
        assert result[2].latitude != 50.0
        assert result[2].longitude != 50.0
        # Interpolated between pts[1] and pts[3]
        assert 1.001 < result[2].latitude < 1.002
        assert 103.001 < result[2].longitude < 103.002

    def test_outlier_preserves_other_fields(self):
        pts = [
            _tp(0, 1.0, 103.0, hr=100),
            _tp(1, 50.0, 50.0, hr=110),  # outlier
            _tp(10, 1.001, 103.001, hr=120),
        ]
        result = _fix_outliers(pts, 50.0)
        assert result[1].heart_rate == 110
        assert result[1].timestamp == 1

    def test_fewer_than_three_points_unchanged(self):
        pts = [_tp(0, 1.0, 2.0), _tp(1, 1.1, 2.1)]
        result = _fix_outliers(pts, 50.0)
        assert len(result) == 2
        assert result[0].latitude == 1.0


# ---------------------------------------------------------------------------
# Savitzky-Golay internals
# ---------------------------------------------------------------------------


class TestSavgolFilter:
    def test_coefficients_sum_to_one(self):
        for window in (5, 7, 9, 11):
            coeffs = _savgol_coeffs(window, 3)
            assert sum(coeffs) == pytest.approx(1.0, abs=1e-10)

    def test_constant_data_unchanged(self):
        data = [42.0] * 20
        result = _apply_savgol(data, 5, 3)
        for v in result:
            assert v == pytest.approx(42.0, abs=1e-10)

    def test_linear_data_unchanged(self):
        data = [float(i) for i in range(20)]
        result = _apply_savgol(data, 5, 3)
        # Interior points should be exactly preserved (SG preserves polynomials up to polyorder)
        for i in range(2, 18):
            assert result[i] == pytest.approx(data[i], abs=1e-8)

    def test_output_length_matches_input(self):
        data = [float(i) for i in range(30)]
        result = _apply_savgol(data, 7, 3)
        assert len(result) == len(data)

    def test_too_short_data_returned_unchanged(self):
        data = [1.0, 2.0, 3.0]
        result = _apply_savgol(data, 5, 3)
        assert result == data


# ---------------------------------------------------------------------------
# _distance_tolerance – bounded hybrid behaviour
# ---------------------------------------------------------------------------


class TestDistanceTolerance:
    def test_short_activity_uses_floor(self):
        # 500 m target → 0.3% = 1.5 m, floor = 20 m wins
        assert _distance_tolerance(500.0) == 20.0

    def test_medium_activity_uses_rate(self):
        # 10 km target → 0.3% = 30 m, between floor (20) and cap (80)
        assert _distance_tolerance(10_000.0) == 30.0

    def test_long_activity_uses_cap(self):
        # 50 km target → 0.3% = 150 m, cap = 80 m wins
        assert _distance_tolerance(50_000.0) == 80.0

    def test_smooths_noisy_data(self):
        # Sine wave with noise
        data = [math.sin(i * 0.1) + (0.1 if i % 2 == 0 else -0.1) for i in range(50)]
        clean = [math.sin(i * 0.1) for i in range(50)]
        result = _apply_savgol(data, 7, 3)
        # Smoothed data should be closer to the clean sine than the noisy input
        half = 7 // 2
        noisy_err = sum((data[i] - clean[i]) ** 2 for i in range(half, 50 - half))
        smooth_err = sum((result[i] - clean[i]) ** 2 for i in range(half, 50 - half))
        assert smooth_err < noisy_err


# ---------------------------------------------------------------------------
# smooth_track – custom max_speed_mps
# ---------------------------------------------------------------------------


class TestSmoothTrackCustomMaxSpeed:
    def test_lower_max_speed_catches_more_outliers(self):
        """A lower max_speed_mps threshold should interpolate more points."""
        pts = [
            _tp(0, 1.0, 103.0),
            _tp(10, 1.001, 103.001),
            # ~1.5 km in 1 s ≈ 1500 m/s jump — always an outlier
            _tp(11, 1.015, 103.015),
            _tp(20, 1.002, 103.002),
            _tp(30, 1.003, 103.003),
        ]
        target = total_haversine_distance(pts) * 0.5

        # With strict speed limit (1 m/s) the spike is caught
        result_strict = smooth_track(pts, target, max_speed_mps=1.0)
        # The outlier point should have been interpolated
        assert result_strict[2].latitude != 1.015


# ---------------------------------------------------------------------------
# smooth_track – full mode
# ---------------------------------------------------------------------------


class TestSmoothTrackFullMode:
    def _noisy_track(self, n: int = 40) -> list[TrackPoint]:
        return [
            _tp(
                1000 + i * 10,
                1.0 + i * 0.001,
                103.0 + 0.0005 * (1 if i % 2 == 0 else -1),
            )
            for i in range(n)
        ]

    def test_full_mode_applies_smoothing(self):
        """full mode should still apply SG smoothing (largest window)."""
        pts = self._noisy_track(40)
        noisy_distance = total_haversine_distance(pts)
        target = noisy_distance * 0.5  # doesn't matter, full ignores target convergence

        result = smooth_track(pts, target, match_target=False)
        smoothed_distance = total_haversine_distance(result)
        # Smoothing should reduce the distance
        assert smoothed_distance < noisy_distance

    def test_full_mode_does_not_match_target(self):
        """full mode should not iterate to converge on the target distance."""
        pts = self._noisy_track(40)
        noisy_distance = total_haversine_distance(pts)
        # Use a very specific target – full mode shouldn't try to hit it
        target = noisy_distance * 0.95

        result_match = smooth_track(pts, target, match_target=True)
        result_full = smooth_track(pts, target, match_target=False)

        match_dist = total_haversine_distance(result_match)
        full_dist = total_haversine_distance(result_full)
        # match mode should be closer to target than full mode
        assert abs(match_dist - target) <= abs(full_dist - target)

    def test_full_mode_skips_tolerance_check(self):
        """full mode should still apply smoothing even when within tolerance."""
        pts = self._noisy_track(40)
        noisy_distance = total_haversine_distance(pts)
        # Target within tolerance of noisy distance
        target = noisy_distance + 5.0

        result = smooth_track(pts, target, match_target=False)
        # Should not be the original list (match mode would return original)
        assert result is not pts
