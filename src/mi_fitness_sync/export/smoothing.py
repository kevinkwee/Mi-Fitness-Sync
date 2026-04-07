"""GPS track smoothing: outlier interpolation + Savitzky-Golay filter.

Adjusts GPS coordinates so that the total haversine distance of the track
converges toward a known summary distance.  Non-GPS fields (timestamps,
heart rate, cadence, etc.) are never modified.
"""

from __future__ import annotations

import math

from mi_fitness_sync.activity.models import TrackPoint

_EARTH_RADIUS_M = 6_371_000
_TOLERANCE_FLOOR_M = 20.0
_TOLERANCE_RATE = 0.003  # 0.3 %
_TOLERANCE_CAP_M = 80.0
_MAX_SPEED_MPS = 50.0  # ~180 km/h – anything faster is treated as an outlier
_MIN_WINDOW = 5
_MAX_WINDOW = 31
_POLYORDER = 3


# ---------------------------------------------------------------------------
# Coordinate helpers
# ---------------------------------------------------------------------------


def _distance_tolerance(target_distance_meters: float) -> float:
    """Bounded hybrid tolerance: max(20m, 0.3% of target) capped at 80m."""
    return max(_TOLERANCE_FLOOR_M, min(_TOLERANCE_RATE * target_distance_meters, _TOLERANCE_CAP_M))


def _has_valid_gps(p: TrackPoint) -> bool:
    """True when lat/lon are present and within WGS-84 bounds."""
    if p.latitude is None or p.longitude is None:
        return False
    return -90.0 <= p.latitude <= 90.0 and -180.0 <= p.longitude < 180.0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres between two (lat, lon) pairs."""
    lat1_r, lon1_r = math.radians(lat1), math.radians(lon1)
    lat2_r, lon2_r = math.radians(lat2), math.radians(lon2)
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    return _EARTH_RADIUS_M * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def total_haversine_distance(points: list[TrackPoint]) -> float:
    """Sum of consecutive haversine distances for points with valid coordinates."""
    valid = [(p.latitude, p.longitude) for p in points if _has_valid_gps(p)]
    if len(valid) < 2:
        return 0.0
    return sum(
        haversine(valid[i][0], valid[i][1], valid[i + 1][0], valid[i + 1][1])
        for i in range(len(valid) - 1)
    )


def smooth_track(
    track_points: list[TrackPoint],
    target_distance_meters: float,
    *,
    max_speed_mps: float = _MAX_SPEED_MPS,
    match_target: bool = True,
) -> list[TrackPoint]:
    """Smooth GPS coordinates so total haversine distance approaches *target_distance_meters*.

    Parameters
    ----------
    max_speed_mps:
        Speed threshold for outlier detection (m/s).
    match_target:
        When *True* (default, ``match`` mode), iteratively pick the SG window
        that best converges on *target_distance_meters*.  When *False*
        (``full`` mode), apply the largest valid SG window unconditionally
        after outlier removal.

    Returns the original list unchanged when:
    - fewer than two GPS points exist
    - ``target_distance_meters`` is non-positive
    - the track distance already matches the target within tolerance (match mode only)
    """
    gps_points = [p for p in track_points if _has_valid_gps(p)]
    if len(gps_points) < 2 or target_distance_meters <= 0:
        return track_points

    original_distance = total_haversine_distance(gps_points)
    if original_distance <= 0:
        return track_points

    if match_target and abs(original_distance - target_distance_meters) <= _distance_tolerance(target_distance_meters):
        return track_points

    # Step 1: interpolate outlier positions
    fixed = _fix_outliers(track_points, max_speed_mps)

    valid_fixed = [p for p in fixed if _has_valid_gps(p)]
    fixed_distance = total_haversine_distance(valid_fixed)

    if match_target:
        if fixed_distance <= 0 or abs(fixed_distance - target_distance_meters) <= _distance_tolerance(target_distance_meters):
            return fixed

        # Step 2: Savitzky-Golay – only useful when GPS distance > target
        if fixed_distance <= target_distance_meters:
            return fixed

    gps_indices = [i for i, p in enumerate(fixed) if _has_valid_gps(p)]
    if len(gps_indices) < _MIN_WINDOW:
        return fixed

    lats = [fixed[i].latitude for i in gps_indices]
    lons = [fixed[i].longitude for i in gps_indices]

    if match_target:
        # --- match mode: pick the window that best converges on target ---
        best_result = fixed
        best_diff = abs(fixed_distance - target_distance_meters)

        for window in range(_MIN_WINDOW, min(_MAX_WINDOW + 1, len(gps_indices) + 1), 2):
            if window <= _POLYORDER:
                continue

            smoothed_lats = _apply_savgol(lats, window, _POLYORDER)
            smoothed_lons = _apply_savgol(lons, window, _POLYORDER)

            candidate = list(fixed)
            for j, idx in enumerate(gps_indices):
                p = fixed[idx]
                candidate[idx] = TrackPoint(
                    timestamp=p.timestamp,
                    latitude=smoothed_lats[j],
                    longitude=smoothed_lons[j],
                    altitude_meters=p.altitude_meters,
                    speed_mps=p.speed_mps,
                    distance_meters=p.distance_meters,
                    heart_rate=p.heart_rate,
                    cadence=p.cadence,
                    raw_point=p.raw_point,
                )

            dist = total_haversine_distance([candidate[i] for i in gps_indices])
            diff = abs(dist - target_distance_meters)

            if diff < best_diff:
                best_diff = diff
                best_result = candidate

        return best_result
    else:
        # --- full mode: apply the largest valid SG window ---
        max_window = min(_MAX_WINDOW, len(gps_indices))
        if max_window % 2 == 0:
            max_window -= 1
        if max_window <= _POLYORDER:
            return fixed

        smoothed_lats = _apply_savgol(lats, max_window, _POLYORDER)
        smoothed_lons = _apply_savgol(lons, max_window, _POLYORDER)

        result = list(fixed)
        for j, idx in enumerate(gps_indices):
            p = fixed[idx]
            result[idx] = TrackPoint(
                timestamp=p.timestamp,
                latitude=smoothed_lats[j],
                longitude=smoothed_lons[j],
                altitude_meters=p.altitude_meters,
                speed_mps=p.speed_mps,
                distance_meters=p.distance_meters,
                heart_rate=p.heart_rate,
                cadence=p.cadence,
                raw_point=p.raw_point,
            )

        return result


# ---------------------------------------------------------------------------
# Outlier handling
# ---------------------------------------------------------------------------


def _fix_outliers(points: list[TrackPoint], max_speed_mps: float) -> list[TrackPoint]:
    """Replace outlier GPS coordinates with linearly interpolated positions.

    All non-GPS fields are preserved.  First and last points are never
    treated as outliers.
    """
    n = len(points)
    gps_mask = [_has_valid_gps(p) for p in points]

    if n < 3 or sum(gps_mask) < 3:
        return points[:]

    # --- identify outliers ---
    is_outlier = [False] * n
    for i in range(1, n):
        if not gps_mask[i]:
            continue
        prev_idx = i - 1
        while prev_idx > 0 and (is_outlier[prev_idx] or not gps_mask[prev_idx]):
            prev_idx -= 1
        if not gps_mask[prev_idx]:
            continue
        dt = points[i].timestamp - points[prev_idx].timestamp
        if dt <= 0:
            continue
        dist = haversine(points[prev_idx].latitude, points[prev_idx].longitude, points[i].latitude, points[i].longitude)
        if dist / dt > max_speed_mps:
            is_outlier[i] = True

    is_outlier[0] = False
    is_outlier[-1] = False

    if not any(is_outlier):
        return points[:]

    # --- interpolate outlier coordinates ---
    result = list(points)
    for i in range(n):
        if not is_outlier[i]:
            continue

        prev_idx = i - 1
        while prev_idx >= 0 and (is_outlier[prev_idx] or not gps_mask[prev_idx]):
            prev_idx -= 1
        next_idx = i + 1
        while next_idx < n and (is_outlier[next_idx] or not gps_mask[next_idx]):
            next_idx += 1

        if prev_idx < 0 or next_idx >= n:
            continue

        prev_p, next_p = points[prev_idx], points[next_idx]
        if prev_p.latitude is None or next_p.latitude is None:
            continue

        total_dt = next_p.timestamp - prev_p.timestamp
        if total_dt <= 0:
            continue
        t = (points[i].timestamp - prev_p.timestamp) / total_dt

        result[i] = TrackPoint(
            timestamp=points[i].timestamp,
            latitude=prev_p.latitude + t * (next_p.latitude - prev_p.latitude),
            longitude=prev_p.longitude + t * (next_p.longitude - prev_p.longitude),
            altitude_meters=points[i].altitude_meters,
            speed_mps=points[i].speed_mps,
            distance_meters=points[i].distance_meters,
            heart_rate=points[i].heart_rate,
            cadence=points[i].cadence,
            raw_point=points[i].raw_point,
        )

    return result


# ---------------------------------------------------------------------------
# Lightweight Savitzky-Golay filter (pure Python, no scipy/numpy needed)
# ---------------------------------------------------------------------------


def _apply_savgol(data: list[float], window_length: int, polyorder: int) -> list[float]:
    """Apply a Savitzky-Golay filter to 1-D *data*.

    Points within half-window of the edges are left unchanged.
    """
    n = len(data)
    if n < window_length:
        return data[:]

    coeffs = _savgol_coeffs(window_length, polyorder)
    half = window_length // 2
    result = data[:]
    for i in range(half, n - half):
        result[i] = sum(coeffs[j] * data[i - half + j] for j in range(window_length))
    return result


def _savgol_coeffs(window_length: int, polyorder: int) -> list[float]:
    """Compute the smoothing coefficients for the centre point of the window."""
    half = window_length // 2
    order = polyorder + 1

    # Vandermonde matrix J[k][j] = k^j, k ∈ [-half … half]
    J = [[float(k ** j) for j in range(order)] for k in range(-half, half + 1)]

    # Normal matrix JᵀJ
    JtJ = [
        [sum(J[r][i] * J[r][j] for r in range(window_length)) for j in range(order)]
        for i in range(order)
    ]

    # Solve JᵀJ · c = e₀  where e₀ = [1, 0, …, 0]
    e0 = [1.0] + [0.0] * (order - 1)
    c = _solve_linear(JtJ, e0)

    # Filter coefficients: coeffs[k] = Σ c[j]·J[k][j]
    return [sum(c[j] * J[k][j] for j in range(order)) for k in range(window_length)]


def _solve_linear(A: list[list[float]], b: list[float]) -> list[float]:
    """Solve Ax = b via Gaussian elimination with partial pivoting."""
    n = len(A)
    M = [A[i][:] + [b[i]] for i in range(n)]

    for col in range(n):
        max_row = max(range(col, n), key=lambda r: abs(M[r][col]))
        M[col], M[max_row] = M[max_row], M[col]

        pivot = M[col][col]
        if abs(pivot) < 1e-15:
            continue

        for row in range(col + 1, n):
            factor = M[row][col] / pivot
            for j in range(col, n + 1):
                M[row][j] -= factor * M[col][j]

    x = [0.0] * n
    for i in range(n - 1, -1, -1):
        s = M[i][n] - sum(M[i][j] * x[j] for j in range(i + 1, n))
        x[i] = s / M[i][i] if abs(M[i][i]) > 1e-15 else 0.0
    return x
