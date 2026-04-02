from __future__ import annotations

from mi_fitness_sync.activity.models import Activity, ActivitySample, TrackPoint
from mi_fitness_sync.activity.utils import (
    build_fds_suffix,
    find_fds_entry,
    merge_fds_samples_into_track_points,
    parse_activity_id,
    render_activities_table,
)


def test_render_activities_table_handles_empty_list():
    assert render_activities_table([]) == "No activities matched the requested time window."


def test_parse_activity_id_round_trips_list_format():
    assert parse_activity_id("sid-1:key-1:1717200000") == ("sid-1", "key-1", 1717200000)


def test_build_fds_suffix_matches_android_server_key_format():
    assert build_fds_suffix(
        sid="882963223",
        timestamp=1774263950,
        timezone_offset=28,
        sport_type=22,
        file_type=0,
    ) == "jh7BaRzY_KdJjWE97g_CXNigsrsLabYEzCPg"


def test_build_fds_suffix_strength_training_proto_type_28():
    assert build_fds_suffix(
        sid="882963223",
        timestamp=1774241243,
        timezone_offset=28,
        sport_type=28,
        file_type=0,
    ) == "28XAaRzw_KdJjWE97g_CXNigsrsLabYEzCPg"


def test_build_fds_suffix_uses_underscore_separator():
    result = build_fds_suffix(
        sid="test-sid",
        timestamp=1000000,
        timezone_offset=0,
        sport_type=1,
        file_type=0,
    )

    assert not result.startswith(":")
    assert ":" not in result.split("_", 1)[0]


def test_find_fds_entry_exact_server_key_match():
    downloads = {"abc_123": {"url": "http://x", "obj_key": "k"}}
    assert find_fds_entry(downloads, "abc", 123) == {"url": "http://x", "obj_key": "k"}


def test_find_fds_entry_no_match_returns_none():
    downloads = {"xyz": {"url": "http://x", "obj_key": "k"}}
    assert find_fds_entry(downloads, "abc", 123) is None


class TestMergeFdsSamplesIntoTrackPoints:
    def test_merges_hr_and_cadence_by_timestamp(self):
        track_points = [
            TrackPoint(
                timestamp=1000,
                latitude=31.2,
                longitude=121.5,
                altitude_meters=50.0,
                speed_mps=5.0,
                distance_meters=None,
                heart_rate=None,
                cadence=None,
                raw_point={},
            ),
            TrackPoint(
                timestamp=1001,
                latitude=31.201,
                longitude=121.501,
                altitude_meters=51.0,
                speed_mps=5.1,
                distance_meters=None,
                heart_rate=None,
                cadence=None,
                raw_point={},
            ),
        ]
        samples = [
            ActivitySample(
                timestamp=1000,
                start_time=1000,
                end_time=1000,
                duration_seconds=1,
                heart_rate=120,
                cadence=80,
                speed_mps=None,
                distance_meters=None,
                altitude_meters=None,
                steps=None,
                calories=None,
                raw_sample={},
            ),
            ActivitySample(
                timestamp=1001,
                start_time=1001,
                end_time=1001,
                duration_seconds=1,
                heart_rate=125,
                cadence=82,
                speed_mps=None,
                distance_meters=None,
                altitude_meters=None,
                steps=None,
                calories=None,
                raw_sample={},
            ),
        ]

        merge_fds_samples_into_track_points(track_points, samples)

        assert track_points[0].heart_rate == 120
        assert track_points[0].cadence == 80
        assert track_points[1].heart_rate == 125
        assert track_points[1].cadence == 82

    def test_no_overwrite_existing_values(self):
        track_points = [
            TrackPoint(
                timestamp=1000,
                latitude=31.2,
                longitude=121.5,
                altitude_meters=50.0,
                speed_mps=5.0,
                distance_meters=None,
                heart_rate=115,
                cadence=None,
                raw_point={},
            ),
        ]
        samples = [
            ActivitySample(
                timestamp=1000,
                start_time=1000,
                end_time=1000,
                duration_seconds=1,
                heart_rate=120,
                cadence=80,
                speed_mps=None,
                distance_meters=None,
                altitude_meters=None,
                steps=None,
                calories=None,
                raw_sample={},
            ),
        ]

        merge_fds_samples_into_track_points(track_points, samples)

        assert track_points[0].heart_rate == 115
        assert track_points[0].cadence == 80

    def test_unmatched_timestamps_left_alone(self):
        track_points = [
            TrackPoint(
                timestamp=1000,
                latitude=31.2,
                longitude=121.5,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=None,
                cadence=None,
                raw_point={},
            ),
        ]
        samples = [
            ActivitySample(
                timestamp=9999,
                start_time=9999,
                end_time=9999,
                duration_seconds=1,
                heart_rate=150,
                cadence=90,
                speed_mps=None,
                distance_meters=None,
                altitude_meters=None,
                steps=None,
                calories=None,
                raw_sample={},
            ),
        ]

        merge_fds_samples_into_track_points(track_points, samples)

        assert track_points[0].heart_rate is None

    def test_empty_inputs(self):
        merge_fds_samples_into_track_points([], [])