from __future__ import annotations

import gzip
from xml.etree import ElementTree as ET

from mi_fitness_sync.activities import Activity, ActivityDetail, ActivitySample, TrackPoint
from mi_fitness_sync.exports import render_export


def sample_detail() -> ActivityDetail:
    activity = Activity(
        activity_id="sid-1:key-1:1717200000",
        sid="sid-1",
        key="key-1",
        category="outdoor_run",
        sport_type=1,
        title="Morning Run",
        start_time=1717200000,
        end_time=1717200060,
        duration_seconds=60,
        distance_meters=500.0,
        calories=42,
        steps=800,
        sync_state="server",
        next_key=None,
        raw_record={"sid": "sid-1", "key": "key-1"},
        raw_report={"name": "Morning Run"},
    )
    return ActivityDetail(
        activity=activity,
        detail_sid="sid-1",
        detail_key="key-1",
        detail_time=1717200000,
        zone_name="UTC",
        zone_offset_seconds=0,
        track_points=[
            TrackPoint(
                timestamp=1717200000,
                latitude=1.1,
                longitude=2.2,
                altitude_meters=10.0,
                speed_mps=2.0,
                distance_meters=0.0,
                heart_rate=120,
                cadence=160,
                raw_point={},
            ),
            TrackPoint(
                timestamp=1717200060,
                latitude=1.2,
                longitude=2.3,
                altitude_meters=12.0,
                speed_mps=3.2,
                distance_meters=500.0,
                heart_rate=125,
                cadence=165,
                raw_point={},
            ),
        ],
        samples=[
            ActivitySample(
                timestamp=1717200000,
                start_time=1717200000,
                end_time=1717200000,
                duration_seconds=0,
                heart_rate=120,
                cadence=160,
                speed_mps=2.0,
                distance_meters=0.0,
                altitude_meters=10.0,
                steps=100,
                calories=10,
                raw_sample={},
            ),
            ActivitySample(
                timestamp=1717200060,
                start_time=1717200060,
                end_time=1717200060,
                duration_seconds=0,
                heart_rate=125,
                cadence=165,
                speed_mps=3.2,
                distance_meters=500.0,
                altitude_meters=12.0,
                steps=800,
                calories=42,
                raw_sample={},
            ),
        ],
        sport_report=None,
        raw_fitness_item={},
        raw_detail={},
    )


def test_render_export_gpx_contains_track_points():
    export = render_export(sample_detail(), "gpx")

    root = ET.fromstring(export.payload)
    namespace = {"gpx": "http://www.topografix.com/GPX/1/1"}

    assert export.file_format == "gpx"
    assert len(root.findall(".//gpx:trkpt", namespace)) == 2


def test_render_export_tcx_contains_trackpoints():
    export = render_export(sample_detail(), "tcx")

    root = ET.fromstring(export.payload)
    namespace = {"tcx": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"}

    assert export.file_format == "tcx"
    assert len(root.findall(".//tcx:Trackpoint", namespace)) == 2


def test_render_export_fit_returns_fit_bytes():
    export = render_export(sample_detail(), "fit")

    assert export.file_format == "fit"
    assert b".FIT" in export.payload[:16]


def test_render_export_gzip_wraps_output():
    export = render_export(sample_detail(), "gpx", compress=True)

    assert export.compressed is True
    assert gzip.decompress(export.payload).startswith(b"<?xml")


def _strength_detail_with_samples() -> ActivityDetail:
    """Build an ActivityDetail with FDS samples where summary calories differ from sample max."""
    activity = Activity(
        activity_id="sid-1:key-1:1717200000",
        sid="sid-1",
        key="key-1",
        category="strength_training",
        sport_type=22,
        title="Strength Training",
        start_time=1717200000,
        end_time=1717203600,
        duration_seconds=3600,
        distance_meters=0,
        calories=372,
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={},
        raw_report={},
    )
    return ActivityDetail(
        activity=activity,
        detail_sid="sid-1",
        detail_key="fds_sport_record",
        detail_time=1717200000,
        zone_name="UTC",
        zone_offset_seconds=0,
        track_points=[],
        samples=[
            ActivitySample(
                timestamp=1717200000, start_time=None, end_time=None,
                duration_seconds=None, heart_rate=100, cadence=None,
                speed_mps=None, distance_meters=None, altitude_meters=None,
                steps=None, calories=50, raw_sample={},
            ),
            ActivitySample(
                timestamp=1717201800, start_time=None, end_time=None,
                duration_seconds=None, heart_rate=145, cadence=None,
                speed_mps=None, distance_meters=None, altitude_meters=None,
                steps=None, calories=200, raw_sample={},
            ),
            ActivitySample(
                timestamp=1717203600, start_time=None, end_time=None,
                duration_seconds=None, heart_rate=110, cadence=None,
                speed_mps=None, distance_meters=None, altitude_meters=None,
                steps=None, calories=372, raw_sample={},
            ),
        ],
        sport_report=None,
        raw_fitness_item={},
        raw_detail={},
    )


def test_tcx_export_uses_summary_calories_and_sample_hr():
    """TCX lap must use summary calories and sample-derived HR values."""
    detail = _strength_detail_with_samples()
    export = render_export(detail, "tcx")
    root = ET.fromstring(export.payload)
    ns = {"tcx": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"}

    lap_calories = root.find(".//tcx:Lap/tcx:Calories", ns)
    assert lap_calories is not None
    assert int(lap_calories.text) == 372

    avg_hr = root.find(".//tcx:Lap/tcx:AverageHeartRateBpm/tcx:Value", ns)
    assert avg_hr is not None
    # avg of [100, 145, 110] = 118.33 → 118
    assert int(avg_hr.text) == 118

    max_hr = root.find(".//tcx:Lap/tcx:MaximumHeartRateBpm/tcx:Value", ns)
    assert max_hr is not None
    assert int(max_hr.text) == 145


def test_fit_export_uses_summary_calories_and_sample_hr():
    """FIT lap/session must use summary calories and sample-derived HR values."""
    from fit_tool.fit_file import FitFile
    from fit_tool.profile.messages.lap_message import LapMessage
    from fit_tool.profile.messages.session_message import SessionMessage

    detail = _strength_detail_with_samples()
    export = render_export(detail, "fit")

    assert export.file_format == "fit"
    assert b".FIT" in export.payload[:16]

    fit = FitFile.from_bytes(export.payload)
    laps = [r.message for r in fit.records if isinstance(r.message, LapMessage)]
    sessions = [r.message for r in fit.records if isinstance(r.message, SessionMessage)]

    assert len(laps) == 1
    assert laps[0].total_calories == 372
    assert laps[0].avg_heart_rate == 118
    assert laps[0].max_heart_rate == 145

    assert len(sessions) == 1
    assert sessions[0].total_calories == 372
    assert sessions[0].avg_heart_rate == 118
    assert sessions[0].max_heart_rate == 145
