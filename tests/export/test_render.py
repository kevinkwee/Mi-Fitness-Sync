from __future__ import annotations

import gzip
from xml.etree import ElementTree as ET

from fit_tool.fit_file import FitFile
from fit_tool.profile.messages.activity_message import ActivityMessage
from fit_tool.profile.messages.lap_message import LapMessage
from fit_tool.profile.messages.session_message import SessionMessage
from fit_tool.profile.profile_type import Sport, SubSport

import pytest

from mi_fitness_sync.activity.models import Activity, ActivityDetail, ActivitySample, TrackPoint
from mi_fitness_sync.export.render import render_export, _clamp_heart_rate, _clamp_cadence, _is_valid_coordinate
from mi_fitness_sync.fds.sport_reports import SportReport

GPX_NS = {"gpx": "http://www.topografix.com/GPX/1/1"}
TPX_NS = {"gpxtpx": "http://www.garmin.com/xmlschemas/TrackPointExtension/v1"}
ALL_NS = {**GPX_NS, **TPX_NS}


def test_render_export_gpx_contains_track_points(sample_activity_detail):
    export = render_export(sample_activity_detail, "gpx")

    root = ET.fromstring(export.payload)
    namespace = {"gpx": "http://www.topografix.com/GPX/1/1"}

    assert export.file_format == "gpx"
    assert len(root.findall(".//gpx:trkpt", namespace)) == 1


def test_render_export_tcx_contains_trackpoints(sample_activity_detail):
    export = render_export(sample_activity_detail, "tcx")

    root = ET.fromstring(export.payload)
    namespace = {"tcx": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"}

    assert export.file_format == "tcx"
    assert len(root.findall(".//tcx:Trackpoint", namespace)) == 1


def test_render_export_fit_returns_fit_bytes(sample_activity_detail):
    export = render_export(sample_activity_detail, "fit")

    assert export.file_format == "fit"
    assert b".FIT" in export.payload[:16]


def test_render_export_gzip_wraps_output(sample_activity_detail):
    export = render_export(sample_activity_detail, "gpx", compress=True)

    assert export.compressed is True
    assert gzip.decompress(export.payload).startswith(b"<?xml")


def _strength_detail_with_samples() -> ActivityDetail:
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
                timestamp=1717200000,
                start_time=None,
                end_time=None,
                duration_seconds=None,
                heart_rate=100,
                cadence=None,
                speed_mps=None,
                distance_meters=None,
                altitude_meters=None,
                steps=None,
                calories=50,
                raw_sample={},
            ),
            ActivitySample(
                timestamp=1717201800,
                start_time=None,
                end_time=None,
                duration_seconds=None,
                heart_rate=145,
                cadence=None,
                speed_mps=None,
                distance_meters=None,
                altitude_meters=None,
                steps=None,
                calories=200,
                raw_sample={},
            ),
            ActivitySample(
                timestamp=1717203600,
                start_time=None,
                end_time=None,
                duration_seconds=None,
                heart_rate=110,
                cadence=None,
                speed_mps=None,
                distance_meters=None,
                altitude_meters=None,
                steps=None,
                calories=372,
                raw_sample={},
            ),
        ],
        sport_report=None,
        recovery_rate=None,
        raw_fitness_item={},
        raw_detail={},
    )


def test_tcx_export_uses_summary_calories_and_sample_hr():
    detail = _strength_detail_with_samples()
    export = render_export(detail, "tcx")
    root = ET.fromstring(export.payload)
    ns = {"tcx": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"}

    lap_calories = root.find(".//tcx:Lap/tcx:Calories", ns)
    avg_hr = root.find(".//tcx:Lap/tcx:AverageHeartRateBpm/tcx:Value", ns)
    max_hr = root.find(".//tcx:Lap/tcx:MaximumHeartRateBpm/tcx:Value", ns)

    assert lap_calories is not None
    assert int(lap_calories.text) == 372
    assert avg_hr is not None
    assert int(avg_hr.text) == 118
    assert max_hr is not None
    assert int(max_hr.text) == 145


def test_fit_export_uses_summary_calories_and_sample_hr():
    detail = _strength_detail_with_samples()
    export = render_export(detail, "fit")

    assert export.file_format == "fit"
    assert b".FIT" in export.payload[:16]

    fit = FitFile.from_bytes(export.payload)
    laps = [record.message for record in fit.records if isinstance(record.message, LapMessage)]
    sessions = [record.message for record in fit.records if isinstance(record.message, SessionMessage)]

    assert len(laps) == 1
    assert laps[0].total_calories == 372
    assert laps[0].avg_heart_rate == 118
    assert laps[0].max_heart_rate == 145
    assert len(sessions) == 1
    assert sessions[0].total_calories == 372
    assert sessions[0].avg_heart_rate == 118
    assert sessions[0].max_heart_rate == 145


def _parse_fit(payload: bytes) -> FitFile:
    return FitFile.from_bytes(payload)


def _fit_sessions(fit: FitFile) -> list[SessionMessage]:
    return [r.message for r in fit.records if isinstance(r.message, SessionMessage)]


def _fit_laps(fit: FitFile) -> list[LapMessage]:
    return [r.message for r in fit.records if isinstance(r.message, LapMessage)]


def _fit_activities(fit: FitFile) -> list[ActivityMessage]:
    return [r.message for r in fit.records if isinstance(r.message, ActivityMessage)]


def _outdoor_run_detail(
    *,
    sport_type: int = 1,
    category: str = "outdoor_run",
    zone_offset_seconds: int | None = 28800,
    steps: int | None = 10000,
    sport_report: SportReport | None = None,
    raw_report: dict | None = None,
) -> ActivityDetail:
    activity = Activity(
        activity_id="sid:key:1",
        sid="sid",
        key="key",
        category=category,
        sport_type=sport_type,
        title="Morning Run",
        start_time=1717200000,
        end_time=1717203600,
        duration_seconds=3600,
        distance_meters=5000,
        calories=400,
        steps=steps,
        sync_state="server",
        next_key=None,
        raw_record={},
        raw_report=raw_report or {},
    )
    return ActivityDetail(
        activity=activity,
        detail_sid="sid",
        detail_key="key",
        detail_time=1717200000,
        zone_name="Asia/Singapore",
        zone_offset_seconds=zone_offset_seconds,
        track_points=[
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=10.0, speed_mps=1.2, distance_meters=0.0, heart_rate=120, cadence=160, raw_point={}),
            TrackPoint(timestamp=1717201800, latitude=1.31, longitude=103.81, altitude_meters=25.0, speed_mps=1.5, distance_meters=2500.0, heart_rate=140, cadence=170, raw_point={}),
            TrackPoint(timestamp=1717203600, latitude=1.32, longitude=103.82, altitude_meters=15.0, speed_mps=1.3, distance_meters=5000.0, heart_rate=130, cadence=165, raw_point={}),
        ],
        samples=[],
        sport_report=sport_report,
        recovery_rate=None,
        raw_fitness_item={},
        raw_detail={},
    )


class TestFitSportSubSportMapping:
    # -- sport_type primary mapping --

    def test_outdoor_run_maps_to_running_street(self):
        detail = _outdoor_run_detail(sport_type=1)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.RUNNING.value
        assert session.sub_sport == SubSport.STREET.value

    def test_outdoor_walk_maps_to_walking(self):
        detail = _outdoor_run_detail(sport_type=2)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.WALKING.value
        assert session.sub_sport == SubSport.CASUAL_WALKING.value

    def test_treadmill_maps_to_running_treadmill(self):
        detail = _outdoor_run_detail(sport_type=3)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.RUNNING.value
        assert session.sub_sport == SubSport.TREADMILL.value

    def test_trail_run_maps_to_running_trail(self):
        detail = _outdoor_run_detail(sport_type=5)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.RUNNING.value
        assert session.sub_sport == SubSport.TRAIL.value

    def test_indoor_cycling_maps_correctly(self):
        detail = _outdoor_run_detail(sport_type=7)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.CYCLING.value
        assert session.sub_sport == SubSport.INDOOR_CYCLING.value

    def test_free_training_maps_to_training_generic(self):
        detail = _outdoor_run_detail(sport_type=8)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.TRAINING.value
        assert session.sub_sport == SubSport.GENERIC.value

    def test_pool_swimming_maps_to_lap_swimming(self):
        detail = _outdoor_run_detail(sport_type=9)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.SWIMMING.value
        assert session.sub_sport == SubSport.LAP_SWIMMING.value

    def test_hiking_maps_correctly(self):
        detail = _outdoor_run_detail(sport_type=15)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.HIKING.value
        assert session.sub_sport == SubSport.GENERIC.value

    def test_hiit_maps_to_cardio_training(self):
        detail = _outdoor_run_detail(sport_type=16)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.TRAINING.value
        assert session.sub_sport == SubSport.CARDIO_TRAINING.value

    def test_jump_rope_maps_to_cardio_training(self):
        detail = _outdoor_run_detail(sport_type=14)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.TRAINING.value
        assert session.sub_sport == SubSport.CARDIO_TRAINING.value

    def test_strength_training_308(self):
        detail = _outdoor_run_detail(sport_type=308)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.TRAINING.value
        assert session.sub_sport == SubSport.STRENGTH_TRAINING.value

    def test_indoor_walking_333(self):
        detail = _outdoor_run_detail(sport_type=333)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.WALKING.value
        assert session.sub_sport == SubSport.INDOOR_WALKING.value

    def test_snowboarding_708(self):
        detail = _outdoor_run_detail(sport_type=708)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.SNOWBOARDING.value
        assert session.sub_sport == SubSport.GENERIC.value

    def test_soccer_600(self):
        detail = _outdoor_run_detail(sport_type=600)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.SOCCER.value
        assert session.sub_sport == SubSport.GENERIC.value

    # -- proto_type fallback --

    def test_proto_type_fallback_when_sport_type_none(self):
        detail = _outdoor_run_detail(sport_type=None, raw_report={"proto_type": 1})
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.RUNNING.value
        assert session.sub_sport == SubSport.STREET.value

    def test_proto_type_track_running_fallback(self):
        detail = _outdoor_run_detail(sport_type=None, raw_report={"proto_type": 2})
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.RUNNING.value
        assert session.sub_sport == SubSport.TRACK.value

    def test_proto_type_outdoor_step_maps_to_generic(self):
        detail = _outdoor_run_detail(sport_type=None, raw_report={"proto_type": 22})
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.GENERIC.value
        assert session.sub_sport == SubSport.GENERIC.value

    # -- priority & fallback --

    def test_sport_type_takes_priority_over_proto_type(self):
        detail = _outdoor_run_detail(sport_type=15, raw_report={"proto_type": 1})
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.HIKING.value
        assert session.sub_sport == SubSport.GENERIC.value

    def test_unknown_types_fall_to_generic(self):
        detail = _outdoor_run_detail(sport_type=999, raw_report={"proto_type": 999})
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.GENERIC.value
        assert session.sub_sport == SubSport.GENERIC.value

    def test_no_sport_type_or_proto_type_falls_to_generic(self):
        detail = _outdoor_run_detail(sport_type=None)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.GENERIC.value
        assert session.sub_sport == SubSport.GENERIC.value

    def test_unmapped_sport_type_22_falls_to_generic(self):
        detail = _outdoor_run_detail(sport_type=22)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.GENERIC.value
        assert session.sub_sport == SubSport.GENERIC.value

    def test_sub_sport_set_on_lap(self):
        detail = _outdoor_run_detail(sport_type=1)
        fit = _parse_fit(render_export(detail, "fit").payload)
        lap = _fit_laps(fit)[0]
        assert lap.sport == Sport.RUNNING.value
        assert lap.sub_sport == SubSport.STREET.value


class TestFitSessionAggregates:
    def test_num_laps_is_set(self):
        detail = _outdoor_run_detail()
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.num_laps == 1

    def test_avg_speed_computed_from_distance_and_time(self):
        detail = _outdoor_run_detail()
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        expected = 5000.0 / 3600.0
        assert abs(session.avg_speed - expected) < 0.01

    def test_avg_speed_prefers_sport_report(self):
        report = SportReport(avg_speed=9.0)  # 9.0 km/h from Mi Fitness
        detail = _outdoor_run_detail(sport_report=report)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert abs(session.avg_speed - 2.5) < 0.01  # 9.0 / 3.6 = 2.5 m/s

    def test_max_speed_computed_from_points(self):
        detail = _outdoor_run_detail()
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert abs(session.max_speed - 1.5) < 0.01

    def test_max_speed_prefers_sport_report(self):
        report = SportReport(max_speed=10.8)  # 10.8 km/h from Mi Fitness
        detail = _outdoor_run_detail(sport_report=report)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert abs(session.max_speed - 3.0) < 0.01  # 10.8 / 3.6 = 3.0 m/s

    def test_avg_cadence_computed_from_points(self):
        detail = _outdoor_run_detail()
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.avg_cadence == 165

    def test_avg_cadence_prefers_sport_report(self):
        report = SportReport(avg_cadence=150)
        detail = _outdoor_run_detail(sport_report=report)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.avg_cadence == 150

    def test_total_ascent_from_sport_report(self):
        report = SportReport(rise_height=120.7)
        detail = _outdoor_run_detail(sport_report=report)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.total_ascent == 121

    def test_total_descent_from_sport_report(self):
        report = SportReport(fall_height=80.3)
        detail = _outdoor_run_detail(sport_report=report)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.total_descent == 80

    def test_total_ascent_computed_from_altitude(self):
        detail = _outdoor_run_detail()
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        # 10 -> 25 (+15), 25 -> 15 (-10)
        assert session.total_ascent == 15

    def test_total_descent_computed_from_altitude(self):
        detail = _outdoor_run_detail()
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        # 10 -> 25 (no descent), 25 -> 15 (-10)
        assert session.total_descent == 10

    def test_lap_total_ascent_set(self):
        report = SportReport(rise_height=50.0)
        detail = _outdoor_run_detail(sport_report=report)
        fit = _parse_fit(render_export(detail, "fit").payload)
        lap = _fit_laps(fit)[0]
        assert lap.total_ascent == 50


class TestFitLocalTimestamp:
    def test_local_timestamp_set_with_zone_offset(self):
        detail = _outdoor_run_detail(zone_offset_seconds=28800)
        fit = _parse_fit(render_export(detail, "fit").payload)
        activities = _fit_activities(fit)
        assert len(activities) == 1
        # local_timestamp = end_time + offset - FIT_EPOCH_OFFSET
        expected = 1717203600 + 28800 - 631065600
        assert activities[0].local_timestamp == expected

    def test_local_timestamp_omitted_when_offset_is_none(self):
        detail = _outdoor_run_detail(zone_offset_seconds=None)
        fit = _parse_fit(render_export(detail, "fit").payload)
        activities = _fit_activities(fit)
        assert len(activities) == 1
        # local_timestamp should not be set (remains None/default)
        assert activities[0].local_timestamp is None


class TestFitStridesAndStepLength:
    def test_total_strides_from_steps(self):
        detail = _outdoor_run_detail(steps=10000)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.total_strides == 5000

    def test_avg_step_length_computed(self):
        detail = _outdoor_run_detail(steps=10000)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        # 5000m / 5000 strides = 1.0m
        assert abs(session.avg_step_length - 1.0) < 0.01

    def test_no_strides_when_no_steps(self):
        detail = _outdoor_run_detail(steps=None)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.total_strides is None


# ---------------------------------------------------------------------------
# GPX compliance tests
# ---------------------------------------------------------------------------

def _gpx_detail(
    *,
    track_points: list[TrackPoint] | None = None,
) -> ActivityDetail:
    activity = Activity(
        activity_id="sid:key:1",
        sid="sid",
        key="key",
        category="outdoor_run",
        sport_type=1,
        title="Test Run",
        start_time=1717200000,
        end_time=1717203600,
        duration_seconds=3600,
        distance_meters=5000,
        calories=400,
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={},
        raw_report={},
    )
    if track_points is None:
        track_points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=10.0, speed_mps=1.2, distance_meters=0.0, heart_rate=120, cadence=80, raw_point={}),
            TrackPoint(timestamp=1717201800, latitude=1.31, longitude=103.81, altitude_meters=25.0, speed_mps=1.5, distance_meters=2500.0, heart_rate=140, cadence=85, raw_point={}),
            TrackPoint(timestamp=1717203600, latitude=1.32, longitude=103.82, altitude_meters=15.0, speed_mps=1.3, distance_meters=5000.0, heart_rate=130, cadence=82, raw_point={}),
        ]
    return ActivityDetail(
        activity=activity,
        detail_sid="sid",
        detail_key="key",
        detail_time=1717200000,
        zone_name="UTC",
        zone_offset_seconds=0,
        track_points=track_points,
        samples=[],
        sport_report=None,
        recovery_rate=None,
        raw_fitness_item={},
        raw_detail={},
    )


class TestGpxSchemaLocation:
    def test_garmin_xsd_url_is_correct(self):
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        schema_loc = root.attrib["{http://www.w3.org/2001/XMLSchema-instance}schemaLocation"]
        assert "http://www.garmin.com/xmlschemas/TrackPointExtensionv1.xsd" in schema_loc
        # Must NOT contain the old broken path
        assert "TrackPointExtension/v1/TrackPointExtensionv1.xsd" not in schema_loc

    def test_gpx_schema_location_present(self):
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        schema_loc = root.attrib["{http://www.w3.org/2001/XMLSchema-instance}schemaLocation"]
        assert "http://www.topografix.com/GPX/1/1/gpx.xsd" in schema_loc


class TestGpxCreator:
    def test_creator_contains_barometer_hint_when_altitude_present(self):
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        assert root.attrib["creator"] == "Mi Fitness Sync with barometer"

    def test_creator_omits_barometer_hint_when_no_altitude(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=1.2, distance_meters=0.0, heart_rate=120, cadence=80, raw_point={}),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        assert root.attrib["creator"] == "Mi Fitness Sync"

    def test_creator_starts_with_app_name(self):
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        assert root.attrib["creator"].startswith("Mi Fitness Sync")


class TestGpxHeartRateAndCadenceBounds:
    """Garmin TrackPointExtension v1 schema bounds:
    - BeatsPerMinute_t: xsd:unsignedByte, minInclusive=1 → [1, 255]
    - RevolutionsPerMinute_t: xsd:unsignedByte, maxInclusive=254 → [0, 254]
    """

    # -- _clamp_heart_rate unit tests --

    def test_hr_normal_value(self):
        assert _clamp_heart_rate(120) == 120

    def test_hr_min_valid(self):
        assert _clamp_heart_rate(1) == 1

    def test_hr_max_valid(self):
        assert _clamp_heart_rate(255) == 255

    def test_hr_zero_returns_none(self):
        assert _clamp_heart_rate(0) is None

    def test_hr_negative_returns_none(self):
        assert _clamp_heart_rate(-5) is None

    def test_hr_over_255_clamped(self):
        assert _clamp_heart_rate(300) == 255

    # -- _clamp_cadence unit tests --

    def test_cad_normal_value(self):
        assert _clamp_cadence(80) == 80

    def test_cad_zero_valid(self):
        assert _clamp_cadence(0) == 0

    def test_cad_max_valid(self):
        assert _clamp_cadence(254) == 254

    def test_cad_over_254_clamped(self):
        assert _clamp_cadence(260) == 254

    def test_cad_negative_clamped_to_zero(self):
        assert _clamp_cadence(-5) == 0

    # -- GPX integration tests --

    def test_hr_clamped_in_gpx_output(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=300, cadence=None, raw_point={}),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        hr = root.find(".//gpxtpx:TrackPointExtension/gpxtpx:hr", TPX_NS)
        assert hr is not None
        assert int(hr.text) == 255

    def test_cadence_clamped_in_gpx_output(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=None, cadence=260, raw_point={}),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        cad = root.find(".//gpxtpx:TrackPointExtension/gpxtpx:cad", TPX_NS)
        assert cad is not None
        assert int(cad.text) == 254

    def test_zero_hr_omitted_from_gpx(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=0, cadence=80, raw_point={}),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        hr = root.find(".//gpxtpx:TrackPointExtension/gpxtpx:hr", TPX_NS)
        assert hr is None
        # cadence should still be present
        cad = root.find(".//gpxtpx:TrackPointExtension/gpxtpx:cad", TPX_NS)
        assert cad is not None

    def test_negative_hr_omitted_from_gpx(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=-10, cadence=80, raw_point={}),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        hr = root.find(".//gpxtpx:TrackPointExtension/gpxtpx:hr", TPX_NS)
        assert hr is None

    def test_zero_cadence_emitted_in_gpx(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=120, cadence=0, raw_point={}),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        cad = root.find(".//gpxtpx:TrackPointExtension/gpxtpx:cad", TPX_NS)
        assert cad is not None
        assert int(cad.text) == 0


class TestGpxCoordinateBoundsCheck:
    def test_valid_coordinate(self):
        assert _is_valid_coordinate(45.0, 90.0) is True

    def test_none_latitude(self):
        assert _is_valid_coordinate(None, 90.0) is False

    def test_none_longitude(self):
        assert _is_valid_coordinate(45.0, None) is False

    def test_lat_too_high(self):
        assert _is_valid_coordinate(91.0, 0.0) is False

    def test_lat_too_low(self):
        assert _is_valid_coordinate(-91.0, 0.0) is False

    def test_lon_at_180_excluded(self):
        assert _is_valid_coordinate(0.0, 180.0) is False

    def test_lon_at_negative_180_included(self):
        assert _is_valid_coordinate(0.0, -180.0) is True

    def test_lat_boundary_90(self):
        assert _is_valid_coordinate(90.0, 0.0) is True

    def test_lat_boundary_neg_90(self):
        assert _is_valid_coordinate(-90.0, 0.0) is True

    def test_lon_just_under_180(self):
        assert _is_valid_coordinate(0.0, 179.9999999) is True

    def test_invalid_points_excluded_from_gpx(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=10.0, speed_mps=None, distance_meters=None, heart_rate=120, cadence=None, raw_point={}),
            TrackPoint(timestamp=1717200060, latitude=91.0, longitude=103.8, altitude_meters=10.0, speed_mps=None, distance_meters=None, heart_rate=130, cadence=None, raw_point={}),
            TrackPoint(timestamp=1717200120, latitude=1.31, longitude=200.0, altitude_meters=10.0, speed_mps=None, distance_meters=None, heart_rate=140, cadence=None, raw_point={}),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        trkpts = root.findall(".//gpx:trkpt", GPX_NS)
        assert len(trkpts) == 1
        assert trkpts[0].attrib["lat"] == "1.30000000"

    def test_all_invalid_coordinates_raises(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=91.0, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=None, cadence=None, raw_point={}),
        ]
        detail = _gpx_detail(track_points=points)
        with pytest.raises(Exception, match="GPS track points"):
            render_export(detail, "gpx")


class TestGpxElementOrdering:
    """Verify all elements are in strict GPX 1.1 XSD sequence order."""

    def _child_local_names(self, element: ET.Element) -> list[str]:
        """Return local names (without namespace) for all child elements."""
        return [child.tag.split("}")[-1] if "}" in child.tag else child.tag for child in element]

    def test_gpx_root_children_order(self):
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        children = self._child_local_names(root)
        # gpxType: metadata, wpt*, rte*, trk*, extensions
        assert children == ["metadata", "trk"]

    def test_metadata_children_order(self):
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        metadata = root.find("{http://www.topografix.com/GPX/1/1}metadata")
        children = self._child_local_names(metadata)
        # metadataType: name, desc, author, copyright, link*, time, keywords, bounds, extensions
        assert children == ["name", "time"]

    def test_trk_children_order(self):
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        trk = root.find("{http://www.topografix.com/GPX/1/1}trk")
        children = self._child_local_names(trk)
        # trkType: name, cmt, desc, src, link*, number, type, extensions, trkseg*
        assert children == ["name", "type", "trkseg"]

    def test_trkpt_children_order(self):
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        trkpt = root.find(".//gpx:trkpt", GPX_NS)
        children = self._child_local_names(trkpt)
        # wptType: ele, time, ..., extensions
        assert children == ["ele", "time", "extensions"]

    def test_trkpt_without_altitude_order(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=100.0, heart_rate=120, cadence=80, raw_point={}),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        trkpt = root.find(".//gpx:trkpt", GPX_NS)
        children = self._child_local_names(trkpt)
        # ele omitted, so time then extensions
        assert children == ["time", "extensions"]

    def test_extension_children_order(self):
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        extensions = root.find(".//gpx:trkpt/gpx:extensions", GPX_NS)
        children = self._child_local_names(extensions)
        assert children == ["TrackPointExtension"]

    def test_trackpoint_extension_children_order(self):
        """hr must come before cad per Garmin TPE v1 XSD."""
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        tpe = root.find(".//gpxtpx:TrackPointExtension", TPX_NS)
        children = self._child_local_names(tpe)
        assert children == ["hr", "cad"]