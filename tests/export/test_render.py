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
from mi_fitness_sync.export.render import render_export, _clamp_heart_rate, _clamp_cadence, _is_valid_coordinate, _clamp_calories, _cadence_spm_to_rpm
from mi_fitness_sync.export.smoothing import smooth_track, total_haversine_distance
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
        assert session.avg_cadence == 82  # (160+170+165)/3=165 SPM -> 165//2=82 RPM

    def test_avg_cadence_prefers_sport_report(self):
        report = SportReport(avg_cadence=150)
        detail = _outdoor_run_detail(sport_report=report)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.avg_cadence == 75  # 150 SPM -> 150//2=75 RPM

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
        assert root.attrib["creator"] == "Strava with barometer"

    def test_creator_omits_barometer_hint_when_no_altitude(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=1.2, distance_meters=0.0, heart_rate=120, cadence=80, raw_point={}),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        assert root.attrib["creator"] == "Strava"

    def test_creator_starts_with_app_name(self):
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        assert root.attrib["creator"].startswith("Strava")


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
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=None, cadence=520, raw_point={}),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        cad = root.find(".//gpxtpx:TrackPointExtension/gpxtpx:cad", TPX_NS)
        assert cad is not None
        # 520 SPM (running) -> 260 RPM -> clamped to 254
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


# ---------------------------------------------------------------------------
# TCX compliance tests
# ---------------------------------------------------------------------------

TCX_NS_MAP = {"tcx": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"}
AEXT_NS_MAP = {"aext": "http://www.garmin.com/xmlschemas/ActivityExtension/v2"}
ALL_TCX_NS = {**TCX_NS_MAP, **AEXT_NS_MAP}


def _tcx_detail(
    *,
    sport_type: int = 1,
    track_points: list[TrackPoint] | None = None,
    calories: int = 400,
) -> ActivityDetail:
    activity = Activity(
        activity_id="sid:key:1",
        sid="sid",
        key="key",
        category="outdoor_run",
        sport_type=sport_type,
        title="Test Run",
        start_time=1717200000,
        end_time=1717203600,
        duration_seconds=3600,
        distance_meters=5000,
        calories=calories,
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


def _tcx_child_local_names(element: ET.Element) -> list[str]:
    return [child.tag.split("}")[-1] if "}" in child.tag else child.tag for child in element]


class TestTcxLapElementOrdering:
    """Verify Lap children follow ActivityLap_t XSD sequence:
    TotalTimeSeconds, DistanceMeters, MaximumSpeed, Calories,
    AverageHeartRateBpm, MaximumHeartRateBpm, Intensity, Cadence,
    TriggerMethod, Track, Notes, Extensions
    """

    def test_lap_children_order_with_hr(self):
        detail = _tcx_detail()
        root = ET.fromstring(render_export(detail, "tcx").payload)
        lap = root.find(".//tcx:Lap", TCX_NS_MAP)
        children = _tcx_child_local_names(lap)
        assert children == [
            "TotalTimeSeconds", "DistanceMeters", "Calories",
            "AverageHeartRateBpm", "MaximumHeartRateBpm",
            "Intensity", "TriggerMethod", "Track",
        ]

    def test_lap_children_order_without_hr(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=10.0, speed_mps=1.2, distance_meters=0.0, heart_rate=None, cadence=None, raw_point={}),
        ]
        detail = _tcx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        lap = root.find(".//tcx:Lap", TCX_NS_MAP)
        children = _tcx_child_local_names(lap)
        assert children == [
            "TotalTimeSeconds", "DistanceMeters", "Calories",
            "Intensity", "TriggerMethod", "Track",
        ]


class TestTcxTrackpointElementOrdering:
    """Verify Trackpoint children follow Trackpoint_t XSD sequence:
    Time, Position, AltitudeMeters, DistanceMeters,
    HeartRateBpm, Cadence, SensorState, Extensions
    """

    def test_full_trackpoint_order_biking(self):
        detail = _tcx_detail(sport_type=6)  # Biking
        root = ET.fromstring(render_export(detail, "tcx").payload)
        tp = root.find(".//tcx:Trackpoint", TCX_NS_MAP)
        children = _tcx_child_local_names(tp)
        # Biking: base Cadence, Extensions has Speed only
        assert children == ["Time", "Position", "AltitudeMeters", "DistanceMeters", "HeartRateBpm", "Cadence", "Extensions"]

    def test_full_trackpoint_order_running(self):
        detail = _tcx_detail(sport_type=1)  # Running
        root = ET.fromstring(render_export(detail, "tcx").payload)
        tp = root.find(".//tcx:Trackpoint", TCX_NS_MAP)
        children = _tcx_child_local_names(tp)
        # Running: no base Cadence, Extensions has Speed + RunCadence
        assert children == ["Time", "Position", "AltitudeMeters", "DistanceMeters", "HeartRateBpm", "Extensions"]


class TestTcxHeartRateClamping:
    def test_trackpoint_hr_clamped_to_255(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=300, cadence=None, raw_point={}),
        ]
        detail = _tcx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        val = root.find(".//tcx:Trackpoint/tcx:HeartRateBpm/tcx:Value", TCX_NS_MAP)
        assert val is not None
        assert int(val.text) == 255

    def test_trackpoint_hr_zero_omitted(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=0, cadence=None, raw_point={}),
        ]
        detail = _tcx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        val = root.find(".//tcx:Trackpoint/tcx:HeartRateBpm", TCX_NS_MAP)
        assert val is None

    def test_lap_avg_hr_clamped(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=None, longitude=None, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=300, cadence=None, raw_point={}),
        ]
        detail = _tcx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        val = root.find(".//tcx:Lap/tcx:AverageHeartRateBpm/tcx:Value", TCX_NS_MAP)
        assert val is not None
        assert int(val.text) == 255

    def test_lap_max_hr_clamped(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=None, longitude=None, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=300, cadence=None, raw_point={}),
        ]
        detail = _tcx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        val = root.find(".//tcx:Lap/tcx:MaximumHeartRateBpm/tcx:Value", TCX_NS_MAP)
        assert val is not None
        assert int(val.text) == 255


class TestTcxCadenceClamping:
    def test_bike_cadence_clamped_to_254(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=None, cadence=260, raw_point={}),
        ]
        detail = _tcx_detail(sport_type=6, track_points=points)  # Biking
        root = ET.fromstring(render_export(detail, "tcx").payload)
        cad = root.find(".//tcx:Trackpoint/tcx:Cadence", TCX_NS_MAP)
        assert cad is not None
        assert int(cad.text) == 254

    def test_run_cadence_clamped_in_extension(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=None, cadence=520, raw_point={}),
        ]
        detail = _tcx_detail(sport_type=1, track_points=points)  # Running
        root = ET.fromstring(render_export(detail, "tcx").payload)
        # Base Cadence should NOT be present for running
        base_cad = root.find(".//tcx:Trackpoint/tcx:Cadence", TCX_NS_MAP)
        assert base_cad is None
        # RunCadence in Extensions: 520 SPM -> 260 RPM -> clamped to 254
        run_cad = root.find(".//tcx:Trackpoint/tcx:Extensions/aext:TPX/aext:RunCadence", ALL_TCX_NS)
        assert run_cad is not None
        assert int(run_cad.text) == 254


class TestTcxCoordinateValidation:
    def test_invalid_coords_excluded_from_tcx(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=None, cadence=None, raw_point={}),
            TrackPoint(timestamp=1717200060, latitude=91.0, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=None, cadence=None, raw_point={}),
            TrackPoint(timestamp=1717200120, latitude=1.31, longitude=200.0, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=None, cadence=None, raw_point={}),
        ]
        detail = _tcx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        positions = root.findall(".//tcx:Trackpoint/tcx:Position", TCX_NS_MAP)
        assert len(positions) == 1
        lat = positions[0].find("tcx:LatitudeDegrees", TCX_NS_MAP)
        assert lat.text == "1.30000000"


class TestTcxCaloriesBounding:
    def test_clamp_calories_normal(self):
        assert _clamp_calories(400) == 400

    def test_clamp_calories_zero(self):
        assert _clamp_calories(0) == 0

    def test_clamp_calories_max_valid(self):
        assert _clamp_calories(65535) == 65535

    def test_clamp_calories_over_max(self):
        assert _clamp_calories(70000) == 65535

    def test_clamp_calories_negative(self):
        assert _clamp_calories(-5) == 0

    def test_calories_clamped_in_tcx_output(self):
        detail = _tcx_detail(calories=70000)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        cal = root.find(".//tcx:Lap/tcx:Calories", TCX_NS_MAP)
        assert int(cal.text) == 65535


class TestTcxSportAttribute:
    def test_running_sport(self):
        detail = _tcx_detail(sport_type=1)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        activity = root.find(".//tcx:Activity", TCX_NS_MAP)
        assert activity.attrib["Sport"] == "Running"

    def test_biking_sport(self):
        detail = _tcx_detail(sport_type=6)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        activity = root.find(".//tcx:Activity", TCX_NS_MAP)
        assert activity.attrib["Sport"] == "Biking"

    def test_hiking_maps_to_other(self):
        detail = _tcx_detail(sport_type=15)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        activity = root.find(".//tcx:Activity", TCX_NS_MAP)
        assert activity.attrib["Sport"] == "Other"

    def test_swimming_maps_to_other(self):
        detail = _tcx_detail(sport_type=9)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        activity = root.find(".//tcx:Activity", TCX_NS_MAP)
        assert activity.attrib["Sport"] == "Other"


class TestTcxCreatorElement:
    def test_creator_present(self):
        detail = _tcx_detail()
        root = ET.fromstring(render_export(detail, "tcx").payload)
        creator = root.find(".//tcx:Activity/tcx:Creator", TCX_NS_MAP)
        assert creator is not None

    def test_creator_barometer_hint_with_altitude(self):
        detail = _tcx_detail()  # default points have altitude
        root = ET.fromstring(render_export(detail, "tcx").payload)
        name = root.find(".//tcx:Activity/tcx:Creator/tcx:Name", TCX_NS_MAP)
        assert name.text == "Strava with barometer"

    def test_creator_no_barometer_without_altitude(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=120, cadence=None, raw_point={}),
        ]
        detail = _tcx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        name = root.find(".//tcx:Activity/tcx:Creator/tcx:Name", TCX_NS_MAP)
        assert name.text == "Strava"

    def test_creator_has_device_type(self):
        detail = _tcx_detail()
        root = ET.fromstring(render_export(detail, "tcx").payload)
        creator = root.find(".//tcx:Activity/tcx:Creator", TCX_NS_MAP)
        assert creator.attrib.get("{http://www.w3.org/2001/XMLSchema-instance}type") == "Device_t"

    def test_creator_has_required_children(self):
        detail = _tcx_detail()
        root = ET.fromstring(render_export(detail, "tcx").payload)
        creator = root.find(".//tcx:Activity/tcx:Creator", TCX_NS_MAP)
        children = _tcx_child_local_names(creator)
        assert children == ["Name", "UnitId", "ProductID", "Version"]

    def test_product_id_equals_102(self):
        detail = _tcx_detail()
        root = ET.fromstring(render_export(detail, "tcx").payload)
        product_id = root.find(".//tcx:Activity/tcx:Creator/tcx:ProductID", TCX_NS_MAP)
        assert product_id is not None
        assert product_id.text == "102"

    def test_creator_after_lap(self):
        """Creator must appear after Lap (and optional Notes/Training) in Activity_t sequence."""
        detail = _tcx_detail()
        root = ET.fromstring(render_export(detail, "tcx").payload)
        activity = root.find(".//tcx:Activity", TCX_NS_MAP)
        children = _tcx_child_local_names(activity)
        assert children == ["Id", "Lap", "Creator"]


class TestTcxRunningCadence:
    def test_running_cadence_in_extensions(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=None, cadence=160, raw_point={}),
        ]
        detail = _tcx_detail(sport_type=1, track_points=points)  # Running
        root = ET.fromstring(render_export(detail, "tcx").payload)
        run_cad = root.find(".//tcx:Trackpoint/tcx:Extensions/aext:TPX/aext:RunCadence", ALL_TCX_NS)
        assert run_cad is not None
        assert int(run_cad.text) == 80  # 160 SPM -> 160//2=80 RPM

    def test_running_no_base_cadence(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=None, cadence=160, raw_point={}),
        ]
        detail = _tcx_detail(sport_type=1, track_points=points)  # Running
        root = ET.fromstring(render_export(detail, "tcx").payload)
        base_cad = root.find(".//tcx:Trackpoint/tcx:Cadence", TCX_NS_MAP)
        assert base_cad is None

    def test_biking_uses_base_cadence(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=None, cadence=80, raw_point={}),
        ]
        detail = _tcx_detail(sport_type=6, track_points=points)  # Biking
        root = ET.fromstring(render_export(detail, "tcx").payload)
        base_cad = root.find(".//tcx:Trackpoint/tcx:Cadence", TCX_NS_MAP)
        assert base_cad is not None
        assert int(base_cad.text) == 80
        # No RunCadence for biking
        run_cad = root.find(".//tcx:Trackpoint/tcx:Extensions/aext:TPX/aext:RunCadence", ALL_TCX_NS)
        assert run_cad is None

    def test_running_speed_and_cadence_in_same_tpx(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=2.5, distance_meters=None, heart_rate=None, cadence=160, raw_point={}),
        ]
        detail = _tcx_detail(sport_type=1, track_points=points)  # Running
        root = ET.fromstring(render_export(detail, "tcx").payload)
        tpx = root.find(".//tcx:Trackpoint/tcx:Extensions/aext:TPX", ALL_TCX_NS)
        assert tpx is not None
        tpx_children = _tcx_child_local_names(tpx)
        assert tpx_children == ["Speed", "RunCadence"]


# ---------------------------------------------------------------------------
# Cadence SPM→RPM conversion tests
# ---------------------------------------------------------------------------

from fit_tool.profile.messages.record_message import RecordMessage


def _cycling_detail(*, cadence: int = 80) -> ActivityDetail:
    activity = Activity(
        activity_id="sid:key:1",
        sid="sid",
        key="key",
        category="outdoor_cycling",
        sport_type=6,
        title="Test Ride",
        start_time=1717200000,
        end_time=1717203600,
        duration_seconds=3600,
        distance_meters=20000,
        calories=500,
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={},
        raw_report={},
    )
    return ActivityDetail(
        activity=activity,
        detail_sid="sid",
        detail_key="key",
        detail_time=1717200000,
        zone_name="UTC",
        zone_offset_seconds=0,
        track_points=[
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=10.0, speed_mps=5.0, distance_meters=0.0, heart_rate=120, cadence=cadence, raw_point={}),
            TrackPoint(timestamp=1717203600, latitude=1.32, longitude=103.82, altitude_meters=15.0, speed_mps=6.0, distance_meters=20000.0, heart_rate=140, cadence=cadence + 10, raw_point={}),
        ],
        samples=[],
        sport_report=None,
        recovery_rate=None,
        raw_fitness_item={},
        raw_detail={},
    )


class TestCadenceSpmToRpm:
    def test_running_cadence_halved(self):
        assert _cadence_spm_to_rpm(160, 1) == 80

    def test_walking_cadence_halved(self):
        assert _cadence_spm_to_rpm(120, 2) == 60

    def test_treadmill_cadence_halved(self):
        assert _cadence_spm_to_rpm(170, 3) == 85

    def test_trail_run_cadence_halved(self):
        assert _cadence_spm_to_rpm(150, 5) == 75

    def test_cycling_cadence_unchanged(self):
        assert _cadence_spm_to_rpm(80, 6) == 80

    def test_indoor_cycling_cadence_unchanged(self):
        assert _cadence_spm_to_rpm(90, 7) == 90

    def test_bmx_cadence_unchanged(self):
        assert _cadence_spm_to_rpm(70, 206) == 70

    def test_spinning_cadence_unchanged(self):
        assert _cadence_spm_to_rpm(95, 324) == 95

    def test_odd_spm_rounds_down(self):
        assert _cadence_spm_to_rpm(165, 1) == 82

    def test_zero_cadence(self):
        assert _cadence_spm_to_rpm(0, 1) == 0

    def test_none_sport_type_treated_as_step_based(self):
        assert _cadence_spm_to_rpm(160, None) == 80


class TestFitCadenceConversion:
    def test_running_record_cadence_is_rpm(self):
        detail = _outdoor_run_detail()
        fit = _parse_fit(render_export(detail, "fit").payload)
        records = [r.message for r in fit.records if isinstance(r.message, RecordMessage)]
        # First point: 160 SPM -> 80 RPM
        assert records[0].cadence == 80
        # Second point: 170 SPM -> 85 RPM
        assert records[1].cadence == 85
        # Third point: 165 SPM -> 82 RPM
        assert records[2].cadence == 82

    def test_cycling_record_cadence_unchanged(self):
        detail = _cycling_detail(cadence=80)
        fit = _parse_fit(render_export(detail, "fit").payload)
        records = [r.message for r in fit.records if isinstance(r.message, RecordMessage)]
        assert records[0].cadence == 80
        assert records[1].cadence == 90  # cadence + 10

    def test_cycling_avg_cadence_unchanged(self):
        detail = _cycling_detail(cadence=80)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.avg_cadence == 85  # (80 + 90) / 2, no halving


class TestGpxCadenceConversion:
    def test_running_gpx_cadence_is_rpm(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=None, cadence=160, raw_point={}),
        ]
        detail = _gpx_detail(track_points=points)  # sport_type=1 (Running)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        cad = root.find(".//gpxtpx:TrackPointExtension/gpxtpx:cad", TPX_NS)
        assert cad is not None
        assert int(cad.text) == 80  # 160 SPM -> 80 RPM

    def test_cycling_gpx_cadence_unchanged(self):
        activity = Activity(
            activity_id="sid:key:1", sid="sid", key="key", category="outdoor_cycling",
            sport_type=6, title="Ride", start_time=1717200000, end_time=1717203600,
            duration_seconds=3600, distance_meters=20000, calories=500, steps=None,
            sync_state="server", next_key=None, raw_record={}, raw_report={},
        )
        detail = ActivityDetail(
            activity=activity, detail_sid="sid", detail_key="key",
            detail_time=1717200000, zone_name="UTC", zone_offset_seconds=0,
            track_points=[
                TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=None, cadence=80, raw_point={}),
            ],
            samples=[], sport_report=None, recovery_rate=None,
            raw_fitness_item={}, raw_detail={},
        )
        root = ET.fromstring(render_export(detail, "gpx").payload)
        cad = root.find(".//gpxtpx:TrackPointExtension/gpxtpx:cad", TPX_NS)
        assert cad is not None
        assert int(cad.text) == 80  # Cycling: 80 RPM stays 80


class TestTcxCadenceConversion:
    def test_running_run_cadence_halved(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=None, cadence=170, raw_point={}),
        ]
        detail = _tcx_detail(sport_type=1, track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        run_cad = root.find(".//tcx:Trackpoint/tcx:Extensions/aext:TPX/aext:RunCadence", ALL_TCX_NS)
        assert run_cad is not None
        assert int(run_cad.text) == 85  # 170 SPM -> 85 RPM

    def test_cycling_base_cadence_unchanged(self):
        points = [
            TrackPoint(timestamp=1717200000, latitude=1.3, longitude=103.8, altitude_meters=None, speed_mps=None, distance_meters=None, heart_rate=None, cadence=90, raw_point={}),
        ]
        detail = _tcx_detail(sport_type=6, track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        cad = root.find(".//tcx:Trackpoint/tcx:Cadence", TCX_NS_MAP)
        assert cad is not None
        assert int(cad.text) == 90  # Cycling: 90 RPM stays 90


# ---------------------------------------------------------------------------
# Smoothing integration — verify smoothed coordinates propagate to all formats
# ---------------------------------------------------------------------------


def _noisy_gps_detail(distance_meters: float) -> ActivityDetail:
    """Activity with a zigzagging GPS track whose haversine distance exceeds *distance_meters*."""
    n = 40
    points = [
        TrackPoint(
            timestamp=1717200000 + i * 10,
            latitude=1.0 + i * 0.001,
            longitude=103.0 + 0.0005 * (1 if i % 2 == 0 else -1),
            altitude_meters=10.0,
            speed_mps=1.5,
            distance_meters=None,
            heart_rate=120,
            cadence=160,
            raw_point={},
        )
        for i in range(n)
    ]
    activity = Activity(
        activity_id="sid:key:1",
        sid="sid",
        key="key",
        category="outdoor_run",
        sport_type=1,
        title="Noisy Run",
        start_time=1717200000,
        end_time=1717200000 + (n - 1) * 10,
        duration_seconds=(n - 1) * 10,
        distance_meters=distance_meters,
        calories=200,
        steps=5000,
        sync_state="server",
        next_key=None,
        raw_record={},
        raw_report={},
    )
    return ActivityDetail(
        activity=activity,
        detail_sid="sid",
        detail_key="key",
        detail_time=1717200000,
        zone_name="UTC",
        zone_offset_seconds=0,
        track_points=points,
        samples=[],
        sport_report=None,
        recovery_rate=None,
        raw_fitness_item={},
        raw_detail={},
    )


class TestSmoothingIntegration:
    """Smoothed coordinates must propagate into GPX, TCX, and FIT outputs."""

    def _make_detail(self) -> ActivityDetail:
        """Detail whose noisy GPS distance exceeds the summary distance."""
        raw_points = [
            TrackPoint(
                timestamp=1717200000 + i * 10,
                latitude=1.0 + i * 0.001,
                longitude=103.0 + 0.0005 * (1 if i % 2 == 0 else -1),
                altitude_meters=10.0,
                speed_mps=1.5,
                distance_meters=None,
                heart_rate=120,
                cadence=160,
                raw_point={},
            )
            for i in range(40)
        ]
        noisy_distance = total_haversine_distance(raw_points)
        # Clean distance (straight line) is used as the summary distance
        clean_points = [
            TrackPoint(
                timestamp=1717200000 + i * 10,
                latitude=1.0 + i * 0.001, longitude=103.0,
                altitude_meters=None, speed_mps=None, distance_meters=None,
                heart_rate=None, cadence=None, raw_point={},
            )
            for i in range(40)
        ]
        clean_distance = total_haversine_distance(clean_points)
        assert noisy_distance > clean_distance * 1.01  # sanity
        return _noisy_gps_detail(clean_distance), raw_points

    def test_gpx_uses_smoothed_coords(self):
        detail, original_points = self._make_detail()
        export = render_export(detail, "gpx")
        root = ET.fromstring(export.payload)
        ns = {"gpx": "http://www.topografix.com/GPX/1/1"}
        trkpts = root.findall(".//gpx:trkpt", ns)
        assert len(trkpts) == 40
        # At least one interior point's longitude should differ from the original zigzag
        changed = any(
            float(tp.attrib["lon"]) != original_points[i].longitude
            for i, tp in enumerate(trkpts)
        )
        assert changed, "GPX output should contain smoothed (different) coordinates"

    def test_tcx_uses_smoothed_coords(self):
        detail, original_points = self._make_detail()
        export = render_export(detail, "tcx")
        root = ET.fromstring(export.payload)
        ns = {"tcx": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"}
        positions = root.findall(".//tcx:Trackpoint/tcx:Position", ns)
        assert len(positions) == 40
        changed = any(
            float(pos.find("tcx:LongitudeDegrees", ns).text) != original_points[i].longitude
            for i, pos in enumerate(positions)
        )
        assert changed, "TCX output should contain smoothed (different) coordinates"

    def test_fit_uses_smoothed_coords(self):
        detail, original_points = self._make_detail()
        # Independently compute expected smoothed coordinates
        expected_smoothed = smooth_track(detail.track_points, detail.activity.distance_meters)
        assert expected_smoothed is not detail.track_points, "smooth_track should have modified coords"

        export = render_export(detail, "fit")
        fit = FitFile.from_bytes(export.payload)
        records = [r.message for r in fit.records if isinstance(r.message, RecordMessage)]
        assert len(records) == 40

        # Decoded FIT coords should be closer to the smoothed coords than to the originals
        closer_to_smoothed = 0
        total_compared = 0
        for i in range(len(records)):
            if records[i].position_long is None:
                continue
            fit_lon = records[i].position_long
            original_lon = original_points[i].longitude
            smoothed_lon = expected_smoothed[i].longitude
            if abs(original_lon - smoothed_lon) < 1e-12:
                continue  # skip points where smoothing didn't change the value
            total_compared += 1
            if abs(fit_lon - smoothed_lon) < abs(fit_lon - original_lon):
                closer_to_smoothed += 1

        assert total_compared > 0, "At least some points should differ between original and smoothed"
        assert closer_to_smoothed == total_compared, (
            f"All compared FIT coords should be closer to smoothed than original, "
            f"but only {closer_to_smoothed}/{total_compared} were"
        )


# ---------------------------------------------------------------------------
# render_export smoothing options
# ---------------------------------------------------------------------------


class TestRenderExportSmoothingOptions:
    def test_no_smooth_skips_smoothing(self, sample_activity_detail):
        """When smooth=False, track_points should pass through unchanged."""
        export = render_export(sample_activity_detail, "gpx", smooth=False)
        assert export.file_format == "gpx"
        # Should still produce valid output
        root = ET.fromstring(export.payload)
        assert len(root.findall(".//gpx:trkpt", GPX_NS)) == 1

    def test_smooth_mode_full_accepted(self, sample_activity_detail):
        """smooth_mode='full' should not raise."""
        export = render_export(sample_activity_detail, "gpx", smooth_mode="full")
        assert export.file_format == "gpx"

    def test_outlier_speed_mps_accepted(self, sample_activity_detail):
        """Custom outlier_speed_mps should not raise."""
        export = render_export(sample_activity_detail, "gpx", outlier_speed_mps=10.0)
        assert export.file_format == "gpx"


def _pool_swim_detail(raw_report: dict | None = None, *, sport_type: int = 9) -> ActivityDetail:
    report = {
        "avg_stroke_frequency": 0,
        "avg_swolf": 68,
        "best_swolf": 58,
        "distance": 1500,
        "duration": 2751,
        "main_posture": 1,
        "max_stroke_freq": 34,
        "pool_width": 0,
        "proto_type": 9,
        "sport_type": 9,
        "stroke_count": 1377,
        "turn_count": 60,
    }
    if raw_report is not None:
        report = raw_report
    activity = Activity(
        activity_id="sid-1:key-1:1794302641",
        sid="sid-1",
        key="key-1",
        category="pool_swimming",
        sport_type=sport_type,
        title="Pool Swimming",
        start_time=1794302641,
        end_time=1794305392,
        duration_seconds=2751,
        distance_meters=1500,
        calories=880,
        steps=None,
        sync_state="server",
        next_key=None,
        raw_record={},
        raw_report=report,
    )
    sample = ActivitySample(
        timestamp=1794302641,
        start_time=None,
        end_time=None,
        duration_seconds=None,
        heart_rate=None,
        cadence=None,
        speed_mps=None,
        distance_meters=25.0,
        altitude_meters=None,
        steps=None,
        calories=None,
        raw_sample={},
    )
    return ActivityDetail(
        activity=activity,
        detail_sid="sid-1",
        detail_key="fds_sport_record",
        detail_time=1794302641,
        zone_name="UTC",
        zone_offset_seconds=0,
        track_points=[],
        samples=[sample],
        sport_report=None,
        recovery_rate=None,
        raw_fitness_item={},
        raw_detail={},
    )


def test_fit_export_pool_swim_includes_swim_summary():
    fit = _parse_fit(render_export(_pool_swim_detail(), "fit").payload)
    session = _fit_sessions(fit)[0]
    lap = _fit_laps(fit)[0]

    assert session.sport == Sport.SWIMMING.value
    assert session.sub_sport == SubSport.LAP_SWIMMING.value
    assert session.pool_length == pytest.approx(25.0)
    assert session.total_cycles == 1377
    assert session.num_lengths == 60
    assert session.num_active_lengths == 60
    assert session.avg_stroke_count == pytest.approx(22.95, abs=0.1)
    assert session.avg_stroke_distance == pytest.approx(1.09, abs=0.01)
    # avg_stroke_frequency is 0 (unknown), so the cadence is derived: 1377 strokes / 45.85 min
    assert session.avg_cadence == 30
    assert session.max_cadence == 34

    assert lap.total_cycles == 1377
    assert lap.num_lengths == 60
    assert lap.avg_cadence == 30
    assert lap.max_cadence == 34


def test_fit_export_pool_swim_prefers_reported_pool_width_and_frequency():
    detail = _pool_swim_detail({"pool_width": 50, "turn_count": 30, "stroke_count": 600, "avg_stroke_frequency": 28})
    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.pool_length == pytest.approx(50.0)
    assert session.avg_cadence == 28


def test_fit_export_pool_swim_skips_implausible_pool_length():
    detail = _pool_swim_detail({"turn_count": 3, "stroke_count": 1377})
    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.pool_length is None
    assert session.num_lengths == 3
    assert session.total_cycles == 1377


def test_fit_export_swim_without_swim_report_has_no_swim_fields():
    detail = _pool_swim_detail({})
    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.pool_length is None
    assert session.total_cycles is None
    assert session.num_lengths is None


def test_fit_export_non_swim_ignores_stroke_fields():
    detail = _pool_swim_detail({"stroke_count": 1377, "turn_count": 60}, sport_type=8)
    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.total_cycles is None
    assert session.num_lengths is None


# Per-length FDS samples from a real Mi Band 9 pool swim (150 m, 25 m pool):
# two sets (4 + 2 lengths) separated by a set marker without distance.
_REAL_SWIM_LENGTHS = [
    # (end_time, pace s/100m, swolf, strokes, stroke rate, calories)
    (1793700560, 240, 83, 23, 23, 16),
    (1793700594, 136, 50, 16, 28, 9),
    (1793700638, 176, 63, 19, 25, 11),
    (1793700677, 156, 56, 17, 26, 10),
    None,
    (1793700727, 200, 71, 21, 25, 13),
    (1793700764, 148, 53, 16, 25, 9),
]


def _real_swim_sample(index: int, values: tuple | None) -> ActivitySample:
    if values is None:
        distance = None
        calories = None
        raw_sample = {"source": "fds_sport_record", "pace": 177, "extras": {"1": 1793700677, "11": 1, "13": 252}}
    else:
        end_time, pace, swolf, strokes, stroke_rate, calories = values
        distance = 25.0
        raw_sample = {
            "source": "fds_sport_record",
            "pace": pace,
            "extras": {"1": end_time, "10": 1, "11": 1, "13": swolf, "16": strokes, "17": stroke_rate},
        }
    return ActivitySample(
        timestamp=1793700499 + index,
        start_time=None,
        end_time=None,
        duration_seconds=1,
        heart_rate=None,
        cadence=None,
        speed_mps=None,
        distance_meters=distance,
        altitude_meters=None,
        steps=None,
        calories=calories,
        raw_sample=raw_sample,
    )


def _real_pool_swim_detail() -> ActivityDetail:
    detail = _pool_swim_detail({"stroke_count": 112, "turn_count": 6, "pool_width": 0, "max_stroke_freq": 28, "avg_stroke_frequency": 0, "main_posture": 1})
    detail.activity.start_time = 1793700499
    detail.activity.end_time = 1793700765
    detail.activity.duration_seconds = 265
    detail.activity.distance_meters = 150
    detail.activity.calories = 70
    detail.samples = [_real_swim_sample(index, values) for index, values in enumerate(_REAL_SWIM_LENGTHS)]
    return detail


def _fit_lengths(fit: FitFile) -> list:
    from fit_tool.profile.messages.length_message import LengthMessage

    return [r.message for r in fit.records if isinstance(r.message, LengthMessage)]


def test_fit_export_pool_swim_writes_length_per_pool_length():
    fit = _parse_fit(render_export(_real_pool_swim_detail(), "fit").payload)
    lengths = _fit_lengths(fit)

    assert len(lengths) == 6
    assert [length.total_strokes for length in lengths] == [23, 16, 19, 17, 21, 16]
    assert [length.total_timer_time for length in lengths] == [60.0, 34.0, 44.0, 39.0, 50.0, 37.0]
    assert [length.avg_swimming_cadence for length in lengths] == [23, 28, 25, 26, 25, 25]
    # SWOLF = seconds + strokes, as reported by Mi Fitness
    assert [int(l.total_timer_time) + l.total_strokes for l in lengths] == [83, 50, 63, 56, 71, 53]


def test_fit_export_pool_swim_writes_lap_per_set():
    fit = _parse_fit(render_export(_real_pool_swim_detail(), "fit").payload)
    laps = _fit_laps(fit)
    session = _fit_sessions(fit)[0]

    assert len(laps) == 2
    assert [lap.total_distance for lap in laps] == [100.0, 50.0]
    assert [lap.num_lengths for lap in laps] == [4, 2]
    assert [lap.first_length_index for lap in laps] == [0, 4]
    assert [lap.total_cycles for lap in laps] == [75, 37]
    assert [lap.max_cadence for lap in laps] == [28, 25]
    assert session.num_laps == 2
    assert session.total_cycles == 112
    assert session.num_lengths == 6
    assert session.pool_length == pytest.approx(25.0)


def test_fit_export_pool_swim_records_have_cumulative_distance():
    from fit_tool.profile.messages.record_message import RecordMessage

    fit = _parse_fit(render_export(_real_pool_swim_detail(), "fit").payload)
    records = [r.message for r in fit.records if isinstance(r.message, RecordMessage)]

    assert [record.distance for record in records] == [0.0, 25.0, 50.0, 75.0, 100.0, 125.0, 150.0]


def test_fit_export_pool_swim_lap_cadence_is_consistent():
    laps = _fit_laps(_parse_fit(render_export(_real_pool_swim_detail(), "fit").payload))

    assert [lap.avg_cadence for lap in laps] == [25, 25]
    assert all(lap.avg_cadence <= lap.max_cadence for lap in laps)


def test_fit_export_pool_swim_writes_swim_stroke():
    from fit_tool.profile.profile_type import SwimStroke

    fit = _parse_fit(render_export(_real_pool_swim_detail(), "fit").payload)

    # Mi Fitness style 1 is breaststroke (confirmed on a real swim)
    assert {length.swim_stroke for length in _fit_lengths(fit)} == {SwimStroke.BREASTSTROKE.value}
    assert {lap.swim_stroke for lap in _fit_laps(fit)} == {SwimStroke.BREASTSTROKE.value}
    assert _fit_sessions(fit)[0].swim_stroke == SwimStroke.BREASTSTROKE.value


def test_fit_export_pool_swim_lap_with_mixed_strokes_is_mixed():
    from fit_tool.profile.profile_type import SwimStroke

    detail = _real_pool_swim_detail()
    detail.samples[1].raw_sample["extras"]["11"] = 2
    laps = _fit_laps(_parse_fit(render_export(detail, "fit").payload))

    assert [lap.swim_stroke for lap in laps] == [SwimStroke.MIXED.value, SwimStroke.BREASTSTROKE.value]


def test_fit_export_pool_swim_summary_only_uses_main_posture():
    from fit_tool.profile.profile_type import SwimStroke

    detail = _pool_swim_detail({"stroke_count": 1377, "turn_count": 60, "main_posture": 2})
    fit = _parse_fit(render_export(detail, "fit").payload)

    assert _fit_laps(fit)[0].swim_stroke == SwimStroke.FREESTYLE.value
    assert _fit_sessions(fit)[0].swim_stroke == SwimStroke.FREESTYLE.value
