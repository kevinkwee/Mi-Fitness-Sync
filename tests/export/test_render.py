from __future__ import annotations

import gzip
import itertools
from typing import Any
from xml.etree import ElementTree as ET

import pytest
from fit_tool.fit_file import FitFile
from fit_tool.profile.messages.activity_message import ActivityMessage
from fit_tool.profile.messages.lap_message import LapMessage
from fit_tool.profile.messages.record_message import RecordMessage
from fit_tool.profile.messages.session_message import SessionMessage
from fit_tool.profile.profile_type import (
    DisplayMeasure,
    Event,
    EventType,
    LengthType,
    Sport,
    SubSport,
    SwimStroke,
)

from mi_fitness_sync.activity.models import Activity, ActivityDetail, ActivitySample, TrackPoint
from mi_fitness_sync.exceptions import MiFitnessError
from mi_fitness_sync.export.render import (
    _cadence_spm_to_rpm,
    _clamp_cadence,
    _clamp_calories,
    _clamp_heart_rate,
    _fit_local_timestamp,
    render_export,
)
from mi_fitness_sync.export.smoothing import smooth_track, total_haversine_distance
from mi_fitness_sync.fds.sport_reports import SportReport


def _require_longitude(point: TrackPoint) -> float:
    """Return the longitude of *point*, failing the test when it is unset."""
    if point.longitude is None:
        msg = "expected track point longitude was not set"
        raise AssertionError(msg)
    return point.longitude


def _require_element(element: ET.Element | None) -> ET.Element:
    """Return *element*, failing the test when the lookup found nothing."""
    if element is None:
        msg = "expected XML element was not found"
        raise AssertionError(msg)
    return element


def _element_text(element: ET.Element) -> str:
    """Return the text of *element*, which must be set."""
    if element.text is None:
        msg = "expected XML element text was not set"
        raise AssertionError(msg)
    return element.text


GPX_NS = {"gpx": "http://www.topografix.com/GPX/1/1"}
TPX_NS = {"gpxtpx": "http://www.garmin.com/xmlschemas/TrackPointExtension/v1"}
ALL_NS = {**GPX_NS, **TPX_NS}


def test_render_export_gpx_contains_track_points(sample_activity_detail: ActivityDetail) -> None:
    export = render_export(sample_activity_detail, "gpx")

    root = ET.fromstring(export.payload)
    namespace = {"gpx": "http://www.topografix.com/GPX/1/1"}

    assert export.file_format == "gpx"
    assert len(root.findall(".//gpx:trkpt", namespace)) == 1


def test_render_export_tcx_contains_trackpoints(sample_activity_detail: ActivityDetail) -> None:
    export = render_export(sample_activity_detail, "tcx")

    root = ET.fromstring(export.payload)
    namespace = {"tcx": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"}

    assert export.file_format == "tcx"
    assert len(root.findall(".//tcx:Trackpoint", namespace)) == 1


def test_render_export_fit_returns_fit_bytes(sample_activity_detail: ActivityDetail) -> None:
    export = render_export(sample_activity_detail, "fit")

    assert export.file_format == "fit"
    assert b".FIT" in export.payload[:16]


def test_render_export_gzip_wraps_output(sample_activity_detail: ActivityDetail) -> None:
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


def test_tcx_export_uses_summary_calories_and_sample_hr() -> None:
    detail = _strength_detail_with_samples()
    export = render_export(detail, "tcx")
    root = ET.fromstring(export.payload)
    ns = {"tcx": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"}

    lap_calories = _require_element(root.find(".//tcx:Lap/tcx:Calories", ns))
    avg_hr = _require_element(root.find(".//tcx:Lap/tcx:AverageHeartRateBpm/tcx:Value", ns))
    max_hr = _require_element(root.find(".//tcx:Lap/tcx:MaximumHeartRateBpm/tcx:Value", ns))
    assert int(_element_text(lap_calories)) == 372
    assert avg_hr is not None
    assert int(_element_text(avg_hr)) == 118
    assert max_hr is not None
    assert int(_element_text(max_hr)) == 145


def test_fit_export_uses_summary_calories_and_sample_hr() -> None:
    detail = _strength_detail_with_samples()
    export = render_export(detail, "fit")

    assert export.file_format == "fit"
    assert b".FIT" in export.payload[:16]

    fit = FitFile.from_bytes(export.payload)
    laps = [record.message for record in fit.records if isinstance(record.message, LapMessage)]
    sessions = [
        record.message for record in fit.records if isinstance(record.message, SessionMessage)
    ]

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
    sport_type: int | None = 1,
    category: str = "outdoor_run",
    zone_offset_seconds: int | None = 28800,
    steps: int | None = 10000,
    sport_report: SportReport | None = None,
    raw_report: dict[str, Any] | None = None,
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
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=10.0,
                speed_mps=1.2,
                distance_meters=0.0,
                heart_rate=120,
                cadence=160,
                raw_point={},
            ),
            TrackPoint(
                timestamp=1717201800,
                latitude=1.31,
                longitude=103.81,
                altitude_meters=25.0,
                speed_mps=1.5,
                distance_meters=2500.0,
                heart_rate=140,
                cadence=170,
                raw_point={},
            ),
            TrackPoint(
                timestamp=1717203600,
                latitude=1.32,
                longitude=103.82,
                altitude_meters=15.0,
                speed_mps=1.3,
                distance_meters=5000.0,
                heart_rate=130,
                cadence=165,
                raw_point={},
            ),
        ],
        samples=[],
        sport_report=sport_report,
        recovery_rate=None,
        raw_fitness_item={},
        raw_detail={},
    )


class TestFitSportSubSportMapping:
    # -- sport_type primary mapping --

    def test_outdoor_run_maps_to_running_street(self) -> None:
        detail = _outdoor_run_detail(sport_type=1)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.RUNNING.value
        assert session.sub_sport == SubSport.STREET.value

    def test_outdoor_walk_maps_to_walking(self) -> None:
        detail = _outdoor_run_detail(sport_type=2)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.WALKING.value
        assert session.sub_sport == SubSport.CASUAL_WALKING.value

    def test_treadmill_maps_to_running_treadmill(self) -> None:
        detail = _outdoor_run_detail(sport_type=3)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.RUNNING.value
        assert session.sub_sport == SubSport.TREADMILL.value

    def test_trail_run_maps_to_running_trail(self) -> None:
        detail = _outdoor_run_detail(sport_type=5)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.RUNNING.value
        assert session.sub_sport == SubSport.TRAIL.value

    def test_indoor_cycling_maps_correctly(self) -> None:
        detail = _outdoor_run_detail(sport_type=7)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.CYCLING.value
        assert session.sub_sport == SubSport.INDOOR_CYCLING.value

    def test_free_training_maps_to_training_generic(self) -> None:
        detail = _outdoor_run_detail(sport_type=8)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.TRAINING.value
        assert session.sub_sport == SubSport.GENERIC.value

    def test_pool_swimming_maps_to_lap_swimming(self) -> None:
        detail = _outdoor_run_detail(sport_type=9)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.SWIMMING.value
        assert session.sub_sport == SubSport.LAP_SWIMMING.value

    def test_hiking_maps_correctly(self) -> None:
        detail = _outdoor_run_detail(sport_type=15)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.HIKING.value
        assert session.sub_sport == SubSport.GENERIC.value

    def test_hiit_maps_to_cardio_training(self) -> None:
        detail = _outdoor_run_detail(sport_type=16)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.TRAINING.value
        assert session.sub_sport == SubSport.CARDIO_TRAINING.value

    def test_jump_rope_maps_to_cardio_training(self) -> None:
        detail = _outdoor_run_detail(sport_type=14)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.TRAINING.value
        assert session.sub_sport == SubSport.CARDIO_TRAINING.value

    def test_strength_training_308(self) -> None:
        detail = _outdoor_run_detail(sport_type=308)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.TRAINING.value
        assert session.sub_sport == SubSport.STRENGTH_TRAINING.value

    def test_indoor_walking_333(self) -> None:
        detail = _outdoor_run_detail(sport_type=333)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.WALKING.value
        assert session.sub_sport == SubSport.INDOOR_WALKING.value

    def test_snowboarding_708(self) -> None:
        detail = _outdoor_run_detail(sport_type=708)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.SNOWBOARDING.value
        assert session.sub_sport == SubSport.GENERIC.value

    def test_soccer_600(self) -> None:
        detail = _outdoor_run_detail(sport_type=600)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.SOCCER.value
        assert session.sub_sport == SubSport.GENERIC.value

    # -- proto_type fallback --

    def test_proto_type_fallback_when_sport_type_none(self) -> None:
        detail = _outdoor_run_detail(sport_type=None, raw_report={"proto_type": 1})
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.RUNNING.value
        assert session.sub_sport == SubSport.STREET.value

    def test_proto_type_track_running_fallback(self) -> None:
        detail = _outdoor_run_detail(sport_type=None, raw_report={"proto_type": 2})
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.RUNNING.value
        assert session.sub_sport == SubSport.TRACK.value

    def test_proto_type_outdoor_step_maps_to_generic(self) -> None:
        detail = _outdoor_run_detail(sport_type=None, raw_report={"proto_type": 22})
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.GENERIC.value
        assert session.sub_sport == SubSport.GENERIC.value

    # -- priority & fallback --

    def test_sport_type_takes_priority_over_proto_type(self) -> None:
        detail = _outdoor_run_detail(sport_type=15, raw_report={"proto_type": 1})
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.HIKING.value
        assert session.sub_sport == SubSport.GENERIC.value

    def test_unknown_types_fall_to_generic(self) -> None:
        detail = _outdoor_run_detail(sport_type=999, raw_report={"proto_type": 999})
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.GENERIC.value
        assert session.sub_sport == SubSport.GENERIC.value

    def test_no_sport_type_or_proto_type_falls_to_generic(self) -> None:
        detail = _outdoor_run_detail(sport_type=None)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.GENERIC.value
        assert session.sub_sport == SubSport.GENERIC.value

    def test_unmapped_sport_type_22_falls_to_generic(self) -> None:
        detail = _outdoor_run_detail(sport_type=22)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.sport == Sport.GENERIC.value
        assert session.sub_sport == SubSport.GENERIC.value

    def test_sub_sport_set_on_lap(self) -> None:
        detail = _outdoor_run_detail(sport_type=1)
        fit = _parse_fit(render_export(detail, "fit").payload)
        lap = _fit_laps(fit)[0]
        assert lap.sport == Sport.RUNNING.value
        assert lap.sub_sport == SubSport.STREET.value


class TestFitSessionAggregates:
    def test_num_laps_is_set(self) -> None:
        detail = _outdoor_run_detail()
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.num_laps == 1

    def test_avg_speed_computed_from_distance_and_time(self) -> None:
        detail = _outdoor_run_detail()
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        expected = 5000.0 / 3600.0
        assert abs(session.avg_speed - expected) < 0.01

    def test_avg_speed_prefers_sport_report(self) -> None:
        report = SportReport(avg_speed=9.0)  # 9.0 km/h from Mi Fitness
        detail = _outdoor_run_detail(sport_report=report)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert abs(session.avg_speed - 2.5) < 0.01  # 9.0 / 3.6 = 2.5 m/s

    def test_max_speed_computed_from_points(self) -> None:
        detail = _outdoor_run_detail()
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert abs(session.max_speed - 1.5) < 0.01

    def test_max_speed_prefers_sport_report(self) -> None:
        report = SportReport(max_speed=10.8)  # 10.8 km/h from Mi Fitness
        detail = _outdoor_run_detail(sport_report=report)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert abs(session.max_speed - 3.0) < 0.01  # 10.8 / 3.6 = 3.0 m/s

    def test_avg_cadence_computed_from_points(self) -> None:
        detail = _outdoor_run_detail()
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.avg_cadence == 82  # (160+170+165)/3=165 SPM -> 165//2=82 RPM

    def test_avg_cadence_prefers_sport_report(self) -> None:
        report = SportReport(avg_cadence=150)
        detail = _outdoor_run_detail(sport_report=report)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.avg_cadence == 75  # 150 SPM -> 150//2=75 RPM

    def test_total_ascent_from_sport_report(self) -> None:
        report = SportReport(rise_height=120.7)
        detail = _outdoor_run_detail(sport_report=report)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.total_ascent == 121

    def test_total_descent_from_sport_report(self) -> None:
        report = SportReport(fall_height=80.3)
        detail = _outdoor_run_detail(sport_report=report)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.total_descent == 80

    def test_total_ascent_computed_from_altitude(self) -> None:
        detail = _outdoor_run_detail()
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        # 10 -> 25 (+15), 25 -> 15 (-10)
        assert session.total_ascent == 15

    def test_total_descent_computed_from_altitude(self) -> None:
        detail = _outdoor_run_detail()
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        # 10 -> 25 (no descent), 25 -> 15 (-10)
        assert session.total_descent == 10

    def test_lap_total_ascent_set(self) -> None:
        report = SportReport(rise_height=50.0)
        detail = _outdoor_run_detail(sport_report=report)
        fit = _parse_fit(render_export(detail, "fit").payload)
        lap = _fit_laps(fit)[0]
        assert lap.total_ascent == 50


class TestFitLocalTimestamp:
    def test_local_timestamp_set_with_zone_offset(self) -> None:
        detail = _outdoor_run_detail(zone_offset_seconds=28800)
        fit = _parse_fit(render_export(detail, "fit").payload)
        activities = _fit_activities(fit)
        assert len(activities) == 1
        # local_timestamp = end_time + offset - FIT_EPOCH_OFFSET  # noqa: ERA001 - states the formula
        expected = 1717203600 + 28800 - 631065600
        assert activities[0].local_timestamp == expected

    def test_local_timestamp_omitted_when_offset_is_none(self) -> None:
        detail = _outdoor_run_detail(zone_offset_seconds=None)
        fit = _parse_fit(render_export(detail, "fit").payload)
        activities = _fit_activities(fit)
        assert len(activities) == 1
        # local_timestamp should not be set (remains None/default)
        assert activities[0].local_timestamp is None


class TestFitStridesAndStepLength:
    def test_total_strides_from_steps(self) -> None:
        detail = _outdoor_run_detail(steps=10000)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.total_strides == 5000

    def test_avg_step_length_computed(self) -> None:
        detail = _outdoor_run_detail(steps=10000)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        # 5000m / 5000 strides = 1.0m
        assert abs(session.avg_step_length - 1.0) < 0.01

    def test_no_strides_when_no_steps(self) -> None:
        detail = _outdoor_run_detail(steps=None)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.total_strides is None


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
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=10.0,
                speed_mps=1.2,
                distance_meters=0.0,
                heart_rate=120,
                cadence=80,
                raw_point={},
            ),
            TrackPoint(
                timestamp=1717201800,
                latitude=1.31,
                longitude=103.81,
                altitude_meters=25.0,
                speed_mps=1.5,
                distance_meters=2500.0,
                heart_rate=140,
                cadence=85,
                raw_point={},
            ),
            TrackPoint(
                timestamp=1717203600,
                latitude=1.32,
                longitude=103.82,
                altitude_meters=15.0,
                speed_mps=1.3,
                distance_meters=5000.0,
                heart_rate=130,
                cadence=82,
                raw_point={},
            ),
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
    def test_garmin_xsd_url_is_correct(self) -> None:
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        schema_loc = root.attrib["{http://www.w3.org/2001/XMLSchema-instance}schemaLocation"]
        assert "http://www.garmin.com/xmlschemas/TrackPointExtensionv1.xsd" in schema_loc
        # Must NOT contain the old broken path
        assert "TrackPointExtension/v1/TrackPointExtensionv1.xsd" not in schema_loc

    def test_gpx_schema_location_present(self) -> None:
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        schema_loc = root.attrib["{http://www.w3.org/2001/XMLSchema-instance}schemaLocation"]
        assert "http://www.topografix.com/GPX/1/1/gpx.xsd" in schema_loc


class TestGpxCreator:
    def test_creator_contains_barometer_hint_when_altitude_present(self) -> None:
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        assert root.attrib["creator"] == "Strava with barometer"

    def test_creator_omits_barometer_hint_when_no_altitude(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=1.2,
                distance_meters=0.0,
                heart_rate=120,
                cadence=80,
                raw_point={},
            ),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        assert root.attrib["creator"] == "Strava"

    def test_creator_starts_with_app_name(self) -> None:
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        assert root.attrib["creator"].startswith("Strava")


class TestGpxHeartRateAndCadenceBounds:
    """Cover the Garmin TrackPointExtension v1 schema bounds.

    BeatsPerMinute_t is an xsd:unsignedByte with minInclusive=1, so [1, 255], and
    RevolutionsPerMinute_t is an xsd:unsignedByte with maxInclusive=254, so [0, 254].
    """

    # -- _clamp_heart_rate unit tests --

    def test_hr_normal_value(self) -> None:
        assert _clamp_heart_rate(120) == 120

    def test_hr_min_valid(self) -> None:
        assert _clamp_heart_rate(1) == 1

    def test_hr_max_valid(self) -> None:
        assert _clamp_heart_rate(255) == 255

    def test_hr_zero_returns_none(self) -> None:
        assert _clamp_heart_rate(0) is None

    def test_hr_negative_returns_none(self) -> None:
        assert _clamp_heart_rate(-5) is None

    def test_hr_over_255_clamped(self) -> None:
        assert _clamp_heart_rate(300) == 255

    # -- _clamp_cadence unit tests --

    def test_cad_normal_value(self) -> None:
        assert _clamp_cadence(80) == 80

    def test_cad_zero_valid(self) -> None:
        assert _clamp_cadence(0) == 0

    def test_cad_max_valid(self) -> None:
        assert _clamp_cadence(254) == 254

    def test_cad_over_254_clamped(self) -> None:
        assert _clamp_cadence(260) == 254

    def test_cad_negative_clamped_to_zero(self) -> None:
        assert _clamp_cadence(-5) == 0

    # -- GPX integration tests --

    def test_hr_clamped_in_gpx_output(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=300,
                cadence=None,
                raw_point={},
            ),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        hr = _require_element(root.find(".//gpxtpx:TrackPointExtension/gpxtpx:hr", TPX_NS))
        assert int(_element_text(hr)) == 255

    def test_cadence_clamped_in_gpx_output(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=None,
                cadence=520,
                raw_point={},
            ),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        cad = _require_element(root.find(".//gpxtpx:TrackPointExtension/gpxtpx:cad", TPX_NS))
        # 520 SPM (running) -> 260 RPM -> clamped to 254
        assert int(_element_text(cad)) == 254

    def test_zero_hr_omitted_from_gpx(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=0,
                cadence=80,
                raw_point={},
            ),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        hr = root.find(".//gpxtpx:TrackPointExtension/gpxtpx:hr", TPX_NS)
        assert hr is None
        # cadence should still be present
        assert root.find(".//gpxtpx:TrackPointExtension/gpxtpx:cad", TPX_NS) is not None

    def test_negative_hr_omitted_from_gpx(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=-10,
                cadence=80,
                raw_point={},
            ),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        hr = root.find(".//gpxtpx:TrackPointExtension/gpxtpx:hr", TPX_NS)
        assert hr is None

    def test_zero_cadence_emitted_in_gpx(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=120,
                cadence=0,
                raw_point={},
            ),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        cad = _require_element(root.find(".//gpxtpx:TrackPointExtension/gpxtpx:cad", TPX_NS))
        assert int(_element_text(cad)) == 0


class TestGpxCoordinateBoundsCheck:
    def test_invalid_points_excluded_from_gpx(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=10.0,
                speed_mps=None,
                distance_meters=None,
                heart_rate=120,
                cadence=None,
                raw_point={},
            ),
            TrackPoint(
                timestamp=1717200060,
                latitude=91.0,
                longitude=103.8,
                altitude_meters=10.0,
                speed_mps=None,
                distance_meters=None,
                heart_rate=130,
                cadence=None,
                raw_point={},
            ),
            TrackPoint(
                timestamp=1717200120,
                latitude=1.31,
                longitude=200.0,
                altitude_meters=10.0,
                speed_mps=None,
                distance_meters=None,
                heart_rate=140,
                cadence=None,
                raw_point={},
            ),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        trkpts = root.findall(".//gpx:trkpt", GPX_NS)
        assert len(trkpts) == 1
        assert trkpts[0].attrib["lat"] == "1.30000000"

    def test_all_invalid_coordinates_raises(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=91.0,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=None,
                cadence=None,
                raw_point={},
            ),
        ]
        detail = _gpx_detail(track_points=points)
        with pytest.raises(MiFitnessError, match="GPS track points"):
            render_export(detail, "gpx")


class TestGpxElementOrdering:
    """Verify all elements are in strict GPX 1.1 XSD sequence order."""

    def _child_local_names(self, element: ET.Element) -> list[str]:
        """Return local names (without namespace) for all child elements."""
        return [child.tag.split("}")[-1] if "}" in child.tag else child.tag for child in element]

    def test_gpx_root_children_order(self) -> None:
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        children = self._child_local_names(root)
        # gpxType: metadata, wpt*, rte*, trk*, extensions
        assert children == ["metadata", "trk"]

    def test_metadata_children_order(self) -> None:
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        metadata = _require_element(root.find("{http://www.topografix.com/GPX/1/1}metadata"))
        children = self._child_local_names(metadata)
        # metadataType: name, desc, author, copyright, link*, time, keywords, bounds, extensions
        assert children == ["name", "time"]

    def test_trk_children_order(self) -> None:
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        trk = _require_element(root.find("{http://www.topografix.com/GPX/1/1}trk"))
        children = self._child_local_names(trk)
        # trkType: name, cmt, desc, src, link*, number, type, extensions, trkseg*
        assert children == ["name", "type", "trkseg"]

    def test_trkpt_children_order(self) -> None:
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        trkpt = _require_element(root.find(".//gpx:trkpt", GPX_NS))
        children = self._child_local_names(trkpt)
        # wptType: ele, time, ..., extensions
        assert children == ["ele", "time", "extensions"]

    def test_trkpt_without_altitude_order(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=100.0,
                heart_rate=120,
                cadence=80,
                raw_point={},
            ),
        ]
        detail = _gpx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        trkpt = _require_element(root.find(".//gpx:trkpt", GPX_NS))
        children = self._child_local_names(trkpt)
        # ele omitted, so time then extensions
        assert children == ["time", "extensions"]

    def test_extension_children_order(self) -> None:
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        extensions = _require_element(root.find(".//gpx:trkpt/gpx:extensions", GPX_NS))
        children = self._child_local_names(extensions)
        assert children == ["TrackPointExtension"]

    def test_trackpoint_extension_children_order(self) -> None:
        """The hr element precedes cad per the Garmin TPE v1 XSD."""
        detail = _gpx_detail()
        root = ET.fromstring(render_export(detail, "gpx").payload)
        tpe = _require_element(root.find(".//gpxtpx:TrackPointExtension", TPX_NS))
        children = self._child_local_names(tpe)
        assert children == ["hr", "cad"]


TCX_NS_MAP = {"tcx": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"}
AEXT_NS_MAP = {"aext": "http://www.garmin.com/xmlschemas/ActivityExtension/v2"}
ALL_TCX_NS = {**TCX_NS_MAP, **AEXT_NS_MAP}


def _tcx_detail(
    *,
    sport_type: int | None = 1,
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
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=10.0,
                speed_mps=1.2,
                distance_meters=0.0,
                heart_rate=120,
                cadence=80,
                raw_point={},
            ),
            TrackPoint(
                timestamp=1717201800,
                latitude=1.31,
                longitude=103.81,
                altitude_meters=25.0,
                speed_mps=1.5,
                distance_meters=2500.0,
                heart_rate=140,
                cadence=85,
                raw_point={},
            ),
            TrackPoint(
                timestamp=1717203600,
                latitude=1.32,
                longitude=103.82,
                altitude_meters=15.0,
                speed_mps=1.3,
                distance_meters=5000.0,
                heart_rate=130,
                cadence=82,
                raw_point={},
            ),
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
    """Verify Lap children follow the ActivityLap_t XSD sequence.

    The sequence is TotalTimeSeconds, DistanceMeters, MaximumSpeed, Calories,
    AverageHeartRateBpm, MaximumHeartRateBpm, Intensity, Cadence,
    TriggerMethod, Track, Notes, Extensions.
    """

    def test_lap_children_order_with_hr(self) -> None:
        detail = _tcx_detail()
        root = ET.fromstring(render_export(detail, "tcx").payload)
        lap = _require_element(root.find(".//tcx:Lap", TCX_NS_MAP))
        children = _tcx_child_local_names(lap)
        assert children == [
            "TotalTimeSeconds",
            "DistanceMeters",
            "Calories",
            "AverageHeartRateBpm",
            "MaximumHeartRateBpm",
            "Intensity",
            "TriggerMethod",
            "Track",
        ]

    def test_lap_children_order_without_hr(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=10.0,
                speed_mps=1.2,
                distance_meters=0.0,
                heart_rate=None,
                cadence=None,
                raw_point={},
            ),
        ]
        detail = _tcx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        lap = _require_element(root.find(".//tcx:Lap", TCX_NS_MAP))
        children = _tcx_child_local_names(lap)
        assert children == [
            "TotalTimeSeconds",
            "DistanceMeters",
            "Calories",
            "Intensity",
            "TriggerMethod",
            "Track",
        ]


class TestTcxTrackpointElementOrdering:
    """Verify Trackpoint children follow the Trackpoint_t XSD sequence.

    The sequence is Time, Position, AltitudeMeters, DistanceMeters,
    HeartRateBpm, Cadence, SensorState, Extensions.
    """

    def test_full_trackpoint_order_biking(self) -> None:
        detail = _tcx_detail(sport_type=6)  # Biking
        root = ET.fromstring(render_export(detail, "tcx").payload)
        tp = _require_element(root.find(".//tcx:Trackpoint", TCX_NS_MAP))
        children = _tcx_child_local_names(tp)
        # Biking: base Cadence, Extensions has Speed only
        assert children == [
            "Time",
            "Position",
            "AltitudeMeters",
            "DistanceMeters",
            "HeartRateBpm",
            "Cadence",
            "Extensions",
        ]

    def test_full_trackpoint_order_running(self) -> None:
        detail = _tcx_detail(sport_type=1)  # Running
        root = ET.fromstring(render_export(detail, "tcx").payload)
        tp = _require_element(root.find(".//tcx:Trackpoint", TCX_NS_MAP))
        children = _tcx_child_local_names(tp)
        # Running: no base Cadence, Extensions has Speed + RunCadence
        assert children == [
            "Time",
            "Position",
            "AltitudeMeters",
            "DistanceMeters",
            "HeartRateBpm",
            "Extensions",
        ]


class TestTcxHeartRateClamping:
    def test_trackpoint_hr_clamped_to_255(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=300,
                cadence=None,
                raw_point={},
            ),
        ]
        detail = _tcx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        val = _require_element(
            root.find(".//tcx:Trackpoint/tcx:HeartRateBpm/tcx:Value", TCX_NS_MAP)
        )
        assert val is not None
        assert int(_element_text(val)) == 255

    def test_trackpoint_hr_zero_omitted(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=0,
                cadence=None,
                raw_point={},
            ),
        ]
        detail = _tcx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        val = root.find(".//tcx:Trackpoint/tcx:HeartRateBpm", TCX_NS_MAP)
        assert val is None

    def test_lap_avg_hr_clamped(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=None,
                longitude=None,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=300,
                cadence=None,
                raw_point={},
            ),
        ]
        detail = _tcx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        val = _require_element(
            root.find(".//tcx:Lap/tcx:AverageHeartRateBpm/tcx:Value", TCX_NS_MAP)
        )
        assert val is not None
        assert int(_element_text(val)) == 255

    def test_lap_max_hr_clamped(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=None,
                longitude=None,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=300,
                cadence=None,
                raw_point={},
            ),
        ]
        detail = _tcx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        val = _require_element(
            root.find(".//tcx:Lap/tcx:MaximumHeartRateBpm/tcx:Value", TCX_NS_MAP)
        )
        assert val is not None
        assert int(_element_text(val)) == 255


class TestTcxCadenceClamping:
    def test_bike_cadence_clamped_to_254(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=None,
                cadence=260,
                raw_point={},
            ),
        ]
        detail = _tcx_detail(sport_type=6, track_points=points)  # Biking
        root = ET.fromstring(render_export(detail, "tcx").payload)
        cad = _require_element(root.find(".//tcx:Trackpoint/tcx:Cadence", TCX_NS_MAP))
        assert int(_element_text(cad)) == 254

    def test_run_cadence_clamped_in_extension(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=None,
                cadence=520,
                raw_point={},
            ),
        ]
        detail = _tcx_detail(sport_type=1, track_points=points)  # Running
        root = ET.fromstring(render_export(detail, "tcx").payload)
        # Base Cadence should NOT be present for running
        base_cad = root.find(".//tcx:Trackpoint/tcx:Cadence", TCX_NS_MAP)
        assert base_cad is None
        # RunCadence in Extensions: 520 SPM -> 260 RPM -> clamped to 254
        run_cad = root.find(".//tcx:Trackpoint/tcx:Extensions/aext:TPX/aext:RunCadence", ALL_TCX_NS)
        assert run_cad is not None
        assert int(_element_text(run_cad)) == 254


class TestTcxCoordinateValidation:
    def test_invalid_coords_excluded_from_tcx(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=None,
                cadence=None,
                raw_point={},
            ),
            TrackPoint(
                timestamp=1717200060,
                latitude=91.0,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=None,
                cadence=None,
                raw_point={},
            ),
            TrackPoint(
                timestamp=1717200120,
                latitude=1.31,
                longitude=200.0,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=None,
                cadence=None,
                raw_point={},
            ),
        ]
        detail = _tcx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        positions = root.findall(".//tcx:Trackpoint/tcx:Position", TCX_NS_MAP)
        assert len(positions) == 1
        lat = _require_element(positions[0].find("tcx:LatitudeDegrees", TCX_NS_MAP))
        assert lat.text == "1.30000000"


class TestTcxCaloriesBounding:
    def test_clamp_calories_normal(self) -> None:
        assert _clamp_calories(400) == 400

    def test_clamp_calories_zero(self) -> None:
        assert _clamp_calories(0) == 0

    def test_clamp_calories_max_valid(self) -> None:
        assert _clamp_calories(65535) == 65535

    def test_clamp_calories_over_max(self) -> None:
        assert _clamp_calories(70000) == 65535

    def test_clamp_calories_negative(self) -> None:
        assert _clamp_calories(-5) == 0

    def test_calories_clamped_in_tcx_output(self) -> None:
        detail = _tcx_detail(calories=70000)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        cal = _require_element(root.find(".//tcx:Lap/tcx:Calories", TCX_NS_MAP))
        assert int(_element_text(cal)) == 65535


class TestTcxSportAttribute:
    def test_running_sport(self) -> None:
        detail = _tcx_detail(sport_type=1)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        activity = _require_element(root.find(".//tcx:Activity", TCX_NS_MAP))
        assert activity.attrib["Sport"] == "Running"

    def test_biking_sport(self) -> None:
        detail = _tcx_detail(sport_type=6)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        activity = _require_element(root.find(".//tcx:Activity", TCX_NS_MAP))
        assert activity.attrib["Sport"] == "Biking"

    def test_hiking_maps_to_other(self) -> None:
        detail = _tcx_detail(sport_type=15)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        activity = _require_element(root.find(".//tcx:Activity", TCX_NS_MAP))
        assert activity.attrib["Sport"] == "Other"

    def test_swimming_maps_to_other(self) -> None:
        detail = _tcx_detail(sport_type=9)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        activity = _require_element(root.find(".//tcx:Activity", TCX_NS_MAP))
        assert activity.attrib["Sport"] == "Other"


class TestTcxCreatorElement:
    def test_creator_present(self) -> None:
        detail = _tcx_detail()
        root = ET.fromstring(render_export(detail, "tcx").payload)
        assert root.find(".//tcx:Activity/tcx:Creator", TCX_NS_MAP) is not None

    def test_creator_barometer_hint_with_altitude(self) -> None:
        detail = _tcx_detail()  # default points have altitude
        root = ET.fromstring(render_export(detail, "tcx").payload)
        name = _require_element(root.find(".//tcx:Activity/tcx:Creator/tcx:Name", TCX_NS_MAP))
        assert name.text == "Strava with barometer"

    def test_creator_no_barometer_without_altitude(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=120,
                cadence=None,
                raw_point={},
            ),
        ]
        detail = _tcx_detail(track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        name = _require_element(root.find(".//tcx:Activity/tcx:Creator/tcx:Name", TCX_NS_MAP))
        assert name.text == "Strava"

    def test_creator_has_device_type(self) -> None:
        detail = _tcx_detail()
        root = ET.fromstring(render_export(detail, "tcx").payload)
        creator = _require_element(root.find(".//tcx:Activity/tcx:Creator", TCX_NS_MAP))
        assert creator.attrib.get("{http://www.w3.org/2001/XMLSchema-instance}type") == "Device_t"

    def test_creator_has_required_children(self) -> None:
        detail = _tcx_detail()
        root = ET.fromstring(render_export(detail, "tcx").payload)
        creator = _require_element(root.find(".//tcx:Activity/tcx:Creator", TCX_NS_MAP))
        children = _tcx_child_local_names(creator)
        assert children == ["Name", "UnitId", "ProductID", "Version"]

    def test_product_id_equals_102(self) -> None:
        detail = _tcx_detail()
        root = ET.fromstring(render_export(detail, "tcx").payload)
        product_id = _require_element(
            root.find(".//tcx:Activity/tcx:Creator/tcx:ProductID", TCX_NS_MAP)
        )
        assert product_id is not None
        assert product_id.text == "102"

    def test_creator_after_lap(self) -> None:
        """Creator must appear after Lap (and optional Notes/Training) in Activity_t sequence."""
        detail = _tcx_detail()
        root = ET.fromstring(render_export(detail, "tcx").payload)
        activity = _require_element(root.find(".//tcx:Activity", TCX_NS_MAP))
        children = _tcx_child_local_names(activity)
        assert children == ["Id", "Lap", "Creator"]


class TestTcxRunningCadence:
    def test_running_cadence_in_extensions(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=None,
                cadence=160,
                raw_point={},
            ),
        ]
        detail = _tcx_detail(sport_type=1, track_points=points)  # Running
        root = ET.fromstring(render_export(detail, "tcx").payload)
        run_cad = _require_element(
            root.find(".//tcx:Trackpoint/tcx:Extensions/aext:TPX/aext:RunCadence", ALL_TCX_NS)
        )
        assert run_cad is not None
        assert int(_element_text(run_cad)) == 80  # 160 SPM -> 160//2=80 RPM

    def test_running_no_base_cadence(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=None,
                cadence=160,
                raw_point={},
            ),
        ]
        detail = _tcx_detail(sport_type=1, track_points=points)  # Running
        root = ET.fromstring(render_export(detail, "tcx").payload)
        base_cad = root.find(".//tcx:Trackpoint/tcx:Cadence", TCX_NS_MAP)
        assert base_cad is None

    def test_biking_uses_base_cadence(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=None,
                cadence=80,
                raw_point={},
            ),
        ]
        detail = _tcx_detail(sport_type=6, track_points=points)  # Biking
        root = ET.fromstring(render_export(detail, "tcx").payload)
        base_cad = _require_element(root.find(".//tcx:Trackpoint/tcx:Cadence", TCX_NS_MAP))
        assert int(_element_text(base_cad)) == 80
        # No RunCadence for biking
        run_cad = root.find(".//tcx:Trackpoint/tcx:Extensions/aext:TPX/aext:RunCadence", ALL_TCX_NS)
        assert run_cad is None

    def test_running_speed_and_cadence_in_same_tpx(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=2.5,
                distance_meters=None,
                heart_rate=None,
                cadence=160,
                raw_point={},
            ),
        ]
        detail = _tcx_detail(sport_type=1, track_points=points)  # Running
        root = ET.fromstring(render_export(detail, "tcx").payload)
        tpx = _require_element(root.find(".//tcx:Trackpoint/tcx:Extensions/aext:TPX", ALL_TCX_NS))
        tpx_children = _tcx_child_local_names(tpx)
        assert tpx_children == ["Speed", "RunCadence"]


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
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=10.0,
                speed_mps=5.0,
                distance_meters=0.0,
                heart_rate=120,
                cadence=cadence,
                raw_point={},
            ),
            TrackPoint(
                timestamp=1717203600,
                latitude=1.32,
                longitude=103.82,
                altitude_meters=15.0,
                speed_mps=6.0,
                distance_meters=20000.0,
                heart_rate=140,
                cadence=cadence + 10,
                raw_point={},
            ),
        ],
        samples=[],
        sport_report=None,
        recovery_rate=None,
        raw_fitness_item={},
        raw_detail={},
    )


class TestCadenceSpmToRpm:
    def test_running_cadence_halved(self) -> None:
        assert _cadence_spm_to_rpm(160, 1) == 80

    def test_walking_cadence_halved(self) -> None:
        assert _cadence_spm_to_rpm(120, 2) == 60

    def test_treadmill_cadence_halved(self) -> None:
        assert _cadence_spm_to_rpm(170, 3) == 85

    def test_trail_run_cadence_halved(self) -> None:
        assert _cadence_spm_to_rpm(150, 5) == 75

    def test_cycling_cadence_unchanged(self) -> None:
        assert _cadence_spm_to_rpm(80, 6) == 80

    def test_indoor_cycling_cadence_unchanged(self) -> None:
        assert _cadence_spm_to_rpm(90, 7) == 90

    def test_bmx_cadence_unchanged(self) -> None:
        assert _cadence_spm_to_rpm(70, 206) == 70

    def test_spinning_cadence_unchanged(self) -> None:
        assert _cadence_spm_to_rpm(95, 324) == 95

    def test_odd_spm_rounds_down(self) -> None:
        assert _cadence_spm_to_rpm(165, 1) == 82

    def test_zero_cadence(self) -> None:
        assert _cadence_spm_to_rpm(0, 1) == 0

    def test_none_sport_type_treated_as_step_based(self) -> None:
        assert _cadence_spm_to_rpm(160, None) == 80


class TestFitCadenceConversion:
    def test_running_record_cadence_is_rpm(self) -> None:
        detail = _outdoor_run_detail()
        fit = _parse_fit(render_export(detail, "fit").payload)
        records = [r.message for r in fit.records if isinstance(r.message, RecordMessage)]
        # First point: 160 SPM -> 80 RPM
        assert records[0].cadence == 80
        # Second point: 170 SPM -> 85 RPM
        assert records[1].cadence == 85
        # Third point: 165 SPM -> 82 RPM
        assert records[2].cadence == 82

    def test_cycling_record_cadence_unchanged(self) -> None:
        detail = _cycling_detail(cadence=80)
        fit = _parse_fit(render_export(detail, "fit").payload)
        records = [r.message for r in fit.records if isinstance(r.message, RecordMessage)]
        assert records[0].cadence == 80
        assert records[1].cadence == 90  # cadence + 10

    def test_cycling_avg_cadence_unchanged(self) -> None:
        detail = _cycling_detail(cadence=80)
        fit = _parse_fit(render_export(detail, "fit").payload)
        session = _fit_sessions(fit)[0]
        assert session.avg_cadence == 85  # (80 + 90) / 2, no halving


class TestGpxCadenceConversion:
    def test_running_gpx_cadence_is_rpm(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=None,
                cadence=160,
                raw_point={},
            ),
        ]
        detail = _gpx_detail(track_points=points)  # sport_type=1 (Running)
        root = ET.fromstring(render_export(detail, "gpx").payload)
        cad = _require_element(root.find(".//gpxtpx:TrackPointExtension/gpxtpx:cad", TPX_NS))
        assert int(_element_text(cad)) == 80  # 160 SPM -> 80 RPM

    def test_cycling_gpx_cadence_unchanged(self) -> None:
        activity = Activity(
            activity_id="sid:key:1",
            sid="sid",
            key="key",
            category="outdoor_cycling",
            sport_type=6,
            title="Ride",
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
        detail = ActivityDetail(
            activity=activity,
            detail_sid="sid",
            detail_key="key",
            detail_time=1717200000,
            zone_name="UTC",
            zone_offset_seconds=0,
            track_points=[
                TrackPoint(
                    timestamp=1717200000,
                    latitude=1.3,
                    longitude=103.8,
                    altitude_meters=None,
                    speed_mps=None,
                    distance_meters=None,
                    heart_rate=None,
                    cadence=80,
                    raw_point={},
                ),
            ],
            samples=[],
            sport_report=None,
            recovery_rate=None,
            raw_fitness_item={},
            raw_detail={},
        )
        root = ET.fromstring(render_export(detail, "gpx").payload)
        cad = _require_element(root.find(".//gpxtpx:TrackPointExtension/gpxtpx:cad", TPX_NS))
        assert int(_element_text(cad)) == 80  # Cycling: 80 RPM stays 80


class TestTcxCadenceConversion:
    def test_running_run_cadence_halved(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=None,
                cadence=170,
                raw_point={},
            ),
        ]
        detail = _tcx_detail(sport_type=1, track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        run_cad = _require_element(
            root.find(".//tcx:Trackpoint/tcx:Extensions/aext:TPX/aext:RunCadence", ALL_TCX_NS)
        )
        assert run_cad is not None
        assert int(_element_text(run_cad)) == 85  # 170 SPM -> 85 RPM

    def test_cycling_base_cadence_unchanged(self) -> None:
        points = [
            TrackPoint(
                timestamp=1717200000,
                latitude=1.3,
                longitude=103.8,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=None,
                cadence=90,
                raw_point={},
            ),
        ]
        detail = _tcx_detail(sport_type=6, track_points=points)
        root = ET.fromstring(render_export(detail, "tcx").payload)
        cad = _require_element(root.find(".//tcx:Trackpoint/tcx:Cadence", TCX_NS_MAP))
        assert int(_element_text(cad)) == 90  # Cycling: 90 RPM stays 90


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
        distance_meters=int(distance_meters),
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

    def _make_detail(self) -> tuple[ActivityDetail, list[TrackPoint]]:
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
                latitude=1.0 + i * 0.001,
                longitude=103.0,
                altitude_meters=None,
                speed_mps=None,
                distance_meters=None,
                heart_rate=None,
                cadence=None,
                raw_point={},
            )
            for i in range(40)
        ]
        clean_distance = total_haversine_distance(clean_points)
        assert noisy_distance > clean_distance * 1.01  # sanity
        return _noisy_gps_detail(clean_distance), raw_points

    def test_gpx_uses_smoothed_coords(self) -> None:
        detail, original_points = self._make_detail()
        export = render_export(detail, "gpx")
        root = ET.fromstring(export.payload)
        ns = {"gpx": "http://www.topografix.com/GPX/1/1"}
        trkpts = root.findall(".//gpx:trkpt", ns)
        assert len(trkpts) == 40
        # At least one interior point's longitude should differ from the original zigzag
        changed = any(
            float(tp.attrib["lon"]) != _require_longitude(original_points[i])
            for i, tp in enumerate(trkpts)
        )
        assert changed, "GPX output should contain smoothed (different) coordinates"

    def test_tcx_uses_smoothed_coords(self) -> None:
        detail, original_points = self._make_detail()
        export = render_export(detail, "tcx")
        root = ET.fromstring(export.payload)
        ns = {"tcx": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"}
        positions = root.findall(".//tcx:Trackpoint/tcx:Position", ns)
        assert len(positions) == 40
        changed = any(
            float(_element_text(_require_element(pos.find("tcx:LongitudeDegrees", ns))))
            != _require_longitude(original_points[i])
            for i, pos in enumerate(positions)
        )
        assert changed, "TCX output should contain smoothed (different) coordinates"

    def test_fit_uses_smoothed_coords(self) -> None:
        detail, original_points = self._make_detail()
        # Independently compute expected smoothed coordinates
        expected_smoothed = smooth_track(
            detail.track_points, float(detail.activity.distance_meters or 0)
        )
        assert expected_smoothed is not detail.track_points, (
            "smooth_track should have modified coords"
        )

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
            original_lon = _require_longitude(original_points[i])
            smoothed_lon = _require_longitude(expected_smoothed[i])
            if abs(original_lon - smoothed_lon) < 1e-12:
                continue  # skip points where smoothing didn't change the value
            total_compared += 1
            if abs(fit_lon - smoothed_lon) < abs(fit_lon - original_lon):
                closer_to_smoothed += 1

        assert total_compared > 0, (
            "At least some points should differ between original and smoothed"
        )
        assert closer_to_smoothed == total_compared, (
            f"All compared FIT coords should be closer to smoothed than original, "
            f"but only {closer_to_smoothed}/{total_compared} were"
        )


class TestRenderExportSmoothingOptions:
    def test_no_smooth_skips_smoothing(self, sample_activity_detail: ActivityDetail) -> None:
        """When smooth=False, track_points should pass through unchanged."""
        export = render_export(sample_activity_detail, "gpx", smooth=False)
        assert export.file_format == "gpx"
        # Should still produce valid output
        root = ET.fromstring(export.payload)
        assert len(root.findall(".//gpx:trkpt", GPX_NS)) == 1

    def test_smooth_mode_full_accepted(self, sample_activity_detail: ActivityDetail) -> None:
        """smooth_mode='full' should not raise."""
        export = render_export(sample_activity_detail, "gpx", smooth_mode="full")
        assert export.file_format == "gpx"

    def test_outlier_speed_mps_accepted(self, sample_activity_detail: ActivityDetail) -> None:
        """Custom outlier_speed_mps should not raise."""
        export = render_export(sample_activity_detail, "gpx", outlier_speed_mps=10.0)
        assert export.file_format == "gpx"


def _pool_swim_detail(
    raw_report: dict[str, Any] | None = None, *, sport_type: int = 9
) -> ActivityDetail:
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


def test_fit_export_pool_swim_includes_swim_summary() -> None:
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


def test_fit_export_pool_swim_prefers_reported_pool_width_and_frequency() -> None:
    detail = _pool_swim_detail(
        {"pool_width": 50, "turn_count": 60, "stroke_count": 600, "avg_stroke_frequency": 28}
    )
    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    # The report width is 50 m and the distance over turn count would give 25 m.
    assert session.pool_length == pytest.approx(50.0)
    assert session.avg_cadence == 28


def test_fit_export_pool_swim_skips_implausible_pool_length() -> None:
    detail = _pool_swim_detail({"turn_count": 3, "stroke_count": 1377})
    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.pool_length is None
    assert session.num_lengths == 3
    assert session.total_cycles == 1377


def test_fit_export_swim_without_swim_report_has_no_swim_fields() -> None:
    detail = _pool_swim_detail({})
    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.pool_length is None
    assert session.total_cycles is None
    assert session.num_lengths is None


def test_fit_export_non_swim_ignores_stroke_fields() -> None:
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


def _real_swim_sample(
    index: int, values: tuple[int, int, int, int, int, int] | None
) -> ActivitySample:
    if values is None:
        distance = None
        calories = None
        raw_sample = {
            "source": "fds_sport_record",
            "pace": 177,
            "extras": {"1": 1793700677, "11": 1, "13": 252},
        }
    else:
        end_time, pace, swolf, strokes, stroke_rate, calories = values
        distance = 25.0
        raw_sample = {
            "source": "fds_sport_record",
            "pace": pace,
            "extras": {
                "1": end_time,
                "10": 1,
                "11": 1,
                "13": swolf,
                "16": strokes,
                "17": stroke_rate,
            },
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
    detail = _pool_swim_detail(
        {
            "stroke_count": 112,
            "turn_count": 6,
            "pool_width": 0,
            "max_stroke_freq": 28,
            "avg_stroke_frequency": 0,
            "main_posture": 1,
        }
    )
    detail.activity.start_time = 1793700499
    detail.activity.end_time = 1793700765
    detail.activity.duration_seconds = 265
    detail.activity.distance_meters = 150
    detail.activity.calories = 70
    detail.samples = [
        _real_swim_sample(index, values) for index, values in enumerate(_REAL_SWIM_LENGTHS)
    ]
    return detail


def _fit_lengths(fit: FitFile) -> list[Any]:
    from fit_tool.profile.messages.length_message import LengthMessage

    return [r.message for r in fit.records if isinstance(r.message, LengthMessage)]


def test_fit_export_pool_swim_writes_active_lengths() -> None:
    from fit_tool.profile.profile_type import LengthType

    fit = _parse_fit(render_export(_real_pool_swim_detail(), "fit").payload)
    lengths = _fit_lengths(fit)
    active_lengths = [length for length in lengths if length.length_type == LengthType.ACTIVE.value]

    assert len(lengths) == 7
    assert len(active_lengths) == 6
    assert [length.total_strokes for length in active_lengths] == [23, 16, 19, 17, 21, 16]
    assert [length.total_timer_time for length in active_lengths] == [
        60.0,
        34.0,
        44.0,
        39.0,
        50.0,
        37.0,
    ]
    assert [length.avg_swimming_cadence for length in active_lengths] == [23, 28, 25, 26, 25, 25]
    # The fixture's FDS SWOLF (type 13) equals length seconds + strokes.
    assert [int(length.total_timer_time) + length.total_strokes for length in active_lengths] == [
        83,
        50,
        63,
        56,
        71,
        53,
    ]


def test_fit_export_pool_swim_writes_lap_per_set_and_rest() -> None:
    fit = _parse_fit(render_export(_real_pool_swim_detail(), "fit").payload)
    laps = _fit_laps(fit)
    session = _fit_sessions(fit)[0]

    assert len(laps) == 3
    assert [lap.total_distance for lap in laps] == [0.0, 100.0, 50.0]
    assert [lap.num_lengths for lap in laps] == [1, 4, 2]
    assert [lap.num_active_lengths for lap in laps] == [0, 4, 2]
    assert [lap.first_length_index for lap in laps] == [0, 1, 5]
    assert [lap.total_cycles for lap in laps] == [None, 75, 37]
    assert [lap.max_cadence for lap in laps] == [None, 28, 25]
    assert session.num_laps == 3
    assert session.total_cycles == 112
    assert session.pool_length == pytest.approx(25.0)


def test_fit_export_pool_swim_records_have_cumulative_distance() -> None:
    from fit_tool.profile.messages.record_message import RecordMessage

    fit = _parse_fit(render_export(_real_pool_swim_detail(), "fit").payload)
    records = [r.message for r in fit.records if isinstance(r.message, RecordMessage)]

    assert [record.distance for record in records] == [
        0.0,
        0.0,
        25.0,
        50.0,
        75.0,
        100.0,
        125.0,
        150.0,
    ]


def test_fit_export_pool_swim_lap_cadence_is_consistent() -> None:
    laps = _fit_laps(_parse_fit(render_export(_real_pool_swim_detail(), "fit").payload))
    rated_laps = [lap for lap in laps if lap.avg_cadence is not None]

    assert [lap.avg_cadence for lap in laps] == [None, 25, 25]
    assert all(lap.avg_cadence <= lap.max_cadence for lap in rated_laps)


def test_fit_export_pool_swim_writes_swim_stroke() -> None:
    from fit_tool.profile.profile_type import LengthType, SwimStroke

    fit = _parse_fit(render_export(_real_pool_swim_detail(), "fit").payload)
    active_lengths = [
        length for length in _fit_lengths(fit) if length.length_type == LengthType.ACTIVE.value
    ]

    # Mi Fitness style 1 is breaststroke.
    assert {length.swim_stroke for length in active_lengths} == {SwimStroke.BREASTSTROKE.value}
    assert {lap.swim_stroke for lap in _fit_laps(fit) if lap.swim_stroke is not None} == {
        SwimStroke.BREASTSTROKE.value
    }
    assert _fit_sessions(fit)[0].swim_stroke == SwimStroke.BREASTSTROKE.value


def test_fit_export_pool_swim_lap_with_mixed_strokes_is_mixed() -> None:
    from fit_tool.profile.profile_type import SwimStroke

    detail = _real_pool_swim_detail()
    detail.samples[1].raw_sample["extras"]["11"] = 2
    laps = _fit_laps(_parse_fit(render_export(detail, "fit").payload))

    assert [lap.swim_stroke for lap in laps] == [
        None,
        SwimStroke.MIXED.value,
        SwimStroke.BREASTSTROKE.value,
    ]


def test_fit_export_pool_swim_summary_only_uses_main_posture() -> None:
    from fit_tool.profile.profile_type import SwimStroke

    detail = _pool_swim_detail({"stroke_count": 1377, "turn_count": 60, "main_posture": 2})
    fit = _parse_fit(render_export(detail, "fit").payload)

    assert _fit_laps(fit)[0].swim_stroke == SwimStroke.FREESTYLE.value
    assert _fit_sessions(fit)[0].swim_stroke == SwimStroke.FREESTYLE.value


def test_fit_export_pool_swim_set_without_strokes_has_no_cycle_fields() -> None:
    detail = _real_pool_swim_detail()
    for sample in detail.samples[5:]:
        sample.raw_sample["extras"]["16"] = 0

    fit = _parse_fit(render_export(detail, "fit").payload)
    laps = _fit_laps(fit)
    session = _fit_sessions(fit)[0]

    assert laps[-1].total_cycles is None
    assert laps[-1].avg_stroke_distance is None
    # 0-stroke lengths hold no stroke data, so the session averages only the set that
    # reported strokes.
    assert session.total_cycles == 75
    assert session.avg_stroke_count == pytest.approx(75 / 4, abs=0.06)
    assert session.avg_stroke_distance == pytest.approx(100 / 75, abs=0.01)
    assert session.avg_cadence == 25
    assert session.max_cadence == 28


def test_fit_export_pool_swim_prefers_avg_stroke_freq() -> None:
    detail = _pool_swim_detail(
        {"stroke_count": 1377, "turn_count": 60, "avg_stroke_freq": 40, "avg_stroke_frequency": 20}
    )
    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.avg_cadence == 40


def test_fit_export_pool_swim_session_counts_written_lengths() -> None:
    detail = _real_pool_swim_detail()
    detail.activity.raw_report = {"stroke_count": 999, "turn_count": 40, "sport_type": 9}
    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.num_lengths == 7
    assert session.num_active_lengths == 6
    assert session.avg_stroke_count == pytest.approx(112 / 6, abs=0.05)
    assert session.total_cycles == 112
    assert session.avg_stroke_distance == pytest.approx(150 / 112, abs=0.01)
    assert session.avg_cadence == 25
    assert session.max_cadence == 28


def test_fit_export_pool_swim_reported_rates_and_pool_length_win() -> None:
    detail = _real_pool_swim_detail()
    detail.activity.raw_report = {
        "stroke_count": 112,
        "turn_count": 6,
        "pool_width": 50,
        "avg_stroke_freq": 30,
        "max_stroke_freq": 35,
        "sport_type": 9,
    }

    fit = _parse_fit(render_export(detail, "fit").payload)
    laps = _fit_laps(fit)
    session = _fit_sessions(fit)[0]

    assert session.avg_cadence == 30
    assert session.max_cadence == 35
    assert session.pool_length == pytest.approx(50.0)
    assert [lap.avg_cadence for lap in laps] == [None, 25, 25]


def test_fit_export_pool_swim_caps_average_rate_at_maximum() -> None:
    detail = _real_pool_swim_detail()
    detail.activity.raw_report = {
        "stroke_count": 112,
        "turn_count": 6,
        "avg_stroke_freq": 40,
        "max_stroke_freq": 30,
        "sport_type": 9,
    }

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.avg_cadence == 30
    assert session.max_cadence == 30


def test_fit_export_pool_swim_caps_derived_rate_at_reported_maximum() -> None:
    detail = _pool_swim_detail({"stroke_count": 1377, "turn_count": 60, "max_stroke_freq": 20})
    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.avg_cadence == 20
    assert session.max_cadence == 20


def test_fit_export_pool_swim_caps_report_only_average_rate() -> None:
    detail = _pool_swim_detail(
        {"stroke_count": 1377, "turn_count": 60, "avg_stroke_freq": 40, "max_stroke_freq": 30}
    )
    fit = _parse_fit(render_export(detail, "fit").payload)

    assert _fit_sessions(fit)[0].avg_cadence == 30
    assert _fit_sessions(fit)[0].max_cadence == 30
    assert _fit_laps(fit)[0].avg_cadence == 30


def test_fit_export_pool_swim_caps_average_at_measured_maximum() -> None:
    detail = _real_pool_swim_detail()
    detail.activity.raw_report = {
        "stroke_count": 112,
        "turn_count": 6,
        "avg_stroke_freq": 50,
        "sport_type": 9,
    }

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.avg_cadence == 28
    assert session.max_cadence == 28


def test_fit_export_pool_swim_ignores_implausible_report_pool_width() -> None:
    detail = _real_pool_swim_detail()
    detail.activity.raw_report = {
        "stroke_count": 112,
        "turn_count": 6,
        "pool_width": 5,
        "sport_type": 9,
    }

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.pool_length == pytest.approx(25.0)


def test_fit_export_pool_swim_measured_pool_length_beats_report_estimate() -> None:
    detail = _real_pool_swim_detail()
    detail.activity.raw_report = {"stroke_count": 112, "turn_count": 7, "sport_type": 9}

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.pool_length == pytest.approx(25.0)


def test_fit_export_pool_swim_ignores_zero_stroke_rate() -> None:
    from fit_tool.profile.messages.record_message import RecordMessage
    from fit_tool.profile.profile_type import LengthType

    detail = _real_pool_swim_detail()
    for sample in detail.samples[5:]:
        sample.raw_sample["extras"]["16"] = 0
        sample.raw_sample["extras"]["17"] = 0

    fit = _parse_fit(render_export(detail, "fit").payload)
    laps = _fit_laps(fit)
    session = _fit_sessions(fit)[0]
    last_lengths = [
        length
        for length in _fit_lengths(fit)
        if length.length_type == LengthType.ACTIVE.value and length.start_time >= 1793700677000
    ]
    last_records = [
        record
        for record in (r.message for r in fit.records)
        if isinstance(record, RecordMessage) and record.timestamp > 1793700677000
    ]

    assert laps[2].avg_cadence is None
    assert laps[2].max_cadence is None
    assert [length.avg_swimming_cadence for length in last_lengths] == [None, None]
    assert [record.cadence for record in last_records] == [None, None]
    assert session.avg_cadence == 25
    assert session.max_cadence == 28


def test_fit_export_pool_swim_zero_stroke_lengths_have_no_stroke_fields() -> None:
    from fit_tool.profile.profile_type import LengthType

    detail = _real_pool_swim_detail()
    for sample in detail.samples[5:]:
        sample.raw_sample["extras"]["16"] = 0

    fit = _parse_fit(render_export(detail, "fit").payload)
    last_lengths = [
        length
        for length in _fit_lengths(fit)
        if length.length_type == LengthType.ACTIVE.value and length.start_time >= 1793700677000
    ]

    assert [length.total_strokes for length in last_lengths] == [None, None]


def test_fit_export_pool_swim_session_maximum_rate_covers_every_lap() -> None:
    detail = _real_pool_swim_detail()
    detail.activity.raw_report = {
        "stroke_count": 112,
        "turn_count": 6,
        "max_stroke_freq": 25,
        "sport_type": 9,
    }

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.max_cadence == 28


def test_fit_export_pool_swim_ignores_sample_cadence_for_session_rate() -> None:
    detail = _pool_swim_detail({"turn_count": 60, "max_stroke_freq": 30})
    detail.samples[0].cadence = 100

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.avg_cadence is None
    assert session.max_cadence == 30


def test_fit_export_pool_swim_length_without_pace_uses_previous_end() -> None:
    detail = _real_pool_swim_detail()
    detail.samples[0].raw_sample.pop("pace")

    laps = _fit_laps(_parse_fit(render_export(detail, "fit").payload))

    # Without its pace, the first length starts at the session start, so there is no leading
    # rest lap.
    assert [lap.num_lengths for lap in laps] == [4, 2]


def test_fit_export_pool_swim_omits_derived_rate_that_rounds_to_zero() -> None:
    detail = _pool_swim_detail({"stroke_count": 20, "turn_count": 60})

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.avg_cadence is None


def test_fit_export_pool_swim_omits_reported_rate_that_rounds_to_zero() -> None:
    detail = _pool_swim_detail({"stroke_count": 1377, "turn_count": 60, "max_stroke_freq": 0.4})

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.max_cadence is None
    assert session.avg_cadence == 30


@pytest.mark.parametrize("rate_key", ["avg_stroke_freq", "avg_stroke_frequency"])
def test_fit_export_pool_swim_omits_reported_average_rate_that_rounds_to_zero(
    rate_key: str,
) -> None:
    detail = _pool_swim_detail({"stroke_count": 1377, "turn_count": 60, rate_key: 0.4})

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    # 0.4 strokes/min rounds to 0, so the derived 30 strokes/min stands instead.
    assert session.avg_cadence == 30


def test_fit_export_pool_swim_without_length_count_writes_strokes() -> None:
    detail = _pool_swim_detail({"stroke_count": 600})

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.total_cycles == 600
    assert session.num_lengths is None
    assert session.pool_length is None


def test_fit_export_pool_swim_falls_back_to_report_pool_length_when_lengths_differ() -> None:
    detail = _real_pool_swim_detail()
    detail.samples[0].distance_meters = 20.0

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    # Mixed written lengths give no pool length, so the report distance over turn count decides.
    assert session.pool_length == pytest.approx(25.0)


def test_fit_export_pool_swim_skips_length_with_non_integer_end_time() -> None:
    detail = _real_pool_swim_detail()
    detail.samples[3].raw_sample["extras"]["1"] = "1793700677"

    fit = _parse_fit(render_export(detail, "fit").payload)
    active_lengths = [
        length for length in _fit_lengths(fit) if length.length_type == LengthType.ACTIVE.value
    ]

    assert len(active_lengths) == 5


def test_fit_export_pool_swim_ignores_boolean_stroke_count() -> None:
    detail = _pool_swim_detail({"stroke_count": True, "turn_count": 60})

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.total_cycles is None


def test_fit_export_pool_swim_ignores_boolean_posture() -> None:
    detail = _pool_swim_detail({"stroke_count": 10, "turn_count": 2, "main_posture": True})

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.swim_stroke is None


def test_fit_export_pool_swim_lengths_carry_speed_and_calories() -> None:
    fit = _parse_fit(render_export(_real_pool_swim_detail(), "fit").payload)
    active_lengths = [
        length for length in _fit_lengths(fit) if length.length_type == LengthType.ACTIVE.value
    ]

    assert [(length.avg_speed, length.total_calories) for length in active_lengths] == [
        (0.417, 16),
        (0.735, 9),
        (0.568, 11),
        (0.641, 10),
        (0.5, 13),
        (0.676, 9),
    ]


def test_fit_export_pool_swim_laps_carry_speed_and_calories() -> None:
    laps = _fit_laps(_parse_fit(render_export(_real_pool_swim_detail(), "fit").payload))

    assert [(lap.avg_speed, lap.total_calories) for lap in laps] == [
        (None, None),
        (0.565, 46),
        (0.575, 22),
    ]


def test_fit_export_pool_swim_records_carry_speed_and_rate() -> None:
    fit = _parse_fit(render_export(_real_pool_swim_detail(), "fit").payload)
    records = [
        record.message for record in fit.records if isinstance(record.message, RecordMessage)
    ]

    assert [(record.speed, record.cadence) for record in records] == [
        (None, None),
        (0.0, None),
        (0.417, 23),
        (0.735, 28),
        (0.568, 25),
        (0.641, 26),
        (0.5, 25),
        (0.676, 25),
    ]


def test_fit_export_pool_swim_lengths_are_indexed_length_events() -> None:
    lengths = _fit_lengths(_parse_fit(render_export(_real_pool_swim_detail(), "fit").payload))

    assert [(length.message_index, length.event, length.event_type) for length in lengths] == [
        (index, Event.LENGTH.value, EventType.STOP.value) for index in range(7)
    ]


def test_fit_export_pool_swim_laps_are_indexed() -> None:
    laps = _fit_laps(_parse_fit(render_export(_real_pool_swim_detail(), "fit").payload))

    assert [lap.message_index for lap in laps] == [0, 1, 2]


def test_fit_export_pool_swim_session_points_at_first_lap_in_metric_pool() -> None:
    session = _fit_sessions(_parse_fit(render_export(_real_pool_swim_detail(), "fit").payload))[0]

    assert (session.first_lap_index, session.pool_length_unit) == (0, DisplayMeasure.METRIC.value)


@pytest.mark.parametrize(
    ("main_posture", "swim_stroke"),
    [
        (0, SwimStroke.MIXED),
        (1, SwimStroke.BREASTSTROKE),
        (2, SwimStroke.FREESTYLE),
        (3, SwimStroke.BACKSTROKE),
        (4, SwimStroke.BUTTERFLY),
    ],
)
def test_fit_export_pool_swim_maps_mi_fitness_style_to_fit_stroke(
    main_posture: int, swim_stroke: SwimStroke
) -> None:
    detail = _pool_swim_detail(
        {"stroke_count": 1377, "turn_count": 60, "main_posture": main_posture}
    )

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.swim_stroke == swim_stroke.value


def test_fit_export_pool_swim_cadence_fallback_counts_stroked_lengths_only() -> None:
    detail = _real_pool_swim_detail()
    for sample in detail.samples:
        if isinstance(sample.raw_sample.get("extras"), dict):
            sample.raw_sample["extras"].pop("17", None)
    for sample in detail.samples[5:]:
        sample.raw_sample["extras"]["16"] = 0

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    # 75 strokes in the 177 s of the set that reported them, not over the whole 264 s of swimming.
    assert session.avg_cadence == 25
    assert session.max_cadence == 28


def test_fit_export_pool_swim_uses_lengths_without_swim_report() -> None:
    detail = _real_pool_swim_detail()
    detail.activity.raw_report = {}
    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.total_cycles == 112
    assert session.num_lengths == 7
    assert session.num_active_lengths == 6
    assert session.pool_length == pytest.approx(25.0)
    assert session.avg_stroke_distance == pytest.approx(150 / 112, abs=0.01)
    assert session.avg_cadence == 25
    assert session.max_cadence == 28


def test_fit_export_pool_swim_average_speed_uses_lap_span() -> None:
    detail = _real_pool_swim_detail()
    detail.activity.duration_seconds = 200

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.total_timer_time == 265.0
    assert session.avg_speed == pytest.approx(150 / 265, abs=0.001)


def test_fit_export_pool_swim_session_timestamp_keeps_the_activity_end() -> None:
    fit = _parse_fit(render_export(_real_pool_swim_detail(), "fit").payload)
    laps = _fit_laps(fit)
    session = _fit_sessions(fit)[0]

    # The activity ends a second after the last lap, and the session timestamp follows the activity.
    assert session.timestamp == 1793700765000
    assert laps[-1].timestamp == 1793700764000


def test_fit_export_pool_swim_laps_tile_the_session() -> None:
    fit = _parse_fit(render_export(_real_pool_swim_detail(), "fit").payload)
    laps = _fit_laps(fit)
    session = _fit_sessions(fit)[0]

    assert laps[0].start_time == session.start_time
    assert sum(lap.total_elapsed_time for lap in laps) == session.total_elapsed_time
    assert sum(lap.total_distance for lap in laps) == session.total_distance
    for lap, next_lap in itertools.pairwise(laps):
        assert next_lap.start_time == lap.start_time + round(lap.total_elapsed_time * 1000)
    assert all(lap.total_elapsed_time == lap.total_timer_time for lap in laps)
    assert all(length.total_elapsed_time == length.total_timer_time for length in _fit_lengths(fit))


def test_fit_export_pool_swim_laps_cover_lengths_past_the_report_duration() -> None:
    detail = _real_pool_swim_detail()
    last_length_end = (detail.activity.end_time or 0) + 10
    detail.samples[6].raw_sample["extras"]["1"] = last_length_end

    fit = _parse_fit(render_export(detail, "fit").payload)
    laps = _fit_laps(fit)
    session = _fit_sessions(fit)[0]
    activity = _fit_activities(fit)[0]

    assert [lap.total_elapsed_time for lap in laps] == [1.0, 177.0, 98.0]
    assert sum(lap.total_elapsed_time for lap in laps) == session.total_elapsed_time == 276.0
    assert session.timestamp == laps[-1].timestamp == last_length_end * 1000
    assert activity.timestamp == last_length_end * 1000
    assert activity.total_timer_time == session.total_timer_time == 276.0
    assert activity.local_timestamp == _fit_local_timestamp(last_length_end, 0)


def test_fit_export_pool_swim_writes_trailing_rest_lap() -> None:
    from fit_tool.profile.profile_type import LengthType

    detail = _real_pool_swim_detail()
    detail.activity.duration_seconds = 300

    fit = _parse_fit(render_export(detail, "fit").payload)
    laps = _fit_laps(fit)
    session = _fit_sessions(fit)[0]
    idle_lengths = [
        length for length in _fit_lengths(fit) if length.length_type == LengthType.IDLE.value
    ]

    assert [lap.total_elapsed_time for lap in laps] == [1.0, 177.0, 87.0, 35.0]
    assert laps[-1].total_distance == 0.0
    assert laps[-1].num_active_lengths == 0
    assert len(idle_lengths) == 2
    assert sum(lap.total_elapsed_time for lap in laps) == session.total_elapsed_time == 300.0


def test_fit_export_pool_swim_skips_length_without_swim_time() -> None:
    from fit_tool.profile.profile_type import LengthType

    detail = _real_pool_swim_detail()
    detail.samples[3].raw_sample["extras"]["1"] = 1793700638

    fit = _parse_fit(render_export(detail, "fit").payload)
    active_lengths = [
        length for length in _fit_lengths(fit) if length.length_type == LengthType.ACTIVE.value
    ]

    assert len(active_lengths) == 5
    assert all(length.total_timer_time > 0 for length in _fit_lengths(fit))
    session = _fit_sessions(fit)[0]
    assert session.num_active_lengths == 5
    assert session.total_distance == 125.0
    assert session.avg_speed == pytest.approx(125 / 265, abs=0.001)


def test_fit_export_pool_swim_skips_length_without_end_time() -> None:
    from fit_tool.profile.profile_type import LengthType

    detail = _real_pool_swim_detail()
    detail.samples[3].raw_sample["extras"].pop("1")

    fit = _parse_fit(render_export(detail, "fit").payload)
    active_lengths = [
        length for length in _fit_lengths(fit) if length.length_type == LengthType.ACTIVE.value
    ]

    assert len(active_lengths) == 5


def test_fit_export_pool_swim_ignores_implausible_written_pool_length() -> None:
    detail = _real_pool_swim_detail()
    detail.activity.raw_report = {}
    for sample in detail.samples:
        if sample.distance_meters:
            sample.distance_meters = 5.0

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.pool_length is None


def test_fit_export_pool_swim_without_any_strokes_keeps_report_summary() -> None:
    detail = _real_pool_swim_detail()
    for sample in detail.samples:
        if isinstance(sample.raw_sample.get("extras"), dict):
            sample.raw_sample["extras"]["16"] = 0

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    # The lengths carry no stroke data, so the report's stroke summary stands.
    assert session.total_cycles == 112
    assert session.avg_stroke_count == pytest.approx(112 / 6, abs=0.05)
    assert session.avg_stroke_distance == pytest.approx(150 / 112, abs=0.01)
    assert session.avg_cadence == 25


def test_fit_export_pool_swim_without_strokes_or_report_has_no_cadence() -> None:
    detail = _real_pool_swim_detail()
    detail.activity.raw_report = {}
    for sample in detail.samples:
        if isinstance(sample.raw_sample.get("extras"), dict):
            sample.raw_sample["extras"]["16"] = 0
            sample.raw_sample["extras"].pop("17", None)

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.total_cycles is None
    assert session.avg_cadence is None
    assert session.avg_stroke_count is None


def test_fit_export_pool_swim_lap_stroke_distance_uses_stroked_lengths() -> None:
    detail = _real_pool_swim_detail()
    detail.samples[1].raw_sample["extras"]["16"] = 0

    laps = _fit_laps(_parse_fit(render_export(detail, "fit").payload))

    assert laps[1].total_cycles == 59
    assert laps[1].avg_stroke_distance == pytest.approx(75 / 59, abs=0.01)


def test_fit_export_pool_swim_lap_cadence_without_rates_uses_active_time() -> None:
    detail = _real_pool_swim_detail()
    for sample in detail.samples[:4]:
        sample.raw_sample["extras"].pop("17")
    detail.samples[1].raw_sample["pace"] = (
        100  # 25 s for 25 m, leaving 9 s of rest after the first length
    )

    laps = _fit_laps(_parse_fit(render_export(detail, "fit").payload))

    assert laps[1].total_elapsed_time == laps[1].total_timer_time == 177.0
    assert laps[1].avg_cadence == 27
    assert laps[1].max_cadence is None


def test_fit_export_pool_swim_session_cadence_uses_active_time() -> None:
    detail = _real_pool_swim_detail()
    detail.activity.duration_seconds = 400
    detail.activity.raw_report = {"stroke_count": 112, "turn_count": 6, "sport_type": 9}
    for sample in detail.samples:
        if isinstance(sample.raw_sample.get("extras"), dict):
            sample.raw_sample["extras"].pop("17", None)

    session = _fit_sessions(_parse_fit(render_export(detail, "fit").payload))[0]

    assert session.total_elapsed_time == 400.0
    assert session.avg_cadence == 25


def test_fit_export_pool_swim_strava_totals_come_from_laps() -> None:
    laps = _fit_laps(_parse_fit(render_export(_real_pool_swim_detail(), "fit").payload))

    assert sum(lap.total_distance for lap in laps) == pytest.approx(150.0)
    assert sum(lap.total_timer_time for lap in laps if lap.total_distance > 0) == pytest.approx(
        264.0
    )


def test_fit_export_pool_swim_writes_rest_lap_between_sets() -> None:
    from fit_tool.profile.profile_type import LengthType

    detail = _real_pool_swim_detail()
    detail.samples[5].raw_sample["pace"] = 108  # 27 s for 25 m, leaving 23 s of rest before the set

    fit = _parse_fit(render_export(detail, "fit").payload)
    laps = _fit_laps(fit)
    idle_lengths = [
        length for length in _fit_lengths(fit) if length.length_type == LengthType.IDLE.value
    ]

    assert [lap.total_distance for lap in laps] == [0.0, 100.0, 0.0, 50.0]
    assert [lap.num_active_lengths for lap in laps] == [0, 4, 0, 2]
    assert len(idle_lengths) == 2
    assert sum(lap.total_elapsed_time for lap in laps) == _fit_sessions(fit)[0].total_elapsed_time


def test_fit_export_pool_swim_rest_inside_set_is_idle_length() -> None:
    from fit_tool.profile.profile_type import LengthType

    detail = _real_pool_swim_detail()
    detail.samples[1].raw_sample["pace"] = (
        100  # 25 s for 25 m, leaving 9 s of rest after the first length
    )

    fit = _parse_fit(render_export(detail, "fit").payload)
    laps = _fit_laps(fit)
    idle_lengths = [
        length for length in _fit_lengths(fit) if length.length_type == LengthType.IDLE.value
    ]

    assert [lap.num_lengths for lap in laps] == [1, 5, 2]
    assert [lap.num_active_lengths for lap in laps] == [0, 4, 2]
    assert laps[1].total_elapsed_time == laps[1].total_timer_time == 177.0
    assert len(idle_lengths) == 2
