from __future__ import annotations

import gzip
from dataclasses import dataclass
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

from mi_fitness_sync.activity.models import ActivityDetail, TrackPoint
from mi_fitness_sync.exceptions import MiFitnessError
from mi_fitness_sync.fds.sport_reports import SportReport


SUPPORTED_EXPORT_FORMATS = ("fit", "gpx", "tcx")
GPX_TRACKPOINT_NS = "http://www.garmin.com/xmlschemas/TrackPointExtension/v1"
TCX_NS = "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
TCX_ACTIVITY_NS = "http://www.garmin.com/xmlschemas/ActivityExtension/v2"
XML_SCHEMA_INSTANCE_NS = "http://www.w3.org/2001/XMLSchema-instance"


@dataclass(slots=True)
class ExportResult:
    file_format: str
    compressed: bool
    payload: bytes


def render_export(detail: ActivityDetail, file_format: str, *, compress: bool = False) -> ExportResult:
    normalized_format = file_format.strip().lower()
    if normalized_format not in SUPPORTED_EXPORT_FORMATS:
        raise MiFitnessError(
            f"Unsupported export format: {file_format}. Expected one of: {', '.join(SUPPORTED_EXPORT_FORMATS)}."
        )

    if normalized_format == "gpx":
        payload = render_gpx(detail)
    elif normalized_format == "tcx":
        payload = render_tcx(detail)
    else:
        payload = render_fit(detail)

    if compress:
        payload = gzip.compress(payload)

    return ExportResult(file_format=normalized_format, compressed=compress, payload=payload)


def render_gpx(detail: ActivityDetail) -> bytes:
    track_points = detail.track_points
    if not track_points:
        raise MiFitnessError("This activity detail payload does not contain GPS track points, so GPX export is not available.")

    ET.register_namespace("", "http://www.topografix.com/GPX/1/1")
    ET.register_namespace("gpxtpx", GPX_TRACKPOINT_NS)

    root = ET.Element(
        "{http://www.topografix.com/GPX/1/1}gpx",
        {
            "version": "1.1",
            "creator": "Mi Fitness Sync",
            "{%s}schemaLocation" % XML_SCHEMA_INSTANCE_NS: (
                "http://www.topografix.com/GPX/1/1 "
                "http://www.topografix.com/GPX/1/1/gpx.xsd "
                f"{GPX_TRACKPOINT_NS} {GPX_TRACKPOINT_NS}/TrackPointExtensionv1.xsd"
            ),
        },
    )

    metadata = ET.SubElement(root, "{http://www.topografix.com/GPX/1/1}metadata")
    ET.SubElement(metadata, "{http://www.topografix.com/GPX/1/1}name").text = detail.activity.title
    ET.SubElement(metadata, "{http://www.topografix.com/GPX/1/1}time").text = _isoformat_utc(detail.start_time)

    track = ET.SubElement(root, "{http://www.topografix.com/GPX/1/1}trk")
    ET.SubElement(track, "{http://www.topografix.com/GPX/1/1}name").text = detail.activity.title
    segment = ET.SubElement(track, "{http://www.topografix.com/GPX/1/1}trkseg")

    for point in track_points:
        track_point = ET.SubElement(
            segment,
            "{http://www.topografix.com/GPX/1/1}trkpt",
            {"lat": _format_coordinate(point.latitude), "lon": _format_coordinate(point.longitude)},
        )
        if point.altitude_meters is not None:
            ET.SubElement(track_point, "{http://www.topografix.com/GPX/1/1}ele").text = _format_decimal(point.altitude_meters)
        ET.SubElement(track_point, "{http://www.topografix.com/GPX/1/1}time").text = _isoformat_utc(point.timestamp)

        if point.heart_rate is not None or point.cadence is not None:
            extensions = ET.SubElement(track_point, "{http://www.topografix.com/GPX/1/1}extensions")
            track_extension = ET.SubElement(extensions, f"{{{GPX_TRACKPOINT_NS}}}TrackPointExtension")
            if point.heart_rate is not None:
                ET.SubElement(track_extension, f"{{{GPX_TRACKPOINT_NS}}}hr").text = str(point.heart_rate)
            if point.cadence is not None:
                ET.SubElement(track_extension, f"{{{GPX_TRACKPOINT_NS}}}cad").text = str(point.cadence)

    return _xml_bytes(root)


def render_tcx(detail: ActivityDetail) -> bytes:
    export_points = _export_points(detail)
    if not export_points:
        raise MiFitnessError("This activity detail payload does not contain timestamped samples, so TCX export is not available.")

    ET.register_namespace("", TCX_NS)
    ET.register_namespace("aext", TCX_ACTIVITY_NS)
    ET.register_namespace("xsi", XML_SCHEMA_INSTANCE_NS)

    root = ET.Element(
        f"{{{TCX_NS}}}TrainingCenterDatabase",
        {
            f"{{{XML_SCHEMA_INSTANCE_NS}}}schemaLocation": (
                f"{TCX_NS} http://www.garmin.com/xmlschemas/TrainingCenterDatabasev2.xsd"
            )
        },
    )
    activities = ET.SubElement(root, f"{{{TCX_NS}}}Activities")
    activity = ET.SubElement(activities, f"{{{TCX_NS}}}Activity", {"Sport": _tcx_sport(detail.activity.category)})
    ET.SubElement(activity, f"{{{TCX_NS}}}Id").text = _isoformat_utc(detail.start_time)

    lap = ET.SubElement(activity, f"{{{TCX_NS}}}Lap", {"StartTime": _isoformat_utc(detail.start_time)})
    ET.SubElement(lap, f"{{{TCX_NS}}}TotalTimeSeconds").text = _format_decimal(float(detail.total_duration_seconds))
    ET.SubElement(lap, f"{{{TCX_NS}}}DistanceMeters").text = _format_decimal(detail.total_distance_meters)
    ET.SubElement(lap, f"{{{TCX_NS}}}Calories").text = str(detail.total_calories or 0)
    ET.SubElement(lap, f"{{{TCX_NS}}}Intensity").text = "Active"
    ET.SubElement(lap, f"{{{TCX_NS}}}TriggerMethod").text = "Manual"

    average_heart_rate = _average_heart_rate(export_points)
    if average_heart_rate is not None:
        average_node = ET.SubElement(lap, f"{{{TCX_NS}}}AverageHeartRateBpm")
        ET.SubElement(average_node, f"{{{TCX_NS}}}Value").text = str(average_heart_rate)

    maximum_heart_rate = _maximum_heart_rate(export_points)
    if maximum_heart_rate is not None:
        maximum_node = ET.SubElement(lap, f"{{{TCX_NS}}}MaximumHeartRateBpm")
        ET.SubElement(maximum_node, f"{{{TCX_NS}}}Value").text = str(maximum_heart_rate)

    track = ET.SubElement(lap, f"{{{TCX_NS}}}Track")
    for point in export_points:
        track_point = ET.SubElement(track, f"{{{TCX_NS}}}Trackpoint")
        ET.SubElement(track_point, f"{{{TCX_NS}}}Time").text = _isoformat_utc(point.timestamp)

        if point.latitude is not None and point.longitude is not None:
            position = ET.SubElement(track_point, f"{{{TCX_NS}}}Position")
            ET.SubElement(position, f"{{{TCX_NS}}}LatitudeDegrees").text = _format_coordinate(point.latitude)
            ET.SubElement(position, f"{{{TCX_NS}}}LongitudeDegrees").text = _format_coordinate(point.longitude)

        if point.altitude_meters is not None:
            ET.SubElement(track_point, f"{{{TCX_NS}}}AltitudeMeters").text = _format_decimal(point.altitude_meters)

        if point.distance_meters is not None:
            ET.SubElement(track_point, f"{{{TCX_NS}}}DistanceMeters").text = _format_decimal(point.distance_meters)

        if point.heart_rate is not None:
            heart_rate = ET.SubElement(track_point, f"{{{TCX_NS}}}HeartRateBpm")
            ET.SubElement(heart_rate, f"{{{TCX_NS}}}Value").text = str(point.heart_rate)

        if point.cadence is not None:
            ET.SubElement(track_point, f"{{{TCX_NS}}}Cadence").text = str(point.cadence)

        if point.speed_mps is not None:
            extensions = ET.SubElement(track_point, f"{{{TCX_NS}}}Extensions")
            tpx = ET.SubElement(extensions, f"{{{TCX_ACTIVITY_NS}}}TPX")
            ET.SubElement(tpx, f"{{{TCX_ACTIVITY_NS}}}Speed").text = _format_decimal(point.speed_mps)

    return _xml_bytes(root)


def render_fit(detail: ActivityDetail) -> bytes:
    export_points = _export_points(detail)
    if not export_points:
        raise MiFitnessError("This activity detail payload does not contain timestamped samples, so FIT export is not available.")

    from fit_tool.fit_file_builder import FitFileBuilder
    from fit_tool.profile.messages.activity_message import ActivityMessage
    from fit_tool.profile.messages.event_message import EventMessage
    from fit_tool.profile.messages.file_id_message import FileIdMessage
    from fit_tool.profile.messages.lap_message import LapMessage
    from fit_tool.profile.messages.record_message import RecordMessage
    from fit_tool.profile.messages.session_message import SessionMessage
    from fit_tool.profile.profile_type import Activity as FitActivity
    from fit_tool.profile.profile_type import Event, EventType, FileType, Manufacturer

    builder = FitFileBuilder(auto_define=True)
    raw_report = detail.activity.raw_report
    proto_type = raw_report.get("proto_type") if isinstance(raw_report.get("proto_type"), int) else None
    fit_sport, fit_sub_sport = _fit_sport_mapping(detail.activity.sport_type, detail.activity.category, proto_type)
    start_timestamp_ms = _unix_millis(detail.start_time)
    end_timestamp_ms = _unix_millis(detail.end_time)

    file_id = FileIdMessage()
    file_id.type = FileType.ACTIVITY
    file_id.manufacturer = Manufacturer.STRAVA
    file_id.product = 0
    file_id.serial_number = 0
    file_id.time_created = end_timestamp_ms
    builder.add(file_id)

    start_event = EventMessage()
    start_event.timestamp = start_timestamp_ms
    start_event.event = Event.TIMER
    start_event.event_type = EventType.START
    builder.add(start_event)

    for point in export_points:
        record = RecordMessage()
        record.timestamp = _unix_millis(point.timestamp)
        if point.latitude is not None and point.longitude is not None:
            record.position_lat = point.latitude
            record.position_long = point.longitude
        if point.altitude_meters is not None:
            record.altitude = point.altitude_meters
        if point.distance_meters is not None:
            record.distance = point.distance_meters
        if point.speed_mps is not None:
            record.speed = point.speed_mps
        if point.heart_rate is not None:
            record.heart_rate = point.heart_rate
        if point.cadence is not None:
            record.cadence = point.cadence
        builder.add(record)

    lap = LapMessage()
    lap.timestamp = end_timestamp_ms
    lap.start_time = start_timestamp_ms
    lap.total_elapsed_time = float(detail.total_duration_seconds)
    lap.total_timer_time = float(detail.total_duration_seconds)
    lap.total_distance = detail.total_distance_meters
    lap.total_calories = detail.total_calories or 0
    lap.sport = fit_sport
    lap.sub_sport = fit_sub_sport
    if export_points and export_points[0].latitude is not None and export_points[0].longitude is not None:
        lap.start_position_lat = export_points[0].latitude
        lap.start_position_long = export_points[0].longitude
    if detail.track_points:
        lap.end_position_lat = detail.track_points[-1].latitude
        lap.end_position_long = detail.track_points[-1].longitude
    average_heart_rate = _average_heart_rate(export_points)
    if average_heart_rate is not None:
        lap.avg_heart_rate = average_heart_rate
    maximum_heart_rate = _maximum_heart_rate(export_points)
    if maximum_heart_rate is not None:
        lap.max_heart_rate = maximum_heart_rate
    report = detail.sport_report
    ascent = _total_ascent(report, export_points)
    descent = _total_descent(report, export_points)
    if ascent is not None:
        lap.total_ascent = ascent
    builder.add(lap)

    session = SessionMessage()
    session.timestamp = end_timestamp_ms
    session.start_time = start_timestamp_ms
    session.total_elapsed_time = float(detail.total_duration_seconds)
    session.total_timer_time = float(detail.total_duration_seconds)
    session.total_distance = detail.total_distance_meters
    session.total_calories = detail.total_calories or 0
    session.sport = fit_sport
    session.sub_sport = fit_sub_sport
    session.num_laps = 1
    if export_points and export_points[0].latitude is not None and export_points[0].longitude is not None:
        session.start_position_lat = export_points[0].latitude
        session.start_position_long = export_points[0].longitude
    if detail.track_points:
        session.end_position_lat = detail.track_points[-1].latitude
        session.end_position_long = detail.track_points[-1].longitude
    if average_heart_rate is not None:
        session.avg_heart_rate = average_heart_rate
    if maximum_heart_rate is not None:
        session.max_heart_rate = maximum_heart_rate
    avg_speed = _avg_speed(report, detail.total_distance_meters, float(detail.total_duration_seconds))
    if avg_speed is not None:
        session.avg_speed = avg_speed
    max_speed = _max_speed(report, export_points)
    if max_speed is not None:
        session.max_speed = max_speed
    avg_cadence = _avg_cadence(report, export_points)
    if avg_cadence is not None:
        session.avg_cadence = avg_cadence
    if ascent is not None:
        session.total_ascent = ascent
    if descent is not None:
        session.total_descent = descent
    total_strides = _total_strides(detail)
    if total_strides is not None:
        session.total_strides = total_strides
        if detail.total_distance_meters > 0:
            session.avg_step_length = detail.total_distance_meters / total_strides
    builder.add(session)

    stop_event = EventMessage()
    stop_event.timestamp = end_timestamp_ms
    stop_event.event = Event.TIMER
    stop_event.event_type = EventType.STOP_DISABLE_ALL
    builder.add(stop_event)

    activity = ActivityMessage()
    activity.timestamp = end_timestamp_ms
    activity.total_timer_time = float(detail.total_duration_seconds)
    activity.num_sessions = 1
    activity.type = FitActivity.MANUAL
    activity.event = Event.ACTIVITY
    activity.event_type = EventType.STOP
    if detail.zone_offset_seconds is not None:
        activity.local_timestamp = _fit_local_timestamp(detail.end_time, detail.zone_offset_seconds)
    builder.add(activity)

    return builder.build().to_bytes()


def _export_points(detail: ActivityDetail) -> list[TrackPoint]:
    if detail.track_points:
        return detail.track_points
    return [
        TrackPoint(
            timestamp=sample.timestamp,
            latitude=None,
            longitude=None,
            altitude_meters=sample.altitude_meters,
            speed_mps=sample.speed_mps,
            distance_meters=sample.distance_meters,
            heart_rate=sample.heart_rate,
            cadence=sample.cadence,
            raw_point=sample.raw_sample,
        )
        for sample in detail.samples
    ]


def _average_heart_rate(points: list[TrackPoint]) -> int | None:
    values = [point.heart_rate for point in points if point.heart_rate is not None]
    if not values:
        return None
    return round(sum(values) / len(values))


def _maximum_heart_rate(points: list[TrackPoint]) -> int | None:
    values = [point.heart_rate for point in points if point.heart_rate is not None]
    if not values:
        return None
    return max(values)


def _avg_speed(report: SportReport | None, total_distance: float, total_time: float) -> float | None:
    if report is not None and report.avg_speed is not None:
        return report.avg_speed / 3.6
    if total_distance > 0 and total_time > 0:
        return total_distance / total_time
    return None


def _max_speed(report: SportReport | None, points: list[TrackPoint]) -> float | None:
    if report is not None and report.max_speed is not None:
        return report.max_speed / 3.6
    values = [p.speed_mps for p in points if p.speed_mps is not None]
    if values:
        return max(values)
    return None


def _avg_cadence(report: SportReport | None, points: list[TrackPoint]) -> int | None:
    if report is not None and report.avg_cadence is not None:
        return report.avg_cadence
    values = [p.cadence for p in points if p.cadence is not None]
    if values:
        return round(sum(values) / len(values))
    return None


def _total_ascent(report: SportReport | None, points: list[TrackPoint]) -> int | None:
    if report is not None and report.rise_height is not None:
        return round(report.rise_height)
    return _compute_elevation_gain(points)


def _total_descent(report: SportReport | None, points: list[TrackPoint]) -> int | None:
    if report is not None and report.fall_height is not None:
        return round(report.fall_height)
    return _compute_elevation_loss(points)


def _compute_elevation_gain(points: list[TrackPoint]) -> int | None:
    altitudes = [p.altitude_meters for p in points if p.altitude_meters is not None]
    if len(altitudes) < 2:
        return None
    gain = sum(b - a for a, b in zip(altitudes, altitudes[1:]) if b > a)
    return round(gain)


def _compute_elevation_loss(points: list[TrackPoint]) -> int | None:
    altitudes = [p.altitude_meters for p in points if p.altitude_meters is not None]
    if len(altitudes) < 2:
        return None
    loss = sum(a - b for a, b in zip(altitudes, altitudes[1:]) if a > b)
    return round(loss)


def _total_strides(detail: "ActivityDetail") -> int | None:
    steps = detail.activity.steps
    if steps is not None and steps > 0:
        return steps // 2
    return None


def _fit_sport_mapping(sport_type: int | None, category: str | None, proto_type: int | None = None) -> tuple:
    from fit_tool.profile.profile_type import Sport, SubSport

    _PROTO_TYPE_MAP: dict[int, tuple] = {
        1: (Sport.RUNNING, SubSport.STREET),           # Outdoor Run
        2: (Sport.RUNNING, SubSport.TRACK),             # Track Running
        3: (Sport.RUNNING, SubSport.TREADMILL),         # Indoor Run
        4: (Sport.WALKING, SubSport.CASUAL_WALKING),    # Outdoor Walk
        5: (Sport.RUNNING, SubSport.TRAIL),             # Trail Run
        6: (Sport.CYCLING, SubSport.ROAD),              # Outdoor Cycling
        7: (Sport.CYCLING, SubSport.INDOOR_CYCLING),    # Indoor Cycling
        8: (Sport.TRAINING, SubSport.GENERIC),          # Free Training
        9: (Sport.SWIMMING, SubSport.LAP_SWIMMING),     # Pool Swimming
        10: (Sport.SWIMMING, SubSport.OPEN_WATER),      # Open Water Swimming
        11: (Sport.FITNESS_EQUIPMENT, SubSport.ELLIPTICAL),  # Elliptical
        12: (Sport.TRAINING, SubSport.YOGA),            # Yoga
        13: (Sport.ROWING, SubSport.INDOOR_ROWING),     # Rowing Machine
        14: (Sport.TRAINING, SubSport.CARDIO_TRAINING), # Jump Rope (no FIT sub-sport)
        15: (Sport.HIKING, SubSport.GENERIC),           # Hiking
        16: (Sport.TRAINING, SubSport.CARDIO_TRAINING), # HIIT
        17: (Sport.MULTISPORT, SubSport.GENERIC),       # Triathlon
        18: (Sport.GENERIC, SubSport.GENERIC),          # Ball Sports
        19: (Sport.BASKETBALL, SubSport.GENERIC),       # Basketball
        20: (Sport.GOLF, SubSport.GENERIC),             # Golf
        21: (Sport.ALPINE_SKIING, SubSport.GENERIC),    # Skiing
        22: (Sport.GENERIC, SubSport.GENERIC),          # Outdoor Step Sports (no FIT equivalent)
        23: (Sport.GENERIC, SubSport.GENERIC),          # Outdoor No-Step Sports (no FIT equivalent)
        24: (Sport.ROCK_CLIMBING, SubSport.GENERIC),    # Rock Climbing
        25: (Sport.DIVING, SubSport.SINGLE_GAS_DIVING), # Diving
        28: (Sport.TRAINING, SubSport.STRENGTH_TRAINING),  # Strength Training
    }

    if proto_type is not None and proto_type in _PROTO_TYPE_MAP:
        return _PROTO_TYPE_MAP[proto_type]

    normalized = (category or "").lower()
    if "run" in normalized:
        return (Sport.RUNNING, SubSport.GENERIC)
    if "ride" in normalized or "bike" in normalized or "cycl" in normalized:
        return (Sport.CYCLING, SubSport.GENERIC)
    if "swim" in normalized:
        return (Sport.SWIMMING, SubSport.GENERIC)
    if "walk" in normalized:
        return (Sport.WALKING, SubSport.GENERIC)
    if "hike" in normalized:
        return (Sport.HIKING, SubSport.GENERIC)
    return (Sport.GENERIC, SubSport.GENERIC)


def _tcx_sport(category: str | None) -> str:
    normalized = (category or "").lower()
    if "run" in normalized:
        return "Running"
    if "ride" in normalized or "bike" in normalized or "cycl" in normalized:
        return "Biking"
    return "Other"


def _xml_bytes(root: ET.Element) -> bytes:
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _isoformat_utc(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _unix_millis(timestamp: int) -> int:
    return int(timestamp) * 1000


_FIT_EPOCH_OFFSET = 631065600


def _fit_local_timestamp(unix_epoch_seconds: int, zone_offset_seconds: int) -> int:
    return unix_epoch_seconds + zone_offset_seconds - _FIT_EPOCH_OFFSET


def _format_coordinate(value: float) -> str:
    return f"{value:.8f}"


def _format_decimal(value: float) -> str:
    return f"{value:.3f}".rstrip("0").rstrip(".")
