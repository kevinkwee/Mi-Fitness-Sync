from __future__ import annotations

import gzip
from dataclasses import dataclass
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

from mi_fitness_sync.activities import ActivityDetail, TrackPoint
from mi_fitness_sync.exceptions import MiFitnessError


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
    fit_sport = _fit_sport(detail.activity.category)
    start_timestamp_ms = _unix_millis(detail.start_time)
    end_timestamp_ms = _unix_millis(detail.end_time)

    file_id = FileIdMessage()
    file_id.type = FileType.ACTIVITY
    file_id.manufacturer = Manufacturer.GARMIN
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
    builder.add(lap)

    session = SessionMessage()
    session.timestamp = end_timestamp_ms
    session.start_time = start_timestamp_ms
    session.total_elapsed_time = float(detail.total_duration_seconds)
    session.total_timer_time = float(detail.total_duration_seconds)
    session.total_distance = detail.total_distance_meters
    session.total_calories = detail.total_calories or 0
    session.sport = fit_sport
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


def _fit_sport(category: str | None):
    from fit_tool.profile.profile_type import Sport

    normalized = (category or "").lower()
    if "run" in normalized:
        return Sport.RUNNING
    if "ride" in normalized or "bike" in normalized or "cycle" in normalized:
        return Sport.CYCLING
    if "swim" in normalized:
        return Sport.SWIMMING
    if "walk" in normalized:
        return Sport.WALKING
    if "hike" in normalized:
        return Sport.HIKING
    return Sport.GENERIC


def _tcx_sport(category: str | None) -> str:
    normalized = (category or "").lower()
    if "run" in normalized:
        return "Running"
    if "ride" in normalized or "bike" in normalized or "cycle" in normalized:
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


def _format_coordinate(value: float) -> str:
    return f"{value:.8f}"


def _format_decimal(value: float) -> str:
    return f"{value:.3f}".rstrip("0").rstrip(".")
