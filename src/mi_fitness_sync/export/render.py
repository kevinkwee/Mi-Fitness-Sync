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

_CYCLING_SPORT_TYPES = frozenset({6, 7, 206, 324})


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
    valid_points = [p for p in detail.track_points if _is_valid_coordinate(p.latitude, p.longitude)]
    if not valid_points:
        raise MiFitnessError("This activity detail payload does not contain GPS track points, so GPX export is not available.")

    _GPX = "http://www.topografix.com/GPX/1/1"
    ET.register_namespace("", _GPX)
    ET.register_namespace("gpxtpx", GPX_TRACKPOINT_NS)

    has_elevation = any(p.altitude_meters is not None for p in valid_points)
    creator = "Strava with barometer" if has_elevation else "Strava"

    root = ET.Element(
        f"{{{_GPX}}}gpx",
        {
            "version": "1.1",
            "creator": creator,
            "{%s}schemaLocation" % XML_SCHEMA_INSTANCE_NS: (
                f"{_GPX} {_GPX}/gpx.xsd "
                f"{GPX_TRACKPOINT_NS} http://www.garmin.com/xmlschemas/TrackPointExtensionv1.xsd"
            ),
        },
    )

    # gpxType sequence: metadata, wpt*, rte*, trk*, extensions
    metadata = ET.SubElement(root, f"{{{_GPX}}}metadata")
    # metadataType sequence: name, desc, author, copyright, link*, time, keywords, bounds, extensions
    ET.SubElement(metadata, f"{{{_GPX}}}name").text = detail.activity.title
    ET.SubElement(metadata, f"{{{_GPX}}}time").text = _isoformat_utc(detail.start_time)

    track = ET.SubElement(root, f"{{{_GPX}}}trk")
    # trkType sequence: name, cmt, desc, src, link*, number, type, extensions, trkseg*
    ET.SubElement(track, f"{{{_GPX}}}name").text = detail.activity.title
    ET.SubElement(track, f"{{{_GPX}}}type").text = _tcx_sport(detail.activity.sport_type)
    segment = ET.SubElement(track, f"{{{_GPX}}}trkseg")

    for point in valid_points:
        track_point = ET.SubElement(
            segment,
            f"{{{_GPX}}}trkpt",
            {"lat": _format_coordinate(point.latitude), "lon": _format_coordinate(point.longitude)},
        )
        # wptType sequence: ele, time, ..., extensions
        if point.altitude_meters is not None:
            ET.SubElement(track_point, f"{{{_GPX}}}ele").text = _format_decimal(point.altitude_meters)
        ET.SubElement(track_point, f"{{{_GPX}}}time").text = _isoformat_utc(point.timestamp)

        clamped_hr = _clamp_heart_rate(point.heart_rate) if point.heart_rate is not None else None
        cadence_rpm = _cadence_spm_to_rpm(point.cadence, detail.activity.sport_type) if point.cadence is not None else None
        clamped_cad = _clamp_cadence(cadence_rpm) if cadence_rpm is not None else None
        has_hr = clamped_hr is not None
        has_cad = cadence_rpm is not None
        if has_hr or has_cad:
            extensions = ET.SubElement(track_point, f"{{{_GPX}}}extensions")
            # TrackPointExtension_t sequence (v1): atemp, wtemp, depth, hr, cad, Extensions
            track_extension = ET.SubElement(extensions, f"{{{GPX_TRACKPOINT_NS}}}TrackPointExtension")
            if has_hr:
                ET.SubElement(track_extension, f"{{{GPX_TRACKPOINT_NS}}}hr").text = str(clamped_hr)
            if has_cad:
                ET.SubElement(track_extension, f"{{{GPX_TRACKPOINT_NS}}}cad").text = str(clamped_cad)

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
    sport = _tcx_sport(detail.activity.sport_type)
    activity = ET.SubElement(activities, f"{{{TCX_NS}}}Activity", {"Sport": sport})
    ET.SubElement(activity, f"{{{TCX_NS}}}Id").text = _isoformat_utc(detail.start_time)
    is_running = sport == "Running"

    # ActivityLap_t XSD sequence:
    # TotalTimeSeconds, DistanceMeters, MaximumSpeed, Calories,
    # AverageHeartRateBpm, MaximumHeartRateBpm, Intensity, Cadence,
    # TriggerMethod, Track, Notes, Extensions
    lap = ET.SubElement(activity, f"{{{TCX_NS}}}Lap", {"StartTime": _isoformat_utc(detail.start_time)})
    ET.SubElement(lap, f"{{{TCX_NS}}}TotalTimeSeconds").text = _format_decimal(float(detail.total_duration_seconds))
    ET.SubElement(lap, f"{{{TCX_NS}}}DistanceMeters").text = _format_decimal(detail.total_distance_meters)
    ET.SubElement(lap, f"{{{TCX_NS}}}Calories").text = str(_clamp_calories(detail.total_calories or 0))

    average_heart_rate = _average_heart_rate(export_points)
    if average_heart_rate is not None:
        clamped_avg_hr = _clamp_heart_rate(average_heart_rate)
        if clamped_avg_hr is not None:
            average_node = ET.SubElement(lap, f"{{{TCX_NS}}}AverageHeartRateBpm")
            ET.SubElement(average_node, f"{{{TCX_NS}}}Value").text = str(clamped_avg_hr)

    maximum_heart_rate = _maximum_heart_rate(export_points)
    if maximum_heart_rate is not None:
        clamped_max_hr = _clamp_heart_rate(maximum_heart_rate)
        if clamped_max_hr is not None:
            maximum_node = ET.SubElement(lap, f"{{{TCX_NS}}}MaximumHeartRateBpm")
            ET.SubElement(maximum_node, f"{{{TCX_NS}}}Value").text = str(clamped_max_hr)

    ET.SubElement(lap, f"{{{TCX_NS}}}Intensity").text = "Active"
    ET.SubElement(lap, f"{{{TCX_NS}}}TriggerMethod").text = "Manual"

    track = ET.SubElement(lap, f"{{{TCX_NS}}}Track")
    for point in export_points:
        # Trackpoint_t XSD sequence:
        # Time, Position, AltitudeMeters, DistanceMeters,
        # HeartRateBpm, Cadence, SensorState, Extensions
        track_point = ET.SubElement(track, f"{{{TCX_NS}}}Trackpoint")
        ET.SubElement(track_point, f"{{{TCX_NS}}}Time").text = _isoformat_utc(point.timestamp)

        if _is_valid_coordinate(point.latitude, point.longitude):
            position = ET.SubElement(track_point, f"{{{TCX_NS}}}Position")
            ET.SubElement(position, f"{{{TCX_NS}}}LatitudeDegrees").text = _format_coordinate(point.latitude)
            ET.SubElement(position, f"{{{TCX_NS}}}LongitudeDegrees").text = _format_coordinate(point.longitude)

        if point.altitude_meters is not None:
            ET.SubElement(track_point, f"{{{TCX_NS}}}AltitudeMeters").text = _format_decimal(point.altitude_meters)

        if point.distance_meters is not None:
            ET.SubElement(track_point, f"{{{TCX_NS}}}DistanceMeters").text = _format_decimal(point.distance_meters)

        clamped_hr = _clamp_heart_rate(point.heart_rate) if point.heart_rate is not None else None
        if clamped_hr is not None:
            heart_rate = ET.SubElement(track_point, f"{{{TCX_NS}}}HeartRateBpm")
            ET.SubElement(heart_rate, f"{{{TCX_NS}}}Value").text = str(clamped_hr)

        cadence_rpm = _cadence_spm_to_rpm(point.cadence, detail.activity.sport_type) if point.cadence is not None else None
        clamped_cad = _clamp_cadence(cadence_rpm) if cadence_rpm is not None else None
        has_speed = point.speed_mps is not None
        has_run_cad = is_running and clamped_cad is not None
        has_bike_cad = not is_running and clamped_cad is not None

        if has_bike_cad:
            ET.SubElement(track_point, f"{{{TCX_NS}}}Cadence").text = str(clamped_cad)

        if has_speed or has_run_cad:
            extensions = ET.SubElement(track_point, f"{{{TCX_NS}}}Extensions")
            tpx = ET.SubElement(extensions, f"{{{TCX_ACTIVITY_NS}}}TPX")
            if has_speed:
                ET.SubElement(tpx, f"{{{TCX_ACTIVITY_NS}}}Speed").text = _format_decimal(point.speed_mps)
            if has_run_cad:
                ET.SubElement(tpx, f"{{{TCX_ACTIVITY_NS}}}RunCadence").text = str(clamped_cad)

    # Activity_t XSD sequence: Id, Lap+, Notes?, Training?, Creator?, Extensions?
    has_elevation = any(p.altitude_meters is not None for p in export_points)
    _tcx_creator(activity, has_elevation)

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
    fit_sport, fit_sub_sport = _fit_sport_mapping(detail.activity.sport_type, proto_type)
    start_timestamp_ms = _unix_millis(detail.start_time)
    end_timestamp_ms = _unix_millis(detail.end_time)

    file_id = FileIdMessage()
    file_id.type = FileType.ACTIVITY
    file_id.manufacturer = Manufacturer.STRAVA
    file_id.product = 102
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
            record.cadence = _cadence_spm_to_rpm(point.cadence, detail.activity.sport_type)
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
    avg_cadence = _avg_cadence(report, export_points, detail.activity.sport_type)
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


def _avg_cadence(report: SportReport | None, points: list[TrackPoint], sport_type: int | None) -> int | None:
    if report is not None and report.avg_cadence is not None:
        raw = report.avg_cadence
    else:
        values = [p.cadence for p in points if p.cadence is not None]
        if not values:
            return None
        raw = round(sum(values) / len(values))
    return _cadence_spm_to_rpm(raw, sport_type)


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


def _fit_sport_mapping(sport_type: int | None, proto_type: int | None = None) -> tuple:
    from fit_tool.profile.profile_type import Sport, SubSport

    _SPORT_TYPE_MAP: dict[int, tuple] = {
        # Core sport types (1-21)
        1: (Sport.RUNNING, SubSport.STREET),                     # Outdoor Running
        2: (Sport.WALKING, SubSport.CASUAL_WALKING),             # Outdoor Walking
        3: (Sport.RUNNING, SubSport.TREADMILL),                  # Indoor Running
        4: (Sport.HIKING, SubSport.GENERIC),                     # Mountaineering
        5: (Sport.RUNNING, SubSport.TRAIL),                      # Trail Running
        6: (Sport.CYCLING, SubSport.ROAD),                       # Outdoor Cycling
        7: (Sport.CYCLING, SubSport.INDOOR_CYCLING),             # Indoor Cycling
        8: (Sport.TRAINING, SubSport.GENERIC),                   # Free Training
        9: (Sport.SWIMMING, SubSport.LAP_SWIMMING),              # Pool Swimming
        10: (Sport.SWIMMING, SubSport.OPEN_WATER),               # Open Water Swimming
        11: (Sport.FITNESS_EQUIPMENT, SubSport.ELLIPTICAL),      # Elliptical
        12: (Sport.TRAINING, SubSport.YOGA),                     # Yoga
        13: (Sport.ROWING, SubSport.INDOOR_ROWING),              # Rowing Machine
        14: (Sport.TRAINING, SubSport.CARDIO_TRAINING),          # Jump Rope
        15: (Sport.HIKING, SubSport.GENERIC),                    # Hiking
        16: (Sport.TRAINING, SubSport.CARDIO_TRAINING),          # HIIT
        17: (Sport.MULTISPORT, SubSport.GENERIC),                # Triathlon
        19: (Sport.BASKETBALL, SubSport.GENERIC),                # Basketball
        20: (Sport.GOLF, SubSport.GENERIC),                      # Golf
        21: (Sport.ALPINE_SKIING, SubSport.GENERIC),             # Skiing
        # Water sports (100-118)
        100: (Sport.SAILING, SubSport.GENERIC),                  # Sailing
        101: (Sport.STAND_UP_PADDLEBOARDING, SubSport.GENERIC),  # Paddle Board
        104: (Sport.WATER_SKIING, SubSport.GENERIC),             # Water Skiing
        105: (Sport.KAYAKING, SubSport.GENERIC),                 # Kayaking
        106: (Sport.RAFTING, SubSport.GENERIC),                  # Kayak Rafting
        107: (Sport.BOATING, SubSport.GENERIC),                  # Boating
        108: (Sport.BOATING, SubSport.GENERIC),                  # Motor Boat
        109: (Sport.SWIMMING, SubSport.GENERIC),                 # Fin Swimming
        110: (Sport.DIVING, SubSport.SINGLE_GAS_DIVING),         # Diving
        111: (Sport.SWIMMING, SubSport.GENERIC),                 # Artistic Swimming
        112: (Sport.DIVING, SubSport.APNEA_DIVING),              # Snorkeling
        113: (Sport.KITESURFING, SubSport.GENERIC),              # Kite Surfing
        114: (Sport.SURFING, SubSport.GENERIC),                  # Indoor Surfing
        115: (Sport.ROWING, SubSport.GENERIC),                   # Dragon Boats
        116: (Sport.DIVING, SubSport.APNEA_DIVING),              # Freediving
        117: (Sport.DIVING, SubSport.SINGLE_GAS_DIVING),         # Recreational Scuba
        118: (Sport.DIVING, SubSport.MULTI_GAS_DIVING),          # Instrument Diving
        # Adventure & outdoor (200-207)
        200: (Sport.ROCK_CLIMBING, SubSport.GENERIC),            # Rock Climbing
        202: (Sport.INLINE_SKATING, SubSport.GENERIC),           # Roller Skating
        205: (Sport.HANG_GLIDING, SubSport.GENERIC),             # Paragliding
        206: (Sport.CYCLING, SubSport.BMX),                      # BMX
        207: (Sport.WALKING, SubSport.SPEED_WALKING),            # Nordic Walking
        # Indoor fitness (300-334)
        301: (Sport.FITNESS_EQUIPMENT, SubSport.STAIR_CLIMBING), # Stair Climbing
        302: (Sport.FITNESS_EQUIPMENT, SubSport.STAIR_CLIMBING), # Stepper
        303: (Sport.TRAINING, SubSport.STRENGTH_TRAINING),       # Core Training
        304: (Sport.TRAINING, SubSport.FLEXIBILITY_TRAINING),    # Flexibility Training
        305: (Sport.TRAINING, SubSport.PILATES),                 # Pilates
        307: (Sport.TRAINING, SubSport.FLEXIBILITY_TRAINING),    # Stretching
        308: (Sport.TRAINING, SubSport.STRENGTH_TRAINING),       # Strength Training
        310: (Sport.TRAINING, SubSport.CARDIO_TRAINING),         # Aerobics
        313: (Sport.TRAINING, SubSport.STRENGTH_TRAINING),       # Dumbbell Training
        314: (Sport.TRAINING, SubSport.STRENGTH_TRAINING),       # Barbell Training
        315: (Sport.TRAINING, SubSport.STRENGTH_TRAINING),       # Weight Lifting
        316: (Sport.TRAINING, SubSport.STRENGTH_TRAINING),       # Deadlift
        317: (Sport.TRAINING, SubSport.CARDIO_TRAINING),         # Burpee
        318: (Sport.TRAINING, SubSport.STRENGTH_TRAINING),       # Sit-Ups
        320: (Sport.TRAINING, SubSport.STRENGTH_TRAINING),       # Upper Limb Training
        321: (Sport.TRAINING, SubSport.STRENGTH_TRAINING),       # Lower Limb Training
        322: (Sport.TRAINING, SubSport.STRENGTH_TRAINING),       # Waist & Abdomen
        323: (Sport.TRAINING, SubSport.STRENGTH_TRAINING),       # Back Training
        324: (Sport.CYCLING, SubSport.SPIN),                     # Spinning
        330: (Sport.BOXING, SubSport.GENERIC),                   # Kickboxing
        331: (Sport.TRAINING, SubSport.CARDIO_TRAINING),         # Battle Rope
        332: (Sport.TRAINING, SubSport.CARDIO_TRAINING),         # Mixed Aerobic
        333: (Sport.WALKING, SubSport.INDOOR_WALKING),           # Indoor Walking
        334: (Sport.TRAINING, SubSport.STRENGTH_TRAINING),       # Ab Wheel
        # Martial arts (500-511)
        500: (Sport.BOXING, SubSport.GENERIC),                   # Boxing
        # Ball sports (600-627)
        600: (Sport.SOCCER, SubSport.GENERIC),                   # Football
        601: (Sport.BASKETBALL, SubSport.GENERIC),               # Basketball (Alt)
        609: (Sport.TENNIS, SubSport.GENERIC),                   # Tennis
        # Snow & ice (700-710)
        700: (Sport.ICE_SKATING, SubSport.GENERIC),              # Outdoor Skating
        703: (Sport.SNOWMOBILING, SubSport.GENERIC),             # Snowmobile
        707: (Sport.ICE_SKATING, SubSport.GENERIC),              # Indoor Skating
        708: (Sport.SNOWBOARDING, SubSport.GENERIC),             # Snowboarding
        709: (Sport.CROSS_COUNTRY_SKIING, SubSport.GENERIC),     # Skiing (General)
        710: (Sport.CROSS_COUNTRY_SKIING, SubSport.GENERIC),     # Cross-Country Skiing
        # Misc (800-812)
        802: (Sport.HORSEBACK_RIDING, SubSport.GENERIC),         # Horse Riding
        806: (Sport.FISHING, SubSport.GENERIC),                  # Fishing
        # Climbing (1000-1001)
        1000: (Sport.ROCK_CLIMBING, SubSport.GENERIC),           # Indoor Rock Climbing
        1001: (Sport.ROCK_CLIMBING, SubSport.GENERIC),           # Outdoor Rock Climbing
        # Special (10000+)
        10000: (Sport.HORSEBACK_RIDING, SubSport.GENERIC),       # Equestrian
    }

    _PROTO_TYPE_MAP: dict[int, tuple] = {
        1: (Sport.RUNNING, SubSport.STREET),
        2: (Sport.RUNNING, SubSport.TRACK),
        3: (Sport.RUNNING, SubSport.TREADMILL),
        4: (Sport.WALKING, SubSport.CASUAL_WALKING),
        5: (Sport.RUNNING, SubSport.TRAIL),
        6: (Sport.CYCLING, SubSport.ROAD),
        7: (Sport.CYCLING, SubSport.INDOOR_CYCLING),
        8: (Sport.TRAINING, SubSport.GENERIC),
        9: (Sport.SWIMMING, SubSport.LAP_SWIMMING),
        10: (Sport.SWIMMING, SubSport.OPEN_WATER),
        11: (Sport.FITNESS_EQUIPMENT, SubSport.ELLIPTICAL),
        12: (Sport.TRAINING, SubSport.YOGA),
        13: (Sport.ROWING, SubSport.INDOOR_ROWING),
        14: (Sport.TRAINING, SubSport.CARDIO_TRAINING),
        15: (Sport.HIKING, SubSport.GENERIC),
        16: (Sport.TRAINING, SubSport.CARDIO_TRAINING),
        17: (Sport.MULTISPORT, SubSport.GENERIC),
        18: (Sport.GENERIC, SubSport.GENERIC),
        19: (Sport.BASKETBALL, SubSport.GENERIC),
        20: (Sport.GOLF, SubSport.GENERIC),
        21: (Sport.ALPINE_SKIING, SubSport.GENERIC),
        22: (Sport.GENERIC, SubSport.GENERIC),
        23: (Sport.GENERIC, SubSport.GENERIC),
        24: (Sport.ROCK_CLIMBING, SubSport.GENERIC),
        25: (Sport.DIVING, SubSport.SINGLE_GAS_DIVING),
    }

    if sport_type is not None and sport_type in _SPORT_TYPE_MAP:
        return _SPORT_TYPE_MAP[sport_type]

    if proto_type is not None and proto_type in _PROTO_TYPE_MAP:
        return _PROTO_TYPE_MAP[proto_type]

    return (Sport.GENERIC, SubSport.GENERIC)


def _tcx_sport(sport_type: int | None) -> str:
    if sport_type in {1, 3, 5}:
        return "Running"
    if sport_type in {6, 7, 206, 324}:
        return "Biking"
    return "Other"


def _tcx_creator(activity_element: ET.Element, has_elevation: bool) -> None:
    """Append a Creator element to the Activity. Includes barometer hint when altitude data is present."""
    creator_name = "Strava with barometer" if has_elevation else "Strava"
    creator = ET.SubElement(
        activity_element,
        f"{{{TCX_NS}}}Creator",
        {f"{{{XML_SCHEMA_INSTANCE_NS}}}type": "Device_t"},
    )
    ET.SubElement(creator, f"{{{TCX_NS}}}Name").text = creator_name
    ET.SubElement(creator, f"{{{TCX_NS}}}UnitId").text = "0"
    ET.SubElement(creator, f"{{{TCX_NS}}}ProductID").text = "102"
    version = ET.SubElement(creator, f"{{{TCX_NS}}}Version")
    ET.SubElement(version, f"{{{TCX_NS}}}VersionMajor").text = "0"
    ET.SubElement(version, f"{{{TCX_NS}}}VersionMinor").text = "0"


def _clamp_calories(value: int) -> int:
    """Clamp to xsd:unsignedShort [0, 65535]."""
    return max(0, min(65535, value))


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


def _is_valid_coordinate(lat: float | None, lon: float | None) -> bool:
    if lat is None or lon is None:
        return False
    return -90.0 <= lat <= 90.0 and -180.0 <= lon < 180.0


def _cadence_spm_to_rpm(spm: int, sport_type: int | None) -> int:
    """Convert Mi Fitness cadence (SPM) to RPM for export formats.

    Cycling sports already report cadence in RPM from the sensor.
    Step-based sports (running, walking, etc.) report SPM which is 2x RPM.
    """
    if sport_type is not None and sport_type in _CYCLING_SPORT_TYPES:
        return spm
    return spm // 2


def _clamp_heart_rate(value: int) -> int | None:
    """Clamp to Garmin BeatsPerMinute_t [1, 255]. Returns None if out of meaningful range."""
    clamped = max(0, min(255, value))
    return clamped if clamped >= 1 else None


def _clamp_cadence(value: int) -> int:
    """Clamp to Garmin RevolutionsPerMinute_t [0, 254]."""
    return max(0, min(254, value))


def _format_coordinate(value: float) -> str:
    return f"{value:.8f}"


def _format_decimal(value: float) -> str:
    return f"{value:.3f}".rstrip("0").rstrip(".")
