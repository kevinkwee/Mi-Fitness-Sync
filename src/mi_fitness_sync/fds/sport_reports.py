from __future__ import annotations

import logging
import struct
from dataclasses import dataclass, field
from typing import Any

import requests

from mi_fitness_sync.fds.cache import FdsCache
from mi_fitness_sync.fds.common import SPORT_SERVER_DATA_ID_LEN, parse_fds_header
from mi_fitness_sync.fds.downloader import download_and_parse_fds_file


logger = logging.getLogger(__name__)

REPORT_START_TIME = 1
REPORT_END_TIME = 2
REPORT_DURATION = 3
REPORT_VALID_DURATION = 4
REPORT_DISTANCE = 5
REPORT_CALORIES = 6
REPORT_TOTAL_CAL = 7
REPORT_MAX_PACE = 8
REPORT_MIN_PACE = 9
REPORT_AVG_PACE = 10
REPORT_AVG_SPEED = 11
REPORT_MAX_SPEED = 12
REPORT_STEPS = 13
REPORT_MAX_CADENCE = 14
REPORT_AVG_CADENCE = 15
REPORT_AVG_HR = 16
REPORT_MAX_HR = 17
REPORT_MIN_HR = 18
REPORT_RISE_HEIGHT = 19
REPORT_FALL_HEIGHT = 20
REPORT_AVG_HEIGHT = 21
REPORT_MAX_HEIGHT = 22
REPORT_MIN_HEIGHT = 23
REPORT_TOTAL_CLIMBING = 24
REPORT_TRAIN_EFFECT = 25
REPORT_ANAEROBIC_TE = 26
REPORT_VO2MAX = 27
REPORT_ENERGY_CONSUME = 28
REPORT_RECOVERY_TIME = 29
REPORT_HR_EXTREME_DUR = 30
REPORT_HR_ANAEROBIC_DUR = 31
REPORT_HR_AEROBIC_DUR = 32
REPORT_HR_FAT_BURNING_DUR = 33
REPORT_HR_WARMUP_DUR = 34


@dataclass(slots=True, frozen=True)
class ReportFieldDef:
    type_id: int
    byte_count: int
    support_version: int
    is_float: bool = False
    depends_on: tuple[int, frozenset[int]] | None = None


@dataclass(slots=True)
class SportReport:
    start_time: int | None = None
    end_time: int | None = None
    duration: int | None = None
    valid_duration: int | None = None
    distance: int | None = None
    calories: int | None = None
    total_calories: int | None = None
    max_pace: int | None = None
    min_pace: int | None = None
    avg_pace: int | None = None
    avg_speed: float | None = None
    max_speed: float | None = None
    steps: int | None = None
    avg_hr: int | None = None
    max_hr: int | None = None
    min_hr: int | None = None
    avg_cadence: int | None = None
    max_cadence: int | None = None
    rise_height: float | None = None
    fall_height: float | None = None
    train_effect: float | None = None
    anaerobic_train_effect: float | None = None
    vo2max: int | None = None
    recovery_time: int | None = None
    hr_extreme_duration: int | None = None
    hr_anaerobic_duration: int | None = None
    hr_aerobic_duration: int | None = None
    hr_fat_burning_duration: int | None = None
    hr_warmup_duration: int | None = None
    raw_values: dict[int, int | float] = field(default_factory=dict)


_REPORT_FIELD_ATTR: dict[int, str] = {REPORT_START_TIME: "start_time", REPORT_END_TIME: "end_time", REPORT_DURATION: "duration", REPORT_VALID_DURATION: "valid_duration", REPORT_DISTANCE: "distance", REPORT_CALORIES: "calories", REPORT_TOTAL_CAL: "total_calories", REPORT_MAX_PACE: "max_pace", REPORT_MIN_PACE: "min_pace", REPORT_AVG_PACE: "avg_pace", REPORT_AVG_SPEED: "avg_speed", REPORT_MAX_SPEED: "max_speed", REPORT_STEPS: "steps", REPORT_AVG_CADENCE: "avg_cadence", REPORT_MAX_CADENCE: "max_cadence", REPORT_AVG_HR: "avg_hr", REPORT_MAX_HR: "max_hr", REPORT_MIN_HR: "min_hr", REPORT_RISE_HEIGHT: "rise_height", REPORT_FALL_HEIGHT: "fall_height", REPORT_TRAIN_EFFECT: "train_effect", REPORT_ANAEROBIC_TE: "anaerobic_train_effect", REPORT_VO2MAX: "vo2max", REPORT_RECOVERY_TIME: "recovery_time", REPORT_HR_EXTREME_DUR: "hr_extreme_duration", REPORT_HR_ANAEROBIC_DUR: "hr_anaerobic_duration", REPORT_HR_AEROBIC_DUR: "hr_aerobic_duration", REPORT_HR_FAT_BURNING_DUR: "hr_fat_burning_duration", REPORT_HR_WARMUP_DUR: "hr_warmup_duration"}

_REPORT_COURSE_ID_DEP = (90, frozenset({251, 252, 253, 255}))
FREE_TRAINING_REPORT_FIELDS = [ReportFieldDef(1, 4, 1), ReportFieldDef(2, 4, 1), ReportFieldDef(3, 4, 1), ReportFieldDef(6, 2, 1), ReportFieldDef(16, 1, 1), ReportFieldDef(17, 1, 1), ReportFieldDef(18, 1, 1), ReportFieldDef(101, 1, 6), ReportFieldDef(102, 1, 6), ReportFieldDef(103, 1, 6), ReportFieldDef(104, 1, 6), ReportFieldDef(105, 1, 6), ReportFieldDef(106, 1, 6), ReportFieldDef(25, 4, 1, True), ReportFieldDef(107, 1, 7), ReportFieldDef(28, 1, 1), ReportFieldDef(29, 2, 1), ReportFieldDef(30, 4, 1), ReportFieldDef(31, 4, 1), ReportFieldDef(32, 4, 1), ReportFieldDef(33, 4, 1), ReportFieldDef(34, 4, 1), ReportFieldDef(158, 1, 11), ReportFieldDef(159, 1, 11), ReportFieldDef(160, 1, 11), ReportFieldDef(161, 1, 11), ReportFieldDef(162, 1, 11), ReportFieldDef(7, 2, 2), ReportFieldDef(4, 4, 3), ReportFieldDef(26, 4, 3, True), ReportFieldDef(108, 1, 7), ReportFieldDef(163, 4, 11, True), ReportFieldDef(164, 1, 11), ReportFieldDef(65, 2, 4), ReportFieldDef(90, 1, 5), ReportFieldDef(-1, 8, 5, depends_on=_REPORT_COURSE_ID_DEP), ReportFieldDef(91, 1, 5), ReportFieldDef(92, 4, 5), ReportFieldDef(93, 2, 5), ReportFieldDef(110, 2, 7), ReportFieldDef(111, 1, 7), ReportFieldDef(117, 1, 8), ReportFieldDef(118, 2, 8), ReportFieldDef(119, 2, 8), ReportFieldDef(120, 2, 8), ReportFieldDef(121, 2, 8), ReportFieldDef(122, 4, 8, True), ReportFieldDef(123, 2, 8), ReportFieldDef(124, 1, 8), ReportFieldDef(149, 2, 9), ReportFieldDef(150, 2, 9), ReportFieldDef(151, 2, 9), ReportFieldDef(152, 1, 10), ReportFieldDef(165, 2, 11), ReportFieldDef(206, 8, 12), ReportFieldDef(207, 4, 12), ReportFieldDef(220, 4, 13), ReportFieldDef(221, 4, 13), ReportFieldDef(222, 2, 13), ReportFieldDef(223, 2, 14), ReportFieldDef(224, 2, 14), ReportFieldDef(225, 2, 14)]
OUTDOOR_SPORT_REPORT_FIELDS = [ReportFieldDef(1, 4, 1), ReportFieldDef(2, 4, 1), ReportFieldDef(3, 4, 1), ReportFieldDef(5, 4, 1), ReportFieldDef(6, 2, 1), ReportFieldDef(8, 4, 1), ReportFieldDef(9, 4, 1), ReportFieldDef(12, 4, 1, True), ReportFieldDef(13, 4, 1), ReportFieldDef(14, 2, 1), ReportFieldDef(16, 1, 1), ReportFieldDef(17, 1, 1), ReportFieldDef(18, 1, 1), ReportFieldDef(19, 4, 1, True), ReportFieldDef(20, 4, 1, True), ReportFieldDef(21, 4, 1, True), ReportFieldDef(22, 4, 1, True), ReportFieldDef(23, 4, 1, True), ReportFieldDef(25, 4, 1, True), ReportFieldDef(27, 1, 1), ReportFieldDef(28, 1, 1), ReportFieldDef(29, 2, 1), ReportFieldDef(30, 4, 1), ReportFieldDef(31, 4, 1), ReportFieldDef(32, 4, 1), ReportFieldDef(33, 4, 1), ReportFieldDef(34, 4, 1), ReportFieldDef(7, 2, 2), ReportFieldDef(4, 4, 3), ReportFieldDef(26, 4, 3, True), ReportFieldDef(90, 1, 4), ReportFieldDef(-1, 8, 4, depends_on=_REPORT_COURSE_ID_DEP), ReportFieldDef(91, 1, 4), ReportFieldDef(92, 4, 4), ReportFieldDef(93, 2, 4), ReportFieldDef(94, 4, 4), ReportFieldDef(95, 4, 4), ReportFieldDef(96, 2, 4)]
_INDOOR_RUN_REPORT_FIELDS = [ReportFieldDef(1, 4, 1), ReportFieldDef(2, 4, 1), ReportFieldDef(3, 4, 1), ReportFieldDef(5, 4, 1), ReportFieldDef(6, 2, 1), ReportFieldDef(10, 4, 10), ReportFieldDef(8, 4, 1), ReportFieldDef(9, 4, 1), ReportFieldDef(13, 4, 1), ReportFieldDef(148, 2, 10), ReportFieldDef(15, 2, 10), ReportFieldDef(14, 2, 1), ReportFieldDef(16, 1, 1), ReportFieldDef(17, 1, 1), ReportFieldDef(18, 1, 1), ReportFieldDef(25, 4, 1, True), ReportFieldDef(107, 1, 7), ReportFieldDef(27, 1, 1), ReportFieldDef(109, 1, 7), ReportFieldDef(28, 1, 1), ReportFieldDef(29, 2, 1), ReportFieldDef(30, 4, 1), ReportFieldDef(31, 4, 1), ReportFieldDef(32, 4, 1), ReportFieldDef(33, 4, 1), ReportFieldDef(34, 4, 1), ReportFieldDef(158, 1, 12), ReportFieldDef(159, 1, 12), ReportFieldDef(160, 1, 12), ReportFieldDef(161, 1, 12), ReportFieldDef(162, 1, 12), ReportFieldDef(166, 4, 12), ReportFieldDef(167, 4, 12), ReportFieldDef(168, 4, 12), ReportFieldDef(169, 4, 12), ReportFieldDef(170, 4, 12), ReportFieldDef(7, 2, 2), ReportFieldDef(4, 4, 3), ReportFieldDef(26, 4, 3, True), ReportFieldDef(108, 1, 7), ReportFieldDef(163, 4, 12, True), ReportFieldDef(164, 1, 12), ReportFieldDef(65, 2, 6), ReportFieldDef(90, 1, 4), ReportFieldDef(-1, 8, 4, depends_on=_REPORT_COURSE_ID_DEP), ReportFieldDef(91, 1, 4), ReportFieldDef(92, 4, 4), ReportFieldDef(93, 2, 4), ReportFieldDef(94, 4, 4), ReportFieldDef(95, 4, 4), ReportFieldDef(99, 4, 6, True), ReportFieldDef(96, 2, 4), ReportFieldDef(100, 4, 5), ReportFieldDef(110, 2, 7), ReportFieldDef(111, 1, 7), ReportFieldDef(112, 4, 7, True), ReportFieldDef(113, 1, 7), ReportFieldDef(114, 1, 8), ReportFieldDef(153, 2, 11), ReportFieldDef(154, 2, 11), ReportFieldDef(156, 2, 11), ReportFieldDef(155, 2, 11), ReportFieldDef(117, 1, 9), ReportFieldDef(125, 4, 9), ReportFieldDef(126, 4, 9), ReportFieldDef(127, 4, 9), ReportFieldDef(128, 1, 9), ReportFieldDef(137, 1, 10), ReportFieldDef(138, 1, 10), ReportFieldDef(139, 1, 10), ReportFieldDef(140, 1, 10), ReportFieldDef(141, 1, 10), ReportFieldDef(142, 2, 10), ReportFieldDef(143, 2, 10), ReportFieldDef(144, 2, 10), ReportFieldDef(145, 2, 10), ReportFieldDef(146, 1, 10), ReportFieldDef(147, 1, 10), ReportFieldDef(199, 2, 13), ReportFieldDef(200, 2, 13), ReportFieldDef(201, 2, 13), ReportFieldDef(202, 2, 13), ReportFieldDef(203, 2, 13), ReportFieldDef(204, 2, 13), ReportFieldDef(205, 2, 13), ReportFieldDef(152, 1, 11), ReportFieldDef(165, 2, 12), ReportFieldDef(173, 2, 12), ReportFieldDef(174, 2, 12), ReportFieldDef(175, 4, 12), ReportFieldDef(176, 4, 12), ReportFieldDef(206, 8, 13), ReportFieldDef(207, 4, 13)]
_OUTDOOR_BIKING_REPORT_FIELDS = [ReportFieldDef(1, 4, 1), ReportFieldDef(2, 4, 1), ReportFieldDef(3, 4, 1), ReportFieldDef(5, 4, 1), ReportFieldDef(6, 2, 1), ReportFieldDef(8, 4, 1), ReportFieldDef(9, 4, 1), ReportFieldDef(12, 4, 1, True), ReportFieldDef(16, 1, 1), ReportFieldDef(17, 1, 1), ReportFieldDef(18, 1, 1), ReportFieldDef(19, 4, 1, True), ReportFieldDef(20, 4, 1, True), ReportFieldDef(21, 4, 1, True), ReportFieldDef(22, 4, 1, True), ReportFieldDef(23, 4, 1, True), ReportFieldDef(25, 4, 1, True), ReportFieldDef(27, 1, 1), ReportFieldDef(28, 1, 1), ReportFieldDef(29, 2, 1), ReportFieldDef(30, 4, 1), ReportFieldDef(31, 4, 1), ReportFieldDef(32, 4, 1), ReportFieldDef(33, 4, 1), ReportFieldDef(34, 4, 1), ReportFieldDef(7, 2, 2), ReportFieldDef(4, 4, 3), ReportFieldDef(26, 4, 3, True), ReportFieldDef(90, 1, 4), ReportFieldDef(-1, 8, 4, depends_on=_REPORT_COURSE_ID_DEP), ReportFieldDef(91, 1, 4), ReportFieldDef(92, 4, 4), ReportFieldDef(93, 2, 4), ReportFieldDef(94, 4, 4), ReportFieldDef(95, 4, 4)]
_SWIMMING_REPORT_FIELDS = [ReportFieldDef(1, 4, 1), ReportFieldDef(2, 4, 1), ReportFieldDef(3, 4, 1), ReportFieldDef(5, 4, 1), ReportFieldDef(6, 2, 1), ReportFieldDef(10, 4, 7), ReportFieldDef(8, 4, 1), ReportFieldDef(9, 4, 1), ReportFieldDef(28, 1, 1), ReportFieldDef(29, 2, 1), ReportFieldDef(35, 2, 1), ReportFieldDef(36, 1, 1), ReportFieldDef(157, 1, 7), ReportFieldDef(38, 1, 1), ReportFieldDef(39, 2, 1), ReportFieldDef(40, 2, 1), ReportFieldDef(41, 2, 1), ReportFieldDef(42, 1, 1), ReportFieldDef(7, 2, 2), ReportFieldDef(4, 4, 3), ReportFieldDef(92, 4, 4), ReportFieldDef(93, 2, 4), ReportFieldDef(94, 4, 4), ReportFieldDef(95, 4, 4), ReportFieldDef(97, 2, 4), ReportFieldDef(25, 4, 5, True), ReportFieldDef(107, 1, 5), ReportFieldDef(26, 4, 5, True), ReportFieldDef(108, 1, 5), ReportFieldDef(110, 2, 5), ReportFieldDef(111, 1, 5), ReportFieldDef(117, 1, 6), ReportFieldDef(152, 1, 7), ReportFieldDef(208, 2, 8), ReportFieldDef(209, 1, 8), ReportFieldDef(210, 1, 8), ReportFieldDef(211, 1, 8), ReportFieldDef(212, 1, 8), ReportFieldDef(213, 2, 8), ReportFieldDef(214, 2, 8), ReportFieldDef(215, 1, 8), ReportFieldDef(216, 1, 8), ReportFieldDef(217, 2, 8), ReportFieldDef(218, 2, 8), ReportFieldDef(219, 1, 8), ReportFieldDef(198, 1, 8), ReportFieldDef(16, 1, 8), ReportFieldDef(17, 1, 8), ReportFieldDef(18, 1, 8), ReportFieldDef(91, 1, 8), ReportFieldDef(30, 4, 8), ReportFieldDef(31, 4, 8), ReportFieldDef(32, 4, 8), ReportFieldDef(33, 4, 8), ReportFieldDef(34, 4, 8), ReportFieldDef(158, 1, 8), ReportFieldDef(159, 1, 8), ReportFieldDef(160, 1, 8), ReportFieldDef(161, 1, 8), ReportFieldDef(162, 1, 8)]
_HIKING_REPORT_FIELDS = [ReportFieldDef(1, 4, 1), ReportFieldDef(2, 4, 1), ReportFieldDef(3, 4, 1), ReportFieldDef(5, 4, 1), ReportFieldDef(6, 2, 1), ReportFieldDef(8, 4, 1), ReportFieldDef(9, 4, 1), ReportFieldDef(12, 4, 1, True), ReportFieldDef(13, 4, 1), ReportFieldDef(14, 2, 1), ReportFieldDef(16, 1, 1), ReportFieldDef(17, 1, 1), ReportFieldDef(18, 1, 1), ReportFieldDef(19, 4, 1, True), ReportFieldDef(20, 4, 1, True), ReportFieldDef(21, 4, 1, True), ReportFieldDef(22, 4, 1, True), ReportFieldDef(23, 4, 1, True), ReportFieldDef(25, 4, 1, True), ReportFieldDef(27, 1, 1), ReportFieldDef(28, 1, 1), ReportFieldDef(29, 2, 1), ReportFieldDef(30, 4, 1), ReportFieldDef(31, 4, 1), ReportFieldDef(32, 4, 1), ReportFieldDef(33, 4, 1), ReportFieldDef(34, 4, 1), ReportFieldDef(7, 2, 1), ReportFieldDef(4, 4, 2), ReportFieldDef(26, 4, 2, True), ReportFieldDef(90, 1, 3), ReportFieldDef(-1, 8, 3, depends_on=_REPORT_COURSE_ID_DEP), ReportFieldDef(91, 1, 3), ReportFieldDef(92, 4, 3), ReportFieldDef(93, 2, 3), ReportFieldDef(94, 4, 3), ReportFieldDef(95, 4, 3), ReportFieldDef(96, 2, 3)]

_SPORT_REPORT_FIELDS: dict[int, list[ReportFieldDef]] = {1: OUTDOOR_SPORT_REPORT_FIELDS, 2: OUTDOOR_SPORT_REPORT_FIELDS, 3: _INDOOR_RUN_REPORT_FIELDS, 4: OUTDOOR_SPORT_REPORT_FIELDS, 5: OUTDOOR_SPORT_REPORT_FIELDS, 6: _OUTDOOR_BIKING_REPORT_FIELDS, 8: FREE_TRAINING_REPORT_FIELDS, 9: _SWIMMING_REPORT_FIELDS, 10: _SWIMMING_REPORT_FIELDS, 12: FREE_TRAINING_REPORT_FIELDS, 15: _HIKING_REPORT_FIELDS, 16: FREE_TRAINING_REPORT_FIELDS, 28: FREE_TRAINING_REPORT_FIELDS}


def compute_report_validity_len(fields: list[ReportFieldDef], version: int) -> int:
    return (sum(1 for field in fields if field.type_id >= 0 and field.support_version <= version) + 7) // 8


def get_report_data_valid_len(sport_type: int, version: int) -> int | None:
    fields = _SPORT_REPORT_FIELDS.get(sport_type)
    if fields is None:
        return None
    return compute_report_validity_len(fields, version)


def _read_report_value(data: memoryview | bytes, offset: int, byte_count: int, is_float: bool) -> tuple[int | float, int]:
    if is_float and byte_count == 4:
        return struct.unpack_from("<f", data, offset)[0], offset + 4
    if byte_count == 1:
        return data[offset], offset + 1
    if byte_count == 2:
        return struct.unpack_from("<H", data, offset)[0], offset + 2
    if byte_count == 4:
        return struct.unpack_from("<I", data, offset)[0], offset + 4
    if byte_count == 8:
        return struct.unpack_from("<Q", data, offset)[0], offset + 8
    raise ValueError(f"Unsupported report field byte_count={byte_count}")


def parse_report_validity(fields: list[ReportFieldDef], version: int, data_valid: bytes) -> dict[int, bool]:
    valid_map: dict[int, bool] = {}
    bit_index = 0
    for field_def in fields:
        if field_def.type_id < 0:
            continue
        if field_def.support_version > version:
            valid_map[field_def.type_id] = False
            continue
        if not data_valid:
            valid_map[field_def.type_id] = True
            continue
        byte_idx = bit_index // 8
        bit_idx = bit_index % 8
        if byte_idx >= len(data_valid):
            raise ValueError(f"Report dataValid too short: need byte {byte_idx}, have {len(data_valid)}")
        valid_map[field_def.type_id] = bool(data_valid[byte_idx] & (1 << (7 - bit_idx)))
        bit_index += 1
    return valid_map


def _parse_report_fields(body: bytes, version: int, data_valid: bytes, fields: list[ReportFieldDef]) -> dict[int, int | float]:
    valid_map = parse_report_validity(fields, version, data_valid)
    result: dict[int, int | float] = {}
    parsed: dict[int, int | float] = {}
    buf = memoryview(body)
    offset = 0
    for field_def in fields:
        if field_def.support_version > version:
            continue
        if field_def.depends_on is not None:
            dep_type_id, dep_values = field_def.depends_on
            dep_val = parsed.get(dep_type_id)
            if dep_val is None or dep_val not in dep_values:
                continue
        if offset + field_def.byte_count > len(buf):
            break
        value, offset = _read_report_value(buf, offset, field_def.byte_count, field_def.is_float)
        parsed[field_def.type_id] = value
        if field_def.type_id >= 0 and valid_map.get(field_def.type_id, False):
            result[field_def.type_id] = value
    return result


def _build_sport_report(raw_values: dict[int, int | float]) -> SportReport:
    report = SportReport()
    for type_id, value in raw_values.items():
        attr = _REPORT_FIELD_ATTR.get(type_id)
        if attr is not None:
            setattr(report, attr, value)
    report.raw_values = dict(raw_values)
    return report


def parse_sport_report(decrypted: bytes, sport_type: int) -> SportReport | None:
    if len(decrypted) < SPORT_SERVER_DATA_ID_LEN + 1:
        logger.warning("Sport report data too short to read header version byte")
        return None
    version = decrypted[5]
    fields = _SPORT_REPORT_FIELDS.get(sport_type)
    if fields is None:
        logger.info("No report parser for sport_type=%d; skipping report parse", sport_type)
        return None
    header = parse_fds_header(decrypted, compute_report_validity_len(fields, version))
    raw_values = _parse_report_fields(header.body_data, version, header.data_valid, fields)
    return _build_sport_report(raw_values)


def download_and_parse_sport_report(
    session: requests.Session,
    fds_entry: dict[str, Any],
    sport_type: int,
    *,
    timeout: int = 30,
    cache: FdsCache | None = None,
    cache_key: str | None = None,
) -> SportReport | None:
    return download_and_parse_fds_file(session, fds_entry, lambda decrypted: parse_sport_report(decrypted, sport_type), lambda: None, timeout=timeout, cache=cache, cache_key=cache_key, entry_label="sport report", download_label="FDS sport report", decrypt_label="FDS sport report", parse_label="FDS sport report binary")