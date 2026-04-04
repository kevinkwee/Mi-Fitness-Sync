from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import requests

from mi_fitness_sync.fds.cache import FdsCache
from mi_fitness_sync.fds.common import (
    SPORT_SERVER_DATA_ID_LEN,
    FdsHeader,
    FourDimenType,
    FourDimenValid,
    OneDimenType,
    TYPE_CALORIES,
    TYPE_CADENCE,
    TYPE_CYCLE_CADENCE,
    TYPE_DISTANCE,
    TYPE_DISTANCE_DOUBLE,
    TYPE_END_TIME,
    TYPE_GYM_ACTION_ID,
    TYPE_GYM_ACTION_TIMES,
    TYPE_GYM_ACTION_WEIGHT,
    TYPE_HEIGHT_CHANGE_SIGN,
    TYPE_HEIGHT_VALUE,
    TYPE_HR,
    TYPE_INTEGER_KM,
    TYPE_IT_STATE,
    TYPE_IT_TOTAL_DURATION,
    TYPE_JUMP_CADENCE,
    TYPE_LANDING_IMPACT,
    TYPE_PACE,
    TYPE_POWER,
    TYPE_PULL_OARS,
    TYPE_RESISTANCE,
    TYPE_ROWING_CADENCE,
    TYPE_RUNNING_POWER,
    TYPE_SHOOT_COUNT,
    TYPE_SKIP_COUNT,
    TYPE_SPO2,
    TYPE_STEPS,
    TYPE_STRESS,
    TYPE_STRIDE,
    TYPE_STROKE_COUNT,
    TYPE_STROKE_FREQ,
    TYPE_SWING_COUNT,
    TYPE_SWOLF,
    TYPE_SPEED,
    TYPE_TOTAL_CAL,
    TYPE_TOUCHDOWN_AIR_RATIO,
    extract_high_value,
    parse_four_dimen_valid,
    parse_one_dimen_valid,
    read_uint,
    get_record_data_valid_len,
    parse_fds_header,
)
from mi_fitness_sync.fds.downloader import download_and_parse_fds_file


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SportSample:
    timestamp: int
    heart_rate: int | None = None
    calories: int | None = None
    spo2: int | None = None
    stress: int | None = None
    steps: int | None = None
    distance: int | None = None
    speed: int | None = None
    cadence: int | None = None
    pace: int | None = None
    power: int | None = None
    stride_length: int | None = None
    resistance: int | None = None
    running_power: int | None = None
    altitude_value: int | None = None
    extras: dict[int, int] = field(default_factory=dict)


_TYPE_TO_ATTR: dict[int, str] = {
    TYPE_HR: "heart_rate",
    TYPE_CALORIES: "calories",
    TYPE_SPO2: "spo2",
    TYPE_STRESS: "stress",
    TYPE_STEPS: "steps",
    TYPE_DISTANCE: "distance",
    TYPE_SPEED: "speed",
    TYPE_CADENCE: "cadence",
    TYPE_CYCLE_CADENCE: "cadence",
    TYPE_PACE: "pace",
    TYPE_POWER: "power",
    TYPE_STRIDE: "stride_length",
    TYPE_RESISTANCE: "resistance",
    TYPE_RUNNING_POWER: "running_power",
    TYPE_HEIGHT_VALUE: "altitude_value",
}


def _record_to_sample(timestamp: int, record: dict[int, int]) -> SportSample:
    sample = SportSample(timestamp=timestamp)
    for type_id, value in record.items():
        attr = _TYPE_TO_ATTR.get(type_id)
        if attr is not None:
            setattr(sample, attr, value)
        else:
            sample.extras[type_id] = value
    return sample


def it_summary_byte_count(types: list[OneDimenType], version: int) -> int:
    return sum(
        data_type.byte_count
        for data_type in types
        if data_type.type_id >= 0 and data_type.support_version <= version and data_type.depends_on is None
    )


def _read_it_summary(
    buf: memoryview | bytes,
    offset: int,
    types: list[OneDimenType],
    version: int,
) -> tuple[dict[int, int], int]:
    result: dict[int, int] = {}
    for data_type in types:
        if data_type.support_version > version:
            continue
        if data_type.depends_on is not None:
            dep_type_id, dep_values = data_type.depends_on
            dep_val = result.get(dep_type_id)
            if dep_val is None or dep_val not in dep_values:
                continue
        if offset + data_type.byte_count > len(buf):
            break
        value, offset = read_uint(buf, offset, data_type.byte_count)
        result[data_type.type_id] = value
    return result, offset


def _pause_init_byte_count(types: list[OneDimenType] | None, version: int) -> int:
    if types is None:
        return 0
    return sum(data_type.byte_count for data_type in types if data_type.support_version <= version)


def parse_one_dimen_records(
    buf: memoryview | bytes,
    offset: int,
    record_count: int,
    data_types: list[OneDimenType],
    version: int,
    valid_map: dict[int, bool],
) -> tuple[list[dict[int, int]], int]:
    records: list[dict[int, int]] = []
    for _ in range(record_count):
        record: dict[int, int] = {}
        parsed: dict[int, int] = {}
        for data_type in data_types:
            if data_type.support_version > version:
                continue
            if data_type.depends_on is not None:
                dep_type_id, dep_values = data_type.depends_on
                dep_val = parsed.get(dep_type_id)
                if dep_val is None or dep_val not in dep_values:
                    continue
            if offset + data_type.byte_count > len(buf):
                return records, offset
            value, offset = read_uint(buf, offset, data_type.byte_count)
            parsed[data_type.type_id] = value
            if data_type.type_id >= 0 and valid_map.get(data_type.type_id, False):
                record[data_type.type_id] = value
        records.append(record)
    return records, offset


def parse_four_dimen_records(
    buf: memoryview | bytes,
    offset: int,
    record_count: int,
    data_types: list[FourDimenType],
    version: int,
    valid_map: dict[int, FourDimenValid],
) -> tuple[list[dict[int, int]], int]:
    records: list[dict[int, int]] = []
    for _ in range(record_count):
        record: dict[int, int] = {}
        for data_type in data_types:
            if data_type.support_version > version:
                continue
            valid = valid_map.get(data_type.type_id)
            if valid is None or not valid.exist:
                continue
            if offset + data_type.byte_size > len(buf):
                return records, offset
            value, offset = read_uint(buf, offset, data_type.byte_size)
            if valid.high:
                record[data_type.type_id] = extract_high_value(value, data_type)
        records.append(record)
    return records, offset


def _parse_body_one_dimen(
    body: bytes,
    data_valid: bytes,
    version: int,
    record_types: list[OneDimenType],
    it_summary_types: list[OneDimenType],
    pause_init_types: list[OneDimenType] | None = None,
) -> list[SportSample]:
    valid_map = parse_one_dimen_valid(record_types, version, data_valid)
    it_bytes = it_summary_byte_count(it_summary_types, version)
    init_bytes = _pause_init_byte_count(pause_init_types, version)
    min_segment = init_bytes + 8 + it_bytes
    samples: list[SportSample] = []
    offset = 0
    buf = memoryview(body)
    while offset + min_segment <= len(buf):
        offset += init_bytes
        record_count, offset = read_uint(buf, offset, 4)
        start_time, offset = read_uint(buf, offset, 4)
        _, offset = _read_it_summary(buf, offset, it_summary_types, version)
        records, offset = parse_one_dimen_records(buf, offset, record_count, record_types, version, valid_map)
        for index, record in enumerate(records):
            samples.append(_record_to_sample(start_time + index, record))
    return samples


def _parse_body_four_dimen(
    body: bytes,
    data_valid: bytes,
    version: int,
    record_types: list[FourDimenType],
    it_summary_types: list[OneDimenType],
    pause_init_types: list[OneDimenType] | None = None,
) -> list[SportSample]:
    valid_map = parse_four_dimen_valid(record_types, version, data_valid)
    it_bytes = it_summary_byte_count(it_summary_types, version)
    init_bytes = _pause_init_byte_count(pause_init_types, version)
    min_segment = init_bytes + 8 + it_bytes
    samples: list[SportSample] = []
    offset = 0
    buf = memoryview(body)
    while offset + min_segment <= len(buf):
        offset += init_bytes
        record_count, offset = read_uint(buf, offset, 4)
        start_time, offset = read_uint(buf, offset, 4)
        _, offset = _read_it_summary(buf, offset, it_summary_types, version)
        records, offset = parse_four_dimen_records(buf, offset, record_count, record_types, version, valid_map)
        for index, record in enumerate(records):
            samples.append(_record_to_sample(start_time + index, record))
    return samples


@dataclass(slots=True, frozen=True)
class SportRecordConfig:
    it_summary_types: list[OneDimenType] = field(default_factory=list)
    one_dimen_types: list[OneDimenType] | None = None
    four_dimen_types: list[FourDimenType] | None = None
    four_dimen_min_version: int = 1
    alt_four_dimen_types: list[FourDimenType] | None = None
    alt_four_dimen_min_version: int = 0
    pause_init_types: list[OneDimenType] | None = None


def parse_with_config(header: FdsHeader, config: SportRecordConfig) -> list[SportSample]:
    version = header.version
    if config.alt_four_dimen_types is not None and version >= config.alt_four_dimen_min_version:
        return _parse_body_four_dimen(header.body_data, header.data_valid, version, config.alt_four_dimen_types, config.it_summary_types, config.pause_init_types)
    if config.four_dimen_types is not None and version >= config.four_dimen_min_version:
        return _parse_body_four_dimen(header.body_data, header.data_valid, version, config.four_dimen_types, config.it_summary_types, config.pause_init_types)
    if config.one_dimen_types is not None:
        return _parse_body_one_dimen(header.body_data, header.data_valid, version, config.one_dimen_types, config.it_summary_types, config.pause_init_types)
    return []


_IT_STATE_ONLY = [OneDimenType(TYPE_IT_STATE, 1, 2)]
FREE_TRAINING_IT_SUMMARY_TYPES = [OneDimenType(TYPE_IT_STATE, 1, 2), OneDimenType(TYPE_IT_TOTAL_DURATION, 4, 4), OneDimenType(TYPE_GYM_ACTION_TIMES, 2, 5), OneDimenType(TYPE_GYM_ACTION_WEIGHT, 2, 5), OneDimenType(TYPE_GYM_ACTION_ID, 2, 5)]
_FREE_TRAINING_RECORD_TYPES = [OneDimenType(TYPE_HR, 1, 1), OneDimenType(TYPE_CALORIES, 1, 1)]
_FREE_TRAINING_FOURDIMEN_TYPES = [FourDimenType(TYPE_HR, 1, 3), FourDimenType(TYPE_CALORIES, 1, 3), FourDimenType(TYPE_SPO2, 1, 3), FourDimenType(TYPE_STRESS, 1, 3)]
_FREE_TRAINING_CONFIG = SportRecordConfig(it_summary_types=FREE_TRAINING_IT_SUMMARY_TYPES, one_dimen_types=_FREE_TRAINING_RECORD_TYPES, four_dimen_types=_FREE_TRAINING_FOURDIMEN_TYPES, four_dimen_min_version=3)
_OUTDOOR_SPORT_CONFIG = SportRecordConfig(it_summary_types=_IT_STATE_ONLY, four_dimen_types=[FourDimenType(TYPE_CALORIES, 1, 1, high_start_bit=4, high_bit_count=4), FourDimenType(TYPE_HR, 1, 1), FourDimenType(TYPE_INTEGER_KM, 1, 1, high_start_bit=7, high_bit_count=1), FourDimenType(TYPE_DISTANCE, 1, 1)], pause_init_types=[OneDimenType(0, 4, 1)])
_INDOOR_RUN_CONFIG = SportRecordConfig(it_summary_types=[OneDimenType(TYPE_IT_STATE, 1, 2), OneDimenType(43, 4, 4), OneDimenType(TYPE_IT_TOTAL_DURATION, 4, 8), OneDimenType(55, 2, 7)], four_dimen_types=[FourDimenType(TYPE_CALORIES, 1, 1, high_start_bit=4, high_bit_count=4), FourDimenType(TYPE_HR, 1, 1), FourDimenType(TYPE_DISTANCE, 1, 1), FourDimenType(TYPE_STRIDE, 1, 3), FourDimenType(TYPE_LANDING_IMPACT, 4, 5, high_start_bit=26, high_bit_count=6), FourDimenType(TYPE_TOUCHDOWN_AIR_RATIO, 1, 6), FourDimenType(TYPE_CADENCE, 1, 6), FourDimenType(TYPE_PACE, 2, 6), FourDimenType(TYPE_RUNNING_POWER, 2, 7), FourDimenType(79, 2, 9), FourDimenType(80, 2, 9)])
_OUTDOOR_BIKING_CONFIG = SportRecordConfig(it_summary_types=_IT_STATE_ONLY, four_dimen_types=[FourDimenType(TYPE_CALORIES, 1, 1), FourDimenType(TYPE_HR, 1, 1), FourDimenType(TYPE_INTEGER_KM, 1, 1, high_start_bit=7, high_bit_count=1)], pause_init_types=[OneDimenType(0, 4, 1)])
_INDOOR_BIKING_CONFIG = SportRecordConfig(it_summary_types=[OneDimenType(TYPE_IT_STATE, 1, 3), OneDimenType(43, 4, 4)], one_dimen_types=[OneDimenType(TYPE_HR, 1, 1), OneDimenType(TYPE_CALORIES, 1, 1)], four_dimen_types=[FourDimenType(TYPE_CALORIES, 1, 2, high_start_bit=4, high_bit_count=4), FourDimenType(TYPE_HR, 1, 2), FourDimenType(TYPE_DISTANCE, 1, 2), FourDimenType(TYPE_RESISTANCE, 1, 2), FourDimenType(TYPE_POWER, 2, 5), FourDimenType(TYPE_SPEED, 2, 6), FourDimenType(TYPE_CYCLE_CADENCE, 1, 6)], four_dimen_min_version=2)
_SWIMMING_DEP = (-1, frozenset({0}))
_SWIMMING_CONFIG = SportRecordConfig(one_dimen_types=[OneDimenType(-1, 1, 1), OneDimenType(TYPE_END_TIME, 4, 1), OneDimenType(11, 1, 1), OneDimenType(TYPE_PACE, 2, 1), OneDimenType(TYPE_SWOLF, 2, 1), OneDimenType(TYPE_DISTANCE, 2, 1, depends_on=_SWIMMING_DEP), OneDimenType(TYPE_CALORIES, 2, 1, depends_on=_SWIMMING_DEP), OneDimenType(TYPE_STROKE_COUNT, 2, 1, depends_on=_SWIMMING_DEP), OneDimenType(10, 2, 1, depends_on=_SWIMMING_DEP), OneDimenType(TYPE_STROKE_FREQ, 1, 1, depends_on=_SWIMMING_DEP), OneDimenType(18, 1, 1, depends_on=_SWIMMING_DEP), OneDimenType(19, 1, 1, depends_on=_SWIMMING_DEP), OneDimenType(20, 1, 1, depends_on=_SWIMMING_DEP), OneDimenType(21, 1, 1, depends_on=_SWIMMING_DEP), OneDimenType(22, 1, 1, depends_on=_SWIMMING_DEP), OneDimenType(TYPE_TOTAL_CAL, 2, 2, depends_on=_SWIMMING_DEP), OneDimenType(81, 1, 3, depends_on=_SWIMMING_DEP), OneDimenType(82, 1, 3, depends_on=_SWIMMING_DEP), OneDimenType(83, 1, 3, depends_on=_SWIMMING_DEP), OneDimenType(84, 2, 3, depends_on=_SWIMMING_DEP), OneDimenType(85, 4, 3, depends_on=_SWIMMING_DEP), OneDimenType(86, 1, 3, depends_on=_SWIMMING_DEP)])
_ELLIPTICAL_CONFIG = SportRecordConfig(it_summary_types=_IT_STATE_ONLY, four_dimen_types=[FourDimenType(TYPE_CALORIES, 1, 1, high_start_bit=4, high_bit_count=4), FourDimenType(TYPE_HR, 1, 1), FourDimenType(TYPE_CADENCE, 1, 3)])
_ROWING_CONFIG = SportRecordConfig(it_summary_types=[OneDimenType(TYPE_IT_STATE, 1, 2), OneDimenType(42, 4, 3)], four_dimen_types=[FourDimenType(TYPE_HR, 1, 1), FourDimenType(TYPE_CALORIES, 1, 1), FourDimenType(TYPE_PULL_OARS, 1, 1, high_start_bit=7, high_bit_count=1)], alt_four_dimen_types=[FourDimenType(TYPE_HR, 1, 1), FourDimenType(TYPE_CALORIES, 1, 1), FourDimenType(TYPE_ROWING_CADENCE, 1, 4)], alt_four_dimen_min_version=4)
_ROPE_SKIPPING_CONFIG = SportRecordConfig(it_summary_types=[OneDimenType(TYPE_IT_STATE, 1, 3), OneDimenType(42, 4, 4)], one_dimen_types=[OneDimenType(TYPE_HR, 1, 1), OneDimenType(TYPE_CALORIES, 1, 1), OneDimenType(TYPE_SKIP_COUNT, 1, 1), OneDimenType(36, 1, 1), OneDimenType(37, 1, 2)], four_dimen_types=[FourDimenType(TYPE_HR, 1, 5), FourDimenType(TYPE_CALORIES, 1, 5), FourDimenType(TYPE_JUMP_CADENCE, 2, 5), FourDimenType(36, 1, 5), FourDimenType(37, 1, 5)], four_dimen_min_version=5, alt_four_dimen_types=[FourDimenType(TYPE_HR, 1, 6), FourDimenType(TYPE_CALORIES, 1, 6), FourDimenType(TYPE_JUMP_CADENCE, 2, 6), FourDimenType(36, 1, 6), FourDimenType(37, 1, 6, high_start_bit=6, high_bit_count=2)], alt_four_dimen_min_version=6)
_TRIATHLON_CONFIG = SportRecordConfig(one_dimen_types=[OneDimenType(TYPE_HR, 1, 1), OneDimenType(TYPE_CALORIES, 1, 1)])
_ORDINARY_BALL_CONFIG = SportRecordConfig(four_dimen_types=[FourDimenType(TYPE_HR, 1, 1), FourDimenType(TYPE_CALORIES, 1, 1), FourDimenType(TYPE_SWING_COUNT, 1, 1, high_start_bit=4, high_bit_count=4), FourDimenType(TYPE_DISTANCE, 1, 1)])
_BASKETBALL_CONFIG = SportRecordConfig(four_dimen_types=[FourDimenType(TYPE_HR, 1, 1), FourDimenType(TYPE_CALORIES, 1, 1), FourDimenType(TYPE_SHOOT_COUNT, 1, 1, high_start_bit=4, high_bit_count=4), FourDimenType(TYPE_DISTANCE, 1, 1)])
_GOLF_CONFIG = SportRecordConfig(one_dimen_types=[OneDimenType(TYPE_END_TIME, 4, 1), OneDimenType(TYPE_CALORIES, 2, 1), OneDimenType(TYPE_TOTAL_CAL, 2, 1), OneDimenType(31, 2, 1), OneDimenType(32, 2, 1), OneDimenType(33, 2, 1), OneDimenType(34, 2, 1)])
_SKI_CONFIG = SportRecordConfig(it_summary_types=[OneDimenType(59, 4, 3), OneDimenType(60, 4, 3), OneDimenType(61, 2, 3), OneDimenType(62, 2, 3), OneDimenType(63, 1, 3)], four_dimen_types=[FourDimenType(TYPE_CALORIES, 1, 1), FourDimenType(TYPE_HR, 1, 1), FourDimenType(TYPE_HEIGHT_VALUE, 4, 4), FourDimenType(TYPE_DISTANCE_DOUBLE, 2, 4), FourDimenType(TYPE_HEIGHT_CHANGE_SIGN, 1, 1, high_start_bit=7, high_bit_count=1, max_support_version=3), FourDimenType(TYPE_DISTANCE, 1, 1, max_support_version=3), FourDimenType(TYPE_SPEED, 2, 2)], pause_init_types=[OneDimenType(-2, 1, 1), OneDimenType(0, 4, 1)])
_OUTDOOR_STEP_CONFIG = SportRecordConfig(it_summary_types=[OneDimenType(TYPE_IT_STATE, 1, 1), OneDimenType(43, 4, 3), OneDimenType(TYPE_IT_TOTAL_DURATION, 4, 7), OneDimenType(54, 4, 6), OneDimenType(55, 2, 6)], four_dimen_types=[FourDimenType(TYPE_CALORIES, 1, 1, high_start_bit=4, high_bit_count=4), FourDimenType(TYPE_HR, 1, 1), FourDimenType(TYPE_HEIGHT_VALUE, 4, 9), FourDimenType(TYPE_INTEGER_KM, 2, 9, high_start_bit=15, high_bit_count=1), FourDimenType(TYPE_INTEGER_KM, 1, 1, high_start_bit=7, high_bit_count=1), FourDimenType(TYPE_DISTANCE, 1, 1), FourDimenType(TYPE_STRIDE, 1, 2), FourDimenType(TYPE_LANDING_IMPACT, 4, 4, high_start_bit=26, high_bit_count=6), FourDimenType(TYPE_TOUCHDOWN_AIR_RATIO, 1, 5), FourDimenType(TYPE_CADENCE, 1, 5), FourDimenType(TYPE_PACE, 2, 5), FourDimenType(56, 2, 6), FourDimenType(TYPE_RUNNING_POWER, 2, 6), FourDimenType(79, 2, 8), FourDimenType(80, 2, 8)], pause_init_types=[OneDimenType(0, 4, 1)])
_OUTDOOR_NO_STEP_CONFIG = SportRecordConfig(it_summary_types=[OneDimenType(TYPE_IT_STATE, 1, 1), OneDimenType(43, 4, 2), OneDimenType(TYPE_IT_TOTAL_DURATION, 4, 5), OneDimenType(58, 2, 4)], four_dimen_types=[FourDimenType(TYPE_CALORIES, 1, 1, high_start_bit=4, high_bit_count=4), FourDimenType(TYPE_HR, 1, 1), FourDimenType(TYPE_HEIGHT_VALUE, 4, 6), FourDimenType(TYPE_INTEGER_KM, 2, 6, high_start_bit=15, high_bit_count=1), FourDimenType(TYPE_INTEGER_KM, 1, 1, high_start_bit=7, high_bit_count=1), FourDimenType(TYPE_DISTANCE, 1, 1), FourDimenType(TYPE_SPEED, 2, 3), FourDimenType(TYPE_CYCLE_CADENCE, 1, 3)], pause_init_types=[OneDimenType(0, 4, 1)])
_ROCK_CLIMBING_CONFIG = SportRecordConfig(four_dimen_types=[FourDimenType(TYPE_HR, 1, 1), FourDimenType(TYPE_CALORIES, 1, 1), FourDimenType(TYPE_HEIGHT_CHANGE_SIGN, 1, 1, high_start_bit=7, high_bit_count=1), FourDimenType(TYPE_HEIGHT_VALUE, 4, 2)], pause_init_types=[OneDimenType(0, 4, 1)])
_DIVING_IT_DEP = (64, frozenset({1}))
_DIVING_CONFIG = SportRecordConfig(it_summary_types=[OneDimenType(64, 1, 1), OneDimenType(65, 4, 1, depends_on=_DIVING_IT_DEP), OneDimenType(66, 4, 1, depends_on=_DIVING_IT_DEP), OneDimenType(67, 2, 1, depends_on=_DIVING_IT_DEP), OneDimenType(68, 2, 1, depends_on=_DIVING_IT_DEP), OneDimenType(69, 2, 1, depends_on=_DIVING_IT_DEP), OneDimenType(75, 2, 2, depends_on=_DIVING_IT_DEP), OneDimenType(76, 2, 2, depends_on=_DIVING_IT_DEP), OneDimenType(77, 2, 2, depends_on=_DIVING_IT_DEP)], four_dimen_types=[FourDimenType(70, 1, 1), FourDimenType(71, 2, 1), FourDimenType(72, 2, 1), FourDimenType(73, 2, 1, high_start_bit=14, high_bit_count=2)])

SPORT_CONFIG: dict[int, SportRecordConfig] = {1: _OUTDOOR_SPORT_CONFIG, 2: _OUTDOOR_SPORT_CONFIG, 3: _INDOOR_RUN_CONFIG, 4: _OUTDOOR_SPORT_CONFIG, 5: _OUTDOOR_SPORT_CONFIG, 6: _OUTDOOR_BIKING_CONFIG, 7: _INDOOR_BIKING_CONFIG, 8: _FREE_TRAINING_CONFIG, 9: _SWIMMING_CONFIG, 10: _SWIMMING_CONFIG, 11: _ELLIPTICAL_CONFIG, 12: _FREE_TRAINING_CONFIG, 13: _ROWING_CONFIG, 14: _ROPE_SKIPPING_CONFIG, 15: _OUTDOOR_SPORT_CONFIG, 16: _FREE_TRAINING_CONFIG, 17: _TRIATHLON_CONFIG, 18: _ORDINARY_BALL_CONFIG, 19: _BASKETBALL_CONFIG, 20: _GOLF_CONFIG, 21: _SKI_CONFIG, 22: _OUTDOOR_STEP_CONFIG, 23: _OUTDOOR_NO_STEP_CONFIG, 24: _ROCK_CLIMBING_CONFIG, 25: _DIVING_CONFIG}


def parse_free_training_record(header: FdsHeader) -> list[SportSample]:
    return parse_with_config(header, _FREE_TRAINING_CONFIG)


def parse_sport_record(decrypted: bytes, sport_type: int) -> list[SportSample]:
    if len(decrypted) < SPORT_SERVER_DATA_ID_LEN + 1:
        logger.warning("Decrypted data too short to read header version byte")
        return []
    version = decrypted[5]
    data_valid_len = get_record_data_valid_len(sport_type, version)
    if data_valid_len is None:
        logger.info("No dataValid mapping for sport_type=%d version=%d; skipping FDS parse", sport_type, version)
        return []
    header = parse_fds_header(decrypted, data_valid_len)
    config = SPORT_CONFIG.get(sport_type)
    if config is None:
        logger.info("No parser for sport_type=%d; skipping FDS parse", sport_type)
        return []
    return parse_with_config(header, config)


def download_and_parse_sport_record(
    session: requests.Session,
    fds_entry: dict[str, Any],
    sport_type: int,
    *,
    timeout: int = 30,
    cache: FdsCache | None = None,
    cache_key: str | None = None,
) -> list[SportSample]:
    return download_and_parse_fds_file(session, fds_entry, lambda decrypted: parse_sport_record(decrypted, sport_type), lambda: [], timeout=timeout, cache=cache, cache_key=cache_key, entry_label="sport record", download_label="FDS sport record", decrypt_label="FDS sport record", parse_label="FDS sport record binary")