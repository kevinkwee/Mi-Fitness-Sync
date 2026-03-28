"""FDS binary sport record download, decryption and parsing.

Implements the Mi Fitness FDS (Fitness Data Service) pipeline:
  1. Download encrypted binary from FDS URL
  2. AES-CBC decrypt using objectKey from FDS metadata
  3. Parse binary header (serverDataId + dataValid)
  4. Parse body as OneDimen or FourDimen sport records
"""

from __future__ import annotations

import base64
import logging
import struct
from dataclasses import dataclass, field
from typing import Any

import requests

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# AES parameters (from decompiled AESCoder.java)
# ---------------------------------------------------------------------------
_AES_IV = b"1234567887654321"

# ---------------------------------------------------------------------------
# Header constants (from FitnessDataId.java / FitnessDataHeader.java)
# ---------------------------------------------------------------------------
_SPORT_SERVER_DATA_ID_LEN = 7  # [timestamp 4LE][tz 1][version 1][sportType 1]

# ---------------------------------------------------------------------------
# DataItemType constants (from SportRecordBaseParser.DataItemType)
# ---------------------------------------------------------------------------
TYPE_END_TIME = 1
TYPE_CALORIES = 2
TYPE_TOTAL_CAL = 3
TYPE_STEPS = 4
TYPE_HR = 5
TYPE_INTEGER_KM = 6
TYPE_HEIGHT_CHANGE_SIGN = 7
TYPE_HEIGHT_CHANGE_VALUE = 8
TYPE_DISTANCE = 9
TYPE_TURN_COUNT = 10
TYPE_PACE = 12
TYPE_SWOLF = 13
TYPE_STROKE_COUNT = 16
TYPE_STROKE_FREQ = 17
TYPE_RESISTANCE = 23
TYPE_PULL_OARS = 24
TYPE_SHOOT_COUNT = 27
TYPE_SWING_COUNT = 29
TYPE_SKIP_COUNT = 35
TYPE_SPO2 = 38
TYPE_STRESS = 39
TYPE_STRIDE = 40
TYPE_IT_STATE = 41
TYPE_LANDING_IMPACT = 44
TYPE_POWER = 47
TYPE_TOUCHDOWN_AIR_RATIO = 48
TYPE_CADENCE = 49
TYPE_CYCLE_CADENCE = 50
TYPE_SPEED = 51
TYPE_ROWING_CADENCE = 52
TYPE_JUMP_CADENCE = 53
TYPE_RUNNING_POWER = 57
TYPE_IT_TOTAL_DURATION = 78
TYPE_HEIGHT_VALUE = 87
TYPE_DISTANCE_DOUBLE = 88
TYPE_GYM_ACTION_TIMES = 89
TYPE_GYM_ACTION_WEIGHT = 90
TYPE_GYM_ACTION_ID = 91


# ---------------------------------------------------------------------------
# AES decryption
# ---------------------------------------------------------------------------


def _b64url_decode(s: str) -> bytes:
    """Decode base64url string that may lack padding (Android flag NO_PADDING|URL_SAFE)."""
    remainder = len(s) % 4
    if remainder:
        s += "=" * (4 - remainder)
    return base64.urlsafe_b64decode(s)


def decrypt_fds_data(response_body: str, object_key: str) -> bytes:
    """Decrypt an FDS response body using AES-CBC with the given objectKey.

    Both *response_body* and *object_key* are base64url-no-padding encoded
    (Android ``Base64.decode(flag=11)``).

    Returns the decrypted binary sport record data.
    """
    key = _b64url_decode(object_key)
    ciphertext = _b64url_decode(response_body)
    cipher = AES.new(key, AES.MODE_CBC, _AES_IV)
    return unpad(cipher.decrypt(ciphertext), AES.block_size)


# ---------------------------------------------------------------------------
# Binary header parsing
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FdsHeader:
    """Parsed FDS binary header."""

    timestamp: int
    tz_in_15min: int
    version: int
    sport_type: int
    data_valid: bytes
    body_data: bytes


def parse_fds_header(data: bytes, data_valid_len: int) -> FdsHeader:
    """Split decrypted binary into header fields and body.

    Header layout:
        [serverDataId: 7 bytes] [0x00 pad] [dataValid: *data_valid_len* bytes]
    serverDataId layout:
        [timestamp: 4 LE uint32] [tzIn15Min: 1] [version: 1] [sportType: 1]
    """
    header_len = _SPORT_SERVER_DATA_ID_LEN + 1 + data_valid_len
    if len(data) < header_len:
        raise ValueError(
            f"Decrypted data too short ({len(data)} bytes) for expected header ({header_len} bytes)"
        )

    timestamp = struct.unpack_from("<I", data, 0)[0]
    tz_in_15min = data[4]
    version = data[5]
    sport_type = data[6]
    # data[7] is a zero-padding byte
    data_valid = data[8 : 8 + data_valid_len]
    body_data = data[header_len:]

    return FdsHeader(
        timestamp=timestamp,
        tz_in_15min=tz_in_15min,
        version=version,
        sport_type=sport_type,
        data_valid=data_valid,
        body_data=body_data,
    )


# ---------------------------------------------------------------------------
# DataValid length lookup (from FitnessDataValidity.java)
# ---------------------------------------------------------------------------

_FREE_TRAINING_RECORD_VALIDITY: dict[int, int] = {1: 1, 2: 1, 3: 2, 4: 2, 5: 2}
_OUTDOOR_RECORD_VALIDITY: dict[int, int] = {1: 2, 2: 2}
_RUNNING_IN_RECORD_VALIDITY: dict[int, int] = {1: 2, 2: 2}
_BIKING_OUT_RECORD_VALIDITY: dict[int, int] = {1: 2, 2: 2}
_BIKING_IN_RECORD_VALIDITY: dict[int, int] = {1: 1, 2: 2, 3: 2, 4: 2, 5: 3, 6: 4}
_SWIMMING_RECORD_VALIDITY: dict[int, int] = {1: 2, 2: 2, 3: 3}
_ELLIPTICAL_RECORD_VALIDITY: dict[int, int] = {1: 1, 2: 1}
_ROWING_RECORD_VALIDITY: dict[int, int] = {1: 1, 2: 1, 3: 2}
_ROPE_SKIPPING_RECORD_VALIDITY: dict[int, int] = {1: 2, 2: 2}
_NO_STEP_RECORD_VALIDITY: dict[int, int] = {1: 2, 2: 2, 3: 3, 4: 3, 5: 3, 6: 3}
_STEP_RECORD_VALIDITY: dict[int, int] = {1: 2, 2: 3, 3: 3, 4: 3, 5: 5, 6: 6, 7: 6, 8: 7, 9: 7}
_TRIATHLON_RECORD_VALIDITY: dict[int, int] = {1: 0, 2: 0}
_ORDINARY_BALL_RECORD_VALIDITY: dict[int, int] = {1: 2}
_BASKETBALL_RECORD_VALIDITY: dict[int, int] = {1: 2}
_GOLF_RECORD_VALIDITY: dict[int, int] = {1: 1}
_SKI_RECORD_VALIDITY: dict[int, int] = {1: 2, 2: 3, 3: 3, 4: 3}
_ROCK_CLIMBING_RECORD_VALIDITY: dict[int, int] = {1: 2, 2: 2}
_DIVING_RECORD_VALIDITY: dict[int, int] = {1: 2, 2: 2}

# Keyed by sport_type (= proto_type from the Mi Fitness report)
_SPORT_RECORD_VALIDITY: dict[int, dict[int, int]] = {
    1: _OUTDOOR_RECORD_VALIDITY,           # outdoor_run
    2: _OUTDOOR_RECORD_VALIDITY,           # track_running
    3: _RUNNING_IN_RECORD_VALIDITY,        # indoor_run / treadmill
    4: _OUTDOOR_RECORD_VALIDITY,           # outdoor_walk
    5: _OUTDOOR_RECORD_VALIDITY,           # trail_running
    6: _BIKING_OUT_RECORD_VALIDITY,        # outdoor_cycling
    7: _BIKING_IN_RECORD_VALIDITY,         # indoor_cycling
    8: _FREE_TRAINING_RECORD_VALIDITY,     # free_training / strength
    9: _SWIMMING_RECORD_VALIDITY,          # pool_swimming
    10: _SWIMMING_RECORD_VALIDITY,         # open_water_swimming
    11: _ELLIPTICAL_RECORD_VALIDITY,       # elliptical
    12: _FREE_TRAINING_RECORD_VALIDITY,    # yoga
    13: _ROWING_RECORD_VALIDITY,           # rowing_machine
    14: _ROPE_SKIPPING_RECORD_VALIDITY,    # jump_rope
    15: _OUTDOOR_RECORD_VALIDITY,          # hiking
    16: _FREE_TRAINING_RECORD_VALIDITY,    # HIIT
    17: _TRIATHLON_RECORD_VALIDITY,        # triathlon
    18: _ORDINARY_BALL_RECORD_VALIDITY,    # ordinary_ball
    19: _BASKETBALL_RECORD_VALIDITY,       # basketball
    20: _GOLF_RECORD_VALIDITY,             # golf
    21: _SKI_RECORD_VALIDITY,              # ski
    22: _STEP_RECORD_VALIDITY,             # step_sport
    23: _NO_STEP_RECORD_VALIDITY,          # no_step_sport
    24: _ROCK_CLIMBING_RECORD_VALIDITY,    # rock_climbing
    25: _DIVING_RECORD_VALIDITY,           # diving
    28: _FREE_TRAINING_RECORD_VALIDITY,    # strength_training (same format as free_training)
}


def get_record_data_valid_len(sport_type: int, version: int) -> int | None:
    """Return the expected dataValid byte length, or None if unsupported."""
    version_map = _SPORT_RECORD_VALIDITY.get(sport_type)
    if version_map is None:
        return None
    return version_map.get(version)


# ---------------------------------------------------------------------------
# OneDimen data type definitions per sport (from decompiled record parsers)
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class OneDimenType:
    type_id: int
    byte_count: int
    support_version: int
    depends_on: tuple[int, frozenset[int]] | None = None


@dataclass(slots=True, frozen=True)
class FourDimenType:
    """One field definition for FourDimen binary records.

    *byte_size* is the total bytes consumed per record when *exist* is True.
    For simple types, the value equals the full uint read.
    For bit-packed compound types, *high_start_bit* and *high_bit_count*
    control extraction of the primary ("high") sub-value.
    """

    type_id: int
    byte_size: int
    support_version: int
    high_start_bit: int | None = None  # None → full value
    high_bit_count: int | None = None
    max_support_version: int | None = None


# ---------------------------------------------------------------------------
# Per-second sport record
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class SportSample:
    """One per-second sport record sample."""

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


# ---------------------------------------------------------------------------
# OneDimen validity parsing
# ---------------------------------------------------------------------------


def _parse_one_dimen_valid(
    data_types: list[OneDimenType], version: int, data_valid: bytes,
) -> dict[int, bool]:
    """Parse OneDimen validity bitmap.  1 bit per supported type, MSB-first.

    If *data_valid* is empty all supported types are treated as valid (used by
    e.g. triathlon where ``data_valid_len == 0``).
    """
    valid_map: dict[int, bool] = {}
    bit_index = 0
    for dt in data_types:
        if dt.type_id < 0:
            continue
        if dt.support_version > version:
            valid_map[dt.type_id] = False
            continue
        if not data_valid:
            # No validity bitmap → all supported types are valid
            valid_map[dt.type_id] = True
            continue
        byte_idx = bit_index // 8
        bit_idx = bit_index % 8
        if byte_idx >= len(data_valid):
            raise ValueError(
                f"dataValid too short: need byte {byte_idx}, have {len(data_valid)}"
            )
        valid_map[dt.type_id] = bool(data_valid[byte_idx] & (1 << (7 - bit_idx)))
        bit_index += 1
    return valid_map


# ---------------------------------------------------------------------------
# FourDimen validity parsing
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class FourDimenValid:
    exist: bool
    high: bool
    middle: bool
    low: bool


def _parse_four_dimen_valid(
    data_types: list[FourDimenType], version: int, data_valid: bytes,
) -> dict[int, FourDimenValid]:
    """Parse FourDimen validity nibbles.  4 bits per supported type.

    Fields whose *max_support_version* is exceeded by *version* are marked
    all-false **without** consuming a nibble (matching Java semantics).
    """
    valid_map: dict[int, FourDimenValid] = {}
    nibble_index = 0
    for dt in data_types:
        if dt.max_support_version is not None and version > dt.max_support_version:
            valid_map[dt.type_id] = FourDimenValid(False, False, False, False)
            continue
        if dt.support_version > version:
            valid_map[dt.type_id] = FourDimenValid(False, False, False, False)
            continue
        byte_idx = nibble_index // 2
        if byte_idx >= len(data_valid):
            raise ValueError(
                f"dataValid too short: need byte {byte_idx}, have {len(data_valid)}"
            )
        if nibble_index % 2 == 0:
            nibble = (data_valid[byte_idx] & 0xF0) >> 4
        else:
            nibble = data_valid[byte_idx] & 0x0F
        valid_map[dt.type_id] = FourDimenValid(
            exist=bool(nibble & 0x8),
            high=bool(nibble & 0x4),
            middle=bool(nibble & 0x2),
            low=bool(nibble & 0x1),
        )
        nibble_index += 1
    return valid_map


# ---------------------------------------------------------------------------
# Low-level buffer reading
# ---------------------------------------------------------------------------


def _read_uint(data: memoryview | bytes, offset: int, size: int) -> tuple[int, int]:
    """Read *size*-byte unsigned LE int.  Returns ``(value, new_offset)``."""
    if size == 1:
        return data[offset], offset + 1
    if size == 2:
        return struct.unpack_from("<H", data, offset)[0], offset + 2
    if size == 4:
        return struct.unpack_from("<I", data, offset)[0], offset + 4
    raise ValueError(f"Unsupported read size {size}")


def _extract_high_value(raw_value: int, dt: FourDimenType) -> int:
    """Extract the 'high' sub-value from a raw uint, applying bit extraction if needed."""
    if dt.high_start_bit is not None and dt.high_bit_count is not None:
        return (raw_value >> dt.high_start_bit) & ((1 << dt.high_bit_count) - 1)
    return raw_value


# ---------------------------------------------------------------------------
# IT summary reading (sequential OneDimen read, all supported types valid)
# ---------------------------------------------------------------------------


def _it_summary_byte_count(types: list[OneDimenType], version: int) -> int:
    return sum(
        t.byte_count for t in types
        if t.type_id >= 0 and t.support_version <= version and t.depends_on is None
    )


def _read_it_summary(
    buf: memoryview | bytes, offset: int, types: list[OneDimenType], version: int,
) -> tuple[dict[int, int], int]:
    """Read IT summary data (one record, dependency-aware)."""
    result: dict[int, int] = {}
    for t in types:
        if t.support_version > version:
            continue
        if t.depends_on is not None:
            dep_type_id, dep_values = t.depends_on
            dep_val = result.get(dep_type_id)
            if dep_val is None or dep_val not in dep_values:
                continue
        if offset + t.byte_count > len(buf):
            break
        value, offset = _read_uint(buf, offset, t.byte_count)
        result[t.type_id] = value
    return result, offset


# ---------------------------------------------------------------------------
# Pause initial data reading
# ---------------------------------------------------------------------------


def _pause_init_byte_count(types: list[OneDimenType] | None, version: int) -> int:
    if types is None:
        return 0
    return sum(t.byte_count for t in types if t.support_version <= version)


# ---------------------------------------------------------------------------
# OneDimen record loop
# ---------------------------------------------------------------------------


def _parse_one_dimen_records(
    buf: memoryview | bytes,
    offset: int,
    record_count: int,
    data_types: list[OneDimenType],
    version: int,
    valid_map: dict[int, bool],
) -> tuple[list[dict[int, int]], int]:
    """Parse *record_count* OneDimen records.  Returns (records, new_offset).

    Supports dependency-aware field skipping: if a field has ``depends_on``
    set and the dependency condition is not met, its bytes are **not**
    consumed from the buffer (matching Java ``isDataExist()`` semantics).
    """
    records: list[dict[int, int]] = []
    for _ in range(record_count):
        rec: dict[int, int] = {}
        parsed: dict[int, int] = {}  # all values incl. negative type_ids
        for dt in data_types:
            if dt.support_version > version:
                continue
            if dt.depends_on is not None:
                dep_type_id, dep_values = dt.depends_on
                dep_val = parsed.get(dep_type_id)
                if dep_val is None or dep_val not in dep_values:
                    continue
            if offset + dt.byte_count > len(buf):
                return records, offset
            value, offset = _read_uint(buf, offset, dt.byte_count)
            parsed[dt.type_id] = value
            if dt.type_id >= 0 and valid_map.get(dt.type_id, False):
                rec[dt.type_id] = value
        records.append(rec)
    return records, offset


# ---------------------------------------------------------------------------
# FourDimen record loop
# ---------------------------------------------------------------------------


def _parse_four_dimen_records(
    buf: memoryview | bytes,
    offset: int,
    record_count: int,
    data_types: list[FourDimenType],
    version: int,
    valid_map: dict[int, FourDimenValid],
) -> tuple[list[dict[int, int]], int]:
    """Parse *record_count* FourDimen records.  Returns (records, new_offset)."""
    records: list[dict[int, int]] = []
    for _ in range(record_count):
        rec: dict[int, int] = {}
        for dt in data_types:
            if dt.support_version > version:
                continue
            dv = valid_map.get(dt.type_id)
            if dv is None or not dv.exist:
                continue
            if offset + dt.byte_size > len(buf):
                return records, offset
            value, offset = _read_uint(buf, offset, dt.byte_size)
            if dv.high:
                rec[dt.type_id] = _extract_high_value(value, dt)
        records.append(rec)
    return records, offset


# ---------------------------------------------------------------------------
# Segment-level parsing (loops over pause segments in body)
# ---------------------------------------------------------------------------


def _parse_body_one_dimen(
    body: bytes,
    data_valid: bytes,
    version: int,
    record_types: list[OneDimenType],
    it_summary_types: list[OneDimenType],
    pause_init_types: list[OneDimenType] | None = None,
) -> list[SportSample]:
    """Parse OneDimen sport record body into per-second samples."""
    valid_map = _parse_one_dimen_valid(record_types, version, data_valid)
    it_bytes = _it_summary_byte_count(it_summary_types, version)
    init_bytes = _pause_init_byte_count(pause_init_types, version)
    min_segment = init_bytes + 8 + it_bytes

    samples: list[SportSample] = []
    offset = 0
    buf = memoryview(body)

    while offset + min_segment <= len(buf):
        offset += init_bytes
        record_count, offset = _read_uint(buf, offset, 4)
        start_time, offset = _read_uint(buf, offset, 4)

        _it_data, offset = _read_it_summary(buf, offset, it_summary_types, version)

        records, offset = _parse_one_dimen_records(
            buf, offset, record_count, record_types, version, valid_map,
        )
        for i, rec in enumerate(records):
            samples.append(_record_to_sample(start_time + i, rec))

    return samples


def _parse_body_four_dimen(
    body: bytes,
    data_valid: bytes,
    version: int,
    record_types: list[FourDimenType],
    it_summary_types: list[OneDimenType],
    pause_init_types: list[OneDimenType] | None = None,
) -> list[SportSample]:
    """Parse FourDimen sport record body into per-second samples."""
    valid_map = _parse_four_dimen_valid(record_types, version, data_valid)
    it_bytes = _it_summary_byte_count(it_summary_types, version)
    init_bytes = _pause_init_byte_count(pause_init_types, version)
    min_segment = init_bytes + 8 + it_bytes

    samples: list[SportSample] = []
    offset = 0
    buf = memoryview(body)

    while offset + min_segment <= len(buf):
        offset += init_bytes
        record_count, offset = _read_uint(buf, offset, 4)
        start_time, offset = _read_uint(buf, offset, 4)

        _it_data, offset = _read_it_summary(buf, offset, it_summary_types, version)

        records, offset = _parse_four_dimen_records(
            buf, offset, record_count, record_types, version, valid_map,
        )
        for i, rec in enumerate(records):
            samples.append(_record_to_sample(start_time + i, rec))

    return samples


# ---------------------------------------------------------------------------
# Sport record config (data-driven approach)
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class SportRecordConfig:
    it_summary_types: list[OneDimenType] = field(default_factory=list)
    one_dimen_types: list[OneDimenType] | None = None
    four_dimen_types: list[FourDimenType] | None = None
    four_dimen_min_version: int = 1
    alt_four_dimen_types: list[FourDimenType] | None = None
    alt_four_dimen_min_version: int = 0
    pause_init_types: list[OneDimenType] | None = None


def _parse_with_config(header: FdsHeader, config: SportRecordConfig) -> list[SportSample]:
    """Generic parser that selects format based on version and config."""
    v = header.version
    if config.alt_four_dimen_types is not None and v >= config.alt_four_dimen_min_version:
        return _parse_body_four_dimen(
            header.body_data, header.data_valid, v,
            config.alt_four_dimen_types, config.it_summary_types,
            config.pause_init_types,
        )
    if config.four_dimen_types is not None and v >= config.four_dimen_min_version:
        return _parse_body_four_dimen(
            header.body_data, header.data_valid, v,
            config.four_dimen_types, config.it_summary_types,
            config.pause_init_types,
        )
    if config.one_dimen_types is not None:
        return _parse_body_one_dimen(
            header.body_data, header.data_valid, v,
            config.one_dimen_types, config.it_summary_types,
            config.pause_init_types,
        )
    return []


# ---------------------------------------------------------------------------
# Sport type configurations (from decompiled record parsers)
# ---------------------------------------------------------------------------

_IT_STATE_ONLY = [OneDimenType(TYPE_IT_STATE, 1, 2)]

_FREE_TRAINING_IT_SUMMARY_TYPES = [
    OneDimenType(TYPE_IT_STATE, 1, 2),
    OneDimenType(TYPE_IT_TOTAL_DURATION, 4, 4),
    OneDimenType(TYPE_GYM_ACTION_TIMES, 2, 5),
    OneDimenType(TYPE_GYM_ACTION_WEIGHT, 2, 5),
    OneDimenType(TYPE_GYM_ACTION_ID, 2, 5),
]
_FREE_TRAINING_RECORD_TYPES = [
    OneDimenType(TYPE_HR, 1, 1),
    OneDimenType(TYPE_CALORIES, 1, 1),
]
_FREE_TRAINING_FOURDIMEN_TYPES = [
    FourDimenType(TYPE_HR, 1, 3),
    FourDimenType(TYPE_CALORIES, 1, 3),
    FourDimenType(TYPE_SPO2, 1, 3),
    FourDimenType(TYPE_STRESS, 1, 3),
]
_FREE_TRAINING_CONFIG = SportRecordConfig(
    it_summary_types=_FREE_TRAINING_IT_SUMMARY_TYPES,
    one_dimen_types=_FREE_TRAINING_RECORD_TYPES,
    four_dimen_types=_FREE_TRAINING_FOURDIMEN_TYPES,
    four_dimen_min_version=3,
)

_OUTDOOR_SPORT_CONFIG = SportRecordConfig(
    it_summary_types=_IT_STATE_ONLY,
    four_dimen_types=[
        FourDimenType(TYPE_CALORIES, 1, 1, high_start_bit=4, high_bit_count=4),
        FourDimenType(TYPE_HR, 1, 1),
        FourDimenType(TYPE_INTEGER_KM, 1, 1, high_start_bit=7, high_bit_count=1),
        FourDimenType(TYPE_DISTANCE, 1, 1),
    ],
    pause_init_types=[OneDimenType(0, 4, 1)],
)

_INDOOR_RUN_CONFIG = SportRecordConfig(
    it_summary_types=[
        OneDimenType(TYPE_IT_STATE, 1, 2),
        OneDimenType(43, 4, 4),
        OneDimenType(TYPE_IT_TOTAL_DURATION, 4, 8),
        OneDimenType(55, 2, 7),
    ],
    four_dimen_types=[
        FourDimenType(TYPE_CALORIES, 1, 1, high_start_bit=4, high_bit_count=4),
        FourDimenType(TYPE_HR, 1, 1),
        FourDimenType(TYPE_DISTANCE, 1, 1),
        FourDimenType(TYPE_STRIDE, 1, 3),
        FourDimenType(TYPE_LANDING_IMPACT, 4, 5, high_start_bit=26, high_bit_count=6),
        FourDimenType(TYPE_TOUCHDOWN_AIR_RATIO, 1, 6),
        FourDimenType(TYPE_CADENCE, 1, 6),
        FourDimenType(TYPE_PACE, 2, 6),
        FourDimenType(TYPE_RUNNING_POWER, 2, 7),
        FourDimenType(79, 2, 9),
        FourDimenType(80, 2, 9),
    ],
)

_OUTDOOR_BIKING_CONFIG = SportRecordConfig(
    it_summary_types=_IT_STATE_ONLY,
    four_dimen_types=[
        FourDimenType(TYPE_CALORIES, 1, 1),
        FourDimenType(TYPE_HR, 1, 1),
        FourDimenType(TYPE_INTEGER_KM, 1, 1, high_start_bit=7, high_bit_count=1),
    ],
    pause_init_types=[OneDimenType(0, 4, 1)],
)

_INDOOR_BIKING_CONFIG = SportRecordConfig(
    it_summary_types=[OneDimenType(TYPE_IT_STATE, 1, 3), OneDimenType(43, 4, 4)],
    one_dimen_types=[OneDimenType(TYPE_HR, 1, 1), OneDimenType(TYPE_CALORIES, 1, 1)],
    four_dimen_types=[
        FourDimenType(TYPE_CALORIES, 1, 2, high_start_bit=4, high_bit_count=4),
        FourDimenType(TYPE_HR, 1, 2),
        FourDimenType(TYPE_DISTANCE, 1, 2),
        FourDimenType(TYPE_RESISTANCE, 1, 2),
        FourDimenType(TYPE_POWER, 2, 5),
        FourDimenType(TYPE_SPEED, 2, 6),
        FourDimenType(TYPE_CYCLE_CADENCE, 1, 6),
    ],
    four_dimen_min_version=2,
)

_SWIMMING_DEP = (-1, frozenset({0}))

_SWIMMING_CONFIG = SportRecordConfig(
    one_dimen_types=[
        OneDimenType(-1, 1, 1),
        OneDimenType(TYPE_END_TIME, 4, 1),
        OneDimenType(11, 1, 1),
        OneDimenType(TYPE_PACE, 2, 1),
        OneDimenType(TYPE_SWOLF, 2, 1),
        OneDimenType(TYPE_DISTANCE, 2, 1, depends_on=_SWIMMING_DEP),
        OneDimenType(TYPE_CALORIES, 2, 1, depends_on=_SWIMMING_DEP),
        OneDimenType(TYPE_STROKE_COUNT, 2, 1, depends_on=_SWIMMING_DEP),
        OneDimenType(TYPE_TURN_COUNT, 2, 1, depends_on=_SWIMMING_DEP),
        OneDimenType(TYPE_STROKE_FREQ, 1, 1, depends_on=_SWIMMING_DEP),
        OneDimenType(18, 1, 1, depends_on=_SWIMMING_DEP),
        OneDimenType(19, 1, 1, depends_on=_SWIMMING_DEP),
        OneDimenType(20, 1, 1, depends_on=_SWIMMING_DEP),
        OneDimenType(21, 1, 1, depends_on=_SWIMMING_DEP),
        OneDimenType(22, 1, 1, depends_on=_SWIMMING_DEP),
        OneDimenType(TYPE_TOTAL_CAL, 2, 2, depends_on=_SWIMMING_DEP),
        OneDimenType(81, 1, 3, depends_on=_SWIMMING_DEP),
        OneDimenType(82, 1, 3, depends_on=_SWIMMING_DEP),
        OneDimenType(83, 1, 3, depends_on=_SWIMMING_DEP),
        OneDimenType(84, 2, 3, depends_on=_SWIMMING_DEP),
        OneDimenType(85, 4, 3, depends_on=_SWIMMING_DEP),
        OneDimenType(86, 1, 3, depends_on=_SWIMMING_DEP),
    ],
)

_ELLIPTICAL_CONFIG = SportRecordConfig(
    it_summary_types=_IT_STATE_ONLY,
    four_dimen_types=[
        FourDimenType(TYPE_CALORIES, 1, 1, high_start_bit=4, high_bit_count=4),
        FourDimenType(TYPE_HR, 1, 1),
        FourDimenType(TYPE_CADENCE, 1, 3),
    ],
)

_ROWING_CONFIG = SportRecordConfig(
    it_summary_types=[OneDimenType(TYPE_IT_STATE, 1, 2), OneDimenType(42, 4, 3)],
    four_dimen_types=[
        FourDimenType(TYPE_HR, 1, 1),
        FourDimenType(TYPE_CALORIES, 1, 1),
        FourDimenType(TYPE_PULL_OARS, 1, 1, high_start_bit=7, high_bit_count=1),
    ],
    alt_four_dimen_types=[
        FourDimenType(TYPE_HR, 1, 1),
        FourDimenType(TYPE_CALORIES, 1, 1),
        FourDimenType(TYPE_ROWING_CADENCE, 1, 4),
    ],
    alt_four_dimen_min_version=4,
)

_ROPE_SKIPPING_CONFIG = SportRecordConfig(
    it_summary_types=[OneDimenType(TYPE_IT_STATE, 1, 3), OneDimenType(42, 4, 4)],
    one_dimen_types=[
        OneDimenType(TYPE_HR, 1, 1),
        OneDimenType(TYPE_CALORIES, 1, 1),
        OneDimenType(TYPE_SKIP_COUNT, 1, 1),
        OneDimenType(36, 1, 1),
        OneDimenType(37, 1, 2),
    ],
    four_dimen_types=[
        FourDimenType(TYPE_HR, 1, 5),
        FourDimenType(TYPE_CALORIES, 1, 5),
        FourDimenType(TYPE_JUMP_CADENCE, 2, 5),
        FourDimenType(36, 1, 5),
        FourDimenType(37, 1, 5),
    ],
    four_dimen_min_version=5,
    alt_four_dimen_types=[
        FourDimenType(TYPE_HR, 1, 6),
        FourDimenType(TYPE_CALORIES, 1, 6),
        FourDimenType(TYPE_JUMP_CADENCE, 2, 6),
        FourDimenType(36, 1, 6),
        FourDimenType(37, 1, 6, high_start_bit=6, high_bit_count=2),
    ],
    alt_four_dimen_min_version=6,
)

_TRIATHLON_CONFIG = SportRecordConfig(
    one_dimen_types=[OneDimenType(TYPE_HR, 1, 1), OneDimenType(TYPE_CALORIES, 1, 1)],
)

_ORDINARY_BALL_CONFIG = SportRecordConfig(
    four_dimen_types=[
        FourDimenType(TYPE_HR, 1, 1),
        FourDimenType(TYPE_CALORIES, 1, 1),
        FourDimenType(TYPE_SWING_COUNT, 1, 1, high_start_bit=4, high_bit_count=4),
        FourDimenType(TYPE_DISTANCE, 1, 1),
    ],
)

_BASKETBALL_CONFIG = SportRecordConfig(
    four_dimen_types=[
        FourDimenType(TYPE_HR, 1, 1),
        FourDimenType(TYPE_CALORIES, 1, 1),
        FourDimenType(TYPE_SHOOT_COUNT, 1, 1, high_start_bit=4, high_bit_count=4),
        FourDimenType(TYPE_DISTANCE, 1, 1),
    ],
)

_GOLF_CONFIG = SportRecordConfig(
    one_dimen_types=[
        OneDimenType(TYPE_END_TIME, 4, 1),
        OneDimenType(TYPE_CALORIES, 2, 1),
        OneDimenType(TYPE_TOTAL_CAL, 2, 1),
        OneDimenType(31, 2, 1),
        OneDimenType(32, 2, 1),
        OneDimenType(33, 2, 1),
        OneDimenType(34, 2, 1),
    ],
)

_SKI_CONFIG = SportRecordConfig(
    it_summary_types=[
        OneDimenType(59, 4, 3), OneDimenType(60, 4, 3),
        OneDimenType(61, 2, 3), OneDimenType(62, 2, 3), OneDimenType(63, 1, 3),
    ],
    four_dimen_types=[
        FourDimenType(TYPE_CALORIES, 1, 1),
        FourDimenType(TYPE_HR, 1, 1),
        FourDimenType(TYPE_HEIGHT_VALUE, 4, 4),
        FourDimenType(TYPE_DISTANCE_DOUBLE, 2, 4),
        FourDimenType(TYPE_HEIGHT_CHANGE_SIGN, 1, 1, high_start_bit=7, high_bit_count=1, max_support_version=3),
        FourDimenType(TYPE_DISTANCE, 1, 1, max_support_version=3),
        FourDimenType(TYPE_SPEED, 2, 2),
    ],
    pause_init_types=[OneDimenType(-2, 1, 1), OneDimenType(0, 4, 1)],
)

_OUTDOOR_STEP_CONFIG = SportRecordConfig(
    it_summary_types=[
        OneDimenType(TYPE_IT_STATE, 1, 1), OneDimenType(43, 4, 3),
        OneDimenType(TYPE_IT_TOTAL_DURATION, 4, 7),
        OneDimenType(54, 4, 6), OneDimenType(55, 2, 6),
    ],
    four_dimen_types=[
        FourDimenType(TYPE_CALORIES, 1, 1, high_start_bit=4, high_bit_count=4),
        FourDimenType(TYPE_HR, 1, 1),
        FourDimenType(TYPE_HEIGHT_VALUE, 4, 9),
        FourDimenType(TYPE_INTEGER_KM, 2, 9, high_start_bit=15, high_bit_count=1),
        FourDimenType(TYPE_INTEGER_KM, 1, 1, high_start_bit=7, high_bit_count=1),
        FourDimenType(TYPE_DISTANCE, 1, 1),
        FourDimenType(TYPE_STRIDE, 1, 2),
        FourDimenType(TYPE_LANDING_IMPACT, 4, 4, high_start_bit=26, high_bit_count=6),
        FourDimenType(TYPE_TOUCHDOWN_AIR_RATIO, 1, 5),
        FourDimenType(TYPE_CADENCE, 1, 5),
        FourDimenType(TYPE_PACE, 2, 5),
        FourDimenType(56, 2, 6),
        FourDimenType(TYPE_RUNNING_POWER, 2, 6),
        FourDimenType(79, 2, 8),
        FourDimenType(80, 2, 8),
    ],
    pause_init_types=[OneDimenType(0, 4, 1)],
)

_OUTDOOR_NO_STEP_CONFIG = SportRecordConfig(
    it_summary_types=[
        OneDimenType(TYPE_IT_STATE, 1, 1), OneDimenType(43, 4, 2),
        OneDimenType(TYPE_IT_TOTAL_DURATION, 4, 5), OneDimenType(58, 2, 4),
    ],
    four_dimen_types=[
        FourDimenType(TYPE_CALORIES, 1, 1, high_start_bit=4, high_bit_count=4),
        FourDimenType(TYPE_HR, 1, 1),
        FourDimenType(TYPE_HEIGHT_VALUE, 4, 6),
        FourDimenType(TYPE_INTEGER_KM, 2, 6, high_start_bit=15, high_bit_count=1),
        FourDimenType(TYPE_INTEGER_KM, 1, 1, high_start_bit=7, high_bit_count=1),
        FourDimenType(TYPE_DISTANCE, 1, 1),
        FourDimenType(TYPE_SPEED, 2, 3),
        FourDimenType(TYPE_CYCLE_CADENCE, 1, 3),
    ],
    pause_init_types=[OneDimenType(0, 4, 1)],
)

_ROCK_CLIMBING_CONFIG = SportRecordConfig(
    four_dimen_types=[
        FourDimenType(TYPE_HR, 1, 1),
        FourDimenType(TYPE_CALORIES, 1, 1),
        FourDimenType(TYPE_HEIGHT_CHANGE_SIGN, 1, 1, high_start_bit=7, high_bit_count=1),
        FourDimenType(TYPE_HEIGHT_VALUE, 4, 2),
    ],
    pause_init_types=[OneDimenType(0, 4, 1)],
)

_DIVING_IT_DEP = (64, frozenset({1}))

_DIVING_CONFIG = SportRecordConfig(
    it_summary_types=[
        OneDimenType(64, 1, 1),
        OneDimenType(65, 4, 1, depends_on=_DIVING_IT_DEP),
        OneDimenType(66, 4, 1, depends_on=_DIVING_IT_DEP),
        OneDimenType(67, 2, 1, depends_on=_DIVING_IT_DEP),
        OneDimenType(68, 2, 1, depends_on=_DIVING_IT_DEP),
        OneDimenType(69, 2, 1, depends_on=_DIVING_IT_DEP),
        OneDimenType(75, 2, 2, depends_on=_DIVING_IT_DEP),
        OneDimenType(76, 2, 2, depends_on=_DIVING_IT_DEP),
        OneDimenType(77, 2, 2, depends_on=_DIVING_IT_DEP),
    ],
    four_dimen_types=[
        FourDimenType(70, 1, 1),
        FourDimenType(71, 2, 1),
        FourDimenType(72, 2, 1),
        FourDimenType(73, 2, 1, high_start_bit=14, high_bit_count=2),
    ],
)


# ---------------------------------------------------------------------------
# Sport parser dispatch table
# ---------------------------------------------------------------------------

_SPORT_CONFIG: dict[int, SportRecordConfig] = {
    1: _OUTDOOR_SPORT_CONFIG,
    2: _OUTDOOR_SPORT_CONFIG,
    3: _INDOOR_RUN_CONFIG,
    4: _OUTDOOR_SPORT_CONFIG,
    5: _OUTDOOR_SPORT_CONFIG,
    6: _OUTDOOR_BIKING_CONFIG,
    7: _INDOOR_BIKING_CONFIG,
    8: _FREE_TRAINING_CONFIG,
    9: _SWIMMING_CONFIG,
    10: _SWIMMING_CONFIG,
    11: _ELLIPTICAL_CONFIG,
    12: _FREE_TRAINING_CONFIG,
    13: _ROWING_CONFIG,
    14: _ROPE_SKIPPING_CONFIG,
    15: _OUTDOOR_SPORT_CONFIG,
    16: _FREE_TRAINING_CONFIG,
    17: _TRIATHLON_CONFIG,
    18: _ORDINARY_BALL_CONFIG,
    19: _BASKETBALL_CONFIG,
    20: _GOLF_CONFIG,
    21: _SKI_CONFIG,
    22: _OUTDOOR_STEP_CONFIG,
    23: _OUTDOOR_NO_STEP_CONFIG,
    24: _ROCK_CLIMBING_CONFIG,
    25: _DIVING_CONFIG,
    28: _FREE_TRAINING_CONFIG,
}


def parse_free_training_record(header: FdsHeader) -> list[SportSample]:
    """Parse a FreeTraining (strength / HIIT / yoga) sport record binary."""
    return _parse_with_config(header, _FREE_TRAINING_CONFIG)


def parse_sport_record(decrypted: bytes, sport_type: int) -> list[SportSample]:
    """Parse decrypted FDS sport record binary for the given sport_type."""
    if len(decrypted) < _SPORT_SERVER_DATA_ID_LEN + 1:
        logger.warning("Decrypted data too short to read header version byte")
        return []

    version = decrypted[5]
    data_valid_len = get_record_data_valid_len(sport_type, version)
    if data_valid_len is None:
        logger.info(
            "No dataValid mapping for sport_type=%d version=%d; skipping FDS parse",
            sport_type, version,
        )
        return []

    header = parse_fds_header(decrypted, data_valid_len)

    config = _SPORT_CONFIG.get(sport_type)
    if config is None:
        logger.info("No parser for sport_type=%d; skipping FDS parse", sport_type)
        return []

    return _parse_with_config(header, config)


# ---------------------------------------------------------------------------
# Full download → decrypt → parse pipeline
# ---------------------------------------------------------------------------


def download_and_parse_sport_record(
    session: requests.Session,
    fds_entry: dict[str, Any],
    sport_type: int,
    *,
    timeout: int = 30,
) -> list[SportSample]:
    """Download, decrypt, and parse a sport record from an FDS entry.

    *fds_entry* must contain ``url`` (str) and ``obj_key`` (str).
    *sport_type* is the proto_type from ``SportBasicReport``.

    Returns per-second :class:`SportSample` list, or empty list on failure.
    """
    url = fds_entry.get("url")
    object_key = fds_entry.get("obj_key")
    if not isinstance(url, str) or not isinstance(object_key, str):
        logger.debug("FDS entry missing url or obj_key")
        return []

    try:
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException:
        logger.warning("Failed to download FDS sport record from %s", url, exc_info=True)
        return []

    try:
        decrypted = decrypt_fds_data(resp.text, object_key)
    except Exception:
        logger.warning("Failed to decrypt FDS sport record", exc_info=True)
        return []

    try:
        return parse_sport_record(decrypted, sport_type)
    except Exception:
        logger.warning("Failed to parse FDS sport record binary", exc_info=True)
        return []


# ===========================================================================
# GPS record parsing (from decompiled SportGpsParser.java)
# ===========================================================================

# ---------------------------------------------------------------------------
# GPS validity length (from FitnessDataValidity.getSportGpsValidityLen)
# ---------------------------------------------------------------------------

_GPS_VALIDITY: dict[int, int] = {1: 1, 2: 1, 3: 1, 4: 1}


def get_gps_data_valid_len(version: int) -> int | None:
    """Return GPS dataValid byte length, or None if version unsupported."""
    return _GPS_VALIDITY.get(version)


# ---------------------------------------------------------------------------
# GPS data types (from SportGpsParser.dataTypeArray)
# ---------------------------------------------------------------------------

GPS_TYPE_TIME = 0
GPS_TYPE_LONGITUDE = 1
GPS_TYPE_LATITUDE = 2
GPS_TYPE_ACCURACY = 3
GPS_TYPE_SPEED = 4
GPS_TYPE_GPS_SOURCE = 5
GPS_TYPE_ALTITUDE = 6
GPS_TYPE_HDOP = 7

_GPS_DATA_TYPES: list[OneDimenType] = [
    OneDimenType(type_id=GPS_TYPE_TIME, byte_count=4, support_version=1),
    OneDimenType(type_id=GPS_TYPE_LONGITUDE, byte_count=4, support_version=1),
    OneDimenType(type_id=GPS_TYPE_LATITUDE, byte_count=4, support_version=1),
    OneDimenType(type_id=GPS_TYPE_ACCURACY, byte_count=4, support_version=2),
    OneDimenType(type_id=GPS_TYPE_SPEED, byte_count=2, support_version=2),
    OneDimenType(type_id=GPS_TYPE_GPS_SOURCE, byte_count=0, support_version=2),
    OneDimenType(type_id=GPS_TYPE_ALTITUDE, byte_count=4, support_version=3),
    OneDimenType(type_id=GPS_TYPE_HDOP, byte_count=4, support_version=3),
]

# Float-type IDs in the GPS schema (read via struct float instead of uint)
_GPS_FLOAT_TYPES = frozenset({
    GPS_TYPE_LONGITUDE, GPS_TYPE_LATITUDE, GPS_TYPE_ACCURACY,
    GPS_TYPE_ALTITUDE, GPS_TYPE_HDOP,
})


# ---------------------------------------------------------------------------
# GpsSample dataclass
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class GpsSample:
    """One GPS point from an FDS GPS binary."""

    timestamp: int
    latitude: float
    longitude: float
    accuracy: float | None = None
    speed: float | None = None
    gps_source: int | None = None
    altitude: float | None = None
    hdop: float | None = None


# ---------------------------------------------------------------------------
# GPS record reading (flat OneDimen, with float fields)
# ---------------------------------------------------------------------------


def _read_gps_field(
    buf: memoryview | bytes, offset: int, dt: OneDimenType,
) -> tuple[int | float, int]:
    """Read a single GPS field. Returns (value, new_offset).

    Float types are decoded as IEEE 754 LE float32.
    """
    if dt.byte_count == 0:
        return 0, offset
    if dt.type_id in _GPS_FLOAT_TYPES and dt.byte_count == 4:
        return struct.unpack_from("<f", buf, offset)[0], offset + 4
    return _read_uint(buf, offset, dt.byte_count)


def _min_gps_record_bytes(version: int) -> int:
    """Minimum bytes for one GPS record at the given version."""
    return sum(
        dt.byte_count for dt in _GPS_DATA_TYPES
        if dt.support_version <= version and dt.depends_on is None
    )


def _parse_gps_records(
    buf: memoryview | bytes,
    offset: int,
    record_count: int,
    version: int,
    valid_map: dict[int, bool],
) -> tuple[list[GpsSample], int]:
    """Parse *record_count* GPS records from *buf*. Returns (samples, new_offset)."""
    min_bytes = _min_gps_record_bytes(version)
    samples: list[GpsSample] = []

    for _ in range(record_count):
        if offset + min_bytes > len(buf):
            break

        raw: dict[int, int | float] = {}
        for dt in _GPS_DATA_TYPES:
            if dt.support_version > version:
                continue
            if dt.byte_count == 0:
                # Virtual field (gpsSource) — derived later from speed
                continue
            if offset + dt.byte_count > len(buf):
                return samples, offset
            value, offset = _read_gps_field(buf, offset, dt)
            if valid_map.get(dt.type_id, False):
                raw[dt.type_id] = value

        timestamp_val = raw.get(GPS_TYPE_TIME)
        lon_val = raw.get(GPS_TYPE_LONGITUDE)
        lat_val = raw.get(GPS_TYPE_LATITUDE)
        if timestamp_val is None or lon_val is None or lat_val is None:
            continue

        sample = GpsSample(
            timestamp=int(timestamp_val),
            longitude=float(lon_val),
            latitude=float(lat_val),
        )

        acc_val = raw.get(GPS_TYPE_ACCURACY)
        if acc_val is not None:
            sample.accuracy = float(acc_val)

        speed_raw = raw.get(GPS_TYPE_SPEED)
        if speed_raw is not None:
            int_speed = int(speed_raw)
            # Upper 12 bits / 10.0 = speed; lower 4 bits = gpsSource
            sample.speed = ((int_speed & 0xFFF0) >> 4) / 10.0
            if valid_map.get(GPS_TYPE_GPS_SOURCE, False):
                sample.gps_source = int_speed & 0x0F

        alt_val = raw.get(GPS_TYPE_ALTITUDE)
        if alt_val is not None:
            sample.altitude = float(alt_val)

        hdop_val = raw.get(GPS_TYPE_HDOP)
        if hdop_val is not None:
            sample.hdop = float(hdop_val)

        samples.append(sample)

    return samples, offset


# ---------------------------------------------------------------------------
# GPS record parsing entry point
# ---------------------------------------------------------------------------


def parse_gps_record(decrypted: bytes) -> list[GpsSample]:
    """Parse decrypted FDS GPS binary into GPS samples.

    The GPS binary uses the same FDS header structure as sport records but
    the body is a flat list of OneDimen records (no segment/pause structure).
    Version >= 4 has a record-count header and optional TGC data appended.
    """
    if len(decrypted) < _SPORT_SERVER_DATA_ID_LEN + 1:
        logger.warning("GPS data too short to read header version byte")
        return []

    version = decrypted[5]
    data_valid_len = get_gps_data_valid_len(version)
    if data_valid_len is None:
        logger.info("No GPS dataValid for version=%d; skipping GPS parse", version)
        return []

    header = parse_fds_header(decrypted, data_valid_len)
    valid_map = _parse_one_dimen_valid(_GPS_DATA_TYPES, version, header.data_valid)

    # Required fields must be valid
    if not (valid_map.get(GPS_TYPE_TIME) and valid_map.get(GPS_TYPE_LONGITUDE)
            and valid_map.get(GPS_TYPE_LATITUDE)):
        logger.warning("GPS validity missing required time/lat/lon fields")
        return []

    buf = memoryview(header.body_data)
    offset = 0

    if version >= 4:
        # v4+: record_count (4B LE) + records + featureType(1B) + tgcSize(4B) [+ tgcData]
        if len(buf) < 4:
            return []
        record_count, offset = _read_uint(buf, offset, 4)
        samples, offset = _parse_gps_records(buf, offset, record_count, version, valid_map)
        # Skip featureType + tgcSize + tgcData (not needed for our purposes)
    else:
        # v1-3: flat loop until buffer exhausted
        min_bytes = _min_gps_record_bytes(version)
        if min_bytes == 0:
            return []
        record_count = len(buf) // min_bytes
        samples, _ = _parse_gps_records(buf, offset, record_count, version, valid_map)

    return samples


# ---------------------------------------------------------------------------
# GPS download → decrypt → parse pipeline
# ---------------------------------------------------------------------------


def download_and_parse_gps_record(
    session: requests.Session,
    fds_entry: dict[str, Any],
    *,
    timeout: int = 30,
) -> list[GpsSample]:
    """Download, decrypt, and parse a GPS record from an FDS entry.

    Returns :class:`GpsSample` list, or empty list on failure.
    """
    url = fds_entry.get("url")
    object_key = fds_entry.get("obj_key")
    if not isinstance(url, str) or not isinstance(object_key, str):
        logger.debug("FDS GPS entry missing url or obj_key")
        return []

    try:
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException:
        logger.warning("Failed to download FDS GPS record from %s", url, exc_info=True)
        return []

    try:
        decrypted = decrypt_fds_data(resp.text, object_key)
    except Exception:
        logger.warning("Failed to decrypt FDS GPS record", exc_info=True)
        return []

    try:
        return parse_gps_record(decrypted)
    except Exception:
        logger.warning("Failed to parse FDS GPS binary", exc_info=True)
        return []
