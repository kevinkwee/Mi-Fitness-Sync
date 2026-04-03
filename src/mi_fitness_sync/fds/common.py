from __future__ import annotations

import base64
import logging
import struct
from dataclasses import dataclass

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad


logger = logging.getLogger(__name__)

_AES_IV = b"1234567887654321"
SPORT_SERVER_DATA_ID_LEN = 7

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


def b64url_decode(value: str) -> bytes:
    remainder = len(value) % 4
    if remainder:
        value += "=" * (4 - remainder)
    return base64.urlsafe_b64decode(value)


def decrypt_fds_data(response_body: str, object_key: str) -> bytes:
    key = b64url_decode(object_key)
    ciphertext = b64url_decode(response_body)
    cipher = AES.new(key, AES.MODE_CBC, _AES_IV)
    return unpad(cipher.decrypt(ciphertext), AES.block_size)


@dataclass(slots=True)
class FdsHeader:
    timestamp: int
    tz_in_15min: int
    version: int
    sport_type: int
    data_valid: bytes
    body_data: bytes


def parse_fds_header(data: bytes, data_valid_len: int) -> FdsHeader:
    header_len = SPORT_SERVER_DATA_ID_LEN + 1 + data_valid_len
    if len(data) < header_len:
        raise ValueError(
            f"Decrypted data too short ({len(data)} bytes) for expected header ({header_len} bytes)"
        )

    timestamp = struct.unpack_from("<I", data, 0)[0]
    tz_in_15min = data[4]
    version = data[5]
    sport_type = data[6]
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

_SPORT_RECORD_VALIDITY: dict[int, dict[int, int]] = {
    1: _OUTDOOR_RECORD_VALIDITY,
    2: _OUTDOOR_RECORD_VALIDITY,
    3: _RUNNING_IN_RECORD_VALIDITY,
    4: _OUTDOOR_RECORD_VALIDITY,
    5: _OUTDOOR_RECORD_VALIDITY,
    6: _BIKING_OUT_RECORD_VALIDITY,
    7: _BIKING_IN_RECORD_VALIDITY,
    8: _FREE_TRAINING_RECORD_VALIDITY,
    9: _SWIMMING_RECORD_VALIDITY,
    10: _SWIMMING_RECORD_VALIDITY,
    11: _ELLIPTICAL_RECORD_VALIDITY,
    12: _FREE_TRAINING_RECORD_VALIDITY,
    13: _ROWING_RECORD_VALIDITY,
    14: _ROPE_SKIPPING_RECORD_VALIDITY,
    15: _OUTDOOR_RECORD_VALIDITY,
    16: _FREE_TRAINING_RECORD_VALIDITY,
    17: _TRIATHLON_RECORD_VALIDITY,
    18: _ORDINARY_BALL_RECORD_VALIDITY,
    19: _BASKETBALL_RECORD_VALIDITY,
    20: _GOLF_RECORD_VALIDITY,
    21: _SKI_RECORD_VALIDITY,
    22: _STEP_RECORD_VALIDITY,
    23: _NO_STEP_RECORD_VALIDITY,
    24: _ROCK_CLIMBING_RECORD_VALIDITY,
    25: _DIVING_RECORD_VALIDITY,
    28: _FREE_TRAINING_RECORD_VALIDITY,
}


def get_record_data_valid_len(sport_type: int, version: int) -> int | None:
    version_map = _SPORT_RECORD_VALIDITY.get(sport_type)
    if version_map is None:
        return None
    return version_map.get(version)


@dataclass(slots=True, frozen=True)
class OneDimenType:
    type_id: int
    byte_count: int
    support_version: int
    depends_on: tuple[int, frozenset[int]] | None = None


@dataclass(slots=True, frozen=True)
class FourDimenType:
    type_id: int
    byte_size: int
    support_version: int
    high_start_bit: int | None = None
    high_bit_count: int | None = None
    max_support_version: int | None = None


@dataclass(slots=True, frozen=True)
class FourDimenValid:
    exist: bool
    high: bool
    middle: bool
    low: bool


def parse_one_dimen_valid(
    data_types: list[OneDimenType],
    version: int,
    data_valid: bytes,
) -> dict[int, bool]:
    valid_map: dict[int, bool] = {}
    bit_index = 0
    for data_type in data_types:
        if data_type.type_id < 0:
            continue
        if data_type.support_version > version:
            valid_map[data_type.type_id] = False
            continue
        if not data_valid:
            valid_map[data_type.type_id] = True
            continue
        byte_idx = bit_index // 8
        bit_idx = bit_index % 8
        if byte_idx >= len(data_valid):
            raise ValueError(f"dataValid too short: need byte {byte_idx}, have {len(data_valid)}")
        valid_map[data_type.type_id] = bool(data_valid[byte_idx] & (1 << (7 - bit_idx)))
        bit_index += 1
    return valid_map


def parse_four_dimen_valid(
    data_types: list[FourDimenType],
    version: int,
    data_valid: bytes,
) -> dict[int, FourDimenValid]:
    valid_map: dict[int, FourDimenValid] = {}
    nibble_index = 0
    for data_type in data_types:
        if data_type.max_support_version is not None and version > data_type.max_support_version:
            valid_map[data_type.type_id] = FourDimenValid(False, False, False, False)
            continue
        if data_type.support_version > version:
            valid_map[data_type.type_id] = FourDimenValid(False, False, False, False)
            continue
        byte_idx = nibble_index // 2
        if byte_idx >= len(data_valid):
            raise ValueError(f"dataValid too short: need byte {byte_idx}, have {len(data_valid)}")
        nibble = (data_valid[byte_idx] & 0xF0) >> 4 if nibble_index % 2 == 0 else data_valid[byte_idx] & 0x0F
        valid_map[data_type.type_id] = FourDimenValid(
            exist=bool(nibble & 0x8),
            high=bool(nibble & 0x4),
            middle=bool(nibble & 0x2),
            low=bool(nibble & 0x1),
        )
        nibble_index += 1
    return valid_map


def read_uint(data: memoryview | bytes, offset: int, size: int) -> tuple[int, int]:
    if size == 1:
        return data[offset], offset + 1
    if size == 2:
        return struct.unpack_from("<H", data, offset)[0], offset + 2
    if size == 4:
        return struct.unpack_from("<I", data, offset)[0], offset + 4
    raise ValueError(f"Unsupported read size {size}")


def extract_high_value(raw_value: int, data_type: FourDimenType) -> int:
    if data_type.high_start_bit is not None and data_type.high_bit_count is not None:
        return (raw_value >> data_type.high_start_bit) & ((1 << data_type.high_bit_count) - 1)
    return raw_value
