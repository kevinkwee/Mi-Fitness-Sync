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
from dataclasses import dataclass
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
TYPE_DISTANCE = 9
TYPE_PACE = 12
TYPE_SPO2 = 38
TYPE_STRESS = 39
TYPE_IT_STATE = 41
TYPE_IT_TOTAL_DURATION = 78
TYPE_CADENCE = 49
TYPE_CYCLE_CADENCE = 50
TYPE_SPEED = 51
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
_BIKING_IN_RECORD_VALIDITY: dict[int, int] = {1: 2, 2: 2}
_SWIMMING_RECORD_VALIDITY: dict[int, int] = {1: 2, 2: 2, 3: 3}
_ELLIPTICAL_RECORD_VALIDITY: dict[int, int] = {1: 1, 2: 1}
_ROWING_RECORD_VALIDITY: dict[int, int] = {1: 1, 2: 1, 3: 2}
_ROPE_SKIPPING_RECORD_VALIDITY: dict[int, int] = {1: 2, 2: 2}
_NO_STEP_RECORD_VALIDITY: dict[int, int] = {1: 2, 2: 2, 3: 3, 4: 3, 5: 3, 6: 3}
_STEP_RECORD_VALIDITY: dict[int, int] = {1: 2, 2: 3, 3: 3, 4: 3, 5: 5, 6: 6, 7: 6, 8: 7, 9: 7}
_TRIATHLON_RECORD_VALIDITY: dict[int, int] = {1: 0, 2: 0}

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
    22: _STEP_RECORD_VALIDITY,             # step_sport
    23: _NO_STEP_RECORD_VALIDITY,          # no_step_sport
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


# FreeTraining record types (OneDimen, version < 3)
_FREE_TRAINING_RECORD_TYPES = [
    OneDimenType(TYPE_HR, 1, 1),
    OneDimenType(TYPE_CALORIES, 1, 1),
]

# FreeTraining IT summary types
_FREE_TRAINING_IT_SUMMARY_TYPES = [
    OneDimenType(TYPE_IT_STATE, 1, 2),
    OneDimenType(TYPE_IT_TOTAL_DURATION, 4, 4),
    OneDimenType(TYPE_GYM_ACTION_TIMES, 2, 5),
    OneDimenType(TYPE_GYM_ACTION_WEIGHT, 2, 5),
    OneDimenType(TYPE_GYM_ACTION_ID, 2, 5),
]


# ---------------------------------------------------------------------------
# FourDimen data type definitions (from decompiled FreeTrainingRecordParser)
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class FourDimenType:
    type_id: int
    byte_size: int
    support_version: int


# FreeTraining record types (FourDimen, version >= 3)
_FREE_TRAINING_FOURDIMEN_TYPES = [
    FourDimenType(TYPE_HR, 1, 3),
    FourDimenType(TYPE_CALORIES, 1, 3),
    FourDimenType(TYPE_SPO2, 1, 3),
    FourDimenType(TYPE_STRESS, 1, 3),
]


# ---------------------------------------------------------------------------
# OneDimen validity parsing
# ---------------------------------------------------------------------------


def _parse_one_dimen_valid(
    data_types: list[OneDimenType], version: int, data_valid: bytes,
) -> dict[int, bool]:
    """Parse OneDimen validity bitmap.  1 bit per supported type, MSB-first."""
    valid_map: dict[int, bool] = {}
    bit_index = 0
    for dt in data_types:
        if dt.type_id < 0:
            continue
        if dt.support_version > version:
            valid_map[dt.type_id] = False
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
    """Parse FourDimen validity nibbles.  4 bits per supported type."""
    valid_map: dict[int, FourDimenValid] = {}
    nibble_index = 0
    for dt in data_types:
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


_TYPE_TO_ATTR: dict[int, str] = {
    TYPE_HR: "heart_rate",
    TYPE_CALORIES: "calories",
    TYPE_SPO2: "spo2",
    TYPE_STRESS: "stress",
    TYPE_STEPS: "steps",
    TYPE_DISTANCE: "distance",
    TYPE_SPEED: "speed",
    TYPE_CADENCE: "cadence",
    TYPE_PACE: "pace",
}


def _record_to_sample(timestamp: int, record: dict[int, int]) -> SportSample:
    sample = SportSample(timestamp=timestamp)
    for type_id, value in record.items():
        attr = _TYPE_TO_ATTR.get(type_id)
        if attr is not None:
            setattr(sample, attr, value)
    return sample


# ---------------------------------------------------------------------------
# IT summary reading (sequential OneDimen read, all supported types valid)
# ---------------------------------------------------------------------------


def _it_summary_byte_count(types: list[OneDimenType], version: int) -> int:
    return sum(t.byte_count for t in types if t.type_id >= 0 and t.support_version <= version)


def _read_it_summary(
    buf: memoryview | bytes, offset: int, types: list[OneDimenType], version: int,
) -> tuple[dict[int, int], int]:
    """Read IT summary data (one record, all supported types treated as valid)."""
    result: dict[int, int] = {}
    for t in types:
        if t.type_id < 0 or t.support_version > version:
            continue
        if offset + t.byte_count > len(buf):
            break
        value, offset = _read_uint(buf, offset, t.byte_count)
        result[t.type_id] = value
    return result, offset


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
    """Parse *record_count* OneDimen records.  Returns (records, new_offset)."""
    records: list[dict[int, int]] = []
    for _ in range(record_count):
        rec: dict[int, int] = {}
        for dt in data_types:
            if dt.type_id < 0 or dt.support_version > version:
                continue
            if offset + dt.byte_count > len(buf):
                return records, offset
            value, offset = _read_uint(buf, offset, dt.byte_count)
            if valid_map.get(dt.type_id, False):
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
            # For FreeTraining: only "high" sub-value defined (full byte = value)
            if dv.high:
                rec[dt.type_id] = value
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
) -> list[SportSample]:
    """Parse OneDimen sport record body into per-second samples."""
    valid_map = _parse_one_dimen_valid(record_types, version, data_valid)
    it_bytes = _it_summary_byte_count(it_summary_types, version)
    min_segment = 8 + it_bytes  # recordCount(4) + startTime(4) + IT summary

    samples: list[SportSample] = []
    offset = 0
    buf = memoryview(body)

    while offset + min_segment <= len(buf):
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
) -> list[SportSample]:
    """Parse FourDimen sport record body into per-second samples."""
    valid_map = _parse_four_dimen_valid(record_types, version, data_valid)
    it_bytes = _it_summary_byte_count(it_summary_types, version)
    min_segment = 8 + it_bytes  # recordCount(4) + startTime(4) + IT summary

    samples: list[SportSample] = []
    offset = 0
    buf = memoryview(body)

    while offset + min_segment <= len(buf):
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
# Free-training parser dispatch (from FreeTrainingRecordParser.java)
# ---------------------------------------------------------------------------


def parse_free_training_record(header: FdsHeader) -> list[SportSample]:
    """Parse a FreeTraining (strength / HIIT / yoga) sport record binary."""
    if header.version >= 3:
        return _parse_body_four_dimen(
            header.body_data,
            header.data_valid,
            header.version,
            _FREE_TRAINING_FOURDIMEN_TYPES,
            _FREE_TRAINING_IT_SUMMARY_TYPES,
        )
    return _parse_body_one_dimen(
        header.body_data,
        header.data_valid,
        header.version,
        _FREE_TRAINING_RECORD_TYPES,
        _FREE_TRAINING_IT_SUMMARY_TYPES,
    )


# ---------------------------------------------------------------------------
# Top-level sport record parser dispatch (from FitnessDataParser.java)
# Maps sport_type (= proto_type from report) → parser function
# ---------------------------------------------------------------------------

_SPORT_PARSER: dict[int, Any] = {
    8: parse_free_training_record,   # free_training
    12: parse_free_training_record,  # yoga
    16: parse_free_training_record,  # HIIT
    28: parse_free_training_record,  # strength_training
}


def parse_sport_record(decrypted: bytes, sport_type: int) -> list[SportSample]:
    """Parse decrypted FDS sport record binary for the given sport_type.

    *sport_type* is the proto_type from ``SportBasicReport``, NOT the
    category-level sport type.

    Returns a list of per-second :class:`SportSample` instances, or an empty
    list if the sport type / version is unsupported.
    """
    # Determine version from header byte [5] to look up dataValid length.
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

    parser = _SPORT_PARSER.get(sport_type)
    if parser is None:
        logger.info("No parser for sport_type=%d; skipping FDS parse", sport_type)
        return []

    return parser(header)


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
