from __future__ import annotations

import logging
import struct
from dataclasses import dataclass
from typing import Any

import requests

from mi_fitness_sync.fds.cache import FdsCache
from mi_fitness_sync.fds.common import (
    SPORT_SERVER_DATA_ID_LEN,
    OneDimenType,
    parse_one_dimen_valid,
    read_uint,
    parse_fds_header,
)
from mi_fitness_sync.fds.downloader import download_and_parse_fds_file


logger = logging.getLogger(__name__)

_GPS_VALIDITY: dict[int, int] = {1: 1, 2: 1, 3: 1, 4: 1}

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

_GPS_FLOAT_TYPES = frozenset(
    {GPS_TYPE_LONGITUDE, GPS_TYPE_LATITUDE, GPS_TYPE_ACCURACY, GPS_TYPE_ALTITUDE, GPS_TYPE_HDOP}
)


def get_gps_data_valid_len(version: int) -> int | None:
    return _GPS_VALIDITY.get(version)


@dataclass(slots=True)
class GpsSample:
    timestamp: int
    latitude: float
    longitude: float
    accuracy: float | None = None
    speed: float | None = None
    gps_source: int | None = None
    altitude: float | None = None
    hdop: float | None = None


def _read_gps_field(buf: memoryview | bytes, offset: int, data_type: OneDimenType) -> tuple[int | float, int]:
    if data_type.byte_count == 0:
        return 0, offset
    if data_type.type_id in _GPS_FLOAT_TYPES and data_type.byte_count == 4:
        return struct.unpack_from("<f", buf, offset)[0], offset + 4
    return read_uint(buf, offset, data_type.byte_count)


def min_gps_record_bytes(version: int) -> int:
    return sum(
        data_type.byte_count
        for data_type in _GPS_DATA_TYPES
        if data_type.support_version <= version and data_type.depends_on is None
    )


def parse_gps_records(
    buf: memoryview | bytes,
    offset: int,
    record_count: int,
    version: int,
    valid_map: dict[int, bool],
) -> tuple[list[GpsSample], int]:
    min_bytes = min_gps_record_bytes(version)
    samples: list[GpsSample] = []

    for _ in range(record_count):
        if offset + min_bytes > len(buf):
            break

        raw: dict[int, int | float] = {}
        for data_type in _GPS_DATA_TYPES:
            if data_type.support_version > version:
                continue
            if data_type.byte_count == 0:
                continue
            if offset + data_type.byte_count > len(buf):
                return samples, offset
            value, offset = _read_gps_field(buf, offset, data_type)
            if valid_map.get(data_type.type_id, False):
                raw[data_type.type_id] = value

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
        accuracy = raw.get(GPS_TYPE_ACCURACY)
        if accuracy is not None:
            sample.accuracy = float(accuracy)

        speed_raw = raw.get(GPS_TYPE_SPEED)
        if speed_raw is not None:
            int_speed = int(speed_raw)
            sample.speed = ((int_speed & 0xFFF0) >> 4) / 10.0
            if valid_map.get(GPS_TYPE_GPS_SOURCE, False):
                sample.gps_source = int_speed & 0x0F

        altitude = raw.get(GPS_TYPE_ALTITUDE)
        if altitude is not None:
            sample.altitude = float(altitude)

        hdop = raw.get(GPS_TYPE_HDOP)
        if hdop is not None:
            sample.hdop = float(hdop)

        samples.append(sample)

    return samples, offset


def parse_gps_record(decrypted: bytes) -> list[GpsSample]:
    if len(decrypted) < SPORT_SERVER_DATA_ID_LEN + 1:
        logger.warning("GPS data too short to read header version byte")
        return []

    version = decrypted[5]
    data_valid_len = get_gps_data_valid_len(version)
    if data_valid_len is None:
        logger.info("No GPS dataValid for version=%d; skipping GPS parse", version)
        return []

    header = parse_fds_header(decrypted, data_valid_len)
    valid_map = parse_one_dimen_valid(_GPS_DATA_TYPES, version, header.data_valid)
    if not (
        valid_map.get(GPS_TYPE_TIME)
        and valid_map.get(GPS_TYPE_LONGITUDE)
        and valid_map.get(GPS_TYPE_LATITUDE)
    ):
        logger.warning("GPS validity missing required time/lat/lon fields")
        return []

    buf = memoryview(header.body_data)
    offset = 0
    if version >= 4:
        if len(buf) < 4:
            return []
        record_count, offset = read_uint(buf, offset, 4)
        samples, _ = parse_gps_records(buf, offset, record_count, version, valid_map)
        return samples

    min_bytes = min_gps_record_bytes(version)
    if min_bytes == 0:
        return []
    record_count = len(buf) // min_bytes
    samples, _ = parse_gps_records(buf, offset, record_count, version, valid_map)
    return samples


def download_and_parse_gps_record(
    session: requests.Session,
    fds_entry: dict[str, Any],
    *,
    timeout: int = 30,
    cache: FdsCache | None = None,
    cache_key: str | None = None,
) -> list[GpsSample]:
    return download_and_parse_fds_file(
        session,
        fds_entry,
        parse_gps_record,
        lambda: [],
        timeout=timeout,
        cache=cache,
        cache_key=cache_key,
        entry_label="GPS record",
        download_label="FDS GPS record",
        decrypt_label="FDS GPS record",
        parse_label="FDS GPS binary",
    )