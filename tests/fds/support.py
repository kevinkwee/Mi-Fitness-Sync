from __future__ import annotations

import base64
import struct

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

AES_IV = b"1234567887654321"


def encrypt(plaintext: bytes, key: bytes) -> str:
    cipher = AES.new(key, AES.MODE_CBC, AES_IV)
    ciphertext = cipher.encrypt(pad(plaintext, AES.block_size))
    return base64.urlsafe_b64encode(ciphertext).decode("ascii").rstrip("=")


def b64url_encode_no_pad(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def make_aes_key(length: int = 16) -> bytes:
    return b"\xab" * length


def build_header(timestamp: int, tz: int, version: int, sport_type: int, data_valid: bytes) -> bytes:
    return struct.pack("<I", timestamp) + bytes([tz, version, sport_type]) + b"\x00" + data_valid


def build_one_dimen_segment(record_count: int, start_time: int, it_summary: bytes, records: bytes) -> bytes:
    return struct.pack("<II", record_count, start_time) + it_summary + records


def build_gps_v1_record(timestamp: int, longitude: float, latitude: float) -> bytes:
    return struct.pack("<Iff", timestamp, longitude, latitude)


def build_gps_v2_record(timestamp: int, longitude: float, latitude: float, accuracy: float, speed_raw: int) -> bytes:
    return struct.pack("<Iff", timestamp, longitude, latitude) + struct.pack("<f", accuracy) + struct.pack("<H", speed_raw)


def build_gps_v3_record(
    timestamp: int,
    longitude: float,
    latitude: float,
    accuracy: float,
    speed_raw: int,
    altitude: float,
    hdop: float,
) -> bytes:
    base = build_gps_v2_record(timestamp, longitude, latitude, accuracy, speed_raw)
    return base + struct.pack("<ff", altitude, hdop)


def build_gps_binary(timestamp: int, tz: int, version: int, sport_type: int, data_valid: bytes, body: bytes) -> bytes:
    return build_header(timestamp, tz, version, sport_type, data_valid) + body


def build_recovery_rate_body(
    rate_count: int,
    recover_timestamp: int,
    heart_rate: int,
    recover_rate_raw: int,
    rates: list[int],
) -> bytes:
    return (
        struct.pack("<H", rate_count)
        + struct.pack("<I", recover_timestamp)
        + bytes([heart_rate, recover_rate_raw])
        + bytes(rates)
    )