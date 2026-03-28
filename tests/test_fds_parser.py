"""Tests for mi_fitness_sync.fds_parser – AES decryption, header parsing, record parsing."""

from __future__ import annotations

import base64
import struct
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from mi_fitness_sync.fds_parser import (
    FdsHeader,
    FourDimenType,
    FourDimenValid,
    OneDimenType,
    SportRecordConfig,
    SportSample,
    TYPE_CALORIES,
    TYPE_DISTANCE,
    TYPE_HR,
    TYPE_INTEGER_KM,
    TYPE_SHOOT_COUNT,
    TYPE_SPO2,
    TYPE_STRESS,
    _b64url_decode,
    _extract_high_value,
    _it_summary_byte_count,
    _parse_four_dimen_records,
    _parse_four_dimen_valid,
    _parse_one_dimen_records,
    _parse_one_dimen_valid,
    _parse_with_config,
    _SPORT_CONFIG,
    decrypt_fds_data,
    download_and_parse_sport_record,
    get_record_data_valid_len,
    parse_fds_header,
    parse_free_training_record,
    parse_sport_record,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_AES_IV = b"1234567887654321"


def _encrypt(plaintext: bytes, key: bytes) -> str:
    """AES-CBC encrypt and return base64url-no-padding string."""
    cipher = AES.new(key, AES.MODE_CBC, _AES_IV)
    ct = cipher.encrypt(pad(plaintext, AES.block_size))
    return base64.urlsafe_b64encode(ct).decode("ascii").rstrip("=")


def _b64url_encode_no_pad(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _make_aes_key(length: int = 16) -> bytes:
    return b"\xab" * length


def _build_header(
    timestamp: int, tz: int, version: int, sport_type: int, data_valid: bytes,
) -> bytes:
    server_data_id = struct.pack("<I", timestamp) + bytes([tz, version, sport_type])
    return server_data_id + b"\x00" + data_valid


def _build_one_dimen_segment(
    record_count: int, start_time: int, it_summary: bytes, records: bytes,
) -> bytes:
    return struct.pack("<II", record_count, start_time) + it_summary + records


# ---------------------------------------------------------------------------
# b64url decode
# ---------------------------------------------------------------------------


class TestB64UrlDecode:
    def test_decode_with_padding(self):
        data = b"hello world"
        encoded = base64.urlsafe_b64encode(data).decode("ascii")
        assert _b64url_decode(encoded) == data

    def test_decode_without_padding(self):
        data = b"hello world"
        encoded = base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")
        assert _b64url_decode(encoded) == data


# ---------------------------------------------------------------------------
# AES decrypt
# ---------------------------------------------------------------------------


class TestDecryptFds:
    def test_round_trip(self):
        key = _make_aes_key()
        plaintext = b"This is a test of AES-CBC decryption"
        object_key = _b64url_encode_no_pad(key)
        encrypted = _encrypt(plaintext, key)
        result = decrypt_fds_data(encrypted, object_key)
        assert result == plaintext

    def test_decrypts_padded_data(self):
        key = _make_aes_key()
        plaintext = b"\x01" * 32  # exactly 2 blocks
        object_key = _b64url_encode_no_pad(key)
        encrypted = _encrypt(plaintext, key)
        result = decrypt_fds_data(encrypted, object_key)
        assert result == plaintext


# ---------------------------------------------------------------------------
# Header parsing
# ---------------------------------------------------------------------------


class TestParseFdsHeader:
    def test_parses_header_fields(self):
        data_valid = bytes([0b11000000])
        timestamp = 1774241243
        header_bytes = _build_header(timestamp, 28, 2, 8, data_valid)
        body = b"\x01\x02\x03\x04"
        raw = header_bytes + body

        result = parse_fds_header(raw, data_valid_len=1)

        assert result.timestamp == timestamp
        assert result.tz_in_15min == 28
        assert result.version == 2
        assert result.sport_type == 8
        assert result.data_valid == data_valid
        assert result.body_data == body

    def test_raises_on_short_data(self):
        with pytest.raises(ValueError, match="too short"):
            parse_fds_header(b"\x00" * 5, data_valid_len=1)


# ---------------------------------------------------------------------------
# DataValid length lookup
# ---------------------------------------------------------------------------


class TestGetRecordDataValidLen:
    def test_free_training_v2(self):
        assert get_record_data_valid_len(8, 2) == 1

    def test_free_training_v3(self):
        assert get_record_data_valid_len(8, 3) == 2

    def test_outdoor_run_v1(self):
        assert get_record_data_valid_len(1, 1) == 2

    def test_unknown_sport(self):
        assert get_record_data_valid_len(999, 1) is None

    def test_unknown_version(self):
        assert get_record_data_valid_len(8, 99) is None

    def test_strength_training_v2(self):
        """proto_type 28 (strength training) uses FreeTraining validity."""
        assert get_record_data_valid_len(28, 2) == 1

    def test_strength_training_v3(self):
        """proto_type 28 (strength training) uses FreeTraining validity."""
        assert get_record_data_valid_len(28, 3) == 2

    def test_strength_training_v5(self):
        assert get_record_data_valid_len(28, 5) == 2


# ---------------------------------------------------------------------------
# OneDimen validity parsing
# ---------------------------------------------------------------------------


class TestOneDimenValid:
    def test_both_types_valid(self):
        types = [OneDimenType(TYPE_HR, 1, 1), OneDimenType(TYPE_CALORIES, 1, 1)]
        data_valid = bytes([0b11000000])  # bits 7,6 set
        result = _parse_one_dimen_valid(types, version=1, data_valid=data_valid)
        assert result[TYPE_HR] is True
        assert result[TYPE_CALORIES] is True

    def test_only_hr_valid(self):
        types = [OneDimenType(TYPE_HR, 1, 1), OneDimenType(TYPE_CALORIES, 1, 1)]
        data_valid = bytes([0b10000000])  # only bit 7
        result = _parse_one_dimen_valid(types, version=1, data_valid=data_valid)
        assert result[TYPE_HR] is True
        assert result[TYPE_CALORIES] is False

    def test_unsupported_version_marked_false(self):
        types = [OneDimenType(TYPE_HR, 1, 1), OneDimenType(TYPE_SPO2, 1, 3)]
        data_valid = bytes([0b10000000])
        result = _parse_one_dimen_valid(types, version=2, data_valid=data_valid)
        assert result[TYPE_HR] is True
        assert result[TYPE_SPO2] is False


# ---------------------------------------------------------------------------
# FourDimen validity parsing
# ---------------------------------------------------------------------------


class TestFourDimenValid:
    def test_all_exist_and_high(self):
        types = [
            FourDimenType(TYPE_HR, 1, 3),
            FourDimenType(TYPE_CALORIES, 1, 3),
            FourDimenType(TYPE_SPO2, 1, 3),
            FourDimenType(TYPE_STRESS, 1, 3),
        ]
        # Nibbles: 0xC = 1100 = exist + high for each
        data_valid = bytes([0xCC, 0xCC])
        result = _parse_four_dimen_valid(types, version=3, data_valid=data_valid)
        for t in [TYPE_HR, TYPE_CALORIES, TYPE_SPO2, TYPE_STRESS]:
            assert result[t].exist is True
            assert result[t].high is True
            assert result[t].middle is False
            assert result[t].low is False

    def test_unsupported_version_no_exist(self):
        types = [FourDimenType(TYPE_HR, 1, 5)]
        data_valid = bytes([0xC0])
        result = _parse_four_dimen_valid(types, version=3, data_valid=data_valid)
        assert result[TYPE_HR].exist is False


# ---------------------------------------------------------------------------
# FourDimen record parsing
# ---------------------------------------------------------------------------


class TestFourDimenRecords:
    def test_parses_records_with_all_types(self):
        types = [
            FourDimenType(TYPE_HR, 1, 3),
            FourDimenType(TYPE_CALORIES, 1, 3),
        ]
        valid_map = {
            TYPE_HR: FourDimenValid(exist=True, high=True, middle=False, low=False),
            TYPE_CALORIES: FourDimenValid(exist=True, high=True, middle=False, low=False),
        }
        # 3 records, 2 bytes each: HR + calories
        buf = bytes([120, 10, 130, 20, 140, 30])
        records, offset = _parse_four_dimen_records(buf, 0, 3, types, 3, valid_map)
        assert len(records) == 3
        assert records[0][TYPE_HR] == 120
        assert records[0][TYPE_CALORIES] == 10
        assert records[2][TYPE_HR] == 140
        assert records[2][TYPE_CALORIES] == 30
        assert offset == 6

    def test_skips_non_exist_types(self):
        types = [
            FourDimenType(TYPE_HR, 1, 3),
            FourDimenType(TYPE_CALORIES, 1, 3),
        ]
        valid_map = {
            TYPE_HR: FourDimenValid(exist=True, high=True, middle=False, low=False),
            TYPE_CALORIES: FourDimenValid(exist=False, high=False, middle=False, low=False),
        }
        buf = bytes([120, 130, 140])  # only HR per record
        records, offset = _parse_four_dimen_records(buf, 0, 3, types, 3, valid_map)
        assert len(records) == 3
        assert records[0][TYPE_HR] == 120
        assert TYPE_CALORIES not in records[0]


# ---------------------------------------------------------------------------
# IT summary byte count
# ---------------------------------------------------------------------------


class TestItSummaryByteCount:
    def test_free_training_v2(self):
        from mi_fitness_sync.fds_parser import _FREE_TRAINING_IT_SUMMARY_TYPES
        # v2: only itState (type=41, byteCount=1, supportVersion=2)
        assert _it_summary_byte_count(_FREE_TRAINING_IT_SUMMARY_TYPES, 2) == 1

    def test_free_training_v4(self):
        from mi_fitness_sync.fds_parser import _FREE_TRAINING_IT_SUMMARY_TYPES
        # v4: itState(1) + itTotalDuration(4) = 5
        assert _it_summary_byte_count(_FREE_TRAINING_IT_SUMMARY_TYPES, 4) == 5

    def test_free_training_v1(self):
        from mi_fitness_sync.fds_parser import _FREE_TRAINING_IT_SUMMARY_TYPES
        # v1: no types supported
        assert _it_summary_byte_count(_FREE_TRAINING_IT_SUMMARY_TYPES, 1) == 0


# ---------------------------------------------------------------------------
# Full OneDimen body parsing (FreeTraining v2)
# ---------------------------------------------------------------------------


class TestFreeTrainingOneDimen:
    def test_parses_v2_single_segment(self):
        """FreeTraining v2: 2 types (HR + calories), 1-byte validity, 1-byte IT summary."""
        data_valid = bytes([0b11000000])  # HR + calories valid
        record_count = 3
        start_time = 1774241243
        it_summary = bytes([0x00])  # itState = 0 (v2 has 1 byte IT summary)
        records = bytes([120, 10, 130, 20, 140, 30])  # 3 records × 2 bytes

        body = _build_one_dimen_segment(record_count, start_time, it_summary, records)
        header = FdsHeader(
            timestamp=start_time, tz_in_15min=28, version=2,
            sport_type=8, data_valid=data_valid, body_data=body,
        )

        samples = parse_free_training_record(header)

        assert len(samples) == 3
        assert samples[0].timestamp == start_time
        assert samples[0].heart_rate == 120
        assert samples[0].calories == 10
        assert samples[1].timestamp == start_time + 1
        assert samples[1].heart_rate == 130
        assert samples[2].timestamp == start_time + 2
        assert samples[2].heart_rate == 140
        assert samples[2].calories == 30


# ---------------------------------------------------------------------------
# Full FourDimen body parsing (FreeTraining v3)
# ---------------------------------------------------------------------------


class TestFreeTrainingFourDimen:
    def test_parses_v3_single_segment(self):
        """FreeTraining v3: 4 types (HR, cal, spo2, stress), 2-byte validity, IT summary."""
        # Nibbles: 0xC (exist+high) for all 4 types
        data_valid = bytes([0xCC, 0xCC])
        record_count = 2
        start_time = 1774241243
        it_summary = bytes([0x00])  # itState = 0 (v3 has 1 byte IT summary)
        # Each record: HR, calories, spo2, stress (1 byte each)
        records = bytes([
            120, 10, 98, 25,  # record 0
            130, 20, 97, 30,  # record 1
        ])

        body = _build_one_dimen_segment(record_count, start_time, it_summary, records)
        header = FdsHeader(
            timestamp=start_time, tz_in_15min=28, version=3,
            sport_type=8, data_valid=data_valid, body_data=body,
        )

        samples = parse_free_training_record(header)

        assert len(samples) == 2
        assert samples[0].timestamp == start_time
        assert samples[0].heart_rate == 120
        assert samples[0].calories == 10
        assert samples[0].spo2 == 98
        assert samples[0].stress == 25
        assert samples[1].timestamp == start_time + 1
        assert samples[1].heart_rate == 130
        assert samples[1].spo2 == 97


# ---------------------------------------------------------------------------
# Full parse_sport_record pipeline
# ---------------------------------------------------------------------------


class TestParseSportRecord:
    def test_free_training_v2_full_pipeline(self):
        data_valid = bytes([0b11000000])
        record_count = 5
        start_time = 1774241243
        it_summary = bytes([0x00])
        records = bytes(sum(([100 + i, i * 5] for i in range(5)), []))

        header_bytes = _build_header(start_time, 28, 2, 8, data_valid)
        body = _build_one_dimen_segment(record_count, start_time, it_summary, records)
        decrypted = header_bytes + body

        samples = parse_sport_record(decrypted, sport_type=8)

        assert len(samples) == 5
        assert samples[0].heart_rate == 100
        assert samples[0].calories == 0
        assert samples[4].timestamp == start_time + 4

    def test_unsupported_sport_returns_empty(self):
        header_bytes = _build_header(1000, 0, 1, 99, b"\x00")
        assert parse_sport_record(header_bytes, sport_type=99) == []

    def test_short_data_returns_empty(self):
        assert parse_sport_record(b"\x00\x01\x02", sport_type=8) == []

    def test_strength_training_v2_uses_free_training_parser(self):
        """proto_type=28 (strength training) parses identically to free_training."""
        data_valid = bytes([0b11000000])
        record_count = 4
        start_time = 1774241243
        it_summary = bytes([0x00])
        records = bytes(sum(([105 + i, i * 3] for i in range(4)), []))

        header_bytes = _build_header(start_time, 28, 2, 28, data_valid)
        body = _build_one_dimen_segment(record_count, start_time, it_summary, records)
        decrypted = header_bytes + body

        samples = parse_sport_record(decrypted, sport_type=28)

        assert len(samples) == 4
        assert samples[0].heart_rate == 105
        assert samples[0].calories == 0
        assert samples[3].timestamp == start_time + 3
        assert samples[3].heart_rate == 108

    def test_strength_training_v3_fourdimen(self):
        """proto_type=28 v3+ uses FourDimen format (HR, cal, spo2, stress)."""
        data_valid = bytes([0xCC, 0xCC])
        record_count = 2
        start_time = 1774241243
        it_summary = bytes([0x00])
        records = bytes([
            120, 10, 98, 25,
            130, 20, 97, 30,
        ])

        header_bytes = _build_header(start_time, 28, 3, 28, data_valid)
        body = _build_one_dimen_segment(record_count, start_time, it_summary, records)
        decrypted = header_bytes + body

        samples = parse_sport_record(decrypted, sport_type=28)

        assert len(samples) == 2
        assert samples[0].heart_rate == 120
        assert samples[0].spo2 == 98
        assert samples[1].heart_rate == 130
        assert samples[1].stress == 30


# ---------------------------------------------------------------------------
# Multiple pause segments
# ---------------------------------------------------------------------------


class TestMultipleSegments:
    def test_two_segments_concatenated(self):
        """Two OneDimen pause segments should produce samples from both."""
        data_valid = bytes([0b11000000])
        start1 = 1000
        start2 = 2000

        it_summary = b""  # v1 has no IT summary types supported
        seg1 = _build_one_dimen_segment(2, start1, it_summary, bytes([80, 5, 90, 10]))
        seg2 = _build_one_dimen_segment(3, start2, it_summary, bytes([100, 15, 110, 20, 120, 25]))

        header = FdsHeader(
            timestamp=start1, tz_in_15min=0, version=1,
            sport_type=8, data_valid=data_valid, body_data=seg1 + seg2,
        )

        samples = parse_free_training_record(header)

        assert len(samples) == 5
        assert samples[0].timestamp == 1000
        assert samples[0].heart_rate == 80
        assert samples[1].timestamp == 1001
        assert samples[2].timestamp == 2000
        assert samples[2].heart_rate == 100
        assert samples[4].timestamp == 2002
        assert samples[4].heart_rate == 120


# ---------------------------------------------------------------------------
# _find_fds_entry
# ---------------------------------------------------------------------------


class TestFindFdsEntry:
    def test_exact_server_key_match(self):
        from mi_fitness_sync.activities import _find_fds_entry
        downloads = {"abc_123": {"url": "http://x", "obj_key": "k"}}
        assert _find_fds_entry(downloads, "abc", 123) == {"url": "http://x", "obj_key": "k"}

    def test_no_match_returns_none(self):
        from mi_fitness_sync.activities import _find_fds_entry
        downloads = {"xyz": {"url": "http://x", "obj_key": "k"}}
        assert _find_fds_entry(downloads, "abc", 123) is None


# ---------------------------------------------------------------------------
# download_and_parse_sport_record – regression tests for real API field names
# ---------------------------------------------------------------------------


class TestDownloadAndParseSportRecordApiShape:
    """Regression: FDS API returns snake_case keys (obj_key, obj_name, etc.).

    An earlier bug silently fell back to timeline data because the code
    read ``objectKey`` (Java property name) instead of ``obj_key`` (the
    actual ``@SerializedName``).  These tests lock in the correct shape.
    """

    def _make_encrypted_binary(self, aes_key: bytes, sport_type: int = 8) -> str:
        """Build a minimal valid encrypted FDS sport record payload."""
        # Header: 7-byte serverDataId + 1 padding + 1 dataValid (version 2)
        data_valid = bytes([0b11000000])  # HR + calories valid
        header = struct.pack("<I", 1700000000) + bytes([28, 2, sport_type]) + b"\x00" + data_valid
        # One segment: 2 records, start_time, IT summary (1 byte for v2), records
        it_summary = b"\x00"
        rec1 = bytes([80, 10])  # HR=80, calories=10
        rec2 = bytes([85, 12])  # HR=85, calories=12
        segment = struct.pack("<II", 2, 1700000000) + it_summary + rec1 + rec2
        plaintext = header + segment
        # Encrypt
        cipher = AES.new(aes_key, AES.MODE_CBC, b"1234567887654321")
        ct = cipher.encrypt(pad(plaintext, AES.block_size))
        return base64.urlsafe_b64encode(ct).decode("ascii").rstrip("=")

    def test_reads_obj_key_and_url_from_fds_entry(self, monkeypatch):
        """download_and_parse_sport_record reads obj_key and url correctly."""
        aes_key = b"\xab" * 16
        obj_key_b64 = base64.urlsafe_b64encode(aes_key).decode("ascii").rstrip("=")
        encrypted_body = self._make_encrypted_binary(aes_key)

        # Realistic FDS API response entry with all snake_case keys
        fds_entry = {
            "url": "https://fds.example.com/download/abc123",
            "obj_name": "sport_record_abc",
            "obj_key": obj_key_b64,
            "method": "GET",
            "expires_time": 1700099999,
        }

        class FakeResponse:
            status_code = 200
            text = encrypted_body
            def raise_for_status(self):
                pass

        class FakeSession:
            def get(self, url, timeout=30):
                assert url == "https://fds.example.com/download/abc123"
                return FakeResponse()

        samples = download_and_parse_sport_record(FakeSession(), fds_entry, sport_type=8)
        assert len(samples) == 2
        assert samples[0].heart_rate == 80
        assert samples[1].heart_rate == 85

    def test_missing_obj_key_returns_empty(self):
        """If obj_key is missing, function returns [] without crashing."""
        class FakeSession:
            pass

        fds_entry_no_key = {
            "url": "https://fds.example.com/download/abc123",
            "obj_name": "sport_record_abc",
            "method": "GET",
            "expires_time": 1700099999,
        }
        assert download_and_parse_sport_record(FakeSession(), fds_entry_no_key, sport_type=8) == []

    def test_none_obj_key_returns_empty(self):
        """If obj_key is None, function returns [] without crashing."""
        class FakeSession:
            pass

        fds_entry_null_key = {
            "url": "https://fds.example.com/download/abc123",
            "obj_key": None,
            "method": "GET",
        }
        assert download_and_parse_sport_record(FakeSession(), fds_entry_null_key, sport_type=8) == []

    def test_camelcase_objectkey_is_not_read(self, monkeypatch):
        """Verify that the old camelCase key 'objectKey' is NOT used."""
        class FakeSession:
            pass

        fds_entry_old_keys = {
            "url": "https://fds.example.com/download/abc123",
            "objectKey": "should_not_be_read",
            "objectName": "should_not_be_read",
        }
        # obj_key is absent → should return empty, proving objectKey is ignored
        assert download_and_parse_sport_record(FakeSession(), fds_entry_old_keys, sport_type=8) == []


# ---------------------------------------------------------------------------
# _extract_high_value – bit extraction for compound FourDimen types
# ---------------------------------------------------------------------------


class TestExtractHighValue:
    def test_no_bit_extraction(self):
        """When high_start_bit is None, raw value returned as-is."""
        dt = FourDimenType(TYPE_HR, 1, 1)
        assert _extract_high_value(0xAB, dt) == 0xAB

    def test_single_bit_extraction(self):
        """Extract 1 bit at position 7 (e.g. integerKm sign bit)."""
        dt = FourDimenType(TYPE_INTEGER_KM, 1, 1, high_start_bit=7, high_bit_count=1)
        assert _extract_high_value(0xFF, dt) == 1
        assert _extract_high_value(0x7F, dt) == 0
        assert _extract_high_value(0x80, dt) == 1

    def test_nibble_extraction(self):
        """Extract 4 bits at position 4 (e.g. calories high nibble)."""
        dt = FourDimenType(TYPE_CALORIES, 1, 1, high_start_bit=4, high_bit_count=4)
        assert _extract_high_value(0xA5, dt) == 0xA  # bits [7:4] = 0xA
        assert _extract_high_value(0x30, dt) == 3

    def test_multi_bit_wide_field(self):
        """Extract 6 bits at position 26 (e.g. landingImpact from 4-byte)."""
        dt = FourDimenType(44, 4, 5, high_start_bit=26, high_bit_count=6)
        raw = 42 << 26
        assert _extract_high_value(raw, dt) == 42


# ---------------------------------------------------------------------------
# FourDimen records with bit extraction
# ---------------------------------------------------------------------------


class TestFourDimenRecordsWithHighExtraction:
    def test_records_apply_high_bit_extraction(self):
        """FourDimen parser applies _extract_high_value for types with bit fields."""
        types = [
            FourDimenType(TYPE_CALORIES, 1, 1, high_start_bit=4, high_bit_count=4),
            FourDimenType(TYPE_HR, 1, 1),
        ]
        valid_map = {
            TYPE_CALORIES: FourDimenValid(exist=True, high=True, middle=False, low=False),
            TYPE_HR: FourDimenValid(exist=True, high=True, middle=False, low=False),
        }
        buf = bytes([0xA5, 120])
        records, offset = _parse_four_dimen_records(buf, 0, 1, types, 1, valid_map)
        assert records[0][TYPE_CALORIES] == 0xA
        assert records[0][TYPE_HR] == 120
        assert offset == 2


# ---------------------------------------------------------------------------
# SportRecordConfig + _parse_with_config
# ---------------------------------------------------------------------------


class TestParseWithConfig:
    def test_selects_one_dimen_for_low_version(self):
        config = SportRecordConfig(
            it_summary_types=[],
            one_dimen_types=[OneDimenType(TYPE_HR, 1, 1), OneDimenType(TYPE_CALORIES, 1, 1)],
            four_dimen_types=[FourDimenType(TYPE_HR, 1, 3), FourDimenType(TYPE_CALORIES, 1, 3)],
            four_dimen_min_version=3,
        )
        data_valid = bytes([0b11000000])
        body = _build_one_dimen_segment(2, 5000, b"", bytes([80, 5, 90, 10]))
        header = FdsHeader(5000, 0, 1, 8, data_valid, body)
        samples = _parse_with_config(header, config)
        assert len(samples) == 2
        assert samples[0].heart_rate == 80

    def test_selects_four_dimen_for_high_version(self):
        config = SportRecordConfig(
            it_summary_types=[],
            one_dimen_types=[OneDimenType(TYPE_HR, 1, 1)],
            four_dimen_types=[FourDimenType(TYPE_HR, 1, 3), FourDimenType(TYPE_CALORIES, 1, 3)],
            four_dimen_min_version=3,
        )
        data_valid = bytes([0xCC])
        body = _build_one_dimen_segment(1, 5000, b"", bytes([120, 10]))
        header = FdsHeader(5000, 0, 3, 8, data_valid, body)
        samples = _parse_with_config(header, config)
        assert len(samples) == 1
        assert samples[0].heart_rate == 120
        assert samples[0].calories == 10

    def test_alt_four_dimen_overrides(self):
        config = SportRecordConfig(
            it_summary_types=[],
            four_dimen_types=[FourDimenType(TYPE_HR, 1, 1)],
            four_dimen_min_version=1,
            alt_four_dimen_types=[
                FourDimenType(TYPE_HR, 1, 5),
                FourDimenType(TYPE_CALORIES, 1, 5),
            ],
            alt_four_dimen_min_version=5,
        )
        data_valid = bytes([0xCC])
        body = _build_one_dimen_segment(1, 5000, b"", bytes([100, 20]))
        header = FdsHeader(5000, 0, 5, 14, data_valid, body)
        samples = _parse_with_config(header, config)
        assert len(samples) == 1
        assert samples[0].heart_rate == 100
        assert samples[0].calories == 20

    def test_empty_config_returns_empty(self):
        config = SportRecordConfig(it_summary_types=[])
        header = FdsHeader(5000, 0, 1, 99, b"\x00", b"")
        assert _parse_with_config(header, config) == []


# ---------------------------------------------------------------------------
# Pause initial data support
# ---------------------------------------------------------------------------


class TestPauseInitData:
    def test_outdoor_running_with_init_height(self):
        """Outdoor running (type 1) skips 4-byte initHeight before each segment."""
        config = _SPORT_CONFIG[1]
        init_height = struct.pack("<I", 12345)
        # At v1, IT summary types have support_version=2 so 0 IT bytes are consumed
        data_valid = bytes([0xCC, 0xCC])
        record_data = bytes([0x52, 120, 0x81, 50])
        segment = struct.pack("<II", 1, 9000) + record_data
        body = init_height + segment

        header = FdsHeader(9000, 0, 1, 1, data_valid, body)
        samples = _parse_with_config(header, config)
        assert len(samples) == 1
        assert samples[0].heart_rate == 120
        assert samples[0].calories == 5
        assert samples[0].timestamp == 9000


# ---------------------------------------------------------------------------
# Sport type coverage – all config entries exist
# ---------------------------------------------------------------------------


class TestSportConfigCoverage:
    @pytest.mark.parametrize("sport_type", list(range(1, 26)) + [28])
    def test_config_exists(self, sport_type):
        assert sport_type in _SPORT_CONFIG

    @pytest.mark.parametrize("sport_type", list(range(1, 26)) + [28])
    def test_validity_exists(self, sport_type):
        assert get_record_data_valid_len(sport_type, 1) is not None


# ---------------------------------------------------------------------------
# parse_sport_record with new sport types
# ---------------------------------------------------------------------------


class TestParseSportRecordNewTypes:
    def test_outdoor_run_type1(self):
        sport_type = 1
        data_valid = bytes([0xCC, 0xCC])
        header_bytes = _build_header(9000, 28, 1, sport_type, data_valid)
        init_height = struct.pack("<I", 0)
        # At v1, IT summary types have support_version=2 so 0 bytes consumed
        records = bytes([0x30, 100, 0x80, 25])
        segment = struct.pack("<II", 1, 9000) + records
        body = init_height + segment
        decrypted = header_bytes + body

        samples = parse_sport_record(decrypted, sport_type)
        assert len(samples) == 1
        assert samples[0].heart_rate == 100

    def test_basketball_type19(self):
        sport_type = 19
        data_valid = bytes([0xCC, 0xCC])
        header_bytes = _build_header(9000, 28, 1, sport_type, data_valid)
        records = bytes([120, 10, 0x35, 50])
        segment = struct.pack("<II", 1, 9000) + records
        body = segment
        decrypted = header_bytes + body

        samples = parse_sport_record(decrypted, sport_type)
        assert len(samples) == 1
        assert samples[0].heart_rate == 120
        assert samples[0].calories == 10

    def test_triathlon_type17_one_dimen(self):
        sport_type = 17
        # Triathlon has data_valid_len=0 (all types always valid)
        data_valid = b""
        header_bytes = _build_header(9000, 28, 1, sport_type, data_valid)
        records = bytes([100, 5, 110, 10])
        segment = struct.pack("<II", 2, 9000) + records
        decrypted = header_bytes + segment

        samples = parse_sport_record(decrypted, sport_type)
        assert len(samples) == 2
        assert samples[0].heart_rate == 100
        assert samples[0].calories == 5
        assert samples[1].heart_rate == 110

    def test_elliptical_type11(self):
        sport_type = 11
        data_valid = bytes([0xCC])
        header_bytes = _build_header(9000, 28, 1, sport_type, data_valid)
        # At v1, IT summary (itState support_version=2) is 0 bytes
        # Type order: calories(high_nibble) then HR. 0x50 → cal high = 5, then HR=120
        records = bytes([0x50, 120])
        segment = struct.pack("<II", 1, 9000) + records
        decrypted = header_bytes + segment

        samples = parse_sport_record(decrypted, sport_type)
        assert len(samples) == 1
        assert samples[0].heart_rate == 120
        assert samples[0].calories == 5


# ---------------------------------------------------------------------------
# OneDimen dependency-aware field skipping
# ---------------------------------------------------------------------------


class TestOneDimenDependency:
    """Test that OneDimen records skip dependent fields when condition is unmet."""

    # Swimming-like config: field -1 is the switch; fields 9, 2 depend on (-1, {0})
    SWIM_DEP = (-1, frozenset({0}))
    SWIM_TYPES = [
        OneDimenType(-1, 1, 1),                                     # swimmingType
        OneDimenType(1, 4, 1),                                      # endTime (no dep)
        OneDimenType(11, 1, 1),                                     # sub-type (no dep)
        OneDimenType(TYPE_DISTANCE, 2, 1, depends_on=SWIM_DEP),     # dep on -1==0
        OneDimenType(TYPE_CALORIES, 2, 1, depends_on=SWIM_DEP),     # dep on -1==0
    ]

    def test_dependency_met_reads_all_fields(self):
        """When swimmingType==0, dependent fields ARE consumed."""
        # swimmingType=0 → dependency met
        # Record: [swimType=0][endTime=1000 LE][subType=5][dist=200 LE][cal=50 LE]
        record = (
            bytes([0])                          # swimType = 0 → dep MET
            + struct.pack("<I", 1000)           # endTime
            + bytes([5])                        # subType
            + struct.pack("<H", 200)            # distance (dep met)
            + struct.pack("<H", 50)             # calories (dep met)
        )
        # validity bitmap: bits for types 1, 11, 9, 2 → 4 bits → need 1 byte
        # All valid: 0b11110000
        data_valid = bytes([0xF0])
        valid_map = _parse_one_dimen_valid(self.SWIM_TYPES, version=1, data_valid=data_valid)

        records, offset = _parse_one_dimen_records(
            record, 0, 1, self.SWIM_TYPES, version=1, valid_map=valid_map,
        )
        assert len(records) == 1
        assert records[0][TYPE_DISTANCE] == 200
        assert records[0][TYPE_CALORIES] == 50
        assert offset == len(record)

    def test_dependency_unmet_skips_fields(self):
        """When swimmingType!=0, dependent fields are NOT consumed."""
        # swimmingType=1 → dependency NOT met → distance + calories bytes absent
        record = (
            bytes([1])                          # swimType = 1 → dep NOT met
            + struct.pack("<I", 2000)           # endTime
            + bytes([3])                        # subType
            # NO distance or calories bytes in the stream
        )
        data_valid = bytes([0xF0])
        valid_map = _parse_one_dimen_valid(self.SWIM_TYPES, version=1, data_valid=data_valid)

        records, offset = _parse_one_dimen_records(
            record, 0, 1, self.SWIM_TYPES, version=1, valid_map=valid_map,
        )
        assert len(records) == 1
        assert TYPE_DISTANCE not in records[0]
        assert TYPE_CALORIES not in records[0]
        assert records[0][1] == 2000  # endTime still read correctly
        assert offset == len(record)

    def test_two_records_different_dependency(self):
        """Two records: first has dep met, second has dep unmet → correct alignment."""
        rec1 = (
            bytes([0])                          # swimType=0 → dep met
            + struct.pack("<I", 100)
            + bytes([1])
            + struct.pack("<H", 300)
            + struct.pack("<H", 40)
        )
        rec2 = (
            bytes([1])                          # swimType=1 → dep not met
            + struct.pack("<I", 200)
            + bytes([2])
            # no dependent fields
        )
        buf = rec1 + rec2
        data_valid = bytes([0xF0])
        valid_map = _parse_one_dimen_valid(self.SWIM_TYPES, version=1, data_valid=data_valid)

        records, offset = _parse_one_dimen_records(
            buf, 0, 2, self.SWIM_TYPES, version=1, valid_map=valid_map,
        )
        assert len(records) == 2
        assert records[0][TYPE_DISTANCE] == 300
        assert records[0][TYPE_CALORIES] == 40
        assert TYPE_DISTANCE not in records[1]
        assert records[1][1] == 200  # endTime of second record
        assert offset == len(buf)

    def test_swimming_full_pipeline_dep_met(self):
        """Full parse_sport_record for swimming with swimmingType=0 (dep met)."""
        sport_type = 9  # pool swimming
        version = 1
        # Swimming has data_valid_len from validity table
        data_valid_len = get_record_data_valid_len(sport_type, version)
        assert data_valid_len is not None

        # Build validity: Swimming v1 has 15 non-negative types (1,11,12,13,9,2,16,10,17,18,19,20,21,22)
        # → 14 bits → 2 bytes. All valid.
        data_valid = bytes([0xFF, 0xFC])  # 14 bits set
        header_bytes = _build_header(9000, 28, version, sport_type, data_valid)

        # One record: swimType=0 (dep met) → all dependent fields present
        record = (
            bytes([0])                          # swimType=0
            + struct.pack("<I", 9000)           # endTime
            + bytes([5])                        # type 11
            + struct.pack("<H", 120)            # pace (type 12)
            + struct.pack("<H", 45)             # swolf (type 13)
            + struct.pack("<H", 400)            # distance (type 9, dep)
            + struct.pack("<H", 80)             # calories (type 2, dep)
            + struct.pack("<H", 30)             # stroke count (type 16, dep)
            + struct.pack("<H", 10)             # turn count (type 10, dep)
            + bytes([25])                       # stroke freq (type 17, dep)
            + bytes([1])                        # type 18
            + bytes([2])                        # type 19
            + bytes([3])                        # type 20
            + bytes([4])                        # type 21
            + bytes([5])                        # type 22
        )
        segment = struct.pack("<II", 1, 9000) + record
        decrypted = header_bytes + segment

        samples = parse_sport_record(decrypted, sport_type)
        assert len(samples) == 1
        assert samples[0].distance == 400
        assert samples[0].calories == 80


# ---------------------------------------------------------------------------
# FourDimen max-support-version semantics
# ---------------------------------------------------------------------------


class TestFourDimenMaxSupportVersion:
    """Test that FourDimen types respect max_support_version."""

    def test_max_version_excludes_field_at_higher_version(self):
        """Field with max_support_version=3 is excluded at version 4."""
        types = [
            FourDimenType(TYPE_CALORIES, 1, 1),
            FourDimenType(TYPE_HR, 1, 1),
            FourDimenType(TYPE_DISTANCE, 1, 1, max_support_version=3),
        ]
        # At version 4, DISTANCE (max=3) should not consume a nibble
        # So we only need nibbles for CALORIES + HR = 2 nibbles = 1 byte
        data_valid = bytes([0xCC])  # 2 nibbles: exist+high for both
        result = _parse_four_dimen_valid(types, version=4, data_valid=data_valid)
        assert result[TYPE_CALORIES].exist is True
        assert result[TYPE_HR].exist is True
        assert result[TYPE_DISTANCE].exist is False

    def test_max_version_includes_field_at_equal_version(self):
        """Field with max_support_version=3 IS included at version 3."""
        types = [
            FourDimenType(TYPE_CALORIES, 1, 1),
            FourDimenType(TYPE_HR, 1, 1),
            FourDimenType(TYPE_DISTANCE, 1, 1, max_support_version=3),
        ]
        # At version 3, all 3 types present → 3 nibbles → 2 bytes
        data_valid = bytes([0xCC, 0xC0])
        result = _parse_four_dimen_valid(types, version=3, data_valid=data_valid)
        assert result[TYPE_CALORIES].exist is True
        assert result[TYPE_HR].exist is True
        assert result[TYPE_DISTANCE].exist is True

    def test_max_version_no_nibble_consumed(self):
        """Exceeded max_support_version field does not shift nibble alignment."""
        # Types: A(sv=1), B(sv=1, max=2), C(sv=1)
        types = [
            FourDimenType(TYPE_CALORIES, 1, 1),
            FourDimenType(TYPE_HR, 1, 1, max_support_version=2),
            FourDimenType(TYPE_DISTANCE, 1, 1),
        ]
        # At version 3: HR is maxed out → only CALORIES + DISTANCE consume nibbles
        # 2 nibbles packed: nibble0 = 0xC (CALORIES exist+high), nibble1 = 0xC (DISTANCE)
        data_valid = bytes([0xCC])
        result = _parse_four_dimen_valid(types, version=3, data_valid=data_valid)
        assert result[TYPE_CALORIES].exist is True
        assert result[TYPE_CALORIES].high is True
        assert result[TYPE_HR].exist is False  # maxed out
        assert result[TYPE_DISTANCE].exist is True
        assert result[TYPE_DISTANCE].high is True

    def test_ski_config_v1_all_legacy_fields_present(self):
        """Ski at v1: legacy fields (max=3) are present, v4+ fields are not."""
        ski_types = _SPORT_CONFIG[21].four_dimen_types
        # v1: CALORIES(sv=1), HR(sv=1), HEIGHT_VALUE(sv=4→skip), DISTANCE_DOUBLE(sv=4→skip),
        #     HEIGHT_CHANGE_SIGN(sv=1,max=3→present), DISTANCE(sv=1,max=3→present),
        #     SPEED(sv=2→skip)
        # Active nibbles: CALORIES, HR, HEIGHT_CHANGE_SIGN, DISTANCE → 4 nibbles = 2 bytes
        data_valid = bytes([0xCC, 0xCC])
        result = _parse_four_dimen_valid(ski_types, version=1, data_valid=data_valid)
        assert result[TYPE_CALORIES].exist is True
        assert result[TYPE_HR].exist is True

    def test_ski_config_v4_legacy_fields_gone(self):
        """Ski at v4: legacy fields (max=3) are gone, new v4 fields appear."""
        from mi_fitness_sync.fds_parser import (
            TYPE_DISTANCE_DOUBLE,
            TYPE_HEIGHT_CHANGE_SIGN,
            TYPE_HEIGHT_VALUE,
            TYPE_SPEED,
        )
        ski_types = _SPORT_CONFIG[21].four_dimen_types
        # v4: CALORIES(sv=1), HR(sv=1), HEIGHT_VALUE(sv=4→present),
        #     DISTANCE_DOUBLE(sv=4→present), HEIGHT_CHANGE_SIGN(sv=1,max=3→GONE),
        #     DISTANCE(sv=1,max=3→GONE), SPEED(sv=2→present)
        # Active nibbles: CALORIES, HR, HEIGHT_VALUE, DISTANCE_DOUBLE, SPEED → 5
        # 5 nibbles → 3 bytes
        data_valid = bytes([0xCC, 0xCC, 0xC0])
        result = _parse_four_dimen_valid(ski_types, version=4, data_valid=data_valid)
        assert result[TYPE_CALORIES].exist is True
        assert result[TYPE_HR].exist is True
        assert result[TYPE_HEIGHT_VALUE].exist is True
        assert result[TYPE_DISTANCE_DOUBLE].exist is True
        assert result[TYPE_HEIGHT_CHANGE_SIGN].exist is False
        assert result[TYPE_DISTANCE].exist is False
        assert result[TYPE_SPEED].exist is True

    def test_ski_records_v4_correct_alignment(self):
        """Ski v4 records: legacy fields gone, new fields present, bytes align correctly."""
        from mi_fitness_sync.fds_parser import (
            TYPE_DISTANCE_DOUBLE,
            TYPE_HEIGHT_VALUE,
            TYPE_SPEED,
        )
        ski_types = _SPORT_CONFIG[21].four_dimen_types
        # v4, 5 active nibbles
        data_valid = bytes([0xCC, 0xCC, 0xC0])
        valid_map = _parse_four_dimen_valid(ski_types, version=4, data_valid=data_valid)

        # Record: CALORIES(1) + HR(1) + HEIGHT_VALUE(4) + DISTANCE_DOUBLE(2) + SPEED(2)
        # = 10 bytes per record
        record = (
            bytes([10])                         # calories
            + bytes([120])                      # hr
            + struct.pack("<I", 5000)           # height_value
            + struct.pack("<H", 300)            # distance_double
            + struct.pack("<H", 150)            # speed
        )
        records, offset = _parse_four_dimen_records(
            record, 0, 1, ski_types, version=4, valid_map=valid_map,
        )
        assert len(records) == 1
        assert records[0][TYPE_CALORIES] == 10
        assert records[0][TYPE_HR] == 120
        assert records[0][TYPE_HEIGHT_VALUE] == 5000
        assert records[0][TYPE_DISTANCE_DOUBLE] == 300
        assert records[0][TYPE_SPEED] == 150
        assert offset == 10
