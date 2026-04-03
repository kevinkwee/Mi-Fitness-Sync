from __future__ import annotations

import base64
import struct

import pytest

from mi_fitness_sync.fds.common import (
    FdsHeader,
    FourDimenType,
    FourDimenValid,
    OneDimenType,
    TYPE_CALORIES,
    TYPE_CADENCE,
    TYPE_DISTANCE,
    TYPE_DISTANCE_DOUBLE,
    TYPE_HEIGHT_CHANGE_SIGN,
    TYPE_HEIGHT_VALUE,
    TYPE_HR,
    TYPE_INTEGER_KM,
    TYPE_SPO2,
    TYPE_STRESS,
    TYPE_SPEED,
    extract_high_value,
    get_record_data_valid_len,
    parse_four_dimen_valid,
    parse_one_dimen_valid,
)
from mi_fitness_sync.fds.cache import FdsCache
from mi_fitness_sync.fds.sport_records import (
    FREE_TRAINING_IT_SUMMARY_TYPES,
    SPORT_CONFIG,
    SportRecordConfig,
    download_and_parse_sport_record,
    it_summary_byte_count,
    parse_four_dimen_records,
    parse_free_training_record,
    parse_one_dimen_records,
    parse_sport_record,
    parse_with_config,
)
from tests.fds.support import b64url_encode_no_pad, build_header, build_one_dimen_segment, encrypt, make_aes_key


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
        buf = bytes([120, 10, 130, 20, 140, 30])
        records, offset = parse_four_dimen_records(buf, 0, 3, types, 3, valid_map)
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
        buf = bytes([120, 130, 140])
        records, offset = parse_four_dimen_records(buf, 0, 3, types, 3, valid_map)
        assert len(records) == 3
        assert records[0][TYPE_HR] == 120
        assert TYPE_CALORIES not in records[0]
        assert offset == 3

    def test_records_apply_high_bit_extraction(self):
        types = [
            FourDimenType(TYPE_CALORIES, 1, 1, high_start_bit=4, high_bit_count=4),
            FourDimenType(TYPE_HR, 1, 1),
        ]
        valid_map = {
            TYPE_CALORIES: FourDimenValid(exist=True, high=True, middle=False, low=False),
            TYPE_HR: FourDimenValid(exist=True, high=True, middle=False, low=False),
        }
        buf = bytes([0xA5, 120])
        records, offset = parse_four_dimen_records(buf, 0, 1, types, 1, valid_map)
        assert records[0][TYPE_CALORIES] == 0xA
        assert records[0][TYPE_HR] == 120
        assert offset == 2


class TestItSummaryByteCount:
    def test_free_training_versions(self):
        assert it_summary_byte_count(FREE_TRAINING_IT_SUMMARY_TYPES, 2) == 1
        assert it_summary_byte_count(FREE_TRAINING_IT_SUMMARY_TYPES, 4) == 5
        assert it_summary_byte_count(FREE_TRAINING_IT_SUMMARY_TYPES, 1) == 0


class TestFreeTrainingOneDimen:
    def test_parses_v2_single_segment(self):
        data_valid = bytes([0b11000000])
        record_count = 3
        start_time = 1774241243
        it_summary = bytes([0x00])
        records = bytes([120, 10, 130, 20, 140, 30])

        body = build_one_dimen_segment(record_count, start_time, it_summary, records)
        header = FdsHeader(start_time, 28, 2, 8, data_valid, body)

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


class TestFreeTrainingFourDimen:
    def test_parses_v3_single_segment(self):
        data_valid = bytes([0xCC, 0xCC])
        record_count = 2
        start_time = 1774241243
        it_summary = bytes([0x00])
        records = bytes([120, 10, 98, 25, 130, 20, 97, 30])

        body = build_one_dimen_segment(record_count, start_time, it_summary, records)
        header = FdsHeader(start_time, 28, 3, 8, data_valid, body)

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


class TestParseSportRecord:
    def test_free_training_v2_full_pipeline(self):
        data_valid = bytes([0b11000000])
        record_count = 5
        start_time = 1774241243
        it_summary = bytes([0x00])
        records = bytes(sum(([100 + index, index * 5] for index in range(5)), []))

        decrypted = build_header(start_time, 28, 2, 8, data_valid) + build_one_dimen_segment(record_count, start_time, it_summary, records)

        samples = parse_sport_record(decrypted, sport_type=8)

        assert len(samples) == 5
        assert samples[0].heart_rate == 100
        assert samples[0].calories == 0
        assert samples[4].timestamp == start_time + 4

    def test_unsupported_sport_returns_empty(self):
        header_bytes = build_header(1000, 0, 1, 99, b"\x00")
        assert parse_sport_record(header_bytes, sport_type=99) == []

    def test_short_data_returns_empty(self):
        assert parse_sport_record(b"\x00\x01\x02", sport_type=8) == []

    def test_strength_training_v2_uses_free_training_parser(self):
        data_valid = bytes([0b11000000])
        record_count = 4
        start_time = 1774241243
        it_summary = bytes([0x00])
        records = bytes(sum(([105 + index, index * 3] for index in range(4)), []))

        decrypted = build_header(start_time, 28, 2, 28, data_valid) + build_one_dimen_segment(record_count, start_time, it_summary, records)

        samples = parse_sport_record(decrypted, sport_type=28)

        assert len(samples) == 4
        assert samples[0].heart_rate == 105
        assert samples[0].calories == 0
        assert samples[3].timestamp == start_time + 3
        assert samples[3].heart_rate == 108

    def test_strength_training_v3_fourdimen(self):
        data_valid = bytes([0xCC, 0xCC])
        record_count = 2
        start_time = 1774241243
        it_summary = bytes([0x00])
        records = bytes([120, 10, 98, 25, 130, 20, 97, 30])

        decrypted = build_header(start_time, 28, 3, 28, data_valid) + build_one_dimen_segment(record_count, start_time, it_summary, records)

        samples = parse_sport_record(decrypted, sport_type=28)

        assert len(samples) == 2
        assert samples[0].heart_rate == 120
        assert samples[0].spo2 == 98
        assert samples[1].heart_rate == 130
        assert samples[1].stress == 30


class TestMultipleSegments:
    def test_two_segments_concatenated(self):
        data_valid = bytes([0b11000000])
        seg1 = build_one_dimen_segment(2, 1000, b"", bytes([80, 5, 90, 10]))
        seg2 = build_one_dimen_segment(3, 2000, b"", bytes([100, 15, 110, 20, 120, 25]))

        header = FdsHeader(1000, 0, 1, 8, data_valid, seg1 + seg2)
        samples = parse_free_training_record(header)

        assert len(samples) == 5
        assert samples[0].timestamp == 1000
        assert samples[0].heart_rate == 80
        assert samples[1].timestamp == 1001
        assert samples[2].timestamp == 2000
        assert samples[2].heart_rate == 100
        assert samples[4].timestamp == 2002
        assert samples[4].heart_rate == 120


class TestParseWithConfig:
    def test_selects_one_dimen_for_low_version(self):
        config = SportRecordConfig(
            it_summary_types=[],
            one_dimen_types=[OneDimenType(TYPE_HR, 1, 1), OneDimenType(TYPE_CALORIES, 1, 1)],
            four_dimen_types=[FourDimenType(TYPE_HR, 1, 3), FourDimenType(TYPE_CALORIES, 1, 3)],
            four_dimen_min_version=3,
        )
        data_valid = bytes([0b11000000])
        body = build_one_dimen_segment(2, 5000, b"", bytes([80, 5, 90, 10]))
        header = FdsHeader(5000, 0, 1, 8, data_valid, body)
        samples = parse_with_config(header, config)
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
        body = build_one_dimen_segment(1, 5000, b"", bytes([120, 10]))
        header = FdsHeader(5000, 0, 3, 8, data_valid, body)
        samples = parse_with_config(header, config)
        assert len(samples) == 1
        assert samples[0].heart_rate == 120
        assert samples[0].calories == 10

    def test_alt_four_dimen_overrides(self):
        config = SportRecordConfig(
            it_summary_types=[],
            four_dimen_types=[FourDimenType(TYPE_HR, 1, 1)],
            four_dimen_min_version=1,
            alt_four_dimen_types=[FourDimenType(TYPE_HR, 1, 5), FourDimenType(TYPE_CALORIES, 1, 5)],
            alt_four_dimen_min_version=5,
        )
        data_valid = bytes([0xCC])
        body = build_one_dimen_segment(1, 5000, b"", bytes([100, 20]))
        header = FdsHeader(5000, 0, 5, 14, data_valid, body)
        samples = parse_with_config(header, config)
        assert len(samples) == 1
        assert samples[0].heart_rate == 100
        assert samples[0].calories == 20

    def test_empty_config_returns_empty(self):
        config = SportRecordConfig(it_summary_types=[])
        header = FdsHeader(5000, 0, 1, 99, b"\x00", b"")
        assert parse_with_config(header, config) == []


class TestPauseInitData:
    def test_outdoor_running_with_init_height(self):
        config = SPORT_CONFIG[1]
        init_height = struct.pack("<I", 12345)
        data_valid = bytes([0xCC, 0xCC])
        records = bytes([0x52, 120, 0x81, 50])
        segment = struct.pack("<II", 1, 9000) + records
        body = init_height + segment

        header = FdsHeader(9000, 0, 1, 1, data_valid, body)
        samples = parse_with_config(header, config)
        assert len(samples) == 1
        assert samples[0].heart_rate == 120
        assert samples[0].calories == 5
        assert samples[0].timestamp == 9000


class TestSportConfigCoverage:
    @pytest.mark.parametrize("sport_type", list(range(1, 26)) + [28])
    def test_config_exists(self, sport_type):
        assert sport_type in SPORT_CONFIG

    @pytest.mark.parametrize("sport_type", list(range(1, 26)) + [28])
    def test_validity_exists(self, sport_type):
        assert get_record_data_valid_len(sport_type, 1) is not None


class TestParseSportRecordNewTypes:
    def test_outdoor_run_type1(self):
        sport_type = 1
        data_valid = bytes([0xCC, 0xCC])
        init_height = struct.pack("<I", 0)
        records = bytes([0x30, 100, 0x80, 25])
        segment = struct.pack("<II", 1, 9000) + records
        decrypted = build_header(9000, 28, 1, sport_type, data_valid) + init_height + segment

        samples = parse_sport_record(decrypted, sport_type)
        assert len(samples) == 1
        assert samples[0].heart_rate == 100

    def test_basketball_type19(self):
        sport_type = 19
        data_valid = bytes([0xCC, 0xCC])
        records = bytes([120, 10, 0x35, 50])
        segment = struct.pack("<II", 1, 9000) + records
        decrypted = build_header(9000, 28, 1, sport_type, data_valid) + segment

        samples = parse_sport_record(decrypted, sport_type)
        assert len(samples) == 1
        assert samples[0].heart_rate == 120
        assert samples[0].calories == 10

    def test_triathlon_type17_one_dimen(self):
        sport_type = 17
        data_valid = b""
        records = bytes([100, 5, 110, 10])
        segment = struct.pack("<II", 2, 9000) + records
        decrypted = build_header(9000, 28, 1, sport_type, data_valid) + segment

        samples = parse_sport_record(decrypted, sport_type)
        assert len(samples) == 2
        assert samples[0].heart_rate == 100
        assert samples[0].calories == 5
        assert samples[1].heart_rate == 110

    def test_elliptical_type11(self):
        sport_type = 11
        data_valid = bytes([0xCC])
        records = bytes([0x50, 120])
        segment = struct.pack("<II", 1, 9000) + records
        decrypted = build_header(9000, 28, 1, sport_type, data_valid) + segment

        samples = parse_sport_record(decrypted, sport_type)
        assert len(samples) == 1
        assert samples[0].heart_rate == 120
        assert samples[0].calories == 5


class TestOneDimenDependency:
    swim_dep = (-1, frozenset({0}))
    swim_types = [
        OneDimenType(-1, 1, 1),
        OneDimenType(1, 4, 1),
        OneDimenType(11, 1, 1),
        OneDimenType(TYPE_DISTANCE, 2, 1, depends_on=swim_dep),
        OneDimenType(TYPE_CALORIES, 2, 1, depends_on=swim_dep),
    ]

    def test_dependency_met_reads_all_fields(self):
        record = bytes([0]) + struct.pack("<I", 1000) + bytes([5]) + struct.pack("<H", 200) + struct.pack("<H", 50)
        valid_map = parse_one_dimen_valid(self.swim_types, version=1, data_valid=bytes([0xF0]))

        records, offset = parse_one_dimen_records(record, 0, 1, self.swim_types, version=1, valid_map=valid_map)
        assert len(records) == 1
        assert records[0][TYPE_DISTANCE] == 200
        assert records[0][TYPE_CALORIES] == 50
        assert offset == len(record)

    def test_dependency_unmet_skips_fields(self):
        record = bytes([1]) + struct.pack("<I", 2000) + bytes([3])
        valid_map = parse_one_dimen_valid(self.swim_types, version=1, data_valid=bytes([0xF0]))

        records, offset = parse_one_dimen_records(record, 0, 1, self.swim_types, version=1, valid_map=valid_map)
        assert len(records) == 1
        assert TYPE_DISTANCE not in records[0]
        assert TYPE_CALORIES not in records[0]
        assert offset == len(record)


class TestSwimmingFullPipeline:
    def test_swimming_full_pipeline_dep_met(self):
        sport_type = 9
        version = 1
        data_valid_len = get_record_data_valid_len(sport_type, version)
        assert data_valid_len is not None

        data_valid = bytes([0xFF, 0xFC])
        record = (
            bytes([0])
            + struct.pack("<I", 9000)
            + bytes([5])
            + struct.pack("<H", 120)
            + struct.pack("<H", 45)
            + struct.pack("<H", 400)
            + struct.pack("<H", 80)
            + struct.pack("<H", 30)
            + struct.pack("<H", 10)
            + bytes([25, 1, 2, 3, 4, 5])
        )
        decrypted = build_header(9000, 28, version, sport_type, data_valid) + struct.pack("<II", 1, 9000) + record

        samples = parse_sport_record(decrypted, sport_type)
        assert len(samples) == 1
        assert samples[0].distance == 400
        assert samples[0].calories == 80


class TestSkiVersionSemantics:
    def test_ski_config_v1_all_legacy_fields_present(self):
        ski_types = SPORT_CONFIG[21].four_dimen_types
        data_valid = bytes([0xCC, 0xCC])
        result = parse_four_dimen_valid(ski_types, version=1, data_valid=data_valid)
        assert result[TYPE_CALORIES].exist is True
        assert result[TYPE_HR].exist is True

    def test_ski_config_v4_legacy_fields_gone(self):
        ski_types = SPORT_CONFIG[21].four_dimen_types
        data_valid = bytes([0xCC, 0xCC, 0xC0])
        result = parse_four_dimen_valid(ski_types, version=4, data_valid=data_valid)
        assert result[TYPE_CALORIES].exist is True
        assert result[TYPE_HR].exist is True
        assert result[TYPE_HEIGHT_VALUE].exist is True
        assert result[TYPE_DISTANCE_DOUBLE].exist is True
        assert result[TYPE_HEIGHT_CHANGE_SIGN].exist is False
        assert result[TYPE_DISTANCE].exist is False
        assert result[TYPE_SPEED].exist is True

    def test_ski_records_v4_correct_alignment(self):
        ski_types = SPORT_CONFIG[21].four_dimen_types
        data_valid = bytes([0xCC, 0xCC, 0xC0])
        valid_map = parse_four_dimen_valid(ski_types, version=4, data_valid=data_valid)

        record = bytes([10, 120]) + struct.pack("<I", 5000) + struct.pack("<H", 300) + struct.pack("<H", 150)
        records, offset = parse_four_dimen_records(record, 0, 1, ski_types, version=4, valid_map=valid_map)
        assert len(records) == 1
        assert records[0][TYPE_CALORIES] == 10
        assert records[0][TYPE_HR] == 120
        assert records[0][TYPE_HEIGHT_VALUE] == 5000
        assert records[0][TYPE_DISTANCE_DOUBLE] == 300
        assert records[0][TYPE_SPEED] == 150
        assert offset == 10


class TestDownloadAndParseSportRecordApiShape:
    def _make_encrypted_binary(self, aes_key: bytes, sport_type: int = 8) -> str:
        data_valid = bytes([0b11000000])
        header = struct.pack("<I", 1700000000) + bytes([28, 2, sport_type]) + b"\x00" + data_valid
        segment = struct.pack("<II", 2, 1700000000) + b"\x00" + bytes([80, 10, 85, 12])
        return encrypt(header + segment, aes_key)

    def test_reads_obj_key_and_url_from_fds_entry(self):
        aes_key = b"\xab" * 16
        obj_key_b64 = b64url_encode_no_pad(aes_key)
        encrypted_body = self._make_encrypted_binary(aes_key)
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

    def test_reads_json_string_wrapped_response_body(self):
        aes_key = b"\xab" * 16
        obj_key_b64 = b64url_encode_no_pad(aes_key)
        encrypted_body = self._make_encrypted_binary(aes_key)
        fds_entry = {
            "url": "https://fds.example.com/download/abc123",
            "obj_key": obj_key_b64,
        }

        class FakeResponse:
            status_code = 200
            text = f'"{encrypted_body}"'

            def raise_for_status(self):
                pass

            def json(self):
                return encrypted_body

        class FakeSession:
            def get(self, url, timeout=30):
                assert url == "https://fds.example.com/download/abc123"
                return FakeResponse()

        samples = download_and_parse_sport_record(FakeSession(), fds_entry, sport_type=8)
        assert len(samples) == 2
        assert samples[0].heart_rate == 80
        assert samples[1].heart_rate == 85

    def test_missing_obj_key_returns_empty(self):
        class FakeSession:
            pass

        fds_entry = {
            "url": "https://fds.example.com/download/abc123",
            "obj_name": "sport_record_abc",
            "method": "GET",
            "expires_time": 1700099999,
        }
        assert download_and_parse_sport_record(FakeSession(), fds_entry, sport_type=8) == []

    def test_none_obj_key_returns_empty(self):
        class FakeSession:
            pass

        fds_entry = {"url": "https://fds.example.com/download/abc123", "obj_key": None, "method": "GET"}
        assert download_and_parse_sport_record(FakeSession(), fds_entry, sport_type=8) == []

    def test_camelcase_objectkey_is_not_read(self):
        class FakeSession:
            pass

        fds_entry = {
            "url": "https://fds.example.com/download/abc123",
            "objectKey": "should_not_be_read",
            "objectName": "should_not_be_read",
        }
        assert download_and_parse_sport_record(FakeSession(), fds_entry, sport_type=8) == []


class TestDownloadAndParseSportRecordCache:
    def _make_encrypted_binary(self, aes_key: bytes, sport_type: int = 8) -> str:
        data_valid = bytes([0b11000000])
        header = struct.pack("<I", 1700000000) + bytes([28, 2, sport_type]) + b"\x00" + data_valid
        segment = struct.pack("<II", 2, 1700000000) + b"\x00" + bytes([80, 10, 85, 12])
        return encrypt(header + segment, aes_key)

    def test_cache_miss_downloads_and_caches(self, tmp_path):
        aes_key = b"\xab" * 16
        obj_key_b64 = b64url_encode_no_pad(aes_key)
        encrypted_body = self._make_encrypted_binary(aes_key)

        fds_entry = {"url": "https://fds.example.com/dl", "obj_key": obj_key_b64}
        download_called: list[str] = []

        class FakeResponse:
            status_code = 200
            text = encrypted_body

            def raise_for_status(self):
                pass

        class FakeSession:
            def get(self, url, timeout=30):
                download_called.append(url)
                return FakeResponse()

        cache = FdsCache(tmp_path / "cache")
        samples = download_and_parse_sport_record(FakeSession(), fds_entry, sport_type=8, cache=cache, cache_key="test_key")
        assert len(samples) == 2
        assert len(download_called) == 1
        assert cache.get("test_key") is not None

    def test_cache_hit_skips_download(self, tmp_path):
        data_valid = bytes([0b11000000])
        header = struct.pack("<I", 1700000000) + bytes([28, 2, 8]) + b"\x00" + data_valid
        segment = struct.pack("<II", 2, 1700000000) + b"\x00" + bytes([90, 20, 95, 25])
        plaintext = header + segment

        cache = FdsCache(tmp_path / "cache")
        cache.put("cached_key", plaintext)

        class FakeSession:
            def get(self, url, timeout=30):
                raise AssertionError("HTTP call should not be made on cache hit")

        samples = download_and_parse_sport_record(
            FakeSession(),
            {"url": "https://unused", "obj_key": "unused"},
            sport_type=8,
            cache=cache,
            cache_key="cached_key",
        )
        assert len(samples) == 2
        assert samples[0].heart_rate == 90
        assert samples[1].heart_rate == 95

    def test_cache_bypass_when_cache_is_none(self):
        aes_key = b"\xab" * 16
        obj_key_b64 = b64url_encode_no_pad(aes_key)
        encrypted_body = self._make_encrypted_binary(aes_key)

        fds_entry = {"url": "https://fds.example.com/dl", "obj_key": obj_key_b64}
        download_called: list[str] = []

        class FakeResponse:
            status_code = 200
            text = encrypted_body

            def raise_for_status(self):
                pass

        class FakeSession:
            def get(self, url, timeout=30):
                download_called.append(url)
                return FakeResponse()

        samples = download_and_parse_sport_record(FakeSession(), fds_entry, sport_type=8, cache=None, cache_key="ignored")
        assert len(samples) == 2
        assert len(download_called) == 1
