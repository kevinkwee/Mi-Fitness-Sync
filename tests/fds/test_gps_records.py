from __future__ import annotations

from mi_fitness_sync.fds.gps_records import (
    GPS_TYPE_ACCURACY,
    GPS_TYPE_ALTITUDE,
    GPS_TYPE_GPS_SOURCE,
    GPS_TYPE_HDOP,
    GPS_TYPE_LATITUDE,
    GPS_TYPE_LONGITUDE,
    GPS_TYPE_SPEED,
    GPS_TYPE_TIME,
    download_and_parse_gps_record,
    get_gps_data_valid_len,
    min_gps_record_bytes,
    parse_gps_record,
    parse_gps_records,
)
from tests.fds.support import (
    b64url_encode_no_pad,
    build_gps_binary,
    build_gps_v1_record,
    build_gps_v2_record,
    build_gps_v3_record,
    encrypt,
    make_aes_key,
)


class TestGpsValidityLen:
    def test_v1(self):
        assert get_gps_data_valid_len(1) == 1

    def test_v2(self):
        assert get_gps_data_valid_len(2) == 1

    def test_v3(self):
        assert get_gps_data_valid_len(3) == 1

    def test_v4(self):
        assert get_gps_data_valid_len(4) == 1

    def test_v5_unsupported(self):
        assert get_gps_data_valid_len(5) is None

    def test_v99_unsupported(self):
        assert get_gps_data_valid_len(99) is None


class TestMinGpsRecordBytes:
    def test_v1(self):
        assert min_gps_record_bytes(1) == 12

    def test_v2(self):
        assert min_gps_record_bytes(2) == 18

    def test_v3(self):
        assert min_gps_record_bytes(3) == 26


class TestParseGpsRecordsV1:
    def test_single_record(self):
        body = build_gps_v1_record(1000, 121.5, 31.2)
        valid_map = {GPS_TYPE_TIME: True, GPS_TYPE_LONGITUDE: True, GPS_TYPE_LATITUDE: True}

        samples, offset = parse_gps_records(body, 0, 1, version=1, valid_map=valid_map)

        assert len(samples) == 1
        assert samples[0].timestamp == 1000
        assert abs(samples[0].longitude - 121.5) < 0.01
        assert abs(samples[0].latitude - 31.2) < 0.01
        assert samples[0].speed is None
        assert samples[0].altitude is None
        assert offset == len(body)

    def test_multiple_records(self):
        body = build_gps_v1_record(1000, 121.5, 31.2) + build_gps_v1_record(1001, 121.501, 31.201)
        valid_map = {GPS_TYPE_TIME: True, GPS_TYPE_LONGITUDE: True, GPS_TYPE_LATITUDE: True}

        samples, _ = parse_gps_records(body, 0, 2, version=1, valid_map=valid_map)

        assert len(samples) == 2
        assert samples[0].timestamp == 1000
        assert samples[1].timestamp == 1001

    def test_truncated_buffer_stops_early(self):
        body = build_gps_v1_record(1000, 121.5, 31.2) + b"\x00\x01"
        valid_map = {GPS_TYPE_TIME: True, GPS_TYPE_LONGITUDE: True, GPS_TYPE_LATITUDE: True}

        samples, _ = parse_gps_records(body, 0, 5, version=1, valid_map=valid_map)

        assert len(samples) == 1


class TestParseGpsRecordsV2:
    def test_speed_decoding(self):
        speed_raw = (150 << 4) | 3
        body = build_gps_v2_record(2000, 121.5, 31.2, 5.0, speed_raw)
        valid_map = {
            GPS_TYPE_TIME: True,
            GPS_TYPE_LONGITUDE: True,
            GPS_TYPE_LATITUDE: True,
            GPS_TYPE_ACCURACY: True,
            GPS_TYPE_SPEED: True,
            GPS_TYPE_GPS_SOURCE: True,
        }

        samples, _ = parse_gps_records(body, 0, 1, version=2, valid_map=valid_map)

        assert len(samples) == 1
        assert abs(samples[0].accuracy - 5.0) < 0.01
        assert abs(samples[0].speed - 15.0) < 0.01
        assert samples[0].gps_source == 3

    def test_gps_source_not_set_when_invalid(self):
        speed_raw = (150 << 4) | 3
        body = build_gps_v2_record(2000, 121.5, 31.2, 5.0, speed_raw)
        valid_map = {
            GPS_TYPE_TIME: True,
            GPS_TYPE_LONGITUDE: True,
            GPS_TYPE_LATITUDE: True,
            GPS_TYPE_ACCURACY: True,
            GPS_TYPE_SPEED: True,
            GPS_TYPE_GPS_SOURCE: False,
        }

        samples, _ = parse_gps_records(body, 0, 1, version=2, valid_map=valid_map)

        assert samples[0].speed is not None
        assert samples[0].gps_source is None


class TestParseGpsRecordsV3:
    def test_full_record(self):
        speed_raw = (100 << 4) | 1
        body = build_gps_v3_record(3000, 116.4, 39.9, 3.5, speed_raw, 45.2, 1.2)
        valid_map = {
            GPS_TYPE_TIME: True,
            GPS_TYPE_LONGITUDE: True,
            GPS_TYPE_LATITUDE: True,
            GPS_TYPE_ACCURACY: True,
            GPS_TYPE_SPEED: True,
            GPS_TYPE_GPS_SOURCE: True,
            GPS_TYPE_ALTITUDE: True,
            GPS_TYPE_HDOP: True,
        }

        samples, _ = parse_gps_records(body, 0, 1, version=3, valid_map=valid_map)

        assert len(samples) == 1
        sample = samples[0]
        assert sample.timestamp == 3000
        assert abs(sample.altitude - 45.2) < 0.1
        assert abs(sample.hdop - 1.2) < 0.1
        assert abs(sample.speed - 10.0) < 0.01


class TestParseGpsRecord:
    def test_v1_end_to_end(self):
        body = build_gps_v1_record(5000, 121.5, 31.2)
        data_valid = bytes([0b11100000])
        raw = build_gps_binary(5000, 28, 1, 1, data_valid, body)

        samples = parse_gps_record(raw)

        assert len(samples) == 1
        assert samples[0].timestamp == 5000
        assert abs(samples[0].longitude - 121.5) < 0.01

    def test_v3_end_to_end(self):
        speed_raw = (50 << 4) | 2
        body = build_gps_v3_record(6000, 116.4, 39.9, 2.5, speed_raw, 100.0, 0.8)
        data_valid = bytes([0xFF])
        raw = build_gps_binary(6000, 28, 3, 1, data_valid, body)

        samples = parse_gps_record(raw)

        assert len(samples) == 1
        sample = samples[0]
        assert sample.timestamp == 6000
        assert abs(sample.altitude - 100.0) < 0.1
        assert abs(sample.hdop - 0.8) < 0.1
        assert abs(sample.speed - 5.0) < 0.01

    def test_v4_with_record_count(self):
        rec1 = build_gps_v3_record(7000, 121.0, 31.0, 5.0, 80 << 4, 50.0, 1.0)
        rec2 = build_gps_v3_record(7001, 121.001, 31.001, 4.0, 90 << 4, 51.0, 0.9)
        body = (2).to_bytes(4, "little") + rec1 + rec2
        data_valid = bytes([0xFF])
        raw = build_gps_binary(7000, 28, 4, 1, data_valid, body)

        samples = parse_gps_record(raw)

        assert len(samples) == 2
        assert samples[0].timestamp == 7000
        assert samples[1].timestamp == 7001

    def test_too_short_returns_empty(self):
        assert parse_gps_record(b"\x00\x01\x02") == []

    def test_unsupported_version_returns_empty(self):
        data_valid = bytes([0xFF])
        raw = build_gps_binary(1000, 28, 99, 1, data_valid, b"\x00" * 50)
        assert parse_gps_record(raw) == []

    def test_required_field_invalid_returns_empty(self):
        body = build_gps_v1_record(5000, 121.5, 31.2)
        data_valid = bytes([0b11000000])
        raw = build_gps_binary(5000, 28, 1, 1, data_valid, body)

        samples = parse_gps_record(raw)
        assert samples == []


class TestDownloadAndParseGpsRecord:
    def test_round_trip(self, monkeypatch):
        key = make_aes_key()
        body = build_gps_v1_record(8000, 121.5, 31.2) + build_gps_v1_record(8001, 121.501, 31.201)
        data_valid = bytes([0b11100000])
        plaintext = build_gps_binary(8000, 28, 1, 1, data_valid, body)
        encrypted_body = encrypt(plaintext, key)

        class FakeResponse:
            status_code = 200
            ok = True
            text = encrypted_body

            def raise_for_status(self):
                pass

        class FakeSession:
            def get(self, *args, **kwargs):
                return FakeResponse()

        fds_entry = {"url": "https://fds.example.com/gps", "obj_key": b64url_encode_no_pad(key)}
        samples = download_and_parse_gps_record(FakeSession(), fds_entry)

        assert len(samples) == 2
        assert samples[0].timestamp == 8000
        assert samples[1].timestamp == 8001

    def test_missing_url_returns_empty(self):
        class FakeSession:
            pass

        assert download_and_parse_gps_record(FakeSession(), {"obj_key": "abc"}) == []

    def test_missing_obj_key_returns_empty(self):
        class FakeSession:
            pass

        assert download_and_parse_gps_record(FakeSession(), {"url": "https://example.com"}) == []