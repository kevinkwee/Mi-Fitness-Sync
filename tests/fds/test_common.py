from __future__ import annotations

import pytest

from mi_fitness_sync.fds.common import (
    FourDimenType,
    OneDimenType,
    TYPE_CALORIES,
    TYPE_DISTANCE,
    TYPE_HR,
    TYPE_INTEGER_KM,
    TYPE_SPO2,
    TYPE_STRESS,
    b64url_decode,
    decrypt_fds_data,
    extract_high_value,
    get_record_data_valid_len,
    parse_fds_header,
    parse_four_dimen_valid,
    parse_one_dimen_valid,
)
from tests.fds.support import b64url_encode_no_pad, build_header, encrypt, make_aes_key


class TestB64UrlDecode:
    def test_decode_with_padding(self):
        data = b"hello world"
        encoded = b64url_encode_no_pad(data) + "="
        assert b64url_decode(encoded) == data

    def test_decode_without_padding(self):
        data = b"hello world"
        encoded = b64url_encode_no_pad(data)
        assert b64url_decode(encoded) == data


class TestDecryptFds:
    def test_round_trip(self):
        key = make_aes_key()
        plaintext = b"This is a test of AES-CBC decryption"
        object_key = b64url_encode_no_pad(key)
        encrypted = encrypt(plaintext, key)
        result = decrypt_fds_data(encrypted, object_key)
        assert result == plaintext

    def test_decrypts_padded_data(self):
        key = make_aes_key()
        plaintext = b"\x01" * 32
        object_key = b64url_encode_no_pad(key)
        encrypted = encrypt(plaintext, key)
        result = decrypt_fds_data(encrypted, object_key)
        assert result == plaintext


class TestParseFdsHeader:
    def test_parses_header_fields(self):
        data_valid = bytes([0b11000000])
        timestamp = 1774241243
        header_bytes = build_header(timestamp, 28, 2, 8, data_valid)
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
        assert get_record_data_valid_len(28, 2) == 1

    def test_strength_training_v3(self):
        assert get_record_data_valid_len(28, 3) == 2

    def test_strength_training_v5(self):
        assert get_record_data_valid_len(28, 5) == 2


class TestOneDimenValid:
    def test_both_types_valid(self):
        types = [OneDimenType(TYPE_HR, 1, 1), OneDimenType(TYPE_CALORIES, 1, 1)]
        data_valid = bytes([0b11000000])
        result = parse_one_dimen_valid(types, version=1, data_valid=data_valid)
        assert result[TYPE_HR] is True
        assert result[TYPE_CALORIES] is True

    def test_only_hr_valid(self):
        types = [OneDimenType(TYPE_HR, 1, 1), OneDimenType(TYPE_CALORIES, 1, 1)]
        data_valid = bytes([0b10000000])
        result = parse_one_dimen_valid(types, version=1, data_valid=data_valid)
        assert result[TYPE_HR] is True
        assert result[TYPE_CALORIES] is False

    def test_unsupported_version_marked_false(self):
        types = [OneDimenType(TYPE_HR, 1, 1), OneDimenType(TYPE_SPO2, 1, 3)]
        data_valid = bytes([0b10000000])
        result = parse_one_dimen_valid(types, version=2, data_valid=data_valid)
        assert result[TYPE_HR] is True
        assert result[TYPE_SPO2] is False


class TestFourDimenValid:
    def test_all_exist_and_high(self):
        types = [
            FourDimenType(TYPE_HR, 1, 3),
            FourDimenType(TYPE_CALORIES, 1, 3),
            FourDimenType(TYPE_SPO2, 1, 3),
            FourDimenType(TYPE_STRESS, 1, 3),
        ]
        data_valid = bytes([0xCC, 0xCC])
        result = parse_four_dimen_valid(types, version=3, data_valid=data_valid)
        for type_id in [TYPE_HR, TYPE_CALORIES, TYPE_SPO2, TYPE_STRESS]:
            assert result[type_id].exist is True
            assert result[type_id].high is True
            assert result[type_id].middle is False
            assert result[type_id].low is False

    def test_unsupported_version_no_exist(self):
        types = [FourDimenType(TYPE_HR, 1, 5)]
        data_valid = bytes([0xC0])
        result = parse_four_dimen_valid(types, version=3, data_valid=data_valid)
        assert result[TYPE_HR].exist is False


class TestExtractHighValue:
    def test_no_bit_extraction(self):
        data_type = FourDimenType(TYPE_HR, 1, 1)
        assert extract_high_value(0xAB, data_type) == 0xAB

    def test_single_bit_extraction(self):
        data_type = FourDimenType(TYPE_INTEGER_KM, 1, 1, high_start_bit=7, high_bit_count=1)
        assert extract_high_value(0xFF, data_type) == 1
        assert extract_high_value(0x7F, data_type) == 0
        assert extract_high_value(0x80, data_type) == 1

    def test_nibble_extraction(self):
        data_type = FourDimenType(TYPE_CALORIES, 1, 1, high_start_bit=4, high_bit_count=4)
        assert extract_high_value(0xA5, data_type) == 0xA
        assert extract_high_value(0x30, data_type) == 3

    def test_multi_bit_wide_field(self):
        data_type = FourDimenType(44, 4, 5, high_start_bit=26, high_bit_count=6)
        raw = 42 << 26
        assert extract_high_value(raw, data_type) == 42

    def test_max_version_excludes_field_at_higher_version(self):
        types = [
            FourDimenType(TYPE_CALORIES, 1, 1),
            FourDimenType(TYPE_HR, 1, 1),
            FourDimenType(TYPE_DISTANCE, 1, 1, max_support_version=3),
        ]
        data_valid = bytes([0xCC])
        result = parse_four_dimen_valid(types, version=4, data_valid=data_valid)
        assert result[TYPE_CALORIES].exist is True
        assert result[TYPE_HR].exist is True
        assert result[TYPE_DISTANCE].exist is False

    def test_max_version_includes_field_at_equal_version(self):
        types = [
            FourDimenType(TYPE_CALORIES, 1, 1),
            FourDimenType(TYPE_HR, 1, 1),
            FourDimenType(TYPE_DISTANCE, 1, 1, max_support_version=3),
        ]
        data_valid = bytes([0xCC, 0xC0])
        result = parse_four_dimen_valid(types, version=3, data_valid=data_valid)
        assert result[TYPE_CALORIES].exist is True
        assert result[TYPE_HR].exist is True
        assert result[TYPE_DISTANCE].exist is True

    def test_max_version_no_nibble_consumed(self):
        types = [
            FourDimenType(TYPE_CALORIES, 1, 1),
            FourDimenType(TYPE_HR, 1, 1, max_support_version=2),
            FourDimenType(TYPE_DISTANCE, 1, 1),
        ]
        data_valid = bytes([0xCC])
        result = parse_four_dimen_valid(types, version=3, data_valid=data_valid)
        assert result[TYPE_CALORIES].exist is True
        assert result[TYPE_CALORIES].high is True
        assert result[TYPE_HR].exist is False
        assert result[TYPE_DISTANCE].exist is True