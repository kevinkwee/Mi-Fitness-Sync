from __future__ import annotations

from mi_fitness_sync.fds.common import decrypt_fds_data
from mi_fitness_sync.fds.recovery_rate import get_recovery_rate_data_valid_len, parse_recovery_rate_record
from tests.fds.support import b64url_encode_no_pad, build_header, build_recovery_rate_body, encrypt, make_aes_key


def test_recovery_rate_validity_len_v1():
    assert get_recovery_rate_data_valid_len(1) == 1


def test_recovery_rate_validity_len_unknown_version():
    assert get_recovery_rate_data_valid_len(99) is None


def test_parse_recovery_rate_record_basic():
    rates = [80, 75, 70]
    body = build_recovery_rate_body(3, 1717200100, 120, 85, rates)
    data_valid = bytes([0x80])
    decrypted = build_header(1717200000, 28, 1, 8, data_valid) + body

    result = parse_recovery_rate_record(decrypted)

    assert result is not None
    assert result.recover_timestamp == 1717200100
    assert result.heart_rate == 120
    assert result.recover_rate == 8.5
    assert [sample.rate for sample in result.rate_samples] == rates
    assert result.start_rate == 80
    assert result.end_rate == 70


def test_parse_recovery_rate_record_empty_rates():
    body = build_recovery_rate_body(0, 1717200100, 90, 0, [])
    data_valid = bytes([0x80])
    decrypted = build_header(1717200000, 28, 1, 8, data_valid) + body

    result = parse_recovery_rate_record(decrypted)

    assert result is not None
    assert len(result.rate_samples) == 0
    assert result.start_rate is None
    assert result.end_rate is None


def test_parse_recovery_rate_record_too_short_returns_none():
    assert parse_recovery_rate_record(b"\x00" * 5) is None


def test_parse_recovery_rate_record_unsupported_version_returns_none():
    decrypted = build_header(1717200000, 28, 99, 8, bytes([0x80])) + build_recovery_rate_body(0, 1717200100, 90, 0, [])
    assert parse_recovery_rate_record(decrypted) is None


def test_parse_recovery_rate_record_truncated_body_returns_none():
    decrypted = build_header(1717200000, 28, 1, 8, bytes([0x80])) + b"\x00\x00\x00\x00"
    assert parse_recovery_rate_record(decrypted) is None


def test_parse_recovery_rate_record_truncated_rate_samples():
    body = build_recovery_rate_body(5, 1717200100, 100, 50, [60, 55])
    data_valid = bytes([0x80])
    decrypted = build_header(1717200000, 28, 1, 8, data_valid) + body

    result = parse_recovery_rate_record(decrypted)

    assert result is not None
    assert len(result.rate_samples) == 2
    assert result.start_rate == 60
    assert result.end_rate == 55


def test_parse_recovery_rate_record_full_decrypt_pipeline():
    rates = [90, 85, 80, 75]
    body = build_recovery_rate_body(4, 1717200200, 130, 42, rates)
    data_valid = bytes([0x80])
    plaintext = build_header(1717200000, 28, 1, 8, data_valid) + body

    key = make_aes_key()
    object_key = b64url_encode_no_pad(key)
    encrypted_body = encrypt(plaintext, key)

    decrypted = decrypt_fds_data(encrypted_body, object_key)
    result = parse_recovery_rate_record(decrypted)

    assert result is not None
    assert result.recover_timestamp == 1717200200
    assert result.heart_rate == 130
    assert result.recover_rate == 4.2
    assert [sample.rate for sample in result.rate_samples] == rates
    assert result.start_rate == 90
    assert result.end_rate == 75