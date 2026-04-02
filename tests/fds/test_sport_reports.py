from __future__ import annotations

import struct

from mi_fitness_sync.fds.sport_reports import (
    FREE_TRAINING_REPORT_FIELDS,
    OUTDOOR_SPORT_REPORT_FIELDS,
    compute_report_validity_len,
    get_report_data_valid_len,
    parse_report_validity,
    parse_sport_report,
)


def test_compute_report_validity_len_free_training_v1():
    assert compute_report_validity_len(FREE_TRAINING_REPORT_FIELDS, 1) == 2


def test_compute_report_validity_len_outdoor_v1():
    assert compute_report_validity_len(OUTDOOR_SPORT_REPORT_FIELDS, 1) == 4


def test_compute_report_validity_len_outdoor_v4():
    assert compute_report_validity_len(OUTDOOR_SPORT_REPORT_FIELDS, 4) == 5


def test_parse_report_validity_bitmap():
    data_valid = bytes([0xFF, 0xFE])
    valid_map = parse_report_validity(FREE_TRAINING_REPORT_FIELDS, version=1, data_valid=data_valid)
    assert valid_map[1] is True
    assert valid_map[6] is True
    assert valid_map[16] is True
    assert valid_map[34] is True


def test_parse_free_training_report_v1():
    sport_type = 8
    version = 1
    timestamp = 1700000000

    body = b""
    body += struct.pack("<I", 1700000000)
    body += struct.pack("<I", 1700003600)
    body += struct.pack("<I", 3600)
    body += struct.pack("<H", 350)
    body += bytes([130, 175, 95])
    body += struct.pack("<f", 3.5)
    body += bytes([42])
    body += struct.pack("<H", 600)
    body += struct.pack("<I", 120)
    body += struct.pack("<I", 300)
    body += struct.pack("<I", 1800)
    body += struct.pack("<I", 900)
    body += struct.pack("<I", 480)

    data_valid = bytes([0xFF, 0xFE])
    header = struct.pack("<I", timestamp) + bytes([32, version, sport_type, 0x00]) + data_valid

    report = parse_sport_report(header + body, sport_type)

    assert report is not None
    assert report.start_time == 1700000000
    assert report.end_time == 1700003600
    assert report.duration == 3600
    assert report.calories == 350
    assert report.avg_hr == 130
    assert report.max_hr == 175
    assert report.min_hr == 95
    assert abs(report.train_effect - 3.5) < 0.01
    assert report.recovery_time == 600
    assert report.hr_extreme_duration == 120
    assert report.hr_aerobic_duration == 1800


def test_parse_outdoor_sport_report_v1():
    sport_type = 1
    version = 1

    body = b""
    body += struct.pack("<I", 1700000000)
    body += struct.pack("<I", 1700001800)
    body += struct.pack("<I", 1800)
    body += struct.pack("<I", 5000)
    body += struct.pack("<H", 250)
    body += struct.pack("<I", 400)
    body += struct.pack("<I", 300)
    body += struct.pack("<f", 4.2)
    body += struct.pack("<I", 2500)
    body += struct.pack("<H", 190)
    body += bytes([145, 180, 110])
    body += struct.pack("<f", 50.5)
    body += struct.pack("<f", 30.2)
    body += struct.pack("<f", 100.0)
    body += struct.pack("<f", 120.0)
    body += struct.pack("<f", 80.0)
    body += struct.pack("<f", 4.0)
    body += bytes([55, 30])
    body += struct.pack("<H", 720)
    body += struct.pack("<I", 60)
    body += struct.pack("<I", 300)
    body += struct.pack("<I", 900)
    body += struct.pack("<I", 400)
    body += struct.pack("<I", 140)

    data_valid = bytes([0xFF, 0xFF, 0xFF, 0xF8])
    header = struct.pack("<I", 1700000000) + bytes([32, version, sport_type, 0x00]) + data_valid

    report = parse_sport_report(header + body, sport_type)

    assert report is not None
    assert report.distance == 5000
    assert report.calories == 250
    assert report.steps == 2500
    assert report.avg_hr == 145
    assert report.max_hr == 180
    assert report.min_hr == 110
    assert report.vo2max == 55
    assert abs(report.max_speed - 4.2) < 0.01
    assert abs(report.rise_height - 50.5) < 0.1
    assert report.max_pace == 400
    assert report.min_pace == 300


def test_parse_report_partial_validity():
    sport_type = 8
    version = 1

    body = b""
    body += struct.pack("<I", 1700000000)
    body += struct.pack("<I", 1700003600)
    body += struct.pack("<I", 3600)
    body += struct.pack("<H", 350)
    body += bytes([130, 175, 95])
    body += struct.pack("<f", 3.5)
    body += bytes([42])
    body += struct.pack("<H", 600)
    body += struct.pack("<I", 0)
    body += struct.pack("<I", 0)
    body += struct.pack("<I", 0)
    body += struct.pack("<I", 0)
    body += struct.pack("<I", 0)

    data_valid = bytes([0xF8, 0x00])
    header = struct.pack("<I", 1700000000) + bytes([32, version, sport_type, 0x00]) + data_valid

    report = parse_sport_report(header + body, sport_type)

    assert report is not None
    assert report.start_time == 1700000000
    assert report.duration == 3600
    assert report.calories == 350
    assert report.avg_hr == 130
    assert report.max_hr is None
    assert report.min_hr is None
    assert report.train_effect is None
    assert report.recovery_time is None


def test_parse_report_unsupported_sport_type():
    header = struct.pack("<I", 1700000000) + bytes([32, 1, 99, 0x00])
    assert parse_sport_report(header, 99) is None


def test_get_report_data_valid_len_free_training_v1():
    assert get_report_data_valid_len(8, 1) == 2


def test_get_report_data_valid_len_outdoor_run_versions():
    assert get_report_data_valid_len(1, 1) == 4
    assert get_report_data_valid_len(1, 4) == 5


def test_get_report_data_valid_len_unsupported_sport_type():
    assert get_report_data_valid_len(99, 1) is None