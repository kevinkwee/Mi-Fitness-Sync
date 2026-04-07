from __future__ import annotations

import pytest

from mi_fitness_sync.cli.speed_parser import parse_speed_input
from mi_fitness_sync.exceptions import MiFitnessError


class TestParseSpeedInputKmh:
    def test_plain_integer(self):
        # 180 km/h → 50 m/s
        assert parse_speed_input("180") == pytest.approx(50.0)

    def test_plain_float(self):
        # 36 km/h → 10 m/s
        assert parse_speed_input("36.0") == pytest.approx(10.0)

    def test_with_kmh_suffix(self):
        assert parse_speed_input("180kmh") == pytest.approx(50.0)

    def test_with_kmh_suffix_uppercase(self):
        assert parse_speed_input("90KMH") == pytest.approx(25.0)

    def test_with_whitespace(self):
        assert parse_speed_input("  72  ") == pytest.approx(20.0)


class TestParseSpeedInputPace:
    def test_colon_format(self):
        # 7:30 per km → 7*60+30 = 450 s/km → 1000/450 ≈ 2.222 m/s
        assert parse_speed_input("7:30") == pytest.approx(1000.0 / 450.0)

    def test_with_km_suffix(self):
        assert parse_speed_input("7:30/km") == pytest.approx(1000.0 / 450.0)

    def test_four_minute_pace(self):
        # 4:00 per km → 240 s/km → ~4.167 m/s
        assert parse_speed_input("4:00") == pytest.approx(1000.0 / 240.0)

    def test_single_digit_seconds(self):
        # 5:5 → 305 s/km
        assert parse_speed_input("5:5") == pytest.approx(1000.0 / 305.0)


class TestParseSpeedInputErrors:
    def test_zero_kmh_raises(self):
        with pytest.raises(MiFitnessError, match="greater than zero"):
            parse_speed_input("0")

    def test_zero_pace_raises(self):
        with pytest.raises(MiFitnessError, match="greater than zero"):
            parse_speed_input("0:00")

    def test_seconds_over_59_raises(self):
        with pytest.raises(MiFitnessError, match="seconds must be 0–59"):
            parse_speed_input("7:60")

    def test_unparseable_raises(self):
        with pytest.raises(MiFitnessError, match="Cannot parse speed"):
            parse_speed_input("fast")

    def test_empty_string_raises(self):
        with pytest.raises(MiFitnessError, match="Cannot parse speed"):
            parse_speed_input("")
