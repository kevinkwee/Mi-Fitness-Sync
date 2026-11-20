from __future__ import annotations

import pytest

from mi_fitness_sync.cli.speed_parser import parse_speed_input
from mi_fitness_sync.exceptions import MiFitnessError


class TestParseSpeedInputKmh:
    def test_plain_integer(self) -> None:
        # 180 km/h is 50 m/s
        assert parse_speed_input("180") == pytest.approx(50.0)

    def test_plain_float(self) -> None:
        # 36 km/h is 10 m/s
        assert parse_speed_input("36.0") == pytest.approx(10.0)

    def test_with_kmh_suffix(self) -> None:
        assert parse_speed_input("180kmh") == pytest.approx(50.0)

    def test_with_kmh_suffix_uppercase(self) -> None:
        assert parse_speed_input("90KMH") == pytest.approx(25.0)

    def test_with_whitespace(self) -> None:
        assert parse_speed_input("  72  ") == pytest.approx(20.0)


class TestParseSpeedInputPace:
    def test_colon_format(self) -> None:
        # 7:30 per km is 450 s/km, so 1000/450, about 2.222 m/s
        assert parse_speed_input("7:30") == pytest.approx(1000.0 / 450.0)

    def test_with_km_suffix(self) -> None:
        assert parse_speed_input("7:30/km") == pytest.approx(1000.0 / 450.0)

    def test_four_minute_pace(self) -> None:
        # 4:00 per km is 240 s/km, about 4.167 m/s
        assert parse_speed_input("4:00") == pytest.approx(1000.0 / 240.0)

    def test_single_digit_seconds(self) -> None:
        # 5:5 is 305 s/km
        assert parse_speed_input("5:5") == pytest.approx(1000.0 / 305.0)


class TestParseSpeedInputErrors:
    def test_zero_kmh_raises(self) -> None:
        with pytest.raises(MiFitnessError, match="greater than zero"):
            parse_speed_input("0")

    def test_zero_pace_raises(self) -> None:
        with pytest.raises(MiFitnessError, match="greater than zero"):
            parse_speed_input("0:00")

    def test_seconds_over_59_raises(self) -> None:
        with pytest.raises(MiFitnessError, match="seconds must be 0-59"):
            parse_speed_input("7:60")

    def test_unparseable_raises(self) -> None:
        with pytest.raises(MiFitnessError, match="Cannot parse speed"):
            parse_speed_input("fast")

    def test_empty_string_raises(self) -> None:
        with pytest.raises(MiFitnessError, match="Cannot parse speed"):
            parse_speed_input("")
