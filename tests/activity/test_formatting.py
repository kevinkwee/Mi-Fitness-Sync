from __future__ import annotations

from mi_fitness_sync.activity.formatting import parse_cli_time


def test_parse_cli_time_accepts_unix_seconds():
    assert parse_cli_time("1717200000") == 1717200000


def test_parse_cli_time_accepts_iso8601_utc():
    assert parse_cli_time("2024-01-01T00:00:00Z") == 1704067200