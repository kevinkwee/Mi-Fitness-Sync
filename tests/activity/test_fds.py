from __future__ import annotations

from unittest.mock import MagicMock

from mi_fitness_sync.activity.fds import ActivityFdsService
from mi_fitness_sync.activity.models import Activity
from mi_fitness_sync.activity.utils import build_fds_suffix


def _make_activity(
    *,
    sid: str = "123456789",
    record_time: int = 1700000100,
    report_time: int = 1700000000,
    proto_type: int = 22,
    timezone_offset: int = 28,
) -> Activity:
    """Build an Activity with explicit record-level and report-level timestamps."""
    return Activity(
        activity_id=f"{sid}:outdoor_running:{record_time}",
        sid=sid,
        key="outdoor_running",
        category="outdoor_run",
        sport_type=proto_type,
        title="Test Run",
        start_time=record_time,
        end_time=record_time + 3600,
        duration_seconds=3600,
        distance_meters=5000,
        calories=300,
        steps=6000,
        sync_state="server",
        next_key=None,
        raw_record={"sid": sid, "key": "outdoor_running", "time": record_time},
        raw_report={
            "time": report_time,
            "proto_type": proto_type,
            "timezone": timezone_offset,
        },
    )


def _make_service() -> ActivityFdsService:
    return ActivityFdsService(
        session=MagicMock(),
        transport=MagicMock(),
        timeout=10,
        cache=None,
    )


class TestBuildContext:
    """Verify _build_context uses the report-level timestamp, not the record-level one."""

    def test_uses_report_time_when_record_and_report_differ(self):
        activity = _make_activity(record_time=1700000100, report_time=1700000000)
        service = _make_service()

        context = service._build_context(activity)

        assert context is not None
        assert context.timestamp == 1700000000, (
            "Expected report-level timestamp, got record-level timestamp"
        )

    def test_falls_back_to_start_time_when_report_time_missing(self):
        activity = _make_activity(record_time=1700000100, report_time=1700000000)
        activity.raw_report.pop("time")
        service = _make_service()

        context = service._build_context(activity)

        assert context is not None
        assert context.timestamp == activity.start_time

    def test_context_fields_populated_from_report(self):
        activity = _make_activity(proto_type=22, timezone_offset=28)
        service = _make_service()

        context = service._build_context(activity)

        assert context is not None
        assert context.sid == activity.sid
        assert context.proto_type == 22
        assert context.timezone_offset == 28


class TestRequestItemUsesReportTimestamp:
    """End-to-end: divergent timestamps produce FDS suffix from report time."""

    def test_request_item_suffix_uses_report_timestamp(self):
        record_time = 1700000100
        report_time = 1700000000
        activity = _make_activity(record_time=record_time, report_time=report_time)
        service = _make_service()

        context = service._build_context(activity)
        assert context is not None

        item = service._build_request_item(context, file_type=0)

        expected_suffix = build_fds_suffix(
            sid=activity.sid,
            timestamp=report_time,
            timezone_offset=28,
            sport_type=22,
            file_type=0,
        )
        wrong_suffix = build_fds_suffix(
            sid=activity.sid,
            timestamp=record_time,
            timezone_offset=28,
            sport_type=22,
            file_type=0,
        )

        assert item["suffix"] == expected_suffix
        assert item["timestamp"] == report_time
        assert item["suffix"] != wrong_suffix, (
            "Suffix must NOT match the record-level timestamp"
        )
