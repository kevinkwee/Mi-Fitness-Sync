from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from mi_fitness_sync.activity.models import Activity
from mi_fitness_sync.export import bulk as bulk_mod
from mi_fitness_sync.export.bulk import BulkExportOptions, BulkExportRecord, BulkExporter
from mi_fitness_sync.export.files import default_export_path


def _make_activity(*, sid: str, key: str, start_time: int, title: str) -> Activity:
    return Activity(
        activity_id=f"{sid}:{key}:{start_time}",
        sid=sid,
        key=key,
        category="outdoor_run",
        sport_type=1,
        title=title,
        start_time=start_time,
        end_time=start_time + 3600,
        duration_seconds=3600,
        distance_meters=10000,
        calories=700,
        steps=12000,
        sync_state="server",
        next_key=None,
        raw_record={"sid": sid, "key": key, "time": start_time},
        raw_report={"name": title},
    )


def _make_options(tmp_path: Path) -> BulkExportOptions:
    output_dir = tmp_path / "exports"
    return BulkExportOptions(
        output_dir=output_dir,
        manifest_path=output_dir / "bulk_export_gpx.jsonl",
        file_format="gpx",
        compress=False,
        country_code="ID",
        no_cache=True,
        cache_dir=None,
        workers=1,
        overwrite=False,
        smooth=True,
        outlier_speed_mps=None,
        smooth_mode="match",
        include_sport_report=False,
    )


def test_bulk_exporter_writes_unique_files_and_manifest(monkeypatch, tmp_path, auth_state, sample_activity_detail):
    activities = [
        _make_activity(sid="sid-a", key="key", start_time=1717200000, title="Morning Run"),
        _make_activity(sid="sid-b", key="key", start_time=1717200000, title="Morning Run"),
    ]
    options = _make_options(tmp_path)

    class DummyClient:
        def __init__(self, state, **kwargs):
            pass

        def get_activity_detail(self, activity, *, include_sport_report=True, include_recovery_rate=True):
            assert include_sport_report is False
            assert include_recovery_rate is False
            return replace(
                sample_activity_detail,
                activity=activity,
                detail_sid=activity.sid,
                detail_key=activity.key,
                detail_time=activity.start_time or 0,
                zone_offset_seconds=0,
            )

    monkeypatch.setattr(bulk_mod, "MiFitnessActivitiesClient", DummyClient)
    monkeypatch.setattr(
        bulk_mod,
        "render_export",
        lambda detail, file_format, compress=False, **kwargs: type(
            "Export",
            (),
            {"payload": detail.activity.activity_id.encode("utf-8"), "file_format": file_format, "compressed": compress},
        )(),
    )

    progress: list[BulkExportRecord] = []
    summary = BulkExporter(auth_state, options).run(activities, progress_callback=progress.append)

    exported_files = sorted(options.output_dir.glob("*.gpx"))
    manifest_lines = options.manifest_path.read_text(encoding="utf-8").strip().splitlines()

    assert summary.exported == 2
    assert len(exported_files) == 2
    assert len({path.name for path in exported_files}) == 2
    assert len(manifest_lines) == 2
    assert {record.status for record in progress} == {"completed"}


def test_bulk_exporter_skips_completed_manifest_entries(monkeypatch, tmp_path, auth_state):
    activity = _make_activity(sid="sid-a", key="key", start_time=1717200000, title="Morning Run")
    options = _make_options(tmp_path)
    output_path = default_export_path(
        options.output_dir,
        activity,
        options.file_format,
        compress=options.compress,
        include_activity_id=True,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(b"existing")
    manifest_record = BulkExportRecord(
        activity_id=activity.activity_id,
        status="completed",
        output_path=str(output_path),
        bytes_written=len(b"existing"),
        error=None,
        recorded_at="2026-05-11T00:00:00+00:00",
    )
    options.manifest_path.write_text(
        json.dumps(
            {
                "activity_id": manifest_record.activity_id,
                "status": manifest_record.status,
                "output_path": manifest_record.output_path,
                "bytes_written": manifest_record.bytes_written,
                "error": manifest_record.error,
                "recorded_at": manifest_record.recorded_at,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    class DummyClient:
        def __init__(self, state, **kwargs):
            raise AssertionError("worker client should not be created for resumed entries")

    monkeypatch.setattr(bulk_mod, "MiFitnessActivitiesClient", DummyClient)

    progress: list[BulkExportRecord] = []
    summary = BulkExporter(auth_state, options).run([activity], progress_callback=progress.append)

    assert summary.skipped_manifest == 1
    assert progress[0].status == "skipped_manifest"


def test_bulk_exporter_records_existing_files_without_reexport(monkeypatch, tmp_path, auth_state):
    activity = _make_activity(sid="sid-a", key="key", start_time=1717200000, title="Morning Run")
    options = _make_options(tmp_path)
    output_path = default_export_path(
        options.output_dir,
        activity,
        options.file_format,
        compress=options.compress,
        include_activity_id=True,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(b"existing")

    class DummyClient:
        def __init__(self, state, **kwargs):
            raise AssertionError("worker client should not be created for existing files")

    monkeypatch.setattr(bulk_mod, "MiFitnessActivitiesClient", DummyClient)

    summary = BulkExporter(auth_state, options).run([activity])
    manifest_lines = options.manifest_path.read_text(encoding="utf-8").strip().splitlines()

    assert summary.skipped_existing == 1
    assert len(manifest_lines) == 1
    payload = json.loads(manifest_lines[0])
    assert payload["status"] == "skipped_existing"


def test_bulk_exporter_failed_records_do_not_store_output_path(monkeypatch, tmp_path, auth_state):
    activity = _make_activity(sid="sid-a", key="key", start_time=1717200000, title="Morning Run")
    options = _make_options(tmp_path)

    class DummyClient:
        def __init__(self, state, **kwargs):
            pass

        def get_activity_detail(self, activity, *, include_sport_report=True, include_recovery_rate=True):
            raise RuntimeError("boom")

    monkeypatch.setattr(bulk_mod, "MiFitnessActivitiesClient", DummyClient)

    summary = BulkExporter(auth_state, options).run([activity])
    manifest_lines = options.manifest_path.read_text(encoding="utf-8").strip().splitlines()

    assert summary.failed == 1
    assert len(manifest_lines) == 1
    payload = json.loads(manifest_lines[0])
    assert payload["status"] == "failed"
    assert payload["output_path"] is None
