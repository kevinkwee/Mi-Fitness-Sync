from __future__ import annotations

import json

from mi_fitness_sync.activity.models import Activity
from mi_fitness_sync.cli import app as cli
from mi_fitness_sync.exceptions import CaptchaRequiredError, XiaomiApiError


def test_format_error_includes_xiaomi_api_code():
    error = XiaomiApiError("boom", code=401)

    assert cli.format_error(error) == "boom (code=401)"


def test_format_error_for_captcha():
    error = CaptchaRequiredError("https://example.com/captcha")

    assert cli.format_error(error) == "Login requires a captcha challenge. URL: https://example.com/captcha"


def test_auth_status_json_output(monkeypatch, capsys, auth_state):
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    exit_code = cli.main(["auth-status", "--json"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert json.loads(output)["email"] == auth_state.email


def test_main_returns_error_for_invalid_limit(monkeypatch, capsys, auth_state):
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    exit_code = cli.main(["list-activities", "--limit", "0"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "--limit must be greater than zero." in captured.err


def test_main_returns_error_for_invalid_country_override(monkeypatch, capsys, auth_state):
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    exit_code = cli.main(["list-activities", "--country-code", "ZZ"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Unsupported Mi Fitness country override: ZZ." in captured.err


def test_list_activities_json_output(monkeypatch, capsys, auth_state):
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    sample_activity = Activity(
        activity_id="sid:key:1",
        sid="sid",
        key="key",
        category="outdoor_run",
        sport_type=1,
        title="Morning Run",
        start_time=1717200000,
        end_time=1717203600,
        duration_seconds=3600,
        distance_meters=10000,
        calories=700,
        steps=12000,
        sync_state="server",
        next_key=None,
        raw_record={"sid": "sid", "key": "key"},
        raw_report={"name": "Morning Run"},
    )

    class DummyClient:
        def __init__(self, state, **kwargs):
            assert state == auth_state
            self.country_code = kwargs.get("country_code")

        def list_activities(self, *, start_time, end_time, limit, category=None):
            assert start_time == 1704067200
            assert end_time is None
            assert limit == 1
            assert category is None
            return [sample_activity]

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyClient)

    exit_code = cli.main([
        "list-activities",
        "--since",
        "2024-01-01T00:00:00Z",
        "--limit",
        "1",
        "--country-code",
        "ID",
        "--json",
    ])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output[0]["title"] == "Morning Run"


def test_activity_detail_json_output(monkeypatch, capsys, auth_state, sample_activity_detail):
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    class DummyClient:
        def __init__(self, state, **kwargs):
            assert state == auth_state

        def get_activity_detail(self, activity_id):
            assert activity_id == "sid:key:1"
            return sample_activity_detail

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyClient)

    exit_code = cli.main(["activity-detail", "sid:key:1", "--json"])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["activity"]["title"] == "Morning Run"
    assert output["track_points"][0]["heart_rate"] == 120


def test_export_activity_writes_requested_file(monkeypatch, tmp_path, capsys, auth_state, sample_activity_detail):
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    class DummyClient:
        def __init__(self, state, **kwargs):
            assert state == auth_state

        def get_activity_detail(self, activity_id):
            assert activity_id == "sid:key:1"
            return sample_activity_detail

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyClient)
    monkeypatch.setattr(
        cli,
        "render_export",
        lambda detail, file_format, compress: type(
            "Export",
            (),
            {"file_format": file_format, "compressed": compress, "payload": b"payload"},
        )(),
    )

    output_path = tmp_path / "exports" / "run.gpx.gz"
    exit_code = cli.main(["export-activity", "sid:key:1", "--format", "gpx", "--gzip", "--output", str(output_path)])
    captured = capsys.readouterr().out

    assert exit_code == 0
    assert output_path.read_bytes() == b"payload"
    assert "Compressed: yes" in captured


def test_activity_detail_no_cache_flag(monkeypatch, auth_state, sample_activity_detail):
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)
    captured_kwargs = {}

    class DummyClient:
        def __init__(self, state, **kwargs):
            captured_kwargs.update(kwargs)

        def get_activity_detail(self, activity_id):
            return sample_activity_detail

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyClient)

    exit_code = cli.main(["activity-detail", "sid:key:1", "--no-cache", "--json"])
    assert exit_code == 0
    assert captured_kwargs["no_cache"] is True


def test_activity_detail_cache_dir_flag(monkeypatch, tmp_path, auth_state, sample_activity_detail):
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)
    captured_kwargs = {}

    class DummyClient:
        def __init__(self, state, **kwargs):
            captured_kwargs.update(kwargs)

        def get_activity_detail(self, activity_id):
            return sample_activity_detail

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyClient)

    cache_path = str(tmp_path / "custom_cache")
    exit_code = cli.main(["activity-detail", "sid:key:1", "--cache-dir", cache_path, "--json"])
    assert exit_code == 0
    assert captured_kwargs["cache_dir"] == cache_path