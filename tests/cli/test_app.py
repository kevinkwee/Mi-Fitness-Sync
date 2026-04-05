from __future__ import annotations

import json

from mi_fitness_sync.activity.models import Activity
from mi_fitness_sync.cli import app as cli
from mi_fitness_sync.exceptions import CaptchaRequiredError, XiaomiApiError
from mi_fitness_sync.strava.store import StravaTokenState


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


def test_export_activity_uses_sanitized_title_and_local_start_time(monkeypatch, tmp_path, capsys, auth_state, sample_activity_detail):
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    class DummyClient:
        def __init__(self, state, **kwargs):
            assert state == auth_state

        def get_activity_detail(self, activity_id):
            assert activity_id == "sid:key:1"
            return sample_activity_detail

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyClient)
    monkeypatch.setattr(cli, "get_exports_dir", lambda: tmp_path / "exports")
    monkeypatch.setattr(
        cli,
        "render_export",
        lambda detail, file_format, compress: type(
            "Export",
            (),
            {"file_format": file_format, "compressed": compress, "payload": b"payload"},
        )(),
    )

    exit_code = cli.main(["export-activity", "sid:key:1", "--format", "gpx"])
    captured = capsys.readouterr().out
    output_path = tmp_path / "exports" / "Morning_Run_20240601_000000.gpx"

    assert exit_code == 0
    assert output_path.read_bytes() == b"payload"
    assert str(output_path) in captured


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


# ---------------------------------------------------------------------------
# Strava CLI command tests
# ---------------------------------------------------------------------------

def _make_strava_token_state() -> StravaTokenState:
    return StravaTokenState(
        client_id="12345",
        client_secret="secret123",
        access_token="access-abc",
        refresh_token="refresh-xyz",
        expires_at=1700000000,
        athlete_id=42,
        created_at="2026-04-01T00:00:00+00:00",
        updated_at="2026-04-01T00:00:00+00:00",
    )


def test_strava_login_success(monkeypatch, capsys, tmp_path):
    import mi_fitness_sync.strava.auth as strava_auth

    monkeypatch.setattr(strava_auth, "run_oauth_flow", lambda cid, csecret, port=5478: {
        "access_token": "at",
        "refresh_token": "rt",
        "expires_at": 9999,
        "athlete": {"id": 42},
    })

    token_path = tmp_path / "tokens.json"
    exit_code = cli.main([
        "strava-login",
        "--client-id", "123",
        "--client-secret", "secret",
        "--strava-token-path", str(token_path),
    ])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Strava login succeeded" in captured.out
    assert "Athlete ID: 42" in captured.out
    assert token_path.exists()


def test_strava_login_missing_credentials(monkeypatch, capsys):
    monkeypatch.setattr("builtins.input", lambda prompt: "")

    exit_code = cli.main(["strava-login"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "client_id and client_secret are required" in captured.err


def test_strava_login_oauth_error(monkeypatch, capsys):
    import mi_fitness_sync.strava.auth as strava_auth
    from mi_fitness_sync.exceptions import StravaAuthError

    def failing_flow(*args, **kwargs):
        raise StravaAuthError("OAuth callback timed out after 120 seconds.")

    monkeypatch.setattr(strava_auth, "run_oauth_flow", failing_flow)

    exit_code = cli.main([
        "strava-login",
        "--client-id", "123",
        "--client-secret", "secret",
    ])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "timed out" in captured.err


def test_strava_status_success(capsys, tmp_path):
    from mi_fitness_sync.strava.store import save_tokens

    state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(state, str(token_path))

    exit_code = cli.main(["strava-status", "--strava-token-path", str(token_path)])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Strava auth state found" in captured.out
    assert "Athlete ID: 42" in captured.out


def test_strava_status_no_tokens(capsys, tmp_path):
    token_path = tmp_path / "nonexistent.json"

    exit_code = cli.main(["strava-status", "--strava-token-path", str(token_path)])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "No Strava token state found" in captured.err


def test_upload_to_strava_success(monkeypatch, capsys, tmp_path, auth_state, sample_activity_detail):
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    # Write a real Strava token file
    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))

    class DummyClient:
        def __init__(self, state, **kwargs):
            pass

        def get_activity_detail(self, activity_id):
            assert activity_id == "sid:key:1"
            return sample_activity_detail

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyClient)
    monkeypatch.setattr(
        cli,
        "render_export",
        lambda detail, file_format, compress=False: type(
            "Export", (), {"payload": b"fitdata", "file_format": "fit", "compressed": False},
        )(),
    )

    class DummyStravaClient:
        def __init__(self, state, token_path=None):
            pass

        def upload_activity(self, payload, sport_type=None, external_id=None):
            assert payload == b"fitdata"
            return {"activity_id": 12345}

    monkeypatch.setattr(strava_client_mod, "StravaClient", DummyStravaClient)

    output_fit = tmp_path / "activity.fit"
    exit_code = cli.main([
        "upload-to-strava", "sid:key:1",
        "--strava-token-path", str(token_path),
        "--output", str(output_fit),
    ])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert output_fit.read_bytes() == b"fitdata"
    assert "Uploaded to Strava successfully" in captured.out
    assert "https://www.strava.com/activities/12345" in captured.out


def test_upload_to_strava_no_tokens(monkeypatch, capsys, tmp_path, auth_state):
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    token_path = tmp_path / "nonexistent.json"
    exit_code = cli.main([
        "upload-to-strava", "sid:key:1",
        "--strava-token-path", str(token_path),
    ])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "No Strava token state found" in captured.err
