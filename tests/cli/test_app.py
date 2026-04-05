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

        def list_activities(self, *, after, before, per_page=30, page=1):
            return []

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


# ---------------------------------------------------------------------------
# Duplicate check tests
# ---------------------------------------------------------------------------

def _setup_upload_mocks(monkeypatch, tmp_path, auth_state, sample_activity_detail, *, strava_activities):
    """Helper to wire up all monkeypatches common to upload duplicate-check tests.

    Returns (token_path, output_path, uploaded) where *uploaded* is a list
    that gets an item appended when upload_activity is called.
    """
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))

    class DummyMiFitnessClient:
        def __init__(self, state, **kwargs):
            pass
        def get_activity_detail(self, activity_id):
            return sample_activity_detail

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyMiFitnessClient)
    monkeypatch.setattr(
        cli,
        "render_export",
        lambda detail, file_format, compress=False: type(
            "Export", (), {"payload": b"fitdata", "file_format": "fit", "compressed": False},
        )(),
    )

    uploaded = []

    class DummyStravaClient:
        def __init__(self, state, token_path=None):
            pass
        def list_activities(self, *, after, before, per_page=30, page=1):
            return strava_activities
        def upload_activity(self, payload, sport_type=None, external_id=None):
            uploaded.append(True)
            return {"activity_id": 99999}

    monkeypatch.setattr(strava_client_mod, "StravaClient", DummyStravaClient)

    output_path = tmp_path / "activity.fit"
    return token_path, output_path, uploaded


def test_upload_duplicate_found_user_cancels(monkeypatch, capsys, tmp_path, auth_state, sample_activity_detail):
    strava_activities = [
        {"name": "Evening Run", "start_date_local": "2026-06-01T00:05:00", "sport_type": "Run"},
    ]
    token_path, output_path, uploaded = _setup_upload_mocks(
        monkeypatch, tmp_path, auth_state, sample_activity_detail,
        strava_activities=strava_activities,
    )
    monkeypatch.setattr("builtins.input", lambda prompt: "n")

    exit_code = cli.main([
        "upload-to-strava", "sid:key:1",
        "--strava-token-path", str(token_path),
        "--output", str(output_path),
    ])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Potential duplicate(s) found" in captured.out
    assert "Evening Run" in captured.out
    assert "Upload cancelled" in captured.out
    assert not uploaded


def test_upload_duplicate_found_user_confirms(monkeypatch, capsys, tmp_path, auth_state, sample_activity_detail):
    strava_activities = [
        {"name": "Evening Run", "start_date_local": "2026-06-01T00:05:00", "sport_type": "Run"},
    ]
    token_path, output_path, uploaded = _setup_upload_mocks(
        monkeypatch, tmp_path, auth_state, sample_activity_detail,
        strava_activities=strava_activities,
    )
    monkeypatch.setattr("builtins.input", lambda prompt: "y")

    exit_code = cli.main([
        "upload-to-strava", "sid:key:1",
        "--strava-token-path", str(token_path),
        "--output", str(output_path),
    ])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Potential duplicate(s) found" in captured.out
    assert "Uploaded to Strava successfully" in captured.out
    assert uploaded


def test_upload_skip_duplicate_check_flag(monkeypatch, capsys, tmp_path, auth_state, sample_activity_detail):
    strava_activities = [
        {"name": "Evening Run", "start_date_local": "2026-06-01T00:05:00", "sport_type": "Run"},
    ]
    token_path, output_path, uploaded = _setup_upload_mocks(
        monkeypatch, tmp_path, auth_state, sample_activity_detail,
        strava_activities=strava_activities,
    )

    exit_code = cli.main([
        "upload-to-strava", "sid:key:1",
        "--strava-token-path", str(token_path),
        "--output", str(output_path),
        "--skip-duplicate-check",
    ])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Potential duplicate" not in captured.out
    assert "Uploaded to Strava successfully" in captured.out
    assert uploaded


def test_upload_no_duplicates_proceeds_silently(monkeypatch, capsys, tmp_path, auth_state, sample_activity_detail):
    token_path, output_path, uploaded = _setup_upload_mocks(
        monkeypatch, tmp_path, auth_state, sample_activity_detail,
        strava_activities=[],
    )

    exit_code = cli.main([
        "upload-to-strava", "sid:key:1",
        "--strava-token-path", str(token_path),
        "--output", str(output_path),
    ])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Potential duplicate" not in captured.out
    assert "Uploaded to Strava successfully" in captured.out
    assert uploaded


# ---------------------------------------------------------------------------
# strava-logout tests
# ---------------------------------------------------------------------------


def test_strava_logout_revokes_and_deletes(monkeypatch, capsys, tmp_path):
    import mi_fitness_sync.strava.auth as strava_auth
    from mi_fitness_sync.strava.store import save_tokens

    state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(state, str(token_path))

    revoked = []
    monkeypatch.setattr(strava_auth, "revoke_access_token", lambda token: revoked.append(token))

    exit_code = cli.main(["strava-logout", "--strava-token-path", str(token_path)])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert revoked == ["access-abc"]
    assert "Strava access token revoked" in captured.out
    assert "Removed Strava tokens" in captured.out
    assert not token_path.exists()


def test_strava_logout_no_tokens(capsys, tmp_path):
    token_path = tmp_path / "nonexistent.json"

    exit_code = cli.main(["strava-logout", "--strava-token-path", str(token_path)])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "No Strava tokens found" in captured.out


def test_strava_logout_revoke_fails_still_deletes(monkeypatch, capsys, tmp_path):
    import mi_fitness_sync.strava.auth as strava_auth
    from mi_fitness_sync.exceptions import StravaAuthError
    from mi_fitness_sync.strava.store import save_tokens

    state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(state, str(token_path))

    def failing_revoke(token):
        raise StravaAuthError("Token revocation failed (HTTP 401).")

    monkeypatch.setattr(strava_auth, "revoke_access_token", failing_revoke)

    exit_code = cli.main(["strava-logout", "--strava-token-path", str(token_path)])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Warning: Failed to revoke Strava token" in captured.err
    assert "Removed Strava tokens" in captured.out
    assert not token_path.exists()


def test_upload_duplicate_check_query_window(monkeypatch, capsys, tmp_path, auth_state, sample_activity_detail):
    """Assert that the upload duplicate check queries ±5 minutes around start_time."""
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))

    class DummyMiFitnessClient:
        def __init__(self, state, **kwargs):
            pass
        def get_activity_detail(self, activity_id):
            return sample_activity_detail

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyMiFitnessClient)
    monkeypatch.setattr(
        cli,
        "render_export",
        lambda detail, file_format, compress=False: type(
            "Export", (), {"payload": b"fitdata", "file_format": "fit", "compressed": False},
        )(),
    )

    captured_params: list[dict] = []

    class SpyStravaClient:
        def __init__(self, state, token_path=None):
            pass
        def list_activities(self, *, after, before, per_page=30, page=1):
            captured_params.append({"after": after, "before": before})
            return []
        def upload_activity(self, payload, sport_type=None, external_id=None):
            return {"activity_id": 99999}

    monkeypatch.setattr(strava_client_mod, "StravaClient", SpyStravaClient)

    output_path = tmp_path / "activity.fit"
    exit_code = cli.main([
        "upload-to-strava", "sid:key:1",
        "--strava-token-path", str(token_path),
        "--output", str(output_path),
    ])

    assert exit_code == 0
    assert len(captured_params) == 1
    expected_start = sample_activity_detail.start_time
    assert captured_params[0]["after"] == expected_start - 5 * 60
    assert captured_params[0]["before"] == expected_start + 5 * 60


# ---------------------------------------------------------------------------
# list-activities --strava tests
# ---------------------------------------------------------------------------

_SAMPLE_ACTIVITY = Activity(
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


def _dummy_mi_client(auth_state, activities):
    class DummyClient:
        def __init__(self, state, **kwargs):
            assert state == auth_state

        def list_activities(self, *, start_time, end_time, limit, category=None):
            return activities

    return DummyClient


def test_list_activities_strava_column_matched(monkeypatch, capsys, tmp_path, auth_state):
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)
    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", _dummy_mi_client(auth_state, [_SAMPLE_ACTIVITY]))

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))

    class DummyStravaClient:
        def __init__(self, state, token_path=None):
            pass

        def list_activities(self, *, after, before, per_page=30, page=1):
            return [{"start_date": "2024-06-01T00:00:00Z", "name": "Matched Run"}]

    monkeypatch.setattr(strava_client_mod, "StravaClient", DummyStravaClient)

    exit_code = cli.main([
        "list-activities",
        "--strava",
        "--strava-token-path", str(token_path),
    ])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Strava" in captured.out
    assert "\u2713" in captured.out


def test_list_activities_strava_column_not_matched(monkeypatch, capsys, tmp_path, auth_state):
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)
    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", _dummy_mi_client(auth_state, [_SAMPLE_ACTIVITY]))

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))

    class DummyStravaClient:
        def __init__(self, state, token_path=None):
            pass

        def list_activities(self, *, after, before, per_page=30, page=1):
            return [{"start_date": "2024-07-01T12:00:00Z", "name": "Unrelated Run"}]

    monkeypatch.setattr(strava_client_mod, "StravaClient", DummyStravaClient)

    exit_code = cli.main([
        "list-activities",
        "--strava",
        "--strava-token-path", str(token_path),
    ])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Strava" in captured.out
    assert "\u2717" in captured.out


def test_list_activities_strava_no_tokens_warns(monkeypatch, capsys, tmp_path, auth_state):
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)
    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", _dummy_mi_client(auth_state, [_SAMPLE_ACTIVITY]))

    token_path = tmp_path / "nonexistent.json"

    exit_code = cli.main([
        "list-activities",
        "--strava",
        "--strava-token-path", str(token_path),
    ])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Warning" in captured.err
    assert "Strava" not in captured.out


def test_list_activities_strava_api_error_warns(monkeypatch, capsys, tmp_path, auth_state):
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)
    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", _dummy_mi_client(auth_state, [_SAMPLE_ACTIVITY]))

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))

    class FailingStravaClient:
        def __init__(self, state, token_path=None):
            pass

        def list_activities(self, *, after, before, per_page=30, page=1):
            from mi_fitness_sync.exceptions import StravaError
            raise StravaError("Token expired")

    monkeypatch.setattr(strava_client_mod, "StravaClient", FailingStravaClient)

    exit_code = cli.main([
        "list-activities",
        "--strava",
        "--strava-token-path", str(token_path),
    ])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Warning" in captured.err
    assert "Strava" not in captured.out


def test_list_activities_strava_json_includes_in_strava(monkeypatch, capsys, tmp_path, auth_state):
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)
    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", _dummy_mi_client(auth_state, [_SAMPLE_ACTIVITY]))

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))

    class DummyStravaClient:
        def __init__(self, state, token_path=None):
            pass

        def list_activities(self, *, after, before, per_page=30, page=1):
            return [{"start_date": "2024-06-01T00:00:00Z", "name": "Matched Run"}]

    monkeypatch.setattr(strava_client_mod, "StravaClient", DummyStravaClient)

    exit_code = cli.main([
        "list-activities",
        "--strava",
        "--strava-token-path", str(token_path),
        "--json",
    ])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output[0]["in_strava"] is True


def test_list_activities_without_strava_flag_no_column(monkeypatch, capsys, auth_state):
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)
    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", _dummy_mi_client(auth_state, [_SAMPLE_ACTIVITY]))

    exit_code = cli.main(["list-activities"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Strava" not in captured.out


def test_fetch_strava_status_computes_correct_range(monkeypatch, tmp_path, auth_state):
    """Assert that _fetch_strava_status passes after = min(start_time) - 1 and before = max(start_time) + 1."""
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    activity_a = Activity(
        activity_id="a:b:1", sid="a", key="b", category="outdoor_run", sport_type=1,
        title="First", start_time=1000000, end_time=1003600, duration_seconds=3600,
        distance_meters=5000, calories=300, steps=6000, sync_state="server",
        next_key=None, raw_record={}, raw_report={},
    )
    activity_b = Activity(
        activity_id="a:b:2", sid="a", key="b", category="outdoor_run", sport_type=1,
        title="Second", start_time=2000000, end_time=2003600, duration_seconds=3600,
        distance_meters=5000, calories=300, steps=6000, sync_state="server",
        next_key=None, raw_record={}, raw_report={},
    )

    captured_params: list[dict] = []

    class SpyStravaClient:
        def __init__(self, state, token_path=None):
            pass

        def list_activities(self, *, after, before, per_page=30, page=1):
            captured_params.append({"after": after, "before": before, "per_page": per_page, "page": page})
            return []

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))
    monkeypatch.setattr(strava_client_mod, "StravaClient", SpyStravaClient)

    result = cli._fetch_strava_status([activity_a, activity_b], str(token_path))

    assert captured_params[0]["after"] == 1000000 - 1
    assert captured_params[0]["before"] == 2000000 + 1


def test_fetch_strava_status_offset_1800s_no_match(monkeypatch, tmp_path):
    """A Strava activity 1800 s away from a Mi Fitness activity should NOT match under exact-match policy."""
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    mi_start = 1717200000
    strava_dt_1800_after = "2024-06-01T00:30:00Z"  # mi_start + 1800

    activity = Activity(
        activity_id="s:k:1", sid="s", key="k", category="outdoor_run", sport_type=1,
        title="Run", start_time=mi_start, end_time=mi_start + 3600, duration_seconds=3600,
        distance_meters=10000, calories=700, steps=12000, sync_state="server",
        next_key=None, raw_record={}, raw_report={},
    )

    class DummyStravaClient:
        def __init__(self, state, token_path=None):
            pass

        def list_activities(self, *, after, before, per_page=30, page=1):
            return [{"start_date": strava_dt_1800_after}]

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))
    monkeypatch.setattr(strava_client_mod, "StravaClient", DummyStravaClient)

    result = cli._fetch_strava_status([activity], str(token_path))

    assert result["s:k:1"] is False


def test_fetch_strava_status_offset_1801s_no_match(monkeypatch, tmp_path):
    """A Strava activity 1801 s away should NOT match under exact-match policy."""
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    mi_start = 1717200000
    strava_dt_1801_after = "2024-06-01T00:30:01Z"  # mi_start + 1801

    activity = Activity(
        activity_id="s:k:1", sid="s", key="k", category="outdoor_run", sport_type=1,
        title="Run", start_time=mi_start, end_time=mi_start + 3600, duration_seconds=3600,
        distance_meters=10000, calories=700, steps=12000, sync_state="server",
        next_key=None, raw_record={}, raw_report={},
    )

    class DummyStravaClient:
        def __init__(self, state, token_path=None):
            pass

        def list_activities(self, *, after, before, per_page=30, page=1):
            return [{"start_date": strava_dt_1801_after}]

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))
    monkeypatch.setattr(strava_client_mod, "StravaClient", DummyStravaClient)

    result = cli._fetch_strava_status([activity], str(token_path))

    assert result["s:k:1"] is False


def test_fetch_strava_status_paginates(monkeypatch, tmp_path):
    """When the first Strava page is full (200 results), a second page is requested."""
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    mi_start = 1717200000

    activity = Activity(
        activity_id="s:k:1", sid="s", key="k", category="outdoor_run", sport_type=1,
        title="Run", start_time=mi_start, end_time=mi_start + 3600, duration_seconds=3600,
        distance_meters=10000, calories=700, steps=12000, sync_state="server",
        next_key=None, raw_record={}, raw_report={},
    )

    page_1 = [{"start_date": "2024-07-01T00:00:00Z"}] * 200  # full page, no match
    page_2 = [{"start_date": "2024-06-01T00:00:00Z"}]          # partial page, exact match

    pages_requested: list[int] = []

    class PaginatingStravaClient:
        def __init__(self, state, token_path=None):
            pass

        def list_activities(self, *, after, before, per_page=30, page=1):
            pages_requested.append(page)
            if page == 1:
                return page_1
            return page_2

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))
    monkeypatch.setattr(strava_client_mod, "StravaClient", PaginatingStravaClient)

    result = cli._fetch_strava_status([activity], str(token_path))

    assert pages_requested == [1, 2]
    assert result["s:k:1"] is True
