from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Never

import pytest

from mi_fitness_sync.activity.models import Activity, ActivityDetail
from mi_fitness_sync.auth.client import LoginSession
from mi_fitness_sync.cli import app as cli
from mi_fitness_sync.exceptions import (
    CaptchaRequiredError,
    MiFitnessError,
    NotificationRequiredError,
    Step2RequiredError,
    VerificationCodeRejectedError,
    XiaomiApiError,
)
from mi_fitness_sync.export.bulk import (
    BulkExportOptions,
    BulkExportRecord,
    BulkExportSummary,
    ProgressCallback,
)
from mi_fitness_sync.strava.store import StravaTokenState

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from mi_fitness_sync.auth.client import MetaLoginData
    from mi_fitness_sync.auth.state import AuthState


def _make_login_session(*, email: str, device_id: str) -> LoginSession:
    """Build the login session the CLI doubles hand back to the command handlers."""
    return LoginSession(
        email=email,
        user_id="uid",
        c_user_id="cuid",
        service_id="miothealth",
        pass_token="pt",
        service_token="st",
        ssecurity="ss",
        psecurity=None,
        auto_login_url="https://example.com",
        device_id=device_id,
        slh=None,
        ph=None,
        sts_cookie_header="cookie",
        cookies=[],
    )


def _patch_captcha_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    captcha_dir = tmp_path / "captcha"
    captcha_dir.mkdir()
    monkeypatch.setattr(cli, "get_captcha_dir", lambda: captcha_dir)
    return captcha_dir


def _patch_captcha_opener(
    monkeypatch: pytest.MonkeyPatch, *, result: bool, expected_dir: Path | None = None
) -> list[Path]:
    opened_paths: list[Path] = []

    def open_captcha_image(path: Path) -> bool:
        if expected_dir is not None:
            assert path.parent == expected_dir
        opened_paths.append(path)
        return result

    monkeypatch.setattr(cli, "_open_captcha_image", open_captcha_image)
    return opened_paths


def test_format_error_includes_xiaomi_api_code() -> None:
    error = XiaomiApiError("boom", code=401)

    assert cli.format_error(error) == "boom (code=401)"


def test_format_error_for_captcha() -> None:
    error = CaptchaRequiredError("https://example.com/captcha")

    assert (
        cli.format_error(error)
        == "Login requires a captcha challenge. URL: https://example.com/captcha"
    )


def test_login_prompts_for_email_and_password(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    inputs = iter(["user@example.com"])
    monkeypatch.setattr("builtins.input", lambda prompt: next(inputs))
    monkeypatch.setattr("getpass.getpass", lambda prompt="Password: ": "s3cret")

    login_args: dict[str, str] = {}

    class DummyAuthClient:
        generate_device_id = staticmethod(lambda: "DEV123")

        def __init__(self, service_id: str) -> None:
            pass

        def login_with_password(self, *, email: str, password: str, device_id: str) -> LoginSession:
            login_args.update(email=email, password=password, device_id=device_id)

            return _make_login_session(
                email=email,
                device_id=device_id,
            )

    monkeypatch.setattr(cli, "MiFitnessAuthClient", DummyAuthClient)
    monkeypatch.setattr(cli, "load_state", lambda path: None)

    state_path = str(tmp_path / "state.json")
    exit_code = cli.main(["login", "--state-path", state_path])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Login succeeded" in captured.out
    assert login_args["email"] == "user@example.com"
    assert login_args["password"] == "s3cret"


def test_login_uses_cli_args_without_prompt(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    prompt_called: list[bool] = []

    def record_prompt(prompt: str = "Password: ") -> str:
        prompt_called.append(True)
        return ""

    monkeypatch.setattr("builtins.input", record_prompt)
    monkeypatch.setattr("getpass.getpass", record_prompt)

    login_args: dict[str, str] = {}

    class DummyAuthClient:
        generate_device_id = staticmethod(lambda: "DEV123")

        def __init__(self, service_id: str) -> None:
            pass

        def login_with_password(self, *, email: str, password: str, device_id: str) -> LoginSession:
            login_args.update(email=email, password=password)

            return _make_login_session(
                email=email,
                device_id=device_id,
            )

    monkeypatch.setattr(cli, "MiFitnessAuthClient", DummyAuthClient)
    monkeypatch.setattr(cli, "load_state", lambda path: None)

    state_path = str(tmp_path / "state.json")
    exit_code = cli.main(
        ["login", "--email", "a@b.com", "--password", "pass", "--state-path", state_path]
    )

    assert exit_code == 0
    assert login_args["email"] == "a@b.com"
    assert login_args["password"] == "pass"
    assert not prompt_called


def test_login_email_on_cli_password_prompted(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    input_called: list[bool] = []

    def record_input(prompt: str) -> str:
        input_called.append(True)
        return ""

    monkeypatch.setattr("builtins.input", record_input)
    monkeypatch.setattr("getpass.getpass", lambda prompt="Password: ": "prompted-pass")

    login_args: dict[str, str] = {}

    class DummyAuthClient:
        generate_device_id = staticmethod(lambda: "DEV123")

        def __init__(self, service_id: str) -> None:
            pass

        def login_with_password(self, *, email: str, password: str, device_id: str) -> LoginSession:
            login_args.update(email=email, password=password)

            return _make_login_session(
                email=email,
                device_id=device_id,
            )

    monkeypatch.setattr(cli, "MiFitnessAuthClient", DummyAuthClient)
    monkeypatch.setattr(cli, "load_state", lambda path: None)

    state_path = str(tmp_path / "state.json")
    exit_code = cli.main(["login", "--email", "a@b.com", "--state-path", state_path])

    assert exit_code == 0
    assert login_args["email"] == "a@b.com"
    assert login_args["password"] == "prompted-pass"
    assert not input_called  # input() should NOT have been called


def test_login_password_on_cli_email_prompted(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    monkeypatch.setattr("builtins.input", lambda prompt: "prompted@example.com")
    getpass_called: list[bool] = []

    def record_getpass(prompt: str = "Password: ") -> str:
        getpass_called.append(True)
        return ""

    monkeypatch.setattr("getpass.getpass", record_getpass)

    login_args: dict[str, str] = {}

    class DummyAuthClient:
        generate_device_id = staticmethod(lambda: "DEV123")

        def __init__(self, service_id: str) -> None:
            pass

        def login_with_password(self, *, email: str, password: str, device_id: str) -> LoginSession:
            login_args.update(email=email, password=password)

            return _make_login_session(
                email=email,
                device_id=device_id,
            )

    monkeypatch.setattr(cli, "MiFitnessAuthClient", DummyAuthClient)
    monkeypatch.setattr(cli, "load_state", lambda path: None)

    state_path = str(tmp_path / "state.json")
    exit_code = cli.main(["login", "--password", "cli-pass", "--state-path", state_path])

    assert exit_code == 0
    assert login_args["email"] == "prompted@example.com"
    assert login_args["password"] == "cli-pass"
    assert not getpass_called  # getpass() should NOT have been called


def test_login_whitespace_only_email_prompt(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("builtins.input", lambda prompt: "   ")
    monkeypatch.setattr("getpass.getpass", lambda prompt="Password: ": "s3cret")

    exit_code = cli.main(["login"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Email and password are required" in captured.err


def test_login_missing_credentials(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("builtins.input", lambda prompt: "")
    monkeypatch.setattr("getpass.getpass", lambda prompt="Password: ": "")

    exit_code = cli.main(["login"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Email and password are required" in captured.err


def test_auth_status_json_output(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], auth_state: AuthState
) -> None:
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    exit_code = cli.main(["auth-status", "--json"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert json.loads(output)["email"] == auth_state.email


def test_main_returns_error_for_invalid_limit(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], auth_state: AuthState
) -> None:
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    exit_code = cli.main(["list-activities", "--limit", "0"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "--limit must be greater than zero." in captured.err


def test_main_returns_error_for_invalid_country_override(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], auth_state: AuthState
) -> None:
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    exit_code = cli.main(["list-activities", "--country-code", "ZZ"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Unsupported Mi Fitness country override: ZZ." in captured.err


def test_list_activities_json_output(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], auth_state: AuthState
) -> None:
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
        def __init__(self, state: AuthState, **kwargs: object) -> None:
            assert state == auth_state
            self.country_code = kwargs.get("country_code")

        def list_activities(
            self,
            *,
            start_time: int | None,
            end_time: int | None,
            limit: int | None,
            category: str | None = None,
        ) -> list[Activity]:
            assert start_time == 1704067200
            assert end_time is None
            assert limit == 1
            assert category is None
            return [sample_activity]

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyClient)

    exit_code = cli.main(
        [
            "list-activities",
            "--since",
            "2024-01-01T00:00:00Z",
            "--limit",
            "1",
            "--country-code",
            "ID",
            "--json",
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output[0]["title"] == "Morning Run"


def test_activity_detail_json_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    auth_state: AuthState,
    sample_activity_detail: ActivityDetail,
) -> None:
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    class DummyClient:
        def __init__(self, state: AuthState, **kwargs: object) -> None:
            assert state == auth_state

        def get_activity_detail(self, activity_id: str) -> ActivityDetail:
            assert activity_id == "sid:key:1"
            return sample_activity_detail

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyClient)

    exit_code = cli.main(["activity-detail", "sid:key:1", "--json"])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["activity"]["title"] == "Morning Run"
    assert output["track_points"][0]["heart_rate"] == 120


def test_export_activity_writes_requested_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    auth_state: AuthState,
    sample_activity_detail: ActivityDetail,
) -> None:
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    class DummyClient:
        def __init__(self, state: AuthState, **kwargs: object) -> None:
            assert state == auth_state

        def get_activity_detail(
            self,
            activity_id: str,
            *,
            include_sport_report: bool = True,
            include_recovery_rate: bool = True,
        ) -> ActivityDetail:
            assert activity_id == "sid:key:1"
            assert include_sport_report is False
            assert include_recovery_rate is False
            return sample_activity_detail

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyClient)
    monkeypatch.setattr(
        cli,
        "render_export",
        lambda detail, file_format, compress=False, **kwargs: type(
            "Export",
            (),
            {"file_format": file_format, "compressed": compress, "payload": b"payload"},
        )(),
    )

    output_path = tmp_path / "exports" / "run.gpx.gz"
    exit_code = cli.main(
        ["export-activity", "sid:key:1", "--format", "gpx", "--gzip", "--output", str(output_path)]
    )
    captured = capsys.readouterr().out

    assert exit_code == 0
    assert output_path.read_bytes() == b"payload"
    assert "Compressed: yes" in captured


def test_export_activity_uses_sanitized_title_and_local_start_time(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    auth_state: AuthState,
    sample_activity_detail: ActivityDetail,
) -> None:
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    class DummyClient:
        def __init__(self, state: AuthState, **kwargs: object) -> None:
            assert state == auth_state

        def get_activity_detail(
            self,
            activity_id: str,
            *,
            include_sport_report: bool = True,
            include_recovery_rate: bool = True,
        ) -> ActivityDetail:
            assert activity_id == "sid:key:1"
            assert include_sport_report is False
            assert include_recovery_rate is False
            return sample_activity_detail

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyClient)
    monkeypatch.setattr(cli, "get_exports_dir", lambda: tmp_path / "exports")
    monkeypatch.setattr(
        cli,
        "render_export",
        lambda detail, file_format, compress=False, **kwargs: type(
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


def test_export_activities_command_invokes_bulk_exporter(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    auth_state: AuthState,
) -> None:
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
        raw_record={"sid": "sid", "key": "key", "time": 1717200000},
        raw_report={"name": "Morning Run"},
    )
    captured: dict[str, object] = {}

    class DummyClient:
        def __init__(self, state: AuthState, **kwargs: object) -> None:
            captured["state"] = state
            captured["kwargs"] = kwargs

        def iter_activities(
            self,
            *,
            start_time: int | None,
            end_time: int | None,
            limit: int | None,
            category: str | None = None,
        ) -> Iterator[Activity]:
            assert start_time == 1704067200
            assert end_time is None
            assert limit == 1
            assert category is None
            return iter([sample_activity])

    class DummyExporter:
        def __init__(self, state: AuthState, options: BulkExportOptions) -> None:
            captured["exporter_state"] = state
            captured["options"] = options

        def run(
            self,
            activities: Iterable[Activity],
            *,
            progress_callback: ProgressCallback | None = None,
        ) -> BulkExportSummary:
            items = list(activities)
            captured["activity_ids"] = [activity.activity_id for activity in items]
            if progress_callback is not None:
                progress_callback(
                    BulkExportRecord(
                        activity_id=items[0].activity_id,
                        status="completed",
                        output_path=str(tmp_path / "exports" / "done.gpx"),
                        bytes_written=7,
                        error=None,
                        recorded_at="2026-05-11T00:00:00+00:00",
                    )
                )
            options = captured["options"]
            assert isinstance(options, BulkExportOptions)
            return BulkExportSummary(
                total_seen=1,
                exported=1,
                output_dir=options.output_dir,
                manifest_path=options.manifest_path,
            )

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyClient)
    monkeypatch.setattr(cli, "BulkExporter", DummyExporter)

    exit_code = cli.main(
        [
            "export-activities",
            "--format",
            "gpx",
            "--output-dir",
            str(tmp_path / "exports"),
            "--since",
            "2024-01-01T00:00:00Z",
            "--limit",
            "1",
        ]
    )
    captured_output = capsys.readouterr().out

    assert exit_code == 0
    assert captured["activity_ids"] == ["sid:key:1"]
    assert "[exported] sid:key:1" in captured_output
    assert "Bulk export complete." in captured_output


def test_export_activities_invalid_workers(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], auth_state: AuthState
) -> None:
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    exit_code = cli.main(["export-activities", "--format", "gpx", "--workers", "0"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "--workers must be greater than zero." in captured.err


def test_activity_detail_no_cache_flag(
    monkeypatch: pytest.MonkeyPatch, auth_state: AuthState, sample_activity_detail: ActivityDetail
) -> None:
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)
    captured_kwargs = {}

    class DummyClient:
        def __init__(self, state: AuthState, **kwargs: object) -> None:
            captured_kwargs.update(kwargs)

        def get_activity_detail(self, activity_id: str) -> ActivityDetail:
            return sample_activity_detail

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyClient)

    exit_code = cli.main(["activity-detail", "sid:key:1", "--no-cache", "--json"])
    assert exit_code == 0
    assert captured_kwargs["no_cache"] is True


def test_activity_detail_cache_dir_flag(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    auth_state: AuthState,
    sample_activity_detail: ActivityDetail,
) -> None:
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)
    captured_kwargs = {}

    class DummyClient:
        def __init__(self, state: AuthState, **kwargs: object) -> None:
            captured_kwargs.update(kwargs)

        def get_activity_detail(self, activity_id: str) -> ActivityDetail:
            return sample_activity_detail

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyClient)

    cache_path = str(tmp_path / "custom_cache")
    exit_code = cli.main(["activity-detail", "sid:key:1", "--cache-dir", cache_path, "--json"])
    assert exit_code == 0
    assert captured_kwargs["cache_dir"] == cache_path


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


def test_strava_login_success(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    import mi_fitness_sync.strava.auth as strava_auth

    monkeypatch.setattr(
        strava_auth,
        "run_oauth_flow",
        lambda cid, csecret, port=5478: {
            "access_token": "at",
            "refresh_token": "rt",
            "expires_at": 9999,
            "athlete": {"id": 42},
        },
    )

    token_path = tmp_path / "tokens.json"
    exit_code = cli.main(
        [
            "strava-login",
            "--client-id",
            "123",
            "--client-secret",
            "secret",
            "--strava-token-path",
            str(token_path),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Strava login succeeded" in captured.out
    assert "Athlete ID: 42" in captured.out
    assert token_path.exists()


def test_strava_login_missing_credentials(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("builtins.input", lambda prompt: "")

    exit_code = cli.main(["strava-login"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "client_id and client_secret are required" in captured.err


def test_strava_login_oauth_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import mi_fitness_sync.strava.auth as strava_auth
    from mi_fitness_sync.exceptions import StravaAuthError

    def failing_flow(*args: object, **kwargs: object) -> Never:
        raise StravaAuthError("OAuth callback timed out after 120 seconds.")

    monkeypatch.setattr(strava_auth, "run_oauth_flow", failing_flow)

    exit_code = cli.main(
        [
            "strava-login",
            "--client-id",
            "123",
            "--client-secret",
            "secret",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "timed out" in captured.err


def test_strava_status_success(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    from mi_fitness_sync.strava.store import save_tokens

    state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(state, str(token_path))

    exit_code = cli.main(["strava-status", "--strava-token-path", str(token_path)])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Strava auth state found" in captured.out
    assert "Athlete ID: 42" in captured.out


def test_strava_status_no_tokens(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    token_path = tmp_path / "nonexistent.json"

    exit_code = cli.main(["strava-status", "--strava-token-path", str(token_path)])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "No Strava token state found" in captured.err


def test_upload_to_strava_success(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    auth_state: AuthState,
    sample_activity_detail: ActivityDetail,
) -> None:
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    # Write a real Strava token file
    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))

    class DummyClient:
        def __init__(self, state: AuthState, **kwargs: object) -> None:
            pass

        def get_activity_detail(
            self,
            activity_id: str,
            *,
            include_sport_report: bool = True,
            include_recovery_rate: bool = True,
        ) -> ActivityDetail:
            assert activity_id == "sid:key:1"
            assert include_sport_report is True
            assert include_recovery_rate is False
            return sample_activity_detail

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyClient)
    monkeypatch.setattr(
        cli,
        "render_export",
        lambda detail, file_format, compress=False, **kwargs: type(
            "Export",
            (),
            {"payload": b"fitdata", "file_format": "fit", "compressed": False},
        )(),
    )

    class DummyStravaClient:
        def __init__(self, state: StravaTokenState, token_path: str | None = None) -> None:
            pass

        def list_activities(
            self, *, after: int, before: int, per_page: int = 30, page: int = 1
        ) -> list[dict[str, Any]]:
            return []

        def upload_activity(
            self, payload: bytes, sport_type: str | None = None, external_id: str | None = None
        ) -> dict[str, Any]:
            assert payload == b"fitdata"
            return {"activity_id": 12345}

    monkeypatch.setattr(strava_client_mod, "StravaClient", DummyStravaClient)

    output_fit = tmp_path / "activity.fit"
    exit_code = cli.main(
        [
            "upload-to-strava",
            "sid:key:1",
            "--strava-token-path",
            str(token_path),
            "--output",
            str(output_fit),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert output_fit.read_bytes() == b"fitdata"
    assert "Uploaded to Strava successfully" in captured.out
    assert "https://www.strava.com/activities/12345" in captured.out


def test_upload_to_strava_no_tokens(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    auth_state: AuthState,
) -> None:
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    token_path = tmp_path / "nonexistent.json"
    exit_code = cli.main(
        [
            "upload-to-strava",
            "sid:key:1",
            "--strava-token-path",
            str(token_path),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "No Strava token state found" in captured.err


def _setup_upload_mocks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    auth_state: AuthState,
    sample_activity_detail: ActivityDetail,
    *,
    strava_activities: list[dict[str, Any]],
) -> tuple[Path, Path, list[bool]]:
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
        def __init__(self, state: AuthState, **kwargs: object) -> None:
            pass

        def get_activity_detail(
            self,
            activity_id: str,
            *,
            include_sport_report: bool = True,
            include_recovery_rate: bool = True,
        ) -> ActivityDetail:
            assert include_sport_report is True
            assert include_recovery_rate is False
            return sample_activity_detail

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyMiFitnessClient)
    monkeypatch.setattr(
        cli,
        "render_export",
        lambda detail, file_format, compress=False, **kwargs: type(
            "Export",
            (),
            {"payload": b"fitdata", "file_format": "fit", "compressed": False},
        )(),
    )

    uploaded = []

    class DummyStravaClient:
        def __init__(self, state: StravaTokenState, token_path: str | None = None) -> None:
            pass

        def list_activities(
            self, *, after: int, before: int, per_page: int = 30, page: int = 1
        ) -> list[dict[str, Any]]:
            return strava_activities

        def upload_activity(
            self, payload: bytes, sport_type: str | None = None, external_id: str | None = None
        ) -> dict[str, Any]:
            uploaded.append(True)
            return {"activity_id": 99999}

    monkeypatch.setattr(strava_client_mod, "StravaClient", DummyStravaClient)

    output_path = tmp_path / "activity.fit"
    return token_path, output_path, uploaded


def test_upload_duplicate_found_user_cancels(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    auth_state: AuthState,
    sample_activity_detail: ActivityDetail,
) -> None:
    strava_activities = [
        {"name": "Evening Run", "start_date_local": "2026-06-01T00:05:00", "sport_type": "Run"},
    ]
    token_path, output_path, uploaded = _setup_upload_mocks(
        monkeypatch,
        tmp_path,
        auth_state,
        sample_activity_detail,
        strava_activities=strava_activities,
    )
    monkeypatch.setattr("builtins.input", lambda prompt: "n")

    exit_code = cli.main(
        [
            "upload-to-strava",
            "sid:key:1",
            "--strava-token-path",
            str(token_path),
            "--output",
            str(output_path),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Potential duplicate(s) found" in captured.out
    assert "Evening Run" in captured.out
    assert "Upload cancelled" in captured.out
    assert not uploaded


def test_upload_duplicate_found_user_confirms(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    auth_state: AuthState,
    sample_activity_detail: ActivityDetail,
) -> None:
    strava_activities = [
        {"name": "Evening Run", "start_date_local": "2026-06-01T00:05:00", "sport_type": "Run"},
    ]
    token_path, output_path, uploaded = _setup_upload_mocks(
        monkeypatch,
        tmp_path,
        auth_state,
        sample_activity_detail,
        strava_activities=strava_activities,
    )
    monkeypatch.setattr("builtins.input", lambda prompt: "y")

    exit_code = cli.main(
        [
            "upload-to-strava",
            "sid:key:1",
            "--strava-token-path",
            str(token_path),
            "--output",
            str(output_path),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Potential duplicate(s) found" in captured.out
    assert "Uploaded to Strava successfully" in captured.out
    assert uploaded


def test_upload_skip_duplicate_check_flag(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    auth_state: AuthState,
    sample_activity_detail: ActivityDetail,
) -> None:
    strava_activities = [
        {"name": "Evening Run", "start_date_local": "2026-06-01T00:05:00", "sport_type": "Run"},
    ]
    token_path, output_path, uploaded = _setup_upload_mocks(
        monkeypatch,
        tmp_path,
        auth_state,
        sample_activity_detail,
        strava_activities=strava_activities,
    )

    exit_code = cli.main(
        [
            "upload-to-strava",
            "sid:key:1",
            "--strava-token-path",
            str(token_path),
            "--output",
            str(output_path),
            "--skip-duplicate-check",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Potential duplicate" not in captured.out
    assert "Uploaded to Strava successfully" in captured.out
    assert uploaded


def test_upload_no_duplicates_proceeds_silently(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    auth_state: AuthState,
    sample_activity_detail: ActivityDetail,
) -> None:
    token_path, output_path, uploaded = _setup_upload_mocks(
        monkeypatch,
        tmp_path,
        auth_state,
        sample_activity_detail,
        strava_activities=[],
    )

    exit_code = cli.main(
        [
            "upload-to-strava",
            "sid:key:1",
            "--strava-token-path",
            str(token_path),
            "--output",
            str(output_path),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Potential duplicate" not in captured.out
    assert "Uploaded to Strava successfully" in captured.out
    assert uploaded


def test_strava_logout_revokes_and_deletes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    import mi_fitness_sync.strava.auth as strava_auth
    from mi_fitness_sync.strava.store import save_tokens

    state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(state, str(token_path))

    revoked: list[str] = []
    monkeypatch.setattr(strava_auth, "revoke_access_token", revoked.append)

    exit_code = cli.main(["strava-logout", "--strava-token-path", str(token_path)])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert revoked == ["access-abc"]
    assert "Strava access token revoked" in captured.out
    assert "Removed Strava tokens" in captured.out
    assert not token_path.exists()


def test_strava_logout_no_tokens(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    token_path = tmp_path / "nonexistent.json"

    exit_code = cli.main(["strava-logout", "--strava-token-path", str(token_path)])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "No Strava tokens found" in captured.out


def test_strava_logout_revoke_fails_still_deletes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    import mi_fitness_sync.strava.auth as strava_auth
    from mi_fitness_sync.exceptions import StravaAuthError
    from mi_fitness_sync.strava.store import save_tokens

    state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(state, str(token_path))

    def failing_revoke(token: str) -> Never:
        raise StravaAuthError("Token revocation failed (HTTP 401).")

    monkeypatch.setattr(strava_auth, "revoke_access_token", failing_revoke)

    exit_code = cli.main(["strava-logout", "--strava-token-path", str(token_path)])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Warning: Failed to revoke Strava token" in captured.err
    assert "Removed Strava tokens" in captured.out
    assert not token_path.exists()


def test_upload_duplicate_check_query_window(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    auth_state: AuthState,
    sample_activity_detail: ActivityDetail,
) -> None:
    """Assert that the upload duplicate check queries 5 minutes either side of start_time."""
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))

    class DummyMiFitnessClient:
        def __init__(self, state: AuthState, **kwargs: object) -> None:
            pass

        def get_activity_detail(
            self,
            activity_id: str,
            *,
            include_sport_report: bool = True,
            include_recovery_rate: bool = True,
        ) -> ActivityDetail:
            assert include_sport_report is True
            assert include_recovery_rate is False
            return sample_activity_detail

    monkeypatch.setattr(cli, "MiFitnessActivitiesClient", DummyMiFitnessClient)
    monkeypatch.setattr(
        cli,
        "render_export",
        lambda detail, file_format, compress=False, **kwargs: type(
            "Export",
            (),
            {"payload": b"fitdata", "file_format": "fit", "compressed": False},
        )(),
    )

    captured_params: list[dict[str, Any]] = []

    class SpyStravaClient:
        def __init__(self, state: StravaTokenState, token_path: str | None = None) -> None:
            pass

        def list_activities(
            self, *, after: int, before: int, per_page: int = 30, page: int = 1
        ) -> list[dict[str, Any]]:
            captured_params.append({"after": after, "before": before})
            return []

        def upload_activity(
            self, payload: bytes, sport_type: str | None = None, external_id: str | None = None
        ) -> dict[str, Any]:
            return {"activity_id": 99999}

    monkeypatch.setattr(strava_client_mod, "StravaClient", SpyStravaClient)

    output_path = tmp_path / "activity.fit"
    exit_code = cli.main(
        [
            "upload-to-strava",
            "sid:key:1",
            "--strava-token-path",
            str(token_path),
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    assert len(captured_params) == 1
    expected_start = sample_activity_detail.start_time
    assert captured_params[0]["after"] == expected_start - 5 * 60
    assert captured_params[0]["before"] == expected_start + 5 * 60


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


def _dummy_mi_client(auth_state: AuthState, activities: list[Activity]) -> type:
    class DummyClient:
        def __init__(self, state: AuthState, **kwargs: object) -> None:
            assert state == auth_state

        def list_activities(
            self,
            *,
            start_time: int | None,
            end_time: int | None,
            limit: int | None,
            category: str | None = None,
        ) -> list[Activity]:
            return activities

    return DummyClient


def test_list_activities_strava_column_matched(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    auth_state: AuthState,
) -> None:
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)
    monkeypatch.setattr(
        cli, "MiFitnessActivitiesClient", _dummy_mi_client(auth_state, [_SAMPLE_ACTIVITY])
    )

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))

    class DummyStravaClient:
        def __init__(self, state: StravaTokenState, token_path: str | None = None) -> None:
            pass

        def list_activities(
            self, *, after: int, before: int, per_page: int = 30, page: int = 1
        ) -> list[dict[str, Any]]:
            return [{"start_date": "2024-06-01T00:00:00Z", "name": "Matched Run"}]

    monkeypatch.setattr(strava_client_mod, "StravaClient", DummyStravaClient)

    exit_code = cli.main(
        [
            "list-activities",
            "--strava",
            "--strava-token-path",
            str(token_path),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Strava" in captured.out
    assert "\u2713" in captured.out


def test_list_activities_strava_column_not_matched(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    auth_state: AuthState,
) -> None:
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)
    monkeypatch.setattr(
        cli, "MiFitnessActivitiesClient", _dummy_mi_client(auth_state, [_SAMPLE_ACTIVITY])
    )

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))

    class DummyStravaClient:
        def __init__(self, state: StravaTokenState, token_path: str | None = None) -> None:
            pass

        def list_activities(
            self, *, after: int, before: int, per_page: int = 30, page: int = 1
        ) -> list[dict[str, Any]]:
            return [{"start_date": "2024-07-01T12:00:00Z", "name": "Unrelated Run"}]

    monkeypatch.setattr(strava_client_mod, "StravaClient", DummyStravaClient)

    exit_code = cli.main(
        [
            "list-activities",
            "--strava",
            "--strava-token-path",
            str(token_path),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Strava" in captured.out
    assert "\u2717" in captured.out


def test_list_activities_strava_no_tokens_warns(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    auth_state: AuthState,
) -> None:
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)
    monkeypatch.setattr(
        cli, "MiFitnessActivitiesClient", _dummy_mi_client(auth_state, [_SAMPLE_ACTIVITY])
    )

    token_path = tmp_path / "nonexistent.json"

    exit_code = cli.main(
        [
            "list-activities",
            "--strava",
            "--strava-token-path",
            str(token_path),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Warning" in captured.err
    assert "Strava" not in captured.out


def test_list_activities_strava_api_error_warns(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    auth_state: AuthState,
) -> None:
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)
    monkeypatch.setattr(
        cli, "MiFitnessActivitiesClient", _dummy_mi_client(auth_state, [_SAMPLE_ACTIVITY])
    )

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))

    class FailingStravaClient:
        def __init__(self, state: StravaTokenState, token_path: str | None = None) -> None:
            pass

        def list_activities(
            self, *, after: int, before: int, per_page: int = 30, page: int = 1
        ) -> Never:
            from mi_fitness_sync.exceptions import StravaError

            raise StravaError("Token expired")

    monkeypatch.setattr(strava_client_mod, "StravaClient", FailingStravaClient)

    exit_code = cli.main(
        [
            "list-activities",
            "--strava",
            "--strava-token-path",
            str(token_path),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Warning" in captured.err
    assert "Strava" not in captured.out


def test_list_activities_strava_json_includes_in_strava(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    auth_state: AuthState,
) -> None:
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)
    monkeypatch.setattr(
        cli, "MiFitnessActivitiesClient", _dummy_mi_client(auth_state, [_SAMPLE_ACTIVITY])
    )

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))

    class DummyStravaClient:
        def __init__(self, state: StravaTokenState, token_path: str | None = None) -> None:
            pass

        def list_activities(
            self, *, after: int, before: int, per_page: int = 30, page: int = 1
        ) -> list[dict[str, Any]]:
            return [{"start_date": "2024-06-01T00:00:00Z", "name": "Matched Run"}]

    monkeypatch.setattr(strava_client_mod, "StravaClient", DummyStravaClient)

    exit_code = cli.main(
        [
            "list-activities",
            "--strava",
            "--strava-token-path",
            str(token_path),
            "--json",
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output[0]["in_strava"] is True


def test_list_activities_without_strava_flag_no_column(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], auth_state: AuthState
) -> None:
    monkeypatch.setattr(cli, "load_state", lambda path: auth_state)
    monkeypatch.setattr(
        cli, "MiFitnessActivitiesClient", _dummy_mi_client(auth_state, [_SAMPLE_ACTIVITY])
    )

    exit_code = cli.main(["list-activities"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Strava" not in captured.out


def test_fetch_strava_status_computes_correct_range(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, auth_state: AuthState
) -> None:
    """Pass after = min(start_time) - 6 and before = max(start_time) + 6."""
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    activity_a = Activity(
        activity_id="a:b:1",
        sid="a",
        key="b",
        category="outdoor_run",
        sport_type=1,
        title="First",
        start_time=1000000,
        end_time=1003600,
        duration_seconds=3600,
        distance_meters=5000,
        calories=300,
        steps=6000,
        sync_state="server",
        next_key=None,
        raw_record={},
        raw_report={},
    )
    activity_b = Activity(
        activity_id="a:b:2",
        sid="a",
        key="b",
        category="outdoor_run",
        sport_type=1,
        title="Second",
        start_time=2000000,
        end_time=2003600,
        duration_seconds=3600,
        distance_meters=5000,
        calories=300,
        steps=6000,
        sync_state="server",
        next_key=None,
        raw_record={},
        raw_report={},
    )

    captured_params: list[dict[str, Any]] = []

    class SpyStravaClient:
        def __init__(self, state: StravaTokenState, token_path: str | None = None) -> None:
            pass

        def list_activities(
            self, *, after: int, before: int, per_page: int = 30, page: int = 1
        ) -> list[dict[str, Any]]:
            captured_params.append(
                {"after": after, "before": before, "per_page": per_page, "page": page}
            )
            return []

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))
    monkeypatch.setattr(strava_client_mod, "StravaClient", SpyStravaClient)

    cli._fetch_strava_status([activity_a, activity_b], str(token_path))

    assert captured_params[0]["after"] == 1000000 - 6
    assert captured_params[0]["before"] == 2000000 + 6


def test_fetch_strava_status_offset_1800s_no_match(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A Strava activity 1800 s away must not match under the exact-match policy."""
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    mi_start = 1717200000
    strava_dt_1800_after = "2024-06-01T00:30:00Z"  # mi_start + 1800

    activity = Activity(
        activity_id="s:k:1",
        sid="s",
        key="k",
        category="outdoor_run",
        sport_type=1,
        title="Run",
        start_time=mi_start,
        end_time=mi_start + 3600,
        duration_seconds=3600,
        distance_meters=10000,
        calories=700,
        steps=12000,
        sync_state="server",
        next_key=None,
        raw_record={},
        raw_report={},
    )

    class DummyStravaClient:
        def __init__(self, state: StravaTokenState, token_path: str | None = None) -> None:
            pass

        def list_activities(
            self, *, after: int, before: int, per_page: int = 30, page: int = 1
        ) -> list[dict[str, Any]]:
            return [{"start_date": strava_dt_1800_after}]

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))
    monkeypatch.setattr(strava_client_mod, "StravaClient", DummyStravaClient)

    result = cli._fetch_strava_status([activity], str(token_path))
    assert result is not None

    assert result["s:k:1"] is False


def test_fetch_strava_status_offset_1801s_no_match(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A Strava activity 1801 s away should NOT match under exact-match policy."""
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    mi_start = 1717200000
    strava_dt_1801_after = "2024-06-01T00:30:01Z"  # mi_start + 1801

    activity = Activity(
        activity_id="s:k:1",
        sid="s",
        key="k",
        category="outdoor_run",
        sport_type=1,
        title="Run",
        start_time=mi_start,
        end_time=mi_start + 3600,
        duration_seconds=3600,
        distance_meters=10000,
        calories=700,
        steps=12000,
        sync_state="server",
        next_key=None,
        raw_record={},
        raw_report={},
    )

    class DummyStravaClient:
        def __init__(self, state: StravaTokenState, token_path: str | None = None) -> None:
            pass

        def list_activities(
            self, *, after: int, before: int, per_page: int = 30, page: int = 1
        ) -> list[dict[str, Any]]:
            return [{"start_date": strava_dt_1801_after}]

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))
    monkeypatch.setattr(strava_client_mod, "StravaClient", DummyStravaClient)

    result = cli._fetch_strava_status([activity], str(token_path))
    assert result is not None

    assert result["s:k:1"] is False


def test_fetch_strava_status_paginates(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """When the first Strava page is full (200 results), a second page is requested."""
    import mi_fitness_sync.strava.client as strava_client_mod
    from mi_fitness_sync.strava.store import save_tokens

    mi_start = 1717200000

    activity = Activity(
        activity_id="s:k:1",
        sid="s",
        key="k",
        category="outdoor_run",
        sport_type=1,
        title="Run",
        start_time=mi_start,
        end_time=mi_start + 3600,
        duration_seconds=3600,
        distance_meters=10000,
        calories=700,
        steps=12000,
        sync_state="server",
        next_key=None,
        raw_record={},
        raw_report={},
    )

    page_1 = [{"start_date": "2024-07-01T00:00:00Z"}] * 200  # full page, no match
    page_2 = [{"start_date": "2024-06-01T00:00:00Z"}]  # partial page, exact match

    pages_requested: list[int] = []

    class PaginatingStravaClient:
        def __init__(self, state: StravaTokenState, token_path: str | None = None) -> None:
            pass

        def list_activities(
            self, *, after: int, before: int, per_page: int = 30, page: int = 1
        ) -> list[dict[str, Any]]:
            pages_requested.append(page)
            if page == 1:
                return page_1
            return page_2

    token_state = _make_strava_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(token_state, str(token_path))
    monkeypatch.setattr(strava_client_mod, "StravaClient", PaginatingStravaClient)

    result = cli._fetch_strava_status([activity], str(token_path))
    assert result is not None

    assert pages_requested == [1, 2]
    assert result["s:k:1"] is True


class TestSmoothingFlagParsing:
    def test_export_activity_accepts_no_smooth(self) -> None:
        parser = cli.build_parser()
        args = parser.parse_args(["export-activity", "sid:key:1", "--format", "gpx", "--no-smooth"])
        assert args.no_smooth is True

    def test_export_activity_default_smooth_enabled(self) -> None:
        parser = cli.build_parser()
        args = parser.parse_args(["export-activity", "sid:key:1", "--format", "gpx"])
        assert args.no_smooth is False

    def test_export_activity_accepts_outlier_speed(self) -> None:
        parser = cli.build_parser()
        args = parser.parse_args(
            ["export-activity", "sid:key:1", "--format", "gpx", "--outlier-speed", "7:30"]
        )
        assert args.outlier_speed == "7:30"

    def test_export_activity_accepts_smooth_mode_full(self) -> None:
        parser = cli.build_parser()
        args = parser.parse_args(
            ["export-activity", "sid:key:1", "--format", "gpx", "--smooth-mode", "full"]
        )
        assert args.smooth_mode == "full"

    def test_export_activity_default_smooth_mode_match(self) -> None:
        parser = cli.build_parser()
        args = parser.parse_args(["export-activity", "sid:key:1", "--format", "gpx"])
        assert args.smooth_mode == "match"

    def test_upload_to_strava_accepts_no_smooth(self) -> None:
        parser = cli.build_parser()
        args = parser.parse_args(["upload-to-strava", "sid:key:1", "--no-smooth"])
        assert args.no_smooth is True

    def test_upload_to_strava_accepts_outlier_speed(self) -> None:
        parser = cli.build_parser()
        args = parser.parse_args(["upload-to-strava", "sid:key:1", "--outlier-speed", "180kmh"])
        assert args.outlier_speed == "180kmh"

    def test_upload_to_strava_accepts_smooth_mode_full(self) -> None:
        parser = cli.build_parser()
        args = parser.parse_args(["upload-to-strava", "sid:key:1", "--smooth-mode", "full"])
        assert args.smooth_mode == "full"


class TestSmoothingKwargsHelper:
    def test_no_smooth_sets_smooth_false(self) -> None:
        import argparse

        args = argparse.Namespace(no_smooth=True, outlier_speed=None, smooth_mode="match")
        kwargs = cli._smoothing_kwargs(args)
        assert kwargs["smooth"] is False

    def test_outlier_speed_parsed(self) -> None:
        import argparse

        args = argparse.Namespace(no_smooth=False, outlier_speed="180", smooth_mode="match")
        kwargs = cli._smoothing_kwargs(args)
        assert "outlier_speed_mps" in kwargs
        assert kwargs["outlier_speed_mps"] == pytest.approx(50.0, abs=0.1)

    def test_smooth_mode_passed(self) -> None:
        import argparse

        args = argparse.Namespace(no_smooth=False, outlier_speed=None, smooth_mode="full")
        kwargs = cli._smoothing_kwargs(args)
        assert kwargs["smooth_mode"] == "full"

    def test_defaults_produce_minimal_kwargs(self) -> None:
        import argparse

        args = argparse.Namespace(no_smooth=False, outlier_speed=None, smooth_mode="match")
        kwargs = cli._smoothing_kwargs(args)
        assert "smooth" not in kwargs
        assert "outlier_speed_mps" not in kwargs
        assert kwargs["smooth_mode"] == "match"


def _make_step2_auth_client(
    monkeypatch: pytest.MonkeyPatch, *, step2_session: LoginSession | None
) -> None:
    """Wire up a DummyAuthClient that raises Step2RequiredError on password login.

    The stub returns *step2_session* on login_with_step2, and fails the test if it is
    asked for a session when *step2_session* is None.
    """
    from mi_fitness_sync.auth.client import MetaLoginData

    class DummyAuthClient:
        generate_device_id = staticmethod(lambda: "DEV123")

        def __init__(self, service_id: str) -> None:
            pass

        def login_with_password(self, *, email: str, password: str, device_id: str) -> Never:
            raise Step2RequiredError(
                "Step-2 required.",
                payload={"_sign": "sig", "qs": "q", "callback": "cb", "code": 81003},
                step1_token="step1tok",
            )

        def login_with_step2(
            self,
            *,
            email: str,
            step2_code: str,
            step1_token: str | None,
            meta: MetaLoginData | None,
            device_id: str,
            trust: bool = True,
        ) -> LoginSession:
            assert step1_token == "step1tok"
            assert isinstance(meta, MetaLoginData)
            assert meta.sign == "sig"
            if step2_session is None:
                msg = "login_with_step2 must not run for this test"
                raise AssertionError(msg)
            return step2_session

    monkeypatch.setattr(cli, "MiFitnessAuthClient", DummyAuthClient)


def test_login_step2_prompts_and_succeeds(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:

    session = _make_login_session(email="u@x.com", device_id="DEV123")

    _make_step2_auth_client(monkeypatch, step2_session=session)
    monkeypatch.setattr(cli, "load_state", lambda path: None)

    # First input call is not used (email from --email), code prompt returns "123456"
    monkeypatch.setattr("builtins.input", lambda prompt: "123456")

    state_path = str(tmp_path / "state.json")
    exit_code = cli.main(
        ["login", "--email", "u@x.com", "--password", "pass", "--state-path", state_path]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Step-2 verification required" in captured.out
    assert "Login succeeded" in captured.out


def test_login_step2_empty_code_fails(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    _make_step2_auth_client(monkeypatch, step2_session=None)
    monkeypatch.setattr(cli, "load_state", lambda path: None)
    monkeypatch.setattr("builtins.input", lambda prompt: "")

    state_path = str(tmp_path / "state.json")
    exit_code = cli.main(
        ["login", "--email", "u@x.com", "--password", "pass", "--state-path", state_path]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Verification code is required" in captured.err


def test_login_step2_missing_step1_token_warns_and_continues(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    class DummyAuthClient:
        generate_device_id = staticmethod(lambda: "DEV123")

        def __init__(self, service_id: str) -> None:
            pass

        def login_with_password(self, *, email: str, password: str, device_id: str) -> Never:
            raise Step2RequiredError(
                "Step-2 required.",
                payload={"_sign": "sig", "qs": "q", "callback": "cb", "code": 81003},
                step1_token=None,
            )

        def login_with_step2(
            self,
            *,
            email: str,
            step2_code: str,
            step1_token: str | None,
            meta: MetaLoginData | None,
            device_id: str,
            trust: bool = True,
        ) -> LoginSession:
            assert step1_token is None
            return LoginSession(
                email=email,
                user_id="uid",
                c_user_id="cuid",
                service_id="miothealth",
                pass_token="pt",
                service_token="st",
                ssecurity="ss",
                psecurity=None,
                auto_login_url="https://x",
                device_id=device_id,
                slh=None,
                ph=None,
                sts_cookie_header="",
                cookies=[],
            )

    monkeypatch.setattr(cli, "MiFitnessAuthClient", DummyAuthClient)
    monkeypatch.setattr(cli, "load_state", lambda path: None)
    monkeypatch.setattr("builtins.input", lambda prompt: "123456")

    state_path = str(tmp_path / "state.json")
    exit_code = cli.main(
        ["login", "--email", "u@x.com", "--password", "pass", "--state-path", state_path]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "step1Token" in captured.err


def test_login_step2_incomplete_meta(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    class DummyAuthClient:
        generate_device_id = staticmethod(lambda: "DEV123")

        def __init__(self, service_id: str) -> None:
            pass

        def login_with_password(self, *, email: str, password: str, device_id: str) -> Never:
            raise Step2RequiredError(
                "Step-2 required.",
                payload={"_sign": "sig", "qs": "", "callback": "cb", "code": 81003},
                step1_token="tok",
            )

    monkeypatch.setattr(cli, "MiFitnessAuthClient", DummyAuthClient)
    monkeypatch.setattr(cli, "load_state", lambda path: None)

    state_path = str(tmp_path / "state.json")
    exit_code = cli.main(
        ["login", "--email", "u@x.com", "--password", "pass", "--state-path", state_path]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "meta login data is incomplete" in captured.err


def test_format_error_step2_message() -> None:
    error = Step2RequiredError("step2 needed", payload={})
    result = cli.format_error(error)
    assert "could not be completed" in result


def test_login_captcha_prompts_and_retries(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:

    captcha_dir = _patch_captcha_dir(monkeypatch, tmp_path)
    opened_paths = _patch_captcha_opener(monkeypatch, result=True, expected_dir=captcha_dir)

    session = _make_login_session(email="u@x.com", device_id="DEV123")

    call_count = {"login": 0}

    class DummyAuthClient:
        generate_device_id = staticmethod(lambda: "DEV123")

        def __init__(self, service_id: str) -> None:
            pass

        def login_with_password(
            self,
            *,
            email: str,
            password: str,
            device_id: str,
            captcha_code: str | None = None,
            ick: str | None = None,
            meta: MetaLoginData | None = None,
        ) -> LoginSession:
            call_count["login"] += 1
            if call_count["login"] == 1:
                raise CaptchaRequiredError(
                    "/pass/getCode?icodeType=login",
                    captcha_type="captcha",
                    payload={"_sign": "s", "qs": "q", "callback": "c", "code": 87001},
                )
            assert captcha_code == "xyzw"
            assert ick == "ick_val"
            return session

        def fetch_captcha_image(self, captcha_url: str) -> tuple[bytes, str]:
            return b"\x89PNG\r\n\x1a\nmock", "ick_val"

    monkeypatch.setattr(cli, "MiFitnessAuthClient", DummyAuthClient)
    monkeypatch.setattr(cli, "load_state", lambda path: None)
    monkeypatch.setattr("builtins.input", lambda prompt: "xyzw")

    state_path = str(tmp_path / "state.json")
    exit_code = cli.main(
        ["login", "--email", "u@x.com", "--password", "pass", "--state-path", state_path]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Captcha required" in captured.out
    assert "opened automatically" in captured.out
    assert "Login succeeded" in captured.out
    assert call_count["login"] == 2
    assert len(opened_paths) == 1
    assert list(captcha_dir.iterdir()) == []


def test_login_captcha_empty_code_fails(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    captcha_dir = _patch_captcha_dir(monkeypatch, tmp_path)
    _patch_captcha_opener(monkeypatch, result=True, expected_dir=captcha_dir)

    class DummyAuthClient:
        generate_device_id = staticmethod(lambda: "DEV123")

        def __init__(self, service_id: str) -> None:
            pass

        def login_with_password(
            self,
            *,
            email: str,
            password: str,
            device_id: str,
            captcha_code: str | None = None,
            ick: str | None = None,
            meta: MetaLoginData | None = None,
        ) -> Never:
            raise CaptchaRequiredError("/pass/getCode", payload={})

        def fetch_captcha_image(self, captcha_url: str) -> tuple[bytes, str]:
            return b"img", "ick"

    monkeypatch.setattr(cli, "MiFitnessAuthClient", DummyAuthClient)
    monkeypatch.setattr(cli, "load_state", lambda path: None)
    monkeypatch.setattr("builtins.input", lambda prompt: "")

    state_path = str(tmp_path / "state.json")
    exit_code = cli.main(
        ["login", "--email", "u@x.com", "--password", "pass", "--state-path", state_path]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Captcha code is required" in captured.err
    assert list(captcha_dir.iterdir()) == []


def test_login_captcha_wrong_code_fails(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    captcha_dir = _patch_captcha_dir(monkeypatch, tmp_path)
    _patch_captcha_opener(monkeypatch, result=True, expected_dir=captcha_dir)

    class DummyAuthClient:
        generate_device_id = staticmethod(lambda: "DEV123")

        def __init__(self, service_id: str) -> None:
            pass

        def login_with_password(
            self,
            *,
            email: str,
            password: str,
            device_id: str,
            captcha_code: str | None = None,
            ick: str | None = None,
            meta: MetaLoginData | None = None,
        ) -> Never:
            raise CaptchaRequiredError("/pass/getCode", payload={})

        def fetch_captcha_image(self, captcha_url: str) -> tuple[bytes, str]:
            return b"img", "ick"

    monkeypatch.setattr(cli, "MiFitnessAuthClient", DummyAuthClient)
    monkeypatch.setattr(cli, "load_state", lambda path: None)
    monkeypatch.setattr("builtins.input", lambda prompt: "wrong")

    state_path = str(tmp_path / "state.json")
    exit_code = cli.main(
        ["login", "--email", "u@x.com", "--password", "pass", "--state-path", state_path]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "incorrect or expired" in captured.err
    assert list(captcha_dir.iterdir()) == []


def test_login_captcha_open_failure_falls_back_to_manual_open(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:

    captcha_dir = _patch_captcha_dir(monkeypatch, tmp_path)
    opened_paths = _patch_captcha_opener(monkeypatch, result=False, expected_dir=captcha_dir)

    session = _make_login_session(email="u@x.com", device_id="DEV123")

    call_count = {"login": 0}

    class DummyAuthClient:
        generate_device_id = staticmethod(lambda: "DEV123")

        def __init__(self, service_id: str) -> None:
            pass

        def login_with_password(
            self,
            *,
            email: str,
            password: str,
            device_id: str,
            captcha_code: str | None = None,
            ick: str | None = None,
            meta: MetaLoginData | None = None,
        ) -> LoginSession:
            call_count["login"] += 1
            if call_count["login"] == 1:
                raise CaptchaRequiredError(
                    "/pass/getCode", payload={"_sign": "s", "qs": "q", "callback": "c"}
                )
            assert captcha_code == "xyzw"
            return session

        def fetch_captcha_image(self, captcha_url: str) -> tuple[bytes, str]:
            return b"img", "ick_val"

    monkeypatch.setattr(cli, "MiFitnessAuthClient", DummyAuthClient)
    monkeypatch.setattr(cli, "load_state", lambda path: None)
    monkeypatch.setattr("builtins.input", lambda prompt: "xyzw")

    state_path = str(tmp_path / "state.json")
    exit_code = cli.main(
        ["login", "--email", "u@x.com", "--password", "pass", "--state-path", state_path]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Captcha image saved to:" in captured.out
    assert "Open the file to view the captcha" in captured.out
    assert len(opened_paths) == 1
    assert list(captcha_dir.iterdir()) == []


def test_login_captcha_cleans_stale_files_before_open(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:

    captcha_dir = _patch_captcha_dir(monkeypatch, tmp_path)
    stale_path = captcha_dir / "mi_fitness_captcha_stale.png"
    stale_path.write_bytes(b"stale")

    def open_captcha_image(path: Path) -> bool:
        assert not stale_path.exists()
        assert path.parent == captcha_dir
        return False

    monkeypatch.setattr(cli, "_open_captcha_image", open_captcha_image)

    session = _make_login_session(email="u@x.com", device_id="DEV123")

    call_count = {"login": 0}

    class DummyAuthClient:
        generate_device_id = staticmethod(lambda: "DEV123")

        def __init__(self, service_id: str) -> None:
            pass

        def login_with_password(
            self,
            *,
            email: str,
            password: str,
            device_id: str,
            captcha_code: str | None = None,
            ick: str | None = None,
            meta: MetaLoginData | None = None,
        ) -> LoginSession:
            call_count["login"] += 1
            if call_count["login"] == 1:
                raise CaptchaRequiredError(
                    "/pass/getCode", payload={"_sign": "s", "qs": "q", "callback": "c"}
                )
            assert captcha_code == "xyzw"
            return session

        def fetch_captcha_image(self, captcha_url: str) -> tuple[bytes, str]:
            return b"img", "ick_val"

    monkeypatch.setattr(cli, "MiFitnessAuthClient", DummyAuthClient)
    monkeypatch.setattr(cli, "load_state", lambda path: None)
    monkeypatch.setattr("builtins.input", lambda prompt: "xyzw")

    state_path = str(tmp_path / "state.json")
    exit_code = cli.main(
        ["login", "--email", "u@x.com", "--password", "pass", "--state-path", state_path]
    )

    assert exit_code == 0
    assert list(captcha_dir.iterdir()) == []


def test_cleanup_captcha_artifacts_warns_when_delete_fails(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    captcha_path = tmp_path / "mi_fitness_captcha.png"
    captcha_path.write_bytes(b"img")

    original_unlink = Path.unlink

    def fail_unlink(self: Path, missing_ok: bool = False) -> None:
        if self == captcha_path:
            raise OSError("still open")
        original_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", fail_unlink)

    cli._cleanup_captcha_artifacts(captcha_path)
    captured = capsys.readouterr()

    assert "Failed to delete captcha image" in captured.err
    assert str(captcha_path) in captured.err
    assert captcha_path.exists()


def test_login_captcha_no_url_fails(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    class DummyAuthClient:
        generate_device_id = staticmethod(lambda: "DEV123")

        def __init__(self, service_id: str) -> None:
            pass

        def login_with_password(
            self,
            *,
            email: str,
            password: str,
            device_id: str,
            captcha_code: str | None = None,
            ick: str | None = None,
            meta: MetaLoginData | None = None,
        ) -> Never:
            raise CaptchaRequiredError("", payload={})

    monkeypatch.setattr(cli, "MiFitnessAuthClient", DummyAuthClient)
    monkeypatch.setattr(cli, "load_state", lambda path: None)

    state_path = str(tmp_path / "state.json")
    exit_code = cli.main(
        ["login", "--email", "u@x.com", "--password", "pass", "--state-path", state_path]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "captcha URL" in captured.err


def test_login_captcha_ick_missing_fails(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    class DummyAuthClient:
        generate_device_id = staticmethod(lambda: "DEV123")

        def __init__(self, service_id: str) -> None:
            pass

        def login_with_password(
            self,
            *,
            email: str,
            password: str,
            device_id: str,
            captcha_code: str | None = None,
            ick: str | None = None,
            meta: MetaLoginData | None = None,
        ) -> Never:
            raise CaptchaRequiredError("/pass/getCode", payload={})

        def fetch_captcha_image(self, captcha_url: str) -> Never:
            raise XiaomiApiError(
                "Captcha response did not include an ICK token; cannot submit captcha."
            )

    monkeypatch.setattr(cli, "MiFitnessAuthClient", DummyAuthClient)
    monkeypatch.setattr(cli, "load_state", lambda path: None)

    state_path = str(tmp_path / "state.json")
    exit_code = cli.main(
        ["login", "--email", "u@x.com", "--password", "pass", "--state-path", state_path]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "ICK token" in captured.err


def _make_notification_auth_client(
    monkeypatch: pytest.MonkeyPatch, *, notification_session: LoginSession
) -> None:
    """Wire up a DummyAuthClient that raises NotificationRequiredError on password login.

    The stub returns *notification_session* on login_with_notification_code.
    """

    class DummyAuthClient:
        generate_device_id = staticmethod(lambda: "DEV123")

        def __init__(self, service_id: str) -> None:
            pass

        def login_with_password(self, *, email: str, password: str, device_id: str) -> Never:
            raise NotificationRequiredError(
                "https://account.xiaomi.com/fe/service/identity/authStart?context=ctx"
            )

        def send_notification_email_code(self, notification_url: str) -> str:
            assert notification_url.endswith("context=ctx")
            return "ctx"

        def login_with_notification_code(
            self, *, email: str, code: str, context: str, device_id: str
        ) -> LoginSession:
            assert code == "123456"
            assert context == "ctx"
            return notification_session

    monkeypatch.setattr(cli, "MiFitnessAuthClient", DummyAuthClient)


def test_login_notification_prompts_and_succeeds(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:

    session = _make_login_session(email="u@x.com", device_id="DEV123")

    _make_notification_auth_client(monkeypatch, notification_session=session)
    monkeypatch.setattr(cli, "load_state", lambda path: None)
    monkeypatch.setattr("builtins.input", lambda prompt: "123456")

    state_path = str(tmp_path / "state.json")
    exit_code = cli.main(
        ["login", "--email", "u@x.com", "--password", "pass", "--state-path", state_path]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Identity verification required" in captured.out
    assert "Login succeeded" in captured.out


NOTIFICATION_URL = (
    "https://account.xiaomi.com/fe/service/identity/authStart?sid=miothealth&context=ctx"
)
STEP2_PAYLOAD = {"_sign": "sig", "qs": "q", "callback": "cb", "code": 81003}
CAPTCHA_PAYLOAD = {"_sign": "s", "qs": "q", "callback": "c", "code": 87001}


def _login_session() -> LoginSession:
    return _make_login_session(email="u@x.com", device_id="DEV123")


def _take[T](outcomes: list[T | MiFitnessError]) -> T:
    """Pop the next scripted outcome, raising it when the script queued an error."""
    outcome = outcomes.pop(0)
    if isinstance(outcome, MiFitnessError):
        raise outcome
    return outcome


def _install_auth_client(
    monkeypatch: pytest.MonkeyPatch,
    *,
    password: Sequence[LoginSession | MiFitnessError] = (),
    step2: Sequence[LoginSession | MiFitnessError] = (),
    codes: Sequence[LoginSession | MiFitnessError] = (),
    sends: Sequence[str | MiFitnessError] = (),
) -> dict[str, list[str]]:
    """Install an auth client double that replays each method's queued outcomes.

    Every call is recorded so the caller can assert on it.
    """
    password_outcomes, step2_outcomes, code_outcomes = list(password), list(step2), list(codes)
    send_outcomes = list(sends)
    calls: dict[str, list[str]] = {"step2": [], "sends": [], "codes": []}

    class ScriptedAuthClient:
        generate_device_id = staticmethod(lambda: "DEV123")

        def __init__(self, service_id: str) -> None:
            pass

        def login_with_password(
            self,
            *,
            email: str,
            password: str,
            device_id: str,
            captcha_code: str | None = None,
            ick: str | None = None,
            meta: MetaLoginData | None = None,
        ) -> LoginSession:
            return _take(password_outcomes)

        def fetch_captcha_image(self, captcha_url: str) -> tuple[bytes, str]:
            return b"\x89PNG\r\n\x1a\nmock", "ick_val"

        def login_with_step2(
            self,
            *,
            email: str,
            step2_code: str,
            step1_token: str | None,
            meta: MetaLoginData | None,
            device_id: str,
            trust: bool = True,
        ) -> LoginSession:
            calls["step2"].append(step2_code)
            return _take(step2_outcomes)

        def send_notification_email_code(self, notification_url: str) -> str:
            calls["sends"].append(notification_url)
            return _take(send_outcomes) if send_outcomes else "ctx"

        def login_with_notification_code(
            self, *, email: str, code: str, context: str, device_id: str
        ) -> LoginSession:
            calls["codes"].append(code)
            return _take(code_outcomes)

    monkeypatch.setattr(cli, "MiFitnessAuthClient", ScriptedAuthClient)
    monkeypatch.setattr(cli, "load_state", lambda path: None)
    return calls


def _feed_inputs(monkeypatch: pytest.MonkeyPatch, answers: list[str]) -> None:
    """Answer prompts in order, failing the test on any prompt beyond the scripted ones."""
    remaining = list(answers)

    def fake_input(prompt: str) -> str:
        assert remaining, f"Unexpected extra prompt: {prompt}"
        return remaining.pop(0)

    monkeypatch.setattr("builtins.input", fake_input)


def _run_login(tmp_path: Path) -> int:
    state_path = str(tmp_path / "state.json")
    return cli.main(
        ["login", "--email", "u@x.com", "--password", "pass", "--state-path", state_path]
    )


def test_login_parser_accepts_verbose() -> None:
    args = cli.build_parser().parse_args(["login", "--verbose"])

    assert args.verbose is True


def test_login_notification_empty_code_reprompts(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    _install_auth_client(
        monkeypatch,
        password=[NotificationRequiredError(NOTIFICATION_URL)],
        codes=[_login_session()],
    )
    _feed_inputs(monkeypatch, ["", "123456"])

    exit_code = _run_login(tmp_path)

    assert exit_code == 0
    assert "Verification code is required" in capsys.readouterr().err


def test_login_notification_rejected_code_reprompts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls = _install_auth_client(
        monkeypatch,
        password=[NotificationRequiredError(NOTIFICATION_URL)],
        codes=[VerificationCodeRejectedError("Rejected", code=70014), _login_session()],
    )
    _feed_inputs(monkeypatch, ["000000", "123456"])

    exit_code = _run_login(tmp_path)

    assert exit_code == 0
    assert calls["codes"] == ["000000", "123456"]


def test_login_notification_other_error_code_aborts(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    _install_auth_client(
        monkeypatch,
        password=[NotificationRequiredError(NOTIFICATION_URL)],
        codes=[XiaomiApiError("Too many attempts", code=70022)],
    )
    _feed_inputs(monkeypatch, ["000000"])

    exit_code = _run_login(tmp_path)

    assert exit_code == 1
    assert "Too many attempts (code=70022)" in capsys.readouterr().err


def test_login_notification_resend_requests_new_code(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    calls = _install_auth_client(
        monkeypatch,
        password=[NotificationRequiredError(NOTIFICATION_URL)],
        codes=[_login_session()],
    )
    _feed_inputs(monkeypatch, ["resend", "123456"])

    exit_code = _run_login(tmp_path)

    assert exit_code == 0
    assert len(calls["sends"]) == 2
    assert "Sent a new verification code" in capsys.readouterr().out


def test_login_notification_resend_failure_reprompts(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    _install_auth_client(
        monkeypatch,
        password=[NotificationRequiredError(NOTIFICATION_URL)],
        sends=["ctx", XiaomiApiError("Sending limit reached", code=70022)],
        codes=[_login_session()],
    )
    _feed_inputs(monkeypatch, ["resend", "123456"])

    exit_code = _run_login(tmp_path)

    assert exit_code == 0
    assert "Sending limit reached (code=70022)" in capsys.readouterr().err


@pytest.mark.parametrize("answer", ["cancel", "CANCEL"])
def test_login_notification_cancel_aborts_without_submitting(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path, answer: str
) -> None:
    calls = _install_auth_client(
        monkeypatch, password=[NotificationRequiredError(NOTIFICATION_URL)]
    )
    _feed_inputs(monkeypatch, [answer])

    exit_code = _run_login(tmp_path)

    assert exit_code == 1
    assert calls["codes"] == []
    assert "Login cancelled" in capsys.readouterr().err


def test_login_notification_send_failure_shows_verification_url(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    _install_auth_client(
        monkeypatch,
        password=[NotificationRequiredError(NOTIFICATION_URL)],
        sends=[XiaomiApiError("Failed to request a Xiaomi verification email (HTTP 503).")],
    )
    _feed_inputs(monkeypatch, [])

    exit_code = _run_login(tmp_path)

    assert exit_code == 1
    assert f"Verification URL: {NOTIFICATION_URL}" in capsys.readouterr().err


def test_login_captcha_then_notification_reaches_email_prompt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captcha_dir = _patch_captcha_dir(monkeypatch, tmp_path)
    _patch_captcha_opener(monkeypatch, result=True, expected_dir=captcha_dir)
    calls = _install_auth_client(
        monkeypatch,
        password=[
            CaptchaRequiredError("/pass/getCode?icodeType=login", payload=CAPTCHA_PAYLOAD),
            NotificationRequiredError(NOTIFICATION_URL),
        ],
        codes=[_login_session()],
    )
    _feed_inputs(monkeypatch, ["xyzw", "123456"])

    exit_code = _run_login(tmp_path)

    assert exit_code == 0
    assert calls["codes"] == ["123456"]


def test_login_step2_then_notification_reaches_email_prompt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls = _install_auth_client(
        monkeypatch,
        password=[Step2RequiredError("Step-2 required.", payload=STEP2_PAYLOAD, step1_token="tok")],
        step2=[NotificationRequiredError(NOTIFICATION_URL)],
        codes=[_login_session()],
    )
    _feed_inputs(monkeypatch, ["111111", "123456"])

    exit_code = _run_login(tmp_path)

    assert exit_code == 0
    assert calls["codes"] == ["123456"]


def test_login_endless_challenges_stop_at_limit(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    def step2_challenge() -> Step2RequiredError:
        return Step2RequiredError("Step-2 required.", payload=STEP2_PAYLOAD, step1_token="tok")

    calls = _install_auth_client(
        monkeypatch,
        password=[step2_challenge()],
        step2=[step2_challenge() for _ in range(cli.MAX_LOGIN_CHALLENGES)],
    )
    _feed_inputs(monkeypatch, ["111111"] * cli.MAX_LOGIN_CHALLENGES)

    exit_code = _run_login(tmp_path)

    assert exit_code == 1
    assert len(calls["step2"]) == cli.MAX_LOGIN_CHALLENGES
    assert "verification steps in a row" in capsys.readouterr().err
