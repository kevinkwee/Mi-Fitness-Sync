from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import asdict, replace
from datetime import datetime, timezone, timedelta
from pathlib import Path

from mi_fitness_sync.activity.client import MiFitnessActivitiesClient
from mi_fitness_sync.activity.formatting import parse_cli_time
from mi_fitness_sync.activity.models import Activity, ActivityDetail
from mi_fitness_sync.activity.utils import render_activities_table
from mi_fitness_sync.auth.client import DEFAULT_SERVICE_ID, MiFitnessAuthClient
from mi_fitness_sync.auth.state import utc_now_iso
from mi_fitness_sync.auth.store import delete_state, load_state, resolve_state_path, save_state
from mi_fitness_sync.export.render import SUPPORTED_EXPORT_FORMATS, render_export
from mi_fitness_sync.exceptions import (
    AuthStateNotFoundError,
    CaptchaRequiredError,
    MiFitnessError,
    NotificationRequiredError,
    Step2RequiredError,
    StravaError,
    XiaomiApiError,
)
from mi_fitness_sync.paths import get_exports_dir


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Mi Fitness Sync CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    login_parser = subparsers.add_parser("login", help="Authenticate with Mi Fitness via Xiaomi Passport")
    login_parser.add_argument("--email", required=True, help="Mi / Xiaomi account email")
    login_parser.add_argument("--password", required=True, help="Mi / Xiaomi account password")
    login_parser.add_argument("--state-path", help="Override the persisted auth state path")

    logout_parser = subparsers.add_parser("logout", help="Delete the persisted auth state")
    logout_parser.add_argument("--state-path", help="Override the persisted auth state path")

    status_parser = subparsers.add_parser("auth-status", help="Show persisted auth status")
    status_parser.add_argument("--state-path", help="Override the persisted auth state path")
    status_parser.add_argument("--json", action="store_true", help="Print full JSON auth state")

    activities_parser = subparsers.add_parser("list-activities", help="List workout activities from Mi Fitness")
    activities_parser.add_argument("--state-path", help="Override the persisted auth state path")
    activities_parser.add_argument("--since", help="Inclusive start time as unix seconds or ISO-8601")
    activities_parser.add_argument("--until", help="Inclusive end time as unix seconds or ISO-8601")
    activities_parser.add_argument("--limit", type=int, default=20, help="Maximum activities to return (default: 20)")
    activities_parser.add_argument("--category", help="Optional Mi Fitness category filter")
    activities_parser.add_argument(
        "--country-code",
        help="Optional two-letter country override such as ID, GB, or US; mapped to the Mi Fitness region automatically",
    )
    activities_parser.add_argument("--json", action="store_true", help="Print activities as JSON")
    activities_parser.add_argument("--strava", action="store_true", help="Show whether each activity is already uploaded to Strava")
    activities_parser.add_argument("--strava-token-path", help="Override the Strava token file path")
    activities_parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    detail_parser = subparsers.add_parser("activity-detail", help="Fetch normalized detail for a listed Mi Fitness activity")
    detail_parser.add_argument("activity_id", help="Activity ID from list-activities, in sid:key:time format")
    detail_parser.add_argument("--state-path", help="Override the persisted auth state path")
    detail_parser.add_argument(
        "--country-code",
        help="Optional two-letter country override such as ID, GB, or US; mapped to the Mi Fitness region automatically",
    )
    detail_parser.add_argument("--json", action="store_true", help="Print the normalized activity detail as JSON")
    detail_parser.add_argument("--no-cache", action="store_true", help="Disable local FDS binary cache")
    detail_parser.add_argument("--cache-dir", help="Override the local FDS cache directory")
    detail_parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    export_parser = subparsers.add_parser("export-activity", help="Export one Mi Fitness activity to GPX, TCX, or FIT")
    export_parser.add_argument("activity_id", help="Activity ID from list-activities, in sid:key:time format")
    export_parser.add_argument("--state-path", help="Override the persisted auth state path")
    export_parser.add_argument(
        "--country-code",
        help="Optional two-letter country override such as ID, GB, or US; mapped to the Mi Fitness region automatically",
    )
    export_parser.add_argument("--format", required=True, choices=SUPPORTED_EXPORT_FORMATS, help="Export format")
    export_parser.add_argument(
        "--output",
        help="Destination file path (default: ~/.mi_fitness_sync/exports/<sanitized_title>_<local_start_time>.<format>)",
    )
    export_parser.add_argument("--gzip", action="store_true", help="Gzip-compress the exported payload before writing it")
    export_parser.add_argument("--no-cache", action="store_true", help="Disable local FDS binary cache")
    export_parser.add_argument("--cache-dir", help="Override the local FDS cache directory")
    export_parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    strava_login_parser = subparsers.add_parser("strava-login", help="Authenticate with Strava via OAuth2")
    strava_login_parser.add_argument("--client-id", help="Strava API client ID")
    strava_login_parser.add_argument("--client-secret", help="Strava API client secret")
    strava_login_parser.add_argument("--port", type=int, default=5478, help="Local port for OAuth callback (default: 5478)")
    strava_login_parser.add_argument("--strava-token-path", help="Override the Strava token file path")

    strava_status_parser = subparsers.add_parser("strava-status", help="Show Strava auth status")
    strava_status_parser.add_argument("--strava-token-path", help="Override the Strava token file path")

    upload_parser = subparsers.add_parser("upload-to-strava", help="Upload a Mi Fitness activity to Strava as FIT")
    upload_parser.add_argument("activity_id", help="Activity ID from list-activities, in sid:key:time format")
    upload_parser.add_argument("--state-path", help="Override the persisted Mi Fitness auth state path")
    upload_parser.add_argument("--strava-token-path", help="Override the Strava token file path")
    upload_parser.add_argument(
        "--country-code",
        help="Optional two-letter country override such as ID, GB, or US; mapped to the Mi Fitness region automatically",
    )
    upload_parser.add_argument(
        "--output",
        help="Destination file path for the local FIT copy (default: ~/.mi_fitness_sync/exports/<title>_<time>.fit)",
    )
    upload_parser.add_argument("--no-cache", action="store_true", help="Disable local FDS binary cache")
    upload_parser.add_argument("--cache-dir", help="Override the local FDS cache directory")
    upload_parser.add_argument(
        "--skip-duplicate-check",
        action="store_true",
        help="Skip checking for existing Strava activities with a similar start time",
    )
    upload_parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if getattr(args, "verbose", False):
        logging.basicConfig(level=logging.DEBUG, format="%(name)s %(levelname)s: %(message)s")

    try:
        if args.command == "login":
            return handle_login(args)
        if args.command == "logout":
            return handle_logout(args)
        if args.command == "auth-status":
            return handle_auth_status(args)
        if args.command == "list-activities":
            return handle_list_activities(args)
        if args.command == "activity-detail":
            return handle_activity_detail(args)
        if args.command == "export-activity":
            return handle_export_activity(args)
        if args.command == "strava-login":
            return handle_strava_login(args)
        if args.command == "strava-status":
            return handle_strava_status(args)
        if args.command == "upload-to-strava":
            return handle_upload_to_strava(args)
    except MiFitnessError as exc:
        print(format_error(exc), file=sys.stderr)
        return 1

    parser.error(f"Unsupported command: {args.command}")
    return 2


def handle_login(args: argparse.Namespace) -> int:
    existing_state = load_state(args.state_path)
    device_id = existing_state.device_id if existing_state else MiFitnessAuthClient.generate_device_id()

    client = MiFitnessAuthClient(service_id=DEFAULT_SERVICE_ID)
    session = client.login_with_password(
        email=args.email,
        password=args.password,
        device_id=device_id,
    )

    state = session.to_auth_state()
    if existing_state:
        state = replace(state, created_at=existing_state.created_at, updated_at=utc_now_iso())
    path = save_state(state, args.state_path)

    print("Login succeeded.")
    print(f"State path: {path}")
    print(f"User ID: {state.user_id}")
    print(f"cUserId: {state.c_user_id}")
    print(f"Service ID: {state.service_id}")
    print(f"Device ID: {state.device_id}")
    print(f"Service token present: {'yes' if bool(state.service_token) else 'no'}")
    return 0


def handle_logout(args: argparse.Namespace) -> int:
    path = resolve_state_path(args.state_path)
    delete_state(args.state_path)
    print(f"Removed auth state at {path}")
    return 0


def handle_auth_status(args: argparse.Namespace) -> int:
    state = load_state(args.state_path)
    if state is None:
        raise AuthStateNotFoundError("No persisted Mi Fitness auth state was found.")

    if args.json:
        print(json.dumps(asdict(state), indent=2, sort_keys=True))
        return 0

    print("Auth state found.")
    print(f"State path: {resolve_state_path(args.state_path)}")
    print(f"Email: {state.email}")
    print(f"User ID: {state.user_id}")
    print(f"cUserId: {state.c_user_id}")
    print(f"Service ID: {state.service_id}")
    print(f"Device ID: {state.device_id}")
    print(f"Service token present: {'yes' if bool(state.service_token) else 'no'}")
    print(f"Created at: {state.created_at}")
    print(f"Updated at: {state.updated_at}")
    return 0


def handle_list_activities(args: argparse.Namespace) -> int:
    if args.limit <= 0:
        raise MiFitnessError("--limit must be greater than zero.")

    end_time = parse_cli_time(args.until) if args.until else None
    start_time = parse_cli_time(args.since) if args.since else None
    if start_time is not None and end_time is not None and start_time > end_time:
        raise MiFitnessError("--since must be earlier than or equal to --until.")

    client = _activities_client(args.state_path, args.country_code)
    activities = client.list_activities(
        start_time=start_time,
        end_time=end_time,
        limit=args.limit,
        category=args.category,
    )

    strava_status = None
    if args.strava:
        strava_status = _fetch_strava_status(activities, args.strava_token_path)

    if args.json:
        items = []
        for activity in activities:
            item = activity.to_json_dict()
            if strava_status is not None:
                item["in_strava"] = strava_status.get(activity.activity_id, False)
            items.append(item)
        print(json.dumps(items, indent=2, sort_keys=True))
        return 0

    print(render_activities_table(activities, strava_status=strava_status))
    return 0


def handle_activity_detail(args: argparse.Namespace) -> int:
    client = _activities_client(
        args.state_path, args.country_code,
        no_cache=args.no_cache, cache_dir=args.cache_dir,
    )
    detail = client.get_activity_detail(args.activity_id)

    if args.json:
        print(json.dumps(detail.to_json_dict(), indent=2, sort_keys=True))
        return 0

    print(f"Activity ID: {detail.activity.activity_id}")
    print(f"Title: {detail.activity.title}")
    print(f"Category: {detail.activity.category or 'unknown'}")
    print(f"Start: {detail.start_time}")
    print(f"End: {detail.end_time}")
    print(f"Duration seconds: {detail.total_duration_seconds}")
    print(f"Distance meters: {detail.total_distance_meters}")
    print(f"Calories: {detail.total_calories if detail.total_calories is not None else 'unknown'}")
    print(f"Track points: {len(detail.track_points)}")
    print(f"Samples: {len(detail.samples)}")
    return 0


def handle_export_activity(args: argparse.Namespace) -> int:
    client = _activities_client(
        args.state_path, args.country_code,
        no_cache=args.no_cache, cache_dir=args.cache_dir,
    )
    detail = client.get_activity_detail(args.activity_id)
    export = render_export(detail, args.format, compress=args.gzip)

    if args.output:
        output_path = Path(args.output)
    else:
        safe_title = _sanitize_filename(detail.activity.title)
        start_dt = _activity_local_datetime(detail)
        date_str = start_dt.strftime("%Y%m%d_%H%M%S")
        suffix = f".{args.format}.gz" if args.gzip else f".{args.format}"
        output_path = get_exports_dir() / f"{safe_title}_{date_str}{suffix}"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(export.payload)

    print(f"Exported {detail.activity.activity_id} to {output_path}")
    print(f"Format: {export.file_format}")
    print(f"Compressed: {'yes' if export.compressed else 'no'}")
    print(f"Bytes written: {len(export.payload)}")
    return 0


def handle_strava_login(args: argparse.Namespace) -> int:
    from mi_fitness_sync.strava.auth import run_oauth_flow
    from mi_fitness_sync.strava.store import StravaTokenState, save_tokens

    client_id = args.client_id
    client_secret = args.client_secret
    if not client_id:
        client_id = input("Strava client ID: ").strip()
    if not client_secret:
        client_secret = input("Strava client secret: ").strip()
    if not client_id or not client_secret:
        raise MiFitnessError(
            "Strava client_id and client_secret are required.\n"
            "Pass --client-id and --client-secret or enter them when prompted."
        )

    token_data = run_oauth_flow(client_id, client_secret, port=args.port)

    athlete = token_data.get("athlete", {})
    state = StravaTokenState(
        client_id=client_id,
        client_secret=client_secret,
        access_token=token_data["access_token"],
        refresh_token=token_data["refresh_token"],
        expires_at=token_data["expires_at"],
        athlete_id=athlete.get("id"),
        created_at=utc_now_iso(),
        updated_at=utc_now_iso(),
    )
    path = save_tokens(state, args.strava_token_path)

    print("Strava login succeeded.")
    print(f"Token path: {path}")
    print(f"Athlete ID: {state.athlete_id}")
    return 0


def handle_strava_status(args: argparse.Namespace) -> int:
    from mi_fitness_sync.strava.store import load_tokens, resolve_token_path

    state = load_tokens(args.strava_token_path)
    if state is None:
        raise MiFitnessError("No Strava token state found. Run 'strava-login' first.")

    print("Strava auth state found.")
    print(f"Token path: {resolve_token_path(args.strava_token_path)}")
    print(f"Athlete ID: {state.athlete_id}")
    print(f"Token expires at: {datetime.fromtimestamp(state.expires_at, tz=timezone.utc).isoformat()}")
    print(f"Created at: {state.created_at}")
    print(f"Updated at: {state.updated_at}")
    return 0


def handle_upload_to_strava(args: argparse.Namespace) -> int:
    from mi_fitness_sync.strava.client import StravaClient
    from mi_fitness_sync.strava.sport_mapping import strava_sport_type
    from mi_fitness_sync.strava.store import load_tokens

    token_state = load_tokens(args.strava_token_path)
    if token_state is None:
        raise MiFitnessError("No Strava token state found. Run 'strava-login' first.")

    client = _activities_client(
        args.state_path, args.country_code,
        no_cache=args.no_cache, cache_dir=args.cache_dir,
    )
    detail = client.get_activity_detail(args.activity_id)
    export = render_export(detail, "fit")

    # Save FIT file locally
    if args.output:
        output_path = Path(args.output)
    else:
        safe_title = _sanitize_filename(detail.activity.title)
        start_dt = _activity_local_datetime(detail)
        date_str = start_dt.strftime("%Y%m%d_%H%M%S")
        output_path = get_exports_dir() / f"{safe_title}_{date_str}.fit"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(export.payload)

    print(f"Saved FIT file to {output_path} ({len(export.payload)} bytes)")

    # Upload to Strava
    sport = strava_sport_type(detail.activity.sport_type)
    strava = StravaClient(token_state, token_path=args.strava_token_path)

    # Duplicate check: look for Strava activities within ±5 minutes of start time
    if not args.skip_duplicate_check:
        _DUPLICATE_WINDOW_SECONDS = 5 * 60
        after_ts = detail.start_time - _DUPLICATE_WINDOW_SECONDS
        before_ts = detail.start_time + _DUPLICATE_WINDOW_SECONDS
        existing = strava.list_activities(after=after_ts, before=before_ts)
        if existing:
            print("\nPotential duplicate(s) found on Strava:")
            for act in existing:
                name = act.get("name", "Untitled")
                start = act.get("start_date_local", act.get("start_date", "unknown"))
                stype = act.get("sport_type", "unknown")
                print(f"  - {name}  |  {start}  |  {stype}")
            answer = input("\nProceed with upload anyway? [y/N] ").strip().lower()
            if answer != "y":
                print("Upload cancelled.")
                return 0

    result = strava.upload_activity(export.payload, sport_type=sport, external_id=args.activity_id)

    activity_id = result.get("activity_id")
    print(f"Uploaded to Strava successfully.")
    if activity_id:
        print(f"Strava activity: https://www.strava.com/activities/{activity_id}")
    return 0


def _fetch_strava_status(
    activities: list[Activity],
    strava_token_path: str | None,
) -> dict[str, bool] | None:
    """Query Strava and return a map of activity_id → matched boolean.

    Returns ``None`` (and prints a warning) when Strava auth is unavailable.
    """
    from mi_fitness_sync.strava.client import StravaClient
    from mi_fitness_sync.strava.store import load_tokens

    token_state = load_tokens(strava_token_path)
    if token_state is None:
        print("Warning: No Strava token state found — skipping Strava column.", file=sys.stderr)
        return None

    start_times = [a.start_time for a in activities if a.start_time is not None]
    if not start_times:
        return {a.activity_id: False for a in activities}

    after_ts = min(start_times) - 1
    before_ts = max(start_times) + 1

    try:
        strava = StravaClient(token_state, token_path=strava_token_path)
        per_page = 200
        strava_activities: list[dict] = []
        page = 1
        while True:
            batch = strava.list_activities(after=after_ts, before=before_ts, per_page=per_page, page=page)
            strava_activities.extend(batch)
            if len(batch) < per_page:
                break
            page += 1
    except (StravaError, Exception) as exc:
        print(f"Warning: Failed to query Strava — skipping Strava column. ({exc})", file=sys.stderr)
        return None

    strava_starts: list[int] = []
    for sa in strava_activities:
        start_str = sa.get("start_date")
        if start_str:
            dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
            strava_starts.append(int(dt.timestamp()))

    status: dict[str, bool] = {}
    for activity in activities:
        if activity.start_time is None:
            status[activity.activity_id] = False
            continue
        matched = activity.start_time in strava_starts
        status[activity.activity_id] = matched
    return status


def _sanitize_filename(title: str) -> str:
    """Replace spaces with underscores and strip non-alphanumeric/underscore chars."""
    name = title.replace(" ", "_")
    return re.sub(r"[^\w]", "", name)


def _activity_local_datetime(detail: ActivityDetail) -> datetime:
    """Return the activity start time as a local datetime.

    Uses the activity's zone_offset_seconds when available, otherwise falls
    back to the system local timezone.
    """
    ts = detail.start_time
    if detail.zone_offset_seconds is not None:
        tz = timezone(timedelta(seconds=detail.zone_offset_seconds))
    else:
        tz = None
    utc_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    if tz is not None:
        return utc_dt.astimezone(tz)
    return utc_dt.astimezone()


def _activities_client(
    state_path: str | None,
    country_code: str | None,
    *,
    no_cache: bool = False,
    cache_dir: str | None = None,
) -> MiFitnessActivitiesClient:
    state = load_state(state_path)
    if state is None:
        raise AuthStateNotFoundError("No persisted Mi Fitness auth state was found.")
    kwargs: dict[str, object] = {"country_code": country_code, "no_cache": no_cache}
    if cache_dir is not None:
        kwargs["cache_dir"] = cache_dir
    return MiFitnessActivitiesClient(state, **kwargs)  # type: ignore[arg-type]


def format_error(exc: MiFitnessError) -> str:
    if isinstance(exc, CaptchaRequiredError):
        return f"Login requires a captcha challenge. URL: {exc.captcha_url}"
    if isinstance(exc, NotificationRequiredError):
        return f"Login requires additional verification in a browser or app. URL: {exc.notification_url}"
    if isinstance(exc, Step2RequiredError):
        return "Login requires a Xiaomi Passport step-2 verification flow that this CLI does not automate yet."
    if isinstance(exc, StravaError):
        return str(exc)
    if isinstance(exc, XiaomiApiError):
        if exc.code is None:
            return str(exc)
        return f"{exc} (code={exc.code})"
    return str(exc)
