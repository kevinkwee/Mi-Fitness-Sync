from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, replace
from pathlib import Path

from mi_fitness_sync.activities import MiFitnessActivitiesClient, parse_cli_time, render_activities_table
from mi_fitness_sync.auth import DEFAULT_SERVICE_ID, MiFitnessAuthClient
from mi_fitness_sync.exports import SUPPORTED_EXPORT_FORMATS, render_export
from mi_fitness_sync.exceptions import (
    AuthStateNotFoundError,
    CaptchaRequiredError,
    MiFitnessError,
    NotificationRequiredError,
    Step2RequiredError,
    XiaomiApiError,
)
from mi_fitness_sync.storage import delete_state, load_state, resolve_state_path, save_state, utc_now_iso


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
    activities_parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    detail_parser = subparsers.add_parser("activity-detail", help="Fetch normalized detail for a listed Mi Fitness activity")
    detail_parser.add_argument("activity_id", help="Activity ID from list-activities, in sid:key:time format")
    detail_parser.add_argument("--state-path", help="Override the persisted auth state path")
    detail_parser.add_argument(
        "--country-code",
        help="Optional two-letter country override such as ID, GB, or US; mapped to the Mi Fitness region automatically",
    )
    detail_parser.add_argument("--json", action="store_true", help="Print the normalized activity detail as JSON")
    detail_parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    export_parser = subparsers.add_parser("export-activity", help="Export one Mi Fitness activity to GPX, TCX, or FIT")
    export_parser.add_argument("activity_id", help="Activity ID from list-activities, in sid:key:time format")
    export_parser.add_argument("--state-path", help="Override the persisted auth state path")
    export_parser.add_argument(
        "--country-code",
        help="Optional two-letter country override such as ID, GB, or US; mapped to the Mi Fitness region automatically",
    )
    export_parser.add_argument("--format", required=True, choices=SUPPORTED_EXPORT_FORMATS, help="Export format")
    export_parser.add_argument("--output", required=True, help="Destination file path")
    export_parser.add_argument("--gzip", action="store_true", help="Gzip-compress the exported payload before writing it")
    export_parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

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

    if args.json:
        print(json.dumps([activity.to_json_dict() for activity in activities], indent=2, sort_keys=True))
        return 0

    print(render_activities_table(activities))
    return 0


def handle_activity_detail(args: argparse.Namespace) -> int:
    client = _activities_client(args.state_path, args.country_code)
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
    client = _activities_client(args.state_path, args.country_code)
    detail = client.get_activity_detail(args.activity_id)
    export = render_export(detail, args.format, compress=args.gzip)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(export.payload)

    print(f"Exported {detail.activity.activity_id} to {output_path}")
    print(f"Format: {export.file_format}")
    print(f"Compressed: {'yes' if export.compressed else 'no'}")
    print(f"Bytes written: {len(export.payload)}")
    return 0


def _activities_client(state_path: str | None, country_code: str | None) -> MiFitnessActivitiesClient:
    state = load_state(state_path)
    if state is None:
        raise AuthStateNotFoundError("No persisted Mi Fitness auth state was found.")
    return MiFitnessActivitiesClient(state, country_code=country_code)


def format_error(exc: MiFitnessError) -> str:
    if isinstance(exc, CaptchaRequiredError):
        return f"Login requires a captcha challenge. URL: {exc.captcha_url}"
    if isinstance(exc, NotificationRequiredError):
        return f"Login requires additional verification in a browser or app. URL: {exc.notification_url}"
    if isinstance(exc, Step2RequiredError):
        return "Login requires a Xiaomi Passport step-2 verification flow that this CLI does not automate yet."
    if isinstance(exc, XiaomiApiError):
        if exc.code is None:
            return str(exc)
        return f"{exc} (code={exc.code})"
    return str(exc)
