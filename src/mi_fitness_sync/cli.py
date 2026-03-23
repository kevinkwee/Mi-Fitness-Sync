from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, replace

from mi_fitness_sync.activities import MiFitnessActivitiesClient, parse_cli_time, render_activities_table
from mi_fitness_sync.auth import DEFAULT_SERVICE_ID, MiFitnessAuthClient
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
    login_parser.add_argument("--country-code", help="Optional country code hint, for example 44")
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
    activities_parser.add_argument("--region", help="Optional Mi Fitness region override such as sg, de, us, or cn")
    activities_parser.add_argument("--json", action="store_true", help="Print activities as JSON")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "login":
            return handle_login(args)
        if args.command == "logout":
            return handle_logout(args)
        if args.command == "auth-status":
            return handle_auth_status(args)
        if args.command == "list-activities":
            return handle_list_activities(args)
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
        country_code=args.country_code,
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
    state = load_state(args.state_path)
    if state is None:
        raise AuthStateNotFoundError("No persisted Mi Fitness auth state was found.")

    if args.limit <= 0:
        raise MiFitnessError("--limit must be greater than zero.")

    end_time = parse_cli_time(args.until) if args.until else None
    start_time = parse_cli_time(args.since) if args.since else None
    if start_time is not None and end_time is not None and start_time > end_time:
        raise MiFitnessError("--since must be earlier than or equal to --until.")

    client = MiFitnessActivitiesClient(state, region=args.region)
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
