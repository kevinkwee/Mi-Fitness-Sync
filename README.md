# Mi Fitness Sync

An unofficial Python CLI for accessing Mi Fitness workout data and manually syncing activities that failed to reach Strava.

## Why This Exists

Mi Fitness is the Android app used by Xiaomi wearable devices. When a workout is recorded on the watch, the data is synced to the phone over Bluetooth/BLE and then uploaded to the Mi Fitness cloud under the user's Xiaomi account.

Mi Fitness officially supports third-party integrations such as Strava. In practice, workout activities are sometimes not uploaded to Strava even though they appear inside Mi Fitness.

I created this project because I wanted a way to manually sync workouts that were recorded in Mi Fitness but never made it to Strava.

## What This Is Good For

This is mainly useful if you want to:

1. Confirm that your workouts really exist in Mi Fitness cloud storage
2. Inspect recent activities when Mi Fitness fails to push them to Strava
3. Pull activity data yourself instead of waiting for the official sync to work
4. Export workouts to GPX, TCX, or FIT for manual backup or re-upload
5. Upload workouts directly to Strava with duplicate detection

## Install

Python 3.12+ is required.

Install in editable mode:

```bash
python -m pip install -e .
```

Then run with:

```bash
mi-fitness-sync --help
```

Or without installing:

```bash
python main.py --help
```

## Strava API Setup

Before using any Strava commands you need a Strava API application:

1. Go to https://www.strava.com/settings/api and create an application
2. Set the **Authorization Callback Domain** to `localhost` (just the domain — no protocol, no port, no path)

The CLI uses `http://localhost:{port}/callback` as the redirect URI. Strava only matches on the domain portion, so the callback domain setting must be exactly `localhost`.

## Quick Start

```bash
# 1. Log in to your Mi / Xiaomi account (prompts for email and password)
mi-fitness-sync login

# 2. Check auth status
mi-fitness-sync auth-status

# 3. List recent workouts
mi-fitness-sync list-activities --limit 10

# 4. List activities and show which ones are already on Strava
mi-fitness-sync list-activities --limit 10 --strava

# 5. View normalized detail for one activity
mi-fitness-sync activity-detail sid:key:1717200000 --json

# 6. Export a workout
mi-fitness-sync export-activity sid:key:1717200000 --format gpx --output run.gpx

# 7. Authenticate with Strava (prompts for client ID and secret)
mi-fitness-sync strava-login

# 8. Upload a workout to Strava
mi-fitness-sync upload-to-strava sid:key:1717200000
```

All credentials are entered via CLI arguments or interactive prompts — there are no environment variables.

## Commands

Commands that access Mi Fitness accept `--state-path` to override the default auth state file location. Commands that access Strava accept `--strava-token-path` to override the default Strava token file location.

### `login`

Logs into Xiaomi Passport for the Mi Fitness service and saves the auth state locally. Email and password can be passed as arguments or entered interactively when omitted. The password prompt does not echo input.

Examples:

```bash
mi-fitness-sync login
mi-fitness-sync login --email you@example.com --password your-password
```

Flags:

| Flag | Description |
|------|-------------|
| `--email` | Mi / Xiaomi account email (prompted if omitted) |
| `--password` | Mi / Xiaomi account password (prompted securely if omitted) |
| `--state-path` | Override the persisted auth state file path |

### `logout`

Deletes the saved local Mi Fitness auth state.

```bash
mi-fitness-sync logout
```

Flags:

| Flag | Description |
|------|-------------|
| `--state-path` | Override the persisted auth state file path |

### `auth-status`

Shows the currently saved Mi Fitness auth state.

```bash
mi-fitness-sync auth-status
mi-fitness-sync auth-status --json
```

Flags:

| Flag | Description |
|------|-------------|
| `--json` | Print the full auth state as JSON |
| `--state-path` | Override the persisted auth state file path |

### `list-activities`

Lists workout activities from the Mi Fitness cloud.

```bash
mi-fitness-sync list-activities --limit 10
mi-fitness-sync list-activities --since 2024-01-01 --json
mi-fitness-sync list-activities --since 2026-03-20 --strava
mi-fitness-sync list-activities --since 1717200000 --until 1719800000 --limit 50
```

Flags:

| Flag | Description |
|------|-------------|
| `--since` | Inclusive start time (unix seconds or ISO-8601) |
| `--until` | Inclusive end time (unix seconds or ISO-8601) |
| `--limit` | Maximum activities to return (default: 20) |
| `--category` | Mi Fitness category filter string |
| `--country-code` | Two-letter country override (e.g. `ID`, `GB`, `US`); mapped to the Mi Fitness region automatically |
| `--strava` | Show whether each activity is already uploaded to Strava |
| `--strava-token-path` | Override the Strava token file path (used with `--strava`) |
| `--json` | Print activities as JSON |
| `--verbose` | Enable debug logging |
| `--state-path` | Override the persisted auth state file path |

When `--country-code` is omitted, the CLI uses automatic Mi Fitness region detection.

### `activity-detail`

Fetches the normalized detail payload for one activity.

```bash
mi-fitness-sync activity-detail sid:key:1717200000
mi-fitness-sync activity-detail sid:key:1717200000 --country-code ID --json
```

Flags:

| Flag | Description |
|------|-------------|
| `activity_id` | Activity ID from `list-activities` (positional, required) |
| `--country-code` | Two-letter country override |
| `--json` | Print normalized detail as JSON |
| `--no-cache` | Disable the local FDS binary cache |
| `--cache-dir` | Override the local FDS cache directory |
| `--verbose` | Enable debug logging |
| `--state-path` | Override the persisted auth state file path |

Detail lookup behavior:

1. Prefers the richer workout JSON payload when Mi Fitness exposes it
2. Checks the Mi Fitness cloud for downloadable binary workout data (FDS files containing GPS tracks, heart rate, etc.)
3. Can synthesize detail from FDS sport samples or GPS track points when the JSON payload is missing
4. Attaches recoverable FDS sport-report and recovery-rate data (not consumed by exporters)
5. Fails if neither the JSON detail nor FDS binary data is available

### `export-activity`

Exports one activity to GPX, TCX, or FIT.

```bash
mi-fitness-sync export-activity sid:key:1717200000 --format gpx --output run.gpx
mi-fitness-sync export-activity sid:key:1717200000 --format tcx --output run.tcx.gz --gzip
mi-fitness-sync export-activity sid:key:1717200000 --format fit
```

Flags:

| Flag | Description |
|------|-------------|
| `activity_id` | Activity ID from `list-activities` (positional, required) |
| `--format` | Export format: `gpx`, `tcx`, or `fit` (required) |
| `--output` | Destination file path (default: `~/.mi_fitness_sync/exports/<title>_<time>.<format>`) |
| `--gzip` | Gzip-compress the output |
| `--country-code` | Two-letter country override |
| `--no-cache` | Disable the local FDS binary cache |
| `--cache-dir` | Override the local FDS cache directory |
| `--verbose` | Enable debug logging |
| `--state-path` | Override the persisted auth state file path |

Export behavior:

1. GPX requires GPS track points in the detail payload
2. TCX and FIT prefer track points but fall back to timestamped sport samples without coordinates
3. FIT generation is best-effort — only fields present in the Mi Fitness source are included

### `strava-login`

Authenticates with Strava using the OAuth2 authorization code flow. Opens a browser for authorization, then stores the tokens locally. Client ID and secret can be passed as arguments or entered interactively.

```bash
mi-fitness-sync strava-login
mi-fitness-sync strava-login --client-id YOUR_ID --client-secret YOUR_SECRET
```

Flags:

| Flag | Description |
|------|-------------|
| `--client-id` | Strava API client ID (prompted if omitted) |
| `--client-secret` | Strava API client secret (prompted if omitted) |
| `--port` | Local port for the OAuth callback server (default: 5478) |
| `--strava-token-path` | Override the Strava token file path |

The flow:

1. Starts a temporary local HTTP server on `localhost:{port}`
2. Opens the Strava authorization page in your browser
3. After authorization, Strava redirects to the local server with an auth code
4. Exchanges the code for access and refresh tokens
5. Saves tokens to `~/.mi_fitness_sync/strava/tokens.json`

Access tokens expire after six hours. The CLI refreshes them automatically using the stored refresh token.

### `strava-status`

Shows the currently saved Strava auth state.

```bash
mi-fitness-sync strava-status
```

Flags:

| Flag | Description |
|------|-------------|
| `--strava-token-path` | Override the Strava token file path |

### `strava-logout`

Revokes the Strava access token via the Strava API and deletes the local token file. Exits cleanly if no tokens exist. Deletes local tokens even if remote revocation fails.

```bash
mi-fitness-sync strava-logout
```

Flags:

| Flag | Description |
|------|-------------|
| `--strava-token-path` | Override the Strava token file path |

### `upload-to-strava`

Uploads one Mi Fitness activity to Strava as a FIT file. The FIT file is also saved locally.

```bash
mi-fitness-sync upload-to-strava sid:key:1717200000
mi-fitness-sync upload-to-strava sid:key:1717200000 --output run.fit
mi-fitness-sync upload-to-strava sid:key:1717200000 --skip-duplicate-check
```

Flags:

| Flag | Description |
|------|-------------|
| `activity_id` | Activity ID from `list-activities` (positional, required) |
| `--output` | Local FIT file save path (default: `~/.mi_fitness_sync/exports/<title>_<time>.fit`) |
| `--country-code` | Two-letter country override |
| `--skip-duplicate-check` | Skip checking Strava for existing activities with a similar start time |
| `--strava-token-path` | Override the Strava token file path |
| `--no-cache` | Disable the local FDS binary cache |
| `--cache-dir` | Override the local FDS cache directory |
| `--verbose` | Enable debug logging |
| `--state-path` | Override the persisted Mi Fitness auth state file path |

Upload behavior:

1. Fetches the activity detail from Mi Fitness
2. Renders to FIT format and saves the file locally
3. Checks Strava for existing activities within ±5 minutes of the start time (unless `--skip-duplicate-check` is set)
4. If potential duplicates are found, prompts for confirmation before uploading
5. Uploads the FIT file to Strava and polls until processing completes
6. Prints the Strava activity URL on success
7. The sport type is mapped from Mi Fitness to the closest Strava equivalent; unmapped types let Strava auto-detect

## Project Layout

```
src/mi_fitness_sync/
  activity/    Activity listing, detail, region routing, normalization
  auth/        Xiaomi Passport auth and persistence
  cli/         Command-line entry point
  export/      GPX, TCX, and FIT rendering
  fds/         Mi Fitness FDS binary parsing and caching
  strava/      Strava OAuth2 auth and upload
tests/         Per-module test files mirroring the package structure
```

Example:

```bash
python -m mi_fitness_sync logout
```

## Local Auth Storage

By default, Mi Fitness auth state is stored in the user profile under `~/.mi_fitness_sync/auth/auth.json`.

You can override that location with `--state-path`.

Strava tokens are stored under `~/.mi_fitness_sync/strava/tokens.json`. You can override that location with `--strava-token-path`.

Both files contain sensitive credentials and should not be shared or committed to version control.

## Limitations

1. This is an unofficial project and is not affiliated with Xiaomi, Mi Fitness, or Strava.
2. Parts of the Xiaomi login flow had to be pieced together from app behavior and decompiled code.
3. Xiaomi may change endpoints, cookies, signatures, or response formats at any time.
4. Some accounts may require captcha, notification approval, or step-2 verification flows that are not fully automated here.
5. Detail retrieval currently depends on the raw `huami_sport_record` payload shape used by the Android app; Xiaomi may change that format without notice.
6. FDS download-url metadata does not guarantee the corresponding cloud object is still downloadable, so some workouts still cannot be enriched from FDS.
7. GPX export is unavailable for workouts where Mi Fitness does not expose GPS track points.
8. TCX and FIT require timestamped export points. The exporter uses GPS track points directly when they are present in the normalized detail and otherwise falls back to timestamped sport samples; if neither source exists, those exports are unavailable.
9. FIT export is best-effort and may omit fields when Mi Fitness does not provide enough source data for a fuller activity file.

## Security Notes

1. The CLI currently accepts account credentials directly on the command line.
2. Shell history may persist those values depending on your environment.
3. The auth state file contains sensitive session data and should be protected.

If you use this on a shared machine, treat the local auth state like a credential.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
