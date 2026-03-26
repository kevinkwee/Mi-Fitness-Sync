# Mi Fitness Sync

An unofficial Python CLI for accessing Mi Fitness workout data and manually syncing activities that failed to reach Strava.

## Why This Project Exists

Mi Fitness is the Android app used by Xiaomi wearable devices. When a workout is recorded on the watch, the data is synced to the phone over Bluetooth/BLE and then uploaded to the Mi Fitness cloud under the user's Xiaomi account.

Mi Fitness officially supports third-party integrations such as Strava. In practice, workout activities are sometimes not uploaded to Strava even though they appear inside Mi Fitness.

I created this project because I wanted a way to manually sync workouts that were recorded in Mi Fitness but never made it to Strava.

## What This Is Good For

This is mainly useful if you want to:

1. Confirm that your workouts really exist in Mi Fitness cloud storage
2. Inspect recent activities when Mi Fitness fails to push them to Strava
3. Pull activity data yourself instead of waiting for the official sync to work
4. Export workouts to GPX, TCX, or FIT for manual backup and re-upload workflows

## Project Layout

The codebase is set up with:

1. A `src/` layout
2. `pyproject.toml` for project metadata
3. A Python package named `mi_fitness_sync`

Most of the actual code lives under `src/mi_fitness_sync`.

## Install

Python 3.12+ is recommended.

Install it in editable mode:

```bash
python -m pip install -e .
```

Then run it with:

```bash
python -m mi_fitness_sync --help
```

If you do not want to install it yet, there is also a simple wrapper at the repo root:

```bash
python main.py --help
```

## Quick Start

1. Authenticate with your Xiaomi / Mi account
2. Verify the saved auth state
3. List recent workouts from the Mi Fitness cloud
4. Fetch normalized activity detail for one workout
5. Export a workout to GPX, TCX, or FIT

Examples:

```bash
python -m mi_fitness_sync login --email you@example.com --password your-password
python -m mi_fitness_sync auth-status
python -m mi_fitness_sync list-activities --limit 10
python -m mi_fitness_sync activity-detail sid:key:1717200000 --json
python -m mi_fitness_sync export-activity sid:key:1717200000 --format gpx --output exports/run.gpx
```

If you want to use the wrapper instead:

```bash
python main.py login --email you@example.com --password your-password
python main.py auth-status
python main.py list-activities --limit 10
python main.py activity-detail sid:key:1717200000 --json
python main.py export-activity sid:key:1717200000 --format fit --output exports/run.fit
```

## Commands

### `login`

Logs into Xiaomi Passport for the Mi Fitness service and saves the auth state locally.

Example:

```bash
python -m mi_fitness_sync login --email you@example.com --password your-password
```

Relevant flags:

1. `--state-path` to override the default auth state file path

### `auth-status`

Shows the currently saved Mi Fitness auth state.

Examples:

```bash
python -m mi_fitness_sync auth-status
python -m mi_fitness_sync auth-status --json
```

### `list-activities`

Lists workout activities from the Mi Fitness cloud using the saved auth state.

Examples:

```bash
python -m mi_fitness_sync list-activities --limit 10
python -m mi_fitness_sync list-activities --since 2024-01-01 --json
python -m mi_fitness_sync list-activities --since 1717200000 --until 1719800000 --limit 50
python -m mi_fitness_sync list-activities --since 2026-03-20 --country-code ID
```

Relevant flags:

1. `--since` and `--until` accept unix seconds or ISO-8601 timestamps
2. `--limit` controls how many activities are returned
3. `--category` passes a Mi Fitness category filter if you already know the category string
4. `--country-code` overrides activity routing with a two-letter country code such as `ID`, `GB`, or `US`; the CLI maps that to the Mi Fitness region used by the Android app
5. `--json` prints the parsed activity list as JSON

If `--country-code` is omitted, the CLI keeps the existing automatic Mi Fitness region detection behavior.

### `activity-detail`

Fetches the richer normalized detail payload for one activity ID returned by `list-activities`.

Examples:

```bash
python -m mi_fitness_sync activity-detail sid:key:1717200000
python -m mi_fitness_sync activity-detail sid:key:1717200000 --country-code ID --json
```

Relevant flags:

1. `activity_id` must come from the `activity_id` field emitted by `list-activities`
2. `--country-code` uses the same region override behavior as `list-activities`
3. `--json` prints the normalized detail model including parsed track points and samples

Detail lookup behavior:

1. The CLI still prefers the richer workout JSON payload when Mi Fitness exposes it.
2. It now also probes the validated `healthapp/service/gen_download_url` control-plane path and includes any discovered FDS metadata in the raw detail output.
3. If Mi Fitness does not return the richer JSON blob, the CLI falls back to timestamped fitness timeline samples so TCX and FIT export can still work on a best-effort basis.

### `export-activity`

Exports one activity to a standard file format using the normalized detail payload.

Examples:

```bash
python -m mi_fitness_sync export-activity sid:key:1717200000 --format gpx --output exports/run.gpx
python -m mi_fitness_sync export-activity sid:key:1717200000 --format tcx --output exports/run.tcx.gz --gzip
python -m mi_fitness_sync export-activity sid:key:1717200000 --format fit --output exports/run.fit
```

Relevant flags:

1. `--format` must be one of `gpx`, `tcx`, or `fit`
2. `--output` is the destination file path
3. `--gzip` wraps the generated payload with gzip compression before writing it
4. `--country-code` uses the same region override behavior as `list-activities`

Export behavior:

1. GPX requires GPS track points in the Mi Fitness detail payload
2. TCX and FIT can fall back to timestamped sport samples when full GPS is missing
3. FIT generation is best-effort and only includes fields that are present in the Mi Fitness source payload

### `logout`

Deletes the saved local auth state.

Example:

```bash
python -m mi_fitness_sync logout
```

## Local Auth Storage

By default, auth state is stored in the user profile under `.mi-fitness-strava-sync/auth.json`.

You can override that location with:

1. `--state-path`
2. the `MI_FITNESS_AUTH_PATH` environment variable

## Limitations

1. This is an unofficial project and is not affiliated with Xiaomi, Mi Fitness, or Strava.
2. Parts of the Xiaomi login flow had to be pieced together from app behavior and decompiled code.
3. Xiaomi may change endpoints, cookies, signatures, or response formats at any time.
4. Some accounts may require captcha, notification approval, or step-2 verification flows that are not fully automated here.
5. Detail retrieval currently depends on the raw `huami_sport_record` payload shape used by the Android app; Xiaomi may change that format without notice.
6. Some accounts appear to expose FDS download-url metadata for workouts without actually retaining the corresponding cloud object, so live GPS/blob retrieval is still incomplete.
7. GPX export is unavailable for workouts where Mi Fitness does not expose GPS track points.
8. TCX and FIT can fall back to timestamped timeline samples, but that fallback does not include route geometry.
9. FIT export is best-effort and may omit fields when Mi Fitness does not provide enough source data for a fuller activity file.

## Security Notes

1. The CLI currently accepts account credentials directly on the command line.
2. Shell history may persist those values depending on your environment.
3. The auth state file contains sensitive session data and should be protected.

If you use this on a shared machine, treat the local auth state like a credential.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
