# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `login` against Xiaomi Passport, following the Mi Fitness app's flow: a captcha, a step-2 code sent by email or SMS, and email-code identity verification, with each challenge the server raises resolved in turn.
- `logout` and `auth-status` to delete or inspect the saved Mi Fitness auth state.
- `list-activities` with a time window, category filter, country override, and an optional column showing which activities already reached Strava.
- `activity-detail`, merging the Mi Fitness JSON payload with the binary FDS sport record, sport report, GPS track, and recovery rate files.
- `export-activity` and `export-activities` to GPX, TCX, or FIT, with gzip, GPS smoothing in match and full modes, and outlier rejection; bulk runs also write atomically and record a resumable JSONL manifest.
- `strava-login` for the OAuth2 authorization-code flow, `strava-status` and `strava-logout` for the saved Strava token, and automatic access-token refresh before every Strava call.
- `upload-to-strava`, which renders a FIT file, checks Strava for an activity within five minutes of the start time, and polls until processing completes.
- A `py.typed` marker, so type checkers apply the package's annotations.

### Changed

- The package builds with hatchling under uv, with dependencies pinned in `uv.lock` and the test and lint tools in a `dev` dependency group.
- Ruff selects every rule set and the formatter owns layout, with a short list of project-wide ignores and file-scoped exceptions, each given a reason.
- mypy runs in strict mode over the package, the test suite, and the release script, and CI fails on any error.
- The sdist is limited to an allowlist, so local captures and editor state cannot be published.

[Unreleased]: https://github.com/kevinkwee/Mi-Fitness-Sync/commits/main
