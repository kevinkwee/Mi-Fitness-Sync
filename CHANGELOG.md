# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-11-20

### Added

- `login` against Xiaomi Passport, following the Mi Fitness app's flow: a captcha, a step-2 code sent by email or SMS, and email-code identity verification. The CLI resolves each challenge the server raises in turn.
- `logout` and `auth-status` to delete or inspect the saved Mi Fitness auth state.
- `list-activities` with a time window, category filter, country override, and an optional column showing which activities already reached Strava.
- `activity-detail`, merging the Mi Fitness JSON payload with the binary FDS files: sport record, sport report, GPS track, and recovery rate.
- `export-activity` and `export-activities` to GPX, TCX, or FIT, with gzip and GPS smoothing in match and full modes, including outlier correction. Bulk runs also write atomically and record a resumable JSONL manifest.
- `strava-login` for the OAuth2 authorization-code flow, plus `strava-status` and `strava-logout` for the saved Strava token. The CLI refreshes an expiring access token automatically before listing activities with the optional Strava column or uploading an activity.
- `upload-to-strava`, which renders a FIT file, checks Strava for an activity within five minutes of the start time, and polls until Strava finishes processing it, giving up after about a minute.
- A `py.typed` marker, so type checkers see the package's annotations.

[Unreleased]: https://github.com/kevinkwee/Mi-Fitness-Sync/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/kevinkwee/Mi-Fitness-Sync/releases/tag/v0.1.0
