from __future__ import annotations

import random
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime


RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
DEFAULT_MAX_ATTEMPTS = 4
DEFAULT_BASE_DELAY_SECONDS = 1.0
MAX_RETRY_DELAY_SECONDS = 30.0


def is_retryable_status_code(status_code: int) -> bool:
    """Return whether an HTTP status should be retried automatically."""
    return status_code in RETRYABLE_STATUS_CODES


def parse_retry_after_seconds(value: str | None) -> float | None:
    """Parse a Retry-After header into seconds when possible."""
    if value is None:
        return None

    stripped = value.strip()
    if not stripped:
        return None

    try:
        seconds = float(stripped)
    except ValueError:
        seconds = None

    if seconds is not None:
        return max(seconds, 0.0)

    try:
        retry_at = parsedate_to_datetime(stripped)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None

    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)

    return max((retry_at - datetime.now(timezone.utc)).total_seconds(), 0.0)


def retry_delay_seconds(
    attempt_index: int,
    retry_after: str | None,
    *,
    base_delay_seconds: float = DEFAULT_BASE_DELAY_SECONDS,
    max_delay_seconds: float = MAX_RETRY_DELAY_SECONDS,
) -> float:
    """Compute a bounded retry delay with jitter."""
    parsed_retry_after = parse_retry_after_seconds(retry_after)
    if parsed_retry_after is not None:
        return min(parsed_retry_after, max_delay_seconds)

    exponential_delay = min(base_delay_seconds * (2 ** attempt_index), max_delay_seconds)
    jitter = random.uniform(0.0, max(exponential_delay * 0.25, 0.1))
    return min(exponential_delay + jitter, max_delay_seconds)
