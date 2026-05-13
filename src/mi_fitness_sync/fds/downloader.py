from __future__ import annotations

import logging
import time
from typing import Any, Callable, TypeVar

import requests

from mi_fitness_sync.fds.cache import FdsCache
from mi_fitness_sync.fds.common import decrypt_fds_data
from mi_fitness_sync.http_retry import DEFAULT_MAX_ATTEMPTS, is_retryable_status_code, retry_delay_seconds


logger = logging.getLogger(__name__)

ParserOutput = TypeVar("ParserOutput")


def _get_fds_response_body(response: requests.Response) -> str:
    try:
        return response.json()
    except Exception:
        logger.debug("Response is not JSON, using raw text for FDS body", exc_info=True)
        return response.text


def download_and_parse_fds_file(
    session: requests.Session,
    fds_entry: dict[str, Any],
    parse_decrypted: Callable[[bytes], ParserOutput],
    fallback: Callable[[], ParserOutput],
    *,
    timeout: int,
    cache: FdsCache | None,
    cache_key: str | None,
    entry_label: str,
    download_label: str,
    decrypt_label: str,
    parse_label: str,
) -> ParserOutput:
    if cache is not None and cache_key is not None:
        cached = cache.get(cache_key)
        if cached is not None:
            try:
                return parse_decrypted(cached)
            except Exception:
                logger.warning("Failed to parse cached %s", entry_label, exc_info=True)
                return fallback()

    url = fds_entry.get("url")
    object_key = fds_entry.get("obj_key")
    if not isinstance(url, str) or not isinstance(object_key, str):
        logger.debug("FDS %s entry missing url or obj_key — raw entry: %s", entry_label, fds_entry)
        return fallback()

    response: requests.Response | None = None
    for attempt_index in range(DEFAULT_MAX_ATTEMPTS):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            break
        except requests.HTTPError as exc:
            status_code = getattr(exc.response, "status_code", None)
            headers = getattr(exc.response, "headers", {})
            retry_after = headers.get("Retry-After") if isinstance(headers, dict) else None
            if (
                isinstance(status_code, int)
                and is_retryable_status_code(status_code)
                and attempt_index < DEFAULT_MAX_ATTEMPTS - 1
            ):
                delay = retry_delay_seconds(attempt_index, retry_after)
                logger.warning(
                    "Failed to download %s from %s with HTTP %d; retrying in %.2fs (%d/%d)",
                    download_label,
                    url,
                    status_code,
                    delay,
                    attempt_index + 1,
                    DEFAULT_MAX_ATTEMPTS - 1,
                )
                time.sleep(delay)
                continue

            logger.warning("Failed to download %s from %s", download_label, url, exc_info=True)
            return fallback()
        except requests.RequestException as exc:
            if attempt_index < DEFAULT_MAX_ATTEMPTS - 1:
                delay = retry_delay_seconds(attempt_index, None)
                logger.warning(
                    "Failed to download %s from %s with %s; retrying in %.2fs (%d/%d)",
                    download_label,
                    url,
                    exc,
                    delay,
                    attempt_index + 1,
                    DEFAULT_MAX_ATTEMPTS - 1,
                )
                time.sleep(delay)
                continue

            logger.warning("Failed to download %s from %s", download_label, url, exc_info=True)
            return fallback()

    if response is None:
        return fallback()

    try:
        decrypted = decrypt_fds_data(_get_fds_response_body(response), object_key)
    except Exception:
        logger.warning("Failed to decrypt %s", decrypt_label, exc_info=True)
        return fallback()

    if cache is not None and cache_key is not None:
        cache.put(cache_key, decrypted)

    try:
        return parse_decrypted(decrypted)
    except Exception:
        logger.warning("Failed to parse %s", parse_label, exc_info=True)
        return fallback()
