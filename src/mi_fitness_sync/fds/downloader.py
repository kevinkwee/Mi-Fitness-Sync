from __future__ import annotations

import logging
from typing import Any, Callable, TypeVar

import requests

from mi_fitness_sync.fds.cache import FdsCache
from mi_fitness_sync.fds.common import decrypt_fds_data


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

    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException:
        logger.warning("Failed to download %s from %s", download_label, url, exc_info=True)
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
