from __future__ import annotations

import logging
import struct
from dataclasses import dataclass
from typing import Any

import requests

from mi_fitness_sync.fds.cache import FdsCache
from mi_fitness_sync.fds.common import SPORT_SERVER_DATA_ID_LEN, parse_fds_header
from mi_fitness_sync.fds.downloader import download_and_parse_fds_file


logger = logging.getLogger(__name__)

_RECOVERY_RATE_RECORD_VALIDITY: dict[int, int] = {1: 1}


def get_recovery_rate_data_valid_len(version: int) -> int | None:
    return _RECOVERY_RATE_RECORD_VALIDITY.get(version)


@dataclass(slots=True)
class RecoveryRateSample:
    rate: int


@dataclass(slots=True)
class RecoveryRateData:
    recover_timestamp: int
    heart_rate: int
    recover_rate: float
    rate_samples: list[RecoveryRateSample]
    start_rate: int | None = None
    end_rate: int | None = None


def parse_recovery_rate_record(decrypted: bytes) -> RecoveryRateData | None:
    if len(decrypted) < SPORT_SERVER_DATA_ID_LEN + 1:
        logger.warning("Recovery rate data too short to read header version byte")
        return None

    version = decrypted[5]
    data_valid_len = get_recovery_rate_data_valid_len(version)
    if data_valid_len is None:
        logger.info("No recovery rate dataValid for version=%d; skipping parse", version)
        return None

    header = parse_fds_header(decrypted, data_valid_len)
    body = header.body_data
    if len(body) < 8:
        logger.warning("Recovery rate body too short: %d bytes", len(body))
        return None

    rate_count = struct.unpack_from("<H", body, 0)[0]
    recover_timestamp = struct.unpack_from("<I", body, 2)[0]
    heart_rate = body[6]
    recover_rate = body[7] / 10.0

    offset = 8
    samples: list[RecoveryRateSample] = []
    for _ in range(rate_count):
        if offset >= len(body):
            break
        samples.append(RecoveryRateSample(rate=body[offset]))
        offset += 1

    result = RecoveryRateData(
        recover_timestamp=recover_timestamp,
        heart_rate=heart_rate,
        recover_rate=recover_rate,
        rate_samples=samples,
    )
    if samples:
        result.start_rate = samples[0].rate
        result.end_rate = samples[-1].rate
    return result


def download_and_parse_recovery_rate(
    session: requests.Session,
    fds_entry: dict[str, Any],
    *,
    timeout: int = 30,
    cache: FdsCache | None = None,
    cache_key: str | None = None,
) -> RecoveryRateData | None:
    return download_and_parse_fds_file(
        session,
        fds_entry,
        parse_recovery_rate_record,
        lambda: None,
        timeout=timeout,
        cache=cache,
        cache_key=cache_key,
        entry_label="recovery rate",
        download_label="FDS recovery rate",
        decrypt_label="FDS recovery rate",
        parse_label="FDS recovery rate binary",
    )