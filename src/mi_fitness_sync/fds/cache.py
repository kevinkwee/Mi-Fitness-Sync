from __future__ import annotations

import logging
from pathlib import Path

from mi_fitness_sync.paths import get_cache_dir


logger = logging.getLogger(__name__)

DEFAULT_CACHE_DIR = get_cache_dir() / "fds"


class FdsCache:
    def __init__(self, cache_dir: Path | str) -> None:
        self._dir = Path(cache_dir)

    def _path_for(self, cache_key: str) -> Path:
        safe_key = cache_key.replace("/", "_").replace("\\", "_")
        return self._dir / f"{safe_key}.bin"

    def get(self, cache_key: str) -> bytes | None:
        path = self._path_for(cache_key)
        if path.is_file():
            logger.debug("FDS cache hit: %s", path)
            return path.read_bytes()
        logger.debug("FDS cache miss: %s", path)
        return None

    def put(self, cache_key: str, data: bytes) -> None:
        path = self._path_for(cache_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        logger.debug("FDS cache write: %s (%d bytes)", path, len(data))
