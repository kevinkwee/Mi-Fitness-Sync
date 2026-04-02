from __future__ import annotations

from mi_fitness_sync.fds.cache import FdsCache


def test_cache_miss_returns_none(tmp_path):
    cache = FdsCache(tmp_path / "cache")
    assert cache.get("nonexistent_key") is None


def test_cache_put_then_hit(tmp_path):
    cache = FdsCache(tmp_path / "cache")
    data = b"\x01\x02\x03\x04"
    cache.put("my_key", data)
    assert cache.get("my_key") == data


def test_cache_creates_directory_on_put(tmp_path):
    cache_dir = tmp_path / "deep" / "nested" / "cache"
    cache = FdsCache(cache_dir)
    cache.put("k", b"\xff")
    assert cache.get("k") == b"\xff"


def test_cache_keeps_keys_independent(tmp_path):
    cache = FdsCache(tmp_path / "cache")
    cache.put("key_a", b"aaa")
    cache.put("key_b", b"bbb")
    assert cache.get("key_a") == b"aaa"
    assert cache.get("key_b") == b"bbb"