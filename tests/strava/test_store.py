from __future__ import annotations

import os
import stat
from pathlib import Path

from mi_fitness_sync.strava.store import StravaTokenState, save_tokens, load_tokens, delete_tokens


def _make_token_state() -> StravaTokenState:
    return StravaTokenState(
        client_id="12345",
        client_secret="secret123",
        access_token="access-abc",
        refresh_token="refresh-xyz",
        expires_at=1700000000,
        athlete_id=42,
        created_at="2026-04-01T00:00:00+00:00",
        updated_at="2026-04-01T00:00:00+00:00",
    )


def test_save_and_load_round_trip(tmp_path: Path):
    state = _make_token_state()
    token_path = tmp_path / "tokens.json"

    saved_path = save_tokens(state, str(token_path))
    loaded = load_tokens(str(token_path))

    assert saved_path == token_path.resolve()
    assert loaded == state


def test_load_returns_none_when_missing(tmp_path: Path):
    assert load_tokens(str(tmp_path / "nonexistent.json")) is None


def test_delete_removes_file(tmp_path: Path):
    state = _make_token_state()
    token_path = tmp_path / "tokens.json"
    save_tokens(state, str(token_path))

    deleted_path = delete_tokens(str(token_path))

    assert deleted_path == token_path.resolve()
    assert not token_path.exists()


def test_delete_no_op_when_missing(tmp_path: Path):
    token_path = tmp_path / "nonexistent.json"
    deleted_path = delete_tokens(str(token_path))
    assert deleted_path == token_path.resolve()


def test_save_tokens_sets_restrictive_permissions(tmp_path: Path):
    state = _make_token_state()
    token_path = tmp_path / "tokens.json"

    save_tokens(state, str(token_path))

    if os.name != "nt":
        mode = token_path.stat().st_mode
        assert mode & stat.S_IRUSR
        assert mode & stat.S_IWUSR
        assert not (mode & stat.S_IRGRP)
        assert not (mode & stat.S_IROTH)
