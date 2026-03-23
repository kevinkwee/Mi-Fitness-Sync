from __future__ import annotations

from pathlib import Path

from mi_fitness_sync import storage


def test_save_and_load_state_round_trip(tmp_path: Path, auth_state):
    state_path = tmp_path / "auth.json"

    saved_path = storage.save_state(auth_state, str(state_path))
    loaded_state = storage.load_state(str(state_path))

    assert saved_path == state_path.resolve()
    assert loaded_state == auth_state


def test_resolve_state_path_prefers_explicit_argument_over_env(monkeypatch, tmp_path: Path):
    monkeypatch.setenv(storage.STATE_PATH_ENV_VAR, str(tmp_path / "from-env.json"))

    resolved = storage.resolve_state_path(str(tmp_path / "from-arg.json"))

    assert resolved == (tmp_path / "from-arg.json").resolve()


def test_delete_state_removes_file(tmp_path: Path, auth_state):
    state_path = tmp_path / "auth.json"
    storage.save_state(auth_state, str(state_path))

    deleted_path = storage.delete_state(str(state_path))

    assert deleted_path == state_path.resolve()
    assert not state_path.exists()
