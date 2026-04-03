from __future__ import annotations

from pathlib import Path

from mi_fitness_sync.auth import store as auth_store


def test_save_and_load_state_round_trip(tmp_path: Path, auth_state):
    state_path = tmp_path / "auth.json"

    saved_path = auth_store.save_state(auth_state, str(state_path))
    loaded_state = auth_store.load_state(str(state_path))

    assert saved_path == state_path.resolve()
    assert loaded_state == auth_state


def test_delete_state_removes_file(tmp_path: Path, auth_state):
    state_path = tmp_path / "auth.json"
    auth_store.save_state(auth_state, str(state_path))

    deleted_path = auth_store.delete_state(str(state_path))

    assert deleted_path == state_path.resolve()
    assert not state_path.exists()
