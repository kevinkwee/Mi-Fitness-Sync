from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from mi_fitness_sync.fds.downloader import _get_fds_response_body, download_and_parse_fds_file


_COMMON_LABELS = dict(
    entry_label="test",
    download_label="test download",
    decrypt_label="test decrypt",
    parse_label="test parse",
)


class TestGetFdsResponseBody:
    def test_unwraps_json_string_response(self):
        class FakeResponse:
            text = '"encrypted-body"'

            def json(self):
                return "encrypted-body"

        assert _get_fds_response_body(FakeResponse()) == "encrypted-body"

    def test_falls_back_to_raw_text_when_json_decode_fails(self):
        class FakeResponse:
            text = "raw-text-body"

            def json(self):
                raise ValueError("not json")

        assert _get_fds_response_body(FakeResponse()) == "raw-text-body"


class TestDownloadAndParseFdsFile:
    def _call(self, session, fds_entry, parse_decrypted, fallback, **kwargs):
        merged = {**_COMMON_LABELS, "timeout": 10, "cache": None, "cache_key": None}
        merged.update(kwargs)
        return download_and_parse_fds_file(session, fds_entry, parse_decrypted, fallback, **merged)

    def test_encrypted_path_with_obj_key(self):
        raw_bytes = b"decrypted-data"
        response = MagicMock()
        response.json.return_value = "encrypted-body"
        response.raise_for_status = MagicMock()
        session = MagicMock()
        session.get.return_value = response

        with patch("mi_fitness_sync.fds.downloader.decrypt_fds_data", return_value=raw_bytes) as mock_decrypt:
            result = self._call(
                session,
                {"url": "https://cdn.example.com/file", "obj_key": "test-key"},
                parse_decrypted=lambda b: ("parsed", b),
                fallback=lambda: "FALLBACK",
            )

        mock_decrypt.assert_called_once_with("encrypted-body", "test-key")
        assert result == ("parsed", raw_bytes)

    def test_missing_obj_key_returns_fallback(self):
        result = self._call(
            MagicMock(),
            {"url": "https://cdn.example.com/file"},
            parse_decrypted=lambda b: ("parsed", b),
            fallback=lambda: "FALLBACK",
        )
        assert result == "FALLBACK"

    def test_obj_key_none_returns_fallback(self):
        result = self._call(
            MagicMock(),
            {"url": "https://cdn.example.com/file", "obj_key": None},
            parse_decrypted=lambda b: ("parsed", b),
            fallback=lambda: "FALLBACK",
        )
        assert result == "FALLBACK"

    def test_obj_key_empty_string_returns_fallback(self):
        result = self._call(
            MagicMock(),
            {"url": "https://cdn.example.com/file", "obj_key": ""},
            parse_decrypted=lambda b: ("parsed", b),
            fallback=lambda: "FALLBACK",
        )
        assert result == "FALLBACK"

    def test_missing_url_returns_fallback(self):
        result = self._call(
            MagicMock(),
            {"obj_key": "test-key"},
            parse_decrypted=lambda b: ("parsed", b),
            fallback=lambda: "FALLBACK",
        )
        assert result == "FALLBACK"

    def test_empty_entry_returns_fallback(self):
        result = self._call(
            MagicMock(),
            {},
            parse_decrypted=lambda b: ("parsed", b),
            fallback=lambda: "FALLBACK",
        )
        assert result == "FALLBACK"

    def test_download_failure_returns_fallback(self):
        session = MagicMock()
        session.get.side_effect = requests.ConnectionError("connection refused")

        result = self._call(
            session,
            {"url": "https://cdn.example.com/file", "obj_key": "test-key"},
            parse_decrypted=lambda b: ("parsed", b),
            fallback=lambda: "FALLBACK",
        )
        assert result == "FALLBACK"

    def test_http_error_returns_fallback(self):
        response = MagicMock()
        response.raise_for_status.side_effect = requests.HTTPError("404")
        session = MagicMock()
        session.get.return_value = response

        result = self._call(
            session,
            {"url": "https://cdn.example.com/file", "obj_key": "test-key"},
            parse_decrypted=lambda b: ("parsed", b),
            fallback=lambda: "FALLBACK",
        )
        assert result == "FALLBACK"

    def test_decrypt_failure_returns_fallback(self):
        response = MagicMock()
        response.json.return_value = "encrypted-body"
        response.raise_for_status = MagicMock()
        session = MagicMock()
        session.get.return_value = response

        with patch("mi_fitness_sync.fds.downloader.decrypt_fds_data", side_effect=ValueError("bad key")):
            result = self._call(
                session,
                {"url": "https://cdn.example.com/file", "obj_key": "bad-key"},
                parse_decrypted=lambda b: ("parsed", b),
                fallback=lambda: "FALLBACK",
            )
        assert result == "FALLBACK"

    def test_parse_failure_returns_fallback(self):
        raw_bytes = b"decrypted-data"
        response = MagicMock()
        response.json.return_value = "encrypted-body"
        response.raise_for_status = MagicMock()
        session = MagicMock()
        session.get.return_value = response

        with patch("mi_fitness_sync.fds.downloader.decrypt_fds_data", return_value=raw_bytes):
            result = self._call(
                session,
                {"url": "https://cdn.example.com/file", "obj_key": "test-key"},
                parse_decrypted=lambda b: (_ for _ in ()).throw(ValueError("parse error")),
                fallback=lambda: "FALLBACK",
            )
        assert result == "FALLBACK"

    def test_encrypted_path_caches_decrypted_bytes(self):
        raw_bytes = b"decrypted-data-to-cache"
        response = MagicMock()
        response.json.return_value = "encrypted-body"
        response.raise_for_status = MagicMock()
        session = MagicMock()
        session.get.return_value = response

        cache = MagicMock()
        cache.get.return_value = None

        with patch("mi_fitness_sync.fds.downloader.decrypt_fds_data", return_value=raw_bytes):
            self._call(
                session,
                {"url": "https://cdn.example.com/file", "obj_key": "test-key"},
                parse_decrypted=lambda b: b,
                fallback=lambda: "FALLBACK",
                cache=cache,
                cache_key="test-cache-key",
            )

        cache.put.assert_called_once_with("test-cache-key", raw_bytes)
