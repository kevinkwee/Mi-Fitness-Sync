from __future__ import annotations

from typing import TYPE_CHECKING, Any, Never, cast
from unittest.mock import MagicMock, patch

import pytest
import requests

from mi_fitness_sync.fds.downloader import _get_fds_response_body, download_and_parse_fds_file

if TYPE_CHECKING:
    from collections.abc import Callable

    from mi_fitness_sync.fds.cache import FdsCache

_COMMON_LABELS = {
    "entry_label": "test",
    "download_label": "test download",
    "decrypt_label": "test decrypt",
    "parse_label": "test parse",
}


class TestGetFdsResponseBody:
    def test_unwraps_json_string_response(self) -> None:
        class FakeResponse:
            text = '"encrypted-body"'

            def json(self) -> str:
                return "encrypted-body"

        assert _get_fds_response_body(cast("requests.Response", FakeResponse())) == "encrypted-body"

    def test_falls_back_to_raw_text_when_json_decode_fails(self) -> None:
        class FakeResponse:
            text = "raw-text-body"

            def json(self) -> Never:
                raise ValueError("not json")

        assert _get_fds_response_body(cast("requests.Response", FakeResponse())) == "raw-text-body"

    def test_rejects_json_body_that_is_not_a_string(self) -> None:
        class FakeResponse:
            text = "raw-text-body"

            def json(self) -> dict[str, str]:
                return {"encrypted": "body"}

        with pytest.raises(ValueError, match="not a string payload"):
            _get_fds_response_body(cast("requests.Response", FakeResponse()))


class TestDownloadAndParseFdsFile:
    def _call(
        self,
        session: requests.Session,
        fds_entry: dict[str, Any],
        parse_decrypted: Callable[[bytes], object],
        fallback: Callable[[], object],
        cache: FdsCache | None = None,
        cache_key: str | None = None,
    ) -> object:
        return download_and_parse_fds_file(
            session,
            fds_entry,
            parse_decrypted,
            fallback,
            timeout=10,
            cache=cache,
            cache_key=cache_key,
            **_COMMON_LABELS,
        )

    def test_encrypted_path_with_obj_key(self) -> None:
        raw_bytes = b"decrypted-data"
        response = MagicMock()
        response.json.return_value = "encrypted-body"
        response.raise_for_status = MagicMock()
        session = MagicMock()
        session.get.return_value = response

        with patch(
            "mi_fitness_sync.fds.downloader.decrypt_fds_data", return_value=raw_bytes
        ) as mock_decrypt:
            result = self._call(
                session,
                {"url": "https://cdn.example.com/file", "obj_key": "test-key"},
                parse_decrypted=lambda b: ("parsed", b),
                fallback=lambda: "FALLBACK",
            )

        mock_decrypt.assert_called_once_with("encrypted-body", "test-key")
        assert result == ("parsed", raw_bytes)

    def test_missing_obj_key_returns_fallback(self) -> None:
        result = self._call(
            MagicMock(),
            {"url": "https://cdn.example.com/file"},
            parse_decrypted=lambda b: ("parsed", b),
            fallback=lambda: "FALLBACK",
        )
        assert result == "FALLBACK"

    def test_obj_key_none_returns_fallback(self) -> None:
        result = self._call(
            MagicMock(),
            {"url": "https://cdn.example.com/file", "obj_key": None},
            parse_decrypted=lambda b: ("parsed", b),
            fallback=lambda: "FALLBACK",
        )
        assert result == "FALLBACK"

    def test_obj_key_empty_string_returns_fallback(self) -> None:
        result = self._call(
            MagicMock(),
            {"url": "https://cdn.example.com/file", "obj_key": ""},
            parse_decrypted=lambda b: ("parsed", b),
            fallback=lambda: "FALLBACK",
        )
        assert result == "FALLBACK"

    def test_missing_url_returns_fallback(self) -> None:
        result = self._call(
            MagicMock(),
            {"obj_key": "test-key"},
            parse_decrypted=lambda b: ("parsed", b),
            fallback=lambda: "FALLBACK",
        )
        assert result == "FALLBACK"

    def test_empty_entry_returns_fallback(self) -> None:
        result = self._call(
            MagicMock(),
            {},
            parse_decrypted=lambda b: ("parsed", b),
            fallback=lambda: "FALLBACK",
        )
        assert result == "FALLBACK"

    def test_download_failure_returns_fallback(self) -> None:
        session = MagicMock()
        session.get.side_effect = requests.ConnectionError("connection refused")

        result = self._call(
            session,
            {"url": "https://cdn.example.com/file", "obj_key": "test-key"},
            parse_decrypted=lambda b: ("parsed", b),
            fallback=lambda: "FALLBACK",
        )
        assert result == "FALLBACK"

    def test_http_error_returns_fallback(self) -> None:
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

    def test_decrypt_failure_returns_fallback(self) -> None:
        response = MagicMock()
        response.json.return_value = "encrypted-body"
        response.raise_for_status = MagicMock()
        session = MagicMock()
        session.get.return_value = response

        with patch(
            "mi_fitness_sync.fds.downloader.decrypt_fds_data", side_effect=ValueError("bad key")
        ):
            result = self._call(
                session,
                {"url": "https://cdn.example.com/file", "obj_key": "bad-key"},
                parse_decrypted=lambda b: ("parsed", b),
                fallback=lambda: "FALLBACK",
            )
        assert result == "FALLBACK"

    def test_parse_failure_returns_fallback(self) -> None:
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

    def test_non_string_json_body_returns_fallback(self) -> None:
        response = MagicMock()
        response.json.return_value = {"encrypted": "body"}
        response.raise_for_status = MagicMock()
        session = MagicMock()
        session.get.return_value = response

        result = self._call(
            session,
            {"url": "https://cdn.example.com/file", "obj_key": "test-key"},
            parse_decrypted=lambda value: value,
            fallback=lambda: b"FALLBACK",
        )

        assert result == b"FALLBACK"

    def test_encrypted_path_caches_decrypted_bytes(self) -> None:
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

    def test_retryable_http_error_retries_then_succeeds(self) -> None:
        retry_response = MagicMock()
        retry_response.status_code = 429
        retry_response.headers = {"Retry-After": "0"}
        retry_response.raise_for_status.side_effect = requests.HTTPError(
            "429", response=retry_response
        )

        success_response = MagicMock()
        success_response.json.return_value = "encrypted-body"
        success_response.raise_for_status = MagicMock()

        session = MagicMock()
        session.get.side_effect = [retry_response, success_response]

        with (
            patch(
                "mi_fitness_sync.fds.downloader.decrypt_fds_data", return_value=b"decrypted-data"
            ),
            patch("mi_fitness_sync.fds.downloader.time.sleep") as sleep_mock,
        ):
            result = self._call(
                session,
                {"url": "https://cdn.example.com/file", "obj_key": "test-key"},
                parse_decrypted=lambda value: value,
                fallback=lambda: b"FALLBACK",
            )

        assert result == b"decrypted-data"
        assert session.get.call_count == 2
        sleep_mock.assert_called_once()
