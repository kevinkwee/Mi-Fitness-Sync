from __future__ import annotations

from mi_fitness_sync.fds.downloader import _get_fds_response_body


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
