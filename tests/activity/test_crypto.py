from __future__ import annotations

import base64
import hashlib
import json

import pytest
from Crypto.Cipher import ARC4

from mi_fitness_sync.activity.crypto import (
    Rc4Cipher,
    build_signature,
    decrypt_response_payload,
    encrypt_query_params,
    signed_nonce,
)
from mi_fitness_sync.exceptions import XiaomiApiError

SSECRET = base64.b64encode(b"s" * 32).decode("ascii")
NONCE = base64.b64encode(b"n" * 12).decode("ascii")


def _response_body(payload: object) -> str:
    """Build a body that ``decrypt_response_payload`` can decrypt for SSECRET and NONCE."""
    key = hashlib.sha256(base64.b64decode(SSECRET) + base64.b64decode(NONCE)).digest()
    plaintext = json.dumps(payload).encode("utf-8")
    return base64.b64encode(Rc4Cipher(key).apply(plaintext)).decode("ascii")


class TestRc4Cipher:
    def test_rejects_a_key_of_the_wrong_length(self) -> None:
        with pytest.raises(ValueError, match="32 bytes"):
            Rc4Cipher(b"short")

    def test_matches_the_reference_stream_cipher(self) -> None:
        key = bytes(range(32))
        payload = b"the quick brown fox jumps over the lazy dog"

        expected = ARC4.new(key, drop=1024).encrypt(payload)

        assert Rc4Cipher(key).apply(payload) == expected


class TestBuildSignature:
    def test_signature_is_independent_of_parameter_order(self) -> None:
        first = build_signature("GET", "/api/x", {"b": "2", "a": "1"}, "nonce")
        second = build_signature("GET", "/api/x", {"a": "1", "b": "2"}, "nonce")

        assert first == second


class TestDecryptResponsePayload:
    def test_returns_the_decrypted_object(self) -> None:
        payload = {"code": 0, "message": "ok"}

        assert decrypt_response_payload(_response_body(payload), NONCE, SSECRET) == payload

    def test_raises_for_a_payload_that_is_not_a_json_object(self) -> None:
        with pytest.raises(XiaomiApiError, match="not a JSON object"):
            decrypt_response_payload(_response_body(["not", "an", "object"]), NONCE, SSECRET)


class TestEncryptQueryParams:
    def test_adds_nonce_and_signature(self) -> None:
        params = encrypt_query_params(
            method="GET",
            path="/api/x",
            params={"data": "{}"},
            nonce=NONCE,
            ssecurity=SSECRET,
        )

        assert params["_nonce"] == NONCE
        assert "signature" in params
        assert "data" in params

    def test_encrypted_parameters_round_trip(self) -> None:
        originals = {"data": "{}", "path": "/api/x"}
        params = encrypt_query_params(
            method="GET",
            path="/api/x",
            params=originals,
            nonce=NONCE,
            ssecurity=SSECRET,
        )

        decrypted = _decrypt_parameters(params, NONCE, SSECRET)

        assert {key: decrypted[key] for key in originals} == originals

    def test_signature_changes_with_the_payload(self) -> None:
        first = encrypt_query_params(
            method="GET", path="/api/x", params={"data": "a"}, nonce=NONCE, ssecurity=SSECRET
        )
        second = encrypt_query_params(
            method="GET", path="/api/x", params={"data": "b"}, nonce=NONCE, ssecurity=SSECRET
        )

        assert first["signature"] != second["signature"]


def _decrypt_parameters(params: dict[str, str], nonce: str, ssecurity: str) -> dict[str, str]:
    """Reverse ``encrypt_query_params`` with the same key stream order."""
    cipher = Rc4Cipher(base64.b64decode(signed_nonce(ssecurity, nonce)))
    keys = sorted(key for key in params if key not in {"signature", "_nonce"})
    return {key: cipher.apply(base64.b64decode(params[key])).decode("utf-8") for key in keys}
