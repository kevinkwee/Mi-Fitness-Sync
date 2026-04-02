from __future__ import annotations

import base64
import hashlib
import json
import os
import struct
import time
from urllib.parse import urlparse


def b64decode(value: str) -> bytes:
    return base64.b64decode(value)


def b64encode(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def signed_nonce(ssecurity: str, nonce: str) -> str:
    digest = hashlib.sha256(b64decode(ssecurity) + b64decode(nonce)).digest()
    return b64encode(digest)


def sha1_signature(parts: list[str]) -> str:
    return b64encode(hashlib.sha1("&".join(parts).encode("utf-8")).digest())


class Rc4Cipher:
    def __init__(self, key: bytes):
        if len(key) != 32:
            raise ValueError("RC4 key must be 32 bytes.")
        self._state = list(range(256))
        j = 0
        for index in range(256):
            j = (j + self._state[index] + key[index % len(key)]) & 0xFF
            self._state[index], self._state[j] = self._state[j], self._state[index]
        self._i = 0
        self._j = 0
        self.apply(bytes(1024))

    def apply(self, data: bytes) -> bytes:
        output = bytearray(data)
        for index, value in enumerate(output):
            self._i = (self._i + 1) & 0xFF
            state_i = self._state[self._i]
            self._j = (self._j + state_i) & 0xFF
            self._state[self._i], self._state[self._j] = self._state[self._j], self._state[self._i]
            output[index] = value ^ self._state[(self._state[self._i] + state_i) & 0xFF]
        return bytes(output)


def build_signature(method: str, path: str, params: dict[str, str], signed_nonce_value: str) -> str:
    uri_path = urlparse(path).path
    parts = [method.upper(), uri_path]
    for key in sorted(params):
        parts.append(f"{key}={params[key]}")
    parts.append(signed_nonce_value)
    return sha1_signature(parts)


def generate_nonce(time_diff_ms: int) -> str:
    payload = os.urandom(8)
    payload += struct.pack(">i", int((time.time() * 1000 + time_diff_ms) // 60000))
    return b64encode(payload)


def decrypt_response_payload(body: str, nonce: str, ssecurity: str) -> dict[str, object]:
    rc4_key = b64decode(signed_nonce(ssecurity, nonce))
    decrypted = Rc4Cipher(rc4_key).apply(b64decode(body)).decode("utf-8")
    payload = json.loads(decrypted)
    if not isinstance(payload, dict):
        raise ValueError("Mi Fitness response payload was not a JSON object.")
    return payload


def encrypt_query_params(
    *,
    method: str,
    path: str,
    params: dict[str, str],
    nonce: str,
    ssecurity: str,
    signature_path: str | None = None,
) -> dict[str, str]:
    signed_nonce_value = signed_nonce(ssecurity, nonce)
    signature_input = {key: value for key, value in params.items() if key and value}
    signature_input["rc4_hash__"] = build_signature(
        method,
        signature_path or path,
        signature_input,
        signed_nonce_value,
    )

    cipher = Rc4Cipher(b64decode(signed_nonce_value))
    encrypted_params = {
        key: b64encode(cipher.apply(value.encode("utf-8")))
        for key, value in sorted(signature_input.items())
    }
    encrypted_params["signature"] = build_signature(
        method,
        signature_path or path,
        encrypted_params,
        signed_nonce_value,
    )
    encrypted_params["_nonce"] = nonce
    return encrypted_params
