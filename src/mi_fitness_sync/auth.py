from __future__ import annotations

import base64
import hashlib
import json
import secrets
from dataclasses import dataclass
from typing import Any
from http.cookies import SimpleCookie

import requests

from mi_fitness_sync.exceptions import (
    CaptchaRequiredError,
    NotificationRequiredError,
    Step2RequiredError,
    XiaomiApiError,
)
from mi_fitness_sync.storage import AuthState, utc_now_iso


ACCOUNT_BASE = "https://account.xiaomi.com"
URL_LOGIN = f"{ACCOUNT_BASE}/pass/serviceLogin"
URL_LOGIN_AUTH2 = f"{ACCOUNT_BASE}/pass/serviceLoginAuth2"
DEFAULT_SERVICE_ID = "miothealth"
USER_AGENT = "MiFitnessStravaSyncAuth/0.1"
SAFE_PREFIX = "&&&START&&&"


@dataclass(slots=True)
class MetaLoginData:
    sign: str
    qs: str
    callback: str


@dataclass(slots=True)
class LoginSession:
    email: str
    user_id: str
    c_user_id: str
    service_id: str
    pass_token: str
    service_token: str
    ssecurity: str
    psecurity: str | None
    auto_login_url: str
    device_id: str
    slh: str | None
    ph: str | None
    sts_cookie_header: str
    cookies: list[dict[str, Any]]

    def to_auth_state(self) -> AuthState:
        timestamp = utc_now_iso()
        return AuthState(
            email=self.email,
            user_id=self.user_id,
            c_user_id=self.c_user_id,
            service_id=self.service_id,
            pass_token=self.pass_token,
            service_token=self.service_token,
            ssecurity=self.ssecurity,
            psecurity=self.psecurity,
            auto_login_url=self.auto_login_url,
            device_id=self.device_id,
            slh=self.slh,
            ph=self.ph,
            sts_cookie_header=self.sts_cookie_header,
            cookies=self.cookies,
            created_at=timestamp,
            updated_at=timestamp,
        )


class MiFitnessAuthClient:
    def __init__(self, *, service_id: str = DEFAULT_SERVICE_ID, timeout: int = 30):
        self.service_id = service_id
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "User-Agent": USER_AGENT,
                "X-Requested-With": "XMLHttpRequest",
            }
        )

    def login_with_password(
        self,
        *,
        email: str,
        password: str,
        device_id: str,
        country_code: str | None = None,
    ) -> LoginSession:
        meta = self._get_meta_login_data(email=email, device_id=device_id)
        response = self.session.post(
            URL_LOGIN_AUTH2,
            params={"_json": "true"},
            data=self._build_password_login_form(
                email=email,
                password=password,
                meta=meta,
                country_code=country_code,
            ),
            cookies={"deviceId": device_id},
            timeout=self.timeout,
        )
        response.raise_for_status()

        payload = self._load_json_payload(response.text)
        self._raise_for_login_requirements(payload)
        self._raise_for_login_error(payload)

        user_id = self._pick_first_non_empty(payload.get("userId"), email)
        pass_token = self._pick_first_non_empty(
            payload.get("passToken"),
            self._cookie_value(response.cookies, "passToken"),
            response.headers.get("passToken"),
        )
        c_user_id = self._pick_first_non_empty(
            payload.get("cUserId"),
            self._cookie_value(response.cookies, "cUserId"),
            response.headers.get("cUserId"),
        )
        ssecurity = self._pick_first_non_empty(payload.get("ssecurity"), self._extension_value(response, "ssecurity"))
        nonce = self._pick_first_non_empty(payload.get("nonce"), self._extension_value(response, "nonce"))
        psecurity = self._pick_first_non_empty(payload.get("psecurity"), self._extension_value(response, "psecurity"))
        auto_login_url = payload.get("location")

        if not pass_token:
            raise XiaomiApiError("Login response did not include a passToken.", payload=payload)
        if not c_user_id:
            raise XiaomiApiError("Login response did not include cUserId.", payload=payload)
        if not ssecurity or nonce in (None, ""):
            raise XiaomiApiError("Login response did not include ssecurity or nonce.", payload=payload)
        if not auto_login_url:
            raise XiaomiApiError("Login response did not include an STS location URL.", payload=payload)

        sts_response = self._follow_sts(auto_login_url=auto_login_url, nonce=str(nonce), ssecurity=ssecurity)
        service_token = self._extract_service_token(sts_response)
        slh = self._read_sts_cookie(sts_response, f"{self.service_id}_slh")
        ph = self._read_sts_cookie(sts_response, f"{self.service_id}_ph")

        return LoginSession(
            email=email,
            user_id=user_id,
            c_user_id=c_user_id,
            service_id=self.service_id,
            pass_token=pass_token,
            service_token=service_token,
            ssecurity=ssecurity,
            psecurity=psecurity,
            auto_login_url=auto_login_url,
            device_id=device_id,
            slh=slh,
            ph=ph,
            sts_cookie_header=self._build_cookie_header(sts_response),
            cookies=self._serialize_cookies(),
        )

    def _build_password_login_form(
        self,
        *,
        email: str,
        password: str,
        meta: MetaLoginData,
        country_code: str | None,
    ) -> dict[str, str]:
        form = {
            "user": email,
            "hash": hashlib.md5(password.encode("utf-8")).hexdigest().upper(),
            "sid": self.service_id,
            "_json": "true",
            "_sign": meta.sign,
            "qs": meta.qs,
            "callback": meta.callback,
        }
        if country_code:
            form["cc"] = country_code
            form["countryCode"] = country_code
        return form

    def _get_meta_login_data(self, *, email: str, device_id: str) -> MetaLoginData:
        response = self.session.get(
            URL_LOGIN,
            params={"sid": self.service_id, "_json": "true"},
            cookies={"deviceId": device_id, "userId": email},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = self._load_json_payload(response.text)

        sign = payload.get("_sign")
        qs = payload.get("qs")
        callback = payload.get("callback")
        if sign and qs and callback:
            return MetaLoginData(sign=sign, qs=qs, callback=callback)

        raise XiaomiApiError(
            "Failed to recover Xiaomi Passport meta login data for password login.",
            code=payload.get("code"),
            payload=payload,
        )

    def _follow_sts(self, *, auto_login_url: str, nonce: str, ssecurity: str) -> requests.Response:
        client_sign = self._generate_client_sign(nonce=nonce, ssecurity=ssecurity)
        response = self.session.get(
            auto_login_url,
            params={"clientSign": client_sign, "_userIdNeedEncrypt": "true"},
            timeout=self.timeout,
            allow_redirects=True,
        )
        response.raise_for_status()
        return response

    def _extract_service_token(self, response: requests.Response) -> str:
        primary_name = f"{self.service_id}_serviceToken"
        for name in (primary_name, "serviceToken"):
            value = self._read_sts_cookie(response, name)
            if value:
                return value
        raise XiaomiApiError("STS response did not include a Mi Fitness serviceToken.")

    def _read_sts_cookie(self, response: requests.Response, cookie_name: str) -> str | None:
        for cookie in self.session.cookies:
            if cookie.name == cookie_name:
                return cookie.value
        raw_cookie = response.headers.get("set-cookie")
        if raw_cookie:
            cookie = SimpleCookie()
            cookie.load(raw_cookie)
            if cookie_name in cookie:
                return cookie[cookie_name].value
        return None

    def _build_cookie_header(self, response: requests.Response) -> str:
        cookies: list[str] = []
        for cookie in self.session.cookies:
            domain = cookie.domain or ""
            if not domain or "xiaomi.com" in domain or "mi.com" in domain:
                cookies.append(f"{cookie.name}={cookie.value}")
        if cookies:
            return "; ".join(cookies)
        raw_cookie = response.headers.get("set-cookie", "")
        return raw_cookie

    def _cookie_value(self, cookie_jar: requests.cookies.RequestsCookieJar, name: str) -> str | None:
        matches = [cookie for cookie in cookie_jar if cookie.name == name]
        if not matches:
            return None

        preferred_domains = ("account.xiaomi.com", ".account.xiaomi.com", "xiaomi.com", ".xiaomi.com")
        for domain in preferred_domains:
            for cookie in reversed(matches):
                if cookie.domain == domain:
                    return cookie.value

        return matches[-1].value

    def _serialize_cookies(self) -> list[dict[str, Any]]:
        serialized: list[dict[str, Any]] = []
        for cookie in self.session.cookies:
            serialized.append(
                {
                    "name": cookie.name,
                    "value": cookie.value,
                    "domain": cookie.domain,
                    "path": cookie.path,
                    "secure": cookie.secure,
                    "expires": cookie.expires,
                }
            )
        return serialized

    def _load_json_payload(self, text: str) -> dict[str, Any]:
        raw = text.strip()
        if raw.startswith(SAFE_PREFIX):
            raw = raw[len(SAFE_PREFIX) :]
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise XiaomiApiError(f"Failed to decode Xiaomi response as JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise XiaomiApiError("Unexpected non-object response from Xiaomi Passport.")
        return payload

    def _raise_for_login_requirements(self, payload: dict[str, Any]) -> None:
        captcha_url = self._pick_first_non_empty(payload.get("captchaUrl"), payload.get("info"))
        if captcha_url and "captcha" in captcha_url.lower():
            raise CaptchaRequiredError(captcha_url)

        if payload.get("notificationUrl"):
            notification_url = str(payload["notificationUrl"])
            if notification_url.startswith("/"):
                notification_url = f"{ACCOUNT_BASE}{notification_url}"
            raise NotificationRequiredError(notification_url)

        if all(payload.get(key) for key in ("_sign", "qs", "callback")) and payload.get("code") not in (0, None):
            raise Step2RequiredError(
                "Xiaomi Passport requested a step-2 or interactive verification flow.",
                payload=payload,
            )

    def _raise_for_login_error(self, payload: dict[str, Any]) -> None:
        code = payload.get("code")
        if code in (None, 0):
            return

        description = self._pick_first_non_empty(payload.get("desc"), payload.get("description"), payload.get("info"))
        message = description or f"Xiaomi Passport login failed with code {code}."
        raise XiaomiApiError(message, code=code, payload=payload)

    def _extension_value(self, response: requests.Response, key: str) -> Any:
        extension = response.headers.get("Extension-Pragma") or response.headers.get("extension-pragma")
        if not extension:
            return None
        try:
            payload = json.loads(extension)
        except json.JSONDecodeError:
            return None
        return payload.get(key)

    def _generate_client_sign(self, *, nonce: str, ssecurity: str) -> str:
        payload = f"nonce={nonce}&{ssecurity}".encode("utf-8")
        return base64.b64encode(hashlib.sha1(payload).digest()).decode("ascii")

    @staticmethod
    def generate_device_id() -> str:
        return secrets.token_hex(8).upper()

    @staticmethod
    def _pick_first_non_empty(*values: Any) -> Any:
        for value in values:
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            return value
        return None
