from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from mi_fitness_sync.auth.client import (
    ACCOUNT_BASE,
    MiFitnessAuthClient,
    MetaLoginData,
    SAFE_PREFIX,
    URL_LOGIN_STEP2,
)
from mi_fitness_sync.exceptions import (
    CaptchaRequiredError,
    NotificationRequiredError,
    Step2RequiredError,
    XiaomiApiError,
)


class TestRaiseForLoginRequirements:
    def test_step2_includes_step1_token_from_header(self):
        client = MiFitnessAuthClient()
        payload = {"_sign": "s", "qs": "q", "callback": "c", "code": 81003}
        response = MagicMock()
        response.headers = {"step1Token": "tok123"}
        response.cookies = []

        with pytest.raises(Step2RequiredError) as exc_info:
            client._raise_for_login_requirements(payload, response=response)

        assert exc_info.value.step1_token == "tok123"
        assert exc_info.value.payload["_sign"] == "s"

    def test_step2_step1_token_from_cookie(self):
        client = MiFitnessAuthClient()
        payload = {"_sign": "s", "qs": "q", "callback": "c", "code": 81003}
        response = MagicMock()
        response.headers = {}

        cookie = MagicMock()
        cookie.name = "step1Token"
        cookie.value = "cookie_tok"
        cookie.domain = "account.xiaomi.com"
        response.cookies = [cookie]

        with pytest.raises(Step2RequiredError) as exc_info:
            client._raise_for_login_requirements(payload, response=response)

        assert exc_info.value.step1_token == "cookie_tok"

    def test_step2_no_response_no_step1_token(self):
        client = MiFitnessAuthClient()
        payload = {"_sign": "s", "qs": "q", "callback": "c", "code": 81003}

        with pytest.raises(Step2RequiredError) as exc_info:
            client._raise_for_login_requirements(payload)

        assert exc_info.value.step1_token is None

    def test_step2_step1_token_from_payload(self):
        """step1Token present in JSON body should be found."""
        client = MiFitnessAuthClient()
        payload = {"_sign": "s", "qs": "q", "callback": "c", "code": 81003, "step1Token": "body_tok"}
        response = MagicMock()
        response.headers = {}
        response.cookies = []

        with pytest.raises(Step2RequiredError) as exc_info:
            client._raise_for_login_requirements(payload, response=response)

        assert exc_info.value.step1_token == "body_tok"

    def test_step2_step1_token_from_session_cookie(self):
        """step1Token in session cookies should be found when absent from response headers/cookies."""
        client = MiFitnessAuthClient()
        client.session.cookies.set("step1Token", "session_tok", domain="account.xiaomi.com")
        payload = {"_sign": "s", "qs": "q", "callback": "c", "code": 81003}
        response = MagicMock()
        response.headers = {}
        response.cookies = []

        with pytest.raises(Step2RequiredError) as exc_info:
            client._raise_for_login_requirements(payload, response=response)

        assert exc_info.value.step1_token == "session_tok"

    def test_step2_header_takes_priority_over_payload(self):
        """Response header should take priority over JSON body for step1Token."""
        client = MiFitnessAuthClient()
        payload = {"_sign": "s", "qs": "q", "callback": "c", "code": 81003, "step1Token": "body_tok"}
        response = MagicMock()
        response.headers = {"step1Token": "header_tok"}
        response.cookies = []

        with pytest.raises(Step2RequiredError) as exc_info:
            client._raise_for_login_requirements(payload, response=response)

        assert exc_info.value.step1_token == "header_tok"

    def test_step2_no_response_step1_token_from_payload(self):
        """When no response is provided, step1Token should be extracted from payload."""
        client = MiFitnessAuthClient()
        payload = {"_sign": "s", "qs": "q", "callback": "c", "code": 81003, "step1Token": "payload_tok"}

        with pytest.raises(Step2RequiredError) as exc_info:
            client._raise_for_login_requirements(payload)

        assert exc_info.value.step1_token == "payload_tok"

    def test_captcha_takes_priority(self):
        client = MiFitnessAuthClient()
        payload = {
            "captchaUrl": "https://captcha.example.com",
            "_sign": "s",
            "qs": "q",
            "callback": "c",
            "code": 87001,
        }

        with pytest.raises(CaptchaRequiredError):
            client._raise_for_login_requirements(payload)

    def test_notification_takes_priority(self):
        client = MiFitnessAuthClient()
        payload = {
            "notificationUrl": "/pass/notification?ticket=abc",
            "_sign": "s",
            "qs": "q",
            "callback": "c",
            "code": 12345,
        }

        with pytest.raises(NotificationRequiredError) as exc_info:
            client._raise_for_login_requirements(payload)

        assert exc_info.value.notification_url == f"{ACCOUNT_BASE}/pass/notification?ticket=abc"

    def test_success_payload_does_not_raise(self):
        client = MiFitnessAuthClient()
        payload = {"code": 0, "_sign": "s", "qs": "q", "callback": "c"}

        # Should not raise
        client._raise_for_login_requirements(payload)

    def test_non_step2_error_with_meta_fields_does_not_raise_step2(self):
        """Code 70016 with captcha metadata must fall through as a normal login error."""
        client = MiFitnessAuthClient()
        payload = {
            "_sign": "s",
            "qs": "q",
            "callback": "c",
            "code": 70016,
            "desc": "Wrong password.",
            "captchaUrl": "https://captcha.example.com/foo",
        }

        client._raise_for_login_requirements(payload)

        with pytest.raises(XiaomiApiError, match="Wrong password") as exc_info:
            client._raise_for_login_error(payload)

        assert exc_info.value.code == 70016
        assert exc_info.value.payload["captchaUrl"] == "https://captcha.example.com/foo"

    def test_20031_with_captcha_metadata_falls_through_to_login_error(self):
        client = MiFitnessAuthClient()
        payload = {
            "_sign": "s",
            "qs": "q",
            "callback": "c",
            "code": 20031,
            "desc": "Verification code is required.",
            "captchaUrl": "/pass/getCode?icodeType=register",
        }

        client._raise_for_login_requirements(payload)

        with pytest.raises(XiaomiApiError, match="Verification code is required") as exc_info:
            client._raise_for_login_error(payload)

        assert exc_info.value.code == 20031
        assert exc_info.value.payload["captchaUrl"] == "/pass/getCode?icodeType=register"

    def test_non_step2_error_code_with_meta_fields_passes_through(self):
        """A non-81003 error code with meta fields but no captcha/notification should pass through."""
        client = MiFitnessAuthClient()
        payload = {"_sign": "s", "qs": "q", "callback": "c", "code": 70002}

        # Should not raise — falls through to _raise_for_login_error
        client._raise_for_login_requirements(payload)

    def test_87001_with_step2_meta_raises_captcha(self):
        """Code 87001 with step-2 metadata should still raise CaptchaRequiredError (87001 = captcha)."""
        client = MiFitnessAuthClient()
        payload = {"_sign": "s", "qs": "q", "callback": "c", "code": 87001}
        response = MagicMock()
        response.headers = {"step1Token": "tok87001"}
        response.cookies = []

        with pytest.raises(CaptchaRequiredError) as exc_info:
            client._raise_for_login_requirements(payload, response=response)

        assert exc_info.value.payload == payload

    def test_87001_without_captcha_url_raises_captcha_with_empty_url(self):
        """Code 87001 still represents a captcha challenge even without a returned URL."""
        client = MiFitnessAuthClient()
        payload = {"code": 87001, "info": "验证码输入错误"}

        with pytest.raises(CaptchaRequiredError) as exc_info:
            client._raise_for_login_requirements(payload)

        assert exc_info.value.captcha_url == ""


class TestLoginWithStep2:
    def _make_success_response(self, *, text: str, headers: dict | None = None, cookies=None):
        resp = MagicMock()
        resp.text = text
        resp.headers = headers or {}
        resp.cookies = cookies or []
        resp.raise_for_status = MagicMock()
        return resp

    def test_login_step2_posts_correct_params(self):
        client = MiFitnessAuthClient()

        step2_json = json.dumps({
            "code": 0,
            "userId": "12345",
            "passToken": "pt",
            "cUserId": "cuid",
            "ssecurity": "sec",
            "nonce": "999",
            "psecurity": None,
            "location": "https://example.com/sts?token=abc",
        })
        step2_text = SAFE_PREFIX + step2_json

        sts_resp = self._make_success_response(text="ok", headers={})

        posted_data = {}

        def mock_post(url, data=None, cookies=None, timeout=None):
            posted_data["url"] = url
            posted_data["data"] = data
            posted_data["cookies"] = cookies
            return self._make_success_response(text=step2_text)

        def mock_get(url, params=None, timeout=None, allow_redirects=True):
            mock_cookie = MagicMock()
            mock_cookie.name = "serviceToken"
            mock_cookie.value = "svc_token"
            mock_cookie.domain = ".mi.com"
            mock_cookie.path = "/"
            mock_cookie.secure = True
            mock_cookie.expires = None
            client.session.cookies.set("serviceToken", "svc_token")
            return sts_resp

        client.session.post = mock_post
        client.session.get = mock_get

        meta = MetaLoginData(sign="sig", qs="q", callback="cb")
        session = client.login_with_step2(
            email="u@x.com",
            step2_code="123456",
            step1_token="step1tok",
            meta=meta,
            device_id="DEV1",
        )

        assert posted_data["url"] == URL_LOGIN_STEP2
        assert posted_data["data"]["code"] == "123456"
        assert posted_data["data"]["user"] == "u@x.com"
        assert posted_data["data"]["_sign"] == "sig"
        assert posted_data["data"]["trust"] == "true"
        assert posted_data["data"]["sid"] == "miothealth"
        assert posted_data["data"]["_json"] == "true"
        assert posted_data["cookies"]["step1Token"] == "step1tok"
        assert posted_data["cookies"]["deviceId"] == "DEV1"

        assert session.email == "u@x.com"
        assert session.service_token == "svc_token"
        assert session.pass_token == "pt"

    def test_login_step2_trust_false(self):
        client = MiFitnessAuthClient()

        step2_json = json.dumps({
            "code": 0,
            "userId": "12345",
            "passToken": "pt",
            "cUserId": "cuid",
            "ssecurity": "sec",
            "nonce": "999",
            "psecurity": None,
            "location": "https://example.com/sts",
        })
        step2_text = SAFE_PREFIX + step2_json

        posted_data = {}

        def mock_post(url, data=None, cookies=None, timeout=None):
            posted_data["data"] = data
            return self._make_success_response(text=step2_text)

        def mock_get(url, params=None, timeout=None, allow_redirects=True):
            client.session.cookies.set("serviceToken", "svc")
            return self._make_success_response(text="ok")

        client.session.post = mock_post
        client.session.get = mock_get

        meta = MetaLoginData(sign="s", qs="q", callback="c")
        client.login_with_step2(
            email="u@x.com",
            step2_code="111",
            step1_token="t",
            meta=meta,
            device_id="D",
            trust=False,
        )

        assert posted_data["data"]["trust"] == "false"

    def test_login_step2_missing_pass_token_raises(self):
        client = MiFitnessAuthClient()

        step2_json = json.dumps({
            "code": 0,
            "userId": "12345",
            "cUserId": "cuid",
            "ssecurity": "sec",
            "nonce": "999",
            "location": "https://example.com/sts",
        })
        step2_text = SAFE_PREFIX + step2_json

        def mock_post(url, data=None, cookies=None, timeout=None):
            return self._make_success_response(text=step2_text, cookies=[])

        client.session.post = mock_post

        meta = MetaLoginData(sign="s", qs="q", callback="c")
        with pytest.raises(XiaomiApiError, match="passToken"):
            client.login_with_step2(
                email="u@x.com",
                step2_code="111",
                step1_token="t",
                meta=meta,
                device_id="D",
            )

    def test_login_step2_invalid_code_surfaces_server_error(self):
        """An invalid/expired verification code should surface as XiaomiApiError with the server message."""
        client = MiFitnessAuthClient()

        error_json = json.dumps({
            "code": 70016,
            "desc": "Verification code is invalid or expired.",
            "_sign": "s",
            "qs": "q",
            "callback": "c",
        })
        error_text = SAFE_PREFIX + error_json

        def mock_post(url, data=None, cookies=None, timeout=None):
            return self._make_success_response(text=error_text)

        client.session.post = mock_post

        meta = MetaLoginData(sign="s", qs="q", callback="c")
        with pytest.raises(XiaomiApiError, match="invalid or expired") as exc_info:
            client.login_with_step2(
                email="u@x.com",
                step2_code="999999",
                step1_token="t",
                meta=meta,
                device_id="D",
            )

        assert exc_info.value.code == 70016


class TestFetchCaptchaImage:
    def test_fetch_captcha_image_returns_bytes_and_ick(self):
        client = MiFitnessAuthClient()

        resp = MagicMock()
        resp.content = b"\x89PNG fake image"
        resp.headers = {"ick": "ick_token_123"}
        resp.cookies = []
        resp.raise_for_status = MagicMock()

        client.session.get = MagicMock(return_value=resp)

        image_bytes, ick = client.fetch_captcha_image("/pass/getCode?icodeType=login")

        assert image_bytes == b"\x89PNG fake image"
        assert ick == "ick_token_123"
        client.session.get.assert_called_once()
        call_url = client.session.get.call_args[0][0]
        assert call_url == "https://account.xiaomi.com/pass/getCode?icodeType=login"

    def test_fetch_captcha_image_ick_from_cookie(self):
        client = MiFitnessAuthClient()

        cookie = MagicMock()
        cookie.name = "ick"
        cookie.value = "cookie_ick_val"

        resp = MagicMock()
        resp.content = b"image data"
        resp.headers = {}
        resp.cookies = [cookie]
        resp.raise_for_status = MagicMock()

        client.session.get = MagicMock(return_value=resp)

        _, ick = client.fetch_captcha_image("https://account.xiaomi.com/pass/getCode")

        assert ick == "cookie_ick_val"

    def test_fetch_captcha_image_absolute_url_not_prefixed(self):
        client = MiFitnessAuthClient()

        resp = MagicMock()
        resp.content = b"img"
        resp.headers = {"ick": "tok"}
        resp.cookies = []
        resp.raise_for_status = MagicMock()

        client.session.get = MagicMock(return_value=resp)

        client.fetch_captcha_image("https://account.xiaomi.com/pass/getCode")

        call_url = client.session.get.call_args[0][0]
        assert call_url == "https://account.xiaomi.com/pass/getCode"

    def test_fetch_captcha_image_raises_when_ick_missing(self):
        client = MiFitnessAuthClient()

        resp = MagicMock()
        resp.content = b"img"
        resp.headers = {}
        resp.cookies = []
        resp.raise_for_status = MagicMock()

        client.session.get = MagicMock(return_value=resp)

        with pytest.raises(XiaomiApiError, match="ICK token"):
            client.fetch_captcha_image("/pass/getCode")


class TestLoginWithCaptcha:
    def _make_success_response(self, *, text, headers=None, cookies=None):
        resp = MagicMock()
        resp.text = text
        resp.headers = headers or {}
        resp.cookies = cookies or []
        resp.raise_for_status = MagicMock()
        return resp

    def test_login_with_captcha_sends_captcode_and_ick(self):
        client = MiFitnessAuthClient()

        success_json = json.dumps({
            "code": 0,
            "userId": "12345",
            "passToken": "pt",
            "cUserId": "cuid",
            "ssecurity": "sec",
            "nonce": "999",
            "psecurity": None,
            "location": "https://example.com/sts",
        })
        success_text = SAFE_PREFIX + success_json

        posted_data = {}

        def mock_post(url, data=None, cookies=None, timeout=None):
            posted_data["data"] = data
            posted_data["cookies"] = cookies
            return self._make_success_response(text=success_text)

        def mock_get(url, params=None, timeout=None, allow_redirects=True):
            client.session.cookies.set("serviceToken", "svc")
            return self._make_success_response(text="ok")

        client.session.post = mock_post
        client.session.get = mock_get

        meta = MetaLoginData(sign="s", qs="q", callback="c")
        client.login_with_password(
            email="u@x.com",
            password="pass",
            device_id="DEV1",
            captcha_code="abcd",
            ick="ick_tok",
            meta=meta,
        )

        assert posted_data["data"]["captCode"] == "abcd"
        assert posted_data["cookies"]["ick"] == "ick_tok"
        assert posted_data["cookies"]["deviceId"] == "DEV1"

    def test_login_without_captcha_does_not_send_captcode(self):
        client = MiFitnessAuthClient()

        success_json = json.dumps({
            "code": 0,
            "userId": "12345",
            "passToken": "pt",
            "cUserId": "cuid",
            "ssecurity": "sec",
            "nonce": "999",
            "psecurity": None,
            "location": "https://example.com/sts",
        })
        success_text = SAFE_PREFIX + success_json

        meta_json = json.dumps({
            "code": 70002,
            "_sign": "s",
            "qs": "q",
            "callback": "c",
        })
        meta_text = SAFE_PREFIX + meta_json

        posted_data = {}
        call_count = {"get": 0}

        def mock_post(url, data=None, cookies=None, timeout=None):
            posted_data["data"] = data
            posted_data["cookies"] = cookies
            return self._make_success_response(text=success_text)

        def mock_get(url, params=None, timeout=None, allow_redirects=True, cookies=None):
            call_count["get"] += 1
            if call_count["get"] == 1:
                # Meta login data fetch
                return self._make_success_response(text=meta_text)
            client.session.cookies.set("serviceToken", "svc")
            return self._make_success_response(text="ok")

        client.session.post = mock_post
        client.session.get = mock_get

        client.login_with_password(email="u@x.com", password="pass", device_id="DEV1")

        assert "captCode" not in posted_data["data"]
        assert "ick" not in posted_data["cookies"]

    def test_login_with_password_70016_surfaces_server_error(self):
        client = MiFitnessAuthClient()

        error_json = json.dumps({
            "code": 70016,
            "desc": "Wrong password.",
            "captchaUrl": "/pass/getCode?icodeType=login",
            "_sign": "s",
            "qs": "q",
            "callback": "c",
        })
        error_text = SAFE_PREFIX + error_json

        client.session.post = MagicMock(return_value=self._make_success_response(text=error_text))

        meta = MetaLoginData(sign="s", qs="q", callback="c")
        with pytest.raises(XiaomiApiError, match="Wrong password") as exc_info:
            client.login_with_password(
                email="u@x.com",
                password="pass",
                device_id="DEV1",
                meta=meta,
            )

        assert exc_info.value.code == 70016
        assert exc_info.value.payload["captchaUrl"] == "/pass/getCode?icodeType=login"


class TestCaptchaRequiredErrorFields:
    def test_captcha_error_carries_type_and_payload(self):
        client = MiFitnessAuthClient()
        payload = {
            "code": 87001,
            "desc": "Need captcha",
            "captchaUrl": "/pass/getCode?icodeType=login",
            "type": "manMachine",
        }

        with pytest.raises(CaptchaRequiredError) as exc_info:
            client._raise_for_login_requirements(payload)

        assert exc_info.value.captcha_url == "/pass/getCode?icodeType=login"
        assert exc_info.value.captcha_type == "manMachine"
        assert exc_info.value.payload["code"] == 87001

    def test_captcha_error_default_type(self):
        client = MiFitnessAuthClient()
        payload = {"code": 87001}

        with pytest.raises(CaptchaRequiredError) as exc_info:
            client._raise_for_login_requirements(payload)

        assert exc_info.value.captcha_type == "captcha"
