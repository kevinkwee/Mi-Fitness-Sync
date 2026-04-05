class MiFitnessError(Exception):
    """Base error for CLI failures."""


class XiaomiApiError(MiFitnessError):
    """Server-side or protocol error."""

    def __init__(self, message: str, *, code: int | None = None, payload: dict | None = None):
        super().__init__(message)
        self.code = code
        self.payload = payload or {}


class CaptchaRequiredError(MiFitnessError):
    """Captcha is required before login can continue."""

    def __init__(self, captcha_url: str):
        super().__init__("Captcha is required for this account.")
        self.captcha_url = captcha_url


class NotificationRequiredError(MiFitnessError):
    """The account requires a browser or app notification confirmation."""

    def __init__(self, notification_url: str):
        super().__init__("Additional account verification is required.")
        self.notification_url = notification_url


class Step2RequiredError(MiFitnessError):
    """Step-2 login was requested by Xiaomi Passport."""

    def __init__(self, message: str, *, payload: dict | None = None):
        super().__init__(message)
        self.payload = payload or {}


class AuthStateNotFoundError(MiFitnessError):
    """No local auth state exists."""


class StravaError(MiFitnessError):
    """Base error for Strava API or OAuth failures."""


class StravaAuthError(StravaError):
    """Strava OAuth authentication failure."""
