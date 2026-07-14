class ScrapingError(Exception):
    """Base exception for extraction errors."""


class AccessBlocked(ScrapingError):
    """The source returned HTTP 403 or another blocking signal."""


class RateLimited(ScrapingError):
    """The source returned HTTP 429."""

    def __init__(self, message: str, retry_after_seconds: int | None = None) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class CaptchaDetected(ScrapingError):
    """A CAPTCHA page was detected and scraping must stop."""


class FetchFailed(ScrapingError):
    """A transient fetch failure exceeded retry limits."""
