import pytest

from automotive_data_project.config import ScrapeConfig
from automotive_data_project.scraping.client import OtomotoClient
from automotive_data_project.scraping.exceptions import AccessBlocked, CaptchaDetected, RateLimited


class FakeResponse:
    def __init__(self, status_code: int, text: str = "<html></html>", headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response

    def get(self, *args, **kwargs) -> FakeResponse:
        return self.response


def client_for(response: FakeResponse) -> OtomotoClient:
    config = ScrapeConfig(request_delay_seconds=0, request_jitter_seconds=0)
    return OtomotoClient(config, session=FakeSession(response), sleep_func=lambda _: None)


def test_stops_on_403() -> None:
    with pytest.raises(AccessBlocked):
        client_for(FakeResponse(403)).fetch("https://example.test")


def test_stops_on_429_and_reads_retry_after() -> None:
    with pytest.raises(RateLimited) as exc:
        client_for(FakeResponse(429, headers={"Retry-After": "30"})).fetch("https://example.test")

    assert exc.value.retry_after_seconds == 30


def test_stops_on_captcha_page() -> None:
    with pytest.raises(CaptchaDetected):
        client_for(FakeResponse(200, "<html><body>captcha</body></html>")).fetch("https://example.test")
