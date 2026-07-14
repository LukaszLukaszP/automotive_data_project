from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests

from automotive_data_project.config import ScrapeConfig
from automotive_data_project.scraping.exceptions import AccessBlocked, CaptchaDetected, FetchFailed, RateLimited
from automotive_data_project.scraping.parser import is_captcha_html

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class FetchResult:
    url: str
    html: str
    status_code: int


def add_page_param(url: str, page: int) -> str:
    parts = urlsplit(url)
    params = dict(parse_qsl(parts.query, keep_blank_values=True))
    params["page"] = str(page)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(params), parts.fragment))


class OtomotoClient:
    """Low-intensity HTTP client. It stops on blocking signals instead of bypassing them."""

    def __init__(
        self,
        config: ScrapeConfig,
        session: requests.Session | None = None,
        sleep_func=time.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.sleep_func = sleep_func
        self.rng = rng or random.Random()

    def _pause(self) -> None:
        delay = self.config.request_delay_seconds + self.rng.uniform(0, self.config.request_jitter_seconds)
        LOGGER.debug("Sleeping %.2f seconds before request", delay)
        self.sleep_func(delay)

    def fetch(self, url: str) -> FetchResult:
        self._pause()
        try:
            response = self.session.get(
                url,
                timeout=self.config.timeout_seconds,
                headers={"Accept": "text/html,application/xhtml+xml"},
            )
        except requests.RequestException as exc:
            raise FetchFailed(str(exc)) from exc

        if response.status_code == 403:
            raise AccessBlocked(f"HTTP 403 for {url}")
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            retry_after_seconds = int(retry_after) if retry_after and retry_after.isdigit() else None
            raise RateLimited(f"HTTP 429 for {url}", retry_after_seconds=retry_after_seconds)
        if response.status_code >= 500:
            raise FetchFailed(f"HTTP {response.status_code} for {url}")
        response.raise_for_status()

        if is_captcha_html(response.text):
            raise CaptchaDetected(f"CAPTCHA detected for {url}")
        return FetchResult(url=url, html=response.text, status_code=response.status_code)

    def save_debug_html(self, html: str, target_dir: Path, name: str) -> Path:
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / name
        path.write_text(html, encoding="utf-8")
        return path
