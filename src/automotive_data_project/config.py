from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _bool_from_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def slugify(value: str) -> str:
    """Create a simple Otomoto path slug from a make or model name."""
    return value.strip().lower().replace(" ", "-")


@dataclass(frozen=True)
class ScrapeConfig:
    make: str = "Toyota"
    model: str = "Corolla"
    year_from: int = 2019
    year_to: int = 2021
    max_pages: int = 2
    max_listings: int = 30
    concurrency: int = 1
    request_delay_seconds: float = 4.0
    request_jitter_seconds: float = 2.0
    timeout_seconds: float = 20.0
    save_html_debug: bool = False
    source: str = "otomoto"
    base_url: str = "https://www.otomoto.pl"

    def search_url(self) -> str:
        """Build the central search URL for the configured filters."""
        path = f"/osobowe/{slugify(self.make)}/{slugify(self.model)}"
        params = {
            "search[filter_float_year:from]": self.year_from,
            "search[filter_float_year:to]": self.year_to,
            "search[advanced_search_expanded]": "true",
        }
        return f"{self.base_url}{path}?{urlencode(params)}"


@dataclass(frozen=True)
class AppConfig:
    database_url: str
    data_dir: Path
    raw_html_dir: Path
    scrape: ScrapeConfig

    @classmethod
    def from_env(cls) -> AppConfig:
        data_dir = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data")).resolve()
        raw_html_dir = Path(os.getenv("RAW_HTML_DIR", PROJECT_ROOT / "raw_data" / "debug_html")).resolve()
        scrape = ScrapeConfig(
            make=os.getenv("SCRAPE_MAKE", "Toyota"),
            model=os.getenv("SCRAPE_MODEL", "Corolla"),
            year_from=int(os.getenv("SCRAPE_YEAR_FROM", "2019")),
            year_to=int(os.getenv("SCRAPE_YEAR_TO", "2021")),
            max_pages=int(os.getenv("SCRAPE_MAX_PAGES", "2")),
            max_listings=int(os.getenv("SCRAPE_MAX_LISTINGS", "30")),
            concurrency=int(os.getenv("SCRAPE_CONCURRENCY", "1")),
            request_delay_seconds=float(os.getenv("SCRAPE_DELAY_SECONDS", "4")),
            request_jitter_seconds=float(os.getenv("SCRAPE_JITTER_SECONDS", "2")),
            timeout_seconds=float(os.getenv("SCRAPE_TIMEOUT_SECONDS", "20")),
            save_html_debug=_bool_from_env("SCRAPE_SAVE_HTML_DEBUG", False),
        )
        return cls(
            database_url=os.getenv("DATABASE_URL", f"sqlite:///{data_dir / 'automotive_data.sqlite3'}"),
            data_dir=data_dir,
            raw_html_dir=raw_html_dir,
            scrape=scrape,
        )
