from dataclasses import replace
from pathlib import Path

import automotive_data_project.pipeline as pipeline_module
from automotive_data_project.config import AppConfig, ScrapeConfig
from automotive_data_project.scraping.client import FetchResult
from automotive_data_project.storage.database import init_schema, make_engine, make_session_factory
from automotive_data_project.storage.repositories import ListingRepository

FIXTURES = Path(__file__).parent / "fixtures"


class FakeClient:
    def __init__(self, config: ScrapeConfig) -> None:
        self.config = config

    def fetch(self, url: str) -> FetchResult:
        if "page=" in url:
            return FetchResult(url, (FIXTURES / "listing_page.html").read_text(encoding="utf-8"), 200)
        if "ID1001" in url:
            return FetchResult(url, (FIXTURES / "offer_complete.html").read_text(encoding="utf-8"), 200)
        return FetchResult(url, (FIXTURES / "offer_missing_field.html").read_text(encoding="utf-8"), 200)

    def save_debug_html(self, html: str, target_dir: Path, name: str) -> Path:
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / name
        path.write_text(html, encoding="utf-8")
        return path


def test_pipeline_on_fixture_database_and_max_listing_stop(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(pipeline_module, "OtomotoClient", FakeClient)
    config = AppConfig(
        database_url=f"sqlite+pysqlite:///{tmp_path / 'test.sqlite3'}",
        data_dir=tmp_path,
        raw_html_dir=tmp_path / "html",
        scrape=replace(ScrapeConfig(), max_pages=1, max_listings=1, request_delay_seconds=0, request_jitter_seconds=0),
    )

    stats = pipeline_module.run_pipeline(config)
    rerun_stats = pipeline_module.run_pipeline(config)

    assert stats.saved_records == 1
    assert stats.stopped_reason == "max_listings"
    assert rerun_stats.saved_records == 1
    assert rerun_stats.skipped_duplicates == 1


def test_repository_deduplication_reads_existing_ids(tmp_path) -> None:
    engine = make_engine(f"sqlite+pysqlite:///{tmp_path / 'test.sqlite3'}")
    init_schema(engine)
    session_factory = make_session_factory(engine)

    with session_factory.begin() as session:
        repo = ListingRepository(session)
        repo.upsert_many(
            [
                {
                    "advert_id": "1001",
                    "source": "otomoto",
                    "source_url": "https://example.test/1001",
                }
            ]
        )
        assert repo.existing_advert_ids("otomoto") == {"1001"}
