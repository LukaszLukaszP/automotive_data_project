from pathlib import Path

from sqlalchemy import func, select

from automotive_data_project.config import AppConfig, ScrapeConfig
from automotive_data_project.pipeline import run_pipeline
from automotive_data_project.storage.database import init_schema, make_engine, make_session_factory
from automotive_data_project.storage.models import Listing
from automotive_data_project.storage.repositories import ListingRepository

FIXTURES = Path(__file__).parent / "fixtures"


def test_pipeline_on_offline_fixtures_deduplicates_and_touches_last_seen(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'offline.sqlite3'}"
    config = AppConfig(
        database_url=database_url,
        data_dir=tmp_path,
        raw_html_dir=tmp_path / "html",
        scrape=ScrapeConfig(max_pages=1, max_listings=30, request_delay_seconds=0, request_jitter_seconds=0),
    )

    first_stats = run_pipeline(config, offline_fixtures=FIXTURES)
    engine = make_engine(database_url)
    session_factory = make_session_factory(engine)
    with session_factory() as session:
        count_after_first = session.execute(select(func.count()).select_from(Listing)).scalar_one()
        first_seen = {
            row.advert_id: row.first_seen_at for row in session.execute(select(Listing)).scalars().all()
        }
        last_seen = {
            row.advert_id: row.last_seen_at for row in session.execute(select(Listing)).scalars().all()
        }

    second_stats = run_pipeline(config, offline_fixtures=FIXTURES)
    with session_factory() as session:
        count_after_second = session.execute(select(func.count()).select_from(Listing)).scalar_one()
        after = {row.advert_id: row for row in session.execute(select(Listing)).scalars().all()}

    assert first_stats.saved_records == 2
    assert first_stats.new_listings == 2
    assert count_after_first == 2
    assert second_stats.saved_records == 0
    assert second_stats.skipped_duplicates == 2
    assert second_stats.last_seen_updates == 2
    assert count_after_second == 2
    assert after["1001"].first_seen_at == first_seen["1001"]
    assert after["1002"].first_seen_at == first_seen["1002"]
    assert after["1001"].last_seen_at >= last_seen["1001"]
    assert after["1002"].last_seen_at >= last_seen["1002"]


def test_pipeline_stops_at_max_listings_on_offline_fixtures(tmp_path) -> None:
    config = AppConfig(
        database_url=f"sqlite+pysqlite:///{tmp_path / 'limited.sqlite3'}",
        data_dir=tmp_path,
        raw_html_dir=tmp_path / "html",
        scrape=ScrapeConfig(max_pages=1, max_listings=1, request_delay_seconds=0, request_jitter_seconds=0),
    )

    stats = run_pipeline(config, offline_fixtures=FIXTURES)

    assert stats.saved_records == 1
    assert stats.stopped_reason == "max_listings"


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
