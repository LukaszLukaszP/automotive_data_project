from __future__ import annotations

import argparse
from pathlib import Path

from sqlalchemy import func, select

from automotive_data_project.config import AppConfig, ScrapeConfig
from automotive_data_project.pipeline import run_pipeline
from automotive_data_project.storage.database import make_engine, make_session_factory
from automotive_data_project.storage.models import Listing


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the full ETL twice on local fixtures and verify deduplication.")
    parser.add_argument("--fixtures", type=Path, default=Path("tests/fixtures"))
    parser.add_argument("--database-url", default="sqlite:///data/offline_verification.sqlite3")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    fixtures = args.fixtures.resolve()
    if not (fixtures / "listing_page.html").exists():
        raise SystemExit(f"Missing fixture directory or listing_page.html: {fixtures}")

    config = AppConfig(
        database_url=args.database_url,
        data_dir=Path("data").resolve(),
        raw_html_dir=Path("raw_data/debug_html").resolve(),
        scrape=ScrapeConfig(max_pages=1, max_listings=30, request_delay_seconds=0, request_jitter_seconds=0),
    )

    first_stats = run_pipeline(config, offline_fixtures=fixtures)
    engine = make_engine(args.database_url)
    session_factory = make_session_factory(engine)
    with session_factory() as session:
        first_count = session.execute(select(func.count()).select_from(Listing)).scalar_one()
        before = {row.advert_id: row for row in session.execute(select(Listing)).scalars().all()}
        first_seen = {advert_id: row.first_seen_at for advert_id, row in before.items()}
        last_seen = {advert_id: row.last_seen_at for advert_id, row in before.items()}

    second_stats = run_pipeline(config, offline_fixtures=fixtures)
    with session_factory() as session:
        second_count = session.execute(select(func.count()).select_from(Listing)).scalar_one()
        after = {row.advert_id: row for row in session.execute(select(Listing)).scalars().all()}

    if first_stats.saved_records != 2 or first_stats.new_listings != 2:
        raise SystemExit(f"Expected first run to save 2 new records, got {first_stats}")
    if first_count != 2:
        raise SystemExit(f"Expected 2 rows after first run, got {first_count}")
    if second_stats.saved_records != 0:
        raise SystemExit(f"Expected second run to save 0 new records, got {second_stats.saved_records}")
    if second_stats.skipped_duplicates != 2 or second_stats.last_seen_updates != 2:
        raise SystemExit(f"Expected second run to touch 2 duplicates, got {second_stats}")
    if second_count != 2:
        raise SystemExit(f"Expected 2 rows after second run, got {second_count}")

    for advert_id, row in after.items():
        if row.first_seen_at != first_seen[advert_id]:
            raise SystemExit(f"first_seen_at changed for advert {advert_id}")
        if row.last_seen_at < last_seen[advert_id]:
            raise SystemExit(f"last_seen_at was not preserved/advanced for advert {advert_id}")

    print("OFFLINE PIPELINE VERIFICATION PASSED")
    print(f"First run: {first_stats}")
    print(f"Second run: {second_stats}")


if __name__ == "__main__":
    main()
