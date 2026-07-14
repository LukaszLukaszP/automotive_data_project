from __future__ import annotations

import logging
from dataclasses import dataclass

from automotive_data_project.config import AppConfig, ScrapeConfig
from automotive_data_project.scraping.client import OtomotoClient, add_page_param
from automotive_data_project.scraping.exceptions import AccessBlocked, CaptchaDetected, FetchFailed, RateLimited
from automotive_data_project.scraping.parser import parse_listing_page, parse_offer_page, parse_total_pages
from automotive_data_project.storage.database import init_schema, make_engine, make_session_factory
from automotive_data_project.storage.repositories import ListingRepository
from automotive_data_project.transformation.normalization import normalize_listing

LOGGER = logging.getLogger(__name__)


@dataclass
class PipelineStats:
    pages_visited: int = 0
    listings_found: int = 0
    new_listings: int = 0
    skipped_duplicates: int = 0
    parse_errors: int = 0
    saved_records: int = 0
    stopped_reason: str | None = None


def collect_from_fixture(html: str, source_url: str = "fixture://offer.html") -> list[dict[str, object]]:
    raw = parse_offer_page(html, source_url=source_url, advert_id="fixture-1")
    return [normalize_listing(raw)]


def run_pipeline(config: AppConfig, scrape_config: ScrapeConfig | None = None) -> PipelineStats:
    scrape = scrape_config or config.scrape
    engine = make_engine(config.database_url)
    init_schema(engine)
    session_factory = make_session_factory(engine)
    client = OtomotoClient(scrape)
    stats = PipelineStats()
    LOGGER.info(
        "Starting pipeline source=%s make=%s model=%s years=%s-%s max_pages=%s max_listings=%s concurrency=%s",
        scrape.source,
        scrape.make,
        scrape.model,
        scrape.year_from,
        scrape.year_to,
        scrape.max_pages,
        scrape.max_listings,
        scrape.concurrency,
    )

    with session_factory.begin() as session:
        repo = ListingRepository(session)
        existing_ids = repo.existing_advert_ids(scrape.source)
        records: list[dict[str, object]] = []
        search_url = scrape.search_url()

        try:
            first_page = client.fetch(add_page_param(search_url, 1))
            total_pages = min(parse_total_pages(first_page.html), scrape.max_pages)
            pages = [(1, first_page.html)]
            for page in range(2, total_pages + 1):
                result = client.fetch(add_page_param(search_url, page))
                pages.append((page, result.html))
        except (AccessBlocked, RateLimited, CaptchaDetected) as exc:
            stats.stopped_reason = exc.__class__.__name__
            LOGGER.warning("Stopping listing-page fetch: %s", exc)
            return stats
        except FetchFailed as exc:
            stats.stopped_reason = "FetchFailed"
            LOGGER.warning("Stopping listing-page fetch after transient failure: %s", exc)
            return stats

        for _page, html in pages:
            stats.pages_visited += 1
            refs = parse_listing_page(html, base_url=scrape.base_url)
            stats.listings_found += len(refs)
            for ref in refs:
                if len(records) >= scrape.max_listings:
                    stats.stopped_reason = "max_listings"
                    break
                if ref.advert_id in existing_ids:
                    stats.skipped_duplicates += 1
                    continue
                try:
                    detail = client.fetch(ref.url)
                    if scrape.save_html_debug:
                        client.save_debug_html(detail.html, config.raw_html_dir, f"offer_{ref.advert_id}.html")
                    raw = parse_offer_page(detail.html, source_url=ref.url, advert_id=ref.advert_id)
                    records.append(normalize_listing(raw))
                    existing_ids.add(ref.advert_id)
                    stats.new_listings += 1
                except (AccessBlocked, RateLimited, CaptchaDetected) as exc:
                    stats.stopped_reason = exc.__class__.__name__
                    LOGGER.warning("Stopping detail-page fetch: %s", exc)
                    break
                except Exception:
                    stats.parse_errors += 1
                    LOGGER.exception("Could not parse listing %s", ref.advert_id)
            if stats.stopped_reason:
                break

        stats.saved_records = repo.upsert_many(records)

    LOGGER.info(
        "Finished pipeline pages=%s found=%s new=%s duplicates=%s parse_errors=%s saved=%s stopped=%s",
        stats.pages_visited,
        stats.listings_found,
        stats.new_listings,
        stats.skipped_duplicates,
        stats.parse_errors,
        stats.saved_records,
        stats.stopped_reason,
    )
    return stats
