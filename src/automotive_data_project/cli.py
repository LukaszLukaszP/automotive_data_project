from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from automotive_data_project.config import AppConfig, ScrapeConfig
from automotive_data_project.logging_config import configure_logging
from automotive_data_project.pipeline import collect_from_fixture, run_pipeline
from automotive_data_project.storage.database import init_schema, make_engine, reset_schema


def _scrape_config_from_args(args: argparse.Namespace, base: ScrapeConfig) -> ScrapeConfig:
    return ScrapeConfig(
        make=args.make or base.make,
        model=args.model or base.model,
        year_from=args.year_from or base.year_from,
        year_to=args.year_to or base.year_to,
        max_pages=args.max_pages or base.max_pages,
        max_listings=args.max_listings or base.max_listings,
        concurrency=args.concurrency or base.concurrency,
        request_delay_seconds=args.delay or base.request_delay_seconds,
        request_jitter_seconds=args.jitter or base.request_jitter_seconds,
        timeout_seconds=args.timeout or base.timeout_seconds,
        save_html_debug=args.save_html_debug or base.save_html_debug,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="automotive_data_project")
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_db = subparsers.add_parser("init-db", help="Create database schema without dropping existing tables.")
    init_db.set_defaults(handler=handle_init_db)

    reset_db = subparsers.add_parser("reset-db", help="Drop and recreate schema. Requires explicit confirmation.")
    reset_db.add_argument("--yes-i-understand-this-drops-data", action="store_true")
    reset_db.set_defaults(handler=handle_reset_db)

    scrape = subparsers.add_parser("scrape", help="Run a small configured scrape and load records.")
    scrape.add_argument("--make")
    scrape.add_argument("--model")
    scrape.add_argument("--year-from", type=int)
    scrape.add_argument("--year-to", type=int)
    scrape.add_argument("--max-pages", type=int)
    scrape.add_argument("--max-listings", type=int)
    scrape.add_argument("--concurrency", type=int)
    scrape.add_argument("--delay", type=float)
    scrape.add_argument("--jitter", type=float)
    scrape.add_argument("--timeout", type=float)
    scrape.add_argument("--save-html-debug", action="store_true")
    scrape.set_defaults(handler=handle_scrape)

    run = subparsers.add_parser("run-pipeline", help="Run the default small ETL pipeline.")
    run.add_argument("--offline-fixtures", type=Path, help="Run the full pipeline using local HTML fixtures only.")
    run.set_defaults(handler=handle_run_pipeline)

    fixture = subparsers.add_parser("parse-fixture", help="Parse a local offer HTML file without network access.")
    fixture.add_argument("path", type=Path)
    fixture.set_defaults(handler=handle_parse_fixture)
    return parser


def handle_init_db(args: argparse.Namespace, config: AppConfig) -> None:
    engine = make_engine(config.database_url)
    init_schema(engine)
    logging.getLogger(__name__).info("Schema initialized")


def handle_reset_db(args: argparse.Namespace, config: AppConfig) -> None:
    if not args.yes_i_understand_this_drops_data:
        raise SystemExit("Refusing to drop data without --yes-i-understand-this-drops-data")
    engine = make_engine(config.database_url)
    reset_schema(engine)
    logging.getLogger(__name__).warning("Schema reset completed")


def handle_scrape(args: argparse.Namespace, config: AppConfig) -> None:
    scrape = _scrape_config_from_args(args, config.scrape)
    stats = run_pipeline(config, scrape)
    print(json.dumps(stats.__dict__, default=str, ensure_ascii=False, indent=2))


def handle_run_pipeline(args: argparse.Namespace, config: AppConfig) -> None:
    offline_fixtures = args.offline_fixtures.resolve() if args.offline_fixtures else None
    stats = run_pipeline(config, offline_fixtures=offline_fixtures)
    print(json.dumps(stats.__dict__, default=str, ensure_ascii=False, indent=2))


def handle_parse_fixture(args: argparse.Namespace, config: AppConfig) -> None:
    path = args.path.resolve()
    html = path.read_text(encoding="utf-8")
    records = collect_from_fixture(html, source_url=path.as_uri())
    print(json.dumps(records, default=str, ensure_ascii=False, indent=2))


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    config = AppConfig.from_env()
    args.handler(args, config)
