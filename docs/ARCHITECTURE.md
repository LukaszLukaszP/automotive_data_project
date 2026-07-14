# Architecture

## Decision

The MVP uses `requests` and `BeautifulSoup`. This is the simplest approach that can parse public HTML, is easy to test with local fixtures, and does not require a browser. Playwright/Selenium are not used because the first stable version should be small, predictable, and respectful of blocking signals.

## Components

```text
src/automotive_data_project/
  cli.py                 command line entry points
  config.py              environment and CLI-driven configuration
  logging_config.py      standard logging setup
  pipeline.py            ETL orchestration
  scraping/
    client.py            low-intensity HTTP client
    parser.py            pure HTML parsing
    models.py            raw extraction dataclasses
    exceptions.py        stop conditions and fetch errors
  transformation/
    cleaning.py          unit parsing and safe conversions
    normalization.py     source fields to database records
  storage/
    database.py          engine, session, schema lifecycle
    models.py            SQLAlchemy ORM models
    repositories.py      deduplication and UPSERT
```

## Data flow

1. CLI builds `AppConfig` from environment variables and command arguments.
2. `OtomotoClient` fetches a small number of result pages.
3. `parser.parse_listing_page` extracts advert IDs and URLs.
4. Database is queried for existing IDs before detail pages are fetched.
5. Detail pages are parsed into `RawListing`.
6. Transformation cleans units and maps labels to normalized columns.
7. Repository writes records inside a transaction using `ON CONFLICT` UPSERT.
8. `first_seen_at` is preserved and `last_seen_at` is updated on repeated listings.

## Storage

The MVP stores equipment as JSON in the `listings` table. This keeps the first version simple and avoids maintaining a second relational equipment model before the extraction surface is stable. The older relational equipment logic can be revisited later.

## Blocking signals

The client stops on:

- HTTP 403,
- HTTP 429, preserving `Retry-After` when present,
- CAPTCHA-like page content.

The project does not attempt to bypass these signals.
