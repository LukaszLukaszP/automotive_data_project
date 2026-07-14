# Scraping Policy

This project is a small portfolio ETL for public listing data. It is not a commercial scraper and is not intended for mass collection.

## Defaults

- one make and one model,
- narrow year range,
- at most 2 result pages,
- at most 30 listings per run,
- concurrency set to 1,
- several seconds of delay between requests,
- random jitter to spread requests, not to hide automation.

## Blocking and rate limits

The pipeline must stop when it sees:

- HTTP 403,
- HTTP 429,
- CAPTCHA or robot-verification content.

It must not attempt to bypass CAPTCHA, anti-bot checks, paywalls, account walls, or other access controls.

## Data minimization

The MVP stores vehicle listing data only. It does not collect seller contact data, VIN reveal flows, personal identifiers, or unnecessary free-form seller information.

## HTML fixtures

Parser development should use local, minimal, anonymized HTML fixtures under `tests/fixtures`. Saving raw HTML is disabled by default and should be used only for short-term debugging or fixture creation.

## Manual compliance

Before running any live scrape, manually review the source site's current terms and robots guidance. If rules or blocking signals indicate the run should stop, stop the run.
