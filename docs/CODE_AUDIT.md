# Code Audit

## Current data flow found in the repository

The original project is a set of experiments rather than one runnable ETL. The intended flow was:

1. scrape Otomoto result pages and detail pages into CSV files,
2. keep `data/ids.csv` to skip previously seen advert IDs,
3. merge raw CSV files,
4. clean Polish source columns into English columns,
5. split equipment into relational CSV files,
6. create PostgreSQL tables,
7. append CSV data into PostgreSQL,
8. explore the database in notebooks.

The new supported MVP is in `src/automotive_data_project` and replaces that manual chain with:

```text
CLI -> config -> low-intensity HTTP client -> parser -> normalization -> SQLAlchemy UPSERT
```

## Important existing scripts and responsibilities

- `scripts/pipeline/scraping_otomoto_skip_duplicates_V1.py` - main historical requests/BeautifulSoup scraper with CSV output and `data/ids.csv` deduplication.
- `scripts/pipeline/makes_and_models_scraping.py` - Selenium script for brand/model mapping.
- `scripts/pipeline/split_data_for_postgres.py` - pandas cleaning script that converts raw CSV columns and creates equipment CSVs.
- `scripts/pipeline/init_db.py` - historical PostgreSQL schema creation script.
- `scripts/pipeline/append_data.py` - historical CSV-to-PostgreSQL loader.
- `scripts/utils/data_cleaning_utils.py` - reusable unit cleaning helpers, but many raise on unexpected units and assume pandas Series.
- `scripts/utils/equipment_utils.py` - equipment splitting and relation-table helpers.
- `scripts/utils/merge_csvs.py` - merges raw CSV exports.
- `scripts/legacy/*` - older scraper variants, including multi-threaded and browser/VIN experiments.
- `otomoto_ingest/*` - unfinished Scrapy/Playwright experiment that saves raw listing HTML.
- `notebooks/*` - EDA and old merge/cleaning exploration.

## Current, old, duplicated, and unfinished elements

- Current supported path: `src/automotive_data_project`.
- Useful legacy ideas: advert ID deduplication, basic field labels, equipment parsing idea, unit cleaning idea.
- Old or duplicated: multiple scraper variants in `scripts/pipeline` and `scripts/legacy`.
- Unfinished: Scrapy items/pipelines are empty; Playwright settings reference dependencies not present in the original requirements; spider only saves raw HTML.
- Not production-ready: notebooks and pandas scripts depend on local files and manual ordering.

## Problems that prevented reliable execution

- Hardcoded local paths such as `C:/Users/Lukasz Pindus/...`.
- Hardcoded PostgreSQL URL with username/password in old scripts and notebooks.
- `init_db.py` dropped tables during normal initialization.
- Old scraper shared one `requests.Session` across threads.
- Multiple competing scraper implementations made the entry point unclear.
- `requirements.txt` included browser automation and stealth dependencies but lacked pytest.
- README paths did not match the actual `scripts/pipeline` and `scripts/utils` layout.
- Parser selectors were tied to volatile Otomoto CSS/test attributes.

## Security and configuration issues

- Database credentials were embedded directly in code.
- `.env.example` did not exist.
- `.gitignore` ignored future CSV files but already-tracked CSV data remained in the repository.
- Old browser experiments included anti-detection tools and automation hiding behavior. These are not part of the MVP.

## Absolute local paths found

- `scripts/pipeline/append_data.py`
- `scripts/pipeline/split_data_for_postgres.py`
- `scripts/utils/merge_csvs.py`
- notebook cells in `notebooks/`

The maintained MVP uses `pathlib`, environment variables, and CLI arguments instead.

## Credentials found in code

- A local PostgreSQL connection string with username and password appeared in old database scripts and notebook cells.

The maintained MVP reads `DATABASE_URL` from the environment and documents only placeholder credentials in `.env.example`.

## README inconsistencies

- README listed scripts as if they were directly under `scripts/`, while they actually live under `scripts/pipeline` and `scripts/utils`.
- README described Selenium as an active dependency, while the new MVP intentionally does not use it.
- README described future ML/dashboard plans before documenting a stable ETL run.

## Missing or excessive dependencies

- Missing for tests: `pytest`.
- Excessive for MVP: Selenium, Scrapy, Playwright, `undetected-chromedriver`, browser stealth helpers.
- New MVP dependencies are intentionally small: `requests`, `beautifulsoup4`, `SQLAlchemy`, `psycopg2-binary`.

## Risk of accidental data deletion

- Historical `init_db.py` dropped views and tables every run.
- Historical `append_data.py` replaced and dropped a temporary table.
- New `init-db` only creates missing schema. Destructive reset is isolated in `reset-db --yes-i-understand-this-drops-data`.

## Deduplication and concurrency issues

- Historical CSV ID tracking was not transaction-safe.
- A global set plus file append could race under multi-threading.
- Shared HTTP session across threads is unsafe.
- New MVP defaults to concurrency `1` and relies on database uniqueness plus UPSERT.

## Potentially stale HTML selectors

- `article[data-id]`
- `h2 a`
- `.offer-price__number`
- `.offer-price__currency`
- `data-sentry-element="Label"`
- `data-testid="content-equipments-section"`
- `data-sentry-element="BottomWrapper"`

These selectors are tested against local fixtures only. Live pages must be manually revalidated when Otomoto changes markup.

## Target architecture

Use one requests/BeautifulSoup-based scraper for MVP. Do not maintain Selenium/Scrapy/Playwright in parallel. Keep browser-based experiments as legacy only unless HTML can no longer expose needed public data.

## Elements worth reusing

- Advert ID deduplication concept.
- Polish field label mapping.
- Equipment list idea, simplified as JSON for MVP.
- Unit cleaning concepts for price, mileage, power, and engine capacity.
- Existing EDA questions from notebooks as inspiration for `examples/example_analysis.py`.
