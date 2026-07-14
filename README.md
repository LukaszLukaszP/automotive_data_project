# Automotive Data Project

Small Python ETL project for collecting a limited sample of public Otomoto car listing data, normalizing it, and loading it into a database for analysis.

The supported MVP is intentionally modest: one make/model filter, narrow year range, low request volume, concurrency `1`, and safe stopping on 403, 429, or CAPTCHA. The project does not bypass blocks or CAPTCHA.

## Quick Start: Verify Everything Offline

This is the first command path for a cloned repository. It does not connect to Otomoto. It creates or reuses `.venv`, installs dependencies, runs tests, parses a fixture, runs the full ETL twice on local fixtures, verifies deduplication/UPSERT behavior, and runs the example analysis.

```powershell
git clone https://github.com/LukaszLukaszP/automotive_data_project.git
cd automotive_data_project
powershell -ExecutionPolicy Bypass -File .\scripts\setup_and_verify.ps1
```

Expected final line:

```text
PROJECT VERIFICATION PASSED
```

## Optional Live Smoke Test

This command performs a very small live check: Toyota Corolla, years 2019-2021, one result page, at most three offers, concurrency `1`. It uses SQLite, no Playwright, no Selenium, no proxy, and no CAPTCHA or block bypass. It may stop on 403, 429, CAPTCHA, changed HTML, or lack of internet access.

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_live_smoke_test.ps1
```

Use `setup_and_verify.ps1` as the repeatable project health check. Use `run_live_smoke_test.ps1` only when you intentionally want a tiny real request test.

## ETL Flow

```text
CLI
 -> config/env
 -> requests HTML client
 -> BeautifulSoup parser
 -> cleaning/normalization
 -> SQLAlchemy UPSERT
 -> analysis examples
```

## Structure

```text
src/automotive_data_project/   supported ETL package
tests/                         offline tests and minimal HTML fixtures
docs/                          audit, architecture, data dictionary, scraping policy
examples/                      small local analysis script
scripts/                       legacy scripts kept for reference
otomoto_ingest/                legacy Scrapy/Playwright experiment
data/                          local CSV/database outputs
raw_data/                      optional debug HTML
```

## Windows Setup

```powershell
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
python -m pip install -e .
```

Copy `.env.example` to `.env` for your local notes, but do not commit `.env`. In PowerShell you can set the database URL for the current session:

```powershell
$env:DATABASE_URL="postgresql+psycopg2://automotive:automotive@localhost:5432/automotive"
```

If `DATABASE_URL` is not set, the project uses SQLite at `data/automotive_data.sqlite3`.

## PostgreSQL

Start a local PostgreSQL container:

```powershell
docker compose up -d postgres
```

Initialize schema without dropping data:

```powershell
python -m automotive_data_project init-db
```

Destructive reset is separate and requires an explicit flag:

```powershell
python -m automotive_data_project reset-db --yes-i-understand-this-drops-data
```

## Run A Small Scrape

```powershell
python -m automotive_data_project scrape `
  --make Toyota `
  --model Corolla `
  --year-from 2019 `
  --year-to 2021 `
  --max-pages 2 `
  --max-listings 30
```

Equivalent default run:

```powershell
python -m automotive_data_project run-pipeline
```

Parser-only development without network:

```powershell
python -m automotive_data_project parse-fixture tests\fixtures\offer_complete.html
```

Full offline ETL without network:

```powershell
python -m automotive_data_project run-pipeline --offline-fixtures tests\fixtures
```

Full offline smoke verification with deduplication checks:

```powershell
python scripts\offline_smoke_test.py --fixtures tests\fixtures --database-url sqlite:///data/offline_verification.sqlite3
```

## Tests

```powershell
python -m pytest -v
```

The normal test suite does not make real requests to Otomoto. It uses local HTML fixtures.

## Example Analysis

After loading data:

```powershell
python examples\example_analysis.py
```

The script prints median price by year, average price by fuel type, price by mileage bucket, and offer count by make/model.

## Example SQL

```sql
SELECT production_year, COUNT(*) AS offers, AVG(price) AS avg_price
FROM listings
GROUP BY production_year
ORDER BY production_year;

SELECT make, model, COUNT(*) AS offers
FROM listings
GROUP BY make, model
ORDER BY offers DESC;
```

## Known Limitations

- Otomoto HTML selectors may change at any time.
- Live scraping was not executed during automated tests.
- Equipment is stored as JSON for MVP simplicity.
- Old Selenium/Scrapy scripts are legacy and not part of the supported run path.
- The project stores listing data only and avoids seller contact data.

See also:

- `docs/CODE_AUDIT.md`
- `docs/ARCHITECTURE.md`
- `docs/DATA_DICTIONARY.md`
- `docs/SCRAPING_POLICY.md`
