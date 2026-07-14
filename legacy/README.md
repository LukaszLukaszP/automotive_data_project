# Legacy code

This repository keeps older experiments for reference, but they are not the supported ETL path.

Current MVP entry points live in `src/automotive_data_project` and are exposed through:

- `python -m automotive_data_project init-db`
- `python -m automotive_data_project scrape`
- `python -m automotive_data_project run-pipeline`
- `python -m automotive_data_project parse-fixture path/to/file.html`

Legacy scripts under `scripts/legacy`, `scripts/pipeline`, `scripts/utils`, and the experimental
`otomoto_ingest` Scrapy/Playwright project should not be used for regular runs. They contain historical
selector attempts, CSV processing helpers, and browser-based experiments that were useful during exploration
but are intentionally not maintained as a second scraper implementation.
